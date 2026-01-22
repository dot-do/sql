/**
 * R2 Circuit Breaker
 *
 * Implements the circuit breaker pattern for R2 storage operations to prevent
 * cascading failures when R2 becomes unavailable.
 *
 * Circuit Breaker States:
 * - CLOSED: Normal operation, requests pass through
 * - OPEN: Failures exceeded threshold, requests rejected immediately
 * - HALF_OPEN: Testing recovery, allows single test request
 *
 * State Transitions:
 * ```
 *         N consecutive failures
 *   CLOSED ─────────────────────> OPEN
 *      ^                           │
 *      │                           │ timeout (configurable)
 *      │                           v
 *      │        success      HALF_OPEN
 *      └─────────────────────────┘
 *           │
 *           │ failure
 *           v
 *         OPEN
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Circuit breaker states
 */
export enum CircuitState {
  CLOSED = 'CLOSED',
  OPEN = 'OPEN',
  HALF_OPEN = 'HALF_OPEN',
}

/**
 * Circuit breaker configuration
 */
export interface CircuitBreakerConfig {
  /** Number of consecutive failures before opening circuit (default: 5) */
  failureThreshold: number;
  /** Time in ms to wait before transitioning to half-open (default: 30000) */
  resetTimeoutMs: number;
  /** Number of successful requests to close circuit (default: 1) */
  successThreshold: number;
  /** Bucket identifier for separate circuits */
  bucketId?: string;
}

/**
 * Circuit breaker metrics
 */
export interface CircuitMetrics {
  state: CircuitState;
  consecutiveFailures: number;
  totalFailures: number;
  totalSuccesses: number;
  lastFailureTime: number | null;
  lastSuccessTime: number | null;
  stateChangedAt: number;
}

/**
 * Result of a circuit-protected operation
 */
export interface CircuitProtectedResult<T> {
  success: boolean;
  data?: T;
  error?: string;
  circuitState: CircuitState;
  rejectedByCircuit: boolean;
}

/**
 * State to persist across DO hibernation
 */
interface PersistedCircuitState {
  state: CircuitState;
  consecutiveFailures: number;
  totalFailures: number;
  totalSuccesses: number;
  lastFailureTime: number | null;
  lastSuccessTime: number | null;
  stateChangedAt: number;
  openedAt: number | null;
}

/**
 * DurableObjectStorage interface for persistence
 */
interface DurableObjectStorage {
  get(key: string): Promise<unknown>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<void>;
}

/**
 * Default circuit breaker configuration
 */
export const DEFAULT_CIRCUIT_BREAKER_CONFIG: CircuitBreakerConfig = {
  failureThreshold: 5,
  resetTimeoutMs: 30000,
  successThreshold: 1,
};

// =============================================================================
// CircuitBreaker Class
// =============================================================================

/**
 * R2 Circuit Breaker implementation
 */
export class R2CircuitBreaker {
  private readonly config: CircuitBreakerConfig;
  private readonly storage?: DurableObjectStorage;
  private readonly storageKey: string;

  // State
  private state: CircuitState = CircuitState.CLOSED;
  private consecutiveFailures: number = 0;
  private totalFailures: number = 0;
  private totalSuccesses: number = 0;
  private lastFailureTime: number | null = null;
  private lastSuccessTime: number | null = null;
  private stateChangedAt: number = Date.now();
  private openedAt: number | null = null;
  private testRequestInProgress: boolean = false;

  constructor(config: CircuitBreakerConfig, storage?: DurableObjectStorage) {
    this.config = { ...DEFAULT_CIRCUIT_BREAKER_CONFIG, ...config };
    this.storage = storage;
    this.storageKey = `circuitBreaker:${config.bucketId ?? 'default'}`;
  }

  // ===========================================================================
  // Core API
  // ===========================================================================

  /**
   * Execute an operation with circuit breaker protection
   */
  async execute<T>(operation: () => Promise<T>): Promise<CircuitProtectedResult<T>> {
    // Update state based on timeout (OPEN -> HALF_OPEN transition)
    this.checkTimeoutTransition();

    // Check if circuit allows requests
    if (!this.canExecute()) {
      return {
        success: false,
        error: this.state === CircuitState.HALF_OPEN
          ? 'Circuit breaker HALF_OPEN: test request in progress'
          : 'Circuit breaker is OPEN',
        circuitState: this.state,
        rejectedByCircuit: true,
      };
    }

    // Mark test request in progress for HALF_OPEN state
    if (this.state === CircuitState.HALF_OPEN) {
      this.testRequestInProgress = true;
    }

    try {
      const result = await operation();
      this.recordSuccess();
      return {
        success: true,
        data: result,
        circuitState: this.state,
        rejectedByCircuit: false,
      };
    } catch (error) {
      this.recordFailure(error instanceof Error ? error : new Error(String(error)));
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
        circuitState: this.state,
        rejectedByCircuit: false,
      };
    } finally {
      // Reset test request flag
      if (this.testRequestInProgress) {
        this.testRequestInProgress = false;
      }
    }
  }

  /**
   * Get current circuit state
   */
  getState(): CircuitState {
    // Check for timeout transition before returning state
    this.checkTimeoutTransition();
    return this.state;
  }

  /**
   * Get circuit metrics
   */
  getMetrics(): CircuitMetrics {
    return {
      state: this.getState(),
      consecutiveFailures: this.consecutiveFailures,
      totalFailures: this.totalFailures,
      totalSuccesses: this.totalSuccesses,
      lastFailureTime: this.lastFailureTime,
      lastSuccessTime: this.lastSuccessTime,
      stateChangedAt: this.stateChangedAt,
    };
  }

  /**
   * Force circuit to specific state (for testing/admin)
   */
  forceState(state: CircuitState): void {
    const previousState = this.state;
    this.state = state;
    this.stateChangedAt = Date.now();

    if (state === CircuitState.OPEN) {
      this.openedAt = Date.now();
    } else if (state === CircuitState.CLOSED) {
      this.openedAt = null;
      this.consecutiveFailures = 0;
    }

    this.logStateTransition(previousState, state, 'forced');
  }

  /**
   * Reset circuit to closed state
   */
  reset(): void {
    const previousState = this.state;
    this.state = CircuitState.CLOSED;
    this.consecutiveFailures = 0;
    this.openedAt = null;
    this.stateChangedAt = Date.now();
    this.testRequestInProgress = false;

    this.logStateTransition(previousState, CircuitState.CLOSED, 'reset');
  }

  /**
   * Check if circuit allows requests
   */
  canExecute(): boolean {
    this.checkTimeoutTransition();

    switch (this.state) {
      case CircuitState.CLOSED:
        return true;
      case CircuitState.OPEN:
        return false;
      case CircuitState.HALF_OPEN:
        // Only allow one test request at a time
        return !this.testRequestInProgress;
      default:
        return false;
    }
  }

  /**
   * Record a failure (internal)
   */
  recordFailure(error: Error): void {
    const now = Date.now();
    this.consecutiveFailures++;
    this.totalFailures++;
    this.lastFailureTime = now;

    const previousState = this.state;

    if (this.state === CircuitState.HALF_OPEN) {
      // Failed test request - re-open circuit
      this.state = CircuitState.OPEN;
      this.openedAt = now;
      this.stateChangedAt = now;
      this.logStateTransition(previousState, CircuitState.OPEN, 'test_failed', error.message);
    } else if (this.state === CircuitState.CLOSED && this.consecutiveFailures >= this.config.failureThreshold) {
      // Threshold exceeded - open circuit
      this.state = CircuitState.OPEN;
      this.openedAt = now;
      this.stateChangedAt = now;
      this.logStateTransition(previousState, CircuitState.OPEN, 'threshold_exceeded', error.message);
    }
  }

  /**
   * Record a success (internal)
   */
  recordSuccess(): void {
    const now = Date.now();
    this.totalSuccesses++;
    this.lastSuccessTime = now;

    const previousState = this.state;

    if (this.state === CircuitState.HALF_OPEN) {
      // Successful test request - close circuit
      this.state = CircuitState.CLOSED;
      this.consecutiveFailures = 0;
      this.openedAt = null;
      this.stateChangedAt = now;
      this.logStateTransition(previousState, CircuitState.CLOSED, 'test_succeeded');
    } else if (this.state === CircuitState.CLOSED) {
      // Reset consecutive failures on success in closed state
      this.consecutiveFailures = 0;
    }
  }

  // ===========================================================================
  // Persistence
  // ===========================================================================

  /**
   * Persist state for DO hibernation
   */
  async persistState(): Promise<void> {
    if (!this.storage) {
      return;
    }

    const persistedState: PersistedCircuitState = {
      state: this.state,
      consecutiveFailures: this.consecutiveFailures,
      totalFailures: this.totalFailures,
      totalSuccesses: this.totalSuccesses,
      lastFailureTime: this.lastFailureTime,
      lastSuccessTime: this.lastSuccessTime,
      stateChangedAt: this.stateChangedAt,
      openedAt: this.openedAt,
    };

    await this.storage.put(this.storageKey, persistedState);
  }

  /**
   * Restore state after DO wake
   */
  async restoreState(): Promise<void> {
    if (!this.storage) {
      return;
    }

    try {
      const persisted = await this.storage.get(this.storageKey);

      if (!persisted || typeof persisted !== 'object') {
        // No persisted state or invalid - keep defaults (CLOSED)
        return;
      }

      const state = persisted as Partial<PersistedCircuitState>;

      // Validate and restore state
      if (state.state && Object.values(CircuitState).includes(state.state)) {
        this.state = state.state;
      }

      if (typeof state.consecutiveFailures === 'number') {
        this.consecutiveFailures = state.consecutiveFailures;
      }

      if (typeof state.totalFailures === 'number') {
        this.totalFailures = state.totalFailures;
      }

      if (typeof state.totalSuccesses === 'number') {
        this.totalSuccesses = state.totalSuccesses;
      }

      if (state.lastFailureTime !== undefined) {
        this.lastFailureTime = state.lastFailureTime;
      }

      if (state.lastSuccessTime !== undefined) {
        this.lastSuccessTime = state.lastSuccessTime;
      }

      if (typeof state.stateChangedAt === 'number') {
        this.stateChangedAt = state.stateChangedAt;
      }

      if (state.openedAt !== undefined) {
        this.openedAt = state.openedAt;
      }

      // Check for timeout transition after restore
      this.checkTimeoutTransition();
    } catch {
      // Handle corrupted state gracefully - keep defaults
      console.warn(`Failed to restore circuit breaker state for ${this.storageKey}, using defaults`);
    }
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  /**
   * Check if timeout has elapsed and transition OPEN -> HALF_OPEN
   */
  private checkTimeoutTransition(): void {
    if (this.state !== CircuitState.OPEN || this.openedAt === null) {
      return;
    }

    const now = Date.now();
    const elapsed = now - this.openedAt;

    if (elapsed > this.config.resetTimeoutMs) {
      const previousState = this.state;
      this.state = CircuitState.HALF_OPEN;
      this.stateChangedAt = now;
      this.testRequestInProgress = false;
      this.logStateTransition(previousState, CircuitState.HALF_OPEN, 'timeout_elapsed');
    }
  }

  /**
   * Log state transitions for monitoring
   */
  private logStateTransition(
    from: CircuitState,
    to: CircuitState,
    reason: string,
    errorMessage?: string
  ): void {
    const bucketId = this.config.bucketId ?? 'default';
    const logMessage = `Circuit breaker [${bucketId}] ${from} -> ${to} (${reason})`;

    if (errorMessage) {
      console.log(`${logMessage}: ${errorMessage}`);
    } else {
      console.log(logMessage);
    }
  }
}

// =============================================================================
// CircuitBreakerManager
// =============================================================================

/**
 * Manager for multiple circuit breakers (per-bucket)
 */
export class CircuitBreakerManager {
  private readonly circuits: Map<string, R2CircuitBreaker> = new Map();
  private readonly config: CircuitBreakerConfig;
  private readonly storage?: DurableObjectStorage;

  constructor(
    config: Partial<CircuitBreakerConfig> = {},
    storage?: DurableObjectStorage
  ) {
    this.config = { ...DEFAULT_CIRCUIT_BREAKER_CONFIG, ...config };
    this.storage = storage;
  }

  /**
   * Get circuit breaker for specific bucket
   */
  getCircuit(bucketId: string): R2CircuitBreaker {
    let circuit = this.circuits.get(bucketId);

    if (!circuit) {
      circuit = new R2CircuitBreaker(
        { ...this.config, bucketId },
        this.storage
      );
      this.circuits.set(bucketId, circuit);
    }

    return circuit;
  }

  /**
   * Get all circuit states
   */
  getAllStates(): Map<string, CircuitState> {
    const states = new Map<string, CircuitState>();
    for (const [bucketId, circuit] of this.circuits) {
      states.set(bucketId, circuit.getState());
    }
    return states;
  }

  /**
   * Reset all circuits
   */
  resetAll(): void {
    for (const circuit of this.circuits.values()) {
      circuit.reset();
    }
  }

  /**
   * Persist all circuit states
   */
  async persistAll(): Promise<void> {
    const promises = Array.from(this.circuits.values()).map(
      circuit => circuit.persistState()
    );
    await Promise.all(promises);
  }

  /**
   * Restore all circuit states
   */
  async restoreAll(): Promise<void> {
    const promises = Array.from(this.circuits.values()).map(
      circuit => circuit.restoreState()
    );
    await Promise.all(promises);
  }

  /**
   * Get all metrics for monitoring
   */
  getAllMetrics(): Map<string, CircuitMetrics> {
    const metrics = new Map<string, CircuitMetrics>();
    for (const [bucketId, circuit] of this.circuits) {
      metrics.set(bucketId, circuit.getMetrics());
    }
    return metrics;
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a new circuit breaker
 */
export function createCircuitBreaker(
  config: CircuitBreakerConfig,
  storage?: DurableObjectStorage
): R2CircuitBreaker {
  return new R2CircuitBreaker(config, storage);
}

/**
 * Create a new circuit breaker manager
 */
export function createCircuitBreakerManager(
  config: Partial<CircuitBreakerConfig> = {},
  storage?: DurableObjectStorage
): CircuitBreakerManager {
  return new CircuitBreakerManager(config, storage);
}
