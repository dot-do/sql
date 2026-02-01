/**
 * Circuit Breaker for Distributed Transaction Coordinator
 *
 * Implements the circuit breaker pattern to protect participant communication
 * from cascading failures. The circuit breaker has three states:
 *
 * - CLOSED: Normal operation, requests pass through
 * - OPEN: Circuit is tripped, requests fail immediately
 * - HALF_OPEN: Recovery probe state, limited requests pass through
 *
 * @packageDocumentation
 */

// =============================================================================
// TYPES
// =============================================================================

/**
 * Circuit breaker states
 */
export type CircuitBreakerState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

/**
 * Configuration for the circuit breaker
 */
export interface CircuitBreakerConfig {
  /** Number of failures before opening the circuit (default: 5) */
  failureThreshold?: number;
  /** Time in ms before attempting recovery after opening (default: 30000) */
  resetTimeoutMs?: number;
  /** Number of successful probe calls to close circuit (default: 2) */
  successThreshold?: number;
  /** Time window for failure counting in ms (default: 60000) */
  failureWindowMs?: number;
  /** Callback when state changes */
  onStateChange?: (participantId: string, oldState: CircuitBreakerState, newState: CircuitBreakerState) => void;
}

/**
 * Per-participant circuit breaker state
 */
export interface ParticipantCircuitState {
  /** Current state of the circuit */
  state: CircuitBreakerState;
  /** Timestamps of recent failures within the failure window */
  failures: number[];
  /** Number of consecutive successes in HALF_OPEN state */
  halfOpenSuccesses: number;
  /** When the circuit was opened */
  openedAt: number | null;
  /** Total failure count (for metrics) */
  totalFailures: number;
  /** Total success count (for metrics) */
  totalSuccesses: number;
}

/**
 * Circuit breaker metrics for observability
 */
export interface CircuitBreakerMetrics {
  /** Participant ID */
  participantId: string;
  /** Current state */
  state: CircuitBreakerState;
  /** Total failures recorded */
  totalFailures: number;
  /** Total successes recorded */
  totalSuccesses: number;
  /** Failures in current window */
  recentFailures: number;
  /** Time since circuit opened (if open) */
  timeSinceOpenMs: number | null;
}

/**
 * Result of a circuit breaker execution
 */
export interface CircuitBreakerResult<T> {
  success: boolean;
  result?: T;
  error?: Error;
  circuitOpen: boolean;
}

// =============================================================================
// CIRCUIT BREAKER IMPLEMENTATION
// =============================================================================

/**
 * Creates a circuit breaker manager for participant communication
 */
export function createCircuitBreaker(config: CircuitBreakerConfig = {}) {
  const {
    failureThreshold = 5,
    resetTimeoutMs = 30000,
    successThreshold = 2,
    failureWindowMs = 60000,
    onStateChange,
  } = config;

  const circuits = new Map<string, ParticipantCircuitState>();

  /**
   * Get or create circuit state for a participant
   */
  function getCircuit(participantId: string): ParticipantCircuitState {
    let circuit = circuits.get(participantId);
    if (!circuit) {
      circuit = {
        state: 'CLOSED',
        failures: [],
        halfOpenSuccesses: 0,
        openedAt: null,
        totalFailures: 0,
        totalSuccesses: 0,
      };
      circuits.set(participantId, circuit);
    }
    return circuit;
  }

  /**
   * Clean up old failures outside the window
   */
  function pruneOldFailures(circuit: ParticipantCircuitState): void {
    const now = Date.now();
    const cutoff = now - failureWindowMs;
    circuit.failures = circuit.failures.filter((ts) => ts > cutoff);
  }

  /**
   * Transition to a new state
   */
  function transitionTo(
    participantId: string,
    circuit: ParticipantCircuitState,
    newState: CircuitBreakerState
  ): void {
    const oldState = circuit.state;
    if (oldState === newState) return;

    circuit.state = newState;

    if (newState === 'OPEN') {
      circuit.openedAt = Date.now();
      circuit.halfOpenSuccesses = 0;
    } else if (newState === 'CLOSED') {
      circuit.openedAt = null;
      circuit.halfOpenSuccesses = 0;
      circuit.failures = [];
    } else if (newState === 'HALF_OPEN') {
      circuit.halfOpenSuccesses = 0;
    }

    onStateChange?.(participantId, oldState, newState);
  }

  /**
   * Check if circuit should transition to HALF_OPEN
   */
  function checkForHalfOpen(participantId: string, circuit: ParticipantCircuitState): void {
    if (circuit.state !== 'OPEN' || circuit.openedAt === null) return;

    const elapsed = Date.now() - circuit.openedAt;
    if (elapsed >= resetTimeoutMs) {
      transitionTo(participantId, circuit, 'HALF_OPEN');
    }
  }

  /**
   * Record a failure for a participant
   */
  function recordFailure(participantId: string): void {
    const circuit = getCircuit(participantId);
    circuit.totalFailures++;
    circuit.failures.push(Date.now());

    // Clean up old failures
    pruneOldFailures(circuit);

    if (circuit.state === 'HALF_OPEN') {
      // Any failure in half-open goes back to open
      transitionTo(participantId, circuit, 'OPEN');
    } else if (circuit.state === 'CLOSED') {
      // Check if we've hit the threshold
      if (circuit.failures.length >= failureThreshold) {
        transitionTo(participantId, circuit, 'OPEN');
      }
    }
  }

  /**
   * Record a success for a participant
   */
  function recordSuccess(participantId: string): void {
    const circuit = getCircuit(participantId);
    circuit.totalSuccesses++;

    if (circuit.state === 'HALF_OPEN') {
      circuit.halfOpenSuccesses++;
      if (circuit.halfOpenSuccesses >= successThreshold) {
        transitionTo(participantId, circuit, 'CLOSED');
      }
    } else if (circuit.state === 'CLOSED') {
      // In closed state, successes help clear the failure window naturally
      // through time-based pruning
    }
  }

  /**
   * Check if the circuit allows a request to pass through
   */
  function canExecute(participantId: string): boolean {
    const circuit = getCircuit(participantId);

    // Always check for half-open transition first
    checkForHalfOpen(participantId, circuit);

    switch (circuit.state) {
      case 'CLOSED':
        return true;
      case 'HALF_OPEN':
        // Allow limited requests in half-open
        return true;
      case 'OPEN':
        return false;
    }
  }

  /**
   * Get the current state for a participant
   */
  function getState(participantId: string): CircuitBreakerState {
    const circuit = getCircuit(participantId);
    checkForHalfOpen(participantId, circuit);
    return circuit.state;
  }

  /**
   * Get metrics for a participant
   */
  function getMetrics(participantId: string): CircuitBreakerMetrics {
    const circuit = getCircuit(participantId);
    checkForHalfOpen(participantId, circuit);
    pruneOldFailures(circuit);

    return {
      participantId,
      state: circuit.state,
      totalFailures: circuit.totalFailures,
      totalSuccesses: circuit.totalSuccesses,
      recentFailures: circuit.failures.length,
      timeSinceOpenMs: circuit.openedAt !== null ? Date.now() - circuit.openedAt : null,
    };
  }

  /**
   * Get metrics for all participants
   */
  function getAllMetrics(): CircuitBreakerMetrics[] {
    return Array.from(circuits.keys()).map((id) => getMetrics(id));
  }

  /**
   * Execute a function with circuit breaker protection
   */
  async function execute<T>(
    participantId: string,
    fn: () => Promise<T>
  ): Promise<CircuitBreakerResult<T>> {
    if (!canExecute(participantId)) {
      return {
        success: false,
        error: new CircuitBreakerOpenError(participantId, getState(participantId)),
        circuitOpen: true,
      };
    }

    try {
      const result = await fn();
      recordSuccess(participantId);
      return {
        success: true,
        result,
        circuitOpen: false,
      };
    } catch (error) {
      recordFailure(participantId);
      return {
        success: false,
        error: error instanceof Error ? error : new Error(String(error)),
        circuitOpen: false,
      };
    }
  }

  /**
   * Reset circuit state for a participant
   */
  function reset(participantId: string): void {
    const circuit = getCircuit(participantId);
    const oldState = circuit.state;
    circuit.state = 'CLOSED';
    circuit.failures = [];
    circuit.halfOpenSuccesses = 0;
    circuit.openedAt = null;

    if (oldState !== 'CLOSED') {
      onStateChange?.(participantId, oldState, 'CLOSED');
    }
  }

  /**
   * Reset all circuits
   */
  function resetAll(): void {
    for (const participantId of circuits.keys()) {
      reset(participantId);
    }
  }

  /**
   * Force open a circuit (for testing or manual intervention)
   */
  function forceOpen(participantId: string): void {
    const circuit = getCircuit(participantId);
    transitionTo(participantId, circuit, 'OPEN');
  }

  /**
   * Force close a circuit (for testing or manual intervention)
   */
  function forceClose(participantId: string): void {
    const circuit = getCircuit(participantId);
    transitionTo(participantId, circuit, 'CLOSED');
  }

  return {
    canExecute,
    recordFailure,
    recordSuccess,
    getState,
    getMetrics,
    getAllMetrics,
    execute,
    reset,
    resetAll,
    forceOpen,
    forceClose,
  };
}

/**
 * Type for the circuit breaker instance
 */
export type CircuitBreaker = ReturnType<typeof createCircuitBreaker>;

// =============================================================================
// ERRORS
// =============================================================================

/**
 * Error thrown when circuit is open
 */
export class CircuitBreakerOpenError extends Error {
  public readonly participantId: string;
  public readonly circuitState: CircuitBreakerState;

  constructor(participantId: string, circuitState: CircuitBreakerState) {
    super(`Circuit breaker is ${circuitState} for participant ${participantId}`);
    this.name = 'CircuitBreakerOpenError';
    this.participantId = participantId;
    this.circuitState = circuitState;
  }
}
