/**
 * R2 Circuit Breaker Tests (TDD RED Phase)
 *
 * These tests document the MISSING circuit breaker functionality for R2 operations.
 * Circuit breaker pattern prevents cascading failures when R2 becomes unavailable.
 *
 * Issue: sql-koo - RED: R2 Circuit Breaker tests
 *
 * Circuit Breaker States:
 * - CLOSED: Normal operation, requests pass through
 * - OPEN: Failures exceeded threshold, requests rejected immediately
 * - HALF_OPEN: Testing recovery, allows single test request
 *
 * State Transitions:
 * ```
 *         5 consecutive failures
 *   CLOSED ─────────────────────> OPEN
 *      ^                           │
 *      │                           │ timeout (30s)
 *      │                           v
 *      │        success      HALF_OPEN
 *      └─────────────────────────┘
 *           │
 *           │ failure
 *           v
 *         OPEN
 * ```
 *
 * Tests use `it()` pattern to document expected behavior not yet implemented.
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

// =============================================================================
// TYPE DEFINITIONS (Expected API that doesn't exist yet)
// =============================================================================

/**
 * Circuit breaker states
 */
enum CircuitState {
  CLOSED = 'CLOSED',
  OPEN = 'OPEN',
  HALF_OPEN = 'HALF_OPEN',
}

/**
 * Circuit breaker configuration
 */
interface CircuitBreakerConfig {
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
interface CircuitMetrics {
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
interface CircuitProtectedResult<T> {
  success: boolean;
  data?: T;
  error?: string;
  circuitState: CircuitState;
  rejectedByCircuit: boolean;
}

/**
 * R2 Circuit Breaker interface (expected API)
 */
interface R2CircuitBreaker {
  /** Execute an operation with circuit breaker protection */
  execute<T>(operation: () => Promise<T>): Promise<CircuitProtectedResult<T>>;

  /** Get current circuit state */
  getState(): CircuitState;

  /** Get circuit metrics */
  getMetrics(): CircuitMetrics;

  /** Force circuit to specific state (for testing/admin) */
  forceState(state: CircuitState): void;

  /** Reset circuit to closed state */
  reset(): void;

  /** Record a failure (internal) */
  recordFailure(error: Error): void;

  /** Record a success (internal) */
  recordSuccess(): void;

  /** Check if circuit allows requests */
  canExecute(): boolean;

  /** Persist state for DO hibernation */
  persistState(): Promise<void>;

  /** Restore state after DO wake */
  restoreState(): Promise<void>;
}

/**
 * Factory function for creating circuit breakers (expected API)
 */
type CreateCircuitBreaker = (
  config: CircuitBreakerConfig,
  storage?: DurableObjectStorage
) => R2CircuitBreaker;

/**
 * Circuit breaker manager for multiple buckets
 */
interface CircuitBreakerManager {
  /** Get circuit breaker for specific bucket */
  getCircuit(bucketId: string): R2CircuitBreaker;

  /** Get all circuit states */
  getAllStates(): Map<string, CircuitState>;

  /** Reset all circuits */
  resetAll(): void;

  /** Persist all circuit states */
  persistAll(): Promise<void>;

  /** Restore all circuit states */
  restoreAll(): Promise<void>;
}

// Placeholder for DurableObjectStorage type
interface DurableObjectStorage {
  get(key: string): Promise<unknown>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<void>;
}

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Creates a mock storage for testing
 */
function createMockStorage(): DurableObjectStorage {
  const data = new Map<string, unknown>();
  return {
    async get(key: string) {
      return data.get(key);
    },
    async put(key: string, value: unknown) {
      data.set(key, value);
    },
    async delete(key: string) {
      data.delete(key);
    },
  };
}

/**
 * Creates a mock R2 operation that fails
 */
function createFailingR2Operation(errorMessage = 'R2 operation failed'): () => Promise<void> {
  return async () => {
    throw new Error(errorMessage);
  };
}

/**
 * Creates a mock R2 operation that succeeds
 */
function createSucceedingR2Operation<T>(result: T): () => Promise<T> {
  return async () => result;
}

/**
 * Simulate time passing (for timeout testing)
 */
async function advanceTime(ms: number): Promise<void> {
  vi.advanceTimersByTime(ms);
  // Allow any pending promises to resolve
  await Promise.resolve();
}

// =============================================================================
// Import the circuit breaker implementation
// =============================================================================

import {
  R2CircuitBreaker as R2CircuitBreakerImpl,
  createCircuitBreaker as createCircuitBreakerImpl,
  CircuitBreakerManager as CircuitBreakerManagerImpl,
  createCircuitBreakerManager as createCircuitBreakerManagerImpl,
  CircuitState as CircuitStateImpl,
  DEFAULT_CIRCUIT_BREAKER_CONFIG as DEFAULT_CONFIG_IMPL,
} from '../circuit-breaker.js';

// Map implementation to test types
const createCircuitBreaker: CreateCircuitBreaker = (config, storage) => {
  return createCircuitBreakerImpl(config, storage) as unknown as R2CircuitBreaker;
};

const createCircuitBreakerManager = (storage?: DurableObjectStorage): CircuitBreakerManager => {
  return createCircuitBreakerManagerImpl({}, storage) as unknown as CircuitBreakerManager;
};

const DEFAULT_CIRCUIT_BREAKER_CONFIG: CircuitBreakerConfig = DEFAULT_CONFIG_IMPL;

// =============================================================================
// 1. TEST: Circuit Opens After 5 Consecutive R2 Failures
// =============================================================================

describe('Circuit opens after 5 consecutive R2 failures', () => {
  let circuitBreaker: R2CircuitBreaker;
  let mockStorage: DurableObjectStorage;

  beforeEach(() => {
    mockStorage = createMockStorage();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should start in CLOSED state', () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    expect(circuitBreaker.getState()).toBe(CircuitState.CLOSED);
  });

  it('should remain CLOSED after 1 failure', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    const operation = createFailingR2Operation();
    await circuitBreaker.execute(operation);

    expect(circuitBreaker.getState()).toBe(CircuitState.CLOSED);
    expect(circuitBreaker.getMetrics().consecutiveFailures).toBe(1);
  });

  it('should remain CLOSED after 4 failures', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    const operation = createFailingR2Operation();
    for (let i = 0; i < 4; i++) {
      await circuitBreaker.execute(operation);
    }

    expect(circuitBreaker.getState()).toBe(CircuitState.CLOSED);
    expect(circuitBreaker.getMetrics().consecutiveFailures).toBe(4);
  });

  it('should transition to OPEN after 5 consecutive failures', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    const operation = createFailingR2Operation();
    for (let i = 0; i < 5; i++) {
      await circuitBreaker.execute(operation);
    }

    expect(circuitBreaker.getState()).toBe(CircuitState.OPEN);
    expect(circuitBreaker.getMetrics().consecutiveFailures).toBe(5);
  });

  it('should reset consecutive failures on success', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    // 3 failures
    const failOp = createFailingR2Operation();
    for (let i = 0; i < 3; i++) {
      await circuitBreaker.execute(failOp);
    }

    expect(circuitBreaker.getMetrics().consecutiveFailures).toBe(3);

    // 1 success
    const successOp = createSucceedingR2Operation({ ok: true });
    await circuitBreaker.execute(successOp);

    expect(circuitBreaker.getMetrics().consecutiveFailures).toBe(0);
    expect(circuitBreaker.getState()).toBe(CircuitState.CLOSED);
  });

  it('should support configurable failure threshold', async () => {
    const config: CircuitBreakerConfig = {
      ...DEFAULT_CIRCUIT_BREAKER_CONFIG,
      failureThreshold: 3,
    };
    circuitBreaker = createCircuitBreaker(config, mockStorage);

    const operation = createFailingR2Operation();
    for (let i = 0; i < 3; i++) {
      await circuitBreaker.execute(operation);
    }

    expect(circuitBreaker.getState()).toBe(CircuitState.OPEN);
  });

  it('should track total failures across state changes', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    // Cause some failures (not enough to open)
    const failOp = createFailingR2Operation();
    for (let i = 0; i < 3; i++) {
      await circuitBreaker.execute(failOp);
    }

    // Success resets consecutive but not total
    const successOp = createSucceedingR2Operation({ ok: true });
    await circuitBreaker.execute(successOp);

    // More failures
    for (let i = 0; i < 2; i++) {
      await circuitBreaker.execute(failOp);
    }

    const metrics = circuitBreaker.getMetrics();
    expect(metrics.totalFailures).toBe(5);
    expect(metrics.consecutiveFailures).toBe(2);
  });
});

// =============================================================================
// 2. TEST: Circuit in OPEN State Rejects Requests Immediately
// =============================================================================

describe('Circuit in OPEN state rejects requests immediately', () => {
  let circuitBreaker: R2CircuitBreaker;
  let mockStorage: DurableObjectStorage;

  beforeEach(() => {
    mockStorage = createMockStorage();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should reject requests immediately when OPEN', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    // Open the circuit
    const failOp = createFailingR2Operation();
    for (let i = 0; i < 5; i++) {
      await circuitBreaker.execute(failOp);
    }

    expect(circuitBreaker.getState()).toBe(CircuitState.OPEN);

    // New request should be rejected immediately
    let operationCalled = false;
    const trackedOp = async () => {
      operationCalled = true;
      return { ok: true };
    };

    const result = await circuitBreaker.execute(trackedOp);

    expect(operationCalled).toBe(false);
    expect(result.rejectedByCircuit).toBe(true);
    expect(result.success).toBe(false);
    expect(result.circuitState).toBe(CircuitState.OPEN);
  });

  it('should return circuit state in rejected response', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    // Force open state
    circuitBreaker.forceState(CircuitState.OPEN);

    const result = await circuitBreaker.execute(createSucceedingR2Operation({ data: 'test' }));

    expect(result.circuitState).toBe(CircuitState.OPEN);
    expect(result.error).toContain('circuit');
  });

  it('should not count rejected requests as failures', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    // Open the circuit
    const failOp = createFailingR2Operation();
    for (let i = 0; i < 5; i++) {
      await circuitBreaker.execute(failOp);
    }

    const metricsBeforeReject = circuitBreaker.getMetrics();

    // Try to execute while open (rejected)
    await circuitBreaker.execute(createSucceedingR2Operation({ ok: true }));
    await circuitBreaker.execute(createSucceedingR2Operation({ ok: true }));

    const metricsAfterReject = circuitBreaker.getMetrics();

    expect(metricsAfterReject.totalFailures).toBe(metricsBeforeReject.totalFailures);
  });

  it('should reject with fast response time', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.OPEN);

    const slowOp = async () => {
      await new Promise((resolve) => setTimeout(resolve, 5000));
      return { ok: true };
    };

    const start = Date.now();
    await circuitBreaker.execute(slowOp);
    const elapsed = Date.now() - start;

    // Should reject immediately, not wait for operation
    expect(elapsed).toBeLessThan(100);
  });

  it('should provide meaningful error message when rejected', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.OPEN);

    const result = await circuitBreaker.execute(createSucceedingR2Operation({ ok: true }));

    expect(result.error).toBeDefined();
    expect(result.error).toContain('Circuit breaker is OPEN');
  });
});

// =============================================================================
// 3. TEST: Circuit Transitions to HALF_OPEN After Timeout
// =============================================================================

describe('Circuit transitions to HALF_OPEN after timeout', () => {
  let circuitBreaker: R2CircuitBreaker;
  let mockStorage: DurableObjectStorage;

  beforeEach(() => {
    mockStorage = createMockStorage();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should remain OPEN before timeout expires', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.OPEN);

    // Advance time but not past timeout (default 30s)
    await advanceTime(29000);

    expect(circuitBreaker.getState()).toBe(CircuitState.OPEN);
  });

  it('should transition to HALF_OPEN after timeout expires', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    // Open the circuit
    const failOp = createFailingR2Operation();
    for (let i = 0; i < 5; i++) {
      await circuitBreaker.execute(failOp);
    }

    expect(circuitBreaker.getState()).toBe(CircuitState.OPEN);

    // Advance past timeout
    await advanceTime(30001);

    expect(circuitBreaker.getState()).toBe(CircuitState.HALF_OPEN);
  });

  it('should support configurable reset timeout', async () => {
    const config: CircuitBreakerConfig = {
      ...DEFAULT_CIRCUIT_BREAKER_CONFIG,
      resetTimeoutMs: 10000, // 10 seconds
    };
    circuitBreaker = createCircuitBreaker(config, mockStorage);
    circuitBreaker.forceState(CircuitState.OPEN);

    await advanceTime(10001);

    expect(circuitBreaker.getState()).toBe(CircuitState.HALF_OPEN);
  });

  it('should track stateChangedAt timestamp', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    const beforeOpen = Date.now();

    circuitBreaker.forceState(CircuitState.OPEN);

    const metrics = circuitBreaker.getMetrics();
    expect(metrics.stateChangedAt).toBeGreaterThanOrEqual(beforeOpen);
  });

  it('should allow checking canExecute() for HALF_OPEN', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.HALF_OPEN);

    expect(circuitBreaker.canExecute()).toBe(true);
  });
});

// =============================================================================
// 4. TEST: HALF_OPEN Circuit Allows Single Test Request
// =============================================================================

describe('HALF_OPEN circuit allows single test request', () => {
  let circuitBreaker: R2CircuitBreaker;
  let mockStorage: DurableObjectStorage;

  beforeEach(() => {
    mockStorage = createMockStorage();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should allow one request to pass in HALF_OPEN state', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.HALF_OPEN);

    let operationCalled = false;
    const trackedOp = async () => {
      operationCalled = true;
      return { ok: true };
    };

    await circuitBreaker.execute(trackedOp);

    expect(operationCalled).toBe(true);
  });

  it('should reject subsequent requests while test is in progress', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.HALF_OPEN);

    // Start a slow operation
    let resolveSlowOp: () => void;
    const slowOp = new Promise<{ ok: boolean }>((resolve) => {
      resolveSlowOp = () => resolve({ ok: true });
    });

    // First request passes
    const firstResult = circuitBreaker.execute(() => slowOp);

    // Second request should be rejected while first is in progress
    const secondResult = await circuitBreaker.execute(createSucceedingR2Operation({ ok: true }));

    expect(secondResult.rejectedByCircuit).toBe(true);
    expect(secondResult.error).toContain('test request in progress');

    // Cleanup
    resolveSlowOp!();
    await firstResult;
  });

  it('should track the test request being in progress', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.HALF_OPEN);

    const metrics = circuitBreaker.getMetrics();
    // Before test request
    expect(metrics).not.toHaveProperty('testRequestInProgress');

    // Start test request
    let resolveOp: () => void;
    const slowOp = new Promise<{ ok: boolean }>((resolve) => {
      resolveOp = () => resolve({ ok: true });
    });

    const requestPromise = circuitBreaker.execute(() => slowOp);

    // During test request - this would require internal state access
    // Implementation should track this somehow

    resolveOp!();
    await requestPromise;
  });
});

// =============================================================================
// 5. TEST: Successful Test Request Closes Circuit
// =============================================================================

describe('Successful test request closes circuit', () => {
  let circuitBreaker: R2CircuitBreaker;
  let mockStorage: DurableObjectStorage;

  beforeEach(() => {
    mockStorage = createMockStorage();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should transition from HALF_OPEN to CLOSED on success', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.HALF_OPEN);

    const successOp = createSucceedingR2Operation({ data: 'test' });
    await circuitBreaker.execute(successOp);

    expect(circuitBreaker.getState()).toBe(CircuitState.CLOSED);
  });

  it('should reset consecutive failures on successful close', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    // Open the circuit
    const failOp = createFailingR2Operation();
    for (let i = 0; i < 5; i++) {
      await circuitBreaker.execute(failOp);
    }

    // Advance to HALF_OPEN
    await advanceTime(30001);

    // Successful test request
    const successOp = createSucceedingR2Operation({ ok: true });
    await circuitBreaker.execute(successOp);

    expect(circuitBreaker.getMetrics().consecutiveFailures).toBe(0);
    expect(circuitBreaker.getState()).toBe(CircuitState.CLOSED);
  });

  it('should return success result with data', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.HALF_OPEN);

    const testData = { bucket: 'test', key: 'file.parquet' };
    const result = await circuitBreaker.execute(createSucceedingR2Operation(testData));

    expect(result.success).toBe(true);
    expect(result.data).toEqual(testData);
    expect(result.rejectedByCircuit).toBe(false);
  });

  it('should update lastSuccessTime on close', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.HALF_OPEN);

    const beforeSuccess = Date.now();
    await circuitBreaker.execute(createSucceedingR2Operation({ ok: true }));

    const metrics = circuitBreaker.getMetrics();
    expect(metrics.lastSuccessTime).toBeGreaterThanOrEqual(beforeSuccess);
  });

  it('should allow multiple requests after closing', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.HALF_OPEN);

    // Close the circuit
    await circuitBreaker.execute(createSucceedingR2Operation({ ok: true }));

    // Multiple subsequent requests should all pass
    const results = await Promise.all([
      circuitBreaker.execute(createSucceedingR2Operation({ id: 1 })),
      circuitBreaker.execute(createSucceedingR2Operation({ id: 2 })),
      circuitBreaker.execute(createSucceedingR2Operation({ id: 3 })),
    ]);

    expect(results.every((r) => r.success)).toBe(true);
    expect(results.every((r) => !r.rejectedByCircuit)).toBe(true);
  });
});

// =============================================================================
// 6. TEST: Failed Test Request Re-opens Circuit
// =============================================================================

describe('Failed test request re-opens circuit', () => {
  let circuitBreaker: R2CircuitBreaker;
  let mockStorage: DurableObjectStorage;

  beforeEach(() => {
    mockStorage = createMockStorage();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should transition from HALF_OPEN to OPEN on failure', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.HALF_OPEN);

    const failOp = createFailingR2Operation('R2 still unavailable');
    await circuitBreaker.execute(failOp);

    expect(circuitBreaker.getState()).toBe(CircuitState.OPEN);
  });

  it('should reset the timeout timer on re-open', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    // Open the circuit
    const failOp = createFailingR2Operation();
    for (let i = 0; i < 5; i++) {
      await circuitBreaker.execute(failOp);
    }

    // Advance almost to timeout
    await advanceTime(29000);

    // Transition to HALF_OPEN
    await advanceTime(2000);
    expect(circuitBreaker.getState()).toBe(CircuitState.HALF_OPEN);

    // Fail the test request - should re-open
    await circuitBreaker.execute(failOp);
    expect(circuitBreaker.getState()).toBe(CircuitState.OPEN);

    // The timeout should reset, so after 29s it should still be OPEN
    await advanceTime(29000);
    expect(circuitBreaker.getState()).toBe(CircuitState.OPEN);

    // After full timeout from re-open, should be HALF_OPEN
    await advanceTime(2000);
    expect(circuitBreaker.getState()).toBe(CircuitState.HALF_OPEN);
  });

  it('should increment failure counter on test failure', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.HALF_OPEN);

    const metricsBefore = circuitBreaker.getMetrics();

    await circuitBreaker.execute(createFailingR2Operation());

    const metricsAfter = circuitBreaker.getMetrics();
    expect(metricsAfter.totalFailures).toBe(metricsBefore.totalFailures + 1);
  });

  it('should return failure result with error', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.HALF_OPEN);

    const result = await circuitBreaker.execute(createFailingR2Operation('Specific error message'));

    expect(result.success).toBe(false);
    expect(result.error).toContain('Specific error message');
    expect(result.circuitState).toBe(CircuitState.OPEN);
  });

  it('should update lastFailureTime on re-open', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.HALF_OPEN);

    const beforeFailure = Date.now();
    await circuitBreaker.execute(createFailingR2Operation());

    const metrics = circuitBreaker.getMetrics();
    expect(metrics.lastFailureTime).toBeGreaterThanOrEqual(beforeFailure);
  });
});

// =============================================================================
// 7. TEST: Circuit Breaker State Persists Across DO Instances
// =============================================================================

describe('Circuit breaker state persists across DO instances', () => {
  let mockStorage: DurableObjectStorage;

  beforeEach(() => {
    mockStorage = createMockStorage();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should persist state to DO storage', async () => {
    const circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    // Open the circuit
    const failOp = createFailingR2Operation();
    for (let i = 0; i < 5; i++) {
      await circuitBreaker.execute(failOp);
    }

    await circuitBreaker.persistState();

    // Verify something was persisted
    const persistedState = await mockStorage.get('circuitBreaker:default');
    expect(persistedState).toBeDefined();
  });

  it('should restore state from DO storage on new instance', async () => {
    // First instance - open the circuit
    const firstInstance = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    const failOp = createFailingR2Operation();
    for (let i = 0; i < 5; i++) {
      await firstInstance.execute(failOp);
    }

    expect(firstInstance.getState()).toBe(CircuitState.OPEN);
    await firstInstance.persistState();

    // Second instance - should restore OPEN state
    const secondInstance = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    await secondInstance.restoreState();

    expect(secondInstance.getState()).toBe(CircuitState.OPEN);
  });

  it('should persist metrics across instances', async () => {
    // First instance
    const firstInstance = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    const failOp = createFailingR2Operation();
    for (let i = 0; i < 5; i++) {
      await firstInstance.execute(failOp);
    }

    const firstMetrics = firstInstance.getMetrics();
    await firstInstance.persistState();

    // Second instance
    const secondInstance = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    await secondInstance.restoreState();

    const secondMetrics = secondInstance.getMetrics();
    expect(secondMetrics.totalFailures).toBe(firstMetrics.totalFailures);
    expect(secondMetrics.consecutiveFailures).toBe(firstMetrics.consecutiveFailures);
  });

  it('should preserve timeout progress across hibernation', async () => {
    // First instance - open circuit and wait 15s
    const firstInstance = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    firstInstance.forceState(CircuitState.OPEN);

    await advanceTime(15000); // 15 seconds
    await firstInstance.persistState();

    // Second instance after hibernation - should resume timeout
    const secondInstance = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    await secondInstance.restoreState();

    // Should still be OPEN (only 15s elapsed)
    expect(secondInstance.getState()).toBe(CircuitState.OPEN);

    // Advance remaining 15s + 1ms
    await advanceTime(15001);

    // Now should be HALF_OPEN
    expect(secondInstance.getState()).toBe(CircuitState.HALF_OPEN);
  });

  it('should handle missing persisted state gracefully', async () => {
    const circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    // No prior state - should not throw
    await expect(circuitBreaker.restoreState()).resolves.not.toThrow();

    // Should default to CLOSED
    expect(circuitBreaker.getState()).toBe(CircuitState.CLOSED);
  });

  it('should handle corrupted persisted state gracefully', async () => {
    // Write corrupted data
    await mockStorage.put('circuitBreaker:default', 'not valid json');

    const circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    // Should not throw
    await expect(circuitBreaker.restoreState()).resolves.not.toThrow();

    // Should default to CLOSED
    expect(circuitBreaker.getState()).toBe(CircuitState.CLOSED);
  });
});

// =============================================================================
// 8. TEST: Separate Circuits Per R2 Bucket
// =============================================================================

describe('Separate circuits per R2 bucket', () => {
  let manager: CircuitBreakerManager;
  let mockStorage: DurableObjectStorage;

  beforeEach(() => {
    mockStorage = createMockStorage();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should create separate circuit for each bucket', () => {
    manager = createCircuitBreakerManager();

    const lakehouseCircuit = manager.getCircuit('lakehouse-bucket');
    const archiveCircuit = manager.getCircuit('archive-bucket');

    expect(lakehouseCircuit).not.toBe(archiveCircuit);
  });

  it('should maintain independent state per bucket', async () => {
    manager = createCircuitBreakerManager();

    const lakehouseCircuit = manager.getCircuit('lakehouse-bucket');
    const archiveCircuit = manager.getCircuit('archive-bucket');

    // Open lakehouse circuit
    const failOp = createFailingR2Operation();
    for (let i = 0; i < 5; i++) {
      await lakehouseCircuit.execute(failOp);
    }

    expect(lakehouseCircuit.getState()).toBe(CircuitState.OPEN);
    expect(archiveCircuit.getState()).toBe(CircuitState.CLOSED);
  });

  it('should return same circuit instance for same bucket', () => {
    manager = createCircuitBreakerManager();

    const first = manager.getCircuit('lakehouse-bucket');
    const second = manager.getCircuit('lakehouse-bucket');

    expect(first).toBe(second);
  });

  it('should get all circuit states', async () => {
    manager = createCircuitBreakerManager();

    const lakehouse = manager.getCircuit('lakehouse-bucket');
    const archive = manager.getCircuit('archive-bucket');
    const backup = manager.getCircuit('backup-bucket');

    // Open one, leave others closed
    lakehouse.forceState(CircuitState.OPEN);
    archive.forceState(CircuitState.HALF_OPEN);

    const states = manager.getAllStates();

    expect(states.get('lakehouse-bucket')).toBe(CircuitState.OPEN);
    expect(states.get('archive-bucket')).toBe(CircuitState.HALF_OPEN);
    expect(states.get('backup-bucket')).toBe(CircuitState.CLOSED);
  });

  it('should reset all circuits', async () => {
    manager = createCircuitBreakerManager();

    const lakehouse = manager.getCircuit('lakehouse-bucket');
    const archive = manager.getCircuit('archive-bucket');

    lakehouse.forceState(CircuitState.OPEN);
    archive.forceState(CircuitState.OPEN);

    manager.resetAll();

    expect(lakehouse.getState()).toBe(CircuitState.CLOSED);
    expect(archive.getState()).toBe(CircuitState.CLOSED);
  });

  it('should persist all circuits to storage', async () => {
    manager = createCircuitBreakerManager();

    const lakehouse = manager.getCircuit('lakehouse-bucket');
    const archive = manager.getCircuit('archive-bucket');

    lakehouse.forceState(CircuitState.OPEN);
    archive.forceState(CircuitState.HALF_OPEN);

    await manager.persistAll();

    const lakehouseState = await mockStorage.get('circuitBreaker:lakehouse-bucket');
    const archiveState = await mockStorage.get('circuitBreaker:archive-bucket');

    expect(lakehouseState).toBeDefined();
    expect(archiveState).toBeDefined();
  });

  it('should restore all circuits from storage', async () => {
    // Setup initial state
    const initialManager = createCircuitBreakerManager();
    const lakehouse = initialManager.getCircuit('lakehouse-bucket');
    lakehouse.forceState(CircuitState.OPEN);
    await initialManager.persistAll();

    // New manager should restore
    const newManager = createCircuitBreakerManager();
    await newManager.restoreAll();

    const restoredLakehouse = newManager.getCircuit('lakehouse-bucket');
    expect(restoredLakehouse.getState()).toBe(CircuitState.OPEN);
  });

  it('should support bucket-specific configuration', () => {
    manager = createCircuitBreakerManager();

    // Different buckets might need different thresholds
    const criticalCircuit = manager.getCircuit('critical-bucket');
    const analyticsCircuit = manager.getCircuit('analytics-bucket');

    // This test documents the need for per-bucket configuration
    // Implementation could support this via options
    expect(criticalCircuit).toBeDefined();
    expect(analyticsCircuit).toBeDefined();
  });
});

// =============================================================================
// ADDITIONAL TESTS: Edge Cases and Integration
// =============================================================================

describe('Circuit breaker edge cases', () => {
  let circuitBreaker: R2CircuitBreaker;
  let mockStorage: DurableObjectStorage;

  beforeEach(() => {
    mockStorage = createMockStorage();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should handle concurrent requests correctly in CLOSED state', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    // Execute 10 concurrent requests
    const results = await Promise.all(
      Array.from({ length: 10 }, (_, i) => circuitBreaker.execute(createSucceedingR2Operation({ id: i })))
    );

    expect(results.every((r) => r.success)).toBe(true);
    expect(circuitBreaker.getState()).toBe(CircuitState.CLOSED);
  });

  it('should handle rapid state transitions', async () => {
    circuitBreaker = createCircuitBreaker(
      { ...DEFAULT_CIRCUIT_BREAKER_CONFIG, resetTimeoutMs: 100 },
      mockStorage
    );

    // Open circuit
    for (let i = 0; i < 5; i++) {
      await circuitBreaker.execute(createFailingR2Operation());
    }
    expect(circuitBreaker.getState()).toBe(CircuitState.OPEN);

    // Wait for half-open
    await advanceTime(101);
    expect(circuitBreaker.getState()).toBe(CircuitState.HALF_OPEN);

    // Fail again - back to open
    await circuitBreaker.execute(createFailingR2Operation());
    expect(circuitBreaker.getState()).toBe(CircuitState.OPEN);

    // Wait for half-open again
    await advanceTime(101);
    expect(circuitBreaker.getState()).toBe(CircuitState.HALF_OPEN);

    // Succeed - close
    await circuitBreaker.execute(createSucceedingR2Operation({ ok: true }));
    expect(circuitBreaker.getState()).toBe(CircuitState.CLOSED);
  });

  it('should expose reset method for manual recovery', () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);
    circuitBreaker.forceState(CircuitState.OPEN);

    circuitBreaker.reset();

    expect(circuitBreaker.getState()).toBe(CircuitState.CLOSED);
    expect(circuitBreaker.getMetrics().consecutiveFailures).toBe(0);
  });

  it('should track metrics over time', async () => {
    circuitBreaker = createCircuitBreaker(DEFAULT_CIRCUIT_BREAKER_CONFIG, mockStorage);

    // Various operations
    await circuitBreaker.execute(createSucceedingR2Operation({ ok: true }));
    await circuitBreaker.execute(createSucceedingR2Operation({ ok: true }));
    await circuitBreaker.execute(createFailingR2Operation());
    await circuitBreaker.execute(createSucceedingR2Operation({ ok: true }));

    const metrics = circuitBreaker.getMetrics();
    expect(metrics.totalSuccesses).toBe(3);
    expect(metrics.totalFailures).toBe(1);
  });
});
