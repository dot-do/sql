/**
 * Transaction Timeout Enforcement Tests
 *
 * Tests for timeout handling in DoSQL transactions.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  createTimeoutEnforcer,
  checkTransactionTimeout,
  type TransactionTimeoutEnforcer,
} from '../timeout.js';
import {
  type TransactionContext,
  type TransactionId,
  type DurableObjectState,
  TransactionState,
  TransactionMode,
  IsolationLevel,
  TransactionError,
  TransactionErrorCode,
  createTransactionLog,
  createSavepointStack,
  DEFAULT_TIMEOUT_CONFIG,
  createTransactionId,
} from '../types.js';

// =============================================================================
// Mock Helpers
// =============================================================================

/**
 * Create a mock DurableObjectState for testing
 */
function createMockDoState(): DurableObjectState {
  return {
    storage: {
      setAlarm: vi.fn().mockResolvedValue(undefined),
      deleteAlarm: vi.fn().mockResolvedValue(undefined),
      getAlarm: vi.fn().mockResolvedValue(null),
    },
  };
}

// =============================================================================
// Test Helpers
// =============================================================================

function createTestContext(
  txnId: TransactionId = createTransactionId('test-txn-1'),
  overrides: Partial<TransactionContext> = {}
): TransactionContext {
  return {
    txnId,
    state: TransactionState.ACTIVE,
    mode: TransactionMode.DEFERRED,
    isolationLevel: IsolationLevel.SERIALIZABLE,
    savepoints: createSavepointStack(),
    log: createTransactionLog(txnId),
    locks: [],
    startedAt: Date.now(),
    readOnly: false,
    autoCommit: false,
    ...overrides,
  };
}

// =============================================================================
// Timeout Enforcer Creation Tests
// =============================================================================

describe('createTimeoutEnforcer', () => {
  it('should create an enforcer with default configuration', () => {
    const enforcer = createTimeoutEnforcer();

    expect(enforcer).toBeDefined();
    expect(enforcer.getTimeoutConfig()).toEqual(DEFAULT_TIMEOUT_CONFIG);
  });

  it('should merge partial config with defaults', () => {
    const enforcer = createTimeoutEnforcer({
      config: { defaultTimeoutMs: 60000 },
    });

    const config = enforcer.getTimeoutConfig();
    expect(config.defaultTimeoutMs).toBe(60000);
    expect(config.maxTimeoutMs).toBe(DEFAULT_TIMEOUT_CONFIG.maxTimeoutMs);
  });

  it('should indicate no alarm support by default', () => {
    const enforcer = createTimeoutEnforcer();

    expect(enforcer.hasAlarmSupport()).toBe(false);
  });

  it('should indicate alarm support when doState provided', () => {
    const mockDoState = createMockDoState();

    const enforcer = createTimeoutEnforcer({
      doState: mockDoState,
    });

    expect(enforcer.hasAlarmSupport()).toBe(true);
  });

  it('should use custom I/O timeout', () => {
    const enforcer = createTimeoutEnforcer({
      ioTimeoutMs: 5000,
    });

    expect(enforcer.getIoTimeoutMs()).toBe(5000);
  });

  it('should default I/O timeout to 30000ms', () => {
    const enforcer = createTimeoutEnforcer();

    expect(enforcer.getIoTimeoutMs()).toBe(30000);
  });
});

// =============================================================================
// Transaction Registration Tests
// =============================================================================

describe('Transaction Registration', () => {
  let enforcer: TransactionTimeoutEnforcer;

  beforeEach(() => {
    enforcer = createTimeoutEnforcer();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should register a transaction', () => {
    const context = createTestContext();

    enforcer.registerTransaction(context.txnId, 30000, context);

    expect(enforcer.getRemainingTime(context.txnId)).toBeGreaterThan(0);
  });

  it('should set expiry on context', () => {
    const context = createTestContext();

    enforcer.registerTransaction(context.txnId, 30000, context);

    expect(context.expiresAt).toBeDefined();
    expect(context.expiresAt).toBeGreaterThan(Date.now());
  });

  it('should cap timeout at maxTimeoutMs', () => {
    const maxMs = DEFAULT_TIMEOUT_CONFIG.maxTimeoutMs;
    const context = createTestContext();

    enforcer.registerTransaction(context.txnId, maxMs + 100000, context);

    // Remaining time should be capped
    expect(enforcer.getRemainingTime(context.txnId)).toBeLessThanOrEqual(maxMs);
  });

  it('should unregister a transaction', () => {
    const context = createTestContext();

    enforcer.registerTransaction(context.txnId, 30000, context);
    enforcer.unregisterTransaction(context.txnId);

    expect(enforcer.getRemainingTime(context.txnId)).toBe(0);
  });
});

// =============================================================================
// Timeout Check Tests
// =============================================================================

describe('Timeout Checks', () => {
  let enforcer: TransactionTimeoutEnforcer;

  beforeEach(() => {
    enforcer = createTimeoutEnforcer();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should report not timed out for valid transaction', () => {
    const context = createTestContext();

    enforcer.registerTransaction(context.txnId, 30000, context);

    expect(enforcer.isTimedOut(context.txnId)).toBe(false);
  });

  it('should report timed out after expiry', async () => {
    const context = createTestContext();

    // Register with timeout of 0ms, which should immediately expire
    enforcer.registerTransaction(context.txnId, 0, context);

    // With 0 timeout, expiry is set to now, so any check after should show timed out
    // Add small delay to ensure clock has moved
    await new Promise((resolve) => setTimeout(resolve, 5));

    expect(enforcer.isTimedOut(context.txnId)).toBe(true);
  });

  it('should return 0 remaining time for unknown transaction', () => {
    expect(enforcer.getRemainingTime(createTransactionId('unknown'))).toBe(0);
  });

  it('should return 0 remaining time after timeout', () => {
    vi.useFakeTimers();
    const context = createTestContext();

    enforcer.registerTransaction(context.txnId, 100, context);
    vi.advanceTimersByTime(150);

    expect(enforcer.getRemainingTime(context.txnId)).toBe(0);
  });
});

// =============================================================================
// Extension Tests
// =============================================================================

describe('Timeout Extension', () => {
  let enforcer: TransactionTimeoutEnforcer;

  beforeEach(() => {
    enforcer = createTimeoutEnforcer({ maxExtensions: 3 });
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should grant extension within limit', () => {
    const context = createTestContext();

    enforcer.registerTransaction(context.txnId, 1000, context);
    vi.advanceTimersByTime(500);

    const granted = enforcer.requestExtension(context.txnId, 500);

    expect(granted).toBe(true);
    expect(enforcer.getRemainingTime(context.txnId)).toBeGreaterThan(500);
  });

  it('should deny extension after max extensions', () => {
    const context = createTestContext();

    enforcer.registerTransaction(context.txnId, 1000, context);

    // Use all extensions
    expect(enforcer.requestExtension(context.txnId, 100)).toBe(true);
    expect(enforcer.requestExtension(context.txnId, 100)).toBe(true);
    expect(enforcer.requestExtension(context.txnId, 100)).toBe(true);

    // Should be denied now
    expect(enforcer.requestExtension(context.txnId, 100)).toBe(false);
  });

  it('should deny extension for unknown transaction', () => {
    expect(enforcer.requestExtension(createTransactionId('unknown'), 100)).toBe(false);
  });

  it('should not extend beyond maxTimeoutMs', () => {
    const maxMs = DEFAULT_TIMEOUT_CONFIG.maxTimeoutMs;
    const context = createTestContext();

    enforcer.registerTransaction(context.txnId, maxMs - 1000, context);

    // Try to extend way beyond max
    enforcer.requestExtension(context.txnId, 100000);

    // Should still be capped at maxTimeoutMs from start
    const remaining = enforcer.getRemainingTime(context.txnId);
    expect(remaining).toBeLessThanOrEqual(maxMs);
  });
});

// =============================================================================
// Callback Tests
// =============================================================================

describe('Timeout Callbacks', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('should call onTimeoutWarning callback', async () => {
    vi.useFakeTimers();
    const warningCallback = vi.fn();

    const enforcer = createTimeoutEnforcer({
      config: {
        ...DEFAULT_TIMEOUT_CONFIG,
        gracePeriodMs: 100,
      },
      onTimeoutWarning: warningCallback,
    });

    const context = createTestContext();
    enforcer.registerTransaction(context.txnId, 200, context);

    // Advance past warning threshold (timeout - gracePeriod)
    vi.advanceTimersByTime(110);

    expect(warningCallback).toHaveBeenCalledWith(context.txnId, expect.any(Number));
  });

  it('should call onTimeout callback', async () => {
    vi.useFakeTimers();
    const timeoutCallback = vi.fn();

    const enforcer = createTimeoutEnforcer({
      onTimeout: timeoutCallback,
    });

    const context = createTestContext();
    enforcer.registerTransaction(context.txnId, 100, context);

    vi.advanceTimersByTime(150);

    expect(timeoutCallback).toHaveBeenCalledWith(context.txnId);
  });

  it('should call rollback function on timeout', async () => {
    vi.useFakeTimers();
    const rollbackFn = vi.fn().mockResolvedValue(undefined);

    const enforcer = createTimeoutEnforcer();
    enforcer.setRollbackFn(rollbackFn);

    const context = createTestContext();
    enforcer.registerTransaction(context.txnId, 100, context);

    vi.advanceTimersByTime(150);

    // Wait for any pending promises
    await vi.runAllTimersAsync();

    expect(rollbackFn).toHaveBeenCalledWith(context.txnId);
  });
});

// =============================================================================
// I/O Timeout Tests
// =============================================================================

describe('I/O Timeout', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('should execute operation within timeout', async () => {
    const enforcer = createTimeoutEnforcer({ ioTimeoutMs: 1000 });

    const result = await enforcer.executeWithIoTimeout(async () => {
      return 'success';
    });

    expect(result).toBe('success');
  });

  it('should throw on I/O timeout', async () => {
    vi.useFakeTimers();
    const enforcer = createTimeoutEnforcer({ ioTimeoutMs: 100 });

    const promise = enforcer.executeWithIoTimeout(async () => {
      // Simulate a long operation
      await new Promise((resolve) => setTimeout(resolve, 200));
      return 'never';
    });

    vi.advanceTimersByTime(150);

    await expect(promise).rejects.toMatchObject({
      code: TransactionErrorCode.IO_TIMEOUT,
    });
  });

  it('should propagate operation errors', async () => {
    const enforcer = createTimeoutEnforcer();

    await expect(
      enforcer.executeWithIoTimeout(async () => {
        throw new Error('Operation failed');
      })
    ).rejects.toThrow('Operation failed');
  });
});

// =============================================================================
// Operation Tracking Tests
// =============================================================================

describe('Operation Tracking', () => {
  it('should track operations when enabled', () => {
    const longRunningCallback = vi.fn();

    const enforcer = createTimeoutEnforcer({
      trackQueries: true,
      config: {
        ...DEFAULT_TIMEOUT_CONFIG,
        warningThresholdMs: 100,
      },
      onLongRunningTransaction: longRunningCallback,
    });

    const context = createTestContext();
    enforcer.registerTransaction(context.txnId, 30000, context);

    // Track some operations
    enforcer.trackOperation(context.txnId, {
      op: 'INSERT',
      table: 'users',
      timestamp: Date.now(),
      sequence: 0,
    });

    enforcer.trackOperation(context.txnId, {
      op: 'UPDATE',
      table: 'users',
      timestamp: Date.now(),
      sequence: 1,
    });

    // Operation tracking should not throw
    enforcer.unregisterTransaction(context.txnId);
  });

  it('should ignore operations for unknown transactions', () => {
    const enforcer = createTimeoutEnforcer({ trackQueries: true });

    // Should not throw
    expect(() => {
      enforcer.trackOperation(createTransactionId('unknown'), {
        op: 'INSERT',
        table: 'users',
        timestamp: Date.now(),
        sequence: 0,
      });
    }).not.toThrow();
  });
});

// =============================================================================
// Held Locks Getter Tests
// =============================================================================

describe('Held Locks Getter', () => {
  it('should set held locks getter', () => {
    const enforcer = createTimeoutEnforcer();
    const context = createTestContext();

    enforcer.registerTransaction(context.txnId, 30000, context);

    const getter = vi.fn().mockReturnValue([
      { txnId: context.txnId, lockType: 'EXCLUSIVE', resource: 'users', acquiredAt: Date.now() },
    ]);

    // Should not throw
    expect(() => {
      enforcer.setHeldLocksGetter(context.txnId, getter);
    }).not.toThrow();
  });

  it('should ignore getter for unknown transaction', () => {
    const enforcer = createTimeoutEnforcer();

    // Should not throw
    expect(() => {
      enforcer.setHeldLocksGetter(createTransactionId('unknown'), () => []);
    }).not.toThrow();
  });
});

// =============================================================================
// Alarm Handling Tests
// =============================================================================

describe('Alarm Handling', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('should handle alarm for timed-out transactions', async () => {
    vi.useFakeTimers();
    const rollbackFn = vi.fn().mockResolvedValue(undefined);

    const enforcer = createTimeoutEnforcer();
    enforcer.setRollbackFn(rollbackFn);

    const context = createTestContext();
    enforcer.registerTransaction(context.txnId, 100, context);

    vi.advanceTimersByTime(150);

    await enforcer.handleAlarm();

    expect(rollbackFn).toHaveBeenCalled();
  });

  it('should not fail if no transactions are timed out', async () => {
    const enforcer = createTimeoutEnforcer();

    // Should not throw
    await expect(enforcer.handleAlarm()).resolves.not.toThrow();
  });
});

// =============================================================================
// checkTransactionTimeout Tests
// =============================================================================

describe('checkTransactionTimeout', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('should not throw for null context', () => {
    expect(() => checkTransactionTimeout(null)).not.toThrow();
  });

  it('should not throw for valid transaction', () => {
    const enforcer = createTimeoutEnforcer();
    const context = createTestContext();

    enforcer.registerTransaction(context.txnId, 30000, context);

    expect(() => checkTransactionTimeout(context, enforcer)).not.toThrow();
  });

  it('should throw for timed-out transaction via enforcer', () => {
    vi.useFakeTimers();
    const enforcer = createTimeoutEnforcer();
    const context = createTestContext();

    enforcer.registerTransaction(context.txnId, 100, context);
    vi.advanceTimersByTime(150);

    expect(() => checkTransactionTimeout(context, enforcer)).toThrow(TransactionError);
    expect(() => checkTransactionTimeout(context, enforcer)).toThrow(/timed out/);
  });

  it('should throw for timed-out transaction via context expiry', () => {
    const context = createTestContext();
    context.expiresAt = Date.now() - 1000; // Expired

    expect(() => checkTransactionTimeout(context)).toThrow(TransactionError);
  });

  it('should not throw if context has no expiry', () => {
    const context = createTestContext();
    // No expiresAt set

    expect(() => checkTransactionTimeout(context)).not.toThrow();
  });
});

// =============================================================================
// Long-Running Transaction Logging Tests
// =============================================================================

describe('Long-Running Transaction Logging', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('should emit long-running transaction log', async () => {
    vi.useFakeTimers();
    const longRunningCallback = vi.fn();

    const enforcer = createTimeoutEnforcer({
      config: {
        ...DEFAULT_TIMEOUT_CONFIG,
        warningThresholdMs: 100,
      },
      onLongRunningTransaction: longRunningCallback,
      trackQueries: true,
    });

    const context = createTestContext();
    enforcer.registerTransaction(context.txnId, 30000, context);

    // Advance past warning threshold
    vi.advanceTimersByTime(150);

    expect(longRunningCallback).toHaveBeenCalledWith(
      expect.objectContaining({
        txnId: context.txnId,
        readOnly: false,
        isolationLevel: IsolationLevel.SERIALIZABLE,
      })
    );
  });

  it('should only emit long-running log once', async () => {
    vi.useFakeTimers();
    const longRunningCallback = vi.fn();

    const enforcer = createTimeoutEnforcer({
      config: {
        ...DEFAULT_TIMEOUT_CONFIG,
        warningThresholdMs: 100,
      },
      onLongRunningTransaction: longRunningCallback,
    });

    const context = createTestContext();
    enforcer.registerTransaction(context.txnId, 30000, context);

    // Advance multiple times past threshold
    vi.advanceTimersByTime(150);
    vi.advanceTimersByTime(100);
    vi.advanceTimersByTime(100);

    expect(longRunningCallback).toHaveBeenCalledTimes(1);
  });
});
