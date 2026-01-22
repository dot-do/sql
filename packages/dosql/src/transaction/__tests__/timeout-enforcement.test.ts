/**
 * Transaction Timeout Enforcement Tests - RED Phase TDD
 *
 * These tests document the MISSING transaction timeout enforcement behavior.
 * Currently, transaction timeouts are calculated client-side (expiresAt) but not
 * enforced server-side, leading to resource leaks and potential starvation.
 *
 * Issue: sql-zhy.19
 *
 * Tests document:
 * 1. Server-side timeout enforcement - Transaction rollback after timeout expires
 * 2. Durable Object alarm integration - Use DO alarms to trigger timeout
 * 3. Grace period handling - Warning before hard timeout, extension requests
 * 4. Timeout configuration - TransactionTimeoutConfig interface
 * 5. Long-running transaction logging - Log transactions exceeding thresholds
 * 6. Timeout behavior during blocking operations - Handle timeout during locks/I/O
 *
 * NOTE: Tests using `it.fails()` document missing features that SHOULD exist
 * Tests using `it()` verify existing behavior or expected failures
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  createTransactionManager,
  executeInTransaction,
  type TransactionManagerOptions,
} from '../manager.js';
import {
  createLockManager,
  type LockManager,
} from '../isolation.js';
import {
  TransactionState,
  TransactionMode,
  IsolationLevel,
  LockType,
  TransactionError,
  TransactionErrorCode,
} from '../types.js';

// =============================================================================
// DOCUMENTED GAPS - Features that should be implemented
// =============================================================================
//
// 1. Server-side timeout enforcement:
//    - TransactionTimeoutEnforcer class
//    - setAlarm() integration for DO environment
//    - Automatic rollback on timeout
//    - Transaction state cleanup
//
// 2. Timeout configuration:
//    interface TransactionTimeoutConfig {
//      defaultTimeoutMs: number;    // Default transaction timeout
//      maxTimeoutMs: number;        // Maximum allowed timeout
//      gracePeriodMs: number;       // Warning period before hard timeout
//      warningThresholdMs: number;  // Log warning after this duration
//    }
//
// 3. Grace period handling:
//    - Emit warning event before hard timeout
//    - Allow client to request extension
//    - Track extension count/limit
//
// 4. Long-running transaction logging:
//    - Log transactions exceeding warningThresholdMs
//    - Include query details, lock information
//    - Structured logging for debugging
//
// 5. Timeout during blocking operations:
//    - Interrupt lock acquisition on timeout
//    - Handle I/O timeout separately from transaction timeout
//    - Cleanup partial state on timeout
//
// 6. DO alarm integration:
//    - Register alarm when transaction begins
//    - Cancel alarm on commit/rollback
//    - Alarm handler performs server-side rollback
//
// =============================================================================

// =============================================================================
// TYPE DEFINITIONS FOR EXPECTED INTERFACES
// =============================================================================

/**
 * Expected configuration interface for transaction timeouts
 */
interface TransactionTimeoutConfig {
  /** Default timeout for transactions in milliseconds */
  defaultTimeoutMs: number;
  /** Maximum allowed timeout in milliseconds */
  maxTimeoutMs: number;
  /** Grace period before hard timeout in milliseconds */
  gracePeriodMs: number;
  /** Threshold for warning logs in milliseconds */
  warningThresholdMs: number;
}

/**
 * Expected interface for timeout enforcer
 */
interface TransactionTimeoutEnforcer {
  /** Register a transaction for timeout tracking */
  registerTransaction(txnId: string, timeoutMs: number): void;
  /** Unregister a transaction (on commit/rollback) */
  unregisterTransaction(txnId: string): void;
  /** Check if transaction is timed out */
  isTimedOut(txnId: string): boolean;
  /** Get remaining time for transaction */
  getRemainingTime(txnId: string): number;
  /** Request timeout extension */
  requestExtension(txnId: string, additionalMs: number): boolean;
  /** Set callback for timeout events */
  onTimeout(callback: (txnId: string) => void): void;
  /** Set callback for warning events */
  onWarning(callback: (txnId: string, remainingMs: number) => void): void;
}

/**
 * Expected interface for Durable Object context with alarm support
 */
interface DurableObjectAlarmContext {
  /** Set an alarm for a specific time */
  setAlarm(scheduledTime: number | Date): void;
  /** Delete the current alarm */
  deleteAlarm(): void;
  /** Get current alarm time */
  getAlarm(): number | null;
}

// =============================================================================
// TEST UTILITIES
// =============================================================================

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Mock WAL Writer for transaction manager
function createMockWALWriter() {
  let currentLSN = 0n;
  return {
    async append(_entry: any, _options?: any) {
      return { lsn: currentLSN++, flushed: true };
    },
    async flush() {
      return null;
    },
    getCurrentLSN() {
      return currentLSN;
    },
    getPendingCount() {
      return 0;
    },
    getCurrentSegmentSize() {
      return 0;
    },
    async close() {},
  };
}

// =============================================================================
// 1. SERVER-SIDE TIMEOUT ENFORCEMENT
// =============================================================================

describe('Server-Side Timeout Enforcement', () => {
  let manager: ReturnType<typeof createTransactionManager>;
  let walWriter: ReturnType<typeof createMockWALWriter>;

  beforeEach(() => {
    walWriter = createMockWALWriter();
    manager = createTransactionManager({ walWriter });
  });

  afterEach(async () => {
    try {
      if (manager.isActive()) {
        await manager.rollback();
      }
    } catch {
      // Ignore cleanup errors
    }
  });

  /**
   * GAP: Transaction should be automatically rolled back server-side after timeout
   * Currently: Transaction remains active until client explicitly rolls back
   */
  it.fails('should automatically rollback transaction after timeout expires', async () => {
    // Begin transaction with short timeout
    const ctx = await manager.begin({
      // NOTE: This option is passed but not enforced server-side
    });

    // Simulate timeout passing (in real implementation, server enforces this)
    await delay(150);

    // GAP: Transaction should be automatically rolled back
    // Currently: isActive() still returns true after timeout
    expect(manager.isActive()).toBe(false);
    expect(manager.getState()).toBe(TransactionState.NONE);
  });

  /**
   * GAP: Server should reject operations on timed-out transactions
   * Currently: Operations continue to work after timeout
   */
  it.fails('should reject operations on timed-out transactions', async () => {
    const ctx = await manager.begin({});

    // Simulate timeout
    await delay(150);

    // GAP: logOperation should throw TRANSACTION_TIMEOUT error
    expect(() => {
      manager.logOperation({
        op: 'INSERT',
        table: 'users',
        afterValue: new Uint8Array([1, 2, 3]),
      });
    }).toThrow(TransactionError);
  });

  /**
   * GAP: Timed-out transaction commit should fail with specific error
   * Currently: Commit may succeed even after timeout
   */
  it.fails('should fail commit on timed-out transaction with TIMEOUT error', async () => {
    await manager.begin({});

    // Simulate timeout
    await delay(150);

    // GAP: commit() should throw TRANSACTION_TIMEOUT error
    try {
      await manager.commit();
      throw new Error('Should have thrown');
    } catch (e: any) {
      expect(e).toBeInstanceOf(TransactionError);
      // NOTE: TransactionErrorCode.TIMEOUT should be specific to transaction timeout
      expect(e.code).toBe('TXN_TIMEOUT');
      expect(e.message).toContain('timeout');
    }
  });

  /**
   * GAP: Transaction state should be cleaned up on timeout
   * Currently: Resources (locks, log entries) remain allocated
   */
  it.fails('should cleanup transaction state on timeout', async () => {
    await manager.begin({});

    // Log some operations
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: new Uint8Array([1]),
      afterValue: new Uint8Array([1, 2, 3]),
    });

    // Simulate timeout
    await delay(150);

    // GAP: Transaction context should be null after timeout cleanup
    expect(manager.getContext()).toBeNull();
  });
});

// =============================================================================
// 2. DURABLE OBJECT ALARM INTEGRATION
// =============================================================================

describe('Durable Object Alarm Integration', () => {
  /**
   * GAP: Transaction manager should accept DO alarm context
   * Currently: No integration with Durable Object alarms
   */
  it.fails('should accept DurableObjectState for alarm scheduling', () => {
    const mockDOState = {
      storage: {
        setAlarm: vi.fn(),
        deleteAlarm: vi.fn(),
        getAlarm: vi.fn().mockReturnValue(null),
      },
    };

    // GAP: createTransactionManager should accept doState option and use it
    const manager = createTransactionManager({
      doState: mockDOState as any,
    } as any);

    expect(manager).toBeDefined();

    // GAP: The manager should expose a way to check if DO alarm integration is enabled
    // Currently: No way to verify DO alarm support is active
    expect((manager as any).hasAlarmSupport?.()).toBe(true);
  });

  /**
   * GAP: Alarm should be set when transaction begins
   * Currently: No alarm scheduling
   */
  it.fails('should set alarm when transaction begins', async () => {
    const mockSetAlarm = vi.fn();
    const mockDOState = {
      storage: {
        setAlarm: mockSetAlarm,
        deleteAlarm: vi.fn(),
        getAlarm: vi.fn().mockReturnValue(null),
      },
    };

    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      doState: mockDOState as any,
    } as any);

    const timeoutMs = 30000;
    await (manager as any).begin({ timeoutMs });

    // GAP: setAlarm should be called with timeout timestamp
    expect(mockSetAlarm).toHaveBeenCalled();
    const alarmTime = mockSetAlarm.mock.calls[0][0];
    expect(alarmTime).toBeGreaterThan(Date.now());
    expect(alarmTime).toBeLessThanOrEqual(Date.now() + timeoutMs + 100);
  });

  /**
   * GAP: Alarm should be cancelled on commit
   * Currently: No alarm cancellation
   */
  it.fails('should cancel alarm on transaction commit', async () => {
    const mockDeleteAlarm = vi.fn();
    const mockDOState = {
      storage: {
        setAlarm: vi.fn(),
        deleteAlarm: mockDeleteAlarm,
        getAlarm: vi.fn().mockReturnValue(Date.now() + 30000),
      },
    };

    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      doState: mockDOState as any,
    } as any);

    await (manager as any).begin({ timeoutMs: 30000 });
    await manager.commit();

    // GAP: deleteAlarm should be called on commit
    expect(mockDeleteAlarm).toHaveBeenCalled();
  });

  /**
   * GAP: Alarm should be cancelled on rollback
   * Currently: No alarm cancellation
   */
  it.fails('should cancel alarm on transaction rollback', async () => {
    const mockDeleteAlarm = vi.fn();
    const mockDOState = {
      storage: {
        setAlarm: vi.fn(),
        deleteAlarm: mockDeleteAlarm,
        getAlarm: vi.fn().mockReturnValue(Date.now() + 30000),
      },
    };

    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      doState: mockDOState as any,
    } as any);

    await (manager as any).begin({ timeoutMs: 30000 });
    await manager.rollback();

    // GAP: deleteAlarm should be called on rollback
    expect(mockDeleteAlarm).toHaveBeenCalled();
  });

  /**
   * GAP: Alarm handler should rollback transaction and cleanup
   * Currently: No alarm handler implementation
   */
  it.fails('should provide alarm handler for timeout enforcement', async () => {
    const walWriter = createMockWALWriter();
    const manager = createTransactionManager({
      walWriter,
    });

    await manager.begin({});

    // GAP: handleAlarm method should exist and perform rollback
    const handleAlarm = (manager as any).handleAlarm;
    expect(handleAlarm).toBeDefined();
    expect(typeof handleAlarm).toBe('function');

    // Call alarm handler
    await handleAlarm();

    // Transaction should be rolled back
    expect(manager.isActive()).toBe(false);
  });
});

// =============================================================================
// 3. GRACE PERIOD HANDLING
// =============================================================================

describe('Grace Period Handling', () => {
  /**
   * GAP: Warning should be emitted before hard timeout
   * Currently: No warning mechanism
   */
  it.fails('should emit warning before hard timeout', async () => {
    const walWriter = createMockWALWriter();
    const onWarning = vi.fn();

    const manager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 1000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 200,
        warningThresholdMs: 800,
      },
      onTimeoutWarning: onWarning,
    } as any);

    await (manager as any).begin({});

    // Wait for warning threshold
    await delay(850);

    // GAP: onWarning callback should be called
    expect(onWarning).toHaveBeenCalled();
    const [txnId, remainingMs] = onWarning.mock.calls[0];
    expect(txnId).toBeDefined();
    expect(remainingMs).toBeLessThan(200);
    expect(remainingMs).toBeGreaterThan(0);
  });

  /**
   * GAP: Client should be able to request timeout extension
   * Currently: No extension mechanism
   */
  it.fails('should allow timeout extension requests', async () => {
    const walWriter = createMockWALWriter();
    const manager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 500,
        maxTimeoutMs: 60000,
        gracePeriodMs: 100,
        warningThresholdMs: 400,
      },
    } as any);

    const ctx = await (manager as any).begin({});

    // Wait for warning period
    await delay(420);

    // GAP: requestExtension method should exist
    const extended = (manager as any).requestExtension?.(ctx.txnId, 5000);
    expect(extended).toBe(true);

    // Transaction should still be active after original timeout
    await delay(200);
    expect(manager.isActive()).toBe(true);
  });

  /**
   * GAP: Extension requests should be limited
   * Currently: No extension tracking
   */
  it.fails('should limit number of timeout extensions', async () => {
    const walWriter = createMockWALWriter();
    const manager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 500,
        maxTimeoutMs: 2000, // Max 2 seconds total
        gracePeriodMs: 100,
        warningThresholdMs: 400,
      },
      maxExtensions: 2,
    } as any);

    const ctx = await (manager as any).begin({});

    // First extension - should succeed
    const ext1 = (manager as any).requestExtension?.(ctx.txnId, 500);
    expect(ext1).toBe(true);

    // Second extension - should succeed
    const ext2 = (manager as any).requestExtension?.(ctx.txnId, 500);
    expect(ext2).toBe(true);

    // Third extension - should fail (limit reached)
    const ext3 = (manager as any).requestExtension?.(ctx.txnId, 500);
    expect(ext3).toBe(false);
  });

  /**
   * GAP: Extension should not exceed maxTimeoutMs
   * Currently: No timeout limits enforced
   */
  it.fails('should cap extension at maxTimeoutMs', async () => {
    const walWriter = createMockWALWriter();
    const manager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 1000,
        maxTimeoutMs: 2000, // Max 2 seconds
        gracePeriodMs: 200,
        warningThresholdMs: 800,
      },
    } as any);

    const ctx = await (manager as any).begin({});

    // Request extension that would exceed max
    const extended = (manager as any).requestExtension?.(ctx.txnId, 5000);

    // Should be capped at maxTimeoutMs
    const remaining = (manager as any).getRemainingTime?.(ctx.txnId);
    expect(remaining).toBeLessThanOrEqual(2000);
  });
});

// =============================================================================
// 4. TIMEOUT CONFIGURATION
// =============================================================================

describe('Timeout Configuration', () => {
  /**
   * GAP: Transaction manager should accept timeout configuration
   * Currently: No timeoutConfig option
   */
  it.fails('should accept TransactionTimeoutConfig', () => {
    const config: TransactionTimeoutConfig = {
      defaultTimeoutMs: 30000,
      maxTimeoutMs: 120000,
      gracePeriodMs: 5000,
      warningThresholdMs: 25000,
    };

    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      timeoutConfig: config,
    } as any);

    // GAP: getTimeoutConfig() should return the configuration
    const retrievedConfig = (manager as any).getTimeoutConfig?.();
    expect(retrievedConfig).toEqual(config);
  });

  /**
   * GAP: Default timeout should be applied when not specified
   * Currently: No default timeout enforcement
   */
  it.fails('should apply default timeout when not specified in begin()', async () => {
    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      timeoutConfig: {
        defaultTimeoutMs: 1000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 200,
        warningThresholdMs: 800,
      },
    } as any);

    const ctx = await (manager as any).begin({}); // No timeoutMs specified

    // GAP: expiresAt should be set based on defaultTimeoutMs
    expect(ctx.expiresAt).toBeDefined();
    expect(ctx.expiresAt).toBeLessThanOrEqual(Date.now() + 1100);
    expect(ctx.expiresAt).toBeGreaterThanOrEqual(Date.now() + 900);
  });

  /**
   * GAP: Requested timeout should be capped at maxTimeoutMs
   * Currently: No maximum timeout enforcement
   */
  it.fails('should cap requested timeout at maxTimeoutMs', async () => {
    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      timeoutConfig: {
        defaultTimeoutMs: 30000,
        maxTimeoutMs: 60000, // Max 1 minute
        gracePeriodMs: 5000,
        warningThresholdMs: 55000,
      },
    } as any);

    // Request 5 minutes - should be capped
    const ctx = await (manager as any).begin({ timeoutMs: 300000 });

    // GAP: expiresAt should be capped at maxTimeoutMs
    const maxExpiry = Date.now() + 60100; // Allow small buffer
    expect(ctx.expiresAt).toBeLessThanOrEqual(maxExpiry);
  });

  /**
   * GAP: Per-transaction timeout override should work
   * Currently: Timeout is passed through but not enforced
   */
  it.fails('should allow per-transaction timeout override', async () => {
    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      timeoutConfig: {
        defaultTimeoutMs: 30000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 5000,
        warningThresholdMs: 25000,
      },
    } as any);

    // Override with shorter timeout
    const ctx = await (manager as any).begin({ timeoutMs: 5000 });

    // GAP: Transaction should timeout after 5 seconds, not 30
    const expectedExpiry = Date.now() + 5100;
    expect(ctx.expiresAt).toBeLessThanOrEqual(expectedExpiry);
  });
});

// =============================================================================
// 5. LONG-RUNNING TRANSACTION LOGGING
// =============================================================================

describe('Long-Running Transaction Logging', () => {
  /**
   * GAP: Should log warning when transaction exceeds threshold
   * Currently: No logging of long-running transactions
   */
  it.fails('should log warning when transaction exceeds warningThreshold', async () => {
    const logSpy = vi.fn();
    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      timeoutConfig: {
        defaultTimeoutMs: 5000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 1000,
        warningThresholdMs: 200, // Short threshold for testing
      },
      onLongRunningTransaction: logSpy,
    } as any);

    await (manager as any).begin({});

    // Wait for warning threshold
    await delay(250);

    // GAP: onLongRunningTransaction callback should be called
    expect(logSpy).toHaveBeenCalled();
    const [logEntry] = logSpy.mock.calls[0];
    expect(logEntry.txnId).toBeDefined();
    expect(logEntry.durationMs).toBeGreaterThanOrEqual(200);
  });

  /**
   * GAP: Log entry should include query details
   * Currently: No query tracking in transaction context
   */
  it.fails('should include query details in long-running transaction log', async () => {
    const logSpy = vi.fn();
    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      timeoutConfig: {
        defaultTimeoutMs: 5000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 1000,
        warningThresholdMs: 200,
      },
      onLongRunningTransaction: logSpy,
      trackQueries: true,
    } as any);

    await (manager as any).begin({});

    // Log some operations
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      afterValue: new Uint8Array([1]),
    });

    await delay(250);

    // GAP: Log entry should include operations/queries
    expect(logSpy).toHaveBeenCalled();
    const [logEntry] = logSpy.mock.calls[0];
    expect(logEntry.operations).toBeDefined();
    expect(logEntry.operations.length).toBeGreaterThan(0);
    expect(logEntry.operations[0].table).toBe('users');
  });

  /**
   * GAP: Log entry should include lock information
   * Currently: No lock tracking in transaction context for logging
   */
  it.fails('should include held locks in long-running transaction log', async () => {
    const logSpy = vi.fn();
    const lockManager = createLockManager({
      defaultTimeout: 5000,
    });

    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      lockManager,
      timeoutConfig: {
        defaultTimeoutMs: 5000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 1000,
        warningThresholdMs: 200,
      },
      onLongRunningTransaction: logSpy,
    } as any);

    const ctx = await (manager as any).begin({});

    // Acquire some locks
    await lockManager.acquire({
      txnId: ctx.txnId,
      resource: 'users',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    await delay(250);

    // GAP: Log entry should include lock information
    expect(logSpy).toHaveBeenCalled();
    const [logEntry] = logSpy.mock.calls[0];
    expect(logEntry.heldLocks).toBeDefined();
    expect(logEntry.heldLocks.length).toBeGreaterThan(0);
    expect(logEntry.heldLocks[0].resource).toBe('users');

    lockManager.releaseAll(ctx.txnId);
  });

  /**
   * GAP: Should log structured data for debugging
   * Currently: No structured logging
   */
  it.fails('should provide structured log format for debugging', async () => {
    const logSpy = vi.fn();
    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      timeoutConfig: {
        defaultTimeoutMs: 5000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 1000,
        warningThresholdMs: 200,
      },
      onLongRunningTransaction: logSpy,
    } as any);

    await (manager as any).begin({ readOnly: false });

    await delay(250);

    // GAP: Log should have structured format
    expect(logSpy).toHaveBeenCalled();
    const [logEntry] = logSpy.mock.calls[0];

    // Expected structured fields
    expect(logEntry).toMatchObject({
      txnId: expect.any(String),
      durationMs: expect.any(Number),
      startedAt: expect.any(Number),
      readOnly: expect.any(Boolean),
      isolationLevel: expect.any(String),
      operationCount: expect.any(Number),
      warningLevel: expect.stringMatching(/warning|critical/),
    });
  });
});

// =============================================================================
// 6. TIMEOUT BEHAVIOR DURING BLOCKING OPERATIONS
// =============================================================================

describe('Timeout During Blocking Operations', () => {
  let lockManager: LockManager;

  beforeEach(() => {
    lockManager = createLockManager({
      defaultTimeout: 10000, // Long timeout for lock manager
      detectDeadlocks: true,
    });
  });

  afterEach(() => {
    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
    lockManager.releaseAll('blocker');
  });

  /**
   * GAP: Transaction timeout should interrupt lock acquisition
   * Currently: Lock wait continues even after transaction should timeout
   */
  it.fails('should interrupt lock acquisition on transaction timeout', async () => {
    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      lockManager,
      timeoutConfig: {
        defaultTimeoutMs: 200, // Short transaction timeout
        maxTimeoutMs: 60000,
        gracePeriodMs: 50,
        warningThresholdMs: 150,
      },
    } as any);

    // Blocker holds lock
    await lockManager.acquire({
      txnId: 'blocker',
      resource: 'users',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    const ctx = await (manager as any).begin({});

    // GAP: prepareWrite should fail with transaction timeout
    // Currently: Lock wait timeout is separate from transaction timeout
    await expect(
      (manager as any).prepareWrite?.(ctx, 'users', new Uint8Array([1]))
    ).rejects.toMatchObject({
      code: 'TXN_TIMEOUT',
    });

    lockManager.releaseAll('blocker');
  });

  /**
   * GAP: Transaction should track separate I/O timeout
   * Currently: No separate I/O timeout tracking
   */
  it.fails('should track I/O timeout separately from transaction timeout', async () => {
    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      timeoutConfig: {
        defaultTimeoutMs: 30000, // Long transaction timeout
        maxTimeoutMs: 60000,
        gracePeriodMs: 5000,
        warningThresholdMs: 25000,
      },
      ioTimeoutMs: 100, // Short I/O timeout
    } as any);

    const ctx = await (manager as any).begin({});

    // GAP: Manager should expose ioTimeoutMs configuration and provide
    // a way to wrap I/O operations with the timeout
    expect((manager as any).getIoTimeoutMs?.()).toBe(100);

    // GAP: executeWithIoTimeout should exist to wrap I/O operations
    const executeWithIoTimeout = (manager as any).executeWithIoTimeout;
    expect(executeWithIoTimeout).toBeDefined();
    expect(typeof executeWithIoTimeout).toBe('function');

    // When I/O times out, it should throw IO_TIMEOUT (not TXN_TIMEOUT)
    // and transaction should remain active
    await expect(
      executeWithIoTimeout(async () => {
        await delay(200); // Exceeds ioTimeoutMs
      })
    ).rejects.toMatchObject({
      code: 'IO_TIMEOUT',
    });

    // Transaction should still be active (I/O timeout != transaction timeout)
    expect(manager.isActive()).toBe(true);
  });

  /**
   * GAP: Partial state should be cleaned up on timeout during operation
   * Currently: No partial state cleanup
   */
  it.fails('should cleanup partial state on timeout during operation', async () => {
    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      lockManager,
      timeoutConfig: {
        defaultTimeoutMs: 200,
        maxTimeoutMs: 60000,
        gracePeriodMs: 50,
        warningThresholdMs: 150,
      },
    } as any);

    // Blocker holds lock
    await lockManager.acquire({
      txnId: 'blocker',
      resource: 'users',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    const ctx = await (manager as any).begin({});

    // Log some operations before timeout
    manager.logOperation({
      op: 'INSERT',
      table: 'orders',
      afterValue: new Uint8Array([1]),
    });

    try {
      // This will timeout waiting for lock
      await (manager as any).prepareWrite?.(ctx, 'users', new Uint8Array([1]));
    } catch {
      // Expected timeout
    }

    // GAP: Transaction log should be cleaned up
    expect(manager.getContext()).toBeNull();
    // GAP: Any partial locks should be released
    expect(lockManager.getHeldLocks(ctx.txnId)).toHaveLength(0);

    lockManager.releaseAll('blocker');
  });

  /**
   * GAP: Timeout should release waiting transactions
   * Currently: Waiting transactions not notified of holder timeout
   */
  it.fails('should notify waiting transactions when holder times out', async () => {
    const manager1 = createTransactionManager({
      walWriter: createMockWALWriter(),
      lockManager,
      timeoutConfig: {
        defaultTimeoutMs: 100, // Short timeout
        maxTimeoutMs: 60000,
        gracePeriodMs: 20,
        warningThresholdMs: 80,
      },
    } as any);

    const manager2 = createTransactionManager({
      walWriter: createMockWALWriter(),
      lockManager,
      timeoutConfig: {
        defaultTimeoutMs: 5000, // Longer timeout
        maxTimeoutMs: 60000,
        gracePeriodMs: 1000,
        warningThresholdMs: 4000,
      },
    } as any);

    // txn1 acquires lock
    const ctx1 = await (manager1 as any).begin({});
    await lockManager.acquire({
      txnId: ctx1.txnId,
      resource: 'R1',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    // txn2 waits for lock
    const ctx2 = await (manager2 as any).begin({});
    const waitPromise = lockManager.acquire({
      txnId: ctx2.txnId,
      resource: 'R1',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
      timeout: 5000,
    });

    // Wait for txn1 to timeout
    await delay(150);

    // GAP: txn1 should be rolled back and locks released
    // GAP: txn2 should acquire the lock after txn1 times out
    await expect(waitPromise).resolves.toMatchObject({
      acquired: true,
    });

    lockManager.releaseAll(ctx1.txnId);
    lockManager.releaseAll(ctx2.txnId);
  });

  /**
   * GAP: Transaction timeout should be tracked per-transaction
   * Currently: No per-transaction timeout tracking
   */
  it.fails('should track timeout independently for concurrent transactions', async () => {
    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      timeoutConfig: {
        defaultTimeoutMs: 5000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 1000,
        warningThresholdMs: 4000,
      },
    } as any);

    // This test requires multi-transaction support
    // which isn't currently in the single-transaction manager

    // GAP: Manager should support tracking multiple transactions
    // with independent timeouts

    // Begin first transaction with short timeout
    // const ctx1 = await manager.beginTracked({ timeoutMs: 100 });

    // Begin second transaction with longer timeout
    // const ctx2 = await manager.beginTracked({ timeoutMs: 5000 });

    // Wait for first to timeout
    // await delay(150);

    // First should be timed out
    // expect(manager.isTimedOut(ctx1.txnId)).toBe(true);

    // Second should still be active
    // expect(manager.isTimedOut(ctx2.txnId)).toBe(false);

    // For now, mark this as fails since multi-transaction tracking doesn't exist
    expect((manager as any).beginTracked).toBeDefined();
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('Timeout Enforcement Integration', () => {
  /**
   * GAP: Full timeout enforcement lifecycle should work end-to-end
   * Currently: No server-side enforcement
   */
  it.fails('should enforce timeout in complete transaction lifecycle', async () => {
    const mockDOState = {
      storage: {
        setAlarm: vi.fn(),
        deleteAlarm: vi.fn(),
        getAlarm: vi.fn().mockReturnValue(null),
      },
    };

    const onWarning = vi.fn();
    const onTimeout = vi.fn();
    const onLongRunning = vi.fn();

    const manager = createTransactionManager({
      walWriter: createMockWALWriter(),
      doState: mockDOState as any,
      timeoutConfig: {
        defaultTimeoutMs: 500,
        maxTimeoutMs: 60000,
        gracePeriodMs: 100,
        warningThresholdMs: 300,
      },
      onTimeoutWarning: onWarning,
      onTimeout: onTimeout,
      onLongRunningTransaction: onLongRunning,
    } as any);

    // Begin transaction
    await (manager as any).begin({});

    // 1. Alarm should be set
    expect(mockDOState.storage.setAlarm).toHaveBeenCalled();

    // 2. Long-running warning should fire
    await delay(350);
    expect(onLongRunning).toHaveBeenCalled();

    // 3. Timeout warning should fire in grace period
    await delay(100);
    expect(onWarning).toHaveBeenCalled();

    // 4. Transaction should be rolled back after full timeout
    await delay(100);
    expect(onTimeout).toHaveBeenCalled();
    expect(manager.isActive()).toBe(false);
    expect(mockDOState.storage.deleteAlarm).toHaveBeenCalled();
  });

  /**
   * GAP: executeInTransaction should respect timeout
   * Currently: Timeout passed but not enforced
   */
  it.fails('should enforce timeout in executeInTransaction wrapper', async () => {
    const walWriter = createMockWALWriter();
    const manager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 100,
        maxTimeoutMs: 60000,
        gracePeriodMs: 20,
        warningThresholdMs: 80,
      },
    } as any);

    const result = await executeInTransaction(
      manager,
      async (ctx) => {
        // Simulate long-running operation
        await delay(200);
        return { success: true };
      },
      {} // Default timeout from config
    );

    // GAP: Transaction should fail due to timeout
    expect(result.committed).toBe(false);
    expect(result.error?.message).toContain('timeout');
  });
});
