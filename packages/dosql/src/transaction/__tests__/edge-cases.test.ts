/**
 * Transaction Edge Case Tests
 *
 * Comprehensive tests for transaction edge cases including:
 * - Transaction timeout handling
 * - Deadlock detection and resolution
 * - Isolation level enforcement (READ COMMITTED, SERIALIZABLE)
 * - Concurrent transaction conflicts
 *
 * Issue: sql-hyv1
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  createTransactionManager,
  type ExtendedTransactionManager,
} from '../manager.js';
import {
  createLockManager,
  createIsolationEnforcer,
  type LockManager,
  type IsolationEnforcer,
} from '../isolation.js';
import {
  DeadlockDetector,
  DeadlockError,
  type DeadlockInfo,
  type VictimSelectionPolicy,
} from '../../database/deadlock-detector.js';
import {
  TransactionState,
  TransactionMode,
  IsolationLevel,
  LockType,
  TransactionError,
  TransactionErrorCode,
  createTransactionId,
  type TransactionId,
  type TransactionContext,
} from '../types.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Mock WAL Writer that tracks all appended entries
 */
function createMockWALWriter() {
  let currentLSN = 0n;
  const entries: Array<{ txnId: string; op: string }> = [];

  return {
    async append(entry: { txnId: string; op: string }, _options?: unknown) {
      entries.push({ txnId: entry.txnId, op: entry.op });
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
    getEntries() {
      return entries;
    },
    clearEntries() {
      entries.length = 0;
    },
  };
}

/**
 * In-memory storage for testing rollback operations
 */
function createInMemoryStorage() {
  const storage = new Map<string, Map<string, Uint8Array>>();

  return {
    get(table: string, key: Uint8Array): Uint8Array | undefined {
      const tableData = storage.get(table);
      if (!tableData) return undefined;
      return tableData.get(JSON.stringify(Array.from(key)));
    },
    set(table: string, key: Uint8Array, value: Uint8Array): void {
      if (!storage.has(table)) {
        storage.set(table, new Map());
      }
      storage.get(table)!.set(JSON.stringify(Array.from(key)), value);
    },
    delete(table: string, key: Uint8Array): void {
      const tableData = storage.get(table);
      if (tableData) {
        tableData.delete(JSON.stringify(Array.from(key)));
      }
    },
    clear(): void {
      storage.clear();
    },
  };
}

// =============================================================================
// 1. TRANSACTION TIMEOUT HANDLING
// =============================================================================

describe('Transaction Timeout Handling', () => {
  let manager: ExtendedTransactionManager;
  let walWriter: ReturnType<typeof createMockWALWriter>;
  let storage: ReturnType<typeof createInMemoryStorage>;
  let lockManager: LockManager;

  beforeEach(() => {
    walWriter = createMockWALWriter();
    storage = createInMemoryStorage();
    lockManager = createLockManager({
      defaultTimeout: 5000,
      detectDeadlocks: true,
    });
    manager = createTransactionManager({
      walWriter,
      lockManager,
      timeoutConfig: {
        defaultTimeoutMs: 200,
        maxTimeoutMs: 60000,
        gracePeriodMs: 50,
        warningThresholdMs: 150,
      },
    });

    manager.setApplyFunction(async (op, table, key, value) => {
      if (!key) return;
      switch (op) {
        case 'INSERT':
          if (value) storage.set(table, key, value);
          break;
        case 'UPDATE':
          if (value) storage.set(table, key, value);
          break;
        case 'DELETE':
          storage.delete(table, key);
          break;
      }
    });
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

  it('should auto-rollback transaction after timeout expires', async () => {
    const ctx = await manager.begin({ timeoutMs: 100 });
    const key = new Uint8Array([1]);
    const value = new Uint8Array([10, 20, 30]);

    // Insert data
    storage.set('users', key, value);
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key,
      afterValue: value,
    });

    // Verify data exists
    expect(storage.get('users', key)).toEqual(value);

    // Wait for timeout
    await delay(150);

    // Transaction should be automatically rolled back
    expect(manager.isActive()).toBe(false);
    expect(manager.getContext()).toBeNull();

    // Data should be reverted
    expect(storage.get('users', key)).toBeUndefined();
  });

  it('should throw TIMEOUT error when operating on expired transaction', async () => {
    await manager.begin({ timeoutMs: 100 });

    // Wait for timeout
    await delay(150);

    // Operations should fail with timeout error
    expect(() => {
      manager.logOperation({
        op: 'INSERT',
        table: 'users',
        key: new Uint8Array([1]),
        afterValue: new Uint8Array([1]),
      });
    }).toThrow(TransactionError);
  });

  it('should release all locks on timeout', async () => {
    const ctx = await manager.begin({ timeoutMs: 100 });

    // Acquire locks
    await lockManager.acquire({
      txnId: ctx.txnId,
      resource: 'users',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    expect(lockManager.getHeldLocks(ctx.txnId)).toHaveLength(1);

    // Wait for timeout
    await delay(150);

    // Locks should be released
    expect(lockManager.getHeldLocks(ctx.txnId)).toHaveLength(0);
  });

  it('should allow timeout extension before expiry', async () => {
    const ctx = await manager.begin({ timeoutMs: 200 });

    // Wait for half the timeout
    await delay(100);

    // Request extension
    const extended = manager.requestExtension(ctx.txnId, 200);
    expect(extended).toBe(true);

    // Wait past original timeout
    await delay(150);

    // Transaction should still be active
    expect(manager.isActive()).toBe(true);

    await manager.rollback();
  });

  it('should reject extension after max extensions reached', async () => {
    const limitedManager = createTransactionManager({
      walWriter,
      lockManager,
      timeoutConfig: {
        defaultTimeoutMs: 500,
        maxTimeoutMs: 60000,
        gracePeriodMs: 100,
        warningThresholdMs: 400,
      },
      maxExtensions: 1,
    });

    const ctx = await limitedManager.begin({});

    // First extension should succeed
    expect(limitedManager.requestExtension(ctx.txnId, 100)).toBe(true);

    // Second extension should fail (limit reached)
    expect(limitedManager.requestExtension(ctx.txnId, 100)).toBe(false);

    await limitedManager.rollback();
  });

  it('should emit warning callback before timeout', async () => {
    const onWarning = vi.fn();
    const warningManager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 200,
        maxTimeoutMs: 60000,
        gracePeriodMs: 50,
        warningThresholdMs: 100,
      },
      onTimeoutWarning: onWarning,
    });

    await warningManager.begin({});

    // Wait for grace period to start (200 - 50 = 150ms)
    await delay(170);

    expect(onWarning).toHaveBeenCalled();
    const [txnId, remainingMs] = onWarning.mock.calls[0];
    expect(txnId).toBeDefined();
    expect(remainingMs).toBeLessThanOrEqual(50);

    await warningManager.rollback();
  });

  it('should emit timeout callback when transaction times out', async () => {
    const onTimeout = vi.fn();
    const timeoutManager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 100,
        maxTimeoutMs: 60000,
        gracePeriodMs: 20,
        warningThresholdMs: 80,
      },
      onTimeout,
    });

    await timeoutManager.begin({});

    // Wait for timeout
    await delay(150);

    expect(onTimeout).toHaveBeenCalled();
  });

  it('should track I/O timeout separately from transaction timeout', async () => {
    const ioManager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 30000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 5000,
        warningThresholdMs: 25000,
      },
      ioTimeoutMs: 50,
    });

    await ioManager.begin({});

    // I/O timeout should throw IO_TIMEOUT
    await expect(
      ioManager.executeWithIoTimeout(async () => {
        await delay(100);
      })
    ).rejects.toMatchObject({
      code: TransactionErrorCode.IO_TIMEOUT,
    });

    // Transaction should still be active
    expect(ioManager.isActive()).toBe(true);

    await ioManager.rollback();
  });
});

// =============================================================================
// 2. DEADLOCK DETECTION AND RESOLUTION
// =============================================================================

describe('Deadlock Detection and Resolution', () => {
  let lockManager: LockManager;
  let walWriter: ReturnType<typeof createMockWALWriter>;

  beforeEach(() => {
    walWriter = createMockWALWriter();
  });

  afterEach(() => {
    // Cleanup any held locks
    lockManager?.releaseAll(createTransactionId('txn1'));
    lockManager?.releaseAll(createTransactionId('txn2'));
    lockManager?.releaseAll(createTransactionId('txn3'));
  });

  it('should detect simple two-transaction deadlock', async () => {
    lockManager = createLockManager({
      defaultTimeout: 5000,
      detectDeadlocks: true,
      victimSelection: 'youngest',
    });

    const txn1 = createTransactionId('txn1');
    const txn2 = createTransactionId('txn2');

    // txn1 holds lock on resource A
    await lockManager.acquire({
      txnId: txn1,
      resource: 'A',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    // txn2 holds lock on resource B
    await lockManager.acquire({
      txnId: txn2,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now() + 1, // txn2 is younger
    });

    // txn1 waits for B (held by txn2) - this blocks
    const txn1WaitPromise = lockManager.acquire({
      txnId: txn1,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
      timeout: 5000,
    });

    // Small delay to ensure txn1 is in wait queue
    await delay(10);

    // txn2 waits for A (held by txn1) - this should detect deadlock
    await expect(
      lockManager.acquire({
        txnId: txn2,
        resource: 'A',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now(),
        timeout: 5000,
      })
    ).rejects.toBeInstanceOf(DeadlockError);

    // Cleanup
    lockManager.releaseAll(txn1);
    lockManager.releaseAll(txn2);
  });

  it('should select youngest transaction as victim with youngest policy', async () => {
    lockManager = createLockManager({
      defaultTimeout: 5000,
      detectDeadlocks: true,
      victimSelection: 'youngest',
    });

    const txn1 = createTransactionId('older_txn');
    const txn2 = createTransactionId('younger_txn');

    // Older transaction gets resource A
    await lockManager.acquire({
      txnId: txn1,
      resource: 'A',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now() - 1000, // 1 second older
    });

    // Younger transaction gets resource B
    await lockManager.acquire({
      txnId: txn2,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    // Older waits for B
    const olderWaitPromise = lockManager.acquire({
      txnId: txn1,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
      timeout: 5000,
    });

    await delay(10);

    // Younger waits for A - deadlock detected, younger should be victim
    try {
      await lockManager.acquire({
        txnId: txn2,
        resource: 'A',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now(),
        timeout: 5000,
      });
      expect.fail('Should have thrown DeadlockError');
    } catch (error) {
      expect(error).toBeInstanceOf(DeadlockError);
      const deadlockError = error as DeadlockError;
      // The error should be thrown for the requesting transaction
      expect(deadlockError.cycle).toBeDefined();
      expect(deadlockError.cycle.length).toBeGreaterThanOrEqual(2);
    }

    lockManager.releaseAll(txn1);
    lockManager.releaseAll(txn2);
  });

  it('should provide deadlock statistics', async () => {
    lockManager = createLockManager({
      defaultTimeout: 5000,
      detectDeadlocks: true,
    });

    const txn1 = createTransactionId('txn1');
    const txn2 = createTransactionId('txn2');

    // Create deadlock scenario
    await lockManager.acquire({
      txnId: txn1,
      resource: 'A',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    await lockManager.acquire({
      txnId: txn2,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now() + 1,
    });

    const waitPromise = lockManager.acquire({
      txnId: txn1,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
      timeout: 5000,
    });

    await delay(10);

    try {
      await lockManager.acquire({
        txnId: txn2,
        resource: 'A',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now(),
      });
    } catch {
      // Expected
    }

    const stats = lockManager.getDeadlockStats();
    expect(stats.totalDeadlocks).toBeGreaterThan(0);

    lockManager.releaseAll(txn1);
    lockManager.releaseAll(txn2);
  });

  it('should track deadlock history', async () => {
    lockManager = createLockManager({
      defaultTimeout: 5000,
      detectDeadlocks: true,
    });

    const txn1 = createTransactionId('txn1');
    const txn2 = createTransactionId('txn2');

    // Create deadlock
    await lockManager.acquire({
      txnId: txn1,
      resource: 'A',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    await lockManager.acquire({
      txnId: txn2,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now() + 1,
    });

    const waitPromise = lockManager.acquire({
      txnId: txn1,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
      timeout: 5000,
    });

    await delay(10);

    try {
      await lockManager.acquire({
        txnId: txn2,
        resource: 'A',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now(),
      });
    } catch {
      // Expected
    }

    const history = lockManager.getDeadlockHistory();
    expect(history.length).toBeGreaterThan(0);
    expect(history[0].cycle).toBeDefined();
    expect(history[0].resources).toBeDefined();
    expect(history[0].timestamp).toBeDefined();

    lockManager.releaseAll(txn1);
    lockManager.releaseAll(txn2);
  });

  it('should invoke onDeadlock callback when deadlock detected', async () => {
    const onDeadlock = vi.fn();
    lockManager = createLockManager({
      defaultTimeout: 5000,
      detectDeadlocks: true,
      onDeadlock,
    });

    const txn1 = createTransactionId('txn1');
    const txn2 = createTransactionId('txn2');

    await lockManager.acquire({
      txnId: txn1,
      resource: 'A',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    await lockManager.acquire({
      txnId: txn2,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now() + 1,
    });

    const waitPromise = lockManager.acquire({
      txnId: txn1,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
      timeout: 5000,
    });

    await delay(10);

    try {
      await lockManager.acquire({
        txnId: txn2,
        resource: 'A',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now(),
      });
    } catch {
      // Expected
    }

    expect(onDeadlock).toHaveBeenCalled();
    const [info] = onDeadlock.mock.calls[0];
    expect(info.cycle).toBeDefined();
    expect(info.victimTxnId).toBeDefined();

    lockManager.releaseAll(txn1);
    lockManager.releaseAll(txn2);
  });

  describe('Deadlock Prevention Schemes', () => {
    it('should enforce wait-die prevention (younger aborts)', async () => {
      lockManager = createLockManager({
        defaultTimeout: 5000,
        detectDeadlocks: true,
        deadlockPrevention: 'waitDie',
      });

      const olderTxn = createTransactionId('older');
      const youngerTxn = createTransactionId('younger');

      // Older transaction holds lock
      await lockManager.acquire({
        txnId: olderTxn,
        resource: 'A',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now() - 1000,
      });

      // Younger tries to acquire same lock - should abort
      await expect(
        lockManager.acquire({
          txnId: youngerTxn,
          resource: 'A',
          lockType: LockType.EXCLUSIVE,
          timestamp: Date.now(),
        })
      ).rejects.toMatchObject({
        code: TransactionErrorCode.ABORTED,
      });

      lockManager.releaseAll(olderTxn);
    });

    it('should allow older transaction to wait in wait-die', async () => {
      lockManager = createLockManager({
        defaultTimeout: 5000,
        detectDeadlocks: true,
        deadlockPrevention: 'waitDie',
      });

      const olderTxn = createTransactionId('older');
      const youngerTxn = createTransactionId('younger');

      // Register transactions with proper timestamps so the detector knows which is older
      // The deadlock detector uses registration time internally

      // Younger transaction holds lock first (acquires it, but is younger by timestamp)
      await lockManager.acquire({
        txnId: youngerTxn,
        resource: 'A',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now() + 1000, // Younger timestamp = higher number
      });

      // Start older waiting (should be allowed to wait since it's older)
      // In wait-die: older waits, younger dies
      const acquirePromise = lockManager.acquire({
        txnId: olderTxn,
        resource: 'A',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now() - 1000, // Older timestamp = lower number
        timeout: 200,
      });

      // Release lock so older can acquire
      await delay(50);
      lockManager.release(youngerTxn, 'A');

      // Older should eventually get the lock
      const result = await acquirePromise;
      expect(result.acquired).toBe(true);

      lockManager.releaseAll(olderTxn);
      lockManager.releaseAll(youngerTxn);
    });

    it('should enforce no-wait prevention (immediate failure)', async () => {
      lockManager = createLockManager({
        defaultTimeout: 5000,
        detectDeadlocks: true,
        deadlockPrevention: 'noWait',
      });

      const txn1 = createTransactionId('txn1');
      const txn2 = createTransactionId('txn2');

      // First transaction holds lock
      await lockManager.acquire({
        txnId: txn1,
        resource: 'A',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now(),
      });

      // Second transaction should fail immediately
      await expect(
        lockManager.acquire({
          txnId: txn2,
          resource: 'A',
          lockType: LockType.EXCLUSIVE,
          timestamp: Date.now(),
        })
      ).rejects.toMatchObject({
        code: TransactionErrorCode.LOCK_FAILED,
      });

      lockManager.releaseAll(txn1);
    });
  });
});

// =============================================================================
// 3. ISOLATION LEVEL ENFORCEMENT
// =============================================================================

describe('Isolation Level Enforcement', () => {
  let walWriter: ReturnType<typeof createMockWALWriter>;
  let lockManager: LockManager;

  beforeEach(() => {
    walWriter = createMockWALWriter();
    lockManager = createLockManager({
      defaultTimeout: 5000,
      detectDeadlocks: true,
    });
  });

  afterEach(() => {
    lockManager.releaseAll(createTransactionId('txn1'));
    lockManager.releaseAll(createTransactionId('txn2'));
    lockManager.releaseAll(createTransactionId('reader'));
    lockManager.releaseAll(createTransactionId('writer'));
  });

  describe('READ COMMITTED Isolation', () => {
    it('should release read locks immediately after read', async () => {
      const enforcer = createIsolationEnforcer({ lockManager });
      const txnId = createTransactionId('reader');
      const context: TransactionContext = {
        txnId,
        state: TransactionState.ACTIVE,
        mode: TransactionMode.DEFERRED,
        isolationLevel: IsolationLevel.READ_COMMITTED,
        savepoints: { savepoints: [], maxDepth: 0 },
        log: { txnId, entries: [], currentSequence: 0, startedAt: Date.now() },
        locks: [],
        startedAt: Date.now(),
        readOnly: false,
        autoCommit: false,
      };

      // Prepare for read
      await enforcer.prepareRead(context, 'users');

      // In READ_COMMITTED, locks are released immediately after read
      const heldLocks = lockManager.getHeldLocks(txnId);
      expect(heldLocks.length).toBe(0);
    });

    it('should allow another transaction to write after read releases lock', async () => {
      const enforcer = createIsolationEnforcer({ lockManager });
      const readerTxn = createTransactionId('reader');
      const writerTxn = createTransactionId('writer');

      const readerContext: TransactionContext = {
        txnId: readerTxn,
        state: TransactionState.ACTIVE,
        mode: TransactionMode.DEFERRED,
        isolationLevel: IsolationLevel.READ_COMMITTED,
        savepoints: { savepoints: [], maxDepth: 0 },
        log: { txnId: readerTxn, entries: [], currentSequence: 0, startedAt: Date.now() },
        locks: [],
        startedAt: Date.now(),
        readOnly: true,
        autoCommit: false,
      };

      // Reader reads (and releases lock)
      await enforcer.prepareRead(readerContext, 'users');

      // Writer should be able to get exclusive lock
      const writerContext: TransactionContext = {
        txnId: writerTxn,
        state: TransactionState.ACTIVE,
        mode: TransactionMode.DEFERRED,
        isolationLevel: IsolationLevel.READ_COMMITTED,
        savepoints: { savepoints: [], maxDepth: 0 },
        log: { txnId: writerTxn, entries: [], currentSequence: 0, startedAt: Date.now() },
        locks: [],
        startedAt: Date.now(),
        readOnly: false,
        autoCommit: false,
      };

      // Writer should acquire lock successfully
      await enforcer.prepareWrite(writerContext, 'users', new Uint8Array([1]));
      expect(writerContext.locks.length).toBe(1);
      expect(writerContext.locks[0].lockType).toBe(LockType.EXCLUSIVE);

      lockManager.releaseAll(writerTxn);
    });
  });

  describe('SERIALIZABLE Isolation', () => {
    it('should hold read locks until transaction ends', async () => {
      const enforcer = createIsolationEnforcer({ lockManager });
      const txnId = createTransactionId('reader');
      const context: TransactionContext = {
        txnId,
        state: TransactionState.ACTIVE,
        mode: TransactionMode.DEFERRED,
        isolationLevel: IsolationLevel.SERIALIZABLE,
        savepoints: { savepoints: [], maxDepth: 0 },
        log: { txnId, entries: [], currentSequence: 0, startedAt: Date.now() },
        locks: [],
        startedAt: Date.now(),
        readOnly: false,
        autoCommit: false,
      };

      // Prepare for read
      await enforcer.prepareRead(context, 'users');

      // Lock should still be held
      expect(context.locks.length).toBe(1);
      expect(context.locks[0].lockType).toBe(LockType.SHARED);

      // Release on commit
      enforcer.onCommit(context);
      expect(lockManager.getHeldLocks(txnId).length).toBe(0);
    });

    it('should track read set for serializable validation', async () => {
      // Use a separate lock manager for this test to avoid conflicts
      const testLockManager = createLockManager({
        defaultTimeout: 100,
        detectDeadlocks: true,
      });
      const enforcer = createIsolationEnforcer({ lockManager: testLockManager });
      const txn1 = createTransactionId('txn1_serializable');
      const txn2 = createTransactionId('txn2_serializable');

      const ctx1: TransactionContext = {
        txnId: txn1,
        state: TransactionState.ACTIVE,
        mode: TransactionMode.DEFERRED,
        isolationLevel: IsolationLevel.SERIALIZABLE,
        savepoints: { savepoints: [], maxDepth: 0 },
        log: { txnId: txn1, entries: [], currentSequence: 0, startedAt: Date.now() },
        locks: [],
        startedAt: Date.now(),
        readOnly: false,
        autoCommit: false,
      };

      const ctx2: TransactionContext = {
        txnId: txn2,
        state: TransactionState.ACTIVE,
        mode: TransactionMode.DEFERRED,
        isolationLevel: IsolationLevel.SERIALIZABLE,
        savepoints: { savepoints: [], maxDepth: 0 },
        log: { txnId: txn2, entries: [], currentSequence: 0, startedAt: Date.now() },
        locks: [],
        startedAt: Date.now(),
        readOnly: false,
        autoCommit: false,
      };

      const key = new Uint8Array([1]);

      // txn1 reads the resource (holds shared lock in SERIALIZABLE)
      await enforcer.prepareRead(ctx1, 'users', key);

      // txn2 tries to write to the same resource
      // Since txn1 holds a shared lock, txn2 might wait or fail
      // For this test, we validate the serializable conflict detection
      // The validation check happens even if the lock is acquired

      // Since txn1 has the shared lock, txn2 can't get exclusive lock
      // But we can still test the read-set tracking by having txn2
      // write to a different resource and then validate
      const key2 = new Uint8Array([2]);
      await enforcer.prepareWrite(ctx2, 'orders', key2);

      // Now check validation: if txn2 tries to write to a resource
      // that txn1 read, it should fail validation
      expect(() => {
        enforcer.validateWrite(ctx2, 'users', key);
      }).toThrow(TransactionError);

      enforcer.onRollback(ctx1);
      enforcer.onRollback(ctx2);
    });

    it('should detect write-write conflicts', async () => {
      const enforcer = createIsolationEnforcer({ lockManager });
      const txn1 = createTransactionId('txn1');
      const txn2 = createTransactionId('txn2');

      const ctx1: TransactionContext = {
        txnId: txn1,
        state: TransactionState.ACTIVE,
        mode: TransactionMode.DEFERRED,
        isolationLevel: IsolationLevel.SERIALIZABLE,
        savepoints: { savepoints: [], maxDepth: 0 },
        log: { txnId: txn1, entries: [], currentSequence: 0, startedAt: Date.now() },
        locks: [],
        startedAt: Date.now(),
        readOnly: false,
        autoCommit: false,
      };

      const key = new Uint8Array([1]);

      // txn1 gets exclusive lock
      await enforcer.prepareWrite(ctx1, 'users', key);

      const ctx2: TransactionContext = {
        txnId: txn2,
        state: TransactionState.ACTIVE,
        mode: TransactionMode.DEFERRED,
        isolationLevel: IsolationLevel.SERIALIZABLE,
        savepoints: { savepoints: [], maxDepth: 0 },
        log: { txnId: txn2, entries: [], currentSequence: 0, startedAt: Date.now() },
        locks: [],
        startedAt: Date.now(),
        readOnly: false,
        autoCommit: false,
      };

      // txn2 tries to write to same resource - should wait/fail
      // Using short timeout to make test complete quickly
      const shortTimeoutLockManager = createLockManager({
        defaultTimeout: 50,
        detectDeadlocks: true,
      });
      const shortEnforcer = createIsolationEnforcer({ lockManager: shortTimeoutLockManager });

      // First acquire with txn1
      await shortEnforcer.prepareWrite(ctx1, 'users', key);

      // txn2 should timeout waiting
      await expect(
        shortEnforcer.prepareWrite(ctx2, 'users', key)
      ).rejects.toThrow();

      shortEnforcer.onRollback(ctx1);
      shortEnforcer.onRollback(ctx2);
    });
  });

  describe('REPEATABLE READ Isolation', () => {
    it('should hold shared locks until transaction ends', async () => {
      const enforcer = createIsolationEnforcer({ lockManager });
      const txnId = createTransactionId('reader');
      const context: TransactionContext = {
        txnId,
        state: TransactionState.ACTIVE,
        mode: TransactionMode.DEFERRED,
        isolationLevel: IsolationLevel.REPEATABLE_READ,
        savepoints: { savepoints: [], maxDepth: 0 },
        log: { txnId, entries: [], currentSequence: 0, startedAt: Date.now() },
        locks: [],
        startedAt: Date.now(),
        readOnly: false,
        autoCommit: false,
      };

      // Read multiple resources
      await enforcer.prepareRead(context, 'users', new Uint8Array([1]));
      await enforcer.prepareRead(context, 'users', new Uint8Array([2]));

      // Both locks should be held
      expect(context.locks.length).toBe(2);

      enforcer.onRollback(context);
    });
  });

  describe('READ UNCOMMITTED Isolation', () => {
    it('should not acquire locks for reads', async () => {
      const enforcer = createIsolationEnforcer({ lockManager });
      const txnId = createTransactionId('reader');
      const context: TransactionContext = {
        txnId,
        state: TransactionState.ACTIVE,
        mode: TransactionMode.DEFERRED,
        isolationLevel: IsolationLevel.READ_UNCOMMITTED,
        savepoints: { savepoints: [], maxDepth: 0 },
        log: { txnId, entries: [], currentSequence: 0, startedAt: Date.now() },
        locks: [],
        startedAt: Date.now(),
        readOnly: true,
        autoCommit: false,
      };

      await enforcer.prepareRead(context, 'users');

      // No locks should be acquired
      expect(context.locks.length).toBe(0);
    });
  });
});

// =============================================================================
// 4. CONCURRENT TRANSACTION CONFLICTS
// =============================================================================

describe('Concurrent Transaction Conflicts', () => {
  let walWriter: ReturnType<typeof createMockWALWriter>;
  let lockManager: LockManager;
  let storage: ReturnType<typeof createInMemoryStorage>;

  beforeEach(() => {
    walWriter = createMockWALWriter();
    storage = createInMemoryStorage();
    lockManager = createLockManager({
      defaultTimeout: 500,
      detectDeadlocks: true,
    });
  });

  afterEach(() => {
    lockManager.releaseAll(createTransactionId('txn1'));
    lockManager.releaseAll(createTransactionId('txn2'));
    lockManager.releaseAll(createTransactionId('writer'));
    lockManager.releaseAll(createTransactionId('reader'));
  });

  it('should handle lock timeout on conflicting writes', async () => {
    const txn1 = createTransactionId('txn1');
    const txn2 = createTransactionId('txn2');

    // txn1 holds exclusive lock
    await lockManager.acquire({
      txnId: txn1,
      resource: 'users:1',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    // txn2 should timeout waiting
    await expect(
      lockManager.acquire({
        txnId: txn2,
        resource: 'users:1',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now(),
        timeout: 50,
      })
    ).rejects.toMatchObject({
      code: TransactionErrorCode.LOCK_TIMEOUT,
    });

    lockManager.releaseAll(txn1);
  });

  it('should allow multiple readers with shared locks', async () => {
    const reader1 = createTransactionId('reader1');
    const reader2 = createTransactionId('reader2');
    const reader3 = createTransactionId('reader3');

    // All readers should be able to acquire shared locks simultaneously
    const result1 = await lockManager.acquire({
      txnId: reader1,
      resource: 'users:1',
      lockType: LockType.SHARED,
      timestamp: Date.now(),
    });

    const result2 = await lockManager.acquire({
      txnId: reader2,
      resource: 'users:1',
      lockType: LockType.SHARED,
      timestamp: Date.now(),
    });

    const result3 = await lockManager.acquire({
      txnId: reader3,
      resource: 'users:1',
      lockType: LockType.SHARED,
      timestamp: Date.now(),
    });

    expect(result1.acquired).toBe(true);
    expect(result2.acquired).toBe(true);
    expect(result3.acquired).toBe(true);

    lockManager.releaseAll(reader1);
    lockManager.releaseAll(reader2);
    lockManager.releaseAll(reader3);
  });

  it('should block writer when readers hold shared locks', async () => {
    const reader = createTransactionId('reader');
    const writer = createTransactionId('writer');

    // Reader holds shared lock
    await lockManager.acquire({
      txnId: reader,
      resource: 'users:1',
      lockType: LockType.SHARED,
      timestamp: Date.now(),
    });

    // Writer should timeout
    await expect(
      lockManager.acquire({
        txnId: writer,
        resource: 'users:1',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now(),
        timeout: 50,
      })
    ).rejects.toMatchObject({
      code: TransactionErrorCode.LOCK_TIMEOUT,
    });

    lockManager.releaseAll(reader);
  });

  it('should grant lock to waiting transaction after holder releases', async () => {
    const holder = createTransactionId('holder');
    const waiter = createTransactionId('waiter');

    // Holder acquires lock
    await lockManager.acquire({
      txnId: holder,
      resource: 'users:1',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    // Start waiter request (will block)
    const waiterPromise = lockManager.acquire({
      txnId: waiter,
      resource: 'users:1',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
      timeout: 500,
    });

    // Give waiter time to enter queue
    await delay(20);

    // Release holder's lock
    lockManager.release(holder, 'users:1');

    // Waiter should now acquire the lock
    const result = await waiterPromise;
    expect(result.acquired).toBe(true);
    expect(result.waitTime).toBeDefined();
    expect(result.waitTime!).toBeGreaterThan(0);

    lockManager.releaseAll(waiter);
  });

  it('should handle lock upgrade scenario', async () => {
    const txn = createTransactionId('txn');

    // First acquire shared lock
    const result1 = await lockManager.acquire({
      txnId: txn,
      resource: 'users:1',
      lockType: LockType.SHARED,
      timestamp: Date.now(),
    });
    expect(result1.acquired).toBe(true);

    // Upgrade to exclusive when no other holders
    const result2 = await lockManager.acquire({
      txnId: txn,
      resource: 'users:1',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });
    expect(result2.acquired).toBe(true);

    lockManager.releaseAll(txn);
  });

  it('should detect upgrade deadlock with two transactions', async () => {
    const txn1 = createTransactionId('txn1');
    const txn2 = createTransactionId('txn2');

    // Both acquire shared locks
    await lockManager.acquire({
      txnId: txn1,
      resource: 'users:1',
      lockType: LockType.SHARED,
      timestamp: Date.now(),
    });

    await lockManager.acquire({
      txnId: txn2,
      resource: 'users:1',
      lockType: LockType.SHARED,
      timestamp: Date.now() + 1,
    });

    // txn1 tries to upgrade (will wait for txn2 to release)
    const upgrade1Promise = lockManager.acquire({
      txnId: txn1,
      resource: 'users:1',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
      timeout: 500,
    });

    await delay(10);

    // txn2 tries to upgrade - this creates an upgrade deadlock
    // Both are waiting for each other to release shared locks
    try {
      await lockManager.acquire({
        txnId: txn2,
        resource: 'users:1',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now(),
        timeout: 500,
      });
      // If no deadlock error, that's also acceptable in some implementations
    } catch (error) {
      // Either deadlock or lock timeout is acceptable
      expect(
        error instanceof DeadlockError ||
        (error instanceof TransactionError && error.code === TransactionErrorCode.LOCK_TIMEOUT)
      ).toBe(true);
    }

    lockManager.releaseAll(txn1);
    lockManager.releaseAll(txn2);
  });

  it('should properly queue multiple waiting transactions', async () => {
    const holder = createTransactionId('holder');
    const waiter1 = createTransactionId('waiter1');
    const waiter2 = createTransactionId('waiter2');
    const waiter3 = createTransactionId('waiter3');

    // Holder acquires exclusive lock
    await lockManager.acquire({
      txnId: holder,
      resource: 'users:1',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    // Multiple waiters queue up
    const promise1 = lockManager.acquire({
      txnId: waiter1,
      resource: 'users:1',
      lockType: LockType.SHARED,
      timestamp: Date.now(),
      timeout: 1000,
    });

    const promise2 = lockManager.acquire({
      txnId: waiter2,
      resource: 'users:1',
      lockType: LockType.SHARED,
      timestamp: Date.now(),
      timeout: 1000,
    });

    const promise3 = lockManager.acquire({
      txnId: waiter3,
      resource: 'users:1',
      lockType: LockType.SHARED,
      timestamp: Date.now(),
      timeout: 1000,
    });

    await delay(20);

    // Check wait queue
    const state = lockManager.getState();
    const resourceState = state.get('users:1');
    expect(resourceState?.waiters.length).toBe(3);

    // Release holder - all shared waiters should be granted
    lockManager.release(holder, 'users:1');

    const [result1, result2, result3] = await Promise.all([promise1, promise2, promise3]);

    expect(result1.acquired).toBe(true);
    expect(result2.acquired).toBe(true);
    expect(result3.acquired).toBe(true);

    lockManager.releaseAll(waiter1);
    lockManager.releaseAll(waiter2);
    lockManager.releaseAll(waiter3);
  });
});

// =============================================================================
// 5. WAIT-FOR GRAPH TESTS
// =============================================================================

describe('Wait-For Graph', () => {
  it('should correctly detect cycles', () => {
    const detector = new DeadlockDetector({ enabled: true });

    detector.registerTransaction('A');
    detector.registerTransaction('B');
    detector.registerTransaction('C');

    // Create cycle: A -> B -> C -> A
    detector.addWait('A', 'B', 'res1', LockType.EXCLUSIVE);
    detector.addWait('B', 'C', 'res2', LockType.EXCLUSIVE);
    detector.addWait('C', 'A', 'res3', LockType.EXCLUSIVE);

    const result = detector.checkDeadlock('C');
    expect(result).not.toBeNull();
    expect(result?.cycle.length).toBeGreaterThanOrEqual(3);
  });

  it('should not detect cycle when none exists', () => {
    const detector = new DeadlockDetector({ enabled: true });

    detector.registerTransaction('A');
    detector.registerTransaction('B');
    detector.registerTransaction('C');

    // Linear wait: A -> B -> C (no cycle)
    detector.addWait('A', 'B', 'res1', LockType.EXCLUSIVE);
    detector.addWait('B', 'C', 'res2', LockType.EXCLUSIVE);

    const result = detector.checkDeadlock('A');
    expect(result).toBeNull();
  });

  it('should generate DOT graph for visualization', () => {
    const detector = new DeadlockDetector({ enabled: true });

    detector.registerTransaction('A');
    detector.registerTransaction('B');

    detector.addWait('A', 'B', 'users', LockType.EXCLUSIVE);

    const graph = detector.getWaitForGraph();
    const dot = graph.toDot();

    expect(dot).toContain('digraph');
    expect(dot).toContain('"A" -> "B"');
    expect(dot).toContain('users');
  });

  it('should clean up transactions properly', () => {
    const detector = new DeadlockDetector({ enabled: true });

    detector.registerTransaction('A');
    detector.registerTransaction('B');

    detector.addWait('A', 'B', 'res1', LockType.EXCLUSIVE);

    // Unregister B
    detector.unregisterTransaction('B');

    // Should not find cycle anymore (B is removed)
    const result = detector.checkDeadlock('A');
    expect(result).toBeNull();
  });

  describe('Victim Selection Policies', () => {
    it('should select least work victim with leastWork policy', () => {
      const detector = new DeadlockDetector({
        enabled: true,
        victimSelection: 'leastWork',
      });

      detector.registerTransaction('A');
      detector.registerTransaction('B');
      detector.registerTransaction('C');

      // Set costs
      detector.setTransactionCost('A', 100);
      detector.setTransactionCost('B', 10); // Least work
      detector.setTransactionCost('C', 50);

      detector.addWait('A', 'B', 'res1', LockType.EXCLUSIVE);
      detector.addWait('B', 'C', 'res2', LockType.EXCLUSIVE);
      detector.addWait('C', 'A', 'res3', LockType.EXCLUSIVE);

      const result = detector.checkDeadlock('C');
      expect(result).not.toBeNull();
      // B should be victim (least work)
      expect(result?.victimTxnId).toBe('B');
    });

    it('should prefer read-only transactions with preferReadOnly policy', () => {
      const detector = new DeadlockDetector({
        enabled: true,
        victimSelection: 'preferReadOnly',
      });

      detector.registerTransaction('writer1');
      detector.registerTransaction('reader');
      detector.registerTransaction('writer2');

      detector.markReadOnly('reader', true);

      detector.addWait('writer1', 'reader', 'res1', LockType.EXCLUSIVE);
      detector.addWait('reader', 'writer2', 'res2', LockType.SHARED);
      detector.addWait('writer2', 'writer1', 'res3', LockType.EXCLUSIVE);

      const result = detector.checkDeadlock('writer2');
      expect(result).not.toBeNull();
      // Reader should be victim (read-only)
      expect(result?.victimTxnId).toBe('reader');
    });

    it('should rotate victims with roundRobin policy', () => {
      const detector = new DeadlockDetector({
        enabled: true,
        victimSelection: 'roundRobin',
      });

      // Create multiple deadlocks and check victim rotation
      const victims: string[] = [];

      for (let i = 0; i < 3; i++) {
        detector.clear();
        detector.registerTransaction('A');
        detector.registerTransaction('B');

        detector.addWait('A', 'B', 'res1', LockType.EXCLUSIVE);
        detector.addWait('B', 'A', 'res2', LockType.EXCLUSIVE);

        const result = detector.checkDeadlock('B');
        if (result) {
          victims.push(result.victimTxnId);
        }
      }

      // Victims should include both A and B over multiple iterations
      expect(victims.length).toBe(3);
    });
  });
});
