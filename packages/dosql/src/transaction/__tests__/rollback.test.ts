/**
 * Rollback Logic Tests - TDD RED Phase
 *
 * These tests verify that all rollback paths behave identically and that
 * the refactored unified rollback implementation handles all cases correctly.
 *
 * Issue: sql-vv1w - Extract duplicated rollback logic (DRY)
 *
 * Tests verify:
 * 1. Basic rollback reverts all changes
 * 2. Rollback to savepoint reverts only changes after savepoint
 * 3. Rollback with pending locks releases them
 * 4. Rollback updates WAL correctly
 * 5. Timeout-triggered rollback behaves identically to manual rollback
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  createTransactionManager,
  type ExtendedTransactionManager,
} from '../manager.js';
import { createLockManager, type LockManager } from '../isolation.js';
import {
  TransactionState,
  LockType,
  TransactionError,
  TransactionErrorCode,
  createTransactionId,
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
    // Test helper to retrieve entries
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
    getAll(table: string): Array<{ key: Uint8Array; value: Uint8Array }> {
      const tableData = storage.get(table);
      if (!tableData) return [];
      return Array.from(tableData.entries()).map(([k, v]) => ({
        key: new Uint8Array(JSON.parse(k)),
        value: v,
      }));
    },
    clear(): void {
      storage.clear();
    },
  };
}

// =============================================================================
// 1. BASIC ROLLBACK REVERTS ALL CHANGES
// =============================================================================

describe('Basic Rollback', () => {
  let manager: ExtendedTransactionManager;
  let walWriter: ReturnType<typeof createMockWALWriter>;
  let storage: ReturnType<typeof createInMemoryStorage>;

  beforeEach(() => {
    walWriter = createMockWALWriter();
    storage = createInMemoryStorage();
    manager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 30000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 5000,
        warningThresholdMs: 25000,
      },
    });

    // Set up apply function that modifies in-memory storage
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

  it('should revert all INSERT operations on rollback', async () => {
    await manager.begin();

    // Simulate INSERT operations
    const key1 = new Uint8Array([1]);
    const key2 = new Uint8Array([2]);
    const value1 = new Uint8Array([10, 20, 30]);
    const value2 = new Uint8Array([40, 50, 60]);

    // Log INSERTs (normally the storage would be updated by the executor)
    storage.set('users', key1, value1);
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: key1,
      afterValue: value1,
    });

    storage.set('users', key2, value2);
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: key2,
      afterValue: value2,
    });

    // Verify data exists before rollback
    expect(storage.get('users', key1)).toEqual(value1);
    expect(storage.get('users', key2)).toEqual(value2);

    // Rollback should revert INSERTs by deleting
    await manager.rollback();

    // Data should be gone
    expect(storage.get('users', key1)).toBeUndefined();
    expect(storage.get('users', key2)).toBeUndefined();
  });

  it('should revert UPDATE operations to previous values on rollback', async () => {
    // Pre-existing data
    const key = new Uint8Array([1]);
    const originalValue = new Uint8Array([10, 20, 30]);
    const updatedValue = new Uint8Array([99, 99, 99]);
    storage.set('users', key, originalValue);

    await manager.begin();

    // Simulate UPDATE
    storage.set('users', key, updatedValue);
    manager.logOperation({
      op: 'UPDATE',
      table: 'users',
      key: key,
      beforeValue: originalValue,
      afterValue: updatedValue,
    });

    // Verify updated value
    expect(storage.get('users', key)).toEqual(updatedValue);

    // Rollback should restore original
    await manager.rollback();

    expect(storage.get('users', key)).toEqual(originalValue);
  });

  it('should revert DELETE operations by restoring data on rollback', async () => {
    // Pre-existing data
    const key = new Uint8Array([1]);
    const value = new Uint8Array([10, 20, 30]);
    storage.set('users', key, value);

    await manager.begin();

    // Simulate DELETE
    storage.delete('users', key);
    manager.logOperation({
      op: 'DELETE',
      table: 'users',
      key: key,
      beforeValue: value,
    });

    // Verify data is deleted
    expect(storage.get('users', key)).toBeUndefined();

    // Rollback should restore
    await manager.rollback();

    expect(storage.get('users', key)).toEqual(value);
  });

  it('should handle mixed operations correctly on rollback', async () => {
    const key1 = new Uint8Array([1]);
    const key2 = new Uint8Array([2]);
    const key3 = new Uint8Array([3]);
    const value1 = new Uint8Array([10]);
    const value2 = new Uint8Array([20]);
    const value3 = new Uint8Array([30]);
    const value2Updated = new Uint8Array([99]);

    // Pre-existing data for key2 and key3
    storage.set('users', key2, value2);
    storage.set('users', key3, value3);

    await manager.begin();

    // INSERT key1
    storage.set('users', key1, value1);
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: key1,
      afterValue: value1,
    });

    // UPDATE key2
    storage.set('users', key2, value2Updated);
    manager.logOperation({
      op: 'UPDATE',
      table: 'users',
      key: key2,
      beforeValue: value2,
      afterValue: value2Updated,
    });

    // DELETE key3
    storage.delete('users', key3);
    manager.logOperation({
      op: 'DELETE',
      table: 'users',
      key: key3,
      beforeValue: value3,
    });

    // Rollback all changes
    await manager.rollback();

    // key1 should be gone (INSERT reverted)
    expect(storage.get('users', key1)).toBeUndefined();
    // key2 should have original value (UPDATE reverted)
    expect(storage.get('users', key2)).toEqual(value2);
    // key3 should exist again (DELETE reverted)
    expect(storage.get('users', key3)).toEqual(value3);
  });

  it('should apply rollback operations in reverse order', async () => {
    const key = new Uint8Array([1]);
    const v1 = new Uint8Array([1]);
    const v2 = new Uint8Array([2]);
    const v3 = new Uint8Array([3]);

    await manager.begin();

    // INSERT with v1
    storage.set('users', key, v1);
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: key,
      afterValue: v1,
    });

    // UPDATE to v2
    storage.set('users', key, v2);
    manager.logOperation({
      op: 'UPDATE',
      table: 'users',
      key: key,
      beforeValue: v1,
      afterValue: v2,
    });

    // UPDATE to v3
    storage.set('users', key, v3);
    manager.logOperation({
      op: 'UPDATE',
      table: 'users',
      key: key,
      beforeValue: v2,
      afterValue: v3,
    });

    // Rollback should undo v3->v2, v2->v1, then delete (reverse INSERT)
    await manager.rollback();

    // After INSERT rollback, key should not exist
    expect(storage.get('users', key)).toBeUndefined();
  });
});

// =============================================================================
// 2. ROLLBACK TO SAVEPOINT REVERTS ONLY CHANGES AFTER SAVEPOINT
// =============================================================================

describe('Rollback to Savepoint', () => {
  let manager: ExtendedTransactionManager;
  let walWriter: ReturnType<typeof createMockWALWriter>;
  let storage: ReturnType<typeof createInMemoryStorage>;

  beforeEach(() => {
    walWriter = createMockWALWriter();
    storage = createInMemoryStorage();
    manager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 30000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 5000,
        warningThresholdMs: 25000,
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

  it('should keep changes before savepoint on rollbackTo', async () => {
    const key1 = new Uint8Array([1]);
    const key2 = new Uint8Array([2]);
    const value1 = new Uint8Array([10]);
    const value2 = new Uint8Array([20]);

    await manager.begin();

    // INSERT key1 before savepoint
    storage.set('users', key1, value1);
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: key1,
      afterValue: value1,
    });

    // Create savepoint
    await manager.savepoint('sp1');

    // INSERT key2 after savepoint
    storage.set('users', key2, value2);
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: key2,
      afterValue: value2,
    });

    // Rollback to savepoint
    await manager.rollbackTo('sp1');

    // key1 should still exist (before savepoint)
    expect(storage.get('users', key1)).toEqual(value1);
    // key2 should be gone (after savepoint)
    expect(storage.get('users', key2)).toBeUndefined();
  });

  it('should revert only operations after savepoint', async () => {
    const key = new Uint8Array([1]);
    const v1 = new Uint8Array([1]);
    const v2 = new Uint8Array([2]);
    const v3 = new Uint8Array([3]);

    await manager.begin();

    // INSERT
    storage.set('users', key, v1);
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: key,
      afterValue: v1,
    });

    // UPDATE to v2
    storage.set('users', key, v2);
    manager.logOperation({
      op: 'UPDATE',
      table: 'users',
      key: key,
      beforeValue: v1,
      afterValue: v2,
    });

    // Create savepoint at v2
    await manager.savepoint('sp1');

    // UPDATE to v3 after savepoint
    storage.set('users', key, v3);
    manager.logOperation({
      op: 'UPDATE',
      table: 'users',
      key: key,
      beforeValue: v2,
      afterValue: v3,
    });

    // Rollback to savepoint
    await manager.rollbackTo('sp1');

    // Should be back to v2 (the state at savepoint)
    expect(storage.get('users', key)).toEqual(v2);
  });

  it('should handle nested savepoints correctly', async () => {
    const key = new Uint8Array([1]);
    const v1 = new Uint8Array([1]);
    const v2 = new Uint8Array([2]);
    const v3 = new Uint8Array([3]);
    const v4 = new Uint8Array([4]);

    await manager.begin();

    // INSERT
    storage.set('users', key, v1);
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: key,
      afterValue: v1,
    });

    await manager.savepoint('sp1');

    // UPDATE to v2
    storage.set('users', key, v2);
    manager.logOperation({
      op: 'UPDATE',
      table: 'users',
      key: key,
      beforeValue: v1,
      afterValue: v2,
    });

    await manager.savepoint('sp2');

    // UPDATE to v3
    storage.set('users', key, v3);
    manager.logOperation({
      op: 'UPDATE',
      table: 'users',
      key: key,
      beforeValue: v2,
      afterValue: v3,
    });

    await manager.savepoint('sp3');

    // UPDATE to v4
    storage.set('users', key, v4);
    manager.logOperation({
      op: 'UPDATE',
      table: 'users',
      key: key,
      beforeValue: v3,
      afterValue: v4,
    });

    // Rollback to sp2 should revert v4->v3 and v3->v2, leaving v2
    await manager.rollbackTo('sp2');
    expect(storage.get('users', key)).toEqual(v2);

    // sp3 should be gone, can't rollback to it
    await expect(manager.rollbackTo('sp3')).rejects.toThrow();

    // Rollback to sp1 should revert v2->v1
    await manager.rollbackTo('sp1');
    expect(storage.get('users', key)).toEqual(v1);
  });

  it('should truncate transaction log to savepoint index', async () => {
    await manager.begin();

    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: new Uint8Array([1]),
      afterValue: new Uint8Array([1]),
    });

    await manager.savepoint('sp1');

    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: new Uint8Array([2]),
      afterValue: new Uint8Array([2]),
    });

    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: new Uint8Array([3]),
      afterValue: new Uint8Array([3]),
    });

    // Log has 4 entries: INSERT, SAVEPOINT, INSERT, INSERT
    const contextBefore = manager.getContext();
    expect(contextBefore?.log.entries.length).toBe(4);

    await manager.rollbackTo('sp1');

    // Log should be truncated to savepoint index (2 entries: INSERT, SAVEPOINT, ROLLBACK_TO)
    // Note: rollbackTo adds a ROLLBACK_TO entry
    const contextAfter = manager.getContext();
    expect(contextAfter?.log.entries.length).toBe(3);
    expect(contextAfter?.log.entries[2].op).toBe('ROLLBACK_TO');
  });
});

// =============================================================================
// 3. ROLLBACK WITH PENDING LOCKS RELEASES THEM
// =============================================================================

describe('Rollback with Locks', () => {
  let manager: ExtendedTransactionManager;
  let walWriter: ReturnType<typeof createMockWALWriter>;
  let lockManager: LockManager;

  beforeEach(() => {
    walWriter = createMockWALWriter();
    lockManager = createLockManager({
      defaultTimeout: 5000,
      detectDeadlocks: true,
    });
    manager = createTransactionManager({
      walWriter,
      lockManager,
      timeoutConfig: {
        defaultTimeoutMs: 30000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 5000,
        warningThresholdMs: 25000,
      },
    });

    manager.setApplyFunction(async () => {});
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

  it('should release all locks on rollback', async () => {
    const ctx = await manager.begin();

    // Acquire some locks
    await lockManager.acquire({
      txnId: ctx.txnId,
      resource: 'users',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    await lockManager.acquire({
      txnId: ctx.txnId,
      resource: 'orders',
      lockType: LockType.SHARED,
      timestamp: Date.now(),
    });

    // Verify locks are held
    expect(lockManager.getHeldLocks(ctx.txnId)).toHaveLength(2);

    // Rollback should release locks
    await manager.rollback();

    // Locks should be released
    expect(lockManager.getHeldLocks(ctx.txnId)).toHaveLength(0);
  });

  it('should release locks on rollback even if undo operations fail', async () => {
    const ctx = await manager.begin();

    // Set apply function that fails
    manager.setApplyFunction(async () => {
      throw new Error('Simulated undo failure');
    });

    // Log an operation
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: new Uint8Array([1]),
      afterValue: new Uint8Array([1]),
    });

    // Acquire lock
    await lockManager.acquire({
      txnId: ctx.txnId,
      resource: 'users',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    // Rollback should still release locks even though undo failed
    await expect(manager.rollback()).rejects.toThrow();

    // Locks should still be released (cleanup happens regardless)
    expect(lockManager.getHeldLocks(ctx.txnId)).toHaveLength(0);
  });

  it('should allow other transactions to acquire locks after rollback', async () => {
    const ctx = await manager.begin();

    // Acquire exclusive lock
    await lockManager.acquire({
      txnId: ctx.txnId,
      resource: 'users',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    await manager.rollback();

    // Another transaction should be able to acquire the same lock
    const otherTxnId = createTransactionId('other-txn');
    const result = await lockManager.acquire({
      txnId: otherTxnId,
      resource: 'users',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    expect(result.acquired).toBe(true);
    lockManager.releaseAll(otherTxnId);
  });
});

// =============================================================================
// 4. ROLLBACK UPDATES WAL CORRECTLY
// =============================================================================

describe('Rollback WAL Updates', () => {
  let manager: ExtendedTransactionManager;
  let walWriter: ReturnType<typeof createMockWALWriter>;

  beforeEach(() => {
    walWriter = createMockWALWriter();
    manager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 30000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 5000,
        warningThresholdMs: 25000,
      },
    });

    manager.setApplyFunction(async () => {});
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

  it('should write ROLLBACK entry to WAL on rollback', async () => {
    const ctx = await manager.begin();
    walWriter.clearEntries();

    await manager.rollback();

    const entries = walWriter.getEntries();
    expect(entries.some((e) => e.op === 'ROLLBACK' && e.txnId === ctx.txnId)).toBe(true);
  });

  it('should write BEGIN and ROLLBACK in correct order', async () => {
    await manager.begin();
    await manager.rollback();

    const entries = walWriter.getEntries();
    const beginIndex = entries.findIndex((e) => e.op === 'BEGIN');
    const rollbackIndex = entries.findIndex((e) => e.op === 'ROLLBACK');

    expect(beginIndex).toBeLessThan(rollbackIndex);
  });

  it('should not write to WAL for rollbackTo (savepoint rollback)', async () => {
    await manager.begin();
    await manager.savepoint('sp1');
    walWriter.clearEntries();

    await manager.rollbackTo('sp1');

    // rollbackTo should not write to WAL (savepoint is in-memory only)
    const entries = walWriter.getEntries();
    expect(entries.filter((e) => e.op === 'ROLLBACK')).toHaveLength(0);
  });

  it('should continue rollback even if WAL write fails', async () => {
    // Create manager with failing WAL writer
    const failingWalWriter = {
      ...createMockWALWriter(),
      async append(entry: { txnId: string; op: string }) {
        if (entry.op === 'ROLLBACK') {
          throw new Error('WAL write failure');
        }
        return { lsn: 0n, flushed: true };
      },
    };

    const failingManager = createTransactionManager({
      walWriter: failingWalWriter as ReturnType<typeof createMockWALWriter>,
      timeoutConfig: {
        defaultTimeoutMs: 30000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 5000,
        warningThresholdMs: 25000,
      },
    });

    failingManager.setApplyFunction(async () => {});
    await failingManager.begin();

    // Rollback should not throw even if WAL write fails
    await expect(failingManager.rollback()).resolves.not.toThrow();

    // Transaction should be inactive
    expect(failingManager.isActive()).toBe(false);
  });
});

// =============================================================================
// 5. TIMEOUT-TRIGGERED ROLLBACK BEHAVES IDENTICALLY TO MANUAL ROLLBACK
// =============================================================================

describe('Timeout-Triggered Rollback Consistency', () => {
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
  });

  it('should revert changes identically when triggered by timeout', async () => {
    const manager = createTransactionManager({
      walWriter,
      lockManager,
      timeoutConfig: {
        defaultTimeoutMs: 100,
        maxTimeoutMs: 60000,
        gracePeriodMs: 20,
        warningThresholdMs: 80,
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

    const ctx = await manager.begin();

    const key = new Uint8Array([1]);
    const value = new Uint8Array([10, 20, 30]);

    // INSERT
    storage.set('users', key, value);
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: key,
      afterValue: value,
    });

    // Acquire lock
    await lockManager.acquire({
      txnId: ctx.txnId,
      resource: 'users',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    // Wait for timeout to trigger automatic rollback
    await delay(150);

    // Transaction should be inactive
    expect(manager.isActive()).toBe(false);

    // Data should be reverted (INSERT undone by DELETE)
    expect(storage.get('users', key)).toBeUndefined();

    // Locks should be released
    expect(lockManager.getHeldLocks(ctx.txnId)).toHaveLength(0);
  });

  it('should write ROLLBACK to WAL when triggered by timeout', async () => {
    const manager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 100,
        maxTimeoutMs: 60000,
        gracePeriodMs: 20,
        warningThresholdMs: 80,
      },
    });

    manager.setApplyFunction(async () => {});
    const ctx = await manager.begin();
    walWriter.clearEntries();

    // Wait for timeout
    await delay(150);

    // WAL should have ROLLBACK entry
    const entries = walWriter.getEntries();
    expect(entries.some((e) => e.op === 'ROLLBACK' && e.txnId === ctx.txnId)).toBe(true);
  });

  it('should clean up transaction state identically for manual and timeout rollback', async () => {
    // Test manual rollback first
    const manualManager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 30000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 5000,
        warningThresholdMs: 25000,
      },
    });
    manualManager.setApplyFunction(async () => {});

    await manualManager.begin();
    await manualManager.rollback();

    expect(manualManager.isActive()).toBe(false);
    expect(manualManager.getContext()).toBeNull();
    expect(manualManager.getState()).toBe(TransactionState.NONE);

    // Test timeout rollback
    const timeoutManager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 100,
        maxTimeoutMs: 60000,
        gracePeriodMs: 20,
        warningThresholdMs: 80,
      },
    });
    timeoutManager.setApplyFunction(async () => {});

    await timeoutManager.begin();
    await delay(150);

    expect(timeoutManager.isActive()).toBe(false);
    expect(timeoutManager.getContext()).toBeNull();
    expect(timeoutManager.getState()).toBe(TransactionState.NONE);
  });
});

// =============================================================================
// 6. EDGE CASES AND ERROR HANDLING
// =============================================================================

describe('Rollback Edge Cases', () => {
  let manager: ExtendedTransactionManager;
  let walWriter: ReturnType<typeof createMockWALWriter>;

  beforeEach(() => {
    walWriter = createMockWALWriter();
    manager = createTransactionManager({
      walWriter,
      timeoutConfig: {
        defaultTimeoutMs: 30000,
        maxTimeoutMs: 60000,
        gracePeriodMs: 5000,
        warningThresholdMs: 25000,
      },
    });
    manager.setApplyFunction(async () => {});
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

  it('should throw NO_ACTIVE_TRANSACTION when rolling back without active transaction', async () => {
    await expect(manager.rollback()).rejects.toThrow(TransactionError);
    await expect(manager.rollback()).rejects.toMatchObject({
      code: TransactionErrorCode.NO_ACTIVE_TRANSACTION,
    });
  });

  it('should handle rollback with empty transaction log', async () => {
    await manager.begin();

    // No operations logged
    await expect(manager.rollback()).resolves.not.toThrow();
    expect(manager.isActive()).toBe(false);
  });

  it('should handle rollbackTo with no changes after savepoint', async () => {
    await manager.begin();

    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: new Uint8Array([1]),
      afterValue: new Uint8Array([1]),
    });

    await manager.savepoint('sp1');

    // No changes after savepoint
    await expect(manager.rollbackTo('sp1')).resolves.not.toThrow();

    // Should still be active
    expect(manager.isActive()).toBe(true);
  });

  it('should throw SAVEPOINT_NOT_FOUND for invalid savepoint name', async () => {
    await manager.begin();

    await expect(manager.rollbackTo('nonexistent')).rejects.toThrow(TransactionError);
    await expect(manager.rollbackTo('nonexistent')).rejects.toMatchObject({
      code: TransactionErrorCode.SAVEPOINT_NOT_FOUND,
    });
  });

  it('should clear transaction context immediately after rollback', async () => {
    await manager.begin();

    const contextBefore = manager.getContext();
    expect(contextBefore).not.toBeNull();

    await manager.rollback();

    const contextAfter = manager.getContext();
    expect(contextAfter).toBeNull();
  });

  it('should unregister from timeout enforcer on rollback', async () => {
    const ctx = await manager.begin();

    // Should have remaining time
    expect(manager.getRemainingTime(ctx.txnId)).toBeGreaterThan(0);

    await manager.rollback();

    // After rollback, remaining time should be 0 (unregistered)
    expect(manager.getRemainingTime(ctx.txnId)).toBe(0);
  });
});
