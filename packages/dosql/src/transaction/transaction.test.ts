/**
 * Transaction Tests for DoSQL
 *
 * Tests for SQLite-compatible transaction semantics:
 * - Basic BEGIN/COMMIT/ROLLBACK
 * - Savepoints and nested transactions
 * - Rollback recovery
 * - WAL integration
 * - Isolation levels
 * - Lock management
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  createTransactionManager,
  executeInTransaction,
  executeWithSavepoint,
  executeReadOnly,
  type TransactionManagerOptions,
} from './manager.js';
import {
  createLockManager,
  createMVCCStore,
  createIsolationEnforcer,
  createSnapshot,
  isVersionVisible,
} from './isolation.js';
import {
  TransactionState,
  TransactionMode,
  IsolationLevel,
  LockType,
  TransactionError,
  TransactionErrorCode,
  createTransactionLog,
  createSavepointStack,
  type ApplyFunction,
  type WALWriter,
  type RowVersion,
} from './types.js';

// =============================================================================
// Mock WAL Writer
// =============================================================================

function createMockWALWriter(): WALWriter & {
  entries: Array<{ op: string; txnId: string; lsn: bigint }>;
} {
  let currentLSN = 0n;
  const entries: Array<{ op: string; txnId: string; lsn: bigint }> = [];

  return {
    entries,
    async append(entry, options) {
      const lsn = currentLSN++;
      entries.push({
        op: entry.op,
        txnId: entry.txnId,
        lsn,
      });
      return { lsn, flushed: options?.sync ?? false };
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
// Basic Transaction Tests
// =============================================================================

describe('Transaction Manager', () => {
  describe('Basic BEGIN/COMMIT/ROLLBACK', () => {
    it('should begin a transaction', async () => {
      const manager = createTransactionManager();

      const context = await manager.begin();

      expect(context).toBeDefined();
      expect(context.txnId).toBeTruthy();
      expect(context.state).toBe(TransactionState.ACTIVE);
      expect(manager.isActive()).toBe(true);
      expect(manager.getState()).toBe(TransactionState.ACTIVE);
    });

    it('should commit a transaction', async () => {
      const walWriter = createMockWALWriter();
      const manager = createTransactionManager({ walWriter });

      await manager.begin();
      const commitLsn = await manager.commit();

      expect(commitLsn).toBeGreaterThanOrEqual(0n);
      expect(manager.isActive()).toBe(false);
      expect(manager.getState()).toBe(TransactionState.NONE);

      // Check WAL entries
      expect(walWriter.entries).toHaveLength(2);
      expect(walWriter.entries[0].op).toBe('BEGIN');
      expect(walWriter.entries[1].op).toBe('COMMIT');
    });

    it('should rollback a transaction', async () => {
      const walWriter = createMockWALWriter();
      const manager = createTransactionManager({ walWriter });

      await manager.begin();
      await manager.rollback();

      expect(manager.isActive()).toBe(false);
      expect(manager.getState()).toBe(TransactionState.NONE);

      // Check WAL entries
      expect(walWriter.entries).toHaveLength(2);
      expect(walWriter.entries[0].op).toBe('BEGIN');
      expect(walWriter.entries[1].op).toBe('ROLLBACK');
    });

    it('should throw when beginning a transaction while one is active', async () => {
      const manager = createTransactionManager();

      await manager.begin();

      await expect(manager.begin()).rejects.toThrow(TransactionError);
      await expect(manager.begin()).rejects.toMatchObject({
        code: TransactionErrorCode.TRANSACTION_ALREADY_ACTIVE,
      });
    });

    it('should throw when committing without active transaction', async () => {
      const manager = createTransactionManager();

      await expect(manager.commit()).rejects.toThrow(TransactionError);
      await expect(manager.commit()).rejects.toMatchObject({
        code: TransactionErrorCode.NO_ACTIVE_TRANSACTION,
      });
    });

    it('should throw when rolling back without active transaction', async () => {
      const manager = createTransactionManager();

      await expect(manager.rollback()).rejects.toThrow(TransactionError);
      await expect(manager.rollback()).rejects.toMatchObject({
        code: TransactionErrorCode.NO_ACTIVE_TRANSACTION,
      });
    });

    it('should support transaction modes', async () => {
      const manager = createTransactionManager();

      const context = await manager.begin({ mode: TransactionMode.IMMEDIATE });

      expect(context.mode).toBe(TransactionMode.IMMEDIATE);

      await manager.commit();
    });

    it('should support read-only transactions', async () => {
      const manager = createTransactionManager();

      const context = await manager.begin({ readOnly: true });

      expect(context.readOnly).toBe(true);

      // Logging a write operation should throw
      expect(() => {
        manager.logOperation({
          op: 'INSERT',
          table: 'users',
          afterValue: new Uint8Array([1, 2, 3]),
        });
      }).toThrow(TransactionError);

      await manager.rollback();
    });
  });

  describe('Transaction Log', () => {
    it('should log operations', async () => {
      const manager = createTransactionManager();

      await manager.begin();

      manager.logOperation({
        op: 'INSERT',
        table: 'users',
        afterValue: new Uint8Array([1, 2, 3]),
      });

      manager.logOperation({
        op: 'UPDATE',
        table: 'users',
        key: new Uint8Array([1]),
        beforeValue: new Uint8Array([1, 2, 3]),
        afterValue: new Uint8Array([4, 5, 6]),
      });

      const context = manager.getContext();
      expect(context?.log.entries).toHaveLength(2);
      expect(context?.log.entries[0].op).toBe('INSERT');
      expect(context?.log.entries[1].op).toBe('UPDATE');
      expect(context?.log.entries[0].sequence).toBe(0);
      expect(context?.log.entries[1].sequence).toBe(1);

      await manager.commit();
    });

    it('should throw when logging without active transaction', () => {
      const manager = createTransactionManager();

      expect(() => {
        manager.logOperation({
          op: 'INSERT',
          table: 'users',
          afterValue: new Uint8Array([1, 2, 3]),
        });
      }).toThrow(TransactionError);
    });
  });
});

// =============================================================================
// Savepoint Tests
// =============================================================================

describe('Savepoints', () => {
  it('should create a savepoint', async () => {
    const manager = createTransactionManager();

    await manager.begin();
    const savepoint = await manager.savepoint('sp1');

    expect(savepoint.name).toBe('sp1');
    expect(savepoint.depth).toBe(0);
    expect(manager.getState()).toBe(TransactionState.SAVEPOINT);

    await manager.commit();
  });

  it('should create nested savepoints', async () => {
    const manager = createTransactionManager();

    await manager.begin();
    const sp1 = await manager.savepoint('sp1');
    const sp2 = await manager.savepoint('sp2');
    const sp3 = await manager.savepoint('sp3');

    expect(sp1.depth).toBe(0);
    expect(sp2.depth).toBe(1);
    expect(sp3.depth).toBe(2);

    const context = manager.getContext();
    expect(context?.savepoints.savepoints).toHaveLength(3);
    expect(context?.savepoints.maxDepth).toBe(3);

    await manager.commit();
  });

  it('should throw on duplicate savepoint name', async () => {
    const manager = createTransactionManager();

    await manager.begin();
    await manager.savepoint('sp1');

    await expect(manager.savepoint('sp1')).rejects.toThrow(TransactionError);
    await expect(manager.savepoint('sp1')).rejects.toMatchObject({
      code: TransactionErrorCode.DUPLICATE_SAVEPOINT,
    });

    await manager.rollback();
  });

  it('should release a savepoint', async () => {
    const manager = createTransactionManager();

    await manager.begin();
    await manager.savepoint('sp1');

    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      afterValue: new Uint8Array([1]),
    });

    await manager.release('sp1');

    // Savepoint removed, but changes remain
    const context = manager.getContext();
    expect(context?.savepoints.savepoints).toHaveLength(0);
    expect(context?.log.entries.length).toBeGreaterThan(0);
    expect(manager.getState()).toBe(TransactionState.ACTIVE);

    await manager.commit();
  });

  it('should release nested savepoints', async () => {
    const manager = createTransactionManager();

    await manager.begin();
    await manager.savepoint('sp1');
    await manager.savepoint('sp2');
    await manager.savepoint('sp3');

    // Releasing sp1 should also remove sp2 and sp3
    await manager.release('sp1');

    const context = manager.getContext();
    expect(context?.savepoints.savepoints).toHaveLength(0);

    await manager.commit();
  });

  it('should throw when releasing non-existent savepoint', async () => {
    const manager = createTransactionManager();

    await manager.begin();

    await expect(manager.release('nonexistent')).rejects.toThrow(TransactionError);
    await expect(manager.release('nonexistent')).rejects.toMatchObject({
      code: TransactionErrorCode.SAVEPOINT_NOT_FOUND,
    });

    await manager.rollback();
  });

  it('should rollback to a savepoint', async () => {
    const manager = createTransactionManager();
    const appliedOps: Array<{ op: string; table: string }> = [];

    manager.setApplyFunction(async (op, table) => {
      appliedOps.push({ op, table });
    });

    await manager.begin();

    // Before savepoint
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: new Uint8Array([1]),
      afterValue: new Uint8Array([1]),
    });

    await manager.savepoint('sp1');

    // After savepoint
    manager.logOperation({
      op: 'INSERT',
      table: 'orders',
      key: new Uint8Array([2]),
      afterValue: new Uint8Array([2]),
    });

    manager.logOperation({
      op: 'UPDATE',
      table: 'orders',
      key: new Uint8Array([2]),
      beforeValue: new Uint8Array([2]),
      afterValue: new Uint8Array([3]),
    });

    // Rollback to savepoint
    await manager.rollbackTo('sp1');

    // Check that undo operations were applied in reverse order
    expect(appliedOps).toHaveLength(2);
    expect(appliedOps[0]).toEqual({ op: 'UPDATE', table: 'orders' }); // Undo UPDATE
    expect(appliedOps[1]).toEqual({ op: 'DELETE', table: 'orders' }); // Undo INSERT

    // Log should be truncated
    const context = manager.getContext();
    // +1 for SAVEPOINT entry, +1 for ROLLBACK_TO entry
    expect(context?.log.entries.filter(e => e.op === 'INSERT').length).toBe(1);

    await manager.commit();
  });

  it('should keep savepoint after rollback to it', async () => {
    const manager = createTransactionManager();
    manager.setApplyFunction(async () => {});

    await manager.begin();
    await manager.savepoint('sp1');

    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      afterValue: new Uint8Array([1]),
    });

    await manager.rollbackTo('sp1');

    // Savepoint should still exist
    const context = manager.getContext();
    expect(context?.savepoints.savepoints).toHaveLength(1);
    expect(context?.savepoints.savepoints[0].name).toBe('sp1');

    await manager.commit();
  });

  it('should throw when rolling back to non-existent savepoint', async () => {
    const manager = createTransactionManager();

    await manager.begin();

    await expect(manager.rollbackTo('nonexistent')).rejects.toThrow(TransactionError);
    await expect(manager.rollbackTo('nonexistent')).rejects.toMatchObject({
      code: TransactionErrorCode.SAVEPOINT_NOT_FOUND,
    });

    await manager.rollback();
  });
});

// =============================================================================
// Rollback Recovery Tests
// =============================================================================

describe('Rollback Recovery', () => {
  it('should apply undo operations on rollback', async () => {
    const manager = createTransactionManager();
    const undoneOps: Array<{ op: string; table: string; value?: Uint8Array }> = [];

    manager.setApplyFunction(async (op, table, key, value) => {
      undoneOps.push({ op, table, value });
    });

    await manager.begin();

    // Insert
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: new Uint8Array([1]),
      afterValue: new Uint8Array([10, 20, 30]),
    });

    // Update
    manager.logOperation({
      op: 'UPDATE',
      table: 'users',
      key: new Uint8Array([1]),
      beforeValue: new Uint8Array([10, 20, 30]),
      afterValue: new Uint8Array([40, 50, 60]),
    });

    // Delete
    manager.logOperation({
      op: 'DELETE',
      table: 'orders',
      key: new Uint8Array([2]),
      beforeValue: new Uint8Array([100]),
    });

    // Rollback
    await manager.rollback();

    // Check undo operations (reverse order)
    expect(undoneOps).toHaveLength(3);

    // Undo DELETE = INSERT
    expect(undoneOps[0].op).toBe('INSERT');
    expect(undoneOps[0].table).toBe('orders');

    // Undo UPDATE = UPDATE with before value
    expect(undoneOps[1].op).toBe('UPDATE');
    expect(undoneOps[1].table).toBe('users');
    expect(undoneOps[1].value).toEqual(new Uint8Array([10, 20, 30]));

    // Undo INSERT = DELETE
    expect(undoneOps[2].op).toBe('DELETE');
    expect(undoneOps[2].table).toBe('users');
  });

  it('should throw when rollback fails without apply function', async () => {
    const manager = createTransactionManager();

    await manager.begin();

    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: new Uint8Array([1]),
      afterValue: new Uint8Array([1]),
    });

    // Should throw because there are entries to undo but no apply function
    await expect(manager.rollback()).rejects.toThrow(TransactionError);

    // Transaction should still be active after failed rollback
    expect(manager.isActive()).toBe(true);
  });
});

// =============================================================================
// WAL Integration Tests
// =============================================================================

describe('WAL Integration', () => {
  it('should write BEGIN to WAL', async () => {
    const walWriter = createMockWALWriter();
    const manager = createTransactionManager({ walWriter });

    const context = await manager.begin();

    expect(walWriter.entries).toHaveLength(1);
    expect(walWriter.entries[0].op).toBe('BEGIN');
    expect(walWriter.entries[0].txnId).toBe(context.txnId);

    await manager.rollback();
  });

  it('should write COMMIT to WAL with sync', async () => {
    const walWriter = createMockWALWriter();
    const manager = createTransactionManager({ walWriter });

    await manager.begin();
    await manager.commit();

    const commitEntry = walWriter.entries.find((e) => e.op === 'COMMIT');
    expect(commitEntry).toBeDefined();
  });

  it('should write ROLLBACK to WAL', async () => {
    const walWriter = createMockWALWriter();
    const manager = createTransactionManager({ walWriter });

    await manager.begin();
    await manager.rollback();

    const rollbackEntry = walWriter.entries.find((e) => e.op === 'ROLLBACK');
    expect(rollbackEntry).toBeDefined();
  });

  it('should use WAL LSN for snapshots', async () => {
    const walWriter = createMockWALWriter();
    const manager = createTransactionManager({
      walWriter,
      enableMVCC: true,
    });

    // Write some entries to advance LSN
    await walWriter.append({ timestamp: Date.now(), txnId: 'pre1', op: 'BEGIN', table: '' });
    await walWriter.append({ timestamp: Date.now(), txnId: 'pre1', op: 'COMMIT', table: '' });

    const context = await manager.begin();

    expect(context.snapshot).toBeDefined();
    expect(context.snapshot?.snapshotLsn).toBeGreaterThanOrEqual(2n);

    await manager.commit();
  });
});

// =============================================================================
// Transaction Wrapper Tests
// =============================================================================

describe('Transaction Wrappers', () => {
  describe('executeInTransaction', () => {
    it('should execute and commit on success', async () => {
      const manager = createTransactionManager();

      const result = await executeInTransaction(manager, async (ctx) => {
        expect(ctx.state).toBe(TransactionState.ACTIVE);
        return { value: 42 };
      });

      expect(result.committed).toBe(true);
      expect(result.value).toEqual({ value: 42 });
      expect(result.error).toBeUndefined();
      expect(manager.isActive()).toBe(false);
    });

    it('should rollback on error', async () => {
      const manager = createTransactionManager();

      const result = await executeInTransaction(manager, async () => {
        throw new Error('Test error');
      });

      expect(result.committed).toBe(false);
      expect(result.error?.message).toBe('Test error');
      expect(manager.isActive()).toBe(false);
    });
  });

  describe('executeWithSavepoint', () => {
    it('should execute within savepoint', async () => {
      const manager = createTransactionManager();

      await manager.begin();

      const result = await executeWithSavepoint(manager, 'sp1', async () => {
        return { nested: true };
      });

      expect(result.committed).toBe(true);
      expect(result.value).toEqual({ nested: true });

      await manager.commit();
    });

    it('should rollback savepoint on error', async () => {
      const manager = createTransactionManager();
      manager.setApplyFunction(async () => {});

      await manager.begin();

      manager.logOperation({
        op: 'INSERT',
        table: 'before',
        afterValue: new Uint8Array([1]),
      });

      const result = await executeWithSavepoint(manager, 'sp1', async () => {
        manager.logOperation({
          op: 'INSERT',
          table: 'inside',
          afterValue: new Uint8Array([2]),
        });
        throw new Error('Nested error');
      });

      expect(result.committed).toBe(false);
      expect(result.error?.message).toBe('Nested error');

      // Outer transaction should still be active
      expect(manager.isActive()).toBe(true);

      await manager.commit();
    });
  });

  describe('executeReadOnly', () => {
    it('should execute read-only transaction', async () => {
      const manager = createTransactionManager();

      const result = await executeReadOnly(manager, async (ctx) => {
        expect(ctx.readOnly).toBe(true);
        return 'read result';
      });

      expect(result).toBe('read result');
    });

    it('should throw on write in read-only transaction', async () => {
      const manager = createTransactionManager();

      await expect(
        executeReadOnly(manager, async () => {
          manager.logOperation({
            op: 'INSERT',
            table: 'users',
            afterValue: new Uint8Array([1]),
          });
        })
      ).rejects.toThrow(TransactionError);
    });
  });
});

// =============================================================================
// Lock Manager Tests
// =============================================================================

describe('Lock Manager', () => {
  it('should acquire and release locks', async () => {
    const lockManager = createLockManager();

    const result = await lockManager.acquire({
      txnId: 'txn1',
      lockType: LockType.SHARED,
      resource: 'users',
      timestamp: Date.now(),
    });

    expect(result.acquired).toBe(true);
    expect(result.grantedType).toBe(LockType.SHARED);

    const locks = lockManager.getHeldLocks('txn1');
    expect(locks).toHaveLength(1);

    lockManager.release('txn1', 'users');

    expect(lockManager.getHeldLocks('txn1')).toHaveLength(0);
  });

  it('should allow multiple shared locks', async () => {
    const lockManager = createLockManager();

    const result1 = await lockManager.acquire({
      txnId: 'txn1',
      lockType: LockType.SHARED,
      resource: 'users',
      timestamp: Date.now(),
    });

    const result2 = await lockManager.acquire({
      txnId: 'txn2',
      lockType: LockType.SHARED,
      resource: 'users',
      timestamp: Date.now(),
    });

    expect(result1.acquired).toBe(true);
    expect(result2.acquired).toBe(true);

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  it('should block exclusive lock when shared locks exist', async () => {
    const lockManager = createLockManager({ defaultTimeout: 100 });

    await lockManager.acquire({
      txnId: 'txn1',
      lockType: LockType.SHARED,
      resource: 'users',
      timestamp: Date.now(),
    });

    // This should timeout waiting for shared lock
    await expect(
      lockManager.acquire({
        txnId: 'txn2',
        lockType: LockType.EXCLUSIVE,
        resource: 'users',
        timestamp: Date.now(),
        timeout: 50,
      })
    ).rejects.toThrow(TransactionError);

    lockManager.releaseAll('txn1');
  });

  it('should detect deadlocks', async () => {
    const lockManager = createLockManager({ detectDeadlocks: true });

    // txn1 holds lock on A
    await lockManager.acquire({
      txnId: 'txn1',
      lockType: LockType.EXCLUSIVE,
      resource: 'A',
      timestamp: Date.now(),
    });

    // txn2 holds lock on B
    await lockManager.acquire({
      txnId: 'txn2',
      lockType: LockType.EXCLUSIVE,
      resource: 'B',
      timestamp: Date.now(),
    });

    // txn1 waits for B (held by txn2)
    const txn1WaitsForB = lockManager.acquire({
      txnId: 'txn1',
      lockType: LockType.EXCLUSIVE,
      resource: 'B',
      timestamp: Date.now(),
      timeout: 5000,
    });

    // Small delay to let txn1's request queue
    await new Promise((r) => setTimeout(r, 10));

    // txn2 waits for A (held by txn1) - should detect deadlock
    await expect(
      lockManager.acquire({
        txnId: 'txn2',
        lockType: LockType.EXCLUSIVE,
        resource: 'A',
        timestamp: Date.now(),
      })
    ).rejects.toMatchObject({
      code: TransactionErrorCode.DEADLOCK,
    });

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  it('should upgrade locks', async () => {
    const lockManager = createLockManager();

    // Acquire shared lock
    await lockManager.acquire({
      txnId: 'txn1',
      lockType: LockType.SHARED,
      resource: 'users',
      timestamp: Date.now(),
    });

    // Upgrade to exclusive (should work since only holder)
    const result = await lockManager.acquire({
      txnId: 'txn1',
      lockType: LockType.EXCLUSIVE,
      resource: 'users',
      timestamp: Date.now(),
    });

    expect(result.acquired).toBe(true);
    expect(result.grantedType).toBe(LockType.EXCLUSIVE);

    lockManager.releaseAll('txn1');
  });

  it('should release all locks for a transaction', async () => {
    const lockManager = createLockManager();

    await lockManager.acquire({
      txnId: 'txn1',
      lockType: LockType.EXCLUSIVE,
      resource: 'A',
      timestamp: Date.now(),
    });

    await lockManager.acquire({
      txnId: 'txn1',
      lockType: LockType.SHARED,
      resource: 'B',
      timestamp: Date.now(),
    });

    await lockManager.acquire({
      txnId: 'txn1',
      lockType: LockType.EXCLUSIVE,
      resource: 'C',
      timestamp: Date.now(),
    });

    expect(lockManager.getHeldLocks('txn1')).toHaveLength(3);

    lockManager.releaseAll('txn1');

    expect(lockManager.getHeldLocks('txn1')).toHaveLength(0);
  });
});

// =============================================================================
// MVCC Tests
// =============================================================================

describe('MVCC Store', () => {
  it('should store and retrieve versions', () => {
    const store = createMVCCStore();

    const version: RowVersion = {
      createdBy: 'txn1',
      createdLsn: 1n,
      data: new Uint8Array([1, 2, 3]),
    };

    store.addVersion('users', new Uint8Array([1]), version);

    const snapshot = createSnapshot('txn2', 10n, new Set());
    const visible = store.getVisible('users', new Uint8Array([1]), snapshot);

    expect(visible).toBeDefined();
    expect(visible?.data).toEqual(new Uint8Array([1, 2, 3]));
  });

  it('should not return versions created after snapshot', () => {
    const store = createMVCCStore();

    const version: RowVersion = {
      createdBy: 'txn1',
      createdLsn: 10n, // Created at LSN 10
      data: new Uint8Array([1, 2, 3]),
    };

    store.addVersion('users', new Uint8Array([1]), version);

    // Snapshot at LSN 5 (before version was created)
    const snapshot = createSnapshot('txn2', 5n, new Set());
    const visible = store.getVisible('users', new Uint8Array([1]), snapshot);

    expect(visible).toBeNull();
  });

  it('should not return versions from active transactions', () => {
    const store = createMVCCStore();

    const version: RowVersion = {
      createdBy: 'txn1',
      createdLsn: 1n,
      data: new Uint8Array([1, 2, 3]),
    };

    store.addVersion('users', new Uint8Array([1]), version);

    // txn1 was active when snapshot was taken
    const snapshot = createSnapshot('txn2', 10n, new Set(['txn1']));
    const visible = store.getVisible('users', new Uint8Array([1]), snapshot);

    expect(visible).toBeNull();
  });

  it('should return own transaction versions', () => {
    const store = createMVCCStore();

    const version: RowVersion = {
      createdBy: 'txn1',
      createdLsn: 1n,
      data: new Uint8Array([1, 2, 3]),
    };

    store.addVersion('users', new Uint8Array([1]), version);

    // Same transaction can see its own changes
    const snapshot = createSnapshot('txn1', 10n, new Set(['txn1']));
    const visible = store.getVisible('users', new Uint8Array([1]), snapshot);

    expect(visible).toBeDefined();
  });

  it('should handle deleted versions', () => {
    const store = createMVCCStore();

    const version: RowVersion = {
      createdBy: 'txn1',
      createdLsn: 1n,
      data: new Uint8Array([1, 2, 3]),
    };

    store.addVersion('users', new Uint8Array([1]), version);
    store.markDeleted('users', new Uint8Array([1]), 'txn2', 5n);

    // Snapshot before deletion
    const snapshotBefore = createSnapshot('txn3', 3n, new Set());
    const visibleBefore = store.getVisible('users', new Uint8Array([1]), snapshotBefore);
    expect(visibleBefore).toBeDefined();

    // Snapshot after deletion
    const snapshotAfter = createSnapshot('txn4', 10n, new Set());
    const visibleAfter = store.getVisible('users', new Uint8Array([1]), snapshotAfter);
    expect(visibleAfter).toBeNull();
  });

  it('should vacuum old versions', () => {
    const store = createMVCCStore();

    // Add multiple versions
    store.addVersion('users', new Uint8Array([1]), {
      createdBy: 'txn1',
      createdLsn: 1n,
      deletedBy: 'txn2',
      deletedLsn: 5n,
      data: new Uint8Array([1]),
    });

    store.addVersion('users', new Uint8Array([1]), {
      createdBy: 'txn2',
      createdLsn: 5n,
      data: new Uint8Array([2]),
    });

    // Vacuum with oldest active LSN of 10
    const cleaned = store.vacuum(10n);

    // Old deleted version should be cleaned
    expect(cleaned).toBe(1);
  });
});

describe('Visibility Functions', () => {
  it('should correctly determine visibility', () => {
    const snapshot = createSnapshot('txn2', 10n, new Set(['txn3']));

    // Visible: created before snapshot, not deleted
    expect(
      isVersionVisible(
        { createdBy: 'txn1', createdLsn: 5n, data: new Uint8Array() },
        snapshot
      )
    ).toBe(true);

    // Not visible: created after snapshot
    expect(
      isVersionVisible(
        { createdBy: 'txn1', createdLsn: 15n, data: new Uint8Array() },
        snapshot
      )
    ).toBe(false);

    // Not visible: created by active transaction
    expect(
      isVersionVisible(
        { createdBy: 'txn3', createdLsn: 5n, data: new Uint8Array() },
        snapshot
      )
    ).toBe(false);

    // Visible: own transaction
    expect(
      isVersionVisible(
        { createdBy: 'txn2', createdLsn: 8n, data: new Uint8Array() },
        snapshot
      )
    ).toBe(true);

    // Visible: deleted after snapshot
    expect(
      isVersionVisible(
        {
          createdBy: 'txn1',
          createdLsn: 5n,
          deletedBy: 'txn4',
          deletedLsn: 15n,
          data: new Uint8Array(),
        },
        snapshot
      )
    ).toBe(true);

    // Not visible: deleted before snapshot
    expect(
      isVersionVisible(
        {
          createdBy: 'txn1',
          createdLsn: 5n,
          deletedBy: 'txn4',
          deletedLsn: 8n,
          data: new Uint8Array(),
        },
        snapshot
      )
    ).toBe(false);
  });
});

// =============================================================================
// Isolation Enforcer Tests
// =============================================================================

describe('Isolation Enforcer', () => {
  it('should acquire shared lock for SERIALIZABLE reads', async () => {
    const lockManager = createLockManager();
    const enforcer = createIsolationEnforcer({ lockManager });

    const context: TransactionContext = {
      txnId: 'txn1',
      state: TransactionState.ACTIVE,
      mode: TransactionMode.DEFERRED,
      isolationLevel: IsolationLevel.SERIALIZABLE,
      savepoints: createSavepointStack(),
      log: createTransactionLog('txn1'),
      locks: [],
      startedAt: Date.now(),
      readOnly: false,
      autoCommit: false,
    };

    await enforcer.prepareRead(context, 'users', new Uint8Array([1]));

    expect(context.locks).toHaveLength(1);
    expect(context.locks[0].lockType).toBe(LockType.SHARED);

    enforcer.onCommit(context);
  });

  it('should not hold locks for READ_COMMITTED', async () => {
    const lockManager = createLockManager();
    const enforcer = createIsolationEnforcer({ lockManager });

    const context: TransactionContext = {
      txnId: 'txn1',
      state: TransactionState.ACTIVE,
      mode: TransactionMode.DEFERRED,
      isolationLevel: IsolationLevel.READ_COMMITTED,
      savepoints: createSavepointStack(),
      log: createTransactionLog('txn1'),
      locks: [],
      startedAt: Date.now(),
      readOnly: false,
      autoCommit: false,
    };

    await enforcer.prepareRead(context, 'users', new Uint8Array([1]));

    // Lock should be released immediately for READ_COMMITTED
    expect(context.locks).toHaveLength(0);
  });

  it('should acquire exclusive lock for writes', async () => {
    const lockManager = createLockManager();
    const enforcer = createIsolationEnforcer({ lockManager });

    const context: TransactionContext = {
      txnId: 'txn1',
      state: TransactionState.ACTIVE,
      mode: TransactionMode.DEFERRED,
      isolationLevel: IsolationLevel.READ_COMMITTED,
      savepoints: createSavepointStack(),
      log: createTransactionLog('txn1'),
      locks: [],
      startedAt: Date.now(),
      readOnly: false,
      autoCommit: false,
    };

    await enforcer.prepareWrite(context, 'users', new Uint8Array([1]));

    expect(context.locks).toHaveLength(1);
    expect(context.locks[0].lockType).toBe(LockType.EXCLUSIVE);

    enforcer.onCommit(context);
  });

  it('should release locks on commit', async () => {
    const lockManager = createLockManager();
    const enforcer = createIsolationEnforcer({ lockManager });

    const context: TransactionContext = {
      txnId: 'txn1',
      state: TransactionState.ACTIVE,
      mode: TransactionMode.DEFERRED,
      isolationLevel: IsolationLevel.SERIALIZABLE,
      savepoints: createSavepointStack(),
      log: createTransactionLog('txn1'),
      locks: [],
      startedAt: Date.now(),
      readOnly: false,
      autoCommit: false,
    };

    await enforcer.prepareRead(context, 'users');
    await enforcer.prepareWrite(context, 'orders', new Uint8Array([1]));

    expect(lockManager.getHeldLocks('txn1').length).toBeGreaterThan(0);

    enforcer.onCommit(context);

    expect(lockManager.getHeldLocks('txn1')).toHaveLength(0);
  });

  it('should release locks on rollback', async () => {
    const lockManager = createLockManager();
    const enforcer = createIsolationEnforcer({ lockManager });

    const context: TransactionContext = {
      txnId: 'txn1',
      state: TransactionState.ACTIVE,
      mode: TransactionMode.DEFERRED,
      isolationLevel: IsolationLevel.SERIALIZABLE,
      savepoints: createSavepointStack(),
      log: createTransactionLog('txn1'),
      locks: [],
      startedAt: Date.now(),
      readOnly: false,
      autoCommit: false,
    };

    await enforcer.prepareWrite(context, 'users', new Uint8Array([1]));

    expect(lockManager.getHeldLocks('txn1').length).toBeGreaterThan(0);

    enforcer.onRollback(context);

    expect(lockManager.getHeldLocks('txn1')).toHaveLength(0);
  });
});

// =============================================================================
// Integration Tests
// =============================================================================

describe('Integration', () => {
  it('should handle complete transaction lifecycle', async () => {
    const walWriter = createMockWALWriter();
    const lockManager = createLockManager();
    const manager = createTransactionManager({ walWriter });

    // Set up apply function
    const changes: Array<{ op: string; table: string }> = [];
    manager.setApplyFunction(async (op, table) => {
      changes.push({ op, table });
    });

    // Begin transaction
    const ctx = await manager.begin({ mode: TransactionMode.IMMEDIATE });
    expect(ctx.mode).toBe(TransactionMode.IMMEDIATE);

    // Log some operations
    manager.logOperation({
      op: 'INSERT',
      table: 'users',
      key: new Uint8Array([1]),
      afterValue: new Uint8Array([10, 20]),
    });

    manager.logOperation({
      op: 'UPDATE',
      table: 'users',
      key: new Uint8Array([1]),
      beforeValue: new Uint8Array([10, 20]),
      afterValue: new Uint8Array([30, 40]),
    });

    // Create savepoint
    await manager.savepoint('sp1');

    manager.logOperation({
      op: 'DELETE',
      table: 'users',
      key: new Uint8Array([1]),
      beforeValue: new Uint8Array([30, 40]),
    });

    // Rollback to savepoint
    await manager.rollbackTo('sp1');

    // Verify undo was applied
    expect(changes).toHaveLength(1);
    expect(changes[0].op).toBe('INSERT'); // Undo DELETE

    // Commit
    const commitLsn = await manager.commit();
    expect(commitLsn).toBeGreaterThan(0n);

    // Verify WAL entries
    expect(walWriter.entries.some((e) => e.op === 'BEGIN')).toBe(true);
    expect(walWriter.entries.some((e) => e.op === 'COMMIT')).toBe(true);
  });

  it('should handle concurrent transactions with different isolation levels', async () => {
    const lockManager = createLockManager();

    // Two transactions with different isolation levels
    const manager1 = createTransactionManager({
      defaultIsolationLevel: IsolationLevel.SERIALIZABLE,
    });

    const manager2 = createTransactionManager({
      defaultIsolationLevel: IsolationLevel.READ_COMMITTED,
    });

    const ctx1 = await manager1.begin();
    const ctx2 = await manager2.begin();

    expect(ctx1.isolationLevel).toBe(IsolationLevel.SERIALIZABLE);
    expect(ctx2.isolationLevel).toBe(IsolationLevel.READ_COMMITTED);

    await manager1.commit();
    await manager2.commit();
  });
});
