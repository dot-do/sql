/**
 * Tests for InMemoryTransactionLog and error types
 */

import { describe, it, expect } from 'vitest';
import { InMemoryTransactionLog } from '../types.js';
import { DistributedTransactionError, DistributedTransactionErrorCode } from '../errors.js';

describe('InMemoryTransactionLog', () => {
  it('should write and read log records', async () => {
    const log = new InMemoryTransactionLog();

    await log.write({
      txnId: 'txn-1',
      type: 'BEGIN',
      participants: ['shard-a'],
      timestamp: 1000,
    });

    const records = await log.read('txn-1');
    expect(records).toHaveLength(1);
    expect(records[0].type).toBe('BEGIN');
    expect(records[0].txnId).toBe('txn-1');
  });

  it('should append multiple records for same transaction', async () => {
    const log = new InMemoryTransactionLog();

    await log.write({ txnId: 'txn-1', type: 'BEGIN', participants: ['a'], timestamp: 1000 });
    await log.write({ txnId: 'txn-1', type: 'PREPARE', participants: ['a'], timestamp: 2000 });
    await log.write({ txnId: 'txn-1', type: 'COMMIT', participants: ['a'], timestamp: 3000 });

    const records = await log.read('txn-1');
    expect(records).toHaveLength(3);
    expect(records.map((r) => r.type)).toEqual(['BEGIN', 'PREPARE', 'COMMIT']);
  });

  it('should return empty array for unknown transaction', async () => {
    const log = new InMemoryTransactionLog();
    const records = await log.read('txn-nonexistent');
    expect(records).toEqual([]);
  });

  it('should delete all records for a transaction', async () => {
    const log = new InMemoryTransactionLog();

    await log.write({ txnId: 'txn-1', type: 'BEGIN', participants: ['a'], timestamp: 1000 });
    await log.write({ txnId: 'txn-1', type: 'COMMIT', participants: ['a'], timestamp: 2000 });

    await log.delete('txn-1');

    const records = await log.read('txn-1');
    expect(records).toEqual([]);
  });

  it('should get pending (non-committed, non-aborted) transactions', async () => {
    const log = new InMemoryTransactionLog();

    // Committed transaction
    await log.write({ txnId: 'txn-1', type: 'BEGIN', participants: ['a'], timestamp: 1000 });
    await log.write({ txnId: 'txn-1', type: 'COMMIT', participants: ['a'], timestamp: 2000 });

    // Aborted transaction
    await log.write({ txnId: 'txn-2', type: 'BEGIN', participants: ['b'], timestamp: 1000 });
    await log.write({ txnId: 'txn-2', type: 'ABORT', participants: ['b'], timestamp: 2000 });

    // Pending transaction (stuck in PREPARE)
    await log.write({ txnId: 'txn-3', type: 'BEGIN', participants: ['c'], timestamp: 1000 });
    await log.write({ txnId: 'txn-3', type: 'PREPARE', participants: ['c'], timestamp: 2000 });

    // Pending transaction (only BEGIN)
    await log.write({ txnId: 'txn-4', type: 'BEGIN', participants: ['d'], timestamp: 1000 });

    const pending = await log.getPending();
    expect(pending).toHaveLength(2);

    const pendingTxns = pending.map((r) => r.txnId);
    expect(pendingTxns).toContain('txn-3');
    expect(pendingTxns).toContain('txn-4');
  });

  it('should handle sync as no-op', async () => {
    const log = new InMemoryTransactionLog();
    await log.sync(); // Should not throw
  });

  it('should isolate records between transactions', async () => {
    const log = new InMemoryTransactionLog();

    await log.write({ txnId: 'txn-1', type: 'BEGIN', participants: ['a'], timestamp: 1000 });
    await log.write({ txnId: 'txn-2', type: 'BEGIN', participants: ['b'], timestamp: 1000 });

    const records1 = await log.read('txn-1');
    const records2 = await log.read('txn-2');

    expect(records1).toHaveLength(1);
    expect(records2).toHaveLength(1);
    expect(records1[0].participants).toEqual(['a']);
    expect(records2[0].participants).toEqual(['b']);
  });
});

describe('DistributedTransactionError', () => {
  it('should create error with code and message', () => {
    const err = new DistributedTransactionError(
      DistributedTransactionErrorCode.NO_ACTIVE_TRANSACTION,
      'No active transaction'
    );

    expect(err).toBeInstanceOf(Error);
    expect(err).toBeInstanceOf(DistributedTransactionError);
    expect(err.name).toBe('DistributedTransactionError');
    expect(err.code).toBe('DTX_NO_ACTIVE');
    expect(err.message).toBe('No active transaction');
    expect(err.txnId).toBeUndefined();
    expect(err.details).toBeUndefined();
  });

  it('should include txnId and details', () => {
    const err = new DistributedTransactionError(
      DistributedTransactionErrorCode.PARTICIPANT_FAILURE,
      'Shard failed',
      'txn-123',
      { shardId: 'shard-a' }
    );

    expect(err.txnId).toBe('txn-123');
    expect(err.details).toEqual({ shardId: 'shard-a' });
  });

  it('should have all error codes defined', () => {
    expect(DistributedTransactionErrorCode.NO_ACTIVE_TRANSACTION).toBe('DTX_NO_ACTIVE');
    expect(DistributedTransactionErrorCode.TRANSACTION_ALREADY_ACTIVE).toBe('DTX_ALREADY_ACTIVE');
    expect(DistributedTransactionErrorCode.INVALID_STATE).toBe('DTX_INVALID_STATE');
    expect(DistributedTransactionErrorCode.PREPARE_FAILED).toBe('DTX_PREPARE_FAILED');
    expect(DistributedTransactionErrorCode.COMMIT_FAILED).toBe('DTX_COMMIT_FAILED');
    expect(DistributedTransactionErrorCode.ROLLBACK_FAILED).toBe('DTX_ROLLBACK_FAILED');
    expect(DistributedTransactionErrorCode.TIMEOUT).toBe('DTX_TIMEOUT');
    expect(DistributedTransactionErrorCode.PARTICIPANT_FAILURE).toBe('DTX_PARTICIPANT_FAILURE');
    expect(DistributedTransactionErrorCode.COORDINATOR_FAILURE).toBe('DTX_COORDINATOR_FAILURE');
    expect(DistributedTransactionErrorCode.LOCK_FAILED).toBe('DTX_LOCK_FAILED');
    expect(DistributedTransactionErrorCode.SERIALIZATION_FAILURE).toBe('DTX_SERIALIZATION_FAILURE');
  });
});
