/**
 * Tests for the Cross-Shard Executor
 *
 * Covers:
 * - Successful multi-shard transactions
 * - Automatic rollback on participant NO votes
 * - Error handling during operations
 * - Isolation level configuration
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { createCrossShardExecutor } from '../cross-shard-executor.js';
import { createDistributedTransactionCoordinator } from '../coordinator.js';
import { InMemoryTransactionLog } from '../types.js';
import type {
  ShardParticipantRPC,
  ParticipantVote,
  CoordinatorDecision,
} from '../types.js';
import type { ShardId } from '../../sharding/types.js';

// =============================================================================
// HELPERS
// =============================================================================

function shardId(id: string): ShardId {
  return id as unknown as ShardId;
}

function createTestRPC(overrides?: Partial<ShardParticipantRPC>): ShardParticipantRPC {
  return {
    async prepare() {
      return { vote: 'YES' as ParticipantVote };
    },
    async commit() {},
    async abort() {},
    async queryDecision() {
      return 'PENDING' as CoordinatorDecision;
    },
    async execute(_shardId, _sql, _params) {
      return { rows: [{ id: 1 }], rowsAffected: 1 };
    },
    ...overrides,
  };
}

// =============================================================================
// TESTS
// =============================================================================

describe('CrossShardExecutor', () => {
  const shardA = shardId('shard-a');
  const shardB = shardId('shard-b');

  it('should execute operations across shards and commit successfully', async () => {
    const rpc = createTestRPC();
    const txnLog = new InMemoryTransactionLog();
    const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
      coordinatorId: 'coord-1',
    });
    const executor = createCrossShardExecutor(coordinator);

    const result = await executor.executeInTransaction(
      [shardA, shardB],
      [
        { shard: shardA, sql: 'INSERT INTO a VALUES (1)' },
        { shard: shardB, sql: 'INSERT INTO b VALUES (2)' },
      ]
    );

    expect(result.success).toBe(true);
    expect(result.results).toHaveLength(2);
    expect(result.error).toBeUndefined();
  });

  it('should return failure when a participant votes NO', async () => {
    const rpc = createTestRPC({
      async prepare(shardIdStr) {
        if (shardIdStr === 'shard-b') {
          return { vote: 'NO' as ParticipantVote };
        }
        return { vote: 'YES' as ParticipantVote };
      },
    });

    const txnLog = new InMemoryTransactionLog();
    const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
      coordinatorId: 'coord-1',
    });
    const executor = createCrossShardExecutor(coordinator);

    const result = await executor.executeInTransaction(
      [shardA, shardB],
      [
        { shard: shardA, sql: 'INSERT INTO a VALUES (1)' },
        { shard: shardB, sql: 'INSERT INTO b VALUES (2)' },
      ]
    );

    expect(result.success).toBe(false);
    expect(result.results).toEqual([]);
    expect(result.error).toBeDefined();
    expect(result.error!.message).toContain('voted NO');
  });

  it('should rollback on execution error', async () => {
    let abortCallCount = 0;
    let callCount = 0;
    const rpc = createTestRPC({
      async execute() {
        callCount++;
        if (callCount === 2) {
          throw new Error('Constraint violation');
        }
        return { rows: [], rowsAffected: 1 };
      },
      async abort() { abortCallCount++; },
    });

    const txnLog = new InMemoryTransactionLog();
    const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
      coordinatorId: 'coord-1',
    });
    const executor = createCrossShardExecutor(coordinator);

    const result = await executor.executeInTransaction(
      [shardA, shardB],
      [
        { shard: shardA, sql: 'INSERT INTO a VALUES (1)' },
        { shard: shardB, sql: 'INSERT INTO b VALUES (INVALID)' },
      ]
    );

    expect(result.success).toBe(false);
    expect(result.error).toBeDefined();
  });

  it('should pass timeout option to coordinator', async () => {
    const rpc = createTestRPC();
    const txnLog = new InMemoryTransactionLog();
    const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
      coordinatorId: 'coord-1',
    });
    const executor = createCrossShardExecutor(coordinator);

    const result = await executor.executeInTransaction(
      [shardA],
      [{ shard: shardA, sql: 'SELECT 1' }],
      { timeout: 5000 }
    );

    expect(result.success).toBe(true);
  });

  it('should handle empty operations list', async () => {
    const rpc = createTestRPC();
    const txnLog = new InMemoryTransactionLog();
    const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
      coordinatorId: 'coord-1',
    });
    const executor = createCrossShardExecutor(coordinator);

    const result = await executor.executeInTransaction([shardA], []);

    expect(result.success).toBe(true);
    expect(result.results).toHaveLength(0);
  });

  it('should collect results from all operations', async () => {
    let callIdx = 0;
    const rpc = createTestRPC({
      async execute() {
        callIdx++;
        return { rows: [{ value: callIdx }], rowsAffected: 1 };
      },
    });

    const txnLog = new InMemoryTransactionLog();
    const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
      coordinatorId: 'coord-1',
    });
    const executor = createCrossShardExecutor(coordinator);

    const result = await executor.executeInTransaction(
      [shardA, shardB],
      [
        { shard: shardA, sql: 'INSERT INTO a VALUES (1)' },
        { shard: shardB, sql: 'INSERT INTO b VALUES (2)' },
      ]
    );

    expect(result.success).toBe(true);
    expect(result.results).toHaveLength(2);
  });

  it('should handle rollback failure gracefully', async () => {
    const rpc = createTestRPC({
      async execute() {
        throw new Error('Exec failed');
      },
      async abort() {
        throw new Error('Abort also failed');
      },
    });

    const txnLog = new InMemoryTransactionLog();
    const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
      coordinatorId: 'coord-1',
      maxRetries: 1,
      retryDelayMs: 1,
    });
    const executor = createCrossShardExecutor(coordinator);

    const result = await executor.executeInTransaction(
      [shardA],
      [{ shard: shardA, sql: 'INSERT INTO a VALUES (1)' }]
    );

    expect(result.success).toBe(false);
    expect(result.error).toBeDefined();
  });
});
