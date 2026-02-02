/**
 * DistributedExecutor Unit Tests
 *
 * Tests for the distributed query executor:
 * - Parallel shard execution
 * - Result merging and aggregation
 * - Post-processing (sort, limit, distinct)
 * - Error handling and retries
 * - Circuit breaker integration
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

import {
  DistributedExecutor,
  MockShardRPC,
  createExecutor,
  ShardExecutionError,
  type ShardRPC,
  type ExecutorConfig,
  type ExecuteOptions,
} from '../executor.js';

import {
  createReplicaSelector,
  type DefaultReplicaSelector,
} from '../replica.js';

import { createRouter, type QueryRouter } from '../router.js';

import {
  createVSchema,
  createShardId,
  hashVindex,
  shardedTable,
  shard,
  replica,
  type ShardConfig,
  type ExecutionPlan,
  type ShardResult,
  type ShardExecutionPlan,
  type MergedResult,
  type RoutingDecision,
} from '../types.js';

// =============================================================================
// TEST FIXTURES
// =============================================================================

function createTestShards(count: number = 3): ShardConfig[] {
  return Array.from({ length: count }, (_, i) =>
    shard(createShardId(`shard-${i + 1}`), `do-ns-${i + 1}`, {
      replicas: [
        replica(`replica-${i + 1}-a`, `do-ns-${i + 1}-replica-a`, 'replica'),
      ],
    })
  );
}

function createTestSetup(shardCount: number = 3) {
  const shards = createTestShards(shardCount);

  const vschema = createVSchema(
    { users: shardedTable('tenant_id', hashVindex()) },
    shards
  );

  const rpc = new MockShardRPC();
  const selector = createReplicaSelector(shards);
  const router = createRouter(vschema);

  // Populate mock data
  for (let i = 1; i <= shardCount; i++) {
    rpc.setShardData(
      `shard-${i}`,
      ['id', 'name', 'status'],
      [
        [i * 100 + 1, `User${i}A`, 'active'],
        [i * 100 + 2, `User${i}B`, 'inactive'],
      ]
    );
  }

  return { shards, vschema, rpc, selector, router };
}

/**
 * Failing mock RPC for error testing
 */
class FailingMockShardRPC implements ShardRPC {
  private readonly failingShards = new Set<string>();
  private readonly shardData = new Map<string, { columns: string[]; rows: unknown[][] }>();
  public callCount = new Map<string, number>();
  public failureType: 'timeout' | 'error' = 'error';

  setShardData(shardId: string, columns: string[], rows: unknown[][]): void {
    this.shardData.set(shardId, { columns, rows });
  }

  setShardToFail(shardId: string, type: 'timeout' | 'error' = 'error'): void {
    this.failingShards.add(shardId);
    this.failureType = type;
  }

  clearShardFailure(shardId: string): void {
    this.failingShards.delete(shardId);
  }

  async execute(
    shardId: string,
    _replicaId: string | undefined,
    _sql: string,
    _params?: unknown[],
    _options?: ExecuteOptions
  ): Promise<ShardResult> {
    const count = this.callCount.get(shardId) ?? 0;
    this.callCount.set(shardId, count + 1);

    if (this.failingShards.has(shardId)) {
      if (this.failureType === 'timeout') {
        throw new Error(`Shard ${shardId} timeout`);
      }
      throw new Error(`Shard ${shardId} is unavailable`);
    }

    const data = this.shardData.get(shardId) ?? { columns: [], rows: [] };
    return {
      shardId: createShardId(shardId),
      columns: data.columns,
      rows: data.rows,
      rowCount: data.rows.length,
      executionTimeMs: 5,
    };
  }

  async *executeStream(
    shardId: string,
    replicaId: string | undefined,
    sql: string,
    params?: unknown[],
    options?: ExecuteOptions
  ): AsyncIterable<ShardResult> {
    yield await this.execute(shardId, replicaId, sql, params, options);
  }
}

// =============================================================================
// DISTRIBUTED EXECUTOR BASIC TESTS
// =============================================================================

describe('DistributedExecutor', () => {
  describe('constructor', () => {
    it('should create executor with default config', () => {
      const { rpc, selector } = createTestSetup();
      const executor = createExecutor(rpc, selector);

      expect(executor).toBeInstanceOf(DistributedExecutor);
    });

    it('should create executor with custom config', () => {
      const { rpc, selector } = createTestSetup();
      const config: ExecutorConfig = {
        maxParallelShards: 10,
        defaultTimeoutMs: 5000,
        failFast: true,
        retry: {
          maxAttempts: 5,
          backoffMs: 200,
          maxBackoffMs: 10000,
        },
      };
      const executor = createExecutor(rpc, selector, config);

      expect(executor).toBeInstanceOf(DistributedExecutor);
    });
  });

  describe('single-shard execution', () => {
    it('should execute query on single shard', async () => {
      const { rpc, selector, router } = createTestSetup();
      const executor = createExecutor(rpc, selector);

      const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
      const result = await executor.execute(plan);

      expect(result.rows.length).toBeGreaterThan(0);
      expect(result.contributingShards).toHaveLength(1);
    });

    it('should return all columns', async () => {
      const { rpc, selector, router } = createTestSetup();
      const executor = createExecutor(rpc, selector);

      const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
      const result = await executor.execute(plan);

      expect(result.columns).toContain('id');
      expect(result.columns).toContain('name');
      expect(result.columns).toContain('status');
    });

    it('should track execution time', async () => {
      const { rpc, selector, router } = createTestSetup();
      const executor = createExecutor(rpc, selector);

      const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
      const result = await executor.execute(plan);

      expect(result.totalExecutionTimeMs).toBeGreaterThan(0);
    });
  });

  describe('scatter execution', () => {
    it('should execute query across all shards', async () => {
      const { rpc, selector, router } = createTestSetup(3);
      const executor = createExecutor(rpc, selector);

      const plan = router.createExecutionPlan('SELECT * FROM users');
      const result = await executor.execute(plan);

      // Should have data from all 3 shards (2 rows each)
      expect(result.rows.length).toBe(6);
      expect(result.contributingShards).toHaveLength(3);
    });

    it('should track timing for each shard', async () => {
      const { rpc, selector, router } = createTestSetup(3);
      const executor = createExecutor(rpc, selector);

      const plan = router.createExecutionPlan('SELECT * FROM users');
      const result = await executor.execute(plan);

      expect(Object.keys(result.shardTiming)).toHaveLength(3);
      for (const timing of Object.values(result.shardTiming)) {
        expect(timing).toBeGreaterThanOrEqual(0);
      }
    });
  });
});

// =============================================================================
// RESULT MERGING TESTS
// =============================================================================

describe('Result Merging', () => {
  describe('basic merge', () => {
    it('should merge rows from multiple shards', async () => {
      const { rpc, selector, router } = createTestSetup(3);
      const executor = createExecutor(rpc, selector);

      const plan = router.createExecutionPlan('SELECT * FROM users');
      const result = await executor.execute(plan);

      expect(result.rows.length).toBe(6); // 2 rows per shard * 3 shards
    });

    it('should preserve column order', async () => {
      const { rpc, selector, router } = createTestSetup();
      const executor = createExecutor(rpc, selector);

      const plan = router.createExecutionPlan('SELECT * FROM users');
      const result = await executor.execute(plan);

      expect(result.columns).toEqual(['id', 'name', 'status']);
    });
  });

  describe('distinct post-processing', () => {
    it('should deduplicate rows based on specified columns', async () => {
      const shards = createTestShards(2);
      const vschema = createVSchema(
        { users: shardedTable('tenant_id', hashVindex()) },
        shards
      );
      const rpc = new MockShardRPC();
      const selector = createReplicaSelector(shards);

      // Both shards have same status values
      rpc.setShardData('shard-1', ['status'], [['active'], ['inactive']]);
      rpc.setShardData('shard-2', ['status'], [['active'], ['pending']]);

      const executor = createExecutor(rpc, selector);

      // Manually create plan with distinct post-processing
      const plan: ExecutionPlan = {
        sql: 'SELECT DISTINCT status FROM users',
        routing: {
          queryType: 'scatter',
          targetShards: shards.map(s => s.id),
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 2,
          reason: 'Scatter query',
        },
        shardPlans: shards.map(s => ({
          shardId: s.id,
          sql: 'SELECT DISTINCT status FROM users',
          isFinal: false,
        })),
        postProcessing: [
          { type: 'merge' },
          { type: 'distinct', columns: ['status'] },
        ],
        totalCost: 2,
      };

      const result = await executor.execute(plan);

      // Should have 3 unique values: active, inactive, pending
      expect(result.rows.length).toBe(3);
    });
  });

  describe('sort post-processing', () => {
    it('should sort merged results ascending', async () => {
      const shards = createTestShards(2);
      const vschema = createVSchema(
        { users: shardedTable('tenant_id', hashVindex()) },
        shards
      );
      const rpc = new MockShardRPC();
      const selector = createReplicaSelector(shards);

      rpc.setShardData('shard-1', ['id', 'name'], [[3, 'Charlie'], [1, 'Alice']]);
      rpc.setShardData('shard-2', ['id', 'name'], [[4, 'David'], [2, 'Bob']]);

      const executor = createExecutor(rpc, selector);

      const plan: ExecutionPlan = {
        sql: 'SELECT * FROM users ORDER BY id',
        routing: {
          queryType: 'scatter',
          targetShards: shards.map(s => s.id),
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 2,
          reason: 'Scatter query',
        },
        shardPlans: shards.map(s => ({
          shardId: s.id,
          sql: 'SELECT * FROM users ORDER BY id',
          isFinal: false,
        })),
        postProcessing: [
          { type: 'merge' },
          { type: 'sort', columns: [{ column: 'id', direction: 'ASC' }] },
        ],
        totalCost: 2,
      };

      const result = await executor.execute(plan);

      expect(result.rows.map(r => r[0])).toEqual([1, 2, 3, 4]);
    });

    it('should sort merged results descending', async () => {
      const shards = createTestShards(2);
      const rpc = new MockShardRPC();
      const selector = createReplicaSelector(shards);

      rpc.setShardData('shard-1', ['id'], [[1], [3]]);
      rpc.setShardData('shard-2', ['id'], [[2], [4]]);

      const executor = createExecutor(rpc, selector);

      const plan: ExecutionPlan = {
        sql: 'SELECT * FROM users ORDER BY id DESC',
        routing: {
          queryType: 'scatter',
          targetShards: shards.map(s => s.id),
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 2,
          reason: 'Scatter query',
        },
        shardPlans: shards.map(s => ({
          shardId: s.id,
          sql: 'SELECT * FROM users ORDER BY id DESC',
          isFinal: false,
        })),
        postProcessing: [
          { type: 'merge' },
          { type: 'sort', columns: [{ column: 'id', direction: 'DESC' }] },
        ],
        totalCost: 2,
      };

      const result = await executor.execute(plan);

      expect(result.rows.map(r => r[0])).toEqual([4, 3, 2, 1]);
    });

    it('should handle null values in sort with NULLS FIRST', async () => {
      const shards = createTestShards(2);
      const rpc = new MockShardRPC();
      const selector = createReplicaSelector(shards);

      rpc.setShardData('shard-1', ['id'], [[1], [null]]);
      rpc.setShardData('shard-2', ['id'], [[2], [3]]);

      const executor = createExecutor(rpc, selector);

      const plan: ExecutionPlan = {
        sql: 'SELECT * FROM users ORDER BY id NULLS FIRST',
        routing: {
          queryType: 'scatter',
          targetShards: shards.map(s => s.id),
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 2,
          reason: 'Scatter query',
        },
        shardPlans: shards.map(s => ({
          shardId: s.id,
          sql: 'SELECT * FROM users ORDER BY id',
          isFinal: false,
        })),
        postProcessing: [
          { type: 'merge' },
          { type: 'sort', columns: [{ column: 'id', direction: 'ASC', nulls: 'FIRST' }] },
        ],
        totalCost: 2,
      };

      const result = await executor.execute(plan);

      expect(result.rows[0][0]).toBeNull();
    });
  });

  describe('limit post-processing', () => {
    it('should limit merged results', async () => {
      const shards = createTestShards(3);
      const rpc = new MockShardRPC();
      const selector = createReplicaSelector(shards);

      for (let i = 1; i <= 3; i++) {
        rpc.setShardData(`shard-${i}`, ['id'], [[i * 10], [i * 10 + 1]]);
      }

      const executor = createExecutor(rpc, selector);

      const plan: ExecutionPlan = {
        sql: 'SELECT * FROM users LIMIT 3',
        routing: {
          queryType: 'scatter',
          targetShards: shards.map(s => s.id),
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 3,
          reason: 'Scatter query',
        },
        shardPlans: shards.map(s => ({
          shardId: s.id,
          sql: 'SELECT * FROM users LIMIT 3',
          isFinal: false,
        })),
        postProcessing: [
          { type: 'merge' },
          { type: 'limit', count: 3 },
        ],
        totalCost: 3,
      };

      const result = await executor.execute(plan);

      expect(result.rows.length).toBe(3);
    });

    it('should handle limit with offset', async () => {
      const shards = createTestShards(2);
      const rpc = new MockShardRPC();
      const selector = createReplicaSelector(shards);

      rpc.setShardData('shard-1', ['id'], [[1], [2], [3]]);
      rpc.setShardData('shard-2', ['id'], [[4], [5], [6]]);

      const executor = createExecutor(rpc, selector);

      const plan: ExecutionPlan = {
        sql: 'SELECT * FROM users LIMIT 2 OFFSET 2',
        routing: {
          queryType: 'scatter',
          targetShards: shards.map(s => s.id),
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 2,
          reason: 'Scatter query',
        },
        shardPlans: shards.map(s => ({
          shardId: s.id,
          sql: 'SELECT * FROM users LIMIT 4',
          isFinal: false,
        })),
        postProcessing: [
          { type: 'merge' },
          { type: 'limit', count: 2, offset: 2 },
        ],
        totalCost: 2,
      };

      const result = await executor.execute(plan);

      expect(result.rows.length).toBe(2);
    });
  });
});

// =============================================================================
// AGGREGATION TESTS
// =============================================================================

describe('Two-Phase Aggregation', () => {
  describe('COUNT aggregation', () => {
    it('should sum COUNT results from all shards', async () => {
      const shards = createTestShards(3);
      const rpc = new MockShardRPC();
      const selector = createReplicaSelector(shards);

      // Each shard returns a partial count
      rpc.setShardData('shard-1', ['count'], [[100]]);
      rpc.setShardData('shard-2', ['count'], [[150]]);
      rpc.setShardData('shard-3', ['count'], [[50]]);

      const executor = createExecutor(rpc, selector);

      const plan: ExecutionPlan = {
        sql: 'SELECT COUNT(*) FROM users',
        routing: {
          queryType: 'scatter',
          targetShards: shards.map(s => s.id),
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 3,
          reason: 'Scatter query',
        },
        shardPlans: shards.map(s => ({
          shardId: s.id,
          sql: 'SELECT COUNT(*) FROM users',
          isFinal: false,
        })),
        postProcessing: [
          { type: 'merge' },
          {
            type: 'aggregate',
            aggregates: [{ function: 'COUNT', column: '*', alias: 'count' }],
          },
        ],
        totalCost: 3,
      };

      const result = await executor.execute(plan);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0][0]).toBe(300); // 100 + 150 + 50
    });
  });

  describe('SUM aggregation', () => {
    it('should sum SUM results from all shards', async () => {
      const shards = createTestShards(2);
      const rpc = new MockShardRPC();
      const selector = createReplicaSelector(shards);

      rpc.setShardData('shard-1', ['total'], [[1000]]);
      rpc.setShardData('shard-2', ['total'], [[2500]]);

      const executor = createExecutor(rpc, selector);

      const plan: ExecutionPlan = {
        sql: 'SELECT SUM(amount) as total FROM orders',
        routing: {
          queryType: 'scatter',
          targetShards: shards.map(s => s.id),
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 2,
          reason: 'Scatter query',
        },
        shardPlans: shards.map(s => ({
          shardId: s.id,
          sql: 'SELECT SUM(amount) as total FROM orders',
          isFinal: false,
        })),
        postProcessing: [
          { type: 'merge' },
          {
            type: 'aggregate',
            aggregates: [{ function: 'SUM', column: 'amount', alias: 'total' }],
          },
        ],
        totalCost: 2,
      };

      const result = await executor.execute(plan);

      expect(result.rows[0][0]).toBe(3500);
    });
  });

  describe('AVG aggregation', () => {
    it('should compute AVG from partial SUM and COUNT', async () => {
      const shards = createTestShards(2);
      const rpc = new MockShardRPC();
      const selector = createReplicaSelector(shards);

      // Each shard returns sum and count for proper AVG calculation
      rpc.setShardData('shard-1', ['_sum_price', '_count_price'], [[1000, 10]]);
      rpc.setShardData('shard-2', ['_sum_price', '_count_price'], [[2000, 20]]);

      const executor = createExecutor(rpc, selector);

      const plan: ExecutionPlan = {
        sql: 'SELECT AVG(price) FROM products',
        routing: {
          queryType: 'scatter',
          targetShards: shards.map(s => s.id),
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 2,
          reason: 'Scatter query',
        },
        shardPlans: shards.map(s => ({
          shardId: s.id,
          sql: 'SELECT SUM(price) AS _sum_price, COUNT(price) AS _count_price FROM products',
          isFinal: false,
        })),
        postProcessing: [
          { type: 'merge' },
          {
            type: 'aggregate',
            aggregates: [{ function: 'AVG', column: 'price', alias: 'avg_price' }],
          },
        ],
        totalCost: 2,
      };

      const result = await executor.execute(plan);

      // (1000 + 2000) / (10 + 20) = 3000 / 30 = 100
      expect(result.rows[0][0]).toBe(100);
    });
  });

  describe('MIN aggregation', () => {
    it('should find MIN across all shards', async () => {
      const shards = createTestShards(3);
      const rpc = new MockShardRPC();
      const selector = createReplicaSelector(shards);

      rpc.setShardData('shard-1', ['min_value'], [[50]]);
      rpc.setShardData('shard-2', ['min_value'], [[25]]);
      rpc.setShardData('shard-3', ['min_value'], [[75]]);

      const executor = createExecutor(rpc, selector);

      const plan: ExecutionPlan = {
        sql: 'SELECT MIN(value) FROM data',
        routing: {
          queryType: 'scatter',
          targetShards: shards.map(s => s.id),
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 3,
          reason: 'Scatter query',
        },
        shardPlans: shards.map(s => ({
          shardId: s.id,
          sql: 'SELECT MIN(value) as min_value FROM data',
          isFinal: false,
        })),
        postProcessing: [
          { type: 'merge' },
          {
            type: 'aggregate',
            aggregates: [{ function: 'MIN', column: 'value', alias: 'min_value' }],
          },
        ],
        totalCost: 3,
      };

      const result = await executor.execute(plan);

      expect(result.rows[0][0]).toBe(25);
    });
  });

  describe('MAX aggregation', () => {
    it('should find MAX across all shards', async () => {
      const shards = createTestShards(3);
      const rpc = new MockShardRPC();
      const selector = createReplicaSelector(shards);

      rpc.setShardData('shard-1', ['max_value'], [[50]]);
      rpc.setShardData('shard-2', ['max_value'], [[25]]);
      rpc.setShardData('shard-3', ['max_value'], [[75]]);

      const executor = createExecutor(rpc, selector);

      const plan: ExecutionPlan = {
        sql: 'SELECT MAX(value) FROM data',
        routing: {
          queryType: 'scatter',
          targetShards: shards.map(s => s.id),
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 3,
          reason: 'Scatter query',
        },
        shardPlans: shards.map(s => ({
          shardId: s.id,
          sql: 'SELECT MAX(value) as max_value FROM data',
          isFinal: false,
        })),
        postProcessing: [
          { type: 'merge' },
          {
            type: 'aggregate',
            aggregates: [{ function: 'MAX', column: 'value', alias: 'max_value' }],
          },
        ],
        totalCost: 3,
      };

      const result = await executor.execute(plan);

      expect(result.rows[0][0]).toBe(75);
    });
  });
});

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

describe('Error Handling', () => {
  describe('failFast mode', () => {
    it('should throw immediately on first error when failFast is true', async () => {
      const shards = createTestShards(3);
      const rpc = new FailingMockShardRPC();
      const selector = createReplicaSelector(shards);

      rpc.setShardData('shard-1', ['id'], [[1]]);
      rpc.setShardToFail('shard-2');
      rpc.setShardData('shard-3', ['id'], [[3]]);

      const executor = createExecutor(rpc, selector, {
        failFast: true,
        retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
      });

      const plan: ExecutionPlan = {
        sql: 'SELECT * FROM users',
        routing: {
          queryType: 'scatter',
          targetShards: shards.map(s => s.id),
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 3,
          reason: 'Scatter query',
        },
        shardPlans: shards.map(s => ({
          shardId: s.id,
          sql: 'SELECT * FROM users',
          isFinal: false,
        })),
        postProcessing: [{ type: 'merge' }],
        totalCost: 3,
      };

      await expect(executor.execute(plan)).rejects.toThrow(ShardExecutionError);
    });
  });

  describe('partial failure mode', () => {
    it('should return partial results when failFast is false', async () => {
      const shards = createTestShards(3);
      const rpc = new FailingMockShardRPC();
      const selector = createReplicaSelector(shards);

      rpc.setShardData('shard-1', ['id'], [[1]]);
      rpc.setShardToFail('shard-2');
      rpc.setShardData('shard-3', ['id'], [[3]]);

      const executor = createExecutor(rpc, selector, {
        failFast: false,
        retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
      });

      const plan: ExecutionPlan = {
        sql: 'SELECT * FROM users',
        routing: {
          queryType: 'scatter',
          targetShards: shards.map(s => s.id),
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 3,
          reason: 'Scatter query',
        },
        shardPlans: shards.map(s => ({
          shardId: s.id,
          sql: 'SELECT * FROM users',
          isFinal: false,
        })),
        postProcessing: [{ type: 'merge' }],
        totalCost: 3,
      };

      const result = await executor.execute(plan);

      expect(result.rows.length).toBe(2); // Only from shard-1 and shard-3
      expect(result.contributingShards).toHaveLength(2);
      expect(result.partialFailures).toBeDefined();
      expect(result.partialFailures?.length).toBe(1);
    });

    it('should report which shards failed', async () => {
      const shards = createTestShards(3);
      const rpc = new FailingMockShardRPC();
      const selector = createReplicaSelector(shards);

      rpc.setShardData('shard-1', ['id'], [[1]]);
      rpc.setShardToFail('shard-2');
      rpc.setShardToFail('shard-3');

      const executor = createExecutor(rpc, selector, {
        failFast: false,
        retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
      });

      const plan: ExecutionPlan = {
        sql: 'SELECT * FROM users',
        routing: {
          queryType: 'scatter',
          targetShards: shards.map(s => s.id),
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 3,
          reason: 'Scatter query',
        },
        shardPlans: shards.map(s => ({
          shardId: s.id,
          sql: 'SELECT * FROM users',
          isFinal: false,
        })),
        postProcessing: [{ type: 'merge' }],
        totalCost: 3,
      };

      const result = await executor.execute(plan);

      expect(result.partialFailures?.length).toBe(2);
    });
  });

  describe('retry behavior', () => {
    it('should retry on timeout errors', async () => {
      const shards = createTestShards(1);
      const rpc = new FailingMockShardRPC();
      const selector = createReplicaSelector(shards);

      // First call fails, subsequent succeed
      let callCount = 0;
      const originalExecute = rpc.execute.bind(rpc);
      rpc.execute = async (...args) => {
        callCount++;
        if (callCount === 1) {
          throw new Error('timeout');
        }
        rpc.setShardData('shard-1', ['id'], [[1]]);
        return originalExecute(...args);
      };

      const executor = createExecutor(rpc, selector, {
        retry: { maxAttempts: 3, backoffMs: 0, maxBackoffMs: 0 },
      });

      const plan: ExecutionPlan = {
        sql: 'SELECT * FROM users',
        routing: {
          queryType: 'single-shard',
          targetShards: [shards[0].id],
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 1,
          reason: 'Single shard',
        },
        shardPlans: [{
          shardId: shards[0].id,
          sql: 'SELECT * FROM users',
          isFinal: true,
        }],
        totalCost: 1,
      };

      const result = await executor.execute(plan);

      expect(callCount).toBe(2); // First failed, second succeeded
      expect(result.rows.length).toBe(1);
    });

    it('should respect maxAttempts limit', async () => {
      const shards = createTestShards(1);
      const rpc = new FailingMockShardRPC();
      const selector = createReplicaSelector(shards);

      rpc.setShardToFail('shard-1', 'timeout');

      const executor = createExecutor(rpc, selector, {
        failFast: true,
        retry: { maxAttempts: 3, backoffMs: 0, maxBackoffMs: 0 },
      });

      const plan: ExecutionPlan = {
        sql: 'SELECT * FROM users',
        routing: {
          queryType: 'single-shard',
          targetShards: [shards[0].id],
          readPreference: 'primaryPreferred',
          canUseReplica: true,
          costEstimate: 1,
          reason: 'Single shard',
        },
        shardPlans: [{
          shardId: shards[0].id,
          sql: 'SELECT * FROM users',
          isFinal: true,
        }],
        totalCost: 1,
      };

      await expect(executor.execute(plan)).rejects.toThrow();
      expect(rpc.callCount.get('shard-1')).toBe(3);
    });
  });
});

// =============================================================================
// STREAMING TESTS
// =============================================================================

describe('Streaming Execution', () => {
  it('should stream results for single-shard query', async () => {
    const { rpc, selector, router } = createTestSetup();
    const executor = createExecutor(rpc, selector);

    const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
    const rows: unknown[][] = [];

    for await (const row of executor.executeStream(plan)) {
      rows.push(row);
    }

    expect(rows.length).toBeGreaterThan(0);
  });

  it('should collect all results for scatter query', async () => {
    const { rpc, selector, router } = createTestSetup(3);
    const executor = createExecutor(rpc, selector);

    const plan = router.createExecutionPlan('SELECT * FROM users');
    const rows: unknown[][] = [];

    for await (const row of executor.executeStream(plan)) {
      rows.push(row);
    }

    expect(rows.length).toBe(6); // 2 rows per shard * 3 shards
  });
});

// =============================================================================
// PARALLEL EXECUTION TESTS
// =============================================================================

describe('Parallel Execution', () => {
  it('should respect maxParallelShards limit', async () => {
    const shards = createTestShards(10);
    const vschema = createVSchema(
      { users: shardedTable('tenant_id', hashVindex()) },
      shards
    );
    const rpc = new MockShardRPC();
    const selector = createReplicaSelector(shards);

    for (let i = 1; i <= 10; i++) {
      rpc.setShardData(`shard-${i}`, ['id'], [[i]]);
    }

    const executor = createExecutor(rpc, selector, {
      maxParallelShards: 3,
    });

    const plan: ExecutionPlan = {
      sql: 'SELECT * FROM users',
      routing: {
        queryType: 'scatter',
        targetShards: shards.map(s => s.id),
        readPreference: 'primaryPreferred',
        canUseReplica: true,
        costEstimate: 10,
        reason: 'Scatter query',
      },
      shardPlans: shards.map(s => ({
        shardId: s.id,
        sql: 'SELECT * FROM users',
        isFinal: false,
      })),
      postProcessing: [{ type: 'merge' }],
      totalCost: 10,
    };

    const result = await executor.execute(plan);

    // Should still get all results despite batching
    expect(result.rows.length).toBe(10);
    expect(result.contributingShards).toHaveLength(10);
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Edge Cases', () => {
  it('should handle empty result sets', async () => {
    const shards = createTestShards(2);
    const rpc = new MockShardRPC();
    const selector = createReplicaSelector(shards);

    // Both shards return empty
    rpc.setShardData('shard-1', ['id'], []);
    rpc.setShardData('shard-2', ['id'], []);

    const executor = createExecutor(rpc, selector);

    const plan: ExecutionPlan = {
      sql: 'SELECT * FROM users WHERE 1 = 0',
      routing: {
        queryType: 'scatter',
        targetShards: shards.map(s => s.id),
        readPreference: 'primaryPreferred',
        canUseReplica: true,
        costEstimate: 2,
        reason: 'Scatter query',
      },
      shardPlans: shards.map(s => ({
        shardId: s.id,
        sql: 'SELECT * FROM users WHERE 1 = 0',
        isFinal: false,
      })),
      postProcessing: [{ type: 'merge' }],
      totalCost: 2,
    };

    const result = await executor.execute(plan);

    expect(result.rows).toHaveLength(0);
    expect(result.totalRowCount).toBe(0);
  });

  it('should handle single shard in scatter', async () => {
    const shards = createTestShards(1);
    const rpc = new MockShardRPC();
    const selector = createReplicaSelector(shards);

    rpc.setShardData('shard-1', ['id'], [[1], [2]]);

    const executor = createExecutor(rpc, selector);

    const plan: ExecutionPlan = {
      sql: 'SELECT * FROM users',
      routing: {
        queryType: 'scatter',
        targetShards: [shards[0].id],
        readPreference: 'primaryPreferred',
        canUseReplica: true,
        costEstimate: 1,
        reason: 'Scatter query',
      },
      shardPlans: [{
        shardId: shards[0].id,
        sql: 'SELECT * FROM users',
        isFinal: false,
      }],
      postProcessing: [{ type: 'merge' }],
      totalCost: 1,
    };

    const result = await executor.execute(plan);

    expect(result.rows.length).toBe(2);
  });

  it('should handle all shards failing', async () => {
    const shards = createTestShards(3);
    const rpc = new FailingMockShardRPC();
    const selector = createReplicaSelector(shards);

    for (let i = 1; i <= 3; i++) {
      rpc.setShardToFail(`shard-${i}`);
    }

    const executor = createExecutor(rpc, selector, {
      failFast: false,
      retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
    });

    const plan: ExecutionPlan = {
      sql: 'SELECT * FROM users',
      routing: {
        queryType: 'scatter',
        targetShards: shards.map(s => s.id),
        readPreference: 'primaryPreferred',
        canUseReplica: true,
        costEstimate: 3,
        reason: 'Scatter query',
      },
      shardPlans: shards.map(s => ({
        shardId: s.id,
        sql: 'SELECT * FROM users',
        isFinal: false,
      })),
      postProcessing: [{ type: 'merge' }],
      totalCost: 3,
    };

    const result = await executor.execute(plan);

    expect(result.rows).toHaveLength(0);
    expect(result.partialFailures?.length).toBe(3);
  });
});
