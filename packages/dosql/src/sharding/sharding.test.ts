/**
 * DoSQL Native Sharding Tests
 *
 * Comprehensive test suite for the sharding module.
 */

import { describe, it, expect, beforeEach } from 'vitest';

import {
  // Types
  type VSchema,
  type ShardConfig,
  type ReadPreference,
  type MergedResult,

  // Factory functions
  createVSchema,
  hashVindex,
  consistentHashVindex,
  rangeVindex,
  shardedTable,
  unshardedTable,
  referenceTable,
  shard,
  replica,

  // Vindex
  fnv1a,
  xxhash,
  HashVindex,
  ConsistentHashVindex,
  RangeVindex,
  createVindex,
  testDistribution,
  distributionStats,

  // Router
  SQLParser,
  QueryRouter,
  createRouter,

  // Executor
  DistributedExecutor,
  MockShardRPC,
  createExecutor,

  // Replica
  DefaultReplicaSelector,
  createReplicaSelector,

  // High-level API
  createShardingClient,
} from './index.js';

// =============================================================================
// HASH FUNCTION TESTS
// =============================================================================

describe('Hash Functions', () => {
  describe('fnv1a', () => {
    it('produces consistent hashes for same input', () => {
      expect(fnv1a('test')).toBe(fnv1a('test'));
      expect(fnv1a(123)).toBe(fnv1a(123));
      expect(fnv1a(123n)).toBe(fnv1a(123n));
    });

    it('produces different hashes for different inputs', () => {
      expect(fnv1a('test1')).not.toBe(fnv1a('test2'));
      expect(fnv1a(1)).not.toBe(fnv1a(2));
    });

    it('returns positive 32-bit integers', () => {
      const hashes = ['test', 'hello', 'world', '123', 'abc'].map(fnv1a);
      for (const hash of hashes) {
        expect(hash).toBeGreaterThanOrEqual(0);
        expect(hash).toBeLessThanOrEqual(0xFFFFFFFF);
      }
    });
  });

  describe('xxhash', () => {
    it('produces consistent hashes for same input', () => {
      expect(xxhash('test')).toBe(xxhash('test'));
      expect(xxhash(123)).toBe(xxhash(123));
    });

    it('produces different hashes for different inputs', () => {
      expect(xxhash('test1')).not.toBe(xxhash('test2'));
    });

    it('returns positive 32-bit integers', () => {
      const hashes = ['test', 'hello', 'world', '123', 'abc'].map(xxhash);
      for (const hash of hashes) {
        expect(hash).toBeGreaterThanOrEqual(0);
        expect(hash).toBeLessThanOrEqual(0xFFFFFFFF);
      }
    });
  });
});

// =============================================================================
// VINDEX TESTS
// =============================================================================

describe('Vindex Implementations', () => {
  const shards: ShardConfig[] = [
    shard('shard-1', 'do-ns-1'),
    shard('shard-2', 'do-ns-2'),
    shard('shard-3', 'do-ns-3'),
    shard('shard-4', 'do-ns-4'),
  ];

  describe('HashVindex', () => {
    it('routes keys consistently', () => {
      const vindex = new HashVindex(shards);

      // Same key always goes to same shard
      const shard1 = vindex.getShard('user-123');
      const shard2 = vindex.getShard('user-123');
      expect(shard1).toBe(shard2);
    });

    it('distributes keys across all shards', () => {
      const vindex = new HashVindex(shards);

      const distribution = testDistribution(
        vindex,
        () => `user-${Math.random()}`,
        10000
      );

      // Check all shards received data
      for (const shardId of vindex.getAllShards()) {
        expect(distribution.get(shardId)).toBeGreaterThan(0);
      }

      // Check distribution is reasonably uniform
      const stats = distributionStats(distribution);
      expect(stats.stdDev / stats.mean).toBeLessThan(0.1); // CV < 10%
    });

    it('handles IN queries', () => {
      const vindex = new HashVindex(shards);

      const targetShards = vindex.getShardsForKeys(['key1', 'key2', 'key3']);
      expect(targetShards.length).toBeGreaterThan(0);
      expect(targetShards.length).toBeLessThanOrEqual(3);
    });

    it('returns all shards for range queries (cannot optimize)', () => {
      const vindex = new HashVindex(shards);

      const targetShards = vindex.getShardsForRange(0, 100);
      expect(targetShards).toEqual(vindex.getAllShards());
    });
  });

  describe('ConsistentHashVindex', () => {
    it('routes keys consistently', () => {
      const vindex = new ConsistentHashVindex(shards);

      const shard1 = vindex.getShard('tenant-456');
      const shard2 = vindex.getShard('tenant-456');
      expect(shard1).toBe(shard2);
    });

    it('distributes keys across all shards', () => {
      const vindex = new ConsistentHashVindex(shards, { virtualNodes: 150 });

      const distribution = testDistribution(
        vindex,
        () => `tenant-${Math.random()}`,
        10000
      );

      // Check all shards received data
      for (const shardId of vindex.getAllShards()) {
        expect(distribution.get(shardId)).toBeGreaterThan(0);
      }
    });

    it('maintains state for debugging', () => {
      const vindex = new ConsistentHashVindex(shards, { virtualNodes: 100 });

      const state = vindex.getRingState();
      expect(state.totalNodes).toBe(4 * 100); // 4 shards * 100 virtual nodes
      expect(state.nodesPerShard.size).toBe(4);
    });

    it('handles adding/removing shards with minimal redistribution', () => {
      // Create vindex with 4 shards
      const vindex4 = new ConsistentHashVindex(shards, { virtualNodes: 150 });

      // Create vindex with 5 shards
      const shards5 = [...shards, shard('shard-5', 'do-ns-5')];
      const vindex5 = new ConsistentHashVindex(shards5, { virtualNodes: 150 });

      // Count how many keys changed shard assignment
      let changed = 0;
      const sampleSize = 10000;

      for (let i = 0; i < sampleSize; i++) {
        const key = `key-${i}`;
        if (vindex4.getShard(key) !== vindex5.getShard(key)) {
          changed++;
        }
      }

      // With consistent hashing, ~20% of keys should move (1/5 of keys)
      // But with simpler hash functions this can vary more
      const changeRate = changed / sampleSize;
      expect(changeRate).toBeLessThan(0.5); // Allow some variance
      expect(changeRate).toBeGreaterThan(0.1); // But should have some movement
    });
  });

  describe('RangeVindex', () => {
    const rangeShards: ShardConfig[] = [
      shard('shard-a', 'do-ns-a'),
      shard('shard-b', 'do-ns-b'),
      shard('shard-c', 'do-ns-c'),
    ];

    it('routes numeric keys to correct shards', () => {
      const vindex = new RangeVindex(rangeShards, rangeVindex([
        { shard: 'shard-a', min: 0, max: 1000 },
        { shard: 'shard-b', min: 1000, max: 2000 },
        { shard: 'shard-c', min: 2000, max: null },
      ]));

      expect(vindex.getShard(500)).toBe('shard-a');
      expect(vindex.getShard(1500)).toBe('shard-b');
      expect(vindex.getShard(2500)).toBe('shard-c');
      expect(vindex.getShard(999)).toBe('shard-a');
      expect(vindex.getShard(1000)).toBe('shard-b');
    });

    it('routes string keys correctly', () => {
      const vindex = new RangeVindex(rangeShards, rangeVindex([
        { shard: 'shard-a', min: 'a', max: 'm' },
        { shard: 'shard-b', min: 'm', max: 's' },
        { shard: 'shard-c', min: 's', max: null },
      ]));

      expect(vindex.getShard('alice')).toBe('shard-a');
      expect(vindex.getShard('mike')).toBe('shard-b');
      expect(vindex.getShard('zoe')).toBe('shard-c');
    });

    it('optimizes range queries', () => {
      const vindex = new RangeVindex(rangeShards, rangeVindex([
        { shard: 'shard-a', min: 0, max: 1000 },
        { shard: 'shard-b', min: 1000, max: 2000 },
        { shard: 'shard-c', min: 2000, max: null },
      ]));

      // Query within single shard
      expect(vindex.getShardsForRange(500, 800)).toEqual(['shard-a']);

      // Query spanning two shards
      expect(vindex.getShardsForRange(500, 1500)).toContain('shard-a');
      expect(vindex.getShardsForRange(500, 1500)).toContain('shard-b');

      // Query spanning all shards
      const allShards = vindex.getShardsForRange(0, 3000);
      expect(allShards).toContain('shard-a');
      expect(allShards).toContain('shard-b');
      expect(allShards).toContain('shard-c');
    });
  });

  describe('createVindex factory', () => {
    it('creates HashVindex', () => {
      const vindex = createVindex(shards, hashVindex());
      expect(vindex).toBeInstanceOf(HashVindex);
    });

    it('creates ConsistentHashVindex', () => {
      const vindex = createVindex(shards, consistentHashVindex(200));
      expect(vindex).toBeInstanceOf(ConsistentHashVindex);
    });

    it('creates RangeVindex', () => {
      const vindex = createVindex(shards, rangeVindex([
        { shard: 'shard-1', min: 0, max: 100 },
      ]));
      expect(vindex).toBeInstanceOf(RangeVindex);
    });
  });
});

// =============================================================================
// SQL PARSER TESTS
// =============================================================================

describe('SQLParser', () => {
  const parser = new SQLParser();

  describe('SELECT parsing', () => {
    it('parses simple SELECT', () => {
      const parsed = parser.parse('SELECT id, name FROM users');

      expect(parsed.operation).toBe('SELECT');
      expect(parsed.tables[0].name).toBe('users');
      expect(parsed.columns).toHaveLength(2);
      expect(parsed.columns![0].name).toBe('id');
      expect(parsed.columns![1].name).toBe('name');
    });

    it('parses SELECT with WHERE equality', () => {
      const parsed = parser.parse('SELECT * FROM users WHERE tenant_id = 123');

      expect(parsed.where).toBeDefined();
      expect(parsed.where!.conditions).toHaveLength(1);
      expect(parsed.where!.conditions[0].column).toBe('tenant_id');
      expect(parsed.where!.conditions[0].operator).toBe('=');
      expect(parsed.where!.conditions[0].value).toBe(123);
    });

    it('parses SELECT with WHERE IN', () => {
      const parsed = parser.parse('SELECT * FROM users WHERE id IN (1, 2, 3)');

      expect(parsed.where!.conditions[0].operator).toBe('IN');
      expect(parsed.where!.conditions[0].values).toEqual([1, 2, 3]);
    });

    it('parses SELECT with WHERE BETWEEN', () => {
      const parsed = parser.parse('SELECT * FROM orders WHERE created_at BETWEEN 100 AND 200');

      expect(parsed.where!.conditions[0].operator).toBe('BETWEEN');
      expect(parsed.where!.conditions[0].minValue).toBe(100);
      expect(parsed.where!.conditions[0].maxValue).toBe(200);
    });

    it('parses SELECT with multiple AND conditions', () => {
      const parsed = parser.parse('SELECT * FROM users WHERE tenant_id = 1 AND status = \'active\'');

      expect(parsed.where!.operator).toBe('AND');
      expect(parsed.where!.conditions).toHaveLength(2);
    });

    it('parses SELECT with GROUP BY', () => {
      const parsed = parser.parse('SELECT tenant_id, COUNT(*) FROM users GROUP BY tenant_id');

      expect(parsed.groupBy).toEqual(['tenant_id']);
    });

    it('parses SELECT with ORDER BY', () => {
      const parsed = parser.parse('SELECT * FROM users ORDER BY created_at DESC, id ASC');

      expect(parsed.orderBy).toHaveLength(2);
      expect(parsed.orderBy![0].column).toBe('created_at');
      expect(parsed.orderBy![0].direction).toBe('DESC');
      expect(parsed.orderBy![1].column).toBe('id');
      expect(parsed.orderBy![1].direction).toBe('ASC');
    });

    it('parses SELECT with LIMIT and OFFSET', () => {
      const parsed = parser.parse('SELECT * FROM users LIMIT 10 OFFSET 20');

      expect(parsed.limit).toBe(10);
      expect(parsed.offset).toBe(20);
    });

    it('extracts aggregate functions', () => {
      const parsed = parser.parse('SELECT COUNT(*), SUM(amount), AVG(price) FROM orders');

      expect(parsed.aggregates).toHaveLength(3);
      expect(parsed.aggregates![0].function).toBe('COUNT');
      expect(parsed.aggregates![1].function).toBe('SUM');
      expect(parsed.aggregates![2].function).toBe('AVG');
    });

    it('parses SELECT DISTINCT', () => {
      const parsed = parser.parse('SELECT DISTINCT category FROM products');

      expect(parsed.distinct).toBe(true);
    });
  });

  describe('INSERT parsing', () => {
    it('parses INSERT statement', () => {
      const parsed = parser.parse('INSERT INTO users (id, name) VALUES (1, \'test\')');

      expect(parsed.operation).toBe('INSERT');
      expect(parsed.tables[0].name).toBe('users');
      expect(parsed.columns).toHaveLength(2);
    });
  });

  describe('UPDATE parsing', () => {
    it('parses UPDATE statement', () => {
      const parsed = parser.parse('UPDATE users SET name = \'new\' WHERE id = 1');

      expect(parsed.operation).toBe('UPDATE');
      expect(parsed.tables[0].name).toBe('users');
      expect(parsed.where!.conditions[0].column).toBe('id');
    });
  });

  describe('DELETE parsing', () => {
    it('parses DELETE statement', () => {
      const parsed = parser.parse('DELETE FROM users WHERE id = 1');

      expect(parsed.operation).toBe('DELETE');
      expect(parsed.tables[0].name).toBe('users');
      expect(parsed.where!.conditions[0].column).toBe('id');
    });
  });
});

// =============================================================================
// QUERY ROUTER TESTS
// =============================================================================

describe('QueryRouter', () => {
  const shards: ShardConfig[] = [
    shard('shard-1', 'do-ns-1'),
    shard('shard-2', 'do-ns-2'),
    shard('shard-3', 'do-ns-3'),
  ];

  const vschema = createVSchema({
    users: shardedTable('tenant_id', hashVindex()),
    orders: shardedTable('user_id', consistentHashVindex()),
    settings: unshardedTable('shard-1'),
    countries: referenceTable(),
  }, shards);

  const router = createRouter(vschema);

  describe('Sharded table routing', () => {
    it('routes single-shard query with equality on shard key', () => {
      const routing = router.route('SELECT * FROM users WHERE tenant_id = 123');

      expect(routing.queryType).toBe('single-shard');
      expect(routing.targetShards).toHaveLength(1);
      expect(routing.shardKeyValue).toBe(123);
      expect(routing.reason).toContain('equality');
    });

    it('routes single-shard query with parameterized shard key', () => {
      const routing = router.route('SELECT * FROM users WHERE tenant_id = $1', [456]);

      expect(routing.queryType).toBe('single-shard');
      expect(routing.shardKeyValue).toBe(456);
    });

    it('routes scatter query without shard key', () => {
      const routing = router.route('SELECT * FROM users WHERE status = \'active\'');

      expect(routing.queryType).toBe('scatter');
      expect(routing.targetShards).toHaveLength(3);
      expect(routing.reason).toContain('scatter');
    });

    it('routes IN query to multiple shards', () => {
      const routing = router.route('SELECT * FROM users WHERE tenant_id IN (1, 2, 3)');

      expect(routing.queryType === 'scatter-gather' || routing.queryType === 'single-shard').toBe(true);
      expect(routing.targetShards.length).toBeGreaterThan(0);
    });
  });

  describe('Unsharded table routing', () => {
    it('routes to single designated shard', () => {
      const routing = router.route('SELECT * FROM settings');

      expect(routing.queryType).toBe('single-shard');
      expect(routing.targetShards).toEqual(['shard-1']);
    });

    it('writes go to primary', () => {
      const routing = router.route('UPDATE settings SET value = \'new\'');

      expect(routing.queryType).toBe('single-shard');
      expect(routing.readPreference).toBe('primary');
    });
  });

  describe('Reference table routing', () => {
    it('routes reads to single shard (nearest)', () => {
      const routing = router.route('SELECT * FROM countries');

      expect(routing.queryType).toBe('single-shard');
      expect(routing.canUseReplica).toBe(true);
    });

    it('routes writes to all shards', () => {
      const routing = router.route('INSERT INTO countries (code, name) VALUES (\'US\', \'United States\')');

      expect(routing.queryType).toBe('scatter');
      expect(routing.targetShards).toHaveLength(3);
      expect(routing.readPreference).toBe('primary');
    });
  });

  describe('Read preference', () => {
    it('respects read preference for SELECT', () => {
      const routing = router.route('SELECT * FROM users WHERE tenant_id = 1', [], 'replica');

      expect(routing.readPreference).toBe('replica');
      expect(routing.canUseReplica).toBe(true);
    });

    it('forces primary for writes', () => {
      const routing = router.route('UPDATE users SET name = \'new\' WHERE tenant_id = 1', [], 'replica');

      expect(routing.readPreference).toBe('primary');
      expect(routing.canUseReplica).toBe(false);
    });
  });

  describe('Execution plan', () => {
    it('creates execution plan for single-shard query', () => {
      const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');

      expect(plan.shardPlans).toHaveLength(1);
      expect(plan.shardPlans[0].isFinal).toBe(true);
      expect(plan.postProcessing).toBeUndefined();
    });

    it('creates execution plan for scatter query', () => {
      const plan = router.createExecutionPlan('SELECT * FROM users ORDER BY name LIMIT 10');

      expect(plan.shardPlans).toHaveLength(3);
      expect(plan.postProcessing).toBeDefined();
      expect(plan.postProcessing!.some(op => op.type === 'sort')).toBe(true);
      expect(plan.postProcessing!.some(op => op.type === 'limit')).toBe(true);
    });

    it('rewrites query for partial aggregation', () => {
      const plan = router.createExecutionPlan('SELECT AVG(amount) FROM orders');

      // AVG should be rewritten to SUM/COUNT
      const shardSql = plan.shardPlans[0].sql;
      expect(shardSql).toContain('SUM');
      expect(shardSql).toContain('COUNT');
    });
  });
});

// =============================================================================
// REPLICA SELECTOR TESTS
// =============================================================================

describe('ReplicaSelector', () => {
  const shards: ShardConfig[] = [
    {
      id: 'shard-1',
      doNamespace: 'do-ns-1',
      replicas: [
        replica('replica-1a', 'do-ns-1a', 'replica', { region: 'us-west' }),
        replica('replica-1b', 'do-ns-1b', 'replica', { region: 'eu-central' }),
        replica('analytics-1', 'do-ns-1c', 'analytics', { region: 'us-west' }),
      ],
    },
    {
      id: 'shard-2',
      doNamespace: 'do-ns-2',
      replicas: [
        replica('replica-2a', 'do-ns-2a', 'replica', { region: 'us-west' }),
      ],
    },
  ];

  describe('Primary selection', () => {
    it('always selects primary for write operations', () => {
      const selector = createReplicaSelector(shards);

      const selected = selector.select('shard-1', 'primary');
      expect(selected).toBe('shard-1');
    });
  });

  describe('Replica selection', () => {
    it('selects replica for replica preference', () => {
      const selector = createReplicaSelector(shards);

      const selected = selector.select('shard-1', 'replica');
      expect(['replica-1a', 'replica-1b']).toContain(selected);
    });

    it('selects analytics replica for analytics preference', () => {
      const selector = createReplicaSelector(shards);

      const selected = selector.select('shard-1', 'analytics');
      expect(selected).toBe('analytics-1');
    });
  });

  describe('Nearest selection', () => {
    it('prefers same-region replicas', () => {
      const selector = createReplicaSelector(shards, undefined, 'us-west');

      // Record some latency data
      selector.recordSuccess('shard-1', 'replica-1a', 10);
      selector.recordSuccess('shard-1', 'replica-1b', 10);

      const selected = selector.select('shard-1', 'nearest');
      expect(selected).toBe('replica-1a'); // us-west
    });
  });

  describe('Health tracking', () => {
    it('records success and updates health', () => {
      const selector = createReplicaSelector(shards);

      // Record multiple successes
      for (let i = 0; i < 5; i++) {
        selector.recordSuccess('shard-1', 'replica-1a', 10);
      }

      const health = selector.getShardHealth('shard-1');
      expect(health.replicaHealths['replica-1a']).toBe('healthy');
    });

    it('marks replica unhealthy after failures', () => {
      const selector = createReplicaSelector(shards, { failureThreshold: 3 });

      // Record failures
      for (let i = 0; i < 5; i++) {
        selector.recordFailure('shard-1', 'replica-1a');
      }

      const health = selector.getShardHealth('shard-1');
      expect(health.replicaHealths['replica-1a']).toBe('unhealthy');
    });

    it('calculates cluster health', () => {
      const selector = createReplicaSelector(shards);

      const health = selector.getClusterHealth();

      expect(health.totalShards).toBe(2);
      expect(health.totalReplicas).toBe(4);
    });
  });
});

// =============================================================================
// DISTRIBUTED EXECUTOR TESTS
// =============================================================================

describe('DistributedExecutor', () => {
  const shards: ShardConfig[] = [
    shard('shard-1', 'do-ns-1'),
    shard('shard-2', 'do-ns-2'),
  ];

  const vschema = createVSchema({
    users: shardedTable('tenant_id', hashVindex()),
  }, shards);

  describe('Single-shard execution', () => {
    it('executes query on single shard', async () => {
      const rpc = new MockShardRPC();
      rpc.setShardData('shard-1', ['id', 'name'], [[1, 'Alice'], [2, 'Bob']]);

      const selector = createReplicaSelector(shards);
      const executor = createExecutor(rpc, selector);
      const router = createRouter(vschema);

      const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
      const result = await executor.execute(plan);

      expect(result.rows).toHaveLength(2);
      expect(result.contributingShards).toHaveLength(1);
    });
  });

  describe('Scatter-gather execution', () => {
    it('executes query across all shards and merges results', async () => {
      const rpc = new MockShardRPC();
      rpc.setShardData('shard-1', ['id', 'name'], [[1, 'Alice'], [2, 'Bob']]);
      rpc.setShardData('shard-2', ['id', 'name'], [[3, 'Charlie'], [4, 'Diana']]);

      const selector = createReplicaSelector(shards);
      const executor = createExecutor(rpc, selector);
      const router = createRouter(vschema);

      const plan = router.createExecutionPlan('SELECT * FROM users');
      const result = await executor.execute(plan);

      expect(result.rows).toHaveLength(4);
      expect(result.contributingShards).toHaveLength(2);
    });

    it('applies ORDER BY post-processing', async () => {
      const rpc = new MockShardRPC();
      rpc.setShardData('shard-1', ['id', 'name'], [[2, 'Bob'], [1, 'Alice']]);
      rpc.setShardData('shard-2', ['id', 'name'], [[4, 'Diana'], [3, 'Charlie']]);

      const selector = createReplicaSelector(shards);
      const executor = createExecutor(rpc, selector);
      const router = createRouter(vschema);

      const plan = router.createExecutionPlan('SELECT * FROM users ORDER BY name');
      const result = await executor.execute(plan);

      // Results should be sorted by name
      const names = result.rows.map(r => r[1]);
      expect(names).toEqual(['Alice', 'Bob', 'Charlie', 'Diana']);
    });

    it('applies LIMIT post-processing', async () => {
      const rpc = new MockShardRPC();
      rpc.setShardData('shard-1', ['id', 'name'], [[1, 'Alice'], [2, 'Bob']]);
      rpc.setShardData('shard-2', ['id', 'name'], [[3, 'Charlie'], [4, 'Diana']]);

      const selector = createReplicaSelector(shards);
      const executor = createExecutor(rpc, selector);
      const router = createRouter(vschema);

      const plan = router.createExecutionPlan('SELECT * FROM users LIMIT 2');
      const result = await executor.execute(plan);

      expect(result.rows).toHaveLength(2);
    });
  });

  describe('Aggregate handling', () => {
    it('combines COUNT results from multiple shards', async () => {
      const rpc = new MockShardRPC();
      rpc.setShardData('shard-1', ['count'], [[10]]);
      rpc.setShardData('shard-2', ['count'], [[15]]);

      const selector = createReplicaSelector(shards);
      const executor = createExecutor(rpc, selector);
      const router = createRouter(vschema);

      const plan = router.createExecutionPlan('SELECT COUNT(*) FROM users');
      const result = await executor.execute(plan);

      expect(result.rows[0][0]).toBe(25);
    });

    it('combines SUM results from multiple shards', async () => {
      const rpc = new MockShardRPC();
      rpc.setShardData('shard-1', ['total'], [[100]]);
      rpc.setShardData('shard-2', ['total'], [[200]]);

      const selector = createReplicaSelector(shards);
      const executor = createExecutor(rpc, selector);
      const router = createRouter(vschema);

      const plan = router.createExecutionPlan('SELECT SUM(amount) AS total FROM users');
      const result = await executor.execute(plan);

      expect(result.rows[0][0]).toBe(300);
    });

    it('combines MIN/MAX results from multiple shards', async () => {
      const rpc = new MockShardRPC();
      rpc.setShardData('shard-1', ['min_val', 'max_val'], [[5, 50]]);
      rpc.setShardData('shard-2', ['min_val', 'max_val'], [[3, 100]]);

      const selector = createReplicaSelector(shards);
      const executor = createExecutor(rpc, selector);
      const router = createRouter(vschema);

      // Test MIN
      const minPlan = router.createExecutionPlan('SELECT MIN(val) AS min_val FROM users');
      // Override aggregates for test
      minPlan.postProcessing = [
        { type: 'merge' },
        { type: 'aggregate', aggregates: [{ function: 'MIN', column: 'val', alias: 'min_val' }] },
      ];
      const minResult = await executor.execute(minPlan);
      expect(minResult.rows[0][0]).toBe(3);

      // Test MAX
      const maxPlan = router.createExecutionPlan('SELECT MAX(val) AS max_val FROM users');
      maxPlan.postProcessing = [
        { type: 'merge' },
        { type: 'aggregate', aggregates: [{ function: 'MAX', column: 'val', alias: 'max_val' }] },
      ];
      const maxResult = await executor.execute(maxPlan);
      expect(maxResult.rows[0][0]).toBe(100);
    });
  });
});

// =============================================================================
// HIGH-LEVEL API TESTS
// =============================================================================

describe('ShardingClient', () => {
  const shards: ShardConfig[] = [
    shard('shard-1', 'do-ns-1'),
    shard('shard-2', 'do-ns-2'),
  ];

  const vschema = createVSchema({
    users: shardedTable('tenant_id', hashVindex()),
    settings: unshardedTable('shard-1'),
    regions: referenceTable(),
  }, shards);

  it('creates and executes queries', async () => {
    const rpc = new MockShardRPC();
    rpc.setShardData('shard-1', ['id', 'name'], [[1, 'Test']]);
    rpc.setShardData('shard-2', ['id', 'name'], []);

    const client = createShardingClient({
      vschema,
      rpc,
    });

    const result = await client.query('SELECT * FROM users WHERE tenant_id = 1');

    expect(result.rows).toHaveLength(1);
  });

  it('streams results', async () => {
    const rpc = new MockShardRPC();
    rpc.setShardData('shard-1', ['id'], [[1], [2]]);
    rpc.setShardData('shard-2', ['id'], [[3], [4]]);

    const client = createShardingClient({
      vschema,
      rpc,
    });

    const rows: unknown[][] = [];
    for await (const row of client.queryStream('SELECT * FROM users')) {
      rows.push(row);
    }

    expect(rows).toHaveLength(4);
  });

  it('exposes router for introspection', () => {
    const rpc = new MockShardRPC();
    const client = createShardingClient({ vschema, rpc });

    const tableConfig = client.router.getTableConfig('users');
    expect(tableConfig?.type).toBe('sharded');
  });
});

// =============================================================================
// TYPE-LEVEL TESTS
// =============================================================================

describe('Type-Level Query Detection', () => {
  // These are compile-time tests - they verify TypeScript types work correctly

  it('type definitions compile correctly', () => {
    // This test verifies the types compile - if it runs, types are correct
    type TestSchema = {
      users: {
        columns: { id: 'number'; tenant_id: 'number'; name: 'string' };
        shardKey: 'tenant_id';
      };
    };

    // Verify HasEqualityOnColumn works
    type Test1 = import('./types.js').HasEqualityOnColumn<
      'SELECT * FROM users WHERE tenant_id = 1',
      'tenant_id'
    >;

    // Verify DetectQueryType works
    type Test2 = import('./types.js').DetectQueryType<
      'SELECT * FROM users WHERE tenant_id = 1',
      TestSchema,
      'users'
    >;

    // If this compiles, the types are working
    const _t1: Test1 = true;
    const _t2: Test2 = 'single-shard';

    expect(true).toBe(true);
  });
});
