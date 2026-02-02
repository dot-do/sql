/**
 * QueryRouter Unit Tests
 *
 * Tests for the query routing functionality:
 * - Query parsing and table detection
 * - Shard key extraction from WHERE clauses
 * - Cost-based routing decisions
 * - Single-shard vs scatter query detection
 * - Execution plan generation
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach } from 'vitest';

import {
  QueryRouter,
  createRouter,
  CostEstimator,
  SQLParser,
  type CostEstimatorConfig,
  type QueryRouterOptions,
} from '../router.js';

import {
  createVSchema,
  createShardId,
  hashVindex,
  consistentHashVindex,
  rangeVindex,
  shardedTable,
  unshardedTable,
  referenceTable,
  shard,
  replica,
  type VSchema,
  type ShardConfig,
  type RangeBoundary,
  type ReadPreference,
} from '../types.js';

import { StatisticsStore, type TableStatistics } from '../../planner/stats.js';

// =============================================================================
// TEST FIXTURES
// =============================================================================

function createTestShards(count: number = 3): ShardConfig[] {
  return Array.from({ length: count }, (_, i) =>
    shard(createShardId(`shard-${i + 1}`), `do-ns-${i + 1}`, {
      replicas: [
        replica(`replica-${i + 1}-a`, `do-ns-${i + 1}-replica-a`, 'replica', { region: 'us-west' }),
        replica(`replica-${i + 1}-b`, `do-ns-${i + 1}-replica-b`, 'replica', { region: 'eu-central' }),
      ],
    })
  );
}

function createBasicVSchema(): VSchema {
  const shards = createTestShards(3);

  return createVSchema(
    {
      users: shardedTable('tenant_id', hashVindex()),
      orders: shardedTable('user_id', hashVindex()),
      products: shardedTable('category_id', consistentHashVindex()),
      config: unshardedTable(),
      countries: referenceTable(true),
    },
    shards,
    { defaultShard: shards[0].id }
  );
}

function createRangeVSchema(): VSchema {
  const shards = createTestShards(3);
  const boundaries: RangeBoundary<number>[] = [
    { shard: shards[0].id, min: 0, max: 1000 },
    { shard: shards[1].id, min: 1000, max: 2000 },
    { shard: shards[2].id, min: 2000, max: null },
  ];

  return createVSchema(
    {
      events: shardedTable('event_id', rangeVindex(boundaries)),
    },
    shards
  );
}

// =============================================================================
// SQL PARSER TESTS
// =============================================================================

describe('SQLParser', () => {
  const parser = new SQLParser();

  describe('SELECT parsing', () => {
    it('should parse simple SELECT query', () => {
      const parsed = parser.parse('SELECT * FROM users');

      expect(parsed.operation).toBe('SELECT');
      expect(parsed.tables).toHaveLength(1);
      expect(parsed.tables[0].name).toBe('users');
    });

    it('should parse SELECT with specific columns', () => {
      const parsed = parser.parse('SELECT id, name, email FROM users');

      expect(parsed.operation).toBe('SELECT');
      expect(parsed.columns).toBeDefined();
      expect(parsed.columns?.length).toBeGreaterThan(0);
    });

    it('should parse SELECT with WHERE clause', () => {
      const parsed = parser.parse('SELECT * FROM users WHERE id = 123');

      expect(parsed.where).toBeDefined();
      expect(parsed.where?.conditions).toHaveLength(1);
      expect(parsed.where?.conditions[0].column).toBe('id');
      expect(parsed.where?.conditions[0].operator).toBe('=');
      expect(parsed.where?.conditions[0].value).toBe(123);
    });

    it('should parse SELECT with multiple WHERE conditions', () => {
      const parsed = parser.parse("SELECT * FROM users WHERE id = 123 AND status = 'active'");

      expect(parsed.where?.conditions.length).toBeGreaterThanOrEqual(2);
    });

    it('should parse SELECT with IN clause', () => {
      const parsed = parser.parse('SELECT * FROM users WHERE id IN (1, 2, 3)');

      expect(parsed.where).toBeDefined();
      const inCondition = parsed.where?.conditions.find(c => c.operator === 'IN');
      expect(inCondition).toBeDefined();
      expect(inCondition?.values).toEqual([1, 2, 3]);
    });

    it('should parse SELECT with BETWEEN clause', () => {
      const parsed = parser.parse('SELECT * FROM users WHERE id BETWEEN 1 AND 100');

      expect(parsed.where).toBeDefined();
      const betweenCondition = parsed.where?.conditions.find(c => c.operator === 'BETWEEN');
      expect(betweenCondition).toBeDefined();
      expect(betweenCondition?.minValue).toBe(1);
      expect(betweenCondition?.maxValue).toBe(100);
    });

    it('should parse SELECT with ORDER BY', () => {
      const parsed = parser.parse('SELECT * FROM users ORDER BY created_at DESC');

      expect(parsed.orderBy).toBeDefined();
      expect(parsed.orderBy?.[0].column).toBe('created_at');
      expect(parsed.orderBy?.[0].direction).toBe('DESC');
    });

    it('should parse SELECT with LIMIT', () => {
      const parsed = parser.parse('SELECT * FROM users LIMIT 10');

      expect(parsed.limit).toBe(10);
    });

    it('should parse SELECT with LIMIT and OFFSET', () => {
      const parsed = parser.parse('SELECT * FROM users LIMIT 10 OFFSET 20');

      expect(parsed.limit).toBe(10);
      expect(parsed.offset).toBe(20);
    });

    it('should parse SELECT DISTINCT', () => {
      const parsed = parser.parse('SELECT DISTINCT status FROM users');

      expect(parsed.distinct).toBe(true);
    });

    it('should parse aggregate functions', () => {
      const parsed = parser.parse('SELECT COUNT(*), SUM(amount), AVG(price) FROM orders');

      expect(parsed.aggregates).toBeDefined();
      expect(parsed.aggregates?.length).toBeGreaterThan(0);
    });
  });

  describe('INSERT parsing', () => {
    it('should parse simple INSERT', () => {
      const parsed = parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')");

      expect(parsed.operation).toBe('INSERT');
      expect(parsed.tables[0].name).toBe('users');
    });
  });

  describe('UPDATE parsing', () => {
    it('should parse simple UPDATE', () => {
      const parsed = parser.parse("UPDATE users SET name = 'Bob' WHERE id = 1");

      expect(parsed.operation).toBe('UPDATE');
      expect(parsed.tables[0].name).toBe('users');
      expect(parsed.where).toBeDefined();
    });
  });

  describe('DELETE parsing', () => {
    it('should parse simple DELETE', () => {
      const parsed = parser.parse('DELETE FROM users WHERE id = 1');

      expect(parsed.operation).toBe('DELETE');
      expect(parsed.tables[0].name).toBe('users');
      expect(parsed.where).toBeDefined();
    });
  });

  describe('table alias handling', () => {
    it('should parse table with alias', () => {
      const parsed = parser.parse('SELECT u.id FROM users u WHERE u.id = 1');

      expect(parsed.tables[0].name).toBe('users');
      expect(parsed.tables[0].alias).toBe('u');
    });

    it('should parse table with AS alias', () => {
      const parsed = parser.parse('SELECT users.id FROM users AS u WHERE u.id = 1');

      expect(parsed.tables[0].name).toBe('users');
    });
  });

  describe('parameter placeholders', () => {
    it('should parse $N placeholders', () => {
      const parsed = parser.parse('SELECT * FROM users WHERE id = $1');

      expect(parsed.where?.conditions[0].value).toEqual({ placeholder: '$1' });
    });

    it('should parse ? placeholders', () => {
      const parsed = parser.parse('SELECT * FROM users WHERE id = ?');

      expect(parsed.where?.conditions[0].value).toEqual({ placeholder: '?' });
    });
  });
});

// =============================================================================
// QUERY ROUTER TESTS
// =============================================================================

describe('QueryRouter', () => {
  let vschema: VSchema;
  let router: QueryRouter;

  beforeEach(() => {
    vschema = createBasicVSchema();
    router = createRouter(vschema);
  });

  describe('constructor', () => {
    it('should create router with VSchema', () => {
      expect(router).toBeInstanceOf(QueryRouter);
    });

    it('should accept cost estimator options', () => {
      const statsStore = new StatisticsStore();
      const routerWithStats = createRouter(vschema, { statsStore });

      expect(routerWithStats).toBeInstanceOf(QueryRouter);
    });

    it('should accept custom cost config', () => {
      const costConfig: Partial<CostEstimatorConfig> = {
        defaultRowCount: 50000,
        perShardNetworkCost: 2.0,
      };
      const routerWithConfig = createRouter(vschema, { costConfig });

      expect(routerWithConfig).toBeInstanceOf(QueryRouter);
    });
  });

  describe('single-shard routing', () => {
    it('should route to single shard with equality on shard key', () => {
      const routing = router.route('SELECT * FROM users WHERE tenant_id = 123');

      expect(routing.queryType).toBe('single-shard');
      expect(routing.targetShards).toHaveLength(1);
      expect(routing.shardKeyValue).toBe(123);
    });

    it('should route to single shard with string shard key', () => {
      const routing = router.route("SELECT * FROM users WHERE tenant_id = 'abc'");

      expect(routing.queryType).toBe('single-shard');
      expect(routing.shardKeyValue).toBe('abc');
    });

    it('should route to single shard with parameter placeholder', () => {
      const routing = router.route('SELECT * FROM users WHERE tenant_id = $1', [456]);

      expect(routing.queryType).toBe('single-shard');
      expect(routing.shardKeyValue).toBe(456);
    });

    it('should route INSERT to single shard based on shard key value', () => {
      const routing = router.route("INSERT INTO users (tenant_id, name) VALUES (123, 'Alice')");

      // INSERT routing depends on parsed shard key value
      expect(routing.targetShards.length).toBeGreaterThanOrEqual(1);
    });

    it('should route UPDATE to single shard with shard key in WHERE', () => {
      const routing = router.route("UPDATE users SET name = 'Bob' WHERE tenant_id = 123");

      expect(routing.queryType).toBe('single-shard');
      expect(routing.shardKeyValue).toBe(123);
    });

    it('should route DELETE to single shard with shard key in WHERE', () => {
      const routing = router.route('DELETE FROM users WHERE tenant_id = 123');

      expect(routing.queryType).toBe('single-shard');
    });
  });

  describe('scatter query routing', () => {
    it('should scatter when no shard key in WHERE', () => {
      const routing = router.route('SELECT * FROM users WHERE status = \'active\'');

      expect(routing.queryType).toBe('scatter');
      expect(routing.targetShards).toHaveLength(3);
    });

    it('should scatter when shard key has OR condition', () => {
      const routing = router.route('SELECT * FROM users WHERE tenant_id = 1 OR tenant_id = 2');

      expect(routing.queryType).toBe('scatter');
    });

    it('should scatter for DELETE without WHERE', () => {
      const routing = router.route('DELETE FROM users');

      expect(routing.queryType).toBe('scatter');
    });
  });

  describe('scatter-gather routing with IN clause', () => {
    it('should route to multiple shards for IN clause', () => {
      const routing = router.route('SELECT * FROM users WHERE tenant_id IN (1, 2, 3)');

      // IN clause should target only the shards containing those values
      expect(routing.queryType).toBe('scatter-gather');
      expect(routing.targetShards.length).toBeGreaterThanOrEqual(1);
      expect(routing.targetShards.length).toBeLessThanOrEqual(3);
    });

    it('should route to single shard if IN values hash to same shard', () => {
      // Create single-shard vschema to guarantee single shard result
      const singleShardVSchema = createVSchema(
        { users: shardedTable('tenant_id', hashVindex()) },
        createTestShards(1)
      );
      const singleRouter = createRouter(singleShardVSchema);

      const routing = singleRouter.route('SELECT * FROM users WHERE tenant_id IN (1, 2, 3)');

      expect(routing.queryType).toBe('single-shard');
      expect(routing.targetShards).toHaveLength(1);
    });
  });

  describe('range query routing', () => {
    it('should route BETWEEN to range vindex shards', () => {
      const rangeSchema = createRangeVSchema();
      const rangeRouter = createRouter(rangeSchema);

      const routing = rangeRouter.route('SELECT * FROM events WHERE event_id BETWEEN 500 AND 1500');

      // Should hit shards covering ranges [0, 1000) and [1000, 2000)
      expect(routing.targetShards.length).toBe(2);
    });

    it('should route to single shard for narrow range', () => {
      const rangeSchema = createRangeVSchema();
      const rangeRouter = createRouter(rangeSchema);

      const routing = rangeRouter.route('SELECT * FROM events WHERE event_id BETWEEN 100 AND 200');

      expect(routing.queryType).toBe('single-shard');
      expect(routing.targetShards).toHaveLength(1);
    });
  });

  describe('unsharded table routing', () => {
    it('should route unsharded table to single shard', () => {
      const routing = router.route('SELECT * FROM config');

      expect(routing.queryType).toBe('single-shard');
      expect(routing.targetShards).toHaveLength(1);
      expect(routing.reason).toContain('Unsharded');
    });

    it('should route unsharded INSERT to single shard', () => {
      const routing = router.route("INSERT INTO config (key, value) VALUES ('setting', 'value')");

      expect(routing.queryType).toBe('single-shard');
    });
  });

  describe('reference table routing', () => {
    it('should route reference table SELECT to single shard', () => {
      const routing = router.route('SELECT * FROM countries');

      expect(routing.queryType).toBe('single-shard');
      expect(routing.canUseReplica).toBe(true);
    });

    it('should route reference table INSERT to all shards', () => {
      const routing = router.route("INSERT INTO countries (code, name) VALUES ('US', 'United States')");

      expect(routing.queryType).toBe('scatter');
      expect(routing.targetShards).toHaveLength(3);
    });
  });

  describe('read preference', () => {
    it('should use primary for writes', () => {
      const routing = router.route("INSERT INTO users (tenant_id, name) VALUES (1, 'Alice')");

      expect(routing.readPreference).toBe('primary');
      expect(routing.canUseReplica).toBe(false);
    });

    it('should use primaryPreferred by default for reads', () => {
      const routing = router.route('SELECT * FROM users WHERE tenant_id = 1');

      expect(routing.readPreference).toBe('primaryPreferred');
      expect(routing.canUseReplica).toBe(true);
    });

    it('should respect explicit read preference', () => {
      const routing = router.route('SELECT * FROM users WHERE tenant_id = 1', undefined, 'replica');

      expect(routing.readPreference).toBe('replica');
    });

    it('should force primary for writes regardless of preference', () => {
      const routing = router.route("UPDATE users SET name = 'Bob' WHERE tenant_id = 1", undefined, 'replica');

      expect(routing.readPreference).toBe('primary');
    });
  });

  describe('error handling', () => {
    it('should throw for unknown table', () => {
      expect(() => router.route('SELECT * FROM unknown_table')).toThrow(
        "Table 'unknown_table' not found in VSchema"
      );
    });

    it('should throw when table cannot be determined', () => {
      expect(() => router.route('SELECT 1 + 1')).toThrow(
        'Could not determine target table from query'
      );
    });
  });
});

// =============================================================================
// COST ESTIMATOR TESTS
// =============================================================================

describe('CostEstimator', () => {
  let estimator: CostEstimator;

  beforeEach(() => {
    estimator = new CostEstimator();
  });

  describe('with default settings', () => {
    it('should estimate single-shard equality cost', () => {
      const cost = estimator.estimateSingleShardEqualityCost('users', 'id', 123);

      expect(cost).toBeGreaterThan(0);
    });

    it('should estimate IN list cost', () => {
      const cost = estimator.estimateInListCost('users', 'id', [1, 2, 3], 2);

      expect(cost).toBeGreaterThan(0);
    });

    it('should estimate range cost', () => {
      const cost = estimator.estimateRangeCost('users', 'id', 1, 100, 2);

      expect(cost).toBeGreaterThan(0);
    });

    it('should estimate scatter cost', () => {
      const cost = estimator.estimateScatterCost('users', 4);

      expect(cost).toBeGreaterThan(0);
    });

    it('should have lower cost for single-shard vs scatter', () => {
      const singleShardCost = estimator.estimateSingleShardEqualityCost('users', 'id', 123);
      const scatterCost = estimator.estimateScatterCost('users', 4);

      expect(singleShardCost).toBeLessThan(scatterCost);
    });
  });

  describe('with table statistics', () => {
    it('should use row count from statistics', () => {
      const stats: TableStatistics = {
        tableName: 'users',
        rowCount: 1000000,
        columnStats: new Map(),
        lastUpdated: Date.now(),
      };
      estimator.setTableStats(stats);

      const totalRows = estimator.estimateTotalRows('users');
      expect(totalRows).toBe(1000000);
    });

    it('should fall back to default for unknown tables', () => {
      const totalRows = estimator.estimateTotalRows('unknown_table');
      expect(totalRows).toBe(10000); // Default
    });

    it('should estimate matching rows with selectivity', () => {
      const stats: TableStatistics = {
        tableName: 'users',
        rowCount: 10000,
        columnStats: new Map(),
        lastUpdated: Date.now(),
      };
      estimator.setTableStats(stats);

      const matchingRows = estimator.estimateMatchingRows('users', 0.1);
      expect(matchingRows).toBe(1000);
    });
  });

  describe('with custom config', () => {
    it('should use custom default row count', () => {
      const customEstimator = new CostEstimator(undefined, { defaultRowCount: 50000 });
      const totalRows = customEstimator.estimateTotalRows('users');
      expect(totalRows).toBe(50000);
    });

    it('should use custom network cost', () => {
      const lowNetworkCost = new CostEstimator(undefined, { perShardNetworkCost: 0.1 });
      const highNetworkCost = new CostEstimator(undefined, { perShardNetworkCost: 10.0 });

      const lowCost = lowNetworkCost.estimateScatterCost('users', 4);
      const highCost = highNetworkCost.estimateScatterCost('users', 4);

      expect(lowCost).toBeLessThan(highCost);
    });
  });
});

// =============================================================================
// EXECUTION PLAN TESTS
// =============================================================================

describe('Execution Plan Generation', () => {
  let vschema: VSchema;
  let router: QueryRouter;

  beforeEach(() => {
    vschema = createBasicVSchema();
    router = createRouter(vschema);
  });

  describe('createExecutionPlan', () => {
    it('should create plan for single-shard query', () => {
      const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 123');

      expect(plan.sql).toBe('SELECT * FROM users WHERE tenant_id = 123');
      expect(plan.routing.queryType).toBe('single-shard');
      expect(plan.shardPlans).toHaveLength(1);
      expect(plan.shardPlans[0].isFinal).toBe(true);
    });

    it('should create plan for scatter query', () => {
      const plan = router.createExecutionPlan('SELECT * FROM users');

      expect(plan.routing.queryType).toBe('scatter');
      expect(plan.shardPlans).toHaveLength(3);
      expect(plan.postProcessing).toBeDefined();
    });

    it('should include merge post-processing for scatter', () => {
      const plan = router.createExecutionPlan('SELECT * FROM users');

      expect(plan.postProcessing?.some(op => op.type === 'merge')).toBe(true);
    });

    it('should include sort post-processing for ORDER BY scatter', () => {
      const plan = router.createExecutionPlan('SELECT * FROM users ORDER BY created_at DESC');

      expect(plan.postProcessing?.some(op => op.type === 'sort')).toBe(true);
    });

    it('should include limit post-processing for LIMIT scatter', () => {
      const plan = router.createExecutionPlan('SELECT * FROM users LIMIT 10');

      expect(plan.postProcessing?.some(op => op.type === 'limit')).toBe(true);
    });

    it('should include aggregate post-processing for aggregates', () => {
      const plan = router.createExecutionPlan('SELECT COUNT(*) FROM users');

      expect(plan.postProcessing?.some(op => op.type === 'aggregate')).toBe(true);
    });

    it('should include distinct post-processing for DISTINCT scatter', () => {
      const plan = router.createExecutionPlan('SELECT DISTINCT status FROM users');

      expect(plan.postProcessing?.some(op => op.type === 'distinct')).toBe(true);
    });

    it('should not include post-processing for single-shard', () => {
      const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 123');

      expect(plan.postProcessing).toBeUndefined();
    });

    it('should pass params through to shard plans', () => {
      const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = $1', [123]);

      expect(plan.shardPlans[0].params).toEqual([123]);
    });
  });

  describe('query rewriting for scatter', () => {
    it('should rewrite LIMIT for scatter queries with ORDER BY', () => {
      const plan = router.createExecutionPlan('SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 5');

      // Each shard should request enough rows (limit + offset)
      for (const shardPlan of plan.shardPlans) {
        expect(shardPlan.sql).toContain('LIMIT 15');
        expect(shardPlan.sql).not.toContain('OFFSET');
      }
    });

    it('should rewrite AVG to SUM and COUNT for aggregation', () => {
      const plan = router.createExecutionPlan('SELECT AVG(price) FROM orders');

      // Should rewrite AVG to SUM and COUNT for two-phase aggregation
      for (const shardPlan of plan.shardPlans) {
        expect(shardPlan.sql).toContain('SUM(price)');
        expect(shardPlan.sql).toContain('COUNT(price)');
      }
    });
  });
});

// =============================================================================
// ROUTER UTILITY METHODS TESTS
// =============================================================================

describe('Router Utility Methods', () => {
  let router: QueryRouter;

  beforeEach(() => {
    router = createRouter(createBasicVSchema());
  });

  describe('getTableConfig', () => {
    it('should return table config for existing table', () => {
      const config = router.getTableConfig('users');

      expect(config).toBeDefined();
      expect(config?.type).toBe('sharded');
    });

    it('should return undefined for unknown table', () => {
      const config = router.getTableConfig('unknown');

      expect(config).toBeUndefined();
    });
  });

  describe('getShards', () => {
    it('should return all shard configs', () => {
      const shards = router.getShards();

      expect(shards).toHaveLength(3);
    });

    it('should return readonly array', () => {
      const shards = router.getShards();

      // TypeScript should prevent mutation (runtime check)
      expect(Array.isArray(shards)).toBe(true);
    });
  });

  describe('getShard', () => {
    it('should return shard config by ID', () => {
      const shardConfig = router.getShard('shard-1');

      expect(shardConfig).toBeDefined();
      expect(shardConfig?.id).toBe('shard-1');
    });

    it('should return undefined for unknown shard', () => {
      const shardConfig = router.getShard('unknown-shard');

      expect(shardConfig).toBeUndefined();
    });
  });

  describe('getCostEstimator', () => {
    it('should return cost estimator instance', () => {
      const estimator = router.getCostEstimator();

      expect(estimator).toBeInstanceOf(CostEstimator);
    });
  });

  describe('setTableStats', () => {
    it('should update cost estimator with stats', () => {
      const stats: TableStatistics = {
        tableName: 'users',
        rowCount: 500000,
        columnStats: new Map(),
        lastUpdated: Date.now(),
      };
      router.setTableStats(stats);

      const estimator = router.getCostEstimator();
      expect(estimator.estimateTotalRows('users')).toBe(500000);
    });
  });
});

// =============================================================================
// CASE SENSITIVITY TESTS
// =============================================================================

describe('Case Sensitivity', () => {
  let router: QueryRouter;

  beforeEach(() => {
    router = createRouter(createBasicVSchema());
  });

  it('should treat shard key column as case-insensitive', () => {
    const routing1 = router.route('SELECT * FROM users WHERE tenant_id = 123');
    const routing2 = router.route('SELECT * FROM users WHERE TENANT_ID = 123');
    const routing3 = router.route('SELECT * FROM users WHERE Tenant_Id = 123');

    expect(routing1.queryType).toBe('single-shard');
    expect(routing2.queryType).toBe('single-shard');
    expect(routing3.queryType).toBe('single-shard');
  });

  it('should handle mixed case SQL keywords', () => {
    const routing = router.route('sElEcT * fRoM users WhErE tenant_id = 123');

    expect(routing.queryType).toBe('single-shard');
  });
});

// =============================================================================
// COMPLEX QUERY TESTS
// =============================================================================

describe('Complex Query Routing', () => {
  let router: QueryRouter;

  beforeEach(() => {
    router = createRouter(createBasicVSchema());
  });

  it('should extract shard key from complex WHERE with AND', () => {
    const routing = router.route(
      "SELECT * FROM users WHERE tenant_id = 123 AND status = 'active' AND created_at > '2024-01-01'"
    );

    expect(routing.queryType).toBe('single-shard');
    expect(routing.shardKeyValue).toBe(123);
  });

  it('should handle aliased table with shard key', () => {
    const routing = router.route('SELECT u.* FROM users u WHERE u.tenant_id = 123');

    expect(routing.queryType).toBe('single-shard');
  });

  it('should handle subqueries in non-shard-key conditions', () => {
    const routing = router.route(
      'SELECT * FROM users WHERE tenant_id = 123 AND status IN (SELECT status FROM config)'
    );

    // Should still route based on tenant_id
    expect(routing.queryType).toBe('single-shard');
    expect(routing.shardKeyValue).toBe(123);
  });

  it('should handle GROUP BY with shard key in WHERE', () => {
    const routing = router.route(
      'SELECT status, COUNT(*) FROM users WHERE tenant_id = 123 GROUP BY status'
    );

    expect(routing.queryType).toBe('single-shard');
  });

  it('should handle HAVING clause', () => {
    const routing = router.route(
      'SELECT status, COUNT(*) as cnt FROM users WHERE tenant_id = 123 GROUP BY status HAVING COUNT(*) > 5'
    );

    expect(routing.queryType).toBe('single-shard');
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Edge Cases', () => {
  it('should handle empty VSchema shards gracefully', () => {
    const emptySchema = createVSchema({ config: unshardedTable() }, []);

    expect(() => createRouter(emptySchema)).not.toThrow();
  });

  it('should handle very long shard key values', () => {
    const router = createRouter(createBasicVSchema());
    const longValue = 'a'.repeat(1000);

    const routing = router.route(`SELECT * FROM users WHERE tenant_id = '${longValue}'`);

    expect(routing.targetShards.length).toBeGreaterThanOrEqual(1);
  });

  it('should handle numeric shard key with negative value', () => {
    const router = createRouter(createBasicVSchema());

    const routing = router.route('SELECT * FROM users WHERE tenant_id = -123');

    expect(routing.queryType).toBe('single-shard');
    expect(routing.shardKeyValue).toBe(-123);
  });

  it('should handle floating point shard key', () => {
    const router = createRouter(createBasicVSchema());

    const routing = router.route('SELECT * FROM users WHERE tenant_id = 123.456');

    expect(routing.queryType).toBe('single-shard');
  });

  it('should handle NULL in WHERE clause', () => {
    const router = createRouter(createBasicVSchema());

    const routing = router.route('SELECT * FROM users WHERE tenant_id IS NULL');

    // NULL condition doesn't extract shard key
    expect(routing.queryType).toBe('scatter');
  });

  it('should handle NOT NULL in WHERE clause', () => {
    const router = createRouter(createBasicVSchema());

    const routing = router.route('SELECT * FROM users WHERE tenant_id IS NOT NULL');

    // IS NOT NULL doesn't extract shard key value
    expect(routing.queryType).toBe('scatter');
  });
});
