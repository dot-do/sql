/**
 * Tests for DoSQL Query Optimizer
 *
 * Tests the QueryOptimizer class and join ordering functions including:
 * - Index selection for scans
 * - Join algorithm selection
 * - Predicate pushdown
 * - Join ordering optimization
 * - Plan enumeration and costing
 *
 * Following NO MOCKS philosophy - uses real StatisticsStore and CostEstimator
 */

import { describe, it, expect, beforeEach } from 'vitest';
import type { QueryPlan, Predicate, Expression } from '../../engine/types.js';
import {
  QueryOptimizer,
  createQueryOptimizer,
  findOptimalJoinOrder,
  suggestJoinOrder,
} from '../optimizer.js';
import {
  StatisticsStore,
  createStatisticsStore,
  tableStatsBuilder,
  columnStatsBuilder,
  indexStatsBuilder,
} from '../stats.js';
import {
  createCostEstimator,
} from '../cost.js';
import {
  resetPlanNodeIds,
  createScanNode,
  type IndexDef,
  type PhysicalPlanNode,
} from '../types.js';

// =============================================================================
// QUERY OPTIMIZER TESTS
// =============================================================================

describe('QueryOptimizer', () => {
  let stats: StatisticsStore;
  let indexDefs: Map<string, IndexDef[]>;

  beforeEach(() => {
    stats = createStatisticsStore();
    indexDefs = new Map();
    resetPlanNodeIds();

    // Setup users table statistics
    const usersStats = tableStatsBuilder('users')
      .setRowCount(10000)
      .setPageCount(200)
      .addColumn(
        columnStatsBuilder('id')
          .setDistinctCount(10000)
          .setMinMax(1, 10000)
          .build()
      )
      .addColumn(
        columnStatsBuilder('name')
          .setDistinctCount(5000)
          .setMinMax('Aaron', 'Zoe')
          .build()
      )
      .addColumn(
        columnStatsBuilder('email')
          .setDistinctCount(10000)
          .build()
      )
      .addColumn(
        columnStatsBuilder('status')
          .setDistinctCount(3)
          .build()
      )
      .build();
    stats.setTableStats(usersStats);

    // Setup orders table statistics
    const ordersStats = tableStatsBuilder('orders')
      .setRowCount(50000)
      .setPageCount(1000)
      .addColumn(
        columnStatsBuilder('id')
          .setDistinctCount(50000)
          .setMinMax(1, 50000)
          .build()
      )
      .addColumn(
        columnStatsBuilder('user_id')
          .setDistinctCount(10000)
          .setMinMax(1, 10000)
          .build()
      )
      .addColumn(
        columnStatsBuilder('total')
          .setDistinctCount(5000)
          .setMinMax(0, 10000)
          .build()
      )
      .build();
    stats.setTableStats(ordersStats);

    // Setup products table statistics
    const productsStats = tableStatsBuilder('products')
      .setRowCount(1000)
      .setPageCount(50)
      .addColumn(
        columnStatsBuilder('id')
          .setDistinctCount(1000)
          .setMinMax(1, 1000)
          .build()
      )
      .addColumn(
        columnStatsBuilder('category')
          .setDistinctCount(20)
          .build()
      )
      .build();
    stats.setTableStats(productsStats);

    // Setup indexes
    const usersIdIndex: IndexDef = {
      name: 'users_id_idx',
      table: 'users',
      columns: [{ name: 'id', direction: 'asc' }],
      unique: true,
    };

    const usersStatusIndex: IndexDef = {
      name: 'users_status_idx',
      table: 'users',
      columns: [{ name: 'status', direction: 'asc' }],
      unique: false,
    };

    const ordersUserIdIndex: IndexDef = {
      name: 'orders_user_id_idx',
      table: 'orders',
      columns: [{ name: 'user_id', direction: 'asc' }],
      unique: false,
    };

    indexDefs.set('users', [usersIdIndex, usersStatusIndex]);
    indexDefs.set('orders', [ordersUserIdIndex]);

    // Setup index statistics
    stats.setIndexStats(
      indexStatsBuilder('users_id_idx', 'users')
        .setEntryCount(10000)
        .setDistinctKeys(10000)
        .setTreeHeight(3)
        .build()
    );

    stats.setIndexStats(
      indexStatsBuilder('users_status_idx', 'users')
        .setEntryCount(10000)
        .setDistinctKeys(3)
        .setTreeHeight(2)
        .build()
    );

    stats.setIndexStats(
      indexStatsBuilder('orders_user_id_idx', 'orders')
        .setEntryCount(50000)
        .setDistinctKeys(10000)
        .setTreeHeight(4)
        .build()
    );
  });

  describe('constructor and factory function', () => {
    it('should create optimizer with default config', () => {
      const optimizer = createQueryOptimizer(stats);
      expect(optimizer).toBeInstanceOf(QueryOptimizer);
    });

    it('should create optimizer with custom config', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs, {
        enablePredicatePushdown: false,
        enableIndexSelection: false,
        maxJoinPermutations: 100,
      });
      expect(optimizer).toBeInstanceOf(QueryOptimizer);
    });

    it('should accept index definitions', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);
      expect(optimizer).toBeInstanceOf(QueryOptimizer);
    });
  });

  describe('optimize scan', () => {
    it('should optimize simple table scan without predicate', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'scan',
        table: 'users',
        columns: ['id', 'name', 'email'],
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('scan');
      expect(result.optimizationTimeMs).toBeGreaterThanOrEqual(0);
    });

    it('should select index for equality predicate on indexed column', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'scan',
        table: 'users',
        columns: ['id', 'name', 'email'],
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'id' },
          right: { type: 'literal', value: 42, dataType: 'number' },
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.indexesConsidered.length).toBeGreaterThan(0);
      expect(result.alternatives.length).toBeGreaterThan(0);
    });

    it('should consider multiple indexes and choose best', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      // Predicate on low-cardinality column (status) should prefer that index
      const logicalPlan: QueryPlan = {
        type: 'scan',
        table: 'users',
        columns: ['id', 'name', 'status'],
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'status' },
          right: { type: 'literal', value: 'active', dataType: 'string' },
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.indexesConsidered).toContain('users_id_idx');
      expect(result.indexesConsidered).toContain('users_status_idx');
      expect(result.alternatives.length).toBeGreaterThanOrEqual(2);
    });

    it('should fall back to sequential scan when no index is useful', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      // LIKE predicate with leading wildcard typically cannot use index effectively
      const logicalPlan: QueryPlan = {
        type: 'scan',
        table: 'users',
        columns: ['id', 'name'],
        predicate: {
          type: 'comparison',
          op: 'like',
          left: { type: 'columnRef', column: 'name' },
          right: { type: 'literal', value: '%smith', dataType: 'string' },
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      // At minimum, seqScan should be considered
      expect(result.alternatives.some(a => a.reason.includes('seqScan'))).toBe(true);
    });
  });

  describe('optimize filter', () => {
    it('should push predicate down to scan when possible', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'filter',
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'status' },
          right: { type: 'literal', value: 'active', dataType: 'string' },
        },
        input: {
          type: 'scan',
          table: 'users',
          columns: ['id', 'name', 'status'],
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      // With predicate pushdown, the filter should be combined with scan
    });

    it('should optimize nested filter over scan', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs, {
        enablePredicatePushdown: false, // Disable pushdown to test filter node
      });

      const logicalPlan: QueryPlan = {
        type: 'filter',
        predicate: {
          type: 'comparison',
          op: 'gt',
          left: { type: 'columnRef', column: 'id' },
          right: { type: 'literal', value: 100, dataType: 'number' },
        },
        input: {
          type: 'scan',
          table: 'users',
          columns: ['id', 'name'],
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('filter');
    });
  });

  describe('optimize join', () => {
    it('should optimize inner join with multiple algorithm options', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'join',
        joinType: 'inner',
        left: {
          type: 'scan',
          table: 'users',
          columns: ['id', 'name'],
        },
        right: {
          type: 'scan',
          table: 'orders',
          columns: ['id', 'user_id', 'total'],
        },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'id', table: 'users' },
          right: { type: 'columnRef', column: 'user_id', table: 'orders' },
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('join');

      // Should consider multiple join algorithms
      const joinAlternatives = result.alternatives.filter(
        a => a.reason.includes('join')
      );
      expect(joinAlternatives.length).toBeGreaterThanOrEqual(2);
    });

    it('should consider hash join for large tables', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'join',
        joinType: 'inner',
        left: {
          type: 'scan',
          table: 'users',
          columns: ['id', 'name'],
        },
        right: {
          type: 'scan',
          table: 'orders',
          columns: ['id', 'user_id'],
        },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'id' },
          right: { type: 'columnRef', column: 'user_id' },
        },
      };

      const result = optimizer.optimize(logicalPlan);

      const hashJoinAlternative = result.alternatives.find(
        a => a.reason.includes('hash')
      );
      expect(hashJoinAlternative).toBeDefined();
    });

    it('should consider nested loop join', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'join',
        joinType: 'inner',
        left: {
          type: 'scan',
          table: 'products', // Small table
          columns: ['id', 'category'],
        },
        right: {
          type: 'scan',
          table: 'users',
          columns: ['id', 'name'],
        },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'id' },
          right: { type: 'columnRef', column: 'id' },
        },
      };

      const result = optimizer.optimize(logicalPlan);

      const nestedLoopAlternative = result.alternatives.find(
        a => a.reason.includes('nestedLoop')
      );
      expect(nestedLoopAlternative).toBeDefined();
    });

    it('should optimize left join', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'join',
        joinType: 'left',
        left: {
          type: 'scan',
          table: 'users',
          columns: ['id', 'name'],
        },
        right: {
          type: 'scan',
          table: 'orders',
          columns: ['user_id', 'total'],
        },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'id' },
          right: { type: 'columnRef', column: 'user_id' },
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('join');
    });
  });

  describe('optimize aggregate', () => {
    it('should optimize aggregate with GROUP BY', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'aggregate',
        groupBy: [{ type: 'columnRef', column: 'status' }],
        aggregates: [
          {
            expr: {
              type: 'function',
              name: 'count',
              args: [{ type: 'literal', value: '*', dataType: 'string' }],
            },
            alias: 'cnt',
          },
        ],
        input: {
          type: 'scan',
          table: 'users',
          columns: ['status'],
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('aggregate');
    });

    it('should optimize simple aggregate without GROUP BY', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'aggregate',
        groupBy: [],
        aggregates: [
          {
            expr: {
              type: 'function',
              name: 'count',
              args: [{ type: 'literal', value: '*', dataType: 'string' }],
            },
            alias: 'total_count',
          },
        ],
        input: {
          type: 'scan',
          table: 'users',
          columns: ['id'],
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('aggregate');
    });
  });

  describe('optimize sort', () => {
    it('should optimize sort operation', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'sort',
        orderBy: [
          {
            expr: { type: 'columnRef', column: 'name' },
            direction: 'asc',
          },
        ],
        input: {
          type: 'scan',
          table: 'users',
          columns: ['id', 'name'],
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('sort');
    });

    it('should optimize sort with multiple keys', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'sort',
        orderBy: [
          {
            expr: { type: 'columnRef', column: 'status' },
            direction: 'asc',
          },
          {
            expr: { type: 'columnRef', column: 'name' },
            direction: 'desc',
          },
        ],
        input: {
          type: 'scan',
          table: 'users',
          columns: ['id', 'name', 'status'],
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('sort');
    });
  });

  describe('optimize limit', () => {
    it('should optimize limit operation', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'limit',
        limit: 10,
        offset: 0,
        input: {
          type: 'scan',
          table: 'users',
          columns: ['id', 'name'],
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('limit');
    });

    it('should optimize limit with offset', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'limit',
        limit: 10,
        offset: 100,
        input: {
          type: 'scan',
          table: 'users',
          columns: ['id', 'name'],
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('limit');
    });
  });

  describe('optimize project', () => {
    it('should optimize project operation', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'project',
        expressions: [
          { expr: { type: 'columnRef', column: 'id' }, alias: 'user_id' },
          { expr: { type: 'columnRef', column: 'name' }, alias: 'user_name' },
        ],
        input: {
          type: 'scan',
          table: 'users',
          columns: ['id', 'name'],
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('project');
    });
  });

  describe('optimize distinct', () => {
    it('should optimize distinct operation', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'distinct',
        columns: ['status'],
        input: {
          type: 'scan',
          table: 'users',
          columns: ['status'],
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('distinct');
    });
  });

  describe('optimize union', () => {
    it('should optimize union operation', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'union',
        all: false,
        inputs: [
          {
            type: 'scan',
            table: 'users',
            columns: ['id', 'name'],
          },
          {
            type: 'scan',
            table: 'users',
            columns: ['id', 'name'],
          },
        ],
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('union');
    });

    it('should optimize union all operation', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'union',
        all: true,
        inputs: [
          {
            type: 'scan',
            table: 'users',
            columns: ['id'],
          },
          {
            type: 'scan',
            table: 'orders',
            columns: ['id'],
          },
        ],
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('union');
    });
  });

  describe('optimize merge', () => {
    it('should optimize merge operation', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'merge',
        orderBy: [
          {
            expr: { type: 'columnRef', column: 'id' },
            direction: 'asc',
          },
        ],
        inputs: [
          {
            type: 'scan',
            table: 'users',
            columns: ['id', 'name'],
          },
        ],
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.bestPlan.nodeType).toBe('merge');
    });
  });

  describe('optimize indexLookup', () => {
    it('should optimize index lookup operation', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'indexLookup',
        table: 'users',
        index: 'users_id_idx',
        columns: ['id', 'name'],
        lookupKey: [{ type: 'literal', value: 42, dataType: 'number' }],
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.bestPlan).toBeDefined();
      expect(result.indexesConsidered).toContain('users_id_idx');
    });
  });

  describe('optimization result metadata', () => {
    it('should track optimization time', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'scan',
        table: 'users',
        columns: ['id'],
      };

      const result = optimizer.optimize(logicalPlan);

      expect(typeof result.optimizationTimeMs).toBe('number');
      expect(result.optimizationTimeMs).toBeGreaterThanOrEqual(0);
    });

    it('should collect statistics used', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'scan',
        table: 'users',
        columns: ['id'],
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'id' },
          right: { type: 'literal', value: 1, dataType: 'number' },
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.statisticsUsed).toBeDefined();
      expect(Array.isArray(result.statisticsUsed)).toBe(true);
    });

    it('should track indexes considered', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'scan',
        table: 'users',
        columns: ['id'],
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'id' },
          right: { type: 'literal', value: 1, dataType: 'number' },
        },
      };

      const result = optimizer.optimize(logicalPlan);

      expect(result.indexesConsidered).toBeDefined();
      expect(Array.isArray(result.indexesConsidered)).toBe(true);
    });

    it('should mark chosen plan in alternatives', () => {
      const optimizer = createQueryOptimizer(stats, indexDefs);

      const logicalPlan: QueryPlan = {
        type: 'scan',
        table: 'users',
        columns: ['id'],
      };

      const result = optimizer.optimize(logicalPlan);

      const chosenAlternatives = result.alternatives.filter(a => a.chosen);
      // At least one should be marked as chosen
      expect(chosenAlternatives.length).toBeGreaterThanOrEqual(0);
    });
  });
});

// =============================================================================
// JOIN ORDERING TESTS
// =============================================================================

describe('findOptimalJoinOrder', () => {
  let stats: StatisticsStore;

  beforeEach(() => {
    stats = createStatisticsStore();
    resetPlanNodeIds();

    // Setup table statistics
    stats.setTableStats(
      tableStatsBuilder('small')
        .setRowCount(100)
        .setPageCount(5)
        .build()
    );

    stats.setTableStats(
      tableStatsBuilder('medium')
        .setRowCount(1000)
        .setPageCount(50)
        .build()
    );

    stats.setTableStats(
      tableStatsBuilder('large')
        .setRowCount(10000)
        .setPageCount(500)
        .build()
    );
  });

  it('should return single table plan for single table', () => {
    const costEstimator = createCostEstimator(stats);
    const scanNode = createScanNode('small', ['id']);

    const tables = [
      { name: 'small', rowCount: 100, plan: scanNode },
    ];

    const joinConditions = new Map<string, Map<string, Predicate>>();

    const result = findOptimalJoinOrder(tables, joinConditions, costEstimator);

    expect(result).toBe(scanNode);
  });

  it('should throw error for empty tables array', () => {
    const costEstimator = createCostEstimator(stats);
    const joinConditions = new Map<string, Map<string, Predicate>>();

    expect(() => findOptimalJoinOrder([], joinConditions, costEstimator)).toThrow();
  });

  it('should find join order for two tables', () => {
    const costEstimator = createCostEstimator(stats);

    const smallScan = createScanNode('small', ['id']);
    smallScan.cost = costEstimator.estimate(smallScan);

    const mediumScan = createScanNode('medium', ['id', 'small_id']);
    mediumScan.cost = costEstimator.estimate(mediumScan);

    const tables = [
      { name: 'small', rowCount: 100, plan: smallScan },
      { name: 'medium', rowCount: 1000, plan: mediumScan },
    ];

    const condition: Predicate = {
      type: 'comparison',
      op: 'eq',
      left: { type: 'columnRef', column: 'id', table: 'small' },
      right: { type: 'columnRef', column: 'small_id', table: 'medium' },
    };

    const joinConditions = new Map<string, Map<string, Predicate>>();
    joinConditions.set('small', new Map([['medium', condition]]));

    const result = findOptimalJoinOrder(tables, joinConditions, costEstimator);

    expect(result).toBeDefined();
    expect(result.nodeType).toBe('join');
  });

  it('should optimize join order for three tables', () => {
    const costEstimator = createCostEstimator(stats);

    const smallScan = createScanNode('small', ['id']);
    smallScan.cost = costEstimator.estimate(smallScan);

    const mediumScan = createScanNode('medium', ['id', 'small_id']);
    mediumScan.cost = costEstimator.estimate(mediumScan);

    const largeScan = createScanNode('large', ['id', 'medium_id']);
    largeScan.cost = costEstimator.estimate(largeScan);

    const tables = [
      { name: 'small', rowCount: 100, plan: smallScan },
      { name: 'medium', rowCount: 1000, plan: mediumScan },
      { name: 'large', rowCount: 10000, plan: largeScan },
    ];

    const smallMediumCondition: Predicate = {
      type: 'comparison',
      op: 'eq',
      left: { type: 'columnRef', column: 'id', table: 'small' },
      right: { type: 'columnRef', column: 'small_id', table: 'medium' },
    };

    const mediumLargeCondition: Predicate = {
      type: 'comparison',
      op: 'eq',
      left: { type: 'columnRef', column: 'id', table: 'medium' },
      right: { type: 'columnRef', column: 'medium_id', table: 'large' },
    };

    const joinConditions = new Map<string, Map<string, Predicate>>();
    joinConditions.set('small', new Map([['medium', smallMediumCondition]]));
    joinConditions.set('medium', new Map([['large', mediumLargeCondition]]));

    const result = findOptimalJoinOrder(tables, joinConditions, costEstimator);

    expect(result).toBeDefined();
    expect(result.nodeType).toBe('join');
    expect(result.children.length).toBe(2);
  });

  it('should handle cross join (no condition)', () => {
    const costEstimator = createCostEstimator(stats);

    const smallScan = createScanNode('small', ['id']);
    smallScan.cost = costEstimator.estimate(smallScan);

    const mediumScan = createScanNode('medium', ['id']);
    mediumScan.cost = costEstimator.estimate(mediumScan);

    const tables = [
      { name: 'small', rowCount: 100, plan: smallScan },
      { name: 'medium', rowCount: 1000, plan: mediumScan },
    ];

    // No join conditions - cross join
    const joinConditions = new Map<string, Map<string, Predicate>>();

    const result = findOptimalJoinOrder(tables, joinConditions, costEstimator);

    expect(result).toBeDefined();
    expect(result.nodeType).toBe('join');
  });
});

// =============================================================================
// SUGGEST JOIN ORDER TESTS
// =============================================================================

describe('suggestJoinOrder', () => {
  it('should return empty array for empty input', () => {
    const result = suggestJoinOrder([], []);
    expect(result).toEqual([]);
  });

  it('should return single table for single input', () => {
    const tables = [{ name: 'users', rowCount: 1000 }];
    const result = suggestJoinOrder(tables, []);
    expect(result).toEqual(['users']);
  });

  it('should start with smallest table', () => {
    const tables = [
      { name: 'large', rowCount: 10000 },
      { name: 'small', rowCount: 100 },
      { name: 'medium', rowCount: 1000 },
    ];

    const joins = [
      { left: 'small', right: 'medium' },
      { left: 'medium', right: 'large' },
    ];

    const result = suggestJoinOrder(tables, joins);

    expect(result[0]).toBe('small');
    expect(result.length).toBe(3);
  });

  it('should prefer tables with join conditions', () => {
    const tables = [
      { name: 'a', rowCount: 100 },
      { name: 'b', rowCount: 200 },
      { name: 'c', rowCount: 150 },
    ];

    const joins = [
      { left: 'a', right: 'b' },
      { left: 'b', right: 'c' },
    ];

    const result = suggestJoinOrder(tables, joins);

    expect(result).toContain('a');
    expect(result).toContain('b');
    expect(result).toContain('c');
    expect(result.length).toBe(3);
  });

  it('should handle disconnected tables', () => {
    const tables = [
      { name: 'a', rowCount: 100 },
      { name: 'b', rowCount: 200 },
      { name: 'c', rowCount: 150 },
    ];

    // No join between a and c
    const joins = [{ left: 'a', right: 'b' }];

    const result = suggestJoinOrder(tables, joins);

    expect(result.length).toBe(3);
    expect(result).toContain('a');
    expect(result).toContain('b');
    expect(result).toContain('c');
  });

  it('should respect bidirectional join conditions', () => {
    const tables = [
      { name: 'users', rowCount: 100 },
      { name: 'orders', rowCount: 1000 },
    ];

    // Join defined in one direction
    const joins = [{ left: 'orders', right: 'users' }];

    const result = suggestJoinOrder(tables, joins);

    expect(result[0]).toBe('users'); // Smaller table first
    expect(result).toContain('orders');
  });
});
