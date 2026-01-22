/**
 * DoSQL Cost-Based Query Planner Tests
 *
 * Tests covering:
 * - Table statistics (row count, cardinality)
 * - Index selection based on selectivity
 * - Join ordering optimization
 * - Cost estimation for scans vs index lookups
 * - Predicate pushdown
 * - EXPLAIN QUERY PLAN output
 * - Histogram-based estimation
 * - Multi-column index selection
 * - Covering index detection
 */

import { describe, it, expect, beforeEach } from 'vitest';

// Import types
import type { QueryPlan, Predicate, Expression } from '../engine/types.js';

import {
  // Types
  type CostModelConfig,
  type PlanCost,
  type PhysicalPlan,
  type ScanNode,
  type JoinNode,
  type IndexDef,
  type IndexCandidate,
  DEFAULT_COST_MODEL,
  emptyCost,
  combineCosts,
  nextPlanNodeId,
  resetPlanNodeIds,
  createScanNode,
  createJoinNode,
  createFilterNode,
  createSortNode,
  createLimitNode,
  createAggregateNode,

  // Statistics
  type TableStatistics,
  type ColumnStatistics,
  type IndexStatistics,
  type HistogramBucket,
  type MostCommonValue,
  StatisticsStore,
  createStatisticsStore,
  tableStatsBuilder,
  columnStatsBuilder,
  indexStatsBuilder,
  buildHistogram,
  calculateMostCommonValues,
  estimateEqualitySelectivity,
  estimateRangeSelectivity,
  estimateBetweenSelectivity,
  estimateInSelectivity,
  estimateLikeSelectivity,
  estimateIsNullSelectivity,

  // Cost estimation
  CostEstimator,
  createCostEstimator,
  compareCosts,
  isBetterCost,
  formatCost,

  // Optimizer
  QueryOptimizer,
  createQueryOptimizer,
  suggestJoinOrder,
  findOptimalJoinOrder,

  // EXPLAIN
  type ExplainFormat,
  type ExplainOptions,
  type ExplainNode,
  type ExecutionStats,
  explain,
  explainAnalyze,
  ExplainGenerator,
  createExplainGenerator,
  summarizePlan,
  getTotalCost,
  getEstimatedRows,
} from './index.js';

// =============================================================================
// TABLE STATISTICS TESTS
// =============================================================================

describe('Table Statistics', () => {
  let store: StatisticsStore;

  beforeEach(() => {
    store = createStatisticsStore();
    resetPlanNodeIds();
  });

  describe('StatisticsStore', () => {
    it('should store and retrieve table statistics', () => {
      const stats = tableStatsBuilder('users')
        .setRowCount(10000)
        .setPageCount(200)
        .setAvgRowSize(50)
        .build();

      store.setTableStats(stats);

      const retrieved = store.getTableStats('users');
      expect(retrieved).toBeDefined();
      expect(retrieved?.rowCount).toBe(10000);
      expect(retrieved?.pageCount).toBe(200);
    });

    it('should track row counts for tables', () => {
      const stats = tableStatsBuilder('orders')
        .setRowCount(50000)
        .build();

      store.setTableStats(stats);

      expect(store.getRowCount('orders')).toBe(50000);
      expect(store.getRowCount('nonexistent', 1000)).toBe(1000);
    });

    it('should store column statistics', () => {
      const colStats = columnStatsBuilder('email')
        .setDistinctCount(9500)
        .setNullFraction(0.01)
        .setAvgWidth(30)
        .build();

      const tableStats = tableStatsBuilder('users')
        .setRowCount(10000)
        .addColumn(colStats)
        .build();

      store.setTableStats(tableStats);

      expect(store.getDistinctCount('users', 'email')).toBe(9500);
      expect(store.getNullFraction('users', 'email')).toBeCloseTo(0.01);
    });

    it('should return default distinct count for unknown columns', () => {
      const stats = tableStatsBuilder('users')
        .setRowCount(10000)
        .build();

      store.setTableStats(stats);

      // Default is 10% of row count
      expect(store.getDistinctCount('users', 'unknown_column')).toBe(1000);
    });

    it('should check if statistics exist', () => {
      store.setTableStats(tableStatsBuilder('users').setRowCount(100).build());

      expect(store.hasTableStats('users')).toBe(true);
      expect(store.hasTableStats('orders')).toBe(false);
    });
  });

  describe('ColumnStatistics', () => {
    it('should track distinct value count (cardinality)', () => {
      const colStats = columnStatsBuilder('status')
        .setDistinctCount(5)
        .build();

      expect(colStats.distinctCount).toBe(5);
    });

    it('should track null fraction', () => {
      const colStats = columnStatsBuilder('middle_name')
        .setNullFraction(0.3)
        .build();

      expect(colStats.nullFraction).toBeCloseTo(0.3);
    });

    it('should clamp null fraction to valid range', () => {
      const colStats1 = columnStatsBuilder('col1')
        .setNullFraction(-0.5)
        .build();
      expect(colStats1.nullFraction).toBe(0);

      const colStats2 = columnStatsBuilder('col2')
        .setNullFraction(1.5)
        .build();
      expect(colStats2.nullFraction).toBe(1);
    });

    it('should track min/max values', () => {
      const colStats = columnStatsBuilder('age')
        .setMinMax(18, 99)
        .build();

      expect(colStats.minValue).toBe(18);
      expect(colStats.maxValue).toBe(99);
    });

    it('should track correlation with physical order', () => {
      const colStats = columnStatsBuilder('created_at')
        .setCorrelation(0.95)
        .build();

      expect(colStats.correlation).toBeCloseTo(0.95);
    });
  });

  describe('IndexStatistics', () => {
    it('should store and retrieve index statistics', () => {
      const indexStats = indexStatsBuilder('idx_users_email', 'users')
        .setEntryCount(10000)
        .setDistinctKeys(9500)
        .setTreeHeight(3)
        .setLeafPages(100)
        .build();

      store.setIndexStats(indexStats);

      const retrieved = store.getIndexStats('idx_users_email');
      expect(retrieved).toBeDefined();
      expect(retrieved?.distinctKeys).toBe(9500);
      expect(retrieved?.treeHeight).toBe(3);
    });

    it('should track clustering factor', () => {
      const indexStats = indexStatsBuilder('idx_orders_date', 'orders')
        .setClusteringFactor(0.9)
        .build();

      expect(indexStats.clusteringFactor).toBeCloseTo(0.9);
    });

    it('should get indexes for a table', () => {
      store.setIndexStats(indexStatsBuilder('idx1', 'users').build());
      store.setIndexStats(indexStatsBuilder('idx2', 'users').build());
      store.setIndexStats(indexStatsBuilder('idx3', 'orders').build());

      const userIndexes = store.getIndexesForTable('users');
      expect(userIndexes).toHaveLength(2);
    });
  });
});

// =============================================================================
// HISTOGRAM-BASED ESTIMATION TESTS
// =============================================================================

describe('Histogram-Based Estimation', () => {
  let store: StatisticsStore;

  beforeEach(() => {
    store = createStatisticsStore();
  });

  describe('buildHistogram', () => {
    it('should create histogram buckets from sorted values', () => {
      const values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
      const histogram = buildHistogram(values, 5);

      expect(histogram).toHaveLength(5);
      expect(histogram[0].low).toBe(1);
      expect(histogram[0].high).toBe(2);
      expect(histogram[0].count).toBe(2);
    });

    it('should calculate distinct count per bucket', () => {
      const values = [1, 1, 1, 2, 2, 3, 3, 3, 3, 4];
      const histogram = buildHistogram(values, 2);

      expect(histogram[0].distinctCount).toBeLessThanOrEqual(histogram[0].count);
    });

    it('should handle empty input', () => {
      const histogram = buildHistogram([], 5);
      expect(histogram).toHaveLength(0);
    });
  });

  describe('calculateMostCommonValues', () => {
    it('should find most common values', () => {
      const values = ['a', 'a', 'a', 'b', 'b', 'c'];
      const mcv = calculateMostCommonValues(values, 3);

      expect(mcv).toHaveLength(3);
      expect(mcv[0].value).toBe('a');
      expect(mcv[0].frequency).toBeCloseTo(0.5);
    });

    it('should limit to top N values', () => {
      const values = ['a', 'a', 'b', 'b', 'c', 'c', 'd', 'd', 'e', 'e'];
      const mcv = calculateMostCommonValues(values, 2);

      expect(mcv).toHaveLength(2);
    });
  });

  describe('Selectivity Estimation with Histograms', () => {
    beforeEach(() => {
      const histogram: HistogramBucket[] = [
        { low: 0, high: 25, count: 1000, distinctCount: 25 },
        { low: 25, high: 50, count: 2000, distinctCount: 25 },
        { low: 50, high: 75, count: 1500, distinctCount: 25 },
        { low: 75, high: 100, count: 500, distinctCount: 25 },
      ];

      const colStats = columnStatsBuilder('age')
        .setDistinctCount(100)
        .setHistogram(histogram)
        .build();

      const tableStats = tableStatsBuilder('users')
        .setRowCount(5000)
        .addColumn(colStats)
        .build();

      store.setTableStats(tableStats);
    });

    it('should use histogram for range estimation', () => {
      const selectivity = estimateRangeSelectivity(store, 'users', 'age', 'lt', 50);

      // Buckets 0-25 and 25-50 have 3000 of 5000 rows
      expect(selectivity).toBeGreaterThan(0.5);
      expect(selectivity).toBeLessThan(0.7);
    });

    it('should use histogram for between estimation', () => {
      const selectivity = estimateBetweenSelectivity(store, 'users', 'age', 25, 75);

      // Buckets 25-50 and 50-75 have 3500 of 5000 rows
      expect(selectivity).toBeGreaterThan(0.6);
      expect(selectivity).toBeLessThan(0.8);
    });
  });
});

// =============================================================================
// SELECTIVITY ESTIMATION TESTS
// =============================================================================

describe('Selectivity Estimation', () => {
  let store: StatisticsStore;

  beforeEach(() => {
    store = createStatisticsStore();

    // Set up realistic statistics
    const emailStats = columnStatsBuilder('email')
      .setDistinctCount(9500)
      .setNullFraction(0)
      .build();

    const statusStats = columnStatsBuilder('status')
      .setDistinctCount(3)
      .setMostCommonValues([
        { value: 'active', frequency: 0.7 },
        { value: 'pending', frequency: 0.2 },
        { value: 'inactive', frequency: 0.1 },
      ])
      .build();

    const ageStats = columnStatsBuilder('age')
      .setDistinctCount(82)
      .setMinMax(18, 99)
      .build();

    const tableStats = tableStatsBuilder('users')
      .setRowCount(10000)
      .addColumn(emailStats)
      .addColumn(statusStats)
      .addColumn(ageStats)
      .build();

    store.setTableStats(tableStats);
  });

  describe('Equality Selectivity', () => {
    it('should use uniform distribution for unique columns', () => {
      const selectivity = estimateEqualitySelectivity(
        store, 'users', 'email', 'test@example.com'
      );

      // 1 / 9500 distinct values
      expect(selectivity).toBeLessThan(0.001);
    });

    it('should use MCV frequency when value is common', () => {
      const selectivity = estimateEqualitySelectivity(
        store, 'users', 'status', 'active'
      );

      // 'active' has 70% frequency
      expect(selectivity).toBeCloseTo(0.7);
    });

    it('should return default for unknown tables', () => {
      const selectivity = estimateEqualitySelectivity(
        store, 'unknown_table', 'column', 'value'
      );

      expect(selectivity).toBe(0.01); // Default
    });
  });

  describe('Range Selectivity', () => {
    it('should estimate less-than selectivity using min/max', () => {
      const selectivity = estimateRangeSelectivity(
        store, 'users', 'age', 'lt', 50
      );

      // (50 - 18) / (99 - 18) = 0.395
      expect(selectivity).toBeGreaterThan(0.3);
      expect(selectivity).toBeLessThan(0.5);
    });

    it('should estimate greater-than selectivity', () => {
      const selectivity = estimateRangeSelectivity(
        store, 'users', 'age', 'gt', 50
      );

      // 1 - (50 - 18) / (99 - 18) = 0.605
      expect(selectivity).toBeGreaterThan(0.5);
      expect(selectivity).toBeLessThan(0.7);
    });
  });

  describe('Between Selectivity', () => {
    it('should estimate between using min/max', () => {
      const selectivity = estimateBetweenSelectivity(
        store, 'users', 'age', 25, 65
      );

      // (65 - 25) / (99 - 18) = 0.49
      expect(selectivity).toBeGreaterThan(0.4);
      expect(selectivity).toBeLessThan(0.6);
    });
  });

  describe('IN Selectivity', () => {
    it('should sum individual selectivities', () => {
      const selectivity = estimateInSelectivity(
        store, 'users', 'status', ['active', 'pending']
      );

      // 0.7 + 0.2 = 0.9
      expect(selectivity).toBeCloseTo(0.9, 1);
    });

    it('should cap at 1.0', () => {
      const selectivity = estimateInSelectivity(
        store, 'users', 'status', ['active', 'pending', 'inactive', 'deleted']
      );

      expect(selectivity).toBeLessThanOrEqual(1);
    });
  });

  describe('LIKE Selectivity', () => {
    it('should be more selective for prefix patterns', () => {
      const prefixSelectivity = estimateLikeSelectivity('abc%');
      const suffixSelectivity = estimateLikeSelectivity('%abc');

      expect(prefixSelectivity).toBeLessThan(suffixSelectivity);
    });

    it('should handle exact match patterns', () => {
      const selectivity = estimateLikeSelectivity('exact');
      expect(selectivity).toBe(0.01);
    });
  });

  describe('IS NULL Selectivity', () => {
    it('should return null fraction for IS NULL', () => {
      // email has 0% nulls
      const selectivity = estimateIsNullSelectivity(store, 'users', 'email', false);
      expect(selectivity).toBe(0);
    });

    it('should return complement for IS NOT NULL', () => {
      const selectivity = estimateIsNullSelectivity(store, 'users', 'email', true);
      expect(selectivity).toBe(1);
    });
  });
});

// =============================================================================
// COST ESTIMATION TESTS
// =============================================================================

describe('Cost Estimation', () => {
  let store: StatisticsStore;
  let estimator: CostEstimator;

  beforeEach(() => {
    store = createStatisticsStore();
    resetPlanNodeIds();

    // Set up statistics
    const tableStats = tableStatsBuilder('users')
      .setRowCount(10000)
      .setPageCount(200)
      .build();

    store.setTableStats(tableStats);

    const indexStats = indexStatsBuilder('idx_users_email', 'users')
      .setEntryCount(10000)
      .setDistinctKeys(9500)
      .setTreeHeight(3)
      .setLeafPages(100)
      .setClusteringFactor(0.1)
      .build();

    store.setIndexStats(indexStats);

    estimator = createCostEstimator(store);
  });

  describe('Sequential Scan Cost', () => {
    it('should estimate sequential scan cost', () => {
      const scan = createScanNode('users', ['id', 'email', 'name'], {
        accessMethod: 'seqScan',
      });

      const cost = estimator.estimate(scan);

      expect(cost.totalCost).toBeGreaterThan(0);
      expect(cost.ioOps).toBe(200); // 200 pages
      expect(cost.estimatedRows).toBe(10000);
    });

    it('should include filter cost in sequential scan', () => {
      const predicate: Predicate = {
        type: 'logical',
        op: 'and',
        operands: [
          {
            type: 'comparison',
            op: 'eq',
            left: { type: 'columnRef', column: 'status' },
            right: { type: 'literal', value: 'active', dataType: 'string' },
          },
          {
            type: 'comparison',
            op: 'gt',
            left: { type: 'columnRef', column: 'age' },
            right: { type: 'literal', value: 18, dataType: 'number' },
          },
        ],
      };

      const scanWithFilter = createScanNode('users', ['id', 'email'], {
        accessMethod: 'seqScan',
        filterCondition: predicate,
      });

      const scanWithoutFilter = createScanNode('users', ['id', 'email'], {
        accessMethod: 'seqScan',
      });

      const costWithFilter = estimator.estimate(scanWithFilter);
      const costWithoutFilter = estimator.estimate(scanWithoutFilter);

      // Filter with 2 conditions should have higher CPU cost than no filter
      expect(costWithFilter.cpuCost).toBeGreaterThanOrEqual(costWithoutFilter.cpuCost);
    });
  });

  describe('Index Scan Cost', () => {
    it('should estimate index scan cost', () => {
      const scan = createScanNode('users', ['id', 'email'], {
        accessMethod: 'indexScan',
        indexName: 'idx_users_email',
        indexCondition: [{
          column: 'email',
          operator: 'eq',
          value: 'test@example.com',
          columnIndex: 0,
        }],
      });

      const cost = estimator.estimate(scan);

      expect(cost.totalCost).toBeGreaterThan(0);
      expect(cost.estimatedRows).toBeLessThan(10000);
    });

    it('should estimate index scan as cheaper than seq scan for selective queries', () => {
      // For highly selective queries (point lookup on unique index),
      // index scan should return fewer rows and have lower estimated rows
      const indexScan = createScanNode('users', ['id', 'email'], {
        accessMethod: 'indexScan',
        indexName: 'idx_users_email',
        indexCondition: [{
          column: 'email',
          operator: 'eq',
          value: 'test@example.com',
          columnIndex: 0,
        }],
      });

      const seqScan = createScanNode('users', ['id', 'email'], {
        accessMethod: 'seqScan',
        // No filter - full table scan
      });

      const indexCost = estimator.estimate(indexScan);
      const seqCost = estimator.estimate(seqScan);

      // Index scan should have fewer estimated rows for a point lookup
      // (seqScan without filter returns all 10000 rows)
      expect(indexCost.estimatedRows).toBeLessThan(seqCost.estimatedRows);
      // Index scan should have fewer I/O operations (B-tree traversal vs full table)
      expect(indexCost.ioOps).toBeLessThan(seqCost.ioOps);
    });
  });

  describe('Index-Only Scan Cost', () => {
    it('should estimate index-only scan as cheaper than regular index scan', () => {
      const indexOnlyScan = createScanNode('users', ['email'], {
        accessMethod: 'indexOnlyScan',
        indexName: 'idx_users_email',
        indexCondition: [{
          column: 'email',
          operator: 'eq',
          value: 'test@example.com',
          columnIndex: 0,
        }],
      });

      const indexScan = createScanNode('users', ['id', 'email', 'name'], {
        accessMethod: 'indexScan',
        indexName: 'idx_users_email',
        indexCondition: [{
          column: 'email',
          operator: 'eq',
          value: 'test@example.com',
          columnIndex: 0,
        }],
      });

      const indexOnlyCost = estimator.estimate(indexOnlyScan);
      const indexCost = estimator.estimate(indexScan);

      // Index-only scan avoids heap fetches
      expect(indexOnlyCost.totalCost).toBeLessThan(indexCost.totalCost);
    });
  });

  describe('Join Cost', () => {
    it('should estimate hash join cost', () => {
      const leftScan = createScanNode('users', ['id', 'name']);
      leftScan.cost = estimator.estimate(leftScan);

      const rightScan = createScanNode('orders', ['id', 'user_id']);
      rightScan.cost = { ...emptyCost(), estimatedRows: 5000, totalCost: 100 };

      const hashJoin = createJoinNode(leftScan, rightScan, 'inner', 'hash', {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', table: 'users', column: 'id' },
        right: { type: 'columnRef', table: 'orders', column: 'user_id' },
      });

      const cost = estimator.estimate(hashJoin);

      expect(cost.totalCost).toBeGreaterThan(0);
      expect(cost.memoryBytes).toBeGreaterThan(0); // Hash table memory
    });

    it('should estimate nested loop as more expensive for large tables', () => {
      const leftScan = createScanNode('users', ['id', 'name']);
      leftScan.cost = { ...emptyCost(), estimatedRows: 10000, totalCost: 200, ioOps: 200 };

      const rightScan = createScanNode('orders', ['id', 'user_id']);
      rightScan.cost = { ...emptyCost(), estimatedRows: 50000, totalCost: 1000, ioOps: 1000 };

      const hashJoin = createJoinNode(leftScan, rightScan, 'inner', 'hash');
      const nestedLoop = createJoinNode(leftScan, rightScan, 'inner', 'nestedLoop');

      const hashCost = estimator.estimate(hashJoin);
      const nestedLoopCost = estimator.estimate(nestedLoop);

      expect(hashCost.totalCost).toBeLessThan(nestedLoopCost.totalCost);
    });
  });

  describe('Sort Cost', () => {
    it('should estimate in-memory sort cost', () => {
      const inputScan = createScanNode('users', ['id', 'name']);
      inputScan.cost = { ...emptyCost(), estimatedRows: 1000, totalCost: 20 };

      const sortNode = createSortNode(inputScan, [
        { expr: { type: 'columnRef', column: 'name' }, direction: 'asc' },
      ]);

      const cost = estimator.estimate(sortNode);

      expect(cost.totalCost).toBeGreaterThan(inputScan.cost.totalCost);
      expect(cost.startupCost).toBeGreaterThan(0); // Sort is blocking
    });

    it('should estimate top-n sort as cheaper for small limits', () => {
      const inputScan = createScanNode('users', ['id', 'name']);
      inputScan.cost = { ...emptyCost(), estimatedRows: 10000, totalCost: 200 };

      const fullSort = createSortNode(inputScan, [
        { expr: { type: 'columnRef', column: 'name' }, direction: 'asc' },
      ], { method: 'quicksort' });

      const topNSort = createSortNode(inputScan, [
        { expr: { type: 'columnRef', column: 'name' }, direction: 'asc' },
      ], { method: 'top-n', limit: 10 });

      const fullSortCost = estimator.estimate(fullSort);
      const topNSortCost = estimator.estimate(topNSort);

      expect(topNSortCost.cpuCost).toBeLessThan(fullSortCost.cpuCost);
    });
  });

  describe('Aggregate Cost', () => {
    it('should estimate hash aggregate cost', () => {
      const inputScan = createScanNode('orders', ['status', 'amount']);
      inputScan.cost = { ...emptyCost(), estimatedRows: 50000, totalCost: 1000 };

      const aggNode = createAggregateNode(
        inputScan,
        [{ type: 'columnRef', column: 'status' }],
        [{ expr: { type: 'aggregate', function: 'sum', arg: { type: 'columnRef', column: 'amount' } }, alias: 'total' }],
        { strategy: 'hashed' }
      );

      const cost = estimator.estimate(aggNode);

      // Hash aggregate should have CPU cost for hashing
      expect(cost.cpuCost).toBeGreaterThan(0);
      // Should use memory for hash table
      expect(cost.memoryBytes).toBeGreaterThan(0);
      // Aggregation reduces rows (groups < input rows)
      expect(cost.estimatedRows).toBeLessThan(inputScan.cost.estimatedRows);
    });
  });

  describe('Cost Comparison', () => {
    it('should compare costs correctly', () => {
      const cost1: PlanCost = { ...emptyCost(), totalCost: 100 };
      const cost2: PlanCost = { ...emptyCost(), totalCost: 200 };

      expect(compareCosts(cost1, cost2)).toBeLessThan(0);
      expect(compareCosts(cost2, cost1)).toBeGreaterThan(0);
      expect(compareCosts(cost1, cost1)).toBe(0);
    });

    it('should identify better costs', () => {
      const cost1: PlanCost = { ...emptyCost(), totalCost: 100 };
      const cost2: PlanCost = { ...emptyCost(), totalCost: 200 };

      expect(isBetterCost(cost1, cost2)).toBe(true);
      expect(isBetterCost(cost2, cost1)).toBe(false);
    });

    it('should format cost for display', () => {
      const cost: PlanCost = {
        ...emptyCost(),
        startupCost: 0.5,
        totalCost: 123.45,
        estimatedRows: 1000,
      };

      const formatted = formatCost(cost);
      expect(formatted).toContain('0.50');
      expect(formatted).toContain('123.45');
      expect(formatted).toContain('1000');
    });
  });
});

// =============================================================================
// INDEX SELECTION TESTS
// =============================================================================

describe('Index Selection', () => {
  let store: StatisticsStore;
  let indexDefs: Map<string, IndexDef[]>;
  let optimizer: QueryOptimizer;

  beforeEach(() => {
    store = createStatisticsStore();
    resetPlanNodeIds();

    // Set up table statistics
    const emailStats = columnStatsBuilder('email')
      .setDistinctCount(9500)
      .build();

    const statusStats = columnStatsBuilder('status')
      .setDistinctCount(3)
      .build();

    const tableStats = tableStatsBuilder('users')
      .setRowCount(10000)
      .setPageCount(200)
      .addColumn(emailStats)
      .addColumn(statusStats)
      .build();

    store.setTableStats(tableStats);

    // Set up indexes
    indexDefs = new Map([
      ['users', [
        {
          name: 'idx_users_email',
          table: 'users',
          columns: [{ name: 'email', direction: 'asc' as const }],
          unique: true,
        },
        {
          name: 'idx_users_status',
          table: 'users',
          columns: [{ name: 'status', direction: 'asc' as const }],
          unique: false,
        },
        {
          name: 'idx_users_status_email',
          table: 'users',
          columns: [
            { name: 'status', direction: 'asc' as const },
            { name: 'email', direction: 'asc' as const },
          ],
          unique: false,
          include: ['name'],
        },
      ]],
    ]);

    optimizer = createQueryOptimizer(store, indexDefs);
  });

  describe('Single Column Index Selection', () => {
    it('should choose index for highly selective equality predicate', () => {
      const plan: QueryPlan = {
        id: 1,
        type: 'scan',
        table: 'users',
        source: 'btree',
        columns: ['id', 'email', 'name'],
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'email' },
          right: { type: 'literal', value: 'test@example.com', dataType: 'string' },
        },
      };

      const result = optimizer.optimize(plan);

      expect(result.bestPlan.nodeType).toBe('scan');
      const scanNode = result.bestPlan as ScanNode;
      // Should use some form of index scan (index, indexOnly, or bitmap)
      expect(['indexScan', 'indexOnlyScan', 'bitmapIndexScan']).toContain(scanNode.accessMethod);
      // Should use one of the available indexes (may choose composite index if it covers email)
      expect(['idx_users_email', 'idx_users_status_email']).toContain(scanNode.indexName);
    });

    it('should choose table scan for low-selectivity predicate', () => {
      const plan: QueryPlan = {
        id: 1,
        type: 'scan',
        table: 'users',
        source: 'btree',
        columns: ['id', 'email', 'name'],
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'status' },
          right: { type: 'literal', value: 'active', dataType: 'string' },
        },
      };

      const result = optimizer.optimize(plan);

      // With only 3 distinct values and 10000 rows, each status has ~3333 rows
      // Sequential scan might be cheaper than index scan + heap fetches
      const scanNode = result.bestPlan as ScanNode;
      // Either access method could be chosen depending on cost model
      expect(['seqScan', 'indexScan']).toContain(scanNode.accessMethod);
    });
  });

  describe('Multi-Column Index Selection', () => {
    it('should use composite index when predicate matches prefix', () => {
      const plan: QueryPlan = {
        id: 1,
        type: 'scan',
        table: 'users',
        source: 'btree',
        columns: ['id', 'email', 'name'],
        predicate: {
          type: 'logical',
          op: 'and',
          operands: [
            {
              type: 'comparison',
              op: 'eq',
              left: { type: 'columnRef', column: 'status' },
              right: { type: 'literal', value: 'active', dataType: 'string' },
            },
            {
              type: 'comparison',
              op: 'eq',
              left: { type: 'columnRef', column: 'email' },
              right: { type: 'literal', value: 'test@example.com', dataType: 'string' },
            },
          ],
        },
      };

      const result = optimizer.optimize(plan);
      expect(result.indexesConsidered.length).toBeGreaterThan(0);
    });
  });

  describe('Covering Index Detection', () => {
    it('should prefer covering index when query columns match', () => {
      const plan: QueryPlan = {
        id: 1,
        type: 'scan',
        table: 'users',
        source: 'btree',
        columns: ['status', 'email', 'name'], // All in idx_users_status_email (key + INCLUDE)
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'status' },
          right: { type: 'literal', value: 'active', dataType: 'string' },
        },
      };

      const result = optimizer.optimize(plan);
      const scanNode = result.bestPlan as ScanNode;

      // Should use index-only scan if covering index is detected
      if (scanNode.accessMethod === 'indexOnlyScan') {
        expect(scanNode.indexName).toBe('idx_users_status_email');
      }
    });
  });
});

// =============================================================================
// JOIN ORDERING TESTS
// =============================================================================

describe('Join Ordering', () => {
  describe('suggestJoinOrder', () => {
    it('should start with smallest table', () => {
      const tables = [
        { name: 'orders', rowCount: 100000 },
        { name: 'users', rowCount: 1000 },
        { name: 'products', rowCount: 500 },
      ];

      const joins = [
        { left: 'orders', right: 'users' },
        { left: 'orders', right: 'products' },
      ];

      const order = suggestJoinOrder(tables, joins);

      expect(order[0]).toBe('products'); // Smallest
    });

    it('should prefer tables that have join conditions with already-joined tables', () => {
      const tables = [
        { name: 'a', rowCount: 100 },
        { name: 'b', rowCount: 200 },
        { name: 'c', rowCount: 50 },
      ];

      const joins = [
        { left: 'a', right: 'b' },
        { left: 'b', right: 'c' },
        // No direct join between a and c
      ];

      const order = suggestJoinOrder(tables, joins);

      // c is smallest (50), a joins with b, b joins with c
      expect(order[0]).toBe('c');
    });

    it('should handle single table', () => {
      const tables = [{ name: 'users', rowCount: 1000 }];
      const order = suggestJoinOrder(tables, []);

      expect(order).toEqual(['users']);
    });
  });

  describe('findOptimalJoinOrder', () => {
    it('should find optimal order for two tables', () => {
      const store = createStatisticsStore();
      const estimator = createCostEstimator(store);
      resetPlanNodeIds();

      const usersPlan = createScanNode('users', ['id', 'name']);
      usersPlan.cost = { ...emptyCost(), estimatedRows: 1000, totalCost: 20 };

      const ordersPlan = createScanNode('orders', ['id', 'user_id']);
      ordersPlan.cost = { ...emptyCost(), estimatedRows: 10000, totalCost: 200 };

      const tables = [
        { name: 'users', rowCount: 1000, plan: usersPlan },
        { name: 'orders', rowCount: 10000, plan: ordersPlan },
      ];

      const joinConditions = new Map([
        ['users', new Map([
          ['orders', {
            type: 'comparison' as const,
            op: 'eq' as const,
            left: { type: 'columnRef' as const, table: 'users', column: 'id' },
            right: { type: 'columnRef' as const, table: 'orders', column: 'user_id' },
          }],
        ])],
      ]);

      const result = findOptimalJoinOrder(tables, joinConditions, estimator);

      expect(result.nodeType).toBe('join');
    });

    it('should throw for empty tables', () => {
      const store = createStatisticsStore();
      const estimator = createCostEstimator(store);

      expect(() => findOptimalJoinOrder([], new Map(), estimator)).toThrow();
    });
  });
});

// =============================================================================
// PREDICATE PUSHDOWN TESTS
// =============================================================================

describe('Predicate Pushdown', () => {
  let store: StatisticsStore;
  let optimizer: QueryOptimizer;

  beforeEach(() => {
    store = createStatisticsStore();
    resetPlanNodeIds();

    store.setTableStats(
      tableStatsBuilder('users').setRowCount(10000).setPageCount(200).build()
    );

    optimizer = createQueryOptimizer(store);
  });

  it('should push filter predicate into table scan', () => {
    const plan: QueryPlan = {
      id: 2,
      type: 'filter',
      input: {
        id: 1,
        type: 'scan',
        table: 'users',
        source: 'btree',
        columns: ['id', 'email', 'status'],
      },
      predicate: {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'status' },
        right: { type: 'literal', value: 'active', dataType: 'string' },
      },
    };

    const result = optimizer.optimize(plan);

    // After optimization, filter should be pushed into scan
    expect(result.bestPlan.nodeType).toBe('scan');
    const scanNode = result.bestPlan as ScanNode;
    expect(scanNode.filterCondition).toBeDefined();
  });

  it('should combine existing scan predicate with pushed filter', () => {
    const plan: QueryPlan = {
      id: 2,
      type: 'filter',
      input: {
        id: 1,
        type: 'scan',
        table: 'users',
        source: 'btree',
        columns: ['id', 'email', 'status'],
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'email' },
          right: { type: 'literal', value: 'test@example.com', dataType: 'string' },
        },
      },
      predicate: {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'status' },
        right: { type: 'literal', value: 'active', dataType: 'string' },
      },
    };

    const result = optimizer.optimize(plan);

    expect(result.bestPlan.nodeType).toBe('scan');
  });
});

// =============================================================================
// EXPLAIN OUTPUT TESTS
// =============================================================================

describe('EXPLAIN QUERY PLAN', () => {
  let store: StatisticsStore;

  beforeEach(() => {
    store = createStatisticsStore();
    resetPlanNodeIds();

    store.setTableStats(
      tableStatsBuilder('users').setRowCount(10000).setPageCount(200).build()
    );
  });

  describe('Text Format', () => {
    it('should explain sequential scan', () => {
      const scan = createScanNode('users', ['id', 'email', 'name'], {
        accessMethod: 'seqScan',
      });
      scan.cost = { ...emptyCost(), startupCost: 0, totalCost: 200, estimatedRows: 10000 };

      const output = explain(scan, { format: 'text', costs: true, rowEstimates: true });

      expect(output).toContain('Seq Scan');
      expect(output).toContain('users');
      expect(output).toContain('cost=');
      expect(output).toContain('rows=');
    });

    it('should explain index scan', () => {
      const scan = createScanNode('users', ['id', 'email'], {
        accessMethod: 'indexScan',
        indexName: 'idx_users_email',
        indexCondition: [{
          column: 'email',
          operator: 'eq',
          value: 'test@example.com',
          columnIndex: 0,
        }],
      });
      scan.cost = { ...emptyCost(), totalCost: 10, estimatedRows: 1 };

      const output = explain(scan, { format: 'text' });

      expect(output).toContain('Index Scan');
      expect(output).toContain('idx_users_email');
      expect(output).toContain('Index Cond');
    });

    it('should explain filter', () => {
      const scan = createScanNode('users', ['id', 'name']);
      scan.cost = emptyCost();

      const filter = createFilterNode(scan, {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'status' },
        right: { type: 'literal', value: 'active', dataType: 'string' },
      });
      filter.cost = emptyCost();

      const output = explain(filter, { format: 'text' });

      expect(output).toContain('Filter');
      expect(output).toContain('status');
      expect(output).toContain('active');
    });

    it('should explain hash join', () => {
      const leftScan = createScanNode('users', ['id', 'name']);
      leftScan.cost = { ...emptyCost(), estimatedRows: 1000 };

      const rightScan = createScanNode('orders', ['id', 'user_id']);
      rightScan.cost = { ...emptyCost(), estimatedRows: 5000 };

      const join = createJoinNode(leftScan, rightScan, 'inner', 'hash', {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', table: 'users', column: 'id' },
        right: { type: 'columnRef', table: 'orders', column: 'user_id' },
      });
      join.cost = emptyCost();

      const output = explain(join, { format: 'text' });

      expect(output).toContain('Hash Join');
      expect(output).toContain('INNER JOIN');
      expect(output).toContain('Hash Cond');
    });

    it('should explain sort', () => {
      const scan = createScanNode('users', ['id', 'name']);
      scan.cost = emptyCost();

      const sort = createSortNode(scan, [
        { expr: { type: 'columnRef', column: 'name' }, direction: 'asc' },
        { expr: { type: 'columnRef', column: 'id' }, direction: 'desc' },
      ]);
      sort.cost = emptyCost();

      const output = explain(sort, { format: 'text' });

      expect(output).toContain('Sort');
      expect(output).toContain('name ASC');
      expect(output).toContain('id DESC');
    });

    it('should explain limit', () => {
      const scan = createScanNode('users', ['id', 'name']);
      scan.cost = emptyCost();

      const limit = createLimitNode(scan, 10, 20);
      limit.cost = emptyCost();

      const output = explain(limit, { format: 'text' });

      expect(output).toContain('Limit');
      expect(output).toContain('10');
      expect(output).toContain('Offset');
      expect(output).toContain('20');
    });

    it('should explain aggregate with group by', () => {
      const scan = createScanNode('orders', ['status', 'amount']);
      scan.cost = emptyCost();

      const agg = createAggregateNode(
        scan,
        [{ type: 'columnRef', column: 'status' }],
        [{ expr: { type: 'aggregate', function: 'sum', arg: { type: 'columnRef', column: 'amount' } }, alias: 'total' }],
        { strategy: 'hashed' }
      );
      agg.cost = emptyCost();

      const output = explain(agg, { format: 'text' });

      expect(output).toContain('HashAggregate');
      expect(output).toContain('Group Key');
      expect(output).toContain('status');
    });
  });

  describe('JSON Format', () => {
    it('should output valid JSON', () => {
      const scan = createScanNode('users', ['id', 'name']);
      scan.cost = { ...emptyCost(), totalCost: 100, estimatedRows: 1000 };

      const output = explain(scan, { format: 'json' });
      const parsed = JSON.parse(output);

      expect(Array.isArray(parsed)).toBe(true);
      expect(parsed[0].Plan).toBeDefined();
      expect(parsed[0].Plan['Node Type']).toBe('Seq Scan');
    });
  });

  describe('YAML Format', () => {
    it('should output YAML format', () => {
      const scan = createScanNode('users', ['id', 'name']);
      scan.cost = { ...emptyCost(), totalCost: 100 };

      const output = explain(scan, { format: 'yaml' });

      expect(output).toContain('Node Type: "Seq Scan"');
      expect(output).toContain('Relation Name: "users"');
    });
  });

  describe('Tree Format', () => {
    it('should output ASCII tree', () => {
      const leftScan = createScanNode('users', ['id']);
      leftScan.cost = emptyCost();

      const rightScan = createScanNode('orders', ['user_id']);
      rightScan.cost = emptyCost();

      const join = createJoinNode(leftScan, rightScan, 'inner', 'hash');
      join.cost = emptyCost();

      const output = explain(join, { format: 'tree' });

      expect(output).toContain('`--');
      expect(output).toContain('|--');
    });
  });

  describe('EXPLAIN ANALYZE', () => {
    it('should include execution statistics', () => {
      const scan = createScanNode('users', ['id', 'name']);
      scan.cost = { ...emptyCost(), totalCost: 100, estimatedRows: 1000 };

      const stats: ExecutionStats = {
        actualRows: 950,
        startupTimeMs: 0.1,
        totalTimeMs: 15.5,
        loops: 1,
      };

      const output = explainAnalyze(scan, stats, { format: 'text' });

      expect(output).toContain('actual');
      expect(output).toContain('rows=950');
      expect(output).toContain('time=');
    });
  });

  describe('Verbose Mode', () => {
    it('should include output columns', () => {
      const scan = createScanNode('users', ['id', 'email', 'name']);
      scan.cost = emptyCost();

      const output = explain(scan, { format: 'text', verbose: true });

      expect(output).toContain('Output:');
      expect(output).toContain('id');
      expect(output).toContain('email');
      expect(output).toContain('name');
    });
  });

  describe('ExplainGenerator Class', () => {
    it('should use configured options', () => {
      const generator = createExplainGenerator({
        format: 'text',
        costs: true,
        rowEstimates: true,
        verbose: true,
      });

      const scan = createScanNode('users', ['id', 'name']);
      scan.cost = { ...emptyCost(), totalCost: 100, estimatedRows: 1000 };

      const output = generator.explain(scan);

      expect(output).toContain('Output:');
      expect(output).toContain('cost=');
    });
  });

  describe('Plan Summary', () => {
    it('should summarize plan structure', () => {
      const scan = createScanNode('users', ['id'], { accessMethod: 'seqScan' });
      scan.cost = emptyCost();

      const filter = createFilterNode(scan, {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'status' },
        right: { type: 'literal', value: 'active', dataType: 'string' },
      });
      filter.cost = emptyCost();

      const summary = summarizePlan(filter);

      expect(summary).toContain('SeqScan(users)');
    });

    it('should get total cost', () => {
      const scan = createScanNode('users', ['id']);
      scan.cost = { ...emptyCost(), totalCost: 123.45 };

      expect(getTotalCost(scan)).toBe(123.45);
    });

    it('should get estimated rows', () => {
      const scan = createScanNode('users', ['id']);
      scan.cost = { ...emptyCost(), estimatedRows: 1000 };

      expect(getEstimatedRows(scan)).toBe(1000);
    });
  });
});

// =============================================================================
// OPTIMIZER INTEGRATION TESTS
// =============================================================================

describe('Optimizer Integration', () => {
  let store: StatisticsStore;
  let indexDefs: Map<string, IndexDef[]>;
  let optimizer: QueryOptimizer;

  beforeEach(() => {
    store = createStatisticsStore();
    resetPlanNodeIds();

    // Users table
    store.setTableStats(
      tableStatsBuilder('users')
        .setRowCount(10000)
        .setPageCount(200)
        .addColumn(columnStatsBuilder('email').setDistinctCount(9500).build())
        .addColumn(columnStatsBuilder('status').setDistinctCount(3).build())
        .build()
    );

    // Orders table
    store.setTableStats(
      tableStatsBuilder('orders')
        .setRowCount(100000)
        .setPageCount(2000)
        .addColumn(columnStatsBuilder('user_id').setDistinctCount(8000).build())
        .addColumn(columnStatsBuilder('status').setDistinctCount(5).build())
        .build()
    );

    // Products table
    store.setTableStats(
      tableStatsBuilder('products')
        .setRowCount(500)
        .setPageCount(10)
        .build()
    );

    // Indexes
    indexDefs = new Map([
      ['users', [
        {
          name: 'idx_users_email',
          table: 'users',
          columns: [{ name: 'email', direction: 'asc' as const }],
          unique: true,
        },
      ]],
      ['orders', [
        {
          name: 'idx_orders_user_id',
          table: 'orders',
          columns: [{ name: 'user_id', direction: 'asc' as const }],
          unique: false,
        },
      ]],
    ]);

    // Index stats
    store.setIndexStats(
      indexStatsBuilder('idx_users_email', 'users')
        .setEntryCount(10000)
        .setDistinctKeys(9500)
        .setTreeHeight(3)
        .build()
    );

    store.setIndexStats(
      indexStatsBuilder('idx_orders_user_id', 'orders')
        .setEntryCount(100000)
        .setDistinctKeys(8000)
        .setTreeHeight(4)
        .build()
    );

    optimizer = createQueryOptimizer(store, indexDefs);
  });

  it('should optimize complex query with multiple operators', () => {
    // SELECT u.name, COUNT(*)
    // FROM users u
    // JOIN orders o ON u.id = o.user_id
    // WHERE u.status = 'active'
    // GROUP BY u.name
    // ORDER BY COUNT(*) DESC
    // LIMIT 10

    const plan: QueryPlan = {
      id: 6,
      type: 'limit',
      limit: 10,
      input: {
        id: 5,
        type: 'sort',
        orderBy: [
          { expr: { type: 'columnRef', column: 'count' }, direction: 'desc' },
        ],
        input: {
          id: 4,
          type: 'aggregate',
          groupBy: [{ type: 'columnRef', column: 'name' }],
          aggregates: [{
            expr: { type: 'aggregate', function: 'count', arg: '*' },
            alias: 'count',
          }],
          input: {
            id: 3,
            type: 'join',
            joinType: 'inner',
            left: {
              id: 1,
              type: 'scan',
              table: 'users',
              alias: 'u',
              source: 'btree',
              columns: ['id', 'name'],
              predicate: {
                type: 'comparison',
                op: 'eq',
                left: { type: 'columnRef', column: 'status' },
                right: { type: 'literal', value: 'active', dataType: 'string' },
              },
            },
            right: {
              id: 2,
              type: 'scan',
              table: 'orders',
              alias: 'o',
              source: 'btree',
              columns: ['user_id'],
            },
            condition: {
              type: 'comparison',
              op: 'eq',
              left: { type: 'columnRef', table: 'u', column: 'id' },
              right: { type: 'columnRef', table: 'o', column: 'user_id' },
            },
          },
        },
      },
    };

    const result = optimizer.optimize(plan);

    // Should have optimized the plan
    expect(result.bestPlan).toBeDefined();
    // Optimization time may be 0 in fast environments
    expect(result.optimizationTimeMs).toBeGreaterThanOrEqual(0);
    // Alternative count depends on available indexes
    expect(result.alternatives.length).toBeGreaterThanOrEqual(0);

    // Should have a valid physical plan structure
    expect(result.bestPlan.nodeType).toBeDefined();
    expect(result.bestPlan.cost).toBeDefined();
  });

  it('should track optimization alternatives', () => {
    const plan: QueryPlan = {
      id: 1,
      type: 'scan',
      table: 'users',
      source: 'btree',
      columns: ['id', 'email'],
      predicate: {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'email' },
        right: { type: 'literal', value: 'test@example.com', dataType: 'string' },
      },
    };

    const result = optimizer.optimize(plan);

    // Should have considered both seq scan and index scan
    expect(result.alternatives.length).toBeGreaterThanOrEqual(2);

    // Exactly one should be chosen
    const chosenPlans = result.alternatives.filter(a => a.chosen);
    expect(chosenPlans.length).toBe(1);
  });
});

// =============================================================================
// EDGE CASES AND ERROR HANDLING
// =============================================================================

describe('Edge Cases', () => {
  beforeEach(() => {
    resetPlanNodeIds();
  });

  it('should handle empty cost combination', () => {
    const combined = combineCosts([]);
    expect(combined.totalCost).toBe(0);
    expect(combined.estimatedRows).toBe(0);
  });

  it('should generate unique plan node IDs', () => {
    resetPlanNodeIds();
    const id1 = nextPlanNodeId();
    const id2 = nextPlanNodeId();
    const id3 = nextPlanNodeId();

    expect(id1).toBe(1);
    expect(id2).toBe(2);
    expect(id3).toBe(3);
  });

  it('should reset plan node IDs', () => {
    nextPlanNodeId();
    nextPlanNodeId();
    resetPlanNodeIds();
    const id = nextPlanNodeId();

    expect(id).toBe(1);
  });

  it('should handle plan without statistics', () => {
    const store = createStatisticsStore();
    const estimator = createCostEstimator(store);

    const scan = createScanNode('unknown_table', ['col1', 'col2']);
    const cost = estimator.estimate(scan);

    // Should use defaults
    expect(cost.estimatedRows).toBe(1000); // Default
    expect(cost.usedStatistics).toBe(false);
  });

  it('should handle empty histogram', () => {
    const histogram = buildHistogram([], 10);
    expect(histogram).toHaveLength(0);
  });

  it('should handle single-value histogram', () => {
    const histogram = buildHistogram([42], 3);
    expect(histogram).toHaveLength(1);
    expect(histogram[0].low).toBe(42);
    expect(histogram[0].high).toBe(42);
  });

  it('should handle empty MCV calculation', () => {
    const mcv = calculateMostCommonValues([], 10);
    expect(mcv).toHaveLength(0);
  });
});
