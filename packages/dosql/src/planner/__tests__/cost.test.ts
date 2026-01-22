/**
 * TDD Tests for DoSQL Cost Estimator SqlValue Type Safety
 *
 * Issue: pocs-ttcz - Replace `any` in planner/cost.ts with SqlValue type
 *
 * These tests verify that:
 * 1. Index condition arrays are properly typed with SqlValue
 * 2. All comparison operations work correctly with SqlValue types
 * 3. Cost estimation handles all SqlValue types (string, number, bigint, boolean, Date, null, Uint8Array)
 */

import { describe, it, expect, beforeEach } from 'vitest';
import type { SqlValue, Predicate, ComparisonOp } from '../../engine/types.js';
import {
  CostEstimator,
  createCostEstimator,
  StatisticsStore,
  createStatisticsStore,
  tableStatsBuilder,
  columnStatsBuilder,
  indexStatsBuilder,
  resetPlanNodeIds,
  createScanNode,
  type IndexCondition,
  type ScanNode,
} from '../index.js';

// =============================================================================
// INDEX CONDITION SQLVALUE TYPE TESTS
// =============================================================================

describe('IndexCondition SqlValue Type Safety', () => {
  let store: StatisticsStore;
  let estimator: CostEstimator;

  beforeEach(() => {
    store = createStatisticsStore();
    resetPlanNodeIds();

    // Setup test table statistics
    const tableStats = tableStatsBuilder('users')
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
        columnStatsBuilder('created_at')
          .setDistinctCount(8000)
          .setMinMax(new Date('2020-01-01'), new Date('2024-01-01'))
          .build()
      )
      .addColumn(
        columnStatsBuilder('is_active')
          .setDistinctCount(2)
          .build()
      )
      .addColumn(
        columnStatsBuilder('balance')
          .setDistinctCount(1000)
          .setMinMax(BigInt(0), BigInt(1000000))
          .build()
      )
      .build();

    store.setTableStats(tableStats);

    // Setup index statistics
    const indexStats = indexStatsBuilder('users_id_idx', 'users')
      .setEntryCount(10000)
      .setDistinctKeys(10000)
      .setTreeHeight(3)
      .setClusteringFactor(0.9)
      .build();

    store.setIndexStats(indexStats);

    estimator = createCostEstimator(store);
  });

  describe('String SqlValue in IndexCondition', () => {
    it('should handle string equality condition', () => {
      const condition: IndexCondition = {
        column: 'name',
        operator: 'eq' as ComparisonOp,
        value: 'Alice' as SqlValue,
        columnIndex: 0,
      };

      expect(condition.value).toBe('Alice');
      expect(typeof condition.value).toBe('string');
    });

    it('should handle string range condition', () => {
      const condition: IndexCondition = {
        column: 'name',
        operator: 'ge' as ComparisonOp,
        value: 'M' as SqlValue,
        columnIndex: 0,
      };

      expect(condition.value).toBe('M');
    });
  });

  describe('Number SqlValue in IndexCondition', () => {
    it('should handle number equality condition', () => {
      const condition: IndexCondition = {
        column: 'id',
        operator: 'eq' as ComparisonOp,
        value: 42 as SqlValue,
        columnIndex: 0,
      };

      expect(condition.value).toBe(42);
      expect(typeof condition.value).toBe('number');
    });

    it('should handle number range conditions', () => {
      const ltCondition: IndexCondition = {
        column: 'id',
        operator: 'lt' as ComparisonOp,
        value: 100 as SqlValue,
        columnIndex: 0,
      };

      const gtCondition: IndexCondition = {
        column: 'id',
        operator: 'gt' as ComparisonOp,
        value: 50 as SqlValue,
        columnIndex: 0,
      };

      expect(ltCondition.value).toBe(100);
      expect(gtCondition.value).toBe(50);
    });
  });

  describe('Bigint SqlValue in IndexCondition', () => {
    it('should handle bigint equality condition', () => {
      const condition: IndexCondition = {
        column: 'balance',
        operator: 'eq' as ComparisonOp,
        value: BigInt(500000) as SqlValue,
        columnIndex: 0,
      };

      expect(condition.value).toBe(BigInt(500000));
      expect(typeof condition.value).toBe('bigint');
    });

    it('should handle bigint range condition', () => {
      const condition: IndexCondition = {
        column: 'balance',
        operator: 'ge' as ComparisonOp,
        value: BigInt(100000) as SqlValue,
        columnIndex: 0,
      };

      expect(condition.value).toBe(BigInt(100000));
    });
  });

  describe('Boolean SqlValue in IndexCondition', () => {
    it('should handle boolean equality condition', () => {
      const trueCondition: IndexCondition = {
        column: 'is_active',
        operator: 'eq' as ComparisonOp,
        value: true as SqlValue,
        columnIndex: 0,
      };

      const falseCondition: IndexCondition = {
        column: 'is_active',
        operator: 'eq' as ComparisonOp,
        value: false as SqlValue,
        columnIndex: 0,
      };

      expect(trueCondition.value).toBe(true);
      expect(falseCondition.value).toBe(false);
      expect(typeof trueCondition.value).toBe('boolean');
    });
  });

  describe('Date SqlValue in IndexCondition', () => {
    it('should handle Date equality condition', () => {
      const date = new Date('2023-06-15');
      const condition: IndexCondition = {
        column: 'created_at',
        operator: 'eq' as ComparisonOp,
        value: date as SqlValue,
        columnIndex: 0,
      };

      expect(condition.value).toEqual(date);
      expect(condition.value).toBeInstanceOf(Date);
    });

    it('should handle Date range condition', () => {
      const startDate = new Date('2023-01-01');
      const condition: IndexCondition = {
        column: 'created_at',
        operator: 'ge' as ComparisonOp,
        value: startDate as SqlValue,
        columnIndex: 0,
      };

      expect(condition.value).toEqual(startDate);
    });
  });

  describe('Null SqlValue in IndexCondition', () => {
    it('should handle null value in condition', () => {
      const condition: IndexCondition = {
        column: 'name',
        operator: 'eq' as ComparisonOp,
        value: null as SqlValue,
        columnIndex: 0,
      };

      expect(condition.value).toBeNull();
    });
  });

  describe('Uint8Array SqlValue in IndexCondition', () => {
    it('should handle Uint8Array (bytes) value in condition', () => {
      const bytes = new Uint8Array([0x01, 0x02, 0x03, 0x04]);
      const condition: IndexCondition = {
        column: 'data',
        operator: 'eq' as ComparisonOp,
        value: bytes as SqlValue,
        columnIndex: 0,
      };

      expect(condition.value).toBeInstanceOf(Uint8Array);
      expect(condition.value).toEqual(bytes);
    });
  });
});

// =============================================================================
// COST ESTIMATOR INDEX CONDITION SELECTIVITY TESTS
// =============================================================================

describe('CostEstimator Index Condition Selectivity with SqlValue', () => {
  let store: StatisticsStore;
  let estimator: CostEstimator;

  beforeEach(() => {
    store = createStatisticsStore();
    resetPlanNodeIds();

    const tableStats = tableStatsBuilder('products')
      .setRowCount(50000)
      .setPageCount(1000)
      .addColumn(
        columnStatsBuilder('price')
          .setDistinctCount(500)
          .setMinMax(0, 1000)
          .build()
      )
      .addColumn(
        columnStatsBuilder('category')
          .setDistinctCount(20)
          .build()
      )
      .addColumn(
        columnStatsBuilder('created_at')
          .setDistinctCount(30000)
          .setMinMax(new Date('2020-01-01'), new Date('2024-12-31'))
          .build()
      )
      .build();

    store.setTableStats(tableStats);

    const indexStats = indexStatsBuilder('products_price_idx', 'products')
      .setEntryCount(50000)
      .setDistinctKeys(500)
      .setTreeHeight(4)
      .build();

    store.setIndexStats(indexStats);

    estimator = createCostEstimator(store);
  });

  it('should estimate cost for index scan with number condition', () => {
    const scanNode = createScanNode('products', ['id', 'name', 'price'], {
      accessMethod: 'indexScan',
      indexName: 'products_price_idx',
      indexCondition: [
        {
          column: 'price',
          operator: 'eq' as ComparisonOp,
          value: 99.99 as SqlValue,
          columnIndex: 0,
        },
      ],
    });

    const cost = estimator.estimate(scanNode);

    expect(cost.estimatedRows).toBeGreaterThan(0);
    expect(cost.estimatedRows).toBeLessThan(50000);
    expect(cost.totalCost).toBeGreaterThan(0);
    expect(cost.confidence).toBeGreaterThan(0);
  });

  it('should estimate cost for index scan with string condition', () => {
    const scanNode = createScanNode('products', ['id', 'name', 'category'], {
      accessMethod: 'indexScan',
      indexName: 'products_category_idx',
      indexCondition: [
        {
          column: 'category',
          operator: 'eq' as ComparisonOp,
          value: 'Electronics' as SqlValue,
          columnIndex: 0,
        },
      ],
    });

    const cost = estimator.estimate(scanNode);

    expect(cost.estimatedRows).toBeGreaterThan(0);
    expect(cost.totalCost).toBeGreaterThan(0);
  });

  it('should estimate cost for index scan with Date range condition', () => {
    const scanNode = createScanNode('products', ['id', 'name', 'created_at'], {
      accessMethod: 'indexScan',
      indexName: 'products_created_idx',
      indexCondition: [
        {
          column: 'created_at',
          operator: 'ge' as ComparisonOp,
          value: new Date('2024-01-01') as SqlValue,
          columnIndex: 0,
        },
      ],
    });

    const cost = estimator.estimate(scanNode);

    expect(cost.estimatedRows).toBeGreaterThan(0);
    expect(cost.totalCost).toBeGreaterThan(0);
  });

  it('should estimate cost for index scan with multiple conditions', () => {
    const conditions: IndexCondition[] = [
      {
        column: 'price',
        operator: 'ge' as ComparisonOp,
        value: 100 as SqlValue,
        columnIndex: 0,
      },
      {
        column: 'price',
        operator: 'le' as ComparisonOp,
        value: 500 as SqlValue,
        columnIndex: 0,
      },
    ];

    const scanNode = createScanNode('products', ['id', 'name', 'price'], {
      accessMethod: 'indexScan',
      indexName: 'products_price_idx',
      indexCondition: conditions,
    });

    const cost = estimator.estimate(scanNode);

    expect(cost.estimatedRows).toBeGreaterThan(0);
    expect(cost.estimatedRows).toBeLessThan(50000);
  });
});

// =============================================================================
// GETLITERALVALUE RETURN TYPE TESTS
// =============================================================================

describe('getLiteralValue SqlValue Return Type', () => {
  let store: StatisticsStore;
  let estimator: CostEstimator;

  beforeEach(() => {
    store = createStatisticsStore();
    resetPlanNodeIds();

    const tableStats = tableStatsBuilder('test_table')
      .setRowCount(1000)
      .addColumn(
        columnStatsBuilder('text_col')
          .setDistinctCount(100)
          .build()
      )
      .addColumn(
        columnStatsBuilder('num_col')
          .setDistinctCount(500)
          .setMinMax(0, 1000)
          .build()
      )
      .build();

    store.setTableStats(tableStats);
    estimator = createCostEstimator(store);
  });

  it('should handle string literal in predicate selectivity', () => {
    const predicate: Predicate = {
      type: 'comparison',
      op: 'eq',
      left: { type: 'columnRef', column: 'text_col' },
      right: { type: 'literal', value: 'test_value' as SqlValue, dataType: 'string' },
    };

    const selectivity = estimator.estimatePredicateSelectivity(predicate, 'test_table');
    expect(selectivity).toBeGreaterThan(0);
    expect(selectivity).toBeLessThanOrEqual(1);
  });

  it('should handle number literal in predicate selectivity', () => {
    const predicate: Predicate = {
      type: 'comparison',
      op: 'lt',
      left: { type: 'columnRef', column: 'num_col' },
      right: { type: 'literal', value: 500 as SqlValue, dataType: 'number' },
    };

    const selectivity = estimator.estimatePredicateSelectivity(predicate, 'test_table');
    expect(selectivity).toBeGreaterThan(0);
    expect(selectivity).toBeLessThanOrEqual(1);
  });

  it('should handle bigint literal in predicate selectivity', () => {
    const predicate: Predicate = {
      type: 'comparison',
      op: 'eq',
      left: { type: 'columnRef', column: 'num_col' },
      right: { type: 'literal', value: BigInt(12345) as SqlValue, dataType: 'bigint' },
    };

    const selectivity = estimator.estimatePredicateSelectivity(predicate, 'test_table');
    expect(selectivity).toBeGreaterThan(0);
    expect(selectivity).toBeLessThanOrEqual(1);
  });

  it('should handle boolean literal in predicate selectivity', () => {
    const predicate: Predicate = {
      type: 'comparison',
      op: 'eq',
      left: { type: 'columnRef', column: 'bool_col' },
      right: { type: 'literal', value: true as SqlValue, dataType: 'boolean' },
    };

    const selectivity = estimator.estimatePredicateSelectivity(predicate, 'test_table');
    expect(selectivity).toBeGreaterThan(0);
    expect(selectivity).toBeLessThanOrEqual(1);
  });

  it('should handle Date literal in predicate selectivity', () => {
    const predicate: Predicate = {
      type: 'comparison',
      op: 'ge',
      left: { type: 'columnRef', column: 'date_col' },
      right: { type: 'literal', value: new Date('2024-01-01') as SqlValue, dataType: 'date' },
    };

    const selectivity = estimator.estimatePredicateSelectivity(predicate, 'test_table');
    expect(selectivity).toBeGreaterThan(0);
    expect(selectivity).toBeLessThanOrEqual(1);
  });

  it('should handle null literal in predicate selectivity', () => {
    const predicate: Predicate = {
      type: 'comparison',
      op: 'eq',
      left: { type: 'columnRef', column: 'nullable_col' },
      right: { type: 'literal', value: null as SqlValue, dataType: 'null' },
    };

    const selectivity = estimator.estimatePredicateSelectivity(predicate, 'test_table');
    expect(selectivity).toBeGreaterThan(0);
    expect(selectivity).toBeLessThanOrEqual(1);
  });

  it('should handle Uint8Array literal in predicate selectivity', () => {
    const predicate: Predicate = {
      type: 'comparison',
      op: 'eq',
      left: { type: 'columnRef', column: 'blob_col' },
      right: { type: 'literal', value: new Uint8Array([1, 2, 3]) as SqlValue, dataType: 'bytes' },
    };

    const selectivity = estimator.estimatePredicateSelectivity(predicate, 'test_table');
    expect(selectivity).toBeGreaterThan(0);
    expect(selectivity).toBeLessThanOrEqual(1);
  });
});

// =============================================================================
// BETWEEN PREDICATE SQLVALUE TESTS
// =============================================================================

describe('BETWEEN Predicate with SqlValue Types', () => {
  let store: StatisticsStore;
  let estimator: CostEstimator;

  beforeEach(() => {
    store = createStatisticsStore();
    resetPlanNodeIds();

    const tableStats = tableStatsBuilder('orders')
      .setRowCount(100000)
      .addColumn(
        columnStatsBuilder('total')
          .setDistinctCount(5000)
          .setMinMax(0, 10000)
          .build()
      )
      .addColumn(
        columnStatsBuilder('order_date')
          .setDistinctCount(1000)
          .setMinMax(new Date('2020-01-01'), new Date('2024-12-31'))
          .build()
      )
      .build();

    store.setTableStats(tableStats);
    estimator = createCostEstimator(store);
  });

  it('should estimate BETWEEN predicate with number SqlValues', () => {
    const predicate: Predicate = {
      type: 'between',
      expr: { type: 'columnRef', column: 'total' },
      low: { type: 'literal', value: 100 as SqlValue, dataType: 'number' },
      high: { type: 'literal', value: 500 as SqlValue, dataType: 'number' },
    };

    const selectivity = estimator.estimatePredicateSelectivity(predicate, 'orders');
    expect(selectivity).toBeGreaterThan(0);
    expect(selectivity).toBeLessThanOrEqual(1);
  });

  it('should estimate BETWEEN predicate with Date SqlValues', () => {
    const predicate: Predicate = {
      type: 'between',
      expr: { type: 'columnRef', column: 'order_date' },
      low: { type: 'literal', value: new Date('2023-01-01') as SqlValue, dataType: 'date' },
      high: { type: 'literal', value: new Date('2023-12-31') as SqlValue, dataType: 'date' },
    };

    const selectivity = estimator.estimatePredicateSelectivity(predicate, 'orders');
    expect(selectivity).toBeGreaterThan(0);
    expect(selectivity).toBeLessThanOrEqual(1);
  });
});

// =============================================================================
// IN PREDICATE SQLVALUE TESTS
// =============================================================================

describe('IN Predicate with SqlValue Types', () => {
  let store: StatisticsStore;
  let estimator: CostEstimator;

  beforeEach(() => {
    store = createStatisticsStore();
    resetPlanNodeIds();

    const tableStats = tableStatsBuilder('products')
      .setRowCount(10000)
      .addColumn(
        columnStatsBuilder('status')
          .setDistinctCount(5)
          .build()
      )
      .addColumn(
        columnStatsBuilder('category_id')
          .setDistinctCount(100)
          .build()
      )
      .build();

    store.setTableStats(tableStats);
    estimator = createCostEstimator(store);
  });

  it('should estimate IN predicate with string SqlValues', () => {
    const predicate: Predicate = {
      type: 'in',
      expr: { type: 'columnRef', column: 'status' },
      values: [
        { type: 'literal', value: 'active' as SqlValue, dataType: 'string' },
        { type: 'literal', value: 'pending' as SqlValue, dataType: 'string' },
        { type: 'literal', value: 'shipped' as SqlValue, dataType: 'string' },
      ],
    };

    const selectivity = estimator.estimatePredicateSelectivity(predicate, 'products');
    expect(selectivity).toBeGreaterThan(0);
    expect(selectivity).toBeLessThanOrEqual(1);
  });

  it('should estimate IN predicate with number SqlValues', () => {
    const predicate: Predicate = {
      type: 'in',
      expr: { type: 'columnRef', column: 'category_id' },
      values: [
        { type: 'literal', value: 1 as SqlValue, dataType: 'number' },
        { type: 'literal', value: 5 as SqlValue, dataType: 'number' },
        { type: 'literal', value: 10 as SqlValue, dataType: 'number' },
        { type: 'literal', value: 15 as SqlValue, dataType: 'number' },
      ],
    };

    const selectivity = estimator.estimatePredicateSelectivity(predicate, 'products');
    expect(selectivity).toBeGreaterThan(0);
    expect(selectivity).toBeLessThanOrEqual(1);
  });
});

// =============================================================================
// COMPOUND PREDICATE WITH MIXED SQLVALUE TYPES
// =============================================================================

describe('Compound Predicates with Mixed SqlValue Types', () => {
  let store: StatisticsStore;
  let estimator: CostEstimator;

  beforeEach(() => {
    store = createStatisticsStore();
    resetPlanNodeIds();

    const tableStats = tableStatsBuilder('events')
      .setRowCount(500000)
      .addColumn(
        columnStatsBuilder('event_type')
          .setDistinctCount(50)
          .build()
      )
      .addColumn(
        columnStatsBuilder('timestamp')
          .setDistinctCount(100000)
          .setMinMax(new Date('2020-01-01'), new Date('2024-12-31'))
          .build()
      )
      .addColumn(
        columnStatsBuilder('user_id')
          .setDistinctCount(10000)
          .build()
      )
      .addColumn(
        columnStatsBuilder('is_processed')
          .setDistinctCount(2)
          .build()
      )
      .build();

    store.setTableStats(tableStats);
    estimator = createCostEstimator(store);
  });

  it('should estimate AND predicate with mixed SqlValue types', () => {
    const predicate: Predicate = {
      type: 'logical',
      op: 'and',
      operands: [
        {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'event_type' },
          right: { type: 'literal', value: 'click' as SqlValue, dataType: 'string' },
        },
        {
          type: 'comparison',
          op: 'ge',
          left: { type: 'columnRef', column: 'timestamp' },
          right: { type: 'literal', value: new Date('2024-01-01') as SqlValue, dataType: 'date' },
        },
        {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'is_processed' },
          right: { type: 'literal', value: false as SqlValue, dataType: 'boolean' },
        },
      ],
    };

    const selectivity = estimator.estimatePredicateSelectivity(predicate, 'events');
    expect(selectivity).toBeGreaterThan(0);
    expect(selectivity).toBeLessThan(1);
  });

  it('should estimate OR predicate with mixed SqlValue types', () => {
    const predicate: Predicate = {
      type: 'logical',
      op: 'or',
      operands: [
        {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'event_type' },
          right: { type: 'literal', value: 'click' as SqlValue, dataType: 'string' },
        },
        {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'user_id' },
          right: { type: 'literal', value: 12345 as SqlValue, dataType: 'number' },
        },
      ],
    };

    const selectivity = estimator.estimatePredicateSelectivity(predicate, 'events');
    expect(selectivity).toBeGreaterThan(0);
    expect(selectivity).toBeLessThanOrEqual(1);
  });
});

// =============================================================================
// TYPE SAFETY COMPILE-TIME CHECKS
// =============================================================================

describe('Type Safety Compile-Time Checks', () => {
  it('IndexCondition value should accept all SqlValue types', () => {
    // These should all compile without error
    const stringCondition: IndexCondition = {
      column: 'name',
      operator: 'eq',
      value: 'test',
      columnIndex: 0,
    };

    const numberCondition: IndexCondition = {
      column: 'id',
      operator: 'eq',
      value: 42,
      columnIndex: 0,
    };

    const bigintCondition: IndexCondition = {
      column: 'big_id',
      operator: 'eq',
      value: BigInt(9007199254740991),
      columnIndex: 0,
    };

    const boolCondition: IndexCondition = {
      column: 'active',
      operator: 'eq',
      value: true,
      columnIndex: 0,
    };

    const dateCondition: IndexCondition = {
      column: 'created',
      operator: 'eq',
      value: new Date(),
      columnIndex: 0,
    };

    const nullCondition: IndexCondition = {
      column: 'nullable',
      operator: 'eq',
      value: null,
      columnIndex: 0,
    };

    const bytesCondition: IndexCondition = {
      column: 'data',
      operator: 'eq',
      value: new Uint8Array([1, 2, 3]),
      columnIndex: 0,
    };

    // All conditions should have properly typed values
    expect(stringCondition.value).toBeDefined();
    expect(numberCondition.value).toBeDefined();
    expect(bigintCondition.value).toBeDefined();
    expect(boolCondition.value).toBeDefined();
    expect(dateCondition.value).toBeDefined();
    expect(nullCondition.value).toBeNull();
    expect(bytesCondition.value).toBeDefined();
  });
});
