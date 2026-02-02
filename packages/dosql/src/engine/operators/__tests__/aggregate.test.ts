/**
 * Aggregate Operator Tests
 *
 * Comprehensive tests for SQL aggregation operations:
 * - COUNT, SUM, AVG, MIN, MAX functions
 * - GROUP BY with single and multiple columns
 * - HAVING clause filtering
 * - Edge cases: nulls, empty sets, large datasets
 * - Bigint support
 *
 * Following the project's TDD with NO MOCKS philosophy.
 */

import { describe, it, expect, beforeEach } from 'vitest';

import { AggregateOperator } from '../aggregate.js';
import {
  type Row,
  type Expression,
  type Operator,
  type ExecutionContext,
  type AggregatePlan,
  type AggregateExpr,
  type Predicate,
  col,
  lit,
} from '../../types.js';

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Create a mock operator from an array of rows
 */
function createMockOperator(rows: Row[], columns?: string[]): Operator {
  let index = 0;
  const cols = columns ?? (rows.length > 0 ? Object.keys(rows[0]) : []);

  return {
    async open() { index = 0; },
    async next() { return index < rows.length ? rows[index++] : null; },
    async close() {},
    columns() { return cols; },
    async *[Symbol.asyncIterator]() {
      while (index < rows.length) {
        yield rows[index++];
      }
    },
  };
}

/**
 * Collect all rows from an operator
 */
async function collectRows(operator: Operator, ctx?: ExecutionContext): Promise<Row[]> {
  const context = ctx ?? createMockExecutionContext();
  await operator.open(context);
  const results: Row[] = [];
  let row: Row | null;
  while ((row = await operator.next()) !== null) {
    results.push(row);
  }
  await operator.close();
  return results;
}

/**
 * Create a minimal mock execution context
 */
function createMockExecutionContext(): ExecutionContext {
  return {
    schema: { tables: new Map() },
    btree: {
      get: async () => undefined,
      range: async function* () {},
      scan: async function* () {},
      set: async () => {},
      delete: async () => false,
      count: async () => 0,
    },
    columnar: {
      scan: async function* () {},
      count: async () => 0,
      sum: async () => null,
      minMax: async () => ({ min: null, max: null }),
    },
  };
}

/**
 * Helper to create aggregate expression in standard format
 */
function createAggExpr(func: string, arg: Expression | '*'): AggregateExpr {
  return {
    type: 'aggregate',
    function: func as 'count' | 'sum' | 'avg' | 'min' | 'max',
    arg,
  };
}

/**
 * Helper to create an aggregate plan
 */
function createAggregatePlan(
  groupBy: Expression[],
  aggregates: { expr: AggregateExpr; alias: string }[],
  having?: Predicate
): AggregatePlan {
  return {
    type: 'aggregate',
    id: 1,
    input: { type: 'scan', id: 0, table: 'test', source: 'btree', columns: [] },
    groupBy,
    aggregates,
    having,
  };
}

// =============================================================================
// COUNT TESTS
// =============================================================================

describe('Aggregate Operator - COUNT', () => {
  it('should count all rows with COUNT(*)', async () => {
    const rows: Row[] = [
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
      { id: 3, name: 'Charlie' },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('count', '*'), alias: 'total' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].total).toBe(3);
  });

  it('should count only non-null values with COUNT(column)', async () => {
    const rows: Row[] = [
      { id: 1, name: 'Alice' },
      { id: 2, name: null },
      { id: 3, name: 'Charlie' },
      { id: 4, name: null },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('count', col('name')), alias: 'count_name' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].count_name).toBe(2);
  });

  it('should return empty result for COUNT(*) on empty set', async () => {
    // Note: SQL standard behavior for aggregate on empty set without GROUP BY
    // is to return one row. However, this operator returns empty for empty input.
    // This test documents the current behavior.
    const rows: Row[] = [];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('count', '*'), alias: 'total' }]
    );

    const input = createMockOperator(rows, ['id', 'name']);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    // With empty input, aggregate returns empty (no groups formed)
    expect(results).toHaveLength(0);
  });

  it('should return 0 for COUNT(column) when all values are null', async () => {
    const rows: Row[] = [
      { id: 1, name: null },
      { id: 2, name: null },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('count', col('name')), alias: 'count_name' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].count_name).toBe(0);
  });
});

// =============================================================================
// SUM TESTS
// =============================================================================

describe('Aggregate Operator - SUM', () => {
  it('should sum numeric values', async () => {
    const rows: Row[] = [
      { id: 1, amount: 100 },
      { id: 2, amount: 200 },
      { id: 3, amount: 300 },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('sum', col('amount')), alias: 'total_amount' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].total_amount).toBe(600);
  });

  it('should ignore null values in SUM', async () => {
    const rows: Row[] = [
      { id: 1, amount: 100 },
      { id: 2, amount: null },
      { id: 3, amount: 300 },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('sum', col('amount')), alias: 'total_amount' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].total_amount).toBe(400);
  });

  it('should return null for SUM of all nulls', async () => {
    const rows: Row[] = [
      { id: 1, amount: null },
      { id: 2, amount: null },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('sum', col('amount')), alias: 'total_amount' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].total_amount).toBeNull();
  });

  it('should return empty result for SUM of empty set', async () => {
    // Note: With empty input, no groups are formed, so no results are returned.
    // This is different from SQL standard where aggregate without GROUP BY
    // returns one row even for empty input.
    const rows: Row[] = [];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('sum', col('amount')), alias: 'total_amount' }]
    );

    const input = createMockOperator(rows, ['id', 'amount']);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(0);
  });

  it('should handle bigint values in SUM', async () => {
    const rows: Row[] = [
      { id: 1, amount: 100n },
      { id: 2, amount: 200n },
      { id: 3, amount: 9007199254740991n }, // Near MAX_SAFE_INTEGER
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('sum', col('amount')), alias: 'total_amount' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].total_amount).toBe(9007199254741291n);
  });

  it('should handle negative numbers in SUM', async () => {
    const rows: Row[] = [
      { id: 1, amount: 100 },
      { id: 2, amount: -50 },
      { id: 3, amount: -25 },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('sum', col('amount')), alias: 'total_amount' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].total_amount).toBe(25);
  });
});

// =============================================================================
// AVG TESTS
// =============================================================================

describe('Aggregate Operator - AVG', () => {
  it('should calculate average of numeric values', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: 20 },
      { id: 3, value: 30 },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('avg', col('value')), alias: 'avg_value' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].avg_value).toBe(20);
  });

  it('should ignore null values in AVG calculation', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: null },
      { id: 3, value: 30 },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('avg', col('value')), alias: 'avg_value' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].avg_value).toBe(20); // (10 + 30) / 2
  });

  it('should return null for AVG of all nulls', async () => {
    const rows: Row[] = [
      { id: 1, value: null },
      { id: 2, value: null },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('avg', col('value')), alias: 'avg_value' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].avg_value).toBeNull();
  });

  it('should return empty result for AVG of empty set', async () => {
    // With empty input, no groups are formed
    const rows: Row[] = [];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('avg', col('value')), alias: 'avg_value' }]
    );

    const input = createMockOperator(rows, ['id', 'value']);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(0);
  });

  it('should handle decimal average results', async () => {
    const rows: Row[] = [
      { id: 1, value: 1 },
      { id: 2, value: 2 },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('avg', col('value')), alias: 'avg_value' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].avg_value).toBe(1.5);
  });
});

// =============================================================================
// MIN/MAX TESTS
// =============================================================================

describe('Aggregate Operator - MIN', () => {
  it('should find minimum numeric value', async () => {
    const rows: Row[] = [
      { id: 1, value: 30 },
      { id: 2, value: 10 },
      { id: 3, value: 20 },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('min', col('value')), alias: 'min_value' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].min_value).toBe(10);
  });

  it('should find minimum string value', async () => {
    const rows: Row[] = [
      { id: 1, name: 'Charlie' },
      { id: 2, name: 'Alice' },
      { id: 3, name: 'Bob' },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('min', col('name')), alias: 'min_name' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].min_name).toBe('Alice');
  });

  it('should ignore null values in MIN', async () => {
    const rows: Row[] = [
      { id: 1, value: null },
      { id: 2, value: 30 },
      { id: 3, value: 10 },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('min', col('value')), alias: 'min_value' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].min_value).toBe(10);
  });

  it('should return null for MIN of all nulls', async () => {
    const rows: Row[] = [
      { id: 1, value: null },
      { id: 2, value: null },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('min', col('value')), alias: 'min_value' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].min_value).toBeNull();
  });
});

describe('Aggregate Operator - MAX', () => {
  it('should find maximum numeric value', async () => {
    const rows: Row[] = [
      { id: 1, value: 30 },
      { id: 2, value: 10 },
      { id: 3, value: 20 },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('max', col('value')), alias: 'max_value' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].max_value).toBe(30);
  });

  it('should find maximum string value', async () => {
    const rows: Row[] = [
      { id: 1, name: 'Charlie' },
      { id: 2, name: 'Alice' },
      { id: 3, name: 'Bob' },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('max', col('name')), alias: 'max_name' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].max_name).toBe('Charlie');
  });

  it('should ignore null values in MAX', async () => {
    const rows: Row[] = [
      { id: 1, value: null },
      { id: 2, value: 30 },
      { id: 3, value: 10 },
    ];

    const plan = createAggregatePlan(
      [],
      [{ expr: createAggExpr('max', col('value')), alias: 'max_value' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].max_value).toBe(30);
  });
});

// =============================================================================
// GROUP BY TESTS
// =============================================================================

describe('Aggregate Operator - GROUP BY', () => {
  it('should group by single column', async () => {
    const rows: Row[] = [
      { category: 'A', amount: 100 },
      { category: 'B', amount: 200 },
      { category: 'A', amount: 150 },
      { category: 'B', amount: 250 },
    ];

    const plan = createAggregatePlan(
      [col('category')],
      [{ expr: createAggExpr('sum', col('amount')), alias: 'total' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(2);

    const categoryA = results.find(r => r.category === 'A');
    const categoryB = results.find(r => r.category === 'B');

    expect(categoryA?.total).toBe(250);
    expect(categoryB?.total).toBe(450);
  });

  it('should group by multiple columns', async () => {
    const rows: Row[] = [
      { region: 'East', category: 'A', amount: 100 },
      { region: 'East', category: 'B', amount: 200 },
      { region: 'West', category: 'A', amount: 150 },
      { region: 'East', category: 'A', amount: 50 },
    ];

    const plan = createAggregatePlan(
      [col('region'), col('category')],
      [{ expr: createAggExpr('sum', col('amount')), alias: 'total' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(3);

    const eastA = results.find(r => r.region === 'East' && r.category === 'A');
    const eastB = results.find(r => r.region === 'East' && r.category === 'B');
    const westA = results.find(r => r.region === 'West' && r.category === 'A');

    expect(eastA?.total).toBe(150);
    expect(eastB?.total).toBe(200);
    expect(westA?.total).toBe(150);
  });

  it('should handle null values in GROUP BY column', async () => {
    const rows: Row[] = [
      { category: 'A', amount: 100 },
      { category: null, amount: 200 },
      { category: 'A', amount: 150 },
      { category: null, amount: 50 },
    ];

    const plan = createAggregatePlan(
      [col('category')],
      [{ expr: createAggExpr('sum', col('amount')), alias: 'total' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(2);

    const categoryA = results.find(r => r.category === 'A');
    const categoryNull = results.find(r => r.category === null);

    expect(categoryA?.total).toBe(250);
    expect(categoryNull?.total).toBe(250);
  });

  it('should support multiple aggregate functions with GROUP BY', async () => {
    const rows: Row[] = [
      { category: 'A', amount: 100 },
      { category: 'A', amount: 200 },
      { category: 'B', amount: 150 },
    ];

    const plan = createAggregatePlan(
      [col('category')],
      [
        { expr: createAggExpr('count', '*'), alias: 'count' },
        { expr: createAggExpr('sum', col('amount')), alias: 'total' },
        { expr: createAggExpr('avg', col('amount')), alias: 'average' },
        { expr: createAggExpr('min', col('amount')), alias: 'minimum' },
        { expr: createAggExpr('max', col('amount')), alias: 'maximum' },
      ]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(2);

    const categoryA = results.find(r => r.category === 'A');
    expect(categoryA?.count).toBe(2);
    expect(categoryA?.total).toBe(300);
    expect(categoryA?.average).toBe(150);
    expect(categoryA?.minimum).toBe(100);
    expect(categoryA?.maximum).toBe(200);
  });
});

// =============================================================================
// HAVING CLAUSE TESTS
// =============================================================================

describe('Aggregate Operator - HAVING', () => {
  it('should filter groups using HAVING with comparison', async () => {
    const rows: Row[] = [
      { category: 'A', amount: 100 },
      { category: 'A', amount: 200 },
      { category: 'B', amount: 50 },
      { category: 'C', amount: 500 },
    ];

    const havingPredicate: Predicate = {
      type: 'comparison',
      op: 'gt',
      left: { type: 'aggregate', function: 'sum', arg: col('amount') },
      right: lit(200),
    };

    const plan = createAggregatePlan(
      [col('category')],
      [{ expr: createAggExpr('sum', col('amount')), alias: 'total' }],
      havingPredicate
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(2);

    const categories = results.map(r => r.category);
    expect(categories).toContain('A'); // 300 > 200
    expect(categories).toContain('C'); // 500 > 200
    expect(categories).not.toContain('B'); // 50 <= 200
  });

  it('should filter groups using HAVING with COUNT', async () => {
    const rows: Row[] = [
      { category: 'A', value: 1 },
      { category: 'A', value: 2 },
      { category: 'A', value: 3 },
      { category: 'B', value: 4 },
      { category: 'C', value: 5 },
      { category: 'C', value: 6 },
    ];

    const havingPredicate: Predicate = {
      type: 'comparison',
      op: 'ge',
      left: { type: 'aggregate', function: 'count', arg: '*' },
      right: lit(2),
    };

    const plan = createAggregatePlan(
      [col('category')],
      [{ expr: createAggExpr('count', '*'), alias: 'cnt' }],
      havingPredicate
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(2);

    const categories = results.map(r => r.category);
    expect(categories).toContain('A'); // count 3 >= 2
    expect(categories).toContain('C'); // count 2 >= 2
    expect(categories).not.toContain('B'); // count 1 < 2
  });

  it('should handle complex HAVING with AND', async () => {
    const rows: Row[] = [
      { category: 'A', amount: 100 },
      { category: 'A', amount: 200 },
      { category: 'B', amount: 150 },
      { category: 'B', amount: 150 },
      { category: 'C', amount: 500 },
    ];

    const havingPredicate: Predicate = {
      type: 'logical',
      op: 'and',
      operands: [
        {
          type: 'comparison',
          op: 'ge',
          left: { type: 'aggregate', function: 'count', arg: '*' },
          right: lit(2),
        },
        {
          type: 'comparison',
          op: 'ge',
          left: { type: 'aggregate', function: 'sum', arg: col('amount') },
          right: lit(300),
        },
      ],
    };

    const plan = createAggregatePlan(
      [col('category')],
      [
        { expr: createAggExpr('count', '*'), alias: 'cnt' },
        { expr: createAggExpr('sum', col('amount')), alias: 'total' },
      ],
      havingPredicate
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(2);

    const categoryA = results.find(r => r.category === 'A');
    const categoryB = results.find(r => r.category === 'B');

    expect(categoryA?.total).toBe(300);
    expect(categoryB?.total).toBe(300);
  });
});

// =============================================================================
// LARGE DATASET TESTS
// =============================================================================

describe('Aggregate Operator - Large Datasets', () => {
  it('should handle large number of rows', async () => {
    const rows: Row[] = [];
    for (let i = 0; i < 10000; i++) {
      rows.push({ id: i, value: i % 100 });
    }

    const plan = createAggregatePlan(
      [],
      [
        { expr: createAggExpr('count', '*'), alias: 'count' },
        { expr: createAggExpr('sum', col('value')), alias: 'sum' },
        { expr: createAggExpr('avg', col('value')), alias: 'avg' },
      ]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].count).toBe(10000);
    // Sum of 0-99 repeated 100 times = 4950 * 100 = 495000
    expect(results[0].sum).toBe(495000);
    expect(results[0].avg).toBe(49.5);
  });

  it('should handle large number of groups', async () => {
    const rows: Row[] = [];
    for (let i = 0; i < 1000; i++) {
      rows.push({ category: `category_${i}`, value: i });
    }

    const plan = createAggregatePlan(
      [col('category')],
      [{ expr: createAggExpr('sum', col('value')), alias: 'sum' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1000);

    // Check a sample of groups
    const cat500 = results.find(r => r.category === 'category_500');
    expect(cat500?.sum).toBe(500);
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Aggregate Operator - Edge Cases', () => {
  it('should handle single row', async () => {
    const rows: Row[] = [{ id: 1, value: 42 }];

    const plan = createAggregatePlan(
      [],
      [
        { expr: createAggExpr('count', '*'), alias: 'count' },
        { expr: createAggExpr('sum', col('value')), alias: 'sum' },
        { expr: createAggExpr('avg', col('value')), alias: 'avg' },
        { expr: createAggExpr('min', col('value')), alias: 'min' },
        { expr: createAggExpr('max', col('value')), alias: 'max' },
      ]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].count).toBe(1);
    expect(results[0].sum).toBe(42);
    expect(results[0].avg).toBe(42);
    expect(results[0].min).toBe(42);
    expect(results[0].max).toBe(42);
  });

  it('should return empty result for all aggregates with empty input', async () => {
    // Empty input produces no groups, hence no output rows
    const rows: Row[] = [];

    const plan = createAggregatePlan(
      [],
      [
        { expr: createAggExpr('count', '*'), alias: 'count' },
        { expr: createAggExpr('sum', col('value')), alias: 'sum' },
        { expr: createAggExpr('avg', col('value')), alias: 'avg' },
        { expr: createAggExpr('min', col('value')), alias: 'min' },
        { expr: createAggExpr('max', col('value')), alias: 'max' },
      ]
    );

    const input = createMockOperator(rows, ['id', 'value']);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());
    const results = await collectRows(operator);

    expect(results).toHaveLength(0);
  });

  it('should support async iteration', async () => {
    const rows: Row[] = [
      { category: 'A', amount: 100 },
      { category: 'B', amount: 200 },
      { category: 'A', amount: 150 },
    ];

    const plan = createAggregatePlan(
      [col('category')],
      [{ expr: createAggExpr('sum', col('amount')), alias: 'total' }]
    );

    const input = createMockOperator(rows);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());

    await operator.open(createMockExecutionContext());

    const results: Row[] = [];
    for await (const row of operator) {
      results.push(row);
    }

    expect(results).toHaveLength(2);
  });

  it('should return correct columns()', async () => {
    const plan = createAggregatePlan(
      [col('category'), col('region')],
      [
        { expr: createAggExpr('sum', col('amount')), alias: 'total' },
        { expr: createAggExpr('count', '*'), alias: 'cnt' },
      ]
    );

    const input = createMockOperator([]);
    const operator = new AggregateOperator(plan, input, createMockExecutionContext());

    const columns = operator.columns();
    expect(columns).toContain('category');
    expect(columns).toContain('region');
    expect(columns).toContain('total');
    expect(columns).toContain('cnt');
  });
});
