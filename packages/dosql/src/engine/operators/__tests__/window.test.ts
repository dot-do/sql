/**
 * Window Operator Tests
 *
 * Comprehensive tests for SQL window functions:
 * - ROW_NUMBER, RANK, DENSE_RANK, NTILE
 * - PERCENT_RANK, CUME_DIST
 * - LAG, LEAD
 * - FIRST_VALUE, LAST_VALUE, NTH_VALUE
 * - Window aggregate functions (SUM, AVG, COUNT, MIN, MAX)
 * - PARTITION BY and ORDER BY
 * - Frame specifications
 * - Edge cases: nulls, empty sets, single partitions
 *
 * Following the project's TDD with NO MOCKS philosophy.
 */

import { describe, it, expect } from 'vitest';

import {
  WindowOperator,
  createWindowOperator,
  containsWindowFunction,
  extractWindowFunctions,
  type WindowPlan,
  type WindowFunctionDef,
} from '../window.js';
import {
  type Row,
  type Expression,
  type Operator,
  type ExecutionContext,
  col,
  lit,
} from '../../types.js';
import { type WindowSpec, type WindowFrame } from '../../../functions/window.js';

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
 * Create a window plan
 */
function createWindowPlan(
  windowFunctions: WindowFunctionDef[]
): WindowPlan {
  return {
    type: 'window',
    id: 1,
    input: { type: 'scan', id: 0, table: 'test', source: 'btree', columns: [] },
    windowFunctions,
  };
}

/**
 * Create a window function definition
 */
function createWindowFunctionDef(
  name: string,
  args: Expression[],
  alias: string,
  windowSpec: WindowSpec
): WindowFunctionDef {
  return { name, args, alias, windowSpec };
}

// =============================================================================
// ROW_NUMBER TESTS
// =============================================================================

describe('Window Operator - ROW_NUMBER', () => {
  it('should assign sequential row numbers', async () => {
    const rows: Row[] = [
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
      { id: 3, name: 'Charlie' },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('row_number', [], 'rn', {}),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    expect(results).toHaveLength(3);
    expect(results[0].rn).toBe(1);
    expect(results[1].rn).toBe(2);
    expect(results[2].rn).toBe(3);
  });

  it('should assign row numbers within each partition', async () => {
    const rows: Row[] = [
      { dept: 'Sales', name: 'Alice' },
      { dept: 'Sales', name: 'Bob' },
      { dept: 'IT', name: 'Charlie' },
      { dept: 'IT', name: 'David' },
      { dept: 'IT', name: 'Eve' },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('row_number', [], 'rn', {
        partitionBy: ['dept'],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    expect(results).toHaveLength(5);

    // Sales partition
    const salesRows = results.filter(r => r.dept === 'Sales');
    const salesRns = salesRows.map(r => r.rn);
    expect(salesRns.sort()).toEqual([1, 2]);

    // IT partition
    const itRows = results.filter(r => r.dept === 'IT');
    const itRns = itRows.map(r => r.rn);
    expect(itRns.sort()).toEqual([1, 2, 3]);
  });

  it('should assign row numbers with ORDER BY', async () => {
    const rows: Row[] = [
      { id: 3, score: 70 },
      { id: 1, score: 90 },
      { id: 2, score: 80 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('row_number', [], 'rn', {
        orderBy: [{ column: 'score', direction: 'desc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    // Should be sorted by score descending: 90, 80, 70
    const sorted = results.sort((a, b) => (b.score as number) - (a.score as number));
    expect(sorted[0].rn).toBe(1); // score 90
    expect(sorted[1].rn).toBe(2); // score 80
    expect(sorted[2].rn).toBe(3); // score 70
  });
});

// =============================================================================
// RANK AND DENSE_RANK TESTS
// =============================================================================

describe('Window Operator - RANK', () => {
  it('should assign rank with gaps for ties', async () => {
    const rows: Row[] = [
      { id: 1, score: 90 },
      { id: 2, score: 90 },
      { id: 3, score: 80 },
      { id: 4, score: 70 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('rank', [], 'rnk', {
        orderBy: [{ column: 'score', direction: 'desc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    expect(results).toHaveLength(4);

    // Two rows with score 90 should both have rank 1
    const score90Rows = results.filter(r => r.score === 90);
    expect(score90Rows[0].rnk).toBe(1);
    expect(score90Rows[1].rnk).toBe(1);

    // Row with score 80 should have rank 3 (skipping 2)
    const score80Row = results.find(r => r.score === 80);
    expect(score80Row?.rnk).toBe(3);

    // Row with score 70 should have rank 4
    const score70Row = results.find(r => r.score === 70);
    expect(score70Row?.rnk).toBe(4);
  });

  it('should handle no ORDER BY (all rows are peers)', async () => {
    const rows: Row[] = [
      { id: 1 },
      { id: 2 },
      { id: 3 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('rank', [], 'rnk', {}),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    // Without ORDER BY, all rows are considered peers
    expect(results.every(r => r.rnk === 1)).toBe(true);
  });
});

describe('Window Operator - DENSE_RANK', () => {
  it('should assign dense rank without gaps for ties', async () => {
    const rows: Row[] = [
      { id: 1, score: 90 },
      { id: 2, score: 90 },
      { id: 3, score: 80 },
      { id: 4, score: 70 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('dense_rank', [], 'drnk', {
        orderBy: [{ column: 'score', direction: 'desc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    expect(results).toHaveLength(4);

    // Two rows with score 90 should both have dense_rank 1
    const score90Rows = results.filter(r => r.score === 90);
    expect(score90Rows[0].drnk).toBe(1);
    expect(score90Rows[1].drnk).toBe(1);

    // Row with score 80 should have dense_rank 2 (no gap)
    const score80Row = results.find(r => r.score === 80);
    expect(score80Row?.drnk).toBe(2);

    // Row with score 70 should have dense_rank 3
    const score70Row = results.find(r => r.score === 70);
    expect(score70Row?.drnk).toBe(3);
  });
});

// =============================================================================
// NTILE TESTS
// =============================================================================

describe('Window Operator - NTILE', () => {
  it('should divide rows into n buckets', async () => {
    const rows: Row[] = [
      { id: 1 },
      { id: 2 },
      { id: 3 },
      { id: 4 },
      { id: 5 },
      { id: 6 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('ntile', [lit(3)], 'bucket', {}),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    expect(results).toHaveLength(6);

    // With 6 rows and 3 buckets, each bucket should have 2 rows
    const buckets = results.map(r => r.bucket);
    expect(buckets.filter(b => b === 1)).toHaveLength(2);
    expect(buckets.filter(b => b === 2)).toHaveLength(2);
    expect(buckets.filter(b => b === 3)).toHaveLength(2);
  });

  it('should handle uneven distribution', async () => {
    const rows: Row[] = [
      { id: 1 },
      { id: 2 },
      { id: 3 },
      { id: 4 },
      { id: 5 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('ntile', [lit(3)], 'bucket', {}),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    expect(results).toHaveLength(5);

    // With 5 rows and 3 buckets: 2, 2, 1
    const buckets = results.map(r => r.bucket);
    expect(buckets.filter(b => b === 1)).toHaveLength(2);
    expect(buckets.filter(b => b === 2)).toHaveLength(2);
    expect(buckets.filter(b => b === 3)).toHaveLength(1);
  });

  it('should return null for invalid n', async () => {
    const rows: Row[] = [{ id: 1 }, { id: 2 }];

    const plan = createWindowPlan([
      createWindowFunctionDef('ntile', [lit(0)], 'bucket', {}),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    expect(results.every(r => r.bucket === null)).toBe(true);
  });
});

// =============================================================================
// PERCENT_RANK AND CUME_DIST TESTS
// =============================================================================

describe('Window Operator - PERCENT_RANK', () => {
  it('should calculate percent rank', async () => {
    const rows: Row[] = [
      { id: 1, score: 100 },
      { id: 2, score: 80 },
      { id: 3, score: 60 },
      { id: 4, score: 40 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('percent_rank', [], 'pct_rank', {
        orderBy: [{ column: 'score', direction: 'desc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    // percent_rank = (rank - 1) / (n - 1)
    const score100 = results.find(r => r.score === 100);
    const score80 = results.find(r => r.score === 80);
    const score60 = results.find(r => r.score === 60);
    const score40 = results.find(r => r.score === 40);

    expect(score100?.pct_rank).toBe(0); // (1-1)/(4-1) = 0
    expect(score80?.pct_rank).toBeCloseTo(1 / 3); // (2-1)/(4-1) = 1/3
    expect(score60?.pct_rank).toBeCloseTo(2 / 3); // (3-1)/(4-1) = 2/3
    expect(score40?.pct_rank).toBe(1); // (4-1)/(4-1) = 1
  });

  it('should return 0 for single row', async () => {
    const rows: Row[] = [{ id: 1, score: 100 }];

    const plan = createWindowPlan([
      createWindowFunctionDef('percent_rank', [], 'pct_rank', {
        orderBy: [{ column: 'score', direction: 'desc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    expect(results[0].pct_rank).toBe(0);
  });
});

describe('Window Operator - CUME_DIST', () => {
  it('should calculate cumulative distribution', async () => {
    const rows: Row[] = [
      { id: 1, score: 100 },
      { id: 2, score: 80 },
      { id: 3, score: 60 },
      { id: 4, score: 40 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('cume_dist', [], 'cume', {
        orderBy: [{ column: 'score', direction: 'desc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    // cume_dist = rows with value <= current / total rows
    const score100 = results.find(r => r.score === 100);
    const score40 = results.find(r => r.score === 40);

    expect(score100?.cume).toBe(0.25); // 1/4
    expect(score40?.cume).toBe(1); // 4/4
  });
});

// =============================================================================
// LAG AND LEAD TESTS
// =============================================================================

describe('Window Operator - LAG', () => {
  it('should access previous row value', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: 20 },
      { id: 3, value: 30 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('lag', [col('value')], 'prev_value', {
        orderBy: [{ column: 'id', direction: 'asc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    // Sort by id to verify
    const sorted = results.sort((a, b) => (a.id as number) - (b.id as number));

    expect(sorted[0].prev_value).toBeNull(); // No previous for first row
    expect(sorted[1].prev_value).toBe(10);
    expect(sorted[2].prev_value).toBe(20);
  });

  it('should use offset parameter', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: 20 },
      { id: 3, value: 30 },
      { id: 4, value: 40 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('lag', [col('value'), lit(2)], 'prev2', {
        orderBy: [{ column: 'id', direction: 'asc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    const sorted = results.sort((a, b) => (a.id as number) - (b.id as number));

    expect(sorted[0].prev2).toBeNull();
    expect(sorted[1].prev2).toBeNull();
    expect(sorted[2].prev2).toBe(10);
    expect(sorted[3].prev2).toBe(20);
  });

  it('should use default value parameter', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: 20 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('lag', [col('value'), lit(1), lit(-1)], 'prev_value', {
        orderBy: [{ column: 'id', direction: 'asc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    const sorted = results.sort((a, b) => (a.id as number) - (b.id as number));

    expect(sorted[0].prev_value).toBe(-1); // Default value
    expect(sorted[1].prev_value).toBe(10);
  });
});

describe('Window Operator - LEAD', () => {
  it('should access next row value', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: 20 },
      { id: 3, value: 30 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('lead', [col('value')], 'next_value', {
        orderBy: [{ column: 'id', direction: 'asc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    const sorted = results.sort((a, b) => (a.id as number) - (b.id as number));

    expect(sorted[0].next_value).toBe(20);
    expect(sorted[1].next_value).toBe(30);
    expect(sorted[2].next_value).toBeNull(); // No next for last row
  });

  it('should use offset and default parameters', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: 20 },
      { id: 3, value: 30 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('lead', [col('value'), lit(2), lit(999)], 'next2', {
        orderBy: [{ column: 'id', direction: 'asc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    const sorted = results.sort((a, b) => (a.id as number) - (b.id as number));

    expect(sorted[0].next2).toBe(30);
    expect(sorted[1].next2).toBe(999); // Default
    expect(sorted[2].next2).toBe(999); // Default
  });
});

// =============================================================================
// FIRST_VALUE, LAST_VALUE, NTH_VALUE TESTS
// =============================================================================

describe('Window Operator - FIRST_VALUE', () => {
  it('should return first value in frame', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: 20 },
      { id: 3, value: 30 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('first_value', [col('value')], 'first', {
        orderBy: [{ column: 'id', direction: 'asc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    // With default frame (RANGE UNBOUNDED PRECEDING to CURRENT ROW)
    // first_value should always be 10
    expect(results.every(r => r.first === 10)).toBe(true);
  });

  it('should return first value per partition', async () => {
    const rows: Row[] = [
      { dept: 'A', id: 1, value: 100 },
      { dept: 'A', id: 2, value: 200 },
      { dept: 'B', id: 3, value: 300 },
      { dept: 'B', id: 4, value: 400 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('first_value', [col('value')], 'first', {
        partitionBy: ['dept'],
        orderBy: [{ column: 'id', direction: 'asc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    const deptA = results.filter(r => r.dept === 'A');
    const deptB = results.filter(r => r.dept === 'B');

    expect(deptA.every(r => r.first === 100)).toBe(true);
    expect(deptB.every(r => r.first === 300)).toBe(true);
  });
});

describe('Window Operator - LAST_VALUE', () => {
  it('should return last value in frame', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: 20 },
      { id: 3, value: 30 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('last_value', [col('value')], 'last', {
        orderBy: [{ column: 'id', direction: 'asc' }],
        frame: {
          mode: 'rows',
          start: { type: 'unboundedPreceding' },
          end: { type: 'unboundedFollowing' },
        },
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    // With ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    // last_value should always be 30
    expect(results.every(r => r.last === 30)).toBe(true);
  });
});

describe('Window Operator - NTH_VALUE', () => {
  it('should return nth value in frame', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: 20 },
      { id: 3, value: 30 },
      { id: 4, value: 40 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('nth_value', [col('value'), lit(2)], 'second', {
        orderBy: [{ column: 'id', direction: 'asc' }],
        frame: {
          mode: 'rows',
          start: { type: 'unboundedPreceding' },
          end: { type: 'unboundedFollowing' },
        },
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    // 2nd value should be 20 for all rows
    expect(results.every(r => r.second === 20)).toBe(true);
  });

  it('should return null for invalid n', async () => {
    const rows: Row[] = [{ id: 1, value: 10 }];

    const plan = createWindowPlan([
      createWindowFunctionDef('nth_value', [col('value'), lit(0)], 'nth', {
        orderBy: [{ column: 'id', direction: 'asc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    expect(results[0].nth).toBeNull();
  });
});

// =============================================================================
// WINDOW AGGREGATE FUNCTIONS TESTS
// =============================================================================

describe('Window Operator - Aggregate Functions', () => {
  it('should calculate running SUM', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: 20 },
      { id: 3, value: 30 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('sum', [col('value')], 'running_sum', {
        orderBy: [{ column: 'id', direction: 'asc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    const sorted = results.sort((a, b) => (a.id as number) - (b.id as number));

    expect(sorted[0].running_sum).toBe(10);
    expect(sorted[1].running_sum).toBe(30);
    expect(sorted[2].running_sum).toBe(60);
  });

  it('should calculate running AVG', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: 20 },
      { id: 3, value: 30 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('avg', [col('value')], 'running_avg', {
        orderBy: [{ column: 'id', direction: 'asc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    const sorted = results.sort((a, b) => (a.id as number) - (b.id as number));

    expect(sorted[0].running_avg).toBe(10);
    expect(sorted[1].running_avg).toBe(15);
    expect(sorted[2].running_avg).toBe(20);
  });

  it('should calculate running COUNT', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: null },
      { id: 3, value: 30 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('count', [col('value')], 'running_count', {
        orderBy: [{ column: 'id', direction: 'asc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    const sorted = results.sort((a, b) => (a.id as number) - (b.id as number));

    expect(sorted[0].running_count).toBe(1);
    expect(sorted[1].running_count).toBe(1); // null not counted
    expect(sorted[2].running_count).toBe(2);
  });

  it('should calculate running MIN and MAX', async () => {
    const rows: Row[] = [
      { id: 1, value: 50 },
      { id: 2, value: 10 },
      { id: 3, value: 80 },
      { id: 4, value: 30 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('min', [col('value')], 'running_min', {
        orderBy: [{ column: 'id', direction: 'asc' }],
      }),
      createWindowFunctionDef('max', [col('value')], 'running_max', {
        orderBy: [{ column: 'id', direction: 'asc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    const sorted = results.sort((a, b) => (a.id as number) - (b.id as number));

    expect(sorted[0].running_min).toBe(50);
    expect(sorted[0].running_max).toBe(50);

    expect(sorted[1].running_min).toBe(10);
    expect(sorted[1].running_max).toBe(50);

    expect(sorted[2].running_min).toBe(10);
    expect(sorted[2].running_max).toBe(80);

    expect(sorted[3].running_min).toBe(10);
    expect(sorted[3].running_max).toBe(80);
  });
});

// =============================================================================
// FRAME SPECIFICATION TESTS
// =============================================================================

describe('Window Operator - Frame Specifications', () => {
  it('should handle ROWS BETWEEN N PRECEDING AND N FOLLOWING', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: 20 },
      { id: 3, value: 30 },
      { id: 4, value: 40 },
      { id: 5, value: 50 },
    ];

    const frame: WindowFrame = {
      mode: 'rows',
      start: { type: 'preceding', offset: 1 },
      end: { type: 'following', offset: 1 },
    };

    const plan = createWindowPlan([
      createWindowFunctionDef('sum', [col('value')], 'window_sum', {
        orderBy: [{ column: 'id', direction: 'asc' }],
        frame,
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    const sorted = results.sort((a, b) => (a.id as number) - (b.id as number));

    // Row 1: 10 + 20 = 30 (no preceding)
    expect(sorted[0].window_sum).toBe(30);

    // Row 2: 10 + 20 + 30 = 60
    expect(sorted[1].window_sum).toBe(60);

    // Row 3: 20 + 30 + 40 = 90
    expect(sorted[2].window_sum).toBe(90);

    // Row 4: 30 + 40 + 50 = 120
    expect(sorted[3].window_sum).toBe(120);

    // Row 5: 40 + 50 = 90 (no following)
    expect(sorted[4].window_sum).toBe(90);
  });

  it('should handle ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: 20 },
      { id: 3, value: 30 },
    ];

    const frame: WindowFrame = {
      mode: 'rows',
      start: { type: 'currentRow' },
      end: { type: 'unboundedFollowing' },
    };

    const plan = createWindowPlan([
      createWindowFunctionDef('sum', [col('value')], 'sum_following', {
        orderBy: [{ column: 'id', direction: 'asc' }],
        frame,
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    const sorted = results.sort((a, b) => (a.id as number) - (b.id as number));

    expect(sorted[0].sum_following).toBe(60); // 10 + 20 + 30
    expect(sorted[1].sum_following).toBe(50); // 20 + 30
    expect(sorted[2].sum_following).toBe(30); // 30
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Window Operator - Edge Cases', () => {
  it('should handle empty input', async () => {
    const rows: Row[] = [];

    const plan = createWindowPlan([
      createWindowFunctionDef('row_number', [], 'rn', {}),
    ]);

    const input = createMockOperator(rows, ['id']);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    expect(results).toHaveLength(0);
  });

  it('should handle single row', async () => {
    const rows: Row[] = [{ id: 1, value: 100 }];

    const plan = createWindowPlan([
      createWindowFunctionDef('row_number', [], 'rn', {}),
      createWindowFunctionDef('rank', [], 'rnk', { orderBy: [{ column: 'id', direction: 'asc' }] }),
      createWindowFunctionDef('sum', [col('value')], 'sum', { orderBy: [{ column: 'id', direction: 'asc' }] }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0].rn).toBe(1);
    expect(results[0].rnk).toBe(1);
    expect(results[0].sum).toBe(100);
  });

  it('should handle null values in partition column', async () => {
    const rows: Row[] = [
      { dept: 'A', value: 10 },
      { dept: null, value: 20 },
      { dept: 'A', value: 30 },
      { dept: null, value: 40 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('row_number', [], 'rn', {
        partitionBy: ['dept'],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    expect(results).toHaveLength(4);

    // Rows with dept 'A' should have rn 1, 2
    const deptA = results.filter(r => r.dept === 'A');
    expect(deptA.map(r => r.rn).sort()).toEqual([1, 2]);

    // Rows with dept null should also have rn 1, 2
    const deptNull = results.filter(r => r.dept === null);
    expect(deptNull.map(r => r.rn).sort()).toEqual([1, 2]);
  });

  it('should handle null values in ORDER BY column', async () => {
    const rows: Row[] = [
      { id: 1, score: null },
      { id: 2, score: 80 },
      { id: 3, score: null },
      { id: 4, score: 90 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('row_number', [], 'rn', {
        orderBy: [{ column: 'score', direction: 'asc' }],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    // All rows should have row numbers 1-4
    const rns = results.map(r => r.rn as number).sort((a, b) => a - b);
    expect(rns).toEqual([1, 2, 3, 4]);
  });

  it('should preserve original row data', async () => {
    const rows: Row[] = [
      { id: 1, name: 'Alice', dept: 'IT' },
      { id: 2, name: 'Bob', dept: 'IT' },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('row_number', [], 'rn', {}),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    expect(results).toHaveLength(2);
    expect(results.some(r => r.id === 1 && r.name === 'Alice' && r.dept === 'IT')).toBe(true);
    expect(results.some(r => r.id === 2 && r.name === 'Bob' && r.dept === 'IT')).toBe(true);
  });

  it('should support multiple window functions with different specs', async () => {
    const rows: Row[] = [
      { dept: 'A', id: 1, value: 100 },
      { dept: 'A', id: 2, value: 200 },
      { dept: 'B', id: 3, value: 300 },
    ];

    const plan = createWindowPlan([
      createWindowFunctionDef('row_number', [], 'global_rn', {}),
      createWindowFunctionDef('row_number', [], 'dept_rn', {
        partitionBy: ['dept'],
      }),
    ]);

    const input = createMockOperator(rows);
    const operator = createWindowOperator(plan, input);
    const results = await collectRows(operator);

    expect(results).toHaveLength(3);

    // Global row numbers: 1, 2, 3
    const globalRns = results.map(r => r.global_rn as number).sort((a, b) => a - b);
    expect(globalRns).toEqual([1, 2, 3]);

    // Department row numbers
    const deptA = results.filter(r => r.dept === 'A');
    expect(deptA.map(r => r.dept_rn).sort()).toEqual([1, 2]);

    const deptB = results.filter(r => r.dept === 'B');
    expect(deptB.map(r => r.dept_rn)).toEqual([1]);
  });
});

// =============================================================================
// UTILITY FUNCTION TESTS
// =============================================================================

describe('Window Operator - Utility Functions', () => {
  it('containsWindowFunction should detect window functions', () => {
    const exprWithWindow: Expression = {
      type: 'function',
      name: 'row_number',
      args: [],
      over: {},
    } as Expression;

    const exprWithoutWindow: Expression = {
      type: 'function',
      name: 'upper',
      args: [col('name')],
    };

    expect(containsWindowFunction(exprWithWindow)).toBe(true);
    expect(containsWindowFunction(exprWithoutWindow)).toBe(false);
    expect(containsWindowFunction(col('id'))).toBe(false);
    expect(containsWindowFunction(lit(42))).toBe(false);
  });

  it('extractWindowFunctions should extract window function definitions', () => {
    const expressions: { expr: Expression; alias: string }[] = [
      {
        expr: {
          type: 'function',
          name: 'row_number',
          args: [],
          over: { partitionBy: ['dept'] },
        } as Expression,
        alias: 'rn',
      },
      {
        expr: col('name'),
        alias: 'name',
      },
      {
        expr: {
          type: 'function',
          name: 'sum',
          args: [col('value')],
          over: { orderBy: [{ column: 'id', direction: 'asc' }] },
        } as Expression,
        alias: 'running_sum',
      },
    ];

    const windowFunctions = extractWindowFunctions(expressions);

    expect(windowFunctions).toHaveLength(2);
    expect(windowFunctions[0].name).toBe('row_number');
    expect(windowFunctions[0].alias).toBe('rn');
    expect(windowFunctions[1].name).toBe('sum');
    expect(windowFunctions[1].alias).toBe('running_sum');
  });

  it('should return correct columns from operator', () => {
    const plan = createWindowPlan([
      createWindowFunctionDef('row_number', [], 'rn', {}),
      createWindowFunctionDef('rank', [], 'rnk', {}),
    ]);

    const input = createMockOperator([], ['id', 'name']);
    const operator = createWindowOperator(plan, input);

    const columns = operator.columns();
    expect(columns).toContain('id');
    expect(columns).toContain('name');
    expect(columns).toContain('rn');
    expect(columns).toContain('rnk');
  });
});
