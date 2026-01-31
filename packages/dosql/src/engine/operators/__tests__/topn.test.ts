/**
 * TopN Operator Tests
 *
 * Tests the optimized sort-with-limit operator that uses a bounded heap
 * instead of full sorting.
 */

import { describe, it, expect, beforeEach } from 'vitest';

import { TopNOperator, shouldUseTopN } from '../topn.js';
import { SortOperator } from '../sort.js';
import { LimitOperator } from '../limit.js';
import {
  type Row,
  type Operator,
  type ExecutionContext,
  type SortPlan,
  type LimitPlan,
  col,
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

// =============================================================================
// TOPN OPERATOR TESTS
// =============================================================================

describe('TopN Operator', () => {
  describe('basic functionality', () => {
    it('should return top N rows in sorted order (ASC)', async () => {
      const inputRows: Row[] = [
        { id: 5, name: 'e' },
        { id: 3, name: 'c' },
        { id: 1, name: 'a' },
        { id: 4, name: 'd' },
        { id: 2, name: 'b' },
      ];

      const sortPlan: SortPlan = {
        id: 1,
        type: 'sort',
        input: { id: 0, type: 'scan', table: 'test', source: 'btree', columns: ['id', 'name'] },
        orderBy: [{ expr: col('id'), direction: 'asc' }],
      };

      const ctx = createMockExecutionContext();
      const operator = new TopNOperator(
        sortPlan,
        createMockOperator(inputRows),
        ctx,
        3  // limit
      );

      const results = await collectRows(operator, ctx);

      expect(results).toEqual([
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
        { id: 3, name: 'c' },
      ]);
    });

    it('should return top N rows in sorted order (DESC)', async () => {
      const inputRows: Row[] = [
        { id: 5, name: 'e' },
        { id: 3, name: 'c' },
        { id: 1, name: 'a' },
        { id: 4, name: 'd' },
        { id: 2, name: 'b' },
      ];

      const sortPlan: SortPlan = {
        id: 1,
        type: 'sort',
        input: { id: 0, type: 'scan', table: 'test', source: 'btree', columns: ['id', 'name'] },
        orderBy: [{ expr: col('id'), direction: 'desc' }],
      };

      const ctx = createMockExecutionContext();
      const operator = new TopNOperator(
        sortPlan,
        createMockOperator(inputRows),
        ctx,
        3  // limit
      );

      const results = await collectRows(operator, ctx);

      expect(results).toEqual([
        { id: 5, name: 'e' },
        { id: 4, name: 'd' },
        { id: 3, name: 'c' },
      ]);
    });

    it('should handle offset correctly', async () => {
      const inputRows: Row[] = [
        { id: 5, name: 'e' },
        { id: 3, name: 'c' },
        { id: 1, name: 'a' },
        { id: 4, name: 'd' },
        { id: 2, name: 'b' },
      ];

      const sortPlan: SortPlan = {
        id: 1,
        type: 'sort',
        input: { id: 0, type: 'scan', table: 'test', source: 'btree', columns: ['id', 'name'] },
        orderBy: [{ expr: col('id'), direction: 'asc' }],
      };

      const ctx = createMockExecutionContext();
      const operator = new TopNOperator(
        sortPlan,
        createMockOperator(inputRows),
        ctx,
        2,  // limit
        2   // offset
      );

      const results = await collectRows(operator, ctx);

      expect(results).toEqual([
        { id: 3, name: 'c' },
        { id: 4, name: 'd' },
      ]);
    });

    it('should handle empty input', async () => {
      const sortPlan: SortPlan = {
        id: 1,
        type: 'sort',
        input: { id: 0, type: 'scan', table: 'test', source: 'btree', columns: ['id'] },
        orderBy: [{ expr: col('id'), direction: 'asc' }],
      };

      const ctx = createMockExecutionContext();
      const operator = new TopNOperator(
        sortPlan,
        createMockOperator([]),
        ctx,
        10
      );

      const results = await collectRows(operator, ctx);
      expect(results).toEqual([]);
    });

    it('should handle limit larger than input', async () => {
      const inputRows: Row[] = [
        { id: 3, name: 'c' },
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
      ];

      const sortPlan: SortPlan = {
        id: 1,
        type: 'sort',
        input: { id: 0, type: 'scan', table: 'test', source: 'btree', columns: ['id', 'name'] },
        orderBy: [{ expr: col('id'), direction: 'asc' }],
      };

      const ctx = createMockExecutionContext();
      const operator = new TopNOperator(
        sortPlan,
        createMockOperator(inputRows),
        ctx,
        100  // limit much larger than input
      );

      const results = await collectRows(operator, ctx);

      expect(results).toEqual([
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
        { id: 3, name: 'c' },
      ]);
    });
  });

  describe('null handling', () => {
    it('should handle nulls with nullsFirst=true (ASC)', async () => {
      const inputRows: Row[] = [
        { id: 3, name: 'c' },
        { id: null, name: 'null1' },
        { id: 1, name: 'a' },
        { id: null, name: 'null2' },
        { id: 2, name: 'b' },
      ];

      const sortPlan: SortPlan = {
        id: 1,
        type: 'sort',
        input: { id: 0, type: 'scan', table: 'test', source: 'btree', columns: ['id', 'name'] },
        orderBy: [{ expr: col('id'), direction: 'asc', nullsFirst: true }],
      };

      const ctx = createMockExecutionContext();
      const operator = new TopNOperator(
        sortPlan,
        createMockOperator(inputRows),
        ctx,
        3
      );

      const results = await collectRows(operator, ctx);

      // Nulls should come first
      expect(results[0].id).toBe(null);
      expect(results[1].id).toBe(null);
      expect(results[2].id).toBe(1);
    });

    it('should handle nulls with nullsFirst=false (ASC)', async () => {
      const inputRows: Row[] = [
        { id: 3, name: 'c' },
        { id: null, name: 'null1' },
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
      ];

      const sortPlan: SortPlan = {
        id: 1,
        type: 'sort',
        input: { id: 0, type: 'scan', table: 'test', source: 'btree', columns: ['id', 'name'] },
        orderBy: [{ expr: col('id'), direction: 'asc', nullsFirst: false }],
      };

      const ctx = createMockExecutionContext();
      const operator = new TopNOperator(
        sortPlan,
        createMockOperator(inputRows),
        ctx,
        3
      );

      const results = await collectRows(operator, ctx);

      // Nulls should come last, so top 3 ASC should be 1, 2, 3
      expect(results).toEqual([
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
        { id: 3, name: 'c' },
      ]);
    });
  });

  describe('multi-column sort', () => {
    it('should handle multiple sort columns', async () => {
      const inputRows: Row[] = [
        { dept: 'B', salary: 50000, name: 'bob' },
        { dept: 'A', salary: 60000, name: 'alice' },
        { dept: 'A', salary: 50000, name: 'andy' },
        { dept: 'B', salary: 60000, name: 'betty' },
        { dept: 'A', salary: 55000, name: 'alan' },
      ];

      const sortPlan: SortPlan = {
        id: 1,
        type: 'sort',
        input: { id: 0, type: 'scan', table: 'test', source: 'btree', columns: ['dept', 'salary', 'name'] },
        orderBy: [
          { expr: col('dept'), direction: 'asc' },
          { expr: col('salary'), direction: 'desc' },
        ],
      };

      const ctx = createMockExecutionContext();
      const operator = new TopNOperator(
        sortPlan,
        createMockOperator(inputRows),
        ctx,
        3
      );

      const results = await collectRows(operator, ctx);

      // Sorted by dept ASC, then salary DESC
      expect(results).toEqual([
        { dept: 'A', salary: 60000, name: 'alice' },
        { dept: 'A', salary: 55000, name: 'alan' },
        { dept: 'A', salary: 50000, name: 'andy' },
      ]);
    });
  });

  describe('correctness vs Sort+Limit', () => {
    it('should produce same results as Sort+Limit', async () => {
      // Generate test data with UNIQUE ids to avoid tie-breaker instability
      const inputRows: Row[] = [];
      for (let i = 0; i < 100; i++) {
        inputRows.push({ id: i * 10, value: `val_${i}` });
      }
      // Shuffle the input
      for (let i = inputRows.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [inputRows[i], inputRows[j]] = [inputRows[j], inputRows[i]];
      }

      const sortPlan: SortPlan = {
        id: 1,
        type: 'sort',
        input: { id: 0, type: 'scan', table: 'test', source: 'btree', columns: ['id', 'value'] },
        orderBy: [{ expr: col('id'), direction: 'desc' }],
      };

      const limitPlan: LimitPlan = {
        id: 2,
        type: 'limit',
        input: sortPlan,
        limit: 10,
      };

      const ctx = createMockExecutionContext();

      // Run TopN
      const topnOperator = new TopNOperator(
        sortPlan,
        createMockOperator([...inputRows]),
        ctx,
        10
      );
      const topnResults = await collectRows(topnOperator, ctx);

      // Run Sort + Limit
      const sortOperator = new SortOperator(
        sortPlan,
        createMockOperator([...inputRows]),
        ctx
      );
      const limitOperator = new LimitOperator(
        limitPlan,
        sortOperator,
        ctx
      );
      const sortLimitResults = await collectRows(limitOperator, ctx);

      // Results should be identical
      expect(topnResults).toEqual(sortLimitResults);
    });
  });
});

describe('shouldUseTopN', () => {
  it('should return true for small limits', () => {
    expect(shouldUseTopN(10)).toBe(true);
    expect(shouldUseTopN(100)).toBe(true);
    expect(shouldUseTopN(1000)).toBe(true);
  });

  it('should return true for large limits with unknown row count', () => {
    expect(shouldUseTopN(5000)).toBe(true);
  });

  it('should return true when limit is small relative to row count', () => {
    expect(shouldUseTopN(10, 10000)).toBe(true);  // 0.1%
    expect(shouldUseTopN(100, 10000)).toBe(true); // 1%
    expect(shouldUseTopN(999, 10000)).toBe(true); // 9.99%
  });

  it('should return true even for large limits (never worse)', () => {
    // TopN is never worse than full sort, so always use it
    expect(shouldUseTopN(5000, 10000)).toBe(true);
  });
});

describe('Executor TopN Integration', () => {
  it('should use TopN when LIMIT follows SORT', async () => {
    const { createOperator } = await import('../../executor.js');

    const inputRows: Row[] = [];
    for (let i = 0; i < 50; i++) {
      inputRows.push({ id: i, value: `val_${i}` });
    }
    // Shuffle
    for (let i = inputRows.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [inputRows[i], inputRows[j]] = [inputRows[j], inputRows[i]];
    }

    // Create a mock context that returns our test data
    const ctx: ExecutionContext = {
      schema: { tables: new Map() },
      btree: {
        get: async () => undefined,
        range: async function* () {},
        scan: async function* (table: string) {
          for (const row of inputRows) {
            yield row;
          }
        },
        set: async () => {},
        delete: async () => false,
        count: async () => inputRows.length,
      },
      columnar: {
        scan: async function* () {},
        count: async () => 0,
        sum: async () => null,
        minMax: async () => ({ min: null, max: null }),
      },
    };

    // Create a LIMIT(SORT(...)) plan
    const sortPlan: SortPlan = {
      id: 1,
      type: 'sort',
      input: { id: 0, type: 'scan', table: 'test', source: 'btree', columns: ['id', 'value'] },
      orderBy: [{ expr: col('id'), direction: 'desc' }],
    };

    const limitPlan: LimitPlan = {
      id: 2,
      type: 'limit',
      input: sortPlan,
      limit: 10,
    };

    // Create operator - should use TopN internally
    const operator = createOperator(limitPlan, ctx);

    await operator.open(ctx);
    const results: Row[] = [];
    let row: Row | null;
    while ((row = await operator.next()) !== null) {
      results.push(row);
    }
    await operator.close();

    // Verify results are correct
    expect(results.length).toBe(10);
    expect(results[0].id).toBe(49);  // Highest ID first (DESC)
    expect(results[9].id).toBe(40);  // 10th highest
  });
});
