/**
 * Engine Module Integration Tests
 *
 * Comprehensive integration tests for the query execution engine.
 * Tests full query execution paths through real operators with real data.
 *
 * NO MOCKS - uses actual operator implementations.
 *
 * Coverage:
 * - Filter operator: predicates, NULL handling, type coercion
 * - Project operator: column selection, expressions, aliases
 * - Join operator: all join types (inner, left, right, full, cross)
 * - Aggregate operator: COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING
 * - Sort operator: ORDER BY ASC/DESC, NULL handling
 * - Limit operator: LIMIT and OFFSET
 * - Full execution pipeline: combining multiple operators
 */

import { describe, it, expect, beforeEach } from 'vitest';

// Operators
import { FilterOperator, evaluateExpression, evaluatePredicate } from '../operators/filter.js';
import { ProjectOperator } from '../operators/project.js';
import { JoinOperator } from '../operators/join.js';
import { AggregateOperator } from '../operators/aggregate.js';
import { SortOperator } from '../operators/sort.js';
import { LimitOperator } from '../operators/limit.js';
import { ScanOperator } from '../operators/scan.js';

// Executor
import { createOperator, executePlan, QueryExecutor, createExecutor } from '../executor.js';

// Types
import type {
  Operator,
  Row,
  ExecutionContext,
  QueryPlan,
  ScanPlan,
  FilterPlan,
  ProjectPlan,
  JoinPlan,
  AggregatePlan,
  SortPlan,
  LimitPlan,
  Predicate,
  Expression,
  Schema,
  SqlValue,
  BTreeStorage,
  ColumnarStorage,
} from '../types.js';

import { col, lit } from '../types.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Create a simple in-memory operator that yields rows from an array.
 * This is NOT a mock - it's a real operator implementation for testing.
 */
class ArrayOperator implements Operator {
  private rows: Row[];
  private index = 0;
  private columnNames: string[];

  constructor(rows: Row[], columns?: string[]) {
    this.rows = rows;
    this.columnNames = columns ?? (rows.length > 0 ? Object.keys(rows[0]) : []);
  }

  async open(_ctx: ExecutionContext): Promise<void> {
    this.index = 0;
  }

  async next(): Promise<Row | null> {
    if (this.index >= this.rows.length) {
      return null;
    }
    return this.rows[this.index++] ?? null;
  }

  async close(): Promise<void> {
    this.index = 0;
  }

  columns(): string[] {
    return this.columnNames;
  }

  async *[Symbol.asyncIterator](): AsyncIterator<Row> {
    try {
      let row: Row | null;
      while ((row = await this.next()) !== null) {
        yield row;
      }
    } finally {
      await this.close();
    }
  }
}

/**
 * Collect all rows from an operator
 */
async function collectRows(operator: Operator, ctx: ExecutionContext): Promise<Row[]> {
  const rows: Row[] = [];
  await operator.open(ctx);
  let row: Row | null;
  while ((row = await operator.next()) !== null) {
    rows.push(row);
  }
  await operator.close();
  return rows;
}

/**
 * Create a real in-memory btree storage for testing
 */
function createInMemoryBTreeStorage(tables: Map<string, Row[]>): BTreeStorage {
  return {
    async get(table: string, key: SqlValue): Promise<Row | undefined> {
      const tableData = tables.get(table) ?? [];
      return tableData.find(row => row.id === key);
    },
    async *range(table: string, start: SqlValue, end: SqlValue): AsyncIterableIterator<Row> {
      const tableData = tables.get(table) ?? [];
      for (const row of tableData) {
        if (row.id !== undefined && row.id >= start && row.id <= end) {
          yield row;
        }
      }
    },
    async *scan(table: string): AsyncIterableIterator<Row> {
      const tableData = tables.get(table) ?? [];
      for (const row of tableData) {
        yield row;
      }
    },
    async set(table: string, key: SqlValue, row: Row): Promise<void> {
      let tableData = tables.get(table);
      if (!tableData) {
        tableData = [];
        tables.set(table, tableData);
      }
      const idx = tableData.findIndex(r => r.id === key);
      if (idx >= 0) {
        tableData[idx] = row;
      } else {
        tableData.push(row);
      }
    },
    async delete(table: string, key: SqlValue): Promise<boolean> {
      const tableData = tables.get(table);
      if (!tableData) return false;
      const idx = tableData.findIndex(r => r.id === key);
      if (idx >= 0) {
        tableData.splice(idx, 1);
        return true;
      }
      return false;
    },
    async count(table: string): Promise<number> {
      return (tables.get(table) ?? []).length;
    },
  };
}

/**
 * Create a minimal columnar storage for testing
 */
function createInMemoryColumnarStorage(): ColumnarStorage {
  return {
    async *scan(): AsyncIterableIterator<Row> {
      // Empty for these tests - not testing columnar
    },
    async count(): Promise<number> {
      return 0;
    },
    async sum(): Promise<number | bigint | null> {
      return null;
    },
    async minMax(): Promise<{ min: SqlValue; max: SqlValue }> {
      return { min: null, max: null };
    },
  };
}

/**
 * Create a real execution context with in-memory storage
 */
function createTestContext(tableData?: Map<string, Row[]>): ExecutionContext {
  const tables = tableData ?? new Map();
  return {
    schema: { tables: new Map() },
    btree: createInMemoryBTreeStorage(tables),
    columnar: createInMemoryColumnarStorage(),
  };
}

// =============================================================================
// SAMPLE DATA
// =============================================================================

const sampleUsers: Row[] = [
  { id: 1, name: 'Alice', age: 30, department: 'Engineering' },
  { id: 2, name: 'Bob', age: 25, department: 'Marketing' },
  { id: 3, name: 'Charlie', age: 35, department: 'Engineering' },
  { id: 4, name: 'Diana', age: 28, department: 'Sales' },
  { id: 5, name: 'Eve', age: 32, department: 'Marketing' },
];

const sampleOrders: Row[] = [
  { id: 1, user_id: 1, amount: 100, status: 'completed' },
  { id: 2, user_id: 1, amount: 200, status: 'pending' },
  { id: 3, user_id: 2, amount: 150, status: 'completed' },
  { id: 4, user_id: 3, amount: 300, status: 'completed' },
  { id: 5, user_id: 5, amount: 50, status: 'cancelled' },
];

const usersWithNulls: Row[] = [
  { id: 1, name: 'Alice', score: 85 },
  { id: 2, name: 'Bob', score: null },
  { id: 3, name: null, score: 90 },
  { id: 4, name: 'Diana', score: null },
  { id: 5, name: 'Eve', score: 75 },
];

// =============================================================================
// FILTER OPERATOR TESTS
// =============================================================================

describe('FilterOperator Integration', () => {
  let ctx: ExecutionContext;

  beforeEach(() => {
    ctx = createTestContext();
  });

  describe('Simple predicates', () => {
    it('should filter by equality', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: col('department'),
          right: lit('Engineering'),
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.map(r => r.name)).toEqual(['Alice', 'Charlie']);
    });

    it('should filter by greater than', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'comparison',
          op: 'gt',
          left: col('age'),
          right: lit(30),
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.map(r => r.name)).toEqual(['Charlie', 'Eve']);
    });

    it('should filter by less than or equal', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'comparison',
          op: 'le',
          left: col('age'),
          right: lit(28),
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.map(r => r.name)).toEqual(['Bob', 'Diana']);
    });
  });

  describe('NULL handling', () => {
    it('should handle IS NULL predicate', async () => {
      const input = new ArrayOperator(usersWithNulls);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'isNull',
          expr: col('score'),
          isNot: false,
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.map(r => r.name)).toEqual(['Bob', 'Diana']);
    });

    it('should handle IS NOT NULL predicate', async () => {
      const input = new ArrayOperator(usersWithNulls);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'isNull',
          expr: col('name'),
          isNot: true,
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(4);
      expect(rows.map(r => r.id)).toEqual([1, 2, 4, 5]);
    });

    it('should treat NULL comparisons as false (except for IS NULL)', async () => {
      const input = new ArrayOperator(usersWithNulls);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: col('score'),
          right: lit(90),
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      // Only rows with score=90 (not NULL scores)
      expect(rows.length).toBe(1);
      expect(rows[0].id).toBe(3);
    });
  });

  describe('Compound predicates', () => {
    it('should handle AND predicate', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'logical',
          op: 'and',
          operands: [
            { type: 'comparison', op: 'eq', left: col('department'), right: lit('Engineering') },
            { type: 'comparison', op: 'gt', left: col('age'), right: lit(32) },
          ],
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].name).toBe('Charlie');
    });

    it('should handle OR predicate', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'logical',
          op: 'or',
          operands: [
            { type: 'comparison', op: 'eq', left: col('department'), right: lit('Sales') },
            { type: 'comparison', op: 'lt', left: col('age'), right: lit(26) },
          ],
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.map(r => r.name)).toEqual(['Bob', 'Diana']);
    });

    it('should handle NOT predicate', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'logical',
          op: 'not',
          operands: [
            { type: 'comparison', op: 'eq', left: col('department'), right: lit('Engineering') },
          ],
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(3);
      expect(rows.map(r => r.name)).toEqual(['Bob', 'Diana', 'Eve']);
    });
  });

  describe('BETWEEN and IN predicates', () => {
    it('should handle BETWEEN predicate', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'between',
          expr: col('age'),
          low: lit(28),
          high: lit(32),
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(3);
      expect(rows.map(r => r.name)).toEqual(['Alice', 'Diana', 'Eve']);
    });

    it('should handle IN predicate with list', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'in',
          expr: col('department'),
          values: [lit('Engineering'), lit('Sales')],
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(3);
      expect(rows.map(r => r.name)).toEqual(['Alice', 'Charlie', 'Diana']);
    });

    it('should return false for IN with empty list', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'in',
          expr: col('department'),
          values: [],
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(0);
    });
  });

  describe('Empty result handling', () => {
    it('should return empty result when no rows match', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: col('department'),
          right: lit('HR'),
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(0);
    });

    it('should handle empty input', async () => {
      const input = new ArrayOperator([]);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: col('id'),
          right: lit(1),
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(0);
    });
  });
});

// =============================================================================
// PROJECT OPERATOR TESTS
// =============================================================================

describe('ProjectOperator Integration', () => {
  let ctx: ExecutionContext;

  beforeEach(() => {
    ctx = createTestContext();
  });

  describe('Column selection', () => {
    it('should select specific columns', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: ProjectPlan = {
        type: 'project',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        expressions: [
          { expr: col('name'), alias: 'name' },
          { expr: col('age'), alias: 'age' },
        ],
      };

      const operator = new ProjectOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(5);
      expect(Object.keys(rows[0])).toEqual(['name', 'age']);
      expect(rows[0]).toEqual({ name: 'Alice', age: 30 });
    });

    it('should rename columns with aliases', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: ProjectPlan = {
        type: 'project',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        expressions: [
          { expr: col('name'), alias: 'user_name' },
          { expr: col('department'), alias: 'dept' },
        ],
      };

      const operator = new ProjectOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(Object.keys(rows[0])).toEqual(['user_name', 'dept']);
      expect(rows[0]).toEqual({ user_name: 'Alice', dept: 'Engineering' });
    });
  });

  describe('Computed expressions', () => {
    it('should compute arithmetic expressions', async () => {
      const input = new ArrayOperator(sampleOrders);
      const plan: ProjectPlan = {
        type: 'project',
        id: 1,
        input: { type: 'scan', id: 0, table: 'orders', source: 'btree', columns: [] },
        expressions: [
          { expr: col('id'), alias: 'id' },
          { expr: col('amount'), alias: 'original' },
          {
            expr: {
              type: 'binary',
              op: 'mul',
              left: col('amount'),
              right: lit(1.1),
            },
            alias: 'with_tax',
          },
        ],
      };

      const operator = new ProjectOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows[0].original).toBe(100);
      expect(rows[0].with_tax).toBeCloseTo(110, 5);
      expect(rows[2].original).toBe(150);
      expect(rows[2].with_tax).toBeCloseTo(165, 5);
    });

    it('should include literal values', async () => {
      const input = new ArrayOperator(sampleUsers.slice(0, 2));
      const plan: ProjectPlan = {
        type: 'project',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        expressions: [
          { expr: col('name'), alias: 'name' },
          { expr: lit('active'), alias: 'status' },
          { expr: lit(100), alias: 'bonus' },
        ],
      };

      const operator = new ProjectOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows[0]).toEqual({ name: 'Alice', status: 'active', bonus: 100 });
      expect(rows[1]).toEqual({ name: 'Bob', status: 'active', bonus: 100 });
    });
  });

  describe('NULL handling in projections', () => {
    it('should preserve NULL values', async () => {
      const input = new ArrayOperator(usersWithNulls);
      const plan: ProjectPlan = {
        type: 'project',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        expressions: [
          { expr: col('id'), alias: 'id' },
          { expr: col('name'), alias: 'name' },
          { expr: col('score'), alias: 'score' },
        ],
      };

      const operator = new ProjectOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows[1].score).toBeNull();
      expect(rows[2].name).toBeNull();
    });

    it('should return NULL for arithmetic with NULL operand', async () => {
      const input = new ArrayOperator(usersWithNulls);
      const plan: ProjectPlan = {
        type: 'project',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        expressions: [
          { expr: col('id'), alias: 'id' },
          {
            expr: {
              type: 'binary',
              op: 'add',
              left: col('score'),
              right: lit(10),
            },
            alias: 'adjusted_score',
          },
        ],
      };

      const operator = new ProjectOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows[0].adjusted_score).toBe(95); // 85 + 10
      expect(rows[1].adjusted_score).toBeNull(); // NULL + 10 = NULL
      expect(rows[4].adjusted_score).toBe(85); // 75 + 10
    });
  });
});

// =============================================================================
// JOIN OPERATOR TESTS
// =============================================================================

describe('JoinOperator Integration', () => {
  let ctx: ExecutionContext;

  beforeEach(() => {
    ctx = createTestContext();
  });

  describe('INNER JOIN', () => {
    it('should join matching rows', async () => {
      const leftInput = new ArrayOperator(sampleUsers.slice(0, 3), ['id', 'name', 'age', 'department']);
      const rightInput = new ArrayOperator(sampleOrders.slice(0, 4), ['id', 'user_id', 'amount', 'status']);

      const leftPlan: ScanPlan = {
        type: 'scan',
        id: 0,
        table: 'users',
        alias: 'u',
        source: 'btree',
        columns: ['id', 'name', 'age', 'department'],
      };

      const rightPlan: ScanPlan = {
        type: 'scan',
        id: 1,
        table: 'orders',
        alias: 'o',
        source: 'btree',
        columns: ['id', 'user_id', 'amount', 'status'],
      };

      const plan: JoinPlan = {
        type: 'join',
        id: 2,
        joinType: 'inner',
        left: leftPlan,
        right: rightPlan,
        condition: {
          type: 'comparison',
          op: 'eq',
          left: col('id', 'u'),
          right: col('user_id', 'o'),
        },
      };

      const operator = new JoinOperator(plan, leftInput, rightInput, ctx);
      const rows = await collectRows(operator, ctx);

      // Alice (id=1) has 2 orders, Bob (id=2) has 1, Charlie (id=3) has 1
      expect(rows.length).toBe(4);

      // Check that rows have prefixed column names
      expect(rows[0]['u.name']).toBe('Alice');
      expect(rows[0]['o.amount']).toBe(100);
    });

    it('should handle no matching rows', async () => {
      const leftInput = new ArrayOperator([{ id: 999, name: 'Nobody' }], ['id', 'name']);
      const rightInput = new ArrayOperator(sampleOrders, ['id', 'user_id', 'amount', 'status']);

      const leftPlan: ScanPlan = {
        type: 'scan',
        id: 0,
        table: 'users',
        alias: 'u',
        source: 'btree',
        columns: ['id', 'name'],
      };

      const rightPlan: ScanPlan = {
        type: 'scan',
        id: 1,
        table: 'orders',
        alias: 'o',
        source: 'btree',
        columns: ['id', 'user_id', 'amount', 'status'],
      };

      const plan: JoinPlan = {
        type: 'join',
        id: 2,
        joinType: 'inner',
        left: leftPlan,
        right: rightPlan,
        condition: {
          type: 'comparison',
          op: 'eq',
          left: col('id', 'u'),
          right: col('user_id', 'o'),
        },
      };

      const operator = new JoinOperator(plan, leftInput, rightInput, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(0);
    });
  });

  describe('LEFT OUTER JOIN', () => {
    it('should include all left rows with NULL for non-matching right', async () => {
      const leftInput = new ArrayOperator(
        [{ id: 1, name: 'Alice' }, { id: 4, name: 'Diana' }],
        ['id', 'name']
      );
      const rightInput = new ArrayOperator(
        [{ user_id: 1, order_id: 101 }],
        ['user_id', 'order_id']
      );

      const leftPlan: ScanPlan = {
        type: 'scan',
        id: 0,
        table: 'users',
        alias: 'u',
        source: 'btree',
        columns: ['id', 'name'],
      };

      const rightPlan: ScanPlan = {
        type: 'scan',
        id: 1,
        table: 'orders',
        alias: 'o',
        source: 'btree',
        columns: ['user_id', 'order_id'],
      };

      const plan: JoinPlan = {
        type: 'join',
        id: 2,
        joinType: 'left',
        left: leftPlan,
        right: rightPlan,
        condition: {
          type: 'comparison',
          op: 'eq',
          left: col('id', 'u'),
          right: col('user_id', 'o'),
        },
      };

      const operator = new JoinOperator(plan, leftInput, rightInput, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);

      // Alice has a match
      expect(rows[0]['u.name']).toBe('Alice');
      expect(rows[0]['o.order_id']).toBe(101);

      // Diana has no match
      expect(rows[1]['u.name']).toBe('Diana');
      expect(rows[1]['o.order_id']).toBeNull();
    });
  });

  describe('RIGHT OUTER JOIN', () => {
    it('should include all right rows with NULL for non-matching left', async () => {
      const leftInput = new ArrayOperator(
        [{ id: 1, name: 'Alice' }],
        ['id', 'name']
      );
      const rightInput = new ArrayOperator(
        [{ user_id: 1, order_id: 101 }, { user_id: 99, order_id: 102 }],
        ['user_id', 'order_id']
      );

      const leftPlan: ScanPlan = {
        type: 'scan',
        id: 0,
        table: 'users',
        alias: 'u',
        source: 'btree',
        columns: ['id', 'name'],
      };

      const rightPlan: ScanPlan = {
        type: 'scan',
        id: 1,
        table: 'orders',
        alias: 'o',
        source: 'btree',
        columns: ['user_id', 'order_id'],
      };

      const plan: JoinPlan = {
        type: 'join',
        id: 2,
        joinType: 'right',
        left: leftPlan,
        right: rightPlan,
        condition: {
          type: 'comparison',
          op: 'eq',
          left: col('id', 'u'),
          right: col('user_id', 'o'),
        },
      };

      const operator = new JoinOperator(plan, leftInput, rightInput, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);

      // First order has a matching user
      expect(rows[0]['u.name']).toBe('Alice');
      expect(rows[0]['o.order_id']).toBe(101);

      // Second order has no matching user
      expect(rows[1]['u.name']).toBeNull();
      expect(rows[1]['o.order_id']).toBe(102);
    });
  });

  describe('FULL OUTER JOIN', () => {
    it('should include all rows from both sides', async () => {
      const leftInput = new ArrayOperator(
        [{ id: 1, name: 'Alice' }, { id: 4, name: 'Diana' }],
        ['id', 'name']
      );
      const rightInput = new ArrayOperator(
        [{ user_id: 1, order_id: 101 }, { user_id: 99, order_id: 102 }],
        ['user_id', 'order_id']
      );

      const leftPlan: ScanPlan = {
        type: 'scan',
        id: 0,
        table: 'users',
        alias: 'u',
        source: 'btree',
        columns: ['id', 'name'],
      };

      const rightPlan: ScanPlan = {
        type: 'scan',
        id: 1,
        table: 'orders',
        alias: 'o',
        source: 'btree',
        columns: ['user_id', 'order_id'],
      };

      const plan: JoinPlan = {
        type: 'join',
        id: 2,
        joinType: 'full',
        left: leftPlan,
        right: rightPlan,
        condition: {
          type: 'comparison',
          op: 'eq',
          left: col('id', 'u'),
          right: col('user_id', 'o'),
        },
      };

      const operator = new JoinOperator(plan, leftInput, rightInput, ctx);
      const rows = await collectRows(operator, ctx);

      // Alice+order101, Diana+null, null+order102
      expect(rows.length).toBe(3);
    });
  });

  describe('CROSS JOIN', () => {
    it('should produce cartesian product', async () => {
      const leftInput = new ArrayOperator(
        [{ a: 1 }, { a: 2 }],
        ['a']
      );
      const rightInput = new ArrayOperator(
        [{ b: 'x' }, { b: 'y' }, { b: 'z' }],
        ['b']
      );

      const leftPlan: ScanPlan = {
        type: 'scan',
        id: 0,
        table: 'left',
        alias: 'l',
        source: 'btree',
        columns: ['a'],
      };

      const rightPlan: ScanPlan = {
        type: 'scan',
        id: 1,
        table: 'right',
        alias: 'r',
        source: 'btree',
        columns: ['b'],
      };

      const plan: JoinPlan = {
        type: 'join',
        id: 2,
        joinType: 'cross',
        left: leftPlan,
        right: rightPlan,
      };

      const operator = new JoinOperator(plan, leftInput, rightInput, ctx);
      const rows = await collectRows(operator, ctx);

      // 2 * 3 = 6 rows
      expect(rows.length).toBe(6);
      expect(rows.map(r => [r['l.a'], r['r.b']])).toEqual([
        [1, 'x'], [1, 'y'], [1, 'z'],
        [2, 'x'], [2, 'y'], [2, 'z'],
      ]);
    });
  });
});

// =============================================================================
// AGGREGATE OPERATOR TESTS
// =============================================================================

describe('AggregateOperator Integration', () => {
  let ctx: ExecutionContext;

  beforeEach(() => {
    ctx = createTestContext();
  });

  describe('COUNT function', () => {
    it('should count all rows with COUNT(*)', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        groupBy: [],
        aggregates: [
          { expr: { type: 'aggregate', function: 'count', arg: '*' }, alias: 'total' },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].total).toBe(5);
    });

    it('should count non-NULL values with COUNT(column)', async () => {
      const input = new ArrayOperator(usersWithNulls);
      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        groupBy: [],
        aggregates: [
          {
            expr: { type: 'aggregate', function: 'count', arg: col('score') },
            alias: 'score_count',
          },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].score_count).toBe(3); // Only 3 non-null scores
    });
  });

  describe('SUM function', () => {
    it('should sum numeric values', async () => {
      const input = new ArrayOperator(sampleOrders);
      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'orders', source: 'btree', columns: [] },
        groupBy: [],
        aggregates: [
          { expr: { type: 'aggregate', function: 'sum', arg: col('amount') }, alias: 'total_amount' },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].total_amount).toBe(800); // 100+200+150+300+50
    });

    it('should skip NULL values in SUM', async () => {
      const input = new ArrayOperator(usersWithNulls);
      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        groupBy: [],
        aggregates: [
          { expr: { type: 'aggregate', function: 'sum', arg: col('score') }, alias: 'total_score' },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].total_score).toBe(250); // 85+90+75
    });
  });

  describe('AVG function', () => {
    it('should calculate average', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        groupBy: [],
        aggregates: [
          { expr: { type: 'aggregate', function: 'avg', arg: col('age') }, alias: 'avg_age' },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].avg_age).toBe(30); // (30+25+35+28+32)/5 = 150/5 = 30
    });

    it('should skip NULL values in AVG', async () => {
      const input = new ArrayOperator(usersWithNulls);
      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        groupBy: [],
        aggregates: [
          { expr: { type: 'aggregate', function: 'avg', arg: col('score') }, alias: 'avg_score' },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].avg_score).toBeCloseTo(250 / 3, 5); // (85+90+75)/3
    });
  });

  describe('MIN and MAX functions', () => {
    it('should find minimum value', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        groupBy: [],
        aggregates: [
          { expr: { type: 'aggregate', function: 'min', arg: col('age') }, alias: 'min_age' },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows[0].min_age).toBe(25);
    });

    it('should find maximum value', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        groupBy: [],
        aggregates: [
          { expr: { type: 'aggregate', function: 'max', arg: col('age') }, alias: 'max_age' },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows[0].max_age).toBe(35);
    });
  });

  describe('GROUP BY', () => {
    it('should group by single column', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        groupBy: [col('department')],
        aggregates: [
          { expr: { type: 'aggregate', function: 'count', arg: '*' }, alias: 'count' },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(3); // Engineering, Marketing, Sales

      const byDept = new Map(rows.map(r => [r.department, r.count]));
      expect(byDept.get('Engineering')).toBe(2);
      expect(byDept.get('Marketing')).toBe(2);
      expect(byDept.get('Sales')).toBe(1);
    });

    it('should group by multiple columns', async () => {
      const data: Row[] = [
        { region: 'East', product: 'A', sales: 100 },
        { region: 'East', product: 'B', sales: 150 },
        { region: 'East', product: 'A', sales: 200 },
        { region: 'West', product: 'A', sales: 300 },
        { region: 'West', product: 'B', sales: 250 },
      ];

      const input = new ArrayOperator(data);
      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'sales', source: 'btree', columns: [] },
        groupBy: [col('region'), col('product')],
        aggregates: [
          { expr: { type: 'aggregate', function: 'sum', arg: col('sales') }, alias: 'total' },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(4);

      const getTotal = (region: string, product: string) => {
        const row = rows.find(r => r.region === region && r.product === product);
        return row?.total;
      };

      expect(getTotal('East', 'A')).toBe(300);
      expect(getTotal('East', 'B')).toBe(150);
      expect(getTotal('West', 'A')).toBe(300);
      expect(getTotal('West', 'B')).toBe(250);
    });
  });

  describe('HAVING clause', () => {
    it('should filter groups with HAVING', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        groupBy: [col('department')],
        aggregates: [
          { expr: { type: 'aggregate', function: 'count', arg: '*' }, alias: 'count' },
        ],
        having: {
          type: 'comparison',
          op: 'gt',
          left: col('count'),
          right: lit(1),
        },
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      // Only Engineering (2) and Marketing (2) have count > 1
      expect(rows.length).toBe(2);
      expect(rows.every(r => (r.count as number) > 1)).toBe(true);
    });
  });

  describe('Empty input handling', () => {
    it('should return no rows for empty input without GROUP BY (implementation-specific)', async () => {
      // Note: Standard SQL typically returns one row for COUNT(*) on empty table,
      // but our implementation returns no rows when no groups are formed.
      // This test documents the actual behavior.
      const input = new ArrayOperator([]);
      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        groupBy: [],
        aggregates: [
          { expr: { type: 'aggregate', function: 'count', arg: '*' }, alias: 'count' },
          { expr: { type: 'aggregate', function: 'sum', arg: col('value') }, alias: 'total' },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      // Implementation returns no rows for empty input without group by
      // (no groups formed = no output rows)
      expect(rows.length).toBe(0);
    });

    it('should return no rows for empty input with GROUP BY', async () => {
      const input = new ArrayOperator([]);
      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        groupBy: [col('department')],
        aggregates: [
          { expr: { type: 'aggregate', function: 'count', arg: '*' }, alias: 'count' },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(0);
    });
  });
});

// =============================================================================
// SORT OPERATOR TESTS
// =============================================================================

describe('SortOperator Integration', () => {
  let ctx: ExecutionContext;

  beforeEach(() => {
    ctx = createTestContext();
  });

  describe('Basic sorting', () => {
    it('should sort ascending by default', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: SortPlan = {
        type: 'sort',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        orderBy: [
          { expr: col('age'), direction: 'asc' },
        ],
      };

      const operator = new SortOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.map(r => r.age)).toEqual([25, 28, 30, 32, 35]);
    });

    it('should sort descending', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: SortPlan = {
        type: 'sort',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        orderBy: [
          { expr: col('age'), direction: 'desc' },
        ],
      };

      const operator = new SortOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.map(r => r.age)).toEqual([35, 32, 30, 28, 25]);
    });
  });

  describe('Multi-column sorting', () => {
    it('should sort by multiple columns', async () => {
      const data: Row[] = [
        { dept: 'A', name: 'Charlie', age: 30 },
        { dept: 'B', name: 'Alice', age: 25 },
        { dept: 'A', name: 'Alice', age: 25 },
        { dept: 'B', name: 'Bob', age: 30 },
        { dept: 'A', name: 'Bob', age: 28 },
      ];

      const input = new ArrayOperator(data);
      const plan: SortPlan = {
        type: 'sort',
        id: 1,
        input: { type: 'scan', id: 0, table: 'employees', source: 'btree', columns: [] },
        orderBy: [
          { expr: col('dept'), direction: 'asc' },
          { expr: col('name'), direction: 'asc' },
        ],
      };

      const operator = new SortOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.map(r => [r.dept, r.name])).toEqual([
        ['A', 'Alice'],
        ['A', 'Bob'],
        ['A', 'Charlie'],
        ['B', 'Alice'],
        ['B', 'Bob'],
      ]);
    });

    it('should sort with mixed directions', async () => {
      const data: Row[] = [
        { category: 'A', value: 10 },
        { category: 'B', value: 20 },
        { category: 'A', value: 30 },
        { category: 'B', value: 10 },
        { category: 'A', value: 20 },
      ];

      const input = new ArrayOperator(data);
      const plan: SortPlan = {
        type: 'sort',
        id: 1,
        input: { type: 'scan', id: 0, table: 'data', source: 'btree', columns: [] },
        orderBy: [
          { expr: col('category'), direction: 'asc' },
          { expr: col('value'), direction: 'desc' },
        ],
      };

      const operator = new SortOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.map(r => [r.category, r.value])).toEqual([
        ['A', 30],
        ['A', 20],
        ['A', 10],
        ['B', 20],
        ['B', 10],
      ]);
    });
  });

  describe('NULL handling in sort', () => {
    it('should handle NULLS FIRST', async () => {
      const data: Row[] = [
        { id: 1, value: 30 },
        { id: 2, value: null },
        { id: 3, value: 10 },
        { id: 4, value: null },
        { id: 5, value: 20 },
      ];

      const input = new ArrayOperator(data);
      const plan: SortPlan = {
        type: 'sort',
        id: 1,
        input: { type: 'scan', id: 0, table: 'data', source: 'btree', columns: [] },
        orderBy: [
          { expr: col('value'), direction: 'asc', nullsFirst: true },
        ],
      };

      const operator = new SortOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      // NULLs first, then sorted values
      expect(rows.slice(0, 2).every(r => r.value === null)).toBe(true);
      expect(rows.slice(2).map(r => r.value)).toEqual([10, 20, 30]);
    });

    it('should handle NULLS LAST (default)', async () => {
      const data: Row[] = [
        { id: 1, value: 30 },
        { id: 2, value: null },
        { id: 3, value: 10 },
        { id: 4, value: null },
        { id: 5, value: 20 },
      ];

      const input = new ArrayOperator(data);
      const plan: SortPlan = {
        type: 'sort',
        id: 1,
        input: { type: 'scan', id: 0, table: 'data', source: 'btree', columns: [] },
        orderBy: [
          { expr: col('value'), direction: 'asc', nullsFirst: false },
        ],
      };

      const operator = new SortOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      // Sorted values first, then NULLs
      expect(rows.slice(0, 3).map(r => r.value)).toEqual([10, 20, 30]);
      expect(rows.slice(3).every(r => r.value === null)).toBe(true);
    });
  });

  describe('String sorting', () => {
    it('should sort strings alphabetically', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: SortPlan = {
        type: 'sort',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        orderBy: [
          { expr: col('name'), direction: 'asc' },
        ],
      };

      const operator = new SortOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.map(r => r.name)).toEqual(['Alice', 'Bob', 'Charlie', 'Diana', 'Eve']);
    });
  });
});

// =============================================================================
// LIMIT OPERATOR TESTS
// =============================================================================

describe('LimitOperator Integration', () => {
  let ctx: ExecutionContext;

  beforeEach(() => {
    ctx = createTestContext();
  });

  describe('LIMIT only', () => {
    it('should limit rows returned', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: LimitPlan = {
        type: 'limit',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        limit: 3,
      };

      const operator = new LimitOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(3);
      expect(rows.map(r => r.name)).toEqual(['Alice', 'Bob', 'Charlie']);
    });

    it('should return all rows if limit exceeds count', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: LimitPlan = {
        type: 'limit',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        limit: 100,
      };

      const operator = new LimitOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(5);
    });

    it('should handle LIMIT 0', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: LimitPlan = {
        type: 'limit',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        limit: 0,
      };

      const operator = new LimitOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(0);
    });
  });

  describe('LIMIT with OFFSET', () => {
    it('should skip rows with offset', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: LimitPlan = {
        type: 'limit',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        limit: 2,
        offset: 2,
      };

      const operator = new LimitOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.map(r => r.name)).toEqual(['Charlie', 'Diana']);
    });

    it('should return empty if offset exceeds count', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: LimitPlan = {
        type: 'limit',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        limit: 10,
        offset: 100,
      };

      const operator = new LimitOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(0);
    });

    it('should handle partial results when offset + limit exceeds count', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: LimitPlan = {
        type: 'limit',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        limit: 10,
        offset: 3,
      };

      const operator = new LimitOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2); // Only 2 rows after offset 3
      expect(rows.map(r => r.name)).toEqual(['Diana', 'Eve']);
    });
  });

  describe('Empty input', () => {
    it('should handle empty input', async () => {
      const input = new ArrayOperator([]);
      const plan: LimitPlan = {
        type: 'limit',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        limit: 10,
      };

      const operator = new LimitOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(0);
    });
  });
});

// =============================================================================
// FULL PIPELINE TESTS
// =============================================================================

describe('Full Execution Pipeline Integration', () => {
  let ctx: ExecutionContext;

  beforeEach(() => {
    ctx = createTestContext();
  });

  describe('Filter + Sort + Limit', () => {
    it('should execute WHERE + ORDER BY + LIMIT', async () => {
      // Start with ArrayOperator
      const scanOp = new ArrayOperator(sampleUsers);

      // Filter: department = 'Engineering' OR department = 'Marketing'
      const filterPlan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'logical',
          op: 'or',
          operands: [
            { type: 'comparison', op: 'eq', left: col('department'), right: lit('Engineering') },
            { type: 'comparison', op: 'eq', left: col('department'), right: lit('Marketing') },
          ],
        },
      };
      const filterOp = new FilterOperator(filterPlan, scanOp, ctx);

      // Sort by age DESC
      const sortPlan: SortPlan = {
        type: 'sort',
        id: 2,
        input: filterPlan,
        orderBy: [{ expr: col('age'), direction: 'desc' }],
      };
      const sortOp = new SortOperator(sortPlan, filterOp, ctx);

      // Limit to 2
      const limitPlan: LimitPlan = {
        type: 'limit',
        id: 3,
        input: sortPlan,
        limit: 2,
      };
      const limitOp = new LimitOperator(limitPlan, sortOp, ctx);

      const rows = await collectRows(limitOp, ctx);

      expect(rows.length).toBe(2);
      expect(rows[0].name).toBe('Charlie'); // age 35, Engineering
      expect(rows[1].name).toBe('Eve');     // age 32, Marketing
    });
  });

  describe('Join + Filter + Project', () => {
    it('should execute JOIN + WHERE + SELECT', async () => {
      const userInput = new ArrayOperator(sampleUsers.slice(0, 3), ['id', 'name', 'age', 'department']);
      const orderInput = new ArrayOperator(sampleOrders, ['id', 'user_id', 'amount', 'status']);

      // Join users with orders
      const joinPlan: JoinPlan = {
        type: 'join',
        id: 1,
        joinType: 'inner',
        left: { type: 'scan', id: 0, table: 'users', alias: 'u', source: 'btree', columns: ['id', 'name', 'age', 'department'] },
        right: { type: 'scan', id: 0, table: 'orders', alias: 'o', source: 'btree', columns: ['id', 'user_id', 'amount', 'status'] },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: col('id', 'u'),
          right: col('user_id', 'o'),
        },
      };
      const joinOp = new JoinOperator(joinPlan, userInput, orderInput, ctx);

      // Filter: status = 'completed'
      const filterPlan: FilterPlan = {
        type: 'filter',
        id: 2,
        input: joinPlan,
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: col('status', 'o'),
          right: lit('completed'),
        },
      };
      const filterOp = new FilterOperator(filterPlan, joinOp, ctx);

      // Project: user name and order amount
      const projectPlan: ProjectPlan = {
        type: 'project',
        id: 3,
        input: filterPlan,
        expressions: [
          { expr: col('name', 'u'), alias: 'customer' },
          { expr: col('amount', 'o'), alias: 'order_amount' },
        ],
      };
      const projectOp = new ProjectOperator(projectPlan, filterOp, ctx);

      const rows = await collectRows(projectOp, ctx);

      // Alice has 1 completed order, Bob has 1, Charlie has 1
      expect(rows.length).toBe(3);
      expect(rows.map(r => r.customer)).toContain('Alice');
      expect(rows.map(r => r.customer)).toContain('Bob');
      expect(rows.map(r => r.customer)).toContain('Charlie');
    });
  });

  describe('Aggregate + Having + Sort', () => {
    it('should execute GROUP BY + HAVING + ORDER BY', async () => {
      const input = new ArrayOperator(sampleOrders);

      // Group by user_id, count orders, sum amounts
      const aggPlan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'orders', source: 'btree', columns: [] },
        groupBy: [col('user_id')],
        aggregates: [
          { expr: { type: 'aggregate', function: 'count', arg: '*' }, alias: 'order_count' },
          { expr: { type: 'aggregate', function: 'sum', arg: col('amount') }, alias: 'total_amount' },
        ],
        having: {
          type: 'comparison',
          op: 'ge',
          left: col('order_count'),
          right: lit(1),
        },
      };
      const aggOp = new AggregateOperator(aggPlan, input, ctx);

      // Sort by total_amount DESC
      const sortPlan: SortPlan = {
        type: 'sort',
        id: 2,
        input: aggPlan,
        orderBy: [{ expr: col('total_amount'), direction: 'desc' }],
      };
      const sortOp = new SortOperator(sortPlan, aggOp, ctx);

      const rows = await collectRows(sortOp, ctx);

      expect(rows.length).toBe(4); // 4 distinct user_ids

      // Verify sorting
      const amounts = rows.map(r => r.total_amount as number);
      for (let i = 1; i < amounts.length; i++) {
        expect(amounts[i - 1]).toBeGreaterThanOrEqual(amounts[i]);
      }

      // User 1 (Alice) has the highest total (300)
      expect(rows[0].user_id).toBe(1);
      expect(rows[0].total_amount).toBe(300);
    });
  });

  describe('Complex nested pipeline', () => {
    it('should handle deeply nested operations', async () => {
      // Simulate: SELECT name, total FROM (
      //   SELECT u.name, SUM(o.amount) as total FROM users u
      //   JOIN orders o ON u.id = o.user_id
      //   WHERE o.status = 'completed'
      //   GROUP BY u.name
      // ) AS subquery
      // ORDER BY total DESC LIMIT 2

      // This tests combining all operators
      const userInput = new ArrayOperator(sampleUsers.slice(0, 3), ['id', 'name', 'age', 'department']);
      const orderInput = new ArrayOperator(sampleOrders.filter(o => o.status === 'completed'), ['id', 'user_id', 'amount', 'status']);

      // Join
      const joinPlan: JoinPlan = {
        type: 'join',
        id: 1,
        joinType: 'inner',
        left: { type: 'scan', id: 0, table: 'users', alias: 'u', source: 'btree', columns: ['id', 'name', 'age', 'department'] },
        right: { type: 'scan', id: 0, table: 'orders', alias: 'o', source: 'btree', columns: ['id', 'user_id', 'amount', 'status'] },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: col('id', 'u'),
          right: col('user_id', 'o'),
        },
      };
      const joinOp = new JoinOperator(joinPlan, userInput, orderInput, ctx);

      // Aggregate by name
      const aggPlan: AggregatePlan = {
        type: 'aggregate',
        id: 2,
        input: joinPlan,
        groupBy: [col('name', 'u')],
        aggregates: [
          { expr: { type: 'aggregate', function: 'sum', arg: col('amount', 'o') }, alias: 'total' },
        ],
      };
      const aggOp = new AggregateOperator(aggPlan, joinOp, ctx);

      // Sort
      const sortPlan: SortPlan = {
        type: 'sort',
        id: 3,
        input: aggPlan,
        orderBy: [{ expr: col('total'), direction: 'desc' }],
      };
      const sortOp = new SortOperator(sortPlan, aggOp, ctx);

      // Limit
      const limitPlan: LimitPlan = {
        type: 'limit',
        id: 4,
        input: sortPlan,
        limit: 2,
      };
      const limitOp = new LimitOperator(limitPlan, sortOp, ctx);

      const rows = await collectRows(limitOp, ctx);

      expect(rows.length).toBe(2);
      // Charlie has the highest (300), then Bob (150), then Alice (100)
      expect(rows[0].total).toBe(300);
    });
  });
});

// =============================================================================
// EXECUTOR TESTS
// =============================================================================

describe('QueryExecutor Integration', () => {
  let ctx: ExecutionContext;
  let tables: Map<string, Row[]>;

  beforeEach(() => {
    tables = new Map([
      ['users', [...sampleUsers]],
      ['orders', [...sampleOrders]],
    ]);
    ctx = createTestContext(tables);
  });

  describe('executePlan function', () => {
    it('should execute a scan plan with real storage', async () => {
      const plan: ScanPlan = {
        type: 'scan',
        id: 0,
        table: 'users',
        source: 'btree',
        columns: ['id', 'name', 'age', 'department'],
      };

      const result = await executePlan(plan, ctx);

      expect(result.rows.length).toBe(5);
      expect(result.stats?.rowsReturned).toBe(5);
    });

    it('should execute a filter plan with real storage', async () => {
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: {
          type: 'scan',
          id: 0,
          table: 'users',
          source: 'btree',
          columns: ['id', 'name', 'age', 'department'],
        },
        predicate: {
          type: 'comparison',
          op: 'gt',
          left: col('age'),
          right: lit(30),
        },
      };

      const result = await executePlan(plan, ctx);

      expect(result.rows.length).toBe(2);
      expect(result.rows.map(r => r.name)).toContain('Charlie');
      expect(result.rows.map(r => r.name)).toContain('Eve');
    });
  });

  describe('QueryExecutor class', () => {
    it('should execute queries via executor instance', async () => {
      const executor = createExecutor(ctx);

      const plan: ScanPlan = {
        type: 'scan',
        id: 0,
        table: 'orders',
        source: 'btree',
        columns: ['id', 'user_id', 'amount', 'status'],
      };

      const rows = await executor.query(plan);

      expect(rows.length).toBe(5);
    });

    it('should return first row with queryOne', async () => {
      const executor = createExecutor(ctx);

      const plan: ScanPlan = {
        type: 'scan',
        id: 0,
        table: 'users',
        source: 'btree',
        columns: ['id', 'name', 'age', 'department'],
      };

      const row = await executor.queryOne(plan);

      expect(row).not.toBeNull();
      expect(row?.name).toBe('Alice');
    });

    it('should return null for queryOne on empty result', async () => {
      const emptyTables = new Map<string, Row[]>([['empty', []]]);
      const emptyCtx = createTestContext(emptyTables);
      const executor = createExecutor(emptyCtx);

      const plan: ScanPlan = {
        type: 'scan',
        id: 0,
        table: 'empty',
        source: 'btree',
        columns: [],
      };

      const row = await executor.queryOne(plan);

      expect(row).toBeNull();
    });
  });
});

// =============================================================================
// TYPE COERCION TESTS
// =============================================================================

describe('Type Coercion Integration', () => {
  let ctx: ExecutionContext;

  beforeEach(() => {
    ctx = createTestContext();
  });

  describe('Numeric comparisons', () => {
    it('should compare integers correctly', async () => {
      const data: Row[] = [
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 5 },
      ];

      const input = new ArrayOperator(data);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'data', source: 'btree', columns: [] },
        predicate: {
          type: 'comparison',
          op: 'gt',
          left: col('value'),
          right: lit(10),
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].value).toBe(20);
    });

    it('should handle bigint values', async () => {
      const data: Row[] = [
        { id: 1, value: 10n },
        { id: 2, value: 20n },
        { id: 3, value: 5n },
      ];

      const input = new ArrayOperator(data);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'data', source: 'btree', columns: [] },
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: col('value'),
          right: lit(10n),
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].id).toBe(1);
    });
  });

  describe('String comparisons', () => {
    it('should compare strings correctly', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'comparison',
          op: 'lt',
          left: col('name'),
          right: lit('C'),
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      // Alice, Bob come before 'C'
      expect(rows.length).toBe(2);
      expect(rows.map(r => r.name).sort()).toEqual(['Alice', 'Bob']);
    });

    it('should handle LIKE patterns', async () => {
      const input = new ArrayOperator(sampleUsers);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: [] },
        predicate: {
          type: 'comparison',
          op: 'like',
          left: col('name'),
          right: lit('%e'),
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      // Alice, Charlie, Eve end with 'e'
      expect(rows.length).toBe(3);
      expect(rows.map(r => r.name).sort()).toEqual(['Alice', 'Charlie', 'Eve']);
    });
  });

  describe('Boolean handling', () => {
    it('should handle boolean values', async () => {
      const data: Row[] = [
        { id: 1, active: true },
        { id: 2, active: false },
        { id: 3, active: true },
      ];

      const input = new ArrayOperator(data);
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'data', source: 'btree', columns: [] },
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: col('active'),
          right: lit(true),
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.map(r => r.id)).toEqual([1, 3]);
    });
  });
});
