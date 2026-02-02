/**
 * Engine Operator Unit Tests
 *
 * Comprehensive tests for the engine operators:
 * - AggregateOperator: COUNT, SUM, AVG, MIN, MAX with GROUP BY and HAVING
 * - WindowOperator: ROW_NUMBER, RANK, DENSE_RANK, etc.
 * - FilterOperator: Predicate evaluation and filter pushdown
 * - CTE Operator: WITH clause evaluation, recursive CTEs
 *
 * Uses real operator implementations - no mocks.
 */

import { describe, it, expect, beforeEach } from 'vitest';

// Aggregate operator imports
import { AggregateOperator } from '../operators/aggregate.js';

// Window operator imports
import {
  WindowOperator,
  WindowPlan,
  WindowFunctionDef,
  containsWindowFunction,
  extractWindowFunctions,
} from '../operators/window.js';

// Filter operator imports
import {
  FilterOperator,
  evaluateExpression,
  evaluatePredicate,
} from '../operators/filter.js';

// CTE operator imports
import {
  CTEScanOperator,
  createCTEContext,
  executeSimpleCTE,
  executeRecursiveCTE,
  executeWithClause,
  getCTERows,
  isCTEMaterialized,
  getCTEColumns,
  createMockQueryExecutor,
  MaterializedCTE,
  CTEExecutionContext,
} from '../operators/cte.js';

// Types
import type {
  Operator,
  Row,
  ExecutionContext,
  AggregatePlan,
  FilterPlan,
  Predicate,
  Expression,
  Schema,
} from '../types.js';

import type { WindowSpec, WindowContext } from '../../functions/window.js';

import type { CTEDefinition, WithClause } from '../../parser/cte-types.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Create a mock execution context for testing
 */
function createMockContext(): ExecutionContext {
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
 * Create a simple mock operator that yields a fixed set of rows
 */
class MockOperator implements Operator {
  private rows: Row[];
  private index = 0;
  private columnNames: string[];

  constructor(rows: Row[]) {
    this.rows = rows;
    this.columnNames = rows.length > 0 ? Object.keys(rows[0]) : [];
  }

  async open(_ctx: ExecutionContext): Promise<void> {
    this.index = 0;
  }

  async next(): Promise<Row | null> {
    if (this.index >= this.rows.length) {
      return null;
    }
    return this.rows[this.index++];
  }

  async close(): Promise<void> {
    this.index = 0;
  }

  columns(): string[] {
    return this.columnNames;
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

// =============================================================================
// AGGREGATE OPERATOR TESTS
// =============================================================================

describe('AggregateOperator', () => {
  let ctx: ExecutionContext;

  beforeEach(() => {
    ctx = createMockContext();
  });

  describe('COUNT function', () => {
    it('should count all rows with COUNT(*)', async () => {
      const input = new MockOperator([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
      ]);

      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: ['id', 'name'] },
        groupBy: [],
        aggregates: [
          {
            expr: { type: 'aggregate', function: 'count', arg: '*' },
            alias: 'total',
          },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].total).toBe(3);
    });

    it('should count non-null values with COUNT(column)', async () => {
      const input = new MockOperator([
        { id: 1, name: 'Alice' },
        { id: 2, name: null },
        { id: 3, name: 'Charlie' },
      ]);

      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: ['id', 'name'] },
        groupBy: [],
        aggregates: [
          {
            expr: {
              type: 'aggregate',
              function: 'count',
              arg: { type: 'columnRef', column: 'name' },
            },
            alias: 'name_count',
          },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].name_count).toBe(2);
    });
  });

  describe('SUM function', () => {
    it('should sum numeric values', async () => {
      const input = new MockOperator([
        { id: 1, amount: 100 },
        { id: 2, amount: 200 },
        { id: 3, amount: 300 },
      ]);

      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'orders', source: 'btree', columns: ['id', 'amount'] },
        groupBy: [],
        aggregates: [
          {
            expr: {
              type: 'aggregate',
              function: 'sum',
              arg: { type: 'columnRef', column: 'amount' },
            },
            alias: 'total_amount',
          },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].total_amount).toBe(600);
    });

    it('should skip null values in SUM', async () => {
      const input = new MockOperator([
        { id: 1, amount: 100 },
        { id: 2, amount: null },
        { id: 3, amount: 300 },
      ]);

      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'orders', source: 'btree', columns: ['id', 'amount'] },
        groupBy: [],
        aggregates: [
          {
            expr: {
              type: 'aggregate',
              function: 'sum',
              arg: { type: 'columnRef', column: 'amount' },
            },
            alias: 'total_amount',
          },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].total_amount).toBe(400);
    });

    it('should handle bigint values in SUM', async () => {
      const input = new MockOperator([
        { id: 1, amount: 100n },
        { id: 2, amount: 200n },
      ]);

      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'orders', source: 'btree', columns: ['id', 'amount'] },
        groupBy: [],
        aggregates: [
          {
            expr: {
              type: 'aggregate',
              function: 'sum',
              arg: { type: 'columnRef', column: 'amount' },
            },
            alias: 'total_amount',
          },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].total_amount).toBe(300n);
    });
  });

  describe('AVG function', () => {
    it('should calculate average of numeric values', async () => {
      const input = new MockOperator([
        { id: 1, score: 80 },
        { id: 2, score: 90 },
        { id: 3, score: 100 },
      ]);

      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'students', source: 'btree', columns: ['id', 'score'] },
        groupBy: [],
        aggregates: [
          {
            expr: {
              type: 'aggregate',
              function: 'avg',
              arg: { type: 'columnRef', column: 'score' },
            },
            alias: 'avg_score',
          },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].avg_score).toBe(90);
    });

    it('should return no rows for AVG of empty set (no groups)', async () => {
      const input = new MockOperator([]);

      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'students', source: 'btree', columns: ['id', 'score'] },
        groupBy: [],
        aggregates: [
          {
            expr: {
              type: 'aggregate',
              function: 'avg',
              arg: { type: 'columnRef', column: 'score' },
            },
            alias: 'avg_score',
          },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      // When there are no input rows, no groups are created, so no output rows
      // This differs from SQL standard which would return one row with NULL
      // The current implementation returns 0 rows for empty input
      expect(rows.length).toBe(0);
    });
  });

  describe('MIN and MAX functions', () => {
    it('should find minimum value', async () => {
      const input = new MockOperator([
        { id: 1, price: 50 },
        { id: 2, price: 25 },
        { id: 3, price: 75 },
      ]);

      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'products', source: 'btree', columns: ['id', 'price'] },
        groupBy: [],
        aggregates: [
          {
            expr: {
              type: 'aggregate',
              function: 'min',
              arg: { type: 'columnRef', column: 'price' },
            },
            alias: 'min_price',
          },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].min_price).toBe(25);
    });

    it('should find maximum value', async () => {
      const input = new MockOperator([
        { id: 1, price: 50 },
        { id: 2, price: 25 },
        { id: 3, price: 75 },
      ]);

      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'products', source: 'btree', columns: ['id', 'price'] },
        groupBy: [],
        aggregates: [
          {
            expr: {
              type: 'aggregate',
              function: 'max',
              arg: { type: 'columnRef', column: 'price' },
            },
            alias: 'max_price',
          },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].max_price).toBe(75);
    });

    it('should find min/max of string values', async () => {
      const input = new MockOperator([
        { id: 1, name: 'Charlie' },
        { id: 2, name: 'Alice' },
        { id: 3, name: 'Bob' },
      ]);

      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: ['id', 'name'] },
        groupBy: [],
        aggregates: [
          {
            expr: {
              type: 'aggregate',
              function: 'min',
              arg: { type: 'columnRef', column: 'name' },
            },
            alias: 'first_name',
          },
          {
            expr: {
              type: 'aggregate',
              function: 'max',
              arg: { type: 'columnRef', column: 'name' },
            },
            alias: 'last_name',
          },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].first_name).toBe('Alice');
      expect(rows[0].last_name).toBe('Charlie');
    });
  });

  describe('GROUP BY', () => {
    it('should group by single column', async () => {
      const input = new MockOperator([
        { dept: 'Sales', salary: 50000 },
        { dept: 'Engineering', salary: 80000 },
        { dept: 'Sales', salary: 55000 },
        { dept: 'Engineering', salary: 90000 },
      ]);

      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'employees', source: 'btree', columns: ['dept', 'salary'] },
        groupBy: [{ type: 'columnRef', column: 'dept' }],
        aggregates: [
          {
            expr: {
              type: 'aggregate',
              function: 'sum',
              arg: { type: 'columnRef', column: 'salary' },
            },
            alias: 'total_salary',
          },
          {
            expr: { type: 'aggregate', function: 'count', arg: '*' },
            alias: 'emp_count',
          },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);

      const salesRow = rows.find(r => r.dept === 'Sales');
      const engRow = rows.find(r => r.dept === 'Engineering');

      expect(salesRow).toBeDefined();
      expect(salesRow!.total_salary).toBe(105000);
      expect(salesRow!.emp_count).toBe(2);

      expect(engRow).toBeDefined();
      expect(engRow!.total_salary).toBe(170000);
      expect(engRow!.emp_count).toBe(2);
    });

    it('should group by multiple columns', async () => {
      const input = new MockOperator([
        { dept: 'Sales', region: 'North', revenue: 1000 },
        { dept: 'Sales', region: 'South', revenue: 1500 },
        { dept: 'Sales', region: 'North', revenue: 2000 },
        { dept: 'Engineering', region: 'North', revenue: 500 },
      ]);

      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'revenue', source: 'btree', columns: ['dept', 'region', 'revenue'] },
        groupBy: [
          { type: 'columnRef', column: 'dept' },
          { type: 'columnRef', column: 'region' },
        ],
        aggregates: [
          {
            expr: {
              type: 'aggregate',
              function: 'sum',
              arg: { type: 'columnRef', column: 'revenue' },
            },
            alias: 'total_revenue',
          },
        ],
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(3);

      const salesNorth = rows.find(r => r.dept === 'Sales' && r.region === 'North');
      expect(salesNorth).toBeDefined();
      expect(salesNorth!.total_revenue).toBe(3000);
    });
  });

  describe('HAVING clause', () => {
    it('should filter groups with HAVING', async () => {
      const input = new MockOperator([
        { category: 'A', value: 10 },
        { category: 'B', value: 5 },
        { category: 'A', value: 20 },
        { category: 'C', value: 100 },
      ]);

      const plan: AggregatePlan = {
        type: 'aggregate',
        id: 1,
        input: { type: 'scan', id: 0, table: 'items', source: 'btree', columns: ['category', 'value'] },
        groupBy: [{ type: 'columnRef', column: 'category' }],
        aggregates: [
          {
            expr: {
              type: 'aggregate',
              function: 'sum',
              arg: { type: 'columnRef', column: 'value' },
            },
            alias: 'total',
          },
        ],
        having: {
          type: 'comparison',
          op: 'gt',
          left: {
            type: 'aggregate',
            function: 'sum',
            arg: { type: 'columnRef', column: 'value' },
          },
          right: { type: 'literal', value: 20, dataType: 'number' },
        },
      };

      const operator = new AggregateOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      // Only 'A' (sum=30) and 'C' (sum=100) should pass HAVING > 20
      expect(rows.length).toBe(2);
      expect(rows.some(r => r.category === 'A')).toBe(true);
      expect(rows.some(r => r.category === 'C')).toBe(true);
      expect(rows.some(r => r.category === 'B')).toBe(false);
    });
  });
});

// =============================================================================
// FILTER OPERATOR TESTS
// =============================================================================

describe('FilterOperator', () => {
  let ctx: ExecutionContext;

  beforeEach(() => {
    ctx = createMockContext();
  });

  describe('comparison predicates', () => {
    it('should filter with equality predicate', async () => {
      const input = new MockOperator([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Alice' },
      ]);

      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: ['id', 'name'] },
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'name' },
          right: { type: 'literal', value: 'Alice', dataType: 'string' },
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.every(r => r.name === 'Alice')).toBe(true);
    });

    it('should filter with greater than predicate', async () => {
      const input = new MockOperator([
        { id: 1, age: 25 },
        { id: 2, age: 30 },
        { id: 3, age: 35 },
      ]);

      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: ['id', 'age'] },
        predicate: {
          type: 'comparison',
          op: 'gt',
          left: { type: 'columnRef', column: 'age' },
          right: { type: 'literal', value: 28, dataType: 'number' },
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.every(r => (r.age as number) > 28)).toBe(true);
    });

    it('should filter with LIKE predicate', async () => {
      const input = new MockOperator([
        { id: 1, email: 'alice@example.com' },
        { id: 2, email: 'bob@test.com' },
        { id: 3, email: 'charlie@example.com' },
      ]);

      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: ['id', 'email'] },
        predicate: {
          type: 'comparison',
          op: 'like',
          left: { type: 'columnRef', column: 'email' },
          right: { type: 'literal', value: '%@example.com', dataType: 'string' },
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.every(r => (r.email as string).endsWith('@example.com'))).toBe(true);
    });
  });

  describe('logical predicates', () => {
    it('should filter with AND predicate', async () => {
      const input = new MockOperator([
        { id: 1, age: 25, active: true },
        { id: 2, age: 35, active: true },
        { id: 3, age: 25, active: false },
      ]);

      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: ['id', 'age', 'active'] },
        predicate: {
          type: 'logical',
          op: 'and',
          operands: [
            {
              type: 'comparison',
              op: 'gt',
              left: { type: 'columnRef', column: 'age' },
              right: { type: 'literal', value: 20, dataType: 'number' },
            },
            {
              type: 'comparison',
              op: 'eq',
              left: { type: 'columnRef', column: 'active' },
              right: { type: 'literal', value: true, dataType: 'boolean' },
            },
          ],
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.every(r => (r.age as number) > 20 && r.active === true)).toBe(true);
    });

    it('should filter with OR predicate', async () => {
      const input = new MockOperator([
        { id: 1, status: 'active' },
        { id: 2, status: 'pending' },
        { id: 3, status: 'inactive' },
      ]);

      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: ['id', 'status'] },
        predicate: {
          type: 'logical',
          op: 'or',
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
              left: { type: 'columnRef', column: 'status' },
              right: { type: 'literal', value: 'pending', dataType: 'string' },
            },
          ],
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.some(r => r.status === 'active')).toBe(true);
      expect(rows.some(r => r.status === 'pending')).toBe(true);
    });

    it('should filter with NOT predicate', async () => {
      const input = new MockOperator([
        { id: 1, deleted: false },
        { id: 2, deleted: true },
        { id: 3, deleted: false },
      ]);

      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: ['id', 'deleted'] },
        predicate: {
          type: 'logical',
          op: 'not',
          operands: [
            {
              type: 'comparison',
              op: 'eq',
              left: { type: 'columnRef', column: 'deleted' },
              right: { type: 'literal', value: true, dataType: 'boolean' },
            },
          ],
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.every(r => r.deleted === false)).toBe(true);
    });
  });

  describe('BETWEEN predicate', () => {
    it('should filter with BETWEEN predicate', async () => {
      const input = new MockOperator([
        { id: 1, price: 10 },
        { id: 2, price: 25 },
        { id: 3, price: 50 },
        { id: 4, price: 75 },
      ]);

      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'products', source: 'btree', columns: ['id', 'price'] },
        predicate: {
          type: 'between',
          expr: { type: 'columnRef', column: 'price' },
          low: { type: 'literal', value: 20, dataType: 'number' },
          high: { type: 'literal', value: 60, dataType: 'number' },
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.every(r => (r.price as number) >= 20 && (r.price as number) <= 60)).toBe(true);
    });
  });

  describe('IN predicate', () => {
    it('should filter with IN predicate', async () => {
      const input = new MockOperator([
        { id: 1, status: 'active' },
        { id: 2, status: 'pending' },
        { id: 3, status: 'inactive' },
        { id: 4, status: 'deleted' },
      ]);

      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: ['id', 'status'] },
        predicate: {
          type: 'in',
          expr: { type: 'columnRef', column: 'status' },
          values: [
            { type: 'literal', value: 'active', dataType: 'string' },
            { type: 'literal', value: 'pending', dataType: 'string' },
          ],
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.some(r => r.status === 'active')).toBe(true);
      expect(rows.some(r => r.status === 'pending')).toBe(true);
    });

    it('should return false for IN with empty list', async () => {
      const input = new MockOperator([
        { id: 1, status: 'active' },
      ]);

      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: ['id', 'status'] },
        predicate: {
          type: 'in',
          expr: { type: 'columnRef', column: 'status' },
          values: [],
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(0);
    });
  });

  describe('IS NULL predicate', () => {
    it('should filter with IS NULL predicate', async () => {
      const input = new MockOperator([
        { id: 1, email: 'alice@test.com' },
        { id: 2, email: null },
        { id: 3, email: 'bob@test.com' },
      ]);

      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: ['id', 'email'] },
        predicate: {
          type: 'isNull',
          expr: { type: 'columnRef', column: 'email' },
          isNot: false,
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(1);
      expect(rows[0].id).toBe(2);
    });

    it('should filter with IS NOT NULL predicate', async () => {
      const input = new MockOperator([
        { id: 1, email: 'alice@test.com' },
        { id: 2, email: null },
        { id: 3, email: 'bob@test.com' },
      ]);

      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: ['id', 'email'] },
        predicate: {
          type: 'isNull',
          expr: { type: 'columnRef', column: 'email' },
          isNot: true,
        },
      };

      const operator = new FilterOperator(plan, input, ctx);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(2);
      expect(rows.every(r => r.email !== null)).toBe(true);
    });
  });
});

describe('evaluateExpression', () => {
  describe('column references', () => {
    it('should evaluate column reference', () => {
      const row = { id: 1, name: 'Alice' };
      const expr: Expression = { type: 'columnRef', column: 'name' };
      expect(evaluateExpression(expr, row)).toBe('Alice');
    });

    it('should evaluate qualified column reference', () => {
      const row = { 'users.name': 'Alice' };
      const expr: Expression = { type: 'columnRef', table: 'users', column: 'name' };
      expect(evaluateExpression(expr, row)).toBe('Alice');
    });

    it('should return null for missing column', () => {
      const row = { id: 1 };
      const expr: Expression = { type: 'columnRef', column: 'name' };
      expect(evaluateExpression(expr, row)).toBe(null);
    });
  });

  describe('literals', () => {
    it('should evaluate string literal', () => {
      const expr: Expression = { type: 'literal', value: 'hello', dataType: 'string' };
      expect(evaluateExpression(expr, {})).toBe('hello');
    });

    it('should evaluate number literal', () => {
      const expr: Expression = { type: 'literal', value: 42, dataType: 'number' };
      expect(evaluateExpression(expr, {})).toBe(42);
    });

    it('should evaluate null literal', () => {
      const expr: Expression = { type: 'literal', value: null, dataType: 'null' };
      expect(evaluateExpression(expr, {})).toBe(null);
    });
  });

  describe('binary expressions', () => {
    it('should evaluate arithmetic addition', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'add',
        left: { type: 'literal', value: 10, dataType: 'number' },
        right: { type: 'literal', value: 5, dataType: 'number' },
      };
      expect(evaluateExpression(expr, {})).toBe(15);
    });

    it('should evaluate arithmetic subtraction', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'sub',
        left: { type: 'literal', value: 10, dataType: 'number' },
        right: { type: 'literal', value: 3, dataType: 'number' },
      };
      expect(evaluateExpression(expr, {})).toBe(7);
    });

    it('should evaluate arithmetic multiplication', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'mul',
        left: { type: 'literal', value: 6, dataType: 'number' },
        right: { type: 'literal', value: 7, dataType: 'number' },
      };
      expect(evaluateExpression(expr, {})).toBe(42);
    });

    it('should evaluate arithmetic division', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'div',
        left: { type: 'literal', value: 20, dataType: 'number' },
        right: { type: 'literal', value: 4, dataType: 'number' },
      };
      expect(evaluateExpression(expr, {})).toBe(5);
    });

    it('should return null for division by zero', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'div',
        left: { type: 'literal', value: 10, dataType: 'number' },
        right: { type: 'literal', value: 0, dataType: 'number' },
      };
      expect(evaluateExpression(expr, {})).toBe(null);
    });

    it('should evaluate comparison returning SQL integer', () => {
      const eqExpr: Expression = {
        type: 'binary',
        op: 'eq',
        left: { type: 'literal', value: 5, dataType: 'number' },
        right: { type: 'literal', value: 5, dataType: 'number' },
      };
      expect(evaluateExpression(eqExpr, {})).toBe(1);

      const neExpr: Expression = {
        type: 'binary',
        op: 'eq',
        left: { type: 'literal', value: 5, dataType: 'number' },
        right: { type: 'literal', value: 6, dataType: 'number' },
      };
      expect(evaluateExpression(neExpr, {})).toBe(0);
    });
  });

  describe('unary expressions', () => {
    it('should evaluate negation', () => {
      const expr: Expression = {
        type: 'unary',
        op: 'neg',
        operand: { type: 'literal', value: 5, dataType: 'number' },
      };
      expect(evaluateExpression(expr, {})).toBe(-5);
    });

    it('should evaluate NOT', () => {
      const expr: Expression = {
        type: 'unary',
        op: 'not',
        operand: { type: 'literal', value: 1, dataType: 'number' },
      };
      expect(evaluateExpression(expr, {})).toBe(0);
    });

    it('should evaluate IS NULL', () => {
      const expr: Expression = {
        type: 'unary',
        op: 'isNull',
        operand: { type: 'literal', value: null, dataType: 'null' },
      };
      expect(evaluateExpression(expr, {})).toBe(1);
    });

    it('should evaluate IS NOT NULL', () => {
      const expr: Expression = {
        type: 'unary',
        op: 'isNotNull',
        operand: { type: 'literal', value: 'hello', dataType: 'string' },
      };
      expect(evaluateExpression(expr, {})).toBe(1);
    });
  });

  describe('function calls', () => {
    it('should evaluate UPPER function', () => {
      const expr: Expression = {
        type: 'function',
        name: 'upper',
        args: [{ type: 'literal', value: 'hello', dataType: 'string' }],
      };
      expect(evaluateExpression(expr, {})).toBe('HELLO');
    });

    it('should evaluate LOWER function', () => {
      const expr: Expression = {
        type: 'function',
        name: 'lower',
        args: [{ type: 'literal', value: 'HELLO', dataType: 'string' }],
      };
      expect(evaluateExpression(expr, {})).toBe('hello');
    });

    it('should evaluate LENGTH function', () => {
      const expr: Expression = {
        type: 'function',
        name: 'length',
        args: [{ type: 'literal', value: 'hello', dataType: 'string' }],
      };
      expect(evaluateExpression(expr, {})).toBe(5);
    });

    it('should evaluate ABS function', () => {
      const expr: Expression = {
        type: 'function',
        name: 'abs',
        args: [{ type: 'literal', value: -42, dataType: 'number' }],
      };
      expect(evaluateExpression(expr, {})).toBe(42);
    });

    it('should evaluate COALESCE function', () => {
      const expr: Expression = {
        type: 'function',
        name: 'coalesce',
        args: [
          { type: 'literal', value: null, dataType: 'null' },
          { type: 'literal', value: null, dataType: 'null' },
          { type: 'literal', value: 'default', dataType: 'string' },
        ],
      };
      expect(evaluateExpression(expr, {})).toBe('default');
    });
  });

  describe('CASE expressions', () => {
    it('should evaluate searched CASE expression', () => {
      const row = { score: 85 };
      const expr: Expression = {
        type: 'case',
        when: [
          {
            condition: {
              type: 'binary',
              op: 'ge',
              left: { type: 'columnRef', column: 'score' },
              right: { type: 'literal', value: 90, dataType: 'number' },
            },
            result: { type: 'literal', value: 'A', dataType: 'string' },
          },
          {
            condition: {
              type: 'binary',
              op: 'ge',
              left: { type: 'columnRef', column: 'score' },
              right: { type: 'literal', value: 80, dataType: 'number' },
            },
            result: { type: 'literal', value: 'B', dataType: 'string' },
          },
        ],
        else: { type: 'literal', value: 'C', dataType: 'string' },
      };
      expect(evaluateExpression(expr, row)).toBe('B');
    });

    it('should evaluate simple CASE expression', () => {
      const row = { status: 'pending' };
      const expr: Expression = {
        type: 'case',
        operand: { type: 'columnRef', column: 'status' },
        when: [
          {
            value: { type: 'literal', value: 'active', dataType: 'string' },
            result: { type: 'literal', value: 1, dataType: 'number' },
          },
          {
            value: { type: 'literal', value: 'pending', dataType: 'string' },
            result: { type: 'literal', value: 2, dataType: 'number' },
          },
        ],
        else: { type: 'literal', value: 0, dataType: 'number' },
      };
      expect(evaluateExpression(expr, row)).toBe(2);
    });
  });
});

// =============================================================================
// WINDOW OPERATOR TESTS
// =============================================================================

describe('WindowOperator', () => {
  let ctx: ExecutionContext;

  beforeEach(() => {
    ctx = createMockContext();
  });

  describe('ROW_NUMBER function', () => {
    it('should assign sequential row numbers', async () => {
      const input = new MockOperator([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
      ]);

      const plan: WindowPlan = {
        type: 'window',
        id: 1,
        input: { type: 'scan', id: 0, table: 'users', source: 'btree', columns: ['id', 'name'] },
        windowFunctions: [
          {
            name: 'row_number',
            args: [],
            windowSpec: {},
            alias: 'rn',
          },
        ],
      };

      const operator = new WindowOperator(plan, input);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(3);
      expect(rows[0].rn).toBe(1);
      expect(rows[1].rn).toBe(2);
      expect(rows[2].rn).toBe(3);
    });

    it('should reset row numbers per partition', async () => {
      const input = new MockOperator([
        { id: 1, dept: 'Sales', name: 'Alice' },
        { id: 2, dept: 'Sales', name: 'Bob' },
        { id: 3, dept: 'Engineering', name: 'Charlie' },
        { id: 4, dept: 'Engineering', name: 'Diana' },
      ]);

      const plan: WindowPlan = {
        type: 'window',
        id: 1,
        input: { type: 'scan', id: 0, table: 'employees', source: 'btree', columns: ['id', 'dept', 'name'] },
        windowFunctions: [
          {
            name: 'row_number',
            args: [],
            windowSpec: {
              partitionBy: ['dept'],
            },
            alias: 'rn',
          },
        ],
      };

      const operator = new WindowOperator(plan, input);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(4);

      // Check Sales partition
      const salesRows = rows.filter(r => r.dept === 'Sales');
      expect(salesRows.length).toBe(2);
      expect(salesRows.map(r => r.rn).sort()).toEqual([1, 2]);

      // Check Engineering partition
      const engRows = rows.filter(r => r.dept === 'Engineering');
      expect(engRows.length).toBe(2);
      expect(engRows.map(r => r.rn).sort()).toEqual([1, 2]);
    });
  });

  describe('RANK function', () => {
    it('should assign ranks with gaps for ties', async () => {
      const input = new MockOperator([
        { id: 1, score: 100 },
        { id: 2, score: 90 },
        { id: 3, score: 90 },
        { id: 4, score: 80 },
      ]);

      const plan: WindowPlan = {
        type: 'window',
        id: 1,
        input: { type: 'scan', id: 0, table: 'students', source: 'btree', columns: ['id', 'score'] },
        windowFunctions: [
          {
            name: 'rank',
            args: [],
            windowSpec: {
              orderBy: [{ column: 'score', direction: 'desc' }],
            },
            alias: 'rnk',
          },
        ],
      };

      const operator = new WindowOperator(plan, input);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(4);
      // Score 100 -> rank 1
      // Score 90 -> rank 2 (two rows)
      // Score 80 -> rank 4 (skips 3 because of tie)
      const row100 = rows.find(r => r.score === 100);
      expect(row100!.rnk).toBe(1);

      const rows90 = rows.filter(r => r.score === 90);
      expect(rows90.every(r => r.rnk === 2)).toBe(true);

      const row80 = rows.find(r => r.score === 80);
      expect(row80!.rnk).toBe(4);
    });
  });

  describe('DENSE_RANK function', () => {
    it('should assign ranks without gaps for ties', async () => {
      const input = new MockOperator([
        { id: 1, score: 100 },
        { id: 2, score: 90 },
        { id: 3, score: 90 },
        { id: 4, score: 80 },
      ]);

      const plan: WindowPlan = {
        type: 'window',
        id: 1,
        input: { type: 'scan', id: 0, table: 'students', source: 'btree', columns: ['id', 'score'] },
        windowFunctions: [
          {
            name: 'dense_rank',
            args: [],
            windowSpec: {
              orderBy: [{ column: 'score', direction: 'desc' }],
            },
            alias: 'dense_rnk',
          },
        ],
      };

      const operator = new WindowOperator(plan, input);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(4);
      // Score 100 -> dense_rank 1
      // Score 90 -> dense_rank 2 (two rows)
      // Score 80 -> dense_rank 3 (no gap)
      const row100 = rows.find(r => r.score === 100);
      expect(row100!.dense_rnk).toBe(1);

      const rows90 = rows.filter(r => r.score === 90);
      expect(rows90.every(r => r.dense_rnk === 2)).toBe(true);

      const row80 = rows.find(r => r.score === 80);
      expect(row80!.dense_rnk).toBe(3);
    });
  });

  describe('NTILE function', () => {
    it('should divide rows into buckets', async () => {
      const input = new MockOperator([
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
        { id: 4, value: 40 },
        { id: 5, value: 50 },
        { id: 6, value: 60 },
      ]);

      const plan: WindowPlan = {
        type: 'window',
        id: 1,
        input: { type: 'scan', id: 0, table: 'data', source: 'btree', columns: ['id', 'value'] },
        windowFunctions: [
          {
            name: 'ntile',
            args: [{ type: 'literal', value: 3, dataType: 'number' }],
            windowSpec: {
              orderBy: [{ column: 'value', direction: 'asc' }],
            },
            alias: 'bucket',
          },
        ],
      };

      const operator = new WindowOperator(plan, input);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(6);
      // 6 rows divided into 3 buckets = 2 rows per bucket
      const buckets = rows.map(r => r.bucket);
      expect(buckets.filter(b => b === 1).length).toBe(2);
      expect(buckets.filter(b => b === 2).length).toBe(2);
      expect(buckets.filter(b => b === 3).length).toBe(2);
    });
  });

  describe('LAG function', () => {
    it('should access previous row value', async () => {
      const input = new MockOperator([
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
      ]);

      const plan: WindowPlan = {
        type: 'window',
        id: 1,
        input: { type: 'scan', id: 0, table: 'data', source: 'btree', columns: ['id', 'value'] },
        windowFunctions: [
          {
            name: 'lag',
            args: [
              { type: 'columnRef', column: 'value' },
              { type: 'literal', value: 1, dataType: 'number' },
              { type: 'literal', value: 0, dataType: 'number' },
            ],
            windowSpec: {
              orderBy: [{ column: 'id', direction: 'asc' }],
            },
            alias: 'prev_value',
          },
        ],
      };

      const operator = new WindowOperator(plan, input);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(3);
      expect(rows[0].prev_value).toBe(0); // default for first row
      expect(rows[1].prev_value).toBe(10);
      expect(rows[2].prev_value).toBe(20);
    });
  });

  describe('LEAD function', () => {
    it('should access next row value', async () => {
      const input = new MockOperator([
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
      ]);

      const plan: WindowPlan = {
        type: 'window',
        id: 1,
        input: { type: 'scan', id: 0, table: 'data', source: 'btree', columns: ['id', 'value'] },
        windowFunctions: [
          {
            name: 'lead',
            args: [
              { type: 'columnRef', column: 'value' },
              { type: 'literal', value: 1, dataType: 'number' },
              { type: 'literal', value: 0, dataType: 'number' },
            ],
            windowSpec: {
              orderBy: [{ column: 'id', direction: 'asc' }],
            },
            alias: 'next_value',
          },
        ],
      };

      const operator = new WindowOperator(plan, input);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(3);
      expect(rows[0].next_value).toBe(20);
      expect(rows[1].next_value).toBe(30);
      expect(rows[2].next_value).toBe(0); // default for last row
    });
  });

  describe('FIRST_VALUE and LAST_VALUE functions', () => {
    it('should return first and last values in frame', async () => {
      const input = new MockOperator([
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
      ]);

      const plan: WindowPlan = {
        type: 'window',
        id: 1,
        input: { type: 'scan', id: 0, table: 'data', source: 'btree', columns: ['id', 'value'] },
        windowFunctions: [
          {
            name: 'first_value',
            args: [{ type: 'columnRef', column: 'value' }],
            windowSpec: {
              orderBy: [{ column: 'id', direction: 'asc' }],
              frame: {
                mode: 'rows',
                start: { type: 'unboundedPreceding' },
                end: { type: 'unboundedFollowing' },
              },
            },
            alias: 'first_val',
          },
          {
            name: 'last_value',
            args: [{ type: 'columnRef', column: 'value' }],
            windowSpec: {
              orderBy: [{ column: 'id', direction: 'asc' }],
              frame: {
                mode: 'rows',
                start: { type: 'unboundedPreceding' },
                end: { type: 'unboundedFollowing' },
              },
            },
            alias: 'last_val',
          },
        ],
      };

      const operator = new WindowOperator(plan, input);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(3);
      // With full frame, all rows should see first=10 and last=30
      expect(rows.every(r => r.first_val === 10)).toBe(true);
      expect(rows.every(r => r.last_val === 30)).toBe(true);
    });
  });

  describe('window aggregate functions', () => {
    it('should compute SUM over window frame', async () => {
      const input = new MockOperator([
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
      ]);

      const plan: WindowPlan = {
        type: 'window',
        id: 1,
        input: { type: 'scan', id: 0, table: 'data', source: 'btree', columns: ['id', 'value'] },
        windowFunctions: [
          {
            name: 'sum',
            args: [{ type: 'columnRef', column: 'value' }],
            windowSpec: {
              orderBy: [{ column: 'id', direction: 'asc' }],
              frame: {
                mode: 'rows',
                start: { type: 'unboundedPreceding' },
                end: { type: 'currentRow' },
              },
            },
            alias: 'running_sum',
          },
        ],
      };

      const operator = new WindowOperator(plan, input);
      const rows = await collectRows(operator, ctx);

      expect(rows.length).toBe(3);
      expect(rows[0].running_sum).toBe(10);  // 10
      expect(rows[1].running_sum).toBe(30);  // 10 + 20
      expect(rows[2].running_sum).toBe(60);  // 10 + 20 + 30
    });
  });
});

describe('containsWindowFunction', () => {
  it('should detect window function in expression', () => {
    const exprWithWindow: Expression = {
      type: 'function',
      name: 'row_number',
      args: [],
      over: {},
    } as Expression & { over: WindowSpec };

    expect(containsWindowFunction(exprWithWindow)).toBe(true);
  });

  it('should return false for non-window expression', () => {
    const expr: Expression = {
      type: 'function',
      name: 'upper',
      args: [{ type: 'literal', value: 'hello', dataType: 'string' }],
    };

    expect(containsWindowFunction(expr)).toBe(false);
  });
});

// =============================================================================
// CTE OPERATOR TESTS
// =============================================================================

describe('CTE Operators', () => {
  describe('createCTEContext', () => {
    it('should create context with default limits', () => {
      const baseCtx = createMockContext();
      const cteCtx = createCTEContext(baseCtx);

      expect(cteCtx.materializedCTEs).toBeInstanceOf(Map);
      expect(cteCtx.materializedCTEs.size).toBe(0);
      expect(cteCtx.recursiveLimits.maxIterations).toBe(1000);
      expect(cteCtx.recursiveLimits.maxRows).toBe(100000);
    });

    it('should create context with custom limits', () => {
      const baseCtx = createMockContext();
      const cteCtx = createCTEContext(baseCtx, {
        maxIterations: 500,
        maxRows: 50000,
      });

      expect(cteCtx.recursiveLimits.maxIterations).toBe(500);
      expect(cteCtx.recursiveLimits.maxRows).toBe(50000);
    });
  });

  describe('CTEScanOperator', () => {
    it('should scan materialized CTE rows', async () => {
      const baseCtx = createMockContext();
      const cteCtx = createCTEContext(baseCtx);

      // Materialize a CTE
      cteCtx.materializedCTEs.set('employees', {
        name: 'employees',
        rows: [
          { id: 1, name: 'Alice' },
          { id: 2, name: 'Bob' },
        ],
        columns: ['id', 'name'],
      });

      const operator = new CTEScanOperator('employees');
      const rows = await collectRows(operator, cteCtx);

      expect(rows.length).toBe(2);
      expect(rows[0]).toEqual({ id: 1, name: 'Alice' });
      expect(rows[1]).toEqual({ id: 2, name: 'Bob' });
    });

    it('should throw error for non-materialized CTE', async () => {
      const cteCtx = createCTEContext(createMockContext());
      const operator = new CTEScanOperator('nonexistent');

      await expect(operator.open(cteCtx)).rejects.toThrow(/CTE.*nonexistent.*not materialized/i);
    });

    it('should apply alias to scanned rows', async () => {
      const baseCtx = createMockContext();
      const cteCtx = createCTEContext(baseCtx);

      cteCtx.materializedCTEs.set('employees', {
        name: 'employees',
        rows: [{ id: 1, name: 'Alice' }],
        columns: ['id', 'name'],
      });

      const operator = new CTEScanOperator('employees', 'emp');
      const rows = await collectRows(operator, cteCtx);

      expect(rows.length).toBe(1);
      // Should have both unqualified and alias-qualified columns
      expect(rows[0].id).toBe(1);
      expect(rows[0].name).toBe('Alice');
      expect(rows[0]['emp.id']).toBe(1);
      expect(rows[0]['emp.name']).toBe('Alice');
    });
  });

  describe('executeSimpleCTE', () => {
    it('should execute simple CTE', async () => {
      const cteCtx = createCTEContext(createMockContext());

      const tables = new Map<string, Row[]>();
      tables.set('employees', [
        { id: 1, name: 'Alice', dept_id: 1 },
        { id: 2, name: 'Bob', dept_id: 2 },
        { id: 3, name: 'Charlie', dept_id: 1 },
      ]);

      const queryExecutor = createMockQueryExecutor(tables);

      const cte: CTEDefinition = {
        name: 'dept_employees',
        query: 'SELECT * FROM employees',
        recursive: false,
      };

      const materialized = await executeSimpleCTE(cte, queryExecutor, cteCtx);

      expect(materialized.name).toBe('dept_employees');
      expect(materialized.rows.length).toBe(3);
      expect(materialized.columns).toEqual(['id', 'name', 'dept_id']);
    });

    it('should apply column aliases', async () => {
      const cteCtx = createCTEContext(createMockContext());

      const tables = new Map<string, Row[]>();
      tables.set('employees', [
        { id: 1, name: 'Alice' },
      ]);

      const queryExecutor = createMockQueryExecutor(tables);

      const cte: CTEDefinition = {
        name: 'renamed',
        columns: ['emp_id', 'emp_name'],
        query: 'SELECT * FROM employees',
        recursive: false,
      };

      const materialized = await executeSimpleCTE(cte, queryExecutor, cteCtx);

      expect(materialized.columns).toEqual(['emp_id', 'emp_name']);
      expect(materialized.rows[0]).toEqual({ emp_id: 1, emp_name: 'Alice' });
    });
  });

  describe('executeRecursiveCTE', () => {
    it('should execute recursive CTE for hierarchy', async () => {
      const cteCtx = createCTEContext(createMockContext());

      // Mock a simple recursive query executor
      let iteration = 0;
      const queryExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
        // Anchor query
        if (sql.includes('manager_id IS NULL')) {
          return [{ id: 1, name: 'CEO', level: 0 }];
        }

        // Recursive query - use materialized CTE
        const cteCtxTyped = ctx as CTEExecutionContext;
        const workingTable = cteCtxTyped.materializedCTEs.get('org_chart')?.rows || [];

        if (iteration === 0) {
          iteration++;
          return [
            { id: 2, name: 'VP1', level: 1 },
            { id: 3, name: 'VP2', level: 1 },
          ];
        }

        if (iteration === 1) {
          iteration++;
          return [
            { id: 4, name: 'Manager1', level: 2 },
          ];
        }

        return [];
      };

      const cte: CTEDefinition = {
        name: 'org_chart',
        columns: ['id', 'name', 'level'],
        query: 'anchor UNION ALL recursive',
        recursive: true,
        anchorQuery: 'SELECT id, name, 0 FROM employees WHERE manager_id IS NULL',
        recursiveQuery: 'SELECT e.id, e.name, oc.level + 1 FROM employees e JOIN org_chart oc ON e.manager_id = oc.id',
      };

      const materialized = await executeRecursiveCTE(cte, queryExecutor, cteCtx);

      expect(materialized.name).toBe('org_chart');
      expect(materialized.rows.length).toBe(4); // CEO + 2 VPs + 1 Manager
      expect(materialized.rows[0]).toEqual({ id: 1, name: 'CEO', level: 0 });
    });

    it('should throw error on missing anchor/recursive queries', async () => {
      const cteCtx = createCTEContext(createMockContext());
      const queryExecutor = async () => [];

      const cte: CTEDefinition = {
        name: 'bad_cte',
        query: 'SELECT 1',
        recursive: true,
        // Missing anchorQuery and recursiveQuery
      };

      await expect(executeRecursiveCTE(cte, queryExecutor, cteCtx)).rejects.toThrow(
        "Recursive CTE 'bad_cte' must have both anchor and recursive queries"
      );
    });

    it('should respect max iterations limit', async () => {
      const cteCtx = createCTEContext(createMockContext(), { maxIterations: 3 });

      // This executor always returns a new row (infinite loop)
      let rowId = 1;
      const queryExecutor = async (sql: string): Promise<Row[]> => {
        if (sql.includes('anchor')) {
          return [{ id: rowId++, value: 1 }];
        }
        return [{ id: rowId++, value: rowId }];
      };

      const cte: CTEDefinition = {
        name: 'infinite',
        query: 'anchor UNION ALL recursive',
        recursive: true,
        anchorQuery: 'SELECT anchor',
        recursiveQuery: 'SELECT recursive',
      };

      await expect(executeRecursiveCTE(cte, queryExecutor, cteCtx)).rejects.toThrow(
        'exceeded maximum iterations (3)'
      );
    });
  });

  describe('executeWithClause', () => {
    it('should execute multiple CTEs in order', async () => {
      const baseCtx = createMockContext();

      const tables = new Map<string, Row[]>();
      tables.set('employees', [
        { id: 1, name: 'Alice', dept_id: 1 },
        { id: 2, name: 'Bob', dept_id: 2 },
      ]);
      tables.set('departments', [
        { id: 1, name: 'Engineering' },
        { id: 2, name: 'Sales' },
      ]);

      const queryExecutor = createMockQueryExecutor(tables);

      const withClause: WithClause = {
        type: 'with',
        recursive: false,
        ctes: [
          {
            name: 'eng_employees',
            query: 'SELECT * FROM employees',
            recursive: false,
          },
          {
            name: 'all_depts',
            query: 'SELECT * FROM departments',
            recursive: false,
          },
        ],
      };

      const cteCtx = await executeWithClause(withClause, queryExecutor, baseCtx);

      expect(isCTEMaterialized(cteCtx, 'eng_employees')).toBe(true);
      expect(isCTEMaterialized(cteCtx, 'all_depts')).toBe(true);

      const engRows = getCTERows(cteCtx, 'eng_employees');
      expect(engRows?.length).toBe(2);

      const deptRows = getCTERows(cteCtx, 'all_depts');
      expect(deptRows?.length).toBe(2);
    });

    it('should allow later CTE to reference earlier CTE', async () => {
      const baseCtx = createMockContext();

      const tables = new Map<string, Row[]>();
      tables.set('employees', [
        { id: 1, name: 'Alice', salary: 100000 },
        { id: 2, name: 'Bob', salary: 80000 },
      ]);

      const queryExecutor = createMockQueryExecutor(tables);

      const withClause: WithClause = {
        type: 'with',
        recursive: false,
        ctes: [
          {
            name: 'all_employees',
            query: 'SELECT * FROM employees',
            recursive: false,
          },
          {
            name: 'high_earners',
            query: 'SELECT * FROM all_employees', // References first CTE
            recursive: false,
          },
        ],
      };

      const cteCtx = await executeWithClause(withClause, queryExecutor, baseCtx);

      // Second CTE should have access to first CTE's results
      const highEarners = getCTERows(cteCtx, 'high_earners');
      expect(highEarners?.length).toBe(2);
    });
  });

  describe('CTE utility functions', () => {
    it('getCTERows should return rows or undefined', () => {
      const cteCtx = createCTEContext(createMockContext());

      cteCtx.materializedCTEs.set('test', {
        name: 'test',
        rows: [{ id: 1 }],
        columns: ['id'],
      });

      expect(getCTERows(cteCtx, 'test')).toEqual([{ id: 1 }]);
      expect(getCTERows(cteCtx, 'nonexistent')).toBeUndefined();
    });

    it('isCTEMaterialized should check existence', () => {
      const cteCtx = createCTEContext(createMockContext());

      cteCtx.materializedCTEs.set('test', {
        name: 'test',
        rows: [],
        columns: [],
      });

      expect(isCTEMaterialized(cteCtx, 'test')).toBe(true);
      expect(isCTEMaterialized(cteCtx, 'nonexistent')).toBe(false);
    });

    it('getCTEColumns should return columns or undefined', () => {
      const cteCtx = createCTEContext(createMockContext());

      cteCtx.materializedCTEs.set('test', {
        name: 'test',
        rows: [],
        columns: ['id', 'name'],
      });

      expect(getCTEColumns(cteCtx, 'test')).toEqual(['id', 'name']);
      expect(getCTEColumns(cteCtx, 'nonexistent')).toBeUndefined();
    });
  });
});

// =============================================================================
// PREDICATE EVALUATION TESTS
// =============================================================================

describe('evaluatePredicate', () => {
  describe('comparison predicates', () => {
    it('should evaluate equality', () => {
      const row = { value: 42 };
      const pred: Predicate = {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'value' },
        right: { type: 'literal', value: 42, dataType: 'number' },
      };
      expect(evaluatePredicate(pred, row)).toBe(true);
    });

    it('should evaluate inequality', () => {
      const row = { value: 42 };
      const pred: Predicate = {
        type: 'comparison',
        op: 'ne',
        left: { type: 'columnRef', column: 'value' },
        right: { type: 'literal', value: 100, dataType: 'number' },
      };
      expect(evaluatePredicate(pred, row)).toBe(true);
    });

    it('should handle null comparisons', () => {
      const row = { value: null };
      const pred: Predicate = {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'value' },
        right: { type: 'literal', value: 42, dataType: 'number' },
      };
      // NULL = anything returns false (SQL semantics)
      expect(evaluatePredicate(pred, row)).toBe(false);
    });
  });

  describe('logical predicates', () => {
    it('should evaluate AND', () => {
      const row = { a: 1, b: 2 };
      const pred: Predicate = {
        type: 'logical',
        op: 'and',
        operands: [
          {
            type: 'comparison',
            op: 'eq',
            left: { type: 'columnRef', column: 'a' },
            right: { type: 'literal', value: 1, dataType: 'number' },
          },
          {
            type: 'comparison',
            op: 'eq',
            left: { type: 'columnRef', column: 'b' },
            right: { type: 'literal', value: 2, dataType: 'number' },
          },
        ],
      };
      expect(evaluatePredicate(pred, row)).toBe(true);
    });

    it('should short-circuit AND on false', () => {
      const row = { a: 0, b: 2 };
      const pred: Predicate = {
        type: 'logical',
        op: 'and',
        operands: [
          {
            type: 'comparison',
            op: 'eq',
            left: { type: 'columnRef', column: 'a' },
            right: { type: 'literal', value: 1, dataType: 'number' },
          },
          {
            type: 'comparison',
            op: 'eq',
            left: { type: 'columnRef', column: 'b' },
            right: { type: 'literal', value: 2, dataType: 'number' },
          },
        ],
      };
      expect(evaluatePredicate(pred, row)).toBe(false);
    });

    it('should evaluate OR', () => {
      const row = { status: 'active' };
      const pred: Predicate = {
        type: 'logical',
        op: 'or',
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
            left: { type: 'columnRef', column: 'status' },
            right: { type: 'literal', value: 'pending', dataType: 'string' },
          },
        ],
      };
      expect(evaluatePredicate(pred, row)).toBe(true);
    });

    it('should evaluate NOT', () => {
      const row = { deleted: false };
      const pred: Predicate = {
        type: 'logical',
        op: 'not',
        operands: [
          {
            type: 'comparison',
            op: 'eq',
            left: { type: 'columnRef', column: 'deleted' },
            right: { type: 'literal', value: true, dataType: 'boolean' },
          },
        ],
      };
      expect(evaluatePredicate(pred, row)).toBe(true);
    });
  });

  describe('BETWEEN predicate', () => {
    it('should evaluate BETWEEN inclusive', () => {
      const row = { price: 50 };
      const pred: Predicate = {
        type: 'between',
        expr: { type: 'columnRef', column: 'price' },
        low: { type: 'literal', value: 50, dataType: 'number' },
        high: { type: 'literal', value: 100, dataType: 'number' },
      };
      expect(evaluatePredicate(pred, row)).toBe(true);
    });

    it('should return false when outside range', () => {
      const row = { price: 150 };
      const pred: Predicate = {
        type: 'between',
        expr: { type: 'columnRef', column: 'price' },
        low: { type: 'literal', value: 50, dataType: 'number' },
        high: { type: 'literal', value: 100, dataType: 'number' },
      };
      expect(evaluatePredicate(pred, row)).toBe(false);
    });
  });

  describe('IN predicate', () => {
    it('should evaluate IN list', () => {
      const row = { status: 'active' };
      const pred: Predicate = {
        type: 'in',
        expr: { type: 'columnRef', column: 'status' },
        values: [
          { type: 'literal', value: 'active', dataType: 'string' },
          { type: 'literal', value: 'pending', dataType: 'string' },
        ],
      };
      expect(evaluatePredicate(pred, row)).toBe(true);
    });

    it('should return false when not in list', () => {
      const row = { status: 'deleted' };
      const pred: Predicate = {
        type: 'in',
        expr: { type: 'columnRef', column: 'status' },
        values: [
          { type: 'literal', value: 'active', dataType: 'string' },
          { type: 'literal', value: 'pending', dataType: 'string' },
        ],
      };
      expect(evaluatePredicate(pred, row)).toBe(false);
    });
  });

  describe('IS NULL predicate', () => {
    it('should evaluate IS NULL', () => {
      const row = { email: null };
      const pred: Predicate = {
        type: 'isNull',
        expr: { type: 'columnRef', column: 'email' },
        isNot: false,
      };
      expect(evaluatePredicate(pred, row)).toBe(true);
    });

    it('should evaluate IS NOT NULL', () => {
      const row = { email: 'test@example.com' };
      const pred: Predicate = {
        type: 'isNull',
        expr: { type: 'columnRef', column: 'email' },
        isNot: true,
      };
      expect(evaluatePredicate(pred, row)).toBe(true);
    });
  });
});
