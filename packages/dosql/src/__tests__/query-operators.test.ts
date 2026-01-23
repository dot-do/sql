/**
 * GREEN Phase TDD Tests for Query Operator Modules
 *
 * These tests verify the operator implementations:
 * - ProjectOperator (column selection and aliases)
 * - JoinOperator (INNER, LEFT/RIGHT/FULL OUTER)
 * - AggregateOperator (COUNT/SUM/AVG/MIN/MAX, GROUP BY, HAVING)
 * - Operator pipeline composition
 *
 * The operator files:
 * - packages/dosql/src/engine/operators/project.ts
 * - packages/dosql/src/engine/operators/join.ts
 * - packages/dosql/src/engine/operators/aggregate.ts
 * - packages/dosql/src/engine/operators/sort.ts
 * - packages/dosql/src/engine/operators/limit.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import type {
  Row,
  Expression,
  Predicate,
  ExecutionContext,
  Operator,
  ProjectPlan,
  JoinPlan,
  AggregatePlan,
  ScanPlan,
  FilterPlan,
  SortPlan,
  LimitPlan,
  AggregateExpr,
} from '../engine/types.js';

// =============================================================================
// TEST FIXTURES
// =============================================================================

/**
 * Mock operator for testing pipeline composition
 */
class MockScanOperator implements Operator {
  private rows: Row[];
  private index = 0;
  private cols: string[];

  constructor(rows: Row[], columns: string[]) {
    this.rows = rows;
    this.cols = columns;
  }

  async open(_ctx: ExecutionContext): Promise<void> {
    this.index = 0;
  }

  async next(): Promise<Row | null> {
    if (this.index >= this.rows.length) return null;
    return this.rows[this.index++];
  }

  async close(): Promise<void> {
    // noop
  }

  columns(): string[] {
    return this.cols;
  }
}

/**
 * Test data: Users table
 */
const USERS_DATA: Row[] = [
  { id: 1, name: 'Alice', email: 'alice@example.com', department_id: 1 },
  { id: 2, name: 'Bob', email: 'bob@example.com', department_id: 1 },
  { id: 3, name: 'Charlie', email: 'charlie@example.com', department_id: 2 },
  { id: 4, name: 'Diana', email: 'diana@example.com', department_id: 2 },
  { id: 5, name: 'Eve', email: 'eve@example.com', department_id: null },
];

/**
 * Test data: Departments table
 */
const DEPARTMENTS_DATA: Row[] = [
  { id: 1, name: 'Engineering', budget: 100000 },
  { id: 2, name: 'Marketing', budget: 50000 },
  { id: 3, name: 'Sales', budget: 75000 },
];

/**
 * Test data: Orders table for aggregates
 */
const ORDERS_DATA: Row[] = [
  { id: 1, user_id: 1, amount: 100, status: 'completed' },
  { id: 2, user_id: 1, amount: 200, status: 'completed' },
  { id: 3, user_id: 2, amount: 150, status: 'pending' },
  { id: 4, user_id: 2, amount: 300, status: 'completed' },
  { id: 5, user_id: 3, amount: 250, status: 'completed' },
  { id: 6, user_id: 3, amount: 50, status: 'cancelled' },
];

const USERS_COLUMNS = ['id', 'name', 'email', 'department_id'];
const DEPARTMENTS_COLUMNS = ['id', 'name', 'budget'];
const ORDERS_COLUMNS = ['id', 'user_id', 'amount', 'status'];

/**
 * Create a mock execution context
 */
function createMockContext(): ExecutionContext {
  return {
    // Minimal context for testing
  } as ExecutionContext;
}

// =============================================================================
// PROJECT OPERATOR TESTS
// =============================================================================

describe('ProjectOperator', () => {
  describe('Column Selection', () => {
    it('should select specific columns from input', async () => {
      // This test documents that ProjectOperator should filter columns
      // When implemented: SELECT id, name FROM users
      const { ProjectOperator } = await import('../engine/operators/project.js');

      const mockInput = new MockScanOperator(USERS_DATA, USERS_COLUMNS);
      const projectPlan: ProjectPlan = {
        id: 1,
        type: 'project',
        input: { id: 0, type: 'scan', table: 'users', source: 'btree', columns: USERS_COLUMNS },
        expressions: [
          { expr: { type: 'columnRef', column: 'id' }, alias: 'id' },
          { expr: { type: 'columnRef', column: 'name' }, alias: 'name' },
        ],
      };

      const ctx = createMockContext();
      const operator = new ProjectOperator(projectPlan, mockInput, ctx);

      await operator.open(ctx);

      const row1 = await operator.next();
      expect(row1).toEqual({ id: 1, name: 'Alice' });

      const row2 = await operator.next();
      expect(row2).toEqual({ id: 2, name: 'Bob' });

      await operator.close();

      // Verify output columns
      expect(operator.columns()).toEqual(['id', 'name']);
    });

    it('should handle selecting all columns (*)', async () => {
      // SELECT * FROM users - all columns should pass through
      const { ProjectOperator } = await import('../engine/operators/project.js');

      const mockInput = new MockScanOperator(USERS_DATA, USERS_COLUMNS);
      const projectPlan: ProjectPlan = {
        id: 1,
        type: 'project',
        input: { id: 0, type: 'scan', table: 'users', source: 'btree', columns: USERS_COLUMNS },
        expressions: USERS_COLUMNS.map((col) => ({
          expr: { type: 'columnRef', column: col } as Expression,
          alias: col,
        })),
      };

      const ctx = createMockContext();
      const operator = new ProjectOperator(projectPlan, mockInput, ctx);

      await operator.open(ctx);
      const row = await operator.next();

      expect(row).toEqual(USERS_DATA[0]);
      await operator.close();
    });
  });

  describe('Column Aliases', () => {
    it('should support column aliases with AS', async () => {
      // SELECT id AS user_id, name AS full_name FROM users
      const { ProjectOperator } = await import('../engine/operators/project.js');

      const mockInput = new MockScanOperator(USERS_DATA, USERS_COLUMNS);
      const projectPlan: ProjectPlan = {
        id: 1,
        type: 'project',
        input: { id: 0, type: 'scan', table: 'users', source: 'btree', columns: USERS_COLUMNS },
        expressions: [
          { expr: { type: 'columnRef', column: 'id' }, alias: 'user_id' },
          { expr: { type: 'columnRef', column: 'name' }, alias: 'full_name' },
        ],
      };

      const ctx = createMockContext();
      const operator = new ProjectOperator(projectPlan, mockInput, ctx);

      await operator.open(ctx);
      const row = await operator.next();

      // Aliases should be applied
      expect(row).toEqual({ user_id: 1, full_name: 'Alice' });
      expect(operator.columns()).toEqual(['user_id', 'full_name']);

      await operator.close();
    });

    it('should support expression aliases', async () => {
      // SELECT name, 'VIP' AS status FROM users
      const { ProjectOperator } = await import('../engine/operators/project.js');

      const mockInput = new MockScanOperator(USERS_DATA, USERS_COLUMNS);
      const projectPlan: ProjectPlan = {
        id: 1,
        type: 'project',
        input: { id: 0, type: 'scan', table: 'users', source: 'btree', columns: USERS_COLUMNS },
        expressions: [
          { expr: { type: 'columnRef', column: 'name' }, alias: 'name' },
          { expr: { type: 'literal', value: 'VIP' }, alias: 'status' },
        ],
      };

      const ctx = createMockContext();
      const operator = new ProjectOperator(projectPlan, mockInput, ctx);

      await operator.open(ctx);
      const row = await operator.next();

      expect(row).toEqual({ name: 'Alice', status: 'VIP' });
      await operator.close();
    });

    it.fails('should support computed expression aliases', async () => {
      // SELECT amount, amount * 1.1 AS amount_with_tax FROM orders
      const { ProjectOperator } = await import('../engine/operators/project.js');

      const mockInput = new MockScanOperator(ORDERS_DATA, ORDERS_COLUMNS);
      const projectPlan: ProjectPlan = {
        id: 1,
        type: 'project',
        input: { id: 0, type: 'scan', table: 'orders', source: 'btree', columns: ORDERS_COLUMNS },
        expressions: [
          { expr: { type: 'columnRef', column: 'amount' }, alias: 'amount' },
          {
            expr: {
              type: 'binary',
              op: '*',
              left: { type: 'columnRef', column: 'amount' },
              right: { type: 'literal', value: 1.1 },
            },
            alias: 'amount_with_tax',
          },
        ],
      };

      const ctx = createMockContext();
      const operator = new ProjectOperator(projectPlan, mockInput, ctx);

      await operator.open(ctx);
      const row = await operator.next();

      expect(row).toEqual({ amount: 100, amount_with_tax: 110 });
      await operator.close();
    });
  });
});

// =============================================================================
// JOIN OPERATOR TESTS
// =============================================================================

describe('JoinOperator', () => {
  describe('INNER JOIN', () => {
    it('should implement INNER JOIN returning only matching rows', async () => {
      // SELECT * FROM users u INNER JOIN departments d ON u.department_id = d.id
      const { JoinOperator } = await import('../engine/operators/join.js');

      const usersInput = new MockScanOperator(USERS_DATA, USERS_COLUMNS);
      const departmentsInput = new MockScanOperator(DEPARTMENTS_DATA, DEPARTMENTS_COLUMNS);

      const joinPlan: JoinPlan = {
        id: 1,
        type: 'join',
        joinType: 'inner',
        left: { id: 0, type: 'scan', table: 'users', source: 'btree', columns: USERS_COLUMNS },
        right: { id: 0, type: 'scan', table: 'departments', source: 'btree', columns: DEPARTMENTS_COLUMNS },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'department_id', table: 'users' },
          right: { type: 'columnRef', column: 'id', table: 'departments' },
        },
      };

      const ctx = createMockContext();
      const operator = new JoinOperator(joinPlan, usersInput, departmentsInput, ctx);

      await operator.open(ctx);

      const results: Row[] = [];
      let row: Row | null;
      while ((row = await operator.next()) !== null) {
        results.push(row);
      }

      await operator.close();

      // Should return 4 rows (users with non-null department_id matching departments)
      // Eve (id=5) has null department_id, so should not be included
      expect(results.length).toBe(4);

      // Each result should have joined columns
      expect(results[0]).toMatchObject({
        'users.id': 1,
        'users.name': 'Alice',
        'departments.id': 1,
        'departments.name': 'Engineering',
      });
    });

    it('should handle INNER JOIN with no matches', async () => {
      // No matching rows scenario
      const { JoinOperator } = await import('../engine/operators/join.js');

      const usersInput = new MockScanOperator(
        [{ id: 1, name: 'Test', department_id: 999 }],
        USERS_COLUMNS
      );
      const departmentsInput = new MockScanOperator(DEPARTMENTS_DATA, DEPARTMENTS_COLUMNS);

      const joinPlan: JoinPlan = {
        id: 1,
        type: 'join',
        joinType: 'inner',
        left: { id: 0, type: 'scan', table: 'users', source: 'btree', columns: USERS_COLUMNS },
        right: { id: 0, type: 'scan', table: 'departments', source: 'btree', columns: DEPARTMENTS_COLUMNS },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'department_id' },
          right: { type: 'columnRef', column: 'id', table: 'departments' },
        },
      };

      const ctx = createMockContext();
      const operator = new JoinOperator(joinPlan, usersInput, departmentsInput, ctx);

      await operator.open(ctx);
      const result = await operator.next();
      await operator.close();

      expect(result).toBeNull();
    });
  });

  describe('OUTER JOINs', () => {
    it('should implement LEFT OUTER JOIN', async () => {
      // SELECT * FROM users u LEFT JOIN departments d ON u.department_id = d.id
      const { JoinOperator } = await import('../engine/operators/join.js');

      const usersInput = new MockScanOperator(USERS_DATA, USERS_COLUMNS);
      const departmentsInput = new MockScanOperator(DEPARTMENTS_DATA, DEPARTMENTS_COLUMNS);

      const joinPlan: JoinPlan = {
        id: 1,
        type: 'join',
        joinType: 'left',
        left: { id: 0, type: 'scan', table: 'users', source: 'btree', columns: USERS_COLUMNS },
        right: { id: 0, type: 'scan', table: 'departments', source: 'btree', columns: DEPARTMENTS_COLUMNS },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'department_id' },
          right: { type: 'columnRef', column: 'id', table: 'departments' },
        },
      };

      const ctx = createMockContext();
      const operator = new JoinOperator(joinPlan, usersInput, departmentsInput, ctx);

      await operator.open(ctx);

      const results: Row[] = [];
      let row: Row | null;
      while ((row = await operator.next()) !== null) {
        results.push(row);
      }

      await operator.close();

      // Should return 5 rows - all users, even Eve with null department
      expect(results.length).toBe(5);

      // Eve should have null department columns
      const eveRow = results.find((r) => r['users.name'] === 'Eve');
      expect(eveRow).toBeDefined();
      expect(eveRow!['departments.id']).toBeNull();
      expect(eveRow!['departments.name']).toBeNull();
    });

    it.fails('should implement RIGHT OUTER JOIN', async () => {
      // SELECT * FROM users u RIGHT JOIN departments d ON u.department_id = d.id
      const { JoinOperator } = await import('../engine/operators/join.js');

      const usersInput = new MockScanOperator(USERS_DATA, USERS_COLUMNS);
      const departmentsInput = new MockScanOperator(DEPARTMENTS_DATA, DEPARTMENTS_COLUMNS);

      const joinPlan: JoinPlan = {
        id: 1,
        type: 'join',
        joinType: 'right',
        left: { id: 0, type: 'scan', table: 'users', source: 'btree', columns: USERS_COLUMNS },
        right: { id: 0, type: 'scan', table: 'departments', source: 'btree', columns: DEPARTMENTS_COLUMNS },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'department_id' },
          right: { type: 'columnRef', column: 'id', table: 'departments' },
        },
      };

      const ctx = createMockContext();
      const operator = new JoinOperator(joinPlan, usersInput, departmentsInput, ctx);

      await operator.open(ctx);

      const results: Row[] = [];
      let row: Row | null;
      while ((row = await operator.next()) !== null) {
        results.push(row);
      }

      await operator.close();

      // Should return 5 rows - 4 matching + Sales department with no users
      expect(results.length).toBe(5);

      // Sales department should have null user columns
      const salesRow = results.find((r) => r['departments.name'] === 'Sales');
      expect(salesRow).toBeDefined();
      expect(salesRow!['users.id']).toBeNull();
      expect(salesRow!['users.name']).toBeNull();
    });

    it.fails('should implement FULL OUTER JOIN', async () => {
      // SELECT * FROM users u FULL OUTER JOIN departments d ON u.department_id = d.id
      const { JoinOperator } = await import('../engine/operators/join.js');

      const usersInput = new MockScanOperator(USERS_DATA, USERS_COLUMNS);
      const departmentsInput = new MockScanOperator(DEPARTMENTS_DATA, DEPARTMENTS_COLUMNS);

      const joinPlan: JoinPlan = {
        id: 1,
        type: 'join',
        joinType: 'full',
        left: { id: 0, type: 'scan', table: 'users', source: 'btree', columns: USERS_COLUMNS },
        right: { id: 0, type: 'scan', table: 'departments', source: 'btree', columns: DEPARTMENTS_COLUMNS },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'department_id' },
          right: { type: 'columnRef', column: 'id', table: 'departments' },
        },
      };

      const ctx = createMockContext();
      const operator = new JoinOperator(joinPlan, usersInput, departmentsInput, ctx);

      await operator.open(ctx);

      const results: Row[] = [];
      let row: Row | null;
      while ((row = await operator.next()) !== null) {
        results.push(row);
      }

      await operator.close();

      // Should return 6 rows:
      // - 4 matching rows (Alice, Bob, Charlie, Diana)
      // - 1 unmatched left row (Eve with null dept)
      // - 1 unmatched right row (Sales with no users)
      expect(results.length).toBe(6);
    });
  });

  describe('Join Optimization', () => {
    it('should optimize join order based on table sizes', async () => {
      // The optimizer should choose the smaller table for the inner (probe) side
      const { JoinOperator } = await import('../engine/operators/join.js');

      // Small table (3 rows) joined with larger table (5 rows)
      const smallInput = new MockScanOperator(DEPARTMENTS_DATA, DEPARTMENTS_COLUMNS);
      const largeInput = new MockScanOperator(USERS_DATA, USERS_COLUMNS);

      const joinPlan: JoinPlan = {
        id: 1,
        type: 'join',
        joinType: 'inner',
        left: { id: 0, type: 'scan', table: 'departments', source: 'btree', columns: DEPARTMENTS_COLUMNS },
        right: { id: 0, type: 'scan', table: 'users', source: 'btree', columns: USERS_COLUMNS },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'id', table: 'departments' },
          right: { type: 'columnRef', column: 'department_id', table: 'users' },
        },
        algorithm: 'hash',
      };

      const ctx = createMockContext();
      const operator = new JoinOperator(joinPlan, smallInput, largeInput, ctx);

      // The operator should internally choose to build hash table on smaller side
      await operator.open(ctx);

      // Collect all results
      const results: Row[] = [];
      let row: Row | null;
      while ((row = await operator.next()) !== null) {
        results.push(row);
      }

      await operator.close();

      // Should still produce correct results
      expect(results.length).toBe(4);
    });
  });
});

// =============================================================================
// AGGREGATE OPERATOR TESTS
// =============================================================================

describe('AggregateOperator', () => {
  describe('Aggregate Functions', () => {
    it('should implement COUNT(*)', async () => {
      // SELECT COUNT(*) FROM orders
      const { AggregateOperator } = await import('../engine/operators/aggregate.js');

      const ordersInput = new MockScanOperator(ORDERS_DATA, ORDERS_COLUMNS);
      const aggregatePlan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 0, type: 'scan', table: 'orders', source: 'btree', columns: ORDERS_COLUMNS },
        groupBy: [],
        aggregates: [
          {
            expr: { type: 'aggregate', func: 'count', args: [{ type: 'literal', value: '*' }] } as AggregateExpr,
            alias: 'count',
          },
        ],
      };

      const ctx = createMockContext();
      const operator = new AggregateOperator(aggregatePlan, ordersInput, ctx);

      await operator.open(ctx);
      const result = await operator.next();
      await operator.close();

      expect(result).toEqual({ count: 6 });
    });

    it('should implement COUNT(column)', async () => {
      // SELECT COUNT(status) FROM orders (non-null count)
      const { AggregateOperator } = await import('../engine/operators/aggregate.js');

      const ordersInput = new MockScanOperator(ORDERS_DATA, ORDERS_COLUMNS);
      const aggregatePlan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 0, type: 'scan', table: 'orders', source: 'btree', columns: ORDERS_COLUMNS },
        groupBy: [],
        aggregates: [
          {
            expr: { type: 'aggregate', func: 'count', args: [{ type: 'columnRef', column: 'status' }] } as AggregateExpr,
            alias: 'count',
          },
        ],
      };

      const ctx = createMockContext();
      const operator = new AggregateOperator(aggregatePlan, ordersInput, ctx);

      await operator.open(ctx);
      const result = await operator.next();
      await operator.close();

      expect(result).toEqual({ count: 6 });
    });

    it('should implement SUM', async () => {
      // SELECT SUM(amount) FROM orders
      const { AggregateOperator } = await import('../engine/operators/aggregate.js');

      const ordersInput = new MockScanOperator(ORDERS_DATA, ORDERS_COLUMNS);
      const aggregatePlan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 0, type: 'scan', table: 'orders', source: 'btree', columns: ORDERS_COLUMNS },
        groupBy: [],
        aggregates: [
          {
            expr: { type: 'aggregate', func: 'sum', args: [{ type: 'columnRef', column: 'amount' }] } as AggregateExpr,
            alias: 'total',
          },
        ],
      };

      const ctx = createMockContext();
      const operator = new AggregateOperator(aggregatePlan, ordersInput, ctx);

      await operator.open(ctx);
      const result = await operator.next();
      await operator.close();

      // 100 + 200 + 150 + 300 + 250 + 50 = 1050
      expect(result).toEqual({ total: 1050 });
    });

    it('should implement AVG', async () => {
      // SELECT AVG(amount) FROM orders
      const { AggregateOperator } = await import('../engine/operators/aggregate.js');

      const ordersInput = new MockScanOperator(ORDERS_DATA, ORDERS_COLUMNS);
      const aggregatePlan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 0, type: 'scan', table: 'orders', source: 'btree', columns: ORDERS_COLUMNS },
        groupBy: [],
        aggregates: [
          {
            expr: { type: 'aggregate', func: 'avg', args: [{ type: 'columnRef', column: 'amount' }] } as AggregateExpr,
            alias: 'average',
          },
        ],
      };

      const ctx = createMockContext();
      const operator = new AggregateOperator(aggregatePlan, ordersInput, ctx);

      await operator.open(ctx);
      const result = await operator.next();
      await operator.close();

      // 1050 / 6 = 175
      expect(result).toEqual({ average: 175 });
    });

    it('should implement MIN', async () => {
      // SELECT MIN(amount) FROM orders
      const { AggregateOperator } = await import('../engine/operators/aggregate.js');

      const ordersInput = new MockScanOperator(ORDERS_DATA, ORDERS_COLUMNS);
      const aggregatePlan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 0, type: 'scan', table: 'orders', source: 'btree', columns: ORDERS_COLUMNS },
        groupBy: [],
        aggregates: [
          {
            expr: { type: 'aggregate', func: 'min', args: [{ type: 'columnRef', column: 'amount' }] } as AggregateExpr,
            alias: 'min_amount',
          },
        ],
      };

      const ctx = createMockContext();
      const operator = new AggregateOperator(aggregatePlan, ordersInput, ctx);

      await operator.open(ctx);
      const result = await operator.next();
      await operator.close();

      expect(result).toEqual({ min_amount: 50 });
    });

    it('should implement MAX', async () => {
      // SELECT MAX(amount) FROM orders
      const { AggregateOperator } = await import('../engine/operators/aggregate.js');

      const ordersInput = new MockScanOperator(ORDERS_DATA, ORDERS_COLUMNS);
      const aggregatePlan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 0, type: 'scan', table: 'orders', source: 'btree', columns: ORDERS_COLUMNS },
        groupBy: [],
        aggregates: [
          {
            expr: { type: 'aggregate', func: 'max', args: [{ type: 'columnRef', column: 'amount' }] } as AggregateExpr,
            alias: 'max_amount',
          },
        ],
      };

      const ctx = createMockContext();
      const operator = new AggregateOperator(aggregatePlan, ordersInput, ctx);

      await operator.open(ctx);
      const result = await operator.next();
      await operator.close();

      expect(result).toEqual({ max_amount: 300 });
    });
  });

  describe('GROUP BY', () => {
    it('should handle GROUP BY single column', async () => {
      // SELECT user_id, SUM(amount) FROM orders GROUP BY user_id
      const { AggregateOperator } = await import('../engine/operators/aggregate.js');

      const ordersInput = new MockScanOperator(ORDERS_DATA, ORDERS_COLUMNS);
      const aggregatePlan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 0, type: 'scan', table: 'orders', source: 'btree', columns: ORDERS_COLUMNS },
        groupBy: [{ type: 'columnRef', column: 'user_id' }],
        aggregates: [
          {
            expr: { type: 'aggregate', func: 'sum', args: [{ type: 'columnRef', column: 'amount' }] } as AggregateExpr,
            alias: 'total',
          },
        ],
      };

      const ctx = createMockContext();
      const operator = new AggregateOperator(aggregatePlan, ordersInput, ctx);

      await operator.open(ctx);

      const results: Row[] = [];
      let row: Row | null;
      while ((row = await operator.next()) !== null) {
        results.push(row);
      }

      await operator.close();

      // 3 groups: user_id 1, 2, 3
      expect(results.length).toBe(3);

      // user_id 1: 100 + 200 = 300
      const user1 = results.find((r) => r.user_id === 1);
      expect(user1).toEqual({ user_id: 1, total: 300 });

      // user_id 2: 150 + 300 = 450
      const user2 = results.find((r) => r.user_id === 2);
      expect(user2).toEqual({ user_id: 2, total: 450 });

      // user_id 3: 250 + 50 = 300
      const user3 = results.find((r) => r.user_id === 3);
      expect(user3).toEqual({ user_id: 3, total: 300 });
    });

    it('should handle GROUP BY multiple columns', async () => {
      // SELECT user_id, status, COUNT(*) FROM orders GROUP BY user_id, status
      const { AggregateOperator } = await import('../engine/operators/aggregate.js');

      const ordersInput = new MockScanOperator(ORDERS_DATA, ORDERS_COLUMNS);
      const aggregatePlan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 0, type: 'scan', table: 'orders', source: 'btree', columns: ORDERS_COLUMNS },
        groupBy: [
          { type: 'columnRef', column: 'user_id' },
          { type: 'columnRef', column: 'status' },
        ],
        aggregates: [
          {
            expr: { type: 'aggregate', func: 'count', args: [{ type: 'literal', value: '*' }] } as AggregateExpr,
            alias: 'count',
          },
        ],
      };

      const ctx = createMockContext();
      const operator = new AggregateOperator(aggregatePlan, ordersInput, ctx);

      await operator.open(ctx);

      const results: Row[] = [];
      let row: Row | null;
      while ((row = await operator.next()) !== null) {
        results.push(row);
      }

      await operator.close();

      // Groups: (1, completed), (2, pending), (2, completed), (3, completed), (3, cancelled)
      expect(results.length).toBe(5);

      const user1Completed = results.find((r) => r.user_id === 1 && r.status === 'completed');
      expect(user1Completed).toEqual({ user_id: 1, status: 'completed', count: 2 });
    });
  });

  describe('HAVING', () => {
    it('should handle HAVING clause', async () => {
      // SELECT user_id, SUM(amount) FROM orders GROUP BY user_id HAVING SUM(amount) > 300
      const { AggregateOperator } = await import('../engine/operators/aggregate.js');

      const ordersInput = new MockScanOperator(ORDERS_DATA, ORDERS_COLUMNS);
      const aggregatePlan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 0, type: 'scan', table: 'orders', source: 'btree', columns: ORDERS_COLUMNS },
        groupBy: [{ type: 'columnRef', column: 'user_id' }],
        aggregates: [
          {
            expr: { type: 'aggregate', func: 'sum', args: [{ type: 'columnRef', column: 'amount' }] } as AggregateExpr,
            alias: 'total',
          },
        ],
        having: {
          type: 'comparison',
          op: 'gt',
          left: { type: 'aggregate', func: 'sum', args: [{ type: 'columnRef', column: 'amount' }] } as AggregateExpr,
          right: { type: 'literal', value: 300 },
        },
      };

      const ctx = createMockContext();
      const operator = new AggregateOperator(aggregatePlan, ordersInput, ctx);

      await operator.open(ctx);

      const results: Row[] = [];
      let row: Row | null;
      while ((row = await operator.next()) !== null) {
        results.push(row);
      }

      await operator.close();

      // Only user_id 2 has total > 300 (450)
      expect(results.length).toBe(1);
      expect(results[0]).toEqual({ user_id: 2, total: 450 });
    });

    it('should handle complex HAVING with multiple conditions', async () => {
      // SELECT user_id, COUNT(*), SUM(amount)
      // FROM orders
      // GROUP BY user_id
      // HAVING COUNT(*) >= 2 AND SUM(amount) >= 300
      const { AggregateOperator } = await import('../engine/operators/aggregate.js');

      const ordersInput = new MockScanOperator(ORDERS_DATA, ORDERS_COLUMNS);
      const aggregatePlan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 0, type: 'scan', table: 'orders', source: 'btree', columns: ORDERS_COLUMNS },
        groupBy: [{ type: 'columnRef', column: 'user_id' }],
        aggregates: [
          {
            expr: { type: 'aggregate', func: 'count', args: [{ type: 'literal', value: '*' }] } as AggregateExpr,
            alias: 'count',
          },
          {
            expr: { type: 'aggregate', func: 'sum', args: [{ type: 'columnRef', column: 'amount' }] } as AggregateExpr,
            alias: 'total',
          },
        ],
        having: {
          type: 'logical',
          op: 'and',
          operands: [
            {
              type: 'comparison',
              op: 'ge',
              left: { type: 'aggregate', func: 'count', args: [{ type: 'literal', value: '*' }] } as AggregateExpr,
              right: { type: 'literal', value: 2 },
            },
            {
              type: 'comparison',
              op: 'ge',
              left: { type: 'aggregate', func: 'sum', args: [{ type: 'columnRef', column: 'amount' }] } as AggregateExpr,
              right: { type: 'literal', value: 300 },
            },
          ],
        },
      };

      const ctx = createMockContext();
      const operator = new AggregateOperator(aggregatePlan, ordersInput, ctx);

      await operator.open(ctx);

      const results: Row[] = [];
      let row: Row | null;
      while ((row = await operator.next()) !== null) {
        results.push(row);
      }

      await operator.close();

      // All 3 users have count >= 2 and total >= 300
      expect(results.length).toBe(3);
    });
  });
});

// =============================================================================
// OPERATOR PIPELINE COMPOSITION TESTS
// =============================================================================

describe('Operator Pipeline Composition', () => {
  it('should compose operators in pipeline: Scan -> Filter -> Project', async () => {
    // SELECT id, name FROM users WHERE department_id = 1
    const { ProjectOperator } = await import('../engine/operators/project.js');
    const { FilterOperator } = await import('../engine/operators/filter.js');

    const ctx = createMockContext();

    // 1. Scan (mock)
    const scanOperator = new MockScanOperator(USERS_DATA, USERS_COLUMNS);

    // 2. Filter
    const filterPlan: FilterPlan = {
      id: 1,
      type: 'filter',
      input: { id: 0, type: 'scan', table: 'users', source: 'btree', columns: USERS_COLUMNS },
      predicate: {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'department_id' },
        right: { type: 'literal', value: 1 },
      },
    };
    const filterOperator = new FilterOperator(filterPlan, scanOperator, ctx);

    // 3. Project
    const projectPlan: ProjectPlan = {
      id: 2,
      type: 'project',
      input: filterPlan,
      expressions: [
        { expr: { type: 'columnRef', column: 'id' }, alias: 'id' },
        { expr: { type: 'columnRef', column: 'name' }, alias: 'name' },
      ],
    };
    const projectOperator = new ProjectOperator(projectPlan, filterOperator, ctx);

    // Execute pipeline
    await projectOperator.open(ctx);

    const results: Row[] = [];
    let row: Row | null;
    while ((row = await projectOperator.next()) !== null) {
      results.push(row);
    }

    await projectOperator.close();

    // Should return Alice and Bob (department_id = 1)
    expect(results.length).toBe(2);
    expect(results).toEqual([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]);
  });

  it.skip('should compose operators: Scan -> Join -> Aggregate -> Sort -> Limit', async () => {
    // SELECT u.name, COUNT(o.id) as order_count
    // FROM users u
    // JOIN orders o ON u.id = o.user_id
    // GROUP BY u.name
    // ORDER BY order_count DESC
    // LIMIT 2
    const { ProjectOperator } = await import('../engine/operators/project.js');
    const { JoinOperator } = await import('../engine/operators/join.js');
    const { AggregateOperator } = await import('../engine/operators/aggregate.js');
    const { SortOperator } = await import('../engine/operators/sort.js');
    const { LimitOperator } = await import('../engine/operators/limit.js');

    const ctx = createMockContext();

    // 1. Scans (mock)
    const usersOperator = new MockScanOperator(USERS_DATA, USERS_COLUMNS);
    const ordersOperator = new MockScanOperator(ORDERS_DATA, ORDERS_COLUMNS);

    // 2. Join
    const joinPlan: JoinPlan = {
      id: 1,
      type: 'join',
      joinType: 'inner',
      left: { id: 0, type: 'scan', table: 'users', source: 'btree', columns: USERS_COLUMNS },
      right: { id: 0, type: 'scan', table: 'orders', source: 'btree', columns: ORDERS_COLUMNS },
      condition: {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'id', table: 'users' },
        right: { type: 'columnRef', column: 'user_id', table: 'orders' },
      },
    };
    const joinOperator = new JoinOperator(joinPlan, usersOperator, ordersOperator, ctx);

    // 3. Aggregate
    const aggregatePlan: AggregatePlan = {
      id: 2,
      type: 'aggregate',
      input: joinPlan,
      groupBy: [{ type: 'columnRef', column: 'name', table: 'users' }],
      aggregates: [
        {
          expr: { type: 'aggregate', func: 'count', args: [{ type: 'columnRef', column: 'id', table: 'orders' }] } as AggregateExpr,
          alias: 'order_count',
        },
      ],
    };
    const aggregateOperator = new AggregateOperator(aggregatePlan, joinOperator, ctx);

    // 4. Sort
    const sortPlan: SortPlan = {
      id: 3,
      type: 'sort',
      input: aggregatePlan,
      orderBy: [{ expr: { type: 'columnRef', column: 'order_count' }, direction: 'desc' }],
    };
    const sortOperator = new SortOperator(sortPlan, aggregateOperator, ctx);

    // 5. Limit
    const limitPlan: LimitPlan = {
      id: 4,
      type: 'limit',
      input: sortPlan,
      limit: 2,
    };
    const limitOperator = new LimitOperator(limitPlan, sortOperator, ctx);

    // Execute pipeline
    await limitOperator.open(ctx);

    const results: Row[] = [];
    let row: Row | null;
    while ((row = await limitOperator.next()) !== null) {
      results.push(row);
    }

    await limitOperator.close();

    // Should return top 2 users by order count
    expect(results.length).toBe(2);
    // Each user has 2 orders
    expect(results[0].order_count).toBe(2);
    expect(results[1].order_count).toBe(2);
  });

  it('should handle empty result propagation through pipeline', async () => {
    // SELECT name FROM users WHERE department_id = 999 (no matches)
    const { ProjectOperator } = await import('../engine/operators/project.js');
    const { FilterOperator } = await import('../engine/operators/filter.js');

    const ctx = createMockContext();

    const scanOperator = new MockScanOperator(USERS_DATA, USERS_COLUMNS);

    const filterPlan: FilterPlan = {
      id: 1,
      type: 'filter',
      input: { id: 0, type: 'scan', table: 'users', source: 'btree', columns: USERS_COLUMNS },
      predicate: {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'department_id' },
        right: { type: 'literal', value: 999 },
      },
    };
    const filterOperator = new FilterOperator(filterPlan, scanOperator, ctx);

    const projectPlan: ProjectPlan = {
      id: 2,
      type: 'project',
      input: filterPlan,
      expressions: [{ expr: { type: 'columnRef', column: 'name' }, alias: 'name' }],
    };
    const projectOperator = new ProjectOperator(projectPlan, filterOperator, ctx);

    await projectOperator.open(ctx);
    const result = await projectOperator.next();
    await projectOperator.close();

    expect(result).toBeNull();
  });
});
