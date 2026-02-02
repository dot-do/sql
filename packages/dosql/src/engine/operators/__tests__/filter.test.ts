/**
 * Filter Operator Tests
 *
 * Comprehensive tests for SQL filter operations:
 * - Comparison operators (=, <>, <, <=, >, >=, LIKE)
 * - Logical operators (AND, OR, NOT)
 * - Special predicates (BETWEEN, IN, IS NULL, IS NOT NULL)
 * - Expression evaluation (arithmetic, functions, CASE)
 * - Edge cases: nulls, type coercion, empty sets
 *
 * Following the project's TDD with NO MOCKS philosophy.
 */

import { describe, it, expect } from 'vitest';

import {
  evaluateExpression,
  evaluatePredicate,
  FilterOperator,
} from '../filter.js';
import {
  type Row,
  type Expression,
  type Predicate,
  type FilterPlan,
  type Operator,
  type ExecutionContext,
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
 * Create a filter plan
 */
function createFilterPlan(predicate: Predicate): FilterPlan {
  return {
    type: 'filter',
    id: 1,
    input: { type: 'scan', id: 0, table: 'test', source: 'btree', columns: [] },
    predicate,
  };
}

// =============================================================================
// EXPRESSION EVALUATION TESTS
// =============================================================================

describe('Filter - Expression Evaluation', () => {
  describe('Column References', () => {
    it('should evaluate column reference', () => {
      const row: Row = { id: 1, name: 'Alice' };
      expect(evaluateExpression(col('id'), row)).toBe(1);
      expect(evaluateExpression(col('name'), row)).toBe('Alice');
    });

    it('should return null for missing column', () => {
      const row: Row = { id: 1 };
      expect(evaluateExpression(col('missing'), row)).toBeNull();
    });

    it('should evaluate qualified column reference', () => {
      const row: Row = { 'users.id': 1, 'users.name': 'Alice' };
      expect(evaluateExpression({ type: 'columnRef', table: 'users', column: 'id' }, row)).toBe(1);
    });

    it('should find column in prefixed keys', () => {
      const row: Row = { 'users.name': 'Alice' };
      expect(evaluateExpression(col('name'), row)).toBe('Alice');
    });
  });

  describe('Literals', () => {
    it('should evaluate literal values', () => {
      expect(evaluateExpression(lit(42), {})).toBe(42);
      expect(evaluateExpression(lit('hello'), {})).toBe('hello');
      expect(evaluateExpression(lit(true), {})).toBe(true);
      expect(evaluateExpression(lit(null), {})).toBeNull();
    });

    it('should evaluate bigint literals', () => {
      expect(evaluateExpression(lit(9007199254740993n), {})).toBe(9007199254740993n);
    });
  });

  describe('Arithmetic Expressions', () => {
    it('should evaluate addition', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'add',
        left: col('a'),
        right: col('b'),
      };
      expect(evaluateExpression(expr, { a: 10, b: 5 })).toBe(15);
    });

    it('should evaluate subtraction', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'sub',
        left: col('a'),
        right: col('b'),
      };
      expect(evaluateExpression(expr, { a: 10, b: 3 })).toBe(7);
    });

    it('should evaluate multiplication', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'mul',
        left: col('a'),
        right: col('b'),
      };
      expect(evaluateExpression(expr, { a: 6, b: 7 })).toBe(42);
    });

    it('should evaluate division', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'div',
        left: col('a'),
        right: col('b'),
      };
      expect(evaluateExpression(expr, { a: 20, b: 4 })).toBe(5);
    });

    it('should return null for division by zero', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'div',
        left: col('a'),
        right: lit(0),
      };
      expect(evaluateExpression(expr, { a: 10 })).toBeNull();
    });

    it('should evaluate modulo', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'mod',
        left: col('a'),
        right: col('b'),
      };
      expect(evaluateExpression(expr, { a: 17, b: 5 })).toBe(2);
    });

    it('should handle bigint arithmetic', () => {
      const addExpr: Expression = {
        type: 'binary',
        op: 'add',
        left: lit(9007199254740991n),
        right: lit(2n),
      };
      expect(evaluateExpression(addExpr, {})).toBe(9007199254740993n);
    });

    it('should support symbol operators (+, -, *, /, %)', () => {
      expect(evaluateExpression({ type: 'binary', op: '+' as 'add', left: lit(2), right: lit(3) }, {})).toBe(5);
      expect(evaluateExpression({ type: 'binary', op: '-' as 'sub', left: lit(5), right: lit(2) }, {})).toBe(3);
      expect(evaluateExpression({ type: 'binary', op: '*' as 'mul', left: lit(3), right: lit(4) }, {})).toBe(12);
      expect(evaluateExpression({ type: 'binary', op: '/' as 'div', left: lit(10), right: lit(2) }, {})).toBe(5);
      expect(evaluateExpression({ type: 'binary', op: '%' as 'mod', left: lit(7), right: lit(3) }, {})).toBe(1);
    });
  });

  describe('Comparison Expressions', () => {
    it('should evaluate equality', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'eq',
        left: col('a'),
        right: lit(5),
      };
      expect(evaluateExpression(expr, { a: 5 })).toBe(1);
      expect(evaluateExpression(expr, { a: 3 })).toBe(0);
    });

    it('should evaluate inequality', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'ne',
        left: col('a'),
        right: lit(5),
      };
      expect(evaluateExpression(expr, { a: 5 })).toBe(0);
      expect(evaluateExpression(expr, { a: 3 })).toBe(1);
    });

    it('should evaluate less than', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'lt',
        left: col('a'),
        right: lit(5),
      };
      expect(evaluateExpression(expr, { a: 3 })).toBe(1);
      expect(evaluateExpression(expr, { a: 5 })).toBe(0);
      expect(evaluateExpression(expr, { a: 7 })).toBe(0);
    });

    it('should evaluate less than or equal', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'le',
        left: col('a'),
        right: lit(5),
      };
      expect(evaluateExpression(expr, { a: 3 })).toBe(1);
      expect(evaluateExpression(expr, { a: 5 })).toBe(1);
      expect(evaluateExpression(expr, { a: 7 })).toBe(0);
    });

    it('should evaluate greater than', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'gt',
        left: col('a'),
        right: lit(5),
      };
      expect(evaluateExpression(expr, { a: 7 })).toBe(1);
      expect(evaluateExpression(expr, { a: 5 })).toBe(0);
      expect(evaluateExpression(expr, { a: 3 })).toBe(0);
    });

    it('should evaluate greater than or equal', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'ge',
        left: col('a'),
        right: lit(5),
      };
      expect(evaluateExpression(expr, { a: 7 })).toBe(1);
      expect(evaluateExpression(expr, { a: 5 })).toBe(1);
      expect(evaluateExpression(expr, { a: 3 })).toBe(0);
    });

    it('should return null for comparisons with null', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'eq',
        left: col('a'),
        right: lit(5),
      };
      expect(evaluateExpression(expr, { a: null })).toBeNull();
    });

    it('should evaluate LIKE pattern', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'like',
        left: col('name'),
        right: lit('A%'),
      };
      expect(evaluateExpression(expr, { name: 'Alice' })).toBe(1);
      expect(evaluateExpression(expr, { name: 'Bob' })).toBe(0);
    });
  });

  describe('Logical Expressions', () => {
    it('should evaluate AND', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'and',
        left: lit(1),
        right: lit(1),
      };
      expect(evaluateExpression(expr, {})).toBe(1);

      const exprFalse: Expression = {
        type: 'binary',
        op: 'and',
        left: lit(1),
        right: lit(0),
      };
      expect(evaluateExpression(exprFalse, {})).toBe(0);
    });

    it('should evaluate OR', () => {
      const expr: Expression = {
        type: 'binary',
        op: 'or',
        left: lit(0),
        right: lit(1),
      };
      expect(evaluateExpression(expr, {})).toBe(1);

      const exprFalse: Expression = {
        type: 'binary',
        op: 'or',
        left: lit(0),
        right: lit(0),
      };
      expect(evaluateExpression(exprFalse, {})).toBe(0);
    });

    it('should handle SQL NULL logic in AND', () => {
      // NULL AND TRUE = NULL
      const nullAndTrue: Expression = {
        type: 'binary',
        op: 'and',
        left: lit(null),
        right: lit(1),
      };
      expect(evaluateExpression(nullAndTrue, {})).toBeNull();

      // NULL AND FALSE = FALSE
      const nullAndFalse: Expression = {
        type: 'binary',
        op: 'and',
        left: lit(null),
        right: lit(0),
      };
      expect(evaluateExpression(nullAndFalse, {})).toBe(0);
    });

    it('should handle SQL NULL logic in OR', () => {
      // NULL OR TRUE = TRUE
      const nullOrTrue: Expression = {
        type: 'binary',
        op: 'or',
        left: lit(null),
        right: lit(1),
      };
      expect(evaluateExpression(nullOrTrue, {})).toBe(1);

      // NULL OR FALSE = NULL
      const nullOrFalse: Expression = {
        type: 'binary',
        op: 'or',
        left: lit(null),
        right: lit(0),
      };
      expect(evaluateExpression(nullOrFalse, {})).toBeNull();
    });
  });

  describe('Unary Expressions', () => {
    it('should evaluate NOT', () => {
      const exprTrue: Expression = {
        type: 'unary',
        op: 'not',
        operand: lit(0),
      };
      expect(evaluateExpression(exprTrue, {})).toBe(1);

      const exprFalse: Expression = {
        type: 'unary',
        op: 'not',
        operand: lit(1),
      };
      expect(evaluateExpression(exprFalse, {})).toBe(0);
    });

    it('should return NULL for NOT NULL', () => {
      const expr: Expression = {
        type: 'unary',
        op: 'not',
        operand: lit(null),
      };
      expect(evaluateExpression(expr, {})).toBeNull();
    });

    it('should evaluate negation', () => {
      const expr: Expression = {
        type: 'unary',
        op: 'neg',
        operand: col('a'),
      };
      expect(evaluateExpression(expr, { a: 5 })).toBe(-5);
      expect(evaluateExpression(expr, { a: -3 })).toBe(3);
    });

    it('should evaluate IS NULL', () => {
      const expr: Expression = {
        type: 'unary',
        op: 'isNull',
        operand: col('a'),
      };
      expect(evaluateExpression(expr, { a: null })).toBe(1);
      expect(evaluateExpression(expr, { a: 5 })).toBe(0);
    });

    it('should evaluate IS NOT NULL', () => {
      const expr: Expression = {
        type: 'unary',
        op: 'isNotNull',
        operand: col('a'),
      };
      expect(evaluateExpression(expr, { a: null })).toBe(0);
      expect(evaluateExpression(expr, { a: 5 })).toBe(1);
    });
  });

  describe('Function Expressions', () => {
    it('should evaluate string functions', () => {
      expect(evaluateExpression({ type: 'function', name: 'upper', args: [lit('hello')] }, {})).toBe('HELLO');
      expect(evaluateExpression({ type: 'function', name: 'lower', args: [lit('WORLD')] }, {})).toBe('world');
      expect(evaluateExpression({ type: 'function', name: 'length', args: [lit('test')] }, {})).toBe(4);
      expect(evaluateExpression({ type: 'function', name: 'trim', args: [lit('  hi  ')] }, {})).toBe('hi');
    });

    it('should evaluate numeric functions', () => {
      expect(evaluateExpression({ type: 'function', name: 'abs', args: [lit(-5)] }, {})).toBe(5);
      expect(evaluateExpression({ type: 'function', name: 'ceil', args: [lit(4.2)] }, {})).toBe(5);
      expect(evaluateExpression({ type: 'function', name: 'floor', args: [lit(4.8)] }, {})).toBe(4);
      expect(evaluateExpression({ type: 'function', name: 'round', args: [lit(4.567), lit(2)] }, {})).toBe(4.57);
    });

    it('should evaluate COALESCE', () => {
      expect(evaluateExpression({ type: 'function', name: 'coalesce', args: [lit(null), lit(null), lit(3)] }, {})).toBe(3);
      expect(evaluateExpression({ type: 'function', name: 'coalesce', args: [lit(1), lit(2)] }, {})).toBe(1);
    });

    it('should evaluate NULLIF', () => {
      expect(evaluateExpression({ type: 'function', name: 'nullif', args: [lit(1), lit(1)] }, {})).toBeNull();
      expect(evaluateExpression({ type: 'function', name: 'nullif', args: [lit(1), lit(2)] }, {})).toBe(1);
    });

    it('should evaluate IFNULL/ISNULL', () => {
      expect(evaluateExpression({ type: 'function', name: 'ifnull', args: [lit(null), lit(5)] }, {})).toBe(5);
      expect(evaluateExpression({ type: 'function', name: 'ifnull', args: [lit(3), lit(5)] }, {})).toBe(3);
    });

    it('should evaluate CONCAT', () => {
      expect(evaluateExpression({ type: 'function', name: 'concat', args: [lit('Hello'), lit(' '), lit('World')] }, {})).toBe('Hello World');
    });

    it('should evaluate SUBSTRING', () => {
      expect(evaluateExpression({ type: 'function', name: 'substring', args: [lit('Hello'), lit(2), lit(3)] }, {})).toBe('ell');
    });
  });

  describe('CASE Expressions', () => {
    it('should evaluate searched CASE expression', () => {
      const caseExpr: Expression = {
        type: 'case',
        when: [
          {
            condition: { type: 'binary', op: 'gt', left: col('score'), right: lit(90) },
            result: lit('A'),
          },
          {
            condition: { type: 'binary', op: 'gt', left: col('score'), right: lit(80) },
            result: lit('B'),
          },
        ],
        else: lit('C'),
      };

      expect(evaluateExpression(caseExpr, { score: 95 })).toBe('A');
      expect(evaluateExpression(caseExpr, { score: 85 })).toBe('B');
      expect(evaluateExpression(caseExpr, { score: 70 })).toBe('C');
    });

    it('should evaluate simple CASE expression', () => {
      const caseExpr: Expression = {
        type: 'case',
        operand: col('status'),
        when: [
          { value: lit('active'), result: lit(1) },
          { value: lit('pending'), result: lit(2) },
        ],
        else: lit(0),
      };

      expect(evaluateExpression(caseExpr, { status: 'active' })).toBe(1);
      expect(evaluateExpression(caseExpr, { status: 'pending' })).toBe(2);
      expect(evaluateExpression(caseExpr, { status: 'closed' })).toBe(0);
    });

    it('should return NULL for CASE with no match and no ELSE', () => {
      const caseExpr: Expression = {
        type: 'case',
        when: [
          {
            condition: { type: 'binary', op: 'eq', left: col('a'), right: lit(1) },
            result: lit('one'),
          },
        ],
      };

      expect(evaluateExpression(caseExpr, { a: 2 })).toBeNull();
    });
  });
});

// =============================================================================
// PREDICATE EVALUATION TESTS
// =============================================================================

describe('Filter - Predicate Evaluation', () => {
  describe('Comparison Predicates', () => {
    it('should evaluate equality predicate', () => {
      const pred: Predicate = {
        type: 'comparison',
        op: 'eq',
        left: col('status'),
        right: lit('active'),
      };
      expect(evaluatePredicate(pred, { status: 'active' })).toBe(true);
      expect(evaluatePredicate(pred, { status: 'inactive' })).toBe(false);
    });

    it('should evaluate inequality predicate', () => {
      const pred: Predicate = {
        type: 'comparison',
        op: 'ne',
        left: col('status'),
        right: lit('deleted'),
      };
      expect(evaluatePredicate(pred, { status: 'active' })).toBe(true);
      expect(evaluatePredicate(pred, { status: 'deleted' })).toBe(false);
    });

    it('should evaluate numeric comparison predicates', () => {
      const ltPred: Predicate = {
        type: 'comparison',
        op: 'lt',
        left: col('age'),
        right: lit(18),
      };
      expect(evaluatePredicate(ltPred, { age: 15 })).toBe(true);
      expect(evaluatePredicate(ltPred, { age: 18 })).toBe(false);

      const gePred: Predicate = {
        type: 'comparison',
        op: 'ge',
        left: col('age'),
        right: lit(21),
      };
      expect(evaluatePredicate(gePred, { age: 21 })).toBe(true);
      expect(evaluatePredicate(gePred, { age: 20 })).toBe(false);
    });

    it('should handle null comparisons', () => {
      const pred: Predicate = {
        type: 'comparison',
        op: 'eq',
        left: col('value'),
        right: lit(5),
      };
      expect(evaluatePredicate(pred, { value: null })).toBe(false);
    });

    it('should evaluate LIKE predicate', () => {
      const pred: Predicate = {
        type: 'comparison',
        op: 'like',
        left: col('name'),
        right: lit('%son'),
      };
      expect(evaluatePredicate(pred, { name: 'Johnson' })).toBe(true);
      expect(evaluatePredicate(pred, { name: 'Smith' })).toBe(false);
    });
  });

  describe('Logical Predicates', () => {
    it('should evaluate AND predicate', () => {
      const pred: Predicate = {
        type: 'logical',
        op: 'and',
        operands: [
          { type: 'comparison', op: 'gt', left: col('age'), right: lit(18) },
          { type: 'comparison', op: 'lt', left: col('age'), right: lit(65) },
        ],
      };
      expect(evaluatePredicate(pred, { age: 30 })).toBe(true);
      expect(evaluatePredicate(pred, { age: 70 })).toBe(false);
      expect(evaluatePredicate(pred, { age: 15 })).toBe(false);
    });

    it('should evaluate OR predicate', () => {
      const pred: Predicate = {
        type: 'logical',
        op: 'or',
        operands: [
          { type: 'comparison', op: 'eq', left: col('status'), right: lit('active') },
          { type: 'comparison', op: 'eq', left: col('status'), right: lit('pending') },
        ],
      };
      expect(evaluatePredicate(pred, { status: 'active' })).toBe(true);
      expect(evaluatePredicate(pred, { status: 'pending' })).toBe(true);
      expect(evaluatePredicate(pred, { status: 'deleted' })).toBe(false);
    });

    it('should evaluate NOT predicate', () => {
      const pred: Predicate = {
        type: 'logical',
        op: 'not',
        operands: [
          { type: 'comparison', op: 'eq', left: col('deleted'), right: lit(true) },
        ],
      };
      expect(evaluatePredicate(pred, { deleted: false })).toBe(true);
      expect(evaluatePredicate(pred, { deleted: true })).toBe(false);
    });

    it('should evaluate complex nested logical predicates', () => {
      // (status = 'active' OR status = 'pending') AND age >= 18
      const pred: Predicate = {
        type: 'logical',
        op: 'and',
        operands: [
          {
            type: 'logical',
            op: 'or',
            operands: [
              { type: 'comparison', op: 'eq', left: col('status'), right: lit('active') },
              { type: 'comparison', op: 'eq', left: col('status'), right: lit('pending') },
            ],
          },
          { type: 'comparison', op: 'ge', left: col('age'), right: lit(18) },
        ],
      };

      expect(evaluatePredicate(pred, { status: 'active', age: 25 })).toBe(true);
      expect(evaluatePredicate(pred, { status: 'pending', age: 20 })).toBe(true);
      expect(evaluatePredicate(pred, { status: 'active', age: 15 })).toBe(false);
      expect(evaluatePredicate(pred, { status: 'deleted', age: 25 })).toBe(false);
    });
  });

  describe('BETWEEN Predicate', () => {
    it('should evaluate BETWEEN predicate', () => {
      const pred: Predicate = {
        type: 'between',
        expr: col('age'),
        low: lit(18),
        high: lit(65),
      };
      expect(evaluatePredicate(pred, { age: 30 })).toBe(true);
      expect(evaluatePredicate(pred, { age: 18 })).toBe(true);
      expect(evaluatePredicate(pred, { age: 65 })).toBe(true);
      expect(evaluatePredicate(pred, { age: 17 })).toBe(false);
      expect(evaluatePredicate(pred, { age: 66 })).toBe(false);
    });

    it('should return false for BETWEEN with null value', () => {
      const pred: Predicate = {
        type: 'between',
        expr: col('age'),
        low: lit(18),
        high: lit(65),
      };
      expect(evaluatePredicate(pred, { age: null })).toBe(false);
    });

    it('should return false for BETWEEN with null boundary', () => {
      const pred: Predicate = {
        type: 'between',
        expr: col('age'),
        low: lit(null),
        high: lit(65),
      };
      expect(evaluatePredicate(pred, { age: 30 })).toBe(false);
    });
  });

  describe('IN Predicate', () => {
    it('should evaluate IN predicate with list', () => {
      const pred: Predicate = {
        type: 'in',
        expr: col('status'),
        values: [lit('active'), lit('pending'), lit('review')],
      };
      expect(evaluatePredicate(pred, { status: 'active' })).toBe(true);
      expect(evaluatePredicate(pred, { status: 'pending' })).toBe(true);
      expect(evaluatePredicate(pred, { status: 'deleted' })).toBe(false);
    });

    it('should return false for IN with empty list', () => {
      const pred: Predicate = {
        type: 'in',
        expr: col('id'),
        values: [],
      };
      expect(evaluatePredicate(pred, { id: 1 })).toBe(false);
    });

    it('should return false for IN with null value', () => {
      const pred: Predicate = {
        type: 'in',
        expr: col('id'),
        values: [lit(1), lit(2), lit(3)],
      };
      expect(evaluatePredicate(pred, { id: null })).toBe(false);
    });

    it('should handle IN with numeric values', () => {
      const pred: Predicate = {
        type: 'in',
        expr: col('id'),
        values: [lit(1), lit(2), lit(3)],
      };
      expect(evaluatePredicate(pred, { id: 2 })).toBe(true);
      expect(evaluatePredicate(pred, { id: 5 })).toBe(false);
    });
  });

  describe('IS NULL Predicate', () => {
    it('should evaluate IS NULL', () => {
      const pred: Predicate = {
        type: 'isNull',
        expr: col('value'),
        isNot: false,
      };
      expect(evaluatePredicate(pred, { value: null })).toBe(true);
      expect(evaluatePredicate(pred, { value: 5 })).toBe(false);
    });

    it('should evaluate IS NOT NULL', () => {
      const pred: Predicate = {
        type: 'isNull',
        expr: col('value'),
        isNot: true,
      };
      expect(evaluatePredicate(pred, { value: null })).toBe(false);
      expect(evaluatePredicate(pred, { value: 5 })).toBe(true);
    });

    it('should handle IS NULL for zero and empty string', () => {
      const pred: Predicate = {
        type: 'isNull',
        expr: col('value'),
        isNot: false,
      };
      expect(evaluatePredicate(pred, { value: 0 })).toBe(false);
      expect(evaluatePredicate(pred, { value: '' })).toBe(false);
    });
  });
});

// =============================================================================
// FILTER OPERATOR TESTS
// =============================================================================

describe('Filter Operator', () => {
  it('should filter rows matching predicate', async () => {
    const rows: Row[] = [
      { id: 1, status: 'active' },
      { id: 2, status: 'inactive' },
      { id: 3, status: 'active' },
      { id: 4, status: 'deleted' },
    ];

    const pred: Predicate = {
      type: 'comparison',
      op: 'eq',
      left: col('status'),
      right: lit('active'),
    };

    const plan = createFilterPlan(pred);
    const input = createMockOperator(rows);
    const ctx = createMockExecutionContext();
    const operator = new FilterOperator(plan, input, ctx);
    const results = await collectRows(operator);

    expect(results).toHaveLength(2);
    expect(results.every(r => r.status === 'active')).toBe(true);
  });

  it('should handle complex WHERE conditions', async () => {
    const rows: Row[] = [
      { id: 1, age: 25, dept: 'IT' },
      { id: 2, age: 35, dept: 'HR' },
      { id: 3, age: 22, dept: 'IT' },
      { id: 4, age: 45, dept: 'IT' },
      { id: 5, age: 28, dept: 'Sales' },
    ];

    // WHERE dept = 'IT' AND age < 30
    const pred: Predicate = {
      type: 'logical',
      op: 'and',
      operands: [
        { type: 'comparison', op: 'eq', left: col('dept'), right: lit('IT') },
        { type: 'comparison', op: 'lt', left: col('age'), right: lit(30) },
      ],
    };

    const plan = createFilterPlan(pred);
    const input = createMockOperator(rows);
    const ctx = createMockExecutionContext();
    const operator = new FilterOperator(plan, input, ctx);
    const results = await collectRows(operator);

    expect(results).toHaveLength(2);
    expect(results.map(r => r.id)).toContain(1);
    expect(results.map(r => r.id)).toContain(3);
  });

  it('should return empty array when no rows match', async () => {
    const rows: Row[] = [
      { id: 1, value: 10 },
      { id: 2, value: 20 },
    ];

    const pred: Predicate = {
      type: 'comparison',
      op: 'gt',
      left: col('value'),
      right: lit(100),
    };

    const plan = createFilterPlan(pred);
    const input = createMockOperator(rows);
    const ctx = createMockExecutionContext();
    const operator = new FilterOperator(plan, input, ctx);
    const results = await collectRows(operator);

    expect(results).toHaveLength(0);
  });

  it('should pass all rows when predicate is always true', async () => {
    const rows: Row[] = [
      { id: 1 },
      { id: 2 },
      { id: 3 },
    ];

    // id IS NOT NULL (always true for these rows)
    const pred: Predicate = {
      type: 'isNull',
      expr: col('id'),
      isNot: true,
    };

    const plan = createFilterPlan(pred);
    const input = createMockOperator(rows);
    const ctx = createMockExecutionContext();
    const operator = new FilterOperator(plan, input, ctx);
    const results = await collectRows(operator);

    expect(results).toHaveLength(3);
  });

  it('should handle empty input', async () => {
    const rows: Row[] = [];

    const pred: Predicate = {
      type: 'comparison',
      op: 'eq',
      left: col('id'),
      right: lit(1),
    };

    const plan = createFilterPlan(pred);
    const input = createMockOperator(rows, ['id']);
    const ctx = createMockExecutionContext();
    const operator = new FilterOperator(plan, input, ctx);
    const results = await collectRows(operator);

    expect(results).toHaveLength(0);
  });

  it('should preserve column order', async () => {
    const rows: Row[] = [{ id: 1, name: 'Alice', age: 25 }];

    const pred: Predicate = {
      type: 'isNull',
      expr: col('id'),
      isNot: true,
    };

    const plan = createFilterPlan(pred);
    const input = createMockOperator(rows);
    const ctx = createMockExecutionContext();
    const operator = new FilterOperator(plan, input, ctx);

    expect(operator.columns()).toEqual(['id', 'name', 'age']);
  });

  it('should support async iteration', async () => {
    const rows: Row[] = [
      { id: 1, status: 'active' },
      { id: 2, status: 'inactive' },
      { id: 3, status: 'active' },
    ];

    const pred: Predicate = {
      type: 'comparison',
      op: 'eq',
      left: col('status'),
      right: lit('active'),
    };

    const plan = createFilterPlan(pred);
    const input = createMockOperator(rows);
    const ctx = createMockExecutionContext();
    const operator = new FilterOperator(plan, input, ctx);

    await operator.open(ctx);

    const results: Row[] = [];
    for await (const row of operator) {
      results.push(row);
    }

    expect(results).toHaveLength(2);
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Filter - Edge Cases', () => {
  it('should handle large datasets', async () => {
    const rows: Row[] = [];
    for (let i = 0; i < 10000; i++) {
      rows.push({ id: i, value: i % 100 });
    }

    const pred: Predicate = {
      type: 'comparison',
      op: 'eq',
      left: col('value'),
      right: lit(42),
    };

    const plan = createFilterPlan(pred);
    const input = createMockOperator(rows);
    const ctx = createMockExecutionContext();
    const operator = new FilterOperator(plan, input, ctx);
    const results = await collectRows(operator);

    expect(results).toHaveLength(100); // 10000 / 100 = 100 rows with value 42
  });

  it('should handle rows with many columns', async () => {
    const row: Row = {};
    for (let i = 0; i < 100; i++) {
      row[`col${i}`] = i;
    }
    const rows = [row];

    const pred: Predicate = {
      type: 'comparison',
      op: 'eq',
      left: col('col50'),
      right: lit(50),
    };

    const plan = createFilterPlan(pred);
    const input = createMockOperator(rows);
    const ctx = createMockExecutionContext();
    const operator = new FilterOperator(plan, input, ctx);
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
  });

  it('should handle deeply nested logical predicates', async () => {
    const rows: Row[] = [
      { a: 1, b: 2, c: 3, d: 4 },
      { a: 1, b: 2, c: 3, d: 5 },
      { a: 1, b: 2, c: 4, d: 4 },
    ];

    // ((a = 1 AND b = 2) AND (c = 3 AND d = 4))
    const pred: Predicate = {
      type: 'logical',
      op: 'and',
      operands: [
        {
          type: 'logical',
          op: 'and',
          operands: [
            { type: 'comparison', op: 'eq', left: col('a'), right: lit(1) },
            { type: 'comparison', op: 'eq', left: col('b'), right: lit(2) },
          ],
        },
        {
          type: 'logical',
          op: 'and',
          operands: [
            { type: 'comparison', op: 'eq', left: col('c'), right: lit(3) },
            { type: 'comparison', op: 'eq', left: col('d'), right: lit(4) },
          ],
        },
      ],
    };

    const plan = createFilterPlan(pred);
    const input = createMockOperator(rows);
    const ctx = createMockExecutionContext();
    const operator = new FilterOperator(plan, input, ctx);
    const results = await collectRows(operator);

    expect(results).toHaveLength(1);
    expect(results[0]).toEqual({ a: 1, b: 2, c: 3, d: 4 });
  });

  it('should handle special string characters in LIKE', async () => {
    const rows: Row[] = [
      { pattern: 'hello_world' },
      { pattern: 'hello%world' },
      { pattern: 'hello world' },
    ];

    // Note: _ and % are wildcards in SQL LIKE
    const pred: Predicate = {
      type: 'comparison',
      op: 'like',
      left: col('pattern'),
      right: lit('hello_world'),
    };

    const plan = createFilterPlan(pred);
    const input = createMockOperator(rows);
    const ctx = createMockExecutionContext();
    const operator = new FilterOperator(plan, input, ctx);
    const results = await collectRows(operator);

    // _ matches any single character, so both 'hello_world' and 'hello world' match
    expect(results.length).toBeGreaterThanOrEqual(1);
  });

  it('should handle Date comparisons', async () => {
    const date1 = new Date('2024-01-01');
    const date2 = new Date('2024-06-01');
    const date3 = new Date('2024-12-01');

    const rows: Row[] = [
      { id: 1, created: date1 },
      { id: 2, created: date2 },
      { id: 3, created: date3 },
    ];

    const pred: Predicate = {
      type: 'comparison',
      op: 'lt',
      left: col('created'),
      right: lit(new Date('2024-07-01')),
    };

    const plan = createFilterPlan(pred);
    const input = createMockOperator(rows);
    const ctx = createMockExecutionContext();
    const operator = new FilterOperator(plan, input, ctx);
    const results = await collectRows(operator);

    expect(results).toHaveLength(2);
    expect(results.map(r => r.id)).toContain(1);
    expect(results.map(r => r.id)).toContain(2);
  });

  it('should handle boolean values correctly', async () => {
    const rows: Row[] = [
      { id: 1, active: true },
      { id: 2, active: false },
      { id: 3, active: true },
    ];

    const pred: Predicate = {
      type: 'comparison',
      op: 'eq',
      left: col('active'),
      right: lit(true),
    };

    const plan = createFilterPlan(pred);
    const input = createMockOperator(rows);
    const ctx = createMockExecutionContext();
    const operator = new FilterOperator(plan, input, ctx);
    const results = await collectRows(operator);

    expect(results).toHaveLength(2);
    expect(results.every(r => r.active === true)).toBe(true);
  });
});
