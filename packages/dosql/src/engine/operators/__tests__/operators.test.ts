/**
 * DoSQL Engine Operators - Comprehensive Tests
 *
 * Tests for all query execution operators:
 * - CASE expression evaluation
 * - CTE (Common Table Expressions)
 * - Filter (WHERE predicates)
 * - Scan (table and index scans)
 * - Set operations (UNION, INTERSECT, EXCEPT)
 * - Subquery operators (scalar, IN, EXISTS, correlated)
 * - Window functions (OVER, PARTITION BY, ORDER BY, frames)
 */

import { describe, it, expect, beforeEach } from 'vitest';

// CASE operator imports
import {
  evaluateCaseExpression,
  evaluateCoalesce,
  evaluateNullif,
  evaluateIif,
  evaluateIfnull,
  evaluateNvl,
  evaluateNvl2,
  evaluateDecode,
  buildSearchedCase,
  buildSimpleCase,
  simpleCaseToSearched,
  isCaseConstant,
  foldConstantCase,
  isTruthy,
  valuesEqual,
  type SimpleCaseExpr,
  type SearchedCaseExpr,
} from '../case.js';

// CTE operator imports
import {
  createCTEContext,
  CTEScanOperator,
  executeSimpleCTE,
  executeRecursiveCTE,
  executeWithClause,
  getCTERows,
  isCTEMaterialized,
  getCTEColumns,
  createMockQueryExecutor,
  type CTEExecutionContext,
  type MaterializedCTE,
} from '../cte.js';

// Filter operator imports
import {
  evaluateExpression,
  evaluatePredicate,
  FilterOperator,
} from '../filter.js';

// Scan operator imports
import { ScanOperator } from '../scan.js';

// Set operations imports
import {
  UnionOperator,
  IntersectOperator,
  ExceptOperator,
  createSetOperationOperator,
  executeSetOperation,
  getSetOperationPrecedence,
} from '../set-ops.js';

// Subquery operator imports
import {
  ScalarSubqueryOperator,
  InSubqueryOperator,
  ExistsSubqueryOperator,
  QuantifiedSubqueryOperator,
  DerivedTableOperator,
  LateralJoinOperator,
  type SubqueryContext,
} from '../subquery.js';

// Window operator imports
import {
  WindowOperator,
  createWindowOperator,
  containsWindowFunction,
  extractWindowFunctions,
  type WindowPlan,
  type WindowFunctionDef,
} from '../window.js';

// Type imports
import {
  type Row,
  type Expression,
  type Predicate,
  type ExecutionContext,
  type Operator,
  type QueryPlan,
  type ScanPlan,
  type FilterPlan,
  type SqlValue,
  col,
  lit,
} from '../../types.js';

import { type WithClause, type CTEDefinition } from '../../../parser/cte-types.js';
import { type WindowSpec } from '../../../functions/window.js';

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
// CASE EXPRESSION TESTS
// =============================================================================

describe('CASE Expression Operator', () => {
  describe('Simple CASE', () => {
    it('should match first value and return result', () => {
      const caseExpr: SimpleCaseExpr = {
        type: 'case',
        caseStyle: 'simple',
        operand: { type: 'columnRef', column: 'status' },
        when: [
          { value: lit('active'), result: lit(1) },
          { value: lit('pending'), result: lit(2) },
          { value: lit('closed'), result: lit(3) },
        ],
      };

      const row: Row = { status: 'active' };
      expect(evaluateCaseExpression(caseExpr, row)).toBe(1);
    });

    it('should match second value when first does not match', () => {
      const caseExpr: SimpleCaseExpr = {
        type: 'case',
        caseStyle: 'simple',
        operand: col('status'),
        when: [
          { value: lit('active'), result: lit(1) },
          { value: lit('pending'), result: lit(2) },
        ],
      };

      const row: Row = { status: 'pending' };
      expect(evaluateCaseExpression(caseExpr, row)).toBe(2);
    });

    it('should return ELSE when no values match', () => {
      const caseExpr: SimpleCaseExpr = {
        type: 'case',
        caseStyle: 'simple',
        operand: col('status'),
        when: [
          { value: lit('active'), result: lit(1) },
          { value: lit('pending'), result: lit(2) },
        ],
        else: lit(0),
      };

      const row: Row = { status: 'unknown' };
      expect(evaluateCaseExpression(caseExpr, row)).toBe(0);
    });

    it('should return NULL when no match and no ELSE', () => {
      const caseExpr: SimpleCaseExpr = {
        type: 'case',
        caseStyle: 'simple',
        operand: col('status'),
        when: [
          { value: lit('active'), result: lit(1) },
        ],
      };

      const row: Row = { status: 'inactive' };
      expect(evaluateCaseExpression(caseExpr, row)).toBeNull();
    });

    it('should handle numeric comparisons', () => {
      const caseExpr: SimpleCaseExpr = {
        type: 'case',
        caseStyle: 'simple',
        operand: col('level'),
        when: [
          { value: lit(1), result: lit('low') },
          { value: lit(2), result: lit('medium') },
          { value: lit(3), result: lit('high') },
        ],
        else: lit('unknown'),
      };

      expect(evaluateCaseExpression(caseExpr, { level: 1 })).toBe('low');
      expect(evaluateCaseExpression(caseExpr, { level: 2 })).toBe('medium');
      expect(evaluateCaseExpression(caseExpr, { level: 3 })).toBe('high');
      expect(evaluateCaseExpression(caseExpr, { level: 99 })).toBe('unknown');
    });
  });

  describe('Searched CASE', () => {
    it('should evaluate conditions and return matching result', () => {
      const caseExpr: SearchedCaseExpr = {
        type: 'case',
        caseStyle: 'searched',
        when: [
          {
            condition: {
              type: 'binary',
              op: 'gt',
              left: col('score'),
              right: lit(90),
            },
            result: lit('A'),
          },
          {
            condition: {
              type: 'binary',
              op: 'gt',
              left: col('score'),
              right: lit(80),
            },
            result: lit('B'),
          },
          {
            condition: {
              type: 'binary',
              op: 'gt',
              left: col('score'),
              right: lit(70),
            },
            result: lit('C'),
          },
        ],
        else: lit('F'),
      };

      expect(evaluateCaseExpression(caseExpr, { score: 95 })).toBe('A');
      expect(evaluateCaseExpression(caseExpr, { score: 85 })).toBe('B');
      expect(evaluateCaseExpression(caseExpr, { score: 75 })).toBe('C');
      expect(evaluateCaseExpression(caseExpr, { score: 50 })).toBe('F');
    });

    it('should stop at first true condition', () => {
      const caseExpr: SearchedCaseExpr = {
        type: 'case',
        caseStyle: 'searched',
        when: [
          {
            condition: {
              type: 'binary',
              op: 'gt',
              left: col('x'),
              right: lit(0),
            },
            result: lit('positive'),
          },
          {
            condition: {
              type: 'binary',
              op: 'gt',
              left: col('x'),
              right: lit(10),
            },
            result: lit('very positive'),
          },
        ],
      };

      // Should return 'positive', not 'very positive'
      expect(evaluateCaseExpression(caseExpr, { x: 100 })).toBe('positive');
    });

    it('should handle complex AND/OR conditions', () => {
      const caseExpr: SearchedCaseExpr = {
        type: 'case',
        caseStyle: 'searched',
        when: [
          {
            condition: {
              type: 'binary',
              op: 'and',
              left: {
                type: 'binary',
                op: 'gt',
                left: col('age'),
                right: lit(18),
              },
              right: {
                type: 'binary',
                op: 'eq',
                left: col('country'),
                right: lit('US'),
              },
            },
            result: lit('eligible'),
          },
        ],
        else: lit('not eligible'),
      };

      expect(evaluateCaseExpression(caseExpr, { age: 21, country: 'US' })).toBe('eligible');
      expect(evaluateCaseExpression(caseExpr, { age: 16, country: 'US' })).toBe('not eligible');
      expect(evaluateCaseExpression(caseExpr, { age: 21, country: 'UK' })).toBe('not eligible');
    });
  });

  describe('Nested CASE', () => {
    it('should handle nested CASE expressions', () => {
      // Outer CASE determines category, inner CASE determines sub-category
      const innerCase: SearchedCaseExpr = {
        type: 'case',
        caseStyle: 'searched',
        when: [
          {
            condition: {
              type: 'binary',
              op: 'eq',
              left: col('subtype'),
              right: lit('premium'),
            },
            result: lit('VIP'),
          },
        ],
        else: lit('standard'),
      };

      const outerCase: SearchedCaseExpr = {
        type: 'case',
        caseStyle: 'searched',
        when: [
          {
            condition: {
              type: 'binary',
              op: 'eq',
              left: col('type'),
              right: lit('customer'),
            },
            result: innerCase,
          },
        ],
        else: lit('other'),
      };

      expect(evaluateCaseExpression(outerCase, { type: 'customer', subtype: 'premium' })).toBe('VIP');
      expect(evaluateCaseExpression(outerCase, { type: 'customer', subtype: 'basic' })).toBe('standard');
      expect(evaluateCaseExpression(outerCase, { type: 'vendor', subtype: 'premium' })).toBe('other');
    });
  });

  describe('NULL Handling', () => {
    it('should not match NULL values in simple CASE', () => {
      const caseExpr: SimpleCaseExpr = {
        type: 'case',
        caseStyle: 'simple',
        operand: col('value'),
        when: [
          { value: lit(null), result: lit('is null') },
        ],
        else: lit('not matched'),
      };

      // NULL = NULL returns false in SQL
      expect(evaluateCaseExpression(caseExpr, { value: null })).toBe('not matched');
    });

    it('should handle NULL in WHEN results', () => {
      const caseExpr: SimpleCaseExpr = {
        type: 'case',
        caseStyle: 'simple',
        operand: col('x'),
        when: [
          { value: lit(1), result: lit(null) },
        ],
        else: lit('else'),
      };

      expect(evaluateCaseExpression(caseExpr, { x: 1 })).toBeNull();
      expect(evaluateCaseExpression(caseExpr, { x: 2 })).toBe('else');
    });

    it('should treat NULL condition as falsy', () => {
      const caseExpr: SearchedCaseExpr = {
        type: 'case',
        caseStyle: 'searched',
        when: [
          {
            condition: col('x'), // NULL is falsy
            result: lit('truthy'),
          },
        ],
        else: lit('falsy'),
      };

      expect(evaluateCaseExpression(caseExpr, { x: null })).toBe('falsy');
      expect(evaluateCaseExpression(caseExpr, { x: 1 })).toBe('truthy');
      expect(evaluateCaseExpression(caseExpr, { x: 0 })).toBe('falsy');
    });
  });

  describe('CASE Shorthand Functions', () => {
    describe('COALESCE', () => {
      it('should return first non-NULL value', () => {
        expect(evaluateCoalesce([lit(null), lit(null), lit(3)], {})).toBe(3);
        expect(evaluateCoalesce([lit('first'), lit('second')], {})).toBe('first');
        expect(evaluateCoalesce([col('a'), lit('default')], { a: null })).toBe('default');
        expect(evaluateCoalesce([col('a'), lit('default')], { a: 'value' })).toBe('value');
      });

      it('should return NULL if all values are NULL', () => {
        expect(evaluateCoalesce([lit(null), lit(null)], {})).toBeNull();
      });
    });

    describe('NULLIF', () => {
      it('should return NULL if values are equal', () => {
        expect(evaluateNullif(lit(5), lit(5), {})).toBeNull();
        expect(evaluateNullif(lit('a'), lit('a'), {})).toBeNull();
      });

      it('should return first value if values are different', () => {
        expect(evaluateNullif(lit(5), lit(10), {})).toBe(5);
        expect(evaluateNullif(lit('a'), lit('b'), {})).toBe('a');
      });

      it('should return NULL for NULL inputs', () => {
        // NULL = NULL is false, so NULLIF(NULL, NULL) returns NULL (first value)
        expect(evaluateNullif(lit(null), lit(null), {})).toBeNull();
        expect(evaluateNullif(lit(5), lit(null), {})).toBe(5);
      });
    });

    describe('IIF', () => {
      it('should return true value when condition is truthy', () => {
        const condition: Expression = {
          type: 'binary',
          op: 'gt',
          left: col('x'),
          right: lit(0),
        };
        expect(evaluateIif(condition, lit('yes'), lit('no'), { x: 5 })).toBe('yes');
      });

      it('should return false value when condition is falsy', () => {
        const condition: Expression = {
          type: 'binary',
          op: 'gt',
          left: col('x'),
          right: lit(0),
        };
        expect(evaluateIif(condition, lit('yes'), lit('no'), { x: -5 })).toBe('no');
      });
    });

    describe('IFNULL / NVL', () => {
      it('should return expr if not NULL', () => {
        expect(evaluateIfnull(col('x'), lit('default'), { x: 'value' })).toBe('value');
        expect(evaluateNvl(col('x'), lit('default'), { x: 'value' })).toBe('value');
      });

      it('should return default if expr is NULL', () => {
        expect(evaluateIfnull(col('x'), lit('default'), { x: null })).toBe('default');
        expect(evaluateNvl(col('x'), lit('default'), { x: null })).toBe('default');
      });
    });

    describe('NVL2', () => {
      it('should return not_null_value when expr is not NULL', () => {
        expect(evaluateNvl2(col('x'), lit('not null'), lit('is null'), { x: 'value' })).toBe('not null');
      });

      it('should return null_value when expr is NULL', () => {
        expect(evaluateNvl2(col('x'), lit('not null'), lit('is null'), { x: null })).toBe('is null');
      });
    });

    describe('DECODE', () => {
      it('should match search values and return results', () => {
        // DECODE(col, 'A', 1, 'B', 2, 0)
        const args = [col('x'), lit('A'), lit(1), lit('B'), lit(2), lit(0)];
        expect(evaluateDecode(args, { x: 'A' })).toBe(1);
        expect(evaluateDecode(args, { x: 'B' })).toBe(2);
        expect(evaluateDecode(args, { x: 'C' })).toBe(0); // default
      });

      it('should return NULL if no match and no default', () => {
        const args = [col('x'), lit('A'), lit(1), lit('B'), lit(2)];
        expect(evaluateDecode(args, { x: 'C' })).toBeNull();
      });

      it('should throw for insufficient arguments', () => {
        expect(() => evaluateDecode([lit(1), lit(2)], {})).toThrow();
      });
    });
  });

  describe('CASE Builders and Utilities', () => {
    it('should build searched CASE expression', () => {
      const caseExpr = buildSearchedCase(
        [
          { condition: { type: 'binary', op: 'eq', left: col('x'), right: lit(1) }, result: lit('one') },
        ],
        lit('other')
      );

      expect(caseExpr.caseStyle).toBe('searched');
      expect(caseExpr.when).toHaveLength(1);
      expect(caseExpr.else).toBeDefined();
    });

    it('should build simple CASE expression', () => {
      const caseExpr = buildSimpleCase(
        col('x'),
        [{ value: lit(1), result: lit('one') }],
        lit('other')
      );

      expect(caseExpr.caseStyle).toBe('simple');
      expect(caseExpr.operand).toBeDefined();
    });

    it('should convert simple CASE to searched CASE', () => {
      const simpleCase = buildSimpleCase(
        col('status'),
        [
          { value: lit('active'), result: lit(1) },
          { value: lit('pending'), result: lit(2) },
        ]
      );

      const searchedCase = simpleCaseToSearched(simpleCase);

      expect(searchedCase.caseStyle).toBe('searched');
      expect(searchedCase.when).toHaveLength(2);
      // Each WHEN should be: status = 'active', status = 'pending'
      expect(searchedCase.when[0].condition.type).toBe('binary');
    });

    it('should identify constant CASE expressions', () => {
      const constantCase: SearchedCaseExpr = {
        type: 'case',
        caseStyle: 'searched',
        when: [
          { condition: lit(true), result: lit('result') },
        ],
        else: lit('default'),
      };

      const nonConstantCase: SearchedCaseExpr = {
        type: 'case',
        caseStyle: 'searched',
        when: [
          { condition: col('x'), result: lit('result') },
        ],
      };

      expect(isCaseConstant(constantCase)).toBe(true);
      expect(isCaseConstant(nonConstantCase)).toBe(false);
    });

    it('should fold constant CASE expressions', () => {
      const constantCase: SearchedCaseExpr = {
        type: 'case',
        caseStyle: 'searched',
        when: [
          { condition: lit(true), result: lit('result') },
        ],
        else: lit('default'),
      };

      const folded = foldConstantCase(constantCase);
      expect(folded).not.toBeNull();
      expect(folded?.type).toBe('literal');
      expect((folded as any).value).toBe('result');
    });
  });

  describe('Helper Functions', () => {
    describe('isTruthy', () => {
      it('should return false for falsy values', () => {
        expect(isTruthy(null)).toBe(false);
        expect(isTruthy(false)).toBe(false);
        expect(isTruthy(0)).toBe(false);
        expect(isTruthy(0n)).toBe(false);
        expect(isTruthy('')).toBe(false);
      });

      it('should return true for truthy values', () => {
        expect(isTruthy(true)).toBe(true);
        expect(isTruthy(1)).toBe(true);
        expect(isTruthy('string')).toBe(true);
        expect(isTruthy(1n)).toBe(true);
      });
    });

    describe('valuesEqual', () => {
      it('should handle NULL comparisons', () => {
        expect(valuesEqual(null, null)).toBe(false); // SQL: NULL = NULL is false
        expect(valuesEqual(null, 1)).toBe(false);
        expect(valuesEqual(1, null)).toBe(false);
      });

      it('should compare dates', () => {
        const d1 = new Date('2024-01-01');
        const d2 = new Date('2024-01-01');
        const d3 = new Date('2024-01-02');
        expect(valuesEqual(d1, d2)).toBe(true);
        expect(valuesEqual(d1, d3)).toBe(false);
      });

      it('should compare Uint8Arrays', () => {
        const a1 = new Uint8Array([1, 2, 3]);
        const a2 = new Uint8Array([1, 2, 3]);
        const a3 = new Uint8Array([1, 2, 4]);
        expect(valuesEqual(a1, a2)).toBe(true);
        expect(valuesEqual(a1, a3)).toBe(false);
      });

      it('should compare bigints', () => {
        expect(valuesEqual(1n, 1n)).toBe(true);
        expect(valuesEqual(1n, 2n)).toBe(false);
        expect(valuesEqual(1n, 1)).toBe(true); // Cross-type
        expect(valuesEqual(1, 1n)).toBe(true);
      });
    });
  });
});

// =============================================================================
// CTE OPERATOR TESTS
// =============================================================================

describe('CTE (Common Table Expression) Operator', () => {
  describe('CTE Context', () => {
    it('should create CTE context with default limits', () => {
      const baseCtx = createMockExecutionContext();
      const cteCtx = createCTEContext(baseCtx);

      expect(cteCtx.materializedCTEs).toBeDefined();
      expect(cteCtx.materializedCTEs.size).toBe(0);
      expect(cteCtx.recursiveLimits.maxIterations).toBe(1000);
      expect(cteCtx.recursiveLimits.maxRows).toBe(100000);
    });

    it('should create CTE context with custom limits', () => {
      const baseCtx = createMockExecutionContext();
      const cteCtx = createCTEContext(baseCtx, { maxIterations: 100, maxRows: 1000 });

      expect(cteCtx.recursiveLimits.maxIterations).toBe(100);
      expect(cteCtx.recursiveLimits.maxRows).toBe(1000);
    });
  });

  describe('Simple CTE Execution', () => {
    it('should execute simple CTE and materialize results', async () => {
      const baseCtx = createMockExecutionContext();
      const cteCtx = createCTEContext(baseCtx);

      const testRows: Row[] = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ];

      const mockExecutor = createMockQueryExecutor(new Map([
        ['employees', testRows],
      ]));

      const cte: CTEDefinition = {
        name: 'active_employees',
        query: 'SELECT * FROM employees',
        recursive: false,
      };

      const materialized = await executeSimpleCTE(cte, mockExecutor, cteCtx);

      expect(materialized.name).toBe('active_employees');
      expect(materialized.rows).toHaveLength(2);
      expect(materialized.rows[0].id).toBe(1);
    });

    it('should rename columns based on CTE column list', async () => {
      const baseCtx = createMockExecutionContext();
      const cteCtx = createCTEContext(baseCtx);

      const testRows: Row[] = [
        { id: 1, name: 'Alice' },
      ];

      const mockExecutor = createMockQueryExecutor(new Map([
        ['employees', testRows],
      ]));

      const cte: CTEDefinition = {
        name: 'emp',
        columns: ['emp_id', 'emp_name'],
        query: 'SELECT * FROM employees',
        recursive: false,
      };

      const materialized = await executeSimpleCTE(cte, mockExecutor, cteCtx);

      expect(materialized.columns).toEqual(['emp_id', 'emp_name']);
      expect(materialized.rows[0].emp_id).toBe(1);
      expect(materialized.rows[0].emp_name).toBe('Alice');
    });

    it('should throw on column count mismatch', async () => {
      const baseCtx = createMockExecutionContext();
      const cteCtx = createCTEContext(baseCtx);

      const testRows: Row[] = [
        { id: 1, name: 'Alice', dept: 'Engineering' },
      ];

      const mockExecutor = createMockQueryExecutor(new Map([
        ['employees', testRows],
      ]));

      const cte: CTEDefinition = {
        name: 'emp',
        columns: ['a', 'b'], // Only 2 columns but result has 3
        query: 'SELECT * FROM employees',
        recursive: false,
      };

      await expect(executeSimpleCTE(cte, mockExecutor, cteCtx)).rejects.toThrow('column count mismatch');
    });
  });

  describe('Multiple CTEs', () => {
    it('should execute multiple CTEs in order', async () => {
      const baseCtx = createMockExecutionContext();

      const employees: Row[] = [
        { id: 1, name: 'Alice', dept_id: 1 },
        { id: 2, name: 'Bob', dept_id: 2 },
      ];
      const departments: Row[] = [
        { id: 1, name: 'Engineering' },
        { id: 2, name: 'Sales' },
      ];

      const mockExecutor = createMockQueryExecutor(new Map([
        ['employees', employees],
        ['departments', departments],
      ]));

      const withClause: WithClause = {
        type: 'with',
        recursive: false,
        ctes: [
          { name: 'emp', query: 'SELECT * FROM employees', recursive: false },
          { name: 'dept', query: 'SELECT * FROM departments', recursive: false },
        ],
      };

      const cteCtx = await executeWithClause(withClause, mockExecutor, baseCtx);

      expect(cteCtx.materializedCTEs.size).toBe(2);
      expect(isCTEMaterialized(cteCtx, 'emp')).toBe(true);
      expect(isCTEMaterialized(cteCtx, 'dept')).toBe(true);
      expect(getCTERows(cteCtx, 'emp')).toHaveLength(2);
      expect(getCTEColumns(cteCtx, 'dept')).toContain('id');
    });

    it('should allow later CTE to reference earlier CTE', async () => {
      const baseCtx = createMockExecutionContext();

      const employees: Row[] = [
        { id: 1, name: 'Alice' },
      ];

      // Mock executor that can handle CTE references
      const mockExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
        const cteCtx = ctx as CTEExecutionContext;
        if (sql.includes('FROM emp')) {
          return cteCtx.materializedCTEs?.get('emp')?.rows ?? [];
        }
        if (sql.includes('FROM employees')) {
          return employees;
        }
        return [];
      };

      const withClause: WithClause = {
        type: 'with',
        recursive: false,
        ctes: [
          { name: 'emp', query: 'SELECT * FROM employees', recursive: false },
          { name: 'emp_names', query: 'SELECT name FROM emp', recursive: false },
        ],
      };

      const cteCtx = await executeWithClause(withClause, mockExecutor, baseCtx);

      expect(cteCtx.materializedCTEs.size).toBe(2);
      // Second CTE should have been able to reference first CTE
      expect(getCTERows(cteCtx, 'emp_names')).toBeDefined();
    });
  });

  describe('Recursive CTE', () => {
    it('should execute recursive CTE with fixed point termination', async () => {
      const baseCtx = createMockExecutionContext();
      const cteCtx = createCTEContext(baseCtx, { maxIterations: 10, maxRows: 100 });

      // Simulating: numbers 1, 2, 3 where anchor is 1 and recursive adds 1
      let iteration = 0;
      const mockExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
        if (sql === 'SELECT 1 AS n') {
          return [{ n: 1 }];
        }
        // Recursive: SELECT n+1 FROM cte WHERE n < 3
        const cCtx = ctx as CTEExecutionContext;
        const current = cCtx.materializedCTEs?.get('numbers')?.rows ?? [];
        if (current.length === 0) return [];

        const maxN = Math.max(...current.map(r => r.n as number));
        if (maxN >= 3) return [];
        return [{ n: maxN + 1 }];
      };

      const cte: CTEDefinition = {
        name: 'numbers',
        recursive: true,
        query: 'SELECT 1 AS n UNION ALL SELECT n+1 FROM numbers WHERE n < 3',
        anchorQuery: 'SELECT 1 AS n',
        recursiveQuery: 'SELECT n+1 FROM numbers WHERE n < 3',
      };

      const materialized = await executeRecursiveCTE(cte, mockExecutor, cteCtx);

      expect(materialized.rows.length).toBeGreaterThanOrEqual(1);
      expect(materialized.rows.some(r => r.n === 1)).toBe(true);
    });

    it('should handle empty anchor query', async () => {
      const baseCtx = createMockExecutionContext();
      const cteCtx = createCTEContext(baseCtx);

      const mockExecutor = async (): Promise<Row[]> => [];

      const cte: CTEDefinition = {
        name: 'empty_cte',
        recursive: true,
        query: 'SELECT * FROM nothing',
        anchorQuery: 'SELECT * FROM nothing',
        recursiveQuery: 'SELECT * FROM empty_cte',
      };

      const materialized = await executeRecursiveCTE(cte, mockExecutor, cteCtx);

      expect(materialized.rows).toHaveLength(0);
    });

    it('should throw on missing anchor or recursive query', async () => {
      const baseCtx = createMockExecutionContext();
      const cteCtx = createCTEContext(baseCtx);

      const mockExecutor = async (): Promise<Row[]> => [];

      const cte: CTEDefinition = {
        name: 'bad_cte',
        recursive: true,
        query: 'SELECT 1',
        // Missing anchorQuery and recursiveQuery
      };

      await expect(executeRecursiveCTE(cte, mockExecutor, cteCtx)).rejects.toThrow('anchor and recursive queries');
    });

    it('should respect max iterations limit', async () => {
      const baseCtx = createMockExecutionContext();
      const cteCtx = createCTEContext(baseCtx, { maxIterations: 3, maxRows: 100 });

      // Infinite loop: always returns unique new rows to prevent deduplication termination
      let counter = 0;
      const mockExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
        if (sql.includes('anchor')) {
          return [{ n: counter++, unique_id: `anchor_${counter}` }];
        }
        // Always return a new unique row to prevent deduplication from stopping iteration
        const result = [{ n: counter++, unique_id: `recursive_${counter}` }];
        return result;
      };

      const cte: CTEDefinition = {
        name: 'infinite',
        recursive: true,
        query: 'infinite loop',
        anchorQuery: 'anchor',
        recursiveQuery: 'recursive',
      };

      await expect(executeRecursiveCTE(cte, mockExecutor, cteCtx)).rejects.toThrow('exceeded maximum iterations');
    });
  });

  describe('CTE Scan Operator', () => {
    it('should scan materialized CTE rows', async () => {
      const baseCtx = createMockExecutionContext();
      const cteCtx = createCTEContext(baseCtx);

      // Manually materialize a CTE
      cteCtx.materializedCTEs.set('users', {
        name: 'users',
        rows: [
          { id: 1, name: 'Alice' },
          { id: 2, name: 'Bob' },
        ],
        columns: ['id', 'name'],
      });

      const scanOp = new CTEScanOperator('users');
      const rows = await collectRows(scanOp, cteCtx);

      expect(rows).toHaveLength(2);
      expect(rows[0].name).toBe('Alice');
      expect(rows[1].name).toBe('Bob');
    });

    it('should throw if CTE is not materialized', async () => {
      const baseCtx = createMockExecutionContext();
      const cteCtx = createCTEContext(baseCtx);

      const scanOp = new CTEScanOperator('nonexistent');

      await expect(scanOp.open(cteCtx)).rejects.toThrow('not materialized');
    });

    it('should apply alias to scanned rows', async () => {
      const baseCtx = createMockExecutionContext();
      const cteCtx = createCTEContext(baseCtx);

      cteCtx.materializedCTEs.set('users', {
        name: 'users',
        rows: [{ id: 1, name: 'Alice' }],
        columns: ['id', 'name'],
      });

      const scanOp = new CTEScanOperator('users', 'u');
      const rows = await collectRows(scanOp, cteCtx);

      expect(rows[0]['u.id']).toBe(1);
      expect(rows[0]['u.name']).toBe('Alice');
    });
  });
});

// =============================================================================
// FILTER OPERATOR TESTS
// =============================================================================

describe('Filter Operator', () => {
  describe('Expression Evaluation', () => {
    it('should evaluate column references', () => {
      const row: Row = { name: 'Alice', age: 30 };
      expect(evaluateExpression(col('name'), row)).toBe('Alice');
      expect(evaluateExpression(col('age'), row)).toBe(30);
      expect(evaluateExpression(col('missing'), row)).toBeNull();
    });

    it('should evaluate table-qualified column references', () => {
      const row: Row = { 'users.name': 'Alice', name: 'Bob' };
      expect(evaluateExpression({ type: 'columnRef', table: 'users', column: 'name' }, row)).toBe('Alice');
    });

    it('should evaluate literals', () => {
      expect(evaluateExpression(lit(42), {})).toBe(42);
      expect(evaluateExpression(lit('hello'), {})).toBe('hello');
      expect(evaluateExpression(lit(null), {})).toBeNull();
    });

    describe('Arithmetic Operations', () => {
      it('should add numbers', () => {
        const expr: Expression = { type: 'binary', op: 'add', left: lit(5), right: lit(3) };
        expect(evaluateExpression(expr, {})).toBe(8);
      });

      it('should subtract numbers', () => {
        const expr: Expression = { type: 'binary', op: 'sub', left: lit(10), right: lit(4) };
        expect(evaluateExpression(expr, {})).toBe(6);
      });

      it('should multiply numbers', () => {
        const expr: Expression = { type: 'binary', op: 'mul', left: lit(6), right: lit(7) };
        expect(evaluateExpression(expr, {})).toBe(42);
      });

      it('should divide numbers', () => {
        const expr: Expression = { type: 'binary', op: 'div', left: lit(15), right: lit(3) };
        expect(evaluateExpression(expr, {})).toBe(5);
      });

      it('should handle division by zero', () => {
        const expr: Expression = { type: 'binary', op: 'div', left: lit(10), right: lit(0) };
        expect(evaluateExpression(expr, {})).toBeNull();
      });

      it('should calculate modulo', () => {
        const expr: Expression = { type: 'binary', op: 'mod', left: lit(17), right: lit(5) };
        expect(evaluateExpression(expr, {})).toBe(2);
      });

      it('should handle bigint arithmetic', () => {
        const add: Expression = { type: 'binary', op: 'add', left: lit(10n), right: lit(5n) };
        expect(evaluateExpression(add, {})).toBe(15n);
      });
    });

    describe('Comparison Operations', () => {
      // SQL comparisons return 1 for true, 0 for false, null if either operand is null
      it('should evaluate equality', () => {
        const eq: Expression = { type: 'binary', op: 'eq', left: col('x'), right: lit(5) };
        expect(evaluateExpression(eq, { x: 5 })).toBe(1);
        expect(evaluateExpression(eq, { x: 6 })).toBe(0);
      });

      it('should evaluate inequality', () => {
        const ne: Expression = { type: 'binary', op: 'ne', left: col('x'), right: lit(5) };
        expect(evaluateExpression(ne, { x: 5 })).toBe(0);
        expect(evaluateExpression(ne, { x: 6 })).toBe(1);
      });

      it('should evaluate less than', () => {
        const lt: Expression = { type: 'binary', op: 'lt', left: col('x'), right: lit(5) };
        expect(evaluateExpression(lt, { x: 3 })).toBe(1);
        expect(evaluateExpression(lt, { x: 5 })).toBe(0);
      });

      it('should evaluate less than or equal', () => {
        const le: Expression = { type: 'binary', op: 'le', left: col('x'), right: lit(5) };
        expect(evaluateExpression(le, { x: 5 })).toBe(1);
        expect(evaluateExpression(le, { x: 6 })).toBe(0);
      });

      it('should evaluate greater than', () => {
        const gt: Expression = { type: 'binary', op: 'gt', left: col('x'), right: lit(5) };
        expect(evaluateExpression(gt, { x: 7 })).toBe(1);
        expect(evaluateExpression(gt, { x: 5 })).toBe(0);
      });

      it('should evaluate greater than or equal', () => {
        const ge: Expression = { type: 'binary', op: 'ge', left: col('x'), right: lit(5) };
        expect(evaluateExpression(ge, { x: 5 })).toBe(1);
        expect(evaluateExpression(ge, { x: 4 })).toBe(0);
      });

      it('should handle NULL in comparisons', () => {
        // In SQL, comparisons with NULL return NULL (not false)
        const lt: Expression = { type: 'binary', op: 'lt', left: col('x'), right: lit(5) };
        expect(evaluateExpression(lt, { x: null })).toBe(null);
      });
    });

    describe('LIKE Pattern Matching', () => {
      // SQL LIKE returns 1 for match, 0 for no match, null if either operand is null
      it('should match % wildcard', () => {
        const like: Expression = { type: 'binary', op: 'like', left: col('name'), right: lit('%son') };
        expect(evaluateExpression(like, { name: 'Johnson' })).toBe(1);
        expect(evaluateExpression(like, { name: 'Smith' })).toBe(0);
      });

      it('should match _ wildcard', () => {
        const like: Expression = { type: 'binary', op: 'like', left: col('code'), right: lit('A_C') };
        expect(evaluateExpression(like, { code: 'ABC' })).toBe(1);
        expect(evaluateExpression(like, { code: 'ABCD' })).toBe(0);
      });

      it('should match combined patterns', () => {
        const like: Expression = { type: 'binary', op: 'like', left: col('email'), right: lit('%@%.com') };
        expect(evaluateExpression(like, { email: 'user@example.com' })).toBe(1);
        expect(evaluateExpression(like, { email: 'user@example.org' })).toBe(0);
      });
    });

    describe('Logical Operations', () => {
      // SQL AND/OR return 1 for true, 0 for false, null with proper three-valued logic
      it('should evaluate AND', () => {
        const and: Expression = {
          type: 'binary',
          op: 'and',
          left: { type: 'binary', op: 'gt', left: col('x'), right: lit(0) },
          right: { type: 'binary', op: 'lt', left: col('x'), right: lit(10) },
        };
        expect(evaluateExpression(and, { x: 5 })).toBe(1);
        expect(evaluateExpression(and, { x: 15 })).toBe(0);
      });

      it('should evaluate OR', () => {
        const or: Expression = {
          type: 'binary',
          op: 'or',
          left: { type: 'binary', op: 'eq', left: col('x'), right: lit(1) },
          right: { type: 'binary', op: 'eq', left: col('x'), right: lit(2) },
        };
        expect(evaluateExpression(or, { x: 1 })).toBe(1);
        expect(evaluateExpression(or, { x: 2 })).toBe(1);
        expect(evaluateExpression(or, { x: 3 })).toBe(0);
      });
    });

    describe('Unary Operations', () => {
      // SQL NOT returns 1 for true, 0 for false, null if operand is null
      it('should evaluate NOT', () => {
        const not: Expression = { type: 'unary', op: 'not', operand: lit(true) };
        expect(evaluateExpression(not, {})).toBe(0);
      });

      it('should evaluate negation', () => {
        const neg: Expression = { type: 'unary', op: 'neg', operand: lit(5) };
        expect(evaluateExpression(neg, {})).toBe(-5);
      });

      // SQL IS NULL always returns 1 or 0 (never null)
      it('should evaluate IS NULL', () => {
        const isNull: Expression = { type: 'unary', op: 'isNull', operand: col('x') };
        expect(evaluateExpression(isNull, { x: null })).toBe(1);
        expect(evaluateExpression(isNull, { x: 5 })).toBe(0);
      });

      // SQL IS NOT NULL always returns 1 or 0 (never null)
      it('should evaluate IS NOT NULL', () => {
        const isNotNull: Expression = { type: 'unary', op: 'isNotNull', operand: col('x') };
        expect(evaluateExpression(isNotNull, { x: null })).toBe(0);
        expect(evaluateExpression(isNotNull, { x: 5 })).toBe(1);
      });
    });

    /**
     * SQL Comparison Operator Semantics (Issue sql-fzxw)
     *
     * This test suite verifies that comparison operators (<, >, <=, >=, =, !=)
     * follow proper SQL semantics:
     * - Return 1 for true, 0 for false (not JavaScript boolean)
     * - Return NULL when either operand is NULL
     *
     * This is critical for:
     * - SELECT 1<2 returning 1 (not null or true)
     * - WHERE clauses working correctly with comparisons
     * - CASE WHEN conditions evaluating properly
     */
    describe('SQL Comparison Semantics (sql-fzxw fix)', () => {
      it('SELECT 1<2 should return 1 (true)', () => {
        const lt: Expression = { type: 'binary', op: 'lt', left: lit(1), right: lit(2) };
        expect(evaluateExpression(lt, {})).toBe(1);
      });

      it('SELECT 2<1 should return 0 (false)', () => {
        const lt: Expression = { type: 'binary', op: 'lt', left: lit(2), right: lit(1) };
        expect(evaluateExpression(lt, {})).toBe(0);
      });

      it('SELECT 2>1 should return 1 (true)', () => {
        const gt: Expression = { type: 'binary', op: 'gt', left: lit(2), right: lit(1) };
        expect(evaluateExpression(gt, {})).toBe(1);
      });

      it('SELECT 1>2 should return 0 (false)', () => {
        const gt: Expression = { type: 'binary', op: 'gt', left: lit(1), right: lit(2) };
        expect(evaluateExpression(gt, {})).toBe(0);
      });

      it('SELECT 1<=1 should return 1 (true)', () => {
        const le: Expression = { type: 'binary', op: 'le', left: lit(1), right: lit(1) };
        expect(evaluateExpression(le, {})).toBe(1);
      });

      it('SELECT 1>=1 should return 1 (true)', () => {
        const ge: Expression = { type: 'binary', op: 'ge', left: lit(1), right: lit(1) };
        expect(evaluateExpression(ge, {})).toBe(1);
      });

      it('comparison with column values should return 1 or 0', () => {
        const lt: Expression = { type: 'binary', op: 'lt', left: col('a'), right: col('b') };
        expect(evaluateExpression(lt, { a: 5, b: 10 })).toBe(1);
        expect(evaluateExpression(lt, { a: 10, b: 5 })).toBe(0);
      });

      it('comparison with NULL should return NULL', () => {
        const lt: Expression = { type: 'binary', op: 'lt', left: col('a'), right: col('b') };
        expect(evaluateExpression(lt, { a: 5, b: null })).toBe(null);
        expect(evaluateExpression(lt, { a: null, b: 10 })).toBe(null);
        expect(evaluateExpression(lt, { a: null, b: null })).toBe(null);
      });

      it('equality with NULL should return NULL', () => {
        const eq: Expression = { type: 'binary', op: 'eq', left: col('a'), right: lit(null) };
        expect(evaluateExpression(eq, { a: 5 })).toBe(null);
        expect(evaluateExpression(eq, { a: null })).toBe(null);
      });

      it('all comparison operators return integers', () => {
        const row = { x: 5, y: 10 };
        expect(evaluateExpression({ type: 'binary', op: 'lt', left: col('x'), right: col('y') }, row)).toBe(1);
        expect(evaluateExpression({ type: 'binary', op: 'gt', left: col('y'), right: col('x') }, row)).toBe(1);
        expect(evaluateExpression({ type: 'binary', op: 'le', left: col('x'), right: col('x') }, row)).toBe(1);
        expect(evaluateExpression({ type: 'binary', op: 'ge', left: col('y'), right: col('y') }, row)).toBe(1);
        expect(evaluateExpression({ type: 'binary', op: 'eq', left: col('x'), right: col('x') }, row)).toBe(1);
        expect(evaluateExpression({ type: 'binary', op: 'ne', left: col('x'), right: col('y') }, row)).toBe(1);
      });
    });

    describe('Built-in Functions', () => {
      it('should evaluate string functions', () => {
        const row = { s: 'Hello World' };
        expect(evaluateExpression({ type: 'function', name: 'upper', args: [col('s')] }, row)).toBe('HELLO WORLD');
        expect(evaluateExpression({ type: 'function', name: 'lower', args: [col('s')] }, row)).toBe('hello world');
        expect(evaluateExpression({ type: 'function', name: 'length', args: [col('s')] }, row)).toBe(11);
        expect(evaluateExpression({ type: 'function', name: 'trim', args: [lit('  hi  ')] }, {})).toBe('hi');
      });

      it('should evaluate numeric functions', () => {
        expect(evaluateExpression({ type: 'function', name: 'abs', args: [lit(-5)] }, {})).toBe(5);
        expect(evaluateExpression({ type: 'function', name: 'ceil', args: [lit(4.3)] }, {})).toBe(5);
        expect(evaluateExpression({ type: 'function', name: 'floor', args: [lit(4.7)] }, {})).toBe(4);
        expect(evaluateExpression({ type: 'function', name: 'round', args: [lit(4.567), lit(2)] }, {})).toBe(4.57);
        expect(evaluateExpression({ type: 'function', name: 'sqrt', args: [lit(16)] }, {})).toBe(4);
      });

      it('should evaluate date functions', () => {
        const date = new Date('2024-06-15T10:30:45Z');
        expect(evaluateExpression({ type: 'function', name: 'year', args: [lit(date)] }, {})).toBe(2024);
        expect(evaluateExpression({ type: 'function', name: 'month', args: [lit(date)] }, {})).toBe(6);
        expect(evaluateExpression({ type: 'function', name: 'day', args: [lit(date)] }, {})).toBe(15);
      });

      it('should evaluate COALESCE function', () => {
        expect(evaluateExpression({ type: 'function', name: 'coalesce', args: [lit(null), lit(null), lit(3)] }, {})).toBe(3);
      });

      it('should evaluate CONCAT function', () => {
        expect(evaluateExpression({ type: 'function', name: 'concat', args: [lit('Hello'), lit(' '), lit('World')] }, {})).toBe('Hello World');
      });

      it('should evaluate SUBSTRING function', () => {
        expect(evaluateExpression({ type: 'function', name: 'substring', args: [lit('Hello'), lit(1), lit(3)] }, {})).toBe('Hel');
      });
    });
  });

  describe('Predicate Evaluation', () => {
    describe('Comparison Predicates', () => {
      it('should evaluate comparison predicate', () => {
        const pred: Predicate = {
          type: 'comparison',
          op: 'eq',
          left: col('status'),
          right: lit('active'),
        };
        expect(evaluatePredicate(pred, { status: 'active' })).toBe(true);
        expect(evaluatePredicate(pred, { status: 'inactive' })).toBe(false);
      });

      it('should handle NULL in comparison predicate', () => {
        const pred: Predicate = {
          type: 'comparison',
          op: 'eq',
          left: col('x'),
          right: lit(5),
        };
        expect(evaluatePredicate(pred, { x: null })).toBe(false);
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
        expect(evaluatePredicate(pred, { age: 10 })).toBe(false);
        expect(evaluatePredicate(pred, { age: 70 })).toBe(false);
      });

      it('should evaluate OR predicate', () => {
        const pred: Predicate = {
          type: 'logical',
          op: 'or',
          operands: [
            { type: 'comparison', op: 'eq', left: col('role'), right: lit('admin') },
            { type: 'comparison', op: 'eq', left: col('role'), right: lit('superuser') },
          ],
        };
        expect(evaluatePredicate(pred, { role: 'admin' })).toBe(true);
        expect(evaluatePredicate(pred, { role: 'superuser' })).toBe(true);
        expect(evaluatePredicate(pred, { role: 'user' })).toBe(false);
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

      it('should handle NULL in BETWEEN', () => {
        const pred: Predicate = {
          type: 'between',
          expr: col('age'),
          low: lit(18),
          high: lit(65),
        };
        expect(evaluatePredicate(pred, { age: null })).toBe(false);
      });

      it('should evaluate NOT BETWEEN predicate (via logical NOT)', () => {
        // NOT BETWEEN is represented as: { type: 'logical', op: 'not', operands: [BETWEEN] }
        const pred: Predicate = {
          type: 'logical',
          op: 'not',
          operands: [{
            type: 'between',
            expr: col('d'),
            low: lit(110),
            high: lit(150),
          }],
        };
        expect(evaluatePredicate(pred, { d: 100 })).toBe(true);   // 100 NOT BETWEEN 110 AND 150 = true
        expect(evaluatePredicate(pred, { d: 110 })).toBe(false);  // 110 NOT BETWEEN 110 AND 150 = false (boundary)
        expect(evaluatePredicate(pred, { d: 130 })).toBe(false);  // 130 NOT BETWEEN 110 AND 150 = false
        expect(evaluatePredicate(pred, { d: 150 })).toBe(false);  // 150 NOT BETWEEN 110 AND 150 = false (boundary)
        expect(evaluatePredicate(pred, { d: 160 })).toBe(true);   // 160 NOT BETWEEN 110 AND 150 = true
      });

      it('should evaluate BETWEEN with expression bounds', () => {
        // c BETWEEN b-2 AND d+2 where b=50, c=60, d=70 should be true (60 >= 48 AND 60 <= 72)
        const pred: Predicate = {
          type: 'between',
          expr: col('c'),
          low: { type: 'binary', op: 'sub', left: col('b'), right: lit(2) },
          high: { type: 'binary', op: 'add', left: col('d'), right: lit(2) },
        };
        expect(evaluatePredicate(pred, { b: 50, c: 60, d: 70 })).toBe(true);  // 60 BETWEEN 48 AND 72
        expect(evaluatePredicate(pred, { b: 50, c: 47, d: 70 })).toBe(false); // 47 < 48
        expect(evaluatePredicate(pred, { b: 50, c: 73, d: 70 })).toBe(false); // 73 > 72
        expect(evaluatePredicate(pred, { b: 50, c: 48, d: 70 })).toBe(true);  // 48 BETWEEN 48 AND 72 (boundary)
        expect(evaluatePredicate(pred, { b: 50, c: 72, d: 70 })).toBe(true);  // 72 BETWEEN 48 AND 72 (boundary)
      });

      it('should evaluate complex OR with NOT BETWEEN', () => {
        // d NOT BETWEEN 110 AND 150 OR c BETWEEN b-2 AND d+2 OR (e>c OR e<d)
        const notBetween: Predicate = {
          type: 'logical',
          op: 'not',
          operands: [{
            type: 'between',
            expr: col('d'),
            low: lit(110),
            high: lit(150),
          }],
        };
        const betweenExpr: Predicate = {
          type: 'between',
          expr: col('c'),
          low: { type: 'binary', op: 'sub', left: col('b'), right: lit(2) },
          high: { type: 'binary', op: 'add', left: col('d'), right: lit(2) },
        };
        const nestedOr: Predicate = {
          type: 'logical',
          op: 'or',
          operands: [
            { type: 'comparison', op: 'gt', left: col('e'), right: col('c') },
            { type: 'comparison', op: 'lt', left: col('e'), right: col('d') },
          ],
        };
        const fullPred: Predicate = {
          type: 'logical',
          op: 'or',
          operands: [notBetween, betweenExpr, nestedOr],
        };

        // Test case 1: d=100, which is NOT BETWEEN 110-150, so should be true
        expect(evaluatePredicate(fullPred, { b: 50, c: 60, d: 100, e: 55 })).toBe(true);

        // Test case 2: d=130 (BETWEEN 110-150), c=60 (BETWEEN 48 AND 132), so 2nd condition true
        expect(evaluatePredicate(fullPred, { b: 50, c: 60, d: 130, e: 55 })).toBe(true);

        // Test case 3: all conditions false
        // d=120 (BETWEEN 110-150, so NOT BETWEEN is false)
        // c=90 with b=50, d=120 -> BETWEEN 48 AND 122, so 90 is in range -> true
        expect(evaluatePredicate(fullPred, { b: 50, c: 90, d: 120, e: 90 })).toBe(true);
      });
    });

    describe('IN Predicate', () => {
      it('should evaluate IN predicate with list', () => {
        const pred: Predicate = {
          type: 'in',
          expr: col('status'),
          values: [lit('active'), lit('pending'), lit('approved')],
        };
        expect(evaluatePredicate(pred, { status: 'active' })).toBe(true);
        expect(evaluatePredicate(pred, { status: 'pending' })).toBe(true);
        expect(evaluatePredicate(pred, { status: 'closed' })).toBe(false);
      });

      it('should handle NULL in IN predicate', () => {
        const pred: Predicate = {
          type: 'in',
          expr: col('x'),
          values: [lit(1), lit(2), lit(3)],
        };
        expect(evaluatePredicate(pred, { x: null })).toBe(false);
      });
    });

    describe('IS NULL Predicate', () => {
      it('should evaluate IS NULL predicate', () => {
        const pred: Predicate = {
          type: 'isNull',
          expr: col('deleted_at'),
          isNot: false,
        };
        expect(evaluatePredicate(pred, { deleted_at: null })).toBe(true);
        expect(evaluatePredicate(pred, { deleted_at: '2024-01-01' })).toBe(false);
      });

      it('should evaluate IS NOT NULL predicate', () => {
        const pred: Predicate = {
          type: 'isNull',
          expr: col('deleted_at'),
          isNot: true,
        };
        expect(evaluatePredicate(pred, { deleted_at: null })).toBe(false);
        expect(evaluatePredicate(pred, { deleted_at: '2024-01-01' })).toBe(true);
      });
    });
  });

  describe('FilterOperator', () => {
    it('should filter rows matching predicate', async () => {
      const inputRows: Row[] = [
        { id: 1, status: 'active' },
        { id: 2, status: 'inactive' },
        { id: 3, status: 'active' },
        { id: 4, status: 'pending' },
      ];

      const mockInput = createMockOperator(inputRows);
      const ctx = createMockExecutionContext();

      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: {} as QueryPlan,
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: col('status'),
          right: lit('active'),
        },
      };

      const filterOp = new FilterOperator(plan, mockInput, ctx);
      const results = await collectRows(filterOp, ctx);

      expect(results).toHaveLength(2);
      expect(results[0].id).toBe(1);
      expect(results[1].id).toBe(3);
    });

    it('should return empty when no rows match', async () => {
      const inputRows: Row[] = [
        { id: 1, status: 'inactive' },
        { id: 2, status: 'inactive' },
      ];

      const mockInput = createMockOperator(inputRows);
      const ctx = createMockExecutionContext();

      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: {} as QueryPlan,
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: col('status'),
          right: lit('active'),
        },
      };

      const filterOp = new FilterOperator(plan, mockInput, ctx);
      const results = await collectRows(filterOp, ctx);

      expect(results).toHaveLength(0);
    });

    it('should handle complex AND/OR predicates', async () => {
      const inputRows: Row[] = [
        { id: 1, age: 25, country: 'US' },
        { id: 2, age: 17, country: 'US' },
        { id: 3, age: 25, country: 'UK' },
        { id: 4, age: 30, country: 'US' },
      ];

      const mockInput = createMockOperator(inputRows);
      const ctx = createMockExecutionContext();

      // age >= 18 AND country = 'US'
      const plan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: {} as QueryPlan,
        predicate: {
          type: 'logical',
          op: 'and',
          operands: [
            { type: 'comparison', op: 'ge', left: col('age'), right: lit(18) },
            { type: 'comparison', op: 'eq', left: col('country'), right: lit('US') },
          ],
        },
      };

      const filterOp = new FilterOperator(plan, mockInput, ctx);
      const results = await collectRows(filterOp, ctx);

      expect(results).toHaveLength(2);
      expect(results[0].id).toBe(1);
      expect(results[1].id).toBe(4);
    });
  });
});

// =============================================================================
// SET OPERATIONS TESTS
// =============================================================================

describe('Set Operations Operator', () => {
  describe('UNION', () => {
    it('should combine results and remove duplicates', async () => {
      const left: Row[] = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ];
      const right: Row[] = [
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
      ];

      const results = await executeSetOperation('UNION', left, right, false);

      expect(results).toHaveLength(3);
      expect(results.some(r => r.id === 1)).toBe(true);
      expect(results.some(r => r.id === 2)).toBe(true);
      expect(results.some(r => r.id === 3)).toBe(true);
    });

    it('should handle empty left side', async () => {
      const left: Row[] = [];
      const right: Row[] = [{ id: 1, name: 'Alice' }];

      const results = await executeSetOperation('UNION', left, right, false);
      expect(results).toHaveLength(1);
    });

    it('should handle empty right side', async () => {
      const left: Row[] = [{ id: 1, name: 'Alice' }];
      const right: Row[] = [];

      const results = await executeSetOperation('UNION', left, right, false);
      expect(results).toHaveLength(1);
    });
  });

  describe('UNION ALL', () => {
    it('should combine results and keep duplicates', async () => {
      const left: Row[] = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ];
      const right: Row[] = [
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
      ];

      const results = await executeSetOperation('UNION', left, right, true);

      expect(results).toHaveLength(4);
      // Bob should appear twice
      expect(results.filter(r => r.id === 2)).toHaveLength(2);
    });

    it('should preserve order (left then right)', async () => {
      const left: Row[] = [{ id: 1 }, { id: 2 }];
      const right: Row[] = [{ id: 3 }, { id: 4 }];

      const results = await executeSetOperation('UNION', left, right, true);

      expect(results.map(r => r.id)).toEqual([1, 2, 3, 4]);
    });
  });

  describe('INTERSECT', () => {
    it('should return only common rows', async () => {
      const left: Row[] = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
      ];
      const right: Row[] = [
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
        { id: 4, name: 'Diana' },
      ];

      const results = await executeSetOperation('INTERSECT', left, right, false);

      expect(results).toHaveLength(2);
      expect(results.some(r => r.id === 2)).toBe(true);
      expect(results.some(r => r.id === 3)).toBe(true);
    });

    it('should return empty for disjoint sets', async () => {
      const left: Row[] = [{ id: 1 }, { id: 2 }];
      const right: Row[] = [{ id: 3 }, { id: 4 }];

      const results = await executeSetOperation('INTERSECT', left, right, false);
      expect(results).toHaveLength(0);
    });

    it('should remove duplicates from result', async () => {
      const left: Row[] = [
        { id: 1, name: 'Alice' },
        { id: 1, name: 'Alice' },
      ];
      const right: Row[] = [
        { id: 1, name: 'Alice' },
      ];

      const results = await executeSetOperation('INTERSECT', left, right, false);
      expect(results).toHaveLength(1);
    });
  });

  describe('INTERSECT ALL', () => {
    it('should use bag semantics for intersection', async () => {
      const left: Row[] = [
        { id: 1, name: 'Alice' },
        { id: 1, name: 'Alice' },
        { id: 1, name: 'Alice' },
      ];
      const right: Row[] = [
        { id: 1, name: 'Alice' },
        { id: 1, name: 'Alice' },
      ];

      const results = await executeSetOperation('INTERSECT', left, right, true);

      // min(3, 2) = 2 copies
      expect(results).toHaveLength(2);
    });
  });

  describe('EXCEPT', () => {
    it('should return rows in left but not in right', async () => {
      const left: Row[] = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
      ];
      const right: Row[] = [
        { id: 2, name: 'Bob' },
      ];

      const results = await executeSetOperation('EXCEPT', left, right, false);

      expect(results).toHaveLength(2);
      expect(results.some(r => r.id === 1)).toBe(true);
      expect(results.some(r => r.id === 3)).toBe(true);
      expect(results.some(r => r.id === 2)).toBe(false);
    });

    it('should return all rows when right is empty', async () => {
      const left: Row[] = [{ id: 1 }, { id: 2 }];
      const right: Row[] = [];

      const results = await executeSetOperation('EXCEPT', left, right, false);
      expect(results).toHaveLength(2);
    });

    it('should return empty when left is subset of right', async () => {
      const left: Row[] = [{ id: 1 }];
      const right: Row[] = [{ id: 1 }, { id: 2 }];

      const results = await executeSetOperation('EXCEPT', left, right, false);
      expect(results).toHaveLength(0);
    });
  });

  describe('EXCEPT ALL', () => {
    it('should use bag semantics for difference', async () => {
      const left: Row[] = [
        { id: 1, name: 'Alice' },
        { id: 1, name: 'Alice' },
        { id: 1, name: 'Alice' },
      ];
      const right: Row[] = [
        { id: 1, name: 'Alice' },
      ];

      const results = await executeSetOperation('EXCEPT', left, right, true);

      // 3 - 1 = 2 copies remain
      expect(results).toHaveLength(2);
    });
  });

  describe('Set Operation Factory', () => {
    it('should create correct operator type', () => {
      const left = createMockOperator([]);
      const right = createMockOperator([]);

      const unionOp = createSetOperationOperator('UNION', left, right, false);
      expect(unionOp).toBeInstanceOf(UnionOperator);

      const intersectOp = createSetOperationOperator('INTERSECT', left, right, false);
      expect(intersectOp).toBeInstanceOf(IntersectOperator);

      const exceptOp = createSetOperationOperator('EXCEPT', left, right, false);
      expect(exceptOp).toBeInstanceOf(ExceptOperator);
    });

    it('should throw for unknown operation', () => {
      const left = createMockOperator([]);
      const right = createMockOperator([]);

      expect(() => createSetOperationOperator('UNKNOWN' as any, left, right, false)).toThrow('Unknown set operation');
    });
  });

  describe('Set Operation Precedence', () => {
    it('should return correct precedence values', () => {
      expect(getSetOperationPrecedence('INTERSECT')).toBe(2);
      expect(getSetOperationPrecedence('UNION')).toBe(1);
      expect(getSetOperationPrecedence('EXCEPT')).toBe(1);
    });

    it('INTERSECT should bind tighter than UNION', () => {
      expect(getSetOperationPrecedence('INTERSECT')).toBeGreaterThan(getSetOperationPrecedence('UNION'));
    });
  });
});

// =============================================================================
// SUBQUERY OPERATOR TESTS
// =============================================================================

describe('Subquery Operators', () => {
  /**
   * Helper to create a mock operator factory
   */
  function createOperatorFactory(mockData: Map<string, Row[]>): (plan: QueryPlan, ctx: ExecutionContext) => Operator {
    return (plan: QueryPlan, ctx: ExecutionContext): Operator => {
      const scanPlan = plan as ScanPlan;
      const rows = mockData.get(scanPlan.table) ?? [];
      return createMockOperator(rows);
    };
  }

  describe('Scalar Subquery', () => {
    it('should return single value from subquery', async () => {
      const ctx: SubqueryContext = {
        ...createMockExecutionContext(),
      };

      const subqueryData = new Map([
        ['prices', [{ price: 100 }]],
      ]);

      const createOperator = createOperatorFactory(subqueryData);

      const scalarOp = new ScalarSubqueryOperator(
        {
          type: 'scalarSubquery',
          id: 1,
          subquery: { type: 'scan', id: 2, table: 'prices', source: 'btree', columns: ['price'] },
        },
        ctx,
        createOperator
      );

      const result = await scalarOp.execute();
      expect(result).toBe(100);
    });

    it('should return NULL for empty subquery', async () => {
      const ctx: SubqueryContext = {
        ...createMockExecutionContext(),
      };

      const subqueryData = new Map([
        ['empty', []],
      ]);

      const createOperator = createOperatorFactory(subqueryData);

      const scalarOp = new ScalarSubqueryOperator(
        {
          type: 'scalarSubquery',
          id: 1,
          subquery: { type: 'scan', id: 2, table: 'empty', source: 'btree', columns: ['value'] },
        },
        ctx,
        createOperator
      );

      const result = await scalarOp.execute();
      expect(result).toBeNull();
    });

    it('should throw error for multiple rows', async () => {
      const ctx: SubqueryContext = {
        ...createMockExecutionContext(),
      };

      const subqueryData = new Map([
        ['multiple', [{ value: 1 }, { value: 2 }]],
      ]);

      const createOperator = createOperatorFactory(subqueryData);

      const scalarOp = new ScalarSubqueryOperator(
        {
          type: 'scalarSubquery',
          id: 1,
          subquery: { type: 'scan', id: 2, table: 'multiple', source: 'btree', columns: ['value'] },
        },
        ctx,
        createOperator
      );

      await expect(scalarOp.execute()).rejects.toThrow('more than one row');
    });

    it('should cache result for non-correlated subquery', async () => {
      const ctx: SubqueryContext = {
        ...createMockExecutionContext(),
      };

      let callCount = 0;
      const createOperator = (): Operator => {
        callCount++;
        return createMockOperator([{ value: 42 }]);
      };

      const scalarOp = new ScalarSubqueryOperator(
        {
          type: 'scalarSubquery',
          id: 1,
          subquery: { type: 'scan', id: 2, table: 'values', source: 'btree', columns: ['value'] },
        },
        ctx,
        createOperator
      );

      // Execute twice
      await scalarOp.execute();
      await scalarOp.execute();

      // Should only create operator once due to caching
      expect(callCount).toBe(1);
    });
  });

  describe('IN Subquery', () => {
    it('should filter rows matching IN subquery', async () => {
      const ctx: SubqueryContext = {
        ...createMockExecutionContext(),
      };

      const mainRows: Row[] = [
        { id: 1, dept_id: 1 },
        { id: 2, dept_id: 2 },
        { id: 3, dept_id: 3 },
      ];

      const subqueryRows: Row[] = [
        { id: 1 },
        { id: 3 },
      ];

      const createOperator = (plan: QueryPlan): Operator => {
        if ((plan as ScanPlan).table === 'valid_depts') {
          return createMockOperator(subqueryRows);
        }
        return createMockOperator([]);
      };

      const inputOp = createMockOperator(mainRows);

      const inOp = new InSubqueryOperator(
        {
          type: 'inSubquery',
          id: 1,
          input: {} as QueryPlan,
          expr: col('dept_id'),
          subquery: { type: 'scan', id: 2, table: 'valid_depts', source: 'btree', columns: ['id'] },
          not: false,
        },
        inputOp,
        ctx,
        createOperator
      );

      const results = await collectRows(inOp, ctx);

      expect(results).toHaveLength(2);
      expect(results[0].dept_id).toBe(1);
      expect(results[1].dept_id).toBe(3);
    });

    it('should handle NOT IN subquery', async () => {
      const ctx: SubqueryContext = {
        ...createMockExecutionContext(),
      };

      const mainRows: Row[] = [
        { id: 1, dept_id: 1 },
        { id: 2, dept_id: 2 },
        { id: 3, dept_id: 3 },
      ];

      const excludedRows: Row[] = [
        { id: 1 },
      ];

      const createOperator = (): Operator => createMockOperator(excludedRows);
      const inputOp = createMockOperator(mainRows);

      const inOp = new InSubqueryOperator(
        {
          type: 'inSubquery',
          id: 1,
          input: {} as QueryPlan,
          expr: col('dept_id'),
          subquery: { type: 'scan', id: 2, table: 'excluded', source: 'btree', columns: ['id'] },
          not: true,
        },
        inputOp,
        ctx,
        createOperator
      );

      const results = await collectRows(inOp, ctx);

      expect(results).toHaveLength(2);
      expect(results[0].dept_id).toBe(2);
      expect(results[1].dept_id).toBe(3);
    });

    it('should handle NULL values in IN', async () => {
      const ctx: SubqueryContext = {
        ...createMockExecutionContext(),
      };

      const mainRows: Row[] = [
        { id: 1, dept_id: null },
        { id: 2, dept_id: 1 },
      ];

      const subqueryRows: Row[] = [{ id: 1 }];

      const createOperator = (): Operator => createMockOperator(subqueryRows);
      const inputOp = createMockOperator(mainRows);

      const inOp = new InSubqueryOperator(
        {
          type: 'inSubquery',
          id: 1,
          input: {} as QueryPlan,
          expr: col('dept_id'),
          subquery: { type: 'scan', id: 2, table: 'depts', source: 'btree', columns: ['id'] },
          not: false,
        },
        inputOp,
        ctx,
        createOperator
      );

      const results = await collectRows(inOp, ctx);

      // NULL IN (...) is false, so only id=2 should match
      expect(results).toHaveLength(1);
      expect(results[0].id).toBe(2);
    });
  });

  describe('EXISTS Subquery', () => {
    it('should filter rows where EXISTS is true', async () => {
      const ctx: SubqueryContext = {
        ...createMockExecutionContext(),
      };

      const mainRows: Row[] = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ];

      // Simulate correlated subquery: EXISTS only for id=1
      const createOperator = (plan: QueryPlan, context: ExecutionContext): Operator => {
        const subCtx = context as SubqueryContext;
        if (subCtx.outerRow?.id === 1) {
          return createMockOperator([{ exists: true }]);
        }
        return createMockOperator([]);
      };

      const inputOp = createMockOperator(mainRows);

      const existsOp = new ExistsSubqueryOperator(
        {
          type: 'existsSubquery',
          id: 1,
          input: {} as QueryPlan,
          subquery: { type: 'scan', id: 2, table: 'related', source: 'btree', columns: ['exists'] },
          not: false,
          correlatedColumns: ['id'],
        },
        inputOp,
        ctx,
        createOperator
      );

      const results = await collectRows(existsOp, ctx);

      expect(results).toHaveLength(1);
      expect(results[0].name).toBe('Alice');
    });

    it('should handle NOT EXISTS', async () => {
      const ctx: SubqueryContext = {
        ...createMockExecutionContext(),
      };

      const mainRows: Row[] = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ];

      // EXISTS for id=1, not for id=2
      const createOperator = (plan: QueryPlan, context: ExecutionContext): Operator => {
        const subCtx = context as SubqueryContext;
        if (subCtx.outerRow?.id === 1) {
          return createMockOperator([{ exists: true }]);
        }
        return createMockOperator([]);
      };

      const inputOp = createMockOperator(mainRows);

      const existsOp = new ExistsSubqueryOperator(
        {
          type: 'existsSubquery',
          id: 1,
          input: {} as QueryPlan,
          subquery: { type: 'scan', id: 2, table: 'related', source: 'btree', columns: ['exists'] },
          not: true,
          correlatedColumns: ['id'],
        },
        inputOp,
        ctx,
        createOperator
      );

      const results = await collectRows(existsOp, ctx);

      expect(results).toHaveLength(1);
      expect(results[0].name).toBe('Bob');
    });
  });

  describe('Quantified Subquery (ANY/ALL/SOME)', () => {
    it('should evaluate = ANY subquery', async () => {
      const ctx: SubqueryContext = {
        ...createMockExecutionContext(),
      };

      const mainRows: Row[] = [
        { id: 1, score: 85 },
        { id: 2, score: 90 },
        { id: 3, score: 75 },
      ];

      // Subquery returns passing scores
      const passingScores: Row[] = [{ score: 85 }, { score: 90 }];

      const createOperator = (): Operator => createMockOperator(passingScores);
      const inputOp = createMockOperator(mainRows);

      const anyOp = new QuantifiedSubqueryOperator(
        {
          type: 'quantifiedSubquery',
          id: 1,
          input: {} as QueryPlan,
          left: col('score'),
          op: 'eq',
          quantifier: 'any',
          subquery: { type: 'scan', id: 2, table: 'passing', source: 'btree', columns: ['score'] },
        },
        inputOp,
        ctx,
        createOperator
      );

      const results = await collectRows(anyOp, ctx);

      expect(results).toHaveLength(2);
      expect(results.map(r => r.id).sort()).toEqual([1, 2]);
    });

    it('should evaluate > ALL subquery', async () => {
      const ctx: SubqueryContext = {
        ...createMockExecutionContext(),
      };

      const mainRows: Row[] = [
        { id: 1, value: 100 },
        { id: 2, value: 50 },
        { id: 3, value: 75 },
      ];

      // Subquery returns thresholds
      const thresholds: Row[] = [{ threshold: 40 }, { threshold: 60 }];

      const createOperator = (): Operator => createMockOperator(thresholds);
      const inputOp = createMockOperator(mainRows);

      const allOp = new QuantifiedSubqueryOperator(
        {
          type: 'quantifiedSubquery',
          id: 1,
          input: {} as QueryPlan,
          left: col('value'),
          op: 'gt',
          quantifier: 'all',
          subquery: { type: 'scan', id: 2, table: 'thresholds', source: 'btree', columns: ['threshold'] },
        },
        inputOp,
        ctx,
        createOperator
      );

      const results = await collectRows(allOp, ctx);

      // Only 100 and 75 are > ALL of (40, 60)
      expect(results).toHaveLength(2);
      expect(results.map(r => r.id).sort()).toEqual([1, 3]);
    });

    it('should return true for empty subquery with ALL', async () => {
      const ctx: SubqueryContext = {
        ...createMockExecutionContext(),
      };

      const mainRows: Row[] = [{ id: 1, value: 50 }];

      const createOperator = (): Operator => createMockOperator([]);
      const inputOp = createMockOperator(mainRows);

      const allOp = new QuantifiedSubqueryOperator(
        {
          type: 'quantifiedSubquery',
          id: 1,
          input: {} as QueryPlan,
          left: col('value'),
          op: 'gt',
          quantifier: 'all',
          subquery: { type: 'scan', id: 2, table: 'empty', source: 'btree', columns: ['value'] },
        },
        inputOp,
        ctx,
        createOperator
      );

      const results = await collectRows(allOp, ctx);

      // ALL with empty set is vacuously true
      expect(results).toHaveLength(1);
    });

    it('should return false for empty subquery with ANY', async () => {
      const ctx: SubqueryContext = {
        ...createMockExecutionContext(),
      };

      const mainRows: Row[] = [{ id: 1, value: 50 }];

      const createOperator = (): Operator => createMockOperator([]);
      const inputOp = createMockOperator(mainRows);

      const anyOp = new QuantifiedSubqueryOperator(
        {
          type: 'quantifiedSubquery',
          id: 1,
          input: {} as QueryPlan,
          left: col('value'),
          op: 'eq',
          quantifier: 'any',
          subquery: { type: 'scan', id: 2, table: 'empty', source: 'btree', columns: ['value'] },
        },
        inputOp,
        ctx,
        createOperator
      );

      const results = await collectRows(anyOp, ctx);

      // ANY with empty set is false
      expect(results).toHaveLength(0);
    });
  });

  describe('Derived Table', () => {
    it('should materialize subquery as derived table', async () => {
      const ctx: SubqueryContext = {
        ...createMockExecutionContext(),
      };

      const subqueryRows: Row[] = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ];

      const createOperator = (): Operator => createMockOperator(subqueryRows);

      const derivedOp = new DerivedTableOperator(
        {
          type: 'derivedTable',
          id: 1,
          subquery: { type: 'scan', id: 2, table: 'users', source: 'btree', columns: ['id', 'name'] },
          alias: 'u',
        },
        ctx,
        createOperator
      );

      const results = await collectRows(derivedOp, ctx);

      expect(results).toHaveLength(2);
      // Columns should be aliased with prefix
      expect(results[0]['u.id']).toBe(1);
      expect(results[0]['u.name']).toBe('Alice');
    });
  });
});

// =============================================================================
// WINDOW OPERATOR TESTS
// =============================================================================

describe('Window Operator', () => {
  describe('ROW_NUMBER', () => {
    it('should assign sequential row numbers', async () => {
      const inputRows: Row[] = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'row_number',
            args: [],
            windowSpec: {},
            alias: 'rn',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      expect(results).toHaveLength(3);
      expect(results[0].rn).toBe(1);
      expect(results[1].rn).toBe(2);
      expect(results[2].rn).toBe(3);
    });

    it('should partition row numbers by PARTITION BY', async () => {
      const inputRows: Row[] = [
        { dept: 'A', name: 'Alice' },
        { dept: 'A', name: 'Bob' },
        { dept: 'B', name: 'Charlie' },
        { dept: 'B', name: 'Diana' },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'row_number',
            args: [],
            windowSpec: { partitionBy: ['dept'] },
            alias: 'rn',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      // Each partition should have row numbers 1, 2
      const deptA = results.filter(r => r.dept === 'A');
      const deptB = results.filter(r => r.dept === 'B');

      expect(deptA.map(r => r.rn).sort()).toEqual([1, 2]);
      expect(deptB.map(r => r.rn).sort()).toEqual([1, 2]);
    });

    it('should respect ORDER BY within partition', async () => {
      const inputRows: Row[] = [
        { dept: 'A', score: 80 },
        { dept: 'A', score: 90 },
        { dept: 'A', score: 70 },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'row_number',
            args: [],
            windowSpec: {
              partitionBy: ['dept'],
              orderBy: [{ column: 'score', direction: 'desc' }],
            },
            alias: 'rn',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      // Sorted by score DESC: 90, 80, 70
      expect(results[0].score).toBe(90);
      expect(results[0].rn).toBe(1);
      expect(results[1].score).toBe(80);
      expect(results[1].rn).toBe(2);
      expect(results[2].score).toBe(70);
      expect(results[2].rn).toBe(3);
    });
  });

  describe('RANK and DENSE_RANK', () => {
    it('should assign rank with gaps for ties', async () => {
      const inputRows: Row[] = [
        { name: 'Alice', score: 100 },
        { name: 'Bob', score: 90 },
        { name: 'Charlie', score: 90 },
        { name: 'Diana', score: 80 },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'rank',
            args: [],
            windowSpec: { orderBy: [{ column: 'score', direction: 'desc' }] },
            alias: 'rnk',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      // Score 100: rank 1, Score 90 (tie): rank 2, Score 80: rank 4 (skipped 3)
      const alice = results.find(r => r.name === 'Alice');
      const bob = results.find(r => r.name === 'Bob');
      const charlie = results.find(r => r.name === 'Charlie');
      const diana = results.find(r => r.name === 'Diana');

      expect(alice?.rnk).toBe(1);
      expect(bob?.rnk).toBe(2);
      expect(charlie?.rnk).toBe(2);
      expect(diana?.rnk).toBe(4);
    });

    it('should assign dense rank without gaps', async () => {
      const inputRows: Row[] = [
        { name: 'Alice', score: 100 },
        { name: 'Bob', score: 90 },
        { name: 'Charlie', score: 90 },
        { name: 'Diana', score: 80 },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'dense_rank',
            args: [],
            windowSpec: { orderBy: [{ column: 'score', direction: 'desc' }] },
            alias: 'drnk',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      // Score 100: rank 1, Score 90 (tie): rank 2, Score 80: rank 3 (no gap)
      const diana = results.find(r => r.name === 'Diana');
      expect(diana?.drnk).toBe(3);
    });
  });

  describe('NTILE', () => {
    it('should divide rows into buckets', async () => {
      const inputRows: Row[] = [
        { id: 1 },
        { id: 2 },
        { id: 3 },
        { id: 4 },
        { id: 5 },
        { id: 6 },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'ntile',
            args: [lit(3)],
            windowSpec: {},
            alias: 'bucket',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      // 6 rows into 3 buckets: [1,1,2,2,3,3]
      const buckets = results.map(r => r.bucket);
      expect(buckets.filter(b => b === 1)).toHaveLength(2);
      expect(buckets.filter(b => b === 2)).toHaveLength(2);
      expect(buckets.filter(b => b === 3)).toHaveLength(2);
    });

    it('should handle uneven distribution', async () => {
      const inputRows: Row[] = [
        { id: 1 },
        { id: 2 },
        { id: 3 },
        { id: 4 },
        { id: 5 },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'ntile',
            args: [lit(3)],
            windowSpec: {},
            alias: 'bucket',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      // 5 rows into 3 buckets: first two buckets get extra row
      const buckets = results.map(r => r.bucket);
      expect(buckets.filter(b => b === 1)).toHaveLength(2);
      expect(buckets.filter(b => b === 2)).toHaveLength(2);
      expect(buckets.filter(b => b === 3)).toHaveLength(1);
    });
  });

  describe('LAG and LEAD', () => {
    it('should access previous row value with LAG', async () => {
      const inputRows: Row[] = [
        { date: '2024-01-01', value: 100 },
        { date: '2024-01-02', value: 110 },
        { date: '2024-01-03', value: 105 },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'lag',
            args: [col('value')],
            windowSpec: { orderBy: [{ column: 'date', direction: 'asc' }] },
            alias: 'prev_value',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      expect(results[0].prev_value).toBeNull(); // First row has no previous
      expect(results[1].prev_value).toBe(100);
      expect(results[2].prev_value).toBe(110);
    });

    it('should access next row value with LEAD', async () => {
      const inputRows: Row[] = [
        { date: '2024-01-01', value: 100 },
        { date: '2024-01-02', value: 110 },
        { date: '2024-01-03', value: 105 },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'lead',
            args: [col('value')],
            windowSpec: { orderBy: [{ column: 'date', direction: 'asc' }] },
            alias: 'next_value',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      expect(results[0].next_value).toBe(110);
      expect(results[1].next_value).toBe(105);
      expect(results[2].next_value).toBeNull(); // Last row has no next
    });

    it('should support LAG with offset and default', async () => {
      const inputRows: Row[] = [
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'lag',
            args: [col('value'), lit(2), lit(-1)],
            windowSpec: {},
            alias: 'lag_2',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      expect(results[0].lag_2).toBe(-1); // Default for offset=2
      expect(results[1].lag_2).toBe(-1); // Default for offset=2
      expect(results[2].lag_2).toBe(10); // 2 rows back
    });
  });

  describe('FIRST_VALUE and LAST_VALUE', () => {
    it('should return first value in frame', async () => {
      const inputRows: Row[] = [
        { id: 1, value: 'A' },
        { id: 2, value: 'B' },
        { id: 3, value: 'C' },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'first_value',
            args: [col('value')],
            windowSpec: {
              orderBy: [{ column: 'id', direction: 'asc' }],
            },
            alias: 'first_val',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      // With default frame (RANGE UNBOUNDED PRECEDING to CURRENT ROW), first is always first
      expect(results[0].first_val).toBe('A');
      expect(results[1].first_val).toBe('A');
      expect(results[2].first_val).toBe('A');
    });

    it('should return last value in frame', async () => {
      const inputRows: Row[] = [
        { id: 1, value: 'A' },
        { id: 2, value: 'B' },
        { id: 3, value: 'C' },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'last_value',
            args: [col('value')],
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

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      // With UNBOUNDED frame, last is always 'C'
      expect(results[0].last_val).toBe('C');
      expect(results[1].last_val).toBe('C');
      expect(results[2].last_val).toBe('C');
    });
  });

  describe('Aggregate Window Functions', () => {
    it('should calculate running SUM', async () => {
      const inputRows: Row[] = [
        { id: 1, amount: 100 },
        { id: 2, amount: 200 },
        { id: 3, amount: 300 },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'sum',
            args: [col('amount')],
            windowSpec: {
              orderBy: [{ column: 'id', direction: 'asc' }],
            },
            alias: 'running_sum',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      expect(results[0].running_sum).toBe(100);
      expect(results[1].running_sum).toBe(300);
      expect(results[2].running_sum).toBe(600);
    });

    it('should calculate running AVG', async () => {
      const inputRows: Row[] = [
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'avg',
            args: [col('value')],
            windowSpec: {
              orderBy: [{ column: 'id', direction: 'asc' }],
            },
            alias: 'running_avg',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      expect(results[0].running_avg).toBe(10);
      expect(results[1].running_avg).toBe(15);
      expect(results[2].running_avg).toBe(20);
    });

    it('should calculate COUNT within frame', async () => {
      const inputRows: Row[] = [
        { dept: 'A', name: 'Alice' },
        { dept: 'A', name: 'Bob' },
        { dept: 'B', name: 'Charlie' },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'count',
            args: [],
            windowSpec: {
              partitionBy: ['dept'],
              frame: {
                mode: 'rows',
                start: { type: 'unboundedPreceding' },
                end: { type: 'unboundedFollowing' },
              },
            },
            alias: 'dept_count',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      const deptA = results.filter(r => r.dept === 'A');
      const deptB = results.filter(r => r.dept === 'B');

      expect(deptA[0].dept_count).toBe(2);
      expect(deptB[0].dept_count).toBe(1);
    });

    it('should calculate MIN and MAX', async () => {
      const inputRows: Row[] = [
        { id: 1, value: 50 },
        { id: 2, value: 30 },
        { id: 3, value: 70 },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'min',
            args: [col('value')],
            windowSpec: {
              frame: {
                mode: 'rows',
                start: { type: 'unboundedPreceding' },
                end: { type: 'unboundedFollowing' },
              },
            },
            alias: 'min_val',
          },
          {
            name: 'max',
            args: [col('value')],
            windowSpec: {
              frame: {
                mode: 'rows',
                start: { type: 'unboundedPreceding' },
                end: { type: 'unboundedFollowing' },
              },
            },
            alias: 'max_val',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      expect(results[0].min_val).toBe(30);
      expect(results[0].max_val).toBe(70);
    });
  });

  describe('Window Frame Specifications', () => {
    it('should handle ROWS BETWEEN frame', async () => {
      const inputRows: Row[] = [
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
        { id: 4, value: 40 },
        { id: 5, value: 50 },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'sum',
            args: [col('value')],
            windowSpec: {
              orderBy: [{ column: 'id', direction: 'asc' }],
              frame: {
                mode: 'rows',
                start: { type: 'preceding', offset: 1 },
                end: { type: 'following', offset: 1 },
              },
            },
            alias: 'window_sum',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      // Row 1: sum(10, 20) = 30 (no preceding)
      // Row 2: sum(10, 20, 30) = 60
      // Row 3: sum(20, 30, 40) = 90
      // Row 4: sum(30, 40, 50) = 120
      // Row 5: sum(40, 50) = 90 (no following)
      expect(results[0].window_sum).toBe(30);
      expect(results[1].window_sum).toBe(60);
      expect(results[2].window_sum).toBe(90);
      expect(results[3].window_sum).toBe(120);
      expect(results[4].window_sum).toBe(90);
    });
  });

  describe('Multiple Window Functions', () => {
    it('should evaluate multiple window functions with same spec', async () => {
      const inputRows: Row[] = [
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
      ];

      const windowSpec: WindowSpec = {
        orderBy: [{ column: 'id', direction: 'asc' }],
      };

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          { name: 'row_number', args: [], windowSpec, alias: 'rn' },
          { name: 'sum', args: [col('value')], windowSpec, alias: 'total' },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      expect(results[0].rn).toBe(1);
      expect(results[0].total).toBe(10);
      expect(results[2].rn).toBe(3);
      expect(results[2].total).toBe(60);
    });
  });

  describe('Utility Functions', () => {
    it('should detect window functions in expressions', () => {
      const windowExpr: Expression & { over?: any } = {
        type: 'function',
        name: 'row_number',
        args: [],
        over: {},
      };

      const regularExpr: Expression = {
        type: 'function',
        name: 'upper',
        args: [col('name')],
      };

      expect(containsWindowFunction(windowExpr)).toBe(true);
      expect(containsWindowFunction(regularExpr)).toBe(false);
    });

    it('should extract window function definitions', () => {
      const expressions = [
        {
          expr: {
            type: 'function' as const,
            name: 'row_number',
            args: [],
            over: { orderBy: [{ column: 'id', direction: 'asc' }] },
          },
          alias: 'rn',
        },
      ];

      const windowFuncs = extractWindowFunctions(expressions as any);

      expect(windowFuncs).toHaveLength(1);
      expect(windowFuncs[0].name).toBe('row_number');
      expect(windowFuncs[0].alias).toBe('rn');
    });
  });
});

// =============================================================================
// OPERATOR COMPOSITION TESTS
// =============================================================================

describe('Operator Composition', () => {
  it('should chain filter and window operators', async () => {
    const inputRows: Row[] = [
      { id: 1, status: 'active', amount: 100 },
      { id: 2, status: 'inactive', amount: 200 },
      { id: 3, status: 'active', amount: 300 },
      { id: 4, status: 'active', amount: 400 },
    ];

    // First filter, then apply window function
    const ctx = createMockExecutionContext();
    const inputOp = createMockOperator(inputRows);

    const filterPlan: FilterPlan = {
      type: 'filter',
      id: 1,
      input: {} as QueryPlan,
      predicate: {
        type: 'comparison',
        op: 'eq',
        left: col('status'),
        right: lit('active'),
      },
    };

    const filterOp = new FilterOperator(filterPlan, inputOp, ctx);

    const windowPlan: WindowPlan = {
      type: 'window',
      id: 2,
      input: null,
      windowFunctions: [
        {
          name: 'row_number',
          args: [],
          windowSpec: { orderBy: [{ column: 'amount', direction: 'asc' }] },
          alias: 'rn',
        },
      ],
    };

    const windowOp = createWindowOperator(windowPlan, filterOp);
    const results = await collectRows(windowOp, ctx);

    // Only 3 active rows
    expect(results).toHaveLength(3);
    // Sorted by amount: 100, 300, 400
    expect(results[0].amount).toBe(100);
    expect(results[0].rn).toBe(1);
    expect(results[1].amount).toBe(300);
    expect(results[1].rn).toBe(2);
    expect(results[2].amount).toBe(400);
    expect(results[2].rn).toBe(3);
  });

  it('should chain set operation with filter', async () => {
    const left: Row[] = [
      { id: 1, type: 'A' },
      { id: 2, type: 'B' },
    ];
    const right: Row[] = [
      { id: 3, type: 'A' },
      { id: 4, type: 'B' },
    ];

    const ctx = createMockExecutionContext();
    const leftOp = createMockOperator(left);
    const rightOp = createMockOperator(right);

    const unionOp = new UnionOperator(leftOp, rightOp, true);

    const filterPlan: FilterPlan = {
      type: 'filter',
      id: 1,
      input: {} as QueryPlan,
      predicate: {
        type: 'comparison',
        op: 'eq',
        left: col('type'),
        right: lit('A'),
      },
    };

    const filterOp = new FilterOperator(filterPlan, unionOp, ctx);
    const results = await collectRows(filterOp, ctx);

    // Only type='A' rows from union
    expect(results).toHaveLength(2);
    expect(results.every(r => r.type === 'A')).toBe(true);
  });
});

// =============================================================================
// EDGE CASES AND ERROR HANDLING
// =============================================================================

describe('Edge Cases and Error Handling', () => {
  describe('Empty Input', () => {
    it('should handle empty input in filter', async () => {
      const ctx = createMockExecutionContext();
      const inputOp = createMockOperator([]);

      const filterPlan: FilterPlan = {
        type: 'filter',
        id: 1,
        input: {} as QueryPlan,
        predicate: { type: 'comparison', op: 'eq', left: col('x'), right: lit(1) },
      };

      const filterOp = new FilterOperator(filterPlan, inputOp, ctx);
      const results = await collectRows(filterOp, ctx);

      expect(results).toHaveLength(0);
    });

    it('should handle empty input in window operator', async () => {
      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          { name: 'row_number', args: [], windowSpec: {}, alias: 'rn' },
        ],
      };

      const inputOp = createMockOperator([]);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      expect(results).toHaveLength(0);
    });

    it('should handle empty sides in set operations', async () => {
      expect(await executeSetOperation('UNION', [], [], false)).toHaveLength(0);
      expect(await executeSetOperation('INTERSECT', [], [], false)).toHaveLength(0);
      expect(await executeSetOperation('EXCEPT', [], [], false)).toHaveLength(0);
    });
  });

  describe('NULL Values', () => {
    it('should handle rows with all NULL values', async () => {
      const inputRows: Row[] = [
        { a: null, b: null },
        { a: 1, b: null },
        { a: null, b: 2 },
      ];

      const windowPlan: WindowPlan = {
        type: 'window',
        id: 1,
        input: null,
        windowFunctions: [
          {
            name: 'count',
            args: [col('a')],
            windowSpec: {
              frame: { mode: 'rows', start: { type: 'unboundedPreceding' }, end: { type: 'unboundedFollowing' } },
            },
            alias: 'count_a',
          },
        ],
      };

      const inputOp = createMockOperator(inputRows);
      const windowOp = createWindowOperator(windowPlan, inputOp);
      const results = await collectRows(windowOp);

      // COUNT(a) excludes NULLs
      expect(results[0].count_a).toBe(1);
    });

    it('should handle NULL in UNION deduplication', async () => {
      const left: Row[] = [{ id: null }];
      const right: Row[] = [{ id: null }];

      const results = await executeSetOperation('UNION', left, right, false);

      // NULL rows should be deduplicated (they serialize the same)
      expect(results).toHaveLength(1);
    });
  });

  describe('Large Values', () => {
    it('should handle large bigint values', () => {
      const large = BigInt('9223372036854775807');
      expect(valuesEqual(large, large)).toBe(true);
      expect(valuesEqual(large, large - 1n)).toBe(false);
    });

    it('should handle Date edge cases', () => {
      const d1 = new Date(0);
      const d2 = new Date(0);
      expect(valuesEqual(d1, d2)).toBe(true);

      const d3 = new Date('invalid');
      const d4 = new Date('invalid');
      // Both are NaN time, so getTime() returns NaN
      expect(valuesEqual(d3, d4)).toBe(false); // NaN !== NaN
    });
  });
});
