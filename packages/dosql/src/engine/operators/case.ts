/**
 * CASE Expression Evaluator
 *
 * Runtime evaluation of CASE expressions in query execution.
 * Supports:
 * - Simple CASE: CASE x WHEN y THEN z END
 * - Searched CASE: CASE WHEN condition THEN value END
 * - COALESCE, NULLIF, IIF function shorthands
 *
 * Integrates with the DoSQL execution engine for efficient evaluation.
 */

import {
  type CaseExpr,
  type Expression,
  type Row,
  type SqlValue,
} from '../types.js';
import { evaluateExpression } from './filter.js';

// =============================================================================
// CASE EXPRESSION TYPES (extended from engine/types.ts)
// =============================================================================

/**
 * Extended CASE expression with simple/searched distinction
 */
export interface ExtendedCaseExpr extends CaseExpr {
  /** Operand for simple CASE (undefined for searched CASE) */
  operand?: Expression;
  /** Whether this is a simple or searched CASE */
  caseStyle: 'simple' | 'searched';
}

/**
 * Simple CASE: CASE operand WHEN value THEN result END
 */
export interface SimpleCaseExpr {
  type: 'case';
  caseStyle: 'simple';
  operand: Expression;
  when: SimpleCaseWhen[];
  else?: Expression;
}

export interface SimpleCaseWhen {
  value: Expression;
  result: Expression;
}

/**
 * Searched CASE: CASE WHEN condition THEN result END
 */
export interface SearchedCaseExpr {
  type: 'case';
  caseStyle: 'searched';
  when: SearchedCaseWhen[];
  else?: Expression;
}

export interface SearchedCaseWhen {
  condition: Expression;
  result: Expression;
}

export type CaseExprUnion = SimpleCaseExpr | SearchedCaseExpr;

// =============================================================================
// CASE EVALUATION
// =============================================================================

/**
 * Evaluate a CASE expression against a row
 *
 * Handles both simple and searched CASE expressions.
 * Returns NULL if no condition matches and no ELSE is provided.
 */
export function evaluateCaseExpression(caseExpr: CaseExpr | CaseExprUnion, row: Row): SqlValue {
  // Detect case style
  if ('caseStyle' in caseExpr) {
    if (caseExpr.caseStyle === 'simple') {
      return evaluateSimpleCase(caseExpr as SimpleCaseExpr, row);
    } else {
      return evaluateSearchedCase(caseExpr as SearchedCaseExpr, row);
    }
  }

  // Standard CaseExpr from engine/types.ts (always searched style)
  return evaluateStandardCase(caseExpr, row);
}

/**
 * Evaluate standard CASE expression (from engine/types.ts)
 */
function evaluateStandardCase(caseExpr: CaseExpr, row: Row): SqlValue {
  for (const { condition, result } of caseExpr.when) {
    const conditionValue = evaluateExpression(condition, row);
    if (isTruthy(conditionValue)) {
      return evaluateExpression(result, row);
    }
  }

  if (caseExpr.else) {
    return evaluateExpression(caseExpr.else, row);
  }

  return null;
}

/**
 * Evaluate simple CASE: CASE operand WHEN value THEN result END
 */
function evaluateSimpleCase(caseExpr: SimpleCaseExpr, row: Row): SqlValue {
  const operandValue = evaluateExpression(caseExpr.operand, row);

  for (const { value, result } of caseExpr.when) {
    const whenValue = evaluateExpression(value, row);

    // Simple CASE uses equality comparison
    if (valuesEqual(operandValue, whenValue)) {
      return evaluateExpression(result, row);
    }
  }

  if (caseExpr.else) {
    return evaluateExpression(caseExpr.else, row);
  }

  return null;
}

/**
 * Evaluate searched CASE: CASE WHEN condition THEN result END
 */
function evaluateSearchedCase(caseExpr: SearchedCaseExpr, row: Row): SqlValue {
  for (const { condition, result } of caseExpr.when) {
    const conditionValue = evaluateExpression(condition, row);
    if (isTruthy(conditionValue)) {
      return evaluateExpression(result, row);
    }
  }

  if (caseExpr.else) {
    return evaluateExpression(caseExpr.else, row);
  }

  return null;
}

// =============================================================================
// CASE SHORTHAND FUNCTIONS
// =============================================================================

/**
 * Evaluate COALESCE(arg1, arg2, ..., argN)
 * Returns first non-NULL argument
 *
 * Equivalent to:
 * CASE WHEN arg1 IS NOT NULL THEN arg1
 *      WHEN arg2 IS NOT NULL THEN arg2
 *      ...
 *      ELSE argN END
 */
export function evaluateCoalesce(args: Expression[], row: Row): SqlValue {
  for (const arg of args) {
    const value = evaluateExpression(arg, row);
    if (value !== null) {
      return value;
    }
  }
  return null;
}

/**
 * Evaluate NULLIF(expr1, expr2)
 * Returns NULL if expr1 = expr2, otherwise returns expr1
 *
 * Equivalent to:
 * CASE WHEN expr1 = expr2 THEN NULL ELSE expr1 END
 */
export function evaluateNullif(expr1: Expression, expr2: Expression, row: Row): SqlValue {
  const value1 = evaluateExpression(expr1, row);
  const value2 = evaluateExpression(expr2, row);

  if (valuesEqual(value1, value2)) {
    return null;
  }

  return value1;
}

/**
 * Evaluate IIF(condition, true_value, false_value)
 * Ternary conditional expression
 *
 * Equivalent to:
 * CASE WHEN condition THEN true_value ELSE false_value END
 */
export function evaluateIif(
  condition: Expression,
  trueValue: Expression,
  falseValue: Expression,
  row: Row
): SqlValue {
  const conditionResult = evaluateExpression(condition, row);

  if (isTruthy(conditionResult)) {
    return evaluateExpression(trueValue, row);
  } else {
    return evaluateExpression(falseValue, row);
  }
}

/**
 * Evaluate IFNULL(expr, default)
 * Returns expr if not NULL, otherwise returns default
 * (Same as COALESCE with 2 arguments)
 *
 * Equivalent to:
 * CASE WHEN expr IS NOT NULL THEN expr ELSE default END
 */
export function evaluateIfnull(expr: Expression, defaultExpr: Expression, row: Row): SqlValue {
  const value = evaluateExpression(expr, row);
  if (value !== null) {
    return value;
  }
  return evaluateExpression(defaultExpr, row);
}

/**
 * Evaluate NVL(expr, default) - Oracle style IFNULL
 * Same as IFNULL
 */
export const evaluateNvl = evaluateIfnull;

/**
 * Evaluate NVL2(expr, not_null_value, null_value)
 * Returns not_null_value if expr is not NULL, otherwise returns null_value
 *
 * Equivalent to:
 * CASE WHEN expr IS NOT NULL THEN not_null_value ELSE null_value END
 */
export function evaluateNvl2(
  expr: Expression,
  notNullValue: Expression,
  nullValue: Expression,
  row: Row
): SqlValue {
  const value = evaluateExpression(expr, row);

  if (value !== null) {
    return evaluateExpression(notNullValue, row);
  } else {
    return evaluateExpression(nullValue, row);
  }
}

/**
 * Evaluate DECODE(expr, search1, result1, [search2, result2, ...], [default])
 * Oracle-style switch expression
 *
 * Equivalent to:
 * CASE expr
 *   WHEN search1 THEN result1
 *   WHEN search2 THEN result2
 *   ...
 *   ELSE default
 * END
 */
export function evaluateDecode(args: Expression[], row: Row): SqlValue {
  if (args.length < 3) {
    throw new Error('DECODE requires at least 3 arguments');
  }

  const exprValue = evaluateExpression(args[0], row);

  // Process search/result pairs
  for (let i = 1; i < args.length - 1; i += 2) {
    const searchValue = evaluateExpression(args[i], row);
    if (valuesEqual(exprValue, searchValue)) {
      return evaluateExpression(args[i + 1], row);
    }
  }

  // If odd number of remaining args after expr, last is default
  if ((args.length - 1) % 2 === 1) {
    return evaluateExpression(args[args.length - 1], row);
  }

  return null;
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Check if a value is truthy in SQL context
 * NULL, FALSE, 0, and empty string are falsy
 */
function isTruthy(value: SqlValue): boolean {
  if (value === null) return false;
  if (value === false) return false;
  if (value === 0) return false;
  if (value === 0n) return false;
  if (value === '') return false;
  return true;
}

/**
 * Check if two SQL values are equal
 * Handles NULL comparison (NULL = NULL is false in SQL)
 */
function valuesEqual(a: SqlValue, b: SqlValue): boolean {
  // In SQL, NULL is not equal to anything, including NULL
  if (a === null || b === null) {
    return false;
  }

  // Handle Date comparison
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime();
  }

  // Handle Uint8Array comparison
  if (a instanceof Uint8Array && b instanceof Uint8Array) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }

  // Handle bigint comparison
  if (typeof a === 'bigint' && typeof b === 'bigint') {
    return a === b;
  }

  // Handle bigint/number cross comparison
  if (typeof a === 'bigint' && typeof b === 'number') {
    return a === BigInt(b);
  }
  if (typeof a === 'number' && typeof b === 'bigint') {
    return BigInt(a) === b;
  }

  return a === b;
}

// =============================================================================
// CASE EXPRESSION BUILDER
// =============================================================================

/**
 * Build a searched CASE expression programmatically
 */
export function buildSearchedCase(
  whens: Array<{ condition: Expression; result: Expression }>,
  elseExpr?: Expression
): SearchedCaseExpr {
  return {
    type: 'case',
    caseStyle: 'searched',
    when: whens.map(w => ({ condition: w.condition, result: w.result })),
    else: elseExpr,
  };
}

/**
 * Build a simple CASE expression programmatically
 */
export function buildSimpleCase(
  operand: Expression,
  whens: Array<{ value: Expression; result: Expression }>,
  elseExpr?: Expression
): SimpleCaseExpr {
  return {
    type: 'case',
    caseStyle: 'simple',
    operand,
    when: whens,
    else: elseExpr,
  };
}

/**
 * Convert simple CASE to searched CASE
 * Useful for optimization and normalization
 */
export function simpleCaseToSearched(simpleCase: SimpleCaseExpr): SearchedCaseExpr {
  const searchedWhens: SearchedCaseWhen[] = simpleCase.when.map(w => ({
    condition: {
      type: 'binary' as const,
      op: 'eq' as const,
      left: simpleCase.operand,
      right: w.value,
    },
    result: w.result,
  }));

  return {
    type: 'case',
    caseStyle: 'searched',
    when: searchedWhens,
    else: simpleCase.else,
  };
}

// =============================================================================
// CASE OPTIMIZATION
// =============================================================================

/**
 * Check if a CASE can be constant-folded
 * (all conditions and results are literals)
 */
export function isCaseConstant(caseExpr: CaseExprUnion): boolean {
  if (caseExpr.caseStyle === 'simple') {
    const simple = caseExpr as SimpleCaseExpr;
    if (simple.operand.type !== 'literal') return false;

    for (const w of simple.when) {
      if (w.value.type !== 'literal') return false;
      if (w.result.type !== 'literal') return false;
    }
  } else {
    const searched = caseExpr as SearchedCaseExpr;
    for (const w of searched.when) {
      if (!isExpressionConstant(w.condition)) return false;
      if (w.result.type !== 'literal') return false;
    }
  }

  if (caseExpr.else && caseExpr.else.type !== 'literal') {
    return false;
  }

  return true;
}

/**
 * Check if an expression is constant (can be evaluated without row)
 */
function isExpressionConstant(expr: Expression): boolean {
  switch (expr.type) {
    case 'literal':
      return true;
    case 'binary':
      return isExpressionConstant(expr.left) && isExpressionConstant(expr.right);
    case 'unary':
      return isExpressionConstant(expr.operand);
    default:
      return false;
  }
}

/**
 * Fold constant CASE to its result
 */
export function foldConstantCase(caseExpr: CaseExprUnion): Expression | null {
  if (!isCaseConstant(caseExpr)) {
    return null;
  }

  // Evaluate with empty row since it's constant
  const result = evaluateCaseExpression(caseExpr, {});

  return {
    type: 'literal',
    value: result,
    dataType: inferDataType(result),
  };
}

function inferDataType(value: SqlValue): 'string' | 'number' | 'bigint' | 'boolean' | 'date' | 'bytes' | 'null' {
  if (value === null) return 'null';
  if (typeof value === 'string') return 'string';
  if (typeof value === 'number') return 'number';
  if (typeof value === 'bigint') return 'bigint';
  if (typeof value === 'boolean') return 'boolean';
  if (value instanceof Date) return 'date';
  if (value instanceof Uint8Array) return 'bytes';
  return 'null';
}

// =============================================================================
// EXPORT ALL
// =============================================================================

export {
  isTruthy,
  valuesEqual,
};
