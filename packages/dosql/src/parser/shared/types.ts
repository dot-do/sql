/**
 * Shared Parser Types
 *
 * Common type definitions used across DoSQL parsers.
 * Consolidates duplicated types from ddl.ts, dml.ts, case.ts, window.ts, subquery.ts.
 *
 * @packageDocumentation
 */

// =============================================================================
// SOURCE LOCATION
// =============================================================================

/**
 * Location information for error reporting and AST node tracking
 */
export interface SourceLocation {
  /** 1-based line number */
  line: number;
  /** 1-based column number */
  column: number;
  /** 0-based character offset */
  offset: number;
}

// =============================================================================
// TOKEN TYPES
// =============================================================================

/**
 * Token types recognized by the SQL lexer
 */
export type TokenType =
  | 'keyword'
  | 'identifier'
  | 'number'
  | 'string'
  | 'operator'
  | 'punctuation'
  | 'parameter'
  | 'whitespace'
  | 'comment'
  | 'eof';

/**
 * A single token from the lexer
 */
export interface Token {
  /** Token type classification */
  type: TokenType;
  /** Raw token value */
  value: string;
  /** Source location of the token */
  location: SourceLocation;
}

// =============================================================================
// EXPRESSION TYPES (Shared across parsers)
// =============================================================================

/**
 * Base expression node with optional location
 */
export interface ExpressionBase {
  location?: SourceLocation;
}

/**
 * Column reference expression
 */
export interface ColumnExpr extends ExpressionBase {
  type: 'column';
  name: string;
  table?: string;
}

/**
 * Literal value expression
 */
export interface LiteralExpr extends ExpressionBase {
  type: 'literal';
  value: string | number | boolean | null;
  /** Raw string representation */
  raw?: string;
}

/**
 * Binary operation expression
 */
export interface BinaryExpr extends ExpressionBase {
  type: 'binary';
  op: string;
  left: BaseExpr;
  right: BaseExpr;
}

/**
 * Unary operation expression
 */
export interface UnaryExpr extends ExpressionBase {
  type: 'unary';
  op: string;
  operand: BaseExpr;
}

/**
 * Function call expression
 */
export interface FunctionExpr extends ExpressionBase {
  type: 'function';
  name: string;
  args: BaseExpr[];
  distinct?: boolean;
}

/**
 * Aggregate function expression
 */
export interface AggregateExpr extends ExpressionBase {
  type: 'aggregate';
  name: string;
  arg: BaseExpr | '*';
  distinct?: boolean;
}

/**
 * BETWEEN expression
 */
export interface BetweenExpr extends ExpressionBase {
  type: 'between';
  expr: BaseExpr;
  low: BaseExpr;
  high: BaseExpr;
  not?: boolean;
}

/**
 * IN expression
 */
export interface InExpr extends ExpressionBase {
  type: 'in';
  expr: BaseExpr;
  values: BaseExpr[] | SubqueryExpr;
  not?: boolean;
}

/**
 * IS NULL expression
 */
export interface IsNullExpr extends ExpressionBase {
  type: 'isNull';
  expr: BaseExpr;
  isNot: boolean;
}

/**
 * Subquery expression
 */
export interface SubqueryExpr extends ExpressionBase {
  type: 'subquery';
  subqueryType: 'scalar' | 'in' | 'exists' | 'any' | 'all';
  query: unknown; // ParsedSelect - avoiding circular import
}

/**
 * EXISTS expression
 */
export interface ExistsExpr extends ExpressionBase {
  type: 'exists';
  query: unknown; // ParsedSelect
}

/**
 * Star (wildcard) expression
 */
export interface StarExpr extends ExpressionBase {
  type: 'star';
  table?: string;
}

/**
 * NULL expression (standalone)
 */
export interface NullExpr extends ExpressionBase {
  type: 'null';
}

/**
 * DEFAULT expression (used in INSERT)
 */
export interface DefaultExpr extends ExpressionBase {
  type: 'default';
}

/**
 * Parameter placeholder expression
 */
export interface ParameterExpr extends ExpressionBase {
  type: 'parameter';
  name: string | number;
  raw: string;
}

/**
 * Union of all base expression types
 */
export type BaseExpr =
  | ColumnExpr
  | LiteralExpr
  | BinaryExpr
  | UnaryExpr
  | FunctionExpr
  | AggregateExpr
  | BetweenExpr
  | InExpr
  | IsNullExpr
  | SubqueryExpr
  | ExistsExpr
  | StarExpr
  | NullExpr
  | DefaultExpr
  | ParameterExpr;

// =============================================================================
// OPERATOR PRECEDENCE
// =============================================================================

/**
 * SQL operator precedence levels (higher = binds tighter)
 */
export const OPERATOR_PRECEDENCE: Record<string, number> = {
  'OR': 1,
  'AND': 2,
  'NOT': 3,
  '=': 4, '!=': 4, '<>': 4, '<': 4, '<=': 4, '>': 4, '>=': 4,
  'IS': 4, 'IS NOT': 4, 'LIKE': 4, 'ILIKE': 4, 'NOT LIKE': 4,
  'GLOB': 4, 'NOT GLOB': 4, 'REGEXP': 4, 'NOT REGEXP': 4,
  'IN': 4, 'NOT IN': 4, 'BETWEEN': 4, 'NOT BETWEEN': 4,
  '|': 5,
  '&': 6,
  '<<': 7, '>>': 7,
  '+': 8, '-': 8,
  '*': 9, '/': 9, '%': 9,
  '||': 10,
};

/**
 * Binary operators
 */
export type BinaryOperator =
  // Arithmetic
  | '+' | '-' | '*' | '/' | '%'
  // Comparison
  | '=' | '!=' | '<>' | '<' | '<=' | '>' | '>='
  | 'IS' | 'IS NOT' | 'LIKE' | 'ILIKE' | 'NOT LIKE'
  | 'GLOB' | 'NOT GLOB' | 'REGEXP' | 'NOT REGEXP'
  | 'IN' | 'NOT IN' | 'BETWEEN' | 'NOT BETWEEN'
  // Logical
  | 'AND' | 'OR'
  // Bitwise
  | '&' | '|' | '<<' | '>>'
  // String concatenation
  | '||';

/**
 * Unary operators
 */
export type UnaryOperator = 'NOT' | '-' | '+' | '~';

// =============================================================================
// PARSE RESULTS
// =============================================================================

/**
 * Successful parse result (generic)
 */
export interface ParseSuccessBase<T> {
  success: true;
  statement?: T;
  ast?: T;
  remaining?: string;
}

/**
 * Failed parse result
 */
export interface ParseErrorBase {
  success: false;
  error: string;
  position?: number;
  line?: number;
  column?: number;
  input?: string;
}

/**
 * Generic parse result
 */
export type ParseResultBase<T> = ParseSuccessBase<T> | ParseErrorBase;

// =============================================================================
// TYPE GUARDS
// =============================================================================

/**
 * Check if expression is a column reference
 */
export function isColumnExpr(expr: BaseExpr): expr is ColumnExpr {
  return expr.type === 'column';
}

/**
 * Check if expression is a literal
 */
export function isLiteralExpr(expr: BaseExpr): expr is LiteralExpr {
  return expr.type === 'literal';
}

/**
 * Check if expression is a binary operation
 */
export function isBinaryExpr(expr: BaseExpr): expr is BinaryExpr {
  return expr.type === 'binary';
}

/**
 * Check if expression is a function call
 */
export function isFunctionExpr(expr: BaseExpr): expr is FunctionExpr {
  return expr.type === 'function';
}

/**
 * Check if expression is an aggregate
 */
export function isAggregateExpr(expr: BaseExpr): expr is AggregateExpr {
  return expr.type === 'aggregate';
}

/**
 * Check if parse result is successful
 */
export function isParseSuccessBase<T>(result: ParseResultBase<T>): result is ParseSuccessBase<T> {
  return result.success === true;
}

/**
 * Check if parse result is an error
 */
export function isParseErrorBase(result: ParseResultBase<unknown>): result is ParseErrorBase {
  return result.success === false;
}
