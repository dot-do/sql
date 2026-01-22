/**
 * Type-Level SQL Aggregate Functions and Expressions
 *
 * Handles parsing and type inference for:
 * - Aggregate functions: COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col)
 * - Arithmetic expressions: col * 1.1, price + tax
 * - AS aliases for computed columns
 * - GROUP BY awareness
 */

import type { ColumnType, DatabaseSchema, TableSchema } from './parser.js';

// =============================================================================
// UTILITY TYPES
// =============================================================================

type Whitespace = ' ' | '\n' | '\t' | '\r';

type Trim<S extends string> =
  S extends `${Whitespace}${infer Rest}` ? Trim<Rest> :
  S extends `${infer Rest}${Whitespace}` ? Trim<Rest> :
  S;

type Upper<S extends string> = Uppercase<S>;

// =============================================================================
// AGGREGATE FUNCTION TYPES
// =============================================================================

/**
 * Supported SQL aggregate functions
 */
export type AggregateFunctionName = 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX';

/**
 * Check if expression is an aggregate function call
 * Handles: COUNT(*), COUNT(col), SUM(col), AVG(col), MIN(col), MAX(col)
 */
export type IsAggregateFunction<S extends string> =
  Upper<Trim<S>> extends `COUNT(${string})` ? true :
  Upper<Trim<S>> extends `SUM(${string})` ? true :
  Upper<Trim<S>> extends `AVG(${string})` ? true :
  Upper<Trim<S>> extends `MIN(${string})` ? true :
  Upper<Trim<S>> extends `MAX(${string})` ? true :
  false;

/**
 * Extract the function name from an aggregate expression
 */
export type ExtractAggregateFn<S extends string> =
  Upper<Trim<S>> extends `COUNT(${string})` ? 'COUNT' :
  Upper<Trim<S>> extends `SUM(${string})` ? 'SUM' :
  Upper<Trim<S>> extends `AVG(${string})` ? 'AVG' :
  Upper<Trim<S>> extends `MIN(${string})` ? 'MIN' :
  Upper<Trim<S>> extends `MAX(${string})` ? 'MAX' :
  never;

/**
 * Extract the argument from an aggregate function call
 * Preserves original case of the argument for column lookups
 */
export type ExtractAggregateArg<S extends string> =
  ExtractArgFromOriginal<Trim<S>>;

// Extract argument from original string (preserving case)
type ExtractArgFromOriginal<S extends string> =
  // Match each case pattern and extract from original
  S extends `${infer _Fn}(${infer Arg})` ? Trim<Arg> :
  never;

/**
 * Determine the result type of an aggregate function
 *
 * - COUNT always returns number (integer)
 * - SUM on number returns number
 * - AVG on number returns number (float)
 * - MIN/MAX preserve the input type
 */
export type AggregateResultType<
  Fn extends AggregateFunctionName,
  InputType extends ColumnType
> =
  Fn extends 'COUNT' ? 'number' :
  Fn extends 'SUM' ? 'number' :
  Fn extends 'AVG' ? 'number' :
  Fn extends 'MIN' ? InputType :
  Fn extends 'MAX' ? InputType :
  'unknown';

// =============================================================================
// ARITHMETIC EXPRESSION TYPES
// =============================================================================

/**
 * Arithmetic operators
 */
export type ArithmeticOperator = '+' | '-' | '*' | '/';

/**
 * Check if string contains arithmetic operators
 * Must be careful to not match inside function calls
 */
export type ContainsArithmeticOp<S extends string> =
  S extends `${string}+${string}` ? true :
  S extends `${string}-${string}` ? true :
  S extends `${string}*${string}` ? true :
  S extends `${string}/${string}` ? true :
  false;

/**
 * Check if expression is a pure arithmetic expression (not inside aggregate)
 */
export type IsArithmeticExpression<S extends string> =
  IsAggregateFunction<S> extends true
    ? false
    : ContainsArithmeticOp<Trim<S>>;

/**
 * Arithmetic expressions on numeric types yield number
 */
export type ArithmeticExpressionType = 'number';

// =============================================================================
// NUMERIC LITERAL DETECTION
// =============================================================================

/**
 * Check if string is a numeric literal
 */
type Digit = '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9';

export type IsNumericLiteral<S extends string> =
  S extends `${Digit}${infer Rest}`
    ? Rest extends '' ? true : IsNumericLiteral<Rest>
    : S extends `${Digit}${infer Rest}.${infer Decimal}`
      ? IsNumericLiteral<`${Rest}${Decimal}`>
      : S extends `.${infer Decimal}`
        ? IsNumericLiteral<Decimal>
        : false;

// =============================================================================
// ALIAS PARSING FOR EXPRESSIONS
// =============================================================================

/**
 * Parse expression with potential AS alias
 * Handles:
 *   COUNT(*) AS count
 *   SUM(total) AS total_sum
 *   price * 1.1 AS with_tax
 *   col AS alias
 *
 * Strategy: Find " AS " or " as " and split there
 */
export type ParseExpressionWithAlias<S extends string> =
  // Try to find " AS " (case-insensitive) and extract parts
  FindAndSplitAtAs<Trim<S>>;

/**
 * Find " AS " or " as " in string and split into expression and alias
 */
type FindAndSplitAtAs<S extends string> =
  // Check for " AS " (uppercase)
  S extends `${infer Expr} AS ${infer Alias}`
    ? { expression: Trim<Expr>; alias: Trim<Lowercase<Alias>> }
    : // Check for " as " (lowercase)
      S extends `${infer Expr} as ${infer Alias}`
      ? { expression: Trim<Expr>; alias: Trim<Lowercase<Alias>> }
      : // Check for " As " (mixed case)
        S extends `${infer Expr} As ${infer Alias}`
        ? { expression: Trim<Expr>; alias: Trim<Lowercase<Alias>> }
        : // Check for " aS " (mixed case)
          S extends `${infer Expr} aS ${infer Alias}`
          ? { expression: Trim<Expr>; alias: Trim<Lowercase<Alias>> }
          : // No alias found
            { expression: Trim<S>; alias: null };

/**
 * Get the output column name for an expression
 * Uses alias if present, otherwise derives from expression
 */
export type GetExpressionOutputName<
  Parsed extends { expression: string; alias: string | null }
> =
  Parsed['alias'] extends string
    ? Parsed['alias']
    : DeriveNameFromExpression<Parsed['expression']>;

/**
 * Derive a column name from an expression when no alias is provided
 */
type DeriveNameFromExpression<Expr extends string> =
  // For aggregates without alias, use lowercase function name
  IsAggregateFunction<Expr> extends true
    ? Lowercase<ExtractAggregateFn<Expr>>
    : // For table.col, use col
      Expr extends `${string}.${infer Col}`
      ? Col
      : // Otherwise use the expression itself
        Expr;

// =============================================================================
// EXPRESSION TYPE RESOLUTION
// =============================================================================

/**
 * Resolve the type of any SQL expression
 */
export type ResolveExpressionType<
  Expr extends string,
  TableTypes extends Record<string, ColumnType>,
  AllTables extends Record<string, TableSchema> = {}
> =
  // 1. Aggregate function
  IsAggregateFunction<Expr> extends true
    ? ResolveAggregateType<Expr, TableTypes, AllTables>
    : // 2. Arithmetic expression
      IsArithmeticExpression<Expr> extends true
      ? ArithmeticExpressionType
      : // 3. Numeric literal
        IsNumericLiteral<Trim<Expr>> extends true
        ? 'number'
        : // 4. String literal (single quotes)
          Trim<Expr> extends `'${string}'`
          ? 'string'
          : // 5. Column reference
            ResolveColumnType<Trim<Expr>, TableTypes, AllTables>;

/**
 * Resolve aggregate function return type
 */
type ResolveAggregateType<
  Expr extends string,
  TableTypes extends Record<string, ColumnType>,
  AllTables extends Record<string, TableSchema>
> =
  ExtractAggregateFn<Expr> extends infer Fn extends AggregateFunctionName
    ? ExtractAggregateArg<Expr> extends infer Arg extends string
      ? Upper<Arg> extends '*'
        ? 'number' // COUNT(*) is always number
        : AggregateResultType<Fn, ResolveColumnType<Arg, TableTypes, AllTables>>
      : 'number'
    : 'unknown';

/**
 * Resolve a column reference to its type
 */
type ResolveColumnType<
  Col extends string,
  TableTypes extends Record<string, ColumnType>,
  AllTables extends Record<string, TableSchema>
> =
  // Handle "table.col" reference
  Col extends `${infer Table}.${infer Column}`
    ? Table extends keyof AllTables
      ? Column extends keyof AllTables[Table]
        ? AllTables[Table][Column]
        : 'unknown'
      : 'unknown'
    : // Handle bare column reference - look in TableTypes
      Col extends keyof TableTypes
      ? TableTypes[Col]
      : 'unknown';

// =============================================================================
// GROUP BY SUPPORT
// =============================================================================

/**
 * Check if query has GROUP BY clause
 */
export type HasGroupBy<SQL extends string> =
  Upper<SQL> extends `${string}GROUP BY${string}` ? true : false;

/**
 * Extract GROUP BY columns
 */
export type ExtractGroupByColumns<SQL extends string> =
  Upper<SQL> extends `${string}GROUP BY ${infer Rest}`
    ? ParseGroupByList<ExtractUntilClause<Rest>>
    : [];

// Extract until next SQL clause (HAVING, ORDER BY, LIMIT, etc.)
type ExtractUntilClause<S extends string> =
  Upper<S> extends `${infer Before} HAVING${string}` ? Trim<Before> :
  Upper<S> extends `${infer Before} ORDER${string}` ? Trim<Before> :
  Upper<S> extends `${infer Before} LIMIT${string}` ? Trim<Before> :
  Trim<S>;

// Parse comma-separated GROUP BY columns
type ParseGroupByList<S extends string> =
  S extends `${infer First},${infer Rest}`
    ? [Trim<Lowercase<First>>, ...ParseGroupByList<Trim<Rest>>]
    : S extends ''
      ? []
      : [Trim<Lowercase<S>>];

// =============================================================================
// COMPLETE EXPRESSION PARSER
// =============================================================================

/**
 * Parse and type a single SELECT column expression
 * Returns { name: string; type: ColumnType }
 */
export type ParseSelectExpression<
  Expr extends string,
  TableTypes extends Record<string, ColumnType>,
  AllTables extends Record<string, TableSchema> = {}
> = ParseExpressionWithAlias<Expr> extends {
  expression: infer E extends string;
  alias: infer A extends string | null;
}
  ? {
      name: A extends string ? A : DeriveNameFromExpression<E>;
      type: ResolveExpressionType<E, TableTypes, AllTables>;
    }
  : never;

/**
 * Build result type from parsed expression
 */
export type ExpressionToResultType<
  Parsed extends { name: string; type: ColumnType }
> = {
  [K in Parsed['name']]: Parsed['type'] extends 'string'
    ? string
    : Parsed['type'] extends 'number'
      ? number
      : Parsed['type'] extends 'boolean'
        ? boolean
        : Parsed['type'] extends 'Date'
          ? Date
          : Parsed['type'] extends 'null'
            ? null
            : unknown;
};

// =============================================================================
// ENHANCED QUERY RESULT WITH AGGREGATES
// =============================================================================

/**
 * Parse multiple SELECT expressions and combine into result type
 */
export type ParseSelectExpressions<
  Exprs extends string[],
  TableTypes extends Record<string, ColumnType>,
  AllTables extends Record<string, TableSchema> = {},
  Result extends object = {}
> = Exprs extends [infer First extends string, ...infer Rest extends string[]]
  ? ParseSelectExpression<First, TableTypes, AllTables> extends {
      name: infer N extends string;
      type: infer T extends ColumnType;
    }
    ? ParseSelectExpressions<
        Rest,
        TableTypes,
        AllTables,
        Result & { [K in N]: TypeToTS<T> }
      >
    : Result
  : Result;

// Convert ColumnType to TypeScript type
type TypeToTS<T extends ColumnType> =
  T extends 'string' ? string :
  T extends 'number' ? number :
  T extends 'boolean' ? boolean :
  T extends 'Date' ? Date :
  T extends 'null' ? null :
  unknown;

// =============================================================================
// EXPORTS FOR TESTING
// =============================================================================

export type {
  Trim,
  Upper,
  DeriveNameFromExpression,
  ResolveAggregateType,
  ResolveColumnType,
  TypeToTS,
};
