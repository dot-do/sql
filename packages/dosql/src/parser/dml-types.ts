/**
 * DoSQL DML (Data Manipulation Language) AST Types
 *
 * Type definitions for INSERT, UPDATE, DELETE, and REPLACE statements.
 * Follows SQLite-compatible syntax with extensions for conflict handling.
 */

// =============================================================================
// COMMON TYPES
// =============================================================================

/**
 * SQL expression node - represents any SQL expression
 */
export type Expression =
  | LiteralExpression
  | ColumnReference
  | FunctionCall
  | BinaryExpression
  | UnaryExpression
  | SubqueryExpression
  | CaseExpression
  | NullExpression
  | DefaultExpression
  | ParameterExpression;

/**
 * Literal value expression
 */
export interface LiteralExpression {
  type: 'literal';
  value: string | number | boolean | null;
  /** Original string representation */
  raw: string;
}

/**
 * Column reference (possibly qualified with table name)
 */
export interface ColumnReference {
  type: 'column';
  /** Column name */
  name: string;
  /** Table name or alias (optional) */
  table?: string;
  /** Schema name (optional, for schema.table.column syntax) */
  schema?: string;
}

/**
 * Function call expression
 */
export interface FunctionCall {
  type: 'function';
  name: string;
  args: Expression[];
  /** Whether DISTINCT is used (e.g., COUNT(DISTINCT x)) */
  distinct?: boolean;
  /** Whether this function has an OVER clause (window function) */
  hasOver?: boolean;
}

/**
 * Binary operation (e.g., a + b, x = y, a AND b)
 */
export interface BinaryExpression {
  type: 'binary';
  operator: BinaryOperator;
  left: Expression;
  right: Expression;
}

/**
 * Unary operation (e.g., NOT x, -y)
 */
export interface UnaryExpression {
  type: 'unary';
  operator: UnaryOperator;
  operand: Expression;
}

/**
 * Subquery as expression
 */
export interface SubqueryExpression {
  type: 'subquery';
  query: string; // For now, keep as string; can be parsed recursively
}

/**
 * CASE expression
 */
export interface CaseExpression {
  type: 'case';
  operand?: Expression;
  when: Array<{ condition: Expression; result: Expression }>;
  else?: Expression;
}

/**
 * NULL expression
 */
export interface NullExpression {
  type: 'null';
}

/**
 * DEFAULT expression (used in INSERT to indicate default value)
 */
export interface DefaultExpression {
  type: 'default';
}

/**
 * Parameter placeholder (? or :name or $n)
 */
export interface ParameterExpression {
  type: 'parameter';
  /** Parameter index (for ?) or name (for :name/$name) */
  name: string | number;
  /** Original representation */
  raw: string;
}

/**
 * Binary operators
 */
export type BinaryOperator =
  // Arithmetic
  | '+'
  | '-'
  | '*'
  | '/'
  | '%'
  // Comparison
  | '='
  | '!='
  | '<>'
  | '<'
  | '<='
  | '>'
  | '>='
  | 'IS'
  | 'IS NOT'
  | 'LIKE'
  | 'NOT LIKE'
  | 'GLOB'
  | 'NOT GLOB'
  | 'REGEXP'
  | 'NOT REGEXP'
  | 'IN'
  | 'NOT IN'
  | 'BETWEEN'
  | 'NOT BETWEEN'
  // Logical
  | 'AND'
  | 'OR'
  // Bitwise
  | '&'
  | '|'
  | '<<'
  | '>>'
  // String concatenation
  | '||';

/**
 * Unary operators
 */
export type UnaryOperator = 'NOT' | '-' | '+' | '~';

// =============================================================================
// WHERE CLAUSE
// =============================================================================

/**
 * WHERE clause for filtering rows
 */
export interface WhereClause {
  type: 'where';
  condition: Expression;
}

// =============================================================================
// CONFLICT HANDLING (SQLite Extensions)
// =============================================================================

/**
 * Conflict action for INSERT/UPDATE/REPLACE
 */
export type ConflictAction = 'ROLLBACK' | 'ABORT' | 'REPLACE' | 'FAIL' | 'IGNORE';

/**
 * Conflict resolution clause (OR REPLACE, OR IGNORE, etc.)
 */
export interface ConflictClause {
  type: 'conflict';
  action: ConflictAction;
}

/**
 * ON CONFLICT clause for UPSERT (SQLite 3.24+)
 */
export interface OnConflictClause {
  type: 'on_conflict';
  /** Conflict target (columns that trigger conflict) */
  target?: ConflictTarget;
  /** Action to take on conflict */
  action: OnConflictAction;
}

/**
 * Conflict target specification
 */
export interface ConflictTarget {
  /** Column names that form the conflict target */
  columns: string[];
  /** Optional WHERE clause for partial unique index */
  where?: Expression;
}

/**
 * Action to take on conflict
 */
export type OnConflictAction = OnConflictDoNothing | OnConflictDoUpdate;

/**
 * DO NOTHING action
 */
export interface OnConflictDoNothing {
  type: 'do_nothing';
}

/**
 * DO UPDATE SET action (UPSERT)
 */
export interface OnConflictDoUpdate {
  type: 'do_update';
  /** SET clauses for update */
  set: SetClause[];
  /** Optional WHERE clause for conditional update */
  where?: Expression;
}

// =============================================================================
// INSERT STATEMENT
// =============================================================================

/**
 * Single row of values in a VALUES clause
 */
export interface ValuesRow {
  type: 'values_row';
  values: Expression[];
}

/**
 * VALUES clause - one or more rows
 */
export interface ValuesList {
  type: 'values_list';
  rows: ValuesRow[];
}

/**
 * INSERT source - either VALUES or SELECT
 */
export type InsertSource = ValuesList | InsertSelect | InsertDefault;

/**
 * INSERT ... SELECT source
 */
export interface InsertSelect {
  type: 'insert_select';
  /** The SELECT query (as string for now; can be parsed recursively) */
  query: string;
}

/**
 * INSERT ... DEFAULT VALUES
 */
export interface InsertDefault {
  type: 'insert_default';
}

/**
 * INSERT statement AST
 */
export interface InsertStatement {
  type: 'insert';
  /** Conflict handling (OR REPLACE, OR IGNORE, etc.) */
  conflict?: ConflictClause;
  /** Target table name */
  table: string;
  /** Optional table alias */
  alias?: string;
  /** Column names (optional - can be omitted for VALUES) */
  columns?: string[];
  /** Source of values */
  source: InsertSource;
  /** ON CONFLICT clause (UPSERT) */
  onConflict?: OnConflictClause;
  /** RETURNING clause columns */
  returning?: ReturningClause;
}

// =============================================================================
// UPDATE STATEMENT
// =============================================================================

/**
 * SET clause - column = expression assignment
 */
export interface SetClause {
  type: 'set';
  /** Column name to update */
  column: string;
  /** New value expression */
  value: Expression;
}

/**
 * UPDATE statement AST
 */
export interface UpdateStatement {
  type: 'update';
  /** Conflict handling (OR REPLACE, OR IGNORE, etc.) */
  conflict?: ConflictClause;
  /** Target table name */
  table: string;
  /** Optional table alias */
  alias?: string;
  /** SET clauses */
  set: SetClause[];
  /** Optional FROM clause for UPDATE ... FROM */
  from?: FromClause;
  /** Optional WHERE clause */
  where?: WhereClause;
  /** ORDER BY clause (SQLite extension) */
  orderBy?: OrderByClause;
  /** LIMIT clause (SQLite extension) */
  limit?: LimitClause;
  /** RETURNING clause columns */
  returning?: ReturningClause;
}

// =============================================================================
// DELETE STATEMENT
// =============================================================================

/**
 * Index hint for forcing/disabling index usage
 */
export interface IndexHint {
  type: 'indexed_by' | 'not_indexed';
  /** Index name (only for 'indexed_by' type) */
  indexName?: string;
}

/**
 * DELETE statement AST
 */
export interface DeleteStatement {
  type: 'delete';
  /** Target table name */
  table: string;
  /** Optional table alias */
  alias?: string;
  /** Optional index hint (INDEXED BY or NOT INDEXED) */
  indexHint?: IndexHint;
  /** Optional WHERE clause */
  where?: WhereClause;
  /** ORDER BY clause (SQLite extension) */
  orderBy?: OrderByClause;
  /** LIMIT clause (SQLite extension) */
  limit?: LimitClause;
  /** RETURNING clause columns */
  returning?: ReturningClause;
}

// =============================================================================
// REPLACE STATEMENT (SQLite Extension)
// =============================================================================

/**
 * REPLACE statement - INSERT OR REPLACE shorthand
 */
export interface ReplaceStatement {
  type: 'replace';
  /** Target table name */
  table: string;
  /** Optional table alias */
  alias?: string;
  /** Column names (optional) */
  columns?: string[];
  /** Source of values */
  source: InsertSource;
  /** RETURNING clause columns */
  returning?: ReturningClause;
}

// =============================================================================
// AUXILIARY CLAUSES
// =============================================================================

/**
 * FROM clause (for UPDATE ... FROM)
 */
export interface FromClause {
  type: 'from';
  /** Table references (simplified for now) */
  tables: TableReference[];
}

/**
 * Table reference in FROM clause
 */
export interface TableReference {
  type: 'table_ref';
  /** Table name */
  name: string;
  /** Optional alias */
  alias?: string;
}

/**
 * ORDER BY clause
 */
export interface OrderByClause {
  type: 'order_by';
  items: OrderByItem[];
}

/**
 * Single ORDER BY item
 */
export interface OrderByItem {
  expression: Expression;
  direction: 'ASC' | 'DESC';
  nulls?: 'FIRST' | 'LAST';
}

/**
 * LIMIT clause
 */
export interface LimitClause {
  type: 'limit';
  /** Number of rows to return */
  count: Expression;
  /** OFFSET value (optional) */
  offset?: Expression;
}

/**
 * RETURNING clause (returns affected rows)
 */
export interface ReturningClause {
  type: 'returning';
  /** Columns to return (* for all, or specific columns/expressions) */
  columns: ReturningColumn[];
  /** Whether the clause contains a wildcard (*) that needs schema expansion */
  requiresWildcardExpansion?: boolean;
}

/**
 * Single column in RETURNING clause
 */
export interface ReturningColumn {
  expression: Expression | '*';
  alias?: string;
}

// =============================================================================
// UNION TYPE FOR ALL DML STATEMENTS
// =============================================================================

/**
 * Any DML statement
 */
export type DMLStatement =
  | InsertStatement
  | UpdateStatement
  | DeleteStatement
  | ReplaceStatement;

// =============================================================================
// PARSER RESULT TYPES
// =============================================================================

/**
 * Successful parse result
 */
export interface ParseSuccess<T extends DMLStatement> {
  success: true;
  statement: T;
  /** Remaining unparsed input (should be empty for valid SQL) */
  remaining: string;
}

/**
 * Failed parse result with enhanced location information
 */
export interface ParseError {
  success: false;
  /** Error message */
  error: string;
  /** Character offset where error occurred (0-indexed) */
  position: number;
  /** The input that caused the error */
  input: string;
  /** Line number (1-indexed) */
  line?: number;
  /** Column number (1-indexed) */
  column?: number;
  /** The problematic token (if available) */
  token?: string;
  /** Expected token(s) (if available) */
  expected?: string;
  /** Suggestion for fix (if available) */
  suggestion?: string;
}

/**
 * Parse result
 */
export type ParseResult<T extends DMLStatement> = ParseSuccess<T> | ParseError;

// =============================================================================
// TYPE GUARDS
// =============================================================================

/**
 * Check if a statement is an INSERT statement
 */
export function isInsertStatement(stmt: DMLStatement): stmt is InsertStatement {
  return stmt.type === 'insert';
}

/**
 * Check if a statement is an UPDATE statement
 */
export function isUpdateStatement(stmt: DMLStatement): stmt is UpdateStatement {
  return stmt.type === 'update';
}

/**
 * Check if a statement is a DELETE statement
 */
export function isDeleteStatement(stmt: DMLStatement): stmt is DeleteStatement {
  return stmt.type === 'delete';
}

/**
 * Check if a statement is a REPLACE statement
 */
export function isReplaceStatement(stmt: DMLStatement): stmt is ReplaceStatement {
  return stmt.type === 'replace';
}

/**
 * Check if an expression is a literal
 */
export function isLiteralExpression(expr: Expression): expr is LiteralExpression {
  return expr.type === 'literal';
}

/**
 * Check if an expression is a column reference
 */
export function isColumnReference(expr: Expression): expr is ColumnReference {
  return expr.type === 'column';
}

/**
 * Check if an expression is a function call
 */
export function isFunctionCall(expr: Expression): expr is FunctionCall {
  return expr.type === 'function';
}

/**
 * Check if an expression is a binary expression
 */
export function isBinaryExpression(expr: Expression): expr is BinaryExpression {
  return expr.type === 'binary';
}

/**
 * Check if parse result is successful
 */
export function isParseSuccess<T extends DMLStatement>(
  result: ParseResult<T>
): result is ParseSuccess<T> {
  return result.success === true;
}

/**
 * Check if parse result is an error
 */
export function isParseError<T extends DMLStatement>(
  result: ParseResult<T>
): result is ParseError {
  return result.success === false;
}
