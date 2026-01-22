/**
 * DoSQL Execution Engine - Core Types
 *
 * Defines types for query plans, execution context, and operator infrastructure.
 * The engine bridges the B-tree (OLTP) and columnar (OLAP) storage layers.
 */

import type { BTree } from '../btree/types.js';
import type { ColumnarReader, Predicate as ColumnarPredicate } from '../columnar/index.js';
import { PlanningContext, getDefaultPlanningContext, resetDefaultPlanningContext } from '../planner/planning-context.js';

// =============================================================================
// BRANDED TYPES
// =============================================================================

/**
 * Branded types for critical identifiers to prevent accidental mixing.
 * These types use TypeScript's structural typing to create nominal types
 * that cannot be accidentally assigned from raw primitives.
 */

/** Brand symbol for Log Sequence Number */
declare const LSNBrand: unique symbol;

/**
 * Log Sequence Number (LSN) - A branded bigint type for WAL positions.
 * LSNs are monotonically increasing identifiers for WAL entries.
 *
 * @example
 * const lsn = createLSN(100n);
 * // lsn is LSN, not assignable from plain bigint
 */
export type LSN = bigint & { readonly [LSNBrand]: never };

/** Brand symbol for Transaction ID */
declare const TxnIdBrand: unique symbol;

/**
 * Transaction ID - A branded string type for transaction identifiers.
 * Transaction IDs uniquely identify a transaction across the system.
 *
 * @example
 * const txnId = createTransactionId('txn_abc123');
 * // txnId is TransactionId, not assignable from plain string
 */
export type TransactionId = string & { readonly [TxnIdBrand]: never };

/** Brand symbol for Shard ID */
declare const ShardIdBrand: unique symbol;

/**
 * Shard ID - A branded string type for shard identifiers.
 * Shard IDs identify a specific shard in a distributed setup.
 *
 * @example
 * const shardId = createShardId('shard_001');
 * // shardId is ShardId, not assignable from plain string
 */
export type ShardId = string & { readonly [ShardIdBrand]: never };

/** Brand symbol for Page ID */
declare const PageIdBrand: unique symbol;

/**
 * Page ID - A branded number type for B-tree page identifiers.
 * Page IDs identify a specific page in the B-tree storage.
 *
 * @example
 * const pageId = createPageId(42);
 * // pageId is PageId, not assignable from plain number
 */
export type PageId = number & { readonly [PageIdBrand]: never };

// =============================================================================
// BRANDED TYPE FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a branded LSN from a bigint value.
 * This is the only safe way to create an LSN.
 *
 * @param value - The bigint value for the LSN
 * @returns A branded LSN value
 * @throws {Error} If value is negative
 */
export function createLSN(value: bigint): LSN {
  if (value < 0n) {
    throw new Error(`LSN cannot be negative: ${value}`);
  }
  return value as LSN;
}

/**
 * Create a branded TransactionId from a string value.
 * This is the only safe way to create a TransactionId.
 *
 * @param value - The string value for the transaction ID
 * @returns A branded TransactionId value
 * @throws {Error} If value is empty
 */
export function createTransactionId(value: string): TransactionId {
  if (!value || value.length === 0) {
    throw new Error('TransactionId cannot be empty');
  }
  return value as TransactionId;
}

/**
 * Create a branded ShardId from a string value.
 * This is the only safe way to create a ShardId.
 *
 * @param value - The string value for the shard ID
 * @returns A branded ShardId value
 * @throws {Error} If value is empty
 */
export function createShardId(value: string): ShardId {
  if (!value || value.length === 0) {
    throw new Error('ShardId cannot be empty');
  }
  return value as ShardId;
}

/**
 * Create a branded PageId from a number value.
 * This is the only safe way to create a PageId.
 *
 * @param value - The number value for the page ID
 * @returns A branded PageId value
 * @throws {Error} If value is negative or not an integer
 */
export function createPageId(value: number): PageId {
  if (value < 0) {
    throw new Error(`PageId cannot be negative: ${value}`);
  }
  if (!Number.isInteger(value)) {
    throw new Error(`PageId must be an integer: ${value}`);
  }
  return value as PageId;
}

// =============================================================================
// BRANDED TYPE UTILITIES
// =============================================================================

/**
 * Type guard to check if a value is a valid LSN candidate.
 * Note: Due to branded type erasure at runtime, this checks if the value
 * could be cast to LSN (i.e., is a non-negative bigint).
 */
export function isValidLSN(value: unknown): value is bigint {
  return typeof value === 'bigint' && value >= 0n;
}

/**
 * Type guard to check if a value is a valid TransactionId candidate.
 */
export function isValidTransactionId(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

/**
 * Type guard to check if a value is a valid ShardId candidate.
 */
export function isValidShardId(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

/**
 * Type guard to check if a value is a valid PageId candidate.
 */
export function isValidPageId(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0;
}

/**
 * Compare two LSN values.
 * @returns negative if a < b, positive if a > b, 0 if equal
 */
export function compareLSN(a: LSN, b: LSN): number {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

/**
 * Get the raw bigint value from an LSN.
 * Useful for serialization or arithmetic operations.
 */
export function lsnValue(lsn: LSN): bigint {
  return lsn as bigint;
}

/**
 * Increment an LSN by a given amount.
 * @param lsn - The LSN to increment
 * @param amount - The amount to add (default: 1n)
 * @returns A new LSN with the incremented value
 */
export function incrementLSN(lsn: LSN, amount: bigint = 1n): LSN {
  return createLSN((lsn as bigint) + amount);
}

// =============================================================================
// VALUE TYPES
// =============================================================================

/**
 * Supported SQL value types at runtime
 */
export type SqlValue = string | number | bigint | boolean | Date | null | Uint8Array;

/**
 * A row is a record with string keys and SQL values
 */
export type Row = Record<string, SqlValue>;

// =============================================================================
// SCHEMA TYPES
// =============================================================================

/**
 * Column definition in a schema
 */
export interface ColumnDef {
  name: string;
  type: 'string' | 'number' | 'bigint' | 'boolean' | 'date' | 'bytes';
  nullable: boolean;
  primaryKey?: boolean;
}

/**
 * Table schema definition
 */
export interface TableSchema {
  name: string;
  columns: ColumnDef[];
  primaryKey?: string[];
  indexes?: IndexDef[];
}

/**
 * Index definition
 */
export interface IndexDef {
  name: string;
  columns: string[];
  unique: boolean;
}

/**
 * Database schema (collection of tables)
 */
export interface Schema {
  tables: Map<string, TableSchema>;
}

// =============================================================================
// EXPRESSION TYPES
// =============================================================================

/**
 * Comparison operators for predicates
 */
export type ComparisonOp = 'eq' | 'ne' | 'lt' | 'le' | 'gt' | 'ge' | 'like' | 'in' | 'between' | 'isNull' | 'isNotNull';

/**
 * Logical operators for combining predicates
 */
export type LogicalOp = 'and' | 'or' | 'not';

/**
 * Arithmetic operators
 */
export type ArithmeticOp = 'add' | 'sub' | 'mul' | 'div' | 'mod';

/**
 * Aggregate function names
 */
export type AggregateFunction = 'count' | 'sum' | 'avg' | 'min' | 'max';

/**
 * Expression types for the query plan
 */
export type Expression =
  | ColumnRef
  | Literal
  | BinaryExpr
  | UnaryExpr
  | FunctionCall
  | AggregateExpr
  | CaseExpr
  | SubqueryExpr;

/**
 * Column reference expression
 */
export interface ColumnRef {
  type: 'columnRef';
  table?: string;
  column: string;
}

/**
 * Literal value expression
 */
export interface Literal {
  type: 'literal';
  value: SqlValue;
  dataType: 'string' | 'number' | 'bigint' | 'boolean' | 'date' | 'bytes' | 'null';
}

/**
 * Binary expression (comparison, arithmetic, logical)
 */
export interface BinaryExpr {
  type: 'binary';
  op: ComparisonOp | ArithmeticOp | LogicalOp;
  left: Expression;
  right: Expression;
}

/**
 * Unary expression (NOT, negation)
 */
export interface UnaryExpr {
  type: 'unary';
  op: 'not' | 'neg' | 'isNull' | 'isNotNull';
  operand: Expression;
}

/**
 * Function call expression
 */
export interface FunctionCall {
  type: 'function';
  name: string;
  args: Expression[];
}

/**
 * Aggregate expression
 */
export interface AggregateExpr {
  type: 'aggregate';
  function: AggregateFunction;
  arg: Expression | '*';
  distinct?: boolean;
}

/**
 * CASE expression
 */
export interface CaseExpr {
  type: 'case';
  when: { condition: Expression; result: Expression }[];
  else?: Expression;
}

/**
 * Subquery expression
 */
export interface SubqueryExpr {
  type: 'subquery';
  plan: QueryPlan;
  kind: 'scalar' | 'exists' | 'in';
}

// =============================================================================
// PREDICATE TYPES
// =============================================================================

/**
 * A predicate for filtering rows
 */
export interface PredicateNode {
  type: 'comparison' | 'logical' | 'between' | 'in' | 'isNull';
}

/**
 * Comparison predicate
 */
export interface ComparisonPredicate extends PredicateNode {
  type: 'comparison';
  op: ComparisonOp;
  left: Expression;
  right: Expression;
}

/**
 * Logical predicate (AND, OR, NOT)
 */
export interface LogicalPredicate extends PredicateNode {
  type: 'logical';
  op: LogicalOp;
  operands: Predicate[];
}

/**
 * BETWEEN predicate
 */
export interface BetweenPredicate extends PredicateNode {
  type: 'between';
  expr: Expression;
  low: Expression;
  high: Expression;
}

/**
 * IN predicate
 */
export interface InPredicate extends PredicateNode {
  type: 'in';
  expr: Expression;
  values: Expression[] | QueryPlan;
}

/**
 * IS NULL / IS NOT NULL predicate
 */
export interface IsNullPredicate extends PredicateNode {
  type: 'isNull';
  expr: Expression;
  isNot: boolean;
}

export type Predicate = ComparisonPredicate | LogicalPredicate | BetweenPredicate | InPredicate | IsNullPredicate;

// =============================================================================
// QUERY PLAN TYPES
// =============================================================================

/**
 * Data source for a scan
 */
export type DataSource = 'btree' | 'columnar' | 'both';

/**
 * Sort direction
 */
export type SortDirection = 'asc' | 'desc';

/**
 * Sort specification
 */
export interface SortSpec {
  expr: Expression;
  direction: SortDirection;
  nullsFirst?: boolean;
}

/**
 * Join type
 */
export type JoinType = 'inner' | 'left' | 'right' | 'full' | 'cross';

/**
 * Base query plan node
 */
export interface BasePlanNode {
  /** Unique identifier for this plan node */
  id: number;
  /** Estimated row count for this node */
  estimatedRows?: number;
  /** Estimated cost for this node */
  estimatedCost?: number;
}

/**
 * Table scan plan node
 */
export interface ScanPlan extends BasePlanNode {
  type: 'scan';
  table: string;
  alias?: string;
  source: DataSource;
  columns: string[];
  predicate?: Predicate;
}

/**
 * Index lookup plan node
 */
export interface IndexLookupPlan extends BasePlanNode {
  type: 'indexLookup';
  table: string;
  alias?: string;
  index: string;
  lookupKey: Expression[];
  columns: string[];
}

/**
 * Filter plan node
 */
export interface FilterPlan extends BasePlanNode {
  type: 'filter';
  input: QueryPlan;
  predicate: Predicate;
}

/**
 * Project plan node (column selection and expressions)
 */
export interface ProjectPlan extends BasePlanNode {
  type: 'project';
  input: QueryPlan;
  expressions: { expr: Expression; alias: string }[];
}

/**
 * Join plan node
 */
export interface JoinPlan extends BasePlanNode {
  type: 'join';
  joinType: JoinType;
  left: QueryPlan;
  right: QueryPlan;
  condition?: Predicate;
  algorithm?: 'nestedLoop' | 'hash' | 'merge';
}

/**
 * Aggregate plan node
 */
export interface AggregatePlan extends BasePlanNode {
  type: 'aggregate';
  input: QueryPlan;
  groupBy: Expression[];
  aggregates: { expr: AggregateExpr; alias: string }[];
  having?: Predicate;
}

/**
 * Sort plan node
 */
export interface SortPlan extends BasePlanNode {
  type: 'sort';
  input: QueryPlan;
  orderBy: SortSpec[];
}

/**
 * Limit plan node
 */
export interface LimitPlan extends BasePlanNode {
  type: 'limit';
  input: QueryPlan;
  limit: number;
  offset?: number;
}

/**
 * Distinct plan node
 */
export interface DistinctPlan extends BasePlanNode {
  type: 'distinct';
  input: QueryPlan;
  columns?: string[];
}

/**
 * Union plan node
 */
export interface UnionPlan extends BasePlanNode {
  type: 'union';
  inputs: QueryPlan[];
  all: boolean;
}

/**
 * Merge plan node (for combining hot/cold data)
 */
export interface MergePlan extends BasePlanNode {
  type: 'merge';
  inputs: QueryPlan[];
  orderBy?: SortSpec[];
}

/**
 * All query plan types
 */
export type QueryPlan =
  | ScanPlan
  | IndexLookupPlan
  | FilterPlan
  | ProjectPlan
  | JoinPlan
  | AggregatePlan
  | SortPlan
  | LimitPlan
  | DistinctPlan
  | UnionPlan
  | MergePlan;

// =============================================================================
// EXECUTION CONTEXT
// =============================================================================

/**
 * B-tree storage interface for the engine
 */
export interface BTreeStorage {
  /** Get a value by primary key */
  get(table: string, key: SqlValue): Promise<Row | undefined>;
  /** Scan a range of keys */
  range(table: string, start: SqlValue, end: SqlValue): AsyncIterableIterator<Row>;
  /** Scan all entries in a table */
  scan(table: string): AsyncIterableIterator<Row>;
  /** Insert or update a row */
  set(table: string, key: SqlValue, row: Row): Promise<void>;
  /** Delete a row by key */
  delete(table: string, key: SqlValue): Promise<boolean>;
  /** Get count of rows in table */
  count(table: string): Promise<number>;
}

/**
 * Columnar storage interface for the engine
 */
export interface ColumnarStorage {
  /** Scan with optional predicates and projection */
  scan(
    table: string,
    options?: {
      columns?: string[];
      predicates?: ColumnarPredicate[];
      limit?: number;
      offset?: number;
    }
  ): AsyncIterableIterator<Row>;
  /** Get count of rows matching predicates */
  count(table: string, predicates?: ColumnarPredicate[]): Promise<number>;
  /** Get sum of a column (can use stats if available) */
  sum(table: string, column: string, predicates?: ColumnarPredicate[]): Promise<number | bigint | null>;
  /** Get min/max of a column (can use stats if available) */
  minMax(table: string, column: string, predicates?: ColumnarPredicate[]): Promise<{ min: SqlValue; max: SqlValue }>;
}

/**
 * Execution context passed to operators
 */
export interface ExecutionContext {
  /** Database schema */
  schema: Schema;
  /** B-tree storage for hot/recent data */
  btree: BTreeStorage;
  /** Columnar storage for cold/historical data */
  columnar: ColumnarStorage;
  /** Current transaction ID (if any) */
  transactionId?: string;
  /** Query parameters (for prepared statements) */
  parameters?: Map<string, SqlValue>;
  /** Execution options */
  options?: ExecutionOptions;
}

/**
 * Execution options
 */
export interface ExecutionOptions {
  /** Maximum rows to return */
  maxRows?: number;
  /** Timeout in milliseconds */
  timeout?: number;
  /** Prefer hot data over cold */
  preferHot?: boolean;
  /** Enable parallel execution */
  parallel?: boolean;
  /** Explain plan only (don't execute) */
  explain?: boolean;
}

// =============================================================================
// OPERATOR INTERFACE
// =============================================================================

/**
 * Pull-based operator interface
 * Each operator produces rows on demand
 */
export interface Operator {
  /** Open the operator (initialize state) */
  open(ctx: ExecutionContext): Promise<void>;
  /** Get the next row (null = no more rows) */
  next(): Promise<Row | null>;
  /** Close the operator (cleanup) */
  close(): Promise<void>;
  /** Get output columns */
  columns(): string[];
}

/**
 * Operator factory function
 */
export type OperatorFactory = (plan: QueryPlan, ctx: ExecutionContext) => Operator;

// =============================================================================
// QUERY RESULT
// =============================================================================

/**
 * Query execution result
 */
export interface QueryResult<T = Row> {
  /** Rows returned by the query */
  rows: T[];
  /** Number of rows affected (for INSERT/UPDATE/DELETE) */
  rowsAffected?: number;
  /** Column metadata */
  columns?: { name: string; type: string }[];
  /** Execution statistics */
  stats?: ExecutionStats;
}

/**
 * Execution statistics
 */
export interface ExecutionStats {
  /** Time to plan the query (ms) */
  planningTime: number;
  /** Time to execute the query (ms) */
  executionTime: number;
  /** Rows scanned */
  rowsScanned: number;
  /** Rows returned */
  rowsReturned: number;
  /** Bytes read */
  bytesRead?: number;
  /** Cache hits */
  cacheHits?: number;
  /** Cache misses */
  cacheMisses?: number;
}

// =============================================================================
// SQL TEMPLATE TAG
// =============================================================================

/**
 * Tagged template result for parameterized queries
 */
export interface SqlTemplate {
  sql: string;
  parameters: SqlValue[];
}

/**
 * Tagged template function for SQL queries
 */
export function sql(strings: TemplateStringsArray, ...values: SqlValue[]): SqlTemplate {
  const parts: string[] = [];
  for (let i = 0; i < strings.length; i++) {
    parts.push(strings[i]);
    if (i < values.length) {
      parts.push(`$${i + 1}`);
    }
  }
  return {
    sql: parts.join(''),
    parameters: values,
  };
}

// =============================================================================
// ENGINE INTERFACE
// =============================================================================

/**
 * DoSQL execution engine interface
 */
export interface Engine {
  /** Execute a SQL query */
  execute<T = Row>(query: string | SqlTemplate): Promise<QueryResult<T>>;
  /** Execute a SQL query and return just the rows */
  query<T = Row>(query: string | SqlTemplate): Promise<T[]>;
  /** Execute a SQL query and return the first row */
  queryOne<T = Row>(query: string | SqlTemplate): Promise<T | null>;
  /** Prepare a query plan (for analysis/debugging) */
  prepare(query: string): Promise<QueryPlan>;
  /** Explain the query plan */
  explain(query: string): Promise<string>;
  /** Get the schema */
  getSchema(): Schema;
}

// =============================================================================
// HELPER TYPES
// =============================================================================

/**
 * Create a column reference expression
 */
export function col(column: string, table?: string): ColumnRef {
  return { type: 'columnRef', column, table };
}

/**
 * Create a literal expression
 */
export function lit(value: SqlValue): Literal {
  const dataType =
    value === null ? 'null' :
    typeof value === 'string' ? 'string' :
    typeof value === 'number' ? 'number' :
    typeof value === 'bigint' ? 'bigint' :
    typeof value === 'boolean' ? 'boolean' :
    value instanceof Date ? 'date' :
    value instanceof Uint8Array ? 'bytes' :
    'null';
  return { type: 'literal', value, dataType };
}

/**
 * Generate unique plan node IDs using the default (shared) context.
 * For concurrent planning, use PlanningContext directly.
 *
 * @deprecated For new code, use PlanningContext.nextId() with an isolated context
 */
export function nextPlanId(): number {
  return getDefaultPlanningContext().nextId();
}

/**
 * Reset plan node ID counter (for testing).
 * This resets the default shared context.
 *
 * @deprecated For new code, use isolated PlanningContext instances
 */
export function resetPlanIds(): void {
  resetDefaultPlanningContext();
}
