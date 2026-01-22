/**
 * Statement Types for DoSQL
 *
 * Compatible with better-sqlite3 and D1 APIs.
 */

// =============================================================================
// BIND PARAMETERS
// =============================================================================

/**
 * Supported SQL value types for binding
 */
export type SqlValue = string | number | bigint | boolean | null | Uint8Array | Date;

/**
 * Named parameters object using :name, @name, or $name syntax
 */
export type NamedParameters = Record<string, SqlValue>;

/**
 * Positional parameters array using ? or ?NNN syntax
 */
export type PositionalParameters = SqlValue[];

/**
 * Union of all parameter binding styles
 */
export type BindParameters = NamedParameters | PositionalParameters | SqlValue[];

// =============================================================================
// RUN RESULT
// =============================================================================

/**
 * Result from executing a write operation (INSERT, UPDATE, DELETE)
 */
export interface RunResult {
  /**
   * Number of rows affected by the statement
   */
  changes: number;

  /**
   * Row ID of the last inserted row (for INSERT statements)
   * Returns 0 if no rows were inserted
   */
  lastInsertRowid: number | bigint;
}

// =============================================================================
// COLUMN INFO
// =============================================================================

/**
 * Column metadata returned by columns()
 */
export interface ColumnInfo {
  /**
   * Column name as defined in the query/table
   */
  name: string;

  /**
   * Original column name (before AS alias)
   */
  column: string | null;

  /**
   * Table name the column belongs to (null for expressions)
   */
  table: string | null;

  /**
   * Database name (always 'main' for single-db)
   */
  database: string | null;

  /**
   * Declared type from schema (e.g., 'INTEGER', 'TEXT')
   */
  type: string | null;
}

// =============================================================================
// STATEMENT OPTIONS
// =============================================================================

/**
 * Options for statement preparation and execution
 */
export interface StatementOptions {
  /**
   * Whether to use BigInt for INTEGER columns (default: false)
   */
  safeIntegers?: boolean;

  /**
   * Whether to expand array parameters (default: false)
   */
  expand?: boolean;

  /**
   * Whether to pluck the first column of each row (default: false)
   */
  pluck?: boolean;

  /**
   * Whether to return raw arrays instead of objects (default: false)
   */
  raw?: boolean;
}

// =============================================================================
// STATEMENT INTERFACE
// =============================================================================

/**
 * Prepared statement interface (better-sqlite3/D1 compatible)
 *
 * @template T - The expected row type for SELECT queries
 * @template P - The parameter types for binding
 */
export interface Statement<T = unknown, P extends BindParameters = BindParameters> {
  /**
   * The original SQL string
   */
  readonly source: string;

  /**
   * Whether the statement is read-only (SELECT, etc.)
   */
  readonly reader: boolean;

  /**
   * Whether the statement has been finalized
   */
  readonly finalized: boolean;

  /**
   * Bind parameters to the statement (chainable)
   *
   * @param params - Parameters to bind
   * @returns this (for chaining)
   *
   * @example
   * ```typescript
   * const stmt = db.prepare('SELECT * FROM users WHERE id = ?');
   * const user = stmt.bind(123).get();
   * ```
   */
  bind(...params: P extends any[] ? P : [P]): this;

  /**
   * Execute the statement and return run result
   * For INSERT, UPDATE, DELETE statements
   *
   * @param params - Optional parameters to bind
   * @returns RunResult with changes and lastInsertRowid
   *
   * @example
   * ```typescript
   * const result = db.prepare('INSERT INTO users (name) VALUES (?)').run('Alice');
   * console.log(result.lastInsertRowid); // 1
   * ```
   */
  run(...params: P extends any[] ? P : [P]): RunResult;

  /**
   * Execute the statement and return the first row
   *
   * @param params - Optional parameters to bind
   * @returns First row or undefined if no results
   *
   * @example
   * ```typescript
   * const user = db.prepare('SELECT * FROM users WHERE id = ?').get(1);
   * ```
   */
  get(...params: P extends any[] ? P : [P]): T | undefined;

  /**
   * Execute the statement and return all rows
   *
   * @param params - Optional parameters to bind
   * @returns Array of all matching rows
   *
   * @example
   * ```typescript
   * const adults = db.prepare('SELECT * FROM users WHERE age > ?').all(21);
   * ```
   */
  all(...params: P extends any[] ? P : [P]): T[];

  /**
   * Execute the statement and return an iterator
   *
   * @param params - Optional parameters to bind
   * @returns Iterator over result rows
   *
   * @example
   * ```typescript
   * for (const user of db.prepare('SELECT * FROM users').iterate()) {
   *   console.log(user.name);
   * }
   * ```
   */
  iterate(...params: P extends any[] ? P : [P]): IterableIterator<T>;

  /**
   * Get column metadata for the result set
   *
   * @returns Array of ColumnInfo objects
   *
   * @example
   * ```typescript
   * const cols = db.prepare('SELECT id, name FROM users').columns();
   * console.log(cols[0].name); // 'id'
   * ```
   */
  columns(): ColumnInfo[];

  /**
   * Release resources associated with the statement
   * Statement cannot be used after calling finalize()
   */
  finalize(): void;

  /**
   * Configure safeIntegers mode
   *
   * @param enabled - Whether to use BigInt for integers
   * @returns this (for chaining)
   */
  safeIntegers(enabled?: boolean): this;

  /**
   * Configure expand mode for object results
   *
   * @param enabled - Whether to expand nested objects
   * @returns this (for chaining)
   */
  expand(enabled?: boolean): this;

  /**
   * Configure pluck mode to return only first column
   *
   * @param enabled - Whether to pluck first column
   * @returns this (for chaining)
   */
  pluck(enabled?: boolean): this;

  /**
   * Configure raw mode to return arrays instead of objects
   *
   * @param enabled - Whether to return raw arrays
   * @returns this (for chaining)
   */
  raw(enabled?: boolean): this;
}

// =============================================================================
// TRANSACTION TYPES
// =============================================================================

/**
 * Transaction mode for transaction wrapper
 */
export type TransactionMode = 'deferred' | 'immediate' | 'exclusive';

/**
 * Transaction function wrapper
 */
export interface TransactionFunction<F extends (...args: any[]) => any> {
  /**
   * Execute the function within a transaction
   */
  (...args: Parameters<F>): ReturnType<F>;

  /**
   * Execute with deferred transaction mode
   */
  deferred(...args: Parameters<F>): ReturnType<F>;

  /**
   * Execute with immediate transaction mode
   */
  immediate(...args: Parameters<F>): ReturnType<F>;

  /**
   * Execute with exclusive transaction mode
   */
  exclusive(...args: Parameters<F>): ReturnType<F>;
}

// =============================================================================
// DATABASE OPTIONS
// =============================================================================

/**
 * Options for Database constructor
 */
export interface DatabaseOptions {
  /**
   * Whether to open database in read-only mode
   */
  readonly?: boolean;

  /**
   * Whether to create the database if it doesn't exist
   */
  fileMustExist?: boolean;

  /**
   * Timeout for acquiring locks (milliseconds)
   */
  timeout?: number;

  /**
   * Whether to log verbose output
   */
  verbose?: (message?: unknown, ...params: unknown[]) => void;

  /**
   * Maximum size of the statement cache
   */
  statementCacheSize?: number;
}

// =============================================================================
// PRAGMA TYPES
// =============================================================================

/**
 * Known PRAGMA names for type-safe pragma() calls
 */
export type PragmaName =
  | 'journal_mode'
  | 'synchronous'
  | 'foreign_keys'
  | 'cache_size'
  | 'page_size'
  | 'auto_vacuum'
  | 'busy_timeout'
  | 'wal_checkpoint'
  | 'table_info'
  | 'index_list'
  | 'database_list'
  | 'compile_options'
  | 'user_version'
  | 'application_id'
  | 'integrity_check'
  | 'quick_check'
  | string;

/**
 * PRAGMA return types based on pragma name
 */
export type PragmaResult<N extends PragmaName> =
  N extends 'journal_mode' ? string :
  N extends 'synchronous' ? number :
  N extends 'foreign_keys' ? number :
  N extends 'cache_size' ? number :
  N extends 'page_size' ? number :
  N extends 'busy_timeout' ? number :
  N extends 'user_version' ? number :
  N extends 'application_id' ? number :
  N extends 'integrity_check' ? string[] :
  N extends 'quick_check' ? string[] :
  N extends 'table_info' ? TableInfoRow[] :
  N extends 'index_list' ? IndexListRow[] :
  N extends 'database_list' ? DatabaseListRow[] :
  N extends 'compile_options' ? string[] :
  unknown;

/**
 * Row from PRAGMA table_info
 */
export interface TableInfoRow {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
}

/**
 * Row from PRAGMA index_list
 */
export interface IndexListRow {
  seq: number;
  name: string;
  unique: number;
  origin: string;
  partial: number;
}

/**
 * Row from PRAGMA database_list
 */
export interface DatabaseListRow {
  seq: number;
  name: string;
  file: string;
}

// =============================================================================
// DATABASE INTERFACE
// =============================================================================

/**
 * Database interface (better-sqlite3/D1 compatible)
 */
export interface Database {
  /**
   * Whether the database is open
   */
  readonly open: boolean;

  /**
   * Whether the database is in memory
   */
  readonly inTransaction: boolean;

  /**
   * Database filename (or ':memory:')
   */
  readonly name: string;

  /**
   * Whether the database is read-only
   */
  readonly readonly: boolean;

  /**
   * Prepare a SQL statement
   *
   * @param sql - SQL string
   * @returns Prepared statement
   *
   * @example
   * ```typescript
   * const stmt = db.prepare('SELECT * FROM users WHERE id = ?');
   * const user = stmt.get(1);
   * ```
   */
  prepare<T = unknown, P extends BindParameters = BindParameters>(sql: string): Statement<T, P>;

  /**
   * Execute one or more SQL statements
   * Does not return results; use for DDL and multi-statement scripts
   *
   * @param sql - SQL string (may contain multiple statements)
   * @returns this (for chaining)
   *
   * @example
   * ```typescript
   * db.exec(`
   *   CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
   *   CREATE INDEX idx_name ON users(name);
   * `);
   * ```
   */
  exec(sql: string): this;

  /**
   * Create a transaction wrapper function
   *
   * @param fn - Function to wrap in transaction
   * @returns Transaction-wrapped function
   *
   * @example
   * ```typescript
   * const transfer = db.transaction((from, to, amount) => {
   *   db.prepare('UPDATE accounts SET balance = balance - ? WHERE id = ?').run(amount, from);
   *   db.prepare('UPDATE accounts SET balance = balance + ? WHERE id = ?').run(amount, to);
   * });
   * transfer(1, 2, 100);
   * ```
   */
  transaction<F extends (...args: any[]) => any>(fn: F): TransactionFunction<F>;

  /**
   * Execute a PRAGMA statement
   *
   * @param name - PRAGMA name
   * @param value - Optional value to set
   * @returns PRAGMA result
   *
   * @example
   * ```typescript
   * db.pragma('journal_mode', 'WAL');
   * const mode = db.pragma('journal_mode');
   * ```
   */
  pragma<N extends PragmaName>(name: N, value?: SqlValue): PragmaResult<N>;

  /**
   * Close the database connection
   * Finalizes all prepared statements
   *
   * @returns this
   */
  close(): this;

  /**
   * Begin a savepoint
   *
   * @param name - Savepoint name
   * @returns this
   */
  savepoint(name: string): this;

  /**
   * Release a savepoint
   *
   * @param name - Savepoint name
   * @returns this
   */
  release(name: string): this;

  /**
   * Rollback to a savepoint
   *
   * @param name - Savepoint name
   * @returns this
   */
  rollback(name?: string): this;

  /**
   * Register a user-defined function
   *
   * @param name - Function name
   * @param fn - Function implementation
   * @returns this
   */
  function(name: string, fn: (...args: SqlValue[]) => SqlValue): this;

  /**
   * Register a user-defined aggregate function
   *
   * @template TAccumulator - The type of the accumulator state
   * @param name - Function name
   * @param options - Aggregate options with typed accumulator
   * @returns this
   *
   * @example
   * ```typescript
   * // Simple sum aggregate
   * db.aggregate<number>('mysum', {
   *   start: 0,
   *   step: (acc, val) => acc + Number(val),
   *   result: (acc) => acc,
   * });
   *
   * // Complex aggregate with object state
   * db.aggregate<{ min: number; max: number }>('minmax', {
   *   start: { min: Infinity, max: -Infinity },
   *   step: (acc, val) => {
   *     const n = Number(val);
   *     acc.min = Math.min(acc.min, n);
   *     acc.max = Math.max(acc.max, n);
   *   },
   *   result: (acc) => `${acc.min}-${acc.max}`,
   * });
   * ```
   */
  aggregate<TAccumulator = unknown>(name: string, options: AggregateOptions<TAccumulator>): this;
}

/**
 * Options for user-defined aggregate functions
 *
 * @template TAccumulator - The type of the accumulator state used during aggregation.
 *                          Defaults to `unknown` for backward compatibility.
 *
 * @example
 * ```typescript
 * // Type-safe custom average aggregate
 * interface AvgState {
 *   sum: number;
 *   count: number;
 * }
 *
 * db.aggregate<AvgState>('avg_custom', {
 *   start: { sum: 0, count: 0 },
 *   step: (acc, value) => {
 *     acc.sum += Number(value);
 *     acc.count++;
 *   },
 *   result: (acc) => acc.count > 0 ? acc.sum / acc.count : null,
 * });
 * ```
 */
export interface AggregateOptions<TAccumulator = unknown> {
  /**
   * Called for each row to update the accumulator state
   *
   * @param accumulator - The current accumulator state
   * @param values - The SQL values passed to the aggregate function
   */
  step: (accumulator: TAccumulator, ...values: SqlValue[]) => void;

  /**
   * Called to finalize and return the aggregate result
   *
   * @param accumulator - The final accumulator state
   * @returns The aggregate result as a SQL value
   */
  result: (accumulator: TAccumulator) => SqlValue;

  /**
   * Initial accumulator value. Required when TAccumulator is not undefined.
   *
   * @remarks
   * If not provided, the accumulator starts as `undefined`.
   * For type safety, always provide a `start` value that matches TAccumulator.
   */
  start?: TAccumulator;

  /**
   * Optional inverse function for window functions.
   * Called when a row leaves the window frame.
   *
   * @param accumulator - The current accumulator state
   * @param values - The SQL values to remove from the aggregate
   *
   * @remarks
   * This enables efficient sliding window calculations by "undoing"
   * the effect of rows that exit the window frame.
   */
  inverse?: (accumulator: TAccumulator, ...values: SqlValue[]) => void;
}
