/**
 * Database Class for DoSQL
 *
 * Provides a better-sqlite3/D1 compatible database API with
 * prepared statements, transactions, and pragma support.
 */

import type {
  Database as IDatabase,
  Statement,
  SqlValue,
  BindParameters,
  RunResult,
  DatabaseOptions,
  TransactionMode,
  TransactionFunction,
  PragmaName,
  PragmaResult,
  AggregateOptions,
} from './statement/types.js';
import {
  PreparedStatement,
  InMemoryEngine,
  createInMemoryStorage,
  type ExecutionEngine,
  type InMemoryStorage,
} from './statement/statement.js';
import { StatementCache, type CacheOptions } from './statement/cache.js';
import { parseParameters } from './statement/binding.js';
import { tokenizeSQL } from './database/tokenizer.js';
import {
  DatabaseError,
  DatabaseErrorCode,
  createClosedDatabaseError,
  createSavepointNotFoundError,
} from './errors/index.js';

// Re-export DatabaseError for backwards compatibility
export { DatabaseError, DatabaseErrorCode } from './errors/index.js';

// =============================================================================
// USER-DEFINED FUNCTIONS
// =============================================================================

/**
 * Registry of user-defined functions
 */
interface FunctionRegistry {
  scalar: Map<string, (...args: SqlValue[]) => SqlValue>;
  /** Stores aggregate options with unknown accumulator type (heterogeneous storage) */
  aggregate: Map<string, AggregateOptions<unknown>>;
}

// =============================================================================
// DATABASE IMPLEMENTATION
// =============================================================================

/**
 * DoSQL Database class
 *
 * Provides a better-sqlite3/D1 compatible API for SQL operations.
 *
 * @example
 * ```typescript
 * const db = new Database();
 *
 * // Create a table
 * db.exec(`
 *   CREATE TABLE users (
 *     id INTEGER PRIMARY KEY,
 *     name TEXT NOT NULL,
 *     email TEXT UNIQUE
 *   )
 * `);
 *
 * // Insert data
 * const insert = db.prepare('INSERT INTO users (name, email) VALUES (?, ?)');
 * insert.run('Alice', 'alice@example.com');
 *
 * // Query data
 * const users = db.prepare('SELECT * FROM users').all();
 * ```
 */
export class Database implements IDatabase {
  /** Database filename */
  readonly name: string;

  /** Whether database is read-only */
  readonly readonly: boolean;

  /** Execution engine */
  private engine: ExecutionEngine;

  /** Storage (for in-memory databases) */
  private storage: InMemoryStorage;

  /** Statement cache */
  private cache: StatementCache;

  /** User-defined functions */
  private functions: FunctionRegistry;

  /** Active savepoints */
  private savepoints: string[] = [];

  /** Whether database is open */
  private _open = true;

  /** Whether in a transaction */
  private _inTransaction = false;

  /** Options */
  private options: DatabaseOptions;

  /**
   * Create a new database instance
   *
   * @param filename - Database filename (use ':memory:' for in-memory)
   * @param options - Database options
   */
  constructor(filename = ':memory:', options: DatabaseOptions = {}) {
    this.name = filename;
    this.options = options;
    this.readonly = options.readonly ?? false;

    // Initialize storage and engine
    this.storage = createInMemoryStorage();
    this.engine = new InMemoryEngine(this.storage);

    // Initialize cache
    const cacheOptions: CacheOptions = {
      maxSize: options.statementCacheSize ?? 100,
    };
    this.cache = new StatementCache(cacheOptions);

    // Initialize function registry
    this.functions = {
      scalar: new Map(),
      aggregate: new Map(),
    };

    // Log if verbose
    if (options.verbose) {
      options.verbose('Database opened:', filename);
    }
  }

  /**
   * Whether the database is open
   */
  get open(): boolean {
    return this._open;
  }

  /**
   * Whether currently in a transaction
   */
  get inTransaction(): boolean {
    return this._inTransaction;
  }

  /**
   * Ensure database is open
   */
  private checkOpen(): void {
    if (!this._open) {
      throw createClosedDatabaseError(this.name);
    }
  }

  /**
   * Ensure database is writable
   */
  private checkWritable(): void {
    this.checkOpen();
    if (this.readonly) {
      throw new DatabaseError(DatabaseErrorCode.READ_ONLY, 'Database is read-only');
    }
  }

  // ==========================================================================
  // STATEMENT PREPARATION
  // ==========================================================================

  /**
   * Prepare a SQL statement
   *
   * @param sql - SQL string
   * @returns Prepared statement
   */
  prepare<T = unknown, P extends BindParameters = BindParameters>(
    sql: string
  ): Statement<T, P> {
    this.checkOpen();

    // Check cache
    const cached = this.cache.get(sql);
    if (cached) {
      // Return a fresh statement that shares the same parsed info
      return new PreparedStatement<T, P>(sql, this.engine);
    }

    // Parse and create new statement
    const parsed = parseParameters(sql);
    const statement = new PreparedStatement<T, P>(sql, this.engine);

    // Cache it
    this.cache.set(sql, statement as Statement<unknown>, parsed);

    // Log if verbose
    if (this.options.verbose) {
      this.options.verbose('Prepared:', sql);
    }

    return statement;
  }

  // ==========================================================================
  // EXEC
  // ==========================================================================

  /**
   * Execute one or more SQL statements
   *
   * Uses a state-aware SQL tokenizer to properly split statements,
   * handling semicolons inside string literals, comments, and identifiers.
   *
   * @param sql - SQL string (may contain multiple statements)
   * @returns this (for chaining)
   */
  exec(sql: string): this {
    this.checkOpen();

    // Use state-aware tokenizer to properly split statements
    // This handles semicolons inside strings, comments, and identifiers
    const statements = tokenizeSQL(sql);

    for (const stmt of statements) {
      this.checkWritable();
      const prepared = this.prepare(stmt);
      prepared.run();
    }

    // Log if verbose
    if (this.options.verbose) {
      this.options.verbose('Executed:', sql.slice(0, 100) + '...');
    }

    return this;
  }

  // ==========================================================================
  // TRANSACTIONS
  // ==========================================================================

  /**
   * Create a transaction wrapper function
   *
   * @param fn - Function to wrap in transaction
   * @returns Transaction-wrapped function
   */
  transaction<F extends (...args: any[]) => any>(fn: F): TransactionFunction<F> {
    const self = this;

    const runTransaction = (mode: TransactionMode, ...args: Parameters<F>): ReturnType<F> => {
      self.checkWritable();

      const beginSql = mode === 'immediate'
        ? 'BEGIN IMMEDIATE'
        : mode === 'exclusive'
          ? 'BEGIN EXCLUSIVE'
          : 'BEGIN DEFERRED';

      try {
        // Note: In-memory engine doesn't actually implement transactions,
        // but we track the state for API compatibility
        self._inTransaction = true;

        if (self.options.verbose) {
          self.options.verbose('Transaction:', beginSql);
        }

        const result = fn.apply(self, args);

        if (self.options.verbose) {
          self.options.verbose('Transaction: COMMIT');
        }

        return result;
      } catch (error) {
        if (self.options.verbose) {
          self.options.verbose('Transaction: ROLLBACK');
        }
        throw error;
      } finally {
        self._inTransaction = false;
      }
    };

    const transactionFn = ((...args: Parameters<F>) => {
      return runTransaction('deferred', ...args);
    }) as TransactionFunction<F>;

    transactionFn.deferred = (...args: Parameters<F>) => {
      return runTransaction('deferred', ...args);
    };

    transactionFn.immediate = (...args: Parameters<F>) => {
      return runTransaction('immediate', ...args);
    };

    transactionFn.exclusive = (...args: Parameters<F>) => {
      return runTransaction('exclusive', ...args);
    };

    return transactionFn;
  }

  // ==========================================================================
  // SAVEPOINTS
  // ==========================================================================

  /**
   * Begin a savepoint
   *
   * @param name - Savepoint name
   * @returns this
   */
  savepoint(name: string): this {
    this.checkWritable();
    this.savepoints.push(name);

    if (this.options.verbose) {
      this.options.verbose('Savepoint:', name);
    }

    return this;
  }

  /**
   * Release a savepoint
   *
   * @param name - Savepoint name
   * @returns this
   */
  release(name: string): this {
    this.checkWritable();

    const index = this.savepoints.lastIndexOf(name);
    if (index === -1) {
      throw createSavepointNotFoundError(name);
    }

    this.savepoints.splice(index, 1);

    if (this.options.verbose) {
      this.options.verbose('Release:', name);
    }

    return this;
  }

  /**
   * Rollback to a savepoint (or the transaction)
   *
   * @param name - Savepoint name (optional)
   * @returns this
   */
  rollback(name?: string): this {
    this.checkOpen();

    if (name) {
      const index = this.savepoints.lastIndexOf(name);
      if (index === -1) {
        throw createSavepointNotFoundError(name);
      }
      this.savepoints.splice(index);
    }

    if (this.options.verbose) {
      this.options.verbose('Rollback:', name ?? 'transaction');
    }

    return this;
  }

  // ==========================================================================
  // PRAGMA
  // ==========================================================================

  /**
   * Execute a PRAGMA statement
   *
   * @param name - PRAGMA name
   * @param value - Optional value to set
   * @returns PRAGMA result
   */
  pragma<N extends PragmaName>(name: N, value?: SqlValue): PragmaResult<N> {
    this.checkOpen();

    // For read-only pragmas, allow even in readonly mode
    const readOnlyPragmas = [
      'journal_mode',
      'synchronous',
      'foreign_keys',
      'cache_size',
      'page_size',
      'busy_timeout',
      'user_version',
      'application_id',
      'integrity_check',
      'quick_check',
      'table_info',
      'index_list',
      'database_list',
      'compile_options',
    ];

    if (value !== undefined && !readOnlyPragmas.includes(name)) {
      this.checkWritable();
    }

    // Simple pragma handling for in-memory database
    const pragmaValues: Record<string, unknown> = {
      journal_mode: 'memory',
      synchronous: 2,
      foreign_keys: 1,
      cache_size: -2000,
      page_size: 4096,
      busy_timeout: this.options.timeout ?? 5000,
      user_version: 0,
      application_id: 0,
      integrity_check: ['ok'],
      quick_check: ['ok'],
      table_info: [],
      index_list: [],
      database_list: [{ seq: 0, name: 'main', file: this.name }],
      compile_options: ['ENABLE_FTS5', 'ENABLE_JSON1'],
    };

    if (this.options.verbose) {
      this.options.verbose('Pragma:', name, value);
    }

    // Handle table_info specially
    if (name === 'table_info' && value !== undefined) {
      const tableName = String(value);
      const table = this.storage.tables.get(tableName);
      if (table) {
        return table.columns.map((col, i) => ({
          cid: i,
          name: col.name,
          type: col.type ?? 'TEXT',
          notnull: 0,
          dflt_value: null,
          pk: col.name.toLowerCase() === 'id' ? 1 : 0,
        })) as PragmaResult<N>;
      }
      return [] as PragmaResult<N>;
    }

    return (pragmaValues[name] ?? null) as PragmaResult<N>;
  }

  // ==========================================================================
  // USER-DEFINED FUNCTIONS
  // ==========================================================================

  /**
   * Register a user-defined scalar function
   *
   * @param name - Function name
   * @param fn - Function implementation
   * @returns this
   */
  function(name: string, fn: (...args: SqlValue[]) => SqlValue): this {
    this.checkOpen();
    this.functions.scalar.set(name.toLowerCase(), fn);

    if (this.options.verbose) {
      this.options.verbose('Function registered:', name);
    }

    return this;
  }

  /**
   * Register a user-defined aggregate function
   *
   * @template TAccumulator - The type of the accumulator state
   * @param name - Function name
   * @param options - Aggregate options with typed accumulator
   * @returns this
   */
  aggregate<TAccumulator = unknown>(name: string, options: AggregateOptions<TAccumulator>): this {
    this.checkOpen();
    // Store as AggregateOptions<unknown> since the registry holds heterogeneous types
    this.functions.aggregate.set(name.toLowerCase(), options as AggregateOptions<unknown>);

    if (this.options.verbose) {
      this.options.verbose('Aggregate registered:', name);
    }

    return this;
  }

  // ==========================================================================
  // CLOSE
  // ==========================================================================

  /**
   * Close the database connection
   *
   * @returns this
   */
  close(): this {
    if (!this._open) {
      return this;
    }

    // Clear the statement cache (finalizes all statements)
    this.cache.clear();

    // Clear function registries
    this.functions.scalar.clear();
    this.functions.aggregate.clear();

    this._open = false;

    if (this.options.verbose) {
      this.options.verbose('Database closed:', this.name);
    }

    return this;
  }

  // ==========================================================================
  // UTILITY METHODS
  // ==========================================================================

  /**
   * Get cache statistics
   */
  getCacheStats() {
    return this.cache.getStats();
  }

  /**
   * Get list of tables
   */
  getTables(): string[] {
    return Array.from(this.storage.tables.keys());
  }

  /**
   * Check if a table exists
   */
  hasTable(name: string): boolean {
    return this.storage.tables.has(name);
  }
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a new database instance
 *
 * @param filename - Database filename (default: ':memory:')
 * @param options - Database options
 * @returns Database instance
 *
 * @example
 * ```typescript
 * const db = createDatabase();
 * db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
 * ```
 */
export function createDatabase(
  filename = ':memory:',
  options: DatabaseOptions = {}
): Database {
  return new Database(filename, options);
}

// =============================================================================
// RE-EXPORTS
// =============================================================================

export type {
  Statement,
  SqlValue,
  BindParameters,
  RunResult,
  ColumnInfo,
  DatabaseOptions,
  TransactionMode,
  TransactionFunction,
  PragmaName,
  PragmaResult,
  AggregateOptions,
  NamedParameters,
  PositionalParameters,
  StatementOptions,
} from './statement/types.js';

export {
  parseParameters,
  bindParameters,
  coerceValue,
  BindingError,
  type ParsedParameters,
  type ParameterToken,
} from './statement/binding.js';

export {
  StatementCache,
  createStatementCache,
  hashString,
  type CacheEntry,
  type CacheOptions,
  type CacheStats,
} from './statement/cache.js';

export {
  PreparedStatement,
  InMemoryEngine,
  createInMemoryStorage,
  createStatement,
  StatementError,
  type ExecutionEngine,
  type ExecutionResult,
  type InMemoryStorage,
  type InMemoryTable,
} from './statement/statement.js';
