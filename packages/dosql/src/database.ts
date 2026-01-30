/**
 * Database Class for DoSQL
 *
 * Provides a better-sqlite3/D1 compatible database API with
 * prepared statements, transactions, and pragma support.
 */

import { TIMEOUTS, SIZE_LIMITS, PRAGMA_DEFAULTS } from './constants.js';
import {
  QueryPlanCacheImpl,
  type PlanCacheStats,
  computePlanCacheKey,
  normalizeQueryForCache,
} from './planner/cache.js';

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
  private readonly engine: ExecutionEngine;

  /** Storage (for in-memory databases) */
  private readonly storage: InMemoryStorage;

  /** Statement cache */
  private readonly cache: StatementCache;

  /** User-defined functions */
  private readonly functions: FunctionRegistry;

  /** Query plan cache */
  private readonly planCache: QueryPlanCacheImpl;

  /** Plan cache configuration */
  private planCacheEnabled = true;

  /** Plan cache hit rate threshold */
  private hitRateThreshold = 0.80;

  /** Active savepoints */
  private savepoints: string[] = [];

  /** Whether database is open */
  private _open = true;

  /** Whether in a transaction */
  private _inTransaction = false;

  /** Options */
  private readonly options: DatabaseOptions;

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
      maxSize: options.statementCacheSize ?? SIZE_LIMITS.DEFAULT_STATEMENT_CACHE_SIZE,
    };
    this.cache = new StatementCache(cacheOptions);

    // Initialize query plan cache
    const planCacheConfig = (options as Record<string, unknown>).planCache as
      | { enabled?: boolean; maxSize?: number }
      | undefined;
    this.planCache = new QueryPlanCacheImpl({
      maxSize: planCacheConfig?.maxSize ?? 1000,
      enabled: planCacheConfig?.enabled ?? true,
    });
    this.planCacheEnabled = planCacheConfig?.enabled ?? true;

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
   * For PRAGMA queries related to the query plan cache, returns an array of
   * result objects. For other statements, returns this for chaining.
   *
   * @param sql - SQL string (may contain multiple statements)
   * @returns Result array for PRAGMA queries, or this for chaining
   */
  exec(sql: string): any {
    this.checkOpen();

    // Check for PRAGMA query plan cache commands
    const pragmaResult = this.handlePlanCachePragma(sql);
    if (pragmaResult !== undefined) {
      return pragmaResult;
    }

    // Use state-aware tokenizer to properly split statements
    // This handles semicolons inside strings, comments, and identifiers
    const statements = tokenizeSQL(sql);

    for (const stmt of statements) {
      // Cache the query plan for SELECT statements
      const trimmed = stmt.trim().toUpperCase();
      if (trimmed.startsWith('SELECT') && this.planCacheEnabled) {
        const normalized = normalizeQueryForCache(stmt);
        const hash = computePlanCacheKey(normalized);
        const cached = this.planCache.get(hash);
        if (!cached) {
          // Cache miss - execute and cache the plan
          this.checkWritable();
          const prepared = this.prepare(stmt);
          prepared.run();
          this.planCache.set(hash, { type: 'plan', sql: normalized }, 1);
        } else {
          // Cache hit - still execute
          this.checkWritable();
          const prepared = this.prepare(stmt);
          prepared.run();
        }
      } else {
        this.checkWritable();
        const prepared = this.prepare(stmt);
        prepared.run();
      }
    }

    // Log if verbose
    if (this.options.verbose) {
      this.options.verbose('Executed:', sql.slice(0, 100) + '...');
    }

    return this;
  }

  /**
   * Explain a query plan
   *
   * @param sql - SQL query to explain
   * @returns String representation of the query plan
   */
  explain(sql: string): string {
    this.checkOpen();

    // Check if there's a cached plan
    const normalized = normalizeQueryForCache(sql);
    const hash = computePlanCacheKey(normalized);
    const cached = this.planCache.get(hash);

    if (cached) {
      const plan = cached.plan as Record<string, unknown>;
      return JSON.stringify(plan);
    }

    // Generate a basic plan description
    const trimmed = sql.trim().toUpperCase();
    if (trimmed.includes('WHERE')) {
      // Check for index usage
      const tables = this.extractTableNames(sql);
      for (const tableName of tables) {
        const indexes = this.storage.indexes;
        for (const index of indexes.values()) {
          if (index.tableName === tableName) {
            // Check if the WHERE clause references indexed columns
            const whereClause = sql.substring(sql.toUpperCase().indexOf('WHERE'));
            for (const col of index.columns) {
              if (whereClause.toLowerCase().includes(col.name.toLowerCase())) {
                const plan = `IndexScan using ${index.name} on ${tableName}`;
                this.planCache.set(hash, { type: 'IndexScan', index: index.name, table: tableName }, 1);
                return plan;
              }
            }
          }
        }
      }
    }

    return 'SeqScan';
  }

  /**
   * Extract table names from SQL
   */
  private extractTableNames(sql: string): string[] {
    const tables: string[] = [];
    const fromMatch = sql.match(/FROM\s+(\w+)/i);
    if (fromMatch) {
      tables.push(fromMatch[1]);
    }
    const joinMatches = sql.matchAll(/JOIN\s+(\w+)/gi);
    for (const match of joinMatches) {
      tables.push(match[1]);
    }
    return tables;
  }

  /**
   * Handle PRAGMA query_plan_cache_* commands
   *
   * @returns Result array for cache PRAGMAs, or undefined if not a cache PRAGMA
   */
  private handlePlanCachePragma(sql: string): unknown[] | undefined {
    const trimmed = sql.trim();
    const upper = trimmed.toUpperCase();

    if (!upper.startsWith('PRAGMA QUERY_PLAN_CACHE')) {
      // Also check for other cache pragmas
      if (!upper.startsWith('PRAGMA QUERY_PLAN_CACHE')) {
        return undefined;
      }
    }

    // PRAGMA query_plan_cache_stats
    if (upper === 'PRAGMA QUERY_PLAN_CACHE_STATS') {
      const stats = this.planCache.getStats();
      return [{
        hits: stats.hits,
        misses: stats.misses,
        size: stats.size,
        maxSize: stats.maxSize,
        hitRate: stats.hitRate,
        evictions: stats.evictions,
        memoryUsage: stats.memoryUsage,
        enabled: this.planCacheEnabled,
      }];
    }

    // PRAGMA query_plan_cache_list
    if (upper === 'PRAGMA QUERY_PLAN_CACHE_LIST') {
      return this.planCache.list();
    }

    // PRAGMA query_plan_cache_clear
    if (upper === 'PRAGMA QUERY_PLAN_CACHE_CLEAR') {
      this.planCache.clear();
      return [];
    }

    // PRAGMA query_plan_cache_max_size = N
    const maxSizeMatch = trimmed.match(/PRAGMA\s+query_plan_cache_max_size\s*=\s*(\d+)/i);
    if (maxSizeMatch) {
      const newMaxSize = parseInt(maxSizeMatch[1], 10);
      // Recreate the cache with new max size is complex, so we update the config
      // by clearing and re-creating. For simplicity, expose via a setter approach.
      (this.planCache as any).config.maxSize = newMaxSize;
      return [];
    }

    // PRAGMA query_plan_cache_enabled = true/false
    const enabledMatch = trimmed.match(/PRAGMA\s+query_plan_cache_enabled\s*=\s*(true|false)/i);
    if (enabledMatch) {
      this.planCacheEnabled = enabledMatch[1].toLowerCase() === 'true';
      (this.planCache as any).config.enabled = this.planCacheEnabled;
      return [];
    }

    // PRAGMA query_plan_cache_show('hash')
    const showMatch = trimmed.match(/PRAGMA\s+query_plan_cache_show\s*\(\s*'([^']+)'\s*\)/i);
    if (showMatch) {
      const hash = showMatch[1];
      const planInfo = this.planCache.show(hash);
      if (planInfo) {
        return [planInfo];
      }
      return [];
    }

    // PRAGMA query_plan_cache_hit_rate_threshold = N
    const thresholdMatch = trimmed.match(/PRAGMA\s+query_plan_cache_hit_rate_threshold\s*=\s*([\d.]+)/i);
    if (thresholdMatch) {
      this.hitRateThreshold = parseFloat(thresholdMatch[1]);
      return [];
    }

    // PRAGMA query_plan_cache_health
    if (upper === 'PRAGMA QUERY_PLAN_CACHE_HEALTH') {
      const stats = this.planCache.getStats();
      const hitRate = stats.hitRate / 100; // Convert from percentage to ratio
      const belowThreshold = hitRate < this.hitRateThreshold;
      return [{
        status: belowThreshold ? 'warning' : 'ok',
        hitRate,
        threshold: this.hitRateThreshold,
        recommendation: belowThreshold
          ? 'Hit rate is below threshold. Consider increasing cache size or reviewing query patterns.'
          : 'Cache is performing well.',
      }];
    }

    return undefined;
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
      synchronous: PRAGMA_DEFAULTS.SYNCHRONOUS,
      foreign_keys: PRAGMA_DEFAULTS.FOREIGN_KEYS,
      cache_size: PRAGMA_DEFAULTS.CACHE_SIZE,
      page_size: PRAGMA_DEFAULTS.PAGE_SIZE,
      busy_timeout: this.options.timeout ?? TIMEOUTS.DEFAULT_BUSY_TIMEOUT_MS,
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

    // Handle index_list specially
    if (name === 'index_list' && value !== undefined) {
      const tableName = String(value);
      const indexes: { seq: number; name: string; unique: number; origin: string; partial: number }[] = [];
      let seq = 0;

      for (const index of this.storage.indexes.values()) {
        if (index.tableName === tableName) {
          indexes.push({
            seq: seq++,
            name: index.name,
            unique: index.unique ? 1 : 0,
            origin: 'c', // 'c' for created via CREATE INDEX
            partial: 0,
          });
        }
      }

      return indexes as PragmaResult<N>;
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
