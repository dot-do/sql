/**
 * SQLite OG Benchmark Adapter
 *
 * Original SQLite implementation using sql.js (SQLite compiled to WASM).
 * This is the gold standard baseline for comparing against other storage backends.
 *
 * For Cloudflare Workers, uses cloudflare-worker-sqlite-wasm which is a fork
 * of sql.js optimized for the Workers runtime.
 *
 * @module benchmarks/adapters/sqlite-og
 */

import type {
  BenchmarkAdapter,
  BenchmarkOperation,
  TableSchemaConfig,
  StorageMetrics,
  ColdStartMetrics,
  CrudOperation,
} from '../types.js';

// Detect Workers runtime
const isWorkersRuntime = typeof globalThis.navigator !== 'undefined' &&
  // @ts-ignore - navigator.userAgent exists in Workers
  globalThis.navigator.userAgent?.includes('Cloudflare-Workers');

// =============================================================================
// sql.js Type Definitions
// =============================================================================

/**
 * sql.js Database interface
 */
interface SqlJsDatabase {
  run(sql: string, params?: unknown[]): SqlJsDatabase;
  exec(sql: string): Array<{ columns: string[]; values: unknown[][] }>;
  prepare(sql: string): SqlJsStatement;
  close(): void;
  getRowsModified(): number;
}

/**
 * sql.js Statement interface
 */
interface SqlJsStatement {
  bind(params?: unknown[] | Record<string, unknown>): boolean;
  step(): boolean;
  getAsObject(params?: Record<string, unknown>): Record<string, unknown>;
  get(params?: Record<string, unknown>): unknown[];
  free(): boolean;
  reset(): void;
  run(params?: unknown[]): void;
}

/**
 * sql.js Module interface
 */
interface SqlJsModule {
  Database: new (data?: ArrayLike<number>) => SqlJsDatabase;
}

/**
 * sql.js initialization function type
 */
type InitSqlJs = (options?: SqlJsInitOptions) => Promise<SqlJsModule>;

/**
 * sql.js initialization options
 */
interface SqlJsInitOptions {
  /** Custom WASM file URL */
  locateFile?: (file: string) => string;
  /** Custom WASM instantiation (for Workers) */
  instantiateWasm?: (
    info: WebAssembly.Imports,
    receive: (instance: WebAssembly.Instance) => void
  ) => WebAssembly.Exports;
}

// =============================================================================
// Configuration
// =============================================================================

/**
 * SQLite OG adapter configuration
 */
export interface SQLiteOGAdapterConfig {
  /** URL to load WASM from (for Workers environment) */
  wasmUrl?: string;
  /** Whether to use in-memory database (default: true) */
  inMemory?: boolean;
  /** Custom sql.js factory function (for dependency injection) */
  initSqlJs?: InitSqlJs;
  /** Pre-loaded WASM module (for Workers with bundled WASM) */
  wasmModule?: WebAssembly.Module;
}

// =============================================================================
// Adapter Implementation
// =============================================================================

/**
 * SQLite OG Benchmark Adapter
 *
 * Implements the BenchmarkAdapter interface using sql.js (SQLite WASM).
 * This serves as the gold standard baseline for benchmarking.
 *
 * Features:
 * - Full SQL compatibility (SQLite 3.x)
 * - In-memory database mode for fast operations
 * - WASM-based - runs in Workers and Node.js
 * - Transaction support
 * - Prepared statements with parameter binding
 *
 * @example
 * ```typescript
 * const adapter = new SQLiteOGAdapter();
 * await adapter.initialize();
 *
 * await adapter.createTable({
 *   tableName: 'users',
 *   columns: [
 *     { name: 'id', type: 'INTEGER' },
 *     { name: 'name', type: 'TEXT' },
 *   ],
 *   primaryKey: 'id',
 * });
 *
 * const result = await adapter.insert('users', { id: 1, name: 'Alice' });
 * console.log(result.durationMs);
 * ```
 */
export class SQLiteOGAdapter implements BenchmarkAdapter {
  readonly name = 'sqlite-og';
  readonly version = '1.0.0';

  private db: SqlJsDatabase | null = null;
  private SQL: SqlJsModule | null = null;
  private config: SQLiteOGAdapterConfig;
  private tables: Map<string, TableSchemaConfig> = new Map();
  private initTime = 0;
  private wasmLoadTime = 0;

  constructor(config: SQLiteOGAdapterConfig = {}) {
    this.config = {
      inMemory: true,
      ...config,
    };
  }

  /**
   * Initialize the adapter and prepare for benchmarking.
   * Loads the sql.js WASM module and creates an in-memory database.
   */
  async initialize(): Promise<void> {
    const startTime = performance.now();

    try {
      // Get sql.js factory
      let initSqlJs: InitSqlJs;

      if (this.config.initSqlJs) {
        initSqlJs = this.config.initSqlJs;
      } else {
        // Dynamic import for Node.js/Workers environment
        const module = await import('sql.js');
        initSqlJs = module.default;
      }

      // Configure WASM loading
      const wasmStart = performance.now();
      const options: SqlJsInitOptions = {};

      // Use CDN URL for WASM file - works in both Workers and Node.js
      // Default to the sql.js CDN if no custom URL provided
      const wasmUrl = this.config.wasmUrl || 'https://sql.js.org/dist/sql-wasm.wasm';
      options.locateFile = (file: string) => {
        if (file.endsWith('.wasm')) {
          return wasmUrl;
        }
        return file;
      };

      if (this.config.wasmModule) {
        options.instantiateWasm = (info, receive) => {
          const instance = new WebAssembly.Instance(this.config.wasmModule!, info);
          receive(instance);
          return instance.exports;
        };
      }

      // Initialize sql.js
      this.SQL = await initSqlJs(options);
      this.wasmLoadTime = performance.now() - wasmStart;

      // Create in-memory database
      this.db = new this.SQL.Database();

      // Verify database is working
      const testResult = this.db.exec('SELECT 1 as test');
      if (!testResult || testResult.length === 0) {
        throw new Error('Database verification failed');
      }

      // Enable foreign keys
      this.db.run('PRAGMA foreign_keys = ON');

      this.initTime = performance.now() - startTime;
    } catch (error) {
      throw new Error(
        `Failed to initialize SQLite OG adapter: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Clean up resources after benchmarking.
   */
  async cleanup(): Promise<void> {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
    this.SQL = null;
    this.tables.clear();
  }

  /**
   * Create a table with the given schema.
   */
  async createTable(schema: TableSchemaConfig): Promise<void> {
    this.ensureInitialized();

    const columns = schema.columns.map((col) => {
      let def = `${col.name} ${col.type}`;
      if (col.name === schema.primaryKey) {
        def += ' PRIMARY KEY';
      }
      if (!col.nullable && col.name !== schema.primaryKey) {
        def += ' NOT NULL';
      }
      if (col.defaultValue !== undefined) {
        const value =
          typeof col.defaultValue === 'string'
            ? `'${col.defaultValue}'`
            : col.defaultValue;
        def += ` DEFAULT ${value}`;
      }
      return def;
    });

    const sql = `CREATE TABLE IF NOT EXISTS ${schema.tableName} (${columns.join(', ')})`;
    this.db!.run(sql);

    // Create indexes
    if (schema.indexes) {
      for (const indexCol of schema.indexes) {
        const indexSql = `CREATE INDEX IF NOT EXISTS idx_${schema.tableName}_${indexCol} ON ${schema.tableName}(${indexCol})`;
        this.db!.run(indexSql);
      }
    }

    this.tables.set(schema.tableName, schema);
  }

  /**
   * Drop a table if it exists.
   */
  async dropTable(tableName: string): Promise<void> {
    this.ensureInitialized();
    this.db!.run(`DROP TABLE IF EXISTS ${tableName}`);
    this.tables.delete(tableName);
  }

  /**
   * Insert a single row.
   */
  async insert(
    tableName: string,
    row: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    this.ensureInitialized();

    const startedAt = performance.now();

    try {
      const columns = Object.keys(row);
      const placeholders = columns.map(() => '?').join(', ');
      const values = columns.map((col) => this.serializeValue(row[col]));

      const sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
      this.db!.run(sql, values);

      const rowCount = this.db!.getRowsModified();
      const durationMs = performance.now() - startedAt;

      return {
        type: 'create',
        sql,
        params: row,
        durationMs,
        rowCount,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'create',
        sql: `INSERT INTO ${tableName}`,
        params: row,
        durationMs,
        rowCount: 0,
        success: false,
        error: error instanceof Error ? error.message : String(error),
        startedAt,
      };
    }
  }

  /**
   * Insert multiple rows in a batch.
   */
  async insertBatch(
    tableName: string,
    rows: Record<string, unknown>[]
  ): Promise<BenchmarkOperation> {
    this.ensureInitialized();

    if (rows.length === 0) {
      return {
        type: 'batch',
        sql: `INSERT INTO ${tableName} (batch)`,
        durationMs: 0,
        rowCount: 0,
        success: true,
        startedAt: performance.now(),
      };
    }

    const startedAt = performance.now();

    try {
      const columns = Object.keys(rows[0]);
      const placeholders = columns.map(() => '?').join(', ');
      const sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;

      // Use transaction for batch performance
      this.db!.run('BEGIN TRANSACTION');

      try {
        const stmt = this.db!.prepare(sql);

        for (const row of rows) {
          const values = columns.map((col) => this.serializeValue(row[col]));
          stmt.run(values);
          stmt.reset();
        }

        stmt.free();
        this.db!.run('COMMIT');
      } catch (error) {
        this.db!.run('ROLLBACK');
        throw error;
      }

      const durationMs = performance.now() - startedAt;

      return {
        type: 'batch',
        sql: `INSERT INTO ${tableName} (batch of ${rows.length})`,
        durationMs,
        rowCount: rows.length,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'batch',
        sql: `INSERT INTO ${tableName} (batch)`,
        durationMs,
        rowCount: 0,
        success: false,
        error: error instanceof Error ? error.message : String(error),
        startedAt,
      };
    }
  }

  /**
   * Read a single row by primary key.
   */
  async read(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation> {
    this.ensureInitialized();

    const startedAt = performance.now();
    const sql = `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?`;

    try {
      const stmt = this.db!.prepare(sql);
      stmt.bind([this.serializeValue(primaryKeyValue)]);

      let rowCount = 0;
      while (stmt.step()) {
        rowCount++;
      }
      stmt.free();

      const durationMs = performance.now() - startedAt;

      return {
        type: 'read',
        sql,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'read',
        sql,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: 0,
        success: false,
        error: error instanceof Error ? error.message : String(error),
        startedAt,
      };
    }
  }

  /**
   * Read multiple rows with a WHERE clause.
   */
  async readMany(
    tableName: string,
    whereClause: string,
    params?: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    this.ensureInitialized();

    const startedAt = performance.now();
    const sql = `SELECT * FROM ${tableName} WHERE ${whereClause}`;

    try {
      let rowCount = 0;

      if (params && Object.keys(params).length > 0) {
        // Named parameters - convert to positional
        const orderedParams = this.extractOrderedParams(whereClause, params);
        const stmt = this.db!.prepare(sql.replace(/:\w+/g, '?'));
        stmt.bind(orderedParams.map((p) => this.serializeValue(p)));

        while (stmt.step()) {
          rowCount++;
        }
        stmt.free();
      } else {
        const result = this.db!.exec(sql);
        rowCount = result[0]?.values.length ?? 0;
      }

      const durationMs = performance.now() - startedAt;

      return {
        type: 'read',
        sql,
        params,
        durationMs,
        rowCount,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'read',
        sql,
        params,
        durationMs,
        rowCount: 0,
        success: false,
        error: error instanceof Error ? error.message : String(error),
        startedAt,
      };
    }
  }

  /**
   * Update a single row.
   */
  async update(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown,
    updates: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    this.ensureInitialized();

    const startedAt = performance.now();

    try {
      const setClauses = Object.keys(updates)
        .map((col) => `${col} = ?`)
        .join(', ');
      const values = [
        ...Object.values(updates).map((v) => this.serializeValue(v)),
        this.serializeValue(primaryKeyValue),
      ];

      const sql = `UPDATE ${tableName} SET ${setClauses} WHERE ${primaryKey} = ?`;
      this.db!.run(sql, values);

      const rowCount = this.db!.getRowsModified();
      const durationMs = performance.now() - startedAt;

      return {
        type: 'update',
        sql,
        params: { ...updates, [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'update',
        sql: `UPDATE ${tableName}`,
        params: updates,
        durationMs,
        rowCount: 0,
        success: false,
        error: error instanceof Error ? error.message : String(error),
        startedAt,
      };
    }
  }

  /**
   * Delete a single row.
   */
  async delete(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation> {
    this.ensureInitialized();

    const startedAt = performance.now();
    const sql = `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`;

    try {
      this.db!.run(sql, [this.serializeValue(primaryKeyValue)]);
      const rowCount = this.db!.getRowsModified();
      const durationMs = performance.now() - startedAt;

      return {
        type: 'delete',
        sql,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'delete',
        sql,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: 0,
        success: false,
        error: error instanceof Error ? error.message : String(error),
        startedAt,
      };
    }
  }

  /**
   * Execute a raw SQL query.
   */
  async query(
    sql: string,
    params?: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    this.ensureInitialized();

    const startedAt = performance.now();

    try {
      let rowCount = 0;
      const operationType = this.detectOperationType(sql);

      if (params && Object.keys(params).length > 0) {
        const orderedParams = this.extractOrderedParams(sql, params);
        const normalizedSql = sql.replace(/:\w+/g, '?');

        if (sql.trim().toUpperCase().startsWith('SELECT')) {
          const stmt = this.db!.prepare(normalizedSql);
          stmt.bind(orderedParams.map((p) => this.serializeValue(p)));

          while (stmt.step()) {
            rowCount++;
          }
          stmt.free();
        } else {
          this.db!.run(normalizedSql, orderedParams.map((p) => this.serializeValue(p)));
          rowCount = this.db!.getRowsModified();
        }
      } else {
        if (sql.trim().toUpperCase().startsWith('SELECT')) {
          const result = this.db!.exec(sql);
          rowCount = result[0]?.values.length ?? 0;
        } else {
          this.db!.run(sql);
          rowCount = this.db!.getRowsModified();
        }
      }

      const durationMs = performance.now() - startedAt;

      return {
        type: operationType,
        sql,
        params,
        durationMs,
        rowCount,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'query',
        sql,
        params,
        durationMs,
        rowCount: 0,
        success: false,
        error: error instanceof Error ? error.message : String(error),
        startedAt,
      };
    }
  }

  /**
   * Execute multiple operations in a transaction.
   */
  async transaction(
    operations: Array<{
      type: 'insert' | 'update' | 'delete';
      tableName: string;
      data: Record<string, unknown>;
    }>
  ): Promise<BenchmarkOperation> {
    this.ensureInitialized();

    const startedAt = performance.now();
    let totalRowCount = 0;

    try {
      this.db!.run('BEGIN TRANSACTION');

      for (const op of operations) {
        const schema = this.tables.get(op.tableName);
        const primaryKey = schema?.primaryKey ?? 'id';
        let result: BenchmarkOperation;

        switch (op.type) {
          case 'insert': {
            result = await this.insert(op.tableName, op.data);
            break;
          }
          case 'update': {
            const pkValue = op.data[primaryKey];
            const updates = { ...op.data };
            delete updates[primaryKey];
            result = await this.update(op.tableName, primaryKey, pkValue, updates);
            break;
          }
          case 'delete': {
            const pkValue = op.data[primaryKey];
            result = await this.delete(op.tableName, primaryKey, pkValue);
            break;
          }
        }

        // Check if operation failed and trigger rollback
        if (!result!.success) {
          throw new Error(result!.error || `${op.type} operation failed`);
        }

        totalRowCount += result!.rowCount;
      }

      this.db!.run('COMMIT');

      const durationMs = performance.now() - startedAt;

      return {
        type: 'transaction',
        sql: `TRANSACTION (${operations.length} operations)`,
        durationMs,
        rowCount: totalRowCount,
        success: true,
        startedAt,
      };
    } catch (error) {
      this.db!.run('ROLLBACK');
      const durationMs = performance.now() - startedAt;

      return {
        type: 'transaction',
        sql: `TRANSACTION (${operations.length} operations)`,
        durationMs,
        rowCount: 0,
        success: false,
        error: error instanceof Error ? error.message : String(error),
        startedAt,
      };
    }
  }

  /**
   * Get current storage metrics.
   */
  async getStorageMetrics(): Promise<StorageMetrics> {
    this.ensureInitialized();

    try {
      // Get database size using SQLite pragmas
      const pageCountResult = this.db!.exec('PRAGMA page_count');
      const pageCount = Number(pageCountResult[0]?.values[0]?.[0] ?? 0);

      const pageSizeResult = this.db!.exec('PRAGMA page_size');
      const pageSize = Number(pageSizeResult[0]?.values[0]?.[0] ?? 4096);

      const totalBytes = pageCount * pageSize;

      // Count rows across all tables
      let rowCount = 0;
      for (const tableName of this.tables.keys()) {
        try {
          const result = this.db!.exec(`SELECT COUNT(*) FROM ${tableName}`);
          rowCount += Number(result[0]?.values[0]?.[0] ?? 0);
        } catch {
          // Table might not exist
        }
      }

      // 128MB is the DO SQLite limit
      const maxSize = 128 * 1024 * 1024;
      const limitUtilization = (totalBytes / maxSize) * 100;

      return {
        totalBytes,
        rowCount,
        tableCount: this.tables.size,
        limitUtilization,
      };
    } catch {
      return {
        totalBytes: 0,
        rowCount: 0,
        tableCount: this.tables.size,
        limitUtilization: 0,
      };
    }
  }

  /**
   * Measure cold start time.
   * Returns time to first successful query after fresh initialization.
   */
  async measureColdStart(): Promise<ColdStartMetrics> {
    // Save current state
    const currentDb = this.db;
    const currentSQL = this.SQL;

    // Force re-initialization
    this.db = null;
    this.SQL = null;

    const startTime = performance.now();

    try {
      await this.initialize();
      const initializationTime = performance.now() - startTime;

      // Time to first query
      const queryStart = performance.now();
      this.db!.exec('SELECT 1');
      const timeToFirstQuery = performance.now() - startTime;
      const connectionTime = performance.now() - queryStart;

      return {
        timeToFirstQuery,
        initializationTime,
        connectionTime,
      };
    } finally {
      // Restore state if we had a previous database
      // (For cold start measurement, we typically use the fresh one)
    }
  }

  // ===========================================================================
  // Private Helper Methods
  // ===========================================================================

  /**
   * Ensure the adapter is initialized before operations.
   */
  private ensureInitialized(): void {
    if (!this.db || !this.SQL) {
      throw new Error('SQLite OG adapter not initialized. Call initialize() first.');
    }
  }

  /**
   * Serialize a value for SQLite storage.
   */
  private serializeValue(value: unknown): unknown {
    if (value === null || value === undefined) {
      return null;
    }
    if (value instanceof Date) {
      return value.getTime();
    }
    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value);
    }
    if (ArrayBuffer.isView(value)) {
      return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    }
    if (typeof value === 'boolean') {
      return value ? 1 : 0;
    }
    if (typeof value === 'object') {
      return JSON.stringify(value);
    }
    return value;
  }

  /**
   * Extract ordered parameters from named params based on SQL placeholders.
   */
  private extractOrderedParams(
    sql: string,
    params: Record<string, unknown>
  ): unknown[] {
    const placeholders = sql.match(/:\w+/g) ?? [];
    return placeholders.map((p) => {
      const name = p.slice(1); // Remove leading ':'
      return params[name];
    });
  }

  /**
   * Detect the operation type from an SQL statement.
   */
  private detectOperationType(
    sql: string
  ): CrudOperation | 'batch' | 'transaction' | 'query' {
    const normalized = sql.trim().toUpperCase();
    if (normalized.startsWith('SELECT')) return 'read';
    if (normalized.startsWith('INSERT')) return 'create';
    if (normalized.startsWith('UPDATE')) return 'update';
    if (normalized.startsWith('DELETE')) return 'delete';
    return 'query';
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a SQLite OG adapter with default configuration.
 *
 * @example
 * ```typescript
 * const adapter = createSQLiteOGAdapter();
 * await adapter.initialize();
 * ```
 */
export function createSQLiteOGAdapter(
  config?: SQLiteOGAdapterConfig
): SQLiteOGAdapter {
  return new SQLiteOGAdapter(config);
}

/**
 * Create a SQLite OG adapter for Cloudflare Workers environment.
 * Uses custom WASM instantiation compatible with Workers.
 *
 * @param wasmModule - Pre-compiled WASM module
 * @param initSqlJs - sql.js initialization function
 *
 * @example
 * ```typescript
 * import wasm from 'cloudflare-worker-sqlite-wasm/dist/sql-cf-wasm.wasm';
 * import initSqlJs from 'cloudflare-worker-sqlite-wasm/dist/sql-cf-wasm.js';
 *
 * const adapter = createWorkersAdapter(wasm, initSqlJs);
 * await adapter.initialize();
 * ```
 */
export function createWorkersAdapter(
  wasmModule: WebAssembly.Module,
  initSqlJs: InitSqlJs
): SQLiteOGAdapter {
  return new SQLiteOGAdapter({
    wasmModule,
    initSqlJs,
  });
}
