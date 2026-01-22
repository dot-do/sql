/**
 * DO SQLite Benchmark Adapter
 *
 * Implements the BenchmarkAdapter interface for Durable Object SQLite storage.
 * Tracks read/write operations for accurate billing estimation.
 *
 * Uses ctx.storage.sql API directly - the native SQLite interface for Durable Objects.
 * See: https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/
 *
 * IMPORTANT: DO SQLite does NOT support explicit transaction statements like
 * BEGIN TRANSACTION or COMMIT. Use ctx.storage.transactionSync() instead.
 * See: https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#transactions
 */

import {
  type BenchmarkAdapter,
  type BenchmarkOperation,
  type TableSchemaConfig,
  type StorageMetrics,
  type ColdStartMetrics,
  type DOBillingMetrics,
  estimateDOBilling,
  DO_SQLITE_PRICING,
} from '../types.js';

// =============================================================================
// Types
// =============================================================================

/**
 * SqlStorage interface matching DO's sql API
 * See: https://developers.cloudflare.com/durable-objects/api/sqlite-api/
 */
export interface SqlStorage {
  exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): SqlStorageCursor<T>;
  databaseSize: number;
}

export interface SqlStorageCursor<T = Record<string, unknown>> {
  toArray(): T[];
  one(): T | null;
  raw<R extends unknown[] = unknown[]>(): IterableIterator<R>;
  columnNames: string[];
  rowsRead: number;
  rowsWritten: number;
}

/**
 * DurableObjectStorage interface for transactionSync support
 */
export interface DOStorage {
  sql: SqlStorage;
  transactionSync<T>(closure: () => T): T;
}

/**
 * Configuration for the DO SQLite adapter
 */
export interface DOSqliteAdapterConfig {
  /** The full storage object from DurableObjectState */
  storage: DOStorage;
}

/**
 * Result of a single statement in execMany batch
 */
export interface ExecManyResult {
  /** Whether the statement executed successfully */
  success: boolean;
  /** Number of rows affected (INSERT/UPDATE/DELETE) or returned (SELECT) */
  rowCount: number;
  /** Rows returned for SELECT statements */
  rows?: Record<string, unknown>[];
  /** Error message if execution failed */
  error?: string;
  /** Execution time in milliseconds */
  durationMs: number;
}

// =============================================================================
// DO SQLite Adapter Implementation
// =============================================================================

/**
 * Benchmark adapter for Durable Object SQLite storage
 *
 * This adapter:
 * - Uses the native ctx.storage.sql API
 * - Uses ctx.storage.transactionSync() for atomic operations
 * - Tracks row read/write operations for billing estimation
 * - Handles the 128MB storage limit gracefully
 * - Provides accurate per-row billing metrics
 */
export class DOSqliteAdapter implements BenchmarkAdapter {
  readonly name = 'do-sqlite';
  readonly version = '1.0.0';

  private storage: DOStorage;
  private sql: SqlStorage;
  private readOps = 0;
  private writeOps = 0;
  private initialized = false;
  private initTime = 0;

  constructor(config: DOSqliteAdapterConfig) {
    this.storage = config.storage;
    this.sql = config.storage.sql;
  }

  // ===========================================================================
  // Lifecycle Methods
  // ===========================================================================

  /**
   * Initialize the adapter
   */
  async initialize(): Promise<void> {
    const startTime = performance.now();

    // DO SQLite is always available, no explicit initialization needed
    // But we record the time for cold start measurement
    this.initTime = performance.now() - startTime;
    this.initialized = true;

    // Reset operation counters
    this.readOps = 0;
    this.writeOps = 0;
  }

  /**
   * Clean up resources
   */
  async cleanup(): Promise<void> {
    // Nothing to clean up for DO SQLite
    // Tables persist unless explicitly dropped
    this.initialized = false;
  }

  // ===========================================================================
  // Table Operations
  // ===========================================================================

  /**
   * Create a table with the given schema
   */
  async createTable(schema: TableSchemaConfig): Promise<void> {
    const columns = schema.columns
      .map((col) => {
        const nullable = col.nullable ? '' : ' NOT NULL';
        const pk = col.name === schema.primaryKey ? ' PRIMARY KEY' : '';
        return `${col.name} ${col.type}${nullable}${pk}`;
      })
      .join(', ');

    const sql = `CREATE TABLE IF NOT EXISTS ${schema.tableName} (${columns})`;
    const cursor = this.sql.exec(sql);

    // Track write operation
    this.writeOps += cursor.rowsWritten;

    // Create indexes
    if (schema.indexes) {
      for (const indexCol of schema.indexes) {
        const indexSql = `CREATE INDEX IF NOT EXISTS idx_${schema.tableName}_${indexCol} ON ${schema.tableName}(${indexCol})`;
        const indexCursor = this.sql.exec(indexSql);
        this.writeOps += indexCursor.rowsWritten;
      }
    }
  }

  /**
   * Drop a table if it exists
   */
  async dropTable(tableName: string): Promise<void> {
    const sql = `DROP TABLE IF EXISTS ${tableName}`;
    const cursor = this.sql.exec(sql);
    this.writeOps += cursor.rowsWritten;
  }

  // ===========================================================================
  // CRUD Operations
  // ===========================================================================

  /**
   * Insert a single row
   *
   * Note: cursor.rowsWritten includes index writes. We use changes() to
   * get the actual number of data rows inserted.
   */
  async insert(
    tableName: string,
    row: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const startTime = performance.now();
    const startedAt = Date.now();

    try {
      const columns = Object.keys(row);
      const placeholders = columns.map(() => '?').join(', ');
      const values = Object.values(row);

      const sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
      const cursor = this.sql.exec(sql, ...values);

      // Get actual rows affected (not including index writes)
      const changesCursor = this.sql.exec<{ changes: number }>('SELECT changes() as changes');
      const changesResult = changesCursor.one();
      const rowsAffected = changesResult?.changes ?? 0;

      const durationMs = performance.now() - startTime;
      this.writeOps += cursor.rowsWritten;
      this.readOps += cursor.rowsRead;

      return {
        type: 'create',
        sql,
        params: row,
        durationMs,
        rowCount: rowsAffected,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startTime;
      return {
        type: 'create',
        sql: `INSERT INTO ${tableName} ...`,
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
   * Insert multiple rows in a batch using transactionSync
   *
   * Note: DO SQLite does not support explicit BEGIN TRANSACTION statements.
   * We use ctx.storage.transactionSync() for atomic batch operations.
   */
  async insertBatch(
    tableName: string,
    rows: Record<string, unknown>[]
  ): Promise<BenchmarkOperation> {
    const startTime = performance.now();
    const startedAt = Date.now();

    if (rows.length === 0) {
      return {
        type: 'batch',
        sql: 'INSERT BATCH (empty)',
        durationMs: 0,
        rowCount: 0,
        success: true,
        startedAt,
      };
    }

    try {
      const columns = Object.keys(rows[0]);
      const placeholders = columns.map(() => '?').join(', ');
      const sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;

      // Use transactionSync for atomic batch insert
      const result = this.storage.transactionSync(() => {
        let totalRowsWritten = 0;
        let totalRowsRead = 0;

        for (const row of rows) {
          const values = columns.map((col) => row[col]);
          const cursor = this.sql.exec(sql, ...values);
          totalRowsWritten += cursor.rowsWritten;
          totalRowsRead += cursor.rowsRead;
        }

        return { totalRowsWritten, totalRowsRead };
      });

      const durationMs = performance.now() - startTime;
      this.writeOps += result.totalRowsWritten;
      this.readOps += result.totalRowsRead;

      return {
        type: 'batch',
        sql: `INSERT BATCH (${rows.length} rows)`,
        durationMs,
        rowCount: rows.length, // Use rows.length as the count of inserted rows
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startTime;
      return {
        type: 'batch',
        sql: `INSERT BATCH (${rows.length} rows)`,
        durationMs,
        rowCount: 0,
        success: false,
        error: error instanceof Error ? error.message : String(error),
        startedAt,
      };
    }
  }

  /**
   * Read a single row by primary key
   */
  async read(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation> {
    const startTime = performance.now();
    const startedAt = Date.now();

    try {
      const sql = `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?`;
      const cursor = this.sql.exec(sql, primaryKeyValue);
      const rows = cursor.toArray();

      const durationMs = performance.now() - startTime;
      this.readOps += cursor.rowsRead;

      return {
        type: 'read',
        sql,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: rows.length,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startTime;
      return {
        type: 'read',
        sql: `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?`,
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
   * Read multiple rows with a WHERE clause
   */
  async readMany(
    tableName: string,
    whereClause: string,
    params?: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const startTime = performance.now();
    const startedAt = Date.now();

    try {
      const sql = `SELECT * FROM ${tableName} WHERE ${whereClause}`;
      const bindings = params ? Object.values(params) : [];
      const cursor = this.sql.exec(sql, ...bindings);
      const rows = cursor.toArray();

      const durationMs = performance.now() - startTime;
      this.readOps += cursor.rowsRead;

      return {
        type: 'query',
        sql,
        params,
        durationMs,
        rowCount: rows.length,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startTime;
      return {
        type: 'query',
        sql: `SELECT * FROM ${tableName} WHERE ${whereClause}`,
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
   * Update a single row
   *
   * Note: cursor.rowsWritten includes index updates. We use changes() to
   * get the actual number of data rows affected.
   */
  async update(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown,
    updates: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const startTime = performance.now();
    const startedAt = Date.now();

    try {
      const setClauses = Object.keys(updates)
        .map((col) => `${col} = ?`)
        .join(', ');
      const values = [...Object.values(updates), primaryKeyValue];

      const sql = `UPDATE ${tableName} SET ${setClauses} WHERE ${primaryKey} = ?`;
      const cursor = this.sql.exec(sql, ...values);

      // Get actual rows affected (not including index writes)
      const changesCursor = this.sql.exec<{ changes: number }>('SELECT changes() as changes');
      const changesResult = changesCursor.one();
      const rowsAffected = changesResult?.changes ?? 0;

      const durationMs = performance.now() - startTime;
      this.writeOps += cursor.rowsWritten;
      this.readOps += cursor.rowsRead;

      return {
        type: 'update',
        sql,
        params: { ...updates, [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: rowsAffected,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startTime;
      return {
        type: 'update',
        sql: `UPDATE ${tableName} SET ... WHERE ${primaryKey} = ?`,
        params: { ...updates, [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: 0,
        success: false,
        error: error instanceof Error ? error.message : String(error),
        startedAt,
      };
    }
  }

  /**
   * Delete a single row
   *
   * Note: cursor.rowsWritten includes index updates. We use changes() to
   * get the actual number of data rows deleted.
   */
  async delete(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation> {
    const startTime = performance.now();
    const startedAt = Date.now();

    try {
      const sql = `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`;
      const cursor = this.sql.exec(sql, primaryKeyValue);

      // Get actual rows affected (not including index writes)
      const changesCursor = this.sql.exec<{ changes: number }>('SELECT changes() as changes');
      const changesResult = changesCursor.one();
      const rowsAffected = changesResult?.changes ?? 0;

      const durationMs = performance.now() - startTime;
      this.writeOps += cursor.rowsWritten;
      this.readOps += cursor.rowsRead;

      return {
        type: 'delete',
        sql,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: rowsAffected,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startTime;
      return {
        type: 'delete',
        sql: `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: 0,
        success: false,
        error: error instanceof Error ? error.message : String(error),
        startedAt,
      };
    }
  }

  // ===========================================================================
  // Query Operations
  // ===========================================================================

  /**
   * Execute a raw SQL query
   */
  async query(
    sql: string,
    params?: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const startTime = performance.now();
    const startedAt = Date.now();

    try {
      const bindings = params ? Object.values(params) : [];
      const cursor = this.sql.exec(sql, ...bindings);
      const rows = cursor.toArray();

      const durationMs = performance.now() - startTime;
      this.readOps += cursor.rowsRead;
      this.writeOps += cursor.rowsWritten;

      // Determine operation type from SQL
      const normalizedSql = sql.trim().toUpperCase();
      let type: 'query' | 'create' | 'update' | 'delete' = 'query';
      if (normalizedSql.startsWith('INSERT')) type = 'create';
      else if (normalizedSql.startsWith('UPDATE')) type = 'update';
      else if (normalizedSql.startsWith('DELETE')) type = 'delete';

      return {
        type,
        sql,
        params,
        durationMs,
        rowCount: type === 'query' ? rows.length : cursor.rowsWritten,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startTime;
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
   * Execute multiple operations in a transaction using transactionSync
   *
   * Note: DO SQLite does not support explicit BEGIN TRANSACTION statements.
   * We use ctx.storage.transactionSync() for atomic operations.
   */
  async transaction(
    operations: Array<{
      type: 'insert' | 'update' | 'delete';
      tableName: string;
      data: Record<string, unknown>;
    }>
  ): Promise<BenchmarkOperation> {
    const startTime = performance.now();
    const startedAt = Date.now();

    try {
      // Use transactionSync for atomic multi-operation transactions
      const result = this.storage.transactionSync(() => {
        let totalRowsAffected = 0;
        let totalRowsRead = 0;
        let totalRowsWritten = 0;

        for (const op of operations) {
          let cursor: SqlStorageCursor;

          switch (op.type) {
            case 'insert': {
              const columns = Object.keys(op.data);
              const placeholders = columns.map(() => '?').join(', ');
              const values = Object.values(op.data);
              const sql = `INSERT INTO ${op.tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
              cursor = this.sql.exec(sql, ...values);
              break;
            }
            case 'update': {
              // Assume data has both update values and a primary key (id)
              const { id, ...updates } = op.data;
              const setClauses = Object.keys(updates)
                .map((col) => `${col} = ?`)
                .join(', ');
              const values = [...Object.values(updates), id];
              const sql = `UPDATE ${op.tableName} SET ${setClauses} WHERE id = ?`;
              cursor = this.sql.exec(sql, ...values);
              break;
            }
            case 'delete': {
              const { id } = op.data;
              const sql = `DELETE FROM ${op.tableName} WHERE id = ?`;
              cursor = this.sql.exec(sql, id);
              break;
            }
            default:
              throw new Error(`Unknown operation type: ${op.type}`);
          }

          totalRowsAffected += 1; // Count by operations, not SQLite internal rowsWritten
          totalRowsRead += cursor.rowsRead;
          totalRowsWritten += cursor.rowsWritten;
        }

        return { totalRowsAffected, totalRowsRead, totalRowsWritten };
      });

      const durationMs = performance.now() - startTime;
      this.readOps += result.totalRowsRead;
      this.writeOps += result.totalRowsWritten;

      return {
        type: 'transaction',
        sql: `TRANSACTION (${operations.length} operations)`,
        durationMs,
        rowCount: result.totalRowsAffected,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - startTime;
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

  // ===========================================================================
  // Metrics Operations
  // ===========================================================================

  /**
   * Get current storage metrics
   */
  async getStorageMetrics(): Promise<StorageMetrics> {
    // Get database size from the sql object
    const totalBytes = this.sql.databaseSize;

    // Count tables
    const tablesCursor = this.sql.exec<{ name: string }>(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    );
    const tables = tablesCursor.toArray();
    this.readOps += tablesCursor.rowsRead;

    // Count total rows across all tables
    let totalRows = 0;
    for (const table of tables) {
      const countCursor = this.sql.exec<{ count: number }>(
        `SELECT COUNT(*) as count FROM ${table.name}`
      );
      const result = countCursor.one();
      totalRows += result?.count ?? 0;
      this.readOps += countCursor.rowsRead;
    }

    // Calculate limit utilization (128MB max)
    const limitUtilization = (totalBytes / DO_SQLITE_PRICING.maxDatabaseSize) * 100;

    return {
      totalBytes,
      rowCount: totalRows,
      tableCount: tables.length,
      limitUtilization,
    };
  }

  /**
   * Measure cold start time
   */
  async measureColdStart(): Promise<ColdStartMetrics> {
    const connectionStart = performance.now();

    // Simulate cold start by executing a simple query
    const queryStart = performance.now();
    this.sql.exec('SELECT 1');
    const timeToFirstQuery = performance.now() - queryStart;

    return {
      timeToFirstQuery,
      initializationTime: this.initTime,
      connectionTime: performance.now() - connectionStart,
    };
  }

  /**
   * Get billing metrics based on tracked operations
   */
  async getBillingMetrics(): Promise<DOBillingMetrics> {
    const storage = await this.getStorageMetrics();
    return estimateDOBilling(this.readOps, this.writeOps, storage.totalBytes);
  }

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  /**
   * Get the current read operation count
   */
  getReadOps(): number {
    return this.readOps;
  }

  /**
   * Get the current write operation count
   */
  getWriteOps(): number {
    return this.writeOps;
  }

  /**
   * Reset operation counters
   */
  resetCounters(): void {
    this.readOps = 0;
    this.writeOps = 0;
  }

  /**
   * Check if the database is approaching the 128MB limit
   * Returns true if > 90% utilized
   */
  async isApproachingLimit(): Promise<boolean> {
    const metrics = await this.getStorageMetrics();
    return metrics.limitUtilization > 90;
  }

  /**
   * Get estimated remaining capacity in bytes
   */
  async getRemainingCapacity(): Promise<number> {
    const totalBytes = this.sql.databaseSize;
    return DO_SQLITE_PRICING.maxDatabaseSize - totalBytes;
  }

  // ===========================================================================
  // execMany Batch Operations
  // ===========================================================================

  /**
   * Execute multiple SQL statements in a single batch
   *
   * All statements are executed within a single transaction for atomicity.
   * If any statement fails, all changes are rolled back.
   *
   * @param statements - Array of SQL statements to execute
   * @returns Array of results matching input order
   */
  execMany(
    statements: string[]
  ): ExecManyResult[] {
    // Return empty array for empty input
    if (statements.length === 0) {
      return [];
    }

    // Execute all statements within a transaction for atomicity
    return this.storage.transactionSync(() => {
      const results: ExecManyResult[] = [];

      for (const statement of statements) {
        const startTime = performance.now();
        const trimmed = statement.trim();

        // Handle empty/whitespace-only statements
        if (!trimmed) {
          results.push({
            success: true,
            rowCount: 0,
            durationMs: performance.now() - startTime,
          });
          continue;
        }

        // Handle comment-only statements
        if (trimmed.startsWith('--') || (trimmed.startsWith('/*') && trimmed.endsWith('*/'))) {
          results.push({
            success: true,
            rowCount: 0,
            durationMs: performance.now() - startTime,
          });
          continue;
        }

        try {
          const cursor = this.sql.exec(trimmed);
          const durationMs = performance.now() - startTime;

          // Determine if this is a SELECT query
          const normalizedSql = trimmed.toUpperCase();
          const isSelect = normalizedSql.startsWith('SELECT');

          if (isSelect) {
            const rows = cursor.toArray();
            this.readOps += cursor.rowsRead;

            results.push({
              success: true,
              rowCount: rows.length,
              rows: rows as Record<string, unknown>[],
              durationMs,
            });
          } else {
            // For INSERT/UPDATE/DELETE/DDL, get actual rows affected
            const changesCursor = this.sql.exec<{ changes: number }>('SELECT changes() as changes');
            const changesResult = changesCursor.one();
            const rowsAffected = changesResult?.changes ?? cursor.rowsWritten;

            this.writeOps += cursor.rowsWritten;
            this.readOps += cursor.rowsRead;

            results.push({
              success: true,
              rowCount: rowsAffected,
              durationMs,
            });
          }
        } catch (error) {
          const durationMs = performance.now() - startTime;
          results.push({
            success: false,
            rowCount: 0,
            error: error instanceof Error ? error.message : String(error),
            durationMs,
          });
          // Throw to trigger transaction rollback
          throw error;
        }
      }

      return results;
    });
  }

  /**
   * Execute multiple SQL statements with parameters in a single batch
   *
   * All statements are executed within a single transaction for atomicity.
   * If any statement fails, all changes are rolled back.
   *
   * @param statements - Array of {sql, params} objects
   * @returns Array of results matching input order
   */
  execManyWithParams(
    statements: Array<{ sql: string; params?: Record<string, unknown> }>
  ): ExecManyResult[] {
    // Return empty array for empty input
    if (statements.length === 0) {
      return [];
    }

    // Execute all statements within a transaction for atomicity
    return this.storage.transactionSync(() => {
      const results: ExecManyResult[] = [];

      for (const { sql, params } of statements) {
        const startTime = performance.now();
        const trimmed = sql.trim();

        // Handle empty/whitespace-only statements
        if (!trimmed) {
          results.push({
            success: true,
            rowCount: 0,
            durationMs: performance.now() - startTime,
          });
          continue;
        }

        try {
          const bindings = params ? Object.values(params) : [];
          const cursor = this.sql.exec(trimmed, ...bindings);
          const durationMs = performance.now() - startTime;

          // Determine if this is a SELECT query
          const normalizedSql = trimmed.toUpperCase();
          const isSelect = normalizedSql.startsWith('SELECT');

          if (isSelect) {
            const rows = cursor.toArray();
            this.readOps += cursor.rowsRead;

            results.push({
              success: true,
              rowCount: rows.length,
              rows: rows as Record<string, unknown>[],
              durationMs,
            });
          } else {
            // For INSERT/UPDATE/DELETE/DDL, get actual rows affected
            const changesCursor = this.sql.exec<{ changes: number }>('SELECT changes() as changes');
            const changesResult = changesCursor.one();
            const rowsAffected = changesResult?.changes ?? cursor.rowsWritten;

            this.writeOps += cursor.rowsWritten;
            this.readOps += cursor.rowsRead;

            results.push({
              success: true,
              rowCount: rowsAffected,
              durationMs,
            });
          }
        } catch (error) {
          const durationMs = performance.now() - startTime;
          results.push({
            success: false,
            rowCount: 0,
            error: error instanceof Error ? error.message : String(error),
            durationMs,
          });
          // Throw to trigger transaction rollback
          throw error;
        }
      }

      return results;
    });
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a new DO SQLite benchmark adapter
 */
export function createDOSqliteAdapter(config: DOSqliteAdapterConfig): DOSqliteAdapter {
  return new DOSqliteAdapter(config);
}
