/**
 * Turso Benchmark Adapter
 *
 * Implements the BenchmarkAdapter interface for Turso (libSQL) databases.
 * Supports both HTTP connections and embedded replicas with sync capabilities.
 */

import { createClient, Client, InValue, InStatement } from '@libsql/client';
import {
  BenchmarkAdapter,
  BenchmarkOperation,
  TableSchemaConfig,
  StorageMetrics,
  ColdStartMetrics,
  ColumnConfig,
} from '../types.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Turso adapter configuration
 */
export interface TursoAdapterConfig {
  /** Database URL (http://, https://, ws://, wss://, or libsql://) */
  url: string;
  /** Authentication token for Turso cloud databases */
  authToken?: string;
  /** Sync URL for embedded replicas (enables local-first mode) */
  syncUrl?: string;
  /** Sync interval in milliseconds (for embedded replicas) */
  syncInterval?: number;
  /** Enable encryption at rest (for embedded replicas) */
  encryptionKey?: string;
}

/**
 * Extended operation result with network metrics
 */
export interface TursoOperationResult extends BenchmarkOperation {
  /** Network latency in milliseconds (time waiting for response) */
  networkLatencyMs?: number;
  /** Sync latency in milliseconds (for embedded replicas) */
  syncLatencyMs?: number;
  /** Whether the operation was served from local replica */
  servedFromReplica?: boolean;
}

// =============================================================================
// Turso Adapter Implementation
// =============================================================================

/**
 * Turso benchmark adapter
 *
 * Features:
 * - HTTP and WebSocket connections to Turso cloud
 * - Embedded replicas with local SQLite
 * - Automatic sync with primary database
 * - Transaction support via client.transaction()
 * - Network latency tracking
 */
export class TursoAdapter implements BenchmarkAdapter {
  readonly name = 'turso';
  readonly version = '1.0.0';

  private client: Client | null = null;
  private readonly adapterConfig: TursoAdapterConfig;
  private isInitialized = false;
  private connectionStartTime = 0;

  constructor(config: TursoAdapterConfig) {
    this.adapterConfig = config;
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  /**
   * Get the client, throwing if not initialized
   */
  private getClient(): Client {
    if (!this.isInitialized || !this.client) {
      throw new Error('TursoAdapter not initialized. Call initialize() first.');
    }
    return this.client;
  }

  /**
   * Convert a value to InValue type for libSQL
   */
  private toInValue(value: unknown): InValue {
    if (value === null || value === undefined) {
      return null;
    }
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'bigint') {
      return value;
    }
    if (typeof value === 'boolean') {
      return value ? 1 : 0;
    }
    if (value instanceof Uint8Array || value instanceof ArrayBuffer) {
      return value instanceof Uint8Array ? value : new Uint8Array(value);
    }
    // Convert other types to string
    return String(value);
  }

  /**
   * Convert an array of unknown values to InValue array
   */
  private toInValues(values: unknown[]): InValue[] {
    return values.map((v) => this.toInValue(v));
  }

  /**
   * Convert column config to SQL definition
   */
  private columnToSQL(col: ColumnConfig, isPrimaryKey: boolean): string {
    let sql = `${col.name} ${col.type}`;

    if (isPrimaryKey) {
      sql += ' PRIMARY KEY';
      if (col.type === 'INTEGER') {
        sql += ' AUTOINCREMENT';
      }
    }

    if (!col.nullable && !isPrimaryKey) {
      sql += ' NOT NULL';
    }

    if (col.defaultValue !== undefined) {
      sql += ` DEFAULT ${this.valueToSQL(col.defaultValue)}`;
    }

    return sql;
  }

  /**
   * Convert a value to SQL literal
   */
  private valueToSQL(value: unknown): string {
    if (value === null) {
      return 'NULL';
    }
    if (typeof value === 'string') {
      return `'${value.replace(/'/g, "''")}'`;
    }
    if (typeof value === 'number' || typeof value === 'bigint') {
      return String(value);
    }
    if (typeof value === 'boolean') {
      return value ? '1' : '0';
    }
    return String(value);
  }

  // ===========================================================================
  // Lifecycle Methods
  // ===========================================================================

  /**
   * Initialize the Turso client
   */
  async initialize(): Promise<void> {
    if (this.isInitialized) {
      return;
    }

    this.connectionStartTime = performance.now();

    this.client = createClient({
      url: this.adapterConfig.url,
      authToken: this.adapterConfig.authToken,
      syncUrl: this.adapterConfig.syncUrl,
      syncInterval: this.adapterConfig.syncInterval,
      encryptionKey: this.adapterConfig.encryptionKey,
    });

    // Verify connection with a simple query
    await this.client.execute('SELECT 1');

    this.isInitialized = true;
  }

  /**
   * Clean up resources
   */
  async cleanup(): Promise<void> {
    if (this.client) {
      this.client.close();
      this.client = null;
    }
    this.isInitialized = false;
  }

  // ===========================================================================
  // Schema Methods
  // ===========================================================================

  /**
   * Create a table with the given schema
   */
  async createTable(schema: TableSchemaConfig): Promise<void> {
    const client = this.getClient();

    const columnDefs = schema.columns
      .map((col) => this.columnToSQL(col, col.name === schema.primaryKey))
      .join(', ');

    const createSQL = `CREATE TABLE IF NOT EXISTS ${schema.tableName} (${columnDefs})`;
    await client.execute(createSQL);

    // Create indexes
    if (schema.indexes) {
      for (const indexCol of schema.indexes) {
        const indexSQL = `CREATE INDEX IF NOT EXISTS idx_${schema.tableName}_${indexCol} ON ${schema.tableName}(${indexCol})`;
        await client.execute(indexSQL);
      }
    }
  }

  /**
   * Drop a table if it exists
   */
  async dropTable(tableName: string): Promise<void> {
    const client = this.getClient();
    await client.execute(`DROP TABLE IF EXISTS ${tableName}`);
  }

  // ===========================================================================
  // CRUD Operations
  // ===========================================================================

  /**
   * Insert a single row
   */
  async insert(
    tableName: string,
    row: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const client = this.getClient();

    const columns = Object.keys(row);
    const placeholders = columns.map((_, i) => `?${i + 1}`).join(', ');
    const values = this.toInValues(Object.values(row));

    const sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    try {
      const result = await client.execute({
        sql,
        args: values,
      });
      rowCount = Number(result.rowsAffected);
    } catch (e) {
      success = false;
      error = e instanceof Error ? e.message : String(e);
    }

    const durationMs = performance.now() - startedAt;

    return {
      type: 'create',
      sql,
      params: row,
      durationMs,
      rowCount,
      success,
      error,
      startedAt,
    };
  }

  /**
   * Insert multiple rows in a batch
   */
  async insertBatch(
    tableName: string,
    rows: Record<string, unknown>[]
  ): Promise<BenchmarkOperation> {
    const client = this.getClient();

    if (rows.length === 0) {
      return {
        type: 'batch',
        sql: '',
        durationMs: 0,
        rowCount: 0,
        success: true,
        startedAt: performance.now(),
      };
    }

    const columns = Object.keys(rows[0]);
    const placeholders = columns.map((_, i) => `?${i + 1}`).join(', ');
    const sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let totalRowCount = 0;

    try {
      // Use batch for efficient multi-row insert
      const statements: InStatement[] = rows.map((row) => ({
        sql,
        args: this.toInValues(Object.values(row)),
      }));

      const results = await client.batch(statements, 'write');
      totalRowCount = results.reduce(
        (sum, r) => sum + Number(r.rowsAffected),
        0
      );
    } catch (e) {
      success = false;
      error = e instanceof Error ? e.message : String(e);
    }

    const durationMs = performance.now() - startedAt;

    return {
      type: 'batch',
      sql: `BATCH INSERT ${rows.length} rows`,
      params: { rowCount: rows.length },
      durationMs,
      rowCount: totalRowCount,
      success,
      error,
      startedAt,
    };
  }

  /**
   * Read a single row by primary key
   */
  async read(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation> {
    const client = this.getClient();

    const sql = `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?1`;

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    try {
      const result = await client.execute({
        sql,
        args: [this.toInValue(primaryKeyValue)],
      });
      rowCount = result.rows.length;
    } catch (e) {
      success = false;
      error = e instanceof Error ? e.message : String(e);
    }

    const durationMs = performance.now() - startedAt;

    return {
      type: 'read',
      sql,
      params: { [primaryKey]: primaryKeyValue },
      durationMs,
      rowCount,
      success,
      error,
      startedAt,
    };
  }

  /**
   * Read multiple rows with a WHERE clause
   */
  async readMany(
    tableName: string,
    whereClause: string,
    params?: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const client = this.getClient();

    const sql = `SELECT * FROM ${tableName} WHERE ${whereClause}`;

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    try {
      const args = params ? this.toInValues(Object.values(params)) : [];
      const result = await client.execute({
        sql,
        args,
      });
      rowCount = result.rows.length;
    } catch (e) {
      success = false;
      error = e instanceof Error ? e.message : String(e);
    }

    const durationMs = performance.now() - startedAt;

    return {
      type: 'read',
      sql,
      params,
      durationMs,
      rowCount,
      success,
      error,
      startedAt,
    };
  }

  /**
   * Update a single row
   */
  async update(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown,
    updates: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const client = this.getClient();

    const setClauses = Object.keys(updates)
      .map((key, i) => `${key} = ?${i + 1}`)
      .join(', ');
    const values = this.toInValues([...Object.values(updates), primaryKeyValue]);

    const sql = `UPDATE ${tableName} SET ${setClauses} WHERE ${primaryKey} = ?${values.length}`;

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    try {
      const result = await client.execute({
        sql,
        args: values,
      });
      rowCount = Number(result.rowsAffected);
    } catch (e) {
      success = false;
      error = e instanceof Error ? e.message : String(e);
    }

    const durationMs = performance.now() - startedAt;

    return {
      type: 'update',
      sql,
      params: { ...updates, [primaryKey]: primaryKeyValue },
      durationMs,
      rowCount,
      success,
      error,
      startedAt,
    };
  }

  /**
   * Delete a single row
   */
  async delete(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation> {
    const client = this.getClient();

    const sql = `DELETE FROM ${tableName} WHERE ${primaryKey} = ?1`;

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    try {
      const result = await client.execute({
        sql,
        args: [this.toInValue(primaryKeyValue)],
      });
      rowCount = Number(result.rowsAffected);
    } catch (e) {
      success = false;
      error = e instanceof Error ? e.message : String(e);
    }

    const durationMs = performance.now() - startedAt;

    return {
      type: 'delete',
      sql,
      params: { [primaryKey]: primaryKeyValue },
      durationMs,
      rowCount,
      success,
      error,
      startedAt,
    };
  }

  /**
   * Execute a raw SQL query
   */
  async query(
    sql: string,
    params?: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const client = this.getClient();

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    try {
      const args = params ? this.toInValues(Object.values(params)) : [];
      const result = await client.execute({
        sql,
        args,
      });
      rowCount = result.rows.length || Number(result.rowsAffected);
    } catch (e) {
      success = false;
      error = e instanceof Error ? e.message : String(e);
    }

    const durationMs = performance.now() - startedAt;

    return {
      type: 'query',
      sql,
      params,
      durationMs,
      rowCount,
      success,
      error,
      startedAt,
    };
  }

  /**
   * Execute multiple operations in a transaction
   */
  async transaction(
    operations: Array<{
      type: 'insert' | 'update' | 'delete';
      tableName: string;
      data: Record<string, unknown>;
    }>
  ): Promise<BenchmarkOperation> {
    const client = this.getClient();

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let totalRowCount = 0;

    const sqlStatements: string[] = [];

    try {
      const tx = await client.transaction('write');

      try {
        for (const op of operations) {
          let sql: string;
          let args: InValue[];

          switch (op.type) {
            case 'insert': {
              const columns = Object.keys(op.data);
              const placeholders = columns.map((_, i) => `?${i + 1}`).join(', ');
              sql = `INSERT INTO ${op.tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
              args = this.toInValues(Object.values(op.data));
              break;
            }
            case 'update': {
              const { id, ...updates } = op.data;
              const setClauses = Object.keys(updates)
                .map((key, i) => `${key} = ?${i + 1}`)
                .join(', ');
              sql = `UPDATE ${op.tableName} SET ${setClauses} WHERE id = ?${Object.keys(updates).length + 1}`;
              args = this.toInValues([...Object.values(updates), id]);
              break;
            }
            case 'delete': {
              const { id } = op.data;
              sql = `DELETE FROM ${op.tableName} WHERE id = ?1`;
              args = [this.toInValue(id)];
              break;
            }
          }

          sqlStatements.push(sql);
          const result = await tx.execute({ sql, args });
          totalRowCount += Number(result.rowsAffected);
        }

        await tx.commit();
      } catch (e) {
        await tx.rollback();
        throw e;
      }
    } catch (e) {
      success = false;
      error = e instanceof Error ? e.message : String(e);
    }

    const durationMs = performance.now() - startedAt;

    return {
      type: 'transaction',
      sql: `TRANSACTION: ${sqlStatements.length} operations`,
      params: { operationCount: operations.length },
      durationMs,
      rowCount: totalRowCount,
      success,
      error,
      startedAt,
    };
  }

  // ===========================================================================
  // Metrics Methods
  // ===========================================================================

  /**
   * Get current storage metrics
   */
  async getStorageMetrics(): Promise<StorageMetrics> {
    const client = this.getClient();

    // Get database size (SQLite-specific)
    const sizeResult = await client.execute(
      "SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()"
    );

    // Get table count
    const tableResult = await client.execute(
      "SELECT COUNT(*) as count FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    );

    // Get total row count across all tables
    const tablesResult = await client.execute(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    );

    let totalRows = 0;
    for (const row of tablesResult.rows) {
      const tableName = row.name as string;
      const countResult = await client.execute(
        `SELECT COUNT(*) as count FROM "${tableName}"`
      );
      totalRows += Number(countResult.rows[0]?.count ?? 0);
    }

    const totalBytes = Number(sizeResult.rows[0]?.size ?? 0);
    const tableCount = Number(tableResult.rows[0]?.count ?? 0);

    // Turso has no hard limit like DO SQLite's 128MB, but we can track utilization
    // against a configurable soft limit (default 10GB for Turso free tier)
    const softLimit = 10 * 1024 * 1024 * 1024; // 10GB

    return {
      totalBytes,
      rowCount: totalRows,
      tableCount,
      limitUtilization: (totalBytes / softLimit) * 100,
    };
  }

  /**
   * Measure cold start time
   */
  async measureColdStart(): Promise<ColdStartMetrics> {
    // Close existing connection
    if (this.client) {
      this.client.close();
      this.client = null;
      this.isInitialized = false;
    }

    const totalStart = performance.now();

    // Re-create client (connection time)
    const connectionStart = performance.now();
    this.client = createClient({
      url: this.adapterConfig.url,
      authToken: this.adapterConfig.authToken,
      syncUrl: this.adapterConfig.syncUrl,
      syncInterval: this.adapterConfig.syncInterval,
      encryptionKey: this.adapterConfig.encryptionKey,
    });
    const connectionTime = performance.now() - connectionStart;

    // First query (initialization time)
    const initStart = performance.now();
    await this.client.execute('SELECT 1');
    const initializationTime = performance.now() - initStart;

    this.isInitialized = true;

    const timeToFirstQuery = performance.now() - totalStart;

    return {
      timeToFirstQuery,
      initializationTime,
      connectionTime,
    };
  }

  // ===========================================================================
  // Turso-specific Methods
  // ===========================================================================

  /**
   * Sync embedded replica with primary database
   * Only applicable when using embedded replicas with syncUrl
   */
  async sync(): Promise<number> {
    const client = this.getClient();

    if (!this.adapterConfig.syncUrl) {
      return 0;
    }

    const startTime = performance.now();
    await client.sync();
    return performance.now() - startTime;
  }

  /**
   * Get the underlying libSQL client for advanced operations
   */
  getLibsqlClient(): Client {
    return this.getClient();
  }

  /**
   * Execute a query and measure network latency separately
   */
  async executeWithMetrics(
    sql: string,
    args?: unknown[]
  ): Promise<TursoOperationResult> {
    const client = this.getClient();

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    try {
      const result = await client.execute({
        sql,
        args: args ? this.toInValues(args) : [],
      });
      rowCount = result.rows.length || Number(result.rowsAffected);
    } catch (e) {
      success = false;
      error = e instanceof Error ? e.message : String(e);
    }

    const durationMs = performance.now() - startedAt;

    return {
      type: 'query',
      sql,
      durationMs,
      rowCount,
      success,
      error,
      startedAt,
      // Network latency is approximated as total duration for HTTP connections
      // For embedded replicas, this would be ~0 for local reads
      networkLatencyMs: this.adapterConfig.syncUrl ? 0 : durationMs,
      servedFromReplica: !!this.adapterConfig.syncUrl,
    };
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a Turso adapter for HTTP/WebSocket connections
 */
export function createTursoAdapter(config: TursoAdapterConfig): TursoAdapter {
  return new TursoAdapter(config);
}

/**
 * Create a Turso adapter with embedded replica support
 */
export function createTursoReplicaAdapter(
  primaryUrl: string,
  localPath: string,
  authToken?: string
): TursoAdapter {
  return new TursoAdapter({
    url: localPath,
    syncUrl: primaryUrl,
    authToken,
  });
}

/**
 * Create a Turso adapter for in-memory database (useful for testing)
 */
export function createTursoMemoryAdapter(): TursoAdapter {
  return new TursoAdapter({
    url: ':memory:',
  });
}
