/**
 * LibSQL Benchmark Adapter
 *
 * Benchmark adapter for libSQL (the original SQLite fork, before Turso's Rust rewrite).
 * Uses @libsql/client/web for Workers compatibility.
 *
 * This adapter enables performance comparisons between:
 * - DoSQL (native columnar storage)
 * - DO SQLite (Cloudflare Durable Objects SQLite)
 * - LibSQL (Turso's SQLite fork)
 * - Limbo (Turso's Rust rewrite, separate adapter)
 *
 * IMPORTANT: The web client (@libsql/client/web) only supports HTTP/HTTPS endpoints.
 * It does NOT support in-memory databases (':memory:') or file URLs ('file:').
 * For Workers testing, you must either:
 * 1. Connect to a real Turso/libSQL endpoint
 * 2. Run sqld locally and connect via HTTP
 * 3. Use the Node.js client (@libsql/client) for local testing
 *
 * For isolated testing in CI, consider using:
 * - A Turso database specifically for testing
 * - sqld running in a Docker container
 * - The Node.js client with a file-based or in-memory database
 */

import { createClient, Client, ResultSet, InStatement, InValue } from '@libsql/client/web';
import {
  BenchmarkAdapter,
  BenchmarkOperation,
  TableSchemaConfig,
  StorageMetrics,
  ColdStartMetrics,
} from '../types.js';

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * LibSQL adapter configuration
 */
export interface LibSQLAdapterConfig {
  /** Database URL for HTTP mode (e.g., 'libsql://my-db.turso.io') */
  url?: string;
  /** Auth token for remote databases */
  authToken?: string;
  /** Use in-memory database for isolated tests */
  inMemory?: boolean;
  /** Sync URL for embedded replicas */
  syncUrl?: string;
  /** Encryption key for encrypted databases */
  encryptionKey?: string;
}

// =============================================================================
// Adapter Implementation
// =============================================================================

/**
 * LibSQL benchmark adapter
 *
 * Implements the BenchmarkAdapter interface for libSQL databases.
 * Supports both in-memory mode (for isolated tests) and HTTP mode (for remote databases).
 */
export class LibSQLAdapter implements BenchmarkAdapter {
  readonly name = 'libsql';
  readonly version = '1.0.0';

  private client: Client | null = null;
  private config: LibSQLAdapterConfig;
  private initialized = false;

  // Tracking for billing/storage metrics
  private totalReads = 0;
  private totalWrites = 0;
  private createdTables: Set<string> = new Set();

  constructor(config: LibSQLAdapterConfig = { inMemory: true }) {
    this.config = config;
  }

  /**
   * Initialize the adapter and establish connection
   */
  async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    if (this.config.inMemory) {
      // Note: @libsql/client/web does NOT support in-memory databases.
      // In-memory mode is only supported by the Node.js client (@libsql/client).
      // For Workers testing, you must use an HTTP endpoint.
      throw new Error(
        'LibSQLAdapter: In-memory mode is not supported by the web client. ' +
        'The @libsql/client/web only supports HTTP/HTTPS endpoints (libsql:, wss:, ws:, https:, http:). ' +
        'For testing in Workers, please provide a Turso database URL or run sqld locally.'
      );
    } else if (this.config.url) {
      // HTTP mode for remote databases
      this.client = createClient({
        url: this.config.url,
        authToken: this.config.authToken,
        syncUrl: this.config.syncUrl,
        encryptionKey: this.config.encryptionKey,
      });
    } else {
      throw new Error('LibSQLAdapter: Either inMemory or url must be specified');
    }

    // Verify connection by executing a simple query
    await this.client.execute('SELECT 1');
    this.initialized = true;
  }

  /**
   * Clean up resources
   */
  async cleanup(): Promise<void> {
    if (this.client) {
      // Drop all created tables
      const tables = Array.from(this.createdTables);
      for (const tableName of tables) {
        try {
          await this.client.execute(`DROP TABLE IF EXISTS ${this.escapeIdentifier(tableName)}`);
        } catch {
          // Ignore errors during cleanup
        }
      }

      this.client.close();
      this.client = null;
    }

    this.initialized = false;
    this.totalReads = 0;
    this.totalWrites = 0;
    this.createdTables.clear();
  }

  /**
   * Create a table with the given schema
   */
  async createTable(schema: TableSchemaConfig): Promise<void> {
    this.assertInitialized();

    const columnDefs = schema.columns.map((col) => {
      let def = `${this.escapeIdentifier(col.name)} ${col.type}`;
      if (col.name === schema.primaryKey) {
        def += ' PRIMARY KEY';
      }
      if (!col.nullable && col.name !== schema.primaryKey) {
        def += ' NOT NULL';
      }
      if (col.defaultValue !== undefined) {
        def += ` DEFAULT ${this.formatValue(col.defaultValue)}`;
      }
      return def;
    });

    const sql = `CREATE TABLE IF NOT EXISTS ${this.escapeIdentifier(schema.tableName)} (${columnDefs.join(', ')})`;
    await this.client!.execute(sql);

    // Create indexes
    if (schema.indexes) {
      for (const indexCol of schema.indexes) {
        const indexName = `idx_${schema.tableName}_${indexCol}`;
        const indexSql = `CREATE INDEX IF NOT EXISTS ${this.escapeIdentifier(indexName)} ON ${this.escapeIdentifier(schema.tableName)} (${this.escapeIdentifier(indexCol)})`;
        await this.client!.execute(indexSql);
      }
    }

    this.createdTables.add(schema.tableName);
    this.totalWrites++;
  }

  /**
   * Drop a table if it exists
   */
  async dropTable(tableName: string): Promise<void> {
    this.assertInitialized();

    await this.client!.execute(`DROP TABLE IF EXISTS ${this.escapeIdentifier(tableName)}`);
    this.createdTables.delete(tableName);
    this.totalWrites++;
  }

  /**
   * Insert a single row
   */
  async insert(
    tableName: string,
    row: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    this.assertInitialized();

    const columns = Object.keys(row);
    const placeholders = columns.map((_, i) => `?${i + 1}`).join(', ');
    const values = columns.map((col) => this.toLibSQLValue(row[col]));

    const sql = `INSERT INTO ${this.escapeIdentifier(tableName)} (${columns.map(c => this.escapeIdentifier(c)).join(', ')}) VALUES (${placeholders})`;

    const startedAt = Date.now();
    const start = performance.now();

    try {
      const result = await this.client!.execute({
        sql,
        args: values,
      });

      const durationMs = performance.now() - start;
      this.totalWrites++;

      return {
        type: 'create',
        sql,
        params: row,
        durationMs,
        rowCount: Number(result.rowsAffected),
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - start;
      return {
        type: 'create',
        sql,
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
   * Insert multiple rows in a batch
   */
  async insertBatch(
    tableName: string,
    rows: Record<string, unknown>[]
  ): Promise<BenchmarkOperation> {
    this.assertInitialized();

    if (rows.length === 0) {
      return {
        type: 'batch',
        sql: 'BATCH INSERT (empty)',
        durationMs: 0,
        rowCount: 0,
        success: true,
        startedAt: Date.now(),
      };
    }

    const columns = Object.keys(rows[0]);
    const placeholders = columns.map((_, i) => `?${i + 1}`).join(', ');
    const sql = `INSERT INTO ${this.escapeIdentifier(tableName)} (${columns.map(c => this.escapeIdentifier(c)).join(', ')}) VALUES (${placeholders})`;

    const statements: InStatement[] = rows.map((row) => ({
      sql,
      args: columns.map((col) => this.toLibSQLValue(row[col])),
    }));

    const startedAt = Date.now();
    const start = performance.now();

    try {
      const results = await this.client!.batch(statements, 'write');
      const durationMs = performance.now() - start;
      this.totalWrites += rows.length;

      const totalAffected = results.reduce((sum, r) => sum + Number(r.rowsAffected), 0);

      return {
        type: 'batch',
        sql: `BATCH INSERT (${rows.length} rows)`,
        durationMs,
        rowCount: totalAffected,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - start;
      return {
        type: 'batch',
        sql: `BATCH INSERT (${rows.length} rows)`,
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
    this.assertInitialized();

    const sql = `SELECT * FROM ${this.escapeIdentifier(tableName)} WHERE ${this.escapeIdentifier(primaryKey)} = ?1`;

    const startedAt = Date.now();
    const start = performance.now();

    try {
      const result = await this.client!.execute({
        sql,
        args: [this.toLibSQLValue(primaryKeyValue)],
      });

      const durationMs = performance.now() - start;
      this.totalReads++;

      return {
        type: 'read',
        sql,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: result.rows.length,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - start;
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
   * Read multiple rows with a WHERE clause
   */
  async readMany(
    tableName: string,
    whereClause: string,
    params?: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    this.assertInitialized();

    const sql = `SELECT * FROM ${this.escapeIdentifier(tableName)} WHERE ${whereClause}`;

    // Convert named params to positional if needed
    const args = params ? Object.values(params).map((v) => this.toLibSQLValue(v)) : [];

    const startedAt = Date.now();
    const start = performance.now();

    try {
      const result = await this.client!.execute({
        sql,
        args,
      });

      const durationMs = performance.now() - start;
      this.totalReads += result.rows.length;

      return {
        type: 'read',
        sql,
        params,
        durationMs,
        rowCount: result.rows.length,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - start;
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
   * Update a single row
   */
  async update(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown,
    updates: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    this.assertInitialized();

    const columns = Object.keys(updates);
    const setClauses = columns.map((col, i) => `${this.escapeIdentifier(col)} = ?${i + 1}`).join(', ');
    const sql = `UPDATE ${this.escapeIdentifier(tableName)} SET ${setClauses} WHERE ${this.escapeIdentifier(primaryKey)} = ?${columns.length + 1}`;

    const args = [
      ...columns.map((col) => this.toLibSQLValue(updates[col])),
      this.toLibSQLValue(primaryKeyValue),
    ];

    const startedAt = Date.now();
    const start = performance.now();

    try {
      const result = await this.client!.execute({
        sql,
        args,
      });

      const durationMs = performance.now() - start;
      this.totalWrites++;

      return {
        type: 'update',
        sql,
        params: { ...updates, [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: Number(result.rowsAffected),
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - start;
      return {
        type: 'update',
        sql,
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
   */
  async delete(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation> {
    this.assertInitialized();

    const sql = `DELETE FROM ${this.escapeIdentifier(tableName)} WHERE ${this.escapeIdentifier(primaryKey)} = ?1`;

    const startedAt = Date.now();
    const start = performance.now();

    try {
      const result = await this.client!.execute({
        sql,
        args: [this.toLibSQLValue(primaryKeyValue)],
      });

      const durationMs = performance.now() - start;
      this.totalWrites++;

      return {
        type: 'delete',
        sql,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: Number(result.rowsAffected),
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - start;
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
   * Execute a raw SQL query
   */
  async query(sql: string, params?: Record<string, unknown>): Promise<BenchmarkOperation> {
    this.assertInitialized();

    const args = params ? Object.values(params).map((v) => this.toLibSQLValue(v)) : [];

    const startedAt = Date.now();
    const start = performance.now();

    try {
      const result = await this.client!.execute({
        sql,
        args,
      });

      const durationMs = performance.now() - start;

      // Track reads/writes based on SQL type
      const sqlUpper = sql.trim().toUpperCase();
      if (sqlUpper.startsWith('SELECT')) {
        this.totalReads += result.rows.length;
      } else {
        this.totalWrites++;
      }

      return {
        type: 'query',
        sql,
        params,
        durationMs,
        rowCount: result.rows.length || Number(result.rowsAffected),
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - start;
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
   * Execute multiple operations in a transaction
   */
  async transaction(
    operations: Array<{
      type: 'insert' | 'update' | 'delete';
      tableName: string;
      data: Record<string, unknown>;
    }>
  ): Promise<BenchmarkOperation> {
    this.assertInitialized();

    const statements: InStatement[] = [];

    for (const op of operations) {
      switch (op.type) {
        case 'insert': {
          const columns = Object.keys(op.data);
          const placeholders = columns.map((_, i) => `?${i + 1}`).join(', ');
          statements.push({
            sql: `INSERT INTO ${this.escapeIdentifier(op.tableName)} (${columns.map(c => this.escapeIdentifier(c)).join(', ')}) VALUES (${placeholders})`,
            args: columns.map((col) => this.toLibSQLValue(op.data[col])),
          });
          break;
        }
        case 'update': {
          // Expect data to contain both update values and primary key info
          const { id, ...updates } = op.data as { id: unknown; [key: string]: unknown };
          const columns = Object.keys(updates);
          const setClauses = columns.map((col, i) => `${this.escapeIdentifier(col)} = ?${i + 1}`).join(', ');
          statements.push({
            sql: `UPDATE ${this.escapeIdentifier(op.tableName)} SET ${setClauses} WHERE id = ?${columns.length + 1}`,
            args: [...columns.map((col) => this.toLibSQLValue(updates[col])), this.toLibSQLValue(id)],
          });
          break;
        }
        case 'delete': {
          const { id } = op.data as { id: unknown };
          statements.push({
            sql: `DELETE FROM ${this.escapeIdentifier(op.tableName)} WHERE id = ?1`,
            args: [this.toLibSQLValue(id)],
          });
          break;
        }
      }
    }

    const startedAt = Date.now();
    const start = performance.now();

    try {
      const results = await this.client!.batch(statements, 'write');
      const durationMs = performance.now() - start;
      this.totalWrites += operations.length;

      const totalAffected = results.reduce((sum, r) => sum + Number(r.rowsAffected), 0);

      return {
        type: 'transaction',
        sql: `TRANSACTION (${operations.length} operations)`,
        durationMs,
        rowCount: totalAffected,
        success: true,
        startedAt,
      };
    } catch (error) {
      const durationMs = performance.now() - start;
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
   * Get current storage metrics
   */
  async getStorageMetrics(): Promise<StorageMetrics> {
    this.assertInitialized();

    // Get database page count and page size to estimate storage
    const pageSizeResult = await this.client!.execute('PRAGMA page_size');
    const pageCountResult = await this.client!.execute('PRAGMA page_count');

    const pageSize = Number(pageSizeResult.rows[0]?.[0] ?? 4096);
    const pageCount = Number(pageCountResult.rows[0]?.[0] ?? 0);
    const totalBytes = pageSize * pageCount;

    // Count rows across all created tables
    let totalRowCount = 0;
    const tables = Array.from(this.createdTables);
    for (const tableName of tables) {
      try {
        const countResult = await this.client!.execute(
          `SELECT COUNT(*) FROM ${this.escapeIdentifier(tableName)}`
        );
        totalRowCount += Number(countResult.rows[0]?.[0] ?? 0);
      } catch {
        // Table might not exist
      }
    }

    // DO SQLite limit is 128MB
    const doSqliteLimit = 128 * 1024 * 1024;

    return {
      totalBytes,
      rowCount: totalRowCount,
      tableCount: this.createdTables.size,
      limitUtilization: (totalBytes / doSqliteLimit) * 100,
    };
  }

  /**
   * Measure cold start time
   */
  async measureColdStart(): Promise<ColdStartMetrics> {
    // Close existing connection
    const wasInitialized = this.initialized;
    const savedClient = this.client;

    this.client = null;
    this.initialized = false;

    const initStart = performance.now();

    // Re-initialize
    await this.initialize();

    const initEnd = performance.now();
    const initializationTime = initEnd - initStart;

    // Time to first query
    const queryStart = performance.now();
    await this.client!.execute('SELECT 1');
    const queryEnd = performance.now();

    const connectionTime = queryEnd - queryStart;
    const timeToFirstQuery = initializationTime + connectionTime;

    return {
      timeToFirstQuery,
      initializationTime,
      connectionTime,
    };
  }

  // =============================================================================
  // Helper Methods
  // =============================================================================

  /**
   * Assert that the adapter is initialized
   */
  private assertInitialized(): void {
    if (!this.initialized || !this.client) {
      throw new Error('LibSQLAdapter: Not initialized. Call initialize() first.');
    }
  }

  /**
   * Escape an SQL identifier (table/column name)
   */
  private escapeIdentifier(identifier: string): string {
    // Use double quotes for identifiers, escape any embedded quotes
    return `"${identifier.replace(/"/g, '""')}"`;
  }

  /**
   * Format a value for SQL (for DEFAULT values, etc.)
   */
  private formatValue(value: unknown): string {
    if (value === null) {
      return 'NULL';
    }
    if (typeof value === 'string') {
      return `'${value.replace(/'/g, "''")}'`;
    }
    if (typeof value === 'number' || typeof value === 'boolean') {
      return String(value);
    }
    return `'${String(value)}'`;
  }

  /**
   * Convert a JavaScript value to a LibSQL Value type
   */
  private toLibSQLValue(value: unknown): InValue {
    if (value === null || value === undefined) {
      return null;
    }
    if (typeof value === 'string') {
      return value;
    }
    if (typeof value === 'number') {
      // LibSQL handles both integers and floats
      return value;
    }
    if (typeof value === 'boolean') {
      return value ? 1 : 0;
    }
    if (typeof value === 'bigint') {
      return value;
    }
    if (value instanceof Date) {
      // Store as ISO string or Unix timestamp
      return value.toISOString();
    }
    if (value instanceof ArrayBuffer || value instanceof Uint8Array) {
      // Store as blob
      return value instanceof Uint8Array ? value : new Uint8Array(value);
    }
    // For objects/arrays, JSON stringify
    return JSON.stringify(value);
  }

  // =============================================================================
  // Additional Query Methods (for convenience)
  // =============================================================================

  /**
   * Get the raw libsql client for advanced operations
   */
  getClient(): Client | null {
    return this.client;
  }

  /**
   * Check if adapter is initialized
   */
  isInitialized(): boolean {
    return this.initialized;
  }

  /**
   * Execute a raw query and return results
   */
  async executeRaw(sql: string, args?: InValue[]): Promise<ResultSet> {
    this.assertInitialized();
    return this.client!.execute({ sql, args: args ?? [] });
  }

  /**
   * Get read/write counts for billing estimation
   */
  getOperationCounts(): { reads: number; writes: number } {
    return {
      reads: this.totalReads,
      writes: this.totalWrites,
    };
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a new LibSQL adapter with the given configuration
 */
export function createLibSQLAdapter(config?: LibSQLAdapterConfig): LibSQLAdapter {
  return new LibSQLAdapter(config);
}

/**
 * Create an in-memory LibSQL adapter for testing
 */
export function createInMemoryLibSQLAdapter(): LibSQLAdapter {
  return new LibSQLAdapter({ inMemory: true });
}

/**
 * Create a remote LibSQL adapter for Turso databases
 */
export function createRemoteLibSQLAdapter(url: string, authToken?: string): LibSQLAdapter {
  return new LibSQLAdapter({
    url,
    authToken,
    inMemory: false,
  });
}
