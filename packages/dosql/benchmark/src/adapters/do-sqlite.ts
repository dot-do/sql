/**
 * DO SQLite Benchmark Adapter
 *
 * Implements the BenchmarkAdapter interface for raw Durable Object SQLite.
 * This adapter uses the native DO SQLite API directly without DoSQL wrapper.
 */

import {
  type BenchmarkAdapter,
  type BenchmarkOperation,
  type TableSchemaConfig,
  type StorageMetrics,
  type ColdStartMetrics,
} from '../types.js';

// =============================================================================
// DO SQLite Types (simplified for standalone package)
// =============================================================================

/**
 * SqlStorage interface (subset of actual DurableObjectStorage)
 */
export interface SqlStorage {
  sql: SqlExecutor;
}

export interface SqlExecutor {
  exec(query: string, ...bindings: unknown[]): SqlCursor;
}

export interface SqlCursor {
  toArray(): Record<string, unknown>[];
  columnNames: string[];
  rowsRead: number;
  rowsWritten: number;
}

// =============================================================================
// DO SQLite Adapter Implementation
// =============================================================================

/**
 * Configuration for DOSqliteAdapter
 */
export interface DOSqliteAdapterConfig {
  /** Durable Object storage with SQL access */
  storage: SqlStorage;
  /** Enable verbose logging */
  verbose?: boolean;
}

/**
 * DO SQLite benchmark adapter
 *
 * Uses raw Durable Object SQLite API for baseline comparison.
 */
export class DOSqliteAdapter implements BenchmarkAdapter {
  readonly name = 'do-sqlite';
  readonly version = '1.0.0';

  private storage: SqlStorage;
  private tables: Set<string> = new Set();
  private initialized = false;
  private coldStartTime: number | null = null;

  constructor(config: DOSqliteAdapterConfig) {
    this.storage = config.storage;
  }

  // ===========================================================================
  // Lifecycle Methods
  // ===========================================================================

  async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    const start = performance.now();

    // Warm up with a simple query
    this.storage.sql.exec('SELECT 1');

    this.coldStartTime = performance.now() - start;
    this.initialized = true;
  }

  async cleanup(): Promise<void> {
    // Drop all tables created during benchmark
    for (const tableName of this.tables) {
      try {
        this.storage.sql.exec(`DROP TABLE IF EXISTS ${tableName}`);
      } catch {
        // Ignore errors during cleanup
      }
    }
    this.tables.clear();
    this.initialized = false;
    this.coldStartTime = null;
  }

  // ===========================================================================
  // Table Operations
  // ===========================================================================

  async createTable(schema: TableSchemaConfig): Promise<void> {
    const columns = schema.columns.map((col) => {
      const nullable = col.nullable ? '' : ' NOT NULL';
      const pk = col.name === schema.primaryKey ? ' PRIMARY KEY' : '';
      return `${col.name} ${col.type}${nullable}${pk}`;
    });

    const createSQL = `CREATE TABLE IF NOT EXISTS ${schema.tableName} (${columns.join(', ')})`;
    this.storage.sql.exec(createSQL);

    // Create indexes
    if (schema.indexes) {
      for (const indexCol of schema.indexes) {
        const indexName = `idx_${schema.tableName}_${indexCol}`;
        this.storage.sql.exec(
          `CREATE INDEX IF NOT EXISTS ${indexName} ON ${schema.tableName}(${indexCol})`
        );
      }
    }

    this.tables.add(schema.tableName);
  }

  async dropTable(tableName: string): Promise<void> {
    this.storage.sql.exec(`DROP TABLE IF EXISTS ${tableName}`);
    this.tables.delete(tableName);
  }

  // ===========================================================================
  // CRUD Operations
  // ===========================================================================

  async insert(
    tableName: string,
    row: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();

    try {
      const keys = Object.keys(row);
      const placeholders = keys.map(() => '?').join(', ');
      const values = Object.values(row);

      const sql = `INSERT INTO ${tableName} (${keys.join(', ')}) VALUES (${placeholders})`;
      const cursor = this.storage.sql.exec(sql, ...values);

      const durationMs = performance.now() - startedAt;

      return {
        type: 'create',
        sql,
        params: row,
        durationMs,
        rowCount: cursor.rowsWritten,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'create',
        sql: `INSERT INTO ${tableName} ...`,
        params: row,
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  async insertBatch(
    tableName: string,
    rows: Record<string, unknown>[]
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();

    if (rows.length === 0) {
      return {
        type: 'batch',
        sql: '',
        durationMs: 0,
        rowCount: 0,
        success: true,
        startedAt,
      };
    }

    try {
      let totalWritten = 0;
      const keys = Object.keys(rows[0]);
      const placeholders = keys.map(() => '?').join(', ');
      const sql = `INSERT INTO ${tableName} (${keys.join(', ')}) VALUES (${placeholders})`;

      for (const row of rows) {
        const values = Object.values(row);
        const cursor = this.storage.sql.exec(sql, ...values);
        totalWritten += cursor.rowsWritten;
      }

      const durationMs = performance.now() - startedAt;

      return {
        type: 'batch',
        sql: `INSERT BATCH (${rows.length} rows)`,
        durationMs,
        rowCount: totalWritten,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'batch',
        sql: `INSERT BATCH (${rows.length} rows)`,
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  async read(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();

    try {
      const sql = `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?`;
      const cursor = this.storage.sql.exec(sql, primaryKeyValue);
      const results = cursor.toArray();

      const durationMs = performance.now() - startedAt;

      return {
        type: 'read',
        sql,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: results.length,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'read',
        sql: `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  async readMany(
    tableName: string,
    whereClause: string,
    params?: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();

    try {
      const sql = `SELECT * FROM ${tableName} WHERE ${whereClause}`;
      const values = params ? Object.values(params) : [];
      const cursor = this.storage.sql.exec(sql, ...values);
      const results = cursor.toArray();

      const durationMs = performance.now() - startedAt;

      return {
        type: 'read',
        sql,
        params,
        durationMs,
        rowCount: results.length,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'read',
        sql: `SELECT * FROM ${tableName} WHERE ${whereClause}`,
        params,
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  async update(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown,
    updates: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();

    try {
      const setClauses = Object.keys(updates)
        .map((key) => `${key} = ?`)
        .join(', ');
      const values = [...Object.values(updates), primaryKeyValue];

      const sql = `UPDATE ${tableName} SET ${setClauses} WHERE ${primaryKey} = ?`;
      const cursor = this.storage.sql.exec(sql, ...values);

      const durationMs = performance.now() - startedAt;

      return {
        type: 'update',
        sql,
        params: { ...updates, [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: cursor.rowsWritten,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'update',
        sql: `UPDATE ${tableName} SET ... WHERE ${primaryKey} = ?`,
        params: { ...updates, [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  async delete(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();

    try {
      const sql = `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`;
      const cursor = this.storage.sql.exec(sql, primaryKeyValue);

      const durationMs = performance.now() - startedAt;

      return {
        type: 'delete',
        sql,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: cursor.rowsWritten,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'delete',
        sql: `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  async query(
    sql: string,
    params?: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();

    try {
      const values = params ? Object.values(params) : [];
      const cursor = this.storage.sql.exec(sql, ...values);
      const results = cursor.toArray();

      const durationMs = performance.now() - startedAt;

      return {
        type: 'query',
        sql,
        params,
        durationMs,
        rowCount: results.length,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'query',
        sql,
        params,
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  async transaction(
    operations: Array<{
      type: 'insert' | 'update' | 'delete';
      tableName: string;
      data: Record<string, unknown>;
    }>
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();
    let rowCount = 0;

    try {
      // DO SQLite supports implicit transactions for each exec call
      // For explicit transactions, we would use BEGIN/COMMIT
      // Note: In Workers runtime, each storage operation is already transactional

      for (const op of operations) {
        switch (op.type) {
          case 'insert': {
            const result = await this.insert(op.tableName, op.data);
            if (!result.success) {
              throw new Error(result.error || 'Insert failed');
            }
            rowCount += result.rowCount;
            break;
          }
          case 'update': {
            const { id, ...updates } = op.data;
            const result = await this.update(op.tableName, 'id', id, updates);
            if (!result.success) {
              throw new Error(result.error || 'Update failed');
            }
            rowCount += result.rowCount;
            break;
          }
          case 'delete': {
            const { id } = op.data;
            const result = await this.delete(op.tableName, 'id', id);
            if (!result.success) {
              throw new Error(result.error || 'Delete failed');
            }
            rowCount += result.rowCount;
            break;
          }
        }
      }

      const durationMs = performance.now() - startedAt;

      return {
        type: 'transaction',
        sql: `TRANSACTION (${operations.length} operations)`,
        durationMs,
        rowCount,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'transaction',
        sql: `TRANSACTION (${operations.length} operations)`,
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  // ===========================================================================
  // Metrics
  // ===========================================================================

  async getStorageMetrics(): Promise<StorageMetrics> {
    let totalRows = 0;

    for (const tableName of this.tables) {
      try {
        const cursor = this.storage.sql.exec(
          `SELECT COUNT(*) as count FROM ${tableName}`
        );
        const results = cursor.toArray();
        if (results.length > 0) {
          totalRows += (results[0].count as number) || 0;
        }
      } catch {
        // Ignore errors
      }
    }

    return {
      totalBytes: totalRows * 100, // Rough estimate
      rowCount: totalRows,
      tableCount: this.tables.size,
      limitUtilization: 0,
    };
  }

  async measureColdStart(): Promise<ColdStartMetrics> {
    if (this.coldStartTime !== null) {
      return {
        timeToFirstQuery: this.coldStartTime,
        initializationTime: 0,
        connectionTime: this.coldStartTime,
      };
    }

    const start = performance.now();
    this.storage.sql.exec('SELECT 1');
    const queryTime = performance.now() - start;

    return {
      timeToFirstQuery: queryTime,
      initializationTime: 0,
      connectionTime: queryTime,
    };
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a DO SQLite benchmark adapter
 */
export function createDOSqliteAdapter(config: DOSqliteAdapterConfig): DOSqliteAdapter {
  return new DOSqliteAdapter(config);
}
