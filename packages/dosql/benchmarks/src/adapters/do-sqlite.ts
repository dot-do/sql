/**
 * Durable Object SQLite Benchmark Adapter
 *
 * Direct DO SQLite adapter using ctx.storage.sql for raw performance benchmarks.
 * Requires Durable Object context in a Cloudflare Worker environment.
 */

import type {
  BenchmarkAdapter,
  BenchmarkOperation,
  TableSchemaConfig,
  StorageMetrics,
  ColdStartMetrics,
} from '../types.js';

// =============================================================================
// DO SQLite Types (from @cloudflare/workers-types)
// =============================================================================

interface SqlStorage {
  exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): SqlStorageCursor<T>;
}

interface SqlStorageCursor<T = Record<string, unknown>> {
  toArray(): T[];
  one(): T | null;
  columnNames: string[];
  rowsRead: number;
  rowsWritten: number;
}

// =============================================================================
// DO-SQLite Adapter Configuration
// =============================================================================

export interface DOSqliteAdapterConfig {
  /** SQL storage from ctx.storage.sql */
  sql: SqlStorage;
  /** Enable verbose logging */
  verbose?: boolean;
}

// =============================================================================
// DO-SQLite Adapter Implementation
// =============================================================================

/**
 * Durable Object SQLite benchmark adapter
 *
 * Uses ctx.storage.sql for direct SQLite access within a Durable Object.
 * This provides the baseline performance for DO-native SQLite operations.
 */
export class DOSqliteAdapter implements BenchmarkAdapter {
  readonly name = 'do-sqlite';
  readonly version = '1.0.0';

  private config: DOSqliteAdapterConfig;
  private sql: SqlStorage;
  private initialized = false;
  private initStartTime: number = 0;
  private firstQueryTime: number | null = null;

  constructor(config: DOSqliteAdapterConfig) {
    this.config = {
      verbose: false,
      ...config,
    };
    this.sql = config.sql;
  }

  // ===========================================================================
  // Lifecycle Methods
  // ===========================================================================

  async initialize(): Promise<void> {
    if (this.initialized) return;

    this.initStartTime = performance.now();

    // DO SQLite is always ready, just verify
    this.sql.exec('SELECT 1').one();

    this.initialized = true;
  }

  async cleanup(): Promise<void> {
    // DO SQLite doesn't need explicit cleanup
    this.initialized = false;
    this.firstQueryTime = null;
  }

  // ===========================================================================
  // Table Operations
  // ===========================================================================

  async createTable(schema: TableSchemaConfig): Promise<void> {
    const columns = schema.columns
      .map((col) => {
        const nullable = col.nullable ? '' : ' NOT NULL';
        const pk = col.name === schema.primaryKey ? ' PRIMARY KEY' : '';
        return `${col.name} ${col.type}${pk}${nullable}`;
      })
      .join(', ');

    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS ${schema.tableName} (${columns})`
    );

    if (schema.indexes) {
      for (const indexCol of schema.indexes) {
        this.sql.exec(
          `CREATE INDEX IF NOT EXISTS idx_${schema.tableName}_${indexCol} ON ${schema.tableName}(${indexCol})`
        );
      }
    }
  }

  async dropTable(tableName: string): Promise<void> {
    this.sql.exec(`DROP TABLE IF EXISTS ${tableName}`);
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
      const columns = Object.keys(row);
      const placeholders = columns.map(() => '?').join(', ');
      const values = columns.map((col) => row[col]);

      this.sql.exec(
        `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`,
        ...values
      );

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'create',
        sql: `INSERT INTO ${tableName} ...`,
        params: row,
        durationMs,
        rowCount: 1,
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

    const startedAt = performance.now();

    try {
      const columns = Object.keys(rows[0]);
      const placeholders = columns.map(() => '?').join(', ');

      // DO SQLite uses implicit transactions for batch operations
      this.sql.exec('BEGIN TRANSACTION');

      for (const row of rows) {
        const values = columns.map((col) => row[col]);
        this.sql.exec(
          `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`,
          ...values
        );
      }

      this.sql.exec('COMMIT');

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'batch',
        sql: `INSERT BATCH (${rows.length} rows)`,
        durationMs,
        rowCount: rows.length,
        success: true,
        startedAt,
      };
    } catch (e) {
      // Rollback on error
      try {
        this.sql.exec('ROLLBACK');
      } catch {
        // Ignore rollback errors
      }

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
      const cursor = this.sql.exec(
        `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?`,
        primaryKeyValue
      );
      const rows = cursor.toArray();

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'read',
        sql: `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: rows.length,
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
      let sql = `SELECT * FROM ${tableName} WHERE ${whereClause}`;
      const values: unknown[] = [];

      if (params) {
        for (const [key, value] of Object.entries(params)) {
          sql = sql.replace(`:${key}`, '?');
          values.push(value);
        }
      }

      const cursor = this.sql.exec(sql, ...values);
      const rows = cursor.toArray();

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'read',
        sql: `SELECT * FROM ${tableName} WHERE ${whereClause}`,
        params,
        durationMs,
        rowCount: rows.length,
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
        .map((col) => `${col} = ?`)
        .join(', ');
      const values = [...Object.values(updates), primaryKeyValue];

      const cursor = this.sql.exec(
        `UPDATE ${tableName} SET ${setClauses} WHERE ${primaryKey} = ?`,
        ...values
      );

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'update',
        sql: `UPDATE ${tableName} SET ... WHERE ${primaryKey} = ?`,
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
      const cursor = this.sql.exec(
        `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`,
        primaryKeyValue
      );

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'delete',
        sql: `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`,
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
      let query = sql;
      const values: unknown[] = [];

      if (params) {
        for (const [key, value] of Object.entries(params)) {
          query = query.replace(`:${key}`, '?');
          values.push(value);
        }
      }

      const cursor = this.sql.exec(query, ...values);
      const isSelect = sql.trim().toUpperCase().startsWith('SELECT');

      let rowCount = 0;
      if (isSelect) {
        const rows = cursor.toArray();
        rowCount = rows.length;
      } else {
        rowCount = cursor.rowsWritten;
      }

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'query',
        sql,
        params,
        durationMs,
        rowCount,
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
      this.sql.exec('BEGIN TRANSACTION');

      for (const op of operations) {
        switch (op.type) {
          case 'insert': {
            const columns = Object.keys(op.data);
            const placeholders = columns.map(() => '?').join(', ');
            const values = columns.map((col) => op.data[col]);
            this.sql.exec(
              `INSERT INTO ${op.tableName} (${columns.join(', ')}) VALUES (${placeholders})`,
              ...values
            );
            rowCount++;
            break;
          }
          case 'update': {
            const { id, ...updates } = op.data;
            const setClauses = Object.keys(updates)
              .map((col) => `${col} = ?`)
              .join(', ');
            const values = [...Object.values(updates), id];
            const cursor = this.sql.exec(
              `UPDATE ${op.tableName} SET ${setClauses} WHERE id = ?`,
              ...values
            );
            rowCount += cursor.rowsWritten;
            break;
          }
          case 'delete': {
            const { id } = op.data;
            const cursor = this.sql.exec(
              `DELETE FROM ${op.tableName} WHERE id = ?`,
              id
            );
            rowCount += cursor.rowsWritten;
            break;
          }
        }
      }

      this.sql.exec('COMMIT');

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'transaction',
        sql: `TRANSACTION (${operations.length} operations)`,
        durationMs,
        rowCount,
        success: true,
        startedAt,
      };
    } catch (e) {
      // Rollback on error
      try {
        this.sql.exec('ROLLBACK');
      } catch {
        // Ignore rollback errors
      }

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
    try {
      const pageCount = this.sql
        .exec<{ page_count: number }>('PRAGMA page_count')
        .one();
      const pageSize = this.sql
        .exec<{ page_size: number }>('PRAGMA page_size')
        .one();
      const totalBytes =
        (pageCount?.page_count ?? 0) * (pageSize?.page_size ?? 4096);

      const tables = this.sql
        .exec<{ name: string }>(
          "SELECT name FROM sqlite_master WHERE type='table'"
        )
        .toArray();

      let totalRows = 0;
      for (const table of tables) {
        const count = this.sql
          .exec<{ count: number }>(`SELECT COUNT(*) as count FROM ${table.name}`)
          .one();
        totalRows += count?.count ?? 0;
      }

      // DO SQLite has a 1GB limit
      const limitBytes = 1024 * 1024 * 1024;
      const limitUtilization = totalBytes / limitBytes;

      return {
        totalBytes,
        rowCount: totalRows,
        tableCount: tables.length,
        limitUtilization,
      };
    } catch {
      return { totalBytes: 0, rowCount: 0, tableCount: 0, limitUtilization: 0 };
    }
  }

  async measureColdStart(): Promise<ColdStartMetrics> {
    const initTime =
      this.initStartTime > 0 ? performance.now() - this.initStartTime : 0;

    return {
      timeToFirstQuery: this.firstQueryTime ?? initTime,
      initializationTime: initTime,
      connectionTime: 0, // DO SQLite is always connected within DO
    };
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  private recordFirstQuery(durationMs: number): void {
    if (this.firstQueryTime === null) {
      this.firstQueryTime = durationMs;
    }
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a DO SQLite benchmark adapter
 */
export function createDOSqliteAdapter(
  config: DOSqliteAdapterConfig
): DOSqliteAdapter {
  return new DOSqliteAdapter(config);
}
