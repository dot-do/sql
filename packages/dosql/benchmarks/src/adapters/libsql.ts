/**
 * LibSQL Benchmark Adapter
 *
 * LibSQL adapter for local embedded SQLite with extended features.
 * This tests the libsql runtime without network overhead.
 */

import type {
  BenchmarkAdapter,
  BenchmarkOperation,
  TableSchemaConfig,
  StorageMetrics,
  ColdStartMetrics,
} from '../types.js';

// =============================================================================
// LibSQL Adapter Configuration
// =============================================================================

export interface LibSQLAdapterConfig {
  /** Database URL (file:path or :memory:) */
  url?: string;
  /** Enable verbose logging */
  verbose?: boolean;
}

// =============================================================================
// LibSQL Adapter Implementation
// =============================================================================

/**
 * LibSQL benchmark adapter (local embedded mode)
 *
 * Uses @libsql/client in local/embedded mode for benchmarking.
 */
export class LibSQLAdapter implements BenchmarkAdapter {
  readonly name = 'libsql';
  readonly version = '1.0.0';

  private config: LibSQLAdapterConfig;
  private client: import('@libsql/client').Client | null = null;
  private initialized = false;
  private initStartTime: number = 0;
  private firstQueryTime: number | null = null;

  constructor(config: LibSQLAdapterConfig = {}) {
    this.config = {
      url: ':memory:',
      verbose: false,
      ...config,
    };
  }

  // ===========================================================================
  // Lifecycle Methods
  // ===========================================================================

  async initialize(): Promise<void> {
    if (this.initialized) return;

    this.initStartTime = performance.now();

    const { createClient } = await import('@libsql/client');
    this.client = createClient({
      url: this.config.url!,
    });

    // Optimize for benchmarks
    await this.client.execute('PRAGMA journal_mode = WAL');
    await this.client.execute('PRAGMA synchronous = OFF');
    await this.client.execute('PRAGMA temp_store = MEMORY');
    await this.client.execute('PRAGMA cache_size = -64000');

    this.initialized = true;
  }

  async cleanup(): Promise<void> {
    if (this.client) {
      this.client.close();
      this.client = null;
    }
    this.initialized = false;
    this.firstQueryTime = null;
  }

  // ===========================================================================
  // Table Operations
  // ===========================================================================

  async createTable(schema: TableSchemaConfig): Promise<void> {
    if (!this.client) throw new Error('Database not initialized');

    const columns = schema.columns
      .map((col) => {
        const nullable = col.nullable ? '' : ' NOT NULL';
        const pk = col.name === schema.primaryKey ? ' PRIMARY KEY' : '';
        return `${col.name} ${col.type}${pk}${nullable}`;
      })
      .join(', ');

    await this.client.execute(
      `CREATE TABLE IF NOT EXISTS ${schema.tableName} (${columns})`
    );

    if (schema.indexes) {
      for (const indexCol of schema.indexes) {
        await this.client.execute(
          `CREATE INDEX IF NOT EXISTS idx_${schema.tableName}_${indexCol} ON ${schema.tableName}(${indexCol})`
        );
      }
    }
  }

  async dropTable(tableName: string): Promise<void> {
    if (!this.client) throw new Error('Database not initialized');
    await this.client.execute(`DROP TABLE IF EXISTS ${tableName}`);
  }

  // ===========================================================================
  // CRUD Operations
  // ===========================================================================

  async insert(
    tableName: string,
    row: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    if (!this.client) throw new Error('Database not initialized');

    const startedAt = performance.now();

    try {
      const columns = Object.keys(row);
      const placeholders = columns.map((_, i) => `?${i + 1}`).join(', ');
      const args = columns.map((col) => row[col] as import('@libsql/client').Value);

      await this.client.execute({
        sql: `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`,
        args,
      });

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
    if (!this.client) throw new Error('Database not initialized');
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
      const placeholders = columns.map((_, i) => `?${i + 1}`).join(', ');

      const statements = rows.map((row) => ({
        sql: `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`,
        args: columns.map((col) => row[col] as import('@libsql/client').Value),
      }));

      await this.client.batch(statements, 'write');

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
    if (!this.client) throw new Error('Database not initialized');

    const startedAt = performance.now();

    try {
      const result = await this.client.execute({
        sql: `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?1`,
        args: [primaryKeyValue as import('@libsql/client').Value],
      });

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'read',
        sql: `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: result.rows.length,
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
    if (!this.client) throw new Error('Database not initialized');

    const startedAt = performance.now();

    try {
      let sql = `SELECT * FROM ${tableName} WHERE ${whereClause}`;
      const args: import('@libsql/client').Value[] = [];

      if (params) {
        let idx = 1;
        for (const [key, value] of Object.entries(params)) {
          sql = sql.replace(`:${key}`, `?${idx}`);
          args.push(value as import('@libsql/client').Value);
          idx++;
        }
      }

      const result = await this.client.execute({ sql, args });

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'read',
        sql: `SELECT * FROM ${tableName} WHERE ${whereClause}`,
        params,
        durationMs,
        rowCount: result.rows.length,
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
    if (!this.client) throw new Error('Database not initialized');

    const startedAt = performance.now();

    try {
      const updateKeys = Object.keys(updates);
      const setClauses = updateKeys.map((col, i) => `${col} = ?${i + 1}`).join(', ');
      const args = [...Object.values(updates), primaryKeyValue] as import('@libsql/client').Value[];

      const result = await this.client.execute({
        sql: `UPDATE ${tableName} SET ${setClauses} WHERE ${primaryKey} = ?${updateKeys.length + 1}`,
        args,
      });

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'update',
        sql: `UPDATE ${tableName} SET ... WHERE ${primaryKey} = ?`,
        params: { ...updates, [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: result.rowsAffected,
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
    if (!this.client) throw new Error('Database not initialized');

    const startedAt = performance.now();

    try {
      const result = await this.client.execute({
        sql: `DELETE FROM ${tableName} WHERE ${primaryKey} = ?1`,
        args: [primaryKeyValue as import('@libsql/client').Value],
      });

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'delete',
        sql: `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: result.rowsAffected,
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
    if (!this.client) throw new Error('Database not initialized');

    const startedAt = performance.now();

    try {
      let query = sql;
      const args: import('@libsql/client').Value[] = [];

      if (params) {
        let idx = 1;
        for (const [key, value] of Object.entries(params)) {
          query = query.replace(`:${key}`, `?${idx}`);
          args.push(value as import('@libsql/client').Value);
          idx++;
        }
      }

      const result = await this.client.execute({ sql: query, args });

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'query',
        sql,
        params,
        durationMs,
        rowCount: result.rows.length || result.rowsAffected,
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
    if (!this.client) throw new Error('Database not initialized');

    const startedAt = performance.now();
    let rowCount = 0;

    try {
      const statements: import('@libsql/client').InStatement[] = [];

      for (const op of operations) {
        switch (op.type) {
          case 'insert': {
            const columns = Object.keys(op.data);
            const placeholders = columns.map((_, i) => `?${i + 1}`).join(', ');
            statements.push({
              sql: `INSERT INTO ${op.tableName} (${columns.join(', ')}) VALUES (${placeholders})`,
              args: columns.map((col) => op.data[col] as import('@libsql/client').Value),
            });
            break;
          }
          case 'update': {
            const { id, ...updates } = op.data;
            const updateKeys = Object.keys(updates);
            const setClauses = updateKeys.map((col, i) => `${col} = ?${i + 1}`).join(', ');
            statements.push({
              sql: `UPDATE ${op.tableName} SET ${setClauses} WHERE id = ?${updateKeys.length + 1}`,
              args: [...Object.values(updates), id] as import('@libsql/client').Value[],
            });
            break;
          }
          case 'delete': {
            const { id } = op.data;
            statements.push({
              sql: `DELETE FROM ${op.tableName} WHERE id = ?1`,
              args: [id as import('@libsql/client').Value],
            });
            break;
          }
        }
      }

      const results = await this.client.batch(statements, 'write');
      for (const result of results) {
        rowCount += result.rowsAffected;
      }

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
    if (!this.client) {
      return { totalBytes: 0, rowCount: 0, tableCount: 0, limitUtilization: 0 };
    }

    const pageCount = await this.client.execute('PRAGMA page_count');
    const pageSize = await this.client.execute('PRAGMA page_size');
    const totalBytes =
      (pageCount.rows[0]?.[0] as number) * (pageSize.rows[0]?.[0] as number);

    const tables = await this.client.execute(
      "SELECT name FROM sqlite_master WHERE type='table'"
    );

    let totalRows = 0;
    for (const row of tables.rows) {
      const tableName = row[0] as string;
      const count = await this.client.execute(`SELECT COUNT(*) FROM ${tableName}`);
      totalRows += count.rows[0]?.[0] as number;
    }

    return {
      totalBytes,
      rowCount: totalRows,
      tableCount: tables.rows.length,
      limitUtilization: 0,
    };
  }

  async measureColdStart(): Promise<ColdStartMetrics> {
    const initTime =
      this.initStartTime > 0 ? performance.now() - this.initStartTime : 0;

    return {
      timeToFirstQuery: this.firstQueryTime ?? initTime,
      initializationTime: initTime,
      connectionTime: 0,
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
 * Create a LibSQL benchmark adapter
 */
export function createLibSQLAdapter(
  config: LibSQLAdapterConfig = {}
): LibSQLAdapter {
  return new LibSQLAdapter(config);
}
