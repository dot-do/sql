/**
 * Cloudflare D1 Benchmark Adapter
 *
 * D1 adapter for Cloudflare's serverless SQLite database.
 * Requires D1 binding in a Cloudflare Worker environment.
 */

import type {
  BenchmarkAdapter,
  BenchmarkOperation,
  TableSchemaConfig,
  StorageMetrics,
  ColdStartMetrics,
} from '../types.js';

// =============================================================================
// D1 Types (from @cloudflare/workers-types)
// =============================================================================

interface D1Database {
  prepare(query: string): D1PreparedStatement;
  batch<T = unknown>(statements: D1PreparedStatement[]): Promise<D1Result<T>[]>;
  exec(query: string): Promise<D1ExecResult>;
}

interface D1PreparedStatement {
  bind(...values: unknown[]): D1PreparedStatement;
  first<T = unknown>(colName?: string): Promise<T | null>;
  run(): Promise<D1Result>;
  all<T = unknown>(): Promise<D1Result<T>>;
  raw<T = unknown>(): Promise<T[]>;
}

interface D1Result<T = unknown> {
  results: T[];
  success: boolean;
  meta: {
    changed_db?: boolean;
    changes?: number;
    last_row_id?: number;
    duration?: number;
    rows_read?: number;
    rows_written?: number;
  };
}

interface D1ExecResult {
  count: number;
  duration: number;
}

// =============================================================================
// D1 Adapter Configuration
// =============================================================================

export interface D1AdapterConfig {
  /** D1 database binding */
  db: D1Database;
  /** Enable verbose logging */
  verbose?: boolean;
}

// =============================================================================
// D1 Adapter Implementation
// =============================================================================

/**
 * Cloudflare D1 benchmark adapter
 *
 * Uses D1 database binding for benchmarking.
 * Must be used within a Cloudflare Worker environment.
 */
export class D1Adapter implements BenchmarkAdapter {
  readonly name = 'd1';
  readonly version = '1.0.0';

  private config: D1AdapterConfig;
  private db: D1Database;
  private initialized = false;
  private initStartTime: number = 0;
  private firstQueryTime: number | null = null;

  constructor(config: D1AdapterConfig) {
    this.config = {
      verbose: false,
      ...config,
    };
    this.db = config.db;
  }

  // ===========================================================================
  // Lifecycle Methods
  // ===========================================================================

  async initialize(): Promise<void> {
    if (this.initialized) return;

    this.initStartTime = performance.now();

    // D1 is already connected via binding, just verify
    await this.db.prepare('SELECT 1').first();

    this.initialized = true;
  }

  async cleanup(): Promise<void> {
    // D1 bindings don't need explicit cleanup
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

    await this.db.exec(
      `CREATE TABLE IF NOT EXISTS ${schema.tableName} (${columns})`
    );

    if (schema.indexes) {
      for (const indexCol of schema.indexes) {
        await this.db.exec(
          `CREATE INDEX IF NOT EXISTS idx_${schema.tableName}_${indexCol} ON ${schema.tableName}(${indexCol})`
        );
      }
    }
  }

  async dropTable(tableName: string): Promise<void> {
    await this.db.exec(`DROP TABLE IF EXISTS ${tableName}`);
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

      const stmt = this.db
        .prepare(
          `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`
        )
        .bind(...values);

      await stmt.run();

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

      const statements = rows.map((row) => {
        const values = columns.map((col) => row[col]);
        return this.db
          .prepare(
            `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`
          )
          .bind(...values);
      });

      await this.db.batch(statements);

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
    const startedAt = performance.now();

    try {
      const result = await this.db
        .prepare(`SELECT * FROM ${tableName} WHERE ${primaryKey} = ?`)
        .bind(primaryKeyValue)
        .all();

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'read',
        sql: `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: result.results.length,
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

      const stmt = this.db.prepare(sql).bind(...values);
      const result = await stmt.all();

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'read',
        sql: `SELECT * FROM ${tableName} WHERE ${whereClause}`,
        params,
        durationMs,
        rowCount: result.results.length,
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

      const result = await this.db
        .prepare(
          `UPDATE ${tableName} SET ${setClauses} WHERE ${primaryKey} = ?`
        )
        .bind(...values)
        .run();

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'update',
        sql: `UPDATE ${tableName} SET ... WHERE ${primaryKey} = ?`,
        params: { ...updates, [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: result.meta.changes ?? 0,
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
      const result = await this.db
        .prepare(`DELETE FROM ${tableName} WHERE ${primaryKey} = ?`)
        .bind(primaryKeyValue)
        .run();

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'delete',
        sql: `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: result.meta.changes ?? 0,
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

      const isSelect = sql.trim().toUpperCase().startsWith('SELECT');
      const stmt = this.db.prepare(query).bind(...values);

      let rowCount = 0;
      if (isSelect) {
        const result = await stmt.all();
        rowCount = result.results.length;
      } else {
        const result = await stmt.run();
        rowCount = result.meta.changes ?? 0;
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
      const statements: D1PreparedStatement[] = [];

      for (const op of operations) {
        switch (op.type) {
          case 'insert': {
            const columns = Object.keys(op.data);
            const placeholders = columns.map(() => '?').join(', ');
            const values = columns.map((col) => op.data[col]);
            statements.push(
              this.db
                .prepare(
                  `INSERT INTO ${op.tableName} (${columns.join(', ')}) VALUES (${placeholders})`
                )
                .bind(...values)
            );
            break;
          }
          case 'update': {
            const { id, ...updates } = op.data;
            const setClauses = Object.keys(updates)
              .map((col) => `${col} = ?`)
              .join(', ');
            const values = [...Object.values(updates), id];
            statements.push(
              this.db
                .prepare(`UPDATE ${op.tableName} SET ${setClauses} WHERE id = ?`)
                .bind(...values)
            );
            break;
          }
          case 'delete': {
            const { id } = op.data;
            statements.push(
              this.db
                .prepare(`DELETE FROM ${op.tableName} WHERE id = ?`)
                .bind(id)
            );
            break;
          }
        }
      }

      const results = await this.db.batch(statements);
      for (const result of results) {
        rowCount += result.meta.changes ?? 0;
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
    try {
      const tables = await this.db
        .prepare("SELECT name FROM sqlite_master WHERE type='table'")
        .all<{ name: string }>();

      let totalRows = 0;
      for (const table of tables.results) {
        const count = await this.db
          .prepare(`SELECT COUNT(*) as count FROM ${table.name}`)
          .first<{ count: number }>();
        totalRows += count?.count ?? 0;
      }

      // D1 storage is managed by Cloudflare, estimate 100 bytes per row
      return {
        totalBytes: totalRows * 100,
        rowCount: totalRows,
        tableCount: tables.results.length,
        limitUtilization: 0, // D1 has 10GB limit
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
      connectionTime: 0, // D1 is always connected in Workers
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
 * Create a D1 benchmark adapter
 */
export function createD1Adapter(config: D1AdapterConfig): D1Adapter {
  return new D1Adapter(config);
}
