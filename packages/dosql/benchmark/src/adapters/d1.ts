/**
 * D1 Benchmark Adapter
 *
 * Implements the BenchmarkAdapter interface for Cloudflare D1.
 * This adapter requires D1 bindings and must be run in a Workers environment.
 */

import {
  type BenchmarkAdapter,
  type BenchmarkOperation,
  type TableSchemaConfig,
  type StorageMetrics,
  type ColdStartMetrics,
} from '../types.js';

// =============================================================================
// D1 Types (simplified for standalone package)
// =============================================================================

/**
 * D1 Database interface (subset of actual D1Database)
 */
export interface D1Database {
  prepare(query: string): D1PreparedStatement;
  batch<T = unknown>(statements: D1PreparedStatement[]): Promise<D1Result<T>[]>;
  exec(query: string): Promise<D1ExecResult>;
}

interface D1PreparedStatement {
  bind(...values: unknown[]): D1PreparedStatement;
  first<T = unknown>(colName?: string): Promise<T | null>;
  run<T = unknown>(): Promise<D1Result<T>>;
  all<T = unknown>(): Promise<D1Result<T>>;
  raw<T = unknown>(): Promise<T[]>;
}

interface D1Result<T = unknown> {
  results?: T[];
  success: boolean;
  error?: string;
  meta: {
    duration: number;
    changes: number;
    last_row_id: number;
    served_by: string;
  };
}

interface D1ExecResult {
  count: number;
  duration: number;
}

// =============================================================================
// D1 Adapter Implementation
// =============================================================================

/**
 * Configuration for D1Adapter
 */
export interface D1AdapterConfig {
  /** D1 database binding */
  database: D1Database;
  /** Enable verbose logging */
  verbose?: boolean;
}

/**
 * D1 benchmark adapter
 *
 * Uses Cloudflare D1 for database operations.
 */
export class D1Adapter implements BenchmarkAdapter {
  readonly name = 'd1';
  readonly version = '1.0.0';

  private db: D1Database;
  private tables: Set<string> = new Set();
  private initialized = false;
  private coldStartTime: number | null = null;

  constructor(config: D1AdapterConfig) {
    this.db = config.database;
  }

  // ===========================================================================
  // Lifecycle Methods
  // ===========================================================================

  async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    const start = performance.now();

    // Warm up the connection with a simple query
    await this.db.prepare('SELECT 1').first();

    this.coldStartTime = performance.now() - start;
    this.initialized = true;
  }

  async cleanup(): Promise<void> {
    // Drop all tables created during benchmark
    for (const tableName of this.tables) {
      try {
        await this.db.exec(`DROP TABLE IF EXISTS ${tableName}`);
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
    await this.db.exec(createSQL);

    // Create indexes
    if (schema.indexes) {
      for (const indexCol of schema.indexes) {
        const indexName = `idx_${schema.tableName}_${indexCol}`;
        await this.db.exec(
          `CREATE INDEX IF NOT EXISTS ${indexName} ON ${schema.tableName}(${indexCol})`
        );
      }
    }

    this.tables.add(schema.tableName);
  }

  async dropTable(tableName: string): Promise<void> {
    await this.db.exec(`DROP TABLE IF EXISTS ${tableName}`);
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
      const result = await this.db.prepare(sql).bind(...values).run();

      const durationMs = performance.now() - startedAt;

      return {
        type: 'create',
        sql,
        params: row,
        durationMs,
        rowCount: result.meta.changes,
        success: result.success,
        error: result.error,
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
      const keys = Object.keys(rows[0]);
      const placeholders = keys.map(() => '?').join(', ');
      const sql = `INSERT INTO ${tableName} (${keys.join(', ')}) VALUES (${placeholders})`;

      const statements = rows.map((row) => {
        const values = Object.values(row);
        return this.db.prepare(sql).bind(...values);
      });

      const results = await this.db.batch(statements);
      const totalChanges = results.reduce((sum, r) => sum + (r.meta?.changes || 0), 0);

      const durationMs = performance.now() - startedAt;

      return {
        type: 'batch',
        sql: `INSERT BATCH (${rows.length} rows)`,
        durationMs,
        rowCount: totalChanges,
        success: results.every((r) => r.success),
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
      const result = await this.db.prepare(sql).bind(primaryKeyValue).all();

      const durationMs = performance.now() - startedAt;

      return {
        type: 'read',
        sql,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: result.results?.length || 0,
        success: result.success,
        error: result.error,
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
      const result = await this.db.prepare(sql).bind(...values).all();

      const durationMs = performance.now() - startedAt;

      return {
        type: 'read',
        sql,
        params,
        durationMs,
        rowCount: result.results?.length || 0,
        success: result.success,
        error: result.error,
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
      const result = await this.db.prepare(sql).bind(...values).run();

      const durationMs = performance.now() - startedAt;

      return {
        type: 'update',
        sql,
        params: { ...updates, [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: result.meta.changes,
        success: result.success,
        error: result.error,
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
      const result = await this.db.prepare(sql).bind(primaryKeyValue).run();

      const durationMs = performance.now() - startedAt;

      return {
        type: 'delete',
        sql,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: result.meta.changes,
        success: result.success,
        error: result.error,
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
      const result = await this.db.prepare(sql).bind(...values).all();

      const durationMs = performance.now() - startedAt;

      return {
        type: 'query',
        sql,
        params,
        durationMs,
        rowCount: result.results?.length || 0,
        success: result.success,
        error: result.error,
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
      // D1 doesn't have explicit transaction support in the same way
      // We use batch operations instead
      const statements: D1PreparedStatement[] = [];

      for (const op of operations) {
        switch (op.type) {
          case 'insert': {
            const keys = Object.keys(op.data);
            const placeholders = keys.map(() => '?').join(', ');
            const sql = `INSERT INTO ${op.tableName} (${keys.join(', ')}) VALUES (${placeholders})`;
            statements.push(this.db.prepare(sql).bind(...Object.values(op.data)));
            break;
          }
          case 'update': {
            const { id, ...updates } = op.data;
            const setClauses = Object.keys(updates)
              .map((key) => `${key} = ?`)
              .join(', ');
            const sql = `UPDATE ${op.tableName} SET ${setClauses} WHERE id = ?`;
            statements.push(this.db.prepare(sql).bind(...Object.values(updates), id));
            break;
          }
          case 'delete': {
            const { id } = op.data;
            const sql = `DELETE FROM ${op.tableName} WHERE id = ?`;
            statements.push(this.db.prepare(sql).bind(id));
            break;
          }
        }
      }

      const results = await this.db.batch(statements);
      rowCount = results.reduce((sum, r) => sum + (r.meta?.changes || 0), 0);

      const durationMs = performance.now() - startedAt;

      return {
        type: 'transaction',
        sql: `TRANSACTION (${operations.length} operations)`,
        durationMs,
        rowCount,
        success: results.every((r) => r.success),
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
    // D1 doesn't expose storage metrics directly
    // We can estimate by counting rows
    let totalRows = 0;

    for (const tableName of this.tables) {
      try {
        const result = await this.db
          .prepare(`SELECT COUNT(*) as count FROM ${tableName}`)
          .first<{ count: number }>();
        if (result) {
          totalRows += result.count;
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
    await this.db.prepare('SELECT 1').first();
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
 * Create a D1 benchmark adapter
 */
export function createD1Adapter(config: D1AdapterConfig): D1Adapter {
  return new D1Adapter(config);
}
