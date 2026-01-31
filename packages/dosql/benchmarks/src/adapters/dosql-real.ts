/**
 * DoSQL REAL Benchmark Adapter
 *
 * Uses the ACTUAL DoSQL Database class from src/database.ts
 * instead of a simulated in-memory store.
 *
 * This gives accurate performance measurements of the real engine.
 */

import type {
  BenchmarkAdapter,
  BenchmarkOperation,
  TableSchemaConfig,
  StorageMetrics,
  ColdStartMetrics,
} from '../types.js';

// Import the REAL DoSQL Database
import { Database, type SqlValue } from '../../../src/database.js';

// =============================================================================
// DoSQL Real Adapter Configuration
// =============================================================================

export interface DoSQLRealAdapterConfig {
  /** Enable verbose logging */
  verbose?: boolean;
}

// =============================================================================
// DoSQL Real Adapter Implementation
// =============================================================================

/**
 * DoSQL benchmark adapter using the REAL DoSQL engine
 *
 * This uses the actual Database class from src/database.ts
 * for accurate performance benchmarking.
 */
export class DoSQLRealAdapter implements BenchmarkAdapter {
  readonly name = 'dosql-real';
  readonly version = '1.0.0';

  private db: Database | null = null;
  private initialized = false;
  private initStartTime: number = 0;
  private firstQueryTime: number | null = null;
  private config: DoSQLRealAdapterConfig;

  constructor(config: DoSQLRealAdapterConfig = {}) {
    this.config = config;
  }

  // ===========================================================================
  // Lifecycle Methods
  // ===========================================================================

  async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    this.initStartTime = performance.now();

    // Create a new in-memory database using the REAL DoSQL Database class
    this.db = new Database(':memory:', {
      verbose: this.config.verbose ? console.log : undefined,
    });

    this.initialized = true;
  }

  async cleanup(): Promise<void> {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
    this.initialized = false;
    this.firstQueryTime = null;
  }

  // ===========================================================================
  // Table Operations
  // ===========================================================================

  async createTable(schema: TableSchemaConfig): Promise<void> {
    if (!this.db) throw new Error('Database not initialized');

    const columns = schema.columns
      .map((col) => {
        const nullable = col.nullable ? '' : ' NOT NULL';
        const pk = col.name === schema.primaryKey ? ' PRIMARY KEY' : '';
        return `${col.name} ${col.type}${pk}${nullable}`;
      })
      .join(', ');

    this.db.exec(`CREATE TABLE IF NOT EXISTS ${schema.tableName} (${columns})`);

    // Create indexes
    if (schema.indexes) {
      for (const indexCol of schema.indexes) {
        this.db.exec(
          `CREATE INDEX IF NOT EXISTS idx_${schema.tableName}_${indexCol} ON ${schema.tableName}(${indexCol})`
        );
      }
    }
  }

  async dropTable(_tableName: string): Promise<void> {
    // The DoSQL in-memory engine doesn't support DROP TABLE
    // This is a no-op - we'll use unique table names in benchmarks
    // to avoid conflicts between tests
  }

  // ===========================================================================
  // CRUD Operations
  // ===========================================================================

  async insert(
    tableName: string,
    row: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    if (!this.db) throw new Error('Database not initialized');

    const startedAt = performance.now();

    try {
      const columns = Object.keys(row);
      const placeholders = columns.map(() => '?').join(', ');
      const values = columns.map((col) => row[col] as SqlValue);

      const stmt = this.db.prepare(
        `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`
      );
      stmt.run(...values);

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
    if (!this.db) throw new Error('Database not initialized');
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

      const stmt = this.db.prepare(
        `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`
      );

      // Use DoSQL's transaction for batch inserts
      const insertMany = this.db.transaction((rows: Record<string, unknown>[]) => {
        for (const row of rows) {
          const values = columns.map((col) => row[col] as SqlValue);
          stmt.run(...values);
        }
      });

      insertMany(rows);

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
    if (!this.db) throw new Error('Database not initialized');

    const startedAt = performance.now();

    try {
      const stmt = this.db.prepare(
        `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?`
      );
      const rows = stmt.all(primaryKeyValue as SqlValue);

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
    if (!this.db) throw new Error('Database not initialized');

    const startedAt = performance.now();

    try {
      let sql = `SELECT * FROM ${tableName} WHERE ${whereClause}`;
      const values: SqlValue[] = [];

      if (params) {
        for (const [key, value] of Object.entries(params)) {
          sql = sql.replace(`:${key}`, '?');
          values.push(value as SqlValue);
        }
      }

      const stmt = this.db.prepare(sql);
      const rows = stmt.all(...values);

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
    if (!this.db) throw new Error('Database not initialized');

    const startedAt = performance.now();

    try {
      const setClauses = Object.keys(updates)
        .map((col) => `${col} = ?`)
        .join(', ');
      const values = [...Object.values(updates), primaryKeyValue] as SqlValue[];

      const stmt = this.db.prepare(
        `UPDATE ${tableName} SET ${setClauses} WHERE ${primaryKey} = ?`
      );
      const result = stmt.run(...values);

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'update',
        sql: `UPDATE ${tableName} SET ... WHERE ${primaryKey} = ?`,
        params: { ...updates, [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: result.changes ?? 0,
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
    if (!this.db) throw new Error('Database not initialized');

    const startedAt = performance.now();

    try {
      const stmt = this.db.prepare(
        `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`
      );
      const result = stmt.run(primaryKeyValue as SqlValue);

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'delete',
        sql: `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: result.changes ?? 0,
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
    if (!this.db) throw new Error('Database not initialized');

    const startedAt = performance.now();

    try {
      let query = sql;
      const values: SqlValue[] = [];

      if (params) {
        for (const [key, value] of Object.entries(params)) {
          query = query.replace(`:${key}`, '?');
          values.push(value as SqlValue);
        }
      }

      const stmt = this.db.prepare(query);
      const isSelect = sql.trim().toUpperCase().startsWith('SELECT');

      let rowCount = 0;
      if (isSelect) {
        const rows = stmt.all(...values);
        rowCount = rows.length;
      } else {
        const result = stmt.run(...values);
        rowCount = result.changes ?? 0;
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
    if (!this.db) throw new Error('Database not initialized');

    const startedAt = performance.now();
    let rowCount = 0;

    try {
      const txn = this.db.transaction(() => {
        for (const op of operations) {
          switch (op.type) {
            case 'insert': {
              const columns = Object.keys(op.data);
              const placeholders = columns.map(() => '?').join(', ');
              const values = columns.map((col) => op.data[col] as SqlValue);
              const stmt = this.db!.prepare(
                `INSERT INTO ${op.tableName} (${columns.join(', ')}) VALUES (${placeholders})`
              );
              stmt.run(...values);
              rowCount++;
              break;
            }
            case 'update': {
              const { id, ...updates } = op.data;
              const setClauses = Object.keys(updates)
                .map((col) => `${col} = ?`)
                .join(', ');
              const values = [...Object.values(updates), id] as SqlValue[];
              const stmt = this.db!.prepare(
                `UPDATE ${op.tableName} SET ${setClauses} WHERE id = ?`
              );
              const result = stmt.run(...values);
              rowCount += result.changes ?? 0;
              break;
            }
            case 'delete': {
              const { id } = op.data;
              const stmt = this.db!.prepare(
                `DELETE FROM ${op.tableName} WHERE id = ?`
              );
              const result = stmt.run(id as SqlValue);
              rowCount += result.changes ?? 0;
              break;
            }
          }
        }
      });

      txn();

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
    if (!this.db) {
      return { totalBytes: 0, rowCount: 0, tableCount: 0, limitUtilization: 0 };
    }

    const tables = this.db.getTables();
    let totalRows = 0;

    for (const tableName of tables) {
      try {
        const stmt = this.db.prepare(`SELECT COUNT(*) as count FROM ${tableName}`);
        const result = stmt.all() as Array<{ count: number }>;
        totalRows += result[0]?.count ?? 0;
      } catch {
        // Ignore errors for individual tables
      }
    }

    // Estimate storage (rough estimate since in-memory)
    const estimatedBytes = totalRows * 100;

    return {
      totalBytes: estimatedBytes,
      rowCount: totalRows,
      tableCount: tables.length,
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
 * Create a DoSQL REAL benchmark adapter
 */
export function createDoSQLRealAdapter(
  config: DoSQLRealAdapterConfig = {}
): DoSQLRealAdapter {
  return new DoSQLRealAdapter(config);
}
