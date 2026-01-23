/**
 * Better-SQLite3 Benchmark Adapter
 *
 * Native SQLite adapter using better-sqlite3 for Node.js benchmarks.
 * This serves as the baseline for raw SQLite performance.
 */

import type {
  BenchmarkAdapter,
  BenchmarkOperation,
  TableSchemaConfig,
  StorageMetrics,
  ColdStartMetrics,
} from '../types.js';

// =============================================================================
// Better-SQLite3 Adapter Configuration
// =============================================================================

export interface BetterSQLite3AdapterConfig {
  /** Path to database file (use ':memory:' for in-memory) */
  dbPath?: string;
  /** Enable verbose logging */
  verbose?: boolean;
  /** Enable WAL mode */
  walMode?: boolean;
}

// =============================================================================
// Better-SQLite3 Adapter Implementation
// =============================================================================

/**
 * Better-SQLite3 benchmark adapter
 *
 * Uses the synchronous better-sqlite3 library for maximum performance.
 * This adapter serves as the baseline for native SQLite performance.
 */
export class BetterSQLite3Adapter implements BenchmarkAdapter {
  readonly name = 'better-sqlite3';
  readonly version = '1.0.0';

  private config: BetterSQLite3AdapterConfig;
  private db: import('better-sqlite3').Database | null = null;
  private initialized = false;
  private initStartTime: number = 0;
  private firstQueryTime: number | null = null;

  constructor(config: BetterSQLite3AdapterConfig = {}) {
    this.config = {
      dbPath: ':memory:',
      verbose: false,
      walMode: true,
      ...config,
    };
  }

  // ===========================================================================
  // Lifecycle Methods
  // ===========================================================================

  async initialize(): Promise<void> {
    if (this.initialized) return;

    this.initStartTime = performance.now();

    // Dynamic import for better-sqlite3
    const Database = (await import('better-sqlite3')).default;
    this.db = new Database(this.config.dbPath!, {
      verbose: this.config.verbose ? console.log : undefined,
    });

    // Enable WAL mode for better concurrent read performance
    if (this.config.walMode) {
      this.db.pragma('journal_mode = WAL');
    }

    // Optimize for benchmarks
    this.db.pragma('synchronous = OFF');
    this.db.pragma('temp_store = MEMORY');
    this.db.pragma('cache_size = -64000'); // 64MB cache

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

  async dropTable(tableName: string): Promise<void> {
    if (!this.db) throw new Error('Database not initialized');
    this.db.exec(`DROP TABLE IF EXISTS ${tableName}`);
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
      const values = columns.map((col) => row[col]);

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

      const insertMany = this.db.transaction((rows: Record<string, unknown>[]) => {
        for (const row of rows) {
          const values = columns.map((col) => row[col]);
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
      const row = stmt.get(primaryKeyValue);

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'read',
        sql: `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: row ? 1 : 0,
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
      // Convert named params to positional for better-sqlite3
      let sql = `SELECT * FROM ${tableName} WHERE ${whereClause}`;
      const values: unknown[] = [];

      if (params) {
        for (const [key, value] of Object.entries(params)) {
          sql = sql.replace(`:${key}`, '?');
          values.push(value);
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
      const values = [...Object.values(updates), primaryKeyValue];

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
        rowCount: result.changes,
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
      const result = stmt.run(primaryKeyValue);

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'delete',
        sql: `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: result.changes,
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
      const values: unknown[] = [];

      if (params) {
        for (const [key, value] of Object.entries(params)) {
          query = query.replace(`:${key}`, '?');
          values.push(value);
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
        rowCount = result.changes;
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
              const values = columns.map((col) => op.data[col]);
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
              const values = [...Object.values(updates), id];
              const stmt = this.db!.prepare(
                `UPDATE ${op.tableName} SET ${setClauses} WHERE id = ?`
              );
              const result = stmt.run(...values);
              rowCount += result.changes;
              break;
            }
            case 'delete': {
              const { id } = op.data;
              const stmt = this.db!.prepare(
                `DELETE FROM ${op.tableName} WHERE id = ?`
              );
              const result = stmt.run(id);
              rowCount += result.changes;
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

    const pageCount = this.db.pragma('page_count')[0] as { page_count: number };
    const pageSize = this.db.pragma('page_size')[0] as { page_size: number };
    const totalBytes = pageCount.page_count * pageSize.page_size;

    const tables = this.db
      .prepare("SELECT name FROM sqlite_master WHERE type='table'")
      .all() as { name: string }[];

    let totalRows = 0;
    for (const table of tables) {
      const count = this.db
        .prepare(`SELECT COUNT(*) as count FROM ${table.name}`)
        .get() as { count: number };
      totalRows += count.count;
    }

    return {
      totalBytes,
      rowCount: totalRows,
      tableCount: tables.length,
      limitUtilization: 0, // No limit for native SQLite
    };
  }

  async measureColdStart(): Promise<ColdStartMetrics> {
    const initTime = this.initStartTime > 0 ? performance.now() - this.initStartTime : 0;

    return {
      timeToFirstQuery: this.firstQueryTime ?? initTime,
      initializationTime: initTime,
      connectionTime: 0, // No network connection
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
 * Create a Better-SQLite3 benchmark adapter
 */
export function createBetterSQLite3Adapter(
  config: BetterSQLite3AdapterConfig = {}
): BetterSQLite3Adapter {
  return new BetterSQLite3Adapter(config);
}
