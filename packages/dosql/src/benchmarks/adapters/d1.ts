/**
 * D1 Benchmark Adapter
 *
 * Implements the BenchmarkAdapter interface for Cloudflare D1.
 * Used for comparing DoSQL performance against D1.
 */

import type { D1Database, D1PreparedStatement } from '@cloudflare/workers-types';

import {
  BenchmarkAdapter,
  BenchmarkOperation,
  TableSchemaConfig,
  StorageMetrics,
  ColdStartMetrics,
  ColumnConfig,
} from '../types.js';

// =============================================================================
// Configuration
// =============================================================================

/**
 * Configuration for D1Adapter
 */
export interface D1AdapterConfig {
  /** D1 database binding */
  db: D1Database;
}

// =============================================================================
// D1 Adapter Implementation
// =============================================================================

/**
 * D1 benchmark adapter
 *
 * Implements all BenchmarkAdapter methods using Cloudflare D1's API.
 * Uses prepared statements with bind() for parameterized queries.
 * Uses batch() for multi-statement operations.
 */
export class D1Adapter implements BenchmarkAdapter {
  readonly name = 'd1';
  readonly version = '1.0.0';

  private db: D1Database;
  private initialized = false;
  private constructedAt: number;
  private firstQueryTime: number | null = null;
  private tables: Set<string> = new Set();

  constructor(config: D1AdapterConfig) {
    this.db = config.db;
    this.constructedAt = performance.now();
  }

  // ===========================================================================
  // Lifecycle Methods
  // ===========================================================================

  /**
   * Initialize the adapter
   */
  async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    // Verify connection by executing a simple query
    const start = performance.now();
    await this.db.prepare('SELECT 1').first();

    if (this.firstQueryTime === null) {
      this.firstQueryTime = performance.now() - start;
    }

    this.initialized = true;
  }

  /**
   * Clean up resources - drop all created tables
   */
  async cleanup(): Promise<void> {
    const dropStatements = Array.from(this.tables).map((tableName) =>
      this.db.prepare(`DROP TABLE IF EXISTS ${this.escapeIdentifier(tableName)}`)
    );

    if (dropStatements.length > 0) {
      await this.db.batch(dropStatements);
    }

    this.tables.clear();
    this.initialized = false;
    this.firstQueryTime = null;
  }

  // ===========================================================================
  // Table Operations
  // ===========================================================================

  /**
   * Create a table with the given schema
   */
  async createTable(schema: TableSchemaConfig): Promise<void> {
    const columnDefs = schema.columns.map((col) => {
      let def = `${this.escapeIdentifier(col.name)} ${col.type}`;
      if (col.name === schema.primaryKey) {
        def += ' PRIMARY KEY';
      }
      if (col.nullable === false) {
        def += ' NOT NULL';
      }
      if (col.defaultValue !== undefined) {
        def += ` DEFAULT ${this.formatValue(col.defaultValue)}`;
      }
      return def;
    }).join(', ');

    const createSql = `CREATE TABLE IF NOT EXISTS ${this.escapeIdentifier(schema.tableName)} (${columnDefs})`;
    await this.db.prepare(createSql).run();

    // Create indexes
    if (schema.indexes && schema.indexes.length > 0) {
      const indexStatements = schema.indexes.map((indexCol) =>
        this.db.prepare(
          `CREATE INDEX IF NOT EXISTS idx_${schema.tableName}_${indexCol} ON ${this.escapeIdentifier(schema.tableName)} (${this.escapeIdentifier(indexCol)})`
        )
      );
      await this.db.batch(indexStatements);
    }

    this.tables.add(schema.tableName);
  }

  /**
   * Drop a table if it exists
   */
  async dropTable(tableName: string): Promise<void> {
    await this.db.prepare(`DROP TABLE IF EXISTS ${this.escapeIdentifier(tableName)}`).run();
    this.tables.delete(tableName);
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
    const columns = Object.keys(row);
    const placeholders = columns.map(() => '?').join(', ');
    const values = columns.map((col) => row[col]);

    const sql = `INSERT INTO ${this.escapeIdentifier(tableName)} (${columns.map(c => this.escapeIdentifier(c)).join(', ')}) VALUES (${placeholders})`;

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    try {
      const result = await this.db.prepare(sql).bind(...values).run();
      rowCount = result.meta.changes ?? 1;
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
    const placeholders = columns.map(() => '?').join(', ');
    const sql = `INSERT INTO ${this.escapeIdentifier(tableName)} (${columns.map(c => this.escapeIdentifier(c)).join(', ')}) VALUES (${placeholders})`;

    const statements = rows.map((row) => {
      const values = columns.map((col) => row[col]);
      return this.db.prepare(sql).bind(...values);
    });

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    try {
      // D1 batch() has a limit of 100 statements per batch
      // Process in chunks if needed
      const BATCH_SIZE = 100;
      for (let i = 0; i < statements.length; i += BATCH_SIZE) {
        const chunk = statements.slice(i, i + BATCH_SIZE);
        const results = await this.db.batch(chunk);
        rowCount += results.reduce((sum, r) => sum + (r.meta.changes ?? 0), 0);
      }
    } catch (e) {
      success = false;
      error = e instanceof Error ? e.message : String(e);
    }

    const durationMs = performance.now() - startedAt;

    return {
      type: 'batch',
      sql: `${sql} (${rows.length} rows)`,
      params: { rows: rows.length },
      durationMs,
      rowCount,
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
    const sql = `SELECT * FROM ${this.escapeIdentifier(tableName)} WHERE ${this.escapeIdentifier(primaryKey)} = ?`;

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    try {
      const result = await this.db.prepare(sql).bind(primaryKeyValue).first();
      rowCount = result ? 1 : 0;
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
    const sql = `SELECT * FROM ${this.escapeIdentifier(tableName)} WHERE ${whereClause}`;

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    try {
      const bindValues = params ? Object.values(params) : [];
      const result = await this.db.prepare(sql).bind(...bindValues).all();
      rowCount = result.results?.length ?? 0;
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
    const setClauses = Object.keys(updates)
      .map((col) => `${this.escapeIdentifier(col)} = ?`)
      .join(', ');
    const values = [...Object.values(updates), primaryKeyValue];

    const sql = `UPDATE ${this.escapeIdentifier(tableName)} SET ${setClauses} WHERE ${this.escapeIdentifier(primaryKey)} = ?`;

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    try {
      const result = await this.db.prepare(sql).bind(...values).run();
      rowCount = result.meta.changes ?? 0;
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
    const sql = `DELETE FROM ${this.escapeIdentifier(tableName)} WHERE ${this.escapeIdentifier(primaryKey)} = ?`;

    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    try {
      const result = await this.db.prepare(sql).bind(primaryKeyValue).run();
      rowCount = result.meta.changes ?? 0;
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
  async query(sql: string, params?: Record<string, unknown>): Promise<BenchmarkOperation> {
    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    try {
      const bindValues = params ? Object.values(params) : [];
      const stmt = this.db.prepare(sql);
      const boundStmt = bindValues.length > 0 ? stmt.bind(...bindValues) : stmt;

      // Determine if it's a read or write query
      const isRead = sql.trim().toUpperCase().startsWith('SELECT');

      if (isRead) {
        const result = await boundStmt.all();
        rowCount = result.results?.length ?? 0;
      } else {
        const result = await boundStmt.run();
        rowCount = result.meta.changes ?? 0;
      }
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
   *
   * D1 batch() provides implicit transaction semantics - if any statement fails,
   * all statements in the batch are rolled back.
   */
  async transaction(
    operations: Array<{
      type: 'insert' | 'update' | 'delete';
      tableName: string;
      data: Record<string, unknown>;
    }>
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();
    let success = true;
    let error: string | undefined;
    let rowCount = 0;

    const statements: D1PreparedStatement[] = [];

    try {
      for (const op of operations) {
        const table = this.escapeIdentifier(op.tableName);

        switch (op.type) {
          case 'insert': {
            const columns = Object.keys(op.data);
            const placeholders = columns.map(() => '?').join(', ');
            const values = Object.values(op.data);
            const sql = `INSERT INTO ${table} (${columns.map(c => this.escapeIdentifier(c)).join(', ')}) VALUES (${placeholders})`;
            statements.push(this.db.prepare(sql).bind(...values));
            break;
          }

          case 'update': {
            // Expect data to have 'id' or first key as primary key, and other fields as updates
            const { id, ...updates } = op.data;
            if (id === undefined) {
              throw new Error('Update operation requires an id field');
            }
            const setClauses = Object.keys(updates)
              .map((col) => `${this.escapeIdentifier(col)} = ?`)
              .join(', ');
            const values = [...Object.values(updates), id];
            const sql = `UPDATE ${table} SET ${setClauses} WHERE id = ?`;
            statements.push(this.db.prepare(sql).bind(...values));
            break;
          }

          case 'delete': {
            const { id } = op.data;
            if (id === undefined) {
              throw new Error('Delete operation requires an id field');
            }
            const sql = `DELETE FROM ${table} WHERE id = ?`;
            statements.push(this.db.prepare(sql).bind(id));
            break;
          }
        }
      }

      // Execute as batch (implicit transaction)
      const results = await this.db.batch(statements);
      rowCount = results.reduce((sum, r) => sum + (r.meta.changes ?? 0), 0);
    } catch (e) {
      success = false;
      error = e instanceof Error ? e.message : String(e);
    }

    const durationMs = performance.now() - startedAt;

    return {
      type: 'transaction',
      sql: `TRANSACTION (${operations.length} operations)`,
      params: { operationCount: operations.length },
      durationMs,
      rowCount,
      success,
      error,
      startedAt,
    };
  }

  // ===========================================================================
  // Metrics
  // ===========================================================================

  /**
   * Get current storage metrics
   */
  async getStorageMetrics(): Promise<StorageMetrics> {
    let totalBytes = 0;
    let rowCount = 0;
    let tableCount = 0;

    try {
      // Get table count from sqlite_master
      const tablesResult = await this.db
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%'")
        .all();

      const tables = tablesResult.results ?? [];
      tableCount = tables.length;

      // Get row count for each table
      for (const tableRow of tables) {
        const tableName = (tableRow as { name: string }).name;
        const countResult = await this.db
          .prepare(`SELECT COUNT(*) as count FROM ${this.escapeIdentifier(tableName)}`)
          .first<{ count: number }>();
        rowCount += countResult?.count ?? 0;
      }

      // D1 doesn't expose actual storage size via SQL, so we estimate
      // based on a rough average of 100 bytes per row
      totalBytes = rowCount * 100;
    } catch (e) {
      // If we can't get metrics, return zeros
    }

    // D1 has a 10GB limit per database (free tier: 500MB)
    const D1_MAX_SIZE = 10 * 1024 * 1024 * 1024; // 10GB

    return {
      totalBytes,
      rowCount,
      tableCount,
      limitUtilization: (totalBytes / D1_MAX_SIZE) * 100,
    };
  }

  /**
   * Measure cold start time
   */
  async measureColdStart(): Promise<ColdStartMetrics> {
    // If we already have a first query time from initialization, use it
    if (this.firstQueryTime !== null) {
      return {
        timeToFirstQuery: this.firstQueryTime,
        initializationTime: 0, // D1 doesn't require separate initialization
        connectionTime: this.firstQueryTime, // Connection is established on first query
      };
    }

    // Otherwise, measure a fresh query
    const start = performance.now();
    await this.db.prepare('SELECT 1').first();
    const queryTime = performance.now() - start;

    return {
      timeToFirstQuery: queryTime,
      initializationTime: 0,
      connectionTime: queryTime,
    };
  }

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  /**
   * Escape a SQL identifier (table/column name)
   */
  private escapeIdentifier(identifier: string): string {
    // Use double quotes for identifier escaping (SQL standard)
    return `"${identifier.replace(/"/g, '""')}"`;
  }

  /**
   * Format a value for SQL
   */
  private formatValue(value: unknown): string {
    if (value === null) {
      return 'NULL';
    }
    if (typeof value === 'number') {
      return String(value);
    }
    if (typeof value === 'boolean') {
      return value ? '1' : '0';
    }
    if (typeof value === 'string') {
      return `'${value.replace(/'/g, "''")}'`;
    }
    return `'${String(value)}'`;
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
