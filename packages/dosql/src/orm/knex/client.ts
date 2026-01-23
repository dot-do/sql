/**
 * DoSQL Knex Client
 *
 * Custom Knex client that routes queries through a DoSQL backend.
 * This enables using Knex's fluent query builder API with DoSQL's
 * TypeScript-native SQLite implementation.
 */

import type { Knex } from 'knex';
import type {
  DoSQLBackend,
  DoSQLKnexConfig,
  KnexQueryObject,
  KnexResponse,
  DoSQLKnexTransaction,
} from './types.js';
import type { SqlValue, Row } from '../../engine/types.js';

// =============================================================================
// DOSQL KNEX CLIENT
// =============================================================================

/**
 * Knex client implementation for DoSQL backend
 *
 * This client extends Knex's SQLite3 dialect to route all query execution
 * through a DoSQL backend instance. This allows using Knex's fluent API
 * while leveraging DoSQL's storage engine.
 *
 * @example
 * ```typescript
 * import { createKnex } from 'dosql/orm/knex';
 *
 * const knex = createKnex({ backend: dosqlBackend });
 *
 * const users = await knex('users')
 *   .where('age', '>', 21)
 *   .select('id', 'name');
 * ```
 */
export class DoSQLKnexClient {
  private readonly config: DoSQLKnexConfig;
  private readonly _connectionSettings: Record<string, unknown>;
  private _transactionId?: string;

  // Knex client interface requirements
  readonly dialect = 'sqlite3';
  readonly driverName = 'dosql';
  readonly canCancelQuery = false;

  /**
   * Get the dialect name for Knex
   */
  static get dialect(): string {
    return 'sqlite3';
  }

  /**
   * Dialect name method (required by Knex)
   */
  static getDialect(): string {
    return 'sqlite3';
  }

  constructor(config: DoSQLKnexConfig) {
    this.config = config;
    this._connectionSettings = {};
  }

  /**
   * Get the backend instance
   */
  get backend(): DoSQLBackend {
    return this.config.backend;
  }

  /**
   * Initialize the driver
   * Required by Knex client interface
   */
  initializeDriver(): void {
    // No driver initialization needed for DoSQL
  }

  /**
   * Acquire a connection from the pool
   * DoSQL uses the backend directly, so we return a connection wrapper
   */
  async acquireConnection(): Promise<DoSQLConnection> {
    return new DoSQLConnection(this.config.backend, this._transactionId);
  }

  /**
   * Release a connection back to the pool
   */
  async releaseConnection(_connection: DoSQLConnection): Promise<void> {
    // No pooling for DoSQL backend
  }

  /**
   * Destroy the connection pool
   */
  async destroy(): Promise<void> {
    // Nothing to destroy for DoSQL
  }

  /**
   * Execute a query through the DoSQL backend
   */
  async _query(
    connection: DoSQLConnection,
    obj: KnexQueryObject
  ): Promise<KnexResponse> {
    const { sql, bindings = [], method } = obj;

    this.log('debug', `Executing: ${sql}`);
    this.log('debug', `Bindings: ${JSON.stringify(bindings)}`);

    try {
      const result = await connection.query(sql, bindings as SqlValue[]);

      // Format response based on query method
      switch (method) {
        case 'select':
        case 'first':
        case 'pluck':
          return {
            rows: result.rows,
            fields: result.columns?.map((c) => ({ name: c.name, type: c.type })),
          };

        case 'insert':
          return {
            rowCount: result.rowsAffected ?? 0,
            lastID: result.lastInsertRowId,
            rows: result.rows,
          };

        case 'update':
        case 'delete':
        case 'del':
          return {
            rowCount: result.rowsAffected ?? 0,
            rows: result.rows,
          };

        case 'raw':
        default:
          return {
            rows: result.rows,
            rowCount: result.rowsAffected,
            lastID: result.lastInsertRowId,
            fields: result.columns?.map((c) => ({ name: c.name, type: c.type })),
          };
      }
    } catch (error) {
      this.log('error', `Query error: ${error}`);
      throw error;
    }
  }

  /**
   * Stream query results
   * Not fully supported by DoSQL yet, falls back to buffered query
   */
  async _stream(
    connection: DoSQLConnection,
    obj: KnexQueryObject,
    stream: NodeJS.WritableStream
  ): Promise<void> {
    const result = await this._query(connection, obj);

    for (const row of result.rows ?? []) {
      stream.write(JSON.stringify(row));
    }

    stream.end();
  }

  /**
   * Process response from query
   */
  processResponse(response: KnexResponse, runner: { method?: string }): unknown {
    const method = runner.method;

    switch (method) {
      case 'select':
        return response.rows ?? [];

      case 'first':
        return response.rows?.[0] ?? undefined;

      case 'pluck':
        // Knex pluck returns array of single column values
        if (response.rows && response.rows.length > 0) {
          const firstKey = Object.keys(response.rows[0])[0];
          return response.rows.map((row) => row[firstKey]);
        }
        return [];

      case 'insert':
        // Return last inserted ID
        return response.lastID !== undefined ? [response.lastID] : [];

      case 'update':
      case 'delete':
      case 'del':
        // Return affected row count
        return response.rowCount ?? 0;

      case 'raw':
      default:
        return response.rows ?? [];
    }
  }

  /**
   * Post-process returning queries (for INSERT/UPDATE with RETURNING)
   */
  postProcessResponse(result: unknown, ctx: { returning?: string | string[] }): unknown {
    if (ctx.returning && Array.isArray(result)) {
      // RETURNING clause was used, result should be rows
      return result;
    }
    return result;
  }

  /**
   * Begin a transaction
   */
  async beginTransaction(): Promise<DoSQLKnexTransaction> {
    if (!this.config.backend.beginTransaction) {
      throw new Error('DoSQL backend does not support transactions');
    }

    const transactionId = await this.config.backend.beginTransaction();
    this._transactionId = transactionId;

    return {
      id: transactionId,
      completed: false,
      commit: async () => {
        if (this.config.backend.commit) {
          await this.config.backend.commit(transactionId);
        }
        this._transactionId = undefined;
      },
      rollback: async () => {
        if (this.config.backend.rollback) {
          await this.config.backend.rollback(transactionId);
        }
        this._transactionId = undefined;
      },
    };
  }

  /**
   * Log a message
   */
  private log(level: 'debug' | 'warn' | 'error', message: string): void {
    if (!this.config.debug) return;

    const logger = this.config.log?.[level];
    if (logger) {
      logger(message);
    } else if (typeof console !== 'undefined') {
      console[level]?.(`[DoSQLKnex] ${message}`);
    }
  }

  // ===========================================================================
  // SCHEMA BUILDER OVERRIDES
  // ===========================================================================

  /**
   * Column compiler for SQLite
   */
  columnCompiler(): ColumnCompiler {
    return new ColumnCompiler();
  }

  /**
   * Table compiler for SQLite
   */
  tableCompiler(): TableCompiler {
    return new TableCompiler();
  }

  /**
   * Schema compiler for SQLite
   */
  schemaCompiler(): SchemaCompiler {
    return new SchemaCompiler();
  }
}

// =============================================================================
// DOSQL CONNECTION WRAPPER
// =============================================================================

/**
 * Connection wrapper for DoSQL backend
 */
export class DoSQLConnection {
  constructor(
    private backend: DoSQLBackend,
    private transactionId?: string
  ) {}

  /**
   * Execute a query
   */
  async query(sql: string, bindings: SqlValue[] = []) {
    return this.backend.query(sql, bindings);
  }

  /**
   * Execute a statement
   */
  async exec(sql: string, bindings: SqlValue[] = []) {
    return this.backend.exec(sql, bindings);
  }

  /**
   * Get the transaction ID
   */
  getTransactionId(): string | undefined {
    return this.transactionId;
  }
}

// =============================================================================
// SQLITE SCHEMA COMPILERS
// =============================================================================

/**
 * Column compiler for SQLite
 */
class ColumnCompiler {
  /**
   * Integer column type
   */
  integer(): string {
    return 'INTEGER';
  }

  /**
   * Big integer column type
   */
  bigInteger(): string {
    return 'INTEGER';
  }

  /**
   * Text column type
   */
  text(): string {
    return 'TEXT';
  }

  /**
   * String/varchar column type
   */
  string(length?: number): string {
    return length ? `VARCHAR(${length})` : 'TEXT';
  }

  /**
   * Float/double column type
   */
  float(): string {
    return 'REAL';
  }

  /**
   * Decimal column type
   */
  decimal(): string {
    return 'REAL';
  }

  /**
   * Boolean column type
   */
  boolean(): string {
    return 'INTEGER';
  }

  /**
   * Date column type
   */
  date(): string {
    return 'TEXT';
  }

  /**
   * DateTime column type
   */
  datetime(): string {
    return 'TEXT';
  }

  /**
   * Timestamp column type
   */
  timestamp(): string {
    return 'TEXT';
  }

  /**
   * Binary/blob column type
   */
  binary(): string {
    return 'BLOB';
  }

  /**
   * JSON column type
   */
  json(): string {
    return 'TEXT';
  }

  /**
   * UUID column type
   */
  uuid(): string {
    return 'TEXT';
  }
}

/**
 * Table compiler for SQLite
 */
class TableCompiler {
  /**
   * Create table SQL
   */
  createTable(
    tableName: string,
    columns: string[],
    constraints: string[] = []
  ): string {
    const allParts = [...columns, ...constraints].join(', ');
    return `CREATE TABLE "${tableName}" (${allParts})`;
  }

  /**
   * Drop table SQL
   */
  dropTable(tableName: string): string {
    return `DROP TABLE IF EXISTS "${tableName}"`;
  }

  /**
   * Rename table SQL
   */
  renameTable(oldName: string, newName: string): string {
    return `ALTER TABLE "${oldName}" RENAME TO "${newName}"`;
  }

  /**
   * Add column SQL
   */
  addColumn(tableName: string, columnDef: string): string {
    return `ALTER TABLE "${tableName}" ADD COLUMN ${columnDef}`;
  }

  /**
   * Create index SQL
   */
  createIndex(
    indexName: string,
    tableName: string,
    columns: string[],
    unique = false
  ): string {
    const uniqueStr = unique ? 'UNIQUE ' : '';
    const columnList = columns.map((c) => `"${c}"`).join(', ');
    return `CREATE ${uniqueStr}INDEX "${indexName}" ON "${tableName}" (${columnList})`;
  }

  /**
   * Drop index SQL
   */
  dropIndex(indexName: string): string {
    return `DROP INDEX IF EXISTS "${indexName}"`;
  }
}

/**
 * Schema compiler for SQLite
 */
class SchemaCompiler {
  /**
   * Check if table exists
   */
  hasTable(tableName: string): string {
    return `SELECT name FROM sqlite_master WHERE type='table' AND name='${tableName}'`;
  }

  /**
   * Check if column exists
   */
  hasColumn(tableName: string, columnName: string): string {
    return `PRAGMA table_info("${tableName}")`;
  }

  /**
   * Get table columns
   */
  columnInfo(tableName: string): string {
    return `PRAGMA table_info("${tableName}")`;
  }

  /**
   * Get table indexes
   */
  indexInfo(tableName: string): string {
    return `PRAGMA index_list("${tableName}")`;
  }
}

// =============================================================================
// EXPORTS
// =============================================================================

export { ColumnCompiler, TableCompiler, SchemaCompiler };
