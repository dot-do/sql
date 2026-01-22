/**
 * DoSQL Kysely Driver
 *
 * Implements Kysely's Driver interface to route queries to the DoSQL backend.
 * This is the core component that bridges Kysely's query builder with DoSQL's
 * execution engine.
 */

import type {
  Driver,
  DatabaseConnection,
  CompiledQuery,
  QueryResult,
  TransactionSettings,
} from 'kysely';

import type {
  DoSQLBackend,
  DoSQLDialectConfig,
  DoSQLKyselyResult,
} from './types.js';

import type { SqlValue, Row } from '../../engine/types.js';

import { QueryExecutionError, ConnectionError } from './types.js';

// =============================================================================
// DOSQL DRIVER
// =============================================================================

/**
 * DoSQL driver for Kysely.
 * Manages connections to the DoSQL backend and routes queries.
 */
export class DoSQLDriver implements Driver {
  private readonly config: DoSQLDialectConfig;
  private connection: DoSQLConnection | null = null;

  constructor(config: DoSQLDialectConfig) {
    this.config = config;
  }

  /**
   * Initialize the driver.
   * Called once when the Kysely instance is created.
   */
  async init(): Promise<void> {
    // DoSQL backend is already initialized, nothing to do here
  }

  /**
   * Acquire a connection from the pool.
   * DoSQL uses a single connection model per backend instance.
   */
  async acquireConnection(): Promise<DatabaseConnection> {
    if (!this.connection) {
      this.connection = new DoSQLConnection(this.config);
    }
    return this.connection;
  }

  /**
   * Begin a transaction on the connection.
   */
  async beginTransaction(
    connection: DatabaseConnection,
    settings: TransactionSettings
  ): Promise<void> {
    const conn = connection as DoSQLConnection;
    await conn.beginTransaction(settings);
  }

  /**
   * Commit a transaction on the connection.
   */
  async commitTransaction(connection: DatabaseConnection): Promise<void> {
    const conn = connection as DoSQLConnection;
    await conn.commitTransaction();
  }

  /**
   * Rollback a transaction on the connection.
   */
  async rollbackTransaction(connection: DatabaseConnection): Promise<void> {
    const conn = connection as DoSQLConnection;
    await conn.rollbackTransaction();
  }

  /**
   * Release a connection back to the pool.
   * For DoSQL, we keep the connection alive.
   */
  async releaseConnection(_connection: DatabaseConnection): Promise<void> {
    // DoSQL maintains persistent connections, no release needed
  }

  /**
   * Destroy the driver and clean up resources.
   */
  async destroy(): Promise<void> {
    if (this.connection) {
      await this.connection.close();
      this.connection = null;
    }
  }
}

// =============================================================================
// DOSQL CONNECTION
// =============================================================================

/**
 * DoSQL database connection for Kysely.
 * Executes compiled queries against the DoSQL backend.
 */
class DoSQLConnection implements DatabaseConnection {
  private readonly config: DoSQLDialectConfig;
  private inTransaction = false;

  constructor(config: DoSQLDialectConfig) {
    this.config = config;
  }

  /**
   * Execute a compiled query.
   */
  async executeQuery<R>(compiledQuery: CompiledQuery): Promise<QueryResult<R>> {
    const { sql, parameters } = compiledQuery;

    // Log query if logging is enabled
    this.logQuery(sql, parameters as SqlValue[]);

    try {
      // Transform parameters if transformer is provided
      const params = this.config.transformParameters
        ? this.config.transformParameters([...parameters])
        : ([...parameters] as SqlValue[]);

      // Execute query through DoSQL backend
      const result = await this.config.backend.execute<R>(sql, params);

      // Convert to Kysely QueryResult format
      return this.toKyselyResult<R>(result, sql);
    } catch (error) {
      throw new QueryExecutionError(
        error instanceof Error ? error.message : String(error),
        sql,
        [...parameters]
      );
    }
  }

  /**
   * Stream query results.
   * DoSQL supports async iteration through the executor.
   */
  async *streamQuery<R>(
    compiledQuery: CompiledQuery,
    _chunkSize?: number
  ): AsyncIterableIterator<QueryResult<R>> {
    const { sql, parameters } = compiledQuery;

    this.logQuery(sql, parameters as SqlValue[]);

    try {
      const params = this.config.transformParameters
        ? this.config.transformParameters([...parameters])
        : ([...parameters] as SqlValue[]);

      // Execute query and yield rows in batches
      const rows = await this.config.backend.query<R>(sql, params);

      // Yield all rows as a single batch for now
      // Future: implement true streaming with chunking
      yield {
        rows,
      };
    } catch (error) {
      throw new QueryExecutionError(
        error instanceof Error ? error.message : String(error),
        sql,
        [...parameters]
      );
    }
  }

  /**
   * Begin a transaction.
   */
  async beginTransaction(_settings: TransactionSettings): Promise<void> {
    if (this.inTransaction) {
      throw new ConnectionError('Transaction already in progress');
    }

    if (this.config.backend.beginTransaction) {
      await this.config.backend.beginTransaction();
    } else {
      // Fallback: execute BEGIN statement
      await this.config.backend.execute('BEGIN TRANSACTION');
    }

    this.inTransaction = true;
  }

  /**
   * Commit the current transaction.
   */
  async commitTransaction(): Promise<void> {
    if (!this.inTransaction) {
      throw new ConnectionError('No transaction in progress');
    }

    if (this.config.backend.commit) {
      await this.config.backend.commit();
    } else {
      await this.config.backend.execute('COMMIT');
    }

    this.inTransaction = false;
  }

  /**
   * Rollback the current transaction.
   */
  async rollbackTransaction(): Promise<void> {
    if (!this.inTransaction) {
      throw new ConnectionError('No transaction in progress');
    }

    if (this.config.backend.rollback) {
      await this.config.backend.rollback();
    } else {
      await this.config.backend.execute('ROLLBACK');
    }

    this.inTransaction = false;
  }

  /**
   * Close the connection.
   */
  async close(): Promise<void> {
    if (this.inTransaction) {
      await this.rollbackTransaction();
    }

    if (this.config.backend.close) {
      await this.config.backend.close();
    }
  }

  /**
   * Convert DoSQL result to Kysely QueryResult format.
   */
  private toKyselyResult<R>(
    result: {
      rows: R[];
      rowsAffected?: number;
      stats?: { executionTime: number; rowsScanned: number; rowsReturned: number };
    },
    sql: string
  ): QueryResult<R> {
    // Detect mutation queries for numAffectedRows
    const isMutation = /^\s*(INSERT|UPDATE|DELETE|REPLACE)/i.test(sql);

    // Determine numAffectedRows for mutations
    let numAffectedRows: bigint | undefined;
    if (isMutation && result.rowsAffected !== undefined) {
      numAffectedRows = BigInt(result.rowsAffected);
    }

    // Determine insertId for INSERTs
    let insertId: bigint | undefined;
    if (/^\s*INSERT/i.test(sql) && result.rows.length > 0) {
      const lastRow = result.rows[result.rows.length - 1] as Record<string, unknown>;
      if (lastRow && typeof lastRow.id === 'number') {
        insertId = BigInt(lastRow.id);
      }
    }

    // Build the result object with all properties at once (QueryResult properties are readonly)
    return {
      rows: result.rows,
      ...(numAffectedRows !== undefined && { numAffectedRows }),
      ...(insertId !== undefined && { insertId }),
    };
  }

  /**
   * Log a query if logging is enabled.
   */
  private logQuery(sql: string, parameters: SqlValue[]): void {
    if (!this.config.log) return;

    if (typeof this.config.log === 'function') {
      this.config.log(sql, parameters);
    } else {
      console.log('[DoSQL Kysely]', sql, parameters);
    }
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a DoSQL driver instance.
 */
export function createDoSQLDriver(config: DoSQLDialectConfig): DoSQLDriver {
  return new DoSQLDriver(config);
}
