/**
 * Prisma Driver Adapter for DoSQL
 *
 * Implements the Prisma driver adapter interface to enable Prisma ORM
 * to use DoSQL as its underlying database on Cloudflare Workers edge runtime.
 *
 * @packageDocumentation
 */

import type { SqlValue, Row, QueryResult } from '../../engine/types.js';

// =============================================================================
// Prisma Driver Adapter Types
// =============================================================================

/**
 * Prisma Query interface
 * Represents a parameterized SQL query
 */
export interface Query {
  sql: string;
  args: unknown[];
}

/**
 * Prisma result wrapper for operations that may fail
 */
export type Result<T> =
  | { ok: true; value: T }
  | { ok: false; error: Error };

/**
 * Column type information for Prisma result sets
 */
export interface ColumnType {
  /** Column name */
  name: string;
  /** Column type (SQLite type name) */
  type: string;
}

/**
 * Prisma ResultSet structure
 * Represents the result of a SELECT query
 */
export interface ResultSet {
  /** Column metadata */
  columnTypes: ColumnType[];
  /** Column names (for quick access) */
  columnNames: string[];
  /** Row data as arrays (positional values matching columnNames) */
  rows: unknown[][];
  /** Last insert rowid (for INSERT operations) */
  lastInsertRowid?: bigint;
}

/**
 * Isolation level for transactions
 */
export type IsolationLevel =
  | 'ReadUncommitted'
  | 'ReadCommitted'
  | 'RepeatableRead'
  | 'Snapshot'
  | 'Serializable';

/**
 * Transaction options
 */
export interface TransactionOptions {
  usePhantomQuery: boolean;
}

/**
 * Prisma Transaction interface
 * Represents an active database transaction
 */
export interface Transaction {
  /** Transaction options */
  readonly options: TransactionOptions;
  /** Execute a query within the transaction */
  queryRaw(params: Query): Promise<Result<ResultSet>>;
  /** Execute a write operation within the transaction */
  executeRaw(params: Query): Promise<Result<number>>;
  /** Commit the transaction */
  commit(): Promise<Result<void>>;
  /** Rollback the transaction */
  rollback(): Promise<Result<void>>;
}

/**
 * Prisma DriverAdapter interface
 * The main interface that database drivers must implement
 */
export interface DriverAdapter {
  /** Provider name (e.g., 'sqlite', 'postgresql', 'mysql') */
  readonly provider: 'sqlite' | 'postgresql' | 'mysql';
  /** Adapter name for identification */
  readonly adapterName: string;
  /** Execute a raw query */
  queryRaw(params: Query): Promise<Result<ResultSet>>;
  /** Execute a raw write operation (INSERT, UPDATE, DELETE) */
  executeRaw(params: Query): Promise<Result<number>>;
  /** Start a new transaction */
  startTransaction(): Promise<Result<Transaction>>;
}

// =============================================================================
// DoSQL Backend Interface
// =============================================================================

/**
 * DoSQL backend interface for query execution
 * This interface represents the DoSQL execution layer
 */
export interface DoSQLBackend {
  /**
   * Execute a SQL query and return results
   * @param sql The SQL query string
   * @param params Query parameters
   * @returns Query result with rows and metadata
   */
  query<T = Row>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;

  /**
   * Execute a SQL write operation (INSERT, UPDATE, DELETE)
   * @param sql The SQL statement
   * @param params Statement parameters
   * @returns Number of affected rows
   */
  execute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number; lastInsertRowid?: bigint }>;

  /**
   * Begin a transaction
   * @returns Transaction handle
   */
  beginTransaction(): Promise<DoSQLTransaction>;
}

/**
 * DoSQL transaction handle
 */
export interface DoSQLTransaction {
  /** Transaction ID */
  readonly id: string;
  /** Execute a query within the transaction */
  query<T = Row>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;
  /** Execute a write operation within the transaction */
  execute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number; lastInsertRowid?: bigint }>;
  /** Commit the transaction */
  commit(): Promise<void>;
  /** Rollback the transaction */
  rollback(): Promise<void>;
}

// =============================================================================
// Prisma DoSQL Adapter Configuration
// =============================================================================

/**
 * Configuration options for the Prisma DoSQL adapter
 */
export interface PrismaDoSQLConfig {
  /** DoSQL backend instance */
  backend: DoSQLBackend;
  /** Enable query logging */
  logging?: boolean;
  /** Custom logger function */
  logger?: (message: string, params?: unknown) => void;
}

// =============================================================================
// DoSQL Transaction Wrapper
// =============================================================================

/**
 * Wraps a DoSQL transaction to implement the Prisma Transaction interface
 */
class DoSQLPrismaTransaction implements Transaction {
  readonly options: TransactionOptions = { usePhantomQuery: false };

  constructor(
    private readonly doSQLTxn: DoSQLTransaction,
    private readonly adapter: PrismaDoSQLAdapter
  ) {}

  async queryRaw(params: Query): Promise<Result<ResultSet>> {
    try {
      const result = await this.doSQLTxn.query(params.sql, params.args);
      return {
        ok: true,
        value: this.adapter.toResultSet(result),
      };
    } catch (error) {
      return {
        ok: false,
        error: error instanceof Error ? error : new Error(String(error)),
      };
    }
  }

  async executeRaw(params: Query): Promise<Result<number>> {
    try {
      const result = await this.doSQLTxn.execute(params.sql, params.args);
      return {
        ok: true,
        value: result.rowsAffected,
      };
    } catch (error) {
      return {
        ok: false,
        error: error instanceof Error ? error : new Error(String(error)),
      };
    }
  }

  async commit(): Promise<Result<void>> {
    try {
      await this.doSQLTxn.commit();
      return { ok: true, value: undefined };
    } catch (error) {
      return {
        ok: false,
        error: error instanceof Error ? error : new Error(String(error)),
      };
    }
  }

  async rollback(): Promise<Result<void>> {
    try {
      await this.doSQLTxn.rollback();
      return { ok: true, value: undefined };
    } catch (error) {
      return {
        ok: false,
        error: error instanceof Error ? error : new Error(String(error)),
      };
    }
  }
}

// =============================================================================
// Prisma DoSQL Adapter Implementation
// =============================================================================

/**
 * Prisma driver adapter for DoSQL
 *
 * Enables using Prisma ORM with DoSQL on Cloudflare Workers.
 *
 * @example
 * ```typescript
 * import { PrismaClient } from '@prisma/client';
 * import { PrismaDoSQLAdapter } from 'dosql/orm/prisma';
 *
 * const adapter = new PrismaDoSQLAdapter({ backend: dosqlBackend });
 * const prisma = new PrismaClient({ adapter });
 *
 * const users = await prisma.user.findMany();
 * ```
 */
export class PrismaDoSQLAdapter implements DriverAdapter {
  /** Provider type (SQLite-compatible) */
  readonly provider = 'sqlite' as const;

  /** Adapter identification name */
  readonly adapterName = 'dosql-prisma';

  /** DoSQL backend instance */
  private readonly backend: DoSQLBackend;

  /** Logging enabled flag */
  private readonly logging: boolean;

  /** Logger function */
  private readonly logger: (message: string, params?: unknown) => void;

  constructor(config: PrismaDoSQLConfig) {
    this.backend = config.backend;
    this.logging = config.logging ?? false;
    this.logger = config.logger ?? console.log;
  }

  /**
   * Execute a raw SQL query
   *
   * @param params Query with SQL and arguments
   * @returns Result containing ResultSet or error
   */
  async queryRaw(params: Query): Promise<Result<ResultSet>> {
    this.log('queryRaw', params);

    try {
      const result = await this.backend.query(params.sql, params.args);
      return {
        ok: true,
        value: this.toResultSet(result),
      };
    } catch (error) {
      return {
        ok: false,
        error: error instanceof Error ? error : new Error(String(error)),
      };
    }
  }

  /**
   * Execute a raw write operation (INSERT, UPDATE, DELETE)
   *
   * @param params Query with SQL and arguments
   * @returns Result containing number of affected rows or error
   */
  async executeRaw(params: Query): Promise<Result<number>> {
    this.log('executeRaw', params);

    try {
      const result = await this.backend.execute(params.sql, params.args);
      return {
        ok: true,
        value: result.rowsAffected,
      };
    } catch (error) {
      return {
        ok: false,
        error: error instanceof Error ? error : new Error(String(error)),
      };
    }
  }

  /**
   * Start a new database transaction
   *
   * @returns Result containing Transaction or error
   */
  async startTransaction(): Promise<Result<Transaction>> {
    this.log('startTransaction');

    try {
      const doSQLTxn = await this.backend.beginTransaction();
      return {
        ok: true,
        value: new DoSQLPrismaTransaction(doSQLTxn, this),
      };
    } catch (error) {
      return {
        ok: false,
        error: error instanceof Error ? error : new Error(String(error)),
      };
    }
  }

  /**
   * Convert DoSQL QueryResult to Prisma ResultSet
   *
   * @param result DoSQL query result
   * @returns Prisma-compatible ResultSet
   */
  toResultSet<T = Row>(result: QueryResult<T>): ResultSet {
    const rows = result.rows as unknown as Record<string, unknown>[];

    // Extract column information from the first row or column metadata
    let columnNames: string[] = [];
    let columnTypes: ColumnType[] = [];

    if (result.columns && result.columns.length > 0) {
      // Use provided column metadata
      columnNames = result.columns.map((c) => c.name);
      columnTypes = result.columns.map((c) => ({
        name: c.name,
        type: this.mapToSqliteType(c.type),
      }));
    } else if (rows.length > 0) {
      // Infer from first row
      const firstRow = rows[0];
      columnNames = Object.keys(firstRow);
      columnTypes = columnNames.map((name) => ({
        name,
        type: this.inferSqliteType(firstRow[name]),
      }));
    }

    // Convert rows from objects to arrays
    const rowArrays = rows.map((row) =>
      columnNames.map((col) => this.convertValue(row[col]))
    );

    return {
      columnTypes,
      columnNames,
      rows: rowArrays,
    };
  }

  /**
   * Map DoSQL type to SQLite type name
   */
  private mapToSqliteType(type: string): string {
    const typeMap: Record<string, string> = {
      string: 'TEXT',
      number: 'REAL',
      bigint: 'INTEGER',
      boolean: 'INTEGER',
      date: 'TEXT',
      bytes: 'BLOB',
    };
    return typeMap[type.toLowerCase()] ?? 'TEXT';
  }

  /**
   * Infer SQLite type from a JavaScript value
   */
  private inferSqliteType(value: unknown): string {
    if (value === null || value === undefined) {
      return 'NULL';
    }
    if (typeof value === 'string') {
      return 'TEXT';
    }
    if (typeof value === 'number') {
      return Number.isInteger(value) ? 'INTEGER' : 'REAL';
    }
    if (typeof value === 'bigint') {
      return 'INTEGER';
    }
    if (typeof value === 'boolean') {
      return 'INTEGER';
    }
    if (value instanceof Date) {
      return 'TEXT';
    }
    if (value instanceof Uint8Array) {
      return 'BLOB';
    }
    return 'TEXT';
  }

  /**
   * Convert a JavaScript value to SQLite-compatible value
   *
   * Prisma expects specific value representations:
   * - Booleans as 0/1 integers
   * - Dates as ISO strings
   * - BigInts as numbers (when safe) or strings
   */
  private convertValue(value: unknown): unknown {
    if (value === null || value === undefined) {
      return null;
    }
    if (typeof value === 'boolean') {
      return value ? 1 : 0;
    }
    if (value instanceof Date) {
      return value.toISOString();
    }
    if (typeof value === 'bigint') {
      // Return as bigint for Prisma
      return value;
    }
    if (value instanceof Uint8Array) {
      // Return as buffer for BLOB
      return value;
    }
    return value;
  }

  /**
   * Log a message if logging is enabled
   */
  private log(operation: string, params?: unknown): void {
    if (this.logging) {
      this.logger(`[PrismaDoSQLAdapter] ${operation}`, params);
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a new Prisma DoSQL adapter
 *
 * @param config Adapter configuration
 * @returns PrismaDoSQLAdapter instance
 *
 * @example
 * ```typescript
 * const adapter = createPrismaAdapter({
 *   backend: dosqlBackend,
 *   logging: true,
 * });
 * ```
 */
export function createPrismaAdapter(config: PrismaDoSQLConfig): PrismaDoSQLAdapter {
  return new PrismaDoSQLAdapter(config);
}

// =============================================================================
// Type Guards and Utilities
// =============================================================================

/**
 * Check if a result is successful
 */
export function isOk<T>(result: Result<T>): result is { ok: true; value: T } {
  return result.ok === true;
}

/**
 * Check if a result is an error
 */
export function isError<T>(result: Result<T>): result is { ok: false; error: Error } {
  return result.ok === false;
}

/**
 * Unwrap a result, throwing if it's an error
 */
export function unwrap<T>(result: Result<T>): T {
  if (result.ok === true) {
    return result.value;
  }
  throw (result as { ok: false; error: Error }).error;
}

/**
 * Unwrap a result with a default value for errors
 */
export function unwrapOr<T>(result: Result<T>, defaultValue: T): T {
  return result.ok ? result.value : defaultValue;
}
