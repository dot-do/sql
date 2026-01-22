/**
 * DoSQL Kysely Integration - Type Definitions
 *
 * Type-safe SQL query builder integration for DoSQL.
 * Provides Kysely dialect that routes queries to the DoSQL engine.
 */

import type {
  SqlValue,
  Row,
  QueryResult as DoSQLQueryResult,
  ExecutionContext,
  BTreeStorage,
  ColumnarStorage,
  Schema,
} from '../../engine/types.js';

// =============================================================================
// BACKEND CONFIGURATION
// =============================================================================

/**
 * DoSQL backend interface for Kysely integration.
 * This is the main entry point for executing SQL queries against DoSQL.
 */
export interface DoSQLBackend {
  /** Execute a SQL query with parameters */
  execute<T = Row>(sql: string, parameters?: SqlValue[]): Promise<DoSQLQueryResult<T>>;

  /** Execute a SQL query and return rows only */
  query<T = Row>(sql: string, parameters?: SqlValue[]): Promise<T[]>;

  /** Execute a SQL query and return a single row */
  queryOne<T = Row>(sql: string, parameters?: SqlValue[]): Promise<T | null>;

  /** Begin a transaction */
  beginTransaction?(): Promise<void>;

  /** Commit a transaction */
  commit?(): Promise<void>;

  /** Rollback a transaction */
  rollback?(): Promise<void>;

  /** Get the database schema */
  getSchema?(): Schema;

  /** Close the backend connection */
  close?(): Promise<void>;
}

/**
 * Configuration options for the DoSQL dialect
 */
export interface DoSQLDialectConfig {
  /** DoSQL backend instance */
  backend: DoSQLBackend;

  /** Enable query logging */
  log?: boolean | ((query: string, parameters: SqlValue[]) => void);

  /** Custom parameter transformer */
  transformParameters?: (parameters: unknown[]) => SqlValue[];

  /** Enable strict mode (fail on type mismatches) */
  strict?: boolean;
}

// =============================================================================
// QUERY RESULT TYPES
// =============================================================================

/**
 * Extended query result with DoSQL-specific metadata
 */
export interface DoSQLKyselyResult<T> {
  /** Rows returned by the query */
  rows: T[];

  /** Number of rows affected (for mutations) */
  numAffectedRows?: bigint;

  /** Insert ID for auto-increment columns */
  insertId?: bigint;

  /** Execution statistics from DoSQL */
  stats?: {
    executionTime: number;
    rowsScanned: number;
    rowsReturned: number;
  };
}

// =============================================================================
// UTILITY TYPES
// =============================================================================

/**
 * Infer the database type from a schema definition
 */
export type InferDatabase<S extends Record<string, Record<string, unknown>>> = {
  [K in keyof S]: S[K];
};

/**
 * Helper to make all properties optional (for updates)
 */
export type Updateable<T> = Partial<T>;

/**
 * Helper to make ID optional (for inserts)
 */
export type Insertable<T> = T extends { id: number | string }
  ? Omit<T, 'id'> & { id?: T['id'] }
  : T;

/**
 * Selection type helper
 */
export type Selectable<T> = T;

// =============================================================================
// COLUMN TYPE MAPPINGS
// =============================================================================

/**
 * SQLite to TypeScript type mappings
 */
export interface SqliteTypeMap {
  INTEGER: number;
  REAL: number;
  TEXT: string;
  BLOB: Uint8Array;
  NULL: null;
  BOOLEAN: boolean;
  DATE: Date;
  DATETIME: Date;
  TIMESTAMP: Date;
  BIGINT: bigint;
}

/**
 * Map a SQLite type to a TypeScript type
 */
export type MapSqliteType<T extends keyof SqliteTypeMap> = SqliteTypeMap[T];

// =============================================================================
// GENERATED COLUMN TYPES
// =============================================================================

/**
 * Mark a column as generated (auto-increment, computed, etc.)
 */
export type Generated<T> = T;

/**
 * Mark a column as having a default value
 */
export type ColumnDefault<T> = T | undefined;

/**
 * Mark a column as nullable
 */
export type Nullable<T> = T | null;

// =============================================================================
// ERROR TYPES
// =============================================================================

/**
 * DoSQL Kysely error
 */
export class DoSQLKyselyError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly query?: string,
    public readonly parameters?: unknown[]
  ) {
    super(message);
    this.name = 'DoSQLKyselyError';
  }
}

/**
 * Query execution error
 */
export class QueryExecutionError extends DoSQLKyselyError {
  constructor(message: string, query?: string, parameters?: unknown[]) {
    super(message, 'QUERY_EXECUTION_ERROR', query, parameters);
    this.name = 'QueryExecutionError';
  }
}

/**
 * Connection error
 */
export class ConnectionError extends DoSQLKyselyError {
  constructor(message: string) {
    super(message, 'CONNECTION_ERROR');
    this.name = 'ConnectionError';
  }
}
