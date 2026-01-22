/**
 * DoSQL Knex Integration - Types
 *
 * Type definitions for the Knex query builder integration with DoSQL backend.
 */

import type { SqlValue, Row, QueryResult } from '../../engine/types.js';

// =============================================================================
// DOSQL BACKEND INTERFACE
// =============================================================================

/**
 * Result from a DoSQL query execution
 */
export interface DoSQLQueryResult {
  /** Rows returned by the query */
  rows: Row[];
  /** Number of rows affected (for INSERT/UPDATE/DELETE) */
  rowsAffected?: number;
  /** Last inserted row ID (for INSERT) */
  lastInsertRowId?: number | bigint;
  /** Column metadata */
  columns?: { name: string; type: string }[];
}

/**
 * DoSQL Backend interface that Knex client uses for query execution
 */
export interface DoSQLBackend {
  /**
   * Execute a SQL query with parameter bindings
   * @param sql - The SQL query string with placeholders
   * @param bindings - Parameter values to bind
   * @returns Query result with rows and metadata
   */
  query(sql: string, bindings?: SqlValue[]): Promise<DoSQLQueryResult>;

  /**
   * Execute a SQL statement that modifies data
   * @param sql - The SQL statement (INSERT/UPDATE/DELETE)
   * @param bindings - Parameter values to bind
   * @returns Result with affected row count and optional last insert ID
   */
  exec(sql: string, bindings?: SqlValue[]): Promise<DoSQLQueryResult>;

  /**
   * Begin a transaction
   * @returns Transaction ID
   */
  beginTransaction?(): Promise<string>;

  /**
   * Commit a transaction
   * @param transactionId - The transaction to commit
   */
  commit?(transactionId: string): Promise<void>;

  /**
   * Rollback a transaction
   * @param transactionId - The transaction to rollback
   */
  rollback?(transactionId: string): Promise<void>;

  /**
   * Get the current transaction ID (if in a transaction)
   */
  getTransactionId?(): string | undefined;
}

// =============================================================================
// KNEX CLIENT CONFIGURATION
// =============================================================================

/**
 * Configuration for DoSQL Knex client
 */
export interface DoSQLKnexConfig {
  /** The DoSQL backend to use for query execution */
  backend: DoSQLBackend;

  /** Enable query logging */
  debug?: boolean;

  /** Custom log function */
  log?: {
    warn?: (message: string) => void;
    error?: (message: string) => void;
    debug?: (message: string) => void;
  };

  /** Pool configuration (ignored for DoSQL, included for Knex compatibility) */
  pool?: {
    min?: number;
    max?: number;
  };
}

// =============================================================================
// KNEX CLIENT TYPES
// =============================================================================

/**
 * Query object passed to _query method
 */
export interface KnexQueryObject {
  /** The SQL query string */
  sql: string;
  /** Parameter bindings */
  bindings?: SqlValue[];
  /** Query method (select, insert, update, delete, raw) */
  method?: string;
  /** Options */
  options?: {
    returning?: string | string[];
  };
  /** Timeout in milliseconds */
  timeout?: number;
  /** Cancel on timeout */
  cancelOnTimeout?: boolean;
}

/**
 * Response format from Knex operations
 */
export interface KnexResponse {
  /** Selected rows (for SELECT) */
  rows?: Row[];
  /** Row count (for INSERT/UPDATE/DELETE) */
  rowCount?: number;
  /** Last inserted ID */
  lastID?: number | bigint;
  /** Columns in result */
  fields?: { name: string; type?: string }[];
}

/**
 * Knex transaction interface
 */
export interface DoSQLKnexTransaction {
  /** Transaction ID */
  id: string;
  /** Whether the transaction is completed */
  completed: boolean;
  /** Commit the transaction */
  commit(): Promise<void>;
  /** Rollback the transaction */
  rollback(): Promise<void>;
}

// =============================================================================
// UTILITY TYPES
// =============================================================================

/**
 * SQLite data types
 */
export type SQLiteType = 'INTEGER' | 'REAL' | 'TEXT' | 'BLOB' | 'NULL';

/**
 * Column definition for schema operations
 */
export interface ColumnDefinition {
  name: string;
  type: SQLiteType;
  nullable: boolean;
  primaryKey: boolean;
  autoIncrement: boolean;
  defaultValue?: SqlValue;
  unique: boolean;
}

/**
 * Table definition for schema operations
 */
export interface TableDefinition {
  name: string;
  columns: ColumnDefinition[];
  primaryKey?: string[];
  unique?: string[][];
  indexes?: { name: string; columns: string[]; unique: boolean }[];
}
