/**
 * DoSQL Drizzle ORM Integration - Types
 *
 * Core type definitions for the DoSQL + Drizzle integration.
 */

import type { ExecutionContext, QueryResult, Row, SqlValue } from '../../engine/types.js';

/**
 * DoSQL backend interface for Drizzle integration.
 * This is the low-level query execution interface that DoSQL provides.
 */
export interface DoSQLBackend {
  /**
   * Execute a SQL query and return all rows.
   */
  all<T = Row>(sql: string, params?: SqlValue[]): Promise<T[]>;

  /**
   * Execute a SQL query and return the first row.
   */
  get<T = Row>(sql: string, params?: SqlValue[]): Promise<T | undefined>;

  /**
   * Execute a SQL query (for INSERT, UPDATE, DELETE).
   * Returns the number of rows affected.
   */
  run(sql: string, params?: SqlValue[]): Promise<{ rowsAffected: number; lastInsertRowId?: number | bigint }>;

  /**
   * Execute a SQL query and return raw values (arrays instead of objects).
   */
  values<T extends unknown[] = unknown[]>(sql: string, params?: SqlValue[]): Promise<T[]>;

  /**
   * Execute multiple statements in a transaction.
   */
  transaction<T>(fn: (tx: DoSQLBackend) => Promise<T>): Promise<T>;

  /**
   * Get the execution context (optional).
   */
  getContext?(): ExecutionContext;
}

/**
 * Result type for run operations (INSERT, UPDATE, DELETE).
 */
export interface DoSQLRunResult {
  rowsAffected: number;
  lastInsertRowId?: number | bigint;
}

/**
 * Logger interface for query logging.
 */
export interface DoSQLLogger {
  logQuery(query: string, params: unknown[]): void;
}

/**
 * Configuration options for the DoSQL Drizzle adapter.
 */
export interface DoSQLDrizzleConfig<TSchema extends Record<string, unknown> = Record<string, never>> {
  /**
   * The DoSQL backend to use for query execution.
   */
  backend: DoSQLBackend;

  /**
   * Optional schema for type-safe queries with relations.
   */
  schema?: TSchema;

  /**
   * Optional logger for query logging.
   */
  logger?: DoSQLLogger | boolean;

  /**
   * Column casing mode.
   */
  casing?: 'snake_case' | 'camelCase';
}
