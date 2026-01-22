/**
 * DoSQL Drizzle ORM Integration - Dialect
 *
 * Extends SQLite dialect with DoSQL-specific SQL generation and execution routing.
 * DoSQL uses an async execution model similar to better-sqlite3's async mode.
 */

import { entityKind } from 'drizzle-orm';
import { SQLiteAsyncDialect } from 'drizzle-orm/sqlite-core/dialect';
import type { SQLiteDialectConfig } from 'drizzle-orm/sqlite-core/dialect';

/**
 * DoSQL Dialect - Async SQLite dialect for DoSQL backend.
 *
 * This dialect extends SQLiteAsyncDialect and can be used to customize
 * SQL generation for DoSQL-specific features in the future.
 *
 * Current features:
 * - Standard SQLite SQL generation
 * - Async query execution
 * - Parameter binding with positional placeholders
 *
 * Future extensions could include:
 * - DoSQL-specific optimizations
 * - B-tree vs columnar routing hints
 * - Time-travel query syntax
 * - CDC subscription hints
 */
export class DoSQLDialect extends SQLiteAsyncDialect {
  static readonly [entityKind]: string = 'DoSQLDialect';

  constructor(config?: SQLiteDialectConfig) {
    super(config);
  }

  /**
   * Escape a parameter placeholder.
   *
   * DoSQL uses `?` for positional parameters like standard SQLite.
   * This can be extended for named parameters if needed.
   */
  override escapeParam(num: number): string {
    return '?';
  }

  /**
   * Escape a string value for SQL.
   *
   * Uses standard SQLite string escaping with single quotes.
   */
  override escapeString(str: string): string {
    return `'${str.replace(/'/g, "''")}'`;
  }
}

// Re-export the base dialect for compatibility
export { SQLiteAsyncDialect };
