/**
 * DoSQL Kysely Dialect
 *
 * Implements Kysely's Dialect interface for DoSQL.
 * Uses SQLite's query compiler since DoSQL is SQLite-compatible.
 */

import {
  Dialect,
  DialectAdapter,
  Driver,
  Kysely,
  QueryCompiler,
  DatabaseIntrospector,
  SqliteAdapter,
  SqliteIntrospector,
  SqliteQueryCompiler,
} from 'kysely';

import { DoSQLDriver } from './driver.js';
import type { DoSQLDialectConfig, DoSQLBackend } from './types.js';

// =============================================================================
// DOSQL DIALECT
// =============================================================================

/**
 * DoSQL dialect for Kysely.
 *
 * This dialect connects Kysely's type-safe query builder to the DoSQL
 * execution engine. It uses SQLite's query compiler since DoSQL maintains
 * SQLite SQL compatibility.
 *
 * @example
 * ```typescript
 * import { Kysely } from 'kysely';
 * import { DoSQLDialect } from '@dotdo/dosql/orm/kysely';
 *
 * interface Database {
 *   users: { id: number; name: string; email: string };
 *   posts: { id: number; userId: number; title: string };
 * }
 *
 * const db = new Kysely<Database>({
 *   dialect: new DoSQLDialect({ backend: dosqlBackend }),
 * });
 *
 * const users = await db.selectFrom('users')
 *   .selectAll()
 *   .where('id', '=', 1)
 *   .execute();
 * ```
 */
export class DoSQLDialect implements Dialect {
  private readonly config: DoSQLDialectConfig;

  /**
   * Create a new DoSQL dialect.
   *
   * @param config - Dialect configuration with DoSQL backend
   */
  constructor(config: DoSQLDialectConfig) {
    this.config = config;
  }

  /**
   * Create the DoSQL driver.
   * The driver handles connection management and query execution.
   */
  createDriver(): Driver {
    return new DoSQLDriver(this.config);
  }

  /**
   * Create the query compiler.
   * DoSQL uses SQLite's query compiler for SQL generation.
   */
  createQueryCompiler(): QueryCompiler {
    return new SqliteQueryCompiler();
  }

  /**
   * Create the dialect adapter.
   * Handles dialect-specific SQL transformations.
   */
  createAdapter(): DialectAdapter {
    return new SqliteAdapter();
  }

  /**
   * Create the database introspector.
   * Used for schema introspection (migrations, etc.).
   */
  createIntrospector(db: Kysely<unknown>): DatabaseIntrospector {
    return new SqliteIntrospector(db);
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a DoSQL dialect with the given configuration.
 *
 * @param config - Dialect configuration
 * @returns Configured DoSQL dialect
 *
 * @example
 * ```typescript
 * const dialect = createDoSQLDialect({
 *   backend: myDoSQLBackend,
 *   log: true,
 * });
 * ```
 */
export function createDoSQLDialect(config: DoSQLDialectConfig): DoSQLDialect {
  return new DoSQLDialect(config);
}

/**
 * Create a Kysely instance configured with DoSQL.
 *
 * @param backend - DoSQL backend instance
 * @param options - Additional dialect options
 * @returns Configured Kysely instance
 *
 * @example
 * ```typescript
 * interface Database {
 *   users: { id: number; name: string };
 * }
 *
 * const db = createDoSQLKysely<Database>(myBackend, { log: true });
 * ```
 */
export function createDoSQLKysely<DB>(
  backend: DoSQLBackend,
  options?: Omit<DoSQLDialectConfig, 'backend'>
): Kysely<DB> {
  return new Kysely<DB>({
    dialect: new DoSQLDialect({
      backend,
      ...options,
    }),
  });
}

// =============================================================================
// TYPE HELPERS
// =============================================================================

/**
 * Helper type to extract the database type from a Kysely instance.
 */
export type InferKyselyDatabase<K> = K extends Kysely<infer DB> ? DB : never;

/**
 * Helper type to create a typed Kysely instance.
 */
export type TypedKysely<DB> = Kysely<DB>;
