/**
 * DoSQL Kysely Integration
 *
 * Type-safe SQL query builder for DoSQL using Kysely.
 *
 * @packageDocumentation
 *
 * @example
 * ```typescript
 * import { Kysely } from 'kysely';
 * import { DoSQLDialect, createDoSQLKysely } from 'dosql/orm/kysely';
 *
 * // Define your database schema
 * interface Database {
 *   users: {
 *     id: number;
 *     name: string;
 *     email: string;
 *     created_at: Date;
 *   };
 *   posts: {
 *     id: number;
 *     user_id: number;
 *     title: string;
 *     content: string;
 *     published: boolean;
 *   };
 * }
 *
 * // Option 1: Create Kysely instance with dialect
 * const db = new Kysely<Database>({
 *   dialect: new DoSQLDialect({ backend: dosqlBackend }),
 * });
 *
 * // Option 2: Use factory function
 * const db = createDoSQLKysely<Database>(dosqlBackend);
 *
 * // Execute type-safe queries
 * const users = await db
 *   .selectFrom('users')
 *   .select(['id', 'name', 'email'])
 *   .where('id', '=', 1)
 *   .execute();
 * // users is typed as { id: number; name: string; email: string }[]
 *
 * // Type-safe joins
 * const postsWithAuthors = await db
 *   .selectFrom('posts')
 *   .innerJoin('users', 'users.id', 'posts.user_id')
 *   .select(['posts.title', 'users.name as author'])
 *   .where('posts.published', '=', true)
 *   .execute();
 *
 * // Type-safe inserts
 * await db.insertInto('users')
 *   .values({ name: 'John', email: 'john@example.com' })
 *   .execute();
 *
 * // Type-safe updates
 * await db.updateTable('users')
 *   .set({ name: 'Jane' })
 *   .where('id', '=', 1)
 *   .execute();
 *
 * // Transactions
 * await db.transaction().execute(async (trx) => {
 *   await trx.insertInto('users').values({ name: 'Bob', email: 'bob@example.com' }).execute();
 *   await trx.insertInto('posts').values({ user_id: 1, title: 'Hello', content: 'World', published: true }).execute();
 * });
 * ```
 */

// =============================================================================
// DIALECT EXPORTS
// =============================================================================

export {
  DoSQLDialect,
  createDoSQLDialect,
  createDoSQLKysely,
  type InferKyselyDatabase,
  type TypedKysely,
} from './dialect.js';

// =============================================================================
// DRIVER EXPORTS
// =============================================================================

export {
  DoSQLDriver,
  createDoSQLDriver,
} from './driver.js';

// =============================================================================
// TYPE EXPORTS
// =============================================================================

export {
  // Backend types
  type DoSQLBackend,
  type DoSQLDialectConfig,
  type DoSQLKyselyResult,

  // Utility types
  type InferDatabase,
  type Updateable,
  type Insertable,
  type Selectable,

  // Column types
  type SqliteTypeMap,
  type MapSqliteType,
  type Generated,
  type ColumnDefault,
  type Nullable,

  // Error types
  DoSQLKyselyError,
  QueryExecutionError,
  ConnectionError,
} from './types.js';

// =============================================================================
// RE-EXPORT USEFUL KYSELY TYPES
// =============================================================================

// These are re-exported for convenience so users don't need to import
// from both packages
export type {
  Kysely,
  Transaction,
  SelectQueryBuilder,
  InsertQueryBuilder,
  UpdateQueryBuilder,
  DeleteQueryBuilder,
  ExpressionBuilder,
  RawBuilder,
  CompiledQuery,
  QueryResult,
} from 'kysely';
