/**
 * DoSQL Drizzle ORM Integration
 *
 * Provides a Drizzle ORM adapter for DoSQL, enabling type-safe SQL queries
 * with DoSQL's TypeScript-native SQLite backend.
 *
 * @example
 * ```typescript
 * import { drizzle } from '@dotdo/dosql/orm/drizzle';
 * import { sqliteTable, text, integer } from 'drizzle-orm/sqlite-core';
 * import { eq } from 'drizzle-orm';
 *
 * // Define schema using Drizzle
 * const users = sqliteTable('users', {
 *   id: integer('id').primaryKey(),
 *   name: text('name').notNull(),
 *   email: text('email').notNull(),
 * });
 *
 * // Create DoSQL-backed Drizzle instance
 * const db = drizzle({ backend: dosqlBackend });
 *
 * // Enjoy type-safe queries!
 * const allUsers = await db.select().from(users);
 * const user = await db.select().from(users).where(eq(users.id, 1));
 * ```
 *
 * @module
 */

// Main drizzle function and database type
export { drizzle, DoSQLDrizzleDatabase, type DoSQLDatabase } from './adapter.js';

// Session and transaction types
export { DoSQLSession, DoSQLTransaction, DoSQLPreparedQuery } from './session.js';

// Dialect
export { DoSQLDialect } from './dialect.js';

// Types
export type {
  DoSQLBackend,
  DoSQLDrizzleConfig,
  DoSQLLogger,
  DoSQLRunResult,
} from './types.js';
