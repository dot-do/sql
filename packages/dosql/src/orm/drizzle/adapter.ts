/**
 * DoSQL Drizzle ORM Integration - Adapter
 *
 * Main entry point for creating a Drizzle database instance with DoSQL backend.
 * This module provides the `drizzle()` function that creates a type-safe ORM interface.
 */

import { entityKind } from 'drizzle-orm';
import { DefaultLogger } from 'drizzle-orm/logger';
import type { Logger } from 'drizzle-orm/logger';
import {
  createTableRelationsHelpers,
  extractTablesRelationalConfig,
  type ExtractTablesWithRelations,
  type RelationalSchemaConfig,
  type TablesRelationalConfig,
} from 'drizzle-orm/relations';
import { BaseSQLiteDatabase } from 'drizzle-orm/sqlite-core/db';

import { DoSQLDialect } from './dialect.js';
import { DoSQLSession } from './session.js';
import type { DoSQLBackend, DoSQLDrizzleConfig, DoSQLRunResult } from './types.js';

/**
 * DoSQL Drizzle Database - Type-safe database instance for DoSQL.
 *
 * This class extends BaseSQLiteDatabase with async execution mode,
 * providing all standard Drizzle query methods:
 * - `select()` - Build SELECT queries
 * - `insert()` - Build INSERT queries
 * - `update()` - Build UPDATE queries
 * - `delete()` - Build DELETE queries
 * - `run()` - Execute raw SQL
 * - `all()` - Execute raw SQL and return all rows
 * - `get()` - Execute raw SQL and return first row
 * - `transaction()` - Execute queries in a transaction
 */
export class DoSQLDrizzleDatabase<
  TSchema extends Record<string, unknown> = Record<string, never>,
> extends BaseSQLiteDatabase<
  'async',
  DoSQLRunResult,
  TSchema,
  ExtractTablesWithRelations<TSchema>
> {
  static readonly [entityKind]: string = 'DoSQLDrizzleDatabase';
}

/**
 * Drizzle ORM database type with DoSQL backend attached.
 */
export type DoSQLDatabase<TSchema extends Record<string, unknown>> = DoSQLDrizzleDatabase<TSchema> & {
  $client: DoSQLBackend;
};

/**
 * Create a Drizzle ORM database instance with DoSQL backend.
 *
 * @example Basic usage
 * ```typescript
 * import { drizzle } from 'dosql/orm/drizzle';
 * import { sqliteTable, text, integer } from 'drizzle-orm/sqlite-core';
 *
 * // Define schema
 * const users = sqliteTable('users', {
 *   id: integer('id').primaryKey(),
 *   name: text('name').notNull(),
 *   email: text('email').notNull(),
 * });
 *
 * // Create database
 * const db = drizzle({ backend: dosqlBackend });
 *
 * // Query
 * const allUsers = await db.select().from(users);
 * const user = await db.select().from(users).where(eq(users.id, 1));
 * ```
 *
 * @example With schema for relations
 * ```typescript
 * import { drizzle } from 'dosql/orm/drizzle';
 * import * as schema from './schema';
 *
 * const db = drizzle({
 *   backend: dosqlBackend,
 *   schema,
 * });
 *
 * // Use relational queries
 * const usersWithPosts = await db.query.users.findMany({
 *   with: { posts: true },
 * });
 * ```
 *
 * @example With logging
 * ```typescript
 * const db = drizzle({
 *   backend: dosqlBackend,
 *   logger: true, // Use default logger
 * });
 *
 * // Or with custom logger
 * const db = drizzle({
 *   backend: dosqlBackend,
 *   logger: {
 *     logQuery(query, params) {
 *       console.log('Query:', query, 'Params:', params);
 *     },
 *   },
 * });
 * ```
 *
 * @param config Configuration object with DoSQL backend and optional schema/logger.
 * @returns A Drizzle database instance configured for DoSQL.
 */
export function drizzle<TSchema extends Record<string, unknown> = Record<string, never>>(
  config: DoSQLDrizzleConfig<TSchema>,
): DoSQLDatabase<TSchema> {
  const { backend, schema: schemaConfig, logger: loggerConfig, casing } = config;

  // Create dialect with optional casing
  const dialect = new DoSQLDialect({ casing });

  // Configure logger
  let logger: Logger | undefined;
  if (loggerConfig === true) {
    logger = new DefaultLogger();
  } else if (loggerConfig && typeof loggerConfig === 'object') {
    logger = loggerConfig;
  }

  // Configure schema for relational queries
  let schema: RelationalSchemaConfig<TablesRelationalConfig> | undefined;
  if (schemaConfig) {
    const tablesConfig = extractTablesRelationalConfig(
      schemaConfig,
      createTableRelationsHelpers,
    );
    schema = {
      fullSchema: schemaConfig,
      schema: tablesConfig.tables,
      tableNamesMap: tablesConfig.tableNamesMap,
    };
  }

  // Create session with backend
  const session = new DoSQLSession(backend, dialect, schema, { logger });

  // Create database instance
  const db = new DoSQLDrizzleDatabase(
    'async',
    dialect,
    session,
    schema as RelationalSchemaConfig<ExtractTablesWithRelations<TSchema>> | undefined,
  );

  // Attach client for direct access
  (db as DoSQLDatabase<TSchema>).$client = backend;

  return db as DoSQLDatabase<TSchema>;
}
