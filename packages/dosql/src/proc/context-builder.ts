/**
 * Context Builder for Functional Procedures
 *
 * Builds the typed database context used by functional procedures.
 * Provides a fluent API for constructing the `db` object with typed table accessors.
 *
 * @example
 * ```typescript
 * const db = createFunctionalDb<MyDB>({
 *   adapters: {
 *     users: userAdapter,
 *     orders: orderAdapter,
 *   },
 *   sqlExecutor,
 *   transactionManager,
 * });
 *
 * // Now use in defineProcedures
 * const procedures = defineProcedures({ ... }, { db });
 * ```
 *
 * @packageDocumentation
 */

import type {
  DatabaseSchema,
  TableSchema,
  TableAccessor,
  TableSchemaToRecord,
  TransactionContext,
  TransactionFunction,
  SqlFunction,
} from './types.js';
import type { FunctionalDb } from './functional.js';
import {
  createTableAccessor,
  createSqlFunction,
  createTransactionFunction,
  type StorageAdapter,
  type SqlExecutor,
  type TransactionManager,
} from './context.js';

// =============================================================================
// FUNCTIONAL DB BUILDER
// =============================================================================

/**
 * Options for creating a functional database context.
 */
export interface FunctionalDbOptions<DB extends DatabaseSchema> {
  /** Storage adapters for each table */
  adapters: {
    [K in keyof DB]: StorageAdapter<TableSchemaToRecord<DB[K]>>
  };

  /** SQL executor for raw queries */
  sqlExecutor: SqlExecutor;

  /** Transaction manager */
  transactionManager: TransactionManager;
}

/**
 * Create a functional database context for use with defineProcedures.
 *
 * @example
 * ```typescript
 * interface MyDB extends DatabaseSchema {
 *   users: { id: 'number'; name: 'string'; email: 'string' };
 *   orders: { id: 'number'; userId: 'number'; totalAmount: 'number' };
 * }
 *
 * const db = createFunctionalDb<MyDB>({
 *   adapters: {
 *     users: createInMemoryAdapter(usersData),
 *     orders: createInMemoryAdapter(ordersData),
 *   },
 *   sqlExecutor: createInMemorySqlExecutor(tableMap),
 *   transactionManager: createInMemoryTransactionManager(),
 * });
 *
 * // Tables are accessible directly
 * const users = await db.users.all();
 * const order = await db.orders.get(1);
 *
 * // Raw SQL via template literal
 * const results = await db.sql`SELECT * FROM users WHERE active = ${true}`;
 *
 * // Transactions
 * await db.transaction(async (tx) => {
 *   await tx.tables.users.update({ id: 1 }, { name: 'Updated' });
 * });
 * ```
 */
export function createFunctionalDb<DB extends DatabaseSchema>(
  options: FunctionalDbOptions<DB>
): FunctionalDb<DB> {
  const { adapters, sqlExecutor, transactionManager } = options;

  // Create the table adapters map for transaction support
  const tableAdaptersMap = new Map<string, StorageAdapter<Record<string, unknown>>>();

  // Build the db object with table accessors
  const db: Record<string, unknown> = {};

  for (const tableName of Object.keys(adapters)) {
    const adapter = adapters[tableName as keyof typeof adapters] as StorageAdapter<Record<string, unknown>>;
    db[tableName] = createTableAccessor(adapter);
    tableAdaptersMap.set(tableName, adapter);
  }

  // Add sql function
  db.sql = createSqlFunction(sqlExecutor);

  // Add transaction function
  db.transaction = createTransactionFunction<DB>(transactionManager, tableAdaptersMap);

  return db as FunctionalDb<DB>;
}

// =============================================================================
// FLUENT BUILDER API
// =============================================================================

/**
 * Fluent builder for creating functional database contexts.
 */
export class FunctionalDbBuilder<DB extends DatabaseSchema> {
  private _adapters: Partial<{
    [K in keyof DB]: StorageAdapter<TableSchemaToRecord<DB[K]>>
  }> = {};
  private _sqlExecutor?: SqlExecutor;
  private _transactionManager?: TransactionManager;

  /**
   * Add a table adapter.
   */
  table<K extends keyof DB>(
    name: K,
    adapter: StorageAdapter<TableSchemaToRecord<DB[K]>>
  ): this {
    this._adapters[name] = adapter;
    return this;
  }

  /**
   * Set the SQL executor.
   */
  sql(executor: SqlExecutor): this {
    this._sqlExecutor = executor;
    return this;
  }

  /**
   * Set the transaction manager.
   */
  transactions(manager: TransactionManager): this {
    this._transactionManager = manager;
    return this;
  }

  /**
   * Build the functional database context.
   */
  build(): FunctionalDb<DB> {
    if (!this._sqlExecutor) {
      throw new Error('SQL executor is required. Call .sql(executor) before .build()');
    }
    if (!this._transactionManager) {
      throw new Error('Transaction manager is required. Call .transactions(manager) before .build()');
    }

    // Verify all tables have adapters
    const tableNames = Object.keys(this._adapters);
    if (tableNames.length === 0) {
      throw new Error('At least one table adapter is required. Call .table(name, adapter) before .build()');
    }

    return createFunctionalDb<DB>({
      adapters: this._adapters as FunctionalDbOptions<DB>['adapters'],
      sqlExecutor: this._sqlExecutor,
      transactionManager: this._transactionManager,
    });
  }
}

/**
 * Create a new fluent builder for functional database contexts.
 *
 * @example
 * ```typescript
 * const db = functionalDb<MyDB>()
 *   .table('users', userAdapter)
 *   .table('orders', orderAdapter)
 *   .sql(sqlExecutor)
 *   .transactions(txManager)
 *   .build();
 * ```
 */
export function functionalDb<DB extends DatabaseSchema>(): FunctionalDbBuilder<DB> {
  return new FunctionalDbBuilder<DB>();
}

// =============================================================================
// IN-MEMORY HELPERS
// =============================================================================

import {
  createInMemoryAdapter,
  createInMemorySqlExecutor,
  createInMemoryTransactionManager,
} from './context.js';

/**
 * Create a simple in-memory functional database for testing.
 *
 * @example
 * ```typescript
 * const db = createInMemoryFunctionalDb<MyDB>({
 *   users: [
 *     { id: 1, name: 'Alice', email: 'alice@example.com' },
 *     { id: 2, name: 'Bob', email: 'bob@example.com' },
 *   ],
 *   orders: [
 *     { id: 1, userId: 1, totalAmount: 100 },
 *   ],
 * });
 *
 * // Ready to use with procedures
 * const procedures = defineProcedures({ ... }, { db });
 * ```
 */
export function createInMemoryFunctionalDb<DB extends DatabaseSchema>(
  data: {
    [K in keyof DB]?: Array<TableSchemaToRecord<DB[K]>>
  }
): FunctionalDb<DB> {
  // Create adapters from data
  const adapters: Record<string, StorageAdapter<Record<string, unknown>>> = {};
  const tableMap = new Map<string, StorageAdapter<Record<string, unknown>>>();

  for (const [tableName, tableData] of Object.entries(data)) {
    const adapter = createInMemoryAdapter(tableData as Array<Record<string, unknown>>);
    adapters[tableName] = adapter;
    tableMap.set(tableName, adapter);
  }

  return createFunctionalDb<DB>({
    adapters: adapters as FunctionalDbOptions<DB>['adapters'],
    sqlExecutor: createInMemorySqlExecutor(tableMap),
    transactionManager: createInMemoryTransactionManager(),
  });
}

// =============================================================================
// SQL TEMPLATE LITERAL HELPER
// =============================================================================

/**
 * SQL template literal tag for use outside of procedures.
 * Creates a raw SQL expression that can be used in update operations.
 *
 * @example
 * ```typescript
 * // Use in procedures for raw SQL expressions
 * await db.accounts.update({ id: fromId }, {
 *   balance: sql`balance - ${amount}`
 * });
 * ```
 */
export function sql(
  strings: TemplateStringsArray,
  ...values: unknown[]
): { __sql: true; query: string; params: unknown[] } {
  let query = strings[0];
  for (let i = 0; i < values.length; i++) {
    query += `$${i + 1}${strings[i + 1]}`;
  }

  return {
    __sql: true,
    query,
    params: values,
  };
}

/**
 * Type guard to check if a value is a raw SQL expression.
 */
export function isSqlExpression(
  value: unknown
): value is { __sql: true; query: string; params: unknown[] } {
  return (
    typeof value === 'object' &&
    value !== null &&
    '__sql' in value &&
    (value as { __sql: unknown }).__sql === true
  );
}

// =============================================================================
// CONTEXT EXTENSION HELPERS
// =============================================================================

/**
 * Extended context type with additional properties.
 */
export interface ExtendedContext<
  DB extends DatabaseSchema,
  Extensions extends Record<string, unknown>
> {
  db: FunctionalDb<DB>;
  env?: Record<string, string>;
  requestId?: string;
  /** Additional context extensions */
  extensions: Extensions;
}

/**
 * Create an extended context builder for adding custom properties.
 *
 * @example
 * ```typescript
 * const extendedDb = extendContext<MyDB>()(db)
 *   .with('cache', redisClient)
 *   .with('logger', logger)
 *   .build();
 *
 * // Now procedures can access: ctx.extensions.cache, ctx.extensions.logger
 * ```
 */
export function extendContext<DB extends DatabaseSchema>() {
  return function (db: FunctionalDb<DB>) {
    return new ContextExtender<DB, {}>(db, {});
  };
}

/**
 * Fluent builder for extending context.
 */
class ContextExtender<
  DB extends DatabaseSchema,
  Extensions extends Record<string, unknown>
> {
  constructor(
    private db: FunctionalDb<DB>,
    private extensions: Extensions
  ) {}

  /**
   * Add an extension to the context.
   */
  with<K extends string, V>(
    key: K,
    value: V
  ): ContextExtender<DB, Extensions & { [P in K]: V }> {
    return new ContextExtender(
      this.db,
      { ...this.extensions, [key]: value } as Extensions & { [P in K]: V }
    );
  }

  /**
   * Build the extended context.
   */
  build(): { db: FunctionalDb<DB>; extensions: Extensions } {
    return {
      db: this.db,
      extensions: this.extensions,
    };
  }
}

// =============================================================================
// TYPE UTILITIES
// =============================================================================

/**
 * Extract the database schema type from a FunctionalDb instance.
 */
export type ExtractSchema<T> = T extends FunctionalDb<infer DB> ? DB : never;

/**
 * Create a typed table accessor type for a specific table.
 */
export type TypedTableAccessor<
  DB extends DatabaseSchema,
  TableName extends keyof DB
> = TableAccessor<TableSchemaToRecord<DB[TableName]>>;
