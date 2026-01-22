/**
 * DoSQL Knex Integration
 *
 * Provides Knex query builder integration with DoSQL backend.
 * Enables using Knex's fluent API to build and execute SQL queries
 * against a DoSQL storage engine.
 *
 * @example
 * ```typescript
 * import { createKnex } from '@dotdo/dosql/orm/knex';
 *
 * const knex = createKnex({ backend: dosqlBackend });
 *
 * // Select with conditions
 * const users = await knex('users')
 *   .where('age', '>', 21)
 *   .select('id', 'name');
 *
 * // Insert
 * const [id] = await knex('users').insert({
 *   name: 'Alice',
 *   email: 'alice@example.com',
 * });
 *
 * // Update
 * const affected = await knex('users')
 *   .where('id', id)
 *   .update({ name: 'Alice Smith' });
 *
 * // Delete
 * await knex('users').where('id', id).delete();
 *
 * // Joins
 * const ordersWithUsers = await knex('orders')
 *   .join('users', 'orders.user_id', 'users.id')
 *   .select('orders.*', 'users.name');
 *
 * // Transactions
 * await knex.transaction(async (trx) => {
 *   const [userId] = await trx('users').insert({ name: 'Bob' });
 *   await trx('orders').insert({ user_id: userId, total: 100 });
 * });
 * ```
 */

import knex, { Knex } from 'knex';
import { DoSQLKnexClient, DoSQLConnection } from './client.js';
import type { DoSQLKnexConfig, DoSQLBackend, DoSQLQueryResult } from './types.js';
import type { SqlValue, Row } from '../../engine/types.js';

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a Knex instance configured to use a DoSQL backend
 *
 * @param config - Configuration with DoSQL backend
 * @returns Knex instance connected to DoSQL
 *
 * @example
 * ```typescript
 * import { createKnex } from '@dotdo/dosql/orm/knex';
 * import { createBackend } from '@dotdo/dosql';
 *
 * const backend = createBackend({ storage: durableObjectStorage });
 * const knex = createKnex({ backend });
 *
 * const users = await knex('users').select('*');
 * ```
 */
export function createKnex(config: DoSQLKnexConfig): Knex {
  // Create the DoSQL client instance
  const client = new DoSQLKnexClient(config);

  // Create Knex with a custom client class that doesn't require native sqlite3
  // We use a minimal custom client that wraps our DoSQL backend
  const CustomClient = class extends (knex.Client as any) {
    constructor(cfg: any) {
      super(cfg);
      this._driver = () => ({});
      this.dialect = 'sqlite3';
      this.driverName = 'dosql';
    }

    initializeDriver() {
      // No-op: DoSQL doesn't need a native driver
    }

    initializePool() {
      // No-op: DoSQL manages its own connections
    }

    destroyRawConnection() {
      return Promise.resolve();
    }

    validateConnection() {
      return Promise.resolve(true);
    }

    acquireRawConnection() {
      return Promise.resolve({});
    }
  };

  // Create Knex with our custom client
  const instance = knex({
    // Use our custom client
    client: CustomClient as any,

    // Empty connection (handled by DoSQL backend)
    connection: {},

    // SQLite-specific settings
    useNullAsDefault: true,

    // Pool settings (not really used with DoSQL)
    pool: {
      min: 1,
      max: 1,
    },

    // Logging
    debug: config.debug,
    log: config.log,

    // Acquire connection wrapper
    acquireConnectionTimeout: 10000,
  });

  // Override the internal client with our DoSQL client
  // This is a workaround since Knex doesn't have a clean plugin API
  const originalRaw = instance.raw.bind(instance);

  // Create a wrapper that routes through DoSQL
  const wrappedInstance = new Proxy(instance, {
    get(target, prop) {
      if (prop === '_dosql') {
        return {
          client,
          backend: config.backend,
          config,
        };
      }

      // Override query execution
      if (prop === 'raw') {
        return (sql: string, bindings?: unknown[]) => {
          const rawQuery = originalRaw(sql, bindings);

          // Override the then method to route through DoSQL
          const originalThen = rawQuery.then.bind(rawQuery);

          rawQuery.then = async (
            onFulfilled?: (value: unknown) => unknown,
            onRejected?: (reason: unknown) => unknown
          ) => {
            try {
              const conn = await client.acquireConnection();
              const result = await client._query(conn, {
                sql,
                bindings: bindings as SqlValue[],
                method: 'raw',
              });
              const processed = client.processResponse(result, { method: 'raw' });

              if (onFulfilled) {
                return onFulfilled(processed);
              }
              return processed;
            } catch (error) {
              if (onRejected) {
                return onRejected(error);
              }
              throw error;
            }
          };

          return rawQuery;
        };
      }

      return Reflect.get(target, prop);
    },
  });

  // Patch the query builder to use DoSQL backend
  patchQueryBuilder(wrappedInstance, client);

  return wrappedInstance;
}

/**
 * Patch Knex query builder to route queries through DoSQL
 */
function patchQueryBuilder(knexInstance: Knex, client: DoSQLKnexClient): void {
  // Store original methods
  const originalQueryBuilder = knexInstance.queryBuilder;

  // Get the query builder prototype
  const QueryBuilder = (knexInstance as unknown as { client: { QueryBuilder: new () => Knex.QueryBuilder } }).client?.QueryBuilder;

  if (QueryBuilder) {
    const proto = QueryBuilder.prototype;
    const originalThen = proto.then;

    // Override then to route through DoSQL
    proto.then = async function (
      this: Knex.QueryBuilder & { toSQL(): { sql: string; bindings: unknown[]; method: string } },
      onFulfilled?: (value: unknown) => unknown,
      onRejected?: (reason: unknown) => unknown
    ) {
      try {
        const queryObj = this.toSQL();
        const conn = await client.acquireConnection();
        const result = await client._query(conn, {
          sql: queryObj.sql,
          bindings: queryObj.bindings as SqlValue[],
          method: queryObj.method,
        });
        const processed = client.processResponse(result, { method: queryObj.method });

        if (onFulfilled) {
          return onFulfilled(processed);
        }
        return processed;
      } catch (error) {
        if (onRejected) {
          return onRejected(error);
        }
        throw error;
      }
    };
  }
}

// =============================================================================
// BACKEND FACTORY
// =============================================================================

/**
 * Create a DoSQL backend from an in-memory store
 * Useful for testing and development
 */
export function createInMemoryBackend(): DoSQLBackend {
  type TableRow = Record<string, SqlValue>;
  type TableMap = Map<string, TableRow>;
  const tables = new Map<string, TableMap>();
  let lastInsertId = 0;
  let transactionId: string | undefined;
  let transactionTables: Map<string, TableMap> | null = null;

  return {
    async query(sql: string, bindings: SqlValue[] = []): Promise<DoSQLQueryResult> {
      const currentTables = transactionTables ?? tables;

      // Simple SQL parser for basic operations
      const upperSql = sql.trim().toUpperCase();

      if (upperSql.startsWith('SELECT')) {
        return executeSelect(sql, bindings, currentTables);
      }

      if (upperSql.startsWith('INSERT')) {
        return executeInsert(sql, bindings, currentTables);
      }

      if (upperSql.startsWith('UPDATE')) {
        return executeUpdate(sql, bindings, currentTables);
      }

      if (upperSql.startsWith('DELETE')) {
        return executeDelete(sql, bindings, currentTables);
      }

      if (upperSql.startsWith('CREATE TABLE')) {
        return executeCreateTable(sql, currentTables);
      }

      if (upperSql.startsWith('DROP TABLE')) {
        return executeDropTable(sql, currentTables);
      }

      throw new Error(`Unsupported SQL: ${sql}`);
    },

    async exec(sql: string, bindings: SqlValue[] = []): Promise<DoSQLQueryResult> {
      return this.query(sql, bindings);
    },

    async beginTransaction() {
      transactionId = `txn_${Date.now()}_${Math.random().toString(36).slice(2)}`;
      // Clone tables for transaction
      transactionTables = new Map<string, TableMap>();
      for (const [name, table] of Array.from(tables.entries())) {
        transactionTables.set(name, new Map(table));
      }
      return transactionId;
    },

    async commit(txnId: string) {
      if (txnId !== transactionId) {
        throw new Error('Invalid transaction ID');
      }
      if (transactionTables) {
        // Apply transaction changes
        tables.clear();
        for (const [name, table] of Array.from(transactionTables.entries())) {
          tables.set(name, table);
        }
      }
      transactionId = undefined;
      transactionTables = null;
    },

    async rollback(txnId: string) {
      if (txnId !== transactionId) {
        throw new Error('Invalid transaction ID');
      }
      // Discard transaction changes
      transactionId = undefined;
      transactionTables = null;
    },

    getTransactionId() {
      return transactionId;
    },
  };

  // Simple SQL execution helpers
  function executeSelect(
    sql: string,
    bindings: SqlValue[],
    currentTables: Map<string, TableMap>
  ): DoSQLQueryResult {
    // Extract table name (very simple parser)
    const match = sql.match(/FROM\s+[`"]?(\w+)[`"]?/i);
    if (!match) {
      return { rows: [] as Row[] };
    }

    const tableName = match[1];
    const table = currentTables.get(tableName);

    if (!table) {
      return { rows: [] as Row[] };
    }

    const rows = Array.from(table.values()) as Row[];

    // Apply WHERE clause if present (basic support)
    const whereMatch = sql.match(/WHERE\s+[`"]?(\w+)[`"]?\s*(=|>|<|>=|<=|<>|!=|LIKE)\s*\?/i);
    if (whereMatch && bindings.length > 0) {
      const column = whereMatch[1];
      const operator = whereMatch[2].toUpperCase();
      const value = bindings[0];

      const filteredRows = rows.filter((row) => {
        const rowValue = row[column];

        switch (operator) {
          case '=':
            return rowValue === value;
          case '>':
            return (rowValue as number) > (value as number);
          case '<':
            return (rowValue as number) < (value as number);
          case '>=':
            return (rowValue as number) >= (value as number);
          case '<=':
            return (rowValue as number) <= (value as number);
          case '<>':
          case '!=':
            return rowValue !== value;
          case 'LIKE':
            if (typeof rowValue === 'string' && typeof value === 'string') {
              const pattern = value.replace(/%/g, '.*').replace(/_/g, '.');
              return new RegExp(`^${pattern}$`, 'i').test(rowValue);
            }
            return false;
          default:
            return true;
        }
      });

      return {
        rows: filteredRows,
        columns: Object.keys(filteredRows[0] ?? {}).map((name) => ({ name, type: 'unknown' })),
      };
    }

    return {
      rows,
      columns: Object.keys(rows[0] ?? {}).map((name) => ({ name, type: 'unknown' })),
    };
  }

  function executeInsert(
    sql: string,
    bindings: SqlValue[],
    currentTables: Map<string, TableMap>
  ): DoSQLQueryResult {
    // Extract table name
    const match = sql.match(/INSERT\s+INTO\s+[`"]?(\w+)[`"]?/i);
    if (!match) {
      throw new Error(`Invalid INSERT: ${sql}`);
    }

    const tableName = match[1];
    let table = currentTables.get(tableName);

    if (!table) {
      table = new Map<string, TableRow>();
      currentTables.set(tableName, table);
    }

    // Extract column names
    const colMatch = sql.match(/\(([^)]+)\)\s+VALUES/i);
    const columns = colMatch
      ? colMatch[1].split(',').map((c) => c.trim().replace(/[`"]/g, ''))
      : [];

    // Create row
    const row: TableRow = {};
    columns.forEach((col, i) => {
      row[col] = bindings[i];
    });

    // Auto-generate ID if not provided
    if (row.id === undefined) {
      lastInsertId++;
      row.id = lastInsertId;
    }

    table.set(String(row.id), row);

    return {
      rows: [row as Row],
      rowsAffected: 1,
      lastInsertRowId: row.id as number,
    };
  }

  function executeUpdate(
    sql: string,
    bindings: SqlValue[],
    currentTables: Map<string, TableMap>
  ): DoSQLQueryResult {
    const match = sql.match(/UPDATE\s+[`"]?(\w+)[`"]?/i);
    if (!match) {
      throw new Error(`Invalid UPDATE: ${sql}`);
    }

    const tableName = match[1];
    const table = currentTables.get(tableName);

    if (!table) {
      return { rows: [] as Row[], rowsAffected: 0 };
    }

    // Extract SET columns (basic)
    const setMatch = sql.match(/SET\s+(.*?)(?:\s+WHERE|$)/i);
    if (!setMatch) {
      return { rows: [] as Row[], rowsAffected: 0 };
    }

    const setParts = setMatch[1].split(',').map((s) => s.trim());
    const setColumns = setParts.map((p) => p.split('=')[0].trim().replace(/[`"]/g, ''));

    // Extract WHERE column (basic)
    const whereMatch = sql.match(/WHERE\s+[`"]?(\w+)[`"]?\s*=\s*\?/i);
    const whereColumn = whereMatch?.[1];
    const whereValueIndex = setColumns.length;
    const whereValue = bindings[whereValueIndex];

    let affected = 0;

    for (const [key, row] of Array.from(table.entries())) {
      if (!whereColumn || row[whereColumn] === whereValue) {
        setColumns.forEach((col, i) => {
          row[col] = bindings[i];
        });
        affected++;
      }
    }

    return { rows: [] as Row[], rowsAffected: affected };
  }

  function executeDelete(
    sql: string,
    bindings: SqlValue[],
    currentTables: Map<string, TableMap>
  ): DoSQLQueryResult {
    const match = sql.match(/DELETE\s+FROM\s+[`"]?(\w+)[`"]?/i);
    if (!match) {
      throw new Error(`Invalid DELETE: ${sql}`);
    }

    const tableName = match[1];
    const table = currentTables.get(tableName);

    if (!table) {
      return { rows: [] as Row[], rowsAffected: 0 };
    }

    // Extract WHERE column (basic)
    const whereMatch = sql.match(/WHERE\s+[`"]?(\w+)[`"]?\s*=\s*\?/i);
    const whereColumn = whereMatch?.[1];
    const whereValue = bindings[0];

    let affected = 0;
    const toDelete: string[] = [];

    for (const [key, row] of Array.from(table.entries())) {
      if (!whereColumn || row[whereColumn] === whereValue) {
        toDelete.push(key);
        affected++;
      }
    }

    for (const key of toDelete) {
      table.delete(key);
    }

    return { rows: [] as Row[], rowsAffected: affected };
  }

  function executeCreateTable(
    sql: string,
    currentTables: Map<string, TableMap>
  ): DoSQLQueryResult {
    const match = sql.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"]?(\w+)[`"]?/i);
    if (!match) {
      throw new Error(`Invalid CREATE TABLE: ${sql}`);
    }

    const tableName = match[1];

    if (!currentTables.has(tableName)) {
      currentTables.set(tableName, new Map<string, TableRow>());
    }

    return { rows: [] as Row[], rowsAffected: 0 };
  }

  function executeDropTable(
    sql: string,
    currentTables: Map<string, TableMap>
  ): DoSQLQueryResult {
    const match = sql.match(/DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?[`"]?(\w+)[`"]?/i);
    if (!match) {
      throw new Error(`Invalid DROP TABLE: ${sql}`);
    }

    const tableName = match[1];
    currentTables.delete(tableName);

    return { rows: [] as Row[], rowsAffected: 0 };
  }
}

// =============================================================================
// EXPORTS
// =============================================================================

export { DoSQLKnexClient, DoSQLConnection } from './client.js';
export type {
  DoSQLBackend,
  DoSQLKnexConfig,
  DoSQLQueryResult,
  KnexQueryObject,
  KnexResponse,
  DoSQLKnexTransaction,
} from './types.js';
