/**
 * Database Context for ESM Stored Procedures
 *
 * Provides a secure database access interface for procedures running
 * in sandboxed V8 isolates.
 */

import type {
  DatabaseSchema,
  TableSchema,
  DatabaseContext,
  DatabaseAccessor,
  TableAccessor,
  TableSchemaToRecord,
  SqlFunction,
  TransactionFunction,
  TransactionContext,
  Predicate,
  QueryOptions,
} from './types.js';

// =============================================================================
// TABLE ACCESSOR IMPLEMENTATION
// =============================================================================

/**
 * Storage adapter interface for actual data operations
 */
export interface StorageAdapter<T extends Record<string, unknown>> {
  get(key: string | number): Promise<T | undefined>;
  query(filter: Partial<T>, options?: QueryOptions): Promise<T[]>;
  queryWithPredicate(predicate: (record: T) => boolean, options?: QueryOptions): Promise<T[]>;
  count(filter?: Partial<T>): Promise<number>;
  countWithPredicate(predicate?: (record: T) => boolean): Promise<number>;
  insert(record: Omit<T, 'id'>): Promise<T>;
  update(filter: Partial<T>, changes: Partial<T>): Promise<number>;
  updateWithPredicate(predicate: (record: T) => boolean, changes: Partial<T>): Promise<number>;
  delete(filter: Partial<T>): Promise<number>;
  deleteWithPredicate(predicate: (record: T) => boolean): Promise<number>;
}

/**
 * Create a table accessor with the given storage adapter
 */
export function createTableAccessor<T extends Record<string, unknown>>(
  adapter: StorageAdapter<T>
): TableAccessor<T> {
  return {
    async get(key: string | number): Promise<T | undefined> {
      return adapter.get(key);
    },

    async where(predicateOrFilter: Predicate<T> | Partial<T>, options?: QueryOptions): Promise<T[]> {
      if (typeof predicateOrFilter === 'function') {
        return adapter.queryWithPredicate(predicateOrFilter, options);
      }
      return adapter.query(predicateOrFilter, options);
    },

    async all(options?: QueryOptions): Promise<T[]> {
      return adapter.query({}, options);
    },

    async count(predicateOrFilter?: Predicate<T> | Partial<T>): Promise<number> {
      if (!predicateOrFilter) {
        return adapter.count();
      }
      if (typeof predicateOrFilter === 'function') {
        return adapter.countWithPredicate(predicateOrFilter);
      }
      return adapter.count(predicateOrFilter);
    },

    async insert(record: Omit<T, 'id'>): Promise<T> {
      return adapter.insert(record);
    },

    async update(predicateOrFilter: Predicate<T> | Partial<T>, changes: Partial<T>): Promise<number> {
      if (typeof predicateOrFilter === 'function') {
        return adapter.updateWithPredicate(predicateOrFilter, changes);
      }
      return adapter.update(predicateOrFilter, changes);
    },

    async delete(predicateOrFilter: Predicate<T> | Partial<T>): Promise<number> {
      if (typeof predicateOrFilter === 'function') {
        return adapter.deleteWithPredicate(predicateOrFilter);
      }
      return adapter.delete(predicateOrFilter);
    },
  };
}

// =============================================================================
// SQL EXECUTION
// =============================================================================

/**
 * SQL executor interface for raw query execution
 */
export interface SqlExecutor {
  execute<Result = unknown>(sql: string, params: unknown[]): Promise<Result[]>;
}

/**
 * Create a SQL template literal function
 */
export function createSqlFunction(executor: SqlExecutor): SqlFunction {
  return async function sql<Result = unknown>(
    strings: TemplateStringsArray,
    ...values: unknown[]
  ): Promise<Result[]> {
    // Build the SQL string with parameterized placeholders
    let query = strings[0];
    for (let i = 0; i < values.length; i++) {
      query += `$${i + 1}${strings[i + 1]}`;
    }

    return executor.execute<Result>(query, values);
  };
}

// =============================================================================
// TRANSACTION SUPPORT
// =============================================================================

/**
 * Transaction manager interface
 */
export interface TransactionManager {
  begin(): Promise<string>; // Returns transaction ID
  commit(txId: string): Promise<void>;
  rollback(txId: string): Promise<void>;
  execute<Result = unknown>(txId: string, sql: string, params: unknown[]): Promise<Result[]>;
}

/**
 * Create a transaction context
 */
export function createTransactionContext<DB extends DatabaseSchema>(
  txId: string,
  manager: TransactionManager,
  tableAdapters: Map<string, StorageAdapter<Record<string, unknown>>>
): TransactionContext<DB> {
  const tables: Record<string, TableAccessor<Record<string, unknown>>> = {};

  // Create transactional table accessors
  for (const [tableName, adapter] of tableAdapters) {
    // Wrap the adapter to use transaction-aware operations
    const txAdapter: StorageAdapter<Record<string, unknown>> = {
      ...adapter,
      async get(key) {
        const results = await manager.execute(txId, `SELECT * FROM ${tableName} WHERE id = $1`, [key]);
        return results[0] as Record<string, unknown> | undefined;
      },
      async query(filter, options) {
        const { sql, params } = buildSelectQuery(tableName, filter, options);
        return manager.execute(txId, sql, params);
      },
      async queryWithPredicate(predicate, options) {
        // For predicate queries, we need to fetch all and filter in memory
        const all = await manager.execute(txId, `SELECT * FROM ${tableName}`, []);
        let results = (all as Record<string, unknown>[]).filter(predicate);
        if (options?.orderBy) {
          results = sortResults(results, options.orderBy, options.orderDirection);
        }
        if (options?.offset) {
          results = results.slice(options.offset);
        }
        if (options?.limit) {
          results = results.slice(0, options.limit);
        }
        return results;
      },
      async count(filter) {
        const { sql, params } = buildCountQuery(tableName, filter);
        const result = await manager.execute<{ count: number }>(txId, sql, params);
        return result[0]?.count ?? 0;
      },
      async countWithPredicate(predicate) {
        const all = await manager.execute(txId, `SELECT * FROM ${tableName}`, []);
        return predicate
          ? (all as Record<string, unknown>[]).filter(predicate).length
          : all.length;
      },
      async insert(record) {
        const { sql, params, returning } = buildInsertQuery(tableName, record);
        const result = await manager.execute(txId, sql + returning, params);
        return result[0] as Record<string, unknown>;
      },
      async update(filter, changes) {
        const { sql, params } = buildUpdateQuery(tableName, filter, changes);
        const result = await manager.execute<{ rowCount: number }>(txId, sql, params);
        return result[0]?.rowCount ?? 0;
      },
      async updateWithPredicate(predicate, changes) {
        // Fetch matching records, then update by ID
        const matching = await this.queryWithPredicate(predicate);
        let count = 0;
        for (const record of matching) {
          if (record.id !== undefined) {
            await this.update({ id: record.id } as Partial<Record<string, unknown>>, changes);
            count++;
          }
        }
        return count;
      },
      async delete(filter) {
        const { sql, params } = buildDeleteQuery(tableName, filter);
        const result = await manager.execute<{ rowCount: number }>(txId, sql, params);
        return result[0]?.rowCount ?? 0;
      },
      async deleteWithPredicate(predicate) {
        const matching = await this.queryWithPredicate(predicate);
        let count = 0;
        for (const record of matching) {
          if (record.id !== undefined) {
            await this.delete({ id: record.id } as Partial<Record<string, unknown>>);
            count++;
          }
        }
        return count;
      },
    };

    tables[tableName] = createTableAccessor(txAdapter);
  }

  return {
    tables: tables as DatabaseAccessor<DB>,

    sql: async <Result = unknown>(
      strings: TemplateStringsArray,
      ...values: unknown[]
    ): Promise<Result[]> => {
      let query = strings[0];
      for (let i = 0; i < values.length; i++) {
        query += `$${i + 1}${strings[i + 1]}`;
      }
      return manager.execute<Result>(txId, query, values);
    },

    async commit(): Promise<void> {
      await manager.commit(txId);
    },

    async rollback(): Promise<void> {
      await manager.rollback(txId);
    },
  };
}

/**
 * Create a transaction function
 */
export function createTransactionFunction<DB extends DatabaseSchema>(
  manager: TransactionManager,
  tableAdapters: Map<string, StorageAdapter<Record<string, unknown>>>
): TransactionFunction<DB> {
  return async function transaction<T>(
    callback: (tx: TransactionContext<DB>) => Promise<T>
  ): Promise<T> {
    const txId = await manager.begin();

    try {
      const txContext = createTransactionContext<DB>(txId, manager, tableAdapters);
      const result = await callback(txContext);
      await manager.commit(txId);
      return result;
    } catch (error) {
      await manager.rollback(txId);
      throw error;
    }
  };
}

// =============================================================================
// DATABASE CONTEXT FACTORY
// =============================================================================

/**
 * Options for creating a database context
 */
export interface DatabaseContextOptions<DB extends DatabaseSchema> {
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
 * Create a database context for procedure execution
 */
export function createDatabaseContext<DB extends DatabaseSchema>(
  options: DatabaseContextOptions<DB>
): DatabaseContext<DB> {
  const { adapters, sqlExecutor, transactionManager } = options;

  // Create table accessors
  const tables: Record<string, TableAccessor<Record<string, unknown>>> = {};
  const tableAdapters = new Map<string, StorageAdapter<Record<string, unknown>>>();

  for (const tableName of Object.keys(adapters)) {
    const adapter = adapters[tableName as keyof typeof adapters] as StorageAdapter<Record<string, unknown>>;
    tables[tableName] = createTableAccessor(adapter);
    tableAdapters.set(tableName, adapter);
  }

  return {
    tables: tables as DatabaseAccessor<DB>,
    sql: createSqlFunction(sqlExecutor),
    transaction: createTransactionFunction<DB>(transactionManager, tableAdapters),
  };
}

// =============================================================================
// IN-MEMORY STORAGE ADAPTER
// =============================================================================

/**
 * In-memory storage adapter for testing
 */
export function createInMemoryAdapter<T extends Record<string, unknown>>(
  initialData: T[] = []
): StorageAdapter<T> {
  let idCounter = initialData.length > 0
    ? Math.max(...initialData.map(r => typeof r.id === 'number' ? r.id : 0)) + 1
    : 1;
  const data = new Map<string | number, T>();

  // Initialize with provided data
  for (const record of initialData) {
    if (record.id !== undefined) {
      data.set(record.id as string | number, record);
    }
  }

  function matchesFilter(record: T, filter: Partial<T>): boolean {
    for (const [key, value] of Object.entries(filter)) {
      if (record[key] !== value) {
        return false;
      }
    }
    return true;
  }

  return {
    async get(key: string | number): Promise<T | undefined> {
      return data.get(key);
    },

    async query(filter: Partial<T>, options?: QueryOptions): Promise<T[]> {
      let results = Array.from(data.values()).filter(r => matchesFilter(r, filter));
      results = applyQueryOptions(results, options);
      return results;
    },

    async queryWithPredicate(predicate: (record: T) => boolean, options?: QueryOptions): Promise<T[]> {
      let results = Array.from(data.values()).filter(predicate);
      results = applyQueryOptions(results, options);
      return results;
    },

    async count(filter?: Partial<T>): Promise<number> {
      if (!filter || Object.keys(filter).length === 0) {
        return data.size;
      }
      return Array.from(data.values()).filter(r => matchesFilter(r, filter)).length;
    },

    async countWithPredicate(predicate?: (record: T) => boolean): Promise<number> {
      if (!predicate) {
        return data.size;
      }
      return Array.from(data.values()).filter(predicate).length;
    },

    async insert(record: Omit<T, 'id'>): Promise<T> {
      const id = idCounter++;
      const newRecord = { ...record, id } as unknown as T;
      data.set(id, newRecord);
      return newRecord;
    },

    async update(filter: Partial<T>, changes: Partial<T>): Promise<number> {
      let count = 0;
      for (const [key, record] of data) {
        if (matchesFilter(record, filter)) {
          data.set(key, { ...record, ...changes });
          count++;
        }
      }
      return count;
    },

    async updateWithPredicate(predicate: (record: T) => boolean, changes: Partial<T>): Promise<number> {
      let count = 0;
      for (const [key, record] of data) {
        if (predicate(record)) {
          data.set(key, { ...record, ...changes });
          count++;
        }
      }
      return count;
    },

    async delete(filter: Partial<T>): Promise<number> {
      let count = 0;
      for (const [key, record] of data) {
        if (matchesFilter(record, filter)) {
          data.delete(key);
          count++;
        }
      }
      return count;
    },

    async deleteWithPredicate(predicate: (record: T) => boolean): Promise<number> {
      let count = 0;
      for (const [key, record] of data) {
        if (predicate(record)) {
          data.delete(key);
          count++;
        }
      }
      return count;
    },
  };
}

// =============================================================================
// IN-MEMORY SQL EXECUTOR
// =============================================================================

/**
 * Simple in-memory SQL executor for testing
 * Only supports basic SELECT, INSERT, UPDATE, DELETE
 */
export function createInMemorySqlExecutor(
  tables: Map<string, StorageAdapter<Record<string, unknown>>>
): SqlExecutor {
  return {
    async execute<Result = unknown>(sql: string, params: unknown[]): Promise<Result[]> {
      const upperSql = sql.trim().toUpperCase();

      // Very basic parsing - for testing purposes only
      if (upperSql.startsWith('SELECT')) {
        const match = sql.match(/FROM\s+(\w+)/i);
        if (match) {
          const tableName = match[1];
          const adapter = tables.get(tableName);
          if (adapter) {
            return adapter.query({}) as Promise<Result[]>;
          }
        }
      }

      // For unsupported queries, return empty
      return [] as Result[];
    },
  };
}

/**
 * Simple in-memory transaction manager for testing
 */
export function createInMemoryTransactionManager(): TransactionManager {
  let txCounter = 0;
  const transactions = new Map<string, boolean>(); // txId -> committed

  return {
    async begin(): Promise<string> {
      const txId = `tx_${++txCounter}`;
      transactions.set(txId, false);
      return txId;
    },

    async commit(txId: string): Promise<void> {
      transactions.set(txId, true);
    },

    async rollback(txId: string): Promise<void> {
      transactions.delete(txId);
    },

    async execute<Result = unknown>(_txId: string, sql: string, _params: unknown[]): Promise<Result[]> {
      // In a real implementation, this would execute within the transaction
      // For testing, we just return empty results
      console.log(`[Transaction] Executing: ${sql}`);
      return [] as Result[];
    },
  };
}

// =============================================================================
// QUERY BUILDERS
// =============================================================================

function buildSelectQuery(
  tableName: string,
  filter: Partial<Record<string, unknown>>,
  options?: QueryOptions
): { sql: string; params: unknown[] } {
  const conditions: string[] = [];
  const params: unknown[] = [];
  let paramIndex = 1;

  for (const [key, value] of Object.entries(filter)) {
    conditions.push(`${key} = $${paramIndex++}`);
    params.push(value);
  }

  let sql = `SELECT * FROM ${tableName}`;
  if (conditions.length > 0) {
    sql += ` WHERE ${conditions.join(' AND ')}`;
  }
  if (options?.orderBy) {
    sql += ` ORDER BY ${options.orderBy} ${options.orderDirection?.toUpperCase() ?? 'ASC'}`;
  }
  if (options?.limit) {
    sql += ` LIMIT ${options.limit}`;
  }
  if (options?.offset) {
    sql += ` OFFSET ${options.offset}`;
  }

  return { sql, params };
}

function buildCountQuery(
  tableName: string,
  filter?: Partial<Record<string, unknown>>
): { sql: string; params: unknown[] } {
  const conditions: string[] = [];
  const params: unknown[] = [];
  let paramIndex = 1;

  if (filter) {
    for (const [key, value] of Object.entries(filter)) {
      conditions.push(`${key} = $${paramIndex++}`);
      params.push(value);
    }
  }

  let sql = `SELECT COUNT(*) as count FROM ${tableName}`;
  if (conditions.length > 0) {
    sql += ` WHERE ${conditions.join(' AND ')}`;
  }

  return { sql, params };
}

function buildInsertQuery(
  tableName: string,
  record: Record<string, unknown>
): { sql: string; params: unknown[]; returning: string } {
  const columns = Object.keys(record);
  const params = Object.values(record);
  const placeholders = params.map((_, i) => `$${i + 1}`);

  const sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders.join(', ')})`;
  const returning = ' RETURNING *';

  return { sql, params, returning };
}

function buildUpdateQuery(
  tableName: string,
  filter: Partial<Record<string, unknown>>,
  changes: Partial<Record<string, unknown>>
): { sql: string; params: unknown[] } {
  const setClause: string[] = [];
  const conditions: string[] = [];
  const params: unknown[] = [];
  let paramIndex = 1;

  for (const [key, value] of Object.entries(changes)) {
    setClause.push(`${key} = $${paramIndex++}`);
    params.push(value);
  }

  for (const [key, value] of Object.entries(filter)) {
    conditions.push(`${key} = $${paramIndex++}`);
    params.push(value);
  }

  let sql = `UPDATE ${tableName} SET ${setClause.join(', ')}`;
  if (conditions.length > 0) {
    sql += ` WHERE ${conditions.join(' AND ')}`;
  }

  return { sql, params };
}

function buildDeleteQuery(
  tableName: string,
  filter: Partial<Record<string, unknown>>
): { sql: string; params: unknown[] } {
  const conditions: string[] = [];
  const params: unknown[] = [];
  let paramIndex = 1;

  for (const [key, value] of Object.entries(filter)) {
    conditions.push(`${key} = $${paramIndex++}`);
    params.push(value);
  }

  let sql = `DELETE FROM ${tableName}`;
  if (conditions.length > 0) {
    sql += ` WHERE ${conditions.join(' AND ')}`;
  }

  return { sql, params };
}

function sortResults<T extends Record<string, unknown>>(
  results: T[],
  orderBy: string,
  direction: 'asc' | 'desc' = 'asc'
): T[] {
  return [...results].sort((a, b) => {
    const aVal = a[orderBy];
    const bVal = b[orderBy];

    if (aVal === bVal) return 0;
    if (aVal === undefined || aVal === null) return direction === 'asc' ? -1 : 1;
    if (bVal === undefined || bVal === null) return direction === 'asc' ? 1 : -1;

    const comparison = aVal < bVal ? -1 : 1;
    return direction === 'asc' ? comparison : -comparison;
  });
}

function applyQueryOptions<T extends Record<string, unknown>>(
  results: T[],
  options?: QueryOptions
): T[] {
  if (!options) return results;

  let processed = results;

  if (options.orderBy) {
    processed = sortResults(processed, options.orderBy, options.orderDirection);
  }

  if (options.offset) {
    processed = processed.slice(options.offset);
  }

  if (options.limit) {
    processed = processed.slice(0, options.limit);
  }

  return processed;
}
