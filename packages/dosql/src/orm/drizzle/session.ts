/**
 * DoSQL Drizzle ORM Integration - Session
 *
 * Implements the SQLite session and prepared query interfaces for DoSQL.
 * This bridges Drizzle's query building with DoSQL's execution engine.
 */

import { entityKind } from 'drizzle-orm';
import { NoopLogger } from 'drizzle-orm/logger';
import type { Logger } from 'drizzle-orm/logger';
import type { RelationalSchemaConfig, TablesRelationalConfig } from 'drizzle-orm/relations';
import { fillPlaceholders, type Query } from 'drizzle-orm/sql/sql';
import { SQLiteTransaction } from 'drizzle-orm/sqlite-core';
import type { SQLiteAsyncDialect } from 'drizzle-orm/sqlite-core';
import type { SelectedFieldsOrdered } from 'drizzle-orm/sqlite-core/query-builders/select.types';
import {
  SQLitePreparedQuery,
  SQLiteSession,
  type PreparedQueryConfig as PreparedQueryConfigBase,
  type SQLiteExecuteMethod,
  type SQLiteTransactionConfig,
} from 'drizzle-orm/sqlite-core/session';

import type { DoSQLBackend, DoSQLRunResult } from './types.js';

/**
 * Session options for DoSQL.
 */
export interface DoSQLSessionOptions {
  logger?: Logger;
}

/**
 * Prepared query config without run statement.
 */
type PreparedQueryConfig = Omit<PreparedQueryConfigBase, 'statement' | 'run'>;

/**
 * Get the name from a field (column, SQL, or subquery).
 */
function getFieldName(field: unknown): string {
  if (field && typeof field === 'object') {
    if ('name' in field && typeof (field as any).name === 'string') {
      return (field as any).name;
    }
    if ('fieldAlias' in field && typeof (field as any).fieldAlias === 'string') {
      return (field as any).fieldAlias;
    }
  }
  return 'unknown';
}

/**
 * Map a row from array format to object format.
 * This is a simplified version of drizzle-orm's internal mapResultRow.
 */
function mapRowToObject(
  fields: SelectedFieldsOrdered,
  values: unknown[],
  _joinsNotNullableMap?: Record<string, boolean>,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  for (let i = 0; i < fields.length; i++) {
    const { path, field } = fields[i];
    const value = values[i];

    // Handle nested paths for joins
    if (path && path.length > 0) {
      let current: Record<string, unknown> = result;
      for (let j = 0; j < path.length - 1; j++) {
        const pathPart = path[j];
        if (!(pathPart in current)) {
          current[pathPart] = {};
        }
        current = current[pathPart] as Record<string, unknown>;
      }
      current[path[path.length - 1]] = value;
    } else {
      // Get field name from the column/SQL
      const fieldName = getFieldName(field);
      result[fieldName] = value;
    }
  }

  return result;
}

/**
 * DoSQL Session - Manages database connections and query execution.
 *
 * Extends SQLiteSession to provide async query execution through DoSQL's backend.
 */
export class DoSQLSession<
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> extends SQLiteSession<'async', DoSQLRunResult, TFullSchema, TSchema> {
  static readonly [entityKind]: string = 'DoSQLSession';

  private logger: Logger;
  /** @internal */
  readonly _dialect: SQLiteAsyncDialect;
  /** @internal */
  readonly _schema: RelationalSchemaConfig<TSchema> | undefined;

  constructor(
    private client: DoSQLBackend,
    dialect: SQLiteAsyncDialect,
    schema: RelationalSchemaConfig<TSchema> | undefined,
    options: DoSQLSessionOptions = {},
  ) {
    super(dialect);
    this._dialect = dialect;
    this._schema = schema;
    this.logger = options.logger ?? new NoopLogger();
  }

  /**
   * Prepare a query for execution.
   */
  prepareQuery<T extends Omit<PreparedQueryConfig, 'run'>>(
    query: Query,
    fields: SelectedFieldsOrdered | undefined,
    executeMethod: SQLiteExecuteMethod,
    isResponseInArrayMode: boolean,
    customResultMapper?: (rows: unknown[][], mapColumnValue?: (value: unknown) => unknown) => unknown,
  ): DoSQLPreparedQuery<T> {
    return new DoSQLPreparedQuery(
      this.client,
      query,
      this.logger,
      fields,
      executeMethod,
      isResponseInArrayMode,
      customResultMapper,
    );
  }

  /**
   * Execute a transaction.
   */
  async transaction<T>(
    transaction: (tx: DoSQLTransaction<TFullSchema, TSchema>) => Promise<T>,
    _config?: SQLiteTransactionConfig,
  ): Promise<T> {
    const tx = new DoSQLTransaction('async', this._dialect, this, this._schema);
    return this.client.transaction(async () => {
      return transaction(tx);
    });
  }
}

/**
 * DoSQL Transaction - Represents a database transaction.
 */
export class DoSQLTransaction<
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> extends SQLiteTransaction<'async', DoSQLRunResult, TFullSchema, TSchema> {
  static readonly [entityKind]: string = 'DoSQLTransaction';

  /** @internal */
  readonly _dialectRef: SQLiteAsyncDialect;
  /** @internal */
  readonly _sessionRef: DoSQLSession<TFullSchema, TSchema>;
  /** @internal */
  readonly _schemaRef: RelationalSchemaConfig<TSchema> | undefined;

  constructor(
    resultType: 'async',
    dialect: SQLiteAsyncDialect,
    session: DoSQLSession<TFullSchema, TSchema>,
    schema: RelationalSchemaConfig<TSchema> | undefined,
    nestedIndex?: number,
  ) {
    super(resultType, dialect, session, schema as any, nestedIndex);
    this._dialectRef = dialect;
    this._sessionRef = session;
    this._schemaRef = schema;
  }

  /**
   * Create a nested transaction (savepoint).
   */
  async transaction<T>(
    transaction: (tx: DoSQLTransaction<TFullSchema, TSchema>) => Promise<T>,
  ): Promise<T> {
    const tx = new DoSQLTransaction<TFullSchema, TSchema>(
      'async',
      this._dialectRef,
      this._sessionRef,
      this._schemaRef,
      this.nestedIndex + 1,
    );
    return this._sessionRef.transaction(() => transaction(tx));
  }
}

/**
 * DoSQL Prepared Query - Executes prepared SQL queries through DoSQL backend.
 */
export class DoSQLPreparedQuery<
  T extends PreparedQueryConfig = PreparedQueryConfig,
> extends SQLitePreparedQuery<{
  type: 'async';
  run: DoSQLRunResult;
  all: T['all'];
  get: T['get'];
  values: T['values'];
  execute: T['execute'];
}> {
  static readonly [entityKind]: string = 'DoSQLPreparedQuery';

  /** @internal */
  joinsNotNullableMap?: Record<string, boolean>;

  constructor(
    private client: DoSQLBackend,
    query: Query,
    private logger: Logger,
    private fields: SelectedFieldsOrdered | undefined,
    executeMethod: SQLiteExecuteMethod,
    private _isResponseInArrayMode: boolean,
    private customResultMapper?: (
      rows: unknown[][],
      mapColumnValue?: (value: unknown) => unknown,
    ) => unknown,
  ) {
    super('async', executeMethod, query, undefined, undefined, undefined);
  }

  /**
   * Execute a statement without returning rows (INSERT, UPDATE, DELETE).
   */
  async run(placeholderValues?: Record<string, unknown>): Promise<DoSQLRunResult> {
    const params = fillPlaceholders(this.query.params, placeholderValues ?? {});
    this.logger.logQuery(this.query.sql, params);
    return this.client.run(this.query.sql, params as any[]);
  }

  /**
   * Map the run result to Drizzle's expected format.
   */
  override mapRunResult(result: DoSQLRunResult, _isFromBatch?: boolean): DoSQLRunResult {
    return result;
  }

  /**
   * Execute a query and return all rows.
   */
  async all(placeholderValues?: Record<string, unknown>): Promise<T['all']> {
    const { fields, joinsNotNullableMap, query, logger, client, customResultMapper } = this;

    if (!fields && !customResultMapper) {
      const params = fillPlaceholders(query.params, placeholderValues ?? {});
      logger.logQuery(query.sql, params);
      return client.all(query.sql, params as any[]) as Promise<T['all']>;
    }

    const rows = await this.values(placeholderValues);

    if (customResultMapper) {
      return customResultMapper(rows as unknown[][]) as T['all'];
    }

    return (rows as unknown[][]).map((row) =>
      mapRowToObject(fields!, row, joinsNotNullableMap),
    ) as T['all'];
  }

  /**
   * Execute a query and return the first row.
   */
  async get(placeholderValues?: Record<string, unknown>): Promise<T['get']> {
    const { fields, joinsNotNullableMap, query, logger, client, customResultMapper } = this;

    if (!fields && !customResultMapper) {
      const params = fillPlaceholders(query.params, placeholderValues ?? {});
      logger.logQuery(query.sql, params);
      return client.get(query.sql, params as any[]) as Promise<T['get']>;
    }

    const rows = await this.values(placeholderValues) as unknown[][];
    const row = rows[0];

    if (!row) {
      return undefined as T['get'];
    }

    if (customResultMapper) {
      return customResultMapper(rows) as T['get'];
    }

    return mapRowToObject(fields!, row, joinsNotNullableMap) as T['get'];
  }

  /**
   * Execute a query and return raw values (arrays).
   */
  async values(placeholderValues?: Record<string, unknown>): Promise<T['values']> {
    const params = fillPlaceholders(this.query.params, placeholderValues ?? {});
    this.logger.logQuery(this.query.sql, params);
    return this.client.values(this.query.sql, params as any[]) as Promise<T['values']>;
  }

  /** @internal */
  isResponseInArrayMode(): boolean {
    return this._isResponseInArrayMode;
  }
}
