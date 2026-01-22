/**
 * @dotdo/sql.do - Shared types for DoSQL client and server
 *
 * This module re-exports types from @dotdo/shared-types for backward compatibility
 * and adds any client-specific type definitions.
 *
 * @packageDocumentation
 */

// =============================================================================
// Re-export all shared types
// =============================================================================

export {
  // Branded Types
  type TransactionId,
  type LSN,
  type StatementHash,
  type ShardId,

  // Brand constructors
  createTransactionId,
  createLSN,
  createStatementHash,
  createShardId,

  // LSN utilities
  compareLSN,
  incrementLSN,
  lsnValue,

  // Type guards for validation
  isValidLSN,
  isValidTransactionId,
  isValidShardId,
  isValidStatementHash,

  // Dev mode configuration
  setDevMode,
  isDevMode,
  setStrictMode,
  isStrictMode,

  // SQL Value Types
  type SQLValue,

  // Column Types
  type ColumnType,
  type SQLColumnType,
  type JSColumnType,
  SQL_TO_JS_TYPE_MAP,
  JS_TO_SQL_TYPE_MAP,
  sqlToJsType,
  jsToSqlType,

  // Query Types
  type QueryRequest,
  type QueryOptions,
  type QueryResponse,
  type QueryResult,

  // Idempotency Types
  type IdempotencyConfig,
  DEFAULT_IDEMPOTENCY_CONFIG,

  // CDC Types
  type CDCOperation,
  type ClientCDCOperation,
  type CDCEvent,
  CDCOperationCode,
  type CDCOperationCodeValue,

  // Transaction Types
  type IsolationLevel,
  type ServerIsolationLevel,
  type TransactionOptions,
  type TransactionState,
  type TransactionHandle,

  // RPC Types
  type RPCMethod,
  type RPCRequest,
  type RPCResponse,
  type RPCError,
  RPCErrorCode,

  // Client Capabilities
  type ClientCapabilities,
  DEFAULT_CLIENT_CAPABILITIES,

  // Schema Types
  type ColumnDefinition,
  type IndexDefinition,
  type ForeignKeyDefinition,
  type TableSchema,

  // Sharding Types
  type ShardConfig,
  type ShardInfo,

  // Connection Types
  type ConnectionOptions,
  type ConnectionStats,

  // Prepared Statement Types
  type PreparedStatement,

  // Type Guards
  isServerCDCEvent,
  isClientCDCEvent,
  isDateTimestamp,
  isNumericTimestamp,

  // Type Converters
  serverToClientCDCEvent,
  clientToServerCDCEvent,
  responseToResult,
  resultToResponse,

  // Retry Types
  type RetryConfig,
  DEFAULT_RETRY_CONFIG,
  isRetryConfig,
  createRetryConfig,
} from '@dotdo/shared-types';

// =============================================================================
// Client Interface (sql.do specific)
// =============================================================================

import type {
  TransactionId,
  LSN,
  SQLValue,
  QueryResult,
  QueryOptions,
  PreparedStatement,
  TransactionOptions,
  TransactionState,
  TableSchema,
} from '@dotdo/shared-types';

/**
 * SQL Client interface for sql.do.
 *
 * Provides a unified interface for executing SQL queries, managing transactions,
 * and working with prepared statements against a DoSQL database.
 *
 * @example
 * ```typescript
 * const client: SQLClient = createSQLClient({ url: 'https://sql.example.com' });
 *
 * // Simple query
 * const users = await client.query<User>('SELECT * FROM users');
 *
 * // Parameterized query
 * const user = await client.query<User>('SELECT * FROM users WHERE id = ?', [1]);
 *
 * // Transaction
 * const txState = await client.beginTransaction();
 * await client.exec('INSERT INTO users (name) VALUES (?)', ['Alice'], { transactionId: txState.id });
 * await client.commit(txState.id);
 *
 * await client.close();
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface SQLClient {
  /**
   * Executes a SQL statement that modifies data (INSERT, UPDATE, DELETE) or schema (DDL).
   *
   * Use this method for write operations. For read-only queries, use {@link query} instead.
   * Automatically generates idempotency keys for mutation queries when enabled.
   *
   * @param sql - The SQL statement to execute
   * @param params - Optional array of parameter values for prepared statement placeholders
   * @param options - Optional query execution options including transaction context
   * @returns Promise resolving to the query result with affected row count
   * @throws {SQLError} When statement execution fails (syntax error, constraint violation, etc.)
   *
   * @example
   * ```typescript
   * // Insert a new user
   * const result = await client.exec(
   *   'INSERT INTO users (name, email) VALUES (?, ?)',
   *   ['Alice', 'alice@example.com']
   * );
   * console.log(`Inserted ${result.rowsAffected} row(s)`);
   *
   * // Update within a transaction
   * await client.exec(
   *   'UPDATE accounts SET balance = balance - ? WHERE id = ?',
   *   [100, accountId],
   *   { transactionId: txState.id }
   * );
   * ```
   */
  exec(sql: string, params?: SQLValue[], options?: QueryOptions): Promise<QueryResult>;

  /**
   * Executes a SQL SELECT query and returns the result rows.
   *
   * Use this method for read operations. For write operations, use {@link exec} instead.
   * Supports generic typing to get typed result rows.
   *
   * @typeParam T - The expected shape of result rows (defaults to Record<string, SQLValue>)
   * @param sql - The SQL SELECT query to execute
   * @param params - Optional array of parameter values for prepared statement placeholders
   * @param options - Optional query execution options including transaction context
   * @returns Promise resolving to query result containing typed rows
   * @throws {SQLError} When query execution fails (syntax error, invalid table, etc.)
   *
   * @example
   * ```typescript
   * interface User {
   *   id: number;
   *   name: string;
   *   email: string;
   * }
   *
   * // Query with type inference
   * const result = await client.query<User>('SELECT * FROM users WHERE active = ?', [true]);
   * for (const user of result.rows) {
   *   console.log(user.name); // TypeScript knows this is a string
   * }
   *
   * // Query with aggregation
   * const countResult = await client.query<{ total: number }>('SELECT COUNT(*) as total FROM users');
   * console.log(`Total users: ${countResult.rows[0].total}`);
   * ```
   */
  query<T = Record<string, SQLValue>>(
    sql: string,
    params?: SQLValue[],
    options?: QueryOptions
  ): Promise<QueryResult<T>>;

  /**
   * Prepares a SQL statement for repeated execution with different parameters.
   *
   * Prepared statements offer performance benefits when executing the same query
   * multiple times with different parameter values. The server parses and plans
   * the query once, then reuses the plan for subsequent executions.
   *
   * @param sql - The SQL statement to prepare (use ? for parameter placeholders)
   * @returns Promise resolving to a prepared statement handle
   * @throws {SQLError} When statement preparation fails (syntax error, etc.)
   *
   * @example
   * ```typescript
   * // Prepare a statement for batch inserts
   * const stmt = await client.prepare('INSERT INTO logs (message, level) VALUES (?, ?)');
   *
   * // Execute multiple times with different parameters
   * await client.execute(stmt, ['User logged in', 'info']);
   * await client.execute(stmt, ['Invalid password', 'warn']);
   * await client.execute(stmt, ['System error', 'error']);
   * ```
   */
  prepare(sql: string): Promise<PreparedStatement>;

  /**
   * Executes a previously prepared statement with the given parameters.
   *
   * @typeParam T - The expected shape of result rows (defaults to Record<string, SQLValue>)
   * @param statement - The prepared statement handle from {@link prepare}
   * @param params - Optional array of parameter values matching the statement's placeholders
   * @param options - Optional query execution options including transaction context
   * @returns Promise resolving to the query result
   * @throws {SQLError} When execution fails (parameter mismatch, constraint violation, etc.)
   *
   * @example
   * ```typescript
   * const stmt = await client.prepare('SELECT * FROM users WHERE department = ?');
   *
   * const engineering = await client.execute<User>(stmt, ['engineering']);
   * const sales = await client.execute<User>(stmt, ['sales']);
   * ```
   */
  execute<T = Record<string, SQLValue>>(
    statement: PreparedStatement,
    params?: SQLValue[],
    options?: QueryOptions
  ): Promise<QueryResult<T>>;

  /**
   * Begins a new database transaction.
   *
   * Returns a transaction state containing the transaction ID, which must be
   * passed to subsequent operations and eventually to {@link commit} or {@link rollback}.
   *
   * @param options - Optional transaction configuration (isolation level, read-only mode)
   * @returns Promise resolving to the transaction state
   * @throws {SQLError} When transaction cannot be started
   *
   * @example
   * ```typescript
   * // Start a read-write transaction
   * const tx = await client.beginTransaction();
   *
   * try {
   *   await client.exec('UPDATE accounts SET balance = balance - 100 WHERE id = ?', [fromId], { transactionId: tx.id });
   *   await client.exec('UPDATE accounts SET balance = balance + 100 WHERE id = ?', [toId], { transactionId: tx.id });
   *   await client.commit(tx.id);
   * } catch (error) {
   *   await client.rollback(tx.id);
   *   throw error;
   * }
   *
   * // Start a read-only transaction with snapshot isolation
   * const readTx = await client.beginTransaction({
   *   isolationLevel: 'SNAPSHOT',
   *   readOnly: true
   * });
   * ```
   */
  beginTransaction(options?: TransactionOptions): Promise<TransactionState>;

  /**
   * Commits a transaction, making all changes permanent.
   *
   * After commit, the transaction ID is no longer valid. All changes made
   * within the transaction become visible to other connections.
   *
   * @param transactionId - The transaction ID from {@link beginTransaction}
   * @returns Promise resolving to the LSN (Log Sequence Number) of the commit
   * @throws {SQLError} When commit fails (transaction already ended, conflict, etc.)
   *
   * @example
   * ```typescript
   * const tx = await client.beginTransaction();
   * await client.exec('INSERT INTO orders (product_id, quantity) VALUES (?, ?)', [42, 5], { transactionId: tx.id });
   * const lsn = await client.commit(tx.id);
   * console.log(`Transaction committed at LSN: ${lsn}`);
   * ```
   */
  commit(transactionId: TransactionId): Promise<LSN>;

  /**
   * Rolls back a transaction, discarding all changes.
   *
   * After rollback, the transaction ID is no longer valid. All changes made
   * within the transaction are discarded.
   *
   * @param transactionId - The transaction ID from {@link beginTransaction}
   * @returns Promise resolving when rollback completes
   * @throws {SQLError} When rollback fails (transaction already ended, etc.)
   *
   * @example
   * ```typescript
   * const tx = await client.beginTransaction();
   * try {
   *   await client.exec('DELETE FROM users WHERE id = ?', [userId], { transactionId: tx.id });
   *   // Validation failed - rollback
   *   await client.rollback(tx.id);
   * } catch (error) {
   *   await client.rollback(tx.id);
   *   throw error;
   * }
   * ```
   */
  rollback(transactionId: TransactionId): Promise<void>;

  /**
   * Retrieves the schema definition for a table.
   *
   * Returns column definitions, indexes, and foreign key constraints for
   * the specified table, or null if the table does not exist.
   *
   * @param tableName - The name of the table to inspect
   * @returns Promise resolving to the table schema, or null if table not found
   * @throws {SQLError} When schema retrieval fails (permission denied, etc.)
   *
   * @example
   * ```typescript
   * const schema = await client.getSchema('users');
   * if (schema) {
   *   console.log(`Table ${schema.name} has ${schema.columns.length} columns:`);
   *   for (const col of schema.columns) {
   *     console.log(`  - ${col.name}: ${col.type}${col.nullable ? '' : ' NOT NULL'}`);
   *   }
   * }
   * ```
   */
  getSchema(tableName: string): Promise<TableSchema | null>;

  /**
   * Checks the connection health by sending a ping request.
   *
   * Useful for connection pool health checks and monitoring.
   *
   * @returns Promise resolving to an object containing the round-trip latency in milliseconds
   * @throws {SQLError} When ping fails (connection closed, timeout, etc.)
   *
   * @example
   * ```typescript
   * const { latency } = await client.ping();
   * console.log(`Database latency: ${latency.toFixed(2)}ms`);
   *
   * // Health check endpoint
   * if (latency > 1000) {
   *   console.warn('High database latency detected');
   * }
   * ```
   */
  ping(): Promise<{ latency: number }>;

  /**
   * Closes the database connection and releases resources.
   *
   * After calling close(), the client instance should not be used.
   * Any pending requests will be rejected with a connection closed error.
   *
   * @returns Promise resolving when the connection is closed
   *
   * @example
   * ```typescript
   * const client = createSQLClient({ url: 'https://sql.example.com' });
   * try {
   *   // Use client...
   * } finally {
   *   await client.close();
   * }
   * ```
   */
  close(): Promise<void>;
}
