/**
 * sql.do - Shared types for DoSQL client and server
 *
 * This module re-exports types from @dotdo/sql-types for backward compatibility
 * and adds any client-specific type definitions.
 *
 * @packageDocumentation
 */

// =============================================================================
// Re-export all shared types
// =============================================================================

/**
 * Re-exported types from `@dotdo/sql-types` for unified type definitions.
 *
 * These types are the canonical definitions for the DoSQL ecosystem and are
 * re-exported here for convenience. Importing from `sql.do` ensures
 * type compatibility across the entire stack.
 *
 * ## Branded Types
 *
 * Branded types provide type-safe identifiers that cannot be accidentally
 * assigned from raw primitives:
 *
 * - {@link TransactionId} - Unique identifier for database transactions
 * - {@link LSN} - Log Sequence Number for WAL positioning and time travel
 * - {@link StatementHash} - Hash for prepared statement caching
 * - {@link ShardId} - Identifier for database shards
 *
 * ## Query Types
 *
 * Core types for executing SQL queries:
 *
 * - {@link QueryRequest} - Request structure for SQL queries
 * - {@link QueryOptions} - Client-facing query options
 * - {@link QueryResponse} - Server response with results
 * - {@link QueryResult} - Client-facing result format
 *
 * ## CDC Types
 *
 * Change Data Capture types for real-time data streaming:
 *
 * - {@link CDCOperation} - Operation types (INSERT, UPDATE, DELETE, TRUNCATE)
 * - {@link CDCEvent} - Change event with before/after row data
 * - {@link CDCOperationCode} - Numeric codes for binary encoding
 *
 * ## Transaction Types
 *
 * Types for managing database transactions:
 *
 * - {@link IsolationLevel} - Transaction isolation levels
 * - {@link TransactionOptions} - Options for beginning transactions
 * - {@link TransactionState} - Current state of an active transaction
 * - {@link TransactionHandle} - Handle returned after beginning a transaction
 *
 * ## RPC Types
 *
 * Types for the CapnWeb RPC protocol:
 *
 * - {@link RPCMethod} - Available RPC methods
 * - {@link RPCRequest} - Request envelope
 * - {@link RPCResponse} - Response envelope
 * - {@link RPCError} - Error structure
 * - {@link RPCErrorCode} - Standard error codes
 *
 * ## Schema Types
 *
 * Types for database schema definitions:
 *
 * - {@link ColumnDefinition} - Column structure and constraints
 * - {@link IndexDefinition} - Index configuration
 * - {@link ForeignKeyDefinition} - Foreign key relationships
 * - {@link TableSchema} - Complete table definition
 *
 * ## Type Guards and Converters
 *
 * Utility functions for working with these types:
 *
 * - `isServerCDCEvent()` / `isClientCDCEvent()` - CDC event type guards
 * - `serverToClientCDCEvent()` / `clientToServerCDCEvent()` - Format converters
 * - `responseToResult()` / `resultToResponse()` - Query result converters
 * - `isValidLSN()` / `isValidTransactionId()` etc. - Validation guards
 *
 * @see {@link https://github.com/dotdo/shared-types | @dotdo/sql-types} for canonical definitions
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
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
} from '@dotdo/sql-types';

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
} from '@dotdo/sql-types';

// =============================================================================
// Event Types
// =============================================================================

/**
 * Connection event data for 'connected' event.
 *
 * Emitted when a WebSocket connection to the database is successfully established.
 *
 * @public
 * @stability stable
 * @since 0.2.0
 */
export interface ConnectedEvent {
  /** The URL that was connected to */
  url: string;
  /** Timestamp when the connection was established */
  timestamp: Date;
}

/**
 * Disconnection event data for 'disconnected' event.
 *
 * Emitted when a WebSocket connection to the database is closed.
 *
 * @public
 * @stability stable
 * @since 0.2.0
 */
export interface DisconnectedEvent {
  /** The URL that was disconnected from */
  url: string;
  /** Timestamp when the disconnection occurred */
  timestamp: Date;
  /** Optional reason for the disconnection */
  reason?: string;
}

/**
 * Error event data for 'error' event.
 *
 * Emitted when an error occurs during WebSocket communication or message parsing.
 * Named `ClientErrorEvent` to avoid conflict with the DOM's `ErrorEvent` interface.
 *
 * @public
 * @stability stable
 * @since 0.3.0
 */
export interface ClientErrorEvent {
  /** The error that occurred */
  error: Error;
  /** Timestamp when the error occurred */
  timestamp: Date;
  /** Context about where the error occurred */
  context: 'message_parse' | 'connection' | 'rpc';
  /** Optional request ID if the error was associated with a specific request */
  requestId?: string;
}

/**
 * @deprecated Use `ClientErrorEvent` instead. This alias exists for backward compatibility.
 * @public
 */
export type ErrorEvent = ClientErrorEvent;

/**
 * Event types for SQLClient.
 *
 * Maps event names to their corresponding event data types. This interface
 * enables type-safe event handling with the {@link SQLClient.on} and
 * {@link SQLClient.off} methods.
 *
 * @example
 * ```typescript
 * // Type-safe event handling
 * const client = createSQLClient({ url: 'wss://sql.example.com' });
 *
 * client.on('connected', (event) => {
 *   // event is typed as ConnectedEvent
 *   console.log(`Connected to ${event.url}`);
 * });
 *
 * client.on('disconnected', (event) => {
 *   // event is typed as DisconnectedEvent
 *   console.log(`Disconnected: ${event.reason}`);
 * });
 *
 * client.on('error', (event) => {
 *   // event is typed as ErrorEvent
 *   console.error(`[${event.context}] ${event.error.message}`);
 * });
 * ```
 *
 * @public
 * @stability stable
 * @since 0.2.0
 */
export interface ClientEventMap {
  connected: ConnectedEvent;
  disconnected: DisconnectedEvent;
  error: ClientErrorEvent;
}

/**
 * Event listener callback type for SQLClient events.
 *
 * A generic type that maps event names to their corresponding callback signatures.
 * The callback receives the appropriate event data type based on the event name.
 *
 * @typeParam K - The event name key from {@link ClientEventMap}
 *
 * @public
 * @stability stable
 * @since 0.2.0
 */
export type ClientEventListener<K extends keyof ClientEventMap> = (event: ClientEventMap[K]) => void;

// =============================================================================
// Idempotency Cache Types
// =============================================================================

/**
 * Statistics for the idempotency cache.
 *
 * Provides metrics about cache usage and effectiveness for monitoring and tuning.
 *
 * @public
 * @stability stable
 * @since 0.3.0
 */
export interface IdempotencyCacheStats {
  /** Current number of entries in the cache */
  size: number;
  /** Number of cache hits (key reused for retry) */
  hits: number;
  /** Number of cache misses (new key generated) */
  misses: number;
  /** Number of entries evicted due to LRU or TTL */
  evictions: number;
  /** Maximum configured cache size */
  maxSize: number;
  /** TTL in milliseconds for cache entries */
  ttlMs: number;
}

/**
 * Transaction context for executing operations within a transaction.
 *
 * Provides a scoped interface for executing SQL statements within an active transaction.
 * All operations performed through this context are automatically associated with the
 * parent transaction. This interface is passed to the callback function in
 * {@link SQLClient.transaction}.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface TransactionContext {
  /**
   * The transaction ID for this context.
   *
   * Can be used for advanced scenarios where you need to pass the transaction ID
   * to other code or for debugging purposes.
   */
  readonly transactionId: TransactionId;

  /**
   * Executes a SQL statement within this transaction.
   *
   * @param sql - The SQL statement to execute
   * @param params - Optional array of parameter values for prepared statement placeholders
   * @returns Promise resolving to the query result with affected row count
   * @throws {SQLError} When statement execution fails
   */
  exec(sql: string, params?: SQLValue[]): Promise<QueryResult>;

  /**
   * Executes a SQL SELECT query within this transaction.
   *
   * @typeParam T - The expected shape of result rows (defaults to Record<string, SQLValue>)
   * @param sql - The SQL SELECT query to execute
   * @param params - Optional array of parameter values for prepared statement placeholders
   * @returns Promise resolving to query result containing typed rows
   * @throws {SQLError} When query execution fails
   */
  query<T = Record<string, SQLValue>>(sql: string, params?: SQLValue[]): Promise<QueryResult<T>>;
}

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
 * // Transaction with auto commit/rollback
 * await client.transaction(async (tx) => {
 *   await tx.exec('INSERT INTO users (name) VALUES (?)', ['Alice']);
 *   await tx.exec('UPDATE accounts SET balance = balance - 100 WHERE id = ?', [1]);
 * });
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
   * For simpler transaction management, consider using {@link transaction} instead.
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
   * Executes a function within a transaction with automatic commit/rollback.
   *
   * This is the recommended way to work with transactions. The transaction is
   * automatically committed if the function completes successfully, or rolled
   * back if an error is thrown.
   *
   * @typeParam T - The return type of the transaction function
   * @param fn - Async function receiving a {@link TransactionContext} for executing queries
   * @param options - Optional transaction configuration (isolation level, read-only mode)
   * @returns Promise resolving to the return value of the transaction function
   * @throws {SQLError} When transaction operations fail or commit/rollback fails
   * @throws Re-throws any error thrown by the transaction function after rollback
   *
   * @example
   * ```typescript
   * // Transfer money between accounts
   * const result = await client.transaction(async (tx) => {
   *   await tx.exec('UPDATE accounts SET balance = balance - ? WHERE id = ?', [amount, fromId]);
   *   await tx.exec('UPDATE accounts SET balance = balance + ? WHERE id = ?', [amount, toId]);
   *   return { transferred: amount };
   * });
   *
   * // Read-only transaction for consistent reads
   * const report = await client.transaction(async (tx) => {
   *   const orders = await tx.query<Order>('SELECT * FROM orders WHERE date > ?', [startDate]);
   *   const total = await tx.query<{sum: number}>('SELECT SUM(amount) as sum FROM orders');
   *   return { orders: orders.rows, total: total.rows[0].sum };
   * }, { readOnly: true, isolationLevel: 'SNAPSHOT' });
   * ```
   */
  transaction<T>(
    fn: (tx: TransactionContext) => Promise<T>,
    options?: TransactionOptions
  ): Promise<T>;

  /**
   * Executes multiple SQL statements in a single batch request.
   *
   * Batch operations are more efficient than individual requests when you need
   * to execute many statements. All statements are sent to the server in one
   * network round-trip.
   *
   * Note: Batch operations are not transactional by default. For atomic operations,
   * combine with {@link transaction} or wrap statements in explicit transaction commands.
   *
   * @param statements - Array of SQL statements with optional parameters
   * @returns Promise resolving to an array of query results in the same order as inputs
   * @throws {SQLError} When any statement fails (partial results may not be returned)
   *
   * @example
   * ```typescript
   * const results = await client.batch([
   *   { sql: 'INSERT INTO logs (msg) VALUES (?)', params: ['event1'] },
   *   { sql: 'INSERT INTO logs (msg) VALUES (?)', params: ['event2'] },
   *   { sql: 'INSERT INTO logs (msg) VALUES (?)', params: ['event3'] },
   * ]);
   *
   * console.log(`Inserted ${results.length} log entries`);
   * ```
   */
  batch(statements: Array<{ sql: string; params?: SQLValue[] }>): Promise<QueryResult[]>;

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
   * Explicitly establishes a WebSocket connection to the database.
   *
   * This method is optional - the client will automatically connect on first query
   * if not already connected. However, calling connect() explicitly allows you to:
   * - Pre-establish the connection before queries are needed
   * - Handle connection errors separately from query errors
   * - Wait for the connection to be ready
   *
   * If already connected, this method returns immediately without reconnecting.
   * Multiple concurrent calls to connect() will share the same connection attempt.
   *
   * @returns Promise that resolves when the connection is established
   * @throws {ConnectionError} When connection fails (network error, auth failure, etc.)
   *
   * @example
   * ```typescript
   * const client = createSQLClient({ url: 'https://sql.example.com' });
   *
   * // Pre-connect before queries
   * try {
   *   await client.connect();
   *   console.log('Connected successfully');
   * } catch (error) {
   *   console.error('Failed to connect:', error);
   * }
   *
   * // Now queries won't have connection latency
   * const result = await client.query('SELECT 1');
   * ```
   */
  connect(): Promise<void>;

  /**
   * Checks if the client is currently connected to the database.
   *
   * Returns true if a WebSocket connection is established and in the OPEN state.
   * Note that this is a point-in-time check; the connection could change state
   * immediately after this method returns.
   *
   * @returns `true` if connected, `false` otherwise
   *
   * @example
   * ```typescript
   * const client = createSQLClient({ url: 'https://sql.example.com' });
   *
   * console.log(client.isConnected()); // false (not connected yet)
   *
   * await client.connect();
   * console.log(client.isConnected()); // true
   *
   * await client.close();
   * console.log(client.isConnected()); // false
   * ```
   */
  isConnected(): boolean;

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

  // ===========================================================================
  // Event Handling
  // ===========================================================================

  /**
   * Registers an event listener for client events.
   *
   * Supported events:
   * - `'connected'`: Emitted when WebSocket connection is established
   * - `'disconnected'`: Emitted when WebSocket connection is closed
   * - `'error'`: Emitted when an error occurs during WebSocket communication
   *
   * @typeParam K - The event name key from {@link ClientEventMap}
   * @param event - The event name to listen for
   * @param listener - The callback function to invoke when the event occurs
   * @returns The client instance (for method chaining)
   *
   * @example
   * ```typescript
   * const client = createSQLClient({ url: 'wss://sql.example.com' });
   *
   * client.on('connected', (event) => {
   *   console.log(`Connected to ${event.url} at ${event.timestamp}`);
   * });
   *
   * client.on('disconnected', (event) => {
   *   console.log(`Disconnected from ${event.url}: ${event.reason}`);
   * });
   *
   * client.on('error', (event) => {
   *   console.error(`Error in ${event.context}:`, event.error);
   * });
   *
   * await client.connect();
   * ```
   *
   * @public
   * @stability stable
   * @since 0.2.0
   */
  on<K extends keyof ClientEventMap>(event: K, listener: ClientEventListener<K>): this;

  /**
   * Removes a previously registered event listener.
   *
   * @typeParam K - The event name key from {@link ClientEventMap}
   * @param event - The event name to stop listening for
   * @param listener - The callback function to remove
   * @returns The client instance (for method chaining)
   *
   * @example
   * ```typescript
   * const onConnected = (event: ConnectedEvent): void => {
   *   console.log(`Connected to ${event.url}`);
   * };
   *
   * // Register listener
   * client.on('connected', onConnected);
   *
   * // Later, remove the listener
   * client.off('connected', onConnected);
   * ```
   *
   * @public
   * @stability stable
   * @since 0.2.0
   */
  off<K extends keyof ClientEventMap>(event: K, listener: ClientEventListener<K>): this;

  // ===========================================================================
  // Idempotency Cache Management
  // ===========================================================================

  /**
   * Gets the current number of entries in the idempotency key cache.
   *
   * The idempotency cache stores keys for mutation queries to ensure the same
   * key is used for retries of the same request.
   *
   * @returns The number of entries currently in the cache
   *
   * @example
   * ```typescript
   * const client = createSQLClient({ url: 'https://sql.example.com' });
   *
   * // After some mutations...
   * console.log(`Cache has ${client.getCacheSize()} entries`);
   * ```
   *
   * @public
   * @stability stable
   * @since 0.3.0
   */
  getCacheSize(): number;

  /**
   * Clears all entries from the idempotency key cache.
   *
   * This forces new idempotency keys to be generated for all future requests.
   *
   * **Warning:** Use with caution as this may affect retry semantics for
   * in-flight requests.
   *
   * @example
   * ```typescript
   * // Clear all cached idempotency keys
   * client.clearIdempotencyCache();
   * console.log(`Cache cleared, now has ${client.getCacheSize()} entries`); // 0
   * ```
   *
   * @public
   * @stability stable
   * @since 0.3.0
   */
  clearIdempotencyCache(): void;

  /**
   * Gets statistics about the idempotency key cache.
   *
   * Useful for monitoring cache effectiveness and tuning configuration.
   *
   * @returns Cache statistics including size, hits, misses, and evictions
   *
   * @example
   * ```typescript
   * const stats = client.getIdempotencyCacheStats();
   *
   * // Calculate hit rate
   * const hitRate = stats.hits / (stats.hits + stats.misses) * 100;
   * console.log(`Cache hit rate: ${hitRate.toFixed(1)}%`);
   *
   * // Monitor cache health
   * console.log(`Size: ${stats.size}/${stats.maxSize}`);
   * console.log(`Evictions: ${stats.evictions}`);
   * ```
   *
   * @public
   * @stability stable
   * @since 0.3.0
   */
  getIdempotencyCacheStats(): IdempotencyCacheStats;

  /**
   * Triggers manual cleanup of expired entries from the idempotency cache.
   *
   * This is normally done automatically on a timer (configurable via
   * `cleanupIntervalMs`), but can be called manually to force immediate cleanup.
   *
   * @returns The number of expired entries that were removed
   *
   * @example
   * ```typescript
   * // Force cleanup before memory-sensitive operation
   * const removed = client.cleanupIdempotencyCache();
   * console.log(`Removed ${removed} expired entries`);
   *
   * // Combine with stats for monitoring
   * const statsBefore = client.getIdempotencyCacheStats();
   * const removedCount = client.cleanupIdempotencyCache();
   * const statsAfter = client.getIdempotencyCacheStats();
   * console.log(`Cleanup removed ${removedCount} entries (${statsBefore.size} -> ${statsAfter.size})`);
   * ```
   *
   * @public
   * @stability stable
   * @since 0.3.0
   */
  cleanupIdempotencyCache(): number;
}

// =============================================================================
// Connection Pool Types (sql.do specific)
// =============================================================================

/**
 * Backpressure strategy when pool is exhausted.
 * @public
 */
export type BackpressureStrategy = 'reject' | 'queue';

/**
 * Configuration for the WebSocket connection pool.
 * @public
 */
export interface PoolConfig {
  /** Maximum number of connections in the pool (default: 10) */
  maxSize?: number;
  /** Minimum number of idle connections to maintain (default: 0) */
  minIdle?: number;
  /** Time in ms before an idle connection is closed (default: 30000) */
  idleTimeout?: number;
  /** Maximum time in ms a connection can live (default: 3600000 = 1 hour) */
  maxLifetime?: number;
  /** Time in ms to wait for a connection when pool is exhausted (default: 30000) */
  waitTimeout?: number;
  /** Interval in ms between health checks (default: 30000) */
  healthCheckInterval?: number;
  /** Whether to validate connections before borrowing (default: true) */
  validateOnBorrow?: boolean;
  /** Strategy when pool is exhausted: 'reject' throws immediately, 'queue' waits (default: 'queue') */
  backpressureStrategy?: BackpressureStrategy;
  /** Maximum number of waiting requests before applying backpressure (default: 100) */
  maxWaitingRequests?: number;
  /** Interval in ms for sending keepalive pings (default: 30000) */
  keepAliveInterval?: number;
  /** Tags to apply to connections for identification (default: []) */
  connectionTags?: string[];
}

/**
 * Default pool configuration values.
 * @public
 */
export const DEFAULT_POOL_CONFIG: Required<Omit<PoolConfig, 'connectionTags'>> & { connectionTags: string[] } = {
  maxSize: 10,
  minIdle: 0,
  idleTimeout: 30000,
  maxLifetime: 3600000,
  waitTimeout: 30000,
  healthCheckInterval: 30000,
  validateOnBorrow: true,
  backpressureStrategy: 'queue',
  maxWaitingRequests: 100,
  keepAliveInterval: 30000,
  connectionTags: [],
};

/**
 * Statistics about the connection pool.
 * @public
 */
export interface PoolStats {
  /** Total number of connections in the pool */
  totalConnections: number;
  /** Number of connections currently in use */
  activeConnections: number;
  /** Number of idle connections available */
  idleConnections: number;
  /** Maximum pool size */
  maxSize: number;
  /** Number of requests waiting for a connection */
  waitingRequests: number;
  /** Total connections created since pool start */
  connectionsCreated: number;
  /** Total connections closed since pool start */
  connectionsClosed: number;
  /** Total requests served by the pool */
  totalRequestsServed: number;
  /** Ratio of reused connections (0-1) */
  connectionReuseRatio: number;
}

/**
 * Health status of the connection pool.
 * @public
 */
export interface PoolHealth {
  /** Whether the pool is healthy */
  healthy: boolean;
  /** Number of healthy connections */
  healthyConnections: number;
  /** Number of unhealthy connections */
  unhealthyConnections: number;
  /** Timestamp of last health check */
  lastHealthCheck: Date | null;
  /** Average latency across connections */
  averageLatency: number;
}

/**
 * Information about a single connection in the pool.
 * @public
 */
export interface ConnectionInfo {
  /** Unique connection identifier */
  id: string;
  /** When the connection was created */
  createdAt: Date;
  /** When the connection was last used */
  lastUsedAt: Date;
  /** Time in ms since last use (0 if active) */
  idleTime: number;
  /** Whether connection is currently in use */
  active: boolean;
  /** Tags associated with this connection */
  tags: string[];
  /** Last measured latency in ms */
  latency: number | null;
}

/**
 * Event data for pool:connection-created event.
 * @public
 */
export interface PoolConnectionCreatedEvent {
  connectionId: string;
  timestamp: Date;
}

/**
 * Event data for pool:connection-reused event.
 * @public
 */
export interface PoolConnectionReusedEvent {
  connectionId: string;
  timestamp: Date;
}

/**
 * Event data for pool:connection-closed event.
 * @public
 */
export interface PoolConnectionClosedEvent {
  connectionId: string;
  reason: string;
  timestamp: Date;
}

/**
 * Event data for pool:backpressure event.
 * @public
 */
export interface PoolBackpressureEvent {
  waitingRequests: number;
  activeConnections: number;
}

/**
 * Event data for do:hibernating event.
 * @public
 */
export interface DOHibernatingEvent {
  timestamp: Date;
}

/**
 * Event data for do:awake event.
 * @public
 */
export interface DOAwakeEvent {
  timestamp: Date;
}

/**
 * Event map for pool events.
 * @public
 */
export interface PoolEventMap {
  'pool:connection-created': PoolConnectionCreatedEvent;
  'pool:connection-reused': PoolConnectionReusedEvent;
  'pool:connection-closed': PoolConnectionClosedEvent;
  'pool:health-check': PoolHealth;
  'pool:backpressure': PoolBackpressureEvent;
  'do:hibernating': DOHibernatingEvent;
  'do:awake': DOAwakeEvent;
}
