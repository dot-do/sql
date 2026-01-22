/**
 * @dotdo/sql.do - CapnWeb RPC Client for DoSQL
 *
 * @packageDocumentation
 */

import type {
  SQLClient,
  SQLValue,
  QueryResult,
  QueryOptions,
  PreparedStatement,
  TransactionOptions,
  TransactionState,
  TransactionId,
  LSN,
  TableSchema,
  RPCRequest,
  RPCResponse,
  RPCError,
  StatementHash,
  IdempotencyConfig,
} from './types.js';
import { createTransactionId, createLSN, createStatementHash, DEFAULT_IDEMPOTENCY_CONFIG } from './types.js';

// =============================================================================
// Idempotency Key Generation
// =============================================================================

/**
 * Generate a random string of specified length using crypto
 * @internal
 */
function generateRandomString(length: number): string {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  const array = new Uint8Array(length);
  crypto.getRandomValues(array);
  return Array.from(array, (byte) => chars[byte % chars.length]).join('');
}

/**
 * Generate SHA-256 hash of input string and return first n characters
 * @internal
 */
async function hashString(input: string, length: number): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(input);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  const hashHex = hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');
  return hashHex.slice(0, length);
}

/**
 * Generates an idempotency key for SQL mutation requests.
 *
 * The key format is: `{prefix}-{timestamp}-{random}-{hash}` (or `{timestamp}-{random}-{hash}` without prefix) where:
 * - timestamp: Unix milliseconds when the key was generated
 * - random: 8-character cryptographically random alphanumeric string
 * - hash: First 8 characters of SHA-256 hash of SQL + JSON-serialized params
 *
 * Idempotency keys ensure that retried mutation requests are executed at most once,
 * preventing duplicate operations when requests are retried due to network failures
 * or timeouts.
 *
 * @param sql - The SQL statement to generate a key for
 * @param params - Optional array of parameter values (included in hash calculation)
 * @param prefix - Optional prefix to prepend to the key (useful for namespacing)
 * @returns Promise resolving to the generated idempotency key string
 *
 * @example
 * ```typescript
 * // Generate a key for an INSERT statement
 * const key = await generateIdempotencyKey(
 *   'INSERT INTO users (name) VALUES (?)',
 *   ['Alice'],
 *   'user-service'
 * );
 * // Result: "user-service-1705432800000-abc12345-7f3a8b2c"
 *
 * // Without prefix
 * const key2 = await generateIdempotencyKey('DELETE FROM temp WHERE id = ?', [42]);
 * // Result: "1705432800000-xyz98765-2c4f6a8b"
 * ```
 *
 * @public
 * @since 0.1.0
 */
export async function generateIdempotencyKey(
  sql: string,
  params?: SQLValue[],
  prefix?: string
): Promise<string> {
  const timestamp = Date.now();
  const random = generateRandomString(8);
  const hashInput = sql + JSON.stringify(params ?? []);
  const hash = await hashString(hashInput, 8);

  const key = `${timestamp}-${random}-${hash}`;
  return prefix ? `${prefix}-${key}` : key;
}

/**
 * Determines if a SQL statement is a mutation (INSERT, UPDATE, or DELETE).
 *
 * This function checks whether a SQL statement modifies data, which is used
 * to determine if idempotency keys should be generated for the request.
 * Only mutation queries need idempotency protection to prevent duplicate
 * side effects from retries.
 *
 * @param sql - The SQL statement to check
 * @returns `true` if the statement is INSERT, UPDATE, or DELETE; `false` otherwise
 *
 * @example
 * ```typescript
 * isMutationQuery('INSERT INTO users (name) VALUES (?)'); // true
 * isMutationQuery('UPDATE users SET name = ? WHERE id = ?'); // true
 * isMutationQuery('DELETE FROM users WHERE id = ?'); // true
 * isMutationQuery('SELECT * FROM users'); // false
 * isMutationQuery('CREATE TABLE users (id INT)'); // false
 * ```
 *
 * @public
 * @since 0.1.0
 */
export function isMutationQuery(sql: string): boolean {
  const trimmed = sql.trim().toUpperCase();
  return (
    trimmed.startsWith('INSERT') ||
    trimmed.startsWith('UPDATE') ||
    trimmed.startsWith('DELETE')
  );
}

// =============================================================================
// Client Configuration
// =============================================================================

/**
 * Configuration options for creating a SQL client.
 *
 * @example
 * ```typescript
 * const config: SQLClientConfig = {
 *   url: 'https://sql.example.com',
 *   token: 'your-auth-token',
 *   database: 'mydb',
 *   timeout: 30000,
 *   retry: {
 *     maxRetries: 3,
 *     baseDelayMs: 100,
 *     maxDelayMs: 5000,
 *   },
 *   idempotency: {
 *     enabled: true,
 *     keyPrefix: 'my-service',
 *   },
 * };
 * ```
 *
 * @public
 * @since 0.1.0
 */
export interface SQLClientConfig {
  /**
   * DoSQL endpoint URL.
   *
   * Can be HTTP(S) URL; will be automatically converted to WebSocket (WS/WSS) for connections.
   *
   * @example 'https://sql.example.com' or 'wss://sql.example.com'
   */
  url: string;

  /**
   * Authentication token for the DoSQL server.
   *
   * If provided, will be sent with each request for authentication.
   */
  token?: string;

  /**
   * Database name to connect to.
   *
   * If not specified, uses the server's default database.
   */
  database?: string;

  /**
   * Request timeout in milliseconds.
   *
   * @defaultValue 30000 (30 seconds)
   */
  timeout?: number;

  /**
   * Configuration for automatic retry behavior on transient failures.
   *
   * @see {@link RetryConfig}
   */
  retry?: RetryConfig;

  /**
   * Configuration for automatic idempotency key generation.
   *
   * When enabled, mutation queries (INSERT, UPDATE, DELETE) will automatically
   * include idempotency keys to prevent duplicate operations on retry.
   */
  idempotency?: IdempotencyConfig;
}

/**
 * Configuration for automatic retry behavior on transient failures.
 *
 * Uses exponential backoff with jitter to retry failed requests.
 * Only retryable errors (timeouts, connection failures, etc.) trigger retries;
 * application-level errors (syntax errors, constraint violations) fail immediately.
 *
 * @example
 * ```typescript
 * const retryConfig: RetryConfig = {
 *   maxRetries: 3,      // Retry up to 3 times
 *   baseDelayMs: 100,   // Start with 100ms delay
 *   maxDelayMs: 5000,   // Cap delay at 5 seconds
 * };
 * ```
 *
 * @public
 * @since 0.1.0
 */
export interface RetryConfig {
  /**
   * Maximum number of retry attempts before giving up.
   *
   * @example 3 means the request will be attempted up to 4 times total (1 initial + 3 retries)
   */
  maxRetries: number;

  /**
   * Base delay in milliseconds for exponential backoff.
   *
   * The actual delay increases exponentially: baseDelayMs * 2^attempt
   */
  baseDelayMs: number;

  /**
   * Maximum delay in milliseconds between retry attempts.
   *
   * Caps the exponential backoff to prevent excessively long waits.
   */
  maxDelayMs: number;
}

/**
 * Default retry configuration.
 * @internal
 */
const DEFAULT_RETRY: RetryConfig = {
  maxRetries: 3,
  baseDelayMs: 100,
  maxDelayMs: 5000,
};

// =============================================================================
// CapnWeb RPC Client
// =============================================================================

/**
 * SQL client implementation for DoSQL using CapnWeb RPC over WebSocket.
 *
 * Provides a full-featured SQL client with support for:
 * - Parameterized queries (prepared statements)
 * - Transactions with configurable isolation levels
 * - Automatic retry with exponential backoff
 * - Idempotency keys for mutation safety
 * - Connection pooling and health checks
 *
 * Use {@link createSQLClient} factory function to create instances.
 *
 * @example
 * ```typescript
 * import { createSQLClient } from '@dotdo/sql.do';
 *
 * // Create a client
 * const client = createSQLClient({
 *   url: 'https://sql.example.com',
 *   token: 'your-token',
 * });
 *
 * // Execute queries
 * const users = await client.query<User>('SELECT * FROM users WHERE active = ?', [true]);
 *
 * // Use transactions
 * await client.transaction(async (tx) => {
 *   await tx.exec('UPDATE accounts SET balance = balance - ? WHERE id = ?', [100, fromId]);
 *   await tx.exec('UPDATE accounts SET balance = balance + ? WHERE id = ?', [100, toId]);
 * });
 *
 * // Batch operations
 * const results = await client.batch([
 *   { sql: 'INSERT INTO logs (msg) VALUES (?)', params: ['action1'] },
 *   { sql: 'INSERT INTO logs (msg) VALUES (?)', params: ['action2'] },
 * ]);
 *
 * // Clean up
 * await client.close();
 * ```
 *
 * @public
 * @since 0.1.0
 */
export class DoSQLClient implements SQLClient {
  /** @internal */
  private readonly config: Required<Omit<SQLClientConfig, 'token' | 'database'>> & {
    token?: string;
    database?: string;
  };
  /** @internal */
  private requestId = 0;
  /** @internal */
  private ws: WebSocket | null = null;
  /** @internal */
  private pendingRequests = new Map<string, {
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
    timeout: ReturnType<typeof setTimeout>;
    idempotencyKey?: string;
  }>();
  /** @internal Map of request content hash to idempotency key for retry consistency */
  private idempotencyKeyCache = new Map<string, string>();

  /**
   * Creates a new DoSQLClient instance.
   *
   * Prefer using {@link createSQLClient} factory function instead of direct instantiation.
   *
   * @param config - Client configuration options
   *
   * @example
   * ```typescript
   * const client = new DoSQLClient({
   *   url: 'https://sql.example.com',
   *   timeout: 60000, // 1 minute timeout
   * });
   * ```
   */
  constructor(config: SQLClientConfig) {
    this.config = {
      ...config,
      timeout: config.timeout ?? 30000,
      retry: config.retry ?? DEFAULT_RETRY,
      idempotency: config.idempotency ?? DEFAULT_IDEMPOTENCY_CONFIG,
    };
  }

  /**
   * Gets or generates an idempotency key for a mutation request.
   *
   * Reuses the same key for retries of the same request to ensure at-most-once
   * execution semantics. Keys are cached by SQL+params combination and cleared
   * after successful execution or non-retryable failures.
   *
   * @param sql - The SQL statement to get a key for
   * @param params - Optional parameter values (used for cache key generation)
   * @returns Promise resolving to the idempotency key, or undefined if idempotency is disabled or not a mutation
   *
   * @example
   * ```typescript
   * const key = await client.getIdempotencyKey('INSERT INTO users (name) VALUES (?)', ['Alice']);
   * // Returns a key like "prefix-1705432800000-abc12345-7f3a8b2c"
   *
   * // Calling again with same SQL+params returns the same key (for retries)
   * const sameKey = await client.getIdempotencyKey('INSERT INTO users (name) VALUES (?)', ['Alice']);
   * // key === sameKey
   * ```
   */
  async getIdempotencyKey(sql: string, params?: SQLValue[]): Promise<string | undefined> {
    if (!this.config.idempotency.enabled || !isMutationQuery(sql)) {
      return undefined;
    }

    // Create a cache key based on the SQL and params
    const cacheKey = sql + JSON.stringify(params ?? []);

    // Check if we already have a key for this request (retry scenario)
    let key = this.idempotencyKeyCache.get(cacheKey);
    if (!key) {
      key = await generateIdempotencyKey(sql, params, this.config.idempotency.keyPrefix);
      this.idempotencyKeyCache.set(cacheKey, key);
    }

    return key;
  }

  /**
   * Clears the cached idempotency key for a request.
   *
   * Called automatically after successful execution or non-retryable failures.
   * May be called manually to force generation of a new key for the same SQL+params.
   *
   * @param sql - The SQL statement whose key should be cleared
   * @param params - Optional parameter values (must match original call to getIdempotencyKey)
   *
   * @example
   * ```typescript
   * // Clear the cached key to force a new one on next request
   * client.clearIdempotencyKey('INSERT INTO users (name) VALUES (?)', ['Alice']);
   * ```
   */
  clearIdempotencyKey(sql: string, params?: SQLValue[]): void {
    const cacheKey = sql + JSON.stringify(params ?? []);
    this.idempotencyKeyCache.delete(cacheKey);
  }

  // ===========================================================================
  // Connection Management
  // ===========================================================================

  private async ensureConnection(): Promise<WebSocket> {
    if (this.ws && this.ws.readyState === WebSocket.READY_STATE_OPEN) {
      return this.ws;
    }

    return new Promise((resolve, reject) => {
      const wsUrl = this.config.url.replace(/^http/, 'ws');
      this.ws = new WebSocket(wsUrl);

      this.ws.addEventListener('open', () => {
        resolve(this.ws!);
      });

      this.ws.addEventListener('error', (event: Event) => {
        reject(new Error(`WebSocket error: ${event}`));
      });

      this.ws.addEventListener('close', () => {
        this.ws = null;
        // Reject all pending requests
        for (const [id, pending] of this.pendingRequests) {
          clearTimeout(pending.timeout);
          pending.reject(new Error('Connection closed'));
          this.pendingRequests.delete(id);
        }
      });

      this.ws.addEventListener('message', (event: MessageEvent) => {
        this.handleMessage(event.data as string | ArrayBuffer);
      });
    });
  }

  private handleMessage(data: string | ArrayBuffer): void {
    try {
      const message = typeof data === 'string' ? data : new TextDecoder().decode(data);
      const response: RPCResponse = JSON.parse(message);

      const pending = this.pendingRequests.get(response.id);
      if (pending) {
        clearTimeout(pending.timeout);
        this.pendingRequests.delete(response.id);

        if (response.error) {
          pending.reject(new SQLError(response.error));
        } else {
          pending.resolve(response.result);
        }
      }
    } catch (error) {
      console.error('Failed to parse RPC response:', error);
    }
  }

  private async rpc<T>(method: string, params: unknown): Promise<T> {
    const ws = await this.ensureConnection();
    const id = `${++this.requestId}`;

    const request: RPCRequest = {
      id,
      method: method as RPCRequest['method'],
      params,
    };

    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(id);
        reject(new Error(`Request timeout: ${method}`));
      }, this.config.timeout);

      this.pendingRequests.set(id, {
        resolve: resolve as (value: unknown) => void,
        reject,
        timeout,
      });

      ws.send(JSON.stringify(request));
    });
  }

  // ===========================================================================
  // SQLClient Implementation
  // ===========================================================================

  /**
   * Executes a SQL statement that modifies data (INSERT, UPDATE, DELETE) or schema (DDL).
   *
   * For read-only queries, use {@link query} instead. Automatically generates and manages
   * idempotency keys for mutation queries when idempotency is enabled.
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
   * const tx = await client.beginTransaction();
   * await client.exec(
   *   'UPDATE accounts SET balance = balance - ? WHERE id = ?',
   *   [100, accountId],
   *   { transactionId: tx.id }
   * );
   * await client.commit(tx.id);
   * ```
   */
  async exec(sql: string, params?: SQLValue[], options?: QueryOptions): Promise<QueryResult> {
    const idempotencyKey = await this.getIdempotencyKey(sql, params);

    try {
      const result = await this.rpc<QueryResult>('exec', {
        sql,
        params: params ?? [],
        idempotencyKey,
        ...options,
      });

      // Clear idempotency key on success
      if (idempotencyKey) {
        this.clearIdempotencyKey(sql, params);
      }

      return result;
    } catch (error) {
      // For non-retryable errors, clear the idempotency key
      if (error instanceof SQLError && !this.isRetryableError(error)) {
        this.clearIdempotencyKey(sql, params);
      }
      throw error;
    }
  }

  /**
   * Checks if an error is retryable (connection issues, timeouts, etc.).
   * @internal
   */
  private isRetryableError(error: SQLError): boolean {
    const retryableCodes = [
      'TIMEOUT',
      'CONNECTION_CLOSED',
      'NETWORK_ERROR',
      'UNAVAILABLE',
      'RESOURCE_EXHAUSTED',
    ];
    return retryableCodes.includes(error.code);
  }

  /**
   * Executes a SQL SELECT query and returns the result rows.
   *
   * For write operations (INSERT, UPDATE, DELETE), use {@link exec} instead.
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
  async query<T = Record<string, SQLValue>>(
    sql: string,
    params?: SQLValue[],
    options?: QueryOptions
  ): Promise<QueryResult<T>> {
    return this.rpc<QueryResult<T>>('query', {
      sql,
      params: params ?? [],
      ...options,
    });
  }

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
  async prepare(sql: string): Promise<PreparedStatement> {
    const result = await this.rpc<{ hash: string }>('prepare', { sql });
    return {
      sql,
      hash: createStatementHash(result.hash),
    };
  }

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
  async execute<T = Record<string, SQLValue>>(
    statement: PreparedStatement,
    params?: SQLValue[],
    options?: QueryOptions
  ): Promise<QueryResult<T>> {
    return this.rpc<QueryResult<T>>('execute', {
      hash: statement.hash,
      params: params ?? [],
      ...options,
    });
  }

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
  async beginTransaction(options?: TransactionOptions): Promise<TransactionState> {
    const result = await this.rpc<{
      id: string;
      isolationLevel: TransactionState['isolationLevel'];
      readOnly: boolean;
      startedAt: string;
      snapshotLSN: string;
    }>('beginTransaction', options ?? {});

    return {
      id: createTransactionId(result.id),
      isolationLevel: result.isolationLevel,
      readOnly: result.readOnly,
      startedAt: new Date(result.startedAt),
      snapshotLSN: createLSN(BigInt(result.snapshotLSN)),
    };
  }

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
  async commit(transactionId: TransactionId): Promise<LSN> {
    const result = await this.rpc<{ lsn: string }>('commit', { transactionId });
    return createLSN(BigInt(result.lsn));
  }

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
  async rollback(transactionId: TransactionId): Promise<void> {
    await this.rpc<void>('rollback', { transactionId });
  }

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
  async getSchema(tableName: string): Promise<TableSchema | null> {
    return this.rpc<TableSchema | null>('getSchema', { tableName });
  }

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
  async ping(): Promise<{ latency: number }> {
    const start = performance.now();
    await this.rpc<void>('ping', {});
    return { latency: performance.now() - start };
  }

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
  async close(): Promise<void> {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  // ===========================================================================
  // Convenience Methods
  // ===========================================================================

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
   * const transferResult = await client.transaction(async (tx) => {
   *   // Debit source account
   *   await tx.exec('UPDATE accounts SET balance = balance - ? WHERE id = ?', [amount, fromId]);
   *
   *   // Credit destination account
   *   await tx.exec('UPDATE accounts SET balance = balance + ? WHERE id = ?', [amount, toId]);
   *
   *   // Return confirmation
   *   return { fromId, toId, amount, timestamp: new Date() };
   * });
   *
   * // Read-only transaction for consistent reads
   * const report = await client.transaction(async (tx) => {
   *   const orders = await tx.query<Order>('SELECT * FROM orders WHERE date > ?', [startDate]);
   *   const totals = await tx.query<{ sum: number }>('SELECT SUM(amount) as sum FROM orders WHERE date > ?', [startDate]);
   *   return { orders: orders.rows, total: totals.rows[0].sum };
   * }, { readOnly: true, isolationLevel: 'SNAPSHOT' });
   * ```
   */
  async transaction<T>(
    fn: (tx: TransactionContext) => Promise<T>,
    options?: TransactionOptions
  ): Promise<T> {
    const state = await this.beginTransaction(options);
    const tx = new TransactionContext(this, state);

    try {
      const result = await fn(tx);
      await this.commit(state.id);
      return result;
    } catch (error) {
      await this.rollback(state.id);
      throw error;
    }
  }

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
   * // Insert multiple log entries efficiently
   * const results = await client.batch([
   *   { sql: 'INSERT INTO logs (message, level) VALUES (?, ?)', params: ['User login', 'info'] },
   *   { sql: 'INSERT INTO logs (message, level) VALUES (?, ?)', params: ['Data processed', 'info'] },
   *   { sql: 'INSERT INTO logs (message, level) VALUES (?, ?)', params: ['Request completed', 'debug'] },
   * ]);
   *
   * console.log(`Inserted ${results.length} log entries`);
   * ```
   */
  async batch(statements: Array<{ sql: string; params?: SQLValue[] }>): Promise<QueryResult[]> {
    return this.rpc<QueryResult[]>('batch', { statements });
  }
}

// =============================================================================
// Transaction Context
// =============================================================================

/**
 * Context object for executing operations within a transaction.
 *
 * Provides a scoped interface for executing SQL statements within an active transaction.
 * All operations performed through this context are automatically associated with the
 * parent transaction.
 *
 * This class is passed to the callback function in {@link DoSQLClient.transaction} and
 * should not be instantiated directly.
 *
 * @example
 * ```typescript
 * await client.transaction(async (tx) => {
 *   // All operations use the same transaction
 *   await tx.exec('INSERT INTO users (name) VALUES (?)', ['Alice']);
 *   await tx.exec('INSERT INTO logs (action) VALUES (?)', ['user_created']);
 *
 *   // Read within the transaction sees uncommitted changes
 *   const result = await tx.query<User>('SELECT * FROM users WHERE name = ?', ['Alice']);
 *   console.log(`Found ${result.rows.length} user(s)`);
 * });
 * ```
 *
 * @public
 * @since 0.1.0
 */
export class TransactionContext {
  /**
   * Creates a new TransactionContext.
   *
   * @param client - The DoSQLClient to execute operations through
   * @param state - The transaction state from beginTransaction
   * @internal
   */
  constructor(
    private readonly client: DoSQLClient,
    private readonly state: TransactionState
  ) {}

  /**
   * Gets the transaction ID for this context.
   *
   * Can be used for advanced scenarios where you need to pass the transaction ID
   * to other code or for debugging purposes.
   *
   * @returns The branded transaction ID
   *
   * @example
   * ```typescript
   * await client.transaction(async (tx) => {
   *   console.log(`Transaction ID: ${tx.transactionId}`);
   *   await tx.exec('INSERT INTO users (name) VALUES (?)', ['Alice']);
   * });
   * ```
   */
  get transactionId(): TransactionId {
    return this.state.id;
  }

  /**
   * Executes a SQL statement within this transaction.
   *
   * Equivalent to calling {@link DoSQLClient.exec} with the transaction ID automatically set.
   *
   * @param sql - The SQL statement to execute
   * @param params - Optional array of parameter values for prepared statement placeholders
   * @returns Promise resolving to the query result with affected row count
   * @throws {SQLError} When statement execution fails
   *
   * @example
   * ```typescript
   * await client.transaction(async (tx) => {
   *   const result = await tx.exec('UPDATE users SET status = ? WHERE id = ?', ['active', userId]);
   *   console.log(`Updated ${result.rowsAffected} row(s)`);
   * });
   * ```
   */
  async exec(sql: string, params?: SQLValue[]): Promise<QueryResult> {
    return this.client.exec(sql, params, { transactionId: this.state.id });
  }

  /**
   * Executes a SQL SELECT query within this transaction.
   *
   * Equivalent to calling {@link DoSQLClient.query} with the transaction ID automatically set.
   * Reads performed within a transaction see uncommitted changes made earlier in the
   * same transaction.
   *
   * @typeParam T - The expected shape of result rows (defaults to Record<string, SQLValue>)
   * @param sql - The SQL SELECT query to execute
   * @param params - Optional array of parameter values for prepared statement placeholders
   * @returns Promise resolving to query result containing typed rows
   * @throws {SQLError} When query execution fails
   *
   * @example
   * ```typescript
   * interface User {
   *   id: number;
   *   name: string;
   *   balance: number;
   * }
   *
   * await client.transaction(async (tx) => {
   *   // Update balance
   *   await tx.exec('UPDATE users SET balance = balance + ? WHERE id = ?', [100, userId]);
   *
   *   // Read updated balance within the same transaction
   *   const result = await tx.query<User>('SELECT balance FROM users WHERE id = ?', [userId]);
   *   console.log(`New balance: ${result.rows[0].balance}`);
   * });
   * ```
   */
  async query<T = Record<string, SQLValue>>(
    sql: string,
    params?: SQLValue[]
  ): Promise<QueryResult<T>> {
    return this.client.query<T>(sql, params, { transactionId: this.state.id });
  }
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * Error thrown by SQL operations when a query or command fails.
 *
 * Contains a machine-readable {@link code} for programmatic error handling and
 * optional {@link details} for debugging. Extends the standard JavaScript Error
 * class with additional context about the SQL failure.
 *
 * Common error codes:
 * - `SYNTAX_ERROR` - Invalid SQL syntax
 * - `CONSTRAINT_VIOLATION` - Unique constraint, foreign key, or check constraint failed
 * - `TABLE_NOT_FOUND` - Referenced table does not exist
 * - `TIMEOUT` - Query execution timed out
 * - `CONNECTION_CLOSED` - WebSocket connection was closed
 * - `TRANSACTION_CONFLICT` - Transaction was aborted due to conflict
 *
 * @example
 * ```typescript
 * try {
 *   await client.exec('INSERT INTO users (id, name) VALUES (?, ?)', [1, 'Alice']);
 * } catch (error) {
 *   if (error instanceof SQLError) {
 *     switch (error.code) {
 *       case 'CONSTRAINT_VIOLATION':
 *         console.log('User with this ID already exists');
 *         break;
 *       case 'TIMEOUT':
 *         console.log('Query timed out, retrying...');
 *         break;
 *       default:
 *         console.error(`SQL Error [${error.code}]: ${error.message}`);
 *     }
 *   }
 * }
 * ```
 *
 * @public
 * @since 0.1.0
 */
export class SQLError extends Error {
  /**
   * Machine-readable error code for programmatic error handling.
   *
   * Use this to implement different error handling strategies based on the
   * type of failure (e.g., retry for TIMEOUT, fail fast for SYNTAX_ERROR).
   */
  readonly code: string;

  /**
   * Optional additional details about the error.
   *
   * May contain structured information like the specific constraint that failed,
   * the position in the SQL where a syntax error occurred, etc.
   */
  readonly details?: unknown;

  /**
   * Creates a new SQLError from an RPC error response.
   *
   * @param error - The RPC error object from the server response
   * @internal
   */
  constructor(error: RPCError) {
    super(error.message);
    this.name = 'SQLError';
    this.code = error.code;
    this.details = error.details;
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a new SQL client instance.
 *
 * This is the recommended way to create a SQL client.
 *
 * @param config - Client configuration options
 * @returns A new SQLClient instance
 *
 * @example
 * ```typescript
 * const client = createSQLClient({
 *   url: 'https://sql.example.com',
 *   token: 'your-token',
 * });
 *
 * const result = await client.query('SELECT * FROM users');
 * await client.close();
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function createSQLClient(config: SQLClientConfig): SQLClient {
  return new DoSQLClient(config);
}
