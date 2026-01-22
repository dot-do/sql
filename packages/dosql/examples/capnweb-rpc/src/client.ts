/**
 * DoSQL CapnWeb RPC Client
 *
 * Example client showing how to connect to and use the DoSQL RPC server.
 * Demonstrates:
 * - WebSocket connection for streaming/pipelining
 * - HTTP batch mode for simple requests
 * - .map() chaining for result transformation
 * - Transaction support
 *
 * @example
 * ```ts
 * // Create WebSocket client
 * const client = createDoSQLClient('wss://example.com/db/mydb/rpc');
 *
 * // Execute SQL
 * const users = await client.query('SELECT * FROM users');
 *
 * // Use pipelining for efficient queries
 * const userName = await client.rpc.exec({ sql: 'SELECT * FROM users WHERE id = ?', params: [1] })
 *   .map(r => r.rows[0])
 *   .map(row => row[1]);
 *
 * // Execute transaction
 * const result = await client.transaction([
 *   { sql: 'INSERT INTO users (name) VALUES (?)', params: ['Alice'] },
 *   { sql: 'INSERT INTO logs (action) VALUES (?)', params: ['user_created'] },
 * ]);
 *
 * // Close when done
 * client.close();
 * ```
 */

import {
  newWebSocketRpcSession,
  newHttpBatchRpcSession,
  type RpcStub,
} from 'capnweb';

import type {
  DoSQLRpcApi,
  ExecRequest,
  ExecResult,
  PreparedStatementHandle,
  TransactionOp,
  TransactionResult,
  DatabaseStatus,
  TableInfo,
  TableSchema,
} from './types.js';

// =============================================================================
// Client Types
// =============================================================================

/**
 * DoSQL client options
 */
export interface DoSQLClientOptions {
  /** Connection URL (ws:// or wss:// for WebSocket, http:// or https:// for HTTP batch) */
  url: string;
  /** Connection timeout in milliseconds */
  connectTimeoutMs?: number;
  /** Query timeout in milliseconds */
  queryTimeoutMs?: number;
  /** Whether to automatically reconnect on disconnect (WebSocket only) */
  autoReconnect?: boolean;
  /** Maximum reconnection attempts */
  maxReconnectAttempts?: number;
}

/**
 * DoSQL client interface
 */
export interface DoSQLClient {
  /** The underlying CapnWeb RPC stub for direct .map() chaining */
  rpc: RpcStub<DoSQLRpcApi>;

  /** Execute a SQL query and return results */
  query(sql: string, params?: unknown[]): Promise<ExecResult>;

  /** Execute a SQL statement (INSERT, UPDATE, DELETE) */
  execute(sql: string, params?: unknown[]): Promise<{ changes: number; lastInsertRowId?: number }>;

  /** Execute multiple statements in a transaction */
  transaction(statements: Array<{ sql: string; params?: unknown[] }>): Promise<TransactionResult>;

  /** Prepare a statement */
  prepare(sql: string): Promise<PreparedStatement>;

  /** Get database status */
  status(): Promise<DatabaseStatus>;

  /** List all tables */
  listTables(): Promise<TableInfo[]>;

  /** Get table schema */
  describeTable(table: string): Promise<TableSchema>;

  /** Ping the database */
  ping(): Promise<number>;

  /** Close the connection */
  close(): void;

  /** Check if connected */
  isConnected(): boolean;
}

/**
 * Prepared statement interface
 */
export interface PreparedStatement {
  /** Statement ID */
  id: string;
  /** Original SQL */
  sql: string;
  /** Number of parameters */
  paramCount: number;
  /** Execute the prepared statement */
  run(params?: unknown[]): Promise<ExecResult>;
  /** Close the prepared statement */
  finalize(): Promise<void>;
}

// =============================================================================
// WebSocket Client Implementation
// =============================================================================

/**
 * Create a DoSQL client using WebSocket transport
 *
 * WebSocket transport is recommended for:
 * - Streaming large result sets
 * - Using .map() pipelining
 * - Long-running connections
 * - Real-time updates
 *
 * @param options - Client options
 * @returns DoSQL client instance
 *
 * @example
 * ```ts
 * const client = createWebSocketClient({
 *   url: 'wss://example.com/db/mydb/rpc',
 *   autoReconnect: true,
 * });
 *
 * // Use .map() chaining for pipelining
 * const user = await client.rpc.exec({ sql: 'SELECT * FROM users WHERE id = ?', params: [1] })
 *   .map(result => result.rows[0])
 *   .map(row => ({ id: row[0], name: row[1], email: row[2] }));
 *
 * client.close();
 * ```
 */
export function createWebSocketClient(options: DoSQLClientOptions): DoSQLClient {
  const rpc = newWebSocketRpcSession<DoSQLRpcApi>(options.url);

  return createClientFromRpc(rpc);
}

/**
 * Create a DoSQL client using HTTP batch transport
 *
 * HTTP batch transport is suitable for:
 * - Stateless requests
 * - Serverless environments
 * - Simple query execution
 *
 * Note: HTTP batch does not support streaming or real-time updates.
 *
 * @param options - Client options
 * @returns DoSQL client instance
 *
 * @example
 * ```ts
 * const client = createHttpClient({
 *   url: 'https://example.com/db/mydb/rpc',
 * });
 *
 * const result = await client.query('SELECT * FROM users LIMIT 10');
 *
 * client.close();
 * ```
 */
export function createHttpClient(options: DoSQLClientOptions): DoSQLClient {
  const rpc = newHttpBatchRpcSession<DoSQLRpcApi>(options.url);

  return createClientFromRpc(rpc);
}

/**
 * Create a DoSQL client (auto-detects transport from URL)
 *
 * @param url - Connection URL (ws://, wss:// for WebSocket; http://, https:// for HTTP)
 * @returns DoSQL client instance
 */
export function createDoSQLClient(url: string): DoSQLClient {
  if (url.startsWith('ws://') || url.startsWith('wss://')) {
    return createWebSocketClient({ url });
  }
  return createHttpClient({ url });
}

// =============================================================================
// Internal Implementation
// =============================================================================

function createClientFromRpc(rpc: RpcStub<DoSQLRpcApi>): DoSQLClient {
  let connected = true;

  const client: DoSQLClient = {
    rpc,

    async query(sql: string, params?: unknown[]): Promise<ExecResult> {
      return rpc.exec({ sql, params });
    },

    async execute(sql: string, params?: unknown[]): Promise<{ changes: number; lastInsertRowId?: number }> {
      const result = await rpc.exec({ sql, params });
      return {
        changes: result.changes ?? 0,
        lastInsertRowId: result.lastInsertRowId,
      };
    },

    async transaction(statements: Array<{ sql: string; params?: unknown[] }>): Promise<TransactionResult> {
      const ops: TransactionOp[] = statements.map((stmt) => ({
        type: 'exec',
        sql: stmt.sql,
        params: stmt.params,
      }));
      return rpc.transaction({ ops });
    },

    async prepare(sql: string): Promise<PreparedStatement> {
      const handle = await rpc.prepare({ sql });

      return {
        id: handle.id,
        sql: handle.sql,
        paramCount: handle.paramCount,

        async run(params?: unknown[]): Promise<ExecResult> {
          return rpc.run({ stmtId: handle.id, params });
        },

        async finalize(): Promise<void> {
          await rpc.finalize({ stmtId: handle.id });
        },
      };
    },

    async status(): Promise<DatabaseStatus> {
      return rpc.status();
    },

    async listTables(): Promise<TableInfo[]> {
      const result = await rpc.listTables();
      return result.tables;
    },

    async describeTable(table: string): Promise<TableSchema> {
      return rpc.describeTable({ table });
    },

    async ping(): Promise<number> {
      const start = performance.now();
      await rpc.ping();
      return performance.now() - start;
    },

    close(): void {
      connected = false;
      const disposable = rpc as unknown as { dispose?: () => void };
      if (typeof disposable.dispose === 'function') {
        disposable.dispose();
      }
    },

    isConnected(): boolean {
      return connected;
    },
  };

  return client;
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Execute a function within a transaction context
 *
 * Automatically handles commit on success and provides rollback capability.
 *
 * @param client - DoSQL client
 * @param fn - Function to execute (receives query function)
 * @returns Result of the function
 *
 * @example
 * ```ts
 * const result = await withTransaction(client, async (tx) => {
 *   await tx.execute('INSERT INTO users (name) VALUES (?)', ['Alice']);
 *   await tx.execute('INSERT INTO logs (action) VALUES (?)', ['user_created']);
 *   return { success: true };
 * });
 * ```
 */
export async function withTransaction<T>(
  client: DoSQLClient,
  fn: (tx: TransactionContext) => Promise<T>
): Promise<T> {
  const statements: Array<{ sql: string; params?: unknown[] }> = [];
  let result: T | undefined;

  const tx: TransactionContext = {
    async query(sql: string, params?: unknown[]): Promise<ExecResult> {
      statements.push({ sql, params });
      // In a real implementation, this would accumulate and execute at the end
      // For now, we just track the statements
      return { columns: [], rows: [], rowCount: 0 };
    },

    async execute(sql: string, params?: unknown[]): Promise<void> {
      statements.push({ sql, params });
    },
  };

  try {
    result = await fn(tx);

    // Execute all statements in a transaction
    const txResult = await client.transaction(statements);

    if (!txResult.committed) {
      const lastResult = txResult.results[txResult.results.length - 1];
      if ('error' in lastResult) {
        throw new Error(lastResult.error);
      }
      throw new Error('Transaction failed');
    }

    return result;
  } catch (error) {
    throw error;
  }
}

/**
 * Transaction context for withTransaction helper
 */
export interface TransactionContext {
  query(sql: string, params?: unknown[]): Promise<ExecResult>;
  execute(sql: string, params?: unknown[]): Promise<void>;
}

// =============================================================================
// Pipelining Examples
// =============================================================================

/**
 * Example: Using .map() chaining for efficient pipelining
 *
 * The .map() method allows you to transform results without waiting
 * for intermediate results. This is more efficient than separate awaits.
 *
 * @example
 * ```ts
 * // Inefficient: 3 round trips
 * const result = await rpc.exec({ sql: 'SELECT * FROM users WHERE id = ?', params: [1] });
 * const row = result.rows[0];
 * const name = row[1];
 *
 * // Efficient: 1 round trip with pipelining
 * const name = await rpc.exec({ sql: 'SELECT * FROM users WHERE id = ?', params: [1] })
 *   .map(result => result.rows[0])
 *   .map(row => row[1]);
 * ```
 */
export async function pipelineExample(rpc: RpcStub<DoSQLRpcApi>): Promise<void> {
  // Example 1: Get a single user's name
  const userName = await rpc.exec({ sql: 'SELECT name FROM users WHERE id = ?', params: [1] })
    .map((result) => result.rows[0])
    .map((row) => row ? row[0] : null);

  console.log('User name:', userName);

  // Example 2: Get count and transform to boolean
  const hasUsers = await rpc.exec({ sql: 'SELECT COUNT(*) FROM users' })
    .map((result) => result.rows[0])
    .map((row) => row ? Number(row[0]) > 0 : false);

  console.log('Has users:', hasUsers);

  // Example 3: Chain multiple operations
  const tables = await rpc.listTables()
    .map((result) => result.tables)
    .map((tables) => tables.map((t) => t.name));

  console.log('Table names:', tables);
}

/**
 * Example: Streaming large result sets
 *
 * For large result sets, you can use streaming to process rows
 * as they arrive without loading everything into memory.
 *
 * Note: This requires WebSocket transport.
 */
export async function streamingExample(rpc: RpcStub<DoSQLRpcApi>): Promise<void> {
  // For large result sets, consider using LIMIT and OFFSET for pagination
  let offset = 0;
  const limit = 100;
  let hasMore = true;

  while (hasMore) {
    const result = await rpc.exec({
      sql: `SELECT * FROM large_table LIMIT ${limit} OFFSET ${offset}`,
    });

    console.log(`Processing batch: ${result.rowCount} rows`);

    for (const row of result.rows) {
      // Process each row
      console.log('Row:', row);
    }

    hasMore = result.rowCount === limit;
    offset += limit;
  }
}
