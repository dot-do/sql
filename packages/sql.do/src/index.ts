/**
 * @dotdo/sql.do - Client SDK for DoSQL
 *
 * SQL database client for Cloudflare Workers using CapnWeb RPC.
 *
 * @example
 * ```typescript
 * import { createSQLClient } from '@dotdo/sql.do';
 *
 * const client = createSQLClient({
 *   url: 'https://sql.example.com',
 *   token: 'your-token',
 * });
 *
 * // Execute queries
 * const result = await client.query('SELECT * FROM users WHERE id = ?', [1]);
 *
 * // Use transactions
 * await client.transaction(async (tx) => {
 *   await tx.exec('INSERT INTO users (name) VALUES (?)', ['Alice']);
 *   await tx.exec('INSERT INTO logs (action) VALUES (?)', ['user_created']);
 * });
 *
 * await client.close();
 * ```
 */

// Client
export { DoSQLClient, TransactionContext, SQLError, createSQLClient } from './client.js';
export type { SQLClientConfig, RetryConfig } from './client.js';

// Types
export type {
  // Branded types
  TransactionId,
  LSN,
  StatementHash,
  ShardId,
  // Query types
  SQLValue,
  QueryResult,
  PreparedStatement,
  QueryOptions,
  // Transaction types
  IsolationLevel,
  TransactionOptions,
  TransactionState,
  // Schema types
  ColumnType,
  ColumnDefinition,
  TableSchema,
  IndexDefinition,
  // RPC types
  RPCMethod,
  RPCRequest,
  RPCResponse,
  RPCError,
  // Client interface
  SQLClient,
  // Sharding types
  ShardConfig,
  ShardInfo,
  // CDC types
  CDCOperation,
  CDCEvent,
} from './types.js';

// Brand constructors
export {
  createTransactionId,
  createLSN,
  createStatementHash,
  createShardId,
} from './types.js';
