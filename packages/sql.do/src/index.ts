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

// Types - re-exported from shared-types via types.ts
export type {
  // Branded types
  TransactionId,
  LSN,
  StatementHash,
  ShardId,
  // Query types
  SQLValue,
  QueryResult,
  QueryResponse,
  QueryRequest,
  PreparedStatement,
  QueryOptions,
  // Transaction types
  IsolationLevel,
  ServerIsolationLevel,
  TransactionOptions,
  TransactionState,
  TransactionHandle,
  // Schema types
  ColumnType,
  SQLColumnType,
  JSColumnType,
  ColumnDefinition,
  TableSchema,
  IndexDefinition,
  ForeignKeyDefinition,
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
  ClientCDCOperation,
  CDCEvent,
  CDCOperationCodeValue,
  // Client capabilities
  ClientCapabilities,
  // Connection types
  ConnectionOptions,
  ConnectionStats,
} from './types.js';

// Brand constructors and utilities
export {
  createTransactionId,
  createLSN,
  createStatementHash,
  createShardId,
  // Column type utilities
  SQL_TO_JS_TYPE_MAP,
  JS_TO_SQL_TYPE_MAP,
  sqlToJsType,
  jsToSqlType,
  // CDC utilities
  CDCOperationCode,
  // Type guards
  isServerCDCEvent,
  isClientCDCEvent,
  isDateTimestamp,
  isNumericTimestamp,
  // Type converters
  serverToClientCDCEvent,
  clientToServerCDCEvent,
  responseToResult,
  resultToResponse,
  // Default values
  DEFAULT_CLIENT_CAPABILITIES,
} from './types.js';

// Re-export RPCErrorCode enum
export { RPCErrorCode } from './types.js';
