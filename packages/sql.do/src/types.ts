/**
 * @dotdo/sql.do - Shared types for DoSQL client and server
 *
 * This module re-exports types from @dotdo/shared-types for backward compatibility
 * and adds any client-specific type definitions.
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
 * SQL Client interface for sql.do
 */
export interface SQLClient {
  /**
   * Execute a SQL statement (INSERT, UPDATE, DELETE, DDL)
   */
  exec(sql: string, params?: SQLValue[], options?: QueryOptions): Promise<QueryResult>;

  /**
   * Execute a SQL query (SELECT)
   */
  query<T = Record<string, SQLValue>>(
    sql: string,
    params?: SQLValue[],
    options?: QueryOptions
  ): Promise<QueryResult<T>>;

  /**
   * Prepare a SQL statement for repeated execution
   */
  prepare(sql: string): Promise<PreparedStatement>;

  /**
   * Execute a prepared statement
   */
  execute<T = Record<string, SQLValue>>(
    statement: PreparedStatement,
    params?: SQLValue[],
    options?: QueryOptions
  ): Promise<QueryResult<T>>;

  /**
   * Begin a transaction
   */
  beginTransaction(options?: TransactionOptions): Promise<TransactionState>;

  /**
   * Commit a transaction
   */
  commit(transactionId: TransactionId): Promise<LSN>;

  /**
   * Rollback a transaction
   */
  rollback(transactionId: TransactionId): Promise<void>;

  /**
   * Get table schema
   */
  getSchema(tableName: string): Promise<TableSchema | null>;

  /**
   * Check connection health
   */
  ping(): Promise<{ latency: number }>;

  /**
   * Close the connection
   */
  close(): Promise<void>;
}
