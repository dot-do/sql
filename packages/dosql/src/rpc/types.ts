/**
 * DoSQL RPC Message Types
 *
 * Defines the message types for CapnWeb RPC communication between
 * DoSQL clients and Durable Objects.
 *
 * This module re-exports unified types from @dotdo/shared-types via @dotdo/sql.do
 * and provides server-specific types.
 *
 * @packageDocumentation
 */

// =============================================================================
// Additional Type Imports (for extends clauses)
// =============================================================================

import type {
  QueryRequest as BaseQueryRequest,
  QueryResponse as BaseQueryResponse,
  CDCEvent as BaseCDCEvent,
  CDCOperation as BaseCDCOperation,
  TransactionHandle as BaseTransactionHandle,
  ServerIsolationLevel as BaseServerIsolationLevel,
  ConnectionStats as BaseConnectionStats,
} from '@dotdo/sql.do';

// =============================================================================
// Re-export Unified Types from shared-types
// =============================================================================

/**
 * Re-exported types from `@dotdo/sql.do` (which re-exports from `@dotdo/shared-types`).
 *
 * These types are re-exported for server-side RPC implementation. Using the same
 * type definitions ensures wire-level compatibility between clients and servers.
 *
 * ## Column Types
 *
 * Types for SQL column definitions and conversions:
 *
 * - {@link ColumnType} - Unified column type (SQL and JS representations)
 * - {@link SQLColumnType} - SQL-style types (INTEGER, TEXT, etc.)
 * - {@link JSColumnType} - JavaScript-style types (string, number, etc.)
 * - `SQL_TO_JS_TYPE_MAP` / `JS_TO_SQL_TYPE_MAP` - Type mappings
 * - `sqlToJsType()` / `jsToSqlType()` - Type conversion functions
 *
 * ## Query Types
 *
 * Core types for query execution:
 *
 * - {@link QueryRequest} - Request structure for SQL queries
 * - {@link QueryResponse} - Server response with results
 * - {@link QueryResult} - Client-facing result format
 * - {@link QueryOptions} - Query execution options
 *
 * ## CDC Types
 *
 * Change Data Capture types for replication:
 *
 * - {@link CDCOperation} - Operation types (INSERT, UPDATE, DELETE, TRUNCATE)
 * - {@link CDCEvent} - Change event with before/after data
 * - `CDCOperationCode` - Numeric codes for binary encoding
 *
 * ## Transaction Types
 *
 * Types for transaction management:
 *
 * - {@link IsolationLevel} - All isolation levels
 * - {@link ServerIsolationLevel} - Server-supported isolation levels
 * - {@link TransactionOptions} - Options for beginning transactions
 * - {@link TransactionState} - Current transaction state
 * - {@link TransactionHandle} - Handle returned after begin
 *
 * ## RPC Types
 *
 * Types for the RPC protocol:
 *
 * - `RPCErrorCode` - Standard error codes enum
 * - {@link RPCError} - Error structure
 * - {@link RPCRequest} - Request envelope
 * - {@link RPCResponse} - Response envelope
 *
 * ## Client Capabilities
 *
 * Protocol negotiation types:
 *
 * - {@link ClientCapabilities} - Client capability flags
 * - `DEFAULT_CLIENT_CAPABILITIES` - Default capability values
 *
 * ## Connection Types
 *
 * Types for connection management:
 *
 * - {@link ConnectionOptions} - Connection configuration
 * - {@link ConnectionStats} - Connection statistics
 *
 * ## Type Guards and Converters
 *
 * Utility functions for type checking and conversion:
 *
 * - `isServerCDCEvent()` / `isClientCDCEvent()` - CDC format detection
 * - `isDateTimestamp()` / `isNumericTimestamp()` - Timestamp format detection
 * - `serverToClientCDCEvent()` / `clientToServerCDCEvent()` - CDC format conversion
 * - `responseToResult()` / `resultToResponse()` - Query result conversion
 *
 * @see {@link https://github.com/dotdo/sql.do | @dotdo/sql.do} for client types
 * @see {@link https://github.com/dotdo/shared-types | @dotdo/shared-types} for canonical definitions
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export {
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
  type QueryResponse,
  type QueryResult,
  type QueryOptions,

  // CDC Types
  type CDCOperation,
  type CDCEvent,
  CDCOperationCode,

  // Transaction Types
  type IsolationLevel,
  type ServerIsolationLevel,
  type TransactionOptions,
  type TransactionState,
  type TransactionHandle,

  // RPC Types
  RPCErrorCode,
  type RPCError,
  type RPCRequest,
  type RPCResponse,

  // Client Capabilities
  type ClientCapabilities,
  DEFAULT_CLIENT_CAPABILITIES,

  // Connection Types
  type ConnectionOptions,
  type ConnectionStats,

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
} from '@dotdo/sql.do';

// =============================================================================
// Streaming Types (Server-specific)
// =============================================================================

/**
 * Request for streaming query results
 *
 * Used for large result sets that should be streamed
 * rather than returned in a single response.
 *
 * @example
 * ```typescript
 * // Stream large query results in chunks of 1000 rows
 * const streamRequest: StreamRequest = {
 *   sql: 'SELECT * FROM events WHERE timestamp > ?',
 *   params: ['2024-01-01'],
 *   chunkSize: 1000,
 *   maxRows: 100000,
 *   branch: 'main'
 * };
 *
 * // Process chunks as they arrive
 * for await (const chunk of client.queryStream(streamRequest)) {
 *   console.log(`Received chunk ${chunk.chunkIndex} with ${chunk.rowCount} rows`);
 *   processRows(chunk.rows);
 *   if (chunk.isLast) {
 *     console.log(`Stream complete: ${chunk.totalRowsSoFar} total rows`);
 *   }
 * }
 * ```
 */
export interface StreamRequest {
  /** SQL query string */
  sql: string;
  /** Query parameters */
  params?: unknown[];
  /** Chunk size (number of rows per chunk) */
  chunkSize?: number;
  /** Maximum total rows to stream */
  maxRows?: number;
  /** Branch for multi-tenant isolation */
  branch?: string;
}

/**
 * A chunk of streaming query results
 */
export interface StreamChunk {
  /** Chunk sequence number (0-indexed) */
  chunkIndex: number;
  /** Rows in this chunk */
  rows: unknown[][];
  /** Row count in this chunk */
  rowCount: number;
  /** Whether this is the final chunk */
  isLast: boolean;
  /** Total rows streamed so far */
  totalRowsSoFar: number;
}

/**
 * Stream completion message
 */
export interface StreamComplete {
  /** Total chunks sent */
  totalChunks: number;
  /** Total rows sent */
  totalRows: number;
  /** Final LSN */
  lsn: bigint;
  /** Total execution time */
  executionTimeMs: number;
}

// =============================================================================
// CDC (Change Data Capture) Server Types
// =============================================================================

/**
 * Request to subscribe to CDC events
 *
 * Enables real-time change notifications for specified tables.
 */
export interface CDCRequest {
  /** Starting LSN (exclusive - will receive changes after this LSN) */
  fromLSN: bigint;
  /** Tables to subscribe to (empty = all tables) */
  tables?: string[];
  /** Operations to filter (empty = all operations) */
  operations?: BaseCDCOperation[];
  /** Branch to subscribe to */
  branch?: string;
  /** Include row data in events */
  includeRowData?: boolean;
  /** Maximum events to buffer before backpressure */
  maxBufferSize?: number;
}

/**
 * CDC subscription acknowledgment
 */
export interface CDCAck {
  /** Subscription ID */
  subscriptionId: string;
  /** Current LSN at subscription start */
  currentLSN: bigint;
  /** Tables being subscribed to */
  subscribedTables: string[];
}

// =============================================================================
// Transaction Server Types
// =============================================================================

/**
 * Request to begin a transaction
 */
export interface BeginTransactionRequest {
  /** Isolation level */
  isolation?: BaseServerIsolationLevel;
  /** Transaction timeout in milliseconds */
  timeoutMs?: number;
  /** Branch for the transaction */
  branch?: string;
  /** Whether this is a read-only transaction */
  readOnly?: boolean;
}

/**
 * Request to execute within a transaction
 */
export interface TransactionQueryRequest extends BaseQueryRequest {
  /** Transaction ID */
  txId: string;
}

/**
 * Request to commit a transaction
 */
export interface CommitRequest {
  /** Transaction ID */
  txId: string;
}

/**
 * Request to rollback a transaction
 */
export interface RollbackRequest {
  /** Transaction ID */
  txId: string;
  /** Optional savepoint to rollback to */
  savepoint?: string;
}

/**
 * Transaction commit/rollback result
 */
export interface TransactionResult {
  /** Whether the operation succeeded */
  success: boolean;
  /** Final LSN after commit (for commits) */
  lsn?: bigint;
  /** Error message (if failed) */
  error?: string;
}

// =============================================================================
// Batch Operations
// =============================================================================

/**
 * Request to execute multiple queries in a batch
 */
export interface BatchRequest {
  /** Array of queries to execute */
  queries: BaseQueryRequest[];
  /** Whether to execute in a single transaction */
  atomic?: boolean;
  /** Whether to continue on error */
  continueOnError?: boolean;
  /** Branch for all queries */
  branch?: string;
}

/**
 * Result of batch execution
 */
export interface BatchResponse {
  /** Results for each query (in order) */
  results: Array<BaseQueryResponse | BatchError>;
  /** Number of successful queries */
  successCount: number;
  /** Number of failed queries */
  errorCount: number;
  /** Total execution time */
  executionTimeMs: number;
  /** Final LSN after batch */
  lsn: bigint;
}

/**
 * Error from a single query in a batch
 */
export interface BatchError {
  /** Index of the failed query */
  index: number;
  /** Error message */
  error: string;
  /** Error code */
  code?: string;
}

// =============================================================================
// Schema Operations
// =============================================================================

/**
 * Request to get schema information
 */
export interface SchemaRequest {
  /** Tables to get schema for (empty = all tables) */
  tables?: string[];
  /** Branch to query */
  branch?: string;
  /** Include indexes */
  includeIndexes?: boolean;
  /** Include foreign keys */
  includeForeignKeys?: boolean;
}

/**
 * Schema information response
 */
export interface SchemaResponse {
  /** Table schemas */
  tables: TableSchema[];
  /** Current schema version */
  version: number;
  /** Last schema modification LSN */
  lastModifiedLSN: bigint;
}

/**
 * Schema for a single table (server-specific with full details)
 */
export interface TableSchema {
  /** Table name */
  name: string;
  /** Column definitions */
  columns: ColumnSchema[];
  /** Primary key columns */
  primaryKey: string[];
  /** Index definitions */
  indexes?: IndexSchema[];
  /** Foreign key definitions */
  foreignKeys?: ForeignKeySchema[];
}

/**
 * Column schema definition
 */
export interface ColumnSchema {
  /** Column name */
  name: string;
  /** SQL type */
  type: string;
  /** Whether column is nullable */
  nullable: boolean;
  /** Default value expression */
  defaultValue?: string;
  /** Whether column is auto-increment */
  autoIncrement?: boolean;
}

/**
 * Index schema definition
 */
export interface IndexSchema {
  /** Index name */
  name: string;
  /** Columns in the index */
  columns: string[];
  /** Whether index is unique */
  unique: boolean;
  /** Index type */
  type?: 'BTREE' | 'HASH' | 'GIN' | 'GIST';
}

/**
 * Foreign key schema definition
 */
export interface ForeignKeySchema {
  /** Constraint name */
  name: string;
  /** Source columns */
  columns: string[];
  /** Referenced table */
  referencedTable: string;
  /** Referenced columns */
  referencedColumns: string[];
  /** On delete action */
  onDelete?: 'CASCADE' | 'SET NULL' | 'SET DEFAULT' | 'RESTRICT' | 'NO ACTION';
  /** On update action */
  onUpdate?: 'CASCADE' | 'SET NULL' | 'SET DEFAULT' | 'RESTRICT' | 'NO ACTION';
}

// =============================================================================
// DoSQL API Interface
// =============================================================================

/**
 * DoSQL RPC API interface
 *
 * This interface defines all methods available via CapnWeb RPC.
 * Used for type-safe client generation.
 */
export interface DoSQLAPI {
  // Query operations
  query(request: BaseQueryRequest): Promise<BaseQueryResponse>;
  queryStream(request: StreamRequest): AsyncIterable<StreamChunk>;

  // Transaction operations
  beginTransaction(request: BeginTransactionRequest): Promise<BaseTransactionHandle>;
  commit(request: CommitRequest): Promise<TransactionResult>;
  rollback(request: RollbackRequest): Promise<TransactionResult>;

  // Batch operations
  batch(request: BatchRequest): Promise<BatchResponse>;

  // CDC operations
  subscribeCDC(request: CDCRequest): AsyncIterable<BaseCDCEvent>;
  unsubscribeCDC(subscriptionId: string): Promise<void>;

  // Schema operations
  getSchema(request: SchemaRequest): Promise<SchemaResponse>;

  // Connection operations
  ping(): Promise<{ pong: true; lsn: bigint; timestamp: number }>;
  getStats(): Promise<BaseConnectionStats>;
}
