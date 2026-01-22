/**
 * DoSQL RPC Message Types
 *
 * Defines the message types for CapnWeb RPC communication between
 * DoSQL clients and Durable Objects.
 *
 * @packageDocumentation
 */

// =============================================================================
// Core Query Types
// =============================================================================

/**
 * Request to execute a SQL query
 *
 * Supports:
 * - Standard SQL execution
 * - Parameterized queries for security
 * - Branch-based queries for multi-tenancy
 * - Time travel queries via LSN
 */
export interface QueryRequest {
  /** SQL query string */
  sql: string;
  /** Positional parameters (prevents SQL injection) */
  params?: unknown[];
  /** Named parameters (alternative to positional) */
  namedParams?: Record<string, unknown>;
  /** Branch/namespace for multi-tenant isolation */
  branch?: string;
  /** LSN (Log Sequence Number) for time travel queries */
  asOf?: bigint;
  /** Query timeout in milliseconds */
  timeoutMs?: number;
  /** Whether to return results as streaming chunks */
  streaming?: boolean;
  /** Maximum rows to return (for pagination) */
  limit?: number;
  /** Offset for pagination */
  offset?: number;
}

/**
 * Response from a SQL query execution
 */
export interface QueryResponse {
  /** Column names in result order */
  columns: string[];
  /** Column types (for client-side type reconstruction) */
  columnTypes: ColumnType[];
  /** Result rows (columnar format for efficiency) */
  rows: unknown[][];
  /** Total row count returned */
  rowCount: number;
  /** Current LSN after query execution */
  lsn: bigint;
  /** Execution time in milliseconds */
  executionTimeMs: number;
  /** Whether there are more rows available (pagination) */
  hasMore?: boolean;
  /** Cursor for fetching next page */
  cursor?: string;
}

/**
 * Column type metadata for client-side type reconstruction
 */
export type ColumnType =
  | 'string'
  | 'number'
  | 'bigint'
  | 'boolean'
  | 'date'
  | 'timestamp'
  | 'json'
  | 'blob'
  | 'null'
  | 'unknown';

// =============================================================================
// Streaming Types
// =============================================================================

/**
 * Request for streaming query results
 *
 * Used for large result sets that should be streamed
 * rather than returned in a single response.
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
// CDC (Change Data Capture) Types
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
  operations?: CDCOperation[];
  /** Branch to subscribe to */
  branch?: string;
  /** Include row data in events */
  includeRowData?: boolean;
  /** Maximum events to buffer before backpressure */
  maxBufferSize?: number;
}

/**
 * CDC operation type
 */
export type CDCOperation = 'INSERT' | 'UPDATE' | 'DELETE' | 'TRUNCATE';

/**
 * A single CDC event
 */
export interface CDCEvent {
  /** LSN of this change */
  lsn: bigint;
  /** Table that was modified */
  table: string;
  /** Type of operation */
  operation: CDCOperation;
  /** Timestamp of the change */
  timestamp: number;
  /** Transaction ID (for grouping related changes) */
  txId: string;
  /** Old row data (for UPDATE and DELETE) */
  oldRow?: Record<string, unknown>;
  /** New row data (for INSERT and UPDATE) */
  newRow?: Record<string, unknown>;
  /** Primary key values */
  primaryKey?: Record<string, unknown>;
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
// Transaction Types
// =============================================================================

/**
 * Request to begin a transaction
 */
export interface BeginTransactionRequest {
  /** Isolation level */
  isolation?: 'READ_COMMITTED' | 'REPEATABLE_READ' | 'SERIALIZABLE';
  /** Transaction timeout in milliseconds */
  timeoutMs?: number;
  /** Branch for the transaction */
  branch?: string;
  /** Whether this is a read-only transaction */
  readOnly?: boolean;
}

/**
 * Transaction handle returned after begin
 */
export interface TransactionHandle {
  /** Transaction ID */
  txId: string;
  /** LSN at transaction start */
  startLSN: bigint;
  /** Expiration timestamp */
  expiresAt: number;
}

/**
 * Request to execute within a transaction
 */
export interface TransactionQueryRequest extends QueryRequest {
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
  queries: QueryRequest[];
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
  results: Array<QueryResponse | BatchError>;
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
 * Schema for a single table
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
// Error Types
// =============================================================================

/**
 * RPC error response
 */
export interface RPCError {
  /** Error code */
  code: RPCErrorCode;
  /** Human-readable message */
  message: string;
  /** Additional error details */
  details?: Record<string, unknown>;
  /** Stack trace (in development) */
  stack?: string;
}

/**
 * RPC error codes
 */
export enum RPCErrorCode {
  // General errors
  UNKNOWN = 'UNKNOWN',
  INVALID_REQUEST = 'INVALID_REQUEST',
  TIMEOUT = 'TIMEOUT',
  INTERNAL_ERROR = 'INTERNAL_ERROR',

  // Query errors
  SYNTAX_ERROR = 'SYNTAX_ERROR',
  TABLE_NOT_FOUND = 'TABLE_NOT_FOUND',
  COLUMN_NOT_FOUND = 'COLUMN_NOT_FOUND',
  CONSTRAINT_VIOLATION = 'CONSTRAINT_VIOLATION',
  TYPE_MISMATCH = 'TYPE_MISMATCH',

  // Transaction errors
  TRANSACTION_NOT_FOUND = 'TRANSACTION_NOT_FOUND',
  TRANSACTION_ABORTED = 'TRANSACTION_ABORTED',
  DEADLOCK_DETECTED = 'DEADLOCK_DETECTED',
  SERIALIZATION_FAILURE = 'SERIALIZATION_FAILURE',

  // CDC errors
  INVALID_LSN = 'INVALID_LSN',
  SUBSCRIPTION_ERROR = 'SUBSCRIPTION_ERROR',
  BUFFER_OVERFLOW = 'BUFFER_OVERFLOW',

  // Authentication/Authorization
  UNAUTHORIZED = 'UNAUTHORIZED',
  FORBIDDEN = 'FORBIDDEN',

  // Resource errors
  RESOURCE_EXHAUSTED = 'RESOURCE_EXHAUSTED',
  QUOTA_EXCEEDED = 'QUOTA_EXCEEDED',
}

// =============================================================================
// Connection Types
// =============================================================================

/**
 * Connection options for DoSQL client
 */
export interface ConnectionOptions {
  /** WebSocket URL or HTTP endpoint */
  url: string;
  /** Default branch */
  defaultBranch?: string;
  /** Connection timeout in milliseconds */
  connectTimeoutMs?: number;
  /** Query timeout in milliseconds */
  queryTimeoutMs?: number;
  /** Auto-reconnect on disconnect */
  autoReconnect?: boolean;
  /** Maximum reconnect attempts */
  maxReconnectAttempts?: number;
  /** Reconnect delay in milliseconds */
  reconnectDelayMs?: number;
}

/**
 * Connection statistics
 */
export interface ConnectionStats {
  /** Whether currently connected */
  connected: boolean;
  /** Connection ID (if connected) */
  connectionId?: string;
  /** Current branch */
  branch?: string;
  /** Current LSN */
  currentLSN?: bigint;
  /** Round-trip latency in milliseconds */
  latencyMs?: number;
  /** Messages sent */
  messagesSent: number;
  /** Messages received */
  messagesReceived: number;
  /** Reconnect count */
  reconnectCount: number;
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
  query(request: QueryRequest): Promise<QueryResponse>;
  queryStream(request: StreamRequest): AsyncIterable<StreamChunk>;

  // Transaction operations
  beginTransaction(request: BeginTransactionRequest): Promise<TransactionHandle>;
  commit(request: CommitRequest): Promise<TransactionResult>;
  rollback(request: RollbackRequest): Promise<TransactionResult>;

  // Batch operations
  batch(request: BatchRequest): Promise<BatchResponse>;

  // CDC operations
  subscribeCDC(request: CDCRequest): AsyncIterable<CDCEvent>;
  unsubscribeCDC(subscriptionId: string): Promise<void>;

  // Schema operations
  getSchema(request: SchemaRequest): Promise<SchemaResponse>;

  // Connection operations
  ping(): Promise<{ pong: true; lsn: bigint; timestamp: number }>;
  getStats(): Promise<ConnectionStats>;
}
