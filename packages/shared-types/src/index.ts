/**
 * @dotdo/shared-types - Unified types for DoSQL ecosystem
 *
 * This package provides the canonical type definitions for:
 * - @dotdo/sql.do (client)
 * - @dotdo/dosql (server)
 * - @dotdo/lake.do (client)
 * - @dotdo/dolake (server)
 *
 * All packages should import shared types from here to ensure compatibility.
 */

// =============================================================================
// Branded Types
// =============================================================================

declare const TransactionIdBrand: unique symbol;
declare const LSNBrand: unique symbol;
declare const StatementHashBrand: unique symbol;
declare const ShardIdBrand: unique symbol;

/**
 * Branded type for transaction IDs
 */
export type TransactionId = string & { readonly [TransactionIdBrand]: never };

/**
 * Branded type for Log Sequence Numbers
 */
export type LSN = bigint & { readonly [LSNBrand]: never };

/**
 * Branded type for statement hashes
 */
export type StatementHash = string & { readonly [StatementHashBrand]: never };

/**
 * Branded type for shard identifiers
 */
export type ShardId = string & { readonly [ShardIdBrand]: never };

/**
 * Create a typed TransactionId from a string
 */
export function createTransactionId(id: string): TransactionId {
  return id as TransactionId;
}

/**
 * Create a typed LSN from a bigint
 */
export function createLSN(lsn: bigint): LSN {
  return lsn as LSN;
}

/**
 * Create a typed StatementHash from a string
 */
export function createStatementHash(hash: string): StatementHash {
  return hash as StatementHash;
}

/**
 * Create a typed ShardId from a string
 */
export function createShardId(id: string): ShardId {
  return id as ShardId;
}

// =============================================================================
// SQL Value Types
// =============================================================================

/**
 * Represents valid SQL values that can be passed as parameters or returned in results
 */
export type SQLValue = string | number | boolean | null | Uint8Array | bigint;

// =============================================================================
// Column Types
// =============================================================================

/**
 * Unified column type covering both SQL and JavaScript type representations
 *
 * SQL-style types (client-facing):
 * - INTEGER, REAL, TEXT, BLOB, NULL, BOOLEAN, DATETIME, JSON
 *
 * JavaScript-style types (wire format):
 * - string, number, bigint, boolean, date, timestamp, json, blob, null, unknown
 */
export type ColumnType =
  // SQL-style types (client-facing)
  | 'INTEGER'
  | 'REAL'
  | 'TEXT'
  | 'BLOB'
  | 'NULL'
  | 'BOOLEAN'
  | 'DATETIME'
  | 'JSON'
  // JavaScript-style types (wire format)
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

/**
 * SQL-style column types (client-facing)
 */
export type SQLColumnType =
  | 'INTEGER'
  | 'REAL'
  | 'TEXT'
  | 'BLOB'
  | 'NULL'
  | 'BOOLEAN'
  | 'DATETIME'
  | 'JSON';

/**
 * JavaScript-style column types (wire format)
 */
export type JSColumnType =
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

/**
 * Mapping from SQL types to JS types
 */
export const SQL_TO_JS_TYPE_MAP: Record<SQLColumnType, JSColumnType> = {
  INTEGER: 'number',
  REAL: 'number',
  TEXT: 'string',
  BLOB: 'blob',
  NULL: 'null',
  BOOLEAN: 'boolean',
  DATETIME: 'timestamp',
  JSON: 'json',
};

/**
 * Mapping from JS types to SQL types
 */
export const JS_TO_SQL_TYPE_MAP: Record<JSColumnType, SQLColumnType> = {
  string: 'TEXT',
  number: 'REAL',
  bigint: 'INTEGER',
  boolean: 'BOOLEAN',
  date: 'DATETIME',
  timestamp: 'DATETIME',
  json: 'JSON',
  blob: 'BLOB',
  null: 'NULL',
  unknown: 'TEXT',
};

/**
 * Convert SQL column type to JS column type
 */
export function sqlToJsType(sqlType: SQLColumnType): JSColumnType {
  return SQL_TO_JS_TYPE_MAP[sqlType];
}

/**
 * Convert JS column type to SQL column type
 */
export function jsToSqlType(jsType: JSColumnType): SQLColumnType {
  return JS_TO_SQL_TYPE_MAP[jsType];
}

// =============================================================================
// Idempotency Types
// =============================================================================

/**
 * Configuration for idempotency key generation and behavior
 */
export interface IdempotencyConfig {
  /** Whether to automatically generate idempotency keys for mutations */
  enabled: boolean;
  /** Custom key prefix (default: none) */
  keyPrefix?: string;
  /** Time-to-live for idempotency keys in milliseconds (server-side) */
  ttlMs?: number;
}

/**
 * Default idempotency configuration
 */
export const DEFAULT_IDEMPOTENCY_CONFIG: IdempotencyConfig = {
  enabled: true,
  ttlMs: 24 * 60 * 60 * 1000, // 24 hours
};

// =============================================================================
// Query Request Types
// =============================================================================

/**
 * Unified query request that supports all client and server features
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
  asOf?: bigint | Date | LSN;
  /** Transaction ID for transactional queries */
  transactionId?: string | TransactionId;
  /** Query timeout in milliseconds */
  timeout?: number;
  /** Alternative timeout field name (for compatibility) */
  timeoutMs?: number;
  /** Target shard for sharded queries */
  shardId?: string | ShardId;
  /** Whether to return results as streaming chunks */
  streaming?: boolean;
  /** Maximum rows to return (for pagination) */
  limit?: number;
  /** Offset for pagination */
  offset?: number;
  /** Idempotency key for mutation deduplication */
  idempotencyKey?: string;
}

/**
 * Client-facing query options (subset of QueryRequest)
 */
export interface QueryOptions {
  /** Transaction ID for transactional queries */
  transactionId?: TransactionId;
  /** Read from a specific point in time */
  asOf?: Date | LSN;
  /** Timeout in milliseconds */
  timeout?: number;
  /** Target shard for sharded queries */
  shardId?: ShardId;
  /** Named parameters (alternative to positional) */
  namedParams?: Record<string, unknown>;
  /** Branch/namespace for multi-tenant isolation */
  branch?: string;
  /** Whether to return results as streaming chunks */
  streaming?: boolean;
  /** Maximum rows to return (for pagination) */
  limit?: number;
  /** Offset for pagination */
  offset?: number;
}

// =============================================================================
// Query Response Types
// =============================================================================

/**
 * Unified query response that supports all features
 */
export interface QueryResponse<T = Record<string, SQLValue>> {
  /** Column names in result order */
  columns: string[];
  /** Column types (for client-side type reconstruction) */
  columnTypes?: ColumnType[];
  /** Result rows as objects (client format) */
  rows: T[];
  /** Result rows as arrays (wire format, optional) */
  rowsRaw?: unknown[][];
  /** Row format indicator */
  rowFormat?: 'objects' | 'arrays';
  /** Number of rows returned */
  rowCount: number;
  /** Number of rows affected (for mutations) */
  rowsAffected?: number;
  /** Current LSN after query execution */
  lsn?: bigint | LSN;
  /** Last inserted row ID */
  lastInsertRowid?: bigint;
  /** Execution time in milliseconds */
  executionTimeMs?: number;
  /** Alternative timing field (for compatibility) */
  duration?: number;
  /** Whether there are more rows available (pagination) */
  hasMore?: boolean;
  /** Cursor for fetching next page */
  cursor?: string;
}

/**
 * Client-facing query result (for backward compatibility)
 */
export interface QueryResult<T = Record<string, SQLValue>> {
  rows: T[];
  columns: string[];
  columnTypes?: ColumnType[];
  rowsAffected: number;
  lastInsertRowid?: bigint;
  duration: number;
  lsn?: LSN;
  hasMore?: boolean;
  cursor?: string;
}

// =============================================================================
// CDC (Change Data Capture) Types
// =============================================================================

/**
 * Unified CDC operation types (includes TRUNCATE for server-side operations)
 */
export type CDCOperation = 'INSERT' | 'UPDATE' | 'DELETE' | 'TRUNCATE';

/**
 * CDC operation types for client-side (excludes TRUNCATE)
 */
export type ClientCDCOperation = 'INSERT' | 'UPDATE' | 'DELETE';

/**
 * Numeric operation codes for efficient binary encoding
 */
export const CDCOperationCode = {
  INSERT: 0,
  UPDATE: 1,
  DELETE: 2,
  TRUNCATE: 3,
} as const;

export type CDCOperationCodeValue = (typeof CDCOperationCode)[CDCOperation];

/**
 * Unified CDC event that covers all package requirements
 */
export interface CDCEvent<T = unknown> {
  // === Identification ===
  /** LSN of this change (branded or plain) */
  lsn: bigint | LSN;
  /** Monotonically increasing sequence number (for dolake) */
  sequence?: number;

  // === Metadata ===
  /** Table that was modified */
  table: string;
  /** Type of operation */
  operation: CDCOperation;
  /** Timestamp of the change (Date or Unix timestamp) */
  timestamp: Date | number;

  // === Transaction Context ===
  /** Transaction ID (string or branded) */
  transactionId?: string | TransactionId;
  /** Alternative transaction ID field (for compatibility) */
  txId?: string;

  // === Data ===
  /** Primary key values */
  primaryKey?: Record<string, unknown>;
  /** Row data before the change (for UPDATE and DELETE) */
  before?: T;
  /** Row data after the change (for INSERT and UPDATE) */
  after?: T;
  /** Alternative: Old row data */
  oldRow?: Record<string, unknown>;
  /** Alternative: New row data */
  newRow?: Record<string, unknown>;
  /** Primary key or row identifier (for dolake) */
  rowId?: string;

  // === Extension ===
  /** Optional metadata */
  metadata?: Record<string, unknown>;
}

// =============================================================================
// Transaction Types
// =============================================================================

/**
 * Unified isolation level that covers all supported levels
 */
export type IsolationLevel =
  | 'READ_UNCOMMITTED'
  | 'READ_COMMITTED'
  | 'REPEATABLE_READ'
  | 'SERIALIZABLE'
  | 'SNAPSHOT';

/**
 * Server-supported isolation levels
 */
export type ServerIsolationLevel =
  | 'READ_COMMITTED'
  | 'REPEATABLE_READ'
  | 'SERIALIZABLE';

/**
 * Transaction options for beginning a transaction
 */
export interface TransactionOptions {
  isolationLevel?: IsolationLevel;
  readOnly?: boolean;
  timeout?: number;
  timeoutMs?: number;
  branch?: string;
}

/**
 * Transaction state information
 */
export interface TransactionState {
  id: TransactionId;
  isolationLevel: IsolationLevel;
  readOnly: boolean;
  startedAt: Date;
  snapshotLSN: LSN;
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

// =============================================================================
// RPC Types
// =============================================================================

/**
 * RPC method names
 */
export type RPCMethod =
  | 'exec'
  | 'query'
  | 'prepare'
  | 'execute'
  | 'beginTransaction'
  | 'commit'
  | 'rollback'
  | 'getSchema'
  | 'ping';

/**
 * RPC request envelope
 */
export interface RPCRequest {
  id: string;
  method: RPCMethod;
  params: unknown;
}

/**
 * RPC response envelope
 */
export interface RPCResponse<T = unknown> {
  id: string;
  result?: T;
  error?: RPCError;
}

/**
 * Unified RPC error codes
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

/**
 * Unified RPC error structure
 */
export interface RPCError {
  /** Error code (string or enum) */
  code: string | RPCErrorCode;
  /** Human-readable message */
  message: string;
  /** Additional error details */
  details?: Record<string, unknown> | unknown;
  /** Stack trace (in development) */
  stack?: string;
}

// =============================================================================
// Client Capabilities
// =============================================================================

/**
 * Capabilities advertised by the client during connection
 */
export interface ClientCapabilities {
  binaryProtocol: boolean;
  compression: boolean;
  batching: boolean;
  maxBatchSize: number;
  maxMessageSize: number;
}

/**
 * Default client capabilities
 */
export const DEFAULT_CLIENT_CAPABILITIES: ClientCapabilities = {
  binaryProtocol: true,
  compression: false,
  batching: true,
  maxBatchSize: 1000,
  maxMessageSize: 4 * 1024 * 1024,
};

// =============================================================================
// Schema Types
// =============================================================================

/**
 * Column definition in a table schema
 */
export interface ColumnDefinition {
  name: string;
  type: ColumnType | string;
  nullable: boolean;
  primaryKey: boolean;
  autoIncrement?: boolean;
  defaultValue?: SQLValue | string;
  unique?: boolean;
  doc?: string;
}

/**
 * Index definition
 */
export interface IndexDefinition {
  name: string;
  columns: string[];
  unique: boolean;
  type?: 'BTREE' | 'HASH' | 'GIN' | 'GIST';
}

/**
 * Foreign key definition
 */
export interface ForeignKeyDefinition {
  name: string;
  columns: string[];
  referencedTable: string;
  referencedColumns: string[];
  onDelete?: 'CASCADE' | 'SET NULL' | 'SET DEFAULT' | 'RESTRICT' | 'NO ACTION';
  onUpdate?: 'CASCADE' | 'SET NULL' | 'SET DEFAULT' | 'RESTRICT' | 'NO ACTION';
}

/**
 * Table schema definition
 */
export interface TableSchema {
  name: string;
  columns: ColumnDefinition[];
  primaryKey: string[];
  indexes?: IndexDefinition[];
  foreignKeys?: ForeignKeyDefinition[];
}

// =============================================================================
// Sharding Types
// =============================================================================

/**
 * Shard configuration
 */
export interface ShardConfig {
  shardCount: number;
  shardKey: string;
  shardFunction: 'hash' | 'range' | 'list';
}

/**
 * Shard information
 */
export interface ShardInfo {
  shardId: ShardId;
  keyRange?: { min: SQLValue; max: SQLValue };
  rowCount?: number;
}

// =============================================================================
// Connection Types
// =============================================================================

/**
 * Connection options for clients
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
// Prepared Statement Types
// =============================================================================

/**
 * Prepared statement handle
 */
export interface PreparedStatement {
  sql: string;
  hash: StatementHash;
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if a CDC event is from the server (has txId)
 */
export function isServerCDCEvent(event: CDCEvent): boolean {
  return 'txId' in event && typeof event.txId === 'string';
}

/**
 * Check if a CDC event is from the client (has transactionId)
 */
export function isClientCDCEvent(event: CDCEvent): boolean {
  return (
    'transactionId' in event &&
    (typeof event.transactionId === 'string' || event.transactionId !== undefined)
  );
}

/**
 * Check if timestamp is a Date
 */
export function isDateTimestamp(
  timestamp: Date | number
): timestamp is Date {
  return timestamp instanceof Date;
}

/**
 * Check if timestamp is a number (Unix timestamp)
 */
export function isNumericTimestamp(
  timestamp: Date | number
): timestamp is number {
  return typeof timestamp === 'number';
}

// =============================================================================
// Type Converters
// =============================================================================

/**
 * Convert server CDC event to client format
 */
export function serverToClientCDCEvent<T = unknown>(
  serverEvent: CDCEvent<T>
): CDCEvent<T> {
  return {
    lsn: serverEvent.lsn,
    timestamp: isNumericTimestamp(serverEvent.timestamp)
      ? new Date(serverEvent.timestamp)
      : serverEvent.timestamp,
    table: serverEvent.table,
    operation: serverEvent.operation,
    primaryKey: serverEvent.primaryKey ?? serverEvent.newRow ?? serverEvent.oldRow,
    before: (serverEvent.before ?? serverEvent.oldRow) as T | undefined,
    after: (serverEvent.after ?? serverEvent.newRow) as T | undefined,
    transactionId: serverEvent.transactionId ?? serverEvent.txId,
    metadata: serverEvent.metadata,
  };
}

/**
 * Convert client CDC event to server format
 */
export function clientToServerCDCEvent<T = unknown>(
  clientEvent: CDCEvent<T>
): CDCEvent<T> {
  return {
    lsn: clientEvent.lsn,
    timestamp: isDateTimestamp(clientEvent.timestamp)
      ? clientEvent.timestamp.getTime()
      : clientEvent.timestamp,
    table: clientEvent.table,
    operation: clientEvent.operation,
    txId: String(clientEvent.transactionId ?? clientEvent.txId ?? ''),
    oldRow: clientEvent.before as Record<string, unknown> | undefined,
    newRow: clientEvent.after as Record<string, unknown> | undefined,
    primaryKey: clientEvent.primaryKey,
    metadata: clientEvent.metadata,
  };
}

/**
 * Convert QueryResponse to QueryResult (client format)
 */
export function responseToResult<T = Record<string, SQLValue>>(
  response: QueryResponse<T>
): QueryResult<T> {
  return {
    rows: response.rows,
    columns: response.columns,
    columnTypes: response.columnTypes,
    rowsAffected: response.rowsAffected ?? response.rowCount,
    lastInsertRowid: response.lastInsertRowid,
    duration: response.duration ?? response.executionTimeMs ?? 0,
    lsn: response.lsn as LSN | undefined,
    hasMore: response.hasMore,
    cursor: response.cursor,
  };
}

/**
 * Convert QueryResult to QueryResponse (server format)
 */
export function resultToResponse<T = Record<string, SQLValue>>(
  result: QueryResult<T>,
  lsn?: bigint
): QueryResponse<T> {
  return {
    rows: result.rows,
    columns: result.columns,
    columnTypes: result.columnTypes,
    rowCount: result.rows.length,
    rowsAffected: result.rowsAffected,
    lastInsertRowid: result.lastInsertRowid,
    executionTimeMs: result.duration,
    lsn: lsn ?? (result.lsn as bigint | undefined),
    hasMore: result.hasMore,
    cursor: result.cursor,
  };
}
