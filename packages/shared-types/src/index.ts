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

// =============================================================================
// Runtime Mode Configuration
// =============================================================================

let _devMode = true; // Default to dev mode for safety
let _strictMode = false; // Strict mode for additional format validation

/**
 * Set development mode for enabling runtime validation
 */
export function setDevMode(enabled: boolean): void {
  _devMode = enabled;
}

/**
 * Check if development mode is enabled
 */
export function isDevMode(): boolean {
  return _devMode;
}

/**
 * Set strict mode for additional format validation
 */
export function setStrictMode(enabled: boolean): void {
  _strictMode = enabled;
}

/**
 * Check if strict mode is enabled
 */
export function isStrictMode(): boolean {
  return _strictMode;
}

// =============================================================================
// Validated Tracking (WeakSet for runtime-created branded types)
// =============================================================================

const validatedLSNs = new WeakSet<object>();
const validatedTransactionIds = new WeakSet<object>();
const validatedShardIds = new WeakSet<object>();
const validatedStatementHashes = new WeakSet<object>();

// Wrapper objects for primitive tracking
const lsnWrappers = new Map<bigint, { value: bigint }>();
const stringWrappers = new Map<string, { value: string; type: 'txn' | 'shard' | 'hash' }>();

/**
 * Check if an LSN was created through the factory function (validated)
 */
export function isValidatedLSN(lsn: LSN): boolean {
  const wrapper = lsnWrappers.get(lsn as bigint);
  return wrapper !== undefined && validatedLSNs.has(wrapper);
}

/**
 * Check if a TransactionId was created through the factory function (validated)
 */
export function isValidatedTransactionId(txnId: TransactionId): boolean {
  const wrapper = stringWrappers.get(txnId as string);
  return wrapper !== undefined && wrapper.type === 'txn' && validatedTransactionIds.has(wrapper);
}

/**
 * Check if a ShardId was created through the factory function (validated)
 */
export function isValidatedShardId(shardId: ShardId): boolean {
  const wrapper = stringWrappers.get(shardId as string);
  return wrapper !== undefined && wrapper.type === 'shard' && validatedShardIds.has(wrapper);
}

/**
 * Check if a StatementHash was created through the factory function (validated)
 */
export function isValidatedStatementHash(hash: StatementHash): boolean {
  const wrapper = stringWrappers.get(hash as string);
  return wrapper !== undefined && wrapper.type === 'hash' && validatedStatementHashes.has(wrapper);
}

// =============================================================================
// Type Guard Functions
// =============================================================================

/**
 * Check if a value is a valid LSN (bigint >= 0)
 */
export function isValidLSN(value: unknown): value is LSN {
  return typeof value === 'bigint' && value >= 0n;
}

/**
 * Check if a value is a valid TransactionId (non-empty string)
 */
export function isValidTransactionId(value: unknown): value is TransactionId {
  return typeof value === 'string' && value.trim().length > 0;
}

/**
 * Check if a value is a valid ShardId (non-empty string, max 255 chars)
 */
export function isValidShardId(value: unknown): value is ShardId {
  return typeof value === 'string' && value.trim().length > 0 && value.length <= 255;
}

/**
 * Check if a value is a valid StatementHash (non-empty string)
 */
export function isValidStatementHash(value: unknown): value is StatementHash {
  return typeof value === 'string' && value.length > 0;
}

// =============================================================================
// Factory Functions with Validation
// =============================================================================

/**
 * Create a typed TransactionId from a string
 * @throws Error if id is empty or whitespace-only (in dev mode)
 */
export function createTransactionId(id: string): TransactionId {
  if (_devMode || _strictMode) {
    if (typeof id !== 'string') {
      throw new Error('TransactionId must be a string');
    }
    if (id.trim().length === 0) {
      throw new Error('TransactionId cannot be empty');
    }
  }

  // Track as validated
  const wrapper = { value: id, type: 'txn' as const };
  stringWrappers.set(id, wrapper);
  validatedTransactionIds.add(wrapper);

  return id as TransactionId;
}

/**
 * Create a typed LSN from a bigint
 * @throws Error if lsn is negative or not a bigint (in dev mode)
 */
export function createLSN(lsn: bigint): LSN {
  if (_devMode || _strictMode) {
    if (typeof lsn !== 'bigint') {
      throw new Error('LSN must be a bigint');
    }
    if (lsn < 0n) {
      throw new Error(`LSN cannot be negative: ${lsn}`);
    }
  }

  // Track as validated
  const wrapper = { value: lsn };
  lsnWrappers.set(lsn, wrapper);
  validatedLSNs.add(wrapper);

  return lsn as LSN;
}

/**
 * Create a typed StatementHash from a string
 * @throws Error if hash is empty (in dev mode)
 */
export function createStatementHash(hash: string): StatementHash {
  if (_devMode || _strictMode) {
    if (typeof hash !== 'string') {
      throw new Error('StatementHash must be a string');
    }
    if (hash.length === 0) {
      throw new Error('StatementHash cannot be empty');
    }
    if (_strictMode && !/^[a-f0-9]+$/i.test(hash)) {
      throw new Error('Invalid StatementHash format');
    }
  }

  // Track as validated
  const wrapper = { value: hash, type: 'hash' as const };
  stringWrappers.set(hash, wrapper);
  validatedStatementHashes.add(wrapper);

  return hash as StatementHash;
}

/**
 * Create a typed ShardId from a string
 * @throws Error if id is empty, whitespace-only, or exceeds max length (in dev mode)
 */
export function createShardId(id: string): ShardId {
  if (_devMode || _strictMode) {
    if (typeof id !== 'string') {
      throw new Error('ShardId must be a string');
    }
    if (id.trim().length === 0) {
      throw new Error('ShardId cannot be empty');
    }
    if (id.length > 255) {
      throw new Error('ShardId exceeds maximum length');
    }
  }

  // Track as validated
  const wrapper = { value: id, type: 'shard' as const };
  stringWrappers.set(id, wrapper);
  validatedShardIds.add(wrapper);

  return id as ShardId;
}

// =============================================================================
// LSN Serialization / Deserialization
// =============================================================================

/**
 * Serialize an LSN to a string for JSON-safe transport
 */
export function serializeLSN(lsn: LSN): string {
  return String(lsn);
}

/**
 * Deserialize an LSN from a string
 * @throws Error if string cannot be parsed as a valid LSN
 */
export function deserializeLSN(str: string): LSN {
  const value = BigInt(str);
  return createLSN(value);
}

/**
 * Convert an LSN to a number (only safe for values within Number.MAX_SAFE_INTEGER)
 * @throws Error if LSN exceeds safe integer range
 */
export function lsnToNumber(lsn: LSN): number {
  if (lsn > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error('LSN exceeds safe integer range');
  }
  return Number(lsn);
}

/**
 * Convert an LSN to a Uint8Array (8 bytes, big-endian)
 */
export function lsnToBytes(lsn: LSN): Uint8Array {
  const bytes = new Uint8Array(8);
  let value = lsn as bigint;
  for (let i = 7; i >= 0; i--) {
    bytes[i] = Number(value & 0xffn);
    value >>= 8n;
  }
  return bytes;
}

/**
 * Convert a Uint8Array (8 bytes, big-endian) to an LSN
 */
export function bytesToLSN(bytes: Uint8Array): LSN {
  if (bytes.length !== 8) {
    throw new Error('LSN bytes must be exactly 8 bytes');
  }
  let value = 0n;
  for (let i = 0; i < 8; i++) {
    value = (value << 8n) | BigInt(bytes[i]);
  }
  return createLSN(value);
}

// =============================================================================
// LSN Utility Functions
// =============================================================================

/**
 * Compare two LSNs
 * @returns negative if a < b, 0 if equal, positive if a > b
 */
export function compareLSN(a: LSN, b: LSN): number {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

/**
 * Increment an LSN by a given amount (default 1)
 */
export function incrementLSN(lsn: LSN, amount: bigint = 1n): LSN {
  return createLSN((lsn as bigint) + amount);
}

/**
 * Extract the raw bigint value from an LSN
 */
export function lsnValue(lsn: LSN): bigint {
  return lsn as bigint;
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
  const result: CDCEvent<T> = {
    lsn: serverEvent.lsn,
    timestamp: isNumericTimestamp(serverEvent.timestamp)
      ? new Date(serverEvent.timestamp)
      : serverEvent.timestamp,
    table: serverEvent.table,
    operation: serverEvent.operation,
  };

  const primaryKey = serverEvent.primaryKey ?? serverEvent.newRow ?? serverEvent.oldRow;
  if (primaryKey !== undefined) result.primaryKey = primaryKey;

  const before = (serverEvent.before ?? serverEvent.oldRow) as T | undefined;
  if (before !== undefined) result.before = before;

  const after = (serverEvent.after ?? serverEvent.newRow) as T | undefined;
  if (after !== undefined) result.after = after;

  const transactionId = serverEvent.transactionId ?? serverEvent.txId;
  if (transactionId !== undefined) result.transactionId = transactionId;

  if (serverEvent.metadata !== undefined) result.metadata = serverEvent.metadata;

  return result;
}

/**
 * Convert client CDC event to server format
 */
export function clientToServerCDCEvent<T = unknown>(
  clientEvent: CDCEvent<T>
): CDCEvent<T> {
  const result: CDCEvent<T> = {
    lsn: clientEvent.lsn,
    timestamp: isDateTimestamp(clientEvent.timestamp)
      ? clientEvent.timestamp.getTime()
      : clientEvent.timestamp,
    table: clientEvent.table,
    operation: clientEvent.operation,
    txId: String(clientEvent.transactionId ?? clientEvent.txId ?? ''),
  };

  const oldRow = clientEvent.before as Record<string, unknown> | undefined;
  if (oldRow !== undefined) result.oldRow = oldRow;

  const newRow = clientEvent.after as Record<string, unknown> | undefined;
  if (newRow !== undefined) result.newRow = newRow;

  if (clientEvent.primaryKey !== undefined) result.primaryKey = clientEvent.primaryKey;
  if (clientEvent.metadata !== undefined) result.metadata = clientEvent.metadata;

  return result;
}

/**
 * Convert QueryResponse to QueryResult (client format)
 */
export function responseToResult<T = Record<string, SQLValue>>(
  response: QueryResponse<T>
): QueryResult<T> {
  const result: QueryResult<T> = {
    rows: response.rows,
    columns: response.columns,
    rowsAffected: response.rowsAffected ?? response.rowCount,
    duration: response.duration ?? response.executionTimeMs ?? 0,
  };

  if (response.columnTypes !== undefined) result.columnTypes = response.columnTypes;
  if (response.lastInsertRowid !== undefined) result.lastInsertRowid = response.lastInsertRowid;
  if (response.lsn !== undefined) result.lsn = response.lsn as LSN;
  if (response.hasMore !== undefined) result.hasMore = response.hasMore;
  if (response.cursor !== undefined) result.cursor = response.cursor;

  return result;
}

/**
 * Convert QueryResult to QueryResponse (server format)
 */
export function resultToResponse<T = Record<string, SQLValue>>(
  result: QueryResult<T>,
  lsn?: bigint
): QueryResponse<T> {
  const response: QueryResponse<T> = {
    rows: result.rows,
    columns: result.columns,
    rowCount: result.rows.length,
    rowsAffected: result.rowsAffected,
    executionTimeMs: result.duration,
  };

  if (result.columnTypes !== undefined) response.columnTypes = result.columnTypes;
  if (result.lastInsertRowid !== undefined) response.lastInsertRowid = result.lastInsertRowid;
  const lsnValue = lsn ?? result.lsn;
  if (lsnValue !== undefined) response.lsn = lsnValue as bigint;
  if (result.hasMore !== undefined) response.hasMore = result.hasMore;
  if (result.cursor !== undefined) response.cursor = result.cursor;

  return response;
}

// =============================================================================
// Standardized Result Pattern
// =============================================================================

/**
 * Standard error information for Result pattern
 *
 * Use this for expected failures (e.g., query errors, validation errors)
 * NOT for programmer errors (those should throw)
 */
export interface ResultError {
  /** Error code for programmatic handling */
  code: string;
  /** Human-readable error message */
  message: string;
  /** Additional error details */
  details?: Record<string, unknown>;
}

/**
 * Successful result with data
 */
export interface Success<T> {
  success: true;
  data: T;
  error?: never;
}

/**
 * Failed result with error
 */
export interface Failure {
  success: false;
  data?: never;
  error: ResultError;
}

/**
 * Standard Result type for operations that can fail
 *
 * Use this pattern for:
 * - Expected failures (query errors, validation errors, network errors)
 * - Operations where callers need to handle both success and failure cases
 *
 * DO NOT use this for:
 * - Programmer errors (invalid arguments) - throw instead
 * - Unexpected errors - let them propagate as exceptions
 *
 * @example
 * ```typescript
 * function parseQuery(sql: string): Result<ParsedQuery> {
 *   if (!sql.trim()) {
 *     return failure('EMPTY_QUERY', 'Query cannot be empty');
 *   }
 *   try {
 *     const parsed = parse(sql);
 *     return success(parsed);
 *   } catch (e) {
 *     return failure('PARSE_ERROR', e.message);
 *   }
 * }
 *
 * const result = parseQuery(sql);
 * if (isSuccess(result)) {
 *   console.log(result.data);
 * } else {
 *   console.error(result.error.message);
 * }
 * ```
 */
export type Result<T> = Success<T> | Failure;

// =============================================================================
// Result Type Guards
// =============================================================================

/**
 * Type guard to check if a Result is successful
 *
 * @example
 * ```typescript
 * const result = parseQuery(sql);
 * if (isSuccess(result)) {
 *   // TypeScript knows result.data is available
 *   console.log(result.data);
 * }
 * ```
 */
export function isSuccess<T>(result: Result<T>): result is Success<T> {
  return result.success === true;
}

/**
 * Type guard to check if a Result is a failure
 *
 * @example
 * ```typescript
 * const result = parseQuery(sql);
 * if (isFailure(result)) {
 *   // TypeScript knows result.error is available
 *   console.error(result.error.code, result.error.message);
 * }
 * ```
 */
export function isFailure<T>(result: Result<T>): result is Failure {
  return result.success === false;
}

// =============================================================================
// Result Constructors
// =============================================================================

/**
 * Create a successful Result
 *
 * @param data - The success data
 * @returns A Success result
 *
 * @example
 * ```typescript
 * return success({ id: 1, name: 'test' });
 * ```
 */
export function success<T>(data: T): Success<T> {
  return { success: true, data };
}

/**
 * Create a failed Result
 *
 * @param code - Error code for programmatic handling
 * @param message - Human-readable error message
 * @param details - Optional additional error details
 * @returns A Failure result
 *
 * @example
 * ```typescript
 * return failure('VALIDATION_ERROR', 'Email is invalid', { field: 'email' });
 * ```
 */
export function failure(
  code: string,
  message: string,
  details?: Record<string, unknown>
): Failure {
  return {
    success: false,
    error: { code, message, details },
  };
}

/**
 * Create a Failure from an Error object
 *
 * @param error - The Error object
 * @param code - Optional error code (defaults to 'UNKNOWN_ERROR')
 * @returns A Failure result
 *
 * @example
 * ```typescript
 * try {
 *   await riskyOperation();
 *   return success(data);
 * } catch (e) {
 *   return failureFromError(e, 'OPERATION_FAILED');
 * }
 * ```
 */
export function failureFromError(
  error: unknown,
  code: string = 'UNKNOWN_ERROR'
): Failure {
  const message = error instanceof Error ? error.message : String(error);
  return failure(code, message);
}

// =============================================================================
// Result Utilities
// =============================================================================

/**
 * Unwrap a Result, throwing if it's a failure
 *
 * Use this when you want to convert a Result back to throw/return style.
 * Useful at API boundaries or when you know the operation should succeed.
 *
 * @param result - The Result to unwrap
 * @returns The success data
 * @throws Error if the Result is a failure
 *
 * @example
 * ```typescript
 * // At an API boundary, convert Result to throw style
 * const data = unwrap(parseQuery(sql));
 * ```
 */
export function unwrap<T>(result: Result<T>): T {
  if (isSuccess(result)) {
    return result.data;
  }
  throw new Error(`${result.error.code}: ${result.error.message}`);
}

/**
 * Unwrap a Result with a default value for failures
 *
 * @param result - The Result to unwrap
 * @param defaultValue - Value to return if the Result is a failure
 * @returns The success data or the default value
 *
 * @example
 * ```typescript
 * const count = unwrapOr(getCount(), 0);
 * ```
 */
export function unwrapOr<T>(result: Result<T>, defaultValue: T): T {
  return isSuccess(result) ? result.data : defaultValue;
}

/**
 * Map the success value of a Result
 *
 * @param result - The Result to map
 * @param fn - Function to transform the success value
 * @returns A new Result with the transformed value
 *
 * @example
 * ```typescript
 * const userResult = getUser(id);
 * const nameResult = mapResult(userResult, user => user.name);
 * ```
 */
export function mapResult<T, U>(
  result: Result<T>,
  fn: (data: T) => U
): Result<U> {
  if (isSuccess(result)) {
    return success(fn(result.data));
  }
  return result;
}

/**
 * Flat-map the success value of a Result
 *
 * @param result - The Result to flat-map
 * @param fn - Function that returns a new Result
 * @returns The Result from the function, or the original failure
 *
 * @example
 * ```typescript
 * const userResult = getUser(id);
 * const profileResult = flatMapResult(userResult, user => getProfile(user.profileId));
 * ```
 */
export function flatMapResult<T, U>(
  result: Result<T>,
  fn: (data: T) => Result<U>
): Result<U> {
  if (isSuccess(result)) {
    return fn(result.data);
  }
  return result;
}

/**
 * Combine multiple Results into a single Result
 *
 * If all Results are successful, returns a Success with an array of data.
 * If any Result fails, returns the first Failure.
 *
 * @param results - Array of Results to combine
 * @returns Combined Result
 *
 * @example
 * ```typescript
 * const results = await Promise.all([getUser(1), getUser(2), getUser(3)]);
 * const combined = combineResults(results);
 * if (isSuccess(combined)) {
 *   console.log(combined.data); // [user1, user2, user3]
 * }
 * ```
 */
export function combineResults<T>(results: Result<T>[]): Result<T[]> {
  const data: T[] = [];
  for (const result of results) {
    if (isFailure(result)) {
      return result;
    }
    data.push(result.data);
  }
  return success(data);
}

// =============================================================================
// Legacy Result Pattern Support
// =============================================================================

/**
 * Legacy success/error result pattern (for backward compatibility)
 *
 * Many existing types use `{ success: boolean, error?: string }`.
 * Use these type guards to work with them consistently.
 */
export interface LegacyResult<T = unknown> {
  success: boolean;
  result?: T;
  error?: string;
  [key: string]: unknown;
}

/**
 * Type guard for legacy success result
 */
export function isLegacySuccess<T>(
  result: LegacyResult<T>
): result is LegacyResult<T> & { success: true } {
  return result.success === true;
}

/**
 * Type guard for legacy failure result
 */
export function isLegacyFailure<T>(
  result: LegacyResult<T>
): result is LegacyResult<T> & { success: false; error: string } {
  return result.success === false;
}

/**
 * Convert a legacy result to the new Result type
 *
 * @param legacy - Legacy result object
 * @param dataKey - Key to extract data from (default: 'result')
 * @returns Standardized Result
 */
export function fromLegacyResult<T>(
  legacy: LegacyResult<T>,
  dataKey: string = 'result'
): Result<T> {
  if (legacy.success) {
    return success(legacy[dataKey] as T);
  }
  return failure('LEGACY_ERROR', legacy.error ?? 'Unknown error');
}
