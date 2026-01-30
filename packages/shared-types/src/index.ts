/**
 * @dotdo/sql-types - Unified types for DoSQL ecosystem
 *
 * This package provides the canonical type definitions for:
 * - sql.do (client)
 * - dosql (server)
 * - lake.do (client)
 * - dolake (server)
 *
 * All packages should import shared types from here to ensure compatibility.
 *
 * ## Stability
 *
 * This package follows semantic versioning. Exports are marked with stability annotations:
 *
 * - **stable**: No breaking changes in minor versions. Safe for production use.
 * - **experimental**: May change in any version. Use with caution.
 *
 * @packageDocumentation
 */

// =============================================================================
// Runtime Configuration (re-exported from config.ts for backwards compatibility)
// =============================================================================

/**
 * Runtime configuration utilities for cache and mode settings.
 * @public
 * @stability stable
 */
export {
  // Wrapper cache configuration
  DEFAULT_MAX_WRAPPER_CACHE_SIZE,
  type WrapperCacheConfig,
  setWrapperCacheConfig,
  getWrapperCacheConfig,
  // Runtime mode configuration
  setDevMode,
  isDevMode,
  setStrictMode,
  isStrictMode,
} from './config.js';

// Internal imports for use in this module
import {
  _getWrapperCacheConfigInternal,
  _isDevModeInternal,
  _isStrictModeInternal,
} from './config.js';

// =============================================================================
// Branded Types
// =============================================================================

/**
 * Branded types for type-safe identifiers.
 * @public
 * @stability stable
 */

declare const TransactionIdBrand: unique symbol;
declare const LSNBrand: unique symbol;
declare const StatementHashBrand: unique symbol;
declare const ShardIdBrand: unique symbol;

/**
 * Branded type for transaction IDs
 *
 * TransactionId is a string branded type that identifies a unique database transaction.
 * Use `createTransactionId()` to create validated instances.
 *
 * @example
 * ```typescript
 * import { createTransactionId, TransactionId, isValidatedTransactionId } from '@dotdo/sql-types';
 *
 * // Create a validated transaction ID
 * const txId: TransactionId = createTransactionId('tx-abc123-def456');
 *
 * // Use in query options
 * const result = await client.query('SELECT * FROM users', [], {
 *   transactionId: txId,
 * });
 *
 * // Check if a transaction ID was created through the factory
 * if (isValidatedTransactionId(txId)) {
 *   console.log('Transaction ID is validated');
 * }
 * ```
 */
export type TransactionId = string & { readonly [TransactionIdBrand]: never };

/**
 * Branded type for Log Sequence Numbers
 *
 * LSN (Log Sequence Number) is a bigint branded type representing the position
 * in the write-ahead log. Used for time-travel queries, CDC, and replication.
 * Use `createLSN()` to create validated instances.
 *
 * @example
 * ```typescript
 * import { createLSN, LSN, compareLSN, incrementLSN, serializeLSN } from '@dotdo/sql-types';
 *
 * // Create a validated LSN
 * const lsn: LSN = createLSN(1000n);
 *
 * // Use for time-travel queries
 * const result = await client.query('SELECT * FROM users', [], {
 *   asOf: lsn,
 * });
 *
 * // Compare LSNs for ordering
 * const comparison = compareLSN(lsn, createLSN(2000n)); // returns -1
 *
 * // Increment LSN
 * const nextLsn = incrementLSN(lsn); // 1001n
 *
 * // Serialize for JSON transport
 * const serialized = serializeLSN(lsn); // "1000"
 * ```
 */
export type LSN = bigint & { readonly [LSNBrand]: never };

/**
 * Branded type for statement hashes
 *
 * StatementHash is a string branded type representing a unique hash of a SQL statement.
 * Used for prepared statement caching and query plan reuse.
 * Use `createStatementHash()` to create validated instances.
 *
 * @example
 * ```typescript
 * import { createStatementHash, StatementHash, PreparedStatement } from '@dotdo/sql-types';
 *
 * // Create a validated statement hash
 * const hash: StatementHash = createStatementHash('a1b2c3d4e5f6');
 *
 * // Use in prepared statement handle
 * const prepared: PreparedStatement = {
 *   sql: 'SELECT * FROM users WHERE id = ?',
 *   hash: hash,
 * };
 *
 * // Execute prepared statement with different parameters
 * const result = await client.execute(prepared.hash, [userId]);
 * ```
 */
export type StatementHash = string & { readonly [StatementHashBrand]: never };

/**
 * Branded type for shard identifiers
 *
 * ShardId is a string branded type identifying a specific database shard.
 * Used for routing queries to the correct shard in a distributed database.
 * Maximum length is 255 characters. Use `createShardId()` to create validated instances.
 *
 * @example
 * ```typescript
 * import { createShardId, ShardId, ShardInfo } from '@dotdo/sql-types';
 *
 * // Create a validated shard ID
 * const shardId: ShardId = createShardId('shard-us-east-001');
 *
 * // Use in query options to target a specific shard
 * const result = await client.query('SELECT * FROM orders', [], {
 *   shardId: shardId,
 * });
 *
 * // Shard information includes the branded ShardId
 * const info: ShardInfo = {
 *   shardId: shardId,
 *   keyRange: { min: 0, max: 1000000 },
 *   rowCount: 50000,
 * };
 * ```
 */
export type ShardId = string & { readonly [ShardIdBrand]: never };


// =============================================================================
// Validated Tracking (WeakSet for runtime-created branded types)
// =============================================================================

/**
 * Validation tracking for branded types.
 * @internal
 */

const validatedLSNs = new WeakSet<object>();
const validatedTransactionIds = new WeakSet<object>();
const validatedShardIds = new WeakSet<object>();
const validatedStatementHashes = new WeakSet<object>();

// Wrapper objects for primitive tracking with LRU support
// Using Map to maintain insertion order for FIFO, and manual tracking for LRU
const lsnWrappers = new Map<bigint, { value: bigint }>();
const stringWrappers = new Map<string, { value: string; type: 'txn' | 'shard' | 'hash' }>();

// LRU tracking: stores keys in order of last access (most recent at end)
const lsnLruOrder: bigint[] = [];
const stringLruOrder: string[] = [];

/**
 * Stats about wrapper Maps for monitoring.
 * @public
 * @stability stable
 */
export interface WrapperMapStats {
  stringWrappersSize: number;
  lsnWrappersSize: number;
}

/**
 * Get current wrapper Map sizes for monitoring.
 * @public
 * @stability stable
 */
export function getWrapperMapStats(): WrapperMapStats {
  return {
    stringWrappersSize: stringWrappers.size,
    lsnWrappersSize: lsnWrappers.size,
  };
}

/**
 * Clear all wrapper Maps (useful for tests and memory management).
 * @public
 * @stability stable
 */
export function clearWrapperMaps(): void {
  stringWrappers.clear();
  lsnWrappers.clear();
  stringLruOrder.length = 0;
  lsnLruOrder.length = 0;
}

// Helper function to perform LRU eviction on string wrappers
function evictStringWrappersIfNeeded(): void {
  if (!_getWrapperCacheConfigInternal().enabled) return;

  const maxSize = _getWrapperCacheConfigInternal().maxSize;
  while (stringWrappers.size >= maxSize && stringLruOrder.length > 0) {
    // Remove the least recently used (first in the array)
    const lruKey = stringLruOrder.shift();
    if (lruKey !== undefined) {
      stringWrappers.delete(lruKey);
    }
  }
}

// Helper function to perform LRU eviction on LSN wrappers
function evictLsnWrappersIfNeeded(): void {
  if (!_getWrapperCacheConfigInternal().enabled) return;

  const maxSize = _getWrapperCacheConfigInternal().maxSize;
  while (lsnWrappers.size >= maxSize && lsnLruOrder.length > 0) {
    // Remove the least recently used (first in the array)
    const lruKey = lsnLruOrder.shift();
    if (lruKey !== undefined) {
      lsnWrappers.delete(lruKey);
    }
  }
}

// Helper function to update LRU order for string keys
function touchStringLru(key: string): void {
  const index = stringLruOrder.indexOf(key);
  if (index !== -1) {
    // Remove from current position
    stringLruOrder.splice(index, 1);
  }
  // Add to end (most recently used)
  stringLruOrder.push(key);
}

// Helper function to update LRU order for LSN keys
function touchLsnLru(key: bigint): void {
  const index = lsnLruOrder.indexOf(key);
  if (index !== -1) {
    // Remove from current position
    lsnLruOrder.splice(index, 1);
  }
  // Add to end (most recently used)
  lsnLruOrder.push(key);
}

/**
 * Check if an LSN was created through the factory function (validated).
 * @public
 * @stability stable
 */
export function isValidatedLSN(lsn: LSN): boolean {
  const wrapper = lsnWrappers.get(lsn as bigint);
  if (wrapper !== undefined && validatedLSNs.has(wrapper)) {
    // Touch LRU on access
    touchLsnLru(lsn as bigint);
    return true;
  }
  return false;
}

// Helper to check if a string-based branded type was validated
function isValidatedString(
  value: string,
  expectedType: 'txn' | 'shard' | 'hash',
  validatedSet: WeakSet<object>
): boolean {
  const wrapper = stringWrappers.get(value);
  if (wrapper !== undefined && wrapper.type === expectedType && validatedSet.has(wrapper)) {
    // Touch LRU on access
    touchStringLru(value);
    return true;
  }
  return false;
}

/**
 * Check if a TransactionId was created through the factory function (validated).
 * @public
 * @stability stable
 */
export function isValidatedTransactionId(txnId: TransactionId): boolean {
  return isValidatedString(txnId as string, 'txn', validatedTransactionIds);
}

/**
 * Check if a ShardId was created through the factory function (validated).
 * @public
 * @stability stable
 */
export function isValidatedShardId(shardId: ShardId): boolean {
  return isValidatedString(shardId as string, 'shard', validatedShardIds);
}

/**
 * Check if a StatementHash was created through the factory function (validated).
 * @public
 * @stability stable
 */
export function isValidatedStatementHash(hash: StatementHash): boolean {
  return isValidatedString(hash as string, 'hash', validatedStatementHashes);
}

// =============================================================================
// Type Guard Functions
// =============================================================================

/**
 * Check if a value is a valid LSN (bigint >= 0).
 * @template T - The input type (defaults to unknown)
 * @public
 * @stability stable
 */
export function isValidLSN<T>(value: T | unknown): value is T extends bigint ? LSN : LSN {
  return typeof value === 'bigint' && value >= 0n;
}

/**
 * Check if a value is a valid TransactionId (non-empty string).
 * @template T - The input type (defaults to unknown)
 * @public
 * @stability stable
 */
export function isValidTransactionId<T>(value: T | unknown): value is T extends string ? TransactionId : TransactionId {
  return typeof value === 'string' && value.trim().length > 0;
}

/**
 * Check if a value is a valid ShardId (non-empty string, max 255 chars).
 * @template T - The input type (defaults to unknown)
 * @public
 * @stability stable
 */
export function isValidShardId<T>(value: T | unknown): value is T extends string ? ShardId : ShardId {
  return typeof value === 'string' && value.trim().length > 0 && value.length <= 255;
}

/**
 * Check if a value is a valid StatementHash (non-empty string).
 * @template T - The input type (defaults to unknown)
 * @public
 * @stability stable
 */
export function isValidStatementHash<T>(value: T | unknown): value is T extends string ? StatementHash : StatementHash {
  return typeof value === 'string' && value.length > 0;
}

// =============================================================================
// Factory Functions with Validation
// =============================================================================

/**
 * Create a typed TransactionId from a string.
 * @throws Error if id is empty or whitespace-only (in dev mode)
 * @public
 * @stability stable
 */
export function createTransactionId(id: string): TransactionId {
  if (_isDevModeInternal() || _isStrictModeInternal()) {
    if (typeof id !== 'string') {
      throw new Error('TransactionId must be a string');
    }
    if (id.trim().length === 0) {
      throw new Error('TransactionId cannot be empty');
    }
  }

  // Track as validated (if caching is enabled)
  if (_getWrapperCacheConfigInternal().enabled) {
    // Check if already exists (reuse)
    const existing = stringWrappers.get(id);
    if (existing && existing.type === 'txn') {
      touchStringLru(id);
      return id as TransactionId;
    }

    // Evict if needed before adding
    evictStringWrappersIfNeeded();

    const wrapper = { value: id, type: 'txn' as const };
    stringWrappers.set(id, wrapper);
    validatedTransactionIds.add(wrapper);
    touchStringLru(id);
  }

  return id as TransactionId;
}

/**
 * Create a typed LSN from a bigint.
 * @throws Error if lsn is negative or not a bigint (in dev mode)
 * @public
 * @stability stable
 */
export function createLSN(lsn: bigint): LSN {
  if (_isDevModeInternal() || _isStrictModeInternal()) {
    if (typeof lsn !== 'bigint') {
      throw new Error('LSN must be a bigint');
    }
    if (lsn < 0n) {
      throw new Error(`LSN cannot be negative: ${lsn}`);
    }
  }

  // Track as validated (if caching is enabled)
  if (_getWrapperCacheConfigInternal().enabled) {
    // Check if already exists (reuse)
    const existing = lsnWrappers.get(lsn);
    if (existing) {
      touchLsnLru(lsn);
      return lsn as LSN;
    }

    // Evict if needed before adding
    evictLsnWrappersIfNeeded();

    const wrapper = { value: lsn };
    lsnWrappers.set(lsn, wrapper);
    validatedLSNs.add(wrapper);
    touchLsnLru(lsn);
  }

  return lsn as LSN;
}

/**
 * Create a typed StatementHash from a string.
 * @throws Error if hash is empty (in dev mode)
 * @public
 * @stability stable
 */
export function createStatementHash(hash: string): StatementHash {
  if (_isDevModeInternal() || _isStrictModeInternal()) {
    if (typeof hash !== 'string') {
      throw new Error('StatementHash must be a string');
    }
    if (hash.length === 0) {
      throw new Error('StatementHash cannot be empty');
    }
    if (_isStrictModeInternal() && !/^[a-f0-9]+$/i.test(hash)) {
      throw new Error('Invalid StatementHash format');
    }
  }

  // Track as validated (if caching is enabled)
  if (_getWrapperCacheConfigInternal().enabled) {
    // Check if already exists (reuse)
    const existing = stringWrappers.get(hash);
    if (existing && existing.type === 'hash') {
      touchStringLru(hash);
      return hash as StatementHash;
    }

    // Evict if needed before adding
    evictStringWrappersIfNeeded();

    const wrapper = { value: hash, type: 'hash' as const };
    stringWrappers.set(hash, wrapper);
    validatedStatementHashes.add(wrapper);
    touchStringLru(hash);
  }

  return hash as StatementHash;
}

/**
 * Create a typed ShardId from a string.
 * @throws Error if id is empty, whitespace-only, or exceeds max length (in dev mode)
 * @public
 * @stability stable
 */
export function createShardId(id: string): ShardId {
  if (_isDevModeInternal() || _isStrictModeInternal()) {
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

  // Track as validated (if caching is enabled)
  if (_getWrapperCacheConfigInternal().enabled) {
    // Check if already exists (reuse)
    const existing = stringWrappers.get(id);
    if (existing && existing.type === 'shard') {
      touchStringLru(id);
      return id as ShardId;
    }

    // Evict if needed before adding
    evictStringWrappersIfNeeded();

    const wrapper = { value: id, type: 'shard' as const };
    stringWrappers.set(id, wrapper);
    validatedShardIds.add(wrapper);
    touchStringLru(id);
  }

  return id as ShardId;
}

// =============================================================================
// LSN Serialization / Deserialization
// =============================================================================

/**
 * Serialize an LSN to a string for JSON-safe transport.
 * @public
 * @stability stable
 */
export function serializeLSN(lsn: LSN): string {
  return String(lsn);
}

/**
 * Deserialize an LSN from a string.
 * @throws Error if string cannot be parsed as a valid LSN
 * @public
 * @stability stable
 */
export function deserializeLSN(str: string): LSN {
  const value = BigInt(str);
  return createLSN(value);
}

/**
 * Convert an LSN to a number (only safe for values within Number.MAX_SAFE_INTEGER).
 * @throws Error if LSN exceeds safe integer range
 * @public
 * @stability stable
 */
export function lsnToNumber(lsn: LSN): number {
  if (lsn > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error('LSN exceeds safe integer range');
  }
  return Number(lsn);
}

/**
 * Convert an LSN to a Uint8Array (8 bytes, big-endian).
 * @public
 * @stability stable
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
 * Convert a Uint8Array (8 bytes, big-endian) to an LSN.
 * @public
 * @stability stable
 */
export function bytesToLSN(bytes: Uint8Array): LSN {
  if (bytes.length !== 8) {
    throw new Error('LSN bytes must be exactly 8 bytes');
  }
  let value = 0n;
  for (let i = 0; i < 8; i++) {
    const byte = bytes[i];
    if (byte === undefined) {
      throw new Error('LSN bytes must be exactly 8 bytes');
    }
    value = (value << 8n) | BigInt(byte);
  }
  return createLSN(value);
}

// =============================================================================
// LSN Utility Functions
// =============================================================================

/**
 * Compare two LSNs.
 * @returns negative if a < b, 0 if equal, positive if a > b
 * @public
 * @stability stable
 */
export function compareLSN(a: LSN, b: LSN): number {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

/**
 * Increment an LSN by a given amount (default 1).
 * @public
 * @stability stable
 */
export function incrementLSN(lsn: LSN, amount: bigint = 1n): LSN {
  return createLSN((lsn as bigint) + amount);
}

/**
 * Extract the raw bigint value from an LSN.
 * @public
 * @stability stable
 */
export function lsnValue(lsn: LSN): bigint {
  return lsn as bigint;
}

// =============================================================================
// SQL Value Types
// =============================================================================

/**
 * Represents valid SQL values that can be passed as parameters or returned in results.
 * @public
 * @stability stable
 */
export type SQLValue = string | number | boolean | null | Uint8Array | bigint;

// =============================================================================
// Column Types
// =============================================================================

/**
 * Unified column type covering both SQL and JavaScript type representations.
 * @public
 * @stability stable
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
 * SQL-style column types (client-facing).
 * @public
 * @stability stable
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
 * JavaScript-style column types (wire format).
 * @public
 * @stability stable
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
 * Mapping from SQL types to JS types.
 * @public
 * @stability stable
 */
export const SQL_TO_JS_TYPE_MAP: Readonly<Record<SQLColumnType, JSColumnType>> = {
  INTEGER: 'number',
  REAL: 'number',
  TEXT: 'string',
  BLOB: 'blob',
  NULL: 'null',
  BOOLEAN: 'boolean',
  DATETIME: 'timestamp',
  JSON: 'json',
} as const;

/**
 * Mapping from JS types to SQL types.
 * @public
 * @stability stable
 */
export const JS_TO_SQL_TYPE_MAP: Readonly<Record<JSColumnType, SQLColumnType>> = {
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
} as const;

/**
 * Convert SQL column type to JS column type.
 * @public
 * @stability stable
 */
export function sqlToJsType(sqlType: SQLColumnType): JSColumnType {
  return SQL_TO_JS_TYPE_MAP[sqlType];
}

/**
 * Convert JS column type to SQL column type.
 * @public
 * @stability stable
 */
export function jsToSqlType(jsType: JSColumnType): SQLColumnType {
  return JS_TO_SQL_TYPE_MAP[jsType];
}

// =============================================================================
// Idempotency Types
// =============================================================================

/**
 * Configuration for idempotency key generation and behavior.
 * @public
 * @stability stable
 */
export interface IdempotencyConfig {
  /** Whether to automatically generate idempotency keys for mutations */
  enabled: boolean;
  /** Custom key prefix (default: none) */
  keyPrefix?: string;
  /** Time-to-live for idempotency keys in milliseconds (server-side) */
  ttlMs?: number;
  /** Maximum number of entries in the client-side idempotency key cache (default: 1000) */
  maxCacheSize?: number;
  /** Time-to-live for cached idempotency keys in milliseconds (client-side, default: 5 minutes) */
  cacheTtlMs?: number;
  /** Interval in milliseconds for periodic cache cleanup (default: 60000 = 1 minute) */
  cleanupIntervalMs?: number;
}

/**
 * Default idempotency configuration.
 * @public
 * @stability stable
 */
export const DEFAULT_IDEMPOTENCY_CONFIG: Readonly<IdempotencyConfig> = {
  enabled: true,
  ttlMs: 24 * 60 * 60 * 1000, // 24 hours (server-side)
  maxCacheSize: 1000,
  cacheTtlMs: 5 * 60 * 1000, // 5 minutes (client-side cache)
  cleanupIntervalMs: 60 * 1000, // 1 minute
} as const;

// =============================================================================
// Query Request Types
// =============================================================================

/**
 * Unified query request that supports all client and server features.
 *
 * @public
 * @stability stable
 *
 * @example Basic SELECT query
 * ```typescript
 * const selectRequest: QueryRequest = {
 *   sql: 'SELECT id, name, email FROM users WHERE active = ?',
 *   params: [true],
 *   branch: 'main',
 * };
 * ```
 *
 * @example INSERT with named parameters
 * ```typescript
 * const insertRequest: QueryRequest = {
 *   sql: 'INSERT INTO users (name, email) VALUES (:name, :email)',
 *   namedParams: { name: 'Alice', email: 'alice@example.com' },
 *   idempotencyKey: 'user-create-alice-20240115',
 * };
 * ```
 *
 * @example Paginated query with time travel
 * ```typescript
 * const paginatedRequest: QueryRequest = {
 *   sql: 'SELECT * FROM orders WHERE status = ?',
 *   params: ['pending'],
 *   limit: 50,
 *   offset: 100,
 *   asOf: 1000n, // Read from LSN 1000
 * };
 * ```
 *
 * @example Transactional query with timeout
 * ```typescript
 * const txRequest: QueryRequest = {
 *   sql: 'UPDATE accounts SET balance = balance - ? WHERE id = ?',
 *   params: [100, 42],
 *   transactionId: 'tx-abc123',
 *   timeoutMs: 5000,
 * };
 * ```
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
  timeoutMs?: number;
  /**
   * @deprecated Use `timeoutMs` instead for consistency with other timeout fields
   */
  timeout?: number;
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
 * Client-facing query options (subset of QueryRequest).
 * @public
 * @stability stable
 */
export interface QueryOptions {
  /** Transaction ID for transactional queries */
  transactionId?: TransactionId;
  /** Read from a specific point in time */
  asOf?: Date | LSN;
  /** Timeout in milliseconds */
  timeoutMs?: number;
  /**
   * @deprecated Use `timeoutMs` instead for consistency with other timeout fields
   */
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
 * Unified query response that supports all features.
 *
 * @public
 * @stability stable
 *
 * @example SELECT query response
 * ```typescript
 * const selectResponse: QueryResponse<{ id: number; name: string; email: string }> = {
 *   columns: ['id', 'name', 'email'],
 *   columnTypes: ['INTEGER', 'TEXT', 'TEXT'],
 *   rows: [
 *     { id: 1, name: 'Alice', email: 'alice@example.com' },
 *     { id: 2, name: 'Bob', email: 'bob@example.com' },
 *   ],
 *   rowCount: 2,
 *   executionTimeMs: 12,
 *   lsn: 42n,
 * };
 * ```
 *
 * @example INSERT mutation response
 * ```typescript
 * const insertResponse: QueryResponse = {
 *   columns: [],
 *   rows: [],
 *   rowCount: 0,
 *   rowsAffected: 1,
 *   lastInsertRowid: 5n,
 *   executionTimeMs: 8,
 *   lsn: 43n,
 * };
 * ```
 *
 * @example UPDATE mutation response
 * ```typescript
 * const updateResponse: QueryResponse = {
 *   columns: [],
 *   rows: [],
 *   rowCount: 0,
 *   rowsAffected: 3,
 *   executionTimeMs: 15,
 *   lsn: 44n,
 * };
 * ```
 *
 * @example Paginated query response
 * ```typescript
 * const paginatedResponse: QueryResponse<{ id: number; title: string }> = {
 *   columns: ['id', 'title'],
 *   rows: [
 *     { id: 1, title: 'First Post' },
 *     { id: 2, title: 'Second Post' },
 *   ],
 *   rowCount: 2,
 *   hasMore: true,
 *   cursor: 'eyJpZCI6Mn0=',
 *   executionTimeMs: 5,
 * };
 * ```
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
 * Client-facing query result (for backward compatibility).
 *
 * @public
 * @stability stable
 *
 * @example SELECT query result
 * ```typescript
 * const selectResult: QueryResult<{ id: number; name: string; active: boolean }> = {
 *   columns: ['id', 'name', 'active'],
 *   columnTypes: ['INTEGER', 'TEXT', 'BOOLEAN'],
 *   rows: [
 *     { id: 1, name: 'Alice', active: true },
 *     { id: 2, name: 'Bob', active: false },
 *   ],
 *   rowsAffected: 0,
 *   duration: 8,
 *   lsn: 100n as LSN,
 * };
 * ```
 *
 * @example INSERT mutation result
 * ```typescript
 * const insertResult: QueryResult = {
 *   columns: [],
 *   rows: [],
 *   rowsAffected: 1,
 *   lastInsertRowid: 42n,
 *   duration: 5,
 *   lsn: 101n as LSN,
 * };
 * ```
 *
 * @example DELETE mutation result
 * ```typescript
 * const deleteResult: QueryResult = {
 *   columns: [],
 *   rows: [],
 *   rowsAffected: 5,
 *   duration: 12,
 *   lsn: 102n as LSN,
 * };
 * ```
 *
 * @example Paginated result with cursor
 * ```typescript
 * const paginatedResult: QueryResult<{ id: number; created_at: string }> = {
 *   columns: ['id', 'created_at'],
 *   rows: [
 *     { id: 10, created_at: '2024-01-15T10:30:00Z' },
 *     { id: 11, created_at: '2024-01-15T11:00:00Z' },
 *   ],
 *   rowsAffected: 0,
 *   duration: 3,
 *   hasMore: true,
 *   cursor: 'eyJpZCI6MTF9',
 * };
 * ```
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
 * Unified CDC operation types (includes TRUNCATE for server-side operations).
 * @public
 * @stability experimental
 */
export type CDCOperation = 'INSERT' | 'UPDATE' | 'DELETE' | 'TRUNCATE';

/**
 * CDC operation types for client-side (excludes TRUNCATE).
 * @public
 * @stability experimental
 */
export type ClientCDCOperation = 'INSERT' | 'UPDATE' | 'DELETE';

/**
 * Numeric operation codes for efficient binary encoding.
 * @public
 * @stability experimental
 */
export const CDCOperationCode = {
  INSERT: 0,
  UPDATE: 1,
  DELETE: 2,
  TRUNCATE: 3,
} as const;

/**
 * CDC operation code value type.
 * @public
 * @stability experimental
 */
export type CDCOperationCodeValue = (typeof CDCOperationCode)[CDCOperation];

/**
 * Unified CDC event that covers all package requirements.
 *
 * Represents a Change Data Capture event for tracking database modifications.
 * Used for replication, audit logging, and real-time data synchronization.
 *
 * @public
 * @stability experimental
 *
 * @example INSERT event - new row created
 * ```typescript
 * const insertEvent: CDCEvent<{ id: number; name: string; email: string }> = {
 *   lsn: 1001n,
 *   table: 'users',
 *   operation: 'INSERT',
 *   timestamp: new Date('2024-01-15T10:30:00Z'),
 *   transactionId: 'tx-abc123',
 *   primaryKey: { id: 42 },
 *   after: { id: 42, name: 'Alice', email: 'alice@example.com' },
 * };
 * ```
 *
 * @example UPDATE event - existing row modified
 * ```typescript
 * const updateEvent: CDCEvent<{ id: number; name: string; email: string }> = {
 *   lsn: 1002n,
 *   table: 'users',
 *   operation: 'UPDATE',
 *   timestamp: new Date('2024-01-15T11:00:00Z'),
 *   transactionId: 'tx-def456',
 *   primaryKey: { id: 42 },
 *   before: { id: 42, name: 'Alice', email: 'alice@example.com' },
 *   after: { id: 42, name: 'Alice Smith', email: 'alice.smith@example.com' },
 * };
 * ```
 *
 * @example DELETE event - row removed
 * ```typescript
 * const deleteEvent: CDCEvent<{ id: number; name: string }> = {
 *   lsn: 1003n,
 *   table: 'users',
 *   operation: 'DELETE',
 *   timestamp: 1705320000000, // Unix timestamp format
 *   txId: 'tx-ghi789', // Server-side format uses txId
 *   primaryKey: { id: 42 },
 *   before: { id: 42, name: 'Alice Smith' },
 * };
 * ```
 *
 * @example TRUNCATE event - table cleared (server-side only)
 * ```typescript
 * const truncateEvent: CDCEvent = {
 *   lsn: 1004n,
 *   table: 'temp_logs',
 *   operation: 'TRUNCATE',
 *   timestamp: new Date(),
 *   metadata: { reason: 'scheduled_cleanup' },
 * };
 * ```
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
 * Unified isolation level that covers all supported levels.
 * @public
 * @stability stable
 */
export type IsolationLevel =
  | 'READ_UNCOMMITTED'
  | 'READ_COMMITTED'
  | 'REPEATABLE_READ'
  | 'SERIALIZABLE'
  | 'SNAPSHOT';

/**
 * Server-supported isolation levels.
 * @public
 * @stability stable
 */
export type ServerIsolationLevel =
  | 'READ_COMMITTED'
  | 'REPEATABLE_READ'
  | 'SERIALIZABLE';

/**
 * Transaction options for beginning a transaction.
 *
 * @public
 * @stability stable
 *
 * @example
 * ```typescript
 * // Basic transaction with default options
 * const basicOptions: TransactionOptions = {};
 *
 * // Read-only transaction with SNAPSHOT isolation
 * const readOnlyOptions: TransactionOptions = {
 *   isolationLevel: 'SNAPSHOT',
 *   readOnly: true,
 *   timeoutMs: 30000, // 30 second timeout
 * };
 *
 * // Serializable transaction for critical operations
 * const serializableOptions: TransactionOptions = {
 *   isolationLevel: 'SERIALIZABLE',
 *   readOnly: false,
 *   timeoutMs: 60000,
 *   branch: 'production',
 * };
 *
 * // Usage with a transaction begin call
 * const txHandle = await client.beginTransaction({
 *   isolationLevel: 'REPEATABLE_READ',
 *   timeoutMs: 10000,
 * });
 * ```
 */
export interface TransactionOptions {
  isolationLevel?: IsolationLevel;
  readOnly?: boolean;
  /** Transaction timeout in milliseconds */
  timeoutMs?: number;
  /**
   * @deprecated Use `timeoutMs` instead for consistency with other timeout fields
   */
  timeout?: number;
  branch?: string;
}

/**
 * Transaction state information.
 *
 * Represents the current state of an active transaction, including its
 * isolation level, read-only status, and the LSN snapshot it's operating on.
 *
 * @public
 * @stability stable
 *
 * @example
 * ```typescript
 * // Inspect active transaction state
 * const state: TransactionState = {
 *   id: createTransactionId('tx-abc123'),
 *   isolationLevel: 'SERIALIZABLE',
 *   readOnly: false,
 *   startedAt: new Date(),
 *   snapshotLSN: createLSN(1000n),
 * };
 *
 * // Check transaction properties
 * if (state.readOnly) {
 *   console.log('Transaction is read-only, no writes allowed');
 * }
 *
 * // Use snapshotLSN for consistent reads
 * console.log(`Reading from snapshot at LSN: ${state.snapshotLSN}`);
 *
 * // Track transaction duration
 * const durationMs = Date.now() - state.startedAt.getTime();
 * if (durationMs > 30000) {
 *   console.warn('Long-running transaction detected');
 * }
 * ```
 */
export interface TransactionState {
  id: TransactionId;
  isolationLevel: IsolationLevel;
  readOnly: boolean;
  startedAt: Date;
  snapshotLSN: LSN;
}

/**
 * Transaction handle returned after begin.
 *
 * Provides the essential identifiers and timing information needed to
 * execute queries within a transaction and manage its lifecycle.
 *
 * @public
 * @stability stable
 *
 * @example
 * ```typescript
 * // Begin a transaction and get the handle
 * const handle: TransactionHandle = await client.beginTransaction({
 *   isolationLevel: 'REPEATABLE_READ',
 *   timeoutMs: 30000,
 * });
 *
 * console.log(`Transaction started: ${handle.txId}`);
 * console.log(`Snapshot LSN: ${handle.startLSN}`);
 *
 * // Check if transaction is still valid before executing
 * if (Date.now() < handle.expiresAt) {
 *   // Execute queries within the transaction
 *   await client.query('INSERT INTO users (name) VALUES (?)', ['Alice'], {
 *     transactionId: handle.txId as TransactionId,
 *   });
 *
 *   // Commit the transaction
 *   await client.commit(handle.txId);
 * } else {
 *   console.error('Transaction expired, rolling back');
 *   await client.rollback(handle.txId);
 * }
 *
 * // Full transaction lifecycle example
 * try {
 *   const tx = await client.beginTransaction({ isolationLevel: 'SERIALIZABLE' });
 *   await client.query('UPDATE accounts SET balance = balance - 100 WHERE id = 1', [], { transactionId: tx.txId as TransactionId });
 *   await client.query('UPDATE accounts SET balance = balance + 100 WHERE id = 2', [], { transactionId: tx.txId as TransactionId });
 *   await client.commit(tx.txId);
 * } catch (error) {
 *   await client.rollback(tx.txId);
 *   throw error;
 * }
 * ```
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
 * RPC method names.
 * @public
 * @stability stable
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
 * RPC request envelope.
 *
 * Wraps method calls with a unique identifier for request/response correlation.
 * The `id` field allows clients to match responses to their corresponding requests,
 * especially important for concurrent operations over a single connection.
 *
 * @public
 * @stability stable
 *
 * @example Query request - fetching data
 * ```typescript
 * const queryRequest: RPCRequest = {
 *   id: 'req-001',
 *   method: 'query',
 *   params: {
 *     sql: 'SELECT id, name, email FROM users WHERE active = ?',
 *     params: [true],
 *     branch: 'main',
 *   },
 * };
 * ```
 *
 * @example Mutation request - inserting data
 * ```typescript
 * const insertRequest: RPCRequest = {
 *   id: 'req-002',
 *   method: 'exec',
 *   params: {
 *     sql: 'INSERT INTO users (name, email) VALUES (?, ?)',
 *     params: ['Alice', 'alice@example.com'],
 *     idempotencyKey: 'user-create-alice-20240115',
 *   },
 * };
 * ```
 *
 * @example Transaction request - beginning a transaction
 * ```typescript
 * const beginTxRequest: RPCRequest = {
 *   id: 'req-003',
 *   method: 'beginTransaction',
 *   params: {
 *     isolationLevel: 'SERIALIZABLE',
 *     readOnly: false,
 *     timeoutMs: 30000,
 *   },
 * };
 * ```
 */
export interface RPCRequest {
  id: string;
  method: RPCMethod;
  params: unknown;
}

/**
 * RPC response envelope.
 *
 * Contains the result of an RPC method call, matched to the original request via `id`.
 * Either `result` (on success) or `error` (on failure) will be present, but not both.
 *
 * @public
 * @stability stable
 *
 * @example Successful query response
 * ```typescript
 * const queryResponse: RPCResponse<QueryResponse> = {
 *   id: 'req-001',
 *   result: {
 *     columns: ['id', 'name', 'email'],
 *     rows: [
 *       { id: 1, name: 'Alice', email: 'alice@example.com' },
 *       { id: 2, name: 'Bob', email: 'bob@example.com' },
 *     ],
 *     rowCount: 2,
 *     executionTimeMs: 5,
 *   },
 * };
 * ```
 *
 * @example Successful mutation response
 * ```typescript
 * const insertResponse: RPCResponse<QueryResponse> = {
 *   id: 'req-002',
 *   result: {
 *     columns: [],
 *     rows: [],
 *     rowCount: 0,
 *     rowsAffected: 1,
 *     lastInsertRowid: 42n,
 *     lsn: 12345n,
 *     executionTimeMs: 3,
 *   },
 * };
 * ```
 *
 * @example Error response
 * ```typescript
 * const errorResponse: RPCResponse = {
 *   id: 'req-004',
 *   error: {
 *     code: RPCErrorCode.SYNTAX_ERROR,
 *     message: 'Syntax error near "SELEC"',
 *     details: { position: 0, suggestion: 'Did you mean SELECT?' },
 *   },
 * };
 * ```
 */
export interface RPCResponse<T = unknown> {
  id: string;
  result?: T;
  error?: RPCError;
}

/**
 * Unified RPC error codes.
 * @public
 * @stability stable
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
 * Unified RPC error structure.
 * @public
 * @stability stable
 */
export interface RPCError<TDetails extends Record<string, unknown> = Record<string, unknown>> {
  /** Error code (string or enum) */
  code: string | RPCErrorCode;
  /** Human-readable message */
  message: string;
  /** Additional error details */
  details?: TDetails;
  /** Stack trace (in development) */
  stack?: string;
  /** Suggestion for error recovery (e.g., "Did you mean SELECT?" for typos) */
  suggestion?: string;
}

// =============================================================================
// Client Capabilities
// =============================================================================

/**
 * Capabilities advertised by the client during connection.
 * @public
 * @stability experimental
 */
export interface ClientCapabilities {
  binaryProtocol: boolean;
  compression: boolean;
  batching: boolean;
  maxBatchSize: number;
  maxMessageSize: number;
}

/**
 * Default client capabilities.
 * @public
 * @stability experimental
 */
export const DEFAULT_CLIENT_CAPABILITIES: Readonly<ClientCapabilities> = {
  binaryProtocol: true,
  compression: false,
  batching: true,
  maxBatchSize: 1000,
  maxMessageSize: 4 * 1024 * 1024,
} as const;

// =============================================================================
// Schema Types
// =============================================================================

/**
 * Column definition in a table schema.
 *
 * Describes a single column's structure, type, and constraints within a table.
 *
 * @public
 * @stability stable
 *
 * @example
 * ```typescript
 * // Auto-incrementing primary key column
 * const idColumn: ColumnDefinition = {
 *   name: 'id',
 *   type: 'INTEGER',
 *   nullable: false,
 *   primaryKey: true,
 *   autoIncrement: true,
 * };
 *
 * // Nullable email column with unique constraint
 * const emailColumn: ColumnDefinition = {
 *   name: 'email',
 *   type: 'TEXT',
 *   nullable: true,
 *   primaryKey: false,
 *   unique: true,
 *   defaultValue: null,
 *   doc: 'User email address for notifications',
 * };
 *
 * // JSON column for storing structured metadata
 * const metadataColumn: ColumnDefinition = {
 *   name: 'metadata',
 *   type: 'JSON',
 *   nullable: false,
 *   primaryKey: false,
 *   defaultValue: '{}',
 * };
 *
 * // Timestamp column with default value
 * const createdAtColumn: ColumnDefinition = {
 *   name: 'created_at',
 *   type: 'DATETIME',
 *   nullable: false,
 *   primaryKey: false,
 *   defaultValue: 'CURRENT_TIMESTAMP',
 * };
 * ```
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
 * Index definition for a table.
 *
 * Defines an index on one or more columns to optimize query performance.
 *
 * @public
 * @stability stable
 *
 * @example
 * ```typescript
 * // Simple single-column index for fast lookups
 * const emailIndex: IndexDefinition = {
 *   name: 'idx_users_email',
 *   columns: ['email'],
 *   unique: true,
 * };
 *
 * // Composite index for multi-column queries
 * const compositeIndex: IndexDefinition = {
 *   name: 'idx_orders_user_date',
 *   columns: ['user_id', 'created_at'],
 *   unique: false,
 *   type: 'BTREE',
 * };
 *
 * // Hash index for equality lookups
 * const hashIndex: IndexDefinition = {
 *   name: 'idx_sessions_token',
 *   columns: ['session_token'],
 *   unique: true,
 *   type: 'HASH',
 * };
 *
 * // GIN index for full-text search on JSON columns
 * const jsonIndex: IndexDefinition = {
 *   name: 'idx_products_tags',
 *   columns: ['tags'],
 *   unique: false,
 *   type: 'GIN',
 * };
 * ```
 */
export interface IndexDefinition {
  name: string;
  columns: string[];
  unique: boolean;
  type?: 'BTREE' | 'HASH' | 'GIN' | 'GIST';
}

/**
 * Foreign key definition.
 * @public
 * @stability stable
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
 * Table schema definition.
 *
 * Complete definition of a database table including columns, primary key,
 * indexes, and foreign key relationships.
 *
 * @public
 * @stability stable
 *
 * @example
 * ```typescript
 * // Simple users table with basic columns
 * const usersTable: TableSchema = {
 *   name: 'users',
 *   columns: [
 *     { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, autoIncrement: true },
 *     { name: 'email', type: 'TEXT', nullable: false, primaryKey: false, unique: true },
 *     { name: 'name', type: 'TEXT', nullable: true, primaryKey: false },
 *     { name: 'created_at', type: 'DATETIME', nullable: false, primaryKey: false, defaultValue: 'CURRENT_TIMESTAMP' },
 *   ],
 *   primaryKey: ['id'],
 *   indexes: [
 *     { name: 'idx_users_email', columns: ['email'], unique: true },
 *   ],
 * };
 *
 * // Orders table with foreign key to users
 * const ordersTable: TableSchema = {
 *   name: 'orders',
 *   columns: [
 *     { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, autoIncrement: true },
 *     { name: 'user_id', type: 'INTEGER', nullable: false, primaryKey: false },
 *     { name: 'total', type: 'REAL', nullable: false, primaryKey: false },
 *     { name: 'status', type: 'TEXT', nullable: false, primaryKey: false, defaultValue: 'pending' },
 *     { name: 'created_at', type: 'DATETIME', nullable: false, primaryKey: false },
 *   ],
 *   primaryKey: ['id'],
 *   indexes: [
 *     { name: 'idx_orders_user', columns: ['user_id'], unique: false },
 *     { name: 'idx_orders_status_date', columns: ['status', 'created_at'], unique: false },
 *   ],
 *   foreignKeys: [
 *     {
 *       name: 'fk_orders_user',
 *       columns: ['user_id'],
 *       referencedTable: 'users',
 *       referencedColumns: ['id'],
 *       onDelete: 'CASCADE',
 *       onUpdate: 'CASCADE',
 *     },
 *   ],
 * };
 *
 * // Junction table for many-to-many relationship
 * const userRolesTable: TableSchema = {
 *   name: 'user_roles',
 *   columns: [
 *     { name: 'user_id', type: 'INTEGER', nullable: false, primaryKey: true },
 *     { name: 'role_id', type: 'INTEGER', nullable: false, primaryKey: true },
 *     { name: 'granted_at', type: 'DATETIME', nullable: false, primaryKey: false },
 *   ],
 *   primaryKey: ['user_id', 'role_id'],
 *   foreignKeys: [
 *     { name: 'fk_user_roles_user', columns: ['user_id'], referencedTable: 'users', referencedColumns: ['id'], onDelete: 'CASCADE' },
 *     { name: 'fk_user_roles_role', columns: ['role_id'], referencedTable: 'roles', referencedColumns: ['id'], onDelete: 'CASCADE' },
 *   ],
 * };
 * ```
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
 * Shard configuration.
 * @public
 * @stability experimental
 */
export interface ShardConfig {
  shardCount: number;
  shardKey: string;
  shardFunction: 'hash' | 'range' | 'list';
}

/**
 * Shard information.
 * @public
 * @stability experimental
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
 * Connection options for clients.
 *
 * Configures how a client connects to and maintains a connection with a DoSQL server.
 * Supports both WebSocket and HTTP transports with automatic reconnection capabilities.
 *
 * @public
 * @stability experimental
 *
 * @example
 * ```typescript
 * // Basic connection to a local server
 * const basicOptions: ConnectionOptions = {
 *   url: 'wss://sql.do/v1/ws',
 * };
 *
 * // Production connection with full configuration
 * const productionOptions: ConnectionOptions = {
 *   url: 'wss://api.sql.do/v1/ws',
 *   defaultBranch: 'main',
 *   connectTimeoutMs: 5000,
 *   queryTimeoutMs: 30000,
 *   autoReconnect: true,
 *   maxReconnectAttempts: 5,
 *   reconnectDelayMs: 1000,
 * };
 *
 * // Multi-tenant connection with branch isolation
 * const tenantOptions: ConnectionOptions = {
 *   url: 'wss://sql.do/v1/ws',
 *   defaultBranch: 'tenant-acme-corp',
 *   autoReconnect: true,
 *   maxReconnectAttempts: 10,
 * };
 * ```
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
 * Connection statistics.
 *
 * Provides real-time metrics and state information about a database connection.
 * Useful for monitoring connection health, debugging connectivity issues, and
 * tracking message throughput.
 *
 * @public
 * @stability experimental
 *
 * @example
 * ```typescript
 * // Check connection health
 * function checkConnectionHealth(stats: ConnectionStats): void {
 *   if (!stats.connected) {
 *     console.warn(`Disconnected. Reconnect attempts: ${stats.reconnectCount}`);
 *     return;
 *   }
 *
 *   console.log(`Connected to branch: ${stats.branch ?? 'default'}`);
 *   console.log(`Connection ID: ${stats.connectionId}`);
 *   console.log(`Current LSN: ${stats.currentLSN}`);
 *   console.log(`Latency: ${stats.latencyMs}ms`);
 * }
 *
 * // Monitor message throughput
 * function logThroughput(stats: ConnectionStats): void {
 *   console.log(`Messages sent: ${stats.messagesSent}`);
 *   console.log(`Messages received: ${stats.messagesReceived}`);
 * }
 *
 * // Example stats from an active connection
 * const exampleStats: ConnectionStats = {
 *   connected: true,
 *   connectionId: 'conn-abc123',
 *   branch: 'production',
 *   currentLSN: 1042n,
 *   latencyMs: 12,
 *   messagesSent: 156,
 *   messagesReceived: 142,
 *   reconnectCount: 0,
 * };
 * ```
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
 * Prepared statement handle.
 * @public
 * @stability stable
 */
export interface PreparedStatement {
  sql: string;
  hash: StatementHash;
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if a CDC event is from the server (has txId).
 * @param event - The CDC event to check
 * @returns True if the event has a txId property with a string value, indicating it originated from the server
 * @public
 * @stability experimental
 */
export function isServerCDCEvent(event: CDCEvent): boolean {
  return 'txId' in event && typeof event.txId === 'string';
}

/**
 * Check if a CDC event is from the client (has transactionId).
 * @param event - The CDC event to check
 * @returns True if the event has a defined transactionId property, indicating it originated from the client
 * @public
 * @stability experimental
 */
export function isClientCDCEvent(event: CDCEvent): boolean {
  return 'transactionId' in event && event.transactionId !== undefined;
}

/**
 * Check if timestamp is a Date.
 * @public
 * @stability stable
 */
export function isDateTimestamp(
  timestamp: Date | number
): timestamp is Date {
  return timestamp instanceof Date;
}

/**
 * Check if timestamp is a number (Unix timestamp).
 * @public
 * @stability stable
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
 * Copy common optional CDC event fields from source to target
 * @internal
 */
function copyOptionalCDCFields<T>(
  target: CDCEvent<T>,
  source: CDCEvent<T>,
  options: {
    primaryKey?: Record<string, unknown>;
  }
): void {
  if (options.primaryKey !== undefined) target.primaryKey = options.primaryKey;
  if (source.metadata !== undefined) target.metadata = source.metadata;
}

/**
 * Convert server CDC event to client format.
 *
 * Transforms a server-side CDC event into client-friendly format by:
 * - Converting numeric timestamps to Date objects
 * - Normalizing `oldRow`/`newRow` to `before`/`after`
 * - Normalizing `txId` to `transactionId`
 *
 * @param serverEvent - The CDC event from the server (may have numeric timestamp, oldRow/newRow, txId)
 * @returns A normalized CDC event suitable for client consumption with Date timestamps and before/after fields
 *
 * @public
 * @stability experimental
 *
 * @example
 * ```typescript
 * const serverEvent: CDCEvent = {
 *   lsn: 100n,
 *   table: 'users',
 *   operation: 'INSERT',
 *   timestamp: 1700000000000, // Unix timestamp
 *   txId: 'tx-123',
 *   newRow: { id: 1, name: 'Alice' },
 * };
 * const clientEvent = serverToClientCDCEvent(serverEvent);
 * // clientEvent.timestamp is now a Date
 * // clientEvent.after contains { id: 1, name: 'Alice' }
 * // clientEvent.transactionId is 'tx-123'
 * ```
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

  const before = (serverEvent.before ?? serverEvent.oldRow) as T | undefined;
  if (before !== undefined) result.before = before;

  const after = (serverEvent.after ?? serverEvent.newRow) as T | undefined;
  if (after !== undefined) result.after = after;

  const transactionId = serverEvent.transactionId ?? serverEvent.txId;
  if (transactionId !== undefined) result.transactionId = transactionId;

  const primaryKey = serverEvent.primaryKey ?? serverEvent.newRow ?? serverEvent.oldRow;
  copyOptionalCDCFields(result, serverEvent, primaryKey !== undefined ? { primaryKey } : {});

  return result;
}

/**
 * Convert client CDC event to server format.
 *
 * Transforms a client-side CDC event into server-friendly format by:
 * - Converting Date timestamps to Unix timestamps (milliseconds)
 * - Normalizing `before`/`after` to `oldRow`/`newRow`
 * - Normalizing `transactionId` to `txId`
 *
 * @param clientEvent - The CDC event from the client (may have Date timestamp, before/after, transactionId)
 * @returns A normalized CDC event suitable for server processing with numeric timestamps and oldRow/newRow fields
 *
 * @public
 * @stability experimental
 *
 * @example
 * ```typescript
 * const clientEvent: CDCEvent = {
 *   lsn: 100n,
 *   table: 'users',
 *   operation: 'UPDATE',
 *   timestamp: new Date('2024-01-15T12:00:00Z'),
 *   transactionId: 'tx-456',
 *   before: { id: 1, name: 'Alice' },
 *   after: { id: 1, name: 'Bob' },
 * };
 * const serverEvent = clientToServerCDCEvent(clientEvent);
 * // serverEvent.timestamp is now a number (Unix timestamp)
 * // serverEvent.oldRow contains { id: 1, name: 'Alice' }
 * // serverEvent.newRow contains { id: 1, name: 'Bob' }
 * // serverEvent.txId is 'tx-456'
 * ```
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

  copyOptionalCDCFields(result, clientEvent, clientEvent.primaryKey !== undefined ? { primaryKey: clientEvent.primaryKey } : {});

  return result;
}

/**
 * Convert QueryResponse to QueryResult (client format).
 *
 * Transforms a server-side QueryResponse into a client-facing QueryResult.
 * This handles differences in field naming conventions between server and client:
 * - `rowsAffected` falls back to `rowCount` if not present
 * - `duration` falls back to `executionTimeMs` if not present
 * - LSN is cast to the branded LSN type
 *
 * @param response - The server QueryResponse to convert
 * @returns A client-formatted QueryResult with normalized field names
 *
 * @public
 * @stability stable
 *
 * @example
 * ```typescript
 * const serverResponse: QueryResponse = {
 *   columns: ['id', 'name'],
 *   rows: [{ id: 1, name: 'Alice' }],
 *   rowCount: 1,
 *   executionTimeMs: 5,
 * };
 * const clientResult = responseToResult(serverResponse);
 * // { rows: [...], columns: [...], rowsAffected: 1, duration: 5 }
 * ```
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
 * Convert QueryResult to QueryResponse (server format).
 *
 * Transforms a client-side QueryResult into a server-facing QueryResponse.
 * This handles differences in field naming conventions between client and server:
 * - Sets both `duration` and `executionTimeMs` for backwards compatibility
 * - Computes `rowCount` from `rows.length`
 * - Optionally overrides LSN with a provided value
 *
 * @param result - The client QueryResult to convert
 * @param lsn - Optional LSN to include in the response (overrides result.lsn if provided)
 * @returns A server-formatted QueryResponse with both timing fields set
 *
 * @public
 * @stability stable
 *
 * @example
 * ```typescript
 * const clientResult: QueryResult = {
 *   columns: ['id', 'name'],
 *   rows: [{ id: 1, name: 'Alice' }],
 *   rowsAffected: 1,
 *   duration: 5,
 * };
 * const serverResponse = resultToResponse(clientResult, 100n);
 * // { rows: [...], columns: [...], rowCount: 1, executionTimeMs: 5, lsn: 100n }
 * ```
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
    duration: result.duration,
    // Also set executionTimeMs for backwards compatibility
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
 * Standard error information for Result pattern.
 *
 * Use this for expected failures (e.g., query errors, validation errors)
 * NOT for programmer errors (those should throw).
 *
 * @public
 * @stability stable
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
 * Successful result with data.
 * @public
 * @stability stable
 */
export interface Success<T> {
  success: true;
  data: T;
  error?: never;
}

/**
 * Failed result with error.
 * @public
 * @stability stable
 */
export interface Failure {
  success: false;
  data?: never;
  error: ResultError;
}

/**
 * Standard Result type for operations that can fail.
 *
 * Use this pattern for:
 * - Expected failures (query errors, validation errors, network errors)
 * - Operations where callers need to handle both success and failure cases
 *
 * DO NOT use this for:
 * - Programmer errors (invalid arguments) - throw instead
 * - Unexpected errors - let them propagate as exceptions
 *
 * @public
 * @stability stable
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
 * Type guard to check if a Result is successful.
 *
 * @public
 * @stability stable
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
 * Type guard to check if a Result is a failure.
 *
 * @public
 * @stability stable
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
 * Create a successful Result.
 *
 * @param data - The success data
 * @returns A Success result
 *
 * @public
 * @stability stable
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
 * Create a failed Result.
 *
 * @param code - Error code for programmatic handling
 * @param message - Human-readable error message
 * @param details - Optional additional error details
 * @returns A Failure result
 *
 * @public
 * @stability stable
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
  const error: ResultError = { code, message };
  if (details !== undefined) error.details = details;
  return { success: false, error };
}

/**
 * Create a Failure from an Error object.
 *
 * @param error - The Error object
 * @param code - Optional error code (defaults to 'UNKNOWN_ERROR')
 * @returns A Failure result
 *
 * @public
 * @stability stable
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
 * Unwrap a Result, throwing if it's a failure.
 *
 * Use this when you want to convert a Result back to throw/return style.
 * Useful at API boundaries or when you know the operation should succeed.
 *
 * @param result - The Result to unwrap
 * @returns The success data
 * @throws Error if the Result is a failure
 *
 * @public
 * @stability stable
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
 * Unwrap a Result with a default value for failures.
 *
 * @param result - The Result to unwrap
 * @param defaultValue - Value to return if the Result is a failure
 * @returns The success data or the default value
 *
 * @public
 * @stability stable
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
 * Map the success value of a Result.
 *
 * @param result - The Result to map
 * @param fn - Function to transform the success value
 * @returns A new Result with the transformed value
 *
 * @public
 * @stability stable
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
 * Flat-map the success value of a Result.
 *
 * @param result - The Result to flat-map
 * @param fn - Function that returns a new Result
 * @returns The Result from the function, or the original failure
 *
 * @public
 * @stability stable
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
 * Combine multiple Results into a single Result.
 *
 * If all Results are successful, returns a Success with an array of data.
 * If any Result fails, returns the first Failure.
 *
 * @param results - Array of Results to combine
 * @returns Combined Result
 *
 * @public
 * @stability stable
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
 * Legacy success/error result pattern (for backward compatibility).
 *
 * Many existing types use `{ success: boolean, error?: string }`.
 * Use these type guards to work with them consistently.
 *
 * @public
 * @stability stable
 */
export interface LegacyResult<T = unknown> {
  success: boolean;
  result?: T;
  error?: string;
  [key: string]: unknown;
}

/**
 * Type guard for legacy success result.
 * @public
 * @stability stable
 */
export function isLegacySuccess<T>(
  result: LegacyResult<T>
): result is LegacyResult<T> & { success: true } {
  return result.success === true;
}

/**
 * Type guard for legacy failure result.
 * @public
 * @stability stable
 */
export function isLegacyFailure<T>(
  result: LegacyResult<T>
): result is LegacyResult<T> & { success: false; error: string } {
  return result.success === false;
}

/**
 * Convert a legacy result to the new Result type.
 *
 * @param legacy - Legacy result object
 * @param dataKey - Key to extract data from (default: 'result')
 * @returns Standardized Result
 *
 * @public
 * @stability stable
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

// =============================================================================
// Retry Configuration
// =============================================================================

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
 *
 * Provides sensible defaults for retry behavior:
 * - maxRetries: 3 (4 total attempts)
 * - baseDelayMs: 100ms
 * - maxDelayMs: 5000ms (5 seconds)
 *
 * @public
 * @since 0.1.0
 */
export const DEFAULT_RETRY_CONFIG: Readonly<RetryConfig> = {
  maxRetries: 3,
  baseDelayMs: 100,
  maxDelayMs: 5000,
} as const;

/**
 * Type guard to check if a value is a valid RetryConfig.
 *
 * @param value - The value to check
 * @returns `true` if the value is a valid RetryConfig object
 *
 * @example
 * ```typescript
 * const config = { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 };
 * if (isRetryConfig(config)) {
 *   // TypeScript knows config is RetryConfig
 *   console.log(`Will retry ${config.maxRetries} times`);
 * }
 * ```
 *
 * @public
 * @since 0.1.0
 */
export function isRetryConfig(value: unknown): value is RetryConfig {
  if (value === null || typeof value !== 'object') {
    return false;
  }

  const obj = value as Record<string, unknown>;
  return (
    typeof obj.maxRetries === 'number' &&
    typeof obj.baseDelayMs === 'number' &&
    typeof obj.maxDelayMs === 'number'
  );
}

/**
 * Creates a validated RetryConfig with constraint checking.
 *
 * @param config - The retry configuration to validate and create
 * @returns A validated RetryConfig object
 * @throws Error if the configuration values are invalid
 *
 * @example
 * ```typescript
 * const config = createRetryConfig({
 *   maxRetries: 5,
 *   baseDelayMs: 200,
 *   maxDelayMs: 10000,
 * });
 * ```
 *
 * @public
 * @since 0.1.0
 */
export function createRetryConfig(config: RetryConfig): RetryConfig {
  if (config.maxRetries < 0) {
    throw new Error('maxRetries cannot be negative');
  }
  if (config.baseDelayMs < 0) {
    throw new Error('baseDelayMs cannot be negative');
  }
  if (config.maxDelayMs < 0) {
    throw new Error('maxDelayMs cannot be negative');
  }
  if (config.maxDelayMs < config.baseDelayMs) {
    throw new Error('maxDelayMs cannot be less than baseDelayMs');
  }

  return {
    maxRetries: config.maxRetries,
    baseDelayMs: config.baseDelayMs,
    maxDelayMs: config.maxDelayMs,
  };
}
