/**
 * sql.do - Client SDK for DoSQL
 *
 * SQL database client for Cloudflare Workers using CapnWeb RPC.
 *
 * ## Stability
 *
 * This package follows semantic versioning. Exports are marked with stability annotations:
 *
 * - **stable**: No breaking changes in minor versions. Safe for production use.
 * - **experimental**: May change in any version. Use with caution.
 *
 * See {@link https://github.com/dot-do/sql/blob/main/docs/STABILITY.md | STABILITY.md} for details.
 *
 * @example
 * ```typescript
 * import { createSQLClient } from 'sql.do';
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
 *
 * @packageDocumentation
 */

// =============================================================================
// Stable Client Exports
// =============================================================================

/**
 * @public
 * @stability stable
 */
export { DoSQLClient, TransactionContext, SQLError, createSQLClient } from './client.js';

/**
 * Error types for connection and message handling.
 * @public
 * @stability stable
 * @since 0.3.0
 */
export { ConnectionError, TimeoutError, MessageParseError, isRetryableError } from './errors.js';
export type { TimeoutOperationType } from './errors.js';

/**
 * Event types for client event listeners.
 * @public
 * @stability stable
 * @since 0.3.0
 */
export type { ConnectedEvent, DisconnectedEvent, ClientErrorEvent, ClientEventMap, ClientEventListener } from './types.js';
/**
 * @deprecated Use `ClientErrorEvent` instead. This alias exists for backward compatibility.
 */
export type { ErrorEvent } from './types.js';

/**
 * Idempotency cache statistics type.
 * @public
 * @stability stable
 * @since 0.3.0
 */
export type { IdempotencyCacheStats } from './types.js';

/**
 * Connection pool types for enterprise-grade connection management.
 * @public
 * @stability experimental
 * @since 0.4.0
 */
export type {
  PoolConfig,
  PoolStats,
  PoolHealth,
  ConnectionInfo,
  PoolEventMap,
} from './types.js';
export { ConnectionPool, type ConnectionPoolConfig, type ConnectionPoolEventMap, type PooledConnection } from './connection-pool.js';

/**
 * Idempotency utilities for mutation safety.
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export { generateIdempotencyKey, isMutationQuery } from './client.js';

/**
 * @public
 * @stability stable
 */
export type { SQLClientConfig } from './client.js';
export type { RetryConfig } from './types.js';
export { DEFAULT_RETRY_CONFIG, isRetryConfig, createRetryConfig } from './types.js';

// =============================================================================
// Stable Type Exports
// =============================================================================

/**
 * Core types for SQL operations - all stable.
 * @public
 * @stability stable
 */
export type {
  // Generic branded type utility
  Brand,
  // Branded types
  TransactionId,
  LSN,
  StatementHash,
  ShardId,
  UUID,
  Email,
  Timestamp,
  PageId,
  SchemaVersion,
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
} from './types.js';

/**
 * Sharding types - experimental, subject to change.
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export type {
  ShardConfig,
  ShardInfo,
} from './types.js';

/**
 * CDC (Change Data Capture) types - experimental, subject to change.
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export type {
  CDCOperation,
  ClientCDCOperation,
  CDCEvent,
  CDCOperationCodeValue,
} from './types.js';

/**
 * Client capabilities - experimental, subject to change.
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export type {
  ClientCapabilities,
} from './types.js';

/**
 * Connection types - experimental, subject to change.
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export type {
  ConnectionOptions,
  ConnectionStats,
} from './types.js';

// =============================================================================
// Stable Utility Exports
// =============================================================================

/**
 * Brand constructors for typed identifiers - stable.
 * @public
 * @stability stable
 */
export {
  createTransactionId,
  createLSN,
  createStatementHash,
  createShardId,
  createUUID,
  createEmail,
  createTimestamp,
  createPageId,
  createSchemaVersion,
  // Generic branded type factory pattern
  createBrandedTypeFactory,
  createBrandedBigintFactory,
  createBrandedTypeGuard,
  createBrandedBigintGuard,
  createBrandedNumberFactory,
  createBrandedNumberGuard,
  // LSN utilities
  compareLSN,
  incrementLSN,
  lsnValue,
  // SchemaVersion utilities
  compareSchemaVersion,
  incrementSchemaVersion,
  serializeSchemaVersion,
  deserializeSchemaVersion,
  // Timestamp utilities
  timestampValue,
  timestampToMillis,
  timestampToISO,
  compareTimestamp,
  // Type guards for validation
  isValidLSN,
  isValidTransactionId,
  isValidShardId,
  isValidStatementHash,
  isValidUUID,
  isValidEmail,
  isValidTimestamp,
  isValidPageId,
  isValidSchemaVersion,
  // Dev mode configuration
  setDevMode,
  isDevMode,
  setStrictMode,
  isStrictMode,
} from './types.js';

/**
 * Column type mapping utilities - stable.
 * @public
 * @stability stable
 */
export {
  SQL_TO_JS_TYPE_MAP,
  JS_TO_SQL_TYPE_MAP,
  sqlToJsType,
  jsToSqlType,
} from './types.js';

/**
 * RPC error codes - stable.
 * @public
 * @stability stable
 */
export { RPCErrorCode } from './types.js';

// =============================================================================
// Storage Configuration Types
// =============================================================================

/**
 * Storage configuration types for per-table and per-database storage settings.
 * Used with the WITH STORAGE clause in CREATE TABLE statements.
 *
 * @public
 * @stability experimental
 * @since 0.2.0
 */
export type {
  StorageConfig,
  TableStorageConfig,
  DatabaseStorageConfig,
  StorageConfigField,
} from './storage-config.js';

/**
 * Storage configuration constants and utilities.
 *
 * @public
 * @stability experimental
 * @since 0.2.0
 */
export {
  DEFAULT_STORAGE_CONFIG,
  STORAGE_CONFIG_FIELDS,
  isValidStorageConfig,
} from './storage-config.js';

// =============================================================================
// Experimental Utility Exports
// =============================================================================

/**
 * CDC utilities - experimental, subject to change.
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export {
  CDCOperationCode,
  // Type guards
  isServerCDCEvent,
  isClientCDCEvent,
  isDateTimestamp,
  isNumericTimestamp,
  // Type converters
  serverToClientCDCEvent,
  clientToServerCDCEvent,
} from './types.js';

/**
 * Response/Result converters - experimental, subject to change.
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export {
  responseToResult,
  resultToResponse,
} from './types.js';

/**
 * Default client capabilities - experimental, subject to change.
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export {
  DEFAULT_CLIENT_CAPABILITIES,
} from './types.js';

// =============================================================================
// Logging Configuration
// =============================================================================

/**
 * Logger configuration for controlling client SDK logging behavior.
 *
 * @public
 * @stability experimental
 * @since 0.4.0
 */
export { configureLogger, NoOpSink } from './logger.js';
export type { LogLevel, LogEntry, LogSink, LoggerConfig } from './logger.js';
