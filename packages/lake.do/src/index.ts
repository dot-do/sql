/**
 * lake.do - Client SDK for DoLake Lakehouse
 *
 * @description A TypeScript client library for interacting with DoLake, a lakehouse
 * solution built on Cloudflare Workers and R2. Provides real-time CDC streaming,
 * SQL queries, time travel, partition management, and file compaction.
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
 * ## Features
 *
 * - **SQL Queries**: Execute SQL queries against Parquet data stored in R2
 * - **CDC Streaming**: Subscribe to real-time change data capture events (experimental)
 * - **Time Travel**: Query historical data at any point in time
 * - **Partition Management**: Inspect and manage table partitions
 * - **Compaction**: Trigger and monitor file compaction jobs (experimental)
 * - **Type Safety**: Full TypeScript support with branded types for IDs
 *
 * ## Installation
 *
 * ```bash
 * npm install lake.do
 * ```
 *
 * ## Quick Start
 *
 * @example
 * ```typescript
 * import { createLakeClient } from 'lake.do';
 *
 * // Create a client
 * const client = createLakeClient({
 *   url: 'https://lake.example.com',
 *   token: 'your-token',
 * });
 *
 * // Query lakehouse data with typed results
 * interface OrderSummary {
 *   date: string;
 *   total: number;
 * }
 *
 * const result = await client.query<OrderSummary>(
 *   'SELECT date, SUM(amount) as total FROM orders GROUP BY date'
 * );
 *
 * console.log(`Scanned ${result.bytesScanned} bytes in ${result.duration}ms`);
 * for (const row of result.rows) {
 *   console.log(`${row.date}: $${row.total}`);
 * }
 *
 * // Subscribe to CDC stream for real-time updates
 * for await (const batch of client.subscribe({ tables: ['orders'] })) {
 *   console.log(`Received ${batch.events.length} events`);
 *   for (const event of batch.events) {
 *     console.log(event.operation, event.table, event.after);
 *   }
 * }
 *
 * // Time travel query to see historical data
 * const historical = await client.query(
 *   'SELECT * FROM inventory',
 *   { asOf: new Date('2024-01-01') }
 * );
 *
 * // Always close the client when done
 * await client.close();
 * ```
 *
 * ## API Reference
 *
 * ### Client
 * - {@link createLakeClient} - Factory function to create a client
 * - {@link DoLakeClient} - Client class implementation
 * - {@link LakeClient} - Client interface
 * - {@link LakeError} - Error type thrown by client operations
 *
 * ### Types
 * - {@link LakeQueryOptions} - Options for query execution
 * - {@link LakeQueryResult} - Query result with statistics
 * - {@link CDCStreamOptions} - Options for CDC subscription
 * - {@link CDCBatch} - Batch of CDC events
 * - {@link TableMetadata} - Table schema and metadata
 * - {@link PartitionInfo} - Partition statistics
 * - {@link CompactionJob} - Compaction job status
 * - {@link Snapshot} - Table snapshot for time travel
 *
 * ### Branded Types
 * - {@link CDCEventId} - CDC event identifier
 * - {@link PartitionKey} - Partition identifier
 * - {@link ParquetFileId} - Parquet file identifier
 * - {@link SnapshotId} - Snapshot identifier
 * - {@link CompactionJobId} - Compaction job identifier
 *
 * @packageDocumentation
 * @module lake.do
 */

// =============================================================================
// Stable Client Exports
// =============================================================================

/**
 * Client class and factory function for creating DoLake connections.
 *
 * @see {@link createLakeClient} - Recommended way to create a client
 * @see {@link DoLakeClient} - Client class implementation
 * @see {@link LakeError} - Error type for lake operations
 *
 * @public
 * @stability stable
 */
export {
  DoLakeClient,
  LakeError,
  ConnectionError,
  QueryError,
  createLakeClient,
} from './client.js';

/**
 * Event types for client connection lifecycle.
 *
 * @see {@link LakeClientEventType} - Union of event type strings
 * @see {@link LakeClientEventHandler} - Event handler function type
 *
 * @public
 * @stability stable
 */
export type { LakeClientEventType, LakeClientEventHandler } from './client.js';

/**
 * Configuration types for client initialization.
 *
 * @see {@link LakeClientConfig} - Main configuration interface
 * @see {@link RetryConfig} - Retry behavior configuration
 *
 * @public
 * @stability stable
 */
export type { LakeClientConfig } from './client.js';
export type { RetryConfig } from './types.js';
export { DEFAULT_RETRY_CONFIG, isRetryConfig, createRetryConfig } from './types.js';

// =============================================================================
// Stable Type Exports
// =============================================================================

/**
 * Branded types for type-safe identifiers - stable.
 *
 * @public
 * @stability stable
 */
export type {
  CDCEventId,
  PartitionKey,
  ParquetFileId,
  SnapshotId,
  CompactionJobId,
} from './types.js';

/**
 * Query types - stable.
 *
 * @public
 * @stability stable
 */
export type {
  LakeQueryOptions,
  LakeQueryResult,
} from './types.js';

/**
 * Partition types - stable.
 *
 * @public
 * @stability stable
 */
export type {
  PartitionStrategy,
  PartitionConfig,
  PartitionInfo,
} from './types.js';

/**
 * Snapshot and time travel types - stable.
 *
 * @public
 * @stability stable
 */
export type {
  Snapshot,
  TimeTravelOptions,
} from './types.js';

/**
 * Schema types - stable.
 *
 * @public
 * @stability stable
 */
export type {
  LakeColumnType,
  LakeColumn,
  LakeSchema,
  TableMetadata,
} from './types.js';

/**
 * Client interface - stable.
 *
 * @public
 * @stability stable
 */
export type {
  LakeClient,
} from './types.js';

// =============================================================================
// Experimental Type Exports
// =============================================================================

/**
 * CDC streaming types - experimental, subject to change.
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export type {
  CDCStreamOptions,
  CDCBatch,
  CDCStreamState,
} from './types.js';

/**
 * Compaction types - experimental, subject to change.
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export type {
  CompactionConfig,
  CompactionJob,
} from './types.js';

/**
 * RPC types - internal, for advanced usage only.
 *
 * @internal
 */
export type {
  LakeRPCMethod,
  LakeRPCRequest,
  LakeRPCResponse,
  LakeRPCError,
} from './types.js';

/**
 * Metrics types - experimental, subject to change.
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export type {
  LakeMetrics,
} from './types.js';

// =============================================================================
// Stable Brand Constructor Exports
// =============================================================================

/**
 * Factory functions for creating branded type instances - stable.
 *
 * These functions convert plain strings to type-safe branded types,
 * ensuring compile-time type safety when working with identifiers.
 *
 * @see {@link createCDCEventId} - Create a CDCEventId
 * @see {@link createPartitionKey} - Create a PartitionKey
 * @see {@link createParquetFileId} - Create a ParquetFileId
 * @see {@link createSnapshotId} - Create a SnapshotId
 * @see {@link createCompactionJobId} - Create a CompactionJobId
 *
 * @example
 * ```typescript
 * import { createPartitionKey, createSnapshotId } from 'lake.do';
 *
 * const partitionKey = createPartitionKey('date=2024-01-15');
 * const snapshotId = createSnapshotId('snap_123');
 * ```
 *
 * @public
 * @stability stable
 */
export {
  createCDCEventId,
  createPartitionKey,
  createParquetFileId,
  createSnapshotId,
  createCompactionJobId,
} from './types.js';

// =============================================================================
// Re-exported Types from sql.do
// =============================================================================

/**
 * Common types re-exported from sql.do for convenience - stable.
 *
 * These types are used in CDC events and are provided here to avoid
 * requiring a separate import from sql.do.
 *
 * @see {@link TransactionId} - Transaction identifier
 * @see {@link LSN} - Log sequence number
 * @see {@link SQLValue} - SQL value union type
 *
 * @public
 * @stability stable
 */
export type {
  TransactionId,
  LSN,
  SQLValue,
} from './types.js';

/**
 * CDC types re-exported from sql.do - experimental.
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export type {
  CDCOperation,
  CDCEvent,
  ClientCDCOperation,
  ClientCapabilities,
} from './types.js';

/**
 * CDC constants and utilities - experimental.
 *
 * @see {@link CDCOperationCode} - Enum of CDC operation codes
 * @see {@link isServerCDCEvent} - Type guard for server CDC events
 * @see {@link isClientCDCEvent} - Type guard for client CDC events
 * @see {@link serverToClientCDCEvent} - Convert server event format
 * @see {@link clientToServerCDCEvent} - Convert client event format
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export {
  CDCOperationCode,
  // Type Guards
  isServerCDCEvent,
  isClientCDCEvent,
  isDateTimestamp,
  isNumericTimestamp,
  // Converters
  serverToClientCDCEvent,
  clientToServerCDCEvent,
} from './types.js';

/**
 * Default client capabilities - experimental.
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export {
  DEFAULT_CLIENT_CAPABILITIES,
} from './types.js';

// =============================================================================
// Advanced Module Exports
// =============================================================================

/**
 * Connection management module - for advanced usage.
 *
 * @see {@link WebSocketConnectionManager} - Manages WebSocket connections
 * @see {@link ConnectionEventEmitter} - Event emitter for connection events
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export {
  WebSocketConnectionManager,
  ConnectionEventEmitter,
} from './connection/index.js';

export type {
  ConnectionEventType,
  ConnectionConfig,
} from './connection/index.js';

/**
 * CDC Stream management module - for advanced usage.
 *
 * @see {@link CDCStreamController} - Manages CDC stream iteration
 * @see {@link BoundedQueue} - Queue with backpressure support
 * @see {@link MetricsTracker} - Metrics collection for queues
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export {
  CDCStreamController,
  BoundedQueue,
  MetricsTracker,
} from './cdc-stream/index.js';

export type {
  CDCStreamControllerOptions,
  CDCStreamMetrics,
  BackpressureStrategy,
  WaterMarkEvent,
  BoundedQueueOptions,
  PushResult,
  MetricsTrackerOptions,
  QueueMetrics,
} from './cdc-stream/index.js';

/**
 * Query execution module - for advanced usage.
 *
 * @see {@link QueryExecutor} - Handles RPC calls and response transformation
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export {
  QueryExecutor,
} from './query/index.js';

export type {
  QueryExecutorConfig,
} from './query/index.js';

/**
 * Error classes - stable.
 *
 * @see {@link TimeoutError} - Error for request timeouts
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export {
  TimeoutError,
} from './errors.js';

/**
 * Constants - stable.
 *
 * @see {@link ErrorCode} - Named error codes
 * @see {@link DEFAULT_TIMEOUT_MS} - Default timeout value
 * @see {@link DEFAULT_MAX_QUEUE_SIZE} - Default CDC queue size
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export {
  ErrorCode,
  DEFAULT_TIMEOUT_MS,
  DEFAULT_MAX_QUEUE_SIZE,
  MIN_QUEUE_SIZE,
  WebSocketState,
} from './constants.js';
