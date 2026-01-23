/**
 * @dotdo/lake.do - Shared types for DoLake client and server
 *
 * This module re-exports types from @dotdo/sql.do (which uses @dotdo/shared-types)
 * and adds lake-specific type definitions.
 *
 * @packageDocumentation
 */

// =============================================================================
// Branded Types for Lake.do
// =============================================================================

declare const CDCEventIdBrand: unique symbol;
declare const PartitionKeyBrand: unique symbol;
declare const ParquetFileIdBrand: unique symbol;
declare const SnapshotIdBrand: unique symbol;
declare const CompactionJobIdBrand: unique symbol;

/**
 * Branded type for CDC event identifiers.
 *
 * @description Uniquely identifies a CDC (Change Data Capture) event within the lakehouse.
 * Uses TypeScript's branded types pattern to prevent accidental mixing with plain strings.
 *
 * @example
 * ```typescript
 * const eventId: CDCEventId = createCDCEventId('evt_123456');
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type CDCEventId = string & { readonly [CDCEventIdBrand]: never };

/**
 * Branded type for partition key identifiers.
 *
 * @description Uniquely identifies a partition within a table. Partitions are used
 * to organize data for efficient querying and compaction.
 *
 * @example
 * ```typescript
 * const partitionKey: PartitionKey = createPartitionKey('date=2024-01-15');
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type PartitionKey = string & { readonly [PartitionKeyBrand]: never };

/**
 * Branded type for Parquet file identifiers.
 *
 * @description Uniquely identifies a Parquet data file within the lakehouse storage.
 *
 * @example
 * ```typescript
 * const fileId: ParquetFileId = createParquetFileId('data_00001.parquet');
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type ParquetFileId = string & { readonly [ParquetFileIdBrand]: never };

/**
 * Branded type for snapshot identifiers.
 *
 * @description Uniquely identifies a snapshot in the table's version history.
 * Snapshots enable time travel queries and rollback operations.
 *
 * @example
 * ```typescript
 * const snapshotId: SnapshotId = createSnapshotId('snap_789');
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type SnapshotId = string & { readonly [SnapshotIdBrand]: never };

/**
 * Branded type for compaction job identifiers.
 *
 * @description Uniquely identifies a compaction job that merges small files
 * into larger, optimized files.
 *
 * @example
 * ```typescript
 * const jobId: CompactionJobId = createCompactionJobId('compact_456');
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type CompactionJobId = string & { readonly [CompactionJobIdBrand]: never };

/**
 * Creates a branded CDCEventId from a plain string.
 *
 * @description Converts a plain string identifier to a type-safe CDCEventId.
 * This ensures type safety when working with CDC event identifiers across the API.
 * In dev mode, validates that the id is a non-empty string.
 *
 * @param id - The plain string identifier for the CDC event
 * @returns A branded CDCEventId that can be used with lake.do APIs
 * @throws {Error} In dev mode: if id is not a string or is empty
 *
 * @example
 * ```typescript
 * const eventId = createCDCEventId('evt_123456');
 * // eventId is now typed as CDCEventId
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function createCDCEventId(id: string): CDCEventId {
  if (isDevMode()) {
    if (typeof id !== 'string') {
      throw new Error('CDCEventId must be a string');
    }
    if (id.trim().length === 0) {
      throw new Error('CDCEventId cannot be empty');
    }
  }
  return id as CDCEventId;
}

/**
 * Creates a branded PartitionKey from a plain string.
 *
 * @description Converts a plain string to a type-safe PartitionKey.
 * Partition keys typically follow the format "column=value" (e.g., "date=2024-01-15").
 * In dev mode, validates that the key is a non-empty string.
 *
 * @param key - The plain string partition key
 * @returns A branded PartitionKey that can be used with lake.do APIs
 * @throws {Error} In dev mode: if key is not a string or is empty
 *
 * @example
 * ```typescript
 * const partitionKey = createPartitionKey('date=2024-01-15');
 * const partitions = await client.listPartitions('orders');
 * await client.compact(partitionKey);
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function createPartitionKey(key: string): PartitionKey {
  if (isDevMode()) {
    if (typeof key !== 'string') {
      throw new Error('PartitionKey must be a string');
    }
    if (key.trim().length === 0) {
      throw new Error('PartitionKey cannot be empty');
    }
  }
  return key as PartitionKey;
}

/**
 * Creates a branded ParquetFileId from a plain string.
 *
 * @description Converts a plain string to a type-safe ParquetFileId.
 * Parquet file IDs identify data files stored in the lakehouse.
 * In dev mode, validates that the id is a non-empty string.
 *
 * @param id - The plain string file identifier
 * @returns A branded ParquetFileId that can be used with lake.do APIs
 * @throws {Error} In dev mode: if id is not a string or is empty
 *
 * @example
 * ```typescript
 * const fileId = createParquetFileId('data_00001.parquet');
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function createParquetFileId(id: string): ParquetFileId {
  if (isDevMode()) {
    if (typeof id !== 'string') {
      throw new Error('ParquetFileId must be a string');
    }
    if (id.trim().length === 0) {
      throw new Error('ParquetFileId cannot be empty');
    }
  }
  return id as ParquetFileId;
}

/**
 * Creates a branded SnapshotId from a plain string.
 *
 * @description Converts a plain string to a type-safe SnapshotId.
 * Snapshot IDs are used for time travel queries and version management.
 * In dev mode, validates that the id is a non-empty string.
 *
 * @param id - The plain string snapshot identifier
 * @returns A branded SnapshotId that can be used with lake.do APIs
 * @throws {Error} In dev mode: if id is not a string or is empty
 *
 * @example
 * ```typescript
 * const snapshotId = createSnapshotId('snap_789');
 * const result = await client.query('SELECT * FROM orders', { asOf: snapshotId });
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function createSnapshotId(id: string): SnapshotId {
  if (isDevMode()) {
    if (typeof id !== 'string') {
      throw new Error('SnapshotId must be a string');
    }
    if (id.trim().length === 0) {
      throw new Error('SnapshotId cannot be empty');
    }
  }
  return id as SnapshotId;
}

/**
 * Creates a branded CompactionJobId from a plain string.
 *
 * @description Converts a plain string to a type-safe CompactionJobId.
 * Compaction job IDs are used to track the status of file compaction operations.
 * In dev mode, validates that the id is a non-empty string.
 *
 * @param id - The plain string job identifier
 * @returns A branded CompactionJobId that can be used with lake.do APIs
 * @throws {Error} In dev mode: if id is not a string or is empty
 *
 * @example
 * ```typescript
 * const jobId = createCompactionJobId('compact_456');
 * const status = await client.getCompactionStatus(jobId);
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function createCompactionJobId(id: string): CompactionJobId {
  if (isDevMode()) {
    if (typeof id !== 'string') {
      throw new Error('CompactionJobId must be a string');
    }
    if (id.trim().length === 0) {
      throw new Error('CompactionJobId cannot be empty');
    }
  }
  return id as CompactionJobId;
}

// =============================================================================
// Re-export common types from sql.do (which uses shared-types)
// =============================================================================

/**
 * Re-exported types from `@dotdo/sql.do` (which re-exports from `@dotdo/shared-types`).
 *
 * These types are re-exported for convenience when building lakehouse clients.
 * They maintain type compatibility across the DoSQL ecosystem.
 *
 * ## Type Re-exports
 *
 * The following types are re-exported from the shared type definitions:
 *
 * - {@link TransactionId} - Unique identifier for database transactions
 * - {@link LSN} - Log Sequence Number for WAL positioning
 * - {@link SQLValue} - Union of valid SQL parameter/result values
 * - {@link CDCOperation} - CDC operation types (INSERT, UPDATE, DELETE, TRUNCATE)
 * - {@link CDCEvent} - Change Data Capture event with row data
 * - {@link ClientCDCOperation} - Client-facing CDC operations (excludes TRUNCATE)
 * - {@link ClientCapabilities} - Protocol capabilities advertised by client
 * - {@link RetryConfig} - Configuration for retry behavior
 *
 * ## Value Re-exports
 *
 * The following values and functions are re-exported:
 *
 * - `CDCOperationCode` - Numeric codes for efficient binary CDC encoding
 * - `DEFAULT_CLIENT_CAPABILITIES` - Default client capability settings
 * - `DEFAULT_RETRY_CONFIG` - Default retry configuration
 * - `isRetryConfig()` / `createRetryConfig()` - Retry config utilities
 * - `setDevMode()` / `isDevMode()` - Development mode configuration
 *
 * ## Type Guards
 *
 * Functions for runtime type checking:
 *
 * - `isServerCDCEvent()` - Check if event has server-side format (txId)
 * - `isClientCDCEvent()` - Check if event has client-side format (transactionId)
 * - `isDateTimestamp()` - Check if timestamp is a Date object
 * - `isNumericTimestamp()` - Check if timestamp is a Unix timestamp number
 *
 * ## Type Converters
 *
 * Functions for converting between formats:
 *
 * - `serverToClientCDCEvent()` - Convert server CDC event to client format
 * - `clientToServerCDCEvent()` - Convert client CDC event to server format
 *
 * @see {@link https://github.com/dotdo/sql.do | @dotdo/sql.do} for client types
 * @see {@link https://github.com/dotdo/shared-types | @dotdo/shared-types} for canonical definitions
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type {
  TransactionId,
  LSN,
  SQLValue,
  CDCOperation,
  CDCEvent,
  ClientCDCOperation,
  ClientCapabilities,
  RetryConfig,
} from '@dotdo/sql.do';

export {
  CDCOperationCode,
  DEFAULT_CLIENT_CAPABILITIES,
  // Type Guards
  isServerCDCEvent,
  isClientCDCEvent,
  isDateTimestamp,
  isNumericTimestamp,
  // Converters
  serverToClientCDCEvent,
  clientToServerCDCEvent,
  // Retry Config
  DEFAULT_RETRY_CONFIG,
  isRetryConfig,
  createRetryConfig,
  // Dev Mode
  setDevMode,
  isDevMode,
} from '@dotdo/sql.do';

// Import isDevMode for local use in factory functions
import { isDevMode } from '@dotdo/sql.do';

// =============================================================================
// CDC Streaming Types
// =============================================================================

import type { CDCEvent, LSN } from '@dotdo/sql.do';

/**
 * Configuration options for subscribing to CDC (Change Data Capture) streams.
 *
 * @description Controls which events are received from the lakehouse CDC stream,
 * including filtering by table, operation type, and starting position.
 *
 * @example
 * ```typescript
 * const options: CDCStreamOptions = {
 *   tables: ['orders', 'inventory'],
 *   operations: ['INSERT', 'UPDATE'],
 *   batchSize: 100,
 *   batchTimeoutMs: 1000,
 * };
 *
 * for await (const batch of client.subscribe(options)) {
 *   console.log(`Received ${batch.events.length} events`);
 * }
 * ```
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export interface CDCStreamOptions {
  /** Starting LSN (exclusive) - resume from this log sequence number */
  fromLSN?: bigint | LSN;
  /** Tables to subscribe to (empty array or undefined = all tables) */
  tables?: string[];
  /** Operations to subscribe to (undefined = all operations) */
  operations?: Array<'INSERT' | 'UPDATE' | 'DELETE'>;
  /** Maximum number of events per batch (default: server-defined) */
  batchSize?: number;
  /** Maximum time to wait before flushing a partial batch, in milliseconds */
  batchTimeoutMs?: number;
}

/**
 * A batch of CDC events received from the lakehouse stream.
 *
 * @description Contains one or more CDC events along with metadata about
 * the batch including sequence number and source Durable Object ID.
 *
 * @example
 * ```typescript
 * for await (const batch of client.subscribe()) {
 *   console.log(`Batch #${batch.sequenceNumber} from ${batch.sourceDoId}`);
 *   for (const event of batch.events) {
 *     if (event.operation === 'INSERT') {
 *       console.log('New row:', event.after);
 *     }
 *   }
 * }
 * ```
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export interface CDCBatch {
  /** Array of CDC events in this batch */
  events: CDCEvent[];
  /** Monotonically increasing sequence number for ordering batches */
  sequenceNumber: number;
  /** Timestamp when this batch was created */
  timestamp: Date;
  /** Identifier of the Durable Object that produced this batch */
  sourceDoId: string;
}

/**
 * Current state of a CDC stream subscription.
 *
 * @description Provides real-time statistics about the CDC stream connection,
 * useful for monitoring and debugging.
 *
 * @example
 * ```typescript
 * // Monitor stream health
 * const state: CDCStreamState = {
 *   connected: true,
 *   lastLSN: 12345n,
 *   eventsReceived: 1000,
 *   bytesReceived: 524288,
 * };
 * ```
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export interface CDCStreamState {
  /** Whether the stream is currently connected */
  connected: boolean;
  /** The last log sequence number received */
  lastLSN: bigint;
  /** Total number of events received since subscription started */
  eventsReceived: number;
  /** Total bytes received since subscription started */
  bytesReceived: number;
}

// =============================================================================
// Query Types
// =============================================================================

/**
 * Options for executing queries against the lakehouse.
 *
 * @description Provides fine-grained control over query execution including
 * partition pruning, time travel, pagination, and column projection.
 *
 * @example
 * ```typescript
 * // Query specific partitions with pagination
 * const options: LakeQueryOptions = {
 *   partitions: [createPartitionKey('date=2024-01-15')],
 *   limit: 100,
 *   offset: 0,
 *   columns: ['id', 'amount', 'status'],
 * };
 *
 * // Time travel query to a specific point in time
 * const historicalOptions: LakeQueryOptions = {
 *   asOf: new Date('2024-01-01T00:00:00Z'),
 * };
 *
 * // Time travel to a specific snapshot
 * const snapshotOptions: LakeQueryOptions = {
 *   asOf: createSnapshotId('snap_789'),
 * };
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface LakeQueryOptions {
  /** Query only specific partitions for partition pruning optimization */
  partitions?: PartitionKey[];
  /** Time travel to a specific point in time (Date) or snapshot (SnapshotId) */
  asOf?: Date | SnapshotId;
  /** Maximum number of rows to return */
  limit?: number;
  /** Number of rows to skip before returning results */
  offset?: number;
  /** Column projection - only return these columns (reduces I/O) */
  columns?: string[];
}

/**
 * Result of a lakehouse query execution.
 *
 * @description Contains the query results along with execution statistics
 * useful for performance monitoring and optimization.
 *
 * @typeParam T - The type of each row in the result set
 *
 * @example
 * ```typescript
 * interface OrderRow {
 *   id: string;
 *   amount: number;
 *   status: string;
 * }
 *
 * const result: LakeQueryResult<OrderRow> = await client.query<OrderRow>(
 *   'SELECT id, amount, status FROM orders WHERE amount > 100'
 * );
 *
 * console.log(`Found ${result.rowCount} orders`);
 * console.log(`Scanned ${result.bytesScanned} bytes from ${result.filesScanned} files`);
 * console.log(`Query took ${result.duration}ms`);
 *
 * for (const order of result.rows) {
 *   console.log(`Order ${order.id}: $${order.amount}`);
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface LakeQueryResult<T = Record<string, unknown>> {
  /** Array of result rows matching the query */
  rows: T[];
  /** Names of columns in the result set */
  columns: string[];
  /** Total number of rows returned */
  rowCount: number;
  /** Total bytes scanned during query execution */
  bytesScanned: number;
  /** Number of Parquet files scanned */
  filesScanned: number;
  /** Number of partitions scanned */
  partitionsScanned: number;
  /** Query execution duration in milliseconds */
  duration: number;
}

// =============================================================================
// Partition Types
// =============================================================================

/**
 * Partitioning strategy for organizing table data.
 *
 * @description Defines how data is distributed across partitions:
 * - `time`: Partition by time intervals (hourly, daily, monthly)
 * - `hash`: Partition by hash of column value into fixed buckets
 * - `list`: Partition by explicit list of values
 * - `range`: Partition by value ranges
 *
 * @example
 * ```typescript
 * const strategy: PartitionStrategy = 'time';
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type PartitionStrategy = 'time' | 'hash' | 'list' | 'range';

/**
 * Configuration for table partitioning.
 *
 * @description Defines how a table's data should be partitioned for
 * efficient querying and data management.
 *
 * @example
 * ```typescript
 * // Time-based partitioning by day
 * const timeConfig: PartitionConfig = {
 *   strategy: 'time',
 *   column: 'created_at',
 *   granularity: 'daily',
 * };
 *
 * // Hash partitioning into 16 buckets
 * const hashConfig: PartitionConfig = {
 *   strategy: 'hash',
 *   column: 'user_id',
 *   buckets: 16,
 * };
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface PartitionConfig {
  /** The partitioning strategy to use */
  strategy: PartitionStrategy;
  /** The column to partition by */
  column: string;
  /** For time partitioning: the time interval granularity */
  granularity?: 'hourly' | 'daily' | 'monthly';
  /** For hash partitioning: the number of hash buckets */
  buckets?: number;
}

/**
 * Information about a specific partition in a table.
 *
 * @description Provides metadata about a partition including size, file count,
 * and row statistics. Useful for monitoring and compaction decisions.
 *
 * @example
 * ```typescript
 * const partitions = await client.listPartitions('orders');
 * for (const partition of partitions) {
 *   console.log(`Partition ${partition.key}:`);
 *   console.log(`  Files: ${partition.fileCount}`);
 *   console.log(`  Rows: ${partition.rowCount}`);
 *   console.log(`  Size: ${partition.sizeBytes} bytes`);
 *   console.log(`  Last modified: ${partition.lastModified}`);
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface PartitionInfo {
  /** Unique identifier for this partition */
  key: PartitionKey;
  /** The partitioning strategy used */
  strategy: PartitionStrategy;
  /** For range/time partitions: the min and max values covered */
  range?: { min: unknown; max: unknown };
  /** Number of data files in this partition */
  fileCount: number;
  /** Total number of rows in this partition */
  rowCount: number;
  /** Total size of data files in bytes */
  sizeBytes: number;
  /** When this partition was last modified */
  lastModified: Date;
}

// =============================================================================
// Compaction Types
// =============================================================================

/**
 * Configuration for file compaction operations.
 *
 * @description Controls how small files are merged into larger, more efficient
 * files. Proper compaction improves query performance and reduces storage costs.
 *
 * @example
 * ```typescript
 * const compactionConfig: CompactionConfig = {
 *   targetFileSize: 128 * 1024 * 1024, // 128 MB target
 *   minFiles: 5,                        // Trigger when 5+ files exist
 *   maxFiles: 100,                      // Compact at most 100 files at once
 *   sortBy: ['timestamp', 'user_id'],   // Sort output by these columns
 * };
 *
 * const job = await client.compact(partitionKey, compactionConfig);
 * ```
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export interface CompactionConfig {
  /** Target size for output files in bytes (e.g., 128 MB = 134217728) */
  targetFileSize: number;
  /** Minimum number of files required to trigger compaction */
  minFiles: number;
  /** Maximum number of files to compact in a single job */
  maxFiles: number;
  /** Columns to sort by in the compacted output files */
  sortBy?: string[];
}

/**
 * Status and metadata for a compaction job.
 *
 * @description Tracks the progress and outcome of a file compaction operation.
 * Use `getCompactionStatus()` to poll for job completion.
 *
 * @example
 * ```typescript
 * const job = await client.compact(partitionKey);
 * console.log(`Started compaction job: ${job.id}`);
 * console.log(`Status: ${job.status}`);
 * console.log(`Input files: ${job.inputFiles.length}`);
 *
 * // Poll for completion
 * while (job.status === 'pending' || job.status === 'running') {
 *   await new Promise(r => setTimeout(r, 1000));
 *   job = await client.getCompactionStatus(job.id);
 * }
 *
 * if (job.status === 'completed') {
 *   console.log(`Compacted ${job.inputFiles.length} files into ${job.outputFiles?.length}`);
 *   console.log(`Read ${job.bytesRead} bytes, wrote ${job.bytesWritten} bytes`);
 * } else if (job.status === 'failed') {
 *   console.error(`Compaction failed: ${job.error}`);
 * }
 * ```
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export interface CompactionJob {
  /** Unique identifier for this compaction job */
  id: CompactionJobId;
  /** The partition being compacted */
  partition: PartitionKey;
  /** Current status of the compaction job */
  status: 'pending' | 'running' | 'completed' | 'failed';
  /** List of input files being compacted */
  inputFiles: ParquetFileId[];
  /** List of output files created (available when completed) */
  outputFiles?: ParquetFileId[];
  /** When the job started running */
  startedAt?: Date;
  /** When the job completed (successfully or with failure) */
  completedAt?: Date;
  /** Error message if the job failed */
  error?: string;
  /** Total bytes read from input files */
  bytesRead?: number;
  /** Total bytes written to output files */
  bytesWritten?: number;
}

// =============================================================================
// Snapshot Types
// =============================================================================

/**
 * A point-in-time snapshot of a table's state.
 *
 * @description Snapshots capture the table's state at a specific moment,
 * enabling time travel queries, auditing, and rollback operations.
 * Each snapshot forms part of a linked list through parentId.
 *
 * @example
 * ```typescript
 * const snapshots = await client.listSnapshots('orders');
 * const latestSnapshot = snapshots[0];
 *
 * console.log(`Snapshot ${latestSnapshot.id}`);
 * console.log(`Created: ${latestSnapshot.timestamp}`);
 * console.log(`Changes: +${latestSnapshot.summary.addedRows} -${latestSnapshot.summary.deletedRows} rows`);
 *
 * // Query as of this snapshot
 * const result = await client.query('SELECT * FROM orders', {
 *   asOf: latestSnapshot.id,
 * });
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface Snapshot {
  /** Unique identifier for this snapshot */
  id: SnapshotId;
  /** When this snapshot was created */
  timestamp: Date;
  /** ID of the parent snapshot (for history traversal) */
  parentId?: SnapshotId;
  /** Summary of changes from the parent snapshot */
  summary: {
    /** Number of files added in this snapshot */
    addedFiles: number;
    /** Number of files removed in this snapshot */
    deletedFiles: number;
    /** Number of rows added in this snapshot */
    addedRows: number;
    /** Number of rows deleted in this snapshot */
    deletedRows: number;
  };
  /** Path to the manifest list file for this snapshot */
  manifestList: string;
}

/**
 * Options for time travel queries.
 *
 * @description Specifies how to identify a historical point in time
 * for time travel queries. Supports absolute time, snapshot ID, or
 * relative time expressions.
 *
 * @example
 * ```typescript
 * // Query at a specific point in time
 * const byTime: TimeTravelOptions = {
 *   asOf: new Date('2024-01-01T00:00:00Z'),
 * };
 *
 * // Query at a specific snapshot
 * const bySnapshot: TimeTravelOptions = {
 *   snapshotId: createSnapshotId('snap_789'),
 * };
 *
 * // Query using relative time
 * const byRelative: TimeTravelOptions = {
 *   before: '1 hour ago',
 * };
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface TimeTravelOptions {
  /** Query at a specific snapshot ID */
  snapshotId?: SnapshotId;
  /** Query at a specific point in time */
  asOf?: Date;
  /** Query at a relative time (e.g., '1 hour ago', '7 days ago') */
  before?: string;
}

// =============================================================================
// Schema Types
// =============================================================================

/**
 * Supported column data types in the lakehouse.
 *
 * @description Defines the primitive and complex types available for table columns.
 * Types are compatible with Apache Iceberg and Parquet specifications.
 *
 * Primitive types:
 * - `boolean`: true/false values
 * - `int`: 32-bit signed integer
 * - `long`: 64-bit signed integer
 * - `float`: 32-bit IEEE 754 floating point
 * - `double`: 64-bit IEEE 754 floating point
 * - `decimal`: Arbitrary precision decimal
 * - `date`: Calendar date (year, month, day)
 * - `time`: Time of day (hour, minute, second, microsecond)
 * - `timestamp`: Timestamp without timezone
 * - `timestamptz`: Timestamp with timezone
 * - `string`: UTF-8 character sequence
 * - `uuid`: Universally unique identifier
 * - `fixed`: Fixed-length byte array
 * - `binary`: Variable-length byte array
 *
 * Complex types:
 * - `struct`: Nested record with named fields
 * - `list`: Ordered collection of elements
 * - `map`: Key-value pairs
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type LakeColumnType =
  | 'boolean'
  | 'int'
  | 'long'
  | 'float'
  | 'double'
  | 'decimal'
  | 'date'
  | 'time'
  | 'timestamp'
  | 'timestamptz'
  | 'string'
  | 'uuid'
  | 'fixed'
  | 'binary'
  | 'struct'
  | 'list'
  | 'map';

/**
 * Definition of a column in a lakehouse table.
 *
 * @description Describes a single column including its type, nullability,
 * and optional documentation.
 *
 * @example
 * ```typescript
 * const column: LakeColumn = {
 *   id: 1,
 *   name: 'user_id',
 *   type: 'long',
 *   required: true,
 *   doc: 'Unique identifier for the user',
 * };
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface LakeColumn {
  /** Unique column ID within the schema (for schema evolution tracking) */
  id: number;
  /** Column name */
  name: string;
  /** Column data type */
  type: LakeColumnType;
  /** Whether the column is required (NOT NULL) */
  required: boolean;
  /** Optional documentation/description for the column */
  doc?: string;
}

/**
 * Schema definition for a lakehouse table.
 *
 * @description Defines the structure of a table including all columns
 * and identifier fields (primary key equivalent).
 *
 * @example
 * ```typescript
 * const schema: LakeSchema = {
 *   schemaId: 1,
 *   columns: [
 *     { id: 1, name: 'id', type: 'long', required: true },
 *     { id: 2, name: 'name', type: 'string', required: true },
 *     { id: 3, name: 'created_at', type: 'timestamptz', required: true },
 *   ],
 *   identifierFields: [1], // 'id' is the primary key
 * };
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface LakeSchema {
  /** Unique identifier for this schema version */
  schemaId: number;
  /** Array of column definitions */
  columns: LakeColumn[];
  /** Column IDs that form the row identifier (primary key) */
  identifierFields?: number[];
}

/**
 * Complete metadata for a lakehouse table.
 *
 * @description Contains all metadata about a table including schema,
 * partitioning, snapshots, and custom properties.
 *
 * @example
 * ```typescript
 * const metadata = await client.getMetadata('orders');
 *
 * console.log(`Table: ${metadata.tableId}`);
 * console.log(`Columns: ${metadata.schema.columns.map(c => c.name).join(', ')}`);
 * console.log(`Partitioned by: ${metadata.partitionSpec.column}`);
 * console.log(`Snapshots: ${metadata.snapshots.length}`);
 * console.log(`Current snapshot: ${metadata.currentSnapshotId}`);
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface TableMetadata {
  /** Unique identifier for the table */
  tableId: string;
  /** Current schema definition */
  schema: LakeSchema;
  /** Partitioning configuration */
  partitionSpec: PartitionConfig;
  /** ID of the current (latest) snapshot */
  currentSnapshotId?: SnapshotId;
  /** List of all available snapshots for time travel */
  snapshots: Snapshot[];
  /** Custom table properties (key-value pairs) */
  properties: Record<string, string>;
}

// =============================================================================
// RPC Message Types
// =============================================================================

/**
 * Available RPC methods in the lake.do protocol.
 *
 * @description Defines all methods available through the CapnWeb RPC protocol.
 * These correspond to the public methods on the LakeClient interface.
 *
 * @internal
 */
export type LakeRPCMethod =
  | 'query'
  | 'subscribe'
  | 'unsubscribe'
  | 'getMetadata'
  | 'listPartitions'
  | 'getPartition'
  | 'compact'
  | 'getCompactionStatus'
  | 'listSnapshots'
  | 'getSnapshot'
  | 'ping';

/**
 * RPC request message format.
 *
 * @description Structure of messages sent from client to server.
 * Follows JSON-RPC-like conventions with request IDs for correlation.
 *
 * @example
 * ```typescript
 * const request: LakeRPCRequest = {
 *   id: '1',
 *   method: 'query',
 *   params: { sql: 'SELECT * FROM orders LIMIT 10' },
 * };
 * ```
 *
 * @internal
 */
export interface LakeRPCRequest {
  /** Unique request ID for correlating responses */
  id: string;
  /** The RPC method to invoke */
  method: LakeRPCMethod;
  /** Method-specific parameters */
  params: unknown;
}

/**
 * RPC response message format.
 *
 * @description Structure of messages sent from server to client.
 * Contains either a result or an error, never both.
 *
 * @typeParam T - The type of the result payload
 *
 * @example
 * ```typescript
 * // Successful response
 * const successResponse: LakeRPCResponse<LakeQueryResult> = {
 *   id: '1',
 *   result: { rows: [...], columns: [...], ... },
 * };
 *
 * // Error response
 * const errorResponse: LakeRPCResponse = {
 *   id: '1',
 *   error: { code: 'INVALID_SQL', message: 'Syntax error near...' },
 * };
 * ```
 *
 * @internal
 */
export interface LakeRPCResponse<T = unknown> {
  /** Request ID this response corresponds to */
  id: string;
  /** The result payload (present on success) */
  result?: T;
  /** Error details (present on failure) */
  error?: LakeRPCError;
}

/**
 * RPC error details.
 *
 * @description Structured error information returned when an RPC call fails.
 *
 * @example
 * ```typescript
 * const error: LakeRPCError = {
 *   code: 'TABLE_NOT_FOUND',
 *   message: 'Table "orders" does not exist',
 *   details: { tableName: 'orders' },
 * };
 * ```
 *
 * @internal
 */
export interface LakeRPCError {
  /** Machine-readable error code */
  code: string;
  /** Human-readable error message */
  message: string;
  /** Additional error context (varies by error type) */
  details?: unknown;
}

// =============================================================================
// Client Interface
// =============================================================================

/**
 * Main client interface for interacting with a DoLake lakehouse.
 *
 * @description Provides methods for querying data, subscribing to CDC streams,
 * managing partitions, triggering compaction, and time travel operations.
 *
 * @example
 * ```typescript
 * import { createLakeClient } from '@dotdo/lake.do';
 *
 * const client = createLakeClient({
 *   url: 'https://lake.example.com',
 *   token: 'your-token',
 * });
 *
 * // Query data
 * const result = await client.query<{ id: string; amount: number }>(
 *   'SELECT id, amount FROM orders WHERE status = $1',
 * );
 *
 * // Subscribe to changes
 * for await (const batch of client.subscribe({ tables: ['orders'] })) {
 *   for (const event of batch.events) {
 *     console.log(event.operation, event.after);
 *   }
 * }
 *
 * // Clean up
 * await client.close();
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface LakeClient {
  /**
   * Executes a SQL query against the lakehouse.
   *
   * @description Runs a SQL query and returns the results. Supports time travel,
   * partition pruning, and column projection through query options.
   *
   * @typeParam T - The expected row type in the result set
   *
   * @param sql - The SQL query to execute
   * @param options - Optional query configuration (time travel, partitions, etc.)
   * @returns Query results including rows and execution statistics
   * @throws {LakeError} When the query fails (syntax error, table not found, etc.)
   *
   * @example
   * ```typescript
   * // Simple query
   * const result = await client.query('SELECT * FROM orders LIMIT 10');
   *
   * // Typed query with time travel
   * interface Order { id: string; amount: number; }
   * const historical = await client.query<Order>(
   *   'SELECT * FROM orders',
   *   { asOf: new Date('2024-01-01') }
   * );
   * ```
   */
  query<T = Record<string, unknown>>(
    sql: string,
    options?: LakeQueryOptions
  ): Promise<LakeQueryResult<T>>;

  /**
   * Subscribes to a CDC (Change Data Capture) event stream.
   *
   * @description Creates an async iterable that yields batches of CDC events
   * as they occur. The stream continues until explicitly closed or an error occurs.
   *
   * @param options - Optional stream configuration (tables, operations, etc.)
   * @returns An async iterable that yields CDC event batches
   * @throws {LakeError} When subscription fails or connection is lost
   *
   * @example
   * ```typescript
   * // Subscribe to all changes
   * for await (const batch of client.subscribe()) {
   *   for (const event of batch.events) {
   *     console.log(`${event.operation} on ${event.table}`);
   *   }
   * }
   *
   * // Subscribe to specific tables and operations
   * for await (const batch of client.subscribe({
   *   tables: ['orders', 'inventory'],
   *   operations: ['INSERT', 'UPDATE'],
   * })) {
   *   processBatch(batch);
   * }
   * ```
   */
  subscribe(options?: CDCStreamOptions): AsyncIterable<CDCBatch>;

  /**
   * Retrieves metadata for a table.
   *
   * @description Returns complete metadata including schema, partition spec,
   * snapshots, and custom properties.
   *
   * @param tableName - The name of the table
   * @returns Table metadata including schema, partitioning, and snapshots
   * @throws {LakeError} When the table is not found
   *
   * @example
   * ```typescript
   * const metadata = await client.getMetadata('orders');
   * console.log(`Table has ${metadata.schema.columns.length} columns`);
   * console.log(`Partitioned by: ${metadata.partitionSpec.column}`);
   * ```
   */
  getMetadata(tableName: string): Promise<TableMetadata>;

  /**
   * Lists all partitions for a table.
   *
   * @description Returns information about each partition including file count,
   * row count, and size. Useful for monitoring and deciding when to compact.
   *
   * @param tableName - The name of the table
   * @returns Array of partition information objects
   * @throws {LakeError} When the table is not found
   *
   * @example
   * ```typescript
   * const partitions = await client.listPartitions('orders');
   * for (const p of partitions) {
   *   if (p.fileCount > 10) {
   *     console.log(`Partition ${p.key} needs compaction`);
   *   }
   * }
   * ```
   */
  listPartitions(tableName: string): Promise<PartitionInfo[]>;

  /**
   * Triggers a compaction job for a partition.
   *
   * @description Initiates a background job to merge small files into larger,
   * more efficient files. Returns immediately with job metadata.
   *
   * @param partition - The partition key to compact
   * @param config - Optional compaction configuration
   * @returns The created compaction job with initial status
   * @throws {LakeError} When the partition is not found or compaction fails to start
   *
   * @example
   * ```typescript
   * const partitions = await client.listPartitions('orders');
   * const partition = partitions.find(p => p.fileCount > 10);
   *
   * if (partition) {
   *   const job = await client.compact(partition.key, {
   *     targetFileSize: 128 * 1024 * 1024,
   *   });
   *   console.log(`Started compaction job: ${job.id}`);
   * }
   * ```
   */
  compact(partition: PartitionKey, config?: Partial<CompactionConfig>): Promise<CompactionJob>;

  /**
   * Gets the current status of a compaction job.
   *
   * @description Retrieves the latest status of a running or completed compaction job.
   * Use this to poll for job completion.
   *
   * @param jobId - The compaction job ID
   * @returns Current job status and metadata
   * @throws {LakeError} When the job is not found
   *
   * @example
   * ```typescript
   * const job = await client.compact(partitionKey);
   *
   * // Poll until complete
   * let status = job;
   * while (status.status === 'pending' || status.status === 'running') {
   *   await new Promise(r => setTimeout(r, 1000));
   *   status = await client.getCompactionStatus(job.id);
   * }
   *
   * console.log(`Compaction ${status.status}`);
   * ```
   */
  getCompactionStatus(jobId: CompactionJobId): Promise<CompactionJob>;

  /**
   * Lists all snapshots for a table.
   *
   * @description Returns the history of snapshots for time travel queries.
   * Snapshots are ordered from newest to oldest.
   *
   * @param tableName - The name of the table
   * @returns Array of snapshots ordered by timestamp descending
   * @throws {LakeError} When the table is not found
   *
   * @example
   * ```typescript
   * const snapshots = await client.listSnapshots('orders');
   * console.log(`Table has ${snapshots.length} snapshots`);
   *
   * // Query at oldest snapshot
   * const oldest = snapshots[snapshots.length - 1];
   * const result = await client.query('SELECT COUNT(*) FROM orders', {
   *   asOf: oldest.id,
   * });
   * ```
   */
  listSnapshots(tableName: string): Promise<Snapshot[]>;

  /**
   * Checks the connection health and measures latency.
   *
   * @description Sends a ping to the server and measures round-trip time.
   * Useful for health checks and connection monitoring.
   *
   * @returns Object containing latency in milliseconds
   * @throws {LakeError} When the connection is not available
   *
   * @example
   * ```typescript
   * const { latency } = await client.ping();
   * console.log(`Connection latency: ${latency}ms`);
   * ```
   */
  ping(): Promise<{ latency: number }>;

  /**
   * Closes the client connection and cleans up resources.
   *
   * @description Closes the WebSocket connection and cancels any pending requests.
   * Always call this when done with the client to prevent resource leaks.
   *
   * @returns Resolves when the connection is fully closed
   *
   * @example
   * ```typescript
   * const client = createLakeClient({ url: 'https://lake.example.com' });
   * try {
   *   // Use client...
   * } finally {
   *   await client.close();
   * }
   * ```
   */
  close(): Promise<void>;
}

// =============================================================================
// Metrics Types
// =============================================================================

/**
 * Metrics for monitoring lakehouse operations.
 *
 * @description Aggregated statistics about lakehouse operations including
 * CDC streaming, file operations, compaction, and query execution.
 *
 * @example
 * ```typescript
 * const metrics: LakeMetrics = {
 *   cdcEventsReceived: 10000,
 *   cdcBytesReceived: 5242880,
 *   parquetFilesWritten: 50,
 *   parquetBytesWritten: 536870912,
 *   compactionsCompleted: 10,
 *   queriesExecuted: 1000,
 *   queryBytesScanned: 10737418240,
 * };
 *
 * console.log(`Processed ${metrics.cdcEventsReceived} CDC events`);
 * console.log(`Written ${metrics.parquetBytesWritten / 1024 / 1024} MB to storage`);
 * ```
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export interface LakeMetrics {
  /** Total CDC events received */
  cdcEventsReceived: number;
  /** Total bytes received via CDC stream */
  cdcBytesReceived: number;
  /** Total Parquet files written to storage */
  parquetFilesWritten: number;
  /** Total bytes written to Parquet files */
  parquetBytesWritten: number;
  /** Total compaction jobs completed */
  compactionsCompleted: number;
  /** Total queries executed */
  queriesExecuted: number;
  /** Total bytes scanned across all queries */
  queryBytesScanned: number;
}
