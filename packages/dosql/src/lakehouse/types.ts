/**
 * Lakehouse Types for DoSQL
 *
 * Core type definitions for DO-to-lakehouse CDC streaming.
 * Supports streaming CDC events from multiple DOs to a shared lakehouse in R2.
 */

import type { WALEntry, WALOperation } from '../wal/types.js';
import type { ColumnarTableSchema, ColumnStats, RowGroup } from '../columnar/types.js';

// =============================================================================
// Lakehouse Configuration
// =============================================================================

/**
 * Configuration for the lakehouse
 */
export interface LakehouseConfig {
  /** R2 bucket name */
  bucket: string;
  /** Path prefix for all lakehouse data */
  prefix: string;
  /** Columns to partition by (e.g., ['date', 'tenantId']) */
  partitionBy: string[];
  /** Target chunk size in bytes (default: 64MB for efficient analytics) */
  targetChunkSize: number;
  /** Maximum rows per chunk (default: 1M rows) */
  maxRowsPerChunk: number;
  /** Flush interval in milliseconds (default: 60000 = 1 minute) */
  flushIntervalMs: number;
  /** Enable compression (default: true) */
  compression?: boolean;
  /** Manifest update strategy */
  manifestStrategy?: 'immediate' | 'batched';
  /** Number of snapshots to retain (default: 100) */
  snapshotRetention?: number;
}

/**
 * Default lakehouse configuration
 */
export const DEFAULT_LAKEHOUSE_CONFIG: Required<LakehouseConfig> = {
  bucket: '',
  prefix: 'lakehouse/',
  partitionBy: [],
  targetChunkSize: 64 * 1024 * 1024, // 64MB
  maxRowsPerChunk: 1_000_000,
  flushIntervalMs: 60_000,
  compression: true,
  manifestStrategy: 'immediate',
  snapshotRetention: 100,
};

// =============================================================================
// Partition Types
// =============================================================================

/**
 * Partition column type
 */
export type PartitionColumnType = 'string' | 'int' | 'date' | 'timestamp';

/**
 * Partition column definition
 */
export interface PartitionColumn {
  /** Column name */
  name: string;
  /** Column type for partitioning */
  type: PartitionColumnType;
  /** Transform function (e.g., 'year', 'month', 'day', 'hour' for timestamps) */
  transform?: 'year' | 'month' | 'day' | 'hour' | 'identity' | 'bucket' | 'truncate';
  /** Transform argument (e.g., bucket count, truncate length) */
  transformArg?: number;
}

/**
 * Partition key value
 */
export interface PartitionKey {
  /** Column name */
  column: string;
  /** Partition value (formatted according to transform) */
  value: string;
}

/**
 * Represents a specific partition in the lakehouse
 */
export interface Partition {
  /** Partition keys defining this partition */
  keys: PartitionKey[];
  /** Path to this partition (e.g., 'year=2024/month=01/tenantId=abc') */
  path: string;
  /** Chunks in this partition */
  chunks: ChunkMetadata[];
  /** Total row count in this partition */
  rowCount: number;
  /** Total byte size of this partition */
  byteSize: number;
  /** When the partition was created */
  createdAt: number;
  /** When the partition was last modified */
  modifiedAt: number;
}

// =============================================================================
// Chunk Types
// =============================================================================

/**
 * Metadata about a single chunk file
 */
export interface ChunkMetadata {
  /** Unique chunk identifier */
  id: string;
  /** Full path to the chunk file in R2 */
  path: string;
  /** Number of rows in this chunk */
  rowCount: number;
  /** Size in bytes */
  byteSize: number;
  /** Per-column statistics for predicate pushdown */
  columnStats: Record<string, ColumnStats>;
  /** Format of the chunk (columnar) */
  format: 'columnar';
  /** Compression used (if any) */
  compression?: 'none' | 'gzip' | 'zstd';
  /** Minimum LSN included in this chunk */
  minLSN: bigint;
  /** Maximum LSN included in this chunk */
  maxLSN: bigint;
  /** Source DO IDs that contributed to this chunk */
  sourceDOs: string[];
  /** Creation timestamp */
  createdAt: number;
  /** Checksum for integrity verification */
  checksum?: string;
}

/**
 * A chunk being built (buffering data)
 */
export interface ChunkBuilder {
  /** Table name */
  table: string;
  /** Partition keys */
  partitionKeys: PartitionKey[];
  /** Buffered rows */
  rows: Record<string, unknown>[];
  /** Current estimated size in bytes */
  estimatedSize: number;
  /** Minimum LSN in buffer */
  minLSN: bigint;
  /** Maximum LSN in buffer */
  maxLSN: bigint;
  /** Source DO IDs */
  sourceDOs: Set<string>;
  /** When buffering started */
  startedAt: number;
}

// =============================================================================
// Manifest Types
// =============================================================================

/**
 * Table manifest - tracks all data for a single table
 */
export interface TableManifest {
  /** Table name */
  tableName: string;
  /** Table schema */
  schema: ColumnarTableSchema;
  /** Partition columns for this table */
  partitionColumns: PartitionColumn[];
  /** All partitions for this table */
  partitions: Map<string, Partition>;
  /** Total row count across all partitions */
  totalRowCount: number;
  /** Total byte size across all partitions */
  totalByteSize: number;
  /** Schema version for evolution tracking */
  schemaVersion: number;
  /** When the table was created */
  createdAt: number;
  /** When the table was last modified */
  modifiedAt: number;
}

/**
 * Snapshot represents a consistent point-in-time view of the lakehouse
 */
export interface Snapshot {
  /** Unique snapshot identifier */
  id: string;
  /** Sequential snapshot number */
  sequenceNumber: number;
  /** Parent snapshot ID (for lineage) */
  parentId: string | null;
  /** Timestamp when snapshot was created */
  timestamp: number;
  /** Summary of what changed in this snapshot */
  summary: SnapshotSummary;
  /** The manifest state at this snapshot */
  manifestPath: string;
  /** Operation that created this snapshot */
  operation: SnapshotOperation;
}

/**
 * Summary of changes in a snapshot
 */
export interface SnapshotSummary {
  /** Tables that were modified */
  tablesModified: string[];
  /** Number of chunks added */
  chunksAdded: number;
  /** Number of chunks removed (compaction) */
  chunksRemoved: number;
  /** Rows added */
  rowsAdded: number;
  /** Rows removed */
  rowsRemoved: number;
  /** Bytes added */
  bytesAdded: number;
  /** Bytes removed */
  bytesRemoved: number;
}

/**
 * Operation type for snapshot creation
 */
export type SnapshotOperation =
  | { type: 'append'; tables: string[] }
  | { type: 'compact'; tables: string[] }
  | { type: 'delete'; predicate: string }
  | { type: 'schema-evolution'; table: string; changes: string[] }
  | { type: 'rollback'; targetSnapshotId: string };

/**
 * The root lakehouse manifest
 */
export interface LakehouseManifest {
  /** Manifest format version */
  formatVersion: number;
  /** Lakehouse identifier */
  lakehouseId: string;
  /** All snapshots (ordered by sequence number) */
  snapshots: Snapshot[];
  /** Current snapshot ID */
  currentSnapshotId: string;
  /** Table manifests (keyed by table name) */
  tables: Record<string, TableManifest>;
  /** Manifest update timestamp */
  updatedAt: number;
  /** Optimistic locking version */
  version: number;
  /** Properties/metadata */
  properties: Record<string, string>;
}

// =============================================================================
// CDC Aggregation Types
// =============================================================================

/**
 * CDC event from a single DO
 */
export interface DOCDCEvent {
  /** Source DO identifier */
  doId: string;
  /** Shard ID (if applicable) */
  shardId?: string;
  /** Original WAL entry */
  entry: WALEntry;
  /** Decoded record data */
  data: Record<string, unknown>;
  /** Timestamp when event was received */
  receivedAt: number;
}

/**
 * Aggregated CDC event (after deduplication and ordering)
 */
export interface AggregatedCDCEvent {
  /** Global ordering key (combines DO + LSN) */
  globalLSN: string;
  /** All source DO IDs (for deduplication tracking) */
  sourceDOs: string[];
  /** The canonical event data */
  event: DOCDCEvent;
  /** Whether this is a deduplicated event */
  isDeduplicated: boolean;
}

/**
 * Deduplication key for CDC events
 */
export interface DeduplicationKey {
  /** Table name */
  table: string;
  /** Primary key value (serialized) */
  primaryKey: string;
  /** Transaction ID */
  txnId: string;
}

/**
 * Cursor position for CDC streaming
 */
export interface CDCCursor {
  /** Per-DO LSN positions */
  doPositions: Map<string, bigint>;
  /** Last processed global LSN */
  lastGlobalLSN: string;
  /** Timestamp of last update */
  updatedAt: number;
}

/**
 * Aggregator state
 */
export interface AggregatorState {
  /** Current cursor position */
  cursor: CDCCursor;
  /** Pending events (buffered but not yet written) */
  pendingEvents: AggregatedCDCEvent[];
  /** Deduplication window (recent keys for dedup) */
  deduplicationWindow: Map<string, number>;
  /** Active DOs being tracked */
  activeDOs: Set<string>;
  /** Statistics */
  stats: AggregatorStats;
}

/**
 * Aggregator statistics
 */
export interface AggregatorStats {
  /** Total events processed */
  eventsProcessed: number;
  /** Events deduplicated */
  eventsDeduplicated: number;
  /** Events written to lakehouse */
  eventsWritten: number;
  /** Chunks created */
  chunksCreated: number;
  /** Processing lag (max DO lag) */
  maxLagMs: number;
  /** Last flush time */
  lastFlushAt: number;
}

// =============================================================================
// Query Types
// =============================================================================

/**
 * Lakehouse query request
 */
export interface LakehouseQuery {
  /** SQL query string */
  sql: string;
  /** Query parameters */
  params?: unknown[];
  /** Target snapshot ID (for time travel, defaults to current) */
  snapshotId?: string;
  /** Query hints */
  hints?: QueryHints;
}

/**
 * Query hints for optimization
 */
export interface QueryHints {
  /** Force specific partitions */
  partitions?: PartitionKey[][];
  /** Disable partition pruning */
  disablePartitionPruning?: boolean;
  /** Disable predicate pushdown */
  disablePredicatePushdown?: boolean;
  /** Maximum chunks to scan */
  maxChunks?: number;
  /** Timeout in milliseconds */
  timeoutMs?: number;
}

/**
 * Query execution plan
 */
export interface QueryPlan {
  /** Parsed SQL */
  sql: string;
  /** Tables involved */
  tables: string[];
  /** Partitions to scan (after pruning) */
  partitionsToScan: string[];
  /** Chunks to scan (after predicate pushdown) */
  chunksToScan: ChunkMetadata[];
  /** Predicates that can be pushed down */
  pushdownPredicates: PushdownPredicate[];
  /** Columns to project */
  projection: string[];
  /** Estimated rows to scan */
  estimatedRows: number;
  /** Estimated bytes to scan */
  estimatedBytes: number;
}

/**
 * Predicate that can be pushed to chunk level
 */
export interface PushdownPredicate {
  /** Column name */
  column: string;
  /** Operator */
  op: 'eq' | 'ne' | 'lt' | 'le' | 'gt' | 'ge' | 'in' | 'between' | 'is_null' | 'is_not_null';
  /** Value(s) */
  value?: unknown;
  /** Second value for between */
  value2?: unknown;
}

/**
 * Query result
 */
export interface QueryResult<T = Record<string, unknown>> {
  /** Result rows */
  rows: T[];
  /** Column names */
  columns: string[];
  /** Total row count (may be approximate for large results) */
  rowCount: number;
  /** Query execution stats */
  stats: QueryStats;
}

/**
 * Query execution statistics
 */
export interface QueryStats {
  /** Execution time in milliseconds */
  executionTimeMs: number;
  /** Partitions scanned */
  partitionsScanned: number;
  /** Chunks scanned */
  chunksScanned: number;
  /** Chunks skipped (by predicate pushdown) */
  chunksSkipped: number;
  /** Rows scanned */
  rowsScanned: number;
  /** Bytes scanned */
  bytesScanned: number;
}

// =============================================================================
// R2 Interface
// =============================================================================

/**
 * R2 bucket interface (subset of Cloudflare R2)
 */
export interface R2Bucket {
  /** Get an object from R2 */
  get(key: string): Promise<R2Object | null>;
  /** Put an object to R2 */
  put(key: string, data: ArrayBuffer | Uint8Array | ReadableStream, options?: R2PutOptions): Promise<R2Object>;
  /** Delete an object from R2 */
  delete(key: string): Promise<void>;
  /** List objects with a prefix */
  list(options?: R2ListOptions): Promise<R2ListResult>;
  /** Head (metadata only) */
  head(key: string): Promise<R2Object | null>;
}

/**
 * R2 object
 */
export interface R2Object {
  key: string;
  size: number;
  etag: string;
  httpMetadata?: Record<string, string>;
  customMetadata?: Record<string, string>;
  uploaded: Date;
  arrayBuffer(): Promise<ArrayBuffer>;
  body?: ReadableStream;
}

/**
 * R2 put options
 */
export interface R2PutOptions {
  httpMetadata?: Record<string, string>;
  customMetadata?: Record<string, string>;
}

/**
 * R2 list options
 */
export interface R2ListOptions {
  prefix?: string;
  delimiter?: string;
  cursor?: string;
  limit?: number;
}

/**
 * R2 list result
 */
export interface R2ListResult {
  objects: R2Object[];
  truncated: boolean;
  cursor?: string;
  delimitedPrefixes?: string[];
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * Lakehouse-specific error codes
 */
export enum LakehouseErrorCode {
  /** Configuration error */
  CONFIG_ERROR = 'LAKEHOUSE_CONFIG_ERROR',
  /** Manifest not found */
  MANIFEST_NOT_FOUND = 'LAKEHOUSE_MANIFEST_NOT_FOUND',
  /** Manifest update conflict (optimistic locking) */
  MANIFEST_CONFLICT = 'LAKEHOUSE_MANIFEST_CONFLICT',
  /** Snapshot not found */
  SNAPSHOT_NOT_FOUND = 'LAKEHOUSE_SNAPSHOT_NOT_FOUND',
  /** Chunk not found */
  CHUNK_NOT_FOUND = 'LAKEHOUSE_CHUNK_NOT_FOUND',
  /** Chunk corrupted */
  CHUNK_CORRUPTED = 'LAKEHOUSE_CHUNK_CORRUPTED',
  /** Schema mismatch */
  SCHEMA_MISMATCH = 'LAKEHOUSE_SCHEMA_MISMATCH',
  /** Query error */
  QUERY_ERROR = 'LAKEHOUSE_QUERY_ERROR',
  /** R2 operation failed */
  R2_ERROR = 'LAKEHOUSE_R2_ERROR',
  /** Deduplication error */
  DEDUP_ERROR = 'LAKEHOUSE_DEDUP_ERROR',
  /** Aggregation error */
  AGGREGATION_ERROR = 'LAKEHOUSE_AGGREGATION_ERROR',
}

/**
 * Custom error class for lakehouse operations
 */
export class LakehouseError extends Error {
  constructor(
    public readonly code: LakehouseErrorCode,
    message: string,
    public readonly details?: Record<string, unknown>,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'LakehouseError';
  }
}

// =============================================================================
// Utility Types
// =============================================================================

/**
 * JSON-serializable version of bigint-containing types
 */
export interface SerializedChunkMetadata extends Omit<ChunkMetadata, 'minLSN' | 'maxLSN'> {
  minLSN: string;
  maxLSN: string;
}

/**
 * Serialization helpers
 */
export function serializeChunkMetadata(chunk: ChunkMetadata): SerializedChunkMetadata {
  return {
    ...chunk,
    minLSN: chunk.minLSN.toString(),
    maxLSN: chunk.maxLSN.toString(),
  };
}

export function deserializeChunkMetadata(data: SerializedChunkMetadata): ChunkMetadata {
  return {
    ...data,
    minLSN: BigInt(data.minLSN),
    maxLSN: BigInt(data.maxLSN),
  };
}

/**
 * Generate a unique chunk ID
 */
export function generateChunkId(table: string, partition: string, sequence: number): string {
  const timestamp = Date.now().toString(36);
  const rand = Math.random().toString(36).substring(2, 8);
  return `${table}-${partition.replace(/\//g, '-')}-${sequence.toString(36)}-${timestamp}-${rand}`;
}

/**
 * Generate a snapshot ID
 */
export function generateSnapshotId(): string {
  const timestamp = Date.now().toString(36);
  const rand = Math.random().toString(36).substring(2, 10);
  return `snap-${timestamp}-${rand}`;
}

/**
 * Parse partition path into partition keys
 */
export function parsePartitionPath(path: string): PartitionKey[] {
  const parts = path.split('/').filter(Boolean);
  return parts.map(part => {
    const [column, value] = part.split('=');
    return { column, value };
  });
}

/**
 * Build partition path from partition keys
 */
export function buildPartitionPath(keys: PartitionKey[]): string {
  return keys.map(k => `${k.column}=${k.value}`).join('/');
}
