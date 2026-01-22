/**
 * @dotdo/lake.do - Shared types for DoLake client and server
 *
 * This module re-exports types from @dotdo/sql.do (which uses @dotdo/shared-types)
 * and adds lake-specific type definitions.
 */

// =============================================================================
// Branded Types for Lake.do
// =============================================================================

declare const CDCEventIdBrand: unique symbol;
declare const PartitionKeyBrand: unique symbol;
declare const ParquetFileIdBrand: unique symbol;
declare const SnapshotIdBrand: unique symbol;
declare const CompactionJobIdBrand: unique symbol;

export type CDCEventId = string & { readonly [CDCEventIdBrand]: never };
export type PartitionKey = string & { readonly [PartitionKeyBrand]: never };
export type ParquetFileId = string & { readonly [ParquetFileIdBrand]: never };
export type SnapshotId = string & { readonly [SnapshotIdBrand]: never };
export type CompactionJobId = string & { readonly [CompactionJobIdBrand]: never };

export function createCDCEventId(id: string): CDCEventId {
  return id as CDCEventId;
}

export function createPartitionKey(key: string): PartitionKey {
  return key as PartitionKey;
}

export function createParquetFileId(id: string): ParquetFileId {
  return id as ParquetFileId;
}

export function createSnapshotId(id: string): SnapshotId {
  return id as SnapshotId;
}

export function createCompactionJobId(id: string): CompactionJobId {
  return id as CompactionJobId;
}

// =============================================================================
// Re-export common types from sql.do (which uses shared-types)
// =============================================================================

export type {
  TransactionId,
  LSN,
  SQLValue,
  CDCOperation,
  CDCEvent,
  ClientCDCOperation,
  ClientCapabilities,
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
} from '@dotdo/sql.do';

// =============================================================================
// CDC Streaming Types
// =============================================================================

import type { CDCEvent, LSN } from '@dotdo/sql.do';

export interface CDCStreamOptions {
  /** Starting LSN (exclusive) */
  fromLSN?: bigint | LSN;
  /** Tables to subscribe to (empty = all) */
  tables?: string[];
  /** Operations to subscribe to */
  operations?: Array<'INSERT' | 'UPDATE' | 'DELETE'>;
  /** Batch size for CDC events */
  batchSize?: number;
  /** Batch timeout in milliseconds */
  batchTimeoutMs?: number;
}

export interface CDCBatch {
  events: CDCEvent[];
  sequenceNumber: number;
  timestamp: Date;
  sourceDoId: string;
}

export interface CDCStreamState {
  connected: boolean;
  lastLSN: bigint;
  eventsReceived: number;
  bytesReceived: number;
}

// =============================================================================
// Query Types
// =============================================================================

export interface LakeQueryOptions {
  /** Query specific partitions */
  partitions?: PartitionKey[];
  /** Time travel to specific snapshot */
  asOf?: Date | SnapshotId;
  /** Limit rows returned */
  limit?: number;
  /** Skip rows */
  offset?: number;
  /** Column projection */
  columns?: string[];
}

export interface LakeQueryResult<T = Record<string, unknown>> {
  rows: T[];
  columns: string[];
  rowCount: number;
  bytesScanned: number;
  filesScanned: number;
  partitionsScanned: number;
  duration: number;
}

// =============================================================================
// Partition Types
// =============================================================================

export type PartitionStrategy = 'time' | 'hash' | 'list' | 'range';

export interface PartitionConfig {
  strategy: PartitionStrategy;
  column: string;
  /** For time partitioning: 'hourly' | 'daily' | 'monthly' */
  granularity?: 'hourly' | 'daily' | 'monthly';
  /** For hash partitioning: number of buckets */
  buckets?: number;
}

export interface PartitionInfo {
  key: PartitionKey;
  strategy: PartitionStrategy;
  range?: { min: unknown; max: unknown };
  fileCount: number;
  rowCount: number;
  sizeBytes: number;
  lastModified: Date;
}

// =============================================================================
// Compaction Types
// =============================================================================

export interface CompactionConfig {
  /** Target file size in bytes */
  targetFileSize: number;
  /** Minimum files to trigger compaction */
  minFiles: number;
  /** Maximum files to compact at once */
  maxFiles: number;
  /** Sort order for compacted files */
  sortBy?: string[];
}

export interface CompactionJob {
  id: CompactionJobId;
  partition: PartitionKey;
  status: 'pending' | 'running' | 'completed' | 'failed';
  inputFiles: ParquetFileId[];
  outputFiles?: ParquetFileId[];
  startedAt?: Date;
  completedAt?: Date;
  error?: string;
  bytesRead?: number;
  bytesWritten?: number;
}

// =============================================================================
// Snapshot Types
// =============================================================================

export interface Snapshot {
  id: SnapshotId;
  timestamp: Date;
  parentId?: SnapshotId;
  summary: {
    addedFiles: number;
    deletedFiles: number;
    addedRows: number;
    deletedRows: number;
  };
  manifestList: string;
}

export interface TimeTravelOptions {
  /** Specific snapshot ID */
  snapshotId?: SnapshotId;
  /** Point in time */
  asOf?: Date;
  /** Relative time (e.g., '1 hour ago') */
  before?: string;
}

// =============================================================================
// Schema Types
// =============================================================================

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

export interface LakeColumn {
  id: number;
  name: string;
  type: LakeColumnType;
  required: boolean;
  doc?: string;
}

export interface LakeSchema {
  schemaId: number;
  columns: LakeColumn[];
  identifierFields?: number[];
}

export interface TableMetadata {
  tableId: string;
  schema: LakeSchema;
  partitionSpec: PartitionConfig;
  currentSnapshotId?: SnapshotId;
  snapshots: Snapshot[];
  properties: Record<string, string>;
}

// =============================================================================
// RPC Message Types
// =============================================================================

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

export interface LakeRPCRequest {
  id: string;
  method: LakeRPCMethod;
  params: unknown;
}

export interface LakeRPCResponse<T = unknown> {
  id: string;
  result?: T;
  error?: LakeRPCError;
}

export interface LakeRPCError {
  code: string;
  message: string;
  details?: unknown;
}

// =============================================================================
// Client Interface
// =============================================================================

export interface LakeClient {
  /**
   * Query lakehouse data
   */
  query<T = Record<string, unknown>>(
    sql: string,
    options?: LakeQueryOptions
  ): Promise<LakeQueryResult<T>>;

  /**
   * Subscribe to CDC stream
   */
  subscribe(options?: CDCStreamOptions): AsyncIterable<CDCBatch>;

  /**
   * Get table metadata
   */
  getMetadata(tableName: string): Promise<TableMetadata>;

  /**
   * List partitions
   */
  listPartitions(tableName: string): Promise<PartitionInfo[]>;

  /**
   * Trigger compaction
   */
  compact(partition: PartitionKey, config?: Partial<CompactionConfig>): Promise<CompactionJob>;

  /**
   * Get compaction job status
   */
  getCompactionStatus(jobId: CompactionJobId): Promise<CompactionJob>;

  /**
   * List snapshots for time travel
   */
  listSnapshots(tableName: string): Promise<Snapshot[]>;

  /**
   * Check connection health
   */
  ping(): Promise<{ latency: number }>;

  /**
   * Close connection
   */
  close(): Promise<void>;
}

// =============================================================================
// Metrics Types
// =============================================================================

export interface LakeMetrics {
  cdcEventsReceived: number;
  cdcBytesReceived: number;
  parquetFilesWritten: number;
  parquetBytesWritten: number;
  compactionsCompleted: number;
  queriesExecuted: number;
  queryBytesScanned: number;
}
