/**
 * lake.do - Data Transformation Utilities
 *
 * This module provides functions for transforming raw RPC responses
 * into properly typed client-side objects with branded types and Date objects.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

import type {
  Snapshot,
  PartitionInfo,
  CompactionJob,
  TableMetadata,
} from './types.js';
import {
  createSnapshotId,
  createPartitionKey,
  createCompactionJobId,
  isSnapshotId,
  isPartitionKey,
  isCompactionJobId,
} from './branded-types.js';

// =============================================================================
// Raw Response Types (from RPC)
// =============================================================================

/**
 * Raw snapshot data from RPC response before transformation.
 *
 * @internal
 */
export interface RawSnapshot {
  id: unknown;
  timestamp: string | number | Date;
  parentId?: unknown;
  summary: {
    addedFiles: number;
    deletedFiles: number;
    addedRows: number;
    deletedRows: number;
  };
  manifestList: string;
}

/**
 * Raw partition info from RPC response before transformation.
 *
 * @internal
 */
export interface RawPartitionInfo {
  key: unknown;
  strategy: 'time' | 'hash' | 'list' | 'range';
  range?: { min: unknown; max: unknown };
  fileCount: number;
  rowCount: number;
  sizeBytes: number;
  lastModified: string | number | Date;
}

/**
 * Raw compaction job from RPC response before transformation.
 *
 * @internal
 */
export interface RawCompactionJob {
  id: unknown;
  partition: unknown;
  status: 'pending' | 'running' | 'completed' | 'failed';
  inputFiles: unknown[];
  outputFiles?: unknown[];
  error?: string;
  bytesRead?: number;
  bytesWritten?: number;
  startedAt?: string | number | Date;
  completedAt?: string | number | Date;
}

/**
 * Raw table metadata from RPC response before transformation.
 *
 * @internal
 */
export interface RawTableMetadata {
  tableId: string;
  schema: TableMetadata['schema'];
  partitionSpec: TableMetadata['partitionSpec'];
  currentSnapshotId?: unknown;
  snapshots: RawSnapshot[];
  properties: Record<string, string>;
}

// =============================================================================
// Type Guards for Raw Data
// =============================================================================

/**
 * Type guard to check if a value can be converted to a string for branded types.
 *
 * @param value - The value to check
 * @returns True if the value is a non-empty string
 *
 * @internal
 */
export function isValidStringId(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

/**
 * Type guard to check if a value is a valid date-like value.
 *
 * @param value - The value to check
 * @returns True if the value can be converted to a Date
 *
 * @internal
 */
export function isValidDateLike(value: unknown): value is string | number | Date {
  if (value instanceof Date) return !isNaN(value.getTime());
  if (typeof value === 'string') return !isNaN(new Date(value).getTime());
  if (typeof value === 'number') return !isNaN(new Date(value).getTime());
  return false;
}

// =============================================================================
// Transformation Functions
// =============================================================================

/**
 * Transforms a raw snapshot from RPC response into a properly typed Snapshot.
 *
 * @description Converts raw ID strings to branded SnapshotId types and
 * timestamp strings/numbers to Date objects.
 *
 * @param raw - The raw snapshot data from RPC
 * @returns A properly typed Snapshot object
 * @throws {Error} If the snapshot ID is not a valid string
 *
 * @example
 * ```typescript
 * const raw = { id: 'snap_123', timestamp: '2024-01-15T00:00:00Z', ... };
 * const snapshot = transformSnapshot(raw);
 * // snapshot.id is SnapshotId, snapshot.timestamp is Date
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function transformSnapshot(raw: RawSnapshot): Snapshot {
  if (!isValidStringId(raw.id)) {
    throw new Error(`Invalid snapshot ID: expected non-empty string, got ${typeof raw.id}`);
  }

  const snapshot: Snapshot = {
    id: createSnapshotId(raw.id),
    timestamp: new Date(raw.timestamp),
    summary: raw.summary,
    manifestList: raw.manifestList,
  };

  if (raw.parentId !== undefined && raw.parentId !== null) {
    if (!isValidStringId(raw.parentId)) {
      throw new Error(`Invalid parent snapshot ID: expected non-empty string, got ${typeof raw.parentId}`);
    }
    snapshot.parentId = createSnapshotId(raw.parentId);
  }

  return snapshot;
}

/**
 * Transforms an array of raw snapshots.
 *
 * @param raws - The array of raw snapshot data
 * @returns An array of properly typed Snapshot objects
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function transformSnapshots(raws: RawSnapshot[]): Snapshot[] {
  return raws.map(transformSnapshot);
}

/**
 * Transforms raw partition info from RPC response into a properly typed PartitionInfo.
 *
 * @description Converts raw key strings to branded PartitionKey types and
 * timestamp strings/numbers to Date objects.
 *
 * @param raw - The raw partition info from RPC
 * @returns A properly typed PartitionInfo object
 * @throws {Error} If the partition key is not a valid string
 *
 * @example
 * ```typescript
 * const raw = { key: 'date=2024-01-15', lastModified: '2024-01-15T12:00:00Z', ... };
 * const partition = transformPartitionInfo(raw);
 * // partition.key is PartitionKey, partition.lastModified is Date
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function transformPartitionInfo(raw: RawPartitionInfo): PartitionInfo {
  if (!isValidStringId(raw.key)) {
    throw new Error(`Invalid partition key: expected non-empty string, got ${typeof raw.key}`);
  }

  return {
    key: createPartitionKey(raw.key),
    strategy: raw.strategy,
    range: raw.range,
    fileCount: raw.fileCount,
    rowCount: raw.rowCount,
    sizeBytes: raw.sizeBytes,
    lastModified: new Date(raw.lastModified),
  };
}

/**
 * Transforms an array of raw partition info objects.
 *
 * @param raws - The array of raw partition info data
 * @returns An array of properly typed PartitionInfo objects
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function transformPartitionInfos(raws: RawPartitionInfo[]): PartitionInfo[] {
  return raws.map(transformPartitionInfo);
}

/**
 * Transforms raw compaction job from RPC response into a properly typed CompactionJob.
 *
 * @description Converts raw ID strings to branded types and
 * timestamp strings/numbers to Date objects.
 *
 * @param raw - The raw compaction job from RPC
 * @returns A properly typed CompactionJob object
 * @throws {Error} If the job ID or partition key is not a valid string
 *
 * @example
 * ```typescript
 * const raw = { id: 'compact_123', partition: 'date=2024-01-15', status: 'running', ... };
 * const job = transformCompactionJob(raw);
 * // job.id is CompactionJobId, job.partition is PartitionKey
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function transformCompactionJob(raw: RawCompactionJob): CompactionJob {
  if (!isValidStringId(raw.id)) {
    throw new Error(`Invalid compaction job ID: expected non-empty string, got ${typeof raw.id}`);
  }
  if (!isValidStringId(raw.partition)) {
    throw new Error(`Invalid partition key: expected non-empty string, got ${typeof raw.partition}`);
  }

  const result: CompactionJob = {
    id: createCompactionJobId(raw.id),
    partition: createPartitionKey(raw.partition),
    status: raw.status,
    inputFiles: raw.inputFiles as CompactionJob['inputFiles'],
  };

  if (raw.outputFiles) {
    result.outputFiles = raw.outputFiles as CompactionJob['outputFiles'];
  }
  if (raw.error) {
    result.error = raw.error;
  }
  if (raw.bytesRead !== undefined) {
    result.bytesRead = raw.bytesRead;
  }
  if (raw.bytesWritten !== undefined) {
    result.bytesWritten = raw.bytesWritten;
  }
  if (raw.startedAt) {
    result.startedAt = new Date(raw.startedAt);
  }
  if (raw.completedAt) {
    result.completedAt = new Date(raw.completedAt);
  }

  return result;
}

/**
 * Transforms raw table metadata from RPC response into a properly typed TableMetadata.
 *
 * @description Converts raw ID strings to branded types, transforms nested snapshots,
 * and converts timestamp strings/numbers to Date objects.
 *
 * @param raw - The raw table metadata from RPC
 * @returns A properly typed TableMetadata object
 *
 * @example
 * ```typescript
 * const raw = { tableId: 'orders', currentSnapshotId: 'snap_123', snapshots: [...], ... };
 * const metadata = transformTableMetadata(raw);
 * // metadata.currentSnapshotId is SnapshotId, metadata.snapshots are Snapshot[]
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function transformTableMetadata(raw: RawTableMetadata): TableMetadata {
  const metadata: TableMetadata = {
    tableId: raw.tableId,
    schema: raw.schema,
    partitionSpec: raw.partitionSpec,
    properties: raw.properties,
    snapshots: transformSnapshots(raw.snapshots),
  };

  if (raw.currentSnapshotId !== undefined && raw.currentSnapshotId !== null) {
    if (!isValidStringId(raw.currentSnapshotId)) {
      throw new Error(`Invalid current snapshot ID: expected non-empty string, got ${typeof raw.currentSnapshotId}`);
    }
    metadata.currentSnapshotId = createSnapshotId(raw.currentSnapshotId);
  }

  return metadata;
}
