/**
 * Compaction Types for DoSQL
 *
 * Core type definitions for the row-to-columnar compaction process.
 * Handles migration of data from B-tree (hot/OLTP) to columnar (cold/OLAP) storage.
 */

import type { BTree, KeyCodec, ValueCodec } from '../btree/types.js';
import type { ColumnarTableSchema, RowGroup } from '../columnar/types.js';
import type { FSXBackend } from '../fsx/types.js';
import type { WALWriter, Checkpoint } from '../wal/types.js';

// =============================================================================
// Configuration
// =============================================================================

/**
 * Schedule modes for compaction
 * - continuous: Run compaction continuously when thresholds are met
 * - scheduled: Run at scheduled intervals (via DO alarms)
 * - manual: Only run when explicitly triggered
 */
export type CompactionSchedule = 'continuous' | 'scheduled' | 'manual';

/**
 * Compaction configuration
 */
export interface CompactionConfig {
  /** Compact when B-tree has this many rows (default: 10000) */
  rowThreshold: number;

  /** Compact rows older than this (ms) (default: 1 hour) */
  ageThreshold: number;

  /** Compact when B-tree size exceeds this (bytes) (default: 10MB) */
  sizeThreshold: number;

  /** Scheduling mode (default: 'continuous') */
  schedule: CompactionSchedule;

  /** Batch size for row scanning (default: 1000) */
  scanBatchSize: number;

  /** Target rows per columnar chunk (default: 65536) */
  targetChunkRows: number;

  /** Target size per columnar chunk in bytes (default: 1MB) */
  targetChunkSize: number;

  /** Minimum interval between compaction runs in ms (default: 5 minutes) */
  minCompactionInterval: number;

  /** Maximum rows to compact in a single run (default: 100000) */
  maxRowsPerRun: number;

  /** Whether to pause during high write load (default: true) */
  pauseDuringHighLoad: boolean;

  /** Write rate threshold (writes/sec) to consider "high load" (default: 100) */
  highLoadThreshold: number;

  /** Key prefix for compaction metadata storage */
  metadataPrefix: string;

  /** Key prefix for columnar chunk storage */
  columnarPrefix: string;
}

/**
 * Default compaction configuration
 */
export const DEFAULT_COMPACTION_CONFIG: CompactionConfig = {
  rowThreshold: 10000,
  ageThreshold: 60 * 60 * 1000, // 1 hour
  sizeThreshold: 10 * 1024 * 1024, // 10MB
  schedule: 'continuous',
  scanBatchSize: 1000,
  targetChunkRows: 65536,
  targetChunkSize: 1 * 1024 * 1024, // 1MB
  minCompactionInterval: 5 * 60 * 1000, // 5 minutes
  maxRowsPerRun: 100000,
  pauseDuringHighLoad: true,
  highLoadThreshold: 100,
  metadataPrefix: '_compaction/',
  columnarPrefix: '_columnar/',
};

// =============================================================================
// Job Types
// =============================================================================

/**
 * Status of a compaction job
 */
export type CompactionJobStatus =
  | 'pending'
  | 'scanning'
  | 'converting'
  | 'writing'
  | 'cleaning'
  | 'completed'
  | 'failed'
  | 'cancelled';

/**
 * A compaction job represents a single compaction operation
 */
export interface CompactionJob {
  /** Unique job identifier */
  id: string;

  /** Current status of the job */
  status: CompactionJobStatus;

  /** When the job was created */
  createdAt: number;

  /** When the job started running */
  startedAt?: number;

  /** When the job completed (success or failure) */
  completedAt?: number;

  /** Number of rows scanned from B-tree */
  rowsScanned: number;

  /** Number of rows actually compacted */
  rowsCompacted: number;

  /** Number of columnar chunks created */
  chunksCreated: number;

  /** Bytes written to columnar storage */
  bytesWritten: number;

  /** Number of rows deleted from B-tree */
  rowsDeleted: number;

  /** LSN range being compacted [start, end] */
  lsnRange?: [bigint, bigint];

  /** Error message if status is 'failed' */
  error?: string;

  /** Progress percentage (0-100) */
  progress: number;

  /** IDs of created row groups */
  rowGroupIds: string[];
}

/**
 * Summary of compaction activity
 */
export interface CompactionSummary {
  /** Total jobs ever run */
  totalJobs: number;

  /** Successful jobs */
  successfulJobs: number;

  /** Failed jobs */
  failedJobs: number;

  /** Total rows compacted across all jobs */
  totalRowsCompacted: number;

  /** Total chunks created */
  totalChunksCreated: number;

  /** Total bytes written */
  totalBytesWritten: number;

  /** Last compaction timestamp */
  lastCompactionAt?: number;

  /** Last error message */
  lastError?: string;

  /** Average compaction duration in ms */
  averageDuration: number;
}

// =============================================================================
// Candidate Selection
// =============================================================================

/**
 * A row that is a candidate for compaction
 */
export interface CompactionCandidate<K, V> {
  /** The row key */
  key: K;

  /** The row value */
  value: V;

  /** Estimated age of the row in ms */
  age: number;

  /** Estimated size in bytes */
  size: number;

  /** LSN when this row was last modified (if tracked) */
  lsn?: bigint;
}

/**
 * Result of scanning for compaction candidates
 */
export interface ScanResult<K, V> {
  /** Candidates found */
  candidates: CompactionCandidate<K, V>[];

  /** Whether more candidates may exist */
  hasMore: boolean;

  /** Cursor for pagination (the last key scanned) */
  cursor?: K;

  /** Total rows scanned */
  scanned: number;

  /** Total estimated size of candidates */
  totalSize: number;

  /** LSN range of scanned data */
  lsnRange?: [bigint, bigint];
}

/**
 * Options for candidate scanning
 */
export interface ScanOptions<K> {
  /** Start key for scanning (exclusive) */
  afterKey?: K;

  /** Maximum candidates to return */
  limit?: number;

  /** Minimum age threshold (ms) */
  minAge?: number;

  /** Only scan rows with LSN <= this value */
  maxLSN?: bigint;
}

// =============================================================================
// Conversion Types
// =============================================================================

/**
 * Options for row-to-columnar conversion
 */
export interface ConversionOptions {
  /** Target rows per chunk */
  targetRows?: number;

  /** Target bytes per chunk */
  targetBytes?: number;

  /** Force specific encoding for columns */
  forceEncoding?: Map<string, 'raw' | 'dict' | 'rle' | 'delta' | 'bitpack'>;

  /** Whether to compute extended statistics */
  computeStats?: boolean;
}

/**
 * Result of a conversion operation
 */
export interface ConversionResult {
  /** Created row groups */
  rowGroups: RowGroup[];

  /** Serialized row group data */
  serialized: Uint8Array[];

  /** Total rows converted */
  rowCount: number;

  /** Total bytes written */
  byteSize: number;

  /** Conversion duration in ms */
  durationMs: number;
}

// =============================================================================
// Cleanup Types
// =============================================================================

/**
 * Options for cleanup operation
 */
export interface CleanupOptions {
  /** Whether to verify columnar chunks are durable before deleting */
  verifyDurability?: boolean;

  /** Keys to delete from B-tree */
  keys: unknown[];

  /** Row groups that have been written */
  rowGroupIds: string[];

  /** Whether to update manifest */
  updateManifest?: boolean;
}

/**
 * Result of cleanup operation
 */
export interface CleanupResult {
  /** Number of rows deleted from B-tree */
  deletedRows: number;

  /** Keys that failed to delete */
  failedDeletes: unknown[];

  /** Whether manifest was updated */
  manifestUpdated: boolean;

  /** Cleanup duration in ms */
  durationMs: number;
}

// =============================================================================
// Manifest Types
// =============================================================================

/**
 * Metadata about a compacted row group
 */
export interface RowGroupManifestEntry {
  /** Row group ID */
  id: string;

  /** Storage key/path */
  storagePath: string;

  /** Number of rows */
  rowCount: number;

  /** Byte size */
  byteSize: number;

  /** Row range (min/max primary key if sortable) */
  keyRange?: {
    min: unknown;
    max: unknown;
  };

  /** LSN range covered by this row group */
  lsnRange?: [bigint, bigint];

  /** Creation timestamp */
  createdAt: number;

  /** Compaction job that created this */
  jobId: string;
}

/**
 * Manifest of all compacted data
 */
export interface CompactionManifest {
  /** Schema version for manifest format */
  version: number;

  /** Table name */
  tableName: string;

  /** Schema used for columnar data */
  schema: ColumnarTableSchema;

  /** List of all row groups */
  rowGroups: RowGroupManifestEntry[];

  /** Total rows across all row groups */
  totalRows: number;

  /** Total bytes across all row groups */
  totalBytes: number;

  /** Last compaction LSN */
  lastCompactedLSN?: bigint;

  /** Last updated timestamp */
  updatedAt: number;
}

// =============================================================================
// Scheduler Types
// =============================================================================

/**
 * State of the compaction scheduler
 */
export type SchedulerState = 'idle' | 'running' | 'paused' | 'stopped';

/**
 * Write rate statistics for adaptive scheduling
 */
export interface WriteRateStats {
  /** Current writes per second */
  currentRate: number;

  /** Average writes per second over last minute */
  averageRate: number;

  /** Peak writes per second */
  peakRate: number;

  /** Last measurement timestamp */
  lastMeasuredAt: number;
}

/**
 * Scheduler status information
 */
export interface SchedulerStatus {
  /** Current state */
  state: SchedulerState;

  /** Next scheduled run time (for 'scheduled' mode) */
  nextRunAt?: number;

  /** Current or last job */
  currentJob?: CompactionJob;

  /** Write rate statistics */
  writeRate: WriteRateStats;

  /** Whether compaction is needed based on thresholds */
  compactionNeeded: boolean;

  /** Reasons why compaction is/isn't needed */
  reasons: string[];
}

// =============================================================================
// Compactor Interface
// =============================================================================

/**
 * Main compactor interface
 */
export interface Compactor {
  /**
   * Run a single compaction cycle
   * @returns The compaction job result
   */
  runOnce(): Promise<CompactionJob>;

  /**
   * Start background compaction
   * For 'continuous' and 'scheduled' modes
   */
  start(): void;

  /**
   * Stop background compaction
   * Waits for current job to complete
   */
  stop(): Promise<void>;

  /**
   * Pause background compaction
   * Can be resumed with resume()
   */
  pause(): void;

  /**
   * Resume paused compaction
   */
  resume(): void;

  /**
   * Get current scheduler status
   */
  getStatus(): SchedulerStatus;

  /**
   * Get compaction summary statistics
   */
  getSummary(): Promise<CompactionSummary>;

  /**
   * Get the current manifest
   */
  getManifest(): Promise<CompactionManifest | null>;

  /**
   * Check if compaction should run based on thresholds
   */
  shouldCompact(): Promise<{ needed: boolean; reasons: string[] }>;

  /**
   * Force a checkpoint after compaction
   */
  checkpoint(): Promise<void>;

  /**
   * Set DO alarm for next scheduled run (for 'scheduled' mode)
   * @param alarmTime Time for the alarm
   */
  setAlarm(alarmTime: number): Promise<void>;

  /**
   * Handle DO alarm callback
   */
  handleAlarm(): Promise<void>;
}

// =============================================================================
// Dependencies
// =============================================================================

/**
 * Dependencies for creating a compactor
 */
export interface CompactorDeps<K, V> {
  /** B-tree for hot data */
  btree: BTree<K, V>;

  /** FSX backend for storage */
  fsx: FSXBackend;

  /** Key codec for serialization */
  keyCodec: KeyCodec<K>;

  /** Value codec for serialization */
  valueCodec: ValueCodec<V>;

  /** Columnar schema for conversion */
  schema: ColumnarTableSchema;

  /** Optional WAL writer for LSN tracking */
  wal?: WALWriter;

  /** Optional callback for DO alarm setting */
  setAlarm?: (time: number) => Promise<void>;

  /** Optional callback for write rate tracking */
  onWrite?: () => void;
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * Compaction-specific error codes
 */
export enum CompactionErrorCode {
  /** Scan operation failed */
  SCAN_FAILED = 'COMPACTION_SCAN_FAILED',
  /** Conversion operation failed */
  CONVERSION_FAILED = 'COMPACTION_CONVERSION_FAILED',
  /** Write operation failed */
  WRITE_FAILED = 'COMPACTION_WRITE_FAILED',
  /** Cleanup operation failed */
  CLEANUP_FAILED = 'COMPACTION_CLEANUP_FAILED',
  /** Manifest operation failed */
  MANIFEST_FAILED = 'COMPACTION_MANIFEST_FAILED',
  /** Job was cancelled */
  JOB_CANCELLED = 'COMPACTION_JOB_CANCELLED',
  /** Invalid configuration */
  INVALID_CONFIG = 'COMPACTION_INVALID_CONFIG',
  /** Scheduler error */
  SCHEDULER_ERROR = 'COMPACTION_SCHEDULER_ERROR',
  /** Durability verification failed */
  DURABILITY_CHECK_FAILED = 'COMPACTION_DURABILITY_CHECK_FAILED',
}

/**
 * Custom error class for compaction operations
 */
export class CompactionError extends Error {
  constructor(
    public readonly code: CompactionErrorCode,
    message: string,
    public readonly jobId?: string,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'CompactionError';
  }
}
