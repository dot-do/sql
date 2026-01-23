/**
 * WAL Retention Policy Types for DoSQL
 *
 * Extracted from retention.ts to provide a clean separation of concerns:
 * - retention-types.ts: Type definitions and interfaces
 * - retention.ts: Implementation
 *
 * @packageDocumentation
 */

import type { WALWriter, WALConfig } from './types.js';

// =============================================================================
// Time Window Types
// =============================================================================

/**
 * Time window for low activity cleanup
 */
export interface LowActivityWindow {
  /** Start time in HH:MM format */
  start: string;
  /** End time in HH:MM format */
  end: string;
}

// =============================================================================
// Dynamic Policy Types
// =============================================================================

/**
 * Dynamic policy configuration
 */
export interface DynamicPolicyConfig {
  /** Storage threshold (0-1) that triggers reduced retention */
  storageThreshold: number;
  /** Reduced retention period in hours */
  reducedRetentionHours: number;
}

// =============================================================================
// CDC Integration Types
// =============================================================================

/**
 * CDC integration configuration
 */
export interface CDCIntegrationConfig {
  /** Function to get the pending CDC LSN */
  getPendingLSN: () => Promise<bigint>;
}

// =============================================================================
// Metrics Types
// =============================================================================

/**
 * Metrics reporter interface
 */
export interface MetricsReporter {
  /** Report a metric */
  report: (metric: RetentionMetric) => void;
}

/**
 * Retention metric structure
 */
export interface RetentionMetric {
  /** Metric type */
  type: string;
  /** Metric value */
  value: number;
  /** Additional labels */
  labels?: Record<string, string>;
  /** Timestamp */
  timestamp: number;
}

// =============================================================================
// Warning & Event Types
// =============================================================================

/**
 * Retention warning structure
 */
export interface RetentionWarning {
  type: 'size_threshold' | 'entry_threshold' | 'age_threshold';
  currentValue: number;
  threshold: number;
  message: string;
}

/**
 * Cleanup progress event
 */
export interface CleanupProgressEvent {
  type: 'start' | 'progress' | 'complete' | 'error';
  segmentsProcessed?: number;
  segmentsTotal?: number;
  bytesFreed?: number;
  error?: string;
}

/**
 * Policy decision for logging
 */
export interface PolicyDecision {
  segmentId: string;
  decision: 'keep' | 'delete';
  reasons: string[];
}

// =============================================================================
// Retention Policy Interface
// =============================================================================

/**
 * Configuration for WAL retention policy
 *
 * This interface defines all configurable aspects of WAL segment retention,
 * including time-based, size-based, entry count-based, and checkpoint-based
 * retention strategies.
 */
export interface RetentionPolicy {
  // ========== Core retention settings ==========
  /** Minimum number of segments to retain regardless of slot positions (default: 2) */
  minSegmentCount: number;
  /** Maximum age in milliseconds for segments before eligible for deletion (default: 24 hours) */
  maxSegmentAge: number;
  /** Whether to respect replication slot positions when deleting (default: true) */
  respectSlotPositions: boolean;
  /** Minimum time in ms since last reader activity before segment can be deleted (default: 5 minutes) */
  readerIdleTimeout: number;
  /** Whether to archive segments before deletion (default: true) */
  archiveBeforeDelete: boolean;

  // ========== Extended time-based retention ==========
  /** Retention period in hours (convenience, auto-calculates maxSegmentAge) */
  retentionHours?: number;
  /** Retention period in days (convenience, auto-calculates maxSegmentAge) */
  retentionDays?: number;
  /** Retention period in minutes (convenience for testing) */
  retentionMinutes?: number;

  // ========== Size-based retention ==========
  /** Maximum total WAL size in bytes */
  maxTotalBytes?: number;
  /** Human-readable size limit (e.g., "100MB", "1GB") */
  maxTotalSize?: string;
  /** Warning threshold (0-1), e.g., 0.8 means warn at 80% */
  warningThreshold?: number;
  /** Size warning threshold (0-1) - alias for warningThreshold */
  sizeWarningThreshold?: number;
  /** Callback for warnings */
  onWarning?: (warning: RetentionWarning) => void;
  /** Callback for size warnings (current bytes, max bytes) */
  onSizeWarning?: (current: number, max: number) => void;

  // ========== Entry count-based retention ==========
  /** Maximum total entry count */
  maxEntryCount?: number;

  // ========== Checkpoint-based retention ==========
  /** Checkpoint retention mode: 'strict' keeps only post-checkpoint, 'relaxed' is default */
  checkpointRetentionMode?: 'strict' | 'relaxed';
  /** Number of checkpoints worth of WAL to keep */
  keepCheckpointCount?: number;
  /** Whether to auto-cleanup after checkpoint */
  cleanupOnCheckpoint?: boolean;
  /** Callback when cleanup is triggered */
  onCleanupTriggered?: () => void;

  // ========== Compaction ==========
  /** Compaction threshold (0-1), e.g., 0.3 means compact when 30% fragmented */
  compactionThreshold?: number;
  /** Whether to auto-trigger compaction */
  autoCompaction?: boolean;
  /** Callback when compaction is needed */
  onCompactionNeeded?: () => void;
  /** Minimum segment size for merging */
  minSegmentSize?: number;
  /** Whether to merge small segments */
  mergeSmallSegments?: boolean;

  // ========== Background cleanup ==========
  /** Cleanup interval in milliseconds */
  cleanupIntervalMs?: number;
  /** Whether to enable background cleanup */
  backgroundCleanup?: boolean;
  /** Cron-style cleanup schedule */
  cleanupSchedule?: string;
  /** Max segments to process per cleanup batch */
  maxCleanupBatchSize?: number;
  /** Delay between deletions in cleanup */
  cleanupThrottleMs?: number;
  /** Callback for cleanup progress */
  onCleanupProgress?: (event: CleanupProgressEvent) => void;
  /** Low activity window for cleanup */
  lowActivityWindow?: LowActivityWindow;
  /** Whether to prefer low activity cleanup */
  preferLowActivityCleanup?: boolean;

  // ========== Policy configuration ==========
  /** Preset name to use */
  preset?: 'aggressive' | 'balanced' | 'conservative';
  /** Base preset to extend */
  extends?: 'aggressive' | 'balanced' | 'conservative';
  /** Overrides for extended preset */
  overrides?: Partial<RetentionPolicy>;
  /** Whether to log verbose decisions */
  verboseLogging?: boolean;

  // ========== Edge cases ==========
  /** Whether to handle clock skew */
  handleClockSkew?: boolean;
  /** Max segments to process in one check */
  maxSegmentsToProcess?: number;
  /** History size for cleanup records */
  cleanupHistorySize?: number;

  // ========== Dynamic policy ==========
  /** Dynamic policy configuration */
  dynamicPolicy?: DynamicPolicyConfig;

  // ========== Metrics ==========
  /** Metrics reporter */
  metricsReporter?: MetricsReporter;

  // ========== CDC Integration ==========
  /** CDC integration config */
  cdcIntegration?: CDCIntegrationConfig;
  /** Replication lag tolerance (in LSN units) */
  replicationLagTolerance?: number;
  /** Regions for multi-region replication */
  regions?: string[];
  /** Whether to wait for all regions */
  waitForAllRegions?: boolean;
}

// =============================================================================
// Default Policy & Presets
// =============================================================================

/**
 * Default retention policy
 */
export const DEFAULT_RETENTION_POLICY: RetentionPolicy = {
  minSegmentCount: 2,
  maxSegmentAge: 24 * 60 * 60 * 1000, // 24 hours
  respectSlotPositions: true,
  readerIdleTimeout: 5 * 60 * 1000, // 5 minutes
  archiveBeforeDelete: true,
};

/**
 * Retention policy presets
 */
export const RETENTION_PRESETS: Record<string, RetentionPolicy> = {
  aggressive: {
    ...DEFAULT_RETENTION_POLICY,
    minSegmentCount: 1,
    maxSegmentAge: 1 * 60 * 60 * 1000, // 1 hour
    retentionHours: 1,
    maxTotalBytes: 50 * 1024 * 1024, // 50MB
    maxEntryCount: 10000,
    archiveBeforeDelete: false,
    compactionThreshold: 0.2,
    autoCompaction: true,
  },
  balanced: {
    ...DEFAULT_RETENTION_POLICY,
    minSegmentCount: 3,
    maxSegmentAge: 24 * 60 * 60 * 1000, // 24 hours
    retentionHours: 24,
    maxTotalBytes: 500 * 1024 * 1024, // 500MB
    maxEntryCount: 100000,
    compactionThreshold: 0.3,
    autoCompaction: true,
  },
  conservative: {
    ...DEFAULT_RETENTION_POLICY,
    minSegmentCount: 5,
    maxSegmentAge: 7 * 24 * 60 * 60 * 1000, // 7 days
    retentionDays: 7,
    maxTotalBytes: 2 * 1024 * 1024 * 1024, // 2GB
    maxEntryCount: 1000000,
    compactionThreshold: 0.5,
    autoCompaction: false,
  },
};

// =============================================================================
// Reader & Result Types
// =============================================================================

/**
 * Information about an active reader
 */
export interface ActiveReader {
  /** Unique reader identifier */
  readerId: string;
  /** Current reading position (LSN) */
  currentLSN: bigint;
  /** Segment currently being read */
  currentSegmentId: string;
  /** Last activity timestamp */
  lastActivityAt: number;
  /** Reader description/name */
  description?: string;
}

/**
 * Result of a retention check
 */
export interface RetentionCheckResult {
  /** Segments eligible for deletion */
  eligibleForDeletion: string[];
  /** Segments protected by replication slots */
  protectedBySlots: string[];
  /** Segments protected by active readers */
  protectedByReaders: string[];
  /** Segments protected by minimum count policy */
  protectedByMinCount: string[];
  /** Minimum LSN across all slots (null if no slots) */
  minSlotLSN: bigint | null;
  /** Oldest segment timestamp */
  oldestSegmentTime: number | null;
  /** Total segment count */
  totalSegmentCount: number;

  // ========== Extended result properties ==========
  /** Bytes over the limit (when maxTotalBytes is exceeded) */
  bytesOverLimit?: number;
  /** Entries over the limit (when maxEntryCount is exceeded) */
  entriesOverLimit?: number;
  /** Segments before checkpoint (in strict mode) */
  segmentsBeforeCheckpoint?: string[];
  /** Whether batch processing was used */
  batchProcessed?: boolean;
  /** Empty segments found */
  emptySegments?: string[];
  /** Segments with invalid IDs that were skipped */
  skippedInvalid?: string[];
  /** Whether clock skew was detected */
  clockSkewDetected?: boolean;
  /** Policy decisions for each segment */
  decisions?: PolicyDecision[];
  /** Segments protected by CDC */
  protectedByCDC?: string[];
}

/**
 * Result of a retention cleanup operation
 */
export interface RetentionCleanupResult {
  /** Segments that were deleted */
  deleted: string[];
  /** Segments that were archived (if archiveBeforeDelete is true) */
  archived: string[];
  /** Segments that failed to delete with error messages */
  failed: Array<{ segmentId: string; error: string }>;
  /** Total bytes freed */
  bytesFreed: number;
  /** Time taken in milliseconds */
  durationMs: number;
  /** Corrupted segments encountered */
  corruptedSegments?: string[];
}

// =============================================================================
// Statistics Types
// =============================================================================

/**
 * Storage statistics
 */
export interface StorageStats {
  /** Total bytes across all segments */
  totalBytes: number;
  /** Total segment count */
  segmentCount: number;
  /** Average segment size */
  averageSegmentSize: number;
}

/**
 * Entry statistics
 */
export interface EntryStats {
  /** Total entries across all segments */
  totalEntries: number;
}

/**
 * Segment entry statistics
 */
export interface SegmentEntryStats {
  /** Segment ID */
  segmentId: string;
  /** Entry count in segment */
  entryCount: number;
  /** Segment size in bytes */
  sizeBytes: number;
}

/**
 * Fragmentation information
 */
export interface FragmentationInfo {
  /** Fragmentation ratio (0-1) */
  ratio: number;
  /** Number of rolled-back entries */
  rolledBackEntries: number;
  /** Total entries */
  totalEntries: number;
}

/**
 * Segment merge result
 */
export interface MergeResult {
  /** New segment ID after merge */
  newSegmentId: string;
  /** Number of segments merged */
  mergedCount: number;
  /** Bytes saved by merging */
  bytesSaved: number;
}

/**
 * Segment compact result
 */
export interface CompactResult {
  /** Entries removed during compaction */
  entriesRemoved: number;
  /** Bytes saved by compaction */
  bytesSaved: number;
}

/**
 * Throttle configuration
 */
export interface ThrottleConfig {
  /** Maximum batch size */
  maxBatchSize: number;
  /** Throttle delay in ms */
  throttleMs: number;
}

/**
 * Retention metrics
 */
export interface RetentionMetrics {
  /** Total segment count */
  totalSegments: number;
  /** Total bytes */
  totalBytes: number;
  /** Oldest segment age in ms */
  oldestSegmentAge: number | null;
  /** Last cleanup timestamp */
  lastCleanupTime: number | null;
  /** Segments deleted in last cleanup */
  segmentsDeleted: number;
  /** Bytes freed in last cleanup */
  bytesFreed: number;
}

/**
 * Cleanup history record
 */
export interface CleanupRecord {
  /** Timestamp of cleanup */
  timestamp: number;
  /** Segments deleted */
  segmentsDeleted: number;
  /** Bytes freed */
  bytesFreed: number;
  /** Duration in ms */
  durationMs: number;
}

/**
 * Health check result
 */
export interface HealthCheckResult {
  /** Health status */
  status: 'healthy' | 'warning' | 'critical';
  /** List of issues found */
  issues: string[];
  /** Recommendations */
  recommendations: string[];
}

/**
 * Dynamic policy evaluation result
 */
export interface DynamicPolicyResult {
  /** Whether dynamic policy was applied */
  applied: boolean;
  /** Reason for application/non-application */
  reason: string;
}

/**
 * Replication status
 */
export interface ReplicationStatus {
  /** Current lag in LSN units */
  currentLag: number;
  /** Whether lag is within tolerance */
  isWithinTolerance: boolean;
}

/**
 * Region replication status
 */
export interface RegionReplicationStatus {
  [region: string]: {
    lastLSN: bigint;
    lag: number;
    healthy: boolean;
  };
}

/**
 * Checkpoint info
 */
export interface CheckpointInfo {
  lsn: bigint;
  timestamp: number;
}

/**
 * Expired entry information
 */
export interface ExpiredEntry {
  lsn: bigint;
  segmentId: string;
  entryTimestamp: number;
  age: number;
  txnId: string;
  table: string;
  op: string;
}

/**
 * WAL statistics
 */
export interface WALStats {
  totalSegments: number;
  totalEntries: number;
  totalBytes: number;
  oldestEntryTimestamp: number | null;
  newestEntryTimestamp: number | null;
  averageSegmentSize: number;
  fragmentationRatio: number;
  deadEntriesCount: number;
  activeTransactionCount: number;
}

/**
 * Truncate result
 */
export interface TruncateResult {
  truncatedSegments: string[];
  bytesFreed: number;
  truncatedLSN: bigint;
}

/**
 * Compact result extended
 */
export interface CompactWALResult {
  segmentsCompacted: number;
  entriesRemoved: number;
  bytesReclaimed: number;
}

/**
 * Force cleanup options
 */
export interface ForceCleanupOptions {
  ignoreMinCount?: boolean;
  ignoreSlots?: boolean;
  ignoreReaders?: boolean;
}

/**
 * Force cleanup result
 */
export interface ForceCleanupResult {
  deleted: string[];
  bytesFreed: number;
}

/**
 * Cleanup latency histogram
 */
export interface CleanupLatencyHistogram {
  p50: number;
  p90: number;
  p99: number;
  min: number;
  max: number;
  avg: number;
  count: number;
}

/**
 * Growth statistics
 */
export interface GrowthStats {
  bytesPerHour: number;
  segmentsPerHour: number;
  entriesPerHour: number;
  estimatedTimeToLimit: number | null;
}

// =============================================================================
// Manager Interface
// =============================================================================

/**
 * WAL Retention Manager interface
 */
export interface WALRetentionManager {
  /**
   * Register an active reader for protection tracking
   * @param reader - Reader information to register
   */
  registerReader(reader: ActiveReader): void;

  /**
   * Unregister a reader when it's done
   * @param readerId - ID of the reader to unregister
   */
  unregisterReader(readerId: string): void;

  /**
   * Update a reader's current position
   * @param readerId - ID of the reader to update
   * @param currentLSN - New current LSN
   * @param currentSegmentId - New current segment ID
   */
  updateReaderPosition(
    readerId: string,
    currentLSN: bigint,
    currentSegmentId: string
  ): void;

  /**
   * Get all registered readers
   */
  getActiveReaders(): ActiveReader[];

  /**
   * Calculate the minimum required LSN across all replication slots
   * @returns The minimum LSN or null if no slots exist
   */
  getMinSlotLSN(): Promise<bigint | null>;

  /**
   * Check which segments are eligible for deletion
   * @returns Detailed result of the retention check
   */
  checkRetention(): Promise<RetentionCheckResult>;

  /**
   * Identify segments that are safe to delete based on policy
   * @returns Array of segment IDs safe to delete
   */
  getSegmentsToDelete(): Promise<string[]>;

  /**
   * Perform cleanup of eligible segments
   * @param dryRun - If true, don't actually delete, just return what would be deleted
   * @returns Result of the cleanup operation
   */
  cleanup(dryRun?: boolean): Promise<RetentionCleanupResult>;

  /**
   * Get the current retention policy
   */
  getPolicy(): RetentionPolicy;

  /**
   * Update the retention policy
   * @param policy - Partial policy to merge with current
   */
  updatePolicy(policy: Partial<RetentionPolicy>): void;
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * Retention-specific error codes
 */
export enum RetentionErrorCode {
  /** Segment deletion failed */
  DELETION_FAILED = 'RETENTION_DELETION_FAILED',
  /** Archive failed */
  ARCHIVE_FAILED = 'RETENTION_ARCHIVE_FAILED',
  /** Invalid policy configuration */
  INVALID_POLICY = 'RETENTION_INVALID_POLICY',
  /** Slot query failed */
  SLOT_QUERY_FAILED = 'RETENTION_SLOT_QUERY_FAILED',
}

/**
 * Custom error class for retention operations
 */
export class RetentionError extends Error {
  constructor(
    public readonly code: RetentionErrorCode,
    message: string,
    public readonly segmentId?: string,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'RetentionError';
  }
}
