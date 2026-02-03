/**
 * WAL Retention Policy Types for DoSQL
 *
 * This module defines the types for WAL segment retention management. The API
 * is organized into two tiers:
 *
 * 1. **{@link RetentionConfig}** - Simplified configuration for common use cases.
 *    Most users should start here. Provides sensible defaults and convenience
 *    options like `retentionHours` and `maxTotalBytes`.
 *
 * 2. **{@link RetentionPolicy}** - Full resolved policy (extends RetentionConfig).
 *    Contains all configuration knobs including advanced features like CDC
 *    integration, dynamic policies, and background cleanup scheduling.
 *
 * @example Basic usage with RetentionConfig
 * ```typescript
 * const manager = createWALRetentionManager(backend, reader, slotManager, {
 *   retentionHours: 24,
 *   maxTotalBytes: 500 * 1024 * 1024,  // 500MB
 * });
 * ```
 *
 * @example Using a preset
 * ```typescript
 * const manager = createWALRetentionManager(backend, reader, slotManager, {
 *   preset: 'balanced',
 * });
 * ```
 *
 * @packageDocumentation
 */

import type { WALWriter, WALConfig } from './types.js';

// =============================================================================
// Time Window Types
// =============================================================================

/**
 * Time window specified in HH:MM format.
 *
 * Used to define periods for low-activity cleanup or scheduled cleanup windows.
 * Supports windows that cross midnight (e.g., start: "22:00", end: "04:00").
 *
 * @example
 * ```typescript
 * const window: LowActivityWindow = { start: '02:00', end: '06:00' };
 * ```
 */
export interface LowActivityWindow {
  /** Start time in HH:MM format (24-hour) */
  start: string;
  /** End time in HH:MM format (24-hour) */
  end: string;
}

// =============================================================================
// Dynamic Policy Types
// =============================================================================

/**
 * Configuration for automatic retention adjustment under storage pressure.
 *
 * When storage usage exceeds `storageThreshold`, the retention period is
 * automatically reduced to `reducedRetentionHours` to free space faster.
 *
 * @example
 * ```typescript
 * const config: DynamicPolicyConfig = {
 *   storageThreshold: 0.9,        // Trigger at 90% usage
 *   reducedRetentionHours: 1,     // Drop to 1-hour retention
 * };
 * ```
 */
export interface DynamicPolicyConfig {
  /** Storage usage ratio (0-1) that triggers reduced retention */
  storageThreshold: number;
  /** Reduced retention period in hours when threshold is exceeded */
  reducedRetentionHours: number;
}

// =============================================================================
// CDC Integration Types
// =============================================================================

/**
 * Configuration for CDC (Change Data Capture) integration.
 *
 * When configured, WAL segments required by pending CDC events are protected
 * from deletion, ensuring no CDC data is lost during cleanup.
 */
export interface CDCIntegrationConfig {
  /** Returns the LSN of the oldest pending CDC event */
  getPendingLSN: () => Promise<bigint>;
}

// =============================================================================
// Metrics Types
// =============================================================================

/**
 * Interface for reporting retention metrics to external monitoring systems.
 *
 * Implement this interface to forward WAL retention metrics to Prometheus,
 * StatsD, Datadog, or any other metrics backend.
 *
 * @example
 * ```typescript
 * const reporter: MetricsReporter = {
 *   report: (metric) => {
 *     statsd.gauge(`wal.${metric.type}`, metric.value, metric.labels);
 *   },
 * };
 * ```
 */
export interface MetricsReporter {
  /** Report a single metric data point */
  report: (metric: RetentionMetric) => void;
}

/**
 * A single metric data point emitted by the retention system.
 */
export interface RetentionMetric {
  /** Metric name (e.g., "cleanup_segments_deleted", "cleanup_bytes_freed") */
  type: string;
  /** Metric value */
  value: number;
  /** Optional key-value labels for metric dimensions */
  labels?: Record<string, string>;
  /** Unix timestamp in milliseconds when the metric was recorded */
  timestamp: number;
}

// =============================================================================
// Warning & Event Types
// =============================================================================

/**
 * Warning emitted when a retention threshold is approaching its limit.
 */
export interface RetentionWarning {
  /** Which threshold triggered the warning */
  type: 'size_threshold' | 'entry_threshold' | 'age_threshold';
  /** Current value of the monitored metric */
  currentValue: number;
  /** Configured threshold value */
  threshold: number;
  /** Human-readable warning message */
  message: string;
}

/**
 * Progress event emitted during cleanup operations.
 *
 * Subscribe to these events via the `onCleanupProgress` callback to
 * monitor long-running cleanup operations.
 */
export interface CleanupProgressEvent {
  /** Current phase of the cleanup operation */
  type: 'start' | 'progress' | 'complete' | 'error';
  /** Number of segments processed so far (during 'progress') */
  segmentsProcessed?: number;
  /** Total segments to process (during 'progress') */
  segmentsTotal?: number;
  /** Cumulative bytes freed so far */
  bytesFreed?: number;
  /** Error message (during 'error') */
  error?: string;
}

/**
 * Detailed per-segment decision record for debugging retention behavior.
 * Only populated when `verboseLogging` is enabled.
 */
export interface PolicyDecision {
  /** Segment that was evaluated */
  segmentId: string;
  /** Whether the segment should be kept or deleted */
  decision: 'keep' | 'delete';
  /** Reasons explaining the decision */
  reasons: string[];
}

// =============================================================================
// Simplified Retention Config (Primary Public API)
// =============================================================================

/**
 * Simplified WAL retention configuration for common use cases.
 *
 * This is the recommended configuration interface for most users. It provides
 * the essential parameters with sensible defaults. For advanced features, pass
 * any additional {@link RetentionPolicy} fields -- `RetentionConfig` extends
 * naturally into the full policy.
 *
 * **Defaults** (when no config is provided):
 * - Retain segments for 24 hours
 * - Keep at least 2 segments
 * - Respect replication slot positions
 * - Archive segments before deletion
 * - 5-minute reader idle timeout
 *
 * @example Minimal config (use defaults)
 * ```typescript
 * const manager = createWALRetentionManager(backend, reader, slotManager);
 * ```
 *
 * @example Time-based retention
 * ```typescript
 * const manager = createWALRetentionManager(backend, reader, slotManager, {
 *   retentionHours: 6,           // Keep 6 hours of WAL
 *   minSegmentCount: 1,          // Keep at least 1 segment
 * });
 * ```
 *
 * @example Size-based retention with preset
 * ```typescript
 * const manager = createWALRetentionManager(backend, reader, slotManager, {
 *   preset: 'balanced',          // Use balanced defaults
 *   maxTotalBytes: 256 * 1024 * 1024,  // Override: 256MB limit
 * });
 * ```
 */
export interface RetentionConfig {
  // ---- Retention duration (pick one; retentionHours is recommended) ----

  /**
   * Retention period in hours. This is the recommended way to set retention.
   * Segments older than this are eligible for deletion.
   *
   * Mutually exclusive with `retentionDays` and `retentionMinutes` --
   * if multiple are set, priority is: `retentionMinutes` > `retentionHours` > `retentionDays`.
   *
   * @default 24 (via maxSegmentAge default of 24 hours)
   *
   * @example
   * ```typescript
   * { retentionHours: 12 }  // Keep 12 hours of WAL
   * ```
   */
  retentionHours?: number;

  /**
   * Retention period in days. Convenience alternative to `retentionHours`.
   * @example
   * ```typescript
   * { retentionDays: 7 }  // Keep 7 days of WAL
   * ```
   */
  retentionDays?: number;

  /**
   * Retention period in minutes. Primarily useful for testing.
   * Takes priority over `retentionHours` and `retentionDays`.
   */
  retentionMinutes?: number;

  // ---- Size limits ----

  /**
   * Maximum total WAL size in bytes. When exceeded, the oldest segments
   * become eligible for deletion regardless of age.
   *
   * @example
   * ```typescript
   * { maxTotalBytes: 500 * 1024 * 1024 }  // 500MB limit
   * ```
   */
  maxTotalBytes?: number;

  /**
   * Maximum total entry count across all WAL segments.
   * When exceeded, oldest segments become eligible for deletion.
   */
  maxEntryCount?: number;

  // ---- Safety ----

  /**
   * Minimum number of segments to always retain, regardless of age or size.
   * Prevents accidentally deleting all WAL data.
   *
   * @default 2
   */
  minSegmentCount?: number;

  /**
   * Whether to respect replication slot positions when determining
   * which segments to delete. When true, segments needed by any
   * replication slot are protected from deletion.
   *
   * @default true
   */
  respectSlotPositions?: boolean;

  /**
   * Whether to copy segments to the archive path before deletion.
   * Provides a safety net for data recovery.
   *
   * @default true
   */
  archiveBeforeDelete?: boolean;

  // ---- Presets ----

  /**
   * Use a named preset as the base configuration. Individual fields
   * in this config override the preset values.
   *
   * Available presets:
   * - `'aggressive'` - 1h retention, 50MB limit, no archiving, auto-compaction
   * - `'balanced'`   - 24h retention, 500MB limit, archiving, auto-compaction
   * - `'conservative'` - 7d retention, 2GB limit, archiving, manual compaction
   *
   * @example
   * ```typescript
   * { preset: 'aggressive', maxTotalBytes: 100 * 1024 * 1024 }
   * ```
   */
  preset?: 'aggressive' | 'balanced' | 'conservative';

  // ---- Callbacks ----

  /**
   * Called when storage usage exceeds the warning threshold.
   * Provides both the current size and the configured maximum.
   *
   * @example
   * ```typescript
   * {
   *   maxTotalBytes: 500 * 1024 * 1024,
   *   warningThreshold: 0.8,
   *   onWarning: (w) => console.warn(w.message),
   * }
   * ```
   */
  onWarning?: (warning: RetentionWarning) => void;

  /**
   * Warning threshold as a ratio (0-1) of `maxTotalBytes`. When storage
   * usage exceeds this fraction, the `onWarning` callback is invoked.
   *
   * @default undefined (no warning)
   *
   * @example
   * ```typescript
   * { warningThreshold: 0.8 }  // Warn at 80% capacity
   * ```
   */
  warningThreshold?: number;
}

// =============================================================================
// Full Retention Policy Interface
// =============================================================================

/**
 * Complete WAL retention policy configuration.
 *
 * This is the fully resolved internal type that includes all configuration
 * knobs. Most users should configure retention using the simpler
 * {@link RetentionConfig} fields (which are included here). The additional
 * fields on this interface are for advanced use cases like CDC integration,
 * background cleanup scheduling, and dynamic policy adjustment.
 *
 * **Required fields** (set by defaults if not provided):
 * - `minSegmentCount` - Minimum segments to retain (default: 2)
 * - `maxSegmentAge` - Maximum segment age in ms (default: 24 hours)
 * - `respectSlotPositions` - Protect segments needed by replication slots (default: true)
 * - `readerIdleTimeout` - Reader idle timeout in ms (default: 5 minutes)
 * - `archiveBeforeDelete` - Archive before deleting (default: true)
 */
export interface RetentionPolicy {
  // ========== Core retention settings (required, always have defaults) ====
  /** Minimum segments to retain regardless of other policies. @default 2 */
  minSegmentCount: number;
  /** Maximum segment age in ms before eligible for deletion. @default 86400000 (24h) */
  maxSegmentAge: number;
  /** Protect segments needed by replication slots. @default true */
  respectSlotPositions: boolean;
  /** Reader idle timeout in ms; idle readers do not protect segments. @default 300000 (5min) */
  readerIdleTimeout: number;
  /** Copy segments to archive before deletion. @default true */
  archiveBeforeDelete: boolean;

  // ========== Time-based retention (pick one) =============================
  /** Retention in hours. Sets `maxSegmentAge` automatically. */
  retentionHours?: number;
  /** Retention in days. Sets `maxSegmentAge` automatically. */
  retentionDays?: number;
  /** Retention in minutes (useful for testing). Sets `maxSegmentAge` automatically. */
  retentionMinutes?: number;

  // ========== Size-based retention ========================================
  /** Maximum total WAL size in bytes. */
  maxTotalBytes?: number;
  /**
   * Human-readable size limit (e.g., "100MB", "1GB").
   * Parsed into `maxTotalBytes` during policy resolution.
   */
  maxTotalSize?: string;
  /**
   * Warning threshold (0-1). Triggers `onWarning` when usage exceeds this ratio.
   * @example 0.8 means warn at 80% of `maxTotalBytes`.
   */
  warningThreshold?: number;
  /**
   * Alias for `warningThreshold`. If both are set, `warningThreshold` takes priority.
   * @deprecated Use `warningThreshold` instead.
   */
  sizeWarningThreshold?: number;
  /** Called when any retention threshold is approaching its limit. */
  onWarning?: (warning: RetentionWarning) => void;
  /**
   * Called when size exceeds the warning threshold. Provides raw byte values.
   * @deprecated Use `onWarning` instead, which provides structured warnings.
   */
  onSizeWarning?: (current: number, max: number) => void;

  // ========== Entry count-based retention =================================
  /** Maximum total entry count across all segments. */
  maxEntryCount?: number;

  // ========== Checkpoint-based retention ==================================
  /** `'strict'` deletes all pre-checkpoint segments; `'relaxed'` uses age-based rules. */
  checkpointRetentionMode?: 'strict' | 'relaxed';
  /** Number of checkpoints worth of WAL history to retain. @default 3 */
  keepCheckpointCount?: number;
  /** Automatically run cleanup after each checkpoint. */
  cleanupOnCheckpoint?: boolean;
  /** Called when a checkpoint-triggered cleanup begins. */
  onCleanupTriggered?: () => void;

  // ========== Compaction ==================================================
  /** Fragmentation ratio (0-1) that triggers compaction. */
  compactionThreshold?: number;
  /** Automatically compact when fragmentation exceeds `compactionThreshold`. */
  autoCompaction?: boolean;
  /** Called when compaction is needed (for custom compaction logic). */
  onCompactionNeeded?: () => void;
  /** Minimum segment size in bytes for merge eligibility. */
  minSegmentSize?: number;
  /** Merge adjacent small segments to reduce segment count. */
  mergeSmallSegments?: boolean;

  // ========== Background cleanup ==========================================
  /** Interval in ms between background cleanup runs. @default 60000 (1min) */
  cleanupIntervalMs?: number;
  /** Enable automatic background cleanup on a timer. */
  backgroundCleanup?: boolean;
  /** Cron expression for scheduled cleanup (e.g., `"0 3 * * *"` for 3 AM daily). */
  cleanupSchedule?: string;
  /** Maximum segments to process per cleanup batch. @default 100 */
  maxCleanupBatchSize?: number;
  /** Delay in ms between individual segment deletions during cleanup. */
  cleanupThrottleMs?: number;
  /** Progress callback for monitoring long-running cleanup operations. */
  onCleanupProgress?: (event: CleanupProgressEvent) => void;
  /** Time window during which cleanup is preferred (e.g., low-traffic hours). */
  lowActivityWindow?: LowActivityWindow;
  /** Only run background cleanup during `lowActivityWindow`. */
  preferLowActivityCleanup?: boolean;

  // ========== Preset / composition ========================================
  /** Use a named preset as the base. Fields in this config override the preset. */
  preset?: 'aggressive' | 'balanced' | 'conservative';
  /**
   * Extend a named preset with `overrides`.
   * @deprecated Use `preset` instead with direct field overrides.
   */
  extends?: 'aggressive' | 'balanced' | 'conservative';
  /**
   * Overrides applied on top of the `extends` preset.
   * @deprecated Use `preset` instead with direct field overrides.
   */
  overrides?: Partial<RetentionPolicy>;
  /** Enable verbose per-segment decision logging. */
  verboseLogging?: boolean;

  // ========== Advanced ====================================================
  /** Detect and handle clock skew between segment timestamps. */
  handleClockSkew?: boolean;
  /** Limit segments evaluated per retention check (for large WALs). */
  maxSegmentsToProcess?: number;
  /** Maximum cleanup history records to retain in memory. @default 100 */
  cleanupHistorySize?: number;

  // ========== Dynamic policy ==============================================
  /** Automatically reduce retention when storage is under pressure. */
  dynamicPolicy?: DynamicPolicyConfig;

  // ========== Metrics =====================================================
  /** External metrics reporter for Prometheus, StatsD, etc. */
  metricsReporter?: MetricsReporter;

  // ========== CDC integration =============================================
  /** Protect segments needed by pending CDC events. */
  cdcIntegration?: CDCIntegrationConfig;
  /** Maximum acceptable replication lag in LSN units. @default 1000 */
  replicationLagTolerance?: number;
  /** Region identifiers for multi-region replication tracking. */
  regions?: string[];
  /** Block cleanup until all regions have acknowledged. */
  waitForAllRegions?: boolean;

  // ========== Per-table retention =========================================
  /** Per-table retention overrides. Use `'*'` as the default/fallback key. */
  tableRetention?: Record<string, { retentionDays?: number; retentionHours?: number; archiveToR2?: boolean }>;

  // ========== Transaction importance ======================================
  /** Honor retention hints embedded in WAL entries. */
  respectRetentionHints?: boolean;

  // ========== Cleanup windows =============================================
  /** Restrict cleanup to specific time windows. */
  cleanupWindows?: CleanupWindow[];
  /** Block all cleanup operations outside configured `cleanupWindows`. */
  blockCleanupOutsideWindows?: boolean;

  // ========== Atomic cleanup ==============================================
  /** Use all-or-nothing cleanup: if any deletion fails, roll back all. */
  atomicCleanup?: boolean;

  // ========== Metrics events ==============================================
  /** Periodic metrics callback for real-time monitoring dashboards. */
  onMetricsUpdate?: (event: { type: string; timestamp: number; data: unknown }) => void;
  /** Interval in ms for `onMetricsUpdate` emissions. @default 5000 */
  metricsInterval?: number;
}

// =============================================================================
// Default Policy & Presets
// =============================================================================

/**
 * Default retention policy values.
 *
 * Applied when no configuration is provided. Retains 24 hours of WAL data,
 * keeps at least 2 segments, respects replication slots, and archives
 * segments before deletion.
 */
export const DEFAULT_RETENTION_POLICY: Readonly<RetentionPolicy> = {
  minSegmentCount: 2,
  maxSegmentAge: 24 * 60 * 60 * 1000, // 24 hours
  respectSlotPositions: true,
  readerIdleTimeout: 5 * 60 * 1000,   // 5 minutes
  archiveBeforeDelete: true,
};

/**
 * Built-in retention policy presets for common scenarios.
 *
 * Use via the `preset` field in your config:
 * ```typescript
 * createWALRetentionManager(backend, reader, slots, { preset: 'balanced' });
 * ```
 *
 * | Preset | Retention | Size Limit | Archive | Compaction |
 * |--------|-----------|------------|---------|------------|
 * | `aggressive` | 1 hour | 50 MB | No | Auto at 20% fragmentation |
 * | `balanced` | 24 hours | 500 MB | Yes | Auto at 30% fragmentation |
 * | `conservative` | 7 days | 2 GB | Yes | Manual (50% threshold) |
 */
export const RETENTION_PRESETS: Record<string, RetentionPolicy> = {
  aggressive: {
    ...DEFAULT_RETENTION_POLICY,
    minSegmentCount: 1,
    maxSegmentAge: 1 * 60 * 60 * 1000, // 1 hour
    retentionHours: 1,
    maxTotalBytes: 50 * 1024 * 1024,    // 50MB
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
    maxTotalBytes: 500 * 1024 * 1024,    // 500MB
    maxEntryCount: 100000,
    compactionThreshold: 0.3,
    autoCompaction: true,
  },
  conservative: {
    ...DEFAULT_RETENTION_POLICY,
    minSegmentCount: 5,
    maxSegmentAge: 7 * 24 * 60 * 60 * 1000, // 7 days
    retentionDays: 7,
    maxTotalBytes: 2 * 1024 * 1024 * 1024,   // 2GB
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
  /** Count of entries protected by retention hint */
  protectedByHint?: number;
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
  /** Transaction ID for atomic cleanup */
  transactionId?: string;
  /** Whether atomic cleanup was committed */
  committed?: boolean;
  /** Whether atomic cleanup was rolled back */
  rolledBack?: boolean;
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
 * WAL Retention Manager -- the primary interface for managing WAL segment lifecycle.
 *
 * The retention manager determines which WAL segments can safely be deleted
 * based on the configured policy, active readers, and replication slot positions.
 *
 * **Core workflow:**
 * 1. Call {@link checkRetention} to identify segments eligible for deletion
 * 2. Call {@link cleanup} to actually delete eligible segments
 * 3. Optionally register readers with {@link registerReader} to protect segments
 *
 * @example
 * ```typescript
 * const manager = createWALRetentionManager(backend, reader, slotManager, {
 *   retentionHours: 24,
 *   maxTotalBytes: 500 * 1024 * 1024,
 * });
 *
 * // Check what can be deleted
 * const check = await manager.checkRetention();
 * console.log(`${check.eligibleForDeletion.length} segments eligible`);
 *
 * // Perform cleanup
 * const result = await manager.cleanup();
 * console.log(`Freed ${result.bytesFreed} bytes`);
 * ```
 */
export interface WALRetentionManager {
  /**
   * Register an active reader so its current segment is protected from deletion.
   * Readers become unprotected after the configured `readerIdleTimeout`.
   *
   * @param reader - Reader information including its current position
   */
  registerReader(reader: ActiveReader): void;

  /**
   * Unregister a reader, removing its segment protection.
   *
   * @param readerId - ID of the reader to unregister
   */
  unregisterReader(readerId: string): void;

  /**
   * Update a reader's current position after it advances to a new segment.
   * Also resets the reader's idle timer.
   *
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
   * Get all currently registered readers and their positions.
   *
   * @returns Array of active reader information
   */
  getActiveReaders(): ActiveReader[];

  /**
   * Get the minimum LSN required by any replication slot. Segments at or
   * after this LSN are protected from deletion when `respectSlotPositions`
   * is enabled.
   *
   * @returns Minimum slot LSN, or null if no replication slots exist
   */
  getMinSlotLSN(): Promise<bigint | null>;

  /**
   * Evaluate the retention policy against current WAL state. Returns a detailed
   * result showing which segments are eligible for deletion and which are
   * protected (and why).
   *
   * This is a read-only operation -- no segments are actually deleted.
   *
   * @returns Detailed retention check result
   */
  checkRetention(): Promise<RetentionCheckResult>;

  /**
   * Convenience method that returns just the segment IDs safe to delete.
   * Equivalent to `(await checkRetention()).eligibleForDeletion`.
   *
   * @returns Array of segment IDs safe to delete
   */
  getSegmentsToDelete(): Promise<string[]>;

  /**
   * Delete eligible segments according to the current retention policy.
   *
   * @param dryRun - If true, return what would be deleted without actually deleting.
   *                 Useful for impact analysis before committing to cleanup.
   * @returns Result including deleted segments, bytes freed, and any failures
   */
  cleanup(dryRun?: boolean): Promise<RetentionCleanupResult>;

  /**
   * Get a copy of the current retention policy configuration.
   * Modifying the returned object does not affect the manager.
   */
  getPolicy(): RetentionPolicy;

  /**
   * Update the retention policy at runtime. The provided fields are merged
   * with the current policy. Useful for adjusting retention dynamically
   * based on operational needs.
   *
   * @param policy - Partial policy fields to merge
   *
   * @example
   * ```typescript
   * // Temporarily reduce retention during high load
   * manager.updatePolicy({ retentionHours: 1, maxTotalBytes: 100_000_000 });
   * ```
   */
  updatePolicy(policy: Partial<RetentionPolicy>): void;
}

// =============================================================================
// Table Retention Types
// =============================================================================

/**
 * Per-table retention policy override.
 * Allows different tables to have different retention periods.
 */
export interface TableRetentionPolicy {
  /** Retention period in hours for this table */
  retentionHours?: number;
  /** Retention period in days for this table */
  retentionDays?: number;
  /** Whether to archive this table's segments to R2 cold storage */
  archiveToR2?: boolean;
}

/**
 * Time window during which cleanup is permitted.
 * Cleanup is blocked outside all configured windows when
 * `blockCleanupOutsideWindows` is enabled.
 */
export interface CleanupWindow {
  /** Start time in HH:MM format (24-hour) */
  start: string;
  /** End time in HH:MM format (24-hour) */
  end: string;
  /** IANA timezone (e.g., "America/New_York"). Defaults to system timezone. */
  timezone?: string;
}

/**
 * Impact analysis for a potential cleanup operation.
 * Returned by `analyzeCleanupImpact()` for pre-cleanup assessment.
 */
export interface CleanupImpact {
  /** Number of segments that would be deleted */
  segmentsToDelete: number;
  /** Estimated bytes that would be freed */
  bytesToFree: number;
  /** Tables with data in the segments to be deleted */
  affectedTables: string[];
  /** LSN of the oldest segment that would be retained */
  oldestRetainedLSN: bigint;
  /** Replication slots affected by the cleanup */
  affectedReplicationSlots: string[];
  /** Estimated duration of the cleanup operation in ms */
  estimatedDuration: number;
  /** Potential risks or issues with this cleanup */
  risks: string[];
}

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
