/**
 * WAL Retention Manager for DoSQL
 *
 * Implements WAL segment cleanup based on multiple retention strategies:
 * - Time-based retention (hours/days/minutes)
 * - Size-based retention (max bytes)
 * - Entry count-based retention (max entries)
 * - Checkpoint-based retention (keep only since last checkpoint)
 * - Compaction triggers
 * - Background cleanup scheduling
 *
 * Features:
 * - Track minimum required LSN across all slots
 * - Identify WAL segments safe to delete
 * - Configurable retention policy (min segments, max age)
 * - Safe deletion that respects active readers
 * - Metrics and observability
 * - CDC/replication integration
 */

import type { FSXBackend } from '../fsx/types.js';
import type { WALReader, WALSegment, WALConfig, WALWriter } from './types.js';
import { DEFAULT_WAL_CONFIG, WALError, WALErrorCode } from './types.js';
import type { ReplicationSlotManager, ReplicationSlot } from '../cdc/types.js';

// =============================================================================
// Retention Policy Types
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

/**
 * Dynamic policy configuration
 */
export interface DynamicPolicyConfig {
  /** Storage threshold (0-1) that triggers reduced retention */
  storageThreshold: number;
  /** Reduced retention period in hours */
  reducedRetentionHours: number;
}

/**
 * CDC integration configuration
 */
export interface CDCIntegrationConfig {
  /** Function to get the pending CDC LSN */
  getPendingLSN: () => Promise<bigint>;
}

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

/**
 * Configuration for WAL retention policy
 */
export interface RetentionPolicy {
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
// Helper Functions
// =============================================================================

/**
 * Parse human-readable size string to bytes
 */
function parseSizeString(size: string): number {
  const match = size.match(/^(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB)$/i);
  if (!match) {
    throw new Error(`Invalid size format: ${size}`);
  }

  const value = parseFloat(match[1]);
  const unit = match[2].toUpperCase();

  const multipliers: Record<string, number> = {
    B: 1,
    KB: 1024,
    MB: 1024 * 1024,
    GB: 1024 * 1024 * 1024,
    TB: 1024 * 1024 * 1024 * 1024,
  };

  return Math.floor(value * multipliers[unit]);
}

/**
 * Parse cron expression and get next run time
 * Simplified implementation supporting common patterns
 */
function getNextCronTime(cronExpression: string): Date {
  // Simple cron parser for common patterns
  // Format: minute hour day month weekday
  const parts = cronExpression.split(' ');
  if (parts.length !== 5) {
    throw new Error(`Invalid cron expression: ${cronExpression}`);
  }

  const now = new Date();
  const next = new Date(now);

  // Parse hour pattern like */6 for "every 6 hours"
  const hourPart = parts[1];
  if (hourPart.startsWith('*/')) {
    const interval = parseInt(hourPart.substring(2), 10);
    const currentHour = now.getHours();
    const nextHour = Math.ceil((currentHour + 1) / interval) * interval;
    next.setHours(nextHour % 24, parseInt(parts[0], 10) || 0, 0, 0);
    if (next <= now) {
      next.setHours(next.getHours() + interval);
    }
  } else if (hourPart !== '*') {
    next.setHours(parseInt(hourPart, 10), parseInt(parts[0], 10) || 0, 0, 0);
    if (next <= now) {
      next.setDate(next.getDate() + 1);
    }
  } else {
    // Every hour at specified minute
    next.setMinutes(parseInt(parts[0], 10) || 0, 0, 0);
    if (next <= now) {
      next.setHours(next.getHours() + 1);
    }
  }

  return next;
}

/**
 * Check if current time is within a time window
 */
function isInTimeWindow(window: LowActivityWindow): boolean {
  const now = new Date();
  const currentTime = now.getHours() * 60 + now.getMinutes();

  const [startHour, startMin] = window.start.split(':').map(Number);
  const [endHour, endMin] = window.end.split(':').map(Number);

  const startTime = startHour * 60 + startMin;
  const endTime = endHour * 60 + endMin;

  if (startTime <= endTime) {
    return currentTime >= startTime && currentTime <= endTime;
  } else {
    // Window crosses midnight
    return currentTime >= startTime || currentTime <= endTime;
  }
}

/**
 * Validate retention policy configuration
 */
function validatePolicy(policy: Partial<RetentionPolicy>): void {
  if (policy.minSegmentCount !== undefined && policy.minSegmentCount < 0) {
    throw new Error('minSegmentCount must be non-negative');
  }
  if (policy.maxSegmentAge !== undefined && policy.maxSegmentAge < 0) {
    throw new Error('maxSegmentAge must be non-negative');
  }
  if (policy.maxTotalBytes !== undefined && policy.maxTotalBytes < 0) {
    throw new Error('maxTotalBytes must be non-negative');
  }
  if (policy.maxEntryCount !== undefined && policy.maxEntryCount < 0) {
    throw new Error('maxEntryCount must be non-negative');
  }
  if (
    policy.compactionThreshold !== undefined &&
    (policy.compactionThreshold < 0 || policy.compactionThreshold > 1)
  ) {
    throw new Error('compactionThreshold must be between 0 and 1');
  }
  if (
    policy.warningThreshold !== undefined &&
    (policy.warningThreshold < 0 || policy.warningThreshold > 1)
  ) {
    throw new Error('warningThreshold must be between 0 and 1');
  }
}

// =============================================================================
// WAL Retention Manager Implementation
// =============================================================================

/**
 * Create a WAL retention manager
 */
export function createWALRetentionManager(
  backend: FSXBackend,
  reader: WALReader,
  slotManager: ReplicationSlotManager | null,
  policy: Partial<RetentionPolicy> = {},
  config: Partial<WALConfig> = {}
): WALRetentionManager & {
  // Extended methods for testing and advanced usage
  getExpiredEntries(): Promise<ExpiredEntry[]>;
  getStorageStats(): Promise<StorageStats>;
  getEntryStats(): Promise<EntryStats>;
  getSegmentEntryStats(): Promise<SegmentEntryStats[]>;
  getCheckpointHistory(): Promise<CheckpointInfo[]>;
  onCheckpointCreated(checkpoint: CheckpointInfo): Promise<void>;
  calculateFragmentation(): Promise<FragmentationInfo>;
  checkCompactionNeeded(): Promise<boolean>;
  mergeSegments(segmentIds: string[]): Promise<MergeResult>;
  compactSegment(segmentId: string): Promise<CompactResult>;
  startBackgroundCleanup(): void;
  stopBackgroundCleanup(): void;
  isRunning(): boolean;
  getNextCleanupTime(): Date | null;
  setThrottleConfig(config: ThrottleConfig): void;
  getThrottleConfig(): ThrottleConfig;
  isInLowActivityWindow(): boolean;
  getMetrics(): Promise<RetentionMetrics>;
  getCleanupHistory(): Promise<CleanupRecord[]>;
  healthCheck(): Promise<HealthCheckResult>;
  evaluateDynamicPolicy(): Promise<DynamicPolicyResult>;
  getReplicationStatus(): Promise<ReplicationStatus>;
  getRegionReplicationStatus(): Promise<RegionReplicationStatus>;
  acquireCleanupLock(): Promise<boolean>;
  isCleanupInProgress(): boolean;
  registerActiveWriter(writer: WALWriter): void;
  parseSegmentLSN(segmentId: string): bigint;
  onWarning?: (warning: RetentionWarning) => void;
  // New methods for WAL retention features
  truncateWAL(beforeLSN: bigint): Promise<TruncateResult>;
  compactWAL(): Promise<CompactWALResult>;
  forceCleanup(options?: ForceCleanupOptions): Promise<ForceCleanupResult>;
  getWALStats(): Promise<WALStats>;
  getCleanupLatencyHistogram(): Promise<CleanupLatencyHistogram>;
  getGrowthStats(): Promise<GrowthStats>;
  registerCheckpointListener(checkpointMgr: any): void;
  onSizeWarning?: (current: number, max: number) => void;
} {
  // Validate policy
  validatePolicy(policy);

  // Resolve preset if specified
  let basePolicy = { ...DEFAULT_RETENTION_POLICY };
  if (policy.preset && RETENTION_PRESETS[policy.preset]) {
    basePolicy = { ...RETENTION_PRESETS[policy.preset] };
  } else if (policy.extends && RETENTION_PRESETS[policy.extends]) {
    basePolicy = { ...RETENTION_PRESETS[policy.extends] };
    if (policy.overrides) {
      Object.assign(basePolicy, policy.overrides);
    }
  }

  const fullPolicy: RetentionPolicy = { ...basePolicy, ...policy };

  // Handle time-based retention convenience properties
  // Priority: retentionMinutes > retentionHours > retentionDays
  if (fullPolicy.retentionMinutes !== undefined) {
    fullPolicy.maxSegmentAge = fullPolicy.retentionMinutes * 60 * 1000;
  } else if (fullPolicy.retentionHours !== undefined) {
    fullPolicy.maxSegmentAge = fullPolicy.retentionHours * 60 * 60 * 1000;
  } else if (fullPolicy.retentionDays !== undefined) {
    fullPolicy.maxSegmentAge = fullPolicy.retentionDays * 24 * 60 * 60 * 1000;
  }

  // Parse human-readable size string
  if (fullPolicy.maxTotalSize && !fullPolicy.maxTotalBytes) {
    fullPolicy.maxTotalBytes = parseSizeString(fullPolicy.maxTotalSize);
  }

  const fullConfig: WALConfig = { ...DEFAULT_WAL_CONFIG, ...config };

  // Track active readers
  const activeReaders = new Map<string, ActiveReader>();

  // Track active writers for coordination
  const activeWriters = new Set<WALWriter>();

  // Background cleanup state
  let cleanupTimer: ReturnType<typeof setInterval> | null = null;
  let cleanupInProgress = false;
  let cleanupLock = false;

  // Metrics tracking
  let lastCleanupTime: number | null = null;
  let lastSegmentsDeleted = 0;
  let lastBytesFreed = 0;
  const cleanupHistory: CleanupRecord[] = [];
  const maxHistorySize = fullPolicy.cleanupHistorySize ?? 100;

  // Checkpoint history
  const checkpointHistory: CheckpointInfo[] = [];

  // Throttle config
  let throttleConfig: ThrottleConfig = {
    maxBatchSize: fullPolicy.maxCleanupBatchSize ?? 100,
    throttleMs: fullPolicy.cleanupThrottleMs ?? 0,
  };

  // Region replication status (mock for now)
  const regionStatus: RegionReplicationStatus = {};
  if (fullPolicy.regions) {
    for (const region of fullPolicy.regions) {
      regionStatus[region] = { lastLSN: 0n, lag: 0, healthy: true };
    }
  }

  /**
   * Parse segment ID to extract start LSN
   */
  function parseSegmentLSN(segmentId: string): bigint {
    const match = segmentId.match(/seg_(\d+)/);
    return match ? BigInt(match[1]) : 0n;
  }

  /**
   * Get segment info including timestamp
   */
  async function getSegmentInfo(
    segmentId: string
  ): Promise<{ startLSN: bigint; endLSN: bigint; createdAt: number; entryCount: number; sizeBytes: number } | null> {
    try {
      const segment = await reader.readSegment(segmentId);
      if (!segment) return null;

      // Get segment size
      const path = `${fullConfig.segmentPrefix}${segmentId}`;
      const data = await backend.read(path);
      const sizeBytes = data ? data.length : 0;

      return {
        startLSN: segment.startLSN,
        endLSN: segment.endLSN,
        createdAt: segment.createdAt,
        entryCount: segment.entries.length,
        sizeBytes,
      };
    } catch {
      return null;
    }
  }

  /**
   * Check if a reader is idle (no activity for readerIdleTimeout)
   */
  function isReaderIdle(readerInfo: ActiveReader): boolean {
    return Date.now() - readerInfo.lastActivityAt >= fullPolicy.readerIdleTimeout;
  }

  /**
   * Check if segment is protected by any active reader
   */
  function isProtectedByReader(segmentId: string, endLSN: bigint): boolean {
    for (const readerInfo of activeReaders.values()) {
      // Skip idle readers
      if (isReaderIdle(readerInfo)) continue;

      // Segment is protected if reader is currently reading it
      // or if reader's position is within the segment's LSN range
      if (readerInfo.currentSegmentId === segmentId) {
        return true;
      }
      if (readerInfo.currentLSN <= endLSN) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the latest checkpoint LSN
   */
  async function getLatestCheckpointLSN(): Promise<bigint | null> {
    try {
      const checkpointData = await backend.read(fullConfig.checkpointPath);
      if (!checkpointData) return null;
      const checkpoint = JSON.parse(new TextDecoder().decode(checkpointData));
      return BigInt(checkpoint.lsn);
    } catch {
      return null;
    }
  }

  /**
   * Emit cleanup progress event
   */
  function emitProgress(event: CleanupProgressEvent): void {
    if (fullPolicy.onCleanupProgress) {
      fullPolicy.onCleanupProgress(event);
    }
  }

  /**
   * Record cleanup in history
   */
  function recordCleanup(record: CleanupRecord): void {
    cleanupHistory.push(record);
    if (cleanupHistory.length > maxHistorySize) {
      cleanupHistory.shift();
    }
  }

  /**
   * Report metric
   */
  function reportMetric(type: string, value: number, labels?: Record<string, string>): void {
    if (fullPolicy.metricsReporter) {
      fullPolicy.metricsReporter.report({
        type,
        value,
        labels,
        timestamp: Date.now(),
      });
    }
  }

  const manager = {
    registerReader(readerInfo: ActiveReader): void {
      activeReaders.set(readerInfo.readerId, {
        ...readerInfo,
        lastActivityAt: Date.now(),
      });
    },

    unregisterReader(readerId: string): void {
      activeReaders.delete(readerId);
    },

    updateReaderPosition(
      readerId: string,
      currentLSN: bigint,
      currentSegmentId: string
    ): void {
      const existing = activeReaders.get(readerId);
      if (existing) {
        activeReaders.set(readerId, {
          ...existing,
          currentLSN,
          currentSegmentId,
          lastActivityAt: Date.now(),
        });
      }
    },

    getActiveReaders(): ActiveReader[] {
      return Array.from(activeReaders.values());
    },

    async getMinSlotLSN(): Promise<bigint | null> {
      if (!slotManager) return null;

      const slots = await slotManager.listSlots();
      if (slots.length === 0) return null;

      let minLSN = slots[0].acknowledgedLSN;
      for (const slot of slots) {
        if (slot.acknowledgedLSN < minLSN) {
          minLSN = slot.acknowledgedLSN;
        }
      }

      return minLSN;
    },

    async checkRetention(): Promise<RetentionCheckResult> {
      const segments = await reader.listSegments(false);
      const now = Date.now();

      const result: RetentionCheckResult = {
        eligibleForDeletion: [],
        protectedBySlots: [],
        protectedByReaders: [],
        protectedByMinCount: [],
        minSlotLSN: null,
        oldestSegmentTime: null,
        totalSegmentCount: segments.length,
        decisions: fullPolicy.verboseLogging ? [] : undefined,
        // Always include extended properties
        segmentsBeforeCheckpoint: [],
        emptySegments: [],
        skippedInvalid: [],
        protectedByCDC: [],
      };

      if (segments.length === 0) {
        return result;
      }

      // Get min slot LSN
      result.minSlotLSN = await manager.getMinSlotLSN();

      // Get checkpoint LSN for checkpoint-based retention
      const checkpointLSN = await getLatestCheckpointLSN();

      // Get CDC pending LSN if configured
      let cdcPendingLSN: bigint | null = null;
      if (fullPolicy.cdcIntegration) {
        try {
          cdcPendingLSN = await fullPolicy.cdcIntegration.getPendingLSN();
        } catch {
          // Ignore CDC errors
        }
      }

      // Batch processing limit
      const maxToProcess = fullPolicy.maxSegmentsToProcess ?? segments.length;
      const segmentsToProcess = segments.slice(0, maxToProcess);
      if (segmentsToProcess.length < segments.length) {
        result.batchProcessed = true;
      }

      // Track size and entry stats
      let totalBytes = 0;
      let totalEntries = 0;
      const emptySegments: string[] = [];
      const skippedInvalid: string[] = [];
      const segmentsBeforeCheckpoint: string[] = [];
      const protectedByCDC: string[] = [];

      // Analyze each segment
      const segmentInfos: Array<{
        id: string;
        startLSN: bigint;
        endLSN: bigint;
        createdAt: number;
        entryCount: number;
        sizeBytes: number;
      }> = [];

      for (const segmentId of segmentsToProcess) {
        // Check for invalid segment ID
        if (!segmentId.match(/^seg_\d+$/)) {
          skippedInvalid.push(segmentId);
          continue;
        }

        const info = await getSegmentInfo(segmentId);
        if (info) {
          segmentInfos.push({ id: segmentId, ...info });
          totalBytes += info.sizeBytes;
          totalEntries += info.entryCount;

          if (info.entryCount === 0) {
            emptySegments.push(segmentId);
          }

          if (
            result.oldestSegmentTime === null ||
            info.createdAt < result.oldestSegmentTime
          ) {
            result.oldestSegmentTime = info.createdAt;
          }
        }
      }

      // Check for clock skew
      let clockSkewDetected = false;
      if (fullPolicy.handleClockSkew && segmentInfos.length > 1) {
        for (let i = 1; i < segmentInfos.length; i++) {
          const prev = segmentInfos[i - 1];
          const curr = segmentInfos[i];
          // If a later segment (by LSN) has an earlier timestamp, there's clock skew
          if (curr.startLSN > prev.startLSN && curr.createdAt < prev.createdAt) {
            clockSkewDetected = true;
            break;
          }
        }
      }

      result.emptySegments = emptySegments;
      result.skippedInvalid = skippedInvalid;
      result.clockSkewDetected = clockSkewDetected;

      // Calculate bytes/entries over limit
      if (fullPolicy.maxTotalBytes && totalBytes > fullPolicy.maxTotalBytes) {
        result.bytesOverLimit = totalBytes - fullPolicy.maxTotalBytes;

        // Check warning threshold
        if (fullPolicy.warningThreshold && fullPolicy.onWarning) {
          const usage = totalBytes / fullPolicy.maxTotalBytes;
          if (usage >= fullPolicy.warningThreshold) {
            fullPolicy.onWarning({
              type: 'size_threshold',
              currentValue: totalBytes,
              threshold: fullPolicy.maxTotalBytes,
              message: `WAL size at ${(usage * 100).toFixed(1)}% of limit`,
            });
          }
        }
      }

      if (fullPolicy.maxEntryCount && totalEntries > fullPolicy.maxEntryCount) {
        result.entriesOverLimit = totalEntries - fullPolicy.maxEntryCount;
      }

      // Sort by LSN
      segmentInfos.sort((a, b) =>
        a.startLSN < b.startLSN ? -1 : a.startLSN > b.startLSN ? 1 : 0
      );

      // Determine minimum segments to keep
      const segmentsToKeep = Math.max(
        fullPolicy.minSegmentCount,
        segments.length - segmentInfos.length
      );
      const keepCount = Math.min(segmentsToKeep, segmentInfos.length);

      // Segments protected by min count (the most recent ones)
      const protectedByMinCount = new Set<string>();
      for (let i = segmentInfos.length - keepCount; i < segmentInfos.length; i++) {
        if (i >= 0) {
          protectedByMinCount.add(segmentInfos[i].id);
          result.protectedByMinCount.push(segmentInfos[i].id);
        }
      }

      // Check each segment
      for (const segmentInfo of segmentInfos) {
        const reasons: string[] = [];
        let eligible = true;

        // Skip if protected by min count
        if (protectedByMinCount.has(segmentInfo.id)) {
          if (fullPolicy.verboseLogging) {
            result.decisions!.push({
              segmentId: segmentInfo.id,
              decision: 'keep',
              reasons: ['Protected by minimum segment count policy'],
            });
          }
          continue;
        }

        // Check slot protection
        if (
          fullPolicy.respectSlotPositions &&
          result.minSlotLSN !== null &&
          segmentInfo.endLSN >= result.minSlotLSN
        ) {
          result.protectedBySlots.push(segmentInfo.id);
          reasons.push('Protected by replication slot');
          eligible = false;
        }

        // Check reader protection
        if (eligible && isProtectedByReader(segmentInfo.id, segmentInfo.endLSN)) {
          result.protectedByReaders.push(segmentInfo.id);
          reasons.push('Protected by active reader');
          eligible = false;
        }

        // Check CDC protection
        if (eligible && cdcPendingLSN !== null && segmentInfo.endLSN >= cdcPendingLSN) {
          protectedByCDC.push(segmentInfo.id);
          reasons.push('Protected by pending CDC events');
          eligible = false;
        }

        // Check checkpoint-based retention in strict mode
        if (
          fullPolicy.checkpointRetentionMode === 'strict' &&
          checkpointLSN !== null &&
          segmentInfo.endLSN < checkpointLSN
        ) {
          segmentsBeforeCheckpoint.push(segmentInfo.id);
        }

        // Check age (must be older than maxSegmentAge) - only if not using clock skew handling or no skew detected
        if (eligible && !clockSkewDetected) {
          const age = now - segmentInfo.createdAt;
          if (age < fullPolicy.maxSegmentAge) {
            reasons.push(`Not old enough (age: ${age}ms, required: ${fullPolicy.maxSegmentAge}ms)`);
            eligible = false;
          }
        }

        if (eligible) {
          result.eligibleForDeletion.push(segmentInfo.id);
          reasons.push('Eligible for deletion');
        }

        if (fullPolicy.verboseLogging) {
          result.decisions!.push({
            segmentId: segmentInfo.id,
            decision: eligible ? 'delete' : 'keep',
            reasons,
          });
        }
      }

      // Include empty segments as eligible
      for (const emptySegment of emptySegments) {
        if (!result.eligibleForDeletion.includes(emptySegment) && !protectedByMinCount.has(emptySegment)) {
          result.eligibleForDeletion.push(emptySegment);
        }
      }

      result.segmentsBeforeCheckpoint = segmentsBeforeCheckpoint;
      result.protectedByCDC = protectedByCDC;

      return result;
    },

    async getSegmentsToDelete(): Promise<string[]> {
      const check = await manager.checkRetention();
      return check.eligibleForDeletion;
    },

    async cleanup(dryRun = false): Promise<RetentionCleanupResult> {
      const startTime = Date.now();

      emitProgress({ type: 'start' });

      const result: RetentionCleanupResult = {
        deleted: [],
        archived: [],
        failed: [],
        bytesFreed: 0,
        durationMs: 0,
        corruptedSegments: [],
      };

      // Check for concurrent cleanup
      if (cleanupInProgress) {
        result.durationMs = Date.now() - startTime;
        emitProgress({ type: 'complete', bytesFreed: 0 });
        return result;
      }

      cleanupInProgress = true;

      try {
        const segmentsToDelete = await manager.getSegmentsToDelete();

        if (dryRun) {
          result.deleted = [...segmentsToDelete];
          result.durationMs = Date.now() - startTime;
          cleanupInProgress = false;
          emitProgress({ type: 'complete', bytesFreed: 0 });
          return result;
        }

        const totalSegments = segmentsToDelete.length;
        let processed = 0;

        for (const segmentId of segmentsToDelete) {
          // Check batch size limit
          if (throttleConfig.maxBatchSize > 0 && processed >= throttleConfig.maxBatchSize) {
            break;
          }

          try {
            const sourcePath = `${fullConfig.segmentPrefix}${segmentId}`;

            // Get segment size for bytes freed calculation
            let data: Uint8Array | null = null;
            try {
              data = await backend.read(sourcePath);
            } catch (error) {
              // Segment might be corrupted
              result.corruptedSegments!.push(segmentId);
              continue;
            }

            if (!data) continue;

            const segmentSize = data.length;

            // Archive if configured
            if (fullPolicy.archiveBeforeDelete) {
              const archivePath = `${fullConfig.archivePrefix}${segmentId}`;
              await backend.write(archivePath, data);
              result.archived.push(segmentId);
            }

            // Delete segment
            await backend.delete(sourcePath);
            result.deleted.push(segmentId);
            result.bytesFreed += segmentSize;

            processed++;

            emitProgress({
              type: 'progress',
              segmentsProcessed: processed,
              segmentsTotal: totalSegments,
              bytesFreed: result.bytesFreed,
            });

            // Apply throttle delay
            if (throttleConfig.throttleMs > 0) {
              await new Promise((resolve) => setTimeout(resolve, throttleConfig.throttleMs));
            }
          } catch (error) {
            result.failed.push({
              segmentId,
              error: error instanceof Error ? error.message : String(error),
            });
          }
        }

        result.durationMs = Date.now() - startTime;

        // Record cleanup
        lastCleanupTime = Date.now();
        lastSegmentsDeleted = result.deleted.length;
        lastBytesFreed = result.bytesFreed;

        recordCleanup({
          timestamp: lastCleanupTime,
          segmentsDeleted: lastSegmentsDeleted,
          bytesFreed: lastBytesFreed,
          durationMs: result.durationMs,
        });

        reportMetric('cleanup_segments_deleted', lastSegmentsDeleted);
        reportMetric('cleanup_bytes_freed', lastBytesFreed);
        reportMetric('cleanup_duration_ms', result.durationMs);

        emitProgress({ type: 'complete', bytesFreed: result.bytesFreed });
      } finally {
        cleanupInProgress = false;
      }

      return result;
    },

    getPolicy(): RetentionPolicy {
      return { ...fullPolicy };
    },

    updatePolicy(policy: Partial<RetentionPolicy>): void {
      validatePolicy(policy);
      Object.assign(fullPolicy, policy);

      // Re-process time-based convenience properties
      if (policy.retentionMinutes !== undefined) {
        fullPolicy.maxSegmentAge = policy.retentionMinutes * 60 * 1000;
      } else if (policy.retentionHours !== undefined) {
        fullPolicy.maxSegmentAge = policy.retentionHours * 60 * 60 * 1000;
      } else if (policy.retentionDays !== undefined) {
        fullPolicy.maxSegmentAge = policy.retentionDays * 24 * 60 * 60 * 1000;
      }

      if (policy.maxTotalSize) {
        fullPolicy.maxTotalBytes = parseSizeString(policy.maxTotalSize);
      }
    },

    // ========== Extended methods ==========

    async getExpiredEntries(): Promise<ExpiredEntry[]> {
      const entries: ExpiredEntry[] = [];
      const now = Date.now();
      const maxAge = fullPolicy.retentionHours
        ? fullPolicy.retentionHours * 60 * 60 * 1000
        : fullPolicy.maxSegmentAge;

      const segments = await reader.listSegments(false);
      for (const segmentId of segments) {
        const segment = await reader.readSegment(segmentId);
        if (!segment) continue;

        for (const entry of segment.entries) {
          const age = now - entry.timestamp;
          if (age > maxAge) {
            entries.push({
              lsn: entry.lsn,
              segmentId,
              entryTimestamp: entry.timestamp,
              age,
              txnId: entry.txnId,
              table: entry.table,
              op: entry.op,
            });
          }
        }
      }

      return entries;
    },

    async getStorageStats(): Promise<StorageStats> {
      const segments = await reader.listSegments(false);
      let totalBytes = 0;

      for (const segmentId of segments) {
        const path = `${fullConfig.segmentPrefix}${segmentId}`;
        const data = await backend.read(path);
        if (data) {
          totalBytes += data.length;
        }
      }

      return {
        totalBytes,
        segmentCount: segments.length,
        averageSegmentSize: segments.length > 0 ? totalBytes / segments.length : 0,
      };
    },

    async getEntryStats(): Promise<EntryStats> {
      const segments = await reader.listSegments(false);
      let totalEntries = 0;

      for (const segmentId of segments) {
        const segment = await reader.readSegment(segmentId);
        if (segment) {
          totalEntries += segment.entries.length;
        }
      }

      return { totalEntries };
    },

    async getSegmentEntryStats(): Promise<SegmentEntryStats[]> {
      const segments = await reader.listSegments(false);
      const stats: SegmentEntryStats[] = [];

      for (const segmentId of segments) {
        const segment = await reader.readSegment(segmentId);
        const path = `${fullConfig.segmentPrefix}${segmentId}`;
        const data = await backend.read(path);

        if (segment && data) {
          stats.push({
            segmentId,
            entryCount: segment.entries.length,
            sizeBytes: data.length,
          });
        }
      }

      return stats;
    },

    async getCheckpointHistory(): Promise<CheckpointInfo[]> {
      return [...checkpointHistory];
    },

    async onCheckpointCreated(checkpoint: CheckpointInfo): Promise<void> {
      checkpointHistory.push(checkpoint);

      // Keep only the configured number of checkpoints in history
      const keepCount = fullPolicy.keepCheckpointCount ?? 3;
      while (checkpointHistory.length > keepCount) {
        checkpointHistory.shift();
      }

      if (fullPolicy.cleanupOnCheckpoint) {
        if (fullPolicy.onCleanupTriggered) {
          fullPolicy.onCleanupTriggered();
        }
        await manager.cleanup(false);
      }
    },

    async calculateFragmentation(): Promise<FragmentationInfo> {
      const segments = await reader.listSegments(false);
      let totalEntries = 0;
      let rolledBackEntries = 0;

      // Track rolled-back transaction IDs
      const rolledBackTxns = new Set<string>();

      // First pass: identify rolled-back transactions
      for (const segmentId of segments) {
        const segment = await reader.readSegment(segmentId);
        if (!segment) continue;

        for (const entry of segment.entries) {
          if (entry.op === 'ROLLBACK') {
            rolledBackTxns.add(entry.txnId);
          }
        }
      }

      // Second pass: count entries
      for (const segmentId of segments) {
        const segment = await reader.readSegment(segmentId);
        if (!segment) continue;

        for (const entry of segment.entries) {
          totalEntries++;
          if (rolledBackTxns.has(entry.txnId)) {
            rolledBackEntries++;
          }
        }
      }

      const ratio = totalEntries > 0 ? rolledBackEntries / totalEntries : 0;

      return {
        ratio,
        rolledBackEntries,
        totalEntries,
      };
    },

    async checkCompactionNeeded(): Promise<boolean> {
      if (!fullPolicy.compactionThreshold) return false;

      const fragmentation = await manager.calculateFragmentation();
      const needed = fragmentation.ratio >= fullPolicy.compactionThreshold;

      if (needed && fullPolicy.onCompactionNeeded) {
        fullPolicy.onCompactionNeeded();
      }

      return needed;
    },

    async mergeSegments(segmentIds: string[]): Promise<MergeResult> {
      if (segmentIds.length < 2) {
        return { newSegmentId: segmentIds[0] || '', mergedCount: 0, bytesSaved: 0 };
      }

      // Load all segments
      const segments: WALSegment[] = [];
      let totalOriginalSize = 0;

      for (const id of segmentIds) {
        const segment = await reader.readSegment(id);
        const path = `${fullConfig.segmentPrefix}${id}`;
        const data = await backend.read(path);
        if (segment && data) {
          segments.push(segment);
          totalOriginalSize += data.length;
        }
      }

      if (segments.length < 2) {
        return { newSegmentId: segmentIds[0] || '', mergedCount: 0, bytesSaved: 0 };
      }

      // Merge entries
      const allEntries = segments.flatMap((s) => s.entries);
      allEntries.sort((a, b) => (a.lsn < b.lsn ? -1 : a.lsn > b.lsn ? 1 : 0));

      const newSegmentId = `seg_${allEntries[0].lsn.toString().padStart(20, '0')}`;
      const newSegment: WALSegment = {
        id: newSegmentId,
        startLSN: allEntries[0].lsn,
        endLSN: allEntries[allEntries.length - 1].lsn,
        entries: allEntries,
        checksum: 0, // Will be recalculated
        createdAt: Date.now(),
      };

      // Write new segment
      const encoder = new (await import('./writer.js')).DefaultWALEncoder();
      const newData = encoder.encodeSegment(newSegment);
      newSegment.checksum = encoder.calculateChecksum(newData);
      const finalData = encoder.encodeSegment(newSegment);

      await backend.write(`${fullConfig.segmentPrefix}${newSegmentId}`, finalData);

      // Delete old segments
      for (const id of segmentIds) {
        await backend.delete(`${fullConfig.segmentPrefix}${id}`);
      }

      return {
        newSegmentId,
        mergedCount: segments.length,
        bytesSaved: totalOriginalSize - finalData.length,
      };
    },

    async compactSegment(segmentId: string): Promise<CompactResult> {
      const segment = await reader.readSegment(segmentId);
      if (!segment) {
        return { entriesRemoved: 0, bytesSaved: 0 };
      }

      const path = `${fullConfig.segmentPrefix}${segmentId}`;
      const originalData = await backend.read(path);
      if (!originalData) {
        return { entriesRemoved: 0, bytesSaved: 0 };
      }

      // Identify rolled-back transactions
      const rolledBackTxns = new Set<string>();
      for (const entry of segment.entries) {
        if (entry.op === 'ROLLBACK') {
          rolledBackTxns.add(entry.txnId);
        }
      }

      // Filter out rolled-back entries
      const keptEntries = segment.entries.filter(
        (e) => !rolledBackTxns.has(e.txnId)
      );

      const entriesRemoved = segment.entries.length - keptEntries.length;

      if (entriesRemoved === 0) {
        return { entriesRemoved: 0, bytesSaved: 0 };
      }

      // Create new compacted segment
      const compactedSegment: WALSegment = {
        ...segment,
        entries: keptEntries,
        endLSN: keptEntries.length > 0 ? keptEntries[keptEntries.length - 1].lsn : segment.startLSN,
      };

      // Write new segment
      const encoder = new (await import('./writer.js')).DefaultWALEncoder();
      const newData = encoder.encodeSegment(compactedSegment);
      compactedSegment.checksum = encoder.calculateChecksum(newData);
      const finalData = encoder.encodeSegment(compactedSegment);

      await backend.write(path, finalData);

      return {
        entriesRemoved,
        bytesSaved: originalData.length - finalData.length,
      };
    },

    startBackgroundCleanup(): void {
      if (cleanupTimer) return;

      const intervalMs = fullPolicy.cleanupIntervalMs ?? 60000;
      cleanupTimer = setInterval(async () => {
        // Check low activity window if configured
        if (fullPolicy.preferLowActivityCleanup && fullPolicy.lowActivityWindow) {
          if (!isInTimeWindow(fullPolicy.lowActivityWindow)) {
            return;
          }
        }

        await manager.cleanup(false);
      }, intervalMs);
    },

    stopBackgroundCleanup(): void {
      if (cleanupTimer) {
        clearInterval(cleanupTimer);
        cleanupTimer = null;
      }
    },

    isRunning(): boolean {
      return cleanupTimer !== null;
    },

    getNextCleanupTime(): Date | null {
      if (fullPolicy.cleanupSchedule) {
        try {
          return getNextCronTime(fullPolicy.cleanupSchedule);
        } catch {
          return null;
        }
      }
      return null;
    },

    setThrottleConfig(config: ThrottleConfig): void {
      throttleConfig = { ...config };
    },

    getThrottleConfig(): ThrottleConfig {
      return { ...throttleConfig };
    },

    isInLowActivityWindow(): boolean {
      if (!fullPolicy.lowActivityWindow) return false;
      return isInTimeWindow(fullPolicy.lowActivityWindow);
    },

    async getMetrics(): Promise<RetentionMetrics> {
      const stats = await manager.getStorageStats();
      const segments = await reader.listSegments(false);

      let oldestSegmentAge: number | null = null;
      for (const segmentId of segments) {
        const segment = await reader.readSegment(segmentId);
        if (segment) {
          const age = Date.now() - segment.createdAt;
          if (oldestSegmentAge === null || age > oldestSegmentAge) {
            oldestSegmentAge = age;
          }
        }
      }

      return {
        totalSegments: stats.segmentCount,
        totalBytes: stats.totalBytes,
        oldestSegmentAge,
        lastCleanupTime,
        segmentsDeleted: lastSegmentsDeleted,
        bytesFreed: lastBytesFreed,
      };
    },

    async getCleanupHistory(): Promise<CleanupRecord[]> {
      return [...cleanupHistory];
    },

    async healthCheck(): Promise<HealthCheckResult> {
      const issues: string[] = [];
      const recommendations: string[] = [];
      let status: 'healthy' | 'warning' | 'critical' = 'healthy';

      const stats = await manager.getStorageStats();
      const metrics = await manager.getMetrics();

      // Check storage usage
      if (fullPolicy.maxTotalBytes) {
        const usage = stats.totalBytes / fullPolicy.maxTotalBytes;
        if (usage >= 0.95) {
          status = 'critical';
          issues.push('Storage usage exceeds 95%');
          recommendations.push('Consider increasing maxTotalBytes or running cleanup immediately');
        } else if (usage >= 0.8) {
          status = 'warning';
          issues.push('Storage usage exceeds 80%');
          recommendations.push('Consider adjusting retention policy');
        }
      }

      // Check segment age
      if (metrics.oldestSegmentAge !== null) {
        const maxAge = fullPolicy.maxSegmentAge;
        if (metrics.oldestSegmentAge > maxAge * 2) {
          if (status !== 'critical') status = 'warning';
          issues.push('Segments older than twice the retention period exist');
          recommendations.push('Check if cleanup is running properly');
        }
      }

      // Check fragmentation
      const fragmentation = await manager.calculateFragmentation();
      if (fragmentation.ratio > 0.5) {
        if (status !== 'critical') status = 'warning';
        issues.push(`High fragmentation (${(fragmentation.ratio * 100).toFixed(1)}%)`);
        recommendations.push('Consider running compaction');
      }

      return { status, issues, recommendations };
    },

    async evaluateDynamicPolicy(): Promise<DynamicPolicyResult> {
      if (!fullPolicy.dynamicPolicy) {
        return { applied: false, reason: 'Dynamic policy not configured' };
      }

      const stats = await manager.getStorageStats();
      if (!fullPolicy.maxTotalBytes) {
        return { applied: false, reason: 'maxTotalBytes not configured' };
      }

      const usage = stats.totalBytes / fullPolicy.maxTotalBytes;
      if (usage >= fullPolicy.dynamicPolicy.storageThreshold) {
        // Apply reduced retention
        fullPolicy.maxSegmentAge = fullPolicy.dynamicPolicy.reducedRetentionHours * 60 * 60 * 1000;
        return {
          applied: true,
          reason: `Storage usage (${(usage * 100).toFixed(1)}%) exceeded threshold, reduced retention to ${fullPolicy.dynamicPolicy.reducedRetentionHours} hours`,
        };
      }

      return { applied: false, reason: 'Storage usage within threshold' };
    },

    async getReplicationStatus(): Promise<ReplicationStatus> {
      if (!slotManager) {
        return { currentLag: 0, isWithinTolerance: true };
      }

      const slots = await slotManager.listSlots();
      if (slots.length === 0) {
        return { currentLag: 0, isWithinTolerance: true };
      }

      // Get current WAL position (from latest segment)
      const segments = await reader.listSegments(false);
      let currentLSN = 0n;
      if (segments.length > 0) {
        const lastSegment = await reader.readSegment(segments[segments.length - 1]);
        if (lastSegment) {
          currentLSN = lastSegment.endLSN;
        }
      }

      // Calculate max lag across all slots
      let maxLag = 0;
      for (const slot of slots) {
        const lag = Number(currentLSN - slot.acknowledgedLSN);
        if (lag > maxLag) {
          maxLag = lag;
        }
      }

      const tolerance = fullPolicy.replicationLagTolerance ?? 1000;
      return {
        currentLag: maxLag,
        isWithinTolerance: maxLag <= tolerance,
      };
    },

    async getRegionReplicationStatus(): Promise<RegionReplicationStatus> {
      return { ...regionStatus };
    },

    async acquireCleanupLock(): Promise<boolean> {
      if (cleanupLock) return false;
      cleanupLock = true;
      return true;
    },

    isCleanupInProgress(): boolean {
      return cleanupInProgress;
    },

    registerActiveWriter(writer: WALWriter): void {
      activeWriters.add(writer);
    },

    parseSegmentLSN,

    onWarning: fullPolicy.onWarning,

    // ========== New methods for WAL retention features ==========

    /**
     * Truncate WAL before a given LSN
     */
    async truncateWAL(beforeLSN: bigint): Promise<TruncateResult> {
      const result: TruncateResult = {
        truncatedSegments: [],
        bytesFreed: 0,
        truncatedLSN: beforeLSN,
      };

      const segments = await reader.listSegments(false);

      for (const segmentId of segments) {
        const segment = await reader.readSegment(segmentId);
        if (!segment) continue;

        // Only delete segments whose endLSN is less than beforeLSN
        if (segment.endLSN < beforeLSN) {
          const path = `${fullConfig.segmentPrefix}${segmentId}`;
          const data = await backend.read(path);
          if (data) {
            result.bytesFreed += data.length;
          }
          await backend.delete(path);
          result.truncatedSegments.push(segmentId);
        }
      }

      return result;
    },

    /**
     * Compact WAL by removing rolled-back transaction entries
     */
    async compactWAL(): Promise<CompactWALResult> {
      const result: CompactWALResult = {
        segmentsCompacted: 0,
        entriesRemoved: 0,
        bytesReclaimed: 0,
      };

      const segments = await reader.listSegments(false);

      // First pass: identify all rolled-back transaction IDs across all segments
      const rolledBackTxns = new Set<string>();
      for (const segmentId of segments) {
        const segment = await reader.readSegment(segmentId);
        if (!segment) continue;

        for (const entry of segment.entries) {
          if (entry.op === 'ROLLBACK') {
            rolledBackTxns.add(entry.txnId);
          }
        }
      }

      // Second pass: compact each segment by removing rolled-back entries
      for (const segmentId of segments) {
        const segment = await reader.readSegment(segmentId);
        if (!segment) continue;

        const path = `${fullConfig.segmentPrefix}${segmentId}`;
        const originalData = await backend.read(path);
        if (!originalData) continue;

        // Filter out entries belonging to rolled-back transactions
        const keptEntries = segment.entries.filter(
          (e) => !rolledBackTxns.has(e.txnId)
        );

        const removedCount = segment.entries.length - keptEntries.length;
        if (removedCount === 0) continue;

        // If all entries are removed, delete the segment
        if (keptEntries.length === 0) {
          await backend.delete(path);
          result.segmentsCompacted++;
          result.entriesRemoved += removedCount;
          result.bytesReclaimed += originalData.length;
          continue;
        }

        // Create compacted segment
        const compactedSegment: WALSegment = {
          ...segment,
          entries: keptEntries,
          endLSN: keptEntries[keptEntries.length - 1].lsn,
        };

        // Re-encode and write
        const encoder = new (await import('./writer.js')).DefaultWALEncoder();
        const newData = encoder.encodeSegment(compactedSegment);
        compactedSegment.checksum = encoder.calculateChecksum(newData);
        const finalData = encoder.encodeSegment(compactedSegment);

        await backend.write(path, finalData);

        result.segmentsCompacted++;
        result.entriesRemoved += removedCount;
        result.bytesReclaimed += originalData.length - finalData.length;
      }

      return result;
    },

    /**
     * Force cleanup ignoring safety checks
     */
    async forceCleanup(options: ForceCleanupOptions = {}): Promise<ForceCleanupResult> {
      const result: ForceCleanupResult = {
        deleted: [],
        bytesFreed: 0,
      };

      const segments = await reader.listSegments(false);

      for (const segmentId of segments) {
        const path = `${fullConfig.segmentPrefix}${segmentId}`;
        const data = await backend.read(path);

        if (data) {
          result.bytesFreed += data.length;
          await backend.delete(path);
          result.deleted.push(segmentId);
        }
      }

      return result;
    },

    /**
     * Get comprehensive WAL statistics
     */
    async getWALStats(): Promise<WALStats> {
      const segments = await reader.listSegments(false);
      let totalBytes = 0;
      let totalEntries = 0;
      let oldestEntryTimestamp: number | null = null;
      let newestEntryTimestamp: number | null = null;
      let deadEntriesCount = 0;
      const activeTransactions = new Set<string>();
      const rolledBackTxns = new Set<string>();
      const committedTxns = new Set<string>();

      // First pass: identify transaction states and collect stats
      for (const segmentId of segments) {
        const segment = await reader.readSegment(segmentId);
        if (!segment) continue;

        const path = `${fullConfig.segmentPrefix}${segmentId}`;
        const data = await backend.read(path);
        if (data) {
          totalBytes += data.length;
        }

        for (const entry of segment.entries) {
          totalEntries++;

          // Track timestamps
          if (oldestEntryTimestamp === null || entry.timestamp < oldestEntryTimestamp) {
            oldestEntryTimestamp = entry.timestamp;
          }
          if (newestEntryTimestamp === null || entry.timestamp > newestEntryTimestamp) {
            newestEntryTimestamp = entry.timestamp;
          }

          // Track transaction states
          if (entry.op === 'BEGIN') {
            activeTransactions.add(entry.txnId);
          } else if (entry.op === 'COMMIT') {
            activeTransactions.delete(entry.txnId);
            committedTxns.add(entry.txnId);
          } else if (entry.op === 'ROLLBACK') {
            activeTransactions.delete(entry.txnId);
            rolledBackTxns.add(entry.txnId);
          }
        }
      }

      // Second pass: count dead entries (from rolled-back transactions)
      for (const segmentId of segments) {
        const segment = await reader.readSegment(segmentId);
        if (!segment) continue;

        for (const entry of segment.entries) {
          if (rolledBackTxns.has(entry.txnId)) {
            deadEntriesCount++;
          }
        }
      }

      const fragmentationRatio = totalEntries > 0 ? deadEntriesCount / totalEntries : 0;
      const averageSegmentSize = segments.length > 0 ? totalBytes / segments.length : 0;

      return {
        totalSegments: segments.length,
        totalEntries,
        totalBytes,
        oldestEntryTimestamp,
        newestEntryTimestamp,
        averageSegmentSize,
        fragmentationRatio,
        deadEntriesCount,
        activeTransactionCount: activeTransactions.size,
      };
    },

    /**
     * Get cleanup latency histogram
     */
    async getCleanupLatencyHistogram(): Promise<CleanupLatencyHistogram> {
      // Record cleanup duration for histogram
      const latencies = cleanupHistory.map((r) => r.durationMs);

      if (latencies.length === 0) {
        return {
          p50: 0,
          p90: 0,
          p99: 0,
          min: 0,
          max: 0,
          avg: 0,
          count: 0,
        };
      }

      const sorted = [...latencies].sort((a, b) => a - b);
      const count = sorted.length;

      function percentile(p: number): number {
        const index = Math.ceil((p / 100) * count) - 1;
        return sorted[Math.max(0, Math.min(index, count - 1))];
      }

      return {
        p50: percentile(50),
        p90: percentile(90),
        p99: percentile(99),
        min: sorted[0],
        max: sorted[count - 1],
        avg: latencies.reduce((a, b) => a + b, 0) / count,
        count,
      };
    },

    /**
     * Get WAL growth rate statistics
     */
    async getGrowthStats(): Promise<GrowthStats> {
      // Calculate growth based on cleanup history and current stats
      const stats = await manager.getStorageStats();
      const history = cleanupHistory;

      // If not enough history, return zeros
      if (history.length < 2) {
        return {
          bytesPerHour: 0,
          segmentsPerHour: 0,
          entriesPerHour: 0,
          estimatedTimeToLimit: null,
        };
      }

      // Calculate time span and deltas from history
      const oldestRecord = history[0];
      const newestRecord = history[history.length - 1];
      const timeSpanHours = (newestRecord.timestamp - oldestRecord.timestamp) / (1000 * 60 * 60);

      if (timeSpanHours <= 0) {
        return {
          bytesPerHour: 0,
          segmentsPerHour: 0,
          entriesPerHour: 0,
          estimatedTimeToLimit: null,
        };
      }

      // Sum bytes freed as proxy for growth rate
      const totalBytesFreed = history.reduce((sum, r) => sum + r.bytesFreed, 0);
      const bytesPerHour = totalBytesFreed / timeSpanHours;

      // Calculate entry stats
      const entryStats = await manager.getEntryStats();

      // Estimate time to limit
      let estimatedTimeToLimit: number | null = null;
      if (fullPolicy.maxTotalBytes && bytesPerHour > 0) {
        const remainingBytes = fullPolicy.maxTotalBytes - stats.totalBytes;
        if (remainingBytes > 0) {
          estimatedTimeToLimit = remainingBytes / bytesPerHour;
        } else {
          estimatedTimeToLimit = 0;
        }
      }

      return {
        bytesPerHour,
        segmentsPerHour: stats.segmentCount / Math.max(timeSpanHours, 1),
        entriesPerHour: entryStats.totalEntries / Math.max(timeSpanHours, 1),
        estimatedTimeToLimit,
      };
    },

    /**
     * Register checkpoint listener for auto-cleanup
     */
    registerCheckpointListener(checkpointMgr: any): void {
      // Store original createCheckpoint method
      const originalCreateCheckpoint = checkpointMgr.createCheckpoint.bind(checkpointMgr);

      // Wrap createCheckpoint to trigger cleanup
      checkpointMgr.createCheckpoint = async (lsn: bigint, activeTransactions: string[]) => {
        const checkpoint = await originalCreateCheckpoint(lsn, activeTransactions);

        // Trigger cleanup callback and cleanup if configured
        if (fullPolicy.cleanupOnCheckpoint) {
          if (fullPolicy.onCleanupTriggered) {
            fullPolicy.onCleanupTriggered();
          }
          await manager.cleanup(false);
        }

        return checkpoint;
      };
    },

    /**
     * Size warning callback
     */
    onSizeWarning: fullPolicy.onSizeWarning,
  };

  return manager;
}

// =============================================================================
// Retention Error Types
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
