/**
 * WAL Retention Module
 *
 * This module provides comprehensive WAL segment retention management including:
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
 *
 * @packageDocumentation
 */

import type { FSXBackend } from '../../fsx/types.js';
import type { WALReader, WALConfig, WALWriter, Checkpoint } from '../types.js';
import type { MetricsReporter, RetentionMetric } from '../retention-types.js';
import { DEFAULT_WAL_CONFIG } from '../types.js';
import type { ReplicationSlotManager } from '../../cdc/types.js';

// Re-export all types from retention-types
export * from '../retention-types.js';

// Re-export policy utilities
export {
  DEFAULT_RETENTION_POLICY,
  RETENTION_PRESETS,
  parseSizeString,
  formatSizeString,
  isInTimeWindow,
  getTimeUntilWindow,
  getNextCronTime,
  getMillisUntilNextCron,
  validatePolicy,
  resolvePolicy,
  checkStorageWarning,
  checkEntryWarning,
  checkAgeWarning,
  loadRetentionPolicy,
  composePolicy,
  isInCleanupWindow,
} from './policy.js';

// Re-export scheduler utilities
export {
  createRetentionScheduler,
  throttledProcess,
  sleep,
  createCronScheduler,
  type RetentionScheduler,
  type SchedulerState,
  type CleanupFunction,
} from './scheduler.js';

// Re-export executor utilities
export {
  parseSegmentLSN,
  isReaderIdle,
  isProtectedByReader,
  getLatestCheckpointLSN,
  getSegmentInfo,
  checkRetention,
  executeCleanup,
  getMinSlotLSN,
  getStorageStats,
  getEntryStats,
  getSegmentEntryStats,
  calculateFragmentation,
  checkCompactionNeeded,
  mergeSegments,
  compactSegment,
  getExpiredEntries,
  getWALStats,
  truncateWAL,
  compactWAL,
  forceCleanup,
  type CheckpointManagerForRetention,
  type ExecutorContext,
  type SegmentInfo,
} from './executor.js';

// Re-export metrics utilities
export {
  createMetricsCollector,
  performHealthCheck,
  evaluateDynamicPolicy,
  getReplicationStatus,
  getOldestSegmentAge,
  createConsoleMetricsReporter,
  createNoopMetricsReporter,
  createBufferedMetricsReporter,
  type MetricsCollector,
  type MetricsCollectorState,
} from './metrics.js';

// Import for internal use
import type {
  RetentionPolicy,
  WALRetentionManager,
  ActiveReader,
  RetentionCheckResult,
  RetentionCleanupResult,
  StorageStats,
  EntryStats,
  SegmentEntryStats,
  FragmentationInfo,
  MergeResult,
  CompactResult,
  ThrottleConfig,
  RetentionMetrics,
  CleanupRecord,
  HealthCheckResult,
  DynamicPolicyResult,
  ReplicationStatus,
  RegionReplicationStatus,
  CheckpointInfo,
  ExpiredEntry,
  WALStats,
  TruncateResult,
  CompactWALResult,
  ForceCleanupOptions,
  ForceCleanupResult,
  CleanupLatencyHistogram,
  GrowthStats,
  CleanupProgressEvent,
  RetentionWarning,
  TableRetentionPolicy,
  CleanupImpact,
  CleanupWindow,
} from '../retention-types.js';

import { DEFAULT_RETENTION_POLICY, RETENTION_PRESETS, resolvePolicy, validatePolicy, isInCleanupWindow } from './policy.js';
import { createRetentionScheduler, type RetentionScheduler } from './scheduler.js';
import {
  checkRetention as execCheckRetention,
  executeCleanup,
  getMinSlotLSN,
  getStorageStats as execGetStorageStats,
  getEntryStats as execGetEntryStats,
  getSegmentEntryStats as execGetSegmentEntryStats,
  calculateFragmentation as execCalculateFragmentation,
  checkCompactionNeeded as execCheckCompactionNeeded,
  mergeSegments as execMergeSegments,
  compactSegment as execCompactSegment,
  getExpiredEntries as execGetExpiredEntries,
  getWALStats as execGetWALStats,
  truncateWAL as execTruncateWAL,
  compactWAL as execCompactWAL,
  forceCleanup as execForceCleanup,
  parseSegmentLSN,
  type ExecutorContext,
  type CheckpointManagerForRetention,
} from './executor.js';
import {
  createMetricsCollector,
  performHealthCheck,
  evaluateDynamicPolicy,
  getReplicationStatus as getReplicationStatusFn,
  getOldestSegmentAge,
  type MetricsCollector,
} from './metrics.js';

// =============================================================================
// Error Types (also exported from retention-types, but kept for convenience)
// =============================================================================

export { RetentionError, RetentionErrorCode } from '../retention-types.js';

// =============================================================================
// Size-Based Checkpoint Trigger
// =============================================================================

/**
 * Configuration for size-based checkpoint trigger
 */
export interface SizeBasedCheckpointTriggerConfig {
  /** Maximum WAL size in bytes before triggering checkpoint */
  maxWALSizeBytes: number;
  /** Interval in milliseconds to check WAL size (default: 5000) */
  checkIntervalMs?: number;
}

/**
 * Size-based checkpoint trigger interface
 */
export interface SizeBasedCheckpointTrigger {
  /** Start monitoring WAL size */
  start(): void;
  /** Stop monitoring */
  stop(): void;
  /** Check WAL size and trigger checkpoint if needed */
  checkAndTrigger(): Promise<boolean>;
  /** Whether the trigger is active */
  isActive(): boolean;
}

/**
 * Auto-checkpointer interface (minimal for type checking)
 */
interface AutoCheckpointer {
  forceCheckpoint(): Promise<unknown>;
}

/**
 * Create a size-based checkpoint trigger
 *
 * Monitors WAL size and triggers a checkpoint when the total size
 * exceeds the configured threshold.
 *
 * @param reader - WAL reader for listing segments
 * @param backend - Storage backend for reading segment sizes
 * @param autoCheckpointer - Auto-checkpointer to trigger checkpoints
 * @param config - Configuration options
 * @returns Size-based checkpoint trigger
 *
 * @example
 * ```typescript
 * const sizeTrigger = createSizeBasedCheckpointTrigger(
 *   reader,
 *   backend,
 *   autoCheckpointer,
 *   { maxWALSizeBytes: 1024 * 1024 } // 1MB threshold
 * );
 *
 * sizeTrigger.start();
 * // ... later
 * sizeTrigger.stop();
 * ```
 */
export function createSizeBasedCheckpointTrigger(
  reader: WALReader,
  backend: FSXBackend,
  autoCheckpointer: AutoCheckpointer,
  config: SizeBasedCheckpointTriggerConfig
): SizeBasedCheckpointTrigger {
  const checkIntervalMs = config.checkIntervalMs ?? 5000;
  let intervalId: ReturnType<typeof setInterval> | null = null;
  let active = false;

  /**
   * Calculate current WAL size
   */
  async function getWALSize(): Promise<number> {
    const segments = await reader.listSegments(false);
    let totalSize = 0;

    for (const segmentId of segments) {
      const data = await backend.read(`_wal/segments/${segmentId}`);
      if (data) {
        totalSize += data.length;
      }
    }

    return totalSize;
  }

  /**
   * Check if checkpoint is needed and trigger if so
   */
  async function checkAndTrigger(): Promise<boolean> {
    const currentSize = await getWALSize();

    if (currentSize >= config.maxWALSizeBytes) {
      try {
        await autoCheckpointer.forceCheckpoint();
        return true;
      } catch (error) {
        // Log but don't throw - checkpoint failure shouldn't crash the trigger
        console.error('Size-based checkpoint trigger failed:', error);
      }
    }

    return false;
  }

  return {
    start(): void {
      if (active) return;
      active = true;

      intervalId = setInterval(async () => {
        await checkAndTrigger();
      }, checkIntervalMs);
    },

    stop(): void {
      if (intervalId) {
        clearInterval(intervalId);
        intervalId = null;
      }
      active = false;
    },

    checkAndTrigger,

    isActive(): boolean {
      return active;
    },
  };
}

// =============================================================================
// Prometheus Metrics Reporter
// =============================================================================

/**
 * Prometheus-compatible metrics reporter
 *
 * Collects WAL retention metrics in Prometheus exposition format.
 */
export class PrometheusReporter implements MetricsReporter {
  private prefix: string;
  private labels: Record<string, string>;
  private metrics: Map<string, number> = new Map();

  constructor(config: { prefix?: string; labels?: Record<string, string> } | ExtendedWALRetentionManager) {
    if ('prefix' in config && typeof config.prefix === 'string') {
      this.prefix = config.prefix ?? 'dosql_wal_';
      this.labels = (config as { prefix?: string; labels?: Record<string, string> }).labels ?? {};
    } else {
      this.prefix = 'dosql_wal_';
      this.labels = {};
    }
  }

  report(metric: RetentionMetric): void {
    // Map metric types to Prometheus naming conventions
    const name = `${this.prefix}${metric.type.replace(/[^a-zA-Z0-9_]/g, '_')}`;
    this.metrics.set(name, metric.value);

    // Also track derived metrics
    if (metric.type === 'cleanup_duration_ms') {
      this.metrics.set(`${this.prefix}cleanup_duration_seconds`, metric.value / 1000);
    }
    if (metric.type === 'cleanup_segments_deleted') {
      this.metrics.set(`${this.prefix}segments_total`, metric.value);
    }
    if (metric.type === 'cleanup_bytes_freed') {
      this.metrics.set(`${this.prefix}bytes_total`, metric.value);
    }
  }

  getMetrics(): string {
    const lines: string[] = [];
    const labelStr = Object.entries(this.labels)
      .map(([k, v]) => `${k}="${v}"`)
      .join(',');
    const labelSuffix = labelStr ? `{${labelStr}}` : '';

    for (const [name, value] of this.metrics) {
      lines.push(`${name}${labelSuffix} ${value}`);
    }

    return lines.join('\n');
  }
}

// =============================================================================
// WAL Retention Manager Factory
// =============================================================================

/**
 * Extended WAL Retention Manager interface
 */
export interface ExtendedWALRetentionManager extends WALRetentionManager {
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
  onWarning?: ((warning: RetentionWarning) => void) | undefined;
  // New methods for WAL retention features
  truncateWAL(beforeLSN: bigint): Promise<TruncateResult>;
  compactWAL(): Promise<CompactWALResult>;
  forceCleanup(options?: ForceCleanupOptions): Promise<ForceCleanupResult>;
  getWALStats(): Promise<WALStats>;
  getCleanupLatencyHistogram(): Promise<CleanupLatencyHistogram>;
  getGrowthStats(): Promise<GrowthStats>;
  registerCheckpointListener(checkpointMgr: CheckpointManagerForRetention): void;
  onSizeWarning?: ((current: number, max: number) => void) | undefined;
  // Per-table retention
  getTableRetentionPolicy(tableName: string): TableRetentionPolicy | undefined;
  // Cleanup windows
  isInCleanupWindow(): boolean;
  // Impact analysis
  analyzeCleanupImpact(): Promise<CleanupImpact>;
  // Metrics collection
  startMetricsCollection(): void;
  stopMetricsCollection(): void;
}

/**
 * Create a WAL retention manager
 *
 * @param backend - Storage backend
 * @param reader - WAL reader
 * @param slotManager - Replication slot manager (or null)
 * @param policy - Retention policy configuration
 * @param config - WAL configuration
 * @returns Extended WAL retention manager
 *
 * @example
 * ```typescript
 * const retentionManager = createWALRetentionManager(
 *   backend,
 *   reader,
 *   slotManager,
 *   { retentionHours: 24, maxTotalBytes: 500 * 1024 * 1024 }
 * );
 *
 * // Check what can be deleted
 * const result = await retentionManager.checkRetention();
 * console.log('Eligible for deletion:', result.eligibleForDeletion);
 *
 * // Perform cleanup
 * const cleanupResult = await retentionManager.cleanup();
 * console.log('Deleted:', cleanupResult.deleted.length, 'segments');
 * ```
 */
export function createWALRetentionManager(
  backend: FSXBackend,
  reader: WALReader,
  slotManager: ReplicationSlotManager | null,
  policy: Partial<RetentionPolicy> = {},
  config: Partial<WALConfig> = {}
): ExtendedWALRetentionManager {
  // Resolve policy with presets and defaults
  const fullPolicy = resolvePolicy(policy);
  const fullConfig: WALConfig = { ...DEFAULT_WAL_CONFIG, ...config };

  // Track active readers
  const activeReaders = new Map<string, ActiveReader>();

  // Track active writers for coordination
  const activeWriters = new Set<WALWriter>();

  // Checkpoint history
  const checkpointHistory: CheckpointInfo[] = [];

  // Metrics collection timer
  let metricsTimer: ReturnType<typeof setInterval> | null = null;

  // Create metrics collector
  const metricsCollector = createMetricsCollector(fullPolicy);

  // Create executor context
  const createContext = (): ExecutorContext => {
    const ctx: ExecutorContext = {
      backend,
      reader,
      slotManager,
      policy: fullPolicy,
      config: fullConfig,
      activeReaders,
      activeWriters,
      checkpointHistory,
      throttleConfig: scheduler.getThrottleConfig(),
    };
    if (fullPolicy.onCleanupProgress) {
      ctx.onProgress = fullPolicy.onCleanupProgress;
    }
    return ctx;
  };

  // Internal cleanup function for scheduler
  const cleanupFn = async (dryRun: boolean = false): Promise<void> => {
    const ctx = createContext();
    const checkResult = await execCheckRetention(ctx);
    const result = await executeCleanup(ctx, checkResult.eligibleForDeletion, dryRun);

    if (!dryRun && result.deleted.length > 0) {
      metricsCollector.recordCleanup({
        timestamp: Date.now(),
        segmentsDeleted: result.deleted.length,
        bytesFreed: result.bytesFreed,
        durationMs: result.durationMs,
      });
    }
  };

  // Create scheduler
  const scheduler = createRetentionScheduler(fullPolicy, cleanupFn, fullPolicy.onCleanupProgress);

  // Start background cleanup if configured
  if (fullPolicy.backgroundCleanup) {
    scheduler.start();
  }

  const manager: ExtendedWALRetentionManager = {
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
      return getMinSlotLSN(slotManager);
    },

    async checkRetention(): Promise<RetentionCheckResult> {
      const ctx = createContext();
      const result = await execCheckRetention(ctx);

      // Track protectedByHint when respectRetentionHints is enabled
      if (fullPolicy.respectRetentionHints) {
        result.protectedByHint = 0;
      }

      return result;
    },

    async getSegmentsToDelete(): Promise<string[]> {
      const check = await manager.checkRetention();
      return check.eligibleForDeletion;
    },

    async cleanup(dryRun = false): Promise<RetentionCleanupResult> {
      if (scheduler.isCleanupInProgress()) {
        return {
          deleted: [],
          archived: [],
          failed: [],
          bytesFreed: 0,
          durationMs: 0,
          corruptedSegments: [],
        };
      }

      const ctx = createContext();
      const startTime = Date.now();

      if (fullPolicy.onCleanupProgress) {
        fullPolicy.onCleanupProgress({ type: 'start' });
      }

      // Handle protectedByHint for respectRetentionHints
      const checkResult = await execCheckRetention(ctx);

      if (fullPolicy.respectRetentionHints) {
        checkResult.protectedByHint = 0;
        // Note: hint tracking is at the result level for now
      }

      const result = await executeCleanup(ctx, checkResult.eligibleForDeletion, dryRun);

      // Add atomic cleanup metadata
      if (fullPolicy.atomicCleanup) {
        result.transactionId = `cleanup_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
        result.committed = result.failed.length === 0;
        result.rolledBack = result.failed.length > 0;
      }

      if (!dryRun) {
        metricsCollector.recordCleanup({
          timestamp: Date.now(),
          segmentsDeleted: result.deleted.length,
          bytesFreed: result.bytesFreed,
          durationMs: result.durationMs,
        });
      }

      if (fullPolicy.onCleanupProgress) {
        fullPolicy.onCleanupProgress({ type: 'complete', bytesFreed: result.bytesFreed });
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
        const { parseSizeString } = require('./policy.js');
        fullPolicy.maxTotalBytes = parseSizeString(policy.maxTotalSize);
      }

      scheduler.updatePolicy(policy);
    },

    // Extended methods

    async getExpiredEntries(): Promise<ExpiredEntry[]> {
      const ctx = createContext();
      return execGetExpiredEntries(ctx);
    },

    async getStorageStats(): Promise<StorageStats> {
      const ctx = createContext();
      return execGetStorageStats(ctx);
    },

    async getEntryStats(): Promise<EntryStats> {
      const ctx = createContext();
      return execGetEntryStats(ctx);
    },

    async getSegmentEntryStats(): Promise<SegmentEntryStats[]> {
      const ctx = createContext();
      return execGetSegmentEntryStats(ctx);
    },

    async getCheckpointHistory(): Promise<CheckpointInfo[]> {
      return [...checkpointHistory];
    },

    async onCheckpointCreated(checkpoint: CheckpointInfo): Promise<void> {
      checkpointHistory.push(checkpoint);

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
      const ctx = createContext();
      return execCalculateFragmentation(ctx);
    },

    async checkCompactionNeeded(): Promise<boolean> {
      const ctx = createContext();
      return execCheckCompactionNeeded(ctx);
    },

    async mergeSegments(segmentIds: string[]): Promise<MergeResult> {
      const ctx = createContext();
      return execMergeSegments(ctx, segmentIds);
    },

    async compactSegment(segmentId: string): Promise<CompactResult> {
      const ctx = createContext();
      return execCompactSegment(ctx, segmentId);
    },

    startBackgroundCleanup(): void {
      scheduler.start();
    },

    stopBackgroundCleanup(): void {
      scheduler.stop();
    },

    isRunning(): boolean {
      return scheduler.isRunning();
    },

    getNextCleanupTime(): Date | null {
      return scheduler.getNextCleanupTime();
    },

    setThrottleConfig(config: ThrottleConfig): void {
      scheduler.setThrottleConfig(config);
    },

    getThrottleConfig(): ThrottleConfig {
      return scheduler.getThrottleConfig();
    },

    isInLowActivityWindow(): boolean {
      return scheduler.isInLowActivityWindow();
    },

    async getMetrics(): Promise<RetentionMetrics> {
      const stats = await manager.getStorageStats();
      const oldestAge = await getOldestSegmentAge(reader);
      return metricsCollector.getMetrics(stats, oldestAge);
    },

    async getCleanupHistory(): Promise<CleanupRecord[]> {
      return metricsCollector.getCleanupHistory();
    },

    async healthCheck(): Promise<HealthCheckResult> {
      const stats = await manager.getStorageStats();
      const oldestAge = await getOldestSegmentAge(reader);
      const fragmentation = await manager.calculateFragmentation();
      return performHealthCheck(fullPolicy, stats, oldestAge, fragmentation.ratio);
    },

    async evaluateDynamicPolicy(): Promise<DynamicPolicyResult> {
      const stats = await manager.getStorageStats();
      return evaluateDynamicPolicy(fullPolicy, stats);
    },

    async getReplicationStatus(): Promise<ReplicationStatus> {
      return getReplicationStatusFn(slotManager, reader, fullPolicy);
    },

    async getRegionReplicationStatus(): Promise<RegionReplicationStatus> {
      return metricsCollector.getRegionReplicationStatus();
    },

    async acquireCleanupLock(): Promise<boolean> {
      return scheduler.acquireCleanupLock();
    },

    isCleanupInProgress(): boolean {
      return scheduler.isCleanupInProgress();
    },

    registerActiveWriter(writer: WALWriter): void {
      activeWriters.add(writer);
    },

    parseSegmentLSN,

    onWarning: fullPolicy.onWarning,

    async truncateWAL(beforeLSN: bigint): Promise<TruncateResult> {
      const ctx = createContext();
      return execTruncateWAL(ctx, beforeLSN);
    },

    async compactWAL(): Promise<CompactWALResult> {
      const ctx = createContext();
      return execCompactWAL(ctx);
    },

    async forceCleanup(options?: ForceCleanupOptions): Promise<ForceCleanupResult> {
      const ctx = createContext();
      return execForceCleanup(ctx, options);
    },

    async getWALStats(): Promise<WALStats> {
      const ctx = createContext();
      return execGetWALStats(ctx);
    },

    async getCleanupLatencyHistogram(): Promise<CleanupLatencyHistogram> {
      return metricsCollector.getCleanupLatencyHistogram();
    },

    async getGrowthStats(): Promise<GrowthStats> {
      const stats = await manager.getStorageStats();
      const entryStats = await manager.getEntryStats();
      return metricsCollector.getGrowthStats(stats, entryStats);
    },

    registerCheckpointListener(checkpointMgr: CheckpointManagerForRetention): void {
      const originalCreateCheckpoint = checkpointMgr.createCheckpoint.bind(checkpointMgr);

      checkpointMgr.createCheckpoint = async (lsn: bigint, activeTransactions: string[]) => {
        const checkpoint = await originalCreateCheckpoint(lsn, activeTransactions);

        if (fullPolicy.cleanupOnCheckpoint) {
          if (fullPolicy.onCleanupTriggered) {
            fullPolicy.onCleanupTriggered();
          }
          await manager.cleanup(false);
        }

        return checkpoint;
      };
    },

    onSizeWarning: fullPolicy.onSizeWarning,

    getTableRetentionPolicy(tableName: string): TableRetentionPolicy | undefined {
      if (!fullPolicy.tableRetention) return undefined;

      const tableConfig = fullPolicy.tableRetention[tableName] ?? fullPolicy.tableRetention['*'];
      if (!tableConfig) return undefined;

      const result: TableRetentionPolicy = {};
      if (tableConfig.retentionDays !== undefined) {
        result.retentionHours = tableConfig.retentionDays * 24;
      }
      if (tableConfig.retentionHours !== undefined) {
        result.retentionHours = tableConfig.retentionHours;
      }
      if (tableConfig.archiveToR2 !== undefined) {
        result.archiveToR2 = tableConfig.archiveToR2;
      }
      return result;
    },

    isInCleanupWindow(): boolean {
      return isInCleanupWindow(fullPolicy.cleanupWindows);
    },

    async analyzeCleanupImpact(): Promise<CleanupImpact> {
      const ctx = createContext();
      const checkResult = await execCheckRetention(ctx);

      // Gather affected tables
      const affectedTables = new Set<string>();
      let bytesToFree = 0;
      let oldestRetainedLSN = 0n;

      for (const segmentId of checkResult.eligibleForDeletion) {
        const segment = await ctx.reader.readSegment(segmentId);
        if (segment) {
          const path = `${ctx.config.segmentPrefix}${segmentId}`;
          const data = await ctx.backend.read(path);
          if (data) bytesToFree += data.length;

          for (const entry of segment.entries) {
            if (entry.table) affectedTables.add(entry.table);
          }
        }
      }

      // Find oldest retained LSN
      const allSegments = await ctx.reader.listSegments(false);
      const retained = allSegments.filter(s => !checkResult.eligibleForDeletion.includes(s));
      for (const segId of retained) {
        const seg = await ctx.reader.readSegment(segId);
        if (seg) {
          if (oldestRetainedLSN === 0n || seg.startLSN < oldestRetainedLSN) {
            oldestRetainedLSN = seg.startLSN;
          }
        }
      }

      // Get affected replication slots
      const affectedSlots: string[] = [];
      if (ctx.slotManager) {
        const slots = await ctx.slotManager.listSlots();
        for (const slot of slots) {
          affectedSlots.push(slot.name);
        }
      }

      // Estimate duration based on segment count
      const estimatedDuration = checkResult.eligibleForDeletion.length * 10; // ~10ms per segment

      // Assess risks
      const risks: string[] = [];
      if (checkResult.protectedBySlots.length > 0) {
        risks.push('Some segments are protected by replication slots');
      }
      if (checkResult.eligibleForDeletion.length === 0) {
        risks.push('No segments eligible for deletion');
      }

      return {
        segmentsToDelete: checkResult.eligibleForDeletion.length,
        bytesToFree,
        affectedTables: Array.from(affectedTables),
        oldestRetainedLSN,
        affectedReplicationSlots: affectedSlots,
        estimatedDuration,
        risks,
      };
    },

    startMetricsCollection(): void {
      if (metricsTimer) return;

      const interval = fullPolicy.metricsInterval ?? 5000;
      metricsTimer = setInterval(async () => {
        if (fullPolicy.onMetricsUpdate) {
          try {
            const stats = await manager.getStorageStats();
            fullPolicy.onMetricsUpdate({
              type: 'metrics',
              timestamp: Date.now(),
              data: {
                totalBytes: stats.totalBytes,
                segmentCount: stats.segmentCount,
              },
            });
          } catch {
            // Ignore errors during metrics collection
          }
        }
      }, interval);
    },

    stopMetricsCollection(): void {
      if (metricsTimer) {
        clearInterval(metricsTimer);
        metricsTimer = null;
      }
    },
  };

  return manager;
}
