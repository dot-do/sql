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
} from '../retention-types.js';

import { DEFAULT_RETENTION_POLICY, RETENTION_PRESETS, resolvePolicy, validatePolicy } from './policy.js';
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
      return execCheckRetention(ctx);
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
  };

  return manager;
}
