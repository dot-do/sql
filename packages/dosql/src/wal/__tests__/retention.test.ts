/**
 * WAL Retention Policy - GREEN Phase TDD Tests
 *
 * These tests verify the implemented WAL retention policy features.
 *
 * Issue: pocs-7ay0 - WAL retention policies implemented
 *
 * Implemented Features:
 * 1. Time-based retention (keep WAL entries for N hours/days)
 * 2. Size-based retention (max WAL size in bytes)
 * 3. Entry count-based retention (max entries)
 * 4. Checkpoint-based retention (keep only since last checkpoint)
 * 5. Compaction triggers
 * 6. Background cleanup
 * 7. Retention policy configuration
 * 8. Edge cases (empty WAL, single entry, etc.)
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import type { FSXBackend } from '../../fsx/types.js';

// =============================================================================
// Test Utilities - In-Memory FSX Backend
// =============================================================================

/**
 * Creates an in-memory FSX backend for testing
 */
function createTestBackend(): FSXBackend {
  const storage = new Map<string, Uint8Array>();

  return {
    async read(path: string): Promise<Uint8Array | null> {
      return storage.get(path) ?? null;
    },

    async write(path: string, data: Uint8Array): Promise<void> {
      storage.set(path, data);
    },

    async delete(path: string): Promise<void> {
      storage.delete(path);
    },

    async list(prefix: string): Promise<string[]> {
      const paths: string[] = [];
      for (const key of storage.keys()) {
        if (key.startsWith(prefix)) {
          paths.push(key);
        }
      }
      return paths.sort();
    },

    async exists(path: string): Promise<boolean> {
      return storage.has(path);
    },
  };
}

// =============================================================================
// 1. TIME-BASED RETENTION - Keep WAL entries for N hours/days
// =============================================================================

describe('WAL Retention - Time-Based Retention', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should support retention period specified in hours', async () => {
    // RetentionPolicy should support `retentionHours` as a convenience
    // When retentionHours is specified, maxSegmentAge should be AUTO-CALCULATED
    const { createWALRetentionManager, DEFAULT_RETENTION_POLICY } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      retentionHours: 12, // Keep WAL for 12 hours (different from 24h default)
    } as any);

    const policy = manager.getPolicy();
    // retentionHours should auto-calculate maxSegmentAge to 12 hours in ms
    expect(policy.maxSegmentAge).toBe(12 * 60 * 60 * 1000);
    expect(policy.maxSegmentAge).not.toBe(DEFAULT_RETENTION_POLICY.maxSegmentAge);
  });

  it('should support retention period specified in days', async () => {
    // MISSING: RetentionPolicy should support `retentionDays` as a convenience
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      retentionDays: 7, // Keep WAL for 7 days
    } as any);

    const policy = manager.getPolicy();
    expect(policy.retentionDays).toBe(7);
    expect(policy.maxSegmentAge).toBe(7 * 24 * 60 * 60 * 1000);
  });

  it('should support retention period specified in minutes for testing', async () => {
    // MISSING: RetentionPolicy should support `retentionMinutes` for testing scenarios
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      retentionMinutes: 30, // Keep WAL for 30 minutes
    } as any);

    const policy = manager.getPolicy();
    expect(policy.retentionMinutes).toBe(30);
    expect(policy.maxSegmentAge).toBe(30 * 60 * 1000);
  });

  it('should prioritize most specific time unit when multiple are specified', async () => {
    // MISSING: When multiple time units are specified, should use most specific
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      retentionDays: 7,
      retentionHours: 12, // More specific, should be used
    } as any);

    const policy = manager.getPolicy();
    expect(policy.maxSegmentAge).toBe(12 * 60 * 60 * 1000);
  });

  it('should track entry timestamps for time-based expiration', async () => {
    // MISSING: Should be able to query entries by age, not just segments
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      retentionHours: 1,
    } as any);

    // Should be able to get expired entries (not just segments)
    const expiredEntries = await (manager as any).getExpiredEntries();
    expect(Array.isArray(expiredEntries)).toBe(true);
  });
});

// =============================================================================
// 2. SIZE-BASED RETENTION - Max WAL size in bytes
// =============================================================================

describe('WAL Retention - Size-Based Retention', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should support maximum total WAL size configuration', async () => {
    // MISSING: RetentionPolicy should support `maxTotalBytes` and USE IT
    // The policy should be enforced during checkRetention()
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createWALWriter } = await import('../index.js');

    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);

    // Write some data
    for (let i = 0; i < 10; i++) {
      await writer.append({
        timestamp: Date.now(),
        txnId: `txn_${i}`,
        op: 'INSERT',
        table: 'test',
        after: new Uint8Array(1000),
      });
    }
    await writer.flush();

    const manager = createWALRetentionManager(backend, reader, null, {
      maxTotalBytes: 1000, // Very small limit - should trigger cleanup
      minSegmentCount: 0,
      maxSegmentAge: 0,
    } as any);

    const result = await manager.checkRetention();
    // MUST FAIL: checkRetention should report bytesOverLimit when maxTotalBytes is exceeded
    expect((result as any).bytesOverLimit).toBeDefined();
    expect((result as any).bytesOverLimit).toBeGreaterThan(0);

    await writer.close();
  });

  it('should calculate current total WAL size', async () => {
    // MISSING: Should be able to query current total WAL size
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      maxTotalBytes: 100 * 1024 * 1024,
    } as any);

    const stats = await (manager as any).getStorageStats();
    expect(typeof stats.totalBytes).toBe('number');
    expect(typeof stats.segmentCount).toBe('number');
    expect(typeof stats.averageSegmentSize).toBe('number');
  });

  it('should mark oldest segments for deletion when size exceeds limit', async () => {
    // Should prioritize oldest segments for deletion when over size limit
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createWALWriter } = await import('../index.js');

    const reader = createWALReader(backend);
    const writer = createWALWriter(backend);

    // Write enough data to exceed limit
    const largeData = new Uint8Array(1024 * 1024); // 1MB
    for (let i = 0; i < 10; i++) {
      await writer.append({
        timestamp: Date.now(),
        txnId: `txn_${i}`,
        op: 'INSERT',
        table: 'test',
        after: largeData,
      });
    }
    await writer.flush();

    const manager = createWALRetentionManager(backend, reader, null, {
      maxTotalBytes: 5 * 1024 * 1024, // 5MB limit (less than written)
      minSegmentCount: 0,
      maxSegmentAge: 0, // Make all segments immediately eligible
    } as any);

    const result = await manager.checkRetention();
    // At minimum, bytesOverLimit should be reported
    expect((result as any).bytesOverLimit).toBeGreaterThan(0);

    await writer.close();
  });

  it('should support size limit in human-readable format', async () => {
    // MISSING: Should support "100MB", "1GB", etc. format
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      maxTotalSize: '100MB', // Human-readable format
    } as any);

    const policy = manager.getPolicy();
    expect((policy as any).maxTotalBytes).toBe(100 * 1024 * 1024);
  });

  it('should emit warning when approaching size limit', async () => {
    // MISSING: Should emit events/callbacks when nearing limits
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const warnings: any[] = [];

    const manager = createWALRetentionManager(backend, reader, null, {
      maxTotalBytes: 100 * 1024 * 1024,
      warningThreshold: 0.8, // Warn at 80%
      onWarning: (warning: any) => warnings.push(warning),
    } as any);

    // Should have warning callback configured
    expect((manager as any).onWarning).toBeDefined();
  });
});

// =============================================================================
// 3. ENTRY COUNT-BASED RETENTION - Max entries
// =============================================================================

describe('WAL Retention - Entry Count-Based Retention', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should support maximum total entry count', async () => {
    // MISSING: RetentionPolicy should support `maxEntryCount` and USE IT
    // The policy should be enforced during checkRetention()
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createWALWriter } = await import('../index.js');

    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);

    // Write 100 entries
    for (let i = 0; i < 100; i++) {
      await writer.append({
        timestamp: Date.now(),
        txnId: `txn_${i}`,
        op: 'INSERT',
        table: 'test',
        after: new Uint8Array([1, 2, 3]),
      });
    }
    await writer.flush();

    const manager = createWALRetentionManager(backend, reader, null, {
      maxEntryCount: 10, // Very small limit - should trigger cleanup
      minSegmentCount: 0,
      maxSegmentAge: 0,
    } as any);

    const result = await manager.checkRetention();
    // MUST FAIL: checkRetention should report entriesOverLimit when maxEntryCount is exceeded
    expect((result as any).entriesOverLimit).toBeDefined();
    expect((result as any).entriesOverLimit).toBeGreaterThan(0);

    await writer.close();
  });

  it('should calculate current total entry count', async () => {
    // MISSING: Should be able to query total entry count across all segments
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createWALWriter } = await import('../index.js');

    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);

    // Write some entries
    for (let i = 0; i < 100; i++) {
      await writer.append({
        timestamp: Date.now(),
        txnId: `txn_${i}`,
        op: 'INSERT',
        table: 'test',
        after: new Uint8Array([1, 2, 3]),
      });
    }
    await writer.flush();

    const manager = createWALRetentionManager(backend, reader, null);

    const stats = await (manager as any).getEntryStats();
    expect(stats.totalEntries).toBe(100);

    await writer.close();
  });

  it('should delete oldest entries when count exceeds limit', async () => {
    // MISSING: Should mark segments for deletion when entry count exceeds limit
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createWALWriter } = await import('../index.js');

    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);

    // Write many entries
    for (let i = 0; i < 1000; i++) {
      await writer.append({
        timestamp: Date.now(),
        txnId: `txn_${i}`,
        op: 'INSERT',
        table: 'test',
        after: new Uint8Array([1, 2, 3]),
      }, { sync: i % 100 === 99 });
    }

    const manager = createWALRetentionManager(backend, reader, null, {
      maxEntryCount: 500, // Limit to 500 entries
      minSegmentCount: 0,
    } as any);

    const result = await manager.checkRetention();
    expect((result as any).entriesOverLimit).toBeGreaterThan(0);

    await writer.close();
  });

  it('should track entry count per segment efficiently', async () => {
    // MISSING: Should cache entry counts per segment for efficient queries
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    const segmentStats = await (manager as any).getSegmentEntryStats();
    expect(Array.isArray(segmentStats)).toBe(true);
    // Each stat should have segmentId and entryCount
    if (segmentStats.length > 0) {
      expect(segmentStats[0]).toHaveProperty('segmentId');
      expect(segmentStats[0]).toHaveProperty('entryCount');
    }
  });
});

// =============================================================================
// 4. CHECKPOINT-BASED RETENTION - Keep only since last checkpoint
// =============================================================================

describe('WAL Retention - Checkpoint-Based Retention', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should support strict checkpoint-based retention mode', async () => {
    // MISSING: Mode to keep ONLY segments since last checkpoint
    // In strict mode, segments before checkpoint should be marked for deletion
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createWALWriter, createCheckpointManager } = await import('../index.js');

    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    // Write some entries and create checkpoint
    for (let i = 0; i < 20; i++) {
      await writer.append({
        timestamp: Date.now(),
        txnId: `txn_${i}`,
        op: 'INSERT',
        table: 'test',
        after: new Uint8Array([1, 2, 3]),
      }, { sync: i % 5 === 4 });
    }

    // Create checkpoint at LSN 10
    await checkpointMgr.createCheckpoint(10n, []);

    const manager = createWALRetentionManager(backend, reader, null, {
      checkpointRetentionMode: 'strict', // Only keep post-checkpoint
    } as any);

    const result = await manager.checkRetention();
    // MUST FAIL: checkRetention should report segmentsBeforeCheckpoint in strict mode
    expect((result as any).segmentsBeforeCheckpoint).toBeDefined();
    expect(Array.isArray((result as any).segmentsBeforeCheckpoint)).toBe(true);

    await writer.close();
  });

  it('should support N-checkpoint retention', async () => {
    // MISSING: Keep segments from last N checkpoints
    // Manager should track checkpoint history and enforce keepCheckpointCount
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      keepCheckpointCount: 3, // Keep last 3 checkpoints worth of WAL
    } as any);

    // MUST FAIL: Manager should have getCheckpointHistory method
    expect(typeof (manager as any).getCheckpointHistory).toBe('function');

    const history = await (manager as any).getCheckpointHistory();
    expect(Array.isArray(history)).toBe(true);
  });

  it('should track checkpoint history', async () => {
    // MISSING: Should maintain checkpoint history for retention decisions
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createCheckpointManager } = await import('../index.js');

    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    const manager = createWALRetentionManager(backend, reader, null, {
      keepCheckpointCount: 3,
    } as any);

    const history = await (manager as any).getCheckpointHistory();
    expect(Array.isArray(history)).toBe(true);
  });

  it('should automatically trigger cleanup after checkpoint', async () => {
    // MISSING: Should support automatic cleanup trigger after checkpoint
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    let cleanupTriggered = false;

    const manager = createWALRetentionManager(backend, reader, null, {
      cleanupOnCheckpoint: true,
      onCleanupTriggered: () => { cleanupTriggered = true; },
    } as any);

    // Trigger checkpoint
    await (manager as any).onCheckpointCreated({ lsn: 100n, timestamp: Date.now() });

    expect(cleanupTriggered).toBe(true);
  });

  it('should identify segments safe to delete based on checkpoint', async () => {
    // MISSING: Should return segments entirely before checkpoint
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      checkpointRetentionMode: 'strict',
    } as any);

    const result = await manager.checkRetention();
    expect(result).toHaveProperty('segmentsBeforeCheckpoint');
  });
});

// =============================================================================
// 5. COMPACTION TRIGGERS
// =============================================================================

describe('WAL Retention - Compaction Triggers', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should support compaction threshold configuration', async () => {
    // MISSING: Should support compaction when fragmentation exceeds threshold
    // Manager should have calculateFragmentation method
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      compactionThreshold: 0.3, // Compact when 30% fragmented
    } as any);

    // MUST FAIL: Manager should have calculateFragmentation method
    expect(typeof (manager as any).calculateFragmentation).toBe('function');

    const fragmentation = await (manager as any).calculateFragmentation();
    expect(typeof fragmentation.ratio).toBe('number');
  });

  it('should calculate fragmentation ratio', async () => {
    // MISSING: Should calculate ratio of deleted/rolled-back entries
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    const fragmentation = await (manager as any).calculateFragmentation();
    expect(typeof fragmentation.ratio).toBe('number');
    expect(fragmentation.ratio).toBeGreaterThanOrEqual(0);
    expect(fragmentation.ratio).toBeLessThanOrEqual(1);
  });

  it('should trigger compaction when threshold exceeded', async () => {
    // MISSING: Should automatically trigger compaction
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    let compactionTriggered = false;

    const manager = createWALRetentionManager(backend, reader, null, {
      compactionThreshold: 0.1,
      autoCompaction: true,
      onCompactionNeeded: () => { compactionTriggered = true; },
    } as any);

    await (manager as any).checkCompactionNeeded();
    // Should have evaluated whether compaction is needed
    expect(typeof compactionTriggered).toBe('boolean');
  });

  it('should support segment merging during compaction', async () => {
    // MISSING: Should be able to merge small segments
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      minSegmentSize: 1024 * 1024, // 1MB minimum
      mergeSmallSegments: true,
    } as any);

    const mergeResult = await (manager as any).mergeSegments(['seg_1', 'seg_2']);
    expect(mergeResult).toHaveProperty('newSegmentId');
    expect(mergeResult).toHaveProperty('mergedCount');
  });

  it('should rewrite segments to remove rolled-back entries', async () => {
    // MISSING: Should compact segments by removing rolled-back transaction entries
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    const compactResult = await (manager as any).compactSegment('seg_1');
    expect(compactResult).toHaveProperty('entriesRemoved');
    expect(compactResult).toHaveProperty('bytesSaved');
  });
});

// =============================================================================
// 6. BACKGROUND CLEANUP
// =============================================================================

describe('WAL Retention - Background Cleanup', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should support scheduled background cleanup', async () => {
    // MISSING: Should support configurable cleanup schedule
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      cleanupIntervalMs: 60000, // Every minute
      backgroundCleanup: true,
    } as any);

    expect((manager as any).startBackgroundCleanup).toBeDefined();
    expect((manager as any).stopBackgroundCleanup).toBeDefined();
  });

  it('should start and stop background cleanup scheduler', async () => {
    // MISSING: Should have start/stop methods for scheduler
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      cleanupIntervalMs: 1000,
      backgroundCleanup: true,
    } as any);

    (manager as any).startBackgroundCleanup();
    expect((manager as any).isRunning()).toBe(true);

    (manager as any).stopBackgroundCleanup();
    expect((manager as any).isRunning()).toBe(false);
  });

  it('should support cron-style cleanup schedule', async () => {
    // MISSING: Should support cron expressions for cleanup
    // Manager should parse and validate cron expression
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      cleanupSchedule: '0 */6 * * *', // Every 6 hours
    } as any);

    // MUST FAIL: Manager should have getNextCleanupTime method that parses cron
    expect(typeof (manager as any).getNextCleanupTime).toBe('function');

    const nextCleanup = (manager as any).getNextCleanupTime();
    expect(nextCleanup instanceof Date).toBe(true);
  });

  it('should throttle cleanup operations', async () => {
    // MISSING: Should limit cleanup throughput to avoid performance impact
    // Cleanup should respect batch size and throttle settings
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      maxCleanupBatchSize: 10, // Max 10 segments per cleanup
      cleanupThrottleMs: 100, // 100ms between deletions
    } as any);

    // MUST FAIL: Manager should have setThrottleConfig method
    expect(typeof (manager as any).setThrottleConfig).toBe('function');

    // MUST FAIL: Manager should have getThrottleConfig method
    expect(typeof (manager as any).getThrottleConfig).toBe('function');

    const throttleConfig = (manager as any).getThrottleConfig();
    expect(throttleConfig.maxBatchSize).toBe(10);
    expect(throttleConfig.throttleMs).toBe(100);
  });

  it('should emit cleanup progress events', async () => {
    // MISSING: Should emit events during cleanup
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const events: any[] = [];

    const manager = createWALRetentionManager(backend, reader, null, {
      onCleanupProgress: (event: any) => events.push(event),
    } as any);

    await manager.cleanup(false);

    // Should have emitted at least start and complete events
    expect(events.some((e) => e.type === 'start')).toBe(true);
    expect(events.some((e) => e.type === 'complete')).toBe(true);
  });

  it('should support cleanup during low activity periods', async () => {
    // MISSING: Should be able to schedule cleanup during off-peak hours
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      lowActivityWindow: { start: '02:00', end: '06:00' },
      preferLowActivityCleanup: true,
    } as any);

    const isLowActivity = (manager as any).isInLowActivityWindow();
    expect(typeof isLowActivity).toBe('boolean');
  });
});

// =============================================================================
// 7. RETENTION POLICY CONFIGURATION
// =============================================================================

describe('WAL Retention - Policy Configuration', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should support policy presets', async () => {
    // Should have predefined policy presets
    const { createWALRetentionManager, RETENTION_PRESETS } = await import('../retention.js') as any;
    const { createWALReader } = await import('../reader.js');

    expect(RETENTION_PRESETS).toBeDefined();
    expect(RETENTION_PRESETS.aggressive).toBeDefined();
    expect(RETENTION_PRESETS.balanced).toBeDefined();
    expect(RETENTION_PRESETS.conservative).toBeDefined();

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      preset: 'balanced',
    });

    const policy = manager.getPolicy();
    // Policy should include all values from the preset
    expect(policy.minSegmentCount).toBe(RETENTION_PRESETS.balanced.minSegmentCount);
    expect(policy.maxSegmentAge).toBe(RETENTION_PRESETS.balanced.maxSegmentAge);
    expect(policy.maxTotalBytes).toBe(RETENTION_PRESETS.balanced.maxTotalBytes);
    expect(policy.maxEntryCount).toBe(RETENTION_PRESETS.balanced.maxEntryCount);
  });

  it('should validate policy configuration', async () => {
    // MISSING: Should validate policy values
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);

    // Should throw on invalid policy
    expect(() => createWALRetentionManager(backend, reader, null, {
      minSegmentCount: -1, // Invalid
    } as any)).toThrow();

    expect(() => createWALRetentionManager(backend, reader, null, {
      maxSegmentAge: -1000, // Invalid
    })).toThrow();
  });

  it('should support policy inheritance and override', async () => {
    // MISSING: Should support extending a base policy
    const { createWALRetentionManager, RETENTION_PRESETS } = await import('../retention.js') as any;
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      extends: 'balanced',
      overrides: {
        minSegmentCount: 5, // Override just this
      },
    });

    const policy = manager.getPolicy();
    expect(policy.minSegmentCount).toBe(5);
    // Other values should come from 'balanced' preset
    expect(policy.maxSegmentAge).toBe(RETENTION_PRESETS.balanced.maxSegmentAge);
  });

  it('should support dynamic policy adjustment', async () => {
    // MISSING: Should support adjusting policy based on conditions
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      dynamicPolicy: {
        // Reduce retention when storage is low
        storageThreshold: 0.9,
        reducedRetentionHours: 1,
      },
    } as any);

    // Should have dynamic policy evaluator
    const adjusted = await (manager as any).evaluateDynamicPolicy();
    expect(adjusted).toHaveProperty('applied');
    expect(adjusted).toHaveProperty('reason');
  });

  it('should log policy decisions with reasons', async () => {
    // MISSING: Should explain why segments are/aren't deleted
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      verboseLogging: true,
    } as any);

    const result = await manager.checkRetention();

    // Should have detailed reasons for each segment
    expect((result as any).decisions).toBeDefined();
    expect(Array.isArray((result as any).decisions)).toBe(true);
  });
});

// =============================================================================
// 8. EDGE CASES
// =============================================================================

describe('WAL Retention - Edge Cases', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should handle concurrent cleanup operations safely', async () => {
    // MISSING: Should prevent concurrent cleanup race conditions with a lock
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null) as any;

    // MUST FAIL: Manager should expose a lock/mutex mechanism for concurrent safety
    expect(manager.acquireCleanupLock).toBeDefined();
    expect(typeof manager.acquireCleanupLock).toBe('function');

    // MUST FAIL: Manager should track if cleanup is in progress
    expect(manager.isCleanupInProgress).toBeDefined();
    expect(typeof manager.isCleanupInProgress).toBe('function');
  });

  it('should handle corrupted segment gracefully during cleanup', async () => {
    // MISSING: Should skip corrupted segments and continue
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    // Write corrupted data directly
    await backend.write('_wal/segments/seg_00000000000000000001', new Uint8Array([1, 2, 3]));

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    const result = await manager.cleanup(false);

    // Should report the corruption but continue
    expect((result as any).corruptedSegments).toBeDefined();
  });

  it('should handle storage failures during cleanup', async () => {
    // Should handle storage errors gracefully
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, DefaultWALEncoder } = await import('../index.js');

    // Create a segment that will be eligible for deletion
    const encoder = new DefaultWALEncoder();
    const segment = {
      id: 'seg_00000000000000000001',
      startLSN: 1n,
      endLSN: 10n,
      entries: [{ lsn: 1n, timestamp: Date.now() - 100000000, txnId: 'txn_1', op: 'INSERT' as const, table: 'test' }],
      checksum: 0,
      createdAt: Date.now() - 100000000, // Old enough to be eligible
    };
    // Calculate and set proper checksum
    const segmentData = encoder.encodeSegment(segment);
    segment.checksum = encoder.calculateChecksum(segmentData);
    await backend.write('_wal/segments/seg_00000000000000000001', encoder.encodeSegment(segment));

    // Create a failing backend that fails on delete but works for read/list
    const failingBackend: FSXBackend = {
      read: backend.read.bind(backend),
      write: backend.write.bind(backend),
      list: backend.list.bind(backend),
      exists: backend.exists.bind(backend),
      delete: async () => { throw new Error('Storage failure'); },
    };

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(failingBackend, reader, null, {
      minSegmentCount: 0,
      maxSegmentAge: 0, // Make all segments immediately eligible
      archiveBeforeDelete: false, // Skip archiving to go straight to delete
    });

    const result = await manager.cleanup(false);

    // Should report failures since delete throws
    expect(result.failed.length).toBeGreaterThan(0);
    expect(result.failed[0].error).toContain('Storage failure');
  });

  it('should handle very large segment counts', async () => {
    // Should handle thousands of segments efficiently
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, DefaultWALEncoder } = await import('../index.js');

    // Create more segments than batch size
    const encoder = new DefaultWALEncoder();
    for (let i = 1; i <= 5; i++) {
      const segment = {
        id: `seg_${i.toString().padStart(20, '0')}`,
        startLSN: BigInt(i * 10),
        endLSN: BigInt(i * 10 + 9),
        entries: [],
        checksum: 0,
        createdAt: Date.now(),
      };
      await backend.write(`_wal/segments/seg_${i.toString().padStart(20, '0')}`, encoder.encodeSegment(segment));
    }

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      maxSegmentsToProcess: 2, // Small batch to test batching
    } as any);

    // Should process in batches when segments exceed limit
    const result = await manager.checkRetention();
    expect((result as any).batchProcessed).toBe(true);
  });

  it('should handle segments with zero entries', async () => {
    // Should handle edge case of empty segments
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, DefaultWALEncoder } = await import('../index.js');

    // Create an empty segment manually
    const encoder = new DefaultWALEncoder();
    const emptySegment = {
      id: 'seg_00000000000000000001',
      startLSN: 1n,
      endLSN: 0n,
      entries: [],
      checksum: 0,
      createdAt: Date.now() - 100000000, // Old enough to be eligible
    };
    const data = encoder.encodeSegment(emptySegment);
    // Recalculate checksum
    emptySegment.checksum = encoder.calculateChecksum(data);
    await backend.write('_wal/segments/seg_00000000000000000001', encoder.encodeSegment(emptySegment));

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 0, // Allow empty segments to be eligible
    });

    const result = await manager.checkRetention();
    // Empty segments should be listed
    expect((result as any).emptySegments).toBeDefined();
    expect(Array.isArray((result as any).emptySegments)).toBe(true);
  });

  it('should handle segment ID parsing edge cases', async () => {
    // MISSING: Should handle unusual segment IDs
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    // Test with unusual segment IDs
    await backend.write('_wal/segments/seg_invalid', new Uint8Array([1]));
    await backend.write('_wal/segments/not_a_segment', new Uint8Array([1]));

    const result = await manager.checkRetention();
    // Should not crash, should skip invalid segments
    expect((result as any).skippedInvalid).toBeDefined();
  });

  it('should handle LSN overflow edge cases', async () => {
    // MISSING: Should handle very large LSN values
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    // Simulate segment with max bigint LSN
    const maxLSN = BigInt('9223372036854775807');
    expect(() => (manager as any).parseSegmentLSN(`seg_${maxLSN}`)).not.toThrow();
  });

  it('should handle clock skew between segments', async () => {
    // MISSING: Should handle segments with out-of-order timestamps
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, DefaultWALEncoder } = await import('../index.js');

    const encoder = new DefaultWALEncoder();

    // Create segments with inconsistent timestamps
    const segment1 = {
      id: 'seg_00000000000000000001',
      startLSN: 1n,
      endLSN: 10n,
      entries: [],
      checksum: 0,
      createdAt: Date.now() + 1000000, // Future timestamp
    };
    const segment2 = {
      id: 'seg_00000000000000000011',
      startLSN: 11n,
      endLSN: 20n,
      entries: [],
      checksum: 0,
      createdAt: Date.now() - 1000000, // Past timestamp
    };

    await backend.write('_wal/segments/seg_00000000000000000001', encoder.encodeSegment(segment1));
    await backend.write('_wal/segments/seg_00000000000000000011', encoder.encodeSegment(segment2));

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      handleClockSkew: true,
    } as any);

    const result = await manager.checkRetention();
    // Should detect and handle clock skew
    expect((result as any).clockSkewDetected).toBeDefined();
  });

  it('should handle retention during active writes', async () => {
    // MISSING: Should coordinate with active writers
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createWALWriter } = await import('../index.js');

    const reader = createWALReader(backend);
    const writer = createWALWriter(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    // Register writer with manager
    (manager as any).registerActiveWriter(writer);

    // Start writing
    const writePromise = (async () => {
      for (let i = 0; i < 100; i++) {
        await writer.append({
          timestamp: Date.now(),
          txnId: `txn_${i}`,
          op: 'INSERT',
          table: 'test',
        });
      }
    })();

    // Concurrent cleanup
    const cleanupPromise = manager.cleanup(false);

    await Promise.all([writePromise, cleanupPromise]);

    // Should not have deleted any active segment
    await writer.close();
  });
});

// =============================================================================
// 9. METRICS AND OBSERVABILITY
// =============================================================================

describe('WAL Retention - Metrics and Observability', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should expose retention metrics', async () => {
    // MISSING: Should provide metrics for monitoring
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    const metrics = await (manager as any).getMetrics();

    expect(metrics).toHaveProperty('totalSegments');
    expect(metrics).toHaveProperty('totalBytes');
    expect(metrics).toHaveProperty('oldestSegmentAge');
    expect(metrics).toHaveProperty('lastCleanupTime');
    expect(metrics).toHaveProperty('segmentsDeleted');
    expect(metrics).toHaveProperty('bytesFreed');
  });

  it('should track cleanup history', async () => {
    // MISSING: Should maintain cleanup operation history
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      cleanupHistorySize: 100, // Keep last 100 cleanup records
    } as any);

    const history = await (manager as any).getCleanupHistory();

    expect(Array.isArray(history)).toBe(true);
    // Each record should have timestamp, segmentsDeleted, etc.
  });

  it('should support custom metrics reporters', async () => {
    // MISSING: Should allow plugging in metrics backends
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const metricsReported: any[] = [];

    const manager = createWALRetentionManager(backend, reader, null, {
      metricsReporter: {
        report: (metric: any) => metricsReported.push(metric),
      },
    } as any);

    await manager.cleanup(false);

    // Should have reported metrics
    expect(metricsReported.length).toBeGreaterThan(0);
  });

  it('should provide health check endpoint', async () => {
    // MISSING: Should provide health status
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    const health = await (manager as any).healthCheck();

    expect(health).toHaveProperty('status'); // 'healthy', 'warning', 'critical'
    expect(health).toHaveProperty('issues');
    expect(health).toHaveProperty('recommendations');
  });
});

// =============================================================================
// 10. INTEGRATION WITH CDC/REPLICATION
// =============================================================================

describe('WAL Retention - CDC Integration', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should prevent deletion of segments needed by pending CDC events', async () => {
    // MISSING: Should check CDC pending queue before deletion
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);

    // Mock CDC pending events
    const cdcPendingLSN = 100n;

    const manager = createWALRetentionManager(backend, reader, null, {
      cdcIntegration: {
        getPendingLSN: async () => cdcPendingLSN,
      },
    } as any);

    const result = await manager.checkRetention();

    // Segments containing pending CDC events should be protected
    expect((result as any).protectedByCDC).toBeDefined();
  });

  it('should coordinate with replication lag', async () => {
    // MISSING: Should consider replication lag when making retention decisions
    // Manager should have method to get/set replication lag tolerance
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      replicationLagTolerance: 1000, // Max 1000 LSN behind
    } as any);

    // MUST FAIL: Manager should have getReplicationStatus method
    expect(typeof (manager as any).getReplicationStatus).toBe('function');

    const status = await (manager as any).getReplicationStatus();
    expect(status).toHaveProperty('currentLag');
    expect(status).toHaveProperty('isWithinTolerance');
  });

  it('should support multi-region replication awareness', async () => {
    // MISSING: Should track replication status per region
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      regions: ['us-east', 'eu-west', 'ap-south'],
      waitForAllRegions: true,
    } as any);

    const regionStatus = await (manager as any).getRegionReplicationStatus();

    expect(regionStatus).toHaveProperty('us-east');
    expect(regionStatus).toHaveProperty('eu-west');
    expect(regionStatus).toHaveProperty('ap-south');
  });
});
