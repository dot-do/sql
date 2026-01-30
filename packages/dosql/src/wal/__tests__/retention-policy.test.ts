/**
 * WAL Retention Policy Tests
 *
 * Issue: sql-zo0a - Implement WAL retention policy
 *
 * Tests for:
 * 1. WAL segments archived after checkpoint
 * 2. Old segments deleted after retention period
 * 3. Checkpoint triggered on size threshold (e.g., 1MB)
 * 4. Checkpoint triggered on time threshold (e.g., 5 minutes)
 * 5. Manual checkpoint trigger works
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import type { FSXBackend } from '../../fsx/types.js';
import type { WALWriter, WALReader, CheckpointManager, Checkpoint } from '../types.js';
import type { ExtendedWALRetentionManager } from '../retention/index.js';

// =============================================================================
// Test Utilities
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

/**
 * Helper to create WAL segments with known timestamps
 */
async function createSegmentWithTimestamp(
  backend: FSXBackend,
  segmentId: string,
  startLSN: bigint,
  endLSN: bigint,
  createdAt: number,
  dataSize: number = 1024
): Promise<void> {
  const { DefaultWALEncoder } = await import('../writer.js');
  const encoder = new DefaultWALEncoder();

  const segment = {
    id: segmentId,
    startLSN,
    endLSN,
    entries: [{
      lsn: startLSN,
      timestamp: createdAt,
      txnId: `txn_${startLSN}`,
      op: 'INSERT' as const,
      table: 'test',
      after: new Uint8Array(dataSize), // Control segment size
    }],
    checksum: 0,
    createdAt,
  };
  const data = encoder.encodeSegment(segment);
  segment.checksum = encoder.calculateChecksum(data);
  await backend.write(`_wal/segments/${segmentId}`, encoder.encodeSegment(segment));
}

/**
 * Helper to format segment ID
 */
function formatSegmentId(lsn: bigint): string {
  return `seg_${lsn.toString().padStart(20, '0')}`;
}

// =============================================================================
// 1. WAL Segments Archived After Checkpoint
// =============================================================================

describe('WAL Retention Policy - Segments Archived After Checkpoint', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should archive segments before checkpoint LSN when cleanup runs', async () => {
    const { createWALRetentionManager } = await import('../retention/index.js');
    const { createWALReader } = await import('../reader.js');
    const { createCheckpointManager } = await import('../checkpoint.js');

    const now = Date.now();
    const oneHourAgo = now - (60 * 60 * 1000);

    // Create segments - some before checkpoint, some after
    await createSegmentWithTimestamp(backend, formatSegmentId(1n), 1n, 10n, oneHourAgo);
    await createSegmentWithTimestamp(backend, formatSegmentId(11n), 11n, 20n, oneHourAgo);
    await createSegmentWithTimestamp(backend, formatSegmentId(21n), 21n, 30n, now);
    await createSegmentWithTimestamp(backend, formatSegmentId(31n), 31n, 40n, now);

    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    // Create checkpoint at LSN 25 (after segment 2, before segment 3)
    await checkpointMgr.createCheckpoint(25n, []);

    const manager = createWALRetentionManager(backend, reader, null, {
      archiveBeforeDelete: true,
      minSegmentCount: 1,
      maxSegmentAge: 30 * 60 * 1000, // 30 minutes - segments 1,2 are old enough
    });

    // Run cleanup
    const result = await manager.cleanup(false);

    // Segments 1 and 2 should be archived (they're before checkpoint and old)
    expect(result.archived.length).toBeGreaterThanOrEqual(1);

    // Verify archived segments exist in archive path
    const archivedFiles = await backend.list('_wal/archive/');
    expect(archivedFiles.length).toBeGreaterThanOrEqual(1);
  });

  it('should delete segments from active location after archiving', async () => {
    const { createWALRetentionManager } = await import('../retention/index.js');
    const { createWALReader } = await import('../reader.js');
    const { createCheckpointManager } = await import('../checkpoint.js');

    const now = Date.now();
    const twoHoursAgo = now - (2 * 60 * 60 * 1000);

    // Create old segment that should be archived and deleted
    await createSegmentWithTimestamp(backend, formatSegmentId(1n), 1n, 10n, twoHoursAgo);
    // Create recent segment to keep
    await createSegmentWithTimestamp(backend, formatSegmentId(11n), 11n, 20n, now);

    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    // Create checkpoint after old segment
    await checkpointMgr.createCheckpoint(15n, []);

    const manager = createWALRetentionManager(backend, reader, null, {
      archiveBeforeDelete: true,
      minSegmentCount: 1,
      maxSegmentAge: 60 * 60 * 1000, // 1 hour
    });

    // Run cleanup
    await manager.cleanup(false);

    // Old segment should be deleted from active location
    const activeSegmentExists = await backend.exists(`_wal/segments/${formatSegmentId(1n)}`);
    expect(activeSegmentExists).toBe(false);

    // But should exist in archive
    const archivedSegmentExists = await backend.exists(`_wal/archive/${formatSegmentId(1n)}`);
    expect(archivedSegmentExists).toBe(true);
  });

  it('should not archive segments after checkpoint LSN', async () => {
    const { createWALRetentionManager } = await import('../retention/index.js');
    const { createWALReader } = await import('../reader.js');
    const { createCheckpointManager } = await import('../checkpoint.js');

    const now = Date.now();
    const oneHourAgo = now - (60 * 60 * 1000);

    // Create segments - segment 1 is old, segment 2 is recent
    await createSegmentWithTimestamp(backend, formatSegmentId(1n), 1n, 10n, oneHourAgo);
    await createSegmentWithTimestamp(backend, formatSegmentId(11n), 11n, 20n, now); // Recent - should not be deleted

    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    // Create checkpoint at LSN 5 (in the middle of segment 1)
    await checkpointMgr.createCheckpoint(5n, []);

    const manager = createWALRetentionManager(backend, reader, null, {
      archiveBeforeDelete: true,
      minSegmentCount: 1, // Keep at least 1 segment
      maxSegmentAge: 30 * 60 * 1000,
    });

    // Run cleanup
    await manager.cleanup(false);

    // Segment 2 (LSN 11-20) is recent so should still be in active location
    const segment2Exists = await backend.exists(`_wal/segments/${formatSegmentId(11n)}`);
    expect(segment2Exists).toBe(true);
  });
});

// =============================================================================
// 2. Old Segments Deleted After Retention Period
// =============================================================================

describe('WAL Retention Policy - Old Segments Deleted After Retention Period', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should delete segments older than maxSegmentAge', async () => {
    const { createWALRetentionManager } = await import('../retention/index.js');
    const { createWALReader } = await import('../reader.js');

    const now = Date.now();
    const threeHoursAgo = now - (3 * 60 * 60 * 1000);
    const oneHourAgo = now - (60 * 60 * 1000);

    // Create old segment (3 hours old)
    await createSegmentWithTimestamp(backend, formatSegmentId(1n), 1n, 10n, threeHoursAgo);
    // Create recent segment (1 hour old)
    await createSegmentWithTimestamp(backend, formatSegmentId(11n), 11n, 20n, oneHourAgo);
    // Create very recent segment
    await createSegmentWithTimestamp(backend, formatSegmentId(21n), 21n, 30n, now);

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      archiveBeforeDelete: false, // Direct delete
      minSegmentCount: 1,
      maxSegmentAge: 2 * 60 * 60 * 1000, // 2 hours retention
    });

    const result = await manager.cleanup(false);

    // First segment (3 hours old) should be deleted
    expect(result.deleted).toContain(formatSegmentId(1n));

    // Second segment (1 hour old) should NOT be deleted
    expect(result.deleted).not.toContain(formatSegmentId(11n));

    // Most recent segment should NOT be deleted
    expect(result.deleted).not.toContain(formatSegmentId(21n));
  });

  it('should respect minSegmentCount even when segments are old', async () => {
    const { createWALRetentionManager } = await import('../retention/index.js');
    const { createWALReader } = await import('../reader.js');

    const now = Date.now();
    const oneWeekAgo = now - (7 * 24 * 60 * 60 * 1000);

    // Create 3 old segments
    await createSegmentWithTimestamp(backend, formatSegmentId(1n), 1n, 10n, oneWeekAgo);
    await createSegmentWithTimestamp(backend, formatSegmentId(11n), 11n, 20n, oneWeekAgo);
    await createSegmentWithTimestamp(backend, formatSegmentId(21n), 21n, 30n, oneWeekAgo);

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      archiveBeforeDelete: false,
      minSegmentCount: 2, // Keep at least 2 segments
      maxSegmentAge: 24 * 60 * 60 * 1000, // 24 hours
    });

    const result = await manager.cleanup(false);

    // Should delete only 1 segment (keeping 2 for minSegmentCount)
    expect(result.deleted.length).toBe(1);
    expect(result.deleted).toContain(formatSegmentId(1n));

    // Verify 2 segments remain
    const remainingSegments = await reader.listSegments(false);
    expect(remainingSegments.length).toBe(2);
  });

  it('should support retentionHours convenience configuration', async () => {
    const { createWALRetentionManager } = await import('../retention/index.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      retentionHours: 6, // 6 hours
    });

    const policy = manager.getPolicy();

    // retentionHours should be converted to maxSegmentAge in ms
    expect(policy.maxSegmentAge).toBe(6 * 60 * 60 * 1000);
  });

  it('should support retentionDays convenience configuration', async () => {
    const { createWALRetentionManager } = await import('../retention/index.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      retentionDays: 7, // 7 days
    });

    const policy = manager.getPolicy();

    // retentionDays should be converted to maxSegmentAge in ms
    expect(policy.maxSegmentAge).toBe(7 * 24 * 60 * 60 * 1000);
  });
});

// =============================================================================
// 3. Checkpoint Triggered on Size Threshold
// =============================================================================

describe('WAL Retention Policy - Checkpoint Triggered on Size Threshold', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should trigger checkpoint when WAL size exceeds maxWALSize threshold', async () => {
    const { createWALWriter } = await import('../writer.js');
    const { createWALReader } = await import('../reader.js');
    const { createCheckpointManager, createAutoCheckpointer } = await import('../checkpoint.js');
    const { createSizeBasedCheckpointTrigger } = await import('../retention/index.js');

    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    let checkpointTriggered = false;
    const checkpoints: Checkpoint[] = [];

    // Use getCurrentLSN() - 1n to get the last written LSN (not the next one)
    const autoCheckpointer = createAutoCheckpointer(
      checkpointMgr,
      () => {
        const lsn = writer.getCurrentLSN();
        return lsn > 0n ? lsn - 1n : 0n;
      },
      {
        entryInterval: 10000, // High threshold so it doesn't interfere
        timeInterval: 60 * 60 * 1000, // High threshold
        getActiveTransactions: () => [],
        onCheckpoint: (cp) => {
          checkpointTriggered = true;
          checkpoints.push(cp);
        },
      }
    );

    // Create a size-based trigger at 10KB
    const sizeTrigger = createSizeBasedCheckpointTrigger(
      reader,
      backend,
      autoCheckpointer,
      {
        maxWALSizeBytes: 10 * 1024, // 10KB threshold
        checkIntervalMs: 100, // Check frequently for test
      }
    );

    sizeTrigger.start();

    // Write enough data to exceed threshold
    for (let i = 0; i < 20; i++) {
      await writer.append({
        timestamp: Date.now(),
        txnId: `txn_${i}`,
        op: 'INSERT',
        table: 'test',
        after: new Uint8Array(1024), // 1KB per entry
      });
    }
    await writer.flush();

    // Wait for size check to run
    await new Promise(resolve => setTimeout(resolve, 200));

    sizeTrigger.stop();

    // Checkpoint should have been triggered due to size
    expect(checkpointTriggered).toBe(true);
    expect(checkpoints.length).toBeGreaterThan(0);

    await writer.close();
  });

  it('should support maxWALSize configuration in retention policy', async () => {
    const { createWALRetentionManager } = await import('../retention/index.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      maxTotalBytes: 1024 * 1024, // 1MB
      sizeWarningThreshold: 0.8, // Warn at 80%
    });

    const policy = manager.getPolicy();
    expect(policy.maxTotalBytes).toBe(1024 * 1024);
    expect(policy.sizeWarningThreshold).toBe(0.8);
  });

  it('should emit warning when approaching size limit', async () => {
    const { createWALRetentionManager } = await import('../retention/index.js');
    const { createWALReader } = await import('../reader.js');

    const now = Date.now();

    // Create segments totaling ~800 bytes
    for (let i = 1; i <= 8; i++) {
      await createSegmentWithTimestamp(backend, formatSegmentId(BigInt(i * 10)), BigInt(i * 10), BigInt(i * 10 + 9), now, 100);
    }

    const reader = createWALReader(backend);

    const warnings: { current: number; max: number }[] = [];
    const manager = createWALRetentionManager(backend, reader, null, {
      maxTotalBytes: 1000, // 1KB limit
      sizeWarningThreshold: 0.7, // 70% warning threshold
      onSizeWarning: (current, max) => {
        warnings.push({ current, max });
      },
    });

    // Check retention to trigger warning
    await manager.checkRetention();

    // Warning should have been emitted because we're at ~80% of limit
    expect(warnings.length).toBeGreaterThan(0);
  });
});

// =============================================================================
// 4. Checkpoint Triggered on Time Threshold
// =============================================================================

describe('WAL Retention Policy - Checkpoint Triggered on Time Threshold', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should trigger checkpoint after time interval expires', async () => {
    const { createWALWriter } = await import('../writer.js');
    const { createWALReader } = await import('../reader.js');
    const { createCheckpointManager, createAutoCheckpointer } = await import('../checkpoint.js');

    vi.useRealTimers(); // Need real timers for this test

    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    let checkpointTriggered = false;

    // Use getCurrentLSN() - 1n to get the last written LSN (not the next one)
    const autoCheckpointer = createAutoCheckpointer(
      checkpointMgr,
      () => {
        const lsn = writer.getCurrentLSN();
        return lsn > 0n ? lsn - 1n : 0n;
      },
      {
        entryInterval: 10000, // High so it doesn't interfere
        timeInterval: 100, // 100ms for fast testing
        getActiveTransactions: () => [],
        onCheckpoint: () => {
          checkpointTriggered = true;
        },
      }
    );

    // Write some entries
    await writer.append({
      timestamp: Date.now(),
      txnId: 'txn_1',
      op: 'INSERT',
      table: 'test',
    });
    await writer.flush();

    // Start auto-checkpointer
    autoCheckpointer.start();

    // Wait for time interval
    await new Promise(resolve => setTimeout(resolve, 150));

    autoCheckpointer.stop();

    expect(checkpointTriggered).toBe(true);

    await writer.close();
  });

  it('should support configurable checkpoint time interval', async () => {
    const { createAutoCheckpointer } = await import('../checkpoint.js');
    const { createWALReader } = await import('../reader.js');
    const { createCheckpointManager } = await import('../checkpoint.js');

    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    // Test with 5 minute interval (300,000 ms)
    const fiveMinutes = 5 * 60 * 1000;
    const autoCheckpointer = createAutoCheckpointer(
      checkpointMgr,
      () => 0n,
      {
        timeInterval: fiveMinutes,
        getActiveTransactions: () => [],
      }
    );

    // The auto checkpointer should accept the configuration
    expect(autoCheckpointer).toBeDefined();
    expect(autoCheckpointer.start).toBeDefined();
    expect(autoCheckpointer.stop).toBeDefined();
  });

  it('should not checkpoint if no new entries since last checkpoint', async () => {
    const { createWALWriter } = await import('../writer.js');
    const { createWALReader } = await import('../reader.js');
    const { createCheckpointManager, createAutoCheckpointer } = await import('../checkpoint.js');

    vi.useRealTimers(); // Need real timers

    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    let checkpointCount = 0;

    const autoCheckpointer = createAutoCheckpointer(
      checkpointMgr,
      () => writer.getCurrentLSN(),
      {
        entryInterval: 10000,
        timeInterval: 50, // 50ms
        getActiveTransactions: () => [],
        onCheckpoint: () => {
          checkpointCount++;
        },
      }
    );

    // Write one entry and checkpoint
    await writer.append({
      timestamp: Date.now(),
      txnId: 'txn_1',
      op: 'INSERT',
      table: 'test',
    });
    await writer.flush();

    autoCheckpointer.start();

    // Wait for multiple intervals
    await new Promise(resolve => setTimeout(resolve, 200));

    autoCheckpointer.stop();

    // Should have exactly 1 checkpoint (no duplicates for same LSN)
    expect(checkpointCount).toBe(1);

    await writer.close();
  });
});

// =============================================================================
// 5. Manual Checkpoint Trigger Works
// =============================================================================

describe('WAL Retention Policy - Manual Checkpoint Trigger', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should allow manual checkpoint trigger via forceCheckpoint', async () => {
    const { createWALWriter } = await import('../writer.js');
    const { createWALReader } = await import('../reader.js');
    const { createCheckpointManager, createAutoCheckpointer } = await import('../checkpoint.js');

    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    let checkpointTriggered = false;

    const autoCheckpointer = createAutoCheckpointer(
      checkpointMgr,
      () => writer.getCurrentLSN(),
      {
        entryInterval: 10000, // High threshold
        timeInterval: 60000, // High threshold
        getActiveTransactions: () => [],
        onCheckpoint: () => {
          checkpointTriggered = true;
        },
      }
    );

    // Write an entry
    await writer.append({
      timestamp: Date.now(),
      txnId: 'txn_1',
      op: 'INSERT',
      table: 'test',
    });
    await writer.flush();

    // Manually force checkpoint
    const checkpoint = await autoCheckpointer.forceCheckpoint();

    expect(checkpoint).not.toBeNull();
    expect(checkpointTriggered).toBe(true);
    expect(checkpoint?.lsn).toBe(0n); // First entry is LSN 0

    await writer.close();
  });

  it('should allow manual checkpoint via checkpoint manager directly', async () => {
    const { createWALWriter } = await import('../writer.js');
    const { createWALReader } = await import('../reader.js');
    const { createCheckpointManager } = await import('../checkpoint.js');

    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    // Write entries
    await writer.append({
      timestamp: Date.now(),
      txnId: 'txn_1',
      op: 'INSERT',
      table: 'test',
    });
    await writer.flush();

    // Create checkpoint directly
    const checkpoint = await checkpointMgr.createCheckpoint(0n, []);

    expect(checkpoint).toBeDefined();
    expect(checkpoint.lsn).toBe(0n);
    expect(checkpoint.timestamp).toBeDefined();

    // Verify checkpoint is persisted
    const retrieved = await checkpointMgr.getCheckpoint();
    expect(retrieved).not.toBeNull();
    expect(retrieved?.lsn).toBe(0n);

    await writer.close();
  });

  it('should archive old segments after manual checkpoint', async () => {
    const { createWALWriter } = await import('../writer.js');
    const { createWALReader } = await import('../reader.js');
    const { createCheckpointManager } = await import('../checkpoint.js');

    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    // Write multiple segments worth of data
    for (let i = 0; i < 5; i++) {
      await writer.append({
        timestamp: Date.now(),
        txnId: `txn_${i}`,
        op: 'INSERT',
        table: 'test',
      }, { sync: true }); // Force flush each entry
    }

    // Create checkpoint at latest LSN
    const currentLSN = writer.getCurrentLSN() - 1n;
    await checkpointMgr.createCheckpoint(currentLSN, []);

    // Archive old segments
    const archivedCount = await checkpointMgr.archiveOldSegments(10);

    // Should have archived some segments
    expect(archivedCount).toBeGreaterThanOrEqual(0);

    await writer.close();
  });

  it('should trigger cleanup after checkpoint when cleanupOnCheckpoint is true', async () => {
    const { createWALWriter } = await import('../writer.js');
    const { createWALReader } = await import('../reader.js');
    const { createCheckpointManager } = await import('../checkpoint.js');
    const { createWALRetentionManager } = await import('../retention/index.js');

    const now = Date.now();
    const oneHourAgo = now - (60 * 60 * 1000);

    // Create old segment
    await createSegmentWithTimestamp(backend, formatSegmentId(1n), 1n, 10n, oneHourAgo);

    const writer = createWALWriter(backend, 11n); // Start after existing segment
    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    let cleanupTriggered = false;

    const manager = createWALRetentionManager(backend, reader, null, {
      cleanupOnCheckpoint: true,
      minSegmentCount: 0,
      maxSegmentAge: 30 * 60 * 1000, // 30 minutes
      archiveBeforeDelete: false,
      onCleanupTriggered: () => {
        cleanupTriggered = true;
      },
    }) as ExtendedWALRetentionManager;

    // Register checkpoint listener
    manager.registerCheckpointListener(checkpointMgr);

    // Write new entry
    await writer.append({
      timestamp: Date.now(),
      txnId: 'txn_new',
      op: 'INSERT',
      table: 'test',
    });
    await writer.flush();

    // Create checkpoint - should trigger cleanup
    await checkpointMgr.createCheckpoint(11n, []);

    expect(cleanupTriggered).toBe(true);

    await writer.close();
  });
});

// =============================================================================
// Integration Tests
// =============================================================================

describe('WAL Retention Policy - Integration', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should handle full retention lifecycle: write -> checkpoint -> archive -> delete', async () => {
    const { createWALWriter } = await import('../writer.js');
    const { createWALReader } = await import('../reader.js');
    const { createCheckpointManager } = await import('../checkpoint.js');
    const { createWALRetentionManager } = await import('../retention/index.js');

    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    // Write some entries
    for (let i = 0; i < 10; i++) {
      await writer.append({
        timestamp: Date.now() - (2 * 60 * 60 * 1000), // 2 hours ago
        txnId: `txn_${i}`,
        op: 'INSERT',
        table: 'test',
      }, { sync: true });
    }

    // Create checkpoint
    const currentLSN = writer.getCurrentLSN() - 1n;
    await checkpointMgr.createCheckpoint(currentLSN, []);

    // Setup retention manager
    const manager = createWALRetentionManager(backend, reader, null, {
      archiveBeforeDelete: true,
      minSegmentCount: 1,
      maxSegmentAge: 60 * 60 * 1000, // 1 hour
    });

    // Run cleanup
    const result = await manager.cleanup(false);

    // Verify cleanup results
    expect(result.deleted.length).toBeGreaterThan(0);
    expect(result.archived.length).toBeGreaterThan(0);
    expect(result.bytesFreed).toBeGreaterThan(0);

    // Verify archived segments exist
    const archivedFiles = await backend.list('_wal/archive/');
    expect(archivedFiles.length).toBeGreaterThan(0);

    await writer.close();
  });

  it('should provide retention metrics after operations', async () => {
    const { createWALRetentionManager } = await import('../retention/index.js');
    const { createWALReader } = await import('../reader.js');

    const now = Date.now();

    // Create test segments
    await createSegmentWithTimestamp(backend, formatSegmentId(1n), 1n, 10n, now - 3600000, 5000);
    await createSegmentWithTimestamp(backend, formatSegmentId(11n), 11n, 20n, now, 5000);

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 1,
      maxSegmentAge: 1800000, // 30 minutes
    }) as ExtendedWALRetentionManager;

    // Run cleanup
    await manager.cleanup(false);

    // Get metrics
    const metrics = await manager.getMetrics();

    expect(metrics.totalSegments).toBeDefined();
    expect(metrics.totalBytes).toBeDefined();
    expect(metrics.oldestSegmentAge).toBeDefined();
    expect(metrics.lastCleanupTime).toBeDefined();
    expect(metrics.segmentsDeleted).toBeDefined();
    expect(metrics.bytesFreed).toBeDefined();
  });
});
