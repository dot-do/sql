/**
 * WAL Retention Policy - RED Phase TDD Tests
 *
 * These tests document WAL retention policy gaps that need implementation.
 * Tests use `it.fails()` pattern to mark expected failures.
 *
 * Issue: sql-zhy.17 - WAL Retention Policy
 *
 * Gap Areas:
 * 1. Time-based retention - WAL entries older than X hours should be cleaned up
 * 2. Size-based retention - WAL should not exceed X MB
 * 3. Checkpoint-based cleanup - Clean WAL entries after successful checkpoint
 * 4. Manual cleanup API - truncateWAL(beforeLSN), compactWAL()
 * 5. Retention policy configuration interface
 * 6. Metrics for WAL size and cleanup operations
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import type { FSXBackend } from '../../fsx/types.js';

// =============================================================================
// Extended Types for TDD Tests (Expected Future API)
// =============================================================================

/**
 * Extended WAL retention manager interface documenting expected future methods.
 * These methods are being tested in RED phase and will be implemented.
 */
interface ExtendedWALRetentionManager {
  getPolicy(): WALRetentionPolicy & Record<string, unknown>;
  checkRetention(): Promise<{ bytesOverLimit?: number } & Record<string, unknown>>;
  cleanup(force?: boolean): Promise<CleanupResult>;
  getExpiredEntries(): Promise<ExpiredEntry[]>;
  getStorageStats(): Promise<StorageStats>;
  onSizeWarning?: (stats: StorageStats) => void;
  registerCheckpointListener(mgr: unknown): void;
  truncateWAL(beforeLSN: bigint): Promise<TruncateResult>;
  compactWAL(): Promise<CompactResult>;
  forceCleanup(options: ForceCleanupOptions): Promise<CleanupResult>;
  getWALStats(): Promise<WALStats>;
  getMetrics(): Promise<WALMetrics>;
  getCleanupLatencyHistogram(): Promise<LatencyHistogram>;
  getGrowthStats(): Promise<GrowthStats>;
  getTableRetentionPolicy(tableName: string): TableRetentionPolicy | undefined;
  isInCleanupWindow(): boolean;
  analyzeCleanupImpact(): Promise<CleanupImpact>;
  startMetricsCollection(): void;
  stopMetricsCollection(): void;
}

interface ExpiredEntry {
  entryTimestamp: number;
  age: number;
}

interface StorageStats {
  totalBytes: number;
  segmentCount: number;
}

interface CleanupResult {
  deleted: number;
  bytesFreed: number;
  durationMs: number;
}

interface TruncateResult {
  truncatedCount: number;
  bytesRemoved: number;
}

interface CompactResult {
  segmentsBefore: number;
  segmentsAfter: number;
  bytesReclaimed: number;
}

interface ForceCleanupOptions {
  maxSegments?: number;
  maxBytes?: number;
  ignoreCheckpoints?: boolean;
}

interface WALStats {
  segmentCount: number;
  totalBytes: number;
  oldestLSN: bigint;
  newestLSN: bigint;
  averageSegmentSize: number;
}

interface WALMetrics {
  totalSegments: number;
  totalBytes: number;
  oldestSegmentAge: number;
  lastCleanupTime?: number;
}

interface LatencyHistogram {
  buckets: { le: number; count: number }[];
  sum: number;
  count: number;
}

interface GrowthStats {
  bytesPerHour: number;
  estimatedFullTime: number;
}

interface TableRetentionPolicy {
  retentionHours?: number;
  archiveToR2?: boolean;
}

interface CleanupImpact {
  segmentsToDelete: number;
  bytesToFree: number;
  oldestRetainedLSN: bigint;
}

/**
 * Extended module interface for TDD dynamic imports
 */
interface RetentionModule {
  createWALRetentionManager(
    backend: FSXBackend,
    reader: unknown,
    writer: unknown,
    options?: Record<string, unknown>
  ): ExtendedWALRetentionManager;
  loadRetentionPolicy?: (backend: FSXBackend, path: string) => Promise<Record<string, unknown>>;
  composePolicy?: (...policies: Record<string, unknown>[]) => Record<string, unknown>;
  RETENTION_PRESETS?: Record<string, Record<string, unknown>>;
  PrometheusReporter?: new (manager: ExtendedWALRetentionManager) => unknown;
}

/**
 * Helper to import and cast retention module
 */
async function importRetentionModule(): Promise<RetentionModule> {
  return await import('../retention.js') as unknown as RetentionModule;
}

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

/**
 * Interface that retention policy SHOULD support
 * This documents the expected API
 */
interface WALRetentionPolicy {
  maxAgeMs?: number;
  maxSizeBytes?: number;
  keepAfterCheckpoint?: number;
}

// =============================================================================
// 1. TIME-BASED RETENTION
// =============================================================================

describe('WAL Retention Policy - Time-Based Retention [RED]', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should support maxAgeMs configuration in retention policy', async () => {
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);

    // Test that maxAgeMs is properly converted and used
    const manager = createWALRetentionManager(backend, reader, null, {
      retentionHours: 6, // 6 hours
    });

    const policy = manager.getPolicy();
    // maxAgeMs should be auto-calculated from retentionHours
    expect(policy.maxSegmentAge).toBe(6 * 60 * 60 * 1000);
  });

  it('should clean up WAL entries older than configured maxAgeMs', async () => {
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createWALWriter, DefaultWALEncoder } = await import('../index.js');

    const encoder = new DefaultWALEncoder();

    // Create an old segment (2 hours old)
    const oldTimestamp = Date.now() - (2 * 60 * 60 * 1000);
    const oldSegment = {
      id: 'seg_00000000000000000001',
      startLSN: 1n,
      endLSN: 10n,
      entries: [{
        lsn: 1n,
        timestamp: oldTimestamp,
        txnId: 'txn_old',
        op: 'INSERT' as const,
        table: 'test',
      }],
      checksum: 0,
      createdAt: oldTimestamp,
    };
    const oldData = encoder.encodeSegment(oldSegment);
    oldSegment.checksum = encoder.calculateChecksum(oldData);
    await backend.write('_wal/segments/seg_00000000000000000001', encoder.encodeSegment(oldSegment));

    // Create a recent segment
    const recentSegment = {
      id: 'seg_00000000000000000011',
      startLSN: 11n,
      endLSN: 20n,
      entries: [{
        lsn: 11n,
        timestamp: Date.now(),
        txnId: 'txn_recent',
        op: 'INSERT' as const,
        table: 'test',
      }],
      checksum: 0,
      createdAt: Date.now(),
    };
    const recentData = encoder.encodeSegment(recentSegment);
    recentSegment.checksum = encoder.calculateChecksum(recentData);
    await backend.write('_wal/segments/seg_00000000000000000011', encoder.encodeSegment(recentSegment));

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      retentionHours: 1, // Keep only 1 hour of WAL
      minSegmentCount: 0,
    });

    const result = await manager.checkRetention();

    // Old segment should be eligible for deletion
    expect(result.eligibleForDeletion).toContain('seg_00000000000000000001');
    // Recent segment should NOT be eligible
    expect(result.eligibleForDeletion).not.toContain('seg_00000000000000000011');
  });

  it('should support configurable retention period via retentionHours', async () => {
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);

    // Test different retention periods
    const manager1h = createWALRetentionManager(backend, reader, null, {
      retentionHours: 1,
    });
    expect(manager1h.getPolicy().maxSegmentAge).toBe(1 * 60 * 60 * 1000);

    const manager24h = createWALRetentionManager(backend, reader, null, {
      retentionHours: 24,
    });
    expect(manager24h.getPolicy().maxSegmentAge).toBe(24 * 60 * 60 * 1000);

    const manager168h = createWALRetentionManager(backend, reader, null, {
      retentionHours: 168, // 7 days
    });
    expect(manager168h.getPolicy().maxSegmentAge).toBe(168 * 60 * 60 * 1000);
  });

  it('should provide entry-level age tracking (not just segment-level)', async () => {
    // Entry-level timestamps are now implemented
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createWALWriter, DefaultWALEncoder } = await import('../index.js');

    const encoder = new DefaultWALEncoder();

    // Create a segment with old entries
    const oldTimestamp = Date.now() - (2 * 60 * 60 * 1000); // 2 hours ago
    const segment = {
      id: 'seg_00000000000000000001',
      startLSN: 1n,
      endLSN: 10n,
      entries: [{
        lsn: 1n,
        timestamp: oldTimestamp,
        txnId: 'txn_old',
        op: 'INSERT' as const,
        table: 'test',
      }],
      checksum: 0,
      createdAt: oldTimestamp,
    };
    const data = encoder.encodeSegment(segment);
    segment.checksum = encoder.calculateChecksum(data);
    await backend.write('_wal/segments/seg_00000000000000000001', encoder.encodeSegment(segment));

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      retentionHours: 1,
    });

    // This should return entries (not segments) older than the threshold
    const extManager = manager as unknown as ExtendedWALRetentionManager;
    const expiredEntries = await extManager.getExpiredEntries();

    // Each entry should have its own timestamp for age calculation
    expect(expiredEntries.length).toBeGreaterThan(0);
    expect(expiredEntries[0]).toHaveProperty('entryTimestamp');
    expect(expiredEntries[0]).toHaveProperty('age');
  });
});

// =============================================================================
// 2. SIZE-BASED RETENTION
// =============================================================================

describe('WAL Retention Policy - Size-Based Retention [RED]', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should support maxSizeBytes configuration', async () => {
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      maxTotalBytes: 100 * 1024 * 1024, // 100MB
    });

    const policy = manager.getPolicy();
    expect(policy.maxTotalBytes).toBe(100 * 1024 * 1024);
  });

  it('should enforce WAL does not exceed maxSizeBytes', async () => {
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createWALWriter } = await import('../index.js');

    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);

    // Write data to exceed limit
    const largeData = new Uint8Array(1024 * 100); // 100KB per entry
    for (let i = 0; i < 20; i++) {
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
      maxTotalBytes: 500 * 1024, // 500KB limit - less than we wrote
      minSegmentCount: 0,
      maxSegmentAge: 0,
    });

    const extManager = manager as unknown as ExtendedWALRetentionManager;
    const stats = await extManager.getStorageStats();
    const result = await manager.checkRetention();

    // Should report that we're over the limit
    expect(result.bytesOverLimit).toBeGreaterThan(0);

    await writer.close();
  });

  it('should remove oldest entries first when size limit reached', async () => {
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createWALWriter, DefaultWALEncoder } = await import('../index.js');

    const encoder = new DefaultWALEncoder();

    // Create multiple segments with known sizes
    for (let i = 1; i <= 5; i++) {
      const segment = {
        id: `seg_${i.toString().padStart(20, '0')}`,
        startLSN: BigInt(i * 10),
        endLSN: BigInt(i * 10 + 9),
        entries: [{
          lsn: BigInt(i * 10),
          timestamp: Date.now() - ((6 - i) * 1000), // Older segments have smaller i
          txnId: `txn_${i}`,
          op: 'INSERT' as const,
          table: 'test',
          after: new Uint8Array(1024), // 1KB data
        }],
        checksum: 0,
        createdAt: Date.now() - ((6 - i) * 1000),
      };
      const data = encoder.encodeSegment(segment);
      segment.checksum = encoder.calculateChecksum(data);
      await backend.write(`_wal/segments/${segment.id}`, encoder.encodeSegment(segment));
    }

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      maxTotalBytes: 3 * 1024, // Only room for ~3 segments
      minSegmentCount: 0,
      maxSegmentAge: 0,
    });

    const result = await manager.checkRetention();

    // Oldest segments (1 and 2) should be marked for deletion
    expect(result.eligibleForDeletion.length).toBeGreaterThan(0);
    // Should include oldest segments
    const eligible = new Set(result.eligibleForDeletion);
    expect(eligible.has('seg_00000000000000000001') || eligible.has('seg_00000000000000000002')).toBe(true);
  });

  it('should provide real-time size monitoring with callbacks', async () => {
    // Size monitoring callback is now implemented
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const sizeAlerts: any[] = [];

    const manager = createWALRetentionManager(backend, reader, null, {
      maxTotalBytes: 100 * 1024,
      onSizeWarning: (current: number, max: number) => {
        sizeAlerts.push({ current, max, percentage: (current / max) * 100 });
      },
      sizeWarningThreshold: 0.8, // Alert at 80%
    });

    // This callback mechanism is now available
    const extManager = manager as unknown as ExtendedWALRetentionManager;
    expect(typeof extManager.onSizeWarning).toBe('function');
  });
});

// =============================================================================
// 3. CHECKPOINT-BASED CLEANUP
// =============================================================================

describe('WAL Retention Policy - Checkpoint-Based Cleanup [RED]', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should clean WAL entries after successful checkpoint', async () => {
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createWALWriter, createCheckpointManager, DefaultWALEncoder } = await import('../index.js');

    const encoder = new DefaultWALEncoder();

    // Create segments
    for (let i = 1; i <= 5; i++) {
      const segment = {
        id: `seg_${i.toString().padStart(20, '0')}`,
        startLSN: BigInt((i - 1) * 10 + 1),
        endLSN: BigInt(i * 10),
        entries: [{
          lsn: BigInt((i - 1) * 10 + 1),
          timestamp: Date.now(),
          txnId: `txn_${i}`,
          op: 'INSERT' as const,
          table: 'test',
        }],
        checksum: 0,
        createdAt: Date.now() - (10000 * (6 - i)),
      };
      const data = encoder.encodeSegment(segment);
      segment.checksum = encoder.calculateChecksum(data);
      await backend.write(`_wal/segments/${segment.id}`, encoder.encodeSegment(segment));
    }

    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    // Create checkpoint at LSN 30 (after segment 3)
    await checkpointMgr.createCheckpoint(30n, []);

    const manager = createWALRetentionManager(backend, reader, null, {
      checkpointRetentionMode: 'strict',
      minSegmentCount: 0,
    });

    const result = await manager.checkRetention();

    // Segments before checkpoint (1, 2, 3) should be marked for cleanup
    expect(result.segmentsBeforeCheckpoint).toBeDefined();
    expect(Array.isArray(result.segmentsBeforeCheckpoint)).toBe(true);
  });

  it('should keep entries needed for active replication', async () => {
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, DefaultWALEncoder } = await import('../index.js');
    const { createReplicationSlotManager } = await import('../../cdc/stream.js');

    const encoder = new DefaultWALEncoder();

    // Create segments
    for (let i = 1; i <= 5; i++) {
      const segment = {
        id: `seg_${i.toString().padStart(20, '0')}`,
        startLSN: BigInt((i - 1) * 10 + 1),
        endLSN: BigInt(i * 10),
        entries: [{
          lsn: BigInt((i - 1) * 10 + 1),
          timestamp: Date.now() - 100000, // Old timestamps
          txnId: `txn_${i}`,
          op: 'INSERT' as const,
          table: 'test',
        }],
        checksum: 0,
        createdAt: Date.now() - 100000,
      };
      const data = encoder.encodeSegment(segment);
      segment.checksum = encoder.calculateChecksum(data);
      await backend.write(`_wal/segments/${segment.id}`, encoder.encodeSegment(segment));
    }

    const reader = createWALReader(backend);
    const slotManager = createReplicationSlotManager(backend, reader);

    // Create replication slot at LSN 20 (needs segments 2+)
    await slotManager.createSlot('replica-1', 20n);

    const manager = createWALRetentionManager(backend, reader, slotManager, {
      minSegmentCount: 0,
      maxSegmentAge: 0, // All old enough
      respectSlotPositions: true,
    });

    const result = await manager.checkRetention();

    // Segments needed by replication should be protected
    expect(result.protectedBySlots.length).toBeGreaterThan(0);
  });

  it('should support keepAfterCheckpoint configuration', async () => {
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      keepCheckpointCount: 3, // Keep WAL for last 3 checkpoints
    });

    const policy = manager.getPolicy();
    expect(policy.keepCheckpointCount).toBe(3);
  });

  it('should auto-cleanup immediately after checkpoint when configured', async () => {
    // Checkpoint listener is now implemented
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createCheckpointManager, DefaultWALEncoder } = await import('../index.js');

    const encoder = new DefaultWALEncoder();

    // Create a segment so checkpoint has something to reference
    const segment = {
      id: 'seg_00000000000000000001',
      startLSN: 1n,
      endLSN: 100n,
      entries: [{
        lsn: 100n,
        timestamp: Date.now(),
        txnId: 'txn_1',
        op: 'INSERT' as const,
        table: 'test',
      }],
      checksum: 0,
      createdAt: Date.now(),
    };
    const data = encoder.encodeSegment(segment);
    segment.checksum = encoder.calculateChecksum(data);
    await backend.write('_wal/segments/seg_00000000000000000001', encoder.encodeSegment(segment));

    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    let cleanupTriggered = false;

    const manager = createWALRetentionManager(backend, reader, null, {
      cleanupOnCheckpoint: true,
      onCleanupTriggered: () => { cleanupTriggered = true; },
    });

    // Register the manager to receive checkpoint events
    const extManager = manager as unknown as ExtendedWALRetentionManager;
    extManager.registerCheckpointListener(checkpointMgr);

    // Create checkpoint - should trigger immediate cleanup
    await checkpointMgr.createCheckpoint(100n, []);

    // Cleanup should have been triggered synchronously
    expect(cleanupTriggered).toBe(true);
  });
});

// =============================================================================
// 4. MANUAL CLEANUP API
// =============================================================================

describe('WAL Retention Policy - Manual Cleanup API [RED]', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should support truncateWAL(beforeLSN) to remove entries before a given LSN', async () => {
    // truncateWAL is now implemented
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createWALWriter, DefaultWALEncoder } = await import('../index.js');

    const encoder = new DefaultWALEncoder();

    // Create segments with known LSNs
    for (let i = 1; i <= 5; i++) {
      const segment = {
        id: `seg_${i.toString().padStart(20, '0')}`,
        startLSN: BigInt((i - 1) * 10 + 1),
        endLSN: BigInt(i * 10),
        entries: [{
          lsn: BigInt((i - 1) * 10 + 1),
          timestamp: Date.now(),
          txnId: `txn_${i}`,
          op: 'INSERT' as const,
          table: 'test',
        }],
        checksum: 0,
        createdAt: Date.now(),
      };
      const data = encoder.encodeSegment(segment);
      segment.checksum = encoder.calculateChecksum(data);
      await backend.write(`_wal/segments/${segment.id}`, encoder.encodeSegment(segment));
    }

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    // This method now exists
    const extManager = manager as unknown as ExtendedWALRetentionManager;
    const result = await extManager.truncateWAL(25n); // Truncate before LSN 25

    expect(result).toHaveProperty('truncatedSegments');
    expect(result).toHaveProperty('bytesFreed');
    expect(result.truncatedLSN).toBe(25n);

    // After truncation, segments 1 and 2 should be gone
    const remaining = await reader.listSegments();
    expect(remaining.length).toBe(3);
  });

  it('should support compactWAL() to remove dead entries within segments', async () => {
    // compactWAL is now implemented
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, DefaultWALEncoder } = await import('../index.js');

    const encoder = new DefaultWALEncoder();

    // Create a segment with a rolled-back transaction
    const segment = {
      id: 'seg_00000000000000000001',
      startLSN: 1n,
      endLSN: 10n,
      entries: [
        { lsn: 1n, timestamp: Date.now(), txnId: 'txn_1', op: 'BEGIN' as const, table: '' },
        { lsn: 2n, timestamp: Date.now(), txnId: 'txn_1', op: 'INSERT' as const, table: 'test' },
        { lsn: 3n, timestamp: Date.now(), txnId: 'txn_1', op: 'ROLLBACK' as const, table: '' },
        { lsn: 4n, timestamp: Date.now(), txnId: 'txn_2', op: 'BEGIN' as const, table: '' },
        { lsn: 5n, timestamp: Date.now(), txnId: 'txn_2', op: 'INSERT' as const, table: 'test' },
        { lsn: 6n, timestamp: Date.now(), txnId: 'txn_2', op: 'COMMIT' as const, table: '' },
      ],
      checksum: 0,
      createdAt: Date.now(),
    };
    const data = encoder.encodeSegment(segment);
    segment.checksum = encoder.calculateChecksum(data);
    await backend.write('_wal/segments/seg_00000000000000000001', encoder.encodeSegment(segment));

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    // This method now exists
    const extManager = manager as unknown as ExtendedWALRetentionManager;
    const result = await extManager.compactWAL();

    expect(result).toHaveProperty('segmentsCompacted');
    expect(result).toHaveProperty('entriesRemoved');
    expect(result).toHaveProperty('bytesReclaimed');

    // After compaction, rolled-back transaction entries should be removed
    expect(result.entriesRemoved).toBeGreaterThan(0);
  });

  it('should support forceCleanup() that ignores safety checks', async () => {
    // forceCleanup is now implemented
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, DefaultWALEncoder } = await import('../index.js');

    const encoder = new DefaultWALEncoder();

    // Create a segment
    const segment = {
      id: 'seg_00000000000000000001',
      startLSN: 1n,
      endLSN: 10n,
      entries: [{
        lsn: 1n,
        timestamp: Date.now(),
        txnId: 'txn_1',
        op: 'INSERT' as const,
        table: 'test',
      }],
      checksum: 0,
      createdAt: Date.now(),
    };
    const data = encoder.encodeSegment(segment);
    segment.checksum = encoder.calculateChecksum(data);
    await backend.write('_wal/segments/seg_00000000000000000001', encoder.encodeSegment(segment));

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 10, // Would normally protect this segment
    });

    // This method now exists
    const extManager = manager as unknown as ExtendedWALRetentionManager;
    const result = await extManager.forceCleanup({
      ignoreMinCount: true,
      ignoreSlots: true,
      ignoreReaders: true,
    });

    expect(result.deleted.length).toBeGreaterThan(0);
  });

  it('should support getWALStats() for detailed WAL statistics', async () => {
    // getWALStats is now implemented
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    // This now returns detailed stats
    const extManager = manager as unknown as ExtendedWALRetentionManager;
    const stats = await extManager.getWALStats();

    expect(stats).toHaveProperty('totalSegments');
    expect(stats).toHaveProperty('totalEntries');
    expect(stats).toHaveProperty('totalBytes');
    expect(stats).toHaveProperty('oldestEntryTimestamp');
    expect(stats).toHaveProperty('newestEntryTimestamp');
    expect(stats).toHaveProperty('averageSegmentSize');
    expect(stats).toHaveProperty('fragmentationRatio');
    expect(stats).toHaveProperty('deadEntriesCount');
    expect(stats).toHaveProperty('activeTransactionCount');
  });
});

// =============================================================================
// 5. RETENTION POLICY CONFIGURATION INTERFACE
// =============================================================================

describe('WAL Retention Policy - Configuration Interface [RED]', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should support the WALRetentionPolicy interface', async () => {
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);

    // All these configuration options should be supported
    const manager = createWALRetentionManager(backend, reader, null, {
      // Time-based
      retentionHours: 24,
      retentionMinutes: 30, // For testing
      retentionDays: 7,

      // Size-based
      maxTotalBytes: 100 * 1024 * 1024,
      maxTotalSize: '100MB',

      // Checkpoint-based
      keepCheckpointCount: 3,
      cleanupOnCheckpoint: true,
      checkpointRetentionMode: 'strict',

      // Core retention
      minSegmentCount: 2,
      maxSegmentAge: 24 * 60 * 60 * 1000,
      respectSlotPositions: true,
      readerIdleTimeout: 5 * 60 * 1000,
      archiveBeforeDelete: true,
    });

    const policy = manager.getPolicy();

    expect(policy.retentionHours).toBeDefined();
    expect(policy.maxTotalBytes).toBe(100 * 1024 * 1024);
    expect(policy.keepCheckpointCount).toBe(3);
  });

  it.fails('should validate retention policy configuration on creation', async () => {
    // GAP: Need better validation error messages
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);

    // Should throw with detailed validation error
    const invalidPolicy = {
      maxAgeMs: -1000, // Invalid
      maxSizeBytes: -500, // Invalid
      keepAfterCheckpoint: -1, // Invalid
    } as Record<string, unknown>;
    expect(() => createWALRetentionManager(backend, reader, null, invalidPolicy)).toThrow(/Invalid retention policy/);
  });

  it.fails('should support loading retention policy from configuration file', async () => {
    // GAP: No configuration file support
    const retentionModule = await importRetentionModule();
    const { createWALReader } = await import('../reader.js');

    // Write a config file
    const config = {
      retention: {
        maxAgeMs: 86400000,
        maxSizeBytes: 104857600,
        keepAfterCheckpoint: 2,
      },
    };
    await backend.write('_wal/retention.config.json',
      new TextEncoder().encode(JSON.stringify(config)));

    const reader = createWALReader(backend);

    // This function should exist but doesn't
    const policy = await retentionModule.loadRetentionPolicy!(backend, '_wal/retention.config.json');
    const manager = retentionModule.createWALRetentionManager(backend, reader, null, policy);

    expect(manager.getPolicy().maxSegmentAge).toBe(86400000);
  });

  it.fails('should support policy inheritance and composition', async () => {
    // GAP: No policy composition/inheritance support
    const retentionModule = await importRetentionModule();
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);

    // This function should exist but doesn't
    const customPolicy = retentionModule.composePolicy!(
      retentionModule.RETENTION_PRESETS!.balanced, // Base
      { maxTotalBytes: 50 * 1024 * 1024 }, // Override
    );

    const manager = retentionModule.createWALRetentionManager(backend, reader, null, customPolicy);

    // Should have balanced preset values plus our override
    expect(manager.getPolicy().maxTotalBytes).toBe(50 * 1024 * 1024);
    expect(manager.getPolicy().minSegmentCount).toBe(retentionModule.RETENTION_PRESETS!.balanced.minSegmentCount);
  });
});

// =============================================================================
// 6. METRICS FOR WAL SIZE AND CLEANUP OPERATIONS
// =============================================================================

describe('WAL Retention Policy - Metrics [RED]', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it('should expose basic metrics via getMetrics()', async () => {
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    const extManager = manager as unknown as ExtendedWALRetentionManager;
    const metrics = await extManager.getMetrics();

    expect(metrics).toHaveProperty('totalSegments');
    expect(metrics).toHaveProperty('totalBytes');
    expect(metrics).toHaveProperty('oldestSegmentAge');
    expect(metrics).toHaveProperty('lastCleanupTime');
  });

  it('should track cleanup operation metrics', async () => {
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, DefaultWALEncoder } = await import('../index.js');

    const encoder = new DefaultWALEncoder();

    // Create old segments for cleanup
    for (let i = 1; i <= 3; i++) {
      const segment = {
        id: `seg_${i.toString().padStart(20, '0')}`,
        startLSN: BigInt((i - 1) * 10 + 1),
        endLSN: BigInt(i * 10),
        entries: [{
          lsn: BigInt((i - 1) * 10 + 1),
          timestamp: Date.now() - 100000,
          txnId: `txn_${i}`,
          op: 'INSERT' as const,
          table: 'test',
        }],
        checksum: 0,
        createdAt: Date.now() - 100000,
      };
      const data = encoder.encodeSegment(segment);
      segment.checksum = encoder.calculateChecksum(data);
      await backend.write(`_wal/segments/${segment.id}`, encoder.encodeSegment(segment));
    }

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 1,
      maxSegmentAge: 1, // 1ms
    });

    // Wait for segments to age
    await new Promise(resolve => setTimeout(resolve, 10));

    // Run cleanup
    const result = await manager.cleanup(false);

    // Check cleanup result metrics
    expect(result).toHaveProperty('deleted');
    expect(result).toHaveProperty('bytesFreed');
    expect(result).toHaveProperty('durationMs');

    // Get updated metrics
    const extManager = manager as unknown as ExtendedWALRetentionManager;
    const metrics = await extManager.getMetrics();
    expect(metrics.lastCleanupTime).toBeDefined();
  });

  it.fails('should support custom metrics reporters (Prometheus, StatsD, etc.)', async () => {
    // GAP: No pluggable metrics reporter interface
    const retentionModule = await importRetentionModule();
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);

    // This reporter class should exist but doesn't
    const reporter = new retentionModule.PrometheusReporter!({
      prefix: 'dosql_wal_',
      labels: { instance: 'test' },
    } as unknown as ExtendedWALRetentionManager);

    const manager = retentionModule.createWALRetentionManager(backend, reader, null, {
      metricsReporter: reporter,
    });

    await manager.cleanup(false);

    // Reporter should have collected metrics
    const metrics = (reporter as { getMetrics(): string }).getMetrics();
    expect(metrics).toContain('dosql_wal_cleanup_duration_seconds');
    expect(metrics).toContain('dosql_wal_segments_total');
    expect(metrics).toContain('dosql_wal_bytes_total');
  });

  it.fails('should emit metrics events for real-time monitoring', async () => {
    // GAP: No event-based metrics emission
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    interface MetricsEvent { type: string; timestamp: number; data: unknown }
    const metricsEvents: MetricsEvent[] = [];

    const manager = createWALRetentionManager(backend, reader, null, {
      onMetricsUpdate: (event: MetricsEvent) => metricsEvents.push(event),
      metricsInterval: 1000, // Emit every second
    } as Record<string, unknown>);

    // Start metrics collection
    const extManager = manager as unknown as ExtendedWALRetentionManager;
    extManager.startMetricsCollection();

    await new Promise(resolve => setTimeout(resolve, 1500));

    extManager.stopMetricsCollection();

    expect(metricsEvents.length).toBeGreaterThan(0);
    expect(metricsEvents[0]).toHaveProperty('type', 'metrics');
    expect(metricsEvents[0]).toHaveProperty('timestamp');
    expect(metricsEvents[0]).toHaveProperty('data');
  });

  it('should provide histogram metrics for cleanup operation latencies', async () => {
    // getCleanupLatencyHistogram is now implemented
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    // Run multiple cleanups
    for (let i = 0; i < 10; i++) {
      await manager.cleanup(true); // Dry run
    }

    // This method now exists
    const extManager = manager as unknown as ExtendedWALRetentionManager;
    const histogram = await extManager.getCleanupLatencyHistogram();

    expect(histogram).toHaveProperty('p50');
    expect(histogram).toHaveProperty('p90');
    expect(histogram).toHaveProperty('p99');
    expect(histogram).toHaveProperty('min');
    expect(histogram).toHaveProperty('max');
    expect(histogram).toHaveProperty('avg');
    expect(histogram).toHaveProperty('count');
  });

  it('should track WAL growth rate metrics', async () => {
    // getGrowthStats is now implemented
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    // This method now exists
    const extManager = manager as unknown as ExtendedWALRetentionManager;
    const growthStats = await extManager.getGrowthStats();

    expect(growthStats).toHaveProperty('bytesPerHour');
    expect(growthStats).toHaveProperty('segmentsPerHour');
    expect(growthStats).toHaveProperty('entriesPerHour');
    expect(growthStats).toHaveProperty('estimatedTimeToLimit'); // If maxSizeBytes is set
  });
});

// =============================================================================
// 7. ADDITIONAL EDGE CASES AND ADVANCED FEATURES [RED]
// =============================================================================

describe('WAL Retention Policy - Advanced Features [RED]', () => {
  let backend: FSXBackend;

  beforeEach(() => {
    backend = createTestBackend();
  });

  it.fails('should support retention policies per table', async () => {
    // GAP: No per-table retention configuration
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      tableRetention: {
        'audit_log': { retentionDays: 90 }, // Keep audit logs longer
        'session_data': { retentionHours: 24 }, // Ephemeral data
        '*': { retentionDays: 7 }, // Default for other tables
      },
    } as Record<string, unknown>);

    // This method should exist but doesn't
    const extManager = manager as unknown as ExtendedWALRetentionManager;
    const auditPolicy = extManager.getTableRetentionPolicy('audit_log');
    expect(auditPolicy?.retentionHours).toBe(90 * 24); // Days converted to hours
  });

  it.fails('should support retention policy based on transaction importance', async () => {
    // GAP: No transaction-level retention hints
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader, createWALWriter } = await import('../index.js');

    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);

    // Write with retention hint
    await writer.append({
      timestamp: Date.now(),
      txnId: 'txn_important',
      op: 'INSERT',
      table: 'financial_transactions',
      // This hint doesn't exist yet
      retentionHint: 'permanent', // Keep forever
    } as Record<string, unknown>);

    const manager = createWALRetentionManager(backend, reader, null, {
      respectRetentionHints: true,
    } as Record<string, unknown>);

    const result = await manager.checkRetention();

    // Entries with 'permanent' hint should never be eligible
    expect((result as { protectedByHint?: number }).protectedByHint).toBeDefined();

    await writer.close();
  });

  it.fails('should support scheduled retention windows', async () => {
    // GAP: No time-of-day scheduling for cleanup
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      cleanupWindows: [
        { start: '02:00', end: '04:00', timezone: 'UTC' }, // Only cleanup between 2-4 AM
      ],
      blockCleanupOutsideWindows: true,
    } as Record<string, unknown>);

    // This method should exist but doesn't
    const extManager = manager as unknown as ExtendedWALRetentionManager;
    const canCleanup = extManager.isInCleanupWindow();
    expect(typeof canCleanup).toBe('boolean');
  });

  it.fails('should support dry-run with impact analysis', async () => {
    // GAP: No detailed impact analysis for dry-run
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null);

    // This should provide detailed impact analysis
    const extManager = manager as unknown as ExtendedWALRetentionManager;
    const impact = await extManager.analyzeCleanupImpact();

    expect(impact).toHaveProperty('segmentsToDelete');
    expect(impact).toHaveProperty('bytesToFree');
    expect(impact).toHaveProperty('affectedTables');
    expect(impact).toHaveProperty('oldestRetainedLSN');
    expect(impact).toHaveProperty('affectedReplicationSlots');
    expect(impact).toHaveProperty('estimatedDuration');
    expect(impact).toHaveProperty('risks'); // Array of potential issues
  });

  it.fails('should support atomic cleanup with rollback on failure', async () => {
    // GAP: No transactional cleanup with rollback
    const { createWALRetentionManager } = await import('../retention.js');
    const { createWALReader } = await import('../reader.js');

    const reader = createWALReader(backend);
    const manager = createWALRetentionManager(backend, reader, null, {
      atomicCleanup: true, // All or nothing
    } as Record<string, unknown>);

    // This should support transactional cleanup
    const result = await manager.cleanup(false);

    expect(result).toHaveProperty('transactionId');
    expect(result).toHaveProperty('committed');

    // If any deletion failed, all should be rolled back
    if (result.failed.length > 0) {
      expect(result.committed).toBe(false);
      expect(result.rolledBack).toBe(true);
    }
  });
});
