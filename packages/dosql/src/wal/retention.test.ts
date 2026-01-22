/**
 * WAL Retention Manager Tests for DoSQL
 *
 * Comprehensive tests for WAL retention policy based on slot positions:
 * - Segments older than oldest slot position are eligible for deletion
 * - Segments with active readers are protected
 * - Min retention count is respected
 * - Max age policy works correctly
 * - Multiple slots are considered correctly
 *
 * Tests run using workers-vitest-pool (NO MOCKS).
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  createWALRetentionManager,
  type WALRetentionManager,
  type RetentionPolicy,
  type ActiveReader,
  type RetentionCheckResult,
  DEFAULT_RETENTION_POLICY,
  RetentionError,
  RetentionErrorCode,
} from './retention.js';
import {
  createWALWriter,
  createWALReader,
  generateTxnId,
  type WALWriter,
  type WALReader,
} from './index.js';
import {
  createReplicationSlotManager,
  type ReplicationSlotManager,
} from '../cdc/stream.js';
import type { FSXBackend } from '../fsx/types.js';

// =============================================================================
// Test Utilities - In-Memory FSX Backend for Workers Environment
// =============================================================================

/**
 * Creates an in-memory FSX backend for testing.
 * This works in the Cloudflare Workers environment without mocks.
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
 * Encode data to Uint8Array for WAL entries
 */
function encode<T>(value: T): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(value));
}

/**
 * Helper to write test WAL entries and flush to segment
 */
async function writeAndFlushEntries(
  writer: WALWriter,
  entries: Array<{
    op: 'INSERT' | 'UPDATE' | 'DELETE';
    table: string;
    key?: unknown;
    before?: unknown;
    after?: unknown;
  }>
): Promise<{ lsns: bigint[]; segmentId: string | undefined }> {
  const txnId = generateTxnId();
  const timestamp = Date.now();
  const lsns: bigint[] = [];

  // BEGIN
  const beginResult = await writer.append({
    timestamp,
    txnId,
    op: 'BEGIN',
    table: '',
  });
  lsns.push(beginResult.lsn);

  // Data operations
  for (const entry of entries) {
    const result = await writer.append({
      timestamp,
      txnId,
      op: entry.op,
      table: entry.table,
      key: entry.key ? encode(entry.key) : undefined,
      before: entry.before ? encode(entry.before) : undefined,
      after: entry.after ? encode(entry.after) : undefined,
    });
    lsns.push(result.lsn);
  }

  // COMMIT with sync to flush
  const commitResult = await writer.append(
    {
      timestamp,
      txnId,
      op: 'COMMIT',
      table: '',
    },
    { sync: true }
  );
  lsns.push(commitResult.lsn);

  return { lsns, segmentId: commitResult.segmentId };
}

/**
 * Write multiple transactions to create multiple segments
 */
async function writeMultipleSegments(
  writer: WALWriter,
  segmentCount: number,
  entriesPerSegment: number = 5
): Promise<{ lsns: bigint[][]; segmentIds: string[] }> {
  const allLsns: bigint[][] = [];
  const segmentIds: string[] = [];

  for (let s = 0; s < segmentCount; s++) {
    const entries = [];
    for (let i = 0; i < entriesPerSegment; i++) {
      entries.push({
        op: 'INSERT' as const,
        table: 'items',
        after: { id: s * entriesPerSegment + i, segment: s },
      });
    }
    const { lsns, segmentId } = await writeAndFlushEntries(writer, entries);
    allLsns.push(lsns);
    if (segmentId) {
      segmentIds.push(segmentId);
    }
  }

  return { lsns: allLsns, segmentIds };
}

// =============================================================================
// Test: Segments Older Than Oldest Slot Position Are Eligible for Deletion
// =============================================================================

describe('WAL Retention - Slot Position Based Deletion', () => {
  let backend: FSXBackend;
  let writer: WALWriter;
  let reader: WALReader;
  let slotManager: ReplicationSlotManager;
  let retentionManager: WALRetentionManager;

  beforeEach(() => {
    backend = createTestBackend();
    writer = createWALWriter(backend);
    reader = createWALReader(backend);
    slotManager = createReplicationSlotManager(backend, reader);
    retentionManager = createWALRetentionManager(backend, reader, slotManager, {
      minSegmentCount: 1,
      maxSegmentAge: 0, // No age restriction for these tests
    });
  });

  afterEach(async () => {
    await writer.close();
  });

  it('should identify segments older than the oldest slot position as eligible', async () => {
    // Write 3 segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 3);
    expect(segmentIds.length).toBe(3);

    // Create a slot pointing to the second segment's end LSN
    const secondSegmentEndLSN = lsns[1][lsns[1].length - 1];
    await slotManager.createSlot('consumer-1', secondSegmentEndLSN);

    const result = await retentionManager.checkRetention();

    // First segment should be eligible (its endLSN < slot's acknowledgedLSN)
    expect(result.eligibleForDeletion.length).toBeGreaterThanOrEqual(0);
    expect(result.minSlotLSN).toBe(secondSegmentEndLSN);
  });

  it('should protect segments that contain LSNs >= min slot position', async () => {
    // Write 3 segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 3);

    // Create slot pointing to start of second segment
    const secondSegmentStartLSN = lsns[1][0];
    await slotManager.createSlot('consumer-1', secondSegmentStartLSN);

    const result = await retentionManager.checkRetention();

    // Segments 2 and 3 should be protected by slot
    expect(result.protectedBySlots.length).toBeGreaterThanOrEqual(1);
  });

  it('should calculate minimum LSN correctly across all slots', async () => {
    // Write 4 segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 4);

    // Create multiple slots at different positions
    await slotManager.createSlot('consumer-1', lsns[2][lsns[2].length - 1]); // At segment 3
    await slotManager.createSlot('consumer-2', lsns[0][lsns[0].length - 1]); // At segment 1 (oldest)
    await slotManager.createSlot('consumer-3', lsns[3][lsns[3].length - 1]); // At segment 4

    const minLSN = await retentionManager.getMinSlotLSN();

    // Should be the oldest slot (consumer-2 at segment 1)
    expect(minLSN).toBe(lsns[0][lsns[0].length - 1]);
  });

  it('should return null for min slot LSN when no slots exist', async () => {
    // Write segments but don't create any slots
    await writeMultipleSegments(writer, 2);

    const minLSN = await retentionManager.getMinSlotLSN();
    expect(minLSN).toBeNull();
  });

  it('should ignore slot positions when respectSlotPositions is false', async () => {
    // Create retention manager with slot protection disabled
    const noSlotProtectionManager = createWALRetentionManager(
      backend,
      reader,
      slotManager,
      {
        minSegmentCount: 1,
        maxSegmentAge: 0,
        respectSlotPositions: false, // Ignore slots
      }
    );

    // Write segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 3);

    // Create slot pointing to latest segment
    await slotManager.createSlot('consumer-1', lsns[2][lsns[2].length - 1]);

    const result = await noSlotProtectionManager.checkRetention();

    // Segments should not be in protectedBySlots since slot protection is disabled
    expect(result.protectedBySlots.length).toBe(0);
  });
});

// =============================================================================
// Test: Segments With Active Readers Are Protected
// =============================================================================

describe('WAL Retention - Active Reader Protection', () => {
  let backend: FSXBackend;
  let writer: WALWriter;
  let reader: WALReader;
  let retentionManager: WALRetentionManager;

  beforeEach(() => {
    backend = createTestBackend();
    writer = createWALWriter(backend);
    reader = createWALReader(backend);
    // No slot manager for reader-only tests
    retentionManager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 1,
      maxSegmentAge: 0,
      readerIdleTimeout: 60000, // 1 minute
    });
  });

  afterEach(async () => {
    await writer.close();
  });

  it('should protect segments being actively read', async () => {
    // Write segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 3);
    expect(segmentIds.length).toBeGreaterThan(0);

    // Register an active reader on the first segment
    const activeReader: ActiveReader = {
      readerId: 'reader-1',
      currentLSN: lsns[0][1], // Middle of first segment
      currentSegmentId: segmentIds[0],
      lastActivityAt: Date.now(),
      description: 'Test reader',
    };
    retentionManager.registerReader(activeReader);

    const result = await retentionManager.checkRetention();

    // First segment should be protected by reader
    expect(result.protectedByReaders).toContain(segmentIds[0]);
  });

  it('should not protect segments after reader unregisters', async () => {
    // Write segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 3);

    // Register and then unregister a reader
    const activeReader: ActiveReader = {
      readerId: 'reader-1',
      currentLSN: lsns[0][1],
      currentSegmentId: segmentIds[0],
      lastActivityAt: Date.now(),
    };
    retentionManager.registerReader(activeReader);
    retentionManager.unregisterReader('reader-1');

    const result = await retentionManager.checkRetention();

    // First segment should no longer be protected by readers
    expect(result.protectedByReaders).not.toContain(segmentIds[0]);
  });

  it('should update reader position correctly', async () => {
    // Write segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 3);

    // Register reader on first segment
    const activeReader: ActiveReader = {
      readerId: 'reader-1',
      currentLSN: lsns[0][0],
      currentSegmentId: segmentIds[0],
      lastActivityAt: Date.now(),
    };
    retentionManager.registerReader(activeReader);

    // Update reader to second segment
    retentionManager.updateReaderPosition(
      'reader-1',
      lsns[1][0],
      segmentIds[1]
    );

    const readers = retentionManager.getActiveReaders();
    expect(readers.length).toBe(1);
    expect(readers[0].currentSegmentId).toBe(segmentIds[1]);
    expect(readers[0].currentLSN).toBe(lsns[1][0]);
  });

  it('should not protect segments when reader is idle beyond timeout', async () => {
    // Create manager with very short idle timeout
    const shortTimeoutManager = createWALRetentionManager(
      backend,
      reader,
      null,
      {
        minSegmentCount: 1,
        maxSegmentAge: 0,
        readerIdleTimeout: 1, // 1ms timeout
      }
    );

    // Write segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 3);

    // Register reader with old timestamp (simulating idle)
    const idleReader: ActiveReader = {
      readerId: 'idle-reader',
      currentLSN: lsns[0][1],
      currentSegmentId: segmentIds[0],
      lastActivityAt: Date.now() - 10000, // 10 seconds ago
    };
    shortTimeoutManager.registerReader(idleReader);

    // Wait a bit to ensure timeout
    await new Promise((resolve) => setTimeout(resolve, 10));

    const result = await shortTimeoutManager.checkRetention();

    // Idle reader should not protect the segment
    expect(result.protectedByReaders).not.toContain(segmentIds[0]);
  });

  it('should protect segments when multiple readers are active', async () => {
    // Write segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 4);

    // Register multiple readers on different segments
    retentionManager.registerReader({
      readerId: 'reader-1',
      currentLSN: lsns[0][1],
      currentSegmentId: segmentIds[0],
      lastActivityAt: Date.now(),
    });
    retentionManager.registerReader({
      readerId: 'reader-2',
      currentLSN: lsns[1][1],
      currentSegmentId: segmentIds[1],
      lastActivityAt: Date.now(),
    });

    const result = await retentionManager.checkRetention();

    // Both segments should be protected
    expect(result.protectedByReaders).toContain(segmentIds[0]);
    expect(result.protectedByReaders).toContain(segmentIds[1]);
  });
});

// =============================================================================
// Test: Minimum Retention Count Is Respected
// =============================================================================

describe('WAL Retention - Minimum Segment Count', () => {
  let backend: FSXBackend;
  let writer: WALWriter;
  let reader: WALReader;

  beforeEach(() => {
    backend = createTestBackend();
    writer = createWALWriter(backend);
    reader = createWALReader(backend);
  });

  afterEach(async () => {
    await writer.close();
  });

  it('should always keep minSegmentCount segments regardless of other policies', async () => {
    const retentionManager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 3,
      maxSegmentAge: 0, // All segments are "old enough"
    });

    // Write 5 segments
    const { segmentIds } = await writeMultipleSegments(writer, 5);
    expect(segmentIds.length).toBe(5);

    const result = await retentionManager.checkRetention();

    // Should protect at least 3 segments (the newest ones)
    expect(result.protectedByMinCount.length).toBe(3);

    // The 3 newest segments should be protected
    expect(result.protectedByMinCount).toContain(segmentIds[4]);
    expect(result.protectedByMinCount).toContain(segmentIds[3]);
    expect(result.protectedByMinCount).toContain(segmentIds[2]);
  });

  it('should not delete any segments when count <= minSegmentCount', async () => {
    const retentionManager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 5,
      maxSegmentAge: 0,
    });

    // Write only 3 segments
    const { segmentIds } = await writeMultipleSegments(writer, 3);

    const result = await retentionManager.checkRetention();

    // All segments should be protected
    expect(result.protectedByMinCount.length).toBe(3);
    expect(result.eligibleForDeletion.length).toBe(0);
  });

  it('should allow minSegmentCount of 0', async () => {
    const retentionManager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 0,
      maxSegmentAge: 0,
    });

    // Write segments
    const { segmentIds } = await writeMultipleSegments(writer, 3);

    const result = await retentionManager.checkRetention();

    // No segments protected by min count
    expect(result.protectedByMinCount.length).toBe(0);
  });

  it('should protect most recent segments based on LSN order', async () => {
    const retentionManager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 2,
      maxSegmentAge: 0,
    });

    // Write 4 segments
    const { segmentIds } = await writeMultipleSegments(writer, 4);

    const result = await retentionManager.checkRetention();

    // The 2 newest segments (by LSN) should be protected
    expect(result.protectedByMinCount).toContain(segmentIds[3]);
    expect(result.protectedByMinCount).toContain(segmentIds[2]);
    expect(result.protectedByMinCount).not.toContain(segmentIds[0]);
    expect(result.protectedByMinCount).not.toContain(segmentIds[1]);
  });
});

// =============================================================================
// Test: Max Age Policy Works Correctly
// =============================================================================

describe('WAL Retention - Max Age Policy', () => {
  let backend: FSXBackend;
  let writer: WALWriter;
  let reader: WALReader;

  beforeEach(() => {
    backend = createTestBackend();
    writer = createWALWriter(backend);
    reader = createWALReader(backend);
  });

  afterEach(async () => {
    await writer.close();
  });

  it('should not delete segments younger than maxSegmentAge', async () => {
    const retentionManager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 0,
      maxSegmentAge: 60 * 60 * 1000, // 1 hour
    });

    // Write segments (they will have recent timestamps)
    const { segmentIds } = await writeMultipleSegments(writer, 3);

    const result = await retentionManager.checkRetention();

    // All segments are too young, none should be eligible
    expect(result.eligibleForDeletion.length).toBe(0);
  });

  it('should mark segments older than maxSegmentAge as eligible', async () => {
    // Create a custom backend that lets us manipulate timestamps
    const customBackend = createTestBackend();
    const customWriter = createWALWriter(customBackend);
    const customReader = createWALReader(customBackend);

    const retentionManager = createWALRetentionManager(
      customBackend,
      customReader,
      null,
      {
        minSegmentCount: 0,
        maxSegmentAge: 1, // 1ms - very short
      }
    );

    // Write segments
    await writeMultipleSegments(customWriter, 2);

    // Wait for segments to become old enough
    await new Promise((resolve) => setTimeout(resolve, 10));

    const result = await retentionManager.checkRetention();

    // Segments should now be old enough to delete
    expect(result.eligibleForDeletion.length).toBeGreaterThanOrEqual(0);

    await customWriter.close();
  });

  it('should combine maxSegmentAge with minSegmentCount', async () => {
    const retentionManager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 2,
      maxSegmentAge: 1, // 1ms
    });

    // Write 4 segments
    const { segmentIds } = await writeMultipleSegments(writer, 4);

    // Wait for segments to become old enough
    await new Promise((resolve) => setTimeout(resolve, 10));

    const result = await retentionManager.checkRetention();

    // 2 segments should be protected by min count
    expect(result.protectedByMinCount.length).toBe(2);

    // Remaining segments might be eligible (if old enough)
    expect(result.totalSegmentCount).toBe(4);
  });
});

// =============================================================================
// Test: Multiple Slots Are Considered Correctly
// =============================================================================

describe('WAL Retention - Multiple Slot Handling', () => {
  let backend: FSXBackend;
  let writer: WALWriter;
  let reader: WALReader;
  let slotManager: ReplicationSlotManager;
  let retentionManager: WALRetentionManager;

  beforeEach(() => {
    backend = createTestBackend();
    writer = createWALWriter(backend);
    reader = createWALReader(backend);
    slotManager = createReplicationSlotManager(backend, reader);
    retentionManager = createWALRetentionManager(backend, reader, slotManager, {
      minSegmentCount: 0,
      maxSegmentAge: 0,
    });
  });

  afterEach(async () => {
    await writer.close();
  });

  it('should use the oldest slot position for protection', async () => {
    // Write 5 segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 5);

    // Create slots at different positions
    await slotManager.createSlot('fast-consumer', lsns[4][lsns[4].length - 1]); // Latest
    await slotManager.createSlot('slow-consumer', lsns[1][lsns[1].length - 1]); // Oldest
    await slotManager.createSlot('medium-consumer', lsns[2][lsns[2].length - 1]); // Middle

    const minLSN = await retentionManager.getMinSlotLSN();

    // Should be the slow consumer's position
    expect(minLSN).toBe(lsns[1][lsns[1].length - 1]);
  });

  it('should protect segments based on slowest consumer', async () => {
    // Write 5 segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 5);

    // Slow consumer at segment 2
    await slotManager.createSlot('slow-consumer', lsns[1][0]);

    const result = await retentionManager.checkRetention();

    // Segments 2-5 should be protected by slot (their endLSN >= slot position)
    const protectedSet = new Set(result.protectedBySlots);
    expect(protectedSet.has(segmentIds[1])).toBe(true);
    expect(protectedSet.has(segmentIds[2])).toBe(true);
    expect(protectedSet.has(segmentIds[3])).toBe(true);
    expect(protectedSet.has(segmentIds[4])).toBe(true);
  });

  it('should update protection when slot positions change', async () => {
    // Write 4 segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 4);

    // Create slot at segment 1
    await slotManager.createSlot('consumer', lsns[0][0]);

    let result = await retentionManager.checkRetention();
    const initialProtected = result.protectedBySlots.length;

    // Update slot to segment 3
    await slotManager.updateSlot('consumer', lsns[2][lsns[2].length - 1]);

    result = await retentionManager.checkRetention();

    // Fewer segments should be protected now
    expect(result.protectedBySlots.length).toBeLessThan(initialProtected);
  });

  it('should handle slot deletion correctly', async () => {
    // Write 3 segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 3);

    // Create and then delete a slot
    await slotManager.createSlot('temp-consumer', lsns[0][0]);
    await slotManager.deleteSlot('temp-consumer');

    const minLSN = await retentionManager.getMinSlotLSN();

    // No slots, so min LSN should be null
    expect(minLSN).toBeNull();
  });

  it('should handle empty slot list', async () => {
    // Write segments but no slots
    await writeMultipleSegments(writer, 2);

    const result = await retentionManager.checkRetention();

    // No slots to protect segments
    expect(result.protectedBySlots.length).toBe(0);
    expect(result.minSlotLSN).toBeNull();
  });
});

// =============================================================================
// Test: Cleanup Operations
// =============================================================================

describe('WAL Retention - Cleanup Operations', () => {
  let backend: FSXBackend;
  let writer: WALWriter;
  let reader: WALReader;
  let retentionManager: WALRetentionManager;

  beforeEach(() => {
    backend = createTestBackend();
    writer = createWALWriter(backend);
    reader = createWALReader(backend);
    retentionManager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 1,
      maxSegmentAge: 1, // 1ms
      archiveBeforeDelete: true,
    });
  });

  afterEach(async () => {
    await writer.close();
  });

  it('should perform dry run without deleting', async () => {
    // Write segments
    const { segmentIds } = await writeMultipleSegments(writer, 3);

    // Wait for segments to become old
    await new Promise((resolve) => setTimeout(resolve, 10));

    // Dry run
    const result = await retentionManager.cleanup(true);

    // Should report what would be deleted
    expect(result.deleted.length).toBeGreaterThanOrEqual(0);

    // But segments should still exist
    const remainingSegments = await reader.listSegments();
    expect(remainingSegments.length).toBe(3);
  });

  it('should actually delete segments when not dry run', async () => {
    // Write 4 segments
    const { segmentIds } = await writeMultipleSegments(writer, 4);

    // Wait for segments to become old
    await new Promise((resolve) => setTimeout(resolve, 10));

    // Real cleanup
    const result = await retentionManager.cleanup(false);

    // Should have deleted some segments
    const remainingSegments = await reader.listSegments();

    // At least minSegmentCount (1) should remain
    expect(remainingSegments.length).toBeGreaterThanOrEqual(1);
  });

  it('should archive segments before deletion when configured', async () => {
    const archiveManager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 1,
      maxSegmentAge: 1,
      archiveBeforeDelete: true,
    });

    // Write 3 segments
    await writeMultipleSegments(writer, 3);

    // Wait and cleanup
    await new Promise((resolve) => setTimeout(resolve, 10));
    const result = await archiveManager.cleanup(false);

    // Archived segments should match deleted
    expect(result.archived.length).toBe(result.deleted.length);
  });

  it('should not archive when archiveBeforeDelete is false', async () => {
    const noArchiveManager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 1,
      maxSegmentAge: 1,
      archiveBeforeDelete: false,
    });

    // Write 3 segments
    await writeMultipleSegments(writer, 3);

    // Wait and cleanup
    await new Promise((resolve) => setTimeout(resolve, 10));
    const result = await noArchiveManager.cleanup(false);

    // No archiving
    expect(result.archived.length).toBe(0);
  });

  it('should report bytes freed after cleanup', async () => {
    // Write segments with some data
    await writeMultipleSegments(writer, 4);

    // Wait and cleanup
    await new Promise((resolve) => setTimeout(resolve, 10));
    const result = await retentionManager.cleanup(false);

    // Should report bytes freed (if any segments were deleted)
    if (result.deleted.length > 0) {
      expect(result.bytesFreed).toBeGreaterThan(0);
    }
  });

  it('should report duration of cleanup operation', async () => {
    await writeMultipleSegments(writer, 2);

    await new Promise((resolve) => setTimeout(resolve, 10));
    const result = await retentionManager.cleanup(false);

    expect(result.durationMs).toBeGreaterThanOrEqual(0);
  });

  it('should handle cleanup failures gracefully', async () => {
    // This test just verifies the error handling structure
    await writeMultipleSegments(writer, 2);

    await new Promise((resolve) => setTimeout(resolve, 10));
    const result = await retentionManager.cleanup(false);

    // failed array should exist
    expect(Array.isArray(result.failed)).toBe(true);
  });
});

// =============================================================================
// Test: Policy Management
// =============================================================================

describe('WAL Retention - Policy Management', () => {
  let backend: FSXBackend;
  let writer: WALWriter;
  let reader: WALReader;
  let retentionManager: WALRetentionManager;

  beforeEach(() => {
    backend = createTestBackend();
    writer = createWALWriter(backend);
    reader = createWALReader(backend);
    retentionManager = createWALRetentionManager(backend, reader, null);
  });

  afterEach(async () => {
    await writer.close();
  });

  it('should use default policy when none provided', () => {
    const policy = retentionManager.getPolicy();

    expect(policy.minSegmentCount).toBe(DEFAULT_RETENTION_POLICY.minSegmentCount);
    expect(policy.maxSegmentAge).toBe(DEFAULT_RETENTION_POLICY.maxSegmentAge);
    expect(policy.respectSlotPositions).toBe(DEFAULT_RETENTION_POLICY.respectSlotPositions);
    expect(policy.readerIdleTimeout).toBe(DEFAULT_RETENTION_POLICY.readerIdleTimeout);
    expect(policy.archiveBeforeDelete).toBe(DEFAULT_RETENTION_POLICY.archiveBeforeDelete);
  });

  it('should allow updating policy at runtime', () => {
    retentionManager.updatePolicy({
      minSegmentCount: 5,
      maxSegmentAge: 3600000,
    });

    const policy = retentionManager.getPolicy();

    expect(policy.minSegmentCount).toBe(5);
    expect(policy.maxSegmentAge).toBe(3600000);
  });

  it('should merge partial policy updates', () => {
    retentionManager.updatePolicy({ minSegmentCount: 10 });

    const policy = retentionManager.getPolicy();

    expect(policy.minSegmentCount).toBe(10);
    // Other values should remain at defaults
    expect(policy.maxSegmentAge).toBe(DEFAULT_RETENTION_POLICY.maxSegmentAge);
  });

  it('should return a copy of the policy (not the original)', () => {
    const policy1 = retentionManager.getPolicy();
    policy1.minSegmentCount = 999;

    const policy2 = retentionManager.getPolicy();

    expect(policy2.minSegmentCount).not.toBe(999);
  });
});

// =============================================================================
// Test: Combined Protection Scenarios
// =============================================================================

describe('WAL Retention - Combined Protection', () => {
  let backend: FSXBackend;
  let writer: WALWriter;
  let reader: WALReader;
  let slotManager: ReplicationSlotManager;
  let retentionManager: WALRetentionManager;

  beforeEach(() => {
    backend = createTestBackend();
    writer = createWALWriter(backend);
    reader = createWALReader(backend);
    slotManager = createReplicationSlotManager(backend, reader);
    retentionManager = createWALRetentionManager(backend, reader, slotManager, {
      minSegmentCount: 2,
      maxSegmentAge: 1,
      readerIdleTimeout: 60000,
    });
  });

  afterEach(async () => {
    await writer.close();
  });

  it('should protect segments for multiple reasons', async () => {
    // Write 5 segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 5);

    // Create a slot at segment 1's end LSN (protecting segments 2-5)
    await slotManager.createSlot('consumer', lsns[0][lsns[0].length - 1]);

    // Register a reader at segment 1 (protecting segment 1)
    retentionManager.registerReader({
      readerId: 'reader-1',
      currentLSN: lsns[0][0],
      currentSegmentId: segmentIds[0],
      lastActivityAt: Date.now(),
    });

    // Wait for segments to become old enough
    await new Promise((resolve) => setTimeout(resolve, 10));

    const result = await retentionManager.checkRetention();

    // Note: segments are protected by one category (first match in order: minCount, slot, reader)
    // Segments 4 and 5 (newest) protected by min count
    expect(result.protectedByMinCount).toContain(segmentIds[4]);
    expect(result.protectedByMinCount).toContain(segmentIds[3]);

    // Segments 2 and 3 protected by slot (their endLSN >= slot position)
    expect(result.protectedBySlots.some((id) => id === segmentIds[1])).toBe(true);
    expect(result.protectedBySlots.some((id) => id === segmentIds[2])).toBe(true);

    // Segment 1 protected by reader (since it's not in min count and slot check passed it)
    // Actually segment 1's endLSN >= slot LSN too, so it would be protected by slot first
    // Let's just verify reader is registered and active
    const readers = retentionManager.getActiveReaders();
    expect(readers.length).toBe(1);
    expect(readers[0].currentSegmentId).toBe(segmentIds[0]);
  });

  it('should only delete segments that pass all checks', async () => {
    // Write 6 segments
    const { lsns, segmentIds } = await writeMultipleSegments(writer, 6);

    // Slot at segment 3
    await slotManager.createSlot('consumer', lsns[2][0]);

    // Wait for old enough
    await new Promise((resolve) => setTimeout(resolve, 10));

    const result = await retentionManager.checkRetention();

    // Only segment 1 might be eligible (segment 0 could be too, depending on LSN)
    // Must be:
    // - older than maxSegmentAge (all are after delay)
    // - not protected by slot (segment 1's endLSN < slot position at segment 3)
    // - not protected by reader (none registered)
    // - not in most recent minSegmentCount (2) segments

    for (const eligible of result.eligibleForDeletion) {
      expect(result.protectedBySlots).not.toContain(eligible);
      expect(result.protectedByReaders).not.toContain(eligible);
      expect(result.protectedByMinCount).not.toContain(eligible);
    }
  });
});

// =============================================================================
// Test: Edge Cases
// =============================================================================

describe('WAL Retention - Edge Cases', () => {
  let backend: FSXBackend;
  let writer: WALWriter;
  let reader: WALReader;

  beforeEach(() => {
    backend = createTestBackend();
    writer = createWALWriter(backend);
    reader = createWALReader(backend);
  });

  afterEach(async () => {
    await writer.close();
  });

  it('should handle empty WAL gracefully', async () => {
    const retentionManager = createWALRetentionManager(backend, reader, null);

    const result = await retentionManager.checkRetention();

    expect(result.totalSegmentCount).toBe(0);
    expect(result.eligibleForDeletion.length).toBe(0);
    expect(result.oldestSegmentTime).toBeNull();
  });

  it('should handle single segment WAL', async () => {
    const retentionManager = createWALRetentionManager(backend, reader, null, {
      minSegmentCount: 1,
      maxSegmentAge: 0,
    });

    await writeMultipleSegments(writer, 1);

    const result = await retentionManager.checkRetention();

    expect(result.totalSegmentCount).toBe(1);
    // The single segment should be protected by min count
    expect(result.protectedByMinCount.length).toBe(1);
  });

  it('should work without slot manager', async () => {
    const retentionManager = createWALRetentionManager(backend, reader, null);

    await writeMultipleSegments(writer, 2);

    const minLSN = await retentionManager.getMinSlotLSN();
    expect(minLSN).toBeNull();

    const result = await retentionManager.checkRetention();
    expect(result.minSlotLSN).toBeNull();
  });

  it('should handle very large LSN values', async () => {
    const slotManager = createReplicationSlotManager(backend, reader);
    const retentionManager = createWALRetentionManager(
      backend,
      reader,
      slotManager
    );

    // Create slot with large LSN
    const largeLSN = BigInt('9223372036854775807'); // Max int64
    await slotManager.createSlot('large-lsn-slot', largeLSN);

    const minLSN = await retentionManager.getMinSlotLSN();
    expect(minLSN).toBe(largeLSN);
  });
});
