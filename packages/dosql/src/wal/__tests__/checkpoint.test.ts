/**
 * WAL Checkpoint Unit Tests
 *
 * Comprehensive tests for checkpoint creation and recovery.
 * Tests the checkpoint.ts module including:
 * - Checkpoint creation and retrieval
 * - Segment archiving
 * - Recovery from checkpoint
 * - Auto-checkpointing
 * - Edge cases and error handling
 *
 * Issue: sql-ta4d
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  createCheckpointManager,
  performRecovery,
  needsRecovery,
  createAutoCheckpointer,
} from '../checkpoint.js';
import { createWALReader } from '../reader.js';
import { createWALWriter, DefaultWALEncoder } from '../writer.js';
import {
  WALError,
  WALErrorCode,
  DEFAULT_WAL_CONFIG,
  createTransactionId,
  type WALSegment,
  type WALEntry,
  type Checkpoint,
} from '../types.js';
import { createMemoryBackend, type MemoryFSXBackend } from '../../fsx/index.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Helper to create a segment directly in storage with calculated checksum
 */
async function writeTestSegment(
  backend: MemoryFSXBackend,
  segment: WALSegment
): Promise<void> {
  const encoder = new DefaultWALEncoder();
  const path = `${DEFAULT_WAL_CONFIG.segmentPrefix}${segment.id}`;

  // Calculate checksum
  const withoutChecksum = { ...segment, checksum: 0 };
  const data = encoder.encodeSegment(withoutChecksum);
  segment.checksum = encoder.calculateChecksum(data);

  await backend.write(path, encoder.encodeSegment(segment));
}

/**
 * Create test entries with sequential LSNs
 */
function makeTestEntries(count: number, startLSN = 0n, txnIdPrefix = 'txn'): WALEntry[] {
  const entries: WALEntry[] = [];
  for (let i = 0; i < count; i++) {
    entries.push({
      lsn: startLSN + BigInt(i),
      timestamp: Date.now() + i,
      txnId: createTransactionId(`${txnIdPrefix}_${startLSN + BigInt(i)}`),
      op: 'INSERT',
      table: 'test',
      after: new Uint8Array([i % 256]),
    });
  }
  return entries;
}

/**
 * Create a complete transaction (BEGIN, INSERT, COMMIT)
 */
function makeTransactionEntries(
  startLSN: bigint,
  txnId: string,
  insertCount = 1
): WALEntry[] {
  const entries: WALEntry[] = [];
  let lsn = startLSN;

  entries.push({
    lsn: lsn++,
    timestamp: Date.now(),
    txnId: createTransactionId(txnId),
    op: 'BEGIN',
    table: '',
  });

  for (let i = 0; i < insertCount; i++) {
    entries.push({
      lsn: lsn++,
      timestamp: Date.now(),
      txnId: createTransactionId(txnId),
      op: 'INSERT',
      table: 'test',
      after: new Uint8Array([i]),
    });
  }

  entries.push({
    lsn: lsn++,
    timestamp: Date.now(),
    txnId: createTransactionId(txnId),
    op: 'COMMIT',
    table: '',
  });

  return entries;
}

// =============================================================================
// createCheckpointManager Tests
// =============================================================================

describe('createCheckpointManager', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  describe('getCheckpoint', () => {
    it('should return null when no checkpoint exists', async () => {
      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      const checkpoint = await manager.getCheckpoint();

      expect(checkpoint).toBeNull();
    });

    it('should return existing checkpoint', async () => {
      // Write a checkpoint manually
      const checkpointData: Checkpoint = {
        lsn: 100n,
        timestamp: Date.now(),
        segmentId: 'seg_00000000000000000000',
        activeTransactions: [],
      };

      await backend.write(
        DEFAULT_WAL_CONFIG.checkpointPath,
        new TextEncoder().encode(JSON.stringify({
          lsn: checkpointData.lsn.toString(),
          timestamp: checkpointData.timestamp,
          segmentId: checkpointData.segmentId,
          activeTransactions: checkpointData.activeTransactions,
        }))
      );

      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      const checkpoint = await manager.getCheckpoint();

      expect(checkpoint).not.toBeNull();
      expect(checkpoint!.lsn).toBe(100n);
      expect(checkpoint!.segmentId).toBe('seg_00000000000000000000');
    });

    it('should throw on corrupted checkpoint data', async () => {
      await backend.write(
        DEFAULT_WAL_CONFIG.checkpointPath,
        new TextEncoder().encode('{ invalid json }')
      );

      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      await expect(manager.getCheckpoint()).rejects.toThrow(WALError);
    });
  });

  describe('createCheckpoint', () => {
    it('should create checkpoint at specified LSN', async () => {
      // Create a segment containing the LSN
      await writeTestSegment(backend, {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 99n,
        entries: makeTestEntries(100, 0n),
        checksum: 0,
        createdAt: Date.now(),
      });

      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      const checkpoint = await manager.createCheckpoint(50n, []);

      expect(checkpoint.lsn).toBe(50n);
      expect(checkpoint.segmentId).toBe('seg_00000000000000000000');
      expect(checkpoint.timestamp).toBeGreaterThan(0);
    });

    it('should include active transactions in checkpoint', async () => {
      await writeTestSegment(backend, {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 99n,
        entries: makeTestEntries(100, 0n),
        checksum: 0,
        createdAt: Date.now(),
      });

      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      const activeTxns = [createTransactionId('txn_1'), createTransactionId('txn_2')];
      const checkpoint = await manager.createCheckpoint(50n, activeTxns);

      expect(checkpoint.activeTransactions).toEqual(activeTxns);
    });

    it('should persist checkpoint to storage', async () => {
      await writeTestSegment(backend, {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 99n,
        entries: makeTestEntries(100, 0n),
        checksum: 0,
        createdAt: Date.now(),
      });

      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      await manager.createCheckpoint(50n, []);

      // Verify checkpoint file exists
      const data = await backend.read(DEFAULT_WAL_CONFIG.checkpointPath);
      expect(data).not.toBeNull();

      // New manager should see the checkpoint
      const manager2 = createCheckpointManager(backend, reader);
      const checkpoint = await manager2.getCheckpoint();
      expect(checkpoint!.lsn).toBe(50n);
    });

    it('should throw if no segment contains the LSN', async () => {
      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      await expect(manager.createCheckpoint(100n, []))
        .rejects.toThrow(WALError);
      await expect(manager.createCheckpoint(100n, []))
        .rejects.toThrow(/No segment found/);
    });

    it('should find segment for LSN that is one past the last entry (getCurrentLSN case)', async () => {
      // This tests the case where createCheckpoint is called with writer.getCurrentLSN()
      // which returns the NEXT LSN to be assigned, not the last written LSN
      await writeTestSegment(backend, {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 99n,
        entries: makeTestEntries(100, 0n),
        checksum: 0,
        createdAt: Date.now(),
      });

      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      // LSN 100 is one past the last entry (99), but the segment covers 0-99
      const checkpoint = await manager.createCheckpoint(100n, []);

      expect(checkpoint.lsn).toBe(100n);
      expect(checkpoint.segmentId).toBe('seg_00000000000000000000');
    });
  });

  describe('archiveOldSegments', () => {
    beforeEach(async () => {
      // Create several segments
      for (let i = 0; i < 5; i++) {
        const startLSN = BigInt(i * 100);
        await writeTestSegment(backend, {
          id: `seg_${startLSN.toString().padStart(20, '0')}`,
          startLSN,
          endLSN: startLSN + 99n,
          entries: makeTestEntries(100, startLSN),
          checksum: 0,
          createdAt: Date.now(),
        });
      }
    });

    it('should return 0 when no checkpoint exists', async () => {
      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      const archived = await manager.archiveOldSegments();

      expect(archived).toBe(0);
    });

    it('should archive segments before checkpoint LSN', async () => {
      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      // Create checkpoint at LSN 250 (segments 0, 1 should be archivable)
      await manager.createCheckpoint(250n, []);

      const archived = await manager.archiveOldSegments();

      expect(archived).toBe(2);

      // Verify segments moved to archive
      const activeSegments = await reader.listSegments(false);
      const allSegments = await reader.listSegments(true);

      expect(activeSegments.length).toBe(3);
      expect(allSegments.length).toBe(5);
    });

    it('should respect retainCount for archives', async () => {
      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      // Checkpoint at end
      await manager.createCheckpoint(450n, []);

      // Archive with retainCount of 2
      await manager.archiveOldSegments(2);

      // Should keep only 2 most recent archived segments
      const archivePaths = await backend.list(DEFAULT_WAL_CONFIG.archivePrefix);
      expect(archivePaths.length).toBeLessThanOrEqual(2);
    });

    it('should not archive segment containing checkpoint', async () => {
      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      // Checkpoint in segment 2 (LSN 200-299)
      await manager.createCheckpoint(250n, []);

      await manager.archiveOldSegments();

      // Segment 2 should NOT be archived
      const activeSegments = await reader.listSegments(false);
      expect(activeSegments).toContain('seg_00000000000000000200');
    });
  });

  describe('recover', () => {
    it('should return empty state when no entries to replay', async () => {
      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      const appliedEntries: WALEntry[] = [];
      const state = await manager.recover(async (entry) => {
        appliedEntries.push(entry);
      });

      expect(state.entriesReplayed).toBe(0);
      expect(appliedEntries.length).toBe(0);
    });

    it('should replay all entries when no checkpoint exists', async () => {
      // Create segment with 3 committed transactions
      const entries: WALEntry[] = [];
      for (let i = 0; i < 3; i++) {
        entries.push(...makeTransactionEntries(BigInt(entries.length), `txn_${i}`, 2));
      }

      await writeTestSegment(backend, {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: BigInt(entries.length - 1),
        entries,
        checksum: 0,
        createdAt: Date.now(),
      });

      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      const appliedEntries: WALEntry[] = [];
      const state = await manager.recover(async (entry) => {
        appliedEntries.push(entry);
      });

      // Should only apply INSERT entries (not BEGIN/COMMIT)
      expect(appliedEntries.every(e => e.op === 'INSERT')).toBe(true);
      expect(appliedEntries.length).toBe(6); // 3 transactions * 2 inserts each
      expect(state.entriesReplayed).toBe(6);
    });

    it('should resume from checkpoint LSN', async () => {
      // Create entries
      const entries1 = makeTransactionEntries(0n, 'txn_1', 3);
      const entries2 = makeTransactionEntries(BigInt(entries1.length), 'txn_2', 3);

      await writeTestSegment(backend, {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: BigInt(entries1.length + entries2.length - 1),
        entries: [...entries1, ...entries2],
        checksum: 0,
        createdAt: Date.now(),
      });

      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      // Create checkpoint after first transaction
      await manager.createCheckpoint(BigInt(entries1.length - 1), []);

      // Recover should only replay entries after checkpoint
      const appliedEntries: WALEntry[] = [];
      const state = await manager.recover(async (entry) => {
        appliedEntries.push(entry);
      });

      // Should only have txn_2's INSERTs
      expect(appliedEntries.length).toBe(3);
      expect(appliedEntries.every(e => e.txnId.includes('txn_2'))).toBe(true);
    });

    it('should skip rolled back transactions', async () => {
      const txn1 = createTransactionId('txn_committed');
      const txn2 = createTransactionId('txn_rolledback');

      await writeTestSegment(backend, {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 7n,
        entries: [
          { lsn: 0n, timestamp: 1000, txnId: txn1, op: 'BEGIN', table: '' },
          { lsn: 1n, timestamp: 1001, txnId: txn1, op: 'INSERT', table: 'test', after: new Uint8Array([1]) },
          { lsn: 2n, timestamp: 1002, txnId: txn2, op: 'BEGIN', table: '' },
          { lsn: 3n, timestamp: 1003, txnId: txn2, op: 'INSERT', table: 'test', after: new Uint8Array([2]) },
          { lsn: 4n, timestamp: 1004, txnId: txn1, op: 'COMMIT', table: '' },
          { lsn: 5n, timestamp: 1005, txnId: txn2, op: 'INSERT', table: 'test', after: new Uint8Array([3]) },
          { lsn: 6n, timestamp: 1006, txnId: txn2, op: 'ROLLBACK', table: '' },
        ],
        checksum: 0,
        createdAt: Date.now(),
      });

      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      const appliedEntries: WALEntry[] = [];
      const state = await manager.recover(async (entry) => {
        appliedEntries.push(entry);
      });

      // Only txn_committed's INSERT should be applied
      expect(appliedEntries.length).toBe(1);
      expect(appliedEntries[0].txnId).toBe(txn1);
      expect(state.rolledBackTransactions).toContain(txn2);
    });

    it('should rollback incomplete transactions', async () => {
      const txn1 = createTransactionId('txn_complete');
      const txn2 = createTransactionId('txn_incomplete');

      await writeTestSegment(backend, {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 5n,
        entries: [
          { lsn: 0n, timestamp: 1000, txnId: txn1, op: 'BEGIN', table: '' },
          { lsn: 1n, timestamp: 1001, txnId: txn1, op: 'INSERT', table: 'test', after: new Uint8Array([1]) },
          { lsn: 2n, timestamp: 1002, txnId: txn2, op: 'BEGIN', table: '' },
          { lsn: 3n, timestamp: 1003, txnId: txn2, op: 'INSERT', table: 'test', after: new Uint8Array([2]) },
          { lsn: 4n, timestamp: 1004, txnId: txn1, op: 'COMMIT', table: '' },
          // txn2 has no COMMIT - simulates crash
          { lsn: 5n, timestamp: 1005, txnId: txn2, op: 'INSERT', table: 'test', after: new Uint8Array([3]) },
        ],
        checksum: 0,
        createdAt: Date.now(),
      });

      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      const appliedEntries: WALEntry[] = [];
      const state = await manager.recover(async (entry) => {
        appliedEntries.push(entry);
      });

      // Only txn_complete's entries should be applied
      expect(appliedEntries.length).toBe(1);
      expect(appliedEntries[0].txnId).toBe(txn1);
      expect(state.rolledBackTransactions).toContain(txn2);
    });

    it('should consider active transactions from checkpoint as rollback candidates', async () => {
      const txn1 = createTransactionId('txn_was_active');

      // Segment 1: Transaction starts
      await writeTestSegment(backend, {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 2n,
        entries: [
          { lsn: 0n, timestamp: 1000, txnId: txn1, op: 'BEGIN', table: '' },
          { lsn: 1n, timestamp: 1001, txnId: txn1, op: 'INSERT', table: 'test', after: new Uint8Array([1]) },
          { lsn: 2n, timestamp: 1002, txnId: txn1, op: 'INSERT', table: 'test', after: new Uint8Array([2]) },
        ],
        checksum: 0,
        createdAt: Date.now(),
      });

      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      // Create checkpoint with txn1 as active
      await manager.createCheckpoint(2n, [txn1]);

      // Segment 2: More entries but no commit for txn1
      await writeTestSegment(backend, {
        id: 'seg_00000000000000000003',
        startLSN: 3n,
        endLSN: 4n,
        entries: [
          { lsn: 3n, timestamp: 1003, txnId: txn1, op: 'INSERT', table: 'test', after: new Uint8Array([3]) },
          { lsn: 4n, timestamp: 1004, txnId: txn1, op: 'INSERT', table: 'test', after: new Uint8Array([4]) },
          // Still no commit - crash
        ],
        checksum: 0,
        createdAt: Date.now(),
      });

      const appliedEntries: WALEntry[] = [];
      const state = await manager.recover(async (entry) => {
        appliedEntries.push(entry);
      });

      // txn1 was active at checkpoint and never committed, should be rolled back
      expect(appliedEntries.length).toBe(0);
      expect(state.rolledBackTransactions).toContain(txn1);
    });

    it('should record errors during apply but continue', async () => {
      const txn1 = createTransactionId('txn_1');
      const txn2 = createTransactionId('txn_2');

      await writeTestSegment(backend, {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 5n,
        entries: [
          { lsn: 0n, timestamp: 1000, txnId: txn1, op: 'BEGIN', table: '' },
          { lsn: 1n, timestamp: 1001, txnId: txn1, op: 'INSERT', table: 'test', after: new Uint8Array([1]) },
          { lsn: 2n, timestamp: 1002, txnId: txn1, op: 'COMMIT', table: '' },
          { lsn: 3n, timestamp: 1003, txnId: txn2, op: 'BEGIN', table: '' },
          { lsn: 4n, timestamp: 1004, txnId: txn2, op: 'INSERT', table: 'test', after: new Uint8Array([2]) },
          { lsn: 5n, timestamp: 1005, txnId: txn2, op: 'COMMIT', table: '' },
        ],
        checksum: 0,
        createdAt: Date.now(),
      });

      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      let callCount = 0;
      const state = await manager.recover(async (entry) => {
        callCount++;
        if (callCount === 1) {
          throw new Error('Simulated apply error');
        }
      });

      // Should record error but continue
      expect(state.errors.length).toBe(1);
      expect(state.errors[0].lsn).toBe(1n);
      expect(state.entriesReplayed).toBe(1); // Second insert succeeded
    });

    it('should track completion time', async () => {
      await writeTestSegment(backend, {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 2n,
        entries: makeTransactionEntries(0n, 'txn_1', 1),
        checksum: 0,
        createdAt: Date.now(),
      });

      const reader = createWALReader(backend);
      const manager = createCheckpointManager(backend, reader);

      const state = await manager.recover(async () => {});

      expect(state.startedAt).toBeLessThanOrEqual(state.completedAt!);
      expect(state.completedAt).toBeDefined();
    });
  });
});

// =============================================================================
// performRecovery Tests
// =============================================================================

describe('performRecovery', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should perform full recovery process', async () => {
    const entries = makeTransactionEntries(0n, 'txn_1', 5);
    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: BigInt(entries.length - 1),
      entries,
      checksum: 0,
      createdAt: Date.now(),
    });

    const appliedEntries: WALEntry[] = [];
    const state = await performRecovery({
      backend,
      applyFn: async (entry) => {
        appliedEntries.push(entry);
      },
      archiveAfterRecovery: false,
    });

    expect(state.entriesReplayed).toBe(5);
    expect(appliedEntries.length).toBe(5);
  });

  it('should archive old segments after recovery when configured', async () => {
    // Create multiple segments
    for (let i = 0; i < 3; i++) {
      const startLSN = BigInt(i * 10);
      const txnEntries = makeTransactionEntries(startLSN, `txn_${i}`, 5);
      await writeTestSegment(backend, {
        id: `seg_${startLSN.toString().padStart(20, '0')}`,
        startLSN,
        endLSN: startLSN + BigInt(txnEntries.length - 1),
        entries: txnEntries,
        checksum: 0,
        createdAt: Date.now(),
      });
    }

    // Create checkpoint
    const reader = createWALReader(backend);
    const manager = createCheckpointManager(backend, reader);
    await manager.createCheckpoint(10n, []);

    const state = await performRecovery({
      backend,
      applyFn: async () => {},
      archiveAfterRecovery: true,
      archiveRetainCount: 1,
    });

    // Should have archived segments before checkpoint
    const activePaths = await backend.list(DEFAULT_WAL_CONFIG.segmentPrefix);
    const archivePaths = await backend.list(DEFAULT_WAL_CONFIG.archivePrefix);

    expect(activePaths.length).toBeLessThan(3);
  });
});

// =============================================================================
// needsRecovery Tests
// =============================================================================

describe('needsRecovery', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should return false when no segments exist', async () => {
    const result = await needsRecovery(backend);

    expect(result.needsRecovery).toBe(false);
    expect(result.checkpointLSN).toBeNull();
  });

  it('should return true when segments exist but no checkpoint', async () => {
    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 9n,
      entries: makeTestEntries(10, 0n),
      checksum: 0,
      createdAt: Date.now(),
    });

    const result = await needsRecovery(backend);

    expect(result.needsRecovery).toBe(true);
    expect(result.checkpointLSN).toBeNull();
  });

  it('should return false when checkpoint covers all entries', async () => {
    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 9n,
      entries: makeTestEntries(10, 0n),
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const manager = createCheckpointManager(backend, reader);
    await manager.createCheckpoint(9n, []);

    const result = await needsRecovery(backend);

    expect(result.needsRecovery).toBe(false);
    expect(result.checkpointLSN).toBe(9n);
  });

  it('should return true when entries exist after checkpoint', async () => {
    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 9n,
      entries: makeTestEntries(10, 0n),
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const manager = createCheckpointManager(backend, reader);
    await manager.createCheckpoint(5n, []);

    const result = await needsRecovery(backend);

    expect(result.needsRecovery).toBe(true);
    expect(result.checkpointLSN).toBe(5n);
  });
});

// =============================================================================
// createAutoCheckpointer Tests
// =============================================================================

describe('createAutoCheckpointer', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should create checkpointer with configured intervals', async () => {
    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 99n,
      entries: makeTestEntries(100, 0n),
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const manager = createCheckpointManager(backend, reader);

    let currentLSN = 10n;
    const checkpointer = createAutoCheckpointer(
      manager,
      () => currentLSN,
      {
        entryInterval: 5,
        timeInterval: 1000,
        getActiveTransactions: () => [],
      }
    );

    expect(checkpointer).toBeDefined();
    expect(typeof checkpointer.onEntryWritten).toBe('function');
    expect(typeof checkpointer.forceCheckpoint).toBe('function');
    expect(typeof checkpointer.start).toBe('function');
    expect(typeof checkpointer.stop).toBe('function');
  });

  it('should trigger checkpoint when entry interval exceeded', async () => {
    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 99n,
      entries: makeTestEntries(100, 0n),
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const manager = createCheckpointManager(backend, reader);

    let currentLSN = 10n;
    const checkpoints: Checkpoint[] = [];

    const checkpointer = createAutoCheckpointer(
      manager,
      () => currentLSN,
      {
        entryInterval: 5,
        timeInterval: 60000,
        getActiveTransactions: () => [],
        onCheckpoint: (cp) => checkpoints.push(cp),
      }
    );

    // Simulate 6 writes (exceeds interval of 5)
    for (let i = 0; i < 6; i++) {
      checkpointer.onEntryWritten();
      currentLSN++;
    }

    await checkpointer.maybeCheckpoint();

    expect(checkpoints.length).toBe(1);
  });

  it('should force checkpoint regardless of interval', async () => {
    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 99n,
      entries: makeTestEntries(100, 0n),
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const manager = createCheckpointManager(backend, reader);

    let currentLSN = 10n;
    const checkpoints: Checkpoint[] = [];

    const checkpointer = createAutoCheckpointer(
      manager,
      () => currentLSN,
      {
        entryInterval: 1000,
        timeInterval: 60000,
        getActiveTransactions: () => [],
        onCheckpoint: (cp) => checkpoints.push(cp),
      }
    );

    const checkpoint = await checkpointer.forceCheckpoint();

    expect(checkpoint).not.toBeNull();
    expect(checkpoints.length).toBe(1);
  });

  it('should return null from forceCheckpoint if no new entries', async () => {
    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 99n,
      entries: makeTestEntries(100, 0n),
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const manager = createCheckpointManager(backend, reader);

    const currentLSN = 10n;
    const checkpointer = createAutoCheckpointer(
      manager,
      () => currentLSN,
      {
        entryInterval: 1000,
        timeInterval: 60000,
        getActiveTransactions: () => [],
      }
    );

    // First checkpoint
    await checkpointer.forceCheckpoint();

    // Second attempt with same LSN should return null
    const second = await checkpointer.forceCheckpoint();

    expect(second).toBeNull();
  });

  it('should start and stop periodic checkpointing', async () => {
    vi.useFakeTimers();

    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 99n,
      entries: makeTestEntries(100, 0n),
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const manager = createCheckpointManager(backend, reader);

    let currentLSN = 10n;
    let checkpointCount = 0;

    const checkpointer = createAutoCheckpointer(
      manager,
      () => currentLSN++, // Increment each time to ensure new entries
      {
        entryInterval: 1000,
        timeInterval: 100, // 100ms interval
        getActiveTransactions: () => [],
        onCheckpoint: () => checkpointCount++,
      }
    );

    checkpointer.start();

    // Advance time
    await vi.advanceTimersByTimeAsync(350);

    checkpointer.stop();

    // Should have created some checkpoints
    expect(checkpointCount).toBeGreaterThan(0);

    // After stop, no more checkpoints should be created
    const countAfterStop = checkpointCount;
    await vi.advanceTimersByTimeAsync(200);

    expect(checkpointCount).toBe(countAfterStop);
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('Checkpoint Edge Cases', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should handle checkpoint at LSN 0', async () => {
    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 9n,
      entries: makeTestEntries(10, 0n),
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const manager = createCheckpointManager(backend, reader);

    const checkpoint = await manager.createCheckpoint(0n, []);

    expect(checkpoint.lsn).toBe(0n);
  });

  it('should handle very large LSN values', async () => {
    const largeLSN = 9007199254740992n;

    await writeTestSegment(backend, {
      id: `seg_${largeLSN.toString().padStart(20, '0')}`,
      startLSN: largeLSN,
      endLSN: largeLSN + 99n,
      entries: makeTestEntries(100, largeLSN),
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const manager = createCheckpointManager(backend, reader);

    const checkpoint = await manager.createCheckpoint(largeLSN + 50n, []);

    expect(checkpoint.lsn).toBe(largeLSN + 50n);
  });

  it('should handle recovery with entries across many segments', async () => {
    // Create 10 segments with transactions
    for (let seg = 0; seg < 10; seg++) {
      const startLSN = BigInt(seg * 10);
      const txnEntries = makeTransactionEntries(startLSN, `txn_${seg}`, 5);
      await writeTestSegment(backend, {
        id: `seg_${startLSN.toString().padStart(20, '0')}`,
        startLSN,
        endLSN: startLSN + BigInt(txnEntries.length - 1),
        entries: txnEntries,
        checksum: 0,
        createdAt: Date.now(),
      });
    }

    const reader = createWALReader(backend);
    const manager = createCheckpointManager(backend, reader);

    const appliedEntries: WALEntry[] = [];
    const state = await manager.recover(async (entry) => {
      appliedEntries.push(entry);
    });

    // Should apply all INSERT entries from all transactions
    expect(appliedEntries.length).toBe(50); // 10 transactions * 5 inserts
    expect(state.entriesReplayed).toBe(50);
  });

  it('should handle interleaved transactions correctly during recovery', async () => {
    const txn1 = createTransactionId('txn_1');
    const txn2 = createTransactionId('txn_2');
    const txn3 = createTransactionId('txn_3');

    // Complex interleaved scenario:
    // txn1: committed
    // txn2: rolled back
    // txn3: incomplete (should be rolled back)
    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 11n,
      entries: [
        { lsn: 0n, timestamp: 1000, txnId: txn1, op: 'BEGIN', table: '' },
        { lsn: 1n, timestamp: 1001, txnId: txn2, op: 'BEGIN', table: '' },
        { lsn: 2n, timestamp: 1002, txnId: txn1, op: 'INSERT', table: 'a', after: new Uint8Array([1]) },
        { lsn: 3n, timestamp: 1003, txnId: txn2, op: 'INSERT', table: 'a', after: new Uint8Array([2]) },
        { lsn: 4n, timestamp: 1004, txnId: txn3, op: 'BEGIN', table: '' },
        { lsn: 5n, timestamp: 1005, txnId: txn1, op: 'INSERT', table: 'b', after: new Uint8Array([3]) },
        { lsn: 6n, timestamp: 1006, txnId: txn3, op: 'INSERT', table: 'c', after: new Uint8Array([4]) },
        { lsn: 7n, timestamp: 1007, txnId: txn2, op: 'ROLLBACK', table: '' },
        { lsn: 8n, timestamp: 1008, txnId: txn1, op: 'INSERT', table: 'a', after: new Uint8Array([5]) },
        { lsn: 9n, timestamp: 1009, txnId: txn3, op: 'INSERT', table: 'c', after: new Uint8Array([6]) },
        { lsn: 10n, timestamp: 1010, txnId: txn1, op: 'COMMIT', table: '' },
        // txn3 has no commit
      ],
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const manager = createCheckpointManager(backend, reader);

    const appliedEntries: WALEntry[] = [];
    const state = await manager.recover(async (entry) => {
      appliedEntries.push(entry);
    });

    // Only txn1's 3 INSERTs should be applied
    expect(appliedEntries.length).toBe(3);
    expect(appliedEntries.every(e => e.txnId === txn1)).toBe(true);

    // txn2 and txn3 should be in rolledBack list
    expect(state.rolledBackTransactions).toContain(txn2);
    expect(state.rolledBackTransactions).toContain(txn3);
  });
});
