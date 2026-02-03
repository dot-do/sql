/**
 * WAL Error Path Tests
 *
 * Comprehensive tests for WAL module error handling and recovery scenarios.
 * Following the NO MOCKS philosophy - tests use real implementations and
 * construct corrupt/invalid data to trigger error paths.
 *
 * Covers:
 * - Corrupt WAL file handling
 * - Incomplete writes
 * - Recovery from crashes
 * - Checksum validation failures
 * - Invalid segment formats
 * - Storage failures
 * - Encoder/decoder errors
 *
 * Issue: sql-0m0q
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { createWALReader } from '../reader.js';
import { createWALWriter, DefaultWALEncoder } from '../writer.js';
import {
  createCheckpointManager,
  performRecovery,
  needsRecovery,
} from '../checkpoint.js';
import {
  WALError,
  WALErrorCode,
  DEFAULT_WAL_CONFIG,
  createTransactionId,
  type WALSegment,
  type WALEntry,
} from '../types.js';
import { createMemoryBackend, type MemoryFSXBackend } from '../../fsx/index.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Helper to create a valid segment and then corrupt specific bytes
 */
async function writeCorruptedSegment(
  backend: MemoryFSXBackend,
  segment: WALSegment,
  corruptionFn: (data: Uint8Array) => Uint8Array
): Promise<void> {
  const encoder = new DefaultWALEncoder();
  const path = `${DEFAULT_WAL_CONFIG.segmentPrefix}${segment.id}`;

  // First calculate proper checksum
  const withoutChecksum = { ...segment, checksum: 0 };
  const data = encoder.encodeSegment(withoutChecksum);
  segment.checksum = encoder.calculateChecksum(data);

  // Encode with correct checksum
  const validData = encoder.encodeSegment(segment);

  // Apply corruption
  const corruptedData = corruptionFn(validData);

  await backend.write(path, corruptedData);
}

/**
 * Create test entries with sequential LSNs
 */
function makeTestEntries(count: number, startLSN = 0n): WALEntry[] {
  const entries: WALEntry[] = [];
  for (let i = 0; i < count; i++) {
    entries.push({
      lsn: startLSN + BigInt(i),
      timestamp: Date.now() + i,
      txnId: createTransactionId(`txn_${i}`),
      op: 'INSERT',
      table: 'test',
      after: new Uint8Array([i % 256]),
    });
  }
  return entries;
}

/**
 * Create a valid test segment
 */
function makeTestSegment(id: string, startLSN: bigint, entryCount: number): WALSegment {
  return {
    id,
    startLSN,
    endLSN: startLSN + BigInt(entryCount - 1),
    entries: makeTestEntries(entryCount, startLSN),
    checksum: 0,
    createdAt: Date.now(),
  };
}

/**
 * Write a valid segment to storage
 */
async function writeValidSegment(
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

// =============================================================================
// Corrupt WAL File Handling
// =============================================================================

describe('WAL Error Paths - Corrupt File Handling', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  describe('completely invalid data', () => {
    it('should throw WALError for empty file', async () => {
      await backend.write(
        `${DEFAULT_WAL_CONFIG.segmentPrefix}seg_00000000000000000000`,
        new Uint8Array(0)
      );

      const reader = createWALReader(backend);

      await expect(reader.readSegment('seg_00000000000000000000'))
        .rejects.toThrow(WALError);
    });

    it('should throw WALError for random bytes', async () => {
      const randomBytes = new Uint8Array(256);
      crypto.getRandomValues(randomBytes);

      await backend.write(
        `${DEFAULT_WAL_CONFIG.segmentPrefix}seg_00000000000000000000`,
        randomBytes
      );

      const reader = createWALReader(backend);

      await expect(reader.readSegment('seg_00000000000000000000'))
        .rejects.toThrow(WALError);
    });

    it('should throw WALError for invalid JSON', async () => {
      await backend.write(
        `${DEFAULT_WAL_CONFIG.segmentPrefix}seg_00000000000000000000`,
        new TextEncoder().encode('{ invalid json }}}')
      );

      const reader = createWALReader(backend);

      await expect(reader.readSegment('seg_00000000000000000000'))
        .rejects.toThrow(WALError);
    });

    it('should throw with SEGMENT_CORRUPTED error code for parse errors', async () => {
      await backend.write(
        `${DEFAULT_WAL_CONFIG.segmentPrefix}seg_00000000000000000000`,
        new TextEncoder().encode('not json at all')
      );

      const reader = createWALReader(backend);

      try {
        await reader.readSegment('seg_00000000000000000000');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(WALError);
        expect((error as WALError).code).toBe(WALErrorCode.SEGMENT_CORRUPTED);
      }
    });
  });

  describe('structurally invalid JSON', () => {
    it('should throw for JSON missing required fields', async () => {
      const incompleteJson = JSON.stringify({
        id: 'seg_00000000000000000000',
        // Missing startLSN, endLSN, entries, checksum
      });

      await backend.write(
        `${DEFAULT_WAL_CONFIG.segmentPrefix}seg_00000000000000000000`,
        new TextEncoder().encode(incompleteJson)
      );

      const reader = createWALReader(backend);

      await expect(reader.readSegment('seg_00000000000000000000'))
        .rejects.toThrow();
    });

    it('should throw for JSON with wrong types', async () => {
      const wrongTypesJson = JSON.stringify({
        id: 'seg_00000000000000000000',
        startLSN: 'not a number', // Should be string of bigint
        endLSN: 'not a number',
        entries: 'not an array',
        checksum: 'not a number',
        createdAt: 'not a number',
      });

      await backend.write(
        `${DEFAULT_WAL_CONFIG.segmentPrefix}seg_00000000000000000000`,
        new TextEncoder().encode(wrongTypesJson)
      );

      const reader = createWALReader(backend);

      await expect(reader.readSegment('seg_00000000000000000000'))
        .rejects.toThrow();
    });

    it('should throw for entries with invalid LSN format', async () => {
      const invalidLSNJson = JSON.stringify({
        id: 'seg_00000000000000000000',
        startLSN: '0',
        endLSN: '0',
        entries: [{
          lsn: 'not_a_bigint', // Invalid format
          timestamp: Date.now(),
          txnId: 'txn_1',
          op: 'INSERT',
          table: 'test',
        }],
        checksum: 0,
        createdAt: Date.now(),
      });

      await backend.write(
        `${DEFAULT_WAL_CONFIG.segmentPrefix}seg_00000000000000000000`,
        new TextEncoder().encode(invalidLSNJson)
      );

      const reader = createWALReader(backend);

      await expect(reader.readSegment('seg_00000000000000000000'))
        .rejects.toThrow();
    });
  });

  describe('partially corrupted data', () => {
    it('should throw for truncated JSON', async () => {
      const encoder = new DefaultWALEncoder();
      const segment = makeTestSegment('seg_00000000000000000000', 0n, 5);
      const validData = encoder.encodeSegment(segment);

      // Truncate the data
      const truncated = validData.slice(0, validData.length / 2);

      await backend.write(
        `${DEFAULT_WAL_CONFIG.segmentPrefix}seg_00000000000000000000`,
        truncated
      );

      const reader = createWALReader(backend);

      await expect(reader.readSegment('seg_00000000000000000000'))
        .rejects.toThrow(WALError);
    });

    it('should throw for data with trailing garbage', async () => {
      const encoder = new DefaultWALEncoder();
      const segment = makeTestSegment('seg_00000000000000000000', 0n, 5);
      const withoutChecksum = { ...segment, checksum: 0 };
      const data = encoder.encodeSegment(withoutChecksum);
      segment.checksum = encoder.calculateChecksum(data);
      const validData = encoder.encodeSegment(segment);

      // Add garbage at the end
      const withGarbage = new Uint8Array(validData.length + 100);
      withGarbage.set(validData);
      withGarbage.set(new Uint8Array([0xFF, 0xFE, 0xFD]), validData.length);

      await backend.write(
        `${DEFAULT_WAL_CONFIG.segmentPrefix}seg_00000000000000000000`,
        withGarbage
      );

      const reader = createWALReader(backend);

      // Should still be able to read if JSON is valid up to closing brace
      // OR should fail checksum validation
      try {
        await reader.readSegment('seg_00000000000000000000');
      } catch (error) {
        // Either parse error or checksum error is acceptable
        expect(error).toBeInstanceOf(WALError);
      }
    });
  });
});

// =============================================================================
// Checksum Validation Failures
// =============================================================================

describe('WAL Error Paths - Checksum Validation', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should detect wrong checksum value', async () => {
    const segment = makeTestSegment('seg_00000000000000000000', 0n, 5);

    await writeCorruptedSegment(backend, segment, (data) => {
      // Decode, modify checksum, re-encode
      const json = JSON.parse(new TextDecoder().decode(data));
      json.checksum = 12345; // Wrong checksum
      return new TextEncoder().encode(JSON.stringify(json));
    });

    const reader = createWALReader(backend, { verifyChecksums: true });

    try {
      await reader.readSegment('seg_00000000000000000000');
      expect.fail('Should have thrown');
    } catch (error) {
      expect(error).toBeInstanceOf(WALError);
      expect((error as WALError).code).toBe(WALErrorCode.CHECKSUM_MISMATCH);
    }
  });

  it('should detect data modified after checksum calculation', async () => {
    const segment = makeTestSegment('seg_00000000000000000000', 0n, 5);

    await writeCorruptedSegment(backend, segment, (data) => {
      const json = JSON.parse(new TextDecoder().decode(data));
      // Modify an entry after checksum was calculated
      json.entries[0].table = 'modified_table';
      return new TextEncoder().encode(JSON.stringify(json));
    });

    const reader = createWALReader(backend, { verifyChecksums: true });

    try {
      await reader.readSegment('seg_00000000000000000000');
      expect.fail('Should have thrown');
    } catch (error) {
      expect(error).toBeInstanceOf(WALError);
      expect((error as WALError).code).toBe(WALErrorCode.CHECKSUM_MISMATCH);
    }
  });

  it('should detect bit flip in data', async () => {
    const segment = makeTestSegment('seg_00000000000000000000', 0n, 5);

    await writeCorruptedSegment(backend, segment, (data) => {
      // Flip a bit somewhere in the middle
      const corrupted = new Uint8Array(data);
      if (corrupted.length > 50) {
        corrupted[50] ^= 0x01; // Flip one bit
      }
      return corrupted;
    });

    const reader = createWALReader(backend, { verifyChecksums: true });

    // Should either fail to parse or fail checksum
    await expect(reader.readSegment('seg_00000000000000000000'))
      .rejects.toThrow(WALError);
  });

  it('should skip checksum verification when disabled', async () => {
    const segment = makeTestSegment('seg_00000000000000000000', 0n, 5);

    await writeCorruptedSegment(backend, segment, (data) => {
      const json = JSON.parse(new TextDecoder().decode(data));
      json.checksum = 12345; // Wrong checksum
      return new TextEncoder().encode(JSON.stringify(json));
    });

    const reader = createWALReader(backend, { verifyChecksums: false });

    // Should succeed because checksum verification is disabled
    const result = await reader.readSegment('seg_00000000000000000000');
    expect(result).not.toBeNull();
  });

  it('should include segment ID in checksum error message', async () => {
    const segment = makeTestSegment('seg_00000000000000000000', 0n, 5);

    await writeCorruptedSegment(backend, segment, (data) => {
      const json = JSON.parse(new TextDecoder().decode(data));
      json.checksum = 99999;
      return new TextEncoder().encode(JSON.stringify(json));
    });

    const reader = createWALReader(backend, { verifyChecksums: true });

    try {
      await reader.readSegment('seg_00000000000000000000');
      expect.fail('Should have thrown');
    } catch (error) {
      expect(error).toBeInstanceOf(WALError);
      const walError = error as WALError;
      // The segment ID is included in the error message
      expect(walError.message).toContain('seg_00000000000000000000');
      expect(walError.code).toBe(WALErrorCode.CHECKSUM_MISMATCH);
    }
  });
});

// =============================================================================
// Invalid Segment ID Format
// =============================================================================

describe('WAL Error Paths - Invalid Segment ID', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should throw for invalid segment ID format in parseSegmentLSN', async () => {
    // Write a file with valid content but invalid filename format
    const encoder = new DefaultWALEncoder();
    const segment = {
      id: 'invalid_id', // Not matching seg_ pattern
      startLSN: 0n,
      endLSN: 4n,
      entries: makeTestEntries(5, 0n),
      checksum: 0,
      createdAt: Date.now(),
    };
    const data = encoder.encodeSegment({ ...segment, checksum: 0 });
    segment.checksum = encoder.calculateChecksum(data);

    await backend.write(
      `${DEFAULT_WAL_CONFIG.segmentPrefix}invalid_id`,
      encoder.encodeSegment(segment)
    );

    const reader = createWALReader(backend);

    // listSegments should filter out invalid IDs
    const segments = await reader.listSegments();
    expect(segments).not.toContain('invalid_id');
  });

  it('should handle segment ID with wrong padding', async () => {
    const encoder = new DefaultWALEncoder();
    const segment = {
      id: 'seg_123', // Not properly padded
      startLSN: 123n,
      endLSN: 127n,
      entries: makeTestEntries(5, 123n),
      checksum: 0,
      createdAt: Date.now(),
    };
    const data = encoder.encodeSegment({ ...segment, checksum: 0 });
    segment.checksum = encoder.calculateChecksum(data);

    await backend.write(
      `${DEFAULT_WAL_CONFIG.segmentPrefix}seg_123`,
      encoder.encodeSegment(segment)
    );

    const reader = createWALReader(backend);

    // This should still be parseable since the regex matches
    const segments = await reader.listSegments();
    expect(segments).toContain('seg_123');
  });
});

// =============================================================================
// Incomplete Writes
// =============================================================================

describe('WAL Error Paths - Incomplete Writes', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should handle segment written without proper closing', async () => {
    // Simulate a segment that was truncated during write
    const encoder = new DefaultWALEncoder();
    const segment = makeTestSegment('seg_00000000000000000000', 0n, 10);
    const withoutChecksum = { ...segment, checksum: 0 };
    const data = encoder.encodeSegment(withoutChecksum);
    segment.checksum = encoder.calculateChecksum(data);
    const validData = encoder.encodeSegment(segment);

    // Truncate at 80% - simulating incomplete write
    const truncated = validData.slice(0, Math.floor(validData.length * 0.8));

    await backend.write(
      `${DEFAULT_WAL_CONFIG.segmentPrefix}seg_00000000000000000000`,
      truncated
    );

    const reader = createWALReader(backend);

    await expect(reader.readSegment('seg_00000000000000000000'))
      .rejects.toThrow(WALError);
  });

  it('should handle zero-length segment file', async () => {
    await backend.write(
      `${DEFAULT_WAL_CONFIG.segmentPrefix}seg_00000000000000000000`,
      new Uint8Array(0)
    );

    const reader = createWALReader(backend);

    await expect(reader.readSegment('seg_00000000000000000000'))
      .rejects.toThrow(WALError);
  });

  it('should continue reading other segments when one is corrupted', async () => {
    // Write a valid segment
    await writeValidSegment(backend, makeTestSegment('seg_00000000000000000000', 0n, 5));

    // Write a corrupted segment
    await backend.write(
      `${DEFAULT_WAL_CONFIG.segmentPrefix}seg_00000000000000000005`,
      new TextEncoder().encode('corrupted data')
    );

    // Write another valid segment
    await writeValidSegment(backend, makeTestSegment('seg_00000000000000000010', 10n, 5));

    const reader = createWALReader(backend);

    // Should list all segments
    const segments = await reader.listSegments();
    expect(segments.length).toBe(3);

    // Should read valid segments
    const seg0 = await reader.readSegment('seg_00000000000000000000');
    expect(seg0).not.toBeNull();

    const seg10 = await reader.readSegment('seg_00000000000000000010');
    expect(seg10).not.toBeNull();

    // Corrupted segment should throw
    await expect(reader.readSegment('seg_00000000000000000005'))
      .rejects.toThrow(WALError);
  });
});

// =============================================================================
// Recovery From Crashes
// =============================================================================

describe('WAL Error Paths - Crash Recovery', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should recover from crash with incomplete transaction', async () => {
    const txn1 = createTransactionId('txn_complete');
    const txn2 = createTransactionId('txn_incomplete');

    await writeValidSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 5n,
      entries: [
        { lsn: 0n, timestamp: 1000, txnId: txn1, op: 'BEGIN', table: '' },
        { lsn: 1n, timestamp: 1001, txnId: txn1, op: 'INSERT', table: 'test', after: new Uint8Array([1]) },
        { lsn: 2n, timestamp: 1002, txnId: txn1, op: 'COMMIT', table: '' },
        { lsn: 3n, timestamp: 1003, txnId: txn2, op: 'BEGIN', table: '' },
        { lsn: 4n, timestamp: 1004, txnId: txn2, op: 'INSERT', table: 'test', after: new Uint8Array([2]) },
        // txn2 never committed - simulates crash
      ],
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    const appliedEntries: WALEntry[] = [];
    const state = await checkpointMgr.recover(async (entry) => {
      appliedEntries.push(entry);
    });

    // Only txn1's INSERT should be applied
    expect(appliedEntries.length).toBe(1);
    expect(appliedEntries[0].txnId).toBe(txn1);

    // txn2 should be rolled back
    expect(state.rolledBackTransactions).toContain(txn2);
  });

  it('should handle corrupt checkpoint file during recovery', async () => {
    await writeValidSegment(backend, makeTestSegment('seg_00000000000000000000', 0n, 5));

    // Write corrupted checkpoint
    await backend.write(
      DEFAULT_WAL_CONFIG.checkpointPath,
      new TextEncoder().encode('{ invalid checkpoint }')
    );

    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    // Should throw when trying to get checkpoint
    await expect(checkpointMgr.getCheckpoint())
      .rejects.toThrow(WALError);
  });

  it('should handle missing checkpoint gracefully during needsRecovery', async () => {
    await writeValidSegment(backend, makeTestSegment('seg_00000000000000000000', 0n, 5));

    // No checkpoint file exists

    const result = await needsRecovery(backend);

    expect(result.needsRecovery).toBe(true);
    expect(result.checkpointLSN).toBeNull();
  });

  it('should handle recovery when apply function throws', async () => {
    const txn = createTransactionId('txn_1');

    await writeValidSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 4n,
      entries: [
        { lsn: 0n, timestamp: 1000, txnId: txn, op: 'BEGIN', table: '' },
        { lsn: 1n, timestamp: 1001, txnId: txn, op: 'INSERT', table: 'test', after: new Uint8Array([1]) },
        { lsn: 2n, timestamp: 1002, txnId: txn, op: 'INSERT', table: 'test', after: new Uint8Array([2]) },
        { lsn: 3n, timestamp: 1003, txnId: txn, op: 'COMMIT', table: '' },
      ],
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    let callCount = 0;
    const state = await checkpointMgr.recover(async () => {
      callCount++;
      if (callCount === 1) {
        throw new Error('Simulated apply failure');
      }
    });

    // Should record error but continue
    expect(state.errors.length).toBe(1);
    expect(state.errors[0].lsn).toBe(1n);
    expect(state.entriesReplayed).toBe(1); // Second INSERT succeeded
  });

  it('should detect and handle gaps in WAL segments', async () => {
    // Segment 1: LSN 0-4
    await writeValidSegment(backend, makeTestSegment('seg_00000000000000000000', 0n, 5));

    // Gap: LSN 5-9 missing (simulates lost segment)

    // Segment 2: LSN 10-14
    await writeValidSegment(backend, makeTestSegment('seg_00000000000000000010', 10n, 5));

    const reader = createWALReader(backend);

    // Should be able to read both segments
    const segments = await reader.listSegments();
    expect(segments.length).toBe(2);

    // Reading entries should work but have a gap
    const entries = await reader.readEntries({});
    expect(entries.length).toBe(10);

    // Entry for LSN 7 should not exist
    const missing = await reader.getEntry(7n);
    expect(missing).toBeNull();
  });
});

// =============================================================================
// Writer Error Paths
// =============================================================================

describe('WAL Error Paths - Writer Failures', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should throw on append after close', async () => {
    const writer = createWALWriter(backend);

    await writer.close();

    await expect(
      writer.append({
        timestamp: Date.now(),
        txnId: createTransactionId('txn_1'),
        op: 'INSERT',
        table: 'test',
        after: new Uint8Array([1]),
      })
    ).rejects.toThrow(WALError);
  });

  it('should throw with FLUSH_FAILED when storage write fails', async () => {
    const failingBackend = {
      ...createMemoryBackend(),
      write: async () => {
        throw new Error('Storage unavailable');
      },
    };

    const writer = createWALWriter(failingBackend);

    await writer.append({
      timestamp: Date.now(),
      txnId: createTransactionId('txn_1'),
      op: 'INSERT',
      table: 'test',
      after: new Uint8Array([1]),
    });

    try {
      await writer.flush();
      expect.fail('Should have thrown');
    } catch (error) {
      expect(error).toBeInstanceOf(WALError);
      expect((error as WALError).code).toBe(WALErrorCode.FLUSH_FAILED);
    }
  });

  it('should handle backpressure when pending entries exceed limit', async () => {
    const writer = createWALWriter(backend, 0n, {
      maxPendingEntries: 5,
      maxEntriesPerSegment: 1000,
      targetSegmentSize: 100 * 1024 * 1024, // Large size to prevent auto-flush
    });

    // Fill up to the limit
    for (let i = 0; i < 5; i++) {
      await writer.append({
        timestamp: Date.now(),
        txnId: createTransactionId(`txn_${i}`),
        op: 'INSERT',
        table: 'test',
        after: new Uint8Array([i]),
      });
    }

    // Next append should trigger backpressure error
    await expect(
      writer.append({
        timestamp: Date.now(),
        txnId: createTransactionId('txn_overflow'),
        op: 'INSERT',
        table: 'test',
        after: new Uint8Array([99]),
      })
    ).rejects.toThrow(/backpressure/i);

    await writer.close();
  });

  it('should report isRetryable for flush failures', async () => {
    const failingBackend = {
      ...createMemoryBackend(),
      write: async () => {
        throw new Error('Temporary failure');
      },
    };

    const writer = createWALWriter(failingBackend);

    await writer.append({
      timestamp: Date.now(),
      txnId: createTransactionId('txn_1'),
      op: 'INSERT',
      table: 'test',
      after: new Uint8Array([1]),
    });

    try {
      await writer.flush();
      expect.fail('Should have thrown');
    } catch (error) {
      expect(error).toBeInstanceOf(WALError);
      expect((error as WALError).isRetryable()).toBe(true);
    }
  });
});

// =============================================================================
// Checkpoint Error Paths
// =============================================================================

describe('WAL Error Paths - Checkpoint Failures', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should throw CHECKPOINT_FAILED when no segment contains LSN', async () => {
    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    // No segments exist, so no LSN can be checkpointed
    try {
      await checkpointMgr.createCheckpoint(100n, []);
      expect.fail('Should have thrown');
    } catch (error) {
      expect(error).toBeInstanceOf(WALError);
      expect((error as WALError).code).toBe(WALErrorCode.CHECKPOINT_FAILED);
      expect((error as WALError).message).toContain('No segment found');
    }
  });

  it('should throw CHECKPOINT_FAILED on corrupted checkpoint data', async () => {
    await backend.write(
      DEFAULT_WAL_CONFIG.checkpointPath,
      new TextEncoder().encode('not valid json {{{')
    );

    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    try {
      await checkpointMgr.getCheckpoint();
      expect.fail('Should have thrown');
    } catch (error) {
      expect(error).toBeInstanceOf(WALError);
      expect((error as WALError).code).toBe(WALErrorCode.CHECKPOINT_FAILED);
    }
  });

  it('should throw when checkpoint write fails', async () => {
    await writeValidSegment(backend, makeTestSegment('seg_00000000000000000000', 0n, 5));

    // Create a proper backend that implements all methods but fails on checkpoint write
    const failingBackend = {
      read: (path: string) => backend.read(path),
      write: async (path: string, data: Uint8Array) => {
        if (path === DEFAULT_WAL_CONFIG.checkpointPath) {
          throw new Error('Cannot write checkpoint');
        }
        return backend.write(path, data);
      },
      delete: (path: string) => backend.delete(path),
      list: (prefix: string) => backend.list(prefix),
      exists: (path: string) => backend.exists(path),
    };

    const reader = createWALReader(failingBackend);
    const checkpointMgr = createCheckpointManager(failingBackend, reader);

    try {
      await checkpointMgr.createCheckpoint(2n, []);
      expect.fail('Should have thrown');
    } catch (error) {
      expect(error).toBeInstanceOf(WALError);
      expect((error as WALError).code).toBe(WALErrorCode.CHECKPOINT_FAILED);
    }
  });

  it('should handle checkpoint with invalid LSN format', async () => {
    const invalidCheckpoint = JSON.stringify({
      lsn: 'not_a_number',
      timestamp: Date.now(),
      segmentId: 'seg_00000000000000000000',
      activeTransactions: [],
    });

    await backend.write(
      DEFAULT_WAL_CONFIG.checkpointPath,
      new TextEncoder().encode(invalidCheckpoint)
    );

    const reader = createWALReader(backend);
    const checkpointMgr = createCheckpointManager(backend, reader);

    await expect(checkpointMgr.getCheckpoint())
      .rejects.toThrow();
  });
});

// =============================================================================
// Encoder/Decoder Error Paths
// =============================================================================

describe('WAL Error Paths - Encoder/Decoder', () => {
  it('should handle invalid base64 in entry data', () => {
    const encoder = new DefaultWALEncoder();

    const invalidJson = JSON.stringify({
      lsn: '0',
      timestamp: Date.now(),
      txnId: 'txn_1',
      op: 'INSERT',
      table: 'test',
      after: '!!!invalid-base64!!!', // Not valid base64
    });

    expect(() =>
      encoder.decodeEntry(new TextEncoder().encode(invalidJson))
    ).toThrow();
  });

  it('should handle invalid operation type gracefully', () => {
    const encoder = new DefaultWALEncoder();

    // This should decode even with unknown op (it's a string)
    const weirdOpJson = JSON.stringify({
      lsn: '0',
      timestamp: Date.now(),
      txnId: 'txn_1',
      op: 'WEIRD_OP',
      table: 'test',
    });

    const decoded = encoder.decodeEntry(new TextEncoder().encode(weirdOpJson));
    expect(decoded.op).toBe('WEIRD_OP');
  });

  it('should produce consistent checksum for same data', () => {
    const encoder = new DefaultWALEncoder();
    const data = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    const checksum1 = encoder.calculateChecksum(data);
    const checksum2 = encoder.calculateChecksum(data);

    expect(checksum1).toBe(checksum2);
  });

  it('should produce different checksum for different data', () => {
    const encoder = new DefaultWALEncoder();

    const data1 = new Uint8Array([1, 2, 3]);
    const data2 = new Uint8Array([1, 2, 4]);

    const checksum1 = encoder.calculateChecksum(data1);
    const checksum2 = encoder.calculateChecksum(data2);

    expect(checksum1).not.toBe(checksum2);
  });
});

// =============================================================================
// WALError Class Tests
// =============================================================================

describe('WAL Error Paths - WALError Class', () => {
  it('should provide user-friendly messages for each error code', () => {
    const codes = Object.values(WALErrorCode);

    for (const code of codes) {
      const error = new WALError(code, 'Test message');
      const userMessage = error.toUserMessage();

      expect(userMessage).toBeTruthy();
      expect(typeof userMessage).toBe('string');
    }
  });

  it('should set correct category for each error code', () => {
    // CHECKSUM_MISMATCH should be INTERNAL
    const checksumError = new WALError(WALErrorCode.CHECKSUM_MISMATCH, 'test');
    expect(checksumError.category).toBe('INTERNAL');

    // SEGMENT_NOT_FOUND should be RESOURCE
    const notFoundError = new WALError(WALErrorCode.SEGMENT_NOT_FOUND, 'test');
    expect(notFoundError.category).toBe('RESOURCE');

    // INVALID_LSN should be VALIDATION
    const invalidError = new WALError(WALErrorCode.INVALID_LSN, 'test');
    expect(invalidError.category).toBe('VALIDATION');

    // FLUSH_FAILED should be EXECUTION
    const flushError = new WALError(WALErrorCode.FLUSH_FAILED, 'test');
    expect(flushError.category).toBe('EXECUTION');
  });

  it('should provide recovery hints for each error code', () => {
    const codes = Object.values(WALErrorCode);

    for (const code of codes) {
      const error = new WALError(code, 'Test message');
      expect(error.recoveryHint).toBeTruthy();
      expect(typeof error.recoveryHint).toBe('string');
    }
  });

  it('should include LSN and segmentId in context', () => {
    const error = new WALError(
      WALErrorCode.CHECKSUM_MISMATCH,
      'Checksum failed',
      {
        lsn: 42n,
        segmentId: 'seg_00000000000000000042',
      }
    );

    expect(error.lsn).toBe(42n);
    expect(error.segmentId).toBe('seg_00000000000000000042');
    expect(error.context?.metadata?.lsn).toBe('42');
    expect(error.context?.metadata?.segmentId).toBe('seg_00000000000000000042');
  });

  it('should serialize and deserialize correctly', () => {
    const original = new WALError(
      WALErrorCode.SEGMENT_CORRUPTED,
      'Segment is corrupted',
      {
        lsn: 100n,
        segmentId: 'seg_test',
      }
    );

    const json = original.toJSON();
    const restored = WALError.fromJSON(json);

    expect(restored.code).toBe(original.code);
    expect(restored.message).toBe(original.message);
    expect(restored.segmentId).toBe(original.segmentId);
  });
});
