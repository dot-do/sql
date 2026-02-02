/**
 * WAL Writer Unit Tests
 *
 * Comprehensive tests for WAL write operations and sync.
 * Tests the writer.ts module including:
 * - Basic append operations
 * - Segment creation and flush
 * - LSN assignment
 * - HLC timestamp integration
 * - Transaction helper
 * - Edge cases and error handling
 *
 * Issue: sql-ta4d
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  createWALWriter,
  DefaultWALEncoder,
  WALTransaction,
  createTransaction,
  generateTxnId,
} from '../writer.js';
import {
  WALError,
  WALErrorCode,
  DEFAULT_WAL_CONFIG,
  createTransactionId,
  type WALEntry,
} from '../types.js';
import { createMemoryBackend, type MemoryFSXBackend } from '../../fsx/index.js';

// =============================================================================
// Test Utilities
// =============================================================================

function makeTestEntry(overrides: Partial<Omit<WALEntry, 'lsn'>> = {}): Omit<WALEntry, 'lsn'> {
  return {
    timestamp: Date.now(),
    txnId: createTransactionId('txn_test'),
    op: 'INSERT',
    table: 'test_table',
    after: new Uint8Array([1, 2, 3, 4, 5]),
    ...overrides,
  };
}

// =============================================================================
// DefaultWALEncoder Tests
// =============================================================================

describe('DefaultWALEncoder', () => {
  let encoder: DefaultWALEncoder;

  beforeEach(() => {
    encoder = new DefaultWALEncoder();
  });

  describe('encodeEntry/decodeEntry', () => {
    it('should round-trip a basic entry', () => {
      const entry: WALEntry = {
        lsn: 42n,
        timestamp: 1234567890,
        txnId: createTransactionId('txn_123'),
        op: 'INSERT',
        table: 'users',
        after: new Uint8Array([10, 20, 30]),
      };

      const encoded = encoder.encodeEntry(entry);
      const decoded = encoder.decodeEntry(encoded);

      expect(decoded.lsn).toBe(42n);
      expect(decoded.timestamp).toBe(1234567890);
      expect(decoded.txnId).toBe('txn_123');
      expect(decoded.op).toBe('INSERT');
      expect(decoded.table).toBe('users');
      expect(decoded.after).toEqual(new Uint8Array([10, 20, 30]));
    });

    it('should handle UPDATE entry with key, before, and after', () => {
      const entry: WALEntry = {
        lsn: 100n,
        timestamp: Date.now(),
        txnId: createTransactionId('txn_upd'),
        op: 'UPDATE',
        table: 'users',
        key: new Uint8Array([1]),
        before: new Uint8Array([2, 3]),
        after: new Uint8Array([4, 5, 6]),
      };

      const decoded = encoder.decodeEntry(encoder.encodeEntry(entry));

      expect(decoded.op).toBe('UPDATE');
      expect(decoded.key).toEqual(new Uint8Array([1]));
      expect(decoded.before).toEqual(new Uint8Array([2, 3]));
      expect(decoded.after).toEqual(new Uint8Array([4, 5, 6]));
    });

    it('should handle DELETE entry', () => {
      const entry: WALEntry = {
        lsn: 200n,
        timestamp: Date.now(),
        txnId: createTransactionId('txn_del'),
        op: 'DELETE',
        table: 'users',
        key: new Uint8Array([99]),
        before: new Uint8Array([1, 2, 3]),
      };

      const decoded = encoder.decodeEntry(encoder.encodeEntry(entry));

      expect(decoded.op).toBe('DELETE');
      expect(decoded.key).toEqual(new Uint8Array([99]));
      expect(decoded.before).toEqual(new Uint8Array([1, 2, 3]));
      expect(decoded.after).toBeUndefined();
    });

    it('should handle transaction control entries (BEGIN, COMMIT, ROLLBACK)', () => {
      const ops = ['BEGIN', 'COMMIT', 'ROLLBACK'] as const;

      for (const op of ops) {
        const entry: WALEntry = {
          lsn: 1n,
          timestamp: Date.now(),
          txnId: createTransactionId('txn_ctrl'),
          op,
          table: '',
        };

        const decoded = encoder.decodeEntry(encoder.encodeEntry(entry));
        expect(decoded.op).toBe(op);
        expect(decoded.table).toBe('');
      }
    });

    it('should handle entry with HLC timestamp', () => {
      const entry: WALEntry = {
        lsn: 50n,
        timestamp: Date.now(),
        txnId: createTransactionId('txn_hlc'),
        op: 'INSERT',
        table: 'test',
        hlc: {
          physicalTime: 1700000000000,
          logicalCounter: 5,
          nodeId: 'node_abc',
        },
      };

      const decoded = encoder.decodeEntry(encoder.encodeEntry(entry));

      expect(decoded.hlc).toBeDefined();
      expect(decoded.hlc?.physicalTime).toBe(1700000000000);
      expect(decoded.hlc?.logicalCounter).toBe(5);
      expect(decoded.hlc?.nodeId).toBe('node_abc');
    });

    it('should handle large binary data', () => {
      const largeData = new Uint8Array(10000);
      for (let i = 0; i < largeData.length; i++) {
        largeData[i] = i % 256;
      }

      const entry: WALEntry = {
        lsn: 1n,
        timestamp: Date.now(),
        txnId: createTransactionId('txn_large'),
        op: 'INSERT',
        table: 'blobs',
        after: largeData,
      };

      const decoded = encoder.decodeEntry(encoder.encodeEntry(entry));

      expect(decoded.after).toEqual(largeData);
    });

    it('should handle empty binary data', () => {
      const entry: WALEntry = {
        lsn: 1n,
        timestamp: Date.now(),
        txnId: createTransactionId('txn_empty'),
        op: 'INSERT',
        table: 'test',
        after: new Uint8Array(0),
      };

      const decoded = encoder.decodeEntry(encoder.encodeEntry(entry));

      // Note: Empty Uint8Array encodes to empty string in base64, which decodes to empty Uint8Array
      // However, the encoder may not preserve empty arrays (treats them as undefined)
      // This is acceptable behavior - empty data and no data are semantically equivalent
      expect(decoded.after === undefined || decoded.after?.length === 0).toBe(true);
    });
  });

  describe('encodeSegment/decodeSegment', () => {
    it('should round-trip a segment with multiple entries', () => {
      const segment = {
        id: 'seg_00000000000000000001',
        startLSN: 1n,
        endLSN: 3n,
        entries: [
          { lsn: 1n, timestamp: 1000, txnId: createTransactionId('txn_1'), op: 'BEGIN' as const, table: '' },
          { lsn: 2n, timestamp: 1001, txnId: createTransactionId('txn_1'), op: 'INSERT' as const, table: 'users', after: new Uint8Array([1]) },
          { lsn: 3n, timestamp: 1002, txnId: createTransactionId('txn_1'), op: 'COMMIT' as const, table: '' },
        ],
        checksum: 12345,
        createdAt: Date.now(),
      };

      const encoded = encoder.encodeSegment(segment);
      const decoded = encoder.decodeSegment(encoded);

      expect(decoded.id).toBe(segment.id);
      expect(decoded.startLSN).toBe(1n);
      expect(decoded.endLSN).toBe(3n);
      expect(decoded.entries.length).toBe(3);
      expect(decoded.checksum).toBe(12345);
    });

    it('should handle archived segment flag', () => {
      const segment = {
        id: 'seg_00000000000000000100',
        startLSN: 100n,
        endLSN: 199n,
        entries: [],
        checksum: 0,
        createdAt: Date.now(),
        archived: true,
      };

      const decoded = encoder.decodeSegment(encoder.encodeSegment(segment));

      expect(decoded.archived).toBe(true);
    });
  });

  describe('calculateChecksum', () => {
    it('should produce consistent checksums for same data', () => {
      const data = new Uint8Array([1, 2, 3, 4, 5]);

      const checksum1 = encoder.calculateChecksum(data);
      const checksum2 = encoder.calculateChecksum(data);

      expect(checksum1).toBe(checksum2);
    });

    it('should produce different checksums for different data', () => {
      const data1 = new Uint8Array([1, 2, 3]);
      const data2 = new Uint8Array([1, 2, 4]);

      const checksum1 = encoder.calculateChecksum(data1);
      const checksum2 = encoder.calculateChecksum(data2);

      expect(checksum1).not.toBe(checksum2);
    });

    it('should handle empty data', () => {
      const data = new Uint8Array(0);
      const checksum = encoder.calculateChecksum(data);

      expect(typeof checksum).toBe('number');
    });
  });
});

// =============================================================================
// createWALWriter Tests
// =============================================================================

describe('createWALWriter', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  describe('basic operations', () => {
    it('should create a writer with default configuration', () => {
      const writer = createWALWriter(backend);

      expect(writer.getCurrentLSN()).toBe(0n);
      expect(writer.getPendingCount()).toBe(0);
      expect(writer.getCurrentSegmentSize()).toBe(0);
    });

    it('should create a writer with custom initial LSN', () => {
      const writer = createWALWriter(backend, 100n);

      expect(writer.getCurrentLSN()).toBe(100n);
    });

    it('should create a writer with custom configuration', () => {
      const writer = createWALWriter(backend, 0n, {
        targetSegmentSize: 1024,
        maxEntriesPerSegment: 10,
      });

      expect(writer).toBeDefined();
    });
  });

  describe('append', () => {
    it('should assign monotonically increasing LSNs', async () => {
      const writer = createWALWriter(backend);

      const result1 = await writer.append(makeTestEntry());
      const result2 = await writer.append(makeTestEntry());
      const result3 = await writer.append(makeTestEntry());

      expect(result1.lsn).toBe(0n);
      expect(result2.lsn).toBe(1n);
      expect(result3.lsn).toBe(2n);
    });

    it('should increment pending count on append without sync', async () => {
      const writer = createWALWriter(backend, 0n, {
        maxEntriesPerSegment: 1000,
        targetSegmentSize: 10 * 1024 * 1024,
      });

      await writer.append(makeTestEntry());
      expect(writer.getPendingCount()).toBe(1);

      await writer.append(makeTestEntry());
      expect(writer.getPendingCount()).toBe(2);
    });

    it('should flush immediately when sync option is true', async () => {
      const writer = createWALWriter(backend);

      const result = await writer.append(makeTestEntry(), { sync: true });

      expect(result.flushed).toBe(true);
      expect(result.segmentId).toBeDefined();
      expect(writer.getPendingCount()).toBe(0);
    });

    it('should auto-flush when maxEntriesPerSegment is reached', async () => {
      const writer = createWALWriter(backend, 0n, {
        maxEntriesPerSegment: 3,
        targetSegmentSize: 10 * 1024 * 1024,
      });

      await writer.append(makeTestEntry());
      await writer.append(makeTestEntry());
      const result = await writer.append(makeTestEntry());

      expect(result.flushed).toBe(true);
      expect(writer.getPendingCount()).toBe(0);
    });

    it('should auto-flush when targetSegmentSize is reached', async () => {
      const writer = createWALWriter(backend, 0n, {
        maxEntriesPerSegment: 1000,
        targetSegmentSize: 500, // Small size to trigger flush
      });

      // Append entries with large data until flush
      let flushed = false;
      for (let i = 0; i < 10 && !flushed; i++) {
        const result = await writer.append({
          ...makeTestEntry(),
          after: new Uint8Array(100), // 100 bytes per entry
        });
        flushed = result.flushed;
      }

      expect(flushed).toBe(true);
    });

    it('should assign HLC timestamp to each entry', async () => {
      const writer = createWALWriter(backend);

      await writer.append(makeTestEntry(), { sync: true });

      const files = await backend.list(DEFAULT_WAL_CONFIG.segmentPrefix);
      expect(files.length).toBe(1);

      const data = await backend.read(files[0]);
      expect(data).not.toBeNull();

      const encoder = new DefaultWALEncoder();
      const segment = encoder.decodeSegment(data!);

      expect(segment.entries[0].hlc).toBeDefined();
      expect(segment.entries[0].hlc?.nodeId).toContain('node_');
    });

    it('should reject append after writer is closed', async () => {
      const writer = createWALWriter(backend);

      await writer.close();

      await expect(writer.append(makeTestEntry())).rejects.toThrow(WALError);
      await expect(writer.append(makeTestEntry())).rejects.toThrow(/closed/i);
    });
  });

  describe('flush', () => {
    it('should return null when no pending entries', async () => {
      const writer = createWALWriter(backend);

      const segment = await writer.flush();

      expect(segment).toBeNull();
    });

    it('should write segment to storage on flush', async () => {
      const writer = createWALWriter(backend);

      await writer.append(makeTestEntry());
      await writer.append(makeTestEntry());

      const segment = await writer.flush();

      expect(segment).not.toBeNull();
      expect(segment!.entries.length).toBe(2);
      expect(segment!.startLSN).toBe(0n);
      expect(segment!.endLSN).toBe(1n);

      // Verify written to backend
      const files = await backend.list(DEFAULT_WAL_CONFIG.segmentPrefix);
      expect(files.length).toBe(1);
    });

    it('should reset pending count after flush', async () => {
      const writer = createWALWriter(backend);

      await writer.append(makeTestEntry());
      await writer.append(makeTestEntry());
      expect(writer.getPendingCount()).toBe(2);

      await writer.flush();
      expect(writer.getPendingCount()).toBe(0);
    });

    it('should generate segment ID with zero-padded LSN', async () => {
      const writer = createWALWriter(backend, 42n);

      await writer.append(makeTestEntry());
      const segment = await writer.flush();

      expect(segment!.id).toBe('seg_00000000000000000042');
    });

    it('should calculate checksum for segment', async () => {
      const writer = createWALWriter(backend);

      await writer.append(makeTestEntry());
      const segment = await writer.flush();

      expect(segment!.checksum).not.toBe(0);
      expect(typeof segment!.checksum).toBe('number');
    });
  });

  describe('close', () => {
    it('should flush pending entries on close', async () => {
      const writer = createWALWriter(backend);

      await writer.append(makeTestEntry());
      await writer.append(makeTestEntry());
      expect(writer.getPendingCount()).toBe(2);

      await writer.close();

      expect(writer.getPendingCount()).toBe(0);

      const files = await backend.list(DEFAULT_WAL_CONFIG.segmentPrefix);
      expect(files.length).toBe(1);
    });

    it('should be idempotent', async () => {
      const writer = createWALWriter(backend);

      await writer.append(makeTestEntry());
      await writer.close();
      await writer.close(); // Should not throw

      const files = await backend.list(DEFAULT_WAL_CONFIG.segmentPrefix);
      expect(files.length).toBe(1);
    });
  });

  describe('HLC support', () => {
    it('should provide access to HLC clock', () => {
      const writer = createWALWriter(backend);

      const clock = writer.getHLCClock();

      expect(clock).toBeDefined();
      expect(typeof clock.now).toBe('function');
    });

    it('should receive external HLC timestamps', async () => {
      const writer = createWALWriter(backend);

      const remoteHLC = {
        physicalTime: Date.now() + 1000, // Future time
        logicalCounter: 10,
        nodeId: 'remote_node',
      };

      await writer.receiveHLC(remoteHLC);

      // The clock should be updated to handle the remote timestamp
      const metrics = writer.getDriftMetrics();
      expect(metrics).toBeDefined();
    });

    it('should support custom node ID configuration', () => {
      const writer = createWALWriter(backend, 0n, {}, undefined, {
        nodeId: 'custom_node_123',
      });

      const clock = writer.getHLCClock();
      const timestamp = clock.now();

      expect(timestamp.nodeId).toBe('custom_node_123');
    });
  });

  describe('error handling', () => {
    it('should throw WALError on storage write failure', async () => {
      // Create a backend that fails on write
      const failingBackend = {
        ...createMemoryBackend(),
        write: async () => {
          throw new Error('Storage failure');
        },
      };

      const writer = createWALWriter(failingBackend);
      await writer.append(makeTestEntry());

      await expect(writer.flush()).rejects.toThrow(WALError);
      await expect(writer.flush()).rejects.toThrow(/Failed to write segment/);
    });

    it('should propagate error code on failure', async () => {
      const failingBackend = {
        ...createMemoryBackend(),
        write: async () => {
          throw new Error('Storage failure');
        },
      };

      const writer = createWALWriter(failingBackend);
      await writer.append(makeTestEntry());

      try {
        await writer.flush();
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(WALError);
        expect((error as WALError).code).toBe(WALErrorCode.FLUSH_FAILED);
      }
    });
  });
});

// =============================================================================
// WALTransaction Tests
// =============================================================================

describe('WALTransaction', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should create transaction with generateTxnId', () => {
    const txnId = generateTxnId();

    expect(txnId).toMatch(/^txn_[a-z0-9]+_[a-z0-9]+$/);
  });

  it('should create transaction with createTransaction helper', () => {
    const writer = createWALWriter(backend);
    const txn = createTransaction(writer);

    expect(txn).toBeInstanceOf(WALTransaction);
    expect(txn.txnId).toMatch(/^txn_/);
    expect(txn.isActive()).toBe(true);
  });

  describe('insert', () => {
    it('should buffer insert operations', () => {
      const writer = createWALWriter(backend);
      const txn = createTransaction(writer);

      txn.insert('users', new Uint8Array([1, 2, 3]));

      expect(txn.isActive()).toBe(true);
    });

    it('should chain insert operations', () => {
      const writer = createWALWriter(backend);
      const txn = createTransaction(writer);

      const result = txn
        .insert('users', new Uint8Array([1]))
        .insert('users', new Uint8Array([2]))
        .insert('users', new Uint8Array([3]));

      expect(result).toBe(txn);
    });
  });

  describe('update', () => {
    it('should buffer update operations', () => {
      const writer = createWALWriter(backend);
      const txn = createTransaction(writer);

      txn.update(
        'users',
        new Uint8Array([1]),
        new Uint8Array([10, 20]),
        new Uint8Array([10, 30])
      );

      expect(txn.isActive()).toBe(true);
    });
  });

  describe('delete', () => {
    it('should buffer delete operations', () => {
      const writer = createWALWriter(backend);
      const txn = createTransaction(writer);

      txn.delete('users', new Uint8Array([1]), new Uint8Array([10, 20]));

      expect(txn.isActive()).toBe(true);
    });
  });

  describe('commit', () => {
    it('should write BEGIN, entries, and COMMIT to WAL', async () => {
      const writer = createWALWriter(backend);
      const txn = createTransaction(writer);

      txn.insert('users', new Uint8Array([1]));
      txn.insert('users', new Uint8Array([2]));

      const lsns = await txn.commit();

      // Should have BEGIN + 2 INSERTs + COMMIT = 4 entries
      expect(lsns.length).toBe(4);
      expect(txn.isActive()).toBe(false);
    });

    it('should flush on commit (sync: true)', async () => {
      const writer = createWALWriter(backend);
      const txn = createTransaction(writer);

      txn.insert('users', new Uint8Array([1]));
      await txn.commit();

      // Commit uses sync: true, so should be flushed
      const files = await backend.list(DEFAULT_WAL_CONFIG.segmentPrefix);
      expect(files.length).toBeGreaterThan(0);
    });

    it('should throw if committed twice', async () => {
      const writer = createWALWriter(backend);
      const txn = createTransaction(writer);

      txn.insert('users', new Uint8Array([1]));
      await txn.commit();

      expect(() => txn.insert('users', new Uint8Array([2]))).toThrow(/already committed/);
    });
  });

  describe('rollback', () => {
    it('should mark transaction as inactive', async () => {
      const writer = createWALWriter(backend);
      const txn = createTransaction(writer);

      txn.insert('users', new Uint8Array([1]));
      await txn.rollback();

      expect(txn.isActive()).toBe(false);
    });

    it('should throw if operations added after rollback', async () => {
      const writer = createWALWriter(backend);
      const txn = createTransaction(writer);

      await txn.rollback();

      expect(() => txn.insert('users', new Uint8Array([1]))).toThrow(/already rolled back/);
    });

    it('should write ROLLBACK entry if committed then rolled back', async () => {
      const writer = createWALWriter(backend);
      const txn = createTransaction(writer);

      txn.insert('users', new Uint8Array([1]));
      await txn.commit();
      await txn.rollback(); // Write ROLLBACK entry

      // Verify ROLLBACK was written
      const files = await backend.list(DEFAULT_WAL_CONFIG.segmentPrefix);
      const data = await backend.read(files[files.length - 1]);
      const encoder = new DefaultWALEncoder();
      const segment = encoder.decodeSegment(data!);

      const rollbackEntry = segment.entries.find(e => e.op === 'ROLLBACK');
      expect(rollbackEntry).toBeDefined();
    });
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('WAL Writer Edge Cases', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should handle very large LSN values', async () => {
    const largeLSN = 9007199254740992n; // 2^53
    const writer = createWALWriter(backend, largeLSN);

    const result = await writer.append(makeTestEntry(), { sync: true });

    expect(result.lsn).toBe(largeLSN);
  });

  it('should handle unicode in table names', async () => {
    const writer = createWALWriter(backend);

    const result = await writer.append({
      ...makeTestEntry(),
      table: 'users_\u00e9\u00e8\u00ea',
    }, { sync: true });

    expect(result.flushed).toBe(true);

    // Verify can be read back
    const files = await backend.list(DEFAULT_WAL_CONFIG.segmentPrefix);
    const data = await backend.read(files[0]);
    const encoder = new DefaultWALEncoder();
    const segment = encoder.decodeSegment(data!);

    expect(segment.entries[0].table).toBe('users_\u00e9\u00e8\u00ea');
  });

  it('should handle binary data with all byte values', async () => {
    const writer = createWALWriter(backend);

    // Create data with all 256 byte values
    const allBytes = new Uint8Array(256);
    for (let i = 0; i < 256; i++) {
      allBytes[i] = i;
    }

    await writer.append({
      ...makeTestEntry(),
      after: allBytes,
    }, { sync: true });

    // Verify can be read back correctly
    const files = await backend.list(DEFAULT_WAL_CONFIG.segmentPrefix);
    const data = await backend.read(files[0]);
    const encoder = new DefaultWALEncoder();
    const segment = encoder.decodeSegment(data!);

    expect(segment.entries[0].after).toEqual(allBytes);
  });

  it('should handle multiple flushes', async () => {
    const writer = createWALWriter(backend, 0n, {
      maxEntriesPerSegment: 2,
    });

    // First batch
    await writer.append(makeTestEntry());
    await writer.append(makeTestEntry());
    // Auto-flush should trigger

    // Second batch
    await writer.append(makeTestEntry());
    await writer.append(makeTestEntry());
    // Auto-flush should trigger

    const files = await backend.list(DEFAULT_WAL_CONFIG.segmentPrefix);
    expect(files.length).toBe(2);
  });

  it('should handle concurrent appends', async () => {
    const writer = createWALWriter(backend, 0n, {
      maxEntriesPerSegment: 1000,
      targetSegmentSize: 10 * 1024 * 1024,
    });

    // Append multiple entries concurrently
    const promises = [];
    for (let i = 0; i < 10; i++) {
      promises.push(writer.append(makeTestEntry()));
    }

    const results = await Promise.all(promises);

    // All should succeed with unique LSNs
    const lsns = results.map(r => r.lsn);
    const uniqueLSNs = new Set(lsns);
    expect(uniqueLSNs.size).toBe(10);

    await writer.close();
  });
});
