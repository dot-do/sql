/**
 * WAL Reader Unit Tests
 *
 * Comprehensive tests for WAL reading and replay.
 * Tests the reader.ts module including:
 * - Segment reading
 * - Entry filtering and range queries
 * - LSN-based lookups
 * - Async iteration
 * - Transaction reconstruction
 * - Tailing and batch reading
 *
 * Issue: sql-ta4d
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  createWALReader,
  tailWAL,
  readWALBatched,
  reconstructTransactions,
} from '../reader.js';
import { createWALWriter, DefaultWALEncoder } from '../writer.js';
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
 * Helper to create a segment directly in storage
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

// =============================================================================
// createWALReader Tests
// =============================================================================

describe('createWALReader', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  describe('listSegments', () => {
    it('should return empty array when no segments exist', async () => {
      const reader = createWALReader(backend);

      const segments = await reader.listSegments();

      expect(segments).toEqual([]);
    });

    it('should list segments in LSN order', async () => {
      // Write segments out of order
      await writeTestSegment(backend, {
        id: 'seg_00000000000000000100',
        startLSN: 100n,
        endLSN: 199n,
        entries: makeTestEntries(5, 100n),
        checksum: 0,
        createdAt: Date.now(),
      });

      await writeTestSegment(backend, {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 99n,
        entries: makeTestEntries(5, 0n),
        checksum: 0,
        createdAt: Date.now(),
      });

      await writeTestSegment(backend, {
        id: 'seg_00000000000000000050',
        startLSN: 50n,
        endLSN: 99n,
        entries: makeTestEntries(5, 50n),
        checksum: 0,
        createdAt: Date.now(),
      });

      const reader = createWALReader(backend);
      const segments = await reader.listSegments();

      expect(segments).toEqual([
        'seg_00000000000000000000',
        'seg_00000000000000000050',
        'seg_00000000000000000100',
      ]);
    });

    it('should optionally include archived segments', async () => {
      // Write active segment
      await writeTestSegment(backend, {
        id: 'seg_00000000000000000100',
        startLSN: 100n,
        endLSN: 199n,
        entries: makeTestEntries(5, 100n),
        checksum: 0,
        createdAt: Date.now(),
      });

      // Write archived segment
      const encoder = new DefaultWALEncoder();
      const archivedSegment = {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 99n,
        entries: makeTestEntries(5, 0n),
        checksum: 0,
        createdAt: Date.now(),
        archived: true,
      };
      await backend.write(
        `${DEFAULT_WAL_CONFIG.archivePrefix}${archivedSegment.id}`,
        encoder.encodeSegment(archivedSegment)
      );

      const reader = createWALReader(backend);

      // Without archived
      const activeOnly = await reader.listSegments(false);
      expect(activeOnly.length).toBe(1);

      // With archived
      const all = await reader.listSegments(true);
      expect(all.length).toBe(2);
    });
  });

  describe('readSegment', () => {
    it('should return null for non-existent segment', async () => {
      const reader = createWALReader(backend);

      const segment = await reader.readSegment('seg_nonexistent');

      expect(segment).toBeNull();
    });

    it('should read and decode segment', async () => {
      const entries = makeTestEntries(3);
      await writeTestSegment(backend, {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 2n,
        entries,
        checksum: 0,
        createdAt: Date.now(),
      });

      const reader = createWALReader(backend);
      const segment = await reader.readSegment('seg_00000000000000000000');

      expect(segment).not.toBeNull();
      expect(segment!.id).toBe('seg_00000000000000000000');
      expect(segment!.entries.length).toBe(3);
      expect(segment!.startLSN).toBe(0n);
      expect(segment!.endLSN).toBe(2n);
    });

    it('should verify checksum when verifyChecksums is true (default)', async () => {
      const encoder = new DefaultWALEncoder();
      const segment = {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 2n,
        entries: makeTestEntries(3),
        checksum: 12345, // Wrong checksum
        createdAt: Date.now(),
      };

      await backend.write(
        `${DEFAULT_WAL_CONFIG.segmentPrefix}${segment.id}`,
        encoder.encodeSegment(segment)
      );

      const reader = createWALReader(backend, { verifyChecksums: true });

      await expect(reader.readSegment('seg_00000000000000000000'))
        .rejects.toThrow(WALError);
    });

    it('should skip checksum verification when verifyChecksums is false', async () => {
      const encoder = new DefaultWALEncoder();
      const segment = {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 2n,
        entries: makeTestEntries(3),
        checksum: 12345, // Wrong checksum
        createdAt: Date.now(),
      };

      await backend.write(
        `${DEFAULT_WAL_CONFIG.segmentPrefix}${segment.id}`,
        encoder.encodeSegment(segment)
      );

      const reader = createWALReader(backend, { verifyChecksums: false });
      const result = await reader.readSegment('seg_00000000000000000000');

      expect(result).not.toBeNull();
    });

    it('should throw on corrupted segment data', async () => {
      // Write invalid JSON
      await backend.write(
        `${DEFAULT_WAL_CONFIG.segmentPrefix}seg_00000000000000000000`,
        new TextEncoder().encode('{ invalid json }')
      );

      const reader = createWALReader(backend);

      await expect(reader.readSegment('seg_00000000000000000000'))
        .rejects.toThrow(WALError);
    });
  });

  describe('readEntries', () => {
    beforeEach(async () => {
      // Set up test data: 3 segments with 5 entries each
      for (let seg = 0; seg < 3; seg++) {
        const startLSN = BigInt(seg * 5);
        await writeTestSegment(backend, {
          id: `seg_${startLSN.toString().padStart(20, '0')}`,
          startLSN,
          endLSN: startLSN + 4n,
          entries: makeTestEntries(5, startLSN).map((e, i) => ({
            ...e,
            table: i % 2 === 0 ? 'users' : 'orders',
            op: i === 0 ? 'BEGIN' as const : i === 4 ? 'COMMIT' as const : 'INSERT' as const,
          })),
          checksum: 0,
          createdAt: Date.now(),
        });
      }
    });

    it('should read all entries without filters', async () => {
      const reader = createWALReader(backend);

      const entries = await reader.readEntries({});

      expect(entries.length).toBe(15);
    });

    it('should filter by fromLSN (inclusive)', async () => {
      const reader = createWALReader(backend);

      const entries = await reader.readEntries({ fromLSN: 7n });

      expect(entries.every(e => e.lsn >= 7n)).toBe(true);
      expect(entries.length).toBe(8); // LSNs 7-14
    });

    it('should filter by toLSN (inclusive)', async () => {
      const reader = createWALReader(backend);

      const entries = await reader.readEntries({ toLSN: 5n });

      expect(entries.every(e => e.lsn <= 5n)).toBe(true);
      expect(entries.length).toBe(6); // LSNs 0-5
    });

    it('should filter by LSN range', async () => {
      const reader = createWALReader(backend);

      const entries = await reader.readEntries({ fromLSN: 3n, toLSN: 8n });

      expect(entries.every(e => e.lsn >= 3n && e.lsn <= 8n)).toBe(true);
      expect(entries.length).toBe(6); // LSNs 3-8
    });

    it('should filter by table name', async () => {
      const reader = createWALReader(backend);

      const entries = await reader.readEntries({ table: 'users' });

      expect(entries.every(e => e.table === 'users')).toBe(true);
    });

    it('should filter by operation types', async () => {
      const reader = createWALReader(backend);

      const entries = await reader.readEntries({ operations: ['INSERT'] });

      expect(entries.every(e => e.op === 'INSERT')).toBe(true);
    });

    it('should filter by transaction ID', async () => {
      const reader = createWALReader(backend);

      const entries = await reader.readEntries({ txnId: createTransactionId('txn_2') });

      expect(entries.every(e => e.txnId === 'txn_2')).toBe(true);
    });

    it('should respect limit option', async () => {
      const reader = createWALReader(backend);

      const entries = await reader.readEntries({ limit: 5 });

      expect(entries.length).toBe(5);
      expect(entries[0].lsn).toBe(0n);
    });

    it('should combine multiple filters', async () => {
      const reader = createWALReader(backend);

      const entries = await reader.readEntries({
        fromLSN: 0n,
        toLSN: 10n,
        table: 'users',
        operations: ['INSERT'],
        limit: 3,
      });

      expect(entries.length).toBeLessThanOrEqual(3);
      expect(entries.every(e => e.table === 'users' && e.op === 'INSERT')).toBe(true);
    });

    it('should return empty array for non-matching filters', async () => {
      const reader = createWALReader(backend);

      const entries = await reader.readEntries({ table: 'nonexistent' });

      expect(entries).toEqual([]);
    });
  });

  describe('getEntry', () => {
    beforeEach(async () => {
      await writeTestSegment(backend, {
        id: 'seg_00000000000000000000',
        startLSN: 0n,
        endLSN: 9n,
        entries: makeTestEntries(10, 0n),
        checksum: 0,
        createdAt: Date.now(),
      });
    });

    it('should find entry by exact LSN', async () => {
      const reader = createWALReader(backend);

      const entry = await reader.getEntry(5n);

      expect(entry).not.toBeNull();
      expect(entry!.lsn).toBe(5n);
    });

    it('should return null for non-existent LSN', async () => {
      const reader = createWALReader(backend);

      const entry = await reader.getEntry(100n);

      expect(entry).toBeNull();
    });

    it('should find entry in correct segment when multiple exist', async () => {
      await writeTestSegment(backend, {
        id: 'seg_00000000000000000010',
        startLSN: 10n,
        endLSN: 19n,
        entries: makeTestEntries(10, 10n),
        checksum: 0,
        createdAt: Date.now(),
      });

      const reader = createWALReader(backend);

      const entry = await reader.getEntry(15n);

      expect(entry).not.toBeNull();
      expect(entry!.lsn).toBe(15n);
    });
  });

  describe('iterate', () => {
    beforeEach(async () => {
      for (let seg = 0; seg < 3; seg++) {
        const startLSN = BigInt(seg * 5);
        await writeTestSegment(backend, {
          id: `seg_${startLSN.toString().padStart(20, '0')}`,
          startLSN,
          endLSN: startLSN + 4n,
          entries: makeTestEntries(5, startLSN),
          checksum: 0,
          createdAt: Date.now(),
        });
      }
    });

    it('should iterate over all entries', async () => {
      const reader = createWALReader(backend);

      const entries: WALEntry[] = [];
      for await (const entry of reader.iterate({})) {
        entries.push(entry);
      }

      expect(entries.length).toBe(15);
    });

    it('should iterate with filters', async () => {
      const reader = createWALReader(backend);

      const entries: WALEntry[] = [];
      for await (const entry of reader.iterate({ fromLSN: 5n, limit: 3 })) {
        entries.push(entry);
      }

      expect(entries.length).toBe(3);
      expect(entries[0].lsn).toBe(5n);
    });

    it('should stop early when limit reached', async () => {
      const reader = createWALReader(backend);

      const entries: WALEntry[] = [];
      for await (const entry of reader.iterate({ limit: 2 })) {
        entries.push(entry);
      }

      expect(entries.length).toBe(2);
    });

    it('should support breaking out of iteration', async () => {
      const reader = createWALReader(backend);

      const entries: WALEntry[] = [];
      for await (const entry of reader.iterate({})) {
        entries.push(entry);
        if (entries.length >= 5) break;
      }

      expect(entries.length).toBe(5);
    });
  });
});

// =============================================================================
// tailWAL Tests
// =============================================================================

describe('tailWAL', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should tail new entries as they are written', async () => {
    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);

    // Write initial entry
    await writer.append({
      timestamp: Date.now(),
      txnId: createTransactionId('txn_1'),
      op: 'INSERT',
      table: 'test',
      after: new Uint8Array([1]),
    }, { sync: true });

    // Write second entry before tailing (to ensure reliability)
    await writer.append({
      timestamp: Date.now(),
      txnId: createTransactionId('txn_2'),
      op: 'INSERT',
      table: 'test',
      after: new Uint8Array([2]),
    }, { sync: true });

    // Start tailing - entries already exist
    const entries: WALEntry[] = [];
    const tail = tailWAL(reader, {
      fromLSN: 0n,
      maxEntries: 2,
      timeout: 500,
      pollInterval: 50,
    });

    for await (const entry of tail) {
      entries.push(entry);
    }

    expect(entries.length).toBe(2);
    await writer.close();
  });

  it('should respect timeout', async () => {
    const reader = createWALReader(backend);

    const startTime = Date.now();
    const entries: WALEntry[] = [];

    for await (const entry of tailWAL(reader, {
      timeout: 200,
      pollInterval: 50,
    })) {
      entries.push(entry);
    }

    const elapsed = Date.now() - startTime;
    expect(elapsed).toBeGreaterThanOrEqual(200);
    expect(elapsed).toBeLessThan(500);
    expect(entries.length).toBe(0);
  });

  it('should respect maxEntries limit', async () => {
    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);

    // Write several entries
    for (let i = 0; i < 10; i++) {
      await writer.append({
        timestamp: Date.now(),
        txnId: createTransactionId(`txn_${i}`),
        op: 'INSERT',
        table: 'test',
        after: new Uint8Array([i]),
      }, { sync: true });
    }

    const entries: WALEntry[] = [];
    for await (const entry of tailWAL(reader, { maxEntries: 3, timeout: 1000 })) {
      entries.push(entry);
    }

    expect(entries.length).toBe(3);
    await writer.close();
  });
});

// =============================================================================
// readWALBatched Tests
// =============================================================================

describe('readWALBatched', () => {
  let backend: MemoryFSXBackend;

  beforeEach(async () => {
    backend = createMemoryBackend();

    // Write 25 entries across 5 segments
    for (let seg = 0; seg < 5; seg++) {
      const startLSN = BigInt(seg * 5);
      await writeTestSegment(backend, {
        id: `seg_${startLSN.toString().padStart(20, '0')}`,
        startLSN,
        endLSN: startLSN + 4n,
        entries: makeTestEntries(5, startLSN),
        checksum: 0,
        createdAt: Date.now(),
      });
    }
  });

  it('should read entries in batches', async () => {
    const reader = createWALReader(backend);

    const batches: WALEntry[][] = [];
    const result = await readWALBatched(
      reader,
      {},
      {
        batchSize: 7,
        onBatch: async (entries) => {
          batches.push([...entries]);
        },
      }
    );

    expect(result.totalEntries).toBe(25);
    expect(result.totalBatches).toBe(4); // 7 + 7 + 7 + 4
    expect(result.lastLSN).toBe(24n);

    expect(batches.length).toBe(4);
    expect(batches[0].length).toBe(7);
    expect(batches[3].length).toBe(4);
  });

  it('should return stats without onBatch callback', async () => {
    const reader = createWALReader(backend);

    const result = await readWALBatched(reader, {}, { batchSize: 10 });

    expect(result.totalEntries).toBe(25);
    expect(result.totalBatches).toBe(3);
  });

  it('should handle empty result', async () => {
    const reader = createWALReader(backend);

    const result = await readWALBatched(
      reader,
      { table: 'nonexistent' },
      { batchSize: 10 }
    );

    expect(result.totalEntries).toBe(0);
    expect(result.totalBatches).toBe(0);
    expect(result.lastLSN).toBeNull();
  });
});

// =============================================================================
// reconstructTransactions Tests
// =============================================================================

describe('reconstructTransactions', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should reconstruct committed transactions', async () => {
    const txn1 = createTransactionId('txn_1');
    const txn2 = createTransactionId('txn_2');

    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 7n,
      entries: [
        { lsn: 0n, timestamp: 1000, txnId: txn1, op: 'BEGIN', table: '' },
        { lsn: 1n, timestamp: 1001, txnId: txn1, op: 'INSERT', table: 'users', after: new Uint8Array([1]) },
        { lsn: 2n, timestamp: 1002, txnId: txn2, op: 'BEGIN', table: '' },
        { lsn: 3n, timestamp: 1003, txnId: txn1, op: 'INSERT', table: 'users', after: new Uint8Array([2]) },
        { lsn: 4n, timestamp: 1004, txnId: txn2, op: 'INSERT', table: 'orders', after: new Uint8Array([3]) },
        { lsn: 5n, timestamp: 1005, txnId: txn1, op: 'COMMIT', table: '' },
        { lsn: 6n, timestamp: 1006, txnId: txn2, op: 'INSERT', table: 'orders', after: new Uint8Array([4]) },
        { lsn: 7n, timestamp: 1007, txnId: txn2, op: 'COMMIT', table: '' },
      ],
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const transactions = await reconstructTransactions(reader);

    expect(transactions.length).toBe(2);

    const t1 = transactions.find(t => t.txnId === txn1);
    expect(t1).toBeDefined();
    expect(t1!.status).toBe('committed');
    expect(t1!.startLSN).toBe(0n);
    expect(t1!.endLSN).toBe(5n);
    expect(t1!.entries.length).toBe(4); // BEGIN + 2 INSERTs + COMMIT

    const t2 = transactions.find(t => t.txnId === txn2);
    expect(t2).toBeDefined();
    expect(t2!.status).toBe('committed');
    expect(t2!.entries.length).toBe(4);
  });

  it('should identify rolled back transactions', async () => {
    const txn1 = createTransactionId('txn_1');

    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 3n,
      entries: [
        { lsn: 0n, timestamp: 1000, txnId: txn1, op: 'BEGIN', table: '' },
        { lsn: 1n, timestamp: 1001, txnId: txn1, op: 'INSERT', table: 'users', after: new Uint8Array([1]) },
        { lsn: 2n, timestamp: 1002, txnId: txn1, op: 'INSERT', table: 'users', after: new Uint8Array([2]) },
        { lsn: 3n, timestamp: 1003, txnId: txn1, op: 'ROLLBACK', table: '' },
      ],
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const transactions = await reconstructTransactions(reader);

    expect(transactions.length).toBe(1);
    expect(transactions[0].status).toBe('rolledBack');
  });

  it('should identify incomplete transactions', async () => {
    const txn1 = createTransactionId('txn_1');

    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 2n,
      entries: [
        { lsn: 0n, timestamp: 1000, txnId: txn1, op: 'BEGIN', table: '' },
        { lsn: 1n, timestamp: 1001, txnId: txn1, op: 'INSERT', table: 'users', after: new Uint8Array([1]) },
        { lsn: 2n, timestamp: 1002, txnId: txn1, op: 'INSERT', table: 'users', after: new Uint8Array([2]) },
        // No COMMIT or ROLLBACK
      ],
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const transactions = await reconstructTransactions(reader);

    expect(transactions.length).toBe(1);
    expect(transactions[0].status).toBe('incomplete');
  });

  it('should sort transactions by startLSN', async () => {
    const txn1 = createTransactionId('txn_1');
    const txn2 = createTransactionId('txn_2');
    const txn3 = createTransactionId('txn_3');

    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 5n,
      entries: [
        { lsn: 0n, timestamp: 1000, txnId: txn2, op: 'BEGIN', table: '' },
        { lsn: 1n, timestamp: 1001, txnId: txn1, op: 'BEGIN', table: '' },
        { lsn: 2n, timestamp: 1002, txnId: txn3, op: 'BEGIN', table: '' },
        { lsn: 3n, timestamp: 1003, txnId: txn1, op: 'COMMIT', table: '' },
        { lsn: 4n, timestamp: 1004, txnId: txn2, op: 'COMMIT', table: '' },
        { lsn: 5n, timestamp: 1005, txnId: txn3, op: 'COMMIT', table: '' },
      ],
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const transactions = await reconstructTransactions(reader);

    expect(transactions[0].txnId).toBe(txn2); // Started at LSN 0
    expect(transactions[1].txnId).toBe(txn1); // Started at LSN 1
    expect(transactions[2].txnId).toBe(txn3); // Started at LSN 2
  });

  it('should handle transactions across multiple segments', async () => {
    const txn1 = createTransactionId('txn_spanning');

    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 1n,
      entries: [
        { lsn: 0n, timestamp: 1000, txnId: txn1, op: 'BEGIN', table: '' },
        { lsn: 1n, timestamp: 1001, txnId: txn1, op: 'INSERT', table: 'users', after: new Uint8Array([1]) },
      ],
      checksum: 0,
      createdAt: Date.now(),
    });

    await writeTestSegment(backend, {
      id: 'seg_00000000000000000002',
      startLSN: 2n,
      endLSN: 3n,
      entries: [
        { lsn: 2n, timestamp: 1002, txnId: txn1, op: 'INSERT', table: 'users', after: new Uint8Array([2]) },
        { lsn: 3n, timestamp: 1003, txnId: txn1, op: 'COMMIT', table: '' },
      ],
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const transactions = await reconstructTransactions(reader);

    expect(transactions.length).toBe(1);
    expect(transactions[0].status).toBe('committed');
    expect(transactions[0].entries.length).toBe(4);
    expect(transactions[0].startLSN).toBe(0n);
    expect(transactions[0].endLSN).toBe(3n);
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('WAL Reader Edge Cases', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should handle segment with single entry', async () => {
    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 0n,
      entries: makeTestEntries(1),
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const entries = await reader.readEntries({});

    expect(entries.length).toBe(1);
  });

  it('should handle very large LSN values', async () => {
    const largeLSN = 9007199254740992n;
    await writeTestSegment(backend, {
      id: `seg_${largeLSN.toString().padStart(20, '0')}`,
      startLSN: largeLSN,
      endLSN: largeLSN + 4n,
      entries: makeTestEntries(5, largeLSN),
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);
    const entry = await reader.getEntry(largeLSN + 2n);

    expect(entry).not.toBeNull();
    expect(entry!.lsn).toBe(largeLSN + 2n);
  });

  it('should handle gap in LSN sequence', async () => {
    // Segment 1: LSN 0-4
    await writeTestSegment(backend, {
      id: 'seg_00000000000000000000',
      startLSN: 0n,
      endLSN: 4n,
      entries: makeTestEntries(5, 0n),
      checksum: 0,
      createdAt: Date.now(),
    });

    // Segment 2: LSN 10-14 (gap from 5-9)
    await writeTestSegment(backend, {
      id: 'seg_00000000000000000010',
      startLSN: 10n,
      endLSN: 14n,
      entries: makeTestEntries(5, 10n),
      checksum: 0,
      createdAt: Date.now(),
    });

    const reader = createWALReader(backend);

    // Entry in gap should not exist
    const gapEntry = await reader.getEntry(7n);
    expect(gapEntry).toBeNull();

    // Entries before and after gap should exist
    const beforeGap = await reader.getEntry(4n);
    const afterGap = await reader.getEntry(10n);
    expect(beforeGap).not.toBeNull();
    expect(afterGap).not.toBeNull();
  });

  it('should read correctly after writer crash and restart', async () => {
    // Simulate writer crash scenario
    const writer1 = createWALWriter(backend);
    await writer1.append({
      timestamp: Date.now(),
      txnId: createTransactionId('txn_1'),
      op: 'INSERT',
      table: 'test',
      after: new Uint8Array([1]),
    }, { sync: true });
    // Writer crashes (no close)

    // New writer starts from latest LSN
    const reader = createWALReader(backend);
    const entries = await reader.readEntries({});
    const maxLSN = entries.reduce((max, e) => e.lsn > max ? e.lsn : max, 0n);

    const writer2 = createWALWriter(backend, maxLSN + 1n);
    await writer2.append({
      timestamp: Date.now(),
      txnId: createTransactionId('txn_2'),
      op: 'INSERT',
      table: 'test',
      after: new Uint8Array([2]),
    }, { sync: true });
    await writer2.close();

    // Reader should see both entries
    const allEntries = await reader.readEntries({});
    expect(allEntries.length).toBe(2);
  });
});
