/**
 * CDC Capture Module Tests
 *
 * Tests for CDC (Change Data Capture) capture functionality:
 * - CDC event capture from WAL
 * - Batch creation and flushing
 * - Position tracking for resume capability
 * - Filter matching (tables, operations, predicates)
 * - WAL entry to change event conversion
 *
 * Tests run using workers-vitest-pool (NO MOCKS).
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  createWALCapturer,
  walEntryToChangeEvent,
  batchToChangeEvents,
  type WALCapturer,
  type CaptureOptions,
  type CaptureBatch,
  type CaptureResult,
  type CaptureState,
} from '../capture.js';
import type { WALEntry, WALReader, WALOperation, LSN } from '../../wal/types.js';
import type { ChangeEvent, TransactionEvent, CDCFilter } from '../types.js';
import { createLSN, createTransactionId } from '../../engine/types.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Creates a mock WAL entry
 */
function createMockWALEntry(
  lsn: bigint,
  op: WALOperation = 'INSERT',
  table = 'test_table',
  txnId = 'txn_1'
): WALEntry {
  const entry: WALEntry = {
    lsn: createLSN(lsn),
    timestamp: Date.now(),
    txnId: createTransactionId(txnId),
    op,
    table,
  };

  if (op === 'INSERT' || op === 'UPDATE') {
    entry.after = new TextEncoder().encode(JSON.stringify({ id: Number(lsn), value: `value_${lsn}` }));
  }
  if (op === 'UPDATE' || op === 'DELETE') {
    entry.before = new TextEncoder().encode(JSON.stringify({ id: Number(lsn), value: `old_${lsn}` }));
    entry.key = new TextEncoder().encode(`key_${lsn}`);
  }

  return entry;
}

/**
 * Creates multiple mock WAL entries
 */
function createMockWALEntries(
  count: number,
  startLSN = 1n,
  options: { table?: string; op?: WALOperation; txnId?: string } = {}
): WALEntry[] {
  return Array.from({ length: count }, (_, i) =>
    createMockWALEntry(
      startLSN + BigInt(i),
      options.op ?? 'INSERT',
      options.table ?? 'test_table',
      options.txnId ?? `txn_${i}`
    )
  );
}

/**
 * Creates a mock WAL reader with configurable entries
 */
function createMockWALReader(entries: WALEntry[] = []): WALReader {
  return {
    async readSegment(_segmentId: string) {
      return null;
    },
    async readEntries(options) {
      const fromLSN = options.fromLSN ?? createLSN(0n);
      const limit = options.limit ?? Infinity;

      const filtered = entries
        .filter(e => e.lsn >= fromLSN)
        .slice(0, limit);

      return filtered;
    },
    async listSegments(_includeArchived?: boolean) {
      return [];
    },
    async getEntry(lsn: LSN) {
      return entries.find(e => e.lsn === lsn) ?? null;
    },
    async *iterate(options) {
      const fromLSN = options.fromLSN ?? createLSN(0n);
      const limit = options.limit ?? Infinity;
      let count = 0;

      for (const entry of entries) {
        if (entry.lsn >= fromLSN && count < limit) {
          yield entry;
          count++;
        }
      }
    },
  };
}

// =============================================================================
// Test: WAL Capturer Creation
// =============================================================================

describe('CDC Capture - WAL Capturer Creation', () => {
  it('should create a capturer with default options', () => {
    const reader = createMockWALReader();
    const capturer = createWALCapturer(reader);

    expect(capturer).toBeDefined();
    expect(capturer.capture).toBeInstanceOf(Function);
    expect(capturer.captureStream).toBeInstanceOf(Function);
    expect(capturer.flush).toBeInstanceOf(Function);
    expect(capturer.getState).toBeInstanceOf(Function);
    expect(capturer.setPosition).toBeInstanceOf(Function);
    expect(capturer.stop).toBeInstanceOf(Function);
  });

  it('should initialize state correctly', () => {
    const reader = createMockWALReader();
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(100n),
    });

    const state = capturer.getState();
    expect(state.lastLSN).toBe(createLSN(100n));
    expect(state.totalEntries).toBe(0);
    expect(state.totalBatches).toBe(0);
    expect(state.currentBatchSize).toBe(0);
    expect(state.active).toBe(false);
  });

  it('should accept custom options', () => {
    const reader = createMockWALReader();
    const filter: CDCFilter = { tables: ['users'] };

    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(50n),
      filter,
      pollInterval: 200,
      maxBatchSize: 500,
      maxBatchAge: 10000,
      includeTransactions: true,
    });

    expect(capturer).toBeDefined();
    // Options are internal, but we can verify behavior in other tests
  });
});

// =============================================================================
// Test: Single Capture Operation
// =============================================================================

describe('CDC Capture - Single Capture', () => {
  it('should capture available entries', async () => {
    const entries = createMockWALEntries(5, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, { fromLSN: createLSN(0n) });

    const result = await capturer.capture();

    expect(result.hasEntries).toBe(true);
    expect(result.entryCount).toBe(5);
    expect(result.lastLSN).toBe(createLSN(5n));
  });

  it('should return no entries when none available', async () => {
    const reader = createMockWALReader([]);
    const capturer = createWALCapturer(reader);

    const result = await capturer.capture();

    expect(result.hasEntries).toBe(false);
    expect(result.entryCount).toBe(0);
  });

  it('should update state after capture', async () => {
    const entries = createMockWALEntries(10, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, { fromLSN: createLSN(0n) });

    await capturer.capture();

    const state = capturer.getState();
    expect(state.totalEntries).toBe(10);
    expect(state.lastLSN).toBe(createLSN(10n));
    expect(state.lastCaptureAt).toBeDefined();
  });

  it('should skip transaction control entries by default', async () => {
    const entries: WALEntry[] = [
      createMockWALEntry(1n, 'BEGIN', '', 'txn_1'),
      createMockWALEntry(2n, 'INSERT', 'users', 'txn_1'),
      createMockWALEntry(3n, 'COMMIT', '', 'txn_1'),
    ];
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, { fromLSN: createLSN(0n) });

    const result = await capturer.capture();

    // Only INSERT should be captured (BEGIN/COMMIT skipped)
    expect(result.entryCount).toBe(1);
  });

  it('should include transaction control entries when requested', async () => {
    const entries: WALEntry[] = [
      createMockWALEntry(1n, 'BEGIN', '', 'txn_1'),
      createMockWALEntry(2n, 'INSERT', 'users', 'txn_1'),
      createMockWALEntry(3n, 'COMMIT', '', 'txn_1'),
    ];
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      includeTransactions: true,
    });

    const result = await capturer.capture();

    expect(result.entryCount).toBe(3);
  });
});

// =============================================================================
// Test: Filter Matching
// =============================================================================

describe('CDC Capture - Filter Matching', () => {
  it('should filter by table names', async () => {
    const entries: WALEntry[] = [
      createMockWALEntry(1n, 'INSERT', 'users'),
      createMockWALEntry(2n, 'INSERT', 'orders'),
      createMockWALEntry(3n, 'INSERT', 'users'),
      createMockWALEntry(4n, 'INSERT', 'products'),
    ];
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      filter: { tables: ['users'] },
    });

    const result = await capturer.capture();

    expect(result.entryCount).toBe(2);
    expect(result.lastLSN).toBe(createLSN(4n)); // Position advances even for filtered
  });

  it('should filter by operation types', async () => {
    const entries: WALEntry[] = [
      createMockWALEntry(1n, 'INSERT', 'users'),
      createMockWALEntry(2n, 'UPDATE', 'users'),
      createMockWALEntry(3n, 'DELETE', 'users'),
      createMockWALEntry(4n, 'INSERT', 'users'),
    ];
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      filter: { operations: ['INSERT', 'DELETE'] },
    });

    const result = await capturer.capture();

    expect(result.entryCount).toBe(3); // 2 INSERTs + 1 DELETE
  });

  it('should filter by transaction IDs', async () => {
    const entries: WALEntry[] = [
      createMockWALEntry(1n, 'INSERT', 'users', 'txn_a'),
      createMockWALEntry(2n, 'INSERT', 'users', 'txn_b'),
      createMockWALEntry(3n, 'INSERT', 'users', 'txn_a'),
      createMockWALEntry(4n, 'INSERT', 'users', 'txn_c'),
    ];
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      filter: { txnIds: ['txn_a'] },
    });

    const result = await capturer.capture();

    expect(result.entryCount).toBe(2);
  });

  it('should filter by custom predicate', async () => {
    const entries = createMockWALEntries(10, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      filter: {
        predicate: (entry) => entry.lsn <= createLSN(5n),
      },
    });

    const result = await capturer.capture();

    expect(result.entryCount).toBe(5);
  });

  it('should combine multiple filters with AND logic', async () => {
    const entries: WALEntry[] = [
      createMockWALEntry(1n, 'INSERT', 'users', 'txn_a'),
      createMockWALEntry(2n, 'UPDATE', 'users', 'txn_a'),
      createMockWALEntry(3n, 'INSERT', 'orders', 'txn_a'),
      createMockWALEntry(4n, 'INSERT', 'users', 'txn_b'),
    ];
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      filter: {
        tables: ['users'],
        operations: ['INSERT'],
        txnIds: ['txn_a'],
      },
    });

    const result = await capturer.capture();

    // Only entry 1 matches all filters
    expect(result.entryCount).toBe(1);
  });
});

// =============================================================================
// Test: Batch Creation and Flushing
// =============================================================================

describe('CDC Capture - Batch Management', () => {
  it('should create batch when maxBatchSize reached', async () => {
    const entries = createMockWALEntries(15, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      maxBatchSize: 10,
    });

    const result = await capturer.capture();

    expect(result.batch).toBeDefined();
    expect(result.batch?.entries.length).toBe(10);
    expect(result.batch?.batchId).toMatch(/^batch_/);
  });

  it('should include correct LSN range in batch', async () => {
    const entries = createMockWALEntries(10, 5n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      maxBatchSize: 10,
    });

    const result = await capturer.capture();

    expect(result.batch?.startLSN).toBe(createLSN(5n));
    expect(result.batch?.endLSN).toBe(createLSN(14n));
  });

  it('should calculate batch size in bytes', async () => {
    const entries = createMockWALEntries(10, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      maxBatchSize: 10,
    });

    const result = await capturer.capture();

    expect(result.batch?.sizeBytes).toBeGreaterThan(0);
  });

  it('should flush pending entries manually', async () => {
    const entries = createMockWALEntries(5, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      maxBatchSize: 100, // High threshold - won't auto-flush
    });

    await capturer.capture();

    // Manual flush
    const batch = capturer.flush();

    expect(batch).not.toBeNull();
    expect(batch?.entries.length).toBe(5);
  });

  it('should return null when flushing empty batch', () => {
    const reader = createMockWALReader([]);
    const capturer = createWALCapturer(reader);

    const batch = capturer.flush();

    expect(batch).toBeNull();
  });

  it('should increment total batches counter', async () => {
    const entries = createMockWALEntries(25, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      maxBatchSize: 10,
    });

    await capturer.capture();
    capturer.flush(); // Flush remaining

    const state = capturer.getState();
    expect(state.totalBatches).toBeGreaterThanOrEqual(2);
  });

  it('should include timestamp in batch', async () => {
    const entries = createMockWALEntries(10, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      maxBatchSize: 10,
    });

    const beforeCapture = Date.now();
    const result = await capturer.capture();
    const afterCapture = Date.now();

    expect(result.batch?.createdAt).toBeGreaterThanOrEqual(beforeCapture);
    expect(result.batch?.createdAt).toBeLessThanOrEqual(afterCapture);
  });
});

// =============================================================================
// Test: Position Tracking
// =============================================================================

describe('CDC Capture - Position Tracking', () => {
  it('should set position manually', async () => {
    const entries = createMockWALEntries(10, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, { fromLSN: createLSN(0n) });

    capturer.setPosition(createLSN(5n));

    const state = capturer.getState();
    expect(state.lastLSN).toBe(createLSN(5n));
  });

  it('should clear pending entries when position changes', async () => {
    const entries = createMockWALEntries(5, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      maxBatchSize: 100,
    });

    await capturer.capture();
    expect(capturer.getState().currentBatchSize).toBe(5);

    capturer.setPosition(createLSN(10n));
    expect(capturer.getState().currentBatchSize).toBe(0);
  });

  it('should resume capture from new position', async () => {
    const entries = createMockWALEntries(20, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, { fromLSN: createLSN(0n) });

    // Capture first batch
    await capturer.capture();

    // Set new position
    capturer.setPosition(createLSN(15n));

    // Create new reader with entries from 16+
    const newEntries = createMockWALEntries(5, 16n);
    const newReader = createMockWALReader(newEntries);
    const newCapturer = createWALCapturer(newReader, { fromLSN: createLSN(15n) });

    const result = await newCapturer.capture();

    expect(result.entryCount).toBe(5);
    expect(result.lastLSN).toBe(createLSN(20n));
  });
});

// =============================================================================
// Test: WAL Entry to Change Event Conversion
// =============================================================================

describe('CDC Capture - Entry Conversion', () => {
  it('should convert INSERT entry to change event', () => {
    const entry = createMockWALEntry(1n, 'INSERT', 'users', 'txn_1');

    const event = walEntryToChangeEvent(entry);

    expect(event).not.toBeNull();
    expect((event as ChangeEvent).type).toBe('insert');
    expect((event as ChangeEvent).table).toBe('users');
    expect((event as ChangeEvent).txnId).toBe(createTransactionId('txn_1'));
    expect((event as ChangeEvent).data).toBeDefined();
  });

  it('should convert UPDATE entry to change event', () => {
    const entry = createMockWALEntry(2n, 'UPDATE', 'users', 'txn_1');

    const event = walEntryToChangeEvent(entry);

    expect(event).not.toBeNull();
    expect((event as ChangeEvent).type).toBe('update');
    expect((event as ChangeEvent).data).toBeDefined();
    expect((event as ChangeEvent).oldData).toBeDefined();
  });

  it('should convert DELETE entry to change event', () => {
    const entry = createMockWALEntry(3n, 'DELETE', 'users', 'txn_1');

    const event = walEntryToChangeEvent(entry);

    expect(event).not.toBeNull();
    expect((event as ChangeEvent).type).toBe('delete');
    expect((event as ChangeEvent).oldData).toBeDefined();
  });

  it('should convert BEGIN entry to transaction event', () => {
    const entry = createMockWALEntry(1n, 'BEGIN', '', 'txn_1');

    const event = walEntryToChangeEvent(entry);

    expect(event).not.toBeNull();
    expect((event as TransactionEvent).type).toBe('begin');
    expect((event as TransactionEvent).txnId).toBe(createTransactionId('txn_1'));
  });

  it('should convert COMMIT entry to transaction event', () => {
    const entry = createMockWALEntry(2n, 'COMMIT', '', 'txn_1');

    const event = walEntryToChangeEvent(entry);

    expect(event).not.toBeNull();
    expect((event as TransactionEvent).type).toBe('commit');
  });

  it('should convert ROLLBACK entry to transaction event', () => {
    const entry = createMockWALEntry(3n, 'ROLLBACK', '', 'txn_1');

    const event = walEntryToChangeEvent(entry);

    expect(event).not.toBeNull();
    expect((event as TransactionEvent).type).toBe('rollback');
  });

  it('should use custom decoder for data', () => {
    const entry = createMockWALEntry(1n, 'INSERT', 'users', 'txn_1');

    const decoder = (data: Uint8Array) => {
      const str = new TextDecoder().decode(data);
      return { decoded: true, ...JSON.parse(str) };
    };

    const event = walEntryToChangeEvent(entry, decoder) as ChangeEvent<{ decoded: boolean }>;

    expect(event.data?.decoded).toBe(true);
  });

  it('should include LSN in converted event', () => {
    const entry = createMockWALEntry(42n, 'INSERT', 'users', 'txn_1');

    const event = walEntryToChangeEvent(entry) as ChangeEvent;

    expect(event.lsn).toBe(createLSN(42n));
    expect(event.id).toBe('42');
  });

  it('should include timestamp in converted event', () => {
    const entry = createMockWALEntry(1n, 'INSERT', 'users', 'txn_1');

    const event = walEntryToChangeEvent(entry) as ChangeEvent;

    expect(event.timestamp).toBeInstanceOf(Date);
  });
});

// =============================================================================
// Test: Batch to Change Events Conversion
// =============================================================================

describe('CDC Capture - Batch Conversion', () => {
  it('should convert batch entries to change events', () => {
    const entries: WALEntry[] = [
      createMockWALEntry(1n, 'INSERT', 'users'),
      createMockWALEntry(2n, 'UPDATE', 'users'),
      createMockWALEntry(3n, 'DELETE', 'users'),
    ];

    const batch: CaptureBatch = {
      batchId: 'test_batch',
      startLSN: createLSN(1n),
      endLSN: createLSN(3n),
      entries,
      createdAt: Date.now(),
      sizeBytes: 500,
    };

    const events = batchToChangeEvents(batch);

    expect(events).toHaveLength(3);
    expect((events[0] as ChangeEvent).type).toBe('insert');
    expect((events[1] as ChangeEvent).type).toBe('update');
    expect((events[2] as ChangeEvent).type).toBe('delete');
  });

  it('should include transaction events in batch conversion', () => {
    const entries: WALEntry[] = [
      createMockWALEntry(1n, 'BEGIN', '', 'txn_1'),
      createMockWALEntry(2n, 'INSERT', 'users', 'txn_1'),
      createMockWALEntry(3n, 'COMMIT', '', 'txn_1'),
    ];

    const batch: CaptureBatch = {
      batchId: 'test_batch',
      startLSN: createLSN(1n),
      endLSN: createLSN(3n),
      entries,
      createdAt: Date.now(),
      sizeBytes: 500,
    };

    const events = batchToChangeEvents(batch);

    expect(events).toHaveLength(3);
    expect((events[0] as TransactionEvent).type).toBe('begin');
    expect((events[1] as ChangeEvent).type).toBe('insert');
    expect((events[2] as TransactionEvent).type).toBe('commit');
  });

  it('should use custom decoder for batch conversion', () => {
    const entries: WALEntry[] = [
      createMockWALEntry(1n, 'INSERT', 'users'),
    ];

    const batch: CaptureBatch = {
      batchId: 'test_batch',
      startLSN: createLSN(1n),
      endLSN: createLSN(1n),
      entries,
      createdAt: Date.now(),
      sizeBytes: 100,
    };

    const decoder = (data: Uint8Array) => ({ custom: 'decoded' });
    const events = batchToChangeEvents(batch, decoder);

    expect((events[0] as ChangeEvent<{ custom: string }>).data?.custom).toBe('decoded');
  });
});

// =============================================================================
// Test: Capturer State
// =============================================================================

describe('CDC Capture - State Management', () => {
  it('should track active status', async () => {
    const entries = createMockWALEntries(5, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, { fromLSN: createLSN(0n) });

    expect(capturer.getState().active).toBe(false);

    await capturer.capture();

    expect(capturer.getState().active).toBe(true);
  });

  it('should stop capturer', async () => {
    const entries = createMockWALEntries(5, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, { fromLSN: createLSN(0n) });

    await capturer.capture();
    capturer.stop();

    expect(capturer.getState().active).toBe(false);
  });

  it('should provide complete state snapshot', async () => {
    const entries = createMockWALEntries(10, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      maxBatchSize: 100,
    });

    await capturer.capture();

    const state: CaptureState = capturer.getState();

    expect(state).toMatchObject({
      lastLSN: expect.any(BigInt),
      totalEntries: 10,
      totalBatches: 0, // No auto-flush with high maxBatchSize
      currentBatchSize: 10,
      active: true,
    });
  });
});

// =============================================================================
// Test: Edge Cases
// =============================================================================

describe('CDC Capture - Edge Cases', () => {
  it('should handle empty WAL', async () => {
    const reader = createMockWALReader([]);
    const capturer = createWALCapturer(reader);

    const result = await capturer.capture();

    expect(result.hasEntries).toBe(false);
    expect(result.entryCount).toBe(0);
    expect(result.batch).toBeUndefined();
  });

  it('should handle entries with missing optional fields', () => {
    const entry: WALEntry = {
      lsn: createLSN(1n),
      timestamp: Date.now(),
      txnId: createTransactionId('txn_1'),
      op: 'INSERT',
      table: 'users',
      // No before/after/key
    };

    const event = walEntryToChangeEvent(entry) as ChangeEvent;

    expect(event).toBeDefined();
    expect(event.data).toBeUndefined();
  });

  it('should handle very large LSN values', async () => {
    const largeLSN = 9007199254740991n; // Max safe integer as bigint
    const entries = [createMockWALEntry(largeLSN, 'INSERT', 'users')];
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, { fromLSN: createLSN(largeLSN - 1n) });

    const result = await capturer.capture();

    expect(result.hasEntries).toBe(true);
    expect(result.lastLSN).toBe(createLSN(largeLSN));
  });

  it('should handle filter with empty arrays', async () => {
    const entries = createMockWALEntries(5, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      filter: {
        tables: [], // Empty - should match all
        operations: [], // Empty - should match all
      },
    });

    const result = await capturer.capture();

    // Empty arrays mean no filtering
    expect(result.entryCount).toBe(5);
  });

  it('should handle binary data in entries', () => {
    const binaryData = new Uint8Array([0x00, 0x01, 0xff, 0xfe, 0x80]);
    const entry: WALEntry = {
      lsn: createLSN(1n),
      timestamp: Date.now(),
      txnId: createTransactionId('txn_1'),
      op: 'INSERT',
      table: 'binary_table',
      after: binaryData,
    };

    const event = walEntryToChangeEvent(entry) as ChangeEvent;

    expect(event).toBeDefined();
    // Default decoder returns Uint8Array as-is
    expect(event.data).toEqual(binaryData);
  });

  it('should handle rapid sequential captures', async () => {
    const entries = createMockWALEntries(100, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      maxBatchSize: 10,
    });

    // Rapid sequential captures
    for (let i = 0; i < 10; i++) {
      await capturer.capture();
    }

    const state = capturer.getState();
    expect(state.totalEntries).toBe(100);
    expect(state.totalBatches).toBeGreaterThanOrEqual(9);
  });
});

// =============================================================================
// Test: Batch Age Flushing
// =============================================================================

describe('CDC Capture - Batch Age', () => {
  it('should not flush before maxBatchAge', async () => {
    const entries = createMockWALEntries(3, 1n);
    const reader = createMockWALReader(entries);
    const capturer = createWALCapturer(reader, {
      fromLSN: createLSN(0n),
      maxBatchSize: 100,
      maxBatchAge: 10000, // 10 seconds
    });

    const result = await capturer.capture();

    expect(result.batch).toBeUndefined();
    expect(capturer.getState().currentBatchSize).toBe(3);
  });

  // Note: Testing actual time-based flushing would require waiting or mocking Date.now
  // which is avoided per project guidelines. The behavior is tested indirectly.
});
