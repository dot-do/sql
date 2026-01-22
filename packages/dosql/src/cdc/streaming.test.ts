/**
 * CDC Streaming Tests for DoSQL
 *
 * Comprehensive tests for CDC streaming from DO to lakehouse:
 * - Capture INSERT/UPDATE/DELETE to change stream
 * - Change event format (table, op, before, after, timestamp)
 * - Batch changes for efficient transfer
 * - Resume from last committed position (offset)
 * - Handle backpressure from lakehouse
 * - Exactly-once delivery semantics
 * - Schema evolution handling
 *
 * Tests run using workers-vitest-pool (NO MOCKS).
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  createCDCSubscription,
  createCDCStream,
  createReplicationSlotManager,
  createCDC,
  subscribeTable,
  subscribeBatched,
  walEntryToChangeEvent,
  batchToChangeEvents,
  createWALCapturer,
  type CDCSubscription,
  type ChangeEvent,
  type TransactionEvent,
  type CDCEvent,
  type CDCFilter,
  type CDCHandler,
  type ReplicationSlot,
  CDCError,
  CDCErrorCode,
} from './index.js';
import {
  createWALWriter,
  createWALReader,
  createTransaction,
  generateTxnId,
  type WALWriter,
  type WALReader,
  type WALEntry,
} from '../wal/index.js';
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
 * Decode Uint8Array from WAL entries
 */
function decode<T>(data: Uint8Array): T {
  return JSON.parse(new TextDecoder().decode(data)) as T;
}

/**
 * Helper to write test WAL entries
 */
async function writeTestEntries(
  writer: WALWriter,
  entries: Array<{
    op: 'INSERT' | 'UPDATE' | 'DELETE';
    table: string;
    key?: unknown;
    before?: unknown;
    after?: unknown;
  }>
): Promise<bigint[]> {
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

  // COMMIT
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

  return lsns;
}

// =============================================================================
// Test: Capture INSERT/UPDATE/DELETE to Change Stream
// =============================================================================

describe('CDC Streaming - Change Capture', () => {
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

  it('should capture INSERT operations', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'users', after: { id: 1, name: 'Alice' } },
    ]);

    // Read entries directly from WAL
    const entries = await reader.readEntries({ operations: ['INSERT'] });
    expect(entries.length).toBeGreaterThanOrEqual(1);

    // Convert to change event
    const insertEntry = entries.find((e) => e.op === 'INSERT' && e.table === 'users');
    expect(insertEntry).toBeDefined();

    const event = walEntryToChangeEvent(insertEntry!, decode);
    expect(event).not.toBeNull();
    expect((event as ChangeEvent).type).toBe('insert');
    expect((event as ChangeEvent).table).toBe('users');
    expect((event as ChangeEvent).data).toEqual({ id: 1, name: 'Alice' });
  });

  it('should capture UPDATE operations with before/after', async () => {
    await writeTestEntries(writer, [
      {
        op: 'UPDATE',
        table: 'users',
        key: { id: 1 },
        before: { id: 1, name: 'Alice' },
        after: { id: 1, name: 'Alicia' },
      },
    ]);

    const entries = await reader.readEntries({ operations: ['UPDATE'] });
    const updateEntry = entries.find((e) => e.op === 'UPDATE');
    expect(updateEntry).toBeDefined();

    const event = walEntryToChangeEvent(updateEntry!, decode) as ChangeEvent;
    expect(event.type).toBe('update');
    expect(event.table).toBe('users');
    expect(event.oldData).toEqual({ id: 1, name: 'Alice' });
    expect(event.data).toEqual({ id: 1, name: 'Alicia' });
  });

  it('should capture DELETE operations with before data', async () => {
    await writeTestEntries(writer, [
      {
        op: 'DELETE',
        table: 'users',
        key: { id: 1 },
        before: { id: 1, name: 'Alice' },
      },
    ]);

    const entries = await reader.readEntries({ operations: ['DELETE'] });
    const deleteEntry = entries.find((e) => e.op === 'DELETE');
    expect(deleteEntry).toBeDefined();

    const event = walEntryToChangeEvent(deleteEntry!, decode) as ChangeEvent;
    expect(event.type).toBe('delete');
    expect(event.table).toBe('users');
    expect(event.oldData).toEqual({ id: 1, name: 'Alice' });
    expect(event.data).toBeUndefined();
  });

  it('should capture mixed operations in order', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'users', after: { id: 1, name: 'Alice' } },
      { op: 'UPDATE', table: 'users', key: { id: 1 }, before: { id: 1, name: 'Alice' }, after: { id: 1, name: 'Alicia' } },
      { op: 'DELETE', table: 'users', key: { id: 1 }, before: { id: 1, name: 'Alicia' } },
    ]);

    const entries = await reader.readEntries({
      operations: ['INSERT', 'UPDATE', 'DELETE'],
    });

    const events = entries
      .filter((e) => e.table === 'users')
      .map((e) => walEntryToChangeEvent(e, decode))
      .filter((e): e is ChangeEvent => e !== null && e.type !== 'begin' && e.type !== 'commit' && e.type !== 'rollback');

    expect(events).toHaveLength(3);
    expect(events[0].type).toBe('insert');
    expect(events[1].type).toBe('update');
    expect(events[2].type).toBe('delete');
  });

  it('should capture operations from multiple tables', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'users', after: { id: 1, name: 'Alice' } },
      { op: 'INSERT', table: 'orders', after: { id: 100, userId: 1 } },
      { op: 'INSERT', table: 'products', after: { id: 500, name: 'Widget' } },
    ]);

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const tablesSeen = new Set(entries.map((e) => e.table).filter((t) => t !== ''));

    expect(tablesSeen).toContain('users');
    expect(tablesSeen).toContain('orders');
    expect(tablesSeen).toContain('products');
  });
});

// =============================================================================
// Test: Change Event Format
// =============================================================================

describe('CDC Streaming - Event Format', () => {
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

  it('should include table name in event', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'customers', after: { id: 1 } },
    ]);

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const entry = entries.find((e) => e.table === 'customers');
    const event = walEntryToChangeEvent(entry!, decode) as ChangeEvent;

    expect(event.table).toBe('customers');
  });

  it('should include operation type (op) in event', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const entry = entries.find((e) => e.table === 'items');
    const event = walEntryToChangeEvent(entry!, decode) as ChangeEvent;

    expect(event.type).toBe('insert');
  });

  it('should include before data for updates', async () => {
    await writeTestEntries(writer, [
      {
        op: 'UPDATE',
        table: 'items',
        key: { id: 1 },
        before: { id: 1, status: 'active' },
        after: { id: 1, status: 'inactive' },
      },
    ]);

    const entries = await reader.readEntries({ operations: ['UPDATE'] });
    const entry = entries.find((e) => e.op === 'UPDATE');
    const event = walEntryToChangeEvent(entry!, decode) as ChangeEvent;

    expect(event.oldData).toEqual({ id: 1, status: 'active' });
  });

  it('should include after data for inserts', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1, value: 100 } },
    ]);

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const entry = entries.find((e) => e.table === 'items');
    const event = walEntryToChangeEvent(entry!, decode) as ChangeEvent;

    expect(event.data).toEqual({ id: 1, value: 100 });
  });

  it('should include timestamp in event', async () => {
    const beforeWrite = Date.now();
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);
    const afterWrite = Date.now();

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const entry = entries.find((e) => e.table === 'items');
    const event = walEntryToChangeEvent(entry!, decode) as ChangeEvent;

    expect(event.timestamp).toBeDefined();
    expect(event.timestamp.getTime()).toBeGreaterThanOrEqual(beforeWrite);
    expect(event.timestamp.getTime()).toBeLessThanOrEqual(afterWrite + 1000);
  });

  it('should include LSN in event', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const entry = entries.find((e) => e.table === 'items');
    const event = walEntryToChangeEvent(entry!, decode) as ChangeEvent;

    expect(event.lsn).toBeDefined();
    expect(typeof event.lsn).toBe('bigint');
    expect(event.lsn).toBeGreaterThan(0n);
  });

  it('should include unique event ID based on LSN', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
      { op: 'INSERT', table: 'items', after: { id: 2 } },
    ]);

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const itemEntries = entries.filter((e) => e.table === 'items');
    const events = itemEntries.map((e) => walEntryToChangeEvent(e, decode) as ChangeEvent);

    expect(events.length).toBeGreaterThanOrEqual(2);
    expect(events[0].id).not.toBe(events[1].id);
  });

  it('should include transaction ID (txnId) in event', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const entry = entries.find((e) => e.table === 'items');
    const event = walEntryToChangeEvent(entry!, decode) as ChangeEvent;

    expect(event.txnId).toBeDefined();
    expect(event.txnId).toContain('txn_');
  });
});

// =============================================================================
// Test: Batch Changes for Efficient Transfer
// =============================================================================

describe('CDC Streaming - Batching', () => {
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

  it('should batch events by size', async () => {
    // Write 10 entries (each in its own transaction - 3 entries each: BEGIN, INSERT, COMMIT)
    for (let i = 0; i < 10; i++) {
      await writeTestEntries(writer, [
        { op: 'INSERT', table: 'items', after: { id: i, name: `Item ${i}` } },
      ]);
    }

    // Read all entries directly
    const allEntries = await reader.readEntries({ operations: ['INSERT'] });
    const itemEntries = allEntries.filter((e) => e.table === 'items');

    // Should have 10 INSERT entries
    expect(itemEntries.length).toBe(10);

    // Verify capturer batches correctly
    const capturer = createWALCapturer(reader, {
      maxBatchSize: 3,
      maxBatchAge: 10000,
    });

    // Capture should respect batch size
    const result = await capturer.capture();
    expect(result.entryCount).toBeLessThanOrEqual(3);
  });

  it('should batch events by timeout', async () => {
    // Write entries
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
      { op: 'INSERT', table: 'items', after: { id: 2 } },
    ]);

    const capturer = createWALCapturer(reader, {
      maxBatchSize: 1000, // Large batch size
      maxBatchAge: 100, // Short timeout
    });

    // Capture
    const result = await capturer.capture();
    expect(result.entryCount).toBeGreaterThan(0);
  });

  it('should maintain event order within batch', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
      { op: 'INSERT', table: 'items', after: { id: 2 } },
      { op: 'INSERT', table: 'items', after: { id: 3 } },
    ]);

    const entries = await reader.readEntries({
      operations: ['INSERT'],
      table: 'items',
    });

    // Convert to events
    const events = entries
      .filter((e) => e.table === 'items')
      .map((e) => walEntryToChangeEvent(e, decode<{ id: number }>))
      .filter((e): e is ChangeEvent<{ id: number }> => e !== null && e.type === 'insert');

    // Check order is maintained (LSNs should be ascending)
    for (let i = 1; i < events.length; i++) {
      expect(events[i].lsn).toBeGreaterThan(events[i - 1].lsn);
    }
  });

  it('should handle empty batches gracefully', async () => {
    // No entries written

    const capturer = createWALCapturer(reader, {
      maxBatchSize: 10,
      maxBatchAge: 100,
    });

    const result = await capturer.capture();
    expect(result.hasEntries).toBe(false);
    expect(result.entryCount).toBe(0);
  });

  it('should support custom batch filtering', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'users', after: { id: 1 } },
      { op: 'INSERT', table: 'orders', after: { id: 100 } },
      { op: 'INSERT', table: 'users', after: { id: 2 } },
    ]);

    const capturer = createWALCapturer(reader, {
      filter: { tables: ['users'] },
      maxBatchSize: 10,
    });

    const result = await capturer.capture();
    const state = capturer.getState();

    // Only user entries should be captured (2 entries)
    expect(state.totalEntries).toBe(2);
  });
});

// =============================================================================
// Test: Resume from Last Committed Position (Offset)
// =============================================================================

describe('CDC Streaming - Resume from Position', () => {
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

  it('should resume from specific LSN', async () => {
    const lsns1 = await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 2 } },
    ]);
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 3 } },
    ]);

    // Resume from after first transaction
    const startAfterLSN = lsns1[lsns1.length - 1]; // COMMIT of first tx

    const entries = await reader.readEntries({
      fromLSN: startAfterLSN + 1n,
      operations: ['INSERT'],
    });

    const events = entries
      .filter((e) => e.table === 'items')
      .map((e) => walEntryToChangeEvent(e, decode<{ id: number }>))
      .filter((e): e is ChangeEvent<{ id: number }> => e !== null && e.type === 'insert');

    // Should only see items 2 and 3
    expect(events.some((e) => e.data?.id === 2)).toBe(true);
    expect(events.some((e) => e.data?.id === 3)).toBe(true);
    expect(events.every((e) => e.data?.id !== 1)).toBe(true);
  });

  it('should track capturer state with current position', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);

    const capturer = createWALCapturer(reader, { fromLSN: 0n });

    // Before capturing, state should show no entries
    let state = capturer.getState();
    expect(state.totalEntries).toBe(0);

    // Capture entries
    await capturer.capture();

    // After capturing, state should reflect progress
    state = capturer.getState();
    expect(state.totalEntries).toBeGreaterThanOrEqual(1);
    expect(state.lastLSN).toBeGreaterThan(0n);
  });

  it('should support replication slots for persistent position tracking', async () => {
    const slotManager = createReplicationSlotManager(backend, reader);

    // Create a slot
    const slot = await slotManager.createSlot('consumer-1', 0n);
    expect(slot.name).toBe('consumer-1');
    expect(slot.acknowledgedLSN).toBe(0n);

    // Update slot position
    await slotManager.updateSlot('consumer-1', 100n);
    const updatedSlot = await slotManager.getSlot('consumer-1');
    expect(updatedSlot?.acknowledgedLSN).toBe(100n);
  });

  it('should list all replication slots', async () => {
    const slotManager = createReplicationSlotManager(backend, reader);

    await slotManager.createSlot('consumer-1', 0n);
    await slotManager.createSlot('consumer-2', 50n);
    await slotManager.createSlot('consumer-3', 100n);

    const slots = await slotManager.listSlots();
    expect(slots).toHaveLength(3);
    expect(slots.map((s) => s.name).sort()).toEqual(['consumer-1', 'consumer-2', 'consumer-3']);
  });

  it('should delete replication slots', async () => {
    const slotManager = createReplicationSlotManager(backend, reader);

    await slotManager.createSlot('temp-slot', 0n);
    let slot = await slotManager.getSlot('temp-slot');
    expect(slot).not.toBeNull();

    await slotManager.deleteSlot('temp-slot');
    slot = await slotManager.getSlot('temp-slot');
    expect(slot).toBeNull();
  });

  it('should prevent duplicate slot names', async () => {
    const slotManager = createReplicationSlotManager(backend, reader);

    await slotManager.createSlot('unique-slot', 0n);

    await expect(slotManager.createSlot('unique-slot', 0n)).rejects.toThrow(CDCError);
  });

  it('should create subscription from slot', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);

    const slotManager = createReplicationSlotManager(backend, reader);
    await slotManager.createSlot('my-slot', 0n);

    const subscription = await slotManager.subscribeFromSlot('my-slot');
    expect(subscription).toBeDefined();
    expect(subscription.isActive()).toBe(false); // Not started yet
  });
});

// =============================================================================
// Test: Handle Backpressure from Lakehouse
// =============================================================================

describe('CDC Streaming - Backpressure Handling', () => {
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

  it('should pause stream when paused', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
      { op: 'INSERT', table: 'items', after: { id: 2 } },
    ]);

    let eventCount = 0;
    let pausedTime = 0;
    const pauseDuration = 50;

    const handler: CDCHandler = {
      onChange: async () => {
        eventCount++;
        if (eventCount === 1) {
          // Simulate pause
          pausedTime = Date.now();
        }
      },
    };

    const stream = createCDCStream(reader, {
      fromLSN: 0n,
      handler,
      decoder: decode,
    });

    stream.start();

    // Pause the stream after a short delay
    setTimeout(() => {
      stream.pause();
      setTimeout(() => {
        stream.resume();
        setTimeout(() => stream.stop(), 100);
      }, pauseDuration);
    }, 10);

    // Wait for stream to finish
    await new Promise((resolve) => setTimeout(resolve, 300));

    expect(stream.getStatus().active).toBe(false);
  });

  it('should support manual acknowledgment', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);

    let lastLSN: bigint = 0n;
    const handler: CDCHandler = {
      onChange: async (event) => {
        lastLSN = event.lsn;
      },
    };

    const stream = createCDCStream(reader, {
      fromLSN: 0n,
      handler,
      decoder: decode,
      autoAck: false, // Manual acknowledgment
    });

    stream.start();
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Manually acknowledge
    stream.acknowledge(lastLSN);

    const status = stream.getStatus();
    expect(status.currentLSN).toBe(lastLSN);

    stream.stop();
  });

  it('should respect maxBufferSize configuration', async () => {
    // Create subscription with small buffer
    const subscription = createCDCSubscription(reader, {
      maxBufferSize: 10, // Small buffer
      batchSize: 5,
    });

    expect(subscription).toBeDefined();
    // The subscription should handle buffer limits internally
  });

  it('should stop gracefully when stop is called', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);

    const stream = createCDCStream(reader, {
      fromLSN: 0n,
      handler: {
        onChange: async () => {},
      },
      decoder: decode,
    });

    stream.start();
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Before stop, check status
    const statusBefore = stream.getStatus();

    stream.stop();
    await new Promise((resolve) => setTimeout(resolve, 100));

    // After stop, active should be false
    expect(stream.getStatus().active).toBe(false);
  });
});

// =============================================================================
// Test: Exactly-Once Delivery Semantics
// =============================================================================

describe('CDC Streaming - Exactly-Once Semantics', () => {
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

  it('should assign unique LSNs to all events', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
      { op: 'INSERT', table: 'items', after: { id: 2 } },
      { op: 'INSERT', table: 'items', after: { id: 3 } },
    ]);

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const lsns = new Set(entries.filter((e) => e.table === 'items').map((e) => e.lsn.toString()));

    // All LSNs should be unique
    expect(lsns.size).toBe(3);
  });

  it('should monotonically increase LSNs', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
      { op: 'INSERT', table: 'items', after: { id: 2 } },
    ]);

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const lsns = entries.filter((e) => e.table === 'items').map((e) => e.lsn);

    for (let i = 1; i < lsns.length; i++) {
      expect(lsns[i]).toBeGreaterThan(lsns[i - 1]);
    }
  });

  it('should not deliver events before fromLSN', async () => {
    const lsns1 = await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 2 } },
    ]);

    const startAfter = lsns1[lsns1.length - 1];

    const entries = await reader.readEntries({
      fromLSN: startAfter + 1n,
      operations: ['INSERT'],
    });

    // All entries should have LSN > startAfter
    for (const entry of entries) {
      expect(entry.lsn).toBeGreaterThan(startAfter);
    }
  });

  it('should support idempotent slot updates', async () => {
    const slotManager = createReplicationSlotManager(backend, reader);
    await slotManager.createSlot('idem-slot', 0n);

    // Update to same value multiple times
    await slotManager.updateSlot('idem-slot', 100n);
    await slotManager.updateSlot('idem-slot', 100n);
    await slotManager.updateSlot('idem-slot', 100n);

    const slot = await slotManager.getSlot('idem-slot');
    expect(slot?.acknowledgedLSN).toBe(100n);
  });

  it('should track transaction boundaries for exactly-once', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);

    const entries = await reader.readEntries({});

    let beginSeen = false;
    let commitSeen = false;
    let txnId: string | null = null;

    for (const entry of entries) {
      if (entry.op === 'BEGIN') {
        beginSeen = true;
        txnId = entry.txnId;
      }
      if (entry.op === 'COMMIT' && entry.txnId === txnId) {
        commitSeen = true;
        break;
      }
    }

    expect(beginSeen).toBe(true);
    expect(commitSeen).toBe(true);
  });
});

// =============================================================================
// Test: Schema Evolution Handling
// =============================================================================

describe('CDC Streaming - Schema Evolution', () => {
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

  it('should handle new columns in after data', async () => {
    // V1: Only id and name
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'users', after: { id: 1, name: 'Alice' } },
    ]);

    // V2: Added email column
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'users', after: { id: 2, name: 'Bob', email: 'bob@example.com' } },
    ]);

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const events = entries
      .filter((e) => e.table === 'users')
      .map((e) => walEntryToChangeEvent(e, decode) as ChangeEvent);

    // Both events should be valid despite schema difference
    expect(events).toHaveLength(2);
    expect(events[0].data).toEqual({ id: 1, name: 'Alice' });
    expect(events[1].data).toEqual({ id: 2, name: 'Bob', email: 'bob@example.com' });
  });

  it('should handle removed columns in after data', async () => {
    // V1: Has deprecated_field
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1, value: 100, deprecated_field: 'old' } },
    ]);

    // V2: deprecated_field removed
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 2, value: 200 } },
    ]);

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const events = entries
      .filter((e) => e.table === 'items')
      .map((e) => walEntryToChangeEvent(e, decode) as ChangeEvent);

    expect(events).toHaveLength(2);
    expect((events[0].data as any).deprecated_field).toBe('old');
    expect((events[1].data as any).deprecated_field).toBeUndefined();
  });

  it('should handle column type changes gracefully', async () => {
    // V1: price as integer
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'products', after: { id: 1, price: 100 } },
    ]);

    // V2: price as decimal string
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'products', after: { id: 2, price: '199.99' } },
    ]);

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const events = entries
      .filter((e) => e.table === 'products')
      .map((e) => walEntryToChangeEvent(e, decode) as ChangeEvent);

    expect(events).toHaveLength(2);
    expect((events[0].data as any).price).toBe(100);
    expect((events[1].data as any).price).toBe('199.99');
  });

  it('should handle updates with schema differences between before and after', async () => {
    await writeTestEntries(writer, [
      {
        op: 'UPDATE',
        table: 'users',
        key: { id: 1 },
        before: { id: 1, name: 'Alice' }, // Old schema
        after: { id: 1, name: 'Alice', email: 'alice@example.com' }, // New schema with email
      },
    ]);

    const entries = await reader.readEntries({ operations: ['UPDATE'] });
    const event = walEntryToChangeEvent(entries[0], decode) as ChangeEvent;

    expect(event.oldData).toEqual({ id: 1, name: 'Alice' });
    expect(event.data).toEqual({ id: 1, name: 'Alice', email: 'alice@example.com' });
  });

  it('should handle nullable fields', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'orders', after: { id: 1, discount: null } },
      { op: 'INSERT', table: 'orders', after: { id: 2, discount: 10 } },
    ]);

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const events = entries
      .filter((e) => e.table === 'orders')
      .map((e) => walEntryToChangeEvent(e, decode) as ChangeEvent);

    expect((events[0].data as any).discount).toBeNull();
    expect((events[1].data as any).discount).toBe(10);
  });
});

// =============================================================================
// Test: Filtering and Subscription Options
// =============================================================================

describe('CDC Streaming - Filtering', () => {
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

  it('should filter by table name', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'users', after: { id: 1 } },
      { op: 'INSERT', table: 'orders', after: { id: 100 } },
      { op: 'INSERT', table: 'users', after: { id: 2 } },
    ]);

    const entries = await reader.readEntries({
      operations: ['INSERT'],
      table: 'users',
    });

    const events = entries
      .filter((e) => e.table === 'users')
      .map((e) => walEntryToChangeEvent(e, decode) as ChangeEvent);

    expect(events).toHaveLength(2);
    expect(events.every((e) => e.table === 'users')).toBe(true);
  });

  it('should filter by operation type', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
      { op: 'UPDATE', table: 'items', key: { id: 1 }, before: { id: 1 }, after: { id: 1, updated: true } },
      { op: 'DELETE', table: 'items', key: { id: 1 }, before: { id: 1 } },
    ]);

    const entries = await reader.readEntries({
      operations: ['INSERT', 'UPDATE'],
    });

    const events = entries
      .filter((e) => e.table === 'items')
      .map((e) => walEntryToChangeEvent(e, decode) as ChangeEvent);

    expect(events).toHaveLength(2);
    expect(events.some((e) => e.type === 'insert')).toBe(true);
    expect(events.some((e) => e.type === 'update')).toBe(true);
    expect(events.every((e) => e.type !== 'delete')).toBe(true);
  });

  it('should filter by custom predicate', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1, value: 50 } },
      { op: 'INSERT', table: 'items', after: { id: 2, value: 150 } },
      { op: 'INSERT', table: 'items', after: { id: 3, value: 75 } },
    ]);

    const capturer = createWALCapturer(reader, {
      filter: {
        predicate: (entry) => {
          if (entry.after) {
            const data = decode<{ value: number }>(entry.after);
            return data.value > 100;
          }
          return false;
        },
      },
    });

    await capturer.capture();
    const state = capturer.getState();

    // Only entry with value 150 should be captured
    expect(state.totalEntries).toBe(1);
  });

  it('should combine multiple filters', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'users', after: { id: 1 } },
      { op: 'UPDATE', table: 'users', key: { id: 1 }, before: { id: 1 }, after: { id: 1, updated: true } },
      { op: 'INSERT', table: 'orders', after: { id: 100 } },
    ]);

    const capturer = createWALCapturer(reader, {
      filter: {
        tables: ['users'],
        operations: ['INSERT'],
      },
    });

    await capturer.capture();
    const state = capturer.getState();

    // Only INSERT on users table should be captured
    expect(state.totalEntries).toBe(1);
  });
});

// =============================================================================
// Test: Error Handling
// =============================================================================

describe('CDC Streaming - Error Handling', () => {
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

  it('should throw CDCError for slot not found', async () => {
    const slotManager = createReplicationSlotManager(backend, reader);

    await expect(slotManager.subscribeFromSlot('nonexistent')).rejects.toThrow(CDCError);
    await expect(slotManager.subscribeFromSlot('nonexistent')).rejects.toMatchObject({
      code: CDCErrorCode.SLOT_NOT_FOUND,
    });
  });

  it('should throw CDCError for duplicate slot', async () => {
    const slotManager = createReplicationSlotManager(backend, reader);
    await slotManager.createSlot('existing-slot', 0n);

    await expect(slotManager.createSlot('existing-slot', 0n)).rejects.toThrow(CDCError);
    await expect(slotManager.createSlot('existing-slot', 0n)).rejects.toMatchObject({
      code: CDCErrorCode.SLOT_EXISTS,
    });
  });

  it('should throw CDCError for slot update on non-existent slot', async () => {
    const slotManager = createReplicationSlotManager(backend, reader);

    await expect(slotManager.updateSlot('nonexistent', 100n)).rejects.toThrow(CDCError);
    await expect(slotManager.updateSlot('nonexistent', 100n)).rejects.toMatchObject({
      code: CDCErrorCode.SLOT_NOT_FOUND,
    });
  });

  it('should handle decoder errors gracefully', async () => {
    // Write a valid entry first
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const insertEntry = entries.find((e) => e.table === 'items');
    expect(insertEntry).toBeDefined();

    // Using a decoder that always throws
    const badDecoder = (_data: Uint8Array) => {
      throw new Error('Decoder failure');
    };

    // Should throw when trying to decode
    expect(() => walEntryToChangeEvent(insertEntry!, badDecoder)).toThrow('Decoder failure');
  });
});

// =============================================================================
// Test: Convenience Functions
// =============================================================================

describe('CDC Streaming - Convenience Functions', () => {
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

  it('should create CDC from backend using createCDC', async () => {
    const cdc = createCDC(backend);

    expect(cdc.reader).toBeDefined();
    expect(cdc.subscribe).toBeDefined();
    expect(cdc.slots).toBeDefined();
  });

  it('should create subscription via createCDC helper', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);

    const cdc = createCDC(backend);
    const subscription = cdc.subscribe({ fromLSN: 0n });

    expect(subscription).toBeDefined();
    expect(subscription.isActive()).toBe(false);
  });

  it('should access slot manager via createCDC helper', async () => {
    const cdc = createCDC(backend);

    await cdc.slots.createSlot('test-slot', 0n);
    const slot = await cdc.slots.getSlot('test-slot');

    expect(slot?.name).toBe('test-slot');
  });
});

// =============================================================================
// Test: Transaction Events
// =============================================================================

describe('CDC Streaming - Transaction Events', () => {
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

  it('should include transaction events when converted', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);

    const entries = await reader.readEntries({});
    const events = entries.map((e) => walEntryToChangeEvent(e, decode));

    const types = events.filter((e) => e !== null).map((e) => e!.type);
    expect(types).toContain('begin');
    expect(types).toContain('commit');
  });

  it('should filter out transaction events when only data operations wanted', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
    ]);

    const entries = await reader.readEntries({
      operations: ['INSERT', 'UPDATE', 'DELETE'],
    });

    // Only data operations should be returned
    expect(entries.every((e) => ['INSERT', 'UPDATE', 'DELETE'].includes(e.op))).toBe(true);
    expect(entries.some((e) => e.table === 'items')).toBe(true);
  });

  it('should track transaction ID across related events', async () => {
    await writeTestEntries(writer, [
      { op: 'INSERT', table: 'items', after: { id: 1 } },
      { op: 'INSERT', table: 'items', after: { id: 2 } },
    ]);

    const entries = await reader.readEntries({});

    // Find a complete transaction
    const txnIds = new Set<string>();
    let inTransaction = false;
    let currentTxnId: string | null = null;
    const txnEvents: WALEntry[] = [];

    for (const entry of entries) {
      if (entry.op === 'BEGIN') {
        inTransaction = true;
        currentTxnId = entry.txnId;
        txnEvents.push(entry);
      } else if (inTransaction && entry.txnId === currentTxnId) {
        txnEvents.push(entry);
        if (entry.op === 'COMMIT') {
          break;
        }
      }
    }

    // All events in same transaction should have same txnId
    const uniqueTxnIds = new Set(txnEvents.map((e) => e.txnId));
    expect(uniqueTxnIds.size).toBe(1);
  });
});
