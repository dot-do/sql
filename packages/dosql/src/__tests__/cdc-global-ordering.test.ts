/**
 * CDC Global Ordering Tests for DoSQL
 *
 * RED PHASE TDD: Tests documenting the missing HLC (Hybrid Logical Clock) ordering
 * implementation for CDC events across shards.
 *
 * HLC combines physical wall-clock time with a logical counter to provide:
 * - Monotonically increasing timestamps
 * - Causal ordering guarantees
 * - Bounded clock drift handling
 * - Globally consistent event ordering
 *
 * Tests use `it.fails()` pattern since implementation doesn't exist yet.
 *
 * @see https://cse.buffalo.edu/tech-reports/2014-04.pdf - HLC Paper
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import type { FSXBackend } from '../fsx/types.js';
import {
  createWALWriter,
  createWALReader,
  generateTxnId,
  type WALWriter,
  type WALReader,
} from '../wal/index.js';
import {
  createCDCSubscription,
  walEntryToChangeEvent,
  type CDCSubscription,
  type ChangeEvent,
} from '../cdc/index.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Creates an in-memory FSX backend for testing.
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

// =============================================================================
// HLC Type Definitions (Expected Interface)
// =============================================================================

/**
 * Hybrid Logical Clock timestamp
 * Combines wall-clock time with a logical counter for causal ordering
 */
interface HLCTimestamp {
  /** Physical wall-clock time in milliseconds */
  physicalTime: number;
  /** Logical counter for ordering events with same physical time */
  logicalCounter: number;
  /** Node/shard identifier for tie-breaking */
  nodeId: string;
}

/**
 * Extended WAL entry with HLC (for TDD - doesn't exist yet)
 */
interface WALEntryWithHLC {
  lsn: bigint;
  timestamp: number;
  txnId: string;
  op: string;
  table: string;
  before?: Uint8Array;
  after?: Uint8Array;
  hlc?: HLCTimestamp;
}

/**
 * Extended WAL Writer with HLC support (for TDD - doesn't exist yet)
 */
interface WALWriterWithHLC extends WALWriter {
  receiveHLC?(hlc: HLCTimestamp, options?: { maxDriftMs?: number; warningThreshold?: number }): Promise<void>;
  on?(event: string, callback: (...args: unknown[]) => void): void;
  getDriftMetrics?(): { maxDrift: number; avgDrift: number; driftCount: number };
}

/**
 * Extended CDC Subscription with HLC ordering (for TDD - doesn't exist yet)
 */
interface CDCSubscriptionWithHLC extends CDCSubscription {
  subscribeByHLC(fromLsn: bigint, options?: { tables?: string[] }): AsyncIterable<HLCCDCEvent>;
}

/**
 * Helper to cast to extended WAL entry
 */
function asHLCEntry(entry: unknown): WALEntryWithHLC {
  return entry as WALEntryWithHLC;
}

/**
 * Helper to cast to extended WAL writer
 */
function asHLCWriter(writer: WALWriter): WALWriterWithHLC {
  return writer as WALWriterWithHLC;
}

/**
 * Helper to cast to extended subscription
 */
function asHLCSubscription(sub: CDCSubscription): CDCSubscriptionWithHLC {
  return sub as CDCSubscriptionWithHLC;
}

/**
 * CDC Event with HLC ordering (expected enhanced interface)
 */
interface HLCCDCEvent<T = unknown> extends ChangeEvent<T> {
  /** HLC timestamp for global ordering */
  hlc: HLCTimestamp;
}

/**
 * HLC Clock interface for generating and comparing timestamps
 */
interface HLCClock {
  /** Get current HLC timestamp */
  now(): HLCTimestamp;
  /** Update clock on receiving a message with higher timestamp */
  receive(remote: HLCTimestamp): HLCTimestamp;
  /** Compare two HLC timestamps (-1, 0, 1) */
  compare(a: HLCTimestamp, b: HLCTimestamp): number;
  /** Get maximum allowed drift from wall clock */
  getMaxDrift(): number;
  /** Check if timestamp is within acceptable drift */
  isValidDrift(timestamp: HLCTimestamp): boolean;
}

// =============================================================================
// Test 1: HLC Timestamp Generated for Each Mutation
// =============================================================================

describe('CDC Global Ordering - HLC Timestamp Generation', () => {
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

  it.fails('should generate HLC timestamp for INSERT mutation', async () => {
    // When: Writing an INSERT mutation
    const txnId = generateTxnId();
    const timestamp = Date.now();

    await writer.append({
      timestamp,
      txnId,
      op: 'BEGIN',
      table: '',
    });

    const insertResult = await writer.append({
      timestamp,
      txnId,
      op: 'INSERT',
      table: 'users',
      after: encode({ id: 1, name: 'Alice' }),
    });

    await writer.append(
      {
        timestamp,
        txnId,
        op: 'COMMIT',
        table: '',
      },
      { sync: true }
    );

    // Then: The WAL entry should include an HLC timestamp
    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const insertEntry = entries.find((e) => e.op === 'INSERT' && e.table === 'users');

    expect(insertEntry).toBeDefined();
    // Expected: WAL entry has hlc field with HLC timestamp
    expect(asHLCEntry(insertEntry).hlc).toBeDefined();
    expect(asHLCEntry(insertEntry).hlc.physicalTime).toBeGreaterThan(0);
    expect(asHLCEntry(insertEntry).hlc.logicalCounter).toBeGreaterThanOrEqual(0);
    expect(asHLCEntry(insertEntry).hlc.nodeId).toBeDefined();
  });

  it.fails('should generate HLC timestamp for UPDATE mutation', async () => {
    const txnId = generateTxnId();
    const timestamp = Date.now();

    await writer.append({ timestamp, txnId, op: 'BEGIN', table: '' });
    await writer.append({
      timestamp,
      txnId,
      op: 'UPDATE',
      table: 'users',
      before: encode({ id: 1, name: 'Alice' }),
      after: encode({ id: 1, name: 'Bob' }),
    });
    await writer.append({ timestamp, txnId, op: 'COMMIT', table: '' }, { sync: true });

    const entries = await reader.readEntries({ operations: ['UPDATE'] });
    const updateEntry = entries.find((e) => e.op === 'UPDATE' && e.table === 'users');

    expect(updateEntry).toBeDefined();
    expect(asHLCEntry(updateEntry).hlc).toBeDefined();
    expect(asHLCEntry(updateEntry).hlc.physicalTime).toBeGreaterThan(0);
    expect(asHLCEntry(updateEntry).hlc.logicalCounter).toBeGreaterThanOrEqual(0);
  });

  it.fails('should generate HLC timestamp for DELETE mutation', async () => {
    const txnId = generateTxnId();
    const timestamp = Date.now();

    await writer.append({ timestamp, txnId, op: 'BEGIN', table: '' });
    await writer.append({
      timestamp,
      txnId,
      op: 'DELETE',
      table: 'users',
      before: encode({ id: 1, name: 'Alice' }),
    });
    await writer.append({ timestamp, txnId, op: 'COMMIT', table: '' }, { sync: true });

    const entries = await reader.readEntries({ operations: ['DELETE'] });
    const deleteEntry = entries.find((e) => e.op === 'DELETE' && e.table === 'users');

    expect(deleteEntry).toBeDefined();
    expect(asHLCEntry(deleteEntry).hlc).toBeDefined();
    expect(asHLCEntry(deleteEntry).hlc.physicalTime).toBeGreaterThan(0);
  });
});

// =============================================================================
// Test 2: HLC Includes Physical Time and Logical Counter
// =============================================================================

describe('CDC Global Ordering - HLC Structure', () => {
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

  it.fails('should include physical time component in HLC', async () => {
    const txnId = generateTxnId();
    const beforeTime = Date.now();

    await writer.append({ timestamp: beforeTime, txnId, op: 'BEGIN', table: '' });
    await writer.append({
      timestamp: beforeTime,
      txnId,
      op: 'INSERT',
      table: 'events',
      after: encode({ id: 1 }),
    });
    await writer.append({ timestamp: beforeTime, txnId, op: 'COMMIT', table: '' }, { sync: true });

    const afterTime = Date.now();

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const entry = entries.find((e) => e.table === 'events');
    const hlc = asHLCEntry(entry).hlc as HLCTimestamp;

    // Physical time should be between before and after time of the operation
    expect(hlc.physicalTime).toBeGreaterThanOrEqual(beforeTime);
    expect(hlc.physicalTime).toBeLessThanOrEqual(afterTime);
  });

  it.fails('should include logical counter component in HLC', async () => {
    const txnId = generateTxnId();
    const timestamp = Date.now();

    await writer.append({ timestamp, txnId, op: 'BEGIN', table: '' });

    // Insert multiple entries within same transaction (same physical time)
    for (let i = 0; i < 5; i++) {
      await writer.append({
        timestamp,
        txnId,
        op: 'INSERT',
        table: 'events',
        after: encode({ id: i }),
      });
    }

    await writer.append({ timestamp, txnId, op: 'COMMIT', table: '' }, { sync: true });

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const eventEntries = entries.filter((e) => e.table === 'events');

    // Each entry should have distinct logical counter if physical time is same
    const counters = eventEntries.map((e) => asHLCEntry(e).hlc?.logicalCounter);
    const uniqueCounters = new Set(counters);

    // All counters should be unique
    expect(uniqueCounters.size).toBe(eventEntries.length);
  });

  it.fails('should include node identifier in HLC for distributed ordering', async () => {
    const txnId = generateTxnId();
    const timestamp = Date.now();

    await writer.append({ timestamp, txnId, op: 'BEGIN', table: '' });
    await writer.append({
      timestamp,
      txnId,
      op: 'INSERT',
      table: 'events',
      after: encode({ id: 1 }),
    });
    await writer.append({ timestamp, txnId, op: 'COMMIT', table: '' }, { sync: true });

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const entry = entries.find((e) => e.table === 'events');
    const hlc = asHLCEntry(entry).hlc as HLCTimestamp;

    // Node ID should be non-empty string identifying the shard/DO
    expect(typeof hlc.nodeId).toBe('string');
    expect(hlc.nodeId.length).toBeGreaterThan(0);
  });

  it.fails('should maintain HLC ordering invariant: physicalTime >= previous.physicalTime', async () => {
    const txnId = generateTxnId();

    // Write entries with artificially decreasing timestamps
    // HLC should still produce monotonically increasing physical time
    for (let i = 0; i < 5; i++) {
      const artificialTimestamp = Date.now() - (i * 1000); // Decreasing time
      await writer.append({ timestamp: artificialTimestamp, txnId: generateTxnId(), op: 'BEGIN', table: '' });
      await writer.append({
        timestamp: artificialTimestamp,
        txnId,
        op: 'INSERT',
        table: 'events',
        after: encode({ id: i }),
      });
      await writer.append({ timestamp: artificialTimestamp, txnId, op: 'COMMIT', table: '' }, { sync: true });
    }

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const eventEntries = entries.filter((e) => e.table === 'events').sort((a, b) => Number(a.lsn - b.lsn));

    // HLC physical time should be monotonically non-decreasing
    for (let i = 1; i < eventEntries.length; i++) {
      const prev = asHLCEntry(eventEntries[i - 1]).hlc as HLCTimestamp;
      const curr = asHLCEntry(eventEntries[i]).hlc as HLCTimestamp;
      expect(curr.physicalTime).toBeGreaterThanOrEqual(prev.physicalTime);
    }
  });
});

// =============================================================================
// Test 3: Causal Ordering Preserved Across Shards
// =============================================================================

describe('CDC Global Ordering - Cross-Shard Causal Ordering', () => {
  it.fails('should preserve causal order when event on shard B happens after event on shard A', async () => {
    // Scenario: Shard A writes event, Shard B reads and writes causally dependent event
    // The HLC on Shard B should be greater than HLC on Shard A

    // Simulating two shards with separate backends
    const shardABackend = createTestBackend();
    const shardBBackend = createTestBackend();

    const shardAWriter = createWALWriter(shardABackend);
    const shardBWriter = createWALWriter(shardBBackend);
    const shardAReader = createWALReader(shardABackend);
    const shardBReader = createWALReader(shardBBackend);

    // Event on Shard A
    const txnA = generateTxnId();
    const timestampA = Date.now();
    await shardAWriter.append({ timestamp: timestampA, txnId: txnA, op: 'BEGIN', table: '' });
    await shardAWriter.append({
      timestamp: timestampA,
      txnId: txnA,
      op: 'INSERT',
      table: 'orders',
      after: encode({ id: 1, status: 'pending' }),
    });
    await shardAWriter.append({ timestamp: timestampA, txnId: txnA, op: 'COMMIT', table: '' }, { sync: true });

    // Read HLC from Shard A event
    const entriesA = await shardAReader.readEntries({ operations: ['INSERT'] });
    const eventA = entriesA.find((e) => e.table === 'orders');
    const hlcA = asHLCEntry(eventA).hlc as HLCTimestamp;

    // Shard B receives the HLC and writes causally dependent event
    // The HLC clock on Shard B should be updated to ensure causal ordering
    const txnB = generateTxnId();
    const timestampB = Date.now();

    // Expected: Writer accepts remote HLC to update its clock
    await asHLCWriter(shardBWriter).receiveHLC?.(hlcA);

    await shardBWriter.append({ timestamp: timestampB, txnId: txnB, op: 'BEGIN', table: '' });
    await shardBWriter.append({
      timestamp: timestampB,
      txnId: txnB,
      op: 'INSERT',
      table: 'shipments',
      after: encode({ id: 1, orderId: 1, status: 'shipped' }),
    });
    await shardBWriter.append({ timestamp: timestampB, txnId: txnB, op: 'COMMIT', table: '' }, { sync: true });

    // Read HLC from Shard B event
    const entriesB = await shardBReader.readEntries({ operations: ['INSERT'] });
    const eventB = entriesB.find((e) => e.table === 'shipments');
    const hlcB = asHLCEntry(eventB).hlc as HLCTimestamp;

    // Causal ordering: Event B must be > Event A in HLC ordering
    const comparison = compareHLC(hlcA, hlcB);
    expect(comparison).toBeLessThan(0); // A < B

    await shardAWriter.close();
    await shardBWriter.close();
  });

  it.fails('should merge HLC from multiple shards maintaining global order', async () => {
    // Scenario: Coordinator receives events from 3 shards and merges them
    const shard1Backend = createTestBackend();
    const shard2Backend = createTestBackend();
    const shard3Backend = createTestBackend();

    const shard1Writer = createWALWriter(shard1Backend);
    const shard2Writer = createWALWriter(shard2Backend);
    const shard3Writer = createWALWriter(shard3Backend);

    // Write events on each shard at slightly different times
    const baseTime = Date.now();

    for (const [writer, shardId, offset] of [
      [shard1Writer, '1', 0],
      [shard2Writer, '2', 5],
      [shard3Writer, '3', 10],
    ] as const) {
      const txn = generateTxnId();
      await writer.append({ timestamp: baseTime + offset, txnId: txn, op: 'BEGIN', table: '' });
      await writer.append({
        timestamp: baseTime + offset,
        txnId: txn,
        op: 'INSERT',
        table: 'events',
        after: encode({ shardId, sequence: offset }),
      });
      await writer.append({ timestamp: baseTime + offset, txnId: txn, op: 'COMMIT', table: '' }, { sync: true });
    }

    // Collect all events with HLC
    const allEvents: HLCCDCEvent[] = [];
    for (const [backend, shardId] of [
      [shard1Backend, '1'],
      [shard2Backend, '2'],
      [shard3Backend, '3'],
    ] as const) {
      const reader = createWALReader(backend);
      const entries = await reader.readEntries({ operations: ['INSERT'] });
      for (const entry of entries.filter((e) => e.table === 'events')) {
        const event = walEntryToChangeEvent(entry, decode) as HLCCDCEvent;
        allEvents.push(event);
      }
    }

    // Sort by HLC
    allEvents.sort((a, b) => compareHLC(a.hlc, b.hlc));

    // Events should be in causal order based on HLC, not just wall clock
    for (let i = 1; i < allEvents.length; i++) {
      expect(compareHLC(allEvents[i - 1].hlc, allEvents[i].hlc)).toBeLessThanOrEqual(0);
    }

    await shard1Writer.close();
    await shard2Writer.close();
    await shard3Writer.close();
  });
});

// =============================================================================
// Test 4: CDC Events Ordered by HLC, Not Wall Clock
// =============================================================================

describe('CDC Global Ordering - HLC vs Wall Clock Ordering', () => {
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

  it.fails('should order events by HLC when wall clocks disagree', async () => {
    // Scenario: Two events with wall clocks that disagree due to clock skew
    // HLC should provide consistent ordering

    const txn1 = generateTxnId();
    const txn2 = generateTxnId();

    // Event 1 at wall clock time T+100
    await writer.append({ timestamp: Date.now() + 100, txnId: txn1, op: 'BEGIN', table: '' });
    await writer.append({
      timestamp: Date.now() + 100,
      txnId: txn1,
      op: 'INSERT',
      table: 'events',
      after: encode({ id: 1, description: 'first event' }),
    });
    await writer.append({ timestamp: Date.now() + 100, txnId: txn1, op: 'COMMIT', table: '' }, { sync: true });

    // Event 2 at wall clock time T (earlier wall clock, but written second)
    await writer.append({ timestamp: Date.now(), txnId: txn2, op: 'BEGIN', table: '' });
    await writer.append({
      timestamp: Date.now(),
      txnId: txn2,
      op: 'INSERT',
      table: 'events',
      after: encode({ id: 2, description: 'second event' }),
    });
    await writer.append({ timestamp: Date.now(), txnId: txn2, op: 'COMMIT', table: '' }, { sync: true });

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const events = entries
      .filter((e) => e.table === 'events')
      .map((e) => ({ entry: e, hlc: asHLCEntry(e).hlc as HLCTimestamp }))
      .sort((a, b) => compareHLC(a.hlc, b.hlc));

    // First event by HLC should be the one written first (id: 1)
    // despite having later wall clock time
    const firstEvent = decode<{ id: number }>(events[0].entry.after!);
    expect(firstEvent.id).toBe(1);
  });

  it.fails('should use HLC for subscription ordering instead of LSN', async () => {
    const subscription = createCDCSubscription(reader, {});

    // Expected: subscribeChanges returns events ordered by HLC
    const orderedEvents: HLCCDCEvent[] = [];

    for await (const event of asHLCSubscription(subscription).subscribeByHLC(0n)) {
      if ((event as HLCCDCEvent).hlc) {
        orderedEvents.push(event as HLCCDCEvent);
      }
      if (orderedEvents.length >= 10) break;
    }

    // Events should be in HLC order
    for (let i = 1; i < orderedEvents.length; i++) {
      expect(compareHLC(orderedEvents[i - 1].hlc, orderedEvents[i].hlc)).toBeLessThanOrEqual(0);
    }
  });
});

// =============================================================================
// Test 5: Concurrent Events on Same Shard Have Distinct HLCs
// =============================================================================

describe('CDC Global Ordering - Concurrent Event Distinction', () => {
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

  it.fails('should assign distinct HLCs to concurrent events in same millisecond', async () => {
    const txnId = generateTxnId();
    const sameTimestamp = Date.now();

    await writer.append({ timestamp: sameTimestamp, txnId, op: 'BEGIN', table: '' });

    // Write multiple events with exact same timestamp
    const eventCount = 10;
    for (let i = 0; i < eventCount; i++) {
      await writer.append({
        timestamp: sameTimestamp, // Same timestamp for all
        txnId,
        op: 'INSERT',
        table: 'concurrent_events',
        after: encode({ id: i }),
      });
    }

    await writer.append({ timestamp: sameTimestamp, txnId, op: 'COMMIT', table: '' }, { sync: true });

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const eventEntries = entries.filter((e) => e.table === 'concurrent_events');

    // All events should have distinct HLCs
    const hlcs = eventEntries.map((e) => asHLCEntry(e).hlc as HLCTimestamp);
    const hlcStrings = hlcs.map((hlc) => `${hlc.physicalTime}-${hlc.logicalCounter}-${hlc.nodeId}`);
    const uniqueHLCs = new Set(hlcStrings);

    expect(uniqueHLCs.size).toBe(eventCount);
  });

  it.fails('should increment logical counter for same physical time', async () => {
    const txnId = generateTxnId();
    const sameTimestamp = Date.now();

    await writer.append({ timestamp: sameTimestamp, txnId, op: 'BEGIN', table: '' });

    for (let i = 0; i < 5; i++) {
      await writer.append({
        timestamp: sameTimestamp,
        txnId,
        op: 'INSERT',
        table: 'counter_test',
        after: encode({ id: i }),
      });
    }

    await writer.append({ timestamp: sameTimestamp, txnId, op: 'COMMIT', table: '' }, { sync: true });

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const eventEntries = entries
      .filter((e) => e.table === 'counter_test')
      .sort((a, b) => Number(a.lsn - b.lsn));

    // Logical counters should be incrementing
    const counters = eventEntries.map((e) => asHLCEntry(e).hlc?.logicalCounter);
    for (let i = 1; i < counters.length; i++) {
      expect(counters[i]).toBeGreaterThan(counters[i - 1]);
    }
  });

  it.fails('should reset logical counter when physical time advances', async () => {
    const eventHLCs: HLCTimestamp[] = [];

    // Write event at time T
    const txn1 = generateTxnId();
    await writer.append({ timestamp: Date.now(), txnId: txn1, op: 'BEGIN', table: '' });
    await writer.append({
      timestamp: Date.now(),
      txnId: txn1,
      op: 'INSERT',
      table: 'reset_test',
      after: encode({ id: 1 }),
    });
    await writer.append({ timestamp: Date.now(), txnId: txn1, op: 'COMMIT', table: '' }, { sync: true });

    // Wait for time to advance
    await new Promise((resolve) => setTimeout(resolve, 10));

    // Write event at time T+10ms
    const txn2 = generateTxnId();
    await writer.append({ timestamp: Date.now(), txnId: txn2, op: 'BEGIN', table: '' });
    await writer.append({
      timestamp: Date.now(),
      txnId: txn2,
      op: 'INSERT',
      table: 'reset_test',
      after: encode({ id: 2 }),
    });
    await writer.append({ timestamp: Date.now(), txnId: txn2, op: 'COMMIT', table: '' }, { sync: true });

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const eventEntries = entries
      .filter((e) => e.table === 'reset_test')
      .sort((a, b) => Number(a.lsn - b.lsn));

    const hlc1 = asHLCEntry(eventEntries[0]).hlc as HLCTimestamp;
    const hlc2 = asHLCEntry(eventEntries[1]).hlc as HLCTimestamp;

    // Physical time should have advanced
    expect(hlc2.physicalTime).toBeGreaterThan(hlc1.physicalTime);
    // Logical counter can be reset to 0 when physical time advances
    expect(hlc2.logicalCounter).toBe(0);
  });
});

// =============================================================================
// Test 6: HLC Advances on Receiving Higher Timestamp
// =============================================================================

describe('CDC Global Ordering - HLC Receive Semantics', () => {
  it.fails('should advance local HLC when receiving higher remote timestamp', async () => {
    const backend = createTestBackend();
    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);

    // Local clock at time T
    const localTime = Date.now();

    // Remote timestamp is in the future (simulating clock skew)
    const remoteHLC: HLCTimestamp = {
      physicalTime: localTime + 5000, // 5 seconds ahead
      logicalCounter: 42,
      nodeId: 'remote-shard',
    };

    // Expected: Writer has receiveHLC method to update its clock
    await asHLCWriter(writer).receiveHLC?.(remoteHLC);

    // Write new event
    const txnId = generateTxnId();
    await writer.append({ timestamp: localTime, txnId, op: 'BEGIN', table: '' });
    await writer.append({
      timestamp: localTime,
      txnId,
      op: 'INSERT',
      table: 'receive_test',
      after: encode({ id: 1 }),
    });
    await writer.append({ timestamp: localTime, txnId, op: 'COMMIT', table: '' }, { sync: true });

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const entry = entries.find((e) => e.table === 'receive_test');
    const localHLC = asHLCEntry(entry).hlc as HLCTimestamp;

    // Local HLC should be greater than received remote HLC
    expect(compareHLC(remoteHLC, localHLC)).toBeLessThan(0);

    await writer.close();
  });

  it.fails('should use max(local, remote) for physical time', async () => {
    const backend = createTestBackend();
    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);

    const localTime = Date.now();
    const futureTime = localTime + 10000; // 10 seconds ahead

    const remoteHLC: HLCTimestamp = {
      physicalTime: futureTime,
      logicalCounter: 5,
      nodeId: 'remote',
    };

    await asHLCWriter(writer).receiveHLC?.(remoteHLC);

    const txnId = generateTxnId();
    await writer.append({ timestamp: localTime, txnId, op: 'BEGIN', table: '' });
    await writer.append({
      timestamp: localTime,
      txnId,
      op: 'INSERT',
      table: 'max_test',
      after: encode({ id: 1 }),
    });
    await writer.append({ timestamp: localTime, txnId, op: 'COMMIT', table: '' }, { sync: true });

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const entry = entries.find((e) => e.table === 'max_test');
    const localHLC = asHLCEntry(entry).hlc as HLCTimestamp;

    // Physical time should be at least as high as the remote's
    expect(localHLC.physicalTime).toBeGreaterThanOrEqual(futureTime);

    await writer.close();
  });

  it.fails('should increment logical counter when physical times equal', async () => {
    const backend = createTestBackend();
    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);

    const currentTime = Date.now();

    const remoteHLC: HLCTimestamp = {
      physicalTime: currentTime,
      logicalCounter: 100,
      nodeId: 'remote',
    };

    await asHLCWriter(writer).receiveHLC?.(remoteHLC);

    const txnId = generateTxnId();
    await writer.append({ timestamp: currentTime, txnId, op: 'BEGIN', table: '' });
    await writer.append({
      timestamp: currentTime,
      txnId,
      op: 'INSERT',
      table: 'counter_advance_test',
      after: encode({ id: 1 }),
    });
    await writer.append({ timestamp: currentTime, txnId, op: 'COMMIT', table: '' }, { sync: true });

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const entry = entries.find((e) => e.table === 'counter_advance_test');
    const localHLC = asHLCEntry(entry).hlc as HLCTimestamp;

    // If physical time equals remote, logical counter should be > remote's counter
    if (localHLC.physicalTime === remoteHLC.physicalTime) {
      expect(localHLC.logicalCounter).toBeGreaterThan(remoteHLC.logicalCounter);
    }

    await writer.close();
  });
});

// =============================================================================
// Test 7: Subscriber Receives Events in Causal Order
// =============================================================================

describe('CDC Global Ordering - Subscriber Causal Delivery', () => {
  it.fails('should deliver events to subscriber in HLC order', async () => {
    const backend = createTestBackend();
    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);

    // Write events
    for (let i = 0; i < 10; i++) {
      const txnId = generateTxnId();
      await writer.append({ timestamp: Date.now(), txnId, op: 'BEGIN', table: '' });
      await writer.append({
        timestamp: Date.now(),
        txnId,
        op: 'INSERT',
        table: 'ordered_events',
        after: encode({ sequence: i }),
      });
      await writer.append({ timestamp: Date.now(), txnId, op: 'COMMIT', table: '' }, { sync: true });
    }

    const subscription = createCDCSubscription(reader, {});

    // Expected: subscribeByHLC delivers events in causal (HLC) order
    const receivedEvents: HLCCDCEvent[] = [];

    for await (const event of asHLCSubscription(subscription).subscribeByHLC(0n, {
      tables: ['ordered_events'],
    })) {
      if ((event as HLCCDCEvent).hlc) {
        receivedEvents.push(event as HLCCDCEvent);
      }
      if (receivedEvents.length >= 10) break;
    }

    // Verify HLC ordering
    for (let i = 1; i < receivedEvents.length; i++) {
      const comparison = compareHLC(receivedEvents[i - 1].hlc, receivedEvents[i].hlc);
      expect(comparison).toBeLessThanOrEqual(0);
    }

    await writer.close();
  });

  it.fails('should buffer out-of-order events until causally ready', async () => {
    // Scenario: Events arrive out of HLC order (e.g., from network reordering)
    // Subscriber should buffer and reorder before delivering

    const backend = createTestBackend();
    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);

    // Expected: Subscription has option for causal delivery buffering
    const subscription = createCDCSubscription(reader, {
      // causalDelivery: true, // Expected option
      // causalBufferMs: 100, // Expected option - max time to wait for reordering
    });

    // Write events that might arrive out of order
    for (let i = 0; i < 5; i++) {
      const txnId = generateTxnId();
      await writer.append({ timestamp: Date.now(), txnId, op: 'BEGIN', table: '' });
      await writer.append({
        timestamp: Date.now(),
        txnId,
        op: 'INSERT',
        table: 'buffered_events',
        after: encode({ id: i }),
      });
      await writer.append({ timestamp: Date.now(), txnId, op: 'COMMIT', table: '' }, { sync: true });
    }

    const events: HLCCDCEvent[] = [];
    for await (const event of asHLCSubscription(subscription).subscribeByHLC(0n)) {
      if ((event as HLCCDCEvent).table === 'buffered_events') {
        events.push(event as HLCCDCEvent);
      }
      if (events.length >= 5) break;
    }

    // All events should be in causal order
    for (let i = 1; i < events.length; i++) {
      expect(compareHLC(events[i - 1].hlc, events[i].hlc)).toBeLessThanOrEqual(0);
    }

    await writer.close();
  });

  it.fails('should support cross-shard subscription with global HLC ordering', async () => {
    // Multi-shard subscription should merge events from all shards in HLC order
    const shard1Backend = createTestBackend();
    const shard2Backend = createTestBackend();

    const shard1Writer = createWALWriter(shard1Backend);
    const shard2Writer = createWALWriter(shard2Backend);

    // Write interleaved events to both shards
    for (let i = 0; i < 5; i++) {
      const txn1 = generateTxnId();
      await shard1Writer.append({ timestamp: Date.now(), txnId: txn1, op: 'BEGIN', table: '' });
      await shard1Writer.append({
        timestamp: Date.now(),
        txnId: txn1,
        op: 'INSERT',
        table: 'events',
        after: encode({ shard: 1, id: i }),
      });
      await shard1Writer.append({ timestamp: Date.now(), txnId: txn1, op: 'COMMIT', table: '' }, { sync: true });

      const txn2 = generateTxnId();
      await shard2Writer.append({ timestamp: Date.now(), txnId: txn2, op: 'BEGIN', table: '' });
      await shard2Writer.append({
        timestamp: Date.now(),
        txnId: txn2,
        op: 'INSERT',
        table: 'events',
        after: encode({ shard: 2, id: i }),
      });
      await shard2Writer.append({ timestamp: Date.now(), txnId: txn2, op: 'COMMIT', table: '' }, { sync: true });
    }

    // Expected: Multi-shard subscription API
    const readers = [createWALReader(shard1Backend), createWALReader(shard2Backend)];

    // Expected: createMultiShardCDCSubscription or similar
    // const multiShardSubscription = createMultiShardCDCSubscription(readers, {
    //   orderBy: 'hlc',
    // });

    // For now, manually collect and verify
    const allEvents: HLCCDCEvent[] = [];
    for (const reader of readers) {
      const entries = await reader.readEntries({ operations: ['INSERT'] });
      for (const entry of entries.filter((e) => e.table === 'events')) {
        const event = walEntryToChangeEvent(entry, decode) as HLCCDCEvent;
        allEvents.push(event);
      }
    }

    // Sort by HLC
    allEvents.sort((a, b) => compareHLC(a.hlc, b.hlc));

    // Verify global HLC ordering
    for (let i = 1; i < allEvents.length; i++) {
      expect(compareHLC(allEvents[i - 1].hlc, allEvents[i].hlc)).toBeLessThanOrEqual(0);
    }

    await shard1Writer.close();
    await shard2Writer.close();
  });
});

// =============================================================================
// Test 8: HLC Drift Detection and Handling
// =============================================================================

describe('CDC Global Ordering - HLC Drift Detection', () => {
  it.fails('should detect clock drift exceeding threshold', async () => {
    const backend = createTestBackend();
    const writer = createWALWriter(backend);

    // Configure max allowed drift (e.g., 1 second)
    const maxDriftMs = 1000;

    // Expected: HLC clock with configurable max drift
    // const hlcClock = createHLCClock({ maxDriftMs, nodeId: 'test-node' });

    // Receive timestamp with excessive drift (10 seconds ahead)
    const remoteHLC: HLCTimestamp = {
      physicalTime: Date.now() + 10000,
      logicalCounter: 0,
      nodeId: 'remote',
    };

    // Expected: receiveHLC should throw or return error on excessive drift
    await expect(
      asHLCWriter(writer).receiveHLC?.(remoteHLC, { maxDriftMs })
    ).rejects.toThrow(/drift|clock|skew/i);

    await writer.close();
  });

  it.fails('should handle backward clock jump gracefully', async () => {
    const backend = createTestBackend();
    const writer = createWALWriter(backend);
    const reader = createWALReader(backend);

    // Write first event at current time
    const txn1 = generateTxnId();
    const currentTime = Date.now();
    await writer.append({ timestamp: currentTime, txnId: txn1, op: 'BEGIN', table: '' });
    await writer.append({
      timestamp: currentTime,
      txnId: txn1,
      op: 'INSERT',
      table: 'drift_test',
      after: encode({ id: 1 }),
    });
    await writer.append({ timestamp: currentTime, txnId: txn1, op: 'COMMIT', table: '' }, { sync: true });

    // Simulate backward clock jump (wall clock goes back 5 seconds)
    const txn2 = generateTxnId();
    const pastTime = currentTime - 5000;
    await writer.append({ timestamp: pastTime, txnId: txn2, op: 'BEGIN', table: '' });
    await writer.append({
      timestamp: pastTime,
      txnId: txn2,
      op: 'INSERT',
      table: 'drift_test',
      after: encode({ id: 2 }),
    });
    await writer.append({ timestamp: pastTime, txnId: txn2, op: 'COMMIT', table: '' }, { sync: true });

    const entries = await reader.readEntries({ operations: ['INSERT'] });
    const events = entries
      .filter((e) => e.table === 'drift_test')
      .sort((a, b) => Number(a.lsn - b.lsn));

    const hlc1 = asHLCEntry(events[0]).hlc as HLCTimestamp;
    const hlc2 = asHLCEntry(events[1]).hlc as HLCTimestamp;

    // HLC should still be monotonically increasing despite wall clock going back
    expect(compareHLC(hlc1, hlc2)).toBeLessThan(0);

    await writer.close();
  });

  it.fails('should emit drift warning event when approaching threshold', async () => {
    const backend = createTestBackend();
    const writer = createWALWriter(backend);

    const maxDriftMs = 1000;
    const warningThreshold = 0.8; // Warn at 80% of max drift

    const warnings: Array<{ drift: number; timestamp: number }> = [];

    // Expected: Writer emits drift warnings
    asHLCWriter(writer).on?.('drift-warning', (warning: { drift: number; timestamp: number }) => {
      warnings.push(warning);
    });

    // Receive timestamp at 80% of max drift
    const remoteHLC: HLCTimestamp = {
      physicalTime: Date.now() + (maxDriftMs * warningThreshold),
      logicalCounter: 0,
      nodeId: 'remote',
    };

    await asHLCWriter(writer).receiveHLC?.(remoteHLC, { maxDriftMs, warningThreshold });

    expect(warnings.length).toBeGreaterThan(0);
    expect(warnings[0].drift).toBeGreaterThanOrEqual(maxDriftMs * warningThreshold);

    await writer.close();
  });

  it.fails('should support drift recovery strategy configuration', async () => {
    const backend = createTestBackend();

    // Expected: Writer accepts drift recovery strategy
    // Strategies: 'reject', 'log-and-accept', 'wait-for-sync'
    const writer = createWALWriter(backend, {
      // hlc: {
      //   maxDriftMs: 1000,
      //   driftStrategy: 'wait-for-sync',
      //   syncTimeoutMs: 5000,
      // },
    } as Record<string, unknown>);

    // When drift exceeds threshold with 'wait-for-sync' strategy
    const remoteHLC: HLCTimestamp = {
      physicalTime: Date.now() + 2000, // 2 seconds drift
      logicalCounter: 0,
      nodeId: 'remote',
    };

    // Should wait for local clock to catch up (with timeout)
    const startTime = Date.now();
    await asHLCWriter(writer).receiveHLC?.(remoteHLC);
    const elapsed = Date.now() - startTime;

    // Should have waited for clock sync
    expect(elapsed).toBeGreaterThan(0);

    await writer.close();
  });

  it.fails('should record drift metrics for monitoring', async () => {
    const backend = createTestBackend();
    const writer = createWALWriter(backend);

    // Simulate receiving several timestamps with varying drift
    const drifts = [100, 200, 500, 150, 300]; // milliseconds

    for (const drift of drifts) {
      const remoteHLC: HLCTimestamp = {
        physicalTime: Date.now() + drift,
        logicalCounter: 0,
        nodeId: 'remote',
      };
      await asHLCWriter(writer).receiveHLC?.(remoteHLC);
    }

    // Expected: Writer provides drift metrics
    const metrics = asHLCWriter(writer).getDriftMetrics?.();

    expect(metrics).toBeDefined();
    expect(metrics.maxDrift).toBeGreaterThanOrEqual(Math.max(...drifts));
    expect(metrics.avgDrift).toBeGreaterThan(0);
    expect(metrics.driftCount).toBe(drifts.length);

    await writer.close();
  });
});

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Compare two HLC timestamps
 * Returns: -1 if a < b, 0 if a == b, 1 if a > b
 */
function compareHLC(a: HLCTimestamp, b: HLCTimestamp): number {
  // First compare physical time
  if (a.physicalTime < b.physicalTime) return -1;
  if (a.physicalTime > b.physicalTime) return 1;

  // Physical times equal, compare logical counter
  if (a.logicalCounter < b.logicalCounter) return -1;
  if (a.logicalCounter > b.logicalCounter) return 1;

  // Both equal, use node ID as tiebreaker
  if (a.nodeId < b.nodeId) return -1;
  if (a.nodeId > b.nodeId) return 1;

  return 0;
}

/**
 * Format HLC for debugging
 */
function formatHLC(hlc: HLCTimestamp): string {
  return `HLC(${hlc.physicalTime}:${hlc.logicalCounter}@${hlc.nodeId})`;
}
