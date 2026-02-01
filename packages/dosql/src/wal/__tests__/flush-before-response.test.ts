/**
 * WAL Flush Before Response Tests - TDD for sql-gu8s
 *
 * Issue: WAL entries must be flushed to durable storage before mutation
 * responses are returned. Without sync:true on each append, entries
 * sit in memory and can be lost if the DO crashes mid-batch.
 *
 * The WAL writer's append() with { sync: true } forces an immediate
 * flush to durable storage. All mutation operations must use this
 * to guarantee durability before acknowledging success.
 */

import { describe, it, expect } from 'vitest';
import { createWALWriter } from '../writer.js';
import { createMemoryBackend } from '../../fsx/index.js';

// =============================================================================
// WAL Flush Durability Tests
// =============================================================================

describe('WAL flush before mutation response', () => {
  it('append with sync:true should flush immediately to storage', async () => {
    const backend = createMemoryBackend();
    const writer = createWALWriter(backend);

    // Append with sync:true
    const result = await writer.append(
      {
        timestamp: Date.now(),
        txnId: 'txn_1',
        op: 'INSERT',
        table: 'users',
        after: new TextEncoder().encode(JSON.stringify({ id: 1, name: 'Alice' })),
      },
      { sync: true }
    );

    // The entry should have been flushed
    expect(result.flushed).toBe(true);
    expect(result.segmentId).toBeDefined();

    // No pending entries should remain in memory
    expect(writer.getPendingCount()).toBe(0);
  });

  it('append without sync should NOT flush (entries remain in memory)', async () => {
    const backend = createMemoryBackend();
    const writer = createWALWriter(backend);

    const result = await writer.append({
      timestamp: Date.now(),
      txnId: 'txn_1',
      op: 'INSERT',
      table: 'users',
      after: new TextEncoder().encode(JSON.stringify({ id: 1, name: 'Alice' })),
    });

    // Without sync, entry stays in memory buffer
    expect(result.flushed).toBe(false);
    expect(writer.getPendingCount()).toBe(1);
  });

  it('should persist entry to backend when sync:true', async () => {
    const backend = createMemoryBackend();
    const writer = createWALWriter(backend);

    await writer.append(
      {
        timestamp: Date.now(),
        txnId: 'txn_1',
        op: 'INSERT',
        table: 'users',
        after: new TextEncoder().encode(JSON.stringify({ id: 1, name: 'Alice' })),
      },
      { sync: true }
    );

    // Verify data is actually in the storage backend
    const files = await backend.list('_wal/segments/');
    expect(files.length).toBeGreaterThan(0);
  });

  it('multiple sync appends should each flush independently', async () => {
    const backend = createMemoryBackend();
    const writer = createWALWriter(backend);

    // First append with sync
    const result1 = await writer.append(
      {
        timestamp: Date.now(),
        txnId: 'txn_1',
        op: 'INSERT',
        table: 'users',
        after: new TextEncoder().encode(JSON.stringify({ id: 1 })),
      },
      { sync: true }
    );

    expect(result1.flushed).toBe(true);
    expect(writer.getPendingCount()).toBe(0);

    // Second append with sync
    const result2 = await writer.append(
      {
        timestamp: Date.now(),
        txnId: 'txn_2',
        op: 'INSERT',
        table: 'users',
        after: new TextEncoder().encode(JSON.stringify({ id: 2 })),
      },
      { sync: true }
    );

    expect(result2.flushed).toBe(true);
    expect(writer.getPendingCount()).toBe(0);

    // Both segments should be in storage
    const files = await backend.list('_wal/segments/');
    expect(files.length).toBe(2);
  });

  it('crash simulation: unflushed entries are lost', async () => {
    const backend = createMemoryBackend();
    const writer = createWALWriter(backend);

    // Append WITHOUT sync (entries only in memory)
    await writer.append({
      timestamp: Date.now(),
      txnId: 'txn_1',
      op: 'INSERT',
      table: 'users',
      after: new TextEncoder().encode(JSON.stringify({ id: 1 })),
    });

    // Simulate crash: create a new writer from same backend
    // The unflushed entry from the first writer is gone
    const writer2 = createWALWriter(backend);

    // The new writer starts fresh - the unflushed entry was lost
    expect(writer2.getPendingCount()).toBe(0);

    // No segments in storage since nothing was flushed
    const files = await backend.list('_wal/segments/');
    expect(files.length).toBe(0);
  });

  it('crash simulation: sync entries survive', async () => {
    const backend = createMemoryBackend();
    const writer = createWALWriter(backend);

    // Append WITH sync (entry is durable)
    await writer.append(
      {
        timestamp: Date.now(),
        txnId: 'txn_1',
        op: 'INSERT',
        table: 'users',
        after: new TextEncoder().encode(JSON.stringify({ id: 1 })),
      },
      { sync: true }
    );

    // Simulate crash: create a new writer from same backend
    const writer2 = createWALWriter(backend);

    // The sync'd entry is in durable storage and survives the crash
    const files = await backend.list('_wal/segments/');
    expect(files.length).toBe(1);

    // The entry data is readable from the backend
    const segmentData = await backend.read(files[0]);
    expect(segmentData).not.toBeNull();
  });
});
