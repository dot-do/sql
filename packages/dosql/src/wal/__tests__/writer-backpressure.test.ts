/**
 * WAL Writer Backpressure Tests - TDD for sql-j4ze
 *
 * Issue: pendingEntries array grows unbounded if flush fails repeatedly,
 * leading to memory exhaustion. This test verifies that:
 *
 * 1. A maxPendingEntries limit is enforced
 * 2. Append rejects with a WAL error when the limit is exceeded
 * 3. After a successful flush, appends resume normally
 * 4. Default config has a sensible maxPendingEntries value
 */

import { describe, it, expect, vi } from 'vitest';
import { createWALWriter, DefaultWALEncoder } from '../writer.js';
import { DEFAULT_WAL_CONFIG, WALError, WALErrorCode, createTransactionId } from '../types.js';
import type { FSXBackend } from '../../fsx/types.js';

/**
 * Create a mock FSX backend for testing
 */
function createMockBackend(options?: { failWrites?: boolean }): FSXBackend {
  const storage = new Map<string, Uint8Array>();
  return {
    async read(path: string): Promise<Uint8Array | null> {
      return storage.get(path) ?? null;
    },
    async write(path: string, data: Uint8Array): Promise<void> {
      if (options?.failWrites) {
        throw new Error('Simulated write failure');
      }
      storage.set(path, data);
    },
    async delete(path: string): Promise<void> {
      storage.delete(path);
    },
    async list(prefix: string): Promise<string[]> {
      return Array.from(storage.keys()).filter((k) => k.startsWith(prefix));
    },
    async exists(path: string): Promise<boolean> {
      return storage.has(path);
    },
  } as FSXBackend;
}

function makeEntry(table = 'test_table') {
  return {
    timestamp: Date.now(),
    txnId: createTransactionId('txn_test'),
    op: 'INSERT' as const,
    table,
    after: new Uint8Array([1, 2, 3]),
  };
}

describe('WAL Writer Backpressure (sql-j4ze)', () => {
  it('should have a default maxPendingEntries in DEFAULT_WAL_CONFIG', () => {
    // The default config should define a maxPendingEntries limit
    expect(DEFAULT_WAL_CONFIG.maxPendingEntries).toBeDefined();
    expect(DEFAULT_WAL_CONFIG.maxPendingEntries).toBeGreaterThan(0);
  });

  it('should reject appends when pendingEntries exceeds maxPendingEntries', async () => {
    const backend = createMockBackend();
    // Set a very low maxPendingEntries for testing, and high segment thresholds
    // so auto-flush does not trigger
    const writer = createWALWriter(backend, 0n, {
      maxPendingEntries: 5,
      maxEntriesPerSegment: 100000,
      targetSegmentSize: 100 * 1024 * 1024,
    });

    // Append 5 entries (should succeed - at the limit)
    for (let i = 0; i < 5; i++) {
      await writer.append(makeEntry());
    }
    expect(writer.getPendingCount()).toBe(5);

    // The 6th append should fail with backpressure error
    await expect(writer.append(makeEntry())).rejects.toThrow(WALError);
    await expect(writer.append(makeEntry())).rejects.toThrow(/backpressure|pending.*limit|max.*pending/i);

    // Pending count should still be 5 (rejected entry not added)
    expect(writer.getPendingCount()).toBe(5);

    await writer.close();
  });

  it('should resume accepting appends after a successful flush', async () => {
    const backend = createMockBackend();
    const writer = createWALWriter(backend, 0n, {
      maxPendingEntries: 5,
      maxEntriesPerSegment: 100000,
      targetSegmentSize: 100 * 1024 * 1024,
    });

    // Fill to the limit
    for (let i = 0; i < 5; i++) {
      await writer.append(makeEntry());
    }

    // Flush clears pending entries
    const segment = await writer.flush();
    expect(segment).not.toBeNull();
    expect(writer.getPendingCount()).toBe(0);

    // Can append again after flush
    await writer.append(makeEntry());
    expect(writer.getPendingCount()).toBe(1);

    await writer.close();
  });

  it('should not allow unbounded growth when flush fails repeatedly', async () => {
    // Backend that always fails writes
    const backend = createMockBackend({ failWrites: true });
    const writer = createWALWriter(backend, 0n, {
      maxPendingEntries: 10,
      maxEntriesPerSegment: 100000,
      targetSegmentSize: 100 * 1024 * 1024,
    });

    // Append entries up to the limit
    for (let i = 0; i < 10; i++) {
      await writer.append(makeEntry());
    }

    // Further appends should be rejected (backpressure)
    await expect(writer.append(makeEntry())).rejects.toThrow(WALError);

    // Pending count should never exceed the limit
    expect(writer.getPendingCount()).toBeLessThanOrEqual(10);

    // Even trying to flush fails, but pending count stays bounded
    await expect(writer.flush()).rejects.toThrow();
    expect(writer.getPendingCount()).toBeLessThanOrEqual(10);
  });

  it('should auto-flush still works and resets pending count', async () => {
    const backend = createMockBackend();
    const writer = createWALWriter(backend, 0n, {
      maxPendingEntries: 100,
      maxEntriesPerSegment: 5, // Low threshold to trigger auto-flush
      targetSegmentSize: 100 * 1024 * 1024,
    });

    // Append entries - auto-flush should trigger at maxEntriesPerSegment
    for (let i = 0; i < 5; i++) {
      await writer.append(makeEntry());
    }

    // After auto-flush, pending should be reset
    expect(writer.getPendingCount()).toBe(0);

    await writer.close();
  });
});
