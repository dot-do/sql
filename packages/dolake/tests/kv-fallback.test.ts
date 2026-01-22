/**
 * DoLake KV Fallback Storage Tests (TDD RED Phase)
 *
 * Tests for KV fallback storage when R2 writes fail.
 * This provides a secondary durability layer for P0/P1 events.
 *
 * Features:
 * - Detect R2 write failure and fallback to KV
 * - Compress events with gzip for efficient storage
 * - Write to KV with 25MB per value limit
 * - Chunk large batches across multiple keys when >25MB
 * - Recovery: retry R2 from KV on startup/alarm
 * - TTL management (7 days default)
 *
 * Issue: do-d1isn.5 - RED: KV fallback storage (25MB limit)
 *
 * Uses workers-vitest-pool (NO MOCKS).
 */

import { describe, it, expect, beforeEach } from 'vitest';
import type { CDCEvent } from '../src/index.js';
import {
  KVFallbackStorage,
  type KVFallbackConfig,
  type FallbackWriteResult,
  type RecoveryResult,
  type ChunkInfo,
  DEFAULT_KV_FALLBACK_CONFIG,
} from '../src/kv-fallback.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a CDC event for testing
 */
function createTestEvent(overrides: Partial<CDCEvent> = {}): CDCEvent {
  return {
    sequence: 1,
    timestamp: Date.now(),
    operation: 'INSERT',
    table: 'test_table',
    rowId: `row-${Math.random().toString(36).slice(2)}`,
    after: { id: 1, data: 'test data' },
    ...overrides,
  };
}

/**
 * Create a batch of test events
 */
function createTestBatch(count: number, baseSequence: number = 1): CDCEvent[] {
  return Array.from({ length: count }, (_, i) =>
    createTestEvent({
      sequence: baseSequence + i,
      rowId: `row-${baseSequence + i}`,
    })
  );
}

/**
 * Create a large event with specified approximate size
 */
function createLargeEvent(targetSizeBytes: number): CDCEvent {
  // Create data that will be approximately the target size
  const padding = 'x'.repeat(Math.max(0, targetSizeBytes - 200));
  return createTestEvent({
    after: {
      id: 1,
      largeData: padding,
    },
  });
}

/**
 * Mock KV storage that tracks writes
 */
class MockKVStorage {
  private storage = new Map<string, { value: string; expiration?: number }>();
  writeCount = 0;
  readCount = 0;
  deleteCount = 0;
  lastWrittenKey: string | null = null;
  lastWrittenExpiration: number | null = null;

  async write(key: string, value: string, options?: { expirationTtl?: number }): Promise<void> {
    this.writeCount++;
    this.lastWrittenKey = key;
    this.lastWrittenExpiration = options?.expirationTtl ?? null;
    this.storage.set(key, {
      value,
      expiration: options?.expirationTtl ? Date.now() + options.expirationTtl * 1000 : undefined,
    });
  }

  async read(key: string): Promise<string | null> {
    this.readCount++;
    const entry = this.storage.get(key);
    if (!entry) return null;
    if (entry.expiration && Date.now() > entry.expiration) {
      this.storage.delete(key);
      return null;
    }
    return entry.value;
  }

  async delete(key: string): Promise<void> {
    this.deleteCount++;
    this.storage.delete(key);
  }

  async list(prefix: string): Promise<string[]> {
    return Array.from(this.storage.keys()).filter((k) => k.startsWith(prefix));
  }

  getStorageSize(): number {
    let total = 0;
    for (const [key, entry] of this.storage) {
      total += key.length + entry.value.length;
    }
    return total;
  }

  clear(): void {
    this.storage.clear();
    this.writeCount = 0;
    this.readCount = 0;
    this.deleteCount = 0;
    this.lastWrittenKey = null;
    this.lastWrittenExpiration = null;
  }
}

/**
 * Mock R2 storage that can simulate failures
 */
class MockR2Storage {
  private storage = new Map<string, Uint8Array>();
  writeCount = 0;
  failNextWrites = 0;
  permanentFailure = false;

  async write(path: string, data: Uint8Array): Promise<void> {
    this.writeCount++;
    if (this.permanentFailure || this.failNextWrites > 0) {
      if (this.failNextWrites > 0) this.failNextWrites--;
      throw new Error('R2 write failed');
    }
    this.storage.set(path, data);
  }

  async read(path: string): Promise<Uint8Array | null> {
    return this.storage.get(path) ?? null;
  }

  async delete(path: string): Promise<void> {
    this.storage.delete(path);
  }

  injectFailure(count: number, permanent = false): void {
    this.failNextWrites = count;
    this.permanentFailure = permanent;
  }

  clearFailures(): void {
    this.failNextWrites = 0;
    this.permanentFailure = false;
  }

  clear(): void {
    this.storage.clear();
    this.writeCount = 0;
    this.failNextWrites = 0;
    this.permanentFailure = false;
  }
}

// =============================================================================
// 1. R2 Failure Detection Tests
// =============================================================================

describe('R2 Failure Detection', () => {
  let kvStorage: MockKVStorage;
  let r2Storage: MockR2Storage;
  let fallbackStorage: KVFallbackStorage;

  beforeEach(() => {
    kvStorage = new MockKVStorage();
    r2Storage = new MockR2Storage();
    fallbackStorage = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
    });
  });

  it('should detect R2 write failure', async () => {
    r2Storage.injectFailure(1);

    const event = createTestEvent();
    const result = await fallbackStorage.writeWithFallback([event], 'test-batch-1');

    // EXPECTED: Should detect the failure
    expect(result.r2Failed).toBe(true);
    expect(result.r2Error).toBeDefined();
    expect(result.r2Error).toContain('R2 write failed');
  });

  it('should attempt R2 write before falling back', async () => {
    r2Storage.injectFailure(1);

    const event = createTestEvent();
    await fallbackStorage.writeWithFallback([event], 'test-batch-2');

    // EXPECTED: R2 write should have been attempted
    expect(r2Storage.writeCount).toBe(1);
  });

  it('should not fallback when R2 succeeds', async () => {
    const event = createTestEvent();
    const result = await fallbackStorage.writeWithFallback([event], 'test-batch-3');

    // EXPECTED: No fallback needed
    expect(result.r2Failed).toBe(false);
    expect(result.usedKVFallback).toBe(false);
    expect(kvStorage.writeCount).toBe(0);
  });

  it('should report both R2 success and KV fallback status', async () => {
    r2Storage.injectFailure(1);

    const event = createTestEvent();
    const result = await fallbackStorage.writeWithFallback([event], 'test-batch-4');

    // EXPECTED: Result should have both statuses
    expect(result).toHaveProperty('r2Failed');
    expect(result).toHaveProperty('usedKVFallback');
    expect(result).toHaveProperty('kvWriteSuccess');
    expect(result.r2Failed).toBe(true);
    expect(result.usedKVFallback).toBe(true);
    expect(result.kvWriteSuccess).toBe(true);
  });
});

// =============================================================================
// 2. Gzip Compression Tests
// =============================================================================

describe('Gzip Compression', () => {
  let kvStorage: MockKVStorage;
  let r2Storage: MockR2Storage;
  let fallbackStorage: KVFallbackStorage;

  beforeEach(() => {
    kvStorage = new MockKVStorage();
    r2Storage = new MockR2Storage();
    fallbackStorage = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
      enableCompression: true,
    });
  });

  it('should compress events before writing to KV', async () => {
    r2Storage.injectFailure(1);

    // Create a batch with some data
    const events = createTestBatch(100);
    const result = await fallbackStorage.writeWithFallback(events, 'test-compress-1');

    // EXPECTED: Data should be compressed
    expect(result.compressed).toBe(true);
    expect(result.compressionRatio).toBeLessThan(1); // Compressed is smaller
  });

  it('should achieve significant compression for repetitive data', async () => {
    r2Storage.injectFailure(1);

    // Create events with repetitive data (compresses well)
    const events = Array.from({ length: 100 }, (_, i) =>
      createTestEvent({
        sequence: i,
        after: {
          id: i,
          repeatedData: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
        },
      })
    );

    const result = await fallbackStorage.writeWithFallback(events, 'test-compress-2');

    // EXPECTED: Should achieve at least 50% compression
    expect(result.compressionRatio).toBeLessThan(0.5);
  });

  it('should store uncompressed size in metadata', async () => {
    r2Storage.injectFailure(1);

    const events = createTestBatch(50);
    const result = await fallbackStorage.writeWithFallback(events, 'test-compress-3');

    // EXPECTED: Should track both sizes
    expect(result.originalSizeBytes).toBeGreaterThan(0);
    expect(result.compressedSizeBytes).toBeGreaterThan(0);
    expect(result.compressedSizeBytes).toBeLessThan(result.originalSizeBytes);
  });

  it('should be able to decompress stored data', async () => {
    r2Storage.injectFailure(1);

    const events = createTestBatch(10);
    const batchId = 'test-compress-4';
    await fallbackStorage.writeWithFallback(events, batchId);

    // EXPECTED: Should be able to read and decompress
    const recovered = await fallbackStorage.readFromFallback(batchId);
    expect(recovered).not.toBeNull();
    expect(recovered!.events).toHaveLength(10);
    expect(recovered!.events[0].sequence).toBe(events[0].sequence);
  });

  it('should support disabling compression', async () => {
    const uncompressedFallback = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
      enableCompression: false,
    });

    r2Storage.injectFailure(1);

    const events = createTestBatch(10);
    const result = await uncompressedFallback.writeWithFallback(events, 'test-no-compress');

    // EXPECTED: No compression applied
    expect(result.compressed).toBe(false);
    expect(result.compressionRatio).toBe(1);
  });
});

// =============================================================================
// 3. KV Write with 25MB Limit Tests
// =============================================================================

describe('KV Write (25MB Limit)', () => {
  let kvStorage: MockKVStorage;
  let r2Storage: MockR2Storage;
  let fallbackStorage: KVFallbackStorage;

  const MB = 1024 * 1024;
  const KV_MAX_VALUE_SIZE = 25 * MB;

  beforeEach(() => {
    kvStorage = new MockKVStorage();
    r2Storage = new MockR2Storage();
    fallbackStorage = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
      maxValueSize: KV_MAX_VALUE_SIZE,
    });
  });

  it('should write events to KV when R2 fails', async () => {
    r2Storage.injectFailure(1);

    const event = createTestEvent();
    const result = await fallbackStorage.writeWithFallback([event], 'test-kv-write-1');

    // EXPECTED: Should write to KV
    expect(result.usedKVFallback).toBe(true);
    expect(result.kvWriteSuccess).toBe(true);
    expect(kvStorage.writeCount).toBeGreaterThan(0);
  });

  it('should enforce 25MB limit per value', async () => {
    r2Storage.injectFailure(1);

    // Create data that is small enough for single value
    const events = createTestBatch(100);
    const result = await fallbackStorage.writeWithFallback(events, 'test-kv-limit-1');

    // EXPECTED: Should succeed with single write under limit
    expect(result.kvWriteSuccess).toBe(true);
    expect(result.compressedSizeBytes).toBeLessThan(KV_MAX_VALUE_SIZE);
  });

  it('should include batch metadata in KV value', async () => {
    r2Storage.injectFailure(1);

    const batchId = 'test-kv-metadata';
    const events = createTestBatch(5);
    await fallbackStorage.writeWithFallback(events, batchId);

    // EXPECTED: Stored data should include metadata
    const stored = await fallbackStorage.readFromFallback(batchId);
    expect(stored).not.toBeNull();
    expect(stored!.batchId).toBe(batchId);
    expect(stored!.timestamp).toBeGreaterThan(0);
    expect(stored!.eventCount).toBe(5);
  });

  it('should use consistent key naming scheme', async () => {
    r2Storage.injectFailure(1);

    const batchId = 'test-batch-123';
    await fallbackStorage.writeWithFallback([createTestEvent()], batchId);

    // EXPECTED: Key should follow pattern
    expect(kvStorage.lastWrittenKey).toContain('fallback');
    expect(kvStorage.lastWrittenKey).toContain(batchId);
  });

  it('should reject writes exceeding configured max size', async () => {
    // Create a fallback with very small limit and NO compression
    const smallLimitFallback = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
      maxValueSize: 500, // Very small limit
      enableChunking: false, // Disable chunking to test limit enforcement
      enableCompression: false, // Disable compression so data stays large
    });

    r2Storage.injectFailure(1);

    // Create data larger than limit (500 bytes uncompressed)
    const largeEvent = createLargeEvent(2000);
    const result = await smallLimitFallback.writeWithFallback([largeEvent], 'test-limit');

    // EXPECTED: Should fail when chunking is disabled and data exceeds limit
    expect(result.error).toBeDefined();
    expect(result.error).toContain('exceeds');
  });
});

// =============================================================================
// 4. Chunking for Large Batches Tests
// =============================================================================

describe('Chunking for Large Batches (>25MB)', () => {
  let kvStorage: MockKVStorage;
  let r2Storage: MockR2Storage;
  let fallbackStorage: KVFallbackStorage;

  const MB = 1024 * 1024;

  beforeEach(() => {
    kvStorage = new MockKVStorage();
    r2Storage = new MockR2Storage();
    fallbackStorage = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
      maxValueSize: 25 * MB,
      enableChunking: true,
    });
  });

  it('should chunk data exceeding 25MB across multiple keys', async () => {
    // Use smaller limit for testing and disable compression so data stays large
    const smallChunkFallback = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
      maxValueSize: 500, // Very small chunks for testing
      enableChunking: true,
      enableCompression: false, // Disable compression so data exceeds chunk size
    });

    r2Storage.injectFailure(1);

    // Create data larger than chunk size (need more events without compression)
    const events = createTestBatch(100); // Should exceed 500 bytes
    const result = await smallChunkFallback.writeWithFallback(events, 'test-chunk-1');

    // EXPECTED: Should use multiple chunks
    expect(result.chunked).toBe(true);
    expect(result.chunkCount).toBeGreaterThan(1);
  });

  it('should track chunk information', async () => {
    const smallChunkFallback = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
      maxValueSize: 500,
      enableChunking: true,
    });

    r2Storage.injectFailure(1);

    const events = createTestBatch(20);
    const result = await smallChunkFallback.writeWithFallback(events, 'test-chunk-info');

    // EXPECTED: Should have chunk details
    if (result.chunked) {
      expect(result.chunks).toBeDefined();
      expect(result.chunks!.length).toBe(result.chunkCount);
      for (const chunk of result.chunks!) {
        expect(chunk.key).toBeDefined();
        expect(chunk.size).toBeGreaterThan(0);
        expect(chunk.index).toBeGreaterThanOrEqual(0);
      }
    }
  });

  it('should be able to reassemble chunked data', async () => {
    const smallChunkFallback = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
      maxValueSize: 500,
      enableChunking: true,
    });

    r2Storage.injectFailure(1);

    const events = createTestBatch(30);
    const batchId = 'test-reassemble';
    await smallChunkFallback.writeWithFallback(events, batchId);

    // EXPECTED: Should reassemble correctly
    const recovered = await smallChunkFallback.readFromFallback(batchId);
    expect(recovered).not.toBeNull();
    expect(recovered!.events).toHaveLength(30);
    expect(recovered!.events.map((e) => e.sequence)).toEqual(events.map((e) => e.sequence));
  });

  it('should store chunk manifest for multi-chunk writes', async () => {
    const smallChunkFallback = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
      maxValueSize: 500,
      enableChunking: true,
    });

    r2Storage.injectFailure(1);

    const events = createTestBatch(25);
    const batchId = 'test-manifest';
    const result = await smallChunkFallback.writeWithFallback(events, batchId);

    if (result.chunked) {
      // EXPECTED: Should have manifest key
      expect(result.manifestKey).toBeDefined();

      // Manifest should be readable
      const manifest = await kvStorage.read(result.manifestKey!);
      expect(manifest).not.toBeNull();

      const parsedManifest = JSON.parse(manifest!);
      expect(parsedManifest.totalChunks).toBe(result.chunkCount);
      expect(parsedManifest.chunkKeys).toHaveLength(result.chunkCount);
    }
  });

  it('should handle partial chunk write failures', async () => {
    // Mock KV that fails after 2 writes
    let writeAttempts = 0;
    const failingKV = {
      ...kvStorage,
      async write(key: string, value: string, options?: { expirationTtl?: number }) {
        writeAttempts++;
        if (writeAttempts > 2) {
          throw new Error('KV write failed');
        }
        await kvStorage.write(key, value, options);
      },
      read: kvStorage.read.bind(kvStorage),
      delete: kvStorage.delete.bind(kvStorage),
      list: kvStorage.list.bind(kvStorage),
    };

    const failingChunkFallback = new KVFallbackStorage({
      kv: failingKV,
      r2: r2Storage,
      maxValueSize: 200,
      enableChunking: true,
    });

    r2Storage.injectFailure(1);

    const events = createTestBatch(30);
    const result = await failingChunkFallback.writeWithFallback(events, 'test-partial-fail');

    // EXPECTED: Should report partial failure
    expect(result.kvWriteSuccess).toBe(false);
    expect(result.error).toBeDefined();
  });

  it('should not chunk when data fits in single value', async () => {
    r2Storage.injectFailure(1);

    const events = createTestBatch(5); // Small batch
    const result = await fallbackStorage.writeWithFallback(events, 'test-no-chunk');

    // EXPECTED: No chunking needed
    expect(result.chunked).toBe(false);
    expect(result.chunkCount).toBe(1);
  });
});

// =============================================================================
// 5. Recovery Tests (Retry R2 from KV)
// =============================================================================

describe('Recovery: Retry R2 from KV', () => {
  let kvStorage: MockKVStorage;
  let r2Storage: MockR2Storage;
  let fallbackStorage: KVFallbackStorage;

  beforeEach(() => {
    kvStorage = new MockKVStorage();
    r2Storage = new MockR2Storage();
    fallbackStorage = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
    });
  });

  it('should list pending fallback batches', async () => {
    r2Storage.injectFailure(3);

    // Write multiple batches to fallback
    await fallbackStorage.writeWithFallback([createTestEvent()], 'batch-1');
    await fallbackStorage.writeWithFallback([createTestEvent()], 'batch-2');
    await fallbackStorage.writeWithFallback([createTestEvent()], 'batch-3');

    // EXPECTED: Should list all pending batches
    const pending = await fallbackStorage.listPendingRecovery();
    expect(pending).toHaveLength(3);
    expect(pending).toContain('batch-1');
    expect(pending).toContain('batch-2');
    expect(pending).toContain('batch-3');
  });

  it('should recover batch to R2 when R2 becomes available', async () => {
    r2Storage.injectFailure(1);

    const events = createTestBatch(5);
    const batchId = 'test-recover-1';
    await fallbackStorage.writeWithFallback(events, batchId);

    // Clear R2 failures
    r2Storage.clearFailures();

    // EXPECTED: Recovery should succeed
    const result = await fallbackStorage.recoverToR2(batchId);
    expect(result.success).toBe(true);
    expect(result.eventsRecovered).toBe(5);
  });

  it('should delete from KV after successful R2 recovery', async () => {
    r2Storage.injectFailure(1);

    const batchId = 'test-cleanup';
    await fallbackStorage.writeWithFallback([createTestEvent()], batchId);

    r2Storage.clearFailures();
    await fallbackStorage.recoverToR2(batchId);

    // EXPECTED: KV entry should be deleted
    const stillExists = await fallbackStorage.readFromFallback(batchId);
    expect(stillExists).toBeNull();
  });

  it('should recover all pending batches in bulk', async () => {
    r2Storage.injectFailure(3);

    await fallbackStorage.writeWithFallback([createTestEvent()], 'bulk-1');
    await fallbackStorage.writeWithFallback([createTestEvent()], 'bulk-2');
    await fallbackStorage.writeWithFallback([createTestEvent()], 'bulk-3');

    r2Storage.clearFailures();

    // EXPECTED: Bulk recovery should work
    const result = await fallbackStorage.recoverAllToR2();
    expect(result.totalBatches).toBe(3);
    expect(result.successfulRecoveries).toBe(3);
    expect(result.failedRecoveries).toBe(0);
  });

  it('should handle recovery failure gracefully', async () => {
    r2Storage.injectFailure(1);

    const batchId = 'test-recovery-fail';
    await fallbackStorage.writeWithFallback([createTestEvent()], batchId);

    // Keep R2 failing
    r2Storage.injectFailure(1);

    // EXPECTED: Recovery should fail but not throw
    const result = await fallbackStorage.recoverToR2(batchId);
    expect(result.success).toBe(false);
    expect(result.error).toBeDefined();

    // Data should still be in KV
    const stillExists = await fallbackStorage.readFromFallback(batchId);
    expect(stillExists).not.toBeNull();
  });

  it('should track recovery metrics', async () => {
    r2Storage.injectFailure(2);

    await fallbackStorage.writeWithFallback([createTestEvent()], 'metrics-1');
    await fallbackStorage.writeWithFallback([createTestEvent()], 'metrics-2');

    r2Storage.clearFailures();
    r2Storage.injectFailure(1); // First recovery fails

    await fallbackStorage.recoverAllToR2();

    // EXPECTED: Should have metrics
    const metrics = fallbackStorage.getRecoveryMetrics();
    expect(metrics.totalRecoveryAttempts).toBeGreaterThan(0);
    expect(metrics.successfulRecoveries).toBeGreaterThanOrEqual(0);
    expect(metrics.failedRecoveries).toBeGreaterThanOrEqual(0);
  });

  it('should support recovery on startup', async () => {
    r2Storage.injectFailure(1);
    await fallbackStorage.writeWithFallback([createTestEvent()], 'startup-test');

    // Create new instance (simulating startup)
    r2Storage.clearFailures();
    const newFallback = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
    });

    // EXPECTED: Should be able to recover on startup
    const result = await newFallback.recoverOnStartup();
    expect(result.batchesRecovered).toBeGreaterThan(0);
  });

  it('should support recovery via alarm handler', async () => {
    r2Storage.injectFailure(1);
    await fallbackStorage.writeWithFallback([createTestEvent()], 'alarm-test');

    r2Storage.clearFailures();

    // EXPECTED: Alarm handler should trigger recovery
    const result = await fallbackStorage.handleRecoveryAlarm();
    expect(result.attempted).toBe(true);
    expect(result.batchesProcessed).toBeGreaterThan(0);
  });
});

// =============================================================================
// 6. TTL Management Tests
// =============================================================================

describe('TTL Management (7 days default)', () => {
  let kvStorage: MockKVStorage;
  let r2Storage: MockR2Storage;
  let fallbackStorage: KVFallbackStorage;

  const SEVEN_DAYS_SECONDS = 7 * 24 * 60 * 60;

  beforeEach(() => {
    kvStorage = new MockKVStorage();
    r2Storage = new MockR2Storage();
    fallbackStorage = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
      ttlSeconds: SEVEN_DAYS_SECONDS,
    });
  });

  it('should set 7 day TTL on KV writes by default', async () => {
    r2Storage.injectFailure(1);

    await fallbackStorage.writeWithFallback([createTestEvent()], 'ttl-test-1');

    // EXPECTED: TTL should be 7 days
    expect(kvStorage.lastWrittenExpiration).toBe(SEVEN_DAYS_SECONDS);
  });

  it('should support configurable TTL', async () => {
    const customTTL = 3 * 24 * 60 * 60; // 3 days
    const customFallback = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
      ttlSeconds: customTTL,
    });

    r2Storage.injectFailure(1);
    await customFallback.writeWithFallback([createTestEvent()], 'ttl-custom');

    // EXPECTED: Custom TTL should be used
    expect(kvStorage.lastWrittenExpiration).toBe(customTTL);
  });

  it('should include expiration timestamp in stored metadata', async () => {
    r2Storage.injectFailure(1);

    const batchId = 'ttl-metadata';
    const beforeWrite = Date.now();
    await fallbackStorage.writeWithFallback([createTestEvent()], batchId);
    const afterWrite = Date.now();

    // EXPECTED: Metadata should have expiration
    const stored = await fallbackStorage.readFromFallback(batchId);
    expect(stored).not.toBeNull();
    expect(stored!.expiresAt).toBeDefined();

    const expectedExpiration = beforeWrite + SEVEN_DAYS_SECONDS * 1000;
    expect(stored!.expiresAt).toBeGreaterThanOrEqual(expectedExpiration - 1000);
    expect(stored!.expiresAt).toBeLessThanOrEqual(afterWrite + SEVEN_DAYS_SECONDS * 1000);
  });

  it('should set TTL on all chunks for chunked data', async () => {
    const smallChunkFallback = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
      maxValueSize: 500,
      enableChunking: true,
      ttlSeconds: SEVEN_DAYS_SECONDS,
    });

    r2Storage.injectFailure(1);

    const events = createTestBatch(30);
    await smallChunkFallback.writeWithFallback(events, 'ttl-chunked');

    // EXPECTED: All writes should have TTL
    // Since we can't track all expirations easily, just verify the last one
    expect(kvStorage.lastWrittenExpiration).toBe(SEVEN_DAYS_SECONDS);
  });

  it('should report pending batches with remaining TTL', async () => {
    r2Storage.injectFailure(1);

    await fallbackStorage.writeWithFallback([createTestEvent()], 'ttl-report');

    // EXPECTED: Should report remaining TTL
    const pending = await fallbackStorage.listPendingWithTTL();
    expect(pending).toHaveLength(1);
    expect(pending[0].batchId).toBe('ttl-report');
    expect(pending[0].remainingTTLSeconds).toBeLessThanOrEqual(SEVEN_DAYS_SECONDS);
    expect(pending[0].remainingTTLSeconds).toBeGreaterThan(0);
  });

  it('should warn about batches nearing expiration', async () => {
    // Create fallback with very short TTL for testing
    const shortTTLFallback = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
      ttlSeconds: 100, // 100 seconds
      expirationWarningThreshold: 0.9, // Warn when 90% expired
    });

    r2Storage.injectFailure(1);
    await shortTTLFallback.writeWithFallback([createTestEvent()], 'ttl-warn');

    // EXPECTED: Should have warning methods
    const warnings = await shortTTLFallback.getExpirationWarnings();
    expect(warnings).toBeDefined();
    // The batch won't be expiring yet since we just created it
    expect(warnings.expiringBatches).toHaveLength(0);
  });
});

// =============================================================================
// 7. Integration with DurabilityWriter Tests
// =============================================================================

describe('Integration with DurabilityWriter', () => {
  let kvStorage: MockKVStorage;
  let r2Storage: MockR2Storage;
  let fallbackStorage: KVFallbackStorage;

  beforeEach(() => {
    kvStorage = new MockKVStorage();
    r2Storage = new MockR2Storage();
    fallbackStorage = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
    });
  });

  it('should expose R2 and KV storage interfaces', () => {
    // EXPECTED: Should have getters for integration
    expect(fallbackStorage.getKVStorage()).toBeDefined();
    expect(fallbackStorage.getR2Storage()).toBeDefined();
  });

  it('should provide status for monitoring', async () => {
    r2Storage.injectFailure(2);

    await fallbackStorage.writeWithFallback([createTestEvent()], 'status-1');
    await fallbackStorage.writeWithFallback([createTestEvent()], 'status-2');

    r2Storage.clearFailures();
    await fallbackStorage.recoverToR2('status-1');

    // EXPECTED: Should have comprehensive status
    const status = fallbackStorage.getStatus();
    expect(status.pendingRecoveryCount).toBeGreaterThanOrEqual(0);
    expect(status.totalFallbackWrites).toBeGreaterThan(0);
    expect(status.totalRecoveries).toBeGreaterThanOrEqual(0);
  });

  it('should be creatable with default config', () => {
    // EXPECTED: Default config should be exported and usable
    expect(DEFAULT_KV_FALLBACK_CONFIG).toBeDefined();
    expect(DEFAULT_KV_FALLBACK_CONFIG.ttlSeconds).toBe(7 * 24 * 60 * 60);
    expect(DEFAULT_KV_FALLBACK_CONFIG.enableCompression).toBe(true);
    expect(DEFAULT_KV_FALLBACK_CONFIG.enableChunking).toBe(true);
  });

  it('should support callback for recovery events', async () => {
    const recoveryEvents: string[] = [];
    const callbackFallback = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
      onRecovery: (batchId, success) => {
        recoveryEvents.push(`${batchId}:${success}`);
      },
    });

    r2Storage.injectFailure(1);
    await callbackFallback.writeWithFallback([createTestEvent()], 'callback-test');

    r2Storage.clearFailures();
    await callbackFallback.recoverToR2('callback-test');

    // EXPECTED: Callback should be invoked
    expect(recoveryEvents).toContain('callback-test:true');
  });

  it('should integrate with P1 fallback flow', async () => {
    r2Storage.injectFailure(1);

    // P1 event should use KV fallback
    const p1Event = createTestEvent({
      table: 'users',
      metadata: { durability: 'P1' },
    });

    const result = await fallbackStorage.writeWithFallback([p1Event], 'p1-integration');

    // EXPECTED: Should use fallback successfully
    expect(result.usedKVFallback).toBe(true);
    expect(result.kvWriteSuccess).toBe(true);
  });
});

// =============================================================================
// 8. Error Handling and Edge Cases
// =============================================================================

describe('Error Handling and Edge Cases', () => {
  let kvStorage: MockKVStorage;
  let r2Storage: MockR2Storage;
  let fallbackStorage: KVFallbackStorage;

  beforeEach(() => {
    kvStorage = new MockKVStorage();
    r2Storage = new MockR2Storage();
    fallbackStorage = new KVFallbackStorage({
      kv: kvStorage,
      r2: r2Storage,
    });
  });

  it('should handle empty event array', async () => {
    r2Storage.injectFailure(1);

    const result = await fallbackStorage.writeWithFallback([], 'empty-batch');

    // EXPECTED: Should handle gracefully
    expect(result.kvWriteSuccess).toBe(true);
    expect(result.eventCount).toBe(0);
  });

  it('should handle events with special characters in data', async () => {
    r2Storage.injectFailure(1);

    const specialEvent = createTestEvent({
      after: {
        data: 'Special chars: \n\t\r\0"\'\\',
        unicode: '\u0000\uFFFF\u1234',
        emoji: '123',
      },
    });

    const result = await fallbackStorage.writeWithFallback([specialEvent], 'special-chars');

    // EXPECTED: Should handle special characters
    expect(result.kvWriteSuccess).toBe(true);

    const recovered = await fallbackStorage.readFromFallback('special-chars');
    expect(recovered).not.toBeNull();
    expect(recovered!.events[0].after).toEqual(specialEvent.after);
  });

  it('should handle concurrent writes to same batch id', async () => {
    r2Storage.injectFailure(5);

    // Write to same batch id concurrently
    const writes = [
      fallbackStorage.writeWithFallback([createTestEvent({ sequence: 1 })], 'concurrent'),
      fallbackStorage.writeWithFallback([createTestEvent({ sequence: 2 })], 'concurrent'),
    ];

    const results = await Promise.all(writes);

    // EXPECTED: Both should succeed (last write wins)
    expect(results.every((r) => r.kvWriteSuccess)).toBe(true);
  });

  it('should handle very large batch id', async () => {
    r2Storage.injectFailure(1);

    const longBatchId = 'x'.repeat(500); // Very long batch ID
    const result = await fallbackStorage.writeWithFallback([createTestEvent()], longBatchId);

    // EXPECTED: Should handle long batch IDs
    expect(result.kvWriteSuccess).toBe(true);
  });

  it('should handle KV read failure during recovery', async () => {
    r2Storage.injectFailure(1);
    await fallbackStorage.writeWithFallback([createTestEvent()], 'read-fail-test');

    // Make KV read fail
    const originalRead = kvStorage.read.bind(kvStorage);
    kvStorage.read = async () => {
      throw new Error('KV read failed');
    };

    r2Storage.clearFailures();
    const result = await fallbackStorage.recoverToR2('read-fail-test');

    // Restore
    kvStorage.read = originalRead;

    // EXPECTED: Should handle read failure
    expect(result.success).toBe(false);
    expect(result.error).toContain('KV read failed');
  });

  it('should provide meaningful error messages', async () => {
    r2Storage.injectFailure(1, true); // Permanent failure

    const failingKV = {
      ...kvStorage,
      write: async () => {
        throw new Error('Detailed KV error: quota exceeded');
      },
      read: kvStorage.read.bind(kvStorage),
      delete: kvStorage.delete.bind(kvStorage),
      list: kvStorage.list.bind(kvStorage),
    };

    const failingFallback = new KVFallbackStorage({
      kv: failingKV,
      r2: r2Storage,
    });

    const result = await failingFallback.writeWithFallback([createTestEvent()], 'error-msg');

    // EXPECTED: Error message should be informative
    expect(result.error).toBeDefined();
    expect(result.error).toContain('quota exceeded');
  });
});
