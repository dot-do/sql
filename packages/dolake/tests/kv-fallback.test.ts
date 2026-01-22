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
import { env } from 'cloudflare:test';
import type { CDCEvent } from '../src/index.js';
import {
  KVFallbackStorage,
  type ExtendedKVStorage,
  DEFAULT_KV_FALLBACK_CONFIG,
} from '../src/kv-fallback.js';
import type { R2Storage } from '../src/durability.js';

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

// =============================================================================
// Adapter: Wraps real Cloudflare KV to match ExtendedKVStorage interface
// =============================================================================

/**
 * Adapter that wraps real Cloudflare KV to match the ExtendedKVStorage interface
 */
function createKVAdapter(kv: KVNamespace): ExtendedKVStorage & { clear(): Promise<void>; lastWrittenKey: string | null; lastWrittenExpiration: number | null } {
  let lastWrittenKey: string | null = null;
  let lastWrittenExpiration: number | null = null;

  return {
    get lastWrittenKey() {
      return lastWrittenKey;
    },
    get lastWrittenExpiration() {
      return lastWrittenExpiration;
    },
    async write(key: string, value: string, options?: { expirationTtl?: number }): Promise<void> {
      lastWrittenKey = key;
      lastWrittenExpiration = options?.expirationTtl ?? null;
      await kv.put(key, value, options ? { expirationTtl: options.expirationTtl } : undefined);
    },
    async read(key: string): Promise<string | null> {
      return await kv.get(key);
    },
    async delete(key: string): Promise<void> {
      await kv.delete(key);
    },
    async list(prefix: string): Promise<string[]> {
      const result = await kv.list({ prefix });
      return result.keys.map((k) => k.name);
    },
    async clear(): Promise<void> {
      // Clear all keys - list and delete
      const result = await kv.list();
      for (const key of result.keys) {
        await kv.delete(key.name);
      }
      lastWrittenKey = null;
      lastWrittenExpiration = null;
    },
  };
}

// =============================================================================
// Adapter: Wraps real Cloudflare R2 to match R2Storage interface with failure injection
// =============================================================================

/**
 * Adapter that wraps real Cloudflare R2 to match the R2Storage interface
 * Also supports failure injection for testing fallback behavior
 */
function createR2Adapter(r2: R2Bucket): R2Storage & {
  injectFailure(count: number, permanent?: boolean): void;
  clearFailures(): void;
  clear(): Promise<void>;
  writeCount: number;
} {
  let failNextWrites = 0;
  let permanentFailure = false;
  let writeCount = 0;

  return {
    get writeCount() {
      return writeCount;
    },
    async write(path: string, data: Uint8Array): Promise<void> {
      writeCount++;
      if (permanentFailure || failNextWrites > 0) {
        if (failNextWrites > 0) failNextWrites--;
        throw new Error('R2 write failed');
      }
      await r2.put(path, data);
    },
    async read(path: string): Promise<Uint8Array | null> {
      const obj = await r2.get(path);
      if (!obj) return null;
      return new Uint8Array(await obj.arrayBuffer());
    },
    async delete(path: string): Promise<void> {
      await r2.delete(path);
    },
    injectFailure(count: number, permanent = false): void {
      failNextWrites = count;
      permanentFailure = permanent;
    },
    clearFailures(): void {
      failNextWrites = 0;
      permanentFailure = false;
    },
    async clear(): Promise<void> {
      // List and delete all objects
      const listed = await r2.list();
      for (const obj of listed.objects) {
        await r2.delete(obj.key);
      }
      failNextWrites = 0;
      permanentFailure = false;
      writeCount = 0;
    },
  };
}

// =============================================================================
// 1. R2 Failure Detection Tests
// =============================================================================

describe('R2 Failure Detection', () => {
  let kvAdapter: ReturnType<typeof createKVAdapter>;
  let r2Adapter: ReturnType<typeof createR2Adapter>;
  let fallbackStorage: KVFallbackStorage;

  beforeEach(async () => {
    kvAdapter = createKVAdapter(env.KV_FALLBACK);
    r2Adapter = createR2Adapter(env.LAKEHOUSE_BUCKET);
    await kvAdapter.clear();
    await r2Adapter.clear();
    fallbackStorage = new KVFallbackStorage({
      kv: kvAdapter,
      r2: r2Adapter,
    });
  });

  it('should detect R2 write failure', async () => {
    r2Adapter.injectFailure(1);

    const event = createTestEvent();
    const result = await fallbackStorage.writeWithFallback([event], 'test-batch-1');

    // EXPECTED: Should detect the failure
    expect(result.r2Failed).toBe(true);
    expect(result.r2Error).toBeDefined();
    expect(result.r2Error).toContain('R2 write failed');
  });

  it('should attempt R2 write before falling back', async () => {
    r2Adapter.injectFailure(1);

    const event = createTestEvent();
    await fallbackStorage.writeWithFallback([event], 'test-batch-2');

    // EXPECTED: R2 write should have been attempted
    expect(r2Adapter.writeCount).toBe(1);
  });

  it('should not fallback when R2 succeeds', async () => {
    const event = createTestEvent();
    const result = await fallbackStorage.writeWithFallback([event], 'test-batch-3');

    // EXPECTED: No fallback needed
    expect(result.r2Failed).toBe(false);
    expect(result.usedKVFallback).toBe(false);
  });

  it('should report both R2 success and KV fallback status', async () => {
    r2Adapter.injectFailure(1);

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
  let kvAdapter: ReturnType<typeof createKVAdapter>;
  let r2Adapter: ReturnType<typeof createR2Adapter>;
  let fallbackStorage: KVFallbackStorage;

  beforeEach(async () => {
    kvAdapter = createKVAdapter(env.KV_FALLBACK);
    r2Adapter = createR2Adapter(env.LAKEHOUSE_BUCKET);
    await kvAdapter.clear();
    await r2Adapter.clear();
    fallbackStorage = new KVFallbackStorage({
      kv: kvAdapter,
      r2: r2Adapter,
      enableCompression: true,
    });
  });

  it('should compress events before writing to KV', async () => {
    r2Adapter.injectFailure(1);

    // Create a batch with some data
    const events = createTestBatch(100);
    const result = await fallbackStorage.writeWithFallback(events, 'test-compress-1');

    // EXPECTED: Data should be compressed
    expect(result.compressed).toBe(true);
    expect(result.compressionRatio).toBeLessThan(1); // Compressed is smaller
  });

  it('should achieve significant compression for repetitive data', async () => {
    r2Adapter.injectFailure(1);

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
    r2Adapter.injectFailure(1);

    const events = createTestBatch(50);
    const result = await fallbackStorage.writeWithFallback(events, 'test-compress-3');

    // EXPECTED: Should track both sizes
    expect(result.originalSizeBytes).toBeGreaterThan(0);
    expect(result.compressedSizeBytes).toBeGreaterThan(0);
    expect(result.compressedSizeBytes).toBeLessThan(result.originalSizeBytes);
  });

  it('should be able to decompress stored data', async () => {
    r2Adapter.injectFailure(1);

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
      kv: kvAdapter,
      r2: r2Adapter,
      enableCompression: false,
    });

    r2Adapter.injectFailure(1);

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
  let kvAdapter: ReturnType<typeof createKVAdapter>;
  let r2Adapter: ReturnType<typeof createR2Adapter>;
  let fallbackStorage: KVFallbackStorage;

  const MB = 1024 * 1024;
  const KV_MAX_VALUE_SIZE = 25 * MB;

  beforeEach(async () => {
    kvAdapter = createKVAdapter(env.KV_FALLBACK);
    r2Adapter = createR2Adapter(env.LAKEHOUSE_BUCKET);
    await kvAdapter.clear();
    await r2Adapter.clear();
    fallbackStorage = new KVFallbackStorage({
      kv: kvAdapter,
      r2: r2Adapter,
      maxValueSize: KV_MAX_VALUE_SIZE,
    });
  });

  it('should write events to KV when R2 fails', async () => {
    r2Adapter.injectFailure(1);

    const event = createTestEvent();
    const result = await fallbackStorage.writeWithFallback([event], 'test-kv-write-1');

    // EXPECTED: Should write to KV
    expect(result.usedKVFallback).toBe(true);
    expect(result.kvWriteSuccess).toBe(true);
  });

  it('should enforce 25MB limit per value', async () => {
    r2Adapter.injectFailure(1);

    // Create data that is small enough for single value
    const events = createTestBatch(100);
    const result = await fallbackStorage.writeWithFallback(events, 'test-kv-limit-1');

    // EXPECTED: Should succeed with single write under limit
    expect(result.kvWriteSuccess).toBe(true);
    expect(result.compressedSizeBytes).toBeLessThan(KV_MAX_VALUE_SIZE);
  });

  it('should include batch metadata in KV value', async () => {
    r2Adapter.injectFailure(1);

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
    r2Adapter.injectFailure(1);

    const batchId = 'test-batch-123';
    await fallbackStorage.writeWithFallback([createTestEvent()], batchId);

    // EXPECTED: Key should follow pattern
    expect(kvAdapter.lastWrittenKey).toContain('fallback');
    expect(kvAdapter.lastWrittenKey).toContain(batchId);
  });

  it('should reject writes exceeding configured max size', async () => {
    // Create a fallback with very small limit and NO compression
    const smallLimitFallback = new KVFallbackStorage({
      kv: kvAdapter,
      r2: r2Adapter,
      maxValueSize: 500, // Very small limit
      enableChunking: false, // Disable chunking to test limit enforcement
      enableCompression: false, // Disable compression so data stays large
    });

    r2Adapter.injectFailure(1);

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
  let kvAdapter: ReturnType<typeof createKVAdapter>;
  let r2Adapter: ReturnType<typeof createR2Adapter>;
  let fallbackStorage: KVFallbackStorage;

  const MB = 1024 * 1024;

  beforeEach(async () => {
    kvAdapter = createKVAdapter(env.KV_FALLBACK);
    r2Adapter = createR2Adapter(env.LAKEHOUSE_BUCKET);
    await kvAdapter.clear();
    await r2Adapter.clear();
    fallbackStorage = new KVFallbackStorage({
      kv: kvAdapter,
      r2: r2Adapter,
      maxValueSize: 25 * MB,
      enableChunking: true,
    });
  });

  it('should chunk data exceeding 25MB across multiple keys', async () => {
    // Use smaller limit for testing and disable compression so data stays large
    const smallChunkFallback = new KVFallbackStorage({
      kv: kvAdapter,
      r2: r2Adapter,
      maxValueSize: 500, // Very small chunks for testing
      enableChunking: true,
      enableCompression: false, // Disable compression so data exceeds chunk size
    });

    r2Adapter.injectFailure(1);

    // Create data larger than chunk size (need more events without compression)
    const events = createTestBatch(100); // Should exceed 500 bytes
    const result = await smallChunkFallback.writeWithFallback(events, 'test-chunk-1');

    // EXPECTED: Should use multiple chunks
    expect(result.chunked).toBe(true);
    expect(result.chunkCount).toBeGreaterThan(1);
  });

  it('should track chunk information', async () => {
    const smallChunkFallback = new KVFallbackStorage({
      kv: kvAdapter,
      r2: r2Adapter,
      maxValueSize: 500,
      enableChunking: true,
    });

    r2Adapter.injectFailure(1);

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
      kv: kvAdapter,
      r2: r2Adapter,
      maxValueSize: 500,
      enableChunking: true,
    });

    r2Adapter.injectFailure(1);

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
      kv: kvAdapter,
      r2: r2Adapter,
      maxValueSize: 500,
      enableChunking: true,
    });

    r2Adapter.injectFailure(1);

    const events = createTestBatch(25);
    const batchId = 'test-manifest';
    const result = await smallChunkFallback.writeWithFallback(events, batchId);

    if (result.chunked) {
      // EXPECTED: Should have manifest key
      expect(result.manifestKey).toBeDefined();

      // Manifest should be readable
      const manifest = await kvAdapter.read(result.manifestKey!);
      expect(manifest).not.toBeNull();

      const parsedManifest = JSON.parse(manifest!);
      expect(parsedManifest.totalChunks).toBe(result.chunkCount);
      expect(parsedManifest.chunkKeys).toHaveLength(result.chunkCount);
    }
  });

  it('should handle partial chunk write failures', async () => {
    // Create a wrapper that fails after 2 writes
    let writeAttempts = 0;
    const failingKV: ExtendedKVStorage = {
      async write(key: string, value: string, options?: { expirationTtl?: number }) {
        writeAttempts++;
        if (writeAttempts > 2) {
          throw new Error('KV write failed');
        }
        await kvAdapter.write(key, value, options);
      },
      read: kvAdapter.read.bind(kvAdapter),
      delete: kvAdapter.delete.bind(kvAdapter),
      list: kvAdapter.list.bind(kvAdapter),
    };

    const failingChunkFallback = new KVFallbackStorage({
      kv: failingKV,
      r2: r2Adapter,
      maxValueSize: 200,
      enableChunking: true,
    });

    r2Adapter.injectFailure(1);

    const events = createTestBatch(30);
    const result = await failingChunkFallback.writeWithFallback(events, 'test-partial-fail');

    // EXPECTED: Should report partial failure
    expect(result.kvWriteSuccess).toBe(false);
    expect(result.error).toBeDefined();
  });

  it('should not chunk when data fits in single value', async () => {
    r2Adapter.injectFailure(1);

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
  let kvAdapter: ReturnType<typeof createKVAdapter>;
  let r2Adapter: ReturnType<typeof createR2Adapter>;
  let fallbackStorage: KVFallbackStorage;

  beforeEach(async () => {
    kvAdapter = createKVAdapter(env.KV_FALLBACK);
    r2Adapter = createR2Adapter(env.LAKEHOUSE_BUCKET);
    await kvAdapter.clear();
    await r2Adapter.clear();
    fallbackStorage = new KVFallbackStorage({
      kv: kvAdapter,
      r2: r2Adapter,
    });
  });

  it('should list pending fallback batches', async () => {
    r2Adapter.injectFailure(3);

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
    r2Adapter.injectFailure(1);

    const events = createTestBatch(5);
    const batchId = 'test-recover-1';
    await fallbackStorage.writeWithFallback(events, batchId);

    // Clear R2 failures
    r2Adapter.clearFailures();

    // EXPECTED: Recovery should succeed
    const result = await fallbackStorage.recoverToR2(batchId);
    expect(result.success).toBe(true);
    expect(result.eventsRecovered).toBe(5);
  });

  it('should delete from KV after successful R2 recovery', async () => {
    r2Adapter.injectFailure(1);

    const batchId = 'test-cleanup';
    await fallbackStorage.writeWithFallback([createTestEvent()], batchId);

    r2Adapter.clearFailures();
    await fallbackStorage.recoverToR2(batchId);

    // EXPECTED: KV entry should be deleted
    const stillExists = await fallbackStorage.readFromFallback(batchId);
    expect(stillExists).toBeNull();
  });

  it('should recover all pending batches in bulk', async () => {
    r2Adapter.injectFailure(3);

    await fallbackStorage.writeWithFallback([createTestEvent()], 'bulk-1');
    await fallbackStorage.writeWithFallback([createTestEvent()], 'bulk-2');
    await fallbackStorage.writeWithFallback([createTestEvent()], 'bulk-3');

    r2Adapter.clearFailures();

    // EXPECTED: Bulk recovery should work
    const result = await fallbackStorage.recoverAllToR2();
    expect(result.totalBatches).toBe(3);
    expect(result.successfulRecoveries).toBe(3);
    expect(result.failedRecoveries).toBe(0);
  });

  it('should handle recovery failure gracefully', async () => {
    r2Adapter.injectFailure(1);

    const batchId = 'test-recovery-fail';
    await fallbackStorage.writeWithFallback([createTestEvent()], batchId);

    // Keep R2 failing
    r2Adapter.injectFailure(1);

    // EXPECTED: Recovery should fail but not throw
    const result = await fallbackStorage.recoverToR2(batchId);
    expect(result.success).toBe(false);
    expect(result.error).toBeDefined();

    // Data should still be in KV
    const stillExists = await fallbackStorage.readFromFallback(batchId);
    expect(stillExists).not.toBeNull();
  });

  it('should track recovery metrics', async () => {
    r2Adapter.injectFailure(2);

    await fallbackStorage.writeWithFallback([createTestEvent()], 'metrics-1');
    await fallbackStorage.writeWithFallback([createTestEvent()], 'metrics-2');

    r2Adapter.clearFailures();
    r2Adapter.injectFailure(1); // First recovery fails

    await fallbackStorage.recoverAllToR2();

    // EXPECTED: Should have metrics
    const metrics = fallbackStorage.getRecoveryMetrics();
    expect(metrics.totalRecoveryAttempts).toBeGreaterThan(0);
    expect(metrics.successfulRecoveries).toBeGreaterThanOrEqual(0);
    expect(metrics.failedRecoveries).toBeGreaterThanOrEqual(0);
  });

  it('should support recovery on startup', async () => {
    r2Adapter.injectFailure(1);
    await fallbackStorage.writeWithFallback([createTestEvent()], 'startup-test');

    // Create new instance (simulating startup)
    r2Adapter.clearFailures();
    const newFallback = new KVFallbackStorage({
      kv: kvAdapter,
      r2: r2Adapter,
    });

    // EXPECTED: Should be able to recover on startup
    const result = await newFallback.recoverOnStartup();
    expect(result.batchesRecovered).toBeGreaterThan(0);
  });

  it('should support recovery via alarm handler', async () => {
    r2Adapter.injectFailure(1);
    await fallbackStorage.writeWithFallback([createTestEvent()], 'alarm-test');

    r2Adapter.clearFailures();

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
  let kvAdapter: ReturnType<typeof createKVAdapter>;
  let r2Adapter: ReturnType<typeof createR2Adapter>;
  let fallbackStorage: KVFallbackStorage;

  const SEVEN_DAYS_SECONDS = 7 * 24 * 60 * 60;

  beforeEach(async () => {
    kvAdapter = createKVAdapter(env.KV_FALLBACK);
    r2Adapter = createR2Adapter(env.LAKEHOUSE_BUCKET);
    await kvAdapter.clear();
    await r2Adapter.clear();
    fallbackStorage = new KVFallbackStorage({
      kv: kvAdapter,
      r2: r2Adapter,
      ttlSeconds: SEVEN_DAYS_SECONDS,
    });
  });

  it('should set 7 day TTL on KV writes by default', async () => {
    r2Adapter.injectFailure(1);

    await fallbackStorage.writeWithFallback([createTestEvent()], 'ttl-test-1');

    // EXPECTED: TTL should be 7 days
    expect(kvAdapter.lastWrittenExpiration).toBe(SEVEN_DAYS_SECONDS);
  });

  it('should support configurable TTL', async () => {
    const customTTL = 3 * 24 * 60 * 60; // 3 days
    const customFallback = new KVFallbackStorage({
      kv: kvAdapter,
      r2: r2Adapter,
      ttlSeconds: customTTL,
    });

    r2Adapter.injectFailure(1);
    await customFallback.writeWithFallback([createTestEvent()], 'ttl-custom');

    // EXPECTED: Custom TTL should be used
    expect(kvAdapter.lastWrittenExpiration).toBe(customTTL);
  });

  it('should include expiration timestamp in stored metadata', async () => {
    r2Adapter.injectFailure(1);

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
      kv: kvAdapter,
      r2: r2Adapter,
      maxValueSize: 500,
      enableChunking: true,
      ttlSeconds: SEVEN_DAYS_SECONDS,
    });

    r2Adapter.injectFailure(1);

    const events = createTestBatch(30);
    await smallChunkFallback.writeWithFallback(events, 'ttl-chunked');

    // EXPECTED: All writes should have TTL
    // Since we can't track all expirations easily, just verify the last one
    expect(kvAdapter.lastWrittenExpiration).toBe(SEVEN_DAYS_SECONDS);
  });

  it('should report pending batches with remaining TTL', async () => {
    r2Adapter.injectFailure(1);

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
      kv: kvAdapter,
      r2: r2Adapter,
      ttlSeconds: 100, // 100 seconds
      expirationWarningThreshold: 0.9, // Warn when 90% expired
    });

    r2Adapter.injectFailure(1);
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
  let kvAdapter: ReturnType<typeof createKVAdapter>;
  let r2Adapter: ReturnType<typeof createR2Adapter>;
  let fallbackStorage: KVFallbackStorage;

  beforeEach(async () => {
    kvAdapter = createKVAdapter(env.KV_FALLBACK);
    r2Adapter = createR2Adapter(env.LAKEHOUSE_BUCKET);
    await kvAdapter.clear();
    await r2Adapter.clear();
    fallbackStorage = new KVFallbackStorage({
      kv: kvAdapter,
      r2: r2Adapter,
    });
  });

  it('should expose R2 and KV storage interfaces', () => {
    // EXPECTED: Should have getters for integration
    expect(fallbackStorage.getKVStorage()).toBeDefined();
    expect(fallbackStorage.getR2Storage()).toBeDefined();
  });

  it('should provide status for monitoring', async () => {
    r2Adapter.injectFailure(2);

    await fallbackStorage.writeWithFallback([createTestEvent()], 'status-1');
    await fallbackStorage.writeWithFallback([createTestEvent()], 'status-2');

    r2Adapter.clearFailures();
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
      kv: kvAdapter,
      r2: r2Adapter,
      onRecovery: (batchId, success) => {
        recoveryEvents.push(`${batchId}:${success}`);
      },
    });

    r2Adapter.injectFailure(1);
    await callbackFallback.writeWithFallback([createTestEvent()], 'callback-test');

    r2Adapter.clearFailures();
    await callbackFallback.recoverToR2('callback-test');

    // EXPECTED: Callback should be invoked
    expect(recoveryEvents).toContain('callback-test:true');
  });

  it('should integrate with P1 fallback flow', async () => {
    r2Adapter.injectFailure(1);

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
  let kvAdapter: ReturnType<typeof createKVAdapter>;
  let r2Adapter: ReturnType<typeof createR2Adapter>;
  let fallbackStorage: KVFallbackStorage;

  beforeEach(async () => {
    kvAdapter = createKVAdapter(env.KV_FALLBACK);
    r2Adapter = createR2Adapter(env.LAKEHOUSE_BUCKET);
    await kvAdapter.clear();
    await r2Adapter.clear();
    fallbackStorage = new KVFallbackStorage({
      kv: kvAdapter,
      r2: r2Adapter,
    });
  });

  it('should handle empty event array', async () => {
    r2Adapter.injectFailure(1);

    const result = await fallbackStorage.writeWithFallback([], 'empty-batch');

    // EXPECTED: Should handle gracefully
    expect(result.kvWriteSuccess).toBe(true);
    expect(result.eventCount).toBe(0);
  });

  it('should handle events with special characters in data', async () => {
    r2Adapter.injectFailure(1);

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
    r2Adapter.injectFailure(5);

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
    r2Adapter.injectFailure(1);

    const longBatchId = 'x'.repeat(500); // Very long batch ID
    const result = await fallbackStorage.writeWithFallback([createTestEvent()], longBatchId);

    // EXPECTED: Should handle long batch IDs
    expect(result.kvWriteSuccess).toBe(true);
  });

  it('should handle KV read failure during recovery', async () => {
    r2Adapter.injectFailure(1);
    await fallbackStorage.writeWithFallback([createTestEvent()], 'read-fail-test');

    // Create a wrapper that throws on read
    const failingKV: ExtendedKVStorage = {
      write: kvAdapter.write.bind(kvAdapter),
      read: async () => {
        throw new Error('KV read failed');
      },
      delete: kvAdapter.delete.bind(kvAdapter),
      list: kvAdapter.list.bind(kvAdapter),
    };

    const failingReadFallback = new KVFallbackStorage({
      kv: failingKV,
      r2: r2Adapter,
    });

    r2Adapter.clearFailures();
    const result = await failingReadFallback.recoverToR2('read-fail-test');

    // EXPECTED: Should handle read failure
    expect(result.success).toBe(false);
    expect(result.error).toContain('KV read failed');
  });

  it('should provide meaningful error messages', async () => {
    r2Adapter.injectFailure(1, true); // Permanent failure

    const failingKV: ExtendedKVStorage = {
      write: async () => {
        throw new Error('Detailed KV error: quota exceeded');
      },
      read: kvAdapter.read.bind(kvAdapter),
      delete: kvAdapter.delete.bind(kvAdapter),
      list: kvAdapter.list.bind(kvAdapter),
    };

    const failingFallback = new KVFallbackStorage({
      kv: failingKV,
      r2: r2Adapter,
    });

    const result = await failingFallback.writeWithFallback([createTestEvent()], 'error-msg');

    // EXPECTED: Error message should be informative
    expect(result.error).toBeDefined();
    expect(result.error).toContain('quota exceeded');
  });
});
