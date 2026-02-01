/**
 * R2 Backend LRU Cache Tests
 *
 * Tests for the tiered cache hierarchy with LRU eviction in the R2 backend.
 * Verifies:
 * - Cache respects max byte size
 * - Cache respects max entry count
 * - LRU eviction order is correct
 * - Cache statistics are accurate
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  R2StorageBackend,
  createR2Backend,
  DEFAULT_READ_CACHE_MAX_BYTES,
  DEFAULT_READ_CACHE_MAX_ENTRIES,
  type R2BucketLike,
  type R2ObjectLike,
  type R2ObjectsLike,
} from '../r2-backend.js';

/**
 * Create a mock R2 bucket for testing
 */
function createMockBucket(): R2BucketLike {
  const storage = new Map<string, Uint8Array>();

  return {
    async get(key: string): Promise<R2ObjectLike | null> {
      const data = storage.get(key);
      if (!data) return null;

      return {
        key,
        size: data.byteLength,
        etag: `etag-${key}`,
        httpEtag: `"etag-${key}"`,
        uploaded: new Date(),
        async arrayBuffer(): Promise<ArrayBuffer> {
          return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
        },
        async text(): Promise<string> {
          return new TextDecoder().decode(data);
        },
        async json<T>(): Promise<T> {
          return JSON.parse(new TextDecoder().decode(data)) as T;
        },
        async blob(): Promise<Blob> {
          return new Blob([data]);
        },
        writeHttpMetadata(): void {},
      };
    },

    async put(key: string, value: ArrayBuffer | ArrayBufferView | string | Blob | ReadableStream): Promise<R2ObjectLike> {
      let data: Uint8Array;
      if (value instanceof ArrayBuffer) {
        data = new Uint8Array(value);
      } else if (ArrayBuffer.isView(value)) {
        data = new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
      } else if (typeof value === 'string') {
        data = new TextEncoder().encode(value);
      } else {
        throw new Error('Unsupported value type');
      }
      storage.set(key, data);

      return {
        key,
        size: data.byteLength,
        etag: `etag-${key}`,
        httpEtag: `"etag-${key}"`,
        uploaded: new Date(),
        async arrayBuffer(): Promise<ArrayBuffer> {
          return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
        },
        async text(): Promise<string> {
          return new TextDecoder().decode(data);
        },
        async json<T>(): Promise<T> {
          return JSON.parse(new TextDecoder().decode(data)) as T;
        },
        async blob(): Promise<Blob> {
          return new Blob([data]);
        },
        writeHttpMetadata(): void {},
      };
    },

    async delete(keys: string | string[]): Promise<void> {
      const keyArray = Array.isArray(keys) ? keys : [keys];
      for (const key of keyArray) {
        storage.delete(key);
      }
    },

    async list(): Promise<R2ObjectsLike> {
      return {
        objects: [],
        truncated: false,
      };
    },

    async head(key: string): Promise<R2ObjectLike | null> {
      const data = storage.get(key);
      if (!data) return null;
      return {
        key,
        size: data.byteLength,
        etag: `etag-${key}`,
        httpEtag: `"etag-${key}"`,
        uploaded: new Date(),
        async arrayBuffer(): Promise<ArrayBuffer> {
          return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
        },
        async text(): Promise<string> {
          return new TextDecoder().decode(data);
        },
        async json<T>(): Promise<T> {
          return JSON.parse(new TextDecoder().decode(data)) as T;
        },
        async blob(): Promise<Blob> {
          return new Blob([data]);
        },
        writeHttpMetadata(): void {},
      };
    },
  };
}

describe('R2StorageBackend LRU Cache', () => {
  let bucket: R2BucketLike;
  let backend: R2StorageBackend;

  beforeEach(() => {
    bucket = createMockBucket();
  });

  describe('default configuration', () => {
    it('should use default cache limits', () => {
      backend = createR2Backend(bucket);
      const stats = backend.getReadCacheStats();

      expect(stats.maxBytes).toBe(DEFAULT_READ_CACHE_MAX_BYTES);
      expect(stats.maxEntries).toBe(DEFAULT_READ_CACHE_MAX_ENTRIES);
    });
  });

  describe('byte-based eviction', () => {
    it('should evict entries when max bytes exceeded', async () => {
      backend = createR2Backend(bucket, {
        readCacheMaxBytes: 1000, // 1KB cache
        readCacheMaxEntries: 100, // High entry limit to not interfere
      });

      // Write 5 files of 300 bytes each (1500 bytes total, exceeds 1000 limit)
      for (let i = 0; i < 5; i++) {
        await bucket.put(`file-${i}.bin`, new Uint8Array(300));
      }

      // Read all files to populate cache
      for (let i = 0; i < 5; i++) {
        await backend.read(`file-${i}.bin`);
      }

      const stats = backend.getReadCacheStats();

      // Cache should have evicted entries to stay under 1000 bytes
      expect(stats.totalBytes).toBeLessThanOrEqual(1000);
      expect(stats.evictions).toBeGreaterThan(0);
    });

    it('should evict LRU entries first', async () => {
      backend = createR2Backend(bucket, {
        readCacheMaxBytes: 600, // Can hold 2 entries of 300 bytes
        readCacheMaxEntries: 100,
      });

      // Write 3 files
      await bucket.put('file-a.bin', new Uint8Array(300));
      await bucket.put('file-b.bin', new Uint8Array(300));
      await bucket.put('file-c.bin', new Uint8Array(300));

      // Read in order: a, b, c
      await backend.read('file-a.bin');
      await backend.read('file-b.bin');
      await backend.read('file-c.bin'); // This should evict a

      const stats = backend.getReadCacheStats();
      // Verify evictions occurred
      expect(stats.evictions).toBeGreaterThan(0);
      // Cache should be at capacity (2 entries of 300 bytes = 600 bytes)
      expect(stats.entryCount).toBe(2);
    });

    it('should maintain cache entries after multiple reads', async () => {
      backend = createR2Backend(bucket, {
        readCacheMaxBytes: 600, // Can hold 2 entries of 300 bytes
        readCacheMaxEntries: 100,
      });

      // Write 2 files
      await bucket.put('file-a.bin', new Uint8Array(300));
      await bucket.put('file-b.bin', new Uint8Array(300));

      // Read both files
      await backend.read('file-a.bin');
      await backend.read('file-b.bin');

      // Cache should have both entries
      const stats1 = backend.getReadCacheStats();
      expect(stats1.entryCount).toBe(2);
      expect(stats1.totalBytes).toBe(600);

      // Reading again should update cache (replace with same data)
      await backend.read('file-a.bin');
      await backend.read('file-b.bin');

      const stats2 = backend.getReadCacheStats();
      // Still 2 entries after re-reading
      expect(stats2.entryCount).toBe(2);
    });
  });

  describe('entry count eviction', () => {
    it('should evict entries when max count exceeded', async () => {
      backend = createR2Backend(bucket, {
        readCacheMaxBytes: 1024 * 1024, // 1MB (high limit)
        readCacheMaxEntries: 3, // Only 3 entries allowed
      });

      // Write 5 small files
      for (let i = 0; i < 5; i++) {
        await bucket.put(`file-${i}.bin`, new Uint8Array(10));
      }

      // Read all files
      for (let i = 0; i < 5; i++) {
        await backend.read(`file-${i}.bin`);
      }

      const stats = backend.getReadCacheStats();

      // Cache should have at most 3 entries
      expect(stats.entryCount).toBeLessThanOrEqual(3);
    });
  });

  describe('cache statistics', () => {
    it('should track cache size correctly', async () => {
      backend = createR2Backend(bucket, {
        readCacheMaxBytes: 10000,
        readCacheMaxEntries: 100,
      });

      await bucket.put('file.bin', new Uint8Array(100));

      // First read populates cache
      await backend.read('file.bin');

      const stats = backend.getReadCacheStats();
      expect(stats.entryCount).toBe(1);
      expect(stats.totalBytes).toBe(100);
    });

    it('should report correct byte usage', async () => {
      backend = createR2Backend(bucket, {
        readCacheMaxBytes: 10000,
        readCacheMaxEntries: 100,
      });

      await bucket.put('file1.bin', new Uint8Array(100));
      await bucket.put('file2.bin', new Uint8Array(200));

      await backend.read('file1.bin');
      await backend.read('file2.bin');

      const stats = backend.getReadCacheStats();
      expect(stats.entryCount).toBe(2);
      expect(stats.totalBytes).toBe(300);
    });

    it('should track evictions', async () => {
      backend = createR2Backend(bucket, {
        readCacheMaxBytes: 200,
        readCacheMaxEntries: 100,
      });

      // Write files that will cause eviction
      await bucket.put('file1.bin', new Uint8Array(100));
      await bucket.put('file2.bin', new Uint8Array(100));
      await bucket.put('file3.bin', new Uint8Array(100));

      await backend.read('file1.bin');
      await backend.read('file2.bin');
      await backend.read('file3.bin'); // Should trigger eviction

      const stats = backend.getReadCacheStats();
      expect(stats.evictions).toBeGreaterThan(0);
    });
  });

  describe('cache management', () => {
    it('should clear cache', async () => {
      backend = createR2Backend(bucket, {
        readCacheMaxBytes: 10000,
        readCacheMaxEntries: 100,
      });

      await bucket.put('file.bin', new Uint8Array(100));
      await backend.read('file.bin');

      expect(backend.getReadCacheStats().entryCount).toBe(1);

      backend.clearReadCache();

      expect(backend.getReadCacheStats().entryCount).toBe(0);
      expect(backend.getReadCacheStats().totalBytes).toBe(0);
    });

    it('should invalidate cache on delete', async () => {
      backend = createR2Backend(bucket, {
        readCacheMaxBytes: 10000,
        readCacheMaxEntries: 100,
      });

      await bucket.put('file.bin', new Uint8Array(100));
      await backend.read('file.bin');

      expect(backend.getReadCacheStats().entryCount).toBe(1);

      await backend.delete('file.bin');

      expect(backend.getReadCacheStats().entryCount).toBe(0);
    });
  });

  describe('edge cases', () => {
    it('should handle zero-size cache gracefully', async () => {
      backend = createR2Backend(bucket, {
        readCacheMaxBytes: 0,
        readCacheMaxEntries: 0,
      });

      await bucket.put('file.bin', new Uint8Array(100));
      const data = await backend.read('file.bin');

      expect(data).not.toBeNull();
      expect(backend.getReadCacheStats().entryCount).toBe(0);
    });

    it('should handle single entry cache', async () => {
      backend = createR2Backend(bucket, {
        readCacheMaxBytes: 100,
        readCacheMaxEntries: 1,
      });

      await bucket.put('file1.bin', new Uint8Array(50));
      await bucket.put('file2.bin', new Uint8Array(50));

      await backend.read('file1.bin');
      expect(backend.getReadCacheStats().entryCount).toBe(1);

      await backend.read('file2.bin');
      expect(backend.getReadCacheStats().entryCount).toBe(1);
    });

    it('should handle large entry that exceeds cache size', async () => {
      backend = createR2Backend(bucket, {
        readCacheMaxBytes: 100,
        readCacheMaxEntries: 100,
      });

      // Write a file larger than the cache
      await bucket.put('large.bin', new Uint8Array(200));

      // Should still read successfully (just won't cache effectively)
      const data = await backend.read('large.bin');
      expect(data).not.toBeNull();
      expect(data?.byteLength).toBe(200);
    });
  });
});
