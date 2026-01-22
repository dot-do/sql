/**
 * R2 Cache Tests
 *
 * Tests for the R2 caching layer implementation.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  R2Cache,
  CachingBackend,
  withCache,
  DEFAULT_CACHE_CONFIG,
} from '../r2-cache.js';
import { MemoryFSXBackend } from '../index.js';

// =============================================================================
// R2Cache Unit Tests
// =============================================================================

describe('R2Cache', () => {
  let cache: R2Cache;

  beforeEach(() => {
    cache = new R2Cache({
      maxSize: 1000,
      maxEntries: 10,
      ttlMs: 100,
      maxEntrySize: 500,
    });
  });

  describe('Basic Operations', () => {
    it('should store and retrieve data', () => {
      const data = new Uint8Array([1, 2, 3, 4, 5]);
      cache.set('test-key', data);

      const result = cache.get('test-key');
      expect(result).not.toBeNull();
      expect(result?.data).toEqual(data);
    });

    it('should return null for non-existent keys', () => {
      const result = cache.get('nonexistent');
      expect(result).toBeNull();
    });

    it('should remove entries', () => {
      const data = new Uint8Array([1, 2, 3]);
      cache.set('key', data);
      expect(cache.has('key')).toBe(true);

      cache.remove('key');
      expect(cache.has('key')).toBe(false);
    });

    it('should clear all entries', () => {
      cache.set('key1', new Uint8Array([1]));
      cache.set('key2', new Uint8Array([2]));

      cache.clear();

      expect(cache.has('key1')).toBe(false);
      expect(cache.has('key2')).toBe(false);
    });
  });

  describe('TTL Expiration', () => {
    it('should expire entries after TTL', async () => {
      const data = new Uint8Array([1, 2, 3]);
      cache.set('expires', data);

      // Immediately should be available
      expect(cache.get('expires')).not.toBeNull();

      // Wait for TTL to expire
      await new Promise((resolve) => setTimeout(resolve, 150));

      // Should be expired
      expect(cache.get('expires')).toBeNull();
    });
  });

  describe('Size Limits', () => {
    it('should not cache entries exceeding maxEntrySize', () => {
      const largeData = new Uint8Array(600); // Exceeds 500 maxEntrySize
      cache.set('large', largeData);

      // Should not be cached
      expect(cache.has('large')).toBe(false);
    });

    it('should evict entries when maxSize exceeded', () => {
      // Fill cache with entries
      for (let i = 0; i < 5; i++) {
        cache.set(`key-${i}`, new Uint8Array(250)); // 250 * 5 = 1250 > 1000
      }

      // Some entries should have been evicted
      const stats = cache.getStats();
      expect(stats.totalSize).toBeLessThanOrEqual(1000);
    });

    it('should evict entries when maxEntries exceeded', () => {
      // Fill cache beyond maxEntries
      for (let i = 0; i < 15; i++) {
        cache.set(`key-${i}`, new Uint8Array(10));
      }

      const stats = cache.getStats();
      expect(stats.entryCount).toBeLessThanOrEqual(10);
    });
  });

  describe('Prefix Invalidation', () => {
    it('should invalidate entries by prefix', () => {
      cache.set('user/1', new Uint8Array([1]));
      cache.set('user/2', new Uint8Array([2]));
      cache.set('order/1', new Uint8Array([3]));

      const count = cache.invalidatePrefix('user/');

      expect(count).toBe(2);
      expect(cache.has('user/1')).toBe(false);
      expect(cache.has('user/2')).toBe(false);
      expect(cache.has('order/1')).toBe(true);
    });
  });

  describe('Statistics', () => {
    it('should track hits and misses', () => {
      cache.set('exists', new Uint8Array([1]));

      // Hit
      cache.get('exists');
      cache.get('exists');

      // Miss
      cache.get('nonexistent');

      const stats = cache.getStats();
      expect(stats.hits).toBe(2);
      expect(stats.misses).toBe(1);
      expect(stats.hitRatio).toBeCloseTo(2 / 3);
    });

    it('should track evictions', () => {
      // Fill and overflow cache
      for (let i = 0; i < 15; i++) {
        cache.set(`key-${i}`, new Uint8Array(10));
      }

      const stats = cache.getStats();
      expect(stats.evictions).toBeGreaterThan(0);
    });

    it('should reset statistics', () => {
      cache.set('key', new Uint8Array([1]));
      cache.get('key');
      cache.get('nonexistent');

      cache.resetStats();

      const stats = cache.getStats();
      expect(stats.hits).toBe(0);
      expect(stats.misses).toBe(0);
    });
  });
});

// =============================================================================
// CachingBackend Integration Tests
// =============================================================================

describe('CachingBackend', () => {
  let backend: MemoryFSXBackend;
  let cachingBackend: CachingBackend;

  beforeEach(() => {
    backend = new MemoryFSXBackend();
    cachingBackend = withCache(backend, {
      maxSize: 1024 * 1024, // 1MB
      ttlMs: 60000, // 1 minute
      validateEtags: false, // Disable for simplicity in tests
    });
  });

  describe('Read Caching', () => {
    it('should cache reads', async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5]);
      await backend.write('test.bin', data);

      // First read - cache miss
      const result1 = await cachingBackend.read('test.bin');
      expect(result1).toEqual(data);

      // Second read - should be cached
      const result2 = await cachingBackend.read('test.bin');
      expect(result2).toEqual(data);

      const stats = cachingBackend.getCacheStats();
      expect(stats.hits).toBe(1);
    });

    it('should not cache range reads', async () => {
      const data = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
      await backend.write('range.bin', data);

      // Range read should not be cached
      await cachingBackend.read('range.bin', [2, 5]);
      await cachingBackend.read('range.bin', [2, 5]);

      const stats = cachingBackend.getCacheStats();
      expect(stats.hits).toBe(0);
    });
  });

  describe('Write Invalidation', () => {
    it('should invalidate cache on write', async () => {
      const data1 = new Uint8Array([1, 2, 3]);
      const data2 = new Uint8Array([4, 5, 6]);

      await cachingBackend.write('file.bin', data1);
      await cachingBackend.read('file.bin'); // Populate cache

      await cachingBackend.write('file.bin', data2);
      const result = await cachingBackend.read('file.bin');

      expect(result).toEqual(data2);
    });
  });

  describe('Delete Invalidation', () => {
    it('should invalidate cache on delete', async () => {
      const data = new Uint8Array([1, 2, 3]);
      await cachingBackend.write('delete.bin', data);
      await cachingBackend.read('delete.bin'); // Populate cache

      await cachingBackend.delete('delete.bin');

      // Should return null after delete
      const result = await cachingBackend.read('delete.bin');
      expect(result).toBeNull();
    });
  });

  describe('Existence Check', () => {
    it('should use cache for existence check', async () => {
      const data = new Uint8Array([1, 2, 3]);
      await cachingBackend.write('exists.bin', data);
      await cachingBackend.read('exists.bin'); // Populate cache

      // Should use cache
      const exists = await cachingBackend.exists('exists.bin');
      expect(exists).toBe(true);
    });
  });

  describe('Clear Cache', () => {
    it('should clear the cache', async () => {
      await cachingBackend.write('file1.bin', new Uint8Array([1]));
      await cachingBackend.write('file2.bin', new Uint8Array([2]));
      await cachingBackend.read('file1.bin');
      await cachingBackend.read('file2.bin');

      cachingBackend.clearCache();

      const stats = cachingBackend.getCacheStats();
      expect(stats.entryCount).toBe(0);
    });
  });

  describe('Prefix Invalidation', () => {
    it('should invalidate by prefix', async () => {
      await cachingBackend.write('users/1.bin', new Uint8Array([1]));
      await cachingBackend.write('users/2.bin', new Uint8Array([2]));
      await cachingBackend.write('orders/1.bin', new Uint8Array([3]));

      // Populate cache
      await cachingBackend.read('users/1.bin');
      await cachingBackend.read('users/2.bin');
      await cachingBackend.read('orders/1.bin');

      const count = cachingBackend.invalidateCachePrefix('users/');

      expect(count).toBe(2);
    });
  });
});
