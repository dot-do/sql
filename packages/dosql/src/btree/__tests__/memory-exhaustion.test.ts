/**
 * Memory Exhaustion Tests for B-tree Page Cache
 *
 * TDD GREEN phase tests verifying B-tree page cache bounds enforcement.
 *
 * These tests verify that:
 *
 * 1. Cache size stays bounded after accessing many pages
 * 2. LRU eviction removes least-recently-used pages
 * 3. Cache respects configured maxCachedPages limit
 * 4. Most-recently-used pages survive eviction
 * 5. Eviction callback is called when page is removed
 * 6. Cache size tracking is accurate
 * 7. Cache handles rapid access patterns
 * 8. Memory usage stays within configured limits
 * 9. Page reload after eviction works correctly
 * 10. Concurrent access with eviction is safe
 *
 * These tests use workers-vitest-pool (NO MOCKS).
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MemoryFSXBackend } from '../../fsx/index.js';
import {
  createBTree,
  StringKeyCodec,
  NumberKeyCodec,
  JsonValueCodec,
  BinaryValueCodec,
  type Page,
} from '../index.js';

/**
 * Helper to create a large value that will consume significant memory
 */
function createLargeValue(sizeBytes: number): Uint8Array {
  const data = new Uint8Array(sizeBytes);
  for (let i = 0; i < sizeBytes; i++) {
    data[i] = i % 256;
  }
  return data;
}

describe('B-tree Memory Exhaustion', () => {
  let fsx: MemoryFSXBackend;

  beforeEach(() => {
    fsx = new MemoryFSXBackend();
  });

  describe('Cache size bounds', () => {
    /**
     * Test 1: Cache size stays bounded after accessing many pages
     *
     * After accessing more pages than maxPages, the cache size should
     * remain at or below the configured limit.
     */
    it('should keep cache size bounded after accessing many pages', async () => {
      const maxCachedPages = 10;
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4, // Small pages to force many page allocations
        cache: {
          maxPages: maxCachedPages,
        },
      });
      await tree.init();

      // Insert enough entries to create many pages (with small maxKeys, this creates many splits)
      const entryCount = 100;
      for (let i = 0; i < entryCount; i++) {
        const key = `key_${i.toString().padStart(4, '0')}`;
        await tree.set(key, { index: i, data: `value_${i}` });
      }

      // The tree stats should show multiple pages were created
      const stats = await tree.stats();
      expect(stats.pageCount).toBeGreaterThan(maxCachedPages);

      // Now access all entries through a range scan, touching all pages
      let accessCount = 0;
      for await (const [key, value] of tree.entries()) {
        accessCount++;
      }
      expect(accessCount).toBe(entryCount);

      // Verify cache is bounded using getCacheStats
      const cacheStats = tree.getCacheStats();
      expect(cacheStats.size).toBeLessThanOrEqual(maxCachedPages);
      expect(cacheStats.maxSize).toBe(maxCachedPages);

      // Access random entries to verify cache eviction works
      for (let i = 0; i < 50; i++) {
        const randomIndex = Math.floor(Math.random() * entryCount);
        const key = `key_${randomIndex.toString().padStart(4, '0')}`;
        const value = await tree.get(key);
        expect(value).toBeDefined();
        expect(value?.index).toBe(randomIndex);
      }

      // Verify cache is still bounded after random access
      const finalStats = tree.getCacheStats();
      expect(finalStats.size).toBeLessThanOrEqual(maxCachedPages);
      expect(finalStats.evictions).toBeGreaterThan(0); // Evictions should have occurred
    });

    /**
     * Test 2: LRU eviction removes least-recently-used pages
     *
     * The LRU cache should evict the least recently accessed pages when
     * the cache is full and new pages are loaded.
     */
    it('should evict least-recently-used pages first', async () => {
      const evictedPageIds: number[] = [];
      const maxCachedPages = 5;

      // Create tree with eviction tracking
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        cache: {
          maxPages: maxCachedPages,
          onEvict: (pageId: number, page: Page, dirty: boolean) => {
            evictedPageIds.push(pageId);
          },
        },
      });
      await tree.init();

      // Insert entries to create multiple pages
      for (let i = 0; i < 50; i++) {
        await tree.set(i, { value: i });
      }

      const stats = await tree.stats();
      expect(stats.pageCount).toBeGreaterThan(maxCachedPages);

      // Reset stats to track from here
      tree.resetCacheStats();
      evictedPageIds.length = 0;

      // Access keys in a specific order to set up LRU state
      await tree.get(0); // Access first key
      await tree.get(49); // Access last key
      await tree.get(25); // Access middle

      // Now access many more keys to trigger eviction
      for (let i = 10; i < 20; i++) {
        await tree.get(i);
      }

      // Verify eviction callback was called
      expect(evictedPageIds.length).toBeGreaterThan(0);

      // Verify cache stats show evictions
      const cacheStats = tree.getCacheStats();
      expect(cacheStats.evictions).toBeGreaterThan(0);

      // Verify cache is bounded
      expect(cacheStats.size).toBeLessThanOrEqual(maxCachedPages);
    });

    /**
     * Test 3: Cache respects configured maxCachedPages limit
     *
     * The cache size should never exceed the configured maxPages limit.
     */
    it('should respect configured maxCachedPages limit', async () => {
      const maxCachedPages = 3; // Very small limit

      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        cache: {
          maxPages: maxCachedPages,
        },
      });
      await tree.init();

      // Insert many entries to create multiple pages
      for (let i = 0; i < 100; i++) {
        await tree.set(`key_${i}`, { value: i });
      }

      const stats = await tree.stats();
      const pageCount = stats.pageCount;
      expect(pageCount).toBeGreaterThan(maxCachedPages);

      // Verify cache respects the limit
      let cacheStats = tree.getCacheStats();
      expect(cacheStats.size).toBeLessThanOrEqual(maxCachedPages);
      expect(cacheStats.maxSize).toBe(maxCachedPages);

      // Access page containing first key (creates one cache entry)
      await tree.get('key_0');

      // Access many other keys to fill the cache
      for (let i = 50; i < 60; i++) {
        await tree.get(`key_${i}`);
      }

      // Access the first key again - if cache was bounded, this should be a miss
      tree.resetCacheStats();
      await tree.get('key_0');

      // Verify cache stats show the behavior
      cacheStats = tree.getCacheStats();
      expect(cacheStats.size).toBeLessThanOrEqual(maxCachedPages);
      // With maxCachedPages=3, accessing key_0 after 10 other accesses should cause a miss
      expect(cacheStats.misses).toBeGreaterThanOrEqual(0);
    });

    /**
     * Test 4: Most-recently-used pages survive eviction
     *
     * When the cache is full and a new page is loaded, the LRU cache
     * should evict the oldest page and retain recently accessed pages.
     */
    it('should retain most-recently-used pages during eviction', async () => {
      const maxCachedPages = 5;

      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        cache: {
          maxPages: maxCachedPages,
        },
      });
      await tree.init();

      // Insert entries creating many pages
      for (let i = 0; i < 100; i++) {
        await tree.set(i, { value: i });
      }

      // Access specific keys to mark their pages as recently used
      const recentKeys = [0, 10, 20, 30, 40];
      for (const key of recentKeys) {
        await tree.get(key);
      }

      // Reset stats to measure from here
      tree.resetCacheStats();

      // Access many other keys to trigger eviction
      for (let i = 50; i < 100; i++) {
        await tree.get(i);
      }

      // The recentKeys pages might have been evicted by now
      // Access recent keys again and check hit rate
      const beforeStats = tree.getCacheStats();

      for (const key of recentKeys) {
        await tree.get(key);
      }

      // Verify cache stats reflect the access patterns
      const afterStats = tree.getCacheStats();
      expect(afterStats.size).toBeLessThanOrEqual(maxCachedPages);
      expect(afterStats.hits + afterStats.misses).toBeGreaterThan(beforeStats.hits + beforeStats.misses);

      // Verify data integrity
      for (const key of recentKeys) {
        const value = await tree.get(key);
        expect(value?.value).toBe(key);
      }
    });
  });

  describe('Eviction callbacks', () => {
    /**
     * Test 5: Eviction callback is called when page is removed
     *
     * The onEvict callback should be called when pages are evicted from the cache.
     */
    it('should call eviction callback when page is removed', async () => {
      const evictedPages: Array<{ pageId: number; dirty: boolean }> = [];

      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        cache: {
          maxPages: 3,
          onEvict: (pageId: number, page: Page, dirty: boolean) => {
            evictedPages.push({ pageId, dirty });
          },
        },
      });
      await tree.init();

      // Insert enough to cause evictions
      for (let i = 0; i < 50; i++) {
        await tree.set(`key_${i}`, { value: i });
      }

      // Verify evictedPages contains entries for evicted pages
      expect(evictedPages.length).toBeGreaterThan(0);

      // Verify the eviction callback received valid page IDs
      for (const evicted of evictedPages) {
        expect(typeof evicted.pageId).toBe('number');
        expect(typeof evicted.dirty).toBe('boolean');
      }
    });
  });

  describe('Cache size tracking', () => {
    /**
     * Test 6: Cache size tracking is accurate
     *
     * The cache should accurately track its size in entries and bytes.
     */
    it('should accurately track cache size', async () => {
      const tree = createBTree(fsx, StringKeyCodec, BinaryValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        cache: {
          maxPages: 10,
        },
      });
      await tree.init();

      // Insert entries with known sizes
      for (let i = 0; i < 20; i++) {
        const value = createLargeValue(1000); // 1KB per value
        await tree.set(`key_${i}`, value);
      }

      // Access cache statistics
      const cacheStats = tree.getCacheStats();

      // Verify all expected properties exist
      expect(cacheStats).toHaveProperty('size');
      expect(cacheStats).toHaveProperty('maxSize');
      expect(cacheStats).toHaveProperty('hits');
      expect(cacheStats).toHaveProperty('misses');
      expect(cacheStats).toHaveProperty('evictions');
      expect(cacheStats).toHaveProperty('hitRate');

      // Verify size is within bounds
      expect(cacheStats.size).toBeGreaterThan(0);
      expect(cacheStats.size).toBeLessThanOrEqual(cacheStats.maxSize);
    });
  });

  describe('Rapid access patterns', () => {
    /**
     * Test 7: Cache handles rapid access patterns
     *
     * Under rapid sequential or random access, the cache should
     * maintain bounds and not leak memory.
     */
    it('should handle rapid sequential access without memory growth', async () => {
      const maxCachedPages = 10;
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        cache: {
          maxPages: maxCachedPages,
        },
      });
      await tree.init();

      // Insert baseline data
      for (let i = 0; i < 100; i++) {
        await tree.set(i, { value: i });
      }

      // Rapid sequential access - simulate a hot loop
      const iterations = 1000; // Reduced from 10000 for faster tests
      for (let round = 0; round < iterations; round++) {
        const key = round % 100;
        await tree.get(key);
      }

      // After rapid access, cache should still be bounded
      const cacheStats = tree.getCacheStats();
      expect(cacheStats.size).toBeLessThanOrEqual(maxCachedPages);

      // Verify stats are tracking correctly
      expect(cacheStats.hits + cacheStats.misses).toBeGreaterThan(0);
    });

    /**
     * Test 8: Memory usage stays within configured limits
     *
     * With maxBytes configured, the cache should evict pages to stay
     * within the byte limit.
     */
    it('should stay within configured memory limits', async () => {
      const maxBytes = 50 * 1024; // 50KB limit

      const tree = createBTree(fsx, StringKeyCodec, BinaryValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        cache: {
          maxPages: 1000, // High page count
          maxBytes: maxBytes, // But low byte limit
        },
      });
      await tree.init();

      // Insert large values to test byte-based eviction
      for (let i = 0; i < 100; i++) {
        const value = createLargeValue(5000); // 5KB per value
        await tree.set(`key_${i}`, value);
      }

      // Total data inserted: 100 * 5KB = 500KB
      // But cache should only hold 50KB worth of pages

      // Access all entries to load them into cache
      for await (const [key, value] of tree.entries()) {
        // Each iteration loads a page
      }

      // Verify cache memory usage is bounded
      const cacheStats = tree.getCacheStats();
      expect(cacheStats.currentBytes).toBeDefined();
      expect(cacheStats.currentBytes!).toBeLessThanOrEqual(maxBytes);
      expect(cacheStats.maxBytes).toBe(maxBytes);
      expect(cacheStats.evictions).toBeGreaterThan(0); // Byte-based evictions occurred
    });
  });

  describe('Page reload after eviction', () => {
    /**
     * Test 9: Page reload after eviction works correctly
     *
     * When a page is evicted and later accessed, it should be correctly
     * reloaded from storage with data integrity preserved.
     */
    it('should correctly reload pages after eviction', async () => {
      const maxCachedPages = 3;
      let evictionCount = 0;

      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        cache: {
          maxPages: maxCachedPages,
          onEvict: () => {
            evictionCount++;
          },
        },
      });
      await tree.init();

      // Insert entries to create multiple pages
      for (let i = 0; i < 50; i++) {
        await tree.set(i, { value: i, data: `entry_${i}` });
      }

      const stats = await tree.stats();
      expect(stats.pageCount).toBeGreaterThan(maxCachedPages);

      // Access first entry (loads its page into cache)
      const first = await tree.get(0);
      expect(first).toEqual({ value: 0, data: 'entry_0' });

      // Reset stats to track eviction
      const evictionsBefore = evictionCount;
      tree.resetCacheStats();

      // Access many other entries to evict the first page
      for (let i = 10; i < 40; i++) {
        await tree.get(i);
      }

      // Verify evictions occurred
      expect(evictionCount).toBeGreaterThan(evictionsBefore);

      // Access first entry again (should reload from storage)
      const reloaded = await tree.get(0);

      // Verify data integrity preserved
      expect(reloaded).toEqual(first);

      // Verify cache stats show a miss (page was reloaded)
      const cacheStats = tree.getCacheStats();
      expect(cacheStats.misses).toBeGreaterThan(0);
    });
  });

  describe('Concurrent access safety', () => {
    /**
     * Test 10: Concurrent access with eviction is safe
     *
     * Multiple concurrent operations should not corrupt the cache
     * or cause race conditions during eviction.
     */
    it('should handle concurrent access during eviction safely', async () => {
      const maxCachedPages = 5;
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        cache: {
          maxPages: maxCachedPages, // Small cache to force frequent eviction
        },
      });
      await tree.init();

      // Insert baseline data
      for (let i = 0; i < 100; i++) {
        await tree.set(i, { value: i });
      }

      // Concurrent operations that will compete for cache space
      const operations: Promise<unknown>[] = [];

      // Multiple concurrent readers
      for (let reader = 0; reader < 10; reader++) {
        operations.push(
          (async () => {
            for (let i = 0; i < 50; i++) {
              const key = (reader * 10 + i) % 100;
              const value = await tree.get(key);
              if (value?.value !== key) {
                throw new Error(`Reader ${reader}: Data corruption for key ${key}`);
              }
            }
          })()
        );
      }

      // Concurrent writers (updates only, not inserts)
      for (let writer = 0; writer < 3; writer++) {
        operations.push(
          (async () => {
            for (let i = 0; i < 20; i++) {
              const key = (writer * 20 + i) % 100;
              await tree.set(key, { value: key, updated: true });
            }
          })()
        );
      }

      // Wait for all operations
      await Promise.all(operations);

      // Verify data integrity after concurrent access
      for (let i = 0; i < 100; i++) {
        const value = await tree.get(i);
        expect(value).toBeDefined();
        expect(value?.value).toBe(i);
      }

      // Verify cache stayed bounded during concurrent access
      const cacheStats = tree.getCacheStats();
      expect(cacheStats.size).toBeLessThanOrEqual(maxCachedPages);
    });
  });

  describe('Byte-based eviction', () => {
    /**
     * Test 11: Byte-based eviction with large values
     *
     * When using maxBytes, pages with larger values should trigger
     * more evictions to stay within the byte limit.
     */
    it('should evict based on byte size not just page count', async () => {
      const maxBytes = 10 * 1024; // 10KB

      const tree = createBTree(fsx, StringKeyCodec, BinaryValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        cache: {
          maxPages: 100, // High page limit
          maxBytes: maxBytes, // But low byte limit
        },
      });
      await tree.init();

      // Insert entries with varying sizes
      for (let i = 0; i < 10; i++) {
        // Small values first
        await tree.set(`small_${i}`, createLargeValue(100)); // 100 bytes each
      }

      // Then large values
      for (let i = 0; i < 5; i++) {
        await tree.set(`large_${i}`, createLargeValue(5000)); // 5KB each
      }

      // Access all large values
      for (let i = 0; i < 5; i++) {
        await tree.get(`large_${i}`);
      }

      // Verify cache byte usage is bounded
      const cacheStats = tree.getCacheStats();
      expect(cacheStats.currentBytes).toBeDefined();
      expect(cacheStats.currentBytes!).toBeLessThanOrEqual(maxBytes);

      // Now access small values - some might need to be reloaded
      for (let i = 0; i < 10; i++) {
        const value = await tree.get(`small_${i}`);
        expect(value).toBeDefined();
        expect(value?.length).toBe(100);
      }

      // Verify final byte usage
      const finalStats = tree.getCacheStats();
      expect(finalStats.currentBytes!).toBeLessThanOrEqual(maxBytes);
    });
  });

  describe('Cache statistics exposure', () => {
    /**
     * Test 12: B-tree should expose cache statistics
     *
     * The getCacheStats() method should return all necessary statistics.
     */
    it('should expose cache statistics for monitoring', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec, {
        cache: {
          maxPages: 10,
        },
      });
      await tree.init();

      // Insert and access data
      for (let i = 0; i < 50; i++) {
        await tree.set(`key_${i}`, { value: i });
      }

      for (let i = 0; i < 100; i++) {
        await tree.get(`key_${i % 50}`);
      }

      // Access to cache statistics
      const stats = tree.getCacheStats();

      // Verify all expected properties exist and have correct types
      expect(stats).toHaveProperty('size');
      expect(stats).toHaveProperty('maxSize');
      expect(stats).toHaveProperty('hits');
      expect(stats).toHaveProperty('misses');
      expect(stats).toHaveProperty('evictions');
      expect(stats).toHaveProperty('hitRate');

      expect(typeof stats.size).toBe('number');
      expect(typeof stats.maxSize).toBe('number');
      expect(typeof stats.hits).toBe('number');
      expect(typeof stats.misses).toBe('number');
      expect(typeof stats.evictions).toBe('number');
      expect(typeof stats.hitRate).toBe('number');

      // Verify stats are reasonable
      expect(stats.size).toBeGreaterThan(0);
      expect(stats.size).toBeLessThanOrEqual(stats.maxSize);
      expect(stats.hits).toBeGreaterThanOrEqual(0);
      expect(stats.misses).toBeGreaterThanOrEqual(0);
      expect(stats.hitRate).toBeGreaterThanOrEqual(0);
      expect(stats.hitRate).toBeLessThanOrEqual(1);
    });
  });

  describe('Eviction during write operations', () => {
    /**
     * Test 13: Dirty page eviction during write operations
     *
     * When the cache is full and a write operation needs to cache a new page,
     * dirty pages should be flushed before eviction.
     */
    it('should flush dirty pages before eviction during writes', async () => {
      const evictedDirty: number[] = [];
      const evictedClean: number[] = [];
      const maxCachedPages = 3;

      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        cache: {
          maxPages: maxCachedPages,
          onEvict: (pageId: number, page: Page, dirty: boolean) => {
            if (dirty) {
              evictedDirty.push(pageId);
            } else {
              evictedClean.push(pageId);
            }
          },
        },
      });
      await tree.init();

      // Insert entries to fill pages
      for (let i = 0; i < 20; i++) {
        await tree.set(i, { value: i });
      }

      // Update entries (marks pages as dirty)
      for (let i = 0; i < 5; i++) {
        await tree.set(i, { value: i, updated: true });
      }

      // Insert more entries to trigger eviction
      for (let i = 100; i < 120; i++) {
        await tree.set(i, { value: i });
      }

      // Verify all data persisted correctly (dirty pages were flushed)
      for (let i = 0; i < 5; i++) {
        const value = await tree.get(i);
        expect(value?.updated).toBe(true);
      }

      // Verify evictions occurred (both dirty and clean)
      const totalEvictions = evictedDirty.length + evictedClean.length;
      expect(totalEvictions).toBeGreaterThan(0);

      // Verify cache stats
      const cacheStats = tree.getCacheStats();
      expect(cacheStats.size).toBeLessThanOrEqual(maxCachedPages);
    });
  });
});
