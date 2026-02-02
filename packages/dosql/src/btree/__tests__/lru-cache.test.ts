/**
 * LRU/LFU Cache Tests for B-tree Page Manager
 *
 * TDD tests for eviction policies in the B-tree page cache.
 * These tests verify:
 * - Cache respects max size
 * - LRU eviction order is correct
 * - LFU eviction order is correct
 * - Recently accessed items are retained
 * - Frequently accessed items are retained (LFU)
 * - onEvict callback fires for evicted items
 * - Memory pressure callbacks work correctly
 * - Edge cases: empty cache, single item, exact capacity
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { LRUCache, type LRUCacheOptions, type MemoryPressureInfo } from '../lru-cache.js';
import { Page, PageType, createLeafPage } from '../types.js';

/**
 * Helper to create a mock page with predictable size
 */
function createMockPage(id: number, dataSize = 100): Page {
  const page = createLeafPage(id);
  // Add some keys and values to make the page have predictable size
  page.keys = [new Uint8Array(dataSize / 2)];
  page.values = [new Uint8Array(dataSize / 2)];
  return page;
}

describe('LRUCache', () => {
  describe('basic operations', () => {
    it('should create an empty cache', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });
      expect(cache.size).toBe(0);
    });

    it('should set and get a value', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });
      const page = createMockPage(1);

      cache.set(1, page);
      const retrieved = cache.get(1);

      expect(retrieved).toBe(page);
      expect(cache.size).toBe(1);
    });

    it('should return undefined for missing keys', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      const result = cache.get(999);

      expect(result).toBeUndefined();
    });

    it('should update existing values', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });
      const page1 = createMockPage(1);
      const page2 = createMockPage(1);
      page2.keys = [new Uint8Array(200)]; // Different content

      cache.set(1, page1);
      cache.set(1, page2);

      expect(cache.get(1)).toBe(page2);
      expect(cache.size).toBe(1);
    });

    it('should delete values', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });
      const page = createMockPage(1);

      cache.set(1, page);
      const deleted = cache.delete(1);

      expect(deleted).toBe(true);
      expect(cache.get(1)).toBeUndefined();
      expect(cache.size).toBe(0);
    });

    it('should return false when deleting non-existent key', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      const deleted = cache.delete(999);

      expect(deleted).toBe(false);
    });

    it('should check if key exists', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });
      const page = createMockPage(1);

      cache.set(1, page);

      expect(cache.has(1)).toBe(true);
      expect(cache.has(999)).toBe(false);
    });

    it('should clear all entries', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      cache.set(1, createMockPage(1));
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));

      cache.clear();

      expect(cache.size).toBe(0);
      expect(cache.get(1)).toBeUndefined();
      expect(cache.get(2)).toBeUndefined();
      expect(cache.get(3)).toBeUndefined();
    });
  });

  describe('max size by entry count', () => {
    it('should respect max size limit', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 3 });

      cache.set(1, createMockPage(1));
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));
      cache.set(4, createMockPage(4)); // This should evict oldest

      expect(cache.size).toBe(3);
    });

    it('should evict least recently used entry when full', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 3 });

      cache.set(1, createMockPage(1)); // LRU order: 1
      cache.set(2, createMockPage(2)); // LRU order: 1, 2
      cache.set(3, createMockPage(3)); // LRU order: 1, 2, 3
      cache.set(4, createMockPage(4)); // Should evict 1, order: 2, 3, 4

      expect(cache.get(1)).toBeUndefined(); // Evicted
      expect(cache.get(2)).toBeDefined();
      expect(cache.get(3)).toBeDefined();
      expect(cache.get(4)).toBeDefined();
    });

    it('should update LRU order on get', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 3 });

      cache.set(1, createMockPage(1)); // LRU order: 1
      cache.set(2, createMockPage(2)); // LRU order: 1, 2
      cache.set(3, createMockPage(3)); // LRU order: 1, 2, 3
      cache.get(1); // Access 1, order: 2, 3, 1
      cache.set(4, createMockPage(4)); // Should evict 2, order: 3, 1, 4

      expect(cache.get(1)).toBeDefined(); // Still present
      expect(cache.get(2)).toBeUndefined(); // Evicted
      expect(cache.get(3)).toBeDefined();
      expect(cache.get(4)).toBeDefined();
    });

    it('should update LRU order on set for existing key', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 3 });

      cache.set(1, createMockPage(1)); // LRU order: 1
      cache.set(2, createMockPage(2)); // LRU order: 1, 2
      cache.set(3, createMockPage(3)); // LRU order: 1, 2, 3
      cache.set(1, createMockPage(1)); // Update 1, order: 2, 3, 1
      cache.set(4, createMockPage(4)); // Should evict 2, order: 3, 1, 4

      expect(cache.get(1)).toBeDefined(); // Still present
      expect(cache.get(2)).toBeUndefined(); // Evicted
      expect(cache.get(3)).toBeDefined();
      expect(cache.get(4)).toBeDefined();
    });

    it('should retain recently accessed items', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 3 });

      // Fill cache
      cache.set(1, createMockPage(1));
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));

      // Access all items to keep them fresh
      cache.get(1);
      cache.get(2);
      cache.get(3);

      // Add new item, should evict 1 (it was accessed longest ago after the gets)
      cache.set(4, createMockPage(4));

      expect(cache.get(1)).toBeUndefined(); // Evicted (oldest after the gets)
      expect(cache.get(2)).toBeDefined();
      expect(cache.get(3)).toBeDefined();
      expect(cache.get(4)).toBeDefined();
    });
  });

  describe('max size by bytes', () => {
    it('should respect max bytes limit', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 1000, // 1000 bytes
        sizeCalculator: (page) => {
          // Simple size calculation
          let size = 0;
          for (const key of page.keys) size += key.byteLength;
          for (const value of page.values) size += value.byteLength;
          return size;
        },
      });

      // Each page is ~100 bytes
      cache.set(1, createMockPage(1, 100));
      cache.set(2, createMockPage(2, 100));
      cache.set(3, createMockPage(3, 100));
      cache.set(4, createMockPage(4, 100));
      cache.set(5, createMockPage(5, 100));
      cache.set(6, createMockPage(6, 100));
      cache.set(7, createMockPage(7, 100));
      cache.set(8, createMockPage(8, 100));
      cache.set(9, createMockPage(9, 100));
      cache.set(10, createMockPage(10, 100));
      cache.set(11, createMockPage(11, 100)); // Should trigger eviction

      // Should have evicted at least one entry to stay under 1000 bytes
      expect(cache.size).toBeLessThanOrEqual(10);
    });

    it('should evict multiple entries if needed for large insert', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 500, // 500 bytes
        sizeCalculator: (page) => {
          let size = 0;
          for (const key of page.keys) size += key.byteLength;
          for (const value of page.values) size += value.byteLength;
          return size;
        },
      });

      // Add 5 entries of 100 bytes each (total 500)
      for (let i = 1; i <= 5; i++) {
        cache.set(i, createMockPage(i, 100));
      }

      // Add a 300 byte entry - should evict 3 entries
      cache.set(6, createMockPage(6, 300));

      // Should have evicted at least 3 entries
      expect(cache.size).toBeLessThanOrEqual(3);
    });

    it('should track byte size correctly', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 1000,
        sizeCalculator: (page) => {
          let size = 0;
          for (const key of page.keys) size += key.byteLength;
          for (const value of page.values) size += value.byteLength;
          return size;
        },
      });

      cache.set(1, createMockPage(1, 100));
      cache.set(2, createMockPage(2, 200));

      expect(cache.currentBytes).toBe(300);

      cache.delete(1);

      expect(cache.currentBytes).toBe(200);
    });
  });

  describe('onEvict callback', () => {
    it('should call onEvict when entry is evicted', () => {
      const evicted: Array<{ key: number; value: Page }> = [];
      const cache = new LRUCache<number, Page>({
        maxSize: 2,
        onEvict: (key, value) => {
          evicted.push({ key, value });
        },
      });

      const page1 = createMockPage(1);
      const page2 = createMockPage(2);
      const page3 = createMockPage(3);

      cache.set(1, page1);
      cache.set(2, page2);
      cache.set(3, page3); // Should evict 1

      expect(evicted).toHaveLength(1);
      expect(evicted[0].key).toBe(1);
      expect(evicted[0].value).toBe(page1);
    });

    it('should call onEvict for multiple evictions', () => {
      const evictedKeys: number[] = [];
      const cache = new LRUCache<number, Page>({
        maxSize: 2,
        onEvict: (key) => {
          evictedKeys.push(key);
        },
      });

      cache.set(1, createMockPage(1));
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3)); // Evicts 1
      cache.set(4, createMockPage(4)); // Evicts 2
      cache.set(5, createMockPage(5)); // Evicts 3

      expect(evictedKeys).toEqual([1, 2, 3]);
    });

    it('should not call onEvict for manual delete', () => {
      const evicted: number[] = [];
      const cache = new LRUCache<number, Page>({
        maxSize: 10,
        onEvict: (key) => {
          evicted.push(key);
        },
      });

      cache.set(1, createMockPage(1));
      cache.delete(1);

      expect(evicted).toHaveLength(0);
    });

    it('should not call onEvict when updating existing entry', () => {
      const evicted: number[] = [];
      const cache = new LRUCache<number, Page>({
        maxSize: 2,
        onEvict: (key) => {
          evicted.push(key);
        },
      });

      cache.set(1, createMockPage(1));
      cache.set(1, createMockPage(1)); // Update

      expect(evicted).toHaveLength(0);
    });

    it('should call onEvict on clear', () => {
      const evictedKeys: number[] = [];
      const cache = new LRUCache<number, Page>({
        maxSize: 10,
        onEvict: (key) => {
          evictedKeys.push(key);
        },
        evictOnClear: true,
      });

      cache.set(1, createMockPage(1));
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));

      cache.clear();

      expect(evictedKeys.sort()).toEqual([1, 2, 3]);
    });
  });

  describe('edge cases', () => {
    it('should handle empty cache operations', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      expect(cache.size).toBe(0);
      expect(cache.get(1)).toBeUndefined();
      expect(cache.delete(1)).toBe(false);
      expect(cache.has(1)).toBe(false);

      cache.clear(); // Should not throw
      expect(cache.size).toBe(0);
    });

    it('should handle single item cache', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 1 });
      const page1 = createMockPage(1);
      const page2 = createMockPage(2);

      cache.set(1, page1);
      expect(cache.get(1)).toBe(page1);
      expect(cache.size).toBe(1);

      cache.set(2, page2);
      expect(cache.get(1)).toBeUndefined(); // Evicted
      expect(cache.get(2)).toBe(page2);
      expect(cache.size).toBe(1);
    });

    it('should handle exact capacity', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 3 });

      cache.set(1, createMockPage(1));
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));

      expect(cache.size).toBe(3);
      expect(cache.get(1)).toBeDefined();
      expect(cache.get(2)).toBeDefined();
      expect(cache.get(3)).toBeDefined();
    });

    it('should handle zero max size', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 0 });

      cache.set(1, createMockPage(1));

      // With maxSize 0, nothing should be stored
      expect(cache.size).toBe(0);
      expect(cache.get(1)).toBeUndefined();
    });

    it('should handle string keys', () => {
      const cache = new LRUCache<string, Page>({ maxSize: 10 });

      cache.set('page_1', createMockPage(1));
      cache.set('page_2', createMockPage(2));

      expect(cache.get('page_1')).toBeDefined();
      expect(cache.get('page_2')).toBeDefined();
    });

    it('should handle rapid set/get operations', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 100 });

      // Rapid operations
      for (let i = 0; i < 1000; i++) {
        cache.set(i, createMockPage(i));
      }

      // Should have only 100 most recent
      expect(cache.size).toBe(100);

      // Oldest 900 should be evicted
      for (let i = 0; i < 900; i++) {
        expect(cache.get(i)).toBeUndefined();
      }

      // Newest 100 should be present
      for (let i = 900; i < 1000; i++) {
        expect(cache.get(i)).toBeDefined();
      }
    });

    it('should handle peek operation without updating LRU order', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 3 });

      cache.set(1, createMockPage(1)); // LRU order: 1
      cache.set(2, createMockPage(2)); // LRU order: 1, 2
      cache.set(3, createMockPage(3)); // LRU order: 1, 2, 3

      // Peek at 1 without updating order
      const peeked = cache.peek(1);
      expect(peeked).toBeDefined();

      // Add new entry - should evict 1 (not 2) since peek doesn't update order
      cache.set(4, createMockPage(4));

      expect(cache.get(1)).toBeUndefined(); // Should still be evicted
      expect(cache.get(2)).toBeDefined();
      expect(cache.get(3)).toBeDefined();
      expect(cache.get(4)).toBeDefined();
    });
  });

  describe('iteration', () => {
    it('should iterate over all entries', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      cache.set(1, createMockPage(1));
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));

      const entries: Array<[number, Page]> = [];
      for (const entry of cache.entries()) {
        entries.push(entry);
      }

      expect(entries).toHaveLength(3);
      expect(entries.map(([k]) => k).sort()).toEqual([1, 2, 3]);
    });

    it('should iterate over keys', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      cache.set(1, createMockPage(1));
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));

      const keys: number[] = [];
      for (const key of cache.keys()) {
        keys.push(key);
      }

      expect(keys.sort()).toEqual([1, 2, 3]);
    });

    it('should iterate over values', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      cache.set(1, createMockPage(1));
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));

      const values: Page[] = [];
      for (const value of cache.values()) {
        values.push(value);
      }

      expect(values).toHaveLength(3);
    });

    it('should iterate in LRU order (oldest to newest)', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      cache.set(1, createMockPage(1));
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));
      cache.get(1); // Move 1 to most recent

      const keys: number[] = [];
      for (const key of cache.keys()) {
        keys.push(key);
      }

      // Should be in LRU order: 2 (oldest), 3, 1 (newest)
      expect(keys).toEqual([2, 3, 1]);
    });
  });

  describe('dirty page handling', () => {
    it('should track dirty entries', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      const page = createMockPage(1);
      cache.set(1, page, { dirty: true });

      expect(cache.isDirty(1)).toBe(true);
    });

    it('should allow marking entry as clean', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      cache.set(1, createMockPage(1), { dirty: true });
      cache.markClean(1);

      expect(cache.isDirty(1)).toBe(false);
    });

    it('should call onEvict with dirty flag', () => {
      let evictedDirty = false;
      const cache = new LRUCache<number, Page>({
        maxSize: 2,
        onEvict: (key, value, dirty) => {
          if (key === 1) evictedDirty = dirty;
        },
      });

      cache.set(1, createMockPage(1), { dirty: true });
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3)); // Evicts 1

      expect(evictedDirty).toBe(true);
    });

    it('should return list of dirty keys', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      cache.set(1, createMockPage(1), { dirty: true });
      cache.set(2, createMockPage(2), { dirty: false });
      cache.set(3, createMockPage(3), { dirty: true });

      const dirtyKeys = cache.getDirtyKeys();

      expect(dirtyKeys.sort()).toEqual([1, 3]);
    });
  });

  describe('async onEvict race condition', () => {
    it('should await async onEvict before completing set', async () => {
      const writeLog: string[] = [];
      let resolveWrite: (() => void) | null = null;

      const cache = new LRUCache<number, Page>({
        maxSize: 2,
        onEvict: async (key, _value, dirty) => {
          writeLog.push(`evict-start:${key}:dirty=${dirty}`);
          if (dirty) {
            // Simulate an async storage write that takes time
            await new Promise<void>((resolve) => {
              resolveWrite = resolve;
            });
          }
          writeLog.push(`evict-end:${key}`);
        },
      });

      cache.set(1, createMockPage(1), { dirty: true });
      cache.set(2, createMockPage(2));

      // This set triggers eviction of page 1 (dirty). The async onEvict
      // must complete before set returns, so the dirty page is persisted.
      const setPromise = cache.setAsync(3, createMockPage(3));

      // The eviction should have started
      expect(writeLog).toContain('evict-start:1:dirty=true');

      // But it should NOT have completed yet (async write still pending)
      expect(writeLog).not.toContain('evict-end:1');

      // Resolve the simulated write
      expect(resolveWrite).not.toBeNull();
      resolveWrite!();

      // Now await the set to complete
      await setPromise;

      // After set completes, eviction should be fully done
      expect(writeLog).toContain('evict-end:1');
    });

    it('should not lose dirty page data during concurrent eviction and read', async () => {
      // This test simulates the actual race: a dirty page is evicted while
      // something else tries to read it. The eviction write must complete
      // before the page is removed from cache.
      const storage = new Map<number, string>();
      let writeDelay: Promise<void> | null = null;
      let resolveWriteDelay: (() => void) | null = null;

      const cache = new LRUCache<number, string>({
        maxSize: 2,
        onEvict: async (key, value, dirty) => {
          if (dirty) {
            // Simulate slow storage write
            writeDelay = new Promise<void>((resolve) => {
              resolveWriteDelay = resolve;
            });
            await writeDelay;
            storage.set(key, value);
          }
        },
      });

      cache.set(1, 'original-value-1', { dirty: true });
      cache.set(2, 'value-2');

      // Trigger eviction of key 1 (dirty)
      const setPromise = cache.setAsync(3, 'value-3');

      // Key 1 is being evicted but write hasn't completed yet
      // In the buggy version, the page would already be gone from cache
      // but not yet written to storage

      // Resolve the write
      resolveWriteDelay!();
      await setPromise;

      // The dirty value should have been persisted to storage
      expect(storage.get(1)).toBe('original-value-1');
    });

    it('should await async onEvict during clear with evictOnClear', async () => {
      const completedEvictions: number[] = [];

      const cache = new LRUCache<number, Page>({
        maxSize: 10,
        onEvict: async (key, _value, dirty) => {
          // Simulate async work
          await new Promise((resolve) => setTimeout(resolve, 1));
          completedEvictions.push(key);
        },
        evictOnClear: true,
      });

      cache.set(1, createMockPage(1), { dirty: true });
      cache.set(2, createMockPage(2), { dirty: true });
      cache.set(3, createMockPage(3), { dirty: true });

      await cache.clearAsync();

      // All evictions should have completed
      expect(completedEvictions.sort()).toEqual([1, 2, 3]);
      expect(cache.size).toBe(0);
    });
  });

  describe('LFU eviction policy', () => {
    it('should evict least frequently used entry', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 3,
        evictionPolicy: 'lfu',
      });

      cache.set(1, createMockPage(1)); // freq: 1
      cache.set(2, createMockPage(2)); // freq: 1
      cache.set(3, createMockPage(3)); // freq: 1

      // Access item 2 and 3 multiple times
      cache.get(2); // freq: 2
      cache.get(3); // freq: 2
      cache.get(3); // freq: 3

      // Add new item - should evict item 1 (lowest frequency)
      cache.set(4, createMockPage(4));

      expect(cache.get(1)).toBeUndefined(); // Evicted (freq was 1)
      expect(cache.get(2)).toBeDefined(); // Still present (freq was 2)
      expect(cache.get(3)).toBeDefined(); // Still present (freq was 3)
      expect(cache.get(4)).toBeDefined(); // Newly added
    });

    it('should use LRU order for ties in frequency', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 3,
        evictionPolicy: 'lfu',
      });

      cache.set(1, createMockPage(1)); // freq: 1, oldest
      cache.set(2, createMockPage(2)); // freq: 1
      cache.set(3, createMockPage(3)); // freq: 1, newest

      // All have same frequency, should evict oldest (1)
      cache.set(4, createMockPage(4));

      expect(cache.get(1)).toBeUndefined(); // Evicted (oldest with freq 1)
      expect(cache.get(2)).toBeDefined();
      expect(cache.get(3)).toBeDefined();
      expect(cache.get(4)).toBeDefined();
    });

    it('should track frequency correctly', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 10,
        evictionPolicy: 'lfu',
      });

      cache.set(1, createMockPage(1));
      expect(cache.getFrequency(1)).toBe(1);

      cache.get(1);
      expect(cache.getFrequency(1)).toBe(2);

      cache.get(1);
      cache.get(1);
      expect(cache.getFrequency(1)).toBe(4);
    });

    it('should report LFU policy', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 10,
        evictionPolicy: 'lfu',
      });

      expect(cache.policy).toBe('lfu');
    });

    it('should handle rapid access patterns', () => {
      const evictedKeys: number[] = [];
      const cache = new LRUCache<number, Page>({
        maxSize: 5,
        evictionPolicy: 'lfu',
        onEvict: (key) => evictedKeys.push(key),
      });

      // Add 5 items
      for (let i = 1; i <= 5; i++) {
        cache.set(i, createMockPage(i));
      }

      // Access items with different frequencies
      // Item 5: accessed 10 times
      for (let i = 0; i < 10; i++) cache.get(5);
      // Item 4: accessed 5 times
      for (let i = 0; i < 5; i++) cache.get(4);
      // Item 3: accessed 3 times
      for (let i = 0; i < 3; i++) cache.get(3);
      // Item 2: accessed 2 times
      for (let i = 0; i < 2; i++) cache.get(2);
      // Item 1: accessed 1 time (original set counts as 1)

      // Add new item - should evict item 1
      cache.set(6, createMockPage(6));
      expect(evictedKeys).toContain(1);

      // Add another - should evict item 6 (freq 1) or item 2 (freq 2)
      cache.set(7, createMockPage(7));
      expect(evictedKeys.length).toBe(2);
    });
  });

  describe('ARC eviction policy', () => {
    it('should create cache with ARC policy', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 10,
        evictionPolicy: 'arc',
      });

      expect(cache.policy).toBe('arc');
    });

    it('should add new items to T1 (recency list)', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 10,
        evictionPolicy: 'arc',
      });

      cache.set(1, createMockPage(1));
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));

      const stats = cache.getARCStats();
      expect(stats.t1Size).toBe(3);
      expect(stats.t2Size).toBe(0);
    });

    it('should move items to T2 on second access', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 10,
        evictionPolicy: 'arc',
      });

      cache.set(1, createMockPage(1)); // T1
      cache.set(2, createMockPage(2)); // T1
      cache.get(1); // Move to T2

      const stats = cache.getARCStats();
      expect(stats.t1Size).toBe(1); // Only item 2
      expect(stats.t2Size).toBe(1); // Item 1 moved to T2
    });

    it('should respect max size', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 3,
        evictionPolicy: 'arc',
      });

      cache.set(1, createMockPage(1));
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));
      cache.set(4, createMockPage(4)); // Should trigger eviction

      expect(cache.size).toBe(3);
    });

    it('should populate ghost lists on eviction', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 2,
        evictionPolicy: 'arc',
      });

      cache.set(1, createMockPage(1)); // T1
      cache.set(2, createMockPage(2)); // T1
      cache.set(3, createMockPage(3)); // Evicts 1 to B1

      const stats = cache.getARCStats();
      expect(stats.b1Size).toBe(1); // Item 1 should be in ghost list B1
    });

    it('should adapt p parameter on ghost list hits', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 3,
        evictionPolicy: 'arc',
      });

      // Fill cache
      cache.set(1, createMockPage(1));
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));

      const initialP = cache.arcTargetT1Size;

      // Evict item 1 by adding item 4
      cache.set(4, createMockPage(4));

      // Item 1 should now be in B1
      // Re-add item 1 - this is a ghost hit on B1
      cache.set(1, createMockPage(1));

      // P should have increased (favor T1)
      expect(cache.arcTargetT1Size).toBeGreaterThanOrEqual(initialP);
    });

    it('should retain frequently accessed items', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 3,
        evictionPolicy: 'arc',
      });

      cache.set(1, createMockPage(1));
      cache.get(1); // Move to T2 (frequently accessed)

      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));
      cache.set(4, createMockPage(4)); // Should evict from T1, not T2

      // Item 1 should still be present (it's in T2)
      expect(cache.get(1)).toBeDefined();
    });

    it('should handle mixed access patterns', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 5,
        evictionPolicy: 'arc',
      });

      // Add items
      for (let i = 1; i <= 5; i++) {
        cache.set(i, createMockPage(i));
      }

      // Make items 1, 2, 3 frequently accessed (move to T2)
      cache.get(1);
      cache.get(2);
      cache.get(3);

      // Now add more items, which should evict from T1 (items 4, 5)
      cache.set(6, createMockPage(6));
      cache.set(7, createMockPage(7));

      // Frequently accessed items should still be present
      expect(cache.get(1)).toBeDefined();
      expect(cache.get(2)).toBeDefined();
      expect(cache.get(3)).toBeDefined();

      // At least one of the less frequently accessed items should be evicted
      const evictedCount = [4, 5].filter((i) => !cache.has(i)).length;
      expect(evictedCount).toBeGreaterThanOrEqual(2);
    });

    it('should clear all ARC state', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 3,
        evictionPolicy: 'arc',
      });

      cache.set(1, createMockPage(1));
      cache.get(1); // Move to T2
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));
      cache.set(4, createMockPage(4)); // Evict to ghost list

      cache.clear();

      const stats = cache.getARCStats();
      expect(stats.t1Size).toBe(0);
      expect(stats.t2Size).toBe(0);
      expect(stats.b1Size).toBe(0);
      expect(stats.b2Size).toBe(0);
      expect(stats.p).toBe(0);
      expect(cache.size).toBe(0);
    });

    it('should delete items correctly', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 10,
        evictionPolicy: 'arc',
      });

      cache.set(1, createMockPage(1));
      cache.get(1); // Move to T2
      cache.set(2, createMockPage(2)); // In T1

      const deleted1 = cache.delete(1);
      const deleted2 = cache.delete(2);

      expect(deleted1).toBe(true);
      expect(deleted2).toBe(true);
      expect(cache.size).toBe(0);

      const stats = cache.getARCStats();
      expect(stats.t1Size).toBe(0);
      expect(stats.t2Size).toBe(0);
    });
  });

  describe('cache statistics', () => {
    it('should track hits and misses', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      cache.set(1, createMockPage(1));
      cache.get(1); // Hit
      cache.get(1); // Hit
      cache.get(2); // Miss (undefined returned, but not tracked)
      cache.recordMiss(); // Explicitly record miss

      expect(cache.hits).toBe(2);
      expect(cache.misses).toBe(1);
    });

    it('should calculate hit rate correctly', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      cache.set(1, createMockPage(1));
      cache.get(1); // Hit
      cache.get(1); // Hit
      cache.recordMiss();
      cache.recordMiss();

      // 2 hits, 2 misses = 50% hit rate
      expect(cache.hitRate).toBe(0.5);
    });

    it('should track evictions', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 2 });

      cache.set(1, createMockPage(1));
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3)); // Evicts 1
      cache.set(4, createMockPage(4)); // Evicts 2

      expect(cache.evictions).toBe(2);
    });

    it('should reset statistics', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 2 });

      cache.set(1, createMockPage(1));
      cache.get(1);
      cache.recordMiss();
      cache.set(2, createMockPage(2));
      cache.set(3, createMockPage(3));

      expect(cache.hits).toBeGreaterThan(0);
      expect(cache.misses).toBeGreaterThan(0);
      expect(cache.evictions).toBeGreaterThan(0);

      cache.resetStats();

      expect(cache.hits).toBe(0);
      expect(cache.misses).toBe(0);
      expect(cache.evictions).toBe(0);
    });

    it('should handle zero requests for hit rate', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      // No requests made
      expect(cache.hitRate).toBe(0);
    });
  });

  describe('memory pressure callbacks', () => {
    it('should call onMemoryPressure when threshold crossed', () => {
      const pressureEvents: MemoryPressureInfo[] = [];
      const cache = new LRUCache<number, Page>({
        maxSize: 1000,
        sizeCalculator: (page) => {
          let size = 0;
          for (const key of page.keys) size += key.byteLength;
          for (const value of page.values) size += value.byteLength;
          return size;
        },
        onMemoryPressure: (info) => pressureEvents.push(info),
        pressureThresholds: {
          low: 0.5,
          medium: 0.75,
          high: 0.9,
        },
      });

      // Add items to trigger pressure thresholds
      // Each page is 100 bytes
      // Need to cross from below 50% to above 75% to trigger callback
      // (initial level is 'low', so crossing into 'low' doesn't trigger callback)
      for (let i = 1; i <= 8; i++) {
        cache.set(i, createMockPage(i, 100)); // 800 bytes total = 80%
      }

      // Should have received callback when crossing 75% threshold
      expect(pressureEvents.length).toBeGreaterThan(0);
      // First callback should be 'medium' when crossing 75%
      expect(pressureEvents[0].level).toBe('medium');
      expect(pressureEvents[0].usageRatio).toBeGreaterThanOrEqual(0.75);
    });

    it('should report memory usage ratio', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 1000,
        sizeCalculator: (page) => {
          let size = 0;
          for (const key of page.keys) size += key.byteLength;
          for (const value of page.values) size += value.byteLength;
          return size;
        },
      });

      cache.set(1, createMockPage(1, 500)); // 500 bytes = 50%

      expect(cache.memoryUsageRatio).toBe(0.5);
    });

    it('should allow manual eviction', () => {
      const cache = new LRUCache<number, Page>({ maxSize: 10 });

      for (let i = 1; i <= 10; i++) {
        cache.set(i, createMockPage(i));
      }

      expect(cache.size).toBe(10);

      cache.evict(3); // Manually evict 3 entries

      expect(cache.size).toBe(7);
    });

    it('should allow eviction to target ratio', () => {
      const cache = new LRUCache<number, Page>({
        maxSize: 1000,
        sizeCalculator: (page) => {
          let size = 0;
          for (const key of page.keys) size += key.byteLength;
          for (const value of page.values) size += value.byteLength;
          return size;
        },
      });

      // Add 10 pages of 100 bytes each = 1000 bytes = 100%
      for (let i = 1; i <= 10; i++) {
        cache.set(i, createMockPage(i, 100));
      }

      expect(cache.memoryUsageRatio).toBe(1.0);

      cache.evictToRatio(0.5); // Evict until 50%

      expect(cache.memoryUsageRatio).toBeLessThanOrEqual(0.5);
    });
  });
});
