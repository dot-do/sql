/**
 * LRU Cache Tests for B-tree Page Manager
 *
 * TDD tests for LRU eviction policy in the B-tree page cache.
 * These tests verify:
 * - Cache respects max size
 * - LRU eviction order is correct
 * - Recently accessed items are retained
 * - onEvict callback fires for evicted items
 * - Edge cases: empty cache, single item, exact capacity
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { LRUCache, type LRUCacheOptions } from '../lru-cache.js';
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
});
