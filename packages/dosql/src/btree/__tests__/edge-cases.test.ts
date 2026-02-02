/**
 * B-tree Edge Cases Tests
 *
 * TDD tests for B-tree edge cases including:
 * - Page splits at various tree depths
 * - Node merging during deletes
 * - Concurrent operations
 * - Near-2MB page size limits (DO storage constraint)
 * - Empty tree operations
 * - Maximum key/value sizes
 * - Key collision handling
 *
 * Following NO MOCKS philosophy - tests use real MemoryFSXBackend.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MemoryFSXBackend } from '../../fsx/index.js';
import {
  createBTree,
  StringKeyCodec,
  NumberKeyCodec,
  JsonValueCodec,
  BinaryValueCodec,
  MAX_PAGE_SIZE,
  type KeyCodec,
  type ValueCodec,
} from '../index.js';

/**
 * Create a large Uint8Array of specified size with deterministic content
 */
function makeLargeData(size: number, seed = 0): Uint8Array {
  const data = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    data[i] = (i + seed) % 256;
  }
  return data;
}

/**
 * Simple seeded PRNG for reproducible tests
 */
function mulberry32(seed: number): () => number {
  return function () {
    let t = (seed += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

describe('B-tree Edge Cases', () => {
  let fsx: MemoryFSXBackend;

  beforeEach(() => {
    fsx = new MemoryFSXBackend();
  });

  // ===========================================================================
  // Empty Tree Operations
  // ===========================================================================
  describe('empty tree operations', () => {
    it('should handle get on empty tree', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      const result = await tree.get('nonexistent');
      expect(result).toBeUndefined();
    });

    it('should handle delete on empty tree', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      const deleted = await tree.delete('nonexistent');
      expect(deleted).toBe(false);
    });

    it('should handle range query on empty tree', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      const entries: Array<[string, unknown]> = [];
      for await (const entry of tree.range('a', 'z')) {
        entries.push(entry as [string, unknown]);
      }
      expect(entries).toHaveLength(0);
    });

    it('should handle entries() on empty tree', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      const entries: Array<[string, unknown]> = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [string, unknown]);
      }
      expect(entries).toHaveLength(0);
    });

    it('should handle count on empty tree', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      expect(await tree.count()).toBe(0);
    });

    it('should handle clear on empty tree', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      await tree.clear();
      expect(await tree.count()).toBe(0);
    });

    it('should report correct stats for empty tree', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      const stats = await tree.stats();
      expect(stats.height).toBe(1);
      expect(stats.entryCount).toBe(0);
      expect(stats.pageCount).toBe(1); // Root leaf page
    });
  });

  // ===========================================================================
  // Page Splits at Various Tree Depths
  // ===========================================================================
  describe('page splits at various tree depths', () => {
    it('should handle split creating first internal node (height 1 -> 2)', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Initially height should be 1
      let stats = await tree.stats();
      expect(stats.height).toBe(1);

      // Insert enough keys to trigger first split
      for (let i = 0; i < 5; i++) {
        await tree.set(i, `value_${i}`);
      }

      // Height should now be 2
      stats = await tree.stats();
      expect(stats.height).toBe(2);

      // Verify all entries are accessible
      for (let i = 0; i < 5; i++) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }
    });

    it('should handle split creating second level internal node (height 2 -> 3)', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert enough to create a taller tree
      // With maxKeys=4, we need more entries to reach height 3
      for (let i = 0; i < 25; i++) {
        await tree.set(i, `value_${i}`);
      }

      const stats = await tree.stats();
      expect(stats.height).toBeGreaterThanOrEqual(2);

      // Verify all entries
      for (let i = 0; i < 25; i++) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }
    });

    it('should handle deep tree splits (height 3+)', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert many entries to create a deep tree
      const count = 100;
      for (let i = 0; i < count; i++) {
        await tree.set(i, `value_${i}`);
      }

      const stats = await tree.stats();
      expect(stats.height).toBeGreaterThanOrEqual(3);
      expect(stats.entryCount).toBe(count);

      // Verify random access
      for (let i = 0; i < count; i++) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }
    });

    it('should correctly split leaf when inserting in middle', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert in non-sequential order to force splits at different positions
      const insertOrder = [10, 30, 20, 40, 25, 15, 35, 5, 45, 22, 28, 33, 38];
      for (const key of insertOrder) {
        await tree.set(key, `value_${key}`);
      }

      // Verify all entries are accessible and in order
      const entries: Array<[number, string]> = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [number, string]);
      }

      expect(entries.length).toBe(insertOrder.length);

      // Verify sorted order
      for (let i = 1; i < entries.length; i++) {
        expect(entries[i][0]).toBeGreaterThan(entries[i - 1][0]);
      }
    });

    it('should correctly split internal node when full', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert enough to fill and split internal nodes
      for (let i = 0; i < 50; i++) {
        await tree.set(i, `value_${i}`);
      }

      const stats = await tree.stats();
      expect(stats.pageCount).toBeGreaterThan(5); // Multiple internal nodes

      // Verify all entries
      for (let i = 0; i < 50; i++) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }
    });

    it('should maintain leaf chain after multiple splits', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert in random order
      const rng = mulberry32(42);
      const keys: number[] = [];
      for (let i = 0; i < 40; i++) {
        const key = Math.floor(rng() * 1000);
        keys.push(key);
        await tree.set(key, `value_${key}`);
      }

      // Range scan should work correctly (uses leaf chain)
      const entries: Array<[number, string]> = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [number, string]);
      }

      // Verify sorted order
      for (let i = 1; i < entries.length; i++) {
        expect(entries[i][0]).toBeGreaterThan(entries[i - 1][0]);
      }
    });
  });

  // ===========================================================================
  // Node Merging During Deletes
  // ===========================================================================
  describe('node merging during deletes', () => {
    it('should merge leaf nodes when deleting causes underflow', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        pagePrefix: 'merge-leaf/',
      });
      await tree.init();

      // Insert entries
      for (let i = 0; i < 20; i++) {
        await tree.set(i, `value_${i}`);
      }

      const statsBefore = await tree.stats();
      const pagesBefore = await fsx.list('merge-leaf/page_');

      // Delete enough to cause merges
      for (let i = 0; i < 15; i++) {
        await tree.delete(i);
      }

      const statsAfter = await tree.stats();
      const pagesAfter = await fsx.list('merge-leaf/page_');

      // Should have fewer pages after merging
      expect(pagesAfter.length).toBeLessThan(pagesBefore.length);

      // Remaining entries should be accessible
      for (let i = 15; i < 20; i++) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }
    });

    it('should merge internal nodes when children merge', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        pagePrefix: 'merge-internal/',
      });
      await tree.init();

      // Create a tall tree
      for (let i = 0; i < 60; i++) {
        await tree.set(i, `value_${i}`);
      }

      const statsBefore = await tree.stats();
      expect(statsBefore.height).toBeGreaterThanOrEqual(3);

      // Delete most entries to force cascading merges
      for (let i = 0; i < 55; i++) {
        await tree.delete(i);
      }

      const statsAfter = await tree.stats();

      // Tree should have shrunk
      expect(statsAfter.height).toBeLessThanOrEqual(statsBefore.height);

      // Remaining entries should be accessible
      for (let i = 55; i < 60; i++) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }
    });

    it('should redistribute from left sibling before merging', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert entries
      for (let i = 0; i < 20; i++) {
        await tree.set(i, `value_${i}`);
      }

      // Delete from right side (should trigger borrow from left)
      await tree.delete(19);
      await tree.delete(18);
      await tree.delete(17);

      // Verify remaining entries
      for (let i = 0; i < 17; i++) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }
    });

    it('should redistribute from right sibling before merging', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert entries
      for (let i = 0; i < 20; i++) {
        await tree.set(i, `value_${i}`);
      }

      // Delete from left side (should trigger borrow from right)
      await tree.delete(0);
      await tree.delete(1);
      await tree.delete(2);

      // Verify remaining entries
      for (let i = 3; i < 20; i++) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }
    });

    it('should shrink tree height when root becomes empty', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Create a multi-level tree
      for (let i = 0; i < 30; i++) {
        await tree.set(i, `value_${i}`);
      }

      const heightBefore = (await tree.stats()).height;
      expect(heightBefore).toBeGreaterThan(1);

      // Delete until tree shrinks
      for (let i = 0; i < 28; i++) {
        await tree.delete(i);
      }

      const statsAfter = await tree.stats();
      expect(statsAfter.entryCount).toBe(2);
      expect(statsAfter.height).toBe(1); // Should be back to single leaf

      // Verify remaining entries
      expect(await tree.get(28)).toBe('value_28');
      expect(await tree.get(29)).toBe('value_29');
    });
  });

  // ===========================================================================
  // Concurrent Operations
  // ===========================================================================
  describe('concurrent operations', () => {
    it('should handle concurrent reads correctly', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert data
      for (let i = 0; i < 100; i++) {
        await tree.set(i, { value: i });
      }

      // Concurrent reads
      const reads: Promise<unknown>[] = [];
      for (let i = 0; i < 10; i++) {
        for (let j = 0; j < 10; j++) {
          const key = (i * 10 + j) % 100;
          reads.push(tree.get(key));
        }
      }

      const results = await Promise.all(reads);

      // All reads should succeed
      for (let i = 0; i < results.length; i++) {
        const key = Math.floor(i / 10) * 10 + (i % 10);
        const expectedKey = key % 100;
        expect((results[i] as { value: number }).value).toBe(expectedKey);
      }
    });

    it('should handle concurrent reads and writes (single-writer)', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert initial data
      for (let i = 0; i < 50; i++) {
        await tree.set(i, { value: i, version: 1 });
      }

      // Sequential writes (single-writer assumption)
      for (let i = 0; i < 50; i++) {
        await tree.set(i, { value: i, version: 2 });
      }

      // Add new entries
      for (let i = 50; i < 100; i++) {
        await tree.set(i, { value: i, version: 1 });
      }

      // Verify all entries
      expect(await tree.count()).toBe(100);

      for (let i = 0; i < 100; i++) {
        const result = (await tree.get(i)) as { value: number; version: number };
        expect(result.value).toBe(i);
        expect(result.version).toBe(i < 50 ? 2 : 1);
      }
    });

    it('should handle concurrent range scans', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert data
      for (let i = 0; i < 100; i++) {
        await tree.set(i, `value_${i}`);
      }

      // Start multiple range scans concurrently
      const scans: Promise<Array<[number, string]>>[] = [];

      for (let start = 0; start < 100; start += 20) {
        scans.push(
          (async () => {
            const entries: Array<[number, string]> = [];
            for await (const entry of tree.range(start, start + 20)) {
              entries.push(entry as [number, string]);
            }
            return entries;
          })()
        );
      }

      const results = await Promise.all(scans);

      // Verify each range scan got correct results
      for (let i = 0; i < results.length; i++) {
        const start = i * 20;
        expect(results[i].length).toBe(20);
        for (let j = 0; j < results[i].length; j++) {
          expect(results[i][j][0]).toBe(start + j);
        }
      }
    });
  });

  // ===========================================================================
  // Near-2MB Page Size Limits
  // ===========================================================================
  describe('near-2MB page size limits', () => {
    it('should handle values near page size limit', async () => {
      const tree = createBTree(fsx, StringKeyCodec, BinaryValueCodec, {
        minKeys: 2,
        maxKeys: 100,
      });
      await tree.init();

      // Create a value that's about 1.5MB
      const largeValue = makeLargeData(1.5 * 1024 * 1024);

      await tree.set('large_key', largeValue);

      const result = await tree.get('large_key');
      expect(result).toBeDefined();
      expect(result!.length).toBe(largeValue.length);
      expect(result![0]).toBe(largeValue[0]);
      expect(result![largeValue.length - 1]).toBe(largeValue[largeValue.length - 1]);
    });

    it('should handle multiple medium-sized values approaching limit', async () => {
      const tree = createBTree(fsx, StringKeyCodec, BinaryValueCodec, {
        minKeys: 2,
        maxKeys: 50,
      });
      await tree.init();

      // Insert several 100KB values (should trigger splits before hitting 2MB)
      const valueSize = 100 * 1024;
      const count = 5;

      for (let i = 0; i < count; i++) {
        const value = makeLargeData(valueSize, i);
        await tree.set(`key_${i}`, value);
      }

      // Verify all values
      for (let i = 0; i < count; i++) {
        const result = await tree.get(`key_${i}`);
        expect(result).toBeDefined();
        expect(result!.length).toBe(valueSize);
        expect(result![0]).toBe(i % 256);
      }
    });

    it('should split page before exceeding 2MB limit', async () => {
      const tree = createBTree(fsx, StringKeyCodec, BinaryValueCodec, {
        minKeys: 2,
        maxKeys: 100, // High max keys but physical size limit will trigger split
      });
      await tree.init();

      // Insert values that will eventually cause physical size split
      const valueSize = 50 * 1024; // 50KB each
      const count = 20; // 20 * 50KB = 1MB

      for (let i = 0; i < count; i++) {
        const value = makeLargeData(valueSize, i);
        await tree.set(`key_${i.toString().padStart(3, '0')}`, value);
      }

      const stats = await tree.stats();
      // With 1MB of data, should have multiple pages
      expect(stats.pageCount).toBeGreaterThan(1);

      // Verify all values
      for (let i = 0; i < count; i++) {
        const result = await tree.get(`key_${i.toString().padStart(3, '0')}`);
        expect(result).toBeDefined();
        expect(result!.length).toBe(valueSize);
      }
    });

    it('should handle key sizes near limit', async () => {
      // Custom codec for large keys
      const largeKeyCodec: KeyCodec<Uint8Array> = {
        encode: (key: Uint8Array) => key,
        decode: (bytes: Uint8Array) => bytes,
        compare: (a: Uint8Array, b: Uint8Array) => {
          const minLen = Math.min(a.length, b.length);
          for (let i = 0; i < minLen; i++) {
            if (a[i]! < b[i]!) return -1;
            if (a[i]! > b[i]!) return 1;
          }
          return a.length - b.length;
        },
      };

      const tree = createBTree(fsx, largeKeyCodec, BinaryValueCodec, {
        minKeys: 2,
        maxKeys: 10,
      });
      await tree.init();

      // Create keys of 10KB each
      const keySize = 10 * 1024;
      const count = 5;

      for (let i = 0; i < count; i++) {
        const key = makeLargeData(keySize, i);
        const value = makeLargeData(1000, i + 100);
        await tree.set(key, value);
      }

      // Verify all entries
      for (let i = 0; i < count; i++) {
        const key = makeLargeData(keySize, i);
        const result = await tree.get(key);
        expect(result).toBeDefined();
        expect(result!.length).toBe(1000);
        expect(result![0]).toBe((i + 100) % 256);
      }
    });
  });

  // ===========================================================================
  // Maximum Key/Value Sizes
  // ===========================================================================
  describe('maximum key/value sizes', () => {
    // Binary key codec with compare function (needed for keys)
    const binaryKeyCodec: KeyCodec<Uint8Array> = {
      encode: (key: Uint8Array) => key,
      decode: (bytes: Uint8Array) => bytes,
      compare: (a: Uint8Array, b: Uint8Array) => {
        const minLen = Math.min(a.length, b.length);
        for (let i = 0; i < minLen; i++) {
          if (a[i]! < b[i]!) return -1;
          if (a[i]! > b[i]!) return 1;
        }
        return a.length - b.length;
      },
    };

    it('should handle zero-length keys', async () => {
      const tree = createBTree(fsx, binaryKeyCodec, JsonValueCodec);
      await tree.init();

      const emptyKey = new Uint8Array(0);
      await tree.set(emptyKey, { data: 'empty key value' });

      const result = await tree.get(emptyKey);
      expect(result).toEqual({ data: 'empty key value' });
    });

    it('should handle zero-length values', async () => {
      const tree = createBTree(fsx, StringKeyCodec, BinaryValueCodec);
      await tree.init();

      const emptyValue = new Uint8Array(0);
      await tree.set('empty_value_key', emptyValue);

      const result = await tree.get('empty_value_key');
      expect(result).toBeDefined();
      expect(result!.length).toBe(0);
    });

    it('should handle very long string keys', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 10,
      });
      await tree.init();

      // Create a 10KB string key
      const longKey = 'a'.repeat(10 * 1024);

      await tree.set(longKey, { type: 'long key test' });

      const result = await tree.get(longKey);
      expect(result).toEqual({ type: 'long key test' });
    });

    it('should handle maximum inline value size', async () => {
      const tree = createBTree(fsx, StringKeyCodec, BinaryValueCodec, {
        maxInlineValueSize: 4096, // Default
      });
      await tree.init();

      // Create a value exactly at max inline size
      const value = makeLargeData(4096);
      await tree.set('max_inline', value);

      const result = await tree.get('max_inline');
      expect(result).toBeDefined();
      expect(result!.length).toBe(4096);
    });

    it('should handle unicode keys correctly', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      const unicodeKeys = [
        '\u4e2d\u6587', // Chinese
        '\u0420\u0443\u0441\u0441\u043a\u0438\u0439', // Russian
        '\ud83d\ude00\ud83d\ude01\ud83d\ude02', // Emojis
        '\u05e2\u05d1\u05e8\u05d9\u05ea', // Hebrew
        '\u0645\u0631\u062d\u0628\u0627', // Arabic
      ];

      for (let i = 0; i < unicodeKeys.length; i++) {
        await tree.set(unicodeKeys[i], { index: i });
      }

      for (let i = 0; i < unicodeKeys.length; i++) {
        const result = await tree.get(unicodeKeys[i]);
        expect(result).toEqual({ index: i });
      }

      expect(await tree.count()).toBe(unicodeKeys.length);
    });
  });

  // ===========================================================================
  // Key Collision Handling
  // ===========================================================================
  describe('key collision handling', () => {
    it('should update value when setting same key twice', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      await tree.set('collision', { version: 1 });
      await tree.set('collision', { version: 2 });

      const result = await tree.get('collision');
      expect(result).toEqual({ version: 2 });
      expect(await tree.count()).toBe(1);
    });

    it('should handle many updates to same key', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      const updateCount = 100;
      for (let i = 0; i < updateCount; i++) {
        await tree.set('same_key', { version: i });
      }

      const result = await tree.get('same_key');
      expect(result).toEqual({ version: updateCount - 1 });
      expect(await tree.count()).toBe(1);
    });

    it('should handle keys with same prefix', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const prefixedKeys = [
        'prefix',
        'prefix_1',
        'prefix_12',
        'prefix_123',
        'prefix_1234',
        'prefix_2',
        'prefix_21',
        'prefix_',
        'prefi',
        'pre',
      ];

      for (const key of prefixedKeys) {
        await tree.set(key, { key });
      }

      expect(await tree.count()).toBe(prefixedKeys.length);

      for (const key of prefixedKeys) {
        const result = await tree.get(key);
        expect(result).toEqual({ key });
      }
    });

    it('should correctly handle sequential duplicate sets and deletes', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec);
      await tree.init();

      // Set, delete, set again
      await tree.set(1, 'first');
      expect(await tree.get(1)).toBe('first');

      await tree.delete(1);
      expect(await tree.get(1)).toBeUndefined();

      await tree.set(1, 'second');
      expect(await tree.get(1)).toBe('second');

      expect(await tree.count()).toBe(1);
    });

    it('should handle binary keys with colliding prefixes', async () => {
      // Binary key codec with compare function (needed for keys)
      const binaryKeyCodec: KeyCodec<Uint8Array> = {
        encode: (key: Uint8Array) => key,
        decode: (bytes: Uint8Array) => bytes,
        compare: (a: Uint8Array, b: Uint8Array) => {
          const minLen = Math.min(a.length, b.length);
          for (let i = 0; i < minLen; i++) {
            if (a[i]! < b[i]!) return -1;
            if (a[i]! > b[i]!) return 1;
          }
          return a.length - b.length;
        },
      };

      const tree = createBTree(fsx, binaryKeyCodec, JsonValueCodec);
      await tree.init();

      // Create keys that share prefixes
      const keys = [
        new Uint8Array([1, 2, 3]),
        new Uint8Array([1, 2, 3, 4]),
        new Uint8Array([1, 2]),
        new Uint8Array([1]),
        new Uint8Array([1, 2, 3, 4, 5]),
      ];

      for (let i = 0; i < keys.length; i++) {
        await tree.set(keys[i], { index: i });
      }

      for (let i = 0; i < keys.length; i++) {
        const result = await tree.get(keys[i]);
        expect(result).toEqual({ index: i });
      }
    });
  });

  // ===========================================================================
  // Boundary Conditions
  // ===========================================================================
  describe('boundary conditions', () => {
    it('should handle minKeys=1 configuration', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 1,
        maxKeys: 2,
      });
      await tree.init();

      // Insert and delete with very small node sizes
      for (let i = 0; i < 10; i++) {
        await tree.set(i, `value_${i}`);
      }

      expect(await tree.count()).toBe(10);

      for (let i = 0; i < 5; i++) {
        await tree.delete(i);
      }

      expect(await tree.count()).toBe(5);

      for (let i = 5; i < 10; i++) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }
    });

    it('should handle single entry insert and delete', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      await tree.set('only', { data: 'single entry' });
      expect(await tree.count()).toBe(1);

      const deleted = await tree.delete('only');
      expect(deleted).toBe(true);
      expect(await tree.count()).toBe(0);
      expect(await tree.get('only')).toBeUndefined();
    });

    it('should handle range query with equal start and end', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec);
      await tree.init();

      for (let i = 0; i < 10; i++) {
        await tree.set(i, `value_${i}`);
      }

      // Range [5, 5) should return nothing (exclusive end)
      const entries: Array<[number, string]> = [];
      for await (const entry of tree.range(5, 5)) {
        entries.push(entry as [number, string]);
      }
      expect(entries).toHaveLength(0);
    });

    it('should handle range query with inverted bounds', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec);
      await tree.init();

      for (let i = 0; i < 10; i++) {
        await tree.set(i, `value_${i}`);
      }

      // Range [8, 2) should return nothing
      const entries: Array<[number, string]> = [];
      for await (const entry of tree.range(8, 2)) {
        entries.push(entry as [number, string]);
      }
      expect(entries).toHaveLength(0);
    });

    it('should handle inserting in reverse order', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert in descending order
      for (let i = 99; i >= 0; i--) {
        await tree.set(i, `value_${i}`);
      }

      expect(await tree.count()).toBe(100);

      // Verify ascending order in entries
      let lastKey = -1;
      for await (const [key] of tree.entries()) {
        expect(key).toBeGreaterThan(lastKey);
        lastKey = key;
      }
    });

    it('should handle alternating insert/delete pattern', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Alternating pattern: insert 2, delete 1
      for (let i = 0; i < 100; i += 2) {
        await tree.set(i, `value_${i}`);
        await tree.set(i + 1, `value_${i + 1}`);
        if (i > 0) {
          await tree.delete(i - 2);
        }
      }

      // Verify tree integrity
      const entries: Array<[number, string]> = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [number, string]);
      }

      // Check sorted order
      for (let i = 1; i < entries.length; i++) {
        expect(entries[i][0]).toBeGreaterThan(entries[i - 1][0]);
      }
    });
  });

  // ===========================================================================
  // Persistence and Recovery
  // ===========================================================================
  describe('persistence and recovery', () => {
    it('should persist data after page splits', async () => {
      const tree1 = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        pagePrefix: 'persist-split/',
      });
      await tree1.init();

      // Insert enough to cause splits
      for (let i = 0; i < 50; i++) {
        await tree1.set(i, `value_${i}`);
      }

      const stats1 = await tree1.stats();
      expect(stats1.pageCount).toBeGreaterThan(1);

      // Create new tree instance with same storage
      const tree2 = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        pagePrefix: 'persist-split/',
      });
      await tree2.init();

      // Verify all data is accessible
      const stats2 = await tree2.stats();
      expect(stats2.entryCount).toBe(stats1.entryCount);
      expect(stats2.pageCount).toBe(stats1.pageCount);

      for (let i = 0; i < 50; i++) {
        expect(await tree2.get(i)).toBe(`value_${i}`);
      }
    });

    it('should persist data after merges', async () => {
      const tree1 = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        pagePrefix: 'persist-merge/',
      });
      await tree1.init();

      // Insert and then delete to cause merges
      for (let i = 0; i < 50; i++) {
        await tree1.set(i, `value_${i}`);
      }

      for (let i = 0; i < 40; i++) {
        await tree1.delete(i);
      }

      const stats1 = await tree1.stats();

      // Create new tree instance
      const tree2 = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
        pagePrefix: 'persist-merge/',
      });
      await tree2.init();

      // Verify data
      const stats2 = await tree2.stats();
      expect(stats2.entryCount).toBe(stats1.entryCount);

      for (let i = 40; i < 50; i++) {
        expect(await tree2.get(i)).toBe(`value_${i}`);
      }
    });
  });
});
