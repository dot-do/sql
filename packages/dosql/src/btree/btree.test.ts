/**
 * B-tree Unit Tests
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MemoryFSXBackend } from '../fsx/index.js';
import {
  createBTree,
  StringKeyCodec,
  NumberKeyCodec,
  JsonValueCodec,
  BinaryValueCodec,
  serializePage,
  deserializePage,
  calculatePageSize,
  binarySearch,
  compareBytes,
  PageType,
  createLeafPage,
  createInternalPage,
} from './index.js';

// =============================================================================
// Page Serialization Tests
// =============================================================================

describe('Page Serialization', () => {
  describe('serializePage / deserializePage', () => {
    it('should round-trip an empty leaf page', () => {
      const page = createLeafPage(42);
      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.id).toBe(42);
      expect(deserialized.type).toBe(PageType.LEAF);
      expect(deserialized.keys).toHaveLength(0);
      expect(deserialized.values).toHaveLength(0);
      expect(deserialized.children).toHaveLength(0);
      expect(deserialized.nextLeaf).toBe(-1);
      expect(deserialized.prevLeaf).toBe(-1);
    });

    it('should round-trip a leaf page with data', () => {
      const page = createLeafPage(1);
      page.keys = [
        new TextEncoder().encode('key1'),
        new TextEncoder().encode('key2'),
        new TextEncoder().encode('key3'),
      ];
      page.values = [
        new TextEncoder().encode('value1'),
        new TextEncoder().encode('value2'),
        new TextEncoder().encode('value3'),
      ];
      page.nextLeaf = 5;
      page.prevLeaf = 0;

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.id).toBe(1);
      expect(deserialized.type).toBe(PageType.LEAF);
      expect(deserialized.keys).toHaveLength(3);
      expect(deserialized.values).toHaveLength(3);
      expect(deserialized.nextLeaf).toBe(5);
      expect(deserialized.prevLeaf).toBe(0);

      // Verify key contents
      expect(new TextDecoder().decode(deserialized.keys[0])).toBe('key1');
      expect(new TextDecoder().decode(deserialized.keys[1])).toBe('key2');
      expect(new TextDecoder().decode(deserialized.keys[2])).toBe('key3');

      // Verify value contents
      expect(new TextDecoder().decode(deserialized.values[0])).toBe('value1');
      expect(new TextDecoder().decode(deserialized.values[1])).toBe('value2');
      expect(new TextDecoder().decode(deserialized.values[2])).toBe('value3');
    });

    it('should round-trip an internal page with children', () => {
      const page = createInternalPage(10);
      page.keys = [
        new TextEncoder().encode('b'),
        new TextEncoder().encode('d'),
      ];
      page.children = [1, 2, 3]; // 3 children for 2 keys

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.id).toBe(10);
      expect(deserialized.type).toBe(PageType.INTERNAL);
      expect(deserialized.keys).toHaveLength(2);
      expect(deserialized.children).toEqual([1, 2, 3]);
      expect(deserialized.values).toHaveLength(0);
    });

    it('should handle binary data correctly', () => {
      const page = createLeafPage(1);
      // Binary data with null bytes and high values
      page.keys = [new Uint8Array([0, 1, 2, 255, 254, 253])];
      page.values = [new Uint8Array([128, 0, 0, 0, 128, 255])];

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.keys[0]).toEqual(new Uint8Array([0, 1, 2, 255, 254, 253]));
      expect(deserialized.values[0]).toEqual(new Uint8Array([128, 0, 0, 0, 128, 255]));
    });
  });

  describe('calculatePageSize', () => {
    it('should calculate size for empty leaf page', () => {
      const page = createLeafPage(1);
      const size = calculatePageSize(page);
      expect(size).toBe(32); // Just the header
    });

    it('should calculate size correctly for page with data', () => {
      const page = createLeafPage(1);
      page.keys = [new Uint8Array(100)];
      page.values = [new Uint8Array(200)];

      const size = calculatePageSize(page);
      // Header (32) + key dir (8) + value dir (8) + key data (100) + value data (200)
      expect(size).toBe(32 + 8 + 8 + 100 + 200);
    });
  });

  describe('binarySearch', () => {
    const encoder = new TextEncoder();
    const keys = ['apple', 'banana', 'cherry', 'date', 'elderberry'].map((s) =>
      encoder.encode(s)
    );

    it('should find existing keys', () => {
      const result = binarySearch(keys, encoder.encode('cherry'), compareBytes);
      expect(result.found).toBe(true);
      expect(result.index).toBe(2);
    });

    it('should find first key', () => {
      const result = binarySearch(keys, encoder.encode('apple'), compareBytes);
      expect(result.found).toBe(true);
      expect(result.index).toBe(0);
    });

    it('should find last key', () => {
      const result = binarySearch(keys, encoder.encode('elderberry'), compareBytes);
      expect(result.found).toBe(true);
      expect(result.index).toBe(4);
    });

    it('should return insertion point for missing key (middle)', () => {
      const result = binarySearch(keys, encoder.encode('coconut'), compareBytes);
      expect(result.found).toBe(false);
      expect(result.index).toBe(3); // Should insert between cherry and date
    });

    it('should return insertion point for missing key (before all)', () => {
      const result = binarySearch(keys, encoder.encode('aardvark'), compareBytes);
      expect(result.found).toBe(false);
      expect(result.index).toBe(0);
    });

    it('should return insertion point for missing key (after all)', () => {
      const result = binarySearch(keys, encoder.encode('zebra'), compareBytes);
      expect(result.found).toBe(false);
      expect(result.index).toBe(5);
    });

    it('should handle empty array', () => {
      const result = binarySearch([], encoder.encode('anything'), compareBytes);
      expect(result.found).toBe(false);
      expect(result.index).toBe(0);
    });
  });

  describe('compareBytes', () => {
    it('should compare equal arrays as equal', () => {
      const a = new Uint8Array([1, 2, 3]);
      const b = new Uint8Array([1, 2, 3]);
      expect(compareBytes(a, b)).toBe(0);
    });

    it('should compare lexicographically', () => {
      const a = new Uint8Array([1, 2, 3]);
      const b = new Uint8Array([1, 2, 4]);
      expect(compareBytes(a, b)).toBeLessThan(0);
      expect(compareBytes(b, a)).toBeGreaterThan(0);
    });

    it('should handle different lengths', () => {
      const a = new Uint8Array([1, 2]);
      const b = new Uint8Array([1, 2, 3]);
      expect(compareBytes(a, b)).toBeLessThan(0);
      expect(compareBytes(b, a)).toBeGreaterThan(0);
    });
  });
});

// =============================================================================
// Key/Value Codec Tests
// =============================================================================

describe('Codecs', () => {
  describe('StringKeyCodec', () => {
    it('should encode and decode strings', () => {
      const original = 'hello world';
      const encoded = StringKeyCodec.encode(original);
      const decoded = StringKeyCodec.decode(encoded);
      expect(decoded).toBe(original);
    });

    it('should compare strings correctly', () => {
      expect(StringKeyCodec.compare('apple', 'banana')).toBeLessThan(0);
      expect(StringKeyCodec.compare('banana', 'apple')).toBeGreaterThan(0);
      expect(StringKeyCodec.compare('apple', 'apple')).toBe(0);
    });

    it('should handle unicode', () => {
      const original = 'hello \u4e16\u754c \ud83d\ude00';
      const encoded = StringKeyCodec.encode(original);
      const decoded = StringKeyCodec.decode(encoded);
      expect(decoded).toBe(original);
    });
  });

  describe('NumberKeyCodec', () => {
    it('should encode and decode numbers', () => {
      const testNumbers = [0, 1, -1, 42, 3.14159, -273.15, Number.MAX_VALUE, Number.MIN_VALUE];
      for (const num of testNumbers) {
        const encoded = NumberKeyCodec.encode(num);
        const decoded = NumberKeyCodec.decode(encoded);
        expect(decoded).toBe(num);
      }
    });

    it('should compare numbers correctly', () => {
      expect(NumberKeyCodec.compare(1, 2)).toBeLessThan(0);
      expect(NumberKeyCodec.compare(2, 1)).toBeGreaterThan(0);
      expect(NumberKeyCodec.compare(5, 5)).toBe(0);
    });
  });

  describe('JsonValueCodec', () => {
    it('should encode and decode objects', () => {
      const original = { name: 'Alice', age: 30, active: true };
      const encoded = JsonValueCodec.encode(original);
      const decoded = JsonValueCodec.decode(encoded);
      expect(decoded).toEqual(original);
    });

    it('should encode and decode arrays', () => {
      const original = [1, 'two', { three: 3 }];
      const encoded = JsonValueCodec.encode(original);
      const decoded = JsonValueCodec.decode(encoded);
      expect(decoded).toEqual(original);
    });

    it('should handle null and primitives', () => {
      expect(JsonValueCodec.decode(JsonValueCodec.encode(null))).toBe(null);
      expect(JsonValueCodec.decode(JsonValueCodec.encode(42))).toBe(42);
      expect(JsonValueCodec.decode(JsonValueCodec.encode('hello'))).toBe('hello');
      expect(JsonValueCodec.decode(JsonValueCodec.encode(true))).toBe(true);
    });
  });
});

// =============================================================================
// B-tree Integration Tests
// =============================================================================

describe('BTree', () => {
  let fsx: MemoryFSXBackend;

  beforeEach(() => {
    fsx = new MemoryFSXBackend();
  });

  describe('basic operations', () => {
    it('should initialize an empty tree', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      const count = await tree.count();
      expect(count).toBe(0);
    });

    it('should set and get a single value', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      await tree.set('key1', { value: 'hello' });
      const result = await tree.get('key1');
      expect(result).toEqual({ value: 'hello' });
    });

    it('should return undefined for missing key', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      const result = await tree.get('nonexistent');
      expect(result).toBeUndefined();
    });

    it('should update existing key', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      await tree.set('key1', { version: 1 });
      await tree.set('key1', { version: 2 });

      const result = await tree.get('key1');
      expect(result).toEqual({ version: 2 });

      const count = await tree.count();
      expect(count).toBe(1);
    });

    it('should delete a key', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      await tree.set('key1', 'value1');
      const deleted = await tree.delete('key1');
      expect(deleted).toBe(true);

      const result = await tree.get('key1');
      expect(result).toBeUndefined();

      const count = await tree.count();
      expect(count).toBe(0);
    });

    it('should return false when deleting nonexistent key', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      const deleted = await tree.delete('nonexistent');
      expect(deleted).toBe(false);
    });
  });

  describe('multiple entries', () => {
    it('should handle multiple entries', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      await tree.set('a', 1);
      await tree.set('b', 2);
      await tree.set('c', 3);
      await tree.set('d', 4);
      await tree.set('e', 5);

      expect(await tree.get('a')).toBe(1);
      expect(await tree.get('b')).toBe(2);
      expect(await tree.get('c')).toBe(3);
      expect(await tree.get('d')).toBe(4);
      expect(await tree.get('e')).toBe(5);
      expect(await tree.count()).toBe(5);
    });

    it('should maintain sorted order', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      // Insert in random order
      await tree.set('delta', 4);
      await tree.set('alpha', 1);
      await tree.set('echo', 5);
      await tree.set('bravo', 2);
      await tree.set('charlie', 3);

      // Verify all entries in sorted order
      const entries: [string, number][] = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [string, number]);
      }

      expect(entries).toEqual([
        ['alpha', 1],
        ['bravo', 2],
        ['charlie', 3],
        ['delta', 4],
        ['echo', 5],
      ]);
    });
  });

  describe('range queries', () => {
    it('should iterate over a range', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      await tree.set('a', 1);
      await tree.set('b', 2);
      await tree.set('c', 3);
      await tree.set('d', 4);
      await tree.set('e', 5);

      const entries: [string, number][] = [];
      for await (const entry of tree.range('b', 'e')) {
        entries.push(entry as [string, number]);
      }

      expect(entries).toEqual([
        ['b', 2],
        ['c', 3],
        ['d', 4],
      ]);
    });

    it('should handle empty range', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      await tree.set('a', 1);
      await tree.set('z', 26);

      const entries: [string, number][] = [];
      for await (const entry of tree.range('m', 'n')) {
        entries.push(entry as [string, number]);
      }

      expect(entries).toHaveLength(0);
    });

    it('should handle range starting before first key', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      await tree.set('b', 2);
      await tree.set('c', 3);
      await tree.set('d', 4);

      const entries: [string, number][] = [];
      for await (const entry of tree.range('a', 'c')) {
        entries.push(entry as [string, number]);
      }

      expect(entries).toEqual([['b', 2]]);
    });
  });

  describe('clear', () => {
    it('should remove all entries', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
      await tree.init();

      await tree.set('a', 1);
      await tree.set('b', 2);
      await tree.set('c', 3);

      await tree.clear();

      expect(await tree.count()).toBe(0);
      expect(await tree.get('a')).toBeUndefined();
      expect(await tree.get('b')).toBeUndefined();
      expect(await tree.get('c')).toBeUndefined();
    });
  });

  describe('numeric keys', () => {
    it('should work with number keys', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec);
      await tree.init();

      await tree.set(100, 'hundred');
      await tree.set(1, 'one');
      await tree.set(50, 'fifty');
      await tree.set(25, 'twenty-five');
      await tree.set(75, 'seventy-five');

      expect(await tree.get(50)).toBe('fifty');
      expect(await tree.get(999)).toBeUndefined();

      const entries: [number, string][] = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [number, string]);
      }

      expect(entries.map(([k]) => k)).toEqual([1, 25, 50, 75, 100]);
    });
  });

  describe('binary values', () => {
    it('should work with binary values', async () => {
      const tree = createBTree(fsx, StringKeyCodec, BinaryValueCodec);
      await tree.init();

      const data = new Uint8Array([0, 1, 2, 128, 255]);
      await tree.set('binary', data);

      const result = await tree.get('binary');
      expect(result).toEqual(data);
    });
  });

  describe('persistence', () => {
    it('should persist data across tree instances', async () => {
      // Create and populate tree
      const tree1 = createBTree(fsx, StringKeyCodec, JsonValueCodec, {
        pagePrefix: 'test/',
      });
      await tree1.init();
      await tree1.set('key1', 'value1');
      await tree1.set('key2', 'value2');

      // Create new tree instance with same storage
      const tree2 = createBTree(fsx, StringKeyCodec, JsonValueCodec, {
        pagePrefix: 'test/',
      });
      await tree2.init();

      expect(await tree2.get('key1')).toBe('value1');
      expect(await tree2.get('key2')).toBe('value2');
      expect(await tree2.count()).toBe(2);
    });
  });

  describe('page splits', () => {
    it('should handle page splits with many entries', async () => {
      // Use smaller maxKeys to force splits
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert enough entries to cause splits
      const count = 50;
      for (let i = 0; i < count; i++) {
        const key = `key_${i.toString().padStart(3, '0')}`;
        await tree.set(key, i);
      }

      // Verify all entries are present
      expect(await tree.count()).toBe(count);

      for (let i = 0; i < count; i++) {
        const key = `key_${i.toString().padStart(3, '0')}`;
        expect(await tree.get(key)).toBe(i);
      }

      // Verify sorted order
      const entries: [string, number][] = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [string, number]);
      }

      expect(entries).toHaveLength(count);
      for (let i = 0; i < count; i++) {
        expect(entries[i][0]).toBe(`key_${i.toString().padStart(3, '0')}`);
        expect(entries[i][1]).toBe(i);
      }
    });

    it('should handle random insertion order with splits', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert in random order
      const numbers = Array.from({ length: 100 }, (_, i) => i);
      // Simple shuffle
      for (let i = numbers.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [numbers[i], numbers[j]] = [numbers[j], numbers[i]];
      }

      for (const n of numbers) {
        await tree.set(n, `value_${n}`);
      }

      // Verify all entries
      expect(await tree.count()).toBe(100);

      // Verify sorted order
      const entries: [number, string][] = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [number, string]);
      }

      expect(entries).toHaveLength(100);
      for (let i = 0; i < 100; i++) {
        expect(entries[i][0]).toBe(i);
        expect(entries[i][1]).toBe(`value_${i}`);
      }
    });
  });

  describe('stats', () => {
    it('should report tree statistics', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      let stats = await tree.stats();
      expect(stats.height).toBe(1);
      expect(stats.entryCount).toBe(0);
      expect(stats.pageCount).toBe(1);

      // Add entries to force splits
      for (let i = 0; i < 20; i++) {
        await tree.set(`key${i}`, i);
      }

      stats = await tree.stats();
      expect(stats.entryCount).toBe(20);
      expect(stats.height).toBeGreaterThan(1); // Should have grown
      expect(stats.pageCount).toBeGreaterThan(1); // Should have multiple pages
    });
  });
});
