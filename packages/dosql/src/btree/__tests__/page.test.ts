/**
 * B-tree Page Serialization Tests
 *
 * TDD tests for page serialization and deserialization in the B-tree module.
 * These tests verify:
 * - Page serialization roundtrip for leaf and internal pages
 * - Binary format correctness (magic number, version, headers)
 * - Key/value data integrity across serialization
 * - Edge cases: empty pages, max capacity, large data
 * - Binary search and byte comparison utilities
 */

import { describe, it, expect } from 'vitest';
import {
  serializePage,
  deserializePage,
  calculatePageSize,
  wouldFit,
  binarySearch,
  compareBytes,
  MAX_PAGE_SIZE,
} from '../page.js';
import {
  Page,
  PageType,
  createLeafPage,
  createInternalPage,
} from '../types.js';

/**
 * Helper to create a key as Uint8Array
 */
function makeKey(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

/**
 * Helper to create a value as Uint8Array
 */
function makeValue(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

/**
 * Helper to decode Uint8Array to string
 */
function decodeBytes(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}

/**
 * Helper to create a large Uint8Array of specified size
 */
function makeLargeData(size: number, seed = 0): Uint8Array {
  const data = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    data[i] = (i + seed) % 256;
  }
  return data;
}

describe('Page Serialization', () => {
  describe('leaf page roundtrip', () => {
    it('should serialize and deserialize an empty leaf page', () => {
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

    it('should serialize and deserialize a leaf page with single key-value', () => {
      const page = createLeafPage(1);
      page.keys.push(makeKey('hello'));
      page.values.push(makeValue('world'));

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.id).toBe(1);
      expect(deserialized.type).toBe(PageType.LEAF);
      expect(deserialized.keys).toHaveLength(1);
      expect(deserialized.values).toHaveLength(1);
      expect(decodeBytes(deserialized.keys[0])).toBe('hello');
      expect(decodeBytes(deserialized.values[0])).toBe('world');
    });

    it('should serialize and deserialize a leaf page with multiple key-values', () => {
      const page = createLeafPage(100);
      const entries = [
        { key: 'alpha', value: 'first' },
        { key: 'beta', value: 'second' },
        { key: 'gamma', value: 'third' },
        { key: 'delta', value: 'fourth' },
        { key: 'epsilon', value: 'fifth' },
      ];

      for (const entry of entries) {
        page.keys.push(makeKey(entry.key));
        page.values.push(makeValue(entry.value));
      }

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.keys).toHaveLength(entries.length);
      expect(deserialized.values).toHaveLength(entries.length);

      for (let i = 0; i < entries.length; i++) {
        expect(decodeBytes(deserialized.keys[i])).toBe(entries[i].key);
        expect(decodeBytes(deserialized.values[i])).toBe(entries[i].value);
      }
    });

    it('should preserve leaf chain pointers', () => {
      const page = createLeafPage(5);
      page.nextLeaf = 6;
      page.prevLeaf = 4;
      page.keys.push(makeKey('test'));
      page.values.push(makeValue('data'));

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.nextLeaf).toBe(6);
      expect(deserialized.prevLeaf).toBe(4);
    });

    it('should handle variable-length keys and values', () => {
      const page = createLeafPage(1);

      // Short key, long value
      page.keys.push(makeKey('k'));
      page.values.push(makeValue('a very long value that contains much more data'));

      // Long key, short value
      page.keys.push(makeKey('a very long key with lots of characters'));
      page.values.push(makeValue('v'));

      // Equal length
      page.keys.push(makeKey('medium_key'));
      page.values.push(makeValue('medium_val'));

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(decodeBytes(deserialized.keys[0])).toBe('k');
      expect(decodeBytes(deserialized.values[0])).toBe('a very long value that contains much more data');
      expect(decodeBytes(deserialized.keys[1])).toBe('a very long key with lots of characters');
      expect(decodeBytes(deserialized.values[1])).toBe('v');
      expect(decodeBytes(deserialized.keys[2])).toBe('medium_key');
      expect(decodeBytes(deserialized.values[2])).toBe('medium_val');
    });

    it('should handle binary data in keys and values', () => {
      const page = createLeafPage(1);

      // Binary key with null bytes and special characters
      const binaryKey = new Uint8Array([0, 1, 255, 128, 0, 100]);
      // Binary value with all byte values
      const binaryValue = new Uint8Array(256);
      for (let i = 0; i < 256; i++) {
        binaryValue[i] = i;
      }

      page.keys.push(binaryKey);
      page.values.push(binaryValue);

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.keys[0]).toEqual(binaryKey);
      expect(deserialized.values[0]).toEqual(binaryValue);
    });
  });

  describe('internal page roundtrip', () => {
    it('should serialize and deserialize an internal page with no keys', () => {
      // An internal page with 0 keys still has 1 child (n keys -> n+1 children)
      const page = createInternalPage(10);
      page.children.push(5); // Single child pointing to a leaf

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.id).toBe(10);
      expect(deserialized.type).toBe(PageType.INTERNAL);
      expect(deserialized.keys).toHaveLength(0);
      expect(deserialized.values).toHaveLength(0);
      expect(deserialized.children).toHaveLength(1);
      expect(deserialized.children[0]).toBe(5);
    });

    it('should serialize and deserialize an internal page with keys and children', () => {
      const page = createInternalPage(1);

      // For n keys, there are n+1 children
      page.keys.push(makeKey('key1'));
      page.keys.push(makeKey('key2'));
      page.keys.push(makeKey('key3'));

      page.children.push(10); // child < key1
      page.children.push(20); // key1 <= child < key2
      page.children.push(30); // key2 <= child < key3
      page.children.push(40); // child >= key3

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.type).toBe(PageType.INTERNAL);
      expect(deserialized.keys).toHaveLength(3);
      expect(deserialized.children).toHaveLength(4);

      expect(decodeBytes(deserialized.keys[0])).toBe('key1');
      expect(decodeBytes(deserialized.keys[1])).toBe('key2');
      expect(decodeBytes(deserialized.keys[2])).toBe('key3');

      expect(deserialized.children).toEqual([10, 20, 30, 40]);
    });

    it('should preserve internal page with single key and two children', () => {
      const page = createInternalPage(99);
      page.keys.push(makeKey('separator'));
      page.children.push(1);
      page.children.push(2);

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.keys).toHaveLength(1);
      expect(deserialized.children).toHaveLength(2);
      expect(decodeBytes(deserialized.keys[0])).toBe('separator');
      expect(deserialized.children[0]).toBe(1);
      expect(deserialized.children[1]).toBe(2);
    });

    it('should not have values array populated for internal pages', () => {
      const page = createInternalPage(1);
      page.keys.push(makeKey('key'));
      page.children.push(1);
      page.children.push(2);

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.values).toHaveLength(0);
    });
  });

  describe('binary format validation', () => {
    it('should reject data smaller than header size', () => {
      const tooSmall = new Uint8Array(16);

      expect(() => deserializePage(tooSmall)).toThrow('Page data too small');
    });

    it('should reject data with invalid magic number', () => {
      // Create a buffer with correct size but wrong magic
      const badMagic = new Uint8Array(100);
      const view = new DataView(badMagic.buffer);
      view.setUint32(0, 0x12345678, false); // Wrong magic
      view.setUint32(4, 1, false); // Version 1

      expect(() => deserializePage(badMagic)).toThrow('Invalid page magic');
    });

    it('should reject data with unsupported version', () => {
      const badVersion = new Uint8Array(100);
      const view = new DataView(badVersion.buffer);
      view.setUint32(0, 0x42545045, false); // Correct magic "BTPE"
      view.setUint32(4, 99, false); // Unsupported version

      expect(() => deserializePage(badVersion)).toThrow('Unsupported page version');
    });

    it('should contain correct magic number in serialized output', () => {
      const page = createLeafPage(1);
      const serialized = serializePage(page);
      const view = new DataView(serialized.buffer, serialized.byteOffset, serialized.byteLength);

      expect(view.getUint32(0, false)).toBe(0x42545045); // "BTPE"
    });

    it('should contain correct version in serialized output', () => {
      const page = createLeafPage(1);
      const serialized = serializePage(page);
      const view = new DataView(serialized.buffer, serialized.byteOffset, serialized.byteLength);

      expect(view.getUint32(4, false)).toBe(1);
    });
  });

  describe('calculatePageSize', () => {
    it('should calculate size of empty leaf page', () => {
      const page = createLeafPage(1);
      const size = calculatePageSize(page);

      // Header is 32 bytes, no keys/values
      expect(size).toBe(32);
    });

    it('should calculate size of internal page with single child', () => {
      // An internal page with 0 keys has 1 child (n keys -> n+1 children)
      const page = createInternalPage(1);
      page.children.push(5); // Single child
      const size = calculatePageSize(page);

      // Header (32) + children dir (4 bytes for 1 child)
      expect(size).toBe(32 + 4);
    });

    it('should calculate size of leaf page with data', () => {
      const page = createLeafPage(1);
      page.keys.push(makeKey('key1')); // 4 bytes
      page.values.push(makeValue('value1')); // 6 bytes

      const size = calculatePageSize(page);

      // Header (32) + key dir (8) + value dir (8) + key data (4) + value data (6)
      expect(size).toBe(32 + 8 + 8 + 4 + 6);
    });

    it('should calculate size of internal page with data', () => {
      const page = createInternalPage(1);
      page.keys.push(makeKey('key1')); // 4 bytes
      page.children.push(1);
      page.children.push(2);

      const size = calculatePageSize(page);

      // Header (32) + key dir (8) + children dir (8) + key data (4)
      expect(size).toBe(32 + 8 + 8 + 4);
    });

    it('should match actual serialized size', () => {
      const page = createLeafPage(1);
      for (let i = 0; i < 10; i++) {
        page.keys.push(makeKey(`key_${i}`));
        page.values.push(makeValue(`value_${i}_data`));
      }

      const calculatedSize = calculatePageSize(page);
      const serialized = serializePage(page);

      expect(calculatedSize).toBe(serialized.byteLength);
    });
  });

  describe('wouldFit', () => {
    it('should return true for small addition to empty page', () => {
      const page = createLeafPage(1);

      expect(wouldFit(page, 100, 100)).toBe(true);
    });

    it('should return false for addition exceeding max size', () => {
      const page = createLeafPage(1);

      // Try to add more than MAX_PAGE_SIZE
      expect(wouldFit(page, MAX_PAGE_SIZE, MAX_PAGE_SIZE)).toBe(false);
    });

    it('should account for existing page content', () => {
      const page = createLeafPage(1);

      // Add data that brings us close to the limit
      const largeData = makeLargeData(MAX_PAGE_SIZE - 100);
      page.keys.push(largeData);
      page.values.push(new Uint8Array(0));

      // Small addition should not fit
      expect(wouldFit(page, 100, 100)).toBe(false);
    });

    it('should handle internal page child pointer overhead', () => {
      const page = createInternalPage(1);

      // Fill up most of the space
      const largeKey = makeLargeData(MAX_PAGE_SIZE - 100);
      page.keys.push(largeKey);
      page.children.push(1);
      page.children.push(2);

      // Adding more should fail due to child pointer overhead
      expect(wouldFit(page, 50, 0)).toBe(false);
    });
  });

  describe('edge cases', () => {
    it('should handle page with many small keys', () => {
      const page = createLeafPage(1);
      const keyCount = 500;

      for (let i = 0; i < keyCount; i++) {
        page.keys.push(makeKey(`k${i}`));
        page.values.push(makeValue(`v${i}`));
      }

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.keys).toHaveLength(keyCount);
      expect(deserialized.values).toHaveLength(keyCount);

      // Spot check some values
      expect(decodeBytes(deserialized.keys[0])).toBe('k0');
      expect(decodeBytes(deserialized.keys[250])).toBe('k250');
      expect(decodeBytes(deserialized.keys[499])).toBe('k499');
    });

    it('should handle page with large keys and values', () => {
      const page = createLeafPage(1);

      // Add a few large keys and values
      for (let i = 0; i < 3; i++) {
        page.keys.push(makeLargeData(10000, i));
        page.values.push(makeLargeData(50000, i + 100));
      }

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.keys).toHaveLength(3);
      expect(deserialized.values).toHaveLength(3);

      for (let i = 0; i < 3; i++) {
        expect(deserialized.keys[i].byteLength).toBe(10000);
        expect(deserialized.values[i].byteLength).toBe(50000);
        expect(deserialized.keys[i][0]).toBe(i % 256);
        expect(deserialized.values[i][0]).toBe((i + 100) % 256);
      }
    });

    it('should throw when page exceeds max size', () => {
      const page = createLeafPage(1);

      // Add data that exceeds the limit
      page.keys.push(makeLargeData(MAX_PAGE_SIZE));
      page.values.push(makeLargeData(MAX_PAGE_SIZE));

      expect(() => serializePage(page)).toThrow(/Page size .* exceeds maximum/);
    });

    it('should handle page with zero-length keys', () => {
      const page = createLeafPage(1);
      page.keys.push(new Uint8Array(0));
      page.values.push(makeValue('value'));

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.keys[0].byteLength).toBe(0);
      expect(decodeBytes(deserialized.values[0])).toBe('value');
    });

    it('should handle page with zero-length values', () => {
      const page = createLeafPage(1);
      page.keys.push(makeKey('key'));
      page.values.push(new Uint8Array(0));

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(decodeBytes(deserialized.keys[0])).toBe('key');
      expect(deserialized.values[0].byteLength).toBe(0);
    });

    it('should handle maximum page ID', () => {
      const page = createLeafPage(0xFFFFFFFF);
      page.keys.push(makeKey('test'));
      page.values.push(makeValue('data'));

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.id).toBe(0xFFFFFFFF);
    });

    it('should handle negative leaf pointers correctly', () => {
      const page = createLeafPage(1);
      page.nextLeaf = -1;
      page.prevLeaf = -1;

      const serialized = serializePage(page);
      const deserialized = deserializePage(serialized);

      expect(deserialized.nextLeaf).toBe(-1);
      expect(deserialized.prevLeaf).toBe(-1);
    });
  });

  describe('page splitting scenarios', () => {
    it('should correctly serialize split left page', () => {
      // Simulate a page that was split - left half keeps original ID
      const leftPage = createLeafPage(1);
      leftPage.nextLeaf = 2; // Points to new right page
      leftPage.prevLeaf = 0;

      for (let i = 0; i < 5; i++) {
        leftPage.keys.push(makeKey(`key_${i.toString().padStart(2, '0')}`));
        leftPage.values.push(makeValue(`value_${i}`));
      }

      const serialized = serializePage(leftPage);
      const deserialized = deserializePage(serialized);

      expect(deserialized.nextLeaf).toBe(2);
      expect(deserialized.keys).toHaveLength(5);
    });

    it('should correctly serialize split right page', () => {
      // Simulate a page that was created from split - new page
      const rightPage = createLeafPage(2);
      rightPage.prevLeaf = 1; // Points back to original page
      rightPage.nextLeaf = 3;

      for (let i = 5; i < 10; i++) {
        rightPage.keys.push(makeKey(`key_${i.toString().padStart(2, '0')}`));
        rightPage.values.push(makeValue(`value_${i}`));
      }

      const serialized = serializePage(rightPage);
      const deserialized = deserializePage(serialized);

      expect(deserialized.prevLeaf).toBe(1);
      expect(deserialized.nextLeaf).toBe(3);
      expect(deserialized.keys).toHaveLength(5);
    });

    it('should correctly serialize merged page', () => {
      // Simulate a page after merge - combines keys from two pages
      const mergedPage = createLeafPage(1);
      mergedPage.nextLeaf = 3; // Skip over deleted page 2
      mergedPage.prevLeaf = 0;

      for (let i = 0; i < 10; i++) {
        mergedPage.keys.push(makeKey(`key_${i.toString().padStart(2, '0')}`));
        mergedPage.values.push(makeValue(`value_${i}`));
      }

      const serialized = serializePage(mergedPage);
      const deserialized = deserializePage(serialized);

      expect(deserialized.keys).toHaveLength(10);
      expect(deserialized.nextLeaf).toBe(3);
    });
  });
});

describe('Binary Search', () => {
  describe('binarySearch', () => {
    it('should find exact match at beginning', () => {
      const keys = [
        makeKey('apple'),
        makeKey('banana'),
        makeKey('cherry'),
      ];

      const result = binarySearch(keys, makeKey('apple'), compareBytes);

      expect(result.found).toBe(true);
      expect(result.index).toBe(0);
    });

    it('should find exact match in middle', () => {
      const keys = [
        makeKey('apple'),
        makeKey('banana'),
        makeKey('cherry'),
      ];

      const result = binarySearch(keys, makeKey('banana'), compareBytes);

      expect(result.found).toBe(true);
      expect(result.index).toBe(1);
    });

    it('should find exact match at end', () => {
      const keys = [
        makeKey('apple'),
        makeKey('banana'),
        makeKey('cherry'),
      ];

      const result = binarySearch(keys, makeKey('cherry'), compareBytes);

      expect(result.found).toBe(true);
      expect(result.index).toBe(2);
    });

    it('should return insertion point for missing key at beginning', () => {
      const keys = [
        makeKey('banana'),
        makeKey('cherry'),
        makeKey('date'),
      ];

      const result = binarySearch(keys, makeKey('apple'), compareBytes);

      expect(result.found).toBe(false);
      expect(result.index).toBe(0);
    });

    it('should return insertion point for missing key in middle', () => {
      const keys = [
        makeKey('apple'),
        makeKey('cherry'),
        makeKey('elderberry'),
      ];

      const result = binarySearch(keys, makeKey('banana'), compareBytes);

      expect(result.found).toBe(false);
      expect(result.index).toBe(1);
    });

    it('should return insertion point for missing key at end', () => {
      const keys = [
        makeKey('apple'),
        makeKey('banana'),
        makeKey('cherry'),
      ];

      const result = binarySearch(keys, makeKey('date'), compareBytes);

      expect(result.found).toBe(false);
      expect(result.index).toBe(3);
    });

    it('should handle empty array', () => {
      const keys: Uint8Array[] = [];

      const result = binarySearch(keys, makeKey('anything'), compareBytes);

      expect(result.found).toBe(false);
      expect(result.index).toBe(0);
    });

    it('should handle single element array - found', () => {
      const keys = [makeKey('only')];

      const result = binarySearch(keys, makeKey('only'), compareBytes);

      expect(result.found).toBe(true);
      expect(result.index).toBe(0);
    });

    it('should handle single element array - before', () => {
      const keys = [makeKey('middle')];

      const result = binarySearch(keys, makeKey('before'), compareBytes);

      expect(result.found).toBe(false);
      expect(result.index).toBe(0);
    });

    it('should handle single element array - after', () => {
      const keys = [makeKey('middle')];

      const result = binarySearch(keys, makeKey('zebra'), compareBytes);

      expect(result.found).toBe(false);
      expect(result.index).toBe(1);
    });

    it('should handle numeric byte comparison correctly', () => {
      // Test with numeric keys stored as big-endian bytes
      const keys = [
        new Uint8Array([0, 0, 0, 1]),
        new Uint8Array([0, 0, 0, 5]),
        new Uint8Array([0, 0, 0, 10]),
      ];

      const result = binarySearch(keys, new Uint8Array([0, 0, 0, 5]), compareBytes);

      expect(result.found).toBe(true);
      expect(result.index).toBe(1);
    });

    it('should handle large arrays efficiently', () => {
      const keys: Uint8Array[] = [];
      for (let i = 0; i < 1000; i++) {
        keys.push(makeKey(i.toString().padStart(4, '0')));
      }

      // Search for various keys
      const result1 = binarySearch(keys, makeKey('0500'), compareBytes);
      expect(result1.found).toBe(true);
      expect(result1.index).toBe(500);

      const result2 = binarySearch(keys, makeKey('0999'), compareBytes);
      expect(result2.found).toBe(true);
      expect(result2.index).toBe(999);

      const result3 = binarySearch(keys, makeKey('0000'), compareBytes);
      expect(result3.found).toBe(true);
      expect(result3.index).toBe(0);
    });
  });

  describe('compareBytes', () => {
    it('should return 0 for equal arrays', () => {
      const a = makeKey('test');
      const b = makeKey('test');

      expect(compareBytes(a, b)).toBe(0);
    });

    it('should return negative for lexicographically smaller first array', () => {
      const a = makeKey('apple');
      const b = makeKey('banana');

      expect(compareBytes(a, b)).toBeLessThan(0);
    });

    it('should return positive for lexicographically larger first array', () => {
      const a = makeKey('banana');
      const b = makeKey('apple');

      expect(compareBytes(a, b)).toBeGreaterThan(0);
    });

    it('should handle prefix comparison - shorter is smaller', () => {
      const a = makeKey('app');
      const b = makeKey('apple');

      expect(compareBytes(a, b)).toBeLessThan(0);
    });

    it('should handle prefix comparison - longer is larger', () => {
      const a = makeKey('apple');
      const b = makeKey('app');

      expect(compareBytes(a, b)).toBeGreaterThan(0);
    });

    it('should handle empty arrays', () => {
      const empty1 = new Uint8Array(0);
      const empty2 = new Uint8Array(0);
      const nonEmpty = makeKey('a');

      expect(compareBytes(empty1, empty2)).toBe(0);
      expect(compareBytes(empty1, nonEmpty)).toBeLessThan(0);
      expect(compareBytes(nonEmpty, empty1)).toBeGreaterThan(0);
    });

    it('should compare by byte value not character', () => {
      // 'Z' (90) < 'a' (97) in ASCII
      const upper = makeKey('Z');
      const lower = makeKey('a');

      expect(compareBytes(upper, lower)).toBeLessThan(0);
    });

    it('should handle high byte values', () => {
      const a = new Uint8Array([255]);
      const b = new Uint8Array([0]);

      expect(compareBytes(a, b)).toBeGreaterThan(0);
      expect(compareBytes(b, a)).toBeLessThan(0);
    });

    it('should compare byte by byte from left to right', () => {
      const a = new Uint8Array([1, 100]);
      const b = new Uint8Array([2, 0]);

      // First byte determines order
      expect(compareBytes(a, b)).toBeLessThan(0);
    });
  });
});

describe('Page Type Handling', () => {
  it('should correctly identify leaf page type after roundtrip', () => {
    const page = createLeafPage(1);
    page.keys.push(makeKey('test'));
    page.values.push(makeValue('data'));

    const serialized = serializePage(page);
    const deserialized = deserializePage(serialized);

    expect(deserialized.type).toBe(PageType.LEAF);
  });

  it('should correctly identify internal page type after roundtrip', () => {
    const page = createInternalPage(1);
    page.keys.push(makeKey('separator'));
    page.children.push(1);
    page.children.push(2);

    const serialized = serializePage(page);
    const deserialized = deserializePage(serialized);

    expect(deserialized.type).toBe(PageType.INTERNAL);
  });
});
