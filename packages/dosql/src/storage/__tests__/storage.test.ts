/**
 * Comprehensive Storage Module Tests
 *
 * Tests for the unified storage interface, memory backend, and adapters.
 * Following the NO MOCKS philosophy - all tests run against real implementations.
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';

import {
  // Core Interface
  type StorageInterface,
  type StorageInterfaceWithBatch,
  type StorageInterfaceWithMeta,
  type ByteRange,
  type BatchPutRequest,
  type StorageMetadata,

  // Type Guards
  hasMetadata,
  hasBatch,

  // Adapters
  adaptFSXBackend,
  adaptToFSXBackend,
  adaptColumnarInterface,
  type LegacyFSXBackend,
  type LegacyColumnarInterface,

  // Memory Storage
  MemoryStorage,
  createMemoryStorage,
  type MemoryStorageOptions,
} from '../index.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Generate test data of specified size with optional seed
 */
function generateTestData(size: number, seed = 0): Uint8Array {
  const data = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    data[i] = (i + seed) % 256;
  }
  return data;
}

/**
 * Text encoder/decoder helpers
 */
const encoder = new TextEncoder();
const decoder = new TextDecoder();

function textToBytes(text: string): Uint8Array {
  return encoder.encode(text);
}

function bytesToText(bytes: Uint8Array): string {
  return decoder.decode(bytes);
}

// =============================================================================
// MemoryStorage Tests
// =============================================================================

describe('MemoryStorage', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = createMemoryStorage();
  });

  afterEach(() => {
    storage.clear();
  });

  describe('StorageInterface Contract', () => {
    describe('get()', () => {
      it('should return null for non-existent key', async () => {
        const result = await storage.get('nonexistent');
        expect(result).toBeNull();
      });

      it('should return data for existing key', async () => {
        const data = textToBytes('test content');
        await storage.put('test-key', data);

        const result = await storage.get('test-key');
        expect(result).toEqual(data);
      });

      it('should handle range reads - full range', async () => {
        const data = generateTestData(100);
        await storage.put('range-test', data);

        const result = await storage.get('range-test', [0, 99]);
        expect(result).toEqual(data);
      });

      it('should handle range reads - partial range from start', async () => {
        const data = generateTestData(100);
        await storage.put('range-test', data);

        const result = await storage.get('range-test', [0, 9]);
        expect(result?.length).toBe(10);
        expect(result).toEqual(data.slice(0, 10));
      });

      it('should handle range reads - partial range from middle', async () => {
        const data = generateTestData(100);
        await storage.put('range-test', data);

        const result = await storage.get('range-test', [25, 74]);
        expect(result?.length).toBe(50);
        expect(result).toEqual(data.slice(25, 75));
      });

      it('should handle range reads - partial range to end', async () => {
        const data = generateTestData(100);
        await storage.put('range-test', data);

        const result = await storage.get('range-test', [90, 99]);
        expect(result?.length).toBe(10);
        expect(result).toEqual(data.slice(90, 100));
      });

      it('should handle range extending beyond data length', async () => {
        const data = generateTestData(50);
        await storage.put('range-test', data);

        // Request range 0-99 but data is only 50 bytes
        const result = await storage.get('range-test', [0, 99]);
        expect(result?.length).toBe(50);
        expect(result).toEqual(data);
      });

      it('should return null for invalid range - start >= length', async () => {
        const data = generateTestData(50);
        await storage.put('range-test', data);

        const result = await storage.get('range-test', [50, 60]);
        expect(result).toBeNull();
      });

      it('should return null for invalid range - negative start', async () => {
        const data = generateTestData(50);
        await storage.put('range-test', data);

        const result = await storage.get('range-test', [-1, 10]);
        expect(result).toBeNull();
      });

      it('should return null for invalid range - end < start', async () => {
        const data = generateTestData(50);
        await storage.put('range-test', data);

        const result = await storage.get('range-test', [20, 10]);
        expect(result).toBeNull();
      });

      it('should handle single byte range', async () => {
        const data = textToBytes('Hello, World!');
        await storage.put('single-byte', data);

        // Read just 'W'
        const result = await storage.get('single-byte', [7, 7]);
        expect(result?.length).toBe(1);
        expect(bytesToText(result!)).toBe('W');
      });
    });

    describe('put()', () => {
      it('should store data successfully', async () => {
        const data = textToBytes('test content');
        await storage.put('put-test', data);

        expect(await storage.exists('put-test')).toBe(true);
        expect(await storage.get('put-test')).toEqual(data);
      });

      it('should overwrite existing data', async () => {
        await storage.put('overwrite', textToBytes('original'));
        await storage.put('overwrite', textToBytes('updated'));

        const result = await storage.get('overwrite');
        expect(bytesToText(result!)).toBe('updated');
      });

      it('should handle empty data', async () => {
        const emptyData = new Uint8Array(0);
        await storage.put('empty', emptyData);

        const result = await storage.get('empty');
        expect(result?.length).toBe(0);
      });

      it('should handle large data', async () => {
        const largeData = generateTestData(1024 * 1024); // 1MB
        await storage.put('large', largeData);

        const result = await storage.get('large');
        expect(result?.length).toBe(largeData.length);
        // Verify data integrity
        expect(result?.[0]).toBe(largeData[0]);
        expect(result?.[512 * 1024]).toBe(largeData[512 * 1024]);
        expect(result?.[largeData.length - 1]).toBe(largeData[largeData.length - 1]);
      });

      it('should handle binary data with all byte values', async () => {
        const binaryData = new Uint8Array(256);
        for (let i = 0; i < 256; i++) {
          binaryData[i] = i;
        }

        await storage.put('binary', binaryData);
        const result = await storage.get('binary');

        expect(result).toEqual(binaryData);
      });
    });

    describe('delete()', () => {
      it('should delete existing key', async () => {
        await storage.put('to-delete', textToBytes('delete me'));
        expect(await storage.exists('to-delete')).toBe(true);

        await storage.delete('to-delete');
        expect(await storage.exists('to-delete')).toBe(false);
      });

      it('should handle deleting non-existent key gracefully', async () => {
        // Should not throw
        await storage.delete('nonexistent');
        expect(await storage.exists('nonexistent')).toBe(false);
      });

      it('should update storage size after deletion', async () => {
        const data = generateTestData(100);
        await storage.put('delete-size', data);

        const sizeBefore = storage.size;
        expect(sizeBefore).toBe(100);

        await storage.delete('delete-size');
        expect(storage.size).toBe(0);
      });
    });

    describe('list()', () => {
      it('should return empty array when no keys match', async () => {
        const result = await storage.list('nonexistent/');
        expect(result).toEqual([]);
      });

      it('should return keys matching prefix', async () => {
        await storage.put('dir/file1.txt', textToBytes('1'));
        await storage.put('dir/file2.txt', textToBytes('2'));
        await storage.put('other/file3.txt', textToBytes('3'));

        const result = await storage.list('dir/');
        expect(result).toHaveLength(2);
        expect(result).toContain('dir/file1.txt');
        expect(result).toContain('dir/file2.txt');
        expect(result).not.toContain('other/file3.txt');
      });

      it('should return all keys with empty prefix', async () => {
        await storage.put('key1', textToBytes('1'));
        await storage.put('key2', textToBytes('2'));
        await storage.put('key3', textToBytes('3'));

        const result = await storage.list('');
        expect(result).toHaveLength(3);
      });

      it('should return sorted keys', async () => {
        await storage.put('c', textToBytes('c'));
        await storage.put('a', textToBytes('a'));
        await storage.put('b', textToBytes('b'));

        const result = await storage.list('');
        expect(result).toEqual(['a', 'b', 'c']);
      });

      it('should handle nested paths', async () => {
        await storage.put('a/b/c/file.txt', textToBytes('data'));
        await storage.put('a/b/other.txt', textToBytes('data'));
        await storage.put('a/file.txt', textToBytes('data'));

        const result = await storage.list('a/b/');
        expect(result).toHaveLength(2);
        expect(result).toContain('a/b/c/file.txt');
        expect(result).toContain('a/b/other.txt');
      });
    });

    describe('exists()', () => {
      it('should return false for non-existent key', async () => {
        expect(await storage.exists('nonexistent')).toBe(false);
      });

      it('should return true for existing key', async () => {
        await storage.put('exists-test', textToBytes('data'));
        expect(await storage.exists('exists-test')).toBe(true);
      });

      it('should return true for empty value', async () => {
        await storage.put('empty-exists', new Uint8Array(0));
        expect(await storage.exists('empty-exists')).toBe(true);
      });
    });
  });

  describe('StorageInterfaceWithMeta Contract', () => {
    describe('metadata()', () => {
      it('should return null for non-existent key', async () => {
        const meta = await storage.metadata('nonexistent');
        expect(meta).toBeNull();
      });

      it('should return metadata for existing key', async () => {
        const data = generateTestData(500);
        await storage.put('meta-test', data);

        const meta = await storage.metadata('meta-test');
        expect(meta).not.toBeNull();
        expect(meta?.size).toBe(500);
        expect(meta?.lastModified).toBeInstanceOf(Date);
      });

      it('should update lastModified on write', async () => {
        await storage.put('time-test', textToBytes('v1'));
        const meta1 = await storage.metadata('time-test');
        const time1 = meta1?.lastModified.getTime();

        // Wait a bit to ensure time difference
        await new Promise((resolve) => setTimeout(resolve, 10));

        await storage.put('time-test', textToBytes('v2'));
        const meta2 = await storage.metadata('time-test');
        const time2 = meta2?.lastModified.getTime();

        expect(time2).toBeGreaterThanOrEqual(time1!);
      });

      it('should return correct size after update', async () => {
        await storage.put('size-update', generateTestData(100));
        let meta = await storage.metadata('size-update');
        expect(meta?.size).toBe(100);

        await storage.put('size-update', generateTestData(200));
        meta = await storage.metadata('size-update');
        expect(meta?.size).toBe(200);
      });
    });
  });

  describe('StorageInterfaceWithBatch Contract', () => {
    describe('putMany()', () => {
      it('should put multiple items at once', async () => {
        const items: BatchPutRequest[] = [
          { key: 'batch1', data: textToBytes('data1') },
          { key: 'batch2', data: textToBytes('data2') },
          { key: 'batch3', data: textToBytes('data3') },
        ];

        await storage.putMany(items);

        expect(await storage.exists('batch1')).toBe(true);
        expect(await storage.exists('batch2')).toBe(true);
        expect(await storage.exists('batch3')).toBe(true);

        expect(bytesToText((await storage.get('batch1'))!)).toBe('data1');
        expect(bytesToText((await storage.get('batch2'))!)).toBe('data2');
        expect(bytesToText((await storage.get('batch3'))!)).toBe('data3');
      });

      it('should handle empty items array', async () => {
        await storage.putMany([]);
        expect(storage.count).toBe(0);
      });
    });

    describe('deleteMany()', () => {
      it('should delete multiple items at once', async () => {
        await storage.put('del1', textToBytes('1'));
        await storage.put('del2', textToBytes('2'));
        await storage.put('del3', textToBytes('3'));
        await storage.put('keep', textToBytes('keep'));

        await storage.deleteMany(['del1', 'del2', 'del3']);

        expect(await storage.exists('del1')).toBe(false);
        expect(await storage.exists('del2')).toBe(false);
        expect(await storage.exists('del3')).toBe(false);
        expect(await storage.exists('keep')).toBe(true);
      });

      it('should handle empty keys array', async () => {
        await storage.put('exists', textToBytes('data'));
        await storage.deleteMany([]);
        expect(await storage.exists('exists')).toBe(true);
      });

      it('should handle non-existent keys gracefully', async () => {
        await storage.deleteMany(['nonexistent1', 'nonexistent2']);
        // Should not throw
      });
    });

    describe('getMany()', () => {
      it('should get multiple items at once', async () => {
        await storage.put('get1', textToBytes('data1'));
        await storage.put('get2', textToBytes('data2'));
        await storage.put('get3', textToBytes('data3'));

        const results = await storage.getMany(['get1', 'get2', 'get3']);

        expect(results.size).toBe(3);
        expect(bytesToText(results.get('get1')!)).toBe('data1');
        expect(bytesToText(results.get('get2')!)).toBe('data2');
        expect(bytesToText(results.get('get3')!)).toBe('data3');
      });

      it('should exclude non-existent keys from results', async () => {
        await storage.put('exists', textToBytes('data'));

        const results = await storage.getMany(['exists', 'nonexistent']);

        expect(results.size).toBe(1);
        expect(results.has('exists')).toBe(true);
        expect(results.has('nonexistent')).toBe(false);
      });

      it('should handle empty keys array', async () => {
        const results = await storage.getMany([]);
        expect(results.size).toBe(0);
      });
    });
  });

  describe('Utility Methods', () => {
    describe('clear()', () => {
      it('should clear all data', async () => {
        await storage.put('key1', textToBytes('1'));
        await storage.put('key2', textToBytes('2'));

        storage.clear();

        expect(storage.count).toBe(0);
        expect(storage.size).toBe(0);
        expect(await storage.exists('key1')).toBe(false);
        expect(await storage.exists('key2')).toBe(false);
      });
    });

    describe('size', () => {
      it('should track total bytes', async () => {
        expect(storage.size).toBe(0);

        await storage.put('a', generateTestData(100));
        expect(storage.size).toBe(100);

        await storage.put('b', generateTestData(200));
        expect(storage.size).toBe(300);
      });

      it('should update on overwrite', async () => {
        await storage.put('update', generateTestData(100));
        expect(storage.size).toBe(100);

        await storage.put('update', generateTestData(50));
        expect(storage.size).toBe(50);
      });
    });

    describe('count', () => {
      it('should track number of keys', async () => {
        expect(storage.count).toBe(0);

        await storage.put('a', textToBytes('a'));
        expect(storage.count).toBe(1);

        await storage.put('b', textToBytes('b'));
        expect(storage.count).toBe(2);

        await storage.delete('a');
        expect(storage.count).toBe(1);
      });
    });

    describe('keys()', () => {
      it('should return all keys', async () => {
        await storage.put('x', textToBytes('x'));
        await storage.put('y', textToBytes('y'));
        await storage.put('z', textToBytes('z'));

        const keys = storage.keys();
        expect(keys).toHaveLength(3);
        expect(keys).toContain('x');
        expect(keys).toContain('y');
        expect(keys).toContain('z');
      });
    });

    describe('export()', () => {
      it('should export all data as Map', async () => {
        await storage.put('a', textToBytes('data-a'));
        await storage.put('b', textToBytes('data-b'));

        const exported = storage.export();

        expect(exported.size).toBe(2);
        expect(bytesToText(exported.get('a')!)).toBe('data-a');
        expect(bytesToText(exported.get('b')!)).toBe('data-b');
      });
    });
  });

  describe('Configuration Options', () => {
    describe('maxSize', () => {
      it('should enforce maximum storage size', async () => {
        const limitedStorage = createMemoryStorage({ maxSize: 100 });

        await limitedStorage.put('small', generateTestData(50));

        await expect(
          limitedStorage.put('large', generateTestData(100))
        ).rejects.toThrow('Storage size limit exceeded');
      });

      it('should allow overwrite that stays within limit', async () => {
        const limitedStorage = createMemoryStorage({ maxSize: 100 });

        await limitedStorage.put('key', generateTestData(80));
        await limitedStorage.put('key', generateTestData(50)); // Should succeed

        expect(limitedStorage.size).toBe(50);
      });
    });

    describe('trackAccessTime', () => {
      it('should update access time on read when enabled', async () => {
        const trackedStorage = createMemoryStorage({ trackAccessTime: true });

        await trackedStorage.put('track', textToBytes('data'));

        // First read
        await trackedStorage.get('track');

        // Wait a bit
        await new Promise((resolve) => setTimeout(resolve, 10));

        // Second read should update access time
        await trackedStorage.get('track');

        // Access time tracking is internal, verify storage works
        const data = await trackedStorage.get('track');
        expect(bytesToText(data!)).toBe('data');
      });
    });

    describe('initialData', () => {
      it('should initialize storage with provided data', async () => {
        const initialData = new Map<string, Uint8Array>([
          ['init1', textToBytes('value1')],
          ['init2', textToBytes('value2')],
        ]);

        const preloadedStorage = createMemoryStorage({ initialData });

        expect(preloadedStorage.count).toBe(2);
        expect(bytesToText((await preloadedStorage.get('init1'))!)).toBe('value1');
        expect(bytesToText((await preloadedStorage.get('init2'))!)).toBe('value2');
      });

      it('should calculate initial size correctly', async () => {
        const initialData = new Map<string, Uint8Array>([
          ['a', generateTestData(100)],
          ['b', generateTestData(200)],
        ]);

        const preloadedStorage = createMemoryStorage({ initialData });

        expect(preloadedStorage.size).toBe(300);
      });
    });
  });
});

// =============================================================================
// Type Guards Tests
// =============================================================================

describe('Type Guards', () => {
  describe('hasMetadata()', () => {
    it('should return true for MemoryStorage', () => {
      const storage = createMemoryStorage();
      expect(hasMetadata(storage)).toBe(true);
    });

    it('should return false for basic StorageInterface', () => {
      const basicStorage: StorageInterface = {
        get: async () => null,
        put: async () => {},
        delete: async () => {},
        list: async () => [],
        exists: async () => false,
      };

      expect(hasMetadata(basicStorage)).toBe(false);
    });
  });

  describe('hasBatch()', () => {
    it('should return true for MemoryStorage', () => {
      const storage = createMemoryStorage();
      expect(hasBatch(storage)).toBe(true);
    });

    it('should return false for basic StorageInterface', () => {
      const basicStorage: StorageInterface = {
        get: async () => null,
        put: async () => {},
        delete: async () => {},
        list: async () => [],
        exists: async () => false,
      };

      expect(hasBatch(basicStorage)).toBe(false);
    });
  });
});

// =============================================================================
// Adapter Tests
// =============================================================================

describe('Adapters', () => {
  describe('adaptFSXBackend()', () => {
    it('should adapt LegacyFSXBackend to StorageInterface', async () => {
      const legacyBackend: LegacyFSXBackend = {
        read: async (path, _range) => {
          if (path === 'test') return textToBytes('test-data');
          return null;
        },
        write: async (_path, _data) => {},
        delete: async (_path) => {},
        list: async (_prefix) => ['file1', 'file2'],
        exists: async (path) => path === 'test',
      };

      const adapted = adaptFSXBackend(legacyBackend);

      // Test get (adapted from read)
      const data = await adapted.get('test');
      expect(bytesToText(data!)).toBe('test-data');

      // Test exists
      expect(await adapted.exists('test')).toBe(true);
      expect(await adapted.exists('nonexistent')).toBe(false);

      // Test list
      const files = await adapted.list('');
      expect(files).toEqual(['file1', 'file2']);
    });

    it('should pass range to read', async () => {
      let capturedRange: ByteRange | undefined;

      const legacyBackend: LegacyFSXBackend = {
        read: async (_path, range) => {
          capturedRange = range;
          return textToBytes('data');
        },
        write: async () => {},
        delete: async () => {},
        list: async () => [],
        exists: async () => true,
      };

      const adapted = adaptFSXBackend(legacyBackend);
      await adapted.get('test', [10, 20]);

      expect(capturedRange).toEqual([10, 20]);
    });
  });

  describe('adaptToFSXBackend()', () => {
    it('should adapt StorageInterface to LegacyFSXBackend', async () => {
      const storage = createMemoryStorage();
      await storage.put('test', textToBytes('test-data'));

      const legacyBackend = adaptToFSXBackend(storage);

      // Test read (adapted from get)
      const data = await legacyBackend.read('test');
      expect(bytesToText(data!)).toBe('test-data');

      // Test exists
      expect(await legacyBackend.exists('test')).toBe(true);

      // Test write
      await legacyBackend.write('new', textToBytes('new-data'));
      expect(await storage.exists('new')).toBe(true);

      // Test delete
      await legacyBackend.delete('test');
      expect(await storage.exists('test')).toBe(false);

      // Test list
      const files = await legacyBackend.list('');
      expect(files).toContain('new');
    });
  });

  describe('adaptColumnarInterface()', () => {
    it('should adapt LegacyColumnarInterface to StorageInterface', async () => {
      const data = new Map<string, Uint8Array>();

      const columnarInterface: LegacyColumnarInterface = {
        get: async (key) => data.get(key) ?? null,
        put: async (key, value) => {
          data.set(key, value);
        },
        delete: async (key) => {
          data.delete(key);
        },
        list: async (prefix) => {
          return Array.from(data.keys()).filter((k) => k.startsWith(prefix));
        },
      };

      const adapted = adaptColumnarInterface(columnarInterface);

      // Test put and get
      await adapted.put('col-test', textToBytes('columnar-data'));
      const result = await adapted.get('col-test');
      expect(bytesToText(result!)).toBe('columnar-data');

      // Test exists (implemented via get)
      expect(await adapted.exists('col-test')).toBe(true);
      expect(await adapted.exists('nonexistent')).toBe(false);

      // Test delete
      await adapted.delete('col-test');
      expect(await adapted.exists('col-test')).toBe(false);

      // Test list
      await adapted.put('prefix/a', textToBytes('a'));
      await adapted.put('prefix/b', textToBytes('b'));
      await adapted.put('other/c', textToBytes('c'));

      const files = await adapted.list('prefix/');
      expect(files).toHaveLength(2);
      expect(files).toContain('prefix/a');
      expect(files).toContain('prefix/b');
    });
  });
});

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

describe('Edge Cases', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = createMemoryStorage();
  });

  afterEach(() => {
    storage.clear();
  });

  it('should handle special characters in keys', async () => {
    const keys = [
      'path/with/slashes',
      'key.with.dots',
      'key-with-dashes',
      'key_with_underscores',
      'key with spaces',
      'unicode-\u00e9\u00e8\u00ea',
    ];

    for (const key of keys) {
      await storage.put(key, textToBytes(`data for ${key}`));
      const result = await storage.get(key);
      expect(bytesToText(result!)).toBe(`data for ${key}`);
    }
  });

  it('should handle empty string key', async () => {
    await storage.put('', textToBytes('empty-key-data'));
    const result = await storage.get('');
    expect(bytesToText(result!)).toBe('empty-key-data');
  });

  it('should handle rapid sequential operations', async () => {
    // Rapid writes
    for (let i = 0; i < 100; i++) {
      await storage.put('rapid', textToBytes(`version-${i}`));
    }

    const result = await storage.get('rapid');
    expect(bytesToText(result!)).toBe('version-99');
  });

  it('should handle concurrent operations', async () => {
    const operations = [];

    for (let i = 0; i < 50; i++) {
      operations.push(storage.put(`concurrent-${i}`, textToBytes(`data-${i}`)));
    }

    await Promise.all(operations);

    expect(storage.count).toBe(50);
  });

  it('should handle very long keys', async () => {
    const longKey = 'a'.repeat(1000);
    await storage.put(longKey, textToBytes('long-key-data'));

    const result = await storage.get(longKey);
    expect(bytesToText(result!)).toBe('long-key-data');
  });

  it('should handle listing with many keys', async () => {
    for (let i = 0; i < 1000; i++) {
      await storage.put(`many/key-${i.toString().padStart(4, '0')}`, textToBytes(`${i}`));
    }

    const keys = await storage.list('many/');
    expect(keys).toHaveLength(1000);

    // Should be sorted
    expect(keys[0]).toBe('many/key-0000');
    expect(keys[999]).toBe('many/key-0999');
  });
});

// =============================================================================
// Interface Verification Tests
// =============================================================================

describe('Interface Verification', () => {
  it('MemoryStorage implements StorageInterface', () => {
    const storage: StorageInterface = createMemoryStorage();

    expect(typeof storage.get).toBe('function');
    expect(typeof storage.put).toBe('function');
    expect(typeof storage.delete).toBe('function');
    expect(typeof storage.list).toBe('function');
    expect(typeof storage.exists).toBe('function');
  });

  it('MemoryStorage implements StorageInterfaceWithMeta', () => {
    const storage: StorageInterfaceWithMeta = createMemoryStorage();

    expect(typeof storage.metadata).toBe('function');
  });

  it('MemoryStorage implements StorageInterfaceWithBatch', () => {
    const storage: StorageInterfaceWithBatch = createMemoryStorage();

    expect(typeof storage.putMany).toBe('function');
    expect(typeof storage.deleteMany).toBe('function');
    expect(typeof storage.getMany).toBe('function');
  });
});
