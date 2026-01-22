/**
 * FSX Storage Abstraction Tests
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  MemoryFSXBackend,
  createMemoryBackend,
  DOStorageBackend,
  createDOBackend,
  R2StorageBackend,
  createR2Backend,
  TieredStorageBackend,
  createTieredBackend,
  FSXError,
  FSXErrorCode,
  StorageTier,
  DEFAULT_CHUNK_CONFIG,
  type DurableObjectStorageLike,
  type R2BucketLike,
  type R2ObjectLike,
} from './index.js';

// =============================================================================
// Mock Implementations
// =============================================================================

/**
 * Mock DurableObjectStorage for testing
 */
class MockDOStorage implements DurableObjectStorageLike {
  private store = new Map<string, unknown>();

  async get<T = unknown>(keyOrKeys: string | string[]): Promise<T | Map<string, T>> {
    if (Array.isArray(keyOrKeys)) {
      const result = new Map<string, T>();
      for (const key of keyOrKeys) {
        const value = this.store.get(key);
        if (value !== undefined) {
          result.set(key, value as T);
        }
      }
      return result as Map<string, T>;
    }
    return this.store.get(keyOrKeys) as T;
  }

  async put(keyOrEntries: string | Record<string, unknown>, value?: unknown): Promise<void> {
    if (typeof keyOrEntries === 'string') {
      this.store.set(keyOrEntries, value);
    } else {
      for (const [k, v] of Object.entries(keyOrEntries)) {
        this.store.set(k, v);
      }
    }
  }

  async delete(keyOrKeys: string | string[]): Promise<boolean | number> {
    if (Array.isArray(keyOrKeys)) {
      let count = 0;
      for (const key of keyOrKeys) {
        if (this.store.delete(key)) count++;
      }
      return count;
    }
    return this.store.delete(keyOrKeys);
  }

  async list(options?: { prefix?: string }): Promise<Map<string, unknown>> {
    const result = new Map<string, unknown>();
    for (const [key, value] of this.store.entries()) {
      if (!options?.prefix || key.startsWith(options.prefix)) {
        result.set(key, value);
      }
    }
    return result;
  }

  clear(): void {
    this.store.clear();
  }
}

/**
 * Mock R2Bucket for testing
 */
class MockR2Bucket implements R2BucketLike {
  private store = new Map<string, { data: ArrayBuffer; metadata: R2ObjectLike }>();

  async get(key: string, options?: { range?: { offset?: number; length?: number } }): Promise<R2ObjectLike | null> {
    const entry = this.store.get(key);
    if (!entry) return null;

    let data = entry.data;
    if (options?.range) {
      const { offset = 0, length } = options.range;
      const end = length ? offset + length : data.byteLength;
      data = data.slice(offset, end);
    }

    return {
      ...entry.metadata,
      arrayBuffer: async () => data,
      text: async () => new TextDecoder().decode(data),
      json: async <T>() => JSON.parse(new TextDecoder().decode(data)) as T,
      blob: async () => new Blob([data]),
      writeHttpMetadata: () => {},
    };
  }

  async put(key: string, value: ArrayBuffer | Uint8Array | string, options?: { customMetadata?: Record<string, string> }): Promise<R2ObjectLike> {
    let buffer: ArrayBuffer;
    if (typeof value === 'string') {
      buffer = new TextEncoder().encode(value).buffer as ArrayBuffer;
    } else if (value instanceof ArrayBuffer) {
      buffer = value;
    } else {
      buffer = value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength);
    }

    const metadata: R2ObjectLike = {
      key,
      size: buffer.byteLength,
      etag: `"${Date.now().toString(16)}"`,
      httpEtag: `"${Date.now().toString(16)}"`,
      uploaded: new Date(),
      customMetadata: options?.customMetadata,
      arrayBuffer: async () => buffer,
      text: async () => new TextDecoder().decode(buffer),
      json: async <T>() => JSON.parse(new TextDecoder().decode(buffer)) as T,
      blob: async () => new Blob([buffer]),
      writeHttpMetadata: () => {},
    };

    this.store.set(key, { data: buffer, metadata });
    return metadata;
  }

  async delete(keys: string | string[]): Promise<void> {
    const keyArray = Array.isArray(keys) ? keys : [keys];
    for (const key of keyArray) {
      this.store.delete(key);
    }
  }

  async list(options?: { prefix?: string; cursor?: string; limit?: number }): Promise<{ objects: R2ObjectLike[]; truncated: boolean; cursor?: string }> {
    const objects: R2ObjectLike[] = [];
    for (const [key, entry] of this.store.entries()) {
      if (!options?.prefix || key.startsWith(options.prefix)) {
        objects.push(entry.metadata);
      }
    }
    return { objects, truncated: false };
  }

  async head(key: string): Promise<R2ObjectLike | null> {
    const entry = this.store.get(key);
    return entry ? entry.metadata : null;
  }

  clear(): void {
    this.store.clear();
  }
}

// =============================================================================
// Memory Backend Tests
// =============================================================================

describe('MemoryFSXBackend', () => {
  let backend: MemoryFSXBackend;

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  it('should write and read data', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    await backend.write('test/file.bin', data);

    const result = await backend.read('test/file.bin');
    expect(result).toEqual(data);
  });

  it('should return null for non-existent files', async () => {
    const result = await backend.read('nonexistent');
    expect(result).toBeNull();
  });

  it('should support range reads', async () => {
    const data = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    await backend.write('test/file.bin', data);

    const result = await backend.read('test/file.bin', [2, 5]);
    expect(result).toEqual(new Uint8Array([2, 3, 4, 5]));
  });

  it('should throw on invalid range', async () => {
    const data = new Uint8Array([0, 1, 2, 3, 4]);
    await backend.write('test/file.bin', data);

    await expect(backend.read('test/file.bin', [3, 10])).rejects.toThrow(FSXError);
  });

  it('should delete files', async () => {
    await backend.write('test/file.bin', new Uint8Array([1, 2, 3]));
    expect(await backend.exists('test/file.bin')).toBe(true);

    await backend.delete('test/file.bin');
    expect(await backend.exists('test/file.bin')).toBe(false);
  });

  it('should list files with prefix', async () => {
    await backend.write('a/file1.bin', new Uint8Array([1]));
    await backend.write('a/file2.bin', new Uint8Array([2]));
    await backend.write('b/file3.bin', new Uint8Array([3]));

    const aFiles = await backend.list('a/');
    expect(aFiles).toEqual(['a/file1.bin', 'a/file2.bin']);

    const allFiles = await backend.list('');
    expect(allFiles.length).toBe(3);
  });

  it('should return metadata', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    await backend.write('test/file.bin', data);

    const meta = await backend.metadata('test/file.bin');
    expect(meta).not.toBeNull();
    expect(meta?.size).toBe(5);
    expect(meta?.lastModified).toBeInstanceOf(Date);
  });
});

// =============================================================================
// DO Backend Tests
// =============================================================================

describe('DOStorageBackend', () => {
  let storage: MockDOStorage;
  let backend: DOStorageBackend;

  beforeEach(() => {
    storage = new MockDOStorage();
    backend = createDOBackend(storage);
  });

  it('should write and read small files inline', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    await backend.write('test/small.bin', data);

    const result = await backend.read('test/small.bin');
    expect(result).toEqual(data);
  });

  it('should chunk large files', async () => {
    // Create data larger than chunk size
    const chunkSize = DEFAULT_CHUNK_CONFIG.maxChunkSize;
    const data = new Uint8Array(chunkSize + 1000);
    for (let i = 0; i < data.length; i++) {
      data[i] = i % 256;
    }

    await backend.write('test/large.bin', data);
    const result = await backend.read('test/large.bin');

    expect(result).toEqual(data);
  });

  it('should support range reads on chunked files', async () => {
    const chunkSize = DEFAULT_CHUNK_CONFIG.maxChunkSize;
    const data = new Uint8Array(chunkSize + 1000);
    for (let i = 0; i < data.length; i++) {
      data[i] = i % 256;
    }

    await backend.write('test/large.bin', data);

    // Read across chunk boundary
    const start = chunkSize - 100;
    const end = chunkSize + 100;
    const result = await backend.read('test/large.bin', [start, end]);

    expect(result?.length).toBe(end - start + 1);
    expect(result).toEqual(data.slice(start, end + 1));
  });

  it('should list files', async () => {
    await backend.write('dir/file1.bin', new Uint8Array([1]));
    await backend.write('dir/file2.bin', new Uint8Array([2]));
    await backend.write('other/file3.bin', new Uint8Array([3]));

    const dirFiles = await backend.list('dir/');
    expect(dirFiles).toEqual(['dir/file1.bin', 'dir/file2.bin']);
  });

  it('should delete files and their chunks', async () => {
    const chunkSize = DEFAULT_CHUNK_CONFIG.maxChunkSize;
    const data = new Uint8Array(chunkSize + 1000);

    await backend.write('test/large.bin', data);
    expect(await backend.exists('test/large.bin')).toBe(true);

    await backend.delete('test/large.bin');
    expect(await backend.exists('test/large.bin')).toBe(false);
  });

  it('should return stats', async () => {
    await backend.write('file1.bin', new Uint8Array(100));
    await backend.write('file2.bin', new Uint8Array(200));

    const stats = await backend.getStats();
    expect(stats.fileCount).toBe(2);
    expect(stats.totalSize).toBe(300);
  });
});

// =============================================================================
// R2 Backend Tests
// =============================================================================

describe('R2StorageBackend', () => {
  let bucket: MockR2Bucket;
  let backend: R2StorageBackend;

  beforeEach(() => {
    bucket = new MockR2Bucket();
    backend = createR2Backend(bucket);
  });

  it('should write and read data', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    await backend.write('test/file.bin', data);

    const result = await backend.read('test/file.bin');
    expect(result).toEqual(data);
  });

  it('should support range reads', async () => {
    const data = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    await backend.write('test/file.bin', data);

    const result = await backend.read('test/file.bin', [2, 5]);
    expect(result).toEqual(new Uint8Array([2, 3, 4, 5]));
  });

  it('should work with key prefix', async () => {
    const prefixedBackend = createR2Backend(bucket, { keyPrefix: 'myprefix' });

    await prefixedBackend.write('file.bin', new Uint8Array([1, 2, 3]));

    // Should store with prefix
    expect(await bucket.head('myprefix/file.bin')).not.toBeNull();

    // Should read correctly
    const result = await prefixedBackend.read('file.bin');
    expect(result).toEqual(new Uint8Array([1, 2, 3]));
  });

  it('should delete files', async () => {
    await backend.write('test.bin', new Uint8Array([1]));
    expect(await backend.exists('test.bin')).toBe(true);

    await backend.delete('test.bin');
    expect(await backend.exists('test.bin')).toBe(false);
  });

  it('should list files', async () => {
    await backend.write('dir/a.bin', new Uint8Array([1]));
    await backend.write('dir/b.bin', new Uint8Array([2]));
    await backend.write('other/c.bin', new Uint8Array([3]));

    const files = await backend.list('dir/');
    expect(files).toEqual(['dir/a.bin', 'dir/b.bin']);
  });

  it('should return metadata', async () => {
    await backend.write('test.bin', new Uint8Array([1, 2, 3, 4, 5]));

    const meta = await backend.metadata('test.bin');
    expect(meta?.size).toBe(5);
    expect(meta?.etag).toBeDefined();
  });
});

// =============================================================================
// Tiered Backend Tests
// =============================================================================

describe('TieredStorageBackend', () => {
  let doStorage: MockDOStorage;
  let r2Bucket: MockR2Bucket;
  let hotBackend: DOStorageBackend;
  let coldBackend: R2StorageBackend;
  let tiered: TieredStorageBackend;

  beforeEach(() => {
    doStorage = new MockDOStorage();
    r2Bucket = new MockR2Bucket();
    hotBackend = createDOBackend(doStorage);
    coldBackend = createR2Backend(r2Bucket);
    tiered = createTieredBackend(hotBackend, coldBackend, {
      maxHotFileSize: 1024 * 1024, // 1MB for testing
      autoMigrate: false, // Disable auto-migration for predictable tests
    });
  });

  it('should write small files to hot storage', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    await tiered.write('small.bin', data);

    // Should be in hot storage
    expect(await hotBackend.exists('small.bin')).toBe(true);
    // Should NOT be in cold storage
    expect(await coldBackend.exists('small.bin')).toBe(false);
  });

  it('should write large files directly to cold storage', async () => {
    // Create data larger than maxHotFileSize
    const data = new Uint8Array(2 * 1024 * 1024); // 2MB
    await tiered.write('large.bin', data);

    // Should NOT be in hot storage
    expect(await hotBackend.exists('large.bin')).toBe(false);
    // Should be in cold storage
    expect(await coldBackend.exists('large.bin')).toBe(true);
  });

  it('should read from hot storage first', async () => {
    const data = new Uint8Array([1, 2, 3]);
    await hotBackend.write('test.bin', data);

    const result = await tiered.read('test.bin');
    expect(result).toEqual(data);
  });

  it('should fallback to cold storage', async () => {
    const data = new Uint8Array([4, 5, 6]);
    await coldBackend.write('cold.bin', data);

    const result = await tiered.read('cold.bin');
    expect(result).toEqual(data);
  });

  it('should delete from both tiers', async () => {
    const data = new Uint8Array([1, 2, 3]);

    // Put in both storages
    await hotBackend.write('both.bin', data);
    await coldBackend.write('both.bin', data);

    await tiered.delete('both.bin');

    expect(await hotBackend.exists('both.bin')).toBe(false);
    expect(await coldBackend.exists('both.bin')).toBe(false);
  });

  it('should list files from both tiers', async () => {
    await hotBackend.write('hot.bin', new Uint8Array([1]));
    await coldBackend.write('cold.bin', new Uint8Array([2]));

    const files = await tiered.list('');
    // Filter out index entries
    const dataFiles = files.filter(f => !f.startsWith('_tier_index/'));
    expect(dataFiles.sort()).toEqual(['cold.bin', 'hot.bin']);
  });

  it('should return tiered metadata', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    await tiered.write('test.bin', data);

    const meta = await tiered.metadata('test.bin');
    expect(meta).not.toBeNull();
    expect(meta?.tier).toBe(StorageTier.HOT);
    expect(meta?.size).toBe(5);
  });

  it('should migrate data to cold storage', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    await tiered.write('migrate.bin', data);

    // Force migration
    const result = await tiered.migrateToR2({
      olderThan: 0, // Migrate everything
      deleteFromHot: true,
    });

    expect(result.migrated).toContain('migrate.bin');
    expect(await hotBackend.exists('migrate.bin')).toBe(false);
    expect(await coldBackend.exists('migrate.bin')).toBe(true);
  });

  it('should promote data back to hot storage', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    await coldBackend.write('promote.bin', data);

    const result = await tiered.promoteToHot(['promote.bin']);

    expect(result.migrated).toContain('promote.bin');
    expect(await hotBackend.exists('promote.bin')).toBe(true);
  });
});

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('FSXError', () => {
  it('should create error with correct properties', () => {
    const error = new FSXError(
      FSXErrorCode.NOT_FOUND,
      'File not found',
      '/path/to/file',
      new Error('underlying error')
    );

    expect(error.code).toBe(FSXErrorCode.NOT_FOUND);
    expect(error.message).toBe('File not found');
    expect(error.path).toBe('/path/to/file');
    expect(error.cause).toBeInstanceOf(Error);
    expect(error.name).toBe('FSXError');
  });
});
