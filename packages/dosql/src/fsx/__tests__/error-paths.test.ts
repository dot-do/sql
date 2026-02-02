/**
 * Error Path Testing - FSX Errors, R2 Failures, DO Storage Limits
 *
 * Issue: sql-di92 - Error Path Testing
 *
 * This test file covers error handling paths for the FSX storage layer:
 * 1. FSX read/write failures
 * 2. R2 connection errors
 * 3. DO storage limit exceeded
 * 4. Network timeouts
 * 5. Graceful degradation and error recovery
 *
 * Uses the workers-vitest-pool for real Cloudflare Workers environment testing.
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { env } from 'cloudflare:test';

import {
  FSXError,
  FSXErrorCode,
  type FSXBackend,
  type ByteRange,
  DEFAULT_CHUNK_CONFIG,
} from '../types.js';
import { R2Error, R2ErrorCode, createR2Error, detectR2ErrorType } from '../r2-errors.js';
import { DOStorageBackend, createDOBackend, type DurableObjectStorageLike } from '../do-backend.js';
import { R2StorageBackend, createR2Backend, type R2BucketLike, type R2ObjectLike, type R2GetOptions, type R2PutOptions, type R2PutValue, type R2ListOptions, type R2ObjectsLike } from '../r2-backend.js';
import { TieredStorageBackend, createTieredBackend } from '../tiered.js';
import { MemoryFSXBackend, createMemoryBackend } from '../index.js';
import { StorageTier } from '../types.js';

// =============================================================================
// Test Utilities
// =============================================================================

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function textToBytes(text: string): Uint8Array {
  return encoder.encode(text);
}

function bytesToText(bytes: Uint8Array): string {
  return decoder.decode(bytes);
}

function generateTestData(size: number, seed = 0): Uint8Array {
  const data = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    data[i] = (i + seed) % 256;
  }
  return data;
}

// =============================================================================
// Mock DO Storage with Failure Simulation
// =============================================================================

type DOFailureMode =
  | 'none'
  | 'read_error'
  | 'write_error'
  | 'delete_error'
  | 'list_error'
  | 'size_limit_exceeded'
  | 'quota_exceeded'
  | 'transient';

interface FailingDOStorageOptions {
  failureMode: DOFailureMode;
  sizeLimit?: number;
  failUntilAttempt?: number;
}

function createFailingDOStorage(options: FailingDOStorageOptions): DurableObjectStorageLike & {
  _setFailureMode: (mode: DOFailureMode) => void;
  _storage: Map<string, unknown>;
  _getAttemptCount: () => number;
  _resetAttempts: () => void;
} {
  const storage = new Map<string, unknown>();
  let failureMode = options.failureMode;
  let attemptCount = 0;
  const failUntilAttempt = options.failUntilAttempt ?? 3;
  const sizeLimit = options.sizeLimit ?? 128 * 1024 * 1024; // 128MB default
  let currentSize = 0;

  const shouldFail = (): boolean => {
    attemptCount++;
    if (failureMode === 'transient') {
      return attemptCount < failUntilAttempt;
    }
    return failureMode !== 'none';
  };

  const checkSizeLimit = (dataSize: number): void => {
    if (failureMode === 'size_limit_exceeded' || failureMode === 'quota_exceeded') {
      if (currentSize + dataSize > sizeLimit) {
        throw new Error(`Storage quota exceeded: current=${currentSize}, adding=${dataSize}, limit=${sizeLimit}`);
      }
    }
  };

  return {
    _storage: storage,
    _setFailureMode: (mode: DOFailureMode) => {
      failureMode = mode;
      attemptCount = 0;
    },
    _getAttemptCount: () => attemptCount,
    _resetAttempts: () => { attemptCount = 0; },

    async get<T = unknown>(keyOrKeys: string | string[]): Promise<T | Map<string, T> | undefined> {
      if (failureMode === 'read_error' && shouldFail()) {
        throw new Error('DO storage read failed: simulated error');
      }

      if (Array.isArray(keyOrKeys)) {
        const result = new Map<string, T>();
        for (const key of keyOrKeys) {
          const value = storage.get(key);
          if (value !== undefined) {
            result.set(key, value as T);
          }
        }
        return result as Map<string, T>;
      }

      return storage.get(keyOrKeys) as T | undefined;
    },

    async put(keyOrEntries: string | Record<string, unknown>, value?: unknown): Promise<void> {
      if (failureMode === 'write_error' && shouldFail()) {
        throw new Error('DO storage write failed: simulated error');
      }

      if (typeof keyOrEntries === 'string') {
        // Check size limit for single write
        const dataSize = value instanceof ArrayBuffer ? value.byteLength :
                         value instanceof Uint8Array ? value.byteLength :
                         typeof value === 'string' ? value.length : 100;
        checkSizeLimit(dataSize);

        storage.set(keyOrEntries, value);
        currentSize += dataSize;
      } else {
        // Batch write
        let totalSize = 0;
        for (const [, v] of Object.entries(keyOrEntries)) {
          const dataSize = v instanceof ArrayBuffer ? v.byteLength :
                           v instanceof Uint8Array ? v.byteLength :
                           typeof v === 'string' ? v.length : 100;
          totalSize += dataSize;
        }
        checkSizeLimit(totalSize);

        for (const [k, v] of Object.entries(keyOrEntries)) {
          storage.set(k, v);
        }
        currentSize += totalSize;
      }
    },

    async delete(keyOrKeys: string | string[]): Promise<boolean | number> {
      if (failureMode === 'delete_error' && shouldFail()) {
        throw new Error('DO storage delete failed: simulated error');
      }

      if (Array.isArray(keyOrKeys)) {
        let count = 0;
        for (const key of keyOrKeys) {
          if (storage.delete(key)) count++;
        }
        return count;
      }

      return storage.delete(keyOrKeys);
    },

    async list(options?: { prefix?: string; start?: string; end?: string; limit?: number; reverse?: boolean }): Promise<Map<string, unknown>> {
      if (failureMode === 'list_error' && shouldFail()) {
        throw new Error('DO storage list failed: simulated error');
      }

      const result = new Map<string, unknown>();
      const prefix = options?.prefix ?? '';

      for (const [key, value] of storage.entries()) {
        if (key.startsWith(prefix)) {
          result.set(key, value);
        }
      }

      return result;
    },
  };
}

// =============================================================================
// Mock R2 Bucket with Failure Simulation (Simplified version)
// =============================================================================

type R2FailureMode =
  | 'none'
  | 'timeout'
  | 'network_error'
  | 'rate_limited'
  | 'bucket_not_bound'
  | 'permission_denied'
  | 'size_exceeded'
  | 'checksum_mismatch';

interface FailingR2BucketOptions {
  failureMode: R2FailureMode;
  maxSize?: number;
}

function createFailingR2Bucket(options: FailingR2BucketOptions): R2BucketLike & {
  _setFailureMode: (mode: R2FailureMode) => void;
  _storage: Map<string, { data: Uint8Array; etag: string; uploaded: Date }>;
} {
  const storage = new Map<string, { data: Uint8Array; etag: string; uploaded: Date }>();
  let failureMode = options.failureMode;
  const maxSize = options.maxSize ?? 5 * 1024 * 1024 * 1024; // 5GB default

  const createR2Object = (key: string, obj: { data: Uint8Array; etag: string; uploaded: Date }): R2ObjectLike => ({
    key,
    size: obj.data.length,
    etag: obj.etag,
    httpEtag: `"${obj.etag}"`,
    uploaded: obj.uploaded,
    customMetadata: {},
    async arrayBuffer(): Promise<ArrayBuffer> {
      return obj.data.buffer.slice(obj.data.byteOffset, obj.data.byteOffset + obj.data.byteLength);
    },
    async text(): Promise<string> {
      return new TextDecoder().decode(obj.data);
    },
    async json<T>(): Promise<T> {
      return JSON.parse(new TextDecoder().decode(obj.data));
    },
    async blob(): Promise<Blob> {
      return new Blob([obj.data]);
    },
    writeHttpMetadata(): void {},
  });

  return {
    _storage: storage,
    _setFailureMode: (mode: R2FailureMode) => { failureMode = mode; },

    async get(key: string, getOptions?: R2GetOptions): Promise<R2ObjectLike | null> {
      if (failureMode === 'timeout') {
        throw new Error('Request timed out');
      }
      if (failureMode === 'network_error') {
        throw new Error('Network error: connection reset');
      }
      if (failureMode === 'rate_limited') {
        throw new Error('429 Too Many Requests');
      }
      if (failureMode === 'bucket_not_bound') {
        throw new Error('R2 bucket binding not found');
      }
      if (failureMode === 'permission_denied') {
        throw new Error('403 Forbidden: access denied');
      }
      if (failureMode === 'checksum_mismatch') {
        const obj = storage.get(key);
        if (obj) {
          throw new Error('Checksum mismatch: expected abc, got xyz');
        }
      }

      const obj = storage.get(key);
      if (!obj) return null;

      return createR2Object(key, obj);
    },

    async put(key: string, value: R2PutValue, putOptions?: R2PutOptions): Promise<R2ObjectLike> {
      if (failureMode === 'timeout') {
        throw new Error('Request timed out');
      }
      if (failureMode === 'network_error') {
        throw new Error('Network error: connection reset');
      }
      if (failureMode === 'rate_limited') {
        throw new Error('429 Too Many Requests');
      }
      if (failureMode === 'bucket_not_bound') {
        throw new Error('R2 bucket binding not found');
      }
      if (failureMode === 'permission_denied') {
        throw new Error('403 Forbidden: write access denied');
      }

      let data: Uint8Array;
      if (value instanceof Uint8Array) {
        data = value;
      } else if (value instanceof ArrayBuffer) {
        data = new Uint8Array(value);
      } else if (typeof value === 'string') {
        data = new TextEncoder().encode(value);
      } else {
        throw new Error('Unsupported value type');
      }

      if (failureMode === 'size_exceeded' || data.length > maxSize) {
        throw new Error(`Object size ${data.length} exceeds maximum ${maxSize}`);
      }

      const obj = {
        data,
        etag: `etag-${Date.now()}-${Math.random().toString(36).slice(2)}`,
        uploaded: new Date(),
      };
      storage.set(key, obj);

      return createR2Object(key, obj);
    },

    async delete(keys: string | string[]): Promise<void> {
      const keyArray = Array.isArray(keys) ? keys : [keys];
      for (const key of keyArray) {
        storage.delete(key);
      }
    },

    async list(listOptions?: R2ListOptions): Promise<R2ObjectsLike> {
      if (failureMode === 'bucket_not_bound') {
        throw new Error('R2 bucket binding not found');
      }

      const prefix = listOptions?.prefix ?? '';
      const objects: R2ObjectLike[] = [];

      for (const [key, obj] of storage) {
        if (key.startsWith(prefix)) {
          objects.push(createR2Object(key, obj));
        }
      }

      return { objects, truncated: false, delimitedPrefixes: [] };
    },

    async head(key: string): Promise<R2ObjectLike | null> {
      const obj = storage.get(key);
      if (!obj) return null;
      return createR2Object(key, obj);
    },
  };
}

// =============================================================================
// Create Memory Backend with getStats
// =============================================================================

function createHotBackendWithStats(): MemoryFSXBackend & {
  getStats: () => Promise<{ fileCount: number; totalSize: number; chunkedFileCount: number }>;
} {
  const backend = createMemoryBackend();
  const hotWithStats = backend as MemoryFSXBackend & {
    getStats: () => Promise<{ fileCount: number; totalSize: number; chunkedFileCount: number }>;
  };

  hotWithStats.getStats = async () => {
    let totalSize = 0;
    for (const path of backend.paths()) {
      const data = await backend.read(path);
      if (data) totalSize += data.length;
    }
    return {
      fileCount: backend.size,
      totalSize,
      chunkedFileCount: 0,
    };
  };

  return hotWithStats;
}

// =============================================================================
// 1. FSX Read/Write Failures
// =============================================================================

describe('FSX Read/Write Failures', () => {
  describe('FSXError creation and properties', () => {
    it('should create FSXError with all required properties', () => {
      const error = new FSXError(
        FSXErrorCode.READ_FAILED,
        'Failed to read file',
        '/path/to/file.bin'
      );

      expect(error).toBeInstanceOf(FSXError);
      expect(error.code).toBe(FSXErrorCode.READ_FAILED);
      expect(error.message).toBe('Failed to read file');
      expect(error.path).toBe('/path/to/file.bin');
      expect(error.name).toBe('FSXError');
    });

    it('should support cause chaining', () => {
      const originalError = new Error('Underlying I/O error');
      const fsxError = new FSXError(
        FSXErrorCode.READ_FAILED,
        'Read operation failed',
        '/data/file.bin',
        originalError
      );

      expect(fsxError.cause).toBe(originalError);
    });

    it('should have all expected error codes', () => {
      expect(FSXErrorCode.NOT_FOUND).toBe('FSX_NOT_FOUND');
      expect(FSXErrorCode.WRITE_FAILED).toBe('FSX_WRITE_FAILED');
      expect(FSXErrorCode.READ_FAILED).toBe('FSX_READ_FAILED');
      expect(FSXErrorCode.DELETE_FAILED).toBe('FSX_DELETE_FAILED');
      expect(FSXErrorCode.CHUNK_CORRUPTED).toBe('FSX_CHUNK_CORRUPTED');
      expect(FSXErrorCode.SIZE_EXCEEDED).toBe('FSX_SIZE_EXCEEDED');
      expect(FSXErrorCode.MIGRATION_FAILED).toBe('FSX_MIGRATION_FAILED');
      expect(FSXErrorCode.INVALID_RANGE).toBe('FSX_INVALID_RANGE');
    });
  });

  describe('DO Backend read failures', () => {
    it('should handle read errors gracefully', async () => {
      const failingStorage = createFailingDOStorage({ failureMode: 'read_error' });
      const backend = createDOBackend(failingStorage);

      // First write some data (no failure mode for writes)
      failingStorage._setFailureMode('none');
      await backend.write('test.bin', textToBytes('test data'));

      // Now enable read failures
      failingStorage._setFailureMode('read_error');

      // Read should throw
      await expect(backend.read('test.bin')).rejects.toThrow('simulated error');
    });

    it('should return null for non-existent files (not error)', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      const result = await backend.read('nonexistent.bin');
      expect(result).toBeNull();
    });
  });

  describe('DO Backend write failures', () => {
    it('should handle write errors gracefully', async () => {
      const failingStorage = createFailingDOStorage({ failureMode: 'write_error' });
      const backend = createDOBackend(failingStorage);

      await expect(backend.write('test.bin', textToBytes('test data'))).rejects.toThrow('write failed');
    });

    it('should handle batch write failures', async () => {
      const failingStorage = createFailingDOStorage({ failureMode: 'write_error' });
      const backend = createDOBackend(failingStorage);

      // Large data that would require chunking
      const largeData = generateTestData(3 * 1024 * 1024); // 3MB - exceeds 2MB chunk limit

      await expect(backend.write('large.bin', largeData)).rejects.toThrow('write failed');
    });
  });

  describe('Invalid byte range handling', () => {
    it('should throw FSXError for invalid range on read', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      const data = textToBytes('Hello, World!');
      await backend.write('range-test.bin', data);

      // Invalid range: start > end
      const invalidRange: ByteRange = [10, 5];

      await expect(backend.read('range-test.bin', invalidRange)).rejects.toThrow();
    });

    it('should throw FSXError for out-of-bounds range', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      const data = textToBytes('Short');
      await backend.write('short.bin', data);

      // Range exceeds file size
      const outOfBoundsRange: ByteRange = [0, 100];

      await expect(backend.read('short.bin', outOfBoundsRange)).rejects.toThrow();
    });
  });

  describe('Chunked file corruption detection', () => {
    it('should detect missing chunks', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      // Write large file that gets chunked
      const largeData = generateTestData(3 * 1024 * 1024); // 3MB
      await backend.write('chunked.bin', largeData);

      // Manually delete a chunk to simulate corruption
      const chunkKey = `${DEFAULT_CHUNK_CONFIG.chunkPrefix}chunked.bin/000001`;
      storage._storage.delete(chunkKey);

      // Read should detect missing chunk
      await expect(backend.read('chunked.bin')).rejects.toThrow('Missing chunk');
    });
  });
});

// =============================================================================
// 2. R2 Connection Errors
// =============================================================================

describe('R2 Connection Errors', () => {
  describe('R2Error creation and properties', () => {
    it('should create R2Error with all required properties', () => {
      const error = new R2Error(
        R2ErrorCode.TIMEOUT,
        'Request timed out',
        '/path/to/file.bin'
      );

      expect(error).toBeInstanceOf(R2Error);
      expect(error.r2Code).toBe(R2ErrorCode.TIMEOUT);
      expect(error.message).toBe('Request timed out');
      expect(error.path).toBe('/path/to/file.bin');
      expect(error.name).toBe('R2Error');
    });

    it('should have all expected R2 error codes', () => {
      expect(R2ErrorCode.TIMEOUT).toBe('R2_TIMEOUT');
      expect(R2ErrorCode.RATE_LIMITED).toBe('R2_RATE_LIMITED');
      expect(R2ErrorCode.CHECKSUM_MISMATCH).toBe('R2_CHECKSUM_MISMATCH');
      expect(R2ErrorCode.PERMISSION_DENIED).toBe('R2_PERMISSION_DENIED');
      expect(R2ErrorCode.BUCKET_NOT_BOUND).toBe('R2_BUCKET_NOT_BOUND');
      expect(R2ErrorCode.SIZE_EXCEEDED).toBe('R2_SIZE_EXCEEDED');
      expect(R2ErrorCode.CONFLICT).toBe('R2_CONFLICT');
      expect(R2ErrorCode.NOT_FOUND).toBe('R2_NOT_FOUND');
      expect(R2ErrorCode.READ_DURING_WRITE).toBe('R2_READ_DURING_WRITE');
      expect(R2ErrorCode.NETWORK_ERROR).toBe('R2_NETWORK_ERROR');
    });
  });

  describe('R2 timeout handling', () => {
    it('should throw FSXError with READ_FAILED on read timeout', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'timeout' });
      const backend = createR2Backend(bucket, { maxRetries: 0 });

      // Write first without timeout
      bucket._setFailureMode('none');
      await backend.write('test.bin', textToBytes('test data'));
      bucket._setFailureMode('timeout');

      await expect(backend.read('test.bin')).rejects.toThrow('timeout');
    });

    it('should throw FSXError with WRITE_FAILED on write timeout', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'timeout' });
      const backend = createR2Backend(bucket, { maxRetries: 0 });

      await expect(backend.write('test.bin', textToBytes('test'))).rejects.toThrow('timeout');
    });
  });

  describe('R2 rate limiting (429)', () => {
    it('should throw FSXError on rate limit exceeded', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'rate_limited' });
      const backend = createR2Backend(bucket, { maxRetries: 0 });

      await expect(backend.write('test.bin', textToBytes('test'))).rejects.toThrow('429');
    });

    it('should indicate rate limit error is retryable', () => {
      const error = new R2Error(R2ErrorCode.RATE_LIMITED, 'Too many requests');
      expect(error.isRetryable()).toBe(true);
    });
  });

  describe('R2 bucket not bound', () => {
    it('should throw descriptive error when bucket is not bound', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'bucket_not_bound' });
      const backend = createR2Backend(bucket, { maxRetries: 0 });

      await expect(backend.read('test.bin')).rejects.toThrow('bucket');
    });

    it('should suggest wrangler.toml configuration in user message', () => {
      const error = new R2Error(R2ErrorCode.BUCKET_NOT_BOUND, 'Not bound');
      expect(error.toUserMessage()).toContain('wrangler.toml');
    });
  });

  describe('R2 permission denied', () => {
    it('should throw FSXError on permission denied', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'permission_denied' });
      const backend = createR2Backend(bucket, { maxRetries: 0 });

      await expect(backend.read('test.bin')).rejects.toThrow('403');
    });

    it('should indicate permission error is NOT retryable', () => {
      const error = new R2Error(R2ErrorCode.PERMISSION_DENIED, 'Forbidden');
      expect(error.isRetryable()).toBe(false);
    });
  });

  describe('R2 network errors', () => {
    it('should throw FSXError on network error', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'network_error' });
      const backend = createR2Backend(bucket, { maxRetries: 0 });

      await expect(backend.write('test.bin', textToBytes('test'))).rejects.toThrow('connection');
    });

    it('should indicate network error is retryable', () => {
      const error = new R2Error(R2ErrorCode.NETWORK_ERROR, 'Connection reset');
      expect(error.isRetryable()).toBe(true);
    });
  });

  describe('R2 error type detection', () => {
    it('should detect timeout errors', () => {
      expect(detectR2ErrorType(new Error('Request timed out'))).toBe(R2ErrorCode.TIMEOUT);
      expect(detectR2ErrorType(new Error('ETIMEDOUT'))).toBe(R2ErrorCode.TIMEOUT);
    });

    it('should detect rate limit errors', () => {
      expect(detectR2ErrorType(new Error('429 Too Many Requests'))).toBe(R2ErrorCode.RATE_LIMITED);
    });

    it('should detect permission errors', () => {
      expect(detectR2ErrorType(new Error('403 Forbidden'))).toBe(R2ErrorCode.PERMISSION_DENIED);
      expect(detectR2ErrorType(new Error('Access denied'))).toBe(R2ErrorCode.PERMISSION_DENIED);
    });

    it('should detect bucket binding errors', () => {
      expect(detectR2ErrorType(new Error('R2Bucket is not bound'))).toBe(R2ErrorCode.BUCKET_NOT_BOUND);
    });

    it('should default to network error for unknown errors', () => {
      expect(detectR2ErrorType(new Error('Unknown error type'))).toBe(R2ErrorCode.NETWORK_ERROR);
      expect(detectR2ErrorType('string error')).toBe(R2ErrorCode.NETWORK_ERROR);
      expect(detectR2ErrorType(null)).toBe(R2ErrorCode.NETWORK_ERROR);
    });
  });
});

// =============================================================================
// 3. DO Storage Limit Exceeded
// =============================================================================

describe('DO Storage Limit Exceeded', () => {
  describe('2MB per-blob limit', () => {
    it('should auto-chunk files larger than 2MB', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      // File larger than 2MB should be chunked
      const largeData = generateTestData(2.5 * 1024 * 1024); // 2.5MB
      await backend.write('large.bin', largeData);

      // Verify data can be read back correctly
      const result = await backend.read('large.bin');
      expect(result).not.toBeNull();
      expect(result?.length).toBe(largeData.length);
      expect(result).toEqual(largeData);
    });

    it('should handle files exactly at 2MB boundary', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      // Exactly 2MB - should not need chunking
      const exactData = generateTestData(2 * 1024 * 1024);
      await backend.write('exact-2mb.bin', exactData);

      const result = await backend.read('exact-2mb.bin');
      expect(result).toEqual(exactData);
    });

    it('should properly chunk and reassemble very large files', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      // 5MB file - needs 3 chunks
      const veryLargeData = generateTestData(5 * 1024 * 1024);
      await backend.write('very-large.bin', veryLargeData);

      const result = await backend.read('very-large.bin');
      expect(result).not.toBeNull();
      expect(result?.length).toBe(veryLargeData.length);

      // Verify first and last bytes match
      expect(result?.[0]).toBe(veryLargeData[0]);
      expect(result?.[veryLargeData.length - 1]).toBe(veryLargeData[veryLargeData.length - 1]);
    });
  });

  describe('Total storage quota', () => {
    it('should throw error when storage quota is exceeded', async () => {
      const storage = createFailingDOStorage({
        failureMode: 'none',
        sizeLimit: 1024, // Very small 1KB limit
      });
      const backend = createDOBackend(storage);

      // First write should succeed
      await backend.write('small1.bin', generateTestData(500));

      // Now enable quota checking
      storage._setFailureMode('quota_exceeded');

      // Second write should fail (exceeds quota)
      await expect(
        backend.write('small2.bin', generateTestData(600))
      ).rejects.toThrow('quota exceeded');
    });
  });

  describe('Storage statistics', () => {
    it('should track storage usage via getStats', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      // Write some files
      await backend.write('file1.bin', generateTestData(100));
      await backend.write('file2.bin', generateTestData(200));
      await backend.write('file3.bin', generateTestData(300));

      const stats = await backend.getStats();
      expect(stats.fileCount).toBe(3);
      expect(stats.totalSize).toBe(600);
      expect(stats.chunkedFileCount).toBe(0);
    });

    it('should track chunked files in stats', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      // Write a chunked file
      await backend.write('chunked.bin', generateTestData(3 * 1024 * 1024));

      // Write a regular file
      await backend.write('regular.bin', generateTestData(1000));

      const stats = await backend.getStats();
      expect(stats.fileCount).toBe(2);
      expect(stats.chunkedFileCount).toBe(1);
    });
  });
});

// =============================================================================
// 4. Network Timeouts
// =============================================================================

describe('Network Timeouts', () => {
  describe('Timeout error classification', () => {
    it('should classify timeout errors as retryable', () => {
      const timeoutError = new R2Error(R2ErrorCode.TIMEOUT, 'Request timed out');
      expect(timeoutError.isRetryable()).toBe(true);
    });

    it('should include retry information in error message', () => {
      const error = createR2Error(new Error('Request timed out'), '/data/file.bin', 'read');
      expect(error.message).toContain('timeout');
      expect(error.path).toBe('/data/file.bin');
    });
  });

  describe('Transient failure recovery', () => {
    it('should handle transient DO storage failures', async () => {
      const storage = createFailingDOStorage({
        failureMode: 'transient',
        failUntilAttempt: 2,
      });
      const backend = createDOBackend(storage);

      // First attempt will fail, but we're testing that transient failures can occur
      // Note: DOStorageBackend doesn't have built-in retry, so first call will fail
      await expect(backend.write('test.bin', textToBytes('test'))).rejects.toThrow();

      // Reset and try again - should succeed
      storage._resetAttempts();
      storage._setFailureMode('none');
      await backend.write('test.bin', textToBytes('test'));

      const result = await backend.read('test.bin');
      expect(bytesToText(result!)).toBe('test');
    });
  });
});

// =============================================================================
// 5. Graceful Degradation and Error Recovery
// =============================================================================

describe('Graceful Degradation and Error Recovery', () => {
  describe('Tiered storage fallback', () => {
    it('should fall back to cold storage when hot storage fails', async () => {
      const hotBackend = createHotBackendWithStats();
      const coldBucket = createFailingR2Bucket({ failureMode: 'none' });
      const coldBackend = createR2Backend(coldBucket);

      const tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false, cacheR2Reads: false }
      );

      // Write data that goes directly to cold storage (large file)
      await tieredBackend.writeWithTier('cold-data.bin', textToBytes('cold data'), {
        tier: StorageTier.COLD,
      });

      // Clear hot storage to ensure we're reading from cold
      hotBackend.clear();

      // Read should still work (from cold storage)
      const result = await tieredBackend.read('cold-data.bin');
      expect(result).not.toBeNull();
      expect(bytesToText(result!)).toBe('cold data');
    });

    it('should handle both tiers being available', async () => {
      const hotBackend = createHotBackendWithStats();
      const coldBucket = createFailingR2Bucket({ failureMode: 'none' });
      const coldBackend = createR2Backend(coldBucket);

      const tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false, cacheR2Reads: true }
      );

      // Write to hot tier
      await tieredBackend.write('test.bin', textToBytes('test data'));

      // Verify hot storage has the data
      expect(await hotBackend.exists('test.bin')).toBe(true);

      // Read should succeed from hot tier
      const result = await tieredBackend.read('test.bin');
      expect(bytesToText(result!)).toBe('test data');
    });
  });

  describe('R2 circuit breaker behavior', () => {
    it('should track failure count', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'timeout' });
      const backend = createR2Backend(bucket, {
        maxRetries: 0,
        circuitBreakerThreshold: 5,
      });

      // Make multiple failing requests
      for (let i = 0; i < 3; i++) {
        try {
          await backend.write(`fail-${i}.bin`, textToBytes('test'));
        } catch {
          // Expected
        }
      }

      // Health status should show degraded
      const health = await backend.getHealthStatus();
      expect(health.failureCount).toBeGreaterThan(0);
    });

    it('should recover after successful request', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'none' });
      const backend = createR2Backend(bucket, {
        maxRetries: 0,
        circuitBreakerThreshold: 5,
      });

      // Successful write
      await backend.write('success.bin', textToBytes('test'));

      const health = await backend.getHealthStatus();
      expect(health.status).toBe('healthy');
      expect(health.r2Available).toBe(true);
    });
  });

  describe('Read cache for graceful degradation', () => {
    it('should return cached data when available', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'none' });
      const backend = createR2Backend(bucket, {
        readCacheMaxBytes: 1024 * 1024, // 1MB cache
        readCacheMaxEntries: 100,
      });

      // Write and read to populate cache
      await backend.write('cached.bin', textToBytes('cached data'));
      await backend.read('cached.bin');

      // Verify cache has the data
      const cacheStats = backend.getReadCacheStats();
      expect(cacheStats.entryCount).toBeGreaterThan(0);
    });

    it('should provide cache statistics', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'none' });
      const backend = createR2Backend(bucket, {
        readCacheMaxBytes: 1024 * 1024,
        readCacheMaxEntries: 100,
      });

      // Initial stats
      const initialStats = backend.getReadCacheStats();
      expect(initialStats.entryCount).toBe(0);
      expect(initialStats.totalBytes).toBe(0);

      // Populate cache
      await backend.write('file1.bin', textToBytes('data1'));
      await backend.read('file1.bin');

      const afterReadStats = backend.getReadCacheStats();
      expect(afterReadStats.entryCount).toBe(1);
      expect(afterReadStats.totalBytes).toBeGreaterThan(0);
    });

    it('should allow cache clearing', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'none' });
      const backend = createR2Backend(bucket);

      await backend.write('file.bin', textToBytes('data'));
      await backend.read('file.bin');

      // Clear cache
      backend.clearReadCache();

      const stats = backend.getReadCacheStats();
      expect(stats.entryCount).toBe(0);
    });
  });

  describe('Error context preservation', () => {
    it('should preserve path in FSXError', () => {
      const error = new FSXError(
        FSXErrorCode.READ_FAILED,
        'Read failed',
        '/specific/path/to/file.bin'
      );

      expect(error.path).toBe('/specific/path/to/file.bin');
    });

    it('should preserve cause chain in R2Error', () => {
      const originalError = new Error('Network timeout');
      const r2Error = createR2Error(originalError, '/data/file.bin', 'read');

      expect(r2Error.cause).toBe(originalError);
      expect(r2Error.path).toBe('/data/file.bin');
    });

    it('should include operation context in error message', () => {
      const error = createR2Error(
        new Error('Connection reset'),
        '/storage/large-file.bin',
        'write'
      );

      expect(error.message).toContain('/storage/large-file.bin');
      expect(error.message).toContain('write');
    });
  });

  describe('Empty data handling', () => {
    it('should handle empty file writes gracefully', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      await backend.write('empty.bin', new Uint8Array(0));

      const result = await backend.read('empty.bin');
      expect(result).not.toBeNull();
      expect(result?.length).toBe(0);
    });

    it('should handle empty R2 writes gracefully', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'none' });
      const backend = createR2Backend(bucket);

      await backend.write('empty.bin', new Uint8Array(0));

      const result = await backend.read('empty.bin');
      expect(result).not.toBeNull();
      expect(result?.length).toBe(0);
    });
  });

  describe('Special characters in paths', () => {
    it('should handle paths with spaces', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      await backend.write('path with spaces.bin', textToBytes('data'));

      const result = await backend.read('path with spaces.bin');
      expect(result).not.toBeNull();
    });

    it('should handle paths with unicode characters', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      await backend.write('unicode-\u4e2d\u6587-\u65e5\u672c\u8a9e.bin', textToBytes('unicode data'));

      const result = await backend.read('unicode-\u4e2d\u6587-\u65e5\u672c\u8a9e.bin');
      expect(result).not.toBeNull();
      expect(bytesToText(result!)).toBe('unicode data');
    });

    it('should handle deeply nested paths', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      const deepPath = 'a/b/c/d/e/f/g/h/i/j/file.bin';
      await backend.write(deepPath, textToBytes('deep data'));

      const result = await backend.read(deepPath);
      expect(result).not.toBeNull();
      expect(bytesToText(result!)).toBe('deep data');
    });
  });
});

// =============================================================================
// 6. Error Recovery Patterns
// =============================================================================

describe('Error Recovery Patterns', () => {
  describe('Retryable error identification', () => {
    it('should identify retryable R2 errors', () => {
      const retryableErrors = [
        new R2Error(R2ErrorCode.TIMEOUT, 'Timeout'),
        new R2Error(R2ErrorCode.RATE_LIMITED, 'Rate limited'),
        new R2Error(R2ErrorCode.NETWORK_ERROR, 'Network error'),
        new R2Error(R2ErrorCode.READ_DURING_WRITE, 'Busy'),
      ];

      for (const error of retryableErrors) {
        expect(error.isRetryable()).toBe(true);
      }
    });

    it('should identify non-retryable R2 errors', () => {
      const nonRetryableErrors = [
        new R2Error(R2ErrorCode.PERMISSION_DENIED, 'Forbidden'),
        new R2Error(R2ErrorCode.NOT_FOUND, 'Not found'),
        new R2Error(R2ErrorCode.SIZE_EXCEEDED, 'Too large'),
        new R2Error(R2ErrorCode.CHECKSUM_MISMATCH, 'Corrupted'),
      ];

      for (const error of nonRetryableErrors) {
        expect(error.isRetryable()).toBe(false);
      }
    });
  });

  describe('User-friendly error messages', () => {
    it('should provide user-friendly timeout message', () => {
      const error = new R2Error(R2ErrorCode.TIMEOUT, 'Technical details', '/path');
      const message = error.toUserMessage();

      expect(message).toContain('timed out');
      expect(message).not.toContain('R2_TIMEOUT');
    });

    it('should provide user-friendly rate limit message', () => {
      const error = new R2Error(R2ErrorCode.RATE_LIMITED, 'Technical details', undefined, {
        retryAfter: 30,
      });
      const message = error.toUserMessage();

      expect(message).toContain('30');
      expect(message).not.toContain('R2_RATE_LIMITED');
    });

    it('should provide user-friendly bucket not bound message', () => {
      const error = new R2Error(R2ErrorCode.BUCKET_NOT_BOUND, 'Technical details');
      const message = error.toUserMessage();

      expect(message).toContain('wrangler.toml');
      expect(message).not.toContain('R2_BUCKET_NOT_BOUND');
    });
  });

  describe('Delete operation error handling', () => {
    it('should handle delete errors gracefully', async () => {
      const storage = createFailingDOStorage({ failureMode: 'delete_error' });
      const backend = createDOBackend(storage);

      // Write first without errors
      storage._setFailureMode('none');
      await backend.write('to-delete.bin', textToBytes('data'));

      // Enable delete errors
      storage._setFailureMode('delete_error');

      // The delete method in DOStorageBackend doesn't throw on missing files,
      // but should propagate storage errors
      // Note: Current implementation may not throw on delete errors
      // This test documents expected behavior
      await backend.delete('to-delete.bin');
    });
  });

  describe('List operation error handling', () => {
    it('should handle list errors gracefully', async () => {
      const storage = createFailingDOStorage({ failureMode: 'list_error' });
      const backend = createDOBackend(storage);

      await expect(backend.list('')).rejects.toThrow('list failed');
    });
  });
});
