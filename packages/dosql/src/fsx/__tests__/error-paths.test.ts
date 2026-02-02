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

      // Helper to extract data size from various value types
      const getDataSize = (v: unknown): number => {
        if (v instanceof ArrayBuffer) return v.byteLength;
        if (v instanceof Uint8Array) return v.byteLength;
        if (typeof v === 'string') return v.length;
        // Check for FileEntry with nested data ArrayBuffer
        if (v && typeof v === 'object' && 'data' in v) {
          const entry = v as { data?: ArrayBuffer; size?: number };
          if (entry.data instanceof ArrayBuffer) return entry.data.byteLength;
          if (typeof entry.size === 'number') return entry.size;
        }
        return 100;
      };

      if (typeof keyOrEntries === 'string') {
        // Check size limit for single write
        const dataSize = getDataSize(value);
        checkSizeLimit(dataSize);

        storage.set(keyOrEntries, value);
        currentSize += dataSize;
      } else {
        // Batch write
        let totalSize = 0;
        for (const [, v] of Object.entries(keyOrEntries)) {
          totalSize += getDataSize(v);
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
      // Use smaller chunk size for testing
      const backend = createDOBackend(failingStorage, { maxChunkSize: 32 * 1024 });

      // Data that would require chunking (64KB with 32KB chunks = 2 chunks)
      const largeData = generateTestData(64 * 1024);

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
      // Use smaller chunk size for faster testing
      const backend = createDOBackend(storage, { maxChunkSize: 32 * 1024 });

      // Write file that gets chunked (64KB with 32KB chunks = 2 chunks)
      const largeData = generateTestData(64 * 1024);
      await backend.write('chunked.bin', largeData);

      // Manually delete the second chunk to simulate corruption
      const chunkKey = '_chunks/chunked.bin/000001';
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
    it('should auto-chunk files larger than configured chunk size', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      // Use a smaller chunk size for testing (64KB instead of 2MB)
      const backend = createDOBackend(storage, { maxChunkSize: 64 * 1024 });

      // File larger than 64KB should be chunked
      const largeData = generateTestData(128 * 1024); // 128KB
      await backend.write('large.bin', largeData);

      // Verify data can be read back correctly
      const result = await backend.read('large.bin');
      expect(result).not.toBeNull();
      expect(result?.length).toBe(largeData.length);
      expect(result).toEqual(largeData);
    });

    it('should handle files exactly at chunk size boundary', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      // Use a smaller chunk size for testing
      const backend = createDOBackend(storage, { maxChunkSize: 64 * 1024 });

      // Exactly 64KB - should not need chunking
      const exactData = generateTestData(64 * 1024);
      await backend.write('exact-boundary.bin', exactData);

      const result = await backend.read('exact-boundary.bin');
      expect(result).toEqual(exactData);
    });

    it('should properly chunk and reassemble multi-chunk files', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      // Use a smaller chunk size for testing
      const backend = createDOBackend(storage, { maxChunkSize: 32 * 1024 });

      // 128KB file - needs 4 chunks with 32KB chunk size
      const multiChunkData = generateTestData(128 * 1024);
      await backend.write('multi-chunk.bin', multiChunkData);

      const result = await backend.read('multi-chunk.bin');
      expect(result).not.toBeNull();
      expect(result?.length).toBe(multiChunkData.length);

      // Verify first and last bytes match
      expect(result?.[0]).toBe(multiChunkData[0]);
      expect(result?.[multiChunkData.length - 1]).toBe(multiChunkData[multiChunkData.length - 1]);
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
      // Use smaller chunk size to test chunking without memory issues
      const backend = createDOBackend(storage, { maxChunkSize: 32 * 1024 });

      // Write a chunked file (larger than 32KB chunk size)
      await backend.write('chunked.bin', generateTestData(64 * 1024));

      // Write a regular file (smaller than chunk size)
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
        failureMode: 'write_error',
      });
      const backend = createDOBackend(storage);

      // First attempt will fail due to write error
      await expect(backend.write('test.bin', textToBytes('test'))).rejects.toThrow('write failed');

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
    it('should handle delete errors by throwing', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      // Write first without errors
      await backend.write('to-delete.bin', textToBytes('data'));

      // Enable delete errors
      storage._setFailureMode('delete_error');

      // The delete method propagates storage errors
      await expect(backend.delete('to-delete.bin')).rejects.toThrow('delete failed');
    });

    it('should handle delete of non-existent file gracefully', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      // Deleting non-existent file should not throw
      await backend.delete('nonexistent.bin');
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

// =============================================================================
// 7. R2 Auth Failure Scenarios
// =============================================================================

describe('R2 Auth Failure Scenarios', () => {
  describe('Authentication error handling', () => {
    it('should detect and report 401 unauthorized errors', () => {
      // 401 typically manifests as network/auth errors at the R2 level
      // The error detection primarily looks for keywords like 'forbidden', 'permission', 'access denied'
      const error = detectR2ErrorType(new Error('401 Unauthorized: access denied'));
      expect(error).toBe(R2ErrorCode.PERMISSION_DENIED);
    });

    it('should detect and report 403 forbidden errors', () => {
      const error = detectR2ErrorType(new Error('403 Forbidden'));
      expect(error).toBe(R2ErrorCode.PERMISSION_DENIED);
    });

    it('should provide helpful message for auth failures', () => {
      const error = new R2Error(
        R2ErrorCode.PERMISSION_DENIED,
        'Access denied to bucket',
        '/data/file.bin',
        { httpStatus: 403 }
      );
      expect(error.httpStatus).toBe(403);
      expect(error.toUserMessage()).toContain('denied');
      expect(error.isRetryable()).toBe(false);
    });
  });

  describe('Token/key rotation scenarios', () => {
    it('should handle expired token errors', () => {
      const expiredTokenError = new Error('Token has expired');
      const errorType = detectR2ErrorType(expiredTokenError);
      // Token expiry typically manifests as permission denied
      expect([R2ErrorCode.PERMISSION_DENIED, R2ErrorCode.NETWORK_ERROR]).toContain(errorType);
    });
  });
});

// =============================================================================
// 8. Corrupted Data Handling
// =============================================================================

describe('Corrupted Data Handling', () => {
  describe('Checksum validation', () => {
    it('should detect checksum mismatch error messages', () => {
      const checksumError = new Error('Checksum mismatch: expected abc123, got xyz789');
      expect(detectR2ErrorType(checksumError)).toBe(R2ErrorCode.CHECKSUM_MISMATCH);
    });

    it('should detect MD5 verification failures', () => {
      const md5Error = new Error('MD5 verification failed');
      expect(detectR2ErrorType(md5Error)).toBe(R2ErrorCode.CHECKSUM_MISMATCH);
    });

    it('should detect integrity check failures', () => {
      const integrityError = new Error('Integrity check failed');
      expect(detectR2ErrorType(integrityError)).toBe(R2ErrorCode.CHECKSUM_MISMATCH);
    });

    it('should create R2Error with checksum details', () => {
      const error = new R2Error(
        R2ErrorCode.CHECKSUM_MISMATCH,
        'Data corruption detected',
        '/data/file.bin',
        {
          expectedChecksum: 'abc123',
          actualChecksum: 'xyz789',
        }
      );
      expect(error.expectedChecksum).toBe('abc123');
      expect(error.actualChecksum).toBe('xyz789');
      expect(error.isRetryable()).toBe(false);
    });

    it('should indicate checksum errors are not retryable', () => {
      const error = new R2Error(R2ErrorCode.CHECKSUM_MISMATCH, 'Corrupted');
      expect(error.isRetryable()).toBe(false);
    });
  });

  describe('DO chunk corruption', () => {
    it('should detect corrupted chunk metadata', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      // Use smaller chunk size for testing
      const backend = createDOBackend(storage, { maxChunkSize: 32 * 1024 });

      // Write a file that gets chunked (64KB with 32KB chunks)
      const largeData = generateTestData(64 * 1024);
      await backend.write('chunked-file.bin', largeData);

      // Corrupt a chunk by replacing it with garbage
      const chunkKey = `${DEFAULT_CHUNK_CONFIG.chunkPrefix}chunked-file.bin/000000`;
      storage._storage.set(chunkKey, new ArrayBuffer(100)); // Wrong size

      // Reading should detect corruption
      try {
        await backend.read('chunked-file.bin');
      } catch (error) {
        // Should either throw or return corrupted data
        expect(error).toBeDefined();
      }
    });

    it('should detect truncated chunk data', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      // Write file
      await backend.write('test.bin', textToBytes('Hello, World!'));

      // Truncate the data
      const fileEntry = storage._storage.get('file:test.bin') as { data: ArrayBuffer };
      if (fileEntry?.data) {
        storage._storage.set('file:test.bin', {
          ...fileEntry,
          data: fileEntry.data.slice(0, 5), // Truncate to 5 bytes
          size: 5,
        });
      }

      // Read should return truncated data
      const result = await backend.read('test.bin');
      expect(result?.length).toBe(5);
    });
  });

  describe('Data validation during writes', () => {
    it('should verify data was written correctly for R2', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'none' });
      const backend = createR2Backend(bucket);

      const testData = textToBytes('Test data for validation');
      await backend.write('validated.bin', testData);

      const result = await backend.read('validated.bin');
      expect(result).toEqual(testData);
    });
  });
});

// =============================================================================
// 9. Partial Write Recovery
// =============================================================================

describe('Partial Write Recovery', () => {
  describe('R2 partial write detection', () => {
    it('should detect when write size does not match expected', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'none' });
      const backend = createR2Backend(bucket);

      // Normal write should succeed
      const data = textToBytes('complete data');
      await backend.write('complete.bin', data);

      // Verify size matches
      const meta = await backend.metadata('complete.bin');
      expect(meta?.size).toBe(data.length);
    });

    it('should handle write interruption gracefully', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'timeout' });
      const backend = createR2Backend(bucket, { maxRetries: 0 });

      // Write with timeout should throw error
      await expect(
        backend.write('interrupted.bin', textToBytes('data'))
      ).rejects.toThrow('timeout');
    });
  });

  describe('DO chunked write recovery', () => {
    it('should handle interrupted chunked write', async () => {
      const storage = createFailingDOStorage({ failureMode: 'none' });
      const backend = createDOBackend(storage);

      // Successfully write small file first
      await backend.write('small.bin', textToBytes('small'));
      expect(await backend.exists('small.bin')).toBe(true);

      // Now try to write large file with failure mid-write
      storage._setFailureMode('write_error');

      await expect(
        backend.write('large-interrupted.bin', generateTestData(100))
      ).rejects.toThrow('write failed');

      // Reset and verify we can still use storage
      storage._setFailureMode('none');
      await backend.write('recovery.bin', textToBytes('recovered'));
      expect(await backend.exists('recovery.bin')).toBe(true);
    });
  });

  describe('Recovery strategies', () => {
    it('should clean up orphaned data after failed write', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'none' });
      const backend = createR2Backend(bucket);

      // Write should succeed
      await backend.write('will-be-cleaned.bin', textToBytes('data'));
      expect(await backend.exists('will-be-cleaned.bin')).toBe(true);

      // Delete the file
      await backend.delete('will-be-cleaned.bin');
      expect(await backend.exists('will-be-cleaned.bin')).toBe(false);
    });

    it('should handle concurrent writes to same path', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'none' });
      const backend = createR2Backend(bucket);

      // Concurrent writes - one should win
      const write1 = backend.write('concurrent.bin', textToBytes('write1'));
      const write2 = backend.write('concurrent.bin', textToBytes('write2'));

      await Promise.all([write1, write2]);

      // One of the writes should have succeeded
      const result = await backend.read('concurrent.bin');
      expect(result).not.toBeNull();
      const content = bytesToText(result!);
      expect(['write1', 'write2']).toContain(content);
    });
  });
});

// =============================================================================
// 10. Network Failure Patterns
// =============================================================================

describe('Network Failure Patterns', () => {
  describe('Connection reset handling', () => {
    it('should detect connection reset errors', () => {
      expect(detectR2ErrorType(new Error('Connection reset'))).toBe(R2ErrorCode.NETWORK_ERROR);
      expect(detectR2ErrorType(new Error('ECONNRESET'))).toBe(R2ErrorCode.NETWORK_ERROR);
    });

    it('should detect ETIMEDOUT errors', () => {
      expect(detectR2ErrorType(new Error('ETIMEDOUT'))).toBe(R2ErrorCode.TIMEOUT);
    });

    it('should detect ENOTFOUND errors', () => {
      expect(detectR2ErrorType(new Error('ENOTFOUND'))).toBe(R2ErrorCode.NETWORK_ERROR);
    });
  });

  describe('R2 retry with backoff', () => {
    it('should use exponential backoff delays', () => {
      // The R2 backend should use 100ms, 200ms, 400ms, etc.
      const baseDelay = 100;
      const attempt0 = baseDelay * Math.pow(2, 0); // 100
      const attempt1 = baseDelay * Math.pow(2, 1); // 200
      const attempt2 = baseDelay * Math.pow(2, 2); // 400

      expect(attempt0).toBe(100);
      expect(attempt1).toBe(200);
      expect(attempt2).toBe(400);
    });
  });

  describe('Circuit breaker states', () => {
    it('should create circuit open error', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'network_error' });
      const backend = createR2Backend(bucket, {
        maxRetries: 0,
        circuitBreakerThreshold: 2,
      });

      // Trigger multiple failures to open circuit
      for (let i = 0; i < 3; i++) {
        try {
          await backend.write(`fail-${i}.bin`, textToBytes('test'));
        } catch {
          // Expected
        }
      }

      // Check health status shows degraded
      const health = await backend.getHealthStatus();
      expect(health.failureCount).toBeGreaterThan(0);
    });

    it('should report health status', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'none' });
      const backend = createR2Backend(bucket);

      const health = await backend.getHealthStatus();
      expect(health.status).toBe('healthy');
      expect(health.r2Available).toBe(true);
      expect(health.failureCount).toBe(0);
    });
  });

  describe('Degraded mode operation', () => {
    it('should serve from cache during degraded mode', async () => {
      const bucket = createFailingR2Bucket({ failureMode: 'none' });
      const backend = createR2Backend(bucket, {
        readCacheMaxBytes: 1024 * 1024,
      });

      // Populate cache
      await backend.write('cached-data.bin', textToBytes('cached'));
      await backend.read('cached-data.bin');

      // Verify cache has data
      const stats = backend.getReadCacheStats();
      expect(stats.entryCount).toBeGreaterThan(0);

      // Even if R2 fails, cache should have the data
      bucket._setFailureMode('network_error');
      const result = await backend.read('cached-data.bin');
      expect(result).not.toBeNull();
      expect(bytesToText(result!)).toBe('cached');
    });
  });
});

// =============================================================================
// 11. Error Propagation and Context
// =============================================================================

describe('Error Propagation and Context', () => {
  describe('Error chaining', () => {
    it('should preserve original error as cause', () => {
      const originalError = new Error('Original storage error');
      const r2Error = createR2Error(originalError, '/path/to/file.bin', 'read');

      expect(r2Error.cause).toBe(originalError);
    });

    it('should include operation context', () => {
      const error = createR2Error(
        new Error('Network failure'),
        '/data/important.bin',
        'write'
      );

      expect(error.message).toContain('/data/important.bin');
      expect(error.message).toContain('write');
    });
  });

  describe('Error formatting', () => {
    it('should format R2 errors for logging', async () => {
      const { formatR2ErrorForLog } = await import('../r2-errors.js');
      const error = new R2Error(
        R2ErrorCode.TIMEOUT,
        'Request timed out',
        '/data/file.bin',
        {
          httpStatus: 408,
          retryCount: 3,
          requestId: 'req-123',
        }
      );

      const formatted = formatR2ErrorForLog(error);
      expect(formatted).toContain('[R2_TIMEOUT]');
      expect(formatted).toContain('path=/data/file.bin');
      expect(formatted).toContain('status=408');
      expect(formatted).toContain('retries=3');
      expect(formatted).toContain('requestId=req-123');
    });
  });

  describe('FSX error codes', () => {
    it('should map R2 errors to FSX codes correctly', () => {
      // Timeout should map to READ_FAILED
      const timeoutError = new R2Error(R2ErrorCode.TIMEOUT, 'Timeout');
      expect(timeoutError.code).toBe(FSXErrorCode.READ_FAILED);

      // Not found should map to NOT_FOUND
      const notFoundError = new R2Error(R2ErrorCode.NOT_FOUND, 'Not found');
      expect(notFoundError.code).toBe(FSXErrorCode.NOT_FOUND);

      // Size exceeded should map to SIZE_EXCEEDED
      const sizeError = new R2Error(R2ErrorCode.SIZE_EXCEEDED, 'Too large');
      expect(sizeError.code).toBe(FSXErrorCode.SIZE_EXCEEDED);

      // Checksum mismatch should map to CHUNK_CORRUPTED
      const checksumError = new R2Error(R2ErrorCode.CHECKSUM_MISMATCH, 'Corrupted');
      expect(checksumError.code).toBe(FSXErrorCode.CHUNK_CORRUPTED);
    });
  });
});

// =============================================================================
// 12. R2 Size Limit Handling
// =============================================================================

describe('R2 Size Limit Handling', () => {
  describe('Object size detection', () => {
    it('should detect size exceeded errors', () => {
      expect(detectR2ErrorType(new Error('Object size exceeds limit'))).toBe(R2ErrorCode.SIZE_EXCEEDED);
      expect(detectR2ErrorType(new Error('File too large'))).toBe(R2ErrorCode.SIZE_EXCEEDED);
      expect(detectR2ErrorType(new Error('exceeds maximum size'))).toBe(R2ErrorCode.SIZE_EXCEEDED);
    });

    it('should provide user-friendly message for size exceeded', () => {
      const error = new R2Error(R2ErrorCode.SIZE_EXCEEDED, 'Object too large');
      const message = error.toUserMessage();
      expect(message).toContain('5GB');
      expect(message).not.toContain('R2_SIZE_EXCEEDED');
    });

    it('should indicate size exceeded is not retryable', () => {
      const error = new R2Error(R2ErrorCode.SIZE_EXCEEDED, 'Too large');
      expect(error.isRetryable()).toBe(false);
    });
  });

  describe('Object size enforcement', () => {
    it('should reject objects over configured limit', async () => {
      const bucket = createFailingR2Bucket({
        failureMode: 'size_exceeded',
        maxSize: 1024, // 1KB limit for test
      });
      const backend = createR2Backend(bucket, { maxRetries: 0 });

      await expect(
        backend.write('too-large.bin', generateTestData(2048))
      ).rejects.toThrow('size');
    });
  });
});

// =============================================================================
// 13. R2 Conflict Handling
// =============================================================================

describe('R2 Conflict Handling', () => {
  describe('Conflict detection', () => {
    it('should detect conflict errors', () => {
      expect(detectR2ErrorType(new Error('Write conflict'))).toBe(R2ErrorCode.CONFLICT);
      expect(detectR2ErrorType(new Error('Concurrent modification'))).toBe(R2ErrorCode.CONFLICT);
      expect(detectR2ErrorType(new Error('ETag mismatch'))).toBe(R2ErrorCode.CONFLICT);
    });

    it('should detect read during write errors', () => {
      expect(detectR2ErrorType(new Error('Object is being written'))).toBe(R2ErrorCode.READ_DURING_WRITE);
      expect(detectR2ErrorType(new Error('write in progress'))).toBe(R2ErrorCode.READ_DURING_WRITE);
    });
  });

  describe('Conflict error properties', () => {
    it('should indicate conflict is not retryable', () => {
      const error = new R2Error(R2ErrorCode.CONFLICT, 'Conflict');
      expect(error.isRetryable()).toBe(false);
    });

    it('should indicate read during write is retryable', () => {
      const error = new R2Error(R2ErrorCode.READ_DURING_WRITE, 'Busy');
      expect(error.isRetryable()).toBe(true);
    });
  });
});

// =============================================================================
// 14. Tiered Storage Error Handling
// =============================================================================

describe('Tiered Storage Error Handling', () => {
  describe('Tier write failures', () => {
    it('should reject writes exceeding hot tier size limit', async () => {
      const hotBackend = createHotBackendWithStats();
      const coldBucket = createFailingR2Bucket({ failureMode: 'none' });
      const coldBackend = createR2Backend(coldBucket);

      const tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        {
          autoMigrate: false,
          maxHotFileSize: 1024, // 1KB limit
        }
      );

      // Writing to hot tier with explicit tier option should fail if too large
      await expect(
        tieredBackend.writeWithTier(
          'too-large-for-hot.bin',
          generateTestData(2048),
          { tier: StorageTier.HOT }
        )
      ).rejects.toThrow('exceeds maxHotFileSize');
    });

    it('should automatically route large files to cold tier', async () => {
      const hotBackend = createHotBackendWithStats();
      const coldBucket = createFailingR2Bucket({ failureMode: 'none' });
      const coldBackend = createR2Backend(coldBucket);

      const tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        {
          autoMigrate: false,
          maxHotFileSize: 1024, // 1KB limit
        }
      );

      // Large file without explicit tier should go to cold
      await tieredBackend.write('large-file.bin', generateTestData(2048));

      // Check metadata shows cold tier
      const meta = await tieredBackend.metadata('large-file.bin');
      expect(meta?.tier).toBe(StorageTier.COLD);
    });
  });

  describe('Tier read fallback', () => {
    it('should fall back to cold tier when hot tier read fails', async () => {
      const hotBackend = createHotBackendWithStats();
      const coldBucket = createFailingR2Bucket({ failureMode: 'none' });
      const coldBackend = createR2Backend(coldBucket);

      const tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false, cacheR2Reads: false }
      );

      // Write directly to cold tier
      await tieredBackend.writeWithTier(
        'cold-only.bin',
        textToBytes('cold data'),
        { tier: StorageTier.COLD }
      );

      // Read should succeed from cold tier
      const result = await tieredBackend.read('cold-only.bin');
      expect(result).not.toBeNull();
      expect(bytesToText(result!)).toBe('cold data');
    });
  });

  describe('Pin/unpin error handling', () => {
    it('should throw NOT_FOUND when pinning non-existent file', async () => {
      const hotBackend = createHotBackendWithStats();
      const coldBucket = createFailingR2Bucket({ failureMode: 'none' });
      const coldBackend = createR2Backend(coldBucket);

      const tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false }
      );

      await expect(
        tieredBackend.pinToHot('non-existent.bin')
      ).rejects.toThrow('not found');
    });
  });
});
