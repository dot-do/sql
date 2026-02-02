/**
 * Iceberg R2 Error Handling Tests (TDD RED Phase)
 *
 * These tests document the EXPECTED behavior for R2 error handling in IcebergStorage.
 * R2 operations (put, get, delete, list) should wrap errors in IcebergError with
 * proper context including operation type, key, and original error.
 *
 * Issue: sql-0v8 - Add R2 error handling in iceberg.ts
 *
 * Expected Behavior:
 * - All R2 operations should be wrapped in try-catch
 * - Errors should be thrown as IcebergError with descriptive message
 * - Error should include the original error as cause
 * - Error context should include operation type, key, and bucket info
 *
 * NOTE: This file uses TEST DOUBLES (fakes) instead of mocks (vi.fn) to comply
 * with the NO MOCKS philosophy. The fake implementations provide real behavior
 * with configurable error injection.
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { R2IcebergStorage } from '../iceberg.js';
import { IcebergError, type IcebergTableMetadata } from '../types.js';

// =============================================================================
// Test Double Types (Fakes, not Mocks)
// =============================================================================

/**
 * Fake R2Bucket that can be configured to throw errors.
 * This is a TEST DOUBLE with real behavior - not a mock.
 */
class FakeR2Bucket {
  private storage = new Map<string, Uint8Array>();
  private putError: Error | null = null;
  private getError: Error | null = null;
  private deleteError: Error | null = null;
  private listError: Error | null = null;

  async put(key: string, data: ArrayBuffer | ReadableStream | string): Promise<R2Object> {
    if (this.putError) {
      throw this.putError;
    }
    const bytes =
      data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : typeof data === 'string'
          ? new TextEncoder().encode(data)
          : new Uint8Array(await new Response(data).arrayBuffer());
    this.storage.set(key, bytes);
    return {
      key,
      size: bytes.length,
      uploaded: new Date(),
      httpEtag: `"${key}-etag"`,
      etag: `${key}-etag`,
      version: '1',
    } as R2Object;
  }

  async get(key: string): Promise<R2ObjectBody | null> {
    if (this.getError) {
      throw this.getError;
    }
    const data = this.storage.get(key);
    if (!data) return null;
    return {
      key,
      size: data.length,
      uploaded: new Date(),
      httpEtag: `"${key}-etag"`,
      etag: `${key}-etag`,
      version: '1',
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(data);
          controller.close();
        },
      }),
      bodyUsed: false,
      arrayBuffer: async () => data.buffer,
      text: async () => new TextDecoder().decode(data),
      json: async () => JSON.parse(new TextDecoder().decode(data)),
      blob: async () => new Blob([data]),
    } as R2ObjectBody;
  }

  async delete(key: string | string[]): Promise<void> {
    if (this.deleteError) {
      throw this.deleteError;
    }
    const keys = Array.isArray(key) ? key : [key];
    for (const k of keys) {
      this.storage.delete(k);
    }
  }

  async list(options?: R2ListOptions): Promise<R2Objects> {
    if (this.listError) {
      throw this.listError;
    }
    const prefix = options?.prefix ?? '';
    const objects: R2Object[] = [];
    for (const [key, data] of this.storage) {
      if (key.startsWith(prefix)) {
        objects.push({
          key,
          size: data.length,
          uploaded: new Date(),
          httpEtag: `"${key}-etag"`,
          etag: `${key}-etag`,
          version: '1',
        } as R2Object);
      }
    }
    return {
      objects,
      truncated: false,
      delimitedPrefixes: [],
    };
  }

  // Test helper methods for error injection
  injectPutError(error: Error): void {
    this.putError = error;
  }

  injectGetError(error: Error): void {
    this.getError = error;
  }

  injectDeleteError(error: Error): void {
    this.deleteError = error;
  }

  injectListError(error: Error): void {
    this.listError = error;
  }

  clearErrors(): void {
    this.putError = null;
    this.getError = null;
    this.deleteError = null;
    this.listError = null;
  }

  clear(): void {
    this.storage.clear();
    this.clearErrors();
  }
}

/**
 * Fake DurableObjectStorage for testing.
 * This is a TEST DOUBLE with real in-memory behavior - not a mock.
 */
class FakeDOStorage {
  private storage = new Map<string, unknown>();

  async get<T>(key: string): Promise<T | undefined>;
  async get<T>(keys: string[]): Promise<Map<string, T>>;
  async get<T>(keyOrKeys: string | string[]): Promise<T | undefined | Map<string, T>> {
    if (Array.isArray(keyOrKeys)) {
      const result = new Map<string, T>();
      for (const key of keyOrKeys) {
        const value = this.storage.get(key);
        if (value !== undefined) {
          result.set(key, value as T);
        }
      }
      return result;
    }
    return this.storage.get(keyOrKeys) as T | undefined;
  }

  async put<T>(key: string, value: T): Promise<void>;
  async put<T>(entries: Record<string, T>): Promise<void>;
  async put<T>(keyOrEntries: string | Record<string, T>, value?: T): Promise<void> {
    if (typeof keyOrEntries === 'string') {
      this.storage.set(keyOrEntries, value);
    } else {
      for (const [k, v] of Object.entries(keyOrEntries)) {
        this.storage.set(k, v);
      }
    }
  }

  async delete(key: string): Promise<boolean>;
  async delete(keys: string[]): Promise<number>;
  async delete(keyOrKeys: string | string[]): Promise<boolean | number> {
    if (Array.isArray(keyOrKeys)) {
      let count = 0;
      for (const key of keyOrKeys) {
        if (this.storage.delete(key)) {
          count++;
        }
      }
      return count;
    }
    return this.storage.delete(keyOrKeys);
  }

  async list(options?: { prefix?: string }): Promise<Map<string, unknown>> {
    const result = new Map<string, unknown>();
    const prefix = options?.prefix ?? '';
    for (const [key, value] of this.storage) {
      if (key.startsWith(prefix)) {
        result.set(key, value);
      }
    }
    return result;
  }

  clear(): void {
    this.storage.clear();
  }
}

// =============================================================================
// Test Utilities
// =============================================================================

function createMinimalMetadata(): IcebergTableMetadata {
  return {
    'format-version': 2,
    'table-uuid': 'test-uuid-1234',
    location: 's3://bucket/tables/test',
    'last-sequence-number': 0n,
    'last-updated-ms': Date.now(),
    'last-column-id': 1,
    schemas: [
      {
        type: 'struct',
        'schema-id': 0,
        fields: [{ id: 1, name: 'id', type: 'long', required: true }],
      },
    ],
    'current-schema-id': 0,
    'partition-specs': [{ 'spec-id': 0, fields: [] }],
    'default-spec-id': 0,
    'last-partition-id': 0,
    'sort-orders': [{ 'order-id': 0, fields: [] }],
    'default-sort-order-id': 0,
    properties: {},
    snapshots: [],
    'snapshot-log': [],
    'metadata-log': [],
  };
}

// =============================================================================
// R2 Put Error Handling Tests
// =============================================================================

describe('R2IcebergStorage R2 Error Handling', () => {
  let fakeBucket: FakeR2Bucket;
  let fakeDOStorage: FakeDOStorage;
  let storage: R2IcebergStorage;

  beforeEach(() => {
    fakeBucket = new FakeR2Bucket();
    fakeDOStorage = new FakeDOStorage();
    storage = new R2IcebergStorage(
      fakeBucket as unknown as R2Bucket,
      'test-base-path',
      fakeDOStorage as unknown as DurableObjectStorage
    );
  });

  describe('writeDataFile - R2 put failures', () => {
    it('should throw IcebergError when R2 put fails', async () => {
      // Arrange
      const r2Error = new Error('R2 service unavailable');
      fakeBucket.injectPutError(r2Error);

      // Act & Assert
      await expect(
        storage.writeDataFile('data/file.parquet', new Uint8Array([1, 2, 3]))
      ).rejects.toThrow(IcebergError);
    });

    it('should include operation type in error message', async () => {
      // Arrange
      const r2Error = new Error('Network timeout');
      fakeBucket.injectPutError(r2Error);

      // Act & Assert
      await expect(
        storage.writeDataFile('data/file.parquet', new Uint8Array([1, 2, 3]))
      ).rejects.toThrow(/put|write/i);
    });

    it('should include key/path in error message', async () => {
      // Arrange
      const r2Error = new Error('Bucket quota exceeded');
      fakeBucket.injectPutError(r2Error);
      const path = 'data/important-file.parquet';

      // Act & Assert
      await expect(storage.writeDataFile(path, new Uint8Array([1, 2, 3]))).rejects.toThrow(
        /important-file\.parquet/
      );
    });

    it('should include original error message in IcebergError', async () => {
      // Arrange
      const r2Error = new Error('Storage backend failure: disk full');
      fakeBucket.injectPutError(r2Error);

      // Act & Assert
      await expect(
        storage.writeDataFile('data/file.parquet', new Uint8Array([1, 2, 3]))
      ).rejects.toThrow(/disk full|Storage backend failure/);
    });

    it('should preserve original error as cause', async () => {
      // Arrange
      const r2Error = new Error('Original R2 error');
      fakeBucket.injectPutError(r2Error);

      // Act
      let caughtError: Error | undefined;
      try {
        await storage.writeDataFile('data/file.parquet', new Uint8Array([1, 2, 3]));
      } catch (error) {
        caughtError = error as Error;
      }

      // Assert
      expect(caughtError).toBeDefined();
      expect(caughtError).toBeInstanceOf(IcebergError);
      expect(caughtError?.cause).toBe(r2Error);
    });
  });

  describe('readDataFile - R2 get failures', () => {
    it('should throw IcebergError when R2 get fails', async () => {
      // Arrange
      const r2Error = new Error('R2 connection refused');
      fakeBucket.injectGetError(r2Error);

      // Act & Assert
      await expect(storage.readDataFile('data/file.parquet')).rejects.toThrow(IcebergError);
    });

    it('should include operation type in error message', async () => {
      // Arrange
      const r2Error = new Error('Timeout reading from R2');
      fakeBucket.injectGetError(r2Error);

      // Act & Assert
      await expect(storage.readDataFile('data/file.parquet')).rejects.toThrow(/get|read/i);
    });

    it('should include key/path in error message', async () => {
      // Arrange
      const r2Error = new Error('Permission denied');
      fakeBucket.injectGetError(r2Error);
      const path = 'data/critical-data.parquet';

      // Act & Assert
      await expect(storage.readDataFile(path)).rejects.toThrow(/critical-data\.parquet/);
    });

    it('should include original error message in IcebergError', async () => {
      // Arrange
      const r2Error = new Error('Access token expired');
      fakeBucket.injectGetError(r2Error);

      // Act & Assert
      await expect(storage.readDataFile('data/file.parquet')).rejects.toThrow(
        /Access token expired/
      );
    });

    it('should preserve original error as cause', async () => {
      // Arrange
      const r2Error = new Error('Original get error');
      fakeBucket.injectGetError(r2Error);

      // Act
      let caughtError: Error | undefined;
      try {
        await storage.readDataFile('data/file.parquet');
      } catch (error) {
        caughtError = error as Error;
      }

      // Assert
      expect(caughtError).toBeDefined();
      expect(caughtError).toBeInstanceOf(IcebergError);
      expect(caughtError?.cause).toBe(r2Error);
    });
  });

  describe('deleteDataFile - R2 delete failures', () => {
    it('should throw IcebergError when R2 delete fails', async () => {
      // Arrange
      const r2Error = new Error('R2 delete operation failed');
      fakeBucket.injectDeleteError(r2Error);

      // Act & Assert
      await expect(storage.deleteDataFile('data/file.parquet')).rejects.toThrow(IcebergError);
    });

    it('should include operation type in error message', async () => {
      // Arrange
      const r2Error = new Error('Delete not allowed');
      fakeBucket.injectDeleteError(r2Error);

      // Act & Assert
      await expect(storage.deleteDataFile('data/file.parquet')).rejects.toThrow(/delete/i);
    });

    it('should include key/path in error message', async () => {
      // Arrange
      const r2Error = new Error('Object locked');
      fakeBucket.injectDeleteError(r2Error);
      const path = 'data/locked-file.parquet';

      // Act & Assert
      await expect(storage.deleteDataFile(path)).rejects.toThrow(/locked-file\.parquet/);
    });

    it('should include original error message in IcebergError', async () => {
      // Arrange
      const r2Error = new Error('Retention policy prevents deletion');
      fakeBucket.injectDeleteError(r2Error);

      // Act & Assert
      await expect(storage.deleteDataFile('data/file.parquet')).rejects.toThrow(
        /Retention policy prevents deletion/
      );
    });

    it('should preserve original error as cause', async () => {
      // Arrange
      const r2Error = new Error('Original delete error');
      fakeBucket.injectDeleteError(r2Error);

      // Act
      let caughtError: Error | undefined;
      try {
        await storage.deleteDataFile('data/file.parquet');
      } catch (error) {
        caughtError = error as Error;
      }

      // Assert
      expect(caughtError).toBeDefined();
      expect(caughtError).toBeInstanceOf(IcebergError);
      expect(caughtError?.cause).toBe(r2Error);
    });
  });

  describe('listDataFiles - R2 list failures', () => {
    it('should throw IcebergError when R2 list fails', async () => {
      // Arrange
      const r2Error = new Error('R2 list operation timed out');
      fakeBucket.injectListError(r2Error);

      // Act & Assert
      await expect(storage.listDataFiles('data/')).rejects.toThrow(IcebergError);
    });

    it('should include operation type in error message', async () => {
      // Arrange
      const r2Error = new Error('Pagination error');
      fakeBucket.injectListError(r2Error);

      // Act & Assert
      await expect(storage.listDataFiles('data/')).rejects.toThrow(/list/i);
    });

    it('should include prefix in error message', async () => {
      // Arrange
      const r2Error = new Error('Invalid prefix');
      fakeBucket.injectListError(r2Error);
      const prefix = 'tables/orders/data/';

      // Act & Assert
      await expect(storage.listDataFiles(prefix)).rejects.toThrow(/tables\/orders\/data/);
    });

    it('should preserve original error as cause', async () => {
      // Arrange
      const r2Error = new Error('Original list error');
      fakeBucket.injectListError(r2Error);

      // Act
      let caughtError: Error | undefined;
      try {
        await storage.listDataFiles('data/');
      } catch (error) {
        caughtError = error as Error;
      }

      // Assert
      expect(caughtError).toBeDefined();
      expect(caughtError).toBeInstanceOf(IcebergError);
      expect(caughtError?.cause).toBe(r2Error);
    });
  });

  describe('writeMetadata - R2 put failures for metadata', () => {
    it('should throw IcebergError when R2 put fails for metadata', async () => {
      // Arrange
      const r2Error = new Error('R2 metadata write failed');
      fakeBucket.injectPutError(r2Error);
      const metadata = createMinimalMetadata();

      // Act & Assert
      await expect(
        storage.writeMetadata('metadata/v1.metadata.json', metadata)
      ).rejects.toThrow(IcebergError);
    });

    it('should include operation type in error message for metadata write', async () => {
      // Arrange
      const r2Error = new Error('Metadata serialization failed');
      fakeBucket.injectPutError(r2Error);
      const metadata = createMinimalMetadata();

      // Act & Assert
      await expect(
        storage.writeMetadata('metadata/v1.metadata.json', metadata)
      ).rejects.toThrow(/put|write/i);
    });

    it('should include metadata path in error message', async () => {
      // Arrange
      const r2Error = new Error('Write conflict');
      fakeBucket.injectPutError(r2Error);
      const metadata = createMinimalMetadata();
      const path = 'metadata/v42.metadata.json';

      // Act & Assert
      await expect(storage.writeMetadata(path, metadata)).rejects.toThrow(
        /v42\.metadata\.json/
      );
    });

    it('should preserve original error as cause for metadata write', async () => {
      // Arrange
      const r2Error = new Error('Original metadata write error');
      fakeBucket.injectPutError(r2Error);
      const metadata = createMinimalMetadata();

      // Act
      let caughtError: Error | undefined;
      try {
        await storage.writeMetadata('metadata/v1.metadata.json', metadata);
      } catch (error) {
        caughtError = error as Error;
      }

      // Assert
      expect(caughtError).toBeDefined();
      expect(caughtError).toBeInstanceOf(IcebergError);
      expect(caughtError?.cause).toBe(r2Error);
    });
  });

  describe('readMetadata - R2 get failures for metadata', () => {
    it('should throw IcebergError when R2 get fails for metadata', async () => {
      // Arrange
      const r2Error = new Error('R2 metadata read failed');
      fakeBucket.injectGetError(r2Error);

      // Act & Assert
      await expect(storage.readMetadata('metadata/v1.metadata.json')).rejects.toThrow(
        IcebergError
      );
    });

    it('should include operation type in error message for metadata read', async () => {
      // Arrange
      const r2Error = new Error('Metadata deserialization failed');
      fakeBucket.injectGetError(r2Error);

      // Act & Assert
      await expect(storage.readMetadata('metadata/v1.metadata.json')).rejects.toThrow(
        /get|read/i
      );
    });

    it('should include metadata path in error message', async () => {
      // Arrange
      const r2Error = new Error('Metadata corrupted');
      fakeBucket.injectGetError(r2Error);
      const path = 'metadata/v99.metadata.json';

      // Act & Assert
      await expect(storage.readMetadata(path)).rejects.toThrow(/v99\.metadata\.json/);
    });

    it('should preserve original error as cause for metadata read', async () => {
      // Arrange
      const r2Error = new Error('Original metadata read error');
      fakeBucket.injectGetError(r2Error);

      // Act
      let caughtError: Error | undefined;
      try {
        await storage.readMetadata('metadata/v1.metadata.json');
      } catch (error) {
        caughtError = error as Error;
      }

      // Assert
      expect(caughtError).toBeDefined();
      expect(caughtError).toBeInstanceOf(IcebergError);
      expect(caughtError?.cause).toBe(r2Error);
    });
  });

  describe('Error context completeness', () => {
    it('should include all context in writeDataFile error: operation, key, original message', async () => {
      // Arrange
      const r2Error = new Error('Quota exceeded for bucket');
      fakeBucket.injectPutError(r2Error);
      const path = 'data/large-file.parquet';

      // Act
      let caughtError: IcebergError | undefined;
      try {
        await storage.writeDataFile(path, new Uint8Array([1, 2, 3]));
      } catch (error) {
        caughtError = error as IcebergError;
      }

      // Assert - error message should contain all context
      expect(caughtError).toBeDefined();
      expect(caughtError?.message).toMatch(/put|write/i);
      expect(caughtError?.message).toMatch(/large-file\.parquet/);
      expect(caughtError?.message).toMatch(/Quota exceeded/);
    });

    it('should include all context in readDataFile error: operation, key, original message', async () => {
      // Arrange
      const r2Error = new Error('Connection reset by peer');
      fakeBucket.injectGetError(r2Error);
      const path = 'data/missing-file.parquet';

      // Act
      let caughtError: IcebergError | undefined;
      try {
        await storage.readDataFile(path);
      } catch (error) {
        caughtError = error as IcebergError;
      }

      // Assert - error message should contain all context
      expect(caughtError).toBeDefined();
      expect(caughtError?.message).toMatch(/get|read/i);
      expect(caughtError?.message).toMatch(/missing-file\.parquet/);
      expect(caughtError?.message).toMatch(/Connection reset/);
    });

    it('should include all context in deleteDataFile error: operation, key, original message', async () => {
      // Arrange
      const r2Error = new Error('Object is immutable');
      fakeBucket.injectDeleteError(r2Error);
      const path = 'data/protected-file.parquet';

      // Act
      let caughtError: IcebergError | undefined;
      try {
        await storage.deleteDataFile(path);
      } catch (error) {
        caughtError = error as IcebergError;
      }

      // Assert - error message should contain all context
      expect(caughtError).toBeDefined();
      expect(caughtError?.message).toMatch(/delete/i);
      expect(caughtError?.message).toMatch(/protected-file\.parquet/);
      expect(caughtError?.message).toMatch(/immutable/);
    });

    it('should include all context in listDataFiles error: operation, prefix, original message', async () => {
      // Arrange
      const r2Error = new Error('Rate limit exceeded');
      fakeBucket.injectListError(r2Error);
      const prefix = 'tables/inventory/data/';

      // Act
      let caughtError: IcebergError | undefined;
      try {
        await storage.listDataFiles(prefix);
      } catch (error) {
        caughtError = error as IcebergError;
      }

      // Assert - error message should contain all context
      expect(caughtError).toBeDefined();
      expect(caughtError?.message).toMatch(/list/i);
      expect(caughtError?.message).toMatch(/inventory/);
      expect(caughtError?.message).toMatch(/Rate limit/);
    });
  });
});
