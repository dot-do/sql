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
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { R2IcebergStorage } from '../iceberg.js';
import { IcebergError, type IcebergTableMetadata } from '../types.js';

// =============================================================================
// Mock Types
// =============================================================================

/**
 * Mock R2Bucket that can be configured to throw errors
 */
interface MockR2Bucket {
  put: ReturnType<typeof vi.fn>;
  get: ReturnType<typeof vi.fn>;
  delete: ReturnType<typeof vi.fn>;
  list: ReturnType<typeof vi.fn>;
}

/**
 * Mock DurableObjectStorage
 */
interface MockDOStorage {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
  delete: ReturnType<typeof vi.fn>;
  list: ReturnType<typeof vi.fn>;
}

// =============================================================================
// Test Utilities
// =============================================================================

function createMockR2Bucket(): MockR2Bucket {
  return {
    put: vi.fn(),
    get: vi.fn(),
    delete: vi.fn(),
    list: vi.fn(),
  };
}

function createMockDOStorage(): MockDOStorage {
  return {
    get: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    list: vi.fn().mockResolvedValue(new Map()),
  };
}

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
        fields: [
          { id: 1, name: 'id', type: 'long', required: true },
        ],
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
  let mockBucket: MockR2Bucket;
  let mockDOStorage: MockDOStorage;
  let storage: R2IcebergStorage;

  beforeEach(() => {
    mockBucket = createMockR2Bucket();
    mockDOStorage = createMockDOStorage();
    storage = new R2IcebergStorage(
      mockBucket as unknown as R2Bucket,
      'test-base-path',
      mockDOStorage as unknown as DurableObjectStorage
    );
  });

  describe('writeDataFile - R2 put failures', () => {
    it('should throw IcebergError when R2 put fails', async () => {
      // Arrange
      const r2Error = new Error('R2 service unavailable');
      mockBucket.put.mockRejectedValue(r2Error);

      // Act & Assert
      await expect(
        storage.writeDataFile('data/file.parquet', new Uint8Array([1, 2, 3]))
      ).rejects.toThrow(IcebergError);
    });

    it('should include operation type in error message', async () => {
      // Arrange
      const r2Error = new Error('Network timeout');
      mockBucket.put.mockRejectedValue(r2Error);

      // Act & Assert
      await expect(
        storage.writeDataFile('data/file.parquet', new Uint8Array([1, 2, 3]))
      ).rejects.toThrow(/put|write/i);
    });

    it('should include key/path in error message', async () => {
      // Arrange
      const r2Error = new Error('Bucket quota exceeded');
      mockBucket.put.mockRejectedValue(r2Error);
      const path = 'data/important-file.parquet';

      // Act & Assert
      await expect(storage.writeDataFile(path, new Uint8Array([1, 2, 3]))).rejects.toThrow(
        /important-file\.parquet/
      );
    });

    it('should include original error message in IcebergError', async () => {
      // Arrange
      const r2Error = new Error('Storage backend failure: disk full');
      mockBucket.put.mockRejectedValue(r2Error);

      // Act & Assert
      await expect(
        storage.writeDataFile('data/file.parquet', new Uint8Array([1, 2, 3]))
      ).rejects.toThrow(/disk full|Storage backend failure/);
    });

    it('should preserve original error as cause', async () => {
      // Arrange
      const r2Error = new Error('Original R2 error');
      mockBucket.put.mockRejectedValue(r2Error);

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
      mockBucket.get.mockRejectedValue(r2Error);

      // Act & Assert
      await expect(storage.readDataFile('data/file.parquet')).rejects.toThrow(IcebergError);
    });

    it('should include operation type in error message', async () => {
      // Arrange
      const r2Error = new Error('Timeout reading from R2');
      mockBucket.get.mockRejectedValue(r2Error);

      // Act & Assert
      await expect(storage.readDataFile('data/file.parquet')).rejects.toThrow(/get|read/i);
    });

    it('should include key/path in error message', async () => {
      // Arrange
      const r2Error = new Error('Permission denied');
      mockBucket.get.mockRejectedValue(r2Error);
      const path = 'data/critical-data.parquet';

      // Act & Assert
      await expect(storage.readDataFile(path)).rejects.toThrow(/critical-data\.parquet/);
    });

    it('should include original error message in IcebergError', async () => {
      // Arrange
      const r2Error = new Error('Access token expired');
      mockBucket.get.mockRejectedValue(r2Error);

      // Act & Assert
      await expect(storage.readDataFile('data/file.parquet')).rejects.toThrow(
        /Access token expired/
      );
    });

    it('should preserve original error as cause', async () => {
      // Arrange
      const r2Error = new Error('Original get error');
      mockBucket.get.mockRejectedValue(r2Error);

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
      mockBucket.delete.mockRejectedValue(r2Error);

      // Act & Assert
      await expect(storage.deleteDataFile('data/file.parquet')).rejects.toThrow(IcebergError);
    });

    it('should include operation type in error message', async () => {
      // Arrange
      const r2Error = new Error('Delete not allowed');
      mockBucket.delete.mockRejectedValue(r2Error);

      // Act & Assert
      await expect(storage.deleteDataFile('data/file.parquet')).rejects.toThrow(/delete/i);
    });

    it('should include key/path in error message', async () => {
      // Arrange
      const r2Error = new Error('Object locked');
      mockBucket.delete.mockRejectedValue(r2Error);
      const path = 'data/locked-file.parquet';

      // Act & Assert
      await expect(storage.deleteDataFile(path)).rejects.toThrow(/locked-file\.parquet/);
    });

    it('should include original error message in IcebergError', async () => {
      // Arrange
      const r2Error = new Error('Retention policy prevents deletion');
      mockBucket.delete.mockRejectedValue(r2Error);

      // Act & Assert
      await expect(storage.deleteDataFile('data/file.parquet')).rejects.toThrow(
        /Retention policy prevents deletion/
      );
    });

    it('should preserve original error as cause', async () => {
      // Arrange
      const r2Error = new Error('Original delete error');
      mockBucket.delete.mockRejectedValue(r2Error);

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
      mockBucket.list.mockRejectedValue(r2Error);

      // Act & Assert
      await expect(storage.listDataFiles('data/')).rejects.toThrow(IcebergError);
    });

    it('should include operation type in error message', async () => {
      // Arrange
      const r2Error = new Error('Pagination error');
      mockBucket.list.mockRejectedValue(r2Error);

      // Act & Assert
      await expect(storage.listDataFiles('data/')).rejects.toThrow(/list/i);
    });

    it('should include prefix in error message', async () => {
      // Arrange
      const r2Error = new Error('Invalid prefix');
      mockBucket.list.mockRejectedValue(r2Error);
      const prefix = 'tables/orders/data/';

      // Act & Assert
      await expect(storage.listDataFiles(prefix)).rejects.toThrow(/tables\/orders\/data/);
    });

    it('should preserve original error as cause', async () => {
      // Arrange
      const r2Error = new Error('Original list error');
      mockBucket.list.mockRejectedValue(r2Error);

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
      mockBucket.put.mockRejectedValue(r2Error);
      const metadata = createMinimalMetadata();

      // Act & Assert
      await expect(
        storage.writeMetadata('metadata/v1.metadata.json', metadata)
      ).rejects.toThrow(IcebergError);
    });

    it('should include operation type in error message for metadata write', async () => {
      // Arrange
      const r2Error = new Error('Metadata serialization failed');
      mockBucket.put.mockRejectedValue(r2Error);
      const metadata = createMinimalMetadata();

      // Act & Assert
      await expect(
        storage.writeMetadata('metadata/v1.metadata.json', metadata)
      ).rejects.toThrow(/put|write/i);
    });

    it('should include metadata path in error message', async () => {
      // Arrange
      const r2Error = new Error('Write conflict');
      mockBucket.put.mockRejectedValue(r2Error);
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
      mockBucket.put.mockRejectedValue(r2Error);
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
      mockBucket.get.mockRejectedValue(r2Error);

      // Act & Assert
      await expect(storage.readMetadata('metadata/v1.metadata.json')).rejects.toThrow(
        IcebergError
      );
    });

    it('should include operation type in error message for metadata read', async () => {
      // Arrange
      const r2Error = new Error('Metadata deserialization failed');
      mockBucket.get.mockRejectedValue(r2Error);

      // Act & Assert
      await expect(storage.readMetadata('metadata/v1.metadata.json')).rejects.toThrow(
        /get|read/i
      );
    });

    it('should include metadata path in error message', async () => {
      // Arrange
      const r2Error = new Error('Metadata corrupted');
      mockBucket.get.mockRejectedValue(r2Error);
      const path = 'metadata/v99.metadata.json';

      // Act & Assert
      await expect(storage.readMetadata(path)).rejects.toThrow(/v99\.metadata\.json/);
    });

    it('should preserve original error as cause for metadata read', async () => {
      // Arrange
      const r2Error = new Error('Original metadata read error');
      mockBucket.get.mockRejectedValue(r2Error);

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
      mockBucket.put.mockRejectedValue(r2Error);
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
      mockBucket.get.mockRejectedValue(r2Error);
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
      mockBucket.delete.mockRejectedValue(r2Error);
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
      mockBucket.list.mockRejectedValue(r2Error);
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
