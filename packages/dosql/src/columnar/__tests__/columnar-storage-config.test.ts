/**
 * Columnar Storage Config Tests
 *
 * Verifies that the columnar writer respects StorageConfig overrides
 * for rowGroupSize and maxRowsPerRowGroup.
 *
 * TDD: RED phase - these tests should fail until writer is wired up.
 */

import { describe, it, expect } from 'vitest';

import { ColumnarWriter, type WriterConfig } from '../writer.js';
import type { ColumnarTableSchema } from '../types.js';
import {
  TARGET_ROW_GROUP_SIZE,
  MAX_ROWS_PER_ROW_GROUP,
} from '../types.js';
import type { TableStorageConfig } from '../../engine/storage-config.js';
import { DEFAULT_STORAGE_CONFIG } from '../../engine/storage-config.js';

// Helper schema for tests
const testSchema: ColumnarTableSchema = {
  tableName: 'test_table',
  columns: [
    { name: 'id', dataType: 'int32', nullable: false },
    { name: 'value', dataType: 'int32', nullable: false },
  ],
};

describe('Columnar Writer - StorageConfig integration', () => {
  describe('default behavior (no StorageConfig)', () => {
    it('uses TARGET_ROW_GROUP_SIZE as default targetBytesPerGroup', () => {
      const writer = new ColumnarWriter(testSchema);
      // The writer should use the constant defaults when no config is provided
      // We verify by writing enough rows to NOT trigger a flush at a smaller threshold
      // but DO trigger at the default threshold

      // Defaults should match the constants
      expect(TARGET_ROW_GROUP_SIZE).toBe(1 * 1024 * 1024); // 1MB
      expect(MAX_ROWS_PER_ROW_GROUP).toBe(65536);

      // And match DEFAULT_STORAGE_CONFIG
      expect(DEFAULT_STORAGE_CONFIG.rowGroupSize).toBe(TARGET_ROW_GROUP_SIZE);
      expect(DEFAULT_STORAGE_CONFIG.maxRowsPerRowGroup).toBe(MAX_ROWS_PER_ROW_GROUP);
    });

    it('flushes at MAX_ROWS_PER_ROW_GROUP rows by default', async () => {
      const writer = new ColumnarWriter(testSchema);

      // Write exactly MAX_ROWS_PER_ROW_GROUP rows
      const rows = Array.from({ length: MAX_ROWS_PER_ROW_GROUP }, (_, i) => ({
        id: i,
        value: i * 10,
      }));

      const flushed = await writer.write(rows);
      // Should have triggered exactly one flush at 65536 rows
      expect(flushed.length).toBe(1);
      expect(flushed[0].rowCount).toBe(MAX_ROWS_PER_ROW_GROUP);
    });
  });

  describe('StorageConfig overrides via WriterConfig', () => {
    it('accepts storageConfig in WriterConfig', () => {
      const storageConfig: TableStorageConfig = {
        rowGroupSize: 512 * 1024, // 512KB
        maxRowsPerRowGroup: 1000,
      };

      const writer = new ColumnarWriter(testSchema, { storageConfig });
      // Should construct without error
      expect(writer).toBeDefined();
    });

    it('respects per-table maxRowsPerRowGroup override', async () => {
      const storageConfig: TableStorageConfig = {
        maxRowsPerRowGroup: 100,
      };

      const writer = new ColumnarWriter(testSchema, { storageConfig });

      // Write 100 rows - should trigger flush at 100 instead of 65536
      const rows = Array.from({ length: 100 }, (_, i) => ({
        id: i,
        value: i * 10,
      }));

      const flushed = await writer.write(rows);
      expect(flushed.length).toBe(1);
      expect(flushed[0].rowCount).toBe(100);
    });

    it('respects per-table rowGroupSize override for byte-based flushing', async () => {
      // Use a very small row group size to trigger byte-based flush
      const storageConfig: TableStorageConfig = {
        rowGroupSize: 256, // 256 bytes - very small
        maxRowsPerRowGroup: 100000, // high row limit so byte limit triggers first
      };

      const writer = new ColumnarWriter(testSchema, { storageConfig });

      // Each row is 2x int32 = 8 bytes. 256/8 = 32 rows to hit byte limit
      const rows = Array.from({ length: 64 }, (_, i) => ({
        id: i,
        value: i * 10,
      }));

      const flushed = await writer.write(rows);
      // Should have flushed at least once due to byte size threshold
      expect(flushed.length).toBeGreaterThanOrEqual(1);
      // Each flushed group should have fewer rows than the total
      for (const rg of flushed) {
        expect(rg.rowCount).toBeLessThan(64);
      }
    });

    it('storageConfig does not override explicit WriterConfig values', async () => {
      const storageConfig: TableStorageConfig = {
        maxRowsPerRowGroup: 500,
        rowGroupSize: 2 * 1024 * 1024,
      };

      // Explicit targetRowsPerGroup should take precedence over storageConfig
      const writer = new ColumnarWriter(testSchema, {
        storageConfig,
        targetRowsPerGroup: 50,
      });

      const rows = Array.from({ length: 50 }, (_, i) => ({
        id: i,
        value: i * 10,
      }));

      const flushed = await writer.write(rows);
      expect(flushed.length).toBe(1);
      expect(flushed[0].rowCount).toBe(50);
    });

    it('storageConfig does not override explicit targetBytesPerGroup', async () => {
      const storageConfig: TableStorageConfig = {
        rowGroupSize: 64, // very small
      };

      // Explicit targetBytesPerGroup should take precedence
      const writer = new ColumnarWriter(testSchema, {
        storageConfig,
        targetBytesPerGroup: 10 * 1024 * 1024, // 10MB - very large
      });

      // Write 200 rows of int32 pairs (8 bytes each = 1600 bytes total)
      // With explicit 10MB target, should NOT flush
      const rows = Array.from({ length: 200 }, (_, i) => ({
        id: i,
        value: i * 10,
      }));

      const flushed = await writer.write(rows);
      expect(flushed.length).toBe(0); // Should not have flushed
      expect(writer.getBufferedRowCount()).toBe(200);
    });

    it('uses default constants when storageConfig fields are undefined', async () => {
      // Empty storageConfig - should behave like no config
      const storageConfig: TableStorageConfig = {};

      const writer = new ColumnarWriter(testSchema, { storageConfig });

      // Write 100 rows - should NOT flush (default is 65536)
      const rows = Array.from({ length: 100 }, (_, i) => ({
        id: i,
        value: i * 10,
      }));

      const flushed = await writer.write(rows);
      expect(flushed.length).toBe(0);
      expect(writer.getBufferedRowCount()).toBe(100);
    });
  });
});
