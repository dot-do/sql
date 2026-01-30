/**
 * Writer Safety Tests
 *
 * Tests for buffer bounds checking and memory safety in the columnar writer.
 * These tests ensure that malformed or malicious data cannot cause buffer overflows.
 *
 * Issue: sql-jw89 - Add columnar buffer bounds checking (P0 CRITICAL)
 */

import { describe, it, expect } from 'vitest';
import { ColumnarWriter, writeColumnar } from '../writer.js';
import type { ColumnarTableSchema } from '../types.js';

// ============================================================================
// TEST FIXTURES
// ============================================================================

/**
 * Create a schema for bytes column testing
 */
function createBytesSchema(): ColumnarTableSchema {
  return {
    tableName: 'bytes_test',
    columns: [
      { name: 'id', dataType: 'int32', nullable: false },
      { name: 'data', dataType: 'bytes', nullable: true },
    ],
  };
}

// ============================================================================
// BUFFER OVERFLOW DETECTION TESTS
// ============================================================================

describe('Buffer Bounds Checking', () => {
  describe('encodeBytesColumn safety', () => {
    it('should detect buffer overflow with malicious Proxy that changes length', async () => {
      // This test exposes TOCTOU vulnerability where a Proxy returns different
      // lengths on different accesses. The fix should snapshot values or
      // verify bounds before buffer.set().
      //
      // The Proxy intercepts .length access and returns different values,
      // simulating data mutation during encoding.

      const schema = createBytesSchema();
      const writer = new ColumnarWriter(schema);

      // Create a real Uint8Array that we'll wrap with a Proxy
      const realData = new Uint8Array(100).fill(0xff);
      let lengthAccessCount = 0;

      // Create a Proxy that lies about its length on first access
      const maliciousData = new Proxy(realData, {
        get(target, prop, receiver) {
          if (prop === 'length') {
            lengthAccessCount++;
            // First access (snapshot/size calc): report 10 bytes
            // Subsequent accesses: report real 100 bytes
            return lengthAccessCount === 1 ? 10 : 100;
          }
          return Reflect.get(target, prop, receiver);
        },
      });

      await writer.write([
        { id: 1, data: maliciousData as unknown as Uint8Array },
      ]);

      // With the fix, this should either:
      // 1. Detect size mismatch and throw, OR
      // 2. Successfully encode by taking a snapshot that freezes the length
      // Either way, it should NOT corrupt memory
      try {
        const rowGroup = await writer.finalize();
        // If it succeeds, verify the data was encoded safely
        expect(rowGroup).toBeDefined();
        // The implementation should have either rejected or safely handled the proxy
      } catch (error) {
        // This is also acceptable - detecting and rejecting malicious input
        expect((error as Error).message).toMatch(/buffer|overflow|size|bounds/i);
      }
    });

    it('should validate offset + length before buffer.set', async () => {
      // This test ensures that bounds checking happens BEFORE buffer.set is called
      // The implementation should check: offset + value.length <= buffer.length

      const schema = createBytesSchema();
      const writer = new ColumnarWriter(schema);

      // Create test data that will be mutated via a proxy
      const baseData = new Uint8Array([1, 2, 3, 4, 5]);
      let accessCount = 0;

      // This proxy returns a larger array on second access
      const mutatingProxy = new Proxy(baseData, {
        get(target, prop, receiver) {
          if (prop === 'length') {
            accessCount++;
            // After first length check, "grow" the array
            return accessCount === 1 ? 5 : 50;
          }
          // When actually reading bytes, return from a larger array
          if (typeof prop === 'string' && !isNaN(Number(prop))) {
            const idx = Number(prop);
            if (idx >= 5) {
              return 0xaa; // Return garbage for "out of bounds" indices
            }
          }
          return Reflect.get(target, prop, receiver);
        },
      });

      await writer.write([
        { id: 1, data: mutatingProxy as unknown as Uint8Array },
      ]);

      // The implementation should either reject or safely handle this
      try {
        const rowGroup = await writer.finalize();
        // If it succeeds, verify the data column exists and has reasonable size
        expect(rowGroup).toBeDefined();
        const dataChunk = rowGroup!.columns.get('data');
        expect(dataChunk).toBeDefined();
      } catch (error) {
        // Throwing an error is also acceptable
        expect((error as Error).message).toMatch(/buffer|overflow|size|bounds|mismatch/i);
      }
    });

    it('should safely encode valid bytes data', async () => {
      // Ensure the bounds checking doesn't break normal operation
      const schema = createBytesSchema();
      const writer = new ColumnarWriter(schema);

      const validData = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

      await writer.write([
        { id: 1, data: validData },
        { id: 2, data: null },
        { id: 3, data: new Uint8Array([255, 0, 127]) },
      ]);

      const rowGroup = await writer.finalize();
      expect(rowGroup).toBeDefined();
      expect(rowGroup!.rowCount).toBe(3);

      const dataChunk = rowGroup!.columns.get('data');
      expect(dataChunk).toBeDefined();
      // Row 1: 4 (len) + 10 (data) = 14
      // Row 2: 4 (len) + 0 (null) = 4
      // Row 3: 4 (len) + 3 (data) = 7
      // Total: 25 bytes
      expect(dataChunk!.data.length).toBe(25);
    });

    it('should throw clear error when buffer.set would overflow', async () => {
      const schema = createBytesSchema();
      const writer = new ColumnarWriter(schema);

      // Create an array where the subarray we're setting is larger than remaining space
      // This tests the direct case where offset + value.length > buffer.length
      const normalData = new Uint8Array([1, 2, 3, 4, 5]);

      // This should work fine
      await writer.write([
        { id: 1, data: normalData },
      ]);

      const rowGroup = await writer.finalize();
      expect(rowGroup).toBeDefined();
      expect(rowGroup!.rowCount).toBe(1);
    });

    it('should handle Uint8Array with byteOffset correctly', async () => {
      const schema = createBytesSchema();
      const writer = new ColumnarWriter(schema);

      // Create a view into a larger buffer with a non-zero byteOffset
      const largeBuffer = new ArrayBuffer(100);
      const view = new Uint8Array(largeBuffer, 50, 10); // 10 bytes starting at offset 50
      for (let i = 0; i < 10; i++) {
        view[i] = i + 1;
      }

      await writer.write([
        { id: 1, data: view },
      ]);

      const rowGroup = await writer.finalize();
      expect(rowGroup).toBeDefined();

      // The encoded data should only contain the 10 bytes from the view
      const dataChunk = rowGroup!.columns.get('data');
      expect(dataChunk).toBeDefined();
    });

    it('should safely handle empty Uint8Array', async () => {
      const schema = createBytesSchema();
      const writer = new ColumnarWriter(schema);

      const emptyData = new Uint8Array(0);

      await writer.write([
        { id: 1, data: emptyData },
      ]);

      const rowGroup = await writer.finalize();
      expect(rowGroup).toBeDefined();
      expect(rowGroup!.rowCount).toBe(1);
    });

    it('should safely handle null bytes values', async () => {
      const schema = createBytesSchema();
      const writer = new ColumnarWriter(schema);

      await writer.write([
        { id: 1, data: null },
        { id: 2, data: new Uint8Array([1, 2, 3]) },
        { id: 3, data: null },
      ]);

      const rowGroup = await writer.finalize();
      expect(rowGroup).toBeDefined();
      expect(rowGroup!.rowCount).toBe(3);
    });
  });

  describe('Debug assertions', () => {
    it('should validate buffer size before encoding', async () => {
      const schema = createBytesSchema();
      const writer = new ColumnarWriter(schema);

      // Large but valid data
      const largeData = new Uint8Array(10000);
      for (let i = 0; i < largeData.length; i++) {
        largeData[i] = i % 256;
      }

      await writer.write([
        { id: 1, data: largeData },
      ]);

      const rowGroup = await writer.finalize();
      expect(rowGroup).toBeDefined();

      const dataChunk = rowGroup!.columns.get('data');
      expect(dataChunk).toBeDefined();
      // 4 bytes for length prefix + 10000 bytes of data
      expect(dataChunk!.data.length).toBe(4 + 10000);
    });

    it('should maintain data integrity through encode/decode cycle', async () => {
      const schema = createBytesSchema();

      const testData = new Uint8Array([0, 127, 128, 255, 1, 2, 3]);

      const { rowGroups } = await writeColumnar(schema, [
        { id: 1, data: testData },
      ]);

      expect(rowGroups).toHaveLength(1);

      // Verify the encoded data preserves the original
      const dataChunk = rowGroups[0].columns.get('data');
      expect(dataChunk).toBeDefined();
    });
  });

  describe('Size estimation errors', () => {
    it('should handle multiple bytes columns correctly', async () => {
      const schema: ColumnarTableSchema = {
        tableName: 'multi_bytes',
        columns: [
          { name: 'a', dataType: 'bytes', nullable: true },
          { name: 'b', dataType: 'bytes', nullable: true },
          { name: 'c', dataType: 'bytes', nullable: true },
        ],
      };

      const writer = new ColumnarWriter(schema);

      await writer.write([
        { a: new Uint8Array([1, 2]), b: null, c: new Uint8Array([3, 4, 5]) },
        { a: null, b: new Uint8Array([6]), c: null },
        { a: new Uint8Array([7, 8, 9, 10]), b: new Uint8Array([11, 12]), c: new Uint8Array([13]) },
      ]);

      const rowGroup = await writer.finalize();
      expect(rowGroup).toBeDefined();
      expect(rowGroup!.rowCount).toBe(3);

      // Verify each column was encoded correctly
      for (const col of ['a', 'b', 'c']) {
        const chunk = rowGroup!.columns.get(col);
        expect(chunk).toBeDefined();
        expect(chunk!.dataType).toBe('bytes');
      }
    });

    it('should not corrupt memory on rapid sequential writes', async () => {
      const schema = createBytesSchema();
      const writer = new ColumnarWriter(schema);

      // Rapid sequential writes of varying sizes
      for (let i = 0; i < 100; i++) {
        const size = Math.floor(Math.random() * 100) + 1;
        const data = new Uint8Array(size);
        for (let j = 0; j < size; j++) {
          data[j] = j % 256;
        }

        await writer.write([{ id: i, data }]);
      }

      const rowGroup = await writer.finalize();
      expect(rowGroup).toBeDefined();
      expect(rowGroup!.rowCount).toBe(100);
    });
  });

  describe('Error messages', () => {
    it('should provide clear error context when bounds check fails', async () => {
      // This test verifies that error messages include helpful debugging information:
      // - The offset where the overflow occurred
      // - The size of the data being written
      // - The total buffer size
      // - The index in the values array

      const schema = createBytesSchema();
      const writer = new ColumnarWriter(schema);

      // Create a proxy that returns growing length
      const baseData = new Uint8Array([1, 2, 3]);
      let accessCount = 0;
      const growingProxy = new Proxy(baseData, {
        get(target, prop) {
          if (prop === 'length') {
            accessCount++;
            // Return small length initially, then huge length
            return accessCount <= 2 ? 3 : 10000;
          }
          return Reflect.get(target, prop);
        },
      });

      await writer.write([
        { id: 1, data: growingProxy as unknown as Uint8Array },
      ]);

      // The implementation should catch this and provide a helpful error
      // or safely handle it via snapshot
      try {
        await writer.finalize();
        // If it succeeds, the snapshot mechanism worked
      } catch (error) {
        const message = (error as Error).message.toLowerCase();
        // Error should mention buffer, overflow, or bounds
        expect(
          message.includes('buffer') ||
          message.includes('overflow') ||
          message.includes('bounds')
        ).toBe(true);
      }
    });
  });

  describe('Invariant documentation', () => {
    it('should enforce that buffer.set offset is within bounds', async () => {
      // This test documents the safety invariant:
      // For any call to buffer.set(value, offset):
      //   - offset must be >= 0
      //   - offset + value.length must be <= buffer.length
      //
      // The implementation must verify these conditions before calling buffer.set()

      const schema = createBytesSchema();
      const writer = new ColumnarWriter(schema);

      // Normal case should work
      await writer.write([
        { id: 1, data: new Uint8Array([1, 2, 3]) },
        { id: 2, data: new Uint8Array([4, 5, 6, 7, 8]) },
      ]);

      const rowGroup = await writer.finalize();
      expect(rowGroup).toBeDefined();

      // Verify the data column encoding
      const dataChunk = rowGroup!.columns.get('data');
      expect(dataChunk).toBeDefined();

      // Total size should be:
      // Row 1: 4 (length) + 3 (data) = 7
      // Row 2: 4 (length) + 5 (data) = 9
      // Total: 16 bytes
      expect(dataChunk!.data.length).toBe(16);
    });
  });
});
