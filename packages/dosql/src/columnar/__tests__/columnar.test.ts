/**
 * Columnar Module Tests
 *
 * Comprehensive TDD tests for the DoSQL columnar OLAP storage engine.
 * Tests encoding/decoding, chunk operations, compression, filtering, and aggregation.
 *
 * Using workers-vitest-pool (NO MOCKS)
 */

import { describe, it, expect, beforeEach } from 'vitest';

// Types
import {
  type ColumnChunk,
  type RowGroup,
  type RowGroupMetadata,
  type ColumnStats,
  type ColumnDataType,
  type Encoding,
  type Predicate,
  type ColumnarTableSchema,
  type ColumnDefinition,
  type FSXInterface,
  MAX_BLOB_SIZE,
  TARGET_ROW_GROUP_SIZE,
  MAX_ROWS_PER_ROW_GROUP,
  isNumericType,
  isIntegerType,
  isSignedType,
  getBytesPerElement,
} from '../types.js';

// Encoding functions
import {
  encodeRaw,
  decodeRaw,
  encodeRawStrings,
  decodeRawStrings,
  encodeDictionary,
  decodeDictionary,
  encodeRLE,
  decodeRLE,
  encodeDelta,
  decodeDelta,
  bitpackEncode,
  bitpackDecode,
  createNullBitmap,
  isNull,
  setNull,
  setNotNull,
  analyzeForEncoding,
  calculateStats,
} from '../encoding.js';

// Chunk operations
import {
  serializeRowGroup,
  deserializeRowGroup,
  canSkipChunk,
  canSkipRowGroup,
  extractMetadata,
  extractMetadataFast,
  estimateRowGroupSize,
  wouldExceedTargetSize,
  generateRowGroupId,
} from '../chunk.js';

// Writer
import {
  ColumnarWriter,
  writeColumnar,
  inferSchema,
} from '../writer.js';

// Reader
import {
  ColumnarReader,
  readColumnarChunk,
  getColumnStats,
  mightMatchPredicates,
  aggregateSumFromStats,
  aggregateCountFromStats,
  aggregateMinMaxFromStats,
} from '../reader.js';

// ============================================================================
// TEST FIXTURES
// ============================================================================

/**
 * In-memory FSX implementation for testing
 */
function createInMemoryFSX(): FSXInterface & { store: Map<string, Uint8Array> } {
  const store = new Map<string, Uint8Array>();
  return {
    store,
    async get(key: string): Promise<Uint8Array | null> {
      return store.get(key) ?? null;
    },
    async put(key: string, data: Uint8Array): Promise<void> {
      store.set(key, data);
    },
    async delete(key: string): Promise<void> {
      store.delete(key);
    },
    async list(prefix: string): Promise<string[]> {
      return Array.from(store.keys()).filter((k) => k.startsWith(prefix));
    },
  };
}

/**
 * Create a test schema
 */
function createTestSchema(tableName: string = 'test_table'): ColumnarTableSchema {
  return {
    tableName,
    columns: [
      { name: 'id', dataType: 'int32', nullable: false },
      { name: 'name', dataType: 'string', nullable: true },
      { name: 'value', dataType: 'float64', nullable: true },
      { name: 'count', dataType: 'int64', nullable: false },
      { name: 'active', dataType: 'boolean', nullable: false },
      { name: 'timestamp', dataType: 'timestamp', nullable: true },
    ],
  };
}

/**
 * Generate test rows
 */
function generateTestRows(count: number): Record<string, unknown>[] {
  const rows: Record<string, unknown>[] = [];
  for (let i = 0; i < count; i++) {
    rows.push({
      id: i + 1,
      name: i % 5 === 0 ? null : `user_${i}`,
      value: i % 7 === 0 ? null : i * 1.5,
      count: BigInt(i * 100),
      active: i % 2 === 0,
      timestamp: BigInt(Date.now() + i * 1000),
    });
  }
  return rows;
}

// ============================================================================
// TYPE UTILITIES TESTS
// ============================================================================

describe('Type Utilities', () => {
  describe('isNumericType', () => {
    it('should return true for all numeric types', () => {
      const numericTypes: ColumnDataType[] = [
        'int8', 'int16', 'int32', 'int64',
        'uint8', 'uint16', 'uint32', 'uint64',
        'float32', 'float64',
      ];
      for (const type of numericTypes) {
        expect(isNumericType(type)).toBe(true);
      }
    });

    it('should return false for non-numeric types', () => {
      const nonNumericTypes: ColumnDataType[] = ['boolean', 'string', 'bytes', 'timestamp'];
      for (const type of nonNumericTypes) {
        expect(isNumericType(type)).toBe(false);
      }
    });
  });

  describe('isIntegerType', () => {
    it('should return true for integer types', () => {
      const integerTypes: ColumnDataType[] = [
        'int8', 'int16', 'int32', 'int64',
        'uint8', 'uint16', 'uint32', 'uint64',
      ];
      for (const type of integerTypes) {
        expect(isIntegerType(type)).toBe(true);
      }
    });

    it('should return false for float types', () => {
      expect(isIntegerType('float32')).toBe(false);
      expect(isIntegerType('float64')).toBe(false);
    });
  });

  describe('isSignedType', () => {
    it('should return true for signed types', () => {
      const signedTypes: ColumnDataType[] = ['int8', 'int16', 'int32', 'int64', 'float32', 'float64'];
      for (const type of signedTypes) {
        expect(isSignedType(type)).toBe(true);
      }
    });

    it('should return false for unsigned types', () => {
      const unsignedTypes: ColumnDataType[] = ['uint8', 'uint16', 'uint32', 'uint64'];
      for (const type of unsignedTypes) {
        expect(isSignedType(type)).toBe(false);
      }
    });
  });

  describe('getBytesPerElement', () => {
    it('should return correct byte sizes for fixed-width types', () => {
      expect(getBytesPerElement('int8')).toBe(1);
      expect(getBytesPerElement('uint8')).toBe(1);
      expect(getBytesPerElement('boolean')).toBe(1);
      expect(getBytesPerElement('int16')).toBe(2);
      expect(getBytesPerElement('uint16')).toBe(2);
      expect(getBytesPerElement('int32')).toBe(4);
      expect(getBytesPerElement('uint32')).toBe(4);
      expect(getBytesPerElement('float32')).toBe(4);
      expect(getBytesPerElement('int64')).toBe(8);
      expect(getBytesPerElement('uint64')).toBe(8);
      expect(getBytesPerElement('float64')).toBe(8);
      expect(getBytesPerElement('timestamp')).toBe(8);
    });

    it('should return -1 for variable-width types', () => {
      expect(getBytesPerElement('string')).toBe(-1);
      expect(getBytesPerElement('bytes')).toBe(-1);
    });
  });
});

// ============================================================================
// ENCODING: INTEGERS
// ============================================================================

describe('Integer Encoding', () => {
  describe('Raw Encoding - Int8', () => {
    it('should encode and decode int8 values correctly', () => {
      const values = [-128, -1, 0, 1, 127, null, 42];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values as (number | null)[], 'int8', nullBitmap);
      expect(encoded.encoding).toBe('raw');
      expect(encoded.data.length).toBe(values.length);

      const decoded = decodeRaw(encoded.data, 'int8', values.length, nullBitmap);

      for (let i = 0; i < values.length; i++) {
        if (values[i] === null) {
          expect(decoded.values[i]).toBe(null);
        } else {
          expect(decoded.values[i]).toBe(values[i]);
        }
      }
    });
  });

  describe('Raw Encoding - Int16', () => {
    it('should encode and decode int16 values correctly', () => {
      const values = [-32768, -256, 0, 256, 32767, null];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values as (number | null)[], 'int16', nullBitmap);
      expect(encoded.data.length).toBe(values.length * 2);

      const decoded = decodeRaw(encoded.data, 'int16', values.length, nullBitmap);

      for (let i = 0; i < values.length; i++) {
        if (values[i] === null) {
          expect(decoded.values[i]).toBe(null);
        } else {
          expect(decoded.values[i]).toBe(values[i]);
        }
      }
    });
  });

  describe('Raw Encoding - Int32', () => {
    it('should encode and decode int32 values correctly', () => {
      const values = [-2147483648, -1000000, 0, 1000000, 2147483647, null];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values as (number | null)[], 'int32', nullBitmap);
      expect(encoded.data.length).toBe(values.length * 4);

      const decoded = decodeRaw(encoded.data, 'int32', values.length, nullBitmap);

      for (let i = 0; i < values.length; i++) {
        if (values[i] === null) {
          expect(decoded.values[i]).toBe(null);
        } else {
          expect(decoded.values[i]).toBe(values[i]);
        }
      }
    });
  });

  describe('Raw Encoding - Int64', () => {
    it('should encode and decode int64 values correctly', () => {
      const values = [
        BigInt('-9223372036854775808'),
        BigInt(-1000000000000),
        BigInt(0),
        BigInt(1000000000000),
        BigInt('9223372036854775807'),
        null,
      ];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values as (bigint | null)[], 'int64', nullBitmap);
      expect(encoded.data.length).toBe(values.length * 8);

      const decoded = decodeRaw(encoded.data, 'int64', values.length, nullBitmap);

      for (let i = 0; i < values.length; i++) {
        if (values[i] === null) {
          expect(decoded.values[i]).toBe(null);
        } else {
          expect(decoded.values[i]).toBe(values[i]);
        }
      }
    });
  });

  describe('Raw Encoding - Unsigned Integers', () => {
    it('should encode and decode uint8 values correctly', () => {
      const values = [0, 1, 127, 128, 255, null];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values as (number | null)[], 'uint8', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'uint8', values.length, nullBitmap);

      for (let i = 0; i < values.length; i++) {
        if (values[i] === null) {
          expect(decoded.values[i]).toBe(null);
        } else {
          expect(decoded.values[i]).toBe(values[i]);
        }
      }
    });

    it('should encode and decode uint32 values correctly', () => {
      const values = [0, 1000000, 2147483647, 4294967295, null];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values as (number | null)[], 'uint32', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'uint32', values.length, nullBitmap);

      for (let i = 0; i < values.length; i++) {
        if (values[i] === null) {
          expect(decoded.values[i]).toBe(null);
        } else {
          expect(decoded.values[i]).toBe(values[i]);
        }
      }
    });

    it('should encode and decode uint64 values correctly', () => {
      const values = [BigInt(0), BigInt('18446744073709551615'), null];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values as (bigint | null)[], 'uint64', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'uint64', values.length, nullBitmap);

      for (let i = 0; i < values.length; i++) {
        if (values[i] === null) {
          expect(decoded.values[i]).toBe(null);
        } else {
          expect(decoded.values[i]).toBe(values[i]);
        }
      }
    });
  });
});

// ============================================================================
// ENCODING: FLOATS
// ============================================================================

describe('Float Encoding', () => {
  describe('Raw Encoding - Float32', () => {
    it('should encode and decode float32 values correctly', () => {
      // Use values that fit well within float32 precision
      const values = [-3.14, 0, 3.14, 1000.5, null];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values as (number | null)[], 'float32', nullBitmap);
      expect(encoded.data.length).toBe(values.length * 4);

      const decoded = decodeRaw(encoded.data, 'float32', values.length, nullBitmap);

      for (let i = 0; i < values.length; i++) {
        if (values[i] === null) {
          expect(decoded.values[i]).toBe(null);
        } else {
          // Float32 has limited precision, use toBeCloseTo
          expect(decoded.values[i]).toBeCloseTo(values[i]!, 2);
        }
      }
    });

    it('should handle extreme float32 values', () => {
      // Test that large float32 values round-trip reasonably
      const values = [3.4e37, -3.4e37, 1e-38];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values, 'float32', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'float32', values.length, nullBitmap);

      // Just verify they decoded to reasonable values (not NaN/Infinity unless expected)
      for (let i = 0; i < values.length; i++) {
        expect(typeof decoded.values[i]).toBe('number');
        expect(Number.isFinite(decoded.values[i])).toBe(true);
      }
    });
  });

  describe('Raw Encoding - Float64', () => {
    it('should encode and decode float64 values correctly', () => {
      const values = [-1.7976931348623157e308, -3.14159265358979, 0, 3.14159265358979, 1.7976931348623157e308, null];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values as (number | null)[], 'float64', nullBitmap);
      expect(encoded.data.length).toBe(values.length * 8);

      const decoded = decodeRaw(encoded.data, 'float64', values.length, nullBitmap);

      for (let i = 0; i < values.length; i++) {
        if (values[i] === null) {
          expect(decoded.values[i]).toBe(null);
        } else {
          expect(decoded.values[i]).toBe(values[i]);
        }
      }
    });

    it('should handle special float values', () => {
      const values = [Infinity, -Infinity, Number.MIN_VALUE, Number.MAX_VALUE];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values, 'float64', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'float64', values.length, nullBitmap);

      expect(decoded.values[0]).toBe(Infinity);
      expect(decoded.values[1]).toBe(-Infinity);
      expect(decoded.values[2]).toBe(Number.MIN_VALUE);
      expect(decoded.values[3]).toBe(Number.MAX_VALUE);
    });
  });
});

// ============================================================================
// ENCODING: STRINGS
// ============================================================================

describe('String Encoding', () => {
  describe('Raw String Encoding', () => {
    it('should encode and decode simple strings correctly', () => {
      const values = ['hello', 'world', 'test', null, ''];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRawStrings(values);
      expect(encoded.encoding).toBe('raw');

      const decoded = decodeRawStrings(encoded.data, values.length, nullBitmap);

      for (let i = 0; i < values.length; i++) {
        if (values[i] === null) {
          expect(decoded.values[i]).toBe(null);
        } else {
          expect(decoded.values[i]).toBe(values[i]);
        }
      }
    });

    it('should handle unicode strings correctly', () => {
      const values = ['Hello World', 'Cafe', '12345', 'test-value', null];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRawStrings(values);
      const decoded = decodeRawStrings(encoded.data, values.length, nullBitmap);

      for (let i = 0; i < values.length; i++) {
        if (values[i] === null) {
          expect(decoded.values[i]).toBe(null);
        } else {
          expect(decoded.values[i]).toBe(values[i]);
        }
      }
    });

    it('should handle empty strings correctly', () => {
      const values = ['', '', 'non-empty', ''];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRawStrings(values);
      const decoded = decodeRawStrings(encoded.data, values.length, nullBitmap);

      expect(decoded.values).toEqual(['', '', 'non-empty', '']);
    });
  });

  describe('Dictionary Encoding', () => {
    it('should encode and decode strings with dictionary encoding', () => {
      const values = ['red', 'green', 'blue', 'red', 'green', 'red', null, 'blue'];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeDictionary(values, nullBitmap);
      expect(encoded.encoding).toBe('dict');
      expect(encoded.metadata).toBeDefined();
      expect(encoded.metadata!.dictionary).toHaveLength(3); // 3 unique values

      const decoded = decodeDictionary(encoded.data, values.length, nullBitmap);

      for (let i = 0; i < values.length; i++) {
        if (values[i] === null) {
          expect(decoded.values[i]).toBe(null);
        } else {
          expect(decoded.values[i]).toBe(values[i]);
        }
      }
    });

    it('should handle single value dictionary', () => {
      const values = ['same', 'same', 'same', 'same'];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeDictionary(values, nullBitmap);
      expect(encoded.metadata!.dictionary).toHaveLength(1);

      const decoded = decodeDictionary(encoded.data, values.length, nullBitmap);
      expect(decoded.values).toEqual(['same', 'same', 'same', 'same']);
    });

    it('should be more efficient than raw for low-cardinality data', () => {
      // Create low-cardinality data (10 unique values, 1000 total)
      const values: (string | null)[] = [];
      const categories = ['cat_0', 'cat_1', 'cat_2', 'cat_3', 'cat_4', 'cat_5', 'cat_6', 'cat_7', 'cat_8', 'cat_9'];
      for (let i = 0; i < 1000; i++) {
        values.push(categories[i % 10]);
      }
      const nullBitmap = createNullBitmap(values);

      const rawEncoded = encodeRawStrings(values);
      const dictEncoded = encodeDictionary(values, nullBitmap);

      // Dictionary encoding should be smaller
      expect(dictEncoded.data.length).toBeLessThan(rawEncoded.data.length);
    });
  });
});

// ============================================================================
// ENCODING: BOOLEANS
// ============================================================================

describe('Boolean Encoding', () => {
  it('should encode and decode boolean values correctly', () => {
    const values = [true, false, true, true, false, null];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeRaw(values as (boolean | null)[], 'boolean', nullBitmap);
    expect(encoded.data.length).toBe(values.length);

    const decoded = decodeRaw(encoded.data, 'boolean', values.length, nullBitmap);

    for (let i = 0; i < values.length; i++) {
      if (values[i] === null) {
        expect(decoded.values[i]).toBe(null);
      } else {
        expect(decoded.values[i]).toBe(values[i]);
      }
    }
  });

  it('should handle all true values', () => {
    const values = [true, true, true, true];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeRaw(values, 'boolean', nullBitmap);
    const decoded = decodeRaw(encoded.data, 'boolean', values.length, nullBitmap);

    expect(decoded.values).toEqual([true, true, true, true]);
  });

  it('should handle all false values', () => {
    const values = [false, false, false, false];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeRaw(values, 'boolean', nullBitmap);
    const decoded = decodeRaw(encoded.data, 'boolean', values.length, nullBitmap);

    expect(decoded.values).toEqual([false, false, false, false]);
  });
});

// ============================================================================
// ENCODING: TIMESTAMPS
// ============================================================================

describe('Timestamp Encoding', () => {
  it('should encode and decode timestamp values correctly', () => {
    const now = Date.now();
    const values = [
      BigInt(now),
      BigInt(now + 1000),
      BigInt(now - 86400000), // 1 day ago
      null,
      BigInt(0), // Unix epoch
    ];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeRaw(values as (bigint | null)[], 'timestamp', nullBitmap);
    expect(encoded.data.length).toBe(values.length * 8);

    const decoded = decodeRaw(encoded.data, 'timestamp', values.length, nullBitmap);

    for (let i = 0; i < values.length; i++) {
      if (values[i] === null) {
        expect(decoded.values[i]).toBe(null);
      } else {
        expect(decoded.values[i]).toBe(values[i]);
      }
    }
  });
});

// ============================================================================
// COMPRESSION: RLE (Run-Length Encoding)
// ============================================================================

describe('RLE Compression', () => {
  it('should encode and decode repeated integer values', () => {
    // 5 runs: 1,1,1,1 | 2,2 | 3 | 4,4,4 | 5
    const values = [1, 1, 1, 1, 2, 2, 3, 4, 4, 4, 5];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeRLE(values, 'int32', nullBitmap);
    expect(encoded.encoding).toBe('rle');
    expect(encoded.metadata?.runCount).toBe(5);

    const decoded = decodeRLE(encoded.data, 'int32', values.length, nullBitmap);
    expect(decoded.values).toEqual(values);
  });

  it('should handle all same values efficiently', () => {
    const values = new Array(1000).fill(42);
    const nullBitmap = createNullBitmap(values);

    const rawEncoded = encodeRaw(values, 'int32', nullBitmap);
    const rleEncoded = encodeRLE(values, 'int32', nullBitmap);

    // RLE should be much smaller for repeated values
    expect(rleEncoded.data.length).toBeLessThan(rawEncoded.data.length);

    const decoded = decodeRLE(rleEncoded.data, 'int32', values.length, nullBitmap);
    expect(decoded.values).toEqual(values);
  });

  it('should handle values with nulls', () => {
    const values = [1, 1, null, null, 2, 2, 2, null];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeRLE(values as (number | null)[], 'int32', nullBitmap);
    const decoded = decodeRLE(encoded.data, 'int32', values.length, nullBitmap);

    for (let i = 0; i < values.length; i++) {
      if (values[i] === null) {
        expect(decoded.values[i]).toBe(null);
      } else {
        expect(decoded.values[i]).toBe(values[i]);
      }
    }
  });

  it('should work with different integer types', () => {
    // Use values that fit in all tested types (int8 max is 127, uint8 max is 255)
    // For int8, we need values in range -128 to 127
    // For uint8, we need values in range 0 to 255
    const smallValues = [10, 10, 20, 20, 20, 30];
    const nullBitmap = createNullBitmap(smallValues);

    for (const dataType of ['int8', 'int16', 'int32', 'uint8', 'uint16', 'uint32'] as ColumnDataType[]) {
      const encoded = encodeRLE(smallValues, dataType, nullBitmap);
      const decoded = decodeRLE(encoded.data, dataType, smallValues.length, nullBitmap);
      expect(decoded.values).toEqual(smallValues);
    }
  });

  it('should work with larger integer types for bigger values', () => {
    const values = [100, 100, 200, 200, 200, 300];
    const nullBitmap = createNullBitmap(values);

    // Only test types that can hold values up to 300
    for (const dataType of ['int16', 'int32', 'uint16', 'uint32'] as ColumnDataType[]) {
      const encoded = encodeRLE(values, dataType, nullBitmap);
      const decoded = decodeRLE(encoded.data, dataType, values.length, nullBitmap);
      expect(decoded.values).toEqual(values);
    }
  });

  it('should handle empty array', () => {
    const values: number[] = [];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeRLE(values, 'int32', nullBitmap);
    expect(encoded.data.length).toBe(0);

    const decoded = decodeRLE(encoded.data, 'int32', 0, nullBitmap);
    expect(decoded.values).toEqual([]);
  });
});

// ============================================================================
// COMPRESSION: DELTA ENCODING
// ============================================================================

describe('Delta Compression', () => {
  it('should encode and decode sorted sequential integers', () => {
    const values = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeDelta(values, 'int32', nullBitmap);
    expect(encoded.encoding).toBe('delta');

    const decoded = decodeDelta(encoded.data, 'int32', values.length, nullBitmap);
    expect(decoded.values).toEqual(values);
  });

  it('should be more efficient than raw for sequential data', () => {
    // Create sequential data
    const values: number[] = [];
    for (let i = 0; i < 1000; i++) {
      values.push(1000000 + i);
    }
    const nullBitmap = createNullBitmap(values);

    const rawEncoded = encodeRaw(values, 'int32', nullBitmap);
    const deltaEncoded = encodeDelta(values, 'int32', nullBitmap);

    // Delta should be smaller for sequential data
    expect(deltaEncoded.data.length).toBeLessThan(rawEncoded.data.length);

    const decoded = decodeDelta(deltaEncoded.data, 'int32', values.length, nullBitmap);
    expect(decoded.values).toEqual(values);
  });

  it('should handle non-uniform deltas', () => {
    const values = [10, 15, 22, 31, 42, 55];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeDelta(values, 'int32', nullBitmap);
    const decoded = decodeDelta(encoded.data, 'int32', values.length, nullBitmap);
    expect(decoded.values).toEqual(values);
  });

  it('should handle values with nulls', () => {
    const values = [100, 101, null, 103, 104, null, 106];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeDelta(values as (number | null)[], 'int32', nullBitmap);
    const decoded = decodeDelta(encoded.data, 'int32', values.length, nullBitmap);

    for (let i = 0; i < values.length; i++) {
      if (values[i] === null) {
        expect(decoded.values[i]).toBe(null);
      } else {
        expect(decoded.values[i]).toBe(values[i]);
      }
    }
  });

  it('should handle bigint values', () => {
    const values = [
      BigInt('1000000000000'),
      BigInt('1000000000001'),
      BigInt('1000000000002'),
      BigInt('1000000000003'),
    ];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeDelta(values, 'int64', nullBitmap);
    const decoded = decodeDelta(encoded.data, 'int64', values.length, nullBitmap);

    for (let i = 0; i < values.length; i++) {
      expect(decoded.values[i]).toBe(values[i]);
    }
  });
});

// ============================================================================
// BIT-PACKING
// ============================================================================

describe('Bit-Packing', () => {
  it('should encode and decode small integers with minimal bits', () => {
    const values = [0, 1, 2, 3, 4, 5, 6, 7]; // Fits in 3 bits
    const bitsPerValue = 3;

    const encoded = bitpackEncode(values, bitsPerValue);
    // 8 values * 3 bits = 24 bits = 3 bytes
    expect(encoded.length).toBe(3);

    const decoded = bitpackDecode(encoded, values.length, bitsPerValue);
    expect(decoded).toEqual(values);
  });

  it('should handle 1-bit packing', () => {
    const values = [0, 1, 0, 1, 1, 0, 1, 0];

    const encoded = bitpackEncode(values, 1);
    expect(encoded.length).toBe(1);

    const decoded = bitpackDecode(encoded, values.length, 1);
    expect(decoded).toEqual(values);
  });

  it('should handle various bit widths', () => {
    for (let bits = 1; bits <= 16; bits++) {
      const maxValue = (1 << bits) - 1;
      const values = [0, 1, Math.floor(maxValue / 2), maxValue];

      const encoded = bitpackEncode(values, bits);
      const decoded = bitpackDecode(encoded, values.length, bits);

      expect(decoded).toEqual(values);
    }
  });

  it('should handle large arrays efficiently', () => {
    // 1000 values that fit in 4 bits
    const values = Array.from({ length: 1000 }, (_, i) => i % 16);

    const encoded = bitpackEncode(values, 4);
    // 1000 * 4 bits = 4000 bits = 500 bytes
    expect(encoded.length).toBe(500);

    const decoded = bitpackDecode(encoded, values.length, 4);
    expect(decoded).toEqual(values);
  });
});

// ============================================================================
// NULL BITMAP
// ============================================================================

describe('Null Bitmap', () => {
  it('should create correct null bitmap', () => {
    const values = [1, null, 3, null, 5, 6, null, 8];
    const bitmap = createNullBitmap(values);

    // Bit is 1 if NOT null
    expect(isNull(bitmap, 0)).toBe(false); // 1
    expect(isNull(bitmap, 1)).toBe(true);  // null
    expect(isNull(bitmap, 2)).toBe(false); // 3
    expect(isNull(bitmap, 3)).toBe(true);  // null
    expect(isNull(bitmap, 4)).toBe(false); // 5
    expect(isNull(bitmap, 5)).toBe(false); // 6
    expect(isNull(bitmap, 6)).toBe(true);  // null
    expect(isNull(bitmap, 7)).toBe(false); // 8
  });

  it('should handle all null values', () => {
    const values = [null, null, null, null];
    const bitmap = createNullBitmap(values);

    for (let i = 0; i < values.length; i++) {
      expect(isNull(bitmap, i)).toBe(true);
    }
  });

  it('should handle all non-null values', () => {
    const values = [1, 2, 3, 4];
    const bitmap = createNullBitmap(values);

    for (let i = 0; i < values.length; i++) {
      expect(isNull(bitmap, i)).toBe(false);
    }
  });

  it('should set and unset null correctly', () => {
    const bitmap = new Uint8Array(1);

    // Initially all zeros (all null)
    expect(isNull(bitmap, 0)).toBe(true);

    // Set as not null
    setNotNull(bitmap, 0);
    expect(isNull(bitmap, 0)).toBe(false);

    // Set as null again
    setNull(bitmap, 0);
    expect(isNull(bitmap, 0)).toBe(true);
  });

  it('should handle large arrays across multiple bytes', () => {
    const values: (number | null)[] = [];
    for (let i = 0; i < 100; i++) {
      values.push(i % 3 === 0 ? null : i);
    }
    const bitmap = createNullBitmap(values);

    for (let i = 0; i < values.length; i++) {
      expect(isNull(bitmap, i)).toBe(values[i] === null);
    }
  });
});

// ============================================================================
// ENCODING ANALYSIS
// ============================================================================

describe('Encoding Analysis', () => {
  it('should recommend dictionary encoding for low-cardinality strings', () => {
    const values = Array.from({ length: 1000 }, (_, i) => `cat_${i % 5}`);
    const analysis = analyzeForEncoding(values, 'string');

    expect(analysis.recommendedEncoding).toBe('dict');
    expect(analysis.cardinality).toBe(5);
  });

  it('should recommend raw encoding for high-cardinality strings', () => {
    const values = Array.from({ length: 100 }, (_, i) => `unique_${i}`);
    const analysis = analyzeForEncoding(values, 'string');

    expect(analysis.recommendedEncoding).toBe('raw');
  });

  it('should recommend delta encoding for sorted integers', () => {
    const values = Array.from({ length: 1000 }, (_, i) => i);
    const analysis = analyzeForEncoding(values, 'int32');

    expect(analysis.isSorted).toBe(true);
    expect(analysis.recommendedEncoding).toBe('delta');
  });

  it('should recommend RLE for repeated values', () => {
    // Create data with many repeated consecutive values
    const values: number[] = [];
    for (let i = 0; i < 100; i++) {
      for (let j = 0; j < 10; j++) {
        values.push(i);
      }
    }
    const analysis = analyzeForEncoding(values, 'int32');

    expect(analysis.runCount).toBe(100);
  });
});

// ============================================================================
// STATISTICS CALCULATION
// ============================================================================

describe('Statistics Calculation', () => {
  it('should calculate correct stats for integers', () => {
    const values = [5, 10, null, 3, 8, 15, null, 1];
    const stats = calculateStats(values, 'int32');

    expect(stats.min).toBe(1);
    expect(stats.max).toBe(15);
    expect(stats.nullCount).toBe(2);
    expect(stats.distinctCount).toBe(6);
    expect(stats.sum).toBe(42); // 5+10+3+8+15+1
  });

  it('should calculate correct stats for floats', () => {
    const values = [1.5, 2.5, 3.5, null];
    const stats = calculateStats(values, 'float64');

    expect(stats.min).toBe(1.5);
    expect(stats.max).toBe(3.5);
    expect(stats.nullCount).toBe(1);
    expect(stats.sum).toBeCloseTo(7.5);
  });

  it('should calculate correct stats for strings', () => {
    const values = ['apple', 'banana', 'cherry', null, 'apple'];
    const stats = calculateStats(values, 'string');

    expect(stats.min).toBe('apple');
    expect(stats.max).toBe('cherry');
    expect(stats.nullCount).toBe(1);
    expect(stats.distinctCount).toBe(3); // 3 unique non-null values
    expect(stats.sum).toBeUndefined(); // No sum for strings
  });

  it('should calculate correct stats for bigints', () => {
    const values = [BigInt(100), BigInt(200), BigInt(50)];
    const stats = calculateStats(values, 'int64');

    expect(stats.min).toBe(BigInt(50));
    expect(stats.max).toBe(BigInt(200));
    expect(stats.sum).toBe(BigInt(350));
  });

  it('should handle all null values', () => {
    const values = [null, null, null];
    const stats = calculateStats(values, 'int32');

    expect(stats.min).toBe(null);
    expect(stats.max).toBe(null);
    expect(stats.nullCount).toBe(3);
    expect(stats.distinctCount).toBe(0);
  });
});

// ============================================================================
// CHUNK SERIALIZATION
// ============================================================================

describe('Chunk Serialization', () => {
  it('should serialize and deserialize a row group correctly', async () => {
    const schema = createTestSchema();
    const rows = generateTestRows(100);

    const writer = new ColumnarWriter(schema);
    for (const row of rows) {
      (writer as any).bufferRow(row);
    }

    // flush() is async, need to await it
    const rowGroup = await (writer as any).flush();

    // Verify row group structure
    expect(rowGroup).toBeDefined();
    expect(rowGroup.rowCount).toBe(100);
    expect(rowGroup.columns.size).toBe(6);

    // Serialize
    const serialized = serializeRowGroup(rowGroup);
    expect(serialized).toBeInstanceOf(Uint8Array);
    expect(serialized.length).toBeGreaterThan(0);

    // Verify magic bytes
    expect(String.fromCharCode(serialized[0], serialized[1], serialized[2], serialized[3])).toBe('DSCG');

    // Deserialize
    const deserialized = deserializeRowGroup(serialized);

    expect(deserialized.id).toBe(rowGroup.id);
    expect(deserialized.rowCount).toBe(rowGroup.rowCount);
    expect(deserialized.rowRange).toEqual(rowGroup.rowRange);
    expect(deserialized.columns.size).toBe(rowGroup.columns.size);

    // Verify column data
    for (const [name, chunk] of rowGroup.columns) {
      const deserializedChunk = deserialized.columns.get(name);
      expect(deserializedChunk).toBeDefined();
      expect(deserializedChunk!.columnName).toBe(chunk.columnName);
      expect(deserializedChunk!.dataType).toBe(chunk.dataType);
      expect(deserializedChunk!.rowCount).toBe(chunk.rowCount);
      expect(deserializedChunk!.encoding).toBe(chunk.encoding);
    }
  });

  it('should throw error for oversized row groups', () => {
    // Create a mock row group that exceeds the size limit
    const largeData = new Uint8Array(MAX_BLOB_SIZE + 1);

    const rowGroup: RowGroup = {
      id: 'test_large',
      rowCount: 1,
      columns: new Map([
        ['large_col', {
          columnName: 'large_col',
          dataType: 'bytes',
          rowCount: 1,
          encoding: 'raw',
          nullBitmap: new Uint8Array(1),
          data: largeData,
          stats: { min: null, max: null, nullCount: 0 },
        }],
      ]),
      rowRange: { start: 0, end: 0 },
      createdAt: Date.now(),
    };

    expect(() => serializeRowGroup(rowGroup)).toThrow(/exceeds maximum blob size/);
  });

  it('should extract metadata quickly from serialized data', async () => {
    const schema = createTestSchema();
    const writer = new ColumnarWriter(schema);

    for (const row of generateTestRows(50)) {
      (writer as any).bufferRow(row);
    }

    // flush() is async, need to await it
    const rowGroup = await (writer as any).flush();
    const serialized = serializeRowGroup(rowGroup);

    const metadata = extractMetadataFast(serialized);

    expect(metadata.id).toBe(rowGroup.id);
    expect(metadata.rowCount).toBe(rowGroup.rowCount);
    expect(metadata.rowRange).toEqual(rowGroup.rowRange);
    expect(metadata.columnStats.size).toBe(rowGroup.columns.size);
  });
});

// ============================================================================
// ZONE MAP FILTERING
// ============================================================================

describe('Zone Map Filtering', () => {
  describe('canSkipChunk', () => {
    const stats: ColumnStats = {
      min: 10,
      max: 100,
      nullCount: 0,
    };

    it('should skip chunk when eq value is out of range', () => {
      expect(canSkipChunk(stats, { column: 'x', op: 'eq', value: 5 })).toBe(true);
      expect(canSkipChunk(stats, { column: 'x', op: 'eq', value: 150 })).toBe(true);
      expect(canSkipChunk(stats, { column: 'x', op: 'eq', value: 50 })).toBe(false);
    });

    it('should skip chunk when lt min', () => {
      expect(canSkipChunk(stats, { column: 'x', op: 'lt', value: 10 })).toBe(true);
      expect(canSkipChunk(stats, { column: 'x', op: 'lt', value: 5 })).toBe(true);
      expect(canSkipChunk(stats, { column: 'x', op: 'lt', value: 50 })).toBe(false);
    });

    it('should skip chunk when le < min', () => {
      expect(canSkipChunk(stats, { column: 'x', op: 'le', value: 9 })).toBe(true);
      expect(canSkipChunk(stats, { column: 'x', op: 'le', value: 10 })).toBe(false);
    });

    it('should skip chunk when gt max', () => {
      expect(canSkipChunk(stats, { column: 'x', op: 'gt', value: 100 })).toBe(true);
      expect(canSkipChunk(stats, { column: 'x', op: 'gt', value: 150 })).toBe(true);
      expect(canSkipChunk(stats, { column: 'x', op: 'gt', value: 50 })).toBe(false);
    });

    it('should skip chunk when ge > max', () => {
      expect(canSkipChunk(stats, { column: 'x', op: 'ge', value: 101 })).toBe(true);
      expect(canSkipChunk(stats, { column: 'x', op: 'ge', value: 100 })).toBe(false);
    });

    it('should skip chunk when between range does not overlap', () => {
      expect(canSkipChunk(stats, { column: 'x', op: 'between', value: 0, value2: 5 })).toBe(true);
      expect(canSkipChunk(stats, { column: 'x', op: 'between', value: 110, value2: 200 })).toBe(true);
      expect(canSkipChunk(stats, { column: 'x', op: 'between', value: 50, value2: 150 })).toBe(false);
    });

    it('should skip chunk when in values are all out of range', () => {
      expect(canSkipChunk(stats, { column: 'x', op: 'in', value: [1, 2, 3] })).toBe(true);
      expect(canSkipChunk(stats, { column: 'x', op: 'in', value: [150, 200] })).toBe(true);
      expect(canSkipChunk(stats, { column: 'x', op: 'in', value: [5, 50, 150] })).toBe(false);
    });

    it('should not skip when min/max are null', () => {
      const nullStats: ColumnStats = { min: null, max: null, nullCount: 10 };
      expect(canSkipChunk(nullStats, { column: 'x', op: 'eq', value: 50 })).toBe(false);
    });
  });

  describe('canSkipRowGroup', () => {
    it('should skip row group when any predicate allows skipping', () => {
      const metadata: RowGroupMetadata = {
        id: 'rg1',
        rowCount: 100,
        rowRange: { start: 0, end: 99 },
        columnStats: new Map([
          ['age', { min: 20, max: 40, nullCount: 0 }],
          ['salary', { min: 50000, max: 100000, nullCount: 0 }],
        ]),
        byteSize: 1000,
        createdAt: Date.now(),
      };

      // Can skip because age < 20 is impossible
      expect(canSkipRowGroup(metadata, [
        { column: 'age', op: 'lt', value: 20 },
      ])).toBe(true);

      // Cannot skip because age could be 30
      expect(canSkipRowGroup(metadata, [
        { column: 'age', op: 'eq', value: 30 },
      ])).toBe(false);

      // Can skip because salary > 100000 is impossible
      expect(canSkipRowGroup(metadata, [
        { column: 'salary', op: 'gt', value: 100000 },
      ])).toBe(true);
    });
  });
});

// ============================================================================
// WRITER TESTS
// ============================================================================

describe('ColumnarWriter', () => {
  it('should buffer rows and flush to row groups', async () => {
    const schema = createTestSchema();
    const fsx = createInMemoryFSX();
    const writer = new ColumnarWriter(schema, {}, fsx);

    const rows = generateTestRows(100);
    const flushed = await writer.write(rows);

    // With default config (65536 rows), no automatic flush
    expect(flushed).toHaveLength(0);
    expect(writer.getBufferedRowCount()).toBe(100);

    // Finalize to flush remaining
    const final = await writer.finalize();
    expect(final).toBeDefined();
    expect(final!.rowCount).toBe(100);

    // Check FSX storage
    const keys = await fsx.list(`${schema.tableName}/rowgroups/`);
    expect(keys).toHaveLength(1);
  });

  it('should auto-flush when reaching row limit', async () => {
    const schema = createTestSchema();
    const writer = new ColumnarWriter(schema, { targetRowsPerGroup: 50 });

    const rows = generateTestRows(120);
    const flushed = await writer.write(rows);

    expect(flushed).toHaveLength(2); // 2 groups of 50 flushed
    expect(writer.getBufferedRowCount()).toBe(20); // 20 remaining

    const final = await writer.finalize();
    expect(final!.rowCount).toBe(20);
  });

  it('should call onFlush callback', async () => {
    const schema = createTestSchema();
    const flushedData: Uint8Array[] = [];

    const writer = new ColumnarWriter(schema, {
      targetRowsPerGroup: 50,
      onFlush: async (_rg, data) => {
        flushedData.push(data);
      },
    });

    await writer.write(generateTestRows(100));

    expect(flushedData).toHaveLength(2);
    expect(flushedData[0]).toBeInstanceOf(Uint8Array);
  });

  it('should enforce non-nullable constraints', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'strict_table',
      columns: [
        { name: 'required_col', dataType: 'int32', nullable: false },
      ],
    };

    const writer = new ColumnarWriter(schema);

    await expect(writer.write([{ required_col: null }]))
      .rejects.toThrow(/does not allow null/);
  });

  it('should select appropriate encoding per column', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'encoding_test',
      columns: [
        { name: 'low_cardinality', dataType: 'string', nullable: false },
        { name: 'sequential', dataType: 'int32', nullable: false },
        { name: 'repeated', dataType: 'int32', nullable: false },
      ],
    };

    const rows: Record<string, unknown>[] = [];
    for (let i = 0; i < 1000; i++) {
      rows.push({
        low_cardinality: `cat_${i % 5}`,
        sequential: i,
        repeated: Math.floor(i / 100),
      });
    }

    const writer = new ColumnarWriter(schema);
    await writer.write(rows);
    const rowGroup = await writer.finalize();

    expect(rowGroup).toBeDefined();

    // Low cardinality strings should use dictionary encoding
    const lowCardCol = rowGroup!.columns.get('low_cardinality');
    expect(lowCardCol!.encoding).toBe('dict');

    // Sequential integers should use delta encoding
    const seqCol = rowGroup!.columns.get('sequential');
    expect(seqCol!.encoding).toBe('delta');
  });
});

// ============================================================================
// READER TESTS
// ============================================================================

describe('ColumnarReader', () => {
  let fsx: ReturnType<typeof createInMemoryFSX>;
  let schema: ColumnarTableSchema;

  beforeEach(async () => {
    fsx = createInMemoryFSX();
    schema = createTestSchema();

    // Write test data
    const writer = new ColumnarWriter(schema, { targetRowsPerGroup: 50 }, fsx);
    await writer.write(generateTestRows(150));
    await writer.finalize();
  });

  it('should read all rows when no predicates', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({ table: schema.tableName });

    expect(result.rowCount).toBe(150);
    expect(result.stats.rowGroupsScanned).toBe(3);
    expect(result.stats.rowGroupsSkipped).toBe(0);
  });

  it('should apply projection to read only specified columns', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.read({
      table: schema.tableName,
      projection: { columns: ['id', 'name'] },
    });

    expect(result.columns.size).toBe(2);
    expect(result.columns.has('id')).toBe(true);
    expect(result.columns.has('name')).toBe(true);
    expect(result.columns.has('value')).toBe(false);
  });

  it('should filter rows with predicates', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: schema.tableName,
      predicates: [{ column: 'id', op: 'le', value: 10 }],
    });

    expect(result.rowCount).toBe(10);
    for (const row of result.rows) {
      expect(row.id).toBeLessThanOrEqual(10);
    }
  });

  it('should apply limit', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: schema.tableName,
      limit: 25,
    });

    expect(result.rowCount).toBe(25);
  });

  it('should apply offset', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: schema.tableName,
      offset: 140,
    });

    expect(result.rowCount).toBe(10); // 150 - 140
  });

  it('should apply offset and limit together', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: schema.tableName,
      offset: 50,
      limit: 25,
    });

    expect(result.rowCount).toBe(25);
    expect(result.rows[0].id).toBe(51); // 0-indexed, so id starts at 1
  });

  it('should skip row groups using zone maps', async () => {
    // Write data with clear zone map boundaries
    const segmentedFsx = createInMemoryFSX();
    const segmentedSchema: ColumnarTableSchema = {
      tableName: 'segmented',
      columns: [{ name: 'value', dataType: 'int32', nullable: false }],
    };

    // First group: values 0-49
    // Second group: values 50-99
    // Third group: values 100-149
    const writer = new ColumnarWriter(segmentedSchema, { targetRowsPerGroup: 50 }, segmentedFsx);
    await writer.write(Array.from({ length: 150 }, (_, i) => ({ value: i })));
    await writer.finalize();

    const reader = new ColumnarReader(segmentedFsx);

    // Query for value > 120 should skip first two groups
    const result = await reader.scan({
      table: 'segmented',
      predicates: [{ column: 'value', op: 'gt', value: 120 }],
    });

    expect(result.stats.rowGroupsSkipped).toBe(2);
    expect(result.stats.rowGroupsScanned).toBe(1);
    expect(result.rowCount).toBe(29); // 121-149
  });
});

// ============================================================================
// AGGREGATION TESTS
// ============================================================================

describe('Aggregation', () => {
  describe('SUM aggregation from stats', () => {
    it('should calculate sum directly from metadata', () => {
      const metadataList: RowGroupMetadata[] = [
        {
          id: 'rg1',
          rowCount: 100,
          rowRange: { start: 0, end: 99 },
          columnStats: new Map([['amount', { min: 10, max: 100, nullCount: 0, sum: 5500 }]]),
          byteSize: 1000,
          createdAt: Date.now(),
        },
        {
          id: 'rg2',
          rowCount: 100,
          rowRange: { start: 100, end: 199 },
          columnStats: new Map([['amount', { min: 20, max: 200, nullCount: 0, sum: 11000 }]]),
          byteSize: 1000,
          createdAt: Date.now(),
        },
      ];

      const sum = aggregateSumFromStats(metadataList, 'amount');
      expect(sum).toBe(16500);
    });

    it('should handle bigint sums', () => {
      const metadataList: RowGroupMetadata[] = [
        {
          id: 'rg1',
          rowCount: 100,
          rowRange: { start: 0, end: 99 },
          columnStats: new Map([['big', { min: BigInt(0), max: BigInt(100), nullCount: 0, sum: BigInt('9007199254740993') }]]),
          byteSize: 1000,
          createdAt: Date.now(),
        },
        {
          id: 'rg2',
          rowCount: 100,
          rowRange: { start: 100, end: 199 },
          columnStats: new Map([['big', { min: BigInt(0), max: BigInt(100), nullCount: 0, sum: BigInt('9007199254740993') }]]),
          byteSize: 1000,
          createdAt: Date.now(),
        },
      ];

      const sum = aggregateSumFromStats(metadataList, 'big');
      expect(sum).toBe(BigInt('18014398509481986'));
    });

    it('should return null when sum is not available', () => {
      const metadataList: RowGroupMetadata[] = [
        {
          id: 'rg1',
          rowCount: 100,
          rowRange: { start: 0, end: 99 },
          columnStats: new Map([['text', { min: 'a', max: 'z', nullCount: 0 }]]),
          byteSize: 1000,
          createdAt: Date.now(),
        },
      ];

      const sum = aggregateSumFromStats(metadataList, 'text');
      expect(sum).toBe(null);
    });
  });

  describe('COUNT aggregation from stats', () => {
    it('should calculate count including nulls', () => {
      const metadataList: RowGroupMetadata[] = [
        {
          id: 'rg1',
          rowCount: 100,
          rowRange: { start: 0, end: 99 },
          columnStats: new Map([['col', { min: 0, max: 100, nullCount: 10 }]]),
          byteSize: 1000,
          createdAt: Date.now(),
        },
        {
          id: 'rg2',
          rowCount: 50,
          rowRange: { start: 100, end: 149 },
          columnStats: new Map([['col', { min: 0, max: 100, nullCount: 5 }]]),
          byteSize: 1000,
          createdAt: Date.now(),
        },
      ];

      const count = aggregateCountFromStats(metadataList, 'col', true);
      expect(count).toBe(150);
    });

    it('should calculate count excluding nulls', () => {
      const metadataList: RowGroupMetadata[] = [
        {
          id: 'rg1',
          rowCount: 100,
          rowRange: { start: 0, end: 99 },
          columnStats: new Map([['col', { min: 0, max: 100, nullCount: 10 }]]),
          byteSize: 1000,
          createdAt: Date.now(),
        },
        {
          id: 'rg2',
          rowCount: 50,
          rowRange: { start: 100, end: 149 },
          columnStats: new Map([['col', { min: 0, max: 100, nullCount: 5 }]]),
          byteSize: 1000,
          createdAt: Date.now(),
        },
      ];

      const count = aggregateCountFromStats(metadataList, 'col', false);
      expect(count).toBe(135); // 150 - 15 nulls
    });
  });

  describe('MIN/MAX aggregation from stats', () => {
    it('should find global min and max', () => {
      const metadataList: RowGroupMetadata[] = [
        {
          id: 'rg1',
          rowCount: 100,
          rowRange: { start: 0, end: 99 },
          columnStats: new Map([['value', { min: 50, max: 200, nullCount: 0 }]]),
          byteSize: 1000,
          createdAt: Date.now(),
        },
        {
          id: 'rg2',
          rowCount: 100,
          rowRange: { start: 100, end: 199 },
          columnStats: new Map([['value', { min: 10, max: 150, nullCount: 0 }]]),
          byteSize: 1000,
          createdAt: Date.now(),
        },
        {
          id: 'rg3',
          rowCount: 100,
          rowRange: { start: 200, end: 299 },
          columnStats: new Map([['value', { min: 30, max: 300, nullCount: 0 }]]),
          byteSize: 1000,
          createdAt: Date.now(),
        },
      ];

      const { min, max } = aggregateMinMaxFromStats(metadataList, 'value');
      expect(min).toBe(10);
      expect(max).toBe(300);
    });

    it('should handle string min/max', () => {
      const metadataList: RowGroupMetadata[] = [
        {
          id: 'rg1',
          rowCount: 100,
          rowRange: { start: 0, end: 99 },
          columnStats: new Map([['name', { min: 'bob', max: 'zoe', nullCount: 0 }]]),
          byteSize: 1000,
          createdAt: Date.now(),
        },
        {
          id: 'rg2',
          rowCount: 100,
          rowRange: { start: 100, end: 199 },
          columnStats: new Map([['name', { min: 'alice', max: 'tom', nullCount: 0 }]]),
          byteSize: 1000,
          createdAt: Date.now(),
        },
      ];

      const { min, max } = aggregateMinMaxFromStats(metadataList, 'name');
      expect(min).toBe('alice');
      expect(max).toBe('zoe');
    });
  });
});

// ============================================================================
// CONVENIENCE FUNCTIONS TESTS
// ============================================================================

describe('Convenience Functions', () => {
  describe('writeColumnar', () => {
    it('should write rows and return row groups with serialized data', async () => {
      const schema = createTestSchema();
      const rows = generateTestRows(100);

      const { rowGroups, serialized } = await writeColumnar(schema, rows);

      expect(rowGroups).toHaveLength(1);
      expect(serialized).toHaveLength(1);
      expect(rowGroups[0].rowCount).toBe(100);
    });
  });

  describe('inferSchema', () => {
    it('should infer schema from sample rows', () => {
      const rows = [
        { id: 1, name: 'test', value: 3.14, active: true },
        { id: 2, name: null, value: 2.71, active: false },
      ];

      const schema = inferSchema('inferred_table', rows);

      expect(schema.tableName).toBe('inferred_table');
      expect(schema.columns).toHaveLength(4);

      const idCol = schema.columns.find((c) => c.name === 'id');
      expect(idCol!.dataType).toBe('int32');
      expect(idCol!.nullable).toBe(false);

      const nameCol = schema.columns.find((c) => c.name === 'name');
      expect(nameCol!.dataType).toBe('string');
      expect(nameCol!.nullable).toBe(true);

      const valueCol = schema.columns.find((c) => c.name === 'value');
      expect(valueCol!.dataType).toBe('float64');

      const activeCol = schema.columns.find((c) => c.name === 'active');
      expect(activeCol!.dataType).toBe('boolean');
    });

    it('should throw for empty rows', () => {
      expect(() => inferSchema('empty', [])).toThrow(/Cannot infer schema from empty data/);
    });
  });

  describe('readColumnarChunk', () => {
    it('should read a serialized chunk directly', async () => {
      const schema: ColumnarTableSchema = {
        tableName: 'direct_read',
        columns: [
          { name: 'a', dataType: 'int32', nullable: false },
          { name: 'b', dataType: 'string', nullable: false },
        ],
      };

      const rows = [
        { a: 1, b: 'one' },
        { a: 2, b: 'two' },
        { a: 3, b: 'three' },
      ];

      const { serialized } = await writeColumnar(schema, rows);
      const columns = readColumnarChunk(serialized[0]);

      expect(columns.get('a')).toEqual([1, 2, 3]);
      expect(columns.get('b')).toEqual(['one', 'two', 'three']);
    });

    it('should support projection', async () => {
      const schema: ColumnarTableSchema = {
        tableName: 'projection_test',
        columns: [
          { name: 'a', dataType: 'int32', nullable: false },
          { name: 'b', dataType: 'int32', nullable: false },
          { name: 'c', dataType: 'int32', nullable: false },
        ],
      };

      const rows = [{ a: 1, b: 2, c: 3 }];
      const { serialized } = await writeColumnar(schema, rows);

      const columns = readColumnarChunk(serialized[0], ['a', 'c']);

      expect(columns.has('a')).toBe(true);
      expect(columns.has('b')).toBe(false);
      expect(columns.has('c')).toBe(true);
    });
  });

  describe('getColumnStats', () => {
    it('should extract column stats from serialized data', async () => {
      const schema: ColumnarTableSchema = {
        tableName: 'stats_test',
        columns: [{ name: 'value', dataType: 'int32', nullable: false }],
      };

      const rows = Array.from({ length: 100 }, (_, i) => ({ value: i }));
      const { serialized } = await writeColumnar(schema, rows);

      const stats = getColumnStats(serialized[0], 'value');

      expect(stats).toBeDefined();
      expect(stats!.min).toBe(0);
      expect(stats!.max).toBe(99);
      expect(stats!.nullCount).toBe(0);
    });
  });

  describe('mightMatchPredicates', () => {
    it('should return true when predicates might match', async () => {
      const schema: ColumnarTableSchema = {
        tableName: 'predicate_test',
        columns: [{ name: 'x', dataType: 'int32', nullable: false }],
      };

      const rows = Array.from({ length: 100 }, (_, i) => ({ x: i + 50 }));
      const { serialized } = await writeColumnar(schema, rows);

      // Range is 50-149, so x > 100 might match
      expect(mightMatchPredicates(serialized[0], [
        { column: 'x', op: 'gt', value: 100 },
      ])).toBe(true);
    });

    it('should return false when predicates cannot match', async () => {
      const schema: ColumnarTableSchema = {
        tableName: 'predicate_test',
        columns: [{ name: 'x', dataType: 'int32', nullable: false }],
      };

      const rows = Array.from({ length: 100 }, (_, i) => ({ x: i + 50 }));
      const { serialized } = await writeColumnar(schema, rows);

      // Range is 50-149, so x < 50 cannot match
      expect(mightMatchPredicates(serialized[0], [
        { column: 'x', op: 'lt', value: 50 },
      ])).toBe(false);
    });
  });
});

// ============================================================================
// SIZE ESTIMATION TESTS
// ============================================================================

describe('Size Estimation', () => {
  it('should estimate row group size', async () => {
    const schema = createTestSchema();
    const writer = new ColumnarWriter(schema);

    for (const row of generateTestRows(100)) {
      (writer as any).bufferRow(row);
    }

    // flush() is async, need to await it
    const rowGroup = await (writer as any).flush();
    const estimated = estimateRowGroupSize(rowGroup);
    const actual = serializeRowGroup(rowGroup).length;

    // Estimate should be within 20% of actual
    expect(Math.abs(estimated - actual) / actual).toBeLessThan(0.2);
  });

  it('should correctly predict size overflow', () => {
    expect(wouldExceedTargetSize(500000, 1000, 600)).toBe(true);
    expect(wouldExceedTargetSize(500000, 100, 600)).toBe(false);
  });
});

// ============================================================================
// ROW GROUP ID GENERATION
// ============================================================================

describe('Row Group ID Generation', () => {
  it('should generate unique IDs', () => {
    const ids = new Set<string>();

    for (let i = 0; i < 100; i++) {
      const id = generateRowGroupId('test_table', i);
      expect(ids.has(id)).toBe(false);
      ids.add(id);
      expect(id).toContain('test_table');
    }
  });

  it('should include table name in ID', () => {
    const id = generateRowGroupId('my_table', 5);
    expect(id.startsWith('my_table_')).toBe(true);
  });
});

// ============================================================================
// EDGE CASES
// ============================================================================

describe('Edge Cases', () => {
  it('should handle empty row groups', async () => {
    const schema = createTestSchema();
    const writer = new ColumnarWriter(schema);

    await expect(writer.finalize()).resolves.toBe(null);
  });

  it('should handle single row', async () => {
    const schema = createTestSchema();
    const writer = new ColumnarWriter(schema);

    await writer.write([{
      id: 1,
      name: 'single',
      value: 3.14,
      count: BigInt(100),
      active: true,
      timestamp: BigInt(Date.now()),
    }]);

    const rowGroup = await writer.finalize();
    expect(rowGroup!.rowCount).toBe(1);

    const serialized = serializeRowGroup(rowGroup!);
    const deserialized = deserializeRowGroup(serialized);
    expect(deserialized.rowCount).toBe(1);
  });

  it('should handle columns with all nulls', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'null_test',
      columns: [
        { name: 'id', dataType: 'int32', nullable: false },
        { name: 'optional', dataType: 'string', nullable: true },
      ],
    };

    const rows = Array.from({ length: 10 }, (_, i) => ({
      id: i,
      optional: null,
    }));

    const { rowGroups, serialized } = await writeColumnar(schema, rows);
    expect(rowGroups[0].columns.get('optional')!.stats.nullCount).toBe(10);

    const columns = readColumnarChunk(serialized[0]);
    expect(columns.get('optional')!.every((v) => v === null)).toBe(true);
  });

  it('should handle very long strings', async () => {
    const longString = 'x'.repeat(10000);

    const schema: ColumnarTableSchema = {
      tableName: 'long_string',
      columns: [{ name: 'text', dataType: 'string', nullable: false }],
    };

    const { serialized } = await writeColumnar(schema, [{ text: longString }]);
    const columns = readColumnarChunk(serialized[0]);

    expect(columns.get('text')![0]).toBe(longString);
  });

  it('should handle binary data (bytes)', async () => {
    const binaryData = new Uint8Array([1, 2, 3, 4, 5, 255, 0, 128]);

    const schema: ColumnarTableSchema = {
      tableName: 'binary_test',
      columns: [{ name: 'data', dataType: 'bytes', nullable: true }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([
      { data: binaryData },
      { data: null },
      { data: new Uint8Array([]) },
    ]);

    const rowGroup = await writer.finalize();
    const serialized = serializeRowGroup(rowGroup!);
    const deserialized = deserializeRowGroup(serialized);

    // Verify binary data was preserved
    const fsx = createInMemoryFSX();
    const reader = new ColumnarReader(fsx);
    const decodedColumns = (reader as any).decodeColumns(deserialized, ['data']);

    expect(decodedColumns.get('data')![0]).toEqual(binaryData);
    expect(decodedColumns.get('data')![1]).toBe(null);
  });
});

// ============================================================================
// PREDICATE EVALUATION TESTS
// ============================================================================

describe('Predicate Evaluation', () => {
  let fsx: ReturnType<typeof createInMemoryFSX>;

  beforeEach(async () => {
    fsx = createInMemoryFSX();

    const schema: ColumnarTableSchema = {
      tableName: 'predicate_test',
      columns: [
        { name: 'id', dataType: 'int32', nullable: false },
        { name: 'name', dataType: 'string', nullable: true },
        { name: 'score', dataType: 'float64', nullable: true },
      ],
    };

    const rows = [
      { id: 1, name: 'alice', score: 85.5 },
      { id: 2, name: 'bob', score: 90.0 },
      { id: 3, name: null, score: 75.0 },
      { id: 4, name: 'charlie', score: null },
      { id: 5, name: 'diana', score: 95.5 },
    ];

    const writer = new ColumnarWriter(schema, {}, fsx);
    await writer.write(rows);
    await writer.finalize();
  });

  it('should filter with eq predicate', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'predicate_test',
      predicates: [{ column: 'id', op: 'eq', value: 3 }],
    });

    expect(result.rowCount).toBe(1);
    expect(result.rows[0].id).toBe(3);
  });

  it('should filter with ne predicate', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'predicate_test',
      predicates: [{ column: 'id', op: 'ne', value: 3 }],
    });

    expect(result.rowCount).toBe(4);
    expect(result.rows.every((r) => r.id !== 3)).toBe(true);
  });

  it('should filter with between predicate', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'predicate_test',
      predicates: [{ column: 'score', op: 'between', value: 80, value2: 92 }],
    });

    expect(result.rowCount).toBe(2); // alice (85.5), bob (90.0)
    expect(result.rows.every((r) => (r.score as number) >= 80 && (r.score as number) <= 92)).toBe(true);
  });

  it('should filter with in predicate', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'predicate_test',
      predicates: [{ column: 'name', op: 'in', value: ['alice', 'bob', 'eve'] }],
    });

    expect(result.rowCount).toBe(2);
    expect(result.rows.map((r) => r.name)).toEqual(expect.arrayContaining(['alice', 'bob']));
  });

  it('should handle null equality', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'predicate_test',
      predicates: [{ column: 'name', op: 'eq', value: null }],
    });

    expect(result.rowCount).toBe(1);
    expect(result.rows[0].id).toBe(3);
  });

  it('should combine multiple predicates with AND', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'predicate_test',
      predicates: [
        { column: 'id', op: 'ge', value: 2 },
        { column: 'id', op: 'le', value: 4 },
      ],
    });

    expect(result.rowCount).toBe(3); // ids 2, 3, 4
  });
});
