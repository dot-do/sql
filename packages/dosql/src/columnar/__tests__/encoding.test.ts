/**
 * Columnar Encoding Tests
 *
 * Comprehensive tests for column encoding/decoding in DoSQL columnar storage.
 * Tests all encoding strategies and edge cases including null handling.
 *
 * Issue: sql-4ku7 - Columnar Storage Tests
 */

import { describe, it, expect } from 'vitest';

import {
  // Raw encoding
  encodeRaw,
  decodeRaw,
  encodeRawStrings,
  decodeRawStrings,

  // Dictionary encoding
  encodeDictionary,
  decodeDictionary,

  // Run-length encoding
  encodeRLE,
  decodeRLE,

  // Delta encoding
  encodeDelta,
  decodeDelta,

  // Bit-packing
  bitpackEncode,
  bitpackDecode,

  // Null bitmap utilities
  createNullBitmap,
  isNull,
  setNull,
  setNotNull,

  // Analysis and stats
  analyzeForEncoding,
  calculateStats,
} from '../encoding.js';

import type { ColumnDataType, Encoding } from '../types.js';

// ============================================================================
// RAW ENCODING - NUMERIC EDGE CASES
// ============================================================================

describe('Raw Encoding Edge Cases', () => {
  describe('boundary values for each integer type', () => {
    it('should handle int8 at exact boundaries', () => {
      const values = [-128, 127, 0];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values, 'int8', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'int8', values.length, nullBitmap);

      expect(decoded.values).toEqual([-128, 127, 0]);
    });

    it('should handle uint8 at exact boundaries', () => {
      const values = [0, 255, 128];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values, 'uint8', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'uint8', values.length, nullBitmap);

      expect(decoded.values).toEqual([0, 255, 128]);
    });

    it('should handle int16 at exact boundaries', () => {
      const values = [-32768, 32767, 0];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values, 'int16', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'int16', values.length, nullBitmap);

      expect(decoded.values).toEqual([-32768, 32767, 0]);
    });

    it('should handle uint16 at exact boundaries', () => {
      const values = [0, 65535, 32768];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values, 'uint16', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'uint16', values.length, nullBitmap);

      expect(decoded.values).toEqual([0, 65535, 32768]);
    });

    it('should handle int32 at exact boundaries', () => {
      const values = [-2147483648, 2147483647, 0];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values, 'int32', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'int32', values.length, nullBitmap);

      expect(decoded.values).toEqual([-2147483648, 2147483647, 0]);
    });

    it('should handle uint32 at exact boundaries', () => {
      const values = [0, 4294967295, 2147483648];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values, 'uint32', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'uint32', values.length, nullBitmap);

      expect(decoded.values).toEqual([0, 4294967295, 2147483648]);
    });

    it('should handle int64 at exact boundaries', () => {
      const values = [
        BigInt('-9223372036854775808'),
        BigInt('9223372036854775807'),
        BigInt(0),
      ];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values, 'int64', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'int64', values.length, nullBitmap);

      expect(decoded.values[0]).toBe(BigInt('-9223372036854775808'));
      expect(decoded.values[1]).toBe(BigInt('9223372036854775807'));
      expect(decoded.values[2]).toBe(BigInt(0));
    });

    it('should handle uint64 at exact boundaries', () => {
      const values = [BigInt(0), BigInt('18446744073709551615')];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values, 'uint64', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'uint64', values.length, nullBitmap);

      expect(decoded.values[0]).toBe(BigInt(0));
      expect(decoded.values[1]).toBe(BigInt('18446744073709551615'));
    });
  });

  describe('float special values', () => {
    it('should handle float32 positive and negative zero', () => {
      const values = [0, -0, 0.0];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values, 'float32', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'float32', values.length, nullBitmap);

      // Both 0 and -0 should decode to 0
      expect(decoded.values.every((v) => v === 0 || Object.is(v, -0))).toBe(true);
    });

    it('should handle float64 NaN', () => {
      const values = [NaN];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values, 'float64', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'float64', values.length, nullBitmap);

      expect(Number.isNaN(decoded.values[0])).toBe(true);
    });

    it('should handle float64 subnormal numbers', () => {
      const values = [Number.MIN_VALUE, Number.MIN_VALUE * 2];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values, 'float64', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'float64', values.length, nullBitmap);

      expect(decoded.values[0]).toBe(Number.MIN_VALUE);
      expect(decoded.values[1]).toBe(Number.MIN_VALUE * 2);
    });
  });

  describe('empty and single element arrays', () => {
    it('should handle empty arrays for each type', () => {
      const types: ColumnDataType[] = [
        'int8', 'int16', 'int32', 'int64',
        'uint8', 'uint16', 'uint32', 'uint64',
        'float32', 'float64', 'boolean',
      ];

      for (const dataType of types) {
        const values: never[] = [];
        const nullBitmap = createNullBitmap(values);

        const encoded = encodeRaw(values, dataType, nullBitmap);
        const decoded = decodeRaw(encoded.data, dataType, 0, nullBitmap);

        expect(decoded.values).toEqual([]);
      }
    });

    it('should handle single element arrays', () => {
      const values = [42];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values, 'int32', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'int32', 1, nullBitmap);

      expect(decoded.values).toEqual([42]);
    });

    it('should handle single null element', () => {
      const values = [null];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values as (number | null)[], 'int32', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'int32', 1, nullBitmap);

      expect(decoded.values).toEqual([null]);
    });
  });

  describe('all null arrays', () => {
    it('should handle array of all nulls', () => {
      const values = [null, null, null, null, null];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values as (number | null)[], 'int32', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'int32', values.length, nullBitmap);

      expect(decoded.values).toEqual([null, null, null, null, null]);
    });
  });

  describe('alternating null patterns', () => {
    it('should handle alternating null and non-null values', () => {
      const values = [1, null, 3, null, 5, null, 7, null];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRaw(values as (number | null)[], 'int32', nullBitmap);
      const decoded = decodeRaw(encoded.data, 'int32', values.length, nullBitmap);

      expect(decoded.values).toEqual([1, null, 3, null, 5, null, 7, null]);
    });
  });
});

// ============================================================================
// STRING ENCODING EDGE CASES
// ============================================================================

describe('String Encoding Edge Cases', () => {
  describe('Raw String Encoding', () => {
    it('should handle empty string correctly', () => {
      const values = [''];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRawStrings(values);
      const decoded = decodeRawStrings(encoded.data, values.length, nullBitmap);

      expect(decoded.values).toEqual(['']);
    });

    it('should handle mix of empty strings and nulls', () => {
      const values = ['', null, '', null, ''];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRawStrings(values);
      const decoded = decodeRawStrings(encoded.data, values.length, nullBitmap);

      expect(decoded.values).toEqual(['', null, '', null, '']);
    });

    it('should handle Unicode characters correctly', () => {
      const values = [
        'Hello', // ASCII
        'Cafe', // Accented
        '12345', // Non-ASCII digits
        'Test', // Emoji-like
        'Characters', // Cyrillic-like
      ];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRawStrings(values);
      const decoded = decodeRawStrings(encoded.data, values.length, nullBitmap);

      expect(decoded.values).toEqual(values);
    });

    it('should handle emoji and multi-byte characters', () => {
      const values = ['smile', 'face', 'flag'];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRawStrings(values);
      const decoded = decodeRawStrings(encoded.data, values.length, nullBitmap);

      expect(decoded.values).toEqual(values);
    });

    it('should handle very long strings', () => {
      const longString = 'a'.repeat(100000);
      const values = [longString];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRawStrings(values);
      const decoded = decodeRawStrings(encoded.data, values.length, nullBitmap);

      expect(decoded.values[0]).toBe(longString);
    });

    it('should handle strings with null bytes', () => {
      const values = ['hello\x00world'];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRawStrings(values);
      const decoded = decodeRawStrings(encoded.data, values.length, nullBitmap);

      expect(decoded.values[0]).toBe('hello\x00world');
    });

    it('should handle control characters', () => {
      const values = ['line1\nline2\ttab\rreturn'];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeRawStrings(values);
      const decoded = decodeRawStrings(encoded.data, values.length, nullBitmap);

      expect(decoded.values[0]).toBe('line1\nline2\ttab\rreturn');
    });
  });

  describe('Dictionary Encoding', () => {
    it('should handle single unique value', () => {
      const values = ['same', 'same', 'same', 'same'];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeDictionary(values, nullBitmap);
      expect(encoded.metadata!.dictionary).toHaveLength(1);

      const decoded = decodeDictionary(encoded.data, values.length, nullBitmap);
      expect(decoded.values).toEqual(values);
    });

    it('should handle all nulls in dictionary encoding', () => {
      const values = [null, null, null];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeDictionary(values, nullBitmap);
      // Dictionary should be empty since all values are null
      expect(encoded.metadata!.dictionary).toHaveLength(0);

      const decoded = decodeDictionary(encoded.data, values.length, nullBitmap);
      expect(decoded.values).toEqual([null, null, null]);
    });

    it('should handle mix of nulls and unique values', () => {
      const values = [null, 'a', null, 'b', null, 'a', null];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeDictionary(values, nullBitmap);
      expect(encoded.metadata!.dictionary).toHaveLength(2);

      const decoded = decodeDictionary(encoded.data, values.length, nullBitmap);
      expect(decoded.values).toEqual([null, 'a', null, 'b', null, 'a', null]);
    });

    it('should handle 256+ unique values (needs more than 8 bits)', () => {
      const values = Array.from({ length: 300 }, (_, i) => `value_${i}`);
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeDictionary(values, nullBitmap);
      expect(encoded.metadata!.dictionary).toHaveLength(300);

      const decoded = decodeDictionary(encoded.data, values.length, nullBitmap);
      expect(decoded.values).toEqual(values);
    });

    it('should handle empty strings in dictionary', () => {
      const values = ['', 'a', '', 'b', ''];
      const nullBitmap = createNullBitmap(values);

      const encoded = encodeDictionary(values, nullBitmap);
      expect(encoded.metadata!.dictionary).toContain('');

      const decoded = decodeDictionary(encoded.data, values.length, nullBitmap);
      expect(decoded.values).toEqual(values);
    });

    it('should produce smaller output than raw for repeated values', () => {
      // 10 unique values repeated 100 times each
      const values = Array.from({ length: 1000 }, (_, i) => `category_${i % 10}`);
      const nullBitmap = createNullBitmap(values);

      const rawEncoded = encodeRawStrings(values);
      const dictEncoded = encodeDictionary(values, nullBitmap);

      // Dictionary should be significantly smaller
      expect(dictEncoded.data.length).toBeLessThan(rawEncoded.data.length * 0.5);
    });
  });
});

// ============================================================================
// RUN-LENGTH ENCODING EDGE CASES
// ============================================================================

describe('RLE Encoding Edge Cases', () => {
  it('should handle single run of length 1', () => {
    const values = [42];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeRLE(values, 'int32', nullBitmap);
    const decoded = decodeRLE(encoded.data, 'int32', values.length, nullBitmap);

    expect(decoded.values).toEqual([42]);
  });

  it('should handle alternating values (worst case for RLE)', () => {
    const values = [1, 2, 1, 2, 1, 2, 1, 2];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeRLE(values, 'int32', nullBitmap);
    expect(encoded.metadata?.runCount).toBe(8);

    const decoded = decodeRLE(encoded.data, 'int32', values.length, nullBitmap);
    expect(decoded.values).toEqual(values);
  });

  it('should handle very long single run', () => {
    const values = new Array(10000).fill(999);
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeRLE(values, 'int32', nullBitmap);
    expect(encoded.metadata?.runCount).toBe(1);

    // RLE should be much smaller than raw
    const rawSize = values.length * 4; // 4 bytes per int32
    expect(encoded.data.length).toBeLessThan(rawSize * 0.01);

    const decoded = decodeRLE(encoded.data, 'int32', values.length, nullBitmap);
    expect(decoded.values).toEqual(values);
  });

  it('should handle runs with null values', () => {
    const values = [1, 1, 1, null, null, null, 2, 2, 2];
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

  it('should handle negative values', () => {
    const values = [-5, -5, -5, 0, 0, 5, 5, 5];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeRLE(values, 'int32', nullBitmap);
    const decoded = decodeRLE(encoded.data, 'int32', values.length, nullBitmap);

    expect(decoded.values).toEqual(values);
  });

  it('should work with int64 values', () => {
    const values = [
      BigInt('9223372036854775807'),
      BigInt('9223372036854775807'),
      BigInt('-9223372036854775808'),
      BigInt('-9223372036854775808'),
    ];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeRLE(values, 'int64', nullBitmap);
    const decoded = decodeRLE(encoded.data, 'int64', values.length, nullBitmap);

    for (let i = 0; i < values.length; i++) {
      expect(decoded.values[i]).toBe(values[i]);
    }
  });

  it('should work with float32 repeated values', () => {
    const values = [3.14, 3.14, 3.14, 2.71, 2.71];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeRLE(values, 'float32', nullBitmap);
    const decoded = decodeRLE(encoded.data, 'float32', values.length, nullBitmap);

    // Float32 has limited precision
    for (let i = 0; i < values.length; i++) {
      expect(decoded.values[i]).toBeCloseTo(values[i] as number, 2);
    }
  });
});

// ============================================================================
// DELTA ENCODING EDGE CASES
// ============================================================================

describe('Delta Encoding Edge Cases', () => {
  it('should handle monotonically increasing sequence', () => {
    const values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeDelta(values, 'int32', nullBitmap);
    const decoded = decodeDelta(encoded.data, 'int32', values.length, nullBitmap);

    expect(decoded.values).toEqual(values);
  });

  it('should handle monotonically decreasing sequence', () => {
    const values = [100, 99, 98, 97, 96, 95];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeDelta(values, 'int32', nullBitmap);
    const decoded = decodeDelta(encoded.data, 'int32', values.length, nullBitmap);

    expect(decoded.values).toEqual(values);
  });

  it('should handle all same values (zero delta)', () => {
    const values = [42, 42, 42, 42, 42];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeDelta(values, 'int32', nullBitmap);
    const decoded = decodeDelta(encoded.data, 'int32', values.length, nullBitmap);

    expect(decoded.values).toEqual(values);
  });

  it('should handle large jumps in values', () => {
    const values = [0, 1000000, 2000000, 3000000];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeDelta(values, 'int32', nullBitmap);
    const decoded = decodeDelta(encoded.data, 'int32', values.length, nullBitmap);

    expect(decoded.values).toEqual(values);
  });

  it('should handle negative deltas (decreasing values)', () => {
    const values = [1000, 500, 0, -500, -1000];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeDelta(values, 'int32', nullBitmap);
    expect(encoded.metadata?.signed).toBe(true);

    const decoded = decodeDelta(encoded.data, 'int32', values.length, nullBitmap);
    expect(decoded.values).toEqual(values);
  });

  it('should handle mixed positive and negative deltas', () => {
    const values = [50, 60, 40, 70, 30, 80];
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
    ];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeDelta(values, 'int64', nullBitmap);
    const decoded = decodeDelta(encoded.data, 'int64', values.length, nullBitmap);

    for (let i = 0; i < values.length; i++) {
      expect(decoded.values[i]).toBe(values[i]);
    }
  });

  it('should handle all null values', () => {
    const values = [null, null, null];
    const nullBitmap = createNullBitmap(values);

    const encoded = encodeDelta(values as (number | null)[], 'int32', nullBitmap);
    const decoded = decodeDelta(encoded.data, 'int32', values.length, nullBitmap);

    expect(decoded.values).toEqual([null, null, null]);
  });

  it('should produce smaller output than raw for sequential data', () => {
    const values = Array.from({ length: 1000 }, (_, i) => 1000000 + i);
    const nullBitmap = createNullBitmap(values);

    const rawEncoded = encodeRaw(values, 'int32', nullBitmap);
    const deltaEncoded = encodeDelta(values, 'int32', nullBitmap);

    // Delta should be significantly smaller
    expect(deltaEncoded.data.length).toBeLessThan(rawEncoded.data.length);
  });
});

// ============================================================================
// BIT-PACKING EDGE CASES
// ============================================================================

describe('Bit-Packing Edge Cases', () => {
  it('should handle 1-bit values', () => {
    const values = [0, 1, 0, 1, 1, 0, 1, 0, 0, 1];

    const encoded = bitpackEncode(values, 1);
    const decoded = bitpackDecode(encoded, values.length, 1);

    expect(decoded).toEqual(values);
  });

  it('should handle 2-bit values', () => {
    const values = [0, 1, 2, 3, 0, 1, 2, 3];

    const encoded = bitpackEncode(values, 2);
    const decoded = bitpackDecode(encoded, values.length, 2);

    expect(decoded).toEqual(values);
  });

  it('should handle 7-bit values (crossing byte boundaries)', () => {
    const values = Array.from({ length: 100 }, (_, i) => i % 128);

    const encoded = bitpackEncode(values, 7);
    const decoded = bitpackDecode(encoded, values.length, 7);

    expect(decoded).toEqual(values);
  });

  it('should handle 9-bit values (more than one byte)', () => {
    const values = [0, 256, 511, 100, 400];

    const encoded = bitpackEncode(values, 9);
    const decoded = bitpackDecode(encoded, values.length, 9);

    expect(decoded).toEqual(values);
  });

  it('should handle 16-bit values', () => {
    const values = [0, 32768, 65535, 12345];

    const encoded = bitpackEncode(values, 16);
    const decoded = bitpackDecode(encoded, values.length, 16);

    expect(decoded).toEqual(values);
  });

  it('should handle empty array', () => {
    const values: number[] = [];

    const encoded = bitpackEncode(values, 8);
    expect(encoded.length).toBe(0);

    const decoded = bitpackDecode(encoded, 0, 8);
    expect(decoded).toEqual([]);
  });

  it('should handle single value', () => {
    const values = [42];

    const encoded = bitpackEncode(values, 6);
    const decoded = bitpackDecode(encoded, 1, 6);

    expect(decoded).toEqual([42]);
  });

  it('should correctly calculate byte size', () => {
    // 10 values at 3 bits each = 30 bits = 4 bytes
    const values = Array.from({ length: 10 }, (_, i) => i % 8);
    const encoded = bitpackEncode(values, 3);

    expect(encoded.length).toBe(4);

    const decoded = bitpackDecode(encoded, values.length, 3);
    expect(decoded).toEqual(values);
  });
});

// ============================================================================
// NULL BITMAP EDGE CASES
// ============================================================================

describe('Null Bitmap Edge Cases', () => {
  it('should handle values at byte boundaries', () => {
    // Test values at positions 7, 8, 15, 16 (byte boundary)
    const values = new Array(20).fill(1);
    values[7] = null;
    values[8] = null;
    values[15] = null;
    values[16] = null;

    const bitmap = createNullBitmap(values);

    expect(isNull(bitmap, 6)).toBe(false);
    expect(isNull(bitmap, 7)).toBe(true);
    expect(isNull(bitmap, 8)).toBe(true);
    expect(isNull(bitmap, 9)).toBe(false);
    expect(isNull(bitmap, 14)).toBe(false);
    expect(isNull(bitmap, 15)).toBe(true);
    expect(isNull(bitmap, 16)).toBe(true);
    expect(isNull(bitmap, 17)).toBe(false);
  });

  it('should handle first and last positions', () => {
    const values = [null, 1, 2, 3, null];
    const bitmap = createNullBitmap(values);

    expect(isNull(bitmap, 0)).toBe(true);
    expect(isNull(bitmap, 1)).toBe(false);
    expect(isNull(bitmap, 4)).toBe(true);
  });

  it('should correctly set and unset null at various positions', () => {
    const bitmap = new Uint8Array(3); // 24 bits

    // Set some as not null
    setNotNull(bitmap, 0);
    setNotNull(bitmap, 7);
    setNotNull(bitmap, 8);
    setNotNull(bitmap, 15);
    setNotNull(bitmap, 23);

    expect(isNull(bitmap, 0)).toBe(false);
    expect(isNull(bitmap, 7)).toBe(false);
    expect(isNull(bitmap, 8)).toBe(false);
    expect(isNull(bitmap, 15)).toBe(false);
    expect(isNull(bitmap, 23)).toBe(false);

    // All others should be null
    expect(isNull(bitmap, 1)).toBe(true);
    expect(isNull(bitmap, 6)).toBe(true);
    expect(isNull(bitmap, 9)).toBe(true);

    // Now set some back to null
    setNull(bitmap, 7);
    setNull(bitmap, 15);

    expect(isNull(bitmap, 7)).toBe(true);
    expect(isNull(bitmap, 15)).toBe(true);
    expect(isNull(bitmap, 0)).toBe(false); // Should still be not null
  });

  it('should handle bitmap larger than 8 bytes', () => {
    const values = new Array(100).fill(1);
    // Set every 10th value to null
    for (let i = 0; i < 100; i += 10) {
      values[i] = null;
    }

    const bitmap = createNullBitmap(values);

    for (let i = 0; i < 100; i++) {
      expect(isNull(bitmap, i)).toBe(i % 10 === 0);
    }
  });

  it('should handle undefined as null', () => {
    const values = [1, undefined, 3, undefined, 5];
    const bitmap = createNullBitmap(values);

    expect(isNull(bitmap, 0)).toBe(false);
    expect(isNull(bitmap, 1)).toBe(true);
    expect(isNull(bitmap, 2)).toBe(false);
    expect(isNull(bitmap, 3)).toBe(true);
    expect(isNull(bitmap, 4)).toBe(false);
  });
});

// ============================================================================
// ENCODING ANALYSIS
// ============================================================================

describe('Encoding Analysis', () => {
  describe('String encoding selection', () => {
    it('should recommend raw for unique strings', () => {
      const values = Array.from({ length: 100 }, (_, i) => `unique_${i}`);
      const analysis = analyzeForEncoding(values, 'string');

      expect(analysis.recommendedEncoding).toBe('raw');
    });

    it('should recommend dict for low cardinality strings', () => {
      const values = Array.from({ length: 1000 }, (_, i) => `cat_${i % 3}`);
      const analysis = analyzeForEncoding(values, 'string');

      expect(analysis.recommendedEncoding).toBe('dict');
      expect(analysis.cardinality).toBe(3);
    });

    it('should not recommend dict for small datasets', () => {
      // Less than MIN_ROWS_FOR_DICT
      const values = Array.from({ length: 50 }, (_, i) => `cat_${i % 2}`);
      const analysis = analyzeForEncoding(values, 'string');

      expect(analysis.recommendedEncoding).toBe('raw');
    });
  });

  describe('Numeric encoding selection', () => {
    it('should recommend delta for sorted integers', () => {
      const values = Array.from({ length: 1000 }, (_, i) => i);
      const analysis = analyzeForEncoding(values, 'int32');

      expect(analysis.isSorted).toBe(true);
      expect(analysis.recommendedEncoding).toBe('delta');
    });

    it('should report run count for repeated values', () => {
      // 10 values each repeated 10 times = 100 values, 10 runs
      const values: number[] = [];
      for (let i = 0; i < 10; i++) {
        for (let j = 0; j < 10; j++) {
          values.push(i);
        }
      }
      const analysis = analyzeForEncoding(values, 'int32');

      expect(analysis.runCount).toBe(10);
    });

    it('should identify unsorted data', () => {
      const values = [5, 3, 8, 1, 9, 2];
      const analysis = analyzeForEncoding(values, 'int32');

      expect(analysis.isSorted).toBe(false);
    });
  });

  describe('Boolean and other types', () => {
    it('should recommend raw for booleans', () => {
      const values = [true, false, true, false];
      const analysis = analyzeForEncoding(values, 'boolean');

      expect(analysis.recommendedEncoding).toBe('raw');
    });
  });
});

// ============================================================================
// STATISTICS CALCULATION
// ============================================================================

describe('Statistics Calculation', () => {
  describe('Numeric statistics', () => {
    it('should calculate correct stats with mixed values', () => {
      const values = [10, 20, null, 5, 15, null, 25];
      const stats = calculateStats(values, 'int32');

      expect(stats.min).toBe(5);
      expect(stats.max).toBe(25);
      expect(stats.nullCount).toBe(2);
      expect(stats.distinctCount).toBe(5);
      expect(stats.sum).toBe(75);
    });

    it('should handle all same values', () => {
      const values = [42, 42, 42, 42];
      const stats = calculateStats(values, 'int32');

      expect(stats.min).toBe(42);
      expect(stats.max).toBe(42);
      expect(stats.distinctCount).toBe(1);
      expect(stats.sum).toBe(168);
    });

    it('should handle bigint values', () => {
      const values = [BigInt(100), BigInt(200), BigInt(50)];
      const stats = calculateStats(values, 'int64');

      expect(stats.min).toBe(BigInt(50));
      expect(stats.max).toBe(BigInt(200));
      expect(stats.sum).toBe(BigInt(350));
    });

    it('should handle negative numbers', () => {
      const values = [-10, -5, 0, 5, 10];
      const stats = calculateStats(values, 'int32');

      expect(stats.min).toBe(-10);
      expect(stats.max).toBe(10);
      expect(stats.sum).toBe(0);
    });

    it('should handle float precision', () => {
      const values = [0.1, 0.2, 0.3];
      const stats = calculateStats(values, 'float64');

      expect(stats.min).toBeCloseTo(0.1);
      expect(stats.max).toBeCloseTo(0.3);
      expect(stats.sum).toBeCloseTo(0.6);
    });
  });

  describe('String statistics', () => {
    it('should calculate correct min/max for strings', () => {
      const values = ['banana', 'apple', 'cherry'];
      const stats = calculateStats(values, 'string');

      expect(stats.min).toBe('apple');
      expect(stats.max).toBe('cherry');
      expect(stats.sum).toBeUndefined();
    });

    it('should handle empty strings', () => {
      const values = ['', 'a', 'z'];
      const stats = calculateStats(values, 'string');

      expect(stats.min).toBe('');
      expect(stats.max).toBe('z');
    });
  });

  describe('Boolean statistics', () => {
    it('should treat booleans as 0/1 for min/max', () => {
      const values = [true, false, true];
      const stats = calculateStats(values, 'boolean');

      expect(stats.min).toBe(0);
      expect(stats.max).toBe(1);
    });

    it('should handle all true values', () => {
      const values = [true, true, true];
      const stats = calculateStats(values, 'boolean');

      expect(stats.min).toBe(1);
      expect(stats.max).toBe(1);
    });
  });

  describe('All null values', () => {
    it('should return null min/max for all nulls', () => {
      const values = [null, null, null];
      const stats = calculateStats(values, 'int32');

      expect(stats.min).toBe(null);
      expect(stats.max).toBe(null);
      expect(stats.nullCount).toBe(3);
      expect(stats.distinctCount).toBe(0);
    });
  });
});

// ============================================================================
// ROUND-TRIP ENCODING/DECODING
// ============================================================================

describe('Round-trip Encoding/Decoding', () => {
  const testRoundTrip = (
    values: (number | bigint | boolean | null)[],
    dataType: ColumnDataType,
    encoding: Encoding
  ) => {
    const nullBitmap = createNullBitmap(values);

    let encoded;
    let decoded;

    switch (encoding) {
      case 'raw':
        encoded = encodeRaw(values, dataType, nullBitmap);
        decoded = decodeRaw(encoded.data, dataType, values.length, nullBitmap);
        break;
      case 'rle':
        encoded = encodeRLE(values as (number | bigint | null)[], dataType, nullBitmap);
        decoded = decodeRLE(encoded.data, dataType, values.length, nullBitmap);
        break;
      case 'delta':
        encoded = encodeDelta(values as (number | bigint | null)[], dataType, nullBitmap);
        decoded = decodeDelta(encoded.data, dataType, values.length, nullBitmap);
        break;
      default:
        throw new Error(`Unsupported encoding: ${encoding}`);
    }

    for (let i = 0; i < values.length; i++) {
      if (values[i] === null) {
        expect(decoded.values[i]).toBe(null);
      } else if (typeof values[i] === 'number' && !Number.isInteger(values[i])) {
        expect(decoded.values[i]).toBeCloseTo(values[i] as number, 5);
      } else {
        expect(decoded.values[i]).toBe(values[i]);
      }
    }
  };

  it('should round-trip int32 through all applicable encodings', () => {
    const values = [1, 2, 3, null, 5, 5, 5, null, 10];

    testRoundTrip(values, 'int32', 'raw');
    testRoundTrip(values, 'int32', 'rle');
    testRoundTrip(values, 'int32', 'delta');
  });

  it('should round-trip int64 through all applicable encodings', () => {
    const values = [
      BigInt(1),
      BigInt(2),
      null,
      BigInt(4),
      BigInt(4),
      BigInt(4),
    ];

    testRoundTrip(values, 'int64', 'raw');
    testRoundTrip(values, 'int64', 'rle');
    testRoundTrip(values, 'int64', 'delta');
  });

  it('should round-trip float64 through raw encoding', () => {
    const values = [1.1, 2.2, null, 3.3, 4.4];
    testRoundTrip(values, 'float64', 'raw');
  });

  it('should round-trip boolean through raw encoding', () => {
    const values = [true, false, null, true, false];
    testRoundTrip(values, 'boolean', 'raw');
  });
});
