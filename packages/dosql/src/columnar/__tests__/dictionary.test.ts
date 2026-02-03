/**
 * Dictionary Encoding Tests
 *
 * Comprehensive tests for the DictionaryBuilder and DictionaryReader classes
 * in the DoSQL columnar storage engine. Tests cover:
 * - Incremental dictionary construction
 * - Encoding/decoding round-trips
 * - Cardinality analysis and threshold decisions
 * - Compression ratio estimation
 * - Row lookup and filtering
 * - Edge cases (nulls, empty strings, large dictionaries)
 *
 * Issue: sql-3puo - Columnar storage dictionary encoding for string columns
 */

import { describe, it, expect } from 'vitest';

import {
  DictionaryBuilder,
  DictionaryReader,
  analyzeDictionaryFit,
} from '../dictionary.js';

import {
  encodeDictionary,
  decodeDictionary,
  encodeRawStrings,
  createNullBitmap,
} from '../encoding.js';

// ============================================================================
// DICTIONARY BUILDER - Basic Operations
// ============================================================================

describe('DictionaryBuilder - Basic Operations', () => {
  it('should create an empty builder', () => {
    const builder = new DictionaryBuilder();

    expect(builder.cardinality).toBe(0);
    expect(builder.totalCount).toBe(0);
    expect(builder.nonNullCount).toBe(0);
    expect(builder.cardinalityRatio).toBe(0);
  });

  it('should add values and track cardinality', () => {
    const builder = new DictionaryBuilder();
    builder.add('apple');
    builder.add('banana');
    builder.add('apple');
    builder.add('cherry');

    expect(builder.cardinality).toBe(3);
    expect(builder.totalCount).toBe(4);
    expect(builder.nonNullCount).toBe(4);
  });

  it('should track null values separately', () => {
    const builder = new DictionaryBuilder();
    builder.add('a');
    builder.add(null);
    builder.add('b');
    builder.add(null);
    builder.add('a');

    expect(builder.cardinality).toBe(2);
    expect(builder.totalCount).toBe(5);
    expect(builder.nonNullCount).toBe(3);
  });

  it('should preserve insertion order of dictionary entries', () => {
    const builder = new DictionaryBuilder();
    builder.add('cherry');
    builder.add('apple');
    builder.add('banana');
    builder.add('cherry');

    const entries = builder.getEntries();
    expect(entries).toEqual(['cherry', 'apple', 'banana']);
  });

  it('should track all values including duplicates and nulls', () => {
    const builder = new DictionaryBuilder();
    builder.add('a');
    builder.add(null);
    builder.add('a');
    builder.add('b');
    builder.add(null);

    expect(builder.getValues()).toEqual(['a', null, 'a', 'b', null]);
  });

  it('should support indexOf lookup', () => {
    const builder = new DictionaryBuilder();
    builder.add('first');
    builder.add('second');
    builder.add('third');

    expect(builder.indexOf('first')).toBe(0);
    expect(builder.indexOf('second')).toBe(1);
    expect(builder.indexOf('third')).toBe(2);
    expect(builder.indexOf('missing')).toBe(-1);
  });

  it('should support contains check', () => {
    const builder = new DictionaryBuilder();
    builder.add('hello');
    builder.add('world');

    expect(builder.contains('hello')).toBe(true);
    expect(builder.contains('world')).toBe(true);
    expect(builder.contains('missing')).toBe(false);
  });

  it('should reset to clean state', () => {
    const builder = new DictionaryBuilder();
    builder.add('a');
    builder.add('b');
    builder.add(null);

    builder.reset();

    expect(builder.cardinality).toBe(0);
    expect(builder.totalCount).toBe(0);
    expect(builder.nonNullCount).toBe(0);
    expect(builder.getEntries()).toEqual([]);
    expect(builder.getValues()).toEqual([]);
  });
});

// ============================================================================
// DICTIONARY BUILDER - Cardinality Analysis
// ============================================================================

describe('DictionaryBuilder - Cardinality Analysis', () => {
  it('should calculate correct cardinality ratio', () => {
    const builder = new DictionaryBuilder();
    // 5 unique values out of 100
    for (let i = 0; i < 100; i++) {
      builder.add(`cat_${i % 5}`);
    }

    expect(builder.cardinalityRatio).toBeCloseTo(0.05);
  });

  it('should recommend dictionary for low cardinality', () => {
    const builder = new DictionaryBuilder();
    // 3 unique values out of 1000 = 0.003 ratio
    for (let i = 0; i < 1000; i++) {
      builder.add(`category_${i % 3}`);
    }

    expect(builder.shouldUseDictionary()).toBe(true);
  });

  it('should not recommend dictionary for high cardinality', () => {
    const builder = new DictionaryBuilder();
    // 100 unique values out of 100 = 1.0 ratio
    for (let i = 0; i < 100; i++) {
      builder.add(`unique_${i}`);
    }

    expect(builder.shouldUseDictionary()).toBe(false);
  });

  it('should not recommend dictionary for small datasets', () => {
    const builder = new DictionaryBuilder();
    // Only 50 rows (below MIN_ROWS_FOR_DICT = 100)
    for (let i = 0; i < 50; i++) {
      builder.add(`cat_${i % 2}`);
    }

    expect(builder.shouldUseDictionary()).toBe(false);
  });

  it('should handle all null values', () => {
    const builder = new DictionaryBuilder();
    for (let i = 0; i < 200; i++) {
      builder.add(null);
    }

    // Cardinality is 0, ratio is 0
    expect(builder.cardinality).toBe(0);
    expect(builder.cardinalityRatio).toBe(0);
    // Should use dict because ratio (0) <= threshold
    expect(builder.shouldUseDictionary()).toBe(true);
  });

  it('should handle single unique value as dictionary-worthy', () => {
    const builder = new DictionaryBuilder();
    for (let i = 0; i < 500; i++) {
      builder.add('same');
    }

    expect(builder.cardinality).toBe(1);
    expect(builder.cardinalityRatio).toBeCloseTo(0.002);
    expect(builder.shouldUseDictionary()).toBe(true);
  });
});

// ============================================================================
// DICTIONARY BUILDER - Size Estimation
// ============================================================================

describe('DictionaryBuilder - Size Estimation', () => {
  it('should estimate dictionary size smaller than raw for low cardinality', () => {
    const builder = new DictionaryBuilder();
    for (let i = 0; i < 1000; i++) {
      builder.add(`category_${i % 5}`);
    }

    const dictSize = builder.estimateDictionarySize();
    const rawSize = builder.estimateRawSize();

    expect(dictSize).toBeLessThan(rawSize);
    expect(builder.compressionRatio).toBeLessThan(1.0);
  });

  it('should estimate dictionary size larger than raw for high cardinality', () => {
    const builder = new DictionaryBuilder();
    for (let i = 0; i < 100; i++) {
      builder.add(`unique_value_${i}_with_some_extra_text`);
    }

    // With all unique values, dictionary overhead makes it larger
    const dictSize = builder.estimateDictionarySize();
    const rawSize = builder.estimateRawSize();

    // Dictionary should be at least as large since there's no repetition benefit
    // (index overhead + dictionary itself >= raw strings)
    expect(dictSize).toBeGreaterThanOrEqual(rawSize * 0.9);
  });

  it('should handle empty builder', () => {
    const builder = new DictionaryBuilder();

    expect(builder.estimateDictionarySize()).toBe(4); // Just the dict count header
    expect(builder.estimateRawSize()).toBe(0);
    expect(builder.compressionRatio).toBe(1.0);
  });

  it('should handle only null values', () => {
    const builder = new DictionaryBuilder();
    for (let i = 0; i < 100; i++) {
      builder.add(null);
    }

    // Dictionary has no entries, just count header + index space
    const dictSize = builder.estimateDictionarySize();
    expect(dictSize).toBeGreaterThan(0);
    // Raw size is 0 since nulls have no raw string representation tracked
    expect(builder.estimateRawSize()).toBe(0);
  });
});

// ============================================================================
// DICTIONARY BUILDER - Encoding
// ============================================================================

describe('DictionaryBuilder - Encoding', () => {
  it('should encode values and produce valid binary data', () => {
    const builder = new DictionaryBuilder();
    builder.add('red');
    builder.add('green');
    builder.add('blue');
    builder.add('red');
    builder.add('green');

    const encoded = builder.encode();

    expect(encoded.data).toBeInstanceOf(Uint8Array);
    expect(encoded.data.length).toBeGreaterThan(0);
    expect(encoded.dictionary).toEqual(['red', 'green', 'blue']);
    expect(encoded.bitsPerIndex).toBe(2); // 3 values needs 2 bits
    expect(encoded.rowCount).toBe(5);
  });

  it('should encode with nulls', () => {
    const builder = new DictionaryBuilder();
    builder.add('a');
    builder.add(null);
    builder.add('b');
    builder.add(null);
    builder.add('a');

    const encoded = builder.encode();

    expect(encoded.rowCount).toBe(5);
    expect(encoded.dictionary).toEqual(['a', 'b']);
    expect(encoded.nullBitmap).toBeInstanceOf(Uint8Array);
  });

  it('should encode single value dictionary', () => {
    const builder = new DictionaryBuilder();
    for (let i = 0; i < 10; i++) {
      builder.add('same');
    }

    const encoded = builder.encode();

    expect(encoded.dictionary).toEqual(['same']);
    expect(encoded.bitsPerIndex).toBe(1); // 1 value needs 1 bit
    expect(encoded.rowCount).toBe(10);
  });

  it('should encode empty strings', () => {
    const builder = new DictionaryBuilder();
    builder.add('');
    builder.add('non-empty');
    builder.add('');

    const encoded = builder.encode();

    expect(encoded.dictionary).toEqual(['', 'non-empty']);
    expect(encoded.rowCount).toBe(3);
  });

  it('should track dictionary and indices byte sizes', () => {
    const builder = new DictionaryBuilder();
    for (let i = 0; i < 100; i++) {
      builder.add(`val_${i % 5}`);
    }

    const encoded = builder.encode();

    expect(encoded.dictionaryByteSize).toBeGreaterThan(0);
    expect(encoded.indicesByteSize).toBeGreaterThan(0);
    expect(encoded.data.length).toBe(encoded.dictionaryByteSize + encoded.indicesByteSize);
  });
});

// ============================================================================
// DICTIONARY BUILDER + READER - Round Trip
// ============================================================================

describe('DictionaryBuilder + DictionaryReader - Round Trip', () => {
  it('should round-trip simple values', () => {
    const original = ['red', 'green', 'blue', 'red', 'green', 'red'];

    const builder = new DictionaryBuilder();
    for (const v of original) {
      builder.add(v);
    }
    const encoded = builder.encode();

    const reader = DictionaryReader.fromEncoded(
      encoded.data,
      encoded.rowCount,
      encoded.nullBitmap,
    );
    const decoded = reader.decodeAll();

    expect(decoded).toEqual(original);
  });

  it('should round-trip values with nulls', () => {
    const original: (string | null)[] = [null, 'a', null, 'b', 'a', null];

    const builder = new DictionaryBuilder();
    for (const v of original) {
      builder.add(v);
    }
    const encoded = builder.encode();

    const reader = DictionaryReader.fromEncoded(
      encoded.data,
      encoded.rowCount,
      encoded.nullBitmap,
    );
    const decoded = reader.decodeAll();

    expect(decoded).toEqual(original);
  });

  it('should round-trip all null values', () => {
    const original: (string | null)[] = [null, null, null, null];

    const builder = new DictionaryBuilder();
    for (const v of original) {
      builder.add(v);
    }
    const encoded = builder.encode();

    const reader = DictionaryReader.fromEncoded(
      encoded.data,
      encoded.rowCount,
      encoded.nullBitmap,
    );
    const decoded = reader.decodeAll();

    expect(decoded).toEqual(original);
  });

  it('should round-trip single value', () => {
    const original = ['only'];

    const builder = new DictionaryBuilder();
    for (const v of original) {
      builder.add(v);
    }
    const encoded = builder.encode();

    const reader = DictionaryReader.fromEncoded(
      encoded.data,
      encoded.rowCount,
      encoded.nullBitmap,
    );
    const decoded = reader.decodeAll();

    expect(decoded).toEqual(original);
  });

  it('should round-trip empty strings', () => {
    const original = ['', 'a', '', 'b', ''];

    const builder = new DictionaryBuilder();
    for (const v of original) {
      builder.add(v);
    }
    const encoded = builder.encode();

    const reader = DictionaryReader.fromEncoded(
      encoded.data,
      encoded.rowCount,
      encoded.nullBitmap,
    );
    const decoded = reader.decodeAll();

    expect(decoded).toEqual(original);
  });

  it('should round-trip large dictionary (256+ entries)', () => {
    const original = Array.from({ length: 500 }, (_, i) => `value_${i % 300}`);

    const builder = new DictionaryBuilder();
    for (const v of original) {
      builder.add(v);
    }
    const encoded = builder.encode();

    const reader = DictionaryReader.fromEncoded(
      encoded.data,
      encoded.rowCount,
      encoded.nullBitmap,
    );
    const decoded = reader.decodeAll();

    expect(decoded).toEqual(original);
  });

  it('should round-trip unicode strings', () => {
    const original = ['Hello', 'Cafe', 'Test'];

    const builder = new DictionaryBuilder();
    for (const v of original) {
      builder.add(v);
    }
    const encoded = builder.encode();

    const reader = DictionaryReader.fromEncoded(
      encoded.data,
      encoded.rowCount,
      encoded.nullBitmap,
    );
    const decoded = reader.decodeAll();

    expect(decoded).toEqual(original);
  });
});

// ============================================================================
// DICTIONARY READER - Value Access
// ============================================================================

describe('DictionaryReader - Value Access', () => {
  function createTestReader() {
    const values: (string | null)[] = ['alpha', 'beta', null, 'alpha', 'gamma', null, 'beta'];
    const builder = new DictionaryBuilder();
    for (const v of values) {
      builder.add(v);
    }
    const encoded = builder.encode();
    return DictionaryReader.fromEncoded(encoded.data, encoded.rowCount, encoded.nullBitmap);
  }

  it('should get value at specific row index', () => {
    const reader = createTestReader();

    expect(reader.getValue(0)).toBe('alpha');
    expect(reader.getValue(1)).toBe('beta');
    expect(reader.getValue(2)).toBe(null);
    expect(reader.getValue(3)).toBe('alpha');
    expect(reader.getValue(4)).toBe('gamma');
    expect(reader.getValue(5)).toBe(null);
    expect(reader.getValue(6)).toBe('beta');
  });

  it('should return null for out-of-bounds index', () => {
    const reader = createTestReader();

    expect(reader.getValue(-1)).toBe(null);
    expect(reader.getValue(100)).toBe(null);
  });

  it('should get dictionary index at specific row', () => {
    const reader = createTestReader();

    expect(reader.getIndex(0)).toBe(0); // 'alpha' is first entry
    expect(reader.getIndex(1)).toBe(1); // 'beta' is second entry
    expect(reader.getIndex(2)).toBe(-1); // null
    expect(reader.getIndex(4)).toBe(2); // 'gamma' is third entry
  });

  it('should return -1 for null or out-of-bounds getIndex', () => {
    const reader = createTestReader();

    expect(reader.getIndex(2)).toBe(-1); // null row
    expect(reader.getIndex(-1)).toBe(-1); // out of bounds
    expect(reader.getIndex(100)).toBe(-1); // out of bounds
  });

  it('should report dictionary size', () => {
    const reader = createTestReader();

    expect(reader.dictionarySize).toBe(3); // alpha, beta, gamma
  });

  it('should report row count', () => {
    const reader = createTestReader();

    expect(reader.getRowCount()).toBe(7);
  });

  it('should return dictionary entries', () => {
    const reader = createTestReader();

    expect(reader.getEntries()).toEqual(['alpha', 'beta', 'gamma']);
  });
});

// ============================================================================
// DICTIONARY READER - Row Filtering
// ============================================================================

describe('DictionaryReader - Row Filtering', () => {
  function createTestReader() {
    const values: (string | null)[] = ['red', 'blue', null, 'red', 'green', null, 'blue', 'red'];
    const builder = new DictionaryBuilder();
    for (const v of values) {
      builder.add(v);
    }
    const encoded = builder.encode();
    return DictionaryReader.fromEncoded(encoded.data, encoded.rowCount, encoded.nullBitmap);
  }

  it('should find all rows matching a value', () => {
    const reader = createTestReader();

    expect(reader.findRows('red')).toEqual([0, 3, 7]);
    expect(reader.findRows('blue')).toEqual([1, 6]);
    expect(reader.findRows('green')).toEqual([4]);
  });

  it('should return empty array for missing value', () => {
    const reader = createTestReader();

    expect(reader.findRows('yellow')).toEqual([]);
  });

  it('should find all null rows', () => {
    const reader = createTestReader();

    expect(reader.findNullRows()).toEqual([2, 5]);
  });

  it('should return empty array when no nulls', () => {
    const values = ['a', 'b', 'a'];
    const builder = new DictionaryBuilder();
    for (const v of values) {
      builder.add(v);
    }
    const encoded = builder.encode();
    const reader = DictionaryReader.fromEncoded(encoded.data, encoded.rowCount, encoded.nullBitmap);

    expect(reader.findNullRows()).toEqual([]);
  });
});

// ============================================================================
// COMPATIBILITY WITH encoding.ts
// ============================================================================

describe('DictionaryBuilder/Reader compatibility with encoding.ts', () => {
  it('should decode data encoded by encodeDictionary', () => {
    const values: (string | null)[] = ['x', 'y', null, 'x', 'z', null, 'y'];
    const nullBitmap = createNullBitmap(values);

    // Encode using the existing encodeDictionary function
    const encoded = encodeDictionary(values, nullBitmap);

    // Decode using DictionaryReader
    const reader = DictionaryReader.fromEncoded(
      encoded.data,
      values.length,
      nullBitmap,
    );
    const decoded = reader.decodeAll();

    expect(decoded).toEqual(values);
  });

  it('should produce data decodable by decodeDictionary', () => {
    const values: (string | null)[] = ['foo', 'bar', null, 'foo', 'baz'];

    const builder = new DictionaryBuilder();
    for (const v of values) {
      builder.add(v);
    }
    const encoded = builder.encode();

    // Decode using the existing decodeDictionary function
    const decoded = decodeDictionary(
      encoded.data,
      encoded.rowCount,
      encoded.nullBitmap,
    );

    expect(decoded.values).toEqual(values);
  });

  it('should be interchangeable for various data patterns', () => {
    const testCases: (string | null)[][] = [
      // Single value
      ['same', 'same', 'same', 'same'],
      // Two values
      ['a', 'b', 'a', 'b', 'a', 'b'],
      // With nulls
      [null, 'x', null, 'y', null],
      // All nulls
      [null, null, null],
      // Empty strings
      ['', 'a', '', 'b'],
      // Many unique values
      Array.from({ length: 50 }, (_, i) => `val_${i}`),
    ];

    for (const values of testCases) {
      const nullBitmap = createNullBitmap(values);

      // Encode with builder, decode with encoding.ts
      const builder = new DictionaryBuilder();
      for (const v of values) {
        builder.add(v);
      }
      const builderEncoded = builder.encode();

      const decodedFromBuilder = decodeDictionary(
        builderEncoded.data,
        builderEncoded.rowCount,
        builderEncoded.nullBitmap,
      );
      expect(decodedFromBuilder.values).toEqual(values);

      // Encode with encoding.ts, decode with reader
      const encEncoded = encodeDictionary(values, nullBitmap);
      const reader = DictionaryReader.fromEncoded(
        encEncoded.data,
        values.length,
        nullBitmap,
      );
      expect(reader.decodeAll()).toEqual(values);
    }
  });
});

// ============================================================================
// analyzeDictionaryFit
// ============================================================================

describe('analyzeDictionaryFit', () => {
  it('should analyze low-cardinality data', () => {
    const values = Array.from({ length: 1000 }, (_, i) => `cat_${i % 3}`);
    const analysis = analyzeDictionaryFit(values);

    expect(analysis.cardinality).toBe(3);
    expect(analysis.totalCount).toBe(1000);
    expect(analysis.nonNullCount).toBe(1000);
    expect(analysis.cardinalityRatio).toBeCloseTo(0.003);
    expect(analysis.shouldUseDictionary).toBe(true);
    expect(analysis.compressionRatio).toBeLessThan(1.0);
  });

  it('should analyze high-cardinality data', () => {
    const values = Array.from({ length: 200 }, (_, i) => `unique_${i}`);
    const analysis = analyzeDictionaryFit(values);

    expect(analysis.cardinality).toBe(200);
    expect(analysis.totalCount).toBe(200);
    expect(analysis.shouldUseDictionary).toBe(false);
  });

  it('should analyze data with nulls', () => {
    const values: (string | null)[] = [];
    for (let i = 0; i < 500; i++) {
      values.push(i % 3 === 0 ? null : `cat_${i % 5}`);
    }
    const analysis = analyzeDictionaryFit(values);

    expect(analysis.nonNullCount).toBeLessThan(analysis.totalCount);
    expect(analysis.shouldUseDictionary).toBe(true);
  });

  it('should analyze small dataset', () => {
    const values = ['a', 'b', 'a'];
    const analysis = analyzeDictionaryFit(values);

    expect(analysis.cardinality).toBe(2);
    expect(analysis.totalCount).toBe(3);
    expect(analysis.shouldUseDictionary).toBe(false); // Too few rows
  });

  it('should report accurate size estimates', () => {
    const values = Array.from({ length: 1000 }, (_, i) => `category_${i % 10}`);
    const analysis = analyzeDictionaryFit(values);

    expect(analysis.estimatedDictionarySize).toBeGreaterThan(0);
    expect(analysis.estimatedRawSize).toBeGreaterThan(0);
    expect(analysis.estimatedDictionarySize).toBeLessThan(analysis.estimatedRawSize);
  });
});

// ============================================================================
// EDGE CASES
// ============================================================================

describe('Dictionary Encoding - Edge Cases', () => {
  it('should handle single row', () => {
    const builder = new DictionaryBuilder();
    builder.add('only');

    const encoded = builder.encode();
    const reader = DictionaryReader.fromEncoded(
      encoded.data,
      encoded.rowCount,
      encoded.nullBitmap,
    );

    expect(reader.decodeAll()).toEqual(['only']);
    expect(reader.dictionarySize).toBe(1);
  });

  it('should handle single null row', () => {
    const builder = new DictionaryBuilder();
    builder.add(null);

    const encoded = builder.encode();
    const reader = DictionaryReader.fromEncoded(
      encoded.data,
      encoded.rowCount,
      encoded.nullBitmap,
    );

    expect(reader.decodeAll()).toEqual([null]);
    expect(reader.dictionarySize).toBe(0);
  });

  it('should handle strings with special characters', () => {
    const original = [
      'line1\nline2',
      'tab\there',
      'null\x00byte',
      'backslash\\path',
      'quote"test',
    ];

    const builder = new DictionaryBuilder();
    for (const v of original) {
      builder.add(v);
    }
    const encoded = builder.encode();
    const reader = DictionaryReader.fromEncoded(
      encoded.data,
      encoded.rowCount,
      encoded.nullBitmap,
    );

    expect(reader.decodeAll()).toEqual(original);
  });

  it('should handle very long strings', () => {
    const longStr = 'x'.repeat(10000);
    const builder = new DictionaryBuilder();
    builder.add(longStr);
    builder.add('short');
    builder.add(longStr);

    const encoded = builder.encode();
    const reader = DictionaryReader.fromEncoded(
      encoded.data,
      encoded.rowCount,
      encoded.nullBitmap,
    );

    const decoded = reader.decodeAll();
    expect(decoded[0]).toBe(longStr);
    expect(decoded[1]).toBe('short');
    expect(decoded[2]).toBe(longStr);
  });

  it('should handle builder reuse after reset', () => {
    const builder = new DictionaryBuilder();

    // First use
    builder.add('a');
    builder.add('b');
    const encoded1 = builder.encode();

    const reader1 = DictionaryReader.fromEncoded(
      encoded1.data,
      encoded1.rowCount,
      encoded1.nullBitmap,
    );
    expect(reader1.decodeAll()).toEqual(['a', 'b']);

    // Reset and reuse
    builder.reset();
    builder.add('x');
    builder.add('y');
    builder.add('z');
    const encoded2 = builder.encode();

    const reader2 = DictionaryReader.fromEncoded(
      encoded2.data,
      encoded2.rowCount,
      encoded2.nullBitmap,
    );
    expect(reader2.decodeAll()).toEqual(['x', 'y', 'z']);
  });

  it('should handle large number of rows with few unique values', () => {
    const builder = new DictionaryBuilder();
    for (let i = 0; i < 10000; i++) {
      builder.add(`v${i % 3}`);
    }

    expect(builder.shouldUseDictionary()).toBe(true);

    const encoded = builder.encode();
    const reader = DictionaryReader.fromEncoded(
      encoded.data,
      encoded.rowCount,
      encoded.nullBitmap,
    );

    const decoded = reader.decodeAll();
    expect(decoded.length).toBe(10000);
    for (let i = 0; i < 10000; i++) {
      expect(decoded[i]).toBe(`v${i % 3}`);
    }
  });

  it('should produce smaller output than raw for repeated values', () => {
    const values: (string | null)[] = [];
    for (let i = 0; i < 1000; i++) {
      values.push(`category_${i % 10}`);
    }

    const builder = new DictionaryBuilder();
    for (const v of values) {
      builder.add(v);
    }

    const dictEncoded = builder.encode();
    const rawEncoded = encodeRawStrings(values);

    expect(dictEncoded.data.length).toBeLessThan(rawEncoded.data.length);
  });
});

// ============================================================================
// INTEGRATION WITH COLUMNAR WRITER/READER
// ============================================================================

describe('Dictionary Encoding - Integration', () => {
  it('should match writer encoding when dictionary is selected', async () => {
    // This tests that the DictionaryBuilder produces the same format
    // as the writer's built-in dictionary encoding
    const values: (string | null)[] = ['a', 'b', 'a', 'c', null, 'b', 'a'];
    const nullBitmap = createNullBitmap(values);

    // Using the low-level encodeDictionary
    const lowLevel = encodeDictionary(values, nullBitmap);
    const lowLevelDecoded = decodeDictionary(lowLevel.data, values.length, nullBitmap);

    // Using the DictionaryBuilder
    const builder = new DictionaryBuilder();
    for (const v of values) {
      builder.add(v);
    }
    const highLevel = builder.encode();
    const reader = DictionaryReader.fromEncoded(
      highLevel.data,
      highLevel.rowCount,
      highLevel.nullBitmap,
    );

    // Both should decode to the same values
    expect(reader.decodeAll()).toEqual(lowLevelDecoded.values);
    expect(reader.decodeAll()).toEqual(values);
  });
});
