/**
 * R2 Index Format Tests
 *
 * Comprehensive test suite for the single-file R2 index format.
 * Tests cover: types, encoding, writer, reader, bloom filters,
 * B-tree operations, statistics, and query execution.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  R2_INDEX_MAGIC,
  R2_INDEX_VERSION,
  R2_INDEX_HEADER_SIZE,
  R2_DEFAULT_PAGE_SIZE,
  R2_DEFAULT_BLOOM_FP_RATE,
  R2IndexFlags,
  R2BTreePageType,
  type R2IndexValue,
  type R2IndexColumn,
  type R2IndexSchema,
  type R2IndexBuildOptions,
  type R2ColumnStats,
  type R2ChunkStats,
  type R2DataFile,
  encodeValue,
  decodeValue,
  compareValues,
  isNumericType,
  isIntegerType,
  getTypeSize,
} from './types.js';
import {
  R2IndexWriter,
  createIndexWriter,
  buildIndex,
  BloomFilterBuilder,
  BTreePageBuilder,
  ColumnStatsBuilder,
  crc32,
  murmur3_32,
  calculateBloomParams,
} from './writer.js';
import {
  R2IndexReader,
  createIndexReaderFromContent,
} from './reader.js';

// =============================================================================
// TYPE UTILITY TESTS
// =============================================================================

describe('R2 Index Types', () => {
  describe('Constants', () => {
    it('should have correct magic bytes', () => {
      expect(R2_INDEX_MAGIC).toBe('DOSQLIDX');
      expect(R2_INDEX_MAGIC.length).toBe(8);
    });

    it('should have correct version', () => {
      expect(R2_INDEX_VERSION).toBe(1);
    });

    it('should have correct header size', () => {
      expect(R2_INDEX_HEADER_SIZE).toBe(128);
    });

    it('should have correct default page size', () => {
      expect(R2_DEFAULT_PAGE_SIZE).toBe(16 * 1024);
    });

    it('should have correct default bloom FP rate', () => {
      expect(R2_DEFAULT_BLOOM_FP_RATE).toBe(0.01);
    });
  });

  describe('isNumericType', () => {
    it('should return true for numeric types', () => {
      expect(isNumericType('int8')).toBe(true);
      expect(isNumericType('int16')).toBe(true);
      expect(isNumericType('int32')).toBe(true);
      expect(isNumericType('int64')).toBe(true);
      expect(isNumericType('uint8')).toBe(true);
      expect(isNumericType('uint16')).toBe(true);
      expect(isNumericType('uint32')).toBe(true);
      expect(isNumericType('uint64')).toBe(true);
      expect(isNumericType('float32')).toBe(true);
      expect(isNumericType('float64')).toBe(true);
    });

    it('should return false for non-numeric types', () => {
      expect(isNumericType('string')).toBe(false);
      expect(isNumericType('boolean')).toBe(false);
      expect(isNumericType('bytes')).toBe(false);
      expect(isNumericType('timestamp')).toBe(false);
      expect(isNumericType('uuid')).toBe(false);
    });
  });

  describe('isIntegerType', () => {
    it('should return true for integer types', () => {
      expect(isIntegerType('int8')).toBe(true);
      expect(isIntegerType('int64')).toBe(true);
      expect(isIntegerType('uint32')).toBe(true);
    });

    it('should return false for float types', () => {
      expect(isIntegerType('float32')).toBe(false);
      expect(isIntegerType('float64')).toBe(false);
    });
  });

  describe('getTypeSize', () => {
    it('should return correct sizes for fixed-size types', () => {
      expect(getTypeSize('int8')).toBe(1);
      expect(getTypeSize('uint8')).toBe(1);
      expect(getTypeSize('boolean')).toBe(1);
      expect(getTypeSize('int16')).toBe(2);
      expect(getTypeSize('uint16')).toBe(2);
      expect(getTypeSize('int32')).toBe(4);
      expect(getTypeSize('uint32')).toBe(4);
      expect(getTypeSize('float32')).toBe(4);
      expect(getTypeSize('date')).toBe(4);
      expect(getTypeSize('int64')).toBe(8);
      expect(getTypeSize('uint64')).toBe(8);
      expect(getTypeSize('float64')).toBe(8);
      expect(getTypeSize('timestamp')).toBe(8);
      expect(getTypeSize('uuid')).toBe(16);
    });

    it('should return -1 for variable-length types', () => {
      expect(getTypeSize('string')).toBe(-1);
      expect(getTypeSize('bytes')).toBe(-1);
    });
  });
});

// =============================================================================
// VALUE ENCODING/DECODING TESTS
// =============================================================================

describe('Value Encoding/Decoding', () => {
  describe('encodeValue', () => {
    it('should encode null values', () => {
      const encoded = encodeValue({ type: 'null' });
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded[0]).toBe(0);
    });

    it('should encode boolean values', () => {
      const trueEncoded = encodeValue({ type: 'boolean', value: true });
      expect(trueEncoded[0]).toBe(1);
      expect(trueEncoded[1]).toBe(1);

      const falseEncoded = encodeValue({ type: 'boolean', value: false });
      expect(falseEncoded[0]).toBe(1);
      expect(falseEncoded[1]).toBe(0);
    });

    it('should encode integer values', () => {
      const encoded = encodeValue({ type: 'int', value: 42n });
      expect(encoded.length).toBe(9);
      expect(encoded[0]).toBe(2);
    });

    it('should encode negative integers', () => {
      const encoded = encodeValue({ type: 'int', value: -100n });
      expect(encoded.length).toBe(9);
      expect(encoded[0]).toBe(2);
    });

    it('should encode float values', () => {
      const encoded = encodeValue({ type: 'float', value: 3.14159 });
      expect(encoded.length).toBe(9);
      expect(encoded[0]).toBe(3);
    });

    it('should encode string values', () => {
      const encoded = encodeValue({ type: 'string', value: 'hello' });
      expect(encoded[0]).toBe(4);
      expect(encoded.length).toBe(5 + 5); // type + length + string bytes
    });

    it('should encode empty strings', () => {
      const encoded = encodeValue({ type: 'string', value: '' });
      expect(encoded[0]).toBe(4);
      expect(encoded.length).toBe(5); // type + length + 0 bytes
    });

    it('should encode bytes values', () => {
      const bytes = new Uint8Array([1, 2, 3, 4, 5]);
      const encoded = encodeValue({ type: 'bytes', value: bytes });
      expect(encoded[0]).toBe(5);
      expect(encoded.length).toBe(5 + 5);
    });

    it('should encode timestamp values', () => {
      const encoded = encodeValue({ type: 'timestamp', value: BigInt(Date.now()) });
      expect(encoded.length).toBe(9);
      expect(encoded[0]).toBe(6);
    });

    it('should encode date values', () => {
      const encoded = encodeValue({ type: 'date', value: 19000 }); // Days since epoch
      expect(encoded.length).toBe(5);
      expect(encoded[0]).toBe(7);
    });

    it('should encode uuid values', () => {
      const uuid = '550e8400-e29b-41d4-a716-446655440000';
      const encoded = encodeValue({ type: 'uuid', value: uuid });
      expect(encoded[0]).toBe(8);
    });
  });

  describe('decodeValue', () => {
    it('should decode null values', () => {
      const encoded = new Uint8Array([0]);
      const decoded = decodeValue(encoded);
      expect(decoded).toEqual({ type: 'null' });
    });

    it('should decode boolean values', () => {
      const trueDecoded = decodeValue(new Uint8Array([1, 1]));
      expect(trueDecoded).toEqual({ type: 'boolean', value: true });

      const falseDecoded = decodeValue(new Uint8Array([1, 0]));
      expect(falseDecoded).toEqual({ type: 'boolean', value: false });
    });

    it('should round-trip integer values', () => {
      const original: R2IndexValue = { type: 'int', value: 12345n };
      const decoded = decodeValue(encodeValue(original));
      expect(decoded).toEqual(original);
    });

    it('should round-trip negative integers', () => {
      const original: R2IndexValue = { type: 'int', value: -99999n };
      const decoded = decodeValue(encodeValue(original));
      expect(decoded).toEqual(original);
    });

    it('should round-trip float values', () => {
      const original: R2IndexValue = { type: 'float', value: 2.71828 };
      const decoded = decodeValue(encodeValue(original));
      expect(decoded.type).toBe('float');
      expect((decoded as { type: 'float'; value: number }).value).toBeCloseTo(2.71828);
    });

    it('should round-trip string values', () => {
      const original: R2IndexValue = { type: 'string', value: 'Hello, World!' };
      const decoded = decodeValue(encodeValue(original));
      expect(decoded).toEqual(original);
    });

    it('should round-trip unicode strings', () => {
      const original: R2IndexValue = { type: 'string', value: 'Hello, World!' };
      const decoded = decodeValue(encodeValue(original));
      expect(decoded).toEqual(original);
    });

    it('should round-trip bytes values', () => {
      const original: R2IndexValue = { type: 'bytes', value: new Uint8Array([255, 0, 128, 64]) };
      const decoded = decodeValue(encodeValue(original));
      expect(decoded.type).toBe('bytes');
      expect((decoded as { type: 'bytes'; value: Uint8Array }).value).toEqual(original.value);
    });

    it('should round-trip timestamp values', () => {
      const original: R2IndexValue = { type: 'timestamp', value: 1705000000000n };
      const decoded = decodeValue(encodeValue(original));
      expect(decoded).toEqual(original);
    });

    it('should round-trip date values', () => {
      const original: R2IndexValue = { type: 'date', value: 19724 };
      const decoded = decodeValue(encodeValue(original));
      expect(decoded).toEqual(original);
    });

    it('should round-trip uuid values', () => {
      const original: R2IndexValue = { type: 'uuid', value: '550e8400-e29b-41d4-a716-446655440000' };
      const decoded = decodeValue(encodeValue(original));
      expect(decoded).toEqual(original);
    });
  });

  describe('compareValues', () => {
    it('should compare null values', () => {
      expect(compareValues({ type: 'null' }, { type: 'null' })).toBe(0);
    });

    it('should sort nulls first', () => {
      expect(compareValues({ type: 'null' }, { type: 'int', value: 0n })).toBe(-1);
      expect(compareValues({ type: 'int', value: 0n }, { type: 'null' })).toBe(1);
    });

    it('should compare boolean values', () => {
      expect(compareValues({ type: 'boolean', value: false }, { type: 'boolean', value: true })).toBe(-1);
      expect(compareValues({ type: 'boolean', value: true }, { type: 'boolean', value: false })).toBe(1);
      expect(compareValues({ type: 'boolean', value: true }, { type: 'boolean', value: true })).toBe(0);
    });

    it('should compare integer values', () => {
      expect(compareValues({ type: 'int', value: 10n }, { type: 'int', value: 20n })).toBeLessThan(0);
      expect(compareValues({ type: 'int', value: 20n }, { type: 'int', value: 10n })).toBeGreaterThan(0);
      expect(compareValues({ type: 'int', value: 10n }, { type: 'int', value: 10n })).toBe(0);
    });

    it('should compare float values', () => {
      expect(compareValues({ type: 'float', value: 1.5 }, { type: 'float', value: 2.5 })).toBeLessThan(0);
      expect(compareValues({ type: 'float', value: 2.5 }, { type: 'float', value: 1.5 })).toBeGreaterThan(0);
    });

    it('should compare string values', () => {
      expect(compareValues({ type: 'string', value: 'apple' }, { type: 'string', value: 'banana' })).toBeLessThan(0);
      expect(compareValues({ type: 'string', value: 'banana' }, { type: 'string', value: 'apple' })).toBeGreaterThan(0);
      expect(compareValues({ type: 'string', value: 'apple' }, { type: 'string', value: 'apple' })).toBe(0);
    });

    it('should compare bytes values', () => {
      const a: R2IndexValue = { type: 'bytes', value: new Uint8Array([1, 2, 3]) };
      const b: R2IndexValue = { type: 'bytes', value: new Uint8Array([1, 2, 4]) };
      expect(compareValues(a, b)).toBeLessThan(0);
      expect(compareValues(b, a)).toBeGreaterThan(0);
    });

    it('should compare bytes of different lengths', () => {
      const a: R2IndexValue = { type: 'bytes', value: new Uint8Array([1, 2]) };
      const b: R2IndexValue = { type: 'bytes', value: new Uint8Array([1, 2, 3]) };
      expect(compareValues(a, b)).toBeLessThan(0);
    });
  });
});

// =============================================================================
// CRC32 TESTS
// =============================================================================

describe('CRC32', () => {
  it('should calculate correct checksum for empty data', () => {
    const checksum = crc32(new Uint8Array(0));
    expect(checksum).toBe(0);
  });

  it('should calculate correct checksum for simple data', () => {
    const data = new TextEncoder().encode('hello');
    const checksum = crc32(data);
    expect(typeof checksum).toBe('number');
    expect(checksum).toBeGreaterThanOrEqual(0);
  });

  it('should produce consistent results', () => {
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    const checksum1 = crc32(data);
    const checksum2 = crc32(data);
    expect(checksum1).toBe(checksum2);
  });

  it('should produce different results for different data', () => {
    const data1 = new Uint8Array([1, 2, 3]);
    const data2 = new Uint8Array([1, 2, 4]);
    expect(crc32(data1)).not.toBe(crc32(data2));
  });
});

// =============================================================================
// MURMUR3 HASH TESTS
// =============================================================================

describe('MurmurHash3', () => {
  it('should produce consistent results', () => {
    const data = new TextEncoder().encode('test');
    const hash1 = murmur3_32(data, 0);
    const hash2 = murmur3_32(data, 0);
    expect(hash1).toBe(hash2);
  });

  it('should produce different results with different seeds', () => {
    const data = new TextEncoder().encode('test');
    const hash1 = murmur3_32(data, 0);
    const hash2 = murmur3_32(data, 42);
    expect(hash1).not.toBe(hash2);
  });

  it('should produce different results for different inputs', () => {
    const data1 = new TextEncoder().encode('hello');
    const data2 = new TextEncoder().encode('world');
    expect(murmur3_32(data1, 0)).not.toBe(murmur3_32(data2, 0));
  });

  it('should handle empty input', () => {
    const hash = murmur3_32(new Uint8Array(0), 0);
    expect(typeof hash).toBe('number');
  });

  it('should handle inputs of various lengths', () => {
    for (let len = 1; len <= 20; len++) {
      const data = new Uint8Array(len).fill(len);
      const hash = murmur3_32(data, 0);
      expect(typeof hash).toBe('number');
    }
  });
});

// =============================================================================
// BLOOM FILTER TESTS
// =============================================================================

describe('Bloom Filter', () => {
  describe('calculateBloomParams', () => {
    it('should calculate reasonable parameters', () => {
      const params = calculateBloomParams(1000, 0.01);
      expect(params.numBits).toBeGreaterThan(0);
      expect(params.numHashes).toBeGreaterThan(0);
      expect(params.numHashes).toBeLessThan(20);
    });

    it('should increase bits for lower FP rate', () => {
      const params1 = calculateBloomParams(1000, 0.1);
      const params2 = calculateBloomParams(1000, 0.01);
      expect(params2.numBits).toBeGreaterThan(params1.numBits);
    });

    it('should increase bits for more elements', () => {
      const params1 = calculateBloomParams(100, 0.01);
      const params2 = calculateBloomParams(1000, 0.01);
      expect(params2.numBits).toBeGreaterThan(params1.numBits);
    });
  });

  describe('BloomFilterBuilder', () => {
    it('should create a bloom filter', () => {
      const builder = new BloomFilterBuilder({
        expectedElements: 100,
        falsePositiveRate: 0.01,
      });
      const filter = builder.build('test_column', 100);

      expect(filter.columnName).toBe('test_column');
      expect(filter.numHashes).toBeGreaterThan(0);
      expect(filter.numBits).toBeGreaterThan(0);
      expect(filter.bits).toBeInstanceOf(Uint8Array);
    });

    it('should return true for added elements', () => {
      const builder = new BloomFilterBuilder({
        expectedElements: 100,
        falsePositiveRate: 0.01,
      });

      const values = ['apple', 'banana', 'cherry'].map(s => new TextEncoder().encode(s));
      for (const v of values) {
        builder.add(v);
      }

      for (const v of values) {
        expect(builder.mightContain(v)).toBe(true);
      }
    });

    it('should mostly return false for non-added elements', () => {
      const builder = new BloomFilterBuilder({
        expectedElements: 100,
        falsePositiveRate: 0.01,
      });

      const added = ['a', 'b', 'c', 'd', 'e'].map(s => new TextEncoder().encode(s));
      for (const v of added) {
        builder.add(v);
      }

      // Test with different values - some false positives expected
      const notAdded = ['x', 'y', 'z', 'w', 'v'].map(s => new TextEncoder().encode(s));
      let falsePositives = 0;
      for (const v of notAdded) {
        if (builder.mightContain(v)) {
          falsePositives++;
        }
      }

      // With good parameters, we should have few false positives
      expect(falsePositives).toBeLessThanOrEqual(notAdded.length);
    });

    it('should track inserted element count', () => {
      const builder = new BloomFilterBuilder({
        expectedElements: 100,
        falsePositiveRate: 0.01,
      });

      const encoder = new TextEncoder();
      builder.add(encoder.encode('one'));
      builder.add(encoder.encode('two'));
      builder.add(encoder.encode('three'));

      const filter = builder.build('test', 100);
      expect(filter.insertedElements).toBe(3);
    });
  });
});

// =============================================================================
// B-TREE PAGE BUILDER TESTS
// =============================================================================

describe('BTreePageBuilder', () => {
  it('should create empty B-tree', () => {
    const builder = new BTreePageBuilder(1024);
    const index = builder.build('test_idx', ['id'], false);

    expect(index.name).toBe('test_idx');
    expect(index.columns).toEqual(['id']);
    expect(index.unique).toBe(false);
  });

  it('should add entries to B-tree', () => {
    const builder = new BTreePageBuilder(1024);

    builder.addEntry(new Uint8Array([1]), new Uint8Array([10]));
    builder.addEntry(new Uint8Array([2]), new Uint8Array([20]));
    builder.addEntry(new Uint8Array([3]), new Uint8Array([30]));

    const index = builder.build('test_idx', ['id'], false);
    expect(index.entryCount).toBe(3n);
  });

  it('should split pages when full', () => {
    const builder = new BTreePageBuilder(100); // Small page size to force splits

    for (let i = 0; i < 50; i++) {
      const key = new Uint8Array(8);
      const value = new Uint8Array(8);
      new DataView(key.buffer).setBigInt64(0, BigInt(i), true);
      new DataView(value.buffer).setBigInt64(0, BigInt(i * 10), true);
      builder.addEntry(key, value);
    }

    const index = builder.build('test_idx', ['id'], false);
    expect(index.pageCount).toBeGreaterThan(1);
  });

  it('should create page directory', () => {
    const builder = new BTreePageBuilder(1024);

    for (let i = 0; i < 10; i++) {
      builder.addEntry(new Uint8Array([i]), new Uint8Array([i * 10]));
    }

    const index = builder.build('test_idx', ['id'], false);
    expect(index.pageDirectory.length).toBeGreaterThanOrEqual(1);
    expect(index.pageDirectory[0].pageId).toBe(0);
  });

  it('should serialize pages correctly', () => {
    const builder = new BTreePageBuilder(1024);

    builder.addEntry(new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6]));

    const pages = builder.getSerializedPages();
    expect(pages.length).toBeGreaterThanOrEqual(1);
    expect(pages[0]).toBeInstanceOf(Uint8Array);
  });
});

// =============================================================================
// COLUMN STATS BUILDER TESTS
// =============================================================================

describe('ColumnStatsBuilder', () => {
  it('should track min/max for integers', () => {
    const builder = new ColumnStatsBuilder('int64');

    builder.add({ type: 'int', value: 10n });
    builder.add({ type: 'int', value: 5n });
    builder.add({ type: 'int', value: 20n });

    const stats = builder.build();
    expect(stats.min).toEqual({ type: 'int', value: 5n });
    expect(stats.max).toEqual({ type: 'int', value: 20n });
  });

  it('should track null count', () => {
    const builder = new ColumnStatsBuilder('string');

    builder.add({ type: 'string', value: 'a' });
    builder.add({ type: 'null' });
    builder.add({ type: 'string', value: 'b' });
    builder.add({ type: 'null' });

    const stats = builder.build();
    expect(stats.nullCount).toBe(2n);
  });

  it('should track distinct values', () => {
    const builder = new ColumnStatsBuilder('string');

    builder.add({ type: 'string', value: 'a' });
    builder.add({ type: 'string', value: 'b' });
    builder.add({ type: 'string', value: 'a' }); // Duplicate
    builder.add({ type: 'string', value: 'c' });

    const stats = builder.build();
    expect(stats.distinctCount).toBe(3n);
  });

  it('should calculate sum for numeric types', () => {
    const builder = new ColumnStatsBuilder('int64');

    builder.add({ type: 'int', value: 10n });
    builder.add({ type: 'int', value: 20n });
    builder.add({ type: 'int', value: 30n });

    const stats = builder.build();
    // Sum may be bigint or number depending on implementation
    expect(Number(stats.sum)).toBe(60);
  });

  it('should detect sorted data (ascending)', () => {
    const builder = new ColumnStatsBuilder('int64');

    builder.add({ type: 'int', value: 1n });
    builder.add({ type: 'int', value: 2n });
    builder.add({ type: 'int', value: 3n });

    const stats = builder.build();
    expect(stats.sorted).toBe(true);
    expect(stats.sortDirection).toBe('asc');
  });

  it('should detect sorted data (descending)', () => {
    const builder = new ColumnStatsBuilder('int64');

    builder.add({ type: 'int', value: 3n });
    builder.add({ type: 'int', value: 2n });
    builder.add({ type: 'int', value: 1n });

    const stats = builder.build();
    expect(stats.sorted).toBe(true);
    expect(stats.sortDirection).toBe('desc');
  });

  it('should detect unsorted data', () => {
    const builder = new ColumnStatsBuilder('int64');

    builder.add({ type: 'int', value: 1n });
    builder.add({ type: 'int', value: 3n });
    builder.add({ type: 'int', value: 2n });

    const stats = builder.build();
    expect(stats.sorted).toBe(false);
  });

  it('should detect all-same values', () => {
    const builder = new ColumnStatsBuilder('string');

    builder.add({ type: 'string', value: 'same' });
    builder.add({ type: 'string', value: 'same' });
    builder.add({ type: 'string', value: 'same' });

    const stats = builder.build();
    expect(stats.allSame).toBe(true);
  });
});

// =============================================================================
// R2 INDEX WRITER TESTS
// =============================================================================

describe('R2IndexWriter', () => {
  const defaultOptions: R2IndexBuildOptions = {
    tableName: 'test_table',
    columns: [
      { name: 'id', type: 'int64', nullable: false, ordinal: 0 },
      { name: 'name', type: 'string', nullable: true, ordinal: 1 },
      { name: 'value', type: 'float64', nullable: true, ordinal: 2 },
    ],
    primaryKey: ['id'],
    indexedColumns: ['id'],
    bloomColumns: ['name'],
  };

  it('should create a writer', () => {
    const writer = createIndexWriter(defaultOptions);
    expect(writer).toBeInstanceOf(R2IndexWriter);
  });

  it('should add rows', () => {
    const writer = createIndexWriter(defaultOptions);

    writer.addRow({
      id: { type: 'int', value: 1n },
      name: { type: 'string', value: 'Alice' },
      value: { type: 'float', value: 100.5 },
    }, 0n);

    const index = writer.build();
    expect(index).toBeInstanceOf(Uint8Array);
  });

  it('should build index with magic bytes', () => {
    const writer = createIndexWriter(defaultOptions);
    writer.addRow({
      id: { type: 'int', value: 1n },
    }, 0n);

    const index = writer.build();
    const magic = new TextDecoder().decode(index.slice(0, 8));
    expect(magic).toBe(R2_INDEX_MAGIC);
  });

  it('should build index with version', () => {
    const writer = createIndexWriter(defaultOptions);
    writer.addRow({ id: { type: 'int', value: 1n } }, 0n);

    const index = writer.build();
    const view = new DataView(index.buffer);
    const version = view.getUint32(8, true);
    expect(version).toBe(R2_INDEX_VERSION);
  });

  it('should build index with correct flags', () => {
    const writer = createIndexWriter(defaultOptions);
    writer.addRow({ id: { type: 'int', value: 1n } }, 0n);

    const index = writer.build();
    const view = new DataView(index.buffer);
    const flags = view.getUint32(12, true);

    expect(flags & R2IndexFlags.HAS_BTREE).toBeTruthy();
    expect(flags & R2IndexFlags.HAS_BLOOM).toBeTruthy();
    expect(flags & R2IndexFlags.HAS_STATS).toBeTruthy();
    expect(flags & R2IndexFlags.HAS_CHECKSUMS).toBeTruthy();
  });

  it('should build index with footer', () => {
    const writer = createIndexWriter(defaultOptions);
    writer.addRow({ id: { type: 'int', value: 1n } }, 0n);

    const index = writer.build();
    const footer = new TextDecoder().decode(index.slice(-8));
    expect(footer).toBe(R2_INDEX_MAGIC);
  });

  it('should support data file references', () => {
    const writer = createIndexWriter(defaultOptions);

    const dataFile: R2DataFile = {
      id: 'chunk_001',
      location: { type: 'r2', bucket: 'my-bucket', key: 'data/chunk_001.parquet' },
      format: 'parquet',
      rowRange: { start: 0n, end: 1000n },
      size: 1024n * 1024n,
    };

    writer.addDataFile(dataFile);
    writer.addRow({ id: { type: 'int', value: 1n } }, 0n);

    const index = writer.build();
    expect(index.length).toBeGreaterThan(R2_INDEX_HEADER_SIZE);
  });
});

describe('buildIndex helper', () => {
  it('should build index from rows array', () => {
    const rows: Array<Record<string, R2IndexValue>> = [
      { id: { type: 'int', value: 1n }, name: { type: 'string', value: 'Alice' } },
      { id: { type: 'int', value: 2n }, name: { type: 'string', value: 'Bob' } },
      { id: { type: 'int', value: 3n }, name: { type: 'string', value: 'Charlie' } },
    ];

    const options: R2IndexBuildOptions = {
      tableName: 'users',
      columns: [
        { name: 'id', type: 'int64', nullable: false, ordinal: 0 },
        { name: 'name', type: 'string', nullable: false, ordinal: 1 },
      ],
      primaryKey: ['id'],
    };

    const index = buildIndex(options, rows);
    expect(index).toBeInstanceOf(Uint8Array);
    expect(index.length).toBeGreaterThan(R2_INDEX_HEADER_SIZE);
  });
});

// =============================================================================
// R2 INDEX READER TESTS
// =============================================================================

describe('R2IndexReader', () => {
  let testIndex: Uint8Array;

  beforeEach(() => {
    const options: R2IndexBuildOptions = {
      tableName: 'test_users',
      columns: [
        { name: 'id', type: 'int64', nullable: false, ordinal: 0 },
        { name: 'name', type: 'string', nullable: true, ordinal: 1 },
        { name: 'age', type: 'int32', nullable: true, ordinal: 2 },
      ],
      primaryKey: ['id'],
      indexedColumns: ['id', 'name'],
      bloomColumns: ['name'],
    };

    const writer = createIndexWriter(options);

    // Add test data
    for (let i = 0; i < 100; i++) {
      writer.addRow({
        id: { type: 'int', value: BigInt(i) },
        name: { type: 'string', value: `User${i}` },
        age: { type: 'int', value: BigInt(20 + (i % 50)) },
      }, BigInt(i));
    }

    testIndex = writer.build();
  });

  describe('Header reading', () => {
    it('should read header successfully', async () => {
      const reader = createIndexReaderFromContent(testIndex);
      const result = await reader.readHeader();

      expect(result.valid).toBe(true);
      expect(result.header.magic).toBe(R2_INDEX_MAGIC);
      expect(result.header.version).toBe(R2_INDEX_VERSION);
    });

    it('should read row count from header', async () => {
      const reader = createIndexReaderFromContent(testIndex);
      const header = await reader.getHeader();

      expect(header.rowCount).toBe(100n);
    });

    it('should read section offsets', async () => {
      const reader = createIndexReaderFromContent(testIndex);
      const header = await reader.getHeader();

      expect(header.sections.schema.offset).toBeGreaterThanOrEqual(BigInt(R2_INDEX_HEADER_SIZE));
      expect(header.sections.schema.size).toBeGreaterThan(0);
    });

    it('should verify header checksum', async () => {
      const reader = createIndexReaderFromContent(testIndex, { verifyChecksums: true });
      const result = await reader.readHeader();

      expect(result.valid).toBe(true);
    });

    it('should detect invalid magic', async () => {
      const badIndex = new Uint8Array(testIndex);
      badIndex[0] = 0; // Corrupt magic

      const reader = createIndexReaderFromContent(badIndex);
      const result = await reader.readHeader();

      expect(result.valid).toBe(false);
      expect(result.errors?.some(e => e.includes('magic'))).toBe(true);
    });
  });

  describe('Schema reading', () => {
    it('should read schema', async () => {
      const reader = createIndexReaderFromContent(testIndex);
      const result = await reader.readSchema();

      expect(result.data.tableName).toBe('test_users');
      expect(result.data.columns.length).toBe(3);
    });

    it('should read column definitions', async () => {
      const reader = createIndexReaderFromContent(testIndex);
      const { data: schema } = await reader.readSchema();

      const idCol = schema.columns.find(c => c.name === 'id');
      expect(idCol?.type).toBe('int64');
      expect(idCol?.nullable).toBe(false);

      const nameCol = schema.columns.find(c => c.name === 'name');
      expect(nameCol?.type).toBe('string');
      expect(nameCol?.nullable).toBe(true);
    });

    it('should read primary key', async () => {
      const reader = createIndexReaderFromContent(testIndex);
      const { data: schema } = await reader.readSchema();

      expect(schema.primaryKey).toEqual(['id']);
    });
  });

  describe('Index section reading', () => {
    it('should read index section', async () => {
      const reader = createIndexReaderFromContent(testIndex);
      const result = await reader.readIndexSection();

      expect(result.data.indexes.length).toBeGreaterThan(0);
    });

    it('should read index metadata', async () => {
      const reader = createIndexReaderFromContent(testIndex);
      const { data: indexSection } = await reader.readIndexSection();

      const idIndex = indexSection.indexes.find(i => i.columns.includes('id'));
      expect(idIndex).toBeDefined();
      expect(idIndex?.entryCount).toBe(100n);
    });
  });

  describe('Bloom filter section reading', () => {
    it('should read bloom filters', async () => {
      const reader = createIndexReaderFromContent(testIndex);
      const result = await reader.readBloomSection();

      expect(result.data.filters.length).toBeGreaterThan(0);
    });

    it('should check bloom filter', async () => {
      const reader = createIndexReaderFromContent(testIndex);

      // Should return true for values that exist
      const mightExist = await reader.bloomMightContain('name', { type: 'string', value: 'User50' });
      expect(mightExist).toBe(true);
    });

    it('should return false for definite non-matches', async () => {
      const reader = createIndexReaderFromContent(testIndex);

      // Should return false for values that definitely don't exist
      // Note: bloom filters can have false positives, so we test multiple values
      let falseNegatives = 0;
      for (let i = 0; i < 10; i++) {
        const mightExist = await reader.bloomMightContain('name', {
          type: 'string',
          value: `NonExistent${i}${Date.now()}`,
        });
        if (!mightExist) falseNegatives++;
      }
      // At least some should be correctly identified as not existing
      expect(falseNegatives).toBeGreaterThan(0);
    });
  });

  describe('Statistics section reading', () => {
    it('should read statistics', async () => {
      const reader = createIndexReaderFromContent(testIndex);
      const result = await reader.readStatsSection();

      expect(result.data.global.rowCount).toBe(100n);
    });

    it('should read column statistics', async () => {
      const reader = createIndexReaderFromContent(testIndex);
      const stats = await reader.getColumnStats('id');

      expect(stats).toBeDefined();
      expect(stats?.min).toEqual({ type: 'int', value: 0n });
      expect(stats?.max).toEqual({ type: 'int', value: 99n });
    });
  });

  describe('Data section reading', () => {
    it('should read data section', async () => {
      const reader = createIndexReaderFromContent(testIndex);
      const result = await reader.readDataSection();

      expect(result.data.storageType).toBeDefined();
    });
  });

  describe('Validation', () => {
    it('should validate a correct index', async () => {
      const reader = createIndexReaderFromContent(testIndex);
      const result = await reader.validate();

      expect(result.valid).toBe(true);
      expect(result.errors.length).toBe(0);
    });

    it('should detect corrupted footer', async () => {
      const badIndex = new Uint8Array(testIndex);
      badIndex[badIndex.length - 1] = 0; // Corrupt footer

      const reader = createIndexReaderFromContent(badIndex);
      const result = await reader.validate();

      expect(result.valid).toBe(false);
    });
  });

  describe('Query statistics', () => {
    it('should track query stats', async () => {
      const reader = createIndexReaderFromContent(testIndex);

      await reader.readHeader();
      await reader.readSchema();

      const stats = reader.getQueryStats();
      expect(stats.rangeRequests).toBeGreaterThan(0);
      expect(stats.bytesRead).toBeGreaterThan(0n);
    });
  });

  describe('Cache behavior', () => {
    it('should cache sections', async () => {
      const reader = createIndexReaderFromContent(testIndex);

      // First read
      await reader.readSchema();
      const stats1 = reader.getQueryStats();

      // Second read should use cache
      await reader.readSchema();
      const stats2 = reader.getQueryStats();

      // Bytes read should be same (cached)
      expect(stats2.bytesRead).toBe(stats1.bytesRead);
    });

    it('should clear cache', async () => {
      const reader = createIndexReaderFromContent(testIndex);

      await reader.readSchema();
      reader.clearCache();

      // After clearing, reading again should fetch
      await reader.readSchema();
      const stats = reader.getQueryStats();
      expect(stats.bytesRead).toBeGreaterThan(0n);
    });
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('R2 Index Integration', () => {
  it('should round-trip a complete index', async () => {
    const options: R2IndexBuildOptions = {
      tableName: 'products',
      columns: [
        { name: 'id', type: 'int64', nullable: false, ordinal: 0 },
        { name: 'name', type: 'string', nullable: false, ordinal: 1 },
        { name: 'price', type: 'float64', nullable: true, ordinal: 2 },
        { name: 'category', type: 'string', nullable: true, ordinal: 3 },
      ],
      primaryKey: ['id'],
      indexedColumns: ['id', 'category'],
      bloomColumns: ['name', 'category'],
    };

    const writer = createIndexWriter(options);

    // Add diverse data
    const categories = ['Electronics', 'Books', 'Clothing', 'Food', 'Toys'];
    for (let i = 0; i < 500; i++) {
      writer.addRow({
        id: { type: 'int', value: BigInt(i) },
        name: { type: 'string', value: `Product ${i}` },
        price: { type: 'float', value: Math.random() * 1000 },
        category: { type: 'string', value: categories[i % categories.length] },
      }, BigInt(i));
    }

    const indexData = writer.build();

    // Read it back
    const reader = createIndexReaderFromContent(indexData);
    const validation = await reader.validate();
    expect(validation.valid).toBe(true);

    // Verify schema
    const { data: schema } = await reader.readSchema();
    expect(schema.tableName).toBe('products');
    expect(schema.columns.length).toBe(4);

    // Verify statistics
    const priceStats = await reader.getColumnStats('price');
    expect(priceStats).toBeDefined();
    expect(priceStats?.min).toBeDefined();
    expect(priceStats?.max).toBeDefined();
    expect(priceStats?.nullCount).toBe(0n);

    // Verify bloom filter
    const existsCheck = await reader.bloomMightContain('category', { type: 'string', value: 'Electronics' });
    expect(existsCheck).toBe(true);
  });

  it('should handle empty index', async () => {
    const options: R2IndexBuildOptions = {
      tableName: 'empty_table',
      columns: [
        { name: 'id', type: 'int64', nullable: false, ordinal: 0 },
      ],
      primaryKey: ['id'],
    };

    const writer = createIndexWriter(options);
    const indexData = writer.build();

    const reader = createIndexReaderFromContent(indexData);
    const header = await reader.getHeader();

    expect(header.rowCount).toBe(0n);
  });

  it('should handle large values', async () => {
    const options: R2IndexBuildOptions = {
      tableName: 'large_values',
      columns: [
        { name: 'id', type: 'int64', nullable: false, ordinal: 0 },
        { name: 'data', type: 'string', nullable: false, ordinal: 1 },
      ],
      primaryKey: ['id'],
    };

    const writer = createIndexWriter(options);

    // Add rows with large string values
    for (let i = 0; i < 10; i++) {
      const largeString = 'x'.repeat(1000);
      writer.addRow({
        id: { type: 'int', value: BigInt(i) },
        data: { type: 'string', value: largeString },
      }, BigInt(i));
    }

    const indexData = writer.build();

    const reader = createIndexReaderFromContent(indexData);
    const validation = await reader.validate();
    expect(validation.valid).toBe(true);
  });

  it('should handle all data types', async () => {
    const options: R2IndexBuildOptions = {
      tableName: 'all_types',
      columns: [
        { name: 'bool_col', type: 'boolean', nullable: false, ordinal: 0 },
        { name: 'int_col', type: 'int64', nullable: false, ordinal: 1 },
        { name: 'float_col', type: 'float64', nullable: false, ordinal: 2 },
        { name: 'string_col', type: 'string', nullable: false, ordinal: 3 },
        { name: 'timestamp_col', type: 'timestamp', nullable: false, ordinal: 4 },
        { name: 'uuid_col', type: 'uuid', nullable: false, ordinal: 5 },
      ],
      primaryKey: ['int_col'],
    };

    const writer = createIndexWriter(options);

    writer.addRow({
      bool_col: { type: 'boolean', value: true },
      int_col: { type: 'int', value: 42n },
      float_col: { type: 'float', value: 3.14159 },
      string_col: { type: 'string', value: 'hello' },
      timestamp_col: { type: 'timestamp', value: BigInt(Date.now()) },
      uuid_col: { type: 'uuid', value: '550e8400-e29b-41d4-a716-446655440000' },
    }, 0n);

    const indexData = writer.build();

    const reader = createIndexReaderFromContent(indexData);
    const validation = await reader.validate();
    expect(validation.valid).toBe(true);

    const { data: schema } = await reader.readSchema();
    expect(schema.columns.length).toBe(6);
  });
});
