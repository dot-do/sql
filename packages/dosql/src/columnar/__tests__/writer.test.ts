/**
 * Columnar Writer Tests
 *
 * Comprehensive tests for columnar write operations including:
 * - Write operations and flushing
 * - Encoding selection
 * - Edge cases with nulls and large columns
 *
 * Issue: sql-4ku7 - Columnar Storage Tests
 */

import { describe, it, expect } from 'vitest';

import { ColumnarWriter, writeColumnar, inferSchema } from '../writer.js';
import { serializeRowGroup, deserializeRowGroup } from '../chunk.js';
import type { ColumnarTableSchema, FSXInterface, RowGroup, Encoding } from '../types.js';

// ============================================================================
// TEST FIXTURES
// ============================================================================

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

// ============================================================================
// BASIC WRITER TESTS
// ============================================================================

describe('ColumnarWriter - Basic Operations', () => {
  it('should create writer with default config', () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    expect(writer).toBeDefined();
    expect(writer.getSchema()).toBe(schema);
    expect(writer.getBufferedRowCount()).toBe(0);
    expect(writer.getTotalRowCount()).toBe(0);
  });

  it('should buffer rows without flushing', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    const flushed = await writer.write([{ id: 1 }, { id: 2 }, { id: 3 }]);

    expect(flushed).toHaveLength(0);
    expect(writer.getBufferedRowCount()).toBe(3);
    expect(writer.getTotalRowCount()).toBe(3);
  });

  it('should finalize and flush remaining rows', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([{ id: 1 }, { id: 2 }]);

    const rowGroup = await writer.finalize();

    expect(rowGroup).toBeDefined();
    expect(rowGroup!.rowCount).toBe(2);
    expect(writer.getBufferedRowCount()).toBe(0);
  });

  it('should return null from finalize when no rows buffered', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    const rowGroup = await writer.finalize();

    expect(rowGroup).toBeNull();
  });

  it('should store row groups in FSX', async () => {
    const fsx = createInMemoryFSX();
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema, {}, fsx);
    await writer.write([{ id: 1 }]);
    await writer.finalize();

    const keys = await fsx.list('test/rowgroups/');
    expect(keys).toHaveLength(1);
  });
});

// ============================================================================
// AUTO-FLUSH TESTS
// ============================================================================

describe('ColumnarWriter - Auto Flush', () => {
  it('should auto-flush at row limit', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema, { targetRowsPerGroup: 10 });

    // Write 25 rows
    const flushed = await writer.write(
      Array.from({ length: 25 }, (_, i) => ({ id: i }))
    );

    expect(flushed).toHaveLength(2); // 2 groups of 10
    expect(writer.getBufferedRowCount()).toBe(5); // 5 remaining
    expect(writer.getTotalRowCount()).toBe(25);
  });

  it('should auto-flush at byte size limit', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    // Very small byte limit to force early flush
    const writer = new ColumnarWriter(schema, {
      targetBytesPerGroup: 32, // 32 bytes = ~8 int32 values
      targetRowsPerGroup: 1000, // High row limit so byte limit triggers first
    });

    const flushed = await writer.write(
      Array.from({ length: 20 }, (_, i) => ({ id: i }))
    );

    // Should have flushed multiple times due to byte limit
    expect(flushed.length).toBeGreaterThanOrEqual(1);
  });

  it('should track flushed row groups', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema, { targetRowsPerGroup: 5 });
    await writer.write(Array.from({ length: 15 }, (_, i) => ({ id: i })));
    await writer.finalize();

    const flushedGroups = writer.getFlushedRowGroups();
    expect(flushedGroups).toHaveLength(3);
  });
});

// ============================================================================
// ON-FLUSH CALLBACK TESTS
// ============================================================================

describe('ColumnarWriter - onFlush Callback', () => {
  it('should call onFlush callback for each flush', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const flushCalls: { rowGroup: RowGroup; data: Uint8Array }[] = [];

    const writer = new ColumnarWriter(schema, {
      targetRowsPerGroup: 5,
      onFlush: async (rowGroup, data) => {
        flushCalls.push({ rowGroup, data });
      },
    });

    await writer.write(Array.from({ length: 15 }, (_, i) => ({ id: i })));
    await writer.finalize();

    expect(flushCalls).toHaveLength(3);
    expect(flushCalls[0].rowGroup.rowCount).toBe(5);
    expect(flushCalls[0].data).toBeInstanceOf(Uint8Array);
  });

  it('should include serialized data in callback', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    let capturedData: Uint8Array | null = null;

    const writer = new ColumnarWriter(schema, {
      onFlush: async (_, data) => {
        capturedData = data;
      },
    });

    await writer.write([{ id: 1 }, { id: 2 }]);
    await writer.finalize();

    expect(capturedData).toBeDefined();

    // Verify we can deserialize it
    const rowGroup = deserializeRowGroup(capturedData!);
    expect(rowGroup.rowCount).toBe(2);
  });
});

// ============================================================================
// ENCODING SELECTION TESTS
// ============================================================================

describe('ColumnarWriter - Encoding Selection', () => {
  it('should select dictionary encoding for low-cardinality strings', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'category', dataType: 'string', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write(
      Array.from({ length: 1000 }, (_, i) => ({ category: `cat_${i % 5}` }))
    );
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('category')!.encoding).toBe('dict');
  });

  it('should select raw encoding for high-cardinality strings', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'string', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write(
      Array.from({ length: 100 }, (_, i) => ({ id: `unique_${i}` }))
    );
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('id')!.encoding).toBe('raw');
  });

  it('should select delta encoding for sorted integers', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'seq', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write(
      Array.from({ length: 1000 }, (_, i) => ({ seq: i }))
    );
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('seq')!.encoding).toBe('delta');
  });

  it('should respect forceEncoding config', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'category', dataType: 'string', nullable: false }],
    };

    const forceEncoding = new Map<string, Encoding>([['category', 'raw']]);

    const writer = new ColumnarWriter(schema, { forceEncoding });
    await writer.write(
      Array.from({ length: 1000 }, (_, i) => ({ category: `cat_${i % 5}` }))
    );
    const rowGroup = await writer.finalize();

    // Should use raw even though dictionary would be better
    expect(rowGroup!.columns.get('category')!.encoding).toBe('raw');
  });

  it('should respect disableAutoEncoding config', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'seq', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema, { disableAutoEncoding: true });
    await writer.write(Array.from({ length: 100 }, (_, i) => ({ seq: i })));
    const rowGroup = await writer.finalize();

    // Should use raw encoding even for sorted data
    expect(rowGroup!.columns.get('seq')!.encoding).toBe('raw');
  });

  it('should respect preferredEncoding in column definition', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [
        { name: 'category', dataType: 'string', nullable: false, preferredEncoding: 'raw' },
      ],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write(
      Array.from({ length: 1000 }, (_, i) => ({ category: `cat_${i % 5}` }))
    );
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('category')!.encoding).toBe('raw');
  });
});

// ============================================================================
// NULL HANDLING TESTS
// ============================================================================

describe('ColumnarWriter - Null Handling', () => {
  it('should throw error for null in non-nullable column', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);

    await expect(writer.write([{ id: null }]))
      .rejects.toThrow(/does not allow null/);
  });

  it('should throw error for undefined in non-nullable column', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);

    await expect(writer.write([{}]))
      .rejects.toThrow(/does not allow null/);
  });

  it('should accept null in nullable column', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'value', dataType: 'int32', nullable: true }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([{ value: 1 }, { value: null }, { value: 3 }]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('value')!.stats.nullCount).toBe(1);
  });

  it('should track null count correctly', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'value', dataType: 'int32', nullable: true }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([
      { value: null },
      { value: 1 },
      { value: null },
      { value: null },
      { value: 5 },
    ]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('value')!.stats.nullCount).toBe(3);
  });

  it('should handle column with all null values', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [
        { name: 'id', dataType: 'int32', nullable: false },
        { name: 'optional', dataType: 'string', nullable: true },
      ],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([
      { id: 1, optional: null },
      { id: 2, optional: null },
      { id: 3, optional: null },
    ]);
    const rowGroup = await writer.finalize();

    const optionalCol = rowGroup!.columns.get('optional')!;
    expect(optionalCol.stats.nullCount).toBe(3);
    expect(optionalCol.stats.min).toBe(null);
    expect(optionalCol.stats.max).toBe(null);
  });
});

// ============================================================================
// DATA TYPE TESTS
// ============================================================================

describe('ColumnarWriter - Data Types', () => {
  it('should handle all integer types', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [
        { name: 'i8', dataType: 'int8', nullable: false },
        { name: 'i16', dataType: 'int16', nullable: false },
        { name: 'i32', dataType: 'int32', nullable: false },
        { name: 'i64', dataType: 'int64', nullable: false },
        { name: 'u8', dataType: 'uint8', nullable: false },
        { name: 'u16', dataType: 'uint16', nullable: false },
        { name: 'u32', dataType: 'uint32', nullable: false },
        { name: 'u64', dataType: 'uint64', nullable: false },
      ],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([{
      i8: -128,
      i16: -32768,
      i32: -2147483648,
      i64: BigInt('-9223372036854775808'),
      u8: 255,
      u16: 65535,
      u32: 4294967295,
      u64: BigInt('18446744073709551615'),
    }]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('i8')!.dataType).toBe('int8');
    expect(rowGroup!.columns.get('i64')!.dataType).toBe('int64');
    expect(rowGroup!.columns.get('u64')!.dataType).toBe('uint64');
  });

  it('should handle float types', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [
        { name: 'f32', dataType: 'float32', nullable: false },
        { name: 'f64', dataType: 'float64', nullable: false },
      ],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([
      { f32: 3.14, f64: 3.141592653589793 },
      { f32: -1e10, f64: 1e308 },
    ]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('f32')!.dataType).toBe('float32');
    expect(rowGroup!.columns.get('f64')!.dataType).toBe('float64');
  });

  it('should handle boolean type', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'flag', dataType: 'boolean', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([{ flag: true }, { flag: false }, { flag: true }]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('flag')!.dataType).toBe('boolean');
    expect(rowGroup!.columns.get('flag')!.encoding).toBe('raw');
  });

  it('should handle timestamp type', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'ts', dataType: 'timestamp', nullable: false }],
    };

    const now = Date.now();
    const writer = new ColumnarWriter(schema);
    await writer.write([
      { ts: BigInt(now) },
      { ts: BigInt(now + 1000) },
    ]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('ts')!.dataType).toBe('timestamp');
  });

  it('should handle bytes type', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'data', dataType: 'bytes', nullable: true }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([
      { data: new Uint8Array([1, 2, 3]) },
      { data: null },
      { data: new Uint8Array([]) },
      { data: new Uint8Array([255, 0, 128]) },
    ]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('data')!.dataType).toBe('bytes');
    expect(rowGroup!.columns.get('data')!.encoding).toBe('raw');
  });
});

// ============================================================================
// STATISTICS TESTS
// ============================================================================

describe('ColumnarWriter - Statistics', () => {
  it('should calculate correct min/max for integers', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'value', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([{ value: 50 }, { value: 10 }, { value: 100 }, { value: 30 }]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('value')!.stats.min).toBe(10);
    expect(rowGroup!.columns.get('value')!.stats.max).toBe(100);
  });

  it('should calculate correct min/max for strings', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'name', dataType: 'string', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([
      { name: 'banana' },
      { name: 'apple' },
      { name: 'cherry' },
    ]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('name')!.stats.min).toBe('apple');
    expect(rowGroup!.columns.get('name')!.stats.max).toBe('cherry');
  });

  it('should calculate sum for numeric columns', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'value', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([{ value: 10 }, { value: 20 }, { value: 30 }]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('value')!.stats.sum).toBe(60);
  });

  it('should calculate distinct count', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'category', dataType: 'string', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([
      { category: 'A' },
      { category: 'B' },
      { category: 'A' },
      { category: 'C' },
      { category: 'B' },
    ]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('category')!.stats.distinctCount).toBe(3);
  });
});

// ============================================================================
// CONVENIENCE FUNCTIONS TESTS
// ============================================================================

describe('writeColumnar', () => {
  it('should write rows and return row groups', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const { rowGroups, serialized } = await writeColumnar(schema, [
      { id: 1 },
      { id: 2 },
      { id: 3 },
    ]);

    expect(rowGroups).toHaveLength(1);
    expect(rowGroups[0].rowCount).toBe(3);
    expect(serialized).toHaveLength(1);
  });

  it('should work with FSX', async () => {
    const fsx = createInMemoryFSX();
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    await writeColumnar(schema, [{ id: 1 }], fsx);

    const keys = await fsx.list('test/rowgroups/');
    expect(keys).toHaveLength(1);
  });
});

describe('inferSchema', () => {
  it('should infer integer type', () => {
    const rows = [{ value: 42 }, { value: 100 }];
    const schema = inferSchema('test', rows);

    expect(schema.columns[0].dataType).toBe('int32');
  });

  it('should infer float type', () => {
    const rows = [{ value: 3.14 }, { value: 2.71 }];
    const schema = inferSchema('test', rows);

    expect(schema.columns[0].dataType).toBe('float64');
  });

  it('should infer boolean type', () => {
    const rows = [{ flag: true }, { flag: false }];
    const schema = inferSchema('test', rows);

    expect(schema.columns[0].dataType).toBe('boolean');
  });

  it('should infer string type', () => {
    const rows = [{ name: 'alice' }, { name: 'bob' }];
    const schema = inferSchema('test', rows);

    expect(schema.columns[0].dataType).toBe('string');
  });

  it('should infer bigint as int64', () => {
    const rows = [{ value: BigInt(100) }];
    const schema = inferSchema('test', rows);

    expect(schema.columns[0].dataType).toBe('int64');
  });

  it('should infer bytes type', () => {
    const rows = [{ data: new Uint8Array([1, 2, 3]) }];
    const schema = inferSchema('test', rows);

    expect(schema.columns[0].dataType).toBe('bytes');
  });

  it('should detect nullable columns', () => {
    const rows = [
      { value: 1, optional: 'a' },
      { value: 2, optional: null },
    ];
    const schema = inferSchema('test', rows);

    expect(schema.columns.find((c) => c.name === 'value')!.nullable).toBe(false);
    expect(schema.columns.find((c) => c.name === 'optional')!.nullable).toBe(true);
  });

  it('should throw for empty rows', () => {
    expect(() => inferSchema('test', [])).toThrow(/Cannot infer schema from empty data/);
  });

  it('should infer multiple columns', () => {
    const rows = [
      { id: 1, name: 'alice', score: 85.5, active: true },
    ];
    const schema = inferSchema('test', rows);

    expect(schema.columns).toHaveLength(4);
    expect(schema.columns.find((c) => c.name === 'id')!.dataType).toBe('int32');
    expect(schema.columns.find((c) => c.name === 'name')!.dataType).toBe('string');
    expect(schema.columns.find((c) => c.name === 'score')!.dataType).toBe('float64');
    expect(schema.columns.find((c) => c.name === 'active')!.dataType).toBe('boolean');
  });
});

// ============================================================================
// EDGE CASES
// ============================================================================

describe('ColumnarWriter - Edge Cases', () => {
  it('should handle single row', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([{ id: 42 }]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.rowCount).toBe(1);
  });

  it('should handle very long strings', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'text', dataType: 'string', nullable: false }],
    };

    const longText = 'x'.repeat(100000);
    const writer = new ColumnarWriter(schema);
    await writer.write([{ text: longText }]);
    const rowGroup = await writer.finalize();

    const serialized = serializeRowGroup(rowGroup!);
    const deserialized = deserializeRowGroup(serialized);

    expect(deserialized.rowCount).toBe(1);
  });

  it('should handle empty strings', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'text', dataType: 'string', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([{ text: '' }, { text: '' }, { text: 'non-empty' }]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('text')!.stats.min).toBe('');
  });

  it('should handle many columns', async () => {
    const columns = Array.from({ length: 50 }, (_, i) => ({
      name: `col_${i}`,
      dataType: 'int32' as const,
      nullable: false,
    }));

    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns,
    };

    const row: Record<string, number> = {};
    for (let i = 0; i < 50; i++) {
      row[`col_${i}`] = i;
    }

    const writer = new ColumnarWriter(schema);
    await writer.write([row]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.size).toBe(50);
  });

  it('should handle mixed null patterns', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [
        { name: 'a', dataType: 'int32', nullable: true },
        { name: 'b', dataType: 'string', nullable: true },
        { name: 'c', dataType: 'float64', nullable: true },
      ],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([
      { a: 1, b: null, c: 1.0 },
      { a: null, b: 'x', c: null },
      { a: 3, b: 'y', c: 3.0 },
      { a: null, b: null, c: null },
    ]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.columns.get('a')!.stats.nullCount).toBe(2);
    expect(rowGroup!.columns.get('b')!.stats.nullCount).toBe(2);
    expect(rowGroup!.columns.get('c')!.stats.nullCount).toBe(2);
  });

  it('should handle empty bytes array', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'data', dataType: 'bytes', nullable: false }],
    };

    const writer = new ColumnarWriter(schema);
    await writer.write([
      { data: new Uint8Array([]) },
      { data: new Uint8Array([1, 2]) },
      { data: new Uint8Array([]) },
    ]);
    const rowGroup = await writer.finalize();

    expect(rowGroup!.rowCount).toBe(3);
  });

  it('should calculate row range correctly', async () => {
    const schema: ColumnarTableSchema = {
      tableName: 'test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema, { targetRowsPerGroup: 10 });
    await writer.write(Array.from({ length: 25 }, (_, i) => ({ id: i })));
    await writer.finalize();

    const groups = writer.getFlushedRowGroups();
    expect(groups).toHaveLength(3);

    expect(groups[0].rowRange).toEqual({ start: 0, end: 9 });
    expect(groups[1].rowRange).toEqual({ start: 10, end: 19 });
    expect(groups[2].rowRange).toEqual({ start: 20, end: 24 });
  });
});
