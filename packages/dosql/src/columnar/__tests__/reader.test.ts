/**
 * Columnar Reader Tests
 *
 * Comprehensive tests for columnar read operations including:
 * - Projection pushdown
 * - Predicate pushdown
 * - Large column handling
 * - Edge cases with nulls and empty data
 *
 * Issue: sql-4ku7 - Columnar Storage Tests
 */

import { describe, it, expect, beforeEach } from 'vitest';

import { ColumnarReader, readColumnarChunk, getColumnStats, mightMatchPredicates } from '../reader.js';
import { ColumnarWriter } from '../writer.js';
import { serializeRowGroup, deserializeRowGroup, canSkipChunk, canSkipRowGroup } from '../chunk.js';
import type { ColumnarTableSchema, FSXInterface, Predicate, RowGroupMetadata, ColumnStats } from '../types.js';

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

// ============================================================================
// READER PROJECTION TESTS
// ============================================================================

describe('ColumnarReader - Projection Pushdown', () => {
  let fsx: ReturnType<typeof createInMemoryFSX>;

  beforeEach(async () => {
    fsx = createInMemoryFSX();

    const schema: ColumnarTableSchema = {
      tableName: 'projection_test',
      columns: [
        { name: 'id', dataType: 'int32', nullable: false },
        { name: 'name', dataType: 'string', nullable: true },
        { name: 'score', dataType: 'float64', nullable: true },
        { name: 'active', dataType: 'boolean', nullable: false },
        { name: 'count', dataType: 'int64', nullable: false },
      ],
    };

    const writer = new ColumnarWriter(schema, {}, fsx);
    await writer.write([
      { id: 1, name: 'alice', score: 85.5, active: true, count: BigInt(100) },
      { id: 2, name: 'bob', score: 92.0, active: false, count: BigInt(200) },
      { id: 3, name: null, score: null, active: true, count: BigInt(150) },
    ]);
    await writer.finalize();
  });

  it('should read all columns when no projection specified', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.read({ table: 'projection_test' });

    expect(result.columns.size).toBe(5);
    expect(result.columns.has('id')).toBe(true);
    expect(result.columns.has('name')).toBe(true);
    expect(result.columns.has('score')).toBe(true);
    expect(result.columns.has('active')).toBe(true);
    expect(result.columns.has('count')).toBe(true);
  });

  it('should read only specified columns', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.read({
      table: 'projection_test',
      projection: { columns: ['id', 'name'] },
    });

    expect(result.columns.size).toBe(2);
    expect(result.columns.has('id')).toBe(true);
    expect(result.columns.has('name')).toBe(true);
    expect(result.columns.has('score')).toBe(false);
    expect(result.columns.has('active')).toBe(false);
  });

  it('should handle single column projection', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.read({
      table: 'projection_test',
      projection: { columns: ['score'] },
    });

    expect(result.columns.size).toBe(1);
    expect(result.columns.has('score')).toBe(true);
    expect(result.columns.get('score')).toEqual([85.5, 92.0, null]);
  });

  it('should handle empty projection (all columns)', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.read({
      table: 'projection_test',
      projection: { columns: [] },
    });

    // Empty columns array should return all columns
    expect(result.columns.size).toBe(5);
  });

  it('should ignore non-existent columns in projection', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.read({
      table: 'projection_test',
      projection: { columns: ['id', 'nonexistent', 'name'] },
    });

    // Should only include columns that exist
    expect(result.columns.has('id')).toBe(true);
    expect(result.columns.has('name')).toBe(true);
    expect(result.columns.has('nonexistent')).toBe(false);
  });
});

// ============================================================================
// READER PREDICATE PUSHDOWN TESTS
// ============================================================================

describe('ColumnarReader - Predicate Pushdown', () => {
  let fsx: ReturnType<typeof createInMemoryFSX>;

  beforeEach(async () => {
    fsx = createInMemoryFSX();

    const schema: ColumnarTableSchema = {
      tableName: 'predicate_test',
      columns: [
        { name: 'id', dataType: 'int32', nullable: false },
        { name: 'value', dataType: 'int32', nullable: true },
        { name: 'category', dataType: 'string', nullable: true },
      ],
    };

    // Create multiple row groups for zone map testing
    const writer = new ColumnarWriter(schema, { targetRowsPerGroup: 5 }, fsx);

    // First group: ids 1-5, values 10-50
    await writer.write([
      { id: 1, value: 10, category: 'A' },
      { id: 2, value: 20, category: 'B' },
      { id: 3, value: 30, category: 'A' },
      { id: 4, value: 40, category: 'C' },
      { id: 5, value: 50, category: 'B' },
    ]);

    // Second group: ids 6-10, values 60-100
    await writer.write([
      { id: 6, value: 60, category: 'A' },
      { id: 7, value: 70, category: 'B' },
      { id: 8, value: 80, category: 'C' },
      { id: 9, value: 90, category: 'A' },
      { id: 10, value: 100, category: 'B' },
    ]);

    // Third group: ids 11-15, values 110-150
    await writer.write([
      { id: 11, value: 110, category: 'C' },
      { id: 12, value: 120, category: 'A' },
      { id: 13, value: 130, category: 'B' },
      { id: 14, value: 140, category: 'C' },
      { id: 15, value: 150, category: 'A' },
    ]);

    await writer.finalize();
  });

  it('should skip row groups using zone maps for gt predicate', async () => {
    const reader = new ColumnarReader(fsx);

    // value > 100 should skip first two groups (values 10-50 and 60-100)
    const result = await reader.scan({
      table: 'predicate_test',
      predicates: [{ column: 'value', op: 'gt', value: 100 }],
    });

    expect(result.stats.rowGroupsSkipped).toBe(2);
    expect(result.stats.rowGroupsScanned).toBe(1);
    expect(result.rowCount).toBe(5);
  });

  it('should skip row groups using zone maps for lt predicate', async () => {
    const reader = new ColumnarReader(fsx);

    // value < 60 should skip last two groups
    const result = await reader.scan({
      table: 'predicate_test',
      predicates: [{ column: 'value', op: 'lt', value: 60 }],
    });

    expect(result.stats.rowGroupsSkipped).toBe(2);
    expect(result.rowCount).toBe(5);
  });

  it('should filter rows within scanned groups', async () => {
    const reader = new ColumnarReader(fsx);

    const result = await reader.scan({
      table: 'predicate_test',
      predicates: [{ column: 'value', op: 'eq', value: 70 }],
    });

    expect(result.rowCount).toBe(1);
    expect(result.rows[0].id).toBe(7);
  });

  it('should handle multiple predicates (AND logic)', async () => {
    const reader = new ColumnarReader(fsx);

    const result = await reader.scan({
      table: 'predicate_test',
      predicates: [
        { column: 'value', op: 'ge', value: 50 },
        { column: 'value', op: 'le', value: 100 },
      ],
    });

    // Values 50-100: 50, 60, 70, 80, 90, 100
    expect(result.rowCount).toBe(6);
  });

  it('should handle between predicate', async () => {
    const reader = new ColumnarReader(fsx);

    const result = await reader.scan({
      table: 'predicate_test',
      predicates: [{ column: 'value', op: 'between', value: 40, value2: 80 }],
    });

    // Values 40-80: 40, 50, 60, 70, 80
    expect(result.rowCount).toBe(5);
  });

  it('should handle in predicate', async () => {
    const reader = new ColumnarReader(fsx);

    const result = await reader.scan({
      table: 'predicate_test',
      predicates: [{ column: 'id', op: 'in', value: [3, 7, 12] }],
    });

    expect(result.rowCount).toBe(3);
    expect(result.rows.map((r) => r.id)).toEqual(expect.arrayContaining([3, 7, 12]));
  });

  it('should handle ne predicate', async () => {
    const reader = new ColumnarReader(fsx);

    const result = await reader.scan({
      table: 'predicate_test',
      predicates: [{ column: 'category', op: 'ne', value: 'A' }],
    });

    // All non-A categories
    expect(result.rows.every((r) => r.category !== 'A')).toBe(true);
  });

  it('should respect enablePredicatePushdown config', async () => {
    const reader = new ColumnarReader(fsx, { enablePredicatePushdown: false });

    // Even with predicate that could skip groups, all groups should be scanned
    const result = await reader.scan({
      table: 'predicate_test',
      predicates: [{ column: 'value', op: 'gt', value: 200 }],
    });

    // No rows match but all groups should be scanned
    expect(result.stats.rowGroupsSkipped).toBe(0);
    expect(result.stats.rowGroupsScanned).toBe(3);
    expect(result.rowCount).toBe(0);
  });
});

// ============================================================================
// READER LIMIT AND OFFSET TESTS
// ============================================================================

describe('ColumnarReader - Limit and Offset', () => {
  let fsx: ReturnType<typeof createInMemoryFSX>;

  beforeEach(async () => {
    fsx = createInMemoryFSX();

    const schema: ColumnarTableSchema = {
      tableName: 'pagination_test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema, { targetRowsPerGroup: 10 }, fsx);

    // Create 50 rows in 5 row groups
    for (let i = 0; i < 50; i++) {
      await writer.write([{ id: i + 1 }]);
    }
    await writer.finalize();
  });

  it('should apply limit correctly', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'pagination_test',
      limit: 15,
    });

    expect(result.rowCount).toBe(15);
    expect(result.rows[0].id).toBe(1);
    expect(result.rows[14].id).toBe(15);
  });

  it('should apply offset correctly', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'pagination_test',
      offset: 20,
    });

    expect(result.rowCount).toBe(30);
    expect(result.rows[0].id).toBe(21);
  });

  it('should apply offset and limit together', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'pagination_test',
      offset: 10,
      limit: 5,
    });

    expect(result.rowCount).toBe(5);
    expect(result.rows[0].id).toBe(11);
    expect(result.rows[4].id).toBe(15);
  });

  it('should handle offset spanning multiple row groups', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'pagination_test',
      offset: 25,
      limit: 10,
    });

    expect(result.rowCount).toBe(10);
    expect(result.rows[0].id).toBe(26);
  });

  it('should handle offset larger than data', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'pagination_test',
      offset: 100,
    });

    expect(result.rowCount).toBe(0);
  });

  it('should handle limit larger than remaining data', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'pagination_test',
      offset: 45,
      limit: 100,
    });

    expect(result.rowCount).toBe(5);
  });
});

// ============================================================================
// READER NULL HANDLING TESTS
// ============================================================================

describe('ColumnarReader - Null Handling', () => {
  let fsx: ReturnType<typeof createInMemoryFSX>;

  beforeEach(async () => {
    fsx = createInMemoryFSX();

    const schema: ColumnarTableSchema = {
      tableName: 'null_test',
      columns: [
        { name: 'id', dataType: 'int32', nullable: false },
        { name: 'value', dataType: 'int32', nullable: true },
        { name: 'name', dataType: 'string', nullable: true },
      ],
    };

    const writer = new ColumnarWriter(schema, {}, fsx);
    await writer.write([
      { id: 1, value: 10, name: 'a' },
      { id: 2, value: null, name: 'b' },
      { id: 3, value: 30, name: null },
      { id: 4, value: null, name: null },
      { id: 5, value: 50, name: 'e' },
    ]);
    await writer.finalize();
  });

  it('should read null values correctly', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({ table: 'null_test' });

    expect(result.rows[1].value).toBe(null);
    expect(result.rows[2].name).toBe(null);
    expect(result.rows[3].value).toBe(null);
    expect(result.rows[3].name).toBe(null);
  });

  it('should filter for null equality', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'null_test',
      predicates: [{ column: 'value', op: 'eq', value: null }],
    });

    expect(result.rowCount).toBe(2);
    expect(result.rows.map((r) => r.id)).toEqual(expect.arrayContaining([2, 4]));
  });

  it('should filter for non-null values', async () => {
    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'null_test',
      predicates: [{ column: 'name', op: 'ne', value: null }],
    });

    expect(result.rowCount).toBe(3);
    expect(result.rows.every((r) => r.name !== null)).toBe(true);
  });
});

// ============================================================================
// READER LARGE COLUMN TESTS
// ============================================================================

describe('ColumnarReader - Large Columns', () => {
  it('should handle large string columns', async () => {
    const fsx = createInMemoryFSX();

    const schema: ColumnarTableSchema = {
      tableName: 'large_string',
      columns: [
        { name: 'id', dataType: 'int32', nullable: false },
        { name: 'text', dataType: 'string', nullable: false },
      ],
    };

    const longText = 'x'.repeat(50000);
    const writer = new ColumnarWriter(schema, {}, fsx);
    await writer.write([
      { id: 1, text: longText },
      { id: 2, text: 'short' },
      { id: 3, text: longText },
    ]);
    await writer.finalize();

    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({ table: 'large_string' });

    expect(result.rowCount).toBe(3);
    expect(result.rows[0].text).toBe(longText);
    expect(result.rows[1].text).toBe('short');
    expect(result.rows[2].text).toBe(longText);
  });

  it('should handle many columns', async () => {
    const fsx = createInMemoryFSX();

    // Create schema with 100 columns
    const columns = Array.from({ length: 100 }, (_, i) => ({
      name: `col_${i}`,
      dataType: 'int32' as const,
      nullable: false,
    }));

    const schema: ColumnarTableSchema = {
      tableName: 'many_columns',
      columns,
    };

    const row: Record<string, number> = {};
    for (let i = 0; i < 100; i++) {
      row[`col_${i}`] = i;
    }

    const writer = new ColumnarWriter(schema, {}, fsx);
    await writer.write([row]);
    await writer.finalize();

    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({ table: 'many_columns' });

    expect(result.rowCount).toBe(1);
    expect(result.columns.size).toBe(100);
  });

  it('should handle large bytes columns', async () => {
    const fsx = createInMemoryFSX();

    const schema: ColumnarTableSchema = {
      tableName: 'large_bytes',
      columns: [
        { name: 'id', dataType: 'int32', nullable: false },
        { name: 'data', dataType: 'bytes', nullable: true },
      ],
    };

    const largeData = new Uint8Array(100000);
    for (let i = 0; i < largeData.length; i++) {
      largeData[i] = i % 256;
    }

    const writer = new ColumnarWriter(schema, {}, fsx);
    await writer.write([
      { id: 1, data: largeData },
      { id: 2, data: null },
      { id: 3, data: new Uint8Array([1, 2, 3]) },
    ]);
    await writer.finalize();

    const reader = new ColumnarReader(fsx);
    const result = await reader.read({ table: 'large_bytes' });

    const dataColumn = result.columns.get('data')!;
    expect(dataColumn[0]).toEqual(largeData);
    expect(dataColumn[1]).toBe(null);
    expect(dataColumn[2]).toEqual(new Uint8Array([1, 2, 3]));
  });
});

// ============================================================================
// ZONE MAP / CHUNK SKIP TESTS
// ============================================================================

describe('Zone Map Filtering', () => {
  describe('canSkipChunk', () => {
    const numericStats: ColumnStats = {
      min: 10,
      max: 100,
      nullCount: 2,
    };

    const stringStats: ColumnStats = {
      min: 'apple',
      max: 'zebra',
      nullCount: 0,
    };

    it('should skip when eq value is below min', () => {
      expect(canSkipChunk(numericStats, { column: 'x', op: 'eq', value: 5 })).toBe(true);
    });

    it('should skip when eq value is above max', () => {
      expect(canSkipChunk(numericStats, { column: 'x', op: 'eq', value: 150 })).toBe(true);
    });

    it('should not skip when eq value is in range', () => {
      expect(canSkipChunk(numericStats, { column: 'x', op: 'eq', value: 50 })).toBe(false);
    });

    it('should skip when lt value <= min', () => {
      expect(canSkipChunk(numericStats, { column: 'x', op: 'lt', value: 10 })).toBe(true);
      expect(canSkipChunk(numericStats, { column: 'x', op: 'lt', value: 5 })).toBe(true);
    });

    it('should not skip when lt value > min', () => {
      expect(canSkipChunk(numericStats, { column: 'x', op: 'lt', value: 50 })).toBe(false);
    });

    it('should skip when gt value >= max', () => {
      expect(canSkipChunk(numericStats, { column: 'x', op: 'gt', value: 100 })).toBe(true);
      expect(canSkipChunk(numericStats, { column: 'x', op: 'gt', value: 150 })).toBe(true);
    });

    it('should not skip when gt value < max', () => {
      expect(canSkipChunk(numericStats, { column: 'x', op: 'gt', value: 50 })).toBe(false);
    });

    it('should handle string comparison', () => {
      expect(canSkipChunk(stringStats, { column: 'x', op: 'eq', value: 'aaa' })).toBe(true);
      expect(canSkipChunk(stringStats, { column: 'x', op: 'eq', value: 'zzz' })).toBe(true);
      expect(canSkipChunk(stringStats, { column: 'x', op: 'eq', value: 'banana' })).toBe(false);
    });

    it('should handle between with non-overlapping range', () => {
      expect(canSkipChunk(numericStats, { column: 'x', op: 'between', value: 0, value2: 5 })).toBe(true);
      expect(canSkipChunk(numericStats, { column: 'x', op: 'between', value: 110, value2: 200 })).toBe(true);
    });

    it('should not skip between with overlapping range', () => {
      expect(canSkipChunk(numericStats, { column: 'x', op: 'between', value: 50, value2: 150 })).toBe(false);
      expect(canSkipChunk(numericStats, { column: 'x', op: 'between', value: 5, value2: 50 })).toBe(false);
    });

    it('should handle in predicate', () => {
      expect(canSkipChunk(numericStats, { column: 'x', op: 'in', value: [1, 2, 3] })).toBe(true);
      expect(canSkipChunk(numericStats, { column: 'x', op: 'in', value: [200, 300] })).toBe(true);
      expect(canSkipChunk(numericStats, { column: 'x', op: 'in', value: [1, 50, 200] })).toBe(false);
    });

    it('should not skip when min/max are null', () => {
      const nullStats: ColumnStats = { min: null, max: null, nullCount: 10 };
      expect(canSkipChunk(nullStats, { column: 'x', op: 'eq', value: 50 })).toBe(false);
    });

    it('should skip ne when min equals max equals value', () => {
      const singleValueStats: ColumnStats = { min: 42, max: 42, nullCount: 0 };
      expect(canSkipChunk(singleValueStats, { column: 'x', op: 'ne', value: 42 })).toBe(true);
      expect(canSkipChunk(singleValueStats, { column: 'x', op: 'ne', value: 100 })).toBe(false);
    });
  });

  describe('canSkipRowGroup', () => {
    it('should skip when any predicate allows skipping', () => {
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

      // Can skip because age > 50 is impossible
      expect(canSkipRowGroup(metadata, [
        { column: 'age', op: 'gt', value: 50 },
      ])).toBe(true);

      // Cannot skip because salary could be 75000
      expect(canSkipRowGroup(metadata, [
        { column: 'salary', op: 'eq', value: 75000 },
      ])).toBe(false);
    });

    it('should not skip when column is not in stats', () => {
      const metadata: RowGroupMetadata = {
        id: 'rg1',
        rowCount: 100,
        rowRange: { start: 0, end: 99 },
        columnStats: new Map([
          ['age', { min: 20, max: 40, nullCount: 0 }],
        ]),
        byteSize: 1000,
        createdAt: Date.now(),
      };

      // Cannot skip because 'name' column has no stats
      expect(canSkipRowGroup(metadata, [
        { column: 'name', op: 'eq', value: 'test' },
      ])).toBe(false);
    });
  });
});

// ============================================================================
// CONVENIENCE FUNCTIONS TESTS
// ============================================================================

describe('Convenience Functions', () => {
  describe('readColumnarChunk', () => {
    it('should read all columns from serialized data', async () => {
      const schema: ColumnarTableSchema = {
        tableName: 'test',
        columns: [
          { name: 'a', dataType: 'int32', nullable: false },
          { name: 'b', dataType: 'string', nullable: false },
        ],
      };

      const fsx = createInMemoryFSX();
      const writer = new ColumnarWriter(schema, {}, fsx);
      await writer.write([
        { a: 1, b: 'one' },
        { a: 2, b: 'two' },
      ]);
      const rowGroup = await writer.finalize();
      const serialized = serializeRowGroup(rowGroup!);

      const columns = readColumnarChunk(serialized);
      expect(columns.get('a')).toEqual([1, 2]);
      expect(columns.get('b')).toEqual(['one', 'two']);
    });

    it('should support projection in readColumnarChunk', async () => {
      const schema: ColumnarTableSchema = {
        tableName: 'test',
        columns: [
          { name: 'a', dataType: 'int32', nullable: false },
          { name: 'b', dataType: 'int32', nullable: false },
          { name: 'c', dataType: 'int32', nullable: false },
        ],
      };

      const fsx = createInMemoryFSX();
      const writer = new ColumnarWriter(schema, {}, fsx);
      await writer.write([{ a: 1, b: 2, c: 3 }]);
      const rowGroup = await writer.finalize();
      const serialized = serializeRowGroup(rowGroup!);

      const columns = readColumnarChunk(serialized, ['b']);
      expect(columns.has('a')).toBe(false);
      expect(columns.has('b')).toBe(true);
      expect(columns.has('c')).toBe(false);
    });
  });

  describe('getColumnStats', () => {
    it('should extract stats from serialized data', async () => {
      const schema: ColumnarTableSchema = {
        tableName: 'test',
        columns: [{ name: 'value', dataType: 'int32', nullable: false }],
      };

      const fsx = createInMemoryFSX();
      const writer = new ColumnarWriter(schema, {}, fsx);
      await writer.write([{ value: 5 }, { value: 15 }, { value: 10 }]);
      const rowGroup = await writer.finalize();
      const serialized = serializeRowGroup(rowGroup!);

      const stats = getColumnStats(serialized, 'value');
      expect(stats).toBeDefined();
      expect(stats!.min).toBe(5);
      expect(stats!.max).toBe(15);
    });

    it('should return null for non-existent column', async () => {
      const schema: ColumnarTableSchema = {
        tableName: 'test',
        columns: [{ name: 'value', dataType: 'int32', nullable: false }],
      };

      const fsx = createInMemoryFSX();
      const writer = new ColumnarWriter(schema, {}, fsx);
      await writer.write([{ value: 5 }]);
      const rowGroup = await writer.finalize();
      const serialized = serializeRowGroup(rowGroup!);

      const stats = getColumnStats(serialized, 'nonexistent');
      expect(stats).toBe(null);
    });
  });

  describe('mightMatchPredicates', () => {
    it('should return true when predicates might match', async () => {
      const schema: ColumnarTableSchema = {
        tableName: 'test',
        columns: [{ name: 'x', dataType: 'int32', nullable: false }],
      };

      const fsx = createInMemoryFSX();
      const writer = new ColumnarWriter(schema, {}, fsx);
      await writer.write(Array.from({ length: 10 }, (_, i) => ({ x: i * 10 })));
      const rowGroup = await writer.finalize();
      const serialized = serializeRowGroup(rowGroup!);

      expect(mightMatchPredicates(serialized, [
        { column: 'x', op: 'eq', value: 50 },
      ])).toBe(true);
    });

    it('should return false when predicates cannot match', async () => {
      const schema: ColumnarTableSchema = {
        tableName: 'test',
        columns: [{ name: 'x', dataType: 'int32', nullable: false }],
      };

      const fsx = createInMemoryFSX();
      const writer = new ColumnarWriter(schema, {}, fsx);
      await writer.write(Array.from({ length: 10 }, (_, i) => ({ x: i * 10 })));
      const rowGroup = await writer.finalize();
      const serialized = serializeRowGroup(rowGroup!);

      expect(mightMatchPredicates(serialized, [
        { column: 'x', op: 'gt', value: 100 },
      ])).toBe(false);
    });
  });
});

// ============================================================================
// READER CACHE TESTS
// ============================================================================

describe('ColumnarReader - Metadata Cache', () => {
  it('should use cached metadata on subsequent reads', async () => {
    const fsx = createInMemoryFSX();

    const schema: ColumnarTableSchema = {
      tableName: 'cache_test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema, {}, fsx);
    await writer.write([{ id: 1 }, { id: 2 }]);
    await writer.finalize();

    const reader = new ColumnarReader(fsx);

    // First read
    await reader.read({ table: 'cache_test' });

    // Second read should use cache (we can't directly test this, but can verify behavior)
    const result = await reader.read({ table: 'cache_test' });
    expect(result.rowCount).toBe(2);
  });

  it('should clear cache when clearCache is called', async () => {
    const fsx = createInMemoryFSX();

    const schema: ColumnarTableSchema = {
      tableName: 'cache_test',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema, {}, fsx);
    await writer.write([{ id: 1 }]);
    await writer.finalize();

    const reader = new ColumnarReader(fsx);

    // Read to populate cache
    await reader.read({ table: 'cache_test' });

    // Clear cache
    reader.clearCache();

    // Read again - should work without errors
    const result = await reader.read({ table: 'cache_test' });
    expect(result.rowCount).toBe(1);
  });
});

// ============================================================================
// EDGE CASES
// ============================================================================

describe('ColumnarReader - Edge Cases', () => {
  it('should handle empty table', async () => {
    const fsx = createInMemoryFSX();

    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({ table: 'nonexistent' });

    expect(result.rowCount).toBe(0);
    expect(result.rows).toEqual([]);
  });

  it('should handle table with single row', async () => {
    const fsx = createInMemoryFSX();

    const schema: ColumnarTableSchema = {
      tableName: 'single_row',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema, {}, fsx);
    await writer.write([{ id: 42 }]);
    await writer.finalize();

    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({ table: 'single_row' });

    expect(result.rowCount).toBe(1);
    expect(result.rows[0].id).toBe(42);
  });

  it('should handle predicate on empty result', async () => {
    const fsx = createInMemoryFSX();

    const schema: ColumnarTableSchema = {
      tableName: 'no_match',
      columns: [{ name: 'id', dataType: 'int32', nullable: false }],
    };

    const writer = new ColumnarWriter(schema, {}, fsx);
    await writer.write([{ id: 1 }, { id: 2 }, { id: 3 }]);
    await writer.finalize();

    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({
      table: 'no_match',
      predicates: [{ column: 'id', op: 'eq', value: 999 }],
    });

    expect(result.rowCount).toBe(0);
  });

  it('should handle all types of data together', async () => {
    const fsx = createInMemoryFSX();

    const schema: ColumnarTableSchema = {
      tableName: 'all_types',
      columns: [
        { name: 'int_col', dataType: 'int32', nullable: true },
        { name: 'bigint_col', dataType: 'int64', nullable: true },
        { name: 'float_col', dataType: 'float64', nullable: true },
        { name: 'bool_col', dataType: 'boolean', nullable: true },
        { name: 'str_col', dataType: 'string', nullable: true },
        { name: 'bytes_col', dataType: 'bytes', nullable: true },
        { name: 'ts_col', dataType: 'timestamp', nullable: true },
      ],
    };

    const now = Date.now();
    const writer = new ColumnarWriter(schema, {}, fsx);
    await writer.write([
      {
        int_col: 42,
        bigint_col: BigInt('9007199254740993'),
        float_col: 3.14159,
        bool_col: true,
        str_col: 'hello',
        bytes_col: new Uint8Array([1, 2, 3]),
        ts_col: BigInt(now),
      },
      {
        int_col: null,
        bigint_col: null,
        float_col: null,
        bool_col: null,
        str_col: null,
        bytes_col: null,
        ts_col: null,
      },
    ]);
    await writer.finalize();

    const reader = new ColumnarReader(fsx);
    const result = await reader.scan({ table: 'all_types' });

    expect(result.rowCount).toBe(2);

    // First row
    expect(result.rows[0].int_col).toBe(42);
    expect(result.rows[0].bigint_col).toBe(BigInt('9007199254740993'));
    expect(result.rows[0].float_col).toBeCloseTo(3.14159);
    expect(result.rows[0].bool_col).toBe(true);
    expect(result.rows[0].str_col).toBe('hello');
    expect(result.rows[0].bytes_col).toEqual(new Uint8Array([1, 2, 3]));
    expect(result.rows[0].ts_col).toBe(BigInt(now));

    // Second row - all null
    expect(result.rows[1].int_col).toBe(null);
    expect(result.rows[1].str_col).toBe(null);
    expect(result.rows[1].bytes_col).toBe(null);
  });
});
