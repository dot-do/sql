/**
 * Vector Column Storage Unit Tests
 *
 * Comprehensive tests for VectorColumn storage including:
 * - Basic CRUD operations
 * - Search with and without HNSW index
 * - Quantization support
 * - Batch operations
 * - Serialization
 * - Hybrid search scenarios
 * - Edge cases
 *
 * @module dosql/vector/__tests__/column.test
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  VectorColumn,
  hybridSearch,
  type VectorColumnOptions,
  type HybridQueryOptions,
} from '../column.js';
import {
  VectorType,
  DistanceMetric,
  type VectorColumnDef,
  type Vector,
} from '../types.js';
import { vector_normalize, vector_distance_cos } from '../distance.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Create a Float32Array from numbers
 */
function vec(...values: number[]): Float32Array {
  return new Float32Array(values);
}

/**
 * Create a seeded random vector generator
 */
function createVectorGenerator(dims: number, seed: number = 42) {
  let state = seed;
  return (): Float32Array => {
    const v = new Float32Array(dims);
    for (let i = 0; i < dims; i++) {
      state = (state * 1664525 + 1013904223) >>> 0;
      v[i] = (state / 0xffffffff) * 2 - 1;
    }
    return v;
  };
}

/**
 * Create a normalized random vector
 */
function randomNormalizedVector(dims: number, seed: number): Float32Array {
  const gen = createVectorGenerator(dims, seed);
  return vector_normalize(gen());
}

/**
 * Standard test column definition
 */
const standardColumnDef: VectorColumnDef = {
  name: 'embedding',
  dimensions: 8,
  type: VectorType.F32,
  distanceMetric: DistanceMetric.Cosine,
};

// =============================================================================
// CONSTRUCTION TESTS
// =============================================================================

describe('VectorColumn Construction', () => {
  it('should create column with default options', () => {
    const column = new VectorColumn({ columnDef: standardColumnDef });
    expect(column.size).toBe(0);
    expect(column.dimensions).toBe(8);
  });

  it('should create column without index', () => {
    const column = new VectorColumn({
      columnDef: standardColumnDef,
      enableIndex: false,
    });
    expect(column.size).toBe(0);
  });

  it('should create column with custom HNSW parameters', () => {
    const column = new VectorColumn({
      columnDef: standardColumnDef,
      hnswM: 32,
      hnswEfConstruction: 400,
      hnswEfSearch: 200,
    });

    // Insert and search to verify it works
    column.set(1n, randomNormalizedVector(8, 1));
    column.set(2n, randomNormalizedVector(8, 2));

    const results = column.search(randomNormalizedVector(8, 99), 2);
    expect(results.length).toBe(2);
  });

  it('should create column with quantization config', () => {
    const column = new VectorColumn({
      columnDef: standardColumnDef,
      quantization: {
        targetType: VectorType.I8,
        minVal: -1,
        maxVal: 1,
      },
    });

    column.set(1n, vec(0.5, -0.5, 0.25, -0.25, 0, 0, 0, 0));
    const quantized = column.getQuantized(1n);
    expect(quantized).toBeDefined();
    expect(quantized).toBeInstanceOf(Int8Array);
  });

  it('should return column definition', () => {
    const column = new VectorColumn({ columnDef: standardColumnDef });
    const def = column.getColumnDef();
    expect(def.name).toBe('embedding');
    expect(def.dimensions).toBe(8);
    expect(def.type).toBe(VectorType.F32);
  });
});

// =============================================================================
// BASIC CRUD TESTS
// =============================================================================

describe('VectorColumn CRUD', () => {
  let column: VectorColumn;

  beforeEach(() => {
    column = new VectorColumn({ columnDef: standardColumnDef });
  });

  describe('set', () => {
    it('should insert a vector', () => {
      const v = vec(1, 2, 3, 4, 5, 6, 7, 8);
      column.set(1n, v);
      expect(column.size).toBe(1);
      expect(column.has(1n)).toBe(true);
    });

    it('should update existing vector', () => {
      column.set(1n, vec(1, 2, 3, 4, 5, 6, 7, 8));
      column.set(1n, vec(8, 7, 6, 5, 4, 3, 2, 1));

      expect(column.size).toBe(1);
      const retrieved = column.get(1n);
      expect(Array.from(retrieved!)).toEqual([8, 7, 6, 5, 4, 3, 2, 1]);
    });

    it('should accept number array', () => {
      column.set(1n, [1, 2, 3, 4, 5, 6, 7, 8]);
      expect(column.has(1n)).toBe(true);
    });

    it('should accept Float64Array', () => {
      column.set(1n, new Float64Array([1, 2, 3, 4, 5, 6, 7, 8]));
      expect(column.has(1n)).toBe(true);
    });

    it('should validate dimensions', () => {
      expect(() => column.set(1n, vec(1, 2, 3))).toThrow('dimension mismatch');
    });
  });

  describe('get', () => {
    it('should retrieve inserted vectors', () => {
      const v = vec(1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5);
      column.set(1n, v);

      const retrieved = column.get(1n);
      expect(retrieved).toBeDefined();
      expect(Array.from(retrieved!)).toEqual(Array.from(v));
    });

    it('should return undefined for non-existent IDs', () => {
      expect(column.get(999n)).toBeUndefined();
    });
  });

  describe('has', () => {
    it('should return true for existing IDs', () => {
      column.set(1n, randomNormalizedVector(8, 1));
      expect(column.has(1n)).toBe(true);
    });

    it('should return false for non-existent IDs', () => {
      expect(column.has(999n)).toBe(false);
    });
  });

  describe('delete', () => {
    it('should delete existing vectors', () => {
      column.set(1n, randomNormalizedVector(8, 1));
      column.set(2n, randomNormalizedVector(8, 2));

      expect(column.delete(1n)).toBe(true);
      expect(column.size).toBe(1);
      expect(column.has(1n)).toBe(false);
      expect(column.has(2n)).toBe(true);
    });

    it('should return false for non-existent IDs', () => {
      expect(column.delete(999n)).toBe(false);
    });

    it('should delete from index', () => {
      column.set(1n, randomNormalizedVector(8, 1));
      column.set(2n, randomNormalizedVector(8, 2));
      column.delete(1n);

      // Search should not find deleted vector
      const results = column.search(randomNormalizedVector(8, 1), 10);
      expect(results.every((r) => r.rowId !== 1n)).toBe(true);
    });
  });

  describe('clear', () => {
    it('should remove all vectors', () => {
      column.set(1n, randomNormalizedVector(8, 1));
      column.set(2n, randomNormalizedVector(8, 2));
      column.set(3n, randomNormalizedVector(8, 3));

      column.clear();

      expect(column.size).toBe(0);
      expect(column.has(1n)).toBe(false);
    });
  });
});

// =============================================================================
// SEARCH TESTS
// =============================================================================

describe('VectorColumn Search', () => {
  describe('with HNSW index', () => {
    let column: VectorColumn;

    beforeEach(() => {
      column = new VectorColumn({
        columnDef: standardColumnDef,
        hnswM: 8,
        hnswEfConstruction: 50,
        hnswEfSearch: 50,
      });

      for (let i = 0; i < 100; i++) {
        column.set(BigInt(i), randomNormalizedVector(8, i));
      }
    });

    it('should find nearest neighbors', () => {
      const query = randomNormalizedVector(8, 999);
      const results = column.search(query, 10);

      expect(results.length).toBe(10);
    });

    it('should return results sorted by distance', () => {
      const results = column.search(randomNormalizedVector(8, 999), 20);

      for (let i = 1; i < results.length; i++) {
        expect(results[i].distance).toBeGreaterThanOrEqual(results[i - 1].distance);
      }
    });

    it('should include distance and score in results', () => {
      const results = column.search(randomNormalizedVector(8, 999), 5);

      for (const result of results) {
        expect(typeof result.rowId).toBe('bigint');
        expect(typeof result.distance).toBe('number');
        expect(typeof result.score).toBe('number');
        expect(result.score).toBeGreaterThanOrEqual(0);
        expect(result.score).toBeLessThanOrEqual(1);
      }
    });

    it('should support efSearch override', () => {
      const query = randomNormalizedVector(8, 999);
      const results = column.search(query, 5, 200);
      expect(results.length).toBe(5);
    });

    it('should support filter function', () => {
      const results = column.search(
        randomNormalizedVector(8, 999),
        10,
        undefined,
        (id) => Number(id) % 2 === 0,
      );

      for (const result of results) {
        expect(Number(result.rowId) % 2).toBe(0);
      }
    });
  });

  describe('without index (brute force)', () => {
    let column: VectorColumn;

    beforeEach(() => {
      column = new VectorColumn({
        columnDef: standardColumnDef,
        enableIndex: false,
      });

      for (let i = 0; i < 50; i++) {
        column.set(BigInt(i), randomNormalizedVector(8, i));
      }
    });

    it('should find nearest neighbors', () => {
      const results = column.search(randomNormalizedVector(8, 999), 10);
      expect(results.length).toBe(10);
    });

    it('should return exact results', () => {
      // Brute force should give exact ordering
      const results = column.search(randomNormalizedVector(8, 999), 50);

      for (let i = 1; i < results.length; i++) {
        expect(results[i].distance).toBeGreaterThanOrEqual(results[i - 1].distance);
      }
    });

    it('should support filter function', () => {
      const results = column.search(
        randomNormalizedVector(8, 999),
        10,
        undefined,
        (id) => Number(id) < 25,
      );

      expect(results.length).toBeLessThanOrEqual(25);
      for (const result of results) {
        expect(Number(result.rowId)).toBeLessThan(25);
      }
    });
  });

  describe('edge cases', () => {
    it('should return empty for empty column', () => {
      const column = new VectorColumn({ columnDef: standardColumnDef });
      const results = column.search(randomNormalizedVector(8, 1), 5);
      expect(results).toEqual([]);
    });

    it('should return all if k > size', () => {
      const column = new VectorColumn({ columnDef: standardColumnDef });
      column.set(1n, randomNormalizedVector(8, 1));
      column.set(2n, randomNormalizedVector(8, 2));

      const results = column.search(randomNormalizedVector(8, 999), 10);
      expect(results.length).toBe(2);
    });

    it('should validate query dimensions', () => {
      const column = new VectorColumn({ columnDef: standardColumnDef });
      column.set(1n, randomNormalizedVector(8, 1));

      expect(() => column.search(vec(1, 2, 3), 5)).toThrow('dimension mismatch');
    });
  });
});

// =============================================================================
// DISTANCE COMPUTATION TESTS
// =============================================================================

describe('VectorColumn Distance', () => {
  let column: VectorColumn;

  beforeEach(() => {
    column = new VectorColumn({ columnDef: standardColumnDef });
  });

  it('should compute distance to stored vector', () => {
    const v = vec(1, 0, 0, 0, 0, 0, 0, 0);
    column.set(1n, v);

    const dist = column.distance(vec(1, 0, 0, 0, 0, 0, 0, 0), 1n);
    expect(dist).toBeCloseTo(0, 10);
  });

  it('should return undefined for non-existent row', () => {
    const dist = column.distance(randomNormalizedVector(8, 1), 999n);
    expect(dist).toBeUndefined();
  });

  it('should validate query dimensions', () => {
    column.set(1n, randomNormalizedVector(8, 1));
    expect(() => column.distance(vec(1, 2, 3), 1n)).toThrow('dimension mismatch');
  });
});

// =============================================================================
// QUANTIZATION TESTS
// =============================================================================

describe('VectorColumn Quantization', () => {
  let column: VectorColumn;

  beforeEach(() => {
    column = new VectorColumn({
      columnDef: standardColumnDef,
      quantization: {
        targetType: VectorType.I8,
        minVal: -1,
        maxVal: 1,
      },
    });
  });

  it('should store quantized vectors', () => {
    column.set(1n, vec(0.5, -0.5, 0.25, -0.25, 0, 0, 0, 0));
    const quantized = column.getQuantized(1n);

    expect(quantized).toBeDefined();
    expect(quantized!.length).toBe(8);
  });

  it('should preserve full-precision vectors', () => {
    const original = vec(0.5, -0.5, 0.25, -0.25, 0, 0.1, 0.2, 0.3);
    column.set(1n, original);

    const retrieved = column.get(1n);
    expect(Array.from(retrieved!)).toEqual(Array.from(original));
  });

  it('should recompute quantization', () => {
    // Insert initial vectors
    column.set(1n, vec(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1));
    column.set(2n, vec(0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2));

    // Recompute
    column.recomputeQuantization();

    // Should still have quantized versions
    expect(column.getQuantized(1n)).toBeDefined();
    expect(column.getQuantized(2n)).toBeDefined();
  });

  it('should delete quantized data on delete', () => {
    column.set(1n, randomNormalizedVector(8, 1));
    column.delete(1n);

    expect(column.getQuantized(1n)).toBeUndefined();
  });

  it('should clear quantized data on clear', () => {
    column.set(1n, randomNormalizedVector(8, 1));
    column.set(2n, randomNormalizedVector(8, 2));
    column.clear();

    expect(column.getQuantized(1n)).toBeUndefined();
    expect(column.getQuantized(2n)).toBeUndefined();
  });
});

// =============================================================================
// BATCH OPERATIONS TESTS
// =============================================================================

describe('VectorColumn Batch Operations', () => {
  it('should batch insert vectors', () => {
    const column = new VectorColumn({ columnDef: standardColumnDef });

    const entries: [bigint, Float32Array][] = [];
    for (let i = 0; i < 100; i++) {
      entries.push([BigInt(i), randomNormalizedVector(8, i)]);
    }

    column.batchSet(entries);
    expect(column.size).toBe(100);
  });

  it('should validate dimensions in batch', () => {
    const column = new VectorColumn({ columnDef: standardColumnDef });

    const entries: [bigint, Float32Array][] = [
      [1n, randomNormalizedVector(8, 1)],
      [2n, vec(1, 2, 3)], // Wrong dimensions
    ];

    expect(() => column.batchSet(entries)).toThrow('dimension mismatch');
  });

  it('should rebuild index after batch insert', () => {
    const column = new VectorColumn({
      columnDef: standardColumnDef,
      hnswM: 8,
      hnswEfConstruction: 50,
      hnswEfSearch: 50,
    });

    const entries: [bigint, Float32Array][] = [];
    for (let i = 0; i < 50; i++) {
      entries.push([BigInt(i), randomNormalizedVector(8, i)]);
    }

    column.batchSet(entries);

    // Search should work after batch insert
    const results = column.search(randomNormalizedVector(8, 999), 5);
    expect(results.length).toBe(5);
  });

  it('should recompute quantization after batch insert', () => {
    const column = new VectorColumn({
      columnDef: standardColumnDef,
      quantization: {
        targetType: VectorType.I8,
        minVal: -1,
        maxVal: 1,
      },
    });

    const entries: [bigint, Float32Array][] = [];
    for (let i = 0; i < 20; i++) {
      entries.push([BigInt(i), randomNormalizedVector(8, i)]);
    }

    column.batchSet(entries);

    // All should have quantized versions
    for (let i = 0; i < 20; i++) {
      expect(column.getQuantized(BigInt(i))).toBeDefined();
    }
  });

  it('should rebuild index explicitly', () => {
    const column = new VectorColumn({
      columnDef: standardColumnDef,
      hnswM: 8,
      hnswEfConstruction: 50,
      hnswEfSearch: 50,
    });

    for (let i = 0; i < 50; i++) {
      column.set(BigInt(i), randomNormalizedVector(8, i));
    }

    // Rebuild index
    column.rebuildIndex();

    // Should still work
    const results = column.search(randomNormalizedVector(8, 999), 5);
    expect(results.length).toBe(5);
  });
});

// =============================================================================
// SERIALIZATION TESTS
// =============================================================================

describe('VectorColumn Serialization', () => {
  it('should serialize and deserialize empty column', () => {
    const column = new VectorColumn({ columnDef: standardColumnDef });
    const binary = column.serialize();

    const restored = VectorColumn.deserialize(binary, standardColumnDef);
    expect(restored.size).toBe(0);
  });

  it('should serialize and deserialize with vectors', () => {
    const column = new VectorColumn({ columnDef: standardColumnDef });
    column.set(1n, vec(1, 2, 3, 4, 5, 6, 7, 8));
    column.set(2n, vec(8, 7, 6, 5, 4, 3, 2, 1));

    const binary = column.serialize();
    const restored = VectorColumn.deserialize(binary, standardColumnDef);

    expect(restored.size).toBe(2);
    expect(restored.has(1n)).toBe(true);
    expect(restored.has(2n)).toBe(true);
    expect(Array.from(restored.get(1n)!)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
  });

  it('should preserve index if present', () => {
    const column = new VectorColumn({
      columnDef: standardColumnDef,
      hnswM: 8,
      hnswEfConstruction: 50,
      hnswEfSearch: 50,
    });

    for (let i = 0; i < 50; i++) {
      column.set(BigInt(i), randomNormalizedVector(8, i));
    }

    const query = randomNormalizedVector(8, 999);
    const resultsBefore = column.search(query, 5);

    const binary = column.serialize();
    const restored = VectorColumn.deserialize(binary, standardColumnDef);

    // Search should return same results
    const resultsAfter = restored.search(query, 5);
    expect(resultsAfter.map((r) => r.rowId)).toEqual(resultsBefore.map((r) => r.rowId));
  });
});

// =============================================================================
// ITERATION TESTS
// =============================================================================

describe('VectorColumn Iteration', () => {
  let column: VectorColumn;

  beforeEach(() => {
    column = new VectorColumn({ columnDef: standardColumnDef });
    column.set(1n, vec(1, 2, 3, 4, 5, 6, 7, 8));
    column.set(2n, vec(8, 7, 6, 5, 4, 3, 2, 1));
    column.set(3n, vec(1, 1, 1, 1, 1, 1, 1, 1));
  });

  it('should iterate over entries', () => {
    const entries = Array.from(column.entries());
    expect(entries.length).toBe(3);

    const ids = entries.map(([id]) => id).sort();
    expect(ids).toEqual([1n, 2n, 3n]);
  });

  it('should iterate over keys', () => {
    const keys = Array.from(column.keys());
    expect(keys.sort()).toEqual([1n, 2n, 3n]);
  });

  it('should iterate over values', () => {
    const values = Array.from(column.values());
    expect(values.length).toBe(3);
    expect(values[0]).toBeInstanceOf(Float32Array);
  });

  it('should get all row IDs', () => {
    const ids = column.getRowIds();
    expect(ids.sort()).toEqual([1n, 2n, 3n]);
  });
});

// =============================================================================
// STATISTICS TESTS
// =============================================================================

describe('VectorColumn Stats', () => {
  it('should return stats for empty column', () => {
    const column = new VectorColumn({ columnDef: standardColumnDef });
    const stats = column.getStats();

    expect(stats.rowCount).toBe(0);
    expect(stats.dimensions).toBe(8);
    expect(stats.memoryBytes).toBe(0);
  });

  it('should return accurate stats', () => {
    const column = new VectorColumn({
      columnDef: standardColumnDef,
      hnswM: 8,
    });

    for (let i = 0; i < 100; i++) {
      column.set(BigInt(i), randomNormalizedVector(8, i));
    }

    const stats = column.getStats();

    expect(stats.rowCount).toBe(100);
    expect(stats.dimensions).toBe(8);
    expect(stats.memoryBytes).toBe(100 * 8 * 4); // 100 vectors * 8 dims * 4 bytes
    expect(stats.indexStats).toBeDefined();
    expect(stats.indexStats?.nodeCount).toBe(100);
  });

  it('should include quantization stats when enabled', () => {
    const column = new VectorColumn({
      columnDef: standardColumnDef,
      quantization: {
        targetType: VectorType.I8,
        minVal: -1,
        maxVal: 1,
      },
    });

    column.set(1n, vec(0.5, -0.5, 0.1, -0.1, 0, 0, 0.2, -0.2));
    column.recomputeQuantization();

    const stats = column.getStats();
    expect(stats.quantizationStats).toBeDefined();
  });
});

// =============================================================================
// HYBRID SEARCH TESTS
// =============================================================================

describe('hybridSearch', () => {
  let column: VectorColumn;

  beforeEach(() => {
    column = new VectorColumn({
      columnDef: standardColumnDef,
      hnswM: 8,
      hnswEfConstruction: 50,
      hnswEfSearch: 50,
    });

    for (let i = 0; i < 100; i++) {
      column.set(BigInt(i), randomNormalizedVector(8, i));
    }
  });

  describe('filter-then-search (small filter set)', () => {
    it('should filter before search for small filter sets', () => {
      const filterIds = new Set([0n, 1n, 2n, 3n, 4n]);

      const results = hybridSearch(column, randomNormalizedVector(8, 999), {
        filterIds,
        k: 3,
      });

      expect(results.length).toBe(3);
      for (const result of results) {
        expect(filterIds.has(result.rowId)).toBe(true);
      }
    });

    it('should return fewer than k if filter set is small', () => {
      const filterIds = new Set([0n, 1n]);

      const results = hybridSearch(column, randomNormalizedVector(8, 999), {
        filterIds,
        k: 10,
      });

      expect(results.length).toBe(2);
    });
  });

  describe('search-then-filter (large filter set)', () => {
    it('should use ANN with filter for large filter sets', () => {
      // Create a large filter set
      const filterIds = new Set<bigint>();
      for (let i = 0; i < 80; i += 2) {
        filterIds.add(BigInt(i)); // Even IDs
      }

      const results = hybridSearch(column, randomNormalizedVector(8, 999), {
        filterIds,
        k: 10,
      });

      expect(results.length).toBe(10);
      for (const result of results) {
        expect(filterIds.has(result.rowId)).toBe(true);
      }
    });
  });

  describe('post-filter', () => {
    it('should apply post-filter to results', () => {
      const results = hybridSearch(column, randomNormalizedVector(8, 999), {
        postFilter: (id) => Number(id) % 3 === 0,
        k: 5,
      });

      for (const result of results) {
        expect(Number(result.rowId) % 3).toBe(0);
      }
    });

    it('should combine pre-filter and post-filter', () => {
      // Pre-filter: IDs < 50
      const filterIds = new Set<bigint>();
      for (let i = 0; i < 50; i++) {
        filterIds.add(BigInt(i));
      }

      // Post-filter: even IDs
      const results = hybridSearch(column, randomNormalizedVector(8, 999), {
        filterIds,
        postFilter: (id) => Number(id) % 2 === 0,
        k: 5,
      });

      for (const result of results) {
        expect(Number(result.rowId)).toBeLessThan(50);
        expect(Number(result.rowId) % 2).toBe(0);
      }
    });
  });

  describe('oversample factor', () => {
    it('should use default oversample factor of 2', () => {
      // Create filter that passes ~50% of results
      const filterIds = new Set<bigint>();
      for (let i = 0; i < 100; i += 2) {
        filterIds.add(BigInt(i));
      }

      const results = hybridSearch(column, randomNormalizedVector(8, 999), {
        filterIds,
        k: 5,
      });

      expect(results.length).toBe(5);
    });

    it('should respect custom oversample factor', () => {
      const filterIds = new Set<bigint>();
      for (let i = 0; i < 100; i += 2) {
        filterIds.add(BigInt(i));
      }

      const results = hybridSearch(column, randomNormalizedVector(8, 999), {
        filterIds,
        k: 5,
        oversampleFactor: 4,
      });

      expect(results.length).toBe(5);
    });
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('VectorColumn Edge Cases', () => {
  it('should handle single-element column', () => {
    const column = new VectorColumn({ columnDef: standardColumnDef });
    column.set(1n, randomNormalizedVector(8, 1));

    const results = column.search(randomNormalizedVector(8, 999), 5);
    expect(results.length).toBe(1);
  });

  it('should handle high-dimensional vectors', () => {
    const highDimDef: VectorColumnDef = {
      name: 'embedding',
      dimensions: 1536,
      type: VectorType.F32,
      distanceMetric: DistanceMetric.Cosine,
    };

    const column = new VectorColumn({
      columnDef: highDimDef,
      hnswM: 16,
      hnswEfConstruction: 100,
    });

    for (let i = 0; i < 50; i++) {
      column.set(BigInt(i), randomNormalizedVector(1536, i));
    }

    const results = column.search(randomNormalizedVector(1536, 999), 5);
    expect(results.length).toBe(5);
  });

  it('should handle different distance metrics', () => {
    const l2Def: VectorColumnDef = {
      name: 'embedding',
      dimensions: 8,
      type: VectorType.F32,
      distanceMetric: DistanceMetric.L2,
    };

    const column = new VectorColumn({ columnDef: l2Def });

    column.set(1n, vec(0, 0, 0, 0, 0, 0, 0, 0));
    column.set(2n, vec(1, 0, 0, 0, 0, 0, 0, 0));
    column.set(3n, vec(2, 0, 0, 0, 0, 0, 0, 0));

    // Query near origin
    const results = column.search(vec(0.1, 0, 0, 0, 0, 0, 0, 0), 3);
    expect(results[0].rowId).toBe(1n);
  });

  it('should handle nullable column definition', () => {
    const nullableDef: VectorColumnDef = {
      name: 'embedding',
      dimensions: 8,
      type: VectorType.F32,
      nullable: true,
    };

    const column = new VectorColumn({ columnDef: nullableDef });
    column.set(1n, randomNormalizedVector(8, 1));
    expect(column.size).toBe(1);
  });

  it('should preserve column definition on getColumnDef', () => {
    const column = new VectorColumn({ columnDef: standardColumnDef });
    const def = column.getColumnDef();

    // Modify returned def should not affect original
    def.name = 'modified';

    const defAgain = column.getColumnDef();
    expect(defAgain.name).toBe('embedding');
  });
});
