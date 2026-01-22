/**
 * Vector Search Tests for DoSQL
 *
 * Tests for native vector search functionality:
 * - Vector creation and storage
 * - Distance calculations
 * - HNSW index build and search
 * - SQL function integration
 */

import { describe, it, expect, beforeEach } from 'vitest';

// Import types
import {
  VectorType,
  DistanceMetric,
  VectorIndexType,
  type Vector,
  type VectorColumnDef,
  type HnswConfig,
  toVector,
  validateDimensions,
  vectorByteSize,
  isVectorInput,
  DEFAULT_HNSW_CONFIG,
} from './types.js';

// Import distance functions
import {
  vector_distance_cos,
  vector_distance_l2,
  vector_distance_l2_squared,
  vector_distance_dot,
  vector_distance_hamming,
  vector_norm,
  vector_normalize,
  vector_add,
  vector_sub,
  vector_scale,
  vector_dot_product,
  getDistanceFunction,
  distanceToScore,
} from './distance.js';

// Import HNSW index
import { HnswIndex } from './hnsw.js';

// Import vector column
import { VectorColumn, hybridSearch } from './column.js';

// Import SQL functions
import {
  vector,
  vector_extract,
  vector_dims,
  sql_vector_distance_cos,
  sql_vector_distance_l2,
  sql_vector_distance_dot,
  sql_vector_norm,
  sql_vector_normalize,
  sql_vector_add,
  sql_vector_sub,
  sql_vector_scale,
  sql_vector_dot,
  createSqlVector,
  extractVector,
  isSqlVector,
} from '../functions/vector.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Generate a random vector of specified dimensions
 */
function randomVector(dims: number): Float32Array {
  const vec = new Float32Array(dims);
  for (let i = 0; i < dims; i++) {
    vec[i] = Math.random() * 2 - 1; // [-1, 1]
  }
  return vec;
}

/**
 * Generate a normalized random vector
 */
function randomNormalizedVector(dims: number): Float32Array {
  const vec = randomVector(dims);
  return vector_normalize(vec);
}

/**
 * Compute exact cosine distance for reference
 */
function exactCosineDistance(a: Float32Array, b: Float32Array): number {
  let dot = 0, normA = 0, normB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  return 1 - dot / (Math.sqrt(normA) * Math.sqrt(normB));
}

// =============================================================================
// VECTOR TYPES TESTS
// =============================================================================

describe('Vector Types', () => {
  describe('VectorType enum', () => {
    it('should have all expected types', () => {
      expect(VectorType.F64).toBe('f64');
      expect(VectorType.F32).toBe('f32');
      expect(VectorType.F16).toBe('f16');
      expect(VectorType.BF16).toBe('bf16');
      expect(VectorType.I8).toBe('i8');
      expect(VectorType.F1BIT).toBe('f1bit');
    });
  });

  describe('DistanceMetric enum', () => {
    it('should have all expected metrics', () => {
      expect(DistanceMetric.Cosine).toBe('cosine');
      expect(DistanceMetric.L2).toBe('l2');
      expect(DistanceMetric.Dot).toBe('dot');
      expect(DistanceMetric.Hamming).toBe('hamming');
    });
  });

  describe('toVector()', () => {
    it('should convert number array to Float32Array', () => {
      const result = toVector([1, 2, 3]);
      expect(result).toBeInstanceOf(Float32Array);
      expect(Array.from(result)).toEqual([1, 2, 3]);
    });

    it('should pass through Float32Array unchanged', () => {
      const input = new Float32Array([1, 2, 3]);
      const result = toVector(input);
      expect(result).toBe(input);
    });

    it('should convert Float64Array to Float32Array', () => {
      const input = new Float64Array([1, 2, 3]);
      const result = toVector(input);
      expect(result).toBeInstanceOf(Float32Array);
      expect(Array.from(result)).toEqual([1, 2, 3]);
    });
  });

  describe('validateDimensions()', () => {
    it('should pass for matching dimensions', () => {
      const vec = new Float32Array([1, 2, 3]);
      expect(() => validateDimensions(vec, 3)).not.toThrow();
    });

    it('should throw for mismatched dimensions', () => {
      const vec = new Float32Array([1, 2, 3]);
      expect(() => validateDimensions(vec, 4)).toThrow('dimension mismatch');
    });
  });

  describe('vectorByteSize()', () => {
    it('should calculate correct sizes', () => {
      expect(vectorByteSize(128, VectorType.F32)).toBe(512);
      expect(vectorByteSize(128, VectorType.F64)).toBe(1024);
      expect(vectorByteSize(128, VectorType.I8)).toBe(128);
      expect(vectorByteSize(128, VectorType.F1BIT)).toBe(16);
    });
  });

  describe('isVectorInput()', () => {
    it('should return true for valid inputs', () => {
      expect(isVectorInput([1, 2, 3])).toBe(true);
      expect(isVectorInput(new Float32Array([1, 2, 3]))).toBe(true);
      expect(isVectorInput(new Float64Array([1, 2, 3]))).toBe(true);
      expect(isVectorInput(new Int8Array([1, 2, 3]))).toBe(true);
    });

    it('should return false for invalid inputs', () => {
      expect(isVectorInput('hello')).toBe(false);
      expect(isVectorInput(123)).toBe(false);
      expect(isVectorInput(null)).toBe(false);
      expect(isVectorInput(undefined)).toBe(false);
    });
  });
});

// =============================================================================
// DISTANCE FUNCTION TESTS
// =============================================================================

describe('Distance Functions', () => {
  const v1 = new Float32Array([1, 0, 0]);
  const v2 = new Float32Array([0, 1, 0]);
  const v3 = new Float32Array([1, 0, 0]);
  const v4 = new Float32Array([-1, 0, 0]);

  describe('vector_distance_cos()', () => {
    it('should return 0 for identical vectors', () => {
      expect(vector_distance_cos(v1, v3)).toBeCloseTo(0, 5);
    });

    it('should return 1 for orthogonal vectors', () => {
      expect(vector_distance_cos(v1, v2)).toBeCloseTo(1, 5);
    });

    it('should return 2 for opposite vectors', () => {
      expect(vector_distance_cos(v1, v4)).toBeCloseTo(2, 5);
    });

    it('should throw for dimension mismatch', () => {
      const short = new Float32Array([1, 2]);
      expect(() => vector_distance_cos(v1, short)).toThrow('dimension mismatch');
    });
  });

  describe('vector_distance_l2()', () => {
    it('should return 0 for identical vectors', () => {
      expect(vector_distance_l2(v1, v3)).toBeCloseTo(0, 5);
    });

    it('should return sqrt(2) for orthogonal unit vectors', () => {
      expect(vector_distance_l2(v1, v2)).toBeCloseTo(Math.sqrt(2), 5);
    });

    it('should return 2 for opposite unit vectors', () => {
      expect(vector_distance_l2(v1, v4)).toBeCloseTo(2, 5);
    });
  });

  describe('vector_distance_l2_squared()', () => {
    it('should return squared distance', () => {
      expect(vector_distance_l2_squared(v1, v2)).toBeCloseTo(2, 5);
    });
  });

  describe('vector_distance_dot()', () => {
    it('should return negative dot product', () => {
      // v1 dot v2 = 0, so distance = -0 = 0
      expect(vector_distance_dot(v1, v2)).toBeCloseTo(0, 5);
      // v1 dot v3 = 1, so distance = -1
      expect(vector_distance_dot(v1, v3)).toBeCloseTo(-1, 5);
      // v1 dot v4 = -1, so distance = 1
      expect(vector_distance_dot(v1, v4)).toBeCloseTo(1, 5);
    });
  });

  describe('vector_distance_hamming()', () => {
    it('should count differing bits', () => {
      const a = new Float32Array([1, 0, 1, 0]);
      const b = new Float32Array([1, 1, 0, 0]);
      // Positions 1 and 2 differ
      expect(vector_distance_hamming(a, b)).toBe(2);
    });
  });

  describe('vector_norm()', () => {
    it('should compute L2 norm', () => {
      const v = new Float32Array([3, 4]);
      expect(vector_norm(v)).toBeCloseTo(5, 5);
    });
  });

  describe('vector_normalize()', () => {
    it('should normalize to unit length', () => {
      const v = new Float32Array([3, 4]);
      const normalized = vector_normalize(v);
      expect(vector_norm(normalized)).toBeCloseTo(1, 5);
      expect(normalized[0]).toBeCloseTo(0.6, 5);
      expect(normalized[1]).toBeCloseTo(0.8, 5);
    });

    it('should handle zero vector', () => {
      const v = new Float32Array([0, 0, 0]);
      const normalized = vector_normalize(v);
      expect(normalized[0]).toBe(0);
      expect(normalized[1]).toBe(0);
      expect(normalized[2]).toBe(0);
    });
  });

  describe('vector operations', () => {
    const a = new Float32Array([1, 2, 3]);
    const b = new Float32Array([4, 5, 6]);

    it('should add vectors', () => {
      const result = vector_add(a, b);
      expect(Array.from(result)).toEqual([5, 7, 9]);
    });

    it('should subtract vectors', () => {
      const result = vector_sub(a, b);
      expect(Array.from(result)).toEqual([-3, -3, -3]);
    });

    it('should scale vectors', () => {
      const result = vector_scale(a, 2);
      expect(Array.from(result)).toEqual([2, 4, 6]);
    });

    it('should compute dot product', () => {
      expect(vector_dot_product(a, b)).toBe(32); // 1*4 + 2*5 + 3*6 = 32
    });
  });

  describe('getDistanceFunction()', () => {
    it('should return correct function for each metric', () => {
      const cosine = getDistanceFunction(DistanceMetric.Cosine);
      const l2 = getDistanceFunction(DistanceMetric.L2);
      const dot = getDistanceFunction(DistanceMetric.Dot);

      expect(cosine(v1, v3)).toBeCloseTo(0, 5);
      expect(l2(v1, v2)).toBeCloseTo(2, 5); // squared
      expect(dot(v1, v3)).toBeCloseTo(-1, 5);
    });
  });

  describe('distanceToScore()', () => {
    it('should convert cosine distance to score', () => {
      expect(distanceToScore(0, DistanceMetric.Cosine)).toBeCloseTo(1, 5);
      expect(distanceToScore(2, DistanceMetric.Cosine)).toBeCloseTo(0, 5);
    });

    it('should convert L2 distance to score', () => {
      expect(distanceToScore(0, DistanceMetric.L2)).toBe(1);
      expect(distanceToScore(1, DistanceMetric.L2)).toBeLessThan(1);
    });
  });
});

// =============================================================================
// HNSW INDEX TESTS
// =============================================================================

describe('HNSW Index', () => {
  describe('construction', () => {
    it('should create with default config', () => {
      const index = new HnswIndex();
      expect(index.size).toBe(0);
      const config = index.getConfig();
      expect(config.M).toBe(DEFAULT_HNSW_CONFIG.M);
      expect(config.efConstruction).toBe(DEFAULT_HNSW_CONFIG.efConstruction);
      expect(config.efSearch).toBe(DEFAULT_HNSW_CONFIG.efSearch);
    });

    it('should create with custom config', () => {
      const index = new HnswIndex({
        M: 32,
        efConstruction: 400,
        efSearch: 200,
        distanceMetric: DistanceMetric.L2,
      });
      const config = index.getConfig();
      expect(config.M).toBe(32);
      expect(config.efConstruction).toBe(400);
      expect(config.efSearch).toBe(200);
      expect(config.distanceMetric).toBe(DistanceMetric.L2);
    });
  });

  describe('insert and search', () => {
    let index: HnswIndex;
    const dims = 8;

    beforeEach(() => {
      index = new HnswIndex({
        M: 8,
        efConstruction: 50,
        efSearch: 50,
        distanceMetric: DistanceMetric.Cosine,
        seed: 42, // For reproducibility
      });
    });

    it('should insert vectors', () => {
      const v1 = randomVector(dims);
      index.insert(1n, v1);
      expect(index.size).toBe(1);
      expect(index.has(1n)).toBe(true);
    });

    it('should retrieve inserted vectors', () => {
      const v1 = new Float32Array([1, 2, 3, 4, 5, 6, 7, 8]);
      index.insert(1n, v1);
      const retrieved = index.getVector(1n);
      expect(retrieved).toBeDefined();
      expect(Array.from(retrieved!)).toEqual(Array.from(v1));
    });

    it('should reject dimension mismatch', () => {
      const v1 = randomVector(dims);
      const v2 = randomVector(dims + 1);
      index.insert(1n, v1);
      expect(() => index.insert(2n, v2)).toThrow('mismatch');
    });

    it('should reject duplicate IDs', () => {
      const v1 = randomVector(dims);
      index.insert(1n, v1);
      expect(() => index.insert(1n, v1)).toThrow('already exists');
    });

    it('should find exact matches', () => {
      const v1 = randomNormalizedVector(dims);
      index.insert(1n, v1);

      const results = index.search(v1, 1);
      expect(results.length).toBe(1);
      expect(results[0].rowId).toBe(1n);
      expect(results[0].distance).toBeCloseTo(0, 5);
    });

    it('should find approximate nearest neighbors', () => {
      // Insert 100 random vectors
      const vectors: Float32Array[] = [];
      for (let i = 0; i < 100; i++) {
        const v = randomNormalizedVector(dims);
        vectors.push(v);
        index.insert(BigInt(i), v);
      }

      // Search for a random query
      const query = randomNormalizedVector(dims);
      const k = 10;
      const results = index.search(query, k);

      expect(results.length).toBe(k);

      // Verify results are sorted by distance
      for (let i = 1; i < results.length; i++) {
        expect(results[i].distance).toBeGreaterThanOrEqual(results[i - 1].distance);
      }

      // Compute exact distances and verify recall
      const exactDistances: { id: bigint; distance: number }[] = [];
      for (let i = 0; i < vectors.length; i++) {
        exactDistances.push({
          id: BigInt(i),
          distance: exactCosineDistance(query, vectors[i]),
        });
      }
      exactDistances.sort((a, b) => a.distance - b.distance);

      // Check if top result from HNSW is in top 5 exact results (good recall)
      const topExactIds = new Set(exactDistances.slice(0, 5).map((d) => d.id));
      const foundInTop5 = results.slice(0, 1).some((r) => topExactIds.has(r.rowId));
      expect(foundInTop5).toBe(true);
    });

    it('should support filtering', () => {
      // Insert 50 vectors
      for (let i = 0; i < 50; i++) {
        index.insert(BigInt(i), randomNormalizedVector(dims));
      }

      const query = randomNormalizedVector(dims);

      // Filter to only even IDs
      const filter = (id: bigint) => Number(id) % 2 === 0;
      const results = index.search(query, 10, undefined, filter);

      // Verify all results have even IDs
      for (const result of results) {
        expect(Number(result.rowId) % 2).toBe(0);
      }
    });
  });

  describe('delete', () => {
    it('should delete vectors', () => {
      const index = new HnswIndex();
      index.insert(1n, new Float32Array([1, 2, 3]));
      index.insert(2n, new Float32Array([4, 5, 6]));

      expect(index.delete(1n)).toBe(true);
      expect(index.size).toBe(1);
      expect(index.has(1n)).toBe(false);
      expect(index.has(2n)).toBe(true);
    });

    it('should return false for non-existent ID', () => {
      const index = new HnswIndex();
      expect(index.delete(999n)).toBe(false);
    });
  });

  describe('serialization', () => {
    it('should serialize and deserialize JSON', () => {
      const index = new HnswIndex({ M: 8, seed: 42 });
      index.insert(1n, new Float32Array([1, 2, 3, 4]));
      index.insert(2n, new Float32Array([5, 6, 7, 8]));

      const json = index.serialize();
      const restored = HnswIndex.deserialize(json);

      expect(restored.size).toBe(2);
      expect(restored.has(1n)).toBe(true);
      expect(restored.has(2n)).toBe(true);

      const v1 = restored.getVector(1n);
      expect(Array.from(v1!)).toEqual([1, 2, 3, 4]);
    });

    it('should serialize and deserialize binary', () => {
      const index = new HnswIndex({ M: 8, seed: 42 });
      index.insert(1n, new Float32Array([1, 2, 3, 4]));
      index.insert(2n, new Float32Array([5, 6, 7, 8]));

      const binary = index.serializeBinary();
      const restored = HnswIndex.deserializeBinary(binary);

      expect(restored.size).toBe(2);
      expect(restored.has(1n)).toBe(true);
      expect(restored.has(2n)).toBe(true);
    });
  });

  describe('stats', () => {
    it('should return index statistics', () => {
      const index = new HnswIndex({ M: 8 });
      for (let i = 0; i < 10; i++) {
        index.insert(BigInt(i), randomVector(16));
      }

      const stats = index.getStats();
      expect(stats.nodeCount).toBe(10);
      expect(stats.dimensions).toBe(16);
      expect(stats.config.M).toBe(8);
    });
  });
});

// =============================================================================
// VECTOR COLUMN TESTS
// =============================================================================

describe('Vector Column', () => {
  const columnDef: VectorColumnDef = {
    name: 'embedding',
    dimensions: 8,
    type: VectorType.F32,
    distanceMetric: DistanceMetric.Cosine,
  };

  describe('basic operations', () => {
    let column: VectorColumn;

    beforeEach(() => {
      column = new VectorColumn({ columnDef });
    });

    it('should store and retrieve vectors', () => {
      const v = new Float32Array([1, 2, 3, 4, 5, 6, 7, 8]);
      column.set(1n, v);

      expect(column.size).toBe(1);
      expect(column.has(1n)).toBe(true);

      const retrieved = column.get(1n);
      expect(Array.from(retrieved!)).toEqual(Array.from(v));
    });

    it('should delete vectors', () => {
      column.set(1n, randomVector(8));
      column.set(2n, randomVector(8));

      expect(column.delete(1n)).toBe(true);
      expect(column.size).toBe(1);
      expect(column.has(1n)).toBe(false);
    });

    it('should clear all vectors', () => {
      column.set(1n, randomVector(8));
      column.set(2n, randomVector(8));

      column.clear();
      expect(column.size).toBe(0);
    });

    it('should validate dimensions', () => {
      expect(() => column.set(1n, randomVector(10))).toThrow('dimension mismatch');
    });
  });

  describe('search', () => {
    let column: VectorColumn;

    beforeEach(() => {
      column = new VectorColumn({
        columnDef,
        hnswM: 8,
        hnswEfConstruction: 50,
        hnswEfSearch: 50,
      });

      // Insert test vectors
      for (let i = 0; i < 50; i++) {
        column.set(BigInt(i), randomNormalizedVector(8));
      }
    });

    it('should find nearest neighbors', () => {
      const query = randomNormalizedVector(8);
      const results = column.search(query, 5);

      expect(results.length).toBe(5);

      // Results should be sorted by distance
      for (let i = 1; i < results.length; i++) {
        expect(results[i].distance).toBeGreaterThanOrEqual(results[i - 1].distance);
      }
    });

    it('should support filtered search', () => {
      const query = randomNormalizedVector(8);
      const filter = (id: bigint) => Number(id) < 25;
      const results = column.search(query, 5, undefined, filter);

      for (const result of results) {
        expect(Number(result.rowId)).toBeLessThan(25);
      }
    });

    it('should compute distance between query and stored vector', () => {
      const v = new Float32Array([1, 0, 0, 0, 0, 0, 0, 0]);
      column.set(100n, v);

      const query = new Float32Array([1, 0, 0, 0, 0, 0, 0, 0]);
      const distance = column.distance(query, 100n);

      expect(distance).toBeCloseTo(0, 5);
    });
  });

  describe('batch operations', () => {
    it('should batch insert vectors', () => {
      const column = new VectorColumn({ columnDef });

      const entries: [bigint, Float32Array][] = [];
      for (let i = 0; i < 100; i++) {
        entries.push([BigInt(i), randomVector(8)]);
      }

      column.batchSet(entries);
      expect(column.size).toBe(100);
    });
  });

  describe('serialization', () => {
    it('should serialize and deserialize', () => {
      const column = new VectorColumn({ columnDef });
      column.set(1n, new Float32Array([1, 2, 3, 4, 5, 6, 7, 8]));
      column.set(2n, new Float32Array([8, 7, 6, 5, 4, 3, 2, 1]));

      const binary = column.serialize();
      const restored = VectorColumn.deserialize(binary, columnDef);

      expect(restored.size).toBe(2);
      expect(Array.from(restored.get(1n)!)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    });
  });

  describe('iteration', () => {
    it('should iterate over entries', () => {
      const column = new VectorColumn({ columnDef });
      column.set(1n, randomVector(8));
      column.set(2n, randomVector(8));

      const entries = Array.from(column.entries());
      expect(entries.length).toBe(2);
    });

    it('should get all row IDs', () => {
      const column = new VectorColumn({ columnDef });
      column.set(1n, randomVector(8));
      column.set(2n, randomVector(8));

      const ids = column.getRowIds();
      expect(ids.sort()).toEqual([1n, 2n].sort());
    });
  });
});

// =============================================================================
// HYBRID SEARCH TESTS
// =============================================================================

describe('Hybrid Search', () => {
  const columnDef: VectorColumnDef = {
    name: 'embedding',
    dimensions: 8,
    type: VectorType.F32,
    distanceMetric: DistanceMetric.Cosine,
  };

  it('should filter before search for small filter sets', () => {
    const column = new VectorColumn({ columnDef });

    // Insert 100 vectors
    for (let i = 0; i < 100; i++) {
      column.set(BigInt(i), randomNormalizedVector(8));
    }

    // Filter to only first 10 IDs
    const filterIds = new Set([0n, 1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n, 9n]);

    const results = hybridSearch(column, randomNormalizedVector(8), {
      filterIds,
      k: 5,
    });

    expect(results.length).toBe(5);
    for (const result of results) {
      expect(filterIds.has(result.rowId)).toBe(true);
    }
  });

  it('should apply post-filter', () => {
    const column = new VectorColumn({ columnDef });

    for (let i = 0; i < 100; i++) {
      column.set(BigInt(i), randomNormalizedVector(8));
    }

    const results = hybridSearch(column, randomNormalizedVector(8), {
      postFilter: (id) => Number(id) % 2 === 0,
      k: 5,
    });

    for (const result of results) {
      expect(Number(result.rowId) % 2).toBe(0);
    }
  });
});

// =============================================================================
// SQL FUNCTION TESTS
// =============================================================================

describe('SQL Vector Functions', () => {
  describe('vector()', () => {
    it('should create vector from array', () => {
      const result = vector([1, 2, 3]);
      expect(result).toBeInstanceOf(Uint8Array);
    });

    it('should create vector from JSON string', () => {
      const result = vector('[1, 2, 3]');
      expect(result).toBeInstanceOf(Uint8Array);
    });

    it('should return null for null input', () => {
      expect(vector(null)).toBeNull();
    });

    it('should throw for invalid input', () => {
      expect(() => vector('not json')).toThrow();
      expect(() => vector(['a', 'b'])).toThrow();
    });
  });

  describe('vector_extract()', () => {
    it('should convert vector to JSON array', () => {
      const v = vector([1, 2, 3]);
      const extracted = vector_extract(v);
      expect(JSON.parse(extracted!)).toEqual([1, 2, 3]);
    });

    it('should return null for null input', () => {
      expect(vector_extract(null)).toBeNull();
    });
  });

  describe('vector_dims()', () => {
    it('should return vector dimensions', () => {
      const v = vector([1, 2, 3, 4, 5]);
      expect(vector_dims(v)).toBe(5);
    });
  });

  describe('sql_vector_distance_cos()', () => {
    it('should compute cosine distance', () => {
      const v1 = vector([1, 0, 0]);
      const v2 = vector([0, 1, 0]);
      expect(sql_vector_distance_cos(v1, v2)).toBeCloseTo(1, 5);
    });

    it('should return 0 for identical vectors', () => {
      const v = vector([1, 2, 3]);
      expect(sql_vector_distance_cos(v, v)).toBeCloseTo(0, 5);
    });
  });

  describe('sql_vector_distance_l2()', () => {
    it('should compute L2 distance', () => {
      const v1 = vector([0, 0, 0]);
      const v2 = vector([3, 4, 0]);
      expect(sql_vector_distance_l2(v1, v2)).toBeCloseTo(5, 5);
    });
  });

  describe('sql_vector_distance_dot()', () => {
    it('should compute dot product distance', () => {
      const v1 = vector([1, 2, 3]);
      const v2 = vector([1, 1, 1]);
      // dot = 1 + 2 + 3 = 6, distance = -6
      expect(sql_vector_distance_dot(v1, v2)).toBeCloseTo(-6, 5);
    });
  });

  describe('sql_vector_norm()', () => {
    it('should compute L2 norm', () => {
      const v = vector([3, 4]);
      expect(sql_vector_norm(v)).toBeCloseTo(5, 5);
    });
  });

  describe('sql_vector_normalize()', () => {
    it('should normalize vector', () => {
      const v = vector([3, 4]);
      const normalized = sql_vector_normalize(v);
      const norm = sql_vector_norm(normalized);
      expect(norm).toBeCloseTo(1, 5);
    });
  });

  describe('sql_vector_add()', () => {
    it('should add vectors', () => {
      const v1 = vector([1, 2, 3]);
      const v2 = vector([4, 5, 6]);
      const result = sql_vector_add(v1, v2);
      const extracted = JSON.parse(vector_extract(result)!);
      expect(extracted).toEqual([5, 7, 9]);
    });
  });

  describe('sql_vector_sub()', () => {
    it('should subtract vectors', () => {
      const v1 = vector([4, 5, 6]);
      const v2 = vector([1, 2, 3]);
      const result = sql_vector_sub(v1, v2);
      const extracted = JSON.parse(vector_extract(result)!);
      expect(extracted).toEqual([3, 3, 3]);
    });
  });

  describe('sql_vector_scale()', () => {
    it('should scale vector', () => {
      const v = vector([1, 2, 3]);
      const result = sql_vector_scale(v, 2);
      const extracted = JSON.parse(vector_extract(result)!);
      expect(extracted).toEqual([2, 4, 6]);
    });
  });

  describe('sql_vector_dot()', () => {
    it('should compute dot product', () => {
      const v1 = vector([1, 2, 3]);
      const v2 = vector([4, 5, 6]);
      expect(sql_vector_dot(v1, v2)).toBe(32);
    });
  });

  describe('createSqlVector() and extractVector()', () => {
    it('should round-trip through SQL format', () => {
      const original = [1.5, 2.5, 3.5];
      const sqlVec = createSqlVector(original);
      const extracted = extractVector(sqlVec);
      expect(Array.from(extracted!)).toEqual(original.map(v => Math.fround(v)));
    });
  });

  describe('isSqlVector()', () => {
    it('should identify valid vectors', () => {
      const v = vector([1, 2, 3]);
      expect(isSqlVector(v)).toBe(true);
    });

    it('should reject non-vectors', () => {
      expect(isSqlVector('hello')).toBe(false);
      expect(isSqlVector(123)).toBe(false);
      expect(isSqlVector(null)).toBe(false);
    });
  });
});

// =============================================================================
// SQL INTEGRATION TESTS
// =============================================================================

describe('SQL Integration', () => {
  it('should support typical vector search query pattern', () => {
    // Simulate: SELECT * FROM items ORDER BY vector_distance_cos(embedding, ?) LIMIT 10

    const column = new VectorColumn({
      columnDef: {
        name: 'embedding',
        dimensions: 8,
        type: VectorType.F32,
        distanceMetric: DistanceMetric.Cosine,
      },
    });

    // Insert test data
    const items = [
      { id: 1n, name: 'Item 1' },
      { id: 2n, name: 'Item 2' },
      { id: 3n, name: 'Item 3' },
      { id: 4n, name: 'Item 4' },
      { id: 5n, name: 'Item 5' },
    ];

    for (const item of items) {
      column.set(item.id, randomNormalizedVector(8));
    }

    // Query vector (from SQL parameter)
    const queryVec = randomNormalizedVector(8);
    const limit = 3;

    // Execute search (what the engine would do)
    const results = column.search(queryVec, limit);

    // Build result set
    const resultSet = results.map((r) => {
      const item = items.find((i) => i.id === r.rowId)!;
      return {
        ...item,
        distance: r.distance,
        score: r.score,
      };
    });

    expect(resultSet.length).toBe(limit);
    expect(resultSet[0].name).toBeDefined();
    expect(resultSet[0].distance).toBeDefined();
  });

  it('should support hybrid queries with scalar filters', () => {
    // Simulate: SELECT * FROM items WHERE category = 'electronics'
    //           ORDER BY vector_distance_cos(embedding, ?) LIMIT 10

    const column = new VectorColumn({
      columnDef: {
        name: 'embedding',
        dimensions: 8,
        type: VectorType.F32,
        distanceMetric: DistanceMetric.Cosine,
      },
    });

    // Insert test data with categories
    const items = [
      { id: 1n, name: 'Phone', category: 'electronics' },
      { id: 2n, name: 'Laptop', category: 'electronics' },
      { id: 3n, name: 'Chair', category: 'furniture' },
      { id: 4n, name: 'Tablet', category: 'electronics' },
      { id: 5n, name: 'Desk', category: 'furniture' },
    ];

    for (const item of items) {
      column.set(item.id, randomNormalizedVector(8));
    }

    // Pre-filter: get IDs matching category = 'electronics'
    const electronicsIds = new Set(
      items.filter((i) => i.category === 'electronics').map((i) => i.id),
    );

    // Execute hybrid search
    const results = hybridSearch(column, randomNormalizedVector(8), {
      filterIds: electronicsIds,
      k: 10,
    });

    // All results should be electronics
    for (const result of results) {
      const item = items.find((i) => i.id === result.rowId)!;
      expect(item.category).toBe('electronics');
    }
  });
});
