/**
 * HNSW Index Unit Tests
 *
 * Comprehensive tests for HNSW (Hierarchical Navigable Small World) index:
 * - Index construction with various configurations
 * - Insert, search, and delete operations
 * - Serialization and deserialization
 * - Edge cases: empty index, single element, duplicate handling
 * - High-dimensional vectors and recall quality
 *
 * @module dosql/vector/__tests__/hnsw.test
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { HnswIndex } from '../hnsw.js';
import {
  DistanceMetric,
  DEFAULT_HNSW_CONFIG,
  type HnswConfig,
  type Vector,
} from '../types.js';
import { vector_distance_cos, vector_normalize } from '../distance.js';

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
 * Create a seeded random vector generator for reproducibility
 */
function createVectorGenerator(dims: number, seed: number = 42) {
  let state = seed;
  return (): Float32Array => {
    const v = new Float32Array(dims);
    for (let i = 0; i < dims; i++) {
      state = (state * 1664525 + 1013904223) >>> 0;
      v[i] = (state / 0xffffffff) * 2 - 1; // [-1, 1]
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
 * Compute exact nearest neighbors using brute force
 */
function exactKNN(
  query: Vector,
  vectors: Map<bigint, Vector>,
  k: number,
  distanceMetric: DistanceMetric = DistanceMetric.Cosine,
): Array<{ id: bigint; distance: number }> {
  const distances: Array<{ id: bigint; distance: number }> = [];

  for (const [id, vector] of vectors) {
    let dist: number;
    switch (distanceMetric) {
      case DistanceMetric.Cosine:
        dist = vector_distance_cos(query, vector);
        break;
      default:
        dist = vector_distance_cos(query, vector);
    }
    distances.push({ id, distance: dist });
  }

  distances.sort((a, b) => a.distance - b.distance);
  return distances.slice(0, k);
}

/**
 * Compute recall@k: what fraction of true top-k are in approximate top-k
 */
function computeRecall(
  exact: Array<{ id: bigint; distance: number }>,
  approximate: Array<{ rowId: bigint; distance: number }>,
  k: number,
): number {
  const exactIds = new Set(exact.slice(0, k).map((e) => e.id));
  const approxIds = new Set(approximate.slice(0, k).map((a) => a.rowId));

  let matches = 0;
  for (const id of approxIds) {
    if (exactIds.has(id)) {
      matches++;
    }
  }

  return matches / k;
}

// =============================================================================
// CONSTRUCTION TESTS
// =============================================================================

describe('HnswIndex Construction', () => {
  describe('default configuration', () => {
    it('should create an empty index with default config', () => {
      const index = new HnswIndex();
      expect(index.size).toBe(0);
      expect(index.dim).toBe(0);
    });

    it('should use default config values', () => {
      const index = new HnswIndex();
      const config = index.getConfig();
      expect(config.M).toBe(DEFAULT_HNSW_CONFIG.M);
      expect(config.efConstruction).toBe(DEFAULT_HNSW_CONFIG.efConstruction);
      expect(config.efSearch).toBe(DEFAULT_HNSW_CONFIG.efSearch);
      expect(config.distanceMetric).toBe(DEFAULT_HNSW_CONFIG.distanceMetric);
    });
  });

  describe('custom configuration', () => {
    it('should accept custom M parameter', () => {
      const index = new HnswIndex({ M: 32 });
      expect(index.getConfig().M).toBe(32);
    });

    it('should accept custom efConstruction parameter', () => {
      const index = new HnswIndex({ efConstruction: 400 });
      expect(index.getConfig().efConstruction).toBe(400);
    });

    it('should accept custom efSearch parameter', () => {
      const index = new HnswIndex({ efSearch: 200 });
      expect(index.getConfig().efSearch).toBe(200);
    });

    it('should accept custom distance metric', () => {
      const index = new HnswIndex({ distanceMetric: DistanceMetric.L2 });
      expect(index.getConfig().distanceMetric).toBe(DistanceMetric.L2);
    });

    it('should accept seed for reproducibility', () => {
      const index = new HnswIndex({ seed: 12345 });
      expect(index.getConfig().seed).toBe(12345);
    });

    it('should produce deterministic results with same seed', () => {
      const dims = 16;
      const gen = createVectorGenerator(dims, 1);

      // Build index 1
      const index1 = new HnswIndex({ M: 8, seed: 42 });
      for (let i = 0; i < 50; i++) {
        index1.insert(BigInt(i), gen());
      }

      // Reset generator and build index 2
      const gen2 = createVectorGenerator(dims, 1);
      const index2 = new HnswIndex({ M: 8, seed: 42 });
      for (let i = 0; i < 50; i++) {
        index2.insert(BigInt(i), gen2());
      }

      // Same query should return same results
      const query = vec(0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5);
      const results1 = index1.search(query, 5);
      const results2 = index2.search(query, 5);

      expect(results1.map((r) => r.rowId)).toEqual(results2.map((r) => r.rowId));
    });
  });
});

// =============================================================================
// INSERT TESTS
// =============================================================================

describe('HnswIndex Insert', () => {
  describe('basic insert', () => {
    it('should insert a single vector', () => {
      const index = new HnswIndex();
      index.insert(1n, vec(1, 2, 3));
      expect(index.size).toBe(1);
      expect(index.dim).toBe(3);
    });

    it('should insert multiple vectors', () => {
      const index = new HnswIndex();
      index.insert(1n, vec(1, 2, 3));
      index.insert(2n, vec(4, 5, 6));
      index.insert(3n, vec(7, 8, 9));
      expect(index.size).toBe(3);
    });

    it('should set dimensions from first vector', () => {
      const index = new HnswIndex();
      expect(index.dim).toBe(0);
      index.insert(1n, vec(1, 2, 3, 4, 5));
      expect(index.dim).toBe(5);
    });
  });

  describe('insert validation', () => {
    it('should reject dimension mismatch', () => {
      const index = new HnswIndex();
      index.insert(1n, vec(1, 2, 3));
      expect(() => index.insert(2n, vec(1, 2, 3, 4))).toThrow('Dimension mismatch');
    });

    it('should reject duplicate IDs', () => {
      const index = new HnswIndex();
      index.insert(1n, vec(1, 2, 3));
      expect(() => index.insert(1n, vec(4, 5, 6))).toThrow('already exists');
    });
  });

  describe('large scale insert', () => {
    it('should handle 1000 vectors', () => {
      const index = new HnswIndex({ M: 8, efConstruction: 50, seed: 42 });
      const dims = 32;
      const gen = createVectorGenerator(dims, 1);

      for (let i = 0; i < 1000; i++) {
        index.insert(BigInt(i), gen());
      }

      expect(index.size).toBe(1000);
    });
  });
});

// =============================================================================
// SEARCH TESTS
// =============================================================================

describe('HnswIndex Search', () => {
  describe('basic search', () => {
    let index: HnswIndex;
    const dims = 8;

    beforeEach(() => {
      index = new HnswIndex({
        M: 8,
        efConstruction: 50,
        efSearch: 50,
        distanceMetric: DistanceMetric.Cosine,
        seed: 42,
      });
    });

    it('should return empty for empty index', () => {
      const results = index.search(vec(1, 2, 3, 4, 5, 6, 7, 8), 5);
      expect(results).toEqual([]);
    });

    it('should find exact match', () => {
      const v = vec(1, 0, 0, 0, 0, 0, 0, 0);
      index.insert(1n, v);

      const results = index.search(v, 1);
      expect(results.length).toBe(1);
      expect(results[0].rowId).toBe(1n);
      expect(results[0].distance).toBeCloseTo(0, 5);
    });

    it('should return k results when available', () => {
      for (let i = 0; i < 100; i++) {
        index.insert(BigInt(i), randomNormalizedVector(dims, i));
      }

      const query = randomNormalizedVector(dims, 999);
      const results = index.search(query, 10);
      expect(results.length).toBe(10);
    });

    it('should return fewer than k results if not enough vectors', () => {
      index.insert(1n, randomNormalizedVector(dims, 1));
      index.insert(2n, randomNormalizedVector(dims, 2));
      index.insert(3n, randomNormalizedVector(dims, 3));

      const results = index.search(randomNormalizedVector(dims, 999), 10);
      expect(results.length).toBe(3);
    });

    it('should return results sorted by distance', () => {
      for (let i = 0; i < 50; i++) {
        index.insert(BigInt(i), randomNormalizedVector(dims, i));
      }

      const results = index.search(randomNormalizedVector(dims, 999), 20);

      for (let i = 1; i < results.length; i++) {
        expect(results[i].distance).toBeGreaterThanOrEqual(results[i - 1].distance);
      }
    });
  });

  describe('search with dimension validation', () => {
    it('should reject query with wrong dimensions', () => {
      const index = new HnswIndex();
      index.insert(1n, vec(1, 2, 3, 4));

      expect(() => index.search(vec(1, 2, 3), 1)).toThrow('dimension mismatch');
    });
  });

  describe('search with efSearch parameter', () => {
    it('should use efSearch override when provided', () => {
      const index = new HnswIndex({
        M: 8,
        efConstruction: 50,
        efSearch: 10, // Low default
        seed: 42,
      });

      for (let i = 0; i < 100; i++) {
        index.insert(BigInt(i), randomNormalizedVector(8, i));
      }

      const query = randomNormalizedVector(8, 999);

      // Search with default efSearch=10
      const results1 = index.search(query, 5);

      // Search with higher efSearch=100
      const results2 = index.search(query, 5, 100);

      // Both should return 5 results but may differ in quality
      expect(results1.length).toBe(5);
      expect(results2.length).toBe(5);
    });
  });

  describe('search with filter', () => {
    it('should respect filter function', () => {
      const index = new HnswIndex({ M: 8, efConstruction: 50, efSearch: 50, seed: 42 });

      for (let i = 0; i < 100; i++) {
        index.insert(BigInt(i), randomNormalizedVector(8, i));
      }

      const query = randomNormalizedVector(8, 999);

      // Filter: only even IDs
      const results = index.search(query, 10, undefined, (id) => Number(id) % 2 === 0);

      expect(results.length).toBe(10);
      for (const result of results) {
        expect(Number(result.rowId) % 2).toBe(0);
      }
    });

    it('should return empty if filter excludes all', () => {
      const index = new HnswIndex({ M: 8, seed: 42 });

      for (let i = 0; i < 10; i++) {
        index.insert(BigInt(i), randomNormalizedVector(4, i));
      }

      const results = index.search(randomNormalizedVector(4, 999), 5, undefined, () => false);
      expect(results.length).toBe(0);
    });

    it('should return fewer results if filter limits candidates', () => {
      const index = new HnswIndex({ M: 8, efConstruction: 50, efSearch: 50, seed: 42 });

      for (let i = 0; i < 100; i++) {
        index.insert(BigInt(i), randomNormalizedVector(8, i));
      }

      // Filter: only IDs < 3
      const results = index.search(
        randomNormalizedVector(8, 999),
        10,
        undefined,
        (id) => Number(id) < 3,
      );

      expect(results.length).toBeLessThanOrEqual(3);
    });
  });

  describe('recall quality', () => {
    it('should achieve good recall on random data', () => {
      const dims = 32;
      const n = 500;
      const k = 10;

      const index = new HnswIndex({
        M: 16,
        efConstruction: 100,
        efSearch: 50,
        distanceMetric: DistanceMetric.Cosine,
        seed: 42,
      });

      const vectors = new Map<bigint, Vector>();
      const gen = createVectorGenerator(dims, 1);

      for (let i = 0; i < n; i++) {
        const v = vector_normalize(gen());
        vectors.set(BigInt(i), v);
        index.insert(BigInt(i), v);
      }

      // Test recall on 10 queries
      let totalRecall = 0;
      for (let q = 0; q < 10; q++) {
        const query = vector_normalize(createVectorGenerator(dims, 1000 + q)());
        const exact = exactKNN(query, vectors, k);
        const approx = index.search(query, k);
        const recall = computeRecall(exact, approx, k);
        totalRecall += recall;
      }

      const avgRecall = totalRecall / 10;
      // Expect at least 70% recall with these parameters
      expect(avgRecall).toBeGreaterThanOrEqual(0.7);
    });

    it('should improve recall with higher efSearch', () => {
      const dims = 32;
      const n = 200;
      const k = 5;

      const index = new HnswIndex({
        M: 8,
        efConstruction: 50,
        efSearch: 10, // Low default
        distanceMetric: DistanceMetric.Cosine,
        seed: 42,
      });

      const vectors = new Map<bigint, Vector>();
      const gen = createVectorGenerator(dims, 1);

      for (let i = 0; i < n; i++) {
        const v = vector_normalize(gen());
        vectors.set(BigInt(i), v);
        index.insert(BigInt(i), v);
      }

      const query = vector_normalize(createVectorGenerator(dims, 999)());
      const exact = exactKNN(query, vectors, k);

      // Low efSearch
      const approxLow = index.search(query, k, 10);
      const recallLow = computeRecall(exact, approxLow, k);

      // High efSearch
      const approxHigh = index.search(query, k, 200);
      const recallHigh = computeRecall(exact, approxHigh, k);

      // Higher efSearch should give equal or better recall
      expect(recallHigh).toBeGreaterThanOrEqual(recallLow);
    });
  });
});

// =============================================================================
// DELETE TESTS
// =============================================================================

describe('HnswIndex Delete', () => {
  it('should delete existing vectors', () => {
    const index = new HnswIndex();
    index.insert(1n, vec(1, 2, 3));
    index.insert(2n, vec(4, 5, 6));

    expect(index.delete(1n)).toBe(true);
    expect(index.size).toBe(1);
    expect(index.has(1n)).toBe(false);
    expect(index.has(2n)).toBe(true);
  });

  it('should return false for non-existent IDs', () => {
    const index = new HnswIndex();
    index.insert(1n, vec(1, 2, 3));

    expect(index.delete(999n)).toBe(false);
    expect(index.size).toBe(1);
  });

  it('should handle delete of entry point', () => {
    const index = new HnswIndex({ seed: 42 });
    index.insert(1n, vec(1, 2, 3));
    index.insert(2n, vec(4, 5, 6));
    index.insert(3n, vec(7, 8, 9));

    // Delete first (likely entry point)
    expect(index.delete(1n)).toBe(true);
    expect(index.size).toBe(2);

    // Index should still work
    const results = index.search(vec(4, 5, 6), 2);
    expect(results.length).toBe(2);
  });

  it('should handle delete of all vectors', () => {
    const index = new HnswIndex();
    index.insert(1n, vec(1, 2, 3));
    index.insert(2n, vec(4, 5, 6));

    index.delete(1n);
    index.delete(2n);

    expect(index.size).toBe(0);

    // Should return empty for search
    const results = index.search(vec(1, 2, 3), 1);
    expect(results).toEqual([]);
  });

  it('should allow reinsertion after delete', () => {
    const index = new HnswIndex();
    index.insert(1n, vec(1, 2, 3));
    index.delete(1n);
    index.insert(1n, vec(4, 5, 6));

    expect(index.size).toBe(1);
    expect(index.has(1n)).toBe(true);
    const v = index.getVector(1n);
    expect(Array.from(v!)).toEqual([4, 5, 6]);
  });
});

// =============================================================================
// GET VECTOR TESTS
// =============================================================================

describe('HnswIndex GetVector', () => {
  it('should retrieve inserted vectors', () => {
    const index = new HnswIndex();
    const v = vec(1.5, 2.5, 3.5);
    index.insert(1n, v);

    const retrieved = index.getVector(1n);
    expect(retrieved).toBeDefined();
    expect(Array.from(retrieved!)).toEqual(Array.from(v));
  });

  it('should return undefined for non-existent IDs', () => {
    const index = new HnswIndex();
    expect(index.getVector(999n)).toBeUndefined();
  });

  it('should return a copy (verify immutability)', () => {
    const index = new HnswIndex();
    const v = vec(1, 2, 3);
    index.insert(1n, v);

    const retrieved = index.getVector(1n);
    retrieved![0] = 999;

    const retrievedAgain = index.getVector(1n);
    // Original should be unchanged in index
    expect(retrievedAgain![0]).toBe(1);
  });
});

// =============================================================================
// HAS TESTS
// =============================================================================

describe('HnswIndex Has', () => {
  it('should return true for existing IDs', () => {
    const index = new HnswIndex();
    index.insert(1n, vec(1, 2, 3));
    expect(index.has(1n)).toBe(true);
  });

  it('should return false for non-existent IDs', () => {
    const index = new HnswIndex();
    expect(index.has(999n)).toBe(false);
  });

  it('should return false after delete', () => {
    const index = new HnswIndex();
    index.insert(1n, vec(1, 2, 3));
    index.delete(1n);
    expect(index.has(1n)).toBe(false);
  });
});

// =============================================================================
// CLEAR TESTS
// =============================================================================

describe('HnswIndex Clear', () => {
  it('should remove all vectors', () => {
    const index = new HnswIndex();
    index.insert(1n, vec(1, 2, 3));
    index.insert(2n, vec(4, 5, 6));
    index.insert(3n, vec(7, 8, 9));

    index.clear();

    expect(index.size).toBe(0);
    expect(index.dim).toBe(0);
    expect(index.has(1n)).toBe(false);
  });

  it('should allow reinsertion after clear', () => {
    const index = new HnswIndex();
    index.insert(1n, vec(1, 2, 3));
    index.clear();
    index.insert(1n, vec(4, 5, 6, 7)); // Different dimensions allowed

    expect(index.size).toBe(1);
    expect(index.dim).toBe(4);
  });
});

// =============================================================================
// SERIALIZATION TESTS
// =============================================================================

describe('HnswIndex Serialization', () => {
  describe('JSON serialization', () => {
    it('should serialize empty index', () => {
      const index = new HnswIndex({ M: 8 });
      const json = index.serialize();

      const restored = HnswIndex.deserialize(json);
      expect(restored.size).toBe(0);
      expect(restored.getConfig().M).toBe(8);
    });

    it('should serialize and restore vectors', () => {
      const index = new HnswIndex({ M: 8, seed: 42 });
      index.insert(1n, vec(1, 2, 3, 4));
      index.insert(2n, vec(5, 6, 7, 8));

      const json = index.serialize();
      const restored = HnswIndex.deserialize(json);

      expect(restored.size).toBe(2);
      expect(restored.has(1n)).toBe(true);
      expect(restored.has(2n)).toBe(true);

      expect(Array.from(restored.getVector(1n)!)).toEqual([1, 2, 3, 4]);
      expect(Array.from(restored.getVector(2n)!)).toEqual([5, 6, 7, 8]);
    });

    it('should preserve search quality after restore', () => {
      const index = new HnswIndex({ M: 8, efConstruction: 50, efSearch: 50, seed: 42 });

      for (let i = 0; i < 50; i++) {
        index.insert(BigInt(i), randomNormalizedVector(8, i));
      }

      const query = randomNormalizedVector(8, 999);
      const resultsBefore = index.search(query, 5);

      const json = index.serialize();
      const restored = HnswIndex.deserialize(json);

      const resultsAfter = restored.search(query, 5);

      expect(resultsAfter.map((r) => r.rowId)).toEqual(resultsBefore.map((r) => r.rowId));
    });

    it('should throw for unsupported version', () => {
      const badJson = JSON.stringify({ version: 999, config: {}, nodes: [] });
      expect(() => HnswIndex.deserialize(badJson)).toThrow('Unsupported index version');
    });
  });

  describe('binary serialization', () => {
    it('should serialize empty index', () => {
      const index = new HnswIndex({ M: 8 });
      const binary = index.serializeBinary();

      const restored = HnswIndex.deserializeBinary(binary);
      expect(restored.size).toBe(0);
      expect(restored.getConfig().M).toBe(8);
    });

    it('should serialize and restore vectors', () => {
      const index = new HnswIndex({ M: 8, seed: 42 });
      index.insert(1n, vec(1.5, 2.5, 3.5, 4.5));
      index.insert(2n, vec(5.5, 6.5, 7.5, 8.5));

      const binary = index.serializeBinary();
      const restored = HnswIndex.deserializeBinary(binary);

      expect(restored.size).toBe(2);
      expect(restored.has(1n)).toBe(true);
      expect(restored.has(2n)).toBe(true);
    });

    it('should be smaller than JSON for large indexes', () => {
      const index = new HnswIndex({ M: 8, seed: 42 });

      for (let i = 0; i < 100; i++) {
        index.insert(BigInt(i), randomNormalizedVector(64, i));
      }

      const json = index.serialize();
      const binary = index.serializeBinary();

      // Binary should be significantly smaller
      expect(binary.length).toBeLessThan(json.length * 0.5);
    });

    it('should preserve config after restore', () => {
      const index = new HnswIndex({
        M: 24,
        efConstruction: 300,
        efSearch: 150,
        distanceMetric: DistanceMetric.L2,
      });
      index.insert(1n, vec(1, 2, 3));

      const binary = index.serializeBinary();
      const restored = HnswIndex.deserializeBinary(binary);

      const config = restored.getConfig();
      expect(config.M).toBe(24);
      expect(config.efConstruction).toBe(300);
      expect(config.efSearch).toBe(150);
      expect(config.distanceMetric).toBe(DistanceMetric.L2);
    });

    it('should throw for unsupported version', () => {
      const badBinary = new Uint8Array(100);
      new DataView(badBinary.buffer).setUint32(0, 999, true); // version 999
      expect(() => HnswIndex.deserializeBinary(badBinary)).toThrow('Unsupported index version');
    });
  });
});

// =============================================================================
// STATS TESTS
// =============================================================================

describe('HnswIndex Stats', () => {
  it('should return stats for empty index', () => {
    const index = new HnswIndex({ M: 16 });
    const stats = index.getStats();

    expect(stats.nodeCount).toBe(0);
    expect(stats.dimensions).toBe(0);
    expect(stats.maxLevel).toBe(0);
    expect(stats.avgConnectionsLevel0).toBe(0);
    expect(stats.config.M).toBe(16);
  });

  it('should return accurate stats', () => {
    const index = new HnswIndex({ M: 8, seed: 42 });

    for (let i = 0; i < 100; i++) {
      index.insert(BigInt(i), randomNormalizedVector(16, i));
    }

    const stats = index.getStats();

    expect(stats.nodeCount).toBe(100);
    expect(stats.dimensions).toBe(16);
    expect(stats.maxLevel).toBeGreaterThanOrEqual(0);
    expect(stats.avgConnectionsLevel0).toBeGreaterThan(0);
    expect(stats.avgConnectionsLevel0).toBeLessThanOrEqual(16); // M * 2 for level 0
  });
});

// =============================================================================
// HIGH-DIMENSIONAL TESTS
// =============================================================================

describe('HnswIndex High Dimensions', () => {
  it('should handle 128-dimensional vectors', () => {
    const index = new HnswIndex({ M: 16, efConstruction: 100, efSearch: 50, seed: 42 });
    const dims = 128;

    for (let i = 0; i < 100; i++) {
      index.insert(BigInt(i), randomNormalizedVector(dims, i));
    }

    expect(index.size).toBe(100);
    expect(index.dim).toBe(dims);

    const results = index.search(randomNormalizedVector(dims, 999), 5);
    expect(results.length).toBe(5);
  });

  it('should handle 768-dimensional vectors (BERT-like)', () => {
    const index = new HnswIndex({ M: 16, efConstruction: 100, efSearch: 50, seed: 42 });
    const dims = 768;

    for (let i = 0; i < 50; i++) {
      index.insert(BigInt(i), randomNormalizedVector(dims, i));
    }

    expect(index.size).toBe(50);
    expect(index.dim).toBe(dims);

    const results = index.search(randomNormalizedVector(dims, 999), 5);
    expect(results.length).toBe(5);
  });

  it('should handle 1536-dimensional vectors (OpenAI ada-002)', () => {
    const index = new HnswIndex({ M: 16, efConstruction: 100, efSearch: 50, seed: 42 });
    const dims = 1536;

    for (let i = 0; i < 20; i++) {
      index.insert(BigInt(i), randomNormalizedVector(dims, i));
    }

    expect(index.size).toBe(20);
    expect(index.dim).toBe(dims);

    const results = index.search(randomNormalizedVector(dims, 999), 5);
    expect(results.length).toBe(5);
  });
});

// =============================================================================
// DISTANCE METRIC TESTS
// =============================================================================

describe('HnswIndex Distance Metrics', () => {
  it('should work with cosine distance', () => {
    const index = new HnswIndex({ M: 8, distanceMetric: DistanceMetric.Cosine, seed: 42 });

    // Insert orthogonal vectors
    index.insert(1n, vec(1, 0, 0, 0));
    index.insert(2n, vec(0, 1, 0, 0));
    index.insert(3n, vec(0, 0, 1, 0));

    // Query should find closest
    const results = index.search(vec(1, 0.1, 0, 0), 3);
    expect(results[0].rowId).toBe(1n);
  });

  it('should work with L2 distance', () => {
    const index = new HnswIndex({ M: 8, distanceMetric: DistanceMetric.L2, seed: 42 });

    index.insert(1n, vec(0, 0, 0, 0));
    index.insert(2n, vec(1, 0, 0, 0));
    index.insert(3n, vec(2, 0, 0, 0));

    // Query close to origin
    const results = index.search(vec(0.1, 0, 0, 0), 3);
    expect(results[0].rowId).toBe(1n);
  });

  it('should work with dot product distance', () => {
    const index = new HnswIndex({ M: 8, distanceMetric: DistanceMetric.Dot, seed: 42 });

    index.insert(1n, vec(1, 0, 0, 0));
    index.insert(2n, vec(0, 1, 0, 0));
    index.insert(3n, vec(-1, 0, 0, 0));

    // Dot distance is negative dot product, so higher dot = lower distance
    const results = index.search(vec(1, 0, 0, 0), 3);
    expect(results[0].rowId).toBe(1n); // Highest dot product = 1
    expect(results[2].rowId).toBe(3n); // Lowest dot product = -1
  });

  it('should work with hamming distance', () => {
    const index = new HnswIndex({ M: 8, distanceMetric: DistanceMetric.Hamming, seed: 42 });

    index.insert(1n, vec(1, 1, 1, 1));
    index.insert(2n, vec(1, 1, 0, 0));
    index.insert(3n, vec(0, 0, 0, 0));

    // Query for all 1s
    const results = index.search(vec(1, 1, 1, 1), 3);
    expect(results[0].rowId).toBe(1n); // Exact match
    expect(results[1].rowId).toBe(2n); // 2 bits different
    expect(results[2].rowId).toBe(3n); // 4 bits different
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('HnswIndex Edge Cases', () => {
  it('should handle single-element index', () => {
    const index = new HnswIndex({ seed: 42 });
    index.insert(1n, vec(1, 2, 3));

    const results = index.search(vec(4, 5, 6), 5);
    expect(results.length).toBe(1);
    expect(results[0].rowId).toBe(1n);
  });

  it('should handle search with k=0', () => {
    const index = new HnswIndex({ seed: 42 });
    index.insert(1n, vec(1, 2, 3));

    const results = index.search(vec(1, 2, 3), 0);
    expect(results).toEqual([]);
  });

  it('should handle very small M parameter', () => {
    const index = new HnswIndex({ M: 2, seed: 42 });

    for (let i = 0; i < 20; i++) {
      index.insert(BigInt(i), randomNormalizedVector(4, i));
    }

    expect(index.size).toBe(20);

    const results = index.search(randomNormalizedVector(4, 999), 5);
    expect(results.length).toBe(5);
  });

  it('should handle very large efSearch', () => {
    const index = new HnswIndex({ M: 8, efSearch: 10, seed: 42 });

    for (let i = 0; i < 50; i++) {
      index.insert(BigInt(i), randomNormalizedVector(8, i));
    }

    // Use efSearch larger than dataset
    const results = index.search(randomNormalizedVector(8, 999), 5, 1000);
    expect(results.length).toBe(5);
  });

  it('should handle 1-dimensional vectors', () => {
    const index = new HnswIndex({ M: 4, seed: 42 });

    index.insert(1n, vec(0.1));
    index.insert(2n, vec(0.5));
    index.insert(3n, vec(0.9));

    const results = index.search(vec(0.5), 3);
    expect(results.length).toBe(3);
  });
});
