/**
 * Distance Functions Unit Tests
 *
 * Comprehensive tests for vector distance calculations including:
 * - Cosine distance
 * - Euclidean (L2) distance
 * - Dot product distance
 * - Hamming distance
 * - Edge cases: zero vectors, high dimensions, sparse vectors
 *
 * @module dosql/vector/__tests__/distance.test
 */

import { describe, it, expect } from 'vitest';
import {
  vector_distance_cos,
  vector_distance_l2,
  vector_distance_l2_squared,
  vector_distance_dot,
  vector_dot_product,
  vector_distance_hamming,
  vector_distance_hamming_packed,
  vector_similarity_cos,
  vector_norm,
  vector_normalize,
  vector_add,
  vector_sub,
  vector_scale,
  getDistanceFunction,
  distanceToScore,
  type DistanceFunction,
} from '../distance.js';
import { DistanceMetric } from '../types.js';

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
 * Create a sparse vector with only a few non-zero values
 */
function sparseVector(dims: number, nonZeroIndices: { [index: number]: number }): Float32Array {
  const v = new Float32Array(dims);
  for (const [idx, val] of Object.entries(nonZeroIndices)) {
    v[Number(idx)] = val;
  }
  return v;
}

/**
 * Create a high-dimensional random vector
 */
function randomVector(dims: number, seed: number = 42): Float32Array {
  const v = new Float32Array(dims);
  // Simple seeded random for reproducibility
  let state = seed;
  for (let i = 0; i < dims; i++) {
    state = (state * 1664525 + 1013904223) >>> 0;
    v[i] = (state / 0xffffffff) * 2 - 1; // [-1, 1]
  }
  return v;
}

// =============================================================================
// COSINE DISTANCE TESTS
// =============================================================================

describe('Cosine Distance (vector_distance_cos)', () => {
  describe('basic cases', () => {
    it('should return 0 for identical unit vectors', () => {
      const v = vec(1, 0, 0);
      expect(vector_distance_cos(v, v)).toBeCloseTo(0, 10);
    });

    it('should return 0 for identical arbitrary vectors', () => {
      const v = vec(3, 4, 5);
      expect(vector_distance_cos(v, v)).toBeCloseTo(0, 10);
    });

    it('should return 1 for orthogonal vectors', () => {
      const v1 = vec(1, 0, 0);
      const v2 = vec(0, 1, 0);
      expect(vector_distance_cos(v1, v2)).toBeCloseTo(1, 10);
    });

    it('should return 2 for opposite vectors', () => {
      const v1 = vec(1, 0, 0);
      const v2 = vec(-1, 0, 0);
      expect(vector_distance_cos(v1, v2)).toBeCloseTo(2, 10);
    });

    it('should return 0 for parallel vectors (same direction)', () => {
      const v1 = vec(1, 2, 3);
      const v2 = vec(2, 4, 6);
      expect(vector_distance_cos(v1, v2)).toBeCloseTo(0, 10);
    });

    it('should be symmetric', () => {
      const v1 = vec(1, 2, 3);
      const v2 = vec(4, 5, 6);
      expect(vector_distance_cos(v1, v2)).toBeCloseTo(vector_distance_cos(v2, v1), 10);
    });
  });

  describe('edge cases', () => {
    it('should return 1 for zero vectors', () => {
      const zero = vec(0, 0, 0);
      const v = vec(1, 2, 3);
      expect(vector_distance_cos(zero, v)).toBe(1);
      expect(vector_distance_cos(zero, zero)).toBe(1);
    });

    it('should throw for dimension mismatch', () => {
      const v1 = vec(1, 2, 3);
      const v2 = vec(1, 2);
      expect(() => vector_distance_cos(v1, v2)).toThrow('dimension mismatch');
    });

    it('should handle very small vectors without NaN', () => {
      const tiny = vec(1e-30, 1e-30, 1e-30);
      const v = vec(1, 0, 0);
      const dist = vector_distance_cos(tiny, v);
      expect(Number.isNaN(dist)).toBe(false);
    });

    it('should handle very large vectors without overflow', () => {
      const large = vec(1e30, 1e30, 1e30);
      const v = vec(1, 1, 1);
      const dist = vector_distance_cos(large, v);
      expect(Number.isNaN(dist)).toBe(false);
      expect(dist).toBeCloseTo(0, 5); // Same direction
    });
  });

  describe('high dimensions', () => {
    it('should work with 128-dimensional vectors', () => {
      const v1 = randomVector(128, 1);
      const v2 = randomVector(128, 2);
      const dist = vector_distance_cos(v1, v2);
      expect(dist).toBeGreaterThanOrEqual(0);
      expect(dist).toBeLessThanOrEqual(2);
    });

    it('should work with 768-dimensional vectors (BERT-like)', () => {
      const v1 = randomVector(768, 1);
      const v2 = randomVector(768, 2);
      const dist = vector_distance_cos(v1, v2);
      expect(dist).toBeGreaterThanOrEqual(0);
      expect(dist).toBeLessThanOrEqual(2);
    });

    it('should work with 1536-dimensional vectors (OpenAI ada-002)', () => {
      const v1 = randomVector(1536, 1);
      const v2 = randomVector(1536, 2);
      const dist = vector_distance_cos(v1, v2);
      expect(dist).toBeGreaterThanOrEqual(0);
      expect(dist).toBeLessThanOrEqual(2);
    });

    it('should work with 3072-dimensional vectors (OpenAI text-embedding-3-large)', () => {
      const v1 = randomVector(3072, 1);
      const v2 = randomVector(3072, 2);
      const dist = vector_distance_cos(v1, v2);
      expect(dist).toBeGreaterThanOrEqual(0);
      expect(dist).toBeLessThanOrEqual(2);
    });
  });

  describe('sparse vectors', () => {
    it('should handle sparse vectors correctly', () => {
      // Two sparse vectors with single non-zero elements
      const v1 = sparseVector(100, { 0: 1.0 });
      const v2 = sparseVector(100, { 50: 1.0 });
      expect(vector_distance_cos(v1, v2)).toBeCloseTo(1, 10); // Orthogonal
    });

    it('should handle sparse vectors with overlapping non-zero elements', () => {
      const v1 = sparseVector(100, { 0: 1.0, 50: 1.0 });
      const v2 = sparseVector(100, { 0: 1.0, 99: 1.0 });
      // dot = 1, normA = sqrt(2), normB = sqrt(2)
      // similarity = 1 / 2 = 0.5
      // distance = 1 - 0.5 = 0.5
      expect(vector_distance_cos(v1, v2)).toBeCloseTo(0.5, 10);
    });

    it('should handle mostly-zero sparse vectors', () => {
      const v1 = sparseVector(1000, { 999: 1.0 });
      const v2 = sparseVector(1000, { 999: 2.0 });
      expect(vector_distance_cos(v1, v2)).toBeCloseTo(0, 10); // Same direction
    });
  });
});

// =============================================================================
// EUCLIDEAN DISTANCE TESTS
// =============================================================================

describe('Euclidean Distance (vector_distance_l2)', () => {
  describe('basic cases', () => {
    it('should return 0 for identical vectors', () => {
      const v = vec(1, 2, 3);
      expect(vector_distance_l2(v, v)).toBe(0);
    });

    it('should compute correct L2 distance', () => {
      const v1 = vec(0, 0, 0);
      const v2 = vec(3, 4, 0);
      expect(vector_distance_l2(v1, v2)).toBe(5);
    });

    it('should be symmetric', () => {
      const v1 = vec(1, 2, 3);
      const v2 = vec(4, 5, 6);
      expect(vector_distance_l2(v1, v2)).toBe(vector_distance_l2(v2, v1));
    });

    it('should satisfy triangle inequality', () => {
      const a = vec(0, 0, 0);
      const b = vec(1, 0, 0);
      const c = vec(1, 1, 0);
      const ab = vector_distance_l2(a, b);
      const bc = vector_distance_l2(b, c);
      const ac = vector_distance_l2(a, c);
      expect(ac).toBeLessThanOrEqual(ab + bc + 1e-10);
    });
  });

  describe('edge cases', () => {
    it('should handle zero vectors', () => {
      const zero = vec(0, 0, 0);
      const v = vec(1, 0, 0);
      expect(vector_distance_l2(zero, v)).toBe(1);
    });

    it('should throw for dimension mismatch', () => {
      const v1 = vec(1, 2, 3);
      const v2 = vec(1, 2);
      expect(() => vector_distance_l2(v1, v2)).toThrow('dimension mismatch');
    });

    it('should handle negative values', () => {
      const v1 = vec(-1, -2, -3);
      const v2 = vec(1, 2, 3);
      // Distance = sqrt(4 + 16 + 36) = sqrt(56)
      expect(vector_distance_l2(v1, v2)).toBeCloseTo(Math.sqrt(56), 10);
    });
  });

  describe('high dimensions', () => {
    it('should work with high-dimensional vectors', () => {
      const dims = 1024;
      const v1 = randomVector(dims, 1);
      const v2 = randomVector(dims, 2);
      const dist = vector_distance_l2(v1, v2);
      expect(dist).toBeGreaterThanOrEqual(0);
      expect(Number.isFinite(dist)).toBe(true);
    });
  });

  describe('sparse vectors', () => {
    it('should handle sparse vectors', () => {
      const v1 = sparseVector(100, { 0: 3, 1: 4 });
      const v2 = sparseVector(100, { 0: 0, 1: 0 });
      expect(vector_distance_l2(v1, v2)).toBe(5);
    });
  });
});

// =============================================================================
// L2 SQUARED DISTANCE TESTS
// =============================================================================

describe('Squared Euclidean Distance (vector_distance_l2_squared)', () => {
  it('should return squared distance', () => {
    const v1 = vec(0, 0, 0);
    const v2 = vec(3, 4, 0);
    expect(vector_distance_l2_squared(v1, v2)).toBe(25);
  });

  it('should be consistent with L2', () => {
    const v1 = vec(1, 2, 3);
    const v2 = vec(4, 5, 6);
    const l2 = vector_distance_l2(v1, v2);
    const l2sq = vector_distance_l2_squared(v1, v2);
    expect(l2 * l2).toBeCloseTo(l2sq, 10);
  });

  it('should preserve ordering', () => {
    const query = vec(0, 0, 0);
    const v1 = vec(1, 0, 0); // dist = 1
    const v2 = vec(2, 0, 0); // dist = 2

    const d1 = vector_distance_l2_squared(query, v1);
    const d2 = vector_distance_l2_squared(query, v2);
    expect(d1).toBeLessThan(d2);
  });
});

// =============================================================================
// DOT PRODUCT DISTANCE TESTS
// =============================================================================

describe('Dot Product Distance (vector_distance_dot)', () => {
  describe('basic cases', () => {
    it('should return negative dot product', () => {
      const v1 = vec(1, 2, 3);
      const v2 = vec(1, 1, 1);
      // dot = 1 + 2 + 3 = 6, distance = -6
      expect(vector_distance_dot(v1, v2)).toBe(-6);
    });

    it('should return 0 for orthogonal vectors', () => {
      const v1 = vec(1, 0, 0);
      const v2 = vec(0, 1, 0);
      expect(vector_distance_dot(v1, v2)).toBe(0);
    });

    it('should return positive for opposite vectors', () => {
      const v1 = vec(1, 0, 0);
      const v2 = vec(-1, 0, 0);
      expect(vector_distance_dot(v1, v2)).toBe(1);
    });
  });

  describe('edge cases', () => {
    it('should handle zero vectors', () => {
      const zero = vec(0, 0, 0);
      const v = vec(1, 2, 3);
      expect(vector_distance_dot(zero, v)).toBe(0);
    });

    it('should throw for dimension mismatch', () => {
      const v1 = vec(1, 2, 3);
      const v2 = vec(1, 2);
      expect(() => vector_distance_dot(v1, v2)).toThrow('dimension mismatch');
    });
  });

  describe('normalized vectors', () => {
    it('should be equivalent to cosine distance for normalized vectors', () => {
      const v1 = vector_normalize(vec(1, 2, 3));
      const v2 = vector_normalize(vec(4, 5, 6));

      const dotDist = vector_distance_dot(v1, v2);
      const cosDist = vector_distance_cos(v1, v2);

      // For normalized vectors: cos_dist = 1 - similarity = 1 + dot_dist
      expect(cosDist).toBeCloseTo(1 + dotDist, 5);
    });
  });
});

// =============================================================================
// RAW DOT PRODUCT TESTS
// =============================================================================

describe('Raw Dot Product (vector_dot_product)', () => {
  it('should compute dot product', () => {
    const v1 = vec(1, 2, 3);
    const v2 = vec(4, 5, 6);
    // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
    expect(vector_dot_product(v1, v2)).toBe(32);
  });

  it('should be commutative', () => {
    const v1 = vec(1, 2, 3);
    const v2 = vec(4, 5, 6);
    expect(vector_dot_product(v1, v2)).toBe(vector_dot_product(v2, v1));
  });

  it('should throw for dimension mismatch', () => {
    const v1 = vec(1, 2, 3);
    const v2 = vec(1, 2);
    expect(() => vector_dot_product(v1, v2)).toThrow('dimension mismatch');
  });
});

// =============================================================================
// HAMMING DISTANCE TESTS
// =============================================================================

describe('Hamming Distance (vector_distance_hamming)', () => {
  describe('basic cases', () => {
    it('should return 0 for identical vectors', () => {
      const v = vec(1, 0, 1, 0);
      expect(vector_distance_hamming(v, v)).toBe(0);
    });

    it('should count differing bits', () => {
      const v1 = vec(1, 0, 1, 0);
      const v2 = vec(1, 1, 0, 0);
      // Positions 1 and 2 differ
      expect(vector_distance_hamming(v1, v2)).toBe(2);
    });

    it('should return dimension count for completely different vectors', () => {
      const v1 = vec(1, 1, 1, 1);
      const v2 = vec(0, 0, 0, 0);
      expect(vector_distance_hamming(v1, v2)).toBe(4);
    });

    it('should be symmetric', () => {
      const v1 = vec(1, 0, 1, 0);
      const v2 = vec(0, 1, 1, 0);
      expect(vector_distance_hamming(v1, v2)).toBe(vector_distance_hamming(v2, v1));
    });
  });

  describe('threshold behavior', () => {
    it('should use 0.5 as threshold', () => {
      const v1 = vec(0.4, 0.6, 0.49, 0.51);
      const v2 = vec(0, 1, 0, 1);
      expect(vector_distance_hamming(v1, v2)).toBe(0);
    });
  });

  describe('edge cases', () => {
    it('should throw for dimension mismatch', () => {
      const v1 = vec(1, 0, 1);
      const v2 = vec(1, 0);
      expect(() => vector_distance_hamming(v1, v2)).toThrow('dimension mismatch');
    });
  });
});

// =============================================================================
// PACKED HAMMING DISTANCE TESTS
// =============================================================================

describe('Packed Hamming Distance (vector_distance_hamming_packed)', () => {
  it('should return 0 for identical packed vectors', () => {
    const v = new Uint8Array([0b10101010, 0b01010101]);
    expect(vector_distance_hamming_packed(v, v)).toBe(0);
  });

  it('should count differing bits in packed format', () => {
    const v1 = new Uint8Array([0b11111111]); // 8 ones
    const v2 = new Uint8Array([0b00000000]); // 8 zeros
    expect(vector_distance_hamming_packed(v1, v2)).toBe(8);
  });

  it('should work with multi-byte vectors', () => {
    const v1 = new Uint8Array([0b11111111, 0b11111111]); // 16 ones
    const v2 = new Uint8Array([0b00000000, 0b00000000]); // 16 zeros
    expect(vector_distance_hamming_packed(v1, v2)).toBe(16);
  });

  it('should count partial differences', () => {
    const v1 = new Uint8Array([0b10101010]); // 4 ones
    const v2 = new Uint8Array([0b01010101]); // 4 ones, all different positions
    expect(vector_distance_hamming_packed(v1, v2)).toBe(8);
  });

  it('should throw for dimension mismatch', () => {
    const v1 = new Uint8Array([0b11111111, 0b11111111]);
    const v2 = new Uint8Array([0b11111111]);
    expect(() => vector_distance_hamming_packed(v1, v2)).toThrow('dimension mismatch');
  });
});

// =============================================================================
// COSINE SIMILARITY TESTS
// =============================================================================

describe('Cosine Similarity (vector_similarity_cos)', () => {
  it('should return 1 for identical vectors', () => {
    const v = vec(1, 2, 3);
    expect(vector_similarity_cos(v, v)).toBeCloseTo(1, 10);
  });

  it('should return 0 for orthogonal vectors', () => {
    const v1 = vec(1, 0, 0);
    const v2 = vec(0, 1, 0);
    expect(vector_similarity_cos(v1, v2)).toBeCloseTo(0, 10);
  });

  it('should return -1 for opposite vectors', () => {
    const v1 = vec(1, 0, 0);
    const v2 = vec(-1, 0, 0);
    expect(vector_similarity_cos(v1, v2)).toBeCloseTo(-1, 10);
  });

  it('should be 1 - distance', () => {
    const v1 = vec(1, 2, 3);
    const v2 = vec(4, 5, 6);
    const dist = vector_distance_cos(v1, v2);
    const sim = vector_similarity_cos(v1, v2);
    expect(sim).toBeCloseTo(1 - dist, 10);
  });
});

// =============================================================================
// VECTOR OPERATIONS TESTS
// =============================================================================

describe('Vector Operations', () => {
  describe('vector_norm', () => {
    it('should compute L2 norm', () => {
      expect(vector_norm(vec(3, 4))).toBe(5);
      expect(vector_norm(vec(1, 0, 0))).toBe(1);
    });

    it('should return 0 for zero vector', () => {
      expect(vector_norm(vec(0, 0, 0))).toBe(0);
    });

    it('should work with high-dimensional vectors', () => {
      // Vector of all 1s, norm = sqrt(n)
      const dims = 100;
      const v = new Float32Array(dims).fill(1);
      expect(vector_norm(v)).toBeCloseTo(10, 5); // sqrt(100) = 10
    });
  });

  describe('vector_normalize', () => {
    it('should produce unit vectors', () => {
      const v = vec(3, 4);
      const normalized = vector_normalize(v);
      expect(vector_norm(normalized)).toBeCloseTo(1, 10);
    });

    it('should preserve direction', () => {
      const v = vec(3, 4);
      const normalized = vector_normalize(v);
      // Check ratio is preserved
      expect(normalized[0] / normalized[1]).toBeCloseTo(3 / 4, 10);
    });

    it('should return zero vector for zero input', () => {
      const v = vec(0, 0, 0);
      const normalized = vector_normalize(v);
      expect(Array.from(normalized)).toEqual([0, 0, 0]);
    });

    it('should return new array', () => {
      const v = vec(3, 4);
      const normalized = vector_normalize(v);
      expect(normalized).not.toBe(v);
    });
  });

  describe('vector_add', () => {
    it('should add vectors element-wise', () => {
      const v1 = vec(1, 2, 3);
      const v2 = vec(4, 5, 6);
      const result = vector_add(v1, v2);
      expect(Array.from(result)).toEqual([5, 7, 9]);
    });

    it('should handle negative values', () => {
      const v1 = vec(-1, 2, -3);
      const v2 = vec(1, -2, 3);
      const result = vector_add(v1, v2);
      expect(Array.from(result)).toEqual([0, 0, 0]);
    });

    it('should throw for dimension mismatch', () => {
      const v1 = vec(1, 2, 3);
      const v2 = vec(1, 2);
      expect(() => vector_add(v1, v2)).toThrow('dimension mismatch');
    });
  });

  describe('vector_sub', () => {
    it('should subtract vectors element-wise', () => {
      const v1 = vec(5, 7, 9);
      const v2 = vec(1, 2, 3);
      const result = vector_sub(v1, v2);
      expect(Array.from(result)).toEqual([4, 5, 6]);
    });

    it('should produce zero for identical vectors', () => {
      const v = vec(1, 2, 3);
      const result = vector_sub(v, v);
      expect(Array.from(result)).toEqual([0, 0, 0]);
    });

    it('should throw for dimension mismatch', () => {
      const v1 = vec(1, 2, 3);
      const v2 = vec(1, 2);
      expect(() => vector_sub(v1, v2)).toThrow('dimension mismatch');
    });
  });

  describe('vector_scale', () => {
    it('should scale vector by scalar', () => {
      const v = vec(1, 2, 3);
      const result = vector_scale(v, 2);
      expect(Array.from(result)).toEqual([2, 4, 6]);
    });

    it('should handle negative scalars', () => {
      const v = vec(1, 2, 3);
      const result = vector_scale(v, -1);
      expect(Array.from(result)).toEqual([-1, -2, -3]);
    });

    it('should handle zero scalar', () => {
      const v = vec(1, 2, 3);
      const result = vector_scale(v, 0);
      expect(Array.from(result)).toEqual([0, 0, 0]);
    });

    it('should handle fractional scalars', () => {
      const v = vec(2, 4, 6);
      const result = vector_scale(v, 0.5);
      expect(Array.from(result)).toEqual([1, 2, 3]);
    });
  });
});

// =============================================================================
// DISTANCE FUNCTION FACTORY TESTS
// =============================================================================

describe('getDistanceFunction', () => {
  it('should return cosine distance function', () => {
    const fn = getDistanceFunction(DistanceMetric.Cosine);
    const v1 = vec(1, 0, 0);
    const v2 = vec(0, 1, 0);
    expect(fn(v1, v2)).toBeCloseTo(1, 10);
  });

  it('should return L2 squared distance function', () => {
    const fn = getDistanceFunction(DistanceMetric.L2);
    const v1 = vec(0, 0, 0);
    const v2 = vec(3, 4, 0);
    expect(fn(v1, v2)).toBe(25); // Squared
  });

  it('should return dot product distance function', () => {
    const fn = getDistanceFunction(DistanceMetric.Dot);
    const v1 = vec(1, 2, 3);
    const v2 = vec(1, 1, 1);
    expect(fn(v1, v2)).toBe(-6);
  });

  it('should return hamming distance function', () => {
    const fn = getDistanceFunction(DistanceMetric.Hamming);
    const v1 = vec(1, 0, 1, 0);
    const v2 = vec(1, 1, 0, 0);
    expect(fn(v1, v2)).toBe(2);
  });
});

// =============================================================================
// DISTANCE TO SCORE CONVERSION TESTS
// =============================================================================

describe('distanceToScore', () => {
  describe('cosine metric', () => {
    it('should return 1 for distance 0', () => {
      expect(distanceToScore(0, DistanceMetric.Cosine)).toBe(1);
    });

    it('should return 0.5 for distance 1', () => {
      expect(distanceToScore(1, DistanceMetric.Cosine)).toBe(0.5);
    });

    it('should return 0 for distance 2', () => {
      expect(distanceToScore(2, DistanceMetric.Cosine)).toBe(0);
    });
  });

  describe('L2 metric', () => {
    it('should return 1 for distance 0', () => {
      expect(distanceToScore(0, DistanceMetric.L2)).toBe(1);
    });

    it('should decrease for larger distances', () => {
      const score1 = distanceToScore(1, DistanceMetric.L2);
      const score2 = distanceToScore(4, DistanceMetric.L2);
      const score3 = distanceToScore(16, DistanceMetric.L2);
      expect(score1).toBeGreaterThan(score2);
      expect(score2).toBeGreaterThan(score3);
    });

    it('should approach 0 for very large distances', () => {
      const score = distanceToScore(10000, DistanceMetric.L2);
      expect(score).toBeLessThan(0.01);
    });
  });

  describe('dot metric', () => {
    it('should use sigmoid transformation', () => {
      // distance = -dot, so negative distance means positive dot (similar)
      const scoreHigh = distanceToScore(-10, DistanceMetric.Dot);
      const scoreLow = distanceToScore(10, DistanceMetric.Dot);
      expect(scoreHigh).toBeGreaterThan(scoreLow);
    });
  });

  describe('hamming metric', () => {
    it('should return 1 for distance 0', () => {
      expect(distanceToScore(0, DistanceMetric.Hamming)).toBe(1);
    });

    it('should decrease for larger distances', () => {
      const score1 = distanceToScore(1, DistanceMetric.Hamming);
      const score5 = distanceToScore(5, DistanceMetric.Hamming);
      expect(score1).toBeGreaterThan(score5);
    });
  });
});

// =============================================================================
// NUMERICAL STABILITY TESTS
// =============================================================================

describe('Numerical Stability', () => {
  it('should handle subnormal numbers in cosine distance', () => {
    const tiny = new Float32Array([1e-38, 1e-38, 1e-38]);
    const v = vec(1, 0, 0);
    const dist = vector_distance_cos(tiny, v);
    expect(Number.isNaN(dist)).toBe(false);
    expect(Number.isFinite(dist)).toBe(true);
  });

  it('should not overflow with large vectors in L2', () => {
    // Large values that could overflow when squared
    const large = vec(1e20, 1e20, 1e20);
    const v = vec(0, 0, 0);
    const dist = vector_distance_l2(large, v);
    // Result may be Infinity but should not be NaN
    expect(Number.isNaN(dist)).toBe(false);
  });

  it('should maintain precision with small differences', () => {
    const v1 = vec(1, 1, 1);
    const v2 = vec(1 + 1e-7, 1 + 1e-7, 1 + 1e-7);
    const dist = vector_distance_cos(v1, v2);
    // Should be very small but not exactly 0
    expect(dist).toBeGreaterThanOrEqual(0);
    expect(dist).toBeLessThan(0.001);
  });
});
