/**
 * Vector Distance Functions for DoSQL
 *
 * Implements distance/similarity metrics for vector search:
 * - Cosine distance (1 - cosine_similarity)
 * - Euclidean distance (L2)
 * - Dot product (for normalized vectors)
 * - Hamming distance (for binary vectors)
 *
 * @module dosql/vector/distance
 */

import { DistanceMetric, type Vector } from './types.js';

// =============================================================================
// DISTANCE FUNCTIONS
// =============================================================================

/**
 * Compute cosine distance between two vectors
 * Distance = 1 - cosine_similarity
 *
 * @param a First vector
 * @param b Second vector
 * @returns Cosine distance (0 = identical, 2 = opposite)
 */
export function vector_distance_cos(a: Vector, b: Vector): number {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
  }

  let dot = 0;
  let normA = 0;
  let normB = 0;

  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }

  const denom = Math.sqrt(normA) * Math.sqrt(normB);

  // Handle zero vectors
  if (denom === 0) {
    return 1.0;
  }

  // Clamp to handle floating point errors
  const similarity = Math.max(-1, Math.min(1, dot / denom));
  return 1 - similarity;
}

/**
 * Compute Euclidean (L2) distance between two vectors
 *
 * @param a First vector
 * @param b Second vector
 * @returns L2 distance (sqrt of sum of squared differences)
 */
export function vector_distance_l2(a: Vector, b: Vector): number {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
  }

  let sum = 0;
  for (let i = 0; i < a.length; i++) {
    const diff = a[i] - b[i];
    sum += diff * diff;
  }

  return Math.sqrt(sum);
}

/**
 * Compute squared Euclidean (L2) distance between two vectors
 * Faster than L2 when only relative ordering matters
 *
 * @param a First vector
 * @param b Second vector
 * @returns Squared L2 distance (sum of squared differences)
 */
export function vector_distance_l2_squared(a: Vector, b: Vector): number {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
  }

  let sum = 0;
  for (let i = 0; i < a.length; i++) {
    const diff = a[i] - b[i];
    sum += diff * diff;
  }

  return sum;
}

/**
 * Compute dot product distance (negative dot product)
 * For normalized vectors, this is equivalent to cosine distance
 *
 * @param a First vector
 * @param b Second vector
 * @returns Negative dot product (lower = more similar)
 */
export function vector_distance_dot(a: Vector, b: Vector): number {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
  }

  let dot = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
  }

  // Return negative so that "smaller distance = more similar"
  return -dot;
}

/**
 * Compute raw dot product (not as distance)
 *
 * @param a First vector
 * @param b Second vector
 * @returns Dot product (higher = more similar for normalized vectors)
 */
export function vector_dot_product(a: Vector, b: Vector): number {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
  }

  let dot = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
  }

  return dot;
}

/**
 * Compute Hamming distance between binary vectors
 * Counts the number of positions with different values
 *
 * @param a First vector (values treated as 0/1)
 * @param b Second vector (values treated as 0/1)
 * @returns Number of differing positions
 */
export function vector_distance_hamming(a: Vector, b: Vector): number {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
  }

  let diff = 0;
  for (let i = 0; i < a.length; i++) {
    // Convert to binary (0 or 1) and compare
    const bitA = a[i] >= 0.5 ? 1 : 0;
    const bitB = b[i] >= 0.5 ? 1 : 0;
    if (bitA !== bitB) {
      diff++;
    }
  }

  return diff;
}

/**
 * Compute Hamming distance for Uint8Array binary vectors (packed bits)
 *
 * @param a First packed binary vector
 * @param b Second packed binary vector
 * @returns Number of differing bits
 */
export function vector_distance_hamming_packed(a: Uint8Array, b: Uint8Array): number {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
  }

  let diff = 0;
  for (let i = 0; i < a.length; i++) {
    // XOR gives us the differing bits, popcount counts them
    let xor = a[i] ^ b[i];
    // Brian Kernighan's algorithm for popcount
    while (xor) {
      diff++;
      xor &= xor - 1;
    }
  }

  return diff;
}

// =============================================================================
// SIMILARITY FUNCTIONS (inverse of distance)
// =============================================================================

/**
 * Compute cosine similarity between two vectors
 *
 * @param a First vector
 * @param b Second vector
 * @returns Cosine similarity (-1 to 1, 1 = identical)
 */
export function vector_similarity_cos(a: Vector, b: Vector): number {
  return 1 - vector_distance_cos(a, b);
}

// =============================================================================
// VECTOR OPERATIONS
// =============================================================================

/**
 * Compute the L2 norm (magnitude) of a vector
 *
 * @param v Vector
 * @returns L2 norm
 */
export function vector_norm(v: Vector): number {
  let sum = 0;
  for (let i = 0; i < v.length; i++) {
    sum += v[i] * v[i];
  }
  return Math.sqrt(sum);
}

/**
 * Normalize a vector to unit length (L2 normalization)
 *
 * @param v Input vector
 * @returns Normalized vector (new array)
 */
export function vector_normalize(v: Vector): Vector {
  const norm = vector_norm(v);
  if (norm === 0) {
    return new Float32Array(v.length);
  }

  const result = new Float32Array(v.length);
  for (let i = 0; i < v.length; i++) {
    result[i] = v[i] / norm;
  }
  return result;
}

/**
 * Add two vectors element-wise
 *
 * @param a First vector
 * @param b Second vector
 * @returns Sum vector (new array)
 */
export function vector_add(a: Vector, b: Vector): Vector {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
  }

  const result = new Float32Array(a.length);
  for (let i = 0; i < a.length; i++) {
    result[i] = a[i] + b[i];
  }
  return result;
}

/**
 * Subtract two vectors element-wise (a - b)
 *
 * @param a First vector
 * @param b Second vector
 * @returns Difference vector (new array)
 */
export function vector_sub(a: Vector, b: Vector): Vector {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
  }

  const result = new Float32Array(a.length);
  for (let i = 0; i < a.length; i++) {
    result[i] = a[i] - b[i];
  }
  return result;
}

/**
 * Scale a vector by a scalar
 *
 * @param v Vector
 * @param scalar Scale factor
 * @returns Scaled vector (new array)
 */
export function vector_scale(v: Vector, scalar: number): Vector {
  const result = new Float32Array(v.length);
  for (let i = 0; i < v.length; i++) {
    result[i] = v[i] * scalar;
  }
  return result;
}

// =============================================================================
// DISTANCE FUNCTION FACTORY
// =============================================================================

/**
 * Distance function type
 */
export type DistanceFunction = (a: Vector, b: Vector) => number;

/**
 * Get the appropriate distance function for a metric type
 *
 * @param metric Distance metric type
 * @returns Distance function
 */
export function getDistanceFunction(metric: DistanceMetric): DistanceFunction {
  switch (metric) {
    case DistanceMetric.Cosine:
      return vector_distance_cos;
    case DistanceMetric.L2:
      return vector_distance_l2_squared; // Use squared for efficiency in comparisons
    case DistanceMetric.Dot:
      return vector_distance_dot;
    case DistanceMetric.Hamming:
      return vector_distance_hamming;
    default:
      throw new Error(`Unknown distance metric: ${metric}`);
  }
}

/**
 * Convert distance to similarity score based on metric
 * Score is normalized to [0, 1] range where 1 = most similar
 *
 * @param distance Distance value
 * @param metric Distance metric type
 * @returns Similarity score [0, 1]
 */
export function distanceToScore(distance: number, metric: DistanceMetric): number {
  switch (metric) {
    case DistanceMetric.Cosine:
      // Cosine distance is [0, 2], similarity is 1 - distance/2
      return 1 - distance / 2;
    case DistanceMetric.L2:
      // L2 distance is [0, infinity), use exponential decay
      return 1 / (1 + Math.sqrt(Math.max(0, distance)));
    case DistanceMetric.Dot:
      // Dot distance is negative dot product, use sigmoid
      return 1 / (1 + Math.exp(distance));
    case DistanceMetric.Hamming:
      // Hamming distance is [0, dimensions], normalize
      return 1 / (1 + distance);
    default:
      return 1 / (1 + distance);
  }
}
