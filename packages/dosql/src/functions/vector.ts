/**
 * Vector SQL Functions for DoSQL
 *
 * SQL-compatible functions for vector operations:
 * - vector(array) - Create vector from array
 * - vector_distance_cos(v1, v2) - Cosine distance
 * - vector_distance_l2(v1, v2) - Euclidean distance
 * - vector_distance_dot(v1, v2) - Dot product distance
 * - vector_extract(v) - Convert vector back to array
 * - vector_dims(v) - Get vector dimensions
 * - vector_norm(v) - Get L2 norm
 *
 * @example
 * ```sql
 * -- Find similar items
 * SELECT * FROM items
 * ORDER BY vector_distance_cos(embedding, vector([0.1, 0.2, ...]))
 * LIMIT 10
 * ```
 *
 * @module @dotdo/dosql/functions/vector
 */

import type { SqlValue } from '../engine/types.js';
import type { SqlFunction, FunctionSignature } from './registry.js';
import {
  toVector,
  isVectorInput,
  type Vector,
  type VectorInput,
} from '../vector/types.js';
import {
  vector_distance_cos as distCos,
  vector_distance_l2 as distL2,
  vector_distance_dot as distDot,
  vector_norm as vecNorm,
  vector_normalize as vecNormalize,
  vector_add as vecAdd,
  vector_sub as vecSub,
  vector_scale as vecScale,
  vector_dot_product as vecDotProduct,
} from '../vector/distance.js';

// =============================================================================
// VECTOR TYPE HANDLING
// =============================================================================

/**
 * Internal representation of a vector in SQL
 * We use a Float32Array prefixed with a marker for identification
 */
const VECTOR_MARKER = '__dosql_vector__';

/**
 * Create a SQL vector value from input
 */
export function createSqlVector(input: VectorInput): Uint8Array {
  const vec = toVector(input);
  // Store as marker + dimensions + data
  const markerBytes = new TextEncoder().encode(VECTOR_MARKER);
  const result = new Uint8Array(markerBytes.length + 4 + vec.length * 4);

  result.set(markerBytes, 0);

  const view = new DataView(result.buffer);
  view.setUint32(markerBytes.length, vec.length, true);

  const floatView = new Float32Array(vec.length);
  floatView.set(vec);
  result.set(new Uint8Array(floatView.buffer), markerBytes.length + 4);

  return result;
}

/**
 * Extract a vector from a SQL value
 */
export function extractVector(value: SqlValue): Vector | null {
  if (value === null) return null;

  // Handle array input
  if (Array.isArray(value)) {
    if (!value.every((v) => typeof v === 'number')) return null;
    return toVector(value as number[]);
  }

  // Handle Float32Array directly
  if (value instanceof Float32Array) {
    return value;
  }

  // Handle Uint8Array (serialized vector)
  if (value instanceof Uint8Array) {
    const markerBytes = new TextEncoder().encode(VECTOR_MARKER);

    // Check marker
    const marker = new TextDecoder().decode(value.slice(0, markerBytes.length));
    if (marker !== VECTOR_MARKER) {
      // Might be raw float data
      if (value.length % 4 === 0) {
        return new Float32Array(value.buffer, value.byteOffset, value.length / 4);
      }
      return null;
    }

    // Parse dimensions and data
    const view = new DataView(value.buffer, value.byteOffset);
    const dims = view.getUint32(markerBytes.length, true);
    const dataOffset = markerBytes.length + 4;
    return new Float32Array(value.buffer, value.byteOffset + dataOffset, dims);
  }

  // Handle JSON string representation
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed) && parsed.every((v) => typeof v === 'number')) {
        return toVector(parsed);
      }
    } catch {
      return null;
    }
  }

  return null;
}

/**
 * Check if a SQL value is a vector
 */
export function isSqlVector(value: SqlValue): boolean {
  return extractVector(value) !== null;
}

// =============================================================================
// SQL FUNCTION IMPLEMENTATIONS
// =============================================================================

/**
 * vector(array) - Create a vector from an array
 *
 * @example
 * ```sql
 * SELECT vector('[1.0, 2.0, 3.0]')
 * SELECT vector(ARRAY[1.0, 2.0, 3.0])
 * ```
 */
export function vector(input: SqlValue): Uint8Array | null {
  if (input === null) return null;

  // Handle array
  if (Array.isArray(input)) {
    if (!input.every((v) => typeof v === 'number')) {
      throw new Error('vector() requires an array of numbers');
    }
    return createSqlVector(input as number[]);
  }

  // Handle JSON string
  if (typeof input === 'string') {
    try {
      const parsed = JSON.parse(input);
      if (!Array.isArray(parsed) || !parsed.every((v) => typeof v === 'number')) {
        throw new Error('vector() requires a JSON array of numbers');
      }
      return createSqlVector(parsed);
    } catch (e) {
      throw new Error(`vector() failed to parse input: ${e}`);
    }
  }

  // Handle typed arrays
  if (input instanceof Float32Array || input instanceof Float64Array) {
    return createSqlVector(input);
  }

  // Handle Uint8Array (already a vector)
  if (input instanceof Uint8Array) {
    const vec = extractVector(input);
    if (vec) return createSqlVector(vec);
  }

  throw new Error('vector() requires an array, JSON string, or typed array');
}

/**
 * vector_distance_cos(v1, v2) - Compute cosine distance
 *
 * @example
 * ```sql
 * SELECT vector_distance_cos(embedding, query_vector) FROM items
 * ```
 */
export function sql_vector_distance_cos(v1: SqlValue, v2: SqlValue): number | null {
  const vec1 = extractVector(v1);
  const vec2 = extractVector(v2);

  if (vec1 === null || vec2 === null) return null;

  return distCos(vec1, vec2);
}

/**
 * vector_distance_l2(v1, v2) - Compute Euclidean distance
 */
export function sql_vector_distance_l2(v1: SqlValue, v2: SqlValue): number | null {
  const vec1 = extractVector(v1);
  const vec2 = extractVector(v2);

  if (vec1 === null || vec2 === null) return null;

  return distL2(vec1, vec2);
}

/**
 * vector_distance_dot(v1, v2) - Compute dot product distance
 * (negative dot product, so smaller = more similar)
 */
export function sql_vector_distance_dot(v1: SqlValue, v2: SqlValue): number | null {
  const vec1 = extractVector(v1);
  const vec2 = extractVector(v2);

  if (vec1 === null || vec2 === null) return null;

  return distDot(vec1, vec2);
}

/**
 * vector_extract(v) - Convert vector back to JSON array
 *
 * @example
 * ```sql
 * SELECT vector_extract(embedding) FROM items
 * ```
 */
export function vector_extract(v: SqlValue): string | null {
  const vec = extractVector(v);
  if (vec === null) return null;

  return JSON.stringify(Array.from(vec));
}

/**
 * vector_dims(v) - Get the number of dimensions
 */
export function vector_dims(v: SqlValue): number | null {
  const vec = extractVector(v);
  if (vec === null) return null;

  return vec.length;
}

/**
 * vector_norm(v) - Get the L2 norm of a vector
 */
export function sql_vector_norm(v: SqlValue): number | null {
  const vec = extractVector(v);
  if (vec === null) return null;

  return vecNorm(vec);
}

/**
 * vector_normalize(v) - Normalize a vector to unit length
 */
export function sql_vector_normalize(v: SqlValue): Uint8Array | null {
  const vec = extractVector(v);
  if (vec === null) return null;

  return createSqlVector(vecNormalize(vec));
}

/**
 * vector_add(v1, v2) - Add two vectors element-wise
 */
export function sql_vector_add(v1: SqlValue, v2: SqlValue): Uint8Array | null {
  const vec1 = extractVector(v1);
  const vec2 = extractVector(v2);

  if (vec1 === null || vec2 === null) return null;

  return createSqlVector(vecAdd(vec1, vec2));
}

/**
 * vector_sub(v1, v2) - Subtract two vectors element-wise
 */
export function sql_vector_sub(v1: SqlValue, v2: SqlValue): Uint8Array | null {
  const vec1 = extractVector(v1);
  const vec2 = extractVector(v2);

  if (vec1 === null || vec2 === null) return null;

  return createSqlVector(vecSub(vec1, vec2));
}

/**
 * vector_scale(v, scalar) - Scale a vector by a scalar
 */
export function sql_vector_scale(v: SqlValue, scalar: SqlValue): Uint8Array | null {
  const vec = extractVector(v);
  if (vec === null) return null;

  if (typeof scalar !== 'number') {
    throw new Error('vector_scale() requires a numeric scalar');
  }

  return createSqlVector(vecScale(vec, scalar));
}

/**
 * vector_dot(v1, v2) - Compute dot product (not as distance)
 */
export function sql_vector_dot(v1: SqlValue, v2: SqlValue): number | null {
  const vec1 = extractVector(v1);
  const vec2 = extractVector(v2);

  if (vec1 === null || vec2 === null) return null;

  return vecDotProduct(vec1, vec2);
}

// =============================================================================
// FUNCTION REGISTRY DATA
// =============================================================================

/**
 * Vector functions for registry
 */
export const vectorFunctions: Record<string, SqlFunction> = {
  vector: {
    fn: (input: SqlValue) => vector(input),
    minArgs: 1,
    maxArgs: 1,
    deterministic: true,
  },
  vector_distance_cos: {
    fn: (v1: SqlValue, v2: SqlValue) => sql_vector_distance_cos(v1, v2),
    minArgs: 2,
    maxArgs: 2,
    deterministic: true,
  },
  vector_distance_l2: {
    fn: (v1: SqlValue, v2: SqlValue) => sql_vector_distance_l2(v1, v2),
    minArgs: 2,
    maxArgs: 2,
    deterministic: true,
  },
  vector_distance_dot: {
    fn: (v1: SqlValue, v2: SqlValue) => sql_vector_distance_dot(v1, v2),
    minArgs: 2,
    maxArgs: 2,
    deterministic: true,
  },
  vector_extract: {
    fn: (v: SqlValue) => vector_extract(v),
    minArgs: 1,
    maxArgs: 1,
    deterministic: true,
  },
  vector_dims: {
    fn: (v: SqlValue) => vector_dims(v),
    minArgs: 1,
    maxArgs: 1,
    deterministic: true,
  },
  vector_norm: {
    fn: (v: SqlValue) => sql_vector_norm(v),
    minArgs: 1,
    maxArgs: 1,
    deterministic: true,
  },
  vector_normalize: {
    fn: (v: SqlValue) => sql_vector_normalize(v),
    minArgs: 1,
    maxArgs: 1,
    deterministic: true,
  },
  vector_add: {
    fn: (v1: SqlValue, v2: SqlValue) => sql_vector_add(v1, v2),
    minArgs: 2,
    maxArgs: 2,
    deterministic: true,
  },
  vector_sub: {
    fn: (v1: SqlValue, v2: SqlValue) => sql_vector_sub(v1, v2),
    minArgs: 2,
    maxArgs: 2,
    deterministic: true,
  },
  vector_scale: {
    fn: (v: SqlValue, s: SqlValue) => sql_vector_scale(v, s),
    minArgs: 2,
    maxArgs: 2,
    deterministic: true,
  },
  vector_dot: {
    fn: (v1: SqlValue, v2: SqlValue) => sql_vector_dot(v1, v2),
    minArgs: 2,
    maxArgs: 2,
    deterministic: true,
  },
};

/**
 * Vector function signatures for documentation
 */
export const vectorSignatures: Record<string, FunctionSignature> = {
  vector: {
    name: 'vector',
    params: [{ name: 'array', type: 'any' }],
    returnType: 'bytes',
    description: 'Create a vector from an array of numbers',
  },
  vector_distance_cos: {
    name: 'vector_distance_cos',
    params: [
      { name: 'v1', type: 'bytes' },
      { name: 'v2', type: 'bytes' },
    ],
    returnType: 'number',
    description: 'Compute cosine distance between two vectors (0 = identical, 2 = opposite)',
  },
  vector_distance_l2: {
    name: 'vector_distance_l2',
    params: [
      { name: 'v1', type: 'bytes' },
      { name: 'v2', type: 'bytes' },
    ],
    returnType: 'number',
    description: 'Compute Euclidean (L2) distance between two vectors',
  },
  vector_distance_dot: {
    name: 'vector_distance_dot',
    params: [
      { name: 'v1', type: 'bytes' },
      { name: 'v2', type: 'bytes' },
    ],
    returnType: 'number',
    description: 'Compute dot product distance (negative dot product)',
  },
  vector_extract: {
    name: 'vector_extract',
    params: [{ name: 'v', type: 'bytes' }],
    returnType: 'string',
    description: 'Convert a vector to a JSON array string',
  },
  vector_dims: {
    name: 'vector_dims',
    params: [{ name: 'v', type: 'bytes' }],
    returnType: 'number',
    description: 'Get the number of dimensions in a vector',
  },
  vector_norm: {
    name: 'vector_norm',
    params: [{ name: 'v', type: 'bytes' }],
    returnType: 'number',
    description: 'Compute the L2 norm (magnitude) of a vector',
  },
  vector_normalize: {
    name: 'vector_normalize',
    params: [{ name: 'v', type: 'bytes' }],
    returnType: 'bytes',
    description: 'Normalize a vector to unit length',
  },
  vector_add: {
    name: 'vector_add',
    params: [
      { name: 'v1', type: 'bytes' },
      { name: 'v2', type: 'bytes' },
    ],
    returnType: 'bytes',
    description: 'Add two vectors element-wise',
  },
  vector_sub: {
    name: 'vector_sub',
    params: [
      { name: 'v1', type: 'bytes' },
      { name: 'v2', type: 'bytes' },
    ],
    returnType: 'bytes',
    description: 'Subtract two vectors element-wise',
  },
  vector_scale: {
    name: 'vector_scale',
    params: [
      { name: 'v', type: 'bytes' },
      { name: 'scalar', type: 'number' },
    ],
    returnType: 'bytes',
    description: 'Scale a vector by a scalar',
  },
  vector_dot: {
    name: 'vector_dot',
    params: [
      { name: 'v1', type: 'bytes' },
      { name: 'v2', type: 'bytes' },
    ],
    returnType: 'number',
    description: 'Compute dot product of two vectors',
  },
};
