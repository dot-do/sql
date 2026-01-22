/**
 * Vector Search Module for DoSQL
 *
 * Native vector search support (Turso-style) for DoSQL including:
 * - Vector types and column definitions
 * - Distance functions (cosine, L2, dot product)
 * - HNSW index for approximate nearest neighbor search
 * - Vector column storage with quantization support
 *
 * @example
 * ```typescript
 * import { HnswIndex, VectorColumn, vector_distance_cos } from '@dotdo/dosql/vector';
 *
 * // Create an HNSW index
 * const index = new HnswIndex({ M: 16, efConstruction: 200 });
 * index.insert(1n, new Float32Array([0.1, 0.2, 0.3]));
 *
 * // Search for nearest neighbors
 * const results = index.search(new Float32Array([0.15, 0.25, 0.35]), 10);
 * ```
 *
 * @module @dotdo/dosql/vector
 */

// =============================================================================
// TYPES
// =============================================================================

export {
  // Enums
  VectorType,
  DistanceMetric,
  VectorIndexType,

  // Vector column types
  type VectorColumnDef,
  type VectorTableColumn,

  // HNSW config types
  type HnswConfig,
  DEFAULT_HNSW_CONFIG,

  // Index definition
  type VectorIndexDef,

  // Search types
  type VectorSearchOptions,
  type VectorSearchResult,

  // Value types
  type Vector,
  type VectorInput,
  type SerializedVector,

  // Quantization types
  type QuantizationConfig,
  type QuantizationStats,

  // Helper functions
  vectorTypeByteSize,
  vectorByteSize,
  toVector,
  validateDimensions,
  isVectorInput,
} from './types.js';

// =============================================================================
// DISTANCE FUNCTIONS
// =============================================================================

export {
  // Primary distance functions
  vector_distance_cos,
  vector_distance_l2,
  vector_distance_l2_squared,
  vector_distance_dot,
  vector_distance_hamming,
  vector_distance_hamming_packed,

  // Similarity functions
  vector_similarity_cos,

  // Vector operations
  vector_norm,
  vector_normalize,
  vector_add,
  vector_sub,
  vector_scale,
  vector_dot_product,

  // Factory
  getDistanceFunction,
  distanceToScore,
  type DistanceFunction,
} from './distance.js';

// =============================================================================
// HNSW INDEX
// =============================================================================

export { HnswIndex } from './hnsw.js';

// =============================================================================
// VECTOR COLUMN STORAGE
// =============================================================================

export {
  VectorColumn,
  type VectorColumnOptions,
  type HybridQueryOptions,
  hybridSearch,
} from './column.js';
