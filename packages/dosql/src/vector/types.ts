/**
 * Vector Types for DoSQL
 *
 * Defines vector data types, column configurations, and index parameters
 * for native vector search support (Turso-style).
 *
 * @module @dotdo/dosql/vector/types
 */

// =============================================================================
// VECTOR DATA TYPES
// =============================================================================

/**
 * Supported vector element types
 * Following common ML/AI vector formats
 */
export enum VectorType {
  /** 64-bit floating point (highest precision) */
  F64 = 'f64',
  /** 32-bit floating point (standard ML vectors) */
  F32 = 'f32',
  /** 16-bit floating point (reduced precision) */
  F16 = 'f16',
  /** Brain floating point 16-bit (optimized for neural networks) */
  BF16 = 'bf16',
  /** 8-bit signed integer (quantized vectors) */
  I8 = 'i8',
  /** 1-bit binary vectors (for binary embeddings) */
  F1BIT = 'f1bit',
}

/**
 * Distance metric types for vector similarity
 */
export enum DistanceMetric {
  /** Cosine similarity (1 - cosine_similarity) */
  Cosine = 'cosine',
  /** Euclidean distance (L2 norm) */
  L2 = 'l2',
  /** Dot product (negative for similarity) */
  Dot = 'dot',
  /** Hamming distance (for binary vectors) */
  Hamming = 'hamming',
}

// =============================================================================
// VECTOR COLUMN DEFINITION
// =============================================================================

/**
 * Vector column configuration
 */
export interface VectorColumnDef {
  /** Column name */
  name: string;
  /** Number of dimensions in the vector */
  dimensions: number;
  /** Element data type */
  type: VectorType;
  /** Whether the column can be null */
  nullable?: boolean;
  /** Distance metric for similarity queries */
  distanceMetric?: DistanceMetric;
}

/**
 * Extended column definition for vector columns
 */
export interface VectorTableColumn {
  name: string;
  type: 'vector';
  vectorDef: VectorColumnDef;
}

// =============================================================================
// HNSW INDEX CONFIGURATION
// =============================================================================

/**
 * HNSW (Hierarchical Navigable Small World) index parameters
 */
export interface HnswConfig {
  /**
   * Maximum number of connections per node per layer
   * Higher values = better recall but more memory/slower build
   * Typical values: 8-64, default: 16
   */
  M: number;

  /**
   * Size of the dynamic candidate list during index construction
   * Higher values = better recall but slower build
   * Typical values: 64-512, default: 200
   */
  efConstruction: number;

  /**
   * Size of the dynamic candidate list during search
   * Higher values = better recall but slower search
   * Typical values: 50-500, default: 100
   */
  efSearch: number;

  /**
   * Distance metric for the index
   */
  distanceMetric: DistanceMetric;

  /**
   * Maximum number of layers in the graph
   * Usually computed as log(n) but can be specified
   */
  maxLevel?: number;

  /**
   * Random seed for reproducible index builds
   */
  seed?: number;
}

/**
 * Default HNSW configuration
 */
export const DEFAULT_HNSW_CONFIG: HnswConfig = {
  M: 16,
  efConstruction: 200,
  efSearch: 100,
  distanceMetric: DistanceMetric.Cosine,
};

// =============================================================================
// VECTOR INDEX DEFINITION
// =============================================================================

/**
 * Index types supported for vector columns
 */
export enum VectorIndexType {
  /** HNSW index (default, best for most use cases) */
  HNSW = 'hnsw',
  /** Flat index (exact search, small datasets) */
  Flat = 'flat',
  /** IVF index (inverted file, large datasets) */
  IVF = 'ivf',
}

/**
 * Vector index definition
 */
export interface VectorIndexDef {
  /** Index name */
  name: string;
  /** Table the index belongs to */
  table: string;
  /** Column being indexed */
  column: string;
  /** Index type */
  indexType: VectorIndexType;
  /** HNSW-specific configuration (if indexType is HNSW) */
  hnswConfig?: HnswConfig;
}

// =============================================================================
// VECTOR SEARCH TYPES
// =============================================================================

/**
 * Vector search query options
 */
export interface VectorSearchOptions {
  /** Number of nearest neighbors to return */
  k: number;
  /** efSearch parameter for HNSW (overrides index default) */
  efSearch?: number;
  /** Pre-filter row IDs to consider */
  filterIds?: Set<bigint>;
  /** Post-filter predicate function */
  postFilter?: (rowId: bigint) => boolean;
  /** Include distances in results */
  includeDistances?: boolean;
}

/**
 * Vector search result
 */
export interface VectorSearchResult {
  /** Row ID of the match */
  rowId: bigint;
  /** Distance to query vector */
  distance: number;
  /** Similarity score (converted from distance) */
  score: number;
}

// =============================================================================
// VECTOR VALUE TYPES
// =============================================================================

/**
 * Runtime vector representation
 * Uses Float32Array as the canonical internal format
 */
export type Vector = Float32Array;

/**
 * Input vector type (can be array or typed array)
 */
export type VectorInput = number[] | Float32Array | Float64Array | Int8Array;

/**
 * Serialized vector format for storage
 */
export interface SerializedVector {
  /** Vector dimensions */
  dimensions: number;
  /** Element type */
  type: VectorType;
  /** Raw bytes */
  data: Uint8Array;
}

// =============================================================================
// QUANTIZATION TYPES
// =============================================================================

/**
 * Quantization configuration for vector compression
 */
export interface QuantizationConfig {
  /** Target type after quantization */
  targetType: VectorType.I8 | VectorType.F1BIT;
  /** Minimum value in the original distribution */
  minVal?: number;
  /** Maximum value in the original distribution */
  maxVal?: number;
  /** Number of bits for binary quantization */
  bits?: number;
}

/**
 * Quantization statistics for a vector column
 */
export interface QuantizationStats {
  /** Minimum value across all vectors */
  minVal: number;
  /** Maximum value across all vectors */
  maxVal: number;
  /** Scale factor for dequantization */
  scale: number;
  /** Zero point for asymmetric quantization */
  zeroPoint: number;
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Get the byte size of a vector element type
 */
export function vectorTypeByteSize(type: VectorType): number {
  switch (type) {
    case VectorType.F64:
      return 8;
    case VectorType.F32:
      return 4;
    case VectorType.F16:
    case VectorType.BF16:
      return 2;
    case VectorType.I8:
      return 1;
    case VectorType.F1BIT:
      return 0.125; // 1 bit = 1/8 byte
    default:
      throw new Error(`Unknown vector type: ${type}`);
  }
}

/**
 * Calculate the storage size of a vector in bytes
 */
export function vectorByteSize(dimensions: number, type: VectorType): number {
  if (type === VectorType.F1BIT) {
    return Math.ceil(dimensions / 8);
  }
  return dimensions * vectorTypeByteSize(type);
}

/**
 * Convert input to Vector (Float32Array)
 */
export function toVector(input: VectorInput): Vector {
  if (input instanceof Float32Array) {
    return input;
  }
  return new Float32Array(input);
}

/**
 * Validate vector dimensions match expected
 */
export function validateDimensions(vector: Vector, expected: number): void {
  if (vector.length !== expected) {
    throw new Error(`Vector dimension mismatch: expected ${expected}, got ${vector.length}`);
  }
}

/**
 * Check if a value is a valid vector input
 */
export function isVectorInput(value: unknown): value is VectorInput {
  return (
    Array.isArray(value) ||
    value instanceof Float32Array ||
    value instanceof Float64Array ||
    value instanceof Int8Array
  );
}
