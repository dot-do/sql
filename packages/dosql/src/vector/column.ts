/**
 * Vector Column Storage for DoSQL
 *
 * Provides columnar storage for vector data with:
 * - Efficient vector storage and retrieval
 * - Quantization support (F32 -> I8)
 * - Integration with B-tree for hybrid queries
 * - Batch operations for performance
 *
 * @module dosql/vector/column
 */

import {
  type Vector,
  type VectorInput,
  type VectorColumnDef,
  type VectorSearchResult,
  type QuantizationConfig,
  type QuantizationStats,
  VectorType,
  DistanceMetric,
  toVector,
  validateDimensions,
  vectorByteSize,
} from './types.js';
import { HnswIndex } from './hnsw.js';
import { getDistanceFunction, distanceToScore, type DistanceFunction } from './distance.js';

// =============================================================================
// VECTOR COLUMN STORAGE
// =============================================================================

/**
 * Storage options for vector column
 */
export interface VectorColumnOptions {
  /** Column definition */
  columnDef: VectorColumnDef;
  /** Enable HNSW index (default: true) */
  enableIndex?: boolean;
  /** HNSW M parameter */
  hnswM?: number;
  /** HNSW efConstruction parameter */
  hnswEfConstruction?: number;
  /** HNSW efSearch parameter */
  hnswEfSearch?: number;
  /** Enable quantization */
  quantization?: QuantizationConfig;
}

/**
 * Vector column storage class
 * Stores vectors in columnar format with optional HNSW index
 */
export class VectorColumn {
  private columnDef: VectorColumnDef;
  private vectors: Map<bigint, Vector> = new Map();
  private index: HnswIndex | null = null;
  private distanceFunc: DistanceFunction;
  private quantizationStats: QuantizationStats | null = null;
  private quantizedVectors: Map<bigint, Int8Array> | null = null;
  private readonly quantizationConfig: QuantizationConfig | null = null;

  constructor(options: VectorColumnOptions) {
    this.columnDef = options.columnDef;
    this.distanceFunc = getDistanceFunction(
      options.columnDef.distanceMetric ?? DistanceMetric.Cosine,
    );

    // Initialize HNSW index if enabled
    if (options.enableIndex !== false) {
      this.index = new HnswIndex({
        M: options.hnswM ?? 16,
        efConstruction: options.hnswEfConstruction ?? 200,
        efSearch: options.hnswEfSearch ?? 100,
        distanceMetric: options.columnDef.distanceMetric ?? DistanceMetric.Cosine,
      });
    }

    // Initialize quantization if configured
    if (options.quantization) {
      this.quantizationConfig = options.quantization;
      this.quantizedVectors = new Map();
    }
  }

  /**
   * Get the column definition
   */
  getColumnDef(): VectorColumnDef {
    return { ...this.columnDef };
  }

  /**
   * Get the number of vectors stored
   */
  get size(): number {
    return this.vectors.size;
  }

  /**
   * Get the dimensions of stored vectors
   */
  get dimensions(): number {
    return this.columnDef.dimensions;
  }

  /**
   * Insert or update a vector
   *
   * @param rowId Row identifier
   * @param vector Vector data
   */
  set(rowId: bigint, vector: VectorInput): void {
    const vec = toVector(vector);
    validateDimensions(vec, this.columnDef.dimensions);

    // Store the full-precision vector
    this.vectors.set(rowId, vec);

    // Update quantized storage if enabled
    if (this.quantizedVectors) {
      const quantized = this.quantize(vec);
      this.quantizedVectors.set(rowId, quantized);
    }

    // Update index if enabled
    if (this.index) {
      // Remove old entry if exists
      if (this.index.has(rowId)) {
        this.index.delete(rowId);
      }
      this.index.insert(rowId, vec);
    }
  }

  /**
   * Get a vector by row ID
   *
   * @param rowId Row identifier
   * @returns Vector or undefined
   */
  get(rowId: bigint): Vector | undefined {
    return this.vectors.get(rowId);
  }

  /**
   * Check if a row ID exists
   */
  has(rowId: bigint): boolean {
    return this.vectors.has(rowId);
  }

  /**
   * Delete a vector
   *
   * @param rowId Row identifier
   * @returns True if deleted
   */
  delete(rowId: bigint): boolean {
    const existed = this.vectors.delete(rowId);
    if (existed) {
      this.quantizedVectors?.delete(rowId);
      this.index?.delete(rowId);
    }
    return existed;
  }

  /**
   * Clear all vectors
   */
  clear(): void {
    this.vectors.clear();
    this.quantizedVectors?.clear();
    this.index?.clear();
    this.quantizationStats = null;
  }

  /**
   * Search for nearest neighbors using the index
   *
   * @param query Query vector
   * @param k Number of neighbors
   * @param efSearch Optional efSearch override
   * @param filter Optional filter function
   * @returns Search results
   */
  search(
    query: VectorInput,
    k: number,
    efSearch?: number,
    filter?: (rowId: bigint) => boolean,
  ): VectorSearchResult[] {
    const queryVec = toVector(query);
    validateDimensions(queryVec, this.columnDef.dimensions);

    // Use index if available
    if (this.index) {
      return this.index.search(queryVec, k, efSearch, filter);
    }

    // Fall back to brute force search
    return this.bruteForceSearch(queryVec, k, filter);
  }

  /**
   * Exact brute-force nearest neighbor search
   */
  private bruteForceSearch(
    query: Vector,
    k: number,
    filter?: (rowId: bigint) => boolean,
  ): VectorSearchResult[] {
    const results: VectorSearchResult[] = [];
    const metric = this.columnDef.distanceMetric ?? DistanceMetric.Cosine;

    for (const [rowId, vector] of this.vectors) {
      if (filter && !filter(rowId)) continue;

      const distance = this.distanceFunc(query, vector);
      results.push({
        rowId,
        distance,
        score: distanceToScore(distance, metric),
      });
    }

    // Sort by distance and return top k
    results.sort((a, b) => a.distance - b.distance);
    return results.slice(0, k);
  }

  /**
   * Compute distance between a query and a stored vector
   *
   * @param query Query vector
   * @param rowId Row ID of stored vector
   * @returns Distance or undefined if row not found
   */
  distance(query: VectorInput, rowId: bigint): number | undefined {
    const stored = this.vectors.get(rowId);
    if (!stored) return undefined;

    const queryVec = toVector(query);
    validateDimensions(queryVec, this.columnDef.dimensions);

    return this.distanceFunc(queryVec, stored);
  }

  // =============================================================================
  // QUANTIZATION
  // =============================================================================

  /**
   * Quantize a vector to Int8
   */
  private quantize(vector: Vector): Int8Array {
    const stats = this.getOrComputeQuantizationStats();
    const result = new Int8Array(vector.length);

    for (let i = 0; i < vector.length; i++) {
      // Map from [minVal, maxVal] to [-128, 127]
      const normalized = (vector[i] - stats.zeroPoint) * stats.scale;
      result[i] = Math.max(-128, Math.min(127, Math.round(normalized)));
    }

    return result;
  }

  /**
   * Dequantize an Int8 vector back to Float32
   */
  private dequantize(quantized: Int8Array): Vector {
    const stats = this.getOrComputeQuantizationStats();
    const result = new Float32Array(quantized.length);

    for (let i = 0; i < quantized.length; i++) {
      result[i] = quantized[i] / stats.scale + stats.zeroPoint;
    }

    return result;
  }

  /**
   * Get or compute quantization statistics
   */
  private getOrComputeQuantizationStats(): QuantizationStats {
    if (this.quantizationStats) {
      return this.quantizationStats;
    }

    // Use config values or compute from data
    let minVal = this.quantizationConfig?.minVal ?? Infinity;
    let maxVal = this.quantizationConfig?.maxVal ?? -Infinity;

    if (!isFinite(minVal) || !isFinite(maxVal)) {
      // Compute from actual data
      for (const vector of this.vectors.values()) {
        for (let i = 0; i < vector.length; i++) {
          minVal = Math.min(minVal, vector[i]);
          maxVal = Math.max(maxVal, vector[i]);
        }
      }
    }

    // Handle edge case of constant vectors
    if (minVal === maxVal) {
      maxVal = minVal + 1;
    }

    const range = maxVal - minVal;
    const scale = 255 / range;
    const zeroPoint = (minVal + maxVal) / 2;

    this.quantizationStats = { minVal, maxVal, scale, zeroPoint };
    return this.quantizationStats;
  }

  /**
   * Get quantized vector (for storage efficiency)
   */
  getQuantized(rowId: bigint): Int8Array | undefined {
    return this.quantizedVectors?.get(rowId);
  }

  /**
   * Recompute quantization for all vectors
   * Call after bulk inserts for optimal quantization
   */
  recomputeQuantization(): void {
    if (!this.quantizedVectors) return;

    // Reset stats to force recomputation
    this.quantizationStats = null;

    // Re-quantize all vectors
    for (const [rowId, vector] of this.vectors) {
      const quantized = this.quantize(vector);
      this.quantizedVectors.set(rowId, quantized);
    }
  }

  // =============================================================================
  // BATCH OPERATIONS
  // =============================================================================

  /**
   * Batch insert vectors
   *
   * @param entries Array of [rowId, vector] pairs
   */
  batchSet(entries: Array<[bigint, VectorInput]>): void {
    for (const [rowId, vector] of entries) {
      const vec = toVector(vector);
      validateDimensions(vec, this.columnDef.dimensions);
      this.vectors.set(rowId, vec);
    }

    // Build/rebuild index after batch insert
    if (this.index) {
      this.rebuildIndex();
    }

    // Recompute quantization after batch insert
    if (this.quantizedVectors) {
      this.recomputeQuantization();
    }
  }

  /**
   * Rebuild the HNSW index from scratch
   */
  rebuildIndex(): void {
    if (!this.index) return;

    const config = this.index.getConfig();
    this.index = new HnswIndex(config);

    for (const [rowId, vector] of this.vectors) {
      this.index.insert(rowId, vector);
    }
  }

  // =============================================================================
  // SERIALIZATION
  // =============================================================================

  /**
   * Serialize the column to binary format
   */
  serialize(): Uint8Array {
    // Calculate size
    const vectorSize = this.columnDef.dimensions * 4; // Float32
    const entrySize = 8 + vectorSize; // rowId(8) + vector
    const headerSize = 4 + 4 + 4; // dimensions(4) + count(4) + flags(4)
    const dataSize = headerSize + this.vectors.size * entrySize;

    // Add index size if present
    let indexData: Uint8Array | null = null;
    if (this.index) {
      indexData = this.index.serializeBinary();
    }

    const totalSize = dataSize + 4 + (indexData?.length ?? 0); // indexSize(4) + indexData

    const buffer = new ArrayBuffer(totalSize);
    const view = new DataView(buffer);
    let offset = 0;

    // Write header
    view.setUint32(offset, this.columnDef.dimensions, true); offset += 4;
    view.setUint32(offset, this.vectors.size, true); offset += 4;
    view.setUint32(offset, this.index ? 1 : 0, true); offset += 4; // flags: has index

    // Write vectors
    for (const [rowId, vector] of this.vectors) {
      view.setBigUint64(offset, rowId, true); offset += 8;
      for (let i = 0; i < vector.length; i++) {
        view.setFloat32(offset, vector[i], true);
        offset += 4;
      }
    }

    // Write index
    view.setUint32(offset, indexData?.length ?? 0, true); offset += 4;
    if (indexData) {
      new Uint8Array(buffer, offset).set(indexData);
    }

    return new Uint8Array(buffer);
  }

  /**
   * Deserialize a column from binary format
   */
  static deserialize(
    buffer: Uint8Array,
    columnDef: VectorColumnDef,
    options?: Partial<VectorColumnOptions>,
  ): VectorColumn {
    const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    let offset = 0;

    // Read header
    const dimensions = view.getUint32(offset, true); offset += 4;
    const count = view.getUint32(offset, true); offset += 4;
    const hasIndex = view.getUint32(offset, true) !== 0; offset += 4;

    // Create column
    const column = new VectorColumn({
      columnDef: { ...columnDef, dimensions },
      enableIndex: false, // We'll load the index separately
      ...options,
    });

    // Read vectors
    for (let i = 0; i < count; i++) {
      const rowId = view.getBigUint64(offset, true); offset += 8;
      const vector = new Float32Array(dimensions);
      for (let j = 0; j < dimensions; j++) {
        vector[j] = view.getFloat32(offset, true);
        offset += 4;
      }
      column.vectors.set(rowId, vector);
    }

    // Read index
    const indexSize = view.getUint32(offset, true); offset += 4;
    if (hasIndex && indexSize > 0) {
      const indexBuffer = buffer.slice(offset, offset + indexSize);
      column.index = HnswIndex.deserializeBinary(indexBuffer);
    }

    return column;
  }

  // =============================================================================
  // ITERATION
  // =============================================================================

  /**
   * Iterate over all vectors
   */
  *entries(): IterableIterator<[bigint, Vector]> {
    yield* this.vectors.entries();
  }

  /**
   * Iterate over all row IDs
   */
  *keys(): IterableIterator<bigint> {
    yield* this.vectors.keys();
  }

  /**
   * Iterate over all vectors
   */
  *values(): IterableIterator<Vector> {
    yield* this.vectors.values();
  }

  /**
   * Get all row IDs as array
   */
  getRowIds(): bigint[] {
    return Array.from(this.vectors.keys());
  }

  // =============================================================================
  // STATISTICS
  // =============================================================================

  /**
   * Get column statistics
   */
  getStats(): {
    rowCount: number;
    dimensions: number;
    memoryBytes: number;
    indexStats: ReturnType<HnswIndex['getStats']> | null;
    quantizationStats: QuantizationStats | null;
  } {
    const vectorBytes = this.vectors.size * vectorByteSize(this.dimensions, VectorType.F32);
    const quantizedBytes = this.quantizedVectors
      ? this.quantizedVectors.size * this.dimensions
      : 0;

    return {
      rowCount: this.vectors.size,
      dimensions: this.dimensions,
      memoryBytes: vectorBytes + quantizedBytes,
      indexStats: this.index?.getStats() ?? null,
      quantizationStats: this.quantizationStats,
    };
  }
}

// =============================================================================
// HYBRID QUERY SUPPORT
// =============================================================================

/**
 * Options for hybrid vector + scalar queries
 */
export interface HybridQueryOptions {
  /** Pre-filter row IDs (from B-tree query) */
  filterIds?: Set<bigint>;
  /** Post-filter function (applied after ANN search) */
  postFilter?: (rowId: bigint) => boolean;
  /** Number of results to return */
  k: number;
  /** Oversample factor for filtering (default: 2) */
  oversampleFactor?: number;
}

/**
 * Execute a hybrid vector search with scalar filtering
 *
 * @param column Vector column
 * @param query Query vector
 * @param options Hybrid query options
 * @returns Filtered search results
 */
export function hybridSearch(
  column: VectorColumn,
  query: VectorInput,
  options: HybridQueryOptions,
): VectorSearchResult[] {
  const { filterIds, postFilter, k, oversampleFactor = 2 } = options;

  // If we have a small filter set, use brute force on those
  if (filterIds && filterIds.size < k * 10) {
    return filterThenSearch(column, query, filterIds, k);
  }

  // Otherwise, oversample from ANN and filter
  const oversampled = column.search(
    query,
    k * oversampleFactor,
    undefined,
    filterIds ? (id) => filterIds.has(id) : undefined,
  );

  // Apply post-filter if provided
  let results = oversampled;
  if (postFilter) {
    results = results.filter((r) => postFilter(r.rowId));
  }

  return results.slice(0, k);
}

/**
 * Filter first, then exact search on filtered set
 */
function filterThenSearch(
  column: VectorColumn,
  query: VectorInput,
  filterIds: Set<bigint>,
  k: number,
): VectorSearchResult[] {
  const queryVec = toVector(query);
  const metric = column.getColumnDef().distanceMetric ?? DistanceMetric.Cosine;
  const distanceFunc = getDistanceFunction(metric);

  const results: VectorSearchResult[] = [];

  for (const rowId of filterIds) {
    const vector = column.get(rowId);
    if (!vector) continue;

    const distance = distanceFunc(queryVec, vector);
    results.push({
      rowId,
      distance,
      score: distanceToScore(distance, metric),
    });
  }

  results.sort((a, b) => a.distance - b.distance);
  return results.slice(0, k);
}
