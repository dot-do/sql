/**
 * HNSW (Hierarchical Navigable Small World) Index for DoSQL
 *
 * Implements approximate nearest neighbor search using the HNSW algorithm.
 * Reference: https://arxiv.org/abs/1603.09320
 *
 * Key features:
 * - Hierarchical graph structure for efficient search
 * - Configurable M, efConstruction, efSearch parameters
 * - Serialization/deserialization for persistence
 * - Support for multiple distance metrics
 *
 * @module dosql/vector/hnsw
 */

import {
  type Vector,
  type HnswConfig,
  type VectorSearchResult,
  DistanceMetric,
  DEFAULT_HNSW_CONFIG,
} from './types.js';
import { getDistanceFunction, distanceToScore, type DistanceFunction } from './distance.js';

// =============================================================================
// PRIORITY QUEUE IMPLEMENTATIONS
// =============================================================================

interface HeapItem {
  id: bigint;
  distance: number;
}

/**
 * Min-heap priority queue (smallest distance first)
 */
class MinHeap {
  private heap: HeapItem[] = [];

  get size(): number {
    return this.heap.length;
  }

  isEmpty(): boolean {
    return this.heap.length === 0;
  }

  push(item: HeapItem): void {
    this.heap.push(item);
    this.bubbleUp(this.heap.length - 1);
  }

  pop(): HeapItem | undefined {
    if (this.heap.length === 0) return undefined;
    const min = this.heap[0];
    const last = this.heap.pop();
    if (this.heap.length > 0 && last) {
      this.heap[0] = last;
      this.bubbleDown(0);
    }
    return min;
  }

  peek(): HeapItem | undefined {
    return this.heap[0];
  }

  private bubbleUp(index: number): void {
    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2);
      if (this.heap[parentIndex].distance <= this.heap[index].distance) break;
      [this.heap[parentIndex], this.heap[index]] = [this.heap[index], this.heap[parentIndex]];
      index = parentIndex;
    }
  }

  private bubbleDown(index: number): void {
    const length = this.heap.length;
    while (true) {
      const left = 2 * index + 1;
      const right = 2 * index + 2;
      let smallest = index;

      if (left < length && this.heap[left].distance < this.heap[smallest].distance) {
        smallest = left;
      }
      if (right < length && this.heap[right].distance < this.heap[smallest].distance) {
        smallest = right;
      }

      if (smallest === index) break;
      [this.heap[index], this.heap[smallest]] = [this.heap[smallest], this.heap[index]];
      index = smallest;
    }
  }
}

/**
 * Max-heap priority queue (largest distance first)
 * Used to maintain top-K candidates
 */
class MaxHeap {
  private heap: HeapItem[] = [];

  get size(): number {
    return this.heap.length;
  }

  isEmpty(): boolean {
    return this.heap.length === 0;
  }

  push(item: HeapItem): void {
    this.heap.push(item);
    this.bubbleUp(this.heap.length - 1);
  }

  pop(): HeapItem | undefined {
    if (this.heap.length === 0) return undefined;
    const max = this.heap[0];
    const last = this.heap.pop();
    if (this.heap.length > 0 && last) {
      this.heap[0] = last;
      this.bubbleDown(0);
    }
    return max;
  }

  peek(): HeapItem | undefined {
    return this.heap[0];
  }

  toArray(): HeapItem[] {
    return [...this.heap];
  }

  private bubbleUp(index: number): void {
    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2);
      if (this.heap[parentIndex].distance >= this.heap[index].distance) break;
      [this.heap[parentIndex], this.heap[index]] = [this.heap[index], this.heap[parentIndex]];
      index = parentIndex;
    }
  }

  private bubbleDown(index: number): void {
    const length = this.heap.length;
    while (true) {
      const left = 2 * index + 1;
      const right = 2 * index + 2;
      let largest = index;

      if (left < length && this.heap[left].distance > this.heap[largest].distance) {
        largest = left;
      }
      if (right < length && this.heap[right].distance > this.heap[largest].distance) {
        largest = right;
      }

      if (largest === index) break;
      [this.heap[index], this.heap[largest]] = [this.heap[largest], this.heap[index]];
      index = largest;
    }
  }
}

// =============================================================================
// HNSW NODE
// =============================================================================

/**
 * A node in the HNSW graph
 */
interface HnswNode {
  /** Unique node identifier (row ID) */
  id: bigint;
  /** The vector data */
  vector: Vector;
  /** Maximum layer this node exists in */
  level: number;
  /** Neighbors at each layer: neighbors[layer] = [neighbor_ids] */
  neighbors: bigint[][];
}

// =============================================================================
// SERIALIZATION TYPES
// =============================================================================

/**
 * Serialized index format for JSON
 */
interface SerializedHnswIndex {
  version: number;
  config: HnswConfig;
  dimensions: number;
  maxLevel: number;
  entryPointId: string | null;
  nodes: Array<{
    id: string;
    vector: number[];
    level: number;
    neighbors: string[][];
  }>;
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

function byteToMetric(byte: number): DistanceMetric {
  switch (byte) {
    case 0: return DistanceMetric.Cosine;
    case 1: return DistanceMetric.L2;
    case 2: return DistanceMetric.Dot;
    case 3: return DistanceMetric.Hamming;
    default: return DistanceMetric.Cosine;
  }
}

function metricToByte(metric: DistanceMetric): number {
  switch (metric) {
    case DistanceMetric.Cosine: return 0;
    case DistanceMetric.L2: return 1;
    case DistanceMetric.Dot: return 2;
    case DistanceMetric.Hamming: return 3;
    default: return 0;
  }
}

// =============================================================================
// HNSW INDEX
// =============================================================================

/**
 * HNSW index for approximate nearest neighbor search
 */
export class HnswIndex {
  private config: HnswConfig;
  private distanceFunc: DistanceFunction;
  private nodes: Map<bigint, HnswNode> = new Map();
  private entryPointId: bigint | null = null;
  private maxLevel: number = 0;
  private dimensions: number = 0;
  private levelMult: number;
  private rng: () => number;

  constructor(config: Partial<HnswConfig> = {}) {
    this.config = { ...DEFAULT_HNSW_CONFIG, ...config };
    this.distanceFunc = getDistanceFunction(this.config.distanceMetric);
    this.levelMult = 1 / Math.log(this.config.M);

    // Initialize RNG
    if (this.config.seed !== undefined) {
      this.rng = this.createSeededRng(this.config.seed);
    } else {
      this.rng = Math.random;
    }
  }

  /**
   * Get the number of vectors in the index
   */
  get size(): number {
    return this.nodes.size;
  }

  /**
   * Get the vector dimensions
   */
  get dim(): number {
    return this.dimensions;
  }

  /**
   * Get the configuration
   */
  getConfig(): HnswConfig {
    return { ...this.config };
  }

  /**
   * Create a seeded random number generator (simple LCG)
   */
  private createSeededRng(seed: number): () => number {
    let state = seed;
    return () => {
      state = (state * 1664525 + 1013904223) >>> 0;
      return state / 0xffffffff;
    };
  }

  /**
   * Generate a random level for a new node
   * Probability of level l = exp(-l/mL) where mL = 1/ln(M)
   */
  private randomLevel(): number {
    const r = this.rng();
    return Math.floor(-Math.log(r) * this.levelMult);
  }

  /**
   * Insert a vector into the index
   *
   * @param id Unique identifier for this vector (row ID)
   * @param vector The vector data
   */
  insert(id: bigint, vector: Vector): void {
    // Set dimensions from first vector
    if (this.dimensions === 0) {
      this.dimensions = vector.length;
    } else if (vector.length !== this.dimensions) {
      throw new Error(`Dimension mismatch: expected ${this.dimensions}, got ${vector.length}`);
    }

    // Check if ID already exists
    if (this.nodes.has(id)) {
      throw new Error(`Node with id ${id} already exists`);
    }

    // Generate random level for this node
    const level = this.randomLevel();

    // Create the node
    const node: HnswNode = {
      id,
      vector: new Float32Array(vector), // Copy the vector
      level,
      neighbors: Array.from({ length: level + 1 }, () => []),
    };

    // Handle first node
    if (this.entryPointId === null) {
      this.nodes.set(id, node);
      this.entryPointId = id;
      this.maxLevel = level;
      return;
    }

    // Find entry point
    let currId = this.entryPointId;

    // Phase 1: Traverse from top to insertion level + 1
    // Greedy search, no connections made
    for (let lc = this.maxLevel; lc > level; lc--) {
      currId = this.searchLayer(vector, currId, 1, lc)[0]?.id ?? currId;
    }

    // Phase 2: From insertion level down to 0
    // Search and connect at each level
    for (let lc = Math.min(level, this.maxLevel); lc >= 0; lc--) {
      // Find ef_construction nearest neighbors at this level
      const candidates = this.searchLayer(vector, currId, this.config.efConstruction, lc);

      // Select M best neighbors (simple heuristic: just take closest M)
      const maxNeighbors = lc === 0 ? this.config.M * 2 : this.config.M;
      const selectedNeighbors = candidates.slice(0, maxNeighbors);

      // Add bidirectional connections
      node.neighbors[lc] = selectedNeighbors.map((c) => c.id);

      for (const neighbor of selectedNeighbors) {
        const neighborNode = this.nodes.get(neighbor.id)!;

        // Extend neighbor's level array if needed
        while (neighborNode.neighbors.length <= lc) {
          neighborNode.neighbors.push([]);
        }

        // Add connection from neighbor to new node
        neighborNode.neighbors[lc].push(id);

        // Prune if too many connections
        if (neighborNode.neighbors[lc].length > maxNeighbors) {
          // Keep the closest neighbors
          const neighborVector = neighborNode.vector;
          neighborNode.neighbors[lc] = neighborNode.neighbors[lc]
            .map((nid) => {
              // The new node may not be in this.nodes yet, use its vector directly
              const nidVector = nid === id ? node.vector : this.nodes.get(nid)?.vector;
              return {
                id: nid,
                distance: nidVector ? this.distanceFunc(neighborVector, nidVector) : Infinity,
              };
            })
            .sort((a, b) => a.distance - b.distance)
            .slice(0, maxNeighbors)
            .map((n) => n.id);
        }
      }

      // Update entry point for next level
      if (selectedNeighbors.length > 0) {
        currId = selectedNeighbors[0].id;
      }
    }

    // Store the node
    this.nodes.set(id, node);

    // Update entry point if new node has higher level
    if (level > this.maxLevel) {
      this.entryPointId = id;
      this.maxLevel = level;
    }
  }

  /**
   * Search a single layer, returning ef closest neighbors
   */
  private searchLayer(
    query: Vector,
    entryId: bigint,
    ef: number,
    level: number,
  ): HeapItem[] {
    const visited = new Set<bigint>();
    const candidates = new MinHeap(); // Nodes to explore
    const results = new MaxHeap(); // Best results (max heap for efficient worst removal)

    const entryDist = this.distanceFunc(query, this.nodes.get(entryId)!.vector);
    candidates.push({ id: entryId, distance: entryDist });
    results.push({ id: entryId, distance: entryDist });
    visited.add(entryId);

    while (!candidates.isEmpty()) {
      const current = candidates.pop()!;

      // Early termination: if current is worse than worst result
      const worstResult = results.peek();
      if (worstResult && current.distance > worstResult.distance && results.size >= ef) {
        break;
      }

      // Explore neighbors
      const node = this.nodes.get(current.id);
      if (!node || !node.neighbors[level]) continue;

      for (const neighborId of node.neighbors[level]) {
        if (visited.has(neighborId)) continue;
        visited.add(neighborId);

        const neighborNode = this.nodes.get(neighborId);
        if (!neighborNode) continue;

        const dist = this.distanceFunc(query, neighborNode.vector);

        // Add to candidates for exploration
        candidates.push({ id: neighborId, distance: dist });

        // Add to results if better than worst
        const worstResult = results.peek();
        if (results.size < ef || (worstResult && dist < worstResult.distance)) {
          results.push({ id: neighborId, distance: dist });

          // Remove worst if too many
          if (results.size > ef) {
            results.pop();
          }
        }
      }
    }

    // Return sorted by distance (ascending)
    return results.toArray().sort((a, b) => a.distance - b.distance);
  }

  /**
   * Search for k nearest neighbors
   *
   * @param query Query vector
   * @param k Number of neighbors to return
   * @param efSearch Search expansion factor (optional, uses config default)
   * @param filter Optional filter function to exclude certain IDs
   * @returns Array of search results sorted by distance
   */
  search(
    query: Vector,
    k: number,
    efSearch?: number,
    filter?: (id: bigint) => boolean,
  ): VectorSearchResult[] {
    if (this.entryPointId === null || this.nodes.size === 0) {
      return [];
    }

    if (query.length !== this.dimensions) {
      throw new Error(`Query dimension mismatch: expected ${this.dimensions}, got ${query.length}`);
    }

    const ef = efSearch ?? this.config.efSearch;

    // Start from entry point
    let currId = this.entryPointId;

    // Phase 1: Greedy traverse from top level to level 1
    for (let lc = this.maxLevel; lc > 0; lc--) {
      const result = this.searchLayer(query, currId, 1, lc);
      if (result.length > 0) {
        currId = result[0].id;
      }
    }

    // Phase 2: Search at level 0 with full ef
    const candidates = this.searchLayer(query, currId, ef, 0);

    // Apply filter if provided
    let filtered = candidates;
    if (filter) {
      filtered = candidates.filter((c) => filter(c.id));
    }

    // Return top k
    return filtered.slice(0, k).map((item) => ({
      rowId: item.id,
      distance: item.distance,
      score: distanceToScore(item.distance, this.config.distanceMetric),
    }));
  }

  /**
   * Get a vector by ID
   */
  getVector(id: bigint): Vector | undefined {
    return this.nodes.get(id)?.vector;
  }

  /**
   * Check if an ID exists in the index
   */
  has(id: bigint): boolean {
    return this.nodes.has(id);
  }

  /**
   * Delete a vector from the index
   * Note: HNSW deletion is complex; this implementation removes connections
   * but may leave the graph in a suboptimal state
   */
  delete(id: bigint): boolean {
    const node = this.nodes.get(id);
    if (!node) return false;

    // Remove connections from neighbors
    for (let level = 0; level < node.neighbors.length; level++) {
      for (const neighborId of node.neighbors[level]) {
        const neighborNode = this.nodes.get(neighborId);
        if (neighborNode && neighborNode.neighbors[level]) {
          neighborNode.neighbors[level] = neighborNode.neighbors[level].filter(
            (nid) => nid !== id,
          );
        }
      }
    }

    // Remove the node
    this.nodes.delete(id);

    // Update entry point if needed
    if (id === this.entryPointId) {
      if (this.nodes.size === 0) {
        this.entryPointId = null;
        this.maxLevel = 0;
      } else {
        // Find a new entry point at the highest level
        let maxLevel = 0;
        let newEntry: bigint | null = null;
        for (const [nodeId, n] of this.nodes) {
          if (n.level >= maxLevel) {
            maxLevel = n.level;
            newEntry = nodeId;
          }
        }
        this.entryPointId = newEntry;
        this.maxLevel = maxLevel;
      }
    }

    return true;
  }

  /**
   * Clear the entire index
   */
  clear(): void {
    this.nodes.clear();
    this.entryPointId = null;
    this.maxLevel = 0;
    this.dimensions = 0;
  }

  // =============================================================================
  // SERIALIZATION
  // =============================================================================

  /**
   * Serialize the index to JSON-compatible format
   */
  serialize(): string {
    const data: SerializedHnswIndex = {
      version: 1,
      config: this.config,
      dimensions: this.dimensions,
      maxLevel: this.maxLevel,
      entryPointId: this.entryPointId?.toString() ?? null,
      nodes: Array.from(this.nodes.values()).map((node) => ({
        id: node.id.toString(),
        vector: Array.from(node.vector),
        level: node.level,
        neighbors: node.neighbors.map((level) => level.map((id) => id.toString())),
      })),
    };
    return JSON.stringify(data);
  }

  /**
   * Serialize the index to a compact binary format
   */
  serializeBinary(): Uint8Array {
    // Header: version(4) + dimensions(4) + maxLevel(4) + nodeCount(4) + entryPointId(8)
    // + config (M(4) + efConstruction(4) + efSearch(4) + metricType(1))
    const headerSize = 4 + 4 + 4 + 4 + 8 + 4 + 4 + 4 + 1;

    // Calculate total size
    let totalSize = headerSize;
    const nodeDataList: Array<{
      node: HnswNode;
      neighborCounts: number[];
    }> = [];

    for (const node of this.nodes.values()) {
      // id(8) + level(4) + vector(dimensions * 4) + neighbor_counts(level+1 * 4) + all_neighbors
      let nodeSize = 8 + 4 + this.dimensions * 4 + (node.level + 1) * 4;
      const neighborCounts: number[] = [];
      for (let l = 0; l <= node.level; l++) {
        const count = node.neighbors[l]?.length ?? 0;
        neighborCounts.push(count);
        nodeSize += count * 8; // each neighbor ID is 8 bytes
      }
      totalSize += nodeSize;
      nodeDataList.push({ node, neighborCounts });
    }

    const buffer = new ArrayBuffer(totalSize);
    const view = new DataView(buffer);
    let offset = 0;

    // Write header
    view.setUint32(offset, 1, true); offset += 4; // version
    view.setUint32(offset, this.dimensions, true); offset += 4;
    view.setUint32(offset, this.maxLevel, true); offset += 4;
    view.setUint32(offset, this.nodes.size, true); offset += 4;
    view.setBigUint64(offset, this.entryPointId ?? 0n, true); offset += 8;

    // Write config
    view.setUint32(offset, this.config.M, true); offset += 4;
    view.setUint32(offset, this.config.efConstruction, true); offset += 4;
    view.setUint32(offset, this.config.efSearch, true); offset += 4;
    view.setUint8(offset, metricToByte(this.config.distanceMetric)); offset += 1;

    // Write nodes
    for (const { node, neighborCounts } of nodeDataList) {
      view.setBigUint64(offset, node.id, true); offset += 8;
      view.setUint32(offset, node.level, true); offset += 4;

      // Write vector
      for (let i = 0; i < node.vector.length; i++) {
        view.setFloat32(offset, node.vector[i], true);
        offset += 4;
      }

      // Write neighbor counts and neighbors
      for (let l = 0; l <= node.level; l++) {
        view.setUint32(offset, neighborCounts[l], true);
        offset += 4;
        for (const neighborId of node.neighbors[l] ?? []) {
          view.setBigUint64(offset, neighborId, true);
          offset += 8;
        }
      }
    }

    return new Uint8Array(buffer);
  }

  /**
   * Deserialize an index from JSON format
   */
  static deserialize(json: string): HnswIndex {
    const data: SerializedHnswIndex = JSON.parse(json);

    if (data.version !== 1) {
      throw new Error(`Unsupported index version: ${data.version}`);
    }

    const index = new HnswIndex(data.config);
    (index as any).dimensions = data.dimensions;
    (index as any).maxLevel = data.maxLevel;
    (index as any).entryPointId = data.entryPointId ? BigInt(data.entryPointId) : null;

    for (const nodeData of data.nodes) {
      const node: HnswNode = {
        id: BigInt(nodeData.id),
        vector: new Float32Array(nodeData.vector),
        level: nodeData.level,
        neighbors: nodeData.neighbors.map((level: string[]) =>
          level.map((id: string) => BigInt(id)),
        ),
      };
      (index as any).nodes.set(node.id, node);
    }

    return index;
  }

  /**
   * Deserialize an index from binary format
   */
  static deserializeBinary(buffer: Uint8Array): HnswIndex {
    const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    let offset = 0;

    // Read header
    const version = view.getUint32(offset, true); offset += 4;
    if (version !== 1) {
      throw new Error(`Unsupported index version: ${version}`);
    }

    const dimensions = view.getUint32(offset, true); offset += 4;
    const maxLevel = view.getUint32(offset, true); offset += 4;
    const nodeCount = view.getUint32(offset, true); offset += 4;
    const entryPointId = view.getBigUint64(offset, true); offset += 8;

    // Read config
    const M = view.getUint32(offset, true); offset += 4;
    const efConstruction = view.getUint32(offset, true); offset += 4;
    const efSearch = view.getUint32(offset, true); offset += 4;
    const metricByte = view.getUint8(offset); offset += 1;

    const index = new HnswIndex({
      M,
      efConstruction,
      efSearch,
      distanceMetric: byteToMetric(metricByte),
    });
    (index as any).dimensions = dimensions;
    (index as any).maxLevel = maxLevel;
    (index as any).entryPointId = entryPointId === 0n ? null : entryPointId;

    // Read nodes
    for (let n = 0; n < nodeCount; n++) {
      const id = view.getBigUint64(offset, true); offset += 8;
      const level = view.getUint32(offset, true); offset += 4;

      // Read vector
      const vector = new Float32Array(dimensions);
      for (let i = 0; i < dimensions; i++) {
        vector[i] = view.getFloat32(offset, true);
        offset += 4;
      }

      // Read neighbors
      const neighbors: bigint[][] = [];
      for (let l = 0; l <= level; l++) {
        const count = view.getUint32(offset, true); offset += 4;
        const levelNeighbors: bigint[] = [];
        for (let c = 0; c < count; c++) {
          levelNeighbors.push(view.getBigUint64(offset, true));
          offset += 8;
        }
        neighbors.push(levelNeighbors);
      }

      const node: HnswNode = { id, vector, level, neighbors };
      (index as any).nodes.set(id, node);
    }

    return index;
  }

  /**
   * Get index statistics
   */
  getStats(): {
    nodeCount: number;
    dimensions: number;
    maxLevel: number;
    avgConnectionsLevel0: number;
    config: HnswConfig;
  } {
    let totalConnections = 0;
    for (const node of this.nodes.values()) {
      totalConnections += node.neighbors[0]?.length ?? 0;
    }

    return {
      nodeCount: this.nodes.size,
      dimensions: this.dimensions,
      maxLevel: this.maxLevel,
      avgConnectionsLevel0: this.nodes.size > 0 ? totalConnections / this.nodes.size : 0,
      config: { ...this.config },
    };
  }
}
