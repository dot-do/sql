/**
 * DoSQL Vindex (Virtual Index) Implementations
 *
 * Vindex determines how data is distributed across shards.
 * Implementations:
 * - Hash vindex: FNV-1a for uniform distribution
 * - Consistent hash vindex: Virtual nodes for seamless rebalancing
 * - Range vindex: Boundary-based partitioning
 *
 * @packageDocumentation
 */

import type {
  VindexConfig,
  HashVindexConfig,
  ConsistentHashVindexConfig,
  RangeVindexConfig,
  RangeBoundary,
  ShardConfig,
} from './types.js';
import { assertNever } from '../utils/assert-never.js';
import { fnv1a, xxhash, getHashFunction } from '../utils/hash.js';

// Re-export hash functions for backward compatibility
export { fnv1a, xxhash, getHashFunction };

// =============================================================================
// VINDEX INTERFACE
// =============================================================================

/**
 * Vindex interface - virtual index for shard routing
 */
export interface Vindex {
  /** Get shard ID for a given key */
  getShard(key: unknown): string;
  /** Get all shard IDs that might contain data for given keys (for IN queries) */
  getShardsForKeys(keys: unknown[]): string[];
  /** Get all shard IDs (for scatter queries) */
  getAllShards(): string[];
  /** Get shards for a range query (min <= key < max) */
  getShardsForRange(min: unknown, max: unknown): string[];
}

// =============================================================================
// HASH VINDEX
// =============================================================================

/**
 * Hash-based vindex implementation
 * Uses modulo arithmetic for uniform distribution
 */
export class HashVindex implements Vindex {
  private readonly shardIds: string[];
  private readonly hashFn: (input: string | number | bigint) => number;

  constructor(shards: ShardConfig[], config?: HashVindexConfig) {
    this.shardIds = shards.map(s => s.id);
    if (this.shardIds.length === 0) {
      throw new Error('HashVindex requires at least one shard');
    }
    this.hashFn = getHashFunction(config?.algorithm ?? 'fnv1a');
  }

  getShard(key: unknown): string {
    const hash = this.hashFn(this.normalizeKey(key));
    const index = hash % this.shardIds.length;
    return this.shardIds[index];
  }

  getShardsForKeys(keys: unknown[]): string[] {
    const shardSet = new Set<string>();
    for (const key of keys) {
      shardSet.add(this.getShard(key));
    }
    return Array.from(shardSet);
  }

  getAllShards(): string[] {
    return [...this.shardIds];
  }

  getShardsForRange(_min: unknown, _max: unknown): string[] {
    // Hash vindex cannot optimize range queries - must scatter
    return this.getAllShards();
  }

  private normalizeKey(key: unknown): string | number | bigint {
    if (typeof key === 'string' || typeof key === 'number' || typeof key === 'bigint') {
      return key;
    }
    return JSON.stringify(key);
  }
}

// =============================================================================
// CONSISTENT HASH VINDEX
// =============================================================================

/**
 * Virtual node entry in the consistent hash ring
 */
interface VirtualNode {
  hash: number;
  shardId: string;
}

/**
 * Consistent hash vindex implementation
 * Uses virtual nodes for better distribution and seamless rebalancing
 */
export class ConsistentHashVindex implements Vindex {
  private readonly shardIds: string[];
  private readonly ring: VirtualNode[];
  private readonly hashFn: (input: string | number | bigint) => number;
  private readonly virtualNodes: number;

  constructor(shards: ShardConfig[], config?: ConsistentHashVindexConfig) {
    this.shardIds = shards.map(s => s.id);
    if (this.shardIds.length === 0) {
      throw new Error('ConsistentHashVindex requires at least one shard');
    }

    this.virtualNodes = config?.virtualNodes ?? 150;
    this.hashFn = getHashFunction(config?.algorithm ?? 'fnv1a');

    // Build the ring with virtual nodes
    this.ring = this.buildRing();
  }

  private buildRing(): VirtualNode[] {
    const nodes: VirtualNode[] = [];

    for (const shardId of this.shardIds) {
      for (let i = 0; i < this.virtualNodes; i++) {
        const virtualKey = `${shardId}#${i}`;
        nodes.push({
          hash: this.hashFn(virtualKey),
          shardId,
        });
      }
    }

    // Sort by hash for binary search
    nodes.sort((a, b) => a.hash - b.hash);
    return nodes;
  }

  getShard(key: unknown): string {
    const hash = this.hashFn(this.normalizeKey(key));
    return this.findShard(hash);
  }

  private findShard(hash: number): string {
    // Binary search for first node with hash >= target
    let left = 0;
    let right = this.ring.length;

    while (left < right) {
      const mid = (left + right) >>> 1;
      if (this.ring[mid].hash < hash) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    // Wrap around if we've gone past the end
    const index = left >= this.ring.length ? 0 : left;
    return this.ring[index].shardId;
  }

  getShardsForKeys(keys: unknown[]): string[] {
    const shardSet = new Set<string>();
    for (const key of keys) {
      shardSet.add(this.getShard(key));
    }
    return Array.from(shardSet);
  }

  getAllShards(): string[] {
    return [...this.shardIds];
  }

  getShardsForRange(_min: unknown, _max: unknown): string[] {
    // Consistent hash cannot optimize range queries - must scatter
    return this.getAllShards();
  }

  private normalizeKey(key: unknown): string | number | bigint {
    if (typeof key === 'string' || typeof key === 'number' || typeof key === 'bigint') {
      return key;
    }
    return JSON.stringify(key);
  }

  /**
   * Get the ring state for debugging/monitoring
   */
  getRingState(): { totalNodes: number; nodesPerShard: Map<string, number> } {
    const nodesPerShard = new Map<string, number>();
    for (const node of this.ring) {
      nodesPerShard.set(node.shardId, (nodesPerShard.get(node.shardId) ?? 0) + 1);
    }
    return {
      totalNodes: this.ring.length,
      nodesPerShard,
    };
  }
}

// =============================================================================
// RANGE VINDEX
// =============================================================================

/**
 * Range-based vindex implementation
 * Routes based on boundary definitions
 */
export class RangeVindex<T extends string | number | bigint | Date> implements Vindex {
  private readonly boundaries: RangeBoundary<T>[];
  private readonly allShardIds: string[];

  constructor(shards: ShardConfig[], config: RangeVindexConfig<T>) {
    if (!config.boundaries || config.boundaries.length === 0) {
      throw new Error('RangeVindex requires at least one boundary');
    }

    this.boundaries = [...config.boundaries];
    this.allShardIds = shards.map(s => s.id);

    // Validate that all boundary shards exist
    const shardIdSet = new Set(this.allShardIds);
    for (const boundary of this.boundaries) {
      if (!shardIdSet.has(boundary.shard)) {
        throw new Error(`Boundary references unknown shard: ${boundary.shard}`);
      }
    }

    // Sort boundaries by min value
    this.boundaries.sort((a, b) => this.compare(a.min, b.min));
  }

  getShard(key: unknown): string {
    const typedKey = key as T;

    // Find the boundary that contains this key
    for (const boundary of this.boundaries) {
      const afterMin = this.compare(typedKey, boundary.min) >= 0;
      const beforeMax = boundary.max === null || this.compare(typedKey, boundary.max) < 0;

      if (afterMin && beforeMax) {
        return boundary.shard;
      }
    }

    // If no boundary matches, use the last boundary (unbounded)
    // This handles values beyond all defined ranges
    if (this.boundaries.length === 0) {
      throw new Error('Range vindex has no boundaries configured');
    }
    return this.boundaries[this.boundaries.length - 1].shard;
  }

  getShardsForKeys(keys: unknown[]): string[] {
    const shardSet = new Set<string>();
    for (const key of keys) {
      shardSet.add(this.getShard(key));
    }
    return Array.from(shardSet);
  }

  getAllShards(): string[] {
    return [...this.allShardIds];
  }

  getShardsForRange(min: unknown, max: unknown): string[] {
    const typedMin = min as T;
    const typedMax = max as T;
    const shardSet = new Set<string>();

    for (const boundary of this.boundaries) {
      // Check if the query range overlaps with this boundary
      const boundaryEnd = boundary.max;

      // Query range: [typedMin, typedMax)
      // Boundary range: [boundary.min, boundary.max)

      // Overlap exists if:
      // - Query starts before boundary ends (or boundary is unbounded)
      // - Query ends after boundary starts

      const queryStartsBeforeBoundaryEnds =
        boundaryEnd === null || this.compare(typedMin, boundaryEnd) < 0;
      const queryEndsAfterBoundaryStarts =
        this.compare(typedMax, boundary.min) > 0;

      if (queryStartsBeforeBoundaryEnds && queryEndsAfterBoundaryStarts) {
        shardSet.add(boundary.shard);
      }
    }

    return Array.from(shardSet);
  }

  private compare(a: T, b: T): number {
    if (typeof a === 'number' && typeof b === 'number') {
      return a - b;
    }
    if (typeof a === 'bigint' && typeof b === 'bigint') {
      return a < b ? -1 : a > b ? 1 : 0;
    }
    if (typeof a === 'string' && typeof b === 'string') {
      return a.localeCompare(b);
    }
    if (a instanceof Date && b instanceof Date) {
      return a.getTime() - b.getTime();
    }

    // Fallback: convert to string and compare
    const strA = String(a);
    const strB = String(b);
    return strA.localeCompare(strB);
  }

  /**
   * Get boundary information for debugging
   */
  getBoundaries(): readonly RangeBoundary<T>[] {
    return this.boundaries;
  }
}

// =============================================================================
// VINDEX FACTORY
// =============================================================================

/**
 * Create a vindex instance from configuration
 */
export function createVindex(shards: ShardConfig[], config: VindexConfig): Vindex {
  switch (config.type) {
    case 'hash':
      return new HashVindex(shards, config);
    case 'consistent-hash':
      return new ConsistentHashVindex(shards, config);
    case 'range':
      return new RangeVindex(shards, config);
    default:
      return assertNever(config, `Unknown vindex type: ${(config as VindexConfig).type}`);
  }
}

// =============================================================================
// VINDEX UTILITIES
// =============================================================================

/**
 * Test distribution of keys across shards
 * Useful for validating vindex configuration
 */
export function testDistribution(
  vindex: Vindex,
  keyGenerator: () => unknown,
  sampleSize: number = 10000
): Map<string, number> {
  const distribution = new Map<string, number>();

  for (const shardId of vindex.getAllShards()) {
    distribution.set(shardId, 0);
  }

  for (let i = 0; i < sampleSize; i++) {
    const key = keyGenerator();
    const shardId = vindex.getShard(key);
    distribution.set(shardId, (distribution.get(shardId) ?? 0) + 1);
  }

  return distribution;
}

/**
 * Calculate distribution statistics
 */
export function distributionStats(distribution: Map<string, number>): {
  min: number;
  max: number;
  mean: number;
  stdDev: number;
  skew: number;
} {
  const values = Array.from(distribution.values());
  const n = values.length;

  if (n === 0) {
    return { min: 0, max: 0, mean: 0, stdDev: 0, skew: 0 };
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const mean = values.reduce((a, b) => a + b, 0) / n;

  const variance = values.reduce((sum, v) => sum + Math.pow(v - mean, 2), 0) / n;
  const stdDev = Math.sqrt(variance);

  // Skewness: measures asymmetry of distribution
  const skew = stdDev === 0 ? 0 :
    values.reduce((sum, v) => sum + Math.pow((v - mean) / stdDev, 3), 0) / n;

  return { min, max, mean, stdDev, skew };
}
