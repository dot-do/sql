/**
 * Vindex (Virtual Index) Unit Tests
 *
 * Tests for the sharding vindex implementations:
 * - HashVindex: FNV-1a and xxhash for uniform distribution
 * - ConsistentHashVindex: Virtual nodes for seamless rebalancing
 * - RangeVindex: Boundary-based partitioning for range queries
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach } from 'vitest';

import {
  fnv1a,
  xxhash,
  getHashFunction,
  HashVindex,
  ConsistentHashVindex,
  RangeVindex,
  createVindex,
  testDistribution,
  distributionStats,
  type Vindex,
} from '../vindex.js';

import {
  createShardId,
  shard,
  hashVindex,
  consistentHashVindex,
  rangeVindex,
  type ShardConfig,
  type RangeBoundary,
  type ShardId,
} from '../types.js';

// =============================================================================
// TEST FIXTURES
// =============================================================================

function createTestShards(count: number = 3): ShardConfig[] {
  return Array.from({ length: count }, (_, i) =>
    shard(createShardId(`shard-${i + 1}`), `do-ns-${i + 1}`)
  );
}

// =============================================================================
// HASH FUNCTION TESTS
// =============================================================================

describe('Hash Functions', () => {
  describe('fnv1a', () => {
    it('should produce consistent hashes for the same input', () => {
      const hash1 = fnv1a('test-key');
      const hash2 = fnv1a('test-key');
      expect(hash1).toBe(hash2);
    });

    it('should produce different hashes for different inputs', () => {
      const hash1 = fnv1a('key-1');
      const hash2 = fnv1a('key-2');
      expect(hash1).not.toBe(hash2);
    });

    it('should handle numeric inputs', () => {
      const hash1 = fnv1a(123);
      const hash2 = fnv1a('123');
      expect(hash1).toBe(hash2); // Numbers are stringified
    });

    it('should handle bigint inputs', () => {
      const hash1 = fnv1a(123n);
      const hash2 = fnv1a('123');
      expect(hash1).toBe(hash2); // Bigints are stringified
    });

    it('should return positive 32-bit integers', () => {
      const testInputs = ['a', 'test', '12345', 'very-long-string-with-many-characters'];
      for (const input of testInputs) {
        const hash = fnv1a(input);
        expect(hash).toBeGreaterThanOrEqual(0);
        expect(hash).toBeLessThanOrEqual(0xFFFFFFFF);
      }
    });

    it('should handle empty string', () => {
      const hash = fnv1a('');
      expect(hash).toBeGreaterThanOrEqual(0);
    });

    it('should handle unicode characters', () => {
      const hash1 = fnv1a('\u4e2d\u6587');
      const hash2 = fnv1a('\u4e2d\u6587');
      expect(hash1).toBe(hash2);
    });
  });

  describe('xxhash', () => {
    it('should produce consistent hashes for the same input', () => {
      const hash1 = xxhash('test-key');
      const hash2 = xxhash('test-key');
      expect(hash1).toBe(hash2);
    });

    it('should produce different hashes for different inputs', () => {
      const hash1 = xxhash('key-1');
      const hash2 = xxhash('key-2');
      expect(hash1).not.toBe(hash2);
    });

    it('should handle long strings (>= 16 chars)', () => {
      const longString = 'this-is-a-very-long-string-for-testing-xxhash';
      const hash = xxhash(longString);
      expect(hash).toBeGreaterThanOrEqual(0);
      expect(hash).toBeLessThanOrEqual(0xFFFFFFFF);
    });

    it('should handle short strings (< 16 chars)', () => {
      const shortString = 'short';
      const hash = xxhash(shortString);
      expect(hash).toBeGreaterThanOrEqual(0);
    });

    it('should return positive 32-bit integers', () => {
      const testInputs = ['a', 'test', '12345', 'very-long-string-with-many-characters'];
      for (const input of testInputs) {
        const hash = xxhash(input);
        expect(hash).toBeGreaterThanOrEqual(0);
        expect(hash).toBeLessThanOrEqual(0xFFFFFFFF);
      }
    });
  });

  describe('getHashFunction', () => {
    it('should return fnv1a by default', () => {
      const fn = getHashFunction('fnv1a');
      expect(fn('test')).toBe(fnv1a('test'));
    });

    it('should return xxhash when specified', () => {
      const fn = getHashFunction('xxhash');
      expect(fn('test')).toBe(xxhash('test'));
    });
  });
});

// =============================================================================
// HASH VINDEX TESTS
// =============================================================================

describe('HashVindex', () => {
  let shards: ShardConfig[];

  beforeEach(() => {
    shards = createTestShards(4);
  });

  describe('constructor', () => {
    it('should create vindex with default algorithm (fnv1a)', () => {
      const vindex = new HashVindex(shards);
      expect(vindex).toBeInstanceOf(HashVindex);
    });

    it('should create vindex with xxhash algorithm', () => {
      const vindex = new HashVindex(shards, { type: 'hash', algorithm: 'xxhash' });
      expect(vindex).toBeInstanceOf(HashVindex);
    });

    it('should throw if no shards provided', () => {
      expect(() => new HashVindex([])).toThrow('HashVindex requires at least one shard');
    });
  });

  describe('getShard', () => {
    it('should return consistent shard for same key', () => {
      const vindex = new HashVindex(shards);
      const shard1 = vindex.getShard('user-123');
      const shard2 = vindex.getShard('user-123');
      expect(shard1).toBe(shard2);
    });

    it('should handle string keys', () => {
      const vindex = new HashVindex(shards);
      const shardId = vindex.getShard('test-key');
      expect(shards.map(s => s.id)).toContain(shardId);
    });

    it('should handle numeric keys', () => {
      const vindex = new HashVindex(shards);
      const shardId = vindex.getShard(12345);
      expect(shards.map(s => s.id)).toContain(shardId);
    });

    it('should handle bigint keys', () => {
      const vindex = new HashVindex(shards);
      const shardId = vindex.getShard(123456789012345678901234567890n);
      expect(shards.map(s => s.id)).toContain(shardId);
    });

    it('should handle object keys (JSON serialized)', () => {
      const vindex = new HashVindex(shards);
      const shardId = vindex.getShard({ id: 123, name: 'test' });
      expect(shards.map(s => s.id)).toContain(shardId);
    });

    it('should distribute keys across all shards', () => {
      const vindex = new HashVindex(shards);
      const shardsUsed = new Set<string>();

      // Generate enough keys to hit all shards
      for (let i = 0; i < 1000; i++) {
        shardsUsed.add(vindex.getShard(`key-${i}`));
      }

      expect(shardsUsed.size).toBe(shards.length);
    });
  });

  describe('getShardsForKeys', () => {
    it('should return unique shards for multiple keys', () => {
      const vindex = new HashVindex(shards);
      const keys = ['key-1', 'key-2', 'key-3'];
      const result = vindex.getShardsForKeys(keys);

      // Result should be a deduplicated list
      expect(new Set(result).size).toBe(result.length);
    });

    it('should return single shard if all keys hash to same shard', () => {
      // Use single shard to ensure same result
      const singleShard = createTestShards(1);
      const vindex = new HashVindex(singleShard);
      const keys = ['key-1', 'key-2', 'key-3'];
      const result = vindex.getShardsForKeys(keys);

      expect(result).toHaveLength(1);
    });

    it('should handle empty keys array', () => {
      const vindex = new HashVindex(shards);
      const result = vindex.getShardsForKeys([]);
      expect(result).toHaveLength(0);
    });
  });

  describe('getAllShards', () => {
    it('should return all shard IDs', () => {
      const vindex = new HashVindex(shards);
      const allShards = vindex.getAllShards();

      expect(allShards).toHaveLength(shards.length);
      for (const shardConfig of shards) {
        expect(allShards).toContain(shardConfig.id);
      }
    });

    it('should return a copy (not reference)', () => {
      const vindex = new HashVindex(shards);
      const result1 = vindex.getAllShards();
      const result2 = vindex.getAllShards();

      expect(result1).not.toBe(result2);
      expect(result1).toEqual(result2);
    });
  });

  describe('getShardsForRange', () => {
    it('should return all shards (hash cannot optimize ranges)', () => {
      const vindex = new HashVindex(shards);
      const result = vindex.getShardsForRange(1, 100);

      expect(result).toHaveLength(shards.length);
    });
  });
});

// =============================================================================
// CONSISTENT HASH VINDEX TESTS
// =============================================================================

describe('ConsistentHashVindex', () => {
  let shards: ShardConfig[];

  beforeEach(() => {
    shards = createTestShards(4);
  });

  describe('constructor', () => {
    it('should create vindex with default settings', () => {
      const vindex = new ConsistentHashVindex(shards);
      expect(vindex).toBeInstanceOf(ConsistentHashVindex);
    });

    it('should create vindex with custom virtual nodes', () => {
      const vindex = new ConsistentHashVindex(shards, {
        type: 'consistent-hash',
        virtualNodes: 200,
      });
      const state = vindex.getRingState();
      expect(state.totalNodes).toBe(200 * shards.length);
    });

    it('should throw if no shards provided', () => {
      expect(() => new ConsistentHashVindex([])).toThrow(
        'ConsistentHashVindex requires at least one shard'
      );
    });
  });

  describe('getShard', () => {
    it('should return consistent shard for same key', () => {
      const vindex = new ConsistentHashVindex(shards);
      const shard1 = vindex.getShard('user-123');
      const shard2 = vindex.getShard('user-123');
      expect(shard1).toBe(shard2);
    });

    it('should distribute keys across all shards', () => {
      const vindex = new ConsistentHashVindex(shards);
      const shardsUsed = new Set<string>();

      for (let i = 0; i < 1000; i++) {
        shardsUsed.add(vindex.getShard(`key-${i}`));
      }

      expect(shardsUsed.size).toBe(shards.length);
    });

    it('should wrap around the ring correctly', () => {
      const vindex = new ConsistentHashVindex(shards);
      // Test many keys to ensure wrap-around works
      for (let i = 0; i < 100; i++) {
        const shardId = vindex.getShard(`wrap-test-${i}`);
        expect(shards.map(s => s.id)).toContain(shardId);
      }
    });
  });

  describe('getRingState', () => {
    it('should return ring statistics', () => {
      const vindex = new ConsistentHashVindex(shards, {
        type: 'consistent-hash',
        virtualNodes: 100,
      });
      const state = vindex.getRingState();

      expect(state.totalNodes).toBe(100 * shards.length);
      expect(state.nodesPerShard.size).toBe(shards.length);

      for (const shardConfig of shards) {
        expect(state.nodesPerShard.get(shardConfig.id)).toBe(100);
      }
    });
  });

  describe('rebalancing behavior', () => {
    it('should minimize key movement when adding a shard', () => {
      const originalShards = createTestShards(3);
      const originalVindex = new ConsistentHashVindex(originalShards);

      // Map keys to their original shards
      const keyCount = 10000;
      const originalMapping = new Map<string, string>();
      for (let i = 0; i < keyCount; i++) {
        const key = `key-${i}`;
        originalMapping.set(key, originalVindex.getShard(key));
      }

      // Add a new shard
      const newShards = [...originalShards, shard(createShardId('shard-4'), 'do-ns-4')];
      const newVindex = new ConsistentHashVindex(newShards);

      // Count keys that moved
      let movedKeys = 0;
      for (let i = 0; i < keyCount; i++) {
        const key = `key-${i}`;
        if (originalMapping.get(key) !== newVindex.getShard(key)) {
          movedKeys++;
        }
      }

      // With consistent hashing, approximately 1/N keys should move
      // where N is the new number of shards
      const expectedMoveRatio = 1 / newShards.length;
      const actualMoveRatio = movedKeys / keyCount;

      // Allow some variance (within 50% of expected)
      expect(actualMoveRatio).toBeLessThan(expectedMoveRatio * 1.5);
    });
  });

  describe('getShardsForRange', () => {
    it('should return all shards (consistent hash cannot optimize ranges)', () => {
      const vindex = new ConsistentHashVindex(shards);
      const result = vindex.getShardsForRange(1, 100);

      expect(result).toHaveLength(shards.length);
    });
  });
});

// =============================================================================
// RANGE VINDEX TESTS
// =============================================================================

describe('RangeVindex', () => {
  let shards: ShardConfig[];
  let boundaries: RangeBoundary<number>[];

  beforeEach(() => {
    shards = createTestShards(3);
    boundaries = [
      { shard: shards[0].id, min: 0, max: 100 },
      { shard: shards[1].id, min: 100, max: 200 },
      { shard: shards[2].id, min: 200, max: null }, // Unbounded
    ];
  });

  describe('constructor', () => {
    it('should create vindex with valid boundaries', () => {
      const vindex = new RangeVindex(shards, { type: 'range', boundaries });
      expect(vindex).toBeInstanceOf(RangeVindex);
    });

    it('should throw if no boundaries provided', () => {
      expect(() => new RangeVindex(shards, { type: 'range', boundaries: [] })).toThrow(
        'RangeVindex requires at least one boundary'
      );
    });

    it('should throw if boundary references unknown shard', () => {
      const invalidBoundaries = [
        { shard: createShardId('unknown-shard'), min: 0, max: 100 },
      ];
      expect(() =>
        new RangeVindex(shards, { type: 'range', boundaries: invalidBoundaries })
      ).toThrow('Boundary references unknown shard: unknown-shard');
    });

    it('should sort boundaries by min value', () => {
      const unsortedBoundaries: RangeBoundary<number>[] = [
        { shard: shards[2].id, min: 200, max: null },
        { shard: shards[0].id, min: 0, max: 100 },
        { shard: shards[1].id, min: 100, max: 200 },
      ];
      const vindex = new RangeVindex(shards, { type: 'range', boundaries: unsortedBoundaries });
      const sortedBoundaries = vindex.getBoundaries();

      expect(sortedBoundaries[0].min).toBe(0);
      expect(sortedBoundaries[1].min).toBe(100);
      expect(sortedBoundaries[2].min).toBe(200);
    });
  });

  describe('getShard with numeric boundaries', () => {
    it('should route to first boundary for values in range [0, 100)', () => {
      const vindex = new RangeVindex(shards, { type: 'range', boundaries });

      expect(vindex.getShard(0)).toBe(shards[0].id);
      expect(vindex.getShard(50)).toBe(shards[0].id);
      expect(vindex.getShard(99)).toBe(shards[0].id);
    });

    it('should route to second boundary for values in range [100, 200)', () => {
      const vindex = new RangeVindex(shards, { type: 'range', boundaries });

      expect(vindex.getShard(100)).toBe(shards[1].id);
      expect(vindex.getShard(150)).toBe(shards[1].id);
      expect(vindex.getShard(199)).toBe(shards[1].id);
    });

    it('should route to unbounded boundary for values >= 200', () => {
      const vindex = new RangeVindex(shards, { type: 'range', boundaries });

      expect(vindex.getShard(200)).toBe(shards[2].id);
      expect(vindex.getShard(1000)).toBe(shards[2].id);
      expect(vindex.getShard(999999)).toBe(shards[2].id);
    });

    it('should handle boundary edge cases', () => {
      const vindex = new RangeVindex(shards, { type: 'range', boundaries });

      // At boundary: 100 belongs to second range (min is inclusive)
      expect(vindex.getShard(100)).toBe(shards[1].id);
      // At boundary: 200 belongs to third range
      expect(vindex.getShard(200)).toBe(shards[2].id);
    });
  });

  describe('getShard with string boundaries', () => {
    it('should route based on string comparison', () => {
      const stringBoundaries: RangeBoundary<string>[] = [
        { shard: shards[0].id, min: 'a', max: 'm' },
        { shard: shards[1].id, min: 'm', max: 'z' },
        { shard: shards[2].id, min: 'z', max: null },
      ];
      const vindex = new RangeVindex(shards, { type: 'range', boundaries: stringBoundaries });

      expect(vindex.getShard('apple')).toBe(shards[0].id);
      expect(vindex.getShard('banana')).toBe(shards[0].id);
      expect(vindex.getShard('mango')).toBe(shards[1].id);
      expect(vindex.getShard('zebra')).toBe(shards[2].id);
    });
  });

  describe('getShard with date boundaries', () => {
    it('should route based on date comparison', () => {
      const dateBoundaries: RangeBoundary<Date>[] = [
        { shard: shards[0].id, min: new Date('2024-01-01'), max: new Date('2024-04-01') },
        { shard: shards[1].id, min: new Date('2024-04-01'), max: new Date('2024-07-01') },
        { shard: shards[2].id, min: new Date('2024-07-01'), max: null },
      ];
      const vindex = new RangeVindex(shards, { type: 'range', boundaries: dateBoundaries });

      expect(vindex.getShard(new Date('2024-02-15'))).toBe(shards[0].id);
      expect(vindex.getShard(new Date('2024-05-15'))).toBe(shards[1].id);
      expect(vindex.getShard(new Date('2024-09-15'))).toBe(shards[2].id);
    });
  });

  describe('getShardsForKeys', () => {
    it('should return unique shards for keys in different ranges', () => {
      const vindex = new RangeVindex(shards, { type: 'range', boundaries });
      const keys = [50, 150, 250];
      const result = vindex.getShardsForKeys(keys);

      expect(result).toHaveLength(3);
      expect(result).toContain(shards[0].id);
      expect(result).toContain(shards[1].id);
      expect(result).toContain(shards[2].id);
    });

    it('should deduplicate shards for keys in same range', () => {
      const vindex = new RangeVindex(shards, { type: 'range', boundaries });
      const keys = [10, 20, 30, 40, 50];
      const result = vindex.getShardsForKeys(keys);

      expect(result).toHaveLength(1);
      expect(result[0]).toBe(shards[0].id);
    });
  });

  describe('getShardsForRange', () => {
    it('should return single shard for range within one boundary', () => {
      const vindex = new RangeVindex(shards, { type: 'range', boundaries });
      const result = vindex.getShardsForRange(10, 50);

      expect(result).toHaveLength(1);
      expect(result[0]).toBe(shards[0].id);
    });

    it('should return multiple shards for range spanning boundaries', () => {
      const vindex = new RangeVindex(shards, { type: 'range', boundaries });
      const result = vindex.getShardsForRange(50, 150);

      expect(result).toHaveLength(2);
      expect(result).toContain(shards[0].id);
      expect(result).toContain(shards[1].id);
    });

    it('should return all shards for range spanning entire key space', () => {
      const vindex = new RangeVindex(shards, { type: 'range', boundaries });
      const result = vindex.getShardsForRange(0, 1000);

      expect(result).toHaveLength(3);
    });

    it('should handle range at boundary edges', () => {
      const vindex = new RangeVindex(shards, { type: 'range', boundaries });

      // Range [99, 101) spans two boundaries
      const result = vindex.getShardsForRange(99, 101);
      expect(result).toHaveLength(2);
    });
  });

  describe('getBoundaries', () => {
    it('should return readonly boundary list', () => {
      const vindex = new RangeVindex(shards, { type: 'range', boundaries });
      const result = vindex.getBoundaries();

      expect(result).toHaveLength(3);
      expect(result[0].min).toBe(0);
    });
  });
});

// =============================================================================
// VINDEX FACTORY TESTS
// =============================================================================

describe('createVindex factory', () => {
  let shards: ShardConfig[];

  beforeEach(() => {
    shards = createTestShards(3);
  });

  it('should create HashVindex for hash config', () => {
    const vindex = createVindex(shards, hashVindex());
    expect(vindex).toBeInstanceOf(HashVindex);
  });

  it('should create HashVindex with xxhash algorithm', () => {
    const vindex = createVindex(shards, hashVindex('xxhash'));
    expect(vindex).toBeInstanceOf(HashVindex);
  });

  it('should create ConsistentHashVindex for consistent-hash config', () => {
    const vindex = createVindex(shards, consistentHashVindex());
    expect(vindex).toBeInstanceOf(ConsistentHashVindex);
  });

  it('should create ConsistentHashVindex with custom virtual nodes', () => {
    const vindex = createVindex(shards, consistentHashVindex(200));
    expect(vindex).toBeInstanceOf(ConsistentHashVindex);
    const state = (vindex as ConsistentHashVindex).getRingState();
    expect(state.totalNodes).toBe(200 * shards.length);
  });

  it('should create RangeVindex for range config', () => {
    const boundaries: RangeBoundary<number>[] = [
      { shard: shards[0].id, min: 0, max: 100 },
      { shard: shards[1].id, min: 100, max: 200 },
      { shard: shards[2].id, min: 200, max: null },
    ];
    const vindex = createVindex(shards, rangeVindex(boundaries));
    expect(vindex).toBeInstanceOf(RangeVindex);
  });

  it('should throw for unknown vindex type', () => {
    const invalidConfig = { type: 'unknown' as 'hash' };
    expect(() => createVindex(shards, invalidConfig)).toThrow();
  });
});

// =============================================================================
// DISTRIBUTION UTILITIES TESTS
// =============================================================================

describe('Distribution Utilities', () => {
  describe('testDistribution', () => {
    it('should generate distribution map for hash vindex', () => {
      const shards = createTestShards(4);
      const vindex = new HashVindex(shards);

      let counter = 0;
      const distribution = testDistribution(vindex, () => `key-${counter++}`, 1000);

      expect(distribution.size).toBe(4);
      let totalCount = 0;
      for (const count of distribution.values()) {
        totalCount += count;
      }
      expect(totalCount).toBe(1000);
    });

    it('should initialize all shards with zero count', () => {
      const shards = createTestShards(4);
      const vindex = new HashVindex(shards);

      // Use generator that always returns same key (single shard hit)
      const distribution = testDistribution(vindex, () => 'same-key', 100);

      // All shards should be in the map
      expect(distribution.size).toBe(4);

      // Only one shard should have non-zero count
      const nonZeroCounts = Array.from(distribution.values()).filter(c => c > 0);
      expect(nonZeroCounts).toHaveLength(1);
      expect(nonZeroCounts[0]).toBe(100);
    });
  });

  describe('distributionStats', () => {
    it('should calculate min, max, mean correctly', () => {
      const distribution = new Map<string, number>([
        ['shard-1', 100],
        ['shard-2', 200],
        ['shard-3', 300],
      ]);

      const stats = distributionStats(distribution);

      expect(stats.min).toBe(100);
      expect(stats.max).toBe(300);
      expect(stats.mean).toBe(200);
    });

    it('should calculate standard deviation correctly', () => {
      const distribution = new Map<string, number>([
        ['shard-1', 100],
        ['shard-2', 100],
        ['shard-3', 100],
      ]);

      const stats = distributionStats(distribution);

      expect(stats.stdDev).toBe(0); // All values equal
    });

    it('should handle empty distribution', () => {
      const distribution = new Map<string, number>();
      const stats = distributionStats(distribution);

      expect(stats.min).toBe(0);
      expect(stats.max).toBe(0);
      expect(stats.mean).toBe(0);
      expect(stats.stdDev).toBe(0);
      expect(stats.skew).toBe(0);
    });

    it('should detect uniform distribution (low stdDev)', () => {
      const shards = createTestShards(4);
      const vindex = new HashVindex(shards);

      let counter = 0;
      const distribution = testDistribution(vindex, () => `key-${counter++}`, 10000);
      const stats = distributionStats(distribution);

      // For uniform distribution, stdDev should be much smaller than mean
      const coefficientOfVariation = stats.stdDev / stats.mean;
      expect(coefficientOfVariation).toBeLessThan(0.2); // CV < 20%
    });
  });
});

// =============================================================================
// EDGE CASES AND ERROR HANDLING
// =============================================================================

describe('Edge Cases', () => {
  describe('single shard scenarios', () => {
    it('should work with single shard hash vindex', () => {
      const shards = createTestShards(1);
      const vindex = new HashVindex(shards);

      expect(vindex.getShard('any-key')).toBe(shards[0].id);
      expect(vindex.getAllShards()).toHaveLength(1);
    });

    it('should work with single shard consistent hash vindex', () => {
      const shards = createTestShards(1);
      const vindex = new ConsistentHashVindex(shards);

      expect(vindex.getShard('any-key')).toBe(shards[0].id);
    });

    it('should work with single boundary range vindex', () => {
      const shards = createTestShards(1);
      const boundaries: RangeBoundary<number>[] = [
        { shard: shards[0].id, min: 0, max: null },
      ];
      const vindex = new RangeVindex(shards, { type: 'range', boundaries });

      expect(vindex.getShard(0)).toBe(shards[0].id);
      expect(vindex.getShard(999999)).toBe(shards[0].id);
    });
  });

  describe('large number of shards', () => {
    it('should handle many shards efficiently', () => {
      const shards = createTestShards(100);
      const vindex = new HashVindex(shards);

      // Should distribute across many shards
      const shardsUsed = new Set<string>();
      for (let i = 0; i < 10000; i++) {
        shardsUsed.add(vindex.getShard(`key-${i}`));
      }

      // Should use most shards
      expect(shardsUsed.size).toBeGreaterThan(90);
    });
  });

  describe('special key values', () => {
    it('should handle null-like values', () => {
      const shards = createTestShards(3);
      const vindex = new HashVindex(shards);

      // These should not throw
      expect(() => vindex.getShard(null)).not.toThrow();
      expect(() => vindex.getShard(undefined)).not.toThrow();
    });

    it('should handle empty string keys', () => {
      const shards = createTestShards(3);
      const vindex = new HashVindex(shards);

      const shardId = vindex.getShard('');
      expect(shards.map(s => s.id)).toContain(shardId);
    });

    it('should handle very long keys', () => {
      const shards = createTestShards(3);
      const vindex = new HashVindex(shards);

      const longKey = 'a'.repeat(10000);
      const shardId = vindex.getShard(longKey);
      expect(shards.map(s => s.id)).toContain(shardId);
    });
  });
});
