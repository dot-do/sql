/**
 * Branded Types TDD Tests
 *
 * Tests for branded types (LSN, TransactionId, ShardId, PageId) to ensure:
 * - Type safety at compile time (cannot assign plain primitives)
 * - Factory functions create valid branded values
 * - Validation rejects invalid inputs
 * - Utility functions work correctly
 *
 * Uses workers-vitest-pool for real Workers environment testing (NO MOCKS).
 */

import { describe, it, expect } from 'vitest';
import {
  // Branded types
  type LSN,
  type TransactionId,
  type ShardId,
  type PageId,
  // Factory functions
  createLSN,
  createTransactionId,
  createShardId,
  createPageId,
  // Type guards
  isValidLSN,
  isValidTransactionId,
  isValidShardId,
  isValidPageId,
  // Utility functions
  compareLSN,
  lsnValue,
  incrementLSN,
} from '../types.js';

// =============================================================================
// LSN BRANDED TYPE TESTS
// =============================================================================

describe('LSN Branded Type', () => {
  describe('createLSN factory function', () => {
    it('should create a valid LSN from a non-negative bigint', () => {
      const lsn = createLSN(0n);
      expect(lsn).toBe(0n);

      const lsn2 = createLSN(100n);
      expect(lsn2).toBe(100n);

      const lsn3 = createLSN(BigInt(Number.MAX_SAFE_INTEGER) * 2n);
      expect(lsn3).toBe(BigInt(Number.MAX_SAFE_INTEGER) * 2n);
    });

    it('should throw an error for negative bigint values', () => {
      expect(() => createLSN(-1n)).toThrow('LSN cannot be negative: -1');
      expect(() => createLSN(-100n)).toThrow('LSN cannot be negative: -100');
    });

    it('should preserve the bigint value exactly', () => {
      const value = 12345678901234567890n;
      const lsn = createLSN(value);
      expect(lsn).toBe(value);
    });
  });

  describe('isValidLSN type guard', () => {
    it('should return true for non-negative bigints', () => {
      expect(isValidLSN(0n)).toBe(true);
      expect(isValidLSN(100n)).toBe(true);
      expect(isValidLSN(BigInt(Number.MAX_SAFE_INTEGER))).toBe(true);
    });

    it('should return false for negative bigints', () => {
      expect(isValidLSN(-1n)).toBe(false);
      expect(isValidLSN(-100n)).toBe(false);
    });

    it('should return false for non-bigint values', () => {
      expect(isValidLSN(0)).toBe(false);
      expect(isValidLSN(100)).toBe(false);
      expect(isValidLSN('100')).toBe(false);
      expect(isValidLSN(null)).toBe(false);
      expect(isValidLSN(undefined)).toBe(false);
      expect(isValidLSN({})).toBe(false);
    });
  });

  describe('compareLSN utility', () => {
    it('should return negative when first LSN is less', () => {
      const a = createLSN(10n);
      const b = createLSN(20n);
      expect(compareLSN(a, b)).toBeLessThan(0);
    });

    it('should return positive when first LSN is greater', () => {
      const a = createLSN(20n);
      const b = createLSN(10n);
      expect(compareLSN(a, b)).toBeGreaterThan(0);
    });

    it('should return zero when LSNs are equal', () => {
      const a = createLSN(100n);
      const b = createLSN(100n);
      expect(compareLSN(a, b)).toBe(0);
    });
  });

  describe('lsnValue utility', () => {
    it('should extract the raw bigint value from an LSN', () => {
      const lsn = createLSN(12345n);
      const value: bigint = lsnValue(lsn);
      expect(value).toBe(12345n);
      expect(typeof value).toBe('bigint');
    });
  });

  describe('incrementLSN utility', () => {
    it('should increment LSN by 1 by default', () => {
      const lsn = createLSN(100n);
      const incremented = incrementLSN(lsn);
      expect(incremented).toBe(101n);
    });

    it('should increment LSN by specified amount', () => {
      const lsn = createLSN(100n);
      const incremented = incrementLSN(lsn, 50n);
      expect(incremented).toBe(150n);
    });

    it('should handle large increments', () => {
      const lsn = createLSN(BigInt(Number.MAX_SAFE_INTEGER));
      const incremented = incrementLSN(lsn, 1000n);
      expect(incremented).toBe(BigInt(Number.MAX_SAFE_INTEGER) + 1000n);
    });
  });
});

// =============================================================================
// TRANSACTION ID BRANDED TYPE TESTS
// =============================================================================

describe('TransactionId Branded Type', () => {
  describe('createTransactionId factory function', () => {
    it('should create a valid TransactionId from a non-empty string', () => {
      const txnId = createTransactionId('txn_001');
      expect(txnId).toBe('txn_001');
    });

    it('should accept UUIDs as transaction IDs', () => {
      const uuid = '550e8400-e29b-41d4-a716-446655440000';
      const txnId = createTransactionId(uuid);
      expect(txnId).toBe(uuid);
    });

    it('should accept prefixed IDs', () => {
      const txnId = createTransactionId('txn_abc123_456');
      expect(txnId).toBe('txn_abc123_456');
    });

    it('should throw an error for empty strings', () => {
      expect(() => createTransactionId('')).toThrow('TransactionId cannot be empty');
    });

    it('should preserve the string value exactly', () => {
      const value = 'complex_txn_id_with_special-chars.and:colons';
      const txnId = createTransactionId(value);
      expect(txnId).toBe(value);
    });
  });

  describe('isValidTransactionId type guard', () => {
    it('should return true for non-empty strings', () => {
      expect(isValidTransactionId('a')).toBe(true);
      expect(isValidTransactionId('txn_001')).toBe(true);
      expect(isValidTransactionId('550e8400-e29b-41d4-a716-446655440000')).toBe(true);
    });

    it('should return false for empty strings', () => {
      expect(isValidTransactionId('')).toBe(false);
    });

    it('should return false for non-string values', () => {
      expect(isValidTransactionId(0)).toBe(false);
      expect(isValidTransactionId(123)).toBe(false);
      expect(isValidTransactionId(null)).toBe(false);
      expect(isValidTransactionId(undefined)).toBe(false);
      expect(isValidTransactionId({})).toBe(false);
      expect(isValidTransactionId([])).toBe(false);
    });
  });
});

// =============================================================================
// SHARD ID BRANDED TYPE TESTS
// =============================================================================

describe('ShardId Branded Type', () => {
  describe('createShardId factory function', () => {
    it('should create a valid ShardId from a non-empty string', () => {
      const shardId = createShardId('shard_001');
      expect(shardId).toBe('shard_001');
    });

    it('should accept various shard ID formats', () => {
      expect(createShardId('shard-us-west-1')).toBe('shard-us-west-1');
      expect(createShardId('s1')).toBe('s1');
      expect(createShardId('primary')).toBe('primary');
    });

    it('should throw an error for empty strings', () => {
      expect(() => createShardId('')).toThrow('ShardId cannot be empty');
    });

    it('should preserve the string value exactly', () => {
      const value = 'shard_region_az1_replica2';
      const shardId = createShardId(value);
      expect(shardId).toBe(value);
    });
  });

  describe('isValidShardId type guard', () => {
    it('should return true for non-empty strings', () => {
      expect(isValidShardId('a')).toBe(true);
      expect(isValidShardId('shard_001')).toBe(true);
      expect(isValidShardId('primary-shard')).toBe(true);
    });

    it('should return false for empty strings', () => {
      expect(isValidShardId('')).toBe(false);
    });

    it('should return false for non-string values', () => {
      expect(isValidShardId(0)).toBe(false);
      expect(isValidShardId(123)).toBe(false);
      expect(isValidShardId(null)).toBe(false);
      expect(isValidShardId(undefined)).toBe(false);
      expect(isValidShardId({})).toBe(false);
    });
  });
});

// =============================================================================
// PAGE ID BRANDED TYPE TESTS
// =============================================================================

describe('PageId Branded Type', () => {
  describe('createPageId factory function', () => {
    it('should create a valid PageId from a non-negative integer', () => {
      const pageId = createPageId(0);
      expect(pageId).toBe(0);

      const pageId2 = createPageId(100);
      expect(pageId2).toBe(100);
    });

    it('should accept large page IDs within safe integer range', () => {
      const pageId = createPageId(Number.MAX_SAFE_INTEGER);
      expect(pageId).toBe(Number.MAX_SAFE_INTEGER);
    });

    it('should throw an error for negative numbers', () => {
      expect(() => createPageId(-1)).toThrow('PageId cannot be negative: -1');
      expect(() => createPageId(-100)).toThrow('PageId cannot be negative: -100');
    });

    it('should throw an error for non-integer numbers', () => {
      expect(() => createPageId(1.5)).toThrow('PageId must be an integer: 1.5');
      expect(() => createPageId(0.001)).toThrow('PageId must be an integer: 0.001');
      expect(() => createPageId(100.99)).toThrow('PageId must be an integer: 100.99');
    });

    it('should accept integer-valued floats', () => {
      // 5.0 is technically a float but represents an integer
      const pageId = createPageId(5.0);
      expect(pageId).toBe(5);
    });
  });

  describe('isValidPageId type guard', () => {
    it('should return true for non-negative integers', () => {
      expect(isValidPageId(0)).toBe(true);
      expect(isValidPageId(100)).toBe(true);
      expect(isValidPageId(Number.MAX_SAFE_INTEGER)).toBe(true);
    });

    it('should return false for negative numbers', () => {
      expect(isValidPageId(-1)).toBe(false);
      expect(isValidPageId(-100)).toBe(false);
    });

    it('should return false for non-integer numbers', () => {
      expect(isValidPageId(1.5)).toBe(false);
      expect(isValidPageId(0.001)).toBe(false);
    });

    it('should return false for non-number values', () => {
      expect(isValidPageId('0')).toBe(false);
      expect(isValidPageId('100')).toBe(false);
      expect(isValidPageId(null)).toBe(false);
      expect(isValidPageId(undefined)).toBe(false);
      expect(isValidPageId({})).toBe(false);
      expect(isValidPageId(0n)).toBe(false); // bigint is not number
    });
  });
});

// =============================================================================
// TYPE SAFETY TESTS (Compile-time behavior verification)
// =============================================================================

describe('Type Safety', () => {
  /**
   * These tests verify that the branded types provide compile-time safety.
   * While we cannot test compile-time errors directly, we can verify that
   * the runtime behavior is consistent with the type system.
   */

  it('branded LSN preserves bigint operations', () => {
    const lsn = createLSN(100n);

    // The branded type should still support bigint operations for comparison
    expect(lsn > 50n).toBe(true);
    expect(lsn < 200n).toBe(true);
    expect(lsn === 100n).toBe(true);
  });

  it('branded TransactionId preserves string operations', () => {
    const txnId = createTransactionId('txn_test_123');

    // The branded type should still support string operations
    expect(txnId.startsWith('txn_')).toBe(true);
    expect(txnId.includes('test')).toBe(true);
    expect(txnId.length).toBe(12);
  });

  it('branded ShardId preserves string operations', () => {
    const shardId = createShardId('shard_primary');

    // The branded type should still support string operations
    expect(shardId.startsWith('shard_')).toBe(true);
    expect(shardId.toUpperCase()).toBe('SHARD_PRIMARY');
  });

  it('branded PageId preserves number operations', () => {
    const pageId = createPageId(42);

    // The branded type should still support number operations
    expect(pageId + 1).toBe(43);
    expect(pageId * 2).toBe(84);
    expect(pageId > 40).toBe(true);
  });

  it('branded types can be used in collections', () => {
    // Test that branded types work correctly in Maps and Sets
    const lsnMap = new Map<LSN, string>();
    const lsn1 = createLSN(1n);
    const lsn2 = createLSN(2n);

    lsnMap.set(lsn1, 'first');
    lsnMap.set(lsn2, 'second');

    expect(lsnMap.get(lsn1)).toBe('first');
    expect(lsnMap.get(lsn2)).toBe('second');
    expect(lsnMap.size).toBe(2);

    const shardSet = new Set<ShardId>();
    const shard1 = createShardId('shard1');
    const shard2 = createShardId('shard2');

    shardSet.add(shard1);
    shardSet.add(shard2);
    shardSet.add(createShardId('shard1')); // Duplicate

    expect(shardSet.size).toBe(2);
    expect(shardSet.has(shard1)).toBe(true);
  });

  it('branded types serialize correctly to JSON', () => {
    const txnId = createTransactionId('txn_json_test');
    const shardId = createShardId('shard_json');
    const pageId = createPageId(123);

    const obj = { txnId, shardId, pageId };
    const json = JSON.stringify(obj);
    const parsed = JSON.parse(json);

    expect(parsed.txnId).toBe('txn_json_test');
    expect(parsed.shardId).toBe('shard_json');
    expect(parsed.pageId).toBe(123);
  });

  it('LSN serializes to string in JSON (bigint behavior)', () => {
    const lsn = createLSN(100n);

    // Note: BigInt cannot be directly serialized to JSON
    // This tests the expected behavior that users need to handle
    expect(() => JSON.stringify({ lsn })).toThrow();

    // Proper serialization requires conversion
    const serializable = { lsn: lsn.toString() };
    expect(JSON.stringify(serializable)).toBe('{"lsn":"100"}');
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('Branded Types Integration', () => {
  it('should work together in a transaction context', () => {
    // Simulating a transaction record
    interface TransactionRecord {
      id: TransactionId;
      startLsn: LSN;
      endLsn?: LSN;
      shard: ShardId;
    }

    const record: TransactionRecord = {
      id: createTransactionId('txn_integration_001'),
      startLsn: createLSN(1000n),
      shard: createShardId('primary'),
    };

    expect(record.id).toBe('txn_integration_001');
    expect(record.startLsn).toBe(1000n);
    expect(record.shard).toBe('primary');

    // Update the end LSN
    record.endLsn = incrementLSN(record.startLsn, 10n);
    expect(record.endLsn).toBe(1010n);
  });

  it('should work in page allocation context', () => {
    // Simulating page allocation
    interface PageAllocation {
      pageId: PageId;
      allocatedAt: LSN;
      shard: ShardId;
    }

    const allocations: PageAllocation[] = [];

    for (let i = 0; i < 5; i++) {
      allocations.push({
        pageId: createPageId(i),
        allocatedAt: createLSN(BigInt(i * 100)),
        shard: createShardId(`shard_${i % 2}`),
      });
    }

    expect(allocations.length).toBe(5);
    expect(allocations[0].pageId).toBe(0);
    expect(allocations[4].pageId).toBe(4);
    expect(allocations[0].allocatedAt).toBe(0n);
    expect(allocations[4].allocatedAt).toBe(400n);
  });

  it('should support LSN range queries', () => {
    const startLsn = createLSN(100n);
    const endLsn = createLSN(200n);

    // Check if an LSN is in range
    const checkLsn = createLSN(150n);

    expect(compareLSN(checkLsn, startLsn)).toBeGreaterThanOrEqual(0);
    expect(compareLSN(checkLsn, endLsn)).toBeLessThanOrEqual(0);

    // Outside range
    const beforeRange = createLSN(50n);
    expect(compareLSN(beforeRange, startLsn)).toBeLessThan(0);

    const afterRange = createLSN(250n);
    expect(compareLSN(afterRange, endLsn)).toBeGreaterThan(0);
  });
});
