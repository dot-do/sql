/**
 * Hash Functions Tests
 *
 * Tests for the shared FNV-1a and xxHash implementations.
 */
import { describe, expect, it } from 'vitest';
import {
  fnv1a,
  fnv1aString,
  fnv1aNumber,
  fnv1aBigInt,
  fnv1aBytes,
  xxhash,
  getHashFunction,
  FNV_OFFSET_BASIS,
  FNV_PRIME,
} from '../hash.js';

describe('FNV-1a Hash', () => {
  describe('fnv1a basic function', () => {
    it('should hash strings consistently', () => {
      const hash1 = fnv1a('hello');
      const hash2 = fnv1a('hello');
      expect(hash1).toBe(hash2);
    });

    it('should produce different hashes for different inputs', () => {
      const hash1 = fnv1a('hello');
      const hash2 = fnv1a('world');
      expect(hash1).not.toBe(hash2);
    });

    it('should hash numbers', () => {
      const hash = fnv1a(12345);
      expect(typeof hash).toBe('number');
      expect(hash).toBeGreaterThan(0);
    });

    it('should hash bigints', () => {
      const hash = fnv1a(BigInt('9007199254740993'));
      expect(typeof hash).toBe('number');
      expect(hash).toBeGreaterThan(0);
    });

    it('should return 32-bit unsigned integer', () => {
      const hash = fnv1a('test');
      expect(hash).toBeGreaterThanOrEqual(0);
      expect(hash).toBeLessThanOrEqual(0xffffffff);
    });

    it('should produce known hash values for reference strings', () => {
      // Test with known FNV-1a 32-bit hash values
      // Empty string should produce the offset basis XOR'd with the empty content
      const emptyHash = fnv1a('');
      expect(emptyHash).toBe(FNV_OFFSET_BASIS);
    });
  });

  describe('fnv1aString with custom start', () => {
    it('should allow continuing hash from a starting value', () => {
      const start = FNV_OFFSET_BASIS;
      const hash1 = fnv1aString('hello', start);
      const hash2 = fnv1a('hello');
      expect(hash1).toBe(hash2);
    });

    it('should produce different results with different starting values', () => {
      const hash1 = fnv1aString('test', FNV_OFFSET_BASIS);
      const hash2 = fnv1aString('test', 0);
      expect(hash1).not.toBe(hash2);
    });
  });

  describe('fnv1aNumber', () => {
    it('should hash small integers efficiently', () => {
      const hash = fnv1aNumber(42, FNV_OFFSET_BASIS);
      expect(typeof hash).toBe('number');
      expect(hash).toBeGreaterThan(0);
    });

    it('should handle negative numbers', () => {
      const hash = fnv1aNumber(-100, FNV_OFFSET_BASIS);
      expect(typeof hash).toBe('number');
    });

    it('should handle floats', () => {
      const hash = fnv1aNumber(3.14159, FNV_OFFSET_BASIS);
      expect(typeof hash).toBe('number');
    });

    it('should handle large numbers', () => {
      const hash = fnv1aNumber(Number.MAX_SAFE_INTEGER, FNV_OFFSET_BASIS);
      expect(typeof hash).toBe('number');
    });
  });

  describe('fnv1aBigInt', () => {
    it('should hash bigints', () => {
      const hash = fnv1aBigInt(BigInt('1234567890123456789'), FNV_OFFSET_BASIS);
      expect(typeof hash).toBe('number');
      expect(hash).toBeGreaterThan(0);
    });

    it('should produce consistent results', () => {
      const value = BigInt('999999999999');
      const hash1 = fnv1aBigInt(value, FNV_OFFSET_BASIS);
      const hash2 = fnv1aBigInt(value, FNV_OFFSET_BASIS);
      expect(hash1).toBe(hash2);
    });
  });

  describe('fnv1aBytes', () => {
    it('should hash byte arrays', () => {
      const bytes = new Uint8Array([1, 2, 3, 4, 5]);
      const hash = fnv1aBytes(bytes, FNV_OFFSET_BASIS);
      expect(typeof hash).toBe('number');
      expect(hash).toBeGreaterThan(0);
    });

    it('should produce consistent results', () => {
      const bytes = new Uint8Array([10, 20, 30]);
      const hash1 = fnv1aBytes(bytes, FNV_OFFSET_BASIS);
      const hash2 = fnv1aBytes(bytes, FNV_OFFSET_BASIS);
      expect(hash1).toBe(hash2);
    });

    it('should handle empty byte array', () => {
      const bytes = new Uint8Array([]);
      const hash = fnv1aBytes(bytes, FNV_OFFSET_BASIS);
      expect(typeof hash).toBe('number');
    });
  });

  describe('constants', () => {
    it('should export correct FNV offset basis', () => {
      expect(FNV_OFFSET_BASIS).toBe(2166136261);
    });

    it('should export correct FNV prime', () => {
      expect(FNV_PRIME).toBe(16777619);
    });
  });
});

describe('xxHash', () => {
  it('should hash strings consistently', () => {
    const hash1 = xxhash('hello world');
    const hash2 = xxhash('hello world');
    expect(hash1).toBe(hash2);
  });

  it('should produce different hashes for different inputs', () => {
    const hash1 = xxhash('hello');
    const hash2 = xxhash('world');
    expect(hash1).not.toBe(hash2);
  });

  it('should hash numbers', () => {
    const hash = xxhash(12345);
    expect(typeof hash).toBe('number');
    expect(hash).toBeGreaterThan(0);
  });

  it('should hash bigints', () => {
    const hash = xxhash(BigInt('9007199254740993'));
    expect(typeof hash).toBe('number');
    expect(hash).toBeGreaterThan(0);
  });

  it('should return 32-bit unsigned integer', () => {
    const hash = xxhash('test input for xxhash');
    expect(hash).toBeGreaterThanOrEqual(0);
    expect(hash).toBeLessThanOrEqual(0xffffffff);
  });

  it('should handle short strings', () => {
    const hash = xxhash('a');
    expect(typeof hash).toBe('number');
  });

  it('should handle long strings (>= 16 chars)', () => {
    const longString = 'this is a very long string that is more than sixteen characters';
    const hash = xxhash(longString);
    expect(typeof hash).toBe('number');
    expect(hash).toBeGreaterThan(0);
  });

  it('should handle empty string', () => {
    const hash = xxhash('');
    expect(typeof hash).toBe('number');
  });
});

describe('getHashFunction', () => {
  it('should return fnv1a function for "fnv1a"', () => {
    const hashFn = getHashFunction('fnv1a');
    expect(hashFn('test')).toBe(fnv1a('test'));
  });

  it('should return xxhash function for "xxhash"', () => {
    const hashFn = getHashFunction('xxhash');
    expect(hashFn('test')).toBe(xxhash('test'));
  });
});

describe('Distribution Quality', () => {
  it('should provide good distribution for sharding', () => {
    // Test that hashes are well-distributed across a range of inputs
    const numKeys = 1000;
    const numBuckets = 10;
    const buckets = new Array(numBuckets).fill(0);

    for (let i = 0; i < numKeys; i++) {
      const hash = fnv1a(`key-${i}`);
      const bucket = hash % numBuckets;
      buckets[bucket]++;
    }

    // Each bucket should have roughly numKeys/numBuckets entries
    const expected = numKeys / numBuckets;
    const tolerance = expected * 0.3; // Allow 30% variance

    for (const count of buckets) {
      expect(count).toBeGreaterThan(expected - tolerance);
      expect(count).toBeLessThan(expected + tolerance);
    }
  });

  it('should produce different hashes for similar keys', () => {
    // Hash collision resistance for similar keys
    const hashes = new Set<number>();
    for (let i = 0; i < 100; i++) {
      hashes.add(fnv1a(`user-${i}`));
    }
    // Should have very few or no collisions
    expect(hashes.size).toBe(100);
  });
});
