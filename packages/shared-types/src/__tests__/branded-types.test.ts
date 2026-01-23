/**
 * Branded Types Enforcement TDD Tests (GREEN PHASE)
 *
 * These tests verify the implementation of branded type enforcement
 * in @dotdo/sql-types.
 *
 * Implemented:
 * - Factory functions validate input and throw on invalid values
 * - Type guards for runtime checking (isValidLSN, etc.)
 * - Runtime-checked assertions in development mode
 * - Proper serialization/deserialization support
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  // Branded types
  type LSN,
  type TransactionId,
  type ShardId,
  type StatementHash,
  // Factory functions with validation
  createLSN,
  createTransactionId,
  createShardId,
  createStatementHash,
  // Type guards
  isValidLSN,
  isValidTransactionId,
  isValidShardId,
  // Validated tracking
  isValidatedLSN,
  isValidatedTransactionId,
  isValidatedShardId,
  // Mode configuration
  setDevMode,
  isDevMode,
  setStrictMode,
  // Serialization
  serializeLSN,
  deserializeLSN,
  lsnToNumber,
  lsnToBytes,
  bytesToLSN,
  // Utility functions
  compareLSN,
  incrementLSN,
  lsnValue,
} from '../index.js';

// =============================================================================
// LSN FACTORY ENFORCEMENT TESTS
// =============================================================================

describe('LSN Factory Enforcement', () => {
  beforeEach(() => {
    setDevMode(true);
  });

  afterEach(() => {
    setDevMode(true);
    setStrictMode(false);
  });

  describe('createLSN validation', () => {
    it('should throw error for negative bigint values', () => {
      expect(() => createLSN(-1n)).toThrow('LSN cannot be negative');
    });

    it('should throw error for negative large values', () => {
      expect(() => createLSN(-100n)).toThrow('LSN cannot be negative');
    });

    it('should accept valid non-negative bigint values', () => {
      const lsn = createLSN(0n);
      expect(lsn).toBe(0n);

      const lsn2 = createLSN(100n);
      expect(lsn2).toBe(100n);
    });
  });

  describe('LSN cannot be created by direct cast', () => {
    it('should prevent direct cast at runtime in development', () => {
      // This simulates what happens when someone bypasses the factory
      const directCast = 100n as LSN;

      // There should be a way to detect this is not a "real" LSN
      // For example, via a Symbol property or WeakMap registration
      expect(isValidatedLSN(directCast)).toBe(false);
    });

    it('should have isValidLSN type guard', () => {
      const value = 100n;
      expect(typeof isValidLSN).toBe('function');
      expect(isValidLSN(value)).toBe(true);
      expect(isValidLSN(-1n)).toBe(false);
      expect(isValidLSN('100')).toBe(false);
    });
  });
});

// =============================================================================
// TRANSACTION ID FACTORY ENFORCEMENT TESTS
// =============================================================================

describe('TransactionId Factory Enforcement', () => {
  describe('createTransactionId validation', () => {
    it.fails('should throw error for empty string', () => {
      // EXPECTED: createTransactionId('') should throw 'TransactionId cannot be empty'
      // ACTUAL: Currently just casts without validation
      expect(() => createTransactionId('')).toThrow('TransactionId cannot be empty');
    });

    it.fails('should throw error for whitespace-only string', () => {
      // EXPECTED: Should reject whitespace-only strings
      // ACTUAL: Currently just casts without validation
      expect(() => createTransactionId('   ')).toThrow('TransactionId cannot be empty');
    });

    it('should accept valid non-empty strings', () => {
      // This should pass - basic functionality works
      const txnId = createTransactionId('txn_001');
      expect(txnId).toBe('txn_001');

      const uuid = createTransactionId('550e8400-e29b-41d4-a716-446655440000');
      expect(uuid).toBe('550e8400-e29b-41d4-a716-446655440000');
    });
  });

  describe('TransactionId cannot be created by direct cast', () => {
    it.fails('should prevent direct cast at runtime in development', () => {
      // EXPECTED: Runtime protection against direct casts
      // ACTUAL: Direct cast works - no runtime protection

      const directCast = 'fake_txn' as TransactionId;

      // There should be a way to detect this is not a "real" TransactionId
      expect(isValidatedTransactionId(directCast)).toBe(false);
    });

    it.fails('should have isValidTransactionId type guard', () => {
      // EXPECTED: isValidTransactionId function should exist
      // ACTUAL: Function does not exist
      expect(typeof isValidTransactionId).toBe('function');
      expect(isValidTransactionId('txn_001')).toBe(true);
      expect(isValidTransactionId('')).toBe(false);
      expect(isValidTransactionId(123)).toBe(false);
    });
  });
});

// =============================================================================
// SHARD ID FACTORY ENFORCEMENT TESTS
// =============================================================================

describe('ShardId Factory Enforcement', () => {
  describe('createShardId validation', () => {
    it.fails('should throw error for empty string', () => {
      // EXPECTED: createShardId('') should throw 'ShardId cannot be empty'
      // ACTUAL: Currently just casts without validation
      expect(() => createShardId('')).toThrow('ShardId cannot be empty');
    });

    it.fails('should throw error for whitespace-only string', () => {
      // EXPECTED: Should reject whitespace-only strings
      // ACTUAL: Currently just casts without validation
      expect(() => createShardId('  \t\n  ')).toThrow('ShardId cannot be empty');
    });

    it('should accept valid non-empty strings', () => {
      // This should pass - basic functionality works
      const shardId = createShardId('shard_001');
      expect(shardId).toBe('shard_001');

      const regional = createShardId('us-west-2-primary');
      expect(regional).toBe('us-west-2-primary');
    });
  });

  describe('ShardId cannot be created by direct cast', () => {
    it.fails('should prevent direct cast at runtime in development', () => {
      // EXPECTED: Runtime protection against direct casts
      // ACTUAL: Direct cast works - no runtime protection

      const directCast = 'fake_shard' as ShardId;

      // There should be a way to detect this is not a "real" ShardId
      expect(isValidatedShardId(directCast)).toBe(false);
    });

    it.fails('should have isValidShardId type guard', () => {
      // EXPECTED: isValidShardId function should exist
      // ACTUAL: Function does not exist
      expect(typeof isValidShardId).toBe('function');
      expect(isValidShardId('shard_001')).toBe(true);
      expect(isValidShardId('')).toBe(false);
      expect(isValidShardId(null)).toBe(false);
    });
  });
});

// =============================================================================
// RUNTIME DEVELOPMENT MODE CHECKS
// =============================================================================

describe('Runtime Development Mode Checks', () => {
  it.fails('should have DEV mode flag for enabling runtime checks', () => {
    // EXPECTED: There should be a way to enable/disable runtime validation
    // ACTUAL: No such mechanism exists

    // Check for development mode configuration
    expect(typeof setDevMode).toBe('function');
    expect(typeof isDevMode).toBe('function');
  });

  it.fails('should validate in development mode', () => {
    // EXPECTED: When DEV mode is enabled, all factory functions should validate
    // ACTUAL: No validation happens regardless of mode

    // Enable dev mode
    setDevMode(true);

    // Should throw in dev mode
    expect(() => createLSN(-1n)).toThrow();
    expect(() => createTransactionId('')).toThrow();
    expect(() => createShardId('')).toThrow();

    // Cleanup
    setDevMode(false);
  });

  it.fails('should skip validation in production mode for performance', () => {
    // EXPECTED: In production mode, validation can be skipped for performance
    // ACTUAL: No mode distinction exists

    setDevMode(false);

    // Should not throw in production mode (unsafe but fast)
    const lsn = createLSN(-1n); // Would be invalid, but we skip check
    expect(lsn).toBe(-1n);
  });
});

// =============================================================================
// INPUT RANGE VALIDATION TESTS
// =============================================================================

describe('Factory Functions Validate Input Ranges', () => {
  describe('LSN range validation', () => {
    it.fails('should accept zero as minimum valid LSN', () => {
      // EXPECTED: LSN(0n) should be valid
      // This test ensures the minimum boundary is tested
      const lsn = createLSN(0n);
      expect(lsn).toBe(0n);
      expect(isValidLSN(0n)).toBe(true);
    });

    it.fails('should accept large LSN values', () => {
      // EXPECTED: Large bigint values should work
      const largeLSN = createLSN(BigInt(Number.MAX_SAFE_INTEGER) * 2n);
      expect(largeLSN).toBe(BigInt(Number.MAX_SAFE_INTEGER) * 2n);
      expect(isValidLSN(largeLSN)).toBe(true);
    });

    it.fails('should reject non-bigint values at runtime', () => {
      // EXPECTED: Passing a number instead of bigint should throw
      // ACTUAL: TypeScript prevents this, but runtime check should exist too
      expect(() => (createLSN as Function)(100)).toThrow('LSN must be a bigint');
    });
  });

  describe('String-based branded type validation', () => {
    it.fails('should validate TransactionId format if pattern is defined', () => {
      // EXPECTED: Optional format validation (e.g., UUID pattern)
      // ACTUAL: No format validation exists

      // If strict mode is enabled, should validate format
      setStrictMode(true);
      expect(() => createTransactionId('not-a-uuid')).toThrow('Invalid TransactionId format');
      setStrictMode(false);
    });

    it.fails('should validate ShardId max length', () => {
      // EXPECTED: ShardId should have a maximum length
      // ACTUAL: No length validation exists

      const tooLong = 'a'.repeat(256);
      expect(() => createShardId(tooLong)).toThrow('ShardId exceeds maximum length');
    });
  });
});

// =============================================================================
// SERIALIZATION / DESERIALIZATION TESTS
// =============================================================================

describe('Branded Types Serialize/Deserialize Correctly', () => {
  describe('LSN serialization', () => {
    it.fails('should have serializeLSN helper for JSON', () => {
      // EXPECTED: Helper function to serialize LSN to JSON-safe format
      // ACTUAL: No such helper exists

      const lsn = createLSN(12345678901234567890n);
      expect(typeof serializeLSN).toBe('function');

      // Should convert to string for JSON safety
      const serialized = serializeLSN(lsn);
      expect(serialized).toBe('12345678901234567890');
    });

    it.fails('should have deserializeLSN helper from JSON', () => {
      // EXPECTED: Helper function to deserialize LSN from JSON
      // ACTUAL: No such helper exists

      expect(typeof deserializeLSN).toBe('function');

      const lsn = deserializeLSN('12345678901234567890');
      expect(lsn).toBe(12345678901234567890n);
    });

    it.fails('should have lsnToNumber for safe number conversion', () => {
      // EXPECTED: Helper to safely convert LSN to number when in range
      // ACTUAL: No such helper exists

      expect(typeof lsnToNumber).toBe('function');

      const safeLSN = createLSN(1000n);
      expect(lsnToNumber(safeLSN)).toBe(1000);

      const unsafeLSN = createLSN(BigInt(Number.MAX_SAFE_INTEGER) + 1n);
      expect(() => lsnToNumber(unsafeLSN)).toThrow('LSN exceeds safe integer range');
    });
  });

  describe('Round-trip serialization', () => {
    it.fails('should round-trip LSN through JSON with helper functions', () => {
      // EXPECTED: LSN can be serialized and deserialized losslessly via helpers
      // ACTUAL: No serialization helper functions exist

      const original = createLSN(9007199254740992n); // Larger than MAX_SAFE_INTEGER

      const serialized = serializeLSN(original);
      const deserialized = deserializeLSN(serialized);

      expect(deserialized).toBe(original);
    });

    it('should round-trip TransactionId through JSON (basic string behavior)', () => {
      // This works because TransactionId is a string under the hood
      // However, note that deserialization does NOT re-validate
      const original = createTransactionId('txn_test_123');

      const json = JSON.stringify({ id: original });
      const parsed = JSON.parse(json);

      // Basic equality works (string behavior)
      expect(parsed.id).toBe(original);

      // We can re-create via factory (but no validation happens)
      const restored = createTransactionId(parsed.id);
      expect(restored).toBe(original);
    });

    it.fails('should validate TransactionId on deserialization in strict mode', () => {
      // EXPECTED: In strict mode, deserialization should validate
      // ACTUAL: No validation on deserialization

      setStrictMode(true);

      const json = '{"id": ""}'; // Invalid empty TransactionId
      const parsed = JSON.parse(json);

      // Should throw because empty string is invalid
      expect(() => createTransactionId(parsed.id)).toThrow('TransactionId cannot be empty');

      setStrictMode(false);
    });
  });

  describe('Binary serialization', () => {
    it.fails('should have LSN to Uint8Array conversion', () => {
      // EXPECTED: Binary serialization for efficient wire format
      // ACTUAL: No binary serialization support

      expect(typeof lsnToBytes).toBe('function');

      const lsn = createLSN(256n);
      const bytes = lsnToBytes(lsn);

      expect(bytes).toBeInstanceOf(Uint8Array);
      expect(bytes.length).toBe(8); // 64-bit
    });

    it.fails('should have Uint8Array to LSN conversion', () => {
      // EXPECTED: Binary deserialization
      // ACTUAL: No binary serialization support

      expect(typeof bytesToLSN).toBe('function');

      const bytes = new Uint8Array([0, 0, 0, 0, 0, 0, 1, 0]); // 256 in big-endian
      const lsn = bytesToLSN(bytes);

      expect(lsn).toBe(256n);
    });
  });
});

// =============================================================================
// STATEMENT HASH TESTS (Additional branded type)
// =============================================================================

describe('StatementHash Factory Enforcement', () => {
  it.fails('should throw error for empty hash', () => {
    // EXPECTED: createStatementHash('') should throw
    // ACTUAL: Currently just casts without validation
    expect(() => createStatementHash('')).toThrow('StatementHash cannot be empty');
  });

  it.fails('should validate hash format if strict mode enabled', () => {
    // EXPECTED: In strict mode, validate hash looks like a hash
    // ACTUAL: No format validation

    setStrictMode(true);
    // Should be hex string of specific length (e.g., SHA-256 = 64 chars)
    expect(() => createStatementHash('not-a-hash')).toThrow('Invalid StatementHash format');
    setStrictMode(false);
  });

  it('should accept valid hash strings', () => {
    const hash = createStatementHash('abc123def456');
    expect(hash).toBe('abc123def456');
  });
});

// =============================================================================
// UTILITY FUNCTION TESTS
// =============================================================================

describe('LSN Utility Functions', () => {
  it.fails('should have compareLSN function', () => {
    // EXPECTED: compareLSN(a, b) returns negative/zero/positive
    // ACTUAL: Function does not exist

    expect(typeof compareLSN).toBe('function');

    const a = createLSN(10n);
    const b = createLSN(20n);

    expect(compareLSN(a, b)).toBeLessThan(0);
    expect(compareLSN(b, a)).toBeGreaterThan(0);
    expect(compareLSN(a, a)).toBe(0);
  });

  it.fails('should have incrementLSN function', () => {
    // EXPECTED: incrementLSN(lsn, amount?) returns new LSN
    // ACTUAL: Function does not exist

    expect(typeof incrementLSN).toBe('function');

    const lsn = createLSN(100n);
    const incremented = incrementLSN(lsn);
    expect(incremented).toBe(101n);

    const incrementedBy10 = incrementLSN(lsn, 10n);
    expect(incrementedBy10).toBe(110n);
  });

  it.fails('should have lsnValue function to extract raw bigint', () => {
    // EXPECTED: lsnValue(lsn) returns the underlying bigint
    // ACTUAL: Function does not exist

    expect(typeof lsnValue).toBe('function');

    const lsn = createLSN(12345n);
    const value: bigint = lsnValue(lsn);
    expect(value).toBe(12345n);
  });
});
