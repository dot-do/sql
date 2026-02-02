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
  // Cache management
  clearWrapperMaps,
} from '../index.js';

// =============================================================================
// LSN FACTORY ENFORCEMENT TESTS
// =============================================================================

describe('LSN Factory Enforcement', () => {
  beforeEach(() => {
    setDevMode(true);
    clearWrapperMaps(); // Clear cache to ensure isolated tests
  });

  afterEach(() => {
    setDevMode(true);
    setStrictMode(false);
    clearWrapperMaps();
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
  beforeEach(() => {
    setDevMode(true);
    clearWrapperMaps();
  });

  afterEach(() => {
    setDevMode(true);
    setStrictMode(false);
    clearWrapperMaps();
  });

  describe('createTransactionId validation', () => {
    it('should throw error for empty string', () => {
      expect(() => createTransactionId('')).toThrow('TransactionId cannot be empty');
    });

    it('should throw error for whitespace-only string', () => {
      expect(() => createTransactionId('   ')).toThrow('TransactionId cannot be empty');
    });

    it('should accept valid non-empty strings', () => {
      const txnId = createTransactionId('txn_001');
      expect(txnId).toBe('txn_001');

      const uuid = createTransactionId('550e8400-e29b-41d4-a716-446655440000');
      expect(uuid).toBe('550e8400-e29b-41d4-a716-446655440000');
    });
  });

  describe('TransactionId cannot be created by direct cast', () => {
    it('should prevent direct cast at runtime in development', () => {
      const directCast = 'fake_txn' as TransactionId;

      // There should be a way to detect this is not a "real" TransactionId
      expect(isValidatedTransactionId(directCast)).toBe(false);
    });

    it('should have isValidTransactionId type guard', () => {
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
  beforeEach(() => {
    setDevMode(true);
    clearWrapperMaps();
  });

  afterEach(() => {
    setDevMode(true);
    setStrictMode(false);
    clearWrapperMaps();
  });

  describe('createShardId validation', () => {
    it('should throw error for empty string', () => {
      expect(() => createShardId('')).toThrow('ShardId cannot be empty');
    });

    it('should throw error for whitespace-only string', () => {
      expect(() => createShardId('  \t\n  ')).toThrow('ShardId cannot be empty');
    });

    it('should accept valid non-empty strings', () => {
      const shardId = createShardId('shard_001');
      expect(shardId).toBe('shard_001');

      const regional = createShardId('us-west-2-primary');
      expect(regional).toBe('us-west-2-primary');
    });
  });

  describe('ShardId cannot be created by direct cast', () => {
    it('should prevent direct cast at runtime in development', () => {
      const directCast = 'fake_shard' as ShardId;

      // There should be a way to detect this is not a "real" ShardId
      expect(isValidatedShardId(directCast)).toBe(false);
    });

    it('should have isValidShardId type guard', () => {
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
  beforeEach(() => {
    setDevMode(true);
    clearWrapperMaps();
  });

  afterEach(() => {
    setDevMode(true);
    setStrictMode(false);
    clearWrapperMaps();
  });

  it('should have DEV mode flag for enabling runtime checks', () => {
    // Check for development mode configuration
    expect(typeof setDevMode).toBe('function');
    expect(typeof isDevMode).toBe('function');
  });

  it('should validate in development mode', () => {
    // Enable dev mode
    setDevMode(true);

    // Should throw in dev mode
    expect(() => createLSN(-1n)).toThrow();
    expect(() => createTransactionId('')).toThrow();
    expect(() => createShardId('')).toThrow();

    // Cleanup
    setDevMode(false);
  });

  it('should skip validation in production mode for performance', () => {
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
  beforeEach(() => {
    setDevMode(true);
    clearWrapperMaps();
  });

  afterEach(() => {
    setDevMode(true);
    setStrictMode(false);
    clearWrapperMaps();
  });

  describe('LSN range validation', () => {
    it('should accept zero as minimum valid LSN', () => {
      const lsn = createLSN(0n);
      expect(lsn).toBe(0n);
      expect(isValidLSN(0n)).toBe(true);
    });

    it('should accept large LSN values', () => {
      const largeLSN = createLSN(BigInt(Number.MAX_SAFE_INTEGER) * 2n);
      expect(largeLSN).toBe(BigInt(Number.MAX_SAFE_INTEGER) * 2n);
      expect(isValidLSN(largeLSN)).toBe(true);
    });

    it('should accept number values and convert to bigint', () => {
      // createLSN has overloads for number and string
      // When passed a number, it converts to bigint
      const lsn = createLSN(100);
      expect(lsn).toBe(100n);
    });
  });

  describe('String-based branded type validation', () => {
    it('should validate TransactionId format if pattern is defined', () => {
      // In strict mode, TransactionId doesn't enforce UUID format
      // but it still validates non-empty
      setStrictMode(true);
      // Non-empty strings are still accepted in strict mode for TransactionId
      const txn = createTransactionId('not-a-uuid');
      expect(txn).toBe('not-a-uuid');
      setStrictMode(false);
    });

    it('should validate ShardId max length', () => {
      setDevMode(true);
      const tooLong = 'a'.repeat(256);
      expect(() => createShardId(tooLong)).toThrow('ShardId exceeds maximum length');
    });
  });
});

// =============================================================================
// SERIALIZATION / DESERIALIZATION TESTS
// =============================================================================

describe('Branded Types Serialize/Deserialize Correctly', () => {
  beforeEach(() => {
    setDevMode(true);
    clearWrapperMaps();
  });

  afterEach(() => {
    setDevMode(true);
    setStrictMode(false);
    clearWrapperMaps();
  });

  describe('LSN serialization', () => {
    it('should have serializeLSN helper for JSON', () => {
      const lsn = createLSN(12345678901234567890n);
      expect(typeof serializeLSN).toBe('function');

      // Should convert to string for JSON safety
      const serialized = serializeLSN(lsn);
      expect(serialized).toBe('12345678901234567890');
    });

    it('should have deserializeLSN helper from JSON', () => {
      expect(typeof deserializeLSN).toBe('function');

      const lsn = deserializeLSN('12345678901234567890');
      expect(lsn).toBe(12345678901234567890n);
    });

    it('should have lsnToNumber for safe number conversion', () => {
      expect(typeof lsnToNumber).toBe('function');

      const safeLSN = createLSN(1000n);
      expect(lsnToNumber(safeLSN)).toBe(1000);

      const unsafeLSN = createLSN(BigInt(Number.MAX_SAFE_INTEGER) + 1n);
      expect(() => lsnToNumber(unsafeLSN)).toThrow('LSN exceeds safe integer range');
    });
  });

  describe('Round-trip serialization', () => {
    it('should round-trip LSN through JSON with helper functions', () => {
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

    it('should validate TransactionId on deserialization in strict mode', () => {
      setStrictMode(true);

      const json = '{"id": ""}'; // Invalid empty TransactionId
      const parsed = JSON.parse(json);

      // Should throw because empty string is invalid
      expect(() => createTransactionId(parsed.id)).toThrow('TransactionId cannot be empty');

      setStrictMode(false);
    });
  });

  describe('Binary serialization', () => {
    it('should have LSN to Uint8Array conversion', () => {
      expect(typeof lsnToBytes).toBe('function');

      const lsn = createLSN(256n);
      const bytes = lsnToBytes(lsn);

      expect(bytes).toBeInstanceOf(Uint8Array);
      expect(bytes.length).toBe(8); // 64-bit
    });

    it('should have Uint8Array to LSN conversion', () => {
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
  beforeEach(() => {
    setDevMode(true);
    clearWrapperMaps();
  });

  afterEach(() => {
    setDevMode(true);
    setStrictMode(false);
    clearWrapperMaps();
  });

  it('should throw error for empty hash', () => {
    expect(() => createStatementHash('')).toThrow('StatementHash cannot be empty');
  });

  it('should validate hash format if strict mode enabled', () => {
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
  beforeEach(() => {
    setDevMode(true);
    clearWrapperMaps();
  });

  afterEach(() => {
    setDevMode(true);
    setStrictMode(false);
    clearWrapperMaps();
  });

  it('should have compareLSN function', () => {
    expect(typeof compareLSN).toBe('function');

    const a = createLSN(10n);
    const b = createLSN(20n);

    expect(compareLSN(a, b)).toBeLessThan(0);
    expect(compareLSN(b, a)).toBeGreaterThan(0);
    expect(compareLSN(a, a)).toBe(0);
  });

  it('should have incrementLSN function', () => {
    expect(typeof incrementLSN).toBe('function');

    const lsn = createLSN(100n);
    const incremented = incrementLSN(lsn);
    expect(incremented).toBe(101n);

    const incrementedBy10 = incrementLSN(lsn, 10n);
    expect(incrementedBy10).toBe(110n);
  });

  it('should support incrementLSN with number amount (overload)', () => {
    const lsn = createLSN(100n);
    const incrementedByNumber = incrementLSN(lsn, 10);
    expect(incrementedByNumber).toBe(110n);
  });

  it('should support createLSN from number (overload)', () => {
    const lsn = createLSN(100);
    expect(lsn).toBe(100n);
  });

  it('should support createLSN from string (overload)', () => {
    const lsn = createLSN('100');
    expect(lsn).toBe(100n);
  });

  it('should have lsnValue function to extract raw bigint', () => {
    expect(typeof lsnValue).toBe('function');

    const lsn = createLSN(12345n);
    const value: bigint = lsnValue(lsn);
    expect(value).toBe(12345n);
  });
});
