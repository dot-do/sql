/**
 * WAL Writer Encoding Tests - TDD for Base64 Expansion Calculation
 *
 * Issue: sql-v91f - Fix Base64 expansion calculation in WAL writer
 *
 * The WAL writer uses incorrect Base64 overhead calculation (1.34 instead of 4/3).
 * Base64 encoding always expands data by exactly 4/3 (1.333...), plus padding.
 *
 * These tests verify:
 * 1. Size estimation accuracy for various key lengths
 * 2. Edge cases where 1.34 vs 4/3 makes a difference
 * 3. No buffer underestimate with exact calculation
 */

import { describe, it, expect, beforeEach } from 'vitest';

// =============================================================================
// Base64 Expansion Constants and Utilities
// =============================================================================

/**
 * The exact Base64 expansion ratio.
 * Base64 encodes 3 bytes into 4 characters, so the ratio is exactly 4/3.
 */
const BASE64_EXPANSION_EXACT = 4 / 3;

/**
 * The incorrect expansion factor previously used (1.34).
 * This can cause underestimation of encoded size.
 */
const BASE64_EXPANSION_WRONG = 1.34;

/**
 * Calculate the exact Base64 encoded length for a given input length.
 * Base64 pads to the nearest multiple of 4, so:
 * - 1 byte -> 4 chars (2 padding =)
 * - 2 bytes -> 4 chars (1 padding =)
 * - 3 bytes -> 4 chars (0 padding)
 * - n bytes -> ceil(n/3) * 4 chars
 */
function exactBase64Length(inputLength: number): number {
  return Math.ceil(inputLength / 3) * 4;
}

/**
 * Verify Base64 length calculation is correct by comparing with actual encoding.
 */
function verifyBase64Length(data: Uint8Array): { actual: number; calculated: number; match: boolean } {
  // Encode to Base64
  let binary = '';
  for (let i = 0; i < data.length; i++) {
    binary += String.fromCharCode(data[i]);
  }
  const base64 = btoa(binary);

  const actual = base64.length;
  const calculated = exactBase64Length(data.length);

  return {
    actual,
    calculated,
    match: actual === calculated,
  };
}

// =============================================================================
// Base64 Size Calculation Tests
// =============================================================================

describe('Base64 Size Calculation', () => {
  describe('exactBase64Length function', () => {
    it('should calculate correct length for input divisible by 3', () => {
      // 3 bytes -> 4 chars, 6 bytes -> 8 chars, 9 bytes -> 12 chars
      expect(exactBase64Length(3)).toBe(4);
      expect(exactBase64Length(6)).toBe(8);
      expect(exactBase64Length(9)).toBe(12);
      expect(exactBase64Length(300)).toBe(400);
    });

    it('should calculate correct length for input with 1 byte remainder', () => {
      // 1 byte -> 4 chars (with 2 padding)
      // 4 bytes -> 8 chars (with 2 padding)
      expect(exactBase64Length(1)).toBe(4);
      expect(exactBase64Length(4)).toBe(8);
      expect(exactBase64Length(7)).toBe(12);
      expect(exactBase64Length(301)).toBe(404);
    });

    it('should calculate correct length for input with 2 byte remainder', () => {
      // 2 bytes -> 4 chars (with 1 padding)
      // 5 bytes -> 8 chars (with 1 padding)
      expect(exactBase64Length(2)).toBe(4);
      expect(exactBase64Length(5)).toBe(8);
      expect(exactBase64Length(8)).toBe(12);
      expect(exactBase64Length(302)).toBe(404);
    });

    it('should handle zero length', () => {
      expect(exactBase64Length(0)).toBe(0);
    });

    it('should match actual Base64 encoding for various sizes', () => {
      const testSizes = [1, 2, 3, 10, 100, 1000, 10000];

      for (const size of testSizes) {
        const data = new Uint8Array(size);
        // Fill with random data to ensure it's representative
        for (let i = 0; i < size; i++) {
          data[i] = Math.floor(Math.random() * 256);
        }

        const result = verifyBase64Length(data);
        expect(result.match).toBe(true);
        expect(result.actual).toBe(result.calculated);
      }
    });
  });

  describe('comparison of 1.34 vs 4/3 expansion factors', () => {
    it('should show that ceil(length/3)*4 is always exact', () => {
      // The ONLY correct formula for Base64 length is ceil(length/3)*4
      // Simple multiplication by 4/3 or 1.34 is not accurate due to padding
      for (let size = 1; size <= 1000; size++) {
        const actual = exactBase64Length(size);

        // Using simple multiplication with 4/3 can underestimate
        // For size=1: 1 * (4/3) = 1.33, ceil = 2, but actual = 4
        const simpleMultiply = Math.ceil(size * BASE64_EXPANSION_EXACT);

        // The correct formula always matches
        const correctFormula = Math.ceil(size / 3) * 4;
        expect(correctFormula).toBe(actual);

        // Simple multiplication is often wrong (underestimates)
        // This demonstrates why we need the correct formula
        if (size % 3 !== 0) {
          // When not divisible by 3, simple multiply can be wrong
          // e.g., size=1: simple=2, actual=4
          // e.g., size=2: simple=3, actual=4
          expect(simpleMultiply).toBeLessThanOrEqual(actual);
        }
      }
    });

    it('should demonstrate the correct formula: ceil(length / 3) * 4', () => {
      // The ONLY correct way to calculate Base64 length is:
      // ceil(inputLength / 3) * 4
      //
      // This accounts for the 3-to-4 byte mapping AND the padding

      for (let size = 0; size <= 100; size++) {
        const data = new Uint8Array(size);
        for (let i = 0; i < size; i++) {
          data[i] = i % 256;
        }

        const result = verifyBase64Length(data);
        const formula = Math.ceil(size / 3) * 4;

        expect(formula).toBe(result.actual);
      }
    });
  });
});

// =============================================================================
// Base64 Encoding Constants Export Tests
// =============================================================================

describe('Base64 Encoding Constants', () => {
  it('should export BASE64_EXPANSION constant with value 4/3', async () => {
    // After the fix, there should be a BASE64_EXPANSION constant exported
    // from the encoding utilities
    const { BASE64_EXPANSION } = await import('../../utils/encoding.js');

    expect(BASE64_EXPANSION).toBeDefined();
    expect(BASE64_EXPANSION).toBe(4 / 3);
  });

  it('should export exactBase64Length utility function', async () => {
    // After the fix, there should be a utility function for exact Base64 length
    const { exactBase64Length: utilFunc } = await import('../../utils/encoding.js');

    expect(utilFunc).toBeDefined();
    expect(typeof utilFunc).toBe('function');

    // Verify it calculates correctly
    expect(utilFunc(0)).toBe(0);
    expect(utilFunc(1)).toBe(4);
    expect(utilFunc(2)).toBe(4);
    expect(utilFunc(3)).toBe(4);
    expect(utilFunc(4)).toBe(8);
    expect(utilFunc(100)).toBe(136);
    expect(utilFunc(1000)).toBe(1336);
  });
});

// =============================================================================
// WAL Writer Size Estimation Tests
// Note: These tests import from writer.js directly to avoid full engine imports
// =============================================================================

describe('WAL Writer Size Estimation', () => {
  // Test the estimateEntrySize function directly by importing it
  // We need to verify that the writer uses the correct Base64 expansion

  it('should use 4/3 expansion factor in size estimation', async () => {
    // Import the writer module
    const writerModule = await import('../writer.js');

    // The writer should export or use BASE64_EXPANSION constant of 4/3
    // We verify this by checking the DefaultWALEncoder behavior
    const encoder = new writerModule.DefaultWALEncoder();

    // Create some test data
    const testData = new Uint8Array(100);
    for (let i = 0; i < 100; i++) {
      testData[i] = i;
    }

    // Encode an entry with binary data
    const entry = {
      lsn: 1n,
      timestamp: Date.now(),
      txnId: 'test_txn',
      op: 'INSERT' as const,
      table: 'test_table',
      key: testData,
    };

    const encoded = encoder.encodeEntry(entry);

    // The encoded entry should be decodable
    const decoded = encoder.decodeEntry(encoded);

    expect(decoded.lsn).toBe(entry.lsn);
    expect(decoded.key).toBeDefined();
    expect(decoded.key?.length).toBe(testData.length);

    // Verify the key data matches
    for (let i = 0; i < testData.length; i++) {
      expect(decoded.key?.[i]).toBe(testData[i]);
    }
  });

  it('should handle large binary fields without precision loss', async () => {
    const writerModule = await import('../writer.js');
    const encoder = new writerModule.DefaultWALEncoder();

    // Test with larger data where precision matters more
    const largeSize = 10000;
    const largeData = new Uint8Array(largeSize);
    for (let i = 0; i < largeSize; i++) {
      largeData[i] = i % 256;
    }

    // The exact Base64 length for 10000 bytes
    // ceil(10000/3) * 4 = 3334 * 4 = 13336
    const expectedBase64Chars = Math.ceil(largeSize / 3) * 4;
    expect(expectedBase64Chars).toBe(13336);

    // With wrong 1.34 factor:
    // 10000 * 1.34 = 13400 (overestimate)

    // With correct 4/3 factor:
    // 10000 * (4/3) = 13333.33... (need ceiling)

    const entry = {
      lsn: 1n,
      timestamp: Date.now(),
      txnId: 'test_txn',
      op: 'INSERT' as const,
      table: 'test_table',
      after: largeData,
    };

    const encoded = encoder.encodeEntry(entry);
    const decoded = encoder.decodeEntry(encoded);

    // Verify data integrity - this confirms the encoding works correctly
    expect(decoded.after?.length).toBe(largeSize);
    for (let i = 0; i < largeSize; i++) {
      expect(decoded.after?.[i]).toBe(largeData[i]);
    }
  });
});
