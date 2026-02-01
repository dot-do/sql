/**
 * Crypto Utilities Tests
 *
 * Tests for CRC32 checksum calculation and verification.
 */

import { describe, it, expect } from 'vitest';
import { crc32, crc32String, verifyCrc32 } from '../crypto.js';

describe('crc32', () => {
  it('should return 0 for empty data', () => {
    const result = crc32(new Uint8Array([]));
    expect(result).toBe(0);
  });

  it('should compute CRC32 for "Hello"', () => {
    // "Hello" = [0x48, 0x65, 0x6c, 0x6c, 0x6f]
    const data = new Uint8Array([0x48, 0x65, 0x6c, 0x6c, 0x6f]);
    const checksum = crc32(data);
    // Known CRC32 for "Hello" using IEEE polynomial
    expect(checksum).toBe(0xf7d18982);
  });

  it('should produce consistent results for same input', () => {
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    expect(crc32(data)).toBe(crc32(data));
  });

  it('should produce different results for different inputs', () => {
    const data1 = new Uint8Array([1, 2, 3]);
    const data2 = new Uint8Array([4, 5, 6]);
    expect(crc32(data1)).not.toBe(crc32(data2));
  });

  it('should return an unsigned 32-bit integer', () => {
    const data = new Uint8Array([0xff, 0xff, 0xff, 0xff]);
    const result = crc32(data);
    expect(result).toBeGreaterThanOrEqual(0);
    expect(result).toBeLessThanOrEqual(0xffffffff);
  });

  it('should handle single byte', () => {
    const result = crc32(new Uint8Array([0x00]));
    expect(typeof result).toBe('number');
    expect(result).toBeGreaterThanOrEqual(0);
  });
});

describe('crc32String', () => {
  it('should compute CRC32 for a string', () => {
    const checksum = crc32String('Hello');
    expect(checksum).toBe(0xf7d18982);
  });

  it('should handle empty string', () => {
    const result = crc32String('');
    expect(result).toBe(0);
  });

  it('should handle multi-byte UTF-8 characters', () => {
    const result = crc32String('\u00e9'); // e-acute
    expect(typeof result).toBe('number');
    expect(result).toBeGreaterThanOrEqual(0);
  });

  it('should match manual encoding', () => {
    const str = 'test';
    const encoder = new TextEncoder();
    expect(crc32String(str)).toBe(crc32(encoder.encode(str)));
  });
});

describe('verifyCrc32', () => {
  it('should return true for matching checksum', () => {
    const data = new Uint8Array([0x48, 0x65, 0x6c, 0x6c, 0x6f]); // "Hello"
    const checksum = crc32(data);
    expect(verifyCrc32(data, checksum)).toBe(true);
  });

  it('should return false for non-matching checksum', () => {
    const data = new Uint8Array([0x48, 0x65, 0x6c, 0x6c, 0x6f]);
    expect(verifyCrc32(data, 0x12345678)).toBe(false);
  });

  it('should detect data corruption', () => {
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    const checksum = crc32(data);

    // Corrupt one byte
    const corrupted = new Uint8Array(data);
    corrupted[2] = 99;
    expect(verifyCrc32(corrupted, checksum)).toBe(false);
  });
});
