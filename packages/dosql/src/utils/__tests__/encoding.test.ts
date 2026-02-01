/**
 * Encoding Utilities Tests
 *
 * Tests for string/binary encoding, JSON encoding/decoding,
 * byte length calculation, and Base64 length computation.
 */

import { describe, it, expect } from 'vitest';
import {
  textEncoder,
  textDecoder,
  encodeString,
  decodeString,
  encodeJson,
  decodeJson,
  getByteLength,
  BASE64_EXPANSION,
  exactBase64Length,
} from '../encoding.js';

// =============================================================================
// SHARED INSTANCES
// =============================================================================

describe('shared encoder/decoder instances', () => {
  it('should export a TextEncoder instance', () => {
    expect(textEncoder).toBeInstanceOf(TextEncoder);
  });

  it('should export a TextDecoder instance', () => {
    expect(textDecoder).toBeInstanceOf(TextDecoder);
  });
});

// =============================================================================
// STRING ENCODING/DECODING
// =============================================================================

describe('encodeString', () => {
  it('should encode ASCII string to Uint8Array', () => {
    const result = encodeString('Hello');
    expect(result).toBeInstanceOf(Uint8Array);
    expect(result).toEqual(new Uint8Array([0x48, 0x65, 0x6c, 0x6c, 0x6f]));
  });

  it('should encode empty string', () => {
    const result = encodeString('');
    expect(result.length).toBe(0);
  });

  it('should encode multi-byte UTF-8 characters', () => {
    const result = encodeString('\u00e9'); // e-acute (2 bytes in UTF-8)
    expect(result.length).toBe(2);
  });
});

describe('decodeString', () => {
  it('should decode Uint8Array to string', () => {
    const data = new Uint8Array([0x48, 0x65, 0x6c, 0x6c, 0x6f]);
    expect(decodeString(data)).toBe('Hello');
  });

  it('should decode empty array', () => {
    expect(decodeString(new Uint8Array([]))).toBe('');
  });

  it('should round-trip with encodeString', () => {
    const original = 'Hello, World!';
    expect(decodeString(encodeString(original))).toBe(original);
  });
});

// =============================================================================
// JSON ENCODING/DECODING
// =============================================================================

describe('encodeJson', () => {
  it('should encode object to JSON bytes', () => {
    const obj = { name: 'Alice', age: 30 };
    const result = encodeJson(obj);
    expect(result).toBeInstanceOf(Uint8Array);
    const decoded = JSON.parse(new TextDecoder().decode(result));
    expect(decoded).toEqual(obj);
  });

  it('should encode arrays', () => {
    const arr = [1, 2, 3];
    const result = encodeJson(arr);
    const decoded = JSON.parse(new TextDecoder().decode(result));
    expect(decoded).toEqual(arr);
  });

  it('should encode primitives', () => {
    const result = encodeJson(42);
    const decoded = JSON.parse(new TextDecoder().decode(result));
    expect(decoded).toBe(42);
  });
});

describe('decodeJson', () => {
  it('should decode JSON bytes to object', () => {
    const bytes = new TextEncoder().encode('{"name":"Alice","age":30}');
    const result = decodeJson<{ name: string; age: number }>(bytes);
    expect(result).toEqual({ name: 'Alice', age: 30 });
  });

  it('should round-trip with encodeJson', () => {
    const original = { nested: { value: [1, 2, 3] }, flag: true };
    const result = decodeJson(encodeJson(original));
    expect(result).toEqual(original);
  });

  it('should throw for invalid JSON', () => {
    const bytes = new TextEncoder().encode('not json');
    expect(() => decodeJson(bytes)).toThrow();
  });
});

// =============================================================================
// BYTE LENGTH
// =============================================================================

describe('getByteLength', () => {
  it('should return byte length for ASCII string', () => {
    expect(getByteLength('Hello')).toBe(5);
  });

  it('should return 0 for empty string', () => {
    expect(getByteLength('')).toBe(0);
  });

  it('should handle multi-byte characters correctly', () => {
    // e-acute is 2 bytes in UTF-8
    expect(getByteLength('\u00e9')).toBe(2);
  });

  it('should differ from string.length for multi-byte chars', () => {
    const str = '\u00e9'; // string.length is 1, byte length is 2
    expect(str.length).toBe(1);
    expect(getByteLength(str)).toBe(2);
  });

  it('should handle emoji (4 bytes in UTF-8)', () => {
    // Emoji are typically 4 bytes
    const emoji = '\u{1F600}'; // grinning face
    expect(getByteLength(emoji)).toBe(4);
  });
});

// =============================================================================
// BASE64 LENGTH
// =============================================================================

describe('BASE64_EXPANSION', () => {
  it('should be exactly 4/3', () => {
    expect(BASE64_EXPANSION).toBe(4 / 3);
  });
});

describe('exactBase64Length', () => {
  it('should return 0 for 0 bytes', () => {
    expect(exactBase64Length(0)).toBe(0);
  });

  it('should return 0 for negative input', () => {
    expect(exactBase64Length(-1)).toBe(0);
  });

  it('should return 4 for 1 byte', () => {
    expect(exactBase64Length(1)).toBe(4);
  });

  it('should return 4 for 2 bytes', () => {
    expect(exactBase64Length(2)).toBe(4);
  });

  it('should return 4 for 3 bytes', () => {
    expect(exactBase64Length(3)).toBe(4);
  });

  it('should return 8 for 4 bytes', () => {
    expect(exactBase64Length(4)).toBe(8);
  });

  it('should return 136 for 100 bytes', () => {
    expect(exactBase64Length(100)).toBe(136);
  });

  it('should return 1336 for 1000 bytes', () => {
    expect(exactBase64Length(1000)).toBe(1336);
  });

  it('should match actual btoa output length', () => {
    // Verify against actual Base64 encoding for a few sizes
    for (const size of [0, 1, 2, 3, 4, 5, 10, 100]) {
      if (size === 0) {
        expect(exactBase64Length(0)).toBe(0);
        continue;
      }
      const data = new Uint8Array(size);
      const base64 = btoa(String.fromCharCode(...data));
      expect(exactBase64Length(size)).toBe(base64.length);
    }
  });
});
