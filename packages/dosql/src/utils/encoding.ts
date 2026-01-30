/**
 * Encoding Utilities for DoSQL
 *
 * Provides shared TextEncoder/TextDecoder instances and encoding utilities
 * to avoid repeated instantiation across the codebase.
 *
 * TextEncoder/TextDecoder are expensive to instantiate, so we provide
 * shared instances that can be reused throughout the application.
 */

// =============================================================================
// SHARED ENCODER/DECODER INSTANCES
// =============================================================================

/**
 * Shared TextEncoder instance for UTF-8 encoding.
 *
 * Use this instead of creating new TextEncoder() instances to avoid
 * repeated allocation overhead.
 *
 * @example
 * ```ts
 * import { textEncoder } from '../utils/encoding.js';
 *
 * const bytes = textEncoder.encode('Hello, World!');
 * ```
 */
export const textEncoder = new TextEncoder();

/**
 * Shared TextDecoder instance for UTF-8 decoding.
 *
 * Use this instead of creating new TextDecoder() instances to avoid
 * repeated allocation overhead.
 *
 * @example
 * ```ts
 * import { textDecoder } from '../utils/encoding.js';
 *
 * const str = textDecoder.decode(bytes);
 * ```
 */
export const textDecoder = new TextDecoder();

// =============================================================================
// ENCODING UTILITIES
// =============================================================================

/**
 * Encode a string to a Uint8Array using UTF-8.
 *
 * Convenience function that uses the shared encoder.
 *
 * @param str - The string to encode
 * @returns UTF-8 encoded bytes
 */
export function encodeString(str: string): Uint8Array {
  return textEncoder.encode(str);
}

/**
 * Decode a Uint8Array to a string using UTF-8.
 *
 * Convenience function that uses the shared decoder.
 *
 * @param data - The bytes to decode
 * @returns Decoded string
 */
export function decodeString(data: BufferSource): string {
  return textDecoder.decode(data);
}

/**
 * Encode an object to JSON bytes.
 *
 * @param obj - The object to encode
 * @returns UTF-8 encoded JSON bytes
 */
export function encodeJson<T>(obj: T): Uint8Array {
  return textEncoder.encode(JSON.stringify(obj));
}

/**
 * Decode JSON bytes to an object.
 *
 * @param data - The bytes to decode
 * @returns Parsed object
 */
export function decodeJson<T>(data: BufferSource): T {
  const str = textDecoder.decode(data);
  return JSON.parse(str) as T;
}

/**
 * Get the byte length of a string when encoded as UTF-8.
 *
 * This is more accurate than string.length for strings containing
 * multi-byte characters.
 *
 * @param str - The string to measure
 * @returns Byte length when encoded as UTF-8
 */
export function getByteLength(str: string): number {
  return textEncoder.encode(str).length;
}

// =============================================================================
// BASE64 ENCODING UTILITIES
// =============================================================================

/**
 * The exact Base64 expansion ratio.
 *
 * Base64 encoding converts 3 bytes of binary data into 4 ASCII characters.
 * The expansion ratio is therefore exactly 4/3 (approximately 1.333...).
 *
 * IMPORTANT: Do NOT use approximations like 1.34 for size calculations.
 * The correct formula for Base64 encoded length is: ceil(inputLength / 3) * 4
 *
 * @example
 * ```ts
 * // Correct usage for size estimation:
 * const estimatedSize = Math.ceil(byteLength / 3) * 4;
 *
 * // Or use the exactBase64Length utility function:
 * const estimatedSize = exactBase64Length(byteLength);
 * ```
 */
export const BASE64_EXPANSION = 4 / 3;

/**
 * Calculate the exact Base64 encoded length for a given input byte length.
 *
 * Base64 encoding works by:
 * 1. Taking 3 bytes of input (24 bits)
 * 2. Splitting into 4 groups of 6 bits each
 * 3. Encoding each 6-bit group as one Base64 character
 *
 * When the input length is not divisible by 3, padding ('=') is added:
 * - 1 byte input -> 4 chars (2 padding '=')
 * - 2 bytes input -> 4 chars (1 padding '=')
 * - 3 bytes input -> 4 chars (0 padding)
 *
 * The formula is: ceil(inputLength / 3) * 4
 *
 * @param inputLength - The number of bytes to be Base64 encoded
 * @returns The exact length of the Base64 encoded string
 *
 * @example
 * ```ts
 * exactBase64Length(0)    // 0
 * exactBase64Length(1)    // 4  (3 bytes of padding)
 * exactBase64Length(2)    // 4  (2 bytes of padding)
 * exactBase64Length(3)    // 4  (no padding)
 * exactBase64Length(4)    // 8
 * exactBase64Length(100)  // 136
 * exactBase64Length(1000) // 1336
 * ```
 */
export function exactBase64Length(inputLength: number): number {
  if (inputLength <= 0) return 0;
  return Math.ceil(inputLength / 3) * 4;
}
