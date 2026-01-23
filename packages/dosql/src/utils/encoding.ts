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
