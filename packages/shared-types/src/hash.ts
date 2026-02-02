/**
 * Hash Functions Module
 *
 * Centralized hash implementations shared across DoSQL and DoLake packages.
 * Provides FNV-1a and xxHash-inspired algorithms for:
 * - Sharding/vindex operations
 * - Row deduplication
 * - Content addressing
 * - Bucket partitioning
 *
 * @packageDocumentation
 */

// =============================================================================
// FNV-1a CONSTANTS
// =============================================================================

/**
 * FNV-1a offset basis (32-bit)
 */
export const FNV_OFFSET_BASIS = 2166136261;

/**
 * FNV-1a prime (32-bit)
 */
export const FNV_PRIME = 16777619;

// =============================================================================
// FNV-1a HASH IMPLEMENTATION
// =============================================================================

/**
 * FNV-1a hash implementation
 *
 * Fast and provides good distribution for sharding, content addressing,
 * and general-purpose hashing.
 *
 * @param input - String, number, or bigint to hash
 * @returns 32-bit unsigned integer hash
 *
 * @example
 * ```typescript
 * const hash1 = fnv1a('user-123');
 * const hash2 = fnv1a(12345);
 * const hash3 = fnv1a(BigInt('9007199254740993'));
 * ```
 */
export function fnv1a(input: string | number | bigint): number {
  const str = String(input);
  let hash = FNV_OFFSET_BASIS;

  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    // FNV prime: 16777619
    // Use multiplication that works within 32-bit range
    hash = Math.imul(hash, FNV_PRIME);
  }

  // Ensure positive 32-bit integer
  return hash >>> 0;
}

/**
 * FNV-1a hash for strings with starting hash value
 *
 * Useful for incremental hashing where you want to continue
 * from a previous hash state.
 *
 * @param str - String to hash
 * @param hash - Starting hash value
 * @returns Updated 32-bit unsigned integer hash
 */
export function fnv1aString(str: string, hash: number): number {
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, FNV_PRIME) >>> 0;
  }
  return hash;
}

/**
 * FNV-1a hash for a number with starting hash value
 *
 * Handles integers and floats by converting to bytes representation.
 *
 * @param num - Number to hash
 * @param hash - Starting hash value
 * @returns Updated 32-bit unsigned integer hash
 */
export function fnv1aNumber(num: number, hash: number): number {
  // XOR with type marker and hash the number's bits
  hash ^= 0x4e; // 'N' for number
  hash = Math.imul(hash, FNV_PRIME) >>> 0;

  if (Number.isInteger(num) && num >= -2147483648 && num <= 2147483647) {
    // Small integer: hash directly
    hash ^= (num >>> 0) & 0xff;
    hash = Math.imul(hash, FNV_PRIME) >>> 0;
    hash ^= (num >>> 8) & 0xff;
    hash = Math.imul(hash, FNV_PRIME) >>> 0;
    hash ^= (num >>> 16) & 0xff;
    hash = Math.imul(hash, FNV_PRIME) >>> 0;
    hash ^= (num >>> 24) & 0xff;
    hash = Math.imul(hash, FNV_PRIME) >>> 0;
  } else {
    // Float or large number: use string representation to preserve precision
    hash = fnv1aString(String(num), hash);
  }
  return hash;
}

/**
 * FNV-1a hash for a bigint with starting hash value
 *
 * @param n - BigInt to hash
 * @param hash - Starting hash value
 * @returns Updated 32-bit unsigned integer hash
 */
export function fnv1aBigInt(n: bigint, hash: number): number {
  hash ^= 0x42; // 'B' for bigint
  hash = Math.imul(hash, FNV_PRIME) >>> 0;
  // Hash the string representation (bigints can be arbitrarily large)
  return fnv1aString(n.toString(), hash);
}

/**
 * FNV-1a hash for a Uint8Array with starting hash value
 *
 * @param bytes - Byte array to hash
 * @param hash - Starting hash value
 * @returns Updated 32-bit unsigned integer hash
 */
export function fnv1aBytes(bytes: Uint8Array, hash: number): number {
  hash ^= 0x59; // 'Y' for bytes
  hash = Math.imul(hash, FNV_PRIME) >>> 0;
  for (const byte of bytes) {
    hash ^= byte;
    hash = Math.imul(hash, FNV_PRIME) >>> 0;
  }
  return hash;
}

// =============================================================================
// XXHASH IMPLEMENTATION
// =============================================================================

/**
 * xxHash-inspired fast hash (simplified 32-bit version)
 *
 * Faster than FNV-1a for longer strings.
 *
 * @param input - String, number, or bigint to hash
 * @returns 32-bit unsigned integer hash
 *
 * @example
 * ```typescript
 * const hash = xxhash('long-email@example.com');
 * ```
 */
export function xxhash(input: string | number | bigint): number {
  const str = String(input);
  const PRIME32_1 = 2654435761;
  const PRIME32_2 = 2246822519;
  const PRIME32_3 = 3266489917;
  const PRIME32_4 = 668265263;
  const PRIME32_5 = 374761393;

  let h32: number;
  let index = 0;
  const len = str.length;

  if (len >= 16) {
    let v1 = (0 + PRIME32_1 + PRIME32_2) | 0;
    let v2 = (0 + PRIME32_2) | 0;
    let v3 = 0;
    let v4 = (0 - PRIME32_1) | 0;

    const limit = len - 16;
    do {
      const c1 =
        str.charCodeAt(index) |
        (str.charCodeAt(index + 1) << 8) |
        (str.charCodeAt(index + 2) << 16) |
        (str.charCodeAt(index + 3) << 24);
      v1 = Math.imul(v1 + Math.imul(c1, PRIME32_2), PRIME32_1);
      v1 = ((v1 << 13) | (v1 >>> 19)) * PRIME32_1;
      index += 4;

      const c2 =
        str.charCodeAt(index) |
        (str.charCodeAt(index + 1) << 8) |
        (str.charCodeAt(index + 2) << 16) |
        (str.charCodeAt(index + 3) << 24);
      v2 = Math.imul(v2 + Math.imul(c2, PRIME32_2), PRIME32_1);
      v2 = ((v2 << 13) | (v2 >>> 19)) * PRIME32_1;
      index += 4;

      const c3 =
        str.charCodeAt(index) |
        (str.charCodeAt(index + 1) << 8) |
        (str.charCodeAt(index + 2) << 16) |
        (str.charCodeAt(index + 3) << 24);
      v3 = Math.imul(v3 + Math.imul(c3, PRIME32_2), PRIME32_1);
      v3 = ((v3 << 13) | (v3 >>> 19)) * PRIME32_1;
      index += 4;

      const c4 =
        str.charCodeAt(index) |
        (str.charCodeAt(index + 1) << 8) |
        (str.charCodeAt(index + 2) << 16) |
        (str.charCodeAt(index + 3) << 24);
      v4 = Math.imul(v4 + Math.imul(c4, PRIME32_2), PRIME32_1);
      v4 = ((v4 << 13) | (v4 >>> 19)) * PRIME32_1;
      index += 4;
    } while (index <= limit);

    h32 =
      ((v1 << 1) | (v1 >>> 31)) +
      ((v2 << 7) | (v2 >>> 25)) +
      ((v3 << 12) | (v3 >>> 20)) +
      ((v4 << 18) | (v4 >>> 14));
  } else {
    h32 = 0 + PRIME32_5;
  }

  h32 += len;

  while (index + 4 <= len) {
    const c =
      str.charCodeAt(index) |
      (str.charCodeAt(index + 1) << 8) |
      (str.charCodeAt(index + 2) << 16) |
      (str.charCodeAt(index + 3) << 24);
    h32 = Math.imul(h32 + Math.imul(c, PRIME32_3), PRIME32_4);
    h32 = (h32 << 17) | (h32 >>> 15);
    index += 4;
  }

  while (index < len) {
    h32 = Math.imul(h32 + Math.imul(str.charCodeAt(index), PRIME32_5), PRIME32_1);
    h32 = (h32 << 11) | (h32 >>> 21);
    index++;
  }

  h32 ^= h32 >>> 15;
  h32 = Math.imul(h32, PRIME32_2);
  h32 ^= h32 >>> 13;
  h32 = Math.imul(h32, PRIME32_3);
  h32 ^= h32 >>> 16;

  return h32 >>> 0;
}

// =============================================================================
// HASH FUNCTION SELECTOR
// =============================================================================

/**
 * Hash algorithm type
 */
export type HashAlgorithm = 'fnv1a' | 'xxhash';

/**
 * Select hash function based on algorithm name
 *
 * @param algorithm - 'fnv1a' or 'xxhash'
 * @returns Hash function
 *
 * @example
 * ```typescript
 * const hashFn = getHashFunction('xxhash');
 * const hash = hashFn('my-key');
 * ```
 */
export function getHashFunction(algorithm: HashAlgorithm): (input: string | number | bigint) => number {
  return algorithm === 'xxhash' ? xxhash : fnv1a;
}
