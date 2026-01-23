/**
 * Crypto Utilities for DoSQL
 *
 * Shared cryptographic utility functions including:
 * - CRC32 checksum calculation
 */

// =============================================================================
// CRC32 IMPLEMENTATION
// =============================================================================

/**
 * CRC32 lookup table for checksum calculation.
 * Uses the standard IEEE 802.3 polynomial (0xEDB88320).
 */
const CRC32_TABLE: Uint32Array = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let j = 0; j < 8; j++) {
      crc = crc & 1 ? (crc >>> 1) ^ 0xedb88320 : crc >>> 1;
    }
    table[i] = crc >>> 0;
  }
  return table;
})();

/**
 * Calculate CRC32 checksum for data integrity verification.
 *
 * Uses the IEEE 802.3 polynomial (0xEDB88320) which is the standard
 * for Ethernet, zip files, and many other applications.
 *
 * @param data - The data to checksum
 * @returns 32-bit CRC checksum as an unsigned integer
 *
 * @example
 * ```ts
 * const data = new Uint8Array([0x48, 0x65, 0x6c, 0x6c, 0x6f]); // "Hello"
 * const checksum = crc32(data);
 * console.log(checksum.toString(16)); // "f7d18982"
 * ```
 */
export function crc32(data: Uint8Array): number {
  let crc = 0xffffffff;
  for (let i = 0; i < data.length; i++) {
    crc = CRC32_TABLE[(crc ^ data[i]) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

/**
 * Calculate CRC32 checksum for a string.
 * Convenience wrapper that handles encoding.
 *
 * @param str - The string to checksum
 * @returns 32-bit CRC checksum as an unsigned integer
 */
export function crc32String(str: string): number {
  const encoder = new TextEncoder();
  return crc32(encoder.encode(str));
}

/**
 * Verify data integrity by comparing checksums.
 *
 * @param data - The data to verify
 * @param expectedChecksum - The expected CRC32 checksum
 * @returns true if checksums match
 */
export function verifyCrc32(data: Uint8Array, expectedChecksum: number): boolean {
  return crc32(data) === expectedChecksum;
}
