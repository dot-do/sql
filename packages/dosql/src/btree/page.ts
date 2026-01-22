/**
 * B-tree Page Serialization
 *
 * Binary format for storing B-tree pages within the 2MB fsx blob limit.
 * Optimized for ~100-1000 keys per page with typical key sizes of 32-256 bytes.
 *
 * Page Format:
 * ============================================================================
 * Header (32 bytes):
 *   [0-3]   Magic number (4 bytes): 0x42545045 ("BTPE")
 *   [4-7]   Version (4 bytes): 1
 *   [8-11]  Page ID (4 bytes)
 *   [12]    Page type (1 byte): 0x01=internal, 0x02=leaf, 0x03=overflow
 *   [13-15] Reserved (3 bytes)
 *   [16-19] Key count (4 bytes)
 *   [20-23] Next leaf page ID (4 bytes, -1 if none)
 *   [24-27] Prev leaf page ID (4 bytes, -1 if none)
 *   [28-31] Reserved (4 bytes)
 *
 * Key Directory (8 bytes per key):
 *   [0-3]   Key offset from start of key data section
 *   [4-7]   Key length
 *
 * Value Directory (leaf pages only, 8 bytes per value):
 *   [0-3]   Value offset from start of value data section
 *   [4-7]   Value length
 *
 * Children Directory (internal pages only, 4 bytes per child):
 *   [0-3]   Child page ID
 *
 * Key Data Section:
 *   Variable-length key data packed contiguously
 *
 * Value Data Section (leaf pages only):
 *   Variable-length value data packed contiguously
 */

import { Page, PageType, createInternalPage, createLeafPage } from './types.js';

/** Magic number for page files */
const PAGE_MAGIC = 0x42545045; // "BTPE" in ASCII

/** Current page format version */
const PAGE_VERSION = 1;

/** Header size in bytes */
const HEADER_SIZE = 32;

/** Maximum page size (must fit in 2MB blob) */
export const MAX_PAGE_SIZE = 2 * 1024 * 1024;

/**
 * Serialize a page to binary format
 *
 * @param page - The page to serialize
 * @returns Binary representation of the page
 * @throws Error if the page exceeds the maximum size
 */
export function serializePage(page: Page): Uint8Array {
  const keyCount = page.keys.length;

  // Calculate sizes
  const keyDirSize = keyCount * 8;
  const valueDirSize = page.type === PageType.LEAF ? keyCount * 8 : 0;
  const childrenDirSize = page.type === PageType.INTERNAL ? (keyCount + 1) * 4 : 0;

  let totalKeyDataSize = 0;
  for (const key of page.keys) {
    totalKeyDataSize += key.byteLength;
  }

  let totalValueDataSize = 0;
  if (page.type === PageType.LEAF) {
    for (const value of page.values) {
      totalValueDataSize += value.byteLength;
    }
  }

  const totalSize =
    HEADER_SIZE +
    keyDirSize +
    valueDirSize +
    childrenDirSize +
    totalKeyDataSize +
    totalValueDataSize;

  if (totalSize > MAX_PAGE_SIZE) {
    throw new Error(
      `Page size ${totalSize} exceeds maximum ${MAX_PAGE_SIZE}. ` +
        `Keys: ${keyCount}, Key data: ${totalKeyDataSize}, Value data: ${totalValueDataSize}`
    );
  }

  const buffer = new ArrayBuffer(totalSize);
  const view = new DataView(buffer);
  const bytes = new Uint8Array(buffer);

  // Write header
  view.setUint32(0, PAGE_MAGIC, false);
  view.setUint32(4, PAGE_VERSION, false);
  view.setUint32(8, page.id, false);
  view.setUint8(12, page.type);
  // bytes 13-15 reserved
  view.setUint32(16, keyCount, false);
  view.setInt32(20, page.nextLeaf, false);
  view.setInt32(24, page.prevLeaf, false);
  // bytes 28-31 reserved

  let offset = HEADER_SIZE;

  // Write key directory and collect key offsets
  const keyDataStart =
    HEADER_SIZE + keyDirSize + valueDirSize + childrenDirSize;
  let keyDataOffset = 0;

  for (let i = 0; i < keyCount; i++) {
    const key = page.keys[i];
    view.setUint32(offset, keyDataOffset, false);
    view.setUint32(offset + 4, key.byteLength, false);
    offset += 8;
    keyDataOffset += key.byteLength;
  }

  // Write value directory (leaf pages only)
  if (page.type === PageType.LEAF) {
    const valueDataStart = keyDataStart + totalKeyDataSize;
    let valueDataOffset = 0;

    for (let i = 0; i < keyCount; i++) {
      const value = page.values[i];
      view.setUint32(offset, valueDataOffset, false);
      view.setUint32(offset + 4, value.byteLength, false);
      offset += 8;
      valueDataOffset += value.byteLength;
    }
  }

  // Write children directory (internal pages only)
  if (page.type === PageType.INTERNAL) {
    for (let i = 0; i <= keyCount; i++) {
      view.setUint32(offset, page.children[i], false);
      offset += 4;
    }
  }

  // Write key data
  for (const key of page.keys) {
    bytes.set(key, offset);
    offset += key.byteLength;
  }

  // Write value data (leaf pages only)
  if (page.type === PageType.LEAF) {
    for (const value of page.values) {
      bytes.set(value, offset);
      offset += value.byteLength;
    }
  }

  return bytes;
}

/**
 * Deserialize a page from binary format
 *
 * @param bytes - Binary data to deserialize
 * @returns The deserialized page
 * @throws Error if the data is invalid
 */
export function deserializePage(bytes: Uint8Array): Page {
  if (bytes.byteLength < HEADER_SIZE) {
    throw new Error(`Page data too small: ${bytes.byteLength} bytes`);
  }

  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

  // Validate header
  const magic = view.getUint32(0, false);
  if (magic !== PAGE_MAGIC) {
    throw new Error(`Invalid page magic: 0x${magic.toString(16)}`);
  }

  const version = view.getUint32(4, false);
  if (version !== PAGE_VERSION) {
    throw new Error(`Unsupported page version: ${version}`);
  }

  const id = view.getUint32(8, false);
  const type = view.getUint8(12) as PageType;
  const keyCount = view.getUint32(16, false);
  const nextLeaf = view.getInt32(20, false);
  const prevLeaf = view.getInt32(24, false);

  // Create the appropriate page type
  const page: Page =
    type === PageType.INTERNAL ? createInternalPage(id) : createLeafPage(id);
  page.nextLeaf = nextLeaf;
  page.prevLeaf = prevLeaf;

  let offset = HEADER_SIZE;

  // Calculate section sizes
  const keyDirSize = keyCount * 8;
  const valueDirSize = type === PageType.LEAF ? keyCount * 8 : 0;
  const childrenDirSize = type === PageType.INTERNAL ? (keyCount + 1) * 4 : 0;

  const keyDataStart =
    HEADER_SIZE + keyDirSize + valueDirSize + childrenDirSize;

  // Read key directory entries
  const keyEntries: Array<{ offset: number; length: number }> = [];
  for (let i = 0; i < keyCount; i++) {
    keyEntries.push({
      offset: view.getUint32(offset, false),
      length: view.getUint32(offset + 4, false),
    });
    offset += 8;
  }

  // Read value directory entries (leaf pages only)
  const valueEntries: Array<{ offset: number; length: number }> = [];
  if (type === PageType.LEAF) {
    for (let i = 0; i < keyCount; i++) {
      valueEntries.push({
        offset: view.getUint32(offset, false),
        length: view.getUint32(offset + 4, false),
      });
      offset += 8;
    }
  }

  // Read children (internal pages only)
  if (type === PageType.INTERNAL) {
    for (let i = 0; i <= keyCount; i++) {
      page.children.push(view.getUint32(offset, false));
      offset += 4;
    }
  }

  // Calculate value data start
  let totalKeyDataSize = 0;
  for (const entry of keyEntries) {
    totalKeyDataSize += entry.length;
  }
  const valueDataStart = keyDataStart + totalKeyDataSize;

  // Read keys
  for (const entry of keyEntries) {
    const start = keyDataStart + entry.offset;
    page.keys.push(bytes.slice(start, start + entry.length));
  }

  // Read values (leaf pages only)
  if (type === PageType.LEAF) {
    for (const entry of valueEntries) {
      const start = valueDataStart + entry.offset;
      page.values.push(bytes.slice(start, start + entry.length));
    }
  }

  return page;
}

/**
 * Calculate the serialized size of a page without actually serializing it
 *
 * @param page - The page to measure
 * @returns The size in bytes
 */
export function calculatePageSize(page: Page): number {
  const keyCount = page.keys.length;
  const keyDirSize = keyCount * 8;
  const valueDirSize = page.type === PageType.LEAF ? keyCount * 8 : 0;
  const childrenDirSize =
    page.type === PageType.INTERNAL ? (keyCount + 1) * 4 : 0;

  let totalKeyDataSize = 0;
  for (const key of page.keys) {
    totalKeyDataSize += key.byteLength;
  }

  let totalValueDataSize = 0;
  if (page.type === PageType.LEAF) {
    for (const value of page.values) {
      totalValueDataSize += value.byteLength;
    }
  }

  return (
    HEADER_SIZE +
    keyDirSize +
    valueDirSize +
    childrenDirSize +
    totalKeyDataSize +
    totalValueDataSize
  );
}

/**
 * Check if adding a key-value pair would exceed page size limits
 *
 * @param page - The page to check
 * @param keySize - Size of the key to add in bytes
 * @param valueSize - Size of the value to add in bytes (0 for internal pages)
 * @returns True if the addition would fit
 */
export function wouldFit(
  page: Page,
  keySize: number,
  valueSize: number
): boolean {
  const currentSize = calculatePageSize(page);
  const additionalSize =
    8 + // key directory entry
    keySize +
    (page.type === PageType.LEAF ? 8 + valueSize : 4); // value dir + data or child pointer

  return currentSize + additionalSize <= MAX_PAGE_SIZE;
}

/**
 * Binary search for a key in a sorted array of serialized keys
 *
 * @param keys - Sorted array of serialized keys
 * @param target - The target key to find
 * @param compare - Comparison function for raw bytes
 * @returns Object with `found` boolean and `index` where key was found or should be inserted
 */
export function binarySearch(
  keys: Uint8Array[],
  target: Uint8Array,
  compare: (a: Uint8Array, b: Uint8Array) => number
): { found: boolean; index: number } {
  let low = 0;
  let high = keys.length - 1;

  while (low <= high) {
    const mid = (low + high) >>> 1;
    const cmp = compare(keys[mid], target);

    if (cmp < 0) {
      low = mid + 1;
    } else if (cmp > 0) {
      high = mid - 1;
    } else {
      return { found: true, index: mid };
    }
  }

  return { found: false, index: low };
}

/**
 * Compare two Uint8Arrays lexicographically
 */
export function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const minLen = Math.min(a.length, b.length);
  for (let i = 0; i < minLen; i++) {
    if (a[i] < b[i]) return -1;
    if (a[i] > b[i]) return 1;
  }
  return a.length - b.length;
}
