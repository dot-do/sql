/**
 * B-tree Types for DoSQL Page Manager
 *
 * Core type definitions for B-tree pages and operations.
 * This implements a disk-based B+tree where:
 * - Internal nodes contain keys and child page pointers
 * - Leaf nodes contain keys and values
 * - All leaves are linked for efficient range scans
 */

/**
 * Page types in the B-tree
 */
export const enum PageType {
  /** Internal node - contains keys and child page IDs */
  INTERNAL = 0x01,
  /** Leaf node - contains keys and values */
  LEAF = 0x02,
  /** Overflow page - for values larger than inline storage */
  OVERFLOW = 0x03,
}

/**
 * A page in the B-tree
 *
 * Pages are stored in fsx blobs. Each page fits within the 2MB limit.
 * For a target of 100-1000 keys per page, with typical key sizes of 32-256 bytes,
 * this gives us plenty of room.
 */
export interface Page {
  /** Unique page identifier */
  id: number;

  /** Page type (leaf or internal) */
  type: PageType;

  /** Serialized keys - sorted in ascending order */
  keys: Uint8Array[];

  /** Serialized values - only present in leaf pages, parallel to keys array */
  values: Uint8Array[];

  /** Child page IDs - only present in internal pages
   *  For n keys, there are n+1 children:
   *  - children[i] points to keys < keys[i]
   *  - children[n] points to keys >= keys[n-1]
   */
  children: number[];

  /** Next page ID for leaf chain (for range scans), -1 if none */
  nextLeaf: number;

  /** Previous page ID for leaf chain, -1 if none */
  prevLeaf: number;
}

/**
 * Create an empty internal page
 */
export function createInternalPage(id: number): Page {
  return {
    id,
    type: PageType.INTERNAL,
    keys: [],
    values: [],
    children: [],
    nextLeaf: -1,
    prevLeaf: -1,
  };
}

/**
 * Create an empty leaf page
 */
export function createLeafPage(id: number): Page {
  return {
    id,
    type: PageType.LEAF,
    keys: [],
    values: [],
    children: [],
    nextLeaf: -1,
    prevLeaf: -1,
  };
}

/**
 * Page cache configuration
 */
export interface PageCacheConfig {
  /**
   * Maximum number of pages to cache in memory.
   * When this limit is reached, the least recently used pages are evicted.
   * Default: 1000 pages
   */
  maxPages: number;

  /**
   * Maximum cache size in bytes (optional).
   * If set, pages are evicted when total cached size exceeds this.
   * Takes precedence over maxPages when both would trigger eviction.
   */
  maxBytes?: number;

  /**
   * Callback invoked when a page is evicted from the cache.
   * This is called after the page has been flushed to storage (if dirty).
   * @param pageId - The ID of the evicted page
   * @param page - The evicted page object
   * @param dirty - Whether the page was dirty (had uncommitted writes)
   */
  onEvict?: (pageId: number, page: Page, dirty: boolean) => void | Promise<void>;
}

/**
 * Default page cache configuration
 */
export const DEFAULT_PAGE_CACHE_CONFIG: PageCacheConfig = {
  maxPages: 1000,
};

/**
 * Cache statistics for monitoring and debugging
 */
export interface CacheStats {
  /** Current number of pages in the cache */
  size: number;

  /** Maximum number of pages allowed in the cache */
  maxSize: number;

  /** Number of cache hits (page found in cache) */
  hits: number;

  /** Number of cache misses (page not found, had to load from storage) */
  misses: number;

  /** Number of pages evicted from cache */
  evictions: number;

  /** Cache hit rate (hits / (hits + misses)) */
  hitRate: number;

  /** Current cache size in bytes (if byte-based tracking is enabled) */
  currentBytes?: number;

  /** Maximum cache size in bytes (if byte limit is configured) */
  maxBytes?: number;
}

/**
 * B-tree configuration
 */
export interface BTreeConfig {
  /**
   * Minimum number of keys in a non-root node
   * The actual order (branching factor) is 2 * minKeys
   */
  minKeys: number;

  /**
   * Maximum number of keys in a node before it splits
   * For a B-tree of order m, maxKeys = m - 1
   */
  maxKeys: number;

  /**
   * Maximum inline value size in bytes
   * Values larger than this are stored in overflow pages
   */
  maxInlineValueSize: number;

  /**
   * Prefix for page storage keys
   */
  pagePrefix: string;

  /**
   * Page cache configuration for LRU eviction
   */
  cache?: PageCacheConfig;
}

/**
 * Default B-tree configuration
 *
 * With minKeys=50, maxKeys=100:
 * - ~100-200 keys per page
 * - Height of 4 can handle ~100M records
 * - Each page uses roughly 100KB-500KB with typical key/value sizes
 */
export const DEFAULT_BTREE_CONFIG: BTreeConfig = {
  minKeys: 50,
  maxKeys: 100,
  maxInlineValueSize: 4096,
  pagePrefix: 'btree/pages/',
};

/**
 * B-tree metadata stored in a dedicated key
 */
export interface BTreeMetadata {
  /** Root page ID */
  rootPageId: number;

  /** Current height of the tree */
  height: number;

  /** Total number of entries */
  entryCount: number;

  /** Next available page ID */
  nextPageId: number;

  /** Configuration snapshot */
  config: BTreeConfig;
}

/**
 * Key comparator function
 */
export type KeyComparator<K> = (a: K, b: K) => number;

/**
 * Key serializer/deserializer
 */
export interface KeyCodec<K> {
  /** Serialize a key to bytes */
  encode(key: K): Uint8Array;

  /** Deserialize a key from bytes */
  decode(bytes: Uint8Array): K;

  /** Compare two keys (for binary search) */
  compare(a: K, b: K): number;
}

/**
 * Value serializer/deserializer
 */
export interface ValueCodec<V> {
  /** Serialize a value to bytes */
  encode(value: V): Uint8Array;

  /** Deserialize a value from bytes */
  decode(bytes: Uint8Array): V;
}

/**
 * B-tree interface for row storage
 */
export interface BTree<K, V> {
  /**
   * Get a value by key
   * @param key - The key to look up
   * @returns The value or undefined if not found
   */
  get(key: K): Promise<V | undefined>;

  /**
   * Set a key-value pair
   * @param key - The key
   * @param value - The value
   */
  set(key: K, value: V): Promise<void>;

  /**
   * Delete a key
   * @param key - The key to delete
   * @returns True if the key was deleted, false if it didn't exist
   */
  delete(key: K): Promise<boolean>;

  /**
   * Iterate over a range of keys
   * @param start - Start key (inclusive)
   * @param end - End key (exclusive)
   * @yields Key-value pairs in sorted order
   */
  range(start: K, end: K): AsyncIterableIterator<[K, V]>;

  /**
   * Iterate over all entries
   * @yields Key-value pairs in sorted order
   */
  entries(): AsyncIterableIterator<[K, V]>;

  /**
   * Get the number of entries in the tree
   */
  count(): Promise<number>;

  /**
   * Clear all entries from the tree
   */
  clear(): Promise<void>;
}

/**
 * Result of a page split operation
 */
export interface SplitResult {
  /** The key that should be promoted to the parent */
  promotedKey: Uint8Array;

  /** The new page created by the split */
  newPage: Page;
}

/**
 * Search result within a page
 */
export interface PageSearchResult {
  /** Whether an exact match was found */
  found: boolean;

  /** Index where the key was found or should be inserted */
  index: number;
}

/**
 * Built-in codec for string keys
 */
export const StringKeyCodec: KeyCodec<string> = {
  encode(key: string): Uint8Array {
    return new TextEncoder().encode(key);
  },

  decode(bytes: Uint8Array): string {
    return new TextDecoder().decode(bytes);
  },

  compare(a: string, b: string): number {
    return a.localeCompare(b);
  },
};

/**
 * Built-in codec for numeric keys (as 64-bit big-endian)
 */
export const NumberKeyCodec: KeyCodec<number> = {
  encode(key: number): Uint8Array {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setFloat64(0, key, false); // big-endian for correct sort order
    return new Uint8Array(buffer);
  },

  decode(bytes: Uint8Array): number {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return view.getFloat64(0, false);
  },

  compare(a: number, b: number): number {
    return a - b;
  },
};

/**
 * Built-in codec for bigint keys (as 64-bit signed big-endian)
 */
export const BigIntKeyCodec: KeyCodec<bigint> = {
  encode(key: bigint): Uint8Array {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setBigInt64(0, key, false); // big-endian for correct sort order
    return new Uint8Array(buffer);
  },

  decode(bytes: Uint8Array): bigint {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return view.getBigInt64(0, false);
  },

  compare(a: bigint, b: bigint): number {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
  },
};

/**
 * Built-in codec for JSON-serializable values
 */
export const JsonValueCodec: ValueCodec<unknown> = {
  encode(value: unknown): Uint8Array {
    return new TextEncoder().encode(JSON.stringify(value));
  },

  decode(bytes: Uint8Array): unknown {
    return JSON.parse(new TextDecoder().decode(bytes));
  },
};

/**
 * Built-in codec for Uint8Array values (passthrough)
 */
export const BinaryValueCodec: ValueCodec<Uint8Array> = {
  encode(value: Uint8Array): Uint8Array {
    return value;
  },

  decode(bytes: Uint8Array): Uint8Array {
    return bytes;
  },
};
