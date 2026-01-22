/**
 * B-tree Page Manager for DoSQL
 *
 * A B+tree implementation for row-oriented storage using fsx blob storage.
 *
 * @example
 * ```typescript
 * import { createBTree, StringKeyCodec, JsonValueCodec } from '@dotdo/dosql/btree';
 * import { MemoryFSXBackend } from '@dotdo/dosql/fsx';
 *
 * // Create storage backend
 * const fsx = new MemoryFSXBackend();
 *
 * // Create B-tree with string keys and JSON values
 * const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec);
 * await tree.init();
 *
 * // Basic operations
 * await tree.set('user:1', { name: 'Alice', age: 30 });
 * const user = await tree.get('user:1');
 *
 * // Range scan
 * for await (const [key, value] of tree.range('user:', 'user:~')) {
 *   console.log(key, value);
 * }
 * ```
 *
 * @packageDocumentation
 */

// Core types
export {
  // Page types
  type Page,
  PageType,
  createInternalPage,
  createLeafPage,

  // B-tree types
  type BTree,
  type BTreeConfig,
  type BTreeMetadata,
  type PageCacheConfig,
  type CacheStats,
  DEFAULT_BTREE_CONFIG,
  DEFAULT_PAGE_CACHE_CONFIG,

  // Codec types
  type KeyCodec,
  type ValueCodec,
  type KeyComparator,

  // Split result
  type SplitResult,
  type PageSearchResult,

  // Built-in codecs
  StringKeyCodec,
  NumberKeyCodec,
  BigIntKeyCodec,
  JsonValueCodec,
  BinaryValueCodec,
} from './types.js';

// Page serialization
export {
  serializePage,
  deserializePage,
  calculatePageSize,
  wouldFit,
  binarySearch,
  compareBytes,
  MAX_PAGE_SIZE,
} from './page.js';

// B-tree implementation
export { BTreeImpl, createBTree, type BTreeExtended } from './btree.js';

// LRU Cache
export { LRUCache, type LRUCacheOptions, type SetOptions } from './lru-cache.js';
