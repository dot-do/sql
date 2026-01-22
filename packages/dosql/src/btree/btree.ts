/**
 * B-tree Implementation for DoSQL
 *
 * A B+tree implementation for row storage in Durable Objects.
 * Uses fsx for page persistence with the following characteristics:
 *
 * - Keys and values are serialized using pluggable codecs
 * - Pages are stored as fsx blobs with configurable prefixes
 * - Leaf pages are linked for efficient range scans
 * - Supports concurrent reads (single-writer assumed)
 */

import type { FSXBackend } from '../fsx/types.js';
import {
  Page,
  PageType,
  BTree,
  BTreeConfig,
  BTreeMetadata,
  KeyCodec,
  ValueCodec,
  DEFAULT_BTREE_CONFIG,
  DEFAULT_PAGE_CACHE_CONFIG,
  SplitResult,
  createInternalPage,
  createLeafPage,
  CacheStats,
} from './types.js';
import {
  serializePage,
  deserializePage,
  calculatePageSize,
  binarySearch,
  wouldFit,
} from './page.js';
import { LRUCache } from './lru-cache.js';

/**
 * Metadata storage key
 */
const METADATA_KEY = '_meta';

/**
 * B+tree implementation
 */
export class BTreeImpl<K, V> implements BTree<K, V> {
  private readonly fsx: FSXBackend;
  private readonly keyCodec: KeyCodec<K>;
  private readonly valueCodec: ValueCodec<V>;
  private readonly config: BTreeConfig;
  private readonly userOnEvict?: (pageId: number, page: Page, dirty: boolean) => void | Promise<void>;

  private metadata: BTreeMetadata | null = null;
  private readonly pageCache: LRUCache<number, Page>;

  constructor(
    fsx: FSXBackend,
    keyCodec: KeyCodec<K>,
    valueCodec: ValueCodec<V>,
    config: Partial<BTreeConfig> = {}
  ) {
    this.fsx = fsx;
    this.keyCodec = keyCodec;
    this.valueCodec = valueCodec;
    this.config = { ...DEFAULT_BTREE_CONFIG, ...config };

    // Store user-provided eviction callback
    const cacheConfig = this.config.cache ?? DEFAULT_PAGE_CACHE_CONFIG;
    this.userOnEvict = cacheConfig.onEvict;

    // Initialize LRU page cache
    this.pageCache = new LRUCache<number, Page>({
      maxSize: cacheConfig.maxBytes ?? cacheConfig.maxPages,
      sizeCalculator: cacheConfig.maxBytes
        ? (page) => calculatePageSize(page)
        : undefined,
      onEvict: async (pageId, page, dirty) => {
        // Write back dirty pages before eviction
        if (dirty) {
          const data = serializePage(page);
          await this.fsx.write(this.pageKey(pageId), data);
        }
        // Call user-provided eviction callback
        if (this.userOnEvict) {
          await this.userOnEvict(pageId, page, dirty);
        }
      },
    });
  }

  /**
   * Initialize the B-tree (load or create metadata)
   */
  async init(): Promise<void> {
    await this.loadMetadata();
  }

  /**
   * Load metadata from storage, creating it if it doesn't exist
   */
  private async loadMetadata(): Promise<void> {
    const key = this.config.pagePrefix + METADATA_KEY;
    const data = await this.fsx.read(key);

    if (data) {
      const json = new TextDecoder().decode(data);
      this.metadata = JSON.parse(json);
    } else {
      // Create a new empty tree
      const rootPage = createLeafPage(0);
      await this.writePage(rootPage);

      this.metadata = {
        rootPageId: 0,
        height: 1,
        entryCount: 0,
        nextPageId: 1,
        config: this.config,
      };
      await this.saveMetadata();
    }
  }

  /**
   * Save metadata to storage
   */
  private async saveMetadata(): Promise<void> {
    if (!this.metadata) return;
    const key = this.config.pagePrefix + METADATA_KEY;
    const data = new TextEncoder().encode(JSON.stringify(this.metadata));
    await this.fsx.write(key, data);
  }

  /**
   * Get the storage key for a page
   */
  private pageKey(pageId: number): string {
    return `${this.config.pagePrefix}page_${pageId.toString(16).padStart(8, '0')}`;
  }

  /**
   * Read a page from storage or cache
   */
  private async readPage(pageId: number): Promise<Page> {
    // Check cache first (get() tracks hits internally)
    let page = this.pageCache.get(pageId);
    if (page) return page;

    // Cache miss - record it and read from storage
    this.pageCache.recordMiss();
    const data = await this.fsx.read(this.pageKey(pageId));
    if (!data) {
      throw new Error(`Page ${pageId} not found`);
    }

    page = deserializePage(data);
    this.pageCache.set(pageId, page);
    return page;
  }

  /**
   * Write a page to storage and cache
   */
  private async writePage(page: Page): Promise<void> {
    const data = serializePage(page);
    await this.fsx.write(this.pageKey(page.id), data);
    // Add to cache as clean (not dirty since we just wrote it)
    this.pageCache.set(page.id, page, { dirty: false });
  }

  /**
   * Mark a page as dirty (needs writing)
   */
  private markDirty(page: Page): void {
    this.pageCache.set(page.id, page, { dirty: true });
  }

  /**
   * Flush all dirty pages to storage
   */
  private async flush(): Promise<void> {
    const dirtyKeys = this.pageCache.getDirtyKeys();
    for (const pageId of dirtyKeys) {
      const page = this.pageCache.peek(pageId); // Use peek to avoid updating LRU order
      if (page) {
        const data = serializePage(page);
        await this.fsx.write(this.pageKey(pageId), data);
        this.pageCache.markClean(pageId);
      }
    }
    await this.saveMetadata();
  }

  /**
   * Allocate a new page ID
   */
  private allocPageId(): number {
    if (!this.metadata) {
      throw new Error('B-tree not initialized');
    }
    return this.metadata.nextPageId++;
  }

  /**
   * Compare two serialized keys
   */
  private compareKeys(a: Uint8Array, b: Uint8Array): number {
    return this.keyCodec.compare(
      this.keyCodec.decode(a),
      this.keyCodec.decode(b)
    );
  }

  /**
   * Get a value by key
   */
  async get(key: K): Promise<V | undefined> {
    if (!this.metadata) await this.loadMetadata();
    if (!this.metadata) throw new Error('Failed to load metadata');

    const serializedKey = this.keyCodec.encode(key);
    const leafPage = await this.findLeaf(serializedKey);

    const { found, index } = binarySearch(
      leafPage.keys,
      serializedKey,
      (a, b) => this.compareKeys(a, b)
    );

    if (found) {
      return this.valueCodec.decode(leafPage.values[index]);
    }

    return undefined;
  }

  /**
   * Find the leaf page that should contain a key
   */
  private async findLeaf(key: Uint8Array): Promise<Page> {
    if (!this.metadata) throw new Error('B-tree not initialized');

    let page = await this.readPage(this.metadata.rootPageId);

    while (page.type === PageType.INTERNAL) {
      // Find the child to descend into
      const { found, index } = binarySearch(page.keys, key, (a, b) =>
        this.compareKeys(a, b)
      );

      // In a B+tree internal node with keys [k0, k1, ...] and children [c0, c1, c2, ...]:
      // - c0 contains keys < k0
      // - c1 contains keys >= k0 and < k1
      // - c2 contains keys >= k1 and < k2
      // etc.
      //
      // If binary search returns found=true at index i, the key equals keys[i],
      // so we should go to children[i+1] (keys >= keys[i]).
      // If binary search returns found=false at index i, the key is between
      // keys[i-1] and keys[i], so we should go to children[i] (keys < keys[i]).
      const childIndex = found ? index + 1 : index;
      const childId = page.children[childIndex];
      page = await this.readPage(childId);
    }

    return page;
  }

  /**
   * Set a key-value pair
   */
  async set(key: K, value: V): Promise<void> {
    if (!this.metadata) await this.loadMetadata();
    if (!this.metadata) throw new Error('Failed to load metadata');

    const serializedKey = this.keyCodec.encode(key);
    const serializedValue = this.valueCodec.encode(value);

    // Find path to leaf
    const path = await this.findPath(serializedKey);
    const leafPage = path[path.length - 1];

    // Check if key already exists
    const { found, index } = binarySearch(
      leafPage.keys,
      serializedKey,
      (a, b) => this.compareKeys(a, b)
    );

    if (found) {
      // Update existing value
      leafPage.values[index] = serializedValue;
      this.markDirty(leafPage);
    } else {
      // Insert new key-value pair
      const wasInserted = await this.insertIntoLeaf(
        leafPage,
        serializedKey,
        serializedValue,
        index,
        path
      );

      if (wasInserted) {
        this.metadata.entryCount++;
      }
    }

    await this.flush();
  }

  /**
   * Find the path from root to the leaf containing a key
   */
  private async findPath(key: Uint8Array): Promise<Page[]> {
    if (!this.metadata) throw new Error('B-tree not initialized');

    const path: Page[] = [];
    let page = await this.readPage(this.metadata.rootPageId);
    path.push(page);

    while (page.type === PageType.INTERNAL) {
      const { found, index } = binarySearch(page.keys, key, (a, b) =>
        this.compareKeys(a, b)
      );

      // Same logic as findLeaf: if found, go right; otherwise, go to insertion point
      const childIndex = found ? index + 1 : index;
      const childId = page.children[childIndex];
      page = await this.readPage(childId);
      path.push(page);
    }

    return path;
  }

  /**
   * Insert a key-value pair into a leaf page, handling splits if necessary
   */
  private async insertIntoLeaf(
    leaf: Page,
    key: Uint8Array,
    value: Uint8Array,
    insertIndex: number,
    path: Page[]
  ): Promise<boolean> {
    // Check if we can insert without splitting:
    // 1. Must not exceed maxKeys
    // 2. Must fit within physical page size
    const withinMaxKeys = leaf.keys.length < this.config.maxKeys;
    const fitsPhysically = wouldFit(leaf, key.byteLength, value.byteLength);

    if (withinMaxKeys && fitsPhysically) {
      // Insert in place
      leaf.keys.splice(insertIndex, 0, key);
      leaf.values.splice(insertIndex, 0, value);
      this.markDirty(leaf);
      return true;
    }

    // Need to split the leaf
    await this.splitLeaf(leaf, key, value, insertIndex, path);
    return true;
  }

  /**
   * Split a leaf page and propagate the split up the tree
   */
  private async splitLeaf(
    leaf: Page,
    key: Uint8Array,
    value: Uint8Array,
    insertIndex: number,
    path: Page[]
  ): Promise<void> {
    if (!this.metadata) throw new Error('B-tree not initialized');

    // Insert the new key-value temporarily
    leaf.keys.splice(insertIndex, 0, key);
    leaf.values.splice(insertIndex, 0, value);

    // Find the split point (middle)
    const midIndex = Math.floor(leaf.keys.length / 2);

    // Create new right sibling page
    const newPageId = this.allocPageId();
    const newLeaf = createLeafPage(newPageId);

    // Move half the entries to the new leaf
    newLeaf.keys = leaf.keys.splice(midIndex);
    newLeaf.values = leaf.values.splice(midIndex);

    // Update leaf chain
    newLeaf.nextLeaf = leaf.nextLeaf;
    newLeaf.prevLeaf = leaf.id;
    leaf.nextLeaf = newLeaf.id;

    // Update the old next leaf's prevLeaf pointer
    if (newLeaf.nextLeaf !== -1) {
      const oldNext = await this.readPage(newLeaf.nextLeaf);
      oldNext.prevLeaf = newLeaf.id;
      this.markDirty(oldNext);
    }

    this.markDirty(leaf);
    await this.writePage(newLeaf);

    // The promoted key is the first key of the new leaf
    const promotedKey = newLeaf.keys[0];

    // Insert the promoted key into the parent
    await this.insertIntoParent(path, path.length - 2, promotedKey, newLeaf.id);
  }

  /**
   * Insert a key and child pointer into an internal node
   */
  private async insertIntoParent(
    path: Page[],
    parentIndex: number,
    key: Uint8Array,
    rightChildId: number
  ): Promise<void> {
    if (!this.metadata) throw new Error('B-tree not initialized');

    if (parentIndex < 0) {
      // Need to create a new root
      const newRootId = this.allocPageId();
      const newRoot = createInternalPage(newRootId);

      const oldRootId = path[0].id;
      newRoot.keys.push(key);
      newRoot.children.push(oldRootId);
      newRoot.children.push(rightChildId);

      await this.writePage(newRoot);

      this.metadata.rootPageId = newRootId;
      this.metadata.height++;
      return;
    }

    const parent = path[parentIndex];
    const { index } = binarySearch(parent.keys, key, (a, b) =>
      this.compareKeys(a, b)
    );

    // Check if we can fit the new key
    if (parent.keys.length < this.config.maxKeys) {
      parent.keys.splice(index, 0, key);
      parent.children.splice(index + 1, 0, rightChildId);
      this.markDirty(parent);
      return;
    }

    // Need to split the internal node
    await this.splitInternal(parent, key, rightChildId, index, path, parentIndex);
  }

  /**
   * Split an internal node and propagate up
   */
  private async splitInternal(
    node: Page,
    key: Uint8Array,
    rightChildId: number,
    insertIndex: number,
    path: Page[],
    nodeIndex: number
  ): Promise<void> {
    // Insert temporarily
    node.keys.splice(insertIndex, 0, key);
    node.children.splice(insertIndex + 1, 0, rightChildId);

    // Find split point
    const midIndex = Math.floor(node.keys.length / 2);
    const promotedKey = node.keys[midIndex];

    // Create new right sibling
    const newPageId = this.allocPageId();
    const newNode = createInternalPage(newPageId);

    // Move right half to new node
    newNode.keys = node.keys.splice(midIndex + 1);
    newNode.children = node.children.splice(midIndex + 1);

    // Remove the promoted key from the original node
    node.keys.pop(); // Remove the middle key that was promoted

    this.markDirty(node);
    await this.writePage(newNode);

    // Recurse to parent
    await this.insertIntoParent(path, nodeIndex - 1, promotedKey, newNode.id);
  }

  /**
   * Delete a key
   */
  async delete(key: K): Promise<boolean> {
    if (!this.metadata) await this.loadMetadata();
    if (!this.metadata) throw new Error('Failed to load metadata');

    const serializedKey = this.keyCodec.encode(key);
    const path = await this.findPath(serializedKey);
    const leaf = path[path.length - 1];

    const { found, index } = binarySearch(leaf.keys, serializedKey, (a, b) =>
      this.compareKeys(a, b)
    );

    if (!found) {
      return false;
    }

    // Remove the key-value pair
    leaf.keys.splice(index, 1);
    leaf.values.splice(index, 1);
    this.markDirty(leaf);

    this.metadata.entryCount--;

    // Handle underflow (simplified - no rebalancing/merging for now)
    // A full implementation would merge or redistribute with siblings
    // when a leaf has fewer than minKeys entries

    await this.flush();
    return true;
  }

  /**
   * Iterate over a range of keys
   */
  async *range(start: K, end: K): AsyncIterableIterator<[K, V]> {
    if (!this.metadata) await this.loadMetadata();
    if (!this.metadata) throw new Error('Failed to load metadata');

    const startKey = this.keyCodec.encode(start);
    const endKey = this.keyCodec.encode(end);

    // Find the starting leaf
    let leaf = await this.findLeaf(startKey);
    const { index: startIndex } = binarySearch(leaf.keys, startKey, (a, b) =>
      this.compareKeys(a, b)
    );

    let i = startIndex;

    while (true) {
      // Iterate through current leaf
      while (i < leaf.keys.length) {
        const keyBytes = leaf.keys[i];

        // Check if we've passed the end
        if (this.compareKeys(keyBytes, endKey) >= 0) {
          return;
        }

        const key = this.keyCodec.decode(keyBytes);
        const value = this.valueCodec.decode(leaf.values[i]);
        yield [key, value];
        i++;
      }

      // Move to next leaf
      if (leaf.nextLeaf === -1) {
        return;
      }

      leaf = await this.readPage(leaf.nextLeaf);
      i = 0;
    }
  }

  /**
   * Iterate over all entries
   */
  async *entries(): AsyncIterableIterator<[K, V]> {
    if (!this.metadata) await this.loadMetadata();
    if (!this.metadata) throw new Error('Failed to load metadata');

    // Find the leftmost leaf
    let page = await this.readPage(this.metadata.rootPageId);
    while (page.type === PageType.INTERNAL) {
      page = await this.readPage(page.children[0]);
    }

    // Iterate through all leaves using the leaf chain
    while (true) {
      for (let i = 0; i < page.keys.length; i++) {
        const key = this.keyCodec.decode(page.keys[i]);
        const value = this.valueCodec.decode(page.values[i]);
        yield [key, value];
      }

      if (page.nextLeaf === -1) {
        return;
      }

      page = await this.readPage(page.nextLeaf);
    }
  }

  /**
   * Get the number of entries
   */
  async count(): Promise<number> {
    if (!this.metadata) await this.loadMetadata();
    if (!this.metadata) throw new Error('Failed to load metadata');
    return this.metadata.entryCount;
  }

  /**
   * Clear all entries
   */
  async clear(): Promise<void> {
    if (!this.metadata) await this.loadMetadata();
    if (!this.metadata) throw new Error('Failed to load metadata');

    // Delete all pages
    const pageKeys = await this.fsx.list(this.config.pagePrefix);
    for (const key of pageKeys) {
      await this.fsx.delete(key);
    }

    // Reset cache
    this.pageCache.clear();

    // Create new root
    const rootPage = createLeafPage(0);
    await this.writePage(rootPage);

    this.metadata = {
      rootPageId: 0,
      height: 1,
      entryCount: 0,
      nextPageId: 1,
      config: this.config,
    };
    await this.saveMetadata();
  }

  /**
   * Get tree statistics for debugging
   */
  async stats(): Promise<{
    height: number;
    entryCount: number;
    pageCount: number;
    rootPageId: number;
  }> {
    if (!this.metadata) await this.loadMetadata();
    if (!this.metadata) throw new Error('Failed to load metadata');

    return {
      height: this.metadata.height,
      entryCount: this.metadata.entryCount,
      pageCount: this.metadata.nextPageId,
      rootPageId: this.metadata.rootPageId,
    };
  }

  /**
   * Get cache statistics for monitoring
   */
  getCacheStats(): CacheStats {
    const cacheConfig = this.config.cache ?? DEFAULT_PAGE_CACHE_CONFIG;
    const stats: CacheStats = {
      size: this.pageCache.size,
      maxSize: cacheConfig.maxBytes ? this.pageCache.maxCacheSize : cacheConfig.maxPages,
      hits: this.pageCache.hits,
      misses: this.pageCache.misses,
      evictions: this.pageCache.evictions,
      hitRate: this.pageCache.hitRate,
    };

    // Add byte statistics if byte-based tracking is enabled
    if (cacheConfig.maxBytes) {
      stats.currentBytes = this.pageCache.currentBytes;
      stats.maxBytes = cacheConfig.maxBytes;
    }

    return stats;
  }

  /**
   * Reset cache statistics counters
   */
  resetCacheStats(): void {
    this.pageCache.resetStats();
  }
}

/**
 * Extended B-tree interface with initialization and statistics
 */
export interface BTreeExtended<K, V> extends BTree<K, V> {
  /** Initialize the B-tree (load or create metadata) */
  init(): Promise<void>;

  /** Get tree statistics for debugging */
  stats(): Promise<{
    height: number;
    entryCount: number;
    pageCount: number;
    rootPageId: number;
  }>;

  /** Get cache statistics for monitoring */
  getCacheStats(): CacheStats;

  /** Reset cache statistics counters */
  resetCacheStats(): void;
}

/**
 * Create a new B-tree instance
 */
export function createBTree<K, V>(
  fsx: FSXBackend,
  keyCodec: KeyCodec<K>,
  valueCodec: ValueCodec<V>,
  config?: Partial<BTreeConfig>
): BTreeExtended<K, V> {
  return new BTreeImpl(fsx, keyCodec, valueCodec, config);
}
