/**
 * B-tree Implementation for DoSQL
 *
 * A B+tree implementation for row storage in Durable Objects.
 * Uses the unified StorageInterface for page persistence with the following characteristics:
 *
 * - Keys and values are serialized using pluggable codecs
 * - Pages are stored as storage blobs with configurable prefixes
 * - Leaf pages are linked for efficient range scans
 * - Supports concurrent reads (single-writer assumed)
 * - Works with any StorageInterface implementation (DO, R2, memory, etc.)
 */

import type { StorageInterface } from '../storage/interface.js';
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
 * Storage backend type that can be either the new StorageInterface or legacy FSXBackend.
 * This union type provides backward compatibility while allowing migration to the new interface.
 */
type StorageBackend = StorageInterface | FSXBackend;

/**
 * Normalize a storage backend to use consistent method names.
 * Supports both new StorageInterface (get/put) and legacy FSXBackend (read/write).
 */
function normalizeStorage(storage: StorageBackend): {
  read: (key: string) => Promise<Uint8Array | null>;
  write: (key: string, data: Uint8Array) => Promise<void>;
  delete: (key: string) => Promise<void>;
  list: (prefix: string) => Promise<string[]>;
} {
  // Check if it's the new StorageInterface (has 'get' method)
  if ('get' in storage && typeof storage.get === 'function') {
    const si = storage as StorageInterface;
    return {
      read: (key) => si.get(key),
      write: (key, data) => si.put(key, data),
      delete: (key) => si.delete(key),
      list: (prefix) => si.list(prefix),
    };
  }

  // It's the legacy FSXBackend
  const fsx = storage as FSXBackend;
  return {
    read: (key) => fsx.read(key),
    write: (key, data) => fsx.write(key, data),
    delete: (key) => fsx.delete(key),
    list: (prefix) => fsx.list(prefix),
  };
}

/**
 * B+tree implementation
 */
export class BTreeImpl<K, V> implements BTree<K, V> {
  private readonly storage: {
    read: (key: string) => Promise<Uint8Array | null>;
    write: (key: string, data: Uint8Array) => Promise<void>;
    delete: (key: string) => Promise<void>;
    list: (prefix: string) => Promise<string[]>;
  };
  private readonly keyCodec: KeyCodec<K>;
  private readonly valueCodec: ValueCodec<V>;
  private readonly config: BTreeConfig;
  private readonly userOnEvict?: (pageId: number, page: Page, dirty: boolean) => void | Promise<void>;

  private metadata: BTreeMetadata | null = null;
  private readonly pageCache: LRUCache<number, Page>;

  constructor(
    storage: StorageBackend,
    keyCodec: KeyCodec<K>,
    valueCodec: ValueCodec<V>,
    config: Partial<BTreeConfig> = {}
  ) {
    this.storage = normalizeStorage(storage);
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
          await this.storage.write(this.pageKey(pageId), data);
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
    const data = await this.storage.read(key);

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
    await this.storage.write(key, data);
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
    // Use setAsync to ensure any evicted dirty pages are written before continuing
    await this.pageCache.setAsync(pageId, page);
    return page;
  }

  /**
   * Write a page to storage and cache
   */
  private async writePage(page: Page): Promise<void> {
    const data = serializePage(page);
    await this.fsx.write(this.pageKey(page.id), data);
    // Add to cache as clean (not dirty since we just wrote it)
    // Use setAsync to ensure any evicted dirty pages are written before continuing
    await this.pageCache.setAsync(page.id, page, { dirty: false });
  }

  /**
   * Mark a page as dirty (needs writing)
   */
  private markDirty(page: Page): void {
    this.pageCache.set(page.id, page, { dirty: true });
  }

  /**
   * Delete a page from storage and cache (for orphaned pages after merges)
   */
  private async deletePage(pageId: number): Promise<void> {
    // Remove from cache first (don't write back since we're deleting)
    this.pageCache.delete(pageId);
    // Delete from persistent storage
    await this.fsx.delete(this.pageKey(pageId));
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
    if (path.length === 0) {
      throw new Error('B-tree findPath returned empty path');
    }
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
    if (path.length === 0) {
      throw new Error('B-tree findPath returned empty path');
    }
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

    // Handle underflow - rebalance or merge when node has fewer than minKeys entries
    // Root is allowed to have fewer than minKeys entries
    if (leaf.id !== this.metadata.rootPageId && leaf.keys.length < this.config.minKeys) {
      await this.handleUnderflow(path, path.length - 1);
    }

    await this.flush();
    return true;
  }

  /**
   * Handle underflow in a node by redistributing or merging with siblings
   */
  private async handleUnderflow(path: Page[], nodeIndex: number): Promise<void> {
    if (!this.metadata) throw new Error('B-tree not initialized');

    const node = path[nodeIndex];

    // Root doesn't need to meet minimum key requirement
    if (node.id === this.metadata.rootPageId) {
      // If root is internal and has only one child, shrink the tree
      if (node.type === PageType.INTERNAL && node.keys.length === 0 && node.children.length === 1) {
        const oldRootId = node.id;
        this.metadata.rootPageId = node.children[0];
        this.metadata.height--;
        // Delete the old root page from storage
        await this.deletePage(oldRootId);
      }
      return;
    }

    const parent = path[nodeIndex - 1];
    const childIndexInParent = this.findChildIndex(parent, node.id);

    // Try to borrow from left sibling first
    if (childIndexInParent > 0) {
      const leftSiblingId = parent.children[childIndexInParent - 1];
      const leftSibling = await this.readPage(leftSiblingId);

      if (leftSibling.keys.length > this.config.minKeys) {
        await this.redistributeFromLeft(parent, leftSibling, node, childIndexInParent);
        return;
      }
    }

    // Try to borrow from right sibling
    if (childIndexInParent < parent.children.length - 1) {
      const rightSiblingId = parent.children[childIndexInParent + 1];
      const rightSibling = await this.readPage(rightSiblingId);

      if (rightSibling.keys.length > this.config.minKeys) {
        await this.redistributeFromRight(parent, node, rightSibling, childIndexInParent);
        return;
      }
    }

    // Neither sibling can donate, so merge
    if (childIndexInParent > 0) {
      // Merge with left sibling (current node merges into left)
      const leftSiblingId = parent.children[childIndexInParent - 1];
      const leftSibling = await this.readPage(leftSiblingId);
      await this.mergeNodes(parent, leftSibling, node, childIndexInParent - 1);
    } else {
      // Merge with right sibling (right merges into current)
      const rightSiblingId = parent.children[childIndexInParent + 1];
      const rightSibling = await this.readPage(rightSiblingId);
      await this.mergeNodes(parent, node, rightSibling, childIndexInParent);
    }

    // After merge, parent might underflow - handle recursively
    if (parent.id !== this.metadata.rootPageId && parent.keys.length < this.config.minKeys) {
      await this.handleUnderflow(path, nodeIndex - 1);
    } else if (parent.id === this.metadata.rootPageId && parent.keys.length === 0) {
      // Root has become empty after merge - shrink tree
      if (parent.type === PageType.INTERNAL && parent.children.length === 1) {
        const oldRootId = parent.id;
        this.metadata.rootPageId = parent.children[0];
        this.metadata.height--;
        // Delete the old root page from storage
        await this.deletePage(oldRootId);
      }
    }
  }

  /**
   * Find the index of a child page within a parent's children array
   */
  private findChildIndex(parent: Page, childId: number): number {
    for (let i = 0; i < parent.children.length; i++) {
      if (parent.children[i] === childId) {
        return i;
      }
    }
    throw new Error(`Child ${childId} not found in parent ${parent.id}`);
  }

  /**
   * Redistribute keys from left sibling to underflowing node
   */
  private async redistributeFromLeft(
    parent: Page,
    leftSibling: Page,
    node: Page,
    nodeChildIndex: number
  ): Promise<void> {
    const separatorKeyIndex = nodeChildIndex - 1;

    if (node.type === PageType.LEAF) {
      // For leaf nodes:
      // 1. Move the last key-value from left sibling to beginning of node
      const borrowedKey = leftSibling.keys.pop()!;
      const borrowedValue = leftSibling.values.pop()!;
      node.keys.unshift(borrowedKey);
      node.values.unshift(borrowedValue);

      // 2. Update parent separator to be the new first key of node
      parent.keys[separatorKeyIndex] = node.keys[0];
    } else {
      // For internal nodes:
      // 1. Move separator key from parent down to beginning of node
      node.keys.unshift(parent.keys[separatorKeyIndex]);

      // 2. Move last key from left sibling up to parent as new separator
      parent.keys[separatorKeyIndex] = leftSibling.keys.pop()!;

      // 3. Move the last child pointer from left sibling to beginning of node
      node.children.unshift(leftSibling.children.pop()!);
    }

    this.markDirty(parent);
    this.markDirty(leftSibling);
    this.markDirty(node);
  }

  /**
   * Redistribute keys from right sibling to underflowing node
   */
  private async redistributeFromRight(
    parent: Page,
    node: Page,
    rightSibling: Page,
    nodeChildIndex: number
  ): Promise<void> {
    const separatorKeyIndex = nodeChildIndex;

    if (node.type === PageType.LEAF) {
      // For leaf nodes:
      // 1. Move the first key-value from right sibling to end of node
      const borrowedKey = rightSibling.keys.shift()!;
      const borrowedValue = rightSibling.values.shift()!;
      node.keys.push(borrowedKey);
      node.values.push(borrowedValue);

      // 2. Update parent separator to be the new first key of right sibling
      parent.keys[separatorKeyIndex] = rightSibling.keys[0];
    } else {
      // For internal nodes:
      // 1. Move separator key from parent down to end of node
      node.keys.push(parent.keys[separatorKeyIndex]);

      // 2. Move first key from right sibling up to parent as new separator
      parent.keys[separatorKeyIndex] = rightSibling.keys.shift()!;

      // 3. Move the first child pointer from right sibling to end of node
      node.children.push(rightSibling.children.shift()!);
    }

    this.markDirty(parent);
    this.markDirty(node);
    this.markDirty(rightSibling);
  }

  /**
   * Merge right node into left node, removing separator key from parent
   */
  private async mergeNodes(
    parent: Page,
    leftNode: Page,
    rightNode: Page,
    separatorKeyIndex: number
  ): Promise<void> {
    if (leftNode.type === PageType.LEAF) {
      // For leaf nodes: just concatenate keys and values
      leftNode.keys.push(...rightNode.keys);
      leftNode.values.push(...rightNode.values);

      // Update leaf chain: left.next = right.next
      leftNode.nextLeaf = rightNode.nextLeaf;

      // Update the next leaf's prev pointer if it exists
      if (rightNode.nextLeaf !== -1) {
        const nextLeaf = await this.readPage(rightNode.nextLeaf);
        nextLeaf.prevLeaf = leftNode.id;
        this.markDirty(nextLeaf);
      }
    } else {
      // For internal nodes: bring separator down and concatenate
      leftNode.keys.push(parent.keys[separatorKeyIndex]);
      leftNode.keys.push(...rightNode.keys);
      leftNode.children.push(...rightNode.children);
    }

    // Remove separator key and right child pointer from parent
    parent.keys.splice(separatorKeyIndex, 1);
    parent.children.splice(separatorKeyIndex + 1, 1);

    this.markDirty(parent);
    this.markDirty(leftNode);

    // Delete the orphaned right node from storage
    await this.deletePage(rightNode.id);
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
