/**
 * LRU/LFU Cache Implementation for B-tree Page Manager
 *
 * A generic cache with configurable eviction policies:
 * - LRU (Least Recently Used) - evicts entries not accessed recently
 * - LFU (Least Frequently Used) - evicts entries accessed least often
 *
 * Features:
 * - Configurable max size (by entry count or bytes)
 * - O(1) get, set, delete operations
 * - Optional eviction callback for dirty page handling
 * - Dirty page tracking for write-back
 * - Memory pressure callbacks for proactive eviction
 */

/**
 * Eviction policy for the cache
 */
export type EvictionPolicy = 'lru' | 'lfu';

/**
 * Memory pressure levels for callbacks
 */
export type MemoryPressureLevel = 'low' | 'medium' | 'high' | 'critical';

/**
 * Memory pressure callback info
 */
export interface MemoryPressureInfo {
  /** Current memory usage in bytes */
  currentBytes: number;
  /** Maximum memory limit in bytes */
  maxBytes: number;
  /** Usage ratio (0-1) */
  usageRatio: number;
  /** Pressure level */
  level: MemoryPressureLevel;
  /** Number of entries in cache */
  entryCount: number;
}

/**
 * Options for creating an LRU cache
 */
export interface LRUCacheOptions<K, V> {
  /**
   * Maximum cache size. When sizeCalculator is provided, this is max bytes.
   * Otherwise, this is max number of entries.
   */
  maxSize: number;

  /**
   * Optional function to calculate the size of a value in bytes.
   * If not provided, each entry counts as 1 toward maxSize.
   */
  sizeCalculator?: (value: V, key: K) => number;

  /**
   * Callback invoked when an entry is evicted due to capacity.
   * Can be async - use setAsync()/clearAsync() to properly await async callbacks.
   * @param key - The key being evicted
   * @param value - The value being evicted
   * @param dirty - Whether the entry was marked as dirty
   */
  onEvict?: (key: K, value: V, dirty: boolean) => void | Promise<void>;

  /**
   * If true, call onEvict for each entry when clear() is called.
   * Default: false
   */
  evictOnClear?: boolean;

  /**
   * Eviction policy to use.
   * - 'lru': Least Recently Used (default) - evicts entries not accessed recently
   * - 'lfu': Least Frequently Used - evicts entries accessed least often
   */
  evictionPolicy?: EvictionPolicy;

  /**
   * Callback invoked when memory pressure changes.
   * Can be used to trigger proactive eviction or other memory management.
   */
  onMemoryPressure?: (info: MemoryPressureInfo) => void;

  /**
   * Thresholds for memory pressure levels (as ratios of maxSize).
   * Default: { low: 0.5, medium: 0.75, high: 0.9 }
   */
  pressureThresholds?: {
    low?: number;
    medium?: number;
    high?: number;
  };
}

/**
 * Options for setting an entry
 */
export interface SetOptions {
  /**
   * Mark this entry as dirty (needs to be written back)
   */
  dirty?: boolean;
}

/**
 * Default memory pressure thresholds
 */
const DEFAULT_PRESSURE_THRESHOLDS = {
  low: 0.5,
  medium: 0.75,
  high: 0.9,
};

/**
 * Internal node for the doubly-linked list
 */
interface LRUNode<K, V> {
  key: K;
  value: V;
  size: number;
  dirty: boolean;
  prev: LRUNode<K, V> | null;
  next: LRUNode<K, V> | null;
  /** Access frequency for LFU policy */
  frequency: number;
}

/**
 * LRU/LFU Cache implementation using a Map and doubly-linked list.
 *
 * For LRU policy, the doubly-linked list maintains LRU order:
 * - Head is the least recently used (oldest)
 * - Tail is the most recently used (newest)
 *
 * For LFU policy, nodes track access frequency and eviction selects
 * the node with the lowest frequency (ties broken by LRU order).
 *
 * Operations:
 * - get: O(1) - looks up in map, updates position/frequency
 * - set: O(1) for LRU, O(n) for LFU - adds to map, evicts if needed
 * - delete: O(1) - removes from map and list
 */
export class LRUCache<K, V> {
  private readonly maxSize: number;
  private readonly sizeCalculator?: (value: V, key: K) => number;
  private readonly onEvict?: (key: K, value: V, dirty: boolean) => void | Promise<void>;
  private readonly evictOnClear: boolean;
  private readonly evictionPolicy: EvictionPolicy;
  private readonly onMemoryPressure?: (info: MemoryPressureInfo) => void;
  private readonly pressureThresholds: { low: number; medium: number; high: number };

  private readonly map = new Map<K, LRUNode<K, V>>();
  private head: LRUNode<K, V> | null = null;
  private tail: LRUNode<K, V> | null = null;
  private _currentBytes = 0;

  // For LFU: track minimum frequency for O(1) eviction candidate finding
  private _minFrequency = 0;
  // For LFU: map from frequency to doubly-linked list of nodes with that frequency
  private readonly frequencyLists = new Map<number, { head: LRUNode<K, V> | null; tail: LRUNode<K, V> | null }>();

  // Statistics tracking
  private _hits = 0;
  private _misses = 0;
  private _evictions = 0;

  // Memory pressure tracking
  private _lastPressureLevel: MemoryPressureLevel = 'low';

  constructor(options: LRUCacheOptions<K, V>) {
    this.maxSize = options.maxSize;
    this.sizeCalculator = options.sizeCalculator;
    this.onEvict = options.onEvict;
    this.evictOnClear = options.evictOnClear ?? false;
    this.evictionPolicy = options.evictionPolicy ?? 'lru';
    this.onMemoryPressure = options.onMemoryPressure;
    this.pressureThresholds = {
      low: options.pressureThresholds?.low ?? DEFAULT_PRESSURE_THRESHOLDS.low,
      medium: options.pressureThresholds?.medium ?? DEFAULT_PRESSURE_THRESHOLDS.medium,
      high: options.pressureThresholds?.high ?? DEFAULT_PRESSURE_THRESHOLDS.high,
    };
  }

  /**
   * Number of entries in the cache
   */
  get size(): number {
    return this.map.size;
  }

  /**
   * Current size in bytes (only meaningful when sizeCalculator is provided)
   */
  get currentBytes(): number {
    return this._currentBytes;
  }

  /**
   * Maximum cache size
   */
  get maxCacheSize(): number {
    return this.maxSize;
  }

  /**
   * Number of cache hits
   */
  get hits(): number {
    return this._hits;
  }

  /**
   * Number of cache misses
   */
  get misses(): number {
    return this._misses;
  }

  /**
   * Number of evictions
   */
  get evictions(): number {
    return this._evictions;
  }

  /**
   * Cache hit rate (hits / (hits + misses))
   */
  get hitRate(): number {
    const total = this._hits + this._misses;
    return total === 0 ? 0 : this._hits / total;
  }

  /**
   * Current eviction policy
   */
  get policy(): EvictionPolicy {
    return this.evictionPolicy;
  }

  /**
   * Current memory pressure level
   */
  get memoryPressureLevel(): MemoryPressureLevel {
    return this._lastPressureLevel;
  }

  /**
   * Get memory usage ratio (0-1)
   */
  get memoryUsageRatio(): number {
    if (this.maxSize === 0) return 0;
    return this._currentBytes / this.maxSize;
  }

  /**
   * Reset statistics counters
   */
  resetStats(): void {
    this._hits = 0;
    this._misses = 0;
    this._evictions = 0;
  }

  /**
   * Record a cache miss (called by external code when a value was not in cache)
   */
  recordMiss(): void {
    this._misses++;
  }

  /**
   * Get a value from the cache, updating its LRU position or LFU frequency
   * @param key - The key to look up
   * @returns The value or undefined if not found
   */
  get(key: K): V | undefined {
    const node = this.map.get(key);
    if (!node) {
      // Don't track misses here - let caller decide (they may load from storage)
      return undefined;
    }

    // Track cache hit
    this._hits++;

    if (this.evictionPolicy === 'lfu') {
      // Update frequency for LFU policy
      this.incrementFrequency(node);
    } else {
      // Move to tail (most recently used) for LRU policy
      this.moveToTail(node);
    }
    return node.value;
  }

  /**
   * Get a value without updating its LRU position
   * @param key - The key to look up
   * @returns The value or undefined if not found
   */
  peek(key: K): V | undefined {
    const node = this.map.get(key);
    return node?.value;
  }

  /**
   * Set a value in the cache
   * @param key - The key
   * @param value - The value
   * @param options - Optional settings (e.g., dirty flag)
   */
  set(key: K, value: V, options: SetOptions = {}): void {
    // Handle zero max size
    if (this.maxSize <= 0) return;

    const newSize = this.sizeCalculator ? this.sizeCalculator(value, key) : 1;

    // Check if key already exists
    const existingNode = this.map.get(key);
    if (existingNode) {
      // Update existing entry
      const oldSize = existingNode.size;
      existingNode.value = value;
      existingNode.size = newSize;
      existingNode.dirty = options.dirty ?? existingNode.dirty;
      this._currentBytes += newSize - oldSize;

      if (this.evictionPolicy === 'lfu') {
        this.incrementFrequency(existingNode);
      } else {
        this.moveToTail(existingNode);
      }
    } else {
      // Create new entry
      const node: LRUNode<K, V> = {
        key,
        value,
        size: newSize,
        dirty: options.dirty ?? false,
        prev: null,
        next: null,
        frequency: 1,
      };

      // Evict if necessary before adding
      this.evictToFit(newSize);

      // Add to map
      this.map.set(key, node);

      if (this.evictionPolicy === 'lfu') {
        // Add to frequency list for LFU
        this.addToFrequencyList(node, 1);
        this._minFrequency = 1;
      } else {
        // Add to tail for LRU
        this.addToTail(node);
      }
      this._currentBytes += newSize;
    }

    // Check memory pressure after set
    this.checkMemoryPressure();
  }

  /**
   * Set a value in the cache, awaiting any async onEvict callbacks.
   * Use this instead of set() when onEvict is async to avoid race conditions
   * where dirty pages are evicted before their write completes.
   * @param key - The key
   * @param value - The value
   * @param options - Optional settings (e.g., dirty flag)
   */
  async setAsync(key: K, value: V, options: SetOptions = {}): Promise<void> {
    // Handle zero max size
    if (this.maxSize <= 0) return;

    const newSize = this.sizeCalculator ? this.sizeCalculator(value, key) : 1;

    // Check if key already exists
    const existingNode = this.map.get(key);
    if (existingNode) {
      // Update existing entry
      const oldSize = existingNode.size;
      existingNode.value = value;
      existingNode.size = newSize;
      existingNode.dirty = options.dirty ?? existingNode.dirty;
      this._currentBytes += newSize - oldSize;

      if (this.evictionPolicy === 'lfu') {
        this.incrementFrequency(existingNode);
      } else {
        this.moveToTail(existingNode);
      }
    } else {
      // Create new entry
      const node: LRUNode<K, V> = {
        key,
        value,
        size: newSize,
        dirty: options.dirty ?? false,
        prev: null,
        next: null,
        frequency: 1,
      };

      // Evict if necessary before adding - await async callbacks
      await this.evictToFitAsync(newSize);

      // Add to map
      this.map.set(key, node);

      if (this.evictionPolicy === 'lfu') {
        // Add to frequency list for LFU
        this.addToFrequencyList(node, 1);
        this._minFrequency = 1;
      } else {
        // Add to tail for LRU
        this.addToTail(node);
      }
      this._currentBytes += newSize;
    }

    // Check memory pressure after set
    this.checkMemoryPressure();
  }

  /**
   * Delete a value from the cache
   * @param key - The key to delete
   * @returns true if the key existed
   */
  delete(key: K): boolean {
    const node = this.map.get(key);
    if (!node) return false;

    this.removeNode(node);
    this.map.delete(key);
    this._currentBytes -= node.size;
    return true;
  }

  /**
   * Check if a key exists in the cache
   * @param key - The key to check
   */
  has(key: K): boolean {
    return this.map.has(key);
  }

  /**
   * Clear all entries from the cache
   */
  clear(): void {
    if (this.evictOnClear && this.onEvict) {
      // Call onEvict for each entry
      for (const node of this.map.values()) {
        this.onEvict(node.key, node.value, node.dirty);
      }
    }

    this.map.clear();
    this.head = null;
    this.tail = null;
    this._currentBytes = 0;

    // Reset LFU state
    this.frequencyLists.clear();
    this._minFrequency = 0;
    this._lastPressureLevel = 'low';
  }

  /**
   * Clear all entries from the cache, awaiting any async onEvict callbacks.
   * Use this instead of clear() when onEvict is async.
   */
  async clearAsync(): Promise<void> {
    if (this.evictOnClear && this.onEvict) {
      // Call onEvict for each entry and await async callbacks
      const promises: Array<void | Promise<void>> = [];
      for (const node of this.map.values()) {
        promises.push(this.onEvict(node.key, node.value, node.dirty));
      }
      await Promise.all(promises);
    }

    this.map.clear();
    this.head = null;
    this.tail = null;
    this._currentBytes = 0;

    // Reset LFU state
    this.frequencyLists.clear();
    this._minFrequency = 0;
    this._lastPressureLevel = 'low';
  }

  /**
   * Check if an entry is marked as dirty
   * @param key - The key to check
   */
  isDirty(key: K): boolean {
    const node = this.map.get(key);
    return node?.dirty ?? false;
  }

  /**
   * Mark an entry as clean
   * @param key - The key to mark clean
   */
  markClean(key: K): void {
    const node = this.map.get(key);
    if (node) {
      node.dirty = false;
    }
  }

  /**
   * Mark an entry as dirty
   * @param key - The key to mark dirty
   */
  markDirty(key: K): void {
    const node = this.map.get(key);
    if (node) {
      node.dirty = true;
    }
  }

  /**
   * Get all dirty keys
   */
  getDirtyKeys(): K[] {
    const keys: K[] = [];
    for (const [key, node] of this.map) {
      if (node.dirty) {
        keys.push(key);
      }
    }
    return keys;
  }

  /**
   * Iterate over entries in LRU order (oldest to newest)
   */
  *entries(): IterableIterator<[K, V]> {
    let node = this.head;
    while (node) {
      yield [node.key, node.value];
      node = node.next;
    }
  }

  /**
   * Iterate over keys in LRU order (oldest to newest)
   */
  *keys(): IterableIterator<K> {
    let node = this.head;
    while (node) {
      yield node.key;
      node = node.next;
    }
  }

  /**
   * Iterate over values in LRU order (oldest to newest)
   */
  *values(): IterableIterator<V> {
    let node = this.head;
    while (node) {
      yield node.value;
      node = node.next;
    }
  }

  /**
   * Evict entries until there's room for a new entry of the given size
   */
  private evictToFit(newSize: number): void {
    if (this.sizeCalculator) {
      // Byte-based eviction
      while (this.head && this._currentBytes + newSize > this.maxSize) {
        this.evictHead();
      }
    } else {
      // Entry count-based eviction
      while (this.head && this.map.size >= this.maxSize) {
        this.evictHead();
      }
    }
  }

  /**
   * Evict the head (least recently used) entry
   */
  private evictHead(): void {
    if (!this.head) return;

    const node = this.head;
    this.removeNode(node);
    this.map.delete(node.key);
    this._currentBytes -= node.size;

    // Track eviction
    this._evictions++;

    if (this.onEvict) {
      this.onEvict(node.key, node.value, node.dirty);
    }
  }

  /**
   * Remove a node from the linked list
   */
  private removeNode(node: LRUNode<K, V>): void {
    if (node.prev) {
      node.prev.next = node.next;
    } else {
      this.head = node.next;
    }

    if (node.next) {
      node.next.prev = node.prev;
    } else {
      this.tail = node.prev;
    }

    node.prev = null;
    node.next = null;
  }

  /**
   * Add a node to the tail (most recently used)
   */
  private addToTail(node: LRUNode<K, V>): void {
    node.prev = this.tail;
    node.next = null;

    if (this.tail) {
      this.tail.next = node;
    } else {
      this.head = node;
    }

    this.tail = node;
  }

  /**
   * Move an existing node to the tail
   */
  private moveToTail(node: LRUNode<K, V>): void {
    if (node === this.tail) return;

    this.removeNode(node);
    this.addToTail(node);
  }

  /**
   * Evict entries until there's room, awaiting async onEvict callbacks
   */
  private async evictToFitAsync(newSize: number): Promise<void> {
    if (this.sizeCalculator) {
      // Byte-based eviction
      while (this.head && this._currentBytes + newSize > this.maxSize) {
        await this.evictHeadAsync();
      }
    } else {
      // Entry count-based eviction
      while (this.head && this.map.size >= this.maxSize) {
        await this.evictHeadAsync();
      }
    }
  }

  /**
   * Evict the head entry, awaiting async onEvict callback
   */
  private async evictHeadAsync(): Promise<void> {
    if (!this.head) return;

    const node = this.head;
    this.removeNode(node);
    this.map.delete(node.key);
    this._currentBytes -= node.size;

    // Track eviction
    this._evictions++;

    if (this.onEvict) {
      await this.onEvict(node.key, node.value, node.dirty);
    }
  }
}
