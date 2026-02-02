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
 * - 'lru': Least Recently Used - evicts entries not accessed recently
 * - 'lfu': Least Frequently Used - evicts entries accessed least often
 * - 'arc': Adaptive Replacement Cache - balances between recency and frequency
 */
export type EvictionPolicy = 'lru' | 'lfu' | 'arc';

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
  /** ARC list membership: 't1' = recency, 't2' = frequency, 'b1' = ghost recency, 'b2' = ghost frequency */
  arcList?: 't1' | 't2' | 'b1' | 'b2';
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

  // ARC state: maintains four lists
  // T1: recently accessed items (recency)
  // T2: frequently accessed items (accessed more than once, frequency)
  // B1: ghost entries for recently evicted from T1
  // B2: ghost entries for recently evicted from T2
  private readonly arcT1: { head: LRUNode<K, V> | null; tail: LRUNode<K, V> | null } = { head: null, tail: null };
  private readonly arcT2: { head: LRUNode<K, V> | null; tail: LRUNode<K, V> | null } = { head: null, tail: null };
  private readonly arcB1 = new Map<K, LRUNode<K, V>>(); // Ghost entries (only key, no value)
  private readonly arcB2 = new Map<K, LRUNode<K, V>>(); // Ghost entries (only key, no value)
  private _arcT1Size = 0; // Actual entries in T1
  private _arcT2Size = 0; // Actual entries in T2
  private _arcP = 0; // Target size for T1 (adaptive parameter)

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
    } else if (this.evictionPolicy === 'arc') {
      // For ARC: move from T1 to T2 if in T1, or move to MRU position in T2 if already in T2
      this.arcOnHit(node);
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
      } else if (this.evictionPolicy === 'arc') {
        this.arcOnHit(existingNode);
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

      // For ARC: check ghost lists before eviction
      if (this.evictionPolicy === 'arc') {
        this.arcOnMiss(key, node, newSize);
      } else {
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
      } else if (this.evictionPolicy === 'arc') {
        this.arcOnHit(existingNode);
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

      // For ARC: check ghost lists before eviction
      if (this.evictionPolicy === 'arc') {
        await this.arcOnMissAsync(key, node, newSize);
      } else {
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

    if (this.evictionPolicy === 'lfu') {
      this.removeFromFrequencyList(node, node.frequency);
    } else if (this.evictionPolicy === 'arc') {
      // Update size counters before removing from list
      if (node.arcList === 't1') {
        this._arcT1Size--;
      } else if (node.arcList === 't2') {
        this._arcT2Size--;
      }
      this.arcRemoveFromList(node);
    } else {
      this.removeNode(node);
    }
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

    // Reset ARC state
    this.arcT1.head = null;
    this.arcT1.tail = null;
    this.arcT2.head = null;
    this.arcT2.tail = null;
    this.arcB1.clear();
    this.arcB2.clear();
    this._arcT1Size = 0;
    this._arcT2Size = 0;
    this._arcP = 0;
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

    // Reset ARC state
    this.arcT1.head = null;
    this.arcT1.tail = null;
    this.arcT2.head = null;
    this.arcT2.tail = null;
    this.arcB1.clear();
    this.arcB2.clear();
    this._arcT1Size = 0;
    this._arcT2Size = 0;
    this._arcP = 0;
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
      while (this.map.size > 0 && this._currentBytes + newSize > this.maxSize) {
        this.evictOne();
      }
    } else {
      // Entry count-based eviction
      while (this.map.size >= this.maxSize) {
        this.evictOne();
      }
    }
  }

  /**
   * Evict one entry based on the eviction policy
   */
  private evictOne(): void {
    if (this.evictionPolicy === 'lfu') {
      this.evictLFU();
    } else if (this.evictionPolicy === 'arc') {
      this.evictARC();
    } else {
      this.evictLRU();
    }
  }

  /**
   * Evict the head (least recently used) entry - LRU policy
   */
  private evictLRU(): void {
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
   * Evict the least frequently used entry - LFU policy
   */
  private evictLFU(): void {
    // Find the list with minimum frequency
    const list = this.frequencyLists.get(this._minFrequency);
    if (!list || !list.head) {
      // Fallback to LRU if frequency lists are empty
      this.evictLRU();
      return;
    }

    // Evict the head of the minimum frequency list (LRU among LFU ties)
    const node = list.head;
    this.removeFromFrequencyList(node, this._minFrequency);
    this.map.delete(node.key);
    this._currentBytes -= node.size;

    // Track eviction
    this._evictions++;

    if (this.onEvict) {
      this.onEvict(node.key, node.value, node.dirty);
    }

    // Update min frequency if the list is now empty
    if (!list.head) {
      this.updateMinFrequency();
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
      while (this.map.size > 0 && this._currentBytes + newSize > this.maxSize) {
        await this.evictOneAsync();
      }
    } else {
      // Entry count-based eviction
      while (this.map.size >= this.maxSize) {
        await this.evictOneAsync();
      }
    }
  }

  /**
   * Evict one entry based on the eviction policy, awaiting async callback
   */
  private async evictOneAsync(): Promise<void> {
    if (this.evictionPolicy === 'lfu') {
      await this.evictLFUAsync();
    } else if (this.evictionPolicy === 'arc') {
      await this.evictARCAsync();
    } else {
      await this.evictLRUAsync();
    }
  }

  /**
   * Evict the head entry (LRU), awaiting async onEvict callback
   */
  private async evictLRUAsync(): Promise<void> {
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

  /**
   * Evict the least frequently used entry, awaiting async callback
   */
  private async evictLFUAsync(): Promise<void> {
    const list = this.frequencyLists.get(this._minFrequency);
    if (!list || !list.head) {
      await this.evictLRUAsync();
      return;
    }

    const node = list.head;
    this.removeFromFrequencyList(node, this._minFrequency);
    this.map.delete(node.key);
    this._currentBytes -= node.size;

    // Track eviction
    this._evictions++;

    if (this.onEvict) {
      await this.onEvict(node.key, node.value, node.dirty);
    }

    if (!list.head) {
      this.updateMinFrequency();
    }
  }

  /**
   * LFU Helper: Add a node to a frequency list
   */
  private addToFrequencyList(node: LRUNode<K, V>, frequency: number): void {
    let list = this.frequencyLists.get(frequency);
    if (!list) {
      list = { head: null, tail: null };
      this.frequencyLists.set(frequency, list);
    }

    // Add to tail of frequency list (most recently used among same frequency)
    node.prev = list.tail;
    node.next = null;

    if (list.tail) {
      list.tail.next = node;
    } else {
      list.head = node;
    }
    list.tail = node;
  }

  /**
   * LFU Helper: Remove a node from a frequency list
   */
  private removeFromFrequencyList(node: LRUNode<K, V>, frequency: number): void {
    const list = this.frequencyLists.get(frequency);
    if (!list) return;

    if (node.prev) {
      node.prev.next = node.next;
    } else {
      list.head = node.next;
    }

    if (node.next) {
      node.next.prev = node.prev;
    } else {
      list.tail = node.prev;
    }

    node.prev = null;
    node.next = null;

    // Clean up empty frequency lists
    if (!list.head) {
      this.frequencyLists.delete(frequency);
    }
  }

  /**
   * LFU Helper: Increment a node's frequency
   */
  private incrementFrequency(node: LRUNode<K, V>): void {
    const oldFreq = node.frequency;
    const newFreq = oldFreq + 1;

    // Remove from old frequency list
    this.removeFromFrequencyList(node, oldFreq);

    // Update node frequency
    node.frequency = newFreq;

    // Add to new frequency list
    this.addToFrequencyList(node, newFreq);

    // Update min frequency if we just emptied the min frequency list
    if (oldFreq === this._minFrequency && !this.frequencyLists.has(oldFreq)) {
      this._minFrequency = newFreq;
    }
  }

  /**
   * LFU Helper: Update min frequency after eviction
   */
  private updateMinFrequency(): void {
    // Find the new minimum frequency
    if (this.frequencyLists.size === 0) {
      this._minFrequency = 0;
      return;
    }

    // Start from current min and find next non-empty frequency
    for (let freq = this._minFrequency; freq <= this._minFrequency + 1000; freq++) {
      if (this.frequencyLists.has(freq)) {
        this._minFrequency = freq;
        return;
      }
    }

    // Fallback: find minimum in all frequencies
    this._minFrequency = Math.min(...this.frequencyLists.keys());
  }

  /**
   * Check memory pressure and invoke callback if level changed
   */
  private checkMemoryPressure(): void {
    if (!this.onMemoryPressure || !this.sizeCalculator) return;

    const ratio = this.memoryUsageRatio;
    let level: MemoryPressureLevel;

    if (ratio >= this.pressureThresholds.high) {
      level = ratio >= 1.0 ? 'critical' : 'high';
    } else if (ratio >= this.pressureThresholds.medium) {
      level = 'medium';
    } else if (ratio >= this.pressureThresholds.low) {
      level = 'low';
    } else {
      level = 'low';
    }

    // Only notify on level change or when entering critical
    if (level !== this._lastPressureLevel || level === 'critical') {
      this._lastPressureLevel = level;
      this.onMemoryPressure({
        currentBytes: this._currentBytes,
        maxBytes: this.maxSize,
        usageRatio: ratio,
        level,
        entryCount: this.map.size,
      });
    }
  }

  /**
   * Manually trigger eviction of N entries.
   * Useful for proactive eviction when memory pressure is high.
   * @param count - Number of entries to evict
   */
  evict(count: number = 1): void {
    for (let i = 0; i < count && this.map.size > 0; i++) {
      this.evictOne();
    }
  }

  /**
   * Manually trigger eviction of N entries, awaiting async callbacks.
   * @param count - Number of entries to evict
   */
  async evictAsync(count: number = 1): Promise<void> {
    for (let i = 0; i < count && this.map.size > 0; i++) {
      await this.evictOneAsync();
    }
  }

  /**
   * Evict entries until memory usage is below the given ratio (0-1).
   * @param targetRatio - Target memory usage ratio
   */
  evictToRatio(targetRatio: number): void {
    if (!this.sizeCalculator) return;

    const targetBytes = this.maxSize * targetRatio;
    while (this.map.size > 0 && this._currentBytes > targetBytes) {
      this.evictOne();
    }
  }

  /**
   * Evict entries until memory usage is below the given ratio, awaiting async callbacks.
   * @param targetRatio - Target memory usage ratio
   */
  async evictToRatioAsync(targetRatio: number): Promise<void> {
    if (!this.sizeCalculator) return;

    const targetBytes = this.maxSize * targetRatio;
    while (this.map.size > 0 && this._currentBytes > targetBytes) {
      await this.evictOneAsync();
    }
  }

  /**
   * Get the frequency of an entry (for LFU policy debugging)
   * @param key - The key to check
   * @returns The access frequency or undefined if not found
   */
  getFrequency(key: K): number | undefined {
    const node = this.map.get(key);
    return node?.frequency;
  }

  // ============================================================================
  // ARC (Adaptive Replacement Cache) Implementation
  // ============================================================================
  //
  // ARC maintains four lists:
  // - T1: Pages seen only once recently (recency)
  // - T2: Pages seen at least twice recently (frequency)
  // - B1: Ghost entries for pages evicted from T1
  // - B2: Ghost entries for pages evicted from T2
  //
  // The parameter p controls the target size of T1, which adapts based on
  // hit patterns in the ghost lists.

  /**
   * Get the current ARC target size for T1 (useful for debugging/monitoring)
   */
  get arcTargetT1Size(): number {
    return this._arcP;
  }

  /**
   * Get the current sizes of ARC lists (useful for debugging/monitoring)
   */
  getARCStats(): { t1Size: number; t2Size: number; b1Size: number; b2Size: number; p: number } {
    return {
      t1Size: this._arcT1Size,
      t2Size: this._arcT2Size,
      b1Size: this.arcB1.size,
      b2Size: this.arcB2.size,
      p: this._arcP,
    };
  }

  /**
   * ARC: Handle a cache hit - move item to T2 (frequency list)
   */
  private arcOnHit(node: LRUNode<K, V>): void {
    if (node.arcList === 't1') {
      // Move from T1 to T2 (item is now frequently accessed)
      this.arcRemoveFromList(node);
      node.arcList = 't2';
      this.arcAddToListTail(this.arcT2, node);
      this._arcT1Size--;
      this._arcT2Size++;
    } else if (node.arcList === 't2') {
      // Already in T2, just move to MRU position
      this.arcRemoveFromList(node);
      this.arcAddToListTail(this.arcT2, node);
    }
  }

  /**
   * ARC: Handle a cache miss (sync version)
   */
  private arcOnMiss(key: K, node: LRUNode<K, V>, newSize: number): void {
    // Check if key is in ghost list B1 (recently evicted from T1)
    if (this.arcB1.has(key)) {
      // Adapt: Increase target size of T1
      const delta = this.arcB2.size >= this.arcB1.size
        ? 1
        : Math.ceil(this.arcB2.size / this.arcB1.size);
      this._arcP = Math.min(this._arcP + delta, this.maxSize);

      // Remove from B1
      this.arcB1.delete(key);

      // Make room in cache by evicting from T2 (since we hit B1)
      this.arcReplace(false, newSize);

      // Add to T2 (since it was in B1, treat as frequently accessed)
      this.map.set(key, node);
      node.arcList = 't2';
      this.arcAddToListTail(this.arcT2, node);
      this._arcT2Size++;
      this._currentBytes += newSize;
      return;
    }

    // Check if key is in ghost list B2 (recently evicted from T2)
    if (this.arcB2.has(key)) {
      // Adapt: Decrease target size of T1
      const delta = this.arcB1.size >= this.arcB2.size
        ? 1
        : Math.ceil(this.arcB1.size / this.arcB2.size);
      this._arcP = Math.max(this._arcP - delta, 0);

      // Remove from B2
      this.arcB2.delete(key);

      // Make room in cache by evicting from T1 (since we hit B2)
      this.arcReplace(true, newSize);

      // Add to T2 (since it was in B2, treat as frequently accessed)
      this.map.set(key, node);
      node.arcList = 't2';
      this.arcAddToListTail(this.arcT2, node);
      this._arcT2Size++;
      this._currentBytes += newSize;
      return;
    }

    // Key is not in any list - completely new item
    // Make room if needed
    this.arcMakeRoom(newSize);

    // Add to T1 (newly seen items go to recency list)
    this.map.set(key, node);
    node.arcList = 't1';
    this.arcAddToListTail(this.arcT1, node);
    this._arcT1Size++;
    this._currentBytes += newSize;
  }

  /**
   * ARC: Handle a cache miss (async version)
   */
  private async arcOnMissAsync(key: K, node: LRUNode<K, V>, newSize: number): Promise<void> {
    // Check if key is in ghost list B1 (recently evicted from T1)
    if (this.arcB1.has(key)) {
      // Adapt: Increase target size of T1
      const delta = this.arcB2.size >= this.arcB1.size
        ? 1
        : Math.ceil(this.arcB2.size / this.arcB1.size);
      this._arcP = Math.min(this._arcP + delta, this.maxSize);

      // Remove from B1
      this.arcB1.delete(key);

      // Make room in cache by evicting from T2 (since we hit B1)
      await this.arcReplaceAsync(false, newSize);

      // Add to T2 (since it was in B1, treat as frequently accessed)
      this.map.set(key, node);
      node.arcList = 't2';
      this.arcAddToListTail(this.arcT2, node);
      this._arcT2Size++;
      this._currentBytes += newSize;
      return;
    }

    // Check if key is in ghost list B2 (recently evicted from T2)
    if (this.arcB2.has(key)) {
      // Adapt: Decrease target size of T1
      const delta = this.arcB1.size >= this.arcB2.size
        ? 1
        : Math.ceil(this.arcB1.size / this.arcB2.size);
      this._arcP = Math.max(this._arcP - delta, 0);

      // Remove from B2
      this.arcB2.delete(key);

      // Make room in cache by evicting from T1 (since we hit B2)
      await this.arcReplaceAsync(true, newSize);

      // Add to T2 (since it was in B2, treat as frequently accessed)
      this.map.set(key, node);
      node.arcList = 't2';
      this.arcAddToListTail(this.arcT2, node);
      this._arcT2Size++;
      this._currentBytes += newSize;
      return;
    }

    // Key is not in any list - completely new item
    // Make room if needed
    await this.arcMakeRoomAsync(newSize);

    // Add to T1 (newly seen items go to recency list)
    this.map.set(key, node);
    node.arcList = 't1';
    this.arcAddToListTail(this.arcT1, node);
    this._arcT1Size++;
    this._currentBytes += newSize;
  }

  /**
   * ARC: Make room for a new entry by evicting from T1 or T2 and managing ghost lists
   */
  private arcMakeRoom(newSize: number): void {
    const cacheSize = this._arcT1Size + this._arcT2Size;

    // If cache is full, we need to evict
    if (this.sizeCalculator) {
      while (this._currentBytes + newSize > this.maxSize && this._arcT1Size + this._arcT2Size > 0) {
        this.arcReplace(this._arcT1Size > this._arcP, newSize);
      }
    } else {
      while (this._arcT1Size + this._arcT2Size >= this.maxSize) {
        this.arcReplace(this._arcT1Size > this._arcP, newSize);
      }
    }

    // If ghost lists are too large, trim them
    // Ghost list size can be up to maxSize each
    while (this.arcB1.size > this.maxSize) {
      const oldest = this.arcB1.keys().next().value;
      if (oldest !== undefined) this.arcB1.delete(oldest);
    }
    while (this.arcB2.size > this.maxSize) {
      const oldest = this.arcB2.keys().next().value;
      if (oldest !== undefined) this.arcB2.delete(oldest);
    }
  }

  /**
   * ARC: Make room for a new entry (async version)
   */
  private async arcMakeRoomAsync(newSize: number): Promise<void> {
    // If cache is full, we need to evict
    if (this.sizeCalculator) {
      while (this._currentBytes + newSize > this.maxSize && this._arcT1Size + this._arcT2Size > 0) {
        await this.arcReplaceAsync(this._arcT1Size > this._arcP, newSize);
      }
    } else {
      while (this._arcT1Size + this._arcT2Size >= this.maxSize) {
        await this.arcReplaceAsync(this._arcT1Size > this._arcP, newSize);
      }
    }

    // If ghost lists are too large, trim them
    while (this.arcB1.size > this.maxSize) {
      const oldest = this.arcB1.keys().next().value;
      if (oldest !== undefined) this.arcB1.delete(oldest);
    }
    while (this.arcB2.size > this.maxSize) {
      const oldest = this.arcB2.keys().next().value;
      if (oldest !== undefined) this.arcB2.delete(oldest);
    }
  }

  /**
   * ARC: Replace (evict) one entry from T1 or T2
   * @param evictFromT1 - If true, prefer evicting from T1; otherwise from T2
   */
  private arcReplace(evictFromT1: boolean, _newSize: number): void {
    // Decide which list to evict from
    let evictT1 = evictFromT1;

    // If T1 is empty, must evict from T2
    if (this._arcT1Size === 0) {
      evictT1 = false;
    }
    // If T2 is empty, must evict from T1
    else if (this._arcT2Size === 0) {
      evictT1 = true;
    }

    if (evictT1 && this.arcT1.head) {
      // Evict LRU from T1, add to B1
      const victim = this.arcT1.head;
      this.arcRemoveFromList(victim);
      this.map.delete(victim.key);
      this._currentBytes -= victim.size;
      this._arcT1Size--;
      this._evictions++;

      // Call eviction callback
      if (this.onEvict) {
        this.onEvict(victim.key, victim.value, victim.dirty);
      }

      // Add to ghost list B1 (we track that this key was recently in T1)
      this.arcB1.set(victim.key, victim);
    } else if (this.arcT2.head) {
      // Evict LRU from T2, add to B2
      const victim = this.arcT2.head;
      this.arcRemoveFromList(victim);
      this.map.delete(victim.key);
      this._currentBytes -= victim.size;
      this._arcT2Size--;
      this._evictions++;

      // Call eviction callback
      if (this.onEvict) {
        this.onEvict(victim.key, victim.value, victim.dirty);
      }

      // Add to ghost list B2 (we track that this key was recently in T2)
      this.arcB2.set(victim.key, victim);
    }
  }

  /**
   * ARC: Replace (evict) one entry from T1 or T2 (async version)
   */
  private async arcReplaceAsync(evictFromT1: boolean, _newSize: number): Promise<void> {
    let evictT1 = evictFromT1;

    if (this._arcT1Size === 0) {
      evictT1 = false;
    } else if (this._arcT2Size === 0) {
      evictT1 = true;
    }

    if (evictT1 && this.arcT1.head) {
      const victim = this.arcT1.head;
      this.arcRemoveFromList(victim);
      this.map.delete(victim.key);
      this._currentBytes -= victim.size;
      this._arcT1Size--;
      this._evictions++;

      if (this.onEvict) {
        await this.onEvict(victim.key, victim.value, victim.dirty);
      }

      this.arcB1.set(victim.key, victim);
    } else if (this.arcT2.head) {
      const victim = this.arcT2.head;
      this.arcRemoveFromList(victim);
      this.map.delete(victim.key);
      this._currentBytes -= victim.size;
      this._arcT2Size--;
      this._evictions++;

      if (this.onEvict) {
        await this.onEvict(victim.key, victim.value, victim.dirty);
      }

      this.arcB2.set(victim.key, victim);
    }
  }

  /**
   * ARC: Evict one entry (used by evictOne)
   */
  private evictARC(): void {
    // Prefer evicting from T1 if T1 size > p, otherwise from T2
    this.arcReplace(this._arcT1Size > this._arcP, 0);
  }

  /**
   * ARC: Evict one entry (async version)
   */
  private async evictARCAsync(): Promise<void> {
    await this.arcReplaceAsync(this._arcT1Size > this._arcP, 0);
  }

  /**
   * ARC: Remove a node from its current list
   */
  private arcRemoveFromList(node: LRUNode<K, V>): void {
    const list = node.arcList === 't1' ? this.arcT1 : this.arcT2;

    if (node.prev) {
      node.prev.next = node.next;
    } else {
      list.head = node.next;
    }

    if (node.next) {
      node.next.prev = node.prev;
    } else {
      list.tail = node.prev;
    }

    node.prev = null;
    node.next = null;
  }

  /**
   * ARC: Add a node to the tail (MRU position) of a list
   */
  private arcAddToListTail(list: { head: LRUNode<K, V> | null; tail: LRUNode<K, V> | null }, node: LRUNode<K, V>): void {
    node.prev = list.tail;
    node.next = null;

    if (list.tail) {
      list.tail.next = node;
    } else {
      list.head = node;
    }

    list.tail = node;
  }
}
