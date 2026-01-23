/**
 * LRU Cache with TTL support
 *
 * A generic LRU (Least Recently Used) cache implementation with time-to-live support.
 * Uses a Map to maintain insertion order (for LRU tracking) and adds TTL-based expiration.
 *
 * @packageDocumentation
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Entry in the LRU cache with timestamp metadata
 * @internal
 */
export interface LRUCacheEntry<T> {
  value: T;
  createdAt: number;
  lastAccessedAt: number;
}

/**
 * Statistics for the LRU cache
 * @public
 */
export interface LRUCacheStats {
  /** Current number of entries in the cache */
  size: number;
  /** Number of cache hits */
  hits: number;
  /** Number of cache misses */
  misses: number;
  /** Number of evictions due to LRU or TTL */
  evictions: number;
  /** Maximum configured cache size */
  maxSize: number;
  /** TTL in milliseconds */
  ttlMs: number;
}

// =============================================================================
// LRU Cache Implementation
// =============================================================================

/**
 * LRU Cache with TTL support.
 *
 * Uses a Map to maintain insertion order (for LRU tracking) and
 * adds TTL-based expiration for entries.
 *
 * @example
 * ```typescript
 * // Create a cache with max 100 entries and 5 minute TTL
 * const cache = new LRUCache<string, string>(100, 5 * 60 * 1000);
 *
 * // Set and get values
 * cache.set('key1', 'value1');
 * const value = cache.get('key1'); // 'value1'
 *
 * // Check existence without updating access time
 * if (cache.has('key1')) {
 *   // ...
 * }
 *
 * // Get cache statistics
 * const stats = cache.getStats();
 * console.log(`Hit rate: ${stats.hits / (stats.hits + stats.misses)}`);
 *
 * // Manual cleanup of expired entries
 * const removed = cache.cleanup();
 * console.log(`Removed ${removed} expired entries`);
 * ```
 *
 * @public
 */
export class LRUCache<K, V> {
  private cache = new Map<K, LRUCacheEntry<V>>();
  private hits = 0;
  private misses = 0;
  private evictions = 0;

  /**
   * Creates a new LRU cache.
   *
   * @param maxSize - Maximum number of entries in the cache
   * @param ttlMs - Time-to-live in milliseconds for cache entries
   */
  constructor(
    private maxSize: number,
    private ttlMs: number
  ) {}

  /**
   * Get a value from the cache, updating its access time if found.
   *
   * @param key - The key to look up
   * @returns The cached value, or undefined if not found or expired
   */
  get(key: K): V | undefined {
    const entry = this.cache.get(key);
    if (!entry) {
      this.misses++;
      return undefined;
    }

    // Check if entry has expired
    const now = Date.now();
    if (now - entry.createdAt > this.ttlMs) {
      this.cache.delete(key);
      this.evictions++;
      this.misses++;
      return undefined;
    }

    // Update last accessed time and move to end (most recently used)
    entry.lastAccessedAt = now;
    this.cache.delete(key);
    this.cache.set(key, entry);
    this.hits++;
    return entry.value;
  }

  /**
   * Set a value in the cache.
   *
   * If the cache is at capacity, the least recently used entry will be evicted.
   *
   * @param key - The key to store
   * @param value - The value to store
   */
  set(key: K, value: V): void {
    const now = Date.now();

    // If key exists, delete it first to update position
    if (this.cache.has(key)) {
      this.cache.delete(key);
    }

    // Evict oldest entries if at capacity
    while (this.cache.size >= this.maxSize) {
      const oldestKey = this.cache.keys().next().value;
      if (oldestKey !== undefined) {
        this.cache.delete(oldestKey);
        this.evictions++;
      }
    }

    this.cache.set(key, {
      value,
      createdAt: now,
      lastAccessedAt: now,
    });
  }

  /**
   * Check if a key exists in the cache (without updating access time).
   *
   * @param key - The key to check
   * @returns `true` if the key exists and is not expired
   */
  has(key: K): boolean {
    const entry = this.cache.get(key);
    if (!entry) return false;

    // Check if entry has expired
    if (Date.now() - entry.createdAt > this.ttlMs) {
      this.cache.delete(key);
      this.evictions++;
      return false;
    }

    return true;
  }

  /**
   * Delete a key from the cache.
   *
   * @param key - The key to delete
   * @returns `true` if the key was deleted, `false` if it didn't exist
   */
  delete(key: K): boolean {
    return this.cache.delete(key);
  }

  /**
   * Get current cache size.
   */
  get size(): number {
    return this.cache.size;
  }

  /**
   * Clear all entries from the cache.
   */
  clear(): void {
    this.cache.clear();
  }

  /**
   * Cleanup expired entries and return count of removed entries.
   *
   * This is typically called periodically by a cleanup timer,
   * but can be called manually to force immediate cleanup.
   *
   * @returns The number of expired entries that were removed
   */
  cleanup(): number {
    const now = Date.now();
    let removed = 0;

    for (const [key, entry] of this.cache) {
      if (now - entry.createdAt > this.ttlMs) {
        this.cache.delete(key);
        this.evictions++;
        removed++;
      }
    }

    return removed;
  }

  /**
   * Get cache statistics.
   *
   * @returns Cache statistics including hits, misses, evictions, and configuration
   */
  getStats(): LRUCacheStats {
    return {
      hits: this.hits,
      misses: this.misses,
      evictions: this.evictions,
      size: this.cache.size,
      maxSize: this.maxSize,
      ttlMs: this.ttlMs,
    };
  }

  /**
   * Update cache configuration.
   *
   * If the new maxSize is smaller than the current size,
   * oldest entries will be evicted to fit.
   *
   * @param maxSize - New maximum cache size
   * @param ttlMs - New TTL in milliseconds
   */
  updateConfig(maxSize: number, ttlMs: number): void {
    this.maxSize = maxSize;
    this.ttlMs = ttlMs;

    // Evict if over new max size
    while (this.cache.size > this.maxSize) {
      const oldestKey = this.cache.keys().next().value;
      if (oldestKey !== undefined) {
        this.cache.delete(oldestKey);
        this.evictions++;
      }
    }
  }
}
