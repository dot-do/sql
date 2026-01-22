/**
 * R2 Caching Layer
 *
 * Provides a caching decorator for R2 storage backends.
 * Supports in-memory caching with TTL and size limits.
 *
 * Design principles:
 * - Transparent caching - doesn't change the API
 * - Configurable eviction policies (LRU, TTL)
 * - Memory-safe with configurable limits
 * - ETag-based cache validation
 */

import type { FSXBackendWithMeta, FSXMetadata, ByteRange } from './types.js';

// =============================================================================
// Cache Entry Types
// =============================================================================

/**
 * Cached data entry
 */
interface CacheEntry {
  /** The cached data */
  data: Uint8Array;
  /** When this entry was cached */
  cachedAt: number;
  /** ETag for cache validation */
  etag?: string;
  /** Last access time (for LRU) */
  lastAccessed: number;
  /** Access count (for LFU) */
  accessCount: number;
  /** Size in bytes */
  size: number;
}

/**
 * Cache statistics
 */
export interface CacheStats {
  /** Total number of entries */
  entryCount: number;
  /** Total size of cached data in bytes */
  totalSize: number;
  /** Cache hit count */
  hits: number;
  /** Cache miss count */
  misses: number;
  /** Hit ratio (0-1) */
  hitRatio: number;
  /** Number of evictions */
  evictions: number;
}

/**
 * Cache configuration
 */
export interface CacheConfig {
  /** Maximum cache size in bytes (default: 50MB) */
  maxSize: number;
  /** Maximum number of entries (default: 1000) */
  maxEntries: number;
  /** Time-to-live in milliseconds (default: 5 minutes) */
  ttlMs: number;
  /** Whether to validate cache using ETags (default: true) */
  validateEtags: boolean;
  /** Eviction policy (default: 'lru') */
  evictionPolicy: 'lru' | 'lfu' | 'fifo';
  /** Maximum file size to cache (default: 1MB) */
  maxEntrySize: number;
}

/**
 * Default cache configuration
 */
export const DEFAULT_CACHE_CONFIG: CacheConfig = {
  maxSize: 50 * 1024 * 1024, // 50MB
  maxEntries: 1000,
  ttlMs: 5 * 60 * 1000, // 5 minutes
  validateEtags: true,
  evictionPolicy: 'lru',
  maxEntrySize: 1024 * 1024, // 1MB
};

// =============================================================================
// Cache Implementation
// =============================================================================

/**
 * In-memory cache for R2 data
 */
export class R2Cache {
  private readonly cache = new Map<string, CacheEntry>();
  private readonly config: CacheConfig;
  private stats = {
    hits: 0,
    misses: 0,
    evictions: 0,
  };
  private currentSize = 0;

  constructor(config: Partial<CacheConfig> = {}) {
    this.config = { ...DEFAULT_CACHE_CONFIG, ...config };
  }

  /**
   * Get cached data if available and valid
   */
  get(key: string): { data: Uint8Array; etag?: string } | null {
    const entry = this.cache.get(key);

    if (!entry) {
      this.stats.misses++;
      return null;
    }

    // Check TTL
    if (Date.now() - entry.cachedAt > this.config.ttlMs) {
      this.remove(key);
      this.stats.misses++;
      return null;
    }

    // Update access tracking
    entry.lastAccessed = Date.now();
    entry.accessCount++;

    this.stats.hits++;
    return { data: entry.data, etag: entry.etag };
  }

  /**
   * Store data in cache
   */
  set(key: string, data: Uint8Array, etag?: string): void {
    // Don't cache if entry is too large
    if (data.length > this.config.maxEntrySize) {
      return;
    }

    // Remove existing entry if present
    if (this.cache.has(key)) {
      this.remove(key);
    }

    // Evict entries if needed
    while (this.shouldEvict(data.length)) {
      this.evictOne();
    }

    // Store new entry
    const entry: CacheEntry = {
      data: data.slice(), // Copy to prevent external mutation
      cachedAt: Date.now(),
      etag,
      lastAccessed: Date.now(),
      accessCount: 1,
      size: data.length,
    };

    this.cache.set(key, entry);
    this.currentSize += data.length;
  }

  /**
   * Remove an entry from cache
   */
  remove(key: string): boolean {
    const entry = this.cache.get(key);
    if (entry) {
      this.currentSize -= entry.size;
      this.cache.delete(key);
      return true;
    }
    return false;
  }

  /**
   * Check if key exists in cache (without affecting stats)
   */
  has(key: string): boolean {
    const entry = this.cache.get(key);
    if (!entry) return false;

    // Check TTL
    if (Date.now() - entry.cachedAt > this.config.ttlMs) {
      return false;
    }

    return true;
  }

  /**
   * Invalidate entries matching a prefix
   */
  invalidatePrefix(prefix: string): number {
    let count = 0;
    for (const key of this.cache.keys()) {
      if (key.startsWith(prefix)) {
        this.remove(key);
        count++;
      }
    }
    return count;
  }

  /**
   * Clear all entries
   */
  clear(): void {
    this.cache.clear();
    this.currentSize = 0;
  }

  /**
   * Get cache statistics
   */
  getStats(): CacheStats {
    const total = this.stats.hits + this.stats.misses;
    return {
      entryCount: this.cache.size,
      totalSize: this.currentSize,
      hits: this.stats.hits,
      misses: this.stats.misses,
      hitRatio: total > 0 ? this.stats.hits / total : 0,
      evictions: this.stats.evictions,
    };
  }

  /**
   * Reset statistics
   */
  resetStats(): void {
    this.stats = { hits: 0, misses: 0, evictions: 0 };
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  private shouldEvict(newEntrySize: number): boolean {
    if (this.cache.size >= this.config.maxEntries) {
      return true;
    }
    if (this.currentSize + newEntrySize > this.config.maxSize) {
      return true;
    }
    return false;
  }

  private evictOne(): void {
    if (this.cache.size === 0) return;

    const keyToEvict = this.selectEvictionCandidate();
    if (keyToEvict) {
      this.remove(keyToEvict);
      this.stats.evictions++;
    }
  }

  private selectEvictionCandidate(): string | null {
    if (this.cache.size === 0) return null;

    let candidate: string | null = null;
    let candidateScore = Infinity;

    for (const [key, entry] of this.cache.entries()) {
      const score = this.calculateEvictionScore(entry);
      if (score < candidateScore) {
        candidateScore = score;
        candidate = key;
      }
    }

    return candidate;
  }

  private calculateEvictionScore(entry: CacheEntry): number {
    switch (this.config.evictionPolicy) {
      case 'lru':
        // Lower lastAccessed = higher priority for eviction
        return entry.lastAccessed;

      case 'lfu':
        // Lower accessCount = higher priority for eviction
        return entry.accessCount;

      case 'fifo':
        // Lower cachedAt = higher priority for eviction
        return entry.cachedAt;

      default:
        return entry.lastAccessed;
    }
  }
}

// =============================================================================
// Caching Backend Decorator
// =============================================================================

/**
 * Wraps an FSXBackendWithMeta with caching
 */
export class CachingBackend implements FSXBackendWithMeta {
  private readonly backend: FSXBackendWithMeta;
  private readonly cache: R2Cache;
  private readonly config: CacheConfig;

  constructor(
    backend: FSXBackendWithMeta,
    config: Partial<CacheConfig> = {}
  ) {
    this.backend = backend;
    this.config = { ...DEFAULT_CACHE_CONFIG, ...config };
    this.cache = new R2Cache(this.config);
  }

  /**
   * Read data with caching
   */
  async read(path: string, range?: ByteRange): Promise<Uint8Array | null> {
    // Don't cache range reads - they're typically for large files
    if (range) {
      return this.backend.read(path, range);
    }

    // Check cache first
    const cached = this.cache.get(path);
    if (cached) {
      // Optionally validate with ETag
      if (this.config.validateEtags && cached.etag) {
        const meta = await this.backend.metadata(path);
        if (meta?.etag !== cached.etag) {
          // ETag changed, invalidate cache
          this.cache.remove(path);
        } else {
          return cached.data;
        }
      } else {
        return cached.data;
      }
    }

    // Cache miss - fetch from backend
    const data = await this.backend.read(path);
    if (data && data.length <= this.config.maxEntrySize) {
      const meta = await this.backend.metadata(path);
      this.cache.set(path, data, meta?.etag);
    }

    return data;
  }

  /**
   * Write data and invalidate cache
   */
  async write(path: string, data: Uint8Array): Promise<void> {
    await this.backend.write(path, data);
    // Invalidate cache on write
    this.cache.remove(path);
  }

  /**
   * Delete data and invalidate cache
   */
  async delete(path: string): Promise<void> {
    await this.backend.delete(path);
    this.cache.remove(path);
  }

  /**
   * List files (pass through to backend)
   */
  async list(prefix: string): Promise<string[]> {
    return this.backend.list(prefix);
  }

  /**
   * Check existence (can use cache)
   */
  async exists(path: string): Promise<boolean> {
    if (this.cache.has(path)) {
      return true;
    }
    return this.backend.exists(path);
  }

  /**
   * Get metadata (pass through to backend)
   */
  async metadata(path: string): Promise<FSXMetadata | null> {
    return this.backend.metadata(path);
  }

  /**
   * Get cache statistics
   */
  getCacheStats(): CacheStats {
    return this.cache.getStats();
  }

  /**
   * Clear the cache
   */
  clearCache(): void {
    this.cache.clear();
  }

  /**
   * Invalidate cache entries by prefix
   */
  invalidateCachePrefix(prefix: string): number {
    return this.cache.invalidatePrefix(prefix);
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a caching wrapper around an FSX backend
 */
export function withCache(
  backend: FSXBackendWithMeta,
  config?: Partial<CacheConfig>
): CachingBackend {
  return new CachingBackend(backend, config);
}
