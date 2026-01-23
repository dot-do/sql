/**
 * Cache Invalidation for DoLake
 *
 * Provides query result cache invalidation based on CDC events.
 * Works alongside MetadataCache to provide comprehensive caching.
 *
 * @module dolake/cache-invalidation
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Cache invalidator configuration
 */
export interface CacheInvalidatorConfig {
  /** Enable query result caching */
  enabled: boolean;
  /** TTL for cached results in milliseconds */
  ttlMs: number;
}

/**
 * CDC event for cache invalidation
 */
export interface CDCEvent {
  operation: 'INSERT' | 'UPDATE' | 'DELETE';
  table: string;
  before?: Record<string, unknown>;
  after?: Record<string, unknown>;
  timestamp: number;
  sequence: number;
  rowId: string;
}

/**
 * Cache entry status
 */
export interface CacheEntryStatus {
  tableId: string;
  cached: boolean;
  lastInvalidatedAt: number | null;
  invalidationCount: number;
}

// =============================================================================
// CacheInvalidator Implementation
// =============================================================================

/**
 * Query result cache invalidator that reacts to CDC events
 */
export class CacheInvalidator {
  private config: CacheInvalidatorConfig;
  private entries: Map<
    string,
    {
      cached: boolean;
      lastInvalidatedAt: number | null;
      invalidationCount: number;
    }
  > = new Map();

  constructor(config: CacheInvalidatorConfig) {
    this.config = config;
  }

  /**
   * Register a cache entry for a table
   */
  registerCacheEntry(tableId: string): void {
    this.entries.set(tableId, {
      cached: true,
      lastInvalidatedAt: null,
      invalidationCount: 0,
    });
  }

  /**
   * Process CDC events and invalidate affected caches
   */
  async processCDCEvents(events: CDCEvent[]): Promise<void> {
    const now = Date.now();

    for (const event of events) {
      const entry = this.entries.get(event.table);
      if (entry) {
        // Data modification events (INSERT, UPDATE, DELETE) invalidate query cache
        entry.cached = false;
        entry.lastInvalidatedAt = now;
        entry.invalidationCount++;
      }
    }
  }

  /**
   * Get cache entry status
   */
  getCacheEntryStatus(tableId: string): CacheEntryStatus | null {
    const entry = this.entries.get(tableId);
    if (!entry) {
      return null;
    }

    return {
      tableId,
      cached: entry.cached,
      lastInvalidatedAt: entry.lastInvalidatedAt,
      invalidationCount: entry.invalidationCount,
    };
  }

  /**
   * Invalidate cache entry for a table
   */
  invalidate(tableId: string): void {
    const entry = this.entries.get(tableId);
    if (entry) {
      entry.cached = false;
      entry.lastInvalidatedAt = Date.now();
      entry.invalidationCount++;
    }
  }

  /**
   * Mark cache entry as valid
   */
  markValid(tableId: string): void {
    const entry = this.entries.get(tableId);
    if (entry) {
      entry.cached = true;
    }
  }

  /**
   * Clear all cache entries
   */
  clear(): void {
    this.entries.clear();
  }
}
