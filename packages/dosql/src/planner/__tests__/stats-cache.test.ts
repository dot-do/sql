/**
 * DoSQL Statistics Cache Tests
 *
 * Tests for CachedStatisticsStore TTL-based caching behavior:
 * - Cache hits within TTL
 * - Cache misses after TTL expiry
 * - DDL invalidation (ALTER TABLE, CREATE INDEX, DROP TABLE, DROP INDEX)
 * - Cache diagnostics
 * - Capacity eviction
 *
 * Issue: sql-kgpt - Add statistics caching to reduce planning overhead
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  CachedStatisticsStore,
  createCachedStatisticsStore,
  TableStatisticsBuilder,
  ColumnStatisticsBuilder,
  IndexStatisticsBuilder,
  type TableStatistics,
  type IndexStatistics,
} from '../stats.js';

// =============================================================================
// TEST HELPERS
// =============================================================================

function makeTableStats(tableName: string, rowCount: number): TableStatistics {
  return new TableStatisticsBuilder(tableName)
    .setRowCount(rowCount)
    .setPageCount(Math.ceil(rowCount / 50))
    .setAvgRowSize(100)
    .setTotalBytes(rowCount * 100)
    .addColumn(
      new ColumnStatisticsBuilder('id')
        .setDistinctCount(rowCount)
        .setNullFraction(0)
        .setAvgWidth(8)
        .build()
    )
    .addColumn(
      new ColumnStatisticsBuilder('name')
        .setDistinctCount(Math.ceil(rowCount * 0.8))
        .setNullFraction(0.01)
        .setAvgWidth(32)
        .build()
    )
    .build();
}

function makeIndexStats(indexName: string, tableName: string, entryCount: number): IndexStatistics {
  return new IndexStatisticsBuilder(indexName, tableName)
    .setEntryCount(entryCount)
    .setDistinctKeys(entryCount)
    .setTreeHeight(3)
    .setLeafPages(Math.ceil(entryCount / 100))
    .setTotalPages(Math.ceil(entryCount / 100) + 3)
    .build();
}

// =============================================================================
// BASIC CACHE BEHAVIOR
// =============================================================================

describe('CachedStatisticsStore - Basic Caching', () => {
  let cache: CachedStatisticsStore;

  beforeEach(() => {
    vi.useFakeTimers();
    cache = createCachedStatisticsStore({ ttlMs: 5000 });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should return table stats from backing store on first access', () => {
    const stats = makeTableStats('users', 1000);
    cache.setTableStats(stats);

    const result = cache.getTableStats('users');
    expect(result).toBeDefined();
    expect(result!.tableName).toBe('users');
    expect(result!.rowCount).toBe(1000);
  });

  it('should return cached table stats on subsequent access within TTL', () => {
    const stats = makeTableStats('users', 1000);
    cache.setTableStats(stats);

    // First access: populates cache
    cache.getTableStats('users');

    // Advance time but stay within TTL
    vi.advanceTimersByTime(3000);

    // Second access: should be a cache hit
    const result = cache.getTableStats('users');
    expect(result).toBeDefined();
    expect(result!.rowCount).toBe(1000);

    const diag = cache.getDiagnostics();
    // First getTableStats is a miss (cache populated by setTableStats, but get still reads from cache)
    // Actually setTableStats populates cache, so first get is a hit
    expect(diag.hits).toBeGreaterThanOrEqual(1);
  });

  it('should return fresh stats after TTL expires', () => {
    let callCount = 0;
    cache.setTableStatsProvider((tableName: string) => {
      callCount++;
      return makeTableStats(tableName, 1000 + callCount * 100);
    });

    // First access: cache miss, calls provider
    const first = cache.getTableStats('users');
    expect(first).toBeDefined();
    expect(first!.rowCount).toBe(1100);
    expect(callCount).toBe(1);

    // Within TTL: cache hit
    vi.advanceTimersByTime(2000);
    const second = cache.getTableStats('users');
    expect(second!.rowCount).toBe(1100); // Same cached value
    expect(callCount).toBe(1); // No new provider call

    // After TTL: cache miss, calls provider again
    vi.advanceTimersByTime(4000); // Total 6000ms > 5000ms TTL
    const third = cache.getTableStats('users');
    expect(third!.rowCount).toBe(1200); // Fresh value
    expect(callCount).toBe(2);
  });

  it('should cache index stats with TTL', () => {
    const stats = makeIndexStats('idx_users_email', 'users', 500);
    cache.setIndexStats(stats);

    // Should be cached
    const result = cache.getIndexStats('idx_users_email');
    expect(result).toBeDefined();
    expect(result!.entryCount).toBe(500);

    // After TTL, should refetch
    vi.advanceTimersByTime(6000);

    // With no provider and no backing data change, it returns from parent
    const afterTTL = cache.getIndexStats('idx_users_email');
    expect(afterTTL).toBeDefined();
  });

  it('should return undefined for non-existent table stats', () => {
    const result = cache.getTableStats('nonexistent');
    expect(result).toBeUndefined();
  });

  it('should return undefined for non-existent index stats', () => {
    const result = cache.getIndexStats('nonexistent');
    expect(result).toBeUndefined();
  });
});

// =============================================================================
// DDL INVALIDATION
// =============================================================================

describe('CachedStatisticsStore - DDL Invalidation', () => {
  let cache: CachedStatisticsStore;

  beforeEach(() => {
    vi.useFakeTimers();
    cache = createCachedStatisticsStore({ ttlMs: 5000 });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should invalidate table cache on ALTER TABLE', () => {
    cache.setTableStats(makeTableStats('users', 1000));
    expect(cache.getTableStats('users')).toBeDefined();

    cache.invalidateForDDL('ALTER TABLE users ADD COLUMN email TEXT');

    // Cache entry removed, must re-fetch from backing store
    const diag = cache.getDiagnostics();
    // The invalidation removed the cache entry
    expect(diag.tableEntries).toBe(0);
  });

  it('should invalidate table cache on DROP TABLE', () => {
    cache.setTableStats(makeTableStats('temp_data', 500));
    expect(cache.getTableStats('temp_data')).toBeDefined();

    cache.invalidateForDDL('DROP TABLE temp_data');

    const diag = cache.getDiagnostics();
    expect(diag.tableEntries).toBe(0);
  });

  it('should invalidate table cache on DROP TABLE IF EXISTS', () => {
    cache.setTableStats(makeTableStats('staging', 200));
    expect(cache.getTableStats('staging')).toBeDefined();

    cache.invalidateForDDL('DROP TABLE IF EXISTS staging');

    const diag = cache.getDiagnostics();
    expect(diag.tableEntries).toBe(0);
  });

  it('should invalidate table and index cache on CREATE INDEX', () => {
    cache.setTableStats(makeTableStats('users', 1000));
    cache.setIndexStats(makeIndexStats('idx_users_email', 'users', 1000));

    cache.invalidateForDDL('CREATE INDEX idx_users_name ON users(name)');

    // Table cache should be invalidated (stats may change with new index)
    const diag = cache.getDiagnostics();
    expect(diag.tableEntries).toBe(0);
  });

  it('should invalidate index cache on DROP INDEX', () => {
    cache.setIndexStats(makeIndexStats('idx_users_email', 'users', 1000));

    cache.invalidateForDDL('DROP INDEX idx_users_email ON users');

    const diag = cache.getDiagnostics();
    expect(diag.indexEntries).toBe(0);
  });

  it('should invalidate index cache on DROP INDEX IF EXISTS', () => {
    cache.setIndexStats(makeIndexStats('idx_users_email', 'users', 1000));

    cache.invalidateForDDL('DROP INDEX IF EXISTS idx_users_email ON users');

    const diag = cache.getDiagnostics();
    expect(diag.indexEntries).toBe(0);
  });

  it('should not invalidate unrelated tables', () => {
    cache.setTableStats(makeTableStats('users', 1000));
    cache.setTableStats(makeTableStats('orders', 5000));

    cache.invalidateForDDL('ALTER TABLE users ADD COLUMN status TEXT');

    // Users should be invalidated, orders should remain
    expect(cache.getDiagnostics().tableEntries).toBe(1);

    // Orders should still be cached
    const orders = cache.getTableStats('orders');
    expect(orders).toBeDefined();
    expect(orders!.rowCount).toBe(5000);
  });

  it('should invalidate table entry via invalidateTable directly', () => {
    cache.setTableStats(makeTableStats('users', 1000));
    cache.setIndexStats(makeIndexStats('idx_users_id', 'users', 1000));

    cache.invalidateTable('users');

    // Both table and related index caches should be cleared
    const diag = cache.getDiagnostics();
    expect(diag.tableEntries).toBe(0);
    expect(diag.indexEntries).toBe(0);
  });

  it('should invalidate index entry via invalidateIndex directly', () => {
    cache.setIndexStats(makeIndexStats('idx_users_email', 'users', 1000));

    cache.invalidateIndex('idx_users_email');

    const diag = cache.getDiagnostics();
    expect(diag.indexEntries).toBe(0);
  });
});

// =============================================================================
// CACHE DIAGNOSTICS
// =============================================================================

describe('CachedStatisticsStore - Diagnostics', () => {
  let cache: CachedStatisticsStore;

  beforeEach(() => {
    vi.useFakeTimers();
    cache = createCachedStatisticsStore({ ttlMs: 5000 });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should track cache hits and misses', () => {
    cache.setTableStats(makeTableStats('users', 1000));

    // First get after set: cache hit (setTableStats populates cache)
    cache.getTableStats('users');
    // Second get: cache hit
    cache.getTableStats('users');
    // Non-existent: cache miss
    cache.getTableStats('nonexistent');

    const diag = cache.getDiagnostics();
    expect(diag.hits).toBe(2);
    expect(diag.misses).toBe(1);
    expect(diag.hitRate).toBeCloseTo(66.67, 0);
  });

  it('should report correct entry counts', () => {
    cache.setTableStats(makeTableStats('users', 1000));
    cache.setTableStats(makeTableStats('orders', 5000));
    cache.setIndexStats(makeIndexStats('idx_users_id', 'users', 1000));

    const diag = cache.getDiagnostics();
    expect(diag.tableEntries).toBe(2);
    expect(diag.indexEntries).toBe(1);
    expect(diag.ttlMs).toBe(5000);
  });

  it('should reset diagnostics on clearCache', () => {
    cache.setTableStats(makeTableStats('users', 1000));
    cache.getTableStats('users');
    cache.getTableStats('nonexistent');

    cache.clearCache();

    const diag = cache.getDiagnostics();
    expect(diag.tableEntries).toBe(0);
    expect(diag.indexEntries).toBe(0);
    expect(diag.hits).toBe(0);
    expect(diag.misses).toBe(0);
  });
});

// =============================================================================
// CAPACITY AND EVICTION
// =============================================================================

describe('CachedStatisticsStore - Capacity Eviction', () => {
  let cache: CachedStatisticsStore;

  beforeEach(() => {
    vi.useFakeTimers();
    cache = createCachedStatisticsStore({
      ttlMs: 5000,
      maxTableEntries: 3,
      maxIndexEntries: 3,
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should evict oldest table entry when capacity is reached', () => {
    // Insert 3 entries at different times
    cache.setTableStats(makeTableStats('table_a', 100));
    vi.advanceTimersByTime(100);
    cache.setTableStats(makeTableStats('table_b', 200));
    vi.advanceTimersByTime(100);
    cache.setTableStats(makeTableStats('table_c', 300));

    expect(cache.getDiagnostics().tableEntries).toBe(3);

    // Insert 4th entry - should evict table_a (oldest)
    vi.advanceTimersByTime(100);
    cache.setTableStats(makeTableStats('table_d', 400));

    expect(cache.getDiagnostics().tableEntries).toBe(3);

    // table_a should have been evicted from cache
    // (but still in backing store via super.setTableStats)
    // table_d should be cached
    const d = cache.getTableStats('table_d');
    expect(d).toBeDefined();
    expect(d!.rowCount).toBe(400);
  });

  it('should evict oldest index entry when capacity is reached', () => {
    cache.setIndexStats(makeIndexStats('idx_a', 'table_a', 100));
    vi.advanceTimersByTime(100);
    cache.setIndexStats(makeIndexStats('idx_b', 'table_b', 200));
    vi.advanceTimersByTime(100);
    cache.setIndexStats(makeIndexStats('idx_c', 'table_c', 300));

    expect(cache.getDiagnostics().indexEntries).toBe(3);

    vi.advanceTimersByTime(100);
    cache.setIndexStats(makeIndexStats('idx_d', 'table_d', 400));

    expect(cache.getDiagnostics().indexEntries).toBe(3);
  });
});

// =============================================================================
// DERIVED STATISTICS METHODS
// =============================================================================

describe('CachedStatisticsStore - Derived Statistics', () => {
  let cache: CachedStatisticsStore;

  beforeEach(() => {
    vi.useFakeTimers();
    cache = createCachedStatisticsStore({ ttlMs: 5000 });
    cache.setTableStats(makeTableStats('users', 1000));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should return cached row count', () => {
    expect(cache.getRowCount('users')).toBe(1000);
    expect(cache.getRowCount('nonexistent')).toBe(1000); // default
    expect(cache.getRowCount('nonexistent', 500)).toBe(500); // custom default
  });

  it('should return cached distinct count', () => {
    const distinct = cache.getDistinctCount('users', 'id');
    expect(distinct).toBe(1000); // id has distinctCount = rowCount
  });

  it('should return cached null fraction', () => {
    const nullFraction = cache.getNullFraction('users', 'name');
    expect(nullFraction).toBeCloseTo(0.01);
  });

  it('should check hasTableStats using cache', () => {
    expect(cache.hasTableStats('users')).toBe(true);
    expect(cache.hasTableStats('nonexistent')).toBe(false);
  });

  it('should check hasIndexStats using cache', () => {
    cache.setIndexStats(makeIndexStats('idx_users_id', 'users', 1000));
    expect(cache.hasIndexStats('idx_users_id')).toBe(true);
    expect(cache.hasIndexStats('nonexistent')).toBe(false);
  });
});

// =============================================================================
// PROVIDER INTEGRATION
// =============================================================================

describe('CachedStatisticsStore - Provider Integration', () => {
  let cache: CachedStatisticsStore;

  beforeEach(() => {
    vi.useFakeTimers();
    cache = createCachedStatisticsStore({ ttlMs: 5000 });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should call table stats provider on cache miss', () => {
    const provider = vi.fn((tableName: string) => makeTableStats(tableName, 2000));
    cache.setTableStatsProvider(provider);

    const result = cache.getTableStats('products');
    expect(result).toBeDefined();
    expect(result!.rowCount).toBe(2000);
    expect(provider).toHaveBeenCalledWith('products');
    expect(provider).toHaveBeenCalledTimes(1);

    // Second call within TTL should not call provider
    cache.getTableStats('products');
    expect(provider).toHaveBeenCalledTimes(1);
  });

  it('should call index stats provider on cache miss', () => {
    const provider = vi.fn((indexName: string) => makeIndexStats(indexName, 'products', 3000));
    cache.setIndexStatsProvider(provider);

    const result = cache.getIndexStats('idx_products_sku');
    expect(result).toBeDefined();
    expect(result!.entryCount).toBe(3000);
    expect(provider).toHaveBeenCalledWith('idx_products_sku');
    expect(provider).toHaveBeenCalledTimes(1);

    // Second call within TTL should not call provider
    cache.getIndexStats('idx_products_sku');
    expect(provider).toHaveBeenCalledTimes(1);
  });

  it('should re-call provider after TTL expires', () => {
    let rowCount = 1000;
    const provider = vi.fn((tableName: string) => makeTableStats(tableName, rowCount++));
    cache.setTableStatsProvider(provider);

    cache.getTableStats('users');
    expect(provider).toHaveBeenCalledTimes(1);

    // Advance past TTL
    vi.advanceTimersByTime(6000);

    const result = cache.getTableStats('users');
    expect(provider).toHaveBeenCalledTimes(2);
    expect(result!.rowCount).toBe(1001);
  });

  it('should re-call provider after invalidation', () => {
    let rowCount = 1000;
    const provider = vi.fn((tableName: string) => makeTableStats(tableName, rowCount++));
    cache.setTableStatsProvider(provider);

    cache.getTableStats('users');
    expect(provider).toHaveBeenCalledTimes(1);

    // Invalidate
    cache.invalidateTable('users');

    // Should call provider again even within TTL
    const result = cache.getTableStats('users');
    expect(provider).toHaveBeenCalledTimes(2);
    expect(result!.rowCount).toBe(1001);
  });
});

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

describe('createCachedStatisticsStore', () => {
  it('should create a CachedStatisticsStore with default config', () => {
    const store = createCachedStatisticsStore();
    expect(store).toBeInstanceOf(CachedStatisticsStore);
    expect(store.getDiagnostics().ttlMs).toBe(5000);
  });

  it('should create a CachedStatisticsStore with custom config', () => {
    const store = createCachedStatisticsStore({ ttlMs: 10000 });
    expect(store.getDiagnostics().ttlMs).toBe(10000);
  });
});
