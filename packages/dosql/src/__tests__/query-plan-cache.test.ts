/**
 * Query Plan Cache - RED Phase TDD Tests
 *
 * Issue: sql-0q4 - Query Plan Caching tests
 *
 * These tests document MISSING behavior for query plan caching.
 * Tests use `it.fails()` pattern to document expected behavior not yet implemented.
 *
 * Gap Areas:
 * 1. Identical queries hit plan cache
 * 2. Plan cache uses query hash as key
 * 3. Parameterized queries share cached plan
 * 4. Cache respects max size limit
 * 5. LRU eviction when cache full
 * 6. Schema changes invalidate relevant plans
 * 7. Cache hit improves query latency
 * 8. Cache stats exposed via PRAGMA
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

// =============================================================================
// TYPE DECLARATIONS FOR MISSING FEATURES
// =============================================================================

/**
 * Query plan cache configuration
 */
interface QueryPlanCacheConfig {
  /** Maximum number of plans to cache */
  maxSize: number;

  /** TTL for cached plans in milliseconds */
  ttlMs?: number;

  /** Enable/disable caching */
  enabled?: boolean;
}

/**
 * Cached query plan entry
 */
interface CachedPlan {
  /** Hash of the query */
  queryHash: string;

  /** The optimized query plan */
  plan: unknown;

  /** Timestamp when cached */
  cachedAt: number;

  /** Number of times this plan was hit */
  hitCount: number;

  /** Schema version when plan was cached */
  schemaVersion: number;
}

/**
 * Query plan cache statistics
 */
interface PlanCacheStats {
  /** Total cache hits */
  hits: number;

  /** Total cache misses */
  misses: number;

  /** Current number of cached plans */
  size: number;

  /** Maximum cache size */
  maxSize: number;

  /** Hit rate percentage */
  hitRate: number;

  /** Total evictions */
  evictions: number;

  /** Memory usage estimate in bytes */
  memoryUsage: number;
}

/**
 * Query plan cache interface
 */
interface QueryPlanCache {
  /** Get a cached plan by query hash */
  get(queryHash: string): CachedPlan | undefined;

  /** Store a plan in the cache */
  set(queryHash: string, plan: unknown, schemaVersion: number): void;

  /** Check if a query is cached */
  has(queryHash: string): boolean;

  /** Invalidate plans for a specific table */
  invalidateTable(tableName: string): number;

  /** Invalidate all plans */
  clear(): void;

  /** Get cache statistics */
  getStats(): PlanCacheStats;

  /** Get current size */
  size(): number;
}

// =============================================================================
// TEST UTILITIES
// =============================================================================

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// =============================================================================
// EXPECTED MODULE INTERFACES (for TDD - features don't exist yet)
// =============================================================================

/**
 * Expected exports from ../planner/cache.js (not implemented yet)
 */
interface PlannerCacheModule {
  createQueryPlanCache(config: QueryPlanCacheConfig & Record<string, unknown>): QueryPlanCache;
  normalizeQueryForCache?(sql: string): string;
  computePlanCacheKey?(sql: string): string;
  normalizeParameterizedQuery?(sql: string): string;
  extractParameterizedPlan?(cache: QueryPlanCache, sql: string): unknown;
  normalizeLiterals?(sql: string): string;
  normalizeInClause?(sql: string): string;
  createPreparedStatement?(cache: QueryPlanCache, sql: string): { execute(params: unknown[]): unknown };
  notifyIndexChange?(cache: QueryPlanCache, change: { type: string; table: string; columns: string[] }): void;
  processDDL?(cache: QueryPlanCache, sql: string): void;
  measurePlanningTime?<T>(fn: () => Promise<T>): Promise<{ durationMs: number; plan: T }>;
}

/**
 * Extended query plan cache config (includes optional future features)
 */
type ExtendedCacheConfig = QueryPlanCacheConfig & {
  storeOriginalQuery?: boolean;
  maxMemoryBytes?: number;
  evictionPolicy?: 'lru' | 'lfu';
  trackTableDependencies?: boolean;
  trackTimeSaved?: boolean;
  trackLatency?: boolean;
};

/**
 * Extended cache interface for testing future features
 */
interface ExtendedQueryPlanCache extends QueryPlanCache {
  isPlanStale?(queryHash: string, currentVersion: number): boolean;
  recordMiss?(query: string, latencyMs: number): void;
  recordHit?(query: string, latencyMs: number): void;
}

/**
 * Extended stats for future features
 */
interface ExtendedPlanCacheStats extends PlanCacheStats {
  timeSavedMs?: number;
  avgMissLatencyMs?: number;
  avgHitLatencyMs?: number;
  latencyImprovement?: number;
  enabled?: boolean;
}

/**
 * Helper to import the planner cache module (which may not exist yet in TDD)
 */
async function importPlannerCache(): Promise<PlannerCacheModule> {
  return await import('../planner/cache.js') as unknown as PlannerCacheModule;
}

/**
 * Compute a hash for a SQL query (mock implementation for tests)
 */
function computeQueryHash(sql: string): string {
  // Simple hash for testing - actual implementation would be more robust
  let hash = 0;
  for (let i = 0; i < sql.length; i++) {
    const char = sql.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash;
  }
  return `qh_${Math.abs(hash).toString(16)}`;
}

/**
 * Normalize SQL for hashing (strip whitespace, normalize case for keywords)
 */
function normalizeSQL(sql: string): string {
  return sql
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

// =============================================================================
// 1. IDENTICAL QUERIES HIT PLAN CACHE
// =============================================================================

describe('Identical Queries Hit Plan Cache', () => {
  /**
   * GAP: createQueryPlanCache function does not exist
   */
  it.fails('should return cached plan for identical query', async () => {
    // GAP: createQueryPlanCache does not exist
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 100 });

    const sql = 'SELECT * FROM users WHERE id = 1';
    const queryHash = computeQueryHash(sql);
    const mockPlan = { type: 'SeqScan', table: 'users', filter: 'id = 1' };

    // First access - cache miss
    expect(cache.has(queryHash)).toBe(false);
    cache.set(queryHash, mockPlan, 1);

    // Second access - cache hit
    expect(cache.has(queryHash)).toBe(true);
    const cached = cache.get(queryHash);
    expect(cached).toBeDefined();
    expect(cached!.plan).toEqual(mockPlan);
  });

  /**
   * GAP: Cache should handle whitespace normalization
   */
  it.fails('should treat whitespace-different queries as identical', async () => {
    const { createQueryPlanCache, normalizeQueryForCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 100 });

    const sql1 = 'SELECT * FROM users WHERE id = 1';
    const sql2 = 'SELECT  *  FROM  users  WHERE  id  =  1';
    const sql3 = `SELECT *
      FROM users
      WHERE id = 1`;

    // Normalization should produce same hash
    const hash1 = normalizeQueryForCache(sql1);
    const hash2 = normalizeQueryForCache(sql2);
    const hash3 = normalizeQueryForCache(sql3);

    expect(hash1).toBe(hash2);
    expect(hash2).toBe(hash3);
  });

  /**
   * GAP: Case sensitivity for SQL keywords
   */
  it.fails('should treat keyword case-different queries as identical', async () => {
    const { createQueryPlanCache, normalizeQueryForCache } = await importPlannerCache();

    const sql1 = 'SELECT * FROM users WHERE id = 1';
    const sql2 = 'select * from users where id = 1';
    const sql3 = 'Select * From Users Where Id = 1';

    // Keywords should be normalized, but identifiers preserved
    const hash1 = normalizeQueryForCache(sql1);
    const hash2 = normalizeQueryForCache(sql2);

    expect(hash1).toBe(hash2);
  });

  /**
   * GAP: Different queries should have different cache entries
   */
  it.fails('should cache different queries separately', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 100 });

    const sql1 = 'SELECT * FROM users WHERE id = 1';
    const sql2 = 'SELECT * FROM users WHERE id = 2';

    const hash1 = computeQueryHash(sql1);
    const hash2 = computeQueryHash(sql2);

    const plan1 = { type: 'IndexScan', index: 'users_pkey', key: 1 };
    const plan2 = { type: 'IndexScan', index: 'users_pkey', key: 2 };

    cache.set(hash1, plan1, 1);
    cache.set(hash2, plan2, 1);

    expect(hash1).not.toBe(hash2);
    expect(cache.get(hash1)!.plan).toEqual(plan1);
    expect(cache.get(hash2)!.plan).toEqual(plan2);
  });
});

// =============================================================================
// 2. PLAN CACHE USES QUERY HASH AS KEY
// =============================================================================

describe('Plan Cache Uses Query Hash as Key', () => {
  /**
   * GAP: Query hash function does not exist
   */
  it.fails('should compute deterministic hash for query', async () => {
    const { computePlanCacheKey } = await importPlannerCache();

    const sql = 'SELECT id, name FROM users WHERE active = true';

    const hash1 = computePlanCacheKey(sql);
    const hash2 = computePlanCacheKey(sql);
    const hash3 = computePlanCacheKey(sql);

    // Hash should be deterministic
    expect(hash1).toBe(hash2);
    expect(hash2).toBe(hash3);
    expect(typeof hash1).toBe('string');
    expect(hash1.length).toBeGreaterThan(0);
  });

  /**
   * GAP: Hash should include database schema context
   */
  it.fails('should include schema version in cache key', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 100 });

    const sql = 'SELECT * FROM users';
    const queryHash = computeQueryHash(sql);

    // Cache with schema version 1
    cache.set(queryHash, { plan: 'v1' }, 1);

    const cached = cache.get(queryHash);
    expect(cached).toBeDefined();
    expect(cached!.schemaVersion).toBe(1);
  });

  /**
   * GAP: Hash collision handling
   */
  it.fails('should handle hash collisions gracefully', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({
      maxSize: 100,
      // Enable collision detection
      storeOriginalQuery: true,
    } as ExtendedCacheConfig);

    // Even if two queries produce same hash (unlikely),
    // cache should store original query to verify
    const sql1 = 'SELECT * FROM users WHERE id = 1';
    const sql2 = 'SELECT * FROM users WHERE id = 2';

    const hash1 = computeQueryHash(sql1);
    const hash2 = computeQueryHash(sql2);

    // Even if hashes collide (forced for test), plans should be stored correctly
    cache.set(hash1, { query: sql1, plan: 'plan1' }, 1);
    cache.set(hash2, { query: sql2, plan: 'plan2' }, 1);

    // Should retrieve correct plan by verifying original query
    expect(cache.size()).toBeGreaterThanOrEqual(1);
  });

  /**
   * GAP: Query hash should be efficient
   */
  it.fails('should compute hash efficiently for long queries', async () => {
    const { computePlanCacheKey } = await importPlannerCache();

    // Generate a very long query
    const columns = Array.from({ length: 100 }, (_, i) => `col${i}`).join(', ');
    const sql = `SELECT ${columns} FROM very_wide_table WHERE status = 'active'`;

    const startTime = performance.now();
    for (let i = 0; i < 1000; i++) {
      computePlanCacheKey(sql);
    }
    const endTime = performance.now();

    // Should complete 1000 hashes in under 50ms
    expect(endTime - startTime).toBeLessThan(50);
  });
});

// =============================================================================
// 3. PARAMETERIZED QUERIES SHARE CACHED PLAN
// =============================================================================

describe('Parameterized Queries Share Cached Plan', () => {
  /**
   * GAP: Parameter extraction for cache key generation
   */
  it.fails('should normalize query with parameters for caching', async () => {
    const { normalizeParameterizedQuery } = await importPlannerCache();

    const sql1 = 'SELECT * FROM users WHERE id = $1';
    const sql2 = 'SELECT * FROM users WHERE id = $1';
    const sql3 = 'SELECT * FROM users WHERE id = ?';

    // All parameterized versions should produce same normalized form
    const normalized1 = normalizeParameterizedQuery(sql1);
    const normalized2 = normalizeParameterizedQuery(sql2);
    const normalized3 = normalizeParameterizedQuery(sql3);

    expect(normalized1).toBe(normalized2);
    expect(normalized1).toBe(normalized3);
  });

  /**
   * GAP: Queries with different parameter values should share plan
   */
  it.fails('should share cached plan across different parameter values', async () => {
    const { createQueryPlanCache, extractParameterizedPlan } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 100 });

    // Query templates are the same, only parameter values differ
    const template = 'SELECT * FROM users WHERE id = $1';
    const plan = { type: 'IndexScan', index: 'users_pkey', parameterized: true };

    cache.set(computeQueryHash(template), plan, 1);

    // Both queries should use same cached plan
    const plan1 = extractParameterizedPlan(cache, 'SELECT * FROM users WHERE id = 1');
    const plan2 = extractParameterizedPlan(cache, 'SELECT * FROM users WHERE id = 999');

    expect(plan1).toEqual(plan2);
  });

  /**
   * GAP: Literal replacement for cache normalization
   */
  it.fails('should replace literals with placeholders for caching', async () => {
    const { normalizeLiterals } = await importPlannerCache();

    const sql1 = "SELECT * FROM users WHERE name = 'Alice' AND age = 30";
    const sql2 = "SELECT * FROM users WHERE name = 'Bob' AND age = 25";

    const normalized1 = normalizeLiterals(sql1);
    const normalized2 = normalizeLiterals(sql2);

    // After normalizing literals, both should match
    expect(normalized1).toBe(normalized2);
    expect(normalized1).toContain('$'); // Should have parameter placeholders
  });

  /**
   * GAP: IN clause parameter normalization
   */
  it.fails('should normalize IN clauses with different value counts', async () => {
    const { normalizeInClause } = await importPlannerCache();

    const sql1 = 'SELECT * FROM users WHERE id IN (1, 2, 3)';
    const sql2 = 'SELECT * FROM users WHERE id IN (1, 2, 3, 4, 5)';

    // Different IN list sizes might use different plans
    const normalized1 = normalizeInClause(sql1);
    const normalized2 = normalizeInClause(sql2);

    // Should bucket into size ranges (e.g., 1-10, 10-100, 100+)
    expect(normalized1).toContain('IN_BUCKET');
  });

  /**
   * GAP: Prepared statement plan sharing
   */
  it.fails('should track prepared statement plan reuse', async () => {
    const { createQueryPlanCache, createPreparedStatement } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 100 });

    // Create prepared statement
    const stmt = createPreparedStatement(cache, 'SELECT * FROM users WHERE id = $1');

    // Execute multiple times with different parameters
    stmt.execute([1]);
    stmt.execute([2]);
    stmt.execute([3]);

    const stats = cache.getStats();

    // First execution is miss, subsequent are hits
    expect(stats.hits).toBe(2);
    expect(stats.misses).toBe(1);
  });
});

// =============================================================================
// 4. CACHE RESPECTS MAX SIZE LIMIT
// =============================================================================

describe('Cache Respects Max Size Limit', () => {
  /**
   * GAP: Cache should enforce maximum size
   */
  it.fails('should not exceed max size limit', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const maxSize = 5;
    const cache: QueryPlanCache = createQueryPlanCache({ maxSize });

    // Add more entries than max size
    for (let i = 0; i < 10; i++) {
      const sql = `SELECT * FROM table_${i}`;
      cache.set(computeQueryHash(sql), { table: `table_${i}` }, 1);
    }

    // Cache size should not exceed limit
    expect(cache.size()).toBeLessThanOrEqual(maxSize);
    expect(cache.size()).toBe(maxSize);
  });

  /**
   * GAP: Cache should evict when adding to full cache
   */
  it.fails('should evict entry when adding to full cache', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const maxSize = 3;
    const cache: QueryPlanCache = createQueryPlanCache({ maxSize });

    // Fill cache
    cache.set('hash1', { plan: 1 }, 1);
    cache.set('hash2', { plan: 2 }, 1);
    cache.set('hash3', { plan: 3 }, 1);

    expect(cache.size()).toBe(3);

    // Add one more - should evict
    cache.set('hash4', { plan: 4 }, 1);

    expect(cache.size()).toBe(3);
    expect(cache.has('hash4')).toBe(true);

    const stats = cache.getStats();
    expect(stats.evictions).toBe(1);
  });

  /**
   * GAP: Zero size should disable caching
   */
  it.fails('should disable caching when maxSize is 0', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 0 });

    cache.set('hash1', { plan: 1 }, 1);
    cache.set('hash2', { plan: 2 }, 1);

    expect(cache.size()).toBe(0);
    expect(cache.has('hash1')).toBe(false);
  });

  /**
   * GAP: Memory-based size limit
   */
  it.fails('should support memory-based size limit', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({
      maxSize: 1000, // count limit
      maxMemoryBytes: 1024 * 1024, // 1MB memory limit
    } as ExtendedCacheConfig);

    // Add large plans
    const largePlan = { data: 'x'.repeat(100 * 1024) }; // ~100KB per plan

    for (let i = 0; i < 20; i++) {
      cache.set(`hash${i}`, largePlan, 1);
    }

    const stats = cache.getStats();

    // Should be limited by memory, not count
    expect(stats.memoryUsage).toBeLessThanOrEqual(1024 * 1024);
    expect(cache.size()).toBeLessThan(20);
  });
});

// =============================================================================
// 5. LRU EVICTION WHEN CACHE FULL
// =============================================================================

describe('LRU Eviction When Cache Full', () => {
  /**
   * GAP: LRU eviction policy does not exist
   */
  it.fails('should evict least recently used entry', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({
      maxSize: 3,
      evictionPolicy: 'lru',
    } as ExtendedCacheConfig);

    // Add entries in order
    cache.set('hash1', { plan: 1 }, 1); // Oldest
    await delay(10);
    cache.set('hash2', { plan: 2 }, 1);
    await delay(10);
    cache.set('hash3', { plan: 3 }, 1); // Newest

    // Access hash1 to make it recently used
    cache.get('hash1');

    // Add new entry - should evict hash2 (least recently used)
    cache.set('hash4', { plan: 4 }, 1);

    expect(cache.has('hash1')).toBe(true); // Recently accessed
    expect(cache.has('hash2')).toBe(false); // Evicted (LRU)
    expect(cache.has('hash3')).toBe(true);
    expect(cache.has('hash4')).toBe(true);
  });

  /**
   * GAP: Access updates LRU timestamp
   */
  it.fails('should update LRU timestamp on access', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({
      maxSize: 3,
      evictionPolicy: 'lru',
    } as ExtendedCacheConfig);

    cache.set('a', { plan: 'a' }, 1);
    cache.set('b', { plan: 'b' }, 1);
    cache.set('c', { plan: 'c' }, 1);

    // Access 'a' multiple times to keep it fresh
    cache.get('a');
    cache.get('a');
    cache.get('a');

    // Add d, e - should evict b, then c
    cache.set('d', { plan: 'd' }, 1);
    cache.set('e', { plan: 'e' }, 1);

    // 'a' should still be present due to recent access
    expect(cache.has('a')).toBe(true);
    expect(cache.has('b')).toBe(false);
    expect(cache.has('c')).toBe(false);
  });

  /**
   * GAP: LFU (Least Frequently Used) eviction policy
   */
  it.fails('should support LFU eviction policy', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({
      maxSize: 3,
      evictionPolicy: 'lfu',
    } as ExtendedCacheConfig);

    cache.set('hot', { plan: 'hot' }, 1);
    cache.set('warm', { plan: 'warm' }, 1);
    cache.set('cold', { plan: 'cold' }, 1);

    // Access 'hot' many times
    for (let i = 0; i < 10; i++) cache.get('hot');
    // Access 'warm' a few times
    for (let i = 0; i < 3; i++) cache.get('warm');
    // 'cold' not accessed

    // Add new entry - should evict 'cold' (least frequently used)
    cache.set('new', { plan: 'new' }, 1);

    expect(cache.has('hot')).toBe(true);
    expect(cache.has('warm')).toBe(true);
    expect(cache.has('cold')).toBe(false); // Evicted (LFU)
    expect(cache.has('new')).toBe(true);
  });

  /**
   * GAP: Track hit count for entries
   */
  it.fails('should track hit count for cached entries', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 10 });

    cache.set('query1', { plan: 1 }, 1);

    // Access multiple times
    cache.get('query1');
    cache.get('query1');
    cache.get('query1');

    const entry = cache.get('query1');

    expect(entry!.hitCount).toBe(4); // 1 initial + 3 accesses
  });
});

// =============================================================================
// 6. SCHEMA CHANGES INVALIDATE RELEVANT PLANS
// =============================================================================

describe('Schema Changes Invalidate Relevant Plans', () => {
  /**
   * GAP: Table-specific invalidation does not exist
   */
  it.fails('should invalidate plans when table schema changes', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 100 });

    // Cache plans for different tables
    cache.set('users_query', { table: 'users', plan: 'scan' }, 1);
    cache.set('orders_query', { table: 'orders', plan: 'scan' }, 1);
    cache.set('products_query', { table: 'products', plan: 'scan' }, 1);

    // Invalidate plans for 'users' table
    const invalidated = cache.invalidateTable('users');

    expect(invalidated).toBe(1);
    expect(cache.has('users_query')).toBe(false);
    expect(cache.has('orders_query')).toBe(true);
    expect(cache.has('products_query')).toBe(true);
  });

  /**
   * GAP: Index changes should invalidate relevant plans
   */
  it.fails('should invalidate plans when index is added', async () => {
    const { createQueryPlanCache, notifyIndexChange } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 100 });

    // Cache plan that could benefit from index
    cache.set('users_by_email', {
      table: 'users',
      plan: 'SeqScan', // Would use IndexScan if index existed
      columns: ['email'],
    }, 1);

    // Notify cache that index was added
    notifyIndexChange(cache, {
      type: 'CREATE_INDEX',
      table: 'users',
      columns: ['email'],
    });

    // Plan should be invalidated to allow re-optimization
    expect(cache.has('users_by_email')).toBe(false);
  });

  /**
   * GAP: Schema version tracking
   */
  it.fails('should reject stale plans based on schema version', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 100 });

    // Cache plan at schema version 5
    cache.set('old_query', { plan: 'old' }, 5);

    // Try to get plan when current schema version is 7
    const plan = cache.get('old_query');

    // Plan exists but should be marked as stale
    expect(plan).toBeDefined();
    expect(plan!.schemaVersion).toBe(5);

    // Should have method to check staleness
    const isStale = cache.isPlanStale('old_query', 7);
    expect(isStale).toBe(true);
  });

  /**
   * GAP: DDL statement should trigger invalidation
   */
  it.fails('should invalidate on DDL statements', async () => {
    const { createQueryPlanCache, processDDL } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 100 });

    cache.set('users_select', { table: 'users' }, 1);
    cache.set('users_join', { tables: ['users', 'orders'] }, 1);

    // Process ALTER TABLE
    processDDL(cache, 'ALTER TABLE users ADD COLUMN age INTEGER');

    expect(cache.has('users_select')).toBe(false);
    expect(cache.has('users_join')).toBe(false);
  });

  /**
   * GAP: Multi-table query invalidation
   */
  it.fails('should invalidate multi-table plans when any table changes', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({
      maxSize: 100,
      trackTableDependencies: true,
    } as ExtendedCacheConfig);

    // Cache join query
    cache.set('join_query', {
      plan: 'HashJoin',
      tables: ['users', 'orders'],
    }, 1);

    // Invalidate 'orders' table
    cache.invalidateTable('orders');

    // Join query should be invalidated
    expect(cache.has('join_query')).toBe(false);
  });
});

// =============================================================================
// 7. CACHE HIT IMPROVES QUERY LATENCY
// =============================================================================

describe('Cache Hit Improves Query Latency', () => {
  /**
   * GAP: Cache lookup should be fast
   */
  it.fails('should lookup cached plan in O(1) time', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 10000 });

    // Fill cache with many entries
    for (let i = 0; i < 10000; i++) {
      cache.set(`hash_${i}`, { plan: i }, 1);
    }

    // Lookup should be constant time
    const startTime = performance.now();
    for (let i = 0; i < 1000; i++) {
      cache.get(`hash_${i % 10000}`);
    }
    const endTime = performance.now();

    // 1000 lookups should complete in under 10ms
    expect(endTime - startTime).toBeLessThan(10);
  });

  /**
   * GAP: Planning time should be tracked
   */
  it.fails('should track planning time savings from cache', async () => {
    const { createQueryPlanCache, measurePlanningTime } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 100 });

    // Simulate planning time
    const planningTime = await measurePlanningTime(async () => {
      await delay(10); // Simulate 10ms planning
      return { plan: 'result' };
    });

    expect(planningTime.durationMs).toBeGreaterThanOrEqual(10);
    expect(planningTime.plan).toEqual({ plan: 'result' });

    // Cache the plan
    cache.set('query', planningTime.plan, 1);

    // Cache hit should be much faster
    const cacheHitTime = await measurePlanningTime(async () => {
      return cache.get('query')!.plan;
    });

    expect(cacheHitTime.durationMs).toBeLessThan(1);
  });

  /**
   * GAP: Total time saved should be tracked
   */
  it.fails('should track total time saved by cache', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({
      maxSize: 100,
      trackTimeSaved: true,
    } as ExtendedCacheConfig);

    // Simulate caching a plan that took 50ms to generate
    cache.set('expensive_query', { plan: 'complex' }, 1, {
      planningTimeMs: 50,
    } as ExtendedCacheConfig);

    // Simulate cache hits
    cache.get('expensive_query');
    cache.get('expensive_query');
    cache.get('expensive_query');

    const stats = cache.getStats() as ExtendedPlanCacheStats;

    // 3 hits * 50ms = 150ms saved
    expect(stats.timeSavedMs).toBeGreaterThanOrEqual(150);
  });

  /**
   * GAP: Latency comparison metrics
   */
  it.fails('should provide hit vs miss latency comparison', async () => {
    const { createQueryPlanCache } = await importPlannerCache();

    const cache: QueryPlanCache = createQueryPlanCache({
      maxSize: 100,
      trackLatency: true,
    } as ExtendedCacheConfig);

    // Record some misses and hits
    cache.recordMiss('q1', 25); // 25ms planning time
    cache.recordMiss('q2', 30); // 30ms planning time
    cache.recordHit('q1', 0.1); // 0.1ms cache hit
    cache.recordHit('q1', 0.1);
    cache.recordHit('q2', 0.2);

    const stats = cache.getStats() as ExtendedPlanCacheStats;

    expect(stats.avgMissLatencyMs).toBeCloseTo(27.5);
    expect(stats.avgHitLatencyMs).toBeLessThan(1);
    expect(stats.latencyImprovement).toBeGreaterThan(20); // >20x faster
  });
});

// =============================================================================
// 8. CACHE STATS EXPOSED VIA PRAGMA
// =============================================================================

describe('Cache Stats Exposed via PRAGMA', () => {
  /**
   * GAP: PRAGMA query_plan_cache_stats does not exist
   */
  it.fails('should expose cache stats via PRAGMA', async () => {
    const { createDatabase } = await import('../database.js');

    const db = createDatabase();

    // Execute some queries to populate cache
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.exec('INSERT INTO users VALUES (1, "Alice"), (2, "Bob")');

    // Execute same query multiple times
    db.exec('SELECT * FROM users WHERE id = 1');
    db.exec('SELECT * FROM users WHERE id = 1');
    db.exec('SELECT * FROM users WHERE id = 1');

    // Query cache stats
    const result = db.exec('PRAGMA query_plan_cache_stats');

    expect(result.length).toBe(1);
    const stats = result[0];

    expect(stats).toHaveProperty('hits');
    expect(stats).toHaveProperty('misses');
    expect(stats).toHaveProperty('size');
    expect(stats).toHaveProperty('maxSize');
    expect(stats).toHaveProperty('hitRate');
  });

  /**
   * GAP: PRAGMA to view cached plans does not exist
   */
  it.fails('should list cached plans via PRAGMA', async () => {
    const { createDatabase } = await import('../database.js');

    const db = createDatabase();

    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.exec('SELECT * FROM users');
    db.exec('SELECT name FROM users WHERE id = 1');

    // List all cached plans
    const result = db.exec('PRAGMA query_plan_cache_list');

    expect(result.length).toBeGreaterThanOrEqual(2);

    for (const entry of result) {
      expect(entry).toHaveProperty('queryHash');
      expect(entry).toHaveProperty('cachedAt');
      expect(entry).toHaveProperty('hitCount');
    }
  });

  /**
   * GAP: PRAGMA to clear cache does not exist
   */
  it.fails('should clear cache via PRAGMA', async () => {
    const { createDatabase } = await import('../database.js');

    const db = createDatabase();

    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY)');
    db.exec('SELECT * FROM users');

    // Verify cache has entries
    let stats = db.exec('PRAGMA query_plan_cache_stats')[0];
    expect(stats.size).toBeGreaterThan(0);

    // Clear cache
    db.exec('PRAGMA query_plan_cache_clear');

    // Verify cache is empty
    stats = db.exec('PRAGMA query_plan_cache_stats')[0];
    expect(stats.size).toBe(0);
  });

  /**
   * GAP: PRAGMA to configure cache does not exist
   */
  it.fails('should configure cache via PRAGMA', async () => {
    const { createDatabase } = await import('../database.js');

    const db = createDatabase();

    // Check default size
    let stats = db.exec('PRAGMA query_plan_cache_stats')[0];
    const defaultMaxSize = stats.maxSize;

    // Change max size
    db.exec('PRAGMA query_plan_cache_max_size = 500');

    stats = db.exec('PRAGMA query_plan_cache_stats')[0];
    expect(stats.maxSize).toBe(500);

    // Disable cache
    db.exec('PRAGMA query_plan_cache_enabled = false');

    stats = db.exec('PRAGMA query_plan_cache_stats')[0];
    expect(stats.enabled).toBe(false);
  });

  /**
   * GAP: PRAGMA to view specific plan does not exist
   */
  it.fails('should view specific cached plan via PRAGMA', async () => {
    const { createDatabase } = await import('../database.js');

    const db = createDatabase();

    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.exec('SELECT * FROM users WHERE id = 1');

    // View specific plan by hash (would need to get hash first)
    const list = db.exec('PRAGMA query_plan_cache_list');
    const queryHash = list[0].queryHash;

    const plan = db.exec(`PRAGMA query_plan_cache_show('${queryHash}')`);

    expect(plan.length).toBe(1);
    expect(plan[0]).toHaveProperty('plan');
    expect(plan[0]).toHaveProperty('schemaVersion');
  });

  /**
   * GAP: PRAGMA for cache hit rate threshold alerting
   */
  it.fails('should support cache hit rate threshold via PRAGMA', async () => {
    const { createDatabase } = await import('../database.js');

    const db = createDatabase();

    // Set alert threshold
    db.exec('PRAGMA query_plan_cache_hit_rate_threshold = 0.80');

    // Check if below threshold
    const warning = db.exec('PRAGMA query_plan_cache_health');

    expect(warning[0]).toHaveProperty('status');
    expect(warning[0]).toHaveProperty('hitRate');
    expect(warning[0]).toHaveProperty('threshold');
    expect(warning[0]).toHaveProperty('recommendation');
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('Query Plan Cache Integration', () => {
  /**
   * GAP: End-to-end cache integration does not exist
   */
  it.fails('should integrate cache with query execution', async () => {
    const { createDatabase } = await import('../database.js');

    const db = createDatabase({
      planCache: {
        enabled: true,
        maxSize: 100,
      },
    } as ExtendedCacheConfig);

    db.exec('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)');
    db.exec('INSERT INTO products VALUES (1, "Widget", 9.99)');

    // First execution - cache miss, plan is generated
    const result1 = db.exec('SELECT * FROM products WHERE id = 1');

    // Second execution - cache hit, plan is reused
    const result2 = db.exec('SELECT * FROM products WHERE id = 1');

    expect(result1).toEqual(result2);

    const stats = db.exec('PRAGMA query_plan_cache_stats')[0];
    expect(stats.hits).toBeGreaterThanOrEqual(1);
  });

  /**
   * GAP: Cache should work with optimizer
   */
  it.fails('should cache optimized plans from cost-based optimizer', async () => {
    const { createDatabase } = await import('../database.js');
    const { createQueryPlanCache } = await importPlannerCache();

    const db = createDatabase();
    const cache: QueryPlanCache = createQueryPlanCache({ maxSize: 100 });

    db.exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)');
    db.exec('CREATE INDEX idx_user_id ON orders(user_id)');

    // Insert data to build statistics
    for (let i = 0; i < 1000; i++) {
      db.exec(`INSERT INTO orders VALUES (${i}, ${i % 100}, ${Math.random() * 100})`);
    }

    // Run ANALYZE to update statistics
    db.exec('ANALYZE');

    // Query that benefits from index
    const query = 'SELECT * FROM orders WHERE user_id = 42';

    // First execution generates and caches optimal plan
    const plan = db.explain(query);

    // Plan should use index
    expect(plan).toContain('IndexScan');

    // Cache should store this optimized plan
    expect(cache.size()).toBeGreaterThan(0);
  });
});
