/**
 * DoSQL Query Plan Cache
 *
 * Hash-based LRU cache for optimized query plans.
 * Caches parsed/optimized plans for reuse across identical queries.
 *
 * Features:
 * - Hash-based query lookup with normalization
 * - Parameterized query sharing (literals normalized)
 * - LRU eviction policy
 * - Schema change invalidation (table-level granularity)
 * - Cache statistics tracking
 *
 * ## Schema Invalidation Strategy
 *
 * The cache implements partial invalidation on schema changes to ensure
 * cached plans remain valid:
 *
 * 1. **Table Dependency Tracking**: Each cached plan extracts and stores the
 *    tables it references. Tables are normalized to lowercase for
 *    case-insensitive matching.
 *
 * 2. **DDL Operations**: When DDL operations occur, only plans referencing
 *    the affected table(s) are invalidated:
 *    - ALTER TABLE: Invalidates all plans for the altered table
 *    - CREATE INDEX: Invalidates plans that may benefit from the new index
 *    - DROP TABLE: Invalidates all plans referencing the dropped table
 *    - DROP INDEX: Invalidates plans that may have used the index
 *
 * 3. **Schema Version**: Each plan stores the schema version at cache time.
 *    Use `isPlanStale()` to check if a plan was cached before recent changes.
 *
 * 4. **Multi-table Queries**: Join queries track all referenced tables and
 *    are invalidated when any referenced table changes.
 *
 * @example
 * ```typescript
 * const cache = createQueryPlanCache({ maxSize: 100 });
 *
 * // Cache a plan
 * cache.set(queryHash, plan, schemaVersion);
 *
 * // After DDL, invalidate affected plans
 * processDDL(cache, 'ALTER TABLE users ADD COLUMN email TEXT');
 *
 * // Or invalidate directly
 * cache.invalidateTable('users');
 * ```
 *
 * @packageDocumentation
 */

import type { PhysicalPlan, OptimizationResult } from './types.js';

// =============================================================================
// CONFIGURATION TYPES
// =============================================================================

/**
 * Query plan cache configuration
 */
export interface QueryPlanCacheConfig {
  /** Maximum number of plans to cache (default: 1000) */
  maxSize: number;

  /** Maximum memory in bytes for cached plans (default: 10MB) */
  maxMemoryBytes?: number;

  /** Time-to-live in milliseconds (0 = no TTL, default: 0) */
  ttlMs?: number;

  /** Enable/disable caching (default: true) */
  enabled?: boolean;

  /** Eviction policy: 'lru' or 'lfu' (default: 'lru') */
  evictionPolicy?: 'lru' | 'lfu';

  /** Track table dependencies for invalidation (default: true) */
  trackTableDependencies?: boolean;

  /** Store original query for collision detection (default: false) */
  storeOriginalQuery?: boolean;

  /** Track latency metrics (default: false) */
  trackLatency?: boolean;

  /** Track time saved by cache hits (default: false) */
  trackTimeSaved?: boolean;
}

/**
 * Default configuration
 */
export const DEFAULT_PLAN_CACHE_CONFIG: Required<QueryPlanCacheConfig> = {
  maxSize: 1000,
  maxMemoryBytes: 10 * 1024 * 1024, // 10MB
  ttlMs: 0,
  enabled: true,
  evictionPolicy: 'lru',
  trackTableDependencies: true,
  storeOriginalQuery: false,
  trackLatency: false,
  trackTimeSaved: false,
};

// =============================================================================
// CACHED PLAN ENTRY
// =============================================================================

/**
 * Cached query plan entry
 */
export interface CachedPlan {
  /** Hash of the query */
  queryHash: string;

  /** The optimized query plan */
  plan: unknown;

  /** Timestamp when cached (ms since epoch) */
  cachedAt: number;

  /** Last access timestamp (ms since epoch) */
  lastAccess: number;

  /** Number of times this plan was hit */
  hitCount: number;

  /** Schema version when plan was cached */
  schemaVersion: number;

  /** Estimated size in bytes */
  sizeBytes: number;

  /** Tables referenced by this plan (for invalidation) */
  tables: Set<string>;

  /** Original query (if storeOriginalQuery enabled) */
  originalQuery?: string;

  /** Planning time in ms (if trackTimeSaved enabled) */
  planningTimeMs?: number;
}

// =============================================================================
// CACHE STATISTICS
// =============================================================================

/**
 * Query plan cache statistics
 */
export interface PlanCacheStats {
  /** Total cache hits */
  hits: number;

  /** Total cache misses */
  misses: number;

  /** Current number of cached plans */
  size: number;

  /** Maximum cache size */
  maxSize: number;

  /** Hit rate percentage (0-100) */
  hitRate: number;

  /** Total evictions */
  evictions: number;

  /** Memory usage estimate in bytes */
  memoryUsage: number;

  /** Whether cache is enabled */
  enabled: boolean;

  /** Average miss latency in ms (if trackLatency) */
  avgMissLatencyMs?: number;

  /** Average hit latency in ms (if trackLatency) */
  avgHitLatencyMs?: number;

  /** Latency improvement ratio (if trackLatency) */
  latencyImprovement?: number;

  /** Total time saved in ms (if trackTimeSaved) */
  timeSavedMs?: number;
}

// =============================================================================
// LRU NODE
// =============================================================================

/**
 * Doubly-linked list node for LRU ordering
 */
interface LRUNode {
  key: string;
  entry: CachedPlan;
  prev: LRUNode | null;
  next: LRUNode | null;
}

// =============================================================================
// HASH FUNCTIONS
// =============================================================================

/**
 * Compute a hash for a SQL query (djb2 algorithm)
 *
 * @param str - String to hash
 * @returns Hex hash string
 */
export function computePlanCacheKey(str: string): string {
  let hash = 5381;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) + hash) ^ str.charCodeAt(i);
  }
  return `qh_${(hash >>> 0).toString(16)}`;
}

/**
 * Normalize SQL query for cache key generation.
 * Normalizes whitespace and SQL keyword case.
 *
 * @param sql - SQL query string
 * @returns Normalized SQL string
 */
export function normalizeQueryForCache(sql: string): string {
  // Normalize whitespace to single spaces
  let normalized = sql.replace(/\s+/g, ' ').trim();

  // Normalize SQL keywords to lowercase
  const keywords = [
    'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'LIKE',
    'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS', 'ON',
    'ORDER', 'BY', 'ASC', 'DESC', 'LIMIT', 'OFFSET',
    'GROUP', 'HAVING', 'DISTINCT', 'AS', 'NULL', 'IS',
    'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
    'CREATE', 'TABLE', 'INDEX', 'DROP', 'ALTER', 'ADD',
    'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'UNIQUE',
    'EXISTS', 'BETWEEN', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
    'TRUE', 'FALSE', 'UNION', 'ALL', 'EXCEPT', 'INTERSECT',
  ];

  // Use word boundaries to replace keywords
  for (const keyword of keywords) {
    const regex = new RegExp(`\\b${keyword}\\b`, 'gi');
    normalized = normalized.replace(regex, keyword.toLowerCase());
  }

  return normalized;
}

/**
 * Normalize SQL query with literals replaced by placeholders.
 * This allows queries with different literal values to share the same cached plan.
 *
 * @param sql - SQL query string
 * @returns Object with normalized SQL and extracted literals
 */
export function normalizeLiterals(sql: string): string {
  let normalized = sql;
  let paramIndex = 1;

  // Replace string literals (single quotes)
  normalized = normalized.replace(/'(?:[^'\\]|\\.)*'/g, () => `$${paramIndex++}`);

  // Replace numeric literals (integers and decimals)
  normalized = normalized.replace(/\b\d+(?:\.\d+)?\b/g, (match, offset) => {
    // Don't replace if it's part of an identifier or already a parameter
    const before = normalized.substring(Math.max(0, offset - 1), offset);
    if (before === '$' || before === '_') {
      return match;
    }
    return `$${paramIndex++}`;
  });

  return normalized;
}

/**
 * Normalize a parameterized query for cache lookup
 *
 * @param sql - SQL with positional parameters ($1, $2, ?) or named parameters
 * @returns Normalized parameterized query
 */
export function normalizeParameterizedQuery(sql: string): string {
  // Replace ? with $? first (before $N replacement to avoid double replacement)
  // Use negative lookbehind to avoid replacing ? that are part of $?
  let normalized = sql.replace(/(?<!\$)\?/g, '$?');

  // Replace $N with $? placeholder
  normalized = normalized.replace(/\$\d+/g, '$?');

  // Normalize whitespace and keywords
  return normalizeQueryForCache(normalized);
}

/**
 * Normalize IN clauses by bucketing the number of values
 *
 * @param sql - SQL query string
 * @returns SQL with IN clauses normalized to bucket sizes
 */
export function normalizeInClause(sql: string): string {
  // Match IN clauses with literal values
  const inPattern = /\bIN\s*\(\s*([^)]+)\s*\)/gi;

  return sql.replace(inPattern, (match, values) => {
    const valueList = values.split(',');
    const count = valueList.length;

    // Bucket sizes: 1-10, 11-100, 101-1000, 1000+
    let bucket: string;
    if (count <= 10) {
      bucket = 'IN_BUCKET_1_10';
    } else if (count <= 100) {
      bucket = 'IN_BUCKET_11_100';
    } else if (count <= 1000) {
      bucket = 'IN_BUCKET_101_1000';
    } else {
      bucket = 'IN_BUCKET_1000_PLUS';
    }

    return `IN (${bucket})`;
  });
}

/**
 * Estimate memory size of a cached plan
 */
function estimatePlanSize(plan: unknown, query?: string): number {
  // Base overhead
  let size = 256;

  // Query string if stored
  if (query) {
    size += query.length * 2;
  }

  // Plan object (rough estimate based on JSON serialization)
  try {
    const json = JSON.stringify(plan);
    size += json.length * 2;
  } catch {
    // If not serializable, use a default estimate
    size += 1024;
  }

  return size;
}

/**
 * Extract table names from a query plan
 * Table names are normalized to lowercase for case-insensitive matching
 */
function extractTablesFromPlan(plan: unknown): Set<string> {
  const tables = new Set<string>();

  function traverse(node: unknown): void {
    if (!node || typeof node !== 'object') return;

    const obj = node as Record<string, unknown>;

    // Check for table property
    if (typeof obj.table === 'string') {
      tables.add(obj.table.toLowerCase());
    }

    // Check for tables array
    if (Array.isArray(obj.tables)) {
      for (const t of obj.tables) {
        if (typeof t === 'string') {
          tables.add(t.toLowerCase());
        }
      }
    }

    // Recurse into children
    if (Array.isArray(obj.children)) {
      for (const child of obj.children) {
        traverse(child);
      }
    }

    // Recurse into other properties
    for (const value of Object.values(obj)) {
      if (value && typeof value === 'object') {
        traverse(value);
      }
    }
  }

  traverse(plan);
  return tables;
}

// =============================================================================
// QUERY PLAN CACHE
// =============================================================================

/**
 * Query plan cache interface
 */
export interface QueryPlanCache {
  /** Get a cached plan by query hash */
  get(queryHash: string): CachedPlan | undefined;

  /** Store a plan in the cache */
  set(
    queryHash: string,
    plan: unknown,
    schemaVersion: number,
    options?: { planningTimeMs?: number }
  ): void;

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

  /** Check if a plan is stale based on schema version */
  isPlanStale(queryHash: string, currentSchemaVersion: number): boolean;

  /** Record a cache miss with latency */
  recordMiss(queryHash: string, latencyMs: number): void;

  /** Record a cache hit with latency */
  recordHit(queryHash: string, latencyMs: number): void;
}

/**
 * Implementation of QueryPlanCache with LRU/LFU eviction
 */
export class QueryPlanCacheImpl implements QueryPlanCache {
  private readonly config: Required<QueryPlanCacheConfig>;
  private cache: Map<string, LRUNode>;
  private head: LRUNode | null = null;
  private tail: LRUNode | null = null;
  private currentMemory = 0;

  // Statistics
  private hits = 0;
  private misses = 0;
  private evictions = 0;
  private totalTimeSavedMs = 0;

  // Latency tracking
  private missLatencies: number[] = [];
  private hitLatencies: number[] = [];

  // Table -> query hash mapping for invalidation
  private tableToPlanMap: Map<string, Set<string>> = new Map();

  constructor(config: Partial<QueryPlanCacheConfig> = {}) {
    this.config = { ...DEFAULT_PLAN_CACHE_CONFIG, ...config };
    this.cache = new Map();
  }

  /**
   * Get a cached plan by query hash
   */
  get(queryHash: string): CachedPlan | undefined {
    if (!this.config.enabled || this.config.maxSize === 0) {
      return undefined;
    }

    const node = this.cache.get(queryHash);

    if (!node) {
      this.misses++;
      return undefined;
    }

    // Check TTL
    if (this.config.ttlMs > 0) {
      const now = Date.now();
      if (now - node.entry.cachedAt > this.config.ttlMs) {
        this.deleteEntry(queryHash);
        this.misses++;
        return undefined;
      }
    }

    // Update access stats
    const now = Date.now();
    node.entry.lastAccess = now;
    node.entry.hitCount++;
    this.hits++;

    // Track time saved
    if (this.config.trackTimeSaved && node.entry.planningTimeMs) {
      this.totalTimeSavedMs += node.entry.planningTimeMs;
    }

    // Move to head for LRU (or adjust for LFU)
    if (this.config.evictionPolicy === 'lru') {
      this.moveToHead(node);
    }
    // For LFU, the position doesn't change immediately but is considered during eviction

    return node.entry;
  }

  /**
   * Store a plan in the cache
   */
  set(
    queryHash: string,
    plan: unknown,
    schemaVersion: number,
    options?: { planningTimeMs?: number }
  ): void {
    if (!this.config.enabled || this.config.maxSize === 0) {
      return;
    }

    const now = Date.now();
    const tables = this.config.trackTableDependencies
      ? extractTablesFromPlan(plan)
      : new Set<string>();
    const sizeBytes = estimatePlanSize(plan);

    // Check if already exists
    const existing = this.cache.get(queryHash);
    if (existing) {
      // Update existing entry
      existing.entry.plan = plan;
      existing.entry.schemaVersion = schemaVersion;
      existing.entry.lastAccess = now;
      existing.entry.sizeBytes = sizeBytes;
      existing.entry.tables = tables;
      if (options?.planningTimeMs !== undefined) {
        existing.entry.planningTimeMs = options.planningTimeMs;
      }

      // Update memory tracking
      const oldSize = existing.entry.sizeBytes;
      this.currentMemory = this.currentMemory - oldSize + sizeBytes;

      // Update table mappings
      this.updateTableMappings(queryHash, tables);

      this.moveToHead(existing);
      return;
    }

    // Create new entry
    const entry: CachedPlan = {
      queryHash,
      plan,
      cachedAt: now,
      lastAccess: now,
      hitCount: 0,
      schemaVersion,
      sizeBytes,
      tables,
      planningTimeMs: options?.planningTimeMs,
    };

    const node: LRUNode = {
      key: queryHash,
      entry,
      prev: null,
      next: null,
    };

    // Add to cache
    this.cache.set(queryHash, node);
    this.addToHead(node);
    this.currentMemory += sizeBytes;

    // Update table mappings
    this.updateTableMappings(queryHash, tables);

    // Evict if necessary
    this.evictIfNeeded();
  }

  /**
   * Check if a query is cached
   */
  has(queryHash: string): boolean {
    if (!this.config.enabled || this.config.maxSize === 0) {
      return false;
    }

    const node = this.cache.get(queryHash);
    if (!node) {
      return false;
    }

    // Check TTL
    if (this.config.ttlMs > 0) {
      const now = Date.now();
      if (now - node.entry.cachedAt > this.config.ttlMs) {
        this.deleteEntry(queryHash);
        return false;
      }
    }

    return true;
  }

  /**
   * Invalidate plans for a specific table
   * Table names are matched case-insensitively
   */
  invalidateTable(tableName: string): number {
    // Normalize table name to lowercase for case-insensitive matching
    const normalizedTableName = tableName.toLowerCase();
    const hashes = this.tableToPlanMap.get(normalizedTableName);
    if (!hashes) {
      return 0;
    }

    let count = 0;
    for (const hash of hashes) {
      if (this.deleteEntry(hash)) {
        count++;
      }
    }

    // Clear the table mapping
    this.tableToPlanMap.delete(normalizedTableName);

    return count;
  }

  /**
   * Invalidate all plans
   */
  clear(): void {
    this.cache.clear();
    this.head = null;
    this.tail = null;
    this.currentMemory = 0;
    this.tableToPlanMap.clear();

    // Reset statistics
    this.hits = 0;
    this.misses = 0;
    this.evictions = 0;
    this.totalTimeSavedMs = 0;
    this.missLatencies = [];
    this.hitLatencies = [];
  }

  /**
   * Get cache statistics
   */
  getStats(): PlanCacheStats {
    const total = this.hits + this.misses;
    const stats: PlanCacheStats = {
      hits: this.hits,
      misses: this.misses,
      size: this.cache.size,
      maxSize: this.config.maxSize,
      hitRate: total > 0 ? (this.hits / total) * 100 : 0,
      evictions: this.evictions,
      memoryUsage: this.currentMemory,
      enabled: this.config.enabled,
    };

    // Add latency metrics if tracking
    if (this.config.trackLatency) {
      if (this.missLatencies.length > 0) {
        stats.avgMissLatencyMs =
          this.missLatencies.reduce((a, b) => a + b, 0) / this.missLatencies.length;
      }
      if (this.hitLatencies.length > 0) {
        stats.avgHitLatencyMs =
          this.hitLatencies.reduce((a, b) => a + b, 0) / this.hitLatencies.length;
      }
      if (stats.avgMissLatencyMs && stats.avgHitLatencyMs && stats.avgHitLatencyMs > 0) {
        stats.latencyImprovement = stats.avgMissLatencyMs / stats.avgHitLatencyMs;
      }
    }

    // Add time saved metric
    if (this.config.trackTimeSaved) {
      stats.timeSavedMs = this.totalTimeSavedMs;
    }

    return stats;
  }

  /**
   * Get current cache size
   */
  size(): number {
    return this.cache.size;
  }

  /**
   * Check if a plan is stale based on schema version
   */
  isPlanStale(queryHash: string, currentSchemaVersion: number): boolean {
    const node = this.cache.get(queryHash);
    if (!node) {
      return true;
    }
    return node.entry.schemaVersion < currentSchemaVersion;
  }

  /**
   * Record a cache miss with latency
   */
  recordMiss(queryHash: string, latencyMs: number): void {
    this.misses++;
    if (this.config.trackLatency) {
      this.missLatencies.push(latencyMs);
      // Keep only last 1000 samples
      if (this.missLatencies.length > 1000) {
        this.missLatencies.shift();
      }
    }
  }

  /**
   * Record a cache hit with latency
   */
  recordHit(queryHash: string, latencyMs: number): void {
    if (this.config.trackLatency) {
      this.hitLatencies.push(latencyMs);
      // Keep only last 1000 samples
      if (this.hitLatencies.length > 1000) {
        this.hitLatencies.shift();
      }
    }
  }

  /**
   * List all cached plans (for PRAGMA)
   */
  list(): Array<{
    queryHash: string;
    cachedAt: number;
    hitCount: number;
    schemaVersion: number;
    sizeBytes: number;
  }> {
    const entries: Array<{
      queryHash: string;
      cachedAt: number;
      hitCount: number;
      schemaVersion: number;
      sizeBytes: number;
    }> = [];

    for (const node of this.cache.values()) {
      entries.push({
        queryHash: node.entry.queryHash,
        cachedAt: node.entry.cachedAt,
        hitCount: node.entry.hitCount,
        schemaVersion: node.entry.schemaVersion,
        sizeBytes: node.entry.sizeBytes,
      });
    }

    return entries;
  }

  /**
   * Get a specific plan by hash (for PRAGMA)
   */
  show(queryHash: string): { plan: unknown; schemaVersion: number } | undefined {
    const node = this.cache.get(queryHash);
    if (!node) {
      return undefined;
    }
    return {
      plan: node.entry.plan,
      schemaVersion: node.entry.schemaVersion,
    };
  }

  // ============================================================================
  // PRIVATE METHODS
  // ============================================================================

  private deleteEntry(queryHash: string): boolean {
    const node = this.cache.get(queryHash);
    if (!node) {
      return false;
    }

    // Remove from linked list
    this.removeNode(node);

    // Update memory
    this.currentMemory -= node.entry.sizeBytes;

    // Remove from table mappings
    for (const table of node.entry.tables) {
      const hashes = this.tableToPlanMap.get(table);
      if (hashes) {
        hashes.delete(queryHash);
        if (hashes.size === 0) {
          this.tableToPlanMap.delete(table);
        }
      }
    }

    // Remove from cache
    this.cache.delete(queryHash);

    return true;
  }

  private updateTableMappings(queryHash: string, tables: Set<string>): void {
    for (const table of tables) {
      let hashes = this.tableToPlanMap.get(table);
      if (!hashes) {
        hashes = new Set();
        this.tableToPlanMap.set(table, hashes);
      }
      hashes.add(queryHash);
    }
  }

  private addToHead(node: LRUNode): void {
    node.prev = null;
    node.next = this.head;

    if (this.head) {
      this.head.prev = node;
    }

    this.head = node;

    if (!this.tail) {
      this.tail = node;
    }
  }

  private removeNode(node: LRUNode): void {
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
  }

  private moveToHead(node: LRUNode): void {
    if (node === this.head) {
      return;
    }

    this.removeNode(node);
    this.addToHead(node);
  }

  private evictIfNeeded(): void {
    if (this.config.evictionPolicy === 'lfu') {
      this.evictLFU();
    } else {
      this.evictLRU();
    }
  }

  private evictLRU(): void {
    // Evict by count
    while (this.cache.size > this.config.maxSize && this.tail) {
      this.evictTail();
    }

    // Evict by memory
    if (this.config.maxMemoryBytes) {
      while (this.currentMemory > this.config.maxMemoryBytes && this.tail) {
        this.evictTail();
      }
    }
  }

  private evictLFU(): void {
    // Evict by count
    while (this.cache.size > this.config.maxSize) {
      this.evictLFUEntry();
    }

    // Evict by memory
    if (this.config.maxMemoryBytes) {
      while (this.currentMemory > this.config.maxMemoryBytes) {
        this.evictLFUEntry();
      }
    }
  }

  private evictTail(): void {
    if (!this.tail) {
      return;
    }

    const node = this.tail;
    this.deleteEntry(node.key);
    this.evictions++;
  }

  private evictLFUEntry(): void {
    if (this.cache.size === 0) {
      return;
    }

    // Find entry with lowest hit count
    let minNode: LRUNode | null = null;
    let minHits = Infinity;

    for (const node of this.cache.values()) {
      if (node.entry.hitCount < minHits) {
        minHits = node.entry.hitCount;
        minNode = node;
      }
    }

    if (minNode) {
      this.deleteEntry(minNode.key);
      this.evictions++;
    }
  }
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a new query plan cache
 *
 * @param config - Cache configuration
 * @returns QueryPlanCache instance
 */
export function createQueryPlanCache(
  config: Partial<QueryPlanCacheConfig> = {}
): QueryPlanCacheImpl {
  return new QueryPlanCacheImpl(config);
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Extract a parameterized plan from cache based on a query with literal values
 *
 * @param cache - The query plan cache
 * @param sql - SQL query with literal values
 * @returns Cached plan if found
 */
export function extractParameterizedPlan(
  cache: QueryPlanCache,
  sql: string
): unknown | undefined {
  // Normalize the query by replacing literals with placeholders
  const normalized = normalizeLiterals(normalizeQueryForCache(sql));
  const hash = computePlanCacheKey(normalized);
  const cached = cache.get(hash);
  return cached?.plan;
}

/**
 * Create a prepared statement wrapper that uses the plan cache
 */
export function createPreparedStatement(
  cache: QueryPlanCacheImpl,
  sql: string
): {
  execute: (params: unknown[]) => void;
  getCacheStats: () => PlanCacheStats;
} {
  const normalized = normalizeParameterizedQuery(sql);
  const hash = computePlanCacheKey(normalized);

  return {
    execute: (_params: unknown[]) => {
      // Check cache
      const cached = cache.get(hash);
      if (cached) {
        // Cache hit - plan already available
        return;
      }

      // Cache miss - would normally parse/optimize here
      cache.set(hash, { type: 'PreparedPlan', sql: normalized }, 1);
    },
    getCacheStats: () => cache.getStats(),
  };
}

/**
 * Notify cache of index changes
 */
export function notifyIndexChange(
  cache: QueryPlanCacheImpl,
  change: {
    type: 'CREATE_INDEX' | 'DROP_INDEX';
    table: string;
    columns: string[];
  }
): void {
  // Invalidate all plans for the affected table
  cache.invalidateTable(change.table);
}

/**
 * Process DDL statement and invalidate affected plans
 *
 * Handles various DDL statements:
 * - ALTER TABLE table_name ...
 * - DROP TABLE [IF EXISTS] table_name [CASCADE]
 * - CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name ON table_name ...
 * - DROP INDEX index_name [ON table_name]
 */
export function processDDL(cache: QueryPlanCacheImpl, sql: string): void {
  // Extract table name from DDL
  // ALTER TABLE table_name ...
  const alterMatch = sql.match(/ALTER\s+TABLE\s+(\w+)/i);

  // DROP TABLE [IF EXISTS] table_name [CASCADE|RESTRICT]
  const dropMatch = sql.match(/DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\w+)/i);

  // CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name ON table_name
  const createIndexMatch = sql.match(
    /CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?\w+\s+ON\s+(\w+)/i
  );

  // DROP INDEX index_name [ON table_name]
  const dropIndexMatch = sql.match(/DROP\s+INDEX\s+\w+(?:\s+ON\s+(\w+))?/i);

  const tableName =
    alterMatch?.[1] ||
    dropMatch?.[1] ||
    createIndexMatch?.[1] ||
    dropIndexMatch?.[1];

  if (tableName) {
    cache.invalidateTable(tableName);
  }
}

/**
 * Measure planning time
 */
export async function measurePlanningTime<T>(
  planFn: () => Promise<T> | T
): Promise<{ plan: T; durationMs: number }> {
  const start = performance.now();
  const plan = await planFn();
  const end = performance.now();
  return {
    plan,
    durationMs: end - start,
  };
}
