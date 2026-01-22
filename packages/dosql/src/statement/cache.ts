/**
 * Statement Cache for DoSQL
 *
 * LRU cache for prepared statements to improve performance
 * by reusing parsed SQL plans.
 */

import type { Statement } from './types.js';
import type { ParsedParameters } from './binding.js';

// =============================================================================
// CACHE ENTRY
// =============================================================================

/**
 * Entry in the statement cache
 */
export interface CacheEntry<T = unknown> {
  /**
   * The cached statement
   */
  statement: Statement<T>;

  /**
   * Parsed parameter information
   */
  parsed: ParsedParameters;

  /**
   * SQL hash key
   */
  key: string;

  /**
   * Last access timestamp
   */
  lastAccess: number;

  /**
   * Number of times this statement was retrieved from cache
   */
  hitCount: number;

  /**
   * Size estimate in bytes
   */
  size: number;
}

// =============================================================================
// CACHE OPTIONS
// =============================================================================

/**
 * Options for the statement cache
 */
export interface CacheOptions {
  /**
   * Maximum number of statements to cache (default: 100)
   */
  maxSize?: number;

  /**
   * Maximum total memory in bytes (default: 10MB)
   */
  maxMemory?: number;

  /**
   * Time-to-live in milliseconds (0 = no TTL, default: 0)
   */
  ttl?: number;

  /**
   * Whether to enable cache statistics (default: true)
   */
  enableStats?: boolean;
}

// =============================================================================
// CACHE STATISTICS
// =============================================================================

/**
 * Statistics about cache performance
 */
export interface CacheStats {
  /**
   * Number of cache hits
   */
  hits: number;

  /**
   * Number of cache misses
   */
  misses: number;

  /**
   * Current number of cached statements
   */
  size: number;

  /**
   * Total memory used by cached statements
   */
  memoryUsage: number;

  /**
   * Number of evictions due to capacity
   */
  evictions: number;

  /**
   * Hit rate as a percentage
   */
  hitRate: number;
}

// =============================================================================
// HASH FUNCTION
// =============================================================================

/**
 * Fast string hash function (djb2)
 */
export function hashString(str: string): string {
  let hash = 5381;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) + hash) ^ str.charCodeAt(i);
  }
  return (hash >>> 0).toString(16);
}

/**
 * Estimate memory size of a SQL string
 */
export function estimateSqlSize(sql: string): number {
  // String size + overhead for statement object
  return sql.length * 2 + 256;
}

// =============================================================================
// LRU CACHE NODE
// =============================================================================

/**
 * Doubly-linked list node for LRU ordering
 */
interface LRUNode<T> {
  key: string;
  entry: CacheEntry<T>;
  prev: LRUNode<T> | null;
  next: LRUNode<T> | null;
}

// =============================================================================
// STATEMENT CACHE
// =============================================================================

/**
 * LRU cache for prepared statements
 *
 * @example
 * ```typescript
 * const cache = new StatementCache({ maxSize: 100 });
 *
 * // Try to get cached statement
 * let entry = cache.get(sql);
 * if (!entry) {
 *   // Parse and create new statement
 *   const parsed = parseParameters(sql);
 *   const statement = createStatement(sql, parsed);
 *   entry = cache.set(sql, statement, parsed);
 * }
 *
 * // Use the statement
 * entry.statement.run(params);
 * ```
 */
export class StatementCache<T = unknown> {
  private readonly maxSize: number;
  private readonly maxMemory: number;
  private readonly ttl: number;
  private readonly enableStats: boolean;

  private cache: Map<string, LRUNode<T>>;
  private head: LRUNode<T> | null = null;
  private tail: LRUNode<T> | null = null;
  private currentMemory = 0;

  // Statistics
  private hits = 0;
  private misses = 0;
  private evictions = 0;

  constructor(options: CacheOptions = {}) {
    this.maxSize = options.maxSize ?? 100;
    this.maxMemory = options.maxMemory ?? 10 * 1024 * 1024; // 10MB
    this.ttl = options.ttl ?? 0;
    this.enableStats = options.enableStats ?? true;
    this.cache = new Map();
  }

  /**
   * Get a statement from the cache
   *
   * @param sql - SQL string to look up
   * @returns Cache entry or undefined if not found
   */
  get(sql: string): CacheEntry<T> | undefined {
    const key = hashString(sql);
    const node = this.cache.get(key);

    if (!node) {
      if (this.enableStats) {
        this.misses++;
      }
      return undefined;
    }

    // Check TTL
    if (this.ttl > 0) {
      const now = Date.now();
      if (now - node.entry.lastAccess > this.ttl) {
        this.delete(sql);
        if (this.enableStats) {
          this.misses++;
        }
        return undefined;
      }
    }

    // Move to head (most recently used)
    this.moveToHead(node);

    // Update access stats
    node.entry.lastAccess = Date.now();
    node.entry.hitCount++;

    if (this.enableStats) {
      this.hits++;
    }

    return node.entry;
  }

  /**
   * Add a statement to the cache
   *
   * @param sql - SQL string
   * @param statement - Prepared statement
   * @param parsed - Parsed parameter information
   * @returns The cache entry
   */
  set(
    sql: string,
    statement: Statement<T>,
    parsed: ParsedParameters
  ): CacheEntry<T> {
    const key = hashString(sql);
    const size = estimateSqlSize(sql);

    // Check if already exists
    const existing = this.cache.get(key);
    if (existing) {
      // Update existing entry
      existing.entry.statement = statement;
      existing.entry.parsed = parsed;
      existing.entry.lastAccess = Date.now();
      this.moveToHead(existing);
      return existing.entry;
    }

    // Create new entry
    const entry: CacheEntry<T> = {
      statement,
      parsed,
      key,
      lastAccess: Date.now(),
      hitCount: 0,
      size,
    };

    const node: LRUNode<T> = {
      key,
      entry,
      prev: null,
      next: null,
    };

    // Add to map and linked list
    this.cache.set(key, node);
    this.addToHead(node);
    this.currentMemory += size;

    // Evict if necessary
    this.evictIfNeeded();

    return entry;
  }

  /**
   * Check if SQL is in cache
   *
   * @param sql - SQL string
   * @returns true if cached
   */
  has(sql: string): boolean {
    const key = hashString(sql);
    return this.cache.has(key);
  }

  /**
   * Remove a statement from the cache
   *
   * @param sql - SQL string
   * @returns true if removed
   */
  delete(sql: string): boolean {
    const key = hashString(sql);
    const node = this.cache.get(key);

    if (!node) {
      return false;
    }

    // Remove from linked list
    this.removeNode(node);

    // Update memory tracking
    this.currentMemory -= node.entry.size;

    // Finalize the statement
    if (!node.entry.statement.finalized) {
      node.entry.statement.finalize();
    }

    // Remove from map
    this.cache.delete(key);

    return true;
  }

  /**
   * Clear all cached statements
   */
  clear(): void {
    // Finalize all statements
    for (const node of this.cache.values()) {
      if (!node.entry.statement.finalized) {
        node.entry.statement.finalize();
      }
    }

    this.cache.clear();
    this.head = null;
    this.tail = null;
    this.currentMemory = 0;
    this.hits = 0;
    this.misses = 0;
    this.evictions = 0;
  }

  /**
   * Get cache statistics
   */
  getStats(): CacheStats {
    const total = this.hits + this.misses;
    return {
      hits: this.hits,
      misses: this.misses,
      size: this.cache.size,
      memoryUsage: this.currentMemory,
      evictions: this.evictions,
      hitRate: total > 0 ? (this.hits / total) * 100 : 0,
    };
  }

  /**
   * Get current cache size
   */
  get size(): number {
    return this.cache.size;
  }

  /**
   * Get current memory usage
   */
  get memoryUsage(): number {
    return this.currentMemory;
  }

  /**
   * Iterate over all cached entries (most recent first)
   */
  *entries(): IterableIterator<CacheEntry<T>> {
    let node = this.head;
    while (node) {
      yield node.entry;
      node = node.next;
    }
  }

  /**
   * Get all cached SQL strings
   */
  keys(): string[] {
    return Array.from(this.cache.values()).map(
      node => node.entry.parsed.originalSql
    );
  }

  // ==========================================================================
  // PRIVATE METHODS
  // ==========================================================================

  /**
   * Add node to head of linked list
   */
  private addToHead(node: LRUNode<T>): void {
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

  /**
   * Remove node from linked list
   */
  private removeNode(node: LRUNode<T>): void {
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

  /**
   * Move node to head (most recently used)
   */
  private moveToHead(node: LRUNode<T>): void {
    if (node === this.head) {
      return;
    }

    this.removeNode(node);
    this.addToHead(node);
  }

  /**
   * Evict entries if over capacity
   */
  private evictIfNeeded(): void {
    // Evict by count
    while (this.cache.size > this.maxSize && this.tail) {
      this.evictTail();
    }

    // Evict by memory
    while (this.currentMemory > this.maxMemory && this.tail) {
      this.evictTail();
    }
  }

  /**
   * Evict the least recently used entry
   */
  private evictTail(): void {
    if (!this.tail) {
      return;
    }

    const node = this.tail;

    // Finalize the statement
    if (!node.entry.statement.finalized) {
      node.entry.statement.finalize();
    }

    // Remove from linked list
    this.removeNode(node);

    // Update memory tracking
    this.currentMemory -= node.entry.size;

    // Remove from map
    this.cache.delete(node.key);

    if (this.enableStats) {
      this.evictions++;
    }
  }
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a new statement cache
 *
 * @param options - Cache options
 * @returns New cache instance
 */
export function createStatementCache<T = unknown>(
  options?: CacheOptions
): StatementCache<T> {
  return new StatementCache<T>(options);
}
