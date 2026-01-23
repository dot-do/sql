/**
 * Idempotency Key Utilities
 *
 * Provides utilities for generating and managing idempotency keys for SQL mutations.
 * Idempotency keys ensure that retried mutation requests are executed at most once,
 * preventing duplicate operations when requests are retried due to network failures
 * or timeouts.
 *
 * @packageDocumentation
 */

import type { SQLValue, IdempotencyConfig } from './types.js';
import { LRUCache, type LRUCacheStats } from './lru-cache.js';

// =============================================================================
// Idempotency Key Generation
// =============================================================================

/**
 * Generate a random string of specified length using crypto
 * @internal
 */
function generateRandomString(length: number): string {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  const array = new Uint8Array(length);
  crypto.getRandomValues(array);
  return Array.from(array, (byte) => chars[byte % chars.length]).join('');
}

/**
 * Generate SHA-256 hash of input string and return first n characters
 * @internal
 */
async function hashString(input: string, length: number): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(input);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  // Optimized: iterate directly over Uint8Array to avoid intermediate array allocations
  const bytes = new Uint8Array(hashBuffer);
  let hashHex = '';
  for (let i = 0; i < bytes.length && hashHex.length < length; i++) {
    hashHex += bytes[i].toString(16).padStart(2, '0');
  }
  return hashHex.slice(0, length);
}

/**
 * Generates an idempotency key for SQL mutation requests.
 *
 * The key format is: `{prefix}-{timestamp}-{random}-{hash}` (or `{timestamp}-{random}-{hash}` without prefix) where:
 * - timestamp: Unix milliseconds when the key was generated
 * - random: 8-character cryptographically random alphanumeric string
 * - hash: First 8 characters of SHA-256 hash of SQL + JSON-serialized params
 *
 * Idempotency keys ensure that retried mutation requests are executed at most once,
 * preventing duplicate operations when requests are retried due to network failures
 * or timeouts.
 *
 * @param sql - The SQL statement to generate a key for
 * @param params - Optional array of parameter values (included in hash calculation)
 * @param prefix - Optional prefix to prepend to the key (useful for namespacing)
 * @returns Promise resolving to the generated idempotency key string
 *
 * @example
 * ```typescript
 * // Generate a key for an INSERT statement
 * const key = await generateIdempotencyKey(
 *   'INSERT INTO users (name) VALUES (?)',
 *   ['Alice'],
 *   'user-service'
 * );
 * // Result: "user-service-1705432800000-abc12345-7f3a8b2c"
 *
 * // Without prefix
 * const key2 = await generateIdempotencyKey('DELETE FROM temp WHERE id = ?', [42]);
 * // Result: "1705432800000-xyz98765-2c4f6a8b"
 * ```
 *
 * @public
 * @since 0.1.0
 */
export async function generateIdempotencyKey(
  sql: string,
  params?: SQLValue[],
  prefix?: string
): Promise<string> {
  // Timestamp (Unix ms): Provides temporal ordering for debugging and enables
  // time-based key expiration policies. Also helps prevent collisions across
  // different time windows without relying solely on randomness.
  const timestamp = Date.now();

  // Random component (8 chars): Ensures uniqueness even when multiple keys are
  // generated in the same millisecond for different logical operations. Uses
  // cryptographic randomness to prevent guessability and collision attacks.
  const random = generateRandomString(8);

  // Content hash (8 chars of SHA-256): Binds the key to specific SQL+params,
  // enabling detection of accidental key reuse across different operations.
  // Provides an additional layer of collision resistance and allows the server
  // to verify that a retried request matches the original operation.
  const hashInput = sql + JSON.stringify(params ?? []);
  const hash = await hashString(hashInput, 8);

  const key = `${timestamp}-${random}-${hash}`;
  return prefix ? `${prefix}-${key}` : key;
}

/**
 * Determines if a SQL statement is a mutation (INSERT, UPDATE, or DELETE).
 *
 * This function checks whether a SQL statement modifies data, which is used
 * to determine if idempotency keys should be generated for the request.
 * Only mutation queries need idempotency protection to prevent duplicate
 * side effects from retries.
 *
 * @param sql - The SQL statement to check
 * @returns `true` if the statement is INSERT, UPDATE, or DELETE; `false` otherwise
 *
 * @example
 * ```typescript
 * isMutationQuery('INSERT INTO users (name) VALUES (?)'); // true
 * isMutationQuery('UPDATE users SET name = ? WHERE id = ?'); // true
 * isMutationQuery('DELETE FROM users WHERE id = ?'); // true
 * isMutationQuery('SELECT * FROM users'); // false
 * isMutationQuery('CREATE TABLE users (id INT)'); // false
 * ```
 *
 * @public
 * @since 0.1.0
 */
export function isMutationQuery(sql: string): boolean {
  const trimmed = sql.trim().toUpperCase();
  return (
    trimmed.startsWith('INSERT') ||
    trimmed.startsWith('UPDATE') ||
    trimmed.startsWith('DELETE')
  );
}

// =============================================================================
// Idempotency Cache Statistics
// =============================================================================

/**
 * Statistics for the idempotency cache.
 *
 * Useful for monitoring cache effectiveness and tuning configuration.
 *
 * @public
 */
export interface IdempotencyCacheStats extends LRUCacheStats {}

// =============================================================================
// Idempotency Key Manager
// =============================================================================

/**
 * Manages idempotency keys for SQL operations.
 *
 * Provides caching of idempotency keys to ensure the same key is used
 * for retries of the same request, with automatic TTL-based expiration
 * and LRU eviction.
 *
 * @example
 * ```typescript
 * const manager = new IdempotencyKeyManager({
 *   enabled: true,
 *   keyPrefix: 'my-service',
 *   maxCacheSize: 1000,
 *   cacheTtlMs: 5 * 60 * 1000, // 5 minutes
 * });
 *
 * // Get or generate a key for a mutation
 * const key = await manager.getKey('INSERT INTO users VALUES (?)', [1]);
 *
 * // Clear a key after successful execution
 * manager.clearKey('INSERT INTO users VALUES (?)', [1]);
 *
 * // Get cache statistics
 * const stats = manager.getStats();
 * console.log(`Cache hit rate: ${stats.hits / (stats.hits + stats.misses)}`);
 * ```
 *
 * @public
 */
export class IdempotencyKeyManager {
  private readonly config: IdempotencyConfig;
  private readonly cache: LRUCache<string, string>;
  private cleanupTimer: ReturnType<typeof setInterval> | null = null;

  /**
   * Creates a new IdempotencyKeyManager.
   *
   * @param config - Idempotency configuration options
   */
  constructor(config: IdempotencyConfig) {
    this.config = config;

    // Initialize LRU cache with configured limits
    const maxCacheSize = config.maxCacheSize ?? 1000;
    const cacheTtlMs = config.cacheTtlMs ?? 5 * 60 * 1000;
    this.cache = new LRUCache<string, string>(maxCacheSize, cacheTtlMs);

    // Start cleanup timer if enabled
    const cleanupIntervalMs = config.cleanupIntervalMs ?? 60 * 1000;
    if (cleanupIntervalMs > 0) {
      this.cleanupTimer = setInterval(() => {
        this.cache.cleanup();
      }, cleanupIntervalMs);
    }
  }

  /**
   * Gets or generates an idempotency key for a mutation request.
   *
   * Reuses the same key for retries of the same request to ensure at-most-once
   * execution semantics. Keys are cached by SQL+params combination.
   *
   * @param sql - The SQL statement to get a key for
   * @param params - Optional parameter values (used for cache key generation)
   * @returns Promise resolving to the idempotency key, or undefined if idempotency is disabled or not a mutation
   */
  async getKey(sql: string, params?: SQLValue[]): Promise<string | undefined> {
    if (!this.config.enabled || !isMutationQuery(sql)) {
      return undefined;
    }

    // Create a cache key based on the SQL and params
    const cacheKey = sql + JSON.stringify(params ?? []);

    // Check if we already have a key for this request (retry scenario)
    let key = this.cache.get(cacheKey);
    if (!key) {
      key = await generateIdempotencyKey(sql, params, this.config.keyPrefix);
      this.cache.set(cacheKey, key);
    }

    return key;
  }

  /**
   * Clears the cached idempotency key for a request.
   *
   * Should be called after successful execution or non-retryable failures
   * to allow generation of a new key for the same SQL+params combination.
   *
   * @param sql - The SQL statement whose key should be cleared
   * @param params - Optional parameter values (must match original call to getKey)
   */
  clearKey(sql: string, params?: SQLValue[]): void {
    const cacheKey = sql + JSON.stringify(params ?? []);
    this.cache.delete(cacheKey);
  }

  /**
   * Gets the current size of the idempotency key cache.
   *
   * @returns The number of entries currently in the cache
   */
  getCacheSize(): number {
    return this.cache.size;
  }

  /**
   * Clears all entries from the idempotency key cache.
   *
   * Use with caution as this may affect retry semantics for in-flight requests.
   */
  clearCache(): void {
    this.cache.clear();
  }

  /**
   * Gets statistics about the idempotency key cache.
   *
   * @returns Cache statistics including size, hits, misses, and evictions
   */
  getStats(): IdempotencyCacheStats {
    return this.cache.getStats();
  }

  /**
   * Triggers manual cleanup of expired entries from the idempotency cache.
   *
   * @returns The number of expired entries that were removed
   */
  cleanupCache(): number {
    return this.cache.cleanup();
  }

  /**
   * Checks if the cleanup timer is currently active.
   *
   * @returns `true` if the cleanup timer is running
   */
  isCleanupTimerActive(): boolean {
    return this.cleanupTimer !== null;
  }

  /**
   * Stops the cleanup timer and releases resources.
   *
   * Should be called when the manager is no longer needed.
   */
  destroy(): void {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
  }
}
