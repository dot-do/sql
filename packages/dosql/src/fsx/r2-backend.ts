/**
 * R2 Object Storage Backend for FSX
 *
 * Implements FSXBackend using Cloudflare R2 for cold/archival storage.
 *
 * Key features:
 * - Efficient range reads for partial data access
 * - Large file support (up to 5GB single part, 5TB multipart)
 * - ETag-based integrity checking
 * - Metadata preservation
 * - Structured error handling with R2-specific error types
 * - Retry with exponential backoff and jitter
 * - Circuit breaker pattern for fault tolerance
 * - Read cache for graceful degradation
 * - Health status reporting
 */

import {
  type FSXBackend,
  type FSXBackendWithMeta,
  type FSXMetadata,
  type ByteRange,
  FSXError,
  FSXErrorCode,
} from './types.js';
import {
  R2Error,
  R2ErrorCode,
  createR2Error,
  createRetriesExhaustedError,
  createCircuitOpenError,
  createPartialWriteError,
  detectR2ErrorType,
} from './r2-errors.js';
import { sleep } from '../utils/retry.js';
import { LRUCache } from '../btree/lru-cache.js';

// =============================================================================
// R2 Interface Types (Cloudflare Workers Types)
// =============================================================================

export interface R2BucketLike {
  get(key: string, options?: R2GetOptions): Promise<R2ObjectLike | null>;
  put(key: string, value: R2PutValue, options?: R2PutOptions): Promise<R2ObjectLike>;
  delete(keys: string | string[]): Promise<void>;
  list(options?: R2ListOptions): Promise<R2ObjectsLike>;
  head(key: string): Promise<R2ObjectLike | null>;
}

export interface R2GetOptions {
  range?: R2Range;
  onlyIf?: R2Conditional;
}

export interface R2Range {
  offset?: number;
  length?: number;
  suffix?: number;
}

export interface R2Conditional {
  etagMatches?: string;
  etagDoesNotMatch?: string;
  uploadedBefore?: Date;
  uploadedAfter?: Date;
}

export type R2PutValue =
  | ArrayBuffer
  | ArrayBufferView
  | string
  | Blob
  | ReadableStream;

export interface R2PutOptions {
  httpMetadata?: R2HTTPMetadata;
  customMetadata?: Record<string, string>;
  md5?: ArrayBuffer | string;
  sha1?: ArrayBuffer | string;
  sha256?: ArrayBuffer | string;
  sha384?: ArrayBuffer | string;
  sha512?: ArrayBuffer | string;
  storageClass?: 'Standard' | 'InfrequentAccess';
}

export interface R2HTTPMetadata {
  contentType?: string;
  contentLanguage?: string;
  contentDisposition?: string;
  contentEncoding?: string;
  cacheControl?: string;
  cacheExpiry?: Date;
}

export interface R2ObjectLike {
  key: string;
  size: number;
  etag: string;
  httpEtag: string;
  uploaded: Date;
  httpMetadata?: R2HTTPMetadata;
  customMetadata?: Record<string, string>;
  range?: R2Range;
  checksums?: R2Checksums;
  storageClass?: string;
  body?: ReadableStream;
  bodyUsed?: boolean;
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
  json<T>(): Promise<T>;
  blob(): Promise<Blob>;
  writeHttpMetadata(headers: Headers): void;
}

export interface R2Checksums {
  md5?: ArrayBuffer;
  sha1?: ArrayBuffer;
  sha256?: ArrayBuffer;
  sha384?: ArrayBuffer;
  sha512?: ArrayBuffer;
}

export interface R2ListOptions {
  prefix?: string;
  cursor?: string;
  delimiter?: string;
  limit?: number;
  include?: ('httpMetadata' | 'customMetadata')[];
  startAfter?: string;
}

export interface R2ObjectsLike {
  objects: R2ObjectLike[];
  truncated: boolean;
  cursor?: string;
  delimitedPrefixes?: string[];
}

// =============================================================================
// Circuit Breaker
// =============================================================================

class CircuitBreaker {
  private _failureCount = 0;
  private readonly threshold: number;

  constructor(threshold = 5) {
    this.threshold = threshold;
  }

  get failureCount(): number {
    return this._failureCount;
  }

  /** Whether circuit is open (too many consecutive failures) */
  isOpen(): boolean {
    return this._failureCount >= this.threshold;
  }

  recordSuccess(): void {
    this._failureCount = 0;
  }

  recordFailure(): void {
    this._failureCount++;
  }
}

// =============================================================================
// Retry Helper (uses shared sleep from utils/retry.ts)
// =============================================================================

const MAX_RETRIES = 3;
const BASE_DELAY_MS = 100;

function computeDelay(attempt: number): number {
  const base = BASE_DELAY_MS * Math.pow(2, attempt);
  const jitter = base * 0.25 * (Math.random() * 2 - 1);
  return Math.max(1, Math.round(base + jitter));
}

function isRetryableError(error: unknown): boolean {
  const errorType = detectR2ErrorType(error);
  return [
    R2ErrorCode.TIMEOUT,
    R2ErrorCode.RATE_LIMITED,
    R2ErrorCode.NETWORK_ERROR,
    R2ErrorCode.READ_DURING_WRITE,
  ].includes(errorType);
}

// =============================================================================
// R2 Backend Implementation
// =============================================================================

/**
 * Default cache size: 16MB
 * This is a reasonable default for typical workloads while preventing unbounded memory growth.
 */
export const DEFAULT_READ_CACHE_MAX_BYTES = 16 * 1024 * 1024;

/**
 * Default maximum number of cache entries: 1000
 * This provides a fallback limit when entries are very small.
 */
export const DEFAULT_READ_CACHE_MAX_ENTRIES = 1000;

export interface R2BackendConfig {
  keyPrefix?: string;
  defaultStorageClass?: 'Standard' | 'InfrequentAccess';
  defaultMetadata?: Record<string, string>;
  maxRetries?: number;
  circuitBreakerThreshold?: number;
  /**
   * Maximum size of the read cache in bytes.
   * When the cache exceeds this size, least recently used entries are evicted.
   * Default: 16MB (DEFAULT_READ_CACHE_MAX_BYTES)
   */
  readCacheMaxBytes?: number;
  /**
   * Maximum number of entries in the read cache.
   * When the cache exceeds this count, least recently used entries are evicted.
   * Default: 1000 (DEFAULT_READ_CACHE_MAX_ENTRIES)
   */
  readCacheMaxEntries?: number;
}

export interface R2HealthStatus {
  status: 'healthy' | 'degraded' | 'unavailable';
  r2Available: boolean;
  circuitState: string;
  failureCount: number;
}

export class R2StorageBackend implements FSXBackendWithMeta {
  private readonly bucket: R2BucketLike;
  private readonly keyPrefix: string;
  private readonly defaultStorageClass: 'Standard' | 'InfrequentAccess';
  private readonly defaultMetadata: Record<string, string>;
  private readonly maxRetries: number;
  private readonly circuitBreaker: CircuitBreaker;
  private readonly readCache: LRUCache<string, Uint8Array>;
  private readonly maxCacheEntries: number;
  private readonly degradedListeners: Array<(entering: boolean) => void> = [];
  private isDegraded = false;

  constructor(bucket: R2BucketLike, config: R2BackendConfig = {}) {
    this.bucket = bucket;
    this.keyPrefix = config.keyPrefix ?? '';
    this.defaultStorageClass = config.defaultStorageClass ?? 'Standard';
    this.defaultMetadata = config.defaultMetadata ?? {};
    this.maxRetries = config.maxRetries ?? MAX_RETRIES;
    this.circuitBreaker = new CircuitBreaker(config.circuitBreakerThreshold ?? 5);

    // Initialize LRU cache with configurable size limits
    const maxBytes = config.readCacheMaxBytes ?? DEFAULT_READ_CACHE_MAX_BYTES;
    this.maxCacheEntries = config.readCacheMaxEntries ?? DEFAULT_READ_CACHE_MAX_ENTRIES;

    this.readCache = new LRUCache<string, Uint8Array>({
      maxSize: maxBytes,
      sizeCalculator: (value: Uint8Array) => value.byteLength,
    });
  }

  /**
   * Set a value in the read cache, enforcing both byte and entry limits.
   * The LRU cache handles byte-based eviction automatically.
   * This method additionally enforces the entry count limit.
   */
  private setCacheEntry(path: string, data: Uint8Array): void {
    // First, add the entry (LRU cache handles byte-based eviction)
    this.readCache.set(path, data);

    // Then, enforce entry count limit by evicting oldest entries
    while (this.readCache.size > this.maxCacheEntries) {
      // Get the oldest key (first in iteration order)
      const oldestKey = this.readCache.keys().next().value;
      if (oldestKey !== undefined) {
        this.readCache.delete(oldestKey);
      } else {
        break;
      }
    }
  }

  // ===========================================================================
  // FSXBackend Implementation
  // ===========================================================================

  async read(path: string, range?: ByteRange): Promise<Uint8Array | null> {
    const key = this.toR2Key(path);
    const circuitOpen = this.circuitBreaker.isOpen();

    // When circuit is open, try one probe attempt (no retries)
    // If we have cached data, return it directly when circuit is open
    if (circuitOpen) {
      const cached = this.readCache.get(path);
      if (cached) return cached;
    }

    const maxAttempts = circuitOpen ? 0 : this.maxRetries;
    let lastError: unknown;

    for (let attempt = 0; attempt <= maxAttempts; attempt++) {
      try {
        const options: R2GetOptions = {};
        if (range) {
          const [start, end] = range;
          options.range = { offset: start, length: end - start + 1 };
        }

        const obj = await this.bucket.get(key, options);
        if (!obj) return null;

        const buffer = await obj.arrayBuffer();
        const result = new Uint8Array(buffer);

        this.setCacheEntry(path, result);
        this.circuitBreaker.recordSuccess();
        this.checkDegradedExit();
        return result;
      } catch (error) {
        lastError = error;
        this.circuitBreaker.recordFailure();
        this.checkDegradedEnter();

        if (!isRetryableError(error) || attempt === maxAttempts) {
          break;
        }

        // If circuit just opened mid-retry, check cache and bail
        if (this.circuitBreaker.isOpen()) {
          const cached = this.readCache.get(path);
          if (cached) return cached;
          break;
        }

        await sleep(computeDelay(attempt));
      }
    }

    // Try cache as last resort
    const cached = this.readCache.get(path);
    if (cached) return cached;

    // If circuit is open, throw circuit error
    if (this.circuitBreaker.isOpen()) {
      throw createCircuitOpenError(path, 'read', this.circuitBreaker.failureCount);
    }

    if (maxAttempts > 0 && isRetryableError(lastError)) {
      throw createRetriesExhaustedError(lastError, path, 'read', maxAttempts);
    }

    throw createR2Error(lastError, path, 'read');
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    const key = this.toR2Key(path);
    const circuitOpen = this.circuitBreaker.isOpen();

    // When circuit is open, allow one probe attempt (no retries)
    const maxAttempts = circuitOpen ? 0 : this.maxRetries;
    let lastError: unknown;

    for (let attempt = 0; attempt <= maxAttempts; attempt++) {
      try {
        const buffer = this.toArrayBuffer(data);

        const result = await this.bucket.put(key, buffer, {
          customMetadata: {
            ...this.defaultMetadata,
            'fsx-created': new Date().toISOString(),
          },
          storageClass: this.defaultStorageClass,
        });

        // Verify write integrity
        if (result.size !== data.length) {
          try { await this.bucket.delete(key); } catch { /* ignore */ }
          throw createPartialWriteError(path, new Error(`Expected ${data.length} bytes, wrote ${result.size}`));
        }

        this.circuitBreaker.recordSuccess();
        this.checkDegradedExit();
        return;
      } catch (error) {
        lastError = error;

        // Propagate FSXError immediately
        if (error instanceof FSXError) {
          try { await this.bucket.delete(key); } catch { /* ignore */ }
          throw error;
        }

        this.circuitBreaker.recordFailure();
        this.checkDegradedEnter();

        if (!isRetryableError(error) || attempt === maxAttempts) {
          break;
        }

        // If circuit just opened, fail fast
        if (this.circuitBreaker.isOpen()) {
          break;
        }

        await sleep(computeDelay(attempt));
      }
    }

    // If circuit is open, throw circuit error
    if (this.circuitBreaker.isOpen()) {
      throw createCircuitOpenError(path, 'write', this.circuitBreaker.failureCount);
    }

    if (maxAttempts > 0 && isRetryableError(lastError)) {
      throw createRetriesExhaustedError(lastError, path, 'write', maxAttempts);
    }

    throw createR2Error(lastError, path, 'write');
  }

  async delete(path: string): Promise<void> {
    const key = this.toR2Key(path);
    try {
      await this.bucket.delete(key);
      this.readCache.delete(path);
    } catch (error) {
      throw createR2Error(error, path, 'delete');
    }
  }

  async list(prefix: string): Promise<string[]> {
    const searchPrefix = this.toR2Key(prefix);
    const paths: string[] = [];
    let cursor: string | undefined;

    try {
      do {
        const result = await this.bucket.list({
          prefix: searchPrefix,
          cursor,
          limit: 1000,
        });

        for (const obj of result.objects) {
          paths.push(this.fromR2Key(obj.key));
        }

        cursor = result.truncated ? result.cursor : undefined;
      } while (cursor);

      return paths.sort();
    } catch (error) {
      throw createR2Error(error, prefix, 'list');
    }
  }

  async exists(path: string): Promise<boolean> {
    const key = this.toR2Key(path);
    try {
      const obj = await this.bucket.head(key);
      return obj !== null;
    } catch {
      return false;
    }
  }

  // ===========================================================================
  // FSXBackendWithMeta Implementation
  // ===========================================================================

  async metadata(path: string): Promise<FSXMetadata | null> {
    const key = this.toR2Key(path);
    try {
      const obj = await this.bucket.head(key);
      if (!obj) return null;
      return {
        size: obj.size,
        lastModified: obj.uploaded,
        etag: obj.etag,
        custom: obj.customMetadata,
      };
    } catch {
      return null;
    }
  }

  // ===========================================================================
  // Extended R2 Operations
  // ===========================================================================

  async readConditional(
    path: string,
    conditions: { etag?: string; ifNotEtag?: string }
  ): Promise<{ data: Uint8Array | null; notModified: boolean }> {
    const key = this.toR2Key(path);

    const options: R2GetOptions = {};
    if (conditions.etag) {
      options.onlyIf = { etagMatches: conditions.etag };
    } else if (conditions.ifNotEtag) {
      options.onlyIf = { etagDoesNotMatch: conditions.ifNotEtag };
    }

    const obj = await this.bucket.get(key, options);

    if (!obj) {
      const exists = await this.exists(path);
      if (exists && conditions.etag) {
        return { data: null, notModified: true };
      }
      return { data: null, notModified: false };
    }

    const buffer = await obj.arrayBuffer();
    return { data: new Uint8Array(buffer), notModified: false };
  }

  async writeWithMetadata(
    path: string,
    data: Uint8Array,
    metadata: {
      contentType?: string;
      customMetadata?: Record<string, string>;
      storageClass?: 'Standard' | 'InfrequentAccess';
    }
  ): Promise<{ etag: string }> {
    const key = this.toR2Key(path);

    // Optimistic locking via if-match ETag
    const ifMatch = metadata.customMetadata?.['if-match'];
    if (ifMatch) {
      const current = await this.bucket.head(key);
      if (current && current.etag !== ifMatch) {
        throw new FSXError(
          FSXErrorCode.WRITE_FAILED,
          `R2 concurrent write conflict for path "${path}": ETag conflict - retry the operation`,
          path,
        );
      }
    }

    try {
      const buffer = this.toArrayBuffer(data);

      const obj = await this.bucket.put(key, buffer, {
        httpMetadata: metadata.contentType
          ? { contentType: metadata.contentType }
          : undefined,
        customMetadata: {
          ...this.defaultMetadata,
          ...metadata.customMetadata,
          'fsx-created': new Date().toISOString(),
        },
        storageClass: metadata.storageClass ?? this.defaultStorageClass,
      });

      return { etag: obj.etag };
    } catch (error) {
      throw createR2Error(error, path, 'writeWithMetadata');
    }
  }

  async deleteMany(paths: string[]): Promise<void> {
    if (paths.length === 0) return;
    try {
      const keys = paths.map((p) => this.toR2Key(p));
      await this.bucket.delete(keys);
    } catch (error) {
      throw createR2Error(error, paths.join(', '), 'deleteMany');
    }
  }

  async listWithMetadata(
    prefix: string
  ): Promise<Array<{ path: string; metadata: FSXMetadata }>> {
    const searchPrefix = this.toR2Key(prefix);
    const results: Array<{ path: string; metadata: FSXMetadata }> = [];
    let cursor: string | undefined;

    try {
      do {
        const result = await this.bucket.list({
          prefix: searchPrefix,
          cursor,
          limit: 1000,
          include: ['customMetadata'],
        });

        for (const obj of result.objects) {
          results.push({
            path: this.fromR2Key(obj.key),
            metadata: {
              size: obj.size,
              lastModified: obj.uploaded,
              etag: obj.etag,
              custom: obj.customMetadata,
            },
          });
        }

        cursor = result.truncated ? result.cursor : undefined;
      } while (cursor);

      return results;
    } catch (error) {
      throw createR2Error(error, prefix, 'listWithMetadata');
    }
  }

  // ===========================================================================
  // Health & Degraded Mode
  // ===========================================================================

  async getHealthStatus(): Promise<R2HealthStatus> {
    const isOpen = this.circuitBreaker.isOpen();

    let status: 'healthy' | 'degraded' | 'unavailable';
    if (!isOpen && this.circuitBreaker.failureCount === 0) {
      status = 'healthy';
    } else {
      status = 'degraded';
    }

    return {
      status,
      r2Available: !isOpen,
      circuitState: isOpen ? 'open' : 'closed',
      failureCount: this.circuitBreaker.failureCount,
    };
  }

  onDegradedMode(listener: (entering: boolean) => void): void {
    this.degradedListeners.push(listener);
  }

  // ===========================================================================
  // Degraded Mode Helpers
  // ===========================================================================

  private checkDegradedEnter(): void {
    if (!this.isDegraded && this.circuitBreaker.isOpen()) {
      this.isDegraded = true;
      for (const listener of this.degradedListeners) {
        listener(true);
      }
    }
  }

  private checkDegradedExit(): void {
    if (this.isDegraded && !this.circuitBreaker.isOpen()) {
      this.isDegraded = false;
      for (const listener of this.degradedListeners) {
        listener(false);
      }
    }
  }

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  private toR2Key(path: string): string {
    if (this.keyPrefix) {
      return `${this.keyPrefix}/${path}`.replace(/\/+/g, '/');
    }
    return path;
  }

  private fromR2Key(key: string): string {
    if (this.keyPrefix && key.startsWith(this.keyPrefix)) {
      return key.slice(this.keyPrefix.length).replace(/^\/+/, '');
    }
    return key;
  }

  private toArrayBuffer(data: Uint8Array): ArrayBuffer {
    return data.buffer.slice(
      data.byteOffset,
      data.byteOffset + data.byteLength
    ) as ArrayBuffer;
  }

  // ===========================================================================
  // Cache Statistics & Management
  // ===========================================================================

  /**
   * Get statistics about the read cache for monitoring and debugging.
   */
  getReadCacheStats(): {
    entryCount: number;
    totalBytes: number;
    maxBytes: number;
    maxEntries: number;
    hitRate: number;
    hits: number;
    misses: number;
    evictions: number;
  } {
    return {
      entryCount: this.readCache.size,
      totalBytes: this.readCache.currentBytes,
      maxBytes: this.readCache.maxCacheSize,
      maxEntries: this.maxCacheEntries,
      hitRate: this.readCache.hitRate,
      hits: this.readCache.hits,
      misses: this.readCache.misses,
      evictions: this.readCache.evictions,
    };
  }

  /**
   * Clear the read cache.
   */
  clearReadCache(): void {
    this.readCache.clear();
  }

  // ===========================================================================
  // Administrative Methods
  // ===========================================================================

  async getStats(prefix: string = ''): Promise<{
    objectCount: number;
    totalSize: number;
    storageClasses: Record<string, number>;
  }> {
    const searchPrefix = this.toR2Key(prefix);
    let objectCount = 0;
    let totalSize = 0;
    const storageClasses: Record<string, number> = {};
    let cursor: string | undefined;

    try {
      do {
        const result = await this.bucket.list({
          prefix: searchPrefix,
          cursor,
          limit: 1000,
        });

        for (const obj of result.objects) {
          objectCount++;
          totalSize += obj.size;
          const storageClass = obj.storageClass ?? 'Standard';
          storageClasses[storageClass] = (storageClasses[storageClass] ?? 0) + 1;
        }

        cursor = result.truncated ? result.cursor : undefined;
      } while (cursor);

      return { objectCount, totalSize, storageClasses };
    } catch (error) {
      throw createR2Error(error, prefix, 'getStats');
    }
  }
}

// =============================================================================
// Factory Function
// =============================================================================

export function createR2Backend(
  bucket: R2BucketLike,
  config?: R2BackendConfig
): R2StorageBackend {
  return new R2StorageBackend(bucket, config);
}
