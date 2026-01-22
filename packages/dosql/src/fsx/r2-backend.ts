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
 */

import {
  type FSXBackend,
  type FSXBackendWithMeta,
  type FSXMetadata,
  type ByteRange,
  FSXError,
  FSXErrorCode,
} from './types.js';
import { R2Error, R2ErrorCode, createR2Error } from './r2-errors.js';

// =============================================================================
// R2 Interface Types (Cloudflare Workers Types)
// =============================================================================

/**
 * Minimal R2Bucket interface
 * Compatible with Cloudflare Workers R2Bucket binding
 */
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
// R2 Backend Implementation
// =============================================================================

/**
 * R2 storage backend configuration
 */
export interface R2BackendConfig {
  /** Key prefix for all objects */
  keyPrefix?: string;
  /** Default storage class for new objects */
  defaultStorageClass?: 'Standard' | 'InfrequentAccess';
  /** Custom metadata to attach to all objects */
  defaultMetadata?: Record<string, string>;
}

/**
 * R2 storage backend implementation
 */
export class R2StorageBackend implements FSXBackendWithMeta {
  private readonly bucket: R2BucketLike;
  private readonly keyPrefix: string;
  private readonly defaultStorageClass: 'Standard' | 'InfrequentAccess';
  private readonly defaultMetadata: Record<string, string>;

  constructor(bucket: R2BucketLike, config: R2BackendConfig = {}) {
    this.bucket = bucket;
    this.keyPrefix = config.keyPrefix ?? '';
    this.defaultStorageClass = config.defaultStorageClass ?? 'Standard';
    this.defaultMetadata = config.defaultMetadata ?? {};
  }

  // ===========================================================================
  // FSXBackend Implementation
  // ===========================================================================

  async read(path: string, range?: ByteRange): Promise<Uint8Array | null> {
    const key = this.toR2Key(path);

    try {
      const options: R2GetOptions = {};

      if (range) {
        const [start, end] = range;
        // R2 uses offset + length, not start + end
        options.range = {
          offset: start,
          length: end - start + 1,
        };
      }

      const obj = await this.bucket.get(key, options);
      if (!obj) return null;

      const buffer = await obj.arrayBuffer();
      return new Uint8Array(buffer);
    } catch (error) {
      throw createR2Error(error, path, 'read');
    }
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    const key = this.toR2Key(path);

    try {
      // Convert to ArrayBuffer for R2
      const buffer = this.toArrayBuffer(data);

      await this.bucket.put(key, buffer, {
        customMetadata: {
          ...this.defaultMetadata,
          'fsx-created': new Date().toISOString(),
        },
        storageClass: this.defaultStorageClass,
      });
    } catch (error) {
      throw createR2Error(error, path, 'write');
    }
  }

  async delete(path: string): Promise<void> {
    const key = this.toR2Key(path);

    try {
      await this.bucket.delete(key);
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

  /**
   * Read with conditional get (if-match/if-none-match)
   */
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
      // Could be not found OR not modified (304)
      const exists = await this.exists(path);
      if (exists && conditions.etag) {
        return { data: null, notModified: true };
      }
      return { data: null, notModified: false };
    }

    const buffer = await obj.arrayBuffer();
    return { data: new Uint8Array(buffer), notModified: false };
  }

  /**
   * Write with custom metadata
   */
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

  /**
   * Batch delete multiple paths
   */
  async deleteMany(paths: string[]): Promise<void> {
    if (paths.length === 0) return;

    try {
      const keys = paths.map((p) => this.toR2Key(p));

      // R2 supports batch delete
      await this.bucket.delete(keys);
    } catch (error) {
      throw createR2Error(error, paths.join(', '), 'deleteMany');
    }
  }

  /**
   * List with metadata included
   */
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

  /**
   * Convert Uint8Array to ArrayBuffer for R2
   * Handles subarray views correctly
   */
  private toArrayBuffer(data: Uint8Array): ArrayBuffer {
    return data.buffer.slice(
      data.byteOffset,
      data.byteOffset + data.byteLength
    ) as ArrayBuffer;
  }

  // ===========================================================================
  // Administrative Methods
  // ===========================================================================

  /**
   * Get storage statistics for a prefix
   */
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

/**
 * Create an R2 storage backend
 * @param bucket - R2Bucket binding from Cloudflare Workers
 * @param config - Optional configuration
 */
export function createR2Backend(
  bucket: R2BucketLike,
  config?: R2BackendConfig
): R2StorageBackend {
  return new R2StorageBackend(bucket, config);
}
