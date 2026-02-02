/**
 * In-Memory Storage Implementation
 *
 * A simple in-memory implementation of StorageInterface for testing
 * and development purposes.
 *
 * @packageDocumentation
 */

import type {
  StorageInterface,
  StorageInterfaceWithBatch,
  StorageInterfaceWithMeta,
  StorageMetadata,
  BatchPutRequest,
  ByteRange,
} from './interface.js';

// =============================================================================
// Memory Storage Implementation
// =============================================================================

/**
 * Configuration options for MemoryStorage.
 */
export interface MemoryStorageOptions {
  /** Maximum total size in bytes (default: no limit) */
  maxSize?: number;

  /** Enable tracking of access times for metadata */
  trackAccessTime?: boolean;

  /** Initial data to populate storage with */
  initialData?: Map<string, Uint8Array>;
}

/**
 * Internal storage entry with metadata.
 */
interface StorageEntry {
  data: Uint8Array;
  createdAt: Date;
  modifiedAt: Date;
  accessedAt: Date;
}

/**
 * In-memory storage implementation for testing and development.
 *
 * Implements all storage interfaces including batch operations and metadata.
 *
 * @example
 * ```typescript
 * const storage = new MemoryStorage();
 *
 * await storage.put('key1', new Uint8Array([1, 2, 3]));
 * const data = await storage.get('key1');
 * ```
 */
export class MemoryStorage
  implements StorageInterface, StorageInterfaceWithBatch, StorageInterfaceWithMeta
{
  private readonly data: Map<string, StorageEntry> = new Map();
  private readonly options: MemoryStorageOptions;
  private currentSize: number = 0;

  constructor(options: MemoryStorageOptions = {}) {
    this.options = options;

    // Initialize with provided data
    if (options.initialData) {
      for (const [key, value] of options.initialData) {
        const now = new Date();
        this.data.set(key, {
          data: value,
          createdAt: now,
          modifiedAt: now,
          accessedAt: now,
        });
        this.currentSize += value.byteLength;
      }
    }
  }

  // ===========================================================================
  // Core StorageInterface Methods
  // ===========================================================================

  async get(key: string, range?: ByteRange): Promise<Uint8Array | null> {
    const entry = this.data.get(key);
    if (!entry) {
      return null;
    }

    // Update access time if tracking is enabled
    if (this.options.trackAccessTime) {
      entry.accessedAt = new Date();
    }

    // Handle range requests
    if (range) {
      const [start, end] = range;
      if (start < 0 || end < start || start >= entry.data.byteLength) {
        return null;
      }
      // Range is inclusive, so we add 1 to end for slice
      return entry.data.slice(start, Math.min(end + 1, entry.data.byteLength));
    }

    return entry.data;
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    const existingEntry = this.data.get(key);

    // Check size limit
    if (this.options.maxSize !== undefined) {
      const newSize =
        this.currentSize -
        (existingEntry?.data.byteLength ?? 0) +
        data.byteLength;
      if (newSize > this.options.maxSize) {
        throw new Error(
          `Storage size limit exceeded: ${newSize} > ${this.options.maxSize}`
        );
      }
      this.currentSize = newSize;
    } else if (existingEntry) {
      this.currentSize =
        this.currentSize - existingEntry.data.byteLength + data.byteLength;
    } else {
      this.currentSize += data.byteLength;
    }

    const now = new Date();
    this.data.set(key, {
      data,
      createdAt: existingEntry?.createdAt ?? now,
      modifiedAt: now,
      accessedAt: now,
    });
  }

  async delete(key: string): Promise<void> {
    const entry = this.data.get(key);
    if (entry) {
      this.currentSize -= entry.data.byteLength;
      this.data.delete(key);
    }
  }

  async list(prefix: string): Promise<string[]> {
    const keys: string[] = [];
    for (const key of this.data.keys()) {
      if (key.startsWith(prefix)) {
        keys.push(key);
      }
    }
    return keys.sort();
  }

  async exists(key: string): Promise<boolean> {
    return this.data.has(key);
  }

  // ===========================================================================
  // StorageInterfaceWithMeta Methods
  // ===========================================================================

  async metadata(key: string): Promise<StorageMetadata | null> {
    const entry = this.data.get(key);
    if (!entry) {
      return null;
    }

    return {
      size: entry.data.byteLength,
      lastModified: entry.modifiedAt,
    };
  }

  // ===========================================================================
  // StorageInterfaceWithBatch Methods
  // ===========================================================================

  async putMany(items: BatchPutRequest[]): Promise<void> {
    for (const { key, data } of items) {
      await this.put(key, data);
    }
  }

  async deleteMany(keys: string[]): Promise<void> {
    for (const key of keys) {
      await this.delete(key);
    }
  }

  async getMany(keys: string[]): Promise<Map<string, Uint8Array>> {
    const results = new Map<string, Uint8Array>();
    for (const key of keys) {
      const data = await this.get(key);
      if (data !== null) {
        results.set(key, data);
      }
    }
    return results;
  }

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  /**
   * Clear all data from storage.
   */
  clear(): void {
    this.data.clear();
    this.currentSize = 0;
  }

  /**
   * Get the current total size of stored data.
   */
  get size(): number {
    return this.currentSize;
  }

  /**
   * Get the number of keys in storage.
   */
  get count(): number {
    return this.data.size;
  }

  /**
   * Get a snapshot of all keys.
   */
  keys(): string[] {
    return Array.from(this.data.keys());
  }

  /**
   * Export all data as a Map (for testing/debugging).
   */
  export(): Map<string, Uint8Array> {
    const result = new Map<string, Uint8Array>();
    for (const [key, entry] of this.data) {
      result.set(key, entry.data);
    }
    return result;
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a new in-memory storage instance.
 *
 * @param options - Configuration options
 * @returns MemoryStorage instance
 *
 * @example
 * ```typescript
 * const storage = createMemoryStorage();
 * const btree = createBTree(storage, keyCodec, valueCodec);
 * ```
 */
export function createMemoryStorage(
  options?: MemoryStorageOptions
): MemoryStorage {
  return new MemoryStorage(options);
}
