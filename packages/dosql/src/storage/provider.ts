/**
 * Unified Storage Provider Interface
 *
 * This module provides a unified storage abstraction that bridges the gap between:
 * - FSXBackend (fsx module) - uses read/write naming
 * - StorageInterface (storage module) - uses get/put naming
 *
 * The StorageProvider is the canonical interface that all storage backends should implement.
 * It provides:
 * - Consistent naming conventions (get/put as primary, with read/write aliases)
 * - Support for byte range reads
 * - Optional capabilities (batch operations, metadata, tiered storage)
 * - Adapters for legacy interfaces
 *
 * ## Storage Tier Architecture
 *
 * ```
 *                     StorageProvider (unified interface)
 *                              |
 *         +-------------------+-------------------+
 *         |                   |                   |
 *    DOProvider          R2Provider         MemoryProvider
 *    (Hot Tier)         (Cold Tier)          (Testing)
 *         |                   |
 *         +-------------------+
 *                   |
 *            TieredProvider
 *         (Hot + Cold combined)
 * ```
 *
 * ## Usage Examples
 *
 * ```typescript
 * // Create a provider from DO storage
 * const provider = createDOProvider(state.storage);
 *
 * // Create a provider from R2
 * const provider = createR2Provider(env.MY_BUCKET);
 *
 * // Create a tiered provider (hot + cold)
 * const hot = createDOProvider(state.storage);
 * const cold = createR2Provider(env.MY_BUCKET);
 * const provider = createTieredProvider(hot, cold);
 *
 * // Use with B-tree
 * const btree = createBTree(provider, keyCodec, valueCodec);
 *
 * // Use with Columnar storage
 * const writer = new ColumnarWriter(schema, config, provider);
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// Core Types
// =============================================================================

/**
 * Byte range specification for partial reads.
 * [start, end] - both inclusive, in bytes.
 *
 * @example
 * ```typescript
 * // Read first 1024 bytes
 * const header = await provider.get('file.dat', [0, 1023]);
 *
 * // Read bytes 1000-1999
 * const chunk = await provider.get('file.dat', [1000, 1999]);
 * ```
 */
export type ByteRange = readonly [start: number, end: number];

/**
 * Storage tier classification for tiered storage systems.
 */
export enum StorageTier {
  /** Hot storage (Durable Object) - fast, limited capacity */
  HOT = 'hot',
  /** Cold storage (R2) - slower, unlimited capacity */
  COLD = 'cold',
  /** Data exists in both tiers */
  BOTH = 'both',
}

// =============================================================================
// Core Storage Provider Interface
// =============================================================================

/**
 * Unified storage provider interface.
 *
 * This is the canonical interface for all storage backends in DoSQL.
 * It provides a consistent API regardless of whether the underlying
 * storage is Durable Object storage, R2, in-memory, or tiered.
 *
 * Method naming uses get/put as primary (matching Durable Object conventions),
 * with read/write aliases for FSX compatibility.
 *
 * @example
 * ```typescript
 * // Basic usage
 * await provider.put('pages/page_001', pageData);
 * const data = await provider.get('pages/page_001');
 *
 * // Partial reads
 * const header = await provider.get('pages/page_001', [0, 63]);
 *
 * // Listing
 * const pages = await provider.list('pages/');
 *
 * // FSX-style aliases also work
 * await provider.write('path', data);
 * const result = await provider.read('path');
 * ```
 */
export interface StorageProvider {
  // ---------------------------------------------------------------------------
  // Primary Methods (get/put naming - matches DO storage conventions)
  // ---------------------------------------------------------------------------

  /**
   * Read data from storage by key.
   *
   * @param key - The storage key to read
   * @param range - Optional byte range for partial reads [start, end] (inclusive)
   * @returns The data as Uint8Array, or null if not found
   */
  get(key: string, range?: ByteRange): Promise<Uint8Array | null>;

  /**
   * Write data to storage.
   *
   * @param key - The storage key to write
   * @param data - The data to write
   */
  put(key: string, data: Uint8Array): Promise<void>;

  /**
   * Delete data from storage.
   *
   * @param key - The storage key to delete
   */
  delete(key: string): Promise<void>;

  /**
   * List keys matching a prefix.
   *
   * @param prefix - The prefix to match
   * @returns Array of matching keys, sorted alphabetically
   */
  list(prefix: string): Promise<string[]>;

  /**
   * Check if a key exists in storage.
   *
   * @param key - The storage key to check
   * @returns True if the key exists
   */
  exists(key: string): Promise<boolean>;

  // ---------------------------------------------------------------------------
  // Alias Methods (read/write naming - for FSX compatibility)
  // ---------------------------------------------------------------------------

  /**
   * Alias for get() - for FSX compatibility.
   * @see get
   */
  read(key: string, range?: ByteRange): Promise<Uint8Array | null>;

  /**
   * Alias for put() - for FSX compatibility.
   * @see put
   */
  write(key: string, data: Uint8Array): Promise<void>;
}

// =============================================================================
// Extended Capabilities (Optional)
// =============================================================================

/**
 * Metadata about a stored value.
 */
export interface StorageMetadata {
  /** Size in bytes */
  size: number;
  /** Last modification timestamp */
  lastModified: Date;
  /** Optional content hash/etag */
  etag?: string;
  /** Custom metadata */
  custom?: Record<string, string>;
}

/**
 * Storage provider with metadata support.
 *
 * Some backends (like R2) can provide metadata without reading
 * the full value, enabling optimizations for large files.
 */
export interface StorageProviderWithMeta extends StorageProvider {
  /**
   * Get metadata for a key without reading its contents.
   *
   * @param key - The storage key
   * @returns Metadata or null if not found
   */
  metadata(key: string): Promise<StorageMetadata | null>;
}

/**
 * Batch operation request for put operations.
 */
export interface BatchPutRequest {
  key: string;
  data: Uint8Array;
}

/**
 * Storage provider with batch operation support.
 *
 * Batch operations can significantly improve performance when
 * writing or deleting multiple items, as they can be executed
 * in a single transaction or network round-trip.
 */
export interface StorageProviderWithBatch extends StorageProvider {
  /**
   * Put multiple items in a single operation.
   *
   * @param items - Array of key-data pairs to write
   */
  putMany(items: BatchPutRequest[]): Promise<void>;

  /**
   * Delete multiple items in a single operation.
   *
   * @param keys - Array of keys to delete
   */
  deleteMany(keys: string[]): Promise<void>;

  /**
   * Get multiple items in a single operation.
   *
   * @param keys - Array of keys to read
   * @returns Map of key to data (missing keys are not included)
   */
  getMany(keys: string[]): Promise<Map<string, Uint8Array>>;
}

/**
 * Options for tiered storage operations.
 */
export interface TieredStorageOptions {
  /** Force operation to specific tier */
  tier?: StorageTier.HOT | StorageTier.COLD;
  /** Skip cache lookup for reads */
  skipCache?: boolean;
}

/**
 * Extended metadata for tiered storage.
 */
export interface TieredMetadata extends StorageMetadata {
  /** Which storage tier(s) contain the data */
  tier: StorageTier;
  /** When data was last accessed */
  lastAccessed?: Date;
  /** When data was migrated to cold storage */
  migratedAt?: Date;
}

/**
 * Storage provider with tiered storage support.
 *
 * Tiered providers manage data across hot (DO) and cold (R2) storage,
 * with automatic migration and caching.
 */
export interface StorageProviderWithTiers extends StorageProviderWithMeta {
  /**
   * Get extended metadata including tier information.
   */
  tieredMetadata(key: string): Promise<TieredMetadata | null>;

  /**
   * Get data with tier-specific options.
   */
  getFromTier(key: string, options: TieredStorageOptions): Promise<Uint8Array | null>;

  /**
   * Put data to a specific tier.
   */
  putToTier(key: string, data: Uint8Array, tier: StorageTier.HOT | StorageTier.COLD): Promise<void>;

  /**
   * Migrate data from hot to cold storage.
   */
  migrateToR2(keys: string[]): Promise<{
    migrated: string[];
    failed: Array<{ key: string; error: string }>;
    bytesTransferred: number;
  }>;

  /**
   * Promote data from cold to hot storage.
   */
  promoteToHot(key: string): Promise<void>;
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if a storage provider supports metadata operations.
 */
export function hasMetadataSupport(
  provider: StorageProvider
): provider is StorageProviderWithMeta {
  return 'metadata' in provider && typeof (provider as StorageProviderWithMeta).metadata === 'function';
}

/**
 * Check if a storage provider supports batch operations.
 */
export function hasBatchSupport(
  provider: StorageProvider
): provider is StorageProviderWithBatch {
  return (
    'putMany' in provider &&
    'deleteMany' in provider &&
    'getMany' in provider &&
    typeof (provider as StorageProviderWithBatch).putMany === 'function' &&
    typeof (provider as StorageProviderWithBatch).deleteMany === 'function' &&
    typeof (provider as StorageProviderWithBatch).getMany === 'function'
  );
}

/**
 * Check if a storage provider supports tiered storage operations.
 */
export function hasTieredSupport(
  provider: StorageProvider
): provider is StorageProviderWithTiers {
  return (
    hasMetadataSupport(provider) &&
    'tieredMetadata' in provider &&
    'getFromTier' in provider &&
    'putToTier' in provider &&
    typeof (provider as StorageProviderWithTiers).tieredMetadata === 'function'
  );
}

// =============================================================================
// Legacy Interface Definitions (for adapters)
// =============================================================================

/**
 * Legacy FSXBackend interface (read/write naming).
 *
 * @deprecated Use StorageProvider instead. This type exists for
 * backward compatibility with existing code using FSXBackend.
 */
export interface LegacyFSXBackend {
  read(path: string, range?: ByteRange): Promise<Uint8Array | null>;
  write(path: string, data: Uint8Array): Promise<void>;
  delete(path: string): Promise<void>;
  list(prefix: string): Promise<string[]>;
  exists(path: string): Promise<boolean>;
}

/**
 * Legacy StorageInterface (get/put naming, no range support).
 *
 * @deprecated Use StorageProvider instead. This type exists for
 * backward compatibility with existing code using StorageInterface.
 */
export interface LegacyStorageInterface {
  get(key: string, range?: ByteRange): Promise<Uint8Array | null>;
  put(key: string, data: Uint8Array): Promise<void>;
  delete(key: string): Promise<void>;
  list(prefix: string): Promise<string[]>;
  exists(key: string): Promise<boolean>;
}

// =============================================================================
// Adapters
// =============================================================================

/**
 * Base class for storage provider implementations.
 *
 * Provides the alias methods (read/write) automatically based on
 * the primary methods (get/put). Subclasses only need to implement
 * the primary methods.
 */
export abstract class StorageProviderBase implements StorageProvider {
  // Primary methods - must be implemented by subclasses
  abstract get(key: string, range?: ByteRange): Promise<Uint8Array | null>;
  abstract put(key: string, data: Uint8Array): Promise<void>;
  abstract delete(key: string): Promise<void>;
  abstract list(prefix: string): Promise<string[]>;
  abstract exists(key: string): Promise<boolean>;

  // Alias methods - delegate to primary methods
  read(key: string, range?: ByteRange): Promise<Uint8Array | null> {
    return this.get(key, range);
  }

  write(key: string, data: Uint8Array): Promise<void> {
    return this.put(key, data);
  }
}

/**
 * Adapt a legacy FSXBackend to the unified StorageProvider interface.
 *
 * @param backend - Legacy FSXBackend instance
 * @returns StorageProvider wrapper
 *
 * @example
 * ```typescript
 * const legacyBackend = new DOStorageBackend(state.storage);
 * const provider = adaptFSXBackend(legacyBackend);
 * const btree = createBTree(provider, keyCodec, valueCodec);
 * ```
 */
export function adaptFSXBackend(backend: LegacyFSXBackend): StorageProvider {
  return {
    // Primary methods
    get: (key, range) => backend.read(key, range),
    put: (key, data) => backend.write(key, data),
    delete: (key) => backend.delete(key),
    list: (prefix) => backend.list(prefix),
    exists: (key) => backend.exists(key),
    // Alias methods
    read: (key, range) => backend.read(key, range),
    write: (key, data) => backend.write(key, data),
  };
}

/**
 * Adapt a legacy StorageInterface to the unified StorageProvider interface.
 *
 * @param storage - Legacy StorageInterface instance
 * @returns StorageProvider wrapper
 *
 * @example
 * ```typescript
 * const legacyStorage = new MemoryStorage();
 * const provider = adaptStorageInterface(legacyStorage);
 * ```
 */
export function adaptStorageInterface(storage: LegacyStorageInterface): StorageProvider {
  return {
    // Primary methods
    get: (key, range) => storage.get(key, range),
    put: (key, data) => storage.put(key, data),
    delete: (key) => storage.delete(key),
    list: (prefix) => storage.list(prefix),
    exists: (key) => storage.exists(key),
    // Alias methods
    read: (key, range) => storage.get(key, range),
    write: (key, data) => storage.put(key, data),
  };
}

/**
 * Adapt a StorageProvider to the legacy FSXBackend interface.
 *
 * Use this when you need to pass a StorageProvider to code that
 * expects an FSXBackend.
 *
 * @param provider - StorageProvider instance
 * @returns LegacyFSXBackend wrapper
 */
export function adaptToFSXBackend(provider: StorageProvider): LegacyFSXBackend {
  return {
    read: (path, range) => provider.get(path, range),
    write: (path, data) => provider.put(path, data),
    delete: (path) => provider.delete(path),
    list: (prefix) => provider.list(prefix),
    exists: (path) => provider.exists(path),
  };
}

/**
 * Adapt a StorageProvider to the legacy StorageInterface.
 *
 * Use this when you need to pass a StorageProvider to code that
 * expects a StorageInterface.
 *
 * @param provider - StorageProvider instance
 * @returns LegacyStorageInterface wrapper
 */
export function adaptToStorageInterface(provider: StorageProvider): LegacyStorageInterface {
  return {
    get: (key, range) => provider.get(key, range),
    put: (key, data) => provider.put(key, data),
    delete: (key) => provider.delete(key),
    list: (prefix) => provider.list(prefix),
    exists: (key) => provider.exists(key),
  };
}

// =============================================================================
// Utility: Detect and normalize any storage backend
// =============================================================================

/**
 * Storage backend types that can be normalized to StorageProvider.
 */
export type AnyStorageBackend = StorageProvider | LegacyFSXBackend | LegacyStorageInterface;

/**
 * Check if an object is a StorageProvider (has both get and read methods).
 */
function isStorageProvider(obj: AnyStorageBackend): obj is StorageProvider {
  return (
    'get' in obj &&
    'put' in obj &&
    'read' in obj &&
    'write' in obj &&
    typeof obj.get === 'function' &&
    typeof obj.read === 'function'
  );
}

/**
 * Check if an object is a legacy FSXBackend (has read but not get).
 */
function isFSXBackend(obj: AnyStorageBackend): obj is LegacyFSXBackend {
  return (
    'read' in obj &&
    'write' in obj &&
    typeof obj.read === 'function' &&
    !('get' in obj && typeof (obj as { get?: unknown }).get === 'function')
  );
}

/**
 * Normalize any storage backend to the unified StorageProvider interface.
 *
 * This function accepts any of the storage interfaces used in DoSQL and
 * returns a unified StorageProvider. It automatically detects the interface
 * type and applies the appropriate adapter.
 *
 * @param backend - Any storage backend (StorageProvider, FSXBackend, or StorageInterface)
 * @returns A normalized StorageProvider
 *
 * @example
 * ```typescript
 * // Works with any storage type
 * const provider = normalizeStorage(doBackend);      // FSXBackend
 * const provider = normalizeStorage(memoryStorage);  // StorageInterface
 * const provider = normalizeStorage(existingProvider); // StorageProvider (no-op)
 *
 * // Use consistently
 * const btree = createBTree(provider, keyCodec, valueCodec);
 * ```
 */
export function normalizeStorage(backend: AnyStorageBackend): StorageProvider {
  // Already a StorageProvider
  if (isStorageProvider(backend)) {
    return backend;
  }

  // Legacy FSXBackend (read/write naming)
  if (isFSXBackend(backend)) {
    return adaptFSXBackend(backend);
  }

  // Legacy StorageInterface (get/put naming)
  return adaptStorageInterface(backend as LegacyStorageInterface);
}

// =============================================================================
// In-Memory Implementation for Testing
// =============================================================================

/**
 * Options for creating an in-memory storage provider.
 */
export interface MemoryProviderOptions {
  /** Maximum total size in bytes (default: no limit) */
  maxSize?: number;
  /** Initial data to populate storage with */
  initialData?: Map<string, Uint8Array>;
}

/**
 * Internal storage entry with metadata.
 */
interface MemoryEntry {
  data: Uint8Array;
  createdAt: Date;
  modifiedAt: Date;
}

/**
 * In-memory storage provider for testing and development.
 *
 * Implements all storage capabilities including batch operations and metadata.
 *
 * @example
 * ```typescript
 * const provider = new MemoryProvider();
 * await provider.put('key1', new Uint8Array([1, 2, 3]));
 * const data = await provider.get('key1');
 *
 * // Works with B-tree
 * const btree = createBTree(provider, keyCodec, valueCodec);
 * ```
 */
export class MemoryProvider
  extends StorageProviderBase
  implements StorageProviderWithMeta, StorageProviderWithBatch
{
  private readonly data: Map<string, MemoryEntry> = new Map();
  private readonly options: MemoryProviderOptions;
  private currentSize: number = 0;

  constructor(options: MemoryProviderOptions = {}) {
    super();
    this.options = options;

    // Initialize with provided data
    if (options.initialData) {
      for (const [key, value] of options.initialData) {
        const now = new Date();
        this.data.set(key, {
          data: value,
          createdAt: now,
          modifiedAt: now,
        });
        this.currentSize += value.byteLength;
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Core StorageProvider Methods
  // ---------------------------------------------------------------------------

  async get(key: string, range?: ByteRange): Promise<Uint8Array | null> {
    const entry = this.data.get(key);
    if (!entry) {
      return null;
    }

    if (range) {
      const [start, end] = range;
      if (start < 0 || end < start || start >= entry.data.byteLength) {
        return null;
      }
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

  // ---------------------------------------------------------------------------
  // StorageProviderWithMeta Methods
  // ---------------------------------------------------------------------------

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

  // ---------------------------------------------------------------------------
  // StorageProviderWithBatch Methods
  // ---------------------------------------------------------------------------

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

  // ---------------------------------------------------------------------------
  // Utility Methods
  // ---------------------------------------------------------------------------

  /** Clear all data from storage */
  clear(): void {
    this.data.clear();
    this.currentSize = 0;
  }

  /** Get the current total size of stored data in bytes */
  get size(): number {
    return this.currentSize;
  }

  /** Get the number of keys in storage */
  get count(): number {
    return this.data.size;
  }

  /** Get a snapshot of all keys */
  keys(): string[] {
    return Array.from(this.data.keys());
  }

  /** Export all data as a Map (for testing/debugging) */
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
 * Create an in-memory storage provider for testing.
 *
 * @param options - Configuration options
 * @returns MemoryProvider instance
 *
 * @example
 * ```typescript
 * const provider = createMemoryProvider();
 * const btree = createBTree(provider, keyCodec, valueCodec);
 * ```
 */
export function createMemoryProvider(options?: MemoryProviderOptions): MemoryProvider {
  return new MemoryProvider(options);
}
