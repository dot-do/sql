/**
 * Unified Storage Interface Abstraction
 *
 * Provides a common storage interface that both B-tree (OLTP) and
 * Columnar (OLAP) engines can use. This abstraction enables:
 *
 * - Consistent storage operations across engines
 * - Easy swapping of storage backends
 * - Unified error handling
 * - Type-safe storage operations
 *
 * @packageDocumentation
 */

// =============================================================================
// Core Storage Interface
// =============================================================================

/**
 * Range specification for partial reads
 * [start, end] - both inclusive, in bytes
 */
export type ByteRange = readonly [start: number, end: number];

/**
 * Unified storage interface for DoSQL engines.
 *
 * This interface provides the minimal set of operations required by both
 * the B-tree (row-oriented OLTP) and Columnar (column-oriented OLAP) engines.
 *
 * Method naming follows the pattern used by Cloudflare Durable Objects storage,
 * making it natural to implement with DO storage, R2, or other backends.
 *
 * @example
 * ```typescript
 * // B-tree usage
 * const btree = createBTree(storage, keyCodec, valueCodec);
 *
 * // Columnar usage
 * const reader = new ColumnarReader(storage);
 * const writer = new ColumnarWriter(schema, config, storage);
 * ```
 */
export interface StorageInterface {
  /**
   * Read data from storage by key.
   *
   * @param key - The storage key to read
   * @param range - Optional byte range for partial reads [start, end] (inclusive)
   * @returns The data as Uint8Array, or null if not found
   *
   * @example
   * ```typescript
   * // Read entire value
   * const data = await storage.get('pages/page_00000001');
   *
   * // Read partial value (bytes 0-1023)
   * const header = await storage.get('pages/page_00000001', [0, 1023]);
   * ```
   */
  get(key: string, range?: ByteRange): Promise<Uint8Array | null>;

  /**
   * Write data to storage.
   *
   * @param key - The storage key to write
   * @param data - The data to write
   *
   * @example
   * ```typescript
   * const pageData = serializePage(page);
   * await storage.put('pages/page_00000001', pageData);
   * ```
   */
  put(key: string, data: Uint8Array): Promise<void>;

  /**
   * Delete data from storage.
   *
   * @param key - The storage key to delete
   *
   * @example
   * ```typescript
   * await storage.delete('pages/page_00000001');
   * ```
   */
  delete(key: string): Promise<void>;

  /**
   * List keys matching a prefix.
   *
   * @param prefix - The prefix to match
   * @returns Array of matching keys
   *
   * @example
   * ```typescript
   * const pageKeys = await storage.list('pages/');
   * const rowGroupKeys = await storage.list('sales/rowgroups/');
   * ```
   */
  list(prefix: string): Promise<string[]>;

  /**
   * Check if a key exists in storage.
   *
   * @param key - The storage key to check
   * @returns True if the key exists
   *
   * @example
   * ```typescript
   * if (await storage.exists('metadata/_meta')) {
   *   // Load existing metadata
   * }
   * ```
   */
  exists(key: string): Promise<boolean>;
}

// =============================================================================
// Extended Storage Interface (Optional Capabilities)
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
 * Extended storage interface with metadata support.
 *
 * Some storage backends (like R2) can provide metadata without
 * reading the full value, enabling optimizations for large files.
 */
export interface StorageInterfaceWithMeta extends StorageInterface {
  /**
   * Get metadata for a key without reading its contents.
   *
   * @param key - The storage key
   * @returns Metadata or null if not found
   */
  metadata(key: string): Promise<StorageMetadata | null>;
}

// =============================================================================
// Batch Operations Interface (Optional)
// =============================================================================

/**
 * Batch operation request for put operations.
 */
export interface BatchPutRequest {
  key: string;
  data: Uint8Array;
}

/**
 * Storage interface with batch operation support.
 *
 * Batch operations can significantly improve performance when
 * writing or deleting multiple items, as they can be executed
 * in a single transaction or network round-trip.
 */
export interface StorageInterfaceWithBatch extends StorageInterface {
  /**
   * Put multiple items in a single operation.
   *
   * @param items - Array of key-data pairs to write
   *
   * @example
   * ```typescript
   * await storage.putMany([
   *   { key: 'page_1', data: pageData1 },
   *   { key: 'page_2', data: pageData2 },
   * ]);
   * ```
   */
  putMany(items: BatchPutRequest[]): Promise<void>;

  /**
   * Delete multiple items in a single operation.
   *
   * @param keys - Array of keys to delete
   *
   * @example
   * ```typescript
   * await storage.deleteMany(['orphan_1', 'orphan_2', 'orphan_3']);
   * ```
   */
  deleteMany(keys: string[]): Promise<void>;

  /**
   * Get multiple items in a single operation.
   *
   * @param keys - Array of keys to read
   * @returns Map of key to data (missing keys are not included)
   *
   * @example
   * ```typescript
   * const results = await storage.getMany(['page_1', 'page_2']);
   * const page1 = results.get('page_1');
   * ```
   */
  getMany(keys: string[]): Promise<Map<string, Uint8Array>>;
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if a storage interface supports metadata operations.
 */
export function hasMetadata(
  storage: StorageInterface
): storage is StorageInterfaceWithMeta {
  return 'metadata' in storage && typeof storage.metadata === 'function';
}

/**
 * Check if a storage interface supports batch operations.
 */
export function hasBatch(
  storage: StorageInterface
): storage is StorageInterfaceWithBatch {
  return (
    'putMany' in storage &&
    'deleteMany' in storage &&
    'getMany' in storage &&
    typeof storage.putMany === 'function' &&
    typeof storage.deleteMany === 'function' &&
    typeof storage.getMany === 'function'
  );
}

// =============================================================================
// Adapter Types (for backward compatibility)
// =============================================================================

/**
 * Legacy FSXBackend interface (for backward compatibility with B-tree).
 *
 * @deprecated Use StorageInterface instead
 */
export interface LegacyFSXBackend {
  read(path: string, range?: ByteRange): Promise<Uint8Array | null>;
  write(path: string, data: Uint8Array): Promise<void>;
  delete(path: string): Promise<void>;
  list(prefix: string): Promise<string[]>;
  exists(path: string): Promise<boolean>;
}

/**
 * Legacy FSXInterface (for backward compatibility with Columnar).
 *
 * @deprecated Use StorageInterface instead
 */
export interface LegacyColumnarInterface {
  get(key: string): Promise<Uint8Array | null>;
  put(key: string, data: Uint8Array): Promise<void>;
  delete(key: string): Promise<void>;
  list(prefix: string): Promise<string[]>;
}

// =============================================================================
// Adapters
// =============================================================================

/**
 * Adapt a legacy FSXBackend to the new StorageInterface.
 *
 * @param backend - Legacy FSXBackend instance
 * @returns StorageInterface wrapper
 *
 * @example
 * ```typescript
 * const legacyBackend = new DOBackend(storage);
 * const storage = adaptFSXBackend(legacyBackend);
 * const btree = createBTree(storage, keyCodec, valueCodec);
 * ```
 */
export function adaptFSXBackend(backend: LegacyFSXBackend): StorageInterface {
  return {
    get: (key, range) => backend.read(key, range),
    put: (key, data) => backend.write(key, data),
    delete: (key) => backend.delete(key),
    list: (prefix) => backend.list(prefix),
    exists: (key) => backend.exists(key),
  };
}

/**
 * Adapt a StorageInterface to the legacy FSXBackend interface.
 *
 * @param storage - StorageInterface instance
 * @returns LegacyFSXBackend wrapper
 *
 * @example
 * ```typescript
 * const storage = new ModernStorage();
 * const legacyBackend = adaptToFSXBackend(storage);
 * // Use with code expecting FSXBackend
 * ```
 */
export function adaptToFSXBackend(storage: StorageInterface): LegacyFSXBackend {
  return {
    read: (path, range) => storage.get(path, range),
    write: (path, data) => storage.put(path, data),
    delete: (path) => storage.delete(path),
    list: (prefix) => storage.list(prefix),
    exists: (path) => storage.exists(path),
  };
}

/**
 * Adapt a legacy Columnar FSXInterface to the new StorageInterface.
 *
 * @param fsx - Legacy FSXInterface instance
 * @returns StorageInterface wrapper
 */
export function adaptColumnarInterface(
  fsx: LegacyColumnarInterface
): StorageInterface {
  return {
    get: (key) => fsx.get(key),
    put: (key, data) => fsx.put(key, data),
    delete: (key) => fsx.delete(key),
    list: (prefix) => fsx.list(prefix),
    // Columnar FSXInterface doesn't have exists, so we implement it
    exists: async (key) => (await fsx.get(key)) !== null,
  };
}
