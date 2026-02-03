/**
 * Unified Page Storage Interface
 *
 * Provides a unified interface for page-based storage operations that both
 * B-tree and columnar storage engines can use. This abstraction enables:
 *
 * - Consistent page read/write operations across storage backends
 * - Easy swapping between DO storage, R2, and memory backends
 * - Optimized batch operations for page management
 * - Configuration-driven storage backend selection
 *
 * @packageDocumentation
 */

import type { StorageInterface, ByteRange } from './interface.js';
import type { StorageProvider } from './provider.js';
import type { FSXBackend } from '../fsx/types.js';

// =============================================================================
// Core Page Storage Interface
// =============================================================================

/**
 * Page identifier type (numeric page IDs used by B-tree)
 */
export type PageId = number;

/**
 * Storage key type (string paths used by storage backends)
 */
export type StorageKey = string;

/**
 * Unified page storage interface optimized for B-tree page operations.
 *
 * This interface provides the essential operations needed by page-based
 * storage engines like B-tree, with methods designed to work efficiently
 * with both Durable Object storage and R2.
 *
 * @example
 * ```typescript
 * // Create page storage from FSX backend
 * const pageStorage = createPageStorage(doBackend, { pagePrefix: 'btree/' });
 *
 * // Use with B-tree
 * const btree = new BTreeImpl(pageStorage, keyCodec, valueCodec);
 * await btree.init();
 * ```
 */
export interface PageStorage {
  /**
   * Read a page by key.
   *
   * @param key - The storage key (typically includes page prefix and ID)
   * @returns The page data as Uint8Array, or null if not found
   *
   * @example
   * ```typescript
   * const pageData = await storage.readPage('btree/pages/page_00000001');
   * ```
   */
  readPage(key: StorageKey): Promise<Uint8Array | null>;

  /**
   * Write a page to storage.
   *
   * @param key - The storage key
   * @param data - The serialized page data
   *
   * @example
   * ```typescript
   * const serialized = serializePage(page);
   * await storage.writePage('btree/pages/page_00000001', serialized);
   * ```
   */
  writePage(key: StorageKey, data: Uint8Array): Promise<void>;

  /**
   * Delete a page from storage.
   *
   * @param key - The storage key to delete
   *
   * @example
   * ```typescript
   * await storage.deletePage('btree/pages/page_00000001');
   * ```
   */
  deletePage(key: StorageKey): Promise<void>;

  /**
   * List all pages matching a prefix.
   *
   * @param prefix - The key prefix to match
   * @returns Array of matching keys
   *
   * @example
   * ```typescript
   * const pageKeys = await storage.listPages('btree/pages/');
   * ```
   */
  listPages(prefix: string): Promise<StorageKey[]>;

  /**
   * Check if a page exists.
   *
   * @param key - The storage key to check
   * @returns True if the page exists
   */
  exists(key: StorageKey): Promise<boolean>;
}

// =============================================================================
// Extended Page Storage with Batch Operations
// =============================================================================

/**
 * Batch write request for multiple pages.
 */
export interface BatchPageWrite {
  key: StorageKey;
  data: Uint8Array;
}

/**
 * Extended page storage interface with batch operation support.
 *
 * Batch operations can significantly improve performance when the underlying
 * storage backend supports transactional writes (like DO storage).
 */
export interface PageStorageWithBatch extends PageStorage {
  /**
   * Write multiple pages in a single batch operation.
   *
   * @param pages - Array of key-data pairs to write
   *
   * @example
   * ```typescript
   * await storage.writePages([
   *   { key: 'btree/pages/page_00000001', data: page1Data },
   *   { key: 'btree/pages/page_00000002', data: page2Data },
   * ]);
   * ```
   */
  writePages(pages: BatchPageWrite[]): Promise<void>;

  /**
   * Delete multiple pages in a single batch operation.
   *
   * @param keys - Array of keys to delete
   *
   * @example
   * ```typescript
   * await storage.deletePages(['btree/pages/page_00000001', 'btree/pages/page_00000002']);
   * ```
   */
  deletePages(keys: StorageKey[]): Promise<void>;

  /**
   * Read multiple pages in a single batch operation.
   *
   * @param keys - Array of keys to read
   * @returns Map of key to data (missing keys are not included)
   *
   * @example
   * ```typescript
   * const pages = await storage.readPages(['btree/pages/page_00000001', 'btree/pages/page_00000002']);
   * const page1 = pages.get('btree/pages/page_00000001');
   * ```
   */
  readPages(keys: StorageKey[]): Promise<Map<StorageKey, Uint8Array>>;
}

// =============================================================================
// Storage Backend Configuration
// =============================================================================

/**
 * Storage backend type enumeration.
 */
export enum StorageBackendType {
  /** Durable Object storage - low latency, limited size */
  DO_STORAGE = 'do_storage',
  /** R2 object storage - higher latency, unlimited size */
  R2 = 'r2',
  /** Tiered storage - DO for hot data, R2 for cold data */
  TIERED = 'tiered',
  /** In-memory storage - for testing */
  MEMORY = 'memory',
}

/**
 * Configuration for page storage.
 */
export interface PageStorageConfig {
  /**
   * Storage backend type to use.
   * @default StorageBackendType.MEMORY
   */
  backendType: StorageBackendType;

  /**
   * Prefix for all page storage keys.
   * @default 'pages/'
   */
  pagePrefix: string;

  /**
   * Maximum page size in bytes (DO storage limit is 2MB).
   * @default 2097152 (2MB)
   */
  maxPageSize: number;

  /**
   * Enable batch operations when supported by the backend.
   * @default true
   */
  enableBatch: boolean;

  /**
   * Cache configuration for frequently accessed pages.
   */
  cache?: {
    /** Maximum number of pages to cache */
    maxPages: number;
    /** Maximum cache size in bytes */
    maxBytes?: number;
  };
}

/**
 * Default page storage configuration.
 */
export const DEFAULT_PAGE_STORAGE_CONFIG: Readonly<PageStorageConfig> = {
  backendType: StorageBackendType.MEMORY,
  pagePrefix: 'pages/',
  maxPageSize: 2 * 1024 * 1024, // 2MB - DO storage limit
  enableBatch: true,
};

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if a page storage supports batch operations.
 */
export function hasBatchPageOperations(
  storage: PageStorage
): storage is PageStorageWithBatch {
  return (
    'writePages' in storage &&
    'deletePages' in storage &&
    'readPages' in storage &&
    typeof (storage as PageStorageWithBatch).writePages === 'function' &&
    typeof (storage as PageStorageWithBatch).deletePages === 'function' &&
    typeof (storage as PageStorageWithBatch).readPages === 'function'
  );
}

/**
 * Check if storage is a StorageProvider (has both get/read and put/write).
 */
export function isStorageProvider(
  storage: unknown
): storage is StorageProvider {
  return (
    storage !== null &&
    typeof storage === 'object' &&
    'get' in storage &&
    'put' in storage &&
    'read' in storage &&
    'write' in storage &&
    typeof (storage as StorageProvider).get === 'function' &&
    typeof (storage as StorageProvider).read === 'function'
  );
}

/**
 * Check if storage is a StorageInterface (uses get/put).
 */
export function isStorageInterface(
  storage: unknown
): storage is StorageInterface {
  return (
    storage !== null &&
    typeof storage === 'object' &&
    'get' in storage &&
    'put' in storage &&
    typeof (storage as StorageInterface).get === 'function' &&
    typeof (storage as StorageInterface).put === 'function'
  );
}

/**
 * Check if storage is an FSXBackend (uses read/write but not get).
 */
export function isFSXBackend(storage: unknown): storage is FSXBackend {
  return (
    storage !== null &&
    typeof storage === 'object' &&
    'read' in storage &&
    'write' in storage &&
    typeof (storage as FSXBackend).read === 'function' &&
    typeof (storage as FSXBackend).write === 'function' &&
    !('get' in storage && typeof (storage as { get?: unknown }).get === 'function')
  );
}

// =============================================================================
// Adapters
// =============================================================================

/**
 * Adapter that wraps a StorageInterface as PageStorage.
 */
class StorageInterfacePageAdapter implements PageStorage {
  constructor(private readonly storage: StorageInterface) {}

  async readPage(key: StorageKey): Promise<Uint8Array | null> {
    return this.storage.get(key);
  }

  async writePage(key: StorageKey, data: Uint8Array): Promise<void> {
    return this.storage.put(key, data);
  }

  async deletePage(key: StorageKey): Promise<void> {
    return this.storage.delete(key);
  }

  async listPages(prefix: string): Promise<StorageKey[]> {
    return this.storage.list(prefix);
  }

  async exists(key: StorageKey): Promise<boolean> {
    return this.storage.exists(key);
  }
}

/**
 * Adapter that wraps an FSXBackend as PageStorage.
 */
class FSXBackendPageAdapter implements PageStorage {
  constructor(private readonly fsx: FSXBackend) {}

  async readPage(key: StorageKey): Promise<Uint8Array | null> {
    return this.fsx.read(key);
  }

  async writePage(key: StorageKey, data: Uint8Array): Promise<void> {
    return this.fsx.write(key, data);
  }

  async deletePage(key: StorageKey): Promise<void> {
    return this.fsx.delete(key);
  }

  async listPages(prefix: string): Promise<StorageKey[]> {
    return this.fsx.list(prefix);
  }

  async exists(key: StorageKey): Promise<boolean> {
    return this.fsx.exists(key);
  }
}

/**
 * Adapter that wraps a PageStorage as StorageInterface (reverse adapter).
 */
class PageStorageToStorageInterfaceAdapter implements StorageInterface {
  constructor(private readonly pageStorage: PageStorage) {}

  async get(key: string, _range?: ByteRange): Promise<Uint8Array | null> {
    // Note: PageStorage doesn't support range reads, so we read the full page
    return this.pageStorage.readPage(key);
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    return this.pageStorage.writePage(key, data);
  }

  async delete(key: string): Promise<void> {
    return this.pageStorage.deletePage(key);
  }

  async list(prefix: string): Promise<string[]> {
    return this.pageStorage.listPages(prefix);
  }

  async exists(key: string): Promise<boolean> {
    return this.pageStorage.exists(key);
  }
}

/**
 * Adapter that wraps a PageStorage as FSXBackend (reverse adapter).
 */
class PageStorageToFSXBackendAdapter implements FSXBackend {
  constructor(private readonly pageStorage: PageStorage) {}

  async read(path: string, _range?: ByteRange): Promise<Uint8Array | null> {
    // Note: PageStorage doesn't support range reads, so we read the full page
    return this.pageStorage.readPage(path);
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    return this.pageStorage.writePage(path, data);
  }

  async delete(path: string): Promise<void> {
    return this.pageStorage.deletePage(path);
  }

  async list(prefix: string): Promise<string[]> {
    return this.pageStorage.listPages(prefix);
  }

  async exists(path: string): Promise<boolean> {
    return this.pageStorage.exists(path);
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Backend type that can be converted to PageStorage.
 * Supports all storage interfaces used in DoSQL:
 * - StorageProvider (unified interface with both get/put and read/write)
 * - StorageInterface (get/put naming)
 * - FSXBackend (read/write naming)
 * - PageStorage (already page-optimized)
 */
export type AnyStorageBackend = StorageProvider | StorageInterface | FSXBackend | PageStorage;

/**
 * Create a PageStorage from any supported storage backend.
 *
 * This function automatically detects the backend type and creates
 * the appropriate adapter.
 *
 * @param backend - Storage backend (StorageInterface, FSXBackend, or PageStorage)
 * @param _config - Optional configuration (reserved for future use)
 * @returns PageStorage instance
 *
 * @example
 * ```typescript
 * // From StorageInterface
 * const storage1 = createPageStorage(memoryStorage);
 *
 * // From FSXBackend
 * const storage2 = createPageStorage(doBackend);
 *
 * // Pass-through for PageStorage
 * const storage3 = createPageStorage(existingPageStorage);
 * ```
 */
export function createPageStorage(
  backend: AnyStorageBackend,
  _config?: Partial<PageStorageConfig>
): PageStorage {
  // If already a PageStorage, return as-is
  if (isPageStorage(backend)) {
    return backend;
  }

  // Adapt StorageProvider (has both get/put and read/write)
  if (isStorageProvider(backend)) {
    return new StorageInterfacePageAdapter(backend);
  }

  // Adapt StorageInterface (get/put only)
  if (isStorageInterface(backend)) {
    return new StorageInterfacePageAdapter(backend);
  }

  // Adapt FSXBackend (read/write only)
  if (isFSXBackend(backend)) {
    return new FSXBackendPageAdapter(backend);
  }

  throw new Error(
    'Unsupported storage backend type. Expected StorageProvider, StorageInterface, FSXBackend, or PageStorage.'
  );
}

/**
 * Check if storage is already a PageStorage.
 */
function isPageStorage(storage: unknown): storage is PageStorage {
  return (
    storage !== null &&
    typeof storage === 'object' &&
    'readPage' in storage &&
    'writePage' in storage &&
    'deletePage' in storage &&
    'listPages' in storage &&
    typeof (storage as PageStorage).readPage === 'function' &&
    typeof (storage as PageStorage).writePage === 'function' &&
    typeof (storage as PageStorage).deletePage === 'function' &&
    typeof (storage as PageStorage).listPages === 'function'
  );
}

/**
 * Convert a PageStorage back to StorageInterface.
 *
 * Useful when you need to pass a PageStorage to code that expects
 * a StorageInterface.
 *
 * @param pageStorage - PageStorage instance
 * @returns StorageInterface adapter
 */
export function pageStorageToStorageInterface(
  pageStorage: PageStorage
): StorageInterface {
  return new PageStorageToStorageInterfaceAdapter(pageStorage);
}

/**
 * Convert a PageStorage back to FSXBackend.
 *
 * Useful when you need to pass a PageStorage to code that expects
 * an FSXBackend.
 *
 * @param pageStorage - PageStorage instance
 * @returns FSXBackend adapter
 */
export function pageStorageToFSXBackend(pageStorage: PageStorage): FSXBackend {
  return new PageStorageToFSXBackendAdapter(pageStorage);
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Generate a page storage key from a page ID.
 *
 * @param pageId - Numeric page ID
 * @param prefix - Key prefix (default: 'pages/')
 * @returns Storage key string
 *
 * @example
 * ```typescript
 * const key = pageIdToKey(1, 'btree/pages/');
 * // Returns: 'btree/pages/page_00000001'
 * ```
 */
export function pageIdToKey(pageId: PageId, prefix: string = 'pages/'): StorageKey {
  return `${prefix}page_${pageId.toString(16).padStart(8, '0')}`;
}

/**
 * Extract a page ID from a storage key.
 *
 * @param key - Storage key string
 * @returns Page ID or null if key doesn't match expected format
 *
 * @example
 * ```typescript
 * const pageId = keyToPageId('btree/pages/page_00000001');
 * // Returns: 1
 * ```
 */
export function keyToPageId(key: StorageKey): PageId | null {
  const match = key.match(/page_([0-9a-f]{8})$/);
  if (!match || !match[1]) {
    return null;
  }
  return parseInt(match[1], 16);
}

/**
 * Normalize storage backend method names for internal use.
 *
 * This is a helper for code that needs to work with both StorageInterface
 * and FSXBackend without caring about the method name differences.
 *
 * @param storage - Any supported storage backend
 * @returns Normalized accessor object
 */
export function normalizeStorageBackend(storage: AnyStorageBackend): {
  read: (key: string) => Promise<Uint8Array | null>;
  write: (key: string, data: Uint8Array) => Promise<void>;
  delete: (key: string) => Promise<void>;
  list: (prefix: string) => Promise<string[]>;
  exists: (key: string) => Promise<boolean>;
} {
  if (isPageStorage(storage)) {
    return {
      read: (key) => storage.readPage(key),
      write: (key, data) => storage.writePage(key, data),
      delete: (key) => storage.deletePage(key),
      list: (prefix) => storage.listPages(prefix),
      exists: (key) => storage.exists(key),
    };
  }

  // StorageProvider has both get/put and read/write
  if (isStorageProvider(storage)) {
    return {
      read: (key) => storage.get(key),
      write: (key, data) => storage.put(key, data),
      delete: (key) => storage.delete(key),
      list: (prefix) => storage.list(prefix),
      exists: (key) => storage.exists(key),
    };
  }

  if (isStorageInterface(storage)) {
    return {
      read: (key) => storage.get(key),
      write: (key, data) => storage.put(key, data),
      delete: (key) => storage.delete(key),
      list: (prefix) => storage.list(prefix),
      exists: (key) => storage.exists(key),
    };
  }

  if (isFSXBackend(storage)) {
    return {
      read: (key) => storage.read(key),
      write: (key, data) => storage.write(key, data),
      delete: (key) => storage.delete(key),
      list: (prefix) => storage.list(prefix),
      exists: (key) => storage.exists(key),
    };
  }

  throw new Error('Unsupported storage backend type');
}
