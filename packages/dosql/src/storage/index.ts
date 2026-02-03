/**
 * DoSQL Storage Abstraction Layer
 *
 * Provides a unified storage interface for both B-tree (OLTP) and
 * Columnar (OLAP) engines. This module enables:
 *
 * - Consistent storage operations across different engine types
 * - Easy swapping between storage backends (DO storage, R2, memory)
 * - Backward compatibility with existing FSXBackend interfaces
 * - Type-safe storage operations
 *
 * @example B-tree with unified storage
 * ```typescript
 * import { createMemoryStorage } from 'dosql/storage';
 * import { createBTree, StringKeyCodec, JsonValueCodec } from 'dosql/btree';
 *
 * const storage = createMemoryStorage();
 * const btree = createBTree(storage, StringKeyCodec, JsonValueCodec);
 * await btree.init();
 * await btree.set('key', { value: 123 });
 * ```
 *
 * @example Columnar with unified storage
 * ```typescript
 * import { createMemoryStorage } from 'dosql/storage';
 * import { ColumnarWriter, ColumnarReader } from 'dosql/columnar';
 *
 * const storage = createMemoryStorage();
 * const writer = new ColumnarWriter(schema, config, storage);
 * const reader = new ColumnarReader(storage);
 * ```
 *
 * @example Adapting legacy backends
 * ```typescript
 * import { adaptFSXBackend } from 'dosql/storage';
 * import { DOBackend } from 'dosql/fsx';
 *
 * const doBackend = new DOBackend(state.storage);
 * const storage = adaptFSXBackend(doBackend);
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// Core Interface
// =============================================================================

export {
  // Types
  type ByteRange,
  type StorageInterface,
  type StorageMetadata,
  type StorageInterfaceWithMeta,
  type StorageInterfaceWithBatch,
  type BatchPutRequest,

  // Legacy types (for backward compatibility)
  type LegacyFSXBackend,
  type LegacyColumnarInterface,

  // Type guards
  hasMetadata,
  hasBatch,

  // Adapters
  adaptFSXBackend,
  adaptToFSXBackend,
  adaptColumnarInterface,
} from './interface.js';

// =============================================================================
// In-Memory Implementation
// =============================================================================

export {
  MemoryStorage,
  createMemoryStorage,
  type MemoryStorageOptions,
} from './memory.js';

// =============================================================================
// File Backend (for Node.js environments)
// =============================================================================

export {
  FileBackedDatabase,
  createFileBackedDatabase,
  openDatabase,
  type FileBackedDatabaseOptions,
  type ExportOptions,
  type ImportOptions,
  type DatabaseFileStats,
  type JournalMode,
  type SynchronousMode,
  type CheckpointMode,
} from './file-backend.js';

// =============================================================================
// Unified Page Storage Interface
// =============================================================================

export {
  // Core types
  type PageId,
  type StorageKey,
  type PageStorage,
  type PageStorageWithBatch,
  type BatchPageWrite,
  type AnyStorageBackend,

  // Configuration
  type PageStorageConfig,
  StorageBackendType,
  DEFAULT_PAGE_STORAGE_CONFIG,

  // Type guards
  hasBatchPageOperations,
  isStorageProvider,
  isStorageInterface,
  isFSXBackend,

  // Factory functions
  createPageStorage,
  pageStorageToStorageInterface,
  pageStorageToFSXBackend,

  // Utility functions
  pageIdToKey,
  keyToPageId,
  normalizeStorageBackend,
} from './page-storage.js';

// =============================================================================
// Unified StorageProvider Interface (Recommended)
// =============================================================================

export {
  // Core provider types
  type StorageProvider,
  type StorageProviderWithMeta,
  type StorageProviderWithBatch,
  type StorageProviderWithTiers,
  type TieredStorageOptions,
  type TieredMetadata,
  StorageTier,

  // Type guards
  hasMetadataSupport,
  hasBatchSupport,
  hasTieredSupport,

  // Base class for implementations
  StorageProviderBase,

  // Legacy interface types (for adapters)
  type LegacyStorageInterface,

  // Adapters
  adaptStorageInterface,
  adaptToStorageInterface,
  normalizeStorage,

  // In-memory implementation
  MemoryProvider,
  createMemoryProvider,
  type MemoryProviderOptions,
} from './provider.js';
