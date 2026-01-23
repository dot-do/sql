/**
 * FSX - File System Abstraction for DoSQL
 *
 * A storage abstraction layer enabling SQLite-like database operations
 * on Cloudflare Workers using:
 * - Durable Object storage for hot/recent data (2MB blob chunks)
 * - R2 for cold/archival storage
 *
 * @example
 * ```typescript
 * import { createDOBackend, createR2Backend, createTieredBackend } from 'dosql/fsx';
 *
 * // In a Durable Object
 * export class MyDO {
 *   private fsx: TieredStorageBackend;
 *
 *   constructor(state: DurableObjectState, env: Env) {
 *     const hot = createDOBackend(state.storage);
 *     const cold = createR2Backend(env.MY_BUCKET);
 *     this.fsx = createTieredBackend(hot, cold);
 *   }
 *
 *   async writeData(path: string, data: Uint8Array) {
 *     await this.fsx.write(path, data);
 *   }
 *
 *   async readData(path: string): Promise<Uint8Array | null> {
 *     return this.fsx.read(path);
 *   }
 * }
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// Core Types
// =============================================================================

export {
  // Core interface
  type FSXBackend,
  type FSXBackendWithMeta,
  type FSXMetadata,
  type ByteRange,

  // Chunk configuration
  type ChunkConfig,
  DEFAULT_CHUNK_CONFIG,

  // Tiered storage configuration
  type TieredStorageConfig,
  type TieredMetadata,
  DEFAULT_TIERED_CONFIG,
  StorageTier,

  // Operation options
  type WriteOptions,
  type ReadOptions,
  type MigrationResult,

  // Error handling
  FSXError,
  FSXErrorCode,
} from './types.js';

// =============================================================================
// DO Storage Backend
// =============================================================================

export {
  DOStorageBackend,
  createDOBackend,
  type DurableObjectStorageLike,
  type DOListOptions,
} from './do-backend.js';

// =============================================================================
// R2 Storage Backend
// =============================================================================

export {
  R2StorageBackend,
  createR2Backend,
  type R2BackendConfig,
  type R2BucketLike,
  type R2GetOptions,
  type R2PutOptions,
  type R2ListOptions,
  type R2ObjectLike,
  type R2ObjectsLike,
} from './r2-backend.js';

// =============================================================================
// R2 Error Types
// =============================================================================

export {
  R2Error,
  R2ErrorCode,
  createR2Error,
  detectR2ErrorType,
  formatR2ErrorForLog,
} from './r2-errors.js';

// =============================================================================
// R2 Caching Layer
// =============================================================================

export {
  R2Cache,
  CachingBackend,
  withCache,
  type CacheConfig,
  type CacheStats,
  DEFAULT_CACHE_CONFIG,
} from './r2-cache.js';

// =============================================================================
// Tiered Storage Backend
// =============================================================================

export {
  TieredStorageBackend,
  createTieredBackend,
  type TieredStorageStats,
} from './tiered.js';

// =============================================================================
// Tier Migration Module
// =============================================================================

export {
  // Migration types
  type TierIndexEntry,
  type MigrationHistoryEntry,
  type MigrateToR2Options,
  type PromoteToHotOptions,
  type MigrationPolicy,
  type HotBackendWithIndex,
  type ColdBackendWithBatch,

  // Migration classes
  TierMigrator,
  MigrationProgressTracker,

  // Factory functions
  createTierMigrator,
  createProgressTracker,
} from './tier-migration.js';

// =============================================================================
// Copy-on-Write (COW) Backend
// =============================================================================

export {
  // Main backend
  COWBackend,
  createCOWBackend,
  type COWBackendConfig,
} from './cow-backend.js';

export {
  // COW types
  type FSXWithCOW,
  type BlobRef,
  type BlobRefType,
  type StoredBlob,
  type Branch,
  type BranchOptions,
  type SnapshotId,
  type Snapshot,
  type SnapshotManifest,
  type ManifestEntry,
  type MergeStrategy,
  type MergeResult,
  type MergeConflict,
  type BranchDiff,
  type GCResult,
  type GCOptions,

  // COW errors
  COWError,
  COWErrorCode,

  // COW utilities
  parseSnapshotId,
  makeSnapshotId,
  DEFAULT_BRANCH,
  MAX_REF_DEPTH,
  BRANCH_PREFIX,
  SNAPSHOT_PREFIX,
  REF_PREFIX,
  BLOB_PREFIX,
} from './cow-types.js';

export {
  // Snapshot management
  SnapshotManager,
  createEmptyManifest,
  buildManifest,
  mergeManifests,
  filterManifest,
} from './snapshot.js';

export {
  // Merge operations
  MergeEngine,
  previewMerge,
  canFastForward,
  fastForwardMerge,
} from './merge.js';

export {
  // Garbage collection
  GarbageCollector,
  formatBytes,
  formatGCResult,
  suggestGCSchedule,
} from './gc.js';

// =============================================================================
// Memory Backend for Testing
// =============================================================================

import {
  type FSXBackend,
  type FSXBackendWithMeta,
  type FSXMetadata,
  type ByteRange,
  FSXError,
  FSXErrorCode,
} from './types.js';

/**
 * In-memory storage backend for testing
 * Implements the full FSXBackendWithMeta interface
 */
export class MemoryFSXBackend implements FSXBackendWithMeta {
  private storage = new Map<string, { data: Uint8Array; createdAt: Date }>();

  async read(path: string, range?: ByteRange): Promise<Uint8Array | null> {
    const entry = this.storage.get(path);
    if (!entry) return null;

    if (range) {
      const [start, end] = range;
      if (start < 0 || end >= entry.data.length || start > end) {
        throw new FSXError(
          FSXErrorCode.INVALID_RANGE,
          `Invalid range [${start}, ${end}] for data of size ${entry.data.length}`,
          path
        );
      }
      return entry.data.slice(start, end + 1);
    }

    return entry.data.slice();
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    this.storage.set(path, {
      data: data.slice(),
      createdAt: new Date(),
    });
  }

  async delete(path: string): Promise<void> {
    this.storage.delete(path);
  }

  async list(prefix: string): Promise<string[]> {
    return [...this.storage.keys()]
      .filter((k) => k.startsWith(prefix))
      .sort();
  }

  async exists(path: string): Promise<boolean> {
    return this.storage.has(path);
  }

  async metadata(path: string): Promise<FSXMetadata | null> {
    const entry = this.storage.get(path);
    if (!entry) return null;

    return {
      size: entry.data.length,
      lastModified: entry.createdAt,
    };
  }

  /** Clear all data (for test cleanup) */
  clear(): void {
    this.storage.clear();
  }

  /** Get number of stored files */
  get size(): number {
    return this.storage.size;
  }

  /** Get all paths (for debugging) */
  paths(): string[] {
    return [...this.storage.keys()];
  }
}

/**
 * Create an in-memory FSX backend for testing
 */
export function createMemoryBackend(): MemoryFSXBackend {
  return new MemoryFSXBackend();
}
