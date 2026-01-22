/**
 * FSX (File System Abstraction) Types
 *
 * Core interfaces for the storage abstraction layer that enables
 * SQLite-like database operations on Cloudflare Workers using
 * DO storage (hot/recent data) and R2 (cold/archival storage).
 */

// =============================================================================
// Core FSX Backend Interface
// =============================================================================

/**
 * Range specification for partial reads
 * [start, end] - both inclusive, in bytes
 */
export type ByteRange = readonly [start: number, end: number];

/**
 * Core file system backend interface
 * All storage backends must implement this interface
 */
export interface FSXBackend {
  /**
   * Read data from a path
   * @param path - The file path to read
   * @param range - Optional byte range for partial reads [start, end] (inclusive)
   * @returns The data as Uint8Array, or null if not found
   */
  read(path: string, range?: ByteRange): Promise<Uint8Array | null>;

  /**
   * Write data to a path
   * @param path - The file path to write
   * @param data - The data to write
   */
  write(path: string, data: Uint8Array): Promise<void>;

  /**
   * Delete a file at path
   * @param path - The file path to delete
   */
  delete(path: string): Promise<void>;

  /**
   * List files matching a prefix
   * @param prefix - The path prefix to match
   * @returns Array of matching file paths
   */
  list(prefix: string): Promise<string[]>;

  /**
   * Check if a file exists at path
   * @param path - The file path to check
   * @returns True if file exists
   */
  exists(path: string): Promise<boolean>;
}

// =============================================================================
// Extended Backend Capabilities
// =============================================================================

/**
 * Metadata about a stored file
 */
export interface FSXMetadata {
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
 * Extended backend interface with metadata support
 */
export interface FSXBackendWithMeta extends FSXBackend {
  /**
   * Get metadata for a file without reading its contents
   * @param path - The file path
   * @returns Metadata or null if not found
   */
  metadata(path: string): Promise<FSXMetadata | null>;
}

// =============================================================================
// Chunking Configuration
// =============================================================================

/**
 * Configuration for chunk-based storage
 */
export interface ChunkConfig {
  /** Maximum chunk size in bytes (default: 2MB for DO storage) */
  maxChunkSize: number;
  /** Prefix for chunk keys */
  chunkPrefix: string;
}

/**
 * Default chunk configuration aligned with DO storage limits
 */
export const DEFAULT_CHUNK_CONFIG: ChunkConfig = {
  maxChunkSize: 2 * 1024 * 1024, // 2MB - DO storage limit
  chunkPrefix: '_chunks/',
};

// =============================================================================
// Tiered Storage Configuration
// =============================================================================

/**
 * Configuration for tiered storage behavior
 */
export interface TieredStorageConfig {
  /** Maximum age in ms before data is considered cold (default: 1 hour) */
  hotDataMaxAge: number;
  /** Maximum size in bytes for hot storage before migration (default: 100MB) */
  hotStorageMaxSize: number;
  /** Whether to automatically migrate cold data to R2 */
  autoMigrate: boolean;
  /** Whether to read from hot storage first (default: true) */
  readHotFirst: boolean;
  /** Whether to cache R2 reads in hot storage */
  cacheR2Reads: boolean;
  /** Maximum size of individual files to keep in hot storage */
  maxHotFileSize: number;
}

/**
 * Default tiered storage configuration
 */
export const DEFAULT_TIERED_CONFIG: TieredStorageConfig = {
  hotDataMaxAge: 60 * 60 * 1000, // 1 hour
  hotStorageMaxSize: 100 * 1024 * 1024, // 100MB
  autoMigrate: true,
  readHotFirst: true,
  cacheR2Reads: false,
  maxHotFileSize: 10 * 1024 * 1024, // 10MB - larger files go to R2 directly
};

// =============================================================================
// Storage Tier Metadata
// =============================================================================

/**
 * Indicates where data is stored in tiered storage
 */
export enum StorageTier {
  /** Data is in Durable Object storage (hot) */
  HOT = 'hot',
  /** Data is in R2 storage (cold) */
  COLD = 'cold',
  /** Data exists in both tiers */
  BOTH = 'both',
}

/**
 * Extended metadata for tiered storage
 */
export interface TieredMetadata extends FSXMetadata {
  /** Which storage tier(s) contain the data */
  tier: StorageTier;
  /** When data was last accessed */
  lastAccessed?: Date;
  /** When data was migrated to cold storage */
  migratedAt?: Date;
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * FSX-specific error codes
 */
export enum FSXErrorCode {
  NOT_FOUND = 'FSX_NOT_FOUND',
  WRITE_FAILED = 'FSX_WRITE_FAILED',
  READ_FAILED = 'FSX_READ_FAILED',
  DELETE_FAILED = 'FSX_DELETE_FAILED',
  CHUNK_CORRUPTED = 'FSX_CHUNK_CORRUPTED',
  SIZE_EXCEEDED = 'FSX_SIZE_EXCEEDED',
  MIGRATION_FAILED = 'FSX_MIGRATION_FAILED',
  INVALID_RANGE = 'FSX_INVALID_RANGE',
}

/**
 * Custom error class for FSX operations
 */
export class FSXError extends Error {
  constructor(
    public readonly code: FSXErrorCode,
    message: string,
    public readonly path?: string,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'FSXError';
  }
}

// =============================================================================
// Utility Types
// =============================================================================

/**
 * Options for write operations
 */
export interface WriteOptions {
  /** Force write to specific tier (for tiered storage) */
  tier?: StorageTier.HOT | StorageTier.COLD;
  /** Custom metadata to attach */
  metadata?: Record<string, string>;
  /** Skip chunking even for large files (use with caution) */
  skipChunking?: boolean;
}

/**
 * Options for read operations
 */
export interface ReadOptions {
  /** Force read from specific tier (for tiered storage) */
  tier?: StorageTier.HOT | StorageTier.COLD;
  /** Skip cache lookup */
  skipCache?: boolean;
}

/**
 * Result of a migration operation
 */
export interface MigrationResult {
  /** Paths that were successfully migrated */
  migrated: string[];
  /** Paths that failed to migrate with error messages */
  failed: Array<{ path: string; error: string }>;
  /** Total bytes migrated */
  bytesTransferred: number;
}
