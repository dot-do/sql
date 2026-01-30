/**
 * Tiered Storage Backend for FSX
 *
 * Combines DO storage (hot) + R2 (cold) for optimal performance and cost.
 *
 * Strategy:
 * - Write to DO first for fast recent access
 * - Migrate to R2 for cold/archival storage
 * - Read from DO first, fallback to R2
 * - Optional: cache R2 reads in DO for repeated access
 */

import {
  type FSXBackendWithMeta,
  type ByteRange,
  type TieredStorageConfig,
  type TieredMetadata,
  type MigrationResult,
  DEFAULT_TIERED_CONFIG,
  StorageTier,
  FSXError,
  FSXErrorCode,
} from './types.js';
import { type DOStorageBackend } from './do-backend.js';
import { type R2StorageBackend } from './r2-backend.js';
import {
  type TierIndexEntry,
  type HotBackendWithIndex,
  type ColdBackendWithBatch,
  type MigrateToR2Options,
  TierMigrator,
  MigrationProgressTracker,
  createProgressTracker,
  createTierMigrator,
} from './tier-migration.js';

/**
 * Extended statistics interface
 */
export interface TieredStorageStats {
  hot: { fileCount: number; totalSize: number };
  cold: { objectCount: number; totalSize: number };
  index: { entryCount: number };
  totalFiles: number;
  hotToTotalRatio: number;
  migrationPending: number;
  hotStorageWarning?: boolean;
  lastMigration?: Date;
  migrationCount: number;
  totalBytesMigrated: number;
}

/**
 * Tiered storage backend that combines DO (hot) and R2 (cold) storage
 */
export class TieredStorageBackend implements FSXBackendWithMeta {
  private readonly hotBackend: HotBackendWithIndex;
  private readonly coldBackend: ColdBackendWithBatch;
  private readonly config: TieredStorageConfig;

  /** In-memory index of file locations (for fast tier lookup) */
  private tierIndex = new Map<string, TierIndexEntry>();

  /** Index key prefix in hot storage */
  private readonly indexPrefix = '_tier_index/';

  /** Migration progress tracker */
  private readonly progressTracker: MigrationProgressTracker;

  /** Tier migrator for handling migration operations */
  private readonly migrator: TierMigrator;

  constructor(
    hotBackend: DOStorageBackend,
    coldBackend: R2StorageBackend,
    config: Partial<TieredStorageConfig> = {}
  ) {
    this.hotBackend = hotBackend as HotBackendWithIndex;
    this.coldBackend = coldBackend as ColdBackendWithBatch;
    this.config = { ...DEFAULT_TIERED_CONFIG, ...config };

    // Initialize migration components
    this.progressTracker = createProgressTracker();
    this.migrator = createTierMigrator(
      this.hotBackend,
      this.coldBackend,
      {
        hotDataMaxAge: this.config.hotDataMaxAge,
        hotStorageMaxSize: this.config.hotStorageMaxSize,
        maxHotFileSize: this.config.maxHotFileSize,
        autoMigrate: this.config.autoMigrate,
      },
      this.progressTracker,
      this.indexPrefix
    );
  }

  // ===========================================================================
  // FSXBackend Implementation
  // ===========================================================================

  async read(path: string, range?: ByteRange): Promise<Uint8Array | null> {
    const entry = await this.getTierEntry(path);

    if (!entry) {
      // No index entry - try both backends
      return this.readWithoutIndex(path, range);
    }

    // Update last accessed time and access count
    entry.lastAccessed = Date.now();
    entry.accessCount = (entry.accessCount || 0) + 1;
    await this.updateTierEntry(entry);

    // Read based on tier
    if (this.config.readHotFirst) {
      return this.readHotFirst(path, entry, range);
    } else {
      return this.readColdFirst(path, entry, range);
    }
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    return this.writeWithTier(path, data, {});
  }

  /**
   * Write data to a specific tier
   * @param path - Path to write
   * @param data - Data to write
   * @param options - Write options including tier hint
   */
  async writeWithTier(
    path: string,
    data: Uint8Array,
    options: { tier?: StorageTier.HOT | StorageTier.COLD } = {}
  ): Promise<void> {
    const now = Date.now();
    const size = data.length;

    // Determine initial tier based on options, then size
    let tier: StorageTier;

    if (options.tier === StorageTier.COLD) {
      // User explicitly requested cold storage
      await this.coldBackend.write(path, data);
      tier = StorageTier.COLD;
    } else if (options.tier === StorageTier.HOT) {
      // User explicitly requested hot storage (if within size limit)
      if (size > this.config.maxHotFileSize) {
        throw new FSXError(
          FSXErrorCode.SIZE_EXCEEDED,
          `File size ${size} exceeds maxHotFileSize ${this.config.maxHotFileSize}`,
          path
        );
      }
      await this.hotBackend.write(path, data);
      tier = StorageTier.HOT;
    } else if (size > this.config.maxHotFileSize) {
      // Large files go directly to cold storage
      await this.coldBackend.write(path, data);
      tier = StorageTier.COLD;
    } else {
      // Normal files go to hot storage first
      await this.hotBackend.write(path, data);
      tier = StorageTier.HOT;
    }

    // Update tier index with incremented write version
    const existingEntry = this.tierIndex.get(path);
    const entry: TierIndexEntry = {
      path,
      size,
      tier,
      createdAt: now,
      lastAccessed: now,
      accessCount: 0,
      writeVersion: (existingEntry?.writeVersion ?? 0) + 1,
    };

    await this.setTierEntry(entry);

    // Check if we need to migrate old data
    if (this.config.autoMigrate) {
      // Await migration to ensure it completes before returning
      // Exclude the file we just wrote from migration
      await this.maybeRunMigration(path).catch(() => {
        // Ignore migration errors
      });
    }
  }

  async delete(path: string): Promise<void> {
    const entry = await this.getTierEntry(path);

    // Delete from both tiers if present
    const promises: Promise<void>[] = [];

    if (!entry || entry.tier === StorageTier.HOT || entry.tier === StorageTier.BOTH) {
      promises.push(this.hotBackend.delete(path).catch(() => {}));
    }

    if (!entry || entry.tier === StorageTier.COLD || entry.tier === StorageTier.BOTH) {
      promises.push(this.coldBackend.delete(path).catch(() => {}));
    }

    await Promise.all(promises);

    // Remove from index
    await this.deleteTierEntry(path);
  }

  async list(prefix: string): Promise<string[]> {
    // List from both backends and deduplicate
    const [hotPaths, coldPaths] = await Promise.all([
      this.hotBackend.list(prefix),
      this.coldBackend.list(prefix),
    ]);

    // Filter out index entries from hot paths
    const filteredHotPaths = hotPaths.filter(
      (p) => !p.startsWith(this.indexPrefix)
    );

    // Combine and deduplicate
    const allPaths = new Set([...filteredHotPaths, ...coldPaths]);
    return [...allPaths].sort();
  }

  async exists(path: string): Promise<boolean> {
    const entry = await this.getTierEntry(path);

    if (entry) {
      return true;
    }

    // Fallback to checking both backends
    const [hotExists, coldExists] = await Promise.all([
      this.hotBackend.exists(path),
      this.coldBackend.exists(path),
    ]);

    return hotExists || coldExists;
  }

  // ===========================================================================
  // FSXBackendWithMeta Implementation
  // ===========================================================================

  async metadata(path: string): Promise<TieredMetadata | null> {
    const entry = await this.getTierEntry(path);

    if (!entry) {
      // Try to get metadata from backends
      const [hotMeta, coldMeta] = await Promise.all([
        this.hotBackend.metadata(path),
        this.coldBackend.metadata(path),
      ]);

      if (hotMeta) {
        return { ...hotMeta, tier: StorageTier.HOT };
      }
      if (coldMeta) {
        return { ...coldMeta, tier: StorageTier.COLD };
      }
      return null;
    }

    return {
      size: entry.size,
      lastModified: new Date(entry.createdAt),
      tier: entry.tier,
      lastAccessed: new Date(entry.lastAccessed),
      migratedAt: entry.migratedAt ? new Date(entry.migratedAt) : undefined,
      etag: entry.r2Etag,
    };
  }

  // ===========================================================================
  // Tier-Specific Read Strategies
  // ===========================================================================

  private async readHotFirst(
    path: string,
    entry: TierIndexEntry,
    range?: ByteRange
  ): Promise<Uint8Array | null> {
    // Try hot storage first
    if (entry.tier === StorageTier.HOT || entry.tier === StorageTier.BOTH) {
      const data = await this.hotBackend.read(path, range);
      if (data) return data;
    }

    // Fall back to cold storage
    if (entry.tier === StorageTier.COLD || entry.tier === StorageTier.BOTH) {
      const data = await this.coldBackend.read(path, range);

      if (data && this.config.cacheR2Reads && !range) {
        // Only cache if file size is within limits
        const fileSize = entry.size;
        if (fileSize <= this.config.maxHotFileSize) {
          // Cache in hot storage for future reads
          await this.cacheInHot(path, data, entry);
        }
      }

      return data;
    }

    return null;
  }

  private async readColdFirst(
    path: string,
    entry: TierIndexEntry,
    range?: ByteRange
  ): Promise<Uint8Array | null> {
    // Try cold storage first (useful for large archived datasets)
    if (entry.tier === StorageTier.COLD || entry.tier === StorageTier.BOTH) {
      const data = await this.coldBackend.read(path, range);
      if (data) return data;
    }

    // Fall back to hot storage
    if (entry.tier === StorageTier.HOT || entry.tier === StorageTier.BOTH) {
      return this.hotBackend.read(path, range);
    }

    return null;
  }

  private async readWithoutIndex(
    path: string,
    range?: ByteRange
  ): Promise<Uint8Array | null> {
    const now = Date.now();

    // No index entry - check both backends
    const hotData = await this.hotBackend.read(path, range);
    if (hotData) {
      // Create index entry
      const fullData = range ? await this.hotBackend.read(path) : hotData;
      if (fullData) {
        await this.setTierEntry({
          path,
          size: fullData.length,
          tier: StorageTier.HOT,
          createdAt: now,
          lastAccessed: now,
          accessCount: 1,
        });
      }
      return hotData;
    }

    const coldData = await this.coldBackend.read(path, range);
    if (coldData) {
      // Create index entry
      const meta = await this.coldBackend.metadata(path);
      const entry: TierIndexEntry = {
        path,
        size: meta?.size ?? coldData.length,
        tier: StorageTier.COLD,
        createdAt: now,
        lastAccessed: now,
        r2Etag: meta?.etag,
        accessCount: 1,
      };

      await this.setTierEntry(entry);

      // Cache in hot if enabled (uses cacheInHot for eviction support)
      if (this.config.cacheR2Reads && !range) {
        await this.cacheInHot(path, coldData, entry);
      }

      return coldData;
    }

    return null;
  }

  private async cacheInHot(
    path: string,
    data: Uint8Array,
    entry: TierIndexEntry
  ): Promise<void> {
    // Only cache if file is small enough
    if (data.length > this.config.maxHotFileSize) {
      return;
    }

    try {
      // Check if caching would exceed hot storage max size; evict cached entries if needed
      if (this.config.hotStorageMaxSize > 0) {
        await this.evictCachedIfNeeded(data.length);
      }

      await this.hotBackend.write(path, data);

      // Update entry to show both tiers
      entry.tier = StorageTier.BOTH;
      await this.updateTierEntry(entry);
    } catch {
      // Ignore caching errors
    }
  }

  /**
   * Evict cached (BOTH-tier) entries from hot storage to make room for new cached data.
   * Only evicts entries that are cached copies of cold data (tier === BOTH),
   * not native hot data.
   */
  private async evictCachedIfNeeded(incomingSize: number): Promise<void> {
    // Calculate current hot data size
    let hotDataSize = 0;
    for (const entry of this.tierIndex.values()) {
      if (entry.tier === StorageTier.HOT || entry.tier === StorageTier.BOTH) {
        hotDataSize += entry.size;
      }
    }

    // If adding new data wouldn't exceed limit, no eviction needed
    if (hotDataSize + incomingSize <= this.config.hotStorageMaxSize) {
      return;
    }

    // Find cached entries (BOTH tier) sorted by lastAccessed (oldest first)
    const cachedEntries: TierIndexEntry[] = [];
    for (const entry of this.tierIndex.values()) {
      if (entry.tier === StorageTier.BOTH && !entry.pinned) {
        cachedEntries.push(entry);
      }
    }
    cachedEntries.sort((a, b) => a.lastAccessed - b.lastAccessed);

    // Evict oldest cached entries until we have enough space
    let spaceNeeded = (hotDataSize + incomingSize) - this.config.hotStorageMaxSize;
    for (const entry of cachedEntries) {
      if (spaceNeeded <= 0) break;

      try {
        await this.hotBackend.delete(entry.path);
        entry.tier = StorageTier.COLD;
        await this.updateTierEntry(entry);
        spaceNeeded -= entry.size;
      } catch {
        // Ignore eviction errors for individual files
      }
    }
  }

  // ===========================================================================
  // Migration Operations
  // ===========================================================================

  /**
   * Migrate cold data from hot storage to R2
   * Delegates to the TierMigrator for actual migration logic
   */
  async migrateToR2(options: MigrateToR2Options = {}): Promise<MigrationResult> {
    return this.migrator.migrateToR2(
      this.tierIndex,
      (entry) => this.setTierEntry(entry),
      options
    );
  }

  /**
   * Promote cold data back to hot storage
   * Delegates to the TierMigrator for actual promotion logic
   */
  async promoteToHot(paths: string[]): Promise<MigrationResult> {
    return this.migrator.promoteToHot(
      paths,
      this.tierIndex,
      (path) => this.getTierEntry(path),
      (entry) => this.setTierEntry(entry),
      (entry) => this.updateTierEntry(entry)
    );
  }

  /**
   * Pin a file to hot storage, preventing automatic migration
   * @param path - Path to pin
   */
  async pinToHot(path: string): Promise<void> {
    const entry = await this.getTierEntry(path);
    if (!entry) {
      throw new FSXError(
        FSXErrorCode.NOT_FOUND,
        `File not found: ${path}`,
        path
      );
    }

    entry.pinned = true;
    await this.updateTierEntry(entry);
  }

  /**
   * Unpin a file from hot storage, allowing automatic migration
   * @param path - Path to unpin
   */
  async unpinFromHot(path: string): Promise<void> {
    const entry = await this.getTierEntry(path);
    if (!entry) {
      throw new FSXError(
        FSXErrorCode.NOT_FOUND,
        `File not found: ${path}`,
        path
      );
    }

    entry.pinned = false;
    await this.updateTierEntry(entry);
  }

  private async maybeRunMigration(excludePath?: string): Promise<void> {
    // Delegate to the TierMigrator
    await this.migrator.maybeRunMigration(
      this.tierIndex,
      (entry) => this.setTierEntry(entry),
      excludePath
    );
  }

  // ===========================================================================
  // Tier Index Management
  // ===========================================================================

  private async getTierEntry(path: string): Promise<TierIndexEntry | null> {
    // Check in-memory cache first
    const cached = this.tierIndex.get(path);
    if (cached) return cached;

    // Load from hot storage
    const indexKey = this.indexPrefix + path;
    const data = await this.hotBackend.read(indexKey);

    if (data) {
      const entry = JSON.parse(new TextDecoder().decode(data)) as TierIndexEntry;
      this.tierIndex.set(path, entry);
      return entry;
    }

    return null;
  }

  private async setTierEntry(entry: TierIndexEntry): Promise<void> {
    this.tierIndex.set(entry.path, entry);

    const indexKey = this.indexPrefix + entry.path;
    const data = new TextEncoder().encode(JSON.stringify(entry));
    await this.hotBackend.write(indexKey, data);
  }

  private async updateTierEntry(entry: TierIndexEntry): Promise<void> {
    await this.setTierEntry(entry);
  }

  private async deleteTierEntry(path: string): Promise<void> {
    this.tierIndex.delete(path);

    const indexKey = this.indexPrefix + path;
    await this.hotBackend.delete(indexKey);
  }

  // ===========================================================================
  // Administrative Methods
  // ===========================================================================

  /**
   * Get storage statistics across both tiers
   */
  async getStats(): Promise<TieredStorageStats> {
    const [hotStats, coldStats] = await Promise.all([
      this.hotBackend.getStats(),
      this.coldBackend.getStats(),
    ]);

    // Calculate actual hot data size (excluding index entries)
    let hotDataSize = 0;
    let hotFileCount = 0;
    let coldFileCount = 0;

    for (const entry of this.tierIndex.values()) {
      if (entry.tier === StorageTier.HOT) {
        hotDataSize += entry.size;
        hotFileCount++;
      } else if (entry.tier === StorageTier.COLD) {
        coldFileCount++;
      } else if (entry.tier === StorageTier.BOTH) {
        hotDataSize += entry.size;
        hotFileCount++;
        coldFileCount++;
      }
    }

    // Get migration pending count from migrator
    const migrationPending = this.migrator.calculatePendingMigrations(this.tierIndex);

    const totalFiles = new Set([...this.tierIndex.keys()]).size;
    const totalSize = hotDataSize + coldStats.totalSize;
    const hotToTotalRatio = totalSize > 0 ? hotDataSize / totalSize : 0;

    // Determine if we should warn about storage limits
    const hotStorageWarning = this.config.hotStorageMaxSize > 0 &&
      hotDataSize / this.config.hotStorageMaxSize > 0.7;

    // Get migration stats from progress tracker
    const migrationStats = this.progressTracker.getStats();

    return {
      hot: {
        fileCount: hotFileCount || hotStats.fileCount,
        totalSize: hotDataSize || hotStats.totalSize,
      },
      cold: coldStats,
      index: {
        entryCount: this.tierIndex.size,
      },
      totalFiles,
      hotToTotalRatio,
      migrationPending,
      hotStorageWarning,
      lastMigration: migrationStats.lastMigration,
      migrationCount: migrationStats.migrationCount,
      totalBytesMigrated: migrationStats.totalBytesMigrated,
    };
  }

  /**
   * Rebuild tier index from actual storage contents
   */
  async rebuildIndex(): Promise<{ indexed: number; errors: number }> {
    let indexed = 0;
    let errors = 0;

    // Clear existing index
    this.tierIndex.clear();

    // Index hot storage
    const hotPaths = await this.hotBackend.list('');
    for (const path of hotPaths) {
      if (path.startsWith(this.indexPrefix)) continue;

      try {
        const meta = await this.hotBackend.metadata(path);
        if (meta) {
          const entry: TierIndexEntry = {
            path,
            size: meta.size,
            tier: StorageTier.HOT,
            createdAt: meta.lastModified.getTime(),
            lastAccessed: Date.now(),
            accessCount: 0,
          };

          // Check if also in cold
          const coldExists = await this.coldBackend.exists(path);
          if (coldExists) {
            entry.tier = StorageTier.BOTH;
            const coldMeta = await this.coldBackend.metadata(path);
            entry.r2Etag = coldMeta?.etag;
          }

          await this.setTierEntry(entry);
          indexed++;
        }
      } catch {
        errors++;
      }
    }

    // Index cold-only storage
    const coldPaths = await this.coldBackend.list('');
    for (const path of coldPaths) {
      if (this.tierIndex.has(path)) continue;

      try {
        const meta = await this.coldBackend.metadata(path);
        if (meta) {
          await this.setTierEntry({
            path,
            size: meta.size,
            tier: StorageTier.COLD,
            createdAt: meta.lastModified.getTime(),
            lastAccessed: Date.now(),
            r2Etag: meta.etag,
            accessCount: 0,
          });
          indexed++;
        }
      } catch {
        errors++;
      }
    }

    return { indexed, errors };
  }

  /**
   * Load tier index from storage into memory
   */
  async loadIndex(): Promise<number> {
    const indexPaths = await this.hotBackend.list(this.indexPrefix);
    let loaded = 0;

    for (const indexPath of indexPaths) {
      try {
        const data = await this.hotBackend.read(indexPath);
        if (data) {
          const entry = JSON.parse(new TextDecoder().decode(data)) as TierIndexEntry;
          this.tierIndex.set(entry.path, entry);
          loaded++;
        }
      } catch {
        // Skip corrupted index entries
      }
    }

    return loaded;
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a tiered storage backend combining DO (hot) and R2 (cold)
 * @param hotBackend - DO storage backend for hot data
 * @param coldBackend - R2 storage backend for cold data
 * @param config - Optional tiered storage configuration
 */
export function createTieredBackend(
  hotBackend: DOStorageBackend,
  coldBackend: R2StorageBackend,
  config?: Partial<TieredStorageConfig>
): TieredStorageBackend {
  return new TieredStorageBackend(hotBackend, coldBackend, config);
}
