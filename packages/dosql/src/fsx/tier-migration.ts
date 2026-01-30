/**
 * Tier Migration Module for FSX
 *
 * Handles data migration between storage tiers (hot/warm/cold):
 * - Migration scheduling and policies
 * - Progress tracking
 * - Retry logic with backoff
 *
 * This module is used by TieredStorageBackend to manage tier transitions
 * while keeping the main backend focused on storage operations.
 */

import {
  type FSXBackendWithMeta,
  type FSXMetadata,
  type MigrationResult,
  StorageTier,
  FSXError,
  FSXErrorCode,
} from './types.js';

// =============================================================================
// Migration Types
// =============================================================================

/**
 * Index entry for tracking file locations across tiers
 */
export interface TierIndexEntry {
  /** Original file path */
  path: string;
  /** File size in bytes */
  size: number;
  /** Which tier(s) contain the file */
  tier: StorageTier;
  /** Creation timestamp */
  createdAt: number;
  /** Last access timestamp */
  lastAccessed: number;
  /** When migrated to cold storage (if applicable) */
  migratedAt?: number;
  /** ETag from R2 (for cache validation) */
  r2Etag?: string;
  /** Access count for prioritizing which files to migrate */
  accessCount: number;
  /** Whether file is pinned to hot tier */
  pinned?: boolean;
  /** Write version counter for detecting concurrent writes during migration */
  writeVersion?: number;
}

/**
 * Migration history entry
 */
export interface MigrationHistoryEntry {
  timestamp: number;
  fileCount: number;
  bytesTransferred: number;
  direction: 'hot-to-cold' | 'cold-to-hot';
}

/**
 * Options for migrating data to cold storage (R2)
 */
export interface MigrateToR2Options {
  /** Only migrate files older than this (ms) */
  olderThan?: number;
  /** Maximum number of files to migrate */
  limit?: number;
  /** Only migrate files matching this prefix */
  prefix?: string;
  /** Delete from hot storage after successful migration */
  deleteFromHot?: boolean;
  /** Target size to free up in bytes */
  targetSize?: number;
  /** Path to exclude from migration (e.g., file just written) */
  excludePath?: string;
}

/**
 * Options for promoting data to hot storage
 */
export interface PromoteToHotOptions {
  /** Paths to promote */
  paths: string[];
  /** Maximum file size to allow in hot storage */
  maxHotFileSize: number;
}

/**
 * Migration policy configuration
 */
export interface MigrationPolicy {
  /** Maximum age in ms before data is considered cold */
  hotDataMaxAge: number;
  /** Maximum size in bytes for hot storage before migration */
  hotStorageMaxSize: number;
  /** Maximum size of individual files to keep in hot storage */
  maxHotFileSize: number;
  /** Whether auto-migration is enabled */
  autoMigrate: boolean;
}

/**
 * Extended backend interface with index access (DO)
 */
export interface HotBackendWithIndex extends FSXBackendWithMeta {
  getStats(): Promise<{ fileCount: number; totalSize: number; chunkedFileCount: number }>;
}

/**
 * Extended backend interface with batch operations (R2)
 */
export interface ColdBackendWithBatch extends FSXBackendWithMeta {
  deleteMany(paths: string[]): Promise<void>;
  getStats(prefix?: string): Promise<{ objectCount: number; totalSize: number }>;
}

// =============================================================================
// Migration Progress Tracker
// =============================================================================

/**
 * Tracks migration progress and history
 */
export class MigrationProgressTracker {
  /** Migration history entries */
  private history: MigrationHistoryEntry[] = [];

  /** Total bytes migrated (cumulative) */
  private _totalBytesMigrated = 0;

  /** Total migration count */
  private _migrationCount = 0;

  /** Maximum history entries to keep */
  private readonly maxHistorySize: number;

  constructor(maxHistorySize = 100) {
    this.maxHistorySize = maxHistorySize;
  }

  /**
   * Record a successful migration
   */
  recordMigration(
    direction: 'hot-to-cold' | 'cold-to-hot',
    fileCount: number,
    bytesTransferred: number
  ): void {
    if (fileCount === 0) return;

    this.history.push({
      timestamp: Date.now(),
      fileCount,
      bytesTransferred,
      direction,
    });

    // Trim history if needed
    if (this.history.length > this.maxHistorySize) {
      this.history = this.history.slice(-this.maxHistorySize);
    }

    this._totalBytesMigrated += bytesTransferred;
    this._migrationCount++;
  }

  /**
   * Get last migration timestamp
   */
  getLastMigration(): Date | undefined {
    if (this.history.length === 0) return undefined;
    return new Date(this.history[this.history.length - 1].timestamp);
  }

  /**
   * Get migration statistics
   */
  getStats(): {
    migrationCount: number;
    totalBytesMigrated: number;
    lastMigration?: Date;
    recentHistory: MigrationHistoryEntry[];
  } {
    return {
      migrationCount: this._migrationCount,
      totalBytesMigrated: this._totalBytesMigrated,
      lastMigration: this.getLastMigration(),
      recentHistory: [...this.history],
    };
  }

  /** Total bytes migrated */
  get totalBytesMigrated(): number {
    return this._totalBytesMigrated;
  }

  /** Total migration count */
  get migrationCount(): number {
    return this._migrationCount;
  }
}

// =============================================================================
// Tier Migrator
// =============================================================================

/**
 * Handles migration of data between storage tiers
 */
export class TierMigrator {
  private readonly hotBackend: HotBackendWithIndex;
  private readonly coldBackend: ColdBackendWithBatch;
  private readonly policy: MigrationPolicy;
  private readonly progressTracker: MigrationProgressTracker;

  /** Index prefix in hot storage */
  private readonly indexPrefix: string;

  constructor(
    hotBackend: HotBackendWithIndex,
    coldBackend: ColdBackendWithBatch,
    policy: MigrationPolicy,
    progressTracker: MigrationProgressTracker,
    indexPrefix = '_tier_index/'
  ) {
    this.hotBackend = hotBackend;
    this.coldBackend = coldBackend;
    this.policy = policy;
    this.progressTracker = progressTracker;
    this.indexPrefix = indexPrefix;
  }

  /**
   * Migrate data from hot storage to cold storage (R2)
   */
  async migrateToR2(
    tierIndex: Map<string, TierIndexEntry>,
    setTierEntry: (entry: TierIndexEntry) => Promise<void>,
    options: MigrateToR2Options = {}
  ): Promise<MigrationResult> {
    const {
      olderThan = this.policy.hotDataMaxAge,
      limit = 100,
      prefix = '',
      deleteFromHot = true,
      targetSize,
      excludePath,
    } = options;

    const result: MigrationResult = {
      migrated: [],
      failed: [],
      bytesTransferred: 0,
    };

    const now = Date.now();
    // Handle olderThan = Infinity (used for size-based migration)
    const cutoff = olderThan === Infinity ? Infinity : now - olderThan;

    // Find candidates for migration
    const candidates = await this.findMigrationCandidates(
      tierIndex,
      cutoff,
      prefix,
      excludePath,
      limit
    );

    // Track bytes freed for targetSize
    let bytesFreed = 0;

    // Migrate each candidate
    for (const entry of candidates) {
      // Stop if we've freed enough space
      if (targetSize && bytesFreed >= targetSize) break;

      // Capture the entry's writeVersion before migration to detect concurrent writes
      const preWriteVersion = entry.writeVersion ?? 0;

      try {
        // Read from hot
        const data = await this.hotBackend.read(entry.path);
        if (!data) {
          result.failed.push({ path: entry.path, error: 'File not found in hot storage' });
          continue;
        }

        // Write to cold
        await this.coldBackend.write(entry.path, data);

        // Check if the file was concurrently written (writeVersion changed in tierIndex)
        const currentEntry = tierIndex.get(entry.path);
        if (currentEntry && (currentEntry.writeVersion ?? 0) !== preWriteVersion) {
          // File was rewritten during migration - the new data is in hot storage.
          // Keep the cold copy (it's stale but harmless), but don't delete from hot
          // and don't update the index to COLD (it should stay HOT for the new data).
          continue;
        }

        // Update index
        const coldMeta = await this.coldBackend.metadata(entry.path);
        entry.tier = deleteFromHot ? StorageTier.COLD : StorageTier.BOTH;
        entry.migratedAt = now;
        entry.r2Etag = coldMeta?.etag;
        await setTierEntry(entry);

        // Optionally delete from hot
        if (deleteFromHot) {
          await this.hotBackend.delete(entry.path);
          bytesFreed += data.length;
        }

        result.migrated.push(entry.path);
        result.bytesTransferred += data.length;
      } catch (error) {
        result.failed.push({
          path: entry.path,
          error: (error as Error).message,
        });
      }
    }

    // Track migration in history
    if (result.migrated.length > 0) {
      this.progressTracker.recordMigration(
        'hot-to-cold',
        result.migrated.length,
        result.bytesTransferred
      );
    }

    return result;
  }

  /**
   * Promote data from cold storage back to hot storage
   */
  async promoteToHot(
    paths: string[],
    tierIndex: Map<string, TierIndexEntry>,
    getTierEntry: (path: string) => Promise<TierIndexEntry | null>,
    setTierEntry: (entry: TierIndexEntry) => Promise<void>,
    updateTierEntry: (entry: TierIndexEntry) => Promise<void>
  ): Promise<MigrationResult> {
    const result: MigrationResult = {
      migrated: [],
      failed: [],
      bytesTransferred: 0,
    };

    for (const path of paths) {
      try {
        let entry = await getTierEntry(path);

        if (entry?.tier === StorageTier.HOT || entry?.tier === StorageTier.BOTH) {
          // Already in hot
          result.migrated.push(path);
          continue;
        }

        // Read from cold
        const data = await this.coldBackend.read(path);
        if (!data) {
          result.failed.push({ path, error: 'File not found in cold storage' });
          continue;
        }

        // Check size limit
        if (data.length > this.policy.maxHotFileSize) {
          result.failed.push({ path, error: 'File too large for hot storage' });
          continue;
        }

        // Write to hot
        await this.hotBackend.write(path, data);

        // Update index
        const now = Date.now();
        if (entry) {
          entry.tier = StorageTier.BOTH;
          entry.lastAccessed = now;
          entry.accessCount = (entry.accessCount || 0) + 1;
          await updateTierEntry(entry);
        } else {
          await setTierEntry({
            path,
            size: data.length,
            tier: StorageTier.BOTH,
            createdAt: now,
            lastAccessed: now,
            accessCount: 1,
          });
        }

        result.migrated.push(path);
        result.bytesTransferred += data.length;
      } catch (error) {
        result.failed.push({
          path,
          error: (error as Error).message,
        });
      }
    }

    // Track migration in history
    if (result.migrated.length > 0) {
      this.progressTracker.recordMigration(
        'cold-to-hot',
        result.migrated.length,
        result.bytesTransferred
      );
    }

    return result;
  }

  /**
   * Run automatic migration based on policy
   * Called after writes when autoMigrate is enabled
   */
  async maybeRunMigration(
    tierIndex: Map<string, TierIndexEntry>,
    setTierEntry: (entry: TierIndexEntry) => Promise<void>,
    excludePath?: string
  ): Promise<void> {
    // Always run age-based migration first
    await this.migrateToR2(tierIndex, setTierEntry, {
      olderThan: this.policy.hotDataMaxAge,
      limit: 50,
      deleteFromHot: true,
      excludePath,
    });

    // Calculate actual data size (excluding index entries)
    let actualDataSize = 0;
    for (const entry of tierIndex.values()) {
      if (entry.tier === StorageTier.HOT || entry.tier === StorageTier.BOTH) {
        actualDataSize += entry.size;
      }
    }

    if (actualDataSize > this.policy.hotStorageMaxSize) {
      // Calculate how much space we need to free
      const excessSize = actualDataSize - this.policy.hotStorageMaxSize;

      // Run migration to free up space - prioritize by access pattern
      // We use olderThan = Infinity to consider all files, but only migrate
      // enough to get under the size limit
      await this.migrateToR2(tierIndex, setTierEntry, {
        olderThan: Infinity, // Consider all files when over size limit
        limit: 100,
        deleteFromHot: true,
        targetSize: excessSize + (this.policy.hotStorageMaxSize * 0.1), // Free 10% buffer
        excludePath,
      });
    }
  }

  /**
   * Find candidates for migration based on criteria
   */
  private async findMigrationCandidates(
    tierIndex: Map<string, TierIndexEntry>,
    cutoff: number,
    prefix: string,
    excludePath: string | undefined,
    limit: number
  ): Promise<TierIndexEntry[]> {
    const candidates: TierIndexEntry[] = [];

    // Find candidates from tier index
    for (const entry of tierIndex.values()) {
      if (entry.tier !== StorageTier.HOT) continue;
      // Skip pinned files
      if (entry.pinned) continue;
      // Skip excluded path (usually the file that triggered migration)
      if (excludePath && entry.path === excludePath) continue;
      // For age-based migration, only consider files older than cutoff
      // For size-based migration (cutoff = Infinity), consider all files
      if (cutoff !== Infinity && entry.lastAccessed > cutoff) continue;
      if (prefix && !entry.path.startsWith(prefix)) continue;

      candidates.push(entry);
    }

    // Also check hot storage for files without index entries
    const hotFiles = await this.hotBackend.list(prefix);
    for (const path of hotFiles) {
      if (path.startsWith(this.indexPrefix)) continue;
      // Skip excluded path
      if (excludePath && path === excludePath) continue;

      if (!tierIndex.has(path)) {
        const meta = await this.hotBackend.metadata(path);
        if (meta) {
          const fileTime = meta.lastModified.getTime();
          if (cutoff === Infinity || fileTime <= cutoff) {
            candidates.push({
              path,
              size: meta.size,
              tier: StorageTier.HOT,
              createdAt: fileTime,
              lastAccessed: fileTime,
              accessCount: 0,
            });
          }
        }
      }
    }

    // Sort candidates by priority:
    // 1. Lower access count = higher priority for migration
    // 2. Older lastAccessed = higher priority for migration
    candidates.sort((a, b) => {
      const accessDiff = (a.accessCount || 0) - (b.accessCount || 0);
      if (accessDiff !== 0) return accessDiff;
      return a.lastAccessed - b.lastAccessed;
    });

    // Limit candidates
    return candidates.slice(0, limit);
  }

  /**
   * Calculate number of files pending migration
   */
  calculatePendingMigrations(tierIndex: Map<string, TierIndexEntry>): number {
    let pending = 0;
    const now = Date.now();

    for (const entry of tierIndex.values()) {
      if (entry.tier === StorageTier.HOT) {
        // Check if file is old enough to be migration candidate
        if (now - entry.lastAccessed > this.policy.hotDataMaxAge) {
          pending++;
        }
      }
    }

    return pending;
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a new MigrationProgressTracker
 */
export function createProgressTracker(maxHistorySize = 100): MigrationProgressTracker {
  return new MigrationProgressTracker(maxHistorySize);
}

/**
 * Create a new TierMigrator
 */
export function createTierMigrator(
  hotBackend: HotBackendWithIndex,
  coldBackend: ColdBackendWithBatch,
  policy: MigrationPolicy,
  progressTracker: MigrationProgressTracker,
  indexPrefix = '_tier_index/'
): TierMigrator {
  return new TierMigrator(hotBackend, coldBackend, policy, progressTracker, indexPrefix);
}
