/**
 * Garbage Collection for COW FSX
 *
 * Handles cleanup of unreferenced blobs to reclaim storage space.
 * Uses reference counting to track blob usage across branches.
 *
 * Key features:
 * - Reference counting based GC
 * - Safe deletion (only removes blobs with refCount = 0)
 * - Dry-run mode for previewing cleanup
 * - Configurable age threshold
 * - Per-branch GC support
 */

import type { FSXBackend } from './types.js';
import type { COWBackend } from './cow-backend.js';
import {
  type BlobRef,
  type GCResult,
  type GCOptions,
  COWError,
  COWErrorCode,
  BLOB_PREFIX,
  REF_PREFIX,
  BRANCH_PREFIX,
  SNAPSHOT_PREFIX,
} from './cow-types.js';

// =============================================================================
// Garbage Collector
// =============================================================================

/**
 * Default GC options
 */
const DEFAULT_GC_OPTIONS: Required<GCOptions> = {
  olderThan: 0, // No age restriction by default
  dryRun: false,
  limit: 1000,
  branches: [],
};

/**
 * Garbage collector for COW storage
 */
export class GarbageCollector {
  private readonly storage: FSXBackend;
  private readonly cow: COWBackend;

  constructor(storage: FSXBackend, cow: COWBackend) {
    this.storage = storage;
    this.cow = cow;
  }

  // ===========================================================================
  // Main GC Entry Point
  // ===========================================================================

  /**
   * Run garbage collection
   */
  async collect(options: GCOptions = {}): Promise<GCResult> {
    const opts = { ...DEFAULT_GC_OPTIONS, ...options };
    const startTime = Date.now();

    const result: GCResult = {
      blobsDeleted: 0,
      bytesReclaimed: 0,
      deletedPaths: [],
      durationMs: 0,
      blobsScanned: 0,
      errors: [],
    };

    try {
      // Scan all blobs
      const blobsToDelete = await this.findUnreferencedBlobs(opts);
      result.blobsScanned = blobsToDelete.scanned;

      // Delete unreferenced blobs
      for (const blob of blobsToDelete.unreferenced) {
        if (result.blobsDeleted >= opts.limit) break;

        try {
          if (!opts.dryRun) {
            await this.deleteBlob(blob);
          }

          result.blobsDeleted++;
          result.bytesReclaimed += blob.size;
          result.deletedPaths.push(blob.path);
        } catch (error) {
          result.errors.push({
            path: blob.path,
            error: (error as Error).message,
          });
        }
      }
    } catch (error) {
      result.errors.push({
        path: '',
        error: `GC failed: ${(error as Error).message}`,
      });
    }

    result.durationMs = Date.now() - startTime;
    return result;
  }

  // ===========================================================================
  // Blob Scanning
  // ===========================================================================

  /**
   * Find all unreferenced blobs
   */
  async findUnreferencedBlobs(options: Required<GCOptions>): Promise<{
    unreferenced: BlobInfo[];
    scanned: number;
  }> {
    const unreferenced: BlobInfo[] = [];
    let scanned = 0;

    // List all blob metadata files
    const blobMetaPaths = await this.storage.list(BLOB_PREFIX);

    // Filter to only .meta files
    const metaPaths = blobMetaPaths.filter((p) => p.endsWith('.meta'));

    for (const metaPath of metaPaths) {
      scanned++;

      try {
        // Read blob metadata
        const metaData = await this.storage.read(metaPath);
        if (!metaData) continue;

        const ref = JSON.parse(new TextDecoder().decode(metaData)) as BlobRef;

        // Check if unreferenced
        if (ref.refCount <= 0) {
          // Check age threshold
          if (options.olderThan > 0) {
            const age = Date.now() - ref.modifiedAt;
            if (age < options.olderThan) continue;
          }

          // Check branch filter
          if (options.branches.length > 0) {
            if (!options.branches.includes(ref.branch)) continue;
          }

          // Extract blob path from meta path
          const blobPath = metaPath.slice(0, -5); // Remove '.meta'

          unreferenced.push({
            path: blobPath,
            metaPath,
            size: ref.size,
            hash: ref.hash,
            modifiedAt: ref.modifiedAt,
            refCount: ref.refCount,
          });
        }
      } catch {
        // Skip invalid metadata
      }
    }

    return { unreferenced, scanned };
  }

  /**
   * Build reference count map for all blobs
   */
  async buildRefCountMap(): Promise<Map<string, number>> {
    const refCounts = new Map<string, number>();

    // Get all branches
    const branches = await this.cow.listBranches();

    for (const branch of branches) {
      // Get all paths in branch
      const paths = await this.cow.listFrom('', branch.name);

      for (const path of paths) {
        const ref = await this.cow.getRefInternal(path, branch.name);
        if (!ref?.hash) continue;

        const blobPath = this.getBlobPath(ref.hash);
        const current = refCounts.get(blobPath) ?? 0;
        refCounts.set(blobPath, current + 1);
      }
    }

    // Also count references from snapshots
    for (const branch of branches) {
      const snapshots = await this.cow.listSnapshots(branch.name);

      for (const snapshot of snapshots) {
        for (const entry of snapshot.manifest.entries) {
          if (!entry.hash) continue;

          const blobPath = this.getBlobPath(entry.hash);
          const current = refCounts.get(blobPath) ?? 0;
          refCounts.set(blobPath, current + 1);
        }
      }
    }

    return refCounts;
  }

  /**
   * Verify and repair reference counts
   */
  async verifyAndRepairRefCounts(): Promise<RefCountRepairResult> {
    const result: RefCountRepairResult = {
      verified: 0,
      repaired: 0,
      errors: [],
    };

    // Build accurate ref count map
    const accurateCounts = await this.buildRefCountMap();

    // List all blob metadata
    const blobMetaPaths = await this.storage.list(BLOB_PREFIX);
    const metaPaths = blobMetaPaths.filter((p) => p.endsWith('.meta'));

    for (const metaPath of metaPaths) {
      try {
        const metaData = await this.storage.read(metaPath);
        if (!metaData) continue;

        const ref = JSON.parse(new TextDecoder().decode(metaData)) as BlobRef;
        const blobPath = metaPath.slice(0, -5);

        const accurateCount = accurateCounts.get(blobPath) ?? 0;

        if (ref.refCount !== accurateCount) {
          // Repair ref count
          ref.refCount = accurateCount;
          await this.storage.write(metaPath, new TextEncoder().encode(JSON.stringify(ref)));
          result.repaired++;
        } else {
          result.verified++;
        }
      } catch (error) {
        result.errors.push({
          path: metaPath,
          error: (error as Error).message,
        });
      }
    }

    return result;
  }

  // ===========================================================================
  // Blob Deletion
  // ===========================================================================

  /**
   * Delete a blob and its metadata
   */
  async deleteBlob(blob: BlobInfo): Promise<void> {
    // Delete blob data
    await this.storage.delete(blob.path);

    // Delete metadata
    await this.storage.delete(blob.metaPath);
  }

  /**
   * Delete multiple blobs in batch
   */
  async deleteBlobs(blobs: BlobInfo[]): Promise<number> {
    let deleted = 0;

    for (const blob of blobs) {
      try {
        await this.deleteBlob(blob);
        deleted++;
      } catch {
        // Continue on error
      }
    }

    return deleted;
  }

  // ===========================================================================
  // Orphan Detection
  // ===========================================================================

  /**
   * Find orphaned blobs (blobs with no metadata)
   */
  async findOrphanedBlobs(): Promise<string[]> {
    const orphaned: string[] = [];

    // List all blob files (non-meta)
    const allPaths = await this.storage.list(BLOB_PREFIX);
    const blobPaths = allPaths.filter((p) => !p.endsWith('.meta'));

    for (const blobPath of blobPaths) {
      const metaPath = blobPath + '.meta';
      const metaExists = await this.storage.exists(metaPath);

      if (!metaExists) {
        orphaned.push(blobPath);
      }
    }

    return orphaned;
  }

  /**
   * Find orphaned metadata (metadata with no blob)
   */
  async findOrphanedMetadata(): Promise<string[]> {
    const orphaned: string[] = [];

    const allPaths = await this.storage.list(BLOB_PREFIX);
    const metaPaths = allPaths.filter((p) => p.endsWith('.meta'));

    for (const metaPath of metaPaths) {
      const blobPath = metaPath.slice(0, -5);
      const blobExists = await this.storage.exists(blobPath);

      if (!blobExists) {
        orphaned.push(metaPath);
      }
    }

    return orphaned;
  }

  /**
   * Clean up orphaned files
   */
  async cleanupOrphans(): Promise<{ blobs: number; metadata: number }> {
    const [orphanedBlobs, orphanedMeta] = await Promise.all([
      this.findOrphanedBlobs(),
      this.findOrphanedMetadata(),
    ]);

    let deletedBlobs = 0;
    let deletedMeta = 0;

    for (const path of orphanedBlobs) {
      try {
        await this.storage.delete(path);
        deletedBlobs++;
      } catch {
        // Continue on error
      }
    }

    for (const path of orphanedMeta) {
      try {
        await this.storage.delete(path);
        deletedMeta++;
      } catch {
        // Continue on error
      }
    }

    return { blobs: deletedBlobs, metadata: deletedMeta };
  }

  // ===========================================================================
  // Storage Statistics
  // ===========================================================================

  /**
   * Get blob storage statistics
   */
  async getStorageStats(): Promise<StorageStats> {
    const stats: StorageStats = {
      totalBlobs: 0,
      totalSize: 0,
      unreferencedBlobs: 0,
      unreferencedSize: 0,
      orphanedBlobs: 0,
      orphanedMetadata: 0,
      sizeByRefCount: new Map(),
    };

    // Count blobs and sizes
    const allPaths = await this.storage.list(BLOB_PREFIX);
    const metaPaths = allPaths.filter((p) => p.endsWith('.meta'));

    for (const metaPath of metaPaths) {
      try {
        const metaData = await this.storage.read(metaPath);
        if (!metaData) continue;

        const ref = JSON.parse(new TextDecoder().decode(metaData)) as BlobRef;

        stats.totalBlobs++;
        stats.totalSize += ref.size;

        // Track by ref count
        const current = stats.sizeByRefCount.get(ref.refCount) ?? 0;
        stats.sizeByRefCount.set(ref.refCount, current + ref.size);

        if (ref.refCount <= 0) {
          stats.unreferencedBlobs++;
          stats.unreferencedSize += ref.size;
        }
      } catch {
        // Skip invalid metadata
      }
    }

    // Count orphans
    const [orphanedBlobs, orphanedMeta] = await Promise.all([
      this.findOrphanedBlobs(),
      this.findOrphanedMetadata(),
    ]);

    stats.orphanedBlobs = orphanedBlobs.length;
    stats.orphanedMetadata = orphanedMeta.length;

    return stats;
  }

  // ===========================================================================
  // Path Helpers
  // ===========================================================================

  private getBlobPath(hash: string): string {
    return `${BLOB_PREFIX}${hash.slice(0, 2)}/${hash}`;
  }
}

// =============================================================================
// Types
// =============================================================================

/**
 * Information about a blob candidate for deletion
 */
interface BlobInfo {
  path: string;
  metaPath: string;
  size: number;
  hash?: string;
  modifiedAt: number;
  refCount: number;
}

/**
 * Result of reference count repair
 */
interface RefCountRepairResult {
  verified: number;
  repaired: number;
  errors: Array<{ path: string; error: string }>;
}

/**
 * Storage statistics
 */
interface StorageStats {
  totalBlobs: number;
  totalSize: number;
  unreferencedBlobs: number;
  unreferencedSize: number;
  orphanedBlobs: number;
  orphanedMetadata: number;
  sizeByRefCount: Map<number, number>;
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Format bytes to human readable string
 */
export function formatBytes(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let unitIndex = 0;
  let value = bytes;

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex++;
  }

  return `${value.toFixed(2)} ${units[unitIndex]}`;
}

/**
 * Format GC result for logging
 */
export function formatGCResult(result: GCResult): string {
  const lines = [
    `GC completed in ${result.durationMs}ms`,
    `  Blobs scanned: ${result.blobsScanned}`,
    `  Blobs deleted: ${result.blobsDeleted}`,
    `  Bytes reclaimed: ${formatBytes(result.bytesReclaimed)}`,
  ];

  if (result.errors.length > 0) {
    lines.push(`  Errors: ${result.errors.length}`);
    for (const err of result.errors.slice(0, 5)) {
      lines.push(`    - ${err.path}: ${err.error}`);
    }
    if (result.errors.length > 5) {
      lines.push(`    ... and ${result.errors.length - 5} more`);
    }
  }

  return lines.join('\n');
}

/**
 * Create a GC schedule based on storage usage
 */
export function suggestGCSchedule(stats: StorageStats): GCScheduleSuggestion {
  const unreferencedRatio = stats.unreferencedSize / (stats.totalSize || 1);
  const orphanCount = stats.orphanedBlobs + stats.orphanedMetadata;

  if (unreferencedRatio > 0.3 || orphanCount > 100) {
    return {
      urgency: 'high',
      recommended: 'immediate',
      reason: 'High amount of unreferenced data or orphans',
    };
  }

  if (unreferencedRatio > 0.1 || orphanCount > 10) {
    return {
      urgency: 'medium',
      recommended: 'daily',
      reason: 'Moderate cleanup needed',
    };
  }

  return {
    urgency: 'low',
    recommended: 'weekly',
    reason: 'Storage is healthy',
  };
}

interface GCScheduleSuggestion {
  urgency: 'low' | 'medium' | 'high';
  recommended: 'immediate' | 'daily' | 'weekly';
  reason: string;
}
