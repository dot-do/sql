/**
 * Snapshot Management for COW FSX
 *
 * Handles creation, retrieval, and management of immutable point-in-time
 * snapshots of branches. Snapshots capture the state of all blobs at a
 * specific version.
 *
 * Key features:
 * - Immutable once created
 * - Manifest tracks all blob paths and versions
 * - Enables time-travel reads
 * - Supports snapshot-based branching
 */

import type { FSXBackend } from './types.js';
import {
  type Snapshot,
  type SnapshotId,
  type SnapshotManifest,
  type ManifestEntry,
  COWError,
  COWErrorCode,
  SNAPSHOT_PREFIX,
  makeSnapshotId,
  parseSnapshotId,
} from './cow-types.js';

// =============================================================================
// Snapshot Manager
// =============================================================================

/**
 * Manages snapshot lifecycle for COW backend
 */
export class SnapshotManager {
  private readonly storage: FSXBackend;

  // In-memory cache of snapshots
  private snapshotCache = new Map<SnapshotId, Snapshot>();

  constructor(storage: FSXBackend) {
    this.storage = storage;
  }

  // ===========================================================================
  // Snapshot Creation
  // ===========================================================================

  /**
   * Create a new snapshot
   * @param branch - Branch name
   * @param version - Version number at snapshot time
   * @param manifest - Manifest of all blobs at snapshot time
   * @param message - Optional snapshot message
   */
  async createSnapshot(
    branch: string,
    version: number,
    manifest: SnapshotManifest,
    message?: string
  ): Promise<SnapshotId> {
    const id = makeSnapshotId(branch, version);

    // Check if snapshot already exists
    const existing = await this.getSnapshot(id);
    if (existing) {
      // Snapshot already exists - return existing
      return id;
    }

    const snapshot: Snapshot = {
      id,
      branch,
      version,
      createdAt: Date.now(),
      message,
      manifest,
    };

    // Store snapshot metadata
    const path = this.getSnapshotPath(branch, version);
    const data = new TextEncoder().encode(JSON.stringify(snapshot));
    await this.storage.write(path, data);

    // Update cache
    this.snapshotCache.set(id, snapshot);

    return id;
  }

  // ===========================================================================
  // Snapshot Retrieval
  // ===========================================================================

  /**
   * Get snapshot by ID
   */
  async getSnapshot(id: SnapshotId): Promise<Snapshot | null> {
    // Check cache
    const cached = this.snapshotCache.get(id);
    if (cached) return cached;

    // Parse ID to get path
    const { branch, version } = parseSnapshotId(id);
    const path = this.getSnapshotPath(branch, version);

    const data = await this.storage.read(path);
    if (!data) return null;

    const snapshot = JSON.parse(new TextDecoder().decode(data)) as Snapshot;

    // Update cache
    this.snapshotCache.set(id, snapshot);

    return snapshot;
  }

  /**
   * List all snapshots for a branch
   */
  async listSnapshots(branch: string): Promise<Snapshot[]> {
    const prefix = `${SNAPSHOT_PREFIX}${branch}/`;
    const keys = await this.storage.list(prefix);

    const snapshots: Snapshot[] = [];

    for (const key of keys) {
      // Extract version from path
      const versionStr = key.slice(prefix.length);
      const version = parseInt(versionStr, 10);

      if (isNaN(version)) continue;

      const id = makeSnapshotId(branch, version);
      const snapshot = await this.getSnapshot(id);

      if (snapshot) {
        snapshots.push(snapshot);
      }
    }

    // Sort by version descending (newest first)
    return snapshots.sort((a, b) => b.version - a.version);
  }

  /**
   * Get the latest snapshot for a branch
   */
  async getLatestSnapshot(branch: string): Promise<Snapshot | null> {
    const snapshots = await this.listSnapshots(branch);
    return snapshots[0] ?? null;
  }

  /**
   * Get snapshot at or before a specific version
   */
  async getSnapshotAtVersion(branch: string, version: number): Promise<Snapshot | null> {
    const snapshots = await this.listSnapshots(branch);

    // Find the latest snapshot at or before the requested version
    for (const snapshot of snapshots) {
      if (snapshot.version <= version) {
        return snapshot;
      }
    }

    return null;
  }

  // ===========================================================================
  // Snapshot Deletion
  // ===========================================================================

  /**
   * Delete a snapshot
   */
  async deleteSnapshot(id: SnapshotId): Promise<void> {
    const { branch, version } = parseSnapshotId(id);
    const path = this.getSnapshotPath(branch, version);

    await this.storage.delete(path);
    this.snapshotCache.delete(id);
  }

  /**
   * Delete all snapshots for a branch
   */
  async deleteAllSnapshots(branch: string): Promise<number> {
    const snapshots = await this.listSnapshots(branch);

    for (const snapshot of snapshots) {
      await this.deleteSnapshot(snapshot.id);
    }

    return snapshots.length;
  }

  /**
   * Delete snapshots older than a certain age
   */
  async deleteOldSnapshots(branch: string, olderThanMs: number): Promise<number> {
    const snapshots = await this.listSnapshots(branch);
    const cutoff = Date.now() - olderThanMs;
    let deleted = 0;

    for (const snapshot of snapshots) {
      if (snapshot.createdAt < cutoff) {
        await this.deleteSnapshot(snapshot.id);
        deleted++;
      }
    }

    return deleted;
  }

  // ===========================================================================
  // Manifest Operations
  // ===========================================================================

  /**
   * Get manifest entry for a specific path
   */
  getManifestEntry(manifest: SnapshotManifest, path: string): ManifestEntry | undefined {
    return manifest.entries.find((e) => e.path === path);
  }

  /**
   * Check if a path exists in a manifest
   */
  hasPath(manifest: SnapshotManifest, path: string): boolean {
    return manifest.entries.some((e) => e.path === path);
  }

  /**
   * List paths matching a prefix in a manifest
   */
  listPathsWithPrefix(manifest: SnapshotManifest, prefix: string): string[] {
    return manifest.entries
      .filter((e) => e.path.startsWith(prefix))
      .map((e) => e.path)
      .sort();
  }

  /**
   * Compare two manifests
   */
  compareManifests(
    a: SnapshotManifest,
    b: SnapshotManifest
  ): {
    added: ManifestEntry[];
    removed: ManifestEntry[];
    modified: Array<{ path: string; a: ManifestEntry; b: ManifestEntry }>;
    unchanged: ManifestEntry[];
  } {
    const aMap = new Map(a.entries.map((e) => [e.path, e]));
    const bMap = new Map(b.entries.map((e) => [e.path, e]));

    const added: ManifestEntry[] = [];
    const removed: ManifestEntry[] = [];
    const modified: Array<{ path: string; a: ManifestEntry; b: ManifestEntry }> = [];
    const unchanged: ManifestEntry[] = [];

    // Find added and modified in b
    for (const [path, entryB] of bMap) {
      const entryA = aMap.get(path);

      if (!entryA) {
        added.push(entryB);
      } else if (entryA.hash !== entryB.hash || entryA.version !== entryB.version) {
        modified.push({ path, a: entryA, b: entryB });
      } else {
        unchanged.push(entryA);
      }
    }

    // Find removed (in a but not in b)
    for (const [path, entryA] of aMap) {
      if (!bMap.has(path)) {
        removed.push(entryA);
      }
    }

    return { added, removed, modified, unchanged };
  }

  // ===========================================================================
  // Snapshot Comparison
  // ===========================================================================

  /**
   * Compare two snapshots
   */
  async compareSnapshots(
    idA: SnapshotId,
    idB: SnapshotId
  ): Promise<{
    added: ManifestEntry[];
    removed: ManifestEntry[];
    modified: Array<{ path: string; a: ManifestEntry; b: ManifestEntry }>;
    unchanged: ManifestEntry[];
  }> {
    const [snapshotA, snapshotB] = await Promise.all([
      this.getSnapshot(idA),
      this.getSnapshot(idB),
    ]);

    if (!snapshotA) {
      throw new COWError(COWErrorCode.SNAPSHOT_NOT_FOUND, `Snapshot not found: ${idA}`);
    }
    if (!snapshotB) {
      throw new COWError(COWErrorCode.SNAPSHOT_NOT_FOUND, `Snapshot not found: ${idB}`);
    }

    return this.compareManifests(snapshotA.manifest, snapshotB.manifest);
  }

  // ===========================================================================
  // Snapshot Statistics
  // ===========================================================================

  /**
   * Get statistics for a snapshot
   */
  getSnapshotStats(snapshot: Snapshot): {
    totalFiles: number;
    totalSize: number;
    averageFileSize: number;
    largestFile: ManifestEntry | null;
    smallestFile: ManifestEntry | null;
  } {
    const { entries, totalSize, count } = snapshot.manifest;

    let largestFile: ManifestEntry | null = null;
    let smallestFile: ManifestEntry | null = null;

    for (const entry of entries) {
      if (!largestFile || entry.size > largestFile.size) {
        largestFile = entry;
      }
      if (!smallestFile || entry.size < smallestFile.size) {
        smallestFile = entry;
      }
    }

    return {
      totalFiles: count,
      totalSize,
      averageFileSize: count > 0 ? Math.round(totalSize / count) : 0,
      largestFile,
      smallestFile,
    };
  }

  /**
   * Get storage statistics across all snapshots for a branch
   */
  async getBranchSnapshotStats(branch: string): Promise<{
    snapshotCount: number;
    totalManifestEntries: number;
    oldestSnapshot: Snapshot | null;
    newestSnapshot: Snapshot | null;
    uniquePaths: number;
  }> {
    const snapshots = await this.listSnapshots(branch);

    if (snapshots.length === 0) {
      return {
        snapshotCount: 0,
        totalManifestEntries: 0,
        oldestSnapshot: null,
        newestSnapshot: null,
        uniquePaths: 0,
      };
    }

    const allPaths = new Set<string>();
    let totalManifestEntries = 0;

    for (const snapshot of snapshots) {
      totalManifestEntries += snapshot.manifest.count;
      for (const entry of snapshot.manifest.entries) {
        allPaths.add(entry.path);
      }
    }

    // Snapshots are sorted newest first
    return {
      snapshotCount: snapshots.length,
      totalManifestEntries,
      oldestSnapshot: snapshots[snapshots.length - 1],
      newestSnapshot: snapshots[0],
      uniquePaths: allPaths.size,
    };
  }

  // ===========================================================================
  // Path Helpers
  // ===========================================================================

  private getSnapshotPath(branch: string, version: number): string {
    // Pad version for proper lexicographic sorting
    const paddedVersion = version.toString().padStart(10, '0');
    return `${SNAPSHOT_PREFIX}${branch}/${paddedVersion}`;
  }

  // ===========================================================================
  // Cache Management
  // ===========================================================================

  /**
   * Clear the snapshot cache
   */
  clearCache(): void {
    this.snapshotCache.clear();
  }

  /**
   * Get cache statistics
   */
  getCacheStats(): { size: number; branches: string[] } {
    const branches = new Set<string>();

    for (const id of this.snapshotCache.keys()) {
      const { branch } = parseSnapshotId(id);
      branches.add(branch);
    }

    return {
      size: this.snapshotCache.size,
      branches: [...branches],
    };
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Create an empty manifest
 */
export function createEmptyManifest(): SnapshotManifest {
  return {
    entries: [],
    totalSize: 0,
    count: 0,
  };
}

/**
 * Build a manifest from an array of entries
 */
export function buildManifest(entries: ManifestEntry[]): SnapshotManifest {
  const totalSize = entries.reduce((sum, e) => sum + e.size, 0);

  return {
    entries: [...entries],
    totalSize,
    count: entries.length,
  };
}

/**
 * Merge two manifests (b overwrites a)
 */
export function mergeManifests(a: SnapshotManifest, b: SnapshotManifest): SnapshotManifest {
  const merged = new Map<string, ManifestEntry>();

  // Add all from a
  for (const entry of a.entries) {
    merged.set(entry.path, entry);
  }

  // Overwrite with b
  for (const entry of b.entries) {
    merged.set(entry.path, entry);
  }

  return buildManifest([...merged.values()]);
}

/**
 * Filter manifest to only include paths matching prefix
 */
export function filterManifest(manifest: SnapshotManifest, prefix: string): SnapshotManifest {
  const filtered = manifest.entries.filter((e) => e.path.startsWith(prefix));
  return buildManifest(filtered);
}
