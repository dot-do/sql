/**
 * Compaction Cleaner for DoSQL
 *
 * Handles cleanup after compaction:
 * - Deletes compacted rows from B-tree
 * - Updates manifest with new row groups
 * - Verifies durability before deletion
 */

import type { BTree, KeyCodec } from '../btree/types.js';
import type { FSXBackend } from '../fsx/types.js';
import type { ColumnarTableSchema, RowGroup } from '../columnar/types.js';
import type {
  CompactionConfig,
  CleanupOptions,
  CleanupResult,
  CompactionManifest,
  RowGroupManifestEntry,
} from './types.js';
import { CompactionError, CompactionErrorCode } from './types.js';

// =============================================================================
// Manifest Management
// =============================================================================

/**
 * Storage path for the manifest
 */
function manifestPath(config: CompactionConfig, tableName: string): string {
  return `${config.metadataPrefix}manifest/${tableName}.json`;
}

/**
 * Load the compaction manifest
 */
export async function loadManifest(
  fsx: FSXBackend,
  config: CompactionConfig,
  tableName: string
): Promise<CompactionManifest | null> {
  const data = await fsx.read(manifestPath(config, tableName));
  if (!data) return null;

  try {
    const manifest = JSON.parse(new TextDecoder().decode(data)) as CompactionManifest;

    // Convert bigint LSN values back from strings
    if (manifest.lastCompactedLSN !== undefined) {
      manifest.lastCompactedLSN = BigInt(manifest.lastCompactedLSN as unknown as string);
    }

    for (const entry of manifest.rowGroups) {
      if (entry.lsnRange) {
        entry.lsnRange = [
          BigInt(entry.lsnRange[0] as unknown as string),
          BigInt(entry.lsnRange[1] as unknown as string),
        ];
      }
    }

    return manifest;
  } catch (error) {
    throw new CompactionError(
      CompactionErrorCode.MANIFEST_FAILED,
      `Failed to parse manifest: ${error instanceof Error ? error.message : 'Unknown error'}`,
      undefined,
      error instanceof Error ? error : undefined
    );
  }
}

/**
 * Save the compaction manifest
 */
export async function saveManifest(
  fsx: FSXBackend,
  config: CompactionConfig,
  manifest: CompactionManifest
): Promise<void> {
  // Prepare manifest for JSON serialization (convert bigints to strings)
  const serializable = {
    ...manifest,
    lastCompactedLSN: manifest.lastCompactedLSN?.toString(),
    rowGroups: manifest.rowGroups.map((entry) => ({
      ...entry,
      lsnRange: entry.lsnRange
        ? [entry.lsnRange[0].toString(), entry.lsnRange[1].toString()]
        : undefined,
    })),
  };

  const data = new TextEncoder().encode(JSON.stringify(serializable, null, 2));

  try {
    await fsx.write(manifestPath(config, manifest.tableName), data);
  } catch (error) {
    throw new CompactionError(
      CompactionErrorCode.MANIFEST_FAILED,
      `Failed to save manifest: ${error instanceof Error ? error.message : 'Unknown error'}`,
      undefined,
      error instanceof Error ? error : undefined
    );
  }
}

/**
 * Create an empty manifest
 */
export function createEmptyManifest(
  tableName: string,
  schema: ColumnarTableSchema
): CompactionManifest {
  return {
    version: 1,
    tableName,
    schema,
    rowGroups: [],
    totalRows: 0,
    totalBytes: 0,
    updatedAt: Date.now(),
  };
}

/**
 * Add row groups to the manifest
 */
export function addToManifest(
  manifest: CompactionManifest,
  entries: RowGroupManifestEntry[]
): CompactionManifest {
  const newTotalRows = entries.reduce((sum, e) => sum + e.rowCount, manifest.totalRows);
  const newTotalBytes = entries.reduce((sum, e) => sum + e.byteSize, manifest.totalBytes);

  // Find max LSN from new entries
  let maxLSN = manifest.lastCompactedLSN;
  for (const entry of entries) {
    if (entry.lsnRange) {
      if (maxLSN === undefined || entry.lsnRange[1] > maxLSN) {
        maxLSN = entry.lsnRange[1];
      }
    }
  }

  return {
    ...manifest,
    rowGroups: [...manifest.rowGroups, ...entries],
    totalRows: newTotalRows,
    totalBytes: newTotalBytes,
    lastCompactedLSN: maxLSN,
    updatedAt: Date.now(),
  };
}

// =============================================================================
// Durability Verification
// =============================================================================

/**
 * Verify that columnar chunks are durable before cleanup
 */
export async function verifyDurability(
  fsx: FSXBackend,
  storagePaths: string[]
): Promise<{ durable: boolean; missing: string[] }> {
  const missing: string[] = [];

  for (const path of storagePaths) {
    const exists = await fsx.exists(path);
    if (!exists) {
      missing.push(path);
    }
  }

  return {
    durable: missing.length === 0,
    missing,
  };
}

// =============================================================================
// Cleaner Implementation
// =============================================================================

/**
 * Cleaner for post-compaction operations
 */
export class CompactionCleaner<K> {
  private readonly btree: BTree<K, unknown>;
  private readonly fsx: FSXBackend;
  private readonly keyCodec: KeyCodec<K>;
  private readonly config: CompactionConfig;
  private readonly schema: ColumnarTableSchema;

  constructor(
    btree: BTree<K, unknown>,
    fsx: FSXBackend,
    keyCodec: KeyCodec<K>,
    config: CompactionConfig,
    schema: ColumnarTableSchema
  ) {
    this.btree = btree;
    this.fsx = fsx;
    this.keyCodec = keyCodec;
    this.config = config;
    this.schema = schema;
  }

  /**
   * Perform cleanup after compaction
   */
  async cleanup(
    keys: K[],
    rowGroups: RowGroup[],
    storagePaths: string[],
    jobId: string,
    lsnRange?: [bigint, bigint],
    options: Partial<CleanupOptions> = {}
  ): Promise<CleanupResult> {
    const startTime = Date.now();
    const { verifyDurability: shouldVerify = true, updateManifest = true } = options;

    // Step 1: Verify durability of columnar chunks
    if (shouldVerify) {
      const durabilityCheck = await verifyDurability(this.fsx, storagePaths);
      if (!durabilityCheck.durable) {
        throw new CompactionError(
          CompactionErrorCode.DURABILITY_CHECK_FAILED,
          `Columnar chunks not durable: ${durabilityCheck.missing.join(', ')}`,
          jobId
        );
      }
    }

    // Step 2: Update manifest
    let manifestUpdated = false;
    if (updateManifest && rowGroups.length > 0) {
      await this.updateManifest(rowGroups, storagePaths, jobId, lsnRange);
      manifestUpdated = true;
    }

    // Step 3: Delete rows from B-tree
    const { deletedCount, failed } = await this.deleteRows(keys);

    // Step 4: Clean up row metadata
    await this.cleanupMetadata(keys);

    return {
      deletedRows: deletedCount,
      failedDeletes: failed,
      manifestUpdated,
      durationMs: Date.now() - startTime,
    };
  }

  /**
   * Delete rows from B-tree
   */
  private async deleteRows(keys: K[]): Promise<{ deletedCount: number; failed: K[] }> {
    let deletedCount = 0;
    const failed: K[] = [];

    for (const key of keys) {
      try {
        const deleted = await this.btree.delete(key);
        if (deleted) {
          deletedCount++;
        }
      } catch (error) {
        failed.push(key);
      }
    }

    return { deletedCount, failed };
  }

  /**
   * Update the manifest with new row groups
   */
  private async updateManifest(
    rowGroups: RowGroup[],
    storagePaths: string[],
    jobId: string,
    lsnRange?: [bigint, bigint]
  ): Promise<void> {
    // Load existing manifest or create new one
    let manifest = await loadManifest(
      this.fsx,
      this.config,
      this.schema.tableName
    );

    if (!manifest) {
      manifest = createEmptyManifest(this.schema.tableName, this.schema);
    }

    // Create manifest entries
    const entries: RowGroupManifestEntry[] = rowGroups.map((rowGroup, i) => ({
      id: rowGroup.id,
      storagePath: storagePaths[i],
      rowCount: rowGroup.rowCount,
      byteSize: rowGroup.byteSize ?? 0,
      lsnRange,
      createdAt: rowGroup.createdAt,
      jobId,
    }));

    // Update and save manifest
    const updatedManifest = addToManifest(manifest, entries);
    await saveManifest(this.fsx, this.config, updatedManifest);
  }

  /**
   * Clean up row metadata for deleted rows
   */
  private async cleanupMetadata(keys: K[]): Promise<void> {
    for (const key of keys) {
      const keyStr = this.keyToString(key);
      const metaPath = `${this.config.metadataPrefix}meta/${keyStr}`;

      try {
        await this.fsx.delete(metaPath);
      } catch {
        // Ignore errors - metadata cleanup is best-effort
      }
    }
  }

  /**
   * Convert a key to string for metadata path
   */
  private keyToString(key: K): string {
    const encoded = this.keyCodec.encode(key);
    return btoa(String.fromCharCode(...encoded));
  }

  /**
   * Get the current manifest
   */
  async getManifest(): Promise<CompactionManifest | null> {
    return loadManifest(this.fsx, this.config, this.schema.tableName);
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a compaction cleaner
 */
export function createCleaner<K>(
  btree: BTree<K, unknown>,
  fsx: FSXBackend,
  keyCodec: KeyCodec<K>,
  config: CompactionConfig,
  schema: ColumnarTableSchema
): CompactionCleaner<K> {
  return new CompactionCleaner(btree, fsx, keyCodec, config, schema);
}

// =============================================================================
// Garbage Collection
// =============================================================================

/**
 * Options for garbage collection
 */
export interface GCOptions {
  /** Keep row groups created after this timestamp */
  keepAfter?: number;

  /** Minimum number of row groups to keep */
  minRowGroups?: number;

  /** Maximum number of row groups to delete in one run */
  maxDeletesPerRun?: number;

  /** Dry run - only report what would be deleted */
  dryRun?: boolean;
}

/**
 * Result of garbage collection
 */
export interface GCResult {
  /** Number of row groups deleted */
  deleted: number;

  /** Paths of deleted row groups */
  deletedPaths: string[];

  /** Bytes freed */
  bytesFreed: number;

  /** Row groups that would be deleted (dry run) */
  wouldDelete?: string[];
}

/**
 * Garbage collect old row groups
 */
export async function garbageCollect(
  fsx: FSXBackend,
  config: CompactionConfig,
  tableName: string,
  schema: ColumnarTableSchema,
  options: GCOptions = {}
): Promise<GCResult> {
  const {
    keepAfter = 0,
    minRowGroups = 3,
    maxDeletesPerRun = 10,
    dryRun = false,
  } = options;

  const manifest = await loadManifest(fsx, config, tableName);
  if (!manifest) {
    return { deleted: 0, deletedPaths: [], bytesFreed: 0 };
  }

  // Sort by creation time (oldest first)
  const sorted = [...manifest.rowGroups].sort((a, b) => a.createdAt - b.createdAt);

  // Determine which row groups to keep
  const toDelete: RowGroupManifestEntry[] = [];

  for (let i = 0; i < sorted.length; i++) {
    const entry = sorted[i];

    // Keep if created after threshold
    if (entry.createdAt >= keepAfter) continue;

    // Keep minimum number of row groups
    if (sorted.length - toDelete.length <= minRowGroups) continue;

    // Respect max deletes per run
    if (toDelete.length >= maxDeletesPerRun) break;

    toDelete.push(entry);
  }

  if (dryRun) {
    return {
      deleted: 0,
      deletedPaths: [],
      bytesFreed: 0,
      wouldDelete: toDelete.map((e) => e.storagePath),
    };
  }

  // Perform deletion
  const deletedPaths: string[] = [];
  let bytesFreed = 0;

  for (const entry of toDelete) {
    try {
      await fsx.delete(entry.storagePath);
      deletedPaths.push(entry.storagePath);
      bytesFreed += entry.byteSize;
    } catch {
      // Continue on error
    }
  }

  // Update manifest to remove deleted row groups
  const remainingRowGroups = manifest.rowGroups.filter(
    (rg) => !deletedPaths.includes(rg.storagePath)
  );

  const updatedManifest: CompactionManifest = {
    ...manifest,
    rowGroups: remainingRowGroups,
    totalRows: remainingRowGroups.reduce((sum, rg) => sum + rg.rowCount, 0),
    totalBytes: remainingRowGroups.reduce((sum, rg) => sum + rg.byteSize, 0),
    updatedAt: Date.now(),
  };

  await saveManifest(fsx, config, updatedManifest);

  return {
    deleted: deletedPaths.length,
    deletedPaths,
    bytesFreed,
  };
}
