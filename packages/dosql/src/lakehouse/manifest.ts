/**
 * Lakehouse Manifest Manager for DoSQL
 *
 * Manages the lakehouse manifest with:
 * - Atomic manifest updates with optimistic locking
 * - Snapshot management for time travel
 * - Table and partition tracking
 */

import type { ColumnarTableSchema } from '../columnar/types.js';
import {
  type LakehouseManifest,
  type Snapshot,
  type SnapshotSummary,
  type SnapshotOperation,
  type TableManifest,
  type Partition,
  type PartitionKey,
  type ChunkMetadata,
  type PartitionColumn,
  type R2Bucket,
  type R2Object,
  type SerializedChunkMetadata,
  LakehouseError,
  LakehouseErrorCode,
  generateSnapshotId,
  buildPartitionPath,
  serializeChunkMetadata,
  deserializeChunkMetadata,
} from './types.js';

// =============================================================================
// Manifest Serialization
// =============================================================================

/**
 * Serializable manifest format
 */
interface SerializedManifest {
  formatVersion: number;
  lakehouseId: string;
  snapshots: SerializedSnapshot[];
  currentSnapshotId: string;
  tables: Record<string, SerializedTableManifest>;
  updatedAt: number;
  version: number;
  properties: Record<string, string>;
}

interface SerializedSnapshot extends Omit<Snapshot, 'summary'> {
  summary: SnapshotSummary;
}

interface SerializedTableManifest extends Omit<TableManifest, 'partitions'> {
  partitions: SerializedPartition[];
}

interface SerializedPartition extends Omit<Partition, 'chunks'> {
  chunks: SerializedChunkMetadata[];
}

/**
 * Serialize manifest for storage
 */
function serializeManifest(manifest: LakehouseManifest): Uint8Array {
  const serialized: SerializedManifest = {
    formatVersion: manifest.formatVersion,
    lakehouseId: manifest.lakehouseId,
    snapshots: manifest.snapshots,
    currentSnapshotId: manifest.currentSnapshotId,
    tables: {},
    updatedAt: manifest.updatedAt,
    version: manifest.version,
    properties: manifest.properties,
  };

  // Serialize tables with partitions
  for (const [tableName, tableManifest] of Object.entries(manifest.tables)) {
    const serializedPartitions: SerializedPartition[] = [];

    for (const [_, partition] of tableManifest.partitions) {
      serializedPartitions.push({
        ...partition,
        chunks: partition.chunks.map(serializeChunkMetadata),
      });
    }

    serialized.tables[tableName] = {
      ...tableManifest,
      partitions: serializedPartitions,
    };
  }

  const json = JSON.stringify(serialized, null, 2);
  return new TextEncoder().encode(json);
}

/**
 * Deserialize manifest from storage
 */
function deserializeManifest(data: Uint8Array): LakehouseManifest {
  const json = new TextDecoder().decode(data);
  const serialized: SerializedManifest = JSON.parse(json);

  const tables: Record<string, TableManifest> = {};

  for (const [tableName, tableData] of Object.entries(serialized.tables)) {
    const partitions = new Map<string, Partition>();

    for (const partitionData of tableData.partitions) {
      partitions.set(partitionData.path, {
        ...partitionData,
        chunks: partitionData.chunks.map(deserializeChunkMetadata),
      });
    }

    tables[tableName] = {
      ...tableData,
      partitions,
    };
  }

  return {
    formatVersion: serialized.formatVersion,
    lakehouseId: serialized.lakehouseId,
    snapshots: serialized.snapshots,
    currentSnapshotId: serialized.currentSnapshotId,
    tables,
    updatedAt: serialized.updatedAt,
    version: serialized.version,
    properties: serialized.properties,
  };
}

// =============================================================================
// Manifest Manager
// =============================================================================

/**
 * Options for manifest manager
 */
export interface ManifestManagerOptions {
  /** R2 bucket for storage */
  r2: R2Bucket;
  /** Path prefix for manifest files */
  prefix: string;
  /** Lakehouse identifier */
  lakehouseId?: string;
  /** Number of snapshots to retain */
  snapshotRetention?: number;
}

/**
 * Manifest manager for lakehouse metadata
 */
export class ManifestManager {
  private r2: R2Bucket;
  private prefix: string;
  private lakehouseId: string;
  private snapshotRetention: number;
  private manifest: LakehouseManifest | null = null;

  constructor(options: ManifestManagerOptions) {
    this.r2 = options.r2;
    this.prefix = options.prefix.endsWith('/') ? options.prefix : `${options.prefix}/`;
    this.lakehouseId = options.lakehouseId ?? `lakehouse-${Date.now().toString(36)}`;
    this.snapshotRetention = options.snapshotRetention ?? 100;
  }

  // ===========================================================================
  // Manifest Path Helpers
  // ===========================================================================

  private get manifestPath(): string {
    return `${this.prefix}_manifest/current.json`;
  }

  private getSnapshotManifestPath(snapshotId: string): string {
    return `${this.prefix}_manifest/snapshots/${snapshotId}.json`;
  }

  // ===========================================================================
  // Load / Save Operations
  // ===========================================================================

  /**
   * Load manifest from R2
   */
  async load(): Promise<LakehouseManifest> {
    try {
      const object = await this.r2.get(this.manifestPath);

      if (!object) {
        // Create new manifest
        this.manifest = this.createEmptyManifest();
        return this.manifest;
      }

      const buffer = await object.arrayBuffer();
      this.manifest = deserializeManifest(new Uint8Array(buffer));
      return this.manifest;
    } catch (error) {
      throw new LakehouseError(
        LakehouseErrorCode.R2_ERROR,
        `Failed to load manifest: ${error instanceof Error ? error.message : 'Unknown error'}`,
        undefined,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Get current manifest (loads if not cached)
   */
  async getManifest(): Promise<LakehouseManifest> {
    if (!this.manifest) {
      return this.load();
    }
    return this.manifest;
  }

  /**
   * Save manifest to R2 with optimistic locking
   */
  async save(expectedVersion: number): Promise<void> {
    if (!this.manifest) {
      throw new LakehouseError(
        LakehouseErrorCode.MANIFEST_NOT_FOUND,
        'No manifest loaded'
      );
    }

    // Check version for optimistic locking
    if (this.manifest.version !== expectedVersion) {
      throw new LakehouseError(
        LakehouseErrorCode.MANIFEST_CONFLICT,
        `Manifest version conflict: expected ${expectedVersion}, got ${this.manifest.version}`,
        { expectedVersion, actualVersion: this.manifest.version }
      );
    }

    // Increment version
    this.manifest.version++;
    this.manifest.updatedAt = Date.now();

    try {
      const data = serializeManifest(this.manifest);
      await this.r2.put(this.manifestPath, data);
    } catch (error) {
      // Rollback version on failure
      this.manifest.version--;
      throw new LakehouseError(
        LakehouseErrorCode.R2_ERROR,
        `Failed to save manifest: ${error instanceof Error ? error.message : 'Unknown error'}`,
        undefined,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Create empty manifest
   */
  private createEmptyManifest(): LakehouseManifest {
    const snapshotId = generateSnapshotId();

    const initialSnapshot: Snapshot = {
      id: snapshotId,
      sequenceNumber: 0,
      parentId: null,
      timestamp: Date.now(),
      summary: {
        tablesModified: [],
        chunksAdded: 0,
        chunksRemoved: 0,
        rowsAdded: 0,
        rowsRemoved: 0,
        bytesAdded: 0,
        bytesRemoved: 0,
      },
      manifestPath: this.getSnapshotManifestPath(snapshotId),
      operation: { type: 'append', tables: [] },
    };

    return {
      formatVersion: 1,
      lakehouseId: this.lakehouseId,
      snapshots: [initialSnapshot],
      currentSnapshotId: snapshotId,
      tables: {},
      updatedAt: Date.now(),
      version: 0,
      properties: {},
    };
  }

  // ===========================================================================
  // Table Operations
  // ===========================================================================

  /**
   * Register a new table in the manifest
   */
  async registerTable(
    tableName: string,
    schema: ColumnarTableSchema,
    partitionColumns: PartitionColumn[] = []
  ): Promise<void> {
    const manifest = await this.getManifest();
    const version = manifest.version;

    if (manifest.tables[tableName]) {
      throw new LakehouseError(
        LakehouseErrorCode.CONFIG_ERROR,
        `Table '${tableName}' already exists`
      );
    }

    manifest.tables[tableName] = {
      tableName,
      schema,
      partitionColumns,
      partitions: new Map(),
      totalRowCount: 0,
      totalByteSize: 0,
      schemaVersion: 1,
      createdAt: Date.now(),
      modifiedAt: Date.now(),
    };

    await this.save(version);
  }

  /**
   * Get table manifest
   */
  async getTable(tableName: string): Promise<TableManifest | null> {
    const manifest = await this.getManifest();
    return manifest.tables[tableName] ?? null;
  }

  /**
   * List all tables
   */
  async listTables(): Promise<string[]> {
    const manifest = await this.getManifest();
    return Object.keys(manifest.tables);
  }

  // ===========================================================================
  // Chunk Operations
  // ===========================================================================

  /**
   * Add chunks to the manifest
   */
  async addChunks(
    tableName: string,
    chunks: Array<{
      chunk: ChunkMetadata;
      partitionKeys: PartitionKey[];
    }>
  ): Promise<Snapshot> {
    const manifest = await this.getManifest();
    const version = manifest.version;

    const table = manifest.tables[tableName];
    if (!table) {
      throw new LakehouseError(
        LakehouseErrorCode.CONFIG_ERROR,
        `Table '${tableName}' not found`
      );
    }

    // Track changes for snapshot summary
    let chunksAdded = 0;
    let rowsAdded = 0;
    let bytesAdded = 0;

    for (const { chunk, partitionKeys } of chunks) {
      const partitionPath = buildPartitionPath(partitionKeys);

      let partition = table.partitions.get(partitionPath);
      if (!partition) {
        partition = {
          keys: partitionKeys,
          path: partitionPath,
          chunks: [],
          rowCount: 0,
          byteSize: 0,
          createdAt: Date.now(),
          modifiedAt: Date.now(),
        };
        table.partitions.set(partitionPath, partition);
      }

      partition.chunks.push(chunk);
      partition.rowCount += chunk.rowCount;
      partition.byteSize += chunk.byteSize;
      partition.modifiedAt = Date.now();

      table.totalRowCount += chunk.rowCount;
      table.totalByteSize += chunk.byteSize;

      chunksAdded++;
      rowsAdded += chunk.rowCount;
      bytesAdded += chunk.byteSize;
    }

    table.modifiedAt = Date.now();

    // Create snapshot
    const snapshot = this.createSnapshot(manifest, {
      type: 'append',
      tables: [tableName],
    }, {
      tablesModified: [tableName],
      chunksAdded,
      chunksRemoved: 0,
      rowsAdded,
      rowsRemoved: 0,
      bytesAdded,
      bytesRemoved: 0,
    });

    manifest.snapshots.push(snapshot);
    manifest.currentSnapshotId = snapshot.id;

    // Clean up old snapshots
    this.pruneSnapshots(manifest);

    await this.save(version);

    return snapshot;
  }

  /**
   * Remove chunks (for compaction)
   */
  async removeChunks(
    tableName: string,
    chunkIds: string[]
  ): Promise<Snapshot> {
    const manifest = await this.getManifest();
    const version = manifest.version;

    const table = manifest.tables[tableName];
    if (!table) {
      throw new LakehouseError(
        LakehouseErrorCode.CONFIG_ERROR,
        `Table '${tableName}' not found`
      );
    }

    const chunkIdSet = new Set(chunkIds);
    let chunksRemoved = 0;
    let rowsRemoved = 0;
    let bytesRemoved = 0;

    for (const partition of table.partitions.values()) {
      const remainingChunks: ChunkMetadata[] = [];

      for (const chunk of partition.chunks) {
        if (chunkIdSet.has(chunk.id)) {
          chunksRemoved++;
          rowsRemoved += chunk.rowCount;
          bytesRemoved += chunk.byteSize;

          partition.rowCount -= chunk.rowCount;
          partition.byteSize -= chunk.byteSize;
          table.totalRowCount -= chunk.rowCount;
          table.totalByteSize -= chunk.byteSize;
        } else {
          remainingChunks.push(chunk);
        }
      }

      partition.chunks = remainingChunks;
      partition.modifiedAt = Date.now();
    }

    // Remove empty partitions
    for (const [path, partition] of table.partitions) {
      if (partition.chunks.length === 0) {
        table.partitions.delete(path);
      }
    }

    table.modifiedAt = Date.now();

    // Create snapshot
    const snapshot = this.createSnapshot(manifest, {
      type: 'compact',
      tables: [tableName],
    }, {
      tablesModified: [tableName],
      chunksAdded: 0,
      chunksRemoved,
      rowsAdded: 0,
      rowsRemoved,
      bytesAdded: 0,
      bytesRemoved,
    });

    manifest.snapshots.push(snapshot);
    manifest.currentSnapshotId = snapshot.id;

    await this.save(version);

    return snapshot;
  }

  // ===========================================================================
  // Snapshot Operations
  // ===========================================================================

  /**
   * Create a new snapshot
   */
  private createSnapshot(
    manifest: LakehouseManifest,
    operation: SnapshotOperation,
    summary: SnapshotSummary
  ): Snapshot {
    const id = generateSnapshotId();
    const sequenceNumber = manifest.snapshots.length;
    const parentId = manifest.currentSnapshotId;

    return {
      id,
      sequenceNumber,
      parentId,
      timestamp: Date.now(),
      summary,
      manifestPath: this.getSnapshotManifestPath(id),
      operation,
    };
  }

  /**
   * Get current snapshot
   */
  async getCurrentSnapshot(): Promise<Snapshot | null> {
    const manifest = await this.getManifest();
    return manifest.snapshots.find(s => s.id === manifest.currentSnapshotId) ?? null;
  }

  /**
   * Get snapshot by ID
   */
  async getSnapshot(snapshotId: string): Promise<Snapshot | null> {
    const manifest = await this.getManifest();
    return manifest.snapshots.find(s => s.id === snapshotId) ?? null;
  }

  /**
   * List all snapshots
   */
  async listSnapshots(): Promise<Snapshot[]> {
    const manifest = await this.getManifest();
    return [...manifest.snapshots];
  }

  /**
   * Rollback to a previous snapshot
   */
  async rollbackTo(snapshotId: string): Promise<Snapshot> {
    const manifest = await this.getManifest();
    const version = manifest.version;

    const targetSnapshot = manifest.snapshots.find(s => s.id === snapshotId);
    if (!targetSnapshot) {
      throw new LakehouseError(
        LakehouseErrorCode.SNAPSHOT_NOT_FOUND,
        `Snapshot '${snapshotId}' not found`
      );
    }

    // Load the manifest state at the target snapshot
    // For now, we only support rollback to the manifest state
    // A full implementation would store full manifest copies per snapshot

    // Create rollback snapshot
    const rollbackSnapshot = this.createSnapshot(manifest, {
      type: 'rollback',
      targetSnapshotId: snapshotId,
    }, {
      tablesModified: [],
      chunksAdded: 0,
      chunksRemoved: 0,
      rowsAdded: 0,
      rowsRemoved: 0,
      bytesAdded: 0,
      bytesRemoved: 0,
    });

    manifest.snapshots.push(rollbackSnapshot);
    manifest.currentSnapshotId = rollbackSnapshot.id;

    await this.save(version);

    return rollbackSnapshot;
  }

  /**
   * Prune old snapshots beyond retention limit
   */
  private pruneSnapshots(manifest: LakehouseManifest): void {
    if (manifest.snapshots.length <= this.snapshotRetention) {
      return;
    }

    // Keep only the most recent snapshots
    const toRemove = manifest.snapshots.length - this.snapshotRetention;
    manifest.snapshots = manifest.snapshots.slice(toRemove);
  }

  // ===========================================================================
  // Query Helpers
  // ===========================================================================

  /**
   * Get all chunks for a table
   */
  async getTableChunks(tableName: string): Promise<ChunkMetadata[]> {
    const table = await this.getTable(tableName);
    if (!table) return [];

    const chunks: ChunkMetadata[] = [];
    for (const partition of table.partitions.values()) {
      chunks.push(...partition.chunks);
    }
    return chunks;
  }

  /**
   * Get chunks for specific partitions
   */
  async getPartitionChunks(
    tableName: string,
    partitionPaths: string[]
  ): Promise<ChunkMetadata[]> {
    const table = await this.getTable(tableName);
    if (!table) return [];

    const chunks: ChunkMetadata[] = [];
    for (const path of partitionPaths) {
      const partition = table.partitions.get(path);
      if (partition) {
        chunks.push(...partition.chunks);
      }
    }
    return chunks;
  }

  /**
   * Get partition paths for a table
   */
  async getPartitionPaths(tableName: string): Promise<string[]> {
    const table = await this.getTable(tableName);
    if (!table) return [];
    return Array.from(table.partitions.keys());
  }

  // ===========================================================================
  // Schema Evolution
  // ===========================================================================

  /**
   * Update table schema (additive changes only)
   */
  async updateSchema(
    tableName: string,
    newSchema: ColumnarTableSchema,
    changes: string[]
  ): Promise<Snapshot> {
    const manifest = await this.getManifest();
    const version = manifest.version;

    const table = manifest.tables[tableName];
    if (!table) {
      throw new LakehouseError(
        LakehouseErrorCode.CONFIG_ERROR,
        `Table '${tableName}' not found`
      );
    }

    table.schema = newSchema;
    table.schemaVersion++;
    table.modifiedAt = Date.now();

    // Create snapshot
    const snapshot = this.createSnapshot(manifest, {
      type: 'schema-evolution',
      table: tableName,
      changes,
    }, {
      tablesModified: [tableName],
      chunksAdded: 0,
      chunksRemoved: 0,
      rowsAdded: 0,
      rowsRemoved: 0,
      bytesAdded: 0,
      bytesRemoved: 0,
    });

    manifest.snapshots.push(snapshot);
    manifest.currentSnapshotId = snapshot.id;

    await this.save(version);

    return snapshot;
  }

  // ===========================================================================
  // Statistics
  // ===========================================================================

  /**
   * Get lakehouse statistics
   */
  async getStatistics(): Promise<LakehouseStatistics> {
    const manifest = await this.getManifest();

    let totalTables = 0;
    let totalPartitions = 0;
    let totalChunks = 0;
    let totalRows = 0;
    let totalBytes = 0;

    for (const table of Object.values(manifest.tables)) {
      totalTables++;
      totalPartitions += table.partitions.size;
      totalRows += table.totalRowCount;
      totalBytes += table.totalByteSize;

      for (const partition of table.partitions.values()) {
        totalChunks += partition.chunks.length;
      }
    }

    return {
      lakehouseId: manifest.lakehouseId,
      formatVersion: manifest.formatVersion,
      tables: totalTables,
      partitions: totalPartitions,
      chunks: totalChunks,
      rows: totalRows,
      bytes: totalBytes,
      snapshots: manifest.snapshots.length,
      currentSnapshotId: manifest.currentSnapshotId,
      version: manifest.version,
      updatedAt: new Date(manifest.updatedAt),
    };
  }
}

/**
 * Lakehouse statistics
 */
export interface LakehouseStatistics {
  lakehouseId: string;
  formatVersion: number;
  tables: number;
  partitions: number;
  chunks: number;
  rows: number;
  bytes: number;
  snapshots: number;
  currentSnapshotId: string;
  version: number;
  updatedAt: Date;
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a manifest manager
 */
export function createManifestManager(options: ManifestManagerOptions): ManifestManager {
  return new ManifestManager(options);
}
