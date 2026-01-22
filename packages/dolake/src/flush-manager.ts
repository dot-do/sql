/**
 * Flush Manager Module
 *
 * Buffer flush logic and Parquet write coordination.
 * Extracted from the DoLake monolith for better separation of concerns.
 */

import type {
  CDCEvent,
  DoLakeConfig,
  FlushResult,
  FlushTrigger,
  TableIdentifier,
  IcebergTableMetadata,
} from './types.js';
import { generateUUID } from './types.js';
import type { CDCBufferManager } from './buffer.js';
import { writeParquet, createDataFile, inferSchemaFromEvents } from './parquet.js';
import {
  type R2IcebergStorage,
  createTableMetadata,
  addSnapshot,
  createAppendSnapshot,
  createManifestFile,
  dataFilePath,
  manifestListPath,
  manifestFilePath,
  partitionToPath,
} from './iceberg.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Partition buffer for flushing
 */
export interface PartitionBuffer {
  table: string;
  partitionKey: string | null;
  events: CDCEvent[];
  sizeBytes: number;
}

/**
 * Table flush result
 */
export interface TableFlushResult {
  paths: string[];
  batchCount: number;
  eventCount: number;
  bytesWritten: number;
}

/**
 * Flush manager dependencies
 */
export interface FlushManagerDeps {
  config: DoLakeConfig;
  storage: R2IcebergStorage;
  ctx: DurableObjectState;
}

/**
 * Flush manager configuration
 */
export interface FlushManagerConfig {
  r2BasePath: string;
  enableFallback: boolean;
}

// =============================================================================
// Flush Manager Class
// =============================================================================

/**
 * Manages buffer flushing to R2 as Parquet/Iceberg
 */
export class FlushManager {
  private readonly deps: FlushManagerDeps;
  private flushPromise: Promise<FlushResult> | null = null;

  constructor(deps: FlushManagerDeps) {
    this.deps = deps;
  }

  /**
   * Check if a flush is currently in progress
   */
  isFlushInProgress(): boolean {
    return this.flushPromise !== null;
  }

  /**
   * Get the current flush promise if one exists
   */
  getCurrentFlushPromise(): Promise<FlushResult> | null {
    return this.flushPromise;
  }

  /**
   * Schedule a flush if needed
   */
  async scheduleFlush(
    trigger: FlushTrigger,
    buffer: CDCBufferManager,
    setState: (state: 'idle' | 'flushing') => void
  ): Promise<void> {
    if (this.flushPromise) {
      await this.flushPromise;
      return;
    }
    await this.flush(trigger, buffer, setState);
  }

  /**
   * Flush buffer to R2 as Parquet/Iceberg
   */
  async flush(
    trigger: FlushTrigger,
    buffer: CDCBufferManager,
    setState: (state: 'idle' | 'flushing') => void
  ): Promise<FlushResult> {
    if (this.flushPromise) {
      return this.flushPromise;
    }

    setState('flushing');
    this.flushPromise = this.doFlush(trigger, buffer);

    try {
      return await this.flushPromise;
    } finally {
      this.flushPromise = null;
      setState('idle');
    }
  }

  /**
   * Perform the actual flush operation
   */
  private async doFlush(trigger: FlushTrigger, buffer: CDCBufferManager): Promise<FlushResult> {
    const startTime = Date.now();
    const partitionBuffers = buffer.getPartitionBuffersForFlush();

    if (partitionBuffers.length === 0) {
      return {
        success: true,
        batchesFlushed: 0,
        eventsFlushed: 0,
        bytesWritten: 0,
        paths: [],
        durationMs: 0,
        usedFallback: false,
      };
    }

    const paths: string[] = [];
    let totalBatches = 0;
    let totalEvents = 0;
    let totalBytes = 0;

    try {
      // Group buffers by table
      const byTable = new Map<string, PartitionBuffer[]>();
      for (const pb of partitionBuffers) {
        let tableBuffers = byTable.get(pb.table);
        if (!tableBuffers) {
          tableBuffers = [];
          byTable.set(pb.table, tableBuffers);
        }
        tableBuffers.push(pb);
      }

      // Process each table
      for (const [tableName, tableBuffers] of byTable) {
        const tableResult = await this.flushTable(tableName, tableBuffers);
        paths.push(...tableResult.paths);
        totalBatches += tableResult.batchCount;
        totalEvents += tableResult.eventCount;
        totalBytes += tableResult.bytesWritten;
      }

      // Clear partition buffers
      buffer.clearPartitionBuffers();

      // Mark batches as persisted
      const batchIds = buffer.getBatchesForFlush().map((b) => b.batchId);
      buffer.markPersisted(batchIds);
      buffer.clearPersisted();

      return {
        success: true,
        batchesFlushed: totalBatches,
        eventsFlushed: totalEvents,
        bytesWritten: totalBytes,
        paths,
        durationMs: Date.now() - startTime,
        usedFallback: false,
      };
    } catch (error) {
      console.error('Flush failed:', error);

      // Fall back to local storage
      if (this.deps.config.enableFallback) {
        try {
          const allEvents = buffer.getAllEventsSorted();
          await this.deps.ctx.storage.put('fallback_events', allEvents);

          return {
            success: true,
            batchesFlushed: partitionBuffers.length,
            eventsFlushed: allEvents.length,
            bytesWritten: 0,
            paths: [],
            durationMs: Date.now() - startTime,
            usedFallback: true,
          };
        } catch (fallbackError) {
          return {
            success: false,
            batchesFlushed: 0,
            eventsFlushed: 0,
            bytesWritten: 0,
            paths: [],
            durationMs: Date.now() - startTime,
            usedFallback: true,
            error: String(fallbackError),
          };
        }
      }

      return {
        success: false,
        batchesFlushed: 0,
        eventsFlushed: 0,
        bytesWritten: 0,
        paths: [],
        durationMs: Date.now() - startTime,
        usedFallback: false,
        error: String(error),
      };
    }
  }

  /**
   * Flush a single table's data
   */
  async flushTable(
    tableName: string,
    buffers: PartitionBuffer[]
  ): Promise<TableFlushResult> {
    const namespace = ['default'];
    const tableId: TableIdentifier = { namespace, name: tableName };
    const paths: string[] = [];
    let eventCount = 0;
    let bytesWritten = 0;

    // Collect all events for schema inference
    const allEvents: CDCEvent[] = [];
    for (const buffer of buffers) {
      allEvents.push(...buffer.events);
    }

    // Infer or get schema
    const schema = inferSchemaFromEvents(allEvents, tableName);

    // Ensure table exists
    let metadata = await this.ensureTableExists(tableId, schema);

    // Write Parquet files for each partition
    const dataFiles = [];

    for (const buffer of buffers) {
      if (buffer.events.length === 0) continue;

      // Write Parquet file
      const parquetResult = await writeParquet(buffer.events, tableName);
      const partitionPath = partitionToPath(
        buffer.partitionKey ? { partition: buffer.partitionKey } : {}
      );
      const filename = `${generateUUID()}.parquet`;
      const filePath = dataFilePath(metadata.location, partitionPath, filename);

      // Upload to R2
      await this.deps.storage.writeDataFile(filePath, parquetResult.content);

      // Create DataFile entry
      const dataFile = createDataFile(
        filePath,
        parquetResult,
        buffer.partitionKey ? { partition: buffer.partitionKey } : {}
      );
      dataFiles.push(dataFile);

      paths.push(filePath);
      eventCount += buffer.events.length;
      bytesWritten += parquetResult.fileSize;
    }

    // Create new snapshot
    if (dataFiles.length > 0) {
      const totalRecords = BigInt(eventCount);
      const totalSize = BigInt(bytesWritten);

      // Create manifest file
      const manifestUuid = generateUUID();
      const manifestPath = manifestFilePath(metadata.location, manifestUuid);
      const manifestFile = createManifestFile(
        manifestPath,
        BigInt(0),
        metadata['last-sequence-number'] + BigInt(1),
        dataFiles.length,
        totalRecords
      );

      // Write manifest (simplified - real impl would use Avro)
      await this.deps.storage.writeDataFile(
        manifestPath,
        new TextEncoder().encode(JSON.stringify(dataFiles, (_, v) =>
          typeof v === 'bigint' ? v.toString() : v
        ))
      );

      // Create manifest list
      const snapshotId = BigInt(Date.now()) * BigInt(1000000) + BigInt(Math.floor(Math.random() * 1000000));
      const manifestListPathVal = manifestListPath(metadata.location, snapshotId);

      // Write manifest list (simplified - real impl would use Avro)
      await this.deps.storage.writeDataFile(
        manifestListPathVal,
        new TextEncoder().encode(JSON.stringify([manifestFile], (_, v) =>
          typeof v === 'bigint' ? v.toString() : v
        ))
      );

      // Create snapshot
      const snapshot = createAppendSnapshot(
        metadata['current-snapshot-id'],
        metadata['last-sequence-number'] + BigInt(1),
        manifestListPathVal,
        dataFiles.length,
        totalRecords,
        totalSize,
        metadata['current-schema-id']
      );

      // Update metadata
      metadata = addSnapshot(metadata, snapshot);
      await this.deps.storage.commitTable(tableId, metadata);
    }

    return {
      paths,
      batchCount: buffers.length,
      eventCount,
      bytesWritten,
    };
  }

  /**
   * Ensure table exists, creating if needed
   */
  private async ensureTableExists(
    tableId: TableIdentifier,
    schema: ReturnType<typeof inferSchemaFromEvents>
  ): Promise<IcebergTableMetadata> {
    try {
      return await this.deps.storage.loadTable(tableId);
    } catch {
      // Table doesn't exist, create it
      const tableUuid = generateUUID();
      const location = `${this.deps.config.r2BasePath}/${tableId.namespace.join('/')}/${tableId.name}`;

      // Ensure namespace exists
      const nsExists = await this.deps.storage.namespaceExists(tableId.namespace);
      if (!nsExists) {
        await this.deps.storage.createNamespace(tableId.namespace, {});
      }

      const metadata = createTableMetadata(tableUuid, location, schema);
      await this.deps.storage.createTable(tableId, metadata);

      return metadata;
    }
  }
}

// =============================================================================
// Recovery Operations
// =============================================================================

/**
 * Recover from fallback storage
 */
export async function recoverFromFallback(
  events: CDCEvent[],
  flushManager: FlushManager,
  setState: (state: 'idle' | 'recovering') => void
): Promise<void> {
  setState('recovering');

  try {
    // Group by table
    const byTable = new Map<string, CDCEvent[]>();
    for (const event of events) {
      let tableEvents = byTable.get(event.table);
      if (!tableEvents) {
        tableEvents = [];
        byTable.set(event.table, tableEvents);
      }
      tableEvents.push(event);
    }

    for (const [tableName, tableEvents] of byTable) {
      const buffers: PartitionBuffer[] = [{
        table: tableName,
        partitionKey: null,
        events: tableEvents,
        sizeBytes: 0,
      }];
      await flushManager.flushTable(tableName, buffers);
    }
  } catch (error) {
    console.error('Fallback recovery failed:', error);
    throw error;
  } finally {
    setState('idle');
  }
}

// =============================================================================
// Flush Utilities
// =============================================================================

/**
 * Calculate estimated flush time based on buffer stats
 */
export function estimateFlushTime(
  eventCount: number,
  sizeBytes: number,
  avgWriteSpeedBytesPerSec: number = 50 * 1024 * 1024 // 50 MB/s default
): number {
  // Minimum 100ms for any flush
  const minTime = 100;

  // Estimate based on data size
  const sizeBasedEstimate = (sizeBytes / avgWriteSpeedBytesPerSec) * 1000;

  // Add overhead for metadata operations (100ms per 1000 events)
  const metadataOverhead = Math.ceil(eventCount / 1000) * 100;

  return Math.max(minTime, sizeBasedEstimate + metadataOverhead);
}

/**
 * Determine optimal flush trigger based on current state
 */
export function determineOptimalFlushTrigger(
  eventCount: number,
  sizeBytes: number,
  timeSinceLastFlushMs: number,
  config: DoLakeConfig
): FlushTrigger | null {
  // Check thresholds in order of priority
  if (sizeBytes >= config.flushThresholdBytes) {
    return 'threshold_bytes';
  }

  if (eventCount >= config.flushThresholdEvents) {
    return 'threshold_events';
  }

  if (timeSinceLastFlushMs >= config.flushIntervalMs) {
    return 'threshold_time';
  }

  return null;
}

/**
 * Group events by table for efficient flushing
 */
export function groupEventsByTable(events: CDCEvent[]): Map<string, CDCEvent[]> {
  const byTable = new Map<string, CDCEvent[]>();

  for (const event of events) {
    let tableEvents = byTable.get(event.table);
    if (!tableEvents) {
      tableEvents = [];
      byTable.set(event.table, tableEvents);
    }
    tableEvents.push(event);
  }

  return byTable;
}

/**
 * Group events by partition within a table
 */
export function groupEventsByPartition(
  events: CDCEvent[],
  getPartitionKey: (event: CDCEvent) => string | null
): Map<string | null, CDCEvent[]> {
  const byPartition = new Map<string | null, CDCEvent[]>();

  for (const event of events) {
    const partitionKey = getPartitionKey(event);
    let partitionEvents = byPartition.get(partitionKey);
    if (!partitionEvents) {
      partitionEvents = [];
      byPartition.set(partitionKey, partitionEvents);
    }
    partitionEvents.push(event);
  }

  return byPartition;
}
