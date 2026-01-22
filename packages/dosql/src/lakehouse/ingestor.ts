/**
 * Lakehouse Ingestor for DoSQL
 *
 * Ingests CDC events from WAL into the lakehouse:
 * - Buffers CDC events from WAL
 * - Batches by table and partition
 * - Converts to columnar format
 * - Writes to R2 with partitioning
 */

import type { WALEntry, WALOperation } from '../wal/types.js';
import type { ColumnarTableSchema, ColumnDefinition, RowGroup } from '../columnar/types.js';
import { ColumnarWriter, inferSchema } from '../columnar/writer.js';
import { serializeRowGroup } from '../columnar/chunk.js';

import {
  type LakehouseConfig,
  type ChunkMetadata,
  type ChunkBuilder,
  type PartitionKey,
  type R2Bucket,
  type DOCDCEvent,
  DEFAULT_LAKEHOUSE_CONFIG,
  LakehouseError,
  LakehouseErrorCode,
  generateChunkId,
  buildPartitionPath,
} from './types.js';

import {
  Partitioner,
  type PartitionerOptions,
} from './partitioner.js';

import { ManifestManager } from './manifest.js';

// =============================================================================
// Ingestor Configuration
// =============================================================================

/**
 * Ingestor configuration options
 */
export interface IngestorConfig {
  /** Lakehouse configuration */
  lakehouse: LakehouseConfig;
  /** Table schemas (required for typed ingestion) */
  schemas?: Record<string, ColumnarTableSchema>;
  /** Decoder for WAL entry data */
  decoder?: (data: Uint8Array) => Record<string, unknown>;
  /** Flush mode: 'auto' flushes on size/time, 'manual' requires explicit flush */
  flushMode?: 'auto' | 'manual';
  /** Callback when chunk is written */
  onChunkWritten?: (table: string, chunk: ChunkMetadata) => void;
  /** Callback when error occurs */
  onError?: (error: Error) => void;
}

/**
 * Ingest result for a batch of events
 */
export interface IngestResult {
  /** Number of events processed */
  eventsProcessed: number;
  /** Number of events buffered (pending flush) */
  eventsBuffered: number;
  /** Chunks written in this batch */
  chunksWritten: ChunkMetadata[];
  /** Tables affected */
  tablesAffected: string[];
}

/**
 * Flush result
 */
export interface FlushResult {
  /** Chunks written during flush */
  chunksWritten: ChunkMetadata[];
  /** Total rows written */
  rowsWritten: number;
  /** Total bytes written */
  bytesWritten: number;
  /** Tables that were flushed */
  tablesFlushed: string[];
}

// =============================================================================
// Ingestor Class
// =============================================================================

/**
 * Lakehouse ingestor - buffers CDC events and writes to R2
 */
export class Ingestor {
  private r2: R2Bucket;
  private manifestManager: ManifestManager;
  private config: Required<Omit<IngestorConfig, 'schemas' | 'decoder' | 'onChunkWritten' | 'onError'>> &
    Pick<IngestorConfig, 'schemas' | 'decoder' | 'onChunkWritten' | 'onError'>;

  private partitioners: Map<string, Partitioner> = new Map();
  private buffers: Map<string, Map<string, ChunkBuilder>> = new Map(); // table -> partition -> buffer
  private inferredSchemas: Map<string, ColumnarTableSchema> = new Map();
  private chunkSequences: Map<string, number> = new Map(); // table-partition -> sequence

  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;

  // Statistics
  private stats = {
    eventsIngested: 0,
    eventsBuffered: 0,
    chunksWritten: 0,
    bytesWritten: 0,
    lastFlushAt: 0,
  };

  constructor(
    r2: R2Bucket,
    manifestManager: ManifestManager,
    config: IngestorConfig
  ) {
    this.r2 = r2;
    this.manifestManager = manifestManager;
    this.config = {
      lakehouse: { ...DEFAULT_LAKEHOUSE_CONFIG, ...config.lakehouse },
      flushMode: config.flushMode ?? 'auto',
      schemas: config.schemas,
      decoder: config.decoder,
      onChunkWritten: config.onChunkWritten,
      onError: config.onError,
    };

    // Start auto-flush timer if enabled
    if (this.config.flushMode === 'auto') {
      this.startFlushTimer();
    }
  }

  // ===========================================================================
  // Ingest Operations
  // ===========================================================================

  /**
   * Ingest a batch of CDC events
   */
  async ingest(events: DOCDCEvent[]): Promise<IngestResult> {
    if (this.closed) {
      throw new LakehouseError(
        LakehouseErrorCode.CONFIG_ERROR,
        'Ingestor is closed'
      );
    }

    const chunksWritten: ChunkMetadata[] = [];
    const tablesAffected = new Set<string>();

    for (const event of events) {
      // Skip non-data operations
      if (!this.isDataOperation(event.entry.op)) {
        continue;
      }

      const table = event.entry.table;
      tablesAffected.add(table);

      // Get or create partitioner for this table
      const partitioner = this.getOrCreatePartitioner(table);

      // Decode data if needed
      const data = event.data ?? this.decodeEntryData(event.entry);
      if (!data) continue;

      // Get partition keys
      const partitionKeys = partitioner.getPartitionKeys(data);
      const partitionPath = buildPartitionPath(partitionKeys);

      // Get or create buffer for this table/partition
      const buffer = this.getOrCreateBuffer(table, partitionPath, partitionKeys);

      // Add row to buffer based on operation
      this.bufferEvent(buffer, event, data);
      this.stats.eventsIngested++;
      this.stats.eventsBuffered++;

      // Check if buffer should be flushed
      if (this.config.flushMode === 'auto' && this.shouldFlush(buffer)) {
        const chunk = await this.flushBuffer(table, partitionPath);
        if (chunk) {
          chunksWritten.push(chunk);
        }
      }
    }

    return {
      eventsProcessed: events.length,
      eventsBuffered: this.stats.eventsBuffered,
      chunksWritten,
      tablesAffected: Array.from(tablesAffected),
    };
  }

  /**
   * Ingest WAL entries directly
   */
  async ingestWALEntries(
    entries: WALEntry[],
    doId: string
  ): Promise<IngestResult> {
    const events: DOCDCEvent[] = entries.map(entry => ({
      doId,
      entry,
      data: this.decodeEntryData(entry) ?? {},
      receivedAt: Date.now(),
    }));

    return this.ingest(events);
  }

  /**
   * Flush all buffers
   */
  async flush(): Promise<FlushResult> {
    const chunksWritten: ChunkMetadata[] = [];
    const tablesFlushed = new Set<string>();
    let rowsWritten = 0;
    let bytesWritten = 0;

    for (const [table, partitions] of this.buffers) {
      for (const partitionPath of partitions.keys()) {
        const chunk = await this.flushBuffer(table, partitionPath);
        if (chunk) {
          chunksWritten.push(chunk);
          tablesFlushed.add(table);
          rowsWritten += chunk.rowCount;
          bytesWritten += chunk.byteSize;
        }
      }
    }

    this.stats.lastFlushAt = Date.now();
    this.stats.eventsBuffered = 0;

    return {
      chunksWritten,
      rowsWritten,
      bytesWritten,
      tablesFlushed: Array.from(tablesFlushed),
    };
  }

  /**
   * Close the ingestor (flushes remaining data)
   */
  async close(): Promise<FlushResult> {
    this.closed = true;

    // Stop flush timer
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }

    // Final flush
    return this.flush();
  }

  // ===========================================================================
  // Buffer Management
  // ===========================================================================

  /**
   * Get or create partitioner for a table
   */
  private getOrCreatePartitioner(table: string): Partitioner {
    let partitioner = this.partitioners.get(table);
    if (!partitioner) {
      // Get partition columns from config or table schema
      const partitionColumns = this.config.lakehouse.partitionBy.map(name => ({
        name,
        type: 'string' as const, // Will be refined when we have schema
        transform: 'identity' as const,
      }));

      partitioner = new Partitioner({
        columns: partitionColumns,
        basePath: `${this.config.lakehouse.prefix}${table}`,
      });
      this.partitioners.set(table, partitioner);
    }
    return partitioner;
  }

  /**
   * Get or create buffer for a table/partition
   */
  private getOrCreateBuffer(
    table: string,
    partitionPath: string,
    partitionKeys: PartitionKey[]
  ): ChunkBuilder {
    let tableBuffers = this.buffers.get(table);
    if (!tableBuffers) {
      tableBuffers = new Map();
      this.buffers.set(table, tableBuffers);
    }

    let buffer = tableBuffers.get(partitionPath);
    if (!buffer) {
      buffer = {
        table,
        partitionKeys,
        rows: [],
        estimatedSize: 0,
        minLSN: BigInt(Number.MAX_SAFE_INTEGER),
        maxLSN: 0n,
        sourceDOs: new Set(),
        startedAt: Date.now(),
      };
      tableBuffers.set(partitionPath, buffer);
    }

    return buffer;
  }

  /**
   * Buffer a CDC event
   */
  private bufferEvent(
    buffer: ChunkBuilder,
    event: DOCDCEvent,
    data: Record<string, unknown>
  ): void {
    // Handle operation type
    switch (event.entry.op) {
      case 'INSERT':
        buffer.rows.push(data);
        break;

      case 'UPDATE':
        // For updates, we store the new value
        // (Full deduplication happens in aggregator)
        buffer.rows.push(data);
        break;

      case 'DELETE':
        // For deletes, we could store a tombstone record
        // For now, we skip deletes in the lakehouse (append-only)
        // A full implementation would handle soft deletes
        buffer.rows.push({ ...data, __deleted__: true });
        break;
    }

    // Update buffer metadata
    buffer.estimatedSize += this.estimateRowSize(data);
    buffer.sourceDOs.add(event.doId);

    if (event.entry.lsn < buffer.minLSN) {
      buffer.minLSN = event.entry.lsn;
    }
    if (event.entry.lsn > buffer.maxLSN) {
      buffer.maxLSN = event.entry.lsn;
    }
  }

  /**
   * Check if buffer should be flushed
   */
  private shouldFlush(buffer: ChunkBuilder): boolean {
    // Check size threshold
    if (buffer.estimatedSize >= this.config.lakehouse.targetChunkSize) {
      return true;
    }

    // Check row count threshold
    if (buffer.rows.length >= this.config.lakehouse.maxRowsPerChunk) {
      return true;
    }

    // Check time threshold
    const elapsed = Date.now() - buffer.startedAt;
    if (elapsed >= this.config.lakehouse.flushIntervalMs && buffer.rows.length > 0) {
      return true;
    }

    return false;
  }

  /**
   * Flush a specific buffer
   */
  private async flushBuffer(
    table: string,
    partitionPath: string
  ): Promise<ChunkMetadata | null> {
    const tableBuffers = this.buffers.get(table);
    if (!tableBuffers) return null;

    const buffer = tableBuffers.get(partitionPath);
    if (!buffer || buffer.rows.length === 0) return null;

    try {
      // Get or infer schema
      const schema = this.getOrInferSchema(table, buffer.rows);

      // Convert to columnar format
      const writer = new ColumnarWriter(schema, {
        targetRowsPerGroup: this.config.lakehouse.maxRowsPerChunk,
        targetBytesPerGroup: this.config.lakehouse.targetChunkSize,
      });

      await writer.write(buffer.rows);
      const finalGroup = await writer.finalize();

      if (!finalGroup) return null;

      const rowGroups = writer.getFlushedRowGroups();
      if (finalGroup) rowGroups.push(finalGroup);

      // Serialize and write to R2
      const chunk = await this.writeChunk(table, buffer, rowGroups);

      // Update manifest
      await this.manifestManager.addChunks(table, [{
        chunk,
        partitionKeys: buffer.partitionKeys,
      }]);

      // Clear buffer
      tableBuffers.delete(partitionPath);
      this.stats.chunksWritten++;
      this.stats.bytesWritten += chunk.byteSize;
      this.stats.eventsBuffered -= buffer.rows.length;

      // Callback
      if (this.config.onChunkWritten) {
        this.config.onChunkWritten(table, chunk);
      }

      return chunk;
    } catch (error) {
      if (this.config.onError) {
        this.config.onError(error instanceof Error ? error : new Error(String(error)));
      }
      throw new LakehouseError(
        LakehouseErrorCode.R2_ERROR,
        `Failed to flush buffer for ${table}/${partitionPath}`,
        { table, partitionPath },
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Write chunk to R2
   */
  private async writeChunk(
    table: string,
    buffer: ChunkBuilder,
    rowGroups: RowGroup[]
  ): Promise<ChunkMetadata> {
    // Generate chunk ID
    const sequenceKey = `${table}-${buildPartitionPath(buffer.partitionKeys)}`;
    const sequence = (this.chunkSequences.get(sequenceKey) ?? 0) + 1;
    this.chunkSequences.set(sequenceKey, sequence);

    const chunkId = generateChunkId(table, buildPartitionPath(buffer.partitionKeys), sequence);

    // Build chunk path
    const chunkPath = `${this.config.lakehouse.prefix}${table}/${buildPartitionPath(buffer.partitionKeys)}/${chunkId}.columnar`;

    // Serialize row groups
    const serializedGroups = rowGroups.map(rg => serializeRowGroup(rg));
    const totalSize = serializedGroups.reduce((sum, data) => sum + data.length, 0);

    // Combine into single buffer with header
    const combinedBuffer = this.combineRowGroups(serializedGroups);

    // Write to R2
    await this.r2.put(chunkPath, combinedBuffer, {
      customMetadata: {
        table,
        partition: buildPartitionPath(buffer.partitionKeys),
        rowGroups: rowGroups.length.toString(),
        minLSN: buffer.minLSN.toString(),
        maxLSN: buffer.maxLSN.toString(),
      },
    });

    // Calculate column stats
    const columnStats: Record<string, any> = {};
    for (const rg of rowGroups) {
      for (const [colName, chunk] of rg.columns) {
        if (!columnStats[colName]) {
          columnStats[colName] = { ...chunk.stats };
        } else {
          // Merge stats
          const existing = columnStats[colName];
          if (chunk.stats.min !== null) {
            if (existing.min === null || chunk.stats.min < existing.min) {
              existing.min = chunk.stats.min;
            }
          }
          if (chunk.stats.max !== null) {
            if (existing.max === null || chunk.stats.max > existing.max) {
              existing.max = chunk.stats.max;
            }
          }
          existing.nullCount += chunk.stats.nullCount;
        }
      }
    }

    // Build chunk metadata
    const totalRowCount = rowGroups.reduce((sum, rg) => sum + rg.rowCount, 0);

    return {
      id: chunkId,
      path: chunkPath,
      rowCount: totalRowCount,
      byteSize: combinedBuffer.byteLength,
      columnStats,
      format: 'columnar',
      compression: this.config.lakehouse.compression ? 'gzip' : 'none',
      minLSN: buffer.minLSN,
      maxLSN: buffer.maxLSN,
      sourceDOs: Array.from(buffer.sourceDOs),
      createdAt: Date.now(),
    };
  }

  /**
   * Combine row groups into a single buffer
   */
  private combineRowGroups(serializedGroups: Uint8Array[]): Uint8Array {
    // Header: [magic:4][version:2][groupCount:2][groupSizes:4*n][groups...]
    const magic = 0x434F4C52; // "COLR"
    const version = 1;
    const groupCount = serializedGroups.length;

    const headerSize = 8 + 4 * groupCount;
    const totalSize = headerSize + serializedGroups.reduce((sum, g) => sum + g.length, 0);

    const buffer = new Uint8Array(totalSize);
    const view = new DataView(buffer.buffer);

    // Write header
    view.setUint32(0, magic, true);
    view.setUint16(4, version, true);
    view.setUint16(6, groupCount, true);

    let offset = 8;
    for (const group of serializedGroups) {
      view.setUint32(offset, group.length, true);
      offset += 4;
    }

    // Write groups
    for (const group of serializedGroups) {
      buffer.set(group, offset);
      offset += group.length;
    }

    return buffer;
  }

  // ===========================================================================
  // Helper Methods
  // ===========================================================================

  /**
   * Check if WAL operation is a data operation
   */
  private isDataOperation(op: WALOperation): boolean {
    return op === 'INSERT' || op === 'UPDATE' || op === 'DELETE';
  }

  /**
   * Decode WAL entry data
   */
  private decodeEntryData(entry: WALEntry): Record<string, unknown> | null {
    if (!entry.after && !entry.before) return null;

    const data = entry.after ?? entry.before;
    if (!data) return null;

    if (this.config.decoder) {
      return this.config.decoder(data);
    }

    // Default JSON decoder
    try {
      const json = new TextDecoder().decode(data);
      return JSON.parse(json);
    } catch {
      return null;
    }
  }

  /**
   * Get or infer schema for a table
   */
  private getOrInferSchema(
    table: string,
    sampleRows: Record<string, unknown>[]
  ): ColumnarTableSchema {
    // Check configured schemas first
    if (this.config.schemas?.[table]) {
      return this.config.schemas[table];
    }

    // Check already inferred schemas
    let schema = this.inferredSchemas.get(table);
    if (schema) return schema;

    // Infer from sample data
    schema = inferSchema(table, sampleRows);
    this.inferredSchemas.set(table, schema);

    return schema;
  }

  /**
   * Estimate row size in bytes
   */
  private estimateRowSize(row: Record<string, unknown>): number {
    let size = 0;
    for (const [key, value] of Object.entries(row)) {
      size += key.length + 1; // Key + separator

      if (value === null || value === undefined) {
        size += 4;
      } else if (typeof value === 'string') {
        size += value.length * 2; // UTF-8 estimate
      } else if (typeof value === 'number') {
        size += 8;
      } else if (typeof value === 'boolean') {
        size += 1;
      } else if (typeof value === 'bigint') {
        size += 8;
      } else if (value instanceof Uint8Array) {
        size += value.length;
      } else if (value instanceof Date) {
        size += 8;
      } else {
        size += JSON.stringify(value).length;
      }
    }
    return size;
  }

  /**
   * Start auto-flush timer
   */
  private startFlushTimer(): void {
    this.flushTimer = setInterval(async () => {
      try {
        // Check all buffers for time-based flush
        for (const [table, partitions] of this.buffers) {
          for (const [partitionPath, buffer] of partitions) {
            if (this.shouldFlush(buffer)) {
              await this.flushBuffer(table, partitionPath);
            }
          }
        }
      } catch (error) {
        if (this.config.onError) {
          this.config.onError(error instanceof Error ? error : new Error(String(error)));
        }
      }
    }, Math.min(this.config.lakehouse.flushIntervalMs / 2, 30000));
  }

  // ===========================================================================
  // Statistics
  // ===========================================================================

  /**
   * Get ingestor statistics
   */
  getStatistics(): IngestorStatistics {
    return {
      eventsIngested: this.stats.eventsIngested,
      eventsBuffered: this.stats.eventsBuffered,
      chunksWritten: this.stats.chunksWritten,
      bytesWritten: this.stats.bytesWritten,
      tablesBuffered: this.buffers.size,
      partitionsBuffered: Array.from(this.buffers.values()).reduce(
        (sum, m) => sum + m.size,
        0
      ),
      lastFlushAt: this.stats.lastFlushAt
        ? new Date(this.stats.lastFlushAt)
        : null,
    };
  }
}

/**
 * Ingestor statistics
 */
export interface IngestorStatistics {
  eventsIngested: number;
  eventsBuffered: number;
  chunksWritten: number;
  bytesWritten: number;
  tablesBuffered: number;
  partitionsBuffered: number;
  lastFlushAt: Date | null;
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an ingestor
 */
export function createIngestor(
  r2: R2Bucket,
  manifestManager: ManifestManager,
  config: IngestorConfig
): Ingestor {
  return new Ingestor(r2, manifestManager, config);
}
