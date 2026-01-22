/**
 * DoLake Buffer Management
 *
 * Manages buffering of CDC events by table and partition before flushing to R2.
 */

import {
  type CDCEvent,
  type BufferedBatch,
  type BufferStats,
  type DoLakeConfig,
  type FlushTrigger,
  DEFAULT_DOLAKE_CONFIG,
  generateBatchId,
  BufferOverflowError,
} from './types.js';

// =============================================================================
// Source Connection State
// =============================================================================

/**
 * State for a connected source DO
 */
export interface SourceConnectionState {
  sourceDoId: string;
  sourceShardName?: string;
  lastReceivedSequence: number;
  lastAckedSequence: number;
  connectedAt: number;
  lastActivityAt: number;
  batchesReceived: number;
  eventsReceived: number;
}

// =============================================================================
// Table Partition Buffer
// =============================================================================

/**
 * Events buffered for a specific table partition
 */
export interface PartitionBuffer {
  table: string;
  partitionKey: string | null;
  events: CDCEvent[];
  sizeBytes: number;
  firstEventTime: number;
  lastEventTime: number;
}

// =============================================================================
// Deduplication
// =============================================================================

/**
 * Deduplication configuration
 */
export interface DedupConfig {
  enabled: boolean;
  windowMs: number;
  maxEntries: number;
}

export const DEFAULT_DEDUP_CONFIG: DedupConfig = {
  enabled: true,
  windowMs: 300_000,
  maxEntries: 100_000,
};

/**
 * Deduplication statistics
 */
export interface DedupStats {
  totalChecks: number;
  duplicatesFound: number;
  entriesTracked: number;
}

// =============================================================================
// Buffer Snapshot (for persistence)
// =============================================================================

/**
 * Serializable buffer snapshot
 */
export interface BufferSnapshot {
  batches: BufferedBatch[];
  sourceStates: Array<[string, SourceConnectionState]>;
  partitionBuffers: Array<[string, PartitionBuffer]>;
  dedupEntries: Array<[string, number]>;
  stats: {
    totalEventsReceived: number;
    totalBatchesReceived: number;
    lastFlushTime: number;
  };
}

// =============================================================================
// CDC Buffer Manager
// =============================================================================

/**
 * CDC Buffer Manager
 *
 * Manages buffering, batching, and deduplication of CDC events.
 * Organizes events by table and partition for efficient Parquet writing.
 */
export class CDCBufferManager {
  private config: DoLakeConfig;
  private batches: Map<string, BufferedBatch> = new Map();
  private sourceStates: Map<string, SourceConnectionState> = new Map();
  private partitionBuffers: Map<string, PartitionBuffer> = new Map();
  private sourceWebSockets: Map<string, WebSocket> = new Map();

  // Deduplication
  private dedupConfig: DedupConfig;
  private dedupSet: Map<string, number> = new Map();
  private dedupStats: DedupStats = {
    totalChecks: 0,
    duplicatesFound: 0,
    entriesTracked: 0,
  };

  // Statistics
  private totalEventsReceived = 0;
  private totalBatchesReceived = 0;
  private lastFlushTime = 0;
  private bufferCreatedAt: number;

  constructor(config: Partial<DoLakeConfig> = {}, dedupConfig: Partial<DedupConfig> = {}) {
    this.config = { ...DEFAULT_DOLAKE_CONFIG, ...config };
    this.dedupConfig = { ...DEFAULT_DEDUP_CONFIG, ...dedupConfig };
    this.bufferCreatedAt = Date.now();
  }

  // ===========================================================================
  // Batch Management
  // ===========================================================================

  /**
   * Add a batch of CDC events to the buffer
   */
  addBatch(
    sourceDoId: string,
    events: CDCEvent[],
    sequenceNumber: number,
    sourceShardName?: string
  ): { added: boolean; batchId: string; isDuplicate: boolean } {
    const batchId = generateBatchId(sourceDoId, sequenceNumber);

    // Check for duplicate batch
    if (this.dedupConfig.enabled) {
      const dedupKey = `batch:${sourceDoId}:${sequenceNumber}`;
      if (this.isDuplicate(dedupKey)) {
        return { added: false, batchId, isDuplicate: true };
      }
      this.markSeen(dedupKey);
    }

    // Calculate batch size
    const sizeBytes = this.estimateBatchSize(events);

    // Check buffer capacity
    const currentSize = this.getTotalSizeBytes();
    if (currentSize + sizeBytes > this.config.maxBufferSize) {
      throw new BufferOverflowError(
        `Buffer full: ${currentSize + sizeBytes} > ${this.config.maxBufferSize}`
      );
    }

    // Create buffered batch
    const batch: BufferedBatch = {
      batchId,
      sourceDoId,
      sourceShardName,
      events,
      receivedAt: Date.now(),
      sequenceNumber,
      persisted: false,
      inFallback: false,
      sizeBytes,
    };

    this.batches.set(batchId, batch);
    this.totalBatchesReceived++;
    this.totalEventsReceived += events.length;

    // Update source state
    this.updateSourceState(
      sourceDoId,
      events.length,
      sequenceNumber,
      sourceShardName
    );

    // Distribute events to partition buffers
    this.distributeToPartitions(events);

    return { added: true, batchId, isDuplicate: false };
  }

  /**
   * Distribute events to table/partition buffers
   */
  private distributeToPartitions(events: CDCEvent[]): void {
    for (const event of events) {
      // Extract partition key from event (default: null for unpartitioned)
      const partitionKey = this.extractPartitionKey(event);
      const bufferKey = `${event.table}:${partitionKey ?? '__default__'}`;

      let buffer = this.partitionBuffers.get(bufferKey);
      if (!buffer) {
        buffer = {
          table: event.table,
          partitionKey,
          events: [],
          sizeBytes: 0,
          firstEventTime: event.timestamp,
          lastEventTime: event.timestamp,
        };
        this.partitionBuffers.set(bufferKey, buffer);
      }

      buffer.events.push(event);
      buffer.sizeBytes += this.estimateEventSize(event);
      buffer.lastEventTime = Math.max(buffer.lastEventTime, event.timestamp);
    }
  }

  /**
   * Extract partition key from an event
   */
  private extractPartitionKey(event: CDCEvent): string | null {
    // Check for partition metadata
    const metadata = event.metadata;
    if (metadata?.partition) {
      return String(metadata.partition);
    }

    // Extract from data (common patterns: date, region, etc.)
    const data = event.after ?? event.before;
    if (data && typeof data === 'object') {
      const record = data as Record<string, unknown>;
      // Common partition keys
      if (record.partition_key) return String(record.partition_key);
      if (record.created_at) {
        // Date-based partitioning
        const date = new Date(record.created_at as string | number);
        return `dt=${date.toISOString().slice(0, 10)}`;
      }
    }

    return null;
  }

  /**
   * Get partition buffers ready for flush
   */
  getPartitionBuffersForFlush(): PartitionBuffer[] {
    return Array.from(this.partitionBuffers.values()).filter(
      (buffer) => buffer.events.length > 0
    );
  }

  /**
   * Clear partition buffers after flush
   */
  clearPartitionBuffers(tables?: string[]): void {
    if (!tables) {
      this.partitionBuffers.clear();
      return;
    }

    for (const [key, buffer] of this.partitionBuffers.entries()) {
      if (tables.includes(buffer.table)) {
        this.partitionBuffers.delete(key);
      }
    }
  }

  // ===========================================================================
  // Source State Management
  // ===========================================================================

  /**
   * Update source connection state
   */
  updateSourceState(
    sourceDoId: string,
    eventsReceived: number,
    sequenceNumber: number,
    sourceShardName?: string
  ): void {
    let state = this.sourceStates.get(sourceDoId);

    if (!state) {
      state = {
        sourceDoId,
        sourceShardName,
        lastReceivedSequence: 0,
        lastAckedSequence: 0,
        connectedAt: Date.now(),
        lastActivityAt: Date.now(),
        batchesReceived: 0,
        eventsReceived: 0,
      };
      this.sourceStates.set(sourceDoId, state);
    }

    state.lastReceivedSequence = sequenceNumber;
    state.lastActivityAt = Date.now();
    state.batchesReceived++;
    state.eventsReceived += eventsReceived;

    if (sourceShardName) {
      state.sourceShardName = sourceShardName;
    }
  }

  /**
   * Register WebSocket for a source
   */
  registerSourceWebSocket(sourceDoId: string, ws: WebSocket): void {
    this.sourceWebSockets.set(sourceDoId, ws);
  }

  /**
   * Unregister WebSocket for a source
   */
  unregisterSourceWebSocket(sourceDoId: string): void {
    this.sourceWebSockets.delete(sourceDoId);
  }

  /**
   * Get WebSocket for a source
   */
  getSourceWebSocket(sourceDoId: string): WebSocket | undefined {
    return this.sourceWebSockets.get(sourceDoId);
  }

  /**
   * Get source states
   */
  getSourceStates(): Map<string, SourceConnectionState> {
    return this.sourceStates;
  }

  // ===========================================================================
  // Deduplication
  // ===========================================================================

  /**
   * Check if a key has been seen (duplicate)
   */
  isDuplicate(key: string): boolean {
    this.dedupStats.totalChecks++;
    this.cleanupDedup();

    const timestamp = this.dedupSet.get(key);
    if (timestamp !== undefined) {
      this.dedupStats.duplicatesFound++;
      return true;
    }
    return false;
  }

  /**
   * Mark a key as seen
   */
  markSeen(key: string): void {
    this.dedupSet.set(key, Date.now());
    this.dedupStats.entriesTracked = this.dedupSet.size;
  }

  /**
   * Clean up expired dedup entries
   */
  private cleanupDedup(): void {
    const cutoff = Date.now() - this.dedupConfig.windowMs;
    for (const [key, timestamp] of this.dedupSet.entries()) {
      if (timestamp < cutoff) {
        this.dedupSet.delete(key);
      }
    }
    this.dedupStats.entriesTracked = this.dedupSet.size;
  }

  /**
   * Get dedup statistics
   */
  getDedupStats(): DedupStats {
    return { ...this.dedupStats };
  }

  // ===========================================================================
  // Flush Management
  // ===========================================================================

  /**
   * Check if buffer should be flushed
   */
  shouldFlush(): FlushTrigger | null {
    const stats = this.getStats();

    // Check event count threshold
    if (stats.eventCount >= this.config.flushThresholdEvents) {
      return 'threshold_events';
    }

    // Check size threshold
    if (stats.totalSizeBytes >= this.config.flushThresholdBytes) {
      return 'threshold_size';
    }

    // Check time threshold
    if (stats.oldestBatchTime) {
      const age = Date.now() - stats.oldestBatchTime;
      if (age >= this.config.flushThresholdMs) {
        return 'threshold_time';
      }
    }

    return null;
  }

  /**
   * Get batches ready for flush
   */
  getBatchesForFlush(): BufferedBatch[] {
    return Array.from(this.batches.values()).filter(
      (batch) => !batch.persisted && !batch.inFallback
    );
  }

  /**
   * Get all events sorted by timestamp
   */
  getAllEventsSorted(): CDCEvent[] {
    const events: CDCEvent[] = [];
    for (const batch of this.batches.values()) {
      if (!batch.persisted && !batch.inFallback) {
        events.push(...batch.events);
      }
    }
    return events.sort((a, b) => a.timestamp - b.timestamp);
  }

  /**
   * Get events grouped by table
   */
  getEventsByTable(): Map<string, CDCEvent[]> {
    const byTable = new Map<string, CDCEvent[]>();

    for (const batch of this.batches.values()) {
      if (!batch.persisted && !batch.inFallback) {
        for (const event of batch.events) {
          let tableEvents = byTable.get(event.table);
          if (!tableEvents) {
            tableEvents = [];
            byTable.set(event.table, tableEvents);
          }
          tableEvents.push(event);
        }
      }
    }

    return byTable;
  }

  /**
   * Mark batches as persisted
   */
  markPersisted(batchIds: string[]): void {
    for (const batchId of batchIds) {
      const batch = this.batches.get(batchId);
      if (batch) {
        batch.persisted = true;
      }
    }
    this.lastFlushTime = Date.now();
  }

  /**
   * Mark batches as in fallback storage
   */
  markInFallback(batchIds: string[]): void {
    for (const batchId of batchIds) {
      const batch = this.batches.get(batchId);
      if (batch) {
        batch.inFallback = true;
      }
    }
  }

  /**
   * Clear persisted batches from buffer
   */
  clearPersisted(): void {
    for (const [batchId, batch] of this.batches.entries()) {
      if (batch.persisted) {
        this.batches.delete(batchId);
      }
    }
  }

  /**
   * Get time until next scheduled flush
   */
  getTimeUntilFlush(): number {
    const stats = this.getStats();
    if (!stats.oldestBatchTime) {
      return this.config.flushThresholdMs;
    }
    const age = Date.now() - stats.oldestBatchTime;
    return Math.max(0, this.config.flushThresholdMs - age);
  }

  // ===========================================================================
  // Statistics
  // ===========================================================================

  /**
   * Get buffer statistics
   */
  getStats(): BufferStats {
    let batchCount = 0;
    let eventCount = 0;
    let totalSizeBytes = 0;
    let oldestBatchTime: number | undefined;
    let newestBatchTime: number | undefined;

    for (const batch of this.batches.values()) {
      if (!batch.persisted && !batch.inFallback) {
        batchCount++;
        eventCount += batch.events.length;
        totalSizeBytes += batch.sizeBytes;

        if (!oldestBatchTime || batch.receivedAt < oldestBatchTime) {
          oldestBatchTime = batch.receivedAt;
        }
        if (!newestBatchTime || batch.receivedAt > newestBatchTime) {
          newestBatchTime = batch.receivedAt;
        }
      }
    }

    return {
      batchCount,
      eventCount,
      totalSizeBytes,
      utilization: totalSizeBytes / this.config.maxBufferSize,
      oldestBatchTime,
      newestBatchTime,
    };
  }

  /**
   * Get total size in bytes
   */
  getTotalSizeBytes(): number {
    let total = 0;
    for (const batch of this.batches.values()) {
      if (!batch.persisted && !batch.inFallback) {
        total += batch.sizeBytes;
      }
    }
    return total;
  }

  /**
   * Estimate size of a batch
   */
  private estimateBatchSize(events: CDCEvent[]): number {
    let size = 0;
    for (const event of events) {
      size += this.estimateEventSize(event);
    }
    return size;
  }

  /**
   * Estimate size of a single event
   */
  private estimateEventSize(event: CDCEvent): number {
    // Rough estimate: JSON serialization + overhead
    let size = 100; // Base overhead
    size += event.table.length;
    size += event.rowId.length;
    if (event.before) size += JSON.stringify(event.before).length;
    if (event.after) size += JSON.stringify(event.after).length;
    if (event.metadata) size += JSON.stringify(event.metadata).length;
    return size;
  }

  // ===========================================================================
  // Serialization
  // ===========================================================================

  /**
   * Serialize buffer state for persistence
   */
  serialize(): BufferSnapshot {
    return {
      batches: Array.from(this.batches.values()),
      sourceStates: Array.from(this.sourceStates.entries()),
      partitionBuffers: Array.from(this.partitionBuffers.entries()),
      dedupEntries: Array.from(this.dedupSet.entries()),
      stats: {
        totalEventsReceived: this.totalEventsReceived,
        totalBatchesReceived: this.totalBatchesReceived,
        lastFlushTime: this.lastFlushTime,
      },
    };
  }

  /**
   * Restore buffer from snapshot
   */
  static restore(
    snapshot: BufferSnapshot,
    config: Partial<DoLakeConfig> = {}
  ): CDCBufferManager {
    const manager = new CDCBufferManager(config);

    // Restore batches
    for (const batch of snapshot.batches) {
      manager.batches.set(batch.batchId, batch);
    }

    // Restore source states
    for (const [id, state] of snapshot.sourceStates) {
      manager.sourceStates.set(id, state);
    }

    // Restore partition buffers
    for (const [key, buffer] of snapshot.partitionBuffers) {
      manager.partitionBuffers.set(key, buffer);
    }

    // Restore dedup entries
    for (const [key, timestamp] of snapshot.dedupEntries) {
      manager.dedupSet.set(key, timestamp);
    }

    // Restore stats
    manager.totalEventsReceived = snapshot.stats.totalEventsReceived;
    manager.totalBatchesReceived = snapshot.stats.totalBatchesReceived;
    manager.lastFlushTime = snapshot.stats.lastFlushTime;

    return manager;
  }

  /**
   * Clear all buffer data
   */
  clear(): void {
    this.batches.clear();
    this.partitionBuffers.clear();
    this.dedupSet.clear();
  }
}
