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
  getNumericTimestamp,
  CURRENT_SCHEMA_VERSION,
  MIN_SUPPORTED_VERSION,
  SCHEMA_VERSION_HISTORY,
  BREAKING_CHANGES,
  VersionMismatchError,
} from './types.js';
import { TIMEOUTS, THRESHOLDS, BUFFER } from './constants.js';

// =============================================================================
// Source Connection State
// =============================================================================

/**
 * State for a connected source DO
 */
export interface SourceConnectionState {
  sourceDoId: string;
  sourceShardName?: string | undefined;
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
  windowMs: TIMEOUTS.DEDUPLICATION_WINDOW_MS,
  maxEntries: THRESHOLDS.MAX_DEDUP_ENTRIES,
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
 * Serializable buffer snapshot with schema versioning
 */
export interface BufferSnapshot {
  /** Schema version for forward/backward compatibility */
  version: number;
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
  private readonly config: DoLakeConfig;
  private readonly batches: Map<string, BufferedBatch> = new Map();
  private readonly sourceStates: Map<string, SourceConnectionState> = new Map();
  private readonly partitionBuffers: Map<string, PartitionBuffer> = new Map();
  private readonly sourceWebSockets: Map<string, WebSocket> = new Map();

  // Deduplication
  private readonly dedupConfig: DedupConfig;
  private readonly dedupSet: Map<string, number> = new Map();
  private readonly dedupStats: DedupStats = {
    totalChecks: 0,
    duplicatesFound: 0,
    entriesTracked: 0,
  };

  // Statistics
  private totalEventsReceived = 0;
  private totalBatchesReceived = 0;
  private lastFlushTime = 0;
  private readonly bufferCreatedAt: number;

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

      const eventTime = getNumericTimestamp(event.timestamp);
      let buffer = this.partitionBuffers.get(bufferKey);
      if (!buffer) {
        buffer = {
          table: event.table,
          partitionKey,
          events: [],
          sizeBytes: 0,
          firstEventTime: eventTime,
          lastEventTime: eventTime,
        };
        this.partitionBuffers.set(bufferKey, buffer);
      }

      buffer.events.push(event);
      buffer.sizeBytes += this.estimateEventSize(event);
      buffer.lastEventTime = Math.max(buffer.lastEventTime, eventTime);
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
   * Updates access time for LRU tracking when key exists.
   */
  isDuplicate(key: string): boolean {
    this.dedupStats.totalChecks++;
    this.cleanupDedup();

    const timestamp = this.dedupSet.get(key);
    if (timestamp !== undefined) {
      // Update access time for LRU tracking
      this.dedupSet.delete(key);
      this.dedupSet.set(key, Date.now());
      this.dedupStats.duplicatesFound++;
      return true;
    }
    return false;
  }

  /**
   * Mark a key as seen
   * Enforces maxEntries limit with LRU eviction.
   */
  markSeen(key: string): void {
    // If key already exists, delete it first to update position in Map iteration order
    if (this.dedupSet.has(key)) {
      this.dedupSet.delete(key);
    }

    this.dedupSet.set(key, Date.now());

    // Enforce maxEntries limit with LRU eviction
    this.evictLRUEntries();

    this.dedupStats.entriesTracked = this.dedupSet.size;
  }

  /**
   * Evict oldest entries when exceeding maxEntries limit.
   * Uses Map iteration order (insertion order) for LRU eviction.
   */
  private evictLRUEntries(): void {
    const excess = this.dedupSet.size - this.dedupConfig.maxEntries;
    if (excess <= 0) {
      return;
    }

    // Map maintains insertion order, so first entries are oldest (LRU)
    let evicted = 0;
    for (const key of this.dedupSet.keys()) {
      if (evicted >= excess) {
        break;
      }
      this.dedupSet.delete(key);
      evicted++;
    }
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
    return events.sort((a, b) => getNumericTimestamp(a.timestamp) - getNumericTimestamp(b.timestamp));
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
    let size = BUFFER.EVENT_SIZE_BASE_OVERHEAD;
    size += event.table.length;
    if (event.rowId) size += event.rowId.length;
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
      version: CURRENT_SCHEMA_VERSION,
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
   * Restore buffer from snapshot with version validation
   */
  static restore(
    snapshot: BufferSnapshot | Omit<BufferSnapshot, 'version'>,
    config: Partial<DoLakeConfig> = {},
    dedupConfig: Partial<DedupConfig> = {}
  ): CDCBufferManager {
    // Validate and migrate version
    const validatedSnapshot = CDCBufferManager.validateAndMigrateSnapshot(snapshot);

    const manager = new CDCBufferManager(config, dedupConfig);

    // Restore batches
    for (const batch of validatedSnapshot.batches) {
      manager.batches.set(batch.batchId, batch);
    }

    // Restore source states
    for (const [id, state] of validatedSnapshot.sourceStates) {
      manager.sourceStates.set(id, state);
    }

    // Restore partition buffers
    for (const [key, buffer] of validatedSnapshot.partitionBuffers) {
      manager.partitionBuffers.set(key, buffer);
    }

    // Restore dedup entries
    for (const [key, timestamp] of validatedSnapshot.dedupEntries) {
      manager.dedupSet.set(key, timestamp);
    }

    // Restore stats
    manager.totalEventsReceived = validatedSnapshot.stats.totalEventsReceived;
    manager.totalBatchesReceived = validatedSnapshot.stats.totalBatchesReceived;
    manager.lastFlushTime = validatedSnapshot.stats.lastFlushTime;

    return manager;
  }

  /**
   * Validate snapshot version and migrate if necessary
   */
  private static validateAndMigrateSnapshot(
    snapshot: BufferSnapshot | Omit<BufferSnapshot, 'version'>
  ): BufferSnapshot {
    // Get version from snapshot, treating missing as v0 (unversioned)
    const snapshotVersion = 'version' in snapshot ? snapshot.version : 0;

    // Validate version is a positive integer
    if (
      snapshotVersion === null ||
      snapshotVersion === undefined ||
      typeof snapshotVersion !== 'number' ||
      !Number.isInteger(snapshotVersion) ||
      snapshotVersion < 0
    ) {
      throw new VersionMismatchError(
        `Invalid version: expected a non-negative integer, got ${JSON.stringify(snapshotVersion)}`,
        typeof snapshotVersion === 'number' ? snapshotVersion : -1,
        CURRENT_SCHEMA_VERSION,
        MIN_SUPPORTED_VERSION
      );
    }

    // Check for future versions we don't support
    if (snapshotVersion > CURRENT_SCHEMA_VERSION) {
      throw new VersionMismatchError(
        `Unsupported snapshot version ${snapshotVersion}. Current version is ${CURRENT_SCHEMA_VERSION}. ` +
        `Please upgrade to a newer version that supports schema version ${snapshotVersion}.`,
        snapshotVersion,
        CURRENT_SCHEMA_VERSION,
        MIN_SUPPORTED_VERSION
      );
    }

    // Check for versions below minimum supported
    if (snapshotVersion < MIN_SUPPORTED_VERSION) {
      throw new VersionMismatchError(
        `Snapshot version ${snapshotVersion} is too old. Minimum supported version is ${MIN_SUPPORTED_VERSION}.`,
        snapshotVersion,
        CURRENT_SCHEMA_VERSION,
        MIN_SUPPORTED_VERSION
      );
    }

    // Migrate snapshot if needed
    return CDCBufferManager.migrateSnapshot(snapshot as BufferSnapshot, snapshotVersion);
  }

  /**
   * Migrate snapshot from one version to current
   */
  static migrateSnapshot(
    snapshot: BufferSnapshot | Omit<BufferSnapshot, 'version'>,
    targetVersion: number = CURRENT_SCHEMA_VERSION
  ): BufferSnapshot {
    let currentVersion = 'version' in snapshot ? snapshot.version : 0;
    let result = { ...snapshot } as BufferSnapshot;

    // Apply migrations sequentially
    while (currentVersion < targetVersion) {
      switch (currentVersion) {
        case 0:
          // Migrate from v0 (unversioned) to v1
          result = {
            ...result,
            version: 1,
          };
          currentVersion = 1;
          break;
        default:
          // No migration needed for this version step
          currentVersion++;
          break;
      }
    }

    // Ensure version is set to target
    result.version = targetVersion;
    return result;
  }

  /**
   * Check if a version is compatible with the current reader
   */
  static isVersionCompatible(version: number): boolean {
    return version >= MIN_SUPPORTED_VERSION && version <= CURRENT_SCHEMA_VERSION;
  }

  /**
   * Get the current schema version
   */
  getSchemaVersion(): number {
    return CURRENT_SCHEMA_VERSION;
  }

  /**
   * Static schema version constant
   */
  static readonly SCHEMA_VERSION = CURRENT_SCHEMA_VERSION;

  /**
   * Static minimum supported version constant
   */
  static readonly MIN_SUPPORTED_VERSION = MIN_SUPPORTED_VERSION;

  /**
   * Static schema version history
   */
  static readonly SCHEMA_VERSION_HISTORY = SCHEMA_VERSION_HISTORY;

  /**
   * Static breaking changes documentation
   */
  static readonly BREAKING_CHANGES = BREAKING_CHANGES;

  /**
   * Clear all buffer data
   */
  clear(): void {
    this.batches.clear();
    this.partitionBuffers.clear();
    this.dedupSet.clear();
  }
}
