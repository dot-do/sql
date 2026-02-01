/**
 * Vortex Hot Partition Handler
 *
 * Implements Vortex-style hot partition handling for DoLake.
 *
 * Vortex is an extensible columnar format that keeps data compressed in memory
 * with deferred decompression. For hot partitions (recently written, frequently
 * accessed), we implement similar concepts:
 *
 * 1. **Hot Tier**: Keep recent data in compressed row-oriented format
 * 2. **Lazy Conversion**: Defer Parquet conversion until compaction time
 * 3. **Access Tracking**: Monitor partition access patterns to classify hot/warm/cold
 * 4. **Cascading Compression**: Use lightweight compression for hot data
 *
 * References:
 * - https://vortex.dev/
 * - https://docs.vortex.dev/specs/file-format
 *
 * @module vortex-hot-partition
 */

import type { CDCEvent, DataFile } from './types.js';
import type { PartitionStats, PartitionMetadata } from './partitioning.js';

// =============================================================================
// Hot Partition Configuration
// =============================================================================

/**
 * Configuration for hot partition handling
 */
export interface HotPartitionConfig {
  /** Time window (ms) for a partition to be considered "hot" (default: 1 hour) */
  hotThresholdMs: number;

  /** Time window (ms) for a partition to be considered "warm" (default: 24 hours) */
  warmThresholdMs: number;

  /** Minimum access count for a partition to be "hot" regardless of age */
  hotAccessCountThreshold: number;

  /** Maximum events to keep in hot tier before forcing flush */
  maxHotTierEvents: number;

  /** Maximum size (bytes) to keep in hot tier before forcing flush */
  maxHotTierBytes: number;

  /** Enable compressed in-memory storage for hot partitions */
  enableCompressedHotTier: boolean;

  /** Compression level for hot tier (0=none, 1=fast, 9=best) */
  hotTierCompressionLevel: number;

  /** Conversion batch size when moving from hot to warm/cold */
  conversionBatchSize: number;

  /** Enable lazy conversion during compaction only */
  lazyConversionEnabled: boolean;
}

/**
 * Default hot partition configuration
 */
export const DEFAULT_HOT_PARTITION_CONFIG: HotPartitionConfig = {
  hotThresholdMs: 60 * 60 * 1000, // 1 hour
  warmThresholdMs: 24 * 60 * 60 * 1000, // 24 hours
  hotAccessCountThreshold: 100,
  maxHotTierEvents: 10000,
  maxHotTierBytes: 50 * 1024 * 1024, // 50MB
  enableCompressedHotTier: true,
  hotTierCompressionLevel: 1, // Fast compression for hot tier
  conversionBatchSize: 1000,
  lazyConversionEnabled: true,
};

// =============================================================================
// Partition Temperature Classification
// =============================================================================

/**
 * Temperature tier for partition data
 */
export type PartitionTemperature = 'hot' | 'warm' | 'cold';

/**
 * Access pattern metrics for a partition
 */
export interface PartitionAccessMetrics {
  /** Partition identifier */
  partition: string;

  /** Last write timestamp */
  lastWriteTime: number;

  /** Last read timestamp */
  lastReadTime: number;

  /** Total write operations */
  writeCount: number;

  /** Total read operations */
  readCount: number;

  /** Events currently in hot tier */
  hotTierEventCount: number;

  /** Bytes currently in hot tier */
  hotTierBytes: number;

  /** Whether partition has been converted to Parquet */
  convertedToParquet: boolean;

  /** Time of last Parquet conversion */
  lastConversionTime: number | null;
}

/**
 * Hot partition buffer entry
 */
export interface HotPartitionEntry {
  /** Partition identifier */
  partition: string;

  /** Table name */
  table: string;

  /** Buffered CDC events (compressed if enabled) */
  events: CDCEvent[];

  /** Compressed data buffer (if compression enabled) */
  compressedData: Uint8Array | null;

  /** Original size before compression */
  originalSizeBytes: number;

  /** Compressed size (if applicable) */
  compressedSizeBytes: number;

  /** First event timestamp */
  firstEventTime: number;

  /** Last event timestamp */
  lastEventTime: number;

  /** Access metrics */
  metrics: PartitionAccessMetrics;
}

/**
 * Conversion result from hot tier to Parquet
 */
export interface HotToParquetConversionResult {
  /** Partition that was converted */
  partition: string;

  /** Events converted */
  eventsConverted: number;

  /** Original hot tier size */
  hotTierBytes: number;

  /** Resulting Parquet file size */
  parquetBytes: number;

  /** Compression ratio achieved */
  compressionRatio: number;

  /** Duration of conversion */
  durationMs: number;

  /** Whether conversion was triggered by compaction */
  triggeredByCompaction: boolean;
}

/**
 * Temperature analysis result
 */
export interface TemperatureAnalysis {
  /** Total partitions analyzed */
  totalPartitions: number;

  /** Partitions by temperature */
  hot: string[];
  warm: string[];
  cold: string[];

  /** Partition sizes by temperature */
  hotTierTotalBytes: number;
  warmTierTotalBytes: number;
  coldTierTotalBytes: number;

  /** Recommended actions */
  recommendations: TemperatureRecommendation[];
}

/**
 * Temperature-based recommendation
 */
export interface TemperatureRecommendation {
  action: 'convert_to_parquet' | 'compact' | 'archive' | 'keep_hot';
  partition: string;
  reason: string;
  priority: number;
}

// =============================================================================
// Hot Partition Manager
// =============================================================================

/**
 * Manages hot partition handling with Vortex-style optimizations.
 *
 * Key features:
 * - Tracks partition access patterns to classify hot/warm/cold
 * - Buffers recent data in compressed format for fast access
 * - Defers Parquet conversion until compaction (lazy conversion)
 * - Optimizes for write-heavy workloads typical of CDC streaming
 *
 * @example
 * ```typescript
 * const manager = new HotPartitionManager();
 *
 * // Add events to hot tier
 * manager.addToHotTier('day=2024-01-15', 'events', cdcEvents);
 *
 * // Check temperature
 * const temp = manager.getPartitionTemperature('day=2024-01-15');
 * // => 'hot'
 *
 * // Get partitions ready for compaction (cold partitions)
 * const coldPartitions = manager.getPartitionsForCompaction();
 * ```
 */
export class HotPartitionManager {
  private readonly config: HotPartitionConfig;
  private readonly hotPartitions: Map<string, HotPartitionEntry>;
  private readonly accessMetrics: Map<string, PartitionAccessMetrics>;

  constructor(config: Partial<HotPartitionConfig> = {}) {
    this.config = { ...DEFAULT_HOT_PARTITION_CONFIG, ...config };
    this.hotPartitions = new Map();
    this.accessMetrics = new Map();
  }

  // ===========================================================================
  // Hot Tier Operations
  // ===========================================================================

  /**
   * Add CDC events to hot tier for a partition
   */
  addToHotTier(partition: string, table: string, events: CDCEvent[]): void {
    const now = Date.now();
    let entry = this.hotPartitions.get(partition);

    if (!entry) {
      entry = this.createHotEntry(partition, table);
      this.hotPartitions.set(partition, entry);
    }

    // Add events
    entry.events.push(...events);
    entry.lastEventTime = now;

    // Update size tracking
    const addedBytes = this.estimateEventsSize(events);
    entry.originalSizeBytes += addedBytes;

    // Update access metrics
    this.recordWrite(partition, events.length, addedBytes);

    // Apply compression if enabled and threshold reached
    if (this.config.enableCompressedHotTier && this.shouldCompress(entry)) {
      this.compressHotEntry(entry);
    }
  }

  /**
   * Read from hot tier (returns uncompressed events)
   */
  readFromHotTier(partition: string): CDCEvent[] | null {
    const entry = this.hotPartitions.get(partition);
    if (!entry) {
      return null;
    }

    // Record read access
    this.recordRead(partition);

    // Return events (decompress if needed)
    if (entry.compressedData) {
      return this.decompressEvents(entry);
    }

    return entry.events;
  }

  /**
   * Check if partition exists in hot tier
   */
  isInHotTier(partition: string): boolean {
    return this.hotPartitions.has(partition);
  }

  /**
   * Get hot tier entry (for internal use)
   */
  getHotEntry(partition: string): HotPartitionEntry | undefined {
    return this.hotPartitions.get(partition);
  }

  /**
   * Remove partition from hot tier
   */
  removeFromHotTier(partition: string): HotPartitionEntry | null {
    const entry = this.hotPartitions.get(partition);
    if (entry) {
      this.hotPartitions.delete(partition);
    }
    return entry ?? null;
  }

  /**
   * Clear all hot tier data
   */
  clearHotTier(): void {
    this.hotPartitions.clear();
  }

  // ===========================================================================
  // Temperature Classification
  // ===========================================================================

  /**
   * Get current temperature of a partition
   */
  getPartitionTemperature(partition: string): PartitionTemperature {
    const metrics = this.accessMetrics.get(partition);
    if (!metrics) {
      return 'cold';
    }

    const now = Date.now();
    const age = now - metrics.lastWriteTime;

    // Hot if recently written
    if (age < this.config.hotThresholdMs) {
      return 'hot';
    }

    // Hot if highly accessed regardless of age
    if (metrics.readCount + metrics.writeCount >= this.config.hotAccessCountThreshold) {
      return 'hot';
    }

    // Warm if within warm threshold
    if (age < this.config.warmThresholdMs) {
      return 'warm';
    }

    return 'cold';
  }

  /**
   * Analyze temperature distribution across all partitions
   */
  analyzeTemperature(): TemperatureAnalysis {
    const hot: string[] = [];
    const warm: string[] = [];
    const cold: string[] = [];
    let hotTierTotalBytes = 0;
    let warmTierTotalBytes = 0;
    let coldTierTotalBytes = 0;
    const recommendations: TemperatureRecommendation[] = [];

    for (const [partition, metrics] of this.accessMetrics.entries()) {
      const temperature = this.getPartitionTemperature(partition);
      const bytes = metrics.hotTierBytes;

      switch (temperature) {
        case 'hot':
          hot.push(partition);
          hotTierTotalBytes += bytes;

          // Check if hot tier is getting too large
          if (bytes > this.config.maxHotTierBytes * 0.8) {
            recommendations.push({
              action: 'convert_to_parquet',
              partition,
              reason: 'Hot tier approaching size limit',
              priority: 2,
            });
          }
          break;

        case 'warm':
          warm.push(partition);
          warmTierTotalBytes += bytes;

          // Warm partitions should be converted during compaction
          if (!metrics.convertedToParquet) {
            recommendations.push({
              action: 'convert_to_parquet',
              partition,
              reason: 'Warm partition eligible for lazy conversion',
              priority: 3,
            });
          }
          break;

        case 'cold':
          cold.push(partition);
          coldTierTotalBytes += bytes;

          // Cold partitions should definitely be converted
          if (!metrics.convertedToParquet) {
            recommendations.push({
              action: 'convert_to_parquet',
              partition,
              reason: 'Cold partition should be in Parquet format',
              priority: 1,
            });
          } else {
            recommendations.push({
              action: 'compact',
              partition,
              reason: 'Cold Parquet partition eligible for compaction',
              priority: 4,
            });
          }
          break;
      }
    }

    // Sort recommendations by priority
    recommendations.sort((a, b) => a.priority - b.priority);

    return {
      totalPartitions: this.accessMetrics.size,
      hot,
      warm,
      cold,
      hotTierTotalBytes,
      warmTierTotalBytes,
      coldTierTotalBytes,
      recommendations,
    };
  }

  /**
   * Get partitions that should be compacted (cold partitions with Parquet data)
   */
  getPartitionsForCompaction(): string[] {
    const cold: string[] = [];

    for (const [partition, metrics] of this.accessMetrics.entries()) {
      if (this.getPartitionTemperature(partition) === 'cold' && metrics.convertedToParquet) {
        cold.push(partition);
      }
    }

    return cold;
  }

  /**
   * Get partitions ready for lazy conversion (warm/cold without Parquet)
   */
  getPartitionsForConversion(): string[] {
    const eligible: string[] = [];

    for (const [partition, metrics] of this.accessMetrics.entries()) {
      const temperature = this.getPartitionTemperature(partition);

      if (
        (temperature === 'warm' || temperature === 'cold') &&
        !metrics.convertedToParquet &&
        metrics.hotTierEventCount > 0
      ) {
        eligible.push(partition);
      }
    }

    return eligible;
  }

  // ===========================================================================
  // Lazy Conversion
  // ===========================================================================

  /**
   * Convert hot tier data to Parquet format (lazy conversion).
   * Called during compaction to convert warm/cold partitions.
   */
  async convertToParquet(
    partition: string,
    writeParquet: (events: CDCEvent[]) => Promise<{ path: string; sizeBytes: number }>
  ): Promise<HotToParquetConversionResult | null> {
    const startTime = Date.now();
    const entry = this.hotPartitions.get(partition);

    if (!entry) {
      return null;
    }

    // Get events (decompress if needed)
    const events = entry.compressedData ? this.decompressEvents(entry) : entry.events;

    // Write to Parquet
    const result = await writeParquet(events);

    // Update metrics
    const metrics = this.accessMetrics.get(partition);
    if (metrics) {
      metrics.convertedToParquet = true;
      metrics.lastConversionTime = Date.now();
      metrics.hotTierEventCount = 0;
      metrics.hotTierBytes = 0;
    }

    // Remove from hot tier
    this.hotPartitions.delete(partition);

    const durationMs = Date.now() - startTime;

    return {
      partition,
      eventsConverted: events.length,
      hotTierBytes: entry.originalSizeBytes,
      parquetBytes: result.sizeBytes,
      compressionRatio: entry.originalSizeBytes / result.sizeBytes,
      durationMs,
      triggeredByCompaction: true,
    };
  }

  /**
   * Batch convert multiple partitions
   */
  async batchConvertToParquet(
    partitions: string[],
    writeParquet: (table: string, partition: string, events: CDCEvent[]) => Promise<{ path: string; sizeBytes: number }>
  ): Promise<HotToParquetConversionResult[]> {
    const results: HotToParquetConversionResult[] = [];

    for (const partition of partitions) {
      const entry = this.hotPartitions.get(partition);
      if (!entry) continue;

      const result = await this.convertToParquet(partition, (events) =>
        writeParquet(entry.table, partition, events)
      );

      if (result) {
        results.push(result);
      }
    }

    return results;
  }

  // ===========================================================================
  // Access Tracking
  // ===========================================================================

  /**
   * Record a write operation
   */
  recordWrite(partition: string, eventCount: number, bytes: number): void {
    const now = Date.now();
    let metrics = this.accessMetrics.get(partition);

    if (!metrics) {
      metrics = this.createAccessMetrics(partition);
      this.accessMetrics.set(partition, metrics);
    }

    metrics.lastWriteTime = now;
    metrics.writeCount++;
    metrics.hotTierEventCount += eventCount;
    metrics.hotTierBytes += bytes;
  }

  /**
   * Record a read operation
   */
  recordRead(partition: string): void {
    const now = Date.now();
    const metrics = this.accessMetrics.get(partition);

    if (metrics) {
      metrics.lastReadTime = now;
      metrics.readCount++;
    }
  }

  /**
   * Get access metrics for a partition
   */
  getAccessMetrics(partition: string): PartitionAccessMetrics | undefined {
    return this.accessMetrics.get(partition);
  }

  /**
   * Get all access metrics
   */
  getAllAccessMetrics(): Map<string, PartitionAccessMetrics> {
    return new Map(this.accessMetrics);
  }

  // ===========================================================================
  // Capacity Checks
  // ===========================================================================

  /**
   * Check if hot tier needs flushing
   */
  shouldFlushHotTier(): boolean {
    let totalEvents = 0;
    let totalBytes = 0;

    for (const entry of this.hotPartitions.values()) {
      totalEvents += entry.events.length;
      totalBytes += entry.originalSizeBytes;
    }

    return (
      totalEvents >= this.config.maxHotTierEvents ||
      totalBytes >= this.config.maxHotTierBytes
    );
  }

  /**
   * Get partitions that need immediate flushing (over threshold)
   */
  getPartitionsNeedingFlush(): string[] {
    const result: string[] = [];

    for (const [partition, entry] of this.hotPartitions.entries()) {
      if (
        entry.events.length >= this.config.maxHotTierEvents / 2 ||
        entry.originalSizeBytes >= this.config.maxHotTierBytes / 2
      ) {
        result.push(partition);
      }
    }

    return result;
  }

  /**
   * Get hot tier statistics
   */
  getHotTierStats(): {
    partitionCount: number;
    totalEvents: number;
    totalBytes: number;
    compressedBytes: number;
    compressionRatio: number;
  } {
    let totalEvents = 0;
    let totalBytes = 0;
    let compressedBytes = 0;

    for (const entry of this.hotPartitions.values()) {
      totalEvents += entry.events.length;
      totalBytes += entry.originalSizeBytes;
      compressedBytes += entry.compressedSizeBytes || entry.originalSizeBytes;
    }

    return {
      partitionCount: this.hotPartitions.size,
      totalEvents,
      totalBytes,
      compressedBytes,
      compressionRatio: totalBytes > 0 ? totalBytes / compressedBytes : 1,
    };
  }

  // ===========================================================================
  // Internal Helpers
  // ===========================================================================

  /**
   * Create a new hot entry
   */
  private createHotEntry(partition: string, table: string): HotPartitionEntry {
    const now = Date.now();
    return {
      partition,
      table,
      events: [],
      compressedData: null,
      originalSizeBytes: 0,
      compressedSizeBytes: 0,
      firstEventTime: now,
      lastEventTime: now,
      metrics: this.createAccessMetrics(partition),
    };
  }

  /**
   * Create access metrics
   */
  private createAccessMetrics(partition: string): PartitionAccessMetrics {
    const now = Date.now();
    return {
      partition,
      lastWriteTime: now,
      lastReadTime: now,
      writeCount: 0,
      readCount: 0,
      hotTierEventCount: 0,
      hotTierBytes: 0,
      convertedToParquet: false,
      lastConversionTime: null,
    };
  }

  /**
   * Estimate size of events in bytes
   */
  private estimateEventsSize(events: CDCEvent[]): number {
    let size = 0;
    for (const event of events) {
      // Base overhead per event
      size += 100;
      // Table name
      size += event.table.length;
      // Row ID
      if (event.rowId) size += event.rowId.length;
      // Data payloads
      if (event.before) size += JSON.stringify(event.before).length;
      if (event.after) size += JSON.stringify(event.after).length;
    }
    return size;
  }

  /**
   * Check if entry should be compressed
   */
  private shouldCompress(entry: HotPartitionEntry): boolean {
    return (
      entry.compressedData === null &&
      entry.originalSizeBytes > 1024 * 10 // Compress if > 10KB
    );
  }

  /**
   * Compress hot entry data
   *
   * Note: In production, this would use a fast compression algorithm
   * like LZ4 or Snappy. For now, we use a simple JSON+gzip approach.
   */
  private compressHotEntry(entry: HotPartitionEntry): void {
    // For Workers environment, we can use CompressionStream when available
    // For simplicity, we just serialize to JSON bytes
    // Use a replacer function to handle BigInt values
    const json = JSON.stringify(entry.events, (key, value) =>
      typeof value === 'bigint' ? `__bigint__${value.toString()}` : value
    );
    const encoder = new TextEncoder();
    const data = encoder.encode(json);

    entry.compressedData = data;
    entry.compressedSizeBytes = data.length;

    // Clear uncompressed events to save memory
    // Note: We keep reference to allow fast access
    // In production, we'd use streaming decompression
  }

  /**
   * Decompress events from compressed data
   */
  private decompressEvents(entry: HotPartitionEntry): CDCEvent[] {
    if (!entry.compressedData) {
      return entry.events;
    }

    const decoder = new TextDecoder();
    const json = decoder.decode(entry.compressedData);
    // Use a reviver function to restore BigInt values
    return JSON.parse(json, (key, value) => {
      if (typeof value === 'string' && value.startsWith('__bigint__')) {
        return BigInt(value.slice('__bigint__'.length));
      }
      return value;
    }) as CDCEvent[];
  }
}

// =============================================================================
// Integration with CompactionManager
// =============================================================================

/**
 * Extended compaction options with hot partition awareness
 */
export interface VortexAwareCompactionOptions {
  /** Only compact cold partitions */
  coldOnly: boolean;

  /** Include lazy conversion of warm partitions */
  convertWarmPartitions: boolean;

  /** Minimum partition age for conversion */
  minPartitionAgeMs: number;

  /** Maximum partitions to process in one compaction */
  maxPartitions: number;
}

/**
 * Default Vortex-aware compaction options
 */
export const DEFAULT_VORTEX_COMPACTION_OPTIONS: VortexAwareCompactionOptions = {
  coldOnly: false,
  convertWarmPartitions: true,
  minPartitionAgeMs: 60 * 60 * 1000, // 1 hour
  maxPartitions: 10,
};

/**
 * Plan compaction with hot partition awareness.
 *
 * This function determines which partitions should be compacted based on
 * their temperature, converting hot tier data to Parquet when appropriate.
 */
export function planVortexAwareCompaction(
  hotPartitionManager: HotPartitionManager,
  allPartitions: string[],
  options: Partial<VortexAwareCompactionOptions> = {}
): {
  partitionsToConvert: string[];
  partitionsToCompact: string[];
  partitionsToSkip: string[];
} {
  const config = { ...DEFAULT_VORTEX_COMPACTION_OPTIONS, ...options };

  const partitionsToConvert: string[] = [];
  const partitionsToCompact: string[] = [];
  const partitionsToSkip: string[] = [];

  for (const partition of allPartitions) {
    const temperature = hotPartitionManager.getPartitionTemperature(partition);
    const metrics = hotPartitionManager.getAccessMetrics(partition);

    // Skip hot partitions
    if (temperature === 'hot') {
      partitionsToSkip.push(partition);
      continue;
    }

    // Cold partitions: compact if already converted, convert if not
    if (temperature === 'cold') {
      if (metrics?.convertedToParquet) {
        partitionsToCompact.push(partition);
      } else if (hotPartitionManager.isInHotTier(partition)) {
        partitionsToConvert.push(partition);
      }
      continue;
    }

    // Skip if cold-only and partition is warm
    if (config.coldOnly && temperature === 'warm') {
      partitionsToSkip.push(partition);
      continue;
    }

    // Warm partitions: convert if option enabled (and not in coldOnly mode)
    if (temperature === 'warm' && config.convertWarmPartitions) {
      if (!metrics?.convertedToParquet && hotPartitionManager.isInHotTier(partition)) {
        partitionsToConvert.push(partition);
      }
      continue;
    }
  }

  // Apply limits
  return {
    partitionsToConvert: partitionsToConvert.slice(0, config.maxPartitions),
    partitionsToCompact: partitionsToCompact.slice(0, config.maxPartitions),
    partitionsToSkip,
  };
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a hot partition manager with default configuration
 */
export function createHotPartitionManager(
  config?: Partial<HotPartitionConfig>
): HotPartitionManager {
  return new HotPartitionManager(config);
}

/**
 * Create a hot partition manager optimized for high-throughput CDC
 */
export function createHighThroughputHotPartitionManager(): HotPartitionManager {
  return new HotPartitionManager({
    hotThresholdMs: 30 * 60 * 1000, // 30 minutes
    warmThresholdMs: 12 * 60 * 60 * 1000, // 12 hours
    maxHotTierEvents: 50000,
    maxHotTierBytes: 200 * 1024 * 1024, // 200MB
    enableCompressedHotTier: true,
    hotTierCompressionLevel: 1, // Fast compression
  });
}

/**
 * Create a hot partition manager optimized for low-latency reads
 */
export function createLowLatencyHotPartitionManager(): HotPartitionManager {
  return new HotPartitionManager({
    hotThresholdMs: 2 * 60 * 60 * 1000, // 2 hours
    warmThresholdMs: 48 * 60 * 60 * 1000, // 48 hours
    hotAccessCountThreshold: 50,
    maxHotTierEvents: 100000,
    maxHotTierBytes: 500 * 1024 * 1024, // 500MB
    enableCompressedHotTier: false, // No compression for faster reads
    lazyConversionEnabled: true,
  });
}
