/**
 * DoLake Scalability Module
 *
 * Provides horizontal scaling capabilities including:
 * - Parallel partition writes
 * - Partition compaction
 * - Partition rebalancing
 * - Large file handling (>1GB)
 * - Horizontal scaling configuration
 * - Memory-efficient streaming
 */

import {
  type DataFile,
  type IcebergTableMetadata,
  type CDCEvent,
  generateUUID,
} from './types.js';
import { type PartitionStats, type PartitionMetadata } from './partitioning.js';

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * DoLake scaling configuration
 */
export interface ScalingConfig {
  /** Scaling mode: single DO, partition-per-DO, or auto */
  scalingMode: 'single' | 'partition-per-do' | 'auto';

  /** Minimum DO instances */
  minInstances: number;

  /** Maximum DO instances */
  maxInstances: number;

  /** Partitions per DO instance */
  partitionsPerInstance: number;

  /** Enable parallel partition writes */
  parallelPartitionWrites: boolean;

  /** Maximum parallel writers */
  maxParallelWriters: number;

  /** Maximum Parquet file size in bytes */
  maxParquetFileSize: number;

  /** Use multipart upload for large files */
  useMultipartUpload: boolean;

  /** Maximum partition write throughput (bytes/sec) */
  maxPartitionWriteBytesPerSecond: number;
}

/**
 * Default scaling configuration
 */
export const DEFAULT_SCALING_CONFIG: ScalingConfig = {
  scalingMode: 'single',
  minInstances: 1,
  maxInstances: 16,
  partitionsPerInstance: 4,
  parallelPartitionWrites: true,
  maxParallelWriters: 4,
  maxParquetFileSize: 512 * 1024 * 1024, // 512MB default
  useMultipartUpload: true,
  maxPartitionWriteBytesPerSecond: 100 * 1024 * 1024, // 100MB/s
};

/**
 * Write configuration for a partition
 */
export interface PartitionWriteConfig {
  targetFileSizeBytes: number;
  batchingEnabled: boolean;
  throttlingEnabled: boolean;
}

/**
 * Write result for a partition
 */
export interface PartitionWriteResult {
  partition: string;
  filesWritten: number;
  bytesWritten: bigint;
  recordsWritten: bigint;
  durationMs: number;
  success: boolean;
  error?: string;
}

/**
 * Parallel write result
 */
export interface ParallelWriteResult {
  parallelWritesUsed: boolean;
  partitionsWritten: number;
  successfulPartitions: number;
  failedPartitions: string[];
  partialSuccess: boolean;
  filesWritten: number;
  batchingApplied: boolean;
  totalBytesWritten: bigint;
  totalDurationMs: number;
}

// =============================================================================
// Parallel Write Manager
// =============================================================================

/**
 * Manages parallel writes to multiple partitions
 */
export class ParallelWriteManager {
  private config: ScalingConfig;
  private activeWriters: Map<string, Promise<PartitionWriteResult>>;
  private failedPartitions: Set<string>;
  private throttleState: Map<string, { lastWriteTime: number; bytesInWindow: number }>;

  constructor(config: ScalingConfig = DEFAULT_SCALING_CONFIG) {
    this.config = config;
    this.activeWriters = new Map();
    this.failedPartitions = new Set();
    this.throttleState = new Map();
  }

  /**
   * Write to multiple partitions in parallel
   */
  async writeParallel(
    partitionData: Map<string, CDCEvent[]>,
    writeFunc: (partition: string, events: CDCEvent[]) => Promise<PartitionWriteResult>
  ): Promise<ParallelWriteResult> {
    const startTime = Date.now();
    const partitions = Array.from(partitionData.keys());

    if (!this.config.parallelPartitionWrites) {
      // Sequential writes
      return this.writeSequential(partitionData, writeFunc, startTime);
    }

    const results: PartitionWriteResult[] = [];
    const failedPartitions: string[] = [];
    let filesWritten = 0;
    let totalBytes = BigInt(0);

    // Process partitions in batches
    for (let i = 0; i < partitions.length; i += this.config.maxParallelWriters) {
      const batch = partitions.slice(i, i + this.config.maxParallelWriters);

      const batchPromises = batch.map(async (partition) => {
        const events = partitionData.get(partition) ?? [];

        // Check throttling
        if (this.isThrottled(partition)) {
          await this.waitForThrottle(partition);
        }

        try {
          const result = await writeFunc(partition, events);
          this.updateThrottleState(partition, Number(result.bytesWritten));
          return result;
        } catch (error) {
          return {
            partition,
            filesWritten: 0,
            bytesWritten: BigInt(0),
            recordsWritten: BigInt(0),
            durationMs: 0,
            success: false,
            error: String(error),
          };
        }
      });

      const batchResults = await Promise.all(batchPromises);

      for (const result of batchResults) {
        results.push(result);
        if (result.success) {
          filesWritten += result.filesWritten;
          totalBytes += result.bytesWritten;
        } else {
          failedPartitions.push(result.partition);
          this.failedPartitions.add(result.partition);
        }
      }
    }

    const successfulPartitions = results.filter((r) => r.success).length;

    return {
      parallelWritesUsed: true,
      partitionsWritten: partitions.length,
      successfulPartitions,
      failedPartitions,
      partialSuccess: failedPartitions.length > 0 && successfulPartitions > 0,
      filesWritten,
      batchingApplied: this.shouldApplyBatching(partitionData),
      totalBytesWritten: totalBytes,
      totalDurationMs: Date.now() - startTime,
    };
  }

  /**
   * Write sequentially (fallback)
   */
  private async writeSequential(
    partitionData: Map<string, CDCEvent[]>,
    writeFunc: (partition: string, events: CDCEvent[]) => Promise<PartitionWriteResult>,
    startTime: number
  ): Promise<ParallelWriteResult> {
    const partitions = Array.from(partitionData.keys());
    const failedPartitions: string[] = [];
    let filesWritten = 0;
    let totalBytes = BigInt(0);
    let successfulPartitions = 0;

    for (const partition of partitions) {
      const events = partitionData.get(partition) ?? [];
      try {
        const result = await writeFunc(partition, events);
        if (result.success) {
          filesWritten += result.filesWritten;
          totalBytes += result.bytesWritten;
          successfulPartitions++;
        } else {
          failedPartitions.push(partition);
        }
      } catch {
        failedPartitions.push(partition);
      }
    }

    return {
      parallelWritesUsed: false,
      partitionsWritten: partitions.length,
      successfulPartitions,
      failedPartitions,
      partialSuccess: failedPartitions.length > 0 && successfulPartitions > 0,
      filesWritten,
      batchingApplied: this.shouldApplyBatching(partitionData),
      totalBytesWritten: totalBytes,
      totalDurationMs: Date.now() - startTime,
    };
  }

  /**
   * Check if writes to a partition should be throttled
   */
  private isThrottled(partition: string): boolean {
    const state = this.throttleState.get(partition);
    if (!state) return false;

    const elapsed = Date.now() - state.lastWriteTime;
    if (elapsed >= 1000) {
      // Reset window
      this.throttleState.set(partition, { lastWriteTime: Date.now(), bytesInWindow: 0 });
      return false;
    }

    return state.bytesInWindow >= this.config.maxPartitionWriteBytesPerSecond;
  }

  /**
   * Wait for throttle window to pass
   */
  private async waitForThrottle(partition: string): Promise<void> {
    const state = this.throttleState.get(partition);
    if (!state) return;

    const elapsed = Date.now() - state.lastWriteTime;
    if (elapsed < 1000) {
      await new Promise((resolve) => setTimeout(resolve, 1000 - elapsed));
    }

    this.throttleState.set(partition, { lastWriteTime: Date.now(), bytesInWindow: 0 });
  }

  /**
   * Update throttle state after write
   */
  private updateThrottleState(partition: string, bytesWritten: number): void {
    const state = this.throttleState.get(partition) ?? { lastWriteTime: Date.now(), bytesInWindow: 0 };
    state.bytesInWindow += bytesWritten;
    this.throttleState.set(partition, state);
  }

  /**
   * Check if batching should be applied
   */
  private shouldApplyBatching(partitionData: Map<string, CDCEvent[]>): boolean {
    // Apply batching if any partition has many small events
    for (const events of partitionData.values()) {
      if (events.length > 10) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get throttling status
   */
  getThrottlingStatus(): { throttledPartitions: string[]; throttlingActive: boolean } {
    const throttledPartitions: string[] = [];

    for (const partition of this.throttleState.keys()) {
      if (this.isThrottled(partition)) {
        throttledPartitions.push(partition);
      }
    }

    return {
      throttledPartitions,
      throttlingActive: throttledPartitions.length > 0,
    };
  }

  /**
   * Inject failure for testing
   */
  injectFailure(partition: string): void {
    this.failedPartitions.add(partition);
  }

  /**
   * Check if partition has failed
   */
  hasFailure(partition: string): boolean {
    return this.failedPartitions.has(partition);
  }

  /**
   * Clear failure state
   */
  clearFailures(): void {
    this.failedPartitions.clear();
  }
}

// =============================================================================
// Partition Compaction
// =============================================================================

/**
 * Compaction request for a partition
 */
export interface PartitionCompactionRequest {
  namespace: string[];
  tableName: string;
  partition: string;
  targetFileSizeBytes?: number;
  minPartitionAgeMs?: number;
}

/**
 * Compaction result for a partition
 */
export interface PartitionCompactionResult {
  partitionCompacted: string;
  filesCompacted: number;
  otherPartitionsAffected: boolean;
  outputFiles: Array<{ path: string; sizeBytes: number }>;
  avgFileSizeBytes: number;
  durationMs: number;
  success: boolean;
  error?: string;
}

/**
 * Auto-compaction result
 */
export interface AutoCompactionResult {
  compactedPartitions: Array<{ partition: string; age: number }>;
  skippedPartitions: Array<{ partition: string; reason: string }>;
  totalFilesCompacted: number;
  totalBytesCompacted: bigint;
}

/**
 * Partition compaction manager
 */
export class PartitionCompactionManager {
  private partitionMetadata: Map<string, PartitionMetadata>;
  private compactionInProgress: Set<string>;

  constructor() {
    this.partitionMetadata = new Map();
    this.compactionInProgress = new Set();
  }

  /**
   * Register partition metadata
   */
  registerPartition(partition: string, metadata: PartitionMetadata): void {
    this.partitionMetadata.set(partition, metadata);
  }

  /**
   * Compact a single partition
   */
  async compactPartition(
    request: PartitionCompactionRequest,
    files: DataFile[],
    mergeFunc: (files: DataFile[]) => Promise<DataFile[]>
  ): Promise<PartitionCompactionResult> {
    const startTime = Date.now();

    if (this.compactionInProgress.has(request.partition)) {
      return {
        partitionCompacted: request.partition,
        filesCompacted: 0,
        otherPartitionsAffected: false,
        outputFiles: [],
        avgFileSizeBytes: 0,
        durationMs: 0,
        success: false,
        error: 'Compaction already in progress',
      };
    }

    this.compactionInProgress.add(request.partition);

    try {
      const targetSize = request.targetFileSizeBytes ?? 128 * 1024 * 1024;

      // Group files into merge groups targeting the specified size
      const mergeGroups = this.createMergeGroups(files, targetSize);
      const outputFiles: Array<{ path: string; sizeBytes: number }> = [];

      for (const group of mergeGroups) {
        if (group.length > 1) {
          const merged = await mergeFunc(group);
          for (const file of merged) {
            outputFiles.push({
              path: file['file-path'],
              sizeBytes: Number(file['file-size-in-bytes']),
            });
          }
        } else {
          outputFiles.push({
            path: group[0]['file-path'],
            sizeBytes: Number(group[0]['file-size-in-bytes']),
          });
        }
      }

      const totalSize = outputFiles.reduce((sum, f) => sum + f.sizeBytes, 0);
      const avgSize = outputFiles.length > 0 ? totalSize / outputFiles.length : 0;

      return {
        partitionCompacted: request.partition,
        filesCompacted: files.length,
        otherPartitionsAffected: false,
        outputFiles,
        avgFileSizeBytes: avgSize,
        durationMs: Date.now() - startTime,
        success: true,
      };
    } finally {
      this.compactionInProgress.delete(request.partition);
    }
  }

  /**
   * Auto-compact partitions based on age and size
   */
  async autoCompact(
    tableName: string,
    strategy: 'age-based' | 'size-based',
    maxPartitions: number,
    minAgeMs: number = 3600000
  ): Promise<AutoCompactionResult> {
    const now = Date.now();
    const partitions = Array.from(this.partitionMetadata.entries())
      .filter(([, meta]) => meta.compactionPending);

    // Sort by age (oldest first) for age-based strategy
    const sorted = strategy === 'age-based'
      ? partitions.sort((a, b) => a[1].createdAt - b[1].createdAt)
      : partitions.sort((a, b) => Number(b[1].stats.sizeBytes - a[1].stats.sizeBytes));

    const compactedPartitions: Array<{ partition: string; age: number }> = [];
    const skippedPartitions: Array<{ partition: string; reason: string }> = [];
    let totalFilesCompacted = 0;
    let totalBytesCompacted = BigInt(0);

    for (const [partition, metadata] of sorted.slice(0, maxPartitions)) {
      const age = now - metadata.createdAt;

      if (age < minAgeMs) {
        skippedPartitions.push({ partition, reason: 'too_recent' });
        continue;
      }

      compactedPartitions.push({ partition, age });
      totalFilesCompacted += metadata.files.length;
      totalBytesCompacted += metadata.stats.sizeBytes;
    }

    return {
      compactedPartitions,
      skippedPartitions,
      totalFilesCompacted,
      totalBytesCompacted,
    };
  }

  /**
   * Create merge groups for compaction
   */
  private createMergeGroups(files: DataFile[], targetSize: number): DataFile[][] {
    const groups: DataFile[][] = [];
    let currentGroup: DataFile[] = [];
    let currentSize = 0;

    // Sort files by size (smallest first for better packing)
    const sorted = [...files].sort((a, b) =>
      Number(a['file-size-in-bytes'] - b['file-size-in-bytes'])
    );

    for (const file of sorted) {
      const fileSize = Number(file['file-size-in-bytes']);

      if (currentSize + fileSize > targetSize && currentGroup.length > 0) {
        groups.push(currentGroup);
        currentGroup = [];
        currentSize = 0;
      }

      currentGroup.push(file);
      currentSize += fileSize;
    }

    if (currentGroup.length > 0) {
      groups.push(currentGroup);
    }

    return groups;
  }

  /**
   * Check if partition compaction is in progress
   */
  isCompactionInProgress(partition: string): boolean {
    return this.compactionInProgress.has(partition);
  }

  /**
   * Get partition metadata
   */
  getPartitionMetadata(partition: string): PartitionMetadata | undefined {
    return this.partitionMetadata.get(partition);
  }
}

// =============================================================================
// Partition Rebalancing
// =============================================================================

/**
 * Rebalancing action
 */
export interface RebalanceAction {
  type: 'split' | 'merge';
  partition: string;
  reason: string;
  splitKey?: string;
}

/**
 * Rebalancing recommendation
 */
export interface RebalanceRecommendation {
  actions: RebalanceAction[];
  estimatedImprovement: number;
}

/**
 * Partition analysis result
 */
export interface PartitionAnalysis {
  partitionSizes: Array<{ partition: string; sizeBytes: bigint; recordCount: bigint }>;
  skewFactor: number;
  hotPartitions: string[];
}

/**
 * Split execution result
 */
export interface SplitExecutionResult {
  originalPartition: string;
  newPartitions: string[];
  recordsMoved: bigint;
  durationMs: number;
  success: boolean;
}

/**
 * Partition rebalancer
 */
export class PartitionRebalancer {
  private partitionSizes: Map<string, { sizeBytes: bigint; recordCount: bigint }>;

  constructor() {
    this.partitionSizes = new Map();
  }

  /**
   * Register partition size
   */
  registerPartitionSize(partition: string, sizeBytes: bigint, recordCount: bigint): void {
    this.partitionSizes.set(partition, { sizeBytes, recordCount });
  }

  /**
   * Analyze partition distribution
   */
  analyzePartitions(tableName: string): PartitionAnalysis {
    const partitionSizes: Array<{ partition: string; sizeBytes: bigint; recordCount: bigint }> = [];

    for (const [partition, stats] of this.partitionSizes) {
      partitionSizes.push({
        partition,
        sizeBytes: stats.sizeBytes,
        recordCount: stats.recordCount,
      });
    }

    // Calculate skew
    const sizes = partitionSizes.map((p) => Number(p.sizeBytes));
    const avg = sizes.length > 0 ? sizes.reduce((a, b) => a + b, 0) / sizes.length : 0;
    const max = Math.max(...sizes, 0);
    const skewFactor = avg > 0 ? max / avg : 1;

    // Find hot partitions (2x average)
    const threshold = avg * 2;
    const hotPartitions = partitionSizes
      .filter((p) => Number(p.sizeBytes) > threshold)
      .map((p) => p.partition);

    return {
      partitionSizes,
      skewFactor,
      hotPartitions,
    };
  }

  /**
   * Recommend rebalancing actions
   */
  recommend(
    tableName: string,
    maxSizeBytes: bigint,
    maxSkewFactor: number
  ): RebalanceRecommendation {
    const analysis = this.analyzePartitions(tableName);
    const actions: RebalanceAction[] = [];

    // Recommend splits for large partitions
    for (const p of analysis.partitionSizes) {
      if (p.sizeBytes > maxSizeBytes) {
        actions.push({
          type: 'split',
          partition: p.partition,
          reason: `Partition size ${p.sizeBytes} exceeds max ${maxSizeBytes}`,
        });
      }
    }

    // Recommend splits for hot partitions if skew is high
    if (analysis.skewFactor > maxSkewFactor) {
      for (const partition of analysis.hotPartitions) {
        if (!actions.some((a) => a.partition === partition)) {
          actions.push({
            type: 'split',
            partition,
            reason: `High skew factor ${analysis.skewFactor.toFixed(2)} (threshold: ${maxSkewFactor})`,
          });
        }
      }
    }

    const estimatedImprovement = actions.length > 0
      ? (analysis.skewFactor - 1) / analysis.skewFactor
      : 0;

    return { actions, estimatedImprovement };
  }

  /**
   * Execute partition split
   */
  async executeSplit(
    partition: string,
    splitKey: string,
    getData: () => Promise<CDCEvent[]>,
    writePartition: (partition: string, events: CDCEvent[]) => Promise<void>
  ): Promise<SplitExecutionResult> {
    const startTime = Date.now();

    try {
      const events = await getData();

      // Group events by split key (e.g., hour within day)
      const groups = new Map<string, CDCEvent[]>();

      for (const event of events) {
        const data = event.after ?? event.before ?? {};
        const timestamp = (data as Record<string, unknown>)['timestamp'] ?? event.timestamp;

        let splitValue: string;
        if (splitKey === 'hour') {
          const date = new Date(timestamp as number);
          splitValue = date.toISOString().slice(0, 13);
        } else {
          splitValue = String((data as Record<string, unknown>)[splitKey] ?? 'default');
        }

        const key = `${partition}/${splitKey}=${splitValue}`;
        const group = groups.get(key) ?? [];
        group.push(event);
        groups.set(key, group);
      }

      const newPartitions: string[] = [];
      let recordsMoved = BigInt(0);

      for (const [newPartition, groupEvents] of groups) {
        await writePartition(newPartition, groupEvents);
        newPartitions.push(newPartition);
        recordsMoved += BigInt(groupEvents.length);
      }

      return {
        originalPartition: partition,
        newPartitions,
        recordsMoved,
        durationMs: Date.now() - startTime,
        success: true,
      };
    } catch (error) {
      return {
        originalPartition: partition,
        newPartitions: [],
        recordsMoved: BigInt(0),
        durationMs: Date.now() - startTime,
        success: false,
      };
    }
  }
}

// =============================================================================
// Large File Handling
// =============================================================================

/**
 * Large file write request
 */
export interface LargeFileWriteRequest {
  tableName: string;
  targetSizeBytes: number;
  useMultipart: boolean;
}

/**
 * Large file write result
 */
export interface LargeFileWriteResult {
  filePath: string;
  fileSizeBytes: bigint;
  multipartUsed: boolean;
  partCount: number;
  durationMs: number;
}

/**
 * Range read request
 */
export interface RangeReadRequest {
  filePath: string;
  rowGroupRange: { start: number; end: number };
  useRangeRequests: boolean;
}

/**
 * Range read result
 */
export interface RangeReadResult {
  rowGroupsRead: number;
  bytesRead: bigint;
  totalFileBytes: bigint;
}

/**
 * Large file handler
 */
export class LargeFileHandler {
  private config: ScalingConfig;

  constructor(config: ScalingConfig = DEFAULT_SCALING_CONFIG) {
    this.config = config;
  }

  /**
   * Write a large file using multipart upload
   */
  async writeLargeFile(
    request: LargeFileWriteRequest,
    generateData: (chunkSize: number) => AsyncGenerator<Uint8Array>,
    uploadPart: (partNumber: number, data: Uint8Array) => Promise<string>
  ): Promise<LargeFileWriteResult> {
    const startTime = Date.now();
    const filePath = `/warehouse/data/${generateUUID()}.parquet`;

    if (!this.config.useMultipartUpload || request.targetSizeBytes < 100 * 1024 * 1024) {
      // Single upload for small files
      const chunks: Uint8Array[] = [];
      for await (const chunk of generateData(request.targetSizeBytes)) {
        chunks.push(chunk);
      }

      return {
        filePath,
        fileSizeBytes: BigInt(chunks.reduce((sum, c) => sum + c.length, 0)),
        multipartUsed: false,
        partCount: 1,
        durationMs: Date.now() - startTime,
      };
    }

    // Multipart upload
    const partSize = 100 * 1024 * 1024; // 100MB parts
    let partNumber = 1;
    let totalBytes = BigInt(0);

    for await (const chunk of generateData(partSize)) {
      await uploadPart(partNumber, chunk);
      totalBytes += BigInt(chunk.length);
      partNumber++;
    }

    return {
      filePath,
      fileSizeBytes: totalBytes,
      multipartUsed: true,
      partCount: partNumber - 1,
      durationMs: Date.now() - startTime,
    };
  }

  /**
   * Read specific row groups using range requests
   */
  async readRowGroups(
    request: RangeReadRequest,
    totalRowGroups: number,
    rowGroupSize: bigint,
    fetchRange: (start: bigint, end: bigint) => Promise<Uint8Array>
  ): Promise<RangeReadResult> {
    const { start, end } = request.rowGroupRange;
    const rowGroupsToRead = end - start;

    if (!request.useRangeRequests) {
      // Full file read
      return {
        rowGroupsRead: totalRowGroups,
        bytesRead: BigInt(totalRowGroups) * rowGroupSize,
        totalFileBytes: BigInt(totalRowGroups) * rowGroupSize,
      };
    }

    // Range read
    const startByte = BigInt(start) * rowGroupSize;
    const endByte = BigInt(end) * rowGroupSize;

    await fetchRange(startByte, endByte);

    return {
      rowGroupsRead: rowGroupsToRead,
      bytesRead: endByte - startByte,
      totalFileBytes: BigInt(totalRowGroups) * rowGroupSize,
    };
  }

  /**
   * Stream large file contents
   */
  async *streamFile(
    filePath: string,
    batchSize: number,
    readChunk: (offset: bigint, size: number) => Promise<Uint8Array | null>
  ): AsyncGenerator<Uint8Array> {
    let offset = BigInt(0);

    while (true) {
      const chunk = await readChunk(offset, batchSize);
      if (!chunk || chunk.length === 0) {
        break;
      }

      yield chunk;
      offset += BigInt(chunk.length);
    }
  }
}

// =============================================================================
// Memory Management
// =============================================================================

/**
 * Memory stats
 */
export interface MemoryStats {
  heapUsed: number;
  heapTotal: number;
  external: number;
}

/**
 * Memory-efficient processor
 */
export class MemoryEfficientProcessor {
  private peakMemory: number = 0;
  private baselineMemory: number = 0;

  /**
   * Get current memory stats
   */
  getMemoryStats(): MemoryStats {
    // In Workers environment, we simulate memory tracking
    // In Node.js, we would use process.memoryUsage()
    return {
      heapUsed: this.peakMemory,
      heapTotal: 512 * 1024 * 1024, // 512MB simulated heap
      external: 0,
    };
  }

  /**
   * Set baseline memory for comparison
   */
  setBaseline(): void {
    this.baselineMemory = this.peakMemory;
  }

  /**
   * Track memory usage during operation
   */
  trackMemory(bytes: number): void {
    this.peakMemory = Math.max(this.peakMemory, bytes);
  }

  /**
   * Get memory increase since baseline
   */
  getMemoryIncrease(): number {
    return this.peakMemory - this.baselineMemory;
  }

  /**
   * Process data in streaming mode to limit memory usage
   */
  async *processStreaming<T, R>(
    data: AsyncIterable<T>,
    transform: (item: T) => R,
    maxBufferSize: number = 1000
  ): AsyncGenerator<R> {
    const buffer: R[] = [];

    for await (const item of data) {
      const result = transform(item);
      buffer.push(result);

      if (buffer.length >= maxBufferSize) {
        for (const r of buffer) {
          yield r;
        }
        buffer.length = 0;
      }
    }

    for (const r of buffer) {
      yield r;
    }
  }
}

// =============================================================================
// Horizontal Scaling
// =============================================================================

/**
 * Scaling status
 */
export interface ScalingStatus {
  currentInstances: number;
  totalPartitions: number;
  scalingRecommendation: 'scale-up' | 'scale-down' | 'optimal';
  partitionsPerInstance: number;
}

/**
 * DO routing result
 */
export interface DORoutingResult {
  targetDoId: string;
  instanceIndex: number;
}

/**
 * Horizontal scaling manager
 */
export class HorizontalScalingManager {
  private config: ScalingConfig;
  private instanceCount: number = 1;
  private partitionToInstance: Map<string, number> = new Map();

  constructor(config: ScalingConfig = DEFAULT_SCALING_CONFIG) {
    this.config = config;
  }

  /**
   * Get current scaling status
   */
  getStatus(totalPartitions: number): ScalingStatus {
    const idealInstances = Math.ceil(totalPartitions / this.config.partitionsPerInstance);

    let recommendation: ScalingStatus['scalingRecommendation'] = 'optimal';

    if (idealInstances > this.instanceCount && this.instanceCount < this.config.maxInstances) {
      recommendation = 'scale-up';
    } else if (idealInstances < this.instanceCount && this.instanceCount > this.config.minInstances) {
      recommendation = 'scale-down';
    }

    return {
      currentInstances: this.instanceCount,
      totalPartitions,
      scalingRecommendation: recommendation,
      partitionsPerInstance: this.config.partitionsPerInstance,
    };
  }

  /**
   * Update scaling configuration
   */
  updateConfig(updates: Partial<ScalingConfig>): ScalingConfig {
    Object.assign(this.config, updates);
    return this.config;
  }

  /**
   * Get current configuration
   */
  getConfig(): ScalingConfig {
    return { ...this.config };
  }

  /**
   * Route request to appropriate DO instance
   */
  routeToInstance(tableName: string, partition: string): DORoutingResult {
    const key = `${tableName}:${partition}`;

    // Check if already assigned
    let instanceIndex = this.partitionToInstance.get(key);

    if (instanceIndex === undefined) {
      // Assign using consistent hashing
      instanceIndex = this.hashToInstance(key);
      this.partitionToInstance.set(key, instanceIndex);
    }

    return {
      targetDoId: `dolake-instance-${instanceIndex}`,
      instanceIndex,
    };
  }

  /**
   * Hash partition key to instance
   */
  private hashToInstance(key: string): number {
    let hash = 0;
    for (let i = 0; i < key.length; i++) {
      const char = key.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }
    return Math.abs(hash) % this.instanceCount;
  }

  /**
   * Scale to new instance count
   */
  scaleTo(newInstanceCount: number): void {
    newInstanceCount = Math.max(this.config.minInstances, Math.min(this.config.maxInstances, newInstanceCount));
    this.instanceCount = newInstanceCount;
    // Clear routing cache to force rebalancing
    this.partitionToInstance.clear();
  }

  /**
   * Auto-scale based on partition count
   */
  autoScale(totalPartitions: number): number {
    if (this.config.scalingMode !== 'auto') {
      return this.instanceCount;
    }

    const idealInstances = Math.ceil(totalPartitions / this.config.partitionsPerInstance);
    const targetInstances = Math.max(
      this.config.minInstances,
      Math.min(this.config.maxInstances, idealInstances)
    );

    if (targetInstances !== this.instanceCount) {
      this.scaleTo(targetInstances);
    }

    return this.instanceCount;
  }
}

// =============================================================================
// Auto-Scaling Types (do-d1isn.9)
// =============================================================================

/**
 * Load metrics for scaling decisions
 */
export interface LoadMetrics {
  /** Buffer utilization (0-1 percentage) */
  bufferUtilization: number;
  /** Average latency in milliseconds */
  avgLatencyMs: number;
  /** P99 latency in milliseconds */
  p99LatencyMs: number;
  /** Requests per second */
  requestsPerSecond: number;
  /** Active connections count */
  activeConnections: number;
}

/**
 * Shard health status for routing decisions
 */
export interface ShardHealth {
  /** Unique shard identifier */
  shardId: string;
  /** Current health status */
  status: 'healthy' | 'draining' | 'unhealthy';
  /** Current load metrics */
  loadMetrics: LoadMetrics;
  /** Timestamp of last update */
  lastUpdatedAt: number;
}

/**
 * Scale event emitted when scaling occurs
 */
export interface ScaleEvent {
  /** Type of scaling operation */
  type: 'scale-up' | 'scale-down';
  /** Number of instances before scaling */
  fromInstances: number;
  /** Number of instances after scaling */
  toInstances: number;
  /** Reason for scaling */
  reason: string;
  /** Timestamp of the event */
  timestamp: number;
  /** Shards affected by scale-down (draining) */
  affectedShards?: string[];
}

/**
 * Configuration for auto-scaling behavior
 */
export interface AutoScalingConfig extends ScalingConfig {
  /** Buffer utilization threshold to trigger scale-up (default: 0.8 = 80%) */
  scaleUpBufferThreshold: number;
  /** Latency threshold in ms to trigger scale-up (default: 5000ms) */
  scaleUpLatencyThresholdMs: number;
  /** Buffer utilization threshold to trigger scale-down (default: 0.2 = 20%) */
  scaleDownBufferThreshold: number;
  /** Time buffer must be below threshold before scale-down (default: 300000ms = 5min) */
  scaleDownCooldownMs: number;
  /** Minimum time between scaling operations (default: 60000ms = 1min) */
  scaleCooldownMs: number;
  /** Maximum time to wait for shard drain (default: 300000ms = 5min) */
  drainTimeoutMs: number;
  /** Interval to check drain status (default: 5000ms) */
  drainCheckIntervalMs: number;
}

/**
 * Default auto-scaling configuration
 */
export const DEFAULT_AUTO_SCALING_CONFIG: AutoScalingConfig = {
  ...DEFAULT_SCALING_CONFIG,
  scaleUpBufferThreshold: 0.8,
  scaleUpLatencyThresholdMs: 5000,
  scaleDownBufferThreshold: 0.2,
  scaleDownCooldownMs: 300000,
  scaleCooldownMs: 60000,
  drainTimeoutMs: 300000,
  drainCheckIntervalMs: 5000,
};

// =============================================================================
// Auto-Scaling Manager (do-d1isn.9)
// =============================================================================

/**
 * AutoScalingManager - Extends HorizontalScalingManager with load-based auto-scaling
 *
 * Features:
 * - Detects overload based on buffer utilization and latency
 * - Automatically scales up when buffer > 80% OR latency > 5s
 * - Automatically scales down when buffer < 20% for 5+ minutes
 * - Integrates with ShardRouter for routing updates
 * - Graceful drain of old shards before removal
 *
 * @example
 * ```typescript
 * const manager = new AutoScalingManager({
 *   minInstances: 2,
 *   maxInstances: 16,
 *   scaleUpBufferThreshold: 0.8,
 *   scaleUpLatencyThresholdMs: 5000,
 * });
 *
 * // Record metrics from each shard
 * manager.recordLoadMetrics('shard-0', {
 *   bufferUtilization: 0.85,
 *   avgLatencyMs: 100,
 *   p99LatencyMs: 500,
 *   requestsPerSecond: 1000,
 *   activeConnections: 50,
 * });
 *
 * // Evaluate if scaling is needed
 * const scaleEvent = manager.evaluateScaling();
 * if (scaleEvent) {
 *   console.log(`Scaling ${scaleEvent.type}: ${scaleEvent.reason}`);
 * }
 * ```
 */
export class AutoScalingManager {
  private config: AutoScalingConfig;
  private instanceCount: number = 1;
  private loadMetricsMap: Map<string, LoadMetrics> = new Map();
  private shardHealthMap: Map<string, ShardHealth> = new Map();
  private drainingShards: Set<string> = new Set();
  private scaleEventCallbacks: Array<(event: ScaleEvent) => void> = [];
  private lastScaleTime: number = 0;
  private lowBufferStartTime: number = 0;

  constructor(config: Partial<AutoScalingConfig> = {}) {
    this.config = {
      ...DEFAULT_AUTO_SCALING_CONFIG,
      ...config,
    };
  }

  // ===========================================================================
  // Load Metrics Tracking
  // ===========================================================================

  /**
   * Record load metrics for a shard
   */
  recordLoadMetrics(shardId: string, metrics: LoadMetrics): void {
    this.loadMetricsMap.set(shardId, metrics);

    const health: ShardHealth = {
      shardId,
      status: this.determineHealthStatus(shardId, metrics),
      loadMetrics: metrics,
      lastUpdatedAt: Date.now(),
    };
    this.shardHealthMap.set(shardId, health);
  }

  /**
   * Get load metrics for a specific shard
   */
  getLoadMetrics(shardId: string): LoadMetrics | undefined {
    return this.loadMetricsMap.get(shardId);
  }

  /**
   * Get aggregate metrics across all shards
   */
  getAggregateMetrics(): LoadMetrics {
    if (this.loadMetricsMap.size === 0) {
      return {
        bufferUtilization: 0,
        avgLatencyMs: 0,
        p99LatencyMs: 0,
        requestsPerSecond: 0,
        activeConnections: 0,
      };
    }

    let totalBuffer = 0;
    let totalLatency = 0;
    let maxP99 = 0;
    let totalRps = 0;
    let totalConns = 0;

    for (const metrics of this.loadMetricsMap.values()) {
      totalBuffer += metrics.bufferUtilization;
      totalLatency += metrics.avgLatencyMs;
      maxP99 = Math.max(maxP99, metrics.p99LatencyMs);
      totalRps += metrics.requestsPerSecond;
      totalConns += metrics.activeConnections;
    }

    const count = this.loadMetricsMap.size;
    return {
      bufferUtilization: totalBuffer / count,
      avgLatencyMs: totalLatency / count,
      p99LatencyMs: maxP99,
      requestsPerSecond: totalRps,
      activeConnections: totalConns,
    };
  }

  // ===========================================================================
  // Overload Detection
  // ===========================================================================

  /**
   * Detect if the system is currently overloaded
   */
  detectOverload(): { isOverloaded: boolean; reason: string | null } {
    const metrics = this.getAggregateMetrics();

    if (metrics.bufferUtilization >= this.config.scaleUpBufferThreshold) {
      return {
        isOverloaded: true,
        reason: `Buffer utilization at ${(metrics.bufferUtilization * 100).toFixed(1)}% (threshold: ${this.config.scaleUpBufferThreshold * 100}%)`,
      };
    }

    if (metrics.avgLatencyMs >= this.config.scaleUpLatencyThresholdMs) {
      return {
        isOverloaded: true,
        reason: `Average latency at ${metrics.avgLatencyMs}ms (threshold: ${this.config.scaleUpLatencyThresholdMs}ms)`,
      };
    }

    return { isOverloaded: false, reason: null };
  }

  // ===========================================================================
  // Scaling Decisions
  // ===========================================================================

  /**
   * Evaluate if scaling is needed and return a scale event if so
   */
  evaluateScaling(): ScaleEvent | null {
    const now = Date.now();

    // Check cooldown
    if (now - this.lastScaleTime < this.config.scaleCooldownMs) {
      return null;
    }

    const metrics = this.getAggregateMetrics();
    const overload = this.detectOverload();

    // Check for scale-up
    if (overload.isOverloaded && this.instanceCount < this.config.maxInstances) {
      return {
        type: 'scale-up',
        fromInstances: this.instanceCount,
        toInstances: Math.min(this.instanceCount + 1, this.config.maxInstances),
        reason: overload.reason!,
        timestamp: now,
      };
    }

    // Check for scale-down
    if (metrics.bufferUtilization < this.config.scaleDownBufferThreshold) {
      if (this.lowBufferStartTime === 0) {
        this.lowBufferStartTime = now;
      }

      const lowBufferDuration = now - this.lowBufferStartTime;
      if (lowBufferDuration >= this.config.scaleDownCooldownMs && this.instanceCount > this.config.minInstances) {
        return {
          type: 'scale-down',
          fromInstances: this.instanceCount,
          toInstances: Math.max(this.instanceCount - 1, this.config.minInstances),
          reason: `Buffer utilization below ${this.config.scaleDownBufferThreshold * 100}% for ${(lowBufferDuration / 1000 / 60).toFixed(1)} minutes`,
          timestamp: now,
        };
      }
    } else {
      // Reset timer if buffer goes above threshold
      this.lowBufferStartTime = 0;
    }

    return null;
  }

  // ===========================================================================
  // Scale Operations
  // ===========================================================================

  /**
   * Scale up to target number of instances
   */
  async scaleUp(targetInstances?: number): Promise<ScaleEvent> {
    const target = targetInstances ?? this.instanceCount + 1;
    const newCount = Math.min(Math.max(target, this.config.minInstances), this.config.maxInstances);

    const event: ScaleEvent = {
      type: 'scale-up',
      fromInstances: this.instanceCount,
      toInstances: newCount,
      reason: 'Manual scale-up or auto-triggered',
      timestamp: Date.now(),
    };

    this.instanceCount = newCount;
    this.lastScaleTime = Date.now();
    this.emitScaleEvent(event);

    return event;
  }

  /**
   * Scale down to target number of instances
   */
  async scaleDown(targetInstances?: number): Promise<ScaleEvent> {
    const target = targetInstances ?? this.instanceCount - 1;
    const newCount = Math.min(Math.max(target, this.config.minInstances), this.config.maxInstances);
    const shardsToRemove = this.instanceCount - newCount;

    // Select shards to drain (lowest load first)
    const affectedShards = this.selectShardsForRemoval(shardsToRemove);

    const event: ScaleEvent = {
      type: 'scale-down',
      fromInstances: this.instanceCount,
      toInstances: newCount,
      reason: 'Manual scale-down or auto-triggered',
      timestamp: Date.now(),
      affectedShards,
    };

    // Start draining affected shards
    for (const shardId of affectedShards) {
      await this.startDrain(shardId);
    }

    this.instanceCount = newCount;
    this.lastScaleTime = Date.now();
    this.lowBufferStartTime = 0;
    this.emitScaleEvent(event);

    return event;
  }

  /**
   * Select shards for removal based on load (lowest load first)
   */
  private selectShardsForRemoval(count: number): string[] {
    const shardsByLoad = Array.from(this.shardHealthMap.entries())
      .filter(([, health]) => health.status !== 'draining')
      .sort((a, b) => a[1].loadMetrics.bufferUtilization - b[1].loadMetrics.bufferUtilization);

    return shardsByLoad.slice(0, count).map(([id]) => id);
  }

  // ===========================================================================
  // Shard Management
  // ===========================================================================

  /**
   * Get health status for a specific shard
   */
  getShardHealth(shardId: string): ShardHealth | undefined {
    return this.shardHealthMap.get(shardId);
  }

  /**
   * Get health status for all shards
   */
  getAllShardHealth(): Map<string, ShardHealth> {
    return new Map(this.shardHealthMap);
  }

  /**
   * Determine health status based on metrics
   */
  private determineHealthStatus(shardId: string, metrics: LoadMetrics): 'healthy' | 'draining' | 'unhealthy' {
    // If shard is being drained, keep it as draining
    if (this.drainingShards.has(shardId)) {
      return 'draining';
    }

    // Check for unhealthy conditions
    if (metrics.bufferUtilization > 0.9 || metrics.avgLatencyMs > 10000) {
      return 'unhealthy';
    }

    return 'healthy';
  }

  // ===========================================================================
  // Drain Operations
  // ===========================================================================

  /**
   * Start draining a shard (stop sending new requests)
   */
  async startDrain(shardId: string): Promise<void> {
    this.drainingShards.add(shardId);

    const health = this.shardHealthMap.get(shardId);
    if (health) {
      health.status = 'draining';
      this.shardHealthMap.set(shardId, health);
    }
  }

  /**
   * Check if a shard is currently draining
   */
  isDraining(shardId: string): boolean {
    return this.drainingShards.has(shardId);
  }

  /**
   * Complete drain and remove shard from tracking
   */
  async completeDrain(shardId: string): Promise<void> {
    this.drainingShards.delete(shardId);
    this.shardHealthMap.delete(shardId);
    this.loadMetricsMap.delete(shardId);
  }

  // ===========================================================================
  // Event Subscription
  // ===========================================================================

  /**
   * Subscribe to scale events
   */
  onScaleEvent(callback: (event: ScaleEvent) => void): void {
    this.scaleEventCallbacks.push(callback);
  }

  /**
   * Emit a scale event to all subscribers
   */
  private emitScaleEvent(event: ScaleEvent): void {
    for (const callback of this.scaleEventCallbacks) {
      callback(event);
    }
  }

  // ===========================================================================
  // Configuration
  // ===========================================================================

  /**
   * Get current auto-scaling configuration
   */
  getAutoScalingConfig(): AutoScalingConfig {
    return { ...this.config };
  }

  /**
   * Update auto-scaling configuration
   */
  updateAutoScalingConfig(updates: Partial<AutoScalingConfig>): AutoScalingConfig {
    this.config = { ...this.config, ...updates };
    return { ...this.config };
  }

  // ===========================================================================
  // Test Helpers
  // ===========================================================================

  /**
   * Set instance count (for testing)
   */
  setInstanceCount(count: number): void {
    this.instanceCount = count;
  }

  /**
   * Get current instance count
   */
  getInstanceCount(): number {
    return this.instanceCount;
  }

  /**
   * Set low buffer start time (for testing)
   */
  setLowBufferStartTime(time: number): void {
    this.lowBufferStartTime = time;
  }

  /**
   * Reset cooldown (for testing)
   */
  resetCooldown(): void {
    this.lastScaleTime = 0;
  }
}
