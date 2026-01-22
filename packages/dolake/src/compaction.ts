/**
 * DoLake Compaction Manager
 *
 * Manages manifest file compaction for DoLake Parquet files.
 * Identifies small files, merges them into larger ones, and updates manifests atomically.
 *
 * Issue: pocs-kh7k - Add manifest file compaction for DoLake Parquet files
 */

import {
  type DataFile,
  type ManifestFile,
  type IcebergTableMetadata,
  type IcebergSnapshot,
  generateUUID,
  generateSnapshotId,
} from './types.js';
import { partitionToPath } from './iceberg.js';

// =============================================================================
// Compaction Configuration
// =============================================================================

/**
 * Compaction configuration options
 */
export interface CompactionConfig {
  /** Minimum file size in bytes - files below this are considered for compaction */
  minFileSizeBytes: number;
  /** Target file size for compacted output */
  targetFileSizeBytes: number;
  /** Maximum number of files to compact in a single operation */
  maxFilesToCompact: number;
  /** Minimum number of files required to trigger compaction */
  minFilesToCompact: number;
  /** Number of small files that triggers automatic compaction */
  compactionTriggerThreshold: number;
  /** Enable automatic compaction after flush */
  enableAutoCompaction: boolean;
}

/**
 * Default compaction configuration
 */
export const DEFAULT_COMPACTION_CONFIG: CompactionConfig = {
  minFileSizeBytes: 8 * 1024 * 1024, // 8MB
  targetFileSizeBytes: 128 * 1024 * 1024, // 128MB
  maxFilesToCompact: 100,
  minFilesToCompact: 2,
  compactionTriggerThreshold: 10,
  enableAutoCompaction: true,
};

// =============================================================================
// Compaction Types
// =============================================================================

/**
 * Information about a file eligible for compaction
 */
export interface FileInfo {
  path: string;
  sizeBytes: bigint;
  recordCount: bigint;
  partition: Record<string, unknown>;
}

/**
 * A group of files selected for compaction
 */
export interface CompactionCandidate {
  /** Files to be compacted together */
  files: DataFile[];
  /** Estimated size of the output file */
  estimatedOutputSize: bigint;
  /** Partition key (if any) */
  partitionKey: string | null;
  /** Priority score for scheduling */
  priority: number;
}

/**
 * Result of a compaction operation
 */
export interface CompactionResult {
  /** Whether the compaction succeeded */
  success: boolean;
  /** Number of input files compacted */
  filesCompacted: number;
  /** Total bytes of input files */
  bytesCompacted: bigint;
  /** Number of output files created */
  outputFiles: number;
  /** Total bytes of output files */
  outputBytes: bigint;
  /** Duration in milliseconds */
  durationMs: number;
  /** Error message if failed */
  error?: string;
}

/**
 * Compaction metrics for monitoring
 */
export interface CompactionMetrics {
  /** Total compaction operations attempted */
  totalCompactions: number;
  /** Successful compaction operations */
  successfulCompactions: number;
  /** Failed compaction operations */
  failedCompactions: number;
  /** Total files compacted */
  filesCompacted: number;
  /** Total bytes compacted */
  bytesCompacted: bigint;
  /** Timestamp of last compaction */
  lastCompactionTime: number;
  /** Average compaction duration in ms */
  averageDurationMs: number;
}

/**
 * Space savings estimation
 */
export interface SpaceSavings {
  /** Estimated reduction in file count */
  estimatedFileReduction: number;
  /** Estimated size reduction in bytes */
  estimatedSizeReduction: bigint;
  /** Compression ratio (output/input) */
  compressionRatio: number;
}

/**
 * Atomic commit preparation result
 */
export interface AtomicCommitPreparation {
  /** New manifest to be written */
  manifest: ManifestFile;
  /** Updated table metadata */
  metadata: IcebergTableMetadata;
  /** Files to delete after successful commit */
  filesToDelete: string[];
}

// =============================================================================
// Compaction Error
// =============================================================================

/**
 * Error thrown during compaction operations
 */
export class CompactionError extends Error {
  constructor(
    message: string,
    public readonly code: string = 'COMPACTION_ERROR',
    public readonly retryable: boolean = true
  ) {
    super(message);
    this.name = 'CompactionError';
  }
}

// =============================================================================
// Compaction Manager
// =============================================================================

/**
 * Compaction Manager
 *
 * Manages the compaction of small Parquet files into larger ones to
 * improve query performance and reduce file count overhead.
 */
export class CompactionManager {
  private config: CompactionConfig;
  private metrics: CompactionMetrics = {
    totalCompactions: 0,
    successfulCompactions: 0,
    failedCompactions: 0,
    filesCompacted: 0,
    bytesCompacted: BigInt(0),
    lastCompactionTime: 0,
    averageDurationMs: 0,
  };
  private totalDurationMs = 0;

  constructor(config: Partial<CompactionConfig> = {}) {
    this.config = { ...DEFAULT_COMPACTION_CONFIG, ...config };
    this.validateConfig();
  }

  /**
   * Validate the configuration
   */
  private validateConfig(): void {
    if (this.config.minFilesToCompact < 1) {
      throw new CompactionError(
        'minFilesToCompact must be at least 1',
        'INVALID_CONFIG',
        false
      );
    }
    if (this.config.maxFilesToCompact < this.config.minFilesToCompact) {
      throw new CompactionError(
        'maxFilesToCompact must be >= minFilesToCompact',
        'INVALID_CONFIG',
        false
      );
    }
    if (this.config.targetFileSizeBytes <= this.config.minFileSizeBytes) {
      throw new CompactionError(
        'targetFileSizeBytes must be > minFileSizeBytes',
        'INVALID_CONFIG',
        false
      );
    }
  }

  /**
   * Get the current configuration
   */
  getConfig(): CompactionConfig {
    return { ...this.config };
  }

  // ===========================================================================
  // Small File Identification
  // ===========================================================================

  /**
   * Identify files that are smaller than or equal to the minimum size threshold
   */
  identifySmallFiles(files: DataFile[]): DataFile[] {
    const threshold = BigInt(this.config.minFileSizeBytes);
    return files.filter((file) => {
      const size = file['file-size-in-bytes'];
      // Handle both bigint and number comparisons safely
      // Files at or below threshold are considered small
      return size <= threshold;
    });
  }

  /**
   * Group files by their partition key
   */
  groupByPartition(files: DataFile[]): Map<string, DataFile[]> {
    const groups = new Map<string, DataFile[]>();

    for (const file of files) {
      const partitionKey = partitionToPath(file.partition) ?? '__unpartitioned__';
      let group = groups.get(partitionKey);
      if (!group) {
        group = [];
        groups.set(partitionKey, group);
      }
      group.push(file);
    }

    return groups;
  }

  // ===========================================================================
  // Compaction Candidate Selection
  // ===========================================================================

  /**
   * Select files eligible for compaction
   *
   * Returns groups of files that should be compacted together,
   * respecting partition boundaries and size limits.
   */
  selectCompactionCandidates(files: DataFile[]): CompactionCandidate[] {
    const smallFiles = this.identifySmallFiles(files);

    // Need at least minFilesToCompact small files
    if (smallFiles.length < this.config.minFilesToCompact) {
      return [];
    }

    // Group by partition
    const byPartition = this.groupByPartition(smallFiles);
    const candidates: CompactionCandidate[] = [];

    for (const [partitionKey, partitionFiles] of byPartition) {
      // Skip if not enough files in this partition
      if (partitionFiles.length < this.config.minFilesToCompact) {
        continue;
      }

      // Create compaction groups based on target size
      const groups = this.createCompactionGroups(partitionFiles);

      for (const group of groups) {
        const estimatedOutputSize = group.reduce(
          (sum, f) => sum + f['file-size-in-bytes'],
          BigInt(0)
        );

        candidates.push({
          files: group,
          estimatedOutputSize,
          partitionKey: partitionKey === '__unpartitioned__' ? null : partitionKey,
          priority: this.calculatePriority(group),
        });
      }
    }

    // Sort by priority (highest first)
    return candidates.sort((a, b) => b.priority - a.priority);
  }

  /**
   * Create groups of files for compaction based on target size
   */
  private createCompactionGroups(files: DataFile[]): DataFile[][] {
    const groups: DataFile[][] = [];
    let currentGroup: DataFile[] = [];
    let currentSize = BigInt(0);
    const targetSize = BigInt(this.config.targetFileSizeBytes);

    // Sort files by size (smallest first) to pack efficiently
    const sortedFiles = [...files].sort((a, b) => {
      const sizeA = a['file-size-in-bytes'];
      const sizeB = b['file-size-in-bytes'];
      return sizeA < sizeB ? -1 : sizeA > sizeB ? 1 : 0;
    });

    for (const file of sortedFiles) {
      const fileSize = file['file-size-in-bytes'];

      // Check if adding this file would exceed target size
      if (currentSize + fileSize > targetSize && currentGroup.length >= this.config.minFilesToCompact) {
        // Start a new group
        groups.push(currentGroup);
        currentGroup = [];
        currentSize = BigInt(0);
      }

      // Check if we've hit the max files per compaction
      if (currentGroup.length >= this.config.maxFilesToCompact) {
        groups.push(currentGroup);
        currentGroup = [];
        currentSize = BigInt(0);
      }

      currentGroup.push(file);
      currentSize += fileSize;
    }

    // Add the last group if it meets minimum requirements
    if (currentGroup.length >= this.config.minFilesToCompact) {
      groups.push(currentGroup);
    }

    return groups;
  }

  /**
   * Calculate priority score for a compaction candidate
   */
  private calculatePriority(files: DataFile[]): number {
    // Priority based on:
    // 1. Number of files (more files = higher priority)
    // 2. Total size (smaller total = higher priority for space savings)
    // 3. Average file size (smaller average = higher priority)

    const fileCount = files.length;
    const totalSize = Number(files.reduce((sum, f) => sum + f['file-size-in-bytes'], BigInt(0)));
    const avgSize = totalSize / fileCount;

    // Normalize factors
    const countScore = Math.min(fileCount / this.config.maxFilesToCompact, 1) * 40;
    const sizeScore = Math.max(0, 1 - totalSize / Number(this.config.targetFileSizeBytes)) * 30;
    const avgSizeScore = Math.max(0, 1 - avgSize / Number(this.config.minFileSizeBytes)) * 30;

    return countScore + sizeScore + avgSizeScore;
  }

  // ===========================================================================
  // Manifest Operations
  // ===========================================================================

  /**
   * Create a new manifest for compacted files
   */
  createCompactedManifest(
    oldManifests: ManifestFile[],
    compactedFile: DataFile,
    snapshotId: bigint
  ): ManifestFile {
    // Calculate total deleted files count from old manifests
    const deletedFilesCount = oldManifests.reduce(
      (sum, m) => sum + m['added-files-count'],
      0
    );

    return {
      'manifest-path': `metadata/${generateUUID()}-manifest.avro`,
      'manifest-length': BigInt(0), // Will be set after writing
      'partition-spec-id': 0,
      content: 'data',
      'sequence-number': snapshotId,
      'min-sequence-number': snapshotId,
      'added-snapshot-id': snapshotId,
      'added-files-count': 1,
      'existing-files-count': 0,
      'deleted-files-count': deletedFilesCount,
      'added-rows-count': compactedFile['record-count'],
      'existing-rows-count': BigInt(0),
      'deleted-rows-count': BigInt(0),
    };
  }

  /**
   * Prepare an atomic commit for compaction
   */
  prepareAtomicCommit(
    metadata: IcebergTableMetadata,
    manifestsToReplace: ManifestFile[],
    compactedFile: DataFile
  ): AtomicCommitPreparation {
    const snapshotId = generateSnapshotId();

    // Create new manifest for compacted file
    const manifest = this.createCompactedManifest(
      manifestsToReplace,
      compactedFile,
      snapshotId
    );

    // Collect files to delete after successful commit
    const filesToDelete = manifestsToReplace.map((m) => m['manifest-path']);

    // We would update metadata here, but for now return placeholder
    // The actual metadata update happens in the commit phase
    return {
      manifest,
      metadata,
      filesToDelete,
    };
  }

  /**
   * Prepare a rollback in case of failure
   */
  prepareRollback(manifestsToRestore: ManifestFile[]): {
    manifests: ManifestFile[];
    restoreOperations: Array<{ action: 'restore'; path: string }>;
  } {
    return {
      manifests: manifestsToRestore,
      restoreOperations: manifestsToRestore.map((m) => ({
        action: 'restore' as const,
        path: m['manifest-path'],
      })),
    };
  }

  // ===========================================================================
  // Space Savings Estimation
  // ===========================================================================

  /**
   * Estimate space savings from compacting files
   */
  estimateSpaceSavings(files: DataFile[]): SpaceSavings {
    if (files.length === 0) {
      return {
        estimatedFileReduction: 0,
        estimatedSizeReduction: BigInt(0),
        compressionRatio: 1,
      };
    }

    const totalSize = files.reduce(
      (sum, f) => sum + f['file-size-in-bytes'],
      BigInt(0)
    );

    // Estimate output files based on target size
    const targetSize = BigInt(this.config.targetFileSizeBytes);
    const estimatedOutputFiles = Math.ceil(Number(totalSize) / Number(targetSize));

    // Compaction typically results in slightly smaller files due to
    // better compression and reduced metadata overhead
    const compressionRatio = 0.95; // Assume 5% space savings from compaction
    const estimatedOutputSize = BigInt(Math.floor(Number(totalSize) * compressionRatio));

    return {
      estimatedFileReduction: files.length - estimatedOutputFiles,
      estimatedSizeReduction: totalSize - estimatedOutputSize,
      compressionRatio,
    };
  }

  /**
   * Calculate compression ratio between input and output
   */
  calculateCompressionRatio(inputSize: bigint, outputSize: bigint): number {
    if (inputSize === BigInt(0)) {
      return 1;
    }
    return Number(outputSize) / Number(inputSize);
  }

  // ===========================================================================
  // Metrics
  // ===========================================================================

  /**
   * Get current compaction metrics
   */
  getMetrics(): CompactionMetrics {
    return { ...this.metrics };
  }

  /**
   * Record the result of a compaction operation
   */
  recordCompactionResult(result: CompactionResult): void {
    this.metrics.totalCompactions++;

    if (result.success) {
      this.metrics.successfulCompactions++;
      this.metrics.filesCompacted += result.filesCompacted;
      this.metrics.bytesCompacted += result.bytesCompacted;
      this.totalDurationMs += result.durationMs;
      this.metrics.averageDurationMs = Math.floor(
        this.totalDurationMs / this.metrics.successfulCompactions
      );
    } else {
      this.metrics.failedCompactions++;
    }

    this.metrics.lastCompactionTime = Date.now();
  }

  /**
   * Reset metrics (for testing)
   */
  resetMetrics(): void {
    this.metrics = {
      totalCompactions: 0,
      successfulCompactions: 0,
      failedCompactions: 0,
      filesCompacted: 0,
      bytesCompacted: BigInt(0),
      lastCompactionTime: 0,
      averageDurationMs: 0,
    };
    this.totalDurationMs = 0;
  }
}
