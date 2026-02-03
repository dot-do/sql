/**
 * DoLake Compaction Manager
 *
 * Manages manifest file compaction for DoLake Parquet files.
 * Identifies small files, merges them into larger ones, and updates manifests atomically.
 *
 * Issue: pocs-kh7k - Add manifest file compaction for DoLake Parquet files
 * Issue: sql-8euu - Add write coordination with locking for compaction
 */

import {
  type DataFile,
  type ManifestFile,
  type IcebergTableMetadata,
  generateUUID,
  generateSnapshotId,
} from './types.js';
import { partitionToPath } from './iceberg.js';
import { CompactionError } from './errors.js';

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
export const DEFAULT_COMPACTION_CONFIG: Readonly<CompactionConfig> = {
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

// Re-export CompactionError from errors module
export { CompactionError } from './errors.js';

// =============================================================================
// Compaction Coordination Types
// =============================================================================

/**
 * Lock mode for compaction coordination
 */
export type LockMode = 'exclusive' | 'shared';

/**
 * Lock state representing current write/compaction locks
 */
export interface LockState {
  /** Whether an exclusive lock is held */
  exclusiveLockHolder: string | null;
  /** Set of shared lock holders (write operations) */
  sharedLockHolders: Set<string>;
  /** Timestamp when the lock was acquired */
  acquiredAt: number;
  /** Lock expiration time (for lease-based coordination) */
  expiresAt: number;
}

/**
 * Lease for compaction coordination
 */
export interface CompactionLease {
  /** Unique lease ID */
  leaseId: string;
  /** Table/partition being compacted */
  targetKey: string;
  /** Worker ID holding the lease */
  workerId: string;
  /** Timestamp when lease was acquired */
  acquiredAt: number;
  /** Timestamp when lease expires */
  expiresAt: number;
  /** Number of times lease has been renewed */
  renewCount: number;
}

/**
 * Compaction checkpoint for resumable compaction
 */
export interface CompactionCheckpoint {
  /** Unique checkpoint ID */
  checkpointId: string;
  /** Table/partition being compacted */
  targetKey: string;
  /** Current phase of compaction */
  phase: CompactionPhase;
  /** Files already processed */
  processedFiles: string[];
  /** Files remaining to process */
  remainingFiles: string[];
  /** Output file path (if any) */
  outputFilePath: string | null;
  /** Timestamp when checkpoint was created */
  createdAt: number;
  /** Timestamp of last update */
  updatedAt: number;
  /** Associated lease ID */
  leaseId: string | null;
  /** Sequence number at start of compaction */
  startSequenceNumber: bigint;
  /** Any intermediate data */
  intermediateData: Record<string, unknown>;
}

/**
 * Phases of compaction operation
 */
export type CompactionPhase =
  | 'initializing'
  | 'acquiring_lock'
  | 'reading_files'
  | 'merging'
  | 'writing_output'
  | 'updating_manifest'
  | 'cleaning_up'
  | 'completed'
  | 'failed';

/**
 * Result of a lock acquisition attempt
 */
export interface LockAcquisitionResult {
  /** Whether the lock was acquired */
  acquired: boolean;
  /** Lock holder ID if acquired */
  lockId: string | null;
  /** Reason for failure if not acquired */
  reason: string | null;
  /** Time to wait before retry (ms) */
  retryAfterMs: number | null;
  /** Current holder if lock is held by another */
  currentHolder: string | null;
}

/**
 * Conflict detection result
 */
export interface ConflictCheckResult {
  /** Whether a conflict was detected */
  hasConflict: boolean;
  /** Type of conflict */
  conflictType: 'new_data' | 'concurrent_compaction' | 'schema_change' | null;
  /** Details about the conflict */
  details: string | null;
  /** Files that caused the conflict */
  conflictingFiles: string[];
  /** Sequence number where conflict was detected */
  conflictSequenceNumber: bigint | null;
}

/**
 * Configuration for compaction coordination
 */
export interface CompactionCoordinationConfig {
  /** Default lease duration in milliseconds */
  leaseDurationMs: number;
  /** Maximum lease duration including renewals */
  maxLeaseDurationMs: number;
  /** Lock acquisition timeout in milliseconds */
  lockTimeoutMs: number;
  /** Maximum wait time for active writes to complete */
  writeWaitTimeoutMs: number;
  /** Checkpoint interval in milliseconds */
  checkpointIntervalMs: number;
  /** Whether to enable automatic checkpointing */
  enableAutoCheckpoint: boolean;
  /** Maximum retries for lock acquisition */
  maxLockRetries: number;
  /** Delay between lock retry attempts */
  lockRetryDelayMs: number;
}

/**
 * Default coordination configuration
 */
export const DEFAULT_COORDINATION_CONFIG: Readonly<CompactionCoordinationConfig> = {
  leaseDurationMs: 60_000, // 1 minute
  maxLeaseDurationMs: 300_000, // 5 minutes
  lockTimeoutMs: 10_000, // 10 seconds
  writeWaitTimeoutMs: 30_000, // 30 seconds
  checkpointIntervalMs: 5_000, // 5 seconds
  enableAutoCheckpoint: true,
  maxLockRetries: 3,
  lockRetryDelayMs: 1_000, // 1 second
};

// =============================================================================
// Compaction Coordinator
// =============================================================================

/**
 * Compaction Coordinator
 *
 * Manages coordination between compaction operations and active writes.
 * Implements lease-based locking, checkpointing, and conflict detection.
 */
export class CompactionCoordinator {
  private readonly config: CompactionCoordinationConfig;
  private readonly locks: Map<string, LockState> = new Map();
  private readonly leases: Map<string, CompactionLease> = new Map();
  private readonly checkpoints: Map<string, CompactionCheckpoint> = new Map();
  private readonly pendingWrites: Map<string, Set<string>> = new Map();
  private readonly workerId: string;

  constructor(config: Partial<CompactionCoordinationConfig> = {}) {
    this.config = { ...DEFAULT_COORDINATION_CONFIG, ...config };
    this.workerId = generateUUID();
  }

  /**
   * Get the worker ID for this coordinator
   */
  getWorkerId(): string {
    return this.workerId;
  }

  // ===========================================================================
  // Lock Management
  // ===========================================================================

  /**
   * Acquire an exclusive lock for compaction, blocking writes
   */
  async acquireExclusiveLock(
    targetKey: string,
    timeout?: number
  ): Promise<LockAcquisitionResult> {
    const effectiveTimeout = timeout ?? this.config.lockTimeoutMs;
    const lockId = generateUUID();

    // Check for existing locks
    const existing = this.locks.get(targetKey);
    if (existing) {
      // Check if existing lock has expired
      if (existing.expiresAt < Date.now()) {
        this.locks.delete(targetKey);
      } else if (existing.exclusiveLockHolder) {
        return {
          acquired: false,
          lockId: null,
          reason: 'exclusive_lock_held',
          retryAfterMs: existing.expiresAt - Date.now(),
          currentHolder: existing.exclusiveLockHolder,
        };
      } else if (existing.sharedLockHolders.size > 0) {
        // Wait for shared locks (writes) to complete
        const waitResult = await this.waitForSharedLocks(targetKey, effectiveTimeout);
        if (!waitResult.acquired) {
          return waitResult;
        }
      }
    }

    // Acquire the exclusive lock
    const now = Date.now();
    this.locks.set(targetKey, {
      exclusiveLockHolder: lockId,
      sharedLockHolders: new Set(),
      acquiredAt: now,
      expiresAt: now + this.config.leaseDurationMs,
    });

    return {
      acquired: true,
      lockId,
      reason: null,
      retryAfterMs: null,
      currentHolder: null,
    };
  }

  /**
   * Acquire a shared lock for write operations
   */
  acquireSharedLock(targetKey: string, writeId: string): LockAcquisitionResult {
    const existing = this.locks.get(targetKey);

    // Check for exclusive lock (compaction in progress)
    if (existing?.exclusiveLockHolder) {
      // Check if the exclusive lock has expired
      if (existing.expiresAt < Date.now()) {
        this.locks.delete(targetKey);
      } else {
        return {
          acquired: false,
          lockId: null,
          reason: 'compaction_in_progress',
          retryAfterMs: existing.expiresAt - Date.now(),
          currentHolder: existing.exclusiveLockHolder,
        };
      }
    }

    // Acquire or add to shared lock
    const now = Date.now();
    if (existing && !existing.exclusiveLockHolder) {
      existing.sharedLockHolders.add(writeId);
      // Extend expiration for the longest-held lock
      existing.expiresAt = Math.max(
        existing.expiresAt,
        now + this.config.leaseDurationMs
      );
    } else {
      const sharedLockHolders = new Set<string>();
      sharedLockHolders.add(writeId);
      this.locks.set(targetKey, {
        exclusiveLockHolder: null,
        sharedLockHolders,
        acquiredAt: now,
        expiresAt: now + this.config.leaseDurationMs,
      });
    }

    // Track pending writes
    let pending = this.pendingWrites.get(targetKey);
    if (!pending) {
      pending = new Set();
      this.pendingWrites.set(targetKey, pending);
    }
    pending.add(writeId);

    return {
      acquired: true,
      lockId: writeId,
      reason: null,
      retryAfterMs: null,
      currentHolder: null,
    };
  }

  /**
   * Release a lock
   */
  releaseLock(targetKey: string, lockId: string): void {
    const existing = this.locks.get(targetKey);
    if (!existing) return;

    if (existing.exclusiveLockHolder === lockId) {
      // Release exclusive lock
      this.locks.delete(targetKey);
    } else if (existing.sharedLockHolders.has(lockId)) {
      // Release shared lock
      existing.sharedLockHolders.delete(lockId);
      if (existing.sharedLockHolders.size === 0 && !existing.exclusiveLockHolder) {
        this.locks.delete(targetKey);
      }
    }

    // Clean up pending writes
    const pending = this.pendingWrites.get(targetKey);
    if (pending) {
      pending.delete(lockId);
      if (pending.size === 0) {
        this.pendingWrites.delete(targetKey);
      }
    }
  }

  /**
   * Check if a target has an exclusive lock (compaction in progress)
   */
  hasExclusiveLock(targetKey: string): boolean {
    const existing = this.locks.get(targetKey);
    if (!existing) return false;

    // Check for expiration
    if (existing.expiresAt < Date.now()) {
      this.locks.delete(targetKey);
      return false;
    }

    return existing.exclusiveLockHolder !== null;
  }

  /**
   * Check if a target has active writes
   */
  hasActiveWrites(targetKey: string): boolean {
    const existing = this.locks.get(targetKey);
    if (!existing) return false;

    // Check for expiration
    if (existing.expiresAt < Date.now()) {
      this.locks.delete(targetKey);
      return false;
    }

    return existing.sharedLockHolders.size > 0;
  }

  /**
   * Get the number of active writes for a target
   */
  getActiveWriteCount(targetKey: string): number {
    const existing = this.locks.get(targetKey);
    if (!existing || existing.expiresAt < Date.now()) {
      return 0;
    }
    return existing.sharedLockHolders.size;
  }

  /**
   * Wait for shared locks to be released
   */
  private async waitForSharedLocks(
    targetKey: string,
    timeout: number
  ): Promise<LockAcquisitionResult> {
    const startTime = Date.now();

    while (Date.now() - startTime < timeout) {
      const existing = this.locks.get(targetKey);
      if (!existing || existing.sharedLockHolders.size === 0) {
        return {
          acquired: true,
          lockId: null,
          reason: null,
          retryAfterMs: null,
          currentHolder: null,
        };
      }

      // Wait a bit before checking again
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    const existing = this.locks.get(targetKey);
    return {
      acquired: false,
      lockId: null,
      reason: 'write_wait_timeout',
      retryAfterMs: this.config.lockRetryDelayMs,
      currentHolder: existing
        ? Array.from(existing.sharedLockHolders).join(', ')
        : null,
    };
  }

  // ===========================================================================
  // Lease Management
  // ===========================================================================

  /**
   * Acquire a lease for compaction
   */
  async acquireLease(targetKey: string): Promise<CompactionLease | null> {
    // First, try to acquire the exclusive lock
    const lockResult = await this.acquireExclusiveLock(targetKey);
    if (!lockResult.acquired || !lockResult.lockId) {
      return null;
    }

    // Check for existing lease
    const existing = this.leases.get(targetKey);
    if (existing && existing.expiresAt > Date.now()) {
      // Release the lock we just acquired
      this.releaseLock(targetKey, lockResult.lockId);
      return null;
    }

    const now = Date.now();
    const lease: CompactionLease = {
      leaseId: generateUUID(),
      targetKey,
      workerId: this.workerId,
      acquiredAt: now,
      expiresAt: now + this.config.leaseDurationMs,
      renewCount: 0,
    };

    this.leases.set(targetKey, lease);
    return lease;
  }

  /**
   * Renew an existing lease
   */
  renewLease(leaseId: string): boolean {
    for (const [key, lease] of this.leases) {
      if (lease.leaseId === leaseId) {
        const now = Date.now();
        const totalDuration = now - lease.acquiredAt + this.config.leaseDurationMs;

        // Check if we've exceeded max lease duration
        if (totalDuration > this.config.maxLeaseDurationMs) {
          return false;
        }

        lease.expiresAt = now + this.config.leaseDurationMs;
        lease.renewCount++;

        // Also extend the lock
        const lock = this.locks.get(key);
        if (lock) {
          lock.expiresAt = lease.expiresAt;
        }

        return true;
      }
    }
    return false;
  }

  /**
   * Release a lease
   */
  releaseLease(leaseId: string): void {
    for (const [key, lease] of this.leases) {
      if (lease.leaseId === leaseId) {
        this.leases.delete(key);

        // Also release the exclusive lock
        const lock = this.locks.get(key);
        if (lock?.exclusiveLockHolder) {
          this.releaseLock(key, lock.exclusiveLockHolder);
        }
        break;
      }
    }
  }

  /**
   * Check if a lease is still valid
   */
  isLeaseValid(leaseId: string): boolean {
    for (const [, lease] of this.leases) {
      if (lease.leaseId === leaseId) {
        return lease.expiresAt > Date.now();
      }
    }
    return false;
  }

  /**
   * Get lease by target key
   */
  getLease(targetKey: string): CompactionLease | null {
    const lease = this.leases.get(targetKey);
    if (lease && lease.expiresAt > Date.now()) {
      return lease;
    }
    return null;
  }

  // ===========================================================================
  // Checkpoint Management
  // ===========================================================================

  /**
   * Create a new checkpoint for a compaction operation
   */
  createCheckpoint(
    targetKey: string,
    leaseId: string | null,
    files: string[],
    sequenceNumber: bigint
  ): CompactionCheckpoint {
    const now = Date.now();
    const checkpoint: CompactionCheckpoint = {
      checkpointId: generateUUID(),
      targetKey,
      phase: 'initializing',
      processedFiles: [],
      remainingFiles: [...files],
      outputFilePath: null,
      createdAt: now,
      updatedAt: now,
      leaseId,
      startSequenceNumber: sequenceNumber,
      intermediateData: {},
    };

    this.checkpoints.set(checkpoint.checkpointId, checkpoint);
    return checkpoint;
  }

  /**
   * Update checkpoint progress
   */
  updateCheckpoint(
    checkpointId: string,
    updates: Partial<Pick<CompactionCheckpoint,
      'phase' | 'processedFiles' | 'remainingFiles' | 'outputFilePath' | 'intermediateData'
    >>
  ): boolean {
    const checkpoint = this.checkpoints.get(checkpointId);
    if (!checkpoint) return false;

    checkpoint.updatedAt = Date.now();

    if (updates.phase !== undefined) {
      checkpoint.phase = updates.phase;
    }
    if (updates.processedFiles !== undefined) {
      checkpoint.processedFiles = updates.processedFiles;
    }
    if (updates.remainingFiles !== undefined) {
      checkpoint.remainingFiles = updates.remainingFiles;
    }
    if (updates.outputFilePath !== undefined) {
      checkpoint.outputFilePath = updates.outputFilePath;
    }
    if (updates.intermediateData !== undefined) {
      checkpoint.intermediateData = {
        ...checkpoint.intermediateData,
        ...updates.intermediateData,
      };
    }

    return true;
  }

  /**
   * Get a checkpoint by ID
   */
  getCheckpoint(checkpointId: string): CompactionCheckpoint | null {
    return this.checkpoints.get(checkpointId) ?? null;
  }

  /**
   * Get checkpoint by target key (for recovery)
   */
  getCheckpointByTarget(targetKey: string): CompactionCheckpoint | null {
    for (const [, checkpoint] of this.checkpoints) {
      if (
        checkpoint.targetKey === targetKey &&
        checkpoint.phase !== 'completed' &&
        checkpoint.phase !== 'failed'
      ) {
        return checkpoint;
      }
    }
    return null;
  }

  /**
   * Mark a checkpoint as completed and clean it up
   */
  completeCheckpoint(checkpointId: string): void {
    const checkpoint = this.checkpoints.get(checkpointId);
    if (checkpoint) {
      checkpoint.phase = 'completed';
      checkpoint.updatedAt = Date.now();
      // Clean up after a short delay
      setTimeout(() => {
        this.checkpoints.delete(checkpointId);
      }, 5000);
    }
  }

  /**
   * Mark a checkpoint as failed
   */
  failCheckpoint(checkpointId: string, reason?: string): void {
    const checkpoint = this.checkpoints.get(checkpointId);
    if (checkpoint) {
      checkpoint.phase = 'failed';
      checkpoint.updatedAt = Date.now();
      if (reason) {
        checkpoint.intermediateData.failureReason = reason;
      }
    }
  }

  /**
   * Get all active (non-completed, non-failed) checkpoints
   */
  getActiveCheckpoints(): CompactionCheckpoint[] {
    const active: CompactionCheckpoint[] = [];
    for (const [, checkpoint] of this.checkpoints) {
      if (checkpoint.phase !== 'completed' && checkpoint.phase !== 'failed') {
        active.push(checkpoint);
      }
    }
    return active;
  }

  // ===========================================================================
  // Conflict Detection
  // ===========================================================================

  /**
   * Check for conflicts during compaction
   */
  checkForConflicts(
    targetKey: string,
    startSequenceNumber: bigint,
    currentSequenceNumber: bigint,
    originalFiles: string[],
    currentFiles: string[]
  ): ConflictCheckResult {
    // Check for new data (sequence number advanced)
    if (currentSequenceNumber > startSequenceNumber) {
      const newFiles = currentFiles.filter(f => !originalFiles.includes(f));
      if (newFiles.length > 0) {
        return {
          hasConflict: true,
          conflictType: 'new_data',
          details: `New files added during compaction: ${newFiles.length} files`,
          conflictingFiles: newFiles,
          conflictSequenceNumber: currentSequenceNumber,
        };
      }
    }

    // Check for concurrent compaction (files removed)
    const removedFiles = originalFiles.filter(f => !currentFiles.includes(f));
    if (removedFiles.length > 0) {
      return {
        hasConflict: true,
        conflictType: 'concurrent_compaction',
        details: `Files removed by concurrent operation: ${removedFiles.length} files`,
        conflictingFiles: removedFiles,
        conflictSequenceNumber: currentSequenceNumber,
      };
    }

    return {
      hasConflict: false,
      conflictType: null,
      details: null,
      conflictingFiles: [],
      conflictSequenceNumber: null,
    };
  }

  /**
   * Resolve a conflict by determining the appropriate action
   */
  resolveConflict(conflict: ConflictCheckResult): 'retry' | 'abort' | 'merge' {
    if (!conflict.hasConflict) {
      return 'merge'; // No conflict, proceed with merge
    }

    switch (conflict.conflictType) {
      case 'new_data':
        // New data arrived - can potentially include in merge
        if (conflict.conflictingFiles.length <= 10) {
          return 'merge'; // Small amount of new data, include it
        }
        return 'retry'; // Too much new data, start over

      case 'concurrent_compaction':
        // Another compaction modified our files - abort
        return 'abort';

      case 'schema_change':
        // Schema changed - abort to avoid data corruption
        return 'abort';

      default:
        return 'abort';
    }
  }

  // ===========================================================================
  // Cleanup
  // ===========================================================================

  /**
   * Clean up expired locks and leases
   */
  cleanupExpired(): void {
    const now = Date.now();

    // Clean up expired locks
    for (const [key, lock] of this.locks) {
      if (lock.expiresAt < now) {
        this.locks.delete(key);
      }
    }

    // Clean up expired leases
    for (const [key, lease] of this.leases) {
      if (lease.expiresAt < now) {
        this.leases.delete(key);
      }
    }
  }

  /**
   * Get current coordination statistics
   */
  getStats(): {
    activeLocks: number;
    exclusiveLocks: number;
    sharedLocks: number;
    activeLeases: number;
    activeCheckpoints: number;
  } {
    let exclusiveLocks = 0;
    let sharedLocks = 0;
    const now = Date.now();

    for (const [, lock] of this.locks) {
      if (lock.expiresAt > now) {
        if (lock.exclusiveLockHolder) {
          exclusiveLocks++;
        }
        sharedLocks += lock.sharedLockHolders.size;
      }
    }

    let activeLeases = 0;
    for (const [, lease] of this.leases) {
      if (lease.expiresAt > now) {
        activeLeases++;
      }
    }

    return {
      activeLocks: exclusiveLocks + (sharedLocks > 0 ? 1 : 0),
      exclusiveLocks,
      sharedLocks,
      activeLeases,
      activeCheckpoints: this.getActiveCheckpoints().length,
    };
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
 *
 * Now includes write coordination to prevent compaction during active writes
 * and vice versa (Issue: sql-8euu).
 */
export class CompactionManager {
  private readonly config: CompactionConfig;
  private readonly coordinator: CompactionCoordinator;
  private readonly coordinationConfig: CompactionCoordinationConfig;
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

  constructor(
    config: Partial<CompactionConfig> = {},
    coordinationConfig: Partial<CompactionCoordinationConfig> = {}
  ) {
    this.config = { ...DEFAULT_COMPACTION_CONFIG, ...config };
    this.coordinationConfig = { ...DEFAULT_COORDINATION_CONFIG, ...coordinationConfig };
    this.coordinator = new CompactionCoordinator(this.coordinationConfig);
    this.validateConfig();
  }

  /**
   * Get the coordinator for advanced usage
   */
  getCoordinator(): CompactionCoordinator {
    return this.coordinator;
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

  // ===========================================================================
  // Write Coordination
  // ===========================================================================

  /**
   * Start a write operation with coordination
   *
   * Call this before writing data to a table/partition to prevent
   * compaction from starting during the write.
   */
  startWrite(targetKey: string): LockAcquisitionResult {
    const writeId = generateUUID();
    return this.coordinator.acquireSharedLock(targetKey, writeId);
  }

  /**
   * Complete a write operation
   *
   * Call this after a write is completed to release the write lock.
   */
  completeWrite(targetKey: string, writeId: string): void {
    this.coordinator.releaseLock(targetKey, writeId);
  }

  /**
   * Check if writes are blocked for a target (compaction in progress)
   */
  isWriteBlocked(targetKey: string): boolean {
    return this.coordinator.hasExclusiveLock(targetKey);
  }

  /**
   * Check if compaction is blocked for a target (active writes)
   */
  isCompactionBlocked(targetKey: string): boolean {
    return this.coordinator.hasActiveWrites(targetKey);
  }

  /**
   * Get the number of active writes for a target
   */
  getActiveWriteCount(targetKey: string): number {
    return this.coordinator.getActiveWriteCount(targetKey);
  }

  // ===========================================================================
  // Coordinated Compaction
  // ===========================================================================

  /**
   * Start a coordinated compaction operation
   *
   * Acquires a lease and blocks writes during compaction.
   */
  async startCompaction(
    targetKey: string,
    files: DataFile[],
    sequenceNumber: bigint
  ): Promise<CompactionSession | null> {
    // Try to acquire a lease
    const lease = await this.coordinator.acquireLease(targetKey);
    if (!lease) {
      return null;
    }

    // Create a checkpoint
    const checkpoint = this.coordinator.createCheckpoint(
      targetKey,
      lease.leaseId,
      files.map(f => f['file-path']),
      sequenceNumber
    );

    return {
      lease,
      checkpoint,
      manager: this,
      coordinator: this.coordinator,
    };
  }

  /**
   * Resume an interrupted compaction from checkpoint
   */
  async resumeCompaction(targetKey: string): Promise<CompactionSession | null> {
    const checkpoint = this.coordinator.getCheckpointByTarget(targetKey);
    if (!checkpoint) {
      return null;
    }

    // Try to acquire a new lease
    const lease = await this.coordinator.acquireLease(targetKey);
    if (!lease) {
      return null;
    }

    // Update checkpoint with new lease
    checkpoint.leaseId = lease.leaseId;
    checkpoint.updatedAt = Date.now();

    return {
      lease,
      checkpoint,
      manager: this,
      coordinator: this.coordinator,
    };
  }

  /**
   * Check for conflicts during compaction
   */
  checkCompactionConflicts(
    session: CompactionSession,
    currentSequenceNumber: bigint,
    currentFiles: string[]
  ): ConflictCheckResult {
    return this.coordinator.checkForConflicts(
      session.checkpoint.targetKey,
      session.checkpoint.startSequenceNumber,
      currentSequenceNumber,
      session.checkpoint.remainingFiles,
      currentFiles
    );
  }

  /**
   * Complete a coordinated compaction operation
   */
  completeCompaction(session: CompactionSession, result: CompactionResult): void {
    // Record the result
    this.recordCompactionResult(result);

    // Complete the checkpoint
    if (result.success) {
      this.coordinator.completeCheckpoint(session.checkpoint.checkpointId);
    } else {
      this.coordinator.failCheckpoint(
        session.checkpoint.checkpointId,
        result.error ?? 'Unknown error'
      );
    }

    // Release the lease
    this.coordinator.releaseLease(session.lease.leaseId);
  }

  /**
   * Abort a coordinated compaction operation
   */
  abortCompaction(session: CompactionSession, reason: string): void {
    this.coordinator.failCheckpoint(session.checkpoint.checkpointId, reason);
    this.coordinator.releaseLease(session.lease.leaseId);

    this.recordCompactionResult({
      success: false,
      filesCompacted: 0,
      bytesCompacted: BigInt(0),
      outputFiles: 0,
      outputBytes: BigInt(0),
      durationMs: Date.now() - session.checkpoint.createdAt,
      error: reason,
    });
  }

  /**
   * Renew the lease for an active compaction session
   */
  renewCompactionLease(session: CompactionSession): boolean {
    return this.coordinator.renewLease(session.lease.leaseId);
  }

  /**
   * Update checkpoint progress during compaction
   */
  updateCompactionProgress(
    session: CompactionSession,
    phase: CompactionPhase,
    processedFiles: string[]
  ): void {
    const remaining = session.checkpoint.remainingFiles.filter(
      f => !processedFiles.includes(f)
    );

    this.coordinator.updateCheckpoint(session.checkpoint.checkpointId, {
      phase,
      processedFiles,
      remainingFiles: remaining,
    });

    session.checkpoint.phase = phase;
    session.checkpoint.processedFiles = processedFiles;
    session.checkpoint.remainingFiles = remaining;
  }

  /**
   * Get coordination statistics
   */
  getCoordinationStats(): ReturnType<CompactionCoordinator['getStats']> {
    return this.coordinator.getStats();
  }

  /**
   * Clean up expired locks and leases
   */
  cleanupExpiredCoordination(): void {
    this.coordinator.cleanupExpired();
  }
}

// =============================================================================
// Compaction Session
// =============================================================================

/**
 * Represents an active compaction session with coordination
 */
export interface CompactionSession {
  /** The lease for this compaction */
  lease: CompactionLease;
  /** Checkpoint for resumable compaction */
  checkpoint: CompactionCheckpoint;
  /** Reference to the manager */
  manager: CompactionManager;
  /** Reference to the coordinator */
  coordinator: CompactionCoordinator;
}
