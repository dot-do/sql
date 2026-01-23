/**
 * WAL (Write-Ahead Log) Types for DoSQL
 *
 * Core types for the write-ahead log system that provides durability
 * and enables CDC (Change Data Capture) streaming.
 */

import type { FSXBackend } from '../fsx/types.js';
import type { LSN, TransactionId } from '../engine/types.js';
import type { HLCTimestamp } from '../hlc.js';
import {
  DoSQLError,
  ErrorCategory,
  registerErrorClass,
  type ErrorContext,
  type SerializedError,
} from '../errors/base.js';

// Re-export branded types for convenience
export type { LSN, TransactionId } from '../engine/types.js';
export { createLSN, createTransactionId } from '../engine/types.js';

// Re-export HLC types for convenience
export type { HLCTimestamp } from '../hlc.js';

// =============================================================================
// Core WAL Entry Types
// =============================================================================

/**
 * Operation types supported by the WAL
 */
export type WALOperation =
  | 'INSERT'
  | 'UPDATE'
  | 'DELETE'
  | 'BEGIN'
  | 'COMMIT'
  | 'ROLLBACK';

/**
 * A single entry in the write-ahead log
 */
export interface WALEntry {
  /** Log Sequence Number - monotonically increasing identifier (branded type) */
  lsn: LSN;
  /** Timestamp when the entry was created (Unix ms) */
  timestamp: number;
  /** Transaction ID for grouping related operations (branded type) */
  txnId: TransactionId;
  /** The operation type */
  op: WALOperation;
  /** Target table name (empty for transaction control ops) */
  table: string;
  /** Primary key for UPDATE/DELETE operations */
  key?: Uint8Array;
  /** Previous value for UPDATE/DELETE operations (enables rollback) */
  before?: Uint8Array;
  /** New value for INSERT/UPDATE operations */
  after?: Uint8Array;
  /** HLC timestamp for causal ordering in CDC (optional for backward compatibility) */
  hlc?: HLCTimestamp;
}

/**
 * A segment of WAL entries
 * Segments are the unit of storage - flushed to fsx when full
 */
export interface WALSegment {
  /** Unique segment identifier (typically based on startLSN) */
  id: string;
  /** First LSN in this segment (branded type) */
  startLSN: LSN;
  /** Last LSN in this segment (branded type) */
  endLSN: LSN;
  /** Entries contained in this segment */
  entries: WALEntry[];
  /** CRC32 checksum for integrity verification */
  checksum: number;
  /** Segment creation timestamp */
  createdAt: number;
  /** Whether this segment has been archived/compacted */
  archived?: boolean;
}

// =============================================================================
// WAL Configuration
// =============================================================================

/**
 * Configuration options for WAL behavior
 */
export interface WALConfig {
  /** Target segment size in bytes (default: 2MB aligned with DO storage) */
  targetSegmentSize: number;
  /** Maximum entries per segment before forced flush */
  maxEntriesPerSegment: number;
  /** Path prefix for WAL segments in fsx */
  segmentPrefix: string;
  /** Path for checkpoint metadata */
  checkpointPath: string;
  /** Whether to verify checksums on read (default: true) */
  verifyChecksums: boolean;
  /** Auto-archive segments after checkpoint (default: true) */
  autoArchive: boolean;
  /** Archive path prefix (for cold storage) */
  archivePrefix: string;
}

/**
 * Default WAL configuration
 */
export const DEFAULT_WAL_CONFIG: WALConfig = {
  targetSegmentSize: 2 * 1024 * 1024, // 2MB - matches DO storage chunk size
  maxEntriesPerSegment: 10000,
  segmentPrefix: '_wal/segments/',
  checkpointPath: '_wal/checkpoint.json',
  verifyChecksums: true,
  autoArchive: true,
  archivePrefix: '_wal/archive/',
};

// =============================================================================
// Checkpoint Types
// =============================================================================

/**
 * Checkpoint metadata - persisted to track recovery point
 */
export interface Checkpoint {
  /** Last LSN that has been fully applied/checkpointed (branded type) */
  lsn: LSN;
  /** Timestamp of the checkpoint */
  timestamp: number;
  /** Segment ID containing the checkpoint LSN */
  segmentId: string;
  /** Transaction IDs that were in-progress at checkpoint time (branded type) */
  activeTransactions: TransactionId[];
  /** Schema version at checkpoint time */
  schemaVersion?: number;
}

/**
 * Recovery state after replaying WAL from checkpoint
 */
export interface RecoveryState {
  /** Last replayed LSN (branded type) */
  lastLSN: LSN;
  /** Number of entries replayed */
  entriesReplayed: number;
  /** Transactions that were rolled back during recovery (branded type) */
  rolledBackTransactions: TransactionId[];
  /** Recovery start time */
  startedAt: number;
  /** Recovery end time */
  completedAt?: number;
  /** Any errors encountered during recovery (branded type for LSN) */
  errors: Array<{ lsn: LSN; error: string }>;
}

// =============================================================================
// WAL Writer Types
// =============================================================================

/**
 * Options for appending an entry to the WAL
 */
export interface AppendOptions {
  /** Force immediate flush to storage (default: false) */
  sync?: boolean;
  /** Skip checksum calculation (for performance, use with caution) */
  skipChecksum?: boolean;
}

/**
 * Result of an append operation
 */
export interface AppendResult {
  /** Assigned LSN for the entry (branded type) */
  lsn: LSN;
  /** Whether segment was flushed as part of this append */
  flushed: boolean;
  /** Segment ID if flushed */
  segmentId?: string;
}

/**
 * WAL Writer interface
 */
export interface WALWriter {
  /**
   * Append an entry to the WAL
   * @param entry Partial entry (LSN will be assigned)
   * @param options Append options
   * @returns Assigned LSN and flush status
   */
  append(
    entry: Omit<WALEntry, 'lsn'>,
    options?: AppendOptions
  ): Promise<AppendResult>;

  /**
   * Force flush of current segment to storage
   * @returns Flushed segment or null if nothing to flush
   */
  flush(): Promise<WALSegment | null>;

  /**
   * Get the current (next to be assigned) LSN (branded type)
   */
  getCurrentLSN(): LSN;

  /**
   * Get pending entries not yet flushed
   */
  getPendingCount(): number;

  /**
   * Get the current segment size in bytes (estimated)
   */
  getCurrentSegmentSize(): number;

  /**
   * Close the writer, flushing any pending entries
   */
  close(): Promise<void>;
}

// =============================================================================
// WAL Reader Types
// =============================================================================

/**
 * Options for reading WAL entries
 */
export interface ReadOptions {
  /** Start LSN (inclusive, branded type) */
  fromLSN?: LSN;
  /** End LSN (inclusive, branded type) */
  toLSN?: LSN;
  /** Filter by table name */
  table?: string;
  /** Filter by operation types */
  operations?: WALOperation[];
  /** Filter by transaction ID (branded type) */
  txnId?: TransactionId;
  /** Maximum entries to return */
  limit?: number;
}

/**
 * WAL Reader interface
 */
export interface WALReader {
  /**
   * Read entries from a specific segment
   * @param segmentId Segment ID to read
   * @returns Segment with all entries
   */
  readSegment(segmentId: string): Promise<WALSegment | null>;

  /**
   * Read entries matching the given options
   * @param options Read options for filtering
   * @returns Matching entries in LSN order
   */
  readEntries(options: ReadOptions): Promise<WALEntry[]>;

  /**
   * List all available segment IDs
   * @param includeArchived Include archived segments (default: false)
   * @returns Segment IDs in LSN order
   */
  listSegments(includeArchived?: boolean): Promise<string[]>;

  /**
   * Get entry by exact LSN (branded type)
   * @param lsn The LSN to find
   * @returns Entry or null if not found
   */
  getEntry(lsn: LSN): Promise<WALEntry | null>;

  /**
   * Create an async iterator over entries
   * @param options Read options
   * @returns Async iterator
   */
  iterate(options: ReadOptions): AsyncIterableIterator<WALEntry>;
}

// =============================================================================
// Checkpointing Types
// =============================================================================

/**
 * Checkpoint Manager interface
 */
export interface CheckpointManager {
  /**
   * Get the current checkpoint
   * @returns Current checkpoint or null if none exists
   */
  getCheckpoint(): Promise<Checkpoint | null>;

  /**
   * Create a new checkpoint at the given LSN (branded type)
   * @param lsn LSN to checkpoint at
   * @param activeTransactions Currently active transaction IDs (branded type)
   * @returns The created checkpoint
   */
  createCheckpoint(
    lsn: LSN,
    activeTransactions: TransactionId[]
  ): Promise<Checkpoint>;

  /**
   * Archive segments before the checkpoint LSN
   * Moves segments to archive prefix and optionally deletes old archives
   * @param retainCount Number of archived segments to retain (default: 10)
   * @returns Number of segments archived
   */
  archiveOldSegments(retainCount?: number): Promise<number>;

  /**
   * Recover from the last checkpoint
   * Replays WAL entries and returns recovery state
   * @param applyFn Function to apply each entry during recovery
   * @returns Recovery state
   */
  recover(
    applyFn: (entry: WALEntry) => Promise<void>
  ): Promise<RecoveryState>;
}

// =============================================================================
// Serialization Utilities
// =============================================================================

/**
 * Encoder for WAL entries and segments
 */
export interface WALEncoder {
  encodeEntry(entry: WALEntry): Uint8Array;
  decodeEntry(data: Uint8Array): WALEntry;
  encodeSegment(segment: WALSegment): Uint8Array;
  decodeSegment(data: Uint8Array): WALSegment;
  calculateChecksum(data: Uint8Array): number;
}

// =============================================================================
// Storage Interface
// =============================================================================

/**
 * WAL Storage interface - wrapper around FSX with WAL-specific operations
 */
export interface WALStorage {
  /** Underlying FSX backend */
  readonly backend: FSXBackend;

  /** Write a segment to storage */
  writeSegment(segment: WALSegment): Promise<void>;

  /** Read a segment from storage */
  readSegment(segmentId: string): Promise<WALSegment | null>;

  /** List segment IDs */
  listSegments(prefix: string): Promise<string[]>;

  /** Delete a segment */
  deleteSegment(segmentId: string): Promise<void>;

  /** Move segment to archive */
  archiveSegment(segmentId: string): Promise<void>;

  /** Write checkpoint */
  writeCheckpoint(checkpoint: Checkpoint): Promise<void>;

  /** Read checkpoint */
  readCheckpoint(): Promise<Checkpoint | null>;
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * WAL-specific error codes
 */
export enum WALErrorCode {
  /** Checksum verification failed */
  CHECKSUM_MISMATCH = 'WAL_CHECKSUM_MISMATCH',
  /** Segment not found */
  SEGMENT_NOT_FOUND = 'WAL_SEGMENT_NOT_FOUND',
  /** Entry not found */
  ENTRY_NOT_FOUND = 'WAL_ENTRY_NOT_FOUND',
  /** Invalid LSN (e.g., out of order) */
  INVALID_LSN = 'WAL_INVALID_LSN',
  /** Segment corrupted */
  SEGMENT_CORRUPTED = 'WAL_SEGMENT_CORRUPTED',
  /** Recovery failed */
  RECOVERY_FAILED = 'WAL_RECOVERY_FAILED',
  /** Checkpoint failed */
  CHECKPOINT_FAILED = 'WAL_CHECKPOINT_FAILED',
  /** Flush failed */
  FLUSH_FAILED = 'WAL_FLUSH_FAILED',
  /** Serialization error */
  SERIALIZATION_ERROR = 'WAL_SERIALIZATION_ERROR',
}

/**
 * Error class for WAL (Write-Ahead Log) operations
 *
 * Extends DoSQLError to provide consistent error handling with:
 * - Machine-readable error codes
 * - Error categories for API layer handling
 * - Recovery hints for developers
 * - LSN and segment context for debugging
 *
 * @example
 * ```typescript
 * try {
 *   await walWriter.append(entry);
 * } catch (error) {
 *   if (error instanceof WALError) {
 *     console.log(error.code);      // 'WAL_FLUSH_FAILED'
 *     console.log(error.lsn);       // LSN at failure
 *     console.log(error.segmentId); // Affected segment
 *     if (error.isRetryable()) {
 *       // Retry the operation
 *     }
 *   }
 * }
 * ```
 */
export class WALError extends DoSQLError {
  readonly code: WALErrorCode;
  readonly category: ErrorCategory;
  readonly lsn?: LSN;
  readonly segmentId?: string;

  constructor(
    code: WALErrorCode,
    message: string,
    options?: {
      cause?: Error;
      context?: ErrorContext;
      lsn?: LSN;
      segmentId?: string;
    }
  ) {
    super(message, { cause: options?.cause, context: options?.context });
    this.name = 'WALError';
    this.code = code;
    this.lsn = options?.lsn;
    this.segmentId = options?.segmentId;

    // Set category based on error code
    this.category = this.determineCategory();

    // Include LSN and segment in context
    if (this.lsn !== undefined || this.segmentId) {
      this.context = {
        ...this.context,
        metadata: {
          ...this.context?.metadata,
          ...(this.lsn !== undefined && { lsn: String(this.lsn) }),
          ...(this.segmentId && { segmentId: this.segmentId }),
        },
      };
    }

    // Set recovery hints
    this.setRecoveryHint();
  }

  private determineCategory(): ErrorCategory {
    switch (this.code) {
      case WALErrorCode.SEGMENT_NOT_FOUND:
      case WALErrorCode.ENTRY_NOT_FOUND:
        return ErrorCategory.RESOURCE;
      case WALErrorCode.CHECKSUM_MISMATCH:
      case WALErrorCode.SEGMENT_CORRUPTED:
      case WALErrorCode.SERIALIZATION_ERROR:
        return ErrorCategory.INTERNAL;
      case WALErrorCode.INVALID_LSN:
        return ErrorCategory.VALIDATION;
      case WALErrorCode.FLUSH_FAILED:
      case WALErrorCode.CHECKPOINT_FAILED:
      case WALErrorCode.RECOVERY_FAILED:
        return ErrorCategory.EXECUTION;
      default:
        return ErrorCategory.EXECUTION;
    }
  }

  private setRecoveryHint(): void {
    switch (this.code) {
      case WALErrorCode.CHECKSUM_MISMATCH:
        this.recoveryHint = 'Data integrity error - segment may be corrupted. Consider recovery from backup.';
        break;
      case WALErrorCode.SEGMENT_NOT_FOUND:
        this.recoveryHint = 'WAL segment was deleted or compacted. Start from a more recent checkpoint.';
        break;
      case WALErrorCode.ENTRY_NOT_FOUND:
        this.recoveryHint = 'LSN may be too old. Check WAL retention settings.';
        break;
      case WALErrorCode.INVALID_LSN:
        this.recoveryHint = 'Ensure LSN values are monotonically increasing and properly ordered.';
        break;
      case WALErrorCode.SEGMENT_CORRUPTED:
        this.recoveryHint = 'Segment is corrupted. Recovery from backup may be required.';
        break;
      case WALErrorCode.RECOVERY_FAILED:
        this.recoveryHint = 'WAL recovery failed. Check disk space, permissions, and segment integrity.';
        break;
      case WALErrorCode.CHECKPOINT_FAILED:
        this.recoveryHint = 'Checkpoint failed. Verify disk space and storage availability.';
        break;
      case WALErrorCode.FLUSH_FAILED:
        this.recoveryHint = 'Flush failed. Check disk I/O and storage health. Retry the operation.';
        break;
      case WALErrorCode.SERIALIZATION_ERROR:
        this.recoveryHint = 'Serialization error. Check WAL entry format and data types.';
        break;
    }
  }

  /**
   * Check if this error is retryable
   */
  isRetryable(): boolean {
    return [
      WALErrorCode.FLUSH_FAILED,
      WALErrorCode.CHECKPOINT_FAILED,
    ].includes(this.code);
  }

  /**
   * Get a user-friendly error message
   */
  toUserMessage(): string {
    switch (this.code) {
      case WALErrorCode.CHECKSUM_MISMATCH:
        return 'Data integrity check failed. The write-ahead log may be corrupted.';
      case WALErrorCode.SEGMENT_NOT_FOUND:
        return 'The requested log segment is no longer available.';
      case WALErrorCode.ENTRY_NOT_FOUND:
        return 'The requested log entry could not be found.';
      case WALErrorCode.INVALID_LSN:
        return 'Invalid log sequence number provided.';
      case WALErrorCode.SEGMENT_CORRUPTED:
        return 'Log segment is corrupted and cannot be read.';
      case WALErrorCode.RECOVERY_FAILED:
        return 'Failed to recover from the write-ahead log.';
      case WALErrorCode.CHECKPOINT_FAILED:
        return 'Failed to create a checkpoint. Please try again.';
      case WALErrorCode.FLUSH_FAILED:
        return 'Failed to flush log entries. Please try again.';
      case WALErrorCode.SERIALIZATION_ERROR:
        return 'Failed to serialize log entry data.';
      default:
        return this.message;
    }
  }

  /**
   * Deserialize from JSON
   */
  static fromJSON(json: SerializedError): WALError {
    return new WALError(
      json.code as WALErrorCode,
      json.message,
      {
        context: json.context,
        lsn: json.context?.metadata?.lsn as LSN | undefined,
        segmentId: json.context?.metadata?.segmentId as string | undefined,
      }
    );
  }
}

// Register for deserialization
registerErrorClass('WALError', WALError);
