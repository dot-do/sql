/**
 * Backup and Restore Types for DoSQL
 *
 * Types for database backup and point-in-time recovery (PITR) functionality.
 * Supports full and incremental backups to R2 storage.
 */

import type { LSN, TransactionId } from '../engine/types.js';
import type { FSXBackend } from '../fsx/types.js';
import {
  DoSQLError,
  ErrorCategory,
  registerErrorClass,
  type ErrorContext,
  type SerializedError,
} from '../errors/base.js';
import { createLSN } from '../engine/types.js';

// =============================================================================
// Backup Types
// =============================================================================

/**
 * Backup type: full snapshot or incremental based on WAL
 */
export type BackupType = 'full' | 'incremental';

/**
 * Backup compression type
 */
export type CompressionType = 'none' | 'gzip';

/**
 * Backup status
 */
export type BackupStatus = 'pending' | 'in_progress' | 'completed' | 'failed' | 'corrupted';

/**
 * Backup metadata stored with each backup
 */
export interface BackupMetadata {
  /** Unique backup identifier */
  id: string;
  /** Backup type (full or incremental) */
  type: BackupType;
  /** Backup status */
  status: BackupStatus;
  /** Database identifier */
  databaseId: string;
  /** Timestamp when backup started */
  startedAt: number;
  /** Timestamp when backup completed */
  completedAt?: number;
  /** LSN at backup start (for consistent snapshot) */
  startLSN: LSN;
  /** LSN at backup completion */
  endLSN: LSN;
  /** For incremental: base backup ID this is built on */
  baseBackupId?: string;
  /** CRC32 checksum of the backup data */
  checksum: number;
  /** Total size of backup in bytes */
  sizeBytes: number;
  /** Number of files/pages backed up */
  fileCount: number;
  /** Compression type used */
  compression: CompressionType;
  /** Schema version at backup time */
  schemaVersion?: number;
  /** Optional description/comment */
  description?: string;
  /** Custom metadata */
  custom?: Record<string, string>;
}

/**
 * Backup file entry - individual file within a backup
 */
export interface BackupFileEntry {
  /** Relative path within the backup */
  path: string;
  /** File size in bytes */
  size: number;
  /** CRC32 checksum of the file */
  checksum: number;
  /** Whether file was modified since base (for incremental) */
  modified?: boolean;
}

/**
 * Backup manifest - describes contents of a backup
 */
export interface BackupManifest {
  /** Backup metadata */
  metadata: BackupMetadata;
  /** Files included in this backup */
  files: BackupFileEntry[];
  /** WAL segments included (for incremental or PITR) */
  walSegments?: string[];
}

/**
 * Configuration for backup operations
 */
export interface BackupConfig {
  /** Prefix for backup storage paths in R2 */
  backupPrefix: string;
  /** Compression type to use */
  compression: CompressionType;
  /** Whether to include WAL segments for PITR */
  includeWAL: boolean;
  /** Maximum WAL segments to include */
  maxWALSegments: number;
  /** Path prefix for WAL segments */
  walPrefix: string;
  /** Path prefix for data files */
  dataPrefix: string;
  /** Whether to verify checksums after backup */
  verifyAfterBackup: boolean;
}

/**
 * Default backup configuration
 */
export const DEFAULT_BACKUP_CONFIG: Readonly<BackupConfig> = {
  backupPrefix: '_backups/',
  compression: 'none',
  includeWAL: true,
  maxWALSegments: 100,
  walPrefix: '_wal/segments/',
  dataPrefix: '_data/',
  verifyAfterBackup: true,
};

/**
 * Options for creating a backup
 */
export interface BackupOptions {
  /** Backup type */
  type?: BackupType;
  /** Base backup ID for incremental backup */
  baseBackupId?: string;
  /** Description/comment for the backup */
  description?: string;
  /** Custom metadata to include */
  customMetadata?: Record<string, string>;
  /** Compression type override */
  compression?: CompressionType;
  /** Include WAL override */
  includeWAL?: boolean;
  /** Callback for progress updates */
  onProgress?: (progress: BackupProgress) => void;
}

/**
 * Progress information during backup
 */
export interface BackupProgress {
  /** Current phase */
  phase: 'preparing' | 'snapshotting' | 'copying_data' | 'copying_wal' | 'finalizing' | 'verifying';
  /** Files processed so far */
  filesProcessed: number;
  /** Total files to process */
  totalFiles: number;
  /** Bytes written so far */
  bytesWritten: number;
  /** Total bytes to write */
  totalBytes: number;
  /** Current file being processed */
  currentFile?: string;
}

/**
 * Result of a backup operation
 */
export interface BackupResult {
  /** Whether backup succeeded */
  success: boolean;
  /** Backup ID */
  backupId: string;
  /** Backup metadata */
  metadata: BackupMetadata;
  /** Duration in milliseconds */
  durationMs: number;
  /** Any warnings during backup */
  warnings: string[];
  /** Error message if failed */
  error?: string;
}

// =============================================================================
// Restore Types
// =============================================================================

/**
 * Options for restore operations
 */
export interface RestoreOptions {
  /** Backup ID to restore from */
  backupId: string;
  /** Target LSN for point-in-time recovery */
  targetLSN?: LSN;
  /** Target timestamp for point-in-time recovery */
  targetTimestamp?: number;
  /** Whether to verify backup integrity before restore */
  verifyBeforeRestore?: boolean;
  /** Whether to skip WAL replay */
  skipWALReplay?: boolean;
  /** Callback for progress updates */
  onProgress?: (progress: RestoreProgress) => void;
}

/**
 * Progress information during restore
 */
export interface RestoreProgress {
  /** Current phase */
  phase: 'verifying' | 'restoring_data' | 'replaying_wal' | 'finalizing';
  /** Files processed so far */
  filesProcessed: number;
  /** Total files to process */
  totalFiles: number;
  /** Bytes restored so far */
  bytesRestored: number;
  /** Total bytes to restore */
  totalBytes: number;
  /** WAL entries replayed (for PITR) */
  walEntriesReplayed?: number;
  /** Current file being processed */
  currentFile?: string;
}

/**
 * Result of a restore operation
 */
export interface RestoreResult {
  /** Whether restore succeeded */
  success: boolean;
  /** Backup ID that was restored */
  backupId: string;
  /** Final LSN after restore */
  restoredLSN: LSN;
  /** Number of files restored */
  filesRestored: number;
  /** Number of WAL entries replayed */
  walEntriesReplayed: number;
  /** Duration in milliseconds */
  durationMs: number;
  /** Any warnings during restore */
  warnings: string[];
  /** Error message if failed */
  error?: string;
}

/**
 * Validation result for backup integrity check
 */
export interface BackupValidation {
  /** Whether backup is valid */
  valid: boolean;
  /** Checksum verified */
  checksumValid: boolean;
  /** Manifest verified */
  manifestValid: boolean;
  /** All files present */
  filesComplete: boolean;
  /** Missing files if any */
  missingFiles: string[];
  /** Corrupted files if any */
  corruptedFiles: string[];
  /** Validation errors */
  errors: string[];
}

// =============================================================================
// Backup Manager Interface
// =============================================================================

/**
 * Backup manager interface
 */
export interface BackupManager {
  /**
   * Create a backup of the database
   * @param options Backup options
   * @returns Backup result
   */
  createBackup(options?: BackupOptions): Promise<BackupResult>;

  /**
   * List available backups
   * @param limit Maximum number of backups to return
   * @returns Array of backup metadata
   */
  listBackups(limit?: number): Promise<BackupMetadata[]>;

  /**
   * Get backup details
   * @param backupId Backup ID
   * @returns Backup manifest or null if not found
   */
  getBackup(backupId: string): Promise<BackupManifest | null>;

  /**
   * Delete a backup
   * @param backupId Backup ID
   * @returns Whether deletion was successful
   */
  deleteBackup(backupId: string): Promise<boolean>;

  /**
   * Validate backup integrity
   * @param backupId Backup ID
   * @returns Validation result
   */
  validateBackup(backupId: string): Promise<BackupValidation>;

  /**
   * Get the latest full backup
   * @returns Latest full backup metadata or null
   */
  getLatestFullBackup(): Promise<BackupMetadata | null>;

  /**
   * Get incremental backups since a full backup
   * @param fullBackupId Full backup ID
   * @returns Array of incremental backup metadata
   */
  getIncrementalChain(fullBackupId: string): Promise<BackupMetadata[]>;
}

/**
 * Restore manager interface
 */
export interface RestoreManager {
  /**
   * Restore database from a backup
   * @param options Restore options
   * @returns Restore result
   */
  restore(options: RestoreOptions): Promise<RestoreResult>;

  /**
   * Validate that restore can be performed
   * @param options Restore options
   * @returns Validation result
   */
  validateRestore(options: RestoreOptions): Promise<BackupValidation>;

  /**
   * Find the appropriate backup chain for point-in-time recovery
   * @param targetLSN Target LSN to recover to
   * @returns Array of backup IDs needed for recovery
   */
  findBackupChainForLSN(targetLSN: LSN): Promise<string[]>;

  /**
   * Find the appropriate backup chain for timestamp-based recovery
   * @param targetTimestamp Target timestamp to recover to
   * @returns Array of backup IDs needed for recovery
   */
  findBackupChainForTimestamp(targetTimestamp: number): Promise<string[]>;
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * Backup-specific error codes
 */
export enum BackupErrorCode {
  /** Backup not found */
  BACKUP_NOT_FOUND = 'BACKUP_NOT_FOUND',
  /** Backup is corrupted */
  BACKUP_CORRUPTED = 'BACKUP_CORRUPTED',
  /** Backup checksum mismatch */
  CHECKSUM_MISMATCH = 'BACKUP_CHECKSUM_MISMATCH',
  /** Backup already exists */
  BACKUP_EXISTS = 'BACKUP_EXISTS',
  /** Backup in progress */
  BACKUP_IN_PROGRESS = 'BACKUP_IN_PROGRESS',
  /** Restore failed */
  RESTORE_FAILED = 'RESTORE_FAILED',
  /** Missing base backup for incremental */
  MISSING_BASE_BACKUP = 'MISSING_BASE_BACKUP',
  /** Invalid backup chain */
  INVALID_CHAIN = 'INVALID_CHAIN',
  /** Target LSN not reachable */
  TARGET_UNREACHABLE = 'TARGET_UNREACHABLE',
  /** Storage error during backup/restore */
  STORAGE_ERROR = 'BACKUP_STORAGE_ERROR',
  /** Invalid backup configuration */
  INVALID_CONFIG = 'BACKUP_INVALID_CONFIG',
}

/**
 * Error class for backup and restore operations
 *
 * Extends DoSQLError to provide consistent error handling with:
 * - Machine-readable error codes
 * - Error categories for API layer handling
 * - Recovery hints for developers
 * - Backup ID context for debugging
 */
export class BackupError extends DoSQLError {
  readonly code: BackupErrorCode;
  readonly category: ErrorCategory;
  readonly backupId?: string;

  constructor(
    code: BackupErrorCode,
    message: string,
    options?: {
      cause?: Error;
      context?: ErrorContext;
      backupId?: string;
    }
  ) {
    super(message, { cause: options?.cause, context: options?.context });
    this.name = 'BackupError';
    this.code = code;
    this.backupId = options?.backupId;

    // Set category based on error code
    this.category = this.determineCategory();

    // Include backup ID in context
    if (this.backupId) {
      this.context = {
        ...this.context,
        metadata: {
          ...this.context?.metadata,
          backupId: this.backupId,
        },
      };
    }

    // Set recovery hints
    this.setRecoveryHint();
  }

  private determineCategory(): ErrorCategory {
    switch (this.code) {
      case BackupErrorCode.BACKUP_NOT_FOUND:
      case BackupErrorCode.MISSING_BASE_BACKUP:
        return ErrorCategory.RESOURCE;
      case BackupErrorCode.BACKUP_CORRUPTED:
      case BackupErrorCode.CHECKSUM_MISMATCH:
        return ErrorCategory.INTERNAL;
      case BackupErrorCode.INVALID_CONFIG:
      case BackupErrorCode.INVALID_CHAIN:
        return ErrorCategory.VALIDATION;
      case BackupErrorCode.BACKUP_EXISTS:
      case BackupErrorCode.BACKUP_IN_PROGRESS:
        return ErrorCategory.CONFLICT;
      case BackupErrorCode.RESTORE_FAILED:
      case BackupErrorCode.TARGET_UNREACHABLE:
      case BackupErrorCode.STORAGE_ERROR:
        return ErrorCategory.EXECUTION;
      default:
        return ErrorCategory.EXECUTION;
    }
  }

  private setRecoveryHint(): void {
    switch (this.code) {
      case BackupErrorCode.BACKUP_NOT_FOUND:
        this.recoveryHint = 'The specified backup does not exist. List available backups to find valid IDs.';
        break;
      case BackupErrorCode.BACKUP_CORRUPTED:
        this.recoveryHint = 'Backup data is corrupted. Try restoring from a different backup.';
        break;
      case BackupErrorCode.CHECKSUM_MISMATCH:
        this.recoveryHint = 'Backup integrity check failed. The backup may be incomplete or corrupted.';
        break;
      case BackupErrorCode.BACKUP_EXISTS:
        this.recoveryHint = 'A backup with this ID already exists. Use a different ID or delete the existing backup.';
        break;
      case BackupErrorCode.BACKUP_IN_PROGRESS:
        this.recoveryHint = 'Another backup is currently in progress. Wait for it to complete.';
        break;
      case BackupErrorCode.RESTORE_FAILED:
        this.recoveryHint = 'Restore operation failed. Check storage availability and backup integrity.';
        break;
      case BackupErrorCode.MISSING_BASE_BACKUP:
        this.recoveryHint = 'The base backup for this incremental backup is missing. Restore from a full backup instead.';
        break;
      case BackupErrorCode.INVALID_CHAIN:
        this.recoveryHint = 'The backup chain is broken. A full backup is required.';
        break;
      case BackupErrorCode.TARGET_UNREACHABLE:
        this.recoveryHint = 'The target LSN/timestamp is not reachable with available backups and WAL.';
        break;
      case BackupErrorCode.STORAGE_ERROR:
        this.recoveryHint = 'Storage error occurred. Check R2 connectivity and permissions.';
        break;
      case BackupErrorCode.INVALID_CONFIG:
        this.recoveryHint = 'Backup configuration is invalid. Check the configuration parameters.';
        break;
    }
  }

  /**
   * Check if this error is retryable
   */
  override isRetryable(): boolean {
    return [
      BackupErrorCode.STORAGE_ERROR,
      BackupErrorCode.BACKUP_IN_PROGRESS,
    ].includes(this.code);
  }

  /**
   * Get a user-friendly error message
   */
  override toUserMessage(): string {
    switch (this.code) {
      case BackupErrorCode.BACKUP_NOT_FOUND:
        return 'The requested backup was not found.';
      case BackupErrorCode.BACKUP_CORRUPTED:
        return 'The backup is corrupted and cannot be used.';
      case BackupErrorCode.CHECKSUM_MISMATCH:
        return 'Backup integrity check failed.';
      case BackupErrorCode.BACKUP_EXISTS:
        return 'A backup with this name already exists.';
      case BackupErrorCode.BACKUP_IN_PROGRESS:
        return 'A backup is already in progress.';
      case BackupErrorCode.RESTORE_FAILED:
        return 'Failed to restore from backup.';
      case BackupErrorCode.MISSING_BASE_BACKUP:
        return 'The base backup required for this restore is missing.';
      case BackupErrorCode.INVALID_CHAIN:
        return 'The backup chain is incomplete.';
      case BackupErrorCode.TARGET_UNREACHABLE:
        return 'Cannot recover to the specified point in time.';
      case BackupErrorCode.STORAGE_ERROR:
        return 'Storage error during backup operation.';
      case BackupErrorCode.INVALID_CONFIG:
        return 'Invalid backup configuration.';
      default:
        return this.message;
    }
  }

  /**
   * Deserialize from JSON
   */
  static fromJSON(json: SerializedError): BackupError {
    return new BackupError(
      json.code as BackupErrorCode,
      json.message,
      {
        context: json.context,
        backupId: json.context?.metadata?.backupId as string | undefined,
      }
    );
  }
}

// Register for deserialization
registerErrorClass('BackupError', BackupError);
