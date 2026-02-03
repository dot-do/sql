/**
 * Backup and Restore Module for DoSQL
 *
 * Provides database backup and point-in-time recovery (PITR) functionality:
 * - Full and incremental backups to R2
 * - Consistent snapshots using MVCC/write pausing
 * - Point-in-time recovery via WAL replay
 * - Backup integrity validation
 *
 * @example
 * ```typescript
 * import {
 *   createBackupManager,
 *   createRestoreManager,
 *   BackupError,
 *   BackupErrorCode,
 * } from 'dosql/backup';
 *
 * // Create backup manager
 * const backupManager = createBackupManager({
 *   sourceBackend: doStorage,
 *   targetBackend: r2Storage,
 *   walReader,
 *   getCurrentLSN: () => currentLSN,
 *   databaseId: 'my-database',
 * });
 *
 * // Create a full backup
 * const result = await backupManager.createBackup({
 *   type: 'full',
 *   description: 'Daily backup',
 * });
 *
 * // Create an incremental backup
 * const incrementalResult = await backupManager.createBackup({
 *   type: 'incremental',
 *   baseBackupId: result.backupId,
 * });
 *
 * // List backups
 * const backups = await backupManager.listBackups();
 *
 * // Validate backup
 * const validation = await backupManager.validateBackup(result.backupId);
 *
 * // Create restore manager
 * const restoreManager = createRestoreManager({
 *   sourceBackend: r2Storage,
 *   targetBackend: doStorage,
 *   applyWALEntry: async (entry) => { ... },
 * });
 *
 * // Restore from backup
 * const restoreResult = await restoreManager.restore({
 *   backupId: result.backupId,
 * });
 *
 * // Point-in-time recovery
 * const pitrResult = await restoreManager.restore({
 *   backupId: result.backupId,
 *   targetLSN: createLSN(1000n),
 * });
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// Types
// =============================================================================

export {
  // Backup types
  type BackupType,
  type CompressionType,
  type BackupStatus,
  type BackupMetadata,
  type BackupFileEntry,
  type BackupManifest,
  type BackupConfig,
  type BackupOptions,
  type BackupProgress,
  type BackupResult,
  DEFAULT_BACKUP_CONFIG,

  // Restore types
  type RestoreOptions,
  type RestoreProgress,
  type RestoreResult,
  type BackupValidation,

  // Manager interfaces
  type BackupManager,
  type RestoreManager,

  // Error types
  BackupError,
  BackupErrorCode,
} from './types.js';

// =============================================================================
// Backup
// =============================================================================

export {
  createBackupManager,
  generateBackupId,
  type CreateBackupManagerOptions,
} from './backup.js';

// =============================================================================
// Restore
// =============================================================================

export {
  createRestoreManager,
  type CreateRestoreManagerOptions,
} from './restore.js';
