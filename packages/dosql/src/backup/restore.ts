/**
 * Database Restore for DoSQL
 *
 * Restores database from R2 backups with point-in-time recovery (PITR) support.
 * Supports restoring from full backups and replaying WAL for precise recovery points.
 */

import type { FSXBackend } from '../fsx/types.js';
import type { WALEntry, WALReader } from '../wal/types.js';
import { DefaultWALEncoder } from '../wal/writer.js';
import { createLSN, type LSN } from '../engine/types.js';
import { crc32 } from '../utils/crypto.js';
import {
  type BackupConfig,
  type BackupManifest,
  type BackupMetadata,
  type RestoreManager,
  type RestoreOptions,
  type RestoreProgress,
  type RestoreResult,
  type BackupValidation,
  DEFAULT_BACKUP_CONFIG,
  BackupError,
  BackupErrorCode,
} from './types.js';

// =============================================================================
// Restore Manager Implementation
// =============================================================================

/**
 * Options for creating a restore manager
 */
export interface CreateRestoreManagerOptions {
  /** Source backend (R2 storage with backups) */
  sourceBackend: FSXBackend;
  /** Target backend (DO storage to restore to) */
  targetBackend: FSXBackend;
  /** WAL entry apply function */
  applyWALEntry?: (entry: WALEntry) => Promise<void>;
  /** Backup configuration */
  config?: Partial<BackupConfig>;
  /** Callback to update current LSN after restore */
  setCurrentLSN?: (lsn: LSN) => void;
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * Decode backup manifest from JSON bytes
 */
function decodeManifest(data: Uint8Array): BackupManifest {
  const json = textDecoder.decode(data);
  const obj = JSON.parse(json);
  return {
    metadata: {
      ...obj.metadata,
      startLSN: createLSN(BigInt(obj.metadata.startLSN)),
      endLSN: createLSN(BigInt(obj.metadata.endLSN)),
    },
    files: obj.files,
    walSegments: obj.walSegments,
  };
}

/**
 * Decode backup metadata from JSON bytes
 */
function decodeMetadata(data: Uint8Array): BackupMetadata {
  const json = textDecoder.decode(data);
  const obj = JSON.parse(json);
  return {
    ...obj,
    startLSN: createLSN(BigInt(obj.startLSN)),
    endLSN: createLSN(BigInt(obj.endLSN)),
  };
}

/**
 * Create a restore manager
 */
export function createRestoreManager(options: CreateRestoreManagerOptions): RestoreManager {
  const {
    sourceBackend,
    targetBackend,
    applyWALEntry,
    setCurrentLSN,
  } = options;

  const config: BackupConfig = { ...DEFAULT_BACKUP_CONFIG, ...options.config };
  const walEncoder = new DefaultWALEncoder();

  /**
   * Get path for backup manifest
   */
  function getManifestPath(backupId: string): string {
    return `${config.backupPrefix}${backupId}/manifest.json`;
  }

  /**
   * Get path for backup metadata
   */
  function getMetadataPath(backupId: string): string {
    return `${config.backupPrefix}${backupId}/metadata.json`;
  }

  /**
   * Get path for backup data file
   */
  function getDataPath(backupId: string, filePath: string): string {
    return `${config.backupPrefix}${backupId}/data/${filePath}`;
  }

  /**
   * Get path for backup WAL segment
   */
  function getWALPath(backupId: string, segmentId: string): string {
    return `${config.backupPrefix}${backupId}/wal/${segmentId}`;
  }

  /**
   * Load backup manifest
   */
  async function loadManifest(backupId: string): Promise<BackupManifest | null> {
    try {
      const manifestPath = getManifestPath(backupId);
      const data = await sourceBackend.read(manifestPath);
      if (!data) {
        return null;
      }
      return decodeManifest(data);
    } catch {
      return null;
    }
  }

  /**
   * Load backup metadata
   */
  async function loadMetadata(backupId: string): Promise<BackupMetadata | null> {
    try {
      const metadataPath = getMetadataPath(backupId);
      const data = await sourceBackend.read(metadataPath);
      if (!data) {
        return null;
      }
      return decodeMetadata(data);
    } catch {
      return null;
    }
  }

  /**
   * List all backups
   */
  async function listBackups(): Promise<BackupMetadata[]> {
    const backups: BackupMetadata[] = [];

    try {
      const allPaths = await sourceBackend.list(config.backupPrefix);

      // Extract unique backup IDs from paths
      const backupIds = new Set<string>();
      for (const path of allPaths) {
        const relativePath = path.replace(config.backupPrefix, '');
        const backupId = relativePath.split('/')[0];
        if (backupId && !backupIds.has(backupId)) {
          backupIds.add(backupId);
        }
      }

      // Load metadata for each backup
      for (const backupId of backupIds) {
        const metadata = await loadMetadata(backupId);
        if (metadata) {
          backups.push(metadata);
        }
      }

      // Sort by completedAt descending (newest first)
      backups.sort((a, b) => (b.completedAt ?? 0) - (a.completedAt ?? 0));

      return backups;
    } catch {
      return [];
    }
  }

  /**
   * Build the full backup chain for an incremental backup
   */
  async function buildBackupChain(backupId: string): Promise<string[]> {
    const chain: string[] = [];
    let currentId: string | undefined = backupId;

    while (currentId) {
      chain.unshift(currentId);
      const metadata = await loadMetadata(currentId);
      if (!metadata) {
        throw new BackupError(
          BackupErrorCode.BACKUP_NOT_FOUND,
          `Backup ${currentId} not found`,
          { backupId: currentId }
        );
      }

      if (metadata.type === 'full') {
        break;
      }

      currentId = metadata.baseBackupId;
      if (!currentId) {
        throw new BackupError(
          BackupErrorCode.INVALID_CHAIN,
          'Incremental backup chain broken - missing base backup',
          { backupId }
        );
      }
    }

    return chain;
  }

  /**
   * Validate backup integrity
   */
  async function validateBackupIntegrity(backupId: string): Promise<BackupValidation> {
    const result: BackupValidation = {
      valid: true,
      checksumValid: true,
      manifestValid: true,
      filesComplete: true,
      missingFiles: [],
      corruptedFiles: [],
      errors: [],
    };

    try {
      const manifest = await loadManifest(backupId);
      if (!manifest) {
        result.valid = false;
        result.manifestValid = false;
        result.errors.push('Manifest not found');
        return result;
      }

      // Verify each file
      for (const file of manifest.files) {
        const dataPath = getDataPath(backupId, file.path);
        const data = await sourceBackend.read(dataPath);

        if (!data) {
          result.valid = false;
          result.filesComplete = false;
          result.missingFiles.push(file.path);
          continue;
        }

        const actualChecksum = crc32(data);
        if (actualChecksum !== file.checksum) {
          result.valid = false;
          result.checksumValid = false;
          result.corruptedFiles.push(file.path);
          result.errors.push(`Checksum mismatch for ${file.path}`);
        }
      }

      return result;
    } catch (error) {
      result.valid = false;
      result.errors.push(`Validation error: ${error instanceof Error ? error.message : String(error)}`);
      return result;
    }
  }

  /**
   * Restore data files from a backup
   */
  async function restoreDataFiles(
    manifest: BackupManifest,
    backupId: string,
    onProgress?: (progress: RestoreProgress) => void
  ): Promise<{ filesRestored: number; bytesRestored: number }> {
    let filesRestored = 0;
    let bytesRestored = 0;
    const totalFiles = manifest.files.length;
    const totalBytes = manifest.files.reduce((sum, f) => sum + f.size, 0);

    for (const file of manifest.files) {
      if (onProgress) {
        onProgress({
          phase: 'restoring_data',
          filesProcessed: filesRestored,
          totalFiles,
          bytesRestored,
          totalBytes,
          currentFile: file.path,
        });
      }

      const sourcePath = getDataPath(backupId, file.path);
      const data = await sourceBackend.read(sourcePath);

      if (!data) {
        throw new BackupError(
          BackupErrorCode.BACKUP_CORRUPTED,
          `Missing file in backup: ${file.path}`,
          { backupId }
        );
      }

      // Verify checksum
      const actualChecksum = crc32(data);
      if (actualChecksum !== file.checksum) {
        throw new BackupError(
          BackupErrorCode.CHECKSUM_MISMATCH,
          `Checksum mismatch for ${file.path}`,
          { backupId }
        );
      }

      // Write to target
      await targetBackend.write(file.path, data);
      filesRestored++;
      bytesRestored += data.length;
    }

    return { filesRestored, bytesRestored };
  }

  /**
   * Replay WAL entries from backup up to a target LSN
   */
  async function replayWAL(
    manifest: BackupManifest,
    backupId: string,
    targetLSN: LSN | undefined,
    onProgress?: (progress: RestoreProgress) => void
  ): Promise<{ entriesReplayed: number; finalLSN: LSN }> {
    if (!applyWALEntry || !manifest.walSegments || manifest.walSegments.length === 0) {
      return { entriesReplayed: 0, finalLSN: manifest.metadata.endLSN };
    }

    let entriesReplayed = 0;
    let finalLSN = manifest.metadata.endLSN;

    for (const segmentId of manifest.walSegments) {
      if (onProgress) {
        onProgress({
          phase: 'replaying_wal',
          filesProcessed: 0,
          totalFiles: 0,
          bytesRestored: 0,
          totalBytes: 0,
          walEntriesReplayed: entriesReplayed,
        });
      }

      const walPath = getWALPath(backupId, segmentId);
      const data = await sourceBackend.read(walPath);

      if (!data) {
        continue;
      }

      try {
        const segment = walEncoder.decodeSegment(data);

        for (const entry of segment.entries) {
          // Skip if past target LSN
          if (targetLSN !== undefined && entry.lsn > targetLSN) {
            return { entriesReplayed, finalLSN };
          }

          // Skip transaction control entries for actual replay
          if (entry.op !== 'BEGIN' && entry.op !== 'COMMIT' && entry.op !== 'ROLLBACK') {
            await applyWALEntry(entry);
          }

          entriesReplayed++;
          finalLSN = entry.lsn;
        }
      } catch {
        // Skip corrupted WAL segment
      }
    }

    return { entriesReplayed, finalLSN };
  }

  // Public interface
  const manager: RestoreManager = {
    async restore(restoreOptions: RestoreOptions): Promise<RestoreResult> {
      const startTime = Date.now();
      const warnings: string[] = [];

      try {
        // Build backup chain
        const backupChain = await buildBackupChain(restoreOptions.backupId);

        // Verify backups if requested
        if (restoreOptions.verifyBeforeRestore !== false) {
          if (restoreOptions.onProgress) {
            restoreOptions.onProgress({
              phase: 'verifying',
              filesProcessed: 0,
              totalFiles: 0,
              bytesRestored: 0,
              totalBytes: 0,
            });
          }

          for (const backupId of backupChain) {
            const validation = await validateBackupIntegrity(backupId);
            if (!validation.valid) {
              throw new BackupError(
                BackupErrorCode.BACKUP_CORRUPTED,
                `Backup ${backupId} is corrupted: ${validation.errors.join(', ')}`,
                { backupId }
              );
            }
          }
        }

        // Restore data files from each backup in the chain
        let totalFilesRestored = 0;
        let totalBytesRestored = 0;
        let lastManifest: BackupManifest | null = null;

        for (const backupId of backupChain) {
          const manifest = await loadManifest(backupId);
          if (!manifest) {
            throw new BackupError(
              BackupErrorCode.BACKUP_NOT_FOUND,
              `Backup manifest not found: ${backupId}`,
              { backupId }
            );
          }

          const { filesRestored, bytesRestored } = await restoreDataFiles(
            manifest,
            backupId,
            restoreOptions.onProgress
          );

          totalFilesRestored += filesRestored;
          totalBytesRestored += bytesRestored;
          lastManifest = manifest;
        }

        if (!lastManifest) {
          throw new BackupError(
            BackupErrorCode.RESTORE_FAILED,
            'No backup manifest found'
          );
        }

        // Replay WAL for PITR if needed
        let walEntriesReplayed = 0;
        let restoredLSN = lastManifest.metadata.endLSN;

        if (!restoreOptions.skipWALReplay) {
          // Determine target LSN
          let targetLSN = restoreOptions.targetLSN;

          // If timestamp is specified, find the appropriate LSN
          if (!targetLSN && restoreOptions.targetTimestamp) {
            // For timestamp-based PITR, we replay all WAL and filter by timestamp
            // The applyWALEntry function should check timestamps
            targetLSN = undefined; // Replay all available
          }

          const walResult = await replayWAL(
            lastManifest,
            restoreOptions.backupId,
            targetLSN,
            restoreOptions.onProgress
          );

          walEntriesReplayed = walResult.entriesReplayed;
          restoredLSN = walResult.finalLSN;
        }

        // Update current LSN if callback provided
        if (setCurrentLSN) {
          setCurrentLSN(restoredLSN);
        }

        // Finalize
        if (restoreOptions.onProgress) {
          restoreOptions.onProgress({
            phase: 'finalizing',
            filesProcessed: totalFilesRestored,
            totalFiles: totalFilesRestored,
            bytesRestored: totalBytesRestored,
            totalBytes: totalBytesRestored,
            walEntriesReplayed,
          });
        }

        return {
          success: true,
          backupId: restoreOptions.backupId,
          restoredLSN,
          filesRestored: totalFilesRestored,
          walEntriesReplayed,
          durationMs: Date.now() - startTime,
          warnings,
        };
      } catch (error) {
        if (error instanceof BackupError) {
          return {
            success: false,
            backupId: restoreOptions.backupId,
            restoredLSN: createLSN(0n),
            filesRestored: 0,
            walEntriesReplayed: 0,
            durationMs: Date.now() - startTime,
            warnings,
            error: error.message,
          };
        }

        return {
          success: false,
          backupId: restoreOptions.backupId,
          restoredLSN: createLSN(0n),
          filesRestored: 0,
          walEntriesReplayed: 0,
          durationMs: Date.now() - startTime,
          warnings,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    },

    async validateRestore(restoreOptions: RestoreOptions): Promise<BackupValidation> {
      const result: BackupValidation = {
        valid: true,
        checksumValid: true,
        manifestValid: true,
        filesComplete: true,
        missingFiles: [],
        corruptedFiles: [],
        errors: [],
      };

      try {
        // Build and validate backup chain
        const backupChain = await buildBackupChain(restoreOptions.backupId);

        for (const backupId of backupChain) {
          const validation = await validateBackupIntegrity(backupId);

          if (!validation.valid) {
            result.valid = false;
          }
          if (!validation.checksumValid) {
            result.checksumValid = false;
          }
          if (!validation.manifestValid) {
            result.manifestValid = false;
          }
          if (!validation.filesComplete) {
            result.filesComplete = false;
          }

          result.missingFiles.push(...validation.missingFiles);
          result.corruptedFiles.push(...validation.corruptedFiles);
          result.errors.push(...validation.errors);
        }

        // Validate target LSN is reachable if specified
        if (restoreOptions.targetLSN !== undefined) {
          const lastManifest = await loadManifest(restoreOptions.backupId);
          if (lastManifest) {
            const hasWAL = lastManifest.walSegments && lastManifest.walSegments.length > 0;

            if (!hasWAL && restoreOptions.targetLSN > lastManifest.metadata.endLSN) {
              result.valid = false;
              result.errors.push(
                `Target LSN ${restoreOptions.targetLSN} is beyond backup end LSN ${lastManifest.metadata.endLSN} and no WAL is available`
              );
            }
          }
        }

        return result;
      } catch (error) {
        result.valid = false;
        result.errors.push(`Validation error: ${error instanceof Error ? error.message : String(error)}`);
        return result;
      }
    },

    async findBackupChainForLSN(targetLSN: LSN): Promise<string[]> {
      const backups = await listBackups();

      // Find the most recent full backup that starts before or at targetLSN
      const fullBackups = backups.filter((b) => b.type === 'full' && b.status === 'completed');

      // Sort by startLSN descending to find the most recent one before target
      fullBackups.sort((a, b) => Number(b.startLSN - a.startLSN));

      let baseBackup: BackupMetadata | null = null;
      for (const backup of fullBackups) {
        if (backup.endLSN <= targetLSN) {
          baseBackup = backup;
          break;
        }
      }

      if (!baseBackup) {
        // Try to find any full backup and check if WAL can reach target
        baseBackup = fullBackups[fullBackups.length - 1] ?? null;
        if (!baseBackup) {
          throw new BackupError(
            BackupErrorCode.TARGET_UNREACHABLE,
            `No backup found that can reach LSN ${targetLSN}`
          );
        }
      }

      // Build chain with any incrementals
      const chain = await buildBackupChain(baseBackup.id);

      // Check if target is reachable
      const lastBackupId = chain[chain.length - 1];
      const lastManifest = await loadManifest(lastBackupId);

      if (lastManifest && lastManifest.metadata.endLSN < targetLSN) {
        // Check if WAL can bridge the gap
        if (!lastManifest.walSegments || lastManifest.walSegments.length === 0) {
          throw new BackupError(
            BackupErrorCode.TARGET_UNREACHABLE,
            `Target LSN ${targetLSN} is not reachable from available backups`
          );
        }
      }

      return chain;
    },

    async findBackupChainForTimestamp(targetTimestamp: number): Promise<string[]> {
      const backups = await listBackups();

      // Find backups completed before the target timestamp
      const eligibleBackups = backups.filter(
        (b) => b.status === 'completed' && (b.completedAt ?? 0) <= targetTimestamp
      );

      if (eligibleBackups.length === 0) {
        throw new BackupError(
          BackupErrorCode.TARGET_UNREACHABLE,
          `No backup found completed before timestamp ${targetTimestamp}`
        );
      }

      // Find the most recent full backup
      const fullBackups = eligibleBackups.filter((b) => b.type === 'full');

      if (fullBackups.length === 0) {
        throw new BackupError(
          BackupErrorCode.TARGET_UNREACHABLE,
          `No full backup found before timestamp ${targetTimestamp}`
        );
      }

      const baseBackup = fullBackups[0]; // Already sorted by completedAt descending
      return buildBackupChain(baseBackup.id);
    },
  };

  return manager;
}
