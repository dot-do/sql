/**
 * Database Backup for DoSQL
 *
 * Creates consistent snapshots of the database to R2 storage.
 * Supports both full backups and incremental backups based on WAL.
 */

import type { FSXBackend } from '../fsx/types.js';
import type { WALReader } from '../wal/types.js';
import { createLSN, type LSN } from '../engine/types.js';
import { crc32 } from '../utils/crypto.js';
import {
  type BackupConfig,
  type BackupManager,
  type BackupMetadata,
  type BackupManifest,
  type BackupFileEntry,
  type BackupOptions,
  type BackupProgress,
  type BackupResult,
  type BackupValidation,
  type BackupType,
  type CompressionType,
  DEFAULT_BACKUP_CONFIG,
  BackupError,
  BackupErrorCode,
} from './types.js';

// =============================================================================
// Backup ID Generation
// =============================================================================

/**
 * Generate a unique backup ID
 */
export function generateBackupId(type: BackupType = 'full'): string {
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).substring(2, 8);
  return `${type}_${timestamp}_${random}`;
}

// =============================================================================
// Backup Encoder/Decoder
// =============================================================================

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * Encode backup manifest to JSON bytes
 */
function encodeManifest(manifest: BackupManifest): Uint8Array {
  const obj = {
    metadata: {
      ...manifest.metadata,
      startLSN: manifest.metadata.startLSN.toString(),
      endLSN: manifest.metadata.endLSN.toString(),
    },
    files: manifest.files,
    walSegments: manifest.walSegments,
  };
  return textEncoder.encode(JSON.stringify(obj, null, 2));
}

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
 * Encode backup metadata to JSON bytes
 */
function encodeMetadata(metadata: BackupMetadata): Uint8Array {
  const obj = {
    ...metadata,
    startLSN: metadata.startLSN.toString(),
    endLSN: metadata.endLSN.toString(),
  };
  return textEncoder.encode(JSON.stringify(obj, null, 2));
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

// =============================================================================
// Backup Manager Implementation
// =============================================================================

/**
 * Options for creating a backup manager
 */
export interface CreateBackupManagerOptions {
  /** Source backend (DO storage) */
  sourceBackend: FSXBackend;
  /** Target backend (R2 storage for backups) */
  targetBackend: FSXBackend;
  /** WAL reader for accessing WAL segments */
  walReader?: WALReader;
  /** Current LSN getter */
  getCurrentLSN: () => LSN;
  /** Database identifier */
  databaseId: string;
  /** Backup configuration */
  config?: Partial<BackupConfig>;
  /** Pause writes callback (optional - for consistent snapshots) */
  pauseWrites?: () => Promise<void>;
  /** Resume writes callback */
  resumeWrites?: () => Promise<void>;
}

/**
 * Create a backup manager
 */
export function createBackupManager(options: CreateBackupManagerOptions): BackupManager {
  const {
    sourceBackend,
    targetBackend,
    walReader,
    getCurrentLSN,
    databaseId,
    pauseWrites,
    resumeWrites,
  } = options;

  const config: BackupConfig = { ...DEFAULT_BACKUP_CONFIG, ...options.config };

  // Track in-progress backups
  let backupInProgress = false;

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
   * List all data files to backup
   */
  async function listDataFiles(): Promise<string[]> {
    const allFiles = await sourceBackend.list(config.dataPrefix);
    // Filter out WAL and backup directories
    return allFiles.filter(
      (f) => !f.startsWith(config.walPrefix) && !f.startsWith(config.backupPrefix)
    );
  }

  /**
   * Calculate total size of files
   */
  async function calculateTotalSize(files: string[]): Promise<number> {
    let totalSize = 0;
    for (const file of files) {
      const data = await sourceBackend.read(file);
      if (data) {
        totalSize += data.length;
      }
    }
    return totalSize;
  }

  /**
   * Copy a file with checksum calculation
   */
  async function copyFile(
    sourcePath: string,
    targetPath: string
  ): Promise<BackupFileEntry | null> {
    const data = await sourceBackend.read(sourcePath);
    if (!data) {
      return null;
    }

    const checksum = crc32(data);
    await targetBackend.write(targetPath, data);

    return {
      path: sourcePath,
      size: data.length,
      checksum,
    };
  }

  /**
   * Get files modified since a base backup
   */
  async function getModifiedFilesSinceBackup(
    baseManifest: BackupManifest
  ): Promise<string[]> {
    const allFiles = await listDataFiles();
    const baseFileMap = new Map(
      baseManifest.files.map((f) => [f.path, f.checksum])
    );

    const modified: string[] = [];

    for (const file of allFiles) {
      const data = await sourceBackend.read(file);
      if (!data) continue;

      const currentChecksum = crc32(data);
      const baseChecksum = baseFileMap.get(file);

      // File is modified if it's new or checksum differs
      if (baseChecksum === undefined || baseChecksum !== currentChecksum) {
        modified.push(file);
      }
    }

    return modified;
  }

  /**
   * Get WAL segments to include in backup
   */
  async function getWALSegments(fromLSN: LSN): Promise<string[]> {
    if (!walReader) {
      return [];
    }

    try {
      const segments = await walReader.listSegments(false);
      const relevantSegments: string[] = [];

      for (const segmentId of segments) {
        const segment = await walReader.readSegment(segmentId);
        if (segment && segment.endLSN >= fromLSN) {
          relevantSegments.push(segmentId);
          if (relevantSegments.length >= config.maxWALSegments) {
            break;
          }
        }
      }

      return relevantSegments;
    } catch {
      return [];
    }
  }

  // Public interface
  const manager: BackupManager = {
    async createBackup(backupOptions: BackupOptions = {}): Promise<BackupResult> {
      if (backupInProgress) {
        throw new BackupError(
          BackupErrorCode.BACKUP_IN_PROGRESS,
          'Another backup is already in progress'
        );
      }

      backupInProgress = true;
      const startTime = Date.now();
      const warnings: string[] = [];

      const type = backupOptions.type ?? 'full';
      const backupId = generateBackupId(type);
      const compression = backupOptions.compression ?? config.compression;
      const includeWAL = backupOptions.includeWAL ?? config.includeWAL;

      try {
        // For incremental backup, get the base backup
        let baseManifest: BackupManifest | null = null;
        if (type === 'incremental') {
          if (!backupOptions.baseBackupId) {
            // Find the latest full backup
            const latestFull = await manager.getLatestFullBackup();
            if (!latestFull) {
              throw new BackupError(
                BackupErrorCode.MISSING_BASE_BACKUP,
                'No base backup found for incremental backup'
              );
            }
            backupOptions.baseBackupId = latestFull.id;
          }

          baseManifest = await manager.getBackup(backupOptions.baseBackupId);
          if (!baseManifest) {
            throw new BackupError(
              BackupErrorCode.MISSING_BASE_BACKUP,
              `Base backup ${backupOptions.baseBackupId} not found`,
              { backupId: backupOptions.baseBackupId }
            );
          }
        }

        // Pause writes for consistent snapshot if available
        if (pauseWrites) {
          await pauseWrites();
        }

        const startLSN = getCurrentLSN();

        // Determine files to backup
        let filesToBackup: string[];
        if (type === 'incremental' && baseManifest) {
          filesToBackup = await getModifiedFilesSinceBackup(baseManifest);
        } else {
          filesToBackup = await listDataFiles();
        }

        const totalSize = await calculateTotalSize(filesToBackup);
        const totalFiles = filesToBackup.length;

        // Report initial progress
        if (backupOptions.onProgress) {
          backupOptions.onProgress({
            phase: 'preparing',
            filesProcessed: 0,
            totalFiles,
            bytesWritten: 0,
            totalBytes: totalSize,
          });
        }

        // Copy data files
        const fileEntries: BackupFileEntry[] = [];
        let bytesWritten = 0;

        for (let i = 0; i < filesToBackup.length; i++) {
          const file = filesToBackup[i];

          if (backupOptions.onProgress) {
            backupOptions.onProgress({
              phase: 'copying_data',
              filesProcessed: i,
              totalFiles,
              bytesWritten,
              totalBytes: totalSize,
              currentFile: file,
            });
          }

          const entry = await copyFile(file, getDataPath(backupId, file));
          if (entry) {
            fileEntries.push({
              ...entry,
              modified: type === 'incremental',
            });
            bytesWritten += entry.size;
          }
        }

        // Get current LSN after data copy
        const endLSN = getCurrentLSN();

        // Resume writes
        if (resumeWrites) {
          await resumeWrites();
        }

        // Copy WAL segments if requested
        const walSegments: string[] = [];
        if (includeWAL && walReader) {
          if (backupOptions.onProgress) {
            backupOptions.onProgress({
              phase: 'copying_wal',
              filesProcessed: fileEntries.length,
              totalFiles,
              bytesWritten,
              totalBytes: totalSize,
            });
          }

          const segments = await getWALSegments(
            type === 'incremental' && baseManifest
              ? baseManifest.metadata.endLSN
              : startLSN
          );

          for (const segmentId of segments) {
            const segment = await walReader.readSegment(segmentId);
            if (segment) {
              // WAL segments are already encoded, just need to read and store
              const segmentData = await sourceBackend.read(
                `${config.walPrefix}${segmentId}`
              );
              if (segmentData) {
                await targetBackend.write(getWALPath(backupId, segmentId), segmentData);
                walSegments.push(segmentId);
              }
            }
          }
        }

        // Calculate overall checksum
        const manifestData = fileEntries.map((f) => `${f.path}:${f.checksum}`).join('\n');
        const overallChecksum = crc32(textEncoder.encode(manifestData));

        // Create metadata
        const metadata: BackupMetadata = {
          id: backupId,
          type,
          status: 'completed',
          databaseId,
          startedAt: startTime,
          completedAt: Date.now(),
          startLSN,
          endLSN,
          baseBackupId: backupOptions.baseBackupId,
          checksum: overallChecksum,
          sizeBytes: bytesWritten,
          fileCount: fileEntries.length,
          compression,
          description: backupOptions.description,
          custom: backupOptions.customMetadata,
        };

        // Create manifest
        const manifest: BackupManifest = {
          metadata,
          files: fileEntries,
          walSegments,
        };

        // Write manifest and metadata
        if (backupOptions.onProgress) {
          backupOptions.onProgress({
            phase: 'finalizing',
            filesProcessed: fileEntries.length,
            totalFiles,
            bytesWritten,
            totalBytes: totalSize,
          });
        }

        await targetBackend.write(getManifestPath(backupId), encodeManifest(manifest));
        await targetBackend.write(getMetadataPath(backupId), encodeMetadata(metadata));

        // Verify backup if configured
        if (config.verifyAfterBackup) {
          if (backupOptions.onProgress) {
            backupOptions.onProgress({
              phase: 'verifying',
              filesProcessed: fileEntries.length,
              totalFiles,
              bytesWritten,
              totalBytes: totalSize,
            });
          }

          const validation = await manager.validateBackup(backupId);
          if (!validation.valid) {
            warnings.push(`Backup verification failed: ${validation.errors.join(', ')}`);
          }
        }

        return {
          success: true,
          backupId,
          metadata,
          durationMs: Date.now() - startTime,
          warnings,
        };
      } catch (error) {
        // Resume writes on error
        if (resumeWrites) {
          try {
            await resumeWrites();
          } catch {
            // Ignore resume error
          }
        }

        if (error instanceof BackupError) {
          throw error;
        }

        throw new BackupError(
          BackupErrorCode.STORAGE_ERROR,
          `Backup failed: ${error instanceof Error ? error.message : String(error)}`,
          {
            backupId,
            cause: error instanceof Error ? error : undefined,
          }
        );
      } finally {
        backupInProgress = false;
      }
    },

    async listBackups(limit = 100): Promise<BackupMetadata[]> {
      const backups: BackupMetadata[] = [];

      try {
        const allPaths = await targetBackend.list(config.backupPrefix);

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
          if (backups.length >= limit) break;

          const metadataPath = getMetadataPath(backupId);
          const data = await targetBackend.read(metadataPath);
          if (data) {
            try {
              backups.push(decodeMetadata(data));
            } catch {
              // Skip corrupted metadata
            }
          }
        }

        // Sort by completedAt descending (newest first)
        backups.sort((a, b) => (b.completedAt ?? 0) - (a.completedAt ?? 0));

        return backups;
      } catch {
        return [];
      }
    },

    async getBackup(backupId: string): Promise<BackupManifest | null> {
      try {
        const manifestPath = getManifestPath(backupId);
        const data = await targetBackend.read(manifestPath);
        if (!data) {
          return null;
        }
        return decodeManifest(data);
      } catch {
        return null;
      }
    },

    async deleteBackup(backupId: string): Promise<boolean> {
      try {
        // List all files for this backup
        const backupPath = `${config.backupPrefix}${backupId}/`;
        const files = await targetBackend.list(backupPath);

        // Delete all files
        for (const file of files) {
          await targetBackend.delete(file);
        }

        return true;
      } catch {
        return false;
      }
    },

    async validateBackup(backupId: string): Promise<BackupValidation> {
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
        // Load manifest
        const manifest = await manager.getBackup(backupId);
        if (!manifest) {
          result.valid = false;
          result.manifestValid = false;
          result.errors.push('Manifest not found');
          return result;
        }

        // Verify each file
        for (const file of manifest.files) {
          const dataPath = getDataPath(backupId, file.path);
          const data = await targetBackend.read(dataPath);

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

          if (data.length !== file.size) {
            result.valid = false;
            result.corruptedFiles.push(file.path);
            result.errors.push(`Size mismatch for ${file.path}`);
          }
        }

        // Verify WAL segments if present
        if (manifest.walSegments) {
          for (const segmentId of manifest.walSegments) {
            const walPath = getWALPath(backupId, segmentId);
            const exists = await targetBackend.exists(walPath);
            if (!exists) {
              result.valid = false;
              result.filesComplete = false;
              result.missingFiles.push(`wal/${segmentId}`);
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

    async getLatestFullBackup(): Promise<BackupMetadata | null> {
      const backups = await manager.listBackups();
      const fullBackups = backups.filter((b) => b.type === 'full' && b.status === 'completed');
      return fullBackups.length > 0 ? fullBackups[0] : null;
    },

    async getIncrementalChain(fullBackupId: string): Promise<BackupMetadata[]> {
      const backups = await manager.listBackups();
      const chain: BackupMetadata[] = [];

      for (const backup of backups) {
        if (backup.type === 'incremental' && backup.baseBackupId === fullBackupId) {
          chain.push(backup);
        }
      }

      // Sort by completedAt ascending (oldest first)
      chain.sort((a, b) => (a.completedAt ?? 0) - (b.completedAt ?? 0));

      return chain;
    },
  };

  return manager;
}
