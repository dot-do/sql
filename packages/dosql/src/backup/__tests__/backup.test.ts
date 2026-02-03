/**
 * Backup and Restore Tests for DoSQL
 *
 * Tests for database backup and point-in-time recovery functionality.
 * Uses actual Cloudflare Workers environment via workers-vitest-pool.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { env } from 'cloudflare:test';

import {
  createBackupManager,
  createRestoreManager,
  generateBackupId,
  BackupError,
  BackupErrorCode,
  DEFAULT_BACKUP_CONFIG,
  type BackupManager,
  type RestoreManager,
  type BackupProgress,
  type RestoreProgress,
} from '../index.js';

import {
  MemoryFSXBackend,
  createMemoryBackend,
  createR2Backend,
  type R2BucketLike,
} from '../../fsx/index.js';

import {
  createWALWriter,
  createWALReader,
  type WALWriter,
  type WALReader,
  type WALEntry,
} from '../../wal/index.js';

import { createLSN, type LSN } from '../../engine/types.js';

// =============================================================================
// Test Utilities
// =============================================================================

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function textToBytes(text: string): Uint8Array {
  return textEncoder.encode(text);
}

function bytesToText(bytes: Uint8Array): string {
  return textDecoder.decode(bytes);
}

function generateTestData(size: number, seed = 0): Uint8Array {
  const data = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    data[i] = (i + seed) % 256;
  }
  return data;
}

// =============================================================================
// Backup ID Generation Tests
// =============================================================================

describe('generateBackupId', () => {
  it('should generate unique backup IDs', () => {
    const id1 = generateBackupId();
    const id2 = generateBackupId();

    expect(id1).not.toBe(id2);
  });

  it('should include backup type in ID', () => {
    const fullId = generateBackupId('full');
    const incrementalId = generateBackupId('incremental');

    expect(fullId).toMatch(/^full_/);
    expect(incrementalId).toMatch(/^incremental_/);
  });

  it('should generate IDs with consistent format', () => {
    const id = generateBackupId('full');
    // Format: type_timestamp_random
    const parts = id.split('_');
    expect(parts.length).toBe(3);
    expect(parts[0]).toBe('full');
  });
});

// =============================================================================
// Backup Manager Tests with Memory Backend
// =============================================================================

describe('BackupManager with MemoryBackend', () => {
  let sourceBackend: MemoryFSXBackend;
  let targetBackend: MemoryFSXBackend;
  let walBackend: MemoryFSXBackend;
  let walWriter: WALWriter;
  let walReader: WALReader;
  let backupManager: BackupManager;
  let currentLSN: LSN;

  beforeEach(async () => {
    sourceBackend = createMemoryBackend();
    targetBackend = createMemoryBackend();
    walBackend = createMemoryBackend();

    currentLSN = createLSN(0n);

    walWriter = createWALWriter(walBackend, 0n);
    walReader = createWALReader(walBackend);

    backupManager = createBackupManager({
      sourceBackend,
      targetBackend,
      walReader,
      getCurrentLSN: () => currentLSN,
      databaseId: 'test-db',
      config: {
        dataPrefix: '',
        walPrefix: '_wal/segments/',
      },
    });
  });

  afterEach(() => {
    sourceBackend.clear();
    targetBackend.clear();
    walBackend.clear();
  });

  describe('Full Backup', () => {
    it('should create a full backup of all data files', async () => {
      // Setup test data
      await sourceBackend.write('table1.dat', textToBytes('table1 data'));
      await sourceBackend.write('table2.dat', textToBytes('table2 data'));
      await sourceBackend.write('index1.idx', textToBytes('index data'));

      const result = await backupManager.createBackup({
        type: 'full',
        description: 'Test full backup',
      });

      expect(result.success).toBe(true);
      expect(result.metadata.type).toBe('full');
      expect(result.metadata.status).toBe('completed');
      expect(result.metadata.fileCount).toBe(3);
      expect(result.metadata.description).toBe('Test full backup');
      expect(result.durationMs).toBeGreaterThanOrEqual(0);
    });

    it('should include custom metadata in backup', async () => {
      await sourceBackend.write('data.dat', textToBytes('test'));

      const result = await backupManager.createBackup({
        customMetadata: {
          version: '1.0',
          environment: 'test',
        },
      });

      expect(result.success).toBe(true);
      expect(result.metadata.custom?.version).toBe('1.0');
      expect(result.metadata.custom?.environment).toBe('test');
    });

    it('should report progress during backup', async () => {
      await sourceBackend.write('file1.dat', generateTestData(1000));
      await sourceBackend.write('file2.dat', generateTestData(2000));

      const progressUpdates: BackupProgress[] = [];

      const result = await backupManager.createBackup({
        onProgress: (progress) => {
          progressUpdates.push({ ...progress });
        },
      });

      expect(result.success).toBe(true);
      expect(progressUpdates.length).toBeGreaterThan(0);

      // Should have preparing phase
      expect(progressUpdates.some((p) => p.phase === 'preparing')).toBe(true);

      // Should have copying_data phase
      expect(progressUpdates.some((p) => p.phase === 'copying_data')).toBe(true);
    });

    it('should calculate correct checksums', async () => {
      const testData = textToBytes('checksum test data');
      await sourceBackend.write('checksum.dat', testData);

      const result = await backupManager.createBackup();

      expect(result.success).toBe(true);

      // Validate backup should pass
      const validation = await backupManager.validateBackup(result.backupId);
      expect(validation.valid).toBe(true);
      expect(validation.checksumValid).toBe(true);
    });

    it('should handle empty database', async () => {
      const result = await backupManager.createBackup();

      expect(result.success).toBe(true);
      expect(result.metadata.fileCount).toBe(0);
      expect(result.metadata.sizeBytes).toBe(0);
    });

    it('should prevent concurrent backups', async () => {
      await sourceBackend.write('data.dat', generateTestData(10000));

      // Start first backup (don't await)
      const backup1Promise = backupManager.createBackup();

      // Try to start second backup immediately
      await expect(backupManager.createBackup()).rejects.toThrow(BackupError);

      // Complete first backup
      const result = await backup1Promise;
      expect(result.success).toBe(true);

      // Now another backup should work
      const result2 = await backupManager.createBackup();
      expect(result2.success).toBe(true);
    });
  });

  describe('Incremental Backup', () => {
    it('should create incremental backup based on full backup', async () => {
      // Create initial data
      await sourceBackend.write('original.dat', textToBytes('original'));

      // Create full backup
      const fullResult = await backupManager.createBackup({ type: 'full' });
      expect(fullResult.success).toBe(true);

      // Modify data
      await sourceBackend.write('original.dat', textToBytes('modified'));
      await sourceBackend.write('new.dat', textToBytes('new file'));

      // Create incremental backup
      const incrementalResult = await backupManager.createBackup({
        type: 'incremental',
        baseBackupId: fullResult.backupId,
      });

      expect(incrementalResult.success).toBe(true);
      expect(incrementalResult.metadata.type).toBe('incremental');
      expect(incrementalResult.metadata.baseBackupId).toBe(fullResult.backupId);
      // Should only backup modified/new files
      expect(incrementalResult.metadata.fileCount).toBe(2);
    });

    it('should auto-find latest full backup for incremental', async () => {
      await sourceBackend.write('data.dat', textToBytes('data'));

      // Create full backup
      const fullResult = await backupManager.createBackup({ type: 'full' });
      expect(fullResult.success).toBe(true);

      // Modify data
      await sourceBackend.write('data.dat', textToBytes('modified'));

      // Create incremental without specifying base
      const incrementalResult = await backupManager.createBackup({
        type: 'incremental',
      });

      expect(incrementalResult.success).toBe(true);
      expect(incrementalResult.metadata.baseBackupId).toBe(fullResult.backupId);
    });

    it('should fail incremental when no base backup exists', async () => {
      await sourceBackend.write('data.dat', textToBytes('data'));

      await expect(
        backupManager.createBackup({
          type: 'incremental',
          baseBackupId: 'nonexistent',
        })
      ).rejects.toThrow(BackupError);
    });
  });

  describe('Backup Management', () => {
    it('should list all backups', async () => {
      await sourceBackend.write('data.dat', textToBytes('data'));

      await backupManager.createBackup({ description: 'Backup 1' });
      await backupManager.createBackup({ description: 'Backup 2' });
      await backupManager.createBackup({ description: 'Backup 3' });

      const backups = await backupManager.listBackups();

      expect(backups.length).toBe(3);
      // Should be sorted by completedAt descending
      expect(backups[0].description).toBe('Backup 3');
    });

    it('should get backup details', async () => {
      await sourceBackend.write('data.dat', textToBytes('test data'));

      const result = await backupManager.createBackup();

      const manifest = await backupManager.getBackup(result.backupId);

      expect(manifest).not.toBeNull();
      expect(manifest?.metadata.id).toBe(result.backupId);
      expect(manifest?.files.length).toBe(1);
      expect(manifest?.files[0].path).toBe('data.dat');
    });

    it('should return null for non-existent backup', async () => {
      const manifest = await backupManager.getBackup('nonexistent');
      expect(manifest).toBeNull();
    });

    it('should delete a backup', async () => {
      await sourceBackend.write('data.dat', textToBytes('data'));

      const result = await backupManager.createBackup();

      // Verify backup exists
      let manifest = await backupManager.getBackup(result.backupId);
      expect(manifest).not.toBeNull();

      // Delete backup
      const deleted = await backupManager.deleteBackup(result.backupId);
      expect(deleted).toBe(true);

      // Verify backup is gone
      manifest = await backupManager.getBackup(result.backupId);
      expect(manifest).toBeNull();
    });

    it('should get latest full backup', async () => {
      await sourceBackend.write('data.dat', textToBytes('data'));

      await backupManager.createBackup({ type: 'full', description: 'First' });
      // Add a small delay to ensure different timestamps
      await new Promise((resolve) => setTimeout(resolve, 10));
      await backupManager.createBackup({ type: 'full', description: 'Second' });

      const latest = await backupManager.getLatestFullBackup();

      expect(latest).not.toBeNull();
      expect(latest?.description).toBe('Second');
    });

    it('should get incremental chain', async () => {
      await sourceBackend.write('data.dat', textToBytes('data'));

      const fullResult = await backupManager.createBackup({ type: 'full' });

      await sourceBackend.write('data.dat', textToBytes('modified1'));
      await backupManager.createBackup({
        type: 'incremental',
        baseBackupId: fullResult.backupId,
      });

      await sourceBackend.write('data.dat', textToBytes('modified2'));
      await backupManager.createBackup({
        type: 'incremental',
        baseBackupId: fullResult.backupId,
      });

      const chain = await backupManager.getIncrementalChain(fullResult.backupId);

      expect(chain.length).toBe(2);
      // Should be sorted oldest first
      expect((chain[0].completedAt ?? 0) < (chain[1].completedAt ?? 0)).toBe(true);
    });
  });

  describe('Backup Validation', () => {
    it('should validate a valid backup', async () => {
      await sourceBackend.write('data.dat', textToBytes('test data'));

      const result = await backupManager.createBackup();

      const validation = await backupManager.validateBackup(result.backupId);

      expect(validation.valid).toBe(true);
      expect(validation.checksumValid).toBe(true);
      expect(validation.manifestValid).toBe(true);
      expect(validation.filesComplete).toBe(true);
      expect(validation.missingFiles.length).toBe(0);
      expect(validation.corruptedFiles.length).toBe(0);
    });

    it('should detect missing files', async () => {
      await sourceBackend.write('data.dat', textToBytes('test data'));

      const result = await backupManager.createBackup();

      // Delete a file from the backup
      await targetBackend.delete(`_backups/${result.backupId}/data/data.dat`);

      const validation = await backupManager.validateBackup(result.backupId);

      expect(validation.valid).toBe(false);
      expect(validation.filesComplete).toBe(false);
      expect(validation.missingFiles).toContain('data.dat');
    });

    it('should detect corrupted files', async () => {
      await sourceBackend.write('data.dat', textToBytes('test data'));

      const result = await backupManager.createBackup();

      // Corrupt a file in the backup
      await targetBackend.write(
        `_backups/${result.backupId}/data/data.dat`,
        textToBytes('corrupted data')
      );

      const validation = await backupManager.validateBackup(result.backupId);

      expect(validation.valid).toBe(false);
      expect(validation.checksumValid).toBe(false);
      expect(validation.corruptedFiles).toContain('data.dat');
    });

    it('should detect missing manifest', async () => {
      const validation = await backupManager.validateBackup('nonexistent');

      expect(validation.valid).toBe(false);
      expect(validation.manifestValid).toBe(false);
    });
  });
});

// =============================================================================
// Restore Manager Tests
// =============================================================================

describe('RestoreManager', () => {
  let sourceBackend: MemoryFSXBackend;
  let targetBackend: MemoryFSXBackend;
  let backupBackend: MemoryFSXBackend;
  let walBackend: MemoryFSXBackend;
  let walWriter: WALWriter;
  let walReader: WALReader;
  let backupManager: BackupManager;
  let restoreManager: RestoreManager;
  let currentLSN: LSN;
  let appliedEntries: WALEntry[];

  beforeEach(async () => {
    sourceBackend = createMemoryBackend();
    targetBackend = createMemoryBackend();
    backupBackend = createMemoryBackend();
    walBackend = createMemoryBackend();

    currentLSN = createLSN(0n);
    appliedEntries = [];

    walWriter = createWALWriter(walBackend, 0n);
    walReader = createWALReader(walBackend);

    backupManager = createBackupManager({
      sourceBackend,
      targetBackend: backupBackend,
      walReader,
      getCurrentLSN: () => currentLSN,
      databaseId: 'test-db',
      config: {
        dataPrefix: '',
        walPrefix: '_wal/segments/',
      },
    });

    restoreManager = createRestoreManager({
      sourceBackend: backupBackend,
      targetBackend,
      applyWALEntry: async (entry) => {
        appliedEntries.push(entry);
      },
      setCurrentLSN: (lsn) => {
        currentLSN = lsn;
      },
    });
  });

  afterEach(() => {
    sourceBackend.clear();
    targetBackend.clear();
    backupBackend.clear();
    walBackend.clear();
  });

  describe('Full Restore', () => {
    it('should restore all files from a backup', async () => {
      // Create test data and backup
      await sourceBackend.write('table1.dat', textToBytes('table1 data'));
      await sourceBackend.write('table2.dat', textToBytes('table2 data'));

      const backupResult = await backupManager.createBackup();

      // Clear target
      targetBackend.clear();

      // Restore
      const restoreResult = await restoreManager.restore({
        backupId: backupResult.backupId,
      });

      expect(restoreResult.success).toBe(true);
      expect(restoreResult.filesRestored).toBe(2);

      // Verify restored data
      const table1 = await targetBackend.read('table1.dat');
      const table2 = await targetBackend.read('table2.dat');

      expect(bytesToText(table1!)).toBe('table1 data');
      expect(bytesToText(table2!)).toBe('table2 data');
    });

    it('should report progress during restore', async () => {
      await sourceBackend.write('data.dat', generateTestData(1000));

      const backupResult = await backupManager.createBackup();
      targetBackend.clear();

      const progressUpdates: RestoreProgress[] = [];

      const restoreResult = await restoreManager.restore({
        backupId: backupResult.backupId,
        onProgress: (progress) => {
          progressUpdates.push({ ...progress });
        },
      });

      expect(restoreResult.success).toBe(true);
      expect(progressUpdates.length).toBeGreaterThan(0);

      // Should have restoring_data phase
      expect(progressUpdates.some((p) => p.phase === 'restoring_data')).toBe(true);
    });

    it('should verify backup before restore by default', async () => {
      await sourceBackend.write('data.dat', textToBytes('data'));

      const backupResult = await backupManager.createBackup();

      // Corrupt the backup
      await backupBackend.write(
        `_backups/${backupResult.backupId}/data/data.dat`,
        textToBytes('corrupted')
      );

      const restoreResult = await restoreManager.restore({
        backupId: backupResult.backupId,
      });

      expect(restoreResult.success).toBe(false);
      expect(restoreResult.error).toContain('corrupted');
    });

    it('should skip verification when requested', async () => {
      await sourceBackend.write('data.dat', textToBytes('data'));

      const backupResult = await backupManager.createBackup();
      targetBackend.clear();

      const restoreResult = await restoreManager.restore({
        backupId: backupResult.backupId,
        verifyBeforeRestore: false,
      });

      expect(restoreResult.success).toBe(true);
    });
  });

  describe('Incremental Restore', () => {
    it('should restore from incremental backup chain', async () => {
      // Create initial data and full backup
      await sourceBackend.write('file1.dat', textToBytes('original1'));
      await sourceBackend.write('file2.dat', textToBytes('original2'));

      const fullResult = await backupManager.createBackup({ type: 'full' });

      // Modify and create incremental
      await sourceBackend.write('file1.dat', textToBytes('modified1'));
      await sourceBackend.write('file3.dat', textToBytes('new file'));

      const incrementalResult = await backupManager.createBackup({
        type: 'incremental',
        baseBackupId: fullResult.backupId,
      });

      // Clear target
      targetBackend.clear();

      // Restore from incremental
      const restoreResult = await restoreManager.restore({
        backupId: incrementalResult.backupId,
      });

      expect(restoreResult.success).toBe(true);

      // Verify all data is present
      expect(await targetBackend.exists('file1.dat')).toBe(true);
      expect(await targetBackend.exists('file2.dat')).toBe(true);
      expect(await targetBackend.exists('file3.dat')).toBe(true);

      // Verify modified data
      const file1 = await targetBackend.read('file1.dat');
      expect(bytesToText(file1!)).toBe('modified1');
    });
  });

  describe('Point-in-Time Recovery', () => {
    it('should find backup chain for target LSN', async () => {
      await sourceBackend.write('data.dat', textToBytes('data'));
      currentLSN = createLSN(100n);

      await backupManager.createBackup({ type: 'full' });

      const chain = await restoreManager.findBackupChainForLSN(createLSN(50n));

      expect(chain.length).toBeGreaterThan(0);
    });

    it('should find backup chain for target timestamp', async () => {
      await sourceBackend.write('data.dat', textToBytes('data'));

      await backupManager.createBackup({ type: 'full' });

      const futureTimestamp = Date.now() + 10000;
      const chain = await restoreManager.findBackupChainForTimestamp(futureTimestamp);

      expect(chain.length).toBeGreaterThan(0);
    });

    it('should throw when target LSN is unreachable', async () => {
      await sourceBackend.write('data.dat', textToBytes('data'));
      currentLSN = createLSN(10n);

      await backupManager.createBackup({ type: 'full' });

      // Try to find chain for LSN way beyond what we have
      await expect(
        restoreManager.findBackupChainForLSN(createLSN(10000n))
      ).rejects.toThrow(BackupError);
    });

    it('should throw when target timestamp is unreachable', async () => {
      await sourceBackend.write('data.dat', textToBytes('data'));

      await backupManager.createBackup({ type: 'full' });

      // Try to find chain for timestamp in the past
      const pastTimestamp = Date.now() - 1000000;

      await expect(
        restoreManager.findBackupChainForTimestamp(pastTimestamp)
      ).rejects.toThrow(BackupError);
    });
  });

  describe('Restore Validation', () => {
    it('should validate restore can be performed', async () => {
      await sourceBackend.write('data.dat', textToBytes('data'));

      const backupResult = await backupManager.createBackup();

      const validation = await restoreManager.validateRestore({
        backupId: backupResult.backupId,
      });

      expect(validation.valid).toBe(true);
    });

    it('should detect invalid restore target', async () => {
      const validation = await restoreManager.validateRestore({
        backupId: 'nonexistent',
      });

      expect(validation.valid).toBe(false);
    });
  });
});

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('BackupError', () => {
  it('should create error with correct properties', () => {
    const error = new BackupError(
      BackupErrorCode.BACKUP_NOT_FOUND,
      'Backup not found',
      { backupId: 'test-backup' }
    );

    expect(error.code).toBe(BackupErrorCode.BACKUP_NOT_FOUND);
    expect(error.message).toBe('Backup not found');
    expect(error.backupId).toBe('test-backup');
    expect(error.name).toBe('BackupError');
  });

  it('should provide user-friendly messages', () => {
    const error = new BackupError(
      BackupErrorCode.BACKUP_CORRUPTED,
      'Internal error'
    );

    expect(error.toUserMessage()).toBe('The backup is corrupted and cannot be used.');
  });

  it('should indicate retryable errors', () => {
    const storageError = new BackupError(
      BackupErrorCode.STORAGE_ERROR,
      'Storage error'
    );
    const notFoundError = new BackupError(
      BackupErrorCode.BACKUP_NOT_FOUND,
      'Not found'
    );

    expect(storageError.isRetryable()).toBe(true);
    expect(notFoundError.isRetryable()).toBe(false);
  });

  it('should include recovery hints', () => {
    const error = new BackupError(
      BackupErrorCode.MISSING_BASE_BACKUP,
      'Missing base'
    );

    expect(error.recoveryHint).toContain('base backup');
  });

  it('should serialize to JSON correctly', () => {
    const error = new BackupError(
      BackupErrorCode.CHECKSUM_MISMATCH,
      'Checksum mismatch',
      { backupId: 'test-123' }
    );

    const json = error.toJSON();

    expect(json.code).toBe(BackupErrorCode.CHECKSUM_MISMATCH);
    expect(json.message).toBe('Checksum mismatch');
    expect(json.context?.metadata?.backupId).toBe('test-123');
  });

  it('should deserialize from JSON correctly', () => {
    const original = new BackupError(
      BackupErrorCode.RESTORE_FAILED,
      'Restore failed',
      { backupId: 'restore-test' }
    );

    const json = original.toJSON();
    const restored = BackupError.fromJSON(json);

    expect(restored.code).toBe(original.code);
    expect(restored.message).toBe(original.message);
  });
});

// =============================================================================
// R2 Backend Tests
// =============================================================================

describe('BackupManager with R2Backend', () => {
  let sourceBackend: MemoryFSXBackend;
  let r2Backend: ReturnType<typeof createR2Backend>;
  let backupManager: BackupManager;
  let currentLSN: LSN;

  beforeEach(() => {
    sourceBackend = createMemoryBackend();
    r2Backend = createR2Backend(env.TEST_R2_BUCKET as R2BucketLike, {
      keyPrefix: 'backup-test',
    });
    currentLSN = createLSN(0n);

    backupManager = createBackupManager({
      sourceBackend,
      targetBackend: r2Backend,
      getCurrentLSN: () => currentLSN,
      databaseId: 'r2-test-db',
      config: {
        dataPrefix: '',
        walPrefix: '_wal/segments/',
      },
    });
  });

  afterEach(async () => {
    sourceBackend.clear();
    // Clean up R2
    const files = await r2Backend.list('');
    for (const file of files) {
      await r2Backend.delete(file);
    }
  });

  it('should create backup to R2', async () => {
    await sourceBackend.write('r2test.dat', textToBytes('r2 backup test'));

    const result = await backupManager.createBackup({
      description: 'R2 backup test',
    });

    expect(result.success).toBe(true);
    expect(result.metadata.fileCount).toBe(1);
  });

  it('should list backups from R2', async () => {
    await sourceBackend.write('data.dat', textToBytes('test'));

    await backupManager.createBackup({ description: 'R2 Backup 1' });
    await backupManager.createBackup({ description: 'R2 Backup 2' });

    const backups = await backupManager.listBackups();

    expect(backups.length).toBe(2);
  });

  it('should validate backup in R2', async () => {
    await sourceBackend.write('validate.dat', textToBytes('validate test'));

    const result = await backupManager.createBackup();

    const validation = await backupManager.validateBackup(result.backupId);

    expect(validation.valid).toBe(true);
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge Cases', () => {
  let sourceBackend: MemoryFSXBackend;
  let targetBackend: MemoryFSXBackend;
  let backupManager: BackupManager;

  beforeEach(() => {
    sourceBackend = createMemoryBackend();
    targetBackend = createMemoryBackend();

    backupManager = createBackupManager({
      sourceBackend,
      targetBackend,
      getCurrentLSN: () => createLSN(0n),
      databaseId: 'edge-test-db',
    });
  });

  afterEach(() => {
    sourceBackend.clear();
    targetBackend.clear();
  });

  it('should handle files with special characters in names', async () => {
    await sourceBackend.write('file with spaces.dat', textToBytes('data1'));
    await sourceBackend.write('file-with-dashes.dat', textToBytes('data2'));
    await sourceBackend.write('file.multiple.dots.dat', textToBytes('data3'));

    const result = await backupManager.createBackup();

    expect(result.success).toBe(true);
    expect(result.metadata.fileCount).toBe(3);

    const validation = await backupManager.validateBackup(result.backupId);
    expect(validation.valid).toBe(true);
  });

  it('should handle binary data correctly', async () => {
    // Create data with all possible byte values
    const binaryData = new Uint8Array(256);
    for (let i = 0; i < 256; i++) {
      binaryData[i] = i;
    }

    await sourceBackend.write('binary.dat', binaryData);

    const result = await backupManager.createBackup();

    expect(result.success).toBe(true);

    const validation = await backupManager.validateBackup(result.backupId);
    expect(validation.valid).toBe(true);
  });

  it('should handle large number of files', async () => {
    // Create many small files
    for (let i = 0; i < 100; i++) {
      await sourceBackend.write(`file_${i.toString().padStart(3, '0')}.dat`, textToBytes(`data ${i}`));
    }

    const result = await backupManager.createBackup();

    expect(result.success).toBe(true);
    expect(result.metadata.fileCount).toBe(100);
  });

  it('should handle deeply nested paths', async () => {
    await sourceBackend.write('level1/level2/level3/deep.dat', textToBytes('deep data'));

    const result = await backupManager.createBackup();

    expect(result.success).toBe(true);

    const manifest = await backupManager.getBackup(result.backupId);
    expect(manifest?.files.some((f) => f.path.includes('level1/level2/level3/'))).toBe(true);
  });
});
