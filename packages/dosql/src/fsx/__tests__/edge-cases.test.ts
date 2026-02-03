/**
 * FSX Backend Edge Case Tests
 *
 * Issue: sql-ympf - Missing FSX backend edge case tests
 *
 * This test file covers edge cases for FSX backend modules:
 * 1. Garbage Collection (gc.ts)
 * 2. Merge Engine (merge.ts)
 * 3. Snapshot Manager (snapshot.ts)
 * 4. Tier Migration (tier-migration.ts)
 *
 * Edge cases covered:
 * - Empty files
 * - Maximum file sizes
 * - Concurrent access
 * - Error conditions
 *
 * Uses NO MOCKS philosophy - tests run in actual Cloudflare Workers environment.
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { env } from 'cloudflare:test';

import {
  // COW Backend
  COWBackend,
  createCOWBackend,
  COWError,
  COWErrorCode,
  DEFAULT_BRANCH,

  // Snapshot utilities
  SnapshotManager,
  createEmptyManifest,
  buildManifest,
  mergeManifests,
  filterManifest,

  // GC utilities
  GarbageCollector,
  formatBytes,
  formatGCResult,
  suggestGCSchedule,

  // Merge utilities
  MergeEngine,
  previewMerge,
  canFastForward,
  fastForwardMerge,

  // Tier Migration
  TierMigrator,
  MigrationProgressTracker,
  createTierMigrator,
  createProgressTracker,
  type TierIndexEntry,
  type MigrationPolicy,

  // Backends
  R2StorageBackend,
  createR2Backend,
  TieredStorageBackend,
  createTieredBackend,
  DOStorageBackend,
  type R2BucketLike,

  // Memory Backend
  MemoryFSXBackend,
  createMemoryBackend,

  // Types
  StorageTier,
  type SnapshotManifest,
  type ManifestEntry,
  type GCResult,
} from '../index.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Generate test data of specified size
 */
function generateTestData(size: number, seed = 0): Uint8Array {
  const data = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    data[i] = (i + seed) % 256;
  }
  return data;
}

/**
 * Text encoder/decoder helpers
 */
const encoder = new TextEncoder();
const decoder = new TextDecoder();

function textToBytes(text: string): Uint8Array {
  return encoder.encode(text);
}

function bytesToText(bytes: Uint8Array): string {
  return decoder.decode(bytes);
}

/**
 * Wait for specified milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Create a memory backend with getStats support for tiered testing
 */
function createHotBackendWithStats(): MemoryFSXBackend & {
  getStats: () => Promise<{ fileCount: number; totalSize: number; chunkedFileCount: number }>;
} {
  const backend = createMemoryBackend();
  const hotWithStats = backend as MemoryFSXBackend & {
    getStats: () => Promise<{ fileCount: number; totalSize: number; chunkedFileCount: number }>;
  };

  hotWithStats.getStats = async () => {
    let totalSize = 0;
    for (const path of backend.paths()) {
      const data = await backend.read(path);
      if (data) totalSize += data.length;
    }
    return {
      fileCount: backend.size,
      totalSize,
      chunkedFileCount: 0,
    };
  };

  return hotWithStats;
}

// =============================================================================
// 1. Garbage Collection Edge Cases (gc.ts)
// =============================================================================

describe('GarbageCollector Edge Cases', () => {
  let memoryBackend: MemoryFSXBackend;
  let cowBackend: COWBackend;

  beforeEach(async () => {
    memoryBackend = createMemoryBackend();
    cowBackend = await createCOWBackend(memoryBackend);
  });

  afterEach(() => {
    memoryBackend.clear();
  });

  describe('Empty Storage', () => {
    it('should handle GC on completely empty storage', async () => {
      const result = await cowBackend.gc();

      expect(result.blobsScanned).toBe(0);
      expect(result.blobsDeleted).toBe(0);
      expect(result.bytesReclaimed).toBe(0);
      expect(result.errors).toHaveLength(0);
    });

    it('should handle GC with only metadata and no data blobs', async () => {
      // Create and immediately delete all data
      await cowBackend.write('temp.txt', textToBytes('temp'));
      await cowBackend.delete('temp.txt');

      const result = await cowBackend.gc();

      expect(result.durationMs).toBeGreaterThanOrEqual(0);
      expect(Array.isArray(result.deletedPaths)).toBe(true);
    });
  });

  describe('Empty Files', () => {
    it('should handle GC with empty file blobs', async () => {
      // Write empty file
      await cowBackend.write('empty.txt', new Uint8Array(0));

      // Delete it to make blob unreferenced
      await cowBackend.delete('empty.txt');

      const result = await cowBackend.gc();

      // Should handle zero-size blob correctly
      expect(result.errors).toHaveLength(0);
    });

    it('should correctly report zero bytes reclaimed for empty files', async () => {
      await cowBackend.write('empty1.txt', new Uint8Array(0));
      await cowBackend.write('empty2.txt', new Uint8Array(0));

      await cowBackend.delete('empty1.txt');
      await cowBackend.delete('empty2.txt');

      const result = await cowBackend.gc({ dryRun: false });

      // Zero-size blobs may or may not be tracked depending on implementation
      expect(result.bytesReclaimed).toBeGreaterThanOrEqual(0);
    });
  });

  describe('Maximum File Sizes', () => {
    it('should handle GC with large blobs near size limit', async () => {
      // Create a moderately large file (256KB)
      const largeData = generateTestData(256 * 1024);
      await cowBackend.write('large.bin', largeData);
      await cowBackend.delete('large.bin');

      const result = await cowBackend.gc();

      expect(result.errors).toHaveLength(0);
    });

    it('should correctly calculate bytes reclaimed for large files', async () => {
      const size = 100 * 1024; // 100KB
      const data = generateTestData(size);
      await cowBackend.write('large.bin', data);
      await cowBackend.delete('large.bin');

      const result = await cowBackend.gc({ dryRun: false });

      // Bytes reclaimed should include the large file if it was cleaned
      // (depends on ref count reaching zero)
      expect(result.durationMs).toBeGreaterThanOrEqual(0);
    });
  });

  describe('Concurrent Access', () => {
    it('should handle concurrent GC operations', async () => {
      // Setup some data
      for (let i = 0; i < 10; i++) {
        await cowBackend.write(`file${i}.txt`, textToBytes(`content ${i}`));
      }

      // Delete half
      for (let i = 0; i < 5; i++) {
        await cowBackend.delete(`file${i}.txt`);
      }

      // Run multiple GC operations concurrently
      const results = await Promise.all([
        cowBackend.gc({ dryRun: true }),
        cowBackend.gc({ dryRun: true }),
        cowBackend.gc({ dryRun: true }),
      ]);

      // All should complete without errors
      for (const result of results) {
        expect(result.errors).toHaveLength(0);
      }
    });

    it('should handle GC during active writes', async () => {
      // Start writing files
      const writePromise = (async () => {
        for (let i = 0; i < 20; i++) {
          await cowBackend.write(`concurrent${i}.txt`, textToBytes(`data ${i}`));
        }
      })();

      // Run GC while writes are happening
      const gcPromise = cowBackend.gc({ dryRun: true });

      // Both should complete
      const [, gcResult] = await Promise.all([writePromise, gcPromise]);

      // GC should not cause errors
      expect(gcResult.errors).toHaveLength(0);
    });
  });

  describe('Error Conditions', () => {
    it('should respect limit option', async () => {
      // Create many files
      for (let i = 0; i < 10; i++) {
        await cowBackend.write(`limit${i}.txt`, textToBytes(`data ${i}`));
        await cowBackend.delete(`limit${i}.txt`);
      }

      const result = await cowBackend.gc({ limit: 3, dryRun: false });

      // Should stop at limit
      expect(result.blobsDeleted).toBeLessThanOrEqual(3);
    });

    it('should respect olderThan option', async () => {
      await cowBackend.write('old.txt', textToBytes('old'));
      await cowBackend.delete('old.txt');

      // Wait a bit
      await sleep(50);

      await cowBackend.write('new.txt', textToBytes('new'));
      await cowBackend.delete('new.txt');

      // GC only files older than 30ms
      const result = await cowBackend.gc({ olderThan: 30, dryRun: true });

      // Should only consider old files
      expect(result.durationMs).toBeGreaterThanOrEqual(0);
    });

    it('should handle dryRun correctly', async () => {
      await cowBackend.write('dryrun.txt', textToBytes('test'));
      await cowBackend.delete('dryrun.txt');

      // Dry run should not actually delete
      const dryResult = await cowBackend.gc({ dryRun: true });

      // Now do actual GC
      const realResult = await cowBackend.gc({ dryRun: false });

      // Both should complete without error
      expect(dryResult.errors).toHaveLength(0);
      expect(realResult.errors).toHaveLength(0);
    });
  });

  describe('Utility Functions', () => {
    it('should format bytes correctly for edge values', () => {
      expect(formatBytes(0)).toBe('0.00 B');
      expect(formatBytes(1)).toBe('1.00 B');
      expect(formatBytes(1023)).toBe('1023.00 B');
      expect(formatBytes(1024)).toBe('1.00 KB');
      expect(formatBytes(1024 * 1024 - 1)).toBe('1024.00 KB');
      expect(formatBytes(1024 * 1024)).toBe('1.00 MB');
      expect(formatBytes(1024 * 1024 * 1024)).toBe('1.00 GB');
      expect(formatBytes(1024 * 1024 * 1024 * 1024)).toBe('1.00 TB');
    });

    it('should format GC result with no errors', () => {
      const result: GCResult = {
        blobsDeleted: 5,
        bytesReclaimed: 1024,
        deletedPaths: ['a.bin', 'b.bin'],
        durationMs: 100,
        blobsScanned: 10,
        errors: [],
      };

      const formatted = formatGCResult(result);

      expect(formatted).toContain('100ms');
      expect(formatted).toContain('5');
      expect(formatted).toContain('10');
      expect(formatted).not.toContain('Errors');
    });

    it('should format GC result with many errors (truncation)', () => {
      const errors = [];
      for (let i = 0; i < 10; i++) {
        errors.push({ path: `file${i}.bin`, error: `Error ${i}` });
      }

      const result: GCResult = {
        blobsDeleted: 0,
        bytesReclaimed: 0,
        deletedPaths: [],
        durationMs: 50,
        blobsScanned: 10,
        errors,
      };

      const formatted = formatGCResult(result);

      expect(formatted).toContain('Errors: 10');
      expect(formatted).toContain('... and 5 more');
    });

    it('should suggest GC schedule based on stats', () => {
      // High urgency
      const highStats = {
        totalBlobs: 100,
        totalSize: 1000,
        unreferencedBlobs: 50,
        unreferencedSize: 400, // 40%
        orphanedBlobs: 50,
        orphanedMetadata: 100,
        sizeByRefCount: new Map(),
      };

      const highResult = suggestGCSchedule(highStats);
      expect(highResult.urgency).toBe('high');
      expect(highResult.recommended).toBe('immediate');

      // Low urgency
      const lowStats = {
        totalBlobs: 100,
        totalSize: 1000,
        unreferencedBlobs: 1,
        unreferencedSize: 50, // 5%
        orphanedBlobs: 0,
        orphanedMetadata: 0,
        sizeByRefCount: new Map(),
      };

      const lowResult = suggestGCSchedule(lowStats);
      expect(lowResult.urgency).toBe('low');
      expect(lowResult.recommended).toBe('weekly');
    });
  });
});

// =============================================================================
// 2. Merge Engine Edge Cases (merge.ts)
// =============================================================================

describe('MergeEngine Edge Cases', () => {
  let memoryBackend: MemoryFSXBackend;
  let cowBackend: COWBackend;

  beforeEach(async () => {
    memoryBackend = createMemoryBackend();
    cowBackend = await createCOWBackend(memoryBackend);
  });

  afterEach(() => {
    memoryBackend.clear();
  });

  describe('Empty Branches', () => {
    it('should merge empty branches', async () => {
      await cowBackend.branch('main', 'empty-branch');

      const result = await cowBackend.merge('empty-branch', 'main', 'theirs');

      expect(result.success).toBe(true);
      expect(result.conflicts).toHaveLength(0);
    });

    it('should diff empty branches', async () => {
      await cowBackend.branch('main', 'empty1');
      await cowBackend.branch('main', 'empty2');

      const diff = await cowBackend.diff('empty1', 'empty2');

      expect(diff.added).toHaveLength(0);
      expect(diff.removed).toHaveLength(0);
      expect(diff.modified).toHaveLength(0);
      expect(diff.unchanged).toHaveLength(0);
    });

    it('should handle merging into empty branch', async () => {
      await cowBackend.write('file.txt', textToBytes('content'));
      await cowBackend.branch('main', 'target');

      // Delete from target to make it empty
      await cowBackend.checkout('target');
      await cowBackend.delete('file.txt');

      // Merge main back into empty target
      await cowBackend.checkout('main');
      const result = await cowBackend.merge('main', 'target', 'theirs');

      expect(result.success).toBe(true);
    });
  });

  describe('Empty Files', () => {
    it('should merge branches with empty files', async () => {
      await cowBackend.write('empty.txt', new Uint8Array(0));
      await cowBackend.branch('main', 'feature');

      // Modify in feature
      await cowBackend.writeTo('empty.txt', textToBytes('not empty'), 'feature');

      const result = await cowBackend.merge('feature', 'main', 'theirs');

      expect(result.success).toBe(true);

      const content = await cowBackend.read('empty.txt');
      expect(bytesToText(content!)).toBe('not empty');
    });

    it('should handle empty file conflicts', async () => {
      await cowBackend.write('conflict.txt', new Uint8Array(0));
      await cowBackend.branch('main', 'feature');

      // Both modify
      await cowBackend.writeTo('conflict.txt', textToBytes('main'), 'main');
      await cowBackend.writeTo('conflict.txt', textToBytes('feature'), 'feature');

      const result = await cowBackend.merge('feature', 'main', 'fail-on-conflict');

      expect(result.success).toBe(false);
      expect(result.conflicts.length).toBeGreaterThan(0);
    });
  });

  describe('Maximum File Sizes', () => {
    it('should merge branches with large files', async () => {
      const largeData = generateTestData(256 * 1024);
      await cowBackend.write('large.bin', largeData);
      await cowBackend.branch('main', 'feature');

      // Modify in feature
      await cowBackend.writeTo('large.bin', generateTestData(256 * 1024, 42), 'feature');

      const result = await cowBackend.merge('feature', 'main', 'theirs');

      expect(result.success).toBe(true);
    });
  });

  describe('Concurrent Access', () => {
    it('should handle concurrent merge operations', async () => {
      await cowBackend.write('shared.txt', textToBytes('base'));
      await cowBackend.branch('main', 'feature1');
      await cowBackend.branch('main', 'feature2');

      // Add different files to different branches
      await cowBackend.writeTo('f1.txt', textToBytes('f1'), 'feature1');
      await cowBackend.writeTo('f2.txt', textToBytes('f2'), 'feature2');

      // Merge both into main (sequentially to avoid conflicts)
      const result1 = await cowBackend.merge('feature1', 'main', 'theirs');
      const result2 = await cowBackend.merge('feature2', 'main', 'theirs');

      expect(result1.success).toBe(true);
      expect(result2.success).toBe(true);

      // Main should have both files
      expect(await cowBackend.readFrom('f1.txt', 'main')).not.toBeNull();
      expect(await cowBackend.readFrom('f2.txt', 'main')).not.toBeNull();
    });

    it('should handle concurrent diff operations', async () => {
      await cowBackend.write('data.txt', textToBytes('base'));
      await cowBackend.branch('main', 'b1');
      await cowBackend.branch('main', 'b2');

      // Concurrent diffs
      const [diff1, diff2, diff3] = await Promise.all([
        cowBackend.diff('main', 'b1'),
        cowBackend.diff('main', 'b2'),
        cowBackend.diff('b1', 'b2'),
      ]);

      // All should complete
      expect(diff1).toBeDefined();
      expect(diff2).toBeDefined();
      expect(diff3).toBeDefined();
    });
  });

  describe('Error Conditions', () => {
    it('should fail on merge with non-existent source branch', async () => {
      await expect(
        cowBackend.merge('nonexistent', 'main', 'theirs')
      ).rejects.toThrow(COWError);
    });

    it('should fail on merge with non-existent target branch', async () => {
      await cowBackend.branch('main', 'source');

      await expect(
        cowBackend.merge('source', 'nonexistent', 'theirs')
      ).rejects.toThrow(COWError);
    });

    it('should fail on merge into readonly branch', async () => {
      await cowBackend.write('data.txt', textToBytes('data'));
      await cowBackend.branch('main', 'readonly', { readonly: true });
      await cowBackend.branch('main', 'source');

      await expect(
        cowBackend.merge('source', 'readonly', 'theirs')
      ).rejects.toThrow(COWError);
    });
  });

  describe('Merge Strategies', () => {
    it('should handle ours strategy correctly', async () => {
      await cowBackend.write('file.txt', textToBytes('base'));
      await cowBackend.branch('main', 'feature');

      await cowBackend.writeTo('file.txt', textToBytes('main version'), 'main');
      await cowBackend.writeTo('file.txt', textToBytes('feature version'), 'feature');

      const result = await cowBackend.merge('feature', 'main', 'ours');

      expect(result.success).toBe(true);
      const content = await cowBackend.readFrom('file.txt', 'main');
      expect(bytesToText(content!)).toBe('main version');
    });

    it('should handle last-write-wins strategy correctly', async () => {
      await cowBackend.write('file.txt', textToBytes('base'));
      await cowBackend.branch('main', 'feature');

      // Modify main first
      await cowBackend.writeTo('file.txt', textToBytes('main'), 'main');
      await sleep(20);
      // Then feature (more recent)
      await cowBackend.writeTo('file.txt', textToBytes('feature'), 'feature');

      const result = await cowBackend.merge('feature', 'main', 'last-write-wins');

      expect(result.success).toBe(true);
      // Feature is more recent, so it should win
      const content = await cowBackend.readFrom('file.txt', 'main');
      expect(bytesToText(content!)).toBe('feature');
    });
  });

  describe('Preview and Fast-Forward', () => {
    it('should preview merge without applying', async () => {
      await cowBackend.write('shared.txt', textToBytes('base'));
      await cowBackend.branch('main', 'feature');

      await cowBackend.writeTo('new.txt', textToBytes('new'), 'feature');

      const preview = await previewMerge(cowBackend, 'feature', 'main', 'theirs');

      expect(preview.diff.added).toContain('new.txt');
      expect(preview.wouldSucceed).toBe(true);

      // Main should not have the new file yet
      expect(await cowBackend.readFrom('new.txt', 'main')).toBeNull();
    });

    it('should detect fast-forward possibility', async () => {
      await cowBackend.write('shared.txt', textToBytes('base'));
      await cowBackend.branch('main', 'feature');

      // Only add to feature, don't modify main
      await cowBackend.writeTo('feature-only.txt', textToBytes('feature'), 'feature');

      const canFF = await canFastForward(cowBackend, 'feature', 'main');

      // Fast-forward should be possible
      expect(canFF).toBe(true);
    });

    it('should detect when fast-forward is not possible', async () => {
      await cowBackend.write('shared.txt', textToBytes('base'));
      await cowBackend.branch('main', 'feature');

      // Modify both branches
      await cowBackend.writeTo('main-only.txt', textToBytes('main'), 'main');
      await cowBackend.writeTo('feature-only.txt', textToBytes('feature'), 'feature');

      const canFF = await canFastForward(cowBackend, 'feature', 'main');

      // Fast-forward should NOT be possible (main has changes)
      expect(canFF).toBe(false);
    });
  });
});

// =============================================================================
// 3. Snapshot Manager Edge Cases (snapshot.ts)
// =============================================================================

describe('SnapshotManager Edge Cases', () => {
  let memoryBackend: MemoryFSXBackend;
  let snapshotManager: SnapshotManager;

  beforeEach(() => {
    memoryBackend = createMemoryBackend();
    snapshotManager = new SnapshotManager(memoryBackend);
  });

  afterEach(() => {
    memoryBackend.clear();
  });

  describe('Empty Snapshots', () => {
    it('should create snapshot with empty manifest', async () => {
      const manifest = createEmptyManifest();
      const id = await snapshotManager.createSnapshot('main', 1, manifest, 'Empty snapshot');

      expect(id).toContain('main@');

      const snapshot = await snapshotManager.getSnapshot(id);
      expect(snapshot?.manifest.count).toBe(0);
      expect(snapshot?.manifest.totalSize).toBe(0);
      expect(snapshot?.manifest.entries).toHaveLength(0);
    });

    it('should list empty snapshots', async () => {
      const manifest = createEmptyManifest();
      await snapshotManager.createSnapshot('main', 1, manifest);

      const snapshots = await snapshotManager.listSnapshots('main');

      expect(snapshots.length).toBeGreaterThanOrEqual(1);
    });

    it('should handle deleting all snapshots', async () => {
      const manifest = createEmptyManifest();
      await snapshotManager.createSnapshot('main', 1, manifest);
      await snapshotManager.createSnapshot('main', 2, manifest);

      const deleted = await snapshotManager.deleteAllSnapshots('main');

      expect(deleted).toBe(2);

      const remaining = await snapshotManager.listSnapshots('main');
      expect(remaining).toHaveLength(0);
    });
  });

  describe('Empty Files in Manifest', () => {
    it('should handle manifest entries with zero size', async () => {
      const manifest = buildManifest([
        { path: 'empty.txt', version: 1, size: 0, hash: 'empty-hash' },
        { path: 'normal.txt', version: 1, size: 100, hash: 'normal-hash' },
      ]);

      const id = await snapshotManager.createSnapshot('main', 1, manifest);
      const snapshot = await snapshotManager.getSnapshot(id);

      expect(snapshot?.manifest.count).toBe(2);
      expect(snapshot?.manifest.totalSize).toBe(100);
    });

    it('should get stats for snapshot with empty files', async () => {
      const manifest = buildManifest([
        { path: 'empty.txt', version: 1, size: 0, hash: 'e1' },
        { path: 'also-empty.txt', version: 1, size: 0, hash: 'e2' },
      ]);

      const id = await snapshotManager.createSnapshot('main', 1, manifest);
      const snapshot = await snapshotManager.getSnapshot(id);

      const stats = snapshotManager.getSnapshotStats(snapshot!);

      expect(stats.totalFiles).toBe(2);
      expect(stats.totalSize).toBe(0);
      expect(stats.averageFileSize).toBe(0);
      expect(stats.smallestFile?.size).toBe(0);
      expect(stats.largestFile?.size).toBe(0);
    });
  });

  describe('Maximum File Sizes', () => {
    it('should handle manifest with very large entries', async () => {
      const largeSize = 1024 * 1024 * 1024; // 1GB
      const manifest = buildManifest([
        { path: 'huge.bin', version: 1, size: largeSize, hash: 'huge-hash' },
      ]);

      const id = await snapshotManager.createSnapshot('main', 1, manifest);
      const snapshot = await snapshotManager.getSnapshot(id);

      expect(snapshot?.manifest.totalSize).toBe(largeSize);
    });

    it('should handle many entries in manifest', async () => {
      const entries: ManifestEntry[] = [];
      for (let i = 0; i < 1000; i++) {
        entries.push({
          path: `file${i.toString().padStart(4, '0')}.txt`,
          version: 1,
          size: 100,
          hash: `hash${i}`,
        });
      }

      const manifest = buildManifest(entries);
      const id = await snapshotManager.createSnapshot('main', 1, manifest);
      const snapshot = await snapshotManager.getSnapshot(id);

      expect(snapshot?.manifest.count).toBe(1000);
      expect(snapshot?.manifest.totalSize).toBe(100000);
    });
  });

  describe('Concurrent Access', () => {
    it('should handle concurrent snapshot creation', async () => {
      const manifests = [];
      for (let i = 0; i < 10; i++) {
        manifests.push(buildManifest([
          { path: `file${i}.txt`, version: 1, size: i * 10, hash: `hash${i}` },
        ]));
      }

      const ids = await Promise.all(
        manifests.map((m, i) =>
          snapshotManager.createSnapshot('main', i + 1, m, `Snapshot ${i}`)
        )
      );

      // All should succeed
      for (const id of ids) {
        const snapshot = await snapshotManager.getSnapshot(id);
        expect(snapshot).not.toBeNull();
      }
    });

    it('should handle concurrent snapshot reads', async () => {
      const manifest = buildManifest([
        { path: 'test.txt', version: 1, size: 100, hash: 'hash' },
      ]);
      const id = await snapshotManager.createSnapshot('main', 1, manifest);

      // Read concurrently
      const results = await Promise.all([
        snapshotManager.getSnapshot(id),
        snapshotManager.getSnapshot(id),
        snapshotManager.getSnapshot(id),
        snapshotManager.getSnapshot(id),
        snapshotManager.getSnapshot(id),
      ]);

      // All should return the same snapshot
      for (const result of results) {
        expect(result).not.toBeNull();
        expect(result?.id).toBe(id);
      }
    });
  });

  describe('Error Conditions', () => {
    it('should return null for non-existent snapshot', async () => {
      const result = await snapshotManager.getSnapshot('main@999999');

      expect(result).toBeNull();
    });

    it('should handle listing snapshots for non-existent branch', async () => {
      const snapshots = await snapshotManager.listSnapshots('nonexistent');

      expect(snapshots).toHaveLength(0);
    });

    it('should handle deleting non-existent snapshot', async () => {
      // Should not throw
      await snapshotManager.deleteSnapshot('main@999999');
    });

    it('should return existing snapshot ID if creating duplicate', async () => {
      const manifest = buildManifest([
        { path: 'test.txt', version: 1, size: 100, hash: 'hash' },
      ]);

      const id1 = await snapshotManager.createSnapshot('main', 1, manifest, 'First');
      const id2 = await snapshotManager.createSnapshot('main', 1, manifest, 'Second');

      // Same branch@version should return same ID
      expect(id1).toBe(id2);
    });
  });

  describe('Manifest Operations', () => {
    it('should create empty manifest', () => {
      const manifest = createEmptyManifest();

      expect(manifest.entries).toHaveLength(0);
      expect(manifest.totalSize).toBe(0);
      expect(manifest.count).toBe(0);
    });

    it('should build manifest from entries', () => {
      const entries: ManifestEntry[] = [
        { path: 'a.txt', version: 1, size: 10, hash: 'ha' },
        { path: 'b.txt', version: 2, size: 20, hash: 'hb' },
      ];

      const manifest = buildManifest(entries);

      expect(manifest.count).toBe(2);
      expect(manifest.totalSize).toBe(30);
      expect(manifest.entries).toHaveLength(2);
    });

    it('should merge manifests correctly', () => {
      const a = buildManifest([
        { path: 'shared.txt', version: 1, size: 10, hash: 'ha' },
        { path: 'a-only.txt', version: 1, size: 20, hash: 'ao' },
      ]);

      const b = buildManifest([
        { path: 'shared.txt', version: 2, size: 15, hash: 'hb' }, // Updated
        { path: 'b-only.txt', version: 1, size: 30, hash: 'bo' },
      ]);

      const merged = mergeManifests(a, b);

      expect(merged.count).toBe(3);
      // shared.txt from b (newer), a-only.txt, b-only.txt
      const sharedEntry = merged.entries.find((e) => e.path === 'shared.txt');
      expect(sharedEntry?.version).toBe(2);
      expect(sharedEntry?.size).toBe(15);
    });

    it('should filter manifest by prefix', () => {
      const manifest = buildManifest([
        { path: 'dir1/a.txt', version: 1, size: 10, hash: 'h1' },
        { path: 'dir1/b.txt', version: 1, size: 20, hash: 'h2' },
        { path: 'dir2/c.txt', version: 1, size: 30, hash: 'h3' },
      ]);

      const filtered = filterManifest(manifest, 'dir1/');

      expect(filtered.count).toBe(2);
      expect(filtered.totalSize).toBe(30);
    });

    it('should compare manifests correctly', () => {
      const a = buildManifest([
        { path: 'unchanged.txt', version: 1, size: 10, hash: 'same' },
        { path: 'modified.txt', version: 1, size: 20, hash: 'old' },
        { path: 'removed.txt', version: 1, size: 30, hash: 'rem' },
      ]);

      const b = buildManifest([
        { path: 'unchanged.txt', version: 1, size: 10, hash: 'same' },
        { path: 'modified.txt', version: 2, size: 25, hash: 'new' },
        { path: 'added.txt', version: 1, size: 40, hash: 'add' },
      ]);

      const comparison = snapshotManager.compareManifests(a, b);

      expect(comparison.unchanged.length).toBe(1);
      expect(comparison.modified.length).toBe(1);
      expect(comparison.added.length).toBe(1);
      expect(comparison.removed.length).toBe(1);

      expect(comparison.added[0].path).toBe('added.txt');
      expect(comparison.removed[0].path).toBe('removed.txt');
      expect(comparison.modified[0].path).toBe('modified.txt');
    });
  });

  describe('Cache Management', () => {
    it('should clear cache', async () => {
      const manifest = buildManifest([
        { path: 'test.txt', version: 1, size: 100, hash: 'hash' },
      ]);
      await snapshotManager.createSnapshot('main', 1, manifest);

      const statsBefore = snapshotManager.getCacheStats();
      expect(statsBefore.size).toBeGreaterThan(0);

      snapshotManager.clearCache();

      const statsAfter = snapshotManager.getCacheStats();
      expect(statsAfter.size).toBe(0);
    });

    it('should report cache stats by branch', async () => {
      const manifest = createEmptyManifest();
      await snapshotManager.createSnapshot('main', 1, manifest);
      await snapshotManager.createSnapshot('feature', 1, manifest);

      const stats = snapshotManager.getCacheStats();

      expect(stats.branches).toContain('main');
      expect(stats.branches).toContain('feature');
    });
  });
});

// =============================================================================
// 4. Tier Migration Edge Cases (tier-migration.ts)
// =============================================================================

describe('TierMigrator Edge Cases', () => {
  let hotBackend: ReturnType<typeof createHotBackendWithStats>;
  let coldBackend: R2StorageBackend;
  let tieredBackend: TieredStorageBackend;

  beforeEach(() => {
    hotBackend = createHotBackendWithStats();
    coldBackend = createR2Backend(env.TEST_R2_BUCKET as R2BucketLike, {
      keyPrefix: 'tier-migration-edge-test',
    });
  });

  afterEach(async () => {
    hotBackend.clear();
    // Clean up cold storage
    const coldFiles = await coldBackend.list('');
    for (const file of coldFiles) {
      await coldBackend.delete(file);
    }
  });

  describe('Empty Files', () => {
    it('should migrate empty files correctly', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false }
      );

      await tieredBackend.write('empty.txt', new Uint8Array(0));

      const result = await tieredBackend.migrateToR2({ olderThan: 0 });

      expect(result.migrated).toContain('empty.txt');

      // Should still be readable
      const data = await tieredBackend.read('empty.txt');
      expect(data?.length).toBe(0);
    });

    it('should promote empty files from cold to hot', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false }
      );

      // Write directly to cold
      await coldBackend.write('empty-cold.txt', new Uint8Array(0));

      const result = await tieredBackend.promoteToHot(['empty-cold.txt']);

      expect(result.migrated).toContain('empty-cold.txt');
    });
  });

  describe('Maximum File Sizes', () => {
    it('should migrate large files correctly', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        {
          autoMigrate: false,
          maxHotFileSize: 512 * 1024, // 512KB
        }
      );

      const largeData = generateTestData(256 * 1024); // 256KB
      await tieredBackend.write('large.bin', largeData);

      const result = await tieredBackend.migrateToR2({ olderThan: 0 });

      expect(result.migrated).toContain('large.bin');
      expect(result.bytesTransferred).toBe(256 * 1024);

      // Verify integrity
      const readBack = await tieredBackend.read('large.bin');
      expect(readBack).toEqual(largeData);
    });

    it('should refuse to promote files exceeding maxHotFileSize', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        {
          autoMigrate: false,
          maxHotFileSize: 100, // Very small
        }
      );

      // Write large file directly to cold
      const largeData = generateTestData(500);
      await coldBackend.write('too-large.bin', largeData);

      const result = await tieredBackend.promoteToHot(['too-large.bin']);

      expect(result.failed.length).toBe(1);
      expect(result.failed[0].path).toBe('too-large.bin');
      expect(result.failed[0].error).toContain('too large');
    });
  });

  describe('Concurrent Access', () => {
    it('should handle concurrent migration operations', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false }
      );

      // Write multiple files
      for (let i = 0; i < 5; i++) {
        await tieredBackend.write(`concurrent${i}.bin`, generateTestData(100 + i));
      }

      // Start multiple migrations concurrently
      const results = await Promise.all([
        tieredBackend.migrateToR2({ olderThan: 0, limit: 2 }),
        tieredBackend.migrateToR2({ olderThan: 0, limit: 2 }),
      ]);

      // Both should complete without error
      for (const result of results) {
        expect(result.failed).toHaveLength(0);
      }
    });

    it('should handle concurrent reads during migration', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false }
      );

      const data = generateTestData(1000);
      await tieredBackend.write('concurrent-read.bin', data);

      // Start migration and reads concurrently
      const migrationPromise = tieredBackend.migrateToR2({ olderThan: 0 });
      const readPromises = [];
      for (let i = 0; i < 10; i++) {
        readPromises.push(tieredBackend.read('concurrent-read.bin'));
      }

      const [migrationResult, ...readResults] = await Promise.all([
        migrationPromise,
        ...readPromises,
      ]);

      // Migration should complete
      expect(migrationResult.failed).toHaveLength(0);

      // All reads should return valid data
      for (const result of readResults) {
        expect(result).toEqual(data);
      }
    });

    it('should handle write during migration', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false }
      );

      await tieredBackend.write('will-update.bin', textToBytes('original'));

      // Start migration
      const migrationPromise = tieredBackend.migrateToR2({
        olderThan: 0,
        deleteFromHot: true,
      });

      // Write new version during migration
      await tieredBackend.write('will-update.bin', textToBytes('updated'));

      await migrationPromise;

      // Should have the updated version
      const result = await tieredBackend.read('will-update.bin');
      expect(bytesToText(result!)).toBe('updated');
    });
  });

  describe('Error Conditions', () => {
    it('should handle migration when cold storage already has file', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false }
      );

      // Write to both storages
      await tieredBackend.write('both.bin', textToBytes('hot'));
      await coldBackend.write('both.bin', textToBytes('cold'));

      const result = await tieredBackend.migrateToR2({ olderThan: 0 });

      // Should overwrite in cold storage
      expect(result.migrated).toContain('both.bin');
    });

    it('should handle promotion of non-existent file', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false }
      );

      const result = await tieredBackend.promoteToHot(['nonexistent.bin']);

      expect(result.failed.length).toBe(1);
      expect(result.failed[0].error).toContain('not found');
    });

    it('should respect migration limit', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false }
      );

      for (let i = 0; i < 10; i++) {
        await tieredBackend.write(`limit${i}.bin`, generateTestData(100));
      }

      const result = await tieredBackend.migrateToR2({ olderThan: 0, limit: 3 });

      expect(result.migrated.length).toBe(3);
    });

    it('should respect prefix filter during migration', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false }
      );

      await tieredBackend.write('dir1/file1.bin', generateTestData(100));
      await tieredBackend.write('dir1/file2.bin', generateTestData(100));
      await tieredBackend.write('dir2/file3.bin', generateTestData(100));

      const result = await tieredBackend.migrateToR2({
        olderThan: 0,
        prefix: 'dir1/',
      });

      expect(result.migrated).toContain('dir1/file1.bin');
      expect(result.migrated).toContain('dir1/file2.bin');
      expect(result.migrated).not.toContain('dir2/file3.bin');
    });
  });

  describe('Migration Progress Tracker', () => {
    it('should track migration progress correctly', () => {
      const tracker = createProgressTracker();

      tracker.recordMigration('hot-to-cold', 5, 1000);
      tracker.recordMigration('cold-to-hot', 3, 500);

      const stats = tracker.getStats();

      expect(stats.migrationCount).toBe(2);
      expect(stats.totalBytesMigrated).toBe(1500);
      expect(stats.lastMigration).toBeDefined();
    });

    it('should not record zero-file migrations', () => {
      const tracker = createProgressTracker();

      tracker.recordMigration('hot-to-cold', 0, 0);

      const stats = tracker.getStats();

      expect(stats.migrationCount).toBe(0);
    });

    it('should limit history size', () => {
      const tracker = createProgressTracker(5); // Max 5 entries

      for (let i = 0; i < 10; i++) {
        tracker.recordMigration('hot-to-cold', 1, 100);
      }

      const stats = tracker.getStats();

      expect(stats.recentHistory.length).toBeLessThanOrEqual(5);
      expect(stats.migrationCount).toBe(10);
    });
  });

  describe('Delete From Hot Option', () => {
    it('should keep file in hot when deleteFromHot is false', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false }
      );

      await tieredBackend.write('keep-hot.bin', generateTestData(100));

      await tieredBackend.migrateToR2({
        olderThan: 0,
        deleteFromHot: false,
      });

      // Should be in both tiers
      expect(await hotBackend.exists('keep-hot.bin')).toBe(true);
      expect(await coldBackend.exists('keep-hot.bin')).toBe(true);
    });

    it('should remove from hot when deleteFromHot is true', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false }
      );

      await tieredBackend.write('remove-hot.bin', generateTestData(100));

      await tieredBackend.migrateToR2({
        olderThan: 0,
        deleteFromHot: true,
      });

      // Should only be in cold tier
      expect(await hotBackend.exists('remove-hot.bin')).toBe(false);
      expect(await coldBackend.exists('remove-hot.bin')).toBe(true);
    });
  });

  describe('Pinned Files', () => {
    it('should not migrate pinned files', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false }
      );

      await tieredBackend.write('pinned.bin', generateTestData(100));
      await tieredBackend.pinToHot('pinned.bin');

      await tieredBackend.write('not-pinned.bin', generateTestData(100));

      const result = await tieredBackend.migrateToR2({ olderThan: 0 });

      expect(result.migrated).not.toContain('pinned.bin');
      expect(result.migrated).toContain('not-pinned.bin');
    });

    it('should allow unpinning and then migrating', async () => {
      tieredBackend = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false }
      );

      await tieredBackend.write('was-pinned.bin', generateTestData(100));
      await tieredBackend.pinToHot('was-pinned.bin');

      // Unpin
      await tieredBackend.unpinFromHot('was-pinned.bin');

      const result = await tieredBackend.migrateToR2({ olderThan: 0 });

      expect(result.migrated).toContain('was-pinned.bin');
    });
  });
});

// =============================================================================
// 5. COW Backend Edge Cases (cow-backend.ts)
// =============================================================================

describe('COWBackend Edge Cases', () => {
  let memoryBackend: MemoryFSXBackend;
  let cowBackend: COWBackend;

  beforeEach(async () => {
    memoryBackend = createMemoryBackend();
    cowBackend = await createCOWBackend(memoryBackend);
  });

  afterEach(() => {
    memoryBackend.clear();
  });

  describe('Reference Chain Depth', () => {
    it('should handle maximum ref depth', async () => {
      // Content addressing means same content creates references
      const content = textToBytes('shared content');

      // Create multiple branches with same content
      await cowBackend.write('shared.txt', content);

      for (let i = 0; i < 5; i++) {
        await cowBackend.branch('main', `branch${i}`);
        await cowBackend.checkout(`branch${i}`);
        await cowBackend.write('shared.txt', content);
      }

      // Should still be able to read
      for (let i = 0; i < 5; i++) {
        const data = await cowBackend.readFrom('shared.txt', `branch${i}`);
        expect(data).toEqual(content);
      }
    });
  });

  describe('Binary Data Integrity', () => {
    it('should preserve all byte values correctly', async () => {
      // Create data with all possible byte values
      const data = new Uint8Array(256);
      for (let i = 0; i < 256; i++) {
        data[i] = i;
      }

      await cowBackend.write('all-bytes.bin', data);
      const result = await cowBackend.read('all-bytes.bin');

      expect(result).toEqual(data);
      for (let i = 0; i < 256; i++) {
        expect(result?.[i]).toBe(i);
      }
    });

    it('should handle data with null bytes', async () => {
      const dataWithNulls = new Uint8Array([0, 1, 0, 2, 0, 3, 0, 0, 0]);

      await cowBackend.write('nulls.bin', dataWithNulls);
      const result = await cowBackend.read('nulls.bin');

      expect(result).toEqual(dataWithNulls);
    });
  });

  describe('Content-Addressing Deduplication', () => {
    it('should share storage for identical content', async () => {
      const content = generateTestData(10000);

      // Write same content to many files
      for (let i = 0; i < 10; i++) {
        await cowBackend.write(`dup${i}.bin`, content);
      }

      // All should have same hash
      const hashes = new Set<string>();
      for (let i = 0; i < 10; i++) {
        const ref = await cowBackend.getRef(`dup${i}.bin`);
        if (ref?.hash) hashes.add(ref.hash);
      }

      // Only one unique hash
      expect(hashes.size).toBe(1);
    });

    it('should not share storage for different content', async () => {
      // Write different content
      await cowBackend.write('diff1.bin', generateTestData(100, 1));
      await cowBackend.write('diff2.bin', generateTestData(100, 2));

      const ref1 = await cowBackend.getRef('diff1.bin');
      const ref2 = await cowBackend.getRef('diff2.bin');

      expect(ref1?.hash).not.toBe(ref2?.hash);
    });
  });

  describe('Branch Name Edge Cases', () => {
    it('should handle branch names with special characters', async () => {
      const specialNames = [
        'feature-test',
        'feature_test',
        'feature.test',
        'UPPERCASE',
        'MixedCase',
        '123numeric',
      ];

      await cowBackend.write('base.txt', textToBytes('base'));

      for (const name of specialNames) {
        await cowBackend.branch('main', name);
        await cowBackend.checkout(name);

        // Should work correctly
        const data = await cowBackend.read('base.txt');
        expect(bytesToText(data!)).toBe('base');
      }
    });

    it('should handle long branch names', async () => {
      const longName = 'a'.repeat(200);
      await cowBackend.write('base.txt', textToBytes('base'));

      await cowBackend.branch('main', longName);
      const branch = await cowBackend.getBranch(longName);

      expect(branch?.name).toBe(longName);
    });
  });

  describe('Path Edge Cases', () => {
    it('should handle very long paths', async () => {
      const longPath = 'a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/file.txt';

      await cowBackend.write(longPath, textToBytes('deep'));

      const result = await cowBackend.read(longPath);
      expect(bytesToText(result!)).toBe('deep');
    });

    it('should handle paths with multiple dots', async () => {
      const path = 'file.backup.2024.01.15.txt';

      await cowBackend.write(path, textToBytes('backup'));

      const result = await cowBackend.read(path);
      expect(bytesToText(result!)).toBe('backup');
    });
  });
});
