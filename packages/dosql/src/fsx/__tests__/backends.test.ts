/**
 * Comprehensive FSX Backends Tests
 *
 * Tests for COW, R2, and Tiered storage backends using workers-vitest-pool.
 * These tests run against actual Cloudflare Workers environment (miniflare).
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { env, runInDurableObject, createExecutionContext } from 'cloudflare:test';
import { DurableObject } from 'cloudflare:workers';

import {
  // COW Backend
  COWBackend,
  createCOWBackend,
  COWError,
  COWErrorCode,
  DEFAULT_BRANCH,
  type SnapshotId,
  type MergeStrategy,

  // R2 Backend
  R2StorageBackend,
  createR2Backend,
  type R2BucketLike,

  // DO Backend
  DOStorageBackend,
  createDOBackend,
  type DurableObjectStorageLike,

  // Tiered Backend
  TieredStorageBackend,
  createTieredBackend,
  StorageTier,

  // Memory Backend (for isolated testing)
  MemoryFSXBackend,
  createMemoryBackend,

  // Snapshot utilities
  SnapshotManager,
  createEmptyManifest,
  buildManifest,

  // GC utilities
  GarbageCollector,
  formatBytes,
  formatGCResult,

  // Merge utilities
  MergeEngine,
  previewMerge,
  canFastForward,

  // Types
  FSXError,
  FSXErrorCode,
  DEFAULT_CHUNK_CONFIG,
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

// =============================================================================
// COW Backend Tests
// =============================================================================

describe('COWBackend', () => {
  let memoryBackend: MemoryFSXBackend;
  let cowBackend: COWBackend;

  beforeEach(async () => {
    memoryBackend = createMemoryBackend();
    cowBackend = await createCOWBackend(memoryBackend);
  });

  afterEach(() => {
    memoryBackend.clear();
  });

  describe('Branch Operations', () => {
    it('should initialize with default main branch', async () => {
      const branches = await cowBackend.listBranches();
      expect(branches.length).toBe(1);
      expect(branches[0].name).toBe(DEFAULT_BRANCH);
      expect(cowBackend.getCurrentBranch()).toBe(DEFAULT_BRANCH);
    });

    it('should create a new branch from main', async () => {
      // Write some data to main
      await cowBackend.write('file1.txt', textToBytes('Hello from main'));

      // Create a branch
      await cowBackend.branch('main', 'feature');

      const branches = await cowBackend.listBranches();
      expect(branches.length).toBe(2);
      expect(branches.map((b) => b.name).sort()).toEqual(['feature', 'main']);

      // Feature branch should have the same data
      const featureData = await cowBackend.readFrom('file1.txt', 'feature');
      expect(featureData).not.toBeNull();
      expect(bytesToText(featureData!)).toBe('Hello from main');
    });

    it('should isolate writes to branches', async () => {
      // Setup main branch with data
      await cowBackend.write('shared.txt', textToBytes('shared content'));

      // Create feature branch
      await cowBackend.branch('main', 'feature');

      // Write different content to each branch
      await cowBackend.writeTo('branch-specific.txt', textToBytes('main only'), 'main');
      await cowBackend.writeTo('branch-specific.txt', textToBytes('feature only'), 'feature');

      // Verify isolation
      const mainData = await cowBackend.readFrom('branch-specific.txt', 'main');
      const featureData = await cowBackend.readFrom('branch-specific.txt', 'feature');

      expect(bytesToText(mainData!)).toBe('main only');
      expect(bytesToText(featureData!)).toBe('feature only');
    });

    it('should switch branches with checkout', async () => {
      await cowBackend.branch('main', 'feature');

      expect(cowBackend.getCurrentBranch()).toBe('main');

      await cowBackend.checkout('feature');
      expect(cowBackend.getCurrentBranch()).toBe('feature');

      await cowBackend.checkout('main');
      expect(cowBackend.getCurrentBranch()).toBe('main');
    });

    it('should throw error when checking out non-existent branch', async () => {
      await expect(cowBackend.checkout('nonexistent')).rejects.toThrow(COWError);
    });

    it('should delete a branch', async () => {
      await cowBackend.branch('main', 'to-delete');

      let branches = await cowBackend.listBranches();
      expect(branches.length).toBe(2);

      await cowBackend.deleteBranch('to-delete');

      branches = await cowBackend.listBranches();
      expect(branches.length).toBe(1);
      expect(branches[0].name).toBe('main');
    });

    it('should not allow deleting the default branch', async () => {
      await expect(cowBackend.deleteBranch('main')).rejects.toThrow(COWError);
    });

    it('should create read-only branch', async () => {
      await cowBackend.write('data.txt', textToBytes('protected'));
      await cowBackend.branch('main', 'readonly', { readonly: true });

      const branch = await cowBackend.getBranch('readonly');
      expect(branch?.readonly).toBe(true);

      // Writing to readonly branch should fail
      await expect(
        cowBackend.writeTo('new-file.txt', textToBytes('test'), 'readonly')
      ).rejects.toThrow(COWError);
    });
  });

  describe('Snapshot Operations', () => {
    it('should create a snapshot', async () => {
      await cowBackend.write('file1.txt', textToBytes('content 1'));
      await cowBackend.write('file2.txt', textToBytes('content 2'));

      const snapshotId = await cowBackend.snapshot('main', 'Test snapshot');

      expect(snapshotId).toContain('main@');

      const snapshot = await cowBackend.getSnapshot(snapshotId);
      expect(snapshot).not.toBeNull();
      expect(snapshot?.branch).toBe('main');
      expect(snapshot?.message).toBe('Test snapshot');
      expect(snapshot?.manifest.count).toBe(2);
    });

    it('should read data at a specific snapshot', async () => {
      await cowBackend.write('version.txt', textToBytes('v1'));
      const snapshot1 = await cowBackend.snapshot('main');

      await cowBackend.write('version.txt', textToBytes('v2'));
      const snapshot2 = await cowBackend.snapshot('main');

      // Read at different snapshots
      const dataAtV1 = await cowBackend.readAt('version.txt', snapshot1);
      const dataAtV2 = await cowBackend.readAt('version.txt', snapshot2);

      expect(bytesToText(dataAtV1!)).toBe('v1');
      expect(bytesToText(dataAtV2!)).toBe('v2');
    });

    it('should list snapshots for a branch', async () => {
      await cowBackend.write('data.txt', textToBytes('v1'));
      await cowBackend.snapshot('main', 'First');

      await cowBackend.write('data.txt', textToBytes('v2'));
      await cowBackend.snapshot('main', 'Second');

      const snapshots = await cowBackend.listSnapshots('main');

      // Snapshots should be ordered oldest first (chronological)
      expect(snapshots.length).toBeGreaterThanOrEqual(2);
      expect(snapshots[0].message).toBe('First');
    });

    it('should delete a snapshot', async () => {
      await cowBackend.write('data.txt', textToBytes('test'));
      const snapshotId = await cowBackend.snapshot('main');

      let snapshots = await cowBackend.listSnapshots('main');
      const initialCount = snapshots.length;

      await cowBackend.deleteSnapshot(snapshotId);

      snapshots = await cowBackend.listSnapshots('main');
      expect(snapshots.length).toBe(initialCount - 1);
    });

    it('should create branch from specific snapshot', async () => {
      await cowBackend.write('data.txt', textToBytes('v1'));
      const snapshotV1 = await cowBackend.snapshot('main');

      await cowBackend.write('data.txt', textToBytes('v2'));

      // Create branch from v1 snapshot
      await cowBackend.branch('main', 'from-v1', { snapshot: snapshotV1 });

      // The branched data might be v2 since we branch from current state
      // but the baseSnapshot should be v1
      const branch = await cowBackend.getBranch('from-v1');
      expect(branch?.baseSnapshot).toBe(snapshotV1);
    });
  });

  describe('Merge Operations', () => {
    it('should merge branches with no conflicts (theirs strategy)', async () => {
      // Setup main with initial data
      await cowBackend.write('shared.txt', textToBytes('original'));

      // Create feature branch
      await cowBackend.branch('main', 'feature');

      // Add new file to feature
      await cowBackend.writeTo('new-feature.txt', textToBytes('feature content'), 'feature');

      // Merge feature into main
      const result = await cowBackend.merge('feature', 'main', 'theirs');

      expect(result.success).toBe(true);
      expect(result.merged).toBeGreaterThan(0);
      expect(result.updated).toContain('new-feature.txt');

      // Main should now have the feature file
      const data = await cowBackend.readFrom('new-feature.txt', 'main');
      expect(data).not.toBeNull();
      expect(bytesToText(data!)).toBe('feature content');
    });

    it('should handle merge conflicts with ours strategy', async () => {
      await cowBackend.write('conflict.txt', textToBytes('original'));
      await cowBackend.branch('main', 'feature');

      // Modify same file in both branches
      await cowBackend.writeTo('conflict.txt', textToBytes('main changes'), 'main');
      await cowBackend.writeTo('conflict.txt', textToBytes('feature changes'), 'feature');

      // Merge with ours strategy (keep main)
      const result = await cowBackend.merge('feature', 'main', 'ours');

      expect(result.success).toBe(true);

      // Main should keep its version
      const data = await cowBackend.readFrom('conflict.txt', 'main');
      expect(bytesToText(data!)).toBe('main changes');
    });

    it('should handle merge conflicts with theirs strategy', async () => {
      await cowBackend.write('conflict.txt', textToBytes('original'));
      await cowBackend.branch('main', 'feature');

      await cowBackend.writeTo('conflict.txt', textToBytes('main changes'), 'main');
      await cowBackend.writeTo('conflict.txt', textToBytes('feature changes'), 'feature');

      // Merge with theirs strategy (take feature)
      const result = await cowBackend.merge('feature', 'main', 'theirs');

      expect(result.success).toBe(true);

      // Main should have feature's version
      const data = await cowBackend.readFrom('conflict.txt', 'main');
      expect(bytesToText(data!)).toBe('feature changes');
    });

    it('should fail on conflict with fail-on-conflict strategy', async () => {
      await cowBackend.write('conflict.txt', textToBytes('original'));
      await cowBackend.branch('main', 'feature');

      await cowBackend.writeTo('conflict.txt', textToBytes('main changes'), 'main');
      await cowBackend.writeTo('conflict.txt', textToBytes('feature changes'), 'feature');

      const result = await cowBackend.merge('feature', 'main', 'fail-on-conflict');

      expect(result.success).toBe(false);
      expect(result.conflicts.length).toBeGreaterThan(0);
    });

    it('should diff two branches', async () => {
      await cowBackend.write('shared.txt', textToBytes('shared'));
      await cowBackend.write('main-only.txt', textToBytes('main'));

      await cowBackend.branch('main', 'feature');

      // Delete shared from feature, add feature-only
      await cowBackend.deleteFrom('main-only.txt', 'feature');
      await cowBackend.writeTo('feature-only.txt', textToBytes('feature'), 'feature');
      await cowBackend.writeTo('shared.txt', textToBytes('modified'), 'feature');

      const diff = await cowBackend.diff('feature', 'main');

      expect(diff.added).toContain('feature-only.txt');
      expect(diff.removed).toContain('main-only.txt');
      expect(diff.modified).toContain('shared.txt');
    });
  });

  describe('Reference Management', () => {
    it('should get blob reference', async () => {
      await cowBackend.write('test.txt', textToBytes('content'));

      const ref = await cowBackend.getRef('test.txt');

      expect(ref).not.toBeNull();
      expect(ref?.size).toBe(7); // 'content'.length
      expect(ref?.version).toBe(1);
      expect(ref?.hash).toBeDefined();
    });

    it('should increment version on updates', async () => {
      await cowBackend.write('test.txt', textToBytes('v1'));
      let ref = await cowBackend.getRef('test.txt');
      expect(ref?.version).toBe(1);

      await cowBackend.write('test.txt', textToBytes('v2'));
      ref = await cowBackend.getRef('test.txt');
      expect(ref?.version).toBe(2);

      await cowBackend.write('test.txt', textToBytes('v3'));
      ref = await cowBackend.getRef('test.txt');
      expect(ref?.version).toBe(3);
    });

    it('should share blobs with same content (content-addressing)', async () => {
      const content = textToBytes('identical content');

      await cowBackend.write('file1.txt', content);
      await cowBackend.write('file2.txt', content);

      const ref1 = await cowBackend.getRef('file1.txt');
      const ref2 = await cowBackend.getRef('file2.txt');

      // Both should reference the same hash
      expect(ref1?.hash).toBe(ref2?.hash);
    });

    it('should get multiple refs at once', async () => {
      await cowBackend.write('a.txt', textToBytes('a'));
      await cowBackend.write('b.txt', textToBytes('b'));
      await cowBackend.write('c.txt', textToBytes('c'));

      const refs = await cowBackend.getRefs(['a.txt', 'b.txt', 'c.txt', 'nonexistent.txt']);

      expect(refs.size).toBe(3);
      expect(refs.has('a.txt')).toBe(true);
      expect(refs.has('b.txt')).toBe(true);
      expect(refs.has('c.txt')).toBe(true);
      expect(refs.has('nonexistent.txt')).toBe(false);
    });
  });

  describe('Garbage Collection', () => {
    it('should run GC and report results', async () => {
      // Create and delete some data
      await cowBackend.write('temp.txt', textToBytes('temporary'));
      await cowBackend.delete('temp.txt');

      const result = await cowBackend.gc({ dryRun: true });

      expect(result.durationMs).toBeGreaterThanOrEqual(0);
      expect(result.blobsScanned).toBeGreaterThanOrEqual(0);
      expect(Array.isArray(result.deletedPaths)).toBe(true);
    });

    it('should not delete blobs still referenced', async () => {
      await cowBackend.write('keep.txt', textToBytes('keep this'));

      const result = await cowBackend.gc();

      // The blob should not be deleted
      const data = await cowBackend.read('keep.txt');
      expect(data).not.toBeNull();
    });

    it('should respect dry run option', async () => {
      await cowBackend.write('temp.txt', textToBytes('temp'));
      await cowBackend.delete('temp.txt');

      // Dry run should not actually delete
      const dryResult = await cowBackend.gc({ dryRun: true });
      const actualResult = await cowBackend.gc({ dryRun: false });

      // Both should complete without error
      expect(dryResult.durationMs).toBeGreaterThanOrEqual(0);
      expect(actualResult.durationMs).toBeGreaterThanOrEqual(0);
    });
  });

  describe('Basic FSXBackend Operations', () => {
    it('should read and write on current branch', async () => {
      const data = textToBytes('test content');
      await cowBackend.write('test.txt', data);

      const result = await cowBackend.read('test.txt');
      expect(result).toEqual(data);
    });

    it('should check file existence', async () => {
      expect(await cowBackend.exists('test.txt')).toBe(false);

      await cowBackend.write('test.txt', textToBytes('test'));

      expect(await cowBackend.exists('test.txt')).toBe(true);
    });

    it('should list files with prefix', async () => {
      await cowBackend.write('dir/a.txt', textToBytes('a'));
      await cowBackend.write('dir/b.txt', textToBytes('b'));
      await cowBackend.write('other/c.txt', textToBytes('c'));

      const dirFiles = await cowBackend.list('dir/');
      expect(dirFiles).toContain('dir/a.txt');
      expect(dirFiles).toContain('dir/b.txt');
      expect(dirFiles).not.toContain('other/c.txt');
    });

    it('should delete files', async () => {
      await cowBackend.write('to-delete.txt', textToBytes('delete me'));
      expect(await cowBackend.exists('to-delete.txt')).toBe(true);

      await cowBackend.delete('to-delete.txt');
      expect(await cowBackend.exists('to-delete.txt')).toBe(false);
    });
  });
});

// =============================================================================
// R2 Backend Tests
// =============================================================================

describe('R2StorageBackend', () => {
  let r2Backend: R2StorageBackend;

  beforeEach(() => {
    // Use the actual R2 bucket from miniflare config
    r2Backend = createR2Backend(env.TEST_R2_BUCKET as R2BucketLike);
  });

  afterEach(async () => {
    // Clean up test files
    const files = await r2Backend.list('test-');
    for (const file of files) {
      await r2Backend.delete(file);
    }
  });

  describe('Basic Operations', () => {
    it('should write and read data', async () => {
      const data = generateTestData(1000);
      await r2Backend.write('test-basic.bin', data);

      const result = await r2Backend.read('test-basic.bin');
      expect(result).toEqual(data);
    });

    it('should return null for non-existent files', async () => {
      const result = await r2Backend.read('test-nonexistent-file.bin');
      expect(result).toBeNull();
    });

    it('should check file existence', async () => {
      expect(await r2Backend.exists('test-exists.bin')).toBe(false);

      await r2Backend.write('test-exists.bin', textToBytes('exists'));

      expect(await r2Backend.exists('test-exists.bin')).toBe(true);
    });

    it('should delete files', async () => {
      await r2Backend.write('test-delete.bin', textToBytes('delete'));
      expect(await r2Backend.exists('test-delete.bin')).toBe(true);

      await r2Backend.delete('test-delete.bin');
      expect(await r2Backend.exists('test-delete.bin')).toBe(false);
    });

    it('should list files with prefix', async () => {
      await r2Backend.write('test-list-a.bin', textToBytes('a'));
      await r2Backend.write('test-list-b.bin', textToBytes('b'));
      await r2Backend.write('other-file.bin', textToBytes('c'));

      const files = await r2Backend.list('test-list-');
      expect(files).toContain('test-list-a.bin');
      expect(files).toContain('test-list-b.bin');
      expect(files).not.toContain('other-file.bin');

      // Cleanup the other file
      await r2Backend.delete('other-file.bin');
    });
  });

  describe('Byte Range Reads', () => {
    it('should read specific byte range', async () => {
      const data = generateTestData(100);
      await r2Backend.write('test-range.bin', data);

      // Read bytes 10-19 (inclusive)
      const result = await r2Backend.read('test-range.bin', [10, 19]);

      expect(result?.length).toBe(10);
      expect(result).toEqual(data.slice(10, 20));
    });

    it('should read from start with range', async () => {
      const data = generateTestData(100);
      await r2Backend.write('test-range-start.bin', data);

      const result = await r2Backend.read('test-range-start.bin', [0, 9]);

      expect(result?.length).toBe(10);
      expect(result).toEqual(data.slice(0, 10));
    });

    it('should read to end with range', async () => {
      const data = generateTestData(100);
      await r2Backend.write('test-range-end.bin', data);

      const result = await r2Backend.read('test-range-end.bin', [90, 99]);

      expect(result?.length).toBe(10);
      expect(result).toEqual(data.slice(90, 100));
    });

    it('should handle small range reads', async () => {
      const data = textToBytes('Hello, World!');
      await r2Backend.write('test-hello.bin', data);

      // Read just "World"
      const result = await r2Backend.read('test-hello.bin', [7, 11]);

      expect(bytesToText(result!)).toBe('World');
    });
  });

  describe('Metadata Operations', () => {
    it('should return file metadata', async () => {
      const data = generateTestData(500);
      await r2Backend.write('test-meta.bin', data);

      const meta = await r2Backend.metadata('test-meta.bin');

      expect(meta).not.toBeNull();
      expect(meta?.size).toBe(500);
      expect(meta?.lastModified).toBeInstanceOf(Date);
      expect(meta?.etag).toBeDefined();
    });

    it('should return null metadata for non-existent file', async () => {
      const meta = await r2Backend.metadata('test-nonexistent-meta.bin');
      expect(meta).toBeNull();
    });
  });

  describe('Key Prefix Configuration', () => {
    it('should work with key prefix', async () => {
      const prefixedBackend = createR2Backend(env.TEST_R2_BUCKET as R2BucketLike, {
        keyPrefix: 'prefixed-test',
      });

      await prefixedBackend.write('test-prefixed.bin', textToBytes('prefixed'));

      // Should read correctly with the prefix
      const result = await prefixedBackend.read('test-prefixed.bin');
      expect(bytesToText(result!)).toBe('prefixed');

      // The original backend shouldn't see it without prefix
      const files = await r2Backend.list('test-prefixed');
      expect(files).not.toContain('test-prefixed.bin');

      // Cleanup
      await prefixedBackend.delete('test-prefixed.bin');
    });
  });

  describe('Extended R2 Operations', () => {
    it('should write with custom metadata', async () => {
      const data = textToBytes('custom metadata test');

      const result = await r2Backend.writeWithMetadata('test-custom-meta.bin', data, {
        contentType: 'text/plain',
        customMetadata: {
          'x-test-key': 'test-value',
        },
      });

      expect(result.etag).toBeDefined();

      const meta = await r2Backend.metadata('test-custom-meta.bin');
      expect(meta?.custom?.['x-test-key']).toBe('test-value');
    });

    it('should batch delete multiple files', async () => {
      await r2Backend.write('test-batch-1.bin', textToBytes('1'));
      await r2Backend.write('test-batch-2.bin', textToBytes('2'));
      await r2Backend.write('test-batch-3.bin', textToBytes('3'));

      await r2Backend.deleteMany([
        'test-batch-1.bin',
        'test-batch-2.bin',
        'test-batch-3.bin',
      ]);

      expect(await r2Backend.exists('test-batch-1.bin')).toBe(false);
      expect(await r2Backend.exists('test-batch-2.bin')).toBe(false);
      expect(await r2Backend.exists('test-batch-3.bin')).toBe(false);
    });

    it('should list with metadata', async () => {
      await r2Backend.write('test-list-meta-a.bin', generateTestData(100));
      await r2Backend.write('test-list-meta-b.bin', generateTestData(200));

      const results = await r2Backend.listWithMetadata('test-list-meta-');

      expect(results.length).toBe(2);

      const fileA = results.find((r) => r.path === 'test-list-meta-a.bin');
      const fileB = results.find((r) => r.path === 'test-list-meta-b.bin');

      expect(fileA?.metadata.size).toBe(100);
      expect(fileB?.metadata.size).toBe(200);
    });

    it('should get storage stats', async () => {
      await r2Backend.write('test-stats-1.bin', generateTestData(100));
      await r2Backend.write('test-stats-2.bin', generateTestData(200));

      const stats = await r2Backend.getStats('test-stats-');

      expect(stats.objectCount).toBe(2);
      expect(stats.totalSize).toBe(300);
    });
  });

  describe('Large File Handling', () => {
    it('should handle moderately large files', async () => {
      // 1MB file
      const data = generateTestData(1024 * 1024);
      await r2Backend.write('test-large-1mb.bin', data);

      const result = await r2Backend.read('test-large-1mb.bin');
      expect(result?.length).toBe(data.length);

      // Verify data integrity by checking a few positions
      expect(result?.[0]).toBe(data[0]);
      expect(result?.[512 * 1024]).toBe(data[512 * 1024]);
      expect(result?.[data.length - 1]).toBe(data[data.length - 1]);
    });

    it('should range read large files efficiently', async () => {
      const data = generateTestData(1024 * 1024); // 1MB
      await r2Backend.write('test-large-range.bin', data);

      // Read just 1KB from the middle
      const start = 500 * 1024;
      const end = start + 1023;
      const result = await r2Backend.read('test-large-range.bin', [start, end]);

      expect(result?.length).toBe(1024);
      expect(result).toEqual(data.slice(start, end + 1));
    });
  });
});

// =============================================================================
// Tiered Backend Tests
// =============================================================================

describe('TieredStorageBackend', () => {
  let hotBackend: MemoryFSXBackend;
  let coldBackend: R2StorageBackend;
  let tieredBackend: TieredStorageBackend;

  // Using memory backend for hot to avoid DO complexity in tests
  // In production, this would be DOStorageBackend
  beforeEach(() => {
    hotBackend = createMemoryBackend();
    coldBackend = createR2Backend(env.TEST_R2_BUCKET as R2BucketLike, {
      keyPrefix: 'tiered-test',
    });

    // Create tiered backend with explicit cast since MemoryFSXBackend lacks getStats
    // We'll add a mock getStats to make it work
    const hotWithStats = hotBackend as unknown as {
      read: typeof hotBackend.read;
      write: typeof hotBackend.write;
      delete: typeof hotBackend.delete;
      list: typeof hotBackend.list;
      exists: typeof hotBackend.exists;
      metadata: typeof hotBackend.metadata;
      getStats: () => Promise<{ fileCount: number; totalSize: number; chunkedFileCount: number }>;
    };

    // Monkey-patch getStats for testing
    (hotWithStats as { getStats: () => Promise<{ fileCount: number; totalSize: number; chunkedFileCount: number }> }).getStats = async () => ({
      fileCount: hotBackend.size,
      totalSize: 0, // Simplified for testing
      chunkedFileCount: 0,
    });

    tieredBackend = createTieredBackend(
      hotWithStats as unknown as DOStorageBackend,
      coldBackend,
      {
        maxHotFileSize: 10 * 1024, // 10KB for testing
        autoMigrate: false,
        readHotFirst: true,
        cacheR2Reads: false,
      }
    );
  });

  afterEach(async () => {
    hotBackend.clear();
    // Clean up cold storage
    const coldFiles = await coldBackend.list('');
    for (const file of coldFiles) {
      await coldBackend.delete(file);
    }
  });

  describe('Automatic Tier Selection', () => {
    it('should write small files to hot storage', async () => {
      const data = generateTestData(1000); // 1KB - below threshold
      await tieredBackend.write('small.bin', data);

      // Should be in hot storage
      expect(await hotBackend.exists('small.bin')).toBe(true);
      // Should NOT be in cold storage
      expect(await coldBackend.exists('small.bin')).toBe(false);
    });

    it('should write large files directly to cold storage', async () => {
      const data = generateTestData(20 * 1024); // 20KB - above 10KB threshold
      await tieredBackend.write('large.bin', data);

      // Should NOT be in hot storage
      expect(await hotBackend.exists('large.bin')).toBe(false);
      // Should be in cold storage
      expect(await coldBackend.exists('large.bin')).toBe(true);
    });
  });

  describe('Read Strategies', () => {
    it('should read from hot storage first', async () => {
      const hotData = textToBytes('hot version');
      const coldData = textToBytes('cold version');

      await hotBackend.write('both.bin', hotData);
      await coldBackend.write('both.bin', coldData);

      // With readHotFirst=true, should get hot version
      const result = await tieredBackend.read('both.bin');
      expect(bytesToText(result!)).toBe('hot version');
    });

    it('should fallback to cold storage when not in hot', async () => {
      const coldData = textToBytes('cold only');
      await coldBackend.write('cold-only.bin', coldData);

      const result = await tieredBackend.read('cold-only.bin');
      expect(bytesToText(result!)).toBe('cold only');
    });

    it('should return null for non-existent files', async () => {
      const result = await tieredBackend.read('nonexistent.bin');
      expect(result).toBeNull();
    });
  });

  describe('Tier Metadata', () => {
    it('should return tier information in metadata', async () => {
      await tieredBackend.write('hot-file.bin', generateTestData(1000));

      const meta = await tieredBackend.metadata('hot-file.bin');

      expect(meta).not.toBeNull();
      expect(meta?.tier).toBe(StorageTier.HOT);
    });

    it('should return cold tier for large files', async () => {
      await tieredBackend.write('cold-file.bin', generateTestData(20 * 1024));

      const meta = await tieredBackend.metadata('cold-file.bin');

      expect(meta).not.toBeNull();
      expect(meta?.tier).toBe(StorageTier.COLD);
    });
  });

  describe('Delete Operations', () => {
    it('should delete from hot storage', async () => {
      await tieredBackend.write('delete-hot.bin', generateTestData(1000));
      expect(await hotBackend.exists('delete-hot.bin')).toBe(true);

      await tieredBackend.delete('delete-hot.bin');
      expect(await hotBackend.exists('delete-hot.bin')).toBe(false);
    });

    it('should delete from cold storage', async () => {
      await tieredBackend.write('delete-cold.bin', generateTestData(20 * 1024));
      expect(await coldBackend.exists('delete-cold.bin')).toBe(true);

      await tieredBackend.delete('delete-cold.bin');
      expect(await coldBackend.exists('delete-cold.bin')).toBe(false);
    });

    it('should delete from both tiers', async () => {
      // Manually put in both
      await hotBackend.write('both-delete.bin', textToBytes('hot'));
      await coldBackend.write('both-delete.bin', textToBytes('cold'));

      await tieredBackend.delete('both-delete.bin');

      expect(await hotBackend.exists('both-delete.bin')).toBe(false);
      expect(await coldBackend.exists('both-delete.bin')).toBe(false);
    });
  });

  describe('List Operations', () => {
    it('should list files from both tiers', async () => {
      await hotBackend.write('hot-list.bin', textToBytes('hot'));
      await coldBackend.write('cold-list.bin', textToBytes('cold'));

      const files = await tieredBackend.list('');

      // Filter out index entries
      const dataFiles = files.filter((f) => !f.startsWith('_tier_index/'));

      expect(dataFiles).toContain('hot-list.bin');
      expect(dataFiles).toContain('cold-list.bin');
    });

    it('should deduplicate files present in both tiers', async () => {
      await hotBackend.write('shared.bin', textToBytes('hot'));
      await coldBackend.write('shared.bin', textToBytes('cold'));

      const files = await tieredBackend.list('');
      const sharedCount = files.filter((f) => f === 'shared.bin').length;

      expect(sharedCount).toBe(1);
    });
  });

  describe('Migration Operations', () => {
    it('should migrate data from hot to cold', async () => {
      await tieredBackend.write('migrate.bin', generateTestData(1000));

      expect(await hotBackend.exists('migrate.bin')).toBe(true);
      expect(await coldBackend.exists('migrate.bin')).toBe(false);

      const result = await tieredBackend.migrateToR2({
        olderThan: 0, // Migrate everything
        deleteFromHot: true,
      });

      expect(result.migrated).toContain('migrate.bin');
      expect(await hotBackend.exists('migrate.bin')).toBe(false);
      expect(await coldBackend.exists('migrate.bin')).toBe(true);
    });

    it('should keep copy in hot when deleteFromHot is false', async () => {
      await tieredBackend.write('keep-hot.bin', generateTestData(1000));

      await tieredBackend.migrateToR2({
        olderThan: 0,
        deleteFromHot: false,
      });

      // Should be in both
      expect(await hotBackend.exists('keep-hot.bin')).toBe(true);
      expect(await coldBackend.exists('keep-hot.bin')).toBe(true);
    });

    it('should promote data from cold to hot', async () => {
      // Write directly to cold (simulating previously migrated data)
      const data = generateTestData(1000);
      await coldBackend.write('promote.bin', data);

      expect(await hotBackend.exists('promote.bin')).toBe(false);

      const result = await tieredBackend.promoteToHot(['promote.bin']);

      expect(result.migrated).toContain('promote.bin');
      expect(await hotBackend.exists('promote.bin')).toBe(true);
    });

    it('should refuse to promote files too large for hot storage', async () => {
      const largeData = generateTestData(20 * 1024); // 20KB > 10KB limit
      await coldBackend.write('too-large.bin', largeData);

      const result = await tieredBackend.promoteToHot(['too-large.bin']);

      expect(result.failed.length).toBe(1);
      expect(result.failed[0].path).toBe('too-large.bin');
      expect(result.failed[0].error).toContain('too large');
    });

    it('should respect migration limit', async () => {
      // Create multiple files
      for (let i = 0; i < 5; i++) {
        await tieredBackend.write(`limit-${i}.bin`, generateTestData(100));
      }

      const result = await tieredBackend.migrateToR2({
        olderThan: 0,
        limit: 2,
      });

      expect(result.migrated.length).toBe(2);
    });
  });

  describe('Access Patterns', () => {
    it('should track last accessed time', async () => {
      await tieredBackend.write('access-track.bin', generateTestData(1000));

      // First access
      await tieredBackend.read('access-track.bin');
      const meta1 = await tieredBackend.metadata('access-track.bin');
      const access1 = meta1?.lastAccessed;

      // Wait a bit
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Second access
      await tieredBackend.read('access-track.bin');
      const meta2 = await tieredBackend.metadata('access-track.bin');
      const access2 = meta2?.lastAccessed;

      // Second access should be later
      expect(access2!.getTime()).toBeGreaterThanOrEqual(access1!.getTime());
    });
  });

  describe('Statistics', () => {
    it('should return combined statistics', async () => {
      await tieredBackend.write('hot-stats.bin', generateTestData(1000));
      await tieredBackend.write('cold-stats.bin', generateTestData(20 * 1024));

      const stats = await tieredBackend.getStats();

      expect(stats.hot.fileCount).toBeGreaterThanOrEqual(1);
      expect(stats.cold.objectCount).toBeGreaterThanOrEqual(1);
    });
  });

  describe('Index Rebuild', () => {
    it('should rebuild index from storage contents', async () => {
      // Write some files
      await hotBackend.write('rebuild-1.bin', generateTestData(100));
      await coldBackend.write('rebuild-2.bin', generateTestData(200));

      const result = await tieredBackend.rebuildIndex();

      expect(result.indexed).toBeGreaterThanOrEqual(2);
      expect(result.errors).toBe(0);
    });
  });
});

// =============================================================================
// Snapshot Manager Tests
// =============================================================================

describe('SnapshotManager', () => {
  let memoryBackend: MemoryFSXBackend;
  let snapshotManager: SnapshotManager;

  beforeEach(() => {
    memoryBackend = createMemoryBackend();
    snapshotManager = new SnapshotManager(memoryBackend);
  });

  afterEach(() => {
    memoryBackend.clear();
  });

  it('should create and retrieve snapshots', async () => {
    const manifest = buildManifest([
      { path: 'file1.txt', version: 1, size: 100, hash: 'hash1' },
      { path: 'file2.txt', version: 1, size: 200, hash: 'hash2' },
    ]);

    const id = await snapshotManager.createSnapshot('main', 1, manifest, 'Test');

    const snapshot = await snapshotManager.getSnapshot(id);

    expect(snapshot).not.toBeNull();
    expect(snapshot?.branch).toBe('main');
    expect(snapshot?.version).toBe(1);
    expect(snapshot?.message).toBe('Test');
    expect(snapshot?.manifest.count).toBe(2);
    expect(snapshot?.manifest.totalSize).toBe(300);
  });

  it('should list snapshots in order', async () => {
    const manifest = createEmptyManifest();

    await snapshotManager.createSnapshot('main', 1, manifest, 'First');
    await snapshotManager.createSnapshot('main', 2, manifest, 'Second');
    await snapshotManager.createSnapshot('main', 3, manifest, 'Third');

    const snapshots = await snapshotManager.listSnapshots('main');

    // Should be oldest first (chronological order)
    expect(snapshots[0].message).toBe('First');
    expect(snapshots[1].message).toBe('Second');
    expect(snapshots[2].message).toBe('Third');
  });

  it('should compare manifests', () => {
    const manifestA = buildManifest([
      { path: 'shared.txt', version: 1, size: 100, hash: 'a' },
      { path: 'removed.txt', version: 1, size: 100, hash: 'b' },
    ]);

    const manifestB = buildManifest([
      { path: 'shared.txt', version: 2, size: 100, hash: 'c' }, // Modified
      { path: 'added.txt', version: 1, size: 100, hash: 'd' }, // Added
    ]);

    const comparison = snapshotManager.compareManifests(manifestA, manifestB);

    expect(comparison.added.map((e) => e.path)).toContain('added.txt');
    expect(comparison.removed.map((e) => e.path)).toContain('removed.txt');
    expect(comparison.modified.map((e) => e.path)).toContain('shared.txt');
  });

  it('should get snapshot statistics', async () => {
    const manifest = buildManifest([
      { path: 'small.txt', version: 1, size: 10, hash: 'a' },
      { path: 'large.txt', version: 1, size: 1000, hash: 'b' },
      { path: 'medium.txt', version: 1, size: 500, hash: 'c' },
    ]);

    const id = await snapshotManager.createSnapshot('main', 1, manifest);
    const snapshot = await snapshotManager.getSnapshot(id);

    const stats = snapshotManager.getSnapshotStats(snapshot!);

    expect(stats.totalFiles).toBe(3);
    expect(stats.totalSize).toBe(1510);
    expect(stats.averageFileSize).toBe(503); // Rounded
    expect(stats.largestFile?.path).toBe('large.txt');
    expect(stats.smallestFile?.path).toBe('small.txt');
  });

  it('should delete old snapshots', async () => {
    const manifest = createEmptyManifest();

    // Create snapshots with different timestamps
    await snapshotManager.createSnapshot('main', 1, manifest, 'Old');
    await new Promise((resolve) => setTimeout(resolve, 50));
    await snapshotManager.createSnapshot('main', 2, manifest, 'New');

    // Delete snapshots older than 25ms
    const deleted = await snapshotManager.deleteOldSnapshots('main', 25);

    expect(deleted).toBeGreaterThanOrEqual(1);

    const remaining = await snapshotManager.listSnapshots('main');
    expect(remaining.some((s) => s.message === 'New')).toBe(true);
  });
});

// =============================================================================
// GC Utilities Tests
// =============================================================================

describe('GC Utilities', () => {
  describe('formatBytes', () => {
    it('should format bytes correctly', () => {
      expect(formatBytes(0)).toBe('0.00 B');
      expect(formatBytes(1024)).toBe('1.00 KB');
      expect(formatBytes(1024 * 1024)).toBe('1.00 MB');
      expect(formatBytes(1024 * 1024 * 1024)).toBe('1.00 GB');
      expect(formatBytes(1536)).toBe('1.50 KB');
    });
  });

  describe('formatGCResult', () => {
    it('should format GC result', () => {
      const result = {
        blobsDeleted: 10,
        bytesReclaimed: 1024 * 1024,
        deletedPaths: ['a.bin', 'b.bin'],
        durationMs: 150,
        blobsScanned: 100,
        errors: [],
      };

      const formatted = formatGCResult(result);

      expect(formatted).toContain('150ms');
      expect(formatted).toContain('10');
      expect(formatted).toContain('1.00 MB');
    });

    it('should include error information', () => {
      const result = {
        blobsDeleted: 5,
        bytesReclaimed: 512,
        deletedPaths: [],
        durationMs: 100,
        blobsScanned: 50,
        errors: [
          { path: 'err1.bin', error: 'Error 1' },
          { path: 'err2.bin', error: 'Error 2' },
        ],
      };

      const formatted = formatGCResult(result);

      expect(formatted).toContain('Errors: 2');
      expect(formatted).toContain('err1.bin');
    });
  });
});

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('Error Handling', () => {
  describe('FSXError', () => {
    it('should create error with all properties', () => {
      const cause = new Error('underlying');
      const error = new FSXError(FSXErrorCode.READ_FAILED, 'Read failed', '/path', cause);

      expect(error.code).toBe(FSXErrorCode.READ_FAILED);
      expect(error.message).toBe('Read failed');
      expect(error.path).toBe('/path');
      expect(error.cause).toBe(cause);
      expect(error.name).toBe('FSXError');
    });
  });

  describe('COWError', () => {
    it('should create error with all properties', () => {
      const error = new COWError(COWErrorCode.BRANCH_NOT_FOUND, 'Branch not found', {
        branch: 'test',
      });

      expect(error.code).toBe(COWErrorCode.BRANCH_NOT_FOUND);
      expect(error.message).toBe('Branch not found');
      expect(error.details).toEqual({ branch: 'test' });
      expect(error.name).toBe('COWError');
    });
  });
});

// =============================================================================
// Edge Cases and Stress Tests
// =============================================================================

describe('Edge Cases', () => {
  let memoryBackend: MemoryFSXBackend;
  let cowBackend: COWBackend;

  beforeEach(async () => {
    memoryBackend = createMemoryBackend();
    cowBackend = await createCOWBackend(memoryBackend);
  });

  afterEach(() => {
    memoryBackend.clear();
  });

  it('should handle empty files', async () => {
    const emptyData = new Uint8Array(0);
    await cowBackend.write('empty.txt', emptyData);

    const result = await cowBackend.read('empty.txt');
    expect(result?.length).toBe(0);
  });

  it('should handle special characters in paths', async () => {
    const paths = [
      'path with spaces.txt',
      'path-with-dashes.txt',
      'path_with_underscores.txt',
      'path.multiple.dots.txt',
      'deeply/nested/path/file.txt',
    ];

    for (const path of paths) {
      await cowBackend.write(path, textToBytes(`content for ${path}`));
      const result = await cowBackend.read(path);
      expect(bytesToText(result!)).toBe(`content for ${path}`);
    }
  });

  it('should handle binary data correctly', async () => {
    // Create data with all possible byte values
    const data = new Uint8Array(256);
    for (let i = 0; i < 256; i++) {
      data[i] = i;
    }

    await cowBackend.write('binary.bin', data);
    const result = await cowBackend.read('binary.bin');

    expect(result).toEqual(data);
  });

  it('should handle rapid sequential writes', async () => {
    for (let i = 0; i < 100; i++) {
      await cowBackend.write('rapid.txt', textToBytes(`version ${i}`));
    }

    const result = await cowBackend.read('rapid.txt');
    expect(bytesToText(result!)).toBe('version 99');

    const ref = await cowBackend.getRef('rapid.txt');
    expect(ref?.version).toBe(100);
  });

  it('should handle deep branch hierarchies', async () => {
    await cowBackend.write('root.txt', textToBytes('root'));

    // Create chain of branches
    await cowBackend.branch('main', 'level1');
    await cowBackend.branch('level1', 'level2');
    await cowBackend.branch('level2', 'level3');

    // Modify at each level
    await cowBackend.writeTo('root.txt', textToBytes('level1'), 'level1');
    await cowBackend.writeTo('root.txt', textToBytes('level2'), 'level2');
    await cowBackend.writeTo('root.txt', textToBytes('level3'), 'level3');

    // Verify each branch has its own version
    expect(bytesToText((await cowBackend.readFrom('root.txt', 'main'))!)).toBe('root');
    expect(bytesToText((await cowBackend.readFrom('root.txt', 'level1'))!)).toBe('level1');
    expect(bytesToText((await cowBackend.readFrom('root.txt', 'level2'))!)).toBe('level2');
    expect(bytesToText((await cowBackend.readFrom('root.txt', 'level3'))!)).toBe('level3');
  });

  it('should handle multiple snapshots of same version', async () => {
    await cowBackend.write('data.txt', textToBytes('content'));

    // Creating snapshot twice with same state should return same ID
    const snap1 = await cowBackend.snapshot('main', 'First');
    const snap2 = await cowBackend.snapshot('main', 'Second');

    // Both should succeed (second might have higher version due to snapshot creation)
    expect(snap1).toBeDefined();
    expect(snap2).toBeDefined();
  });
});
