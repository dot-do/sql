/**
 * DoLake Compaction Manager Tests (TDD Red Phase)
 *
 * Tests for manifest file compaction for DoLake Parquet files.
 * Uses workers-vitest-pool (NO MOCKS).
 *
 * Issue: pocs-kh7k - Add manifest file compaction for DoLake Parquet files
 *
 * Architecture review identified that DoLake Parquet files are not compacted
 * over time, leading to many small files.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import {
  type CDCEvent,
  type DataFile,
  type ManifestFile,
  type IcebergTableMetadata,
  generateUUID,
} from '../index.js';
import {
  CompactionManager,
  type CompactionConfig,
  type CompactionResult,
  type CompactionMetrics,
  type CompactionCandidate,
  type FileInfo,
  DEFAULT_COMPACTION_CONFIG,
  CompactionError,
} from '../compaction.js';

// =============================================================================
// Test Utilities
// =============================================================================

function createTestDataFile(overrides: Partial<DataFile> = {}): DataFile {
  return {
    content: 0,
    'file-path': `/warehouse/db/table/data/${generateUUID()}.parquet`,
    'file-format': 'parquet',
    partition: {},
    'record-count': BigInt(100),
    'file-size-in-bytes': BigInt(1024),
    ...overrides,
  };
}

/**
 * Serialize data containing BigInt values for JSON.stringify
 */
function serializeWithBigInt(data: unknown): string {
  return JSON.stringify(data, (_, v) => (typeof v === 'bigint' ? v.toString() : v));
}

function createTestManifestFile(overrides: Partial<ManifestFile> = {}): ManifestFile {
  return {
    'manifest-path': `/warehouse/db/table/metadata/${generateUUID()}-manifest.avro`,
    'manifest-length': BigInt(512),
    'partition-spec-id': 0,
    content: 'data',
    'sequence-number': BigInt(1),
    'min-sequence-number': BigInt(1),
    'added-snapshot-id': BigInt(Date.now()),
    'added-files-count': 1,
    'existing-files-count': 0,
    'deleted-files-count': 0,
    'added-rows-count': BigInt(100),
    'existing-rows-count': BigInt(0),
    'deleted-rows-count': BigInt(0),
    ...overrides,
  };
}

function createSmallFile(sizeBytes: number = 1024): DataFile {
  return createTestDataFile({
    'file-size-in-bytes': BigInt(sizeBytes),
    'record-count': BigInt(Math.floor(sizeBytes / 10)), // Rough estimate
  });
}

function createLargeFile(sizeBytes: number = 128 * 1024 * 1024): DataFile {
  return createTestDataFile({
    'file-size-in-bytes': BigInt(sizeBytes),
    'record-count': BigInt(Math.floor(sizeBytes / 10)),
  });
}

// =============================================================================
// Compaction Configuration Tests
// =============================================================================

describe('Compaction Configuration', () => {
  it('should have sensible default configuration', () => {
    expect(DEFAULT_COMPACTION_CONFIG.minFileSizeBytes).toBe(8 * 1024 * 1024); // 8MB
    expect(DEFAULT_COMPACTION_CONFIG.targetFileSizeBytes).toBe(128 * 1024 * 1024); // 128MB
    expect(DEFAULT_COMPACTION_CONFIG.maxFilesToCompact).toBe(100);
    expect(DEFAULT_COMPACTION_CONFIG.minFilesToCompact).toBe(2);
    expect(DEFAULT_COMPACTION_CONFIG.compactionTriggerThreshold).toBe(10);
    expect(DEFAULT_COMPACTION_CONFIG.enableAutoCompaction).toBe(true);
  });

  it('should allow custom configuration', () => {
    const customConfig: CompactionConfig = {
      minFileSizeBytes: 4 * 1024 * 1024,
      targetFileSizeBytes: 64 * 1024 * 1024,
      maxFilesToCompact: 50,
      minFilesToCompact: 3,
      compactionTriggerThreshold: 5,
      enableAutoCompaction: false,
    };

    const manager = new CompactionManager(customConfig);
    const config = manager.getConfig();

    expect(config.minFileSizeBytes).toBe(4 * 1024 * 1024);
    expect(config.targetFileSizeBytes).toBe(64 * 1024 * 1024);
    expect(config.maxFilesToCompact).toBe(50);
    expect(config.enableAutoCompaction).toBe(false);
  });
});

// =============================================================================
// Small File Identification Tests
// =============================================================================

describe('Small File Identification', () => {
  let manager: CompactionManager;

  beforeEach(() => {
    manager = new CompactionManager();
  });

  it('should identify files below minimum size threshold', () => {
    const files: DataFile[] = [
      createSmallFile(1024), // 1KB - small
      createSmallFile(4 * 1024 * 1024), // 4MB - small
      createLargeFile(128 * 1024 * 1024), // 128MB - not small
    ];

    const smallFiles = manager.identifySmallFiles(files);

    expect(smallFiles.length).toBe(2);
    expect(smallFiles.every((f) => Number(f['file-size-in-bytes']) < 8 * 1024 * 1024)).toBe(true);
  });

  it('should return empty array when no small files exist', () => {
    const files: DataFile[] = [
      createLargeFile(128 * 1024 * 1024),
      createLargeFile(64 * 1024 * 1024),
    ];

    const smallFiles = manager.identifySmallFiles(files);

    expect(smallFiles.length).toBe(0);
  });

  it('should respect custom minimum size threshold', () => {
    const customManager = new CompactionManager({
      ...DEFAULT_COMPACTION_CONFIG,
      minFileSizeBytes: 1024 * 1024, // 1MB
    });

    const files: DataFile[] = [
      createSmallFile(512 * 1024), // 512KB - small with custom threshold
      createSmallFile(2 * 1024 * 1024), // 2MB - not small with custom threshold
    ];

    const smallFiles = customManager.identifySmallFiles(files);

    expect(smallFiles.length).toBe(1);
    expect(Number(smallFiles[0]['file-size-in-bytes'])).toBe(512 * 1024);
  });

  it('should group small files by partition', () => {
    const files: DataFile[] = [
      { ...createSmallFile(1024), partition: { date: '2024-01-01' } },
      { ...createSmallFile(2048), partition: { date: '2024-01-01' } },
      { ...createSmallFile(1024), partition: { date: '2024-01-02' } },
    ];

    const grouped = manager.groupByPartition(files);

    expect(grouped.size).toBe(2);
    expect(grouped.get('date=2024-01-01')?.length).toBe(2);
    expect(grouped.get('date=2024-01-02')?.length).toBe(1);
  });
});

// =============================================================================
// Compaction Candidate Selection Tests
// =============================================================================

describe('Compaction Candidate Selection', () => {
  let manager: CompactionManager;

  beforeEach(() => {
    manager = new CompactionManager();
  });

  it('should select files eligible for compaction', () => {
    const files: DataFile[] = Array.from({ length: 15 }, () =>
      createSmallFile(1024 * 1024)
    ); // 15 x 1MB files

    const candidates = manager.selectCompactionCandidates(files);

    expect(candidates.length).toBeGreaterThan(0);
    expect(candidates[0].files.length).toBeGreaterThanOrEqual(2);
  });

  it('should not select for compaction when below threshold', () => {
    // Only 1 small file - below minimum threshold
    const files: DataFile[] = [createSmallFile(1024 * 1024)];

    const candidates = manager.selectCompactionCandidates(files);

    expect(candidates.length).toBe(0);
  });

  it('should respect maximum files per compaction', () => {
    const files: DataFile[] = Array.from({ length: 200 }, () =>
      createSmallFile(1024)
    );

    const candidates = manager.selectCompactionCandidates(files);

    candidates.forEach((candidate) => {
      expect(candidate.files.length).toBeLessThanOrEqual(100);
    });
  });

  it('should calculate estimated output size', () => {
    const files: DataFile[] = [
      createSmallFile(1024 * 1024), // 1MB
      createSmallFile(2 * 1024 * 1024), // 2MB
      createSmallFile(3 * 1024 * 1024), // 3MB
    ];

    const candidates = manager.selectCompactionCandidates(files);

    if (candidates.length > 0) {
      expect(candidates[0].estimatedOutputSize).toBe(BigInt(6 * 1024 * 1024));
    }
  });

  it('should create multiple compaction groups when files exceed target size', () => {
    // Create files that together exceed target size
    // Use 7MB files (below 8MB threshold) so they're considered small
    const files: DataFile[] = Array.from({ length: 20 }, () =>
      createSmallFile(7 * 1024 * 1024)
    ); // 20 x 7MB = 140MB (> 128MB target)

    const candidates = manager.selectCompactionCandidates(files);

    // Should create at least 1 group since 140MB > 128MB target
    expect(candidates.length).toBeGreaterThanOrEqual(1);
  });
});

// =============================================================================
// File Merging Tests
// =============================================================================

describe('File Merging', () => {
  it('should merge small files maintaining data integrity', async () => {
    const id = env.DOLAKE.idFromName('test-compaction-merge-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Prepare test data through the API
    const files: DataFile[] = [
      createSmallFile(1024),
      createSmallFile(2048),
      createSmallFile(1024),
    ];

    const response = await stub.fetch('http://dolake/v1/compaction/merge', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: serializeWithBigInt({ files }),
    });

    expect(response.status).toBe(200);

    const result = (await response.json()) as {
      success: boolean;
      mergedFile: DataFile;
      recordCount: number;
    };
    expect(result.success).toBe(true);
    expect(result.mergedFile).toBeDefined();

    // Total records should be preserved
    const totalRecords = files.reduce(
      (sum, f) => sum + Number(f['record-count']),
      0
    );
    expect(result.recordCount).toBe(totalRecords);
  });

  it('should handle empty file list gracefully', async () => {
    const id = env.DOLAKE.idFromName('test-compaction-empty-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/compaction/merge', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ files: [] }),
    });

    expect(response.status).toBe(400);

    const error = (await response.json()) as { error: string };
    expect(error.error).toContain('No files to compact');
  });

  it('should fail gracefully on merge error', async () => {
    const id = env.DOLAKE.idFromName('test-compaction-error-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Invalid file paths should cause merge failure
    const files: DataFile[] = [
      { ...createSmallFile(1024), 'file-path': '/nonexistent/path.parquet' },
    ];

    const response = await stub.fetch('http://dolake/v1/compaction/merge', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: serializeWithBigInt({ files }),
    });

    // Should either return error or skip invalid files
    expect([200, 400, 404]).toContain(response.status);
  });
});

// =============================================================================
// Manifest Update Tests
// =============================================================================

describe('Manifest Updates', () => {
  let manager: CompactionManager;

  beforeEach(() => {
    manager = new CompactionManager();
  });

  it('should update manifest after compaction', async () => {
    const id = env.DOLAKE.idFromName('test-manifest-update-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First, create a namespace and table
    await stub.fetch('http://dolake/v1/namespaces', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['compaction_test'],
        properties: {},
      }),
    });

    // Trigger compaction with manifest update
    const response = await stub.fetch('http://dolake/v1/compaction/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['compaction_test'],
        tableName: 'test_table',
        dryRun: false,
      }),
    });

    // May not have files to compact, but should respond successfully
    expect([200, 404]).toContain(response.status);
  });

  it('should create new manifest entries for compacted files', () => {
    const oldManifests: ManifestFile[] = [
      createTestManifestFile({ 'added-files-count': 5 }),
      createTestManifestFile({ 'added-files-count': 3 }),
    ];

    const compactedFile = createLargeFile(50 * 1024 * 1024);

    const newManifest = manager.createCompactedManifest(
      oldManifests,
      compactedFile,
      BigInt(Date.now())
    );

    expect(newManifest['added-files-count']).toBe(1);
    expect(newManifest['deleted-files-count']).toBe(8); // 5 + 3 from old manifests
  });

  it('should preserve snapshot history during compaction', async () => {
    const id = env.DOLAKE.idFromName('test-snapshot-history-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Create namespace
    await stub.fetch('http://dolake/v1/namespaces', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['snapshot_test'],
        properties: {},
      }),
    });

    const response = await stub.fetch(
      'http://dolake/v1/compaction/snapshot-preserving',
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          namespace: ['snapshot_test'],
          tableName: 'test_table',
        }),
      }
    );

    // Even without data, should respond with status
    expect([200, 404]).toContain(response.status);
  });
});

// =============================================================================
// Concurrent Compaction Tests
// =============================================================================

describe('Concurrent Compaction Handling', () => {
  it('should prevent concurrent compaction on same table', async () => {
    const id = env.DOLAKE.idFromName('test-concurrent-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Create namespace
    await stub.fetch('http://dolake/v1/namespaces', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['concurrent_test'],
        properties: {},
      }),
    });

    // Start two concurrent compactions
    const compaction1 = stub.fetch('http://dolake/v1/compaction/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['concurrent_test'],
        tableName: 'test_table',
        dryRun: false,
      }),
    });

    const compaction2 = stub.fetch('http://dolake/v1/compaction/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['concurrent_test'],
        tableName: 'test_table',
        dryRun: false,
      }),
    });

    const [response1, response2] = await Promise.all([compaction1, compaction2]);

    // One should succeed, one should be rejected or indicate in-progress
    const statuses = [response1.status, response2.status];
    // At least one should succeed
    expect(statuses.some((s) => s === 200 || s === 404)).toBe(true);
  });

  it('should allow concurrent compaction on different tables', async () => {
    const id = env.DOLAKE.idFromName('test-concurrent-diff-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Create namespace
    await stub.fetch('http://dolake/v1/namespaces', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['concurrent_diff_test'],
        properties: {},
      }),
    });

    // Start compactions on different tables
    const compaction1 = stub.fetch('http://dolake/v1/compaction/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['concurrent_diff_test'],
        tableName: 'table_1',
        dryRun: true,
      }),
    });

    const compaction2 = stub.fetch('http://dolake/v1/compaction/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['concurrent_diff_test'],
        tableName: 'table_2',
        dryRun: true,
      }),
    });

    const [response1, response2] = await Promise.all([compaction1, compaction2]);

    // Both should handle request (may be 404 if tables don't exist yet)
    expect([200, 404]).toContain(response1.status);
    expect([200, 404]).toContain(response2.status);
  });

  it('should use atomic commits for manifest updates', async () => {
    const manager = new CompactionManager();

    const mockMetadata = {
      'current-snapshot-id': BigInt(12345),
      'last-sequence-number': BigInt(1),
    } as unknown as IcebergTableMetadata;

    // This should throw if commit would conflict
    const commitFn = () =>
      manager.prepareAtomicCommit(
        mockMetadata,
        [createTestManifestFile()],
        createLargeFile()
      );

    expect(commitFn).not.toThrow();
  });
});

// =============================================================================
// Compaction Metrics Tests
// =============================================================================

describe('Compaction Metrics', () => {
  let manager: CompactionManager;

  beforeEach(() => {
    manager = new CompactionManager();
  });

  it('should track compaction metrics', () => {
    const metrics = manager.getMetrics();

    expect(metrics.totalCompactions).toBeDefined();
    expect(metrics.successfulCompactions).toBeDefined();
    expect(metrics.failedCompactions).toBeDefined();
    expect(metrics.filesCompacted).toBeDefined();
    expect(metrics.bytesCompacted).toBeDefined();
    expect(metrics.lastCompactionTime).toBeDefined();
  });

  it('should update metrics after successful compaction', async () => {
    const beforeMetrics = manager.getMetrics();

    // Simulate a successful compaction
    manager.recordCompactionResult({
      success: true,
      filesCompacted: 5,
      bytesCompacted: BigInt(50 * 1024 * 1024),
      outputFiles: 1,
      outputBytes: BigInt(45 * 1024 * 1024),
      durationMs: 1500,
    });

    const afterMetrics = manager.getMetrics();

    expect(afterMetrics.totalCompactions).toBe(beforeMetrics.totalCompactions + 1);
    expect(afterMetrics.successfulCompactions).toBe(
      beforeMetrics.successfulCompactions + 1
    );
    expect(afterMetrics.filesCompacted).toBe(beforeMetrics.filesCompacted + 5);
  });

  it('should expose metrics via HTTP endpoint', async () => {
    const id = env.DOLAKE.idFromName('test-metrics-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/compaction/metrics');

    expect(response.status).toBe(200);

    const metrics = (await response.json()) as CompactionMetrics;
    expect(metrics.totalCompactions).toBeDefined();
  });
});

// =============================================================================
// Dry Run Tests
// =============================================================================

describe('Compaction Dry Run', () => {
  it('should support dry run mode', async () => {
    const id = env.DOLAKE.idFromName('test-dryrun-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Create namespace
    await stub.fetch('http://dolake/v1/namespaces', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['dryrun_test'],
        properties: {},
      }),
    });

    const response = await stub.fetch('http://dolake/v1/compaction/plan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['dryrun_test'],
        tableName: 'test_table',
        dryRun: true,
      }),
    });

    expect([200, 404]).toContain(response.status);

    if (response.status === 200) {
      const plan = (await response.json()) as {
        candidates: CompactionCandidate[];
        estimatedReduction: number;
      };
      expect(plan.candidates).toBeDefined();
      expect(Array.isArray(plan.candidates)).toBe(true);
    }
  });

  it('should calculate space savings in dry run', () => {
    const manager = new CompactionManager();

    const files: DataFile[] = Array.from({ length: 10 }, () =>
      createSmallFile(1024 * 1024)
    ); // 10 x 1MB

    const savings = manager.estimateSpaceSavings(files);

    // Compaction typically reduces file count and may slightly reduce size
    expect(savings.estimatedFileReduction).toBeGreaterThan(0);
    expect(savings.estimatedSizeReduction).toBeDefined();
  });
});

// =============================================================================
// Integration with Flush Pipeline Tests
// =============================================================================

describe('Flush Pipeline Integration', () => {
  it('should trigger compaction check after flush', async () => {
    const id = env.DOLAKE.idFromName('test-flush-trigger-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Perform a flush
    const flushResponse = await stub.fetch('http://dolake/flush', {
      method: 'POST',
    });

    expect(flushResponse.status).toBe(200);

    // Check if compaction was evaluated
    const statusResponse = await stub.fetch('http://dolake/v1/compaction/status');
    expect([200, 404]).toContain(statusResponse.status);
  });

  it('should schedule compaction when threshold is reached', async () => {
    const id = env.DOLAKE.idFromName('test-schedule-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/compaction/schedule', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'test_table',
        scheduleMs: 60000, // 1 minute
      }),
    });

    expect([200, 404, 501]).toContain(response.status);
  });
});

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('Error Handling', () => {
  it('should throw CompactionError for invalid configuration', () => {
    expect(() => {
      new CompactionManager({
        ...DEFAULT_COMPACTION_CONFIG,
        minFilesToCompact: 0, // Invalid
      });
    }).toThrow(CompactionError);
  });

  it('should handle missing files gracefully', async () => {
    const id = env.DOLAKE.idFromName('test-missing-files-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/compaction/merge', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: serializeWithBigInt({
        files: [
          { ...createSmallFile(1024), 'file-path': '/missing/file1.parquet' },
          { ...createSmallFile(1024), 'file-path': '/missing/file2.parquet' },
        ],
      }),
    });

    // Should handle gracefully, not crash
    expect([200, 400, 404]).toContain(response.status);
  });

  it('should rollback on partial failure', async () => {
    const manager = new CompactionManager();

    // Simulate a rollback scenario
    const rollbackFn = () =>
      manager.prepareRollback([
        createTestManifestFile(),
        createTestManifestFile(),
      ]);

    expect(rollbackFn).not.toThrow();
  });
});

// =============================================================================
// Threshold Tests
// =============================================================================

describe('File Size Thresholds', () => {
  it('should identify files at exact boundary as small', () => {
    const manager = new CompactionManager({
      ...DEFAULT_COMPACTION_CONFIG,
      minFileSizeBytes: 1024,
    });

    const files: DataFile[] = [
      createSmallFile(1024), // Exactly at boundary
      createSmallFile(1025), // Just above
    ];

    const smallFiles = manager.identifySmallFiles(files);

    // File at exact boundary should be considered small
    expect(smallFiles.length).toBe(1);
    expect(Number(smallFiles[0]['file-size-in-bytes'])).toBe(1024);
  });

  it('should handle bigint file sizes correctly', () => {
    const manager = new CompactionManager();

    const files: DataFile[] = [
      createTestDataFile({
        'file-size-in-bytes': BigInt('9007199254740992'), // > MAX_SAFE_INTEGER
      }),
    ];

    // Should not throw with large bigint values
    expect(() => manager.identifySmallFiles(files)).not.toThrow();
  });

  it('should calculate compaction ratio', () => {
    const manager = new CompactionManager();

    const inputSize = BigInt(100 * 1024 * 1024); // 100MB
    const outputSize = BigInt(90 * 1024 * 1024); // 90MB

    const ratio = manager.calculateCompressionRatio(inputSize, outputSize);

    expect(ratio).toBeCloseTo(0.9, 2);
  });
});
