/**
 * DoLake Core Module Tests
 *
 * Comprehensive tests for DoLake core modules:
 * - dolake.ts: DoLake initialization and CDC ingestion
 * - compaction.ts: Parquet file compaction
 * - partitioning.ts: Table partitioning strategies
 *
 * Issue: sql-ld6a - DoLake Core Module Tests
 *
 * Uses workers-vitest-pool (NO MOCKS) - tests run against real
 * Cloudflare Workers runtime.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import {
  type CDCEvent,
  type DataFile,
  type ManifestFile,
  type IcebergTableMetadata,
  type IcebergPartitionSpec,
  type IcebergSchema,
  generateUUID,
  generateBatchId,
  generateCorrelationId,
  DEFAULT_DOLAKE_CONFIG,
} from '../index.js';
import { CDCBufferManager, type DedupConfig, DEFAULT_DEDUP_CONFIG } from '../buffer.js';
import {
  CompactionManager,
  type CompactionConfig,
  type CompactionResult,
  DEFAULT_COMPACTION_CONFIG,
  CompactionError,
} from '../compaction.js';
import {
  PartitionManager,
  yearTransform,
  monthTransform,
  dayTransform,
  hourTransform,
  bucketTransform,
  hashValue,
  truncateTransform,
  dayToDateString,
  hourToDatetimeString,
  createDayPartitionSpec,
  createHourPartitionSpec,
  createBucketPartitionSpec,
  createCompositePartitionSpec,
  computePartitionValue,
  computePartitionIdentifier,
  partitionKeyToString,
  prunePartitions,
  partitionMatchesPredicate,
  parseWhereClause,
  calculatePartitionStats,
  calculateBucketDistribution,
  type PartitionPredicate,
  DEFAULT_PARTITION_MANAGER_CONFIG,
} from '../partitioning.js';

// =============================================================================
// Test Utilities
// =============================================================================

function createCDCEvent(overrides: Partial<CDCEvent> = {}): CDCEvent {
  return {
    sequence: Date.now(),
    timestamp: Date.now(),
    operation: 'INSERT',
    table: 'test_table',
    rowId: generateUUID(),
    after: { id: generateUUID(), name: 'Test', value: Math.random() * 1000 },
    ...overrides,
  };
}

function createTimestampedEvent(
  table: string,
  timestamp: number,
  data: Record<string, unknown> = {}
): CDCEvent {
  return {
    sequence: Date.now(),
    timestamp,
    operation: 'INSERT',
    table,
    rowId: generateUUID(),
    after: { timestamp, ...data },
  };
}

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

function createSmallFile(sizeBytes: number = 1024): DataFile {
  return createTestDataFile({
    'file-size-in-bytes': BigInt(sizeBytes),
    'record-count': BigInt(Math.floor(sizeBytes / 10)),
  });
}

function createLargeFile(sizeBytes: number = 128 * 1024 * 1024): DataFile {
  return createTestDataFile({
    'file-size-in-bytes': BigInt(sizeBytes),
    'record-count': BigInt(Math.floor(sizeBytes / 10)),
  });
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

function serializeWithBigInt(data: unknown): string {
  return JSON.stringify(data, (_, v) => (typeof v === 'bigint' ? v.toString() : v));
}

// =============================================================================
// DoLake Initialization Tests
// =============================================================================

describe('DoLake Initialization', () => {
  describe('Health and Status Endpoints', () => {
    it('should respond to health check on fresh instance', async () => {
      const id = env.DOLAKE.idFromName('test-init-health-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/health');
      expect(response.status).toBe(200);

      const text = await response.text();
      expect(text).toBe('OK');
    });

    it('should return idle state on fresh instance', async () => {
      const id = env.DOLAKE.idFromName('test-init-status-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/status');
      expect(response.status).toBe(200);

      const status = await response.json() as { state: string; buffer: { eventCount: number } };
      expect(status.state).toBeDefined();
      expect(status.buffer.eventCount).toBe(0);
    });

    it('should return Prometheus metrics on fresh instance', async () => {
      const id = env.DOLAKE.idFromName('test-init-metrics-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/metrics');
      expect(response.status).toBe(200);

      const metrics = await response.text();
      expect(metrics).toContain('dolake_buffer_events');
      expect(metrics).toContain('dolake_connected_sources');
      expect(metrics).toContain('dolake_dedup_checks');
    });

    it('should return 404 for unknown endpoints', async () => {
      const id = env.DOLAKE.idFromName('test-init-404-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/unknown-endpoint');
      expect(response.status).toBe(404);
    });
  });

  describe('REST Catalog Initialization', () => {
    it('should return catalog config', async () => {
      const id = env.DOLAKE.idFromName('test-init-catalog-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/v1/config');
      expect(response.status).toBe(200);

      const config = await response.json() as { defaults: { warehouse: string } };
      expect(config.defaults).toBeDefined();
      expect(config.defaults.warehouse).toBeDefined();
    });

    it('should list empty namespaces on fresh instance', async () => {
      const id = env.DOLAKE.idFromName('test-init-ns-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/v1/namespaces');
      expect(response.status).toBe(200);

      const result = await response.json() as { namespaces: string[][] };
      expect(result.namespaces).toBeDefined();
      expect(Array.isArray(result.namespaces)).toBe(true);
    });
  });

  describe('Buffer Manager Initialization', () => {
    it('should initialize buffer manager with default config', () => {
      const buffer = new CDCBufferManager();

      const stats = buffer.getStats();
      expect(stats.batchCount).toBe(0);
      expect(stats.eventCount).toBe(0);
      expect(stats.totalSizeBytes).toBe(0);
      expect(stats.utilization).toBe(0);
    });

    it('should initialize buffer manager with custom config', () => {
      const customConfig = {
        ...DEFAULT_DOLAKE_CONFIG,
        flushThresholdEvents: 500,
        maxBufferSize: 16 * 1024 * 1024,
      };
      const buffer = new CDCBufferManager(customConfig);

      // Should not throw and should be properly initialized
      const stats = buffer.getStats();
      expect(stats.eventCount).toBe(0);
    });

    it('should track schema version', () => {
      const buffer = new CDCBufferManager();
      expect(buffer.getSchemaVersion()).toBe(1);
      expect(CDCBufferManager.SCHEMA_VERSION).toBe(1);
      expect(CDCBufferManager.MIN_SUPPORTED_VERSION).toBe(0);
    });
  });
});

// =============================================================================
// CDC Ingestion Tests
// =============================================================================

describe('CDC Ingestion', () => {
  describe('HTTP CDC Endpoint', () => {
    it('should accept CDC events via HTTP POST', async () => {
      const id = env.DOLAKE.idFromName('test-cdc-http-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const events = [
        createCDCEvent({ table: 'users', after: { id: 1, name: 'Alice' } }),
        createCDCEvent({ table: 'users', after: { id: 2, name: 'Bob' } }),
      ];

      const response = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events }),
      });

      expect(response.status).toBe(200);

      const result = await response.json() as {
        success: boolean;
        eventsReceived: number;
        eventsAccepted: number;
      };
      expect(result.success).toBe(true);
      expect(result.eventsReceived).toBe(2);
      expect(result.eventsAccepted).toBe(2);
    });

    it('should reject empty events array', async () => {
      const id = env.DOLAKE.idFromName('test-cdc-empty-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events: [] }),
      });

      expect(response.status).toBe(400);

      const result = await response.json() as { error: string };
      expect(result.error).toContain('No events provided');
    });

    it('should reject malformed request', async () => {
      const id = env.DOLAKE.idFromName('test-cdc-malformed-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: 'not valid json',
      });

      expect(response.status).toBe(500);
    });
  });

  describe('Buffer Manager CDC Processing', () => {
    let buffer: CDCBufferManager;

    beforeEach(() => {
      buffer = new CDCBufferManager();
    });

    it('should add CDC batch to buffer', () => {
      const events = [createCDCEvent()];
      const result = buffer.addBatch('source1', events, 1);

      expect(result.added).toBe(true);
      expect(result.isDuplicate).toBe(false);

      const stats = buffer.getStats();
      expect(stats.batchCount).toBe(1);
      expect(stats.eventCount).toBe(1);
    });

    it('should handle large batches efficiently', () => {
      const events: CDCEvent[] = [];
      for (let i = 0; i < 1000; i++) {
        events.push(createCDCEvent({ sequence: i }));
      }

      const startTime = Date.now();
      const result = buffer.addBatch('source1', events, 1);
      const duration = Date.now() - startTime;

      expect(result.added).toBe(true);
      expect(buffer.getStats().eventCount).toBe(1000);
      // Should process 1000 events in under 100ms
      expect(duration).toBeLessThan(100);
    });

    it('should deduplicate repeated batches', () => {
      const events = [createCDCEvent()];

      // First batch should succeed
      const result1 = buffer.addBatch('source1', events, 1);
      expect(result1.added).toBe(true);
      expect(result1.isDuplicate).toBe(false);

      // Same batch should be deduplicated
      const result2 = buffer.addBatch('source1', events, 1);
      expect(result2.added).toBe(false);
      expect(result2.isDuplicate).toBe(true);

      // Buffer should only have 1 batch
      expect(buffer.getStats().batchCount).toBe(1);
    });

    it('should group events by table', () => {
      const usersEvent = createCDCEvent({ table: 'users' });
      const ordersEvent = createCDCEvent({ table: 'orders' });
      const productsEvent = createCDCEvent({ table: 'products' });

      buffer.addBatch('source1', [usersEvent, ordersEvent, productsEvent], 1);

      const byTable = buffer.getEventsByTable();
      expect(byTable.get('users')?.length).toBe(1);
      expect(byTable.get('orders')?.length).toBe(1);
      expect(byTable.get('products')?.length).toBe(1);
    });

    it('should distribute events to partition buffers', () => {
      const event1 = createCDCEvent({
        table: 'events',
        metadata: { partition: 'p1' },
      });
      const event2 = createCDCEvent({
        table: 'events',
        metadata: { partition: 'p2' },
      });
      const event3 = createCDCEvent({
        table: 'events',
        metadata: { partition: 'p1' },
      });

      buffer.addBatch('source1', [event1, event2, event3], 1);

      const partitionBuffers = buffer.getPartitionBuffersForFlush();
      expect(partitionBuffers.length).toBe(2);

      const p1Buffer = partitionBuffers.find(b => b.partitionKey === 'p1');
      const p2Buffer = partitionBuffers.find(b => b.partitionKey === 'p2');

      expect(p1Buffer?.events.length).toBe(2);
      expect(p2Buffer?.events.length).toBe(1);
    });

    it('should track source connection state', () => {
      const events = [createCDCEvent()];
      buffer.addBatch('source1', events, 1, 'shard-1');
      buffer.addBatch('source1', [createCDCEvent()], 2, 'shard-1');

      const state = buffer.getSourceStates().get('source1');
      expect(state).toBeDefined();
      expect(state?.sourceDoId).toBe('source1');
      expect(state?.sourceShardName).toBe('shard-1');
      expect(state?.lastReceivedSequence).toBe(2);
      expect(state?.batchesReceived).toBe(2);
      expect(state?.eventsReceived).toBe(2);
    });

    it('should trigger flush on event threshold', () => {
      const smallConfig = { ...DEFAULT_DOLAKE_CONFIG, flushThresholdEvents: 5 };
      const bufferWithConfig = new CDCBufferManager(smallConfig);

      // Add events one by one
      for (let i = 0; i < 6; i++) {
        bufferWithConfig.addBatch('source1', [createCDCEvent()], i);
      }

      const trigger = bufferWithConfig.shouldFlush();
      expect(trigger).toBe('threshold_events');
    });

    it('should sort events by timestamp', () => {
      const event1 = createCDCEvent({ timestamp: 3000 });
      const event2 = createCDCEvent({ timestamp: 1000 });
      const event3 = createCDCEvent({ timestamp: 2000 });

      buffer.addBatch('source1', [event1, event2, event3], 1);

      const sorted = buffer.getAllEventsSorted();
      expect(sorted[0].timestamp).toBe(1000);
      expect(sorted[1].timestamp).toBe(2000);
      expect(sorted[2].timestamp).toBe(3000);
    });

    it('should serialize and restore buffer state', () => {
      buffer.addBatch('source1', [createCDCEvent({ table: 'users' })], 1);
      buffer.addBatch('source2', [createCDCEvent({ table: 'orders' })], 1);
      buffer.updateSourceState('source1', 1, 1, 'shard-1');

      const snapshot = buffer.serialize();
      expect(snapshot.version).toBe(1);

      const restored = CDCBufferManager.restore(snapshot);
      expect(restored.getStats().batchCount).toBe(2);
      expect(restored.getSourceStates().get('source1')?.sourceShardName).toBe('shard-1');
    });
  });

  describe('Concurrent Write Handling', () => {
    it('should handle concurrent CDC requests', async () => {
      const id = env.DOLAKE.idFromName('test-cdc-concurrent-' + Date.now());
      const stub = env.DOLAKE.get(id);

      // Send multiple concurrent requests
      const requests = Array.from({ length: 10 }, (_, i) =>
        stub.fetch('http://dolake/cdc', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            events: [createCDCEvent({ table: `table_${i}` })],
          }),
        })
      );

      const responses = await Promise.all(requests);

      // All requests should succeed
      for (const response of responses) {
        expect(response.status).toBe(200);
        const result = await response.json() as { success: boolean };
        expect(result.success).toBe(true);
      }
    });
  });
});

// =============================================================================
// Parquet File Compaction Tests
// =============================================================================

describe('Parquet File Compaction', () => {
  let manager: CompactionManager;

  beforeEach(() => {
    manager = new CompactionManager();
  });

  describe('Configuration', () => {
    it('should use default configuration', () => {
      const config = manager.getConfig();
      expect(config.minFileSizeBytes).toBe(8 * 1024 * 1024);
      expect(config.targetFileSizeBytes).toBe(128 * 1024 * 1024);
      expect(config.maxFilesToCompact).toBe(100);
      expect(config.minFilesToCompact).toBe(2);
    });

    it('should accept custom configuration', () => {
      const customManager = new CompactionManager({
        minFileSizeBytes: 4 * 1024 * 1024,
        targetFileSizeBytes: 64 * 1024 * 1024,
        maxFilesToCompact: 50,
        minFilesToCompact: 3,
      });

      const config = customManager.getConfig();
      expect(config.minFileSizeBytes).toBe(4 * 1024 * 1024);
      expect(config.targetFileSizeBytes).toBe(64 * 1024 * 1024);
    });

    it('should throw on invalid configuration', () => {
      expect(() => {
        new CompactionManager({ minFilesToCompact: 0 });
      }).toThrow(CompactionError);

      expect(() => {
        new CompactionManager({
          minFilesToCompact: 10,
          maxFilesToCompact: 5,
        });
      }).toThrow(CompactionError);
    });
  });

  describe('Small File Identification', () => {
    it('should identify files below threshold', () => {
      const files: DataFile[] = [
        createSmallFile(1024 * 1024), // 1MB - small
        createSmallFile(4 * 1024 * 1024), // 4MB - small
        createLargeFile(128 * 1024 * 1024), // 128MB - not small
      ];

      const smallFiles = manager.identifySmallFiles(files);
      expect(smallFiles.length).toBe(2);
    });

    it('should include files at exact threshold as small', () => {
      const customManager = new CompactionManager({
        ...DEFAULT_COMPACTION_CONFIG,
        minFileSizeBytes: 1024,
      });

      const files: DataFile[] = [
        createSmallFile(1024), // Exactly at threshold
        createSmallFile(1025), // Just above
      ];

      const smallFiles = customManager.identifySmallFiles(files);
      expect(smallFiles.length).toBe(1);
    });

    it('should return empty array when no small files', () => {
      const files: DataFile[] = [
        createLargeFile(128 * 1024 * 1024),
        createLargeFile(64 * 1024 * 1024),
      ];

      const smallFiles = manager.identifySmallFiles(files);
      expect(smallFiles.length).toBe(0);
    });

    it('should handle bigint file sizes', () => {
      const files: DataFile[] = [
        createTestDataFile({
          'file-size-in-bytes': BigInt('9007199254740992'), // > MAX_SAFE_INTEGER
        }),
      ];

      expect(() => manager.identifySmallFiles(files)).not.toThrow();
      expect(manager.identifySmallFiles(files).length).toBe(0);
    });
  });

  describe('Partition Grouping', () => {
    it('should group files by partition', () => {
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

    it('should handle unpartitioned files', () => {
      const files: DataFile[] = [
        createSmallFile(1024),
        createSmallFile(2048),
      ];

      const grouped = manager.groupByPartition(files);
      expect(grouped.size).toBe(1);
      expect(grouped.get('__unpartitioned__')?.length).toBe(2);
    });
  });

  describe('Compaction Candidate Selection', () => {
    it('should select files eligible for compaction', () => {
      const files: DataFile[] = Array.from({ length: 15 }, () =>
        createSmallFile(1024 * 1024)
      );

      const candidates = manager.selectCompactionCandidates(files);
      expect(candidates.length).toBeGreaterThan(0);
      expect(candidates[0].files.length).toBeGreaterThanOrEqual(2);
    });

    it('should not select when below minimum threshold', () => {
      const files: DataFile[] = [createSmallFile(1024 * 1024)];

      const candidates = manager.selectCompactionCandidates(files);
      expect(candidates.length).toBe(0);
    });

    it('should respect maximum files per compaction', () => {
      const files: DataFile[] = Array.from({ length: 200 }, () =>
        createSmallFile(1024)
      );

      const candidates = manager.selectCompactionCandidates(files);
      for (const candidate of candidates) {
        expect(candidate.files.length).toBeLessThanOrEqual(100);
      }
    });

    it('should calculate estimated output size', () => {
      const files: DataFile[] = [
        createSmallFile(1024 * 1024),
        createSmallFile(2 * 1024 * 1024),
        createSmallFile(3 * 1024 * 1024),
      ];

      const candidates = manager.selectCompactionCandidates(files);
      if (candidates.length > 0) {
        expect(candidates[0].estimatedOutputSize).toBe(BigInt(6 * 1024 * 1024));
      }
    });

    it('should assign priority scores', () => {
      const files: DataFile[] = Array.from({ length: 20 }, () =>
        createSmallFile(1024 * 1024)
      );

      const candidates = manager.selectCompactionCandidates(files);
      if (candidates.length > 1) {
        // Should be sorted by priority (highest first)
        expect(candidates[0].priority).toBeGreaterThanOrEqual(candidates[1].priority);
      }
    });
  });

  describe('Space Savings Estimation', () => {
    it('should estimate space savings', () => {
      const files: DataFile[] = Array.from({ length: 10 }, () =>
        createSmallFile(1024 * 1024)
      );

      const savings = manager.estimateSpaceSavings(files);
      expect(savings.estimatedFileReduction).toBeGreaterThan(0);
      expect(savings.estimatedSizeReduction).toBeDefined();
      expect(savings.compressionRatio).toBeCloseTo(0.95, 2);
    });

    it('should handle empty file list', () => {
      const savings = manager.estimateSpaceSavings([]);
      expect(savings.estimatedFileReduction).toBe(0);
      expect(savings.estimatedSizeReduction).toBe(BigInt(0));
      expect(savings.compressionRatio).toBe(1);
    });

    it('should calculate compression ratio', () => {
      const inputSize = BigInt(100 * 1024 * 1024);
      const outputSize = BigInt(90 * 1024 * 1024);

      const ratio = manager.calculateCompressionRatio(inputSize, outputSize);
      expect(ratio).toBeCloseTo(0.9, 2);
    });
  });

  describe('Manifest Operations', () => {
    it('should create compacted manifest', () => {
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
      expect(newManifest['deleted-files-count']).toBe(8);
    });

    it('should prepare atomic commit', () => {
      const mockMetadata = {
        'current-snapshot-id': BigInt(12345),
        'last-sequence-number': BigInt(1),
      } as unknown as IcebergTableMetadata;

      const preparation = manager.prepareAtomicCommit(
        mockMetadata,
        [createTestManifestFile()],
        createLargeFile()
      );

      expect(preparation.manifest).toBeDefined();
      expect(preparation.filesToDelete.length).toBe(1);
    });

    it('should prepare rollback', () => {
      const manifests = [createTestManifestFile(), createTestManifestFile()];
      const rollback = manager.prepareRollback(manifests);

      expect(rollback.manifests.length).toBe(2);
      expect(rollback.restoreOperations.length).toBe(2);
      expect(rollback.restoreOperations[0].action).toBe('restore');
    });
  });

  describe('Metrics Tracking', () => {
    it('should initialize with zero metrics', () => {
      const metrics = manager.getMetrics();
      expect(metrics.totalCompactions).toBe(0);
      expect(metrics.successfulCompactions).toBe(0);
      expect(metrics.failedCompactions).toBe(0);
    });

    it('should record successful compaction', () => {
      manager.recordCompactionResult({
        success: true,
        filesCompacted: 5,
        bytesCompacted: BigInt(50 * 1024 * 1024),
        outputFiles: 1,
        outputBytes: BigInt(45 * 1024 * 1024),
        durationMs: 1500,
      });

      const metrics = manager.getMetrics();
      expect(metrics.totalCompactions).toBe(1);
      expect(metrics.successfulCompactions).toBe(1);
      expect(metrics.filesCompacted).toBe(5);
      expect(metrics.averageDurationMs).toBe(1500);
    });

    it('should record failed compaction', () => {
      manager.recordCompactionResult({
        success: false,
        filesCompacted: 0,
        bytesCompacted: BigInt(0),
        outputFiles: 0,
        outputBytes: BigInt(0),
        durationMs: 100,
        error: 'Test error',
      });

      const metrics = manager.getMetrics();
      expect(metrics.totalCompactions).toBe(1);
      expect(metrics.failedCompactions).toBe(1);
    });

    it('should reset metrics', () => {
      manager.recordCompactionResult({
        success: true,
        filesCompacted: 5,
        bytesCompacted: BigInt(50 * 1024 * 1024),
        outputFiles: 1,
        outputBytes: BigInt(45 * 1024 * 1024),
        durationMs: 1500,
      });

      manager.resetMetrics();

      const metrics = manager.getMetrics();
      expect(metrics.totalCompactions).toBe(0);
    });
  });

  describe('Compaction API Integration', () => {
    it('should expose compaction metrics endpoint', async () => {
      const id = env.DOLAKE.idFromName('test-compact-metrics-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/v1/compaction/metrics');
      expect(response.status).toBe(200);

      const metrics = await response.json() as { totalCompactions: number };
      expect(metrics.totalCompactions).toBeDefined();
    });

    it('should handle compaction dry run', async () => {
      const id = env.DOLAKE.idFromName('test-compact-dryrun-' + Date.now());
      const stub = env.DOLAKE.get(id);

      // Create namespace first
      await stub.fetch('http://dolake/v1/namespaces', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          namespace: ['compact_test'],
          properties: {},
        }),
      });

      const response = await stub.fetch('http://dolake/v1/compaction/plan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          namespace: ['compact_test'],
          tableName: 'test_table',
          dryRun: true,
        }),
      });

      expect([200, 404]).toContain(response.status);
    });
  });
});

// =============================================================================
// Table Partitioning Tests
// =============================================================================

describe('Table Partitioning', () => {
  describe('Partition Transform Functions', () => {
    it('should transform timestamp to year', () => {
      const timestamp = new Date('2024-06-15T12:30:00Z').getTime();
      expect(yearTransform(timestamp)).toBe(2024);
    });

    it('should transform timestamp to month', () => {
      const timestamp = new Date('2024-06-15T12:30:00Z').getTime();
      const month = monthTransform(timestamp);
      // Month is year * 12 + month (0-indexed)
      expect(month).toBe(2024 * 12 + 5); // June is month 5 (0-indexed)
    });

    it('should transform timestamp to day', () => {
      const timestamp = new Date('2024-06-15T12:30:00Z').getTime();
      const day = dayTransform(timestamp);
      expect(day).toBeGreaterThan(0);
      // Verify it's the same day for different times
      const laterTime = new Date('2024-06-15T23:59:59Z').getTime();
      expect(dayTransform(laterTime)).toBe(day);
    });

    it('should transform timestamp to hour', () => {
      const timestamp = new Date('2024-06-15T12:30:00Z').getTime();
      const hour = hourTransform(timestamp);
      expect(hour).toBeGreaterThan(0);
      // Verify different hours give different values
      const nextHour = new Date('2024-06-15T13:30:00Z').getTime();
      expect(hourTransform(nextHour)).toBe(hour + 1);
    });

    it('should bucket values consistently', () => {
      const value = 'test-value';
      const bucket = bucketTransform(value, 16);
      expect(bucket).toBeGreaterThanOrEqual(0);
      expect(bucket).toBeLessThan(16);
      // Same value should always give same bucket
      expect(bucketTransform(value, 16)).toBe(bucket);
    });

    it('should distribute buckets evenly', () => {
      const buckets = new Array(16).fill(0);
      for (let i = 0; i < 1000; i++) {
        const bucket = bucketTransform(`value-${i}`, 16);
        buckets[bucket]++;
      }
      // Each bucket should have roughly 1000/16 = 62.5 values
      // Allow some variance (30-100 per bucket)
      for (const count of buckets) {
        expect(count).toBeGreaterThan(30);
        expect(count).toBeLessThan(100);
      }
    });

    it('should hash null values to 0', () => {
      expect(hashValue(null)).toBe(0);
      expect(hashValue(undefined)).toBe(0);
    });

    it('should truncate strings', () => {
      expect(truncateTransform('hello world', 5)).toBe('hello');
      expect(truncateTransform('hi', 10)).toBe('hi');
    });

    it('should convert day to date string', () => {
      const day = dayTransform(new Date('2024-06-15T12:30:00Z').getTime());
      const dateStr = dayToDateString(day);
      expect(dateStr).toBe('2024-06-15');
    });

    it('should convert hour to datetime string', () => {
      const hour = hourTransform(new Date('2024-06-15T12:30:00Z').getTime());
      const dtStr = hourToDatetimeString(hour);
      expect(dtStr).toBe('2024-06-15T12:00');
    });
  });

  describe('Partition Spec Creation', () => {
    it('should create day partition spec', () => {
      const spec = createDayPartitionSpec(2, 'event_day', 0);
      expect(spec['spec-id']).toBe(0);
      expect(spec.fields.length).toBe(1);
      expect(spec.fields[0].transform).toBe('day');
      expect(spec.fields[0]['source-id']).toBe(2);
      expect(spec.fields[0].name).toBe('event_day');
    });

    it('should create hour partition spec', () => {
      const spec = createHourPartitionSpec(2, 'event_hour', 0);
      expect(spec.fields[0].transform).toBe('hour');
    });

    it('should create bucket partition spec', () => {
      const spec = createBucketPartitionSpec(2, 16, 'customer_bucket', 0);
      expect(spec.fields[0].transform).toBe('bucket[16]');
    });

    it('should create composite partition spec', () => {
      const spec = createCompositePartitionSpec(2, 'day', 3, 8, 0);
      expect(spec.fields.length).toBe(2);
      expect(spec.fields[0].transform).toBe('day');
      expect(spec.fields[1].transform).toBe('bucket[8]');
    });
  });

  describe('Partition Value Computation', () => {
    it('should compute identity partition value', () => {
      expect(computePartitionValue('test', 'identity')).toBe('test');
    });

    it('should compute void partition value', () => {
      expect(computePartitionValue('anything', 'void')).toBeNull();
    });

    it('should compute day partition value from timestamp', () => {
      const timestamp = new Date('2024-06-15T12:30:00Z').getTime();
      const value = computePartitionValue(timestamp, 'day');
      expect(value).toBe(dayTransform(timestamp));
    });

    it('should compute bucket partition value', () => {
      const value = computePartitionValue('test-value', 'bucket[16]');
      expect(value).toBeGreaterThanOrEqual(0);
      expect(value as number).toBeLessThan(16);
    });

    it('should handle null values', () => {
      expect(computePartitionValue(null, 'day')).toBeNull();
      expect(computePartitionValue(undefined, 'bucket[16]')).toBeNull();
    });

    it('should compute partition identifier from event', () => {
      const event: CDCEvent = {
        sequence: 1,
        timestamp: Date.now(),
        operation: 'INSERT',
        table: 'events',
        rowId: 'row1',
        after: { id: 1, event_time: new Date('2024-06-15T12:00:00Z').getTime() },
      };

      const spec: IcebergPartitionSpec = {
        'spec-id': 0,
        fields: [
          { 'source-id': 2, 'field-id': 1000, name: 'day', transform: 'day' },
        ],
      };

      const schema: IcebergSchema = {
        type: 'struct',
        'schema-id': 0,
        fields: [
          { id: 1, name: 'id', type: 'long', required: true },
          { id: 2, name: 'event_time', type: 'timestamp', required: true },
        ],
      };

      const identifier = computePartitionIdentifier(event, spec, schema);
      expect(identifier.keys.length).toBe(1);
      expect(identifier.keys[0].field).toBe('day');
      expect(identifier.path).toContain('day=');
    });

    it('should convert partition identifier to string', () => {
      const identifier = {
        keys: [{ field: 'day', value: 19889, transform: 'day' as const }],
        path: 'day=19889',
      };

      expect(partitionKeyToString(identifier)).toBe('day=19889');
    });
  });

  describe('Partition Pruning', () => {
    it('should prune partitions with equality predicate', () => {
      const partitions = [
        'day=2024-01-01',
        'day=2024-01-02',
        'day=2024-01-03',
      ];

      const predicates: PartitionPredicate[] = [
        { field: 'day', operator: '=', value: '2024-01-02' },
      ];

      const result = prunePartitions(partitions, predicates);
      expect(result.included).toEqual(['day=2024-01-02']);
      expect(result.pruned.length).toBe(2);
    });

    it('should prune partitions with range predicate', () => {
      const partitions = [
        'day=2024-01-01',
        'day=2024-01-02',
        'day=2024-01-03',
        'day=2024-01-04',
        'day=2024-01-05',
      ];

      const predicates: PartitionPredicate[] = [
        { field: 'day', operator: '>=', value: '2024-01-02' },
        { field: 'day', operator: '<', value: '2024-01-04' },
      ];

      const result = prunePartitions(partitions, predicates);
      expect(result.included).toEqual(['day=2024-01-02', 'day=2024-01-03']);
    });

    it('should prune partitions with IN predicate', () => {
      const partitions = [
        'day=2024-01-01',
        'day=2024-01-02',
        'day=2024-01-03',
        'day=2024-01-04',
      ];

      const predicates: PartitionPredicate[] = [
        { field: 'day', operator: 'IN', value: ['2024-01-01', '2024-01-03'] },
      ];

      const result = prunePartitions(partitions, predicates);
      expect(result.included).toEqual(['day=2024-01-01', 'day=2024-01-03']);
    });

    it('should prune partitions with BETWEEN predicate', () => {
      const partitions = [
        'day=2024-01-01',
        'day=2024-01-02',
        'day=2024-01-03',
        'day=2024-01-04',
      ];

      const predicates: PartitionPredicate[] = [
        { field: 'day', operator: 'BETWEEN', value: '2024-01-02', endValue: '2024-01-03' },
      ];

      const result = prunePartitions(partitions, predicates);
      expect(result.included).toEqual(['day=2024-01-02', 'day=2024-01-03']);
    });

    it('should include all partitions when no matching predicate', () => {
      const partitions = ['day=2024-01-01', 'day=2024-01-02'];
      const predicates: PartitionPredicate[] = [
        { field: 'other_field', operator: '=', value: 'test' },
      ];

      const result = prunePartitions(partitions, predicates);
      expect(result.included.length).toBe(2);
    });

    it('should match partition predicate correctly', () => {
      expect(partitionMatchesPredicate('day=2024-01-01', {
        field: 'day',
        operator: '=',
        value: '2024-01-01',
      })).toBe(true);

      expect(partitionMatchesPredicate('day=2024-01-01', {
        field: 'day',
        operator: '!=',
        value: '2024-01-02',
      })).toBe(true);
    });
  });

  describe('WHERE Clause Parsing', () => {
    it('should parse equality predicates', () => {
      const predicates = parseWhereClause("WHERE day = '2024-01-01'");
      expect(predicates.some(p => p.field === 'day' && p.operator === '=' && p.value === '2024-01-01')).toBe(true);
    });

    it('should parse IN predicates', () => {
      const predicates = parseWhereClause("WHERE day IN ('2024-01-01', '2024-01-02')");
      const inPred = predicates.find(p => p.operator === 'IN');
      expect(inPred).toBeDefined();
      expect(Array.isArray(inPred?.value)).toBe(true);
    });

    it('should parse BETWEEN predicates', () => {
      const predicates = parseWhereClause("WHERE day BETWEEN '2024-01-01' AND '2024-01-31'");
      const betweenPred = predicates.find(p => p.operator === 'BETWEEN');
      expect(betweenPred).toBeDefined();
      expect(betweenPred?.endValue).toBe('2024-01-31');
    });

    it('should parse comparison predicates', () => {
      const predicates = parseWhereClause("WHERE value >= 100 AND count < 50");
      expect(predicates.some(p => p.operator === '>=')).toBe(true);
      expect(predicates.some(p => p.operator === '<')).toBe(true);
    });
  });

  describe('Partition Manager', () => {
    let partitionManager: PartitionManager;

    beforeEach(() => {
      partitionManager = new PartitionManager();
    });

    it('should register and retrieve partitions', () => {
      partitionManager.registerPartition('events', 'day=2024-01-01');
      partitionManager.registerPartition('events', 'day=2024-01-02');

      const partitions = partitionManager.getPartitions('events');
      expect(partitions.length).toBe(2);
      expect(partitions).toContain('day=2024-01-01');
    });

    it('should check partition existence', () => {
      partitionManager.registerPartition('events', 'day=2024-01-01');

      expect(partitionManager.hasPartition('events', 'day=2024-01-01')).toBe(true);
      expect(partitionManager.hasPartition('events', 'day=2024-01-02')).toBe(false);
    });

    it('should get partition count', () => {
      partitionManager.registerPartition('events', 'day=2024-01-01');
      partitionManager.registerPartition('events', 'day=2024-01-02');
      partitionManager.registerPartition('orders', 'day=2024-01-01');

      expect(partitionManager.getPartitionCount('events')).toBe(2);
      expect(partitionManager.getPartitionCount('orders')).toBe(1);
      expect(partitionManager.getPartitionCount('nonexistent')).toBe(0);
    });

    it('should remove partition', () => {
      partitionManager.registerPartition('events', 'day=2024-01-01');
      partitionManager.registerPartition('events', 'day=2024-01-02');

      partitionManager.removePartition('events', 'day=2024-01-01');

      expect(partitionManager.getPartitionCount('events')).toBe(1);
      expect(partitionManager.hasPartition('events', 'day=2024-01-01')).toBe(false);
    });

    it('should clear all partitions for table', () => {
      partitionManager.registerPartition('events', 'day=2024-01-01');
      partitionManager.registerPartition('events', 'day=2024-01-02');

      partitionManager.clearPartitions('events');

      expect(partitionManager.getPartitionCount('events')).toBe(0);
    });

    it('should paginate partition listings', () => {
      // Register many partitions
      for (let i = 0; i < 150; i++) {
        const day = String(i).padStart(3, '0');
        partitionManager.registerPartition('events', `day=2024-${day}`);
      }

      const page1 = partitionManager.listPartitions('events', 50);
      expect(page1.partitions.length).toBe(50);
      expect(page1.nextPageToken).not.toBeNull();
      expect(page1.totalCount).toBe(150);

      const page2 = partitionManager.listPartitions('events', 50, page1.nextPageToken!);
      expect(page2.partitions.length).toBe(50);
    });

    it('should create test partitions', () => {
      partitionManager.createTestPartitions('events', 100);

      const count = partitionManager.getPartitionCount('events');
      // 100 partitions spread across 365 unique days
      expect(count).toBeGreaterThan(0);
    });

    it('should create bucket partitions', () => {
      partitionManager.createBucketPartitions('orders', 16);

      const count = partitionManager.getPartitionCount('orders');
      expect(count).toBe(16);
    });

    it('should cache and invalidate metadata', () => {
      const metadata = [{ partition: 'test', files: [], stats: { partition: 'test', recordCount: BigInt(100), fileCount: 1, sizeBytes: BigInt(1024), lastModified: Date.now() }, compactionPending: false, createdAt: Date.now() }];

      partitionManager.setCachedMetadata('events', metadata);
      expect(partitionManager.getCachedMetadata('events')).toBeDefined();

      partitionManager.invalidateCache('events');
      expect(partitionManager.getCachedMetadata('events')).toBeNull();
    });
  });

  describe('Partition Statistics', () => {
    it('should calculate partition distribution stats', () => {
      const partitionSizes = new Map<string, bigint>();
      partitionSizes.set('p1', BigInt(100));
      partitionSizes.set('p2', BigInt(100));
      partitionSizes.set('p3', BigInt(200));

      const stats = calculatePartitionStats(partitionSizes);
      expect(stats.skewRatio).toBeGreaterThanOrEqual(1);
    });

    it('should identify hot partitions', () => {
      const partitionSizes = new Map<string, bigint>();
      partitionSizes.set('p1', BigInt(100));
      partitionSizes.set('p2', BigInt(100));
      partitionSizes.set('p3', BigInt(500)); // Hot partition

      const stats = calculatePartitionStats(partitionSizes);
      expect(stats.hotPartitions).toContain('p3');
    });

    it('should handle empty partition map', () => {
      const stats = calculatePartitionStats(new Map());
      expect(stats.skewRatio).toBe(1);
      expect(stats.hotPartitions.length).toBe(0);
    });

    it('should calculate bucket distribution', () => {
      // Even distribution
      const evenCounts = [100, 100, 100, 100];
      const evenStats = calculateBucketDistribution(evenCounts);
      expect(evenStats.isEven).toBe(true);
      expect(evenStats.skewRatio).toBe(1);

      // Skewed distribution
      const skewedCounts = [50, 50, 200, 50];
      const skewedStats = calculateBucketDistribution(skewedCounts);
      expect(skewedStats.skewRatio).toBeGreaterThan(1);
    });
  });

  describe('Partitioning API Integration', () => {
    it('should create table with day partitioning', async () => {
      const id = env.DOLAKE.idFromName('test-part-day-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/v1/namespaces/default/tables', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: 'events_daily',
          schema: {
            type: 'struct',
            'schema-id': 0,
            fields: [
              { id: 1, name: 'id', type: 'string', required: true },
              { id: 2, name: 'event_time', type: 'timestamp', required: true },
            ],
          },
          'partition-spec': {
            'spec-id': 0,
            fields: [
              { 'source-id': 2, 'field-id': 1000, name: 'day', transform: 'day' },
            ],
          },
        }),
      });

      expect(response.status).toBe(200);

      const table = await response.json() as IcebergTableMetadata;
      expect(table['partition-specs'][0].fields[0].transform).toBe('day');
    });

    it('should create table with bucket partitioning', async () => {
      const id = env.DOLAKE.idFromName('test-part-bucket-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/v1/namespaces/default/tables', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: 'orders_bucketed',
          schema: {
            type: 'struct',
            'schema-id': 0,
            fields: [
              { id: 1, name: 'order_id', type: 'string', required: true },
              { id: 2, name: 'customer_id', type: 'string', required: true },
            ],
          },
          'partition-spec': {
            'spec-id': 0,
            fields: [
              { 'source-id': 2, 'field-id': 1000, name: 'customer_bucket', transform: 'bucket[16]' },
            ],
          },
        }),
      });

      expect(response.status).toBe(200);

      const table = await response.json() as IcebergTableMetadata;
      expect(table['partition-specs'][0].fields[0].transform).toBe('bucket[16]');
    });

    it('should list partitions for table', async () => {
      const id = env.DOLAKE.idFromName('test-part-list-' + Date.now());
      const stub = env.DOLAKE.get(id);

      // Create test partitions
      await stub.fetch('http://dolake/v1/test/create-partitions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          namespace: ['default'],
          tableName: 'events',
          numPartitions: 10,
        }),
      });

      const response = await stub.fetch(
        'http://dolake/v1/namespaces/default/tables/events/partitions'
      );
      expect(response.status).toBe(200);

      const result = await response.json() as { partitions: string[] };
      expect(result.partitions.length).toBeGreaterThan(0);
    });

    it('should create query plan with partition pruning', async () => {
      const id = env.DOLAKE.idFromName('test-part-plan-' + Date.now());
      const stub = env.DOLAKE.get(id);

      // Create test partitions
      await stub.fetch('http://dolake/v1/test/create-partitions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          namespace: ['default'],
          tableName: 'events',
          numPartitions: 30,
        }),
      });

      const response = await stub.fetch('http://dolake/v1/query/plan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: "SELECT * FROM events WHERE day = '2024-01-15'",
        }),
      });

      expect(response.status).toBe(200);

      const plan = await response.json() as {
        partitionsIncluded: string[];
        partitionsPruned: string[];
        pruningRatio: number;
      };

      expect(plan.partitionsIncluded.length).toBeLessThan(30);
      expect(plan.pruningRatio).toBeGreaterThan(0);
    });
  });
});

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

describe('Edge Cases', () => {
  describe('Large Batch Handling', () => {
    it('should handle batch with 10000 events', () => {
      const buffer = new CDCBufferManager();
      const events: CDCEvent[] = [];

      for (let i = 0; i < 10000; i++) {
        events.push(createCDCEvent({ sequence: i }));
      }

      const result = buffer.addBatch('source1', events, 1);
      expect(result.added).toBe(true);
      expect(buffer.getStats().eventCount).toBe(10000);
    });

    it('should distribute large batch to correct partitions', () => {
      const buffer = new CDCBufferManager();
      const events: CDCEvent[] = [];

      // Create events across 5 different tables
      for (let i = 0; i < 500; i++) {
        events.push(createCDCEvent({
          table: `table_${i % 5}`,
          sequence: i,
        }));
      }

      buffer.addBatch('source1', events, 1);

      const byTable = buffer.getEventsByTable();
      expect(byTable.size).toBe(5);

      for (let i = 0; i < 5; i++) {
        expect(byTable.get(`table_${i}`)?.length).toBe(100);
      }
    });
  });

  describe('Concurrent Write Edge Cases', () => {
    it('should handle same source sending interleaved batches', () => {
      const buffer = new CDCBufferManager();

      // Interleaved sequence numbers (out of order)
      buffer.addBatch('source1', [createCDCEvent()], 1);
      buffer.addBatch('source1', [createCDCEvent()], 3);
      buffer.addBatch('source1', [createCDCEvent()], 2);

      const state = buffer.getSourceStates().get('source1');
      expect(state?.batchesReceived).toBe(3);
    });

    it('should handle multiple sources sending simultaneously', () => {
      const buffer = new CDCBufferManager();

      // Simulate interleaved batches from different sources
      buffer.addBatch('source1', [createCDCEvent()], 1);
      buffer.addBatch('source2', [createCDCEvent()], 1);
      buffer.addBatch('source3', [createCDCEvent()], 1);
      buffer.addBatch('source1', [createCDCEvent()], 2);
      buffer.addBatch('source2', [createCDCEvent()], 2);

      expect(buffer.getSourceStates().size).toBe(3);
      expect(buffer.getStats().batchCount).toBe(5);
    });
  });

  describe('Compaction Edge Cases', () => {
    it('should handle files with zero records', () => {
      const manager = new CompactionManager();
      const files: DataFile[] = [
        createTestDataFile({ 'record-count': BigInt(0), 'file-size-in-bytes': BigInt(100) }),
        createTestDataFile({ 'record-count': BigInt(0), 'file-size-in-bytes': BigInt(100) }),
        createTestDataFile({ 'record-count': BigInt(0), 'file-size-in-bytes': BigInt(100) }),
      ];

      // Should still identify as small files
      const smallFiles = manager.identifySmallFiles(files);
      expect(smallFiles.length).toBe(3);
    });

    it('should handle single partition with many small files', () => {
      const manager = new CompactionManager();
      const files: DataFile[] = Array.from({ length: 150 }, () =>
        createTestDataFile({
          partition: { day: '2024-01-01' },
          'file-size-in-bytes': BigInt(1024),
        })
      );

      const candidates = manager.selectCompactionCandidates(files);
      // Should create multiple compaction groups due to maxFilesToCompact
      expect(candidates.length).toBeGreaterThan(1);
    });
  });

  describe('Partition Edge Cases', () => {
    it('should handle partitions with special characters', () => {
      const partitionManager = new PartitionManager();

      partitionManager.registerPartition('events', 'region=us-east-1');
      partitionManager.registerPartition('events', 'tag=hello_world');

      expect(partitionManager.hasPartition('events', 'region=us-east-1')).toBe(true);
      expect(partitionManager.hasPartition('events', 'tag=hello_world')).toBe(true);
    });

    it('should handle empty partition value (HIVE_DEFAULT)', () => {
      const result = prunePartitions(
        ['col=__HIVE_DEFAULT_PARTITION__', 'col=value'],
        [{ field: 'col', operator: '=', value: 'value' }]
      );

      expect(result.included).toEqual(['col=value']);
    });

    it('should handle very large partition count', () => {
      const partitionManager = new PartitionManager();

      // Register 10000 partitions
      for (let i = 0; i < 10000; i++) {
        partitionManager.registerPartition('events', `day=${i}`);
      }

      expect(partitionManager.getPartitionCount('events')).toBe(10000);

      // Listing should still be efficient with pagination
      const startTime = Date.now();
      const page = partitionManager.listPartitions('events', 100);
      const duration = Date.now() - startTime;

      expect(page.partitions.length).toBe(100);
      expect(duration).toBeLessThan(100); // Should be fast
    });
  });
});
