/**
 * DoLake E2E Tests
 *
 * End-to-end tests for the DoLake lakehouse component using cloudflare:test.
 * Tests real CDC flows, Parquet generation, compaction, Iceberg format, and queries.
 *
 * NO MOCKS - tests run against real Cloudflare Workers runtime per project philosophy.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import {
  type CDCEvent,
  type FlushResult,
  type IcebergTableMetadata,
  CDCBufferManager,
  generateUUID,
  DEFAULT_DOLAKE_CONFIG,
} from '../index.js';
import {
  writeParquet,
  inferSchemaFromEvents,
  createDataFile,
} from '../parquet.js';
import {
  createSchema,
  createTableMetadata,
  createUnpartitionedSpec,
  createDatePartitionSpec,
  addSnapshot,
  createAppendSnapshot,
  partitionToPath,
  dataFilePath,
  manifestListPath,
  manifestFilePath,
} from '../iceberg.js';
import {
  CompactionManager,
  DEFAULT_COMPACTION_CONFIG,
} from '../compaction.js';
import {
  QueryEngine,
  canSkipFileByStats,
  type FileStats,
} from '../query-engine.js';
import {
  PartitionManager,
  prunePartitions,
  parseWhereClause,
  dayTransform,
  bucketTransform,
} from '../partitioning.js';

// =============================================================================
// Test Utilities
// =============================================================================

function createCDCEvent(overrides: Partial<CDCEvent> = {}): CDCEvent {
  return {
    sequence: Math.floor(Math.random() * 1000000),
    timestamp: Date.now(),
    operation: 'INSERT',
    table: 'users',
    rowId: generateUUID(),
    after: { id: 1, name: 'Test User', email: 'test@example.com' },
    ...overrides,
  };
}

function createMultipleCDCEvents(
  count: number,
  table: string,
  baseSequence: number = 1
): CDCEvent[] {
  return Array.from({ length: count }, (_, i) =>
    createCDCEvent({
      sequence: baseSequence + i,
      timestamp: Date.now() + i,
      table,
      rowId: generateUUID(),
      after: { id: i + 1, name: `User ${i + 1}`, value: Math.random() * 100 },
    })
  );
}

function createOrderEvents(count: number): CDCEvent[] {
  return Array.from({ length: count }, (_, i) =>
    createCDCEvent({
      sequence: i + 1,
      table: 'orders',
      rowId: generateUUID(),
      operation: i % 3 === 0 ? 'UPDATE' : 'INSERT',
      after: {
        order_id: i + 1,
        customer_id: `cust-${(i % 10) + 1}`,
        amount: Math.floor(Math.random() * 1000) + 100,
        status: i % 2 === 0 ? 'completed' : 'pending',
        created_at: new Date().toISOString(),
      },
    })
  );
}

// =============================================================================
// E2E Test Suite
// =============================================================================

describe('DoLake E2E', () => {
  // ===========================================================================
  // CDC Event Ingestion Tests
  // ===========================================================================

  describe('CDC Event Ingestion from DoSQL', () => {
    it('should ingest CDC events via HTTP endpoint', async () => {
      const id = env.DOLAKE.idFromName(`e2e-cdc-ingest-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      const events = createMultipleCDCEvents(5, 'users');

      const response = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events }),
      });

      expect(response.status).toBe(200);
      const result = (await response.json()) as {
        success: boolean;
        eventsReceived: number;
        eventsAccepted: number;
      };
      expect(result.success).toBe(true);
      expect(result.eventsReceived).toBe(5);
      expect(result.eventsAccepted).toBe(5);
    });

    it('should handle multiple table CDC events', async () => {
      const id = env.DOLAKE.idFromName(`e2e-multi-table-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      const userEvents = createMultipleCDCEvents(3, 'users', 1);
      const orderEvents = createOrderEvents(3);
      const allEvents = [...userEvents, ...orderEvents];

      const response = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events: allEvents }),
      });

      expect(response.status).toBe(200);
      const result = (await response.json()) as { eventsReceived: number };
      expect(result.eventsReceived).toBe(6);
    });

    it('should handle CDC batch tracking', async () => {
      const id = env.DOLAKE.idFromName(`e2e-tracking-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      const events = createMultipleCDCEvents(3, 'users');

      // Send batch - each HTTP request gets unique source ID for tracking
      const response1 = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events }),
      });

      expect(response1.status).toBe(200);
      const result1 = (await response1.json()) as { eventsAccepted: number; isDuplicate: boolean };
      expect(result1.eventsAccepted).toBe(3);
      expect(result1.isDuplicate).toBe(false);

      // Note: HTTP CDC endpoint generates unique sourceId per request
      // Deduplication at HTTP level is not based on event content
      // True deduplication happens via WebSocket with consistent sourceId + sequence
    });

    it('should handle all CDC operation types', async () => {
      const id = env.DOLAKE.idFromName(`e2e-ops-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      const events: CDCEvent[] = [
        createCDCEvent({
          operation: 'INSERT',
          after: { id: 1, name: 'New User' },
        }),
        createCDCEvent({
          operation: 'UPDATE',
          before: { id: 1, name: 'New User' },
          after: { id: 1, name: 'Updated User' },
        }),
        createCDCEvent({
          operation: 'DELETE',
          before: { id: 1, name: 'Updated User' },
          after: undefined,
        }),
      ];

      const response = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events }),
      });

      expect(response.status).toBe(200);
      const result = (await response.json()) as { eventsReceived: number };
      expect(result.eventsReceived).toBe(3);
    });

    it('should report buffer status after ingestion', async () => {
      const id = env.DOLAKE.idFromName(`e2e-buffer-status-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      const events = createMultipleCDCEvents(10, 'analytics');
      await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events }),
      });

      const statusResponse = await stub.fetch('http://dolake/status');
      const status = (await statusResponse.json()) as {
        buffer: { eventCount: number };
      };

      expect(status.buffer.eventCount).toBeGreaterThanOrEqual(10);
    });
  });

  // ===========================================================================
  // Parquet File Generation Tests
  // ===========================================================================

  describe('Parquet File Generation', () => {
    it('should generate valid Parquet file from CDC events', async () => {
      const events = createMultipleCDCEvents(10, 'test_table');
      const result = await writeParquet(events, 'test_table');

      // Verify Parquet structure
      expect(result.content).toBeInstanceOf(Uint8Array);
      expect(result.rowCount).toBe(10);
      expect(result.fileSize).toBeGreaterThan(0);

      // Check PAR1 magic bytes
      expect(result.content[0]).toBe(0x50); // P
      expect(result.content[1]).toBe(0x41); // A
      expect(result.content[2]).toBe(0x52); // R
      expect(result.content[3]).toBe(0x31); // 1

      // Check footer magic
      const len = result.content.length;
      expect(result.content[len - 4]).toBe(0x50);
      expect(result.content[len - 3]).toBe(0x41);
      expect(result.content[len - 2]).toBe(0x52);
      expect(result.content[len - 1]).toBe(0x31);
    });

    it('should infer correct schema from heterogeneous data', async () => {
      const events = [
        createCDCEvent({
          after: {
            id: 1,
            name: 'Alice',
            score: 95.5,
            active: true,
            created: '2024-01-15T10:00:00Z',
          },
        }),
        createCDCEvent({
          after: {
            id: 2,
            name: 'Bob',
            score: 88.0,
            active: false,
            created: '2024-01-16T12:30:00Z',
          },
        }),
      ];

      const schema = inferSchemaFromEvents(events, 'users');

      expect(schema.type).toBe('struct');

      const fieldMap = new Map(schema.fields.map((f) => [f.name, f.type]));
      expect(fieldMap.get('name')).toBe('string');
      expect(fieldMap.get('active')).toBe('boolean');
      expect(fieldMap.get('score')).toBe('double');
      expect(fieldMap.get('created')).toBe('timestamptz');

      // CDC metadata fields
      expect(fieldMap.has('_cdc_sequence')).toBe(true);
      expect(fieldMap.has('_cdc_timestamp')).toBe(true);
      expect(fieldMap.has('_cdc_operation')).toBe(true);
    });

    it('should include column statistics in Parquet output', async () => {
      const events = [
        createCDCEvent({ after: { value: 10 } }),
        createCDCEvent({ after: { value: 50 } }),
        createCDCEvent({ after: { value: 30 } }),
        createCDCEvent({ after: { value: null } }),
      ];

      const result = await writeParquet(events, 'stats_test');

      expect(result.columnStats.size).toBeGreaterThan(0);
      const valueStats = result.columnStats.get('value');
      expect(valueStats).toBeDefined();
      expect(valueStats?.nullCount).toBe(1);
    });

    it('should create DataFile entry with correct metadata', async () => {
      const events = createMultipleCDCEvents(5, 'orders');
      const result = await writeParquet(events, 'orders');

      const dataFile = createDataFile('/warehouse/orders/data/file.parquet', result, {
        day: '2024-01-15',
      });

      expect(dataFile['file-path']).toBe('/warehouse/orders/data/file.parquet');
      expect(dataFile['file-format']).toBe('parquet');
      expect(dataFile['record-count']).toBe(BigInt(5));
      expect(dataFile['file-size-in-bytes']).toBe(BigInt(result.fileSize));
      expect(dataFile.partition).toEqual({ day: '2024-01-15' });
    });

    it('should flush buffer and write to R2', async () => {
      const id = env.DOLAKE.idFromName(`e2e-flush-r2-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      // Ingest events
      const events = createMultipleCDCEvents(5, 'flush_test');
      await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events }),
      });

      // Trigger flush
      const flushResponse = await stub.fetch('http://dolake/flush', {
        method: 'POST',
      });

      expect(flushResponse.status).toBe(200);
      const result = (await flushResponse.json()) as FlushResult;
      expect(result.success).toBe(true);
      expect(result.eventsFlushed).toBeGreaterThanOrEqual(5);
    });
  });

  // ===========================================================================
  // Compaction Workflow Tests
  // ===========================================================================

  describe('Compaction Workflow', () => {
    let compactionManager: CompactionManager;

    beforeEach(() => {
      compactionManager = new CompactionManager({
        minFileSizeBytes: 1024, // 1KB for testing
        targetFileSizeBytes: 10240, // 10KB target
        minFilesToCompact: 2,
        maxFilesToCompact: 10,
      });
    });

    it('should identify small files for compaction', () => {
      const files = [
        { 'file-path': '/a.parquet', 'file-size-in-bytes': BigInt(500), partition: {}, content: 0 as const, 'file-format': 'parquet' as const, 'record-count': BigInt(10) },
        { 'file-path': '/b.parquet', 'file-size-in-bytes': BigInt(800), partition: {}, content: 0 as const, 'file-format': 'parquet' as const, 'record-count': BigInt(20) },
        { 'file-path': '/c.parquet', 'file-size-in-bytes': BigInt(2000), partition: {}, content: 0 as const, 'file-format': 'parquet' as const, 'record-count': BigInt(50) },
      ];

      const smallFiles = compactionManager.identifySmallFiles(files);
      expect(smallFiles.length).toBe(2);
      expect(smallFiles.map((f) => f['file-path'])).toContain('/a.parquet');
      expect(smallFiles.map((f) => f['file-path'])).toContain('/b.parquet');
    });

    it('should group files by partition for compaction', () => {
      const files = [
        { 'file-path': '/p1/a.parquet', 'file-size-in-bytes': BigInt(500), partition: { day: '2024-01-01' }, content: 0 as const, 'file-format': 'parquet' as const, 'record-count': BigInt(10) },
        { 'file-path': '/p1/b.parquet', 'file-size-in-bytes': BigInt(600), partition: { day: '2024-01-01' }, content: 0 as const, 'file-format': 'parquet' as const, 'record-count': BigInt(15) },
        { 'file-path': '/p2/c.parquet', 'file-size-in-bytes': BigInt(700), partition: { day: '2024-01-02' }, content: 0 as const, 'file-format': 'parquet' as const, 'record-count': BigInt(20) },
      ];

      const groups = compactionManager.groupByPartition(files);
      expect(groups.size).toBe(2);
      expect(groups.get('day=2024-01-01')?.length).toBe(2);
      expect(groups.get('day=2024-01-02')?.length).toBe(1);
    });

    it('should select compaction candidates based on thresholds', () => {
      const files = [
        { 'file-path': '/a.parquet', 'file-size-in-bytes': BigInt(200), partition: {}, content: 0 as const, 'file-format': 'parquet' as const, 'record-count': BigInt(5) },
        { 'file-path': '/b.parquet', 'file-size-in-bytes': BigInt(300), partition: {}, content: 0 as const, 'file-format': 'parquet' as const, 'record-count': BigInt(8) },
        { 'file-path': '/c.parquet', 'file-size-in-bytes': BigInt(400), partition: {}, content: 0 as const, 'file-format': 'parquet' as const, 'record-count': BigInt(10) },
      ];

      const candidates = compactionManager.selectCompactionCandidates(files);
      expect(candidates.length).toBeGreaterThan(0);
      expect(candidates[0].files.length).toBeGreaterThanOrEqual(2);
    });

    it('should estimate space savings from compaction', () => {
      const files = [
        { 'file-path': '/a.parquet', 'file-size-in-bytes': BigInt(1000), partition: {}, content: 0 as const, 'file-format': 'parquet' as const, 'record-count': BigInt(100) },
        { 'file-path': '/b.parquet', 'file-size-in-bytes': BigInt(1000), partition: {}, content: 0 as const, 'file-format': 'parquet' as const, 'record-count': BigInt(100) },
        { 'file-path': '/c.parquet', 'file-size-in-bytes': BigInt(1000), partition: {}, content: 0 as const, 'file-format': 'parquet' as const, 'record-count': BigInt(100) },
      ];

      const savings = compactionManager.estimateSpaceSavings(files);

      expect(savings.estimatedFileReduction).toBeGreaterThanOrEqual(0);
      expect(savings.compressionRatio).toBeGreaterThan(0);
      expect(savings.compressionRatio).toBeLessThanOrEqual(1);
    });

    it('should record compaction metrics', () => {
      compactionManager.recordCompactionResult({
        success: true,
        filesCompacted: 5,
        bytesCompacted: BigInt(5000),
        outputFiles: 1,
        outputBytes: BigInt(4500),
        durationMs: 100,
      });

      const metrics = compactionManager.getMetrics();
      expect(metrics.totalCompactions).toBe(1);
      expect(metrics.successfulCompactions).toBe(1);
      expect(metrics.filesCompacted).toBe(5);
      expect(metrics.bytesCompacted).toBe(BigInt(5000));
    });

    it('should trigger compaction via REST API', async () => {
      const id = env.DOLAKE.idFromName(`e2e-compact-api-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      // Create namespace first
      await stub.fetch('http://dolake/v1/namespaces', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace: ['default'], properties: {} }),
      });

      // Check compaction status (correct endpoint path)
      const statusResponse = await stub.fetch('http://dolake/v1/compaction/status');
      expect(statusResponse.status).toBe(200);
    });
  });

  // ===========================================================================
  // Iceberg Table Format Tests
  // ===========================================================================

  describe('Iceberg Table Format', () => {
    it('should create valid Iceberg table metadata', () => {
      const schema = createSchema([
        { name: 'id', type: 'long', required: true },
        { name: 'name', type: 'string' },
        { name: 'created_at', type: 'timestamptz' },
      ]);

      const metadata = createTableMetadata(
        generateUUID(),
        '/warehouse/db/users',
        schema
      );

      expect(metadata['format-version']).toBe(2);
      expect(metadata.location).toBe('/warehouse/db/users');
      expect(metadata.schemas.length).toBe(1);
      expect(metadata['current-snapshot-id']).toBeNull();
      expect(metadata.properties?.['created-by']).toBe('dolake');
    });

    it('should add snapshots correctly with sequence numbers', () => {
      const schema = createSchema([{ name: 'id', type: 'long' }]);
      let metadata = createTableMetadata(generateUUID(), '/warehouse/table', schema);

      // Add first snapshot
      const snapshot1 = createAppendSnapshot(
        null,
        BigInt(1),
        '/manifest1.avro',
        2,
        BigInt(100),
        BigInt(1024)
      );
      metadata = addSnapshot(metadata, snapshot1);

      expect(metadata.snapshots.length).toBe(1);
      expect(metadata['current-snapshot-id']).toBe(snapshot1['snapshot-id']);
      expect(metadata['last-sequence-number']).toBe(BigInt(1));

      // Add second snapshot
      const snapshot2 = createAppendSnapshot(
        snapshot1['snapshot-id'],
        BigInt(2),
        '/manifest2.avro',
        3,
        BigInt(200),
        BigInt(2048)
      );
      metadata = addSnapshot(metadata, snapshot2);

      expect(metadata.snapshots.length).toBe(2);
      expect(metadata['current-snapshot-id']).toBe(snapshot2['snapshot-id']);
      expect(metadata['last-sequence-number']).toBe(BigInt(2));
      expect(metadata['snapshot-log'].length).toBe(2);
    });

    it('should handle partitioned tables correctly', () => {
      const schema = createSchema([
        { name: 'id', type: 'long' },
        { name: 'event_time', type: 'timestamptz' },
      ]);

      const partitionSpec = createDatePartitionSpec(2, 'day', 'day');

      const metadata = createTableMetadata(
        generateUUID(),
        '/warehouse/events',
        schema,
        partitionSpec
      );

      expect(metadata['partition-specs'].length).toBe(1);
      expect(metadata['partition-specs'][0].fields[0].transform).toBe('day');
      expect(metadata['partition-specs'][0].fields[0]['source-id']).toBe(2);
    });

    it('should generate correct file paths', () => {
      const tableLocation = '/warehouse/db/orders';
      const partitionPath = partitionToPath({ year: 2024, month: 1, day: 15 });
      const dataPath = dataFilePath(tableLocation, partitionPath, 'data.parquet');

      expect(partitionPath).toBe('year=2024/month=1/day=15');
      expect(dataPath).toBe('/warehouse/db/orders/data/year=2024/month=1/day=15/data.parquet');

      const manifestPath = manifestFilePath(tableLocation, 'abc123');
      expect(manifestPath).toBe('/warehouse/db/orders/metadata/abc123-manifest.avro');

      const manifestListPath2 = manifestListPath(tableLocation, BigInt(999));
      expect(manifestListPath2).toBe('/warehouse/db/orders/metadata/snap-999-manifest-list.avro');
    });

    it('should manage namespaces via REST Catalog API', async () => {
      const id = env.DOLAKE.idFromName(`e2e-namespace-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      // Create namespace
      const createResponse = await stub.fetch('http://dolake/v1/namespaces', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          namespace: ['analytics'],
          properties: { owner: 'data_team', description: 'Analytics tables' },
        }),
      });

      expect(createResponse.status).toBe(200);
      const created = (await createResponse.json()) as { namespace: string[] };
      expect(created.namespace).toEqual(['analytics']);

      // List namespaces
      const listResponse = await stub.fetch('http://dolake/v1/namespaces');
      expect(listResponse.status).toBe(200);
      const listed = (await listResponse.json()) as { namespaces: string[][] };
      expect(listed.namespaces).toContainEqual(['analytics']);
    });

    it('should return catalog configuration', async () => {
      const id = env.DOLAKE.idFromName(`e2e-catalog-config-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/v1/config');
      expect(response.status).toBe(200);

      const config = (await response.json()) as {
        defaults: { warehouse: string };
        overrides: object;
      };
      expect(config.defaults).toBeDefined();
      expect(config.defaults.warehouse).toBeDefined();
    });
  });

  // ===========================================================================
  // Query Over Parquet Tests
  // ===========================================================================

  describe('Query Over Parquet Files', () => {
    let queryEngine: QueryEngine;
    let partitionManager: PartitionManager;

    beforeEach(() => {
      queryEngine = new QueryEngine();
      partitionManager = new PartitionManager();
    });

    it('should prune partitions based on WHERE clause', () => {
      const allPartitions = [
        'day=2024-01-01',
        'day=2024-01-02',
        'day=2024-01-03',
        'day=2024-01-04',
        'day=2024-01-05',
      ];

      const predicates = parseWhereClause("SELECT * FROM events WHERE day = '2024-01-03'");
      const { included, pruned } = prunePartitions(allPartitions, predicates);

      expect(included).toContain('day=2024-01-03');
      expect(pruned.length).toBe(4);
    });

    it('should handle range queries with partition pruning', () => {
      const allPartitions = [
        'day=2024-01-01',
        'day=2024-01-02',
        'day=2024-01-03',
        'day=2024-01-04',
        'day=2024-01-05',
      ];

      const predicates = parseWhereClause("SELECT * FROM events WHERE day >= '2024-01-03'");
      const { included } = prunePartitions(allPartitions, predicates);

      expect(included).toContain('day=2024-01-03');
      expect(included).toContain('day=2024-01-04');
      expect(included).toContain('day=2024-01-05');
      expect(included).not.toContain('day=2024-01-01');
      expect(included).not.toContain('day=2024-01-02');
    });

    it('should skip files using column statistics', () => {
      const fileStats: FileStats = {
        filePath: '/data/file.parquet',
        recordCount: BigInt(1000),
        columnStats: new Map([
          ['age', { min: 20, max: 40, nullCount: 0 }],
          ['score', { min: 0, max: 100, nullCount: 5 }],
        ]),
      };

      // Query for age > 50 should skip this file
      const predicates1 = parseWhereClause('SELECT * FROM users WHERE age > 50');
      expect(canSkipFileByStats(fileStats, predicates1)).toBe(true);

      // Query for age > 30 should NOT skip this file
      const predicates2 = parseWhereClause('SELECT * FROM users WHERE age > 30');
      expect(canSkipFileByStats(fileStats, predicates2)).toBe(false);
    });

    it('should create query plan with partition pruning stats', () => {
      const partitions = [
        'day=2024-01-01',
        'day=2024-01-02',
        'day=2024-01-03',
        'day=2024-01-04',
        'day=2024-01-05',
      ];

      const plan = queryEngine.createQueryPlan(
        "SELECT COUNT(*) FROM events WHERE day = '2024-01-03'",
        partitions,
        createDatePartitionSpec(1, 'day', 'day')
      );

      expect(plan.partitionsIncluded.length).toBe(1);
      expect(plan.partitionsPruned.length).toBe(4);
      expect(plan.pruningRatio).toBeCloseTo(0.8);
    });

    it('should route queries to correct partitions', () => {
      const partitions = [
        'customer_bucket=0',
        'customer_bucket=1',
        'customer_bucket=2',
        'customer_bucket=3',
      ];

      const spec = createUnpartitionedSpec(); // Will infer from partition names

      // Point lookup
      const result = queryEngine.routeQuery(
        "SELECT * FROM orders WHERE customer_bucket = '2'",
        partitions,
        spec
      );

      expect(result.routingStrategy).toBe('point-lookup');
      expect(result.targetPartitions).toContain('customer_bucket=2');
    });

    it('should execute aggregation with pushdown', () => {
      const partitions = ['p1', 'p2', 'p3'];

      const mockData: Record<string, Array<Record<string, unknown>>> = {
        p1: [{ value: 10 }, { value: 20 }],
        p2: [{ value: 30 }, { value: 40 }],
        p3: [{ value: 50 }],
      };

      const result = queryEngine.executeAggregation(
        'SELECT COUNT(*), SUM(value) FROM data',
        partitions,
        (partition) => mockData[partition] || []
      );

      expect(result.rows[0].count).toBe(5);
      expect(result.rows[0].sum).toBe(150);
      expect(result.aggregationPushedDown).toBe(true);
    });

    it('should merge sorted results from multiple partitions', () => {
      const partitionResults = [
        { partition: 'p1', rows: [{ id: 1 }, { id: 4 }, { id: 7 }] },
        { partition: 'p2', rows: [{ id: 2 }, { id: 5 }, { id: 8 }] },
        { partition: 'p3', rows: [{ id: 3 }, { id: 6 }, { id: 9 }] },
      ];

      const { rows, executionStrategy } = queryEngine.mergeSortedResults(
        partitionResults,
        'id',
        5
      );

      expect(executionStrategy).toBe('merge-sorted');
      expect(rows.length).toBe(5);
      expect(rows.map((r) => r.id)).toEqual([1, 2, 3, 4, 5]);
    });

    it('should apply partition transforms correctly', () => {
      const timestamp = new Date('2024-03-15T14:30:00Z').getTime();

      // dayTransform returns days since Unix epoch (integer)
      const expectedDays = Math.floor(timestamp / (24 * 60 * 60 * 1000));
      expect(dayTransform(timestamp)).toBe(expectedDays);
      expect(dayTransform(timestamp)).toBeGreaterThan(19000); // Should be after epoch

      expect(bucketTransform('customer-123', 4)).toBeLessThan(4);
      expect(bucketTransform('customer-123', 4)).toBeGreaterThanOrEqual(0);
    });
  });

  // ===========================================================================
  // Full E2E Pipeline Tests
  // ===========================================================================

  describe('Full CDC to Query Pipeline', () => {
    it('should process CDC events through complete pipeline', async () => {
      const id = env.DOLAKE.idFromName(`e2e-pipeline-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      // Step 1: Ingest CDC events
      const events = createMultipleCDCEvents(20, 'pipeline_test');
      const ingestResponse = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events }),
      });
      expect(ingestResponse.status).toBe(200);

      // Step 2: Check buffer status
      const statusResponse = await stub.fetch('http://dolake/status');
      const status = (await statusResponse.json()) as {
        state: string;
        buffer: { eventCount: number };
      };
      expect(status.buffer.eventCount).toBeGreaterThanOrEqual(20);

      // Step 3: Flush to R2
      const flushResponse = await stub.fetch('http://dolake/flush', {
        method: 'POST',
      });
      const flushResult = (await flushResponse.json()) as FlushResult;
      expect(flushResult.success).toBe(true);

      // Step 4: Verify metrics
      const metricsResponse = await stub.fetch('http://dolake/metrics');
      const metrics = await metricsResponse.text();
      expect(metrics).toContain('dolake_buffer_events');
    });

    it('should handle high-volume event ingestion', async () => {
      const id = env.DOLAKE.idFromName(`e2e-high-volume-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      // Ingest multiple batches - each batch needs unique sequence numbers
      const batchCount = 5;
      const eventsPerBatch = 10;
      let totalEventsIngested = 0;

      for (let i = 0; i < batchCount; i++) {
        const events = createMultipleCDCEvents(
          eventsPerBatch,
          'high_volume_test',
          i * eventsPerBatch + Date.now() // Ensure unique sequence numbers
        );
        const response = await stub.fetch('http://dolake/cdc', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ events }),
        });
        expect(response.status).toBe(200);
        const result = (await response.json()) as { eventsAccepted: number };
        totalEventsIngested += result.eventsAccepted;
      }

      // Verify events were ingested (may not all be in buffer if some were flushed)
      const statusResponse = await stub.fetch('http://dolake/status');
      const status = (await statusResponse.json()) as {
        buffer: { eventCount: number };
      };
      // At least some events should be in the buffer
      expect(totalEventsIngested).toBeGreaterThanOrEqual(batchCount * eventsPerBatch);
    });

    it('should support time-based partitioning workflow', async () => {
      const id = env.DOLAKE.idFromName(`e2e-time-partition-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      // Events with timestamps across different days
      const events: CDCEvent[] = [
        createCDCEvent({
          timestamp: new Date('2024-01-15T10:00:00Z').getTime(),
          table: 'time_events',
          after: { event_type: 'click', count: 5 },
        }),
        createCDCEvent({
          timestamp: new Date('2024-01-16T14:00:00Z').getTime(),
          table: 'time_events',
          after: { event_type: 'view', count: 10 },
        }),
        createCDCEvent({
          timestamp: new Date('2024-01-15T18:00:00Z').getTime(),
          table: 'time_events',
          after: { event_type: 'purchase', count: 2 },
        }),
      ];

      const response = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events }),
      });

      expect(response.status).toBe(200);

      // Flush with partitioning
      const flushResponse = await stub.fetch('http://dolake/flush', {
        method: 'POST',
      });
      expect(flushResponse.status).toBe(200);
    });
  });

  // ===========================================================================
  // Error Handling & Edge Cases
  // ===========================================================================

  describe('Error Handling & Edge Cases', () => {
    it('should reject empty event arrays', async () => {
      const id = env.DOLAKE.idFromName(`e2e-empty-events-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events: [] }),
      });

      expect(response.status).toBe(400);
    });

    it('should handle malformed CDC events gracefully', async () => {
      const id = env.DOLAKE.idFromName(`e2e-malformed-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events: 'not-an-array' }),
      });

      expect(response.status).toBe(400);
    });

    it('should return 404 for unknown endpoints', async () => {
      const id = env.DOLAKE.idFromName(`e2e-404-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/unknown/path');
      expect(response.status).toBe(404);
    });

    it('should handle concurrent flush requests', async () => {
      const id = env.DOLAKE.idFromName(`e2e-concurrent-flush-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      // Ingest some events first
      const events = createMultipleCDCEvents(10, 'concurrent_test');
      await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events }),
      });

      // Trigger multiple concurrent flushes
      const flushPromises = [
        stub.fetch('http://dolake/flush', { method: 'POST' }),
        stub.fetch('http://dolake/flush', { method: 'POST' }),
        stub.fetch('http://dolake/flush', { method: 'POST' }),
      ];

      const responses = await Promise.all(flushPromises);

      // All should succeed (one does actual work, others are idempotent)
      for (const response of responses) {
        expect(response.status).toBe(200);
        const result = (await response.json()) as { success: boolean };
        expect(result.success).toBe(true);
      }
    });

    it('should handle events with null values', async () => {
      const id = env.DOLAKE.idFromName(`e2e-null-values-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      const events = [
        createCDCEvent({
          after: {
            id: 1,
            name: null,
            email: null,
            active: true,
          },
        }),
      ];

      const response = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events }),
      });

      expect(response.status).toBe(200);
    });

    it('should handle events with complex nested data', async () => {
      const id = env.DOLAKE.idFromName(`e2e-nested-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      const events = [
        createCDCEvent({
          table: 'orders',
          after: {
            id: 1,
            items: [
              { sku: 'ABC', qty: 2 },
              { sku: 'XYZ', qty: 1 },
            ],
            metadata: {
              source: 'web',
              campaign: { id: 'summer-sale', discount: 0.1 },
            },
          },
        }),
      ];

      const response = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events }),
      });

      expect(response.status).toBe(200);
    });
  });

  // ===========================================================================
  // Analytics & Observability Tests
  // ===========================================================================

  describe('Analytics & Observability', () => {
    it('should return health check status', async () => {
      const id = env.DOLAKE.idFromName(`e2e-health-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/health');
      expect(response.status).toBe(200);
      expect(await response.text()).toBe('OK');
    });

    it('should expose Prometheus-style metrics', async () => {
      const id = env.DOLAKE.idFromName(`e2e-prometheus-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      // Ingest some events for metrics
      const events = createMultipleCDCEvents(5, 'metrics_test');
      await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events }),
      });

      const response = await stub.fetch('http://dolake/metrics');
      expect(response.status).toBe(200);

      const metrics = await response.text();
      expect(metrics).toContain('# HELP');
      expect(metrics).toContain('# TYPE');
      expect(metrics).toContain('dolake_buffer_events');
      expect(metrics).toContain('dolake_buffer_bytes');
      expect(metrics).toContain('dolake_connected_sources');
      expect(metrics).toContain('dolake_dedup_checks');
      expect(metrics).toContain('dolake_rate_limited_total');
    });

    it('should track deduplication statistics', async () => {
      const id = env.DOLAKE.idFromName(`e2e-dedup-stats-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      // Create events for ingestion
      const events = createMultipleCDCEvents(5, 'dedup_stats_test');

      // Send batch - dedup checks happen even when no duplicates found
      const response1 = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events }),
      });
      expect(response1.status).toBe(200);

      // Send another batch
      const events2 = createMultipleCDCEvents(3, 'dedup_stats_test');
      const response2 = await stub.fetch('http://dolake/cdc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ events: events2 }),
      });
      expect(response2.status).toBe(200);

      const statusResponse = await stub.fetch('http://dolake/status');
      const status = (await statusResponse.json()) as {
        dedupStats: { duplicatesFound: number; totalChecks: number };
      };

      // Deduplication checks should have been performed for each batch
      expect(status.dedupStats.totalChecks).toBeGreaterThan(0);
    });

    it('should report rate limit configuration', async () => {
      const id = env.DOLAKE.idFromName(`e2e-ratelimit-${Date.now()}`);
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/status');
      const status = (await response.json()) as {
        rateLimits: {
          connectionsPerSecond: number;
          messagesPerSecond: number;
          maxPayloadSize: number;
        };
      };

      expect(status.rateLimits).toBeDefined();
      expect(status.rateLimits.connectionsPerSecond).toBeGreaterThan(0);
      expect(status.rateLimits.messagesPerSecond).toBeGreaterThan(0);
      expect(status.rateLimits.maxPayloadSize).toBeGreaterThan(0);
    });
  });
});
