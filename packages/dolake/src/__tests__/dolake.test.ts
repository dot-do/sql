/**
 * DoLake Comprehensive Tests
 *
 * Tests for the DoLake lakehouse component using workers-vitest-pool.
 * NO MOCKS - tests run against real Cloudflare Workers runtime.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import {
  type CDCEvent,
  type CDCBatchMessage,
  type IcebergSchema,
  CDCBufferManager,
  generateBatchId,
  generateCorrelationId,
  generateUUID,
  DEFAULT_DOLAKE_CONFIG,
  DEFAULT_DEDUP_CONFIG,
  DEFAULT_CLIENT_CAPABILITIES,
} from '../index.js';
import {
  inferSchemaFromEvents,
  eventsToRows,
  writeParquet,
  createDataFile,
} from '../parquet.js';
import {
  createSchema,
  createUnpartitionedSpec,
  createIdentityPartitionSpec,
  createDatePartitionSpec,
  createUnsortedOrder,
  createSortOrder,
  createTableMetadata,
  addSnapshot,
  createAppendSnapshot,
  metadataFilePath,
  manifestListPath,
  dataFilePath,
  partitionToPath,
} from '../iceberg.js';

// =============================================================================
// Test Utilities
// =============================================================================

function createCDCEvent(overrides: Partial<CDCEvent> = {}): CDCEvent {
  return {
    sequence: 1,
    timestamp: Date.now(),
    operation: 'INSERT',
    table: 'users',
    rowId: generateUUID(),
    after: { id: 1, name: 'Test User', email: 'test@example.com' },
    ...overrides,
  };
}

// =============================================================================
// Types Tests
// =============================================================================

describe('Types', () => {
  describe('CDCEvent', () => {
    it('should create a valid INSERT event', () => {
      const event = createCDCEvent({
        operation: 'INSERT',
        after: { id: 1, name: 'Test' },
      });
      expect(event.operation).toBe('INSERT');
      expect(event.after).toEqual({ id: 1, name: 'Test' });
      expect(event.before).toBeUndefined();
    });

    it('should create a valid UPDATE event', () => {
      const event = createCDCEvent({
        operation: 'UPDATE',
        before: { id: 1, name: 'Old' },
        after: { id: 1, name: 'New' },
      });
      expect(event.operation).toBe('UPDATE');
      expect(event.before).toEqual({ id: 1, name: 'Old' });
      expect(event.after).toEqual({ id: 1, name: 'New' });
    });

    it('should create a valid DELETE event', () => {
      const event = createCDCEvent({
        operation: 'DELETE',
        before: { id: 1, name: 'Deleted' },
        after: undefined,
      });
      expect(event.operation).toBe('DELETE');
      expect(event.before).toEqual({ id: 1, name: 'Deleted' });
      expect(event.after).toBeUndefined();
    });
  });

  describe('Utility Functions', () => {
    it('should generate unique batch IDs', () => {
      const id1 = generateBatchId('source1', 1);
      const id2 = generateBatchId('source1', 2);
      const id3 = generateBatchId('source2', 1);
      expect(id1).not.toBe(id2);
      expect(id1).not.toBe(id3);
    });

    it('should generate unique correlation IDs', () => {
      const ids = new Set<string>();
      for (let i = 0; i < 100; i++) {
        ids.add(generateCorrelationId());
      }
      expect(ids.size).toBe(100);
    });

    it('should generate valid UUIDs', () => {
      const uuid = generateUUID();
      expect(uuid).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    });
  });
});

// =============================================================================
// Buffer Manager Tests
// =============================================================================

describe('CDCBufferManager', () => {
  let buffer: CDCBufferManager;

  beforeEach(() => {
    buffer = new CDCBufferManager();
  });

  describe('Batch Management', () => {
    it('should add a batch successfully', () => {
      const events = [createCDCEvent()];
      const result = buffer.addBatch('source1', events, 1);
      expect(result.added).toBe(true);
      expect(result.isDuplicate).toBe(false);
    });

    it('should detect duplicate batches', () => {
      const events = [createCDCEvent()];
      buffer.addBatch('source1', events, 1);
      const result = buffer.addBatch('source1', events, 1);
      expect(result.added).toBe(false);
      expect(result.isDuplicate).toBe(true);
    });

    it('should track batch statistics', () => {
      const events1 = [createCDCEvent({ sequence: 1 })];
      const events2 = [createCDCEvent({ sequence: 2 }), createCDCEvent({ sequence: 3 })];

      buffer.addBatch('source1', events1, 1);
      buffer.addBatch('source1', events2, 2);

      const stats = buffer.getStats();
      expect(stats.batchCount).toBe(2);
      expect(stats.eventCount).toBe(3);
    });

    it('should group events by table', () => {
      const event1 = createCDCEvent({ table: 'users', sequence: 1 });
      const event2 = createCDCEvent({ table: 'orders', sequence: 2 });
      const event3 = createCDCEvent({ table: 'users', sequence: 3 });

      buffer.addBatch('source1', [event1, event2, event3], 1);

      const byTable = buffer.getEventsByTable();
      expect(byTable.get('users')?.length).toBe(2);
      expect(byTable.get('orders')?.length).toBe(1);
    });

    it('should distribute events to partition buffers', () => {
      const event1 = createCDCEvent({ table: 'users', metadata: { partition: 'p1' } });
      const event2 = createCDCEvent({ table: 'users', metadata: { partition: 'p2' } });

      buffer.addBatch('source1', [event1, event2], 1);

      const partitionBuffers = buffer.getPartitionBuffersForFlush();
      expect(partitionBuffers.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Source State Management', () => {
    it('should track source connection state', () => {
      buffer.updateSourceState('source1', 10, 5, 'shard1');
      const state = buffer.getSourceStates().get('source1');

      expect(state?.sourceDoId).toBe('source1');
      expect(state?.sourceShardName).toBe('shard1');
      expect(state?.lastReceivedSequence).toBe(5);
    });

    it('should update existing source state', () => {
      buffer.updateSourceState('source1', 5, 1);
      buffer.updateSourceState('source1', 10, 2);

      const state = buffer.getSourceStates().get('source1');
      expect(state?.eventsReceived).toBe(15);
      expect(state?.batchesReceived).toBe(2);
    });
  });

  describe('Deduplication', () => {
    it('should deduplicate within window', () => {
      const dedupStats = buffer.getDedupStats();
      expect(dedupStats.duplicatesFound).toBe(0);

      buffer.addBatch('source1', [createCDCEvent()], 1);
      buffer.addBatch('source1', [createCDCEvent()], 1); // Duplicate

      const statsAfter = buffer.getDedupStats();
      expect(statsAfter.duplicatesFound).toBe(1);
    });

    it('should track dedup statistics', () => {
      for (let i = 0; i < 10; i++) {
        buffer.addBatch('source1', [createCDCEvent()], i);
      }
      // Add some duplicates
      buffer.addBatch('source1', [createCDCEvent()], 5);
      buffer.addBatch('source1', [createCDCEvent()], 7);

      const stats = buffer.getDedupStats();
      expect(stats.totalChecks).toBeGreaterThan(0);
      expect(stats.duplicatesFound).toBe(2);
    });
  });

  describe('Flush Management', () => {
    it('should trigger flush on event threshold', () => {
      const smallConfig = { ...DEFAULT_DOLAKE_CONFIG, flushThresholdEvents: 5 };
      const bufferWithConfig = new CDCBufferManager(smallConfig);

      for (let i = 0; i < 6; i++) {
        bufferWithConfig.addBatch('source1', [createCDCEvent()], i);
      }

      const trigger = bufferWithConfig.shouldFlush();
      expect(trigger).toBe('threshold_events');
    });

    it('should get batches for flush', () => {
      buffer.addBatch('source1', [createCDCEvent()], 1);
      buffer.addBatch('source1', [createCDCEvent()], 2);

      const batches = buffer.getBatchesForFlush();
      expect(batches.length).toBe(2);
    });

    it('should mark batches as persisted', () => {
      buffer.addBatch('source1', [createCDCEvent()], 1);
      const batches = buffer.getBatchesForFlush();
      const batchIds = batches.map(b => b.batchId);

      buffer.markPersisted(batchIds);
      buffer.clearPersisted();

      const remainingBatches = buffer.getBatchesForFlush();
      expect(remainingBatches.length).toBe(0);
    });

    it('should sort events by timestamp', () => {
      const event1 = createCDCEvent({ timestamp: 1000 });
      const event3 = createCDCEvent({ timestamp: 3000 });
      const event2 = createCDCEvent({ timestamp: 2000 });

      buffer.addBatch('source1', [event3, event1], 1);
      buffer.addBatch('source1', [event2], 2);

      const sorted = buffer.getAllEventsSorted();
      expect(sorted[0].timestamp).toBe(1000);
      expect(sorted[1].timestamp).toBe(2000);
      expect(sorted[2].timestamp).toBe(3000);
    });
  });

  describe('Serialization', () => {
    it('should serialize and restore buffer state', () => {
      buffer.addBatch('source1', [createCDCEvent()], 1);
      buffer.addBatch('source2', [createCDCEvent()], 1);
      buffer.updateSourceState('source1', 0, 1, 'shard1');

      const snapshot = buffer.serialize();
      const restored = CDCBufferManager.restore(snapshot);

      expect(restored.getStats().batchCount).toBe(2);
      expect(restored.getSourceStates().get('source1')?.sourceShardName).toBe('shard1');
    });
  });
});

// =============================================================================
// Parquet Writer Tests
// =============================================================================

describe('Parquet Writer', () => {
  describe('Schema Inference', () => {
    it('should infer schema from events', () => {
      const events = [
        createCDCEvent({
          after: { id: 1, name: 'Test', active: true },
        }),
      ];

      const schema = inferSchemaFromEvents(events, 'users');
      expect(schema.type).toBe('struct');
      expect(schema.fields.length).toBeGreaterThan(0);

      // Should have CDC metadata fields
      const fieldNames = schema.fields.map(f => f.name);
      expect(fieldNames).toContain('_cdc_sequence');
      expect(fieldNames).toContain('_cdc_timestamp');
      expect(fieldNames).toContain('_cdc_operation');
    });

    it('should infer correct types', () => {
      const events = [
        createCDCEvent({
          after: {
            id: 123,
            name: 'Test',
            active: true,
            score: 3.14,
            created: '2024-01-15T10:00:00Z',
          },
        }),
      ];

      const schema = inferSchemaFromEvents(events, 'test');
      const fieldMap = new Map(schema.fields.map(f => [f.name, f.type]));

      // Data fields
      expect(fieldMap.get('name')).toBe('string');
      expect(fieldMap.get('active')).toBe('boolean');
      expect(['int', 'long', 'double']).toContain(fieldMap.get('id'));
      expect(fieldMap.get('score')).toBe('double');
      expect(fieldMap.get('created')).toBe('timestamptz');
    });

    it('should handle null values', () => {
      const events = [
        createCDCEvent({
          after: { id: 1, name: null },
        }),
      ];

      const schema = inferSchemaFromEvents(events, 'test');
      const nameField = schema.fields.find(f => f.name === 'name');
      expect(nameField).toBeDefined();
    });
  });

  describe('Row Conversion', () => {
    it('should convert events to rows', () => {
      const events = [
        createCDCEvent({
          sequence: 1,
          operation: 'INSERT',
          after: { id: 1, name: 'Test' },
        }),
      ];

      const schema = inferSchemaFromEvents(events, 'test');
      const rows = eventsToRows(events, schema);

      expect(rows.length).toBe(1);
      expect(rows[0]._cdc_sequence).toBe(1);
      expect(rows[0]._cdc_operation).toBe('INSERT');
      expect(rows[0].id).toBeDefined();
      expect(rows[0].name).toBeDefined();
    });

    it('should handle multiple events', () => {
      const events = [
        createCDCEvent({ sequence: 1, after: { id: 1 } }),
        createCDCEvent({ sequence: 2, after: { id: 2 } }),
        createCDCEvent({ sequence: 3, after: { id: 3 } }),
      ];

      const schema = inferSchemaFromEvents(events, 'test');
      const rows = eventsToRows(events, schema);

      expect(rows.length).toBe(3);
    });
  });

  describe('Parquet Writing', () => {
    it('should write valid Parquet file', async () => {
      const events = [
        createCDCEvent({ after: { id: 1, name: 'User 1' } }),
        createCDCEvent({ after: { id: 2, name: 'User 2' } }),
      ];

      const result = await writeParquet(events, 'users');

      expect(result.rowCount).toBe(2);
      expect(result.fileSize).toBeGreaterThan(0);
      expect(result.content).toBeInstanceOf(Uint8Array);

      // Check Parquet magic bytes
      expect(result.content[0]).toBe(0x50); // P
      expect(result.content[1]).toBe(0x41); // A
      expect(result.content[2]).toBe(0x52); // R
      expect(result.content[3]).toBe(0x31); // 1
    });

    it('should include column statistics', async () => {
      const events = [
        createCDCEvent({ after: { value: 10 } }),
        createCDCEvent({ after: { value: 50 } }),
        createCDCEvent({ after: { value: 30 } }),
      ];

      const result = await writeParquet(events, 'test');

      expect(result.columnStats.size).toBeGreaterThan(0);
    });

    it('should reject empty event list', async () => {
      await expect(writeParquet([], 'test')).rejects.toThrow('Cannot write empty event list');
    });
  });

  describe('DataFile Creation', () => {
    it('should create valid DataFile entry', async () => {
      const events = [createCDCEvent({ after: { id: 1 } })];
      const result = await writeParquet(events, 'test');

      const dataFile = createDataFile('/path/to/file.parquet', result, {});

      expect(dataFile['file-path']).toBe('/path/to/file.parquet');
      expect(dataFile['file-format']).toBe('parquet');
      expect(dataFile['record-count']).toBe(BigInt(1));
      expect(dataFile['file-size-in-bytes']).toBe(BigInt(result.fileSize));
    });
  });
});

// =============================================================================
// Iceberg Metadata Tests
// =============================================================================

describe('Iceberg Metadata', () => {
  describe('Path Utilities', () => {
    it('should generate correct metadata file path', () => {
      const path = metadataFilePath('/warehouse/db/table', 1);
      expect(path).toBe('/warehouse/db/table/metadata/v1.metadata.json');
    });

    it('should generate correct manifest list path', () => {
      const path = manifestListPath('/warehouse/db/table', BigInt(12345));
      expect(path).toBe('/warehouse/db/table/metadata/snap-12345-manifest-list.avro');
    });

    it('should generate correct data file path without partition', () => {
      const path = dataFilePath('/warehouse/db/table', null, 'data.parquet');
      expect(path).toBe('/warehouse/db/table/data/data.parquet');
    });

    it('should generate correct data file path with partition', () => {
      const path = dataFilePath('/warehouse/db/table', 'dt=2024-01-15', 'data.parquet');
      expect(path).toBe('/warehouse/db/table/data/dt=2024-01-15/data.parquet');
    });

    it('should convert partition to path', () => {
      const path = partitionToPath({ year: 2024, month: 1 });
      expect(path).toBe('year=2024/month=1');
    });

    it('should handle empty partition', () => {
      const path = partitionToPath({});
      expect(path).toBeNull();
    });
  });

  describe('Schema Builders', () => {
    it('should create a schema', () => {
      const schema = createSchema([
        { name: 'id', type: 'long', required: true },
        { name: 'name', type: 'string' },
      ]);

      expect(schema.type).toBe('struct');
      expect(schema.fields.length).toBe(2);
      expect(schema.fields[0].id).toBe(1);
      expect(schema.fields[1].id).toBe(2);
    });

    it('should set identifier field IDs', () => {
      const schema = createSchema(
        [{ name: 'id', type: 'long', required: true }],
        0,
        [1]
      );

      expect(schema['identifier-field-ids']).toEqual([1]);
    });
  });

  describe('Partition Spec Builders', () => {
    it('should create unpartitioned spec', () => {
      const spec = createUnpartitionedSpec();
      expect(spec['spec-id']).toBe(0);
      expect(spec.fields.length).toBe(0);
    });

    it('should create identity partition spec', () => {
      const spec = createIdentityPartitionSpec([
        { sourceId: 1, name: 'region' },
      ]);

      expect(spec.fields.length).toBe(1);
      expect(spec.fields[0].transform).toBe('identity');
      expect(spec.fields[0]['source-id']).toBe(1);
    });

    it('should create date partition spec', () => {
      const spec = createDatePartitionSpec(2, 'day', 'day');
      expect(spec.fields[0].transform).toBe('day');
      expect(spec.fields[0]['source-id']).toBe(2);
    });
  });

  describe('Sort Order Builders', () => {
    it('should create unsorted order', () => {
      const order = createUnsortedOrder();
      expect(order['order-id']).toBe(0);
      expect(order.fields.length).toBe(0);
    });

    it('should create sort order', () => {
      const order = createSortOrder([
        { sourceId: 1, direction: 'asc' },
        { sourceId: 2, direction: 'desc' },
      ]);

      expect(order.fields.length).toBe(2);
      expect(order.fields[0].direction).toBe('asc');
      expect(order.fields[0]['null-order']).toBe('nulls-first');
      expect(order.fields[1].direction).toBe('desc');
      expect(order.fields[1]['null-order']).toBe('nulls-last');
    });
  });

  describe('Table Metadata', () => {
    it('should create initial table metadata', () => {
      const schema = createSchema([
        { name: 'id', type: 'long', required: true },
        { name: 'name', type: 'string' },
      ]);

      const metadata = createTableMetadata(
        generateUUID(),
        '/warehouse/db/table',
        schema
      );

      expect(metadata['format-version']).toBe(2);
      expect(metadata.location).toBe('/warehouse/db/table');
      expect(metadata.schemas.length).toBe(1);
      expect(metadata['current-snapshot-id']).toBeNull();
      expect(metadata.snapshots.length).toBe(0);
    });

    it('should add snapshot to metadata', () => {
      const schema = createSchema([{ name: 'id', type: 'long' }]);
      const metadata = createTableMetadata(
        generateUUID(),
        '/warehouse/db/table',
        schema
      );

      const snapshot = createAppendSnapshot(
        null,
        BigInt(1),
        '/path/to/manifest-list.avro',
        1,
        BigInt(100),
        BigInt(1024)
      );

      const updated = addSnapshot(metadata, snapshot);

      expect(updated.snapshots.length).toBe(1);
      expect(updated['current-snapshot-id']).toBe(snapshot['snapshot-id']);
      expect(updated['last-sequence-number']).toBe(BigInt(1));
    });

    it('should maintain snapshot log', () => {
      const schema = createSchema([{ name: 'id', type: 'long' }]);
      let metadata = createTableMetadata(
        generateUUID(),
        '/warehouse/db/table',
        schema
      );

      // Add multiple snapshots
      for (let i = 1; i <= 3; i++) {
        const snapshot = createAppendSnapshot(
          metadata['current-snapshot-id'],
          BigInt(i),
          `/path/to/manifest-list-${i}.avro`,
          1,
          BigInt(100 * i),
          BigInt(1024 * i)
        );
        metadata = addSnapshot(metadata, snapshot);
      }

      expect(metadata.snapshots.length).toBe(3);
      expect(metadata['snapshot-log'].length).toBe(3);
    });

    it('should update refs on snapshot', () => {
      const schema = createSchema([{ name: 'id', type: 'long' }]);
      const metadata = createTableMetadata(
        generateUUID(),
        '/warehouse/db/table',
        schema
      );

      const snapshot = createAppendSnapshot(
        null,
        BigInt(1),
        '/path/to/manifest-list.avro',
        1,
        BigInt(100),
        BigInt(1024)
      );

      const updated = addSnapshot(metadata, snapshot);

      expect(updated.refs?.main?.['snapshot-id']).toBe(snapshot['snapshot-id']);
      expect(updated.refs?.main?.type).toBe('branch');
    });
  });

  describe('Snapshot Builder', () => {
    it('should create append snapshot', () => {
      const snapshot = createAppendSnapshot(
        null,
        BigInt(1),
        '/path/to/manifest-list.avro',
        5,
        BigInt(1000),
        BigInt(10240)
      );

      expect(snapshot['parent-snapshot-id']).toBeNull();
      expect(snapshot['manifest-list']).toBe('/path/to/manifest-list.avro');
      expect(snapshot.summary.operation).toBe('append');
      expect(snapshot.summary['added-data-files']).toBe('5');
      expect(snapshot.summary['added-records']).toBe('1000');
    });

    it('should set parent snapshot ID', () => {
      const parentId = BigInt(12345);
      const snapshot = createAppendSnapshot(
        parentId,
        BigInt(2),
        '/path/to/manifest-list.avro',
        1,
        BigInt(100),
        BigInt(1024)
      );

      expect(snapshot['parent-snapshot-id']).toBe(parentId);
    });
  });
});

// =============================================================================
// Configuration Tests
// =============================================================================

describe('Configuration', () => {
  it('should have sensible default config values', () => {
    expect(DEFAULT_DOLAKE_CONFIG.flushThresholdEvents).toBe(10000);
    expect(DEFAULT_DOLAKE_CONFIG.flushThresholdBytes).toBe(32 * 1024 * 1024);
    expect(DEFAULT_DOLAKE_CONFIG.flushThresholdMs).toBe(60_000);
    expect(DEFAULT_DOLAKE_CONFIG.enableDeduplication).toBe(true);
  });

  it('should have sensible dedup config', () => {
    expect(DEFAULT_DEDUP_CONFIG.enabled).toBe(true);
    expect(DEFAULT_DEDUP_CONFIG.windowMs).toBe(300_000);
  });

  it('should have sensible client capabilities', () => {
    expect(DEFAULT_CLIENT_CAPABILITIES.binaryProtocol).toBe(true);
    expect(DEFAULT_CLIENT_CAPABILITIES.batching).toBe(true);
    expect(DEFAULT_CLIENT_CAPABILITIES.maxBatchSize).toBe(1000);
  });
});

// =============================================================================
// Integration Tests (with Durable Object)
// =============================================================================

describe('DoLake Integration', () => {
  describe('Health Check', () => {
    it('should respond to health check', async () => {
      const id = env.DOLAKE.idFromName('test-health');
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/health');
      expect(response.status).toBe(200);

      const text = await response.text();
      expect(text).toBe('OK');
    });
  });

  describe('Status Endpoint', () => {
    it('should return status', async () => {
      const id = env.DOLAKE.idFromName('test-status');
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/status');
      expect(response.status).toBe(200);

      const status = await response.json() as { state: string; buffer: object; connectedSources: number };
      expect(status.state).toBeDefined();
      expect(status.buffer).toBeDefined();
      expect(status.connectedSources).toBeDefined();
    });
  });

  describe('Metrics Endpoint', () => {
    it('should return Prometheus metrics', async () => {
      const id = env.DOLAKE.idFromName('test-metrics');
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/metrics');
      expect(response.status).toBe(200);

      const metrics = await response.text();
      expect(metrics).toContain('dolake_buffer_events');
      expect(metrics).toContain('dolake_connected_sources');
    });
  });

  describe('Manual Flush', () => {
    it('should handle flush request', async () => {
      const id = env.DOLAKE.idFromName('test-flush');
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/flush', {
        method: 'POST',
      });
      expect(response.status).toBe(200);

      const result = await response.json() as { success: boolean };
      expect(result.success).toBe(true);
    });
  });

  describe('REST Catalog - Config', () => {
    it('should return catalog config', async () => {
      const id = env.DOLAKE.idFromName('test-catalog-config');
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/v1/config');
      expect(response.status).toBe(200);

      const config = await response.json() as { defaults: { warehouse: string } };
      expect(config.defaults).toBeDefined();
      expect(config.defaults.warehouse).toBeDefined();
    });
  });

  describe('REST Catalog - Namespaces', () => {
    it('should list namespaces (empty)', async () => {
      const id = env.DOLAKE.idFromName('test-list-ns-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/v1/namespaces');
      expect(response.status).toBe(200);

      const result = await response.json() as { namespaces: string[][] };
      expect(result.namespaces).toBeDefined();
      expect(Array.isArray(result.namespaces)).toBe(true);
    });

    it('should create namespace', async () => {
      const id = env.DOLAKE.idFromName('test-create-ns-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/v1/namespaces', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          namespace: ['test_db'],
          properties: { owner: 'test' },
        }),
      });
      expect(response.status).toBe(200);

      const result = await response.json() as { namespace: string[]; properties: { owner: string } };
      expect(result.namespace).toEqual(['test_db']);
      expect(result.properties.owner).toBe('test');
    });
  });

});
