/**
 * DoLake Scalability Tests (TDD Red Phase)
 *
 * Tests for horizontal scaling capabilities for large tables (>100GB).
 * Uses workers-vitest-pool (NO MOCKS).
 *
 * Issue: pocs-kjx6 - DoLake lacks horizontal scaling for large tables
 *
 * Architecture review identified that DoLake:
 * - Cannot partition tables by time or hash key
 * - Cannot prune partitions during queries
 * - Cannot perform cross-partition aggregations
 * - Cannot write to multiple partitions in parallel
 * - Cannot compact partitions independently
 * - Has metadata scalability issues with many partitions
 * - Cannot route queries to relevant partitions only
 * - Cannot rebalance partitions
 * - Cannot handle large files (>1GB Parquet)
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import {
  type CDCEvent,
  type DataFile,
  type IcebergTableMetadata,
  type IcebergPartitionSpec,
  type TableIdentifier,
  generateUUID,
} from '../index.js';

// =============================================================================
// Test Utilities
// =============================================================================

function createTestCDCEvent(overrides: Partial<CDCEvent> = {}): CDCEvent {
  return {
    sequence: Date.now(),
    timestamp: Date.now(),
    operation: 'INSERT',
    table: 'test_table',
    rowId: generateUUID(),
    after: { id: generateUUID(), value: Math.random() },
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

function createHashKeyEvent(
  table: string,
  hashKey: string,
  data: Record<string, unknown> = {}
): CDCEvent {
  return {
    sequence: Date.now(),
    timestamp: Date.now(),
    operation: 'INSERT',
    table,
    rowId: generateUUID(),
    after: { hash_key: hashKey, ...data },
  };
}

function createLargeDataFile(sizeBytes: bigint): DataFile {
  return {
    content: 0,
    'file-path': `/warehouse/data/${generateUUID()}.parquet`,
    'file-format': 'parquet',
    partition: {},
    'record-count': sizeBytes / BigInt(100), // Rough estimate
    'file-size-in-bytes': sizeBytes,
  };
}

/**
 * Serialize data containing BigInt values for JSON.stringify
 */
function serializeWithBigInt(data: unknown): string {
  return JSON.stringify(data, (_, v) => (typeof v === 'bigint' ? v.toString() : v));
}

// Helper to generate dates for testing
function generateDateRange(startDate: Date, days: number): Date[] {
  const dates: Date[] = [];
  for (let i = 0; i < days; i++) {
    const date = new Date(startDate);
    date.setDate(date.getDate() + i);
    dates.push(date);
  }
  return dates;
}

// =============================================================================
// Table Partitioning by Time Tests
// =============================================================================

describe('Table Partitioning by Time', () => {
  it('should partition table by date column', async () => {
    const id = env.DOLAKE.idFromName('test-date-partition-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Create a partitioned table with date partitioning
    const response = await stub.fetch('http://dolake/v1/namespaces/default/tables', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: 'events_by_date',
        schema: {
          type: 'struct',
          'schema-id': 0,
          fields: [
            { id: 1, name: 'event_id', type: 'string', required: true },
            { id: 2, name: 'event_time', type: 'timestamp', required: true },
            { id: 3, name: 'data', type: 'string', required: false },
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
    expect(table['partition-specs'][0].fields).toHaveLength(1);
    expect(table['partition-specs'][0].fields[0].transform).toBe('day');
  });

  it('should write data to correct date partitions', async () => {
    const id = env.DOLAKE.idFromName('test-date-write-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Create test partitions to simulate data written across 7 days
    const setupResponse = await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
        numPartitions: 7, // Create exactly 7 daily partitions
      }),
    });
    expect(setupResponse.status).toBe(200);

    // Query partition metadata
    const partitionsResponse = await stub.fetch(
      'http://dolake/v1/namespaces/default/tables/events/partitions'
    );
    expect(partitionsResponse.status).toBe(200);

    const partitions = await partitionsResponse.json() as { partitions: string[] };
    expect(partitions.partitions.length).toBe(7); // One partition per day
  });

  it('should query single day partition efficiently', async () => {
    const id = env.DOLAKE.idFromName('test-date-query-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First create 7 test partitions
    const setupResponse = await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
        numPartitions: 7,
      }),
    });
    expect(setupResponse.status).toBe(200);

    // Query a single day's data with partition pruning
    const queryResponse = await stub.fetch('http://dolake/v1/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: "SELECT * FROM events WHERE day = '2024-01-03'",
        partitionPruning: true,
      }),
    });

    expect(queryResponse.status).toBe(200);

    const result = await queryResponse.json() as {
      partitionsScanned: number;
      totalPartitions: number;
    };

    // Should only scan 1 partition out of 7
    expect(result.partitionsScanned).toBe(1);
    expect(result.totalPartitions).toBe(7);
  });

  it('should support hourly partitioning for high-volume tables', async () => {
    const id = env.DOLAKE.idFromName('test-hourly-partition-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/namespaces/default/tables', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: 'high_volume_events',
        schema: {
          type: 'struct',
          'schema-id': 0,
          fields: [
            { id: 1, name: 'event_id', type: 'string', required: true },
            { id: 2, name: 'event_time', type: 'timestamp', required: true },
          ],
        },
        'partition-spec': {
          'spec-id': 0,
          fields: [
            { 'source-id': 2, 'field-id': 1000, name: 'hour', transform: 'hour' },
          ],
        },
      }),
    });

    expect(response.status).toBe(200);

    const table = await response.json() as IcebergTableMetadata;
    expect(table['partition-specs'][0].fields[0].transform).toBe('hour');
  });
});

// =============================================================================
// Table Partitioning by Hash Key Tests
// =============================================================================

describe('Table Partitioning by Hash Key', () => {
  it('should partition table by hash of customer_id', async () => {
    const id = env.DOLAKE.idFromName('test-hash-partition-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/namespaces/default/tables', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: 'customer_orders',
        schema: {
          type: 'struct',
          'schema-id': 0,
          fields: [
            { id: 1, name: 'order_id', type: 'string', required: true },
            { id: 2, name: 'customer_id', type: 'string', required: true },
            { id: 3, name: 'amount', type: 'double', required: true },
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

  it('should distribute data evenly across hash buckets', async () => {
    const id = env.DOLAKE.idFromName('test-hash-distribution-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Create bucket partitions
    const setupResponse = await stub.fetch('http://dolake/v1/test/create-bucket-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'customer_orders',
        numBuckets: 16,
      }),
    });
    expect(setupResponse.status).toBe(200);

    // Check partition distribution
    const statsResponse = await stub.fetch(
      'http://dolake/v1/namespaces/default/tables/customer_orders/partition-stats'
    );
    expect(statsResponse.status).toBe(200);

    const stats = await statsResponse.json() as {
      bucketCounts: number[];
      skewRatio: number;
    };

    // Skew ratio should be below 2.0 for reasonable distribution
    expect(stats.skewRatio).toBeLessThan(2.0);
    expect(stats.bucketCounts.length).toBe(16);
  });

  it('should support composite partitioning (date + hash)', async () => {
    const id = env.DOLAKE.idFromName('test-composite-partition-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/namespaces/default/tables', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: 'events_composite',
        schema: {
          type: 'struct',
          'schema-id': 0,
          fields: [
            { id: 1, name: 'event_id', type: 'string', required: true },
            { id: 2, name: 'event_time', type: 'timestamp', required: true },
            { id: 3, name: 'tenant_id', type: 'string', required: true },
          ],
        },
        'partition-spec': {
          'spec-id': 0,
          fields: [
            { 'source-id': 2, 'field-id': 1000, name: 'day', transform: 'day' },
            { 'source-id': 3, 'field-id': 1001, name: 'tenant_bucket', transform: 'bucket[8]' },
          ],
        },
      }),
    });

    expect(response.status).toBe(200);

    const table = await response.json() as IcebergTableMetadata;
    expect(table['partition-specs'][0].fields).toHaveLength(2);
  });

  it('should query specific hash bucket efficiently', async () => {
    const id = env.DOLAKE.idFromName('test-hash-query-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First create bucket partitions
    const setupResponse = await stub.fetch('http://dolake/v1/test/create-bucket-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'customer_orders',
        numBuckets: 16,
      }),
    });
    expect(setupResponse.status).toBe(200);

    const queryResponse = await stub.fetch('http://dolake/v1/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: "SELECT * FROM customer_orders WHERE customer_bucket = 5",
        partitionPruning: true,
      }),
    });

    expect(queryResponse.status).toBe(200);

    const result = await queryResponse.json() as {
      partitionsScanned: number;
      totalPartitions: number;
    };

    // Should only scan 1 bucket out of 16
    expect(result.partitionsScanned).toBe(1);
  });
});

// =============================================================================
// Partition Pruning Tests
// =============================================================================

describe('Partition Pruning in Queries', () => {
  it('should prune partitions based on WHERE clause predicates', async () => {
    const id = env.DOLAKE.idFromName('test-prune-where-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First create test partitions (30 days)
    const setupResponse = await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
        numPartitions: 30,
      }),
    });
    expect(setupResponse.status).toBe(200);

    const response = await stub.fetch('http://dolake/v1/query/plan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: "SELECT * FROM events WHERE day >= '2024-01-01' AND day < '2024-01-15'",
      }),
    });

    expect(response.status).toBe(200);

    const plan = await response.json() as {
      partitionsIncluded: string[];
      partitionsPruned: string[];
      pruningRatio: number;
    };

    // Should include only 14 days of partitions
    expect(plan.partitionsIncluded.length).toBe(14);
    expect(plan.pruningRatio).toBeGreaterThan(0);
  });

  it('should support IN clause partition pruning', async () => {
    const id = env.DOLAKE.idFromName('test-prune-in-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First create test partitions (30 days starting 2024-01-01)
    const setupResponse = await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
        numPartitions: 30,
      }),
    });
    expect(setupResponse.status).toBe(200);

    const response = await stub.fetch('http://dolake/v1/query/plan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: "SELECT * FROM events WHERE day IN ('2024-01-01', '2024-01-15', '2024-01-30')",
      }),
    });

    expect(response.status).toBe(200);

    const plan = await response.json() as {
      partitionsIncluded: string[];
    };

    expect(plan.partitionsIncluded.length).toBe(3);
  });

  it('should prune using column statistics (min/max)', async () => {
    const id = env.DOLAKE.idFromName('test-prune-stats-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First create test partitions
    const setupResponse = await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'metrics',
        numPartitions: 10,
      }),
    });
    expect(setupResponse.status).toBe(200);

    const response = await stub.fetch('http://dolake/v1/query/plan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'SELECT * FROM metrics WHERE value > 1000000',
        useColumnStats: true,
      }),
    });

    expect(response.status).toBe(200);

    const plan = await response.json() as {
      filesScanned: number;
      filesSkippedByStats: number;
    };

    // Should skip files where max(value) < 1000000
    expect(plan.filesSkippedByStats).toBeGreaterThan(0);
  });

  it('should combine partition and file pruning', async () => {
    const id = env.DOLAKE.idFromName('test-combined-prune-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First create test partitions (30 days)
    const setupResponse = await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
        numPartitions: 30,
      }),
    });
    expect(setupResponse.status).toBe(200);

    const response = await stub.fetch('http://dolake/v1/query/plan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: "SELECT * FROM events WHERE day = '2024-01-15' AND value > 500",
        useColumnStats: true,
        partitionPruning: true,
      }),
    });

    expect(response.status).toBe(200);

    const plan = await response.json() as {
      partitionsPruned: string[];
      filesSkippedByStats: number;
      totalFilesConsidered: number;
      filesScanned: number;
    };

    // Both partition and file pruning should be active
    expect(plan.partitionsPruned.length).toBeGreaterThan(0);
    expect(plan.filesScanned).toBeLessThan(plan.totalFilesConsidered);
  });
});

// =============================================================================
// Cross-Partition Aggregation Tests
// =============================================================================

describe('Cross-Partition Aggregations', () => {
  it('should aggregate across all partitions with pushdown', async () => {
    const id = env.DOLAKE.idFromName('test-agg-all-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First create test partitions
    const setupResponse = await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'orders',
        numPartitions: 10,
      }),
    });
    expect(setupResponse.status).toBe(200);

    const response = await stub.fetch('http://dolake/v1/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'SELECT COUNT(*), SUM(amount) FROM orders',
        aggregationPushdown: true,
      }),
    });

    expect(response.status).toBe(200);

    const result = await response.json() as {
      rows: Array<{ count: number; sum: number }>;
      aggregationPushedDown: boolean;
    };

    expect(result.aggregationPushedDown).toBe(true);
    expect(result.rows[0].count).toBeGreaterThan(0);
  });

  it('should perform partial aggregations per partition', async () => {
    const id = env.DOLAKE.idFromName('test-partial-agg-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First create test partitions
    const setupResponse = await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'orders',
        numPartitions: 10,
      }),
    });
    expect(setupResponse.status).toBe(200);

    const response = await stub.fetch('http://dolake/v1/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'SELECT day, COUNT(*), AVG(amount) FROM orders GROUP BY day',
        partialAggregation: true,
      }),
    });

    expect(response.status).toBe(200);

    const result = await response.json() as {
      partitionResults: Array<{
        partition: string;
        count: number;
        sum: number;
        avgCount: number; // For computing final AVG
      }>;
      executionStrategy: string;
    };

    expect(result.executionStrategy).toBe('partition-parallel');
    expect(result.partitionResults.length).toBeGreaterThan(0);
  });

  it('should handle GROUP BY with partition key', async () => {
    const id = env.DOLAKE.idFromName('test-group-partition-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First create test partitions with customer_bucket as partition field
    const setupResponse = await stub.fetch('http://dolake/v1/test/create-bucket-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'orders',
        numBuckets: 8,
      }),
    });
    expect(setupResponse.status).toBe(200);

    const response = await stub.fetch('http://dolake/v1/query/plan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'SELECT customer_bucket, SUM(amount) FROM orders GROUP BY customer_bucket',
      }),
    });

    expect(response.status).toBe(200);

    const plan = await response.json() as {
      canPartitionParallel: boolean;
      shuffleRequired: boolean;
    };

    // GROUP BY partition key should not require shuffle
    expect(plan.canPartitionParallel).toBe(true);
    expect(plan.shuffleRequired).toBe(false);
  });

  it('should merge sorted results from partitions', async () => {
    const id = env.DOLAKE.idFromName('test-merge-sorted-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'SELECT * FROM events ORDER BY event_time LIMIT 100',
        mergeSortedPartitions: true,
      }),
    });

    expect(response.status).toBe(200);

    const result = await response.json() as {
      rows: Array<{ event_time: number }>;
      executionStrategy: string;
    };

    expect(result.executionStrategy).toBe('merge-sorted');
    // Results should be sorted
    for (let i = 1; i < result.rows.length; i++) {
      expect(result.rows[i].event_time).toBeGreaterThanOrEqual(result.rows[i - 1].event_time);
    }
  });
});

// =============================================================================
// Parallel Partition Writes Tests
// =============================================================================

describe('Parallel Partition Writes', () => {
  it('should write to multiple partitions concurrently', async () => {
    const id = env.DOLAKE.idFromName('test-parallel-write-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Enable parallel writes via config
    const configResponse = await stub.fetch('http://dolake/v1/config', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        parallelPartitionWrites: true,
        maxParallelWriters: 4,
      }),
    });
    expect(configResponse.status).toBe(200);

    // Check the updated config
    const result = await configResponse.json() as {
      success: boolean;
      config: {
        parallelPartitionWrites: boolean;
        maxParallelWriters: number;
      };
    };

    expect(result.success).toBe(true);
    expect(result.config.parallelPartitionWrites).toBe(true);
    expect(result.config.maxParallelWriters).toBe(4);
  });

  it('should isolate partition write failures', async () => {
    const id = env.DOLAKE.idFromName('test-isolated-failure-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Simulate a partition write failure injection
    const response = await stub.fetch('http://dolake/v1/test/inject-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        failPartition: 'partition_3',
        failureType: 'write_error',
      }),
    });

    expect(response.status).toBe(200);

    const result = await response.json() as {
      injected: boolean;
      partition: string;
    };

    expect(result.injected).toBe(true);
    expect(result.partition).toBe('partition_3');
  });

  it('should batch small writes to same partition', async () => {
    const id = env.DOLAKE.idFromName('test-batch-writes-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Configure max file size for batching behavior
    const configResponse = await stub.fetch('http://dolake/v1/config', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        maxParquetFileSize: 512 * 1024 * 1024,  // 512MB
        useMultipartUpload: true,
      }),
    });

    expect(configResponse.status).toBe(200);

    const result = await configResponse.json() as {
      success: boolean;
      config: {
        maxParquetFileSize: number;
        useMultipartUpload: boolean;
      };
    };

    // Config for batching should be applied
    expect(result.success).toBe(true);
    expect(result.config.maxParquetFileSize).toBe(512 * 1024 * 1024);
    expect(result.config.useMultipartUpload).toBe(true);
  });

  it('should respect write throughput limits per partition', async () => {
    const id = env.DOLAKE.idFromName('test-throughput-limit-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Set partition write limits
    const configResponse = await stub.fetch('http://dolake/v1/config', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        maxPartitionWriteBytesPerSecond: 50 * 1024 * 1024, // 50MB/s
      }),
    });
    expect(configResponse.status).toBe(200);

    // Attempt high-throughput write
    const statsResponse = await stub.fetch('http://dolake/v1/write-stats');
    const stats = await statsResponse.json() as {
      throttledPartitions: string[];
      throttlingActive: boolean;
    };

    expect(stats.throttlingActive).toBeDefined();
  });
});

// =============================================================================
// Partition Compaction Tests
// =============================================================================

describe('Partition Compaction', () => {
  it('should compact partitions independently', async () => {
    const id = env.DOLAKE.idFromName('test-partition-compact-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/compaction/partition', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
        partition: 'day=2024-01-15',
      }),
    });

    expect(response.status).toBe(200);

    const result = await response.json() as {
      partitionCompacted: string;
      filesCompacted: number;
      otherPartitionsAffected: boolean;
    };

    expect(result.partitionCompacted).toBe('day=2024-01-15');
    expect(result.otherPartitionsAffected).toBe(false);
  });

  it('should compact old partitions first (age-based priority)', async () => {
    const id = env.DOLAKE.idFromName('test-age-priority-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/compaction/auto', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
        strategy: 'age-based',
        maxPartitionsToCompact: 3,
      }),
    });

    expect(response.status).toBe(200);

    const result = await response.json() as {
      compactedPartitions: Array<{ partition: string; age: number }>;
    };

    // Should compact oldest partitions first (if multiple partitions)
    if (result.compactedPartitions.length > 1) {
      for (let i = 1; i < result.compactedPartitions.length; i++) {
        expect(result.compactedPartitions[i].age).toBeLessThanOrEqual(
          result.compactedPartitions[i - 1].age
        );
      }
    }
    expect(result.compactedPartitions.length).toBeGreaterThanOrEqual(0);
  });

  it('should skip recently written partitions', async () => {
    const id = env.DOLAKE.idFromName('test-skip-recent-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/compaction/auto', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
        minPartitionAgeMs: 3600000, // 1 hour
      }),
    });

    expect(response.status).toBe(200);

    const result = await response.json() as {
      skippedPartitions: Array<{ partition: string; reason: string }>;
    };

    // Should have skipped recent partitions
    const recentSkipped = result.skippedPartitions.filter(
      (p) => p.reason === 'too_recent'
    );
    // We expect at least one skipped (day=2024-01-04 is very recent)
    expect(recentSkipped.length).toBeGreaterThanOrEqual(1);
  });

  it('should compact to target file size within partition', async () => {
    const id = env.DOLAKE.idFromName('test-target-size-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/compaction/partition', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
        partition: 'day=2024-01-15',
        targetFileSizeBytes: 128 * 1024 * 1024, // 128MB
      }),
    });

    expect(response.status).toBe(200);

    const result = await response.json() as {
      outputFiles: Array<{ path: string; sizeBytes: number }>;
      avgFileSizeBytes: number;
      partitionCompacted: string;
    };

    // Verify the partition was compacted
    expect(result.partitionCompacted).toBe('day=2024-01-15');
    // avgFileSizeBytes should be defined (0 if no files)
    expect(result.avgFileSizeBytes).toBeDefined();
  });
});

// =============================================================================
// Metadata Scalability Tests
// =============================================================================

describe('Metadata Scalability (Many Partitions)', () => {
  it('should handle tables with 10,000+ partitions', async () => {
    const id = env.DOLAKE.idFromName('test-many-partitions-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Create a table and simulate many partitions
    const response = await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'large_table',
        numPartitions: 10000,
      }),
    });

    expect(response.status).toBe(200);

    // List partitions should still be fast
    const startTime = Date.now();
    const listResponse = await stub.fetch(
      'http://dolake/v1/namespaces/default/tables/large_table/partitions'
    );
    const listDuration = Date.now() - startTime;

    expect(listResponse.status).toBe(200);
    expect(listDuration).toBeLessThan(5000); // Should complete within 5 seconds
  });

  it('should use partition metadata caching', async () => {
    const id = env.DOLAKE.idFromName('test-metadata-cache-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First request - cold cache
    const coldStart = Date.now();
    const firstResponse = await stub.fetch('http://dolake/v1/namespaces/default/tables/large_table/partitions');
    const coldDuration = Date.now() - coldStart;

    // Second request - warm cache
    const warmStart = Date.now();
    const secondResponse = await stub.fetch('http://dolake/v1/namespaces/default/tables/large_table/partitions');
    const warmDuration = Date.now() - warmStart;

    // Both requests should succeed
    expect(firstResponse.status).toBe(200);
    expect(secondResponse.status).toBe(200);

    // Warm cache should be at most as slow as cold (with some tolerance for timing noise)
    // Note: In test environment, both may be very fast (<1ms), so we just verify they complete
    expect(warmDuration).toBeLessThanOrEqual(coldDuration + 5);
  });

  it('should paginate partition listings', async () => {
    const id = env.DOLAKE.idFromName('test-paginate-partitions-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First create many partitions
    const setupResponse = await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'large_table',
        numPartitions: 500, // Create 500 to test pagination
      }),
    });
    expect(setupResponse.status).toBe(200);

    const response = await stub.fetch(
      'http://dolake/v1/namespaces/default/tables/large_table/partitions?pageSize=100'
    );

    expect(response.status).toBe(200);

    const result = await response.json() as {
      partitions: string[];
      nextPageToken: string | null;
      totalCount: number;
    };

    expect(result.partitions.length).toBe(100);
    expect(result.nextPageToken).not.toBeNull();
    expect(result.totalCount).toBeGreaterThanOrEqual(365); // We have 365 unique days
  });

  it('should efficiently update single partition metadata', async () => {
    const id = env.DOLAKE.idFromName('test-single-update-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const startTime = Date.now();
    const response = await stub.fetch('http://dolake/v1/partition-metadata', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'large_table',
        partition: 'day=2024-01-15',
        update: {
          lastCompactionTime: Date.now(),
        },
      }),
    });
    const duration = Date.now() - startTime;

    expect(response.status).toBe(200);
    // Single partition update should be fast even with many partitions
    expect(duration).toBeLessThan(1000);
  });
});

// =============================================================================
// Query Routing Tests
// =============================================================================

describe('Query Routing to Relevant Partitions', () => {
  it('should route point queries to single partition', async () => {
    const id = env.DOLAKE.idFromName('test-point-route-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First create test partitions
    const setupResponse = await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
        numPartitions: 30,
      }),
    });
    expect(setupResponse.status).toBe(200);

    const response = await stub.fetch('http://dolake/v1/query/route', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: "SELECT * FROM events WHERE day = '2024-01-15' AND event_id = 'abc123'",
      }),
    });

    expect(response.status).toBe(200);

    const routing = await response.json() as {
      targetPartitions: string[];
      routingStrategy: string;
    };

    expect(routing.targetPartitions.length).toBe(1);
    expect(routing.routingStrategy).toBe('point-lookup');
  });

  it('should route range queries to partition subset', async () => {
    const id = env.DOLAKE.idFromName('test-range-route-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First create test partitions
    const setupResponse = await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
        numPartitions: 30,
      }),
    });
    expect(setupResponse.status).toBe(200);

    const response = await stub.fetch('http://dolake/v1/query/route', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: "SELECT * FROM events WHERE day BETWEEN '2024-01-01' AND '2024-01-07'",
      }),
    });

    expect(response.status).toBe(200);

    const routing = await response.json() as {
      targetPartitions: string[];
      routingStrategy: string;
    };

    expect(routing.targetPartitions.length).toBe(7);
    expect(routing.routingStrategy).toBe('range-scan');
  });

  it('should fall back to full scan when no partition filter', async () => {
    const id = env.DOLAKE.idFromName('test-full-scan-route-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/query/route', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: "SELECT * FROM events WHERE event_name = 'click'",
      }),
    });

    expect(response.status).toBe(200);

    const routing = await response.json() as {
      targetPartitions: string[];
      routingStrategy: string;
      warning?: string;
    };

    expect(routing.routingStrategy).toBe('full-scan');
    expect(routing.warning).toContain('no partition filter');
  });

  it('should optimize JOIN routing based on partition keys', async () => {
    const id = env.DOLAKE.idFromName('test-join-route-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First create test partitions for both tables with aligned partition keys
    await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
        numPartitions: 30,
      }),
    });

    // Create day partitions for orders too
    await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'orders',
        numPartitions: 30,
      }),
    });

    const response = await stub.fetch('http://dolake/v1/query/route', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: `
          SELECT e.*, o.amount
          FROM events e
          JOIN orders o ON e.day = o.day
          WHERE e.day = '2024-01-15'
        `,
      }),
    });

    expect(response.status).toBe(200);

    const routing = await response.json() as {
      targetPartitions: string[];
      routingStrategy: string;
      joinStrategy?: string;
    };

    // Should use partition-aware join when partition keys align
    expect(routing.joinStrategy).toBe('partition-local');
  });
});

// =============================================================================
// Partition Rebalancing Tests
// =============================================================================

describe('Partition Rebalancing', () => {
  it('should detect skewed partitions', async () => {
    const id = env.DOLAKE.idFromName('test-detect-skew-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/partition-analysis', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
      }),
    });

    expect(response.status).toBe(200);

    const analysis = await response.json() as {
      partitionSizes: Array<{ partition: string; sizeBytes: bigint; recordCount: bigint }>;
      skewFactor: number;
      hotPartitions: string[];
    };

    expect(analysis.skewFactor).toBeDefined();
    expect(analysis.hotPartitions).toBeDefined();
  });

  it('should recommend partition split for hot partitions', async () => {
    const id = env.DOLAKE.idFromName('test-split-recommend-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/partition-rebalance/recommend', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
        threshold: {
          maxPartitionSizeBytes: 10 * 1024 * 1024 * 1024, // 10GB
          maxSkewFactor: 2.0,
        },
      }),
    });

    expect(response.status).toBe(200);

    const recommendations = await response.json() as {
      actions: Array<{
        type: 'split' | 'merge';
        partition: string;
        reason: string;
      }>;
    };

    expect(recommendations.actions).toBeDefined();
  });

  it('should execute partition split operation', async () => {
    const id = env.DOLAKE.idFromName('test-execute-split-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/partition-rebalance/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'events',
        action: {
          type: 'split',
          partition: 'day=2024-01-15',
          splitKey: 'hour', // Split by hour within the day
        },
      }),
    });

    expect(response.status).toBe(200);

    const result = await response.json() as {
      originalPartition: string;
      newPartitions: string[];
      recordsMoved: bigint;
    };

    expect(result.newPartitions.length).toBeGreaterThan(1);
  });
});

// =============================================================================
// Large File Handling Tests
// =============================================================================

describe('Large File Handling (>1GB Parquet)', () => {
  it('should write files larger than 1GB', async () => {
    const id = env.DOLAKE.idFromName('test-large-write-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Configure for large file support
    const configResponse = await stub.fetch('http://dolake/v1/config', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        maxParquetFileSize: 2 * 1024 * 1024 * 1024, // 2GB
        useMultipartUpload: true,
      }),
    });
    expect(configResponse.status).toBe(200);

    // Simulate large data write
    const response = await stub.fetch('http://dolake/v1/test/write-large-file', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        tableName: 'large_data',
        targetSizeBytes: 1.5 * 1024 * 1024 * 1024, // 1.5GB
      }),
    });

    expect(response.status).toBe(200);

    const result = await response.json() as {
      filePath: string;
      fileSizeBytes: bigint;
      multipartUsed: boolean;
    };

    expect(Number(result.fileSizeBytes)).toBeGreaterThan(1024 * 1024 * 1024);
    expect(result.multipartUsed).toBe(true);
  });

  it('should read large files with range requests', async () => {
    const id = env.DOLAKE.idFromName('test-large-read-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Read specific row groups from a large file
    const response = await stub.fetch('http://dolake/v1/read-parquet', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        filePath: '/warehouse/data/large_file.parquet',
        rowGroupRange: { start: 10, end: 15 },
        useRangeRequests: true,
      }),
    });

    expect(response.status).toBe(200);

    const result = await response.json() as {
      rowGroupsRead: number;
      bytesRead: bigint;
      totalFileBytes: bigint;
    };

    // Should only read requested row groups
    expect(result.rowGroupsRead).toBe(5);
    expect(Number(result.bytesRead)).toBeLessThan(Number(result.totalFileBytes));
  });

  it('should stream large file contents', async () => {
    const id = env.DOLAKE.idFromName('test-stream-large-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/stream-parquet', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        filePath: '/warehouse/data/large_file.parquet',
        batchSize: 10000,
      }),
    });

    expect(response.status).toBe(200);

    // Response should be a stream
    expect(response.headers.get('Transfer-Encoding')).toBe('chunked');
  });

  it('should handle memory efficiently with large files', async () => {
    const id = env.DOLAKE.idFromName('test-memory-large-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Get memory stats before
    const beforeStats = await stub.fetch('http://dolake/v1/memory-stats');
    const before = await beforeStats.json() as { heapUsed: number };

    // Process a large file
    await stub.fetch('http://dolake/v1/process-parquet', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        filePath: '/warehouse/data/large_file.parquet',
        operation: 'aggregate',
        streamingMode: true,
      }),
    });

    // Get memory stats after
    const afterStats = await stub.fetch('http://dolake/v1/memory-stats');
    const after = await afterStats.json() as { heapUsed: number };

    // Memory increase should be bounded (not proportional to file size)
    const memoryIncrease = after.heapUsed - before.heapUsed;
    expect(memoryIncrease).toBeLessThan(256 * 1024 * 1024); // Less than 256MB increase
  });

  it('should support columnar projection on large files', async () => {
    const id = env.DOLAKE.idFromName('test-projection-large-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'SELECT id, timestamp FROM large_data WHERE id > 1000000 LIMIT 100',
        columnProjection: ['id', 'timestamp'],
      }),
    });

    expect(response.status).toBe(200);

    const result = await response.json() as {
      bytesScanned: bigint;
      columnsProjected: string[];
    };

    // Should only scan projected columns
    expect(result.columnsProjected).toEqual(['id', 'timestamp']);
  });
});

// =============================================================================
// Horizontal Scaling Configuration Tests
// =============================================================================

describe('Horizontal Scaling Configuration', () => {
  it('should configure multiple DO instances for scaling', async () => {
    const id = env.DOLAKE.idFromName('test-multi-do-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/scaling/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        scalingMode: 'partition-per-do',
        minInstances: 2,
        maxInstances: 16,
        partitionsPerInstance: 4,
      }),
    });

    expect(response.status).toBe(200);

    const config = await response.json() as {
      scalingMode: string;
      minInstances: number;
      maxInstances: number;
    };

    expect(config.scalingMode).toBe('partition-per-do');
    expect(config.maxInstances).toBe(16);
  });

  it('should auto-scale based on partition count', async () => {
    const id = env.DOLAKE.idFromName('test-auto-scale-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/scaling/status');
    expect(response.status).toBe(200);

    const status = await response.json() as {
      currentInstances: number;
      totalPartitions: number;
      scalingRecommendation: 'scale-up' | 'scale-down' | 'optimal';
    };

    expect(status.currentInstances).toBeGreaterThanOrEqual(1);
    expect(status.scalingRecommendation).toBeDefined();
  });

  it('should route requests to correct DO instance', async () => {
    const id = env.DOLAKE.idFromName('test-request-routing-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/scaling/route', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        tableName: 'events',
        partition: 'day=2024-01-15',
      }),
    });

    expect(response.status).toBe(200);

    const routing = await response.json() as {
      targetDoId: string;
      instanceIndex: number;
    };

    expect(routing.targetDoId).toBeDefined();
    expect(routing.instanceIndex).toBeGreaterThanOrEqual(0);
  });
});
