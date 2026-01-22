/**
 * Warm Tier Cache Invalidation Tests (TDD RED Phase)
 *
 * Tests for R2 cache invalidation in the warm tier storage layer.
 * Uses workers-vitest-pool (NO MOCKS).
 *
 * Issue: sql-lfy - Warm Tier Cache Invalidation
 *
 * Architecture review identified that DoLake's warm tier cache:
 * - Does not invalidate R2 cache on table UPDATE operations
 * - Does not invalidate R2 cache on table INSERT operations
 * - Does not invalidate R2 cache on table DELETE operations
 * - Does not propagate invalidation across replicas
 * - Does not support partial cache invalidation for specific partitions
 * - Does not properly handle cache TTL expiry
 * - Does not batch multiple invalidation requests efficiently
 * - Does not prevent stale reads after invalidation
 *
 * These tests use `it.fails()` pattern to document expected behavior that is currently MISSING.
 * When using it.fails():
 * - Test PASSES in vitest = the inner assertions FAILED = behavior is MISSING (RED phase)
 * - Test FAILS in vitest = the inner assertions PASSED = behavior already exists
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import {
  type CDCEvent,
  type DataFile,
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

/**
 * Serialize data containing BigInt values for JSON.stringify
 */
function serializeWithBigInt(data: unknown): string {
  return JSON.stringify(data, (_, v) => (typeof v === 'bigint' ? v.toString() : v));
}

/**
 * Interface for cache invalidation configuration
 * This documents the expected API that should exist
 */
interface CacheInvalidationConfig {
  enabled: boolean;
  batchSize: number;
  batchDelayMs: number;
  ttlMs: number;
  propagateToReplicas: boolean;
}

/**
 * Interface for cache invalidation result
 */
interface CacheInvalidationResult {
  success: boolean;
  keysInvalidated: string[];
  replicasNotified: number;
  durationMs: number;
}

/**
 * Interface for cache status
 */
interface CacheStatus {
  hitRate: number;
  missRate: number;
  staleReads: number;
  invalidationsPending: number;
  lastInvalidationTime: number;
}

// =============================================================================
// 1. R2 Cache Invalidation on UPDATE Tests
// =============================================================================

describe('R2 Cache Invalidation on UPDATE [RED]', () => {
  it.fails('should invalidate R2 cache when table UPDATE occurs', async () => {
    // GAP: Cache is not invalidated on UPDATE operations
    const id = env.DOLAKE.idFromName('test-cache-update-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Create a namespace and table
    await stub.fetch('http://dolake/v1/namespaces', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['cache_test'],
        properties: {},
      }),
    });

    // First, insert a record to populate cache
    const insertEvent = createTestCDCEvent({
      operation: 'INSERT',
      table: 'cache_test_table',
      rowId: 'row_001',
      after: { id: 'row_001', value: 100 },
    });

    await stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events: [insertEvent] }),
    });

    // Flush to R2
    await stub.fetch('http://dolake/flush', { method: 'POST' });

    // Read to populate cache
    const readResponse1 = await stub.fetch(
      'http://dolake/v1/query?sql=SELECT%20*%20FROM%20cache_test_table%20WHERE%20id=%27row_001%27'
    );
    const readResult1 = await readResponse1.json() as { rows: Array<{ value: number }> };
    expect(readResult1.rows[0].value).toBe(100);

    // Now update the record
    const updateEvent = createTestCDCEvent({
      operation: 'UPDATE',
      table: 'cache_test_table',
      rowId: 'row_001',
      before: { id: 'row_001', value: 100 },
      after: { id: 'row_001', value: 200 },
    });

    await stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events: [updateEvent] }),
    });

    // Flush to R2
    await stub.fetch('http://dolake/flush', { method: 'POST' });

    // Read again - should see updated value (cache should be invalidated)
    const readResponse2 = await stub.fetch(
      'http://dolake/v1/query?sql=SELECT%20*%20FROM%20cache_test_table%20WHERE%20id=%27row_001%27'
    );
    const readResult2 = await readResponse2.json() as { rows: Array<{ value: number }> };

    // This assertion will FAIL because cache is not invalidated
    expect(readResult2.rows[0].value).toBe(200);
  });

  it.fails('should track cache invalidation metrics on UPDATE', async () => {
    // GAP: No metrics for cache invalidation events
    const id = env.DOLAKE.idFromName('test-cache-update-metrics-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Get cache metrics before
    const beforeMetrics = await stub.fetch('http://dolake/v1/cache/metrics');
    const before = await beforeMetrics.json() as CacheStatus;

    // Perform an UPDATE operation
    const updateEvent = createTestCDCEvent({
      operation: 'UPDATE',
      table: 'metrics_test_table',
      rowId: 'row_001',
      before: { id: 'row_001', value: 100 },
      after: { id: 'row_001', value: 200 },
    });

    await stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events: [updateEvent] }),
    });

    await stub.fetch('http://dolake/flush', { method: 'POST' });

    // Get cache metrics after
    const afterMetrics = await stub.fetch('http://dolake/v1/cache/metrics');
    expect(afterMetrics.status).toBe(200);

    const after = await afterMetrics.json() as CacheStatus;

    // Should have recorded the invalidation
    expect(after.lastInvalidationTime).toBeGreaterThan(before.lastInvalidationTime || 0);
  });
});

// =============================================================================
// 2. R2 Cache Invalidation on INSERT Tests
// =============================================================================

describe('R2 Cache Invalidation on INSERT [RED]', () => {
  it.fails('should invalidate R2 cache when table INSERT occurs', async () => {
    // GAP: Cache is not invalidated on INSERT operations
    const id = env.DOLAKE.idFromName('test-cache-insert-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Query to populate empty result in cache
    const readResponse1 = await stub.fetch(
      'http://dolake/v1/query?sql=SELECT%20COUNT(*)%20FROM%20insert_test_table'
    );
    const readResult1 = await readResponse1.json() as { rows: Array<{ count: number }> };
    const countBefore = readResult1.rows[0]?.count || 0;

    // Insert a new record
    const insertEvent = createTestCDCEvent({
      operation: 'INSERT',
      table: 'insert_test_table',
      rowId: 'new_row_001',
      after: { id: 'new_row_001', value: 42 },
    });

    await stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events: [insertEvent] }),
    });

    // Flush to R2
    await stub.fetch('http://dolake/flush', { method: 'POST' });

    // Query again - should see the new record (cache should be invalidated)
    const readResponse2 = await stub.fetch(
      'http://dolake/v1/query?sql=SELECT%20COUNT(*)%20FROM%20insert_test_table'
    );
    const readResult2 = await readResponse2.json() as { rows: Array<{ count: number }> };

    // This assertion will FAIL because cache is not invalidated
    expect(readResult2.rows[0].count).toBe(countBefore + 1);
  });

  it.fails('should invalidate partition-specific cache on INSERT', async () => {
    // GAP: Partition-specific cache invalidation not implemented
    const id = env.DOLAKE.idFromName('test-cache-insert-partition-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Insert into specific partition
    const insertEvent = createTestCDCEvent({
      operation: 'INSERT',
      table: 'partitioned_table',
      rowId: 'row_001',
      after: { id: 'row_001', date: '2024-01-15', value: 100 },
    });

    await stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events: [insertEvent] }),
    });

    await stub.fetch('http://dolake/flush', { method: 'POST' });

    // Check that only the affected partition was invalidated
    const cacheStatus = await stub.fetch('http://dolake/v1/cache/partitions/partitioned_table');
    expect(cacheStatus.status).toBe(200);

    const status = await cacheStatus.json() as {
      invalidatedPartitions: string[];
      unchangedPartitions: string[];
    };

    // Only date=2024-01-15 partition should be invalidated
    expect(status.invalidatedPartitions).toContain('date=2024-01-15');
    expect(status.unchangedPartitions.length).toBeGreaterThan(0);
  });
});

// =============================================================================
// 3. R2 Cache Invalidation on DELETE Tests
// =============================================================================

describe('R2 Cache Invalidation on DELETE [RED]', () => {
  it.fails('should invalidate R2 cache when table DELETE occurs', async () => {
    // GAP: Cache is not invalidated on DELETE operations
    const id = env.DOLAKE.idFromName('test-cache-delete-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // First insert a record
    const insertEvent = createTestCDCEvent({
      operation: 'INSERT',
      table: 'delete_test_table',
      rowId: 'row_to_delete',
      after: { id: 'row_to_delete', value: 999 },
    });

    await stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events: [insertEvent] }),
    });

    await stub.fetch('http://dolake/flush', { method: 'POST' });

    // Verify record exists (populates cache)
    const readResponse1 = await stub.fetch(
      'http://dolake/v1/query?sql=SELECT%20*%20FROM%20delete_test_table%20WHERE%20id=%27row_to_delete%27'
    );
    const readResult1 = await readResponse1.json() as { rows: Array<{ id: string }> };
    expect(readResult1.rows.length).toBe(1);

    // Delete the record
    const deleteEvent = createTestCDCEvent({
      operation: 'DELETE',
      table: 'delete_test_table',
      rowId: 'row_to_delete',
      before: { id: 'row_to_delete', value: 999 },
    });

    await stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events: [deleteEvent] }),
    });

    await stub.fetch('http://dolake/flush', { method: 'POST' });

    // Read again - should NOT see deleted record (cache should be invalidated)
    const readResponse2 = await stub.fetch(
      'http://dolake/v1/query?sql=SELECT%20*%20FROM%20delete_test_table%20WHERE%20id=%27row_to_delete%27'
    );
    const readResult2 = await readResponse2.json() as { rows: Array<{ id: string }> };

    // This assertion will FAIL because cache is not invalidated
    expect(readResult2.rows.length).toBe(0);
  });

  it.fails('should handle bulk DELETE cache invalidation efficiently', async () => {
    // GAP: Bulk delete does not efficiently invalidate cache
    const id = env.DOLAKE.idFromName('test-cache-bulk-delete-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Create multiple delete events
    const deleteEvents = Array.from({ length: 100 }, (_, i) =>
      createTestCDCEvent({
        operation: 'DELETE',
        table: 'bulk_delete_table',
        rowId: `row_${i}`,
        before: { id: `row_${i}`, value: i },
      })
    );

    const startTime = Date.now();

    await stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events: deleteEvents }),
    });

    await stub.fetch('http://dolake/flush', { method: 'POST' });

    const duration = Date.now() - startTime;

    // Bulk delete should be efficient (batched invalidation)
    expect(duration).toBeLessThan(5000); // Should complete in under 5 seconds

    // Check cache invalidation was batched
    const cacheStatus = await stub.fetch('http://dolake/v1/cache/metrics');
    const status = await cacheStatus.json() as {
      bulkInvalidationCount: number;
      individualInvalidationCount: number;
    };

    // Should have used bulk invalidation, not individual
    expect(status.bulkInvalidationCount).toBeGreaterThan(0);
  });
});

// =============================================================================
// 4. Cache Invalidation Propagation Across Replicas Tests
// =============================================================================

describe('Cache Invalidation Propagation Across Replicas [RED]', () => {
  it.fails('should propagate cache invalidation to all replicas', async () => {
    // GAP: No replica cache invalidation propagation
    const primaryId = env.DOLAKE.idFromName('test-primary-' + Date.now());
    const primaryStub = env.DOLAKE.get(primaryId);

    // Configure replication
    const configResponse = await primaryStub.fetch('http://dolake/v1/config/replication', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: true,
        replicas: ['replica-1', 'replica-2', 'replica-3'],
        invalidationPropagation: true,
      }),
    });

    expect(configResponse.status).toBe(200);

    // Perform an update on primary
    const updateEvent = createTestCDCEvent({
      operation: 'UPDATE',
      table: 'replicated_table',
      rowId: 'row_001',
      before: { id: 'row_001', value: 100 },
      after: { id: 'row_001', value: 200 },
    });

    await primaryStub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events: [updateEvent] }),
    });

    await primaryStub.fetch('http://dolake/flush', { method: 'POST' });

    // Check that invalidation was propagated to replicas
    const propagationStatus = await primaryStub.fetch(
      'http://dolake/v1/cache/invalidation/status'
    );
    expect(propagationStatus.status).toBe(200);

    const status = await propagationStatus.json() as {
      replicasNotified: number;
      replicasConfirmed: number;
      pendingReplicas: string[];
    };

    // All replicas should be notified
    expect(status.replicasNotified).toBe(3);
    expect(status.replicasConfirmed).toBe(3);
    expect(status.pendingReplicas).toHaveLength(0);
  });

  it.fails('should handle replica failure during invalidation propagation', async () => {
    // GAP: No failure handling for replica invalidation
    const primaryId = env.DOLAKE.idFromName('test-primary-failure-' + Date.now());
    const primaryStub = env.DOLAKE.get(primaryId);

    // Configure with one failing replica
    await primaryStub.fetch('http://dolake/v1/config/replication', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: true,
        replicas: ['replica-1', 'replica-failing', 'replica-3'],
        invalidationPropagation: true,
      }),
    });

    // Inject failure on one replica
    await primaryStub.fetch('http://dolake/v1/test/inject-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        replica: 'replica-failing',
        failureType: 'network_timeout',
      }),
    });

    // Perform an update
    const updateEvent = createTestCDCEvent({
      operation: 'UPDATE',
      table: 'replicated_table',
      rowId: 'row_002',
      before: { id: 'row_002', value: 50 },
      after: { id: 'row_002', value: 150 },
    });

    await primaryStub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events: [updateEvent] }),
    });

    await primaryStub.fetch('http://dolake/flush', { method: 'POST' });

    // Check propagation status
    const propagationStatus = await primaryStub.fetch(
      'http://dolake/v1/cache/invalidation/status'
    );
    const status = await propagationStatus.json() as {
      replicasNotified: number;
      replicasConfirmed: number;
      failedReplicas: Array<{ name: string; error: string; retryScheduled: boolean }>;
    };

    // Should track failed replica with retry
    expect(status.failedReplicas.length).toBe(1);
    expect(status.failedReplicas[0].name).toBe('replica-failing');
    expect(status.failedReplicas[0].retryScheduled).toBe(true);
  });

  it.fails('should support eventual consistency mode for replica invalidation', async () => {
    // GAP: No eventual consistency mode for invalidation
    const primaryId = env.DOLAKE.idFromName('test-eventual-' + Date.now());
    const primaryStub = env.DOLAKE.get(primaryId);

    // Configure with eventual consistency
    await primaryStub.fetch('http://dolake/v1/config/replication', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: true,
        replicas: ['replica-1', 'replica-2'],
        invalidationPropagation: true,
        consistencyMode: 'eventual', // Don't wait for all replicas
        maxPropagationDelayMs: 5000,
      }),
    });

    const configResult = await primaryStub.fetch('http://dolake/v1/config/replication');
    const config = await configResult.json() as { consistencyMode: string };

    expect(config.consistencyMode).toBe('eventual');
  });
});

// =============================================================================
// 5. Partial Cache Invalidation for Specific Partitions Tests
// =============================================================================

describe('Partial Cache Invalidation for Specific Partitions [RED]', () => {
  it.fails('should only invalidate affected partitions on UPDATE', async () => {
    // GAP: All partitions are invalidated instead of just affected ones
    const id = env.DOLAKE.idFromName('test-partial-update-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Create test partitions (setup)
    await stub.fetch('http://dolake/v1/test/create-partitions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['default'],
        tableName: 'partitioned_events',
        numPartitions: 30,
      }),
    });

    // Warm up cache for multiple partitions
    for (const day of ['2024-01-01', '2024-01-15', '2024-01-30']) {
      await stub.fetch(
        `http://dolake/v1/query?sql=SELECT%20*%20FROM%20partitioned_events%20WHERE%20day=%27${day}%27`
      );
    }

    // Update a record in only one partition
    const updateEvent = createTestCDCEvent({
      operation: 'UPDATE',
      table: 'partitioned_events',
      rowId: 'row_in_jan15',
      before: { id: 'row_in_jan15', day: '2024-01-15', value: 100 },
      after: { id: 'row_in_jan15', day: '2024-01-15', value: 200 },
    });

    await stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events: [updateEvent] }),
    });

    await stub.fetch('http://dolake/flush', { method: 'POST' });

    // Check cache status per partition
    const cacheStatus = await stub.fetch(
      'http://dolake/v1/cache/partitions/partitioned_events'
    );
    expect(cacheStatus.status).toBe(200);

    const status = await cacheStatus.json() as {
      partitions: Array<{
        partition: string;
        cached: boolean;
        invalidated: boolean;
      }>;
    };

    // Only 2024-01-15 should be invalidated
    const jan01 = status.partitions.find(p => p.partition === 'day=2024-01-01');
    const jan15 = status.partitions.find(p => p.partition === 'day=2024-01-15');
    const jan30 = status.partitions.find(p => p.partition === 'day=2024-01-30');

    expect(jan01?.invalidated).toBe(false);
    expect(jan15?.invalidated).toBe(true);
    expect(jan30?.invalidated).toBe(false);
  });

  it.fails('should support fine-grained invalidation by partition key', async () => {
    // GAP: No API for partition-specific invalidation
    const id = env.DOLAKE.idFromName('test-fine-grained-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Manually invalidate a specific partition
    const response = await stub.fetch('http://dolake/v1/cache/invalidate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        table: 'partitioned_events',
        partitions: ['day=2024-01-15', 'day=2024-01-16'],
      }),
    });

    expect(response.status).toBe(200);

    const result = await response.json() as CacheInvalidationResult;

    expect(result.success).toBe(true);
    expect(result.keysInvalidated).toContain('partitioned_events/day=2024-01-15');
    expect(result.keysInvalidated).toContain('partitioned_events/day=2024-01-16');
  });

  it.fails('should support wildcard partition invalidation', async () => {
    // GAP: No wildcard support for partition invalidation
    const id = env.DOLAKE.idFromName('test-wildcard-partition-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Invalidate all January 2024 partitions
    const response = await stub.fetch('http://dolake/v1/cache/invalidate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        table: 'partitioned_events',
        partitionPattern: 'day=2024-01-*',
      }),
    });

    expect(response.status).toBe(200);

    const result = await response.json() as CacheInvalidationResult;

    // Should invalidate all 31 days of January
    expect(result.keysInvalidated.length).toBe(31);
  });
});

// =============================================================================
// 6. Cache TTL Expiry Tests
// =============================================================================

describe('Cache TTL Expiry [RED]', () => {
  it.fails('should expire cache entries after configured TTL', async () => {
    // GAP: Cache TTL is not implemented
    const id = env.DOLAKE.idFromName('test-ttl-expiry-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Configure cache with short TTL
    await stub.fetch('http://dolake/v1/cache/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: true,
        ttlMs: 1000, // 1 second TTL
      }),
    });

    // Populate cache
    const readResponse1 = await stub.fetch(
      'http://dolake/v1/query?sql=SELECT%20*%20FROM%20ttl_test_table'
    );
    expect(readResponse1.status).toBe(200);

    // Check cache hit
    const cacheStatus1 = await stub.fetch('http://dolake/v1/cache/entry/ttl_test_table');
    const status1 = await cacheStatus1.json() as { cached: boolean; expiresAt: number };
    expect(status1.cached).toBe(true);

    // Wait for TTL to expire
    await new Promise(resolve => setTimeout(resolve, 1500));

    // Check cache miss after TTL
    const cacheStatus2 = await stub.fetch('http://dolake/v1/cache/entry/ttl_test_table');
    const status2 = await cacheStatus2.json() as { cached: boolean };
    expect(status2.cached).toBe(false);
  });

  it.fails('should support per-table TTL configuration', async () => {
    // GAP: No per-table TTL support
    const id = env.DOLAKE.idFromName('test-per-table-ttl-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Configure different TTLs per table
    await stub.fetch('http://dolake/v1/cache/config/tables', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        hot_table: { ttlMs: 60000 }, // 1 minute
        cold_table: { ttlMs: 3600000 }, // 1 hour
        default: { ttlMs: 300000 }, // 5 minutes
      }),
    });

    const configResponse = await stub.fetch('http://dolake/v1/cache/config/tables');
    expect(configResponse.status).toBe(200);

    const config = await configResponse.json() as {
      tables: Record<string, { ttlMs: number }>;
    };

    expect(config.tables.hot_table.ttlMs).toBe(60000);
    expect(config.tables.cold_table.ttlMs).toBe(3600000);
  });

  it.fails('should refresh TTL on cache hit (sliding expiration)', async () => {
    // GAP: No sliding expiration support
    const id = env.DOLAKE.idFromName('test-sliding-ttl-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Configure with sliding expiration
    await stub.fetch('http://dolake/v1/cache/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: true,
        ttlMs: 2000,
        slidingExpiration: true,
      }),
    });

    // Populate cache
    await stub.fetch('http://dolake/v1/query?sql=SELECT%20*%20FROM%20sliding_test');

    // Get initial expiry time
    const status1 = await stub.fetch('http://dolake/v1/cache/entry/sliding_test');
    const initial = await status1.json() as { expiresAt: number };
    const initialExpiry = initial.expiresAt;

    // Wait a bit and access again
    await new Promise(resolve => setTimeout(resolve, 500));
    await stub.fetch('http://dolake/v1/query?sql=SELECT%20*%20FROM%20sliding_test');

    // Get updated expiry time
    const status2 = await stub.fetch('http://dolake/v1/cache/entry/sliding_test');
    const updated = await status2.json() as { expiresAt: number };

    // Expiry should have been extended
    expect(updated.expiresAt).toBeGreaterThan(initialExpiry);
  });
});

// =============================================================================
// 7. Batched Invalidation Tests
// =============================================================================

describe('Batched Invalidation for Multiple Changes [RED]', () => {
  it.fails('should batch multiple invalidations into single operation', async () => {
    // GAP: Each change triggers individual invalidation
    const id = env.DOLAKE.idFromName('test-batch-invalidation-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Configure batching
    await stub.fetch('http://dolake/v1/cache/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: true,
        batchInvalidation: true,
        batchSize: 100,
        batchDelayMs: 50,
      }),
    });

    // Send many CDC events quickly
    const events = Array.from({ length: 50 }, (_, i) =>
      createTestCDCEvent({
        operation: 'UPDATE',
        table: 'batch_test_table',
        rowId: `row_${i}`,
        before: { id: `row_${i}`, value: i },
        after: { id: `row_${i}`, value: i * 2 },
      })
    );

    await stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events }),
    });

    await stub.fetch('http://dolake/flush', { method: 'POST' });

    // Check invalidation metrics
    const metricsResponse = await stub.fetch('http://dolake/v1/cache/metrics');
    const metrics = await metricsResponse.json() as {
      batchInvalidationOps: number;
      individualInvalidationOps: number;
      keysInvalidated: number;
    };

    // Should have batched, not individual operations
    expect(metrics.batchInvalidationOps).toBeLessThan(5);
    expect(metrics.individualInvalidationOps).toBe(0);
    expect(metrics.keysInvalidated).toBe(50);
  });

  it.fails('should flush pending invalidations on timeout', async () => {
    // GAP: No timeout-based batch flush
    const id = env.DOLAKE.idFromName('test-batch-timeout-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Configure with batch timeout
    await stub.fetch('http://dolake/v1/cache/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: true,
        batchInvalidation: true,
        batchSize: 100, // High threshold
        batchDelayMs: 100, // But short timeout
      }),
    });

    // Send only a few events (won't reach batch size)
    const events = Array.from({ length: 5 }, (_, i) =>
      createTestCDCEvent({
        operation: 'UPDATE',
        table: 'timeout_test_table',
        rowId: `row_${i}`,
        before: { id: `row_${i}`, value: i },
        after: { id: `row_${i}`, value: i * 2 },
      })
    );

    await stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events }),
    });

    await stub.fetch('http://dolake/flush', { method: 'POST' });

    // Wait for batch timeout
    await new Promise(resolve => setTimeout(resolve, 200));

    // Check that pending invalidations were flushed
    const statusResponse = await stub.fetch('http://dolake/v1/cache/invalidation/pending');
    const status = await statusResponse.json() as { pendingCount: number };

    expect(status.pendingCount).toBe(0);
  });

  it.fails('should deduplicate invalidations for same key in batch', async () => {
    // GAP: No deduplication of invalidation requests
    const id = env.DOLAKE.idFromName('test-batch-dedup-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Configure batching
    await stub.fetch('http://dolake/v1/cache/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: true,
        batchInvalidation: true,
        deduplicateInvalidations: true,
      }),
    });

    // Send multiple updates to the same row
    const events = Array.from({ length: 10 }, (_, i) =>
      createTestCDCEvent({
        operation: 'UPDATE',
        table: 'dedup_test_table',
        rowId: 'same_row', // Same row every time
        before: { id: 'same_row', value: i },
        after: { id: 'same_row', value: i + 1 },
      })
    );

    await stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events }),
    });

    await stub.fetch('http://dolake/flush', { method: 'POST' });

    // Check deduplication metrics
    const metricsResponse = await stub.fetch('http://dolake/v1/cache/metrics');
    const metrics = await metricsResponse.json() as {
      invalidationRequestsReceived: number;
      invalidationRequestsDeduplicated: number;
      actualInvalidationsPerformed: number;
    };

    // 10 requests, but only 1 actual invalidation (all for same key)
    expect(metrics.invalidationRequestsReceived).toBe(10);
    expect(metrics.invalidationRequestsDeduplicated).toBe(9);
    expect(metrics.actualInvalidationsPerformed).toBe(1);
  });
});

// =============================================================================
// 8. Stale Read Prevention Tests
// =============================================================================

describe('Stale Read Prevention After Invalidation [RED]', () => {
  it.fails('should prevent stale reads during invalidation in progress', async () => {
    // GAP: Reads can return stale data during invalidation
    const id = env.DOLAKE.idFromName('test-stale-reads-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Configure strict consistency mode
    await stub.fetch('http://dolake/v1/cache/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: true,
        staleReadPrevention: true,
        consistencyMode: 'strict',
      }),
    });

    // Populate cache
    await stub.fetch('http://dolake/v1/query?sql=SELECT%20*%20FROM%20stale_test%20WHERE%20id=%27row_001%27');

    // Start an update (this triggers invalidation)
    const updatePromise = stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        events: [createTestCDCEvent({
          operation: 'UPDATE',
          table: 'stale_test',
          rowId: 'row_001',
          before: { id: 'row_001', value: 100 },
          after: { id: 'row_001', value: 200 },
        })],
      }),
    });

    // Immediately try to read (during invalidation)
    const readPromise = stub.fetch(
      'http://dolake/v1/query?sql=SELECT%20*%20FROM%20stale_test%20WHERE%20id=%27row_001%27&consistency=strict'
    );

    await Promise.all([updatePromise, stub.fetch('http://dolake/flush', { method: 'POST' })]);
    const readResponse = await readPromise;
    const readResult = await readResponse.json() as {
      rows: Array<{ value: number }>;
      cacheStatus: string;
    };

    // Should either block until fresh or return uncached data
    expect(readResult.cacheStatus).not.toBe('stale');
    expect(readResult.rows[0].value).toBe(200);
  });

  it.fails('should track stale read metrics', async () => {
    // GAP: No stale read tracking
    const id = env.DOLAKE.idFromName('test-stale-metrics-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Get metrics
    const metricsResponse = await stub.fetch('http://dolake/v1/cache/metrics');
    expect(metricsResponse.status).toBe(200);

    const metrics = await metricsResponse.json() as {
      staleReadsBlocked: number;
      staleReadsServed: number;
      staleReadAttempts: number;
    };

    expect(metrics.staleReadsBlocked).toBeDefined();
    expect(metrics.staleReadsServed).toBeDefined();
    expect(metrics.staleReadAttempts).toBeDefined();
  });

  it.fails('should support read-your-writes consistency', async () => {
    // GAP: No read-your-writes consistency guarantee
    const id = env.DOLAKE.idFromName('test-ryw-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Configure read-your-writes mode
    await stub.fetch('http://dolake/v1/cache/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: true,
        readYourWritesConsistency: true,
      }),
    });

    // Get a session token
    const sessionResponse = await stub.fetch('http://dolake/v1/session/start', {
      method: 'POST',
    });
    const session = await sessionResponse.json() as { token: string };

    // Write with session token
    await stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Session-Token': session.token,
      },
      body: JSON.stringify({
        events: [createTestCDCEvent({
          operation: 'INSERT',
          table: 'ryw_test',
          rowId: 'new_row',
          after: { id: 'new_row', value: 999 },
        })],
      }),
    });

    await stub.fetch('http://dolake/flush', { method: 'POST' });

    // Read with same session token - should see own write
    const readResponse = await stub.fetch(
      'http://dolake/v1/query?sql=SELECT%20*%20FROM%20ryw_test%20WHERE%20id=%27new_row%27',
      {
        headers: { 'X-Session-Token': session.token },
      }
    );
    const readResult = await readResponse.json() as { rows: Array<{ value: number }> };

    expect(readResult.rows.length).toBe(1);
    expect(readResult.rows[0].value).toBe(999);
  });

  it.fails('should invalidate related query caches on data change', async () => {
    // GAP: Related queries not invalidated
    const id = env.DOLAKE.idFromName('test-related-queries-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Cache multiple related queries
    await stub.fetch('http://dolake/v1/query?sql=SELECT%20*%20FROM%20orders%20WHERE%20customer_id=%27c1%27');
    await stub.fetch('http://dolake/v1/query?sql=SELECT%20SUM(amount)%20FROM%20orders%20WHERE%20customer_id=%27c1%27');
    await stub.fetch('http://dolake/v1/query?sql=SELECT%20COUNT(*)%20FROM%20orders%20WHERE%20customer_id=%27c1%27');

    // Insert new order for customer c1
    const insertEvent = createTestCDCEvent({
      operation: 'INSERT',
      table: 'orders',
      rowId: 'order_new',
      after: { id: 'order_new', customer_id: 'c1', amount: 500 },
    });

    await stub.fetch('http://dolake/cdc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events: [insertEvent] }),
    });

    await stub.fetch('http://dolake/flush', { method: 'POST' });

    // Check that all related queries were invalidated
    const cacheStatus = await stub.fetch('http://dolake/v1/cache/queries/orders');
    const status = await cacheStatus.json() as {
      invalidatedQueries: string[];
      cachedQueries: string[];
    };

    // All three queries should be invalidated (they all reference customer c1)
    expect(status.invalidatedQueries.length).toBe(3);
    expect(status.cachedQueries.length).toBe(0);
  });
});
