/**
 * TDD RED Phase: DoLake Metadata Caching Tests
 *
 * These tests document the expected behavior for Iceberg table metadata caching
 * in Durable Objects. Currently, metadata is fetched from R2 on every request
 * which introduces unnecessary latency.
 *
 * Issue: sql-9n78.1
 *
 * Current behavior (problematic):
 * - Iceberg metadata fetched from R2 on every table access
 * - No caching layer between DO instances
 * - Schema changes require manual cache invalidation
 * - High latency for repeated metadata lookups
 *
 * Expected behavior:
 * - Iceberg table metadata cached in DO persistent storage
 * - Automatic cache invalidation on schema changes via CDC
 * - Sub-millisecond metadata fetch from cache
 * - Cache coherence across multiple DO instances
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

// =============================================================================
// Mock Types for Testing
// =============================================================================

/**
 * Mock Iceberg table metadata structure
 * Based on IcebergTableMetadata from dolake/types.ts
 */
interface MockIcebergMetadata {
  'format-version': 2;
  'table-uuid': string;
  location: string;
  'last-sequence-number': bigint;
  'last-updated-ms': bigint;
  'last-column-id': number;
  'current-schema-id': number;
  schemas: Array<{
    'schema-id': number;
    type: 'struct';
    fields: Array<{
      id: number;
      name: string;
      type: string;
      required: boolean;
    }>;
  }>;
  'default-spec-id': number;
  'partition-specs': Array<{
    'spec-id': number;
    fields: Array<{
      'source-id': number;
      'field-id': number;
      name: string;
      transform: string;
    }>;
  }>;
  'default-sort-order-id': number;
  'sort-orders': Array<{
    'order-id': number;
    fields: Array<{
      transform: string;
      'source-id': number;
      direction: 'asc' | 'desc';
      'null-order': 'nulls-first' | 'nulls-last';
    }>;
  }>;
  'current-snapshot-id': bigint | null;
  snapshots: Array<{
    'snapshot-id': bigint;
    'timestamp-ms': bigint;
    'manifest-list': string;
  }>;
  'snapshot-log': Array<{
    'snapshot-id': bigint;
    'timestamp-ms': bigint;
  }>;
}

/**
 * Mock metadata cache configuration
 */
interface MetadataCacheConfig {
  /** Enable metadata caching */
  enabled: boolean;
  /** TTL for cached metadata in milliseconds */
  ttlMs: number;
  /** Maximum number of tables to cache */
  maxTables: number;
  /** Enable cache coherence across DO instances */
  enableCoherence: boolean;
  /** Latency threshold for sub-ms requirement (microseconds) */
  latencyThresholdUs: number;
}

/**
 * Mock metadata cache entry
 */
interface MetadataCacheEntry {
  metadata: MockIcebergMetadata;
  cachedAt: number;
  expiresAt: number;
  version: number;
  hitCount: number;
  lastAccessedAt: number;
}

/**
 * Mock cache coherence message
 */
interface CoherenceMessage {
  type: 'invalidate' | 'update';
  tableId: string;
  version: number;
  timestamp: number;
  sourceDoId: string;
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Create mock Iceberg table metadata
 */
function createMockMetadata(tableId: string, schemaId = 1): MockIcebergMetadata {
  return {
    'format-version': 2,
    'table-uuid': tableId,
    location: `r2://lakehouse/warehouse/${tableId}`,
    'last-sequence-number': BigInt(1),
    'last-updated-ms': BigInt(Date.now()),
    'last-column-id': 3,
    'current-schema-id': schemaId,
    schemas: [
      {
        'schema-id': schemaId,
        type: 'struct',
        fields: [
          { id: 1, name: 'id', type: 'long', required: true },
          { id: 2, name: 'name', type: 'string', required: false },
          { id: 3, name: 'created_at', type: 'timestamptz', required: true },
        ],
      },
    ],
    'default-spec-id': 0,
    'partition-specs': [
      {
        'spec-id': 0,
        fields: [],
      },
    ],
    'default-sort-order-id': 0,
    'sort-orders': [
      {
        'order-id': 0,
        fields: [],
      },
    ],
    'current-snapshot-id': BigInt(1000),
    snapshots: [
      {
        'snapshot-id': BigInt(1000),
        'timestamp-ms': BigInt(Date.now()),
        'manifest-list': `r2://lakehouse/warehouse/${tableId}/metadata/snap-1000-manifest-list.avro`,
      },
    ],
    'snapshot-log': [
      {
        'snapshot-id': BigInt(1000),
        'timestamp-ms': BigInt(Date.now()),
      },
    ],
  };
}

// =============================================================================
// Test Suite: Metadata Caching in Durable Objects
// =============================================================================

describe('DoLake Metadata Caching', () => {
  describe('Caching Iceberg Table Metadata in Durable Objects', () => {
    /**
     * GAP: No MetadataCache class exists
     * Expected: MetadataCache class should be importable and instantiable
     */
    it('should have MetadataCache class available for import', async () => {
      // MetadataCache does not exist yet
      const { MetadataCache } = await import('../dolake/metadata-cache.js');
      expect(MetadataCache).toBeDefined();
      expect(typeof MetadataCache).toBe('function');
    });

    /**
     * GAP: No metadata cache configuration options
     * Expected: MetadataCache should accept configuration options
     */
    it('should accept configuration options', async () => {
      const { MetadataCache } = await import('../dolake/metadata-cache.js');

      const config: MetadataCacheConfig = {
        enabled: true,
        ttlMs: 300_000, // 5 minutes
        maxTables: 1000,
        enableCoherence: true,
        latencyThresholdUs: 1000, // 1ms in microseconds
      };

      const cache = new MetadataCache(config);

      expect(cache.config.enabled).toBe(true);
      expect(cache.config.ttlMs).toBe(300_000);
      expect(cache.config.maxTables).toBe(1000);
    });

    /**
     * GAP: Cannot cache table metadata
     * Expected: put() method should store metadata in DO persistent storage
     */
    it('should cache table metadata in DO persistent storage', async () => {
      const { MetadataCache } = await import('../dolake/metadata-cache.js');

      const cache = new MetadataCache({ enabled: true, ttlMs: 60_000, maxTables: 100, enableCoherence: false, latencyThresholdUs: 1000 });
      const metadata = createMockMetadata('users');

      // put() should store metadata
      await cache.put('users', metadata);

      // Verify entry exists
      const entry = await cache.getEntry('users');
      expect(entry).toBeDefined();
      expect(entry?.metadata['table-uuid']).toBe('users');
      expect(entry?.cachedAt).toBeLessThanOrEqual(Date.now());
      expect(entry?.expiresAt).toBeGreaterThan(Date.now());
    });

    /**
     * GAP: Cannot retrieve cached metadata
     * Expected: get() method should return cached metadata
     */
    it('should retrieve cached metadata', async () => {
      const { MetadataCache } = await import('../dolake/metadata-cache.js');

      const cache = new MetadataCache({ enabled: true, ttlMs: 60_000, maxTables: 100, enableCoherence: false, latencyThresholdUs: 1000 });
      const originalMetadata = createMockMetadata('orders');

      await cache.put('orders', originalMetadata);
      const retrieved = await cache.get('orders');

      expect(retrieved).toBeDefined();
      expect(retrieved?.['table-uuid']).toBe('orders');
      expect(retrieved?.schemas[0].fields.length).toBe(3);
    });

    /**
     * GAP: No hit/miss tracking
     * Expected: Cache should track hit and miss statistics
     */
    it('should track cache hits and misses', async () => {
      const { MetadataCache } = await import('../dolake/metadata-cache.js');

      const cache = new MetadataCache({ enabled: true, ttlMs: 60_000, maxTables: 100, enableCoherence: false, latencyThresholdUs: 1000 });
      const metadata = createMockMetadata('events');

      await cache.put('events', metadata);

      // First access - hit
      await cache.get('events');
      // Second access - hit
      await cache.get('events');
      // Non-existent - miss
      await cache.get('nonexistent');

      const stats = cache.getStats();
      expect(stats.hits).toBe(2);
      expect(stats.misses).toBe(1);
      expect(stats.hitRate).toBeCloseTo(0.667, 2);
    });

    /**
     * GAP: No TTL-based expiration
     * Expected: Cached entries should expire after TTL
     */
    it('should expire cached entries after TTL', async () => {
      vi.useFakeTimers();

      const { MetadataCache } = await import('../dolake/metadata-cache.js');

      const cache = new MetadataCache({ enabled: true, ttlMs: 1000, maxTables: 100, enableCoherence: false, latencyThresholdUs: 1000 }); // 1 second TTL
      const metadata = createMockMetadata('expiring_table');

      await cache.put('expiring_table', metadata);

      // Should be cached initially
      let result = await cache.get('expiring_table');
      expect(result).toBeDefined();

      // Advance time past TTL
      vi.advanceTimersByTime(1500);

      // Should be expired
      result = await cache.get('expiring_table');
      expect(result).toBeNull();

      vi.useRealTimers();
    });

    /**
     * GAP: No LRU eviction when max tables exceeded
     * Expected: Should evict least recently used entries when capacity exceeded
     */
    it('should evict LRU entries when capacity exceeded', async () => {
      const { MetadataCache } = await import('../dolake/metadata-cache.js');

      const cache = new MetadataCache({ enabled: true, ttlMs: 60_000, maxTables: 3, enableCoherence: false, latencyThresholdUs: 1000 });

      // Fill cache to capacity
      await cache.put('table1', createMockMetadata('table1'));
      await cache.get('table1'); // Access to make it recently used
      await cache.put('table2', createMockMetadata('table2'));
      await cache.put('table3', createMockMetadata('table3'));

      // Access table1 again to make it most recently used
      await cache.get('table1');

      // Add a fourth entry - should evict table2 (least recently used)
      await cache.put('table4', createMockMetadata('table4'));

      // table1 should still be cached (recently accessed)
      expect(await cache.get('table1')).toBeDefined();
      // table2 should be evicted (LRU)
      expect(await cache.get('table2')).toBeNull();
      // table3 and table4 should be cached
      expect(await cache.get('table3')).toBeDefined();
      expect(await cache.get('table4')).toBeDefined();
    });

    /**
     * GAP: No integration with DO persistent storage
     * Expected: Cache should persist across DO hibernation
     */
    it('should persist cache across DO hibernation', async () => {
      const { MetadataCache } = await import('../dolake/metadata-cache.js');

      // Simulate DO storage
      const mockStorage = new Map<string, unknown>();

      const cache1 = new MetadataCache(
        { enabled: true, ttlMs: 60_000, maxTables: 100, enableCoherence: false, latencyThresholdUs: 1000 },
        { storage: mockStorage }
      );

      await cache1.put('persistent_table', createMockMetadata('persistent_table'));
      await cache1.flush(); // Persist to storage

      // Simulate DO hibernation and recovery
      const cache2 = new MetadataCache(
        { enabled: true, ttlMs: 60_000, maxTables: 100, enableCoherence: false, latencyThresholdUs: 1000 },
        { storage: mockStorage }
      );
      await cache2.restore();

      // Should recover cached metadata
      const recovered = await cache2.get('persistent_table');
      expect(recovered).toBeDefined();
      expect(recovered?.['table-uuid']).toBe('persistent_table');
    });
  });

  // ===========================================================================
  // Test Suite: Cache Invalidation on Schema Changes
  // ===========================================================================

  describe('Cache Invalidation on Schema Changes', () => {
    /**
     * GAP: No automatic invalidation on schema changes
     * Expected: Cache should invalidate when schema changes are detected
     */
    it('should invalidate cache on schema change CDC event', async () => {
      const { MetadataCache, processSchemaChangeEvent } = await import('../dolake/metadata-cache.js');

      const cache = new MetadataCache({ enabled: true, ttlMs: 60_000, maxTables: 100, enableCoherence: false, latencyThresholdUs: 1000 });

      // Cache initial metadata
      const initialMetadata = createMockMetadata('users', 1);
      await cache.put('users', initialMetadata);

      // Simulate schema change CDC event (e.g., ALTER TABLE ADD COLUMN)
      const schemaChangeEvent = {
        type: 'schema_change' as const,
        table: 'users',
        operation: 'ADD_COLUMN' as const,
        column: { id: 4, name: 'email', type: 'string', required: false },
        newSchemaId: 2,
        timestamp: Date.now(),
      };

      await processSchemaChangeEvent(cache, schemaChangeEvent);

      // Cache should be invalidated
      const entry = await cache.getEntry('users');
      expect(entry).toBeNull();
    });

    /**
     * GAP: No version tracking for cache entries
     * Expected: Cache should track schema version to detect staleness
     */
    it('should track schema version for cache entries', async () => {
      const { MetadataCache } = await import('../dolake/metadata-cache.js');

      const cache = new MetadataCache({ enabled: true, ttlMs: 60_000, maxTables: 100, enableCoherence: false, latencyThresholdUs: 1000 });

      const metadata = createMockMetadata('versioned_table', 5);
      await cache.put('versioned_table', metadata);

      const entry = await cache.getEntry('versioned_table');
      expect(entry?.version).toBe(5);

      // Update with new version
      const newMetadata = createMockMetadata('versioned_table', 6);
      await cache.put('versioned_table', newMetadata);

      const updatedEntry = await cache.getEntry('versioned_table');
      expect(updatedEntry?.version).toBe(6);
    });

    /**
     * GAP: No invalidation on partition spec changes
     * Expected: Should invalidate when partition spec evolves
     */
    it('should invalidate on partition spec change', async () => {
      const { MetadataCache, processPartitionSpecChange } = await import('../dolake/metadata-cache.js');

      const cache = new MetadataCache({ enabled: true, ttlMs: 60_000, maxTables: 100, enableCoherence: false, latencyThresholdUs: 1000 });

      await cache.put('partitioned_table', createMockMetadata('partitioned_table'));

      // Simulate partition spec evolution
      await processPartitionSpecChange(cache, 'partitioned_table', {
        oldSpecId: 0,
        newSpecId: 1,
        newFields: [
          { 'source-id': 3, 'field-id': 1000, name: 'day', transform: 'day' },
        ],
      });

      // Should invalidate or update the cache
      const entry = await cache.getEntry('partitioned_table');
      expect(entry).toBeNull();
    });

    /**
     * GAP: No selective invalidation based on change type
     * Expected: Different change types should have different invalidation strategies
     */
    it('should support selective invalidation strategies', async () => {
      const { MetadataCache } = await import('../dolake/metadata-cache.js');

      const cache = new MetadataCache({
        enabled: true,
        ttlMs: 60_000,
        maxTables: 100,
        enableCoherence: false,
        latencyThresholdUs: 1000,
        invalidationStrategies: {
          SCHEMA_CHANGE: 'immediate',
          SNAPSHOT_APPEND: 'refresh', // Refresh metadata instead of invalidating
          PROPERTY_CHANGE: 'lazy', // Invalidate on next access
        },
      });

      await cache.put('strategy_table', createMockMetadata('strategy_table'));

      // Property change - should mark for lazy invalidation
      await cache.markForLazyInvalidation('strategy_table', 'PROPERTY_CHANGE');

      // Should still return cached data until accessed
      const entry = await cache.getEntry('strategy_table');
      expect(entry?.pendingInvalidation).toBe(true);

      // On access, should trigger refresh
      const result = await cache.get('strategy_table');
      expect(result).toBeNull(); // Lazy invalidation triggered
    });

    /**
     * GAP: No bulk invalidation for table drops
     * Expected: Table drop should clear all related cache entries
     */
    it('should clear all entries for dropped table', async () => {
      const { MetadataCache, processTableDrop } = await import('../dolake/metadata-cache.js');

      const cache = new MetadataCache({ enabled: true, ttlMs: 60_000, maxTables: 100, enableCoherence: false, latencyThresholdUs: 1000 });

      await cache.put('table_to_drop', createMockMetadata('table_to_drop'));

      // Also cache some related data (e.g., partition stats)
      await cache.putAuxiliary('table_to_drop', 'partition_stats', { partitions: 10 });

      await processTableDrop(cache, 'table_to_drop');

      expect(await cache.get('table_to_drop')).toBeNull();
      expect(await cache.getAuxiliary('table_to_drop', 'partition_stats')).toBeNull();
    });
  });

  // ===========================================================================
  // Test Suite: Sub-millisecond Latency Requirements
  // ===========================================================================

  describe('Metadata Fetch Latency Requirements', () => {
    /**
     * GAP: No latency measurement for cache hits
     * Expected: Cache hits should complete in sub-millisecond time
     */
    it('should fetch cached metadata in sub-millisecond time', async () => {
      const { MetadataCache } = await import('../dolake/metadata-cache.js');

      const cache = new MetadataCache({ enabled: true, ttlMs: 60_000, maxTables: 100, enableCoherence: false, latencyThresholdUs: 1000 });
      const metadata = createMockMetadata('fast_table');

      await cache.put('fast_table', metadata);

      // Warm up
      await cache.get('fast_table');

      // Measure latency
      const iterations = 1000;
      const startTime = performance.now();

      for (let i = 0; i < iterations; i++) {
        await cache.get('fast_table');
      }

      const endTime = performance.now();
      const avgLatencyMs = (endTime - startTime) / iterations;
      const avgLatencyUs = avgLatencyMs * 1000;

      // Average latency should be under 1ms (1000 microseconds)
      expect(avgLatencyUs).toBeLessThan(1000);
    });

    /**
     * GAP: No latency SLA tracking
     * Expected: Cache should track latency percentiles (p50, p99, p999)
     */
    it('should track latency percentiles', async () => {
      const { MetadataCache } = await import('../dolake/metadata-cache.js');

      const cache = new MetadataCache({ enabled: true, ttlMs: 60_000, maxTables: 100, enableCoherence: false, latencyThresholdUs: 1000 });
      const metadata = createMockMetadata('latency_tracked');

      await cache.put('latency_tracked', metadata);

      // Perform multiple accesses
      for (let i = 0; i < 100; i++) {
        await cache.get('latency_tracked');
      }

      const latencyStats = cache.getLatencyStats();

      expect(latencyStats.p50Us).toBeDefined();
      expect(latencyStats.p99Us).toBeDefined();
      expect(latencyStats.p999Us).toBeDefined();
      expect(latencyStats.avgUs).toBeDefined();

      // All percentiles should be sub-ms for cache hits
      expect(latencyStats.p99Us).toBeLessThan(1000);
    });

    /**
     * GAP: No latency budget enforcement
     * Expected: Operations exceeding latency budget should be reported
     */
    it('should report latency budget violations', async () => {
      const { MetadataCache } = await import('../dolake/metadata-cache.js');

      const onLatencyViolation = vi.fn();

      const cache = new MetadataCache({
        enabled: true,
        ttlMs: 60_000,
        maxTables: 100,
        enableCoherence: false,
        latencyThresholdUs: 100, // Very strict 100us threshold
        onLatencyViolation,
      });

      const metadata = createMockMetadata('strict_latency');
      await cache.put('strict_latency', metadata);

      // Access multiple times
      for (let i = 0; i < 100; i++) {
        await cache.get('strict_latency');
      }

      // Should report violations if any
      const stats = cache.getLatencyStats();
      if (stats.violations > 0) {
        expect(onLatencyViolation).toHaveBeenCalled();
      }
    });

    /**
     * GAP: No pre-warming capability
     * Expected: Cache should support pre-warming for hot tables
     */
    it('should support cache pre-warming', async () => {
      const { MetadataCache } = await import('../dolake/metadata-cache.js');

      const cache = new MetadataCache({ enabled: true, ttlMs: 60_000, maxTables: 100, enableCoherence: false, latencyThresholdUs: 1000 });

      // Pre-warm with list of hot tables
      const hotTables = ['users', 'orders', 'events', 'sessions'];
      const metadataLoader = async (tableId: string) => createMockMetadata(tableId);

      await cache.prewarm(hotTables, metadataLoader);

      // All hot tables should be cached
      for (const table of hotTables) {
        const metadata = await cache.get(table);
        expect(metadata).toBeDefined();
      }

      const stats = cache.getStats();
      expect(stats.entriesCount).toBe(4);
    });

    /**
     * GAP: No memory-optimized storage format
     * Expected: Large metadata should be stored efficiently
     */
    it('should use memory-efficient storage for large metadata', async () => {
      const { MetadataCache, estimateMemoryUsage } = await import('../dolake/metadata-cache.js');

      const cache = new MetadataCache({ enabled: true, ttlMs: 60_000, maxTables: 100, enableCoherence: false, latencyThresholdUs: 1000 });

      // Create metadata with many columns (simulating a wide table)
      const wideMetadata = createMockMetadata('wide_table');
      for (let i = 4; i <= 100; i++) {
        wideMetadata.schemas[0].fields.push({
          id: i,
          name: `column_${i}`,
          type: 'string',
          required: false,
        });
      }

      await cache.put('wide_table', wideMetadata);

      const memoryUsage = estimateMemoryUsage(cache);

      // Should use efficient encoding (e.g., schema deduplication)
      expect(memoryUsage.perEntryAvgBytes).toBeLessThan(10_000);
    });
  });

  // ===========================================================================
  // Test Suite: Cache Coherence Across DO Instances
  // ===========================================================================

  describe('Cache Coherence Across Multiple DO Instances', () => {
    /**
     * GAP: No inter-DO cache invalidation
     * Expected: Cache invalidation should propagate to all DO instances
     */
    it('should propagate invalidation across DO instances', async () => {
      const { MetadataCache, CacheCoherenceManager } = await import('../dolake/metadata-cache.js');

      // Create a coherence manager to coordinate between DOs
      const coherenceManager = new CacheCoherenceManager();

      // Simulate two DO instances
      const cache1 = new MetadataCache({
        enabled: true,
        ttlMs: 60_000,
        maxTables: 100,
        enableCoherence: true,
        latencyThresholdUs: 1000,
      });
      const cache2 = new MetadataCache({
        enabled: true,
        ttlMs: 60_000,
        maxTables: 100,
        enableCoherence: true,
        latencyThresholdUs: 1000,
      });

      coherenceManager.register('do-1', cache1);
      coherenceManager.register('do-2', cache2);

      // Both caches have the same metadata
      const metadata = createMockMetadata('shared_table');
      await cache1.put('shared_table', metadata);
      await cache2.put('shared_table', metadata);

      // Invalidate in cache1
      await cache1.invalidate('shared_table');

      // Invalidation should propagate to cache2
      await coherenceManager.flush();

      expect(await cache2.get('shared_table')).toBeNull();
    });

    /**
     * GAP: No version vector for conflict resolution
     * Expected: Should use version vectors to detect concurrent modifications
     */
    it('should use version vectors for conflict detection', async () => {
      const { MetadataCache, CacheCoherenceManager } = await import('../dolake/metadata-cache.js');

      const coherenceManager = new CacheCoherenceManager();

      const cache1 = new MetadataCache({
        enabled: true,
        ttlMs: 60_000,
        maxTables: 100,
        enableCoherence: true,
        latencyThresholdUs: 1000,
      });
      const cache2 = new MetadataCache({
        enabled: true,
        ttlMs: 60_000,
        maxTables: 100,
        enableCoherence: true,
        latencyThresholdUs: 1000,
      });

      coherenceManager.register('do-1', cache1);
      coherenceManager.register('do-2', cache2);

      // Concurrent updates
      const metadata1 = createMockMetadata('concurrent_table', 1);
      const metadata2 = createMockMetadata('concurrent_table', 2);

      await cache1.put('concurrent_table', metadata1);
      await cache2.put('concurrent_table', metadata2);

      // Coherence manager should detect conflict
      const conflicts = await coherenceManager.detectConflicts();
      expect(conflicts.length).toBeGreaterThan(0);
      expect(conflicts[0].tableId).toBe('concurrent_table');
    });

    /**
     * GAP: No eventual consistency mode
     * Expected: Should support eventual consistency for performance
     */
    it('should support eventual consistency mode', async () => {
      const { MetadataCache, CacheCoherenceManager } = await import('../dolake/metadata-cache.js');

      const coherenceManager = new CacheCoherenceManager({
        mode: 'eventual', // Not strict consistency
        propagationDelayMs: 100,
      });

      const cache1 = new MetadataCache({
        enabled: true,
        ttlMs: 60_000,
        maxTables: 100,
        enableCoherence: true,
        latencyThresholdUs: 1000,
      });
      const cache2 = new MetadataCache({
        enabled: true,
        ttlMs: 60_000,
        maxTables: 100,
        enableCoherence: true,
        latencyThresholdUs: 1000,
      });

      coherenceManager.register('do-1', cache1);
      coherenceManager.register('do-2', cache2);

      const metadata = createMockMetadata('eventual_table');
      await cache1.put('eventual_table', metadata);
      await cache2.put('eventual_table', metadata);

      // Invalidate in cache1
      await cache1.invalidate('eventual_table');

      // Immediately after, cache2 may still have the data (eventual consistency)
      const immediateResult = await cache2.get('eventual_table');
      // Result may or may not be null depending on timing

      // After propagation delay, cache2 should be invalidated
      await new Promise((resolve) => setTimeout(resolve, 150));
      await coherenceManager.flush();

      expect(await cache2.get('eventual_table')).toBeNull();
    });

    /**
     * GAP: No coherence message batching
     * Expected: Should batch coherence messages for efficiency
     */
    it('should batch coherence messages', async () => {
      const { MetadataCache, CacheCoherenceManager } = await import('../dolake/metadata-cache.js');

      const coherenceManager = new CacheCoherenceManager({
        batchSize: 10,
        batchDelayMs: 50,
      });

      const onBatchSent = vi.fn();
      coherenceManager.on('batchSent', onBatchSent);

      const cache1 = new MetadataCache({
        enabled: true,
        ttlMs: 60_000,
        maxTables: 100,
        enableCoherence: true,
        latencyThresholdUs: 1000,
      });

      coherenceManager.register('do-1', cache1);

      // Multiple rapid invalidations
      for (let i = 0; i < 5; i++) {
        await cache1.put(`table_${i}`, createMockMetadata(`table_${i}`));
        await cache1.invalidate(`table_${i}`);
      }

      await coherenceManager.flush();

      // Should have sent 1 batch, not 5 individual messages
      expect(onBatchSent).toHaveBeenCalledTimes(1);
      const batchArg = onBatchSent.mock.calls[0][0];
      expect(batchArg.messageCount).toBe(5);
    });

    /**
     * GAP: No coherence health monitoring
     * Expected: Should monitor coherence status and detect issues
     */
    it('should monitor coherence health', async () => {
      const { MetadataCache, CacheCoherenceManager } = await import('../dolake/metadata-cache.js');

      const coherenceManager = new CacheCoherenceManager({
        healthCheckIntervalMs: 1000,
      });

      const cache1 = new MetadataCache({
        enabled: true,
        ttlMs: 60_000,
        maxTables: 100,
        enableCoherence: true,
        latencyThresholdUs: 1000,
      });
      const cache2 = new MetadataCache({
        enabled: true,
        ttlMs: 60_000,
        maxTables: 100,
        enableCoherence: true,
        latencyThresholdUs: 1000,
      });

      coherenceManager.register('do-1', cache1);
      coherenceManager.register('do-2', cache2);

      const health = await coherenceManager.getHealth();

      expect(health.status).toBe('healthy');
      expect(health.registeredDOs).toBe(2);
      expect(health.pendingMessages).toBe(0);
      expect(health.lastSyncTimestamp).toBeDefined();
    });

    /**
     * GAP: No partition-aware coherence
     * Expected: Should support partition-level cache coherence
     */
    it('should support partition-level coherence', async () => {
      const { MetadataCache, CacheCoherenceManager } = await import('../dolake/metadata-cache.js');

      const coherenceManager = new CacheCoherenceManager();

      const cache1 = new MetadataCache({
        enabled: true,
        ttlMs: 60_000,
        maxTables: 100,
        enableCoherence: true,
        latencyThresholdUs: 1000,
      });
      const cache2 = new MetadataCache({
        enabled: true,
        ttlMs: 60_000,
        maxTables: 100,
        enableCoherence: true,
        latencyThresholdUs: 1000,
      });

      coherenceManager.register('do-1', cache1);
      coherenceManager.register('do-2', cache2);

      // Cache partition-specific metadata
      await cache1.putPartition('events', 'day=2024-01-15', { rowCount: 1000 });
      await cache2.putPartition('events', 'day=2024-01-15', { rowCount: 1000 });

      // Invalidate specific partition
      await cache1.invalidatePartition('events', 'day=2024-01-15');
      await coherenceManager.flush();

      // Only that partition should be invalidated in cache2
      expect(await cache2.getPartition('events', 'day=2024-01-15')).toBeNull();
      // Other partitions should remain cached
      expect(await cache2.getPartition('events', 'day=2024-01-14')).toBeDefined();
    });
  });

  // ===========================================================================
  // Test Suite: Integration with Existing Cache Infrastructure
  // ===========================================================================

  describe('Integration with Existing CacheInvalidator', () => {
    /**
     * GAP: No integration with existing CacheInvalidator from dolake
     * Expected: MetadataCache should work alongside CacheInvalidator
     */
    it('should integrate with existing CacheInvalidator', async () => {
      const { MetadataCache } = await import('../dolake/metadata-cache.js');
      const { CacheInvalidator } = await import('../dolake/cache-invalidation.js');

      const metadataCache = new MetadataCache({
        enabled: true,
        ttlMs: 60_000,
        maxTables: 100,
        enableCoherence: false,
        latencyThresholdUs: 1000,
      });

      const queryCache = new CacheInvalidator({
        enabled: true,
        ttlMs: 300_000,
      });

      // Cache both metadata and query results
      await metadataCache.put('users', createMockMetadata('users'));
      queryCache.registerCacheEntry('users');

      // CDC event should invalidate both caches
      const cdcEvent = {
        operation: 'INSERT' as const,
        table: 'users',
        after: { id: 1, name: 'Alice' },
        timestamp: Date.now(),
        sequence: 1,
        rowId: 'row-1',
      };

      await queryCache.processCDCEvents([cdcEvent]);

      // Query cache should be invalidated
      const queryStatus = queryCache.getCacheEntryStatus('users');
      expect(queryStatus?.cached).toBe(false);

      // Metadata cache should NOT be invalidated by INSERT (only schema changes)
      const metadataResult = await metadataCache.get('users');
      expect(metadataResult).toBeDefined();
    });

    /**
     * GAP: No unified cache management API
     * Expected: Should have a unified API to manage all caches
     */
    it('should support unified cache management', async () => {
      const { createUnifiedCacheManager } = await import('../dolake/metadata-cache.js');

      const cacheManager = await createUnifiedCacheManager({
        metadata: {
          enabled: true,
          ttlMs: 300_000,
          maxTables: 1000,
        },
        query: {
          enabled: true,
          ttlMs: 60_000,
        },
        partition: {
          enabled: true,
          ttlMs: 120_000,
        },
      });

      // Unified API for all cache types
      await cacheManager.invalidateAll('users');

      const stats = cacheManager.getAllStats();
      expect(stats.metadata).toBeDefined();
      expect(stats.query).toBeDefined();
      expect(stats.partition).toBeDefined();
    });
  });
});
