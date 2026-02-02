/**
 * Metadata Cache Tests for DoLake Module
 *
 * Tests for the DO-resident Iceberg table metadata cache including:
 * - TTL-based expiration
 * - LRU eviction
 * - Concurrent access patterns
 * - Persistence across DO hibernation
 * - Cache coherence across multiple DO instances
 *
 * Uses real implementations (no mocks) per project philosophy.
 *
 * @module dolake/__tests__/metadata-cache
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  MetadataCache,
  CacheCoherenceManager,
  processSchemaChangeEvent,
  processPartitionSpecChange,
  processTableDrop,
  estimateMemoryUsage,
  createUnifiedCacheManager,
  type IcebergMetadata,
  type MetadataCacheConfig,
  type PartitionSpec,
  type PartitionStats,
} from '../metadata-cache.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create mock Iceberg table metadata for testing
 */
function createMockMetadata(tableId: string, schemaId = 1): IcebergMetadata {
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

/**
 * Create default test config
 */
function createTestConfig(overrides: Partial<MetadataCacheConfig> = {}): MetadataCacheConfig {
  return {
    enabled: true,
    ttlMs: 60_000,
    maxTables: 100,
    enableCoherence: false,
    latencyThresholdUs: 1000,
    ...overrides,
  };
}

// =============================================================================
// Test Suite: MetadataCache Class
// =============================================================================

describe('MetadataCache', () => {
  describe('Configuration', () => {
    it('should accept configuration options', () => {
      const config = createTestConfig({
        ttlMs: 300_000,
        maxTables: 1000,
      });

      const cache = new MetadataCache(config);

      expect(cache.config.enabled).toBe(true);
      expect(cache.config.ttlMs).toBe(300_000);
      expect(cache.config.maxTables).toBe(1000);
    });

    it('should support invalidation strategies', () => {
      const config = createTestConfig({
        invalidationStrategies: {
          SCHEMA_CHANGE: 'immediate',
          SNAPSHOT_APPEND: 'refresh',
          PROPERTY_CHANGE: 'lazy',
        },
      });

      const cache = new MetadataCache(config);
      expect(cache.config.invalidationStrategies?.SCHEMA_CHANGE).toBe('immediate');
    });

    it('should support latency violation callback', () => {
      const onViolation = vi.fn();
      const config = createTestConfig({
        latencyThresholdUs: 100,
        onLatencyViolation: onViolation,
      });

      const cache = new MetadataCache(config);
      expect(cache).toBeDefined();
    });
  });

  describe('Basic Cache Operations', () => {
    let cache: MetadataCache;

    beforeEach(() => {
      cache = new MetadataCache(createTestConfig());
    });

    it('should store metadata in cache', async () => {
      const metadata = createMockMetadata('users');

      await cache.put('users', metadata);

      const entry = await cache.getEntry('users');
      expect(entry).toBeDefined();
      expect(entry?.metadata['table-uuid']).toBe('users');
    });

    it('should retrieve cached metadata', async () => {
      const metadata = createMockMetadata('orders');
      await cache.put('orders', metadata);

      const retrieved = await cache.get('orders');

      expect(retrieved).toBeDefined();
      expect(retrieved?.['table-uuid']).toBe('orders');
    });

    it('should return null for non-cached table', async () => {
      const result = await cache.get('nonexistent');
      expect(result).toBeNull();
    });

    it('should track cache entry metadata', async () => {
      const metadata = createMockMetadata('events');
      await cache.put('events', metadata);

      const entry = await cache.getEntry('events');

      expect(entry?.cachedAt).toBeLessThanOrEqual(Date.now());
      expect(entry?.expiresAt).toBeGreaterThan(Date.now());
      expect(entry?.version).toBe(1); // current-schema-id
      expect(entry?.hitCount).toBe(0);
    });

    it('should increment hit count on access', async () => {
      const metadata = createMockMetadata('users');
      await cache.put('users', metadata);

      await cache.get('users');
      await cache.get('users');
      await cache.get('users');

      const entry = await cache.getEntry('users');
      expect(entry?.hitCount).toBe(3);
    });
  });

  describe('Cache Statistics', () => {
    let cache: MetadataCache;

    beforeEach(() => {
      cache = new MetadataCache(createTestConfig());
    });

    it('should track cache hits and misses', async () => {
      const metadata = createMockMetadata('users');
      await cache.put('users', metadata);

      await cache.get('users'); // hit
      await cache.get('users'); // hit
      await cache.get('nonexistent'); // miss

      const stats = cache.getStats();
      expect(stats.hits).toBe(2);
      expect(stats.misses).toBe(1);
    });

    it('should calculate hit rate correctly', async () => {
      const metadata = createMockMetadata('users');
      await cache.put('users', metadata);

      await cache.get('users');
      await cache.get('users');
      await cache.get('nonexistent');

      const stats = cache.getStats();
      expect(stats.hitRate).toBeCloseTo(2 / 3, 2);
    });

    it('should track entry count', async () => {
      await cache.put('t1', createMockMetadata('t1'));
      await cache.put('t2', createMockMetadata('t2'));
      await cache.put('t3', createMockMetadata('t3'));

      const stats = cache.getStats();
      expect(stats.entriesCount).toBe(3);
    });

    it('should track evictions', async () => {
      const smallCache = new MetadataCache(createTestConfig({ maxTables: 2 }));

      await smallCache.put('t1', createMockMetadata('t1'));
      await smallCache.put('t2', createMockMetadata('t2'));
      await smallCache.put('t3', createMockMetadata('t3')); // Should evict one

      const stats = smallCache.getStats();
      expect(stats.evictions).toBe(1);
    });
  });

  describe('TTL Expiration', () => {
    afterEach(() => {
      vi.useRealTimers();
    });

    it('should expire entries after TTL', async () => {
      vi.useFakeTimers();

      const cache = new MetadataCache(createTestConfig({ ttlMs: 1000 }));
      const metadata = createMockMetadata('expiring');

      await cache.put('expiring', metadata);

      // Should be cached initially
      let result = await cache.get('expiring');
      expect(result).toBeDefined();

      // Advance time past TTL
      vi.advanceTimersByTime(1500);

      // Should be expired
      result = await cache.get('expiring');
      expect(result).toBeNull();
    });

    it('should track expirations in stats', async () => {
      vi.useFakeTimers();

      const cache = new MetadataCache(createTestConfig({ ttlMs: 1000 }));
      await cache.put('expiring', createMockMetadata('expiring'));

      vi.advanceTimersByTime(1500);
      await cache.get('expiring'); // Triggers expiration check

      const stats = cache.getStats();
      expect(stats.expirations).toBe(1);
    });

    it('should not expire entry before TTL', async () => {
      vi.useFakeTimers();

      const cache = new MetadataCache(createTestConfig({ ttlMs: 5000 }));
      await cache.put('valid', createMockMetadata('valid'));

      vi.advanceTimersByTime(1000); // Only 1 second

      const result = await cache.get('valid');
      expect(result).toBeDefined();
    });
  });

  describe('LRU Eviction', () => {
    it('should evict least recently used entry when capacity exceeded', async () => {
      const cache = new MetadataCache(createTestConfig({ maxTables: 3 }));

      await cache.put('t1', createMockMetadata('t1'));
      await cache.get('t1'); // Access to make recently used
      await cache.put('t2', createMockMetadata('t2'));
      await cache.put('t3', createMockMetadata('t3'));

      // Access t1 again to make it most recently used
      await cache.get('t1');

      // Add t4 - should evict t2 (least recently used)
      await cache.put('t4', createMockMetadata('t4'));

      expect(await cache.get('t1')).toBeDefined();
      expect(await cache.get('t2')).toBeNull(); // Evicted
      expect(await cache.get('t3')).toBeDefined();
      expect(await cache.get('t4')).toBeDefined();
    });

    it('should update LRU order on access', async () => {
      const cache = new MetadataCache(createTestConfig({ maxTables: 2 }));

      await cache.put('t1', createMockMetadata('t1'));
      await cache.put('t2', createMockMetadata('t2'));

      // Access t1 to make it recently used
      await cache.get('t1');

      // Add t3 - should evict t2 (now LRU)
      await cache.put('t3', createMockMetadata('t3'));

      expect(await cache.get('t1')).toBeDefined();
      expect(await cache.get('t2')).toBeNull();
      expect(await cache.get('t3')).toBeDefined();
    });

    it('should not evict when updating existing entry', async () => {
      const cache = new MetadataCache(createTestConfig({ maxTables: 2 }));

      await cache.put('t1', createMockMetadata('t1', 1));
      await cache.put('t2', createMockMetadata('t2'));

      // Update t1 with new version
      await cache.put('t1', createMockMetadata('t1', 2));

      expect(await cache.get('t1')).toBeDefined();
      expect(await cache.get('t2')).toBeDefined();

      const stats = cache.getStats();
      expect(stats.evictions).toBe(0);
    });
  });

  describe('Cache Invalidation', () => {
    let cache: MetadataCache;

    beforeEach(() => {
      cache = new MetadataCache(createTestConfig());
    });

    it('should invalidate a cached entry', async () => {
      await cache.put('users', createMockMetadata('users'));

      await cache.invalidate('users');

      const result = await cache.get('users');
      expect(result).toBeNull();
    });

    it('should clear auxiliary data on invalidation', async () => {
      await cache.put('users', createMockMetadata('users'));
      await cache.putAuxiliary('users', 'stats', { rows: 1000 });

      await cache.invalidate('users');

      const aux = await cache.getAuxiliary('users', 'stats');
      expect(aux).toBeNull();
    });

    it('should clear partition data on invalidation', async () => {
      await cache.put('events', createMockMetadata('events'));
      await cache.putPartition('events', 'day=2024-01-15', { rowCount: 500 });

      await cache.invalidate('events');

      const partition = await cache.getPartition('events', 'day=2024-01-15');
      expect(partition).toEqual({ rowCount: 0 }); // Empty placeholder for non-existent table
    });

    it('should handle invalidation of non-existent entry', async () => {
      // Should not throw
      await cache.invalidate('nonexistent');
    });
  });

  describe('Lazy Invalidation', () => {
    let cache: MetadataCache;

    beforeEach(() => {
      cache = new MetadataCache(createTestConfig());
    });

    it('should mark entry for lazy invalidation', async () => {
      await cache.put('users', createMockMetadata('users'));

      await cache.markForLazyInvalidation('users', 'PROPERTY_CHANGE');

      const entry = await cache.getEntry('users');
      expect(entry?.pendingInvalidation).toBe(true);
    });

    it('should invalidate on access when pending', async () => {
      await cache.put('users', createMockMetadata('users'));
      await cache.markForLazyInvalidation('users', 'PROPERTY_CHANGE');

      const result = await cache.get('users');
      expect(result).toBeNull();
    });
  });

  describe('Auxiliary Data', () => {
    let cache: MetadataCache;

    beforeEach(() => {
      cache = new MetadataCache(createTestConfig());
    });

    it('should store auxiliary data for a table', async () => {
      await cache.putAuxiliary('users', 'stats', { rows: 1000, size: 5000 });

      const aux = await cache.getAuxiliary('users', 'stats');
      expect(aux).toEqual({ rows: 1000, size: 5000 });
    });

    it('should support multiple auxiliary keys per table', async () => {
      await cache.putAuxiliary('users', 'stats', { rows: 1000 });
      await cache.putAuxiliary('users', 'schema', { columns: 5 });

      expect(await cache.getAuxiliary('users', 'stats')).toEqual({ rows: 1000 });
      expect(await cache.getAuxiliary('users', 'schema')).toEqual({ columns: 5 });
    });

    it('should return null for non-existent auxiliary data', async () => {
      const aux = await cache.getAuxiliary('users', 'nonexistent');
      expect(aux).toBeNull();
    });
  });

  describe('Partition Data', () => {
    let cache: MetadataCache;

    beforeEach(() => {
      cache = new MetadataCache(createTestConfig());
    });

    it('should store partition-specific data', async () => {
      await cache.putPartition('events', 'day=2024-01-15', { rowCount: 500 });

      const partition = await cache.getPartition('events', 'day=2024-01-15');
      expect(partition).toEqual({ rowCount: 500 });
    });

    it('should invalidate specific partition', async () => {
      await cache.putPartition('events', 'day=2024-01-15', { rowCount: 500 });
      await cache.putPartition('events', 'day=2024-01-16', { rowCount: 600 });

      await cache.invalidatePartition('events', 'day=2024-01-15');

      expect(await cache.getPartition('events', 'day=2024-01-15')).toBeNull();
      expect(await cache.getPartition('events', 'day=2024-01-16')).toEqual({ rowCount: 600 });
    });
  });

  describe('Partition Statistics', () => {
    let cache: MetadataCache;

    beforeEach(() => {
      cache = new MetadataCache(createTestConfig());
    });

    it('should store and retrieve partition statistics', async () => {
      const spec: PartitionSpec = { values: { year: 2024, month: 1 } };
      const stats: PartitionStats = {
        recordCount: 1000,
        fileCount: 5,
        sizeBytes: 50000,
        lastModifiedMs: Date.now(),
        collectedAt: Date.now(),
      };

      await cache.updatePartitionStats('events', spec, stats);

      const retrieved = await cache.getPartitionStats('events', spec);
      expect(retrieved?.recordCount).toBe(1000);
      expect(retrieved?.fileCount).toBe(5);
    });

    it('should track partition access', async () => {
      const spec: PartitionSpec = { values: { day: '2024-01-15' } };
      const stats: PartitionStats = {
        recordCount: 1000,
        fileCount: 5,
        sizeBytes: 50000,
        lastModifiedMs: Date.now(),
        collectedAt: Date.now(),
      };

      await cache.updatePartitionStats('events', spec, stats);

      // Access multiple times
      await cache.getPartitionStats('events', spec);
      await cache.getPartitionStats('events', spec);
      await cache.getPartitionStats('events', spec);

      const metrics = cache.getPartitionAccessMetrics('events', spec);
      expect(metrics?.accessCount).toBe(3);
    });

    it('should list all partitions for a table', async () => {
      await cache.updatePartitionStats(
        'events',
        { values: { day: '2024-01-15' } },
        { recordCount: 100, fileCount: 1, sizeBytes: 1000, lastModifiedMs: Date.now(), collectedAt: Date.now() }
      );
      await cache.updatePartitionStats(
        'events',
        { values: { day: '2024-01-16' } },
        { recordCount: 200, fileCount: 2, sizeBytes: 2000, lastModifiedMs: Date.now(), collectedAt: Date.now() }
      );

      const partitions = await cache.listPartitions('events');
      expect(partitions.length).toBe(2);
    });

    it('should provide table partition summary', async () => {
      await cache.updatePartitionStats(
        'events',
        { values: { day: '2024-01-15' } },
        { recordCount: 100, fileCount: 1, sizeBytes: 1000, lastModifiedMs: Date.now(), collectedAt: Date.now() }
      );
      await cache.updatePartitionStats(
        'events',
        { values: { day: '2024-01-16' } },
        { recordCount: 200, fileCount: 2, sizeBytes: 2000, lastModifiedMs: Date.now(), collectedAt: Date.now() }
      );

      const summary = await cache.getTablePartitionSummary('events');
      expect(summary.partitionCount).toBe(2);
      expect(summary.totalRecordCount).toBe(300);
      expect(summary.totalFileCount).toBe(3);
      expect(summary.totalSizeBytes).toBe(3000);
    });

    it('should identify hot partitions', async () => {
      cache.configurePartitionStats({
        accessRateWindowMs: 60_000,
        hotPartitionThreshold: 1, // Low threshold for testing
      });

      const spec: PartitionSpec = { values: { day: '2024-01-15' } };
      await cache.updatePartitionStats(
        'events',
        spec,
        { recordCount: 100, fileCount: 1, sizeBytes: 1000, lastModifiedMs: Date.now(), collectedAt: Date.now() }
      );

      // Access many times quickly
      for (let i = 0; i < 100; i++) {
        await cache.getPartitionStats('events', spec);
      }

      expect(cache.isHotPartition('events', spec)).toBe(true);
    });
  });

  describe('Persistence', () => {
    it('should persist cache to storage on flush', async () => {
      const mockStorage = new Map<string, unknown>();
      const cache = new MetadataCache(createTestConfig(), { storage: mockStorage });

      await cache.put('users', createMockMetadata('users'));
      await cache.flush();

      expect(mockStorage.has('__metadata_cache__')).toBe(true);
      expect(mockStorage.has('__metadata_cache_stats__')).toBe(true);
    });

    it('should restore cache from storage', async () => {
      const mockStorage = new Map<string, unknown>();

      // Create and populate first cache
      const cache1 = new MetadataCache(createTestConfig(), { storage: mockStorage });
      await cache1.put('users', createMockMetadata('users'));
      await cache1.flush();

      // Create second cache and restore
      const cache2 = new MetadataCache(createTestConfig(), { storage: mockStorage });
      await cache2.restore();

      const recovered = await cache2.get('users');
      expect(recovered).toBeDefined();
      expect(recovered?.['table-uuid']).toBe('users');
    });

    it('should not restore expired entries', async () => {
      vi.useFakeTimers();

      const mockStorage = new Map<string, unknown>();
      const cache1 = new MetadataCache(createTestConfig({ ttlMs: 1000 }), { storage: mockStorage });

      await cache1.put('expiring', createMockMetadata('expiring'));
      await cache1.flush();

      // Advance time past TTL
      vi.advanceTimersByTime(2000);

      const cache2 = new MetadataCache(createTestConfig({ ttlMs: 1000 }), { storage: mockStorage });
      await cache2.restore();

      const result = await cache2.get('expiring');
      expect(result).toBeNull();

      vi.useRealTimers();
    });

    it('should persist partition statistics', async () => {
      const mockStorage = new Map<string, unknown>();
      const cache1 = new MetadataCache(createTestConfig(), { storage: mockStorage });

      await cache1.updatePartitionStats(
        'events',
        { values: { day: '2024-01-15' } },
        { recordCount: 100, fileCount: 1, sizeBytes: 1000, lastModifiedMs: Date.now(), collectedAt: Date.now() }
      );
      await cache1.flush();

      const cache2 = new MetadataCache(createTestConfig(), { storage: mockStorage });
      await cache2.restore();

      const stats = await cache2.getPartitionStats('events', { values: { day: '2024-01-15' } });
      expect(stats?.recordCount).toBe(100);
    });
  });

  describe('Latency Tracking', () => {
    let cache: MetadataCache;

    beforeEach(() => {
      cache = new MetadataCache(createTestConfig());
    });

    it('should track latency statistics', async () => {
      await cache.put('users', createMockMetadata('users'));

      for (let i = 0; i < 100; i++) {
        await cache.get('users');
      }

      const latencyStats = cache.getLatencyStats();
      expect(latencyStats.p50Us).toBeDefined();
      expect(latencyStats.p99Us).toBeDefined();
      expect(latencyStats.p999Us).toBeDefined();
      expect(latencyStats.avgUs).toBeDefined();
    });

    it('should return zero latency stats when no operations', () => {
      const latencyStats = cache.getLatencyStats();
      expect(latencyStats.p50Us).toBe(0);
      expect(latencyStats.avgUs).toBe(0);
    });

    it('should achieve sub-millisecond latency for cache hits', async () => {
      await cache.put('fast', createMockMetadata('fast'));

      // Warm up
      await cache.get('fast');

      const iterations = 1000;
      const startTime = performance.now();

      for (let i = 0; i < iterations; i++) {
        await cache.get('fast');
      }

      const avgLatencyMs = (performance.now() - startTime) / iterations;
      expect(avgLatencyMs).toBeLessThan(1);
    });
  });

  describe('Pre-warming', () => {
    it('should pre-warm cache with multiple tables', async () => {
      const cache = new MetadataCache(createTestConfig());

      const tables = ['users', 'orders', 'events', 'sessions'];
      const loader = async (tableId: string) => createMockMetadata(tableId);

      await cache.prewarm(tables, loader);

      for (const table of tables) {
        const metadata = await cache.get(table);
        expect(metadata).toBeDefined();
      }

      expect(cache.getStats().entriesCount).toBe(4);
    });
  });

  describe('Concurrent Access Patterns', () => {
    let cache: MetadataCache;

    beforeEach(() => {
      cache = new MetadataCache(createTestConfig());
    });

    it('should handle concurrent puts', async () => {
      const tables = ['t1', 't2', 't3', 't4', 't5'];

      await Promise.all(tables.map((t) => cache.put(t, createMockMetadata(t))));

      for (const t of tables) {
        expect(await cache.get(t)).toBeDefined();
      }
    });

    it('should handle concurrent gets', async () => {
      await cache.put('users', createMockMetadata('users'));

      const results = await Promise.all(
        Array.from({ length: 100 }).map(() => cache.get('users'))
      );

      expect(results.every((r) => r?.['table-uuid'] === 'users')).toBe(true);
    });

    it('should handle concurrent puts and gets', async () => {
      const operations = [];

      for (let i = 0; i < 50; i++) {
        operations.push(cache.put(`table_${i % 10}`, createMockMetadata(`table_${i % 10}`)));
        operations.push(cache.get(`table_${i % 10}`));
      }

      await Promise.all(operations);

      // Cache should be in consistent state
      const stats = cache.getStats();
      expect(stats.entriesCount).toBeLessThanOrEqual(10);
    });

    it('should handle concurrent invalidations', async () => {
      await cache.put('users', createMockMetadata('users'));

      await Promise.all(
        Array.from({ length: 10 }).map(() => cache.invalidate('users'))
      );

      expect(await cache.get('users')).toBeNull();
    });
  });
});

// =============================================================================
// Test Suite: CacheCoherenceManager
// =============================================================================

describe('CacheCoherenceManager', () => {
  describe('Registration', () => {
    it('should register cache instances', () => {
      const manager = new CacheCoherenceManager();
      const cache1 = new MetadataCache(createTestConfig({ enableCoherence: true }));
      const cache2 = new MetadataCache(createTestConfig({ enableCoherence: true }));

      manager.register('do-1', cache1);
      manager.register('do-2', cache2);

      // No exceptions should be thrown
    });

    it('should unregister cache instances', async () => {
      const manager = new CacheCoherenceManager();
      const cache = new MetadataCache(createTestConfig({ enableCoherence: true }));

      manager.register('do-1', cache);
      manager.unregister('do-1');

      const health = await manager.getHealth();
      expect(health.registeredDOs).toBe(0);
    });
  });

  describe('Invalidation Propagation', () => {
    it('should propagate invalidation across DO instances', async () => {
      const manager = new CacheCoherenceManager();
      const cache1 = new MetadataCache(createTestConfig({ enableCoherence: true }));
      const cache2 = new MetadataCache(createTestConfig({ enableCoherence: true }));

      manager.register('do-1', cache1);
      manager.register('do-2', cache2);

      const metadata = createMockMetadata('shared');
      await cache1.put('shared', metadata);
      await cache2.put('shared', metadata);

      await cache1.invalidate('shared');
      await manager.flush();

      expect(await cache2.get('shared')).toBeNull();
    });

    it('should not invalidate source cache again', async () => {
      const manager = new CacheCoherenceManager();
      const cache1 = new MetadataCache(createTestConfig({ enableCoherence: true }));

      manager.register('do-1', cache1);

      await cache1.put('local', createMockMetadata('local'));
      await cache1.invalidate('local');
      await manager.flush();

      // Source cache should already be invalidated, no loops
      expect(await cache1.get('local')).toBeNull();
    });
  });

  describe('Partition Invalidation', () => {
    it('should propagate partition invalidation', async () => {
      const manager = new CacheCoherenceManager();
      const cache1 = new MetadataCache(createTestConfig({ enableCoherence: true }));
      const cache2 = new MetadataCache(createTestConfig({ enableCoherence: true }));

      manager.register('do-1', cache1);
      manager.register('do-2', cache2);

      await cache1.putPartition('events', 'day=2024-01-15', { rowCount: 500 });
      await cache2.putPartition('events', 'day=2024-01-15', { rowCount: 500 });

      await cache1.invalidatePartition('events', 'day=2024-01-15');
      await manager.flush();

      expect(await cache2.getPartition('events', 'day=2024-01-15')).toBeNull();
    });
  });

  describe('Conflict Detection', () => {
    it('should detect version conflicts', async () => {
      const manager = new CacheCoherenceManager();
      const cache1 = new MetadataCache(createTestConfig({ enableCoherence: true }));
      const cache2 = new MetadataCache(createTestConfig({ enableCoherence: true }));

      manager.register('do-1', cache1);
      manager.register('do-2', cache2);

      // Put different versions in each cache
      await cache1.put('concurrent', createMockMetadata('concurrent', 1));
      await cache2.put('concurrent', createMockMetadata('concurrent', 2));

      const conflicts = await manager.detectConflicts();
      expect(conflicts.length).toBeGreaterThan(0);
      expect(conflicts[0].tableId).toBe('concurrent');
    });
  });

  describe('Health Monitoring', () => {
    it('should report health status', async () => {
      const manager = new CacheCoherenceManager();
      const cache1 = new MetadataCache(createTestConfig({ enableCoherence: true }));
      const cache2 = new MetadataCache(createTestConfig({ enableCoherence: true }));

      manager.register('do-1', cache1);
      manager.register('do-2', cache2);

      const health = await manager.getHealth();

      expect(health.status).toBe('healthy');
      expect(health.registeredDOs).toBe(2);
      expect(health.pendingMessages).toBe(0);
      expect(health.lastSyncTimestamp).toBeDefined();
    });
  });

  describe('Eventual Consistency', () => {
    it('should support eventual consistency mode', async () => {
      const manager = new CacheCoherenceManager({
        mode: 'eventual',
        propagationDelayMs: 100,
      });

      const cache1 = new MetadataCache(createTestConfig({ enableCoherence: true }));
      const cache2 = new MetadataCache(createTestConfig({ enableCoherence: true }));

      manager.register('do-1', cache1);
      manager.register('do-2', cache2);

      const metadata = createMockMetadata('eventual');
      await cache1.put('eventual', metadata);
      await cache2.put('eventual', metadata);

      await cache1.invalidate('eventual');

      // Flush with delay for eventual consistency
      await manager.flush();

      // After propagation, cache2 should be invalidated
      expect(await cache2.get('eventual')).toBeNull();
    });
  });

  describe('Message Batching', () => {
    it('should batch coherence messages', async () => {
      const manager = new CacheCoherenceManager({
        batchSize: 10,
        batchDelayMs: 50,
      });

      const onBatchSent = vi.fn();
      manager.on('batchSent', onBatchSent);

      const cache = new MetadataCache(createTestConfig({ enableCoherence: true }));
      manager.register('do-1', cache);

      // Multiple rapid invalidations
      for (let i = 0; i < 5; i++) {
        await cache.put(`table_${i}`, createMockMetadata(`table_${i}`));
        await cache.invalidate(`table_${i}`);
      }

      await manager.flush();

      expect(onBatchSent).toHaveBeenCalledTimes(1);
      expect(onBatchSent.mock.calls[0][0].messageCount).toBe(5);
    });
  });
});

// =============================================================================
// Test Suite: Helper Functions
// =============================================================================

describe('Helper Functions', () => {
  describe('processSchemaChangeEvent', () => {
    it('should invalidate cache on schema change', async () => {
      const cache = new MetadataCache(createTestConfig());
      await cache.put('users', createMockMetadata('users'));

      await processSchemaChangeEvent(cache, {
        type: 'schema_change',
        table: 'users',
        operation: 'ADD_COLUMN',
        column: { id: 4, name: 'email', type: 'string', required: false },
        newSchemaId: 2,
        timestamp: Date.now(),
      });

      expect(await cache.get('users')).toBeNull();
    });
  });

  describe('processPartitionSpecChange', () => {
    it('should invalidate cache on partition spec change', async () => {
      const cache = new MetadataCache(createTestConfig());
      await cache.put('events', createMockMetadata('events'));

      await processPartitionSpecChange(cache, 'events', {
        oldSpecId: 0,
        newSpecId: 1,
        newFields: [
          { 'source-id': 3, 'field-id': 1000, name: 'day', transform: 'day' },
        ],
      });

      expect(await cache.get('events')).toBeNull();
    });
  });

  describe('processTableDrop', () => {
    it('should clear all data for dropped table', async () => {
      const cache = new MetadataCache(createTestConfig());
      await cache.put('dropped', createMockMetadata('dropped'));
      await cache.putAuxiliary('dropped', 'stats', { rows: 1000 });

      await processTableDrop(cache, 'dropped');

      expect(await cache.get('dropped')).toBeNull();
      expect(await cache.getAuxiliary('dropped', 'stats')).toBeNull();
    });
  });

  describe('estimateMemoryUsage', () => {
    it('should estimate memory usage', async () => {
      const cache = new MetadataCache(createTestConfig());
      await cache.put('t1', createMockMetadata('t1'));
      await cache.put('t2', createMockMetadata('t2'));
      await cache.put('t3', createMockMetadata('t3'));

      const usage = estimateMemoryUsage(cache);

      expect(usage.totalBytes).toBeGreaterThan(0);
      expect(usage.perEntryAvgBytes).toBeGreaterThan(0);
    });
  });

  describe('createUnifiedCacheManager', () => {
    it('should create unified cache manager', async () => {
      const manager = await createUnifiedCacheManager({
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

      expect(manager.metadata).toBeDefined();
      expect(manager.invalidateAll).toBeDefined();
      expect(manager.getAllStats).toBeDefined();
    });

    it('should invalidate all caches', async () => {
      const manager = await createUnifiedCacheManager({
        metadata: { enabled: true, ttlMs: 60_000, maxTables: 100 },
      });

      await manager.metadata.put('users', createMockMetadata('users'));
      await manager.invalidateAll('users');

      expect(await manager.metadata.get('users')).toBeNull();
    });

    it('should return all stats', async () => {
      const manager = await createUnifiedCacheManager({
        metadata: { enabled: true, ttlMs: 60_000, maxTables: 100 },
        query: { enabled: false, ttlMs: 60_000 },
        partition: { enabled: false, ttlMs: 60_000 },
      });

      const stats = manager.getAllStats();

      expect(stats.metadata).toBeDefined();
      expect(stats.query).toBeDefined();
      expect(stats.partition).toBeDefined();
    });
  });
});
