/**
 * Tests for Vortex Hot Partition Handler
 *
 * Verifies hot partition detection, lazy conversion, and temperature classification.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  HotPartitionManager,
  createHotPartitionManager,
  createHighThroughputHotPartitionManager,
  createLowLatencyHotPartitionManager,
  planVortexAwareCompaction,
  DEFAULT_HOT_PARTITION_CONFIG,
  type HotPartitionConfig,
} from '../vortex-hot-partition.js';
import type { CDCEvent } from '../types.js';

// =============================================================================
// Test Helpers
// =============================================================================

function createTestEvent(table: string, id: number): CDCEvent {
  return {
    table,
    operation: 'INSERT',
    timestamp: Date.now(),
    lsn: BigInt(id),
    rowId: `row-${id}`,
    after: { id, name: `item-${id}`, value: Math.random() * 1000 },
    metadata: {},
  };
}

function createTestEvents(table: string, count: number): CDCEvent[] {
  return Array.from({ length: count }, (_, i) => createTestEvent(table, i));
}

// =============================================================================
// HotPartitionManager Tests
// =============================================================================

describe('HotPartitionManager', () => {
  let manager: HotPartitionManager;

  beforeEach(() => {
    manager = new HotPartitionManager();
  });

  describe('Hot Tier Operations', () => {
    it('should add events to hot tier', () => {
      const events = createTestEvents('users', 10);

      manager.addToHotTier('day=2024-01-15', 'users', events);

      expect(manager.isInHotTier('day=2024-01-15')).toBe(true);

      const retrieved = manager.readFromHotTier('day=2024-01-15');
      expect(retrieved).toHaveLength(10);
    });

    it('should return null for non-existent partition', () => {
      const result = manager.readFromHotTier('day=non-existent');
      expect(result).toBeNull();
    });

    it('should accumulate events across multiple adds', () => {
      manager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));
      manager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));
      manager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));

      const retrieved = manager.readFromHotTier('day=2024-01-15');
      expect(retrieved).toHaveLength(15);
    });

    it('should remove partition from hot tier', () => {
      manager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));

      const removed = manager.removeFromHotTier('day=2024-01-15');

      expect(removed).not.toBeNull();
      expect(removed?.events).toHaveLength(5);
      expect(manager.isInHotTier('day=2024-01-15')).toBe(false);
    });

    it('should clear all hot tier data', () => {
      manager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));
      manager.addToHotTier('day=2024-01-16', 'users', createTestEvents('users', 5));
      manager.addToHotTier('day=2024-01-17', 'orders', createTestEvents('orders', 5));

      manager.clearHotTier();

      expect(manager.isInHotTier('day=2024-01-15')).toBe(false);
      expect(manager.isInHotTier('day=2024-01-16')).toBe(false);
      expect(manager.isInHotTier('day=2024-01-17')).toBe(false);
    });
  });

  describe('Temperature Classification', () => {
    it('should classify recently written partition as hot', () => {
      manager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));

      const temperature = manager.getPartitionTemperature('day=2024-01-15');
      expect(temperature).toBe('hot');
    });

    it('should classify unknown partition as cold', () => {
      const temperature = manager.getPartitionTemperature('day=unknown');
      expect(temperature).toBe('cold');
    });

    it('should classify old partition as cold', () => {
      // Use custom config with short thresholds for testing
      const testManager = new HotPartitionManager({
        hotThresholdMs: 100,
        warmThresholdMs: 200,
      });

      testManager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));

      // Manually adjust the lastWriteTime to simulate old data
      const metrics = testManager.getAccessMetrics('day=2024-01-15');
      if (metrics) {
        metrics.lastWriteTime = Date.now() - 300; // 300ms ago (past warm threshold)
      }

      const temperature = testManager.getPartitionTemperature('day=2024-01-15');
      expect(temperature).toBe('cold');
    });

    it('should classify partition as warm based on age', () => {
      const testManager = new HotPartitionManager({
        hotThresholdMs: 100,
        warmThresholdMs: 500,
      });

      testManager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));

      // Adjust to warm age
      const metrics = testManager.getAccessMetrics('day=2024-01-15');
      if (metrics) {
        metrics.lastWriteTime = Date.now() - 200; // 200ms ago (past hot, but not warm)
      }

      const temperature = testManager.getPartitionTemperature('day=2024-01-15');
      expect(temperature).toBe('warm');
    });

    it('should consider high-access partition as hot regardless of age', () => {
      const testManager = new HotPartitionManager({
        hotThresholdMs: 100,
        warmThresholdMs: 200,
        hotAccessCountThreshold: 5,
      });

      testManager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));

      // Make it old
      const metrics = testManager.getAccessMetrics('day=2024-01-15');
      if (metrics) {
        metrics.lastWriteTime = Date.now() - 300; // Old
        metrics.readCount = 10; // But frequently accessed
      }

      const temperature = testManager.getPartitionTemperature('day=2024-01-15');
      expect(temperature).toBe('hot');
    });
  });

  describe('Temperature Analysis', () => {
    it('should analyze temperature distribution', () => {
      manager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));
      manager.addToHotTier('day=2024-01-16', 'users', createTestEvents('users', 5));

      const analysis = manager.analyzeTemperature();

      expect(analysis.totalPartitions).toBe(2);
      expect(analysis.hot).toContain('day=2024-01-15');
      expect(analysis.hot).toContain('day=2024-01-16');
      expect(analysis.hot.length).toBe(2);
    });

    it('should provide recommendations', () => {
      const testManager = new HotPartitionManager({
        hotThresholdMs: 100,
        warmThresholdMs: 200,
      });

      testManager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));

      // Make it cold
      const metrics = testManager.getAccessMetrics('day=2024-01-15');
      if (metrics) {
        metrics.lastWriteTime = Date.now() - 300;
      }

      const analysis = testManager.analyzeTemperature();

      expect(analysis.cold.length).toBe(1);
      expect(analysis.recommendations.length).toBeGreaterThan(0);
      expect(analysis.recommendations[0].action).toBe('convert_to_parquet');
    });
  });

  describe('Lazy Conversion', () => {
    it('should convert hot tier to Parquet', async () => {
      manager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 10));

      const writeParquet = vi.fn().mockResolvedValue({
        path: 'data/users/day=2024-01-15/file.parquet',
        sizeBytes: 500,
      });

      const result = await manager.convertToParquet('day=2024-01-15', writeParquet);

      expect(result).not.toBeNull();
      expect(result?.eventsConverted).toBe(10);
      expect(result?.triggeredByCompaction).toBe(true);
      expect(writeParquet).toHaveBeenCalledTimes(1);

      // Should be removed from hot tier
      expect(manager.isInHotTier('day=2024-01-15')).toBe(false);

      // Metrics should be updated
      const metrics = manager.getAccessMetrics('day=2024-01-15');
      expect(metrics?.convertedToParquet).toBe(true);
    });

    it('should return null for non-existent partition', async () => {
      const writeParquet = vi.fn();

      const result = await manager.convertToParquet('day=non-existent', writeParquet);

      expect(result).toBeNull();
      expect(writeParquet).not.toHaveBeenCalled();
    });

    it('should batch convert multiple partitions', async () => {
      manager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));
      manager.addToHotTier('day=2024-01-16', 'users', createTestEvents('users', 5));
      manager.addToHotTier('day=2024-01-17', 'users', createTestEvents('users', 5));

      const writeParquet = vi.fn().mockResolvedValue({
        path: 'data/file.parquet',
        sizeBytes: 500,
      });

      const results = await manager.batchConvertToParquet(
        ['day=2024-01-15', 'day=2024-01-16', 'day=2024-01-17'],
        writeParquet
      );

      expect(results).toHaveLength(3);
      expect(writeParquet).toHaveBeenCalledTimes(3);
    });

    it('should get partitions ready for conversion', () => {
      const testManager = new HotPartitionManager({
        hotThresholdMs: 100,
        warmThresholdMs: 200,
      });

      testManager.addToHotTier('hot-partition', 'users', createTestEvents('users', 5));
      testManager.addToHotTier('warm-partition', 'users', createTestEvents('users', 5));
      testManager.addToHotTier('cold-partition', 'users', createTestEvents('users', 5));

      // Adjust ages
      const hotMetrics = testManager.getAccessMetrics('hot-partition');
      if (hotMetrics) hotMetrics.lastWriteTime = Date.now(); // Hot

      const warmMetrics = testManager.getAccessMetrics('warm-partition');
      if (warmMetrics) warmMetrics.lastWriteTime = Date.now() - 150; // Warm

      const coldMetrics = testManager.getAccessMetrics('cold-partition');
      if (coldMetrics) coldMetrics.lastWriteTime = Date.now() - 300; // Cold

      const eligible = testManager.getPartitionsForConversion();

      expect(eligible).toContain('warm-partition');
      expect(eligible).toContain('cold-partition');
      expect(eligible).not.toContain('hot-partition');
    });
  });

  describe('Access Tracking', () => {
    it('should track write operations', () => {
      manager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 10));
      manager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));

      const metrics = manager.getAccessMetrics('day=2024-01-15');

      expect(metrics).toBeDefined();
      expect(metrics?.writeCount).toBe(2);
      expect(metrics?.hotTierEventCount).toBe(15);
    });

    it('should track read operations', () => {
      manager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));

      manager.readFromHotTier('day=2024-01-15');
      manager.readFromHotTier('day=2024-01-15');
      manager.readFromHotTier('day=2024-01-15');

      const metrics = manager.getAccessMetrics('day=2024-01-15');

      expect(metrics?.readCount).toBe(3);
    });

    it('should update lastReadTime on read', () => {
      manager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 5));

      const before = manager.getAccessMetrics('day=2024-01-15')?.lastReadTime;

      // Small delay
      manager.readFromHotTier('day=2024-01-15');

      const after = manager.getAccessMetrics('day=2024-01-15')?.lastReadTime;

      expect(after).toBeGreaterThanOrEqual(before!);
    });
  });

  describe('Capacity Management', () => {
    it('should detect when hot tier needs flushing', () => {
      const testManager = new HotPartitionManager({
        maxHotTierEvents: 100,
        maxHotTierBytes: 1024 * 1024,
      });

      // Add enough events to trigger flush
      testManager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 150));

      expect(testManager.shouldFlushHotTier()).toBe(true);
    });

    it('should identify partitions needing flush', () => {
      const testManager = new HotPartitionManager({
        maxHotTierEvents: 100,
        maxHotTierBytes: 1024 * 1024,
      });

      testManager.addToHotTier('small', 'users', createTestEvents('users', 10));
      testManager.addToHotTier('large', 'users', createTestEvents('users', 80));

      const needingFlush = testManager.getPartitionsNeedingFlush();

      expect(needingFlush).toContain('large');
      expect(needingFlush).not.toContain('small');
    });

    it('should report hot tier statistics', () => {
      manager.addToHotTier('day=2024-01-15', 'users', createTestEvents('users', 10));
      manager.addToHotTier('day=2024-01-16', 'orders', createTestEvents('orders', 20));

      const stats = manager.getHotTierStats();

      expect(stats.partitionCount).toBe(2);
      expect(stats.totalEvents).toBe(30);
      expect(stats.totalBytes).toBeGreaterThan(0);
    });
  });

  describe('Compression', () => {
    it('should compress entries when threshold is reached', () => {
      const testManager = new HotPartitionManager({
        enableCompressedHotTier: true,
        hotTierCompressionLevel: 1,
      });

      // Add enough data to trigger compression (>10KB)
      const largeEvents = createTestEvents('users', 200);

      testManager.addToHotTier('day=2024-01-15', 'users', largeEvents);

      const entry = testManager.getHotEntry('day=2024-01-15');

      // If compression was applied, compressedData should not be null
      // Note: Compression is only applied if data > 10KB
      if (entry && entry.originalSizeBytes > 10 * 1024) {
        expect(entry.compressedData).not.toBeNull();
      }
    });

    it('should decompress on read', () => {
      const testManager = new HotPartitionManager({
        enableCompressedHotTier: true,
      });

      const events = createTestEvents('users', 200);
      testManager.addToHotTier('day=2024-01-15', 'users', events);

      const retrieved = testManager.readFromHotTier('day=2024-01-15');

      expect(retrieved).toHaveLength(200);
    });
  });
});

// =============================================================================
// Vortex-Aware Compaction Planning Tests
// =============================================================================

describe('planVortexAwareCompaction', () => {
  let manager: HotPartitionManager;

  beforeEach(() => {
    manager = new HotPartitionManager({
      hotThresholdMs: 100,
      warmThresholdMs: 200,
    });
  });

  it('should skip hot partitions', () => {
    manager.addToHotTier('hot-partition', 'users', createTestEvents('users', 5));

    const plan = planVortexAwareCompaction(manager, ['hot-partition']);

    expect(plan.partitionsToSkip).toContain('hot-partition');
    expect(plan.partitionsToConvert).not.toContain('hot-partition');
    expect(plan.partitionsToCompact).not.toContain('hot-partition');
  });

  it('should plan conversion for cold partitions', () => {
    manager.addToHotTier('cold-partition', 'users', createTestEvents('users', 5));

    // Make it cold
    const metrics = manager.getAccessMetrics('cold-partition');
    if (metrics) {
      metrics.lastWriteTime = Date.now() - 300;
    }

    const plan = planVortexAwareCompaction(manager, ['cold-partition']);

    expect(plan.partitionsToConvert).toContain('cold-partition');
  });

  it('should plan compaction for already-converted cold partitions', () => {
    manager.addToHotTier('cold-converted', 'users', createTestEvents('users', 5));

    // Make it cold and already converted
    const metrics = manager.getAccessMetrics('cold-converted');
    if (metrics) {
      metrics.lastWriteTime = Date.now() - 300;
      metrics.convertedToParquet = true;
    }

    // Remove from hot tier (already converted)
    manager.removeFromHotTier('cold-converted');

    const plan = planVortexAwareCompaction(manager, ['cold-converted']);

    expect(plan.partitionsToCompact).toContain('cold-converted');
  });

  it('should optionally convert warm partitions', () => {
    manager.addToHotTier('warm-partition', 'users', createTestEvents('users', 5));

    // Make it warm
    const metrics = manager.getAccessMetrics('warm-partition');
    if (metrics) {
      metrics.lastWriteTime = Date.now() - 150;
    }

    const planWithWarm = planVortexAwareCompaction(manager, ['warm-partition'], {
      convertWarmPartitions: true,
    });

    expect(planWithWarm.partitionsToConvert).toContain('warm-partition');

    const planWithoutWarm = planVortexAwareCompaction(manager, ['warm-partition'], {
      convertWarmPartitions: false,
    });

    expect(planWithoutWarm.partitionsToConvert).not.toContain('warm-partition');
  });

  it('should respect maxPartitions limit', () => {
    // Add many cold partitions
    for (let i = 0; i < 20; i++) {
      manager.addToHotTier(`partition-${i}`, 'users', createTestEvents('users', 5));
      const metrics = manager.getAccessMetrics(`partition-${i}`);
      if (metrics) {
        metrics.lastWriteTime = Date.now() - 300; // Make cold
      }
    }

    const plan = planVortexAwareCompaction(
      manager,
      Array.from({ length: 20 }, (_, i) => `partition-${i}`),
      { maxPartitions: 5 }
    );

    expect(plan.partitionsToConvert.length).toBeLessThanOrEqual(5);
  });

  it('should skip warm partitions when coldOnly is true', () => {
    manager.addToHotTier('warm-partition', 'users', createTestEvents('users', 5));

    const metrics = manager.getAccessMetrics('warm-partition');
    if (metrics) {
      metrics.lastWriteTime = Date.now() - 150; // Warm
    }

    const plan = planVortexAwareCompaction(manager, ['warm-partition'], {
      coldOnly: true,
    });

    expect(plan.partitionsToSkip).toContain('warm-partition');
  });
});

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('Factory Functions', () => {
  it('should create default hot partition manager', () => {
    const manager = createHotPartitionManager();

    manager.addToHotTier('test', 'users', createTestEvents('users', 5));
    expect(manager.isInHotTier('test')).toBe(true);
  });

  it('should create high-throughput manager with custom config', () => {
    const manager = createHighThroughputHotPartitionManager();

    // Should have higher limits
    const stats = manager.getHotTierStats();
    expect(stats.partitionCount).toBe(0);

    // Add lots of events
    for (let i = 0; i < 100; i++) {
      manager.addToHotTier(`partition-${i}`, 'users', createTestEvents('users', 100));
    }

    // Should handle many events without flush
    expect(manager.shouldFlushHotTier()).toBe(false);
  });

  it('should create low-latency manager optimized for reads', () => {
    const manager = createLowLatencyHotPartitionManager();

    manager.addToHotTier('test', 'users', createTestEvents('users', 1000));

    // Entry should not be compressed (low latency = no compression overhead)
    const entry = manager.getHotEntry('test');
    expect(entry?.compressedData).toBeNull();
  });
});

// =============================================================================
// Configuration Tests
// =============================================================================

describe('Configuration', () => {
  it('should use default configuration', () => {
    const manager = new HotPartitionManager();

    // Verify defaults are applied by checking behavior
    expect(DEFAULT_HOT_PARTITION_CONFIG.hotThresholdMs).toBe(60 * 60 * 1000);
    expect(DEFAULT_HOT_PARTITION_CONFIG.warmThresholdMs).toBe(24 * 60 * 60 * 1000);
  });

  it('should allow partial configuration override', () => {
    const manager = new HotPartitionManager({
      hotThresholdMs: 1000,
      // Other values should be defaults
    });

    manager.addToHotTier('test', 'users', createTestEvents('users', 5));

    // With 1s threshold, partition should still be hot immediately after write
    expect(manager.getPartitionTemperature('test')).toBe('hot');
  });
});
