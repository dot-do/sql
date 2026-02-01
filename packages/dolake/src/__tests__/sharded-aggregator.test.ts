/**
 * Sharded Aggregator Tests
 *
 * Tests for the sharded aggregator that distributes CDC events
 * across multiple DoLake DO instances by table name.
 *
 * Issue: sql-f2in - DoLake single aggregator scalability bottleneck
 *
 * NO MOCKS - tests run against real Cloudflare Workers runtime.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import {
  ShardedAggregatorRouter,
  ShardedCDCCoordinator,
  fnv1aHash,
  tableToShardIndex,
  DEFAULT_SHARDED_AGGREGATOR_CONFIG,
  type CDCEvent,
  generateUUID,
} from '../index.js';

// =============================================================================
// Test Utilities
// =============================================================================

function createCDCEvent(table: string, overrides: Partial<CDCEvent> = {}): CDCEvent {
  return {
    sequence: Date.now(),
    timestamp: Date.now(),
    operation: 'INSERT',
    table,
    rowId: generateUUID(),
    after: { id: generateUUID(), value: Math.random() },
    ...overrides,
  };
}

function createEventsForTables(tables: string[], eventsPerTable: number): CDCEvent[] {
  const events: CDCEvent[] = [];
  for (const table of tables) {
    for (let i = 0; i < eventsPerTable; i++) {
      events.push(createCDCEvent(table));
    }
  }
  return events;
}

// =============================================================================
// Hash Function Tests
// =============================================================================

describe('fnv1aHash', () => {
  it('should produce deterministic results', () => {
    const hash1 = fnv1aHash('users');
    const hash2 = fnv1aHash('users');
    expect(hash1).toBe(hash2);
  });

  it('should produce different hashes for different strings', () => {
    const hash1 = fnv1aHash('users');
    const hash2 = fnv1aHash('orders');
    expect(hash1).not.toBe(hash2);
  });

  it('should produce a positive 32-bit integer', () => {
    const hash = fnv1aHash('test');
    expect(hash).toBeGreaterThanOrEqual(0);
    expect(hash).toBeLessThanOrEqual(0xFFFFFFFF);
  });

  it('should handle empty string', () => {
    const hash = fnv1aHash('');
    expect(typeof hash).toBe('number');
    expect(hash).toBeGreaterThanOrEqual(0);
  });
});

describe('tableToShardIndex', () => {
  it('should return 0 for single shard', () => {
    expect(tableToShardIndex('users', 1)).toBe(0);
    expect(tableToShardIndex('orders', 1)).toBe(0);
    expect(tableToShardIndex('anything', 1)).toBe(0);
  });

  it('should return valid index within shard range', () => {
    const shardCount = 4;
    for (const table of ['users', 'orders', 'events', 'logs', 'metrics', 'sessions']) {
      const index = tableToShardIndex(table, shardCount);
      expect(index).toBeGreaterThanOrEqual(0);
      expect(index).toBeLessThan(shardCount);
    }
  });

  it('should be deterministic', () => {
    const index1 = tableToShardIndex('users', 4);
    const index2 = tableToShardIndex('users', 4);
    expect(index1).toBe(index2);
  });

  it('should distribute tables across shards', () => {
    const shardCount = 4;
    const tables = Array.from({ length: 100 }, (_, i) => `table_${i}`);
    const shardCounts = new Map<number, number>();

    for (const table of tables) {
      const shard = tableToShardIndex(table, shardCount);
      shardCounts.set(shard, (shardCounts.get(shard) ?? 0) + 1);
    }

    // Each shard should get at least some tables (with 100 tables across 4 shards)
    for (let i = 0; i < shardCount; i++) {
      expect(shardCounts.get(i) ?? 0).toBeGreaterThan(0);
    }
  });
});

// =============================================================================
// ShardedAggregatorRouter Tests
// =============================================================================

describe('ShardedAggregatorRouter', () => {
  describe('single shard mode (backward compatibility)', () => {
    let router: ShardedAggregatorRouter;

    beforeEach(() => {
      router = new ShardedAggregatorRouter({ shardCount: 1 });
    });

    it('should default to single shard mode', () => {
      const defaultRouter = new ShardedAggregatorRouter();
      expect(defaultRouter.isSingleShardMode()).toBe(true);
      expect(defaultRouter.getShardCount()).toBe(1);
    });

    it('should route all events to shard 0', () => {
      const events = createEventsForTables(['users', 'orders', 'events'], 5);
      const result = router.routeEvents(events);

      expect(result.shardsUsed).toBe(1);
      expect(result.totalEventsRouted).toBe(15);
      expect(result.shardEvents.get(0)?.length).toBe(15);
    });

    it('should report single shard mode in status', () => {
      const status = router.getStatus();
      expect(status.isSingleShardMode).toBe(true);
      expect(status.shardCount).toBe(1);
    });

    it('should return shard name for index 0', () => {
      expect(router.getShardName(0)).toBe('dolake-shard-0');
    });
  });

  describe('multi-shard mode', () => {
    let router: ShardedAggregatorRouter;

    beforeEach(() => {
      router = new ShardedAggregatorRouter({ shardCount: 4 });
    });

    it('should not be in single shard mode', () => {
      expect(router.isSingleShardMode()).toBe(false);
      expect(router.getShardCount()).toBe(4);
    });

    it('should distribute events across multiple shards', () => {
      const tables = Array.from({ length: 20 }, (_, i) => `table_${i}`);
      const events = createEventsForTables(tables, 3);
      const result = router.routeEvents(events);

      // With 20 tables across 4 shards, we should use multiple shards
      expect(result.shardsUsed).toBeGreaterThan(1);
      expect(result.totalEventsRouted).toBe(60);

      // All events should be accounted for
      let total = 0;
      for (const shardEvents of result.shardEvents.values()) {
        total += shardEvents.length;
      }
      expect(total).toBe(60);
    });

    it('should route same table consistently to same shard', () => {
      const events1 = [createCDCEvent('users'), createCDCEvent('users')];
      const events2 = [createCDCEvent('users'), createCDCEvent('users')];

      const result1 = router.routeEvents(events1);
      const result2 = router.routeEvents(events2);

      const shard1 = result1.tableShardMap.get('users');
      const shard2 = result2.tableShardMap.get('users');
      expect(shard1).toBe(shard2);
    });

    it('should generate correct shard names', () => {
      for (let i = 0; i < 4; i++) {
        expect(router.getShardName(i)).toBe(`dolake-shard-${i}`);
      }
    });

    it('should throw for invalid shard index', () => {
      expect(() => router.getShardName(5)).toThrow('Invalid shard index');
    });

    it('should track per-shard statistics', () => {
      const events = createEventsForTables(['users', 'orders'], 10);
      router.routeEvents(events);

      const status = router.getStatus();
      expect(status.totalEventsRouted).toBe(20);
      expect(status.totalTables).toBe(2);
      expect(status.activeShardsCount).toBeGreaterThan(0);
    });

    it('should preserve event data when routing', () => {
      const originalEvent = createCDCEvent('users', {
        operation: 'UPDATE',
        before: { id: 1, name: 'old' },
        after: { id: 1, name: 'new' },
      });

      const result = router.routeEvents([originalEvent]);
      const shardIndex = result.tableShardMap.get('users')!;
      const routedEvents = result.shardEvents.get(shardIndex)!;

      expect(routedEvents).toHaveLength(1);
      expect(routedEvents[0].operation).toBe('UPDATE');
      expect(routedEvents[0].before).toEqual({ id: 1, name: 'old' });
      expect(routedEvents[0].after).toEqual({ id: 1, name: 'new' });
    });
  });

  describe('table pinning', () => {
    it('should pin tables to specified shards', () => {
      const router = new ShardedAggregatorRouter({
        shardCount: 4,
        tablePinning: { 'users': 0, 'profiles': 0 },
      });

      expect(router.getShardForTable('users')).toBe(0);
      expect(router.getShardForTable('profiles')).toBe(0);
    });

    it('should co-locate pinned tables on same shard', () => {
      const router = new ShardedAggregatorRouter({
        shardCount: 4,
        tablePinning: { 'users': 2, 'user_settings': 2 },
      });

      const events = [
        createCDCEvent('users'),
        createCDCEvent('user_settings'),
      ];

      const result = router.routeEvents(events);
      const usersShard = result.tableShardMap.get('users');
      const settingsShard = result.tableShardMap.get('user_settings');

      expect(usersShard).toBe(2);
      expect(settingsShard).toBe(2);
    });

    it('should allow dynamic pinning', () => {
      const router = new ShardedAggregatorRouter({ shardCount: 4 });

      // Initially hash-routed
      const initialShard = router.getShardForTable('orders');

      // Pin to specific shard
      router.pinTable('orders', 3);
      expect(router.getShardForTable('orders')).toBe(3);

      // Unpin reverts to hash-based
      router.unpinTable('orders');
      // After unpinning, the table is removed from cache, so re-routing uses hash
      const newShard = router.getShardForTable('orders');
      expect(typeof newShard).toBe('number');
    });

    it('should reject invalid shard index for pinning', () => {
      const router = new ShardedAggregatorRouter({ shardCount: 4 });
      expect(() => router.pinTable('users', 10)).toThrow('Invalid shard index');
    });

    it('should ignore invalid pinning in config', () => {
      const router = new ShardedAggregatorRouter({
        shardCount: 4,
        tablePinning: { 'users': 99 }, // Invalid, should be ignored
      });

      // Should fall back to hash-based routing
      const shard = router.getShardForTable('users');
      expect(shard).toBeGreaterThanOrEqual(0);
      expect(shard).toBeLessThan(4);
    });
  });

  describe('configuration edge cases', () => {
    it('should clamp shard count to minimum of 1', () => {
      const router = new ShardedAggregatorRouter({ shardCount: 0 });
      expect(router.getShardCount()).toBe(1);
    });

    it('should clamp shard count to maxShards', () => {
      const router = new ShardedAggregatorRouter({ shardCount: 100, maxShards: 16 });
      expect(router.getShardCount()).toBe(16);
    });

    it('should support custom shard name prefix', () => {
      const router = new ShardedAggregatorRouter({
        shardCount: 2,
        shardNamePrefix: 'my-lake',
      });
      expect(router.getShardName(0)).toBe('my-lake-0');
      expect(router.getShardName(1)).toBe('my-lake-1');
    });

    it('should handle empty event batch', () => {
      const router = new ShardedAggregatorRouter({ shardCount: 4 });
      const result = router.routeEvents([]);

      expect(result.shardsUsed).toBe(0);
      expect(result.totalEventsRouted).toBe(0);
      expect(result.shardEvents.size).toBe(0);
    });

    it('should return table assignments', () => {
      const router = new ShardedAggregatorRouter({ shardCount: 4 });
      router.routeEvents([createCDCEvent('users'), createCDCEvent('orders')]);

      const assignments = router.getTableAssignments();
      expect(assignments.has('users')).toBe(true);
      expect(assignments.has('orders')).toBe(true);
    });

    it('should reset stats', () => {
      const router = new ShardedAggregatorRouter({ shardCount: 2 });
      router.routeEvents(createEventsForTables(['users'], 10));

      const statusBefore = router.getStatus();
      expect(statusBefore.totalEventsRouted).toBe(10);

      router.resetStats();

      const statusAfter = router.getStatus();
      expect(statusAfter.totalEventsRouted).toBe(0);
    });
  });
});

// =============================================================================
// ShardedCDCCoordinator Tests
// =============================================================================

describe('ShardedCDCCoordinator', () => {
  describe('single shard mode', () => {
    it('should forward all events to single shard', async () => {
      const coordinator = new ShardedCDCCoordinator({ shardCount: 1 });
      const events = createEventsForTables(['users', 'orders'], 5);
      const forwardCalls: Array<{ shardIndex: number; shardName: string; eventCount: number }> = [];

      const result = await coordinator.ingestBatch(events, async (shardIndex, shardName, shardEvents) => {
        forwardCalls.push({ shardIndex, shardName, eventCount: shardEvents.length });
        return { success: true, eventsAccepted: shardEvents.length };
      });

      expect(result.success).toBe(true);
      expect(result.isSingleShardMode).toBe(true);
      expect(result.totalEventsAccepted).toBe(10);
      expect(result.shardsUsed).toBe(1);
      expect(forwardCalls).toHaveLength(1);
      expect(forwardCalls[0].eventCount).toBe(10);
    });
  });

  describe('multi-shard mode', () => {
    it('should distribute events across shards', async () => {
      const coordinator = new ShardedCDCCoordinator({ shardCount: 4 });
      const tables = Array.from({ length: 10 }, (_, i) => `table_${i}`);
      const events = createEventsForTables(tables, 5);
      const forwardCalls: Array<{ shardIndex: number; eventCount: number }> = [];

      const result = await coordinator.ingestBatch(events, async (shardIndex, shardName, shardEvents) => {
        forwardCalls.push({ shardIndex, eventCount: shardEvents.length });
        return { success: true, eventsAccepted: shardEvents.length };
      });

      expect(result.success).toBe(true);
      expect(result.isSingleShardMode).toBe(false);
      expect(result.totalEventsAccepted).toBe(50);
      expect(result.shardsUsed).toBeGreaterThan(1);

      // Total forwarded should equal input
      const totalForwarded = forwardCalls.reduce((sum, c) => sum + c.eventCount, 0);
      expect(totalForwarded).toBe(50);
    });

    it('should handle partial shard failures', async () => {
      const coordinator = new ShardedCDCCoordinator({ shardCount: 4 });
      const tables = Array.from({ length: 8 }, (_, i) => `table_${i}`);
      const events = createEventsForTables(tables, 3);
      let callCount = 0;

      const result = await coordinator.ingestBatch(events, async (shardIndex, shardName, shardEvents) => {
        callCount++;
        // Fail the second shard call
        if (callCount === 2) {
          return { success: false, eventsAccepted: 0, error: 'Shard unavailable' };
        }
        return { success: true, eventsAccepted: shardEvents.length };
      });

      expect(result.success).toBe(false); // Not all succeeded
      expect(result.totalEvents).toBe(24);

      // Check that we have both success and failure results
      const successResults = result.shardResults.filter(r => r.success);
      const failedResults = result.shardResults.filter(r => !r.success);
      expect(successResults.length).toBeGreaterThan(0);
      expect(failedResults.length).toBeGreaterThan(0);
    });

    it('should handle shard forward exceptions', async () => {
      const coordinator = new ShardedCDCCoordinator({ shardCount: 2 });
      const events = createEventsForTables(['users', 'orders'], 5);
      let callCount = 0;

      const result = await coordinator.ingestBatch(events, async (shardIndex, shardName, shardEvents) => {
        callCount++;
        if (callCount === 1) {
          throw new Error('Network error');
        }
        return { success: true, eventsAccepted: shardEvents.length };
      });

      // Should handle exception gracefully
      const failedShards = result.shardResults.filter(r => !r.success);
      expect(failedShards.length).toBeGreaterThan(0);
      expect(failedShards[0].error).toContain('Network error');
    });

    it('should handle empty batch', async () => {
      const coordinator = new ShardedCDCCoordinator({ shardCount: 4 });

      const result = await coordinator.ingestBatch([], async () => {
        return { success: true, eventsAccepted: 0 };
      });

      expect(result.success).toBe(true);
      expect(result.totalEvents).toBe(0);
      expect(result.shardsUsed).toBe(0);
      expect(result.shardResults).toHaveLength(0);
    });
  });

  describe('coordinator status', () => {
    it('should expose router status', async () => {
      const coordinator = new ShardedCDCCoordinator({ shardCount: 4 });
      const events = createEventsForTables(['users', 'orders', 'events'], 5);

      await coordinator.ingestBatch(events, async (_, __, shardEvents) => {
        return { success: true, eventsAccepted: shardEvents.length };
      });

      const status = coordinator.getStatus();
      expect(status.shardCount).toBe(4);
      expect(status.totalEventsRouted).toBe(15);
      expect(status.totalTables).toBe(3);
    });

    it('should expose underlying router', () => {
      const coordinator = new ShardedCDCCoordinator({ shardCount: 4 });
      const router = coordinator.getRouter();

      expect(router).toBeInstanceOf(ShardedAggregatorRouter);
      expect(router.getShardCount()).toBe(4);
    });
  });

  describe('table co-location via pinning', () => {
    it('should route co-located tables to same shard', async () => {
      const coordinator = new ShardedCDCCoordinator({
        shardCount: 4,
        tablePinning: { 'users': 1, 'profiles': 1, 'user_settings': 1 },
      });

      const events = [
        createCDCEvent('users'),
        createCDCEvent('profiles'),
        createCDCEvent('user_settings'),
      ];

      const shardsSeen = new Set<number>();

      await coordinator.ingestBatch(events, async (shardIndex, shardName, shardEvents) => {
        shardsSeen.add(shardIndex);
        return { success: true, eventsAccepted: shardEvents.length };
      });

      // All three tables should go to the same shard (1)
      expect(shardsSeen.size).toBe(1);
      expect(shardsSeen.has(1)).toBe(true);
    });
  });
});

// =============================================================================
// Integration: Sharded Aggregator with Buffer
// =============================================================================

describe('Sharded Aggregator Integration', () => {
  it('should maintain event integrity through routing', () => {
    const router = new ShardedAggregatorRouter({ shardCount: 4 });

    // Create events with specific data
    const events: CDCEvent[] = [];
    for (let i = 0; i < 100; i++) {
      events.push(createCDCEvent(`table_${i % 10}`, {
        sequence: i,
        after: { id: i, data: `record_${i}` },
      }));
    }

    const result = router.routeEvents(events);

    // Verify all events are accounted for
    let totalRouted = 0;
    const seenSequences = new Set<number>();
    for (const shardEvents of result.shardEvents.values()) {
      for (const event of shardEvents) {
        seenSequences.add(event.sequence);
        totalRouted++;
      }
    }

    expect(totalRouted).toBe(100);
    expect(seenSequences.size).toBe(100);
  });

  it('should group events by table within each shard', () => {
    const router = new ShardedAggregatorRouter({ shardCount: 4 });
    const events = createEventsForTables(['users', 'orders', 'logs'], 10);

    const result = router.routeEvents(events);

    // Within each shard, events should belong to tables assigned to that shard
    for (const [shardIndex, shardEvents] of result.shardEvents) {
      const tablesInShard = new Set(shardEvents.map(e => e.table));
      for (const table of tablesInShard) {
        expect(result.tableShardMap.get(table)).toBe(shardIndex);
      }
    }
  });

  it('should handle high-cardinality table names', () => {
    const router = new ShardedAggregatorRouter({ shardCount: 8 });
    const tables = Array.from({ length: 1000 }, (_, i) => `dynamic_table_${i}`);
    const events = createEventsForTables(tables, 1);

    const result = router.routeEvents(events);

    // Should use most/all shards with 1000 tables
    expect(result.shardsUsed).toBeGreaterThanOrEqual(4);
    expect(result.totalEventsRouted).toBe(1000);
  });

  it('should handle concurrent coordinator calls', async () => {
    const coordinator = new ShardedCDCCoordinator({ shardCount: 4 });
    const batches = Array.from({ length: 5 }, (_, i) =>
      createEventsForTables([`batch_${i}_table_a`, `batch_${i}_table_b`], 10)
    );

    const results = await Promise.all(
      batches.map(batch =>
        coordinator.ingestBatch(batch, async (_, __, shardEvents) => {
          return { success: true, eventsAccepted: shardEvents.length };
        })
      )
    );

    for (const result of results) {
      expect(result.success).toBe(true);
      expect(result.totalEventsAccepted).toBe(20);
    }
  });
});
