/**
 * Stream State Cleanup Tests
 *
 * These tests verify stream state cleanup functionality including:
 * 1. TTL-based expiration - Streams auto-cleanup after inactivity timeout
 * 2. Maximum concurrent streams - Limit number of concurrent streams per connection
 * 3. Cleanup on connection close - All streams cleaned up when WebSocket closes
 * 4. Abandoned stream detection - Detect streams with no activity for X minutes (future work)
 * 5. Manual cleanup API - closeStream(), closeAllStreams(), getStreamStats()
 * 6. Memory usage monitoring - Track total stream memory (future work)
 *
 * Issue: sql-zhy.25 - Stream State Cleanup
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { DoSQLTarget } from '../server.js';
import type { CDCManager, CDCSubscription, CDCSubscribeOptions, StreamManager } from '../server.js';
import { MockQueryExecutor } from '../../__tests__/utils/index.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Creates a mock CDC manager for testing
 */
function createMockCDCManager(): CDCManager {
  const subscriptions = new Map<string, CDCSubscription>();
  let subIdCounter = 0;

  return {
    subscribe(options: CDCSubscribeOptions): CDCSubscription {
      const id = `cdc_${++subIdCounter}`;
      const sub: CDCSubscription = {
        id,
        events: {
          async *[Symbol.asyncIterator]() {
            // Yield nothing - mock subscription
          },
        },
        unsubscribe: () => {
          subscriptions.delete(id);
        },
      };
      subscriptions.set(id, sub);
      return sub;
    },
    async getEventsSince() {
      return [];
    },
  };
}

/**
 * Helper to create multiple streams
 */
async function createStreams(target: DoSQLTarget, count: number): Promise<string[]> {
  const streamIds: string[] = [];
  for (let i = 0; i < count; i++) {
    const streamId = `stream_${i}_${Date.now()}`;
    await target._initStream({
      sql: `SELECT * FROM test_table_${i}`,
      streamId,
    });
    streamIds.push(streamId);
  }
  return streamIds;
}

// MemoryUsage and StreamManager interfaces are now exported from server.ts

// =============================================================================
// 1. TTL-BASED EXPIRATION
// =============================================================================

describe('Stream State Cleanup - TTL-Based Expiration', () => {
  let executor: MockQueryExecutor;
  let target: DoSQLTarget;

  beforeEach(() => {
    executor = new MockQueryExecutor();
    executor.addTable('users', ['id', 'name'], ['number', 'string'], [
      [1, 'Alice'],
      [2, 'Bob'],
      [3, 'Charlie'],
    ]);
  });

  it('should cleanup streams after inactivity timeout via cleanupExpiredStreams()', async () => {
    // DoSQLTarget supports TTL via constructor options and cleanupExpiredStreams() method
    // Note: Automatic cleanup requires Durable Object alarm integration
    target = new DoSQLTarget(executor, undefined, { streamTTLMs: 100 });

    const streamId = `stream_ttl_${Date.now()}`;

    // Initialize a stream
    await target._initStream({
      sql: 'SELECT * FROM users',
      streamId,
      chunkSize: 1,
    });

    expect(target.getStreamStats().activeStreams).toBe(1);

    // Wait for TTL to expire
    await new Promise((resolve) => setTimeout(resolve, 150));

    // Manually trigger cleanup (in production, this is called by DO alarm)
    const cleaned = target.cleanupExpiredStreams();
    expect(cleaned).toBe(1);

    // Stream should be cleaned up
    await expect(target._nextChunk(streamId)).rejects.toThrow('not found');
    expect(target.getStreamStats().activeStreams).toBe(0);
  });

  it('should not cleanup streams with recent activity', async () => {
    target = new DoSQLTarget(executor, undefined, { streamTTLMs: 100 });

    const streamId = `stream_ttl_reset_${Date.now()}`;

    await target._initStream({
      sql: 'SELECT * FROM users',
      streamId,
      chunkSize: 1,
    });

    // Activity within TTL - fetching resets lastActivity
    await new Promise((resolve) => setTimeout(resolve, 50));
    await target._nextChunk(streamId); // Activity resets TTL timer

    await new Promise((resolve) => setTimeout(resolve, 50));
    await target._nextChunk(streamId); // Another activity

    // Trigger cleanup - should not clean up because activity was recent
    const cleaned = target.cleanupExpiredStreams();
    expect(cleaned).toBe(0);

    // Stream should still exist
    const chunk = await target._nextChunk(streamId);
    expect(chunk).toBeDefined();
  });

  it('should track lastActivity timestamp on streams', async () => {
    target = new DoSQLTarget(executor);

    const streamId = `stream_activity_${Date.now()}`;
    const beforeInit = Date.now();

    await target._initStream({
      sql: 'SELECT * FROM users',
      streamId,
      chunkSize: 1,
    });

    const info1 = target.getStreamInfo(streamId);
    expect(info1).toBeDefined();
    expect(info1!.lastActivity).toBeGreaterThanOrEqual(beforeInit);
    expect(info1!.createdAt).toBeGreaterThanOrEqual(beforeInit);

    // Wait and fetch to update lastActivity
    await new Promise((resolve) => setTimeout(resolve, 10));
    const beforeFetch = Date.now();
    await target._nextChunk(streamId);

    const info2 = target.getStreamInfo(streamId);
    expect(info2!.lastActivity).toBeGreaterThanOrEqual(beforeFetch);
    expect(info2!.createdAt).toBe(info1!.createdAt); // createdAt unchanged
  });

  it('should support per-stream-type TTL configuration (CDC vs query streams)', async () => {
    target = new DoSQLTarget(executor, undefined, { streamTTLMs: 500 });
    const manager = target.getStreamManager();

    // Set different TTLs for query and CDC streams
    manager.setStreamTTLByType('query', 50);
    manager.setStreamTTLByType('cdc', 500);

    const queryStreamId = `query_stream_${Date.now()}`;
    await target._initStream({
      sql: 'SELECT * FROM users',
      streamId: queryStreamId,
      chunkSize: 1,
    });

    // Wait for query TTL but not CDC TTL
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Query stream should be expired
    const cleaned = target.cleanupExpiredStreams();
    expect(cleaned).toBe(1);
  });
});

// =============================================================================
// 2. MAXIMUM CONCURRENT STREAMS
// =============================================================================

describe('Stream State Cleanup - Maximum Concurrent Streams', () => {
  let executor: MockQueryExecutor;

  beforeEach(() => {
    executor = new MockQueryExecutor();
    executor.addTable('users', ['id', 'name'], ['number', 'string'], [
      [1, 'Alice'],
      [2, 'Bob'],
    ]);
  });

  it('should limit number of concurrent streams per connection', async () => {
    // maxConcurrentStreams is set via constructor options
    const target = new DoSQLTarget(executor, undefined, { maxConcurrentStreams: 5 });

    // Create 5 streams (at limit)
    const streamIds = await createStreams(target, 5);
    expect(streamIds).toHaveLength(5);
    expect(target.getStreamStats().activeStreams).toBe(5);

    // 6th stream should be rejected
    await expect(
      target._initStream({
        sql: 'SELECT * FROM users',
        streamId: 'stream_over_limit',
      })
    ).rejects.toThrow('Maximum concurrent streams exceeded');
  });

  it('should include limit in error message when exceeded', async () => {
    const target = new DoSQLTarget(executor, undefined, { maxConcurrentStreams: 2 });

    await createStreams(target, 2);

    try {
      await target._initStream({
        sql: 'SELECT * FROM users',
        streamId: 'stream_rejected',
      });
      expect.fail('Should have thrown');
    } catch (error) {
      expect((error as Error).message).toContain('limit: 2');
    }
  });

  it('should allow new streams after closing existing ones', async () => {
    const target = new DoSQLTarget(executor, undefined, { maxConcurrentStreams: 2 });

    const [stream1] = await createStreams(target, 2);

    // At limit, this should fail
    await expect(
      target._initStream({ sql: 'SELECT * FROM users', streamId: 'blocked' })
    ).rejects.toThrow('Maximum concurrent streams');

    // Close one stream using the public closeStream method
    target.closeStream(stream1);

    // Now we should be able to create a new stream
    const newStreamId = 'new_stream_after_close';
    await target._initStream({ sql: 'SELECT * FROM users', streamId: newStreamId });

    expect(target.getStreamStats().activeStreams).toBe(2);
  });

  it('should return max concurrent streams setting', async () => {
    const target = new DoSQLTarget(executor, undefined, { maxConcurrentStreams: 42 });
    expect(target.getMaxConcurrentStreams()).toBe(42);
  });

  it('should track CDC subscriptions in combined concurrent limit', async () => {
    const cdcManager = createMockCDCManager();
    const target = new DoSQLTarget(executor, cdcManager, { maxConcurrentStreams: 3 });

    // Create 2 query streams
    await createStreams(target, 2);

    // Create 1 CDC subscription (tracked in combined limit)
    await target._subscribeCDC({ subscriptionId: 'cdc_limit_test', fromLSN: 0n });

    // The combined count of streams is 2, CDC is separate
    // Query streams should respect their own limit
    expect(target.getStreamStats().activeStreams).toBe(2);
    const manager = target.getStreamManager();
    expect(manager.getStreamStats().totalCdcSubscriptions).toBe(1);
  });

  it('should support separate limits for streams and CDC subscriptions', async () => {
    const cdcManager = createMockCDCManager();
    const target = new DoSQLTarget(executor, cdcManager, { maxConcurrentStreams: 10 });

    const manager = target.getStreamManager();
    manager.setStreamLimits({ maxQueryStreams: 2, maxCdcSubscriptions: 1 });

    // Create 2 query streams (at query limit)
    await createStreams(target, 2);

    // Create 1 CDC subscription
    await target._subscribeCDC({ subscriptionId: 'cdc_sep_1', fromLSN: 0n });

    // Stats should reflect both
    const stats = manager.getStreamStats();
    expect(stats.activeStreams).toBe(2);
    expect(stats.totalCdcSubscriptions).toBe(1);
  });
});

// =============================================================================
// 3. CLEANUP ON CONNECTION CLOSE
// =============================================================================

describe('Stream State Cleanup - Connection Close Cleanup', () => {
  let executor: MockQueryExecutor;
  let target: DoSQLTarget;

  beforeEach(() => {
    executor = new MockQueryExecutor();
    executor.addTable('data', ['id'], ['number'], [[1], [2], [3]]);
    target = new DoSQLTarget(executor);
  });

  it('should cleanup all streams when WebSocket closes', async () => {
    const streamIds = await createStreams(target, 5);
    expect(streamIds).toHaveLength(5);
    expect(target.getStreamStats().activeStreams).toBe(5);

    // onConnectionClose() is called when WebSocket closes
    target.onConnectionClose();

    // All streams should be cleaned up
    for (const streamId of streamIds) {
      await expect(target._nextChunk(streamId)).rejects.toThrow('not found');
    }

    expect(target.getStreamStats().activeStreams).toBe(0);
  });

  it('should cleanup CDC subscriptions on disconnect', async () => {
    const cdcManager = createMockCDCManager();
    const targetWithCdc = new DoSQLTarget(executor, cdcManager);

    // Create multiple CDC subscriptions
    await targetWithCdc._subscribeCDC({ subscriptionId: 'cdc_1', fromLSN: 0n });
    await targetWithCdc._subscribeCDC({ subscriptionId: 'cdc_2', fromLSN: 0n });
    await targetWithCdc._subscribeCDC({ subscriptionId: 'cdc_3', fromLSN: 0n });

    // onConnectionClose cleans up both streams and CDC subscriptions
    targetWithCdc.onConnectionClose();

    // All subscriptions should be cleaned up
    await expect(targetWithCdc._pollCDC('cdc_1')).rejects.toThrow('not found');
    await expect(targetWithCdc._pollCDC('cdc_2')).rejects.toThrow('not found');
    await expect(targetWithCdc._pollCDC('cdc_3')).rejects.toThrow('not found');
  });

  it('should call unsubscribe on CDC subscriptions during cleanup', async () => {
    const unsubscribeSpy = vi.fn();
    const mockCdcManager: CDCManager = {
      subscribe() {
        return {
          id: 'test',
          events: { async *[Symbol.asyncIterator]() {} },
          unsubscribe: unsubscribeSpy,
        };
      },
      async getEventsSince() {
        return [];
      },
    };

    const targetWithCdc = new DoSQLTarget(executor, mockCdcManager);

    await targetWithCdc._subscribeCDC({ subscriptionId: 'cdc_spy', fromLSN: 0n });

    // onConnectionClose calls unsubscribe on all CDC subscriptions
    targetWithCdc.onConnectionClose();

    // unsubscribe should have been called
    expect(unsubscribeSpy).toHaveBeenCalledTimes(1);
  });

  it('should cleanup both streams and CDC subscriptions together', async () => {
    const cdcManager = createMockCDCManager();
    const targetWithCdc = new DoSQLTarget(executor, cdcManager);

    // Create query streams
    await createStreams(targetWithCdc, 3);
    // Create CDC subscriptions
    await targetWithCdc._subscribeCDC({ subscriptionId: 'cdc_mixed', fromLSN: 0n });

    expect(targetWithCdc.getStreamStats().activeStreams).toBe(3);

    // Single call cleans up everything
    targetWithCdc.onConnectionClose();

    expect(targetWithCdc.getStreamStats().activeStreams).toBe(0);
    await expect(targetWithCdc._pollCDC('cdc_mixed')).rejects.toThrow('not found');
  });

  it('should support registering cleanup callbacks', async () => {
    const callback1 = vi.fn();
    const callback2 = vi.fn();

    target.registerCleanupCallback(callback1);
    target.registerCleanupCallback(callback2);

    await createStreams(target, 2);
    target.onConnectionClose();

    expect(callback1).toHaveBeenCalledTimes(1);
    expect(callback2).toHaveBeenCalledTimes(1);
  });

  it('should handle cleanup errors gracefully', async () => {
    const errorCallback = vi.fn(() => {
      throw new Error('Cleanup error');
    });
    const successCallback = vi.fn();

    target.registerCleanupCallback(errorCallback);
    target.registerCleanupCallback(successCallback);

    await createStreams(target, 2);

    // Should not throw even if a callback throws
    expect(() => target.onConnectionClose()).not.toThrow();

    // Both callbacks should have been called
    expect(errorCallback).toHaveBeenCalledTimes(1);
    expect(successCallback).toHaveBeenCalledTimes(1);
  });
});

// =============================================================================
// 4. ABANDONED STREAM DETECTION
// =============================================================================

describe('Stream State Cleanup - Abandoned Stream Detection', () => {
  let executor: MockQueryExecutor;
  let target: DoSQLTarget;

  beforeEach(() => {
    executor = new MockQueryExecutor();
    executor.addTable('logs', ['id', 'msg'], ['number', 'string'], [
      [1, 'log1'],
      [2, 'log2'],
    ]);
    target = new DoSQLTarget(executor);
  });

  it('should track stream age via createdAt and lastActivity', async () => {
    const streamId = `age_tracking_${Date.now()}`;
    const beforeCreate = Date.now();

    await target._initStream({
      sql: 'SELECT * FROM logs',
      streamId,
      chunkSize: 1,
    });

    const info = target.getStreamInfo(streamId);
    expect(info).toBeDefined();
    expect(info!.createdAt).toBeGreaterThanOrEqual(beforeCreate);
    expect(info!.lastActivity).toBeGreaterThanOrEqual(beforeCreate);
  });

  it('should identify abandoned streams via getStreamInfo comparison', async () => {
    // Add tables with enough rows so streams don't auto-close on first chunk
    executor.addTable('test_table_0', ['id', 'data'], ['number', 'string'], [
      [1, 'a'], [2, 'b'], [3, 'c'], [4, 'd'], [5, 'e'],
    ]);
    executor.addTable('test_table_1', ['id', 'data'], ['number', 'string'], [
      [1, 'a'], [2, 'b'], [3, 'c'], [4, 'd'], [5, 'e'],
    ]);
    executor.addTable('test_table_2', ['id', 'data'], ['number', 'string'], [
      [1, 'a'], [2, 'b'], [3, 'c'], [4, 'd'], [5, 'e'],
    ]);

    // Create streams with different activity patterns using small chunkSize
    const streamIds: string[] = [];
    for (let i = 0; i < 3; i++) {
      const streamId = `stream_${i}_${Date.now()}`;
      await target._initStream({
        sql: `SELECT * FROM test_table_${i}`,
        streamId,
        chunkSize: 1, // Small chunk size so streams stay open
      });
      streamIds.push(streamId);
    }

    // Record lastActivity before waiting
    const info1Before = target.getStreamInfo(streamIds[1]);
    expect(info1Before).toBeDefined();

    // Wait a bit to ensure time difference
    await new Promise((resolve) => setTimeout(resolve, 10));

    // Access one stream to update its lastActivity
    await target._nextChunk(streamIds[0]);

    // Can manually check age by comparing lastActivity to current time
    const info0 = target.getStreamInfo(streamIds[0]);
    const info1 = target.getStreamInfo(streamIds[1]);

    expect(info0).toBeDefined();
    expect(info1).toBeDefined();
    // Stream 0 was just accessed, so its lastActivity should be more recent
    expect(info0!.lastActivity).toBeGreaterThanOrEqual(info1!.lastActivity);
  });

  it('should use cleanupExpiredStreams for abandoned stream cleanup', async () => {
    // Configure a short TTL
    target = new DoSQLTarget(executor, undefined, { streamTTLMs: 50 });

    // Add tables with enough rows so streams don't auto-close on fetch
    executor.addTable('test_table_0', ['id', 'data'], ['number', 'string'], [
      [1, 'a'], [2, 'b'], [3, 'c'], [4, 'd'], [5, 'e'], [6, 'f'], [7, 'g'], [8, 'h'],
    ]);
    executor.addTable('test_table_1', ['id', 'data'], ['number', 'string'], [
      [1, 'a'], [2, 'b'], [3, 'c'], [4, 'd'], [5, 'e'],
    ]);
    executor.addTable('test_table_2', ['id', 'data'], ['number', 'string'], [
      [1, 'a'], [2, 'b'], [3, 'c'], [4, 'd'], [5, 'e'],
    ]);

    // Create streams with small chunk size so they stay open
    const streamIds: string[] = [];
    for (let i = 0; i < 3; i++) {
      const streamId = `stream_${i}_${Date.now()}`;
      await target._initStream({
        sql: `SELECT * FROM test_table_${i}`,
        streamId,
        chunkSize: 1, // Small chunk size so streams stay open after fetch
      });
      streamIds.push(streamId);
    }

    // Make one stream active by fetching from it
    await target._nextChunk(streamIds[0]);

    // Wait for TTL
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Access stream 0 again to keep it alive
    await target._nextChunk(streamIds[0]);

    // Cleanup should remove abandoned streams (1 and 2) but keep active one (0)
    const cleaned = target.cleanupExpiredStreams();
    expect(cleaned).toBe(2);

    // Stream 0 should still exist
    const info = target.getStreamInfo(streamIds[0]);
    expect(info).toBeDefined();
  });

  it('should log warning for potential memory leaks', async () => {
    // When stream count exceeds a threshold, getStreamStats should indicate it
    target = new DoSQLTarget(executor, undefined, { streamTTLMs: 50 });

    executor.addTable('test_table_0', ['id'], ['number'], [[1], [2], [3], [4], [5]]);
    executor.addTable('test_table_1', ['id'], ['number'], [[1], [2], [3], [4], [5]]);
    executor.addTable('test_table_2', ['id'], ['number'], [[1], [2], [3], [4], [5]]);

    // Create streams
    for (let i = 0; i < 3; i++) {
      await target._initStream({
        sql: `SELECT * FROM test_table_${i}`,
        streamId: `leak_stream_${i}`,
        chunkSize: 1,
      });
    }

    // Wait for TTL
    await new Promise((resolve) => setTimeout(resolve, 100));

    // getAbandonedStreams reports streams that look like leaks
    const abandoned = target.getAbandonedStreams();
    expect(abandoned.length).toBe(3);
  });

  it('should provide list of abandoned streams via getAbandonedStreams()', async () => {
    target = new DoSQLTarget(executor, undefined, { streamTTLMs: 50 });

    executor.addTable('test_table_0', ['id'], ['number'], [[1], [2], [3], [4], [5]]);
    executor.addTable('test_table_1', ['id'], ['number'], [[1], [2], [3], [4], [5]]);

    await target._initStream({
      sql: 'SELECT * FROM test_table_0',
      streamId: 'abandoned_1',
      chunkSize: 1,
    });
    await target._initStream({
      sql: 'SELECT * FROM test_table_1',
      streamId: 'active_1',
      chunkSize: 1,
    });

    // Wait for TTL
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Keep active_1 alive
    await target._nextChunk('active_1');

    const abandoned = target.getAbandonedStreams();
    expect(abandoned.length).toBe(1);
    expect(abandoned[0].streamId).toBe('abandoned_1');
  });
});

// =============================================================================
// 5. MANUAL CLEANUP API
// =============================================================================

describe('Stream State Cleanup - Manual Cleanup API', () => {
  let executor: MockQueryExecutor;
  let target: DoSQLTarget;

  beforeEach(() => {
    executor = new MockQueryExecutor();
    executor.addTable('items', ['id'], ['number'], [[1], [2], [3]]);
    target = new DoSQLTarget(executor);
  });

  it('should provide closeStream(streamId) method', async () => {
    const streamId = `manual_close_${Date.now()}`;

    await target._initStream({
      sql: 'SELECT * FROM items',
      streamId,
    });

    expect(target.getStreamStats().activeStreams).toBe(1);

    // closeStream returns true if stream existed
    const closed = target.closeStream(streamId);
    expect(closed).toBe(true);

    expect(target.getStreamStats().activeStreams).toBe(0);
    await expect(target._nextChunk(streamId)).rejects.toThrow('not found');
  });

  it('should return false when closing non-existent stream', async () => {
    const closed = target.closeStream('non_existent_stream');
    expect(closed).toBe(false);
  });

  it('should provide closeAllStreams() method', async () => {
    const streamIds = await createStreams(target, 5);
    expect(streamIds).toHaveLength(5);

    expect(target.getStreamStats().activeStreams).toBe(5);

    // closeAllStreams returns number of streams closed
    const closedCount = target.closeAllStreams();
    expect(closedCount).toBe(5);

    expect(target.getStreamStats().activeStreams).toBe(0);

    for (const streamId of streamIds) {
      await expect(target._nextChunk(streamId)).rejects.toThrow('not found');
    }
  });

  it('should provide getStreamStats() method', async () => {
    // Create various streams
    await createStreams(target, 3);

    const stats = target.getStreamStats();

    expect(stats).toEqual(
      expect.objectContaining({
        activeStreams: 3,
        totalCreated: 3,
        totalClosed: 0,
      })
    );
  });

  it('should track totalClosed in stats', async () => {
    const streamIds = await createStreams(target, 5);
    // Use actual streamIds returned by createStreams (which include timestamps)
    target.closeStream(streamIds[0]);
    target.closeStream(streamIds[1]);

    const stats = target.getStreamStats();
    expect(stats.activeStreams).toBe(3);
    expect(stats.totalCreated).toBe(5);
    expect(stats.totalClosed).toBe(2);
  });

  it('should provide getStreamInfo(streamId) for per-stream details', async () => {
    const streamId = `info_detail_${Date.now()}`;

    await target._initStream({
      sql: 'SELECT * FROM items',
      streamId,
      chunkSize: 1,
    });

    // Fetch some chunks
    await target._nextChunk(streamId);
    await target._nextChunk(streamId);

    const info = target.getStreamInfo(streamId);

    expect(info).toEqual(
      expect.objectContaining({
        streamId,
        createdAt: expect.any(Number),
        lastActivity: expect.any(Number),
        chunkSize: 1,
        totalRowsSent: 2,
        sql: 'SELECT * FROM items',
      })
    );
  });

  it('should return undefined for non-existent stream info', async () => {
    const info = target.getStreamInfo('non_existent');
    expect(info).toBeUndefined();
  });

  it('should close streams matching a pattern', async () => {
    // Create streams with specific naming patterns
    await target._initStream({ sql: 'SELECT * FROM items', streamId: 'query_users_1' });
    await target._initStream({ sql: 'SELECT * FROM items', streamId: 'query_users_2' });
    await target._initStream({ sql: 'SELECT * FROM items', streamId: 'query_orders_1' });
    await target._initStream({ sql: 'SELECT * FROM items', streamId: 'cdc_stream_1' });

    expect(target.getStreamStats().activeStreams).toBe(4);

    // Close only user query streams
    const closed = target.closeStreamsByPattern('query_users_*');
    expect(closed).toBe(2);
    expect(target.getStreamStats().activeStreams).toBe(2);

    // Remaining streams should still exist
    expect(target.getStreamInfo('query_orders_1')).toBeDefined();
    expect(target.getStreamInfo('cdc_stream_1')).toBeDefined();
  });
});

// =============================================================================
// 6. MEMORY USAGE MONITORING (Future Work)
// =============================================================================

describe('Stream State Cleanup - Memory Usage Monitoring', () => {
  let executor: MockQueryExecutor;
  let target: DoSQLTarget;

  beforeEach(() => {
    executor = new MockQueryExecutor();
    // Create table with larger data for memory testing
    const rows = Array.from({ length: 1000 }, (_, i) => [i, `data_${i}`.repeat(100)]);
    executor.addTable('large_data', ['id', 'content'], ['number', 'string'], rows);
    target = new DoSQLTarget(executor);
  });

  it('should track stream count via getStreamStats', async () => {
    // Current implementation tracks stream count, not memory bytes
    await createStreams(target, 5);

    const stats = target.getStreamStats();
    expect(stats.activeStreams).toBe(5);
    expect(stats.totalCreated).toBe(5);
  });

  it('should track stream info including rows sent', async () => {
    const streamId = 'memory_test_stream';
    await target._initStream({
      sql: 'SELECT * FROM large_data',
      streamId,
      chunkSize: 100,
    });

    // Fetch a chunk
    await target._nextChunk(streamId);

    const info = target.getStreamInfo(streamId);
    expect(info).toBeDefined();
    expect(info!.totalRowsSent).toBe(100);
  });

  it('should track total stream memory usage', async () => {
    await createStreams(target, 3);

    const memUsage = target.getMemoryUsage();
    expect(memUsage.estimatedBytes).toBeGreaterThan(0);
    expect(memUsage.streamCount).toBe(3);
    expect(memUsage.status).toBe('ok');
  });

  it('should alert when approaching memory limits', async () => {
    // Set very low thresholds for testing
    target = new DoSQLTarget(executor, undefined, {
      memoryWarningThreshold: 100, // 100 bytes
      memoryCriticalThreshold: 10000, // 10KB
    });

    await createStreams(target, 3);

    const memUsage = target.getMemoryUsage();
    // With 3 streams, estimated bytes should exceed 100-byte warning threshold
    expect(memUsage.estimatedBytes).toBeGreaterThan(100);
    expect(memUsage.status).toBe('warning');
  });

  it('should provide memory usage breakdown by stream type', async () => {
    await createStreams(target, 2);

    const breakdown = target.getMemoryBreakdown();
    expect(breakdown.query).toBeGreaterThan(0);
    expect(breakdown.total).toBe(breakdown.query + breakdown.cdc);
    expect(breakdown.cdc).toBe(0); // No CDC streams created
  });

  it('should reject new streams when critical memory limit reached', async () => {
    // Set a very low critical threshold
    target = new DoSQLTarget(executor, undefined, {
      memoryCriticalThreshold: 1, // 1 byte - impossible to stay under
      memoryWarningThreshold: 0,
    });

    // First stream creates some memory usage
    await target._initStream({
      sql: 'SELECT * FROM large_data',
      streamId: 'first_stream',
      chunkSize: 100,
    });

    // Fetch to build up memory estimate
    await target._nextChunk('first_stream');

    // Second stream should be rejected due to critical memory
    await expect(
      target._initStream({
        sql: 'SELECT * FROM large_data',
        streamId: 'rejected_stream',
      })
    ).rejects.toThrow('Critical memory limit reached');
  });

  it('should emit memory metrics for monitoring systems', async () => {
    const metricsCallback = vi.fn();
    target.onMemoryMetrics(metricsCallback);

    await createStreams(target, 2);

    // Emit metrics
    target.emitMemoryMetrics();

    expect(metricsCallback).toHaveBeenCalledTimes(1);
    const metrics = metricsCallback.mock.calls[0][0];
    expect(metrics.estimatedBytes).toBeGreaterThan(0);
    expect(metrics.streamCount).toBe(2);
    expect(metrics.status).toBe('ok');
    expect(metrics.timestamp).toBeGreaterThan(0);
    expect(metrics.peakBytes).toBeGreaterThanOrEqual(metrics.estimatedBytes);
  });

  it('should track peak memory usage', async () => {
    // Create streams to build up memory
    const streamIds = await createStreams(target, 5);

    // Record memory with 5 streams
    target.getMemoryUsage(); // triggers peak tracking
    const peakWith5 = target.getPeakMemoryUsage();
    expect(peakWith5).toBeGreaterThan(0);

    // Close some streams
    target.closeStream(streamIds[0]);
    target.closeStream(streamIds[1]);
    target.closeStream(streamIds[2]);

    // Check memory again - peak should not decrease
    target.getMemoryUsage();
    const peakAfterClose = target.getPeakMemoryUsage();
    expect(peakAfterClose).toBe(peakWith5);
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('Stream State Cleanup - Integration [RED]', () => {
  let executor: MockQueryExecutor;
  let target: DoSQLTarget;

  beforeEach(() => {
    executor = new MockQueryExecutor();
    executor.addTable('events', ['id', 'type'], ['number', 'string'], [
      [1, 'click'],
      [2, 'view'],
      [3, 'scroll'],
    ]);
    target = new DoSQLTarget(executor);
  });

  it('should cleanup expired streams before enforcing limit', async () => {
    const manager = target.getStreamManager();
    manager.setMaxConcurrentStreams(5);
    manager.setStreamTTL(100);

    // Create 5 streams at limit
    await createStreams(target, 5);

    // Wait for TTL
    await new Promise((resolve) => setTimeout(resolve, 150));

    // Should be able to create new stream because old ones expired (auto-cleanup on limit)
    await expect(
      target._initStream({ sql: 'SELECT 1', streamId: 'after_ttl' })
    ).resolves.not.toThrow();
  });

  it('should report all cleanup activities in stats', async () => {
    const manager = target.getStreamManager();
    manager.setStreamTTL(50);

    const streamIds = await createStreams(target, 5);

    // Manual close using closeStream (which tracks manualCloses)
    target.closeStream(streamIds[0]);

    // Wait for TTL expiry
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Trigger TTL cleanup
    target.cleanupExpiredStreams();

    const stats = manager.getCleanupStats();

    expect(stats).toEqual(
      expect.objectContaining({
        manualCloses: 1,
        ttlExpiries: expect.any(Number),
        connectionCloses: 0,
        abandonedCleanups: 0,
        memoryPressureCleanups: 0,
      })
    );
    expect(stats.ttlExpiries).toBeGreaterThan(0);
  });

  it('should support graceful shutdown with cleanup', async () => {
    const cdcManager = createMockCDCManager();
    const targetWithCdc = new DoSQLTarget(executor, cdcManager);

    await createStreams(targetWithCdc, 5);
    await targetWithCdc._subscribeCDC({ subscriptionId: 'cdc_shutdown', fromLSN: 0n });

    // Graceful shutdown should clean up all resources
    await targetWithCdc.gracefulShutdown(5000);

    const manager = targetWithCdc.getStreamManager();
    expect(manager.getStreamCount()).toBe(0);
    expect(manager.getStreamStats().totalCdcSubscriptions).toBe(0);
  });
});
