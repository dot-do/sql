/**
 * Stream State Cleanup - RED Phase TDD Tests
 *
 * These tests document stream state cleanup gaps that need implementation.
 * Tests use `it.fails()` pattern to mark expected failures.
 *
 * Issue: sql-zhy.25 - Stream State Cleanup
 *
 * Gap Areas:
 * 1. TTL-based expiration - Streams should auto-cleanup after inactivity timeout
 * 2. Maximum concurrent streams - Limit number of concurrent streams per connection
 * 3. Cleanup on connection close - All streams cleaned up when WebSocket closes
 * 4. Abandoned stream detection - Detect streams with no activity for X minutes
 * 5. Manual cleanup API - closeStream(), closeAllStreams(), getStreamStats()
 * 6. Memory usage monitoring - Track total stream memory, alert on limits
 *
 * Current State (from server.ts lines 157-158, 210-276):
 * - #streams: Map<string, StreamState> - stores active streams
 * - #cdcSubscriptions: Map<string, CDCSubscription> - stores CDC subscriptions
 * - No TTL mechanism for automatic cleanup
 * - No limits on concurrent streams
 * - No cleanup hooks for connection close
 * - No memory monitoring
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { DoSQLTarget, MockQueryExecutor } from '../server.js';
import type { CDCManager, CDCSubscription, CDCSubscribeOptions } from '../server.js';

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

/**
 * Interface that stream manager SHOULD support
 * This documents the expected API for stream cleanup
 */
interface StreamManager {
  /** Configure stream TTL */
  setStreamTTL(ttlMs: number): void;
  /** Configure max concurrent streams */
  setMaxConcurrentStreams(limit: number): void;
  /** Get current stream count */
  getStreamCount(): number;
  /** Get stream statistics */
  getStreamStats(): StreamStats;
  /** Close a specific stream */
  closeStream(streamId: string): Promise<void>;
  /** Close all streams */
  closeAllStreams(): Promise<void>;
  /** Get memory usage estimate */
  getMemoryUsage(): MemoryUsage;
  /** Register cleanup callback for connection close */
  onConnectionClose(callback: () => void): void;
}

interface StreamStats {
  totalStreams: number;
  totalCdcSubscriptions: number;
  oldestStreamAgeMs: number;
  totalRowsBuffered: number;
  averageStreamAgeMs: number;
}

interface MemoryUsage {
  estimatedBytes: number;
  streamCount: number;
  cdcSubscriptionCount: number;
  warningThreshold: number;
  criticalThreshold: number;
  status: 'ok' | 'warning' | 'critical';
}

// =============================================================================
// 1. TTL-BASED EXPIRATION
// =============================================================================

describe('Stream State Cleanup - TTL-Based Expiration [RED]', () => {
  let executor: MockQueryExecutor;
  let target: DoSQLTarget;

  beforeEach(() => {
    executor = new MockQueryExecutor();
    executor.addTable('users', ['id', 'name'], ['number', 'string'], [
      [1, 'Alice'],
      [2, 'Bob'],
      [3, 'Charlie'],
    ]);
    target = new DoSQLTarget(executor);
  });

  it.fails('should automatically cleanup streams after inactivity timeout', async () => {
    // Gap: Streams are never automatically cleaned up
    // They remain in #streams Map indefinitely until manually deleted

    const streamId = `stream_ttl_${Date.now()}`;

    // Initialize a stream
    await target._initStream({
      sql: 'SELECT * FROM users',
      streamId,
      chunkSize: 1,
    });

    // Configure TTL (this method doesn't exist yet)
    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    manager.setStreamTTL(100); // 100ms TTL

    // Wait for TTL to expire
    await new Promise((resolve) => setTimeout(resolve, 150));

    // Stream should be automatically cleaned up
    // Currently this will fail because no TTL mechanism exists
    await expect(target._nextChunk(streamId)).rejects.toThrow('Stream stream_ttl');
  });

  it.fails('should reset TTL timer on stream activity', async () => {
    // Gap: No TTL timer exists to reset

    const streamId = `stream_ttl_reset_${Date.now()}`;

    await target._initStream({
      sql: 'SELECT * FROM users',
      streamId,
      chunkSize: 1,
    });

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    manager.setStreamTTL(100); // 100ms TTL

    // Activity within TTL should reset timer
    await new Promise((resolve) => setTimeout(resolve, 50));
    await target._nextChunk(streamId); // Activity resets TTL

    await new Promise((resolve) => setTimeout(resolve, 50));
    await target._nextChunk(streamId); // Another activity

    // Stream should still exist because we had activity
    const chunk = await target._nextChunk(streamId);
    expect(chunk).toBeDefined();
  });

  it.fails('should cleanup CDC subscriptions after no messages received', async () => {
    // Gap: CDC subscriptions have no TTL mechanism

    const cdcManager = createMockCDCManager();
    const targetWithCdc = new DoSQLTarget(executor, cdcManager);

    const subscriptionId = `cdc_ttl_${Date.now()}`;

    await targetWithCdc._subscribeCDC({
      subscriptionId,
      fromLSN: 0n,
      tables: ['users'],
    });

    // Configure CDC subscription TTL
    const manager = (targetWithCdc as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    manager.setStreamTTL(100); // 100ms TTL for CDC too

    // Wait for TTL to expire with no messages
    await new Promise((resolve) => setTimeout(resolve, 150));

    // Subscription should be automatically cleaned up
    await expect(targetWithCdc._pollCDC(subscriptionId)).rejects.toThrow('Subscription');
  });

  it.fails('should support configurable TTL per stream type', async () => {
    // Gap: No per-stream-type TTL configuration

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();

    // Should support different TTLs for query streams vs CDC subscriptions
    const config = {
      queryStreamTTL: 5 * 60 * 1000, // 5 minutes for query streams
      cdcSubscriptionTTL: 30 * 60 * 1000, // 30 minutes for CDC
    };

    // This configuration API doesn't exist
    expect(() => {
      (manager as unknown as { configure(cfg: unknown): void }).configure(config);
    }).not.toThrow();
  });
});

// =============================================================================
// 2. MAXIMUM CONCURRENT STREAMS
// =============================================================================

describe('Stream State Cleanup - Maximum Concurrent Streams [RED]', () => {
  let executor: MockQueryExecutor;
  let target: DoSQLTarget;

  beforeEach(() => {
    executor = new MockQueryExecutor();
    executor.addTable('users', ['id', 'name'], ['number', 'string'], [
      [1, 'Alice'],
      [2, 'Bob'],
    ]);
    target = new DoSQLTarget(executor);
  });

  it.fails('should limit number of concurrent streams per connection', async () => {
    // Gap: No limit on concurrent streams - can create unlimited streams

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    manager.setMaxConcurrentStreams(5);

    // Create 5 streams (at limit)
    const streamIds = await createStreams(target, 5);
    expect(streamIds).toHaveLength(5);

    // 6th stream should be rejected
    await expect(
      target._initStream({
        sql: 'SELECT * FROM users',
        streamId: 'stream_over_limit',
      })
    ).rejects.toThrow('Maximum concurrent streams exceeded');
  });

  it.fails('should reject new streams when limit reached with specific error code', async () => {
    // Gap: No limit mechanism exists, so no error codes for limit exceeded

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    manager.setMaxConcurrentStreams(2);

    await createStreams(target, 2);

    try {
      await target._initStream({
        sql: 'SELECT * FROM users',
        streamId: 'stream_rejected',
      });
      expect.fail('Should have thrown');
    } catch (error) {
      expect((error as { code?: string }).code).toBe('MAX_STREAMS_EXCEEDED');
      expect((error as Error).message).toContain('limit: 2');
    }
  });

  it.fails('should allow new streams after closing existing ones', async () => {
    // Gap: Even though _closeStream exists, there's no limit enforcement

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    manager.setMaxConcurrentStreams(2);

    const [stream1, stream2] = await createStreams(target, 2);

    // At limit, this should fail
    await expect(
      target._initStream({ sql: 'SELECT 1', streamId: 'blocked' })
    ).rejects.toThrow('Maximum concurrent streams');

    // Close one stream
    await target._closeStream(stream1);

    // Now we should be able to create a new stream
    const newStreamId = 'new_stream_after_close';
    await target._initStream({ sql: 'SELECT 1', streamId: newStreamId });

    expect(manager.getStreamCount()).toBe(2);
  });

  it.fails('should track CDC subscriptions in concurrent limit', async () => {
    // Gap: CDC subscriptions and query streams have separate Maps with no combined limit

    const cdcManager = createMockCDCManager();
    const targetWithCdc = new DoSQLTarget(executor, cdcManager);

    const manager = (targetWithCdc as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    manager.setMaxConcurrentStreams(3); // Combined limit

    // Create 2 query streams
    await createStreams(targetWithCdc, 2);

    // Create 1 CDC subscription
    await targetWithCdc._subscribeCDC({
      subscriptionId: 'cdc_1',
      fromLSN: 0n,
    });

    // Should be at limit now (2 streams + 1 CDC = 3)
    await expect(
      targetWithCdc._initStream({ sql: 'SELECT 1', streamId: 'blocked' })
    ).rejects.toThrow('Maximum concurrent streams');

    await expect(
      targetWithCdc._subscribeCDC({ subscriptionId: 'cdc_blocked', fromLSN: 0n })
    ).rejects.toThrow('Maximum concurrent streams');
  });

  it.fails('should support separate limits for streams and CDC subscriptions', async () => {
    // Gap: No separate limit configuration

    const cdcManager = createMockCDCManager();
    const targetWithCdc = new DoSQLTarget(executor, cdcManager);

    const config = {
      maxQueryStreams: 10,
      maxCdcSubscriptions: 5,
    };

    // This configuration doesn't exist
    const manager = (targetWithCdc as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    (manager as unknown as { configure(cfg: unknown): void }).configure(config);

    // Should allow up to 10 query streams
    await createStreams(targetWithCdc, 10);

    // And up to 5 CDC subscriptions
    for (let i = 0; i < 5; i++) {
      await targetWithCdc._subscribeCDC({
        subscriptionId: `cdc_${i}`,
        fromLSN: 0n,
      });
    }

    // Both should be at their respective limits
    expect(manager.getStreamStats().totalStreams).toBe(10);
    expect(manager.getStreamStats().totalCdcSubscriptions).toBe(5);
  });
});

// =============================================================================
// 3. CLEANUP ON CONNECTION CLOSE
// =============================================================================

describe('Stream State Cleanup - Connection Close Cleanup [RED]', () => {
  let executor: MockQueryExecutor;
  let target: DoSQLTarget;

  beforeEach(() => {
    executor = new MockQueryExecutor();
    executor.addTable('data', ['id'], ['number'], [[1], [2], [3]]);
    target = new DoSQLTarget(executor);
  });

  it.fails('should cleanup all streams when WebSocket closes', async () => {
    // Gap: No hook for WebSocket close events in DoSQLTarget

    const streamIds = await createStreams(target, 5);
    expect(streamIds).toHaveLength(5);

    // Simulate WebSocket close
    // This method doesn't exist - we need a way to signal connection close
    await (target as unknown as { handleConnectionClose(): Promise<void> }).handleConnectionClose();

    // All streams should be cleaned up
    for (const streamId of streamIds) {
      await expect(target._nextChunk(streamId)).rejects.toThrow('not found');
    }

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    expect(manager.getStreamCount()).toBe(0);
  });

  it.fails('should cleanup CDC subscriptions on disconnect', async () => {
    // Gap: No disconnect handler for CDC subscriptions

    const cdcManager = createMockCDCManager();
    const targetWithCdc = new DoSQLTarget(executor, cdcManager);

    // Create multiple CDC subscriptions
    await targetWithCdc._subscribeCDC({ subscriptionId: 'cdc_1', fromLSN: 0n });
    await targetWithCdc._subscribeCDC({ subscriptionId: 'cdc_2', fromLSN: 0n });
    await targetWithCdc._subscribeCDC({ subscriptionId: 'cdc_3', fromLSN: 0n });

    // Simulate disconnect
    await (targetWithCdc as unknown as { handleConnectionClose(): Promise<void> }).handleConnectionClose();

    // All subscriptions should be cleaned up
    await expect(targetWithCdc._pollCDC('cdc_1')).rejects.toThrow('not found');
    await expect(targetWithCdc._pollCDC('cdc_2')).rejects.toThrow('not found');
    await expect(targetWithCdc._pollCDC('cdc_3')).rejects.toThrow('not found');
  });

  it.fails('should call unsubscribe on CDC subscriptions during cleanup', async () => {
    // Gap: Cleanup doesn't properly call unsubscribe() on CDC subscriptions

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

    // Simulate disconnect
    await (targetWithCdc as unknown as { handleConnectionClose(): Promise<void> }).handleConnectionClose();

    // unsubscribe should have been called
    expect(unsubscribeSpy).toHaveBeenCalledTimes(1);
  });

  it.fails('should support registering cleanup callbacks', async () => {
    // Gap: No callback registration for cleanup events

    const cleanupCallback = vi.fn();

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    manager.onConnectionClose(cleanupCallback);

    await createStreams(target, 3);

    // Simulate connection close
    await (target as unknown as { handleConnectionClose(): Promise<void> }).handleConnectionClose();

    expect(cleanupCallback).toHaveBeenCalledTimes(1);
  });

  it.fails('should handle cleanup errors gracefully', async () => {
    // Gap: No error handling during cleanup

    const errorCdcManager: CDCManager = {
      subscribe() {
        return {
          id: 'error_test',
          events: { async *[Symbol.asyncIterator]() {} },
          unsubscribe: () => {
            throw new Error('Unsubscribe failed');
          },
        };
      },
      async getEventsSince() {
        return [];
      },
    };

    const targetWithCdc = new DoSQLTarget(executor, errorCdcManager);

    await targetWithCdc._subscribeCDC({ subscriptionId: 'cdc_error', fromLSN: 0n });
    await createStreams(targetWithCdc, 2);

    // Should not throw even if individual cleanup fails
    await expect(
      (targetWithCdc as unknown as { handleConnectionClose(): Promise<void> }).handleConnectionClose()
    ).resolves.not.toThrow();

    // Other streams should still be cleaned up
    const manager = (targetWithCdc as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    expect(manager.getStreamCount()).toBe(0);
  });
});

// =============================================================================
// 4. ABANDONED STREAM DETECTION
// =============================================================================

describe('Stream State Cleanup - Abandoned Stream Detection [RED]', () => {
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

  it.fails('should detect streams with no activity for X minutes', async () => {
    // Gap: No activity tracking on streams
    // StreamState interface has no lastActivity timestamp

    const streamId = `abandoned_${Date.now()}`;

    await target._initStream({
      sql: 'SELECT * FROM logs',
      streamId,
      chunkSize: 1,
    });

    // Advance time by 5 minutes (simulated)
    vi.useFakeTimers();
    vi.advanceTimersByTime(5 * 60 * 1000);

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    const stats = manager.getStreamStats();

    // Should report this stream as potentially abandoned
    expect(stats.oldestStreamAgeMs).toBeGreaterThanOrEqual(5 * 60 * 1000);

    vi.useRealTimers();
  });

  it.fails('should log warning for potential memory leaks', async () => {
    // Gap: No logging for abandoned streams

    const warnSpy = vi.spyOn(console, 'warn');

    await createStreams(target, 10);

    // Advance time
    vi.useFakeTimers();
    vi.advanceTimersByTime(10 * 60 * 1000); // 10 minutes

    // Run abandoned stream check
    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    await (manager as unknown as { checkAbandonedStreams(): Promise<void> }).checkAbandonedStreams();

    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining('Potential memory leak'),
      expect.objectContaining({ abandonedCount: 10 })
    );

    vi.useRealTimers();
    warnSpy.mockRestore();
  });

  it.fails('should provide list of abandoned streams', async () => {
    // Gap: No method to list abandoned streams

    const streamIds = await createStreams(target, 3);

    // Make one stream "active" by fetching from it
    await target._nextChunk(streamIds[0]);

    vi.useFakeTimers();
    vi.advanceTimersByTime(5 * 60 * 1000);

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    const abandoned = (manager as unknown as { getAbandonedStreams(thresholdMs: number): string[] })
      .getAbandonedStreams(4 * 60 * 1000);

    // streamIds[0] was active, so only [1] and [2] should be abandoned
    expect(abandoned).toHaveLength(2);
    expect(abandoned).toContain(streamIds[1]);
    expect(abandoned).toContain(streamIds[2]);
    expect(abandoned).not.toContain(streamIds[0]);

    vi.useRealTimers();
  });

  it.fails('should auto-cleanup abandoned streams when threshold exceeded', async () => {
    // Gap: No auto-cleanup mechanism for abandoned streams

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();

    // Configure auto-cleanup threshold
    (manager as unknown as { setAbandonedThreshold(ms: number): void }).setAbandonedThreshold(
      5 * 60 * 1000 // 5 minutes
    );
    (manager as unknown as { enableAutoCleanup(enabled: boolean): void }).enableAutoCleanup(true);

    const streamIds = await createStreams(target, 5);

    vi.useFakeTimers();
    vi.advanceTimersByTime(6 * 60 * 1000); // Past threshold

    // Auto-cleanup should have run
    expect(manager.getStreamCount()).toBe(0);

    // All streams should be gone
    for (const streamId of streamIds) {
      await expect(target._nextChunk(streamId)).rejects.toThrow('not found');
    }

    vi.useRealTimers();
  });
});

// =============================================================================
// 5. MANUAL CLEANUP API
// =============================================================================

describe('Stream State Cleanup - Manual Cleanup API [RED]', () => {
  let executor: MockQueryExecutor;
  let target: DoSQLTarget;

  beforeEach(() => {
    executor = new MockQueryExecutor();
    executor.addTable('items', ['id'], ['number'], [[1], [2], [3]]);
    target = new DoSQLTarget(executor);
  });

  it.fails('should provide closeStream(streamId) method via manager', async () => {
    // Gap: _closeStream exists but there's no manager interface

    const streamId = `manual_close_${Date.now()}`;

    await target._initStream({
      sql: 'SELECT * FROM items',
      streamId,
    });

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    expect(manager.getStreamCount()).toBe(1);

    await manager.closeStream(streamId);

    expect(manager.getStreamCount()).toBe(0);
    await expect(target._nextChunk(streamId)).rejects.toThrow('not found');
  });

  it.fails('should provide closeAllStreams() method', async () => {
    // Gap: No closeAllStreams method exists

    const streamIds = await createStreams(target, 5);
    expect(streamIds).toHaveLength(5);

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    expect(manager.getStreamCount()).toBe(5);

    await manager.closeAllStreams();

    expect(manager.getStreamCount()).toBe(0);

    for (const streamId of streamIds) {
      await expect(target._nextChunk(streamId)).rejects.toThrow('not found');
    }
  });

  it.fails('should provide getStreamStats() method', async () => {
    // Gap: No stream statistics API

    const cdcManager = createMockCDCManager();
    const targetWithCdc = new DoSQLTarget(executor, cdcManager);

    // Create various streams
    await createStreams(targetWithCdc, 3);
    await targetWithCdc._subscribeCDC({ subscriptionId: 'cdc_stats', fromLSN: 0n });

    const manager = (targetWithCdc as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    const stats = manager.getStreamStats();

    expect(stats).toEqual(
      expect.objectContaining({
        totalStreams: 3,
        totalCdcSubscriptions: 1,
        oldestStreamAgeMs: expect.any(Number),
        totalRowsBuffered: expect.any(Number),
        averageStreamAgeMs: expect.any(Number),
      })
    );
  });

  it.fails('should return detailed stats per stream', async () => {
    // Gap: No per-stream statistics

    const streamId = `stats_detail_${Date.now()}`;

    await target._initStream({
      sql: 'SELECT * FROM items',
      streamId,
      chunkSize: 1,
    });

    // Fetch some chunks
    await target._nextChunk(streamId);
    await target._nextChunk(streamId);

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    const streamStats = (manager as unknown as { getStreamDetails(id: string): unknown }).getStreamDetails(streamId);

    expect(streamStats).toEqual(
      expect.objectContaining({
        streamId,
        createdAt: expect.any(Number),
        lastActivityAt: expect.any(Number),
        chunksDelivered: 2,
        rowsDelivered: 2,
        sql: 'SELECT * FROM items',
      })
    );
  });

  it.fails('should close streams matching a pattern', async () => {
    // Gap: No pattern-based stream closing

    await target._initStream({ sql: 'SELECT * FROM items', streamId: 'user_123_stream_1' });
    await target._initStream({ sql: 'SELECT * FROM items', streamId: 'user_123_stream_2' });
    await target._initStream({ sql: 'SELECT * FROM items', streamId: 'user_456_stream_1' });

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();

    // Close all streams for user 123
    const closed = (manager as unknown as { closeStreamsByPattern(pattern: RegExp): Promise<number> })
      .closeStreamsByPattern(/^user_123_/);

    expect(await closed).toBe(2);
    expect(manager.getStreamCount()).toBe(1);
  });
});

// =============================================================================
// 6. MEMORY USAGE MONITORING
// =============================================================================

describe('Stream State Cleanup - Memory Usage Monitoring [RED]', () => {
  let executor: MockQueryExecutor;
  let target: DoSQLTarget;

  beforeEach(() => {
    executor = new MockQueryExecutor();
    // Create table with larger data for memory testing
    const rows = Array.from({ length: 1000 }, (_, i) => [i, `data_${i}`.repeat(100)]);
    executor.addTable('large_data', ['id', 'content'], ['number', 'string'], rows);
    target = new DoSQLTarget(executor);
  });

  it.fails('should track total stream memory', async () => {
    // Gap: No memory tracking for streams
    // StreamState stores data but doesn't track memory size

    await createStreams(target, 5);

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    const memoryUsage = manager.getMemoryUsage();

    expect(memoryUsage.estimatedBytes).toBeGreaterThan(0);
    expect(memoryUsage.streamCount).toBe(5);
  });

  it.fails('should alert when approaching memory limits', async () => {
    // Gap: No memory limit configuration or alerting

    const warnSpy = vi.spyOn(console, 'warn');

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();

    // Configure memory limits
    (manager as unknown as { setMemoryLimits(warn: number, critical: number): void }).setMemoryLimits(
      1024 * 1024, // 1MB warning
      5 * 1024 * 1024 // 5MB critical
    );

    // Create streams that consume significant memory
    for (let i = 0; i < 50; i++) {
      await target._initStream({
        sql: 'SELECT * FROM large_data',
        streamId: `memory_stream_${i}`,
        chunkSize: 100,
      });
    }

    const memoryUsage = manager.getMemoryUsage();

    // Should have triggered warning
    expect(memoryUsage.status).toBe('warning');
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining('Memory warning'),
      expect.objectContaining({ estimatedBytes: expect.any(Number) })
    );

    warnSpy.mockRestore();
  });

  it.fails('should provide memory usage breakdown by stream type', async () => {
    // Gap: No memory breakdown by type

    const cdcManager = createMockCDCManager();
    const targetWithCdc = new DoSQLTarget(executor, cdcManager);

    await createStreams(targetWithCdc, 3);
    await targetWithCdc._subscribeCDC({ subscriptionId: 'cdc_mem', fromLSN: 0n });

    const manager = (targetWithCdc as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    const breakdown = (manager as unknown as { getMemoryBreakdown(): Record<string, number> }).getMemoryBreakdown();

    expect(breakdown).toEqual(
      expect.objectContaining({
        queryStreams: expect.any(Number),
        cdcSubscriptions: expect.any(Number),
        metadata: expect.any(Number),
        total: expect.any(Number),
      })
    );
  });

  it.fails('should reject new streams when critical memory limit reached', async () => {
    // Gap: No memory-based rejection of new streams

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();

    // Set a very low critical threshold for testing
    (manager as unknown as { setMemoryLimits(warn: number, critical: number): void }).setMemoryLimits(
      100, // 100 bytes warning
      500 // 500 bytes critical
    );

    // Create streams until we hit the limit
    let created = 0;
    try {
      for (let i = 0; i < 100; i++) {
        await target._initStream({
          sql: 'SELECT * FROM large_data',
          streamId: `overflow_${i}`,
        });
        created++;
      }
    } catch (error) {
      expect((error as Error).message).toContain('Memory limit exceeded');
    }

    // Should have created some but not all
    expect(created).toBeGreaterThan(0);
    expect(created).toBeLessThan(100);

    const memoryUsage = manager.getMemoryUsage();
    expect(memoryUsage.status).toBe('critical');
  });

  it.fails('should emit memory metrics for monitoring systems', async () => {
    // Gap: No metrics emission for monitoring integration

    const metricsCallback = vi.fn();

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();

    // Register metrics callback
    (manager as unknown as { onMetrics(cb: (m: unknown) => void): void }).onMetrics(metricsCallback);

    await createStreams(target, 5);

    expect(metricsCallback).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'stream_memory',
        estimatedBytes: expect.any(Number),
        streamCount: 5,
        timestamp: expect.any(Number),
      })
    );
  });

  it.fails('should track peak memory usage', async () => {
    // Gap: No peak memory tracking

    await createStreams(target, 10);

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    const peakBefore = (manager as unknown as { getPeakMemoryUsage(): number }).getPeakMemoryUsage();

    // Close half the streams
    await manager.closeAllStreams();

    const peakAfter = (manager as unknown as { getPeakMemoryUsage(): number }).getPeakMemoryUsage();

    // Peak should remain at the higher value
    expect(peakAfter).toBe(peakBefore);
    expect(peakAfter).toBeGreaterThan(manager.getMemoryUsage().estimatedBytes);
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

  it.fails('should cleanup expired streams before enforcing limit', async () => {
    // Gap: No coordination between TTL cleanup and limit enforcement

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    manager.setMaxConcurrentStreams(5);
    manager.setStreamTTL(100);

    // Create 5 streams at limit
    await createStreams(target, 5);

    // Wait for TTL
    await new Promise((resolve) => setTimeout(resolve, 150));

    // Should be able to create new stream because old ones expired
    await expect(
      target._initStream({ sql: 'SELECT 1', streamId: 'after_ttl' })
    ).resolves.not.toThrow();
  });

  it.fails('should report all cleanup activities in stats', async () => {
    // Gap: No cleanup activity reporting

    const manager = (target as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    manager.setStreamTTL(50);

    await createStreams(target, 5);

    // Manual close
    await target._closeStream('stream_0');

    // Wait for TTL expiry
    await new Promise((resolve) => setTimeout(resolve, 100));

    const stats = (manager as unknown as { getCleanupStats(): unknown }).getCleanupStats();

    expect(stats).toEqual(
      expect.objectContaining({
        manualCloses: 1,
        ttlExpiries: expect.any(Number),
        connectionCloses: 0,
        abandonedCleanups: 0,
        memoryPressureCleanups: 0,
      })
    );
  });

  it.fails('should support graceful shutdown with cleanup', async () => {
    // Gap: No graceful shutdown support

    const cdcManager = createMockCDCManager();
    const targetWithCdc = new DoSQLTarget(executor, cdcManager);

    await createStreams(targetWithCdc, 5);
    await targetWithCdc._subscribeCDC({ subscriptionId: 'cdc_shutdown', fromLSN: 0n });

    // Graceful shutdown should wait for in-flight operations
    await (targetWithCdc as unknown as { gracefulShutdown(timeoutMs: number): Promise<void> })
      .gracefulShutdown(5000);

    const manager = (targetWithCdc as unknown as { getStreamManager(): StreamManager }).getStreamManager();
    expect(manager.getStreamCount()).toBe(0);
    expect(manager.getStreamStats().totalCdcSubscriptions).toBe(0);
  });
});
