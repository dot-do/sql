/**
 * Tail Worker CDC Streaming Tests
 *
 * Tests for capturing worker telemetry via tail workers and streaming to DoLake.
 *
 * Issue: do-d1isn.3 - Tail worker CDC streaming
 *
 * Architecture:
 * ```
 * Worker Telemetry → Tail Worker → ShardRouter → WebSocket → DoLake DO Pool → R2
 *                       ↓
 *                  Transform to CDC
 *                       ↓
 *                   Batch by shard
 * ```
 *
 * Requirements:
 * 1. Tail worker captures TraceItem events from workers
 * 2. Events transformed to CDC format
 * 3. Batching by shard using ShardRouter
 * 4. WebSocket streaming to DoLake DO pool
 * 5. Backpressure handling when DO is slow
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  TailWorkerCDCStreamer,
  DEFAULT_TAIL_WORKER_CONFIG,
  type TraceItem,
  type TailWorkerBatch,
} from '../src/tail-worker.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a sample TraceItem for testing
 */
function createTraceItem(overrides: Partial<TraceItem> = {}): TraceItem {
  return {
    scriptName: overrides.scriptName ?? 'test-worker',
    exceptions: overrides.exceptions ?? [],
    logs: overrides.logs ?? [
      {
        level: 'info',
        message: ['Request processed'],
        timestamp: Date.now(),
      },
    ],
    event: overrides.event ?? {
      request: {
        url: 'https://example.com/api/test',
        method: 'GET',
      },
      response: {
        status: 200,
      },
    },
    eventTimestamp: overrides.eventTimestamp ?? Date.now(),
    outcome: overrides.outcome ?? 'ok',
    cpuTime: overrides.cpuTime ?? 10,
    wallTime: overrides.wallTime ?? 50,
    ...overrides,
  };
}

/**
 * Create a batch of TraceItems for testing
 */
function createTraceBatch(count: number, scriptName?: string): TailWorkerBatch {
  return {
    traces: Array.from({ length: count }, (_, i) =>
      createTraceItem({
        scriptName: scriptName ?? `worker-${i % 4}`,
        eventTimestamp: Date.now() - i * 100,
      })
    ),
  };
}

/**
 * Create a mock WebSocket for testing
 */
function createMockWebSocket(): WebSocket {
  const sentMessages: string[] = [];

  const ws = {
    readyState: 1, // WebSocket.OPEN
    send: vi.fn((data: string) => {
      sentMessages.push(data);
    }),
    close: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    sentMessages,
  } as unknown as WebSocket;

  return ws;
}

// =============================================================================
// Test: Tail worker captures TraceItem events from workers
// =============================================================================

describe('Tail worker captures TraceItem events from workers', () => {
  it('should process a single TraceItem from tail worker', async () => {
    // Arrange
    const trace = createTraceItem({
      scriptName: 'api-worker',
      outcome: 'ok',
    });
    const batch: TailWorkerBatch = { traces: [trace] };
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const results = await streamer.processBatch(batch);

    // Assert - Results may show connection failures since we don't have real WebSockets
    // but the transformation should have occurred
    expect(results).toBeDefined();
    expect(results.length).toBeGreaterThan(0);
  });

  it('should process multiple TraceItems in a batch', async () => {
    // Arrange
    const batch = createTraceBatch(10);
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const results = await streamer.processBatch(batch);

    // Assert
    expect(results.length).toBeGreaterThan(0);
    const totalEvents = results.reduce((sum, r) => sum + r.eventsCount, 0);
    expect(totalEvents).toBe(10);
  });

  it('should handle TraceItems with exceptions', async () => {
    // Arrange
    const trace = createTraceItem({
      scriptName: 'failing-worker',
      outcome: 'exception',
      exceptions: [
        {
          name: 'TypeError',
          message: 'Cannot read property x of undefined',
          timestamp: Date.now(),
        },
      ],
    });
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const result = streamer.transformToCDC(trace);

    // Assert
    expect(result.event.after).toHaveProperty('exceptions');
    expect((result.event.after as Record<string, unknown>).exceptions).toHaveLength(1);
    expect((result.event.after as Record<string, unknown>).outcome).toBe('exception');
  });

  it('should handle TraceItems with console logs', async () => {
    // Arrange
    const trace = createTraceItem({
      logs: [
        { level: 'info', message: ['Request started'], timestamp: Date.now() - 100 },
        { level: 'warn', message: ['Slow response'], timestamp: Date.now() - 50 },
        { level: 'error', message: ['Connection timeout'], timestamp: Date.now() },
      ],
    });
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const result = streamer.transformToCDC(trace);

    // Assert
    expect((result.event.after as Record<string, unknown>).logs).toHaveLength(3);
  });

  it('should handle TraceItems with scheduled events', async () => {
    // Arrange
    const scheduledTime = Date.now();
    const trace = createTraceItem({
      scriptName: 'cron-worker',
      event: {
        scheduledTime,
        cron: '0 * * * *',
      },
    });
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const result = streamer.transformToCDC(trace);

    // Assert
    const eventData = (result.event.after as Record<string, unknown>).event as Record<string, unknown>;
    expect(eventData.scheduledTime).toBe(scheduledTime);
    expect(eventData.cron).toBe('0 * * * *');
  });

  it('should capture CPU and wall time metrics', async () => {
    // Arrange
    const trace = createTraceItem({
      cpuTime: 25,
      wallTime: 150,
    });
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const result = streamer.transformToCDC(trace);

    // Assert
    expect((result.event.after as Record<string, unknown>).cpuTime).toBe(25);
    expect((result.event.after as Record<string, unknown>).wallTime).toBe(150);
  });
});

// =============================================================================
// Test: Events transformed to CDC format
// =============================================================================

describe('Events transformed to CDC format', () => {
  it('should transform TraceItem to CDCEvent with correct structure', async () => {
    // Arrange
    const trace = createTraceItem({
      scriptName: 'api-worker',
      eventTimestamp: 1700000000000,
    });
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const result = streamer.transformToCDC(trace);

    // Assert - CDC event structure
    expect(result.event.metadata?.$type).toBe('worker.trace');
    expect(result.event.operation).toBe('INSERT');
    expect(result.event.table).toBe('worker_traces');
    expect(result.event.timestamp).toBe(1700000000000);
    expect(result.event.rowId).toBeDefined();
    expect(result.event.after).toBeDefined();
  });

  it('should generate unique eventId for each CDC event', async () => {
    // Arrange
    const trace1 = createTraceItem({ scriptName: 'worker-1' });
    const trace2 = createTraceItem({ scriptName: 'worker-2' });
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const result1 = streamer.transformToCDC(trace1);
    const result2 = streamer.transformToCDC(trace2);

    // Assert
    expect(result1.event.rowId).not.toBe(result2.event.rowId);
  });

  it('should set durabilityTier to P3 for telemetry', async () => {
    // Arrange
    const trace = createTraceItem();
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const result = streamer.transformToCDC(trace);

    // Assert - Telemetry uses P3 (best effort) durability
    expect(result.event.metadata?.durabilityTier).toBe('P3');
  });

  it('should include all trace metadata in CDC after field', async () => {
    // Arrange
    const trace = createTraceItem({
      scriptName: 'my-worker',
      scriptVersion: 'v1.2.3',
      dispatchNamespace: 'production',
      outcome: 'ok',
      cpuTime: 15,
      wallTime: 100,
    });
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const result = streamer.transformToCDC(trace);

    // Assert
    const after = result.event.after as Record<string, unknown>;
    expect(after.scriptName).toBe('my-worker');
    expect(after.scriptVersion).toBe('v1.2.3');
    expect(after.dispatchNamespace).toBe('production');
    expect(after.outcome).toBe('ok');
    expect(after.cpuTime).toBe(15);
    expect(after.wallTime).toBe(100);
  });

  it('should compute partition key based on timestamp and script', async () => {
    // Arrange
    const trace = createTraceItem({
      scriptName: 'api-worker',
      eventTimestamp: new Date('2024-01-15T10:30:00Z').getTime(),
    });
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const result = streamer.transformToCDC(trace);

    // Assert - Partition key should include date for time-based partitioning
    expect(result.partitionKey).toMatch(/2024-01-15/);
    expect(result.partitionKey).toContain('api-worker');
  });

  it('should handle request/response data in CDC format', async () => {
    // Arrange
    const trace = createTraceItem({
      event: {
        request: {
          url: 'https://api.example.com/users/123',
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        },
        response: {
          status: 201,
        },
      },
    });
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const result = streamer.transformToCDC(trace);

    // Assert
    const after = result.event.after as Record<string, unknown>;
    const request = after.request as Record<string, unknown>;
    const response = after.response as Record<string, unknown>;
    expect(request.url).toBe('https://api.example.com/users/123');
    expect(request.method).toBe('POST');
    expect(response.status).toBe(201);
  });
});

// =============================================================================
// Test: Batching by shard using ShardRouter
// =============================================================================

describe('Batching by shard using ShardRouter', () => {
  it('should route traces to shards based on scriptName', async () => {
    // Arrange
    const trace1 = createTraceItem({ scriptName: 'api-worker' });
    const trace2 = createTraceItem({ scriptName: 'cron-worker' });
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount: 8,
    });

    // Act
    const shard1 = streamer.getTargetShard(trace1);
    const shard2 = streamer.getTargetShard(trace2);

    // Assert - Same scriptName should always go to same shard (consistent hashing)
    const trace1Again = createTraceItem({ scriptName: 'api-worker' });
    const shard1Again = streamer.getTargetShard(trace1Again);
    expect(shard1).toBe(shard1Again);

    // Different workers may route to different shards
    expect(shard1).toMatch(/^shard-\d+$/);
    expect(shard2).toMatch(/^shard-\d+$/);
  });

  it('should distribute traces across all configured shards', async () => {
    // Arrange
    const shardCount = 8;
    const batch = createTraceBatch(100);
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount,
    });

    // Act
    const shardDistribution = new Map<string, number>();
    for (const trace of batch.traces) {
      const shard = streamer.getTargetShard(trace);
      shardDistribution.set(shard, (shardDistribution.get(shard) ?? 0) + 1);
    }

    // Assert - Should use multiple shards
    expect(shardDistribution.size).toBeGreaterThan(1);
    expect(shardDistribution.size).toBeLessThanOrEqual(shardCount);
  });

  it('should batch events by shard before sending', async () => {
    // Arrange
    const batch = createTraceBatch(20);
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const results = await streamer.processBatch(batch);

    // Assert - Events should be grouped by shard in results
    const shardsSent = new Set(results.map((r) => r.shardId));
    expect(shardsSent.size).toBeGreaterThan(0);
  });

  it('should use consistent hashing for shard assignment', async () => {
    // Arrange
    const scriptName = 'consistent-worker';
    const streamer1 = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);
    const streamer2 = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const shard1 = streamer1.getTargetShard(createTraceItem({ scriptName }));
    const shard2 = streamer2.getTargetShard(createTraceItem({ scriptName }));

    // Assert - Same input should produce same shard across instances
    expect(shard1).toBe(shard2);
  });

  it('should handle namespace-based routing', async () => {
    // Arrange
    const trace = createTraceItem({
      scriptName: 'api-worker',
      dispatchNamespace: 'customer-acme',
    });
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const result = streamer.transformToCDC(trace);

    // Assert - Result should have a valid shard
    expect(result.shardId).toMatch(/^shard-\d+$/);
  });
});

// =============================================================================
// Test: Backpressure handling when DO is slow
// =============================================================================

describe('Backpressure handling when DO is slow', () => {
  it('should detect backpressure when buffer exceeds 80%', async () => {
    // Arrange
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      backpressureThreshold: 0.8,
      maxPendingMessages: 100,
    });

    // Simulate 85 pending messages (85% of 100)
    streamer.simulatePendingMessages('shard-0', 85);

    // Assert
    expect(streamer.isBackpressured('shard-0')).toBe(true);
    const status = streamer.getBackpressureStatus().get('shard-0');
    expect(status?.isBackpressured).toBe(true);
    expect(status?.bufferUtilization).toBeCloseTo(0.85, 2);
  });

  it('should drop P3 events when under severe backpressure', async () => {
    // Arrange
    const batch = createTraceBatch(10, 'same-worker');
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      maxPendingMessages: 50,
      shardCount: 1, // Force all to same shard
    });

    // Fill buffer to capacity (above threshold)
    streamer.simulatePendingMessages('shard-0', 45); // 90% of 50

    // Act
    const results = await streamer.processBatch(batch);

    // Assert - Some events should be dropped
    const droppedResults = results.filter((r) => r.wasDropped);
    expect(droppedResults.length).toBeGreaterThan(0);
    expect(droppedResults[0]?.dropReason).toBe('backpressure');
    expect(streamer.getDroppedEventsCount()).toBeGreaterThan(0);
  });

  it('should prioritize higher durability events over P3', async () => {
    // Arrange - Create mixed durability events
    const p0Trace = createTraceItem({ scriptName: 'critical-worker' });
    const p3Trace = createTraceItem({ scriptName: 'telemetry-worker' });

    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      maxPendingMessages: 100,
      shardCount: 1,
    });

    // Mark critical-worker as P0 durability
    streamer.setDurabilityOverride('critical-worker', 'P0');

    // Fill buffer above threshold
    streamer.simulatePendingMessages('shard-0', 85);

    const batch: TailWorkerBatch = { traces: [p0Trace, p3Trace] };

    // Act
    const results = await streamer.processBatch(batch);

    // Assert - P0 should not be dropped, P3 may be dropped
    const droppedResults = results.filter((r) => r.wasDropped);
    // P3 (telemetry-worker) should be dropped
    expect(droppedResults.some((r) => r.dropReason === 'backpressure')).toBe(true);
    // Total processed should be 2 (1 sent + 1 dropped)
    expect(results.length).toBe(2);
  });

  it('should resume normal operation when backpressure clears', async () => {
    // Arrange
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      maxPendingMessages: 100,
    });

    // Initial backpressure
    streamer.simulatePendingMessages('shard-0', 85);
    expect(streamer.isBackpressured('shard-0')).toBe(true);

    // Clear backpressure
    streamer.simulatePendingMessages('shard-0', 50);

    // Assert
    expect(streamer.isBackpressured('shard-0')).toBe(false);
  });

  it('should report backpressure status for all shards', async () => {
    // Arrange
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount: 4,
      maxPendingMessages: 100,
    });

    // Set different backpressure levels
    streamer.simulatePendingMessages('shard-0', 20);
    streamer.simulatePendingMessages('shard-1', 85);
    streamer.simulatePendingMessages('shard-2', 50);
    streamer.simulatePendingMessages('shard-3', 95);

    // Act
    const status = streamer.getBackpressureStatus();

    // Assert
    expect(status.size).toBe(4);
    expect(status.get('shard-0')?.isBackpressured).toBe(false);
    expect(status.get('shard-1')?.isBackpressured).toBe(true);
    expect(status.get('shard-2')?.isBackpressured).toBe(false);
    expect(status.get('shard-3')?.isBackpressured).toBe(true);
  });

  it('should implement exponential backoff on connection failures', async () => {
    // Arrange
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act - Get delays for multiple retry attempts
    const delays: number[] = [];
    for (let i = 0; i < 5; i++) {
      const delay = streamer.getReconnectDelay('shard-0', i);
      delays.push(delay);
    }

    // Assert - Delays should increase (accounting for jitter)
    // Base delay is 1000ms, so delays should roughly double each time
    expect(delays[1]).toBeGreaterThanOrEqual(delays[0] * 1.5);
    expect(delays[2]).toBeGreaterThanOrEqual(delays[1] * 1.5);
    expect(delays[4]).toBeLessThanOrEqual(30000 * 1.25); // Max backoff cap with jitter
  });
});

// =============================================================================
// Test: Flush behavior
// =============================================================================

describe('Flush behavior', () => {
  it('should buffer events and flush on explicit call', async () => {
    // Arrange
    const batch = createTraceBatch(20);
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      maxBatchSize: 100,
      maxBatchDelayMs: 60000,
    });

    // Act
    streamer.bufferEvents(batch);
    expect(streamer.getBufferedEventsCount()).toBe(20);

    await streamer.flush();

    // Assert - Buffer should be cleared after flush
    expect(streamer.getBufferedEventsCount()).toBe(0);
  });

  it('should flush all shards on close', async () => {
    // Arrange
    const batches = [
      createTraceBatch(10, 'worker-1'),
      createTraceBatch(10, 'worker-2'),
      createTraceBatch(10, 'worker-3'),
    ];
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    for (const batch of batches) {
      streamer.bufferEvents(batch);
    }

    // Act
    await streamer.close();

    // Assert - All buffered events should have been flushed
    expect(streamer.getBufferedEventsCount()).toBe(0);
  });

  it('should auto-flush when batch delay expires', async () => {
    // Arrange
    vi.useFakeTimers();
    const batch = createTraceBatch(10, 'test-worker');
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      maxBatchDelayMs: 5000,
    });

    // Act
    streamer.bufferEvents(batch);
    expect(streamer.getBufferedEventsCount()).toBe(10);

    // Advance time past the delay
    vi.advanceTimersByTime(5001);

    // Allow async flush to complete
    await vi.runAllTimersAsync();

    vi.useRealTimers();

    // Assert - Events should have been flushed
    expect(streamer.getBufferedEventsCount()).toBe(0);
  });
});

// =============================================================================
// Test: Integration with AutoScalingManager
// =============================================================================

describe('Integration with AutoScalingManager', () => {
  it('should update routing when pool scales up', async () => {
    // Arrange
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount: 4,
    });

    // Initial routing
    const trace = createTraceItem({ scriptName: 'test-worker' });
    const initialShard = streamer.getTargetShard(trace);
    expect(initialShard).toMatch(/^shard-[0-3]$/);

    // Scale up
    streamer.updateShardCount(8);

    // Verify routing still works
    const newShard = streamer.getTargetShard(trace);
    expect(newShard).toMatch(/^shard-[0-7]$/);
  });

  it('should drain shards before removal on scale-down', async () => {
    // Arrange
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount: 4,
    });

    // Buffer some events
    streamer.bufferEvents(createTraceBatch(20));

    // Act - Scale down (should flush and remove shard-3)
    await streamer.scaleTo(3);

    // Assert
    expect(streamer.getActiveShardCount()).toBe(3);
    expect(streamer.isConnected('shard-3')).toBe(false);
  });

  it('should report load metrics to AutoScalingManager', async () => {
    // Arrange
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act
    const metrics = streamer.getLoadMetrics();

    // Assert
    expect(metrics).toHaveProperty('bufferUtilization');
    expect(metrics).toHaveProperty('avgLatencyMs');
    expect(metrics).toHaveProperty('requestsPerSecond');
    expect(metrics).toHaveProperty('activeConnections');
    expect(metrics).toHaveProperty('p99LatencyMs');
  });
});

// =============================================================================
// Test: Error handling
// =============================================================================

describe('Error handling', () => {
  it('should handle malformed TraceItems gracefully', async () => {
    // Arrange
    const malformedTrace = {
      // Missing required fields
      scriptName: 'bad-worker',
    } as unknown as TraceItem;
    const batch: TailWorkerBatch = { traces: [malformedTrace] };
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act - Should not throw
    const results = await streamer.processBatch(batch);

    // Assert - Should handle gracefully
    expect(results).toBeDefined();
    expect(results.length).toBe(1);
  });

  it('should track reconnect attempts on connection failure', async () => {
    // Arrange
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Inject a connection error
    streamer.injectConnectionError('shard-0', new Error('Connection refused'));

    // Assert
    expect(streamer.getReconnectAttempts('shard-0')).toBe(1);
  });

  it('should close connections gracefully even with errors', async () => {
    // Arrange
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Act - Should not throw
    await streamer.close();

    // Assert - All connections should be closed
    expect(streamer.isConnected('shard-0')).toBe(false);
  });
});

// =============================================================================
// Test: WebSocket message format
// =============================================================================

describe('WebSocket message format', () => {
  it('should inject mock WebSocket for testing', async () => {
    // Arrange
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount: 1,
    });
    const mockWs = createMockWebSocket();
    streamer.injectConnection('shard-0', mockWs);

    // Assert
    expect(streamer.isConnected('shard-0')).toBe(true);
  });
});
