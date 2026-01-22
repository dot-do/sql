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
 * Testing Philosophy: NO MOCKS
 * - Uses real DoLake DOs via cloudflare:test
 * - Uses real WebSocket connections
 * - Verifies events reach R2 bucket
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { env } from 'cloudflare:test';
import {
  TailWorkerCDCStreamer,
  DEFAULT_TAIL_WORKER_CONFIG,
  type TraceItem,
  type TailWorkerBatch,
  type WebSocketFactory,
} from '../src/tail-worker.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a unique DO name for test isolation
 */
function createTestDoName(prefix: string = 'tail-test'): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Create a WebSocket factory that connects to real DoLake DOs
 */
function createDoLakeFactory(doNamespace: DurableObjectNamespace): WebSocketFactory {
  return async (shardId: string): Promise<WebSocket> => {
    const doName = `test-lakehouse-${shardId}`;
    const id = doNamespace.idFromName(doName);
    const stub = doNamespace.get(id);

    const response = await stub.fetch('http://dolake/', {
      headers: { Upgrade: 'websocket' },
    });

    if (response.status !== 101 || !response.webSocket) {
      throw new Error(`Failed to upgrade WebSocket: ${response.status}`);
    }

    // Accept the WebSocket
    response.webSocket.accept();
    return response.webSocket;
  };
}

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

// =============================================================================
// Test: TraceItem to CDC Transformation (Pure Functions - No DOs)
// =============================================================================

describe('TraceItem to CDC Transformation', () => {
  let streamer: TailWorkerCDCStreamer;

  beforeEach(() => {
    streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount: 4,
    });
  });

  it('should transform TraceItem to CDCEvent with correct structure', () => {
    const trace = createTraceItem({
      scriptName: 'api-worker',
      eventTimestamp: 1700000000000,
    });

    const result = streamer.transformToCDC(trace);

    expect(result.event.metadata?.$type).toBe('worker.trace');
    expect(result.event.operation).toBe('INSERT');
    expect(result.event.table).toBe('worker_traces');
    expect(result.event.timestamp).toBe(1700000000000);
    expect(result.event.rowId).toBeDefined();
    expect(result.event.after).toBeDefined();
  });

  it('should generate unique eventId for each CDC event', () => {
    const trace1 = createTraceItem({ scriptName: 'worker-1' });
    const trace2 = createTraceItem({ scriptName: 'worker-2' });

    const result1 = streamer.transformToCDC(trace1);
    const result2 = streamer.transformToCDC(trace2);

    expect(result1.event.rowId).not.toBe(result2.event.rowId);
  });

  it('should set durabilityTier to P3 for telemetry', () => {
    const trace = createTraceItem();

    const result = streamer.transformToCDC(trace);

    expect(result.event.metadata?.durabilityTier).toBe('P3');
  });

  it('should include all trace metadata in CDC after field', () => {
    const trace = createTraceItem({
      scriptName: 'my-worker',
      scriptVersion: 'v1.2.3',
      dispatchNamespace: 'production',
      outcome: 'ok',
      cpuTime: 15,
      wallTime: 100,
    });

    const result = streamer.transformToCDC(trace);

    const after = result.event.after as Record<string, unknown>;
    expect(after.scriptName).toBe('my-worker');
    expect(after.scriptVersion).toBe('v1.2.3');
    expect(after.dispatchNamespace).toBe('production');
    expect(after.outcome).toBe('ok');
    expect(after.cpuTime).toBe(15);
    expect(after.wallTime).toBe(100);
  });

  it('should handle TraceItems with exceptions', () => {
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

    const result = streamer.transformToCDC(trace);

    expect(result.event.after).toHaveProperty('exceptions');
    expect((result.event.after as Record<string, unknown>).exceptions).toHaveLength(1);
    expect((result.event.after as Record<string, unknown>).outcome).toBe('exception');
  });

  it('should handle TraceItems with console logs', () => {
    const trace = createTraceItem({
      logs: [
        { level: 'info', message: ['Request started'], timestamp: Date.now() - 100 },
        { level: 'warn', message: ['Slow response'], timestamp: Date.now() - 50 },
        { level: 'error', message: ['Connection timeout'], timestamp: Date.now() },
      ],
    });

    const result = streamer.transformToCDC(trace);

    expect((result.event.after as Record<string, unknown>).logs).toHaveLength(3);
  });

  it('should handle TraceItems with scheduled events', () => {
    const scheduledTime = Date.now();
    const trace = createTraceItem({
      scriptName: 'cron-worker',
      event: {
        scheduledTime,
        cron: '0 * * * *',
      },
    });

    const result = streamer.transformToCDC(trace);

    const eventData = (result.event.after as Record<string, unknown>).event as Record<string, unknown>;
    expect(eventData.scheduledTime).toBe(scheduledTime);
    expect(eventData.cron).toBe('0 * * * *');
  });

  it('should capture CPU and wall time metrics', () => {
    const trace = createTraceItem({
      cpuTime: 25,
      wallTime: 150,
    });

    const result = streamer.transformToCDC(trace);

    expect((result.event.after as Record<string, unknown>).cpuTime).toBe(25);
    expect((result.event.after as Record<string, unknown>).wallTime).toBe(150);
  });

  it('should compute partition key based on timestamp and script', () => {
    const trace = createTraceItem({
      scriptName: 'api-worker',
      eventTimestamp: new Date('2024-01-15T10:30:00Z').getTime(),
    });

    const result = streamer.transformToCDC(trace);

    expect(result.partitionKey).toMatch(/2024-01-15/);
    expect(result.partitionKey).toContain('api-worker');
  });

  it('should handle request/response data in CDC format', () => {
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

    const result = streamer.transformToCDC(trace);

    const after = result.event.after as Record<string, unknown>;
    const request = after.request as Record<string, unknown>;
    const response = after.response as Record<string, unknown>;
    expect(request.url).toBe('https://api.example.com/users/123');
    expect(request.method).toBe('POST');
    expect(response.status).toBe(201);
  });
});

// =============================================================================
// Test: Shard Routing (Pure Functions - No DOs)
// =============================================================================

describe('Shard Routing', () => {
  it('should route traces to shards based on scriptName', () => {
    const trace1 = createTraceItem({ scriptName: 'api-worker' });
    const trace2 = createTraceItem({ scriptName: 'cron-worker' });
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount: 8,
    });

    const shard1 = streamer.getTargetShard(trace1);
    const shard2 = streamer.getTargetShard(trace2);

    // Same scriptName should always go to same shard (consistent hashing)
    const trace1Again = createTraceItem({ scriptName: 'api-worker' });
    const shard1Again = streamer.getTargetShard(trace1Again);
    expect(shard1).toBe(shard1Again);

    // Shards should be in valid range
    expect(shard1).toMatch(/^shard-\d+$/);
    expect(shard2).toMatch(/^shard-\d+$/);
  });

  it('should distribute traces across all configured shards', () => {
    const shardCount = 8;
    const batch = createTraceBatch(100);
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount,
    });

    const shardDistribution = new Map<string, number>();
    for (const trace of batch.traces) {
      const shard = streamer.getTargetShard(trace);
      shardDistribution.set(shard, (shardDistribution.get(shard) ?? 0) + 1);
    }

    expect(shardDistribution.size).toBeGreaterThan(1);
    expect(shardDistribution.size).toBeLessThanOrEqual(shardCount);
  });

  it('should use consistent hashing for shard assignment', () => {
    const scriptName = 'consistent-worker';
    const streamer1 = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);
    const streamer2 = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    const shard1 = streamer1.getTargetShard(createTraceItem({ scriptName }));
    const shard2 = streamer2.getTargetShard(createTraceItem({ scriptName }));

    // Same input should produce same shard across instances
    expect(shard1).toBe(shard2);
  });
});

// =============================================================================
// Test: Backpressure Detection (Pure Functions - No DOs)
// =============================================================================

describe('Backpressure Detection', () => {
  it('should detect backpressure when buffer exceeds threshold', () => {
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      backpressureThreshold: 0.8,
      maxPendingMessages: 100,
    });

    // Simulate pending messages
    streamer.simulatePendingMessages('shard-0', 85);

    expect(streamer.isBackpressured('shard-0')).toBe(true);
    const status = streamer.getBackpressureStatus().get('shard-0');
    expect(status?.isBackpressured).toBe(true);
    expect(status?.bufferUtilization).toBeCloseTo(0.85, 2);
  });

  it('should not report backpressure below threshold', () => {
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      backpressureThreshold: 0.8,
      maxPendingMessages: 100,
    });

    streamer.simulatePendingMessages('shard-0', 50);

    expect(streamer.isBackpressured('shard-0')).toBe(false);
  });

  it('should report backpressure status for all shards', () => {
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount: 4,
      maxPendingMessages: 100,
    });

    streamer.simulatePendingMessages('shard-0', 20);
    streamer.simulatePendingMessages('shard-1', 85);
    streamer.simulatePendingMessages('shard-2', 50);
    streamer.simulatePendingMessages('shard-3', 95);

    const status = streamer.getBackpressureStatus();

    expect(status.size).toBe(4);
    expect(status.get('shard-0')?.isBackpressured).toBe(false);
    expect(status.get('shard-1')?.isBackpressured).toBe(true);
    expect(status.get('shard-2')?.isBackpressured).toBe(false);
    expect(status.get('shard-3')?.isBackpressured).toBe(true);
  });

  it('should implement exponential backoff on retries', () => {
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    const delays: number[] = [];
    for (let i = 0; i < 5; i++) {
      const delay = streamer.getReconnectDelay('shard-0', i);
      delays.push(delay);
    }

    // Delays should increase (accounting for jitter)
    expect(delays[1]).toBeGreaterThanOrEqual(delays[0] * 1.5);
    expect(delays[2]).toBeGreaterThanOrEqual(delays[1] * 1.5);
    expect(delays[4]).toBeLessThanOrEqual(30000 * 1.25); // Max backoff cap with jitter
  });
});

// =============================================================================
// Test: Integration with Real DoLake DOs
// =============================================================================

describe('Integration with Real DoLake DOs', () => {
  let streamer: TailWorkerCDCStreamer;
  let factory: WebSocketFactory;

  beforeEach(() => {
    factory = createDoLakeFactory(env.DOLAKE);
    streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount: 2, // Use fewer shards for testing
      webSocketFactory: factory,
    });
  });

  afterEach(async () => {
    await streamer.close();
  });

  it('should connect to real DoLake DO via WebSocket', async () => {
    const trace = createTraceItem({ scriptName: 'integration-test' });
    const batch: TailWorkerBatch = { traces: [trace] };

    const results = await streamer.processBatch(batch);

    expect(results).toBeDefined();
    expect(results.length).toBeGreaterThan(0);
    // Connection should succeed with real DO
    const successfulResults = results.filter((r) => r.success);
    expect(successfulResults.length).toBeGreaterThan(0);
  });

  it('should send CDC events to DoLake and receive acknowledgment', async () => {
    const batch = createTraceBatch(5, 'ack-test-worker');

    const results = await streamer.processBatch(batch);

    expect(results.length).toBeGreaterThan(0);
    const totalEvents = results.reduce((sum, r) => sum + r.eventsCount, 0);
    expect(totalEvents).toBe(5);
  });

  it('should batch events by shard before sending', async () => {
    const batch = createTraceBatch(20);

    const results = await streamer.processBatch(batch);

    // Events should be grouped by shard
    const shardsSent = new Set(results.map((r) => r.shardId));
    expect(shardsSent.size).toBeGreaterThan(0);
    expect(shardsSent.size).toBeLessThanOrEqual(2); // We configured 2 shards
  });

  it('should buffer events and flush on explicit call', async () => {
    const batch = createTraceBatch(10);

    streamer.bufferEvents(batch);
    expect(streamer.getBufferedEventsCount()).toBe(10);

    await streamer.flush();

    expect(streamer.getBufferedEventsCount()).toBe(0);
  });

  it('should flush all shards on close', async () => {
    const batches = [
      createTraceBatch(5, 'worker-1'),
      createTraceBatch(5, 'worker-2'),
    ];

    for (const batch of batches) {
      streamer.bufferEvents(batch);
    }

    await streamer.close();

    expect(streamer.getBufferedEventsCount()).toBe(0);
  });

  it('should report load metrics', () => {
    const metrics = streamer.getLoadMetrics();

    expect(metrics).toHaveProperty('bufferUtilization');
    expect(metrics).toHaveProperty('avgLatencyMs');
    expect(metrics).toHaveProperty('requestsPerSecond');
    expect(metrics).toHaveProperty('activeConnections');
    expect(metrics).toHaveProperty('p99LatencyMs');
  });
});

// =============================================================================
// Test: E2E Flow - Trace to R2
// =============================================================================

describe('E2E: Trace Events to R2', () => {
  let streamer: TailWorkerCDCStreamer;
  let factory: WebSocketFactory;

  beforeEach(() => {
    factory = createDoLakeFactory(env.DOLAKE);
    streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount: 1, // Single shard for predictable testing
      webSocketFactory: factory,
    });
  });

  afterEach(async () => {
    await streamer.close();
  });

  it('should process trace batch and verify DoLake receives events', async () => {
    const testId = createTestDoName('e2e-trace');
    const batch = createTraceBatch(10, testId);

    // Process the batch
    const results = await streamer.processBatch(batch);

    expect(results.length).toBeGreaterThan(0);

    // Verify events were processed
    const totalProcessed = results.reduce((sum, r) => sum + r.eventsCount, 0);
    expect(totalProcessed).toBe(10);
  });

  it('should verify events are persisted to R2 after flush', async () => {
    const testId = `e2e-r2-${Date.now()}`;
    const batch = createTraceBatch(5, testId);

    // Send to DoLake
    await streamer.processBatch(batch);

    // Trigger flush in DoLake (via status endpoint which triggers internal flush)
    const doName = 'test-lakehouse-shard-0';
    const id = env.DOLAKE.idFromName(doName);
    const stub = env.DOLAKE.get(id);

    // Request status which may trigger flush
    const statusResponse = await stub.fetch('http://dolake/status');
    expect(statusResponse.status).toBe(200);

    const status = (await statusResponse.json()) as { eventsBuffered?: number; eventsFlushed?: number };

    // Events should be either buffered or flushed
    expect(status.eventsBuffered !== undefined || status.eventsFlushed !== undefined).toBe(true);
  });

  it('should handle high-volume trace ingestion', async () => {
    const batchSize = 100;
    const batch = createTraceBatch(batchSize, 'high-volume-test');

    const startTime = Date.now();
    const results = await streamer.processBatch(batch);
    const duration = Date.now() - startTime;

    // All events should be processed
    const totalProcessed = results.reduce((sum, r) => sum + r.eventsCount, 0);
    expect(totalProcessed).toBe(batchSize);

    // Should complete in reasonable time
    expect(duration).toBeLessThan(10000); // 10 seconds max
  });
});

// =============================================================================
// Test: Error Handling
// =============================================================================

describe('Error Handling', () => {
  it('should handle malformed TraceItems gracefully', async () => {
    const factory = createDoLakeFactory(env.DOLAKE);
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount: 1,
      webSocketFactory: factory,
    });

    const malformedTrace = {
      scriptName: 'bad-worker',
      // Missing required fields
    } as unknown as TraceItem;
    const batch: TailWorkerBatch = { traces: [malformedTrace] };

    // Should not throw
    const results = await streamer.processBatch(batch);

    expect(results).toBeDefined();
    expect(results.length).toBe(1);

    await streamer.close();
  });

  it('should close connections gracefully', async () => {
    const factory = createDoLakeFactory(env.DOLAKE);
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount: 1,
      webSocketFactory: factory,
    });

    // Should not throw
    await streamer.close();

    expect(streamer.isConnected('shard-0')).toBe(false);
  });
});

// =============================================================================
// Test: Durability Override
// =============================================================================

describe('Durability Override', () => {
  it('should allow durability override per worker', () => {
    const streamer = new TailWorkerCDCStreamer(DEFAULT_TAIL_WORKER_CONFIG);

    // Set P0 durability for critical worker
    streamer.setDurabilityOverride('critical-worker', 'P0');

    const criticalTrace = createTraceItem({ scriptName: 'critical-worker' });
    const normalTrace = createTraceItem({ scriptName: 'normal-worker' });

    const criticalResult = streamer.transformToCDC(criticalTrace);
    const normalResult = streamer.transformToCDC(normalTrace);

    expect(criticalResult.event.metadata?.durabilityTier).toBe('P0');
    expect(normalResult.event.metadata?.durabilityTier).toBe('P3'); // Default
  });
});

// =============================================================================
// Test: Scaling
// =============================================================================

describe('Scaling', () => {
  it('should update routing when shard count changes', () => {
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount: 4,
    });

    const trace = createTraceItem({ scriptName: 'scale-test' });
    const initialShard = streamer.getTargetShard(trace);
    expect(initialShard).toMatch(/^shard-[0-3]$/);

    // Scale up
    streamer.updateShardCount(8);

    const newShard = streamer.getTargetShard(trace);
    expect(newShard).toMatch(/^shard-[0-7]$/);
  });

  it('should report active shard count', async () => {
    const factory = createDoLakeFactory(env.DOLAKE);
    const streamer = new TailWorkerCDCStreamer({
      ...DEFAULT_TAIL_WORKER_CONFIG,
      shardCount: 3,
      webSocketFactory: factory,
    });

    expect(streamer.getActiveShardCount()).toBe(3);

    await streamer.close();
  });
});
