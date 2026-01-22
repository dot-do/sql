/**
 * Analytics Events Ingestion Tests
 *
 * TDD tests for analytics events ingestion with P2 durability tier.
 * Analytics events use R2 with VFS fallback (P2 durability).
 *
 * @see do-d1isn.8 - RED: Analytics events ingestion
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { env } from 'cloudflare:test';
import {
  AnalyticsEventBuffer,
  AnalyticsEventHandler,
  inferAnalyticsSchema,
  createDatePartition,
  type AnalyticsEvent,
  type AnalyticsEventBatch,
  type AnalyticsDurabilityConfig,
  type AnalyticsSchema,
  DEFAULT_ANALYTICS_CONFIG,
  P2_DURABILITY_CONFIG,
} from '../src/analytics-events.js';

// =============================================================================
// Test Utilities
// =============================================================================

function createAnalyticsEvent(overrides: Partial<AnalyticsEvent> = {}): AnalyticsEvent {
  return {
    eventId: crypto.randomUUID(),
    eventType: 'page_view',
    timestamp: Date.now(),
    sessionId: crypto.randomUUID(),
    userId: 'user-123',
    payload: {
      page: '/home',
      referrer: 'https://google.com',
      userAgent: 'Mozilla/5.0',
    },
    ...overrides,
  };
}

function createEventBatch(count: number): AnalyticsEvent[] {
  return Array.from({ length: count }, (_, i) =>
    createAnalyticsEvent({
      eventId: `event-${i}`,
      timestamp: Date.now() - i * 1000, // Spread timestamps
    })
  );
}

// =============================================================================
// JavaScript SDK Client Batching Tests
// =============================================================================

describe('Analytics Events - Client Batching', () => {
  describe('Batch Size Threshold', () => {
    it('should batch up to 100 events before sending', () => {
      const buffer = new AnalyticsEventBuffer();

      // Add 99 events - should not trigger flush
      for (let i = 0; i < 99; i++) {
        const shouldFlush = buffer.add(createAnalyticsEvent());
        expect(shouldFlush).toBe(false);
      }

      expect(buffer.size()).toBe(99);

      // Add 100th event - should trigger flush
      const shouldFlush = buffer.add(createAnalyticsEvent());
      expect(shouldFlush).toBe(true);
      expect(buffer.size()).toBe(100);
    });

    it('should return batch when flushing', () => {
      const buffer = new AnalyticsEventBuffer();

      // Add 100 events
      for (let i = 0; i < 100; i++) {
        buffer.add(createAnalyticsEvent({ eventId: `event-${i}` }));
      }

      const batch = buffer.flush();
      expect(batch.events.length).toBe(100);
      expect(batch.events[0].eventId).toBe('event-0');
      expect(buffer.size()).toBe(0);
    });

    it('should support configurable batch size', () => {
      const buffer = new AnalyticsEventBuffer({ maxBatchSize: 50 });

      for (let i = 0; i < 49; i++) {
        expect(buffer.add(createAnalyticsEvent())).toBe(false);
      }

      expect(buffer.add(createAnalyticsEvent())).toBe(true);
      expect(buffer.size()).toBe(50);
    });
  });

  describe('Time-based Flush Threshold', () => {
    it('should flush after 5 seconds timeout', async () => {
      vi.useFakeTimers();
      const buffer = new AnalyticsEventBuffer({ flushTimeoutMs: 5000 });

      // Add fewer than 100 events
      for (let i = 0; i < 50; i++) {
        buffer.add(createAnalyticsEvent());
      }

      expect(buffer.shouldFlushByTime()).toBe(false);

      // Advance time by 5 seconds
      vi.advanceTimersByTime(5000);

      expect(buffer.shouldFlushByTime()).toBe(true);

      vi.useRealTimers();
    });

    it('should reset timeout after flush', async () => {
      vi.useFakeTimers();
      const buffer = new AnalyticsEventBuffer({ flushTimeoutMs: 5000 });

      buffer.add(createAnalyticsEvent());
      vi.advanceTimersByTime(5000);
      expect(buffer.shouldFlushByTime()).toBe(true);

      buffer.flush();
      expect(buffer.shouldFlushByTime()).toBe(false);

      vi.useRealTimers();
    });

    it('should support configurable timeout', async () => {
      vi.useFakeTimers();
      const buffer = new AnalyticsEventBuffer({ flushTimeoutMs: 10000 });

      buffer.add(createAnalyticsEvent());

      vi.advanceTimersByTime(5000);
      expect(buffer.shouldFlushByTime()).toBe(false);

      vi.advanceTimersByTime(5000);
      expect(buffer.shouldFlushByTime()).toBe(true);

      vi.useRealTimers();
    });
  });

  describe('WebSocket Connection', () => {
    it('should serialize batch for WebSocket transmission', () => {
      const buffer = new AnalyticsEventBuffer();

      for (let i = 0; i < 10; i++) {
        buffer.add(createAnalyticsEvent());
      }

      const batch = buffer.flush();
      const serialized = buffer.serializeBatch(batch);

      expect(typeof serialized).toBe('string');
      const parsed = JSON.parse(serialized);
      expect(parsed.type).toBe('analytics_batch');
      expect(parsed.events.length).toBe(10);
      expect(typeof parsed.timestamp).toBe('number');
      expect(typeof parsed.batchId).toBe('string');
    });

    it('should include batch metadata', () => {
      const buffer = new AnalyticsEventBuffer();

      for (let i = 0; i < 5; i++) {
        buffer.add(createAnalyticsEvent());
      }

      const batch = buffer.flush();
      const serialized = buffer.serializeBatch(batch);
      const parsed = JSON.parse(serialized);

      expect(parsed.sizeBytes).toBeGreaterThan(0);
      expect(parsed.firstEventTime).toBeLessThanOrEqual(parsed.lastEventTime);
    });
  });
});

// =============================================================================
// P2 Durability Tier Tests
// =============================================================================

describe('Analytics Events - P2 Durability', () => {
  describe('Durability Configuration', () => {
    it('should have correct P2 durability defaults', () => {
      expect(P2_DURABILITY_CONFIG.primaryStorage).toBe('r2');
      expect(P2_DURABILITY_CONFIG.fallbackStorage).toBe('vfs');
      expect(P2_DURABILITY_CONFIG.retryAttempts).toBeGreaterThan(0);
      expect(P2_DURABILITY_CONFIG.fallbackEnabled).toBe(true);
    });

    it('should configure analytics events with P2 durability', () => {
      const handler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);

      expect(handler.getDurabilityTier()).toBe('P2');
      expect(handler.getPrimaryStorage()).toBe('r2');
      expect(handler.getFallbackStorage()).toBe('vfs');
    });
  });

  describe('R2 Primary Storage', () => {
    it('should write batch to R2 as primary storage', async () => {
      const handler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);
      const batch = createEventBatch(50);

      const result = await handler.persistBatch(batch, {
        r2Bucket: env.LAKEHOUSE_BUCKET,
      });

      expect(result.success).toBe(true);
      expect(result.storageTier).toBe('r2');
      expect(result.path).toContain('analytics/');
      expect(result.bytesWritten).toBeGreaterThan(0);
    });

    it('should partition by date when writing to R2', async () => {
      const handler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);
      const now = new Date();
      const batch = createEventBatch(10);

      const result = await handler.persistBatch(batch, {
        r2Bucket: env.LAKEHOUSE_BUCKET,
      });

      // Path should include year/month/day
      const expectedPrefix = `analytics/year=${now.getUTCFullYear()}/month=${String(now.getUTCMonth() + 1).padStart(2, '0')}/day=${String(now.getUTCDate()).padStart(2, '0')}`;
      expect(result.path).toContain(expectedPrefix);
    });
  });

  describe('VFS Fallback Storage', () => {
    // Create a mock VFS storage with put method
    function createMockVfsStorage(): DurableObjectStorage {
      const storage = new Map<string, unknown>();
      return {
        put: async (key: string, value: unknown) => {
          storage.set(key, value);
        },
        get: async (key: string) => storage.get(key),
        delete: async (key: string) => storage.delete(key),
        list: async () => new Map(),
        getAlarm: async () => null,
        setAlarm: async () => {},
        deleteAlarm: async () => {},
        sync: async () => {},
        transaction: async (closure: () => Promise<void>) => closure(),
        transactionSync: (closure: () => void) => closure(),
      } as unknown as DurableObjectStorage;
    }

    it('should fallback to VFS when R2 fails', async () => {
      const handler = new AnalyticsEventHandler({
        ...P2_DURABILITY_CONFIG,
        simulateR2Failure: true, // For testing
      });
      const batch = createEventBatch(50);

      const result = await handler.persistBatch(batch, {
        r2Bucket: env.LAKEHOUSE_BUCKET,
        vfsStorage: createMockVfsStorage(),
      });

      expect(result.success).toBe(true);
      expect(result.storageTier).toBe('vfs');
      expect(result.usedFallback).toBe(true);
    });

    it('should retry before falling back', async () => {
      const handler = new AnalyticsEventHandler({
        ...P2_DURABILITY_CONFIG,
        retryAttempts: 3,
        simulateR2Failure: true,
      });

      const batch = createEventBatch(10);
      const result = await handler.persistBatch(batch, {
        r2Bucket: env.LAKEHOUSE_BUCKET,
        vfsStorage: createMockVfsStorage(),
      });

      expect(result.retryCount).toBe(3);
      expect(result.usedFallback).toBe(true);
    });

    it('should recover VFS data to R2 when available', async () => {
      const handler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);

      // Simulate data in VFS fallback
      const vfsData = {
        batchId: 'batch-123',
        events: createEventBatch(20),
        storedAt: Date.now(),
      };

      const result = await handler.recoverFromFallback(vfsData, {
        r2Bucket: env.LAKEHOUSE_BUCKET,
      });

      expect(result.success).toBe(true);
      expect(result.recoveredEvents).toBe(20);
      expect(result.newStorageTier).toBe('r2');
    });
  });
});

// =============================================================================
// Schema Inference Tests
// =============================================================================

describe('Analytics Events - Schema Inference', () => {
  describe('Basic Type Inference', () => {
    it('should infer string fields', () => {
      const events = [
        createAnalyticsEvent({
          payload: { name: 'John', email: 'john@example.com' },
        }),
      ];

      const schema = inferAnalyticsSchema(events);

      expect(schema.fields.find((f) => f.name === 'payload.name')?.type).toBe('string');
      expect(schema.fields.find((f) => f.name === 'payload.email')?.type).toBe('string');
    });

    it('should infer numeric fields', () => {
      const events = [
        createAnalyticsEvent({
          payload: { count: 42, price: 19.99, timestamp: Date.now() },
        }),
      ];

      const schema = inferAnalyticsSchema(events);

      expect(schema.fields.find((f) => f.name === 'payload.count')?.type).toBe('long');
      expect(schema.fields.find((f) => f.name === 'payload.price')?.type).toBe('double');
    });

    it('should infer boolean fields', () => {
      const events = [
        createAnalyticsEvent({
          payload: { isActive: true, hasSubscription: false },
        }),
      ];

      const schema = inferAnalyticsSchema(events);

      expect(schema.fields.find((f) => f.name === 'payload.isActive')?.type).toBe('boolean');
      expect(schema.fields.find((f) => f.name === 'payload.hasSubscription')?.type).toBe('boolean');
    });

    it('should infer timestamp fields from ISO strings', () => {
      const events = [
        createAnalyticsEvent({
          payload: { createdAt: '2024-01-15T10:30:00Z' },
        }),
      ];

      const schema = inferAnalyticsSchema(events);

      expect(schema.fields.find((f) => f.name === 'payload.createdAt')?.type).toBe('timestamptz');
    });
  });

  describe('Nested Object Inference', () => {
    it('should flatten nested objects into dot-notation fields', () => {
      const events = [
        createAnalyticsEvent({
          payload: {
            user: {
              profile: {
                name: 'John',
                age: 30,
              },
            },
          },
        }),
      ];

      const schema = inferAnalyticsSchema(events);

      expect(schema.fields.find((f) => f.name === 'payload.user.profile.name')?.type).toBe('string');
      expect(schema.fields.find((f) => f.name === 'payload.user.profile.age')?.type).toBe('long');
    });

    it('should handle arrays as JSON type', () => {
      const events = [
        createAnalyticsEvent({
          payload: { tags: ['a', 'b', 'c'], items: [1, 2, 3] },
        }),
      ];

      const schema = inferAnalyticsSchema(events);

      expect(schema.fields.find((f) => f.name === 'payload.tags')?.type).toBe('string'); // JSON serialized
      expect(schema.fields.find((f) => f.name === 'payload.items')?.type).toBe('string'); // JSON serialized
    });
  });

  describe('Schema Evolution', () => {
    it('should merge schemas from multiple events', () => {
      const events = [
        createAnalyticsEvent({ payload: { fieldA: 'value1' } }),
        createAnalyticsEvent({ payload: { fieldB: 42 } }),
        createAnalyticsEvent({ payload: { fieldA: 'value2', fieldC: true } }),
      ];

      const schema = inferAnalyticsSchema(events);

      expect(schema.fields.find((f) => f.name === 'payload.fieldA')).toBeDefined();
      expect(schema.fields.find((f) => f.name === 'payload.fieldB')).toBeDefined();
      expect(schema.fields.find((f) => f.name === 'payload.fieldC')).toBeDefined();
    });

    it('should handle nullable fields', () => {
      const events = [
        createAnalyticsEvent({ payload: { optional: 'present' } }),
        createAnalyticsEvent({ payload: { optional: null } }),
      ];

      const schema = inferAnalyticsSchema(events);

      const optionalField = schema.fields.find((f) => f.name === 'payload.optional');
      expect(optionalField?.required).toBe(false);
    });

    it('should always include base analytics fields', () => {
      const events = [createAnalyticsEvent()];
      const schema = inferAnalyticsSchema(events);

      expect(schema.fields.find((f) => f.name === 'eventId')).toBeDefined();
      expect(schema.fields.find((f) => f.name === 'eventType')).toBeDefined();
      expect(schema.fields.find((f) => f.name === 'timestamp')).toBeDefined();
      expect(schema.fields.find((f) => f.name === 'sessionId')).toBeDefined();
      expect(schema.fields.find((f) => f.name === 'userId')).toBeDefined();
    });
  });
});

// =============================================================================
// Date-based Partitioning Tests
// =============================================================================

describe('Analytics Events - Date Partitioning', () => {
  describe('Partition Key Generation', () => {
    it('should create partition key from event timestamp', () => {
      const event = createAnalyticsEvent({
        timestamp: new Date('2024-03-15T14:30:00Z').getTime(),
      });

      const partition = createDatePartition(event);

      expect(partition.year).toBe(2024);
      expect(partition.month).toBe(3);
      expect(partition.day).toBe(15);
    });

    it('should generate partition path', () => {
      const event = createAnalyticsEvent({
        timestamp: new Date('2024-12-25T00:00:00Z').getTime(),
      });

      const partition = createDatePartition(event);

      expect(partition.path).toBe('year=2024/month=12/day=25');
    });

    it('should handle events from different dates', () => {
      const events = [
        createAnalyticsEvent({ timestamp: new Date('2024-01-01').getTime() }),
        createAnalyticsEvent({ timestamp: new Date('2024-01-02').getTime() }),
        createAnalyticsEvent({ timestamp: new Date('2024-02-01').getTime() }),
      ];

      const partitions = events.map((e) => createDatePartition(e));

      expect(new Set(partitions.map((p) => p.path)).size).toBe(3);
    });
  });

  describe('Batch Partitioning', () => {
    it('should group events by partition', () => {
      const handler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);
      const events = [
        createAnalyticsEvent({ timestamp: new Date('2024-01-01T10:00:00Z').getTime() }),
        createAnalyticsEvent({ timestamp: new Date('2024-01-01T14:00:00Z').getTime() }),
        createAnalyticsEvent({ timestamp: new Date('2024-01-02T10:00:00Z').getTime() }),
      ];

      const partitioned = handler.partitionEvents(events);

      expect(partitioned.size).toBe(2);
      expect(partitioned.get('year=2024/month=01/day=01')?.length).toBe(2);
      expect(partitioned.get('year=2024/month=01/day=02')?.length).toBe(1);
    });

    it('should write separate files per partition', async () => {
      const handler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);
      const events = [
        createAnalyticsEvent({ timestamp: new Date('2024-01-01').getTime() }),
        createAnalyticsEvent({ timestamp: new Date('2024-01-02').getTime() }),
      ];

      const result = await handler.persistBatch(events, {
        r2Bucket: env.LAKEHOUSE_BUCKET,
      });

      expect(result.filesWritten).toBe(2);
      expect(result.partitions.length).toBe(2);
    });
  });

  describe('Partition Pruning', () => {
    it('should support date range queries for partition pruning', () => {
      const handler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);

      // Register partitions
      handler.registerPartition('year=2024/month=01/day=01');
      handler.registerPartition('year=2024/month=01/day=02');
      handler.registerPartition('year=2024/month=01/day=03');
      handler.registerPartition('year=2024/month=02/day=01');

      const relevantPartitions = handler.getPartitionsForRange(
        new Date('2024-01-01'),
        new Date('2024-01-02')
      );

      expect(relevantPartitions.length).toBe(2);
      expect(relevantPartitions).toContain('year=2024/month=01/day=01');
      expect(relevantPartitions).toContain('year=2024/month=01/day=02');
    });
  });
});

// =============================================================================
// Server Handler Integration Tests
// =============================================================================

describe('Analytics Events - Server Handler', () => {
  describe('WebSocket Message Handling', () => {
    it('should accept analytics_batch message type', async () => {
      const id = env.DOLAKE.idFromName('test-analytics-' + Date.now());
      const stub = env.DOLAKE.get(id);

      // Connect via WebSocket
      const upgradeRequest = new Request('http://dolake/analytics', {
        headers: {
          'Upgrade': 'websocket',
          'X-Client-ID': 'analytics-sdk-test',
        },
      });

      const response = await stub.fetch(upgradeRequest);
      expect(response.status).toBe(101);
    });

    it('should acknowledge successful batch ingestion', async () => {
      const handler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);
      const batch: AnalyticsEventBatch = {
        type: 'analytics_batch',
        batchId: 'batch-123',
        events: createEventBatch(50),
        timestamp: Date.now(),
        sizeBytes: 5000,
        firstEventTime: Date.now() - 1000,
        lastEventTime: Date.now(),
      };

      const result = await handler.handleBatch(batch, {
        r2Bucket: env.LAKEHOUSE_BUCKET,
      });

      expect(result.type).toBe('analytics_ack');
      expect(result.batchId).toBe('batch-123');
      expect(result.success).toBe(true);
      expect(result.eventsProcessed).toBe(50);
    });

    it('should return error for invalid batch', async () => {
      const handler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);
      const invalidBatch = {
        type: 'analytics_batch',
        // Missing required fields
      };

      const result = await handler.handleBatch(invalidBatch as AnalyticsEventBatch, {
        r2Bucket: env.LAKEHOUSE_BUCKET,
      });

      expect(result.type).toBe('analytics_nack');
      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });
  });

  describe('Metrics and Status', () => {
    it('should track ingestion metrics', async () => {
      const handler = new AnalyticsEventHandler({
        ...P2_DURABILITY_CONFIG,
        deduplicationEnabled: false, // Disable dedup for this test
      });

      // Ingest some batches - each with unique event IDs
      for (let i = 0; i < 3; i++) {
        // Create events with unique IDs per batch
        const events = Array.from({ length: 100 }, (_, j) =>
          createAnalyticsEvent({
            eventId: `batch-${i}-event-${j}`,
            timestamp: Date.now() - j * 1000,
          })
        );

        await handler.handleBatch(
          {
            type: 'analytics_batch',
            batchId: `batch-${i}`,
            events,
            timestamp: Date.now(),
            sizeBytes: 10000,
            firstEventTime: Date.now() - 1000,
            lastEventTime: Date.now(),
          },
          { r2Bucket: env.LAKEHOUSE_BUCKET }
        );
      }

      const metrics = handler.getMetrics();

      expect(metrics.batchesReceived).toBe(3);
      expect(metrics.eventsReceived).toBe(300);
      expect(metrics.bytesReceived).toBeGreaterThan(0);
    });

    it('should expose analytics-specific endpoints', async () => {
      const id = env.DOLAKE.idFromName('test-analytics-status-' + Date.now());
      const stub = env.DOLAKE.get(id);

      const response = await stub.fetch('http://dolake/analytics/status');
      expect(response.status).toBe(200);

      const status = (await response.json()) as {
        durabilityTier: string;
        eventsBuffered: number;
      };
      expect(status.durabilityTier).toBe('P2');
      expect(typeof status.eventsBuffered).toBe('number');
    });
  });
});

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

describe('Analytics Events - Edge Cases', () => {
  it('should handle empty batch gracefully', async () => {
    const handler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);
    const batch: AnalyticsEventBatch = {
      type: 'analytics_batch',
      batchId: 'empty-batch',
      events: [],
      timestamp: Date.now(),
      sizeBytes: 0,
      firstEventTime: 0,
      lastEventTime: 0,
    };

    const result = await handler.handleBatch(batch, {
      r2Bucket: env.LAKEHOUSE_BUCKET,
    });

    expect(result.success).toBe(true);
    expect(result.eventsProcessed).toBe(0);
  });

  it('should handle very large payloads', async () => {
    const handler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);
    const largePayload = 'x'.repeat(100_000); // 100KB payload

    const events = [
      createAnalyticsEvent({
        payload: { data: largePayload },
      }),
    ];

    const result = await handler.persistBatch(events, {
      r2Bucket: env.LAKEHOUSE_BUCKET,
    });

    expect(result.success).toBe(true);
  });

  it('should handle malformed event payloads', async () => {
    const handler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);
    const events = [
      {
        eventId: 'test-1',
        eventType: 'test',
        timestamp: Date.now(),
        sessionId: 'session-1',
        userId: 'user-1',
        payload: { circular: {} as Record<string, unknown> },
      },
    ];

    // Create circular reference
    (events[0].payload.circular as Record<string, unknown>).self = events[0].payload;

    // Should handle circular references gracefully
    const result = await handler.persistBatch(events as AnalyticsEvent[], {
      r2Bucket: env.LAKEHOUSE_BUCKET,
    });

    // Should either succeed with sanitized data or fail gracefully
    expect(result).toBeDefined();
  });

  it('should deduplicate events by eventId', async () => {
    const handler = new AnalyticsEventHandler({
      ...P2_DURABILITY_CONFIG,
      deduplicationEnabled: true,
    });

    const duplicateId = 'duplicate-event-id';
    const events = [
      createAnalyticsEvent({ eventId: duplicateId }),
      createAnalyticsEvent({ eventId: duplicateId }), // Duplicate
      createAnalyticsEvent({ eventId: 'unique-event' }),
    ];

    const result = await handler.handleBatch(
      {
        type: 'analytics_batch',
        batchId: 'batch-dedup',
        events,
        timestamp: Date.now(),
        sizeBytes: 1000,
        firstEventTime: Date.now(),
        lastEventTime: Date.now(),
      },
      { r2Bucket: env.LAKEHOUSE_BUCKET }
    );

    expect(result.eventsProcessed).toBe(2); // Only unique events
    expect(result.duplicatesSkipped).toBe(1);
  });
});
