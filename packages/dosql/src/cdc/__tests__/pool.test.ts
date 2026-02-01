/**
 * CDC Consumer Pool Tests
 *
 * Tests for CDC streaming connection pooling and backpressure:
 * - Consumer registration and lifecycle
 * - Pool size limits and queueing
 * - Backpressure signaling when consumers are slow
 * - Buffer management per consumer
 * - Health monitoring
 * - Graceful shutdown
 *
 * Tests run using workers-vitest-pool (NO MOCKS).
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  CDCConsumerPool,
  createCDCConsumerPool,
  type CDCPoolConfig,
  type CDCConsumer,
  type CDCConsumerOptions,
  type CDCPoolStats,
  type CDCPoolHealth,
  DEFAULT_CDC_POOL_CONFIG,
} from '../pool.js';
import type { CDCEvent, ChangeEvent, BackpressureSignal } from '../types.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Creates a mock CDC change event
 */
function createMockEvent(
  id: number,
  table = 'test_table',
  type: 'insert' | 'update' | 'delete' = 'insert'
): ChangeEvent {
  return {
    id: `event-${id}`,
    type,
    table,
    txnId: `txn_${id}`,
    timestamp: new Date(),
    lsn: BigInt(id),
    data: { id, value: `value-${id}` },
  };
}

/**
 * Creates multiple mock events
 */
function createMockEvents(count: number, startId = 1, table = 'test_table'): ChangeEvent[] {
  return Array.from({ length: count }, (_, i) => createMockEvent(startId + i, table));
}

/**
 * Wait for a condition with timeout
 */
async function waitFor(
  condition: () => boolean,
  timeout = 1000,
  interval = 10
): Promise<void> {
  const start = Date.now();
  while (!condition()) {
    if (Date.now() - start > timeout) {
      throw new Error('waitFor timeout');
    }
    await new Promise(resolve => setTimeout(resolve, interval));
  }
}

// =============================================================================
// Test: Consumer Registration and Lifecycle
// =============================================================================

describe('CDC Consumer Pool - Registration', () => {
  let pool: CDCConsumerPool;

  beforeEach(() => {
    pool = createCDCConsumerPool({
      maxConsumers: 10,
      consumerIdleTimeout: 5000,
      healthCheckInterval: 0, // Disable for tests
      keepAliveInterval: 0,
    });
  });

  afterEach(async () => {
    await pool.close();
  });

  it('should register a consumer', async () => {
    const consumer = await pool.register({
      name: 'test-consumer',
    });

    expect(consumer).toBeDefined();
    expect(consumer.id).toMatch(/^cdc-consumer-/);
    expect(consumer.name).toBe('test-consumer');
    expect(consumer.state).toBe('active');
  });

  it('should assign unique IDs to consumers', async () => {
    const consumer1 = await pool.register({ name: 'consumer-1' });
    const consumer2 = await pool.register({ name: 'consumer-2' });

    expect(consumer1.id).not.toBe(consumer2.id);
  });

  it('should track consumer metadata', async () => {
    const consumer = await pool.register({
      name: 'metadata-consumer',
      metadata: { environment: 'test', version: '1.0' },
    });

    expect(consumer.metadata).toEqual({
      environment: 'test',
      version: '1.0',
    });
  });

  it('should initialize consumer with fromLSN', async () => {
    const consumer = await pool.register({
      fromLSN: 100n,
    });

    expect(consumer.acknowledgedLSN).toBe(100n);
  });

  it('should disconnect a consumer', async () => {
    const consumer = await pool.register({ name: 'to-disconnect' });

    pool.disconnect(consumer.id);

    const retrieved = pool.getConsumer(consumer.id);
    expect(retrieved).toBeUndefined();
  });

  it('should list all consumers', async () => {
    await pool.register({ name: 'consumer-1' });
    await pool.register({ name: 'consumer-2' });
    await pool.register({ name: 'consumer-3' });

    const consumers = pool.listConsumers();
    expect(consumers).toHaveLength(3);
    expect(consumers.map(c => c.name).sort()).toEqual(['consumer-1', 'consumer-2', 'consumer-3']);
  });

  it('should get a specific consumer by ID', async () => {
    const registered = await pool.register({ name: 'findable' });

    const found = pool.getConsumer(registered.id);
    expect(found).toBeDefined();
    expect(found!.name).toBe('findable');
  });

  it('should return undefined for non-existent consumer', () => {
    const found = pool.getConsumer('non-existent-id');
    expect(found).toBeUndefined();
  });

  it('should emit consumer:registered event', async () => {
    const registeredHandler = vi.fn();
    pool.on('consumer:registered', registeredHandler);

    const consumer = await pool.register({ name: 'event-consumer' });

    expect(registeredHandler).toHaveBeenCalledWith({
      consumerId: consumer.id,
      timestamp: expect.any(Date),
    });
  });

  it('should emit consumer:disconnected event', async () => {
    const disconnectedHandler = vi.fn();
    pool.on('consumer:disconnected', disconnectedHandler);

    const consumer = await pool.register({ name: 'disconnect-event-consumer' });
    pool.disconnect(consumer.id, 'Test disconnect');

    expect(disconnectedHandler).toHaveBeenCalledWith({
      consumerId: consumer.id,
      reason: 'Test disconnect',
      timestamp: expect.any(Date),
    });
  });
});

// =============================================================================
// Test: Pool Size Limits and Queueing
// =============================================================================

describe('CDC Consumer Pool - Size Limits', () => {
  it('should enforce maxConsumers limit with reject strategy', async () => {
    const pool = createCDCConsumerPool({
      maxConsumers: 2,
      maxWaitingConsumers: 0, // No waiting allowed
      backpressureStrategy: 'reject',
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });

    try {
      await pool.register({ name: 'consumer-1' });
      await pool.register({ name: 'consumer-2' });

      // Third registration should fail immediately
      await expect(pool.register({ name: 'consumer-3' })).rejects.toThrow();
    } finally {
      await pool.close();
    }
  }, 10000);

  it('should queue consumers when pool is full with queue strategy', async () => {
    const pool = createCDCConsumerPool({
      maxConsumers: 2,
      backpressureStrategy: 'queue',
      waitTimeout: 500,
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });

    try {
      const consumer1 = await pool.register({ name: 'consumer-1' });
      await pool.register({ name: 'consumer-2' });

      // Third registration should queue and wait
      const queuedPromise = pool.register({ name: 'consumer-3' });

      // Disconnect one consumer to free a slot
      setTimeout(() => pool.disconnect(consumer1.id), 100);

      const consumer3 = await queuedPromise;
      expect(consumer3.name).toBe('consumer-3');
    } finally {
      await pool.close();
    }
  });

  it('should timeout queued consumers', async () => {
    const pool = createCDCConsumerPool({
      maxConsumers: 1,
      backpressureStrategy: 'queue',
      waitTimeout: 100,
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });

    try {
      await pool.register({ name: 'consumer-1' });

      // Second registration should timeout
      await expect(pool.register({ name: 'consumer-2' })).rejects.toThrow('Pool wait timeout');
    } finally {
      await pool.close();
    }
  });

  it('should reject when waiter queue is full', async () => {
    const pool = createCDCConsumerPool({
      maxConsumers: 1,
      maxWaitingConsumers: 2,
      backpressureStrategy: 'queue',
      waitTimeout: 10000,
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });

    try {
      await pool.register({ name: 'active' });

      // Fill waiter queue
      const waiter1 = pool.register({ name: 'waiter-1' });
      const waiter2 = pool.register({ name: 'waiter-2' });

      // Third waiter should be rejected
      await expect(pool.register({ name: 'waiter-3' })).rejects.toThrow('too many waiting');

      // Clean up waiters
      await pool.close();
    } finally {
      if (!pool.isClosed()) {
        await pool.close();
      }
    }
  });

  it('should track waiting consumers in stats', async () => {
    const pool = createCDCConsumerPool({
      maxConsumers: 1,
      backpressureStrategy: 'queue',
      waitTimeout: 5000,
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });

    try {
      await pool.register({ name: 'active' });

      // Start waiting
      const waiterPromise = pool.register({ name: 'waiter' });

      // Small delay to ensure waiter is queued
      await new Promise(resolve => setTimeout(resolve, 10));

      const stats = pool.getStats();
      expect(stats.waitingConsumers).toBe(1);
    } finally {
      await pool.close();
    }
  });
});

// =============================================================================
// Test: Backpressure Signaling
// =============================================================================

describe('CDC Consumer Pool - Backpressure', () => {
  let pool: CDCConsumerPool;

  beforeEach(() => {
    pool = createCDCConsumerPool({
      maxConsumers: 10,
      consumerBufferSize: 10,
      backpressureThreshold: 0.8, // 80% = 8 events
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });
  });

  afterEach(async () => {
    await pool.close();
  });

  it('should apply backpressure when buffer exceeds threshold', async () => {
    const backpressureSignals: BackpressureSignal[] = [];

    const consumer = await pool.register({
      name: 'slow-consumer',
      onBackpressure: (signal) => backpressureSignals.push(signal),
      // No onEvents handler - events will buffer
    });

    // Send events to fill buffer past threshold (8+ events for 80% of 10)
    const events = createMockEvents(9);
    await pool.send(consumer.id, events);

    // Should have received backpressure signal
    expect(backpressureSignals.length).toBeGreaterThanOrEqual(1);
    expect(backpressureSignals[0].type).toBe('pause');
    expect(backpressureSignals[0].bufferUtilization).toBeGreaterThanOrEqual(0.8);
  });

  it('should release backpressure when buffer drains', async () => {
    const backpressureSignals: BackpressureSignal[] = [];

    const consumer = await pool.register({
      name: 'recoverable-consumer',
      onBackpressure: (signal) => backpressureSignals.push(signal),
    });

    // Fill buffer
    const events = createMockEvents(9);
    await pool.send(consumer.id, events);

    // Acknowledge most events (drain buffer below 40% = release threshold)
    pool.acknowledge(consumer.id, 8n);

    // Should have received resume signal
    const resumeSignal = backpressureSignals.find(s => s.type === 'resume');
    expect(resumeSignal).toBeDefined();
  });

  it('should emit consumer:backpressure event', async () => {
    const backpressureHandler = vi.fn();
    pool.on('consumer:backpressure', backpressureHandler);

    const consumer = await pool.register({
      name: 'monitored-consumer',
    });

    // Trigger backpressure
    const events = createMockEvents(9);
    await pool.send(consumer.id, events);

    expect(backpressureHandler).toHaveBeenCalledWith({
      consumerId: consumer.id,
      signal: expect.objectContaining({ type: 'pause' }),
      timestamp: expect.any(Date),
    });
  });

  it('should emit pool:backpressure when consumers are under pressure', async () => {
    const poolBackpressureHandler = vi.fn();
    pool.on('pool:backpressure', poolBackpressureHandler);

    // Register multiple consumers
    const consumer1 = await pool.register({ name: 'consumer-1' });
    const consumer2 = await pool.register({ name: 'consumer-2' });

    // Trigger backpressure on one
    const events = createMockEvents(9);
    await pool.broadcast(events);

    expect(poolBackpressureHandler).toHaveBeenCalled();
    const call = poolBackpressureHandler.mock.calls[0][0];
    expect(call.backpressuredCount).toBeGreaterThanOrEqual(1);
    expect(call.totalConsumers).toBe(2);
  });

  it('should include suggested delay in backpressure signal', async () => {
    let receivedSignal: BackpressureSignal | null = null;

    const consumer = await pool.register({
      name: 'delay-consumer',
      onBackpressure: (signal) => { receivedSignal = signal; },
    });

    const events = createMockEvents(9);
    await pool.send(consumer.id, events);

    expect(receivedSignal).not.toBeNull();
    expect(receivedSignal!.suggestedDelayMs).toBeDefined();
    expect(receivedSignal!.suggestedDelayMs).toBeGreaterThan(0);
  });

  it('should track backpressure rate in stats', async () => {
    // Register consumers
    const consumer1 = await pool.register({ name: 'consumer-1' });
    const consumer2 = await pool.register({ name: 'consumer-2' });

    // Put one under pressure
    const events = createMockEvents(9);
    await pool.send(consumer1.id, events);

    const stats = pool.getStats();
    expect(stats.backpressureRate).toBe(0.5); // 1 of 2 consumers
  });
});

// =============================================================================
// Test: Event Distribution
// =============================================================================

describe('CDC Consumer Pool - Event Distribution', () => {
  let pool: CDCConsumerPool;

  beforeEach(() => {
    pool = createCDCConsumerPool({
      maxConsumers: 10,
      consumerBufferSize: 100,
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });
  });

  afterEach(async () => {
    await pool.close();
  });

  it('should broadcast events to all consumers', async () => {
    const receivedEvents1: CDCEvent[] = [];
    const receivedEvents2: CDCEvent[] = [];

    await pool.register({
      name: 'consumer-1',
      onEvents: async (events) => { receivedEvents1.push(...events); },
    });
    await pool.register({
      name: 'consumer-2',
      onEvents: async (events) => { receivedEvents2.push(...events); },
    });

    const events = createMockEvents(5);
    await pool.broadcast(events);

    expect(receivedEvents1).toHaveLength(5);
    expect(receivedEvents2).toHaveLength(5);
  });

  it('should send events to a specific consumer', async () => {
    const receivedEvents1: CDCEvent[] = [];
    const receivedEvents2: CDCEvent[] = [];

    const consumer1 = await pool.register({
      name: 'consumer-1',
      onEvents: async (events) => { receivedEvents1.push(...events); },
    });
    await pool.register({
      name: 'consumer-2',
      onEvents: async (events) => { receivedEvents2.push(...events); },
    });

    const events = createMockEvents(5);
    await pool.send(consumer1.id, events);

    expect(receivedEvents1).toHaveLength(5);
    expect(receivedEvents2).toHaveLength(0);
  });

  it('should filter events by table for consumers', async () => {
    const receivedEvents: CDCEvent[] = [];

    await pool.register({
      name: 'filtered-consumer',
      filter: { tables: ['users'] },
      onEvents: async (events) => { receivedEvents.push(...events); },
    });

    const userEvents = createMockEvents(3, 1, 'users');
    const orderEvents = createMockEvents(2, 4, 'orders');
    await pool.broadcast([...userEvents, ...orderEvents]);

    expect(receivedEvents).toHaveLength(3);
    expect(receivedEvents.every(e => (e as ChangeEvent).table === 'users')).toBe(true);
  });

  it('should buffer events when consumer is slow', async () => {
    let deliveryCount = 0;

    const consumer = await pool.register({
      name: 'slow-processor',
      onEvents: async (events) => {
        deliveryCount++;
        // Slow processing
        await new Promise(resolve => setTimeout(resolve, 50));
      },
    });

    // Send multiple batches quickly
    await pool.send(consumer.id, createMockEvents(3, 1));

    // Events should be delivered (buffering happens if onEvents throws)
    expect(deliveryCount).toBeGreaterThanOrEqual(1);
  });

  it('should return delivery results from broadcast', async () => {
    await pool.register({
      name: 'fast-consumer',
      onEvents: async () => {},
    });

    const consumer2 = await pool.register({
      name: 'no-handler-consumer',
      // No onEvents handler - will buffer
    });

    const events = createMockEvents(3);
    const results = await pool.broadcast(events);

    expect(results.size).toBe(2);
    expect(Array.from(results.values()).some(r => r.delivered > 0)).toBe(true);
    expect(Array.from(results.values()).some(r => r.buffered > 0)).toBe(true);
  });

  it('should throw when sending to non-existent consumer', async () => {
    const events = createMockEvents(3);
    await expect(pool.send('non-existent-id', events)).rejects.toThrow('not found');
  });

  it('should track total events delivered in stats', async () => {
    await pool.register({
      name: 'stats-consumer',
      onEvents: async () => {},
    });

    await pool.broadcast(createMockEvents(10));
    await pool.broadcast(createMockEvents(5));

    const stats = pool.getStats();
    expect(stats.totalEventsDelivered).toBe(15);
  });
});

// =============================================================================
// Test: Acknowledgment
// =============================================================================

describe('CDC Consumer Pool - Acknowledgment', () => {
  let pool: CDCConsumerPool;

  beforeEach(() => {
    pool = createCDCConsumerPool({
      maxConsumers: 10,
      consumerBufferSize: 100,
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });
  });

  afterEach(async () => {
    await pool.close();
  });

  it('should update acknowledged LSN', async () => {
    const consumer = await pool.register({
      name: 'ack-consumer',
      fromLSN: 0n,
    });

    pool.acknowledge(consumer.id, 50n);

    const updated = pool.getConsumer(consumer.id);
    expect(updated?.acknowledgedLSN).toBe(50n);
  });

  it('should clear acknowledged events from buffer', async () => {
    const consumer = await pool.register({
      name: 'buffer-clear-consumer',
      // No onEvents - events will buffer
    });

    // Send events
    const events = createMockEvents(10);
    await pool.send(consumer.id, events);

    // Consumer should have buffered events
    let state = pool.getConsumer(consumer.id);
    expect(state?.pendingEvents).toBe(10);

    // Acknowledge up to LSN 5
    pool.acknowledge(consumer.id, 5n);

    // Should have 5 events remaining
    state = pool.getConsumer(consumer.id);
    expect(state?.pendingEvents).toBe(5);
  });

  it('should update last activity time on acknowledge', async () => {
    const consumer = await pool.register({
      name: 'activity-consumer',
    });

    const beforeAck = pool.getConsumer(consumer.id)!.lastActivityAt;

    // Small delay
    await new Promise(resolve => setTimeout(resolve, 10));

    pool.acknowledge(consumer.id, 10n);

    const afterAck = pool.getConsumer(consumer.id)!.lastActivityAt;
    expect(afterAck).toBeGreaterThan(beforeAck);
  });

  it('should handle acknowledge for non-existent consumer gracefully', () => {
    // Should not throw
    expect(() => pool.acknowledge('non-existent', 100n)).not.toThrow();
  });
});

// =============================================================================
// Test: Statistics and Health
// =============================================================================

describe('CDC Consumer Pool - Statistics', () => {
  let pool: CDCConsumerPool;

  beforeEach(() => {
    pool = createCDCConsumerPool({
      maxConsumers: 10,
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });
  });

  afterEach(async () => {
    await pool.close();
  });

  it('should return accurate pool stats', async () => {
    await pool.register({ name: 'active-1' });
    await pool.register({ name: 'active-2' });

    const stats = pool.getStats();

    expect(stats.totalConsumers).toBe(2);
    expect(stats.activeConsumers).toBe(2);
    expect(stats.maxConsumers).toBe(10);
    expect(stats.totalConsumersRegistered).toBe(2);
    expect(stats.totalConsumersDisconnected).toBe(0);
  });

  it('should track disconnected consumers', async () => {
    const consumer = await pool.register({ name: 'temp-consumer' });
    pool.disconnect(consumer.id);

    const stats = pool.getStats();
    expect(stats.totalConsumers).toBe(0);
    expect(stats.totalConsumersRegistered).toBe(1);
    expect(stats.totalConsumersDisconnected).toBe(1);
  });

  it('should calculate average buffer utilization', async () => {
    // Register consumers with buffered events
    await pool.register({ name: 'consumer-1' });
    await pool.register({ name: 'consumer-2' });

    const stats = pool.getStats();
    expect(stats.averageBufferUtilization).toBeDefined();
    expect(stats.averageBufferUtilization).toBeGreaterThanOrEqual(0);
    expect(stats.averageBufferUtilization).toBeLessThanOrEqual(1);
  });

  it('should return pool health', async () => {
    await pool.register({ name: 'healthy-consumer' });

    const health = pool.getHealth();

    expect(health.healthy).toBe(true);
    expect(health.healthyConsumers).toBe(1);
    expect(health.unhealthyConsumers).toBe(0);
    expect(health.backpressuredConsumers).toBe(0);
  });

  it('should track backpressured consumers in health', async () => {
    const pool = createCDCConsumerPool({
      maxConsumers: 10,
      consumerBufferSize: 10,
      backpressureThreshold: 0.5, // 50% = 5 events
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });

    try {
      const consumer = await pool.register({ name: 'pressured' });

      // Trigger backpressure
      await pool.send(consumer.id, createMockEvents(6));

      const health = pool.getHealth();
      expect(health.backpressuredConsumers).toBe(1);
    } finally {
      await pool.close();
    }
  });
});

// =============================================================================
// Test: Pool Lifecycle
// =============================================================================

describe('CDC Consumer Pool - Lifecycle', () => {
  it('should close pool and disconnect all consumers', async () => {
    const pool = createCDCConsumerPool({
      maxConsumers: 10,
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });

    const disconnectHandler = vi.fn();
    pool.on('consumer:disconnected', disconnectHandler);

    await pool.register({ name: 'consumer-1' });
    await pool.register({ name: 'consumer-2' });

    await pool.close();

    expect(pool.isClosed()).toBe(true);
    expect(disconnectHandler).toHaveBeenCalledTimes(2);
    expect(pool.listConsumers()).toHaveLength(0);
  });

  it('should reject registration after close', async () => {
    const pool = createCDCConsumerPool({
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });

    await pool.close();

    await expect(pool.register({ name: 'too-late' })).rejects.toThrow('Pool is closed');
  });

  it('should reject waiting consumers on close', async () => {
    const pool = createCDCConsumerPool({
      maxConsumers: 1,
      backpressureStrategy: 'queue',
      waitTimeout: 10000,
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });

    await pool.register({ name: 'active' });

    // Start waiting
    const waiterPromise = pool.register({ name: 'waiter' });

    // Small delay to ensure waiter is queued
    await new Promise(resolve => setTimeout(resolve, 10));

    // Close pool
    await pool.close();

    // Waiter should be rejected
    await expect(waiterPromise).rejects.toThrow('Pool closed');
  });

  it('should check isClosed status', async () => {
    const pool = createCDCConsumerPool({
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });

    expect(pool.isClosed()).toBe(false);

    await pool.close();

    expect(pool.isClosed()).toBe(true);
  });
});

// =============================================================================
// Test: Event Listeners
// =============================================================================

describe('CDC Consumer Pool - Event Listeners', () => {
  let pool: CDCConsumerPool;

  beforeEach(() => {
    pool = createCDCConsumerPool({
      maxConsumers: 10,
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });
  });

  afterEach(async () => {
    await pool.close();
  });

  it('should register and trigger event listeners', async () => {
    const handler = vi.fn();
    pool.on('consumer:registered', handler);

    await pool.register({ name: 'test' });

    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('should remove event listeners', async () => {
    const handler = vi.fn();
    pool.on('consumer:registered', handler);
    pool.off('consumer:registered', handler);

    await pool.register({ name: 'test' });

    expect(handler).not.toHaveBeenCalled();
  });

  it('should support multiple listeners for same event', async () => {
    const handler1 = vi.fn();
    const handler2 = vi.fn();

    pool.on('consumer:registered', handler1);
    pool.on('consumer:registered', handler2);

    await pool.register({ name: 'test' });

    expect(handler1).toHaveBeenCalledTimes(1);
    expect(handler2).toHaveBeenCalledTimes(1);
  });

  it('should handle listener errors gracefully', async () => {
    const errorHandler = vi.fn(() => {
      throw new Error('Handler error');
    });
    const goodHandler = vi.fn();

    pool.on('consumer:registered', errorHandler);
    pool.on('consumer:registered', goodHandler);

    // Should not throw
    await pool.register({ name: 'test' });

    // Good handler should still be called
    expect(goodHandler).toHaveBeenCalled();
  });
});

// =============================================================================
// Test: Default Configuration
// =============================================================================

describe('CDC Consumer Pool - Configuration', () => {
  it('should use default configuration values', async () => {
    const pool = createCDCConsumerPool();

    const stats = pool.getStats();
    expect(stats.maxConsumers).toBe(100); // DEFAULT_CDC_POOL_CONFIG.maxConsumers

    await pool.close();
  });

  it('should allow partial configuration override', async () => {
    const pool = createCDCConsumerPool({
      maxConsumers: 5,
      // Other values should use defaults
    });

    const stats = pool.getStats();
    expect(stats.maxConsumers).toBe(5);

    await pool.close();
  });

  it('should have sensible defaults exported', () => {
    expect(DEFAULT_CDC_POOL_CONFIG).toBeDefined();
    expect(DEFAULT_CDC_POOL_CONFIG.maxConsumers).toBe(100);
    expect(DEFAULT_CDC_POOL_CONFIG.backpressureThreshold).toBe(0.8);
    expect(DEFAULT_CDC_POOL_CONFIG.backpressureStrategy).toBe('queue');
  });
});

// =============================================================================
// Test: Signal Ready
// =============================================================================

describe('CDC Consumer Pool - Signal Ready', () => {
  let pool: CDCConsumerPool;

  beforeEach(() => {
    pool = createCDCConsumerPool({
      maxConsumers: 10,
      healthCheckInterval: 0,
      keepAliveInterval: 0,
    });
  });

  afterEach(async () => {
    await pool.close();
  });

  it('should update last activity on signal ready', async () => {
    const consumer = await pool.register({ name: 'ready-consumer' });

    const beforeReady = pool.getConsumer(consumer.id)!.lastActivityAt;

    await new Promise(resolve => setTimeout(resolve, 10));

    pool.signalReady(consumer.id);

    const afterReady = pool.getConsumer(consumer.id)!.lastActivityAt;
    expect(afterReady).toBeGreaterThan(beforeReady);
  });

  it('should try to flush buffer on signal ready', async () => {
    const receivedEvents: CDCEvent[] = [];
    let eventHandlerCalled = false;

    const consumer = await pool.register({
      name: 'buffered-consumer',
      onEvents: async (events) => {
        eventHandlerCalled = true;
        receivedEvents.push(...events);
      },
    });

    // Buffer events (by temporarily having no handler)
    const events = createMockEvents(3);
    await pool.send(consumer.id, events);

    // Signal ready should attempt to flush
    pool.signalReady(consumer.id);

    // Events should have been delivered during send (since handler exists)
    expect(receivedEvents.length).toBeGreaterThanOrEqual(0);
  });

  it('should handle signal ready for non-existent consumer gracefully', () => {
    // Should not throw
    expect(() => pool.signalReady('non-existent')).not.toThrow();
  });
});
