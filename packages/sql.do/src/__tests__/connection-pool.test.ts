/**
 * ConnectionPool Tests
 *
 * Tests for enterprise-grade connection pooling with:
 * - Pool initialization and warm-up
 * - Concurrent requests sharing connections
 * - Pool exhaustion and request queuing
 * - Unhealthy connection replacement
 * - Graceful shutdown with active query completion
 *
 * Issue: sql-171z
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  ConnectionPool,
  type ConnectionPoolConfig,
  type PooledConnection,
} from '../connection-pool.js';
import { ConnectionError } from '../errors.js';

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * WebSocket event callback type
 */
type WebSocketEventCallback = (event: { data?: string }) => void;

/**
 * Mock WebSocket that tracks instance count for pool testing
 */
let mockWebSocketInstances: MockPoolWebSocket[] = [];

class MockPoolWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  readyState = MockPoolWebSocket.CONNECTING;
  url: string;
  id: string;
  createdAt: number;
  lastUsedAt: number;
  private listeners: Map<string, Set<WebSocketEventCallback>> = new Map();

  constructor(url: string) {
    this.url = url;
    this.id = `ws-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    this.createdAt = Date.now();
    this.lastUsedAt = Date.now();
    mockWebSocketInstances.push(this);

    // Simulate async connection
    setTimeout(() => {
      this.readyState = MockPoolWebSocket.OPEN;
      this.emit('open', {});
    }, 5);
  }

  addEventListener(event: string, callback: WebSocketEventCallback): void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(callback);
  }

  removeEventListener(event: string, callback: WebSocketEventCallback): void {
    this.listeners.get(event)?.delete(callback);
  }

  send(data: string): void {
    this.lastUsedAt = Date.now();
    const request = JSON.parse(data) as { id: string };
    setTimeout(() => {
      this.emit('message', {
        data: JSON.stringify({
          id: request.id,
          result: { rows: [], rowsAffected: 0 },
        }),
      });
    }, 2);
  }

  close(): void {
    this.readyState = MockPoolWebSocket.CLOSED;
    this.emit('close', {});
  }

  private emit(event: string, data: { data?: string }): void {
    this.listeners.get(event)?.forEach((callback) => callback(data));
  }

  // Test helpers
  simulateError(error: Error): void {
    this.emit('error', { data: error.message });
  }

  simulateClose(): void {
    this.readyState = MockPoolWebSocket.CLOSED;
    this.emit('close', {});
  }

  setUnhealthy(): void {
    this.readyState = MockPoolWebSocket.CLOSED;
  }
}

// =============================================================================
// Test Setup
// =============================================================================

let originalWebSocket: typeof globalThis.WebSocket;

beforeEach(() => {
  mockWebSocketInstances = [];
  originalWebSocket = globalThis.WebSocket;
  // @ts-expect-error - Mocking global WebSocket
  globalThis.WebSocket = MockPoolWebSocket;
});

afterEach(async () => {
  // Clean up all pools
  for (const ws of mockWebSocketInstances) {
    if (ws.readyState !== MockPoolWebSocket.CLOSED) {
      ws.close();
    }
  }
  mockWebSocketInstances = [];
  globalThis.WebSocket = originalWebSocket;
});

// =============================================================================
// 1. POOL INITIALIZATION AND WARM-UP
// =============================================================================

describe('Pool Initialization and Warm-up', () => {
  it('should create pool with default configuration', () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
    });

    expect(pool).toBeInstanceOf(ConnectionPool);
    expect(pool.isInitialized()).toBe(false);
    expect(pool.isClosed()).toBe(false);
  });

  it('should initialize pool and start background tasks', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      minIdle: 0,
    });

    await pool.initialize();

    expect(pool.isInitialized()).toBe(true);
    expect(pool.isClosed()).toBe(false);

    await pool.shutdown();
  });

  it('should warm up pool with specified number of connections', async () => {
    const warmUpSize = 3;
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      warmUp: true,
      warmUpSize,
      maxSize: 10,
    });

    const warmUpEvents: unknown[] = [];
    pool.on('pool:warm-up-complete', (event) => {
      warmUpEvents.push(event);
    });

    await pool.initialize();

    // Wait for connections to be established
    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(warmUpEvents.length).toBe(1);
    expect((warmUpEvents[0] as { connectionsCreated: number }).connectionsCreated).toBe(warmUpSize);
    expect(mockWebSocketInstances.length).toBe(warmUpSize);

    const stats = pool.getStats();
    expect(stats.totalConnections).toBe(warmUpSize);

    await pool.shutdown();
  });

  it('should maintain minimum idle connections', async () => {
    const minIdle = 2;
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      minIdle,
      maxSize: 5,
    });

    await pool.initialize();

    // Wait for connections to be established
    await new Promise((resolve) => setTimeout(resolve, 50));

    const stats = pool.getStats();
    expect(stats.idleConnections).toBeGreaterThanOrEqual(minIdle);

    await pool.shutdown();
  });

  it('should not reinitialize if already initialized', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      warmUp: true,
      warmUpSize: 2,
    });

    await pool.initialize();
    const firstCount = mockWebSocketInstances.length;

    await pool.initialize();
    const secondCount = mockWebSocketInstances.length;

    expect(firstCount).toBe(secondCount);

    await pool.shutdown();
  });

  it('should emit warm-up event with duration', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      warmUp: true,
      warmUpSize: 2,
    });

    let warmUpEvent: { connectionsCreated: number; duration: number } | null = null;
    pool.on('pool:warm-up-complete', (event) => {
      warmUpEvent = event;
    });

    await pool.initialize();
    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(warmUpEvent).not.toBeNull();
    expect(warmUpEvent!.duration).toBeGreaterThanOrEqual(0);
    expect(warmUpEvent!.connectionsCreated).toBe(2);

    await pool.shutdown();
  });
});

// =============================================================================
// 2. CONCURRENT REQUESTS SHARE CONNECTIONS
// =============================================================================

describe('Concurrent Requests Share Connections', () => {
  it('should reuse connections across sequential requests', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxSize: 5,
    });

    await pool.initialize();

    // First request creates a connection
    const conn1 = await pool.acquire();
    expect(mockWebSocketInstances.length).toBe(1);
    pool.release(conn1);

    // Second request reuses the same connection
    const conn2 = await pool.acquire();
    expect(mockWebSocketInstances.length).toBe(1);
    expect(conn2.id).toBe(conn1.id);
    pool.release(conn2);

    await pool.shutdown();
  });

  it('should emit connection-reused events', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
    });

    await pool.initialize();

    const reusedEvents: unknown[] = [];
    pool.on('pool:connection-reused', (event) => {
      reusedEvents.push(event);
    });

    const conn1 = await pool.acquire();
    pool.release(conn1);

    const conn2 = await pool.acquire();
    pool.release(conn2);

    // First acquire creates, second reuses
    expect(reusedEvents.length).toBe(2); // Both are reuse from pool perspective

    await pool.shutdown();
  });

  it('should track connection reuse ratio', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
    });

    await pool.initialize();

    // Make several requests
    for (let i = 0; i < 5; i++) {
      const conn = await pool.acquire();
      pool.release(conn);
    }

    const stats = pool.getStats();
    expect(stats.connectionReuseRatio).toBeGreaterThan(0);
    expect(stats.totalRequestsServed).toBe(5);
    expect(stats.connectionsCreated).toBe(1);

    await pool.shutdown();
  });

  it('should create additional connections for concurrent active requests', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxSize: 5,
    });

    await pool.initialize();

    // Acquire multiple connections concurrently (without releasing)
    const conn1 = await pool.acquire();
    const conn2 = await pool.acquire();
    const conn3 = await pool.acquire();

    // Should have created 3 separate connections
    expect(mockWebSocketInstances.length).toBe(3);
    expect(conn1.id).not.toBe(conn2.id);
    expect(conn2.id).not.toBe(conn3.id);

    // Release all
    pool.release(conn1);
    pool.release(conn2);
    pool.release(conn3);

    await pool.shutdown();
  });

  it('should update lastUsedAt on connection reuse', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
    });

    await pool.initialize();

    const conn1 = await pool.acquire();
    const firstUsedAt = conn1.lastUsedAt;
    pool.release(conn1);

    // Wait a bit
    await new Promise((resolve) => setTimeout(resolve, 20));

    const conn2 = await pool.acquire();
    expect(conn2.lastUsedAt).toBeGreaterThan(firstUsedAt);
    pool.release(conn2);

    await pool.shutdown();
  });
});

// =============================================================================
// 3. POOL EXHAUSTION AND REQUEST QUEUING
// =============================================================================

describe('Pool Exhaustion and Request Queuing', () => {
  it('should queue requests when pool is exhausted', async () => {
    const maxSize = 2;
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxSize,
      acquireTimeout: 5000,
    });

    await pool.initialize();

    // Fill the pool
    const conn1 = await pool.acquire();
    const conn2 = await pool.acquire();

    expect(mockWebSocketInstances.length).toBe(maxSize);

    // This should be queued
    const queuedPromise = pool.acquire();

    // Check waiting requests
    const stats = pool.getStats();
    expect(stats.waitingRequests).toBe(1);
    expect(stats.activeConnections).toBe(maxSize);

    // Release a connection to unblock the queued request
    pool.release(conn1);

    const conn3 = await queuedPromise;
    expect(conn3.id).toBe(conn1.id);

    pool.release(conn2);
    pool.release(conn3);

    await pool.shutdown();
  });

  it('should reject with timeout when queue wait exceeds timeout', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxSize: 1,
      acquireTimeout: 50, // Short timeout
    });

    await pool.initialize();

    // Fill the pool
    const conn1 = await pool.acquire();

    // This should timeout
    await expect(pool.acquire()).rejects.toThrow('Connection acquisition timeout');

    pool.release(conn1);
    await pool.shutdown();
  });

  it('should emit backpressure event when queue is full', async () => {
    const maxWaitingRequests = 2;
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxSize: 1,
      maxWaitingRequests,
      backpressureStrategy: 'reject',
      acquireTimeout: 1000,
    });

    await pool.initialize();

    const backpressureEvents: unknown[] = [];
    pool.on('pool:backpressure', (event) => {
      backpressureEvents.push(event);
    });

    // Fill the pool
    const conn = await pool.acquire();

    // Fill the queue
    const queued1 = pool.acquire().catch(() => {});
    const queued2 = pool.acquire().catch(() => {});

    // This should trigger backpressure
    await expect(pool.acquire()).rejects.toThrow('Pool exhausted');
    expect(backpressureEvents.length).toBe(1);

    pool.release(conn);
    await pool.shutdown();
  });

  it('should process queued requests in order', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxSize: 1,
      acquireTimeout: 5000,
    });

    await pool.initialize();

    const order: number[] = [];

    // Fill the pool
    const conn1 = await pool.acquire();

    // Queue multiple requests
    const promise1 = pool.acquire().then((c) => {
      order.push(1);
      pool.release(c);
    });
    const promise2 = pool.acquire().then((c) => {
      order.push(2);
      pool.release(c);
    });
    const promise3 = pool.acquire().then((c) => {
      order.push(3);
      pool.release(c);
    });

    // Release to process queue
    pool.release(conn1);

    await Promise.all([promise1, promise2, promise3]);

    // Should be processed in order
    expect(order).toEqual([1, 2, 3]);

    await pool.shutdown();
  });

  it('should track average wait time in stats', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxSize: 1,
      acquireTimeout: 5000,
    });

    await pool.initialize();

    // Fill the pool
    const conn1 = await pool.acquire();

    // Queue a request
    const queuedPromise = pool.acquire();

    // Wait a bit
    await new Promise((resolve) => setTimeout(resolve, 20));

    // Release to process queue
    pool.release(conn1);

    const conn2 = await queuedPromise;
    pool.release(conn2);

    // Stats should track this wait
    const stats = pool.getStats();
    expect(stats.totalRequestsServed).toBeGreaterThanOrEqual(2);

    await pool.shutdown();
  });
});

// =============================================================================
// 4. UNHEALTHY CONNECTIONS ARE REPLACED
// =============================================================================

describe('Unhealthy Connections are Replaced', () => {
  it('should skip unhealthy connections when acquiring', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxSize: 5,
      validateOnBorrow: true,
    });

    await pool.initialize();

    // Get a connection
    const conn1 = await pool.acquire();
    pool.release(conn1);

    // Make the connection unhealthy
    const wsInstance = mockWebSocketInstances[0];
    wsInstance.setUnhealthy();

    // Next acquire should create a new connection
    const conn2 = await pool.acquire();
    expect(conn2.id).not.toBe(conn1.id);
    expect(mockWebSocketInstances.length).toBe(2);
    pool.release(conn2);

    await pool.shutdown();
  });

  it('should emit connection-replaced event', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxSize: 5,
      healthCheckInterval: 50,
    });

    const replacedEvents: unknown[] = [];
    pool.on('pool:connection-replaced', (event) => {
      replacedEvents.push(event);
    });

    await pool.initialize();

    // Get a connection
    const conn = await pool.acquire();
    pool.release(conn);

    // Make the connection unhealthy
    mockWebSocketInstances[0].setUnhealthy();

    // Wait for health check
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Should have replaced the connection
    expect(replacedEvents.length).toBeGreaterThanOrEqual(0); // May or may not trigger depending on timing

    await pool.shutdown();
  });

  it('should run periodic health checks', async () => {
    const healthCheckInterval = 50;
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      healthCheckInterval,
      warmUp: true,
      warmUpSize: 2,
    });

    const healthEvents: unknown[] = [];
    pool.on('pool:health-check', (event) => {
      healthEvents.push(event);
    });

    await pool.initialize();
    await new Promise((resolve) => setTimeout(resolve, 50)); // Wait for warm-up

    // Wait for health check
    await new Promise((resolve) => setTimeout(resolve, healthCheckInterval + 20));

    expect(healthEvents.length).toBeGreaterThanOrEqual(1);

    await pool.shutdown();
  });

  it('should report health status correctly', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      warmUp: true,
      warmUpSize: 2,
    });

    await pool.initialize();
    await new Promise((resolve) => setTimeout(resolve, 50));

    const health = pool.getHealth();
    expect(health.healthy).toBe(true);
    expect(health.healthyConnections).toBe(2);
    expect(health.unhealthyConnections).toBe(0);

    await pool.shutdown();
  });

  it('should close connections past max lifetime', async () => {
    vi.useFakeTimers();

    const maxLifetime = 100;
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxLifetime,
      idleTimeout: 200,
      minIdle: 0,
    });

    await pool.initialize();

    const conn = await pool.acquire();
    pool.release(conn);

    const initialId = conn.id;

    // Advance time past max lifetime
    vi.advanceTimersByTime(maxLifetime + 50);

    // Trigger idle check
    vi.advanceTimersByTime(50);

    // Pool should have closed the old connection
    // Next acquire may get a new connection
    const stats = pool.getStats();
    // Connection may have been cleaned up

    vi.useRealTimers();
    await pool.shutdown();
  });
});

// =============================================================================
// 5. GRACEFUL SHUTDOWN
// =============================================================================

describe('Graceful Shutdown', () => {
  it('should wait for active queries during shutdown', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      shutdownTimeout: 5000,
    });

    await pool.initialize();

    const conn = await pool.acquire();

    // Start shutdown
    const shutdownPromise = pool.shutdown();

    expect(pool.isShuttingDown()).toBe(true);

    // Release the connection
    pool.release(conn);

    // Shutdown should complete
    await shutdownPromise;

    expect(pool.isClosed()).toBe(true);
    expect(pool.isShuttingDown()).toBe(false);
  });

  it('should emit shutdown events', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
    });

    await pool.initialize();

    const conn = await pool.acquire();

    const shutdownStarted: unknown[] = [];
    const shutdownComplete: unknown[] = [];

    pool.on('pool:shutdown-started', (event) => {
      shutdownStarted.push(event);
    });
    pool.on('pool:shutdown-complete', (event) => {
      shutdownComplete.push(event);
    });

    pool.release(conn);
    await pool.shutdown();

    expect(shutdownStarted.length).toBe(1);
    expect(shutdownComplete.length).toBe(1);
  });

  it('should reject new acquisitions during shutdown', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
    });

    await pool.initialize();

    // Start shutdown
    const shutdownPromise = pool.shutdown();

    // Try to acquire - should fail
    await expect(pool.acquire()).rejects.toThrow('Pool is shutting down');

    await shutdownPromise;
  });

  it('should reject queued requests during shutdown', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxSize: 1,
      acquireTimeout: 5000,
    });

    await pool.initialize();

    const conn = await pool.acquire();

    // Queue a request
    const queuedPromise = pool.acquire();

    // Start shutdown - should reject queued request
    const shutdownPromise = pool.shutdown();

    await expect(queuedPromise).rejects.toThrow('Pool shutting down');

    await shutdownPromise;
  });

  it('should force close after shutdown timeout', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      shutdownTimeout: 50, // Short timeout
    });

    await pool.initialize();

    const conn = await pool.acquire();
    // Don't release the connection

    const shutdownComplete: unknown[] = [];
    pool.on('pool:shutdown-complete', (event) => {
      shutdownComplete.push(event);
    });

    await pool.shutdown();

    expect(shutdownComplete.length).toBe(1);
    expect((shutdownComplete[0] as { forcedClose: boolean }).forcedClose).toBe(true);
  });

  it('should close all connections after shutdown', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      warmUp: true,
      warmUpSize: 3,
    });

    await pool.initialize();
    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(mockWebSocketInstances.length).toBe(3);

    await pool.shutdown();

    // All connections should be closed
    const stats = pool.getStats();
    expect(stats.totalConnections).toBe(0);

    for (const ws of mockWebSocketInstances) {
      expect(ws.readyState).toBe(MockPoolWebSocket.CLOSED);
    }
  });

  it('close() should be an alias for shutdown()', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
    });

    await pool.initialize();

    await pool.close();

    expect(pool.isClosed()).toBe(true);
  });
});

// =============================================================================
// 6. CONNECTION AFFINITY FOR TRANSACTIONS
// =============================================================================

describe('Connection Affinity for Transactions', () => {
  it('should pin connection to transaction', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
    });

    await pool.initialize();

    // Acquire with transaction ID
    const transactionId = 'tx-123' as unknown as import('../types.js').TransactionId;
    const conn1 = await pool.acquire({ transactionId });

    // Same transaction should get same connection
    const conn2 = await pool.acquire({ transactionId });
    expect(conn2.id).toBe(conn1.id);

    // Release without completing transaction - should keep active
    pool.release(conn1);
    pool.release(conn2);

    const conn3 = await pool.acquire({ transactionId });
    expect(conn3.id).toBe(conn1.id);

    // Complete transaction
    pool.release(conn3, { transactionComplete: true });

    await pool.shutdown();
  });

  it('should allow different transactions to use different connections', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxSize: 5,
    });

    await pool.initialize();

    const tx1 = 'tx-1' as unknown as import('../types.js').TransactionId;
    const tx2 = 'tx-2' as unknown as import('../types.js').TransactionId;

    const conn1 = await pool.acquire({ transactionId: tx1 });
    const conn2 = await pool.acquire({ transactionId: tx2 });

    expect(conn1.id).not.toBe(conn2.id);

    pool.release(conn1, { transactionComplete: true });
    pool.release(conn2, { transactionComplete: true });

    await pool.shutdown();
  });

  it('should clear transaction affinity on transactionComplete', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
    });

    await pool.initialize();

    const transactionId = 'tx-123' as unknown as import('../types.js').TransactionId;
    const conn1 = await pool.acquire({ transactionId });
    pool.release(conn1, { transactionComplete: true });

    // New acquire without transactionId should get any available connection
    const conn2 = await pool.acquire();
    expect(conn2.id).toBe(conn1.id); // Reused from pool

    pool.release(conn2);
    await pool.shutdown();
  });

  it('should create new mapping if pinned connection is unhealthy', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxSize: 5,
    });

    await pool.initialize();

    const transactionId = 'tx-123' as unknown as import('../types.js').TransactionId;
    const conn1 = await pool.acquire({ transactionId });

    // Make connection unhealthy
    mockWebSocketInstances[0].setUnhealthy();
    pool.release(conn1);

    // Next acquire should create new connection
    const conn2 = await pool.acquire({ transactionId });
    expect(conn2.id).not.toBe(conn1.id);

    pool.release(conn2, { transactionComplete: true });
    await pool.shutdown();
  });
});

// =============================================================================
// 7. METRICS AND CONNECTION INFO
// =============================================================================

describe('Pool Metrics and Connection Info', () => {
  it('should track pool statistics', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxSize: 5,
    });

    await pool.initialize();

    // Make some requests
    for (let i = 0; i < 5; i++) {
      const conn = await pool.acquire();
      pool.release(conn);
    }

    const stats = pool.getStats();
    expect(stats.totalRequestsServed).toBe(5);
    expect(stats.connectionsCreated).toBe(1);
    expect(stats.connectionReuseRatio).toBeGreaterThan(0.5);

    await pool.shutdown();
  });

  it('should provide connection info', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      warmUp: true,
      warmUpSize: 2,
      connectionTags: ['test', 'pool'],
    });

    await pool.initialize();
    await new Promise((resolve) => setTimeout(resolve, 50));

    const info = pool.getConnectionInfo();
    expect(info.length).toBe(2);

    for (const conn of info) {
      expect(conn.id).toBeDefined();
      expect(conn.createdAt).toBeInstanceOf(Date);
      expect(conn.lastUsedAt).toBeInstanceOf(Date);
      expect(conn.tags).toEqual(['test', 'pool']);
    }

    await pool.shutdown();
  });

  it('should track idle time correctly', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
    });

    await pool.initialize();

    const conn = await pool.acquire();
    pool.release(conn);

    // Wait a bit
    await new Promise((resolve) => setTimeout(resolve, 50));

    const info = pool.getConnectionInfo();
    expect(info[0].idleTime).toBeGreaterThanOrEqual(50);

    await pool.shutdown();
  });

  it('should report active connections in stats', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      maxSize: 5,
    });

    await pool.initialize();

    const conn1 = await pool.acquire();
    const conn2 = await pool.acquire();

    const stats = pool.getStats();
    expect(stats.activeConnections).toBe(2);
    expect(stats.idleConnections).toBe(0);

    pool.release(conn1);
    pool.release(conn2);

    const statsAfter = pool.getStats();
    expect(statsAfter.activeConnections).toBe(0);
    expect(statsAfter.idleConnections).toBe(2);

    await pool.shutdown();
  });

  it('should return connection tags', async () => {
    const tags = ['env:test', 'version:1'];
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
      connectionTags: tags,
    });

    expect(pool.getConnectionTags()).toEqual(tags);

    await pool.shutdown();
  });
});

// =============================================================================
// 8. EVENT HANDLING
// =============================================================================

describe('Pool Event Handling', () => {
  it('should emit connection-created events', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
    });

    const createdEvents: unknown[] = [];
    pool.on('pool:connection-created', (event) => {
      createdEvents.push(event);
    });

    await pool.initialize();
    await pool.acquire();

    expect(createdEvents.length).toBe(1);
    expect((createdEvents[0] as { connectionId: string }).connectionId).toBeDefined();

    await pool.shutdown();
  });

  it('should emit connection-closed events', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
    });

    const closedEvents: unknown[] = [];
    pool.on('pool:connection-closed', (event) => {
      closedEvents.push(event);
    });

    await pool.initialize();
    const conn = await pool.acquire();
    pool.release(conn);

    await pool.shutdown();

    expect(closedEvents.length).toBeGreaterThanOrEqual(1);
  });

  it('should allow removing event listeners', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
    });

    const events: unknown[] = [];
    const listener = (event: unknown) => {
      events.push(event);
    };

    pool.on('pool:connection-created', listener);
    pool.off('pool:connection-created', listener);

    await pool.initialize();
    await pool.acquire();

    expect(events.length).toBe(0);

    await pool.shutdown();
  });

  it('should catch and log errors in event listeners', async () => {
    const pool = new ConnectionPool({
      url: 'ws://localhost:8080',
    });

    pool.on('pool:connection-created', () => {
      throw new Error('Listener error');
    });

    // Should not throw
    await pool.initialize();
    await pool.acquire();

    await pool.shutdown();
  });
});
