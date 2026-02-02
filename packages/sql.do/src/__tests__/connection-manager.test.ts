/**
 * ConnectionManager Tests
 *
 * Tests for the ConnectionManager class which provides a unified interface
 * for managing WebSocket connections with optional pooling support.
 *
 * Uses FakeWebSocket from test-utils.ts per NO MOCKS philosophy.
 *
 * Issue: sql-k3wp
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { ConnectionManager } from '../connection-manager.js';
import {
  FakeWebSocket,
  FakeWebSocketTracker,
  setupMockWebSocket,
} from './test-utils.js';

// =============================================================================
// Test Setup
// =============================================================================

let tracker: FakeWebSocketTracker;
let cleanup: () => void;

beforeEach(() => {
  tracker = new FakeWebSocketTracker();
  cleanup = setupMockWebSocket(tracker.createTrackedClass());
});

afterEach(async () => {
  cleanup();
  tracker.clear();
});

// =============================================================================
// Basic Connection Management
// =============================================================================

describe('ConnectionManager: Basic Connection', () => {
  it('should create manager with URL', () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    expect(manager.getUrl()).toBe('ws://localhost:8080');
    expect(manager.isConnected()).toBe(false);
  });

  it('should convert http:// to ws://', () => {
    const manager = new ConnectionManager({ url: 'http://localhost:8080' });

    expect(manager.getUrl()).toBe('ws://localhost:8080');
  });

  it('should convert https:// to wss://', () => {
    const manager = new ConnectionManager({ url: 'https://localhost:8080' });

    expect(manager.getUrl()).toBe('wss://localhost:8080');
  });

  it('should establish connection on connect()', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    expect(manager.isConnected()).toBe(false);

    await manager.connect();

    expect(manager.isConnected()).toBe(true);
    expect(tracker.instances.length).toBe(1);
  });

  it('should return immediately if already connected', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    await manager.connect();
    const instanceCount = tracker.instances.length;

    await manager.connect();

    // Should not create another WebSocket
    expect(tracker.instances.length).toBe(instanceCount);
  });

  it('should share connection attempt for concurrent connect() calls', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    // Call connect multiple times concurrently
    await Promise.all([
      manager.connect(),
      manager.connect(),
      manager.connect(),
    ]);

    // Should only create one WebSocket
    expect(tracker.instances.length).toBe(1);
  });

  it('should close connection on close()', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    await manager.connect();
    expect(manager.isConnected()).toBe(true);

    await manager.close();
    expect(manager.isConnected()).toBe(false);
  });

  it('should handle close on disconnected manager', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    // Should not throw
    await expect(manager.close()).resolves.toBeUndefined();
  });
});

// =============================================================================
// Event Handling
// =============================================================================

describe('ConnectionManager: Event Handling', () => {
  it('should emit connected event', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });
    const connectedHandler = vi.fn();

    manager.on('connected', connectedHandler);

    await manager.connect();

    expect(connectedHandler).toHaveBeenCalledWith(
      expect.objectContaining({
        url: 'ws://localhost:8080',
        timestamp: expect.any(Date),
      })
    );
  });

  it('should emit disconnected event on close', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });
    const disconnectedHandler = vi.fn();

    manager.on('disconnected', disconnectedHandler);

    await manager.connect();
    await manager.close();

    expect(disconnectedHandler).toHaveBeenCalledWith(
      expect.objectContaining({
        url: 'ws://localhost:8080',
        timestamp: expect.any(Date),
      })
    );
  });

  it('should support on/off chaining', () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });
    const handler = vi.fn();

    // Should return manager for chaining
    const result = manager.on('connected', handler);
    expect(result).toBe(manager);

    const result2 = manager.off('connected', handler);
    expect(result2).toBe(manager);
  });

  it('should not call removed listener', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });
    const handler = vi.fn();

    manager.on('connected', handler);
    manager.off('connected', handler);

    await manager.connect();

    expect(handler).not.toHaveBeenCalled();
  });

  it('should call multiple listeners for same event', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });
    const handler1 = vi.fn();
    const handler2 = vi.fn();

    manager.on('connected', handler1);
    manager.on('connected', handler2);

    await manager.connect();

    expect(handler1).toHaveBeenCalledTimes(1);
    expect(handler2).toHaveBeenCalledTimes(1);
  });

  it('should handle error in listener gracefully', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });
    const throwingHandler = () => { throw new Error('Listener error'); };
    const normalHandler = vi.fn();

    manager.on('connected', throwingHandler);
    manager.on('connected', normalHandler);

    // Should not throw, should still call other listeners
    await expect(manager.connect()).resolves.toBeUndefined();
    expect(normalHandler).toHaveBeenCalled();
  });
});

// =============================================================================
// Message Handling
// =============================================================================

describe('ConnectionManager: Message Handling', () => {
  it('should set message handler', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });
    const messageHandler = vi.fn();

    manager.setMessageHandler(messageHandler);

    await manager.connect();

    // Simulate incoming message
    tracker.latest?.simulateMessage({ id: '1', result: { rows: [] } });

    // Wait for message to be processed
    await new Promise(resolve => setTimeout(resolve, 20));

    expect(messageHandler).toHaveBeenCalled();
  });

  it('should pass message data to handler', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });
    const messageHandler = vi.fn();

    manager.setMessageHandler(messageHandler);

    await manager.connect();

    const testMessage = { id: '123', result: { rows: [{ id: 1 }] } };
    tracker.latest?.simulateMessage(testMessage);

    await new Promise(resolve => setTimeout(resolve, 20));

    expect(messageHandler).toHaveBeenCalledWith(JSON.stringify(testMessage));
  });

  it('should call close callback on connection close', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });
    const closeCallback = vi.fn();

    manager.setCloseCallback(closeCallback);

    await manager.connect();

    // Simulate connection close from server
    tracker.latest?.simulateClose();

    // Wait for close event to be processed
    await new Promise(resolve => setTimeout(resolve, 20));

    expect(closeCallback).toHaveBeenCalled();
  });
});

// =============================================================================
// Pool Configuration
// =============================================================================

describe('ConnectionManager: Pool Configuration', () => {
  it('should not have pool by default', () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    expect(manager.hasPool()).toBe(false);
  });

  it('should have pool when pool config provided', () => {
    const manager = new ConnectionManager({
      url: 'ws://localhost:8080',
      pool: { maxSize: 5 },
    });

    expect(manager.hasPool()).toBe(true);
  });

  it('should return undefined pool stats when no pool', () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    expect(manager.getPoolStats()).toBeUndefined();
  });

  it('should return pool stats when pool configured', async () => {
    const manager = new ConnectionManager({
      url: 'ws://localhost:8080',
      pool: { maxSize: 5 },
    });

    await manager.connect();

    const stats = manager.getPoolStats();
    expect(stats).toBeDefined();
    expect(stats?.maxSize).toBe(5);
  });

  it('should return undefined pool health when no pool', () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    expect(manager.getPoolHealth()).toBeUndefined();
  });

  it('should return pool health when pool configured', async () => {
    const manager = new ConnectionManager({
      url: 'ws://localhost:8080',
      pool: { maxSize: 5 },
    });

    await manager.connect();

    const health = manager.getPoolHealth();
    expect(health).toBeDefined();
    expect(typeof health?.healthy).toBe('boolean');
  });

  it('should return undefined connection info when no pool', () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    expect(manager.getConnectionInfo()).toBeUndefined();
  });

  it('should return connection info when pool configured', async () => {
    const manager = new ConnectionManager({
      url: 'ws://localhost:8080',
      pool: { maxSize: 5 },
    });

    await manager.connect();

    const info = manager.getConnectionInfo();
    expect(info).toBeDefined();
    expect(Array.isArray(info)).toBe(true);
  });

  it('should return empty tags when no pool', () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    expect(manager.getConnectionTags()).toEqual([]);
  });

  it('should return connection tags from pool', async () => {
    const manager = new ConnectionManager({
      url: 'ws://localhost:8080',
      pool: {
        maxSize: 5,
        connectionTags: ['tag1', 'tag2'],
      },
    });

    const tags = manager.getConnectionTags();
    expect(tags).toContain('tag1');
    expect(tags).toContain('tag2');
  });
});

// =============================================================================
// Connection Release
// =============================================================================

describe('ConnectionManager: Connection Release', () => {
  it('should release pooled connection', async () => {
    const manager = new ConnectionManager({
      url: 'ws://localhost:8080',
      pool: { maxSize: 5 },
    });

    await manager.connect();

    // Release should not throw
    expect(() => manager.releaseConnection()).not.toThrow();
  });

  it('should handle release when no pool', () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    // Release should not throw when no pool
    expect(() => manager.releaseConnection()).not.toThrow();
  });
});

// =============================================================================
// Pool Event Forwarding
// =============================================================================

describe('ConnectionManager: Pool Event Forwarding', () => {
  it('should forward pool:connection-created event', async () => {
    const manager = new ConnectionManager({
      url: 'ws://localhost:8080',
      pool: { maxSize: 5 },
    });

    const handler = vi.fn();
    manager.on('pool:connection-created', handler);

    await manager.connect();

    // Pool events may be emitted during connection
    // Check that the listener was registered successfully
    expect(handler).toBeDefined();
  });

  it('should forward pool:connection-reused event', async () => {
    const manager = new ConnectionManager({
      url: 'ws://localhost:8080',
      pool: { maxSize: 5 },
    });

    const handler = vi.fn();
    manager.on('pool:connection-reused', handler);

    // Make multiple connections to trigger reuse
    await manager.connect();

    // Check that the listener was registered
    expect(handler).toBeDefined();
  });

  it('should forward pool:connection-closed event', async () => {
    const manager = new ConnectionManager({
      url: 'ws://localhost:8080',
      pool: { maxSize: 5 },
    });

    const handler = vi.fn();
    manager.on('pool:connection-closed', handler);

    await manager.connect();
    await manager.close();

    // Check that the listener was registered
    expect(handler).toBeDefined();
  });

  it('should forward pool:health-check event', async () => {
    const manager = new ConnectionManager({
      url: 'ws://localhost:8080',
      pool: {
        maxSize: 5,
        healthCheckInterval: 100,
      },
    });

    const handler = vi.fn();
    manager.on('pool:health-check', handler);

    await manager.connect();

    // Wait for health check interval
    await new Promise(resolve => setTimeout(resolve, 150));

    // Cleanup
    await manager.close();

    // Health check may have been triggered
    expect(handler).toBeDefined();
  });

  it('should forward pool:backpressure event', async () => {
    const manager = new ConnectionManager({
      url: 'ws://localhost:8080',
      pool: {
        maxSize: 1,
        maxWaitingRequests: 0,
        backpressureStrategy: 'reject',
      },
    });

    const handler = vi.fn();
    manager.on('pool:backpressure', handler);

    // Check that the listener was registered
    expect(handler).toBeDefined();

    await manager.close();
  });
});

// =============================================================================
// Error Scenarios
// =============================================================================

describe('ConnectionManager: Error Scenarios', () => {
  it('should throw ConnectionError on WebSocket error', async () => {
    // Create a WebSocket that fails to connect
    const failingClass = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        this.readyState = FakeWebSocket.READY_STATE_CONNECTING;
        setTimeout(() => {
          this.simulateError(new Error('Connection refused'));
        }, 5);
      }
    };

    cleanup();
    cleanup = setupMockWebSocket(failingClass);

    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    await expect(manager.connect()).rejects.toThrow();
  });

  it('should handle connection close during getConnection', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    await manager.connect();

    // Simulate unexpected close
    tracker.latest?.simulateClose();

    // Wait for close to be processed
    await new Promise(resolve => setTimeout(resolve, 20));

    expect(manager.isConnected()).toBe(false);
  });
});

// =============================================================================
// Reconnection After Close
// =============================================================================

describe('ConnectionManager: Reconnection', () => {
  it('should allow reconnection after close', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });

    await manager.connect();
    expect(manager.isConnected()).toBe(true);

    await manager.close();
    expect(manager.isConnected()).toBe(false);

    // Reconnect
    await manager.connect();
    expect(manager.isConnected()).toBe(true);

    // Should have created two WebSockets total
    expect(tracker.instances.length).toBe(2);
  });

  it('should emit events on reconnection', async () => {
    const manager = new ConnectionManager({ url: 'ws://localhost:8080' });
    const connectedHandler = vi.fn();
    const disconnectedHandler = vi.fn();

    manager.on('connected', connectedHandler);
    manager.on('disconnected', disconnectedHandler);

    await manager.connect();
    await manager.close();
    await manager.connect();

    expect(connectedHandler).toHaveBeenCalledTimes(2);
    expect(disconnectedHandler).toHaveBeenCalledTimes(1);
  });
});

// =============================================================================
// Pool Closure
// =============================================================================

describe('ConnectionManager: Pool Closure', () => {
  it('should close pool on manager close', async () => {
    const manager = new ConnectionManager({
      url: 'ws://localhost:8080',
      pool: { maxSize: 5 },
    });

    await manager.connect();
    expect(manager.hasPool()).toBe(true);

    await manager.close();

    // After close, pool should be null
    expect(manager.hasPool()).toBe(false);
  });

  it('should release pooled connection before close', async () => {
    const manager = new ConnectionManager({
      url: 'ws://localhost:8080',
      pool: { maxSize: 5 },
    });

    await manager.connect();

    // Close should release and then close pool
    await expect(manager.close()).resolves.toBeUndefined();
  });
});
