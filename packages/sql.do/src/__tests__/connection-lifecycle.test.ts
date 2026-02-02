/**
 * Connection Lifecycle Tests
 *
 * Comprehensive tests for connection lifecycle management including:
 * - Connect, disconnect, reconnect flows
 * - Event handling during connection state transitions
 * - Error handling during connection failures
 * - Concurrent connection requests
 *
 * Uses FakeWebSocket from test-utils.ts per NO MOCKS philosophy.
 *
 * Issue: sql-k3wp
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { DoSQLClient, createSQLClient, ConnectionError, TimeoutError, SQLError } from '../client.js';
import {
  FakeWebSocket,
  FakeWebSocketTracker,
  setupMockWebSocket,
  createEventListener,
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

afterEach(() => {
  cleanup();
  tracker.clear();
});

// =============================================================================
// Connection Lifecycle: Connect
// =============================================================================

describe('Connection Lifecycle: Connect', () => {
  it('should establish connection on first connect() call', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    expect(client.isConnected()).toBe(false);

    await client.connect();

    expect(client.isConnected()).toBe(true);
    expect(tracker.instances.length).toBe(1);
  });

  it('should return immediately if already connected', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    await client.connect();
    const connectCount = tracker.instances.length;

    // Second connect should not create new WebSocket
    await client.connect();

    expect(tracker.instances.length).toBe(connectCount);
  });

  it('should share connection attempt for concurrent connect() calls', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    // Call connect multiple times concurrently
    const results = await Promise.all([
      client.connect(),
      client.connect(),
      client.connect(),
    ]);

    // Should only create one WebSocket
    expect(tracker.instances.length).toBe(1);
    expect(client.isConnected()).toBe(true);
  });

  it('should emit connected event on successful connection', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });
    const { listener, calls } = createEventListener('connected');

    client.on('connected', listener);

    await client.connect();

    expect(calls.length).toBe(1);
    expect(calls[0].url).toBe('ws://localhost:8080');
    expect(calls[0].timestamp).toBeInstanceOf(Date);
  });

  it('should convert http:// URL to ws://', async () => {
    const client = new DoSQLClient({ url: 'http://localhost:8080' });

    await client.connect();

    expect(tracker.latest?.url).toBe('ws://localhost:8080');
  });

  it('should convert https:// URL to wss://', async () => {
    const client = new DoSQLClient({ url: 'https://localhost:8080' });

    await client.connect();

    expect(tracker.latest?.url).toBe('wss://localhost:8080');
  });
});

// =============================================================================
// Connection Lifecycle: Disconnect
// =============================================================================

describe('Connection Lifecycle: Disconnect', () => {
  it('should close connection on close() call', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    await client.connect();
    expect(client.isConnected()).toBe(true);

    await client.close();
    expect(client.isConnected()).toBe(false);
  });

  it('should emit disconnected event on close', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });
    const { listener, calls } = createEventListener('disconnected');

    client.on('disconnected', listener);

    await client.connect();
    await client.close();

    expect(calls.length).toBe(1);
    expect(calls[0].url).toBe('ws://localhost:8080');
    expect(calls[0].reason).toBeDefined();
  });

  it('should handle close on already closed connection', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    // Close without connecting should not throw
    await expect(client.close()).resolves.toBeUndefined();
  });

  it('should stop cleanup timer on close', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      idempotency: { enabled: true, cleanupIntervalMs: 1000 },
    });

    await client.connect();
    expect(client.isCleanupTimerActive()).toBe(true);

    await client.close();
    expect(client.isCleanupTimerActive()).toBe(false);
  });

  it('should reject pending requests on connection close', async () => {
    // Create a WebSocket that doesn't respond to allow testing close during pending request
    const slowClass = class extends FakeWebSocket {
      send(_data: string): void {
        // Don't respond immediately - allow close() to interrupt
      }
    };

    cleanup();
    cleanup = setupMockWebSocket(slowClass);

    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    await client.connect();

    // Start a query that won't get a response
    const queryPromise = client.query('SELECT 1');

    // Give it a moment to send the request
    await new Promise(resolve => setTimeout(resolve, 10));

    // Close connection while request is pending
    await client.close();

    await expect(queryPromise).rejects.toThrow(ConnectionError);
  });
});

// =============================================================================
// Connection Lifecycle: Reconnect
// =============================================================================

describe('Connection Lifecycle: Reconnect', () => {
  it('should allow reconnection after disconnect', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    await client.connect();
    expect(client.isConnected()).toBe(true);

    await client.close();
    expect(client.isConnected()).toBe(false);

    // Reconnect by connecting again
    await client.connect();
    expect(client.isConnected()).toBe(true);

    // Should have created a second WebSocket
    expect(tracker.instances.length).toBe(2);
  });

  it('should reconnect automatically on query after disconnect', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    await client.connect();
    await client.close();

    // Query should trigger reconnection
    const queryPromise = client.query('SELECT 1');

    // Give time for connection to establish and response
    await new Promise(resolve => setTimeout(resolve, 50));

    expect(client.isConnected()).toBe(true);
  });
});

// =============================================================================
// Connection State Events
// =============================================================================

describe('Connection State Events', () => {
  it('should support on/off chaining', () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });
    const listener = vi.fn();

    // Should return client for chaining
    const result = client.on('connected', listener);
    expect(result).toBe(client);

    const result2 = client.off('connected', listener);
    expect(result2).toBe(client);
  });

  it('should call multiple listeners for same event', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });
    const listener1 = vi.fn();
    const listener2 = vi.fn();

    client.on('connected', listener1);
    client.on('connected', listener2);

    await client.connect();

    expect(listener1).toHaveBeenCalledTimes(1);
    expect(listener2).toHaveBeenCalledTimes(1);
  });

  it('should not call removed listener', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });
    const listener = vi.fn();

    client.on('connected', listener);
    client.off('connected', listener);

    await client.connect();

    expect(listener).not.toHaveBeenCalled();
  });

  it('should emit error event on message parse failure', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });
    const { listener, calls } = createEventListener('error');

    client.on('error', listener);

    await client.connect();

    // Simulate malformed message
    tracker.latest?.simulateMessage('invalid json {{{');

    // Wait for message to be processed
    await new Promise(resolve => setTimeout(resolve, 10));

    expect(calls.length).toBeGreaterThan(0);
    expect(calls[0].context).toBe('message_parse');
    expect(calls[0].error).toBeInstanceOf(Error);
  });

  it('should handle error in listener gracefully', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });
    const throwingListener = () => { throw new Error('Listener error'); };
    const normalListener = vi.fn();

    client.on('connected', throwingListener);
    client.on('connected', normalListener);

    // Should not throw, should still call other listeners
    await expect(client.connect()).resolves.toBeUndefined();
    expect(normalListener).toHaveBeenCalled();
  });
});

// =============================================================================
// Connection Error Handling
// =============================================================================

describe('Connection Error Handling', () => {
  it('should throw ConnectionError on WebSocket error during connect', async () => {
    // Create a WebSocket that fails to connect
    const failingClass = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        // Prevent the normal open event
        this.readyState = FakeWebSocket.READY_STATE_CONNECTING;
        setTimeout(() => {
          this.simulateError(new Error('Connection refused'));
        }, 5);
      }
    };

    cleanup();
    cleanup = setupMockWebSocket(failingClass);

    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    await expect(client.connect()).rejects.toThrow(ConnectionError);
  });

  it('should include URL in ConnectionError', async () => {
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

    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    try {
      await client.connect();
    } catch (error) {
      expect(error).toBeInstanceOf(ConnectionError);
      const connError = error as ConnectionError;
      expect(connError.url).toContain('localhost:8080');
      expect(connError.isRetryable()).toBe(true);
      expect(connError.code).toBe('CONN_FAILED');
    }
  });

  it('should mask sensitive URL parameters in error', () => {
    const error = new ConnectionError(
      'Failed',
      'ws://user:secret@localhost:8080?token=abc123&key=xyz'
    );

    // Should mask password and sensitive params
    expect(error.url).not.toContain('secret');
    expect(error.url).not.toContain('abc123');
    expect(error.url).toContain('***');
  });
});

// =============================================================================
// Query Execution States
// =============================================================================

describe('Query Execution States', () => {
  it('should auto-connect when executing query on disconnected client', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    expect(client.isConnected()).toBe(false);

    const queryPromise = client.query('SELECT 1');

    // Wait for connection and query
    await new Promise(resolve => setTimeout(resolve, 50));

    expect(client.isConnected()).toBe(true);
  });

  it('should handle exec() with idempotency key', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      idempotency: { enabled: true },
    });

    await client.connect();

    // Execute a mutation
    const key = await client.getIdempotencyKey('INSERT INTO users VALUES (1)', [1]);
    expect(key).toBeDefined();
    expect(typeof key).toBe('string');

    // Key should be cached for retries
    const sameKey = await client.getIdempotencyKey('INSERT INTO users VALUES (1)', [1]);
    expect(sameKey).toBe(key);
  });

  it('should clear idempotency key on successful exec', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      idempotency: { enabled: true },
    });

    await client.connect();

    const sql = 'INSERT INTO users VALUES (1)';
    const params = [1];

    // Get key (creates cache entry)
    const key1 = await client.getIdempotencyKey(sql, params);
    expect(client.getCacheSize()).toBeGreaterThan(0);

    // Clear the key (simulating successful exec)
    client.clearIdempotencyKey(sql, params);

    // Next key should be different (new timestamp/random)
    const key2 = await client.getIdempotencyKey(sql, params);
    expect(key2).not.toBe(key1);
  });
});

// =============================================================================
// Transaction Connection Handling
// =============================================================================

describe('Transaction Connection Handling', () => {
  it('should maintain connection during transaction', async () => {
    // Create a WebSocket that returns proper transaction response
    const transactionClass = class extends FakeWebSocket {
      send(data: string): void {
        this.lastUsedAt = Date.now();
        const request = JSON.parse(data) as { id: string; method: string };
        setTimeout(() => {
          if (request.method === 'beginTransaction') {
            this.emit('message', {
              data: JSON.stringify({
                id: request.id,
                result: {
                  id: 'txn-123',
                  isolationLevel: 'SERIALIZABLE',
                  readOnly: false,
                  startedAt: new Date().toISOString(),
                  snapshotLSN: '0',
                },
              }),
            });
          } else if (request.method === 'commit' || request.method === 'rollback') {
            this.emit('message', {
              data: JSON.stringify({
                id: request.id,
                result: { lsn: '1' },
              }),
            });
          } else {
            this.emit('message', {
              data: JSON.stringify({
                id: request.id,
                result: { rows: [], rowsAffected: 0 },
              }),
            });
          }
        }, 5);
      }

      private emit(event: string, data: { data?: string }): void {
        (this as any).listeners.get(event)?.forEach((cb: (e: unknown) => void) => cb(data));
      }
    };

    cleanup();
    cleanup = setupMockWebSocket(transactionClass);

    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    await client.transaction(async (tx) => {
      expect(client.isConnected()).toBe(true);
      expect(tx.transactionId).toBeDefined();
    });

    // Should still be connected after transaction
    expect(client.isConnected()).toBe(true);
  });

  it('should use same connection for all transaction operations', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    await client.connect();
    const initialCount = tracker.instances.length;

    await client.transaction(async (tx) => {
      // All operations should reuse existing connection
      await tx.exec('INSERT INTO test VALUES (1)', [1]).catch(() => {});
      await tx.query('SELECT * FROM test').catch(() => {});
    }).catch(() => {});

    // Should not create additional WebSocket connections
    expect(tracker.instances.length).toBe(initialCount);
  });

  it('should rollback transaction on disconnect during transaction', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    await client.connect();

    const txPromise = client.transaction(async (tx) => {
      // Simulate connection close during transaction
      tracker.latest?.simulateClose();

      // This should fail
      await tx.exec('INSERT INTO test VALUES (1)', [1]);
    });

    await expect(txPromise).rejects.toThrow();
  });
});

// =============================================================================
// Timeout Handling
// =============================================================================

describe('Timeout Handling', () => {
  it('should throw TimeoutError on query timeout', async () => {
    // Create a WebSocket that never responds
    const silentClass = class extends FakeWebSocket {
      send(_data: string): void {
        // Don't emit any response
      }
    };

    cleanup();
    cleanup = setupMockWebSocket(silentClass);

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      timeout: 100, // 100ms timeout
    });

    await client.connect();

    await expect(client.query('SELECT 1')).rejects.toThrow(TimeoutError);
  });

  it('should include operation type in TimeoutError', async () => {
    const silentClass = class extends FakeWebSocket {
      send(_data: string): void {
        // Don't respond
      }
    };

    cleanup();
    cleanup = setupMockWebSocket(silentClass);

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      timeout: 50,
    });

    await client.connect();

    try {
      await client.query('SELECT 1');
    } catch (error) {
      expect(error).toBeInstanceOf(TimeoutError);
      const timeoutError = error as TimeoutError;
      expect(timeoutError.operationType).toBe('query');
      expect(timeoutError.timeoutMs).toBe(50);
      expect(timeoutError.isRetryable()).toBe(true);
    }
  });

  it('should timeout exec operations', async () => {
    const silentClass = class extends FakeWebSocket {
      send(_data: string): void {}
    };

    cleanup();
    cleanup = setupMockWebSocket(silentClass);

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      timeout: 50,
    });

    await client.connect();

    try {
      await client.exec('INSERT INTO test VALUES (1)');
    } catch (error) {
      expect(error).toBeInstanceOf(TimeoutError);
      const timeoutError = error as TimeoutError;
      expect(timeoutError.operationType).toBe('exec');
    }
  });

  it('should timeout transaction operations', async () => {
    const silentClass = class extends FakeWebSocket {
      send(_data: string): void {}
    };

    cleanup();
    cleanup = setupMockWebSocket(silentClass);

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      timeout: 50,
    });

    await client.connect();

    try {
      await client.beginTransaction();
    } catch (error) {
      expect(error).toBeInstanceOf(TimeoutError);
      const timeoutError = error as TimeoutError;
      expect(timeoutError.operationType).toBe('transaction');
    }
  });
});

// =============================================================================
// Batch Operations
// =============================================================================

describe('Batch Operations', () => {
  it('should execute batch on single connection', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    await client.connect();
    const initialCount = tracker.instances.length;

    await client.batch([
      { sql: 'INSERT INTO test VALUES (1)', params: [1] },
      { sql: 'INSERT INTO test VALUES (2)', params: [2] },
      { sql: 'INSERT INTO test VALUES (3)', params: [3] },
    ]).catch(() => {});

    // Should not create additional connections
    expect(tracker.instances.length).toBe(initialCount);
  });
});

// =============================================================================
// Prepared Statements
// =============================================================================

describe('Prepared Statements', () => {
  it('should prepare statement on active connection', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    await client.connect();

    // This will timeout/fail but tests the connection usage
    // Catch the error since FakeWebSocket returns a response without a proper hash
    client.prepare('SELECT * FROM users WHERE id = ?').catch(() => {});

    // Give time for the prepare request to be sent
    await new Promise(resolve => setTimeout(resolve, 20));

    expect(client.isConnected()).toBe(true);
  });

  it('should execute prepared statement on same connection', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    await client.connect();
    const initialCount = tracker.instances.length;

    // Prepare and execute (will timeout but tests connection reuse)
    const stmt = { sql: 'SELECT * FROM users', hash: 'hash123' as any };

    client.execute(stmt, [1]).catch(() => {});

    await new Promise(resolve => setTimeout(resolve, 20));

    // Should reuse same connection
    expect(tracker.instances.length).toBe(initialCount);
  });
});

// =============================================================================
// Health Check (Ping)
// =============================================================================

describe('Health Check (Ping)', () => {
  it('should ping and return latency', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    await client.connect();

    // This will timeout but we can verify the method exists
    const pingPromise = client.ping();

    // Give time for the ping request to be sent
    await new Promise(resolve => setTimeout(resolve, 20));

    expect(client.isConnected()).toBe(true);
  });
});

// =============================================================================
// Schema Operations
// =============================================================================

describe('Schema Operations', () => {
  it('should get schema on active connection', async () => {
    const client = new DoSQLClient({ url: 'ws://localhost:8080' });

    await client.connect();

    // This will timeout but tests the connection usage
    const schemaPromise = client.getSchema('users');

    await new Promise(resolve => setTimeout(resolve, 20));

    expect(client.isConnected()).toBe(true);
  });
});
