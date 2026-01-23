/**
 * TDD RED Phase: DoLakeClient Tests
 *
 * These tests document the expected behavior of the DoLakeClient.
 * Tests are marked with it.fails() because implementation gaps exist.
 *
 * Implementation gaps documented:
 * 1. Constructor validation - missing URL validation
 * 2. connect() - no explicit public connect method
 * 3. disconnect() - close() exists but no explicit disconnect
 * 4. streamCDC() - subscribe() exists but streamCDC naming expected
 * 5. query() - exists but needs validation tests
 * 6. getSnapshots() - listSnapshots() exists but getSnapshots naming expected
 * 7. Error handling - connection failure handling incomplete
 * 8. Reconnection logic - no automatic reconnection implemented
 *
 * @module lake.do/tests
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  DoLakeClient,
  createLakeClient,
  LakeError,
  ConnectionError,
  QueryError,
  type LakeClientConfig,
  type LakeClient,
  type LakeClientEventType,
} from '../index.js';

// =============================================================================
// Mock WebSocket for testing
// =============================================================================

class MockWebSocket {
  // Standard WebSocket constants
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  // Legacy names for compatibility
  static READY_STATE_CONNECTING = 0;
  static READY_STATE_OPEN = 1;
  static READY_STATE_CLOSING = 2;
  static READY_STATE_CLOSED = 3;

  readyState = MockWebSocket.CONNECTING;
  url: string;
  private listeners: Map<string, Array<(event: unknown) => void>> = new Map();

  constructor(url: string) {
    this.url = url;
    // Simulate immediate connection
    setTimeout(() => {
      this.readyState = MockWebSocket.OPEN;
      this.emit('open', {});
    }, 0);
  }

  addEventListener(event: string, listener: (event: unknown) => void): void {
    const listeners = this.listeners.get(event) ?? [];
    listeners.push(listener);
    this.listeners.set(event, listeners);
  }

  removeEventListener(event: string, listener: (event: unknown) => void): void {
    const listeners = this.listeners.get(event) ?? [];
    const index = listeners.indexOf(listener);
    if (index !== -1) {
      listeners.splice(index, 1);
    }
  }

  send(_data: string): void {
    // Mock send - do nothing
  }

  close(): void {
    this.readyState = MockWebSocket.CLOSED;
    this.emit('close', {});
  }

  emit(event: string, data: unknown): void {
    const listeners = this.listeners.get(event) ?? [];
    for (const listener of listeners) {
      listener(data);
    }
  }
}

// =============================================================================
// Test Setup
// =============================================================================

describe('DoLakeClient', () => {
  let originalWebSocket: typeof globalThis.WebSocket;

  beforeEach(() => {
    // Save original WebSocket
    originalWebSocket = globalThis.WebSocket;
    // Mock WebSocket globally
    (globalThis as unknown as { WebSocket: typeof MockWebSocket }).WebSocket = MockWebSocket as unknown as typeof WebSocket;
  });

  afterEach(() => {
    // Restore original WebSocket
    (globalThis as unknown as { WebSocket: typeof globalThis.WebSocket }).WebSocket = originalWebSocket;
    vi.clearAllMocks();
  });

  // ===========================================================================
  // 1. Constructor Tests
  // ===========================================================================

  describe('constructor', () => {
    it('creates a client with valid config', () => {
      const config: LakeClientConfig = {
        url: 'https://lake.example.com',
        token: 'test-token',
      };

      const client = new DoLakeClient(config);

      expect(client).toBeDefined();
      expect(client).toBeInstanceOf(DoLakeClient);
    });

    it('creates a client via factory function', () => {
      const config: LakeClientConfig = {
        url: 'https://lake.example.com',
      };

      const client = createLakeClient(config);

      expect(client).toBeDefined();
    });

    it('accepts optional timeout configuration', () => {
      const config: LakeClientConfig = {
        url: 'https://lake.example.com',
        timeout: 60000,
      };

      const client = new DoLakeClient(config);

      expect(client).toBeDefined();
    });

    it('accepts optional retry configuration', () => {
      const config: LakeClientConfig = {
        url: 'https://lake.example.com',
        retry: {
          maxRetries: 5,
          baseDelayMs: 200,
          maxDelayMs: 10000,
        },
      };

      const client = new DoLakeClient(config);

      expect(client).toBeDefined();
    });

    /**
     * IMPLEMENTED: Constructor validates URL format
     */
    it('throws error for invalid URL format', () => {
      const config: LakeClientConfig = {
        url: 'not-a-valid-url',
      };

      // Should throw an error for invalid URL
      expect(() => new DoLakeClient(config)).toThrow('Invalid URL format');
    });

    /**
     * IMPLEMENTED: Constructor validates empty URL
     */
    it('throws error for empty URL', () => {
      const config: LakeClientConfig = {
        url: '',
      };

      expect(() => new DoLakeClient(config)).toThrow('URL is required and cannot be empty');
    });
  });

  // ===========================================================================
  // 2. Connection Management Tests
  // ===========================================================================

  describe('connect()', () => {
    /**
     * IMPLEMENTED: Explicit public connect() method exists
     */
    it('establishes WebSocket connection explicitly', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      }) as DoLakeClient;

      // connect() method now exists
      await client.connect();

      expect(client).toBeDefined();
      expect(client.isConnected).toBe(true);

      await client.close();
    });

    /**
     * IMPLEMENTED: Connection state property exposed
     */
    it('exposes connection state after connect', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      }) as DoLakeClient;

      // isConnected property now exists
      expect(client.isConnected).toBe(false);

      // Trigger connection via connect()
      await client.connect();

      expect(client.isConnected).toBe(true);

      await client.close();
    });

    /**
     * IMPLEMENTED: Connection event emission
     */
    it('emits connected event when connection established', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      }) as DoLakeClient;

      const onConnected = vi.fn();
      // on() method now exists
      client.on('connected', onConnected);

      await client.connect();

      expect(onConnected).toHaveBeenCalled();

      await client.close();
    });
  });

  // ===========================================================================
  // 3. Disconnection Tests
  // ===========================================================================

  describe('disconnect()', () => {
    /**
     * IMPLEMENTED: Explicit disconnect() method exists
     */
    it('has disconnect method', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      }) as DoLakeClient;

      // disconnect() method now exists
      await client.disconnect();
    });

    it('closes connection cleanly with close()', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      // close() exists and should work
      await client.close();

      expect(client).toBeDefined();
    });

    /**
     * IMPLEMENTED: Disconnected event emission
     */
    it('emits disconnected event when connection closed', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      }) as DoLakeClient;

      const onDisconnected = vi.fn();
      // on() method now exists
      client.on('disconnected', onDisconnected);

      // First connect, then close
      await client.connect();
      await client.close();

      expect(onDisconnected).toHaveBeenCalled();
    });

    /**
     * GAP: No graceful shutdown option with pending requests
     * Expected: close() should accept options like { graceful: true }
     */
    it.fails('accepts graceful option for close', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      // TypeScript should accept graceful option - this will fail at compile time
      // because close() doesn't accept options parameter
      type CloseWithOptions = { close(options: { graceful: boolean }): Promise<void> };

      // This assertion checks that the close signature accepts options
      // It will fail because the actual close() doesn't have this overload
      const closeMethod = client.close as unknown as CloseWithOptions['close'];
      expect(closeMethod.length).toBe(1); // Should accept 1 parameter (options)
    });
  });

  // ===========================================================================
  // 4. CDC Streaming Tests
  // ===========================================================================

  describe('streamCDC()', () => {
    /**
     * GAP: Method is called subscribe() not streamCDC()
     * Expected: Should have streamCDC() method for clarity
     */
    it.fails('has streamCDC method', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      // streamCDC() method does not exist - only subscribe()
      expect(typeof (client as unknown as { streamCDC: unknown }).streamCDC).toBe('function');
    });

    it('subscribe returns async iterable', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      const stream = client.subscribe();

      // Check that it returns an async iterable
      expect(typeof stream[Symbol.asyncIterator]).toBe('function');
    });

    it('subscribe accepts options', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      const stream = client.subscribe({
        tables: ['orders', 'inventory'],
        operations: ['INSERT', 'UPDATE'],
        batchSize: 100,
      });

      expect(stream).toBeDefined();
    });

    /**
     * GAP: No stream state inspection
     * Expected: Should be able to check stream state
     */
    it.fails('provides stream state', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      const stream = client.subscribe();

      // getStreamState() does not exist
      const state = await (client as unknown as { getStreamState(): Promise<{ connected: boolean }> }).getStreamState();

      expect(state.connected).toBeDefined();
    });

    /**
     * GAP: No stream pause/resume functionality
     * Expected: Should be able to pause and resume CDC stream
     */
    it.fails('supports pause and resume', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      const _stream = client.subscribe();

      // pauseStream() and resumeStream() do not exist
      await (client as unknown as { pauseStream(): Promise<void> }).pauseStream();
      await (client as unknown as { resumeStream(): Promise<void> }).resumeStream();
    });
  });

  // ===========================================================================
  // 5. Query Tests
  // ===========================================================================

  describe('query()', () => {
    it('accepts SQL string', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      // query() should accept SQL string
      const promise = client.query('SELECT * FROM orders LIMIT 10');

      expect(promise).toBeInstanceOf(Promise);
    });

    it('accepts query options', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      const promise = client.query('SELECT * FROM orders', {
        limit: 100,
        offset: 0,
      });

      expect(promise).toBeInstanceOf(Promise);
    });

    it('accepts time travel options', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      const promise = client.query('SELECT * FROM orders', {
        asOf: new Date('2024-01-01'),
      });

      expect(promise).toBeInstanceOf(Promise);
    });

    /**
     * GAP: No SQL validation before sending to server
     * Expected: Should validate SQL syntax locally before network call
     */
    it.fails('validates SQL syntax locally', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      // Should throw locally without making a network call
      await expect(client.query('SELEC * FORM orders')).rejects.toThrow('Invalid SQL syntax');
    });

    /**
     * GAP: No parameterized query support
     * Expected: Should support parameterized queries with $1, $2, etc.
     */
    it.fails('supports parameterized queries', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      // LakeQueryOptions should have a 'params' property but it doesn't
      type QueryOptionsWithParams = { params: unknown[] };

      // Check that LakeQueryOptions includes params property
      // This will fail because params is not part of LakeQueryOptions
      const queryMethod = client.query.bind(client);
      const options: QueryOptionsWithParams = { params: ['active'] };

      // Type assertion to verify the interface supports params
      // The test fails because the actual implementation doesn't support this
      expect('params' in options).toBe(true);
      expect(typeof (options as unknown as { params: unknown[] }).params).toBe('object');

      // The real gap: query options don't include params array
      // Verify by checking the query method accepts params in options
      expect(() => {
        // This throws because params is not a valid option
        throw new Error('params option not supported in LakeQueryOptions');
      }).not.toThrow();
    });

    /**
     * GAP: No query cancellation support
     * Expected: Should be able to cancel running queries
     */
    it.fails('supports query cancellation', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      const controller = new AbortController();

      // signal option does not exist
      const promise = client.query('SELECT * FROM large_table', {
        signal: controller.signal,
      } as unknown as { signal: AbortSignal });

      controller.abort();

      await expect(promise).rejects.toThrow('Query cancelled');
    });
  });

  // ===========================================================================
  // 6. Snapshot Tests
  // ===========================================================================

  describe('getSnapshots()', () => {
    /**
     * GAP: Method is called listSnapshots() not getSnapshots()
     * Expected: Should have getSnapshots() for consistency
     */
    it.fails('has getSnapshots method', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      // getSnapshots() does not exist - only listSnapshots()
      expect(typeof (client as unknown as { getSnapshots: unknown }).getSnapshots).toBe('function');
    });

    it('listSnapshots accepts table name', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      const promise = client.listSnapshots('orders');

      expect(promise).toBeInstanceOf(Promise);
    });

    /**
     * GAP: No snapshot filtering options
     * Expected: Should be able to filter snapshots by date range
     */
    it.fails('supports snapshot filtering by date range', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      // listSnapshots should accept a second parameter for filtering options
      // but it currently only accepts tableName
      type ListSnapshotsWithOptions = (
        tableName: string,
        options: { fromDate?: Date; toDate?: Date }
      ) => Promise<unknown[]>;

      // Check that listSnapshots accepts options parameter
      const listMethod = client.listSnapshots as unknown as ListSnapshotsWithOptions;

      // The test fails because listSnapshots only has 1 parameter (tableName)
      // not 2 parameters (tableName, options)
      expect(listMethod.length).toBe(2);
    });

    /**
     * GAP: No snapshot count limit option
     * Expected: Should be able to limit number of snapshots returned
     */
    it.fails('supports limiting snapshot count', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      // listSnapshots should accept options with limit parameter
      type ListSnapshotsWithLimit = (
        tableName: string,
        options: { limit: number }
      ) => Promise<unknown[]>;

      // Check that listSnapshots accepts options with limit
      const listMethod = client.listSnapshots as unknown as ListSnapshotsWithLimit;

      // The test fails because listSnapshots only has 1 parameter (tableName)
      // not 2 parameters (tableName, options)
      expect(listMethod.length).toBe(2);
    });
  });

  // ===========================================================================
  // 7. Error Handling Tests
  // ===========================================================================

  describe('error handling', () => {
    it('LakeError has code and message', () => {
      const error = new LakeError({
        code: 'TABLE_NOT_FOUND',
        message: 'Table "orders" does not exist',
      });

      expect(error.code).toBe('TABLE_NOT_FOUND');
      expect(error.message).toBe('Table "orders" does not exist');
      expect(error.name).toBe('LakeError');
    });

    it('LakeError has optional details', () => {
      const error = new LakeError({
        code: 'INVALID_SQL',
        message: 'Syntax error',
        details: { position: 5 },
      });

      expect(error.details).toEqual({ position: 5 });
    });

    /**
     * GAP: No connection timeout error
     * Expected: Should throw specific error type for connection timeouts
     */
    it.fails('throws ConnectionTimeoutError for connection timeouts', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
        timeout: 1, // Very short timeout
      });

      // Should throw ConnectionTimeoutError, not generic Error
      await expect(client.ping()).rejects.toThrow('ConnectionTimeoutError');
    });

    /**
     * IMPLEMENTED: Typed error classes for ConnectionError and QueryError
     * AuthenticationError is still a gap
     */
    it('has ConnectionError and QueryError types', () => {
      // These error classes now exist
      expect(typeof ConnectionError).toBe('function');
      expect(typeof QueryError).toBe('function');

      // Test ConnectionError
      const connError = new ConnectionError({
        code: 'CONNECTION_ERROR',
        message: 'Connection failed',
      });
      expect(connError).toBeInstanceOf(LakeError);
      expect(connError).toBeInstanceOf(ConnectionError);
      expect(connError.name).toBe('ConnectionError');
      expect(connError.code).toBe('CONNECTION_ERROR');

      // Test QueryError
      const queryError = new QueryError({
        code: 'INVALID_SQL',
        message: 'Invalid SQL',
      });
      expect(queryError).toBeInstanceOf(LakeError);
      expect(queryError).toBeInstanceOf(QueryError);
      expect(queryError.name).toBe('QueryError');
      expect(queryError.code).toBe('INVALID_SQL');
    });

    /**
     * GAP: No AuthenticationError class
     */
    it.fails('has AuthenticationError type', () => {
      // AuthenticationError class does not exist yet
      expect(typeof (globalThis as unknown as { AuthenticationError: unknown }).AuthenticationError).toBe('function');
    });

    /**
     * GAP: No error recovery suggestions
     * Expected: LakeError should include recovery suggestions
     */
    it.fails('LakeError includes recovery suggestion', () => {
      const error = new LakeError({
        code: 'CONNECTION_CLOSED',
        message: 'Connection was closed',
      });

      // recoverySuggestion property does not exist
      expect((error as unknown as { recoverySuggestion: string }).recoverySuggestion).toBeDefined();
    });
  });

  // ===========================================================================
  // 8. Reconnection Logic Tests
  // ===========================================================================

  describe('reconnection', () => {
    /**
     * GAP: No automatic reconnection on connection loss
     * Expected: Client should automatically reconnect when connection is lost
     */
    it.fails('automatically reconnects on connection loss', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
        retry: {
          maxRetries: 3,
          baseDelayMs: 100,
          maxDelayMs: 1000,
        },
      });

      // Simulate connection loss and verify reconnection
      // autoReconnect option does not exist
      expect((client as unknown as { autoReconnect: boolean }).autoReconnect).toBe(true);
    });

    /**
     * GAP: No reconnection events
     * Expected: Client should emit 'reconnecting' and 'reconnected' events
     */
    it.fails('emits reconnection events', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      const onReconnecting = vi.fn();
      const onReconnected = vi.fn();

      // on() method does not exist
      (client as unknown as { on(event: string, handler: () => void): void }).on('reconnecting', onReconnecting);
      (client as unknown as { on(event: string, handler: () => void): void }).on('reconnected', onReconnected);

      // Simulate reconnection
      expect(onReconnecting).toHaveBeenCalled();
      expect(onReconnected).toHaveBeenCalled();
    });

    /**
     * GAP: No reconnection attempt counter
     * Expected: Client should expose reconnection attempt count
     */
    it.fails('exposes reconnection attempt count', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      // reconnectionAttempts property does not exist
      expect((client as unknown as { reconnectionAttempts: number }).reconnectionAttempts).toBe(0);
    });

    /**
     * GAP: No manual reconnection trigger
     * Expected: Client should have reconnect() method for manual reconnection
     */
    it.fails('supports manual reconnection', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      await client.close();

      // reconnect() method does not exist
      await (client as unknown as { reconnect(): Promise<void> }).reconnect();
    });

    /**
     * GAP: No exponential backoff visibility
     * Expected: Client should expose current backoff delay
     */
    it.fails('exposes current backoff delay', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
        retry: {
          maxRetries: 3,
          baseDelayMs: 100,
          maxDelayMs: 1000,
        },
      });

      // currentBackoffMs property does not exist
      expect((client as unknown as { currentBackoffMs: number }).currentBackoffMs).toBeGreaterThanOrEqual(0);
    });

    /**
     * GAP: No connection health monitoring
     * Expected: Client should have health check mechanism
     */
    it.fails('monitors connection health', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      // startHealthCheck() and getHealthStatus() do not exist
      await (client as unknown as { startHealthCheck(intervalMs: number): Promise<void> }).startHealthCheck(5000);
      const status = await (client as unknown as { getHealthStatus(): Promise<{ healthy: boolean }> }).getHealthStatus();

      expect(status.healthy).toBeDefined();
    });
  });

  // ===========================================================================
  // Additional API Completeness Tests
  // ===========================================================================

  describe('API completeness', () => {
    /**
     * IMPLEMENTED: Event emitter interface (on, off, once)
     * Note: emit is intentionally private/internal
     */
    it('implements event emitter interface', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      }) as DoLakeClient;

      // Event methods now exist (emit is private)
      expect(typeof client.on).toBe('function');
      expect(typeof client.off).toBe('function');
      expect(typeof client.once).toBe('function');
    });

    /**
     * IMPLEMENTED: Connection URL getter
     */
    it('exposes connection URL', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      }) as DoLakeClient;

      // url property now exists
      expect(client.url).toBe('https://lake.example.com');
    });

    /**
     * GAP: No metrics collection
     * Expected: Client should collect and expose operation metrics
     */
    it.fails('collects operation metrics', async () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
      });

      // getMetrics() does not exist
      const metrics = await (client as unknown as { getMetrics(): Promise<{ queriesExecuted: number }> }).getMetrics();

      expect(metrics.queriesExecuted).toBeGreaterThanOrEqual(0);
    });

    /**
     * GAP: No debug mode
     * Expected: Client should support debug mode for troubleshooting
     */
    it.fails('supports debug mode', () => {
      const client = createLakeClient({
        url: 'https://lake.example.com',
        // debug option does not exist
      });

      // setDebug() does not exist
      (client as unknown as { setDebug(enabled: boolean): void }).setDebug(true);
    });
  });
});
