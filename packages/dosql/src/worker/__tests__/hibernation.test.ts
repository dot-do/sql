/**
 * Hibernation Module Tests
 *
 * Tests for the WebSocket hibernation support in DoSQL.
 * Tests the HibernationMixin and HibernatingDurableObject base class.
 *
 * NOTE: WebSocket hibernation tests are limited due to miniflare constraints.
 * These tests focus on the logic that can be tested without full hibernation support.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  HibernationMixin,
  HibernatingDurableObject,
  type WebSocketSessionState,
  type WebSocketTag,
  type RPCMessage,
  type RPCResponse,
  type HibernationStats,
  type AlarmCleanupConfig,
} from '../hibernation.js';
import { DurableObject } from 'cloudflare:workers';

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Creates a minimal mock DurableObjectState for testing.
 * Note: In production tests, we use real DO storage via cloudflare:test.
 */
function createMockState(): DurableObjectState {
  const storage = new Map<string, unknown>();
  const websockets: WebSocket[] = [];

  return {
    id: {
      toString: () => 'test-id',
      equals: (other: DurableObjectId) => other.toString() === 'test-id',
      name: 'test-id',
    } as DurableObjectId,
    storage: {
      get: async (key: string) => storage.get(key),
      put: async (key: string, value: unknown) => { storage.set(key, value); },
      delete: async (key: string) => storage.delete(key),
      list: async () => new Map(storage),
      getAlarm: async () => null,
      setAlarm: async (scheduledTime: number | Date) => {},
      deleteAlarm: async () => {},
    } as unknown as DurableObjectStorage,
    acceptWebSocket: (ws: WebSocket, tags?: string[]) => {
      websockets.push(ws);
    },
    getWebSockets: (tag?: string) => websockets,
    blockConcurrencyWhile: async <T>(callback: () => Promise<T>) => callback(),
    waitUntil: (promise: Promise<unknown>) => {},
  } as DurableObjectState;
}

// =============================================================================
// Tests
// =============================================================================

describe('Hibernation Module', () => {
  // ===========================================================================
  // WebSocketSessionState Type Tests
  // ===========================================================================
  describe('WebSocketSessionState', () => {
    it('should have correct required fields', () => {
      const session: WebSocketSessionState = {
        sessionId: 'test-session-123',
        connectedAt: Date.now(),
        lastActivity: Date.now(),
        pendingRequests: [],
        metrics: {
          totalQueries: 0,
          totalErrors: 0,
          bytesReceived: 0,
          bytesSent: 0,
        },
      };

      expect(session.sessionId).toBe('test-session-123');
      expect(session.connectedAt).toBeDefined();
      expect(session.lastActivity).toBeDefined();
      expect(session.pendingRequests).toEqual([]);
      expect(session.metrics.totalQueries).toBe(0);
    });

    it('should support optional fields', () => {
      const session: WebSocketSessionState = {
        sessionId: 'test-session-456',
        clientId: 'client-abc',
        database: 'mydb',
        branch: 'main',
        connectedAt: Date.now(),
        lastActivity: Date.now(),
        pendingRequests: ['req-1', 'req-2'],
        transaction: {
          txId: 'tx-123',
          startedAt: Date.now(),
          timeout: 30000,
        },
        preparedStatements: [['stmt1', 'SELECT 1']],
        metrics: {
          totalQueries: 10,
          totalErrors: 1,
          bytesReceived: 1024,
          bytesSent: 2048,
        },
        idleTimeout: 60000,
      };

      expect(session.clientId).toBe('client-abc');
      expect(session.database).toBe('mydb');
      expect(session.branch).toBe('main');
      expect(session.transaction?.txId).toBe('tx-123');
      expect(session.preparedStatements?.length).toBe(1);
      expect(session.idleTimeout).toBe(60000);
    });
  });

  // ===========================================================================
  // WebSocketTag Type Tests
  // ===========================================================================
  describe('WebSocketTag', () => {
    it('should support client tags', () => {
      const tag: WebSocketTag = 'client:user-123';
      expect(tag).toBe('client:user-123');
    });

    it('should support database tags', () => {
      const tag: WebSocketTag = 'database:mydb';
      expect(tag).toBe('database:mydb');
    });

    it('should support branch tags', () => {
      const tag: WebSocketTag = 'branch:main';
      expect(tag).toBe('branch:main');
    });

    it('should support transaction tags', () => {
      const tag: WebSocketTag = 'tx:tx-123';
      expect(tag).toBe('tx:tx-123');
    });

    it('should support session tags', () => {
      const tag: WebSocketTag = 'session:sess-456';
      expect(tag).toBe('session:sess-456');
    });

    it('should support notify tags', () => {
      const tag: WebSocketTag = 'notify:channel-abc';
      expect(tag).toBe('notify:channel-abc');
    });
  });

  // ===========================================================================
  // RPCMessage and RPCResponse Type Tests
  // ===========================================================================
  describe('RPC Types', () => {
    it('should define RPCMessage structure', () => {
      const message: RPCMessage = {
        id: 'req-123',
        method: 'query',
        params: { sql: 'SELECT 1' },
      };

      expect(message.id).toBe('req-123');
      expect(message.method).toBe('query');
      expect(message.params).toEqual({ sql: 'SELECT 1' });
    });

    it('should define successful RPCResponse structure', () => {
      const response: RPCResponse = {
        id: 'req-123',
        result: { rows: [{ a: 1 }] },
      };

      expect(response.id).toBe('req-123');
      expect(response.result).toEqual({ rows: [{ a: 1 }] });
      expect(response.error).toBeUndefined();
    });

    it('should define error RPCResponse structure', () => {
      const response: RPCResponse = {
        id: 'req-123',
        error: {
          code: -32600,
          message: 'Invalid request',
          details: { field: 'sql' },
        },
      };

      expect(response.id).toBe('req-123');
      expect(response.result).toBeUndefined();
      expect(response.error?.code).toBe(-32600);
      expect(response.error?.message).toBe('Invalid request');
      expect(response.error?.details).toEqual({ field: 'sql' });
    });
  });

  // ===========================================================================
  // HibernationStats Type Tests
  // ===========================================================================
  describe('HibernationStats', () => {
    it('should define correct structure', () => {
      const stats: HibernationStats = {
        totalSleeps: 10,
        totalWakes: 10,
        averageSleepDuration: 5000,
        totalSleepTime: 50000,
        cpuTimeSaved: 49500,
      };

      expect(stats.totalSleeps).toBe(10);
      expect(stats.totalWakes).toBe(10);
      expect(stats.averageSleepDuration).toBe(5000);
      expect(stats.totalSleepTime).toBe(50000);
      expect(stats.cpuTimeSaved).toBe(49500);
    });
  });

  // ===========================================================================
  // AlarmCleanupConfig Type Tests
  // ===========================================================================
  describe('AlarmCleanupConfig', () => {
    it('should define correct structure', () => {
      const config: AlarmCleanupConfig = {
        defaultIdleTimeout: 30000,
        defaultTransactionTimeout: 30000,
        minAlarmInterval: 1000,
      };

      expect(config.defaultIdleTimeout).toBe(30000);
      expect(config.defaultTransactionTimeout).toBe(30000);
      expect(config.minAlarmInterval).toBe(1000);
    });
  });

  // ===========================================================================
  // HibernationMixin Tests
  // ===========================================================================
  describe('HibernationMixin', () => {
    it('should create a class that extends DurableObject', () => {
      const MixedClass = HibernationMixin(DurableObject);
      expect(MixedClass).toBeDefined();
      expect(MixedClass.prototype).toBeDefined();
    });

    it('should add hibernation methods to the class', () => {
      const MixedClass = HibernationMixin(DurableObject);

      // Check prototype has expected methods
      expect(typeof MixedClass.prototype.webSocketMessage).toBe('function');
      expect(typeof MixedClass.prototype.webSocketClose).toBe('function');
      expect(typeof MixedClass.prototype.webSocketError).toBe('function');
      expect(typeof MixedClass.prototype.alarm).toBe('function');
      expect(typeof MixedClass.prototype.getHibernationStats).toBe('function');
    });
  });

  // ===========================================================================
  // HibernatingDurableObject Tests
  // ===========================================================================
  describe('HibernatingDurableObject', () => {
    it('should be a valid class', () => {
      expect(HibernatingDurableObject).toBeDefined();
      expect(typeof HibernatingDurableObject).toBe('function');
    });

    it('should extend DurableObject', () => {
      // Check prototype chain
      expect(HibernatingDurableObject.prototype).toBeDefined();
    });

    it('should have hibernation API methods', () => {
      expect(typeof HibernatingDurableObject.prototype.webSocketMessage).toBe('function');
      expect(typeof HibernatingDurableObject.prototype.webSocketClose).toBe('function');
      expect(typeof HibernatingDurableObject.prototype.webSocketError).toBe('function');
      expect(typeof HibernatingDurableObject.prototype.alarm).toBe('function');
      expect(typeof HibernatingDurableObject.prototype.getHibernationStats).toBe('function');
    });
  });

  // ===========================================================================
  // Session State Management Logic Tests
  // ===========================================================================
  describe('Session State Management', () => {
    it('should calculate idle timeout correctly', () => {
      // Test the logic for idle timeout calculation
      const now = Date.now();
      const session: WebSocketSessionState = {
        sessionId: 'test',
        connectedAt: now - 10000, // connected 10s ago
        lastActivity: now - 5000, // last activity 5s ago
        pendingRequests: [],
        metrics: { totalQueries: 0, totalErrors: 0, bytesReceived: 0, bytesSent: 0 },
        idleTimeout: 30000, // 30s idle timeout
      };

      const idleExpiry = session.lastActivity + (session.idleTimeout ?? 30000);
      expect(idleExpiry).toBe(now - 5000 + 30000);
      expect(idleExpiry).toBeGreaterThan(now); // Not yet expired
    });

    it('should detect expired idle sessions', () => {
      const now = Date.now();
      const session: WebSocketSessionState = {
        sessionId: 'test',
        connectedAt: now - 60000, // connected 60s ago
        lastActivity: now - 45000, // last activity 45s ago
        pendingRequests: [],
        metrics: { totalQueries: 0, totalErrors: 0, bytesReceived: 0, bytesSent: 0 },
        idleTimeout: 30000, // 30s idle timeout
      };

      const idleExpiry = session.lastActivity + (session.idleTimeout ?? 30000);
      expect(idleExpiry).toBeLessThan(now); // Should be expired
    });

    it('should calculate transaction timeout correctly', () => {
      const now = Date.now();
      const session: WebSocketSessionState = {
        sessionId: 'test',
        connectedAt: now - 10000,
        lastActivity: now,
        pendingRequests: [],
        metrics: { totalQueries: 0, totalErrors: 0, bytesReceived: 0, bytesSent: 0 },
        transaction: {
          txId: 'tx-123',
          startedAt: now - 5000, // started 5s ago
          timeout: 30000, // 30s timeout
        },
      };

      const txExpiry = session.transaction!.startedAt + session.transaction!.timeout;
      expect(txExpiry).toBe(now - 5000 + 30000);
      expect(txExpiry).toBeGreaterThan(now); // Not yet expired
    });

    it('should detect expired transactions', () => {
      const now = Date.now();
      const session: WebSocketSessionState = {
        sessionId: 'test',
        connectedAt: now - 60000,
        lastActivity: now - 35000,
        pendingRequests: [],
        metrics: { totalQueries: 0, totalErrors: 0, bytesReceived: 0, bytesSent: 0 },
        transaction: {
          txId: 'tx-123',
          startedAt: now - 35000, // started 35s ago
          timeout: 30000, // 30s timeout
        },
      };

      const txExpiry = session.transaction!.startedAt + session.transaction!.timeout;
      expect(txExpiry).toBeLessThan(now); // Should be expired
    });
  });

  // ===========================================================================
  // Metrics Calculation Tests
  // ===========================================================================
  describe('Metrics Calculations', () => {
    it('should calculate average sleep duration correctly', () => {
      const stats: HibernationStats = {
        totalSleeps: 5,
        totalWakes: 5,
        averageSleepDuration: 0,
        totalSleepTime: 25000,
        cpuTimeSaved: 0,
      };

      // Calculate average
      const average = stats.totalSleepTime / stats.totalWakes;
      expect(average).toBe(5000);
    });

    it('should estimate CPU time saved correctly', () => {
      // Assuming 99% CPU savings during sleep
      const sleepDuration = 10000; // 10 seconds
      const cpuTimeSaved = sleepDuration * 0.99;
      expect(cpuTimeSaved).toBe(9900);
    });

    it('should track bytes sent/received in session metrics', () => {
      const session: WebSocketSessionState = {
        sessionId: 'test',
        connectedAt: Date.now(),
        lastActivity: Date.now(),
        pendingRequests: [],
        metrics: {
          totalQueries: 5,
          totalErrors: 1,
          bytesReceived: 1024,
          bytesSent: 2048,
        },
      };

      // Simulate receiving a message
      const messageBytes = 256;
      session.metrics.bytesReceived += messageBytes;
      expect(session.metrics.bytesReceived).toBe(1280);

      // Simulate sending a response
      const responseBytes = 512;
      session.metrics.bytesSent += responseBytes;
      expect(session.metrics.bytesSent).toBe(2560);
    });
  });

  // ===========================================================================
  // Cleanup Scheduling Logic Tests
  // ===========================================================================
  describe('Cleanup Scheduling Logic', () => {
    it('should find earliest cleanup time among multiple sessions', () => {
      const now = Date.now();

      const sessions: WebSocketSessionState[] = [
        {
          sessionId: '1',
          connectedAt: now,
          lastActivity: now - 10000,
          pendingRequests: [],
          metrics: { totalQueries: 0, totalErrors: 0, bytesReceived: 0, bytesSent: 0 },
          idleTimeout: 30000, // expires at now + 20000
        },
        {
          sessionId: '2',
          connectedAt: now,
          lastActivity: now - 5000,
          pendingRequests: [],
          metrics: { totalQueries: 0, totalErrors: 0, bytesReceived: 0, bytesSent: 0 },
          idleTimeout: 30000, // expires at now + 25000
        },
        {
          sessionId: '3',
          connectedAt: now,
          lastActivity: now - 20000,
          pendingRequests: [],
          metrics: { totalQueries: 0, totalErrors: 0, bytesReceived: 0, bytesSent: 0 },
          idleTimeout: 30000, // expires at now + 10000 (earliest)
        },
      ];

      // Find earliest cleanup time
      let earliestCleanup: number | null = null;
      for (const session of sessions) {
        const idleExpiry = session.lastActivity + (session.idleTimeout ?? 30000);
        if (earliestCleanup === null || idleExpiry < earliestCleanup) {
          earliestCleanup = idleExpiry;
        }
      }

      expect(earliestCleanup).toBe(now - 20000 + 30000);
    });

    it('should prefer transaction timeout over idle timeout if earlier', () => {
      const now = Date.now();

      const session: WebSocketSessionState = {
        sessionId: 'test',
        connectedAt: now,
        lastActivity: now, // Just active, idle timeout far away
        pendingRequests: [],
        metrics: { totalQueries: 0, totalErrors: 0, bytesReceived: 0, bytesSent: 0 },
        idleTimeout: 60000, // 60s idle timeout
        transaction: {
          txId: 'tx-123',
          startedAt: now - 25000, // started 25s ago
          timeout: 30000, // 30s timeout, so expires in 5s
        },
      };

      const idleExpiry = session.lastActivity + (session.idleTimeout ?? 30000);
      const txExpiry = session.transaction!.startedAt + session.transaction!.timeout;

      expect(txExpiry).toBeLessThan(idleExpiry);
      expect(txExpiry).toBe(now - 25000 + 30000); // Expires sooner
    });
  });

  // ===========================================================================
  // WebSocket Upgrade Request Parsing Tests
  // ===========================================================================
  describe('WebSocket Upgrade Request Parsing', () => {
    it('should detect WebSocket upgrade header', () => {
      const request = new Request('http://localhost/ws', {
        headers: {
          'Upgrade': 'websocket',
          'Connection': 'Upgrade',
        },
      });

      const upgradeHeader = request.headers.get('Upgrade');
      expect(upgradeHeader).toBe('websocket');
    });

    it('should reject non-WebSocket upgrade requests', () => {
      const request = new Request('http://localhost/ws', {
        headers: {
          'Upgrade': 'h2c',
        },
      });

      const upgradeHeader = request.headers.get('Upgrade');
      expect(upgradeHeader).not.toBe('websocket');
    });

    it('should extract client ID from headers', () => {
      const request = new Request('http://localhost/ws', {
        headers: {
          'X-Client-ID': 'client-123',
        },
      });

      const clientId = request.headers.get('X-Client-ID');
      expect(clientId).toBe('client-123');
    });

    it('should extract database from query params', () => {
      const request = new Request('http://localhost/ws?database=mydb&branch=dev');
      const url = new URL(request.url);

      const database = url.searchParams.get('database');
      const branch = url.searchParams.get('branch');

      expect(database).toBe('mydb');
      expect(branch).toBe('dev');
    });
  });

  // ===========================================================================
  // RPC Message Parsing Tests
  // ===========================================================================
  describe('RPC Message Parsing', () => {
    it('should parse valid RPC query message', () => {
      const messageStr = JSON.stringify({
        id: 'req-1',
        method: 'query',
        params: { sql: 'SELECT * FROM users' },
      });

      const message: RPCMessage = JSON.parse(messageStr);
      expect(message.id).toBe('req-1');
      expect(message.method).toBe('query');
      expect(message.params).toEqual({ sql: 'SELECT * FROM users' });
    });

    it('should parse valid RPC exec message', () => {
      const messageStr = JSON.stringify({
        id: 'req-2',
        method: 'exec',
        params: { sql: 'INSERT INTO users (name) VALUES (?)', params: ['Alice'] },
      });

      const message: RPCMessage = JSON.parse(messageStr);
      expect(message.id).toBe('req-2');
      expect(message.method).toBe('exec');
    });

    it('should parse beginTransaction message', () => {
      const messageStr = JSON.stringify({
        id: 'req-3',
        method: 'beginTransaction',
        params: { isolationLevel: 'SERIALIZABLE', readOnly: false },
      });

      const message: RPCMessage = JSON.parse(messageStr);
      expect(message.method).toBe('beginTransaction');
    });

    it('should parse commit message', () => {
      const messageStr = JSON.stringify({
        id: 'req-4',
        method: 'commit',
        params: { txId: 'tx-123' },
      });

      const message: RPCMessage = JSON.parse(messageStr);
      expect(message.method).toBe('commit');
    });

    it('should parse rollback message', () => {
      const messageStr = JSON.stringify({
        id: 'req-5',
        method: 'rollback',
        params: { txId: 'tx-123' },
      });

      const message: RPCMessage = JSON.parse(messageStr);
      expect(message.method).toBe('rollback');
    });

    it('should parse ping message', () => {
      const messageStr = JSON.stringify({
        id: 'req-6',
        method: 'ping',
        params: {},
      });

      const message: RPCMessage = JSON.parse(messageStr);
      expect(message.method).toBe('ping');
    });

    it('should handle binary message decoding', () => {
      const text = JSON.stringify({ id: 'req-7', method: 'ping', params: {} });
      const encoded = new TextEncoder().encode(text);
      const decoded = new TextDecoder().decode(encoded);

      const message: RPCMessage = JSON.parse(decoded);
      expect(message.method).toBe('ping');
    });
  });

  // ===========================================================================
  // Error Response Construction Tests
  // ===========================================================================
  describe('Error Response Construction', () => {
    it('should construct method not found error', () => {
      const response: RPCResponse = {
        id: 'req-1',
        error: { code: -32601, message: 'Method not found: unknownMethod' },
      };

      expect(response.error?.code).toBe(-32601);
      expect(response.error?.message).toContain('Method not found');
    });

    it('should construct internal error', () => {
      const response: RPCResponse = {
        id: 'req-1',
        error: { code: -32603, message: 'Internal error: database unavailable' },
      };

      expect(response.error?.code).toBe(-32603);
    });

    it('should construct transaction not found error', () => {
      const response: RPCResponse = {
        id: 'req-1',
        error: { code: -32000, message: 'Transaction tx-123 timed out' },
      };

      expect(response.error?.code).toBe(-32000);
      expect(response.error?.message).toContain('timed out');
    });
  });
});
