/**
 * Tests for lake.do WebSocketConnectionManager
 *
 * @module lake.do/tests/connection-manager
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { WebSocketConnectionManager, ConnectionError } from '../connection/manager.js';

// =============================================================================
// Mock WebSocket
// =============================================================================

class MockWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readyState = MockWebSocket.CONNECTING;
  url: string;
  private listeners: Map<string, Array<(event: unknown) => void>> = new Map();
  private _shouldFail = false;

  static instances: MockWebSocket[] = [];

  constructor(url: string) {
    this.url = url;
    MockWebSocket.instances.push(this);

    // Simulate connection
    if (!(globalThis as any).__mockWebSocketShouldFail) {
      setTimeout(() => {
        this.readyState = MockWebSocket.OPEN;
        this.emit('open', {});
      }, 0);
    } else {
      setTimeout(() => {
        this.emit('error', new Event('error'));
      }, 0);
    }
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

  send(data: string): void {
    // Mock send
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

  // Helper to simulate receiving a message
  simulateMessage(data: string | ArrayBuffer): void {
    this.emit('message', { data });
  }
}

describe('WebSocketConnectionManager', () => {
  let originalWebSocket: typeof globalThis.WebSocket;

  beforeEach(() => {
    // Save original WebSocket
    originalWebSocket = globalThis.WebSocket;
    // Mock WebSocket globally
    (globalThis as any).WebSocket = MockWebSocket;
    (globalThis as any).__mockWebSocketShouldFail = false;
    MockWebSocket.instances = [];
  });

  afterEach(() => {
    // Restore original WebSocket
    (globalThis as any).WebSocket = originalWebSocket;
    delete (globalThis as any).__mockWebSocketShouldFail;
  });

  describe('constructor', () => {
    it('creates manager with config', () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      expect(manager.url).toBe('https://example.com');
      expect(manager.isConnected).toBe(false);
    });
  });

  describe('url property', () => {
    it('returns configured URL', () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://lake.example.com/api',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      expect(manager.url).toBe('https://lake.example.com/api');
    });
  });

  describe('isConnected', () => {
    it('returns false when not connected', () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      expect(manager.isConnected).toBe(false);
    });

    it('returns true after successful connection', async () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      await manager.connect();

      expect(manager.isConnected).toBe(true);
    });

    it('returns false after closing', async () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      await manager.connect();
      await manager.close();

      expect(manager.isConnected).toBe(false);
    });
  });

  describe('connect', () => {
    it('establishes WebSocket connection', async () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      await manager.connect();

      expect(manager.isConnected).toBe(true);
      expect(MockWebSocket.instances).toHaveLength(1);
    });

    it('converts http to ws protocol', async () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      await manager.connect();

      expect(MockWebSocket.instances[0].url).toBe('wss://example.com');
    });

    it('does not reconnect if already connected', async () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      await manager.connect();
      await manager.connect();
      await manager.connect();

      expect(MockWebSocket.instances).toHaveLength(1);
    });

    it('emits connected event', async () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      const onConnected = vi.fn();
      manager.on('connected', onConnected);

      await manager.connect();

      expect(onConnected).toHaveBeenCalled();
    });

    it('rejects on connection error', async () => {
      (globalThis as any).__mockWebSocketShouldFail = true;

      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      await expect(manager.connect()).rejects.toThrow(ConnectionError);
    });
  });

  describe('close', () => {
    it('closes WebSocket connection', async () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      await manager.connect();
      await manager.close();

      expect(manager.isConnected).toBe(false);
    });

    it('emits disconnected event', async () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      const onDisconnected = vi.fn();
      manager.on('disconnected', onDisconnected);

      await manager.connect();
      await manager.close();

      expect(onDisconnected).toHaveBeenCalled();
    });

    it('does nothing when not connected', async () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      const onDisconnected = vi.fn();
      manager.on('disconnected', onDisconnected);

      await manager.close();

      expect(onDisconnected).not.toHaveBeenCalled();
    });
  });

  describe('send', () => {
    it('sends data over WebSocket', async () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      await manager.connect();

      const ws = MockWebSocket.instances[0];
      const sendSpy = vi.spyOn(ws, 'send');

      manager.send('test data');

      expect(sendSpy).toHaveBeenCalledWith('test data');
    });

    it('throws when not connected', () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      expect(() => manager.send('test')).toThrow(ConnectionError);
      expect(() => manager.send('test')).toThrow('WebSocket is not connected');
    });
  });

  describe('event handling', () => {
    describe('on', () => {
      it('registers event listener', async () => {
        const manager = new WebSocketConnectionManager({
          url: 'https://example.com',
          timeout: 30000,
          retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
        });

        const handler = vi.fn();
        manager.on('connected', handler);

        await manager.connect();

        expect(handler).toHaveBeenCalled();
      });
    });

    describe('off', () => {
      it('removes event listener', async () => {
        const manager = new WebSocketConnectionManager({
          url: 'https://example.com',
          timeout: 30000,
          retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
        });

        const handler = vi.fn();
        manager.on('connected', handler);
        manager.off('connected', handler);

        await manager.connect();

        expect(handler).not.toHaveBeenCalled();
      });
    });

    describe('once', () => {
      it('calls handler only once', async () => {
        const manager = new WebSocketConnectionManager({
          url: 'https://example.com',
          timeout: 30000,
          retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
        });

        const handler = vi.fn();
        manager.once('disconnected', handler);

        await manager.connect();
        await manager.close();
        await manager.connect();
        await manager.close();

        expect(handler).toHaveBeenCalledTimes(1);
      });
    });
  });

  describe('setMessageHandler', () => {
    it('calls message handler on incoming messages', async () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      const messageHandler = vi.fn();
      manager.setMessageHandler(messageHandler);

      await manager.connect();

      const ws = MockWebSocket.instances[0];
      ws.simulateMessage('{"test": "data"}');

      expect(messageHandler).toHaveBeenCalledWith('{"test": "data"}');
    });

    it('handles ArrayBuffer messages', async () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      const messageHandler = vi.fn();
      manager.setMessageHandler(messageHandler);

      await manager.connect();

      const ws = MockWebSocket.instances[0];
      const buffer = new ArrayBuffer(8);
      ws.simulateMessage(buffer);

      expect(messageHandler).toHaveBeenCalledWith(buffer);
    });
  });

  describe('pending request management', () => {
    it('registers and resolves pending requests', () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      const resolve = vi.fn();
      const reject = vi.fn();

      manager.registerRequest('req-1', resolve, reject, 5000);

      expect(manager.pendingRequestCount).toBe(1);

      const resolved = manager.resolveRequest('req-1', { success: true });

      expect(resolved).toBe(true);
      expect(resolve).toHaveBeenCalledWith({ success: true });
      expect(manager.pendingRequestCount).toBe(0);
    });

    it('registers and rejects pending requests', () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      const resolve = vi.fn();
      const reject = vi.fn();

      manager.registerRequest('req-1', resolve, reject, 5000);

      const error = new Error('Request failed');
      const rejected = manager.rejectRequest('req-1', error);

      expect(rejected).toBe(true);
      expect(reject).toHaveBeenCalledWith(error);
      expect(manager.pendingRequestCount).toBe(0);
    });

    it('returns false for unknown request IDs', () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      expect(manager.resolveRequest('unknown', {})).toBe(false);
      expect(manager.rejectRequest('unknown', new Error())).toBe(false);
    });

    it('times out pending requests', async () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      const resolve = vi.fn();
      const reject = vi.fn();

      manager.registerRequest('req-1', resolve, reject, 50);

      // Wait for timeout
      await new Promise(resolve => setTimeout(resolve, 100));

      expect(reject).toHaveBeenCalled();
      expect(reject.mock.calls[0][0].message).toContain('timeout');
      expect(manager.pendingRequestCount).toBe(0);
    });

    it('rejects pending requests on connection close', async () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      await manager.connect();

      const resolve = vi.fn();
      const reject = vi.fn();

      manager.registerRequest('req-1', resolve, reject, 30000);
      manager.registerRequest('req-2', resolve, reject, 30000);

      expect(manager.pendingRequestCount).toBe(2);

      // Simulate connection close
      const ws = MockWebSocket.instances[0];
      ws.emit('close', {});

      expect(reject).toHaveBeenCalledTimes(2);
      expect(manager.pendingRequestCount).toBe(0);
    });
  });

  describe('pendingRequestCount', () => {
    it('returns 0 when no pending requests', () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      expect(manager.pendingRequestCount).toBe(0);
    });

    it('tracks pending request count', () => {
      const manager = new WebSocketConnectionManager({
        url: 'https://example.com',
        timeout: 30000,
        retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
      });

      manager.registerRequest('req-1', vi.fn(), vi.fn(), 30000);
      manager.registerRequest('req-2', vi.fn(), vi.fn(), 30000);
      manager.registerRequest('req-3', vi.fn(), vi.fn(), 30000);

      expect(manager.pendingRequestCount).toBe(3);

      manager.resolveRequest('req-1', {});

      expect(manager.pendingRequestCount).toBe(2);
    });
  });

  describe('ConnectionError', () => {
    it('includes error code and message', () => {
      const error = new ConnectionError({
        code: 'CONNECTION_ERROR',
        message: 'Failed to connect',
      });

      expect(error.code).toBe('CONNECTION_ERROR');
      expect(error.message).toBe('Failed to connect');
      expect(error.name).toBe('ConnectionError');
    });

    it('masks URL with sensitive data', () => {
      const error = new ConnectionError(
        { code: 'CONNECTION_ERROR', message: 'Failed' },
        'https://user:password@example.com?token=secret'
      );

      expect(error.url).toContain('***');
      expect(error.url).not.toContain('password');
      expect(error.url).not.toContain('secret');
    });
  });
});
