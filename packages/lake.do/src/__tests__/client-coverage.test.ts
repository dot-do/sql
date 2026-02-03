/**
 * DoLakeClient Extended Coverage Tests
 *
 * Tests for implemented features that were previously documented as GAPs.
 * Covers: constructor validation, getConfig, connection management, event system,
 * error handling, retryability, branded type factories, constants, and transformers
 * edge cases.
 *
 * Issue: sql-3129
 *
 * @packageDocumentation
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  DoLakeClient,
  createLakeClient,
  LakeError,
  ConnectionError,
  QueryError,
  type LakeClientConfig,
} from '../client.js';
import { TimeoutError, ConfigurationError } from '../errors.js';
import {
  ErrorCode,
  WebSocketState,
  DEFAULT_TIMEOUT_MS,
  DEFAULT_MAX_QUEUE_SIZE,
  MIN_QUEUE_SIZE,
} from '../constants.js';
import {
  isValidStringId,
  isValidDateLike,
  transformSnapshot,
  transformPartitionInfo,
  transformCompactionJob,
  transformTableMetadata,
  type RawSnapshot,
  type RawPartitionInfo,
  type RawCompactionJob,
  type RawTableMetadata,
} from '../transformers.js';

// =============================================================================
// Mock WebSocket
// =============================================================================

class MockWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  static READY_STATE_CONNECTING = 0;
  static READY_STATE_OPEN = 1;
  static READY_STATE_CLOSING = 2;
  static READY_STATE_CLOSED = 3;

  readyState = MockWebSocket.CONNECTING;
  url: string;
  private listeners: Map<string, Array<(event: unknown) => void>> = new Map();

  constructor(url: string) {
    this.url = url;
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
}

// =============================================================================
// Test Setup
// =============================================================================

describe('DoLakeClient Extended Coverage', () => {
  let originalWebSocket: typeof globalThis.WebSocket;

  beforeEach(() => {
    originalWebSocket = globalThis.WebSocket;
    (globalThis as unknown as { WebSocket: typeof MockWebSocket }).WebSocket =
      MockWebSocket as unknown as typeof WebSocket;
  });

  afterEach(() => {
    (globalThis as unknown as { WebSocket: typeof globalThis.WebSocket }).WebSocket =
      originalWebSocket;
    vi.clearAllMocks();
  });

  // ===========================================================================
  // 1. Constructor Validation (Now Implemented)
  // ===========================================================================

  describe('constructor validation', () => {
    it('throws ConfigurationError for empty URL', () => {
      expect(() => new DoLakeClient({ url: '' })).toThrow(ConfigurationError);
      expect(() => new DoLakeClient({ url: '' })).toThrow('URL is required and cannot be empty');
    });

    it('throws ConfigurationError for whitespace-only URL', () => {
      expect(() => new DoLakeClient({ url: '   ' })).toThrow('URL is required and cannot be empty');
    });

    it('throws ConfigurationError for invalid URL format', () => {
      expect(() => new DoLakeClient({ url: 'not-a-url' })).toThrow('Invalid URL format');
      expect(() => new DoLakeClient({ url: 'not-a-url' })).toThrow(ConfigurationError);
    });

    it('throws ConfigurationError for unsupported protocol', () => {
      expect(() => new DoLakeClient({ url: 'ftp://lake.example.com' })).toThrow('http or https protocol');
    });

    it('throws ConfigurationError for ws:// protocol', () => {
      expect(() => new DoLakeClient({ url: 'ws://lake.example.com' })).toThrow('http or https protocol');
    });

    it('accepts http:// protocol', () => {
      const client = new DoLakeClient({ url: 'http://lake.example.com' });
      expect(client).toBeDefined();
    });

    it('accepts https:// protocol', () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      expect(client).toBeDefined();
    });

    it('throws ConfigurationError for negative timeout', () => {
      expect(
        () => new DoLakeClient({ url: 'https://lake.example.com', timeout: -1000 })
      ).toThrow('Timeout must be a positive number');
    });

    it('throws ConfigurationError for zero timeout', () => {
      expect(
        () => new DoLakeClient({ url: 'https://lake.example.com', timeout: 0 })
      ).toThrow('Timeout must be a positive number');
    });

    it('throws ConfigurationError for negative maxRetries', () => {
      expect(
        () =>
          new DoLakeClient({
            url: 'https://lake.example.com',
            retry: { maxRetries: -1, baseDelayMs: 100, maxDelayMs: 5000 },
          })
      ).toThrow('maxRetries must be non-negative');
    });

    it('throws ConfigurationError when baseDelayMs > maxDelayMs', () => {
      expect(
        () =>
          new DoLakeClient({
            url: 'https://lake.example.com',
            retry: { maxRetries: 3, baseDelayMs: 10000, maxDelayMs: 1000 },
          })
      ).toThrow('baseDelayMs cannot exceed maxDelayMs');
    });

    it('throws ConfigurationError for negative baseDelayMs', () => {
      expect(
        () =>
          new DoLakeClient({
            url: 'https://lake.example.com',
            retry: { maxRetries: 3, baseDelayMs: -100, maxDelayMs: 5000 },
          })
      ).toThrow('baseDelayMs must be non-negative');
    });

    it('throws ConfigurationError for negative maxDelayMs', () => {
      expect(
        () =>
          new DoLakeClient({
            url: 'https://lake.example.com',
            retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: -5000 },
          })
      ).toThrow('maxDelayMs must be non-negative');
    });

    it('creates client with zero maxRetries', () => {
      const client = new DoLakeClient({
        url: 'https://lake.example.com',
        retry: { maxRetries: 0, baseDelayMs: 100, maxDelayMs: 5000 },
      });
      expect(client).toBeDefined();
    });
  });

  // ===========================================================================
  // 2. getConfig()
  // ===========================================================================

  describe('getConfig()', () => {
    it('returns client configuration', () => {
      const client = new DoLakeClient({
        url: 'https://lake.example.com',
        timeout: 60000,
      });

      const config = client.getConfig();
      expect(config.url).toBe('https://lake.example.com');
      expect(config.timeout).toBe(60000);
    });

    it('returns frozen config object', () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      const config = client.getConfig();
      expect(Object.isFrozen(config)).toBe(true);
    });

    it('returns default timeout when not specified', () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      const config = client.getConfig();
      expect(config.timeout).toBe(DEFAULT_TIMEOUT_MS);
    });

    it('includes token when provided', () => {
      const client = new DoLakeClient({
        url: 'https://lake.example.com',
        token: 'test-token',
      });
      const config = client.getConfig();
      expect(config.token).toBe('test-token');
    });

    it('returns copy not the original', () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      const config1 = client.getConfig();
      const config2 = client.getConfig();
      expect(config1).not.toBe(config2);
      expect(config1.url).toBe(config2.url);
    });
  });

  // ===========================================================================
  // 3. Connection Properties
  // ===========================================================================

  describe('connection properties', () => {
    it('url getter returns configured URL', () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' }) as DoLakeClient;
      expect(client.url).toBe('https://lake.example.com');
    });

    it('isConnected is false initially', () => {
      const client = createLakeClient({ url: 'https://lake.example.com' }) as DoLakeClient;
      expect(client.isConnected).toBe(false);
    });

    it('connected is false initially', () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      expect(client.connected).toBe(false);
    });

    it('isConnected becomes true after connect()', async () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      await client.connect();
      expect(client.isConnected).toBe(true);
      await client.close();
    });

    it('connected becomes true after connect()', async () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      await client.connect();
      expect(client.connected).toBe(true);
      await client.close();
    });
  });

  // ===========================================================================
  // 4. Event System
  // ===========================================================================

  describe('event system', () => {
    it('on() returns this for chaining', () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      const result = client.on('connected', () => {});
      expect(result).toBe(client);
    });

    it('off() returns this for chaining', () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      const handler = () => {};
      client.on('connected', handler);
      const result = client.off('connected', handler);
      expect(result).toBe(client);
    });

    it('once() returns this for chaining', () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      const result = client.once('connected', () => {});
      expect(result).toBe(client);
    });

    it('chains multiple event registrations', () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      const result = client
        .on('connected', () => {})
        .on('disconnected', () => {})
        .on('error', () => {});
      expect(result).toBe(client);
    });

    it('emits connected event on connect()', async () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      const onConnected = vi.fn();
      client.on('connected', onConnected);
      await client.connect();
      expect(onConnected).toHaveBeenCalled();
      await client.close();
    });

    it('emits disconnected event on close()', async () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      const onDisconnected = vi.fn();
      client.on('disconnected', onDisconnected);
      await client.connect();
      await client.close();
      expect(onDisconnected).toHaveBeenCalled();
    });

    it('removes listener with off()', async () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      const handler = vi.fn();
      client.on('connected', handler);
      client.off('connected', handler);
      await client.connect();
      expect(handler).not.toHaveBeenCalled();
      await client.close();
    });
  });

  // ===========================================================================
  // 5. disconnect() Method
  // ===========================================================================

  describe('disconnect()', () => {
    it('exists as a method', () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      expect(typeof client.disconnect).toBe('function');
    });

    it('can be called without connecting first', async () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      await client.disconnect();
      expect(client.isConnected).toBe(false);
    });

    it('disconnects an active connection', async () => {
      const client = new DoLakeClient({ url: 'https://lake.example.com' });
      await client.connect();
      expect(client.isConnected).toBe(true);
      await client.disconnect();
      expect(client.isConnected).toBe(false);
    });
  });

  // ===========================================================================
  // 6. API Method Existence
  // ===========================================================================

  describe('API method existence', () => {
    let client: DoLakeClient;

    beforeEach(() => {
      client = new DoLakeClient({ url: 'https://lake.example.com' });
    });

    it('has query method', () => {
      expect(typeof client.query).toBe('function');
    });

    it('has subscribe method', () => {
      expect(typeof client.subscribe).toBe('function');
    });

    it('has getMetadata method', () => {
      expect(typeof client.getMetadata).toBe('function');
    });

    it('has listPartitions method', () => {
      expect(typeof client.listPartitions).toBe('function');
    });

    it('has compact method', () => {
      expect(typeof client.compact).toBe('function');
    });

    it('has getCompactionStatus method', () => {
      expect(typeof client.getCompactionStatus).toBe('function');
    });

    it('has listSnapshots method', () => {
      expect(typeof client.listSnapshots).toBe('function');
    });

    it('has ping method', () => {
      expect(typeof client.ping).toBe('function');
    });

    it('has close method', () => {
      expect(typeof client.close).toBe('function');
    });

    it('has connect method', () => {
      expect(typeof client.connect).toBe('function');
    });

    it('has disconnect method', () => {
      expect(typeof client.disconnect).toBe('function');
    });

    it('has on method', () => {
      expect(typeof client.on).toBe('function');
    });

    it('has off method', () => {
      expect(typeof client.off).toBe('function');
    });

    it('has once method', () => {
      expect(typeof client.once).toBe('function');
    });

    it('has getConfig method', () => {
      expect(typeof client.getConfig).toBe('function');
    });
  });
});

// =============================================================================
// Error Retryability
// =============================================================================

describe('LakeError retryability', () => {
  it('TIMEOUT errors are retryable', () => {
    const error = new LakeError({ code: ErrorCode.TIMEOUT, message: 'Timeout' });
    expect(error.isRetryable()).toBe(true);
  });

  it('CONNECTION_ERROR errors are retryable', () => {
    const error = new LakeError({ code: ErrorCode.CONNECTION_ERROR, message: 'Failed' });
    expect(error.isRetryable()).toBe(true);
  });

  it('CONNECTION_CLOSED errors are retryable', () => {
    const error = new LakeError({ code: ErrorCode.CONNECTION_CLOSED, message: 'Closed' });
    expect(error.isRetryable()).toBe(true);
  });

  it('CONNECTION_TIMEOUT errors are retryable', () => {
    const error = new LakeError({ code: ErrorCode.CONNECTION_TIMEOUT, message: 'Timeout' });
    expect(error.isRetryable()).toBe(true);
  });

  it('INVALID_SQL errors are not retryable', () => {
    const error = new LakeError({ code: ErrorCode.INVALID_SQL, message: 'Invalid' });
    expect(error.isRetryable()).toBe(false);
  });

  it('TABLE_NOT_FOUND errors are not retryable', () => {
    const error = new LakeError({ code: ErrorCode.TABLE_NOT_FOUND, message: 'Not found' });
    expect(error.isRetryable()).toBe(false);
  });

  it('INTERNAL_ERROR errors are not retryable', () => {
    const error = new LakeError({ code: ErrorCode.INTERNAL_ERROR, message: 'Internal' });
    expect(error.isRetryable()).toBe(false);
  });
});

// =============================================================================
// LakeError Category Assignment
// =============================================================================

describe('LakeError category assignment', () => {
  it('assigns CONNECTION category for connection errors', () => {
    const error1 = new LakeError({ code: ErrorCode.CONNECTION_ERROR, message: '' });
    expect(error1.category).toBe('CONNECTION');

    const error2 = new LakeError({ code: ErrorCode.CONNECTION_CLOSED, message: '' });
    expect(error2.category).toBe('CONNECTION');

    const error3 = new LakeError({ code: ErrorCode.NOT_CONNECTED, message: '' });
    expect(error3.category).toBe('CONNECTION');
  });

  it('assigns TIMEOUT category for timeout errors', () => {
    const error1 = new LakeError({ code: ErrorCode.TIMEOUT, message: '' });
    expect(error1.category).toBe('TIMEOUT');

    const error2 = new LakeError({ code: ErrorCode.QUERY_TIMEOUT, message: '' });
    expect(error2.category).toBe('TIMEOUT');
  });

  it('assigns VALIDATION category for validation errors', () => {
    const error = new LakeError({ code: ErrorCode.INVALID_SQL, message: '' });
    expect(error.category).toBe('VALIDATION');
  });

  it('assigns RESOURCE category for resource errors', () => {
    const error1 = new LakeError({ code: ErrorCode.TABLE_NOT_FOUND, message: '' });
    expect(error1.category).toBe('RESOURCE');

    const error2 = new LakeError({ code: ErrorCode.PARTITION_NOT_FOUND, message: '' });
    expect(error2.category).toBe('RESOURCE');
  });

  it('assigns VALIDATION category for auth errors', () => {
    const error1 = new LakeError({ code: ErrorCode.UNAUTHORIZED, message: '' });
    expect(error1.category).toBe('VALIDATION');

    const error2 = new LakeError({ code: ErrorCode.TOKEN_EXPIRED, message: '' });
    expect(error2.category).toBe('VALIDATION');
  });

  it('assigns EXECUTION category for unknown codes', () => {
    const error = new LakeError({ code: 'UNKNOWN_CODE', message: '' });
    expect(error.category).toBe('EXECUTION');
  });
});

// =============================================================================
// LakeError Static Methods
// =============================================================================

describe('LakeError static methods', () => {
  it('creates error via static create', () => {
    const error = LakeError.create('TEST', 'Test message', { key: 'value' });
    expect(error).toBeInstanceOf(LakeError);
    expect(error.code).toBe('TEST');
    expect(error.message).toBe('Test message');
    expect(error.details).toEqual({ key: 'value' });
  });

  it('deserializes from JSON', () => {
    const error = LakeError.fromJSON({
      name: 'LakeError',
      code: 'TEST',
      message: 'Test',
      category: 'EXECUTION',
      context: { metadata: { details: { key: 'value' } } },
    });
    expect(error).toBeInstanceOf(LakeError);
    expect(error.code).toBe('TEST');
    expect(error.details).toEqual({ key: 'value' });
  });
});

// =============================================================================
// ConnectionError Specific Tests
// =============================================================================

describe('ConnectionError specifics', () => {
  it('is always retryable', () => {
    const error = new ConnectionError({ code: 'ANY_CODE', message: 'Any' });
    expect(error.isRetryable()).toBe(true);
  });

  it('has user-friendly message', () => {
    const error = new ConnectionError({ code: 'TEST', message: 'Test' });
    expect(error.toUserMessage()).toContain('lake service');
  });

  it('creates via closed() factory', () => {
    const error = ConnectionError.closed();
    expect(error.code).toBe(ErrorCode.CONNECTION_CLOSED);
    expect(error.message).toContain('Connection closed');
  });

  it('creates via closed() with custom message', () => {
    const error = ConnectionError.closed('Custom close message');
    expect(error.message).toContain('Custom close message');
  });

  it('creates via closed() with URL', () => {
    const error = ConnectionError.closed('Closed', 'https://example.com');
    expect(error.url).toBeDefined();
  });

  it('creates via notConnected() factory', () => {
    const error = ConnectionError.notConnected();
    expect(error.code).toBe(ErrorCode.NOT_CONNECTED);
    expect(error.message).toContain('not connected');
  });

  it('creates via failed() factory', () => {
    const error = ConnectionError.failed();
    expect(error.code).toBe(ErrorCode.CONNECTION_ERROR);
    expect(error.message).toContain('Connection failed');
  });

  it('creates via failed() with URL', () => {
    const error = ConnectionError.failed('Server down', 'https://example.com');
    expect(error.url).toBeDefined();
    expect(error.message).toContain('Server down');
  });

  it('deserializes from JSON', () => {
    const error = ConnectionError.fromJSON({
      name: 'ConnectionError',
      code: 'CONNECTION_ERROR',
      message: 'Connection failed',
      category: 'CONNECTION',
      context: { metadata: { url: 'https://example.com' } },
    });
    expect(error).toBeInstanceOf(ConnectionError);
  });
});

// =============================================================================
// QueryError Specific Tests
// =============================================================================

describe('QueryError specifics', () => {
  it('is retryable only for QUERY_TIMEOUT', () => {
    const timeoutError = new QueryError({ code: ErrorCode.QUERY_TIMEOUT, message: 'Timeout' });
    expect(timeoutError.isRetryable()).toBe(true);

    const syntaxError = new QueryError({ code: ErrorCode.INVALID_SQL, message: 'Invalid' });
    expect(syntaxError.isRetryable()).toBe(false);

    const notFoundError = new QueryError({ code: ErrorCode.TABLE_NOT_FOUND, message: 'Not found' });
    expect(notFoundError.isRetryable()).toBe(false);
  });

  it('creates via tableNotFound() factory', () => {
    const error = QueryError.tableNotFound('orders');
    expect(error.code).toBe(ErrorCode.TABLE_NOT_FOUND);
    expect(error.message).toContain('orders');
    expect(error.details).toEqual({ tableName: 'orders' });
  });

  it('creates via invalidSql() factory', () => {
    const error = QueryError.invalidSql('Unexpected token', { position: 5 });
    expect(error.code).toBe(ErrorCode.INVALID_SQL);
    expect(error.details).toEqual({ position: 5 });
  });

  it('creates via timeout() factory', () => {
    const error = QueryError.timeout();
    expect(error.code).toBe(ErrorCode.QUERY_TIMEOUT);
    expect(error.message).toContain('timed out');
  });

  it('creates via timeout() with custom message', () => {
    const error = QueryError.timeout('Custom timeout');
    expect(error.message).toContain('Custom timeout');
  });

  it('deserializes from JSON', () => {
    const error = QueryError.fromJSON({
      name: 'QueryError',
      code: 'INVALID_SQL',
      message: 'Bad SQL',
      category: 'VALIDATION',
      context: { metadata: { details: { position: 10 } } },
    });
    expect(error).toBeInstanceOf(QueryError);
    expect(error.details).toEqual({ position: 10 });
  });
});

// =============================================================================
// TimeoutError Specific Tests
// =============================================================================

describe('TimeoutError specifics', () => {
  it('is always retryable', () => {
    const error = new TimeoutError('query');
    expect(error.isRetryable()).toBe(true);
  });

  it('has user-friendly message', () => {
    const error = new TimeoutError('query');
    expect(error.toUserMessage()).toContain('query');
    expect(error.toUserMessage()).toContain('timed out');
  });

  it('has method property', () => {
    const error = new TimeoutError('listSnapshots');
    expect(error.method).toBe('listSnapshots');
  });

  it('includes method in details', () => {
    const error = new TimeoutError('compact');
    expect(error.details).toEqual({ method: 'compact' });
  });

  it('deserializes from JSON', () => {
    const error = TimeoutError.fromJSON({
      name: 'TimeoutError',
      code: 'TIMEOUT',
      message: 'Request timeout: query',
      category: 'TIMEOUT',
      context: { metadata: { method: 'query' } },
    });
    expect(error).toBeInstanceOf(TimeoutError);
    expect(error.method).toBe('query');
  });

  it('deserializes with default method when missing', () => {
    const error = TimeoutError.fromJSON({
      name: 'TimeoutError',
      code: 'TIMEOUT',
      message: 'Timeout',
      category: 'TIMEOUT',
    });
    expect(error.method).toBe('unknown');
  });
});

// =============================================================================
// ConfigurationError Tests
// =============================================================================

describe('ConfigurationError', () => {
  it('creates error with message', () => {
    const error = new ConfigurationError('Invalid config');
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe('ConfigurationError');
    expect(error.code).toBe('CONFIG_INVALID');
    expect(error.message).toBe('Invalid config');
  });

  it('includes field when provided', () => {
    const error = new ConfigurationError('Bad URL', 'url');
    expect(error.field).toBe('url');
  });

  it('is not retryable', () => {
    const error = new ConfigurationError('Invalid');
    expect(error.isRetryable()).toBe(false);
  });

  it('returns message from toUserMessage()', () => {
    const error = new ConfigurationError('URL is required');
    expect(error.toUserMessage()).toBe('URL is required');
  });

  it('deserializes from JSON', () => {
    const error = ConfigurationError.fromJSON({
      name: 'ConfigurationError',
      code: 'CONFIG_INVALID',
      message: 'Invalid URL',
      category: 'VALIDATION',
      context: { metadata: { field: 'url' } },
    });
    expect(error).toBeInstanceOf(ConfigurationError);
    expect(error.field).toBe('url');
  });
});

// =============================================================================
// Constants Tests
// =============================================================================

describe('Constants', () => {
  describe('WebSocketState', () => {
    it('has standard WebSocket state values', () => {
      expect(WebSocketState.CONNECTING).toBe(0);
      expect(WebSocketState.OPEN).toBe(1);
      expect(WebSocketState.CLOSING).toBe(2);
      expect(WebSocketState.CLOSED).toBe(3);
    });
  });

  describe('DEFAULT_TIMEOUT_MS', () => {
    it('is 30000ms', () => {
      expect(DEFAULT_TIMEOUT_MS).toBe(30000);
    });
  });

  describe('DEFAULT_MAX_QUEUE_SIZE', () => {
    it('is 1000', () => {
      expect(DEFAULT_MAX_QUEUE_SIZE).toBe(1000);
    });
  });

  describe('MIN_QUEUE_SIZE', () => {
    it('is 1', () => {
      expect(MIN_QUEUE_SIZE).toBe(1);
    });
  });

  describe('ErrorCode', () => {
    it('has all expected error codes', () => {
      expect(ErrorCode.CONNECTION_ERROR).toBe('CONNECTION_ERROR');
      expect(ErrorCode.CONNECTION_CLOSED).toBe('CONNECTION_CLOSED');
      expect(ErrorCode.CONNECTION_TIMEOUT).toBe('CONNECTION_TIMEOUT');
      expect(ErrorCode.NOT_CONNECTED).toBe('NOT_CONNECTED');
      expect(ErrorCode.INVALID_SQL).toBe('INVALID_SQL');
      expect(ErrorCode.QUERY_TIMEOUT).toBe('QUERY_TIMEOUT');
      expect(ErrorCode.TABLE_NOT_FOUND).toBe('TABLE_NOT_FOUND');
      expect(ErrorCode.PARTITION_NOT_FOUND).toBe('PARTITION_NOT_FOUND');
      expect(ErrorCode.UNAUTHORIZED).toBe('UNAUTHORIZED');
      expect(ErrorCode.TOKEN_EXPIRED).toBe('TOKEN_EXPIRED');
      expect(ErrorCode.TIMEOUT).toBe('TIMEOUT');
      expect(ErrorCode.INTERNAL_ERROR).toBe('INTERNAL_ERROR');
    });
  });
});

// =============================================================================
// Transformer Edge Cases
// =============================================================================

describe('Transformer edge cases', () => {
  describe('isValidStringId edge cases', () => {
    it('returns false for boolean values', () => {
      expect(isValidStringId(true)).toBe(false);
      expect(isValidStringId(false)).toBe(false);
    });

    it('returns false for zero', () => {
      expect(isValidStringId(0)).toBe(false);
    });

    it('returns true for strings with spaces in the middle', () => {
      expect(isValidStringId('hello world')).toBe(true);
    });
  });

  describe('isValidDateLike edge cases', () => {
    it('returns true for epoch zero', () => {
      expect(isValidDateLike(0)).toBe(true);
    });

    it('returns true for negative timestamps', () => {
      expect(isValidDateLike(-1000)).toBe(true);
    });

    it('returns false for NaN', () => {
      expect(isValidDateLike(NaN)).toBe(false);
    });

    it('returns false for boolean', () => {
      expect(isValidDateLike(true)).toBe(false);
    });
  });

  describe('transformSnapshot edge cases', () => {
    it('throws for numeric snapshot ID', () => {
      const raw: RawSnapshot = {
        id: 123 as unknown as string,
        timestamp: '2024-01-15',
        summary: { addedFiles: 0, deletedFiles: 0, addedRows: 0, deletedRows: 0 },
        manifestList: '/manifest.json',
      };
      expect(() => transformSnapshot(raw)).toThrow('Invalid snapshot ID');
    });

    it('throws for undefined snapshot ID', () => {
      const raw: RawSnapshot = {
        id: undefined as unknown as string,
        timestamp: '2024-01-15',
        summary: { addedFiles: 0, deletedFiles: 0, addedRows: 0, deletedRows: 0 },
        manifestList: '/manifest.json',
      };
      expect(() => transformSnapshot(raw)).toThrow('Invalid snapshot ID');
    });

    it('throws for empty string parent snapshot ID', () => {
      const raw: RawSnapshot = {
        id: 'snap_123',
        timestamp: '2024-01-15',
        parentId: '' as unknown as string,
        summary: { addedFiles: 0, deletedFiles: 0, addedRows: 0, deletedRows: 0 },
        manifestList: '/manifest.json',
      };
      expect(() => transformSnapshot(raw)).toThrow('Invalid parent snapshot ID');
    });
  });

  describe('transformPartitionInfo edge cases', () => {
    it('throws for numeric partition key', () => {
      const raw: RawPartitionInfo = {
        key: 123 as unknown as string,
        strategy: 'time',
        fileCount: 1,
        rowCount: 100,
        sizeBytes: 1024,
        lastModified: '2024-01-15',
      };
      expect(() => transformPartitionInfo(raw)).toThrow('Invalid partition key');
    });

    it('throws for null partition key', () => {
      const raw: RawPartitionInfo = {
        key: null as unknown as string,
        strategy: 'hash',
        fileCount: 1,
        rowCount: 100,
        sizeBytes: 1024,
        lastModified: '2024-01-15',
      };
      expect(() => transformPartitionInfo(raw)).toThrow('Invalid partition key');
    });

    it('preserves numeric lastModified', () => {
      const timestamp = 1705320000000;
      const raw: RawPartitionInfo = {
        key: 'part_1',
        strategy: 'time',
        fileCount: 1,
        rowCount: 100,
        sizeBytes: 1024,
        lastModified: timestamp,
      };
      const result = transformPartitionInfo(raw);
      expect(result.lastModified.getTime()).toBe(timestamp);
    });
  });

  describe('transformCompactionJob edge cases', () => {
    it('handles empty inputFiles', () => {
      const raw: RawCompactionJob = {
        id: 'compact_1',
        partition: 'part_1',
        status: 'pending',
        inputFiles: [],
      };
      const job = transformCompactionJob(raw);
      expect(job.inputFiles).toEqual([]);
    });

    it('handles bytesRead of 0', () => {
      const raw: RawCompactionJob = {
        id: 'compact_1',
        partition: 'part_1',
        status: 'completed',
        inputFiles: ['file.parquet'],
        bytesRead: 0,
        bytesWritten: 0,
      };
      const job = transformCompactionJob(raw);
      expect(job.bytesRead).toBe(0);
      expect(job.bytesWritten).toBe(0);
    });

    it('handles numeric startedAt', () => {
      const timestamp = 1705320000000;
      const raw: RawCompactionJob = {
        id: 'compact_1',
        partition: 'part_1',
        status: 'running',
        inputFiles: ['file.parquet'],
        startedAt: timestamp,
      };
      const job = transformCompactionJob(raw);
      expect(job.startedAt).toBeInstanceOf(Date);
      expect(job.startedAt?.getTime()).toBe(timestamp);
    });
  });

  describe('transformTableMetadata edge cases', () => {
    it('handles empty snapshots array', () => {
      const raw: RawTableMetadata = {
        tableId: 'test',
        schema: { columns: [] },
        partitionSpec: { column: 'id', strategy: 'hash', granularity: undefined },
        snapshots: [],
        properties: {},
      };
      const metadata = transformTableMetadata(raw);
      expect(metadata.snapshots).toEqual([]);
      expect(metadata.currentSnapshotId).toBeUndefined();
    });

    it('handles undefined currentSnapshotId', () => {
      const raw: RawTableMetadata = {
        tableId: 'test',
        schema: { columns: [] },
        partitionSpec: { column: 'id', strategy: 'hash', granularity: undefined },
        snapshots: [],
        properties: {},
      };
      const metadata = transformTableMetadata(raw);
      expect(metadata.currentSnapshotId).toBeUndefined();
    });

    it('throws for empty string currentSnapshotId', () => {
      const raw: RawTableMetadata = {
        tableId: 'test',
        schema: { columns: [] },
        partitionSpec: { column: 'id', strategy: 'hash', granularity: undefined },
        currentSnapshotId: '' as unknown as string,
        snapshots: [],
        properties: {},
      };
      expect(() => transformTableMetadata(raw)).toThrow('Invalid current snapshot ID');
    });

    it('preserves properties', () => {
      const raw: RawTableMetadata = {
        tableId: 'test',
        schema: { columns: [] },
        partitionSpec: { column: 'id', strategy: 'hash', granularity: undefined },
        snapshots: [],
        properties: { owner: 'team', version: '1.0' },
      };
      const metadata = transformTableMetadata(raw);
      expect(metadata.properties).toEqual({ owner: 'team', version: '1.0' });
    });
  });
});

// =============================================================================
// createLakeClient Factory
// =============================================================================

describe('createLakeClient', () => {
  let originalWebSocket: typeof globalThis.WebSocket;

  beforeEach(() => {
    originalWebSocket = globalThis.WebSocket;
    (globalThis as unknown as { WebSocket: typeof MockWebSocket }).WebSocket =
      MockWebSocket as unknown as typeof WebSocket;
  });

  afterEach(() => {
    (globalThis as unknown as { WebSocket: typeof globalThis.WebSocket }).WebSocket =
      originalWebSocket;
  });

  it('creates a client instance', () => {
    const client = createLakeClient({ url: 'https://lake.example.com' });
    expect(client).toBeDefined();
  });

  it('validates config same as constructor', () => {
    expect(() => createLakeClient({ url: '' })).toThrow();
    expect(() => createLakeClient({ url: 'not-valid' })).toThrow();
  });

  it('returns a LakeClient interface', () => {
    const client = createLakeClient({ url: 'https://lake.example.com' });
    expect(typeof client.query).toBe('function');
    expect(typeof client.subscribe).toBe('function');
    expect(typeof client.getMetadata).toBe('function');
    expect(typeof client.listPartitions).toBe('function');
    expect(typeof client.compact).toBe('function');
    expect(typeof client.getCompactionStatus).toBe('function');
    expect(typeof client.listSnapshots).toBe('function');
    expect(typeof client.ping).toBe('function');
    expect(typeof client.close).toBe('function');
  });
});
