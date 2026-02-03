/**
 * Structured Logging Tests
 *
 * Tests for the DoSQL structured logging module.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  createLogger,
  compareLogLevels,
  extractTraceId,
  withTraceId,
  withTraceContext,
  ConsoleSink,
  JsonSink,
  NoOpSink,
  MultiSink,
  FilteringSink,
  BatchingSink,
  SamplingSink,
  RedactingSink,
  FileSink,
  StandardContext,
  type LogEntry,
  type LogLevel,
  type LogSink,
} from '../index.js';

// =============================================================================
// Test Helpers
// =============================================================================

class TestSink implements LogSink {
  public entries: LogEntry[] = [];

  write(entry: LogEntry): void {
    this.entries.push(entry);
  }

  clear(): void {
    this.entries = [];
  }
}

/**
 * Captures console output without using vi.fn() mocking.
 * Returns an object that intercepts console method calls.
 */
function captureConsole(method: 'log' | 'warn' | 'error' = 'log') {
  const captured: unknown[][] = [];
  const original = console[method];
  console[method] = (...args: unknown[]) => { captured.push(args); };
  return {
    captured,
    restore: () => { console[method] = original; },
  };
}

/**
 * Creates a recording function that tracks all calls and their arguments.
 * Replaces vi.fn() with a real implementation that implements the callback interface.
 */
function createCallRecorder<T extends unknown[], R = void>(
  impl?: (...args: T) => R,
): ((...args: T) => R) & { calls: T[]; callCount: number } {
  const calls: T[] = [];
  const fn = ((...args: T) => {
    calls.push(args);
    return impl ? impl(...args) : (undefined as R);
  }) as ((...args: T) => R) & { calls: T[]; callCount: number };
  Object.defineProperty(fn, 'calls', { get: () => calls });
  Object.defineProperty(fn, 'callCount', { get: () => calls.length });
  return fn;
}

// =============================================================================
// Logger Creation Tests
// =============================================================================

describe('createLogger', () => {
  it('should create a logger with default configuration', () => {
    const logger = createLogger();

    expect(logger).toHaveProperty('debug');
    expect(logger.debug).toBeTypeOf('function');
    expect(logger.info).toBeTypeOf('function');
    expect(logger.warn).toBeTypeOf('function');
    expect(logger.error).toBeTypeOf('function');
  });

  it('should use custom sink', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink });

    logger.info('Test message');

    expect(sink.entries).toHaveLength(1);
    expect(sink.entries[0].message).toBe('Test message');
  });

  it('should respect log level', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink, level: 'warn' });

    logger.debug('Debug message');
    logger.info('Info message');
    logger.warn('Warn message');
    logger.error('Error message');

    expect(sink.entries).toHaveLength(2);
    expect(sink.entries[0].level).toBe('warn');
    expect(sink.entries[1].level).toBe('error');
  });

  it('should include default context in all entries', () => {
    const sink = new TestSink();
    const logger = createLogger({
      sink,
      defaultContext: { service: 'test-service', version: '1.0.0' },
    });

    logger.info('Test message');

    expect(sink.entries[0].context?.service).toBe('test-service');
    expect(sink.entries[0].context?.version).toBe('1.0.0');
  });

  it('should generate trace ID if not provided', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink });

    logger.info('Test message');

    expect(typeof sink.entries[0].traceId).toBe('string');
    expect(sink.entries[0].traceId.length).toBeGreaterThan(0);
  });

  it('should use provided trace ID', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink, traceId: 'custom-trace-id' });

    logger.info('Test message');

    expect(sink.entries[0].traceId).toBe('custom-trace-id');
  });
});

// =============================================================================
// Log Level Tests
// =============================================================================

describe('Log Levels', () => {
  it('should log debug messages when level is debug', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink, level: 'debug' });

    logger.debug('Debug message');

    expect(sink.entries).toHaveLength(1);
    expect(sink.entries[0].level).toBe('debug');
  });

  it('should skip debug messages when level is info', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink, level: 'info' });

    logger.debug('Debug message');

    expect(sink.entries).toHaveLength(0);
  });

  it('should allow runtime level changes', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink, level: 'error' });

    logger.info('Should not appear');
    expect(sink.entries).toHaveLength(0);

    logger.setLevel('debug');
    logger.info('Should appear');
    expect(sink.entries).toHaveLength(1);
  });

  it('should report current log level', () => {
    const logger = createLogger({ level: 'warn' });

    expect(logger.getLevel()).toBe('warn');

    logger.setLevel('debug');
    expect(logger.getLevel()).toBe('debug');
  });
});

describe('compareLogLevels', () => {
  it('should correctly compare log levels', () => {
    expect(compareLogLevels('debug', 'info')).toBeLessThan(0);
    expect(compareLogLevels('info', 'warn')).toBeLessThan(0);
    expect(compareLogLevels('warn', 'error')).toBeLessThan(0);
    expect(compareLogLevels('error', 'debug')).toBeGreaterThan(0);
    expect(compareLogLevels('info', 'info')).toBe(0);
  });
});

// =============================================================================
// Message Formatting Tests
// =============================================================================

describe('Message Formatting', () => {
  it('should support lazy message evaluation', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink, level: 'info' });

    const lazyFn = vi.fn(() => 'Lazy message');

    // This should not call the lazy function (level too low)
    logger.setLevel('error');
    logger.info(lazyFn);
    expect(lazyFn).not.toHaveBeenCalled();

    // This should call the lazy function
    logger.setLevel('info');
    logger.info(lazyFn);
    expect(lazyFn).toHaveBeenCalled();
  });

  it('should format message with placeholders', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink });

    logger.info('User {userId} logged in', { userId: '123' });

    expect(sink.entries[0].message).toBe('User 123 logged in');
  });

  it('should leave unknown placeholders unchanged', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink });

    logger.info('User {unknown} logged in', { userId: '123' });

    expect(sink.entries[0].message).toBe('User {unknown} logged in');
  });
});

// =============================================================================
// Context Tests
// =============================================================================

describe('Context Handling', () => {
  it('should merge context with default context', () => {
    const sink = new TestSink();
    const logger = createLogger({
      sink,
      defaultContext: { service: 'api' },
    });

    logger.info('Message', { userId: '123' });

    expect(sink.entries[0].context).toEqual({
      service: 'api',
      userId: '123',
    });
  });

  it('should override default context with message context', () => {
    const sink = new TestSink();
    const logger = createLogger({
      sink,
      defaultContext: { env: 'prod' },
    });

    logger.info('Message', { env: 'test' });

    expect(sink.entries[0].context?.env).toBe('test');
  });

  it('should handle circular references in context', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink });

    const circular: Record<string, unknown> = { name: 'test' };
    circular.self = circular;

    // Should not throw
    expect(() => {
      logger.info('Message', { data: circular });
    }).not.toThrow();

    expect(sink.entries[0].context).toHaveProperty('data');
  });
});

// =============================================================================
// Error Logging Tests
// =============================================================================

describe('Error Logging', () => {
  it('should include error details', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink });

    const error = new Error('Test error');
    logger.error('An error occurred', error);

    expect(sink.entries[0].error).toMatchObject({
      name: 'Error',
      message: 'Test error',
    });
  });

  it('should include error stack trace by default', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink, includeStackTraces: true });

    const error = new Error('Test error');
    logger.error('An error occurred', error);

    expect(typeof sink.entries[0].error?.stack).toBe('string');
  });

  it('should exclude stack traces when disabled', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink, includeStackTraces: false });

    const error = new Error('Test error');
    logger.error('An error occurred', error);

    expect(sink.entries[0].error?.stack).toBeUndefined();
  });

  it('should include error code if present', () => {
    const sink = new TestSink();
    const logger = createLogger({ sink });

    const error = Object.assign(new Error('Test error'), { code: 'ERR_TEST' });
    logger.error('An error occurred', error);

    expect(sink.entries[0].error?.code).toBe('ERR_TEST');
  });
});

// =============================================================================
// Child Logger Tests
// =============================================================================

describe('Child Loggers', () => {
  it('should create child logger with additional context', () => {
    const sink = new TestSink();
    const parent = createLogger({ sink, defaultContext: { service: 'api' } });

    const child = parent.child({ component: 'auth' });
    child.info('Child message');

    expect(sink.entries[0].context).toEqual({
      service: 'api',
      component: 'auth',
    });
  });

  it('should share sink with parent', () => {
    const sink = new TestSink();
    const parent = createLogger({ sink });

    const child = parent.child({ child: true });
    parent.info('Parent');
    child.info('Child');

    expect(sink.entries).toHaveLength(2);
  });

  it('should inherit parent settings', () => {
    const sink = new TestSink();
    const parent = createLogger({ sink, level: 'warn' });

    const child = parent.child({});
    child.info('Should not appear');
    child.warn('Should appear');

    expect(sink.entries).toHaveLength(1);
  });
});

// =============================================================================
// Trace ID Tests
// =============================================================================

describe('Trace ID Handling', () => {
  it('should allow getting and setting trace ID', () => {
    const logger = createLogger({ traceId: 'initial-trace' });

    expect(logger.getTraceId()).toBe('initial-trace');

    logger.setTraceId('new-trace');
    expect(logger.getTraceId()).toBe('new-trace');
  });

  it('should use async context trace ID when available', async () => {
    const sink = new TestSink();
    const logger = createLogger({ sink, traceId: 'default-trace' });

    await withTraceContext('async-trace', () => {
      logger.info('Within async context');
    });

    expect(sink.entries[0].traceId).toBe('async-trace');
  });
});

describe('extractTraceId', () => {
  it('should extract x-trace-id header', () => {
    const request = new Request('https://example.com', {
      headers: { 'x-trace-id': 'trace-123' },
    });

    expect(extractTraceId(request)).toBe('trace-123');
  });

  it('should extract x-request-id header', () => {
    const request = new Request('https://example.com', {
      headers: { 'x-request-id': 'request-456' },
    });

    expect(extractTraceId(request)).toBe('request-456');
  });

  it('should extract traceparent header (W3C format)', () => {
    const request = new Request('https://example.com', {
      headers: { traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01' },
    });

    expect(extractTraceId(request)).toBe('0af7651916cd43dd8448eb211c80319c');
  });

  it('should return undefined for missing headers', () => {
    const request = new Request('https://example.com');

    expect(extractTraceId(request)).toBeUndefined();
  });

  it('should respect custom header order', () => {
    const request = new Request('https://example.com', {
      headers: {
        'x-trace-id': 'trace',
        'x-request-id': 'request',
      },
    });

    expect(extractTraceId(request, ['x-request-id', 'x-trace-id'])).toBe('request');
  });
});

describe('withTraceId', () => {
  it('should add trace ID to error context', () => {
    const error = new Error('Test') as Error & { context?: Record<string, unknown> };
    withTraceId(error, 'trace-123');

    expect(error.context?.traceId).toBe('trace-123');
  });

  it('should preserve existing error context', () => {
    const error = new Error('Test') as Error & { context?: Record<string, unknown> };
    error.context = { existing: 'value' };
    withTraceId(error, 'trace-123');

    expect(error.context?.existing).toBe('value');
    expect(error.context?.traceId).toBe('trace-123');
  });
});

// =============================================================================
// Built-in Sink Tests
// =============================================================================

describe('ConsoleSink', () => {
  it('should write to console', () => {
    const capture = captureConsole('log');
    try {
      const sink = new ConsoleSink({});

      sink.write({
        timestamp: '2024-01-01T00:00:00.000Z',
        level: 'info',
        message: 'Test',
        traceId: 'trace',
      });

      expect(capture.captured.length).toBeGreaterThan(0);
    } finally {
      capture.restore();
    }
  });

  it('should pretty print when enabled', () => {
    const capture = captureConsole('log');
    try {
      const sink = new ConsoleSink({ prettyPrint: true });

      sink.write({
        timestamp: '2024-01-01T00:00:00.000Z',
        level: 'info',
        message: 'Test',
        traceId: 'trace',
      });

      const output = capture.captured[0][0] as string;
      expect(output).toContain('\n'); // Pretty printed JSON contains newlines
    } finally {
      capture.restore();
    }
  });
});

describe('JsonSink', () => {
  it('should call write function with JSON', () => {
    const writtenValues: string[] = [];
    const sink = new JsonSink({ write: (val: string) => { writtenValues.push(val); } });

    const entry: LogEntry = {
      timestamp: '2024-01-01T00:00:00.000Z',
      level: 'info',
      message: 'Test',
      traceId: 'trace',
    };
    sink.write(entry);

    expect(writtenValues).toHaveLength(1);
    expect(writtenValues[0]).toBe(JSON.stringify(entry));
  });
});

describe('NoOpSink', () => {
  it('should not throw on write', () => {
    const sink = new NoOpSink();

    expect(() => {
      sink.write({
        timestamp: '2024-01-01T00:00:00.000Z',
        level: 'info',
        message: 'Test',
        traceId: 'trace',
      });
    }).not.toThrow();
  });
});

describe('MultiSink', () => {
  it('should write to all sinks', () => {
    const sink1 = new TestSink();
    const sink2 = new TestSink();
    const multi = new MultiSink([sink1, sink2]);

    multi.write({
      timestamp: '2024-01-01T00:00:00.000Z',
      level: 'info',
      message: 'Test',
      traceId: 'trace',
    });

    expect(sink1.entries).toHaveLength(1);
    expect(sink2.entries).toHaveLength(1);
  });

  it('should continue on sink failure', () => {
    const failingSink: LogSink = {
      write() {
        throw new Error('Sink failed');
      },
    };
    const workingSink = new TestSink();
    const multi = new MultiSink([failingSink, workingSink]);

    expect(() => {
      multi.write({
        timestamp: '2024-01-01T00:00:00.000Z',
        level: 'info',
        message: 'Test',
        traceId: 'trace',
      });
    }).not.toThrow();

    expect(workingSink.entries).toHaveLength(1);
  });

  it('should flush all sinks', async () => {
    let flush1Called = false;
    let flush2Called = false;
    const sink1: LogSink = { write() {}, flush: () => { flush1Called = true; } };
    const sink2: LogSink = { write() {}, flush: () => { flush2Called = true; } };
    const multi = new MultiSink([sink1, sink2]);

    await multi.flush();

    expect(flush1Called).toBe(true);
    expect(flush2Called).toBe(true);
  });
});

describe('FilteringSink', () => {
  it('should filter based on predicate', () => {
    const baseSink = new TestSink();
    const sink = new FilteringSink(baseSink, {
      filter: (entry) => entry.level === 'error',
    });

    sink.write({
      timestamp: '2024-01-01T00:00:00.000Z',
      level: 'info',
      message: 'Info',
      traceId: 'trace',
    });
    sink.write({
      timestamp: '2024-01-01T00:00:00.000Z',
      level: 'error',
      message: 'Error',
      traceId: 'trace',
    });

    expect(baseSink.entries).toHaveLength(1);
    expect(baseSink.entries[0].level).toBe('error');
  });
});

describe('BatchingSink', () => {
  it('should batch entries', async () => {
    const batches: LogEntry[][] = [];
    const sink = new BatchingSink({
      batchSize: 3,
      flushInterval: 10000,
      onBatch: (entries) => {
        batches.push([...entries]);
      },
    });

    sink.write({
      timestamp: '2024-01-01T00:00:00.000Z',
      level: 'info',
      message: '1',
      traceId: 'trace',
    });
    sink.write({
      timestamp: '2024-01-01T00:00:00.000Z',
      level: 'info',
      message: '2',
      traceId: 'trace',
    });
    expect(batches).toHaveLength(0);

    sink.write({
      timestamp: '2024-01-01T00:00:00.000Z',
      level: 'info',
      message: '3',
      traceId: 'trace',
    });
    expect(batches).toHaveLength(1);
    expect(batches[0]).toHaveLength(3);

    await sink.close();
  });

  it('should flush remaining on close', async () => {
    const batches: LogEntry[][] = [];
    const sink = new BatchingSink({
      batchSize: 10,
      flushInterval: 10000,
      onBatch: (entries) => {
        batches.push([...entries]);
      },
    });

    sink.write({
      timestamp: '2024-01-01T00:00:00.000Z',
      level: 'info',
      message: 'Test',
      traceId: 'trace',
    });

    await sink.close();

    expect(batches).toHaveLength(1);
  });
});

describe('SamplingSink', () => {
  it('should always pass entries with rate 1', () => {
    const baseSink = new TestSink();
    const sink = new SamplingSink(baseSink, {
      sampleRate: { debug: 1, info: 1, warn: 1, error: 1 },
    });

    for (let i = 0; i < 100; i++) {
      sink.write({
        timestamp: '2024-01-01T00:00:00.000Z',
        level: 'info',
        message: 'Test',
        traceId: 'trace',
      });
    }

    expect(baseSink.entries).toHaveLength(100);
  });

  it('should block entries with rate 0', () => {
    const baseSink = new TestSink();
    const sink = new SamplingSink(baseSink, {
      sampleRate: { debug: 0, info: 0, warn: 0, error: 0 },
    });

    for (let i = 0; i < 100; i++) {
      sink.write({
        timestamp: '2024-01-01T00:00:00.000Z',
        level: 'info',
        message: 'Test',
        traceId: 'trace',
      });
    }

    expect(baseSink.entries).toHaveLength(0);
  });
});

describe('RedactingSink', () => {
  it('should redact specified fields', () => {
    const baseSink = new TestSink();
    const sink = new RedactingSink(baseSink, {
      redactFields: ['password', 'apiKey'],
    });

    sink.write({
      timestamp: '2024-01-01T00:00:00.000Z',
      level: 'info',
      message: 'Login',
      traceId: 'trace',
      context: {
        username: 'user',
        password: 'secret123',
        apiKey: 'key-abc',
      },
    });

    const context = baseSink.entries[0].context;
    expect(context?.username).toBe('user');
    expect(context?.password).toBe('[REDACTED]');
    expect(context?.apiKey).toBe('[REDACTED]');
  });

  it('should redact patterns in messages', () => {
    const baseSink = new TestSink();
    const sink = new RedactingSink(baseSink, {
      redactFields: [],
      redactPatterns: [/\b\d{16}\b/g], // Credit card pattern
    });

    sink.write({
      timestamp: '2024-01-01T00:00:00.000Z',
      level: 'info',
      message: 'Payment with card 1234567890123456',
      traceId: 'trace',
    });

    expect(baseSink.entries[0].message).toBe('Payment with card [REDACTED]');
  });
});

describe('FileSink', () => {
  it('should track entries written', () => {
    const sink = new FileSink({ path: '/tmp/test.log' });

    sink.write({
      timestamp: '2024-01-01T00:00:00.000Z',
      level: 'info',
      message: 'Test',
      traceId: 'trace',
    });

    const stats = sink.getStats();
    expect(stats.entriesWritten).toBe(1);
  });
});

// =============================================================================
// Standard Context Builder Tests
// =============================================================================

describe('StandardContext', () => {
  it('should build context with common fields', () => {
    const context = new StandardContext()
      .withUserId('user-123')
      .withRequestId('req-456')
      .withDuration(150)
      .withDatabase('mydb')
      .withTable('users')
      .withRowsAffected(10)
      .build();

    expect(context).toEqual({
      userId: 'user-123',
      requestId: 'req-456',
      durationMs: 150,
      database: 'mydb',
      table: 'users',
      rowsAffected: 10,
    });
  });

  it('should support custom fields', () => {
    const context = new StandardContext()
      .with('custom', 'value')
      .with('count', 42)
      .build();

    expect(context).toEqual({
      custom: 'value',
      count: 42,
    });
  });

  it('should return new object on each build', () => {
    const builder = new StandardContext().withUserId('user');
    const context1 = builder.build();
    const context2 = builder.build();

    expect(context1).toEqual(context2);
    expect(context1).not.toBe(context2);
  });
});

// =============================================================================
// Flush Tests
// =============================================================================

describe('Logger Flush', () => {
  it('should flush underlying sink', async () => {
    let flushCalled = false;
    const sink: LogSink = {
      write() {},
      flush: () => { flushCalled = true; },
    };
    const logger = createLogger({ sink });

    await logger.flush();

    expect(flushCalled).toBe(true);
  });

  it('should not throw if sink has no flush', async () => {
    const sink: LogSink = { write() {} };
    const logger = createLogger({ sink });

    await expect(logger.flush()).resolves.not.toThrow();
  });
});

// =============================================================================
// Fallback Sink Tests
// =============================================================================

describe('Fallback Sink', () => {
  it('should use fallback when primary fails', () => {
    const failingSink: LogSink = {
      write() {
        throw new Error('Primary failed');
      },
    };
    const fallbackSink = new TestSink();
    const logger = createLogger({ sink: failingSink, fallbackSink });

    logger.info('Test message');

    expect(fallbackSink.entries).toHaveLength(1);
  });
});
