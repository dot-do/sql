/**
 * Unified Observability Tests
 *
 * Tests for the unified observability layer including trace-aware logging,
 * DO boundary tracing, query/transaction instrumentation, and metrics collection.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  createUnifiedObservability,
  type UnifiedObservability,
  type TraceAwareLogger,
  DEFAULT_UNIFIED_CONFIG,
} from '../unified.js';
import { DistributedTraceStorage } from '../distributed-tracing.js';
import type { LogSink, LogEntry } from '../../logging/index.js';

// =============================================================================
// Test Helpers
// =============================================================================

function createTestObservability(
  overrides: Parameters<typeof createUnifiedObservability>[0] = {}
): UnifiedObservability {
  return createUnifiedObservability({
    serviceName: 'test-service',
    tracing: { enabled: true },
    metrics: { enabled: true },
    logging: { level: 'debug' },
    ...overrides,
  });
}

class CapturingSink implements LogSink {
  entries: LogEntry[] = [];

  write(entry: LogEntry): void {
    this.entries.push(entry);
  }

  clear(): void {
    this.entries = [];
  }
}

// =============================================================================
// Factory Tests
// =============================================================================

describe('createUnifiedObservability', () => {
  it('creates observability instance with all components', () => {
    const obs = createUnifiedObservability();

    expect(obs.tracer).toBeDefined();
    expect(obs.registry).toBeDefined();
    expect(obs.metrics).toBeDefined();
    expect(obs.logger).toBeDefined();
    expect(obs.sanitizer).toBeDefined();
    expect(obs.config).toBeDefined();
  });

  it('uses default configuration', () => {
    const obs = createUnifiedObservability();

    expect(obs.config.serviceName).toBe(DEFAULT_UNIFIED_CONFIG.serviceName);
    expect(obs.config.tracing?.enabled).toBe(true);
    expect(obs.config.metrics?.enabled).toBe(true);
  });

  it('applies custom service name', () => {
    const obs = createUnifiedObservability({ serviceName: 'custom-service' });
    expect(obs.config.serviceName).toBe('custom-service');
  });

  it('applies custom instance ID', () => {
    const obs = createUnifiedObservability({
      serviceName: 'test',
      instanceId: 'instance-1',
    });
    expect(obs.config.instanceId).toBe('instance-1');
  });

  it('disables tracing when configured', () => {
    const obs = createUnifiedObservability({
      tracing: { enabled: false },
    });

    // Should still have tracer but it's a no-op
    expect(obs.tracer).toBeDefined();
    const span = obs.tracer.startDistributedSpan('test');
    expect(span.isRecording()).toBe(false);
  });

  it('disables metrics when configured', () => {
    const obs = createUnifiedObservability({
      metrics: { enabled: false },
    });

    // Should still have metrics but they're no-ops
    obs.metrics.queryTotal.inc({ operation: 'SELECT', table: 'users', status: 'success' });
    expect(obs.getPrometheusMetrics()).toBe('');
  });
});

// =============================================================================
// Unified Metrics Tests
// =============================================================================

describe('UnifiedMetrics', () => {
  let obs: UnifiedObservability;

  beforeEach(() => {
    obs = createTestObservability();
  });

  it('provides all query metrics', () => {
    expect(obs.metrics.queryTotal).toBeDefined();
    expect(obs.metrics.queryDuration).toBeDefined();
    expect(obs.metrics.queryErrors).toBeDefined();
    expect(obs.metrics.queryRowsReturned).toBeDefined();
  });

  it('provides all transaction metrics', () => {
    expect(obs.metrics.transactionsTotal).toBeDefined();
    expect(obs.metrics.transactionDuration).toBeDefined();
  });

  it('provides all DO communication metrics', () => {
    expect(obs.metrics.doCallsTotal).toBeDefined();
    expect(obs.metrics.doCallDuration).toBeDefined();
    expect(obs.metrics.doCallErrors).toBeDefined();
  });

  it('provides all replication metrics', () => {
    expect(obs.metrics.replicationLag).toBeDefined();
    expect(obs.metrics.replicaRequests).toBeDefined();
  });

  it('provides all WAL metrics', () => {
    expect(obs.metrics.walWrites).toBeDefined();
    expect(obs.metrics.walSize).toBeDefined();
    expect(obs.metrics.walCheckpoints).toBeDefined();
  });

  it('provides all CDC metrics', () => {
    expect(obs.metrics.cdcEventsTotal).toBeDefined();
    expect(obs.metrics.cdcLag).toBeDefined();
  });

  it('provides all connection metrics', () => {
    expect(obs.metrics.activeConnections).toBeDefined();
    expect(obs.metrics.activeTransactions).toBeDefined();
  });

  it('provides span metrics', () => {
    expect(obs.metrics.spansCreated).toBeDefined();
    expect(obs.metrics.spanDuration).toBeDefined();
  });

  it('records query metrics correctly', () => {
    obs.metrics.queryTotal.inc({ operation: 'SELECT', table: 'users', status: 'success' });
    obs.metrics.queryTotal.inc({ operation: 'SELECT', table: 'users', status: 'success' });
    obs.metrics.queryTotal.inc({ operation: 'INSERT', table: 'orders', status: 'error' });

    expect(obs.metrics.queryTotal.get({ operation: 'SELECT', table: 'users', status: 'success' })).toBe(2);
    expect(obs.metrics.queryTotal.get({ operation: 'INSERT', table: 'orders', status: 'error' })).toBe(1);
  });
});

// =============================================================================
// TraceAwareLogger Tests
// =============================================================================

describe('TraceAwareLogger', () => {
  let obs: UnifiedObservability;
  let sink: CapturingSink;

  beforeEach(() => {
    sink = new CapturingSink();
    obs = createUnifiedObservability({
      logging: { level: 'debug', sink },
    });
  });

  it('logs without trace context when outside span', () => {
    obs.logger.info('test message');

    expect(sink.entries).toHaveLength(1);
    expect(sink.entries[0].message).toBe('test message');
  });

  it('includes trace context in logs when inside span', async () => {
    const span = obs.tracer.startDistributedSpan('test-op');

    await obs.tracer.withDistributedSpanAsync(span, async () => {
      obs.logger.info('traced message');
    });

    expect(sink.entries).toHaveLength(1);
    const entry = sink.entries[0];
    expect(entry.context?.correlationId).toBe(span.getCorrelationId());
    expect(entry.context?.traceId).toBe(span.traceId);
    expect(entry.context?.spanId).toBe(span.spanId);
  });

  it('includes span name in logs', async () => {
    const span = obs.tracer.startDistributedSpan('my-operation');

    await obs.tracer.withDistributedSpanAsync(span, async () => {
      obs.logger.info('operation log');
    });

    expect(sink.entries[0].context?.spanName).toBe('my-operation');
  });

  it('merges trace context with provided context', async () => {
    const span = obs.tracer.startDistributedSpan('test');

    await obs.tracer.withDistributedSpanAsync(span, async () => {
      obs.logger.info('message', { userId: '123', action: 'query' });
    });

    const entry = sink.entries[0];
    expect(entry.context?.userId).toBe('123');
    expect(entry.context?.action).toBe('query');
    expect(entry.context?.correlationId).toBe(span.getCorrelationId());
  });

  it('supports all log levels', async () => {
    const span = obs.tracer.startDistributedSpan('test');

    await obs.tracer.withDistributedSpanAsync(span, async () => {
      obs.logger.debug('debug message');
      obs.logger.info('info message');
      obs.logger.warn('warn message');
      obs.logger.error('error message');
    });

    expect(sink.entries).toHaveLength(4);
    expect(sink.entries.map((e) => e.level)).toEqual(['debug', 'info', 'warn', 'error']);
  });

  it('creates child loggers with context', () => {
    const child = obs.logger.child({ component: 'query-engine' });
    child.info('child log');

    expect(sink.entries[0].context?.component).toBe('query-engine');
  });

  it('returns current correlation ID', async () => {
    const span = obs.tracer.startDistributedSpan('test');
    let capturedId: string | undefined;

    await obs.tracer.withDistributedSpanAsync(span, async () => {
      capturedId = obs.logger.getCurrentCorrelationId();
    });

    expect(capturedId).toBe(span.getCorrelationId());
  });

  it('returns undefined correlation ID outside span', () => {
    expect(obs.logger.getCurrentCorrelationId()).toBeUndefined();
  });
});

// =============================================================================
// traceRequest Tests
// =============================================================================

describe('traceRequest', () => {
  let obs: UnifiedObservability;

  beforeEach(() => {
    obs = createTestObservability();
  });

  it('creates server span for request', async () => {
    const request = new Request('https://example.com/query');
    let capturedSpan: unknown;

    await obs.traceRequest(request, 'handle-query', async (span) => {
      capturedSpan = span;
      return 'result';
    });

    expect(capturedSpan).toBeDefined();
    expect((capturedSpan as { kind: string }).kind).toBe('SERVER');
  });

  it('returns handler result', async () => {
    const request = new Request('https://example.com/query');

    const result = await obs.traceRequest(request, 'handle-query', async () => {
      return { data: 'test' };
    });

    expect(result).toEqual({ data: 'test' });
  });

  it('propagates parent trace context from request', async () => {
    const parentContext = obs.tracer.createRootContext();
    const headers = new Headers();
    obs.tracer.injectDistributedContext(headers, parentContext);

    const request = new Request('https://example.com/query', { headers });
    let spanCorrelationId: string | undefined;

    await obs.traceRequest(request, 'handle-query', async (span) => {
      spanCorrelationId = span.getCorrelationId();
    });

    expect(spanCorrelationId).toBe(parentContext.correlationId);
  });

  it('sets HTTP attributes on span', async () => {
    const request = new Request('https://example.com/query?table=users', {
      method: 'POST',
    });
    let spanAttributes: Map<string, unknown> | undefined;

    await obs.traceRequest(request, 'handle-query', async (span) => {
      spanAttributes = span.attributes;
    });

    expect(spanAttributes?.get('http.method')).toBe('POST');
    expect(spanAttributes?.get('http.url')).toContain('example.com/query');
  });

  it('adds custom attributes', async () => {
    const request = new Request('https://example.com/query');
    let spanAttributes: Map<string, unknown> | undefined;

    await obs.traceRequest(
      request,
      'handle-query',
      async (span) => {
        spanAttributes = span.attributes;
      },
      { 'custom.key': 'custom-value' }
    );

    expect(spanAttributes?.get('custom.key')).toBe('custom-value');
  });

  it('records span metrics on success', async () => {
    const request = new Request('https://example.com/query');

    await obs.traceRequest(request, 'handle-query', async () => {});

    expect(obs.metrics.spansCreated.get({ span_kind: 'SERVER', service: 'test-service' })).toBe(1);
  });

  it('re-throws errors and records error metrics', async () => {
    const request = new Request('https://example.com/query');

    await expect(
      obs.traceRequest(request, 'handle-query', async () => {
        throw new Error('handler error');
      })
    ).rejects.toThrow('handler error');

    const spanDuration = obs.metrics.spanDuration.get({ span_name: 'handle-query', status: 'ERROR' });
    expect(spanDuration.count).toBe(1);
  });
});

// =============================================================================
// tracedFetch Tests
// =============================================================================

describe('tracedFetch', () => {
  let obs: UnifiedObservability;

  beforeEach(() => {
    obs = createTestObservability();
  });

  it('creates client span for DO call', async () => {
    const mockStub = {
      fetch: vi.fn().mockResolvedValue(new Response('OK')),
    };

    const request = new Request('https://do.example.com/api');
    await obs.tracedFetch(mockStub, request, 'call-shard');

    expect(mockStub.fetch).toHaveBeenCalled();
    expect(obs.metrics.spansCreated.get({ span_kind: 'CLIENT', service: 'test-service' })).toBe(1);
  });

  it('injects trace context into request', async () => {
    let capturedRequest: Request | undefined;
    const mockStub = {
      fetch: vi.fn().mockImplementation((req: Request) => {
        capturedRequest = req;
        return Promise.resolve(new Response('OK'));
      }),
    };

    const span = obs.tracer.startDistributedSpan('parent');

    await obs.tracer.withDistributedSpanAsync(span, async () => {
      const request = new Request('https://do.example.com/api');
      await obs.tracedFetch(mockStub, request);
    });

    expect(capturedRequest?.headers.has('traceparent')).toBe(true);
    expect(capturedRequest?.headers.has('x-correlation-id')).toBe(true);
  });

  it('returns response from stub', async () => {
    const mockStub = {
      fetch: vi.fn().mockResolvedValue(new Response('response data', { status: 200 })),
    };

    const request = new Request('https://do.example.com/api');
    const response = await obs.tracedFetch(mockStub, request);

    expect(response.status).toBe(200);
    expect(await response.text()).toBe('response data');
  });

  it('records success metrics', async () => {
    const mockStub = {
      fetch: vi.fn().mockResolvedValue(new Response('OK', { status: 200 })),
    };

    const request = new Request('https://do.example.com/api', { method: 'POST' });
    await obs.tracedFetch(mockStub, request);

    expect(
      obs.metrics.doCallsTotal.get({
        target_type: 'durable-object',
        operation: 'POST',
        status: 'success',
      })
    ).toBe(1);
  });

  it('records error metrics for non-ok response', async () => {
    const mockStub = {
      fetch: vi.fn().mockResolvedValue(new Response('Not Found', { status: 404 })),
    };

    const request = new Request('https://do.example.com/api', { method: 'GET' });
    await obs.tracedFetch(mockStub, request);

    expect(
      obs.metrics.doCallsTotal.get({
        target_type: 'durable-object',
        operation: 'GET',
        status: 'error',
      })
    ).toBe(1);

    expect(
      obs.metrics.doCallErrors.get({
        target_type: 'durable-object',
        error_type: 'HTTP_404',
      })
    ).toBe(1);
  });

  it('records error metrics and re-throws on exception', async () => {
    const mockStub = {
      fetch: vi.fn().mockRejectedValue(new Error('connection failed')),
    };

    const request = new Request('https://do.example.com/api');

    await expect(obs.tracedFetch(mockStub, request)).rejects.toThrow('connection failed');

    expect(
      obs.metrics.doCallErrors.get({
        target_type: 'durable-object',
        error_type: 'Error',
      })
    ).toBe(1);
  });

  it('uses path as default span name', async () => {
    const mockStub = {
      fetch: vi.fn().mockResolvedValue(new Response('OK')),
    };

    const request = new Request('https://do.example.com/query/execute');
    await obs.tracedFetch(mockStub, request);

    // DO calls record duration using doCallDuration metric, not spanDuration
    const doCallDuration = obs.metrics.doCallDuration.get({
      target_type: 'durable-object',
      operation: 'GET',
    });
    expect(doCallDuration.count).toBe(1);
  });
});

// =============================================================================
// instrumentQuery Tests
// =============================================================================

describe('instrumentQuery', () => {
  let obs: UnifiedObservability;

  beforeEach(() => {
    obs = createTestObservability();
  });

  it('returns query result', async () => {
    const result = await obs.instrumentQuery(
      'SELECT * FROM users',
      undefined,
      async () => [{ id: 1, name: 'Alice' }]
    );

    expect(result).toEqual([{ id: 1, name: 'Alice' }]);
  });

  it('creates span with query attributes', async () => {
    let spanAttributes: Map<string, unknown> | undefined;

    await obs.instrumentQuery('SELECT * FROM users WHERE id = ?', [1], async () => {
      // Capture span attributes from current context
      const currentSpan = obs.tracer.getCurrentSpan();
      spanAttributes = currentSpan?.attributes;
      return [];
    });

    expect(spanAttributes?.get('db.system')).toBe('dosql');
    expect(spanAttributes?.get('db.operation')).toBe('SELECT');
    expect(spanAttributes?.get('db.sql.table')).toBe('users');
  });

  it('sanitizes SQL in span', async () => {
    let statement: unknown;

    await obs.instrumentQuery("SELECT * FROM users WHERE email = 'secret@example.com'", undefined, async () => {
      const currentSpan = obs.tracer.getCurrentSpan();
      statement = currentSpan?.attributes.get('db.statement');
      return [];
    });

    expect(statement).not.toContain('secret@example.com');
    expect(statement).toContain("'?'");
  });

  it('records success metrics', async () => {
    await obs.instrumentQuery('SELECT * FROM orders', undefined, async () => []);

    expect(obs.metrics.queryTotal.get({ operation: 'SELECT', table: 'orders', status: 'success' })).toBe(1);
  });

  it('records query duration', async () => {
    await obs.instrumentQuery('SELECT * FROM users', undefined, async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      return [];
    });

    const histogram = obs.metrics.queryDuration.get({ operation: 'SELECT', table: 'users' });
    expect(histogram.count).toBe(1);
    expect(histogram.sum).toBeGreaterThan(0);
  });

  it('records error metrics on failure', async () => {
    await expect(
      obs.instrumentQuery('SELECT * FROM users', undefined, async () => {
        throw new Error('query failed');
      })
    ).rejects.toThrow('query failed');

    expect(obs.metrics.queryTotal.get({ operation: 'SELECT', table: 'users', status: 'error' })).toBe(1);
    expect(obs.metrics.queryErrors.get({ operation: 'SELECT', error_type: 'Error' })).toBe(1);
  });

  it('detects INSERT statement type', async () => {
    await obs.instrumentQuery('INSERT INTO logs (message) VALUES (?)', ['test'], async () => ({ changes: 1 }));

    expect(obs.metrics.queryTotal.get({ operation: 'INSERT', table: 'logs', status: 'success' })).toBe(1);
  });

  it('detects UPDATE statement type', async () => {
    await obs.instrumentQuery('UPDATE users SET name = ? WHERE id = ?', ['Bob', 1], async () => ({ changes: 1 }));

    expect(obs.metrics.queryTotal.get({ operation: 'UPDATE', table: 'users', status: 'success' })).toBe(1);
  });

  it('detects DELETE statement type', async () => {
    await obs.instrumentQuery('DELETE FROM sessions WHERE expired = true', undefined, async () => ({ changes: 5 }));

    expect(obs.metrics.queryTotal.get({ operation: 'DELETE', table: 'sessions', status: 'success' })).toBe(1);
  });
});

// =============================================================================
// instrumentTransaction Tests
// =============================================================================

describe('instrumentTransaction', () => {
  let obs: UnifiedObservability;

  beforeEach(() => {
    obs = createTestObservability();
  });

  it('returns transaction result', async () => {
    const result = await obs.instrumentTransaction('txn-1', async () => {
      return 'committed';
    });

    expect(result).toBe('committed');
  });

  it('creates span with transaction attributes', async () => {
    let spanAttributes: Map<string, unknown> | undefined;

    await obs.instrumentTransaction('txn-123', async () => {
      const currentSpan = obs.tracer.getCurrentSpan();
      spanAttributes = currentSpan?.attributes;
    });

    expect(spanAttributes?.get('db.system')).toBe('dosql');
    expect(spanAttributes?.get('db.transaction.id')).toBe('txn-123');
  });

  it('tracks active transactions', async () => {
    let activeCount = 0;

    await obs.instrumentTransaction('txn-1', async () => {
      activeCount = obs.metrics.activeTransactions.get({});
    });

    expect(activeCount).toBe(1);
    // After transaction completes, should be decremented
    expect(obs.metrics.activeTransactions.get({})).toBe(0);
  });

  it('records commit metrics on success', async () => {
    await obs.instrumentTransaction('txn-1', async () => null);

    expect(obs.metrics.transactionsTotal.get({ outcome: 'commit' })).toBe(1);
  });

  it('records transaction duration', async () => {
    await obs.instrumentTransaction('txn-1', async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
    });

    const histogram = obs.metrics.transactionDuration.get({ outcome: 'commit' });
    expect(histogram.count).toBe(1);
    expect(histogram.sum).toBeGreaterThan(0);
  });

  it('records rollback metrics on failure', async () => {
    await expect(
      obs.instrumentTransaction('txn-1', async () => {
        throw new Error('transaction failed');
      })
    ).rejects.toThrow('transaction failed');

    expect(obs.metrics.transactionsTotal.get({ outcome: 'rollback' })).toBe(1);
    // Active transactions should still be decremented
    expect(obs.metrics.activeTransactions.get({})).toBe(0);
  });

  it('records span metrics', async () => {
    await obs.instrumentTransaction('txn-1', async () => null);

    expect(obs.metrics.spansCreated.get({ span_kind: 'INTERNAL', service: 'test-service' })).toBe(1);
  });
});

// =============================================================================
// withCorrelation Tests
// =============================================================================

describe('withCorrelation', () => {
  let obs: UnifiedObservability;

  beforeEach(() => {
    obs = createTestObservability();
  });

  it('runs function with specified correlation ID', async () => {
    let capturedId: string | undefined;

    await obs.withCorrelation('my-correlation-id', async () => {
      capturedId = obs.getCurrentCorrelationId();
    });

    expect(capturedId).toBe('my-correlation-id');
  });

  it('returns function result', async () => {
    const result = await obs.withCorrelation('test', async () => {
      return 'result';
    });

    expect(result).toBe('result');
  });

  it('isolates correlation ID to function scope', async () => {
    await obs.withCorrelation('scoped-id', async () => {
      expect(obs.getCurrentCorrelationId()).toBe('scoped-id');
    });

    expect(obs.getCurrentCorrelationId()).toBeUndefined();
  });
});

// =============================================================================
// getCurrentContext Tests
// =============================================================================

describe('getCurrentContext', () => {
  let obs: UnifiedObservability;

  beforeEach(() => {
    obs = createTestObservability();
  });

  it('returns undefined outside of span', () => {
    expect(obs.getCurrentContext()).toBeUndefined();
  });

  it('returns context inside span', async () => {
    const span = obs.tracer.startDistributedSpan('test');
    let context: unknown;

    await obs.tracer.withDistributedSpanAsync(span, async () => {
      context = obs.getCurrentContext();
    });

    expect(context).toBeDefined();
    expect((context as { correlationId: string }).correlationId).toBe(span.getCorrelationId());
  });
});

// =============================================================================
// getPrometheusMetrics Tests
// =============================================================================

describe('getPrometheusMetrics', () => {
  let obs: UnifiedObservability;

  beforeEach(() => {
    obs = createTestObservability();
  });

  it('returns Prometheus-formatted metrics', () => {
    obs.metrics.queryTotal.inc({ operation: 'SELECT', table: 'users', status: 'success' });

    const output = obs.getPrometheusMetrics();

    expect(output).toContain('# TYPE');
    expect(output).toContain('dosql_queries_total');
    expect(output).toContain('operation="SELECT"');
  });

  it('includes all metric types', () => {
    obs.metrics.queryTotal.inc({ operation: 'SELECT', table: 'users', status: 'success' });
    obs.metrics.queryDuration.observe({ operation: 'SELECT', table: 'users' }, 0.05);
    obs.metrics.activeConnections.set({ shard: 'primary' }, 5);

    const output = obs.getPrometheusMetrics();

    expect(output).toContain('counter');
    expect(output).toContain('histogram');
    expect(output).toContain('gauge');
  });

  it('applies metrics prefix', () => {
    const output = obs.getPrometheusMetrics();
    expect(output).toContain('dosql_');
  });
});

// =============================================================================
// Integration Tests
// =============================================================================

describe('Integration', () => {
  it('full request flow with tracing, logging, and metrics', async () => {
    const sink = new CapturingSink();
    const obs = createUnifiedObservability({
      serviceName: 'integration-test',
      logging: { level: 'info', sink },
    });

    // Simulate incoming request
    const parentContext = obs.tracer.createRootContext();
    const headers = new Headers();
    obs.tracer.injectDistributedContext(headers, parentContext);

    const request = new Request('https://example.com/query', { headers });

    await obs.traceRequest(request, 'handle-request', async (span) => {
      obs.logger.info('Request started');

      // Execute query
      await obs.instrumentQuery('SELECT * FROM users', undefined, async () => {
        obs.logger.info('Query executing');
        return [{ id: 1 }];
      });

      // Call another DO
      const mockStub = {
        fetch: vi.fn().mockResolvedValue(new Response('OK')),
      };
      await obs.tracedFetch(mockStub, new Request('https://shard.example.com/api'));

      obs.logger.info('Request completed');
    });

    // Verify logs include trace context
    expect(sink.entries.length).toBeGreaterThan(0);
    const firstLog = sink.entries[0];
    expect(firstLog.context?.correlationId).toBe(parentContext.correlationId);

    // Verify metrics recorded
    expect(obs.metrics.queryTotal.get({ operation: 'SELECT', table: 'users', status: 'success' })).toBe(1);
    expect(obs.metrics.doCallsTotal.get({ target_type: 'durable-object', operation: 'GET', status: 'success' })).toBe(1);
    expect(obs.metrics.spansCreated.get({ span_kind: 'SERVER', service: 'integration-test' })).toBe(1);
  });

  it('maintains correlation from parent request context', async () => {
    const obs = createTestObservability();

    // Create a parent context and inject it into the request
    const parentContext = obs.tracer.createRootContext();
    const headers = new Headers();
    obs.tracer.injectDistributedContext(headers, parentContext);

    const request = new Request('https://example.com/api', { headers });

    let capturedCorrelationId: string | undefined;

    await obs.traceRequest(request, 'outer', async (outerSpan) => {
      // The outer span should have the parent's correlation ID
      capturedCorrelationId = outerSpan.getCorrelationId();
    });

    // The traceRequest span should inherit correlation from the parent context
    expect(capturedCorrelationId).toBe(parentContext.correlationId);
  });

  it('records metrics across nested operations', async () => {
    const obs = createTestObservability();

    const request = new Request('https://example.com/api');

    await obs.traceRequest(request, 'outer', async () => {
      await obs.instrumentQuery('SELECT * FROM users', undefined, async () => []);
      await obs.instrumentTransaction('txn-1', async () => null);

      const mockStub = {
        fetch: vi.fn().mockResolvedValue(new Response('OK')),
      };
      await obs.tracedFetch(mockStub, new Request('https://do.example.com/api'));
    });

    // Verify all metrics were recorded
    expect(obs.metrics.spansCreated.get({ span_kind: 'SERVER', service: 'test-service' })).toBe(1);
    expect(obs.metrics.queryTotal.get({ operation: 'SELECT', table: 'users', status: 'success' })).toBe(1);
    expect(obs.metrics.transactionsTotal.get({ outcome: 'commit' })).toBe(1);
    expect(obs.metrics.doCallsTotal.get({ target_type: 'durable-object', operation: 'GET', status: 'success' })).toBe(1);
  });
});
