/**
 * Distributed Tracing Tests
 *
 * Tests for distributed tracing with correlation IDs, context propagation
 * across DO boundaries, baggage support, and async context tracking.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  createDistributedTracer,
  DistributedTracerImpl,
  NoOpDistributedTracer,
  DistributedTraceStorage,
  prepareTracedFetch,
  startServerSpan,
  withDistributedContext,
  type DistributedTracer,
  type DistributedTraceContext,
  type DistributedSpan,
} from '../distributed-tracing.js';

// =============================================================================
// Test Helpers
// =============================================================================

function createTestTracer(overrides: Parameters<typeof createDistributedTracer>[0] = {}): DistributedTracer {
  return createDistributedTracer({
    enabled: true,
    serviceName: 'test-service',
    sampler: 'always_on',
    samplingRate: 1.0,
    ...overrides,
  });
}

// =============================================================================
// Factory Tests
// =============================================================================

describe('createDistributedTracer', () => {
  it('returns DistributedTracerImpl when enabled', () => {
    const tracer = createDistributedTracer({ enabled: true });
    expect(tracer).toBeInstanceOf(DistributedTracerImpl);
  });

  it('returns NoOpDistributedTracer when disabled', () => {
    const tracer = createDistributedTracer({ enabled: false });
    expect(tracer).toBeInstanceOf(NoOpDistributedTracer);
  });

  it('uses default configuration', () => {
    const tracer = createDistributedTracer();
    const span = tracer.startDistributedSpan('test');
    expect(span.attributes.get('service.name')).toBe('dosql');
  });

  it('uses custom service name', () => {
    const tracer = createDistributedTracer({ serviceName: 'my-service' });
    const span = tracer.startDistributedSpan('test');
    expect(span.attributes.get('service.name')).toBe('my-service');
  });
});

// =============================================================================
// DistributedSpan Tests
// =============================================================================

describe('DistributedSpan', () => {
  let tracer: DistributedTracer;

  beforeEach(() => {
    tracer = createTestTracer();
  });

  it('creates a span with correlation ID', () => {
    const span = tracer.startDistributedSpan('test.operation');
    expect(span.getCorrelationId()).toBeDefined();
    expect(span.getCorrelationId()).not.toBe('');
  });

  it('generates unique correlation IDs for root spans', () => {
    const span1 = tracer.startDistributedSpan('op1');
    span1.end();
    const span2 = tracer.startDistributedSpan('op2');

    expect(span1.getCorrelationId()).not.toBe(span2.getCorrelationId());
  });

  it('preserves correlation ID in child spans', () => {
    const parentContext = tracer.createRootContext();
    const span = tracer.startDistributedSpan('child', { parentContext });

    expect(span.getCorrelationId()).toBe(parentContext.correlationId);
  });

  it('sets correlation.id attribute', () => {
    const span = tracer.startDistributedSpan('test');
    expect(span.attributes.get('correlation.id')).toBe(span.getCorrelationId());
  });

  describe('baggage', () => {
    it('supports setting and getting baggage items', () => {
      const span = tracer.startDistributedSpan('test');
      span.setBaggage('user-id', '12345');
      span.setBaggage('tenant', 'acme');

      expect(span.getBaggage('user-id')).toBe('12345');
      expect(span.getBaggage('tenant')).toBe('acme');
    });

    it('returns undefined for non-existent baggage', () => {
      const span = tracer.startDistributedSpan('test');
      expect(span.getBaggage('missing')).toBeUndefined();
    });

    it('returns all baggage items', () => {
      const span = tracer.startDistributedSpan('test');
      span.setBaggage('key1', 'value1');
      span.setBaggage('key2', 'value2');

      const all = span.getAllBaggage();
      expect(all.get('key1')).toBe('value1');
      expect(all.get('key2')).toBe('value2');
    });

    it('inherits baggage from parent context', () => {
      const parentContext = tracer.createRootContext();
      parentContext.baggage.set('inherited', 'value');

      const span = tracer.startDistributedSpan('child', { parentContext });
      expect(span.getBaggage('inherited')).toBe('value');
    });

    it('can add baggage via options', () => {
      const span = tracer.startDistributedSpan('test', {
        baggage: { 'from-options': 'option-value' },
      });

      expect(span.getBaggage('from-options')).toBe('option-value');
    });
  });

  describe('distributed context', () => {
    it('returns distributed context for propagation', () => {
      const span = tracer.startDistributedSpan('test');
      span.setBaggage('key', 'value');

      const context = span.getDistributedContext();

      expect(context.traceId).toBe(span.traceId);
      expect(context.spanId).toBe(span.spanId);
      expect(context.correlationId).toBe(span.getCorrelationId());
      expect(context.baggage.get('key')).toBe('value');
    });

    it('includes sampling decision', () => {
      const span = tracer.startDistributedSpan('test');
      const context = span.getDistributedContext();

      expect(context.sampled).toBe(true);
      expect(context.traceFlags).toBe(1);
    });
  });

  describe('elapsed time', () => {
    it('calculates elapsed time for active span', async () => {
      const span = tracer.startDistributedSpan('test');
      await new Promise((resolve) => setTimeout(resolve, 10));
      const elapsed = span.getElapsedMs();

      expect(elapsed).toBeGreaterThanOrEqual(10);
    });

    it('calculates elapsed time for ended span', async () => {
      const span = tracer.startDistributedSpan('test');
      await new Promise((resolve) => setTimeout(resolve, 10));
      span.end();

      const elapsed = span.getElapsedMs();
      expect(elapsed).toBeGreaterThanOrEqual(10);
    });
  });
});

// =============================================================================
// Context Propagation Tests
// =============================================================================

describe('Context Propagation', () => {
  let tracer: DistributedTracer;

  beforeEach(() => {
    tracer = createTestTracer();
  });

  describe('extractDistributedContext', () => {
    it('extracts W3C traceparent header', () => {
      const headers = new Headers();
      headers.set('traceparent', '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');
      headers.set('x-correlation-id', 'my-correlation-id');

      const context = tracer.extractDistributedContext(headers);

      expect(context).not.toBeNull();
      expect(context!.traceId).toBe('0af7651916cd43dd8448eb211c80319c');
      expect(context!.spanId).toBe('b7ad6b7169203331');
      expect(context!.traceFlags).toBe(1);
      expect(context!.correlationId).toBe('my-correlation-id');
    });

    it('generates correlation ID if not present', () => {
      const headers = new Headers();
      headers.set('traceparent', '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');

      const context = tracer.extractDistributedContext(headers);

      expect(context).not.toBeNull();
      expect(context!.correlationId).toBeDefined();
      expect(context!.correlationId.length).toBeGreaterThan(0);
    });

    it('extracts baggage header', () => {
      const headers = new Headers();
      headers.set('traceparent', '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');
      headers.set('baggage', 'userId=12345,tenant=acme');

      const context = tracer.extractDistributedContext(headers);

      expect(context!.baggage.get('userId')).toBe('12345');
      expect(context!.baggage.get('tenant')).toBe('acme');
    });

    it('handles URL-encoded baggage values', () => {
      const headers = new Headers();
      headers.set('traceparent', '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');
      headers.set('baggage', 'name=John%20Doe');

      const context = tracer.extractDistributedContext(headers);

      expect(context!.baggage.get('name')).toBe('John Doe');
    });

    it('returns null for missing traceparent', () => {
      const headers = new Headers();
      const context = tracer.extractDistributedContext(headers);
      expect(context).toBeNull();
    });

    it('creates context from correlation ID alone', () => {
      const headers = new Headers();
      headers.set('x-correlation-id', 'standalone-correlation');

      const context = tracer.extractDistributedContext(headers);

      expect(context).not.toBeNull();
      expect(context!.correlationId).toBe('standalone-correlation');
      expect(context!.traceId).toMatch(/^[0-9a-f]{32}$/);
    });

    it('rejects invalid trace ID (all zeros)', () => {
      const headers = new Headers();
      headers.set('traceparent', '00-00000000000000000000000000000000-b7ad6b7169203331-01');

      const context = tracer.extractDistributedContext(headers);
      expect(context).toBeNull();
    });

    it('rejects invalid span ID (all zeros)', () => {
      const headers = new Headers();
      headers.set('traceparent', '00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01');

      const context = tracer.extractDistributedContext(headers);
      expect(context).toBeNull();
    });

    it('rejects invalid version', () => {
      const headers = new Headers();
      headers.set('traceparent', '01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');

      const context = tracer.extractDistributedContext(headers);
      expect(context).toBeNull();
    });

    it('rejects malformed traceparent', () => {
      const headers = new Headers();
      headers.set('traceparent', 'invalid-format');

      const context = tracer.extractDistributedContext(headers);
      expect(context).toBeNull();
    });

    it('determines sampled from trace flags', () => {
      const headers = new Headers();
      headers.set('traceparent', '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00');

      const context = tracer.extractDistributedContext(headers);
      expect(context!.sampled).toBe(false);
    });
  });

  describe('injectDistributedContext', () => {
    it('injects W3C traceparent header', () => {
      const context = tracer.createRootContext();
      const headers = new Headers();

      tracer.injectDistributedContext(headers, context);

      const traceparent = headers.get('traceparent');
      expect(traceparent).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/);
    });

    it('injects correlation ID header', () => {
      const context = tracer.createRootContext();
      const headers = new Headers();

      tracer.injectDistributedContext(headers, context);

      expect(headers.get('x-correlation-id')).toBe(context.correlationId);
    });

    it('injects baggage header', () => {
      const context = tracer.createRootContext();
      context.baggage.set('key1', 'value1');
      context.baggage.set('key2', 'value2');

      const headers = new Headers();
      tracer.injectDistributedContext(headers, context);

      const baggage = headers.get('baggage');
      expect(baggage).toContain('key1=value1');
      expect(baggage).toContain('key2=value2');
    });

    it('URL-encodes baggage values', () => {
      const context = tracer.createRootContext();
      context.baggage.set('name', 'John Doe');

      const headers = new Headers();
      tracer.injectDistributedContext(headers, context);

      expect(headers.get('baggage')).toContain('name=John%20Doe');
    });

    it('injects tracestate when present', () => {
      const context = tracer.createRootContext();
      context.traceState = 'vendor=opaque';

      const headers = new Headers();
      tracer.injectDistributedContext(headers, context);

      expect(headers.get('tracestate')).toBe('vendor=opaque');
    });

    it('omits baggage header when empty', () => {
      const context = tracer.createRootContext();
      const headers = new Headers();

      tracer.injectDistributedContext(headers, context);

      expect(headers.has('baggage')).toBe(false);
    });
  });

  describe('round-trip', () => {
    it('preserves context through inject/extract', () => {
      const original = tracer.createRootContext();
      original.baggage.set('user', 'test');
      original.traceState = 'state=value';

      const headers = new Headers();
      tracer.injectDistributedContext(headers, original);
      const extracted = tracer.extractDistributedContext(headers);

      expect(extracted).not.toBeNull();
      expect(extracted!.traceId).toBe(original.traceId);
      expect(extracted!.spanId).toBe(original.spanId);
      expect(extracted!.correlationId).toBe(original.correlationId);
      expect(extracted!.baggage.get('user')).toBe('test');
      expect(extracted!.traceState).toBe('state=value');
    });
  });
});

// =============================================================================
// Async Context (AsyncLocalStorage) Tests
// =============================================================================

describe('Async Context Propagation', () => {
  let tracer: DistributedTracer;

  beforeEach(() => {
    tracer = createTestTracer();
  });

  it('propagates context via withDistributedSpanAsync', async () => {
    const span = tracer.startDistributedSpan('root');
    let capturedCorrelationId: string | undefined;

    await tracer.withDistributedSpanAsync(span, async () => {
      capturedCorrelationId = tracer.getCurrentCorrelationId();
    });

    expect(capturedCorrelationId).toBe(span.getCorrelationId());
  });

  it('propagates context to nested async operations', async () => {
    const span = tracer.startDistributedSpan('root');
    const correlationIds: (string | undefined)[] = [];

    await tracer.withDistributedSpanAsync(span, async () => {
      correlationIds.push(tracer.getCurrentCorrelationId());

      await new Promise((resolve) => setTimeout(resolve, 1));
      correlationIds.push(tracer.getCurrentCorrelationId());

      await Promise.resolve();
      correlationIds.push(tracer.getCurrentCorrelationId());
    });

    expect(correlationIds).toHaveLength(3);
    expect(correlationIds.every((id) => id === span.getCorrelationId())).toBe(true);
  });

  it('isolates context between parallel operations', async () => {
    const span1 = tracer.startDistributedSpan('op1');
    const span2 = tracer.startDistributedSpan('op2');
    const results: { id: string; correlation: string | undefined }[] = [];

    await Promise.all([
      tracer.withDistributedSpanAsync(span1, async () => {
        await new Promise((resolve) => setTimeout(resolve, 5));
        results.push({ id: 'op1', correlation: tracer.getCurrentCorrelationId() });
      }),
      tracer.withDistributedSpanAsync(span2, async () => {
        results.push({ id: 'op2', correlation: tracer.getCurrentCorrelationId() });
      }),
    ]);

    const op1Result = results.find((r) => r.id === 'op1');
    const op2Result = results.find((r) => r.id === 'op2');

    expect(op1Result?.correlation).toBe(span1.getCorrelationId());
    expect(op2Result?.correlation).toBe(span2.getCorrelationId());
  });

  it('supports withDistributedContext helper', async () => {
    const context = tracer.createRootContext();
    let capturedContext: DistributedTraceContext | undefined;

    await withDistributedContext(context, () => {
      capturedContext = DistributedTraceStorage.getStore();
    });

    expect(capturedContext).toEqual(context);
  });

  it('returns undefined outside of context', () => {
    expect(tracer.getCurrentCorrelationId()).toBeUndefined();
    expect(tracer.getCurrentDistributedContext()).toBeUndefined();
  });
});

// =============================================================================
// Root Context Tests
// =============================================================================

describe('createRootContext', () => {
  let tracer: DistributedTracer;

  beforeEach(() => {
    tracer = createTestTracer({ serviceName: 'my-service' });
  });

  it('creates context with valid trace ID', () => {
    const context = tracer.createRootContext();
    expect(context.traceId).toMatch(/^[0-9a-f]{32}$/);
  });

  it('creates context with valid span ID', () => {
    const context = tracer.createRootContext();
    expect(context.spanId).toMatch(/^[0-9a-f]{16}$/);
  });

  it('creates context with unique correlation ID', () => {
    const context1 = tracer.createRootContext();
    const context2 = tracer.createRootContext();

    expect(context1.correlationId).not.toBe(context2.correlationId);
  });

  it('sets origin service from config', () => {
    const context = tracer.createRootContext();
    expect(context.originService).toBe('my-service');
  });

  it('allows override of origin service', () => {
    const context = tracer.createRootContext('override-service');
    expect(context.originService).toBe('override-service');
  });

  it('sets start timestamp', () => {
    const before = Date.now();
    const context = tracer.createRootContext();
    const after = Date.now();

    expect(context.startTimestamp).toBeGreaterThanOrEqual(before);
    expect(context.startTimestamp).toBeLessThanOrEqual(after);
  });

  it('initializes empty baggage', () => {
    const context = tracer.createRootContext();
    expect(context.baggage.size).toBe(0);
  });

  it('sets sampled to true', () => {
    const context = tracer.createRootContext();
    expect(context.sampled).toBe(true);
    expect(context.traceFlags).toBe(1);
  });
});

// =============================================================================
// DO Boundary Helpers Tests
// =============================================================================

describe('DO Boundary Helpers', () => {
  let tracer: DistributedTracer;

  beforeEach(() => {
    tracer = createTestTracer();
  });

  describe('prepareTracedFetch', () => {
    it('injects trace context into request headers', async () => {
      const span = tracer.startDistributedSpan('test');

      await tracer.withDistributedSpanAsync(span, async () => {
        const originalRequest = new Request('https://do.example.com/api');
        const tracedRequest = prepareTracedFetch(tracer, originalRequest);

        expect(tracedRequest.headers.has('traceparent')).toBe(true);
        expect(tracedRequest.headers.has('x-correlation-id')).toBe(true);
      });
    });

    it('preserves original request properties', async () => {
      const span = tracer.startDistributedSpan('test');

      await tracer.withDistributedSpanAsync(span, async () => {
        const originalRequest = new Request('https://do.example.com/api', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        });
        const tracedRequest = prepareTracedFetch(tracer, originalRequest);

        expect(tracedRequest.method).toBe('POST');
        expect(tracedRequest.headers.get('Content-Type')).toBe('application/json');
      });
    });

    it('adds additional headers', async () => {
      const span = tracer.startDistributedSpan('test');

      await tracer.withDistributedSpanAsync(span, async () => {
        const originalRequest = new Request('https://do.example.com/api');
        const tracedRequest = prepareTracedFetch(tracer, originalRequest, {
          'X-Custom-Header': 'custom-value',
        });

        expect(tracedRequest.headers.get('X-Custom-Header')).toBe('custom-value');
      });
    });

    it('returns original request when no context', () => {
      const originalRequest = new Request('https://do.example.com/api');
      const result = prepareTracedFetch(tracer, originalRequest);

      // Should return same request when no context
      expect(result.url).toBe(originalRequest.url);
    });
  });

  describe('startServerSpan', () => {
    it('extracts context from request and creates server span', () => {
      const parentContext = tracer.createRootContext();
      const headers = new Headers();
      tracer.injectDistributedContext(headers, parentContext);

      const request = new Request('https://do.example.com/query', {
        method: 'POST',
        headers,
      });

      const span = startServerSpan(tracer, request, 'handle-query');

      expect(span.kind).toBe('SERVER');
      expect(span.name).toBe('handle-query');
      expect(span.getCorrelationId()).toBe(parentContext.correlationId);
    });

    it('sets HTTP attributes', () => {
      const request = new Request('https://do.example.com/query?foo=bar', {
        method: 'POST',
      });

      const span = startServerSpan(tracer, request, 'handle-query');

      expect(span.attributes.get('http.method')).toBe('POST');
      expect(span.attributes.get('http.url')).toBe('https://do.example.com/query?foo=bar');
      expect(span.attributes.get('http.target')).toBe('/query');
    });

    it('includes additional attributes', () => {
      const request = new Request('https://do.example.com/query');

      const span = startServerSpan(tracer, request, 'handle-query', {
        'db.operation': 'SELECT',
        'custom.key': 42,
      });

      expect(span.attributes.get('db.operation')).toBe('SELECT');
      expect(span.attributes.get('custom.key')).toBe(42);
    });

    it('creates new context when no parent', () => {
      const request = new Request('https://do.example.com/query');
      const span = startServerSpan(tracer, request, 'handle-query');

      expect(span.getCorrelationId()).toBeDefined();
      expect(span.traceId).toMatch(/^[0-9a-f]{32}$/);
    });
  });
});

// =============================================================================
// NoOpDistributedTracer Tests
// =============================================================================

describe('NoOpDistributedTracer', () => {
  let tracer: NoOpDistributedTracer;

  beforeEach(() => {
    tracer = new NoOpDistributedTracer();
  });

  it('returns non-recording spans', () => {
    const span = tracer.startDistributedSpan('test');
    expect(span.isRecording()).toBe(false);
  });

  it('returns noop correlation ID', () => {
    const span = tracer.startDistributedSpan('test');
    expect(span.getCorrelationId()).toBe('noop');
  });

  it('executes functions without context', async () => {
    const span = tracer.startDistributedSpan('test');
    const result = await tracer.withDistributedSpanAsync(span, async () => 'result');
    expect(result).toBe('result');
  });

  it('returns null for extractDistributedContext', () => {
    const headers = new Headers();
    headers.set('traceparent', '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');

    expect(tracer.extractDistributedContext(headers)).toBeNull();
  });

  it('does nothing for injectDistributedContext', () => {
    const headers = new Headers();
    tracer.injectDistributedContext(headers, tracer.createRootContext());

    expect(headers.has('traceparent')).toBe(false);
  });

  it('returns undefined for getCurrentCorrelationId', () => {
    expect(tracer.getCurrentCorrelationId()).toBeUndefined();
  });

  it('creates noop root context', () => {
    const context = tracer.createRootContext();

    expect(context.traceId).toBe('00000000000000000000000000000000');
    expect(context.spanId).toBe('0000000000000000');
    expect(context.correlationId).toBe('noop');
    expect(context.sampled).toBe(false);
  });
});

// =============================================================================
// Sampling Tests
// =============================================================================

describe('Sampling', () => {
  it('always_on sampler creates recording spans', () => {
    const tracer = createDistributedTracer({ enabled: true, sampler: 'always_on' });
    const span = tracer.startDistributedSpan('test');

    expect(span.isRecording()).toBe(true);
  });

  it('always_off sampler creates non-recording spans', () => {
    const tracer = createDistributedTracer({ enabled: true, sampler: 'always_off' });
    const span = tracer.startDistributedSpan('test');

    expect(span.isRecording()).toBe(false);
  });

  it('probability sampler respects sampling rate', () => {
    const tracer = createDistributedTracer({
      enabled: true,
      sampler: 'probability',
      samplingRate: 0, // Never sample
    });

    const span = tracer.startDistributedSpan('test');
    expect(span.isRecording()).toBe(false);
  });

  it('probability sampler with 1.0 rate always samples', () => {
    const tracer = createDistributedTracer({
      enabled: true,
      sampler: 'probability',
      samplingRate: 1.0,
    });

    // With 100% rate, should always sample
    const spans = Array.from({ length: 10 }, () =>
      tracer.startDistributedSpan('test')
    );

    expect(spans.every((s) => s.isRecording())).toBe(true);
  });
});

// =============================================================================
// Span Kind Tests
// =============================================================================

describe('Span Kinds', () => {
  let tracer: DistributedTracer;

  beforeEach(() => {
    tracer = createTestTracer();
  });

  it('defaults to INTERNAL kind', () => {
    const span = tracer.startDistributedSpan('test');
    expect(span.kind).toBe('INTERNAL');
  });

  it('supports SERVER kind', () => {
    const span = tracer.startDistributedSpan('test', { kind: 'SERVER' });
    expect(span.kind).toBe('SERVER');
  });

  it('supports CLIENT kind', () => {
    const span = tracer.startDistributedSpan('test', { kind: 'CLIENT' });
    expect(span.kind).toBe('CLIENT');
  });

  it('supports PRODUCER kind', () => {
    const span = tracer.startDistributedSpan('test', { kind: 'PRODUCER' });
    expect(span.kind).toBe('PRODUCER');
  });

  it('supports CONSUMER kind', () => {
    const span = tracer.startDistributedSpan('test', { kind: 'CONSUMER' });
    expect(span.kind).toBe('CONSUMER');
  });
});

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('Error Handling', () => {
  let tracer: DistributedTracer;

  beforeEach(() => {
    tracer = createTestTracer();
  });

  it('sets ERROR status on exception in withDistributedSpan', () => {
    const span = tracer.startDistributedSpan('test');

    expect(() =>
      tracer.withDistributedSpan(span, () => {
        throw new Error('test error');
      })
    ).toThrow('test error');

    expect(span.status).toBe('ERROR');
    expect(span.statusMessage).toBe('test error');
  });

  it('adds exception event on error', () => {
    const span = tracer.startDistributedSpan('test');

    expect(() =>
      tracer.withDistributedSpan(span, () => {
        throw new TypeError('type error');
      })
    ).toThrow('type error');

    expect(span.events.length).toBe(1);
    expect(span.events[0].name).toBe('exception');
    expect(span.events[0].attributes?.get('exception.type')).toBe('TypeError');
    expect(span.events[0].attributes?.get('exception.message')).toBe('type error');
  });

  it('handles non-Error throws', () => {
    const span = tracer.startDistributedSpan('test');

    expect(() =>
      tracer.withDistributedSpan(span, () => {
        throw 'string error';
      })
    ).toThrow('string error');

    expect(span.status).toBe('ERROR');
    expect(span.statusMessage).toBe('string error');
  });
});
