/**
 * Tracer Tests
 *
 * Tests for OpenTelemetry-compatible tracing including span creation,
 * status management, context propagation, and sampling.
 */

import { describe, it, expect } from 'vitest';
import { TracerImpl, NoOpTracer, createTracer } from '../tracer.js';
import type { TracingConfig, SpanOptions } from '../types.js';

function makeConfig(overrides: Partial<TracingConfig> = {}): TracingConfig {
  return {
    enabled: true,
    serviceName: 'test-service',
    sampler: 'always_on',
    samplingRate: 1.0,
    maxAttributeLength: 256,
    ...overrides,
  };
}

// =============================================================================
// Span Lifecycle
// =============================================================================

describe('TracerImpl - Span Lifecycle', () => {
  it('creates a span with a name and default kind', () => {
    const tracer = new TracerImpl(makeConfig());
    const span = tracer.startSpan('test.operation');

    expect(span.name).toBe('test.operation');
    expect(span.kind).toBe('INTERNAL');
    expect(span.isRecording()).toBe(true);
    expect(span.traceId).toMatch(/^[0-9a-f]{32}$/);
    expect(span.spanId).toMatch(/^[0-9a-f]{16}$/);
  });

  it('sets the service.name attribute from config', () => {
    const tracer = new TracerImpl(makeConfig({ serviceName: 'my-svc' }));
    const span = tracer.startSpan('op');

    expect(span.attributes.get('service.name')).toBe('my-svc');
  });

  it('generates unique span IDs', () => {
    const tracer = new TracerImpl(makeConfig());
    const span1 = tracer.startSpan('a');
    const span2 = tracer.startSpan('b');

    expect(span1.spanId).not.toBe(span2.spanId);
  });

  it('applies initial attributes from options', () => {
    const tracer = new TracerImpl(makeConfig());
    const span = tracer.startSpan('op', {
      attributes: { 'db.system': 'dosql', 'db.operation': 'SELECT' },
    });

    expect(span.attributes.get('db.system')).toBe('dosql');
    expect(span.attributes.get('db.operation')).toBe('SELECT');
  });

  it('allows setting attributes after creation', () => {
    const tracer = new TracerImpl(makeConfig());
    const span = tracer.startSpan('op');
    span.setAttribute('custom.key', 'value');

    expect(span.attributes.get('custom.key')).toBe('value');
  });

  it('ends a span and stops recording', () => {
    const tracer = new TracerImpl(makeConfig());
    const span = tracer.startSpan('op');

    expect(span.isRecording()).toBe(true);
    span.end();
    expect(span.isRecording()).toBe(false);
    expect(span.endTime).toBeDefined();
  });

  it('sets status to OK on end if still UNSET', () => {
    const tracer = new TracerImpl(makeConfig());
    const span = tracer.startSpan('op');
    expect(span.status).toBe('UNSET');

    span.end();
    expect(span.status).toBe('OK');
  });

  it('preserves explicit status on end', () => {
    const tracer = new TracerImpl(makeConfig());
    const span = tracer.startSpan('op');
    span.setStatus('ERROR', 'something failed');
    span.end();

    expect(span.status).toBe('ERROR');
    expect(span.statusMessage).toBe('something failed');
  });

  it('ignores mutations after end', () => {
    const tracer = new TracerImpl(makeConfig());
    const span = tracer.startSpan('op');
    span.end();

    span.setAttribute('late', 'value');
    span.setStatus('ERROR', 'late error');
    span.addEvent('late-event');

    expect(span.attributes.has('late')).toBe(false);
    expect(span.status).toBe('OK');
    expect(span.events.length).toBe(0);
  });
});

// =============================================================================
// Span Events
// =============================================================================

describe('TracerImpl - Span Events', () => {
  it('records events with name and timestamp', () => {
    const tracer = new TracerImpl(makeConfig());
    const span = tracer.startSpan('op');
    span.addEvent('query.start');

    expect(span.events.length).toBe(1);
    expect(span.events[0].name).toBe('query.start');
    expect(span.events[0].timestamp).toBeGreaterThan(0);
  });

  it('records events with attributes', () => {
    const tracer = new TracerImpl(makeConfig());
    const span = tracer.startSpan('op');
    span.addEvent('exception', {
      'exception.type': 'TypeError',
      'exception.message': 'null ref',
    });

    expect(span.events[0].attributes?.get('exception.type')).toBe('TypeError');
    expect(span.events[0].attributes?.get('exception.message')).toBe('null ref');
  });
});

// =============================================================================
// Parent-child Span Relationships
// =============================================================================

describe('TracerImpl - Span Hierarchy', () => {
  it('sets parent span ID from current span', () => {
    const tracer = new TracerImpl(makeConfig());
    const parent = tracer.startSpan('parent');
    const child = tracer.startSpan('child');

    expect(child.parentSpanId).toBe(parent.spanId);
  });

  it('sets parent from explicit trace context', () => {
    const tracer = new TracerImpl(makeConfig());
    const parentContext = {
      traceId: 'aabbccdd11223344aabbccdd11223344',
      spanId: 'aabbccdd11223344',
      traceFlags: 1,
    };

    const span = tracer.startSpan('child', { parent: parentContext });

    expect(span.traceId).toBe(parentContext.traceId);
    expect(span.parentSpanId).toBe(parentContext.spanId);
  });

  it('tracks current span', () => {
    const tracer = new TracerImpl(makeConfig());
    expect(tracer.getCurrentSpan()).toBeUndefined();

    const span = tracer.startSpan('op');
    expect(tracer.getCurrentSpan()).toBe(span);
  });
});

// =============================================================================
// withSpan / withSpanAsync
// =============================================================================

describe('TracerImpl - withSpan', () => {
  it('executes function and sets OK status on success', () => {
    const tracer = new TracerImpl(makeConfig());
    const span = tracer.startSpan('op');

    const result = tracer.withSpan(span, () => 42);

    expect(result).toBe(42);
    expect(span.status).toBe('OK');
    expect(span.isRecording()).toBe(false);
  });

  it('sets ERROR status and re-throws on failure', () => {
    const tracer = new TracerImpl(makeConfig());
    const span = tracer.startSpan('op');

    expect(() =>
      tracer.withSpan(span, () => {
        throw new Error('boom');
      })
    ).toThrow('boom');

    expect(span.status).toBe('ERROR');
    expect(span.statusMessage).toBe('boom');
    expect(span.events.length).toBe(1);
    expect(span.events[0].name).toBe('exception');
  });
});

describe('TracerImpl - withSpanAsync', () => {
  it('executes async function and sets OK status on success', async () => {
    const tracer = new TracerImpl(makeConfig());
    const span = tracer.startSpan('op');

    const result = await tracer.withSpanAsync(span, async () => 'hello');

    expect(result).toBe('hello');
    expect(span.status).toBe('OK');
    expect(span.isRecording()).toBe(false);
  });

  it('sets ERROR status and re-throws on async failure', async () => {
    const tracer = new TracerImpl(makeConfig());
    const span = tracer.startSpan('op');

    await expect(
      tracer.withSpanAsync(span, async () => {
        throw new Error('async boom');
      })
    ).rejects.toThrow('async boom');

    expect(span.status).toBe('ERROR');
    expect(span.statusMessage).toBe('async boom');
    expect(span.events.length).toBe(1);
    expect(span.events[0].name).toBe('exception');
    expect(span.events[0].attributes?.get('exception.type')).toBe('Error');
    expect(span.events[0].attributes?.get('exception.message')).toBe('async boom');
  });
});

// =============================================================================
// W3C Trace Context Propagation
// =============================================================================

describe('TracerImpl - W3C Trace Context', () => {
  it('extracts valid traceparent header', () => {
    const tracer = new TracerImpl(makeConfig());
    const headers = new Headers();
    headers.set('traceparent', '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');

    const ctx = tracer.extractContext(headers);

    expect(ctx).not.toBeNull();
    expect(ctx!.traceId).toBe('0af7651916cd43dd8448eb211c80319c');
    expect(ctx!.spanId).toBe('b7ad6b7169203331');
    expect(ctx!.traceFlags).toBe(1);
  });

  it('extracts tracestate when present', () => {
    const tracer = new TracerImpl(makeConfig());
    const headers = new Headers();
    headers.set('traceparent', '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');
    headers.set('tracestate', 'congo=t61rcWkgMzE');

    const ctx = tracer.extractContext(headers);
    expect(ctx!.traceState).toBe('congo=t61rcWkgMzE');
  });

  it('returns null for missing traceparent', () => {
    const tracer = new TracerImpl(makeConfig());
    const headers = new Headers();

    expect(tracer.extractContext(headers)).toBeNull();
  });

  it('returns null for invalid version', () => {
    const tracer = new TracerImpl(makeConfig());
    const headers = new Headers();
    headers.set('traceparent', '01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');

    expect(tracer.extractContext(headers)).toBeNull();
  });

  it('returns null for all-zero trace ID', () => {
    const tracer = new TracerImpl(makeConfig());
    const headers = new Headers();
    headers.set('traceparent', '00-00000000000000000000000000000000-b7ad6b7169203331-01');

    expect(tracer.extractContext(headers)).toBeNull();
  });

  it('returns null for all-zero span ID', () => {
    const tracer = new TracerImpl(makeConfig());
    const headers = new Headers();
    headers.set('traceparent', '00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01');

    expect(tracer.extractContext(headers)).toBeNull();
  });

  it('returns null for malformed traceparent (wrong segment count)', () => {
    const tracer = new TracerImpl(makeConfig());
    const headers = new Headers();
    headers.set('traceparent', '00-abc-01');

    expect(tracer.extractContext(headers)).toBeNull();
  });

  it('injects context into headers', () => {
    const tracer = new TracerImpl(makeConfig());
    const headers = new Headers();
    const ctx = {
      traceId: '0af7651916cd43dd8448eb211c80319c',
      spanId: 'b7ad6b7169203331',
      traceFlags: 1,
    };

    tracer.injectContext(headers, ctx);

    expect(headers.get('traceparent')).toBe('00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');
  });

  it('injects tracestate when present', () => {
    const tracer = new TracerImpl(makeConfig());
    const headers = new Headers();
    const ctx = {
      traceId: '0af7651916cd43dd8448eb211c80319c',
      spanId: 'b7ad6b7169203331',
      traceFlags: 1,
      traceState: 'vendor=opaque',
    };

    tracer.injectContext(headers, ctx);

    expect(headers.get('tracestate')).toBe('vendor=opaque');
  });

  it('round-trips context through inject and extract', () => {
    const tracer = new TracerImpl(makeConfig());
    const original = {
      traceId: '0af7651916cd43dd8448eb211c80319c',
      spanId: 'b7ad6b7169203331',
      traceFlags: 1,
      traceState: 'key=val',
    };

    const headers = new Headers();
    tracer.injectContext(headers, original);
    const extracted = tracer.extractContext(headers);

    expect(extracted).toEqual(original);
  });
});

// =============================================================================
// Sampling
// =============================================================================

describe('TracerImpl - Sampling', () => {
  it('always_off sampler returns NoOp spans', () => {
    const tracer = new TracerImpl(makeConfig({ sampler: 'always_off' }));
    const span = tracer.startSpan('test');

    expect(span.isRecording()).toBe(false);
    expect(span.spanId).toBe('0000000000000000');
  });

  it('always_on sampler returns real spans', () => {
    const tracer = new TracerImpl(makeConfig({ sampler: 'always_on' }));
    const span = tracer.startSpan('test');

    expect(span.isRecording()).toBe(true);
    expect(span.spanId).not.toBe('0000000000000000');
  });
});

// =============================================================================
// NoOpTracer
// =============================================================================

describe('NoOpTracer', () => {
  it('returns non-recording spans', () => {
    const tracer = new NoOpTracer();
    const span = tracer.startSpan('test');

    expect(span.isRecording()).toBe(false);
  });

  it('executes withSpan without tracing', () => {
    const tracer = new NoOpTracer();
    const span = tracer.startSpan('test');

    const result = tracer.withSpan(span, () => 'result');
    expect(result).toBe('result');
  });

  it('executes withSpanAsync without tracing', async () => {
    const tracer = new NoOpTracer();
    const span = tracer.startSpan('test');

    const result = await tracer.withSpanAsync(span, async () => 'async-result');
    expect(result).toBe('async-result');
  });

  it('returns undefined for getCurrentSpan', () => {
    const tracer = new NoOpTracer();
    expect(tracer.getCurrentSpan()).toBeUndefined();
  });

  it('returns null for extractContext', () => {
    const tracer = new NoOpTracer();
    expect(tracer.extractContext(new Headers())).toBeNull();
  });
});

// =============================================================================
// createTracer factory
// =============================================================================

describe('createTracer', () => {
  it('returns TracerImpl when enabled', () => {
    const tracer = createTracer(makeConfig({ enabled: true }));
    expect(tracer).toBeInstanceOf(TracerImpl);
  });

  it('returns NoOpTracer when disabled', () => {
    const tracer = createTracer(makeConfig({ enabled: false }));
    expect(tracer).toBeInstanceOf(NoOpTracer);
  });
});
