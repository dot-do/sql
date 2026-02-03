/**
 * Observability Exporters Tests
 *
 * Tests for Prometheus, OpenTelemetry, and Datadog exporters.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

import {
  // Types
  spanToSpanData,
  type SpanData,
  type MetricDataPoint,

  // Prometheus
  PrometheusExporter,
  createPrometheusExporter,
  createPrometheusEndpoint,

  // OpenTelemetry
  OTLPTraceExporter,
  SpanCollector,
  createOTLPTraceExporter,
  createSpanCollector,

  // Datadog
  DatadogMetricsExporter,
  DatadogTraceExporter,
  createDatadogMetricsExporter,
  createDatadogTraceExporter,
  createDatadogIntegration,
  getDatadogLogContext,
  formatDatadogTraceString,
} from '../exporters/index.js';

import { MetricsRegistryImpl } from '../metrics.js';
import type { MetricsConfig, Span } from '../types.js';

// =============================================================================
// TEST HELPERS
// =============================================================================

function makeMetricsConfig(overrides: Partial<MetricsConfig> = {}): MetricsConfig {
  return {
    enabled: true,
    prefix: 'test',
    defaultLabels: {},
    histogramBuckets: {
      latency: [0.01, 0.05, 0.1, 0.5, 1],
      size: [100, 1000, 10000],
    },
    ...overrides,
  };
}

function createMockSpan(overrides: Partial<Span> = {}): Span {
  return {
    spanId: 'abc123def456abc1',
    traceId: '0123456789abcdef0123456789abcdef',
    parentSpanId: undefined,
    name: 'test-span',
    kind: 'INTERNAL',
    startTime: Date.now(),
    endTime: Date.now() + 100,
    status: 'OK',
    statusMessage: undefined,
    attributes: new Map([
      ['db.system', 'dosql'],
      ['db.operation', 'SELECT'],
    ]),
    events: [],
    setAttribute: vi.fn().mockReturnThis(),
    setStatus: vi.fn().mockReturnThis(),
    addEvent: vi.fn().mockReturnThis(),
    end: vi.fn(),
    isRecording: () => true,
    ...overrides,
  };
}

function createMockSpanData(overrides: Partial<SpanData> = {}): SpanData {
  return {
    spanId: 'abc123def456abc1',
    traceId: '0123456789abcdef0123456789abcdef',
    parentSpanId: undefined,
    name: 'test-span',
    kind: 'INTERNAL',
    startTimeUnixNano: BigInt(Date.now() * 1_000_000),
    endTimeUnixNano: BigInt((Date.now() + 100) * 1_000_000),
    status: { code: 'OK' },
    attributes: {
      'db.system': 'dosql',
      'db.operation': 'SELECT',
    },
    events: [],
    ...overrides,
  };
}

// =============================================================================
// spanToSpanData Tests
// =============================================================================

describe('spanToSpanData', () => {
  it('converts a span to SpanData', () => {
    const span = createMockSpan();
    const spanData = spanToSpanData(span);

    expect(spanData.spanId).toBe(span.spanId);
    expect(spanData.traceId).toBe(span.traceId);
    expect(spanData.name).toBe(span.name);
    expect(spanData.kind).toBe(span.kind);
    expect(spanData.status.code).toBe(span.status);
    expect(spanData.attributes['db.system']).toBe('dosql');
    expect(spanData.attributes['db.operation']).toBe('SELECT');
  });

  it('includes resource information when provided', () => {
    const span = createMockSpan();
    const resource = {
      serviceName: 'my-service',
      serviceVersion: '1.0.0',
    };

    const spanData = spanToSpanData(span, resource);

    expect(spanData.resource).toEqual(resource);
  });

  it('converts span events', () => {
    const span = createMockSpan({
      events: [
        {
          name: 'exception',
          timestamp: Date.now(),
          attributes: new Map([
            ['exception.type', 'Error'],
            ['exception.message', 'Test error'],
          ]),
        },
      ],
    });

    const spanData = spanToSpanData(span);

    expect(spanData.events).toHaveLength(1);
    expect(spanData.events[0].name).toBe('exception');
    expect(spanData.events[0].attributes?.['exception.type']).toBe('Error');
  });
});

// =============================================================================
// PrometheusExporter Tests
// =============================================================================

describe('PrometheusExporter', () => {
  let registry: MetricsRegistryImpl;

  beforeEach(() => {
    registry = new MetricsRegistryImpl(makeMetricsConfig());
  });

  it('exports metrics in Prometheus format', async () => {
    const exporter = new PrometheusExporter();
    const counter = registry.createCounter('requests_total', 'Total requests');
    counter.inc({}, 5);

    const output = await exporter.export(registry);

    expect(output).toContain('# HELP test_requests_total Total requests');
    expect(output).toContain('# TYPE test_requests_total counter');
    expect(output).toContain('test_requests_total 5');
  });

  it('exports metrics with timestamps when configured', async () => {
    const exporter = new PrometheusExporter({ includeTimestamp: true });
    const counter = registry.createCounter('requests_total', 'Total requests');
    counter.inc({}, 5);

    const output = await exporter.export(registry);

    // Should have a timestamp (13 digit millisecond timestamp)
    expect(output).toMatch(/test_requests_total 5 \d{13}/);
  });

  it('filters metrics by pattern', async () => {
    const exporter = new PrometheusExporter({ metricFilter: 'requests' });

    const counter1 = registry.createCounter('requests_total', 'Total requests');
    const counter2 = registry.createCounter('errors_total', 'Total errors');
    counter1.inc({}, 5);
    counter2.inc({}, 2);

    const output = await exporter.export(registry);

    expect(output).toContain('test_requests_total');
    expect(output).not.toContain('test_errors_total');
  });

  it('returns OpenMetrics format when configured', async () => {
    const exporter = new PrometheusExporter({ format: 'openmetrics' });
    const counter = registry.createCounter('requests_total', 'Total requests');
    counter.inc({}, 5);

    const output = await exporter.export(registry);

    expect(output).toContain('# EOF');
    expect(exporter.contentType).toContain('openmetrics');
  });

  it('has correct content type for Prometheus format', () => {
    const exporter = new PrometheusExporter({ format: 'prometheus' });
    expect(exporter.contentType).toContain('text/plain');
  });

  it('exports data points directly', async () => {
    const exporter = new PrometheusExporter();

    const dataPoints: MetricDataPoint[] = [
      {
        name: 'custom_metric',
        type: 'counter',
        help: 'A custom metric',
        labels: { env: 'test' },
        value: 42,
      },
    ];

    const output = await exporter.exportDataPoints(dataPoints);

    expect(output).toContain('# HELP custom_metric A custom metric');
    expect(output).toContain('# TYPE custom_metric counter');
    expect(output).toContain('custom_metric{env="test"} 42');
  });

  it('exports histogram data points with buckets', async () => {
    const exporter = new PrometheusExporter();

    const dataPoints: MetricDataPoint[] = [
      {
        name: 'latency',
        type: 'histogram',
        help: 'Request latency',
        labels: {},
        histogram: {
          sum: 1.5,
          count: 10,
          buckets: [
            { le: 0.1, count: 5 },
            { le: 0.5, count: 8 },
            { le: 1.0, count: 10 },
            { le: '+Inf', count: 10 },
          ],
        },
      },
    ];

    const output = await exporter.exportDataPoints(dataPoints);

    expect(output).toContain('latency_bucket{le="0.1"} 5');
    expect(output).toContain('latency_bucket{le="+Inf"} 10');
    expect(output).toContain('latency_sum 1.5');
    expect(output).toContain('latency_count 10');
  });
});

describe('createPrometheusExporter', () => {
  it('creates a PrometheusExporter instance', () => {
    const exporter = createPrometheusExporter({ prefix: 'myapp' });
    expect(exporter).toBeInstanceOf(PrometheusExporter);
    expect(exporter.name).toBe('prometheus');
  });
});

describe('createPrometheusEndpoint', () => {
  let registry: MetricsRegistryImpl;

  beforeEach(() => {
    registry = new MetricsRegistryImpl(makeMetricsConfig());
  });

  it('returns null for non-metrics paths', async () => {
    const handler = createPrometheusEndpoint({ registry });
    const request = new Request('http://localhost/api/data');

    const response = await handler(request);

    expect(response).toBeNull();
  });

  it('returns metrics for /metrics path', async () => {
    const counter = registry.createCounter('test', 'Test counter');
    counter.inc();

    const handler = createPrometheusEndpoint({ registry });
    const request = new Request('http://localhost/metrics');

    const response = await handler(request);

    expect(response).not.toBeNull();
    expect(response!.status).toBe(200);
    expect(response!.headers.get('Content-Type')).toContain('text/plain');

    const body = await response!.text();
    expect(body).toContain('test_test');
  });

  it('supports custom path', async () => {
    const handler = createPrometheusEndpoint({
      registry,
      path: '/custom/metrics',
    });

    const request1 = new Request('http://localhost/metrics');
    const response1 = await handler(request1);
    expect(response1).toBeNull();

    const request2 = new Request('http://localhost/custom/metrics');
    const response2 = await handler(request2);
    expect(response2).not.toBeNull();
  });

  it('enforces basic authentication when configured', async () => {
    const handler = createPrometheusEndpoint({
      registry,
      auth: { username: 'admin', password: 'secret' },
    });

    // Without auth
    const request1 = new Request('http://localhost/metrics');
    const response1 = await handler(request1);
    expect(response1!.status).toBe(401);

    // With wrong credentials
    const request2 = new Request('http://localhost/metrics', {
      headers: {
        Authorization: `Basic ${btoa('wrong:wrong')}`,
      },
    });
    const response2 = await handler(request2);
    expect(response2!.status).toBe(401);

    // With correct credentials
    const request3 = new Request('http://localhost/metrics', {
      headers: {
        Authorization: `Basic ${btoa('admin:secret')}`,
      },
    });
    const response3 = await handler(request3);
    expect(response3!.status).toBe(200);
  });

  it('rejects non-GET methods', async () => {
    const handler = createPrometheusEndpoint({ registry });
    const request = new Request('http://localhost/metrics', { method: 'POST' });

    const response = await handler(request);

    expect(response!.status).toBe(405);
  });
});

// =============================================================================
// OTLPTraceExporter Tests
// =============================================================================

describe('OTLPTraceExporter', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('exports spans in OTLP format', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
    });
    vi.stubGlobal('fetch', mockFetch);

    const exporter = new OTLPTraceExporter({
      endpoint: 'http://localhost:4318/v1/traces',
      serviceName: 'test-service',
    });

    const spans = [createMockSpanData()];
    const result = await exporter.export(spans);

    expect(result.success).toBe(true);
    expect(result.count).toBe(1);
    expect(mockFetch).toHaveBeenCalledTimes(1);

    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toBe('http://localhost:4318/v1/traces');
    expect(options.method).toBe('POST');
    expect(options.headers['Content-Type']).toBe('application/json');

    const body = JSON.parse(options.body);
    expect(body.resourceSpans).toBeDefined();
    expect(body.resourceSpans[0].scopeSpans[0].spans).toHaveLength(1);
  });

  it('returns empty result for empty span array', async () => {
    const exporter = new OTLPTraceExporter({
      endpoint: 'http://localhost:4318/v1/traces',
    });

    const result = await exporter.export([]);

    expect(result.success).toBe(true);
    expect(result.count).toBe(0);
  });

  it('handles export errors gracefully', async () => {
    const mockFetch = vi.fn().mockRejectedValue(new Error('Network error'));
    vi.stubGlobal('fetch', mockFetch);

    const exporter = new OTLPTraceExporter({
      endpoint: 'http://localhost:4318/v1/traces',
      retry: { maxAttempts: 1, initialDelayMs: 1, maxDelayMs: 10 },
    });

    const spans = [createMockSpanData()];
    const result = await exporter.export(spans);

    expect(result.success).toBe(false);
    expect(result.error).toBe('Network error');
  });

  it('retries on 5xx errors', async () => {
    let callCount = 0;
    const mockFetch = vi.fn().mockImplementation(() => {
      callCount++;
      if (callCount < 2) {
        return Promise.resolve({ ok: false, status: 503, statusText: 'Service Unavailable' });
      }
      return Promise.resolve({ ok: true, status: 200 });
    });
    vi.stubGlobal('fetch', mockFetch);

    const exporter = new OTLPTraceExporter({
      endpoint: 'http://localhost:4318/v1/traces',
      retry: { maxAttempts: 3, initialDelayMs: 1, maxDelayMs: 10 },
    });

    const spans = [createMockSpanData()];
    const result = await exporter.export(spans);

    expect(result.success).toBe(true);
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  it('does not export after shutdown', async () => {
    const exporter = new OTLPTraceExporter({
      endpoint: 'http://localhost:4318/v1/traces',
    });

    await exporter.shutdown();

    const spans = [createMockSpanData()];
    const result = await exporter.export(spans);

    expect(result.success).toBe(false);
    expect(result.error).toBe('Exporter is shutdown');
  });

  it('includes resource attributes', async () => {
    const mockFetch = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    vi.stubGlobal('fetch', mockFetch);

    const exporter = new OTLPTraceExporter({
      endpoint: 'http://localhost:4318/v1/traces',
      serviceName: 'my-service',
      serviceVersion: '2.0.0',
      resourceAttributes: {
        'deployment.environment': 'test',
      },
    });

    await exporter.export([createMockSpanData()]);

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    const resourceAttrs = body.resourceSpans[0].resource.attributes;

    expect(resourceAttrs).toContainEqual({
      key: 'service.name',
      value: { stringValue: 'my-service' },
    });
    expect(resourceAttrs).toContainEqual({
      key: 'service.version',
      value: { stringValue: '2.0.0' },
    });
    expect(resourceAttrs).toContainEqual({
      key: 'deployment.environment',
      value: { stringValue: 'test' },
    });
  });
});

describe('SpanCollector', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('batches spans up to maxBatchSize', async () => {
    const mockExporter = {
      name: 'mock',
      export: vi.fn().mockResolvedValue({ success: true, count: 0, durationMs: 0 }),
      forceFlush: vi.fn(),
      shutdown: vi.fn(),
    };

    const collector = new SpanCollector(mockExporter, {
      maxBatchSize: 2,
      exportDelayMs: 10000,
    });

    // Add first span - should not trigger export
    collector.addSpan(createMockSpanData({ spanId: 'span1' }));
    expect(mockExporter.export).not.toHaveBeenCalled();

    // Add second span - should trigger export due to batch size
    collector.addSpan(createMockSpanData({ spanId: 'span2' }));
    await collector.flush();

    expect(mockExporter.export).toHaveBeenCalledTimes(1);
    expect(mockExporter.export.mock.calls[0][0]).toHaveLength(2);
  });

  it('exports on timer when not full', async () => {
    const mockExporter = {
      name: 'mock',
      export: vi.fn().mockResolvedValue({ success: true, count: 0, durationMs: 0 }),
      forceFlush: vi.fn(),
      shutdown: vi.fn(),
    };

    const collector = new SpanCollector(mockExporter, {
      maxBatchSize: 10,
      exportDelayMs: 1000,
    });

    collector.addSpan(createMockSpanData());

    // Advance timer to trigger export
    vi.advanceTimersByTime(1000);
    await collector.flush();

    expect(mockExporter.export).toHaveBeenCalled();
  });

  it('reports pending count', () => {
    const mockExporter = {
      name: 'mock',
      export: vi.fn().mockResolvedValue({ success: true, count: 0, durationMs: 0 }),
      forceFlush: vi.fn(),
      shutdown: vi.fn(),
    };

    const collector = new SpanCollector(mockExporter, {
      maxBatchSize: 10,
      exportDelayMs: 10000,
    });

    expect(collector.pendingCount).toBe(0);

    collector.addSpan(createMockSpanData());
    expect(collector.pendingCount).toBe(1);

    collector.addSpan(createMockSpanData());
    expect(collector.pendingCount).toBe(2);
  });
});

describe('createOTLPTraceExporter', () => {
  it('creates an OTLPTraceExporter instance', () => {
    const exporter = createOTLPTraceExporter({
      endpoint: 'http://localhost:4318/v1/traces',
    });
    expect(exporter).toBeInstanceOf(OTLPTraceExporter);
    expect(exporter.name).toBe('otlp');
  });
});

describe('createSpanCollector', () => {
  it('creates a SpanCollector with OTLP exporter', () => {
    const collector = createSpanCollector(
      { endpoint: 'http://localhost:4318/v1/traces' },
      { maxBatchSize: 50 }
    );
    expect(collector).toBeInstanceOf(SpanCollector);
  });
});

// =============================================================================
// DatadogMetricsExporter Tests
// =============================================================================

describe('DatadogMetricsExporter', () => {
  let registry: MetricsRegistryImpl;

  beforeEach(() => {
    registry = new MetricsRegistryImpl(makeMetricsConfig({ prefix: '' }));
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('exports metrics to Datadog API', async () => {
    const mockFetch = vi.fn().mockResolvedValue({ ok: true, status: 202 });
    vi.stubGlobal('fetch', mockFetch);

    const exporter = new DatadogMetricsExporter({
      apiKey: 'test-api-key',
      site: 'datadoghq.com',
    });

    const counter = registry.createCounter('requests_total', 'Total requests');
    counter.inc({}, 10);

    const result = await exporter.export(registry);

    expect(result).toContain('Exported');
    expect(mockFetch).toHaveBeenCalledTimes(1);

    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toBe('https://api.datadoghq.com/api/v2/series');
    expect(options.headers['DD-API-KEY']).toBe('test-api-key');
  });

  it('converts metric names to Datadog format', async () => {
    const mockFetch = vi.fn().mockResolvedValue({ ok: true, status: 202 });
    vi.stubGlobal('fetch', mockFetch);

    const exporter = new DatadogMetricsExporter({
      apiKey: 'test-api-key',
      prefix: 'myapp',
    });

    const counter = registry.createCounter('http_requests_total', 'Total HTTP requests');
    counter.inc({ method: 'GET' }, 5);

    await exporter.export(registry);

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    const metricNames = body.series.map((s: { metric: string }) => s.metric);

    // Should have dots instead of underscores and include prefix
    expect(metricNames.some((n: string) => n.includes('myapp'))).toBe(true);
  });

  it('includes global tags', async () => {
    const mockFetch = vi.fn().mockResolvedValue({ ok: true, status: 202 });
    vi.stubGlobal('fetch', mockFetch);

    const exporter = new DatadogMetricsExporter({
      apiKey: 'test-api-key',
      tags: ['env:production', 'region:us-west-2'],
    });

    const counter = registry.createCounter('test', 'Test');
    counter.inc();

    await exporter.export(registry);

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    const tags = body.series[0].tags;

    expect(tags).toContain('env:production');
    expect(tags).toContain('region:us-west-2');
  });

  it('exports data points directly', async () => {
    const mockFetch = vi.fn().mockResolvedValue({ ok: true, status: 202 });
    vi.stubGlobal('fetch', mockFetch);

    const exporter = new DatadogMetricsExporter({
      apiKey: 'test-api-key',
    });

    const dataPoints: MetricDataPoint[] = [
      {
        name: 'custom_metric',
        type: 'gauge',
        help: 'A custom metric',
        labels: { host: 'server1' },
        value: 42,
      },
    ];

    await exporter.exportDataPoints(dataPoints);

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.series[0].metric).toBe('custom_metric');
    expect(body.series[0].type).toBe('gauge');
    expect(body.series[0].tags).toContain('host:server1');
  });
});

describe('createDatadogMetricsExporter', () => {
  it('creates a DatadogMetricsExporter instance', () => {
    const exporter = createDatadogMetricsExporter({
      apiKey: 'test-key',
    });
    expect(exporter).toBeInstanceOf(DatadogMetricsExporter);
    expect(exporter.name).toBe('datadog');
  });
});

// =============================================================================
// DatadogTraceExporter Tests
// =============================================================================

describe('DatadogTraceExporter', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('exports spans to Datadog APM', async () => {
    const mockFetch = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    vi.stubGlobal('fetch', mockFetch);

    const exporter = new DatadogTraceExporter({
      apiKey: 'test-api-key',
      serviceName: 'test-service',
      environment: 'test',
    });

    const spans = [createMockSpanData()];
    const result = await exporter.export(spans);

    expect(result.success).toBe(true);
    expect(result.count).toBe(1);
    expect(mockFetch).toHaveBeenCalledTimes(1);

    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toContain('trace.agent');
    expect(options.headers['DD-API-KEY']).toBe('test-api-key');
  });

  it('converts spans to Datadog format', async () => {
    const mockFetch = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    vi.stubGlobal('fetch', mockFetch);

    const exporter = new DatadogTraceExporter({
      apiKey: 'test-api-key',
      serviceName: 'my-service',
      environment: 'production',
      version: '1.0.0',
    });

    const spans = [
      createMockSpanData({
        name: 'db.query',
        status: { code: 'ERROR', message: 'Query failed' },
      }),
    ];

    await exporter.export(spans);

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    const ddSpan = body[0][0];

    expect(ddSpan.service).toBe('my-service');
    expect(ddSpan.name).toBe('db.query');
    expect(ddSpan.error).toBe(1);
    expect(ddSpan.meta.env).toBe('production');
    expect(ddSpan.meta.version).toBe('1.0.0');
  });

  it('returns empty result for empty span array', async () => {
    const exporter = new DatadogTraceExporter({
      apiKey: 'test-api-key',
      serviceName: 'test-service',
    });

    const result = await exporter.export([]);

    expect(result.success).toBe(true);
    expect(result.count).toBe(0);
  });
});

describe('createDatadogTraceExporter', () => {
  it('creates a DatadogTraceExporter instance', () => {
    const exporter = createDatadogTraceExporter({
      apiKey: 'test-key',
      serviceName: 'test-service',
    });
    expect(exporter).toBeInstanceOf(DatadogTraceExporter);
    expect(exporter.name).toBe('datadog-apm');
  });
});

// =============================================================================
// Datadog Log Correlation Tests
// =============================================================================

describe('getDatadogLogContext', () => {
  it('returns trace context for logs', () => {
    const span = createMockSpanData({
      traceId: '0123456789abcdef0123456789abcdef',
      spanId: 'abc123def456abc1',
    });

    const context = getDatadogLogContext(span);

    expect(context['dd.trace_id']).toBe('0123456789abcdef'); // Last 16 chars
    expect(context['dd.span_id']).toBe('abc123def456abc1');
  });
});

describe('formatDatadogTraceString', () => {
  it('formats trace context as string', () => {
    const span = createMockSpanData({
      traceId: '0123456789abcdef0123456789abcdef',
      spanId: 'abc123def456abc1',
    });

    const result = formatDatadogTraceString(span);

    expect(result).toBe('dd.trace_id=0123456789abcdef dd.span_id=abc123def456abc1');
  });
});

// =============================================================================
// Datadog Integration Tests
// =============================================================================

describe('createDatadogIntegration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    const mockFetch = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    vi.stubGlobal('fetch', mockFetch);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('creates metrics and trace exporters', () => {
    const integration = createDatadogIntegration({
      apiKey: 'test-key',
      serviceName: 'test-service',
    });

    expect(integration.metricsExporter).toBeInstanceOf(DatadogMetricsExporter);
    expect(integration.traceExporter).toBeInstanceOf(DatadogTraceExporter);
  });

  it('can disable metrics export', () => {
    const integration = createDatadogIntegration({
      apiKey: 'test-key',
      serviceName: 'test-service',
      enableMetrics: false,
    });

    expect(integration.metricsExporter).toBeNull();
    expect(integration.traceExporter).not.toBeNull();
  });

  it('can disable trace export', () => {
    const integration = createDatadogIntegration({
      apiKey: 'test-key',
      serviceName: 'test-service',
      enableTraces: false,
    });

    expect(integration.metricsExporter).not.toBeNull();
    expect(integration.traceExporter).toBeNull();
  });

  it('exports metrics via integration', async () => {
    const integration = createDatadogIntegration({
      apiKey: 'test-key',
      serviceName: 'test-service',
    });

    const registry = new MetricsRegistryImpl(makeMetricsConfig());
    registry.createCounter('test', 'Test').inc();

    const result = await integration.exportMetrics(registry);

    expect(result).toContain('Exported');
  });

  it('exports traces via integration', async () => {
    const integration = createDatadogIntegration({
      apiKey: 'test-key',
      serviceName: 'test-service',
    });

    const result = await integration.exportTraces([createMockSpanData()]);

    expect(result.success).toBe(true);
    expect(result.count).toBe(1);
  });

  it('returns appropriate message when export disabled', async () => {
    const integration = createDatadogIntegration({
      apiKey: 'test-key',
      serviceName: 'test-service',
      enableMetrics: false,
      enableTraces: false,
    });

    const registry = new MetricsRegistryImpl(makeMetricsConfig());
    const metricsResult = await integration.exportMetrics(registry);
    expect(metricsResult).toContain('disabled');

    const tracesResult = await integration.exportTraces([createMockSpanData()]);
    expect(tracesResult.error).toContain('disabled');
  });
});
