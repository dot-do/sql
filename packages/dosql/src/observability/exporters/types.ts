/**
 * DoSQL Observability Exporters - Type Definitions
 *
 * Defines interfaces for metrics and trace exporters to support
 * external observability platforms like Prometheus, Datadog, and OpenTelemetry.
 *
 * @packageDocumentation
 */

import type { Span, TraceContext, Counter, Histogram, Gauge, MetricsRegistry } from '../types.js';

// =============================================================================
// METRICS EXPORTER INTERFACE
// =============================================================================

/**
 * Metric data point for export
 */
export interface MetricDataPoint {
  /** Metric name */
  name: string;
  /** Metric type */
  type: 'counter' | 'histogram' | 'gauge';
  /** Help/description text */
  help: string;
  /** Label key-value pairs */
  labels: Record<string, string>;
  /** Metric value (for counter/gauge) */
  value?: number | undefined;
  /** Histogram data (for histogram type) */
  histogram?: {
    sum: number;
    count: number;
    buckets: Array<{ le: number | '+Inf'; count: number }>;
  } | undefined;
  /** Timestamp in milliseconds */
  timestamp?: number | undefined;
}

/**
 * Configuration for metrics exporter
 */
export interface MetricsExporterConfig {
  /** Whether the exporter is enabled */
  enabled: boolean;
  /** Export interval in milliseconds (0 for on-demand) */
  exportIntervalMs: number;
  /** Endpoint URL for push-based exporters */
  endpoint?: string | undefined;
  /** Additional headers for HTTP requests */
  headers?: Record<string, string> | undefined;
  /** Timeout for export requests in milliseconds */
  timeoutMs: number;
  /** Global labels to add to all metrics */
  globalLabels?: Record<string, string> | undefined;
}

/**
 * Default metrics exporter configuration
 */
export const DEFAULT_METRICS_EXPORTER_CONFIG: Readonly<MetricsExporterConfig> = {
  enabled: true,
  exportIntervalMs: 0, // On-demand by default
  timeoutMs: 30000,
};

/**
 * Metrics exporter interface for sending metrics to external systems.
 *
 * Implementations support both push-based (periodic export) and pull-based
 * (on-demand export) patterns. Each exporter formats metrics according to
 * the target system's requirements.
 *
 * @example Push-based exporter (Datadog)
 * ```typescript
 * const exporter = new DatadogMetricsExporter({
 *   apiKey: process.env.DD_API_KEY,
 *   site: 'datadoghq.com',
 * });
 *
 * // Periodic push
 * setInterval(async () => {
 *   await exporter.export(registry);
 * }, 60000);
 * ```
 *
 * @example Pull-based exporter (Prometheus)
 * ```typescript
 * const exporter = new PrometheusExporter({ prefix: 'dosql' });
 *
 * // Serve /metrics endpoint
 * if (url.pathname === '/metrics') {
 *   const metrics = await exporter.export(registry);
 *   return new Response(metrics, {
 *     headers: { 'Content-Type': exporter.contentType },
 *   });
 * }
 * ```
 */
export interface MetricsExporter {
  /** Exporter name for identification */
  readonly name: string;

  /** Content type for HTTP responses */
  readonly contentType: string;

  /** Export metrics from a registry */
  export(registry: MetricsRegistry): Promise<string>;

  /** Export individual metric data points */
  exportDataPoints(dataPoints: MetricDataPoint[]): Promise<string>;

  /** Shutdown the exporter and flush pending data */
  shutdown(): Promise<void>;
}

// =============================================================================
// TRACE EXPORTER INTERFACE
// =============================================================================

/**
 * Span data for export (serializable representation)
 */
export interface SpanData {
  /** Unique span identifier (16 hex characters) */
  spanId: string;
  /** Trace identifier (32 hex characters) */
  traceId: string;
  /** Parent span identifier */
  parentSpanId?: string | undefined;
  /** Span name/operation */
  name: string;
  /** Span kind */
  kind: 'INTERNAL' | 'SERVER' | 'CLIENT' | 'PRODUCER' | 'CONSUMER';
  /** Start time in nanoseconds since epoch */
  startTimeUnixNano: bigint;
  /** End time in nanoseconds since epoch */
  endTimeUnixNano?: bigint | undefined;
  /** Span status */
  status: {
    code: 'UNSET' | 'OK' | 'ERROR';
    message?: string | undefined;
  };
  /** Span attributes */
  attributes: Record<string, string | number | boolean | string[] | number[] | boolean[]>;
  /** Span events */
  events: Array<{
    name: string;
    timeUnixNano: bigint;
    attributes?: Record<string, string | number | boolean> | undefined;
  }>;
  /** Resource attributes (service info) */
  resource?: {
    serviceName: string;
    serviceVersion?: string | undefined;
    attributes?: Record<string, string> | undefined;
  } | undefined;
}

/**
 * Configuration for trace exporter
 */
export interface TraceExporterConfig {
  /** Whether the exporter is enabled */
  enabled: boolean;
  /** Endpoint URL for sending traces */
  endpoint?: string | undefined;
  /** Additional headers for HTTP requests */
  headers?: Record<string, string> | undefined;
  /** Timeout for export requests in milliseconds */
  timeoutMs: number;
  /** Maximum batch size for span export */
  maxBatchSize: number;
  /** Export delay in milliseconds (batching window) */
  exportDelayMs: number;
}

/**
 * Default trace exporter configuration
 */
export const DEFAULT_TRACE_EXPORTER_CONFIG: Readonly<TraceExporterConfig> = {
  enabled: true,
  timeoutMs: 30000,
  maxBatchSize: 100,
  exportDelayMs: 5000,
};

/**
 * Export result status
 */
export interface ExportResult {
  /** Whether export succeeded */
  success: boolean;
  /** Number of spans/metrics exported */
  count: number;
  /** Error message if failed */
  error?: string | undefined;
  /** Duration of export in milliseconds */
  durationMs: number;
}

/**
 * Trace exporter interface for sending spans to external tracing systems.
 *
 * Implementations batch spans and export them to backends like
 * Jaeger, Zipkin, or OpenTelemetry collectors.
 *
 * @example
 * ```typescript
 * const exporter = new OTLPTraceExporter({
 *   endpoint: 'http://collector:4318/v1/traces',
 * });
 *
 * // After span completes
 * span.end();
 * await exporter.export([spanToSpanData(span)]);
 * ```
 */
export interface TraceExporter {
  /** Exporter name for identification */
  readonly name: string;

  /** Export a batch of spans */
  export(spans: SpanData[]): Promise<ExportResult>;

  /** Force flush any pending spans */
  forceFlush(): Promise<void>;

  /** Shutdown the exporter */
  shutdown(): Promise<void>;
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Converts a Span to SpanData for export
 */
export function spanToSpanData(span: Span, resource?: SpanData['resource']): SpanData {
  const startTimeUnixNano = BigInt(Math.floor(span.startTime * 1_000_000));
  const endTimeUnixNano = span.endTime
    ? BigInt(Math.floor(span.endTime * 1_000_000))
    : undefined;

  const attributes: SpanData['attributes'] = {};
  for (const [key, value] of span.attributes) {
    attributes[key] = value;
  }

  const events: SpanData['events'] = span.events.map((event) => {
    const eventAttrs: Record<string, string | number | boolean> = {};
    if (event.attributes) {
      for (const [key, value] of event.attributes) {
        if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
          eventAttrs[key] = value;
        }
      }
    }
    return {
      name: event.name,
      timeUnixNano: BigInt(event.timestamp * 1_000_000),
      attributes: Object.keys(eventAttrs).length > 0 ? eventAttrs : undefined,
    };
  });

  return {
    spanId: span.spanId,
    traceId: span.traceId,
    parentSpanId: span.parentSpanId,
    name: span.name,
    kind: span.kind,
    startTimeUnixNano,
    endTimeUnixNano,
    status: {
      code: span.status,
      message: span.statusMessage,
    },
    attributes,
    events,
    resource,
  };
}
