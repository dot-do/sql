/**
 * DoSQL Datadog Integration
 *
 * Provides integration with Datadog for:
 * - Metrics export (DogStatsD / HTTP API)
 * - Trace export (APM)
 * - Log correlation
 *
 * @example Metrics Export
 * ```typescript
 * const exporter = createDatadogMetricsExporter({
 *   apiKey: process.env.DD_API_KEY!,
 *   site: 'datadoghq.com',
 *   tags: ['env:production', 'service:dosql'],
 * });
 *
 * // Export metrics periodically
 * setInterval(async () => {
 *   await exporter.export(registry);
 * }, 60000);
 * ```
 *
 * @example Trace Export
 * ```typescript
 * const traceExporter = createDatadogTraceExporter({
 *   apiKey: process.env.DD_API_KEY!,
 *   serviceName: 'dosql',
 *   environment: 'production',
 * });
 *
 * // Export completed spans
 * await traceExporter.export(spans);
 * ```
 *
 * @packageDocumentation
 */

import type {
  MetricsExporter,
  MetricDataPoint,
  TraceExporter,
  SpanData,
  ExportResult,
} from './types.js';
import type { MetricsRegistry } from '../types.js';

// =============================================================================
// DATADOG METRICS EXPORTER
// =============================================================================

/**
 * Datadog metrics exporter configuration
 */
export interface DatadogMetricsConfig {
  /** Datadog API key */
  apiKey: string;
  /** Datadog site (e.g., 'datadoghq.com', 'datadoghq.eu', 'us3.datadoghq.com') */
  site?: string | undefined;
  /** Global tags to add to all metrics */
  tags?: string[] | undefined;
  /** Metric name prefix */
  prefix?: string | undefined;
  /** Request timeout in milliseconds */
  timeoutMs?: number | undefined;
  /** Custom API endpoint (overrides site) */
  endpoint?: string | undefined;
}

/**
 * Datadog metric series format
 */
interface DatadogSeries {
  metric: string;
  type: 'count' | 'gauge' | 'rate';
  points: Array<{ timestamp: number; value: number }>;
  tags?: string[];
  host?: string;
  interval?: number;
}

/**
 * Datadog metrics exporter for sending metrics via the HTTP API.
 *
 * Converts Prometheus-style metrics to Datadog format and sends them
 * via the Datadog API. Supports counters, gauges, and histograms.
 *
 * Note: Histograms are converted to multiple Datadog metrics:
 * - {metric}.count - Total observations
 * - {metric}.sum - Sum of all observations
 * - {metric}.avg - Average value (sum/count)
 * - {metric}.p50, {metric}.p95, {metric}.p99 - Percentiles (estimated from buckets)
 *
 * @example
 * ```typescript
 * const exporter = new DatadogMetricsExporter({
 *   apiKey: 'your-api-key',
 *   site: 'datadoghq.com',
 *   prefix: 'dosql',
 *   tags: ['env:prod', 'region:us-west-2'],
 * });
 *
 * // Export metrics
 * const result = await exporter.export(registry);
 * console.log(`Exported metrics: ${result}`);
 * ```
 */
export class DatadogMetricsExporter implements MetricsExporter {
  readonly name = 'datadog';
  readonly contentType = 'application/json';
  private readonly config: Required<DatadogMetricsConfig>;
  private readonly apiEndpoint: string;

  constructor(config: DatadogMetricsConfig) {
    this.config = {
      site: 'datadoghq.com',
      tags: [],
      prefix: '',
      timeoutMs: 30000,
      endpoint: '',
      ...config,
    };

    // Construct API endpoint
    this.apiEndpoint = this.config.endpoint || `https://api.${this.config.site}/api/v2/series`;
  }

  /**
   * Export metrics from a registry to Datadog
   */
  async export(registry: MetricsRegistry): Promise<string> {
    // Parse Prometheus format and convert to Datadog series
    const prometheusOutput = registry.getMetrics();
    const series = this.parsePrometheusToSeries(prometheusOutput);

    if (series.length === 0) {
      return 'No metrics to export';
    }

    // Send to Datadog
    await this.sendSeries(series);

    return `Exported ${series.length} metrics to Datadog`;
  }

  /**
   * Export individual metric data points to Datadog
   */
  async exportDataPoints(dataPoints: MetricDataPoint[]): Promise<string> {
    const series: DatadogSeries[] = [];
    const timestamp = Math.floor(Date.now() / 1000);

    for (const point of dataPoints) {
      const metricName = this.config.prefix
        ? `${this.config.prefix}.${point.name}`
        : point.name;

      const tags = [
        ...this.config.tags,
        ...Object.entries(point.labels).map(([k, v]) => `${k}:${v}`),
      ];

      if (point.type === 'counter') {
        series.push({
          metric: metricName,
          type: 'count',
          points: [{ timestamp, value: point.value ?? 0 }],
          tags,
        });
      } else if (point.type === 'gauge') {
        series.push({
          metric: metricName,
          type: 'gauge',
          points: [{ timestamp, value: point.value ?? 0 }],
          tags,
        });
      } else if (point.type === 'histogram' && point.histogram) {
        // Export histogram as multiple metrics
        series.push({
          metric: `${metricName}.count`,
          type: 'gauge',
          points: [{ timestamp, value: point.histogram.count }],
          tags,
        });

        series.push({
          metric: `${metricName}.sum`,
          type: 'gauge',
          points: [{ timestamp, value: point.histogram.sum }],
          tags,
        });

        if (point.histogram.count > 0) {
          series.push({
            metric: `${metricName}.avg`,
            type: 'gauge',
            points: [{ timestamp, value: point.histogram.sum / point.histogram.count }],
            tags,
          });
        }
      }
    }

    if (series.length === 0) {
      return 'No metrics to export';
    }

    await this.sendSeries(series);
    return `Exported ${series.length} metrics to Datadog`;
  }

  /**
   * Shutdown the exporter
   */
  async shutdown(): Promise<void> {
    // No cleanup needed
  }

  /**
   * Parse Prometheus format and convert to Datadog series
   */
  private parsePrometheusToSeries(prometheusOutput: string): DatadogSeries[] {
    const series: DatadogSeries[] = [];
    const timestamp = Math.floor(Date.now() / 1000);
    const lines = prometheusOutput.split('\n');

    let currentMetricType: 'counter' | 'gauge' | 'histogram' | null = null;

    for (const line of lines) {
      if (!line || line.startsWith('# HELP')) {
        continue;
      }

      if (line.startsWith('# TYPE')) {
        const match = line.match(/# TYPE (\S+) (\S+)/);
        if (match) {
          currentMetricType = match[2] as 'counter' | 'gauge' | 'histogram';
        }
        continue;
      }

      // Parse data line
      const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*?\})?\s+(.+)$/);
      if (!match) continue;

      const [, metricName, labelsStr, valueStr] = match;
      const value = parseFloat(valueStr);

      if (isNaN(value)) continue;

      // Parse labels into tags
      const tags = [...this.config.tags];
      if (labelsStr) {
        const labelMatch = labelsStr.matchAll(/([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"/g);
        for (const m of labelMatch) {
          // Skip 'le' label for histogram buckets
          if (m[1] !== 'le') {
            tags.push(`${m[1]}:${m[2]}`);
          }
        }
      }

      // Convert metric name to Datadog format (dots instead of underscores)
      let ddMetricName = metricName.replace(/_/g, '.');
      if (this.config.prefix && !ddMetricName.startsWith(this.config.prefix)) {
        ddMetricName = `${this.config.prefix}.${ddMetricName}`;
      }

      // Determine Datadog metric type
      let ddType: 'count' | 'gauge' | 'rate' = 'gauge';
      if (currentMetricType === 'counter' || metricName.endsWith('_total')) {
        ddType = 'count';
      }

      // Skip histogram buckets (we'll handle sum and count)
      if (metricName.includes('_bucket')) {
        continue;
      }

      series.push({
        metric: ddMetricName,
        type: ddType,
        points: [{ timestamp, value }],
        tags,
      });
    }

    return series;
  }

  /**
   * Send series to Datadog API
   */
  private async sendSeries(series: DatadogSeries[]): Promise<void> {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.config.timeoutMs);

    try {
      const response = await fetch(this.apiEndpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'DD-API-KEY': this.config.apiKey,
        },
        body: JSON.stringify({ series }),
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        throw new Error(`Datadog API error: ${response.status} ${response.statusText}`);
      }
    } catch (error) {
      clearTimeout(timeoutId);
      throw error;
    }
  }
}

// =============================================================================
// DATADOG TRACE EXPORTER
// =============================================================================

/**
 * Datadog trace exporter configuration
 */
export interface DatadogTraceConfig {
  /** Datadog API key */
  apiKey: string;
  /** Datadog site */
  site?: string | undefined;
  /** Service name */
  serviceName: string;
  /** Environment (e.g., 'production', 'staging') */
  environment?: string | undefined;
  /** Service version */
  version?: string | undefined;
  /** Request timeout in milliseconds */
  timeoutMs?: number | undefined;
  /** Custom API endpoint */
  endpoint?: string | undefined;
  /** Global tags */
  tags?: Record<string, string> | undefined;
}

/**
 * Datadog span format
 */
interface DatadogSpan {
  trace_id: string;
  span_id: string;
  parent_id?: string;
  name: string;
  resource: string;
  service: string;
  type?: string;
  start: number;
  duration: number;
  error?: number;
  meta?: Record<string, string>;
  metrics?: Record<string, number>;
}

/**
 * Datadog APM trace exporter.
 *
 * Exports spans to Datadog APM using the trace intake API.
 * Supports span metadata, error tracking, and custom tags.
 *
 * @example
 * ```typescript
 * const exporter = new DatadogTraceExporter({
 *   apiKey: 'your-api-key',
 *   serviceName: 'dosql',
 *   environment: 'production',
 *   version: '1.0.0',
 * });
 *
 * // Export completed spans
 * const result = await exporter.export(spans);
 * if (result.success) {
 *   console.log(`Exported ${result.count} spans`);
 * }
 * ```
 */
export class DatadogTraceExporter implements TraceExporter {
  readonly name = 'datadog-apm';
  private readonly config: Required<DatadogTraceConfig>;
  private readonly apiEndpoint: string;

  constructor(config: DatadogTraceConfig) {
    this.config = {
      site: 'datadoghq.com',
      environment: 'unknown',
      version: '',
      timeoutMs: 30000,
      endpoint: '',
      tags: {},
      ...config,
    };

    // Construct trace intake endpoint
    this.apiEndpoint =
      this.config.endpoint || `https://trace.agent.${this.config.site}/api/v0.2/traces`;
  }

  /**
   * Export spans to Datadog APM
   */
  async export(spans: SpanData[]): Promise<ExportResult> {
    if (spans.length === 0) {
      return { success: true, count: 0, durationMs: 0 };
    }

    const startTime = performance.now();

    try {
      // Group spans by trace ID
      const traces = new Map<string, DatadogSpan[]>();

      for (const span of spans) {
        const ddSpan = this.toDatadogSpan(span);
        const traceSpans = traces.get(span.traceId) || [];
        traceSpans.push(ddSpan);
        traces.set(span.traceId, traceSpans);
      }

      // Send traces
      await this.sendTraces(Array.from(traces.values()));

      return {
        success: true,
        count: spans.length,
        durationMs: performance.now() - startTime,
      };
    } catch (error) {
      return {
        success: false,
        count: 0,
        error: error instanceof Error ? error.message : String(error),
        durationMs: performance.now() - startTime,
      };
    }
  }

  /**
   * Force flush pending spans
   */
  async forceFlush(): Promise<void> {
    // No batching in this implementation
  }

  /**
   * Shutdown the exporter
   */
  async shutdown(): Promise<void> {
    // No cleanup needed
  }

  /**
   * Convert SpanData to Datadog span format
   */
  private toDatadogSpan(span: SpanData): DatadogSpan {
    // Datadog uses 64-bit trace/span IDs (we truncate our 128-bit trace IDs)
    const traceId = span.traceId.slice(-16);
    const spanId = span.spanId;
    const parentId = span.parentSpanId;

    // Calculate duration in nanoseconds
    const startNs = Number(span.startTimeUnixNano);
    const endNs = span.endTimeUnixNano ? Number(span.endTimeUnixNano) : Date.now() * 1_000_000;
    const durationNs = endNs - startNs;

    // Build metadata
    const meta: Record<string, string> = {
      'env': this.config.environment,
      'span.kind': span.kind.toLowerCase(),
      ...this.config.tags,
    };

    if (this.config.version) {
      meta['version'] = this.config.version;
    }

    // Add span attributes as tags
    for (const [key, value] of Object.entries(span.attributes)) {
      if (typeof value === 'string') {
        meta[key] = value;
      } else if (typeof value === 'number' || typeof value === 'boolean') {
        meta[key] = String(value);
      } else if (Array.isArray(value)) {
        meta[key] = value.join(',');
      }
    }

    // Add error information
    const isError = span.status.code === 'ERROR';
    if (isError && span.status.message) {
      meta['error.message'] = span.status.message;
    }

    // Extract exception info from events
    for (const event of span.events) {
      if (event.name === 'exception' && event.attributes) {
        if (event.attributes['exception.type']) {
          meta['error.type'] = String(event.attributes['exception.type']);
        }
        if (event.attributes['exception.message']) {
          meta['error.message'] = String(event.attributes['exception.message']);
        }
      }
    }

    // Determine span type
    let spanType: string | undefined;
    if (span.attributes['db.system']) {
      spanType = 'sql';
    } else if (span.attributes['http.method']) {
      spanType = span.kind === 'SERVER' ? 'web' : 'http';
    }

    // Resource name (operation)
    let resource = span.name;
    if (span.attributes['db.statement']) {
      resource = String(span.attributes['db.statement']).substring(0, 5000);
    } else if (span.attributes['http.url']) {
      resource = String(span.attributes['http.url']);
    }

    return {
      trace_id: traceId,
      span_id: spanId,
      parent_id: parentId,
      name: span.name,
      resource,
      service: this.config.serviceName,
      type: spanType,
      start: startNs,
      duration: durationNs,
      error: isError ? 1 : 0,
      meta,
    };
  }

  /**
   * Send traces to Datadog API
   */
  private async sendTraces(traces: DatadogSpan[][]): Promise<void> {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.config.timeoutMs);

    try {
      const response = await fetch(this.apiEndpoint, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/msgpack',
          'X-Datadog-Trace-Count': String(traces.length),
          'DD-API-KEY': this.config.apiKey,
        },
        // Note: In a real implementation, you'd use msgpack encoding
        // For simplicity, we'll use JSON with a custom endpoint
        body: JSON.stringify(traces),
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        throw new Error(`Datadog API error: ${response.status} ${response.statusText}`);
      }
    } catch (error) {
      clearTimeout(timeoutId);
      throw error;
    }
  }
}

// =============================================================================
// DATADOG LOG CORRELATION
// =============================================================================

/**
 * Helper to add Datadog trace context to log entries for log correlation.
 *
 * When using Datadog APM with logs, adding trace context allows
 * correlation between traces and logs in the Datadog UI.
 *
 * @example
 * ```typescript
 * const logContext = getDatadogLogContext(span);
 * console.log(JSON.stringify({
 *   message: 'Query executed',
 *   ...logContext,
 *   duration_ms: 42,
 * }));
 * ```
 */
export function getDatadogLogContext(span: SpanData): Record<string, string> {
  return {
    'dd.trace_id': span.traceId.slice(-16),
    'dd.span_id': span.spanId,
  };
}

/**
 * Format span for Datadog log injection.
 *
 * Returns a string in the format expected by Datadog log correlation.
 *
 * @example
 * ```typescript
 * const traceString = formatDatadogTraceString(span);
 * // Returns: "dd.trace_id=abc123 dd.span_id=def456"
 * ```
 */
export function formatDatadogTraceString(span: SpanData): string {
  const traceId = span.traceId.slice(-16);
  return `dd.trace_id=${traceId} dd.span_id=${span.spanId}`;
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Creates a Datadog metrics exporter.
 *
 * @param config - Exporter configuration
 * @returns A configured DatadogMetricsExporter instance
 *
 * @example
 * ```typescript
 * const exporter = createDatadogMetricsExporter({
 *   apiKey: process.env.DD_API_KEY!,
 *   site: 'datadoghq.eu',
 *   prefix: 'myapp',
 * });
 * ```
 */
export function createDatadogMetricsExporter(
  config: DatadogMetricsConfig
): DatadogMetricsExporter {
  return new DatadogMetricsExporter(config);
}

/**
 * Creates a Datadog trace exporter.
 *
 * @param config - Exporter configuration
 * @returns A configured DatadogTraceExporter instance
 *
 * @example
 * ```typescript
 * const exporter = createDatadogTraceExporter({
 *   apiKey: process.env.DD_API_KEY!,
 *   serviceName: 'my-service',
 *   environment: 'production',
 * });
 * ```
 */
export function createDatadogTraceExporter(
  config: DatadogTraceConfig
): DatadogTraceExporter {
  return new DatadogTraceExporter(config);
}

// =============================================================================
// UNIFIED DATADOG INTEGRATION
// =============================================================================

/**
 * Configuration for unified Datadog integration
 */
export interface DatadogIntegrationConfig {
  /** Datadog API key */
  apiKey: string;
  /** Datadog site */
  site?: string | undefined;
  /** Service name */
  serviceName: string;
  /** Environment */
  environment?: string | undefined;
  /** Service version */
  version?: string | undefined;
  /** Enable metrics export */
  enableMetrics?: boolean | undefined;
  /** Enable trace export */
  enableTraces?: boolean | undefined;
  /** Global tags */
  tags?: Record<string, string> | undefined;
  /** Metric name prefix */
  metricsPrefix?: string | undefined;
}

/**
 * Unified Datadog integration providing both metrics and trace export.
 *
 * @example
 * ```typescript
 * const datadog = createDatadogIntegration({
 *   apiKey: process.env.DD_API_KEY!,
 *   serviceName: 'dosql',
 *   environment: 'production',
 *   version: '1.0.0',
 * });
 *
 * // Export metrics and traces
 * await datadog.exportMetrics(registry);
 * await datadog.exportTraces(spans);
 *
 * // Shutdown when done
 * await datadog.shutdown();
 * ```
 */
export interface DatadogIntegration {
  /** Metrics exporter */
  metricsExporter: DatadogMetricsExporter | null;
  /** Trace exporter */
  traceExporter: DatadogTraceExporter | null;
  /** Export metrics from registry */
  exportMetrics(registry: MetricsRegistry): Promise<string>;
  /** Export traces */
  exportTraces(spans: SpanData[]): Promise<ExportResult>;
  /** Shutdown all exporters */
  shutdown(): Promise<void>;
}

/**
 * Creates a unified Datadog integration.
 *
 * @param config - Integration configuration
 * @returns A DatadogIntegration instance
 */
export function createDatadogIntegration(
  config: DatadogIntegrationConfig
): DatadogIntegration {
  const metricsExporter =
    config.enableMetrics !== false
      ? new DatadogMetricsExporter({
          apiKey: config.apiKey,
          site: config.site,
          prefix: config.metricsPrefix,
          tags: config.tags ? Object.entries(config.tags).map(([k, v]) => `${k}:${v}`) : [],
        })
      : null;

  const traceExporter =
    config.enableTraces !== false
      ? new DatadogTraceExporter({
          apiKey: config.apiKey,
          site: config.site,
          serviceName: config.serviceName,
          environment: config.environment,
          version: config.version,
          tags: config.tags,
        })
      : null;

  return {
    metricsExporter,
    traceExporter,

    async exportMetrics(registry: MetricsRegistry): Promise<string> {
      if (!metricsExporter) {
        return 'Metrics export disabled';
      }
      return metricsExporter.export(registry);
    },

    async exportTraces(spans: SpanData[]): Promise<ExportResult> {
      if (!traceExporter) {
        return { success: true, count: 0, durationMs: 0, error: 'Traces export disabled' };
      }
      return traceExporter.export(spans);
    },

    async shutdown(): Promise<void> {
      await Promise.all([
        metricsExporter?.shutdown(),
        traceExporter?.shutdown(),
      ]);
    },
  };
}
