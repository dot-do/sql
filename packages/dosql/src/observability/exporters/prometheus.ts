/**
 * DoSQL Prometheus Metrics Exporter
 *
 * Provides a Prometheus-compatible metrics endpoint with support for:
 * - Standard Prometheus text format (text/plain)
 * - OpenMetrics format (application/openmetrics-text)
 * - Custom label filtering and metric selection
 * - Basic authentication support
 *
 * @example
 * ```typescript
 * const exporter = createPrometheusExporter({
 *   prefix: 'dosql',
 *   includeTimestamp: true,
 * });
 *
 * // Serve /metrics endpoint
 * if (url.pathname === '/metrics') {
 *   const accept = request.headers.get('Accept') || '';
 *   const format = accept.includes('openmetrics') ? 'openmetrics' : 'prometheus';
 *
 *   return new Response(await exporter.export(registry), {
 *     headers: { 'Content-Type': exporter.contentType },
 *   });
 * }
 * ```
 *
 * @packageDocumentation
 */

import type {
  MetricsExporter,
  MetricDataPoint,
  MetricsExporterConfig,
} from './types.js';
import type { MetricsRegistry } from '../types.js';

// =============================================================================
// PROMETHEUS EXPORTER CONFIGURATION
// =============================================================================

/**
 * Prometheus exporter configuration
 */
export interface PrometheusExporterConfig extends Partial<MetricsExporterConfig> {
  /** Metric name prefix */
  prefix?: string | undefined;
  /** Output format */
  format?: 'prometheus' | 'openmetrics' | undefined;
  /** Include timestamps in output */
  includeTimestamp?: boolean | undefined;
  /** Default histogram buckets if not specified */
  defaultBuckets?: number[] | undefined;
  /** Metric name filter (regex pattern) */
  metricFilter?: string | undefined;
  /** Label filter (only include these labels) */
  labelFilter?: string[] | undefined;
}

/**
 * Default Prometheus exporter configuration
 */
export const DEFAULT_PROMETHEUS_CONFIG: Required<PrometheusExporterConfig> = {
  enabled: true,
  exportIntervalMs: 0,
  timeoutMs: 30000,
  prefix: '',
  format: 'prometheus',
  includeTimestamp: false,
  defaultBuckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  metricFilter: undefined as unknown as string,
  labelFilter: undefined as unknown as string[],
  endpoint: undefined as unknown as string,
  headers: undefined as unknown as Record<string, string>,
  globalLabels: undefined as unknown as Record<string, string>,
};

// =============================================================================
// PROMETHEUS EXPORTER IMPLEMENTATION
// =============================================================================

/**
 * Prometheus-compatible metrics exporter.
 *
 * Exports metrics in Prometheus text format or OpenMetrics format for scraping
 * by Prometheus, Grafana Agent, or other compatible collectors.
 *
 * Features:
 * - Standard Prometheus text format (0.0.4)
 * - OpenMetrics text format (1.0.0)
 * - Counter, Gauge, and Histogram metric types
 * - Label support with proper escaping
 * - Optional timestamp inclusion
 * - Metric name prefixing
 *
 * @example Basic usage
 * ```typescript
 * const exporter = new PrometheusExporter({ prefix: 'dosql' });
 * const output = await exporter.export(registry);
 * // Returns Prometheus-formatted metrics string
 * ```
 *
 * @example With filtering
 * ```typescript
 * const exporter = new PrometheusExporter({
 *   metricFilter: '^dosql_query',  // Only export query metrics
 *   labelFilter: ['operation', 'status'],  // Only include these labels
 * });
 * ```
 */
export class PrometheusExporter implements MetricsExporter {
  readonly name = 'prometheus';
  private readonly config: Required<PrometheusExporterConfig>;
  private readonly metricFilterRegex?: RegExp;

  constructor(config: PrometheusExporterConfig = {}) {
    this.config = {
      ...DEFAULT_PROMETHEUS_CONFIG,
      ...config,
    };

    if (this.config.metricFilter) {
      this.metricFilterRegex = new RegExp(this.config.metricFilter);
    }
  }

  /**
   * Content type for HTTP responses based on format
   */
  get contentType(): string {
    return this.config.format === 'openmetrics'
      ? 'application/openmetrics-text; version=1.0.0; charset=utf-8'
      : 'text/plain; version=0.0.4; charset=utf-8';
  }

  /**
   * Export metrics from a registry in Prometheus format
   */
  async export(registry: MetricsRegistry): Promise<string> {
    // Get the raw Prometheus output from the registry
    const rawMetrics = registry.getMetrics();

    if (!rawMetrics) {
      return this.config.format === 'openmetrics' ? '# EOF\n' : '';
    }

    // Parse and optionally transform the metrics
    const lines = rawMetrics.split('\n');
    const outputLines: string[] = [];

    let currentMetricName = '';
    const timestamp = this.config.includeTimestamp ? ` ${Date.now()}` : '';

    for (const line of lines) {
      if (!line || line.startsWith('#')) {
        // Pass through comments and HELP/TYPE lines
        if (line.startsWith('# HELP') || line.startsWith('# TYPE')) {
          const parts = line.split(' ');
          const metricName = parts[2];

          // Apply metric filter
          if (this.metricFilterRegex && !this.metricFilterRegex.test(metricName)) {
            currentMetricName = '';
            continue;
          }

          currentMetricName = metricName;

          // Apply prefix if not already applied
          if (this.config.prefix && !metricName.startsWith(this.config.prefix)) {
            parts[2] = `${this.config.prefix}_${metricName}`;
          }

          outputLines.push(parts.join(' '));
        } else {
          outputLines.push(line);
        }
        continue;
      }

      // Data line - check if it matches current metric
      const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*?\})?\s+(.+)$/);
      if (!match) {
        outputLines.push(line);
        continue;
      }

      const [, metricName, labels, value] = match;

      // Apply metric filter
      if (this.metricFilterRegex) {
        const baseName = metricName.replace(/_(bucket|count|sum|total)$/, '');
        if (!this.metricFilterRegex.test(baseName) && !this.metricFilterRegex.test(metricName)) {
          continue;
        }
      }

      // Apply label filter
      let filteredLabels = labels || '';
      if (this.config.labelFilter && filteredLabels) {
        filteredLabels = this.filterLabels(filteredLabels);
      }

      // Apply global labels
      if (this.config.globalLabels && Object.keys(this.config.globalLabels).length > 0) {
        filteredLabels = this.addGlobalLabels(filteredLabels);
      }

      // Apply prefix if not already applied
      let outputName = metricName;
      if (this.config.prefix && !metricName.startsWith(this.config.prefix)) {
        outputName = `${this.config.prefix}_${metricName}`;
      }

      outputLines.push(`${outputName}${filteredLabels} ${value}${timestamp}`);
    }

    // OpenMetrics requires EOF marker
    if (this.config.format === 'openmetrics') {
      outputLines.push('# EOF');
    }

    return outputLines.join('\n') + '\n';
  }

  /**
   * Export individual metric data points in Prometheus format
   */
  async exportDataPoints(dataPoints: MetricDataPoint[]): Promise<string> {
    const outputLines: string[] = [];
    const timestamp = this.config.includeTimestamp ? ` ${Date.now()}` : '';

    // Group by metric name for proper formatting
    const grouped = new Map<string, MetricDataPoint[]>();
    for (const point of dataPoints) {
      const existing = grouped.get(point.name) || [];
      existing.push(point);
      grouped.set(point.name, existing);
    }

    for (const [name, points] of grouped) {
      // Apply metric filter
      if (this.metricFilterRegex && !this.metricFilterRegex.test(name)) {
        continue;
      }

      const first = points[0];
      const fullName = this.config.prefix ? `${this.config.prefix}_${name}` : name;

      // HELP line
      outputLines.push(`# HELP ${fullName} ${first.help}`);

      // TYPE line
      outputLines.push(`# TYPE ${fullName} ${first.type}`);

      // Data lines
      for (const point of points) {
        const labels = this.formatLabels(point.labels);

        if (point.type === 'histogram' && point.histogram) {
          // Histogram: output bucket, sum, count
          for (const bucket of point.histogram.buckets) {
            const le = bucket.le === '+Inf' ? '+Inf' : bucket.le.toString();
            const bucketLabels = labels
              ? labels.replace('}', `,le="${le}"}`)
              : `{le="${le}"}`;
            outputLines.push(`${fullName}_bucket${bucketLabels} ${bucket.count}${timestamp}`);
          }
          outputLines.push(`${fullName}_sum${labels} ${point.histogram.sum}${timestamp}`);
          outputLines.push(`${fullName}_count${labels} ${point.histogram.count}${timestamp}`);
        } else {
          // Counter or Gauge
          outputLines.push(`${fullName}${labels} ${point.value ?? 0}${timestamp}`);
        }
      }
    }

    // OpenMetrics requires EOF marker
    if (this.config.format === 'openmetrics') {
      outputLines.push('# EOF');
    }

    return outputLines.join('\n') + '\n';
  }

  /**
   * Shutdown the exporter (no-op for pull-based exporter)
   */
  async shutdown(): Promise<void> {
    // No cleanup needed for pull-based exporter
  }

  /**
   * Format labels for Prometheus output
   */
  private formatLabels(labels: Record<string, string>): string {
    const entries = Object.entries(labels);
    if (entries.length === 0) {
      return '';
    }

    // Apply label filter if configured
    let filtered = entries;
    if (this.config.labelFilter) {
      filtered = entries.filter(([key]) => this.config.labelFilter!.includes(key));
    }

    if (filtered.length === 0) {
      return '';
    }

    const labelStr = filtered
      .map(([key, value]) => `${key}="${this.escapeLabel(value)}"`)
      .join(',');

    return `{${labelStr}}`;
  }

  /**
   * Filter labels from an existing label string
   */
  private filterLabels(labelsStr: string): string {
    // Parse {key="value",key2="value2"}
    const match = labelsStr.match(/^\{(.*)\}$/);
    if (!match) return labelsStr;

    const pairs = match[1].split(',').filter((pair) => {
      const keyMatch = pair.match(/^([a-zA-Z_][a-zA-Z0-9_]*)=/);
      if (!keyMatch) return true;
      return this.config.labelFilter!.includes(keyMatch[1]);
    });

    if (pairs.length === 0) {
      return '';
    }

    return `{${pairs.join(',')}}`;
  }

  /**
   * Add global labels to an existing label string
   */
  private addGlobalLabels(labelsStr: string): string {
    const globalPairs = Object.entries(this.config.globalLabels!)
      .map(([key, value]) => `${key}="${this.escapeLabel(value)}"`)
      .join(',');

    if (!labelsStr) {
      return `{${globalPairs}}`;
    }

    // Insert global labels into existing string
    return labelsStr.replace(/^\{/, `{${globalPairs},`);
  }

  /**
   * Escape special characters in label values
   */
  private escapeLabel(value: string): string {
    return value
      .replace(/\\/g, '\\\\')
      .replace(/"/g, '\\"')
      .replace(/\n/g, '\\n');
  }
}

// =============================================================================
// PROMETHEUS ENDPOINT HANDLER
// =============================================================================

/**
 * Options for Prometheus endpoint handler
 */
export interface PrometheusEndpointOptions {
  /** Metrics registry to export */
  registry: MetricsRegistry;
  /** Exporter configuration */
  exporterConfig?: PrometheusExporterConfig | undefined;
  /** Enable basic authentication */
  auth?: {
    username: string;
    password: string;
  } | undefined;
  /** Custom path (default: /metrics) */
  path?: string | undefined;
}

/**
 * Creates a Prometheus metrics endpoint handler for Cloudflare Workers.
 *
 * Returns a function that handles HTTP requests to the /metrics endpoint,
 * returning Prometheus-formatted metrics for scraping.
 *
 * @example
 * ```typescript
 * const metricsHandler = createPrometheusEndpoint({
 *   registry: obs.registry,
 *   auth: { username: 'prometheus', password: 'secret' },
 * });
 *
 * export default {
 *   async fetch(request: Request): Promise<Response> {
 *     const url = new URL(request.url);
 *
 *     // Handle /metrics endpoint
 *     const metricsResponse = await metricsHandler(request);
 *     if (metricsResponse) {
 *       return metricsResponse;
 *     }
 *
 *     // Normal request handling...
 *   }
 * };
 * ```
 */
export function createPrometheusEndpoint(
  options: PrometheusEndpointOptions
): (request: Request) => Promise<Response | null> {
  const exporter = new PrometheusExporter(options.exporterConfig);
  const path = options.path ?? '/metrics';

  return async (request: Request): Promise<Response | null> => {
    const url = new URL(request.url);

    // Only handle configured path
    if (url.pathname !== path) {
      return null;
    }

    // Only handle GET requests
    if (request.method !== 'GET') {
      return new Response('Method Not Allowed', { status: 405 });
    }

    // Check authentication if configured
    if (options.auth) {
      const authHeader = request.headers.get('Authorization');
      if (!authHeader) {
        return new Response('Unauthorized', {
          status: 401,
          headers: { 'WWW-Authenticate': 'Basic realm="Metrics"' },
        });
      }

      const [scheme, credentials] = authHeader.split(' ');
      if (scheme !== 'Basic' || !credentials) {
        return new Response('Unauthorized', { status: 401 });
      }

      const decoded = atob(credentials);
      const [username, password] = decoded.split(':');
      if (username !== options.auth.username || password !== options.auth.password) {
        return new Response('Unauthorized', { status: 401 });
      }
    }

    // Export metrics
    try {
      const metrics = await exporter.export(options.registry);
      return new Response(metrics, {
        status: 200,
        headers: {
          'Content-Type': exporter.contentType,
          'Cache-Control': 'no-cache, no-store, must-revalidate',
        },
      });
    } catch (error) {
      return new Response(
        `Error exporting metrics: ${error instanceof Error ? error.message : String(error)}`,
        { status: 500 }
      );
    }
  };
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Creates a Prometheus exporter with the given configuration.
 *
 * @param config - Exporter configuration options
 * @returns A configured PrometheusExporter instance
 *
 * @example
 * ```typescript
 * const exporter = createPrometheusExporter({
 *   prefix: 'myapp',
 *   format: 'openmetrics',
 *   includeTimestamp: true,
 * });
 * ```
 */
export function createPrometheusExporter(
  config: PrometheusExporterConfig = {}
): PrometheusExporter {
  return new PrometheusExporter(config);
}
