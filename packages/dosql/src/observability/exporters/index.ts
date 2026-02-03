/**
 * DoSQL Observability Exporters
 *
 * This module provides exporters for sending observability data to external
 * monitoring platforms like Prometheus/Grafana, Datadog, and OpenTelemetry collectors.
 *
 * ## Supported Exporters
 *
 * ### Prometheus (Pull-based metrics)
 * - Standard Prometheus text format
 * - OpenMetrics format
 * - Basic authentication support
 * - Metric filtering and labeling
 *
 * ### OpenTelemetry (OTLP traces)
 * - HTTP/JSON protocol
 * - Batched span export
 * - Retry with exponential backoff
 * - Grafana Tempo, Jaeger support
 *
 * ### Datadog (Metrics + APM)
 * - Metrics via HTTP API
 * - Traces via APM intake
 * - Log correlation helpers
 *
 * @example Prometheus metrics endpoint
 * ```typescript
 * import { createPrometheusEndpoint } from 'dosql/observability/exporters';
 *
 * const metricsHandler = createPrometheusEndpoint({
 *   registry: obs.registry,
 *   auth: { username: 'prometheus', password: 'secret' },
 * });
 *
 * export default {
 *   async fetch(request: Request): Promise<Response> {
 *     const metricsResponse = await metricsHandler(request);
 *     if (metricsResponse) return metricsResponse;
 *     // ... normal request handling
 *   }
 * };
 * ```
 *
 * @example OpenTelemetry trace export
 * ```typescript
 * import { createSpanCollector, spanToSpanData } from 'dosql/observability/exporters';
 *
 * const collector = createSpanCollector({
 *   endpoint: 'http://otel-collector:4318/v1/traces',
 *   serviceName: 'dosql',
 * });
 *
 * // When span completes
 * span.end();
 * collector.addSpan(spanToSpanData(span));
 *
 * // On shutdown
 * await collector.shutdown();
 * ```
 *
 * @example Datadog integration
 * ```typescript
 * import { createDatadogIntegration } from 'dosql/observability/exporters';
 *
 * const datadog = createDatadogIntegration({
 *   apiKey: process.env.DD_API_KEY!,
 *   serviceName: 'dosql',
 *   environment: 'production',
 * });
 *
 * // Export periodically
 * await datadog.exportMetrics(registry);
 * await datadog.exportTraces(spans);
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// TYPE EXPORTS
// =============================================================================

export type {
  // Core exporter interfaces
  MetricsExporter,
  TraceExporter,
  ExportResult,

  // Metric types
  MetricDataPoint,
  MetricsExporterConfig,

  // Trace types
  SpanData,
  TraceExporterConfig,
} from './types.js';

export {
  DEFAULT_METRICS_EXPORTER_CONFIG,
  DEFAULT_TRACE_EXPORTER_CONFIG,
  spanToSpanData,
} from './types.js';

// =============================================================================
// PROMETHEUS EXPORTS
// =============================================================================

export type { PrometheusExporterConfig, PrometheusEndpointOptions } from './prometheus.js';

export {
  PrometheusExporter,
  DEFAULT_PROMETHEUS_CONFIG,
  createPrometheusExporter,
  createPrometheusEndpoint,
} from './prometheus.js';

// =============================================================================
// OPENTELEMETRY EXPORTS
// =============================================================================

export type { OTLPTraceExporterConfig } from './opentelemetry.js';

export {
  OTLPTraceExporter,
  SpanCollector,
  DEFAULT_OTLP_CONFIG,
  createOTLPTraceExporter,
  createSpanCollector,
} from './opentelemetry.js';

// =============================================================================
// DATADOG EXPORTS
// =============================================================================

export type {
  DatadogMetricsConfig,
  DatadogTraceConfig,
  DatadogIntegrationConfig,
  DatadogIntegration,
} from './datadog.js';

export {
  DatadogMetricsExporter,
  DatadogTraceExporter,
  createDatadogMetricsExporter,
  createDatadogTraceExporter,
  createDatadogIntegration,
  getDatadogLogContext,
  formatDatadogTraceString,
} from './datadog.js';
