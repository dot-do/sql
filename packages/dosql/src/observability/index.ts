/**
 * DoSQL Observability Module
 *
 * Provides unified observability with distributed tracing, metrics, and structured logging
 * for production monitoring of DoSQL across Durable Object boundaries.
 *
 * Features:
 * - Distributed tracing with correlation IDs across DO boundaries
 * - W3C Trace Context propagation for interoperability
 * - Prometheus-format metrics collection
 * - Trace-aware structured logging
 * - SQL statement sanitization for safe tracing
 * - Query and transaction instrumentation
 *
 * @example Unified Observability (Recommended)
 * ```typescript
 * import { createUnifiedObservability } from 'dosql/observability';
 *
 * const obs = createUnifiedObservability({
 *   serviceName: 'my-database',
 *   tracing: { enabled: true },
 *   metrics: { enabled: true },
 * });
 *
 * // Handle incoming request with automatic context propagation
 * async function fetch(request: Request) {
 *   return obs.traceRequest(request, 'handle-query', async (span) => {
 *     span.setAttribute('db.operation', 'SELECT');
 *
 *     // Logger automatically includes trace context (correlationId, traceId, spanId)
 *     obs.logger.info('Processing query', { sql: 'SELECT * FROM users' });
 *
 *     // Make traced calls to other DOs with automatic context propagation
 *     const result = await obs.tracedFetch(otherDO, shardRequest);
 *
 *     obs.metrics.queryTotal.inc({ operation: 'SELECT', status: 'success' });
 *     return result;
 *   });
 * }
 * ```
 *
 * @example Basic Observability
 * ```typescript
 * import { createObservability } from 'dosql/observability';
 *
 * const { tracer, metrics, sanitizer } = createObservability({
 *   tracing: { enabled: true, serviceName: 'my-service' },
 *   metrics: { enabled: true, prefix: 'dosql' },
 * });
 *
 * // Create a span for query execution
 * const span = tracer.startSpan('dosql.query', {
 *   attributes: {
 *     'db.operation': 'SELECT',
 *     'db.statement': sanitizer.sanitize(sql),
 *   },
 * });
 *
 * try {
 *   const result = await db.execute(sql);
 *   span.setStatus('OK');
 *   return result;
 * } catch (error) {
 *   span.setStatus('ERROR', error.message);
 *   throw error;
 * } finally {
 *   span.end();
 * }
 * ```
 *
 * @example Metrics Collection
 * ```typescript
 * // Create metrics
 * const queryCounter = metrics.createCounter(
 *   'queries_total',
 *   'Total queries executed',
 *   ['operation', 'table', 'status']
 * );
 *
 * const queryDuration = metrics.createHistogram(
 *   'query_duration_seconds',
 *   'Query execution duration',
 *   ['operation', 'table']
 * );
 *
 * // Record metrics
 * queryCounter.inc({ operation: 'SELECT', table: 'users', status: 'success' });
 * queryDuration.observe({ operation: 'SELECT', table: 'users' }, 0.05);
 *
 * // Export for /metrics endpoint
 * return new Response(metrics.getMetrics(), {
 *   headers: { 'Content-Type': 'text/plain' },
 * });
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// TYPE EXPORTS
// =============================================================================

export {
  // Span types
  type Span,
  type SpanKind,
  type SpanStatus,
  type SpanEvent,
  type SpanOptions,
  type TraceContext,
  type Tracer,
  type AttributeValue,

  // Metric types
  type Counter,
  type Histogram,
  type HistogramValue,
  type Gauge,
  type MetricsRegistry,

  // SQL sanitizer types
  type SQLSanitizer,
  type StatementType,

  // Observable query types
  type ObservableQuery,
  type ObservableQueryResult,

  // Configuration types
  type TracingConfig,
  type MetricsConfig,
  type ObservabilityConfig,
  DEFAULT_OBSERVABILITY_CONFIG,
} from './types.js';

// =============================================================================
// TRACER EXPORTS
// =============================================================================

export {
  TracerImpl,
  NoOpTracer,
  createTracer,
} from './tracer.js';

// =============================================================================
// METRICS EXPORTS
// =============================================================================

export {
  MetricsRegistryImpl,
  NoOpMetricsRegistry,
  createMetricsRegistry,
} from './metrics.js';

// =============================================================================
// SANITIZER EXPORTS
// =============================================================================

export {
  SQLSanitizerImpl,
  createSQLSanitizer,
} from './sanitizer.js';

// =============================================================================
// CONVENIENCE FACTORY
// =============================================================================

import type {
  Tracer,
  MetricsRegistry,
  SQLSanitizer,
  ObservabilityConfig,
  Counter,
  Histogram,
  Gauge,
} from './types.js';
import { DEFAULT_OBSERVABILITY_CONFIG } from './types.js';
import { createTracer } from './tracer.js';
import { createMetricsRegistry } from './metrics.js';
import { createSQLSanitizer } from './sanitizer.js';

/**
 * Observability instance containing all components
 */
export interface Observability {
  tracer: Tracer;
  metrics: MetricsRegistry;
  sanitizer: SQLSanitizer;
  config: ObservabilityConfig;
}

/**
 * Creates a complete observability instance with tracer, metrics registry, and SQL sanitizer.
 *
 * This factory function initializes all observability components with the provided configuration,
 * merging with sensible defaults. Use this when you need access to individual observability
 * components rather than the unified observability interface.
 *
 * @param config - Partial configuration to customize observability behavior.
 *   Missing values will be filled from DEFAULT_OBSERVABILITY_CONFIG.
 * @returns An Observability instance containing tracer, metrics, sanitizer, and merged config.
 *
 * @example Basic usage with defaults
 * ```typescript
 * const obs = createObservability();
 * const span = obs.tracer.startSpan('my-operation');
 * ```
 *
 * @example Custom configuration
 * ```typescript
 * const obs = createObservability({
 *   tracing: { enabled: true, serviceName: 'my-service', sampler: 'probability', samplingRate: 0.1 },
 *   metrics: { enabled: true, prefix: 'myapp' },
 * });
 * ```
 *
 * @see {@link createUnifiedObservability} for a higher-level API with integrated logging
 */
export function createObservability(config: Partial<ObservabilityConfig> = {}): Observability {
  const mergedConfig: ObservabilityConfig = {
    tracing: { ...DEFAULT_OBSERVABILITY_CONFIG.tracing, ...config.tracing },
    metrics: { ...DEFAULT_OBSERVABILITY_CONFIG.metrics, ...config.metrics },
  };

  return {
    tracer: createTracer(mergedConfig.tracing),
    metrics: createMetricsRegistry(mergedConfig.metrics),
    sanitizer: createSQLSanitizer(),
    config: mergedConfig,
  };
}

// =============================================================================
// PRE-CONFIGURED METRICS
// =============================================================================

/**
 * Standard DoSQL metrics
 */
export interface DoSQLMetrics {
  queryTotal: Counter;
  queryDuration: Histogram;
  queryErrors: Counter;
  activeConnections: Gauge;
  transactionsTotal: Counter;
  transactionDuration: Histogram;
  walWrites: Counter;
  walSize: Gauge;
  walCheckpoints: Counter;
  cdcEventsTotal: Counter;
  cdcLag: Gauge;
}

/**
 * Creates standard DoSQL metrics registered with the provided metrics registry.
 *
 * This function initializes all standard database metrics including query counters,
 * duration histograms, error tracking, transaction metrics, WAL metrics, and CDC metrics.
 * All metrics are automatically prefixed according to the registry's configuration.
 *
 * @param registry - The metrics registry to register all DoSQL metrics with.
 * @returns A DoSQLMetrics object containing all standard database metrics.
 *
 * @example
 * ```typescript
 * const registry = createMetricsRegistry({ enabled: true, prefix: 'dosql' });
 * const metrics = createDoSQLMetrics(registry);
 *
 * // Record a successful query
 * metrics.queryTotal.inc({ operation: 'SELECT', table: 'users', status: 'success' });
 * metrics.queryDuration.observe({ operation: 'SELECT', table: 'users' }, 0.025);
 *
 * // Record a transaction
 * metrics.transactionsTotal.inc({ outcome: 'commit' });
 * ```
 *
 * @see {@link DoSQLMetrics} for the full list of available metrics
 */
export function createDoSQLMetrics(registry: MetricsRegistry): DoSQLMetrics {
  return {
    queryTotal: registry.createCounter(
      'queries_total',
      'Total number of SQL queries executed',
      ['operation', 'table', 'status']
    ),

    queryDuration: registry.createHistogram(
      'query_duration_seconds',
      'Query execution duration in seconds',
      ['operation', 'table'],
      [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
    ),

    queryErrors: registry.createCounter(
      'query_errors_total',
      'Total number of query errors',
      ['operation', 'error_type']
    ),

    activeConnections: registry.createGauge(
      'active_connections',
      'Number of active database connections',
      ['shard']
    ),

    transactionsTotal: registry.createCounter(
      'transactions_total',
      'Total number of transactions',
      ['outcome']
    ),

    transactionDuration: registry.createHistogram(
      'transaction_duration_seconds',
      'Transaction duration in seconds',
      ['outcome'],
      [0.01, 0.05, 0.1, 0.5, 1, 5, 10]
    ),

    walWrites: registry.createCounter(
      'wal_writes_total',
      'Total WAL write operations'
    ),

    walSize: registry.createGauge(
      'wal_size_bytes',
      'Current WAL size in bytes'
    ),

    walCheckpoints: registry.createCounter(
      'wal_checkpoints_total',
      'Total WAL checkpoints'
    ),

    cdcEventsTotal: registry.createCounter(
      'cdc_events_total',
      'Total CDC events emitted',
      ['operation', 'table']
    ),

    cdcLag: registry.createGauge(
      'cdc_lag_seconds',
      'CDC replication lag in seconds',
      ['table']
    ),
  };
}

// =============================================================================
// INSTRUMENTATION HELPERS
// =============================================================================

/**
 * Instruments a SQL query execution with distributed tracing and metrics collection.
 *
 * This function wraps a query execution to automatically:
 * - Create a trace span with SQL metadata (operation type, table, sanitized statement)
 * - Record query duration in a histogram metric
 * - Increment success/error counters
 * - Capture exceptions with stack traces in the span
 *
 * The SQL statement is automatically sanitized before being recorded in the span
 * to prevent sensitive data from appearing in traces.
 *
 * @typeParam T - The return type of the query execution
 * @param observability - The observability instance containing tracer and sanitizer
 * @param doSQLMetrics - The DoSQL metrics instance for recording query metrics
 * @param sql - The SQL statement being executed (will be sanitized for tracing)
 * @param params - Optional query parameters (used for sanitization context)
 * @param execute - Async function that performs the actual query execution
 * @returns The result from the execute function
 * @throws Re-throws any error from the execute function after recording metrics
 *
 * @example
 * ```typescript
 * const result = await instrumentQuery(
 *   observability,
 *   metrics,
 *   'SELECT * FROM users WHERE id = ?',
 *   [123],
 *   () => db.query('SELECT * FROM users WHERE id = ?', [123])
 * );
 * ```
 */
export async function instrumentQuery<T>(
  observability: Observability,
  doSQLMetrics: DoSQLMetrics,
  sql: string,
  params: unknown[] | undefined,
  execute: () => Promise<T>
): Promise<T> {
  const { tracer, sanitizer } = observability;

  const statementType = sanitizer.extractStatementType(sql);
  const tables = sanitizer.extractTableNames(sql);
  const tableName = tables[0] ?? 'unknown';

  const span = tracer.startSpan('dosql.query', {
    kind: 'INTERNAL',
    attributes: {
      'db.system': 'dosql',
      'db.operation': statementType,
      'db.statement': sanitizer.sanitize(sql, params),
      'db.sql.table': tableName,
    },
  });

  const startTime = performance.now();

  try {
    const result = await execute();

    const durationSeconds = (performance.now() - startTime) / 1000;

    span.setStatus('OK');
    doSQLMetrics.queryTotal.inc({ operation: statementType, table: tableName, status: 'success' });
    doSQLMetrics.queryDuration.observe({ operation: statementType, table: tableName }, durationSeconds);

    return result;
  } catch (error) {
    const durationSeconds = (performance.now() - startTime) / 1000;
    const errorType = error instanceof Error ? error.constructor.name : 'Error';

    span.setStatus('ERROR', error instanceof Error ? error.message : String(error));
    span.addEvent('exception', {
      'exception.type': errorType,
      'exception.message': error instanceof Error ? error.message : String(error),
    });

    doSQLMetrics.queryTotal.inc({ operation: statementType, table: tableName, status: 'error' });
    doSQLMetrics.queryErrors.inc({ operation: statementType, error_type: errorType });
    doSQLMetrics.queryDuration.observe({ operation: statementType, table: tableName }, durationSeconds);

    throw error;
  } finally {
    span.end();
  }
}

/**
 * Instruments a database transaction with distributed tracing and metrics collection.
 *
 * This function wraps a transaction execution to automatically:
 * - Create a trace span for the entire transaction
 * - Record transaction duration in a histogram metric
 * - Track transaction outcomes (commit/rollback) in counters
 * - Capture exceptions with details in the span
 *
 * @typeParam T - The return type of the transaction execution
 * @param observability - The observability instance containing the tracer
 * @param doSQLMetrics - The DoSQL metrics instance for recording transaction metrics
 * @param execute - Async function that performs the transaction operations
 * @returns The result from the execute function
 * @throws Re-throws any error from the execute function after recording rollback metrics
 *
 * @example
 * ```typescript
 * const result = await instrumentTransaction(
 *   observability,
 *   metrics,
 *   async () => {
 *     await db.execute('BEGIN');
 *     await db.execute('INSERT INTO orders ...');
 *     await db.execute('UPDATE inventory ...');
 *     await db.execute('COMMIT');
 *     return { orderId: 123 };
 *   }
 * );
 * ```
 */
export async function instrumentTransaction<T>(
  observability: Observability,
  doSQLMetrics: DoSQLMetrics,
  execute: () => Promise<T>
): Promise<T> {
  const { tracer } = observability;

  const span = tracer.startSpan('dosql.transaction', {
    kind: 'INTERNAL',
    attributes: {
      'db.system': 'dosql',
    },
  });

  const startTime = performance.now();

  try {
    const result = await execute();

    const durationSeconds = (performance.now() - startTime) / 1000;

    span.setStatus('OK');
    doSQLMetrics.transactionsTotal.inc({ outcome: 'commit' });
    doSQLMetrics.transactionDuration.observe({ outcome: 'commit' }, durationSeconds);

    return result;
  } catch (error) {
    const durationSeconds = (performance.now() - startTime) / 1000;

    span.setStatus('ERROR', error instanceof Error ? error.message : String(error));
    span.addEvent('exception', {
      'exception.type': error instanceof Error ? error.constructor.name : 'Error',
      'exception.message': error instanceof Error ? error.message : String(error),
    });

    doSQLMetrics.transactionsTotal.inc({ outcome: 'rollback' });
    doSQLMetrics.transactionDuration.observe({ outcome: 'rollback' }, durationSeconds);

    throw error;
  } finally {
    span.end();
  }
}

// =============================================================================
// DISTRIBUTED TRACING EXPORTS
// =============================================================================

export {
  createDistributedTracer,
  DistributedTracerImpl,
  NoOpDistributedTracer,
  DistributedTraceStorage,
  prepareTracedFetch,
  startServerSpan,
  withDistributedContext,
  type DistributedTracer,
  type DistributedSpan,
  type DistributedTraceContext,
  type DistributedTracerConfig,
  type DistributedSpanOptions,
  DEFAULT_DISTRIBUTED_TRACER_CONFIG,
} from './distributed-tracing.js';

// =============================================================================
// UNIFIED OBSERVABILITY EXPORTS
// =============================================================================

export {
  createUnifiedObservability,
  type UnifiedObservability,
  type UnifiedObservabilityConfig,
  type UnifiedMetrics,
  type TraceAwareLogger,
  DEFAULT_UNIFIED_CONFIG,
} from './unified.js';

// =============================================================================
// EXPORTERS
// =============================================================================

export {
  // Types
  type MetricsExporter,
  type TraceExporter,
  type ExportResult,
  type MetricDataPoint,
  type MetricsExporterConfig,
  type SpanData,
  type TraceExporterConfig,
  DEFAULT_METRICS_EXPORTER_CONFIG,
  DEFAULT_TRACE_EXPORTER_CONFIG,
  spanToSpanData,

  // Prometheus
  type PrometheusExporterConfig,
  type PrometheusEndpointOptions,
  PrometheusExporter,
  DEFAULT_PROMETHEUS_CONFIG,
  createPrometheusExporter,
  createPrometheusEndpoint,

  // OpenTelemetry
  type OTLPTraceExporterConfig,
  OTLPTraceExporter,
  SpanCollector,
  DEFAULT_OTLP_CONFIG,
  createOTLPTraceExporter,
  createSpanCollector,

  // Datadog
  type DatadogMetricsConfig,
  type DatadogTraceConfig,
  type DatadogIntegrationConfig,
  type DatadogIntegration,
  DatadogMetricsExporter,
  DatadogTraceExporter,
  createDatadogMetricsExporter,
  createDatadogTraceExporter,
  createDatadogIntegration,
  getDatadogLogContext,
  formatDatadogTraceString,
} from './exporters/index.js';
