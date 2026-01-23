/**
 * DoSQL Observability Module
 *
 * Provides OpenTelemetry tracing and Prometheus metrics for production monitoring.
 *
 * Features:
 * - OpenTelemetry-compatible tracing with W3C Trace Context propagation
 * - Prometheus-format metrics collection
 * - SQL statement sanitization for safe tracing
 * - Query execution instrumentation
 *
 * @example Basic Usage
 * ```typescript
 * import { createObservability } from '@dotdo/dosql/observability';
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
 * Create an observability instance with all components
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
 * Create standard DoSQL metrics
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
 * Instrument a query execution with tracing and metrics
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
 * Instrument a transaction with tracing and metrics
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
