/**
 * DoSQL Unified Observability Layer
 *
 * Provides a unified interface for observability across DoSQL that integrates:
 * - Distributed tracing with correlation IDs
 * - Structured logging with trace context
 * - Prometheus metrics
 * - DO boundary context propagation
 *
 * @example Basic Usage
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
 *     // Logger automatically includes trace context
 *     obs.logger.info('Processing query', { sql: 'SELECT * FROM users' });
 *
 *     // Make traced calls to other DOs
 *     const result = await obs.tracedFetch(otherDO, shardRequest);
 *
 *     obs.metrics.queryTotal.inc({ operation: 'SELECT', status: 'success' });
 *     return result;
 *   });
 * }
 * ```
 *
 * @packageDocumentation
 */

import {
  createLogger,
  type StructuredLogger,
  type LogLevel,
  type LogSink,
} from '../logging/index.js';
import {
  createDistributedTracer,
  type DistributedTracer,
  type DistributedSpan,
  type DistributedTraceContext,
  type DistributedTracerConfig,
  DistributedTraceStorage,
  prepareTracedFetch,
  startServerSpan,
  withDistributedContext,
} from './distributed-tracing.js';
import {
  createMetricsRegistry,
  type MetricsRegistry,
  type Counter,
  type Histogram,
  type Gauge,
} from './metrics.js';
import { createSQLSanitizer, type SQLSanitizer } from './sanitizer.js';
import type { MetricsConfig, AttributeValue } from './types.js';

// =============================================================================
// UNIFIED OBSERVABILITY CONFIGURATION
// =============================================================================

/**
 * Configuration for unified observability
 */
export interface UnifiedObservabilityConfig {
  /** Service name for identification */
  serviceName: string;
  /** Instance ID for multi-instance deployments */
  instanceId?: string | undefined;
  /** Tracing configuration */
  tracing?: Partial<DistributedTracerConfig> | undefined;
  /** Metrics configuration */
  metrics?: Partial<MetricsConfig> | undefined;
  /** Logging configuration */
  logging?: {
    level?: LogLevel | undefined;
    sink?: LogSink | undefined;
    includeTraceContext?: boolean | undefined;
  } | undefined;
}

/**
 * Default unified observability configuration
 */
export const DEFAULT_UNIFIED_CONFIG: UnifiedObservabilityConfig = {
  serviceName: 'dosql',
  tracing: {
    enabled: true,
    sampler: 'always_on',
    samplingRate: 1.0,
  },
  metrics: {
    enabled: true,
    prefix: 'dosql',
    defaultLabels: {},
    histogramBuckets: {
      latency: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
      size: [100, 1000, 10000, 100000, 1000000, 10000000],
    },
  },
  logging: {
    level: 'info',
    includeTraceContext: true,
  },
};

// =============================================================================
// STANDARD DOSQL METRICS
// =============================================================================

/**
 * Standard DoSQL metrics collection
 */
export interface UnifiedMetrics {
  // Query metrics
  queryTotal: Counter;
  queryDuration: Histogram;
  queryErrors: Counter;
  queryRowsReturned: Histogram;

  // Transaction metrics
  transactionsTotal: Counter;
  transactionDuration: Histogram;

  // DO communication metrics
  doCallsTotal: Counter;
  doCallDuration: Histogram;
  doCallErrors: Counter;

  // Replication metrics
  replicationLag: Gauge;
  replicaRequests: Counter;

  // WAL metrics
  walWrites: Counter;
  walSize: Gauge;
  walCheckpoints: Counter;

  // CDC metrics
  cdcEventsTotal: Counter;
  cdcLag: Gauge;

  // Connection metrics
  activeConnections: Gauge;
  activeTransactions: Gauge;

  // Span/trace metrics
  spansCreated: Counter;
  spanDuration: Histogram;
}

/**
 * Create standard DoSQL metrics
 */
function createUnifiedMetrics(registry: MetricsRegistry): UnifiedMetrics {
  return {
    // Query metrics
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
    queryRowsReturned: registry.createHistogram(
      'query_rows_returned',
      'Number of rows returned by queries',
      ['operation'],
      [0, 1, 10, 100, 1000, 10000, 100000]
    ),

    // Transaction metrics
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

    // DO communication metrics
    doCallsTotal: registry.createCounter(
      'do_calls_total',
      'Total number of Durable Object calls',
      ['target_type', 'operation', 'status']
    ),
    doCallDuration: registry.createHistogram(
      'do_call_duration_seconds',
      'Durable Object call duration in seconds',
      ['target_type', 'operation'],
      [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5]
    ),
    doCallErrors: registry.createCounter(
      'do_call_errors_total',
      'Total number of Durable Object call errors',
      ['target_type', 'error_type']
    ),

    // Replication metrics
    replicationLag: registry.createGauge(
      'replication_lag_seconds',
      'Replication lag in seconds',
      ['replica_id', 'region']
    ),
    replicaRequests: registry.createCounter(
      'replica_requests_total',
      'Total number of requests routed to replicas',
      ['replica_id', 'consistency_level']
    ),

    // WAL metrics
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

    // CDC metrics
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

    // Connection metrics
    activeConnections: registry.createGauge(
      'active_connections',
      'Number of active database connections',
      ['shard']
    ),
    activeTransactions: registry.createGauge(
      'active_transactions',
      'Number of active transactions'
    ),

    // Span/trace metrics
    spansCreated: registry.createCounter(
      'spans_created_total',
      'Total number of trace spans created',
      ['span_kind', 'service']
    ),
    spanDuration: registry.createHistogram(
      'span_duration_seconds',
      'Span duration in seconds',
      ['span_name', 'status'],
      [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
    ),
  };
}

// =============================================================================
// TRACE-AWARE LOGGER
// =============================================================================

/**
 * Logger that automatically includes trace context in log entries
 */
export interface TraceAwareLogger extends StructuredLogger {
  /** Get the current correlation ID from trace context */
  getCurrentCorrelationId(): string | undefined;
}

/**
 * Create a logger that automatically includes trace context
 */
function createTraceAwareLogger(
  baseLogger: StructuredLogger,
  tracer: DistributedTracer
): TraceAwareLogger {
  const wrapMethod = <T extends (message: string | (() => string), ...args: unknown[]) => void>(
    method: T
  ): T => {
    return ((message: string | (() => string), ...args: unknown[]) => {
      const context = tracer.getCurrentDistributedContext();
      const currentSpan = tracer.getCurrentSpan();

      // Build trace context to add to log entry
      const traceContext: Record<string, unknown> = {};
      if (context) {
        traceContext.correlationId = context.correlationId;
        traceContext.traceId = context.traceId;
      }
      if (currentSpan) {
        traceContext.spanId = currentSpan.spanId;
        traceContext.spanName = currentSpan.name;
      }

      // Merge trace context with provided context
      const providedContext = args[args.length - 1];
      if (providedContext && typeof providedContext === 'object' && !(providedContext instanceof Error)) {
        args[args.length - 1] = { ...traceContext, ...providedContext };
      } else if (Object.keys(traceContext).length > 0) {
        args.push(traceContext);
      }

      (method as Function).call(baseLogger, message, ...args);
    }) as T;
  };

  return {
    debug: wrapMethod(baseLogger.debug.bind(baseLogger)),
    info: wrapMethod(baseLogger.info.bind(baseLogger)),
    warn: wrapMethod(baseLogger.warn.bind(baseLogger)),
    error: (message: string | (() => string), error?: Error, context?: Record<string, unknown>) => {
      const traceCtx = tracer.getCurrentDistributedContext();
      const currentSpan = tracer.getCurrentSpan();

      const traceContext: Record<string, unknown> = {};
      if (traceCtx) {
        traceContext.correlationId = traceCtx.correlationId;
        traceContext.traceId = traceCtx.traceId;
      }
      if (currentSpan) {
        traceContext.spanId = currentSpan.spanId;
        traceContext.spanName = currentSpan.name;
      }

      baseLogger.error(message, error, { ...traceContext, ...context });
    },
    child: (context: Record<string, unknown>) => createTraceAwareLogger(baseLogger.child(context), tracer),
    getTraceId: () => baseLogger.getTraceId(),
    setTraceId: (id: string) => baseLogger.setTraceId(id),
    getLevel: () => baseLogger.getLevel(),
    setLevel: (level: LogLevel) => baseLogger.setLevel(level),
    flush: () => baseLogger.flush(),
    getCurrentCorrelationId: () => tracer.getCurrentCorrelationId(),
  };
}

// =============================================================================
// UNIFIED OBSERVABILITY INTERFACE
// =============================================================================

/**
 * Unified observability instance
 */
export interface UnifiedObservability {
  /** Distributed tracer */
  tracer: DistributedTracer;
  /** Metrics registry */
  registry: MetricsRegistry;
  /** Standard DoSQL metrics */
  metrics: UnifiedMetrics;
  /** Trace-aware logger */
  logger: TraceAwareLogger;
  /** SQL sanitizer */
  sanitizer: SQLSanitizer;
  /** Configuration */
  config: UnifiedObservabilityConfig;

  /**
   * Handle an incoming request with automatic trace context propagation
   */
  traceRequest<T>(
    request: Request,
    spanName: string,
    handler: (span: DistributedSpan) => Promise<T>,
    attributes?: Record<string, AttributeValue>
  ): Promise<T>;

  /**
   * Make a traced fetch call to another DO
   */
  tracedFetch(
    stub: { fetch: (request: Request) => Promise<Response> },
    request: Request,
    spanName?: string
  ): Promise<Response>;

  /**
   * Instrument a query execution
   */
  instrumentQuery<T>(
    sql: string,
    params: unknown[] | undefined,
    execute: () => Promise<T>
  ): Promise<T>;

  /**
   * Instrument a transaction
   */
  instrumentTransaction<T>(
    txnId: string,
    execute: () => Promise<T>
  ): Promise<T>;

  /**
   * Run a function with a specific correlation ID
   */
  withCorrelation<T>(
    correlationId: string,
    fn: () => Promise<T>
  ): Promise<T>;

  /**
   * Get current correlation ID
   */
  getCurrentCorrelationId(): string | undefined;

  /**
   * Get current trace context
   */
  getCurrentContext(): DistributedTraceContext | undefined;

  /**
   * Export Prometheus metrics
   */
  getPrometheusMetrics(): string;
}

// =============================================================================
// UNIFIED OBSERVABILITY IMPLEMENTATION
// =============================================================================

/**
 * Creates a unified observability instance with integrated tracing, metrics, and logging.
 *
 * This is the recommended entry point for DoSQL observability. It provides:
 * - Distributed tracing with correlation IDs across Durable Object boundaries
 * - Prometheus-compatible metrics collection
 * - Structured logging with automatic trace context injection
 * - SQL statement sanitization for safe tracing
 * - High-level instrumentation methods for queries, transactions, and DO calls
 *
 * @param config - Partial configuration for unified observability.
 *   Missing values will be filled from DEFAULT_UNIFIED_CONFIG.
 * @returns A UnifiedObservability instance with all components initialized
 *
 * @example Basic usage
 * ```typescript
 * const obs = createUnifiedObservability({
 *   serviceName: 'my-database',
 *   instanceId: 'shard-1',
 * });
 *
 * // Handle incoming request with automatic trace propagation
 * async function fetch(request: Request): Promise<Response> {
 *   return obs.traceRequest(request, 'handle-query', async (span) => {
 *     span.setAttribute('db.operation', 'SELECT');
 *
 *     // Logger automatically includes correlationId, traceId, spanId
 *     obs.logger.info('Processing query');
 *
 *     // Instrument query execution
 *     const result = await obs.instrumentQuery(sql, params, () => db.query(sql, params));
 *
 *     obs.metrics.queryTotal.inc({ operation: 'SELECT', table: 'users', status: 'success' });
 *     return new Response(JSON.stringify(result));
 *   });
 * }
 * ```
 *
 * @example Traced DO-to-DO calls
 * ```typescript
 * const response = await obs.tracedFetch(shardStub, shardRequest, 'shard-query');
 * // Trace context automatically propagated to the target DO
 * ```
 *
 * @example Export Prometheus metrics
 * ```typescript
 * if (url.pathname === '/metrics') {
 *   return new Response(obs.getPrometheusMetrics(), {
 *     headers: { 'Content-Type': 'text/plain' },
 *   });
 * }
 * ```
 *
 * @see {@link UnifiedObservability} for the full API interface
 * @see {@link DEFAULT_UNIFIED_CONFIG} for default configuration values
 */
export function createUnifiedObservability(
  config: Partial<UnifiedObservabilityConfig> = {}
): UnifiedObservability {
  const mergedConfig: UnifiedObservabilityConfig = {
    serviceName: config.serviceName ?? DEFAULT_UNIFIED_CONFIG.serviceName,
    instanceId: config.instanceId,
    tracing: { ...DEFAULT_UNIFIED_CONFIG.tracing, ...config.tracing },
    metrics: { ...DEFAULT_UNIFIED_CONFIG.metrics, ...config.metrics },
    logging: { ...DEFAULT_UNIFIED_CONFIG.logging, ...config.logging },
  };

  // Create components
  const tracer = createDistributedTracer({
    enabled: mergedConfig.tracing?.enabled ?? true,
    serviceName: mergedConfig.serviceName,
    sampler: mergedConfig.tracing?.sampler ?? 'always_on',
    samplingRate: mergedConfig.tracing?.samplingRate ?? 1.0,
  });

  const registry = createMetricsRegistry({
    enabled: mergedConfig.metrics?.enabled ?? true,
    prefix: mergedConfig.metrics?.prefix ?? 'dosql',
    defaultLabels: {
      service: mergedConfig.serviceName,
      instance: mergedConfig.instanceId ?? 'default',
      ...mergedConfig.metrics?.defaultLabels,
    },
    histogramBuckets: mergedConfig.metrics?.histogramBuckets ?? {
      latency: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
      size: [100, 1000, 10000, 100000, 1000000, 10000000],
    },
  });

  const metrics = createUnifiedMetrics(registry);

  const baseLogger = createLogger({
    level: mergedConfig.logging?.level ?? 'info',
    sink: mergedConfig.logging?.sink,
    defaultContext: {
      service: mergedConfig.serviceName,
      instance: mergedConfig.instanceId,
    },
  });

  const logger = createTraceAwareLogger(baseLogger, tracer);

  const sanitizer = createSQLSanitizer();

  // ==========================================================================
  // Unified observability methods
  // ==========================================================================

  async function traceRequest<T>(
    request: Request,
    spanName: string,
    handler: (span: DistributedSpan) => Promise<T>,
    attributes?: Record<string, AttributeValue>
  ): Promise<T> {
    const span = startServerSpan(tracer, request, spanName, attributes);

    metrics.spansCreated.inc({
      span_kind: 'SERVER',
      service: mergedConfig.serviceName,
    });

    const startTime = performance.now();

    try {
      const result = await tracer.withDistributedSpanAsync(span, () => handler(span));

      const durationSeconds = (performance.now() - startTime) / 1000;
      metrics.spanDuration.observe({ span_name: spanName, status: 'OK' }, durationSeconds);

      return result;
    } catch (error) {
      const durationSeconds = (performance.now() - startTime) / 1000;
      metrics.spanDuration.observe({ span_name: spanName, status: 'ERROR' }, durationSeconds);

      throw error;
    }
  }

  async function tracedFetch(
    stub: { fetch: (request: Request) => Promise<Response> },
    request: Request,
    spanName?: string
  ): Promise<Response> {
    const name = spanName ?? `DO:${new URL(request.url).pathname}`;
    const span = tracer.startDistributedSpan(name, {
      kind: 'CLIENT',
      attributes: {
        'http.method': request.method,
        'http.url': request.url,
        'peer.service': 'durable-object',
      },
    });

    metrics.spansCreated.inc({
      span_kind: 'CLIENT',
      service: mergedConfig.serviceName,
    });

    const startTime = performance.now();

    try {
      const tracedRequest = prepareTracedFetch(tracer, request);
      const response = await tracer.withDistributedSpanAsync(span, () => stub.fetch(tracedRequest));

      const durationSeconds = (performance.now() - startTime) / 1000;

      span.setAttribute('http.status_code', response.status);

      if (response.ok) {
        metrics.doCallsTotal.inc({
          target_type: 'durable-object',
          operation: request.method,
          status: 'success',
        });
        metrics.doCallDuration.observe(
          { target_type: 'durable-object', operation: request.method },
          durationSeconds
        );
      } else {
        metrics.doCallsTotal.inc({
          target_type: 'durable-object',
          operation: request.method,
          status: 'error',
        });
        metrics.doCallErrors.inc({
          target_type: 'durable-object',
          error_type: `HTTP_${response.status}`,
        });
      }

      return response;
    } catch (error) {
      const durationSeconds = (performance.now() - startTime) / 1000;

      span.setStatus('ERROR', error instanceof Error ? error.message : String(error));
      span.addEvent('exception', {
        'exception.type': error instanceof Error ? error.constructor.name : 'Error',
        'exception.message': error instanceof Error ? error.message : String(error),
      });

      metrics.doCallsTotal.inc({
        target_type: 'durable-object',
        operation: request.method,
        status: 'error',
      });
      metrics.doCallErrors.inc({
        target_type: 'durable-object',
        error_type: error instanceof Error ? error.constructor.name : 'Error',
      });
      metrics.doCallDuration.observe(
        { target_type: 'durable-object', operation: request.method },
        durationSeconds
      );

      throw error;
    }
  }

  async function instrumentQuery<T>(
    sql: string,
    params: unknown[] | undefined,
    execute: () => Promise<T>
  ): Promise<T> {
    const statementType = sanitizer.extractStatementType(sql);
    const tables = sanitizer.extractTableNames(sql);
    const tableName = tables[0] ?? 'unknown';

    const span = tracer.startDistributedSpan('dosql.query', {
      kind: 'INTERNAL',
      attributes: {
        'db.system': 'dosql',
        'db.operation': statementType,
        'db.statement': sanitizer.sanitize(sql, params),
        'db.sql.table': tableName,
      },
    });

    metrics.spansCreated.inc({
      span_kind: 'INTERNAL',
      service: mergedConfig.serviceName,
    });

    const startTime = performance.now();

    try {
      const result = await tracer.withDistributedSpanAsync(span, execute);

      const durationSeconds = (performance.now() - startTime) / 1000;

      span.setStatus('OK');
      metrics.queryTotal.inc({ operation: statementType, table: tableName, status: 'success' });
      metrics.queryDuration.observe({ operation: statementType, table: tableName }, durationSeconds);

      return result;
    } catch (error) {
      const durationSeconds = (performance.now() - startTime) / 1000;
      const errorType = error instanceof Error ? error.constructor.name : 'Error';

      span.setStatus('ERROR', error instanceof Error ? error.message : String(error));
      span.addEvent('exception', {
        'exception.type': errorType,
        'exception.message': error instanceof Error ? error.message : String(error),
      });

      metrics.queryTotal.inc({ operation: statementType, table: tableName, status: 'error' });
      metrics.queryErrors.inc({ operation: statementType, error_type: errorType });
      metrics.queryDuration.observe({ operation: statementType, table: tableName }, durationSeconds);

      throw error;
    }
  }

  async function instrumentTransaction<T>(
    txnId: string,
    execute: () => Promise<T>
  ): Promise<T> {
    const span = tracer.startDistributedSpan('dosql.transaction', {
      kind: 'INTERNAL',
      attributes: {
        'db.system': 'dosql',
        'db.transaction.id': txnId,
      },
    });

    metrics.spansCreated.inc({
      span_kind: 'INTERNAL',
      service: mergedConfig.serviceName,
    });
    metrics.activeTransactions.inc({}, 1);

    const startTime = performance.now();

    try {
      const result = await tracer.withDistributedSpanAsync(span, execute);

      const durationSeconds = (performance.now() - startTime) / 1000;

      span.setStatus('OK');
      metrics.transactionsTotal.inc({ outcome: 'commit' });
      metrics.transactionDuration.observe({ outcome: 'commit' }, durationSeconds);
      metrics.activeTransactions.dec({}, 1);

      return result;
    } catch (error) {
      const durationSeconds = (performance.now() - startTime) / 1000;

      span.setStatus('ERROR', error instanceof Error ? error.message : String(error));
      span.addEvent('exception', {
        'exception.type': error instanceof Error ? error.constructor.name : 'Error',
        'exception.message': error instanceof Error ? error.message : String(error),
      });

      metrics.transactionsTotal.inc({ outcome: 'rollback' });
      metrics.transactionDuration.observe({ outcome: 'rollback' }, durationSeconds);
      metrics.activeTransactions.dec({}, 1);

      throw error;
    }
  }

  async function withCorrelation<T>(
    correlationId: string,
    fn: () => Promise<T>
  ): Promise<T> {
    const context = tracer.createRootContext(mergedConfig.serviceName);
    (context as DistributedTraceContext).correlationId = correlationId;
    return withDistributedContext(context, fn);
  }

  function getCurrentCorrelationId(): string | undefined {
    return tracer.getCurrentCorrelationId();
  }

  function getCurrentContext(): DistributedTraceContext | undefined {
    return tracer.getCurrentDistributedContext();
  }

  function getPrometheusMetrics(): string {
    return registry.getMetrics();
  }

  return {
    tracer,
    registry,
    metrics,
    logger,
    sanitizer,
    config: mergedConfig,
    traceRequest,
    tracedFetch,
    instrumentQuery,
    instrumentTransaction,
    withCorrelation,
    getCurrentCorrelationId,
    getCurrentContext,
    getPrometheusMetrics,
  };
}

// =============================================================================
// RE-EXPORTS FOR CONVENIENCE
// =============================================================================

export {
  DistributedTraceStorage,
  prepareTracedFetch,
  startServerSpan,
  withDistributedContext,
} from './distributed-tracing.js';

export type {
  DistributedTracer,
  DistributedSpan,
  DistributedTraceContext,
  DistributedTracerConfig,
  DistributedSpanOptions,
} from './distributed-tracing.js';
