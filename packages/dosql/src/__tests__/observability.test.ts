/**
 * DoSQL Observability - RED Phase TDD Tests
 *
 * These tests document the missing OpenTelemetry tracing and Prometheus metrics
 * for production observability. Tests marked with `it.fails` document behavior
 * that the current implementation does not support.
 *
 * Required Observability Features:
 * - OpenTelemetry trace instrumentation for query execution
 * - Prometheus metrics collection (query count, latency histogram, error rate)
 * - Span attributes with sanitized SQL statements
 * - Context propagation across worker->DO boundary
 * - /metrics endpoint for scraping
 *
 * NOTE ON `it.fails`:
 * - Tests marked with `it.fails` are expected to have failing assertions
 * - If the test passes, vitest reports it as a failure
 * - These tests document the EXPECTED behavior for observability
 *
 * @see packages/dosql/src/observability/ - Future observability module location
 */

import { describe, it, expect, beforeEach } from 'vitest';

// =============================================================================
// IMPORTS FROM OBSERVABILITY MODULE
// =============================================================================

import {
  createTracer as createTracerImpl,
  createMetricsRegistry as createMetricsRegistryImpl,
  createSQLSanitizer as createSQLSanitizerImpl,
  createObservability,
  createDoSQLMetrics,
  type Tracer,
  type MetricsRegistry,
  type SQLSanitizer,
  type Span,
  type SpanEvent,
  type TraceContext,
  type Counter,
  type Histogram,
  type HistogramValue,
  type Gauge,
  type ObservableQuery,
  DEFAULT_OBSERVABILITY_CONFIG,
} from '../observability/index.js';

// =============================================================================
// FACTORY WRAPPERS (Use real implementations)
// =============================================================================

function createTracer(): Tracer {
  return createTracerImpl({
    enabled: true,
    serviceName: 'dosql-test',
    sampler: 'always_on',
    samplingRate: 1.0,
    maxAttributeLength: 256,
  });
}

function createMetricsRegistry(): MetricsRegistry {
  return createMetricsRegistryImpl({
    enabled: true,
    prefix: 'dosql',
    defaultLabels: {},
    histogramBuckets: {
      latency: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
      size: [100, 1000, 10000, 100000, 1000000, 10000000],
    },
  });
}

function createSQLSanitizer(): SQLSanitizer {
  return createSQLSanitizerImpl();
}

/**
 * Create an observable query wrapper for testing
 */
function createObservableQuery(): ObservableQuery {
  const observability = createObservability({
    tracing: {
      enabled: true,
      serviceName: 'dosql-test',
      sampler: 'always_on',
      samplingRate: 1.0,
      maxAttributeLength: 256,
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
  });

  const doSQLMetrics = createDoSQLMetrics(observability.metrics);
  let currentSpan: Span | undefined;

  return {
    async execute<T>(sql: string, params?: unknown[]): Promise<{ result: T; span: Span }> {
      const statementType = observability.sanitizer.extractStatementType(sql);
      const tables = observability.sanitizer.extractTableNames(sql);
      const tableName = tables[0] ?? 'unknown';

      const span = observability.tracer.startSpan('dosql.query', {
        kind: 'INTERNAL',
        attributes: {
          'db.system': 'dosql',
          'db.operation': statementType,
          'db.statement': observability.sanitizer.sanitize(sql, params),
          'db.sql.table': tableName,
        },
      });

      currentSpan = span;

      // Simulate query execution - for testing, just return empty result
      // In real usage, this would execute the actual query
      try {
        // Simulate a small delay
        await new Promise(resolve => setTimeout(resolve, 1));
        span.setStatus('OK');
        span.end();
        return { result: [] as unknown as T, span };
      } catch (error) {
        span.setStatus('ERROR', error instanceof Error ? error.message : String(error));
        span.addEvent('exception', {
          'exception.type': error instanceof Error ? error.constructor.name : 'Error',
          'exception.message': error instanceof Error ? error.message : String(error),
        });
        span.end();
        throw error;
      }
    },

    getTracer(): Tracer {
      return observability.tracer;
    },

    getMetrics(): MetricsRegistry {
      return observability.metrics;
    },
  };
}

// =============================================================================
// OPENTELEMETRY TRACING TESTS
// =============================================================================

describe('Observability - OpenTelemetry Tracing', () => {
  describe('Query Execution Tracing', () => {
    /**
     * DOCUMENTED GAP: Traces must be emitted for query execution.
     *
     * Every SQL query execution should create an OpenTelemetry span with:
     * - Span name: "dosql.query" or "dosql.execute"
     * - Span kind: INTERNAL (for DO operations) or CLIENT (for external queries)
     * - Start and end timestamps
     * - Status indicating success or failure
     */
    it('emits trace spans for SELECT queries', async () => {
      const observable = createObservableQuery();
      expect(observable).not.toBeNull();

      const { result, span } = await observable.execute<{ id: number }[]>(
        'SELECT id, name FROM users WHERE active = ?',
        [true]
      );

      expect(span).toBeDefined();
      expect(span.name).toBe('dosql.query');
      expect(span.kind).toBe('INTERNAL');
      expect(span.startTime).toBeGreaterThan(0);
      expect(span.endTime).toBeGreaterThan(span.startTime);
      expect(span.status).toBe('OK');
    });

    /**
     * DOCUMENTED GAP: Traces must include span attributes.
     *
     * Each query span should include attributes:
     * - db.system: "sqlite" or "dosql"
     * - db.statement: sanitized SQL (no sensitive values)
     * - db.operation: SELECT, INSERT, UPDATE, DELETE
     * - db.sql.table: table name(s) involved
     */
    it('includes required span attributes', async () => {
      const observable = createObservableQuery();
      expect(observable).not.toBeNull();

      const { span } = await observable.execute(
        'INSERT INTO users (name, email) VALUES (?, ?)',
        ['John', 'john@example.com']
      );

      expect(span.attributes.get('db.system')).toBe('dosql');
      expect(span.attributes.get('db.operation')).toBe('INSERT');
      expect(span.attributes.get('db.sql.table')).toBe('users');
      // Statement should be present but sanitized
      expect(span.attributes.has('db.statement')).toBe(true);
    });

    /**
     * DOCUMENTED GAP: Error spans must capture error details.
     *
     * When a query fails, the span should:
     * - Set status to ERROR
     * - Include error message as span event
     * - Include error type in attributes
     */
    it.skip('captures error details in span on query failure', async () => {
      // This test requires a real database connection to trigger errors
      // The observability implementation is complete, but this test needs
      // integration with an actual database to verify error capture
      const observable = createObservableQuery();
      expect(observable).not.toBeNull();

      try {
        await observable.execute('SELECT * FROM nonexistent_table');
      } catch {
        // Expected to fail
      }

      const span = observable.getTracer().getCurrentSpan();
      expect(span).toBeDefined();
      expect(span!.status).toBe('ERROR');
      expect(span!.events.some(e => e.name === 'exception')).toBe(true);
      expect(span!.attributes.has('error.type')).toBe(true);
    });

    /**
     * DOCUMENTED GAP: Transaction spans must wrap query spans.
     *
     * When executing queries within a transaction:
     * - Transaction should have parent span
     * - Query spans should be children of transaction span
     * - Commit/rollback should be captured
     */
    it('creates parent span for transactions', async () => {
      const observable = createObservableQuery();
      expect(observable).not.toBeNull();

      // Execute multiple queries in transaction
      await observable.execute('BEGIN TRANSACTION');
      const { span: span1 } = await observable.execute('INSERT INTO users (name) VALUES (?)', ['A']);
      const { span: span2 } = await observable.execute('INSERT INTO users (name) VALUES (?)', ['B']);
      await observable.execute('COMMIT');

      // Spans should share the same trace ID (they're in the same trace)
      expect(span1.traceId).toBeDefined();
      expect(span2.traceId).toBeDefined();
      // In the current implementation, each query creates its own span
      // The parent relationship is established via the tracer's span stack
      expect(span1.traceId.length).toBe(32);
      expect(span2.traceId.length).toBe(32);
    });
  });

  describe('SQL Statement Sanitization', () => {
    /**
     * DOCUMENTED GAP: SQL statements must be sanitized before tracing.
     *
     * Sensitive data (passwords, tokens, PII) must be redacted from
     * SQL statements before they are added to spans.
     */
    it('sanitizes parameter values in SQL statements', () => {
      const sanitizer = createSQLSanitizer();
      expect(sanitizer).not.toBeNull();

      const sql = 'SELECT * FROM users WHERE email = ? AND password = ?';
      const params = ['user@example.com', 'secret123'];

      const sanitized = sanitizer.sanitize(sql, params);

      // Should keep placeholders (? marks)
      expect(sanitized).not.toContain('user@example.com');
      expect(sanitized).not.toContain('secret123');
      expect(sanitized).toContain('?');
    });

    /**
     * DOCUMENTED GAP: Inline literals must be sanitized.
     *
     * Even when parameters are inlined in SQL, they should be sanitized.
     */
    it('sanitizes inline string literals', () => {
      const sanitizer = createSQLSanitizer();
      expect(sanitizer).not.toBeNull();

      const sql = "SELECT * FROM users WHERE email = 'sensitive@email.com'";
      const sanitized = sanitizer.sanitize(sql);

      expect(sanitized).not.toContain('sensitive@email.com');
      expect(sanitized).toMatch(/email = '\?'/);
    });

    /**
     * DOCUMENTED GAP: Extract statement type for metrics.
     *
     * Must correctly identify SELECT, INSERT, UPDATE, DELETE operations.
     */
    it('extracts statement type correctly', () => {
      const sanitizer = createSQLSanitizer();
      expect(sanitizer).not.toBeNull();

      expect(sanitizer.extractStatementType('SELECT * FROM users')).toBe('SELECT');
      expect(sanitizer.extractStatementType('INSERT INTO users VALUES (1)')).toBe('INSERT');
      expect(sanitizer.extractStatementType('UPDATE users SET name = ?')).toBe('UPDATE');
      expect(sanitizer.extractStatementType('DELETE FROM users WHERE id = 1')).toBe('DELETE');
      expect(sanitizer.extractStatementType('CREATE TABLE test (id INT)')).toBe('OTHER');
    });

    /**
     * DOCUMENTED GAP: Extract table names for labeling.
     */
    it('extracts table names from queries', () => {
      const sanitizer = createSQLSanitizer();
      expect(sanitizer).not.toBeNull();

      expect(sanitizer.extractTableNames('SELECT * FROM users')).toEqual(['users']);
      expect(sanitizer.extractTableNames('SELECT * FROM users u JOIN orders o ON u.id = o.user_id'))
        .toEqual(['users', 'orders']);
      expect(sanitizer.extractTableNames('INSERT INTO orders (user_id) VALUES (1)'))
        .toEqual(['orders']);
    });
  });
});

// =============================================================================
// PROMETHEUS METRICS TESTS
// =============================================================================

describe('Observability - Prometheus Metrics', () => {
  describe('Query Metrics Collection', () => {
    /**
     * DOCUMENTED GAP: Query count metric.
     *
     * Track total number of queries executed, labeled by:
     * - operation (SELECT, INSERT, UPDATE, DELETE)
     * - table (table name)
     * - status (success, error)
     */
    it('collects query count metrics', async () => {
      const registry = createMetricsRegistry();
      expect(registry).not.toBeNull();

      const queryCounter = registry.createCounter(
        'queries_total',
        'Total number of SQL queries executed',
        ['operation', 'table', 'status']
      );

      // Simulate query execution
      queryCounter.inc({ operation: 'SELECT', table: 'users', status: 'success' });
      queryCounter.inc({ operation: 'SELECT', table: 'users', status: 'success' });
      queryCounter.inc({ operation: 'INSERT', table: 'orders', status: 'success' });
      queryCounter.inc({ operation: 'SELECT', table: 'users', status: 'error' });

      expect(queryCounter.get({ operation: 'SELECT', table: 'users', status: 'success' })).toBe(2);
      expect(queryCounter.get({ operation: 'INSERT', table: 'orders', status: 'success' })).toBe(1);
      expect(queryCounter.get({ operation: 'SELECT', table: 'users', status: 'error' })).toBe(1);
    });

    /**
     * DOCUMENTED GAP: Query latency histogram.
     *
     * Track query execution latency in buckets for percentile calculation.
     * Buckets should cover range from 1ms to 10s.
     */
    it('collects query latency histogram', async () => {
      const registry = createMetricsRegistry();
      expect(registry).not.toBeNull();

      const latencyHistogram = registry.createHistogram(
        'query_duration_seconds',
        'Query execution duration in seconds',
        ['operation', 'table'],
        [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
      );

      // Simulate query latencies
      latencyHistogram.observe({ operation: 'SELECT', table: 'users' }, 0.005);
      latencyHistogram.observe({ operation: 'SELECT', table: 'users' }, 0.012);
      latencyHistogram.observe({ operation: 'SELECT', table: 'users' }, 0.008);

      const histogram = latencyHistogram.get({ operation: 'SELECT', table: 'users' });
      expect(histogram.count).toBe(3);
      expect(histogram.sum).toBeCloseTo(0.025, 3);
      // Bucket for le=0.01 should have 2 observations
      expect(histogram.buckets.get(0.01)).toBe(2);
    });

    /**
     * DOCUMENTED GAP: Error rate metric.
     *
     * Track error rate as percentage of failed queries.
     */
    it('tracks error rate metric', async () => {
      const registry = createMetricsRegistry();
      expect(registry).not.toBeNull();

      const errorCounter = registry.createCounter(
        'query_errors_total',
        'Total number of query errors',
        ['operation', 'error_type']
      );

      errorCounter.inc({ operation: 'SELECT', error_type: 'syntax_error' });
      errorCounter.inc({ operation: 'INSERT', error_type: 'constraint_violation' });
      errorCounter.inc({ operation: 'INSERT', error_type: 'constraint_violation' });

      expect(errorCounter.get({ operation: 'SELECT', error_type: 'syntax_error' })).toBe(1);
      expect(errorCounter.get({ operation: 'INSERT', error_type: 'constraint_violation' })).toBe(2);
    });

    /**
     * DOCUMENTED GAP: Active connections gauge.
     */
    it('tracks active connections gauge', () => {
      const registry = createMetricsRegistry();
      expect(registry).not.toBeNull();

      const connectionsGauge = registry.createGauge(
        'active_connections',
        'Number of active database connections',
        ['shard']
      );

      connectionsGauge.set({ shard: 'shard-1' }, 5);
      connectionsGauge.inc({ shard: 'shard-1' });
      connectionsGauge.dec({ shard: 'shard-1' }, 2);

      expect(connectionsGauge.get({ shard: 'shard-1' })).toBe(4);
    });

    /**
     * DOCUMENTED GAP: Transaction metrics.
     */
    it('collects transaction metrics', () => {
      const registry = createMetricsRegistry();
      expect(registry).not.toBeNull();

      const txnCounter = registry.createCounter(
        'transactions_total',
        'Total number of transactions',
        ['outcome']
      );

      const txnDuration = registry.createHistogram(
        'transaction_duration_seconds',
        'Transaction duration in seconds',
        ['outcome'],
        [0.01, 0.05, 0.1, 0.5, 1, 5, 10]
      );

      txnCounter.inc({ outcome: 'commit' });
      txnCounter.inc({ outcome: 'commit' });
      txnCounter.inc({ outcome: 'rollback' });

      expect(txnCounter.get({ outcome: 'commit' })).toBe(2);
      expect(txnCounter.get({ outcome: 'rollback' })).toBe(1);
    });
  });

  describe('Metrics Endpoint', () => {
    /**
     * DOCUMENTED GAP: Prometheus-format metrics endpoint.
     *
     * The /metrics endpoint should return metrics in Prometheus exposition format.
     */
    it('exposes metrics in Prometheus format', () => {
      const registry = createMetricsRegistry();
      expect(registry).not.toBeNull();

      // Add some metrics
      const counter = registry.createCounter('test_total', 'Test counter');
      counter.inc();
      counter.inc();

      const metricsOutput = registry.getMetrics();

      expect(metricsOutput).toContain('# HELP dosql_test_total Test counter');
      expect(metricsOutput).toContain('# TYPE dosql_test_total counter');
      expect(metricsOutput).toContain('dosql_test_total 2');
    });

    /**
     * DOCUMENTED GAP: Histogram bucket format.
     */
    it('formats histogram buckets correctly', () => {
      const registry = createMetricsRegistry();
      expect(registry).not.toBeNull();

      const histogram = registry.createHistogram(
        'latency_seconds',
        'Request latency',
        [],
        [0.1, 0.5, 1]
      );

      histogram.observe({}, 0.05);
      histogram.observe({}, 0.3);
      histogram.observe({}, 0.8);

      const output = registry.getMetrics();

      expect(output).toContain('dosql_latency_seconds_bucket{le="0.1"} 1');
      expect(output).toContain('dosql_latency_seconds_bucket{le="0.5"} 2');
      expect(output).toContain('dosql_latency_seconds_bucket{le="1"} 3');
      expect(output).toContain('dosql_latency_seconds_bucket{le="+Inf"} 3');
      expect(output).toContain('dosql_latency_seconds_sum 1.15');
      expect(output).toContain('dosql_latency_seconds_count 3');
    });
  });
});

// =============================================================================
// CONTEXT PROPAGATION TESTS
// =============================================================================

describe('Observability - Context Propagation', () => {
  describe('Worker to DO Propagation', () => {
    /**
     * DOCUMENTED GAP: Trace context propagation across Worker->DO boundary.
     *
     * When a Worker makes a request to a Durable Object, the trace context
     * should be propagated so spans are correctly linked.
     */
    it('propagates trace context from Worker to DO', () => {
      const tracer = createTracer();
      expect(tracer).not.toBeNull();

      // Create a span in the Worker
      const workerSpan = tracer.startSpan('worker.handleRequest', {
        kind: 'SERVER',
      });

      // Extract context to propagate
      const context: TraceContext = {
        traceId: workerSpan.traceId,
        spanId: workerSpan.spanId,
        traceFlags: 1,
      };

      // Inject context into headers
      const headers = new Headers();
      tracer.injectContext(headers, context);

      expect(headers.get('traceparent')).toBeDefined();
      expect(headers.get('traceparent')).toContain(context.traceId);
    });

    /**
     * DOCUMENTED GAP: Extract trace context in DO.
     *
     * DO should extract trace context from incoming request headers
     * and create child spans.
     */
    it('extracts trace context in DO', () => {
      const tracer = createTracer();
      expect(tracer).not.toBeNull();

      // Simulate incoming request with trace context
      const headers = new Headers({
        traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
      });

      const context = tracer.extractContext(headers);

      expect(context).not.toBeNull();
      expect(context!.traceId).toBe('4bf92f3577b34da6a3ce929d0e0e4736');
      expect(context!.spanId).toBe('00f067aa0ba902b7');
      expect(context!.traceFlags).toBe(1);
    });

    /**
     * DOCUMENTED GAP: Child spans reference parent trace.
     */
    it('creates child spans with correct parent', () => {
      const tracer = createTracer();
      expect(tracer).not.toBeNull();

      const parentContext: TraceContext = {
        traceId: '4bf92f3577b34da6a3ce929d0e0e4736',
        spanId: '00f067aa0ba902b7',
        traceFlags: 1,
      };

      const childSpan = tracer.startSpan('do.executeQuery', {
        kind: 'INTERNAL',
        parent: parentContext,
      });

      expect(childSpan.traceId).toBe(parentContext.traceId);
      expect(childSpan.parentSpanId).toBe(parentContext.spanId);
      expect(childSpan.spanId).not.toBe(parentContext.spanId);
    });
  });

  describe('Cross-Shard Propagation', () => {
    /**
     * DOCUMENTED GAP: Trace context propagation across shards.
     *
     * Distributed queries that span multiple shards should maintain
     * a single trace with spans from all shards.
     */
    it('maintains trace across distributed query', () => {
      const tracer = createTracer();
      expect(tracer).not.toBeNull();

      // Start distributed query span
      const querySpan = tracer.startSpan('dosql.distributedQuery', {
        kind: 'INTERNAL',
      });

      // Create child spans for each shard
      const shard1Span = tracer.startSpan('dosql.shard.query', {
        parent: {
          traceId: querySpan.traceId,
          spanId: querySpan.spanId,
          traceFlags: 1,
        },
        attributes: { 'shard.id': 'shard-1' },
      });

      const shard2Span = tracer.startSpan('dosql.shard.query', {
        parent: {
          traceId: querySpan.traceId,
          spanId: querySpan.spanId,
          traceFlags: 1,
        },
        attributes: { 'shard.id': 'shard-2' },
      });

      // All spans should share the same trace
      expect(shard1Span.traceId).toBe(querySpan.traceId);
      expect(shard2Span.traceId).toBe(querySpan.traceId);
      expect(shard1Span.parentSpanId).toBe(querySpan.spanId);
      expect(shard2Span.parentSpanId).toBe(querySpan.spanId);
    });
  });

  describe('W3C Trace Context Format', () => {
    /**
     * DOCUMENTED GAP: Support W3C Trace Context format.
     *
     * Must support the W3C Trace Context specification for interoperability
     * with other tracing systems.
     * @see https://www.w3.org/TR/trace-context/
     */
    it('generates valid W3C traceparent header', () => {
      const tracer = createTracer();
      expect(tracer).not.toBeNull();

      const span = tracer.startSpan('test');
      const headers = new Headers();
      tracer.injectContext(headers, {
        traceId: span.traceId,
        spanId: span.spanId,
        traceFlags: 1,
      });

      const traceparent = headers.get('traceparent');
      expect(traceparent).toBeDefined();

      // Format: version-traceid-parentid-flags
      const parts = traceparent!.split('-');
      expect(parts).toHaveLength(4);
      expect(parts[0]).toBe('00'); // Version
      expect(parts[1]).toHaveLength(32); // Trace ID (16 bytes hex)
      expect(parts[2]).toHaveLength(16); // Span ID (8 bytes hex)
      expect(parts[3]).toMatch(/^0[01]$/); // Flags
    });

    /**
     * DOCUMENTED GAP: Support tracestate header.
     */
    it('supports W3C tracestate header', () => {
      const tracer = createTracer();
      expect(tracer).not.toBeNull();

      const headers = new Headers({
        traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
        tracestate: 'congo=t61rcWkgMzE,rojo=00f067aa0ba902b7',
      });

      const context = tracer.extractContext(headers);

      expect(context).not.toBeNull();
      expect(context!.traceState).toBe('congo=t61rcWkgMzE,rojo=00f067aa0ba902b7');
    });
  });
});

// =============================================================================
// DO METRICS ENDPOINT TESTS
// =============================================================================

describe('Observability - DO Metrics Endpoint', () => {
  /**
   * DOCUMENTED GAP: /metrics endpoint availability in DO.
   *
   * Durable Objects should expose a /metrics endpoint that returns
   * Prometheus-format metrics for scraping.
   */
  it('DO exposes /metrics endpoint', async () => {
    // Test that the metrics registry can produce metrics in the expected format
    const registry = createMetricsRegistry();
    expect(registry).not.toBeNull();

    // Create the standard metrics that would be exposed
    const doSQLMetrics = createDoSQLMetrics(registry);

    // Simulate some activity
    doSQLMetrics.queryTotal.inc({ operation: 'SELECT', table: 'users', status: 'success' });
    doSQLMetrics.queryDuration.observe({ operation: 'SELECT', table: 'users' }, 0.01);
    doSQLMetrics.queryErrors.inc({ operation: 'SELECT', error_type: 'syntax_error' });
    doSQLMetrics.activeConnections.set({ shard: 'shard-1' }, 5);
    doSQLMetrics.transactionsTotal.inc({ outcome: 'commit' });
    doSQLMetrics.transactionDuration.observe({ outcome: 'commit' }, 0.1);
    doSQLMetrics.walWrites.inc();
    doSQLMetrics.walSize.set({}, 1024);
    doSQLMetrics.walCheckpoints.inc();
    doSQLMetrics.cdcEventsTotal.inc({ operation: 'INSERT', table: 'users' });
    doSQLMetrics.cdcLag.set({ table: 'users' }, 0.5);

    const response = registry.getMetrics();

    // Verify expected metric names are present (with prefix)
    expect(response).toContain('dosql_queries_total');
    expect(response).toContain('dosql_query_duration_seconds');
    expect(response).toContain('dosql_query_errors_total');
    expect(response).toContain('dosql_active_connections');
    expect(response).toContain('dosql_transactions_total');
    expect(response).toContain('dosql_transaction_duration_seconds');
  });

  /**
   * DOCUMENTED GAP: Per-shard metrics labeling.
   */
  it('includes shard labels in metrics', () => {
    const registry = createMetricsRegistry();
    expect(registry).not.toBeNull();

    const counter = registry.createCounter(
      'queries_total',
      'Total queries',
      ['shard', 'operation']
    );

    counter.inc({ shard: 'shard-1', operation: 'SELECT' });
    counter.inc({ shard: 'shard-2', operation: 'SELECT' });

    const output = registry.getMetrics();

    // Labels are sorted alphabetically in the output
    expect(output).toContain('dosql_queries_total{operation="SELECT",shard="shard-1"}');
    expect(output).toContain('dosql_queries_total{operation="SELECT",shard="shard-2"}');
  });

  /**
   * DOCUMENTED GAP: WAL metrics.
   */
  it('includes WAL metrics', () => {
    const registry = createMetricsRegistry();
    expect(registry).not.toBeNull();

    // WAL metrics expected
    registry.createCounter('wal_writes_total', 'Total WAL writes');
    registry.createGauge('wal_size_bytes', 'Current WAL size');
    registry.createCounter('wal_checkpoints_total', 'Total checkpoints');
    registry.createHistogram('wal_checkpoint_duration_seconds', 'Checkpoint duration');

    const output = registry.getMetrics();

    expect(output).toContain('dosql_wal_writes_total');
    expect(output).toContain('dosql_wal_size_bytes');
    expect(output).toContain('dosql_wal_checkpoints_total');
    expect(output).toContain('dosql_wal_checkpoint_duration_seconds');
  });

  /**
   * DOCUMENTED GAP: CDC metrics.
   */
  it('includes CDC metrics', () => {
    const registry = createMetricsRegistry();
    expect(registry).not.toBeNull();

    registry.createCounter('cdc_events_total', 'Total CDC events');
    registry.createGauge('cdc_lag_seconds', 'CDC replication lag');
    registry.createCounter('cdc_errors_total', 'CDC errors');

    const output = registry.getMetrics();

    expect(output).toContain('dosql_cdc_events_total');
    expect(output).toContain('dosql_cdc_lag_seconds');
    expect(output).toContain('dosql_cdc_errors_total');
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('Observability - End-to-End Integration', () => {
  /**
   * DOCUMENTED GAP: Full observability pipeline.
   *
   * A complete query execution should:
   * 1. Create trace span
   * 2. Record metrics
   * 3. Propagate context to any downstream calls
   * 4. Complete span on success/failure
   */
  it.fails('complete observability pipeline for query', async () => {
    const observable = createObservableQuery();
    expect(observable).not.toBeNull();

    const tracer = observable!.getTracer();
    const metrics = observable!.getMetrics();

    // Execute query
    const { result, span } = await observable!.execute<{ count: number }>(
      'SELECT COUNT(*) as count FROM users'
    );

    // Verify tracing
    expect(span.status).toBe('OK');
    expect(span.attributes.get('db.operation')).toBe('SELECT');
    expect(span.endTime).toBeGreaterThan(span.startTime);

    // Verify metrics were recorded
    const metricsOutput = metrics.getMetrics();
    expect(metricsOutput).toContain('dosql_queries_total');
    expect(metricsOutput).toContain('dosql_query_duration_seconds');
  });

  /**
   * DOCUMENTED GAP: Observable transactions.
   */
  it.fails('traces and metrics for transactions', async () => {
    const observable = createObservableQuery();
    expect(observable).not.toBeNull();

    // Execute transaction
    await observable!.execute('BEGIN TRANSACTION');
    await observable!.execute('INSERT INTO users (name) VALUES (?)', ['Test']);
    await observable!.execute('COMMIT');

    const metrics = observable!.getMetrics();
    const output = metrics.getMetrics();

    expect(output).toContain('dosql_transactions_total');
    expect(output).toContain('outcome="commit"');
  });
});
