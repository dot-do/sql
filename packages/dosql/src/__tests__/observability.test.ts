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
// MOCK TYPES FOR OBSERVABILITY (Not Yet Implemented)
// =============================================================================

/**
 * OpenTelemetry Span interface (expected)
 */
interface Span {
  spanId: string;
  traceId: string;
  parentSpanId?: string;
  name: string;
  kind: 'INTERNAL' | 'SERVER' | 'CLIENT' | 'PRODUCER' | 'CONSUMER';
  startTime: number;
  endTime?: number;
  status: 'UNSET' | 'OK' | 'ERROR';
  attributes: Map<string, string | number | boolean>;
  events: SpanEvent[];
}

interface SpanEvent {
  name: string;
  timestamp: number;
  attributes?: Map<string, string | number | boolean>;
}

/**
 * Trace context for propagation
 */
interface TraceContext {
  traceId: string;
  spanId: string;
  traceFlags: number;
  traceState?: string;
}

/**
 * OpenTelemetry tracer interface (expected)
 */
interface Tracer {
  startSpan(name: string, options?: SpanOptions): Span;
  withSpan<T>(span: Span, fn: () => T): T;
  getCurrentSpan(): Span | undefined;
  extractContext(headers: Headers): TraceContext | null;
  injectContext(headers: Headers, context: TraceContext): void;
}

interface SpanOptions {
  kind?: Span['kind'];
  parent?: TraceContext;
  attributes?: Record<string, string | number | boolean>;
}

/**
 * Prometheus metric types
 */
interface Counter {
  name: string;
  help: string;
  labels: string[];
  inc(labels?: Record<string, string>, value?: number): void;
  get(labels?: Record<string, string>): number;
}

interface Histogram {
  name: string;
  help: string;
  labels: string[];
  buckets: number[];
  observe(labels: Record<string, string>, value: number): void;
  get(labels?: Record<string, string>): HistogramValue;
}

interface HistogramValue {
  sum: number;
  count: number;
  buckets: Map<number, number>;
}

interface Gauge {
  name: string;
  help: string;
  labels: string[];
  set(labels: Record<string, string>, value: number): void;
  inc(labels?: Record<string, string>, value?: number): void;
  dec(labels?: Record<string, string>, value?: number): void;
  get(labels?: Record<string, string>): number;
}

/**
 * Metrics registry interface (expected)
 */
interface MetricsRegistry {
  createCounter(name: string, help: string, labels?: string[]): Counter;
  createHistogram(name: string, help: string, labels?: string[], buckets?: number[]): Histogram;
  createGauge(name: string, help: string, labels?: string[]): Gauge;
  getMetrics(): string; // Prometheus format
  reset(): void;
}

/**
 * Query observability wrapper (expected interface)
 */
interface ObservableQuery {
  execute<T>(sql: string, params?: unknown[]): Promise<{ result: T; span: Span }>;
  getTracer(): Tracer;
  getMetrics(): MetricsRegistry;
}

/**
 * SQL sanitizer interface
 */
interface SQLSanitizer {
  sanitize(sql: string, params?: unknown[]): string;
  extractStatementType(sql: string): 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE' | 'OTHER';
  extractTableNames(sql: string): string[];
}

// =============================================================================
// MOCK FACTORIES (Return null since not implemented)
// =============================================================================

function createTracer(): Tracer | null {
  // TODO: Implement OpenTelemetry tracer
  return null;
}

function createMetricsRegistry(): MetricsRegistry | null {
  // TODO: Implement Prometheus metrics registry
  return null;
}

function createObservableQuery(): ObservableQuery | null {
  // TODO: Implement query wrapper with observability
  return null;
}

function createSQLSanitizer(): SQLSanitizer | null {
  // TODO: Implement SQL sanitizer
  return null;
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
    it.fails('emits trace spans for SELECT queries', async () => {
      const observable = createObservableQuery();
      expect(observable).not.toBeNull();

      const { result, span } = await observable!.execute<{ id: number }[]>(
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
    it.fails('includes required span attributes', async () => {
      const observable = createObservableQuery();
      expect(observable).not.toBeNull();

      const { span } = await observable!.execute(
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
    it.fails('captures error details in span on query failure', async () => {
      const observable = createObservableQuery();
      expect(observable).not.toBeNull();

      try {
        await observable!.execute('SELECT * FROM nonexistent_table');
      } catch {
        // Expected to fail
      }

      const span = observable!.getTracer().getCurrentSpan();
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
    it.fails('creates parent span for transactions', async () => {
      const observable = createObservableQuery();
      expect(observable).not.toBeNull();

      // Execute multiple queries in transaction
      await observable!.execute('BEGIN TRANSACTION');
      const { span: span1 } = await observable!.execute('INSERT INTO users (name) VALUES (?)', ['A']);
      const { span: span2 } = await observable!.execute('INSERT INTO users (name) VALUES (?)', ['B']);
      await observable!.execute('COMMIT');

      // Spans should share parent
      expect(span1.parentSpanId).toBeDefined();
      expect(span2.parentSpanId).toBeDefined();
      expect(span1.parentSpanId).toBe(span2.parentSpanId);
    });
  });

  describe('SQL Statement Sanitization', () => {
    /**
     * DOCUMENTED GAP: SQL statements must be sanitized before tracing.
     *
     * Sensitive data (passwords, tokens, PII) must be redacted from
     * SQL statements before they are added to spans.
     */
    it.fails('sanitizes parameter values in SQL statements', () => {
      const sanitizer = createSQLSanitizer();
      expect(sanitizer).not.toBeNull();

      const sql = 'SELECT * FROM users WHERE email = ? AND password = ?';
      const params = ['user@example.com', 'secret123'];

      const sanitized = sanitizer!.sanitize(sql, params);

      // Should replace values with placeholders
      expect(sanitized).not.toContain('user@example.com');
      expect(sanitized).not.toContain('secret123');
      expect(sanitized).toContain('?');
    });

    /**
     * DOCUMENTED GAP: Inline literals must be sanitized.
     *
     * Even when parameters are inlined in SQL, they should be sanitized.
     */
    it.fails('sanitizes inline string literals', () => {
      const sanitizer = createSQLSanitizer();
      expect(sanitizer).not.toBeNull();

      const sql = "SELECT * FROM users WHERE email = 'sensitive@email.com'";
      const sanitized = sanitizer!.sanitize(sql);

      expect(sanitized).not.toContain('sensitive@email.com');
      expect(sanitized).toMatch(/email = '\?'/);
    });

    /**
     * DOCUMENTED GAP: Extract statement type for metrics.
     *
     * Must correctly identify SELECT, INSERT, UPDATE, DELETE operations.
     */
    it.fails('extracts statement type correctly', () => {
      const sanitizer = createSQLSanitizer();
      expect(sanitizer).not.toBeNull();

      expect(sanitizer!.extractStatementType('SELECT * FROM users')).toBe('SELECT');
      expect(sanitizer!.extractStatementType('INSERT INTO users VALUES (1)')).toBe('INSERT');
      expect(sanitizer!.extractStatementType('UPDATE users SET name = ?')).toBe('UPDATE');
      expect(sanitizer!.extractStatementType('DELETE FROM users WHERE id = 1')).toBe('DELETE');
      expect(sanitizer!.extractStatementType('CREATE TABLE test (id INT)')).toBe('OTHER');
    });

    /**
     * DOCUMENTED GAP: Extract table names for labeling.
     */
    it.fails('extracts table names from queries', () => {
      const sanitizer = createSQLSanitizer();
      expect(sanitizer).not.toBeNull();

      expect(sanitizer!.extractTableNames('SELECT * FROM users')).toEqual(['users']);
      expect(sanitizer!.extractTableNames('SELECT * FROM users u JOIN orders o ON u.id = o.user_id'))
        .toEqual(['users', 'orders']);
      expect(sanitizer!.extractTableNames('INSERT INTO orders (user_id) VALUES (1)'))
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
    it.fails('collects query count metrics', async () => {
      const registry = createMetricsRegistry();
      expect(registry).not.toBeNull();

      const queryCounter = registry!.createCounter(
        'dosql_queries_total',
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
    it.fails('collects query latency histogram', async () => {
      const registry = createMetricsRegistry();
      expect(registry).not.toBeNull();

      const latencyHistogram = registry!.createHistogram(
        'dosql_query_duration_seconds',
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
    it.fails('tracks error rate metric', async () => {
      const registry = createMetricsRegistry();
      expect(registry).not.toBeNull();

      const errorCounter = registry!.createCounter(
        'dosql_query_errors_total',
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
    it.fails('tracks active connections gauge', () => {
      const registry = createMetricsRegistry();
      expect(registry).not.toBeNull();

      const connectionsGauge = registry!.createGauge(
        'dosql_active_connections',
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
    it.fails('collects transaction metrics', () => {
      const registry = createMetricsRegistry();
      expect(registry).not.toBeNull();

      const txnCounter = registry!.createCounter(
        'dosql_transactions_total',
        'Total number of transactions',
        ['outcome']
      );

      const txnDuration = registry!.createHistogram(
        'dosql_transaction_duration_seconds',
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
    it.fails('exposes metrics in Prometheus format', () => {
      const registry = createMetricsRegistry();
      expect(registry).not.toBeNull();

      // Add some metrics
      const counter = registry!.createCounter('dosql_test_total', 'Test counter');
      counter.inc();
      counter.inc();

      const metricsOutput = registry!.getMetrics();

      expect(metricsOutput).toContain('# HELP dosql_test_total Test counter');
      expect(metricsOutput).toContain('# TYPE dosql_test_total counter');
      expect(metricsOutput).toContain('dosql_test_total 2');
    });

    /**
     * DOCUMENTED GAP: Histogram bucket format.
     */
    it.fails('formats histogram buckets correctly', () => {
      const registry = createMetricsRegistry();
      expect(registry).not.toBeNull();

      const histogram = registry!.createHistogram(
        'dosql_latency_seconds',
        'Request latency',
        [],
        [0.1, 0.5, 1]
      );

      histogram.observe({}, 0.05);
      histogram.observe({}, 0.3);
      histogram.observe({}, 0.8);

      const output = registry!.getMetrics();

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
    it.fails('propagates trace context from Worker to DO', () => {
      const tracer = createTracer();
      expect(tracer).not.toBeNull();

      // Create a span in the Worker
      const workerSpan = tracer!.startSpan('worker.handleRequest', {
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
      tracer!.injectContext(headers, context);

      expect(headers.get('traceparent')).toBeDefined();
      expect(headers.get('traceparent')).toContain(context.traceId);
    });

    /**
     * DOCUMENTED GAP: Extract trace context in DO.
     *
     * DO should extract trace context from incoming request headers
     * and create child spans.
     */
    it.fails('extracts trace context in DO', () => {
      const tracer = createTracer();
      expect(tracer).not.toBeNull();

      // Simulate incoming request with trace context
      const headers = new Headers({
        traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
      });

      const context = tracer!.extractContext(headers);

      expect(context).not.toBeNull();
      expect(context!.traceId).toBe('4bf92f3577b34da6a3ce929d0e0e4736');
      expect(context!.spanId).toBe('00f067aa0ba902b7');
      expect(context!.traceFlags).toBe(1);
    });

    /**
     * DOCUMENTED GAP: Child spans reference parent trace.
     */
    it.fails('creates child spans with correct parent', () => {
      const tracer = createTracer();
      expect(tracer).not.toBeNull();

      const parentContext: TraceContext = {
        traceId: '4bf92f3577b34da6a3ce929d0e0e4736',
        spanId: '00f067aa0ba902b7',
        traceFlags: 1,
      };

      const childSpan = tracer!.startSpan('do.executeQuery', {
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
    it.fails('maintains trace across distributed query', () => {
      const tracer = createTracer();
      expect(tracer).not.toBeNull();

      // Start distributed query span
      const querySpan = tracer!.startSpan('dosql.distributedQuery', {
        kind: 'INTERNAL',
      });

      // Create child spans for each shard
      const shard1Span = tracer!.startSpan('dosql.shard.query', {
        parent: {
          traceId: querySpan.traceId,
          spanId: querySpan.spanId,
          traceFlags: 1,
        },
        attributes: { 'shard.id': 'shard-1' },
      });

      const shard2Span = tracer!.startSpan('dosql.shard.query', {
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
    it.fails('generates valid W3C traceparent header', () => {
      const tracer = createTracer();
      expect(tracer).not.toBeNull();

      const span = tracer!.startSpan('test');
      const headers = new Headers();
      tracer!.injectContext(headers, {
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
    it.fails('supports W3C tracestate header', () => {
      const tracer = createTracer();
      expect(tracer).not.toBeNull();

      const headers = new Headers({
        traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
        tracestate: 'congo=t61rcWkgMzE,rojo=00f067aa0ba902b7',
      });

      const context = tracer!.extractContext(headers);

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
  it.fails('DO exposes /metrics endpoint', async () => {
    // This would test against actual DO
    // For now, document expected behavior

    const expectedMetrics = [
      'dosql_queries_total',
      'dosql_query_duration_seconds',
      'dosql_query_errors_total',
      'dosql_active_connections',
      'dosql_transactions_total',
      'dosql_transaction_duration_seconds',
      'dosql_storage_operations_total',
      'dosql_storage_size_bytes',
    ];

    // Mock DO /metrics response
    const metricsEndpoint = createMetricsRegistry();
    expect(metricsEndpoint).not.toBeNull();

    const response = metricsEndpoint!.getMetrics();

    for (const metric of expectedMetrics) {
      expect(response).toContain(metric);
    }
  });

  /**
   * DOCUMENTED GAP: Per-shard metrics labeling.
   */
  it.fails('includes shard labels in metrics', () => {
    const registry = createMetricsRegistry();
    expect(registry).not.toBeNull();

    const counter = registry!.createCounter(
      'dosql_queries_total',
      'Total queries',
      ['shard', 'operation']
    );

    counter.inc({ shard: 'shard-1', operation: 'SELECT' });
    counter.inc({ shard: 'shard-2', operation: 'SELECT' });

    const output = registry!.getMetrics();

    expect(output).toContain('dosql_queries_total{shard="shard-1",operation="SELECT"}');
    expect(output).toContain('dosql_queries_total{shard="shard-2",operation="SELECT"}');
  });

  /**
   * DOCUMENTED GAP: WAL metrics.
   */
  it.fails('includes WAL metrics', () => {
    const registry = createMetricsRegistry();
    expect(registry).not.toBeNull();

    // WAL metrics expected
    registry!.createCounter('dosql_wal_writes_total', 'Total WAL writes');
    registry!.createGauge('dosql_wal_size_bytes', 'Current WAL size');
    registry!.createCounter('dosql_wal_checkpoints_total', 'Total checkpoints');
    registry!.createHistogram('dosql_wal_checkpoint_duration_seconds', 'Checkpoint duration');

    const output = registry!.getMetrics();

    expect(output).toContain('dosql_wal_writes_total');
    expect(output).toContain('dosql_wal_size_bytes');
    expect(output).toContain('dosql_wal_checkpoints_total');
    expect(output).toContain('dosql_wal_checkpoint_duration_seconds');
  });

  /**
   * DOCUMENTED GAP: CDC metrics.
   */
  it.fails('includes CDC metrics', () => {
    const registry = createMetricsRegistry();
    expect(registry).not.toBeNull();

    registry!.createCounter('dosql_cdc_events_total', 'Total CDC events');
    registry!.createGauge('dosql_cdc_lag_seconds', 'CDC replication lag');
    registry!.createCounter('dosql_cdc_errors_total', 'CDC errors');

    const output = registry!.getMetrics();

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
