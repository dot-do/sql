/**
 * DoSQL Observability Types
 *
 * Core types for OpenTelemetry tracing and Prometheus metrics.
 */

// =============================================================================
// SPAN TYPES
// =============================================================================

/**
 * Span kind following OpenTelemetry specification
 */
export type SpanKind = 'INTERNAL' | 'SERVER' | 'CLIENT' | 'PRODUCER' | 'CONSUMER';

/**
 * Span status following OpenTelemetry specification
 */
export type SpanStatus = 'UNSET' | 'OK' | 'ERROR';

/**
 * Span event for recording point-in-time occurrences
 */
export interface SpanEvent {
  name: string;
  timestamp: number;
  attributes?: Map<string, AttributeValue>;
}

/**
 * Attribute value types
 */
export type AttributeValue = string | number | boolean | string[] | number[] | boolean[];

/**
 * OpenTelemetry Span interface
 */
export interface Span {
  readonly spanId: string;
  readonly traceId: string;
  readonly parentSpanId?: string;
  readonly name: string;
  readonly kind: SpanKind;
  readonly startTime: number;
  endTime?: number;
  status: SpanStatus;
  statusMessage?: string;
  readonly attributes: Map<string, AttributeValue>;
  readonly events: SpanEvent[];

  // Mutation methods
  setAttribute(key: string, value: AttributeValue): this;
  setStatus(status: SpanStatus, message?: string): this;
  addEvent(name: string, attributes?: Record<string, AttributeValue>): this;
  end(endTime?: number): void;
  isRecording(): boolean;
}

/**
 * Trace context for propagation (W3C Trace Context format)
 */
export interface TraceContext {
  traceId: string;
  spanId: string;
  traceFlags: number;
  traceState?: string;
}

/**
 * Span creation options
 */
export interface SpanOptions {
  kind?: SpanKind;
  parent?: TraceContext;
  attributes?: Record<string, AttributeValue>;
  startTime?: number;
}

/**
 * Tracer interface for creating and managing spans
 */
export interface Tracer {
  startSpan(name: string, options?: SpanOptions): Span;
  withSpan<T>(span: Span, fn: () => T): T;
  withSpanAsync<T>(span: Span, fn: () => Promise<T>): Promise<T>;
  getCurrentSpan(): Span | undefined;
  extractContext(headers: Headers): TraceContext | null;
  injectContext(headers: Headers, context: TraceContext): void;
}

// =============================================================================
// METRIC TYPES
// =============================================================================

/**
 * Counter metric - monotonically increasing value
 */
export interface Counter {
  readonly name: string;
  readonly help: string;
  readonly labels: string[];
  inc(labels?: Record<string, string>, value?: number): void;
  get(labels?: Record<string, string>): number;
  reset(): void;
}

/**
 * Histogram metric - distribution of values
 */
export interface Histogram {
  readonly name: string;
  readonly help: string;
  readonly labels: string[];
  readonly buckets: number[];
  observe(labels: Record<string, string>, value: number): void;
  get(labels?: Record<string, string>): HistogramValue;
  reset(): void;
}

/**
 * Histogram value with bucket counts and summary statistics
 */
export interface HistogramValue {
  sum: number;
  count: number;
  buckets: Map<number, number>;
}

/**
 * Gauge metric - value that can go up and down
 */
export interface Gauge {
  readonly name: string;
  readonly help: string;
  readonly labels: string[];
  set(labels: Record<string, string>, value: number): void;
  inc(labels?: Record<string, string>, value?: number): void;
  dec(labels?: Record<string, string>, value?: number): void;
  get(labels?: Record<string, string>): number;
  reset(): void;
}

/**
 * Metrics registry for managing and exporting metrics
 */
export interface MetricsRegistry {
  createCounter(name: string, help: string, labels?: string[]): Counter;
  createHistogram(name: string, help: string, labels?: string[], buckets?: number[]): Histogram;
  createGauge(name: string, help: string, labels?: string[]): Gauge;
  getMetrics(): string;
  reset(): void;
}

// =============================================================================
// SQL SANITIZER TYPES
// =============================================================================

/**
 * SQL statement types for categorization
 */
export type StatementType = 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE' | 'OTHER';

/**
 * SQL sanitizer interface for safe tracing
 */
export interface SQLSanitizer {
  sanitize(sql: string, params?: unknown[]): string;
  extractStatementType(sql: string): StatementType;
  extractTableNames(sql: string): string[];
}

// =============================================================================
// OBSERVABLE QUERY TYPES
// =============================================================================

/**
 * Query execution result with observability data
 */
export interface ObservableQueryResult<T> {
  result: T;
  span: Span;
}

/**
 * Observable query wrapper interface
 */
export interface ObservableQuery {
  execute<T>(sql: string, params?: unknown[]): Promise<ObservableQueryResult<T>>;
  getTracer(): Tracer;
  getMetrics(): MetricsRegistry;
}

// =============================================================================
// CONFIGURATION TYPES
// =============================================================================

/**
 * Tracing configuration
 */
export interface TracingConfig {
  enabled: boolean;
  serviceName: string;
  sampler: 'always_on' | 'always_off' | 'probability' | 'rate_limiting';
  samplingRate: number;
  maxAttributeLength: number;
}

/**
 * Metrics configuration
 */
export interface MetricsConfig {
  enabled: boolean;
  prefix: string;
  defaultLabels: Record<string, string>;
  histogramBuckets: {
    latency: number[];
    size: number[];
  };
}

/**
 * Full observability configuration
 */
export interface ObservabilityConfig {
  tracing: TracingConfig;
  metrics: MetricsConfig;
}

/**
 * Default configuration
 */
export const DEFAULT_OBSERVABILITY_CONFIG: ObservabilityConfig = {
  tracing: {
    enabled: true,
    serviceName: 'dosql',
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
};
