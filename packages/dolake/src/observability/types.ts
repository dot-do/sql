/**
 * DoLake Observability Types
 *
 * Core types for OpenTelemetry tracing and Prometheus metrics
 * specific to CDC processing and lakehouse operations.
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
  extractContextFromWebSocket(attachment: WebSocketTraceAttachment): TraceContext | null;
  injectContext(headers: Headers, context: TraceContext): void;
}

/**
 * WebSocket attachment with trace context
 */
export interface WebSocketTraceAttachment {
  sourceDoId?: string;
  traceContext?: {
    traceparent?: string;
    tracestate?: string;
  };
}

// =============================================================================
// METRIC TYPES
// =============================================================================

/**
 * Counter metric
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
 * Histogram metric
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
 * Histogram value with bucket counts
 */
export interface HistogramValue {
  sum: number;
  count: number;
  buckets: Map<number, number>;
}

/**
 * Gauge metric
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
 * Metrics registry
 */
export interface MetricsRegistry {
  createCounter(name: string, help: string, labels?: string[]): Counter;
  createHistogram(name: string, help: string, labels?: string[], buckets?: number[]): Histogram;
  createGauge(name: string, help: string, labels?: string[]): Gauge;
  getMetrics(): string;
  reset(): void;
}

// =============================================================================
// DOLAKE-SPECIFIC TYPES
// =============================================================================

/**
 * CDC batch processing result for tracing
 */
export interface CDCProcessResult {
  eventsProcessed: number;
  eventsAccepted: number;
  isDuplicate: boolean;
  table?: string;
}

/**
 * Flush operation result for tracing
 */
export interface FlushTraceResult {
  filesWritten: number;
  eventsWritten: number;
  bytesWritten: number;
  tables: string[];
}

/**
 * Enhanced metrics for DoLake
 */
export interface DoLakeMetrics {
  // CDC metrics
  cdcBatchesTotal: Counter;
  cdcEventsTotal: Counter;
  cdcBytesTotal: Counter;
  cdcProcessingDuration: Histogram;
  cdcDuplicatesTotal: Counter;
  cdcLag: Gauge;

  // Flush metrics
  flushTotal: Counter;
  flushDuration: Histogram;
  flushFilesWritten: Counter;
  flushEventsWritten: Counter;
  bufferPressure: Gauge;
  bufferEvents: Gauge;
  bufferBytes: Gauge;

  // Parquet metrics
  parquetWritesTotal: Counter;
  parquetRowsWritten: Counter;
  parquetBytesWritten: Counter;
  parquetWriteDuration: Histogram;

  // Iceberg metrics
  icebergCommitsTotal: Counter;
  icebergCommitDuration: Histogram;
  icebergSnapshots: Gauge;
  icebergDataFiles: Gauge;

  // Compaction metrics
  compactionTotal: Counter;
  compactionFilesMerged: Counter;
  compactionBytesSaved: Counter;
  compactionDuration: Histogram;

  // Error metrics
  errorsTotal: Counter;

  // Connection metrics
  connectedSources: Gauge;
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
    serviceName: 'dolake',
    sampler: 'always_on',
    samplingRate: 1.0,
    maxAttributeLength: 256,
  },
  metrics: {
    enabled: true,
    prefix: 'dolake',
    defaultLabels: {},
    histogramBuckets: {
      latency: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
      size: [1000, 10000, 100000, 1000000, 10000000, 100000000],
    },
  },
};
