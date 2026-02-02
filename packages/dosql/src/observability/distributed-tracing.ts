/**
 * DoSQL Distributed Tracing Module
 *
 * Provides distributed tracing with correlation IDs that propagate across
 * Durable Object boundaries. This module bridges the observability tracer
 * with the logging system for unified observability.
 *
 * Key features:
 * - Correlation ID propagation across DO boundaries via HTTP headers
 * - Integration with structured logging for trace correlation
 * - Span timing with metadata collection
 * - Context propagation for async operations
 *
 * @packageDocumentation
 */

import { AsyncLocalStorage } from 'node:async_hooks';
import type {
  Span,
  SpanKind,
  SpanOptions,
  TraceContext,
  Tracer,
  AttributeValue,
} from './types.js';

// =============================================================================
// DISTRIBUTED TRACE CONTEXT
// =============================================================================

/**
 * Extended trace context with additional distributed tracing metadata
 */
export interface DistributedTraceContext extends TraceContext {
  /** Correlation ID for cross-service request tracking */
  correlationId: string;
  /** Origin service/DO that started the trace */
  originService?: string | undefined;
  /** Timestamp when trace was started */
  startTimestamp: number;
  /** Baggage items for cross-boundary context propagation */
  baggage: Map<string, string>;
  /** Sampling decision for consistent sampling across services */
  sampled: boolean;
}

/**
 * Span with extended distributed tracing metadata
 */
export interface DistributedSpan extends Span {
  /** Get the correlation ID */
  getCorrelationId(): string;
  /** Get baggage item */
  getBaggage(key: string): string | undefined;
  /** Set baggage item (propagates to child spans) */
  setBaggage(key: string, value: string): void;
  /** Get all baggage items */
  getAllBaggage(): Map<string, string>;
  /** Get the distributed trace context for propagation */
  getDistributedContext(): DistributedTraceContext;
  /** Get elapsed time in milliseconds */
  getElapsedMs(): number;
}

/**
 * Async storage for distributed trace context
 */
export const DistributedTraceStorage = new AsyncLocalStorage<DistributedTraceContext>();

// =============================================================================
// SPAN IMPLEMENTATION
// =============================================================================

/**
 * Generate a correlation ID
 */
function generateCorrelationId(): string {
  // Use crypto.randomUUID if available
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  // Fallback: generate 32 hex characters
  return Array.from({ length: 32 }, () =>
    Math.floor(Math.random() * 16).toString(16)
  ).join('');
}

/**
 * Generate a random hex ID of specified byte length
 */
function generateId(bytes: number): string {
  if (typeof crypto !== 'undefined' && crypto.getRandomValues) {
    const array = new Uint8Array(bytes);
    crypto.getRandomValues(array);
    return Array.from(array)
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');
  }
  // Fallback
  return Array.from({ length: bytes * 2 }, () =>
    Math.floor(Math.random() * 16).toString(16)
  ).join('');
}

/**
 * Distributed span implementation with correlation ID support
 */
class DistributedSpanImpl implements DistributedSpan {
  readonly spanId: string;
  readonly traceId: string;
  readonly parentSpanId?: string;
  readonly name: string;
  readonly kind: SpanKind;
  readonly startTime: number;
  endTime?: number;
  status: 'UNSET' | 'OK' | 'ERROR' = 'UNSET';
  statusMessage?: string;
  readonly attributes: Map<string, AttributeValue> = new Map();
  readonly events: Array<{
    name: string;
    timestamp: number;
    attributes?: Map<string, AttributeValue>;
  }> = [];

  private readonly correlationId: string;
  private readonly baggage: Map<string, string>;
  private readonly originService?: string;
  private readonly startTimestamp: number;
  private readonly sampled: boolean;
  private recording = true;

  constructor(
    name: string,
    context: DistributedTraceContext,
    kind: SpanKind,
    parentSpanId?: string,
    startTime?: number
  ) {
    this.name = name;
    this.traceId = context.traceId;
    this.spanId = generateId(8);
    this.kind = kind;
    this.parentSpanId = parentSpanId ?? context.spanId;
    this.startTime = startTime ?? (typeof performance !== 'undefined' ? performance.now() : Date.now());
    this.correlationId = context.correlationId;
    this.baggage = new Map(context.baggage);
    this.originService = context.originService;
    this.startTimestamp = context.startTimestamp;
    this.sampled = context.sampled;
  }

  setAttribute(key: string, value: AttributeValue): this {
    if (this.recording) {
      this.attributes.set(key, value);
    }
    return this;
  }

  setStatus(status: 'UNSET' | 'OK' | 'ERROR', message?: string): this {
    if (this.recording) {
      this.status = status;
      this.statusMessage = message;
    }
    return this;
  }

  addEvent(name: string, attributes?: Record<string, AttributeValue>): this {
    if (this.recording) {
      this.events.push({
        name,
        timestamp: Date.now(),
        attributes: attributes ? new Map(Object.entries(attributes)) : undefined,
      });
    }
    return this;
  }

  end(endTime?: number): void {
    if (this.recording) {
      this.endTime = endTime ?? (typeof performance !== 'undefined' ? performance.now() : Date.now());
      this.recording = false;
      if (this.status === 'UNSET') {
        this.status = 'OK';
      }
    }
  }

  isRecording(): boolean {
    return this.recording;
  }

  getCorrelationId(): string {
    return this.correlationId;
  }

  getBaggage(key: string): string | undefined {
    return this.baggage.get(key);
  }

  setBaggage(key: string, value: string): void {
    this.baggage.set(key, value);
  }

  getAllBaggage(): Map<string, string> {
    return new Map(this.baggage);
  }

  getDistributedContext(): DistributedTraceContext {
    return {
      traceId: this.traceId,
      spanId: this.spanId,
      traceFlags: this.sampled ? 1 : 0,
      correlationId: this.correlationId,
      originService: this.originService,
      startTimestamp: this.startTimestamp,
      baggage: new Map(this.baggage),
      sampled: this.sampled,
    };
  }

  getElapsedMs(): number {
    const endTime = this.endTime ?? (typeof performance !== 'undefined' ? performance.now() : Date.now());
    return endTime - this.startTime;
  }
}

/**
 * No-op distributed span implementation
 */
class NoOpDistributedSpan implements DistributedSpan {
  readonly spanId = '0000000000000000';
  readonly traceId = '00000000000000000000000000000000';
  readonly parentSpanId = undefined;
  readonly name = '';
  readonly kind: SpanKind = 'INTERNAL';
  readonly startTime = 0;
  endTime = 0;
  status: 'UNSET' | 'OK' | 'ERROR' = 'OK';
  statusMessage = undefined;
  readonly attributes: Map<string, AttributeValue> = new Map();
  readonly events: Array<{
    name: string;
    timestamp: number;
    attributes?: Map<string, AttributeValue>;
  }> = [];

  setAttribute(): this { return this; }
  setStatus(): this { return this; }
  addEvent(): this { return this; }
  end(): void {}
  isRecording(): boolean { return false; }
  getCorrelationId(): string { return 'noop'; }
  getBaggage(): string | undefined { return undefined; }
  setBaggage(): void {}
  getAllBaggage(): Map<string, string> { return new Map(); }
  getDistributedContext(): DistributedTraceContext {
    return {
      traceId: this.traceId,
      spanId: this.spanId,
      traceFlags: 0,
      correlationId: 'noop',
      startTimestamp: 0,
      baggage: new Map(),
      sampled: false,
    };
  }
  getElapsedMs(): number { return 0; }
}

// =============================================================================
// DISTRIBUTED TRACER
// =============================================================================

/**
 * Configuration for distributed tracer
 */
export interface DistributedTracerConfig {
  /** Whether tracing is enabled */
  enabled: boolean;
  /** Service name for this instance */
  serviceName: string;
  /** Sampling strategy */
  sampler: 'always_on' | 'always_off' | 'probability';
  /** Sampling rate when using probability sampler */
  samplingRate: number;
  /** Custom header names for context propagation */
  propagationHeaders?: {
    traceParent?: string;
    traceState?: string;
    correlationId?: string;
    baggage?: string;
  };
}

/**
 * Default distributed tracer configuration
 */
export const DEFAULT_DISTRIBUTED_TRACER_CONFIG: DistributedTracerConfig = {
  enabled: true,
  serviceName: 'dosql',
  sampler: 'always_on',
  samplingRate: 1.0,
  propagationHeaders: {
    traceParent: 'traceparent',
    traceState: 'tracestate',
    correlationId: 'x-correlation-id',
    baggage: 'baggage',
  },
};

/**
 * Distributed tracer interface extending base tracer
 */
export interface DistributedTracer extends Tracer {
  /** Start a span with distributed context */
  startDistributedSpan(name: string, options?: DistributedSpanOptions): DistributedSpan;

  /** Run a function within a distributed span context */
  withDistributedSpan<T>(span: DistributedSpan, fn: () => T): T;

  /** Run an async function within a distributed span context */
  withDistributedSpanAsync<T>(span: DistributedSpan, fn: () => Promise<T>): Promise<T>;

  /** Extract distributed context from HTTP headers */
  extractDistributedContext(headers: Headers): DistributedTraceContext | null;

  /** Inject distributed context into HTTP headers */
  injectDistributedContext(headers: Headers, context: DistributedTraceContext): void;

  /** Get the current distributed context from async local storage */
  getCurrentDistributedContext(): DistributedTraceContext | undefined;

  /** Get the current correlation ID */
  getCurrentCorrelationId(): string | undefined;

  /** Create a new root context for starting a trace */
  createRootContext(serviceName?: string): DistributedTraceContext;
}

/**
 * Options for distributed span creation
 */
export interface DistributedSpanOptions extends SpanOptions {
  /** Parent distributed context */
  parentContext?: DistributedTraceContext;
  /** Initial baggage items */
  baggage?: Record<string, string>;
}

/**
 * Distributed tracer implementation
 */
export class DistributedTracerImpl implements DistributedTracer {
  private readonly config: DistributedTracerConfig;
  private currentSpan: Span | undefined;
  private readonly spanStack: Span[] = [];

  constructor(config: Partial<DistributedTracerConfig> = {}) {
    this.config = { ...DEFAULT_DISTRIBUTED_TRACER_CONFIG, ...config };
  }

  // ==========================================================================
  // Tracer interface implementation
  // ==========================================================================

  startSpan(name: string, options?: SpanOptions): Span {
    const distContext = this.getCurrentDistributedContext();
    if (distContext) {
      return this.startDistributedSpan(name, {
        ...options,
        parentContext: distContext,
      });
    }

    // Create with new root context
    return this.startDistributedSpan(name, options);
  }

  withSpan<T>(span: Span, fn: () => T): T {
    const previousSpan = this.currentSpan;
    this.currentSpan = span;

    try {
      const result = fn();
      span.setStatus('OK');
      return result;
    } catch (error) {
      span.setStatus('ERROR', error instanceof Error ? error.message : String(error));
      span.addEvent('exception', {
        'exception.type': error instanceof Error ? error.constructor.name : 'Error',
        'exception.message': error instanceof Error ? error.message : String(error),
      });
      throw error;
    } finally {
      span.end();
      this.currentSpan = previousSpan;
      this.spanStack.pop();
    }
  }

  async withSpanAsync<T>(span: Span, fn: () => Promise<T>): Promise<T> {
    const previousSpan = this.currentSpan;
    this.currentSpan = span;

    try {
      const result = await fn();
      span.setStatus('OK');
      return result;
    } catch (error) {
      span.setStatus('ERROR', error instanceof Error ? error.message : String(error));
      span.addEvent('exception', {
        'exception.type': error instanceof Error ? error.constructor.name : 'Error',
        'exception.message': error instanceof Error ? error.message : String(error),
      });
      throw error;
    } finally {
      span.end();
      this.currentSpan = previousSpan;
      this.spanStack.pop();
    }
  }

  getCurrentSpan(): Span | undefined {
    return this.currentSpan;
  }

  extractContext(headers: Headers): TraceContext | null {
    const distContext = this.extractDistributedContext(headers);
    if (!distContext) return null;
    return {
      traceId: distContext.traceId,
      spanId: distContext.spanId,
      traceFlags: distContext.traceFlags,
      traceState: distContext.traceState,
    };
  }

  injectContext(headers: Headers, context: TraceContext): void {
    const flags = context.traceFlags.toString(16).padStart(2, '0');
    const headerName = this.config.propagationHeaders?.traceParent ?? 'traceparent';
    headers.set(headerName, `00-${context.traceId}-${context.spanId}-${flags}`);

    if (context.traceState) {
      const stateHeader = this.config.propagationHeaders?.traceState ?? 'tracestate';
      headers.set(stateHeader, context.traceState);
    }
  }

  // ==========================================================================
  // Distributed tracer interface implementation
  // ==========================================================================

  startDistributedSpan(name: string, options?: DistributedSpanOptions): DistributedSpan {
    if (!this.config.enabled || !this.shouldSample()) {
      return new NoOpDistributedSpan();
    }

    let context: DistributedTraceContext;

    if (options?.parentContext) {
      context = options.parentContext;
    } else if (options?.parent) {
      // Convert basic TraceContext to DistributedTraceContext
      context = {
        ...options.parent,
        correlationId: generateCorrelationId(),
        startTimestamp: Date.now(),
        baggage: new Map(),
        sampled: (options.parent.traceFlags & 1) === 1,
      };
    } else {
      // Create new root context
      context = this.createRootContext();
    }

    // Merge baggage from options
    if (options?.baggage) {
      for (const [key, value] of Object.entries(options.baggage)) {
        context.baggage.set(key, value);
      }
    }

    const span = new DistributedSpanImpl(
      name,
      context,
      options?.kind ?? 'INTERNAL',
      this.currentSpan?.spanId,
      options?.startTime
    );

    // Set initial attributes
    if (options?.attributes) {
      for (const [key, value] of Object.entries(options.attributes)) {
        span.setAttribute(key, value);
      }
    }

    // Set service name
    span.setAttribute('service.name', this.config.serviceName);
    span.setAttribute('correlation.id', context.correlationId);

    this.spanStack.push(span);
    this.currentSpan = span;

    return span;
  }

  withDistributedSpan<T>(span: DistributedSpan, fn: () => T): T {
    const context = span.getDistributedContext();
    return DistributedTraceStorage.run(context, () => {
      return this.withSpan(span, fn);
    });
  }

  async withDistributedSpanAsync<T>(span: DistributedSpan, fn: () => Promise<T>): Promise<T> {
    const context = span.getDistributedContext();
    return DistributedTraceStorage.run(context, () => {
      return this.withSpanAsync(span, fn);
    });
  }

  extractDistributedContext(headers: Headers): DistributedTraceContext | null {
    const headerNames = this.config.propagationHeaders ?? {};
    const traceparentHeader = headerNames.traceParent ?? 'traceparent';
    const correlationHeader = headerNames.correlationId ?? 'x-correlation-id';
    const baggageHeader = headerNames.baggage ?? 'baggage';

    const traceparent = headers.get(traceparentHeader);
    if (!traceparent) {
      // Try to extract just correlation ID if available
      const correlationId = headers.get(correlationHeader);
      if (correlationId) {
        return {
          traceId: generateId(16),
          spanId: generateId(8),
          traceFlags: 1,
          correlationId,
          startTimestamp: Date.now(),
          baggage: this.parseBaggage(headers.get(baggageHeader) ?? ''),
          sampled: true,
        };
      }
      return null;
    }

    // Parse W3C traceparent: version-traceId-parentId-flags
    const parts = traceparent.split('-');
    if (parts.length !== 4) return null;

    const [version, traceId, spanId, flags] = parts;

    // Validate version
    if (version !== '00') return null;

    // Validate trace ID (32 hex chars, not all zeros)
    if (!/^[0-9a-f]{32}$/.test(traceId) || traceId === '00000000000000000000000000000000') {
      return null;
    }

    // Validate span ID (16 hex chars, not all zeros)
    if (!/^[0-9a-f]{16}$/.test(spanId) || spanId === '0000000000000000') {
      return null;
    }

    // Validate flags (2 hex chars)
    if (!/^[0-9a-f]{2}$/.test(flags)) return null;

    const traceFlags = parseInt(flags, 16);
    const correlationId = headers.get(correlationHeader) ?? generateCorrelationId();
    const traceState = headers.get(headerNames.traceState ?? 'tracestate') ?? undefined;
    const baggage = this.parseBaggage(headers.get(baggageHeader) ?? '');

    return {
      traceId,
      spanId,
      traceFlags,
      traceState,
      correlationId,
      startTimestamp: Date.now(),
      baggage,
      sampled: (traceFlags & 1) === 1,
    };
  }

  injectDistributedContext(headers: Headers, context: DistributedTraceContext): void {
    const headerNames = this.config.propagationHeaders ?? {};

    // Inject W3C traceparent
    const flags = context.traceFlags.toString(16).padStart(2, '0');
    headers.set(
      headerNames.traceParent ?? 'traceparent',
      `00-${context.traceId}-${context.spanId}-${flags}`
    );

    // Inject tracestate if present
    if (context.traceState) {
      headers.set(headerNames.traceState ?? 'tracestate', context.traceState);
    }

    // Inject correlation ID
    headers.set(headerNames.correlationId ?? 'x-correlation-id', context.correlationId);

    // Inject baggage
    if (context.baggage.size > 0) {
      const baggageStr = this.serializeBaggage(context.baggage);
      headers.set(headerNames.baggage ?? 'baggage', baggageStr);
    }
  }

  getCurrentDistributedContext(): DistributedTraceContext | undefined {
    return DistributedTraceStorage.getStore();
  }

  getCurrentCorrelationId(): string | undefined {
    return DistributedTraceStorage.getStore()?.correlationId;
  }

  createRootContext(serviceName?: string): DistributedTraceContext {
    return {
      traceId: generateId(16),
      spanId: generateId(8),
      traceFlags: 1,
      correlationId: generateCorrelationId(),
      originService: serviceName ?? this.config.serviceName,
      startTimestamp: Date.now(),
      baggage: new Map(),
      sampled: true,
    };
  }

  // ==========================================================================
  // Private helpers
  // ==========================================================================

  private shouldSample(): boolean {
    switch (this.config.sampler) {
      case 'always_on':
        return true;
      case 'always_off':
        return false;
      case 'probability':
        return Math.random() < this.config.samplingRate;
      default:
        return true;
    }
  }

  private parseBaggage(baggageStr: string): Map<string, string> {
    const baggage = new Map<string, string>();
    if (!baggageStr) return baggage;

    // Parse W3C baggage format: key1=value1,key2=value2
    const pairs = baggageStr.split(',');
    for (const pair of pairs) {
      const [key, value] = pair.split('=');
      if (key && value) {
        baggage.set(key.trim(), decodeURIComponent(value.trim()));
      }
    }
    return baggage;
  }

  private serializeBaggage(baggage: Map<string, string>): string {
    const pairs: string[] = [];
    for (const [key, value] of baggage) {
      pairs.push(`${key}=${encodeURIComponent(value)}`);
    }
    return pairs.join(',');
  }
}

/**
 * No-op distributed tracer for when tracing is disabled
 */
export class NoOpDistributedTracer implements DistributedTracer {
  private static readonly noOpSpan = new NoOpDistributedSpan();

  startSpan(): Span {
    return NoOpDistributedTracer.noOpSpan;
  }

  withSpan<T>(_span: Span, fn: () => T): T {
    return fn();
  }

  async withSpanAsync<T>(_span: Span, fn: () => Promise<T>): Promise<T> {
    return fn();
  }

  getCurrentSpan(): Span | undefined {
    return undefined;
  }

  extractContext(): TraceContext | null {
    return null;
  }

  injectContext(): void {}

  startDistributedSpan(): DistributedSpan {
    return NoOpDistributedTracer.noOpSpan;
  }

  withDistributedSpan<T>(_span: DistributedSpan, fn: () => T): T {
    return fn();
  }

  async withDistributedSpanAsync<T>(_span: DistributedSpan, fn: () => Promise<T>): Promise<T> {
    return fn();
  }

  extractDistributedContext(): DistributedTraceContext | null {
    return null;
  }

  injectDistributedContext(): void {}

  getCurrentDistributedContext(): DistributedTraceContext | undefined {
    return undefined;
  }

  getCurrentCorrelationId(): string | undefined {
    return undefined;
  }

  createRootContext(): DistributedTraceContext {
    return {
      traceId: '00000000000000000000000000000000',
      spanId: '0000000000000000',
      traceFlags: 0,
      correlationId: 'noop',
      startTimestamp: 0,
      baggage: new Map(),
      sampled: false,
    };
  }
}

/**
 * Create a distributed tracer instance
 */
export function createDistributedTracer(config: Partial<DistributedTracerConfig> = {}): DistributedTracer {
  const mergedConfig = { ...DEFAULT_DISTRIBUTED_TRACER_CONFIG, ...config };
  if (!mergedConfig.enabled) {
    return new NoOpDistributedTracer();
  }
  return new DistributedTracerImpl(mergedConfig);
}

// =============================================================================
// HELPER FUNCTIONS FOR DO BOUNDARY CROSSING
// =============================================================================

/**
 * Prepare headers for a DO fetch call with trace context propagation
 */
export function prepareTracedFetch(
  tracer: DistributedTracer,
  request: Request,
  additionalHeaders?: Record<string, string>
): Request {
  const context = tracer.getCurrentDistributedContext();
  if (!context) {
    return request;
  }

  const headers = new Headers(request.headers);
  tracer.injectDistributedContext(headers, context);

  // Add any additional headers
  if (additionalHeaders) {
    for (const [key, value] of Object.entries(additionalHeaders)) {
      headers.set(key, value);
    }
  }

  return new Request(request.url, {
    method: request.method,
    headers,
    body: request.body,
    redirect: request.redirect,
  });
}

/**
 * Extract trace context from incoming request and start a server span
 */
export function startServerSpan(
  tracer: DistributedTracer,
  request: Request,
  spanName: string,
  additionalAttributes?: Record<string, AttributeValue>
): DistributedSpan {
  const parentContext = tracer.extractDistributedContext(request.headers);

  const span = tracer.startDistributedSpan(spanName, {
    kind: 'SERVER',
    parentContext: parentContext ?? undefined,
    attributes: {
      'http.method': request.method,
      'http.url': request.url,
      'http.target': new URL(request.url).pathname,
      ...additionalAttributes,
    },
  });

  return span;
}

/**
 * Run a function with distributed tracing context
 */
export async function withDistributedContext<T>(
  context: DistributedTraceContext,
  fn: () => T | Promise<T>
): Promise<T> {
  return DistributedTraceStorage.run(context, fn);
}
