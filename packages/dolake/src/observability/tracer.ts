/**
 * DoLake OpenTelemetry Tracer Implementation
 *
 * Lightweight OpenTelemetry-compatible tracer for Cloudflare Workers.
 * Includes WebSocket context extraction for CDC tracing.
 */

import type {
  Span,
  SpanKind,
  SpanStatus,
  SpanEvent,
  SpanOptions,
  TraceContext,
  Tracer,
  TracingConfig,
  AttributeValue,
  WebSocketTraceAttachment,
} from './types.js';

// =============================================================================
// SPAN IMPLEMENTATION
// =============================================================================

/**
 * Default span implementation
 */
class SpanImpl implements Span {
  readonly spanId: string;
  readonly traceId: string;
  readonly parentSpanId?: string;
  readonly name: string;
  readonly kind: SpanKind;
  readonly startTime: number;
  endTime?: number;
  status: SpanStatus = 'UNSET';
  statusMessage?: string;
  readonly attributes: Map<string, AttributeValue> = new Map();
  readonly events: SpanEvent[] = [];

  private recording = true;

  constructor(
    name: string,
    traceId: string,
    spanId: string,
    kind: SpanKind,
    parentSpanId?: string,
    startTime?: number
  ) {
    this.name = name;
    this.traceId = traceId;
    this.spanId = spanId;
    this.kind = kind;
    this.parentSpanId = parentSpanId;
    this.startTime = startTime ?? Date.now();
  }

  setAttribute(key: string, value: AttributeValue): this {
    if (this.recording) {
      this.attributes.set(key, value);
    }
    return this;
  }

  setStatus(status: SpanStatus, message?: string): this {
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
      this.endTime = endTime ?? Date.now();
      this.recording = false;
      if (this.status === 'UNSET') {
        this.status = 'OK';
      }
    }
  }

  isRecording(): boolean {
    return this.recording;
  }
}

/**
 * No-op span for when tracing is disabled
 */
class NoOpSpan implements Span {
  readonly spanId = '0000000000000000';
  readonly traceId = '00000000000000000000000000000000';
  readonly parentSpanId = undefined;
  readonly name = '';
  readonly kind: SpanKind = 'INTERNAL';
  readonly startTime = 0;
  endTime = 0;
  status: SpanStatus = 'OK';
  statusMessage = undefined;
  readonly attributes: Map<string, AttributeValue> = new Map();
  readonly events: SpanEvent[] = [];

  setAttribute(): this { return this; }
  setStatus(): this { return this; }
  addEvent(): this { return this; }
  end(): void {}
  isRecording(): boolean { return false; }
}

// =============================================================================
// TRACER IMPLEMENTATION
// =============================================================================

/**
 * Generate a random hex ID
 */
function generateId(bytes: number): string {
  const array = new Uint8Array(bytes);
  crypto.getRandomValues(array);
  return Array.from(array)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

/**
 * Default tracer implementation
 */
export class TracerImpl implements Tracer {
  private readonly config: TracingConfig;
  private currentSpan: Span | undefined;
  private readonly spanStack: Span[] = [];

  constructor(config: TracingConfig) {
    this.config = config;
  }

  startSpan(name: string, options?: SpanOptions): Span {
    if (!this.config.enabled || !this.shouldSample()) {
      return new NoOpSpan();
    }

    const traceId = options?.parent?.traceId ?? generateId(16);
    const spanId = generateId(8);
    const parentSpanId = options?.parent?.spanId ?? this.currentSpan?.spanId;

    const span = new SpanImpl(
      name,
      traceId,
      spanId,
      options?.kind ?? 'INTERNAL',
      parentSpanId,
      options?.startTime
    );

    if (options?.attributes) {
      for (const [key, value] of Object.entries(options.attributes)) {
        span.setAttribute(key, value);
      }
    }

    span.setAttribute('service.name', this.config.serviceName);

    this.spanStack.push(span);
    this.currentSpan = span;

    return span;
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

  /**
   * Extract W3C Trace Context from HTTP headers
   */
  extractContext(headers: Headers): TraceContext | null {
    const traceparent = headers.get('traceparent');
    if (!traceparent) {
      return null;
    }

    return this.parseTraceparent(traceparent, headers.get('tracestate') ?? undefined);
  }

  /**
   * Extract trace context from WebSocket attachment
   * Used for CDC batches from DoSQL
   */
  extractContextFromWebSocket(attachment: WebSocketTraceAttachment): TraceContext | null {
    if (!attachment.traceContext?.traceparent) {
      return null;
    }

    return this.parseTraceparent(
      attachment.traceContext.traceparent,
      attachment.traceContext.tracestate
    );
  }

  /**
   * Inject W3C Trace Context into HTTP headers
   */
  injectContext(headers: Headers, context: TraceContext): void {
    const flags = context.traceFlags.toString(16).padStart(2, '0');
    headers.set('traceparent', `00-${context.traceId}-${context.spanId}-${flags}`);

    if (context.traceState) {
      headers.set('tracestate', context.traceState);
    }
  }

  private parseTraceparent(traceparent: string, tracestate?: string): TraceContext | null {
    const parts = traceparent.split('-');
    if (parts.length !== 4) {
      return null;
    }

    const [version, traceId, spanId, flags] = parts;

    if (version !== '00') {
      return null;
    }

    if (!/^[0-9a-f]{32}$/.test(traceId) || traceId === '00000000000000000000000000000000') {
      return null;
    }

    if (!/^[0-9a-f]{16}$/.test(spanId) || spanId === '0000000000000000') {
      return null;
    }

    if (!/^[0-9a-f]{2}$/.test(flags)) {
      return null;
    }

    return {
      traceId,
      spanId,
      traceFlags: parseInt(flags, 16),
      traceState: tracestate,
    };
  }

  private shouldSample(): boolean {
    switch (this.config.sampler) {
      case 'always_on':
        return true;
      case 'always_off':
        return false;
      case 'probability':
        return Math.random() < this.config.samplingRate;
      case 'rate_limiting':
        return Math.random() < this.config.samplingRate;
      default:
        return true;
    }
  }
}

/**
 * No-op tracer
 */
export class NoOpTracer implements Tracer {
  private static readonly noOpSpan = new NoOpSpan();

  startSpan(): Span { return NoOpTracer.noOpSpan; }
  withSpan<T>(_span: Span, fn: () => T): T { return fn(); }
  async withSpanAsync<T>(_span: Span, fn: () => Promise<T>): Promise<T> { return fn(); }
  getCurrentSpan(): Span | undefined { return undefined; }
  extractContext(): TraceContext | null { return null; }
  extractContextFromWebSocket(): TraceContext | null { return null; }
  injectContext(): void {}
}

/**
 * Create a tracer instance
 */
export function createTracer(config: TracingConfig): Tracer {
  if (!config.enabled) {
    return new NoOpTracer();
  }
  return new TracerImpl(config);
}
