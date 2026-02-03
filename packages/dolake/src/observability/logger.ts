/**
 * DoLake Trace-Aware Structured Logger
 *
 * Provides structured logging with automatic trace context propagation.
 * Log entries include traceId, spanId, and correlationId when available,
 * enabling correlation between logs and distributed traces.
 */

import type {
  TraceAwareLogger,
  LoggingConfig,
  StructuredLogEntry,
  Tracer,
} from './types.js';

// =============================================================================
// LOG LEVEL VALUES
// =============================================================================

const LOG_LEVEL_VALUES: Record<string, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

// =============================================================================
// TRACE-AWARE LOGGER IMPLEMENTATION
// =============================================================================

/**
 * Logger implementation that automatically includes trace context
 */
class TraceAwareLoggerImpl implements TraceAwareLogger {
  private readonly config: LoggingConfig;
  private readonly tracer: Tracer | null;
  private readonly defaultContext: Record<string, unknown>;
  private readonly traceId?: string;
  private readonly spanId?: string;
  private readonly correlationId?: string;

  constructor(
    config: LoggingConfig,
    tracer: Tracer | null = null,
    defaultContext: Record<string, unknown> = {},
    traceId?: string,
    spanId?: string,
    correlationId?: string
  ) {
    this.config = config;
    this.tracer = tracer;
    this.defaultContext = defaultContext;
    this.traceId = traceId;
    this.spanId = spanId;
    this.correlationId = correlationId;
  }

  private shouldLog(level: string): boolean {
    return (LOG_LEVEL_VALUES[level] ?? 0) >= (LOG_LEVEL_VALUES[this.config.level] ?? 0);
  }

  private getTraceContext(): { traceId?: string; spanId?: string } {
    if (this.traceId || this.spanId) {
      return { traceId: this.traceId, spanId: this.spanId };
    }

    if (this.tracer) {
      const currentSpan = this.tracer.getCurrentSpan();
      if (currentSpan && currentSpan.isRecording()) {
        return {
          traceId: currentSpan.traceId,
          spanId: currentSpan.spanId,
        };
      }
    }

    return {};
  }

  private log(
    level: 'debug' | 'info' | 'warn' | 'error',
    message: string,
    context?: Record<string, unknown>,
    error?: unknown
  ): void {
    if (!this.config.enabled || !this.shouldLog(level)) {
      return;
    }

    const traceContext = this.getTraceContext();

    const mergedContext = context
      ? { ...this.defaultContext, ...context }
      : Object.keys(this.defaultContext).length > 0
        ? this.defaultContext
        : undefined;

    const entry: StructuredLogEntry = {
      timestamp: new Date().toISOString(),
      level,
      message,
      component: this.config.component,
    };

    if (traceContext.traceId) {
      entry.traceId = traceContext.traceId;
    }
    if (traceContext.spanId) {
      entry.spanId = traceContext.spanId;
    }
    if (this.correlationId) {
      entry.correlationId = this.correlationId;
    }

    if (mergedContext && Object.keys(mergedContext).length > 0) {
      entry.context = mergedContext;
    }

    if (error) {
      if (error instanceof Error) {
        entry.error = {
          name: error.name,
          message: error.message,
        };
        if (this.config.includeStackTraces && error.stack) {
          entry.error.stack = error.stack;
        }
      } else {
        entry.error = {
          name: 'UnknownError',
          message: String(error),
        };
      }
    }

    const output = JSON.stringify(entry);
    switch (level) {
      case 'debug':
      case 'info':
        console.log(output);
        break;
      case 'warn':
        console.warn(output);
        break;
      case 'error':
        console.error(output);
        break;
    }
  }

  debug(message: string, context?: Record<string, unknown>): void {
    this.log('debug', message, context);
  }

  info(message: string, context?: Record<string, unknown>): void {
    this.log('info', message, context);
  }

  warn(message: string, context?: Record<string, unknown>): void {
    this.log('warn', message, context);
  }

  error(message: string, error?: unknown, context?: Record<string, unknown>): void {
    this.log('error', message, context, error);
  }

  child(context: Record<string, unknown>): TraceAwareLogger {
    return new TraceAwareLoggerImpl(
      this.config,
      this.tracer,
      { ...this.defaultContext, ...context },
      this.traceId,
      this.spanId,
      this.correlationId
    );
  }

  withTrace(traceId: string, spanId: string): TraceAwareLogger {
    return new TraceAwareLoggerImpl(
      this.config,
      this.tracer,
      this.defaultContext,
      traceId,
      spanId,
      this.correlationId
    );
  }

  withCorrelation(correlationId: string): TraceAwareLogger {
    return new TraceAwareLoggerImpl(
      this.config,
      this.tracer,
      this.defaultContext,
      this.traceId,
      this.spanId,
      correlationId
    );
  }

  getCurrentCorrelationId(): string | undefined {
    return this.correlationId;
  }
}

/**
 * No-op logger for when logging is disabled
 */
class NoOpTraceAwareLogger implements TraceAwareLogger {
  debug(): void {}
  info(): void {}
  warn(): void {}
  error(): void {}
  child(): TraceAwareLogger { return this; }
  withTrace(): TraceAwareLogger { return this; }
  withCorrelation(): TraceAwareLogger { return this; }
  getCurrentCorrelationId(): string | undefined { return undefined; }
}

/**
 * Create a trace-aware structured logger
 */
export function createTraceAwareLogger(
  config: LoggingConfig,
  tracer: Tracer | null = null
): TraceAwareLogger {
  if (!config.enabled) {
    return new NoOpTraceAwareLogger();
  }
  return new TraceAwareLoggerImpl(config, tracer);
}
