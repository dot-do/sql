/**
 * DoLake Structured Logging Module
 *
 * Provides structured logging with log levels, JSON output, and context propagation.
 *
 * @packageDocumentation
 */

// =============================================================================
// Types and Interfaces
// =============================================================================

/**
 * Log levels supported by the structured logger
 */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

/**
 * Numeric log level values for comparison
 */
const LOG_LEVEL_VALUES: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

/**
 * Structured log entry format
 */
export interface LogEntry {
  /** ISO 8601 timestamp */
  timestamp: string;
  /** Log level */
  level: LogLevel;
  /** Log message */
  message: string;
  /** Component that generated the log */
  component: string;
  /** Additional context data */
  context?: Record<string, unknown>;
  /** Error details if logging an error */
  error?: {
    name: string;
    message: string;
    stack?: string;
  };
}

/**
 * Logger configuration options
 */
export interface LoggerConfig {
  /** Component name for this logger */
  component: string;
  /** Minimum log level to output */
  level?: LogLevel;
  /** Include stack traces in error logs */
  includeStackTraces?: boolean;
  /** Additional default context */
  defaultContext?: Record<string, unknown>;
}

/**
 * Structured logger interface
 */
export interface StructuredLogger {
  debug(message: string, context?: Record<string, unknown>): void;
  info(message: string, context?: Record<string, unknown>): void;
  warn(message: string, context?: Record<string, unknown>): void;
  error(message: string, error?: unknown, context?: Record<string, unknown>): void;

  /** Create a child logger with additional context */
  child(context: Record<string, unknown>): StructuredLogger;
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Create ISO 8601 timestamp
 */
function createTimestamp(): string {
  return new Date().toISOString();
}

/**
 * Safely stringify objects with circular reference handling
 */
function safeStringify(obj: unknown, seen = new WeakSet()): unknown {
  if (obj === null || typeof obj !== 'object') {
    return obj;
  }

  if (seen.has(obj)) {
    return '[Circular]';
  }

  seen.add(obj);

  if (Array.isArray(obj)) {
    return obj.map(item => safeStringify(item, seen));
  }

  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(obj)) {
    result[key] = safeStringify(value, seen);
  }

  return result;
}

// =============================================================================
// Logger Implementation
// =============================================================================

/**
 * Logger implementation
 */
class Logger implements StructuredLogger {
  private level: LogLevel;
  private component: string;
  private defaultContext: Record<string, unknown>;
  private includeStackTraces: boolean;

  constructor(config: LoggerConfig) {
    this.component = config.component;
    this.level = config.level ?? 'info';
    this.defaultContext = config.defaultContext ?? {};
    this.includeStackTraces = config.includeStackTraces ?? true;
  }

  private shouldLog(level: LogLevel): boolean {
    return LOG_LEVEL_VALUES[level] >= LOG_LEVEL_VALUES[this.level];
  }

  private log(
    level: LogLevel,
    message: string,
    context?: Record<string, unknown>,
    error?: unknown
  ): void {
    if (!this.shouldLog(level)) {
      return;
    }

    // Merge contexts
    const mergedContext = context
      ? { ...this.defaultContext, ...context }
      : Object.keys(this.defaultContext).length > 0
        ? this.defaultContext
        : undefined;

    // Build log entry
    const entry: LogEntry = {
      timestamp: createTimestamp(),
      level,
      message,
      component: this.component,
    };

    if (mergedContext && Object.keys(mergedContext).length > 0) {
      entry.context = safeStringify(mergedContext) as Record<string, unknown>;
    }

    if (error) {
      if (error instanceof Error) {
        entry.error = {
          name: error.name,
          message: error.message,
        };
        if (this.includeStackTraces && error.stack) {
          entry.error.stack = error.stack;
        }
      } else {
        entry.error = {
          name: 'UnknownError',
          message: String(error),
        };
      }
    }

    // Output JSON to console
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

  child(context: Record<string, unknown>): StructuredLogger {
    return new Logger({
      component: this.component,
      level: this.level,
      defaultContext: { ...this.defaultContext, ...context },
      includeStackTraces: this.includeStackTraces,
    });
  }
}

/**
 * Create a new structured logger
 */
export function createLogger(config: LoggerConfig): StructuredLogger {
  return new Logger(config);
}
