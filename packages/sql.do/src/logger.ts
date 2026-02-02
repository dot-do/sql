/**
 * Internal structured logging module for the sql.do client SDK.
 *
 * Provides a configurable logger that outputs structured JSON logs by default,
 * with support for custom log sinks and log levels.
 *
 * @module logger
 * @internal
 */

/**
 * Log levels supported by the client logger.
 */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';

/**
 * Numeric log level values for comparison.
 */
const LOG_LEVEL_VALUES: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
  silent: 4,
};

/**
 * Structured log entry format.
 */
export interface LogEntry {
  /** ISO 8601 timestamp */
  timestamp: string;
  /** Log level */
  level: LogLevel;
  /** Log message */
  message: string;
  /** Component name (e.g., 'DoSQLClient', 'ConnectionPool') */
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
 * Log sink interface for custom output destinations.
 */
export interface LogSink {
  write(entry: LogEntry): void;
}

/**
 * Logger configuration options.
 */
export interface LoggerConfig {
  /** Minimum log level to output. @default 'warn' */
  level?: LogLevel;
  /** Custom log sink. @default ConsoleSink */
  sink?: LogSink;
  /** Include stack traces in error logs. @default false */
  includeStackTraces?: boolean;
}

/**
 * Default console sink that outputs structured JSON.
 */
class ConsoleSink implements LogSink {
  write(entry: LogEntry): void {
    const output = JSON.stringify(entry);
    if (entry.level === 'error') {
      console.error(output);
    } else if (entry.level === 'warn') {
      console.warn(output);
    } else {
      console.log(output);
    }
  }
}

/**
 * No-op sink for disabling logging.
 */
export class NoOpSink implements LogSink {
  write(_entry: LogEntry): void {
    // Intentionally empty
  }
}

/**
 * Internal logger implementation.
 */
class Logger {
  private level: LogLevel;
  private sink: LogSink;
  private component: string;
  private includeStackTraces: boolean;

  constructor(component: string, config: LoggerConfig = {}) {
    this.component = component;
    this.level = config.level ?? 'warn';
    this.sink = config.sink ?? new ConsoleSink();
    this.includeStackTraces = config.includeStackTraces ?? false;
  }

  private shouldLog(level: LogLevel): boolean {
    return LOG_LEVEL_VALUES[level] >= LOG_LEVEL_VALUES[this.level];
  }

  private log(
    level: LogLevel,
    message: string,
    context?: Record<string, unknown>,
    error?: Error
  ): void {
    if (!this.shouldLog(level)) {
      return;
    }

    const entry: LogEntry = {
      timestamp: new Date().toISOString(),
      level,
      message,
      component: this.component,
    };

    if (context && Object.keys(context).length > 0) {
      entry.context = context;
    }

    if (error) {
      entry.error = {
        name: error.name,
        message: error.message,
      };
      if (this.includeStackTraces && error.stack) {
        entry.error.stack = error.stack;
      }
    }

    this.sink.write(entry);
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

  error(message: string, error?: Error, context?: Record<string, unknown>): void {
    this.log('error', message, context, error);
  }

  /**
   * Creates a child logger with a different component name.
   */
  child(component: string): Logger {
    const child = new Logger(component, {
      level: this.level,
      sink: this.sink,
      includeStackTraces: this.includeStackTraces,
    });
    return child;
  }

  /**
   * Updates the log level at runtime.
   */
  setLevel(level: LogLevel): void {
    this.level = level;
  }

  /**
   * Gets the current log level.
   */
  getLevel(): LogLevel {
    return this.level;
  }
}

/**
 * Global logger configuration.
 */
let globalConfig: LoggerConfig = {
  level: 'warn',
  sink: new ConsoleSink(),
  includeStackTraces: false,
};

/**
 * Configures the global logger settings.
 *
 * @param config - Logger configuration options
 *
 * @example
 * ```typescript
 * import { configureLogger, NoOpSink } from 'sql.do';
 *
 * // Disable all logging
 * configureLogger({ sink: new NoOpSink() });
 *
 * // Enable debug logging
 * configureLogger({ level: 'debug' });
 *
 * // Custom log sink
 * configureLogger({
 *   sink: {
 *     write: (entry) => myCustomLogger.log(entry),
 *   },
 * });
 * ```
 */
export function configureLogger(config: Partial<LoggerConfig>): void {
  globalConfig = { ...globalConfig, ...config };
}

/**
 * Creates a logger instance for a component.
 *
 * @param component - Component name for log attribution
 * @returns Logger instance
 *
 * @internal
 */
export function createLogger(component: string): Logger {
  return new Logger(component, globalConfig);
}

/**
 * Pre-configured loggers for internal components.
 */
export const clientLogger = createLogger('DoSQLClient');
export const poolLogger = createLogger('ConnectionPool');
export const connectionLogger = createLogger('ConnectionManager');
