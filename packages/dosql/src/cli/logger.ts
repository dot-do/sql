/**
 * CLI Logger Module
 *
 * Provides structured logging for CLI operations while maintaining
 * user-friendly output for interactive use.
 *
 * @module cli/logger
 */

import { createLogger, type StructuredLogger, type LogLevel, ConsoleSink } from '../logging/index.js';

// =============================================================================
// TYPES
// =============================================================================

/**
 * CLI output types for different kinds of messages
 */
export type OutputType = 'result' | 'info' | 'success' | 'warning' | 'error';

/**
 * CLI Logger configuration
 */
export interface CLILoggerConfig {
  /** Log level for structured logging */
  level?: LogLevel;
  /** Whether to use JSON output format */
  jsonOutput?: boolean;
  /** Custom output function (defaults to console.log) */
  outputFn?: (msg: string) => void;
  /** Custom error output function (defaults to console.error) */
  errorFn?: (msg: string) => void;
}

/**
 * CLI Logger interface combining user output and structured logging
 */
export interface CLILogger {
  /** Output a result to the user (query results, etc.) */
  output(message: string): void;

  /** Output structured data (JSON, tables, etc.) */
  outputData(data: unknown): void;

  /** Log informational message */
  info(message: string, context?: Record<string, unknown>): void;

  /** Log success message */
  success(message: string, context?: Record<string, unknown>): void;

  /** Log warning message */
  warn(message: string, context?: Record<string, unknown>): void;

  /** Log error message */
  error(message: string, error?: Error, context?: Record<string, unknown>): void;

  /** Log debug message (only shown when debug level is enabled) */
  debug(message: string, context?: Record<string, unknown>): void;

  /** Get the underlying structured logger */
  getStructuredLogger(): StructuredLogger;
}

// =============================================================================
// IMPLEMENTATION
// =============================================================================

/**
 * Create a CLI logger instance
 *
 * Note: Uses console.log/console.error at call time (not creation time)
 * to support test mocking.
 */
export function createCLILogger(config: CLILoggerConfig = {}): CLILogger {
  const customOutputFn = config.outputFn;
  const customErrorFn = config.errorFn;
  const jsonOutput = config.jsonOutput ?? false;

  // Helper to get output function at call time (allows test mocking)
  const getOutputFn = (): ((msg: string) => void) => customOutputFn ?? console.log;
  const getErrorFn = (): ((msg: string) => void) => customErrorFn ?? console.error;

  // Create underlying structured logger for operational logging
  const structuredLogger = createLogger({
    level: config.level ?? 'info',
    sink: new ConsoleSink({ prettyPrint: !jsonOutput }),
  });

  return {
    output(message: string): void {
      getOutputFn()(message);
    },

    outputData(data: unknown): void {
      if (jsonOutput) {
        getOutputFn()(JSON.stringify(data, null, 2));
      } else if (Array.isArray(data) && data.length > 0) {
        console.table(data);
      } else {
        getOutputFn()(JSON.stringify(data, null, 2));
      }
    },

    info(message: string, context?: Record<string, unknown>): void {
      if (jsonOutput) {
        structuredLogger.info(message, { ...context, operation: 'cli' });
      } else {
        getOutputFn()(message);
        if (context && Object.keys(context).length > 0) {
          structuredLogger.debug(message, context);
        }
      }
    },

    success(message: string, context?: Record<string, unknown>): void {
      if (jsonOutput) {
        structuredLogger.info(message, { ...context, operation: 'cli', status: 'success' });
      } else {
        getOutputFn()(message);
        if (context && Object.keys(context).length > 0) {
          structuredLogger.debug(message, context);
        }
      }
    },

    warn(message: string, context?: Record<string, unknown>): void {
      if (jsonOutput) {
        structuredLogger.warn(message, { ...context, operation: 'cli' });
      } else {
        getErrorFn()(`Warning: ${message}`);
        if (context && Object.keys(context).length > 0) {
          structuredLogger.warn(message, context);
        }
      }
    },

    error(message: string, error?: Error, context?: Record<string, unknown>): void {
      if (jsonOutput) {
        structuredLogger.error(message, error, { ...context, operation: 'cli' });
      } else {
        getErrorFn()(`Error: ${message}`);
        if (error || (context && Object.keys(context).length > 0)) {
          structuredLogger.error(message, error, context);
        }
      }
    },

    debug(message: string, context?: Record<string, unknown>): void {
      structuredLogger.debug(message, { ...context, operation: 'cli' });
    },

    getStructuredLogger(): StructuredLogger {
      return structuredLogger;
    },
  };
}

// =============================================================================
// DEFAULT INSTANCE
// =============================================================================

/**
 * Default CLI logger instance
 */
let defaultLogger: CLILogger | null = null;

/**
 * Get or create the default CLI logger
 */
export function getDefaultCLILogger(): CLILogger {
  if (!defaultLogger) {
    defaultLogger = createCLILogger();
  }
  return defaultLogger;
}

/**
 * Set the default CLI logger (useful for testing)
 */
export function setDefaultCLILogger(logger: CLILogger): void {
  defaultLogger = logger;
}

/**
 * Reset the default CLI logger
 */
export function resetDefaultCLILogger(): void {
  defaultLogger = null;
}
