/**
 * Shared logging utilities for consistent CLI output.
 */

/**
 * Log levels for filtering output.
 */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';

/**
 * Logger configuration options.
 */
export interface LoggerOptions {
  /**
   * Minimum log level to output.
   * @default 'info'
   */
  level?: LogLevel;

  /**
   * Custom output function for info/debug messages.
   * @default console.log
   */
  stdout?: (...args: unknown[]) => void;

  /**
   * Custom output function for error/warning messages.
   * @default console.error
   */
  stderr?: (...args: unknown[]) => void;
}

const LOG_LEVELS: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
  silent: 4,
};

/**
 * Logger instance for CLI output.
 */
export class Logger {
  private level: LogLevel;
  private stdout: (...args: unknown[]) => void;
  private stderr: (...args: unknown[]) => void;

  constructor(options: LoggerOptions = {}) {
    this.level = options.level ?? 'info';
    this.stdout = options.stdout ?? console.log;
    this.stderr = options.stderr ?? console.error;
  }

  /**
   * Checks if a log level should be output.
   */
  private shouldLog(level: LogLevel): boolean {
    return LOG_LEVELS[level] >= LOG_LEVELS[this.level];
  }

  /**
   * Logs a debug message.
   */
  debug(message: string): void {
    if (this.shouldLog('debug')) {
      this.stdout(`[DEBUG] ${message}`);
    }
  }

  /**
   * Logs an info message.
   */
  info(message: string): void {
    if (this.shouldLog('info')) {
      this.stdout(message);
    }
  }

  /**
   * Logs a warning message.
   */
  warn(message: string): void {
    if (this.shouldLog('warn')) {
      this.stderr(`Warning: ${message}`);
    }
  }

  /**
   * Logs an error message.
   */
  error(message: string): void {
    if (this.shouldLog('error')) {
      this.stderr(`Error: ${message}`);
    }
  }

  /**
   * Logs a success message with checkmark.
   */
  success(message: string): void {
    if (this.shouldLog('info')) {
      this.stdout(message);
    }
  }

  /**
   * Logs a list of items with bullet points.
   */
  list(items: string[], indent = 2): void {
    if (this.shouldLog('info')) {
      const prefix = ' '.repeat(indent) + '- ';
      items.forEach(item => this.stdout(`${prefix}${item}`));
    }
  }

  /**
   * Logs a section header.
   */
  section(title: string): void {
    if (this.shouldLog('info')) {
      this.stdout(`\n${title}:`);
    }
  }

  /**
   * Logs an empty line.
   */
  newline(): void {
    if (this.shouldLog('info')) {
      this.stdout('');
    }
  }
}

/**
 * Default logger instance.
 */
export const logger = new Logger();

/**
 * Creates a new logger instance with custom options.
 *
 * @param options - Logger configuration
 * @returns New Logger instance
 */
export function createLogger(options: LoggerOptions = {}): Logger {
  return new Logger(options);
}

// Convenience functions using the default logger

/**
 * Logs an info message.
 */
export function logInfo(message: string): void {
  logger.info(message);
}

/**
 * Logs an error message.
 */
export function logError(message: string): void {
  logger.error(message);
}

/**
 * Logs a success message.
 */
export function logSuccess(message: string): void {
  logger.success(message);
}

/**
 * Logs a list of items.
 */
export function logList(items: string[], indent = 2): void {
  logger.list(items, indent);
}

/**
 * Logs a section header.
 */
export function logSection(title: string): void {
  logger.section(title);
}
