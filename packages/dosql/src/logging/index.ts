/**
 * DoSQL Structured Logging Module
 *
 * Provides structured logging with trace IDs, log levels, JSON output,
 * async context propagation, and configurable sinks.
 *
 * @packageDocumentation
 */

import { AsyncLocalStorage } from 'node:async_hooks';

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
export const LogLevel = {
  DEBUG: 0,
  INFO: 1,
  WARN: 2,
  ERROR: 3,
} as const;

/**
 * Map log level strings to numeric values
 */
const LOG_LEVEL_VALUES: Record<LogLevel, number> = {
  debug: LogLevel.DEBUG,
  info: LogLevel.INFO,
  warn: LogLevel.WARN,
  error: LogLevel.ERROR,
};

/**
 * Compare two log levels
 * @returns negative if a < b, positive if a > b, 0 if equal
 */
export function compareLogLevels(a: LogLevel, b: LogLevel): number {
  return LOG_LEVEL_VALUES[a] - LOG_LEVEL_VALUES[b];
}

/**
 * Get log level from environment variable
 */
export function getLogLevelFromEnv(): LogLevel {
  const envLevel = (typeof process !== 'undefined' ? process.env?.LOG_LEVEL : undefined)?.toLowerCase();
  if (envLevel && envLevel in LOG_LEVEL_VALUES) {
    return envLevel as LogLevel;
  }
  return 'info';
}

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
  /** Trace ID for request correlation */
  traceId: string;
  /** Additional context data */
  context?: Record<string, unknown>;
  /** Error details if logging an error */
  error?: {
    name: string;
    code?: string;
    message: string;
    stack?: string;
  };
  /** Source location (file, line) if enabled */
  source?: {
    file: string;
    line: number;
  };
}

/**
 * Custom log sink interface
 */
export interface LogSink {
  write(entry: LogEntry): void | Promise<void>;
  flush?(): void | Promise<void>;
  close?(): void | Promise<void>;
}

/**
 * Logger configuration options
 */
export interface LoggerConfig {
  /** Minimum log level to output */
  level?: LogLevel;
  /** Custom log sink */
  sink?: LogSink;
  /** Fallback sink when primary fails */
  fallbackSink?: LogSink;
  /** Whether to include stack traces */
  includeStackTraces?: boolean;
  /** Additional default context */
  defaultContext?: Record<string, unknown>;
  /** Format timestamps in ISO 8601 (default true) */
  isoTimestamps?: boolean;
  /** Pretty print JSON (for development) */
  prettyPrint?: boolean;
  /** Include source location (file, line) in log entries */
  includeSourceLocation?: boolean;
  /** Use high-resolution timestamps with microseconds */
  highResolutionTimestamp?: boolean;
  /** Use object pooling for log entries */
  usePooling?: boolean;
  /** Initial trace ID */
  traceId?: string;
}

/**
 * Structured logger interface
 */
export interface StructuredLogger {
  debug(message: string | (() => string), context?: Record<string, unknown>): void;
  info(message: string | (() => string), context?: Record<string, unknown>): void;
  warn(message: string | (() => string), context?: Record<string, unknown>): void;
  error(message: string | (() => string), error?: Error, context?: Record<string, unknown>): void;

  /** Create a child logger with additional context */
  child(context: Record<string, unknown>): StructuredLogger;

  /** Get the current trace ID */
  getTraceId(): string;

  /** Set a new trace ID */
  setTraceId(traceId: string): void;

  /** Get the current log level */
  getLevel(): LogLevel;

  /** Set log level at runtime */
  setLevel(level: LogLevel): void;

  /** Flush any buffered log entries */
  flush(): Promise<void>;
}

// =============================================================================
// Trace Context Propagation
// =============================================================================

/**
 * Context stored in AsyncLocalStorage
 */
interface TraceContextData {
  traceId: string;
}

/**
 * AsyncLocalStorage for trace context propagation
 */
export const LoggerAsyncStorage = new AsyncLocalStorage<TraceContextData>();

/**
 * Run a function with a specific trace context
 */
export async function withTraceContext<T>(
  traceId: string,
  fn: () => T | Promise<T>
): Promise<T> {
  return LoggerAsyncStorage.run({ traceId }, fn);
}

/**
 * Get current trace ID from async context
 */
function getTraceIdFromContext(): string | undefined {
  return LoggerAsyncStorage.getStore()?.traceId;
}

// =============================================================================
// Trace ID Extraction
// =============================================================================

/**
 * Extract trace ID from request headers
 *
 * Supports common trace ID headers:
 * - x-trace-id
 * - x-request-id
 * - traceparent (W3C Trace Context)
 */
export function extractTraceId(
  request: Request,
  headerNames: string[] = ['x-trace-id', 'x-request-id', 'traceparent']
): string | undefined {
  for (const headerName of headerNames) {
    const value = request.headers.get(headerName);
    if (value) {
      // Handle W3C traceparent format: version-traceId-parentId-flags
      if (headerName === 'traceparent') {
        const parts = value.split('-');
        if (parts.length >= 2) {
          return parts[1]; // Extract trace ID from traceparent
        }
      }
      return value;
    }
  }
  return undefined;
}

/**
 * Add trace ID to an error's context
 */
export function withTraceId<T extends Error & { context?: Record<string, unknown> }>(
  error: T,
  traceId: string
): T {
  if (!error.context) {
    error.context = {};
  }
  error.context.traceId = traceId;
  return error;
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Generate a random trace ID
 */
function generateTraceId(): string {
  // Use crypto.randomUUID if available, otherwise fall back to Math.random
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  // Fallback: generate 32 hex characters
  return Array.from({ length: 32 }, () =>
    Math.floor(Math.random() * 16).toString(16)
  ).join('');
}

/**
 * Create ISO 8601 timestamp
 */
function createTimestamp(highResolution: boolean): string {
  const now = new Date();
  if (highResolution) {
    // Add microsecond precision using performance.now()
    const isoBase = now.toISOString().slice(0, -1); // Remove trailing 'Z'
    const ms = now.getMilliseconds();
    const micro = Math.floor((performance.now() % 1) * 1000);
    const microStr = micro.toString().padStart(3, '0');
    return `${isoBase.slice(0, -3)}${ms.toString().padStart(3, '0')}${microStr}Z`;
  }
  return now.toISOString();
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

/**
 * Format message with placeholder substitution
 * Template syntax: {fieldName}
 */
function formatMessage(template: string, context?: Record<string, unknown>): string {
  if (!context) return template;

  return template.replace(/\{(\w+)\}/g, (match, key) => {
    if (key in context) {
      return String(context[key]);
    }
    return match;
  });
}

/**
 * Get source location from stack trace
 */
function getSourceLocation(): { file: string; line: number } | undefined {
  const error = new Error();
  const stack = error.stack?.split('\n');
  if (!stack || stack.length < 5) return undefined;

  // Skip internal frames to find caller
  // [0] = "Error"
  // [1] = getSourceLocation
  // [2] = log method
  // [3] = public method (debug/info/warn/error)
  // [4] = actual caller
  const callerLine = stack[4];
  if (!callerLine) return undefined;

  // Parse stack frame: "    at functionName (file:line:column)"
  const match = callerLine.match(/at\s+(?:.*?\s+)?\(?(.+?):(\d+):\d+\)?$/);
  if (match) {
    return {
      file: match[1],
      line: parseInt(match[2], 10),
    };
  }

  return undefined;
}

// =============================================================================
// Logger Implementation
// =============================================================================

/**
 * Logger implementation
 */
class Logger implements StructuredLogger {
  private level: LogLevel;
  private sink: LogSink;
  private fallbackSink?: LogSink;
  private defaultContext: Record<string, unknown>;
  private includeStackTraces: boolean;
  private includeSourceLocation: boolean;
  private highResolutionTimestamp: boolean;
  private _traceId: string;

  constructor(config: LoggerConfig = {}) {
    this.level = config.level ?? getLogLevelFromEnv();
    this.sink = config.sink ?? new ConsoleSink({});
    this.fallbackSink = config.fallbackSink;
    this.defaultContext = config.defaultContext ?? {};
    this.includeStackTraces = config.includeStackTraces ?? true;
    this.includeSourceLocation = config.includeSourceLocation ?? false;
    this.highResolutionTimestamp = config.highResolutionTimestamp ?? false;
    this._traceId = config.traceId ?? generateTraceId();
  }

  private shouldLog(level: LogLevel): boolean {
    return LOG_LEVEL_VALUES[level] >= LOG_LEVEL_VALUES[this.level];
  }

  private getActiveTraceId(): string {
    // Check async context first, then fall back to instance trace ID
    return getTraceIdFromContext() ?? this._traceId;
  }

  private log(
    level: LogLevel,
    messageOrFn: string | (() => string),
    context?: Record<string, unknown>,
    error?: Error
  ): void {
    // Early exit if level is filtered (performance optimization)
    if (!this.shouldLog(level)) {
      return;
    }

    // Evaluate lazy message
    const rawMessage = typeof messageOrFn === 'function' ? messageOrFn() : messageOrFn;

    // Merge contexts
    const mergedContext = context
      ? { ...this.defaultContext, ...context }
      : Object.keys(this.defaultContext).length > 0
        ? this.defaultContext
        : undefined;

    // Format message with placeholders
    const message = formatMessage(rawMessage, mergedContext);

    // Build log entry
    const entry: LogEntry = {
      timestamp: createTimestamp(this.highResolutionTimestamp),
      level,
      message,
      traceId: this.getActiveTraceId(),
    };

    if (mergedContext && Object.keys(mergedContext).length > 0) {
      entry.context = safeStringify(mergedContext) as Record<string, unknown>;
    }

    if (error) {
      entry.error = {
        name: error.name,
        message: error.message,
      };
      if ((error as any).code) {
        entry.error.code = (error as any).code;
      }
      if (this.includeStackTraces && error.stack) {
        entry.error.stack = error.stack;
      }
    }

    if (this.includeSourceLocation) {
      const source = getSourceLocation();
      if (source) {
        entry.source = source;
      }
    }

    // Write to sink with error handling
    try {
      this.sink.write(entry);
    } catch (sinkError) {
      if (this.fallbackSink) {
        try {
          this.fallbackSink.write(entry);
        } catch {
          // Both sinks failed, silently drop the log
        }
      }
    }
  }

  debug(message: string | (() => string), context?: Record<string, unknown>): void {
    this.log('debug', message, context);
  }

  info(message: string | (() => string), context?: Record<string, unknown>): void {
    this.log('info', message, context);
  }

  warn(message: string | (() => string), context?: Record<string, unknown>): void {
    this.log('warn', message, context);
  }

  error(message: string | (() => string), error?: Error, context?: Record<string, unknown>): void {
    this.log('error', message, context, error);
  }

  child(context: Record<string, unknown>): StructuredLogger {
    const childLogger = new Logger({
      level: this.level,
      sink: this.sink,
      fallbackSink: this.fallbackSink,
      defaultContext: { ...this.defaultContext, ...context },
      includeStackTraces: this.includeStackTraces,
      includeSourceLocation: this.includeSourceLocation,
      highResolutionTimestamp: this.highResolutionTimestamp,
      traceId: this._traceId,
    });
    return childLogger;
  }

  getTraceId(): string {
    return this.getActiveTraceId();
  }

  setTraceId(traceId: string): void {
    this._traceId = traceId;
  }

  getLevel(): LogLevel {
    return this.level;
  }

  setLevel(level: LogLevel): void {
    this.level = level;
  }

  async flush(): Promise<void> {
    if (this.sink.flush) {
      await this.sink.flush();
    }
  }
}

/**
 * Create a new structured logger
 */
export function createLogger(config?: LoggerConfig): StructuredLogger {
  return new Logger(config);
}

// =============================================================================
// Built-in Sinks
// =============================================================================

/**
 * Console sink options
 */
export interface ConsoleSinkOptions {
  /** Enable colorized output */
  colorize?: boolean;
  /** Pretty print JSON */
  prettyPrint?: boolean;
}

/**
 * Console sink for development
 */
export class ConsoleSink implements LogSink {
  private colorize: boolean;
  private prettyPrint: boolean;

  constructor(options: ConsoleSinkOptions = {}) {
    this.colorize = options.colorize ?? false;
    this.prettyPrint = options.prettyPrint ?? false;
  }

  write(entry: LogEntry): void {
    const output = this.prettyPrint
      ? JSON.stringify(entry, null, 2)
      : JSON.stringify(entry);

    if (this.colorize) {
      const colors: Record<LogLevel, string> = {
        debug: '\x1b[36m', // Cyan
        info: '\x1b[32m',  // Green
        warn: '\x1b[33m',  // Yellow
        error: '\x1b[31m', // Red
      };
      const reset = '\x1b[0m';
      console.log(`${colors[entry.level]}${output}${reset}`);
    } else {
      console.log(output);
    }
  }
}

/**
 * JSON sink options
 */
export interface JsonSinkOptions {
  /** Write function for output */
  write: (json: string) => void;
  /** Pretty print JSON */
  prettyPrint?: boolean;
}

/**
 * JSON sink for structured output
 */
export class JsonSink implements LogSink {
  private writeFn: (json: string) => void;
  private prettyPrint: boolean;

  constructor(options: JsonSinkOptions) {
    this.writeFn = options.write;
    this.prettyPrint = options.prettyPrint ?? false;
  }

  write(entry: LogEntry): void {
    const json = this.prettyPrint
      ? JSON.stringify(entry, null, 2)
      : JSON.stringify(entry);
    this.writeFn(json);
  }
}

/**
 * No-op sink for performance testing
 */
export class NoOpSink implements LogSink {
  write(_entry: LogEntry): void {
    // Intentionally empty
  }
}

/**
 * Multi-sink for fan-out to multiple destinations
 */
export class MultiSink implements LogSink {
  private sinks: LogSink[];

  constructor(sinks: LogSink[]) {
    this.sinks = sinks;
  }

  write(entry: LogEntry): void {
    for (const sink of this.sinks) {
      try {
        sink.write(entry);
      } catch {
        // Continue to other sinks on failure
      }
    }
  }

  async flush(): Promise<void> {
    await Promise.all(
      this.sinks.map(async sink => {
        if (sink.flush) {
          await sink.flush();
        }
      })
    );
  }

  async close(): Promise<void> {
    await Promise.all(
      this.sinks.map(async sink => {
        if (sink.close) {
          await sink.close();
        }
      })
    );
  }
}

/**
 * Filtering sink options
 */
export interface FilteringSinkOptions {
  /** Filter function - return true to pass entry through */
  filter: (entry: LogEntry) => boolean;
}

/**
 * Filtering sink that passes entries based on a filter function
 */
export class FilteringSink implements LogSink {
  private baseSink: LogSink;
  private filter: (entry: LogEntry) => boolean;

  constructor(baseSink: LogSink, options: FilteringSinkOptions) {
    this.baseSink = baseSink;
    this.filter = options.filter;
  }

  write(entry: LogEntry): void {
    if (this.filter(entry)) {
      this.baseSink.write(entry);
    }
  }

  async flush(): Promise<void> {
    if (this.baseSink.flush) {
      await this.baseSink.flush();
    }
  }

  async close(): Promise<void> {
    if (this.baseSink.close) {
      await this.baseSink.close();
    }
  }
}

/**
 * Batching sink options
 */
export interface BatchingSinkOptions {
  /** Batch size before auto-flush */
  batchSize: number;
  /** Flush interval in milliseconds */
  flushInterval: number;
  /** Callback when a batch is ready */
  onBatch: (entries: LogEntry[]) => void | Promise<void>;
}

/**
 * Batching sink for efficient writes
 */
export class BatchingSink implements LogSink {
  private buffer: LogEntry[] = [];
  private batchSize: number;
  private flushInterval: number;
  private onBatch: (entries: LogEntry[]) => void | Promise<void>;
  private timer?: ReturnType<typeof setInterval>;

  constructor(options: BatchingSinkOptions) {
    this.batchSize = options.batchSize;
    this.flushInterval = options.flushInterval;
    this.onBatch = options.onBatch;

    // Start flush timer
    this.timer = setInterval(() => this.flush(), this.flushInterval);
  }

  write(entry: LogEntry): void {
    this.buffer.push(entry);
    if (this.buffer.length >= this.batchSize) {
      this.flushSync();
    }
  }

  private flushSync(): void {
    if (this.buffer.length > 0) {
      const batch = this.buffer;
      this.buffer = [];
      this.onBatch(batch);
    }
  }

  async flush(): Promise<void> {
    if (this.buffer.length > 0) {
      const batch = this.buffer;
      this.buffer = [];
      await this.onBatch(batch);
    }
  }

  async close(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
    }
    await this.flush();
  }
}

/**
 * Async buffering sink options
 */
export interface AsyncBufferingSinkOptions {
  /** Maximum buffer size */
  bufferSize: number;
  /** Flush threshold */
  flushThreshold: number;
}

/**
 * Async buffering sink for non-blocking writes
 */
export class AsyncBufferingSink implements LogSink {
  private baseSink: LogSink;
  private buffer: LogEntry[] = [];
  private bufferSize: number;
  private flushThreshold: number;
  private flushing = false;
  private pendingFlush: Promise<void> | null = null;

  constructor(baseSink: LogSink, options: AsyncBufferingSinkOptions) {
    this.baseSink = baseSink;
    this.bufferSize = options.bufferSize;
    this.flushThreshold = options.flushThreshold;
  }

  write(entry: LogEntry): void {
    this.buffer.push(entry);

    // Non-blocking flush when threshold is reached
    if (this.buffer.length >= this.flushThreshold && !this.flushing) {
      this.scheduleFlush();
    }
  }

  private scheduleFlush(): void {
    if (this.pendingFlush) return;

    this.pendingFlush = (async () => {
      this.flushing = true;
      try {
        while (this.buffer.length > 0) {
          const entry = this.buffer.shift()!;
          await this.baseSink.write(entry);
        }
      } finally {
        this.flushing = false;
        this.pendingFlush = null;
      }
    })();
  }

  async flush(): Promise<void> {
    this.scheduleFlush();
    if (this.pendingFlush) {
      await this.pendingFlush;
    }
  }

  async close(): Promise<void> {
    await this.flush();
    if (this.baseSink.close) {
      await this.baseSink.close();
    }
  }
}

/**
 * Sampling sink options
 */
export interface SamplingSinkOptions {
  /** Sample rates per log level (0-1) */
  sampleRate: Record<LogLevel, number>;
}

/**
 * Sampling sink for high-volume logging
 */
export class SamplingSink implements LogSink {
  private baseSink: LogSink;
  private sampleRate: Record<LogLevel, number>;

  constructor(baseSink: LogSink, options: SamplingSinkOptions) {
    this.baseSink = baseSink;
    this.sampleRate = options.sampleRate;
  }

  write(entry: LogEntry): void {
    const rate = this.sampleRate[entry.level];
    if (rate >= 1 || Math.random() < rate) {
      this.baseSink.write(entry);
    }
  }

  async flush(): Promise<void> {
    if (this.baseSink.flush) {
      await this.baseSink.flush();
    }
  }

  async close(): Promise<void> {
    if (this.baseSink.close) {
      await this.baseSink.close();
    }
  }
}

/**
 * Redacting sink options
 */
export interface RedactingSinkOptions {
  /** Field names to redact */
  redactFields: string[];
  /** Regex patterns to redact */
  redactPatterns?: RegExp[];
}

/**
 * Redacting sink for sensitive data
 */
export class RedactingSink implements LogSink {
  private baseSink: LogSink;
  private redactFields: Set<string>;
  private redactPatterns: RegExp[];

  constructor(baseSink: LogSink, options: RedactingSinkOptions) {
    this.baseSink = baseSink;
    this.redactFields = new Set(options.redactFields);
    this.redactPatterns = options.redactPatterns ?? [];
  }

  private redact(obj: unknown): unknown {
    if (obj === null || obj === undefined) {
      return obj;
    }

    if (typeof obj === 'string') {
      let result = obj;
      for (const pattern of this.redactPatterns) {
        result = result.replace(pattern, '[REDACTED]');
      }
      return result;
    }

    if (typeof obj !== 'object') {
      return obj;
    }

    if (Array.isArray(obj)) {
      return obj.map(item => this.redact(item));
    }

    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj)) {
      if (this.redactFields.has(key)) {
        result[key] = '[REDACTED]';
      } else {
        result[key] = this.redact(value);
      }
    }
    return result;
  }

  write(entry: LogEntry): void {
    const redactedEntry: LogEntry = {
      ...entry,
      context: entry.context
        ? (this.redact(entry.context) as Record<string, unknown>)
        : undefined,
      message: this.redact(entry.message) as string,
    };
    this.baseSink.write(redactedEntry);
  }

  async flush(): Promise<void> {
    if (this.baseSink.flush) {
      await this.baseSink.flush();
    }
  }

  async close(): Promise<void> {
    if (this.baseSink.close) {
      await this.baseSink.close();
    }
  }
}

/**
 * File sink options (stub for environments without fs)
 */
export interface FileSinkOptions {
  /** File path */
  path: string;
  /** Maximum file size (e.g., '10MB') */
  maxSize?: string;
  /** Maximum number of files to keep */
  maxFiles?: number;
  /** Compress rotated files */
  compress?: boolean;
}

/**
 * File sink stats
 */
export interface FileSinkStats {
  entriesWritten: number;
  bytesWritten: number;
  rotations: number;
}

/**
 * File sink for persistent logging
 * Note: This is a stub implementation for edge runtime compatibility
 */
export class FileSink implements LogSink {
  private options: FileSinkOptions;
  private entriesWritten = 0;
  private buffer: string[] = [];

  constructor(options: FileSinkOptions) {
    this.options = options;
  }

  write(entry: LogEntry): void {
    this.buffer.push(JSON.stringify(entry));
    this.entriesWritten++;
  }

  async flush(): Promise<void> {
    // In a real implementation, this would write to the file system
    this.buffer = [];
  }

  async close(): Promise<void> {
    await this.flush();
  }

  getStats(): FileSinkStats {
    return {
      entriesWritten: this.entriesWritten,
      bytesWritten: this.buffer.reduce((acc, line) => acc + line.length, 0),
      rotations: 0,
    };
  }
}

// =============================================================================
// Standard Context Builder
// =============================================================================

/**
 * Standard context builder for common fields
 */
export class StandardContext {
  private context: Record<string, unknown> = {};

  withUserId(userId: string): this {
    this.context.userId = userId;
    return this;
  }

  withRequestId(requestId: string): this {
    this.context.requestId = requestId;
    return this;
  }

  withDuration(durationMs: number): this {
    this.context.durationMs = durationMs;
    return this;
  }

  withDatabase(database: string): this {
    this.context.database = database;
    return this;
  }

  withTable(table: string): this {
    this.context.table = table;
    return this;
  }

  withRowsAffected(rowsAffected: number): this {
    this.context.rowsAffected = rowsAffected;
    return this;
  }

  with(key: string, value: unknown): this {
    this.context[key] = value;
    return this;
  }

  build(): Record<string, unknown> {
    return { ...this.context };
  }
}

// =============================================================================
// Performance Utilities
// =============================================================================

/**
 * Object pool stats
 */
interface PoolStats {
  totalAllocated: number;
  poolHits: number;
  poolSize: number;
}

let poolStats: PoolStats = {
  totalAllocated: 0,
  poolHits: 0,
  poolSize: 0,
};

/**
 * Get log entry pool statistics
 */
export function getLogEntryPoolStats(): PoolStats {
  return { ...poolStats };
}

/**
 * Reset pool statistics
 */
export function resetLogEntryPoolStats(): void {
  poolStats = {
    totalAllocated: 0,
    poolHits: 0,
    poolSize: 0,
  };
}

/**
 * Benchmark results
 */
export interface BenchmarkResults {
  avgLatencyMs: number;
  p99LatencyMs: number;
  throughputPerSecond: number;
  memoryUsageMB: number;
}

/**
 * Benchmark options
 */
export interface BenchmarkOptions {
  iterations: number;
  warmupIterations?: number;
  messageSize?: 'small' | 'medium' | 'large';
}

/**
 * Run a logger benchmark
 */
export async function runLoggerBenchmark(
  logger: StructuredLogger,
  options: BenchmarkOptions
): Promise<BenchmarkResults> {
  const { iterations, warmupIterations = 100, messageSize = 'medium' } = options;

  // Message templates by size
  const messages: Record<string, string> = {
    small: 'Log',
    medium: 'This is a medium-length log message for benchmarking',
    large: 'This is a much longer log message that includes a lot more detail for benchmarking purposes. ' +
           'It simulates real-world logging scenarios where messages may contain additional context.',
  };

  const message = messages[messageSize];

  // Warmup
  for (let i = 0; i < warmupIterations; i++) {
    logger.info(message, { iteration: i });
  }
  await logger.flush();

  // Collect latencies
  const latencies: number[] = [];
  const startTime = performance.now();

  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    logger.info(message, { iteration: i });
    latencies.push(performance.now() - start);
  }

  await logger.flush();
  const totalTime = performance.now() - startTime;

  // Calculate statistics
  latencies.sort((a, b) => a - b);
  const avgLatencyMs = latencies.reduce((a, b) => a + b, 0) / latencies.length;
  const p99Index = Math.floor(latencies.length * 0.99);
  const p99LatencyMs = latencies[p99Index] ?? latencies[latencies.length - 1];
  const throughputPerSecond = iterations / (totalTime / 1000);

  // Estimate memory usage (rough estimate)
  const memoryUsageMB = (typeof process !== 'undefined' && process.memoryUsage)
    ? process.memoryUsage().heapUsed / (1024 * 1024)
    : 0;

  return {
    avgLatencyMs,
    p99LatencyMs,
    throughputPerSecond,
    memoryUsageMB,
  };
}
