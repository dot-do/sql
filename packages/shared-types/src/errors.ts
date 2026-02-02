/**
 * Unified Error Hierarchy for DoSQL Ecosystem
 *
 * This module provides the base error classes and utilities that are shared
 * across all DoSQL packages (dosql, dolake, sql.do, lake.do).
 *
 * All errors extend BaseError which provides:
 * - Required error codes
 * - Timestamps
 * - Context preservation
 * - Serialization/deserialization for RPC
 * - Recovery hints
 * - Structured logging support
 * - Retryability indication
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.4.0
 */

// =============================================================================
// Error Categories
// =============================================================================

/**
 * High-level error categories for consistent handling at API layer.
 *
 * Used to map specific error codes to general handling strategies:
 * - CONNECTION: Retry with backoff, check network
 * - EXECUTION: May be retryable depending on specific error
 * - VALIDATION: Not retryable, fix input
 * - RESOURCE: Check resource exists/permissions
 * - CONFLICT: Retry with backoff (deadlock, serialization)
 * - TIMEOUT: Retry with increased timeout
 * - INTERNAL: Report bug, not retryable
 *
 * @public
 * @stability stable
 */
export enum ErrorCategory {
  /** Connection/networking errors - typically retryable */
  CONNECTION = 'CONNECTION',
  /** Query execution errors - may be retryable */
  EXECUTION = 'EXECUTION',
  /** Input validation errors - not retryable */
  VALIDATION = 'VALIDATION',
  /** Resource errors (not found, quota exceeded) */
  RESOURCE = 'RESOURCE',
  /** Conflict errors (deadlock, serialization) - retryable with backoff */
  CONFLICT = 'CONFLICT',
  /** Timeout errors - retryable with increased timeout */
  TIMEOUT = 'TIMEOUT',
  /** Internal errors (bugs, unexpected states) - not retryable */
  INTERNAL = 'INTERNAL',
}

// =============================================================================
// Error Context
// =============================================================================

/**
 * Context that can be attached to any error.
 *
 * Provides structured information for debugging and logging without
 * exposing internal implementation details in error messages.
 *
 * @public
 * @stability stable
 */
export interface ErrorContext {
  /** Request ID for distributed tracing */
  requestId?: string | undefined;
  /** Transaction ID if within a transaction */
  transactionId?: string | undefined;
  /** SQL statement that caused the error */
  sql?: string | undefined;
  /** Table name involved */
  table?: string | undefined;
  /** Column name involved */
  column?: string | undefined;
  /** Additional metadata specific to the error type */
  metadata?: Record<string, unknown> | undefined;
}

/**
 * Serialized error format for RPC/API responses.
 *
 * This format is used when errors cross process boundaries (e.g., client-server).
 * All fields are JSON-serializable.
 *
 * @public
 * @stability stable
 */
export interface SerializedError {
  /** Error class name (e.g., 'SQLError', 'ConnectionError') */
  name: string;
  /** Machine-readable error code (e.g., 'DB_CLOSED', 'TIMEOUT') */
  code: string;
  /** Human-readable error message */
  message: string;
  /** Timestamp when error occurred (Unix milliseconds) */
  timestamp: number;
  /** Error context */
  context?: ErrorContext | undefined;
  /** Stack trace (optional, may be omitted in production) */
  stack?: string | undefined;
  /** Serialized cause error (for error chains) */
  cause?: SerializedError | undefined;
}

/**
 * Log entry format for structured logging.
 *
 * Compatible with JSON logging systems like Pino, Winston, etc.
 *
 * @public
 * @stability stable
 */
export interface ErrorLogEntry {
  /** Log level */
  level: 'error' | 'warn';
  /** ISO 8601 timestamp */
  timestamp: string;
  /** Error details */
  error: {
    name: string;
    code: string;
    message: string;
    stack?: string | undefined;
  };
  /** Additional metadata for structured logging */
  metadata: Record<string, unknown>;
}

// =============================================================================
// Base Error Class
// =============================================================================

/**
 * Base error class for all DoSQL ecosystem errors.
 *
 * Provides a consistent interface across all packages:
 * - Required error code for programmatic handling
 * - Category for high-level error handling strategies
 * - Timestamp for debugging and logging
 * - Context preservation for structured logging
 * - Serialization for RPC boundaries
 * - Retryability indication
 * - User-friendly messages
 *
 * @example
 * ```typescript
 * try {
 *   await client.query('SELECT * FROM users');
 * } catch (error) {
 *   if (error instanceof BaseError) {
 *     console.log(error.code);           // 'TIMEOUT'
 *     console.log(error.isRetryable());  // true
 *     console.log(error.toUserMessage()); // 'The operation timed out. Please try again.'
 *
 *     // For RPC responses
 *     const json = error.toJSON();
 *
 *     // For structured logging
 *     logger.log(error.toLogEntry());
 *   }
 * }
 * ```
 *
 * @public
 * @stability stable
 */
export abstract class BaseError extends Error {
  /** Machine-readable error code */
  abstract readonly code: string;

  /** Error category for consistent handling */
  abstract readonly category: ErrorCategory;

  /** Timestamp when error occurred (Unix milliseconds) */
  readonly timestamp: number;

  /** Error context */
  context?: ErrorContext;

  /** Recovery hint for developers */
  recoveryHint?: string;

  /** Error cause for error chaining */
  declare readonly cause?: Error;

  constructor(message: string, options?: { cause?: Error; context?: ErrorContext | undefined }) {
    super(message);
    this.timestamp = Date.now();
    if (options?.cause) {
      (this as { cause?: Error }).cause = options.cause;
    }
    if (options?.context) {
      this.context = options.context;
    }

    // Ensure proper prototype chain for instanceof checks
    Object.setPrototypeOf(this, new.target.prototype);
  }

  /**
   * Check if this error is retryable.
   *
   * Override in subclasses for specific retry logic.
   * Default is false - errors are not retryable unless explicitly marked.
   */
  isRetryable(): boolean {
    return false;
  }

  /**
   * Get a user-friendly error message.
   *
   * Override in subclasses for specific user messages.
   * Default returns the technical error message.
   */
  toUserMessage(): string {
    return this.message;
  }

  /**
   * Serialize error for RPC/API responses.
   *
   * Creates a JSON-serializable representation of the error
   * that can be sent across process boundaries.
   */
  toJSON(): SerializedError {
    const result: SerializedError = {
      name: this.name,
      code: this.code,
      message: this.message,
      timestamp: this.timestamp,
    };

    if (this.context) {
      result.context = this.context;
    }

    if (this.stack) {
      result.stack = this.stack;
    }

    if (this.cause instanceof BaseError) {
      result.cause = this.cause.toJSON();
    } else if (this.cause instanceof Error) {
      result.cause = {
        name: this.cause.name,
        code: 'UNKNOWN',
        message: this.cause.message,
        timestamp: this.timestamp,
        stack: this.cause.stack,
      };
    }

    return result;
  }

  /**
   * Format error for structured logging.
   *
   * Returns a log entry that includes all relevant error information
   * in a format suitable for JSON logging systems.
   */
  toLogEntry(): ErrorLogEntry {
    return {
      level: 'error',
      timestamp: new Date(this.timestamp).toISOString(),
      error: {
        name: this.name,
        code: this.code,
        message: this.message,
        stack: this.stack,
      },
      metadata: {
        category: this.category,
        recoveryHint: this.recoveryHint,
        ...this.context,
      },
    };
  }

  /**
   * Create error with additional context.
   *
   * Returns this for method chaining.
   */
  withContext(context: ErrorContext): this {
    this.context = { ...this.context, ...context };
    return this;
  }

  /**
   * Set recovery hint.
   *
   * Returns this for method chaining.
   */
  withRecoveryHint(hint: string): this {
    this.recoveryHint = hint;
    return this;
  }
}

// =============================================================================
// Generic Error
// =============================================================================

/**
 * Generic error for deserialized errors without registered classes.
 *
 * Used when deserializing an error from JSON and the specific error class
 * is not available (e.g., client receiving server-specific error type).
 *
 * @public
 * @stability stable
 */
export class GenericError extends BaseError {
  readonly code: string;
  readonly category: ErrorCategory;

  constructor(
    code: string,
    message: string,
    category: ErrorCategory = ErrorCategory.INTERNAL,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    super(message, options);
    this.name = 'GenericError';
    this.code = code;
    this.category = category;
  }
}

// =============================================================================
// Aggregate Error
// =============================================================================

/**
 * Aggregates multiple errors for batch operations.
 *
 * Use this when multiple operations can fail independently and you want
 * to report all failures together.
 *
 * @example
 * ```typescript
 * const errors: BaseError[] = [];
 * for (const item of items) {
 *   try {
 *     await process(item);
 *   } catch (error) {
 *     if (error instanceof BaseError) {
 *       errors.push(error);
 *     }
 *   }
 * }
 * if (errors.length > 0) {
 *   throw new AggregateError(errors);
 * }
 * ```
 *
 * @public
 * @stability stable
 */
export class AggregateError extends BaseError {
  readonly code = 'AGGREGATE_ERROR';
  readonly category = ErrorCategory.EXECUTION;

  /** Individual errors */
  readonly errors: BaseError[];

  constructor(errors: BaseError[], message?: string) {
    super(message ?? `${errors.length} errors occurred`);
    this.name = 'AggregateError';
    this.errors = errors;
  }

  override isRetryable(): boolean {
    // Only retryable if all individual errors are retryable
    return this.errors.every(e => e.isRetryable());
  }

  override toJSON(): SerializedError & { errors: SerializedError[] } {
    return {
      ...super.toJSON(),
      errors: this.errors.map(e => e.toJSON()),
    };
  }
}

// =============================================================================
// Error Registry and Deserialization
// =============================================================================

/** Registry of error constructors for deserialization */
const errorRegistry = new Map<string, ErrorDeserializer>();

/** Type for error deserializer function */
type ErrorDeserializer = (json: SerializedError) => BaseError;

/**
 * Register an error class for deserialization.
 *
 * Call this for each custom error class to enable deserializeError()
 * to recreate the correct error type from serialized JSON.
 *
 * @param name - The error class name (must match error.name)
 * @param deserializer - Function to create the error from JSON
 *
 * @example
 * ```typescript
 * registerErrorDeserializer('CustomError', (json) => {
 *   return new CustomError(json.code, json.message, { context: json.context });
 * });
 * ```
 *
 * @public
 * @stability stable
 */
export function registerErrorDeserializer(
  name: string,
  deserializer: ErrorDeserializer
): void {
  errorRegistry.set(name, deserializer);
}

/**
 * Deserialize an error from JSON.
 *
 * Attempts to recreate the original error type using registered deserializers.
 * Falls back to GenericError if the specific class is not registered.
 *
 * @param json - Serialized error object
 * @returns Deserialized error instance
 *
 * @public
 * @stability stable
 */
export function deserializeError(json: SerializedError): BaseError {
  const deserializer = errorRegistry.get(json.name);

  if (deserializer) {
    return deserializer(json);
  }

  // Fallback: create a GenericError
  const options = json.context ? { context: json.context } : undefined;
  return new GenericError(json.code, json.message, ErrorCategory.INTERNAL, options);
}

// =============================================================================
// Common Error Code Prefixes
// =============================================================================

/**
 * Standard error code prefixes for consistent naming across packages.
 *
 * Error codes should follow the pattern: PREFIX_SPECIFIC
 * Examples: DB_CLOSED, CONN_TIMEOUT, QUERY_SYNTAX
 *
 * @public
 * @stability stable
 */
export const ErrorCodePrefix = {
  /** Database-level errors */
  DATABASE: 'DB',
  /** Connection errors */
  CONNECTION: 'CONN',
  /** Query/Statement errors */
  QUERY: 'QUERY',
  /** Transaction errors */
  TRANSACTION: 'TX',
  /** Storage errors */
  STORAGE: 'STORAGE',
  /** Parser errors */
  PARSER: 'PARSER',
  /** Planner errors */
  PLANNER: 'PLANNER',
  /** Executor errors */
  EXECUTOR: 'EXECUTOR',
  /** Binding/parameter errors */
  BINDING: 'BIND',
  /** Syntax errors */
  SYNTAX: 'SYNTAX',
  /** Lake/CDC errors */
  LAKE: 'LAKE',
  /** Parquet errors */
  PARQUET: 'PARQUET',
  /** Iceberg errors */
  ICEBERG: 'ICEBERG',
  /** Buffer errors */
  BUFFER: 'BUFFER',
  /** Compaction errors */
  COMPACTION: 'COMPACT',
} as const;

/**
 * Type for error code prefix values.
 * @public
 */
export type ErrorCodePrefix = typeof ErrorCodePrefix[keyof typeof ErrorCodePrefix];

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Get error code category prefix from a full error code.
 *
 * @param code - Full error code (e.g., 'DB_CLOSED')
 * @returns Category prefix (e.g., 'DB')
 *
 * @public
 * @stability stable
 */
export function getErrorCodePrefix(code: string): string {
  const parts = code.split('_');
  return parts[0] ?? 'UNKNOWN';
}

/**
 * Check if a code belongs to a specific category.
 *
 * @param code - Full error code (e.g., 'DB_CLOSED')
 * @param prefix - Category prefix to check (e.g., 'DB')
 * @returns True if the code belongs to the category
 *
 * @public
 * @stability stable
 */
export function isErrorCodeInCategory(code: string, prefix: string): boolean {
  return code.startsWith(prefix + '_');
}

/**
 * Masks sensitive data in a URL for safe logging and error messages.
 *
 * Removes or masks:
 * - Password in userinfo (user:password@host)
 * - Query parameters that may contain tokens
 *
 * @param url - The URL to mask
 * @returns A masked version of the URL safe for logging
 *
 * @public
 * @stability stable
 */
export function maskUrl(url: string): string {
  try {
    const parsed = new URL(url);

    // Mask password in userinfo
    if (parsed.password) {
      parsed.password = '***';
    }

    // Mask sensitive query parameters
    const sensitiveParams = [
      'token', 'key', 'secret', 'password', 'auth',
      'api_key', 'apikey', 'access_token', 'bearer',
    ];
    for (const param of sensitiveParams) {
      if (parsed.searchParams.has(param)) {
        parsed.searchParams.set(param, '***');
      }
    }

    return parsed.toString();
  } catch {
    // If URL parsing fails, mask everything after :// except the host
    const match = url.match(/^(\w+:\/\/)([^/?#]+)/);
    if (match) {
      return `${match[1]}${match[2]}/***`;
    }
    // Fallback: return a generic masked version
    return '[invalid-url]';
  }
}

/**
 * Standard retryable error codes.
 *
 * These codes indicate transient failures that may succeed on retry.
 *
 * @public
 * @stability stable
 */
export const RETRYABLE_ERROR_CODES = [
  'TIMEOUT',
  'CONN_TIMEOUT',
  'CONN_CLOSED',
  'CONN_FAILED',
  'DB_TIMEOUT',
  'DB_CONNECTION_FAILED',
  'QUERY_TIMEOUT',
  'EXECUTOR_QUERY_TIMEOUT',
  'STORAGE_READ_FAILED',
  'STORAGE_WRITE_FAILED',
  'BUFFER_OVERFLOW',
  'LAKE_FLUSH_ERROR',
  'PARQUET_WRITE_ERROR',
  'NETWORK_ERROR',
  'UNAVAILABLE',
  'RESOURCE_EXHAUSTED',
] as const;

/**
 * Type for retryable error codes.
 * @public
 */
export type RetryableErrorCode = typeof RETRYABLE_ERROR_CODES[number];

/** Set of retryable error codes for O(1) lookup */
const RETRYABLE_ERROR_CODES_SET: ReadonlySet<string> = new Set(RETRYABLE_ERROR_CODES);

/**
 * Check if an error code is typically retryable.
 *
 * @param code - Error code to check
 * @returns True if the code indicates a retryable error
 *
 * @public
 * @stability stable
 */
export function isRetryableCode(code: string): boolean {
  return RETRYABLE_ERROR_CODES_SET.has(code);
}
