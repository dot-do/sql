/**
 * DoSQL Error Hierarchy
 *
 * Unified error handling across all DoSQL packages.
 * All errors extend DoSQLError which provides:
 * - Required error codes
 * - Timestamps
 * - Context preservation
 * - Serialization/deserialization for RPC
 * - Recovery hints
 * - Structured logging support
 *
 * @packageDocumentation
 */

// =============================================================================
// Error Context
// =============================================================================

/**
 * Context that can be attached to any error
 */
export interface ErrorContext {
  /** Request ID for tracing */
  requestId?: string;
  /** Transaction ID if within a transaction */
  transactionId?: string;
  /** SQL statement that caused the error */
  sql?: string;
  /** Table name involved */
  table?: string;
  /** Column name involved */
  column?: string;
  /** Additional metadata */
  metadata?: Record<string, unknown>;
}

/**
 * Serialized error format for RPC/API responses
 */
export interface SerializedError {
  /** Error class name */
  name: string;
  /** Machine-readable error code */
  code: string;
  /** Human-readable error message */
  message: string;
  /** Timestamp when error occurred */
  timestamp: number;
  /** Error context */
  context?: ErrorContext;
  /** Stack trace (optional, may be omitted in production) */
  stack?: string;
  /** Serialized cause error */
  cause?: SerializedError;
}

/**
 * Log entry format for structured logging
 */
export interface ErrorLogEntry {
  /** Log level */
  level: 'error' | 'warn';
  /** ISO timestamp */
  timestamp: string;
  /** Error details */
  error: {
    name: string;
    code: string;
    message: string;
    stack?: string;
  };
  /** Additional metadata */
  metadata: Record<string, unknown>;
}

// =============================================================================
// Error Categories
// =============================================================================

/**
 * High-level error categories for consistent handling at API layer
 */
export enum ErrorCategory {
  /** Connection/networking errors */
  CONNECTION = 'CONNECTION',
  /** Query execution errors */
  EXECUTION = 'EXECUTION',
  /** Input validation errors */
  VALIDATION = 'VALIDATION',
  /** Resource errors (not found, quota exceeded) */
  RESOURCE = 'RESOURCE',
  /** Conflict errors (deadlock, serialization) */
  CONFLICT = 'CONFLICT',
  /** Timeout errors */
  TIMEOUT = 'TIMEOUT',
  /** Internal errors (bugs, unexpected states) */
  INTERNAL = 'INTERNAL',
}

// =============================================================================
// Base DoSQL Error
// =============================================================================

/**
 * Base error class for all DoSQL errors
 *
 * Provides:
 * - Required error code
 * - Timestamp
 * - Context preservation
 * - Serialization/deserialization
 * - Recovery hints
 * - Retryability indication
 * - User-friendly messages
 * - Structured logging
 *
 * @example
 * ```typescript
 * try {
 *   db.exec('SELECT * FROM users');
 * } catch (error) {
 *   if (error instanceof DoSQLError) {
 *     console.log(error.code);           // 'DB_CLOSED'
 *     console.log(error.isRetryable());  // false
 *     console.log(error.toUserMessage()); // User-friendly message
 *
 *     // For RPC responses
 *     const json = error.toJSON();
 *
 *     // For structured logging
 *     logger.log(error.toLogEntry());
 *   }
 * }
 * ```
 */
export abstract class DoSQLError extends Error {
  /** Machine-readable error code */
  abstract readonly code: string;

  /** Error category for consistent handling */
  abstract readonly category: ErrorCategory;

  /** Timestamp when error occurred */
  readonly timestamp: number;

  /** Error context */
  context?: ErrorContext;

  /** Recovery hint for developers */
  recoveryHint?: string;

  constructor(message: string, options?: { cause?: Error; context?: ErrorContext }) {
    super(message, { cause: options?.cause });
    this.timestamp = Date.now();
    this.context = options?.context;

    // Ensure proper prototype chain for instanceof checks
    Object.setPrototypeOf(this, new.target.prototype);
  }

  /**
   * Check if this error is retryable
   * Override in subclasses for specific retry logic
   */
  isRetryable(): boolean {
    return false;
  }

  /**
   * Get a user-friendly error message
   * Override in subclasses for specific messages
   */
  toUserMessage(): string {
    return this.message;
  }

  /**
   * Serialize error for RPC/API responses
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

    if (this.cause instanceof DoSQLError) {
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
   * Format error for structured logging
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
   * Create error with additional context
   */
  withContext(context: ErrorContext): this {
    this.context = { ...this.context, ...context };
    return this;
  }

  /**
   * Set recovery hint
   */
  withRecoveryHint(hint: string): this {
    this.recoveryHint = hint;
    return this;
  }
}

// =============================================================================
// Aggregate Error
// =============================================================================

/**
 * Aggregates multiple DoSQL errors for batch operations
 */
export class AggregateDoSQLError extends DoSQLError {
  readonly code = 'AGGREGATE_ERROR';
  readonly category = ErrorCategory.EXECUTION;

  /** Individual errors */
  readonly errors: DoSQLError[];

  constructor(errors: DoSQLError[], message?: string) {
    super(message ?? `${errors.length} errors occurred`);
    this.name = 'AggregateDoSQLError';
    this.errors = errors;
  }

  isRetryable(): boolean {
    // Only retryable if all individual errors are retryable
    return this.errors.every(e => e.isRetryable());
  }

  toJSON(): SerializedError & { errors: SerializedError[] } {
    return {
      ...super.toJSON(),
      errors: this.errors.map(e => e.toJSON()),
    };
  }
}

// =============================================================================
// Error Deserialization
// =============================================================================

/** Registry of error constructors for deserialization */
const errorRegistry = new Map<string, new (...args: any[]) => DoSQLError>();

/**
 * Register an error class for deserialization
 */
export function registerErrorClass(
  name: string,
  constructor: new (...args: any[]) => DoSQLError
): void {
  errorRegistry.set(name, constructor);
}

/**
 * Deserialize an error from JSON
 * Returns a generic DoSQLError if the specific class is not registered
 */
export function deserializeError(json: SerializedError): DoSQLError {
  // Try to get the registered constructor
  const ErrorClass = errorRegistry.get(json.name) as any;

  if (ErrorClass && typeof ErrorClass.fromJSON === 'function') {
    // Use the static fromJSON method if available
    return ErrorClass.fromJSON(json);
  }

  // Fallback: create a GenericDoSQLError
  return new GenericDoSQLError(json.code, json.message, {
    context: json.context,
  });
}

/**
 * Generic DoSQL error for deserialized errors without registered classes
 */
export class GenericDoSQLError extends DoSQLError {
  readonly code: string;
  readonly category = ErrorCategory.INTERNAL;

  constructor(code: string, message: string, options?: { cause?: Error; context?: ErrorContext }) {
    super(message, options);
    this.name = 'GenericDoSQLError';
    this.code = code;
  }
}
