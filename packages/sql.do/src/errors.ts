/**
 * SQL Client Error Types
 *
 * Unified error handling for the sql.do client package.
 * All errors extend BaseError from @dotdo/sql-types for consistency
 * across the DoSQL ecosystem.
 *
 * @packageDocumentation
 */

import {
  BaseError,
  ErrorCategory,
  registerErrorDeserializer,
  maskUrl,
  type ErrorContext,
  type SerializedError,
} from '@dotdo/sql-types';

import type { RPCError } from './types.js';

// Re-export shared types for convenience
export {
  ErrorCategory,
  type ErrorContext,
  type SerializedError,
  type ErrorLogEntry,
  maskUrl,
} from '@dotdo/sql-types';

// =============================================================================
// Error Codes
// =============================================================================

/**
 * SQL client error codes following standardized naming convention.
 */
export const SQLErrorCode = {
  // Query errors
  SYNTAX_ERROR: 'QUERY_SYNTAX_ERROR',
  CONSTRAINT_VIOLATION: 'QUERY_CONSTRAINT_VIOLATION',
  TABLE_NOT_FOUND: 'QUERY_TABLE_NOT_FOUND',
  COLUMN_NOT_FOUND: 'QUERY_COLUMN_NOT_FOUND',

  // Connection errors
  CONNECTION_FAILED: 'CONN_FAILED',
  CONNECTION_CLOSED: 'CONN_CLOSED',

  // Timeout errors
  TIMEOUT: 'TIMEOUT',
  QUERY_TIMEOUT: 'QUERY_TIMEOUT',

  // Transaction errors
  TRANSACTION_CONFLICT: 'TX_CONFLICT',
  TRANSACTION_ABORTED: 'TX_ABORTED',

  // Protocol errors
  MESSAGE_PARSE_ERROR: 'PROTOCOL_MESSAGE_PARSE_ERROR',

  // Network errors
  NETWORK_ERROR: 'NETWORK_ERROR',
  UNAVAILABLE: 'UNAVAILABLE',
  RESOURCE_EXHAUSTED: 'RESOURCE_EXHAUSTED',
} as const;

export type SQLErrorCode = typeof SQLErrorCode[keyof typeof SQLErrorCode];

// =============================================================================
// Retryable Error Codes
// =============================================================================

/**
 * Error codes that indicate the error is retryable.
 */
export const RETRYABLE_ERROR_CODES = [
  'TIMEOUT',
  'QUERY_TIMEOUT',
  'CONN_CLOSED',
  'CONN_FAILED',
  'NETWORK_ERROR',
  'UNAVAILABLE',
  'RESOURCE_EXHAUSTED',
] as const;

export type RetryableErrorCode = typeof RETRYABLE_ERROR_CODES[number];

const RETRYABLE_ERROR_CODES_SET: ReadonlySet<string> = new Set(RETRYABLE_ERROR_CODES);

/**
 * Checks if a SQL error code is retryable.
 */
export function isRetryableCode(code: string): boolean {
  return RETRYABLE_ERROR_CODES_SET.has(code);
}

// =============================================================================
// SQL Error
// =============================================================================

/**
 * Error thrown by SQL operations when a query or command fails.
 */
export class SQLError extends BaseError {
  readonly code: string;
  readonly category: ErrorCategory;
  readonly details?: unknown;
  readonly suggestion?: string;

  constructor(error: RPCError) {
    super(error.message);
    this.name = 'SQLError';
    this.code = error.code;
    this.category = SQLError.getCategoryFromCode(error.code);
    if (error.details !== undefined) this.details = error.details;
    if (error.suggestion !== undefined) this.suggestion = error.suggestion;
  }

  static create(code: string, message: string, details?: unknown, suggestion?: string): SQLError {
    const error: RPCError = { code, message } as RPCError;
    if (details !== undefined) {
      (error as { details: unknown }).details = details;
    }
    if (suggestion !== undefined) {
      (error as { suggestion: string }).suggestion = suggestion;
    }
    return new SQLError(error);
  }

  override isRetryable(): boolean {
    return isRetryableCode(this.code);
  }

  override toUserMessage(): string {
    if (this.suggestion) {
      return `${this.message}. Suggestion: ${this.suggestion}`;
    }
    return this.message;
  }

  private static getCategoryFromCode(code: string): ErrorCategory {
    if (code.startsWith('CONN_') || code === 'NETWORK_ERROR') return ErrorCategory.CONNECTION;
    if (code === 'TIMEOUT' || code.endsWith('_TIMEOUT')) return ErrorCategory.TIMEOUT;
    if (code.startsWith('QUERY_SYNTAX') || code.startsWith('QUERY_TABLE')) return ErrorCategory.VALIDATION;
    if (code.startsWith('TX_')) return ErrorCategory.CONFLICT;
    return ErrorCategory.EXECUTION;
  }

  static fromJSON(json: SerializedError): SQLError {
    const error: RPCError = { code: json.code, message: json.message } as RPCError;
    if (json.context?.metadata?.details !== undefined) {
      (error as { details: unknown }).details = json.context.metadata.details;
    }
    if (json.context?.metadata?.suggestion !== undefined) {
      (error as { suggestion: string }).suggestion = json.context.metadata.suggestion as string;
    }
    return new SQLError(error);
  }
}

registerErrorDeserializer('SQLError', SQLError.fromJSON);

// =============================================================================
// Connection Error
// =============================================================================

/**
 * Error thrown when a connection to the database fails.
 */
export class ConnectionError extends BaseError {
  readonly code = SQLErrorCode.CONNECTION_FAILED;
  readonly category = ErrorCategory.CONNECTION;
  readonly url?: string;

  /** Backward compatibility property */
  readonly retryable = true;

  constructor(message: string, url?: string) {
    const maskedUrl = url ? maskUrl(url) : undefined;
    const fullMessage = maskedUrl ? `${message} (url: ${maskedUrl})` : message;
    super(fullMessage);
    this.name = 'ConnectionError';
    if (maskedUrl) {
      this.url = maskedUrl;
    }
  }

  override isRetryable(): boolean {
    return true;
  }

  override toUserMessage(): string {
    return 'Failed to connect to the database. Please check your network connection and try again.';
  }

  static fromJSON(json: SerializedError): ConnectionError {
    const url = json.context?.metadata?.url as string | undefined;
    return new ConnectionError(json.message, url);
  }
}

registerErrorDeserializer('ConnectionError', ConnectionError.fromJSON);

// =============================================================================
// Timeout Error
// =============================================================================

/**
 * The type of operation that timed out.
 */
export type TimeoutOperationType = 'query' | 'exec' | 'transaction' | 'rpc';

/**
 * Error thrown when an operation times out.
 */
export class TimeoutError extends BaseError {
  readonly code = SQLErrorCode.TIMEOUT;
  readonly category = ErrorCategory.TIMEOUT;
  readonly timeoutMs: number;
  readonly operationType: TimeoutOperationType;

  /** Backward compatibility property */
  readonly retryable = true;

  constructor(operationType: TimeoutOperationType, timeoutMs: number) {
    super(`${operationType} timeout after ${timeoutMs}ms`);
    this.name = 'TimeoutError';
    this.timeoutMs = timeoutMs;
    this.operationType = operationType;
    this.context = {
      metadata: { timeoutMs, operationType },
    };
  }

  override isRetryable(): boolean {
    return true;
  }

  override toUserMessage(): string {
    return `The ${this.operationType} operation timed out. Please try again or consider breaking the operation into smaller parts.`;
  }

  static fromJSON(json: SerializedError): TimeoutError {
    const meta = json.context?.metadata ?? {};
    return new TimeoutError(
      (meta.operationType as TimeoutOperationType) ?? 'query',
      (meta.timeoutMs as number) ?? 30000
    );
  }
}

registerErrorDeserializer('TimeoutError', TimeoutError.fromJSON);

// =============================================================================
// Message Parse Error
// =============================================================================

/**
 * Error thrown when WebSocket message parsing fails.
 */
export class MessageParseError extends BaseError {
  readonly code = SQLErrorCode.MESSAGE_PARSE_ERROR;
  readonly category = ErrorCategory.INTERNAL;
  readonly rawMessage: string | undefined;
  readonly originalError: Error | undefined;

  constructor(message: string, rawMessage?: string, originalError?: Error) {
    const superOpts: { cause?: Error } = {};
    if (originalError) {
      superOpts.cause = originalError;
    }
    super(message, Object.keys(superOpts).length > 0 ? superOpts : undefined);
    this.name = 'MessageParseError';
    this.rawMessage = rawMessage !== undefined ? rawMessage.substring(0, 1000) : undefined;
    this.originalError = originalError;
  }

  override isRetryable(): boolean {
    return false;
  }

  override toUserMessage(): string {
    return 'A protocol error occurred. Please report this issue if it persists.';
  }

  static fromJSON(json: SerializedError): MessageParseError {
    return new MessageParseError(
      json.message,
      json.context?.metadata?.rawMessage as string | undefined
    );
  }
}

registerErrorDeserializer('MessageParseError', MessageParseError.fromJSON);

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Checks if a SQL error is retryable.
 */
export function isRetryableError(error: SQLError | ConnectionError | TimeoutError): boolean {
  return error.isRetryable();
}
