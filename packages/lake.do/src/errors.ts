/**
 * lake.do - Error Classes
 *
 * Unified error handling for the lake.do client package.
 * All errors extend BaseError from @dotdo/sql-types for consistency
 * across the DoSQL ecosystem.
 *
 * @packageDocumentation
 * @stability stable
 */

import {
  BaseError,
  ErrorCategory,
  registerErrorDeserializer,
  maskUrl,
  type ErrorContext,
  type SerializedError,
} from '@dotdo/sql-types';

import type { LakeRPCError } from './types.js';
import { ErrorCode } from './constants.js';

// Re-export shared types for convenience
export {
  ErrorCategory,
  type ErrorContext,
  type SerializedError,
  type ErrorLogEntry,
  maskUrl,
} from '@dotdo/sql-types';

// =============================================================================
// Base Lake Error
// =============================================================================

/**
 * Base error thrown by Lake operations.
 */
export class LakeError extends BaseError {
  readonly code: string;
  readonly category: ErrorCategory;
  readonly details?: unknown;

  constructor(error: LakeRPCError) {
    super(error.message);
    this.name = 'LakeError';
    this.code = error.code;
    this.category = LakeError.getCategoryFromCode(error.code);
    this.details = error.details;
  }

  static create(code: string, message: string, details?: unknown): LakeError {
    return new LakeError({ code, message, details });
  }

  override isRetryable(): boolean {
    const retryableCodes: string[] = [
      ErrorCode.TIMEOUT,
      ErrorCode.CONNECTION_ERROR,
      ErrorCode.CONNECTION_CLOSED,
      ErrorCode.CONNECTION_TIMEOUT,
    ];
    return retryableCodes.includes(this.code);
  }

  private static getCategoryFromCode(code: string): ErrorCategory {
    if (code.startsWith('CONNECTION') || code === ErrorCode.NOT_CONNECTED) return ErrorCategory.CONNECTION;
    if (code === ErrorCode.TIMEOUT || code === ErrorCode.QUERY_TIMEOUT) return ErrorCategory.TIMEOUT;
    if (code === ErrorCode.INVALID_SQL) return ErrorCategory.VALIDATION;
    if (code === ErrorCode.TABLE_NOT_FOUND || code === ErrorCode.PARTITION_NOT_FOUND) return ErrorCategory.RESOURCE;
    if (code === ErrorCode.UNAUTHORIZED || code === ErrorCode.TOKEN_EXPIRED) return ErrorCategory.VALIDATION;
    return ErrorCategory.EXECUTION;
  }

  static fromJSON(json: SerializedError): LakeError {
    return new LakeError({
      code: json.code,
      message: json.message,
      details: json.context?.metadata?.details,
    });
  }
}

registerErrorDeserializer('LakeError', LakeError.fromJSON);

// =============================================================================
// Connection Error
// =============================================================================

/**
 * Error thrown when a connection operation fails.
 */
export class ConnectionError extends LakeError {
  readonly url?: string;

  constructor(error: LakeRPCError, url?: string) {
    const maskedUrl = url ? maskUrl(url) : undefined;
    const errorWithUrl = maskedUrl
      ? { ...error, message: `${error.message} (url: ${maskedUrl})` }
      : error;
    super(errorWithUrl);
    this.name = 'ConnectionError';
    if (maskedUrl) {
      this.url = maskedUrl;
    }
  }

  override isRetryable(): boolean {
    return true;
  }

  override toUserMessage(): string {
    return 'Failed to connect to the lake service. Please check your network connection and try again.';
  }

  static override create(code: string, message: string, details?: unknown, url?: string): ConnectionError {
    return new ConnectionError({ code, message, details }, url);
  }

  static closed(message = 'Connection closed', url?: string): ConnectionError {
    return ConnectionError.create(ErrorCode.CONNECTION_CLOSED, message, undefined, url);
  }

  static notConnected(message = 'WebSocket is not connected', url?: string): ConnectionError {
    return ConnectionError.create(ErrorCode.NOT_CONNECTED, message, undefined, url);
  }

  static failed(message = 'Connection failed', url?: string): ConnectionError {
    return ConnectionError.create(ErrorCode.CONNECTION_ERROR, message, undefined, url);
  }

  static override fromJSON(json: SerializedError): ConnectionError {
    const url = json.context?.metadata?.url as string | undefined;
    return new ConnectionError({ code: json.code, message: json.message }, url);
  }
}

registerErrorDeserializer('ConnectionError', ConnectionError.fromJSON);

// =============================================================================
// Query Error
// =============================================================================

/**
 * Error thrown when a query operation fails.
 */
export class QueryError extends LakeError {
  constructor(error: LakeRPCError) {
    super(error);
    this.name = 'QueryError';
  }

  override isRetryable(): boolean {
    return this.code === ErrorCode.QUERY_TIMEOUT;
  }

  static override create(code: string, message: string, details?: unknown): QueryError {
    return new QueryError({ code, message, details });
  }

  static tableNotFound(tableName: string): QueryError {
    return QueryError.create(
      ErrorCode.TABLE_NOT_FOUND,
      `Table "${tableName}" does not exist`,
      { tableName }
    );
  }

  static invalidSql(message: string, details?: unknown): QueryError {
    return QueryError.create(ErrorCode.INVALID_SQL, message, details);
  }

  static timeout(message = 'Query execution timed out'): QueryError {
    return QueryError.create(ErrorCode.QUERY_TIMEOUT, message);
  }

  static override fromJSON(json: SerializedError): QueryError {
    return new QueryError({ code: json.code, message: json.message, details: json.context?.metadata?.details });
  }
}

registerErrorDeserializer('QueryError', QueryError.fromJSON);

// =============================================================================
// Timeout Error
// =============================================================================

/**
 * Error thrown when a request times out.
 */
export class TimeoutError extends LakeError {
  readonly method: string;

  constructor(method: string) {
    super({
      code: ErrorCode.TIMEOUT,
      message: `Request timeout: ${method}`,
      details: { method },
    });
    this.name = 'TimeoutError';
    this.method = method;
  }

  override isRetryable(): boolean {
    return true;
  }

  override toUserMessage(): string {
    return `The ${this.method} operation timed out. Please try again.`;
  }

  static override fromJSON(json: SerializedError): TimeoutError {
    const method = (json.context?.metadata?.method as string) ?? 'unknown';
    return new TimeoutError(method);
  }
}

registerErrorDeserializer('TimeoutError', TimeoutError.fromJSON);
