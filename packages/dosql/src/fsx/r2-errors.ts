/**
 * R2 Error Types
 *
 * Structured error types for R2 storage operations.
 * Provides detailed error information for debugging and monitoring.
 */

import { FSXError, FSXErrorCode } from './types.js';

// =============================================================================
// R2-Specific Error Codes
// =============================================================================

/**
 * Extended error codes for R2 operations
 */
export enum R2ErrorCode {
  /** Network timeout during operation */
  TIMEOUT = 'R2_TIMEOUT',
  /** Rate limit (429) exceeded */
  RATE_LIMITED = 'R2_RATE_LIMITED',
  /** Checksum mismatch on read */
  CHECKSUM_MISMATCH = 'R2_CHECKSUM_MISMATCH',
  /** Permission denied (403) */
  PERMISSION_DENIED = 'R2_PERMISSION_DENIED',
  /** Bucket not bound in environment */
  BUCKET_NOT_BOUND = 'R2_BUCKET_NOT_BOUND',
  /** Object size exceeds limit */
  SIZE_EXCEEDED = 'R2_SIZE_EXCEEDED',
  /** Concurrent write conflict */
  CONFLICT = 'R2_CONFLICT',
  /** Object not found (404) */
  NOT_FOUND = 'R2_NOT_FOUND',
  /** Read operation during ongoing write */
  READ_DURING_WRITE = 'R2_READ_DURING_WRITE',
  /** General network error */
  NETWORK_ERROR = 'R2_NETWORK_ERROR',
}

// =============================================================================
// R2 Error Class
// =============================================================================

/**
 * R2-specific error with detailed context
 */
export class R2Error extends FSXError {
  /** R2-specific error code */
  readonly r2Code: R2ErrorCode;
  /** HTTP status code if applicable */
  readonly httpStatus?: number;
  /** Retry-After header value if rate limited */
  readonly retryAfter?: number;
  /** Number of retries attempted */
  readonly retryCount?: number;
  /** Expected checksum (for checksum errors) */
  readonly expectedChecksum?: string;
  /** Actual checksum (for checksum errors) */
  readonly actualChecksum?: string;
  /** Request ID for debugging */
  readonly requestId?: string;

  constructor(
    r2Code: R2ErrorCode,
    message: string,
    path?: string,
    options?: {
      cause?: Error;
      httpStatus?: number;
      retryAfter?: number;
      retryCount?: number;
      expectedChecksum?: string;
      actualChecksum?: string;
      requestId?: string;
    }
  ) {
    // Map R2 error code to FSX error code
    const fsxCode = mapR2ToFSXErrorCode(r2Code);
    super(fsxCode, message, path, options?.cause);

    this.name = 'R2Error';
    this.r2Code = r2Code;
    this.httpStatus = options?.httpStatus;
    this.retryAfter = options?.retryAfter;
    this.retryCount = options?.retryCount;
    this.expectedChecksum = options?.expectedChecksum;
    this.actualChecksum = options?.actualChecksum;
    this.requestId = options?.requestId;
  }

  /**
   * Check if this error is retryable
   */
  isRetryable(): boolean {
    return [
      R2ErrorCode.TIMEOUT,
      R2ErrorCode.RATE_LIMITED,
      R2ErrorCode.NETWORK_ERROR,
      R2ErrorCode.READ_DURING_WRITE,
    ].includes(this.r2Code);
  }

  /**
   * Get a user-friendly error message
   */
  toUserMessage(): string {
    switch (this.r2Code) {
      case R2ErrorCode.TIMEOUT:
        return `Operation timed out for ${this.path ?? 'unknown path'}. Please try again.`;
      case R2ErrorCode.RATE_LIMITED:
        return `Too many requests. Please wait ${this.retryAfter ?? 'a few'} seconds.`;
      case R2ErrorCode.PERMISSION_DENIED:
        return `Access denied for ${this.path ?? 'this resource'}.`;
      case R2ErrorCode.BUCKET_NOT_BOUND:
        return 'Storage bucket is not configured. Check your wrangler.toml bindings.';
      case R2ErrorCode.SIZE_EXCEEDED:
        return 'File size exceeds the maximum allowed (5GB for single upload).';
      case R2ErrorCode.CONFLICT:
        return `Write conflict for ${this.path ?? 'this file'}. Please retry.`;
      case R2ErrorCode.CHECKSUM_MISMATCH:
        return `Data integrity error for ${this.path ?? 'this file'}. The file may be corrupted.`;
      default:
        return this.message;
    }
  }
}

// =============================================================================
// Error Detection Utilities
// =============================================================================

/**
 * Map R2 error code to FSX error code
 */
function mapR2ToFSXErrorCode(r2Code: R2ErrorCode): FSXErrorCode {
  switch (r2Code) {
    case R2ErrorCode.TIMEOUT:
    case R2ErrorCode.RATE_LIMITED:
    case R2ErrorCode.NETWORK_ERROR:
      return FSXErrorCode.READ_FAILED;
    case R2ErrorCode.CHECKSUM_MISMATCH:
      return FSXErrorCode.CHUNK_CORRUPTED;
    case R2ErrorCode.PERMISSION_DENIED:
    case R2ErrorCode.BUCKET_NOT_BOUND:
      return FSXErrorCode.READ_FAILED;
    case R2ErrorCode.SIZE_EXCEEDED:
      return FSXErrorCode.SIZE_EXCEEDED;
    case R2ErrorCode.CONFLICT:
    case R2ErrorCode.READ_DURING_WRITE:
      return FSXErrorCode.WRITE_FAILED;
    case R2ErrorCode.NOT_FOUND:
      return FSXErrorCode.NOT_FOUND;
    default:
      return FSXErrorCode.READ_FAILED;
  }
}

/**
 * Detect R2 error type from an unknown error
 */
export function detectR2ErrorType(error: unknown): R2ErrorCode {
  if (!(error instanceof Error)) {
    return R2ErrorCode.NETWORK_ERROR;
  }

  const message = error.message.toLowerCase();
  const name = error.name.toLowerCase();

  // Timeout detection
  if (message.includes('timeout') || message.includes('timed out') || message.includes('etimedout') || name.includes('timeout')) {
    return R2ErrorCode.TIMEOUT;
  }

  // Rate limit detection
  if (message.includes('429') || message.includes('rate limit') || message.includes('too many')) {
    return R2ErrorCode.RATE_LIMITED;
  }

  // Permission detection
  if (message.includes('403') || message.includes('forbidden') || message.includes('permission') || message.includes('access denied')) {
    return R2ErrorCode.PERMISSION_DENIED;
  }

  // Not found detection
  if (message.includes('404') || message.includes('not found')) {
    return R2ErrorCode.NOT_FOUND;
  }

  // Bucket not bound detection
  if (message.includes('binding') || message.includes('not bound') || message.includes('undefined')) {
    return R2ErrorCode.BUCKET_NOT_BOUND;
  }

  // Checksum detection
  if (message.includes('checksum') || message.includes('md5') || message.includes('integrity')) {
    return R2ErrorCode.CHECKSUM_MISMATCH;
  }

  // Conflict detection
  if (message.includes('conflict') || message.includes('concurrent') || message.includes('etag')) {
    return R2ErrorCode.CONFLICT;
  }

  // Size detection
  if (message.includes('size') || message.includes('too large') || message.includes('exceed')) {
    return R2ErrorCode.SIZE_EXCEEDED;
  }

  return R2ErrorCode.NETWORK_ERROR;
}

/**
 * Create an R2Error from a generic error
 */
export function createR2Error(error: unknown, path?: string, context?: string): R2Error {
  const errorType = detectR2ErrorType(error);
  const originalError = error instanceof Error ? error : new Error(String(error));

  let message = `R2 operation failed`;
  if (context) {
    message += ` during ${context}`;
  }
  if (path) {
    message += ` for path "${path}"`;
  }
  message += `: ${originalError.message}`;

  return new R2Error(errorType, message, path, { cause: originalError });
}

// =============================================================================
// Error Message Formatting
// =============================================================================

/**
 * Format R2 error for logging
 */
export function formatR2ErrorForLog(error: R2Error): string {
  const parts: string[] = [
    `[${error.r2Code}]`,
    error.message,
  ];

  if (error.path) {
    parts.push(`path=${error.path}`);
  }

  if (error.httpStatus) {
    parts.push(`status=${error.httpStatus}`);
  }

  if (error.retryCount !== undefined) {
    parts.push(`retries=${error.retryCount}`);
  }

  if (error.requestId) {
    parts.push(`requestId=${error.requestId}`);
  }

  return parts.join(' | ');
}
