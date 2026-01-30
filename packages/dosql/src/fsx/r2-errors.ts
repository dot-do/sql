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

export enum R2ErrorCode {
  TIMEOUT = 'R2_TIMEOUT',
  RATE_LIMITED = 'R2_RATE_LIMITED',
  CHECKSUM_MISMATCH = 'R2_CHECKSUM_MISMATCH',
  PERMISSION_DENIED = 'R2_PERMISSION_DENIED',
  BUCKET_NOT_BOUND = 'R2_BUCKET_NOT_BOUND',
  SIZE_EXCEEDED = 'R2_SIZE_EXCEEDED',
  CONFLICT = 'R2_CONFLICT',
  NOT_FOUND = 'R2_NOT_FOUND',
  READ_DURING_WRITE = 'R2_READ_DURING_WRITE',
  NETWORK_ERROR = 'R2_NETWORK_ERROR',
}

// =============================================================================
// R2 Error Class
// =============================================================================

export class R2Error extends FSXError {
  readonly r2Code: R2ErrorCode;
  readonly httpStatus?: number;
  readonly retryAfter?: number;
  readonly retryCount?: number;
  readonly expectedChecksum?: string;
  readonly actualChecksum?: string;
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
      fsxCode?: FSXErrorCode;
    }
  ) {
    const fsxCode = options?.fsxCode ?? mapR2ToFSXErrorCode(r2Code);
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

  isRetryable(): boolean {
    return [
      R2ErrorCode.TIMEOUT,
      R2ErrorCode.RATE_LIMITED,
      R2ErrorCode.NETWORK_ERROR,
      R2ErrorCode.READ_DURING_WRITE,
    ].includes(this.r2Code);
  }

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

function mapR2ToFSXErrorCode(r2Code: R2ErrorCode): FSXErrorCode {
  switch (r2Code) {
    case R2ErrorCode.TIMEOUT:
    case R2ErrorCode.NETWORK_ERROR:
    case R2ErrorCode.RATE_LIMITED:
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

export function mapR2ToFSXErrorCodeForOp(r2Code: R2ErrorCode, operation: string): FSXErrorCode {
  const isWrite = operation === 'write' || operation === 'writeWithMetadata' || operation === 'delete' || operation === 'deleteMany';

  switch (r2Code) {
    case R2ErrorCode.TIMEOUT:
    case R2ErrorCode.NETWORK_ERROR:
    case R2ErrorCode.RATE_LIMITED:
      return isWrite ? FSXErrorCode.WRITE_FAILED : FSXErrorCode.READ_FAILED;
    case R2ErrorCode.CHECKSUM_MISMATCH:
      return FSXErrorCode.CHUNK_CORRUPTED;
    case R2ErrorCode.PERMISSION_DENIED:
    case R2ErrorCode.BUCKET_NOT_BOUND:
      return isWrite ? FSXErrorCode.WRITE_FAILED : FSXErrorCode.READ_FAILED;
    case R2ErrorCode.SIZE_EXCEEDED:
      return FSXErrorCode.SIZE_EXCEEDED;
    case R2ErrorCode.CONFLICT:
    case R2ErrorCode.READ_DURING_WRITE:
      return FSXErrorCode.WRITE_FAILED;
    case R2ErrorCode.NOT_FOUND:
      return FSXErrorCode.NOT_FOUND;
    default:
      return isWrite ? FSXErrorCode.WRITE_FAILED : FSXErrorCode.READ_FAILED;
  }
}

export function detectR2ErrorType(error: unknown): R2ErrorCode {
  if (!(error instanceof Error)) {
    return R2ErrorCode.NETWORK_ERROR;
  }

  const message = error.message.toLowerCase();
  const name = error.name.toLowerCase();

  if (message.includes('timeout') || message.includes('timed out') || message.includes('etimedout') || name.includes('timeout')) {
    return R2ErrorCode.TIMEOUT;
  }

  if (message.includes('429') || message.includes('rate limit') || message.includes('too many')) {
    return R2ErrorCode.RATE_LIMITED;
  }

  if (message.includes('checksum') || name.includes('checksum') || message.includes('md5') || message.includes('integrity')) {
    return R2ErrorCode.CHECKSUM_MISMATCH;
  }

  if (message.includes('permission') || message.includes('access denied') || name.includes('permission') || message.includes('forbidden')) {
    return R2ErrorCode.PERMISSION_DENIED;
  }

  // Bucket not bound - check BEFORE "not found"
  if (message.includes('bucket') || message.includes('binding') || message.includes('not bound')) {
    return R2ErrorCode.BUCKET_NOT_BOUND;
  }

  if (message.includes('404') || message.includes('not found')) {
    return R2ErrorCode.NOT_FOUND;
  }

  if (message.includes('being written') || message.includes('write in progress')) {
    return R2ErrorCode.READ_DURING_WRITE;
  }

  if (message.includes('conflict') || message.includes('concurrent') || message.includes('etag')) {
    return R2ErrorCode.CONFLICT;
  }

  if (message.includes('size') || message.includes('too large') || message.includes('exceed')) {
    return R2ErrorCode.SIZE_EXCEEDED;
  }

  return R2ErrorCode.NETWORK_ERROR;
}

function buildR2ErrorMessage(
  errorType: R2ErrorCode,
  originalError: Error,
  path?: string,
  operation?: string,
): string {
  const pathStr = path ? ` for path "${path}"` : '';

  switch (errorType) {
    case R2ErrorCode.TIMEOUT:
      return `R2 timeout during ${operation}${pathStr}: ${originalError.message}`;

    case R2ErrorCode.RATE_LIMITED:
      return `R2 rate limit (429) exceeded during ${operation}${pathStr}: retry after 1s - ${originalError.message}`;

    case R2ErrorCode.PERMISSION_DENIED:
      return `R2 permission denied (403) during ${operation}${pathStr}: ${originalError.message}`;

    case R2ErrorCode.BUCKET_NOT_BOUND:
      return `R2 bucket not bound during ${operation}${pathStr}: check wrangler.toml binding configuration - ${originalError.message}`;

    case R2ErrorCode.CHECKSUM_MISMATCH: {
      const ce = originalError as { expected?: string; actual?: string };
      let msg = `R2 checksum mismatch during ${operation}${pathStr}`;
      if (ce.expected && ce.actual) {
        msg += `: expected ${ce.expected}, got ${ce.actual}`;
      } else {
        msg += `: ${originalError.message}`;
      }
      return msg;
    }

    case R2ErrorCode.SIZE_EXCEEDED:
      return `R2 object size exceeded (max 5GB) during ${operation}${pathStr}: consider using multipart upload - ${originalError.message}`;

    case R2ErrorCode.CONFLICT:
      return `R2 concurrent write conflict during ${operation}${pathStr}: retry the operation - ${originalError.message}`;

    case R2ErrorCode.READ_DURING_WRITE:
      return `R2 write in progress during ${operation}${pathStr}: retry after write completes - ${originalError.message}`;

    default:
      return `R2 operation failed during ${operation}${pathStr}: ${originalError.message}`;
  }
}

/**
 * Create a detailed R2Error from a generic error
 */
export function createR2Error(error: unknown, path?: string, context?: string): R2Error {
  const errorType = detectR2ErrorType(error);
  const originalError = error instanceof Error ? error : new Error(String(error));
  const operation = context ?? 'unknown';

  const fsxCode = mapR2ToFSXErrorCodeForOp(errorType, operation);
  const message = buildR2ErrorMessage(errorType, originalError, path, operation);

  const options: {
    cause?: Error;
    httpStatus?: number;
    retryAfter?: number;
    expectedChecksum?: string;
    actualChecksum?: string;
    fsxCode: FSXErrorCode;
  } = { cause: originalError, fsxCode };

  if (errorType === R2ErrorCode.RATE_LIMITED) {
    options.httpStatus = 429;
    options.retryAfter = 1;
  } else if (errorType === R2ErrorCode.PERMISSION_DENIED) {
    options.httpStatus = 403;
  }

  if (errorType === R2ErrorCode.CHECKSUM_MISMATCH && originalError) {
    const checksumError = originalError as { expected?: string; actual?: string };
    if (checksumError.expected) options.expectedChecksum = checksumError.expected;
    if (checksumError.actual) options.actualChecksum = checksumError.actual;
  }

  return new R2Error(errorType, message, path, options);
}

/**
 * Create an R2Error for retries-exhausted scenario
 */
export function createRetriesExhaustedError(
  lastError: unknown,
  path: string | undefined,
  operation: string,
  retryCount: number,
): R2Error {
  const originalError = lastError instanceof Error ? lastError : new Error(String(lastError));
  const errorType = detectR2ErrorType(lastError);
  const fsxCode = mapR2ToFSXErrorCodeForOp(errorType, operation);
  const baseMessage = buildR2ErrorMessage(errorType, originalError, path, operation);

  const message = `${baseMessage} (retries exhausted after ${retryCount} attempts)`;

  return new R2Error(errorType, message, path, {
    cause: originalError,
    retryCount,
    fsxCode,
  });
}

/**
 * Create an R2Error for circuit breaker open state
 */
export function createCircuitOpenError(
  path: string | undefined,
  operation: string,
  failureCount: number,
): R2Error {
  const pathStr = path ? ` for path "${path}"` : '';
  const message = `R2 circuit open${pathStr}: ${operation} rejected - failure count: ${failureCount}`;

  return new R2Error(R2ErrorCode.NETWORK_ERROR, message, path);
}

/**
 * Create an R2Error for partial write failure
 */
export function createPartialWriteError(path: string, cause: Error): R2Error {
  const message = `R2 write failed for path "${path}": partial write detected - checksum mismatch after write`;
  return new R2Error(R2ErrorCode.NETWORK_ERROR, message, path, {
    cause,
    fsxCode: FSXErrorCode.WRITE_FAILED,
  });
}

// =============================================================================
// Error Message Formatting
// =============================================================================

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
