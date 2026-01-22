/**
 * R2 Error Types Tests
 *
 * Tests for the R2 error detection and formatting utilities.
 */

import { describe, it, expect } from 'vitest';
import {
  R2Error,
  R2ErrorCode,
  detectR2ErrorType,
  createR2Error,
  formatR2ErrorForLog,
} from '../r2-errors.js';
import { FSXErrorCode } from '../types.js';

// =============================================================================
// R2Error Class Tests
// =============================================================================

describe('R2Error', () => {
  describe('Construction', () => {
    it('should create error with r2 code', () => {
      const error = new R2Error(R2ErrorCode.TIMEOUT, 'Request timed out', '/path/to/file');

      expect(error.r2Code).toBe(R2ErrorCode.TIMEOUT);
      expect(error.message).toBe('Request timed out');
      expect(error.path).toBe('/path/to/file');
      expect(error.name).toBe('R2Error');
    });

    it('should include optional properties', () => {
      const cause = new Error('Original error');
      const error = new R2Error(R2ErrorCode.RATE_LIMITED, 'Too many requests', '/data', {
        cause,
        httpStatus: 429,
        retryAfter: 30,
        retryCount: 3,
        requestId: 'req-123',
      });

      expect(error.cause).toBe(cause);
      expect(error.httpStatus).toBe(429);
      expect(error.retryAfter).toBe(30);
      expect(error.retryCount).toBe(3);
      expect(error.requestId).toBe('req-123');
    });

    it('should map to FSX error code', () => {
      const timeoutError = new R2Error(R2ErrorCode.TIMEOUT, 'Timeout');
      expect(timeoutError.code).toBe(FSXErrorCode.READ_FAILED);

      const sizeError = new R2Error(R2ErrorCode.SIZE_EXCEEDED, 'Too large');
      expect(sizeError.code).toBe(FSXErrorCode.SIZE_EXCEEDED);

      const notFoundError = new R2Error(R2ErrorCode.NOT_FOUND, 'Not found');
      expect(notFoundError.code).toBe(FSXErrorCode.NOT_FOUND);
    });
  });

  describe('isRetryable', () => {
    it('should return true for retryable errors', () => {
      expect(new R2Error(R2ErrorCode.TIMEOUT, 'Timeout').isRetryable()).toBe(true);
      expect(new R2Error(R2ErrorCode.RATE_LIMITED, 'Rate limited').isRetryable()).toBe(true);
      expect(new R2Error(R2ErrorCode.NETWORK_ERROR, 'Network error').isRetryable()).toBe(true);
      expect(new R2Error(R2ErrorCode.READ_DURING_WRITE, 'Busy').isRetryable()).toBe(true);
    });

    it('should return false for non-retryable errors', () => {
      expect(new R2Error(R2ErrorCode.PERMISSION_DENIED, 'Forbidden').isRetryable()).toBe(false);
      expect(new R2Error(R2ErrorCode.NOT_FOUND, 'Not found').isRetryable()).toBe(false);
      expect(new R2Error(R2ErrorCode.SIZE_EXCEEDED, 'Too large').isRetryable()).toBe(false);
      expect(new R2Error(R2ErrorCode.CHECKSUM_MISMATCH, 'Corrupted').isRetryable()).toBe(false);
    });
  });

  describe('toUserMessage', () => {
    it('should return user-friendly messages', () => {
      expect(new R2Error(R2ErrorCode.TIMEOUT, '', '/data').toUserMessage()).toContain('timed out');
      expect(new R2Error(R2ErrorCode.RATE_LIMITED, '', undefined, { retryAfter: 5 }).toUserMessage()).toContain('5');
      expect(new R2Error(R2ErrorCode.BUCKET_NOT_BOUND, '').toUserMessage()).toContain('wrangler.toml');
      expect(new R2Error(R2ErrorCode.SIZE_EXCEEDED, '').toUserMessage()).toContain('5GB');
    });
  });
});

// =============================================================================
// Error Detection Tests
// =============================================================================

describe('detectR2ErrorType', () => {
  it('should detect timeout errors', () => {
    expect(detectR2ErrorType(new Error('Request timed out'))).toBe(R2ErrorCode.TIMEOUT);
    expect(detectR2ErrorType(new Error('ETIMEDOUT'))).toBe(R2ErrorCode.TIMEOUT);
  });

  it('should detect rate limit errors', () => {
    expect(detectR2ErrorType(new Error('429 Too Many Requests'))).toBe(R2ErrorCode.RATE_LIMITED);
    expect(detectR2ErrorType(new Error('Rate limit exceeded'))).toBe(R2ErrorCode.RATE_LIMITED);
  });

  it('should detect permission errors', () => {
    expect(detectR2ErrorType(new Error('403 Forbidden'))).toBe(R2ErrorCode.PERMISSION_DENIED);
    expect(detectR2ErrorType(new Error('Access denied'))).toBe(R2ErrorCode.PERMISSION_DENIED);
    expect(detectR2ErrorType(new Error('Permission denied'))).toBe(R2ErrorCode.PERMISSION_DENIED);
  });

  it('should detect not found errors', () => {
    expect(detectR2ErrorType(new Error('404 Not Found'))).toBe(R2ErrorCode.NOT_FOUND);
    expect(detectR2ErrorType(new Error('Object not found'))).toBe(R2ErrorCode.NOT_FOUND);
  });

  it('should detect bucket not bound errors', () => {
    expect(detectR2ErrorType(new Error('R2Bucket is not bound'))).toBe(R2ErrorCode.BUCKET_NOT_BOUND);
    expect(detectR2ErrorType(new Error('binding undefined'))).toBe(R2ErrorCode.BUCKET_NOT_BOUND);
  });

  it('should detect checksum errors', () => {
    expect(detectR2ErrorType(new Error('Checksum mismatch'))).toBe(R2ErrorCode.CHECKSUM_MISMATCH);
    expect(detectR2ErrorType(new Error('MD5 verification failed'))).toBe(R2ErrorCode.CHECKSUM_MISMATCH);
    expect(detectR2ErrorType(new Error('Integrity check failed'))).toBe(R2ErrorCode.CHECKSUM_MISMATCH);
  });

  it('should detect conflict errors', () => {
    expect(detectR2ErrorType(new Error('Write conflict'))).toBe(R2ErrorCode.CONFLICT);
    expect(detectR2ErrorType(new Error('Concurrent modification'))).toBe(R2ErrorCode.CONFLICT);
    expect(detectR2ErrorType(new Error('ETag mismatch'))).toBe(R2ErrorCode.CONFLICT);
  });

  it('should detect size errors', () => {
    expect(detectR2ErrorType(new Error('Object size exceeds limit'))).toBe(R2ErrorCode.SIZE_EXCEEDED);
    expect(detectR2ErrorType(new Error('File too large'))).toBe(R2ErrorCode.SIZE_EXCEEDED);
  });

  it('should default to network error for unknown errors', () => {
    expect(detectR2ErrorType(new Error('Unknown error'))).toBe(R2ErrorCode.NETWORK_ERROR);
    expect(detectR2ErrorType('string error')).toBe(R2ErrorCode.NETWORK_ERROR);
    expect(detectR2ErrorType(null)).toBe(R2ErrorCode.NETWORK_ERROR);
  });
});

// =============================================================================
// createR2Error Tests
// =============================================================================

describe('createR2Error', () => {
  it('should create R2Error from generic error', () => {
    const original = new Error('Request timed out');
    const r2Error = createR2Error(original, '/data/file.bin', 'read');

    expect(r2Error).toBeInstanceOf(R2Error);
    expect(r2Error.r2Code).toBe(R2ErrorCode.TIMEOUT);
    expect(r2Error.path).toBe('/data/file.bin');
    expect(r2Error.message).toContain('read');
    expect(r2Error.message).toContain('/data/file.bin');
    expect(r2Error.cause).toBe(original);
  });

  it('should handle string errors', () => {
    const r2Error = createR2Error('something went wrong');
    expect(r2Error).toBeInstanceOf(R2Error);
    expect(r2Error.message).toContain('something went wrong');
  });

  it('should work without path and context', () => {
    const r2Error = createR2Error(new Error('Failed'));
    expect(r2Error).toBeInstanceOf(R2Error);
    expect(r2Error.path).toBeUndefined();
  });
});

// =============================================================================
// formatR2ErrorForLog Tests
// =============================================================================

describe('formatR2ErrorForLog', () => {
  it('should format error with all fields', () => {
    const error = new R2Error(R2ErrorCode.TIMEOUT, 'Request timed out', '/data/file.bin', {
      httpStatus: 408,
      retryCount: 3,
      requestId: 'req-abc-123',
    });

    const formatted = formatR2ErrorForLog(error);

    expect(formatted).toContain('[R2_TIMEOUT]');
    expect(formatted).toContain('Request timed out');
    expect(formatted).toContain('path=/data/file.bin');
    expect(formatted).toContain('status=408');
    expect(formatted).toContain('retries=3');
    expect(formatted).toContain('requestId=req-abc-123');
  });

  it('should handle minimal error', () => {
    const error = new R2Error(R2ErrorCode.NOT_FOUND, 'Object not found');
    const formatted = formatR2ErrorForLog(error);

    expect(formatted).toContain('[R2_NOT_FOUND]');
    expect(formatted).toContain('Object not found');
    expect(formatted).not.toContain('path=');
    expect(formatted).not.toContain('status=');
  });
});
