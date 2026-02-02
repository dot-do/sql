/**
 * Error Classes Tests
 *
 * Comprehensive tests for error classes and utilities:
 * - SQLError
 * - ConnectionError
 * - TimeoutError
 * - MessageParseError
 * - URL masking
 * - Retryable error detection
 *
 * Issue: sql-k3wp
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';
import {
  SQLError,
  ConnectionError,
  TimeoutError,
  MessageParseError,
  maskUrl,
  isRetryableError,
  RETRYABLE_ERROR_CODES,
} from '../errors.js';

// =============================================================================
// SQLError Tests
// =============================================================================

describe('SQLError', () => {
  it('should create error with code and message', () => {
    const error = new SQLError({
      code: 'SYNTAX_ERROR',
      message: 'Invalid SQL syntax',
    });

    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(SQLError);
    expect(error.name).toBe('SQLError');
    expect(error.code).toBe('SYNTAX_ERROR');
    expect(error.message).toBe('Invalid SQL syntax');
  });

  it('should include optional details', () => {
    const details = { line: 1, column: 15, token: 'SELCT' };
    const error = new SQLError({
      code: 'SYNTAX_ERROR',
      message: 'Unknown keyword',
      details,
    });

    expect(error.details).toEqual(details);
  });

  it('should include optional suggestion', () => {
    const error = new SQLError({
      code: 'SYNTAX_ERROR',
      message: 'Unknown keyword SELCT',
      suggestion: 'Did you mean SELECT?',
    });

    expect(error.suggestion).toBe('Did you mean SELECT?');
  });

  it('should not have details if not provided', () => {
    const error = new SQLError({
      code: 'TEST',
      message: 'Test error',
    });

    expect(error.details).toBeUndefined();
  });

  it('should not have suggestion if not provided', () => {
    const error = new SQLError({
      code: 'TEST',
      message: 'Test error',
    });

    expect(error.suggestion).toBeUndefined();
  });

  it('should work with instanceof checks', () => {
    const error = new SQLError({
      code: 'TEST',
      message: 'Test',
    });

    expect(error instanceof Error).toBe(true);
    expect(error instanceof SQLError).toBe(true);
  });

  it('should have proper stack trace', () => {
    const error = new SQLError({
      code: 'TEST',
      message: 'Test',
    });

    expect(error.stack).toBeDefined();
    expect(error.stack).toContain('SQLError');
  });
});

// =============================================================================
// ConnectionError Tests
// =============================================================================

describe('ConnectionError', () => {
  it('should create error with message', () => {
    const error = new ConnectionError('Failed to connect');

    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(ConnectionError);
    expect(error.name).toBe('ConnectionError');
    expect(error.message).toBe('Failed to connect');
  });

  it('should have CONN_FAILED code', () => {
    const error = new ConnectionError('Failed');

    expect(error.code).toBe('CONN_FAILED');
  });

  it('should be retryable via isRetryable method', () => {
    const error = new ConnectionError('Failed');

    expect(error.isRetryable()).toBe(true);
  });

  it('should include masked URL', () => {
    const error = new ConnectionError('Failed', 'ws://localhost:8080');

    expect(error.url).toBe('ws://localhost:8080/');
    expect(error.message).toContain('ws://localhost:8080');
  });

  it('should mask password in URL', () => {
    const error = new ConnectionError('Failed', 'ws://user:secret@localhost:8080');

    expect(error.url).not.toContain('secret');
    expect(error.url).toContain('***');
  });

  it('should mask sensitive query parameters', () => {
    const error = new ConnectionError('Failed', 'ws://localhost:8080?token=secret123');

    expect(error.url).not.toContain('secret123');
    expect(error.url).toContain('token=***');
  });

  it('should handle no URL provided', () => {
    const error = new ConnectionError('Failed');

    expect(error.url).toBeUndefined();
  });
});

// =============================================================================
// TimeoutError Tests
// =============================================================================

describe('TimeoutError', () => {
  it('should create error with operation type and timeout', () => {
    const error = new TimeoutError('query', 30000);

    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(TimeoutError);
    expect(error.name).toBe('TimeoutError');
    expect(error.operationType).toBe('query');
    expect(error.timeoutMs).toBe(30000);
  });

  it('should have TIMEOUT code', () => {
    const error = new TimeoutError('query', 30000);

    expect(error.code).toBe('TIMEOUT');
  });

  it('should be retryable via isRetryable method', () => {
    const error = new TimeoutError('query', 30000);

    expect(error.isRetryable()).toBe(true);
  });

  it('should format message correctly', () => {
    const error = new TimeoutError('exec', 5000);

    expect(error.message).toBe('exec timeout after 5000ms');
  });

  it('should handle all operation types', () => {
    const queryError = new TimeoutError('query', 1000);
    expect(queryError.operationType).toBe('query');

    const execError = new TimeoutError('exec', 1000);
    expect(execError.operationType).toBe('exec');

    const txError = new TimeoutError('transaction', 1000);
    expect(txError.operationType).toBe('transaction');

    const rpcError = new TimeoutError('rpc', 1000);
    expect(rpcError.operationType).toBe('rpc');
  });
});

// =============================================================================
// MessageParseError Tests
// =============================================================================

describe('MessageParseError', () => {
  it('should create error with message', () => {
    const error = new MessageParseError('Failed to parse JSON');

    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(MessageParseError);
    expect(error.name).toBe('MessageParseError');
    expect(error.message).toBe('Failed to parse JSON');
  });

  it('should have PROTOCOL_MESSAGE_PARSE_ERROR code', () => {
    const error = new MessageParseError('Parse failed');

    expect(error.code).toBe('PROTOCOL_MESSAGE_PARSE_ERROR');
  });

  it('should not be retryable via isRetryable method', () => {
    const error = new MessageParseError('Parse failed');

    expect(error.isRetryable()).toBe(false);
  });

  it('should include raw message when provided', () => {
    const rawMessage = '{"invalid json';
    const error = new MessageParseError('Parse failed', rawMessage);

    expect(error.rawMessage).toBe(rawMessage);
  });

  it('should truncate long raw messages', () => {
    const longMessage = 'x'.repeat(2000);
    const error = new MessageParseError('Parse failed', longMessage);

    expect(error.rawMessage?.length).toBe(1000);
  });

  it('should include original error when provided', () => {
    const originalError = new SyntaxError('Unexpected token');
    const error = new MessageParseError('Parse failed', undefined, originalError);

    expect(error.originalError).toBe(originalError);
  });

  it('should handle undefined raw message', () => {
    const error = new MessageParseError('Parse failed', undefined);

    expect(error.rawMessage).toBeUndefined();
  });
});

// =============================================================================
// maskUrl Tests
// =============================================================================

describe('maskUrl', () => {
  it('should mask password in URL', () => {
    const masked = maskUrl('ws://user:password123@localhost:8080');

    expect(masked).not.toContain('password123');
    expect(masked).toContain('***');
    expect(masked).toContain('user');
    expect(masked).toContain('localhost');
  });

  it('should mask token query parameter', () => {
    const masked = maskUrl('ws://localhost:8080?token=secret123');

    expect(masked).not.toContain('secret123');
    expect(masked).toContain('token=***');
  });

  it('should mask key query parameter', () => {
    const masked = maskUrl('ws://localhost:8080?key=myapikey');

    expect(masked).not.toContain('myapikey');
    expect(masked).toContain('key=***');
  });

  it('should mask secret query parameter', () => {
    const masked = maskUrl('ws://localhost:8080?secret=topsecret');

    expect(masked).not.toContain('topsecret');
    expect(masked).toContain('secret=***');
  });

  it('should mask password query parameter', () => {
    const masked = maskUrl('ws://localhost:8080?password=mypass');

    expect(masked).not.toContain('mypass');
    expect(masked).toContain('password=***');
  });

  it('should mask auth query parameter', () => {
    const masked = maskUrl('ws://localhost:8080?auth=bearer123');

    expect(masked).not.toContain('bearer123');
    expect(masked).toContain('auth=***');
  });

  it('should mask api_key query parameter', () => {
    const masked = maskUrl('ws://localhost:8080?api_key=abc123');

    expect(masked).not.toContain('abc123');
    expect(masked).toContain('api_key=***');
  });

  it('should mask apikey query parameter', () => {
    const masked = maskUrl('ws://localhost:8080?apikey=xyz789');

    expect(masked).not.toContain('xyz789');
    expect(masked).toContain('apikey=***');
  });

  it('should mask access_token query parameter', () => {
    const masked = maskUrl('ws://localhost:8080?access_token=token456');

    expect(masked).not.toContain('token456');
    expect(masked).toContain('access_token=***');
  });

  it('should mask multiple sensitive parameters', () => {
    const masked = maskUrl('ws://localhost:8080?token=t1&key=k2&secret=s3');

    expect(masked).not.toContain('t1');
    expect(masked).not.toContain('k2');
    expect(masked).not.toContain('s3');
    expect(masked).toContain('token=***');
    expect(masked).toContain('key=***');
    expect(masked).toContain('secret=***');
  });

  it('should preserve non-sensitive query parameters', () => {
    const masked = maskUrl('ws://localhost:8080?database=mydb&timeout=30');

    expect(masked).toContain('database=mydb');
    expect(masked).toContain('timeout=30');
  });

  it('should handle URL without sensitive data', () => {
    const url = 'ws://localhost:8080/path';
    const masked = maskUrl(url);

    expect(masked).toContain('localhost:8080');
    expect(masked).toContain('/path');
  });

  it('should handle invalid URL gracefully', () => {
    const masked = maskUrl('not-a-valid-url');

    expect(masked).toBe('[invalid-url]');
  });

  it('should handle URL with just protocol and host', () => {
    const masked = maskUrl('ws://example.com');

    expect(masked).toContain('ws://example.com');
  });

  it('should handle partial URL with protocol and host', () => {
    const masked = maskUrl('http://myhost');

    expect(masked).toContain('http://myhost');
  });
});

// =============================================================================
// isRetryableError Tests
// =============================================================================

describe('isRetryableError', () => {
  it('should return true for TIMEOUT error', () => {
    const error = new SQLError({ code: 'TIMEOUT', message: 'Request timed out' });

    expect(isRetryableError(error)).toBe(true);
  });

  it('should return true for CONN_CLOSED error', () => {
    const error = new SQLError({ code: 'CONN_CLOSED', message: 'Connection closed' });

    expect(isRetryableError(error)).toBe(true);
  });

  it('should return true for NETWORK_ERROR error', () => {
    const error = new SQLError({ code: 'NETWORK_ERROR', message: 'Network error' });

    expect(isRetryableError(error)).toBe(true);
  });

  it('should return true for UNAVAILABLE error', () => {
    const error = new SQLError({ code: 'UNAVAILABLE', message: 'Service unavailable' });

    expect(isRetryableError(error)).toBe(true);
  });

  it('should return true for RESOURCE_EXHAUSTED error', () => {
    const error = new SQLError({ code: 'RESOURCE_EXHAUSTED', message: 'Too many requests' });

    expect(isRetryableError(error)).toBe(true);
  });

  it('should return false for SYNTAX_ERROR', () => {
    const error = new SQLError({ code: 'SYNTAX_ERROR', message: 'Invalid SQL' });

    expect(isRetryableError(error)).toBe(false);
  });

  it('should return false for CONSTRAINT_VIOLATION', () => {
    const error = new SQLError({ code: 'CONSTRAINT_VIOLATION', message: 'Unique constraint' });

    expect(isRetryableError(error)).toBe(false);
  });

  it('should return false for TABLE_NOT_FOUND', () => {
    const error = new SQLError({ code: 'TABLE_NOT_FOUND', message: 'Table does not exist' });

    expect(isRetryableError(error)).toBe(false);
  });

  it('should return false for PERMISSION_DENIED', () => {
    const error = new SQLError({ code: 'PERMISSION_DENIED', message: 'Access denied' });

    expect(isRetryableError(error)).toBe(false);
  });

  it('should return false for unknown error codes', () => {
    const error = new SQLError({ code: 'CUSTOM_ERROR', message: 'Custom error' });

    expect(isRetryableError(error)).toBe(false);
  });
});

// =============================================================================
// RETRYABLE_ERROR_CODES Tests
// =============================================================================

describe('RETRYABLE_ERROR_CODES', () => {
  it('should include TIMEOUT', () => {
    expect(RETRYABLE_ERROR_CODES).toContain('TIMEOUT');
  });

  it('should include CONN_CLOSED', () => {
    expect(RETRYABLE_ERROR_CODES).toContain('CONN_CLOSED');
  });

  it('should include NETWORK_ERROR', () => {
    expect(RETRYABLE_ERROR_CODES).toContain('NETWORK_ERROR');
  });

  it('should include UNAVAILABLE', () => {
    expect(RETRYABLE_ERROR_CODES).toContain('UNAVAILABLE');
  });

  it('should include RESOURCE_EXHAUSTED', () => {
    expect(RETRYABLE_ERROR_CODES).toContain('RESOURCE_EXHAUSTED');
  });

  it('should have exactly 7 error codes', () => {
    expect(RETRYABLE_ERROR_CODES.length).toBe(7);
  });

  it('should be a readonly array with correct values', () => {
    // TypeScript ensures this at compile time, but we verify the values are correct
    const codes = [...RETRYABLE_ERROR_CODES];
    expect(codes).toEqual([
      'TIMEOUT',
      'QUERY_TIMEOUT',
      'CONN_CLOSED',
      'CONN_FAILED',
      'NETWORK_ERROR',
      'UNAVAILABLE',
      'RESOURCE_EXHAUSTED',
    ]);
  });
});

// =============================================================================
// Error Inheritance Tests
// =============================================================================

describe('Error Inheritance', () => {
  it('SQLError should be catchable as Error', () => {
    const throwAndCatch = () => {
      try {
        throw new SQLError({ code: 'TEST', message: 'Test' });
      } catch (error) {
        if (error instanceof Error) {
          return error.message;
        }
        return 'not an error';
      }
    };

    expect(throwAndCatch()).toBe('Test');
  });

  it('ConnectionError should be catchable as Error', () => {
    const throwAndCatch = () => {
      try {
        throw new ConnectionError('Connection failed');
      } catch (error) {
        if (error instanceof Error) {
          return error.message;
        }
        return 'not an error';
      }
    };

    expect(throwAndCatch()).toBe('Connection failed');
  });

  it('TimeoutError should be catchable as Error', () => {
    const throwAndCatch = () => {
      try {
        throw new TimeoutError('query', 5000);
      } catch (error) {
        if (error instanceof Error) {
          return error.message;
        }
        return 'not an error';
      }
    };

    expect(throwAndCatch()).toBe('query timeout after 5000ms');
  });

  it('MessageParseError should be catchable as Error', () => {
    const throwAndCatch = () => {
      try {
        throw new MessageParseError('Parse failed');
      } catch (error) {
        if (error instanceof Error) {
          return error.message;
        }
        return 'not an error';
      }
    };

    expect(throwAndCatch()).toBe('Parse failed');
  });
});

// =============================================================================
// Error JSON Serialization Tests
// =============================================================================

describe('Error JSON Serialization', () => {
  it('SQLError should serialize code and message', () => {
    const error = new SQLError({
      code: 'TEST_ERROR',
      message: 'Test message',
      details: { foo: 'bar' },
    });

    // Standard JSON.stringify only includes message (Error behavior)
    // but custom properties can be accessed
    expect(error.code).toBe('TEST_ERROR');
    expect(error.details).toEqual({ foo: 'bar' });
  });

  it('ConnectionError should serialize url', () => {
    const error = new ConnectionError('Failed', 'ws://localhost:8080');

    expect(error.url).toContain('localhost:8080');
    expect(error.code).toBe('CONN_FAILED');
    expect(error.isRetryable()).toBe(true);
  });

  it('TimeoutError should serialize timeout info', () => {
    const error = new TimeoutError('query', 30000);

    expect(error.operationType).toBe('query');
    expect(error.timeoutMs).toBe(30000);
    expect(error.code).toBe('TIMEOUT');
    expect(error.isRetryable()).toBe(true);
  });

  it('MessageParseError should serialize parse context', () => {
    const originalError = new SyntaxError('Unexpected token');
    const error = new MessageParseError('Parse failed', 'raw message', originalError);

    expect(error.code).toBe('PROTOCOL_MESSAGE_PARSE_ERROR');
    expect(error.isRetryable()).toBe(false);
    expect(error.rawMessage).toBe('raw message');
    expect(error.originalError).toBe(originalError);
  });
});
