/**
 * Tests for lake.do error classes
 *
 * @module lake.do/tests/errors
 */

import { describe, it, expect } from 'vitest';
import {
  LakeError,
  ConnectionError,
  QueryError,
  TimeoutError,
  maskUrl,
} from '../errors.js';
import { ErrorCode } from '../constants.js';

describe('maskUrl', () => {
  it('masks password in URL', () => {
    const url = 'https://user:secret123@example.com/path';
    const masked = maskUrl(url);
    expect(masked).toBe('https://user:***@example.com/path');
  });

  it('masks sensitive query parameters', () => {
    const url = 'https://example.com?token=abc123&api_key=xyz789&other=keep';
    const masked = maskUrl(url);
    expect(masked).toContain('token=***');
    expect(masked).toContain('api_key=***');
    expect(masked).toContain('other=keep');
  });

  it('masks multiple sensitive parameters', () => {
    const sensitiveParams = ['token', 'key', 'secret', 'password', 'auth', 'api_key', 'apikey', 'access_token'];
    for (const param of sensitiveParams) {
      const url = `https://example.com?${param}=sensitive`;
      const masked = maskUrl(url);
      expect(masked).toContain(`${param}=***`);
    }
  });

  it('handles invalid URL gracefully', () => {
    const url = 'not-a-valid-url';
    const masked = maskUrl(url);
    expect(masked).toBe('[invalid-url]');
  });

  it('handles URL with protocol but invalid format', () => {
    const url = 'https://example.com/***invalid***';
    const masked = maskUrl(url);
    expect(masked).toBeDefined();
  });

  it('preserves URL without sensitive data', () => {
    const url = 'https://example.com/path?query=value';
    const masked = maskUrl(url);
    expect(masked).toBe('https://example.com/path?query=value');
  });
});

describe('LakeError', () => {
  it('creates error with code and message', () => {
    const error = new LakeError({
      code: 'TABLE_NOT_FOUND',
      message: 'Table "orders" does not exist',
    });

    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(LakeError);
    expect(error.name).toBe('LakeError');
    expect(error.code).toBe('TABLE_NOT_FOUND');
    expect(error.message).toBe('Table "orders" does not exist');
    expect(error.details).toBeUndefined();
  });

  it('creates error with details', () => {
    const error = new LakeError({
      code: 'INVALID_SQL',
      message: 'Syntax error at position 5',
      details: { position: 5, token: 'SELEC' },
    });

    expect(error.code).toBe('INVALID_SQL');
    expect(error.details).toEqual({ position: 5, token: 'SELEC' });
  });

  it('has correct error stack', () => {
    const error = new LakeError({
      code: 'TEST',
      message: 'Test error',
    });

    expect(error.stack).toBeDefined();
    expect(error.stack).toContain('LakeError');
  });
});

describe('ConnectionError', () => {
  it('creates connection error without URL', () => {
    const error = new ConnectionError({
      code: 'CONNECTION_ERROR',
      message: 'Connection failed',
    });

    expect(error).toBeInstanceOf(LakeError);
    expect(error).toBeInstanceOf(ConnectionError);
    expect(error.name).toBe('ConnectionError');
    expect(error.code).toBe('CONNECTION_ERROR');
    expect(error.message).toBe('Connection failed');
    expect(error.url).toBeUndefined();
  });

  it('creates connection error with URL (masked)', () => {
    const error = new ConnectionError(
      { code: 'CONNECTION_ERROR', message: 'Connection failed' },
      'https://user:secret@example.com'
    );

    expect(error.url).toBe('https://user:***@example.com/');
    expect(error.message).toContain('url: https://user:***@example.com');
  });

  it('creates via static create method', () => {
    const error = ConnectionError.create(
      ErrorCode.CONNECTION_ERROR,
      'Connection failed',
      { attempt: 3 },
      'https://example.com'
    );

    expect(error).toBeInstanceOf(ConnectionError);
    expect(error.code).toBe(ErrorCode.CONNECTION_ERROR);
    expect(error.details).toEqual({ attempt: 3 });
    expect(error.url).toBeDefined();
  });

  it('creates connection closed error via static method', () => {
    const error = ConnectionError.closed();

    expect(error.code).toBe(ErrorCode.CONNECTION_CLOSED);
    expect(error.message).toContain('Connection closed');
  });

  it('creates connection closed error with custom message', () => {
    const error = ConnectionError.closed('WebSocket connection terminated');

    expect(error.message).toContain('WebSocket connection terminated');
  });

  it('creates not connected error via static method', () => {
    const error = ConnectionError.notConnected();

    expect(error.code).toBe(ErrorCode.NOT_CONNECTED);
    expect(error.message).toContain('WebSocket is not connected');
  });

  it('creates connection failed error via static method', () => {
    const error = ConnectionError.failed('Server unreachable', 'https://example.com');

    expect(error.code).toBe(ErrorCode.CONNECTION_ERROR);
    expect(error.message).toContain('Server unreachable');
    expect(error.url).toBeDefined();
  });
});

describe('QueryError', () => {
  it('creates query error', () => {
    const error = new QueryError({
      code: 'INVALID_SQL',
      message: 'Invalid SQL syntax',
    });

    expect(error).toBeInstanceOf(LakeError);
    expect(error).toBeInstanceOf(QueryError);
    expect(error.name).toBe('QueryError');
    expect(error.code).toBe('INVALID_SQL');
  });

  it('creates via static create method', () => {
    const error = QueryError.create(
      ErrorCode.INVALID_SQL,
      'Unexpected token',
      { position: 10 }
    );

    expect(error).toBeInstanceOf(QueryError);
    expect(error.details).toEqual({ position: 10 });
  });

  it('creates table not found error via static method', () => {
    const error = QueryError.tableNotFound('orders');

    expect(error.code).toBe(ErrorCode.TABLE_NOT_FOUND);
    expect(error.message).toContain('Table "orders" does not exist');
    expect(error.details).toEqual({ tableName: 'orders' });
  });

  it('creates invalid SQL error via static method', () => {
    const error = QueryError.invalidSql('Unexpected keyword', { position: 5 });

    expect(error.code).toBe(ErrorCode.INVALID_SQL);
    expect(error.message).toContain('Unexpected keyword');
    expect(error.details).toEqual({ position: 5 });
  });

  it('creates query timeout error via static method', () => {
    const error = QueryError.timeout();

    expect(error.code).toBe(ErrorCode.QUERY_TIMEOUT);
    expect(error.message).toContain('Query execution timed out');
  });

  it('creates query timeout error with custom message', () => {
    const error = QueryError.timeout('Query exceeded 30s limit');

    expect(error.message).toContain('Query exceeded 30s limit');
  });
});

describe('TimeoutError', () => {
  it('creates timeout error for RPC method', () => {
    const error = new TimeoutError('query');

    expect(error).toBeInstanceOf(LakeError);
    expect(error).toBeInstanceOf(TimeoutError);
    expect(error.name).toBe('TimeoutError');
    expect(error.code).toBe(ErrorCode.TIMEOUT);
    expect(error.method).toBe('query');
    expect(error.message).toContain('Request timeout: query');
    expect(error.details).toEqual({ method: 'query' });
  });

  it('creates timeout error for different methods', () => {
    const methods = ['ping', 'listSnapshots', 'compact', 'getMetadata'];

    for (const method of methods) {
      const error = new TimeoutError(method);
      expect(error.method).toBe(method);
      expect(error.message).toContain(method);
    }
  });
});

describe('Error Inheritance Chain', () => {
  it('ConnectionError extends LakeError extends Error', () => {
    const error = new ConnectionError({ code: 'TEST', message: 'Test' });

    expect(error instanceof Error).toBe(true);
    expect(error instanceof LakeError).toBe(true);
    expect(error instanceof ConnectionError).toBe(true);
    expect(error instanceof QueryError).toBe(false);
    expect(error instanceof TimeoutError).toBe(false);
  });

  it('QueryError extends LakeError extends Error', () => {
    const error = new QueryError({ code: 'TEST', message: 'Test' });

    expect(error instanceof Error).toBe(true);
    expect(error instanceof LakeError).toBe(true);
    expect(error instanceof QueryError).toBe(true);
    expect(error instanceof ConnectionError).toBe(false);
    expect(error instanceof TimeoutError).toBe(false);
  });

  it('TimeoutError extends LakeError extends Error', () => {
    const error = new TimeoutError('test');

    expect(error instanceof Error).toBe(true);
    expect(error instanceof LakeError).toBe(true);
    expect(error instanceof TimeoutError).toBe(true);
    expect(error instanceof ConnectionError).toBe(false);
    expect(error instanceof QueryError).toBe(false);
  });
});
