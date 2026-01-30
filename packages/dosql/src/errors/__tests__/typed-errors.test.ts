/**
 * Typed Errors Tests
 *
 * Tests for PlannerError, ExecutorError, ParserError, StorageError
 * Following TDD: these tests are written first, before implementation
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';
import {
  PlannerError,
  ExecutorError,
  ParserError,
  StorageError,
  PlannerErrorCode,
  ExecutorErrorCode,
  ParserErrorCode,
  StorageErrorCode,
  createNoTablesToJoinError,
  createJoinOrderFailedError,
  createUnknownPlanTypeError,
  createQueryTimeoutError,
  createEmptySqlError,
  createUnsupportedOperationError,
  createInvalidSnapshotIdError,
  createStorageReadError,
  createStorageWriteError,
} from '../index.js';
import { ErrorCategory, deserializeError, type SerializedError } from '../base.js';

// =============================================================================
// PLANNER ERROR TESTS
// =============================================================================

describe('PlannerError', () => {
  describe('basic functionality', () => {
    it('should create error with code and message', () => {
      const error = new PlannerError(
        PlannerErrorCode.NO_TABLES,
        'No tables to join'
      );

      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(PlannerError);
      expect(error.code).toBe('PLANNER_NO_TABLES');
      expect(error.message).toBe('No tables to join');
      expect(error.name).toBe('PlannerError');
      expect(error.category).toBe(ErrorCategory.EXECUTION);
    });

    it('should include query context', () => {
      const error = new PlannerError(
        PlannerErrorCode.NO_TABLES,
        'No tables to join',
        {
          context: {
            sql: 'SELECT * FROM',
            metadata: { tableCount: 0 },
          },
        }
      );

      expect(error.context?.sql).toBe('SELECT * FROM');
      expect(error.context?.metadata?.tableCount).toBe(0);
    });

    it('should support cause chain', () => {
      const cause = new Error('Original error');
      const error = new PlannerError(
        PlannerErrorCode.JOIN_ORDER_FAILED,
        'Failed to find join order',
        { cause }
      );

      expect(error.cause).toBe(cause);
    });

    it('should include tables involved in error', () => {
      const error = new PlannerError(
        PlannerErrorCode.JOIN_ORDER_FAILED,
        'Failed to find join order',
        {
          context: {
            metadata: {
              tables: ['users', 'orders', 'products'],
            },
          },
        }
      );

      expect(error.context?.metadata?.tables).toEqual(['users', 'orders', 'products']);
    });

    it('should not be retryable by default', () => {
      const error = new PlannerError(PlannerErrorCode.NO_TABLES, 'No tables');
      expect(error.isRetryable()).toBe(false);
    });
  });

  describe('factory functions', () => {
    it('should create no tables error with context', () => {
      const error = createNoTablesToJoinError('SELECT * FROM');

      expect(error.code).toBe('PLANNER_NO_TABLES');
      expect(error.message).toBe('No tables to join');
      expect(error.context?.sql).toBe('SELECT * FROM');
    });

    it('should create join order failed error with tables', () => {
      const tables = ['users', 'orders'];
      const error = createJoinOrderFailedError(tables, 'SELECT * FROM users JOIN orders');

      expect(error.code).toBe('PLANNER_JOIN_ORDER_FAILED');
      expect(error.message).toContain('Failed to find join order');
      expect(error.context?.metadata?.tables).toEqual(tables);
    });
  });

  describe('serialization', () => {
    it('should serialize to JSON', () => {
      const error = new PlannerError(
        PlannerErrorCode.NO_TABLES,
        'No tables to join',
        {
          context: {
            sql: 'SELECT * FROM',
            metadata: { tableCount: 0 },
          },
        }
      );

      const json = error.toJSON();

      expect(json.name).toBe('PlannerError');
      expect(json.code).toBe('PLANNER_NO_TABLES');
      expect(json.message).toBe('No tables to join');
      expect(json.context?.sql).toBe('SELECT * FROM');
      expect(typeof json.timestamp).toBe('number');
    });

    it('should deserialize from JSON', () => {
      const json: SerializedError = {
        name: 'PlannerError',
        code: 'PLANNER_NO_TABLES',
        message: 'No tables to join',
        timestamp: Date.now(),
        context: { sql: 'SELECT * FROM' },
      };

      const error = deserializeError(json);

      expect(error.code).toBe('PLANNER_NO_TABLES');
      expect(error.message).toBe('No tables to join');
      expect(error.context?.sql).toBe('SELECT * FROM');
    });
  });

  describe('user-friendly messages', () => {
    it('should provide user message for NO_TABLES', () => {
      const error = new PlannerError(PlannerErrorCode.NO_TABLES, 'No tables');
      expect(error.toUserMessage()).toContain('query');
    });

    it('should provide user message for JOIN_ORDER_FAILED', () => {
      const error = new PlannerError(PlannerErrorCode.JOIN_ORDER_FAILED, 'Failed');
      expect(error.toUserMessage()).toContain('join');
    });
  });
});

// =============================================================================
// EXECUTOR ERROR TESTS
// =============================================================================

describe('ExecutorError', () => {
  describe('basic functionality', () => {
    it('should create error with code and message', () => {
      const error = new ExecutorError(
        ExecutorErrorCode.UNKNOWN_PLAN_TYPE,
        'Unknown plan type: foo'
      );

      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(ExecutorError);
      expect(error.code).toBe('EXECUTOR_UNKNOWN_PLAN_TYPE');
      expect(error.message).toBe('Unknown plan type: foo');
      expect(error.name).toBe('ExecutorError');
      expect(error.category).toBe(ErrorCategory.EXECUTION);
    });

    it('should include operation details', () => {
      const error = new ExecutorError(
        ExecutorErrorCode.QUERY_TIMEOUT,
        'Query timeout after 5000ms',
        {
          context: {
            metadata: {
              operation: 'execute',
              planType: 'scan',
              elapsed: 5000,
              timeout: 5000,
            },
          },
        }
      );

      expect(error.context?.metadata?.operation).toBe('execute');
      expect(error.context?.metadata?.elapsed).toBe(5000);
    });

    it('should be retryable for timeout errors', () => {
      const error = new ExecutorError(
        ExecutorErrorCode.QUERY_TIMEOUT,
        'Query timeout'
      );
      expect(error.isRetryable()).toBe(true);
    });

    it('should not be retryable for unknown plan type', () => {
      const error = new ExecutorError(
        ExecutorErrorCode.UNKNOWN_PLAN_TYPE,
        'Unknown plan type'
      );
      expect(error.isRetryable()).toBe(false);
    });
  });

  describe('factory functions', () => {
    it('should create unknown plan type error', () => {
      const error = createUnknownPlanTypeError('customScan', 'SELECT * FROM users');

      expect(error.code).toBe('EXECUTOR_UNKNOWN_PLAN_TYPE');
      expect(error.message).toContain('customScan');
      expect(error.context?.sql).toBe('SELECT * FROM users');
    });

    it('should create query timeout error', () => {
      const error = createQueryTimeoutError(5000, 3000, 'SELECT * FROM large_table');

      expect(error.code).toBe('EXECUTOR_QUERY_TIMEOUT');
      expect(error.message).toContain('5000');
      expect(error.context?.metadata?.elapsed).toBe(5000);
      expect(error.context?.metadata?.timeout).toBe(3000);
      expect(error.isRetryable()).toBe(true);
    });
  });

  describe('serialization', () => {
    it('should serialize to JSON with operation context', () => {
      const error = new ExecutorError(
        ExecutorErrorCode.QUERY_TIMEOUT,
        'Query timeout',
        {
          context: {
            sql: 'SELECT * FROM users',
            metadata: { elapsed: 5000 },
          },
        }
      );

      const json = error.toJSON();

      expect(json.name).toBe('ExecutorError');
      expect(json.code).toBe('EXECUTOR_QUERY_TIMEOUT');
      expect(json.context?.sql).toBe('SELECT * FROM users');
    });

    it('should deserialize from JSON', () => {
      const json: SerializedError = {
        name: 'ExecutorError',
        code: 'EXECUTOR_QUERY_TIMEOUT',
        message: 'Query timeout after 5000ms',
        timestamp: Date.now(),
        context: { metadata: { elapsed: 5000 } },
      };

      const error = deserializeError(json);

      expect(error.code).toBe('EXECUTOR_QUERY_TIMEOUT');
      expect(error.message).toBe('Query timeout after 5000ms');
    });
  });
});

// =============================================================================
// PARSER ERROR TESTS
// =============================================================================

describe('ParserError', () => {
  describe('basic functionality', () => {
    it('should create error with code and message', () => {
      const error = new ParserError(
        ParserErrorCode.EMPTY_SQL,
        'Empty SQL query'
      );

      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(ParserError);
      expect(error.code).toBe('PARSER_EMPTY_SQL');
      expect(error.message).toBe('Empty SQL query');
      expect(error.name).toBe('ParserError');
      expect(error.category).toBe(ErrorCategory.VALIDATION);
    });

    it('should include SQL and position context', () => {
      const error = new ParserError(
        ParserErrorCode.UNSUPPORTED_OPERATION,
        'Unsupported operation: MERGE',
        {
          context: {
            sql: 'MERGE INTO users USING ...',
            metadata: {
              operation: 'MERGE',
              position: 0,
            },
          },
        }
      );

      expect(error.context?.sql).toBe('MERGE INTO users USING ...');
      expect(error.context?.metadata?.operation).toBe('MERGE');
    });

    it('should not be retryable', () => {
      const error = new ParserError(ParserErrorCode.EMPTY_SQL, 'Empty SQL');
      expect(error.isRetryable()).toBe(false);
    });
  });

  describe('factory functions', () => {
    it('should create empty SQL error', () => {
      const error = createEmptySqlError();

      expect(error.code).toBe('PARSER_EMPTY_SQL');
      expect(error.message).toBe('Empty SQL query');
    });

    it('should create unsupported operation error', () => {
      const error = createUnsupportedOperationError('MERGE', 'MERGE INTO users');

      expect(error.code).toBe('PARSER_UNSUPPORTED_OPERATION');
      expect(error.message).toContain('MERGE');
      expect(error.context?.sql).toBe('MERGE INTO users');
    });
  });

  describe('serialization', () => {
    it('should serialize and deserialize properly', () => {
      const original = new ParserError(
        ParserErrorCode.UNSUPPORTED_OPERATION,
        'Unsupported: MERGE',
        { context: { sql: 'MERGE ...' } }
      );

      const json = original.toJSON();
      const deserialized = deserializeError(json);

      expect(deserialized.code).toBe('PARSER_UNSUPPORTED_OPERATION');
      expect(deserialized.message).toBe('Unsupported: MERGE');
    });
  });
});

// =============================================================================
// STORAGE ERROR TESTS
// =============================================================================

describe('StorageError', () => {
  describe('basic functionality', () => {
    it('should create error with code and message', () => {
      const error = new StorageError(
        StorageErrorCode.INVALID_SNAPSHOT_ID,
        'Invalid snapshot ID: abc'
      );

      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(StorageError);
      expect(error.code).toBe('STORAGE_INVALID_SNAPSHOT_ID');
      expect(error.message).toBe('Invalid snapshot ID: abc');
      expect(error.name).toBe('StorageError');
      expect(error.category).toBe(ErrorCategory.RESOURCE);
    });

    it('should include storage context', () => {
      const error = new StorageError(
        StorageErrorCode.READ_FAILED,
        'Read failed',
        {
          context: {
            metadata: {
              path: '/data/page_001.bin',
              operation: 'read',
            },
          },
        }
      );

      expect(error.context?.metadata?.path).toBe('/data/page_001.bin');
      expect(error.context?.metadata?.operation).toBe('read');
    });

    it('should be retryable for transient errors', () => {
      const readError = new StorageError(StorageErrorCode.READ_FAILED, 'Read failed');
      const writeError = new StorageError(StorageErrorCode.WRITE_FAILED, 'Write failed');

      expect(readError.isRetryable()).toBe(true);
      expect(writeError.isRetryable()).toBe(true);
    });

    it('should not be retryable for invalid ID errors', () => {
      const error = new StorageError(StorageErrorCode.INVALID_SNAPSHOT_ID, 'Invalid ID');
      expect(error.isRetryable()).toBe(false);
    });
  });

  describe('factory functions', () => {
    it('should create invalid snapshot ID error', () => {
      const error = createInvalidSnapshotIdError('abc123');

      expect(error.code).toBe('STORAGE_INVALID_SNAPSHOT_ID');
      expect(error.message).toContain('abc123');
    });

    it('should create storage read error with path', () => {
      const cause = new Error('ENOENT');
      const error = createStorageReadError('/data/page.bin', cause);

      expect(error.code).toBe('STORAGE_READ_FAILED');
      expect(error.message).toContain('/data/page.bin');
      expect(error.cause).toBe(cause);
      expect(error.context?.metadata?.path).toBe('/data/page.bin');
    });

    it('should create storage write error with path', () => {
      const cause = new Error('ENOSPC');
      const error = createStorageWriteError('/data/page.bin', cause);

      expect(error.code).toBe('STORAGE_WRITE_FAILED');
      expect(error.message).toContain('/data/page.bin');
      expect(error.cause).toBe(cause);
      expect(error.isRetryable()).toBe(true);
    });
  });

  describe('serialization', () => {
    it('should serialize and deserialize properly', () => {
      const original = new StorageError(
        StorageErrorCode.READ_FAILED,
        'Read failed: /data/page.bin',
        { context: { metadata: { path: '/data/page.bin' } } }
      );

      const json = original.toJSON();
      const deserialized = deserializeError(json);

      expect(deserialized.code).toBe('STORAGE_READ_FAILED');
      expect(deserialized.message).toBe('Read failed: /data/page.bin');
    });
  });
});

// =============================================================================
// ERROR CODE TESTS
// =============================================================================

describe('Error Codes', () => {
  it('should have standardized planner error codes', () => {
    expect(PlannerErrorCode.NO_TABLES).toBe('PLANNER_NO_TABLES');
    expect(PlannerErrorCode.JOIN_ORDER_FAILED).toBe('PLANNER_JOIN_ORDER_FAILED');
    expect(PlannerErrorCode.INVALID_PLAN).toBe('PLANNER_INVALID_PLAN');
  });

  it('should have standardized executor error codes', () => {
    expect(ExecutorErrorCode.UNKNOWN_PLAN_TYPE).toBe('EXECUTOR_UNKNOWN_PLAN_TYPE');
    expect(ExecutorErrorCode.QUERY_TIMEOUT).toBe('EXECUTOR_QUERY_TIMEOUT');
    expect(ExecutorErrorCode.OPERATOR_ERROR).toBe('EXECUTOR_OPERATOR_ERROR');
  });

  it('should have standardized parser error codes', () => {
    expect(ParserErrorCode.EMPTY_SQL).toBe('PARSER_EMPTY_SQL');
    expect(ParserErrorCode.UNSUPPORTED_OPERATION).toBe('PARSER_UNSUPPORTED_OPERATION');
    expect(ParserErrorCode.INVALID_STATEMENT).toBe('PARSER_INVALID_STATEMENT');
  });

  it('should have standardized storage error codes', () => {
    expect(StorageErrorCode.INVALID_SNAPSHOT_ID).toBe('STORAGE_INVALID_SNAPSHOT_ID');
    expect(StorageErrorCode.READ_FAILED).toBe('STORAGE_READ_FAILED');
    expect(StorageErrorCode.WRITE_FAILED).toBe('STORAGE_WRITE_FAILED');
  });
});

// =============================================================================
// RPC SERIALIZATION TESTS
// =============================================================================

describe('RPC Serialization', () => {
  it('should serialize error with cause chain', () => {
    const rootCause = new Error('Network failure');
    const storageCause = new StorageError(
      StorageErrorCode.READ_FAILED,
      'Read failed',
      { cause: rootCause }
    );
    const executorError = new ExecutorError(
      ExecutorErrorCode.OPERATOR_ERROR,
      'Scan operator failed',
      { cause: storageCause }
    );

    const json = executorError.toJSON();

    expect(json.cause).toBeDefined();
    expect(json.cause?.code).toBe('STORAGE_READ_FAILED');
    expect(json.cause?.cause).toBeDefined();
  });

  it('should include all context in serialized form', () => {
    const error = new PlannerError(
      PlannerErrorCode.JOIN_ORDER_FAILED,
      'Failed to find join order',
      {
        context: {
          sql: 'SELECT * FROM a JOIN b JOIN c',
          transactionId: 'tx-123',
          requestId: 'req-456',
          metadata: {
            tables: ['a', 'b', 'c'],
            joinConditions: 2,
          },
        },
      }
    );

    const json = error.toJSON();

    expect(json.context?.sql).toBe('SELECT * FROM a JOIN b JOIN c');
    expect(json.context?.transactionId).toBe('tx-123');
    expect(json.context?.requestId).toBe('req-456');
    expect(json.context?.metadata?.tables).toEqual(['a', 'b', 'c']);
  });

  it('should preserve error type through serialization', () => {
    const errors = [
      new PlannerError(PlannerErrorCode.NO_TABLES, 'No tables'),
      new ExecutorError(ExecutorErrorCode.QUERY_TIMEOUT, 'Timeout'),
      new ParserError(ParserErrorCode.EMPTY_SQL, 'Empty'),
      new StorageError(StorageErrorCode.READ_FAILED, 'Read failed'),
    ];

    for (const original of errors) {
      const json = original.toJSON();
      const deserialized = deserializeError(json);

      expect(deserialized.code).toBe(original.code);
      expect(deserialized.message).toBe(original.message);
    }
  });
});

// =============================================================================
// STRUCTURED LOGGING TESTS
// =============================================================================

describe('Structured Logging', () => {
  it('should format error for logging', () => {
    const error = new ExecutorError(
      ExecutorErrorCode.QUERY_TIMEOUT,
      'Query timeout after 5000ms',
      {
        context: {
          sql: 'SELECT * FROM users',
          transactionId: 'tx-123',
        },
      }
    );

    const logEntry = error.toLogEntry();

    expect(logEntry.level).toBe('error');
    expect(logEntry.timestamp).toBeDefined();
    expect(logEntry.error.name).toBe('ExecutorError');
    expect(logEntry.error.code).toBe('EXECUTOR_QUERY_TIMEOUT');
    expect(logEntry.metadata.category).toBe(ErrorCategory.EXECUTION);
    expect(logEntry.metadata.sql).toBe('SELECT * FROM users');
    expect(logEntry.metadata.transactionId).toBe('tx-123');
  });
});
