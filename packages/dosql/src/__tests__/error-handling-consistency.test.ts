/**
 * Error Handling Standardization Tests - GREEN Phase TDD
 *
 * Issue: sql-zhy.12 - Error Handling Standardization
 *
 * These tests verify the UNIFIED error handling patterns across DoSQL.
 * The error hierarchy now provides:
 * - Required error codes for all errors
 * - Timestamps
 * - Context preservation
 * - Serialization/deserialization for RPC
 * - Recovery hints
 * - Structured logging support
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';
import { Database, createDatabase } from '../database.js';
import { coerceValue } from '../statement/binding.js';
import { FSXError, FSXErrorCode } from '../fsx/types.js';
import { R2Error, R2ErrorCode } from '../fsx/r2-errors.js';
import { TransactionError, TransactionErrorCode } from '../transaction/types.js';

// Import from the new unified errors module
import {
  DoSQLError,
  AggregateDoSQLError,
  DatabaseError,
  DatabaseErrorCode,
  StatementError,
  StatementErrorCode,
  BindingError,
  BindingErrorCode,
  SQLSyntaxError,
  SyntaxErrorCode,
  UnexpectedTokenError,
  UnexpectedEOFError,
  createClosedDatabaseError,
  createSavepointNotFoundError,
  createFinalizedStatementError,
  createTableNotFoundError,
  createMissingNamedParamError,
  createInvalidTypeError,
  createCountMismatchError,
  deserializeError,
  ErrorCategory,
} from '../errors/index.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Standard error interface that all DoSQL errors should implement
 */
interface StandardError {
  name: string;
  message: string;
  code: string;
  stack?: string;
  timestamp?: number;
  context?: {
    requestId?: string;
    transactionId?: string;
    sql?: string;
  };
  cause?: Error;
}

/**
 * Helper to check if an error follows the standard interface
 */
function isStandardError(error: unknown): error is StandardError {
  return (
    error !== null &&
    typeof error === 'object' &&
    'name' in error &&
    'message' in error &&
    'code' in error &&
    typeof (error as StandardError).code === 'string'
  );
}

// =============================================================================
// 1. THROW VS RETURN PATTERN CONSISTENCY
// =============================================================================

describe('Error Pattern Consistency: Throw vs Return', () => {
  describe('Database class methods', () => {
    it('Database.prepare() throws on closed database', () => {
      const db = createDatabase();
      db.close();

      expect(() => db.prepare('SELECT 1')).toThrow(DatabaseError);
    });

    it('thrown DatabaseError has standardized code', () => {
      const db = createDatabase();
      db.close();

      let caughtError: DatabaseError | undefined;
      try {
        db.prepare('SELECT 1');
      } catch (e) {
        caughtError = e as DatabaseError;
      }

      expect(caughtError).toBeInstanceOf(DatabaseError);
      expect(caughtError!.code).toBe(DatabaseErrorCode.CLOSED);
    });

    it('thrown errors extend DoSQLError base class', () => {
      const db = createDatabase();
      db.close();

      let caughtError: Error | undefined;
      try {
        db.prepare('SELECT 1');
      } catch (e) {
        caughtError = e as Error;
      }

      expect(caughtError).toBeInstanceOf(DoSQLError);
    });
  });
});

// =============================================================================
// 2. ERROR CODES STANDARDIZATION
// =============================================================================

describe('Error Code Standardization', () => {
  describe('DatabaseError', () => {
    it('DatabaseError requires error code', () => {
      const error = new DatabaseError(DatabaseErrorCode.CLOSED, 'Database is closed');

      expect(error.code).toBe('DB_CLOSED');
      expect(error.code).toBeDefined();
      expect(typeof error.code).toBe('string');
      expect(error.code.length).toBeGreaterThan(0);
    });

    it('should use consistent error code format: CATEGORY_SPECIFIC', () => {
      const db = createDatabase();
      db.close();

      let caughtError: DatabaseError | undefined;
      try {
        db.prepare('SELECT 1');
      } catch (e) {
        caughtError = e as DatabaseError;
      }

      expect(caughtError).toBeInstanceOf(DatabaseError);
      // Expected format: DB_CLOSED, DB_READ_ONLY, etc.
      expect(caughtError!.code).toMatch(/^DB_[A-Z_]+$/);
    });

    it('has all expected error codes', () => {
      expect(DatabaseErrorCode.CLOSED).toBe('DB_CLOSED');
      expect(DatabaseErrorCode.READ_ONLY).toBe('DB_READ_ONLY');
      expect(DatabaseErrorCode.NOT_FOUND).toBe('DB_NOT_FOUND');
      expect(DatabaseErrorCode.CONSTRAINT_VIOLATION).toBe('DB_CONSTRAINT');
      expect(DatabaseErrorCode.TIMEOUT).toBe('DB_TIMEOUT');
    });
  });

  describe('StatementError', () => {
    it('StatementError has error code', () => {
      const error = new StatementError(
        StatementErrorCode.SYNTAX_ERROR,
        'Invalid SQL',
        'SELECT * FORM users'
      );

      expect(error.code).toBe('STMT_SYNTAX');
      expect(error.sql).toBe('SELECT * FORM users');
    });

    it('has all expected error codes', () => {
      expect(StatementErrorCode.FINALIZED).toBe('STMT_FINALIZED');
      expect(StatementErrorCode.SYNTAX_ERROR).toBe('STMT_SYNTAX');
      expect(StatementErrorCode.EXECUTION_ERROR).toBe('STMT_EXECUTION');
      expect(StatementErrorCode.TABLE_NOT_FOUND).toBe('STMT_TABLE_NOT_FOUND');
    });
  });

  describe('BindingError', () => {
    it('BindingError has error code', () => {
      const error = new BindingError(BindingErrorCode.MISSING_PARAM, 'Missing parameter');

      expect(error.name).toBe('BindingError');
      expect(error.code).toBe('BIND_MISSING_PARAM');
    });

    it('should include error code when coerceValue fails', () => {
      let error: Error | undefined;
      try {
        coerceValue(Symbol('test'));
      } catch (e) {
        error = e as Error;
      }

      expect(error).toBeInstanceOf(BindingError);
      expect((error as BindingError).code).toBe('BIND_INVALID_TYPE');
    });

    it('has all expected error codes', () => {
      expect(BindingErrorCode.MISSING_PARAM).toBe('BIND_MISSING_PARAM');
      expect(BindingErrorCode.TYPE_MISMATCH).toBe('BIND_TYPE_MISMATCH');
      expect(BindingErrorCode.INVALID_TYPE).toBe('BIND_INVALID_TYPE');
      expect(BindingErrorCode.COUNT_MISMATCH).toBe('BIND_COUNT_MISMATCH');
    });
  });

  describe('SQLSyntaxError', () => {
    it('SQLSyntaxError has location and error code', () => {
      const error = new SQLSyntaxError(
        SyntaxErrorCode.UNEXPECTED_TOKEN,
        'Unexpected token',
        { line: 1, column: 5, offset: 4 }
      );

      expect(error.location?.line).toBe(1);
      expect(error.location?.column).toBe(5);
      expect(error.code).toBe('SYNTAX_UNEXPECTED_TOKEN');
    });

    it('UnexpectedTokenError has proper code', () => {
      const error = new UnexpectedTokenError('FORM', 'FROM');

      expect(error.code).toBe('SYNTAX_UNEXPECTED_TOKEN');
      expect(error.token).toBe('FORM');
      expect(error.expected).toBe('FROM');
    });

    it('has all expected error codes', () => {
      expect(SyntaxErrorCode.UNEXPECTED_TOKEN).toBe('SYNTAX_UNEXPECTED_TOKEN');
      expect(SyntaxErrorCode.UNEXPECTED_EOF).toBe('SYNTAX_UNEXPECTED_EOF');
      expect(SyntaxErrorCode.GENERAL).toBe('SYNTAX_ERROR');
    });
  });

  describe('Existing good patterns', () => {
    it('TransactionError uses proper error code enum', () => {
      const error = new TransactionError(TransactionErrorCode.DEADLOCK, 'Deadlock detected');

      expect(error.code).toBe('TXN_DEADLOCK');
      expect(Object.values(TransactionErrorCode)).toContain(error.code);
    });

    it('FSXError uses proper error code enum', () => {
      const error = new FSXError(FSXErrorCode.NOT_FOUND, 'File not found', '/path/to/file');

      expect(error.code).toBe('FSX_NOT_FOUND');
      expect(Object.values(FSXErrorCode)).toContain(error.code);
    });
  });
});

// =============================================================================
// 3. ERROR TYPE HIERARCHY
// =============================================================================

describe('Error Type Hierarchy', () => {
  it('all DoSQL errors extend DoSQLError base class', () => {
    const dbError = new DatabaseError(DatabaseErrorCode.CLOSED, 'test');
    const stmtError = new StatementError(StatementErrorCode.FINALIZED, 'test');
    const bindError = new BindingError(BindingErrorCode.MISSING_PARAM, 'test');
    const syntaxError = new SQLSyntaxError(SyntaxErrorCode.GENERAL, 'test');

    expect(dbError).toBeInstanceOf(DoSQLError);
    expect(stmtError).toBeInstanceOf(DoSQLError);
    expect(bindError).toBeInstanceOf(DoSQLError);
    expect(syntaxError).toBeInstanceOf(DoSQLError);
  });

  it('all DoSQL errors have timestamp', () => {
    const before = Date.now();
    const error = new DatabaseError(DatabaseErrorCode.CLOSED, 'test');
    const after = Date.now();

    expect(error.timestamp).toBeGreaterThanOrEqual(before);
    expect(error.timestamp).toBeLessThanOrEqual(after);
  });

  it('all DoSQL errors have error category', () => {
    const dbError = new DatabaseError(DatabaseErrorCode.CLOSED, 'test');
    const bindError = new BindingError(BindingErrorCode.MISSING_PARAM, 'test');
    const syntaxError = new SQLSyntaxError(SyntaxErrorCode.GENERAL, 'test');

    expect(dbError.category).toBe(ErrorCategory.EXECUTION);
    expect(bindError.category).toBe(ErrorCategory.VALIDATION);
    expect(syntaxError.category).toBe(ErrorCategory.VALIDATION);
  });

  it('AggregateDoSQLError aggregates multiple errors', () => {
    const errors = [
      new StatementError(StatementErrorCode.SYNTAX_ERROR, 'Error 1'),
      new StatementError(StatementErrorCode.EXECUTION_ERROR, 'Error 2'),
    ];

    const aggregated = new AggregateDoSQLError(errors);

    expect(aggregated.errors.length).toBe(2);
    expect(aggregated.code).toBe('AGGREGATE_ERROR');
  });
});

// =============================================================================
// 4. ERROR SERIALIZATION FOR RPC/API
// =============================================================================

describe('Error Serialization', () => {
  it('errors have toJSON method for RPC responses', () => {
    const error = new DatabaseError(DatabaseErrorCode.CLOSED, 'Database is closed');

    const serialized = error.toJSON();

    expect(serialized).toHaveProperty('name', 'DatabaseError');
    expect(serialized).toHaveProperty('code', 'DB_CLOSED');
    expect(serialized).toHaveProperty('message');
    expect(serialized).toHaveProperty('timestamp');
  });

  it('serialized errors include context when provided', () => {
    const error = new StatementError(
      StatementErrorCode.TABLE_NOT_FOUND,
      'Table not found',
      'SELECT * FROM users'
    );

    const serialized = error.toJSON();

    expect(serialized.context?.sql).toBe('SELECT * FROM users');
  });

  it('should deserialize RPC error responses back to error objects', () => {
    const json = {
      name: 'DatabaseError',
      code: 'DB_CLOSED',
      message: 'Database is closed',
      timestamp: Date.now(),
    };

    const error = deserializeError(json);

    expect(error).toBeInstanceOf(DoSQLError);
    expect(error.code).toBe('DB_CLOSED');
    expect(error.message).toBe('Database is closed');
  });

  it('DatabaseError has fromJSON static method', () => {
    const json = {
      name: 'DatabaseError',
      code: 'DB_CLOSED',
      message: 'Database is closed',
      timestamp: Date.now(),
    };

    const error = DatabaseError.fromJSON(json);

    expect(error).toBeInstanceOf(DatabaseError);
    expect(error.code).toBe('DB_CLOSED');
  });
});

// =============================================================================
// 5. ERROR RECOVERY AND CONTEXT
// =============================================================================

describe('Error Context and Recovery', () => {
  it('should include recovery hints in errors', () => {
    const db = createDatabase();
    db.close();

    let error: DatabaseError | undefined;
    try {
      db.prepare('SELECT 1');
    } catch (e) {
      error = e as DatabaseError;
    }

    expect(error!.recoveryHint).toBeDefined();
    expect(error!.recoveryHint).toContain('createDatabase');
  });

  it('errors can have context attached', () => {
    const error = new StatementError(
      StatementErrorCode.EXECUTION_ERROR,
      'Execution failed',
      'INSERT INTO users (id) VALUES (1)'
    );

    error.withContext({
      transactionId: 'txn_123',
      requestId: 'req_456',
    });

    expect(error.context?.transactionId).toBe('txn_123');
    expect(error.context?.requestId).toBe('req_456');
    expect(error.context?.sql).toBe('INSERT INTO users (id) VALUES (1)');
  });

  it('should support structured logging format', () => {
    const error = new DatabaseError(DatabaseErrorCode.CLOSED, 'Database is closed');

    const logEntry = error.toLogEntry();

    expect(logEntry).toHaveProperty('level', 'error');
    expect(logEntry).toHaveProperty('timestamp');
    expect(logEntry).toHaveProperty('error');
    expect(logEntry.error).toHaveProperty('name', 'DatabaseError');
    expect(logEntry.error).toHaveProperty('code', 'DB_CLOSED');
    expect(logEntry).toHaveProperty('metadata');
  });
});

// =============================================================================
// 6. ERROR HANDLING BEST PRACTICES
// =============================================================================

describe('Error Handling Best Practices', () => {
  it('R2Error follows good practices with isRetryable()', () => {
    const timeoutError = new R2Error(R2ErrorCode.TIMEOUT, 'Timeout');
    const permissionError = new R2Error(R2ErrorCode.PERMISSION_DENIED, 'Forbidden');

    expect(timeoutError.isRetryable()).toBe(true);
    expect(permissionError.isRetryable()).toBe(false);
  });

  it('R2Error has user-friendly message method', () => {
    const error = new R2Error(R2ErrorCode.BUCKET_NOT_BOUND, 'Not bound');

    const userMessage = error.toUserMessage();

    expect(userMessage).not.toContain('R2_BUCKET_NOT_BOUND');
    expect(userMessage).toContain('wrangler.toml');
  });

  it('should have isRetryable() on all error types', () => {
    const dbError = new DatabaseError(DatabaseErrorCode.TIMEOUT, 'Timeout');
    const stmtError = new StatementError(StatementErrorCode.SYNTAX_ERROR, 'Syntax error');

    expect(typeof dbError.isRetryable).toBe('function');
    expect(typeof stmtError.isRetryable).toBe('function');
    expect(dbError.isRetryable()).toBe(true);  // Timeout is retryable
    expect(stmtError.isRetryable()).toBe(false);  // Syntax errors are not
  });

  it('should have toUserMessage() on all error types', () => {
    const dbError = new DatabaseError(DatabaseErrorCode.CLOSED, 'Database is closed');
    const stmtError = new StatementError(StatementErrorCode.FINALIZED, 'Statement finalized');

    expect(typeof dbError.toUserMessage).toBe('function');
    expect(typeof stmtError.toUserMessage).toBe('function');
    expect(dbError.toUserMessage()).not.toContain('DB_CLOSED');
    expect(stmtError.toUserMessage()).toBeDefined();
  });

  it('AggregateDoSQLError is retryable only if all errors are retryable', () => {
    const retryableErrors = [
      new DatabaseError(DatabaseErrorCode.TIMEOUT, 'Timeout 1'),
      new DatabaseError(DatabaseErrorCode.CONNECTION_FAILED, 'Connection failed'),
    ];

    const mixedErrors = [
      new DatabaseError(DatabaseErrorCode.TIMEOUT, 'Timeout'),
      new StatementError(StatementErrorCode.SYNTAX_ERROR, 'Syntax error'),
    ];

    const retryableAggregate = new AggregateDoSQLError(retryableErrors);
    const mixedAggregate = new AggregateDoSQLError(mixedErrors);

    expect(retryableAggregate.isRetryable()).toBe(true);
    expect(mixedAggregate.isRetryable()).toBe(false);
  });
});

// =============================================================================
// 7. FACTORY FUNCTIONS
// =============================================================================

describe('Error Factory Functions', () => {
  it('createClosedDatabaseError creates proper error', () => {
    const error = createClosedDatabaseError(':memory:');

    expect(error.code).toBe(DatabaseErrorCode.CLOSED);
    expect(error.message).toContain('closed');
  });

  it('createSavepointNotFoundError creates proper error', () => {
    const error = createSavepointNotFoundError('my_savepoint');

    expect(error.code).toBe(DatabaseErrorCode.NOT_FOUND);
    expect(error.message).toContain('my_savepoint');
  });

  it('createFinalizedStatementError creates proper error', () => {
    const error = createFinalizedStatementError('SELECT 1');

    expect(error.code).toBe(StatementErrorCode.FINALIZED);
    expect(error.sql).toBe('SELECT 1');
  });

  it('createTableNotFoundError creates proper error', () => {
    const error = createTableNotFoundError('users', 'SELECT * FROM users');

    expect(error.code).toBe(StatementErrorCode.TABLE_NOT_FOUND);
    expect(error.message).toContain('users');
    expect(error.sql).toBe('SELECT * FROM users');
  });

  it('createMissingNamedParamError creates proper error', () => {
    const error = createMissingNamedParamError('user_id');

    expect(error.code).toBe(BindingErrorCode.MISSING_PARAM);
    expect(error.message).toContain('user_id');
  });

  it('createInvalidTypeError creates proper error', () => {
    const error = createInvalidTypeError('symbol');

    expect(error.code).toBe(BindingErrorCode.INVALID_TYPE);
    expect(error.message).toContain('symbol');
  });

  it('createCountMismatchError creates proper error', () => {
    const error = createCountMismatchError(3, 2);

    expect(error.code).toBe(BindingErrorCode.COUNT_MISMATCH);
    expect(error.message).toContain('3');
    expect(error.message).toContain('2');
  });
});

// =============================================================================
// 8. BACKWARDS COMPATIBILITY
// =============================================================================

describe('Backwards Compatibility', () => {
  it('errors are instanceof Error', () => {
    const dbError = new DatabaseError(DatabaseErrorCode.CLOSED, 'test');
    const stmtError = new StatementError(StatementErrorCode.FINALIZED, 'test');
    const bindError = new BindingError(BindingErrorCode.MISSING_PARAM, 'test');
    const syntaxError = new SQLSyntaxError(SyntaxErrorCode.GENERAL, 'test');

    expect(dbError).toBeInstanceOf(Error);
    expect(stmtError).toBeInstanceOf(Error);
    expect(bindError).toBeInstanceOf(Error);
    expect(syntaxError).toBeInstanceOf(Error);
  });

  it('errors have standard Error properties', () => {
    const error = new DatabaseError(DatabaseErrorCode.CLOSED, 'Database is closed');

    expect(error.name).toBe('DatabaseError');
    expect(error.message).toBe('Database is closed');
    expect(error.stack).toBeDefined();
  });

  it('errors support cause property', () => {
    const originalError = new Error('Original network error');
    const wrappedError = new DatabaseError(
      DatabaseErrorCode.CONNECTION_FAILED,
      'Database connection failed',
      { cause: originalError }
    );

    expect(wrappedError.cause).toBe(originalError);
  });
});
