/**
 * Database Error Classes
 *
 * Standardized error classes for database operations.
 *
 * @packageDocumentation
 */

import {
  DoSQLError,
  ErrorCategory,
  registerErrorClass,
  type ErrorContext,
  type SerializedError,
} from './base.js';
import { DatabaseErrorCode } from './codes.js';

// =============================================================================
// Database Error
// =============================================================================

/**
 * Error thrown for database operations
 *
 * @example
 * ```typescript
 * const db = createDatabase();
 * db.close();
 *
 * try {
 *   db.prepare('SELECT 1');
 * } catch (error) {
 *   if (error instanceof DatabaseError) {
 *     console.log(error.code);  // 'DB_CLOSED'
 *     console.log(error.isRetryable());  // false
 *   }
 * }
 * ```
 */
export class DatabaseError extends DoSQLError {
  readonly code: DatabaseErrorCode;
  readonly category = ErrorCategory.EXECUTION;

  constructor(
    code: DatabaseErrorCode,
    message: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    super(message, options);
    this.name = 'DatabaseError';
    this.code = code;

    // Set recovery hints based on error code
    this.setRecoveryHint();
  }

  private setRecoveryHint(): void {
    switch (this.code) {
      case DatabaseErrorCode.CLOSED:
        this.recoveryHint = 'Create a new Database instance with createDatabase()';
        break;
      case DatabaseErrorCode.READ_ONLY:
        this.recoveryHint = 'Open the database without the readonly option';
        break;
      case DatabaseErrorCode.CONNECTION_FAILED:
        this.recoveryHint = 'Check database path and permissions';
        break;
      case DatabaseErrorCode.TIMEOUT:
        this.recoveryHint = 'Increase the timeout setting or optimize the query';
        break;
    }
  }

  isRetryable(): boolean {
    return [
      DatabaseErrorCode.CONNECTION_FAILED,
      DatabaseErrorCode.TIMEOUT,
    ].includes(this.code);
  }

  toUserMessage(): string {
    switch (this.code) {
      case DatabaseErrorCode.CLOSED:
        return 'The database connection has been closed. Please reconnect.';
      case DatabaseErrorCode.READ_ONLY:
        return 'This database is read-only. Write operations are not permitted.';
      case DatabaseErrorCode.NOT_FOUND:
        return 'The requested database could not be found.';
      case DatabaseErrorCode.CONSTRAINT_VIOLATION:
        return 'The operation violates a database constraint.';
      case DatabaseErrorCode.TIMEOUT:
        return 'The database operation timed out. Please try again.';
      default:
        return this.message;
    }
  }

  /**
   * Deserialize from JSON
   */
  static fromJSON(json: SerializedError): DatabaseError {
    const error = new DatabaseError(
      json.code as DatabaseErrorCode,
      json.message,
      { context: json.context }
    );
    return error;
  }
}

// Register for deserialization
registerErrorClass('DatabaseError', DatabaseError);

// =============================================================================
// Connection Error
// =============================================================================

/**
 * Error for database connection issues
 */
export class ConnectionError extends DatabaseError {
  constructor(
    message: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    super(DatabaseErrorCode.CONNECTION_FAILED, message, options);
    this.name = 'ConnectionError';
  }
}

registerErrorClass('ConnectionError', ConnectionError);

// =============================================================================
// Read-Only Error
// =============================================================================

/**
 * Error when attempting write operations on read-only database
 */
export class ReadOnlyError extends DatabaseError {
  constructor(
    message?: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    super(
      DatabaseErrorCode.READ_ONLY,
      message ?? 'Database is read-only',
      options
    );
    this.name = 'ReadOnlyError';
  }
}

registerErrorClass('ReadOnlyError', ReadOnlyError);

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a DatabaseError for a closed database
 */
export function createClosedDatabaseError(databaseName?: string): DatabaseError {
  const error = new DatabaseError(
    DatabaseErrorCode.CLOSED,
    'Database is closed'
  );
  if (databaseName) {
    error.withContext({ metadata: { databaseName } });
  }
  return error;
}

/**
 * Create a DatabaseError for savepoint not found
 */
export function createSavepointNotFoundError(name: string): DatabaseError {
  return new DatabaseError(
    DatabaseErrorCode.NOT_FOUND,
    `Savepoint '${name}' not found`,
    { context: { metadata: { savepointName: name } } }
  );
}
