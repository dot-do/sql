/**
 * Statement Error Classes
 *
 * Standardized error classes for prepared statement operations.
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
import { StatementErrorCode } from './codes.js';

// =============================================================================
// Statement Error
// =============================================================================

/**
 * Error thrown when statement execution fails
 *
 * @example
 * ```typescript
 * try {
 *   stmt.run();
 * } catch (error) {
 *   if (error instanceof StatementError) {
 *     console.log(error.code);  // 'STMT_FINALIZED'
 *     console.log(error.sql);   // The SQL that failed
 *   }
 * }
 * ```
 */
export class StatementError extends DoSQLError {
  readonly code: StatementErrorCode;
  readonly category = ErrorCategory.EXECUTION;

  /** SQL statement that caused the error */
  readonly sql?: string;

  constructor(
    code: StatementErrorCode,
    message: string,
    sql?: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    super(message, options);
    this.name = 'StatementError';
    this.code = code;
    this.sql = sql;

    // Include SQL in context
    if (sql) {
      this.context = { ...this.context, sql };
    }

    // Set recovery hints
    this.setRecoveryHint();
  }

  private setRecoveryHint(): void {
    switch (this.code) {
      case StatementErrorCode.FINALIZED:
        this.recoveryHint = 'Create a new prepared statement with db.prepare()';
        break;
      case StatementErrorCode.TABLE_NOT_FOUND:
        this.recoveryHint = 'Check that the table exists and the name is spelled correctly';
        break;
      case StatementErrorCode.SYNTAX_ERROR:
        this.recoveryHint = 'Check the SQL syntax for errors';
        break;
    }
  }

  isRetryable(): boolean {
    // Statement errors are generally not retryable
    return false;
  }

  toUserMessage(): string {
    switch (this.code) {
      case StatementErrorCode.FINALIZED:
        return 'This statement has already been closed and cannot be used.';
      case StatementErrorCode.TABLE_NOT_FOUND:
        return `The specified table could not be found.`;
      case StatementErrorCode.COLUMN_NOT_FOUND:
        return `The specified column could not be found.`;
      case StatementErrorCode.SYNTAX_ERROR:
        return 'There is a syntax error in the SQL statement.';
      case StatementErrorCode.UNSUPPORTED:
        return 'This SQL operation is not supported.';
      default:
        return this.message;
    }
  }

  /**
   * Deserialize from JSON
   */
  static fromJSON(json: SerializedError): StatementError {
    const error = new StatementError(
      json.code as StatementErrorCode,
      json.message,
      json.context?.sql,
      { context: json.context }
    );
    return error;
  }
}

// Register for deserialization
registerErrorClass('StatementError', StatementError);

// =============================================================================
// Prepare Error
// =============================================================================

/**
 * Error when preparing a statement fails
 */
export class PrepareError extends StatementError {
  constructor(
    message: string,
    sql?: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    super(StatementErrorCode.SYNTAX_ERROR, message, sql, options);
    this.name = 'PrepareError';
  }
}

registerErrorClass('PrepareError', PrepareError);

// =============================================================================
// Execute Error
// =============================================================================

/**
 * Error when executing a statement fails
 */
export class ExecuteError extends StatementError {
  constructor(
    message: string,
    sql?: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    super(StatementErrorCode.EXECUTION_ERROR, message, sql, options);
    this.name = 'ExecuteError';
  }
}

registerErrorClass('ExecuteError', ExecuteError);

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a StatementError for finalized statement
 */
export function createFinalizedStatementError(sql?: string): StatementError {
  return new StatementError(
    StatementErrorCode.FINALIZED,
    'Statement has been finalized and cannot be used',
    sql
  );
}

/**
 * Create a StatementError for table not found
 */
export function createTableNotFoundError(tableName: string, sql?: string): StatementError {
  return new StatementError(
    StatementErrorCode.TABLE_NOT_FOUND,
    `Table ${tableName} does not exist`,
    sql,
    { context: { table: tableName } }
  );
}

/**
 * Create a StatementError for unsupported SQL
 */
export function createUnsupportedSqlError(sql: string): StatementError {
  return new StatementError(
    StatementErrorCode.UNSUPPORTED,
    `Unsupported SQL: ${sql}`,
    sql
  );
}
