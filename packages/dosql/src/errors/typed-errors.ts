/**
 * Typed Error Classes
 *
 * Specific error classes for planner, executor, parser, and storage operations.
 * Each error includes:
 * - Specific error code
 * - Context object (query, tables, operation, etc.)
 * - Cause chain support
 * - User-friendly messages
 * - Retryability indication
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
import {
  PlannerErrorCode,
  ExecutorErrorCode,
  ParserErrorCode,
  StorageErrorCode,
} from './codes.js';
import { assertNever } from '../utils/assert-never.js';

// =============================================================================
// PLANNER ERROR
// =============================================================================

/**
 * Error thrown during query planning operations
 *
 * @example
 * ```typescript
 * throw new PlannerError(
 *   PlannerErrorCode.NO_TABLES,
 *   'No tables to join',
 *   { context: { sql: 'SELECT * FROM' } }
 * );
 * ```
 */
export class PlannerError extends DoSQLError {
  readonly code: PlannerErrorCode;
  readonly category = ErrorCategory.EXECUTION;

  constructor(
    code: PlannerErrorCode,
    message: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    super(message, options);
    this.name = 'PlannerError';
    this.code = code;
    this.setRecoveryHint();
  }

  private setRecoveryHint(): void {
    switch (this.code) {
      case PlannerErrorCode.NO_TABLES:
        this.recoveryHint = 'Ensure your query references at least one table';
        break;
      case PlannerErrorCode.JOIN_ORDER_FAILED:
        this.recoveryHint = 'Simplify the join conditions or break into smaller queries';
        break;
      case PlannerErrorCode.INVALID_PLAN:
        this.recoveryHint = 'Check the query syntax and table references';
        break;
      case PlannerErrorCode.COST_ESTIMATION_FAILED:
        this.recoveryHint = 'Ensure table statistics are up to date';
        break;
      case PlannerErrorCode.INDEX_SELECTION_FAILED:
        this.recoveryHint = 'Check available indexes for the query';
        break;
      case PlannerErrorCode.OPTIMIZATION_FAILED:
        this.recoveryHint = 'Try simplifying the query';
        break;
      default:
        assertNever(this.code);
    }
  }

  isRetryable(): boolean {
    // Planner errors are not retryable as they indicate structural issues
    return false;
  }

  toUserMessage(): string {
    switch (this.code) {
      case PlannerErrorCode.NO_TABLES:
        return 'The query does not reference any tables. Please add a FROM clause.';
      case PlannerErrorCode.JOIN_ORDER_FAILED:
        return 'Could not determine the optimal order for joining tables. Please simplify the query.';
      case PlannerErrorCode.INVALID_PLAN:
        return 'The query plan is invalid. Please check the query syntax.';
      case PlannerErrorCode.COST_ESTIMATION_FAILED:
        return 'Failed to estimate query cost. Please ensure table statistics are available.';
      case PlannerErrorCode.INDEX_SELECTION_FAILED:
        return 'Failed to select an appropriate index for the query.';
      case PlannerErrorCode.OPTIMIZATION_FAILED:
        return 'Query optimization failed. Please try simplifying the query.';
      default:
        return assertNever(this.code);
    }
  }

  static fromJSON(json: SerializedError): PlannerError {
    const error = new PlannerError(
      json.code as PlannerErrorCode,
      json.message,
      { context: json.context }
    );
    return error;
  }
}

registerErrorClass('PlannerError', PlannerError);

// =============================================================================
// EXECUTOR ERROR
// =============================================================================

/**
 * Error thrown during query execution
 *
 * @example
 * ```typescript
 * throw new ExecutorError(
 *   ExecutorErrorCode.QUERY_TIMEOUT,
 *   'Query timeout after 5000ms',
 *   { context: { metadata: { elapsed: 5000, timeout: 5000 } } }
 * );
 * ```
 */
export class ExecutorError extends DoSQLError {
  readonly code: ExecutorErrorCode;
  readonly category = ErrorCategory.EXECUTION;

  constructor(
    code: ExecutorErrorCode,
    message: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    super(message, options);
    this.name = 'ExecutorError';
    this.code = code;
    this.setRecoveryHint();
  }

  private setRecoveryHint(): void {
    switch (this.code) {
      case ExecutorErrorCode.QUERY_TIMEOUT:
        this.recoveryHint = 'Increase the timeout setting or optimize the query';
        break;
      case ExecutorErrorCode.UNKNOWN_PLAN_TYPE:
        this.recoveryHint = 'This may be an unsupported query type';
        break;
      case ExecutorErrorCode.OPERATOR_ERROR:
        this.recoveryHint = 'Check the query for invalid operations';
        break;
      case ExecutorErrorCode.SUBQUERY_ERROR:
        this.recoveryHint = 'Check the subquery for errors';
        break;
      case ExecutorErrorCode.CTE_NOT_MATERIALIZED:
        this.recoveryHint = 'Ensure CTEs are executed before being referenced';
        break;
      case ExecutorErrorCode.AGGREGATE_ERROR:
        this.recoveryHint = 'Check aggregate function usage';
        break;
      case ExecutorErrorCode.WINDOW_FUNCTION_ERROR:
        this.recoveryHint = 'Check window function syntax and usage';
        break;
      case ExecutorErrorCode.TRANSACTION_ERROR:
        this.recoveryHint = 'Check transaction state and retry if needed';
        break;
      default:
        assertNever(this.code);
    }
  }

  isRetryable(): boolean {
    // Timeout errors are retryable
    return this.code === ExecutorErrorCode.QUERY_TIMEOUT;
  }

  toUserMessage(): string {
    switch (this.code) {
      case ExecutorErrorCode.QUERY_TIMEOUT:
        return 'The query took too long to execute. Please try again or simplify the query.';
      case ExecutorErrorCode.UNKNOWN_PLAN_TYPE:
        return 'The query contains an unsupported operation type.';
      case ExecutorErrorCode.OPERATOR_ERROR:
        return 'An error occurred while executing the query.';
      case ExecutorErrorCode.SUBQUERY_ERROR:
        return 'An error occurred in a subquery.';
      case ExecutorErrorCode.CTE_NOT_MATERIALIZED:
        return 'A common table expression (CTE) was referenced before it was executed.';
      case ExecutorErrorCode.AGGREGATE_ERROR:
        return 'An error occurred while executing an aggregate function.';
      case ExecutorErrorCode.WINDOW_FUNCTION_ERROR:
        return 'An error occurred while executing a window function.';
      case ExecutorErrorCode.TRANSACTION_ERROR:
        return 'A transaction error occurred.';
      default:
        return assertNever(this.code);
    }
  }

  static fromJSON(json: SerializedError): ExecutorError {
    const error = new ExecutorError(
      json.code as ExecutorErrorCode,
      json.message,
      { context: json.context }
    );
    return error;
  }
}

registerErrorClass('ExecutorError', ExecutorError);

// =============================================================================
// PARSER ERROR
// =============================================================================

/**
 * Error thrown during SQL parsing
 *
 * @example
 * ```typescript
 * throw new ParserError(
 *   ParserErrorCode.EMPTY_SQL,
 *   'Empty SQL query'
 * );
 * ```
 */
export class ParserError extends DoSQLError {
  readonly code: ParserErrorCode;
  readonly category = ErrorCategory.VALIDATION;

  constructor(
    code: ParserErrorCode,
    message: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    super(message, options);
    this.name = 'ParserError';
    this.code = code;
    this.setRecoveryHint();
  }

  private setRecoveryHint(): void {
    switch (this.code) {
      case ParserErrorCode.EMPTY_SQL:
        this.recoveryHint = 'Provide a valid SQL statement';
        break;
      case ParserErrorCode.UNSUPPORTED_OPERATION:
        this.recoveryHint = 'Use a supported SQL operation';
        break;
      case ParserErrorCode.INVALID_STATEMENT:
        this.recoveryHint = 'Check the SQL syntax';
        break;
      case ParserErrorCode.UNKNOWN_OPERATION:
        this.recoveryHint = 'Use a recognized SQL operation';
        break;
      case ParserErrorCode.INVALID_DATA_TYPE:
        this.recoveryHint = 'Use a valid SQL data type';
        break;
      case ParserErrorCode.INVALID_REFERENCE_ACTION:
        this.recoveryHint = 'Use a valid reference action (CASCADE, SET NULL, etc.)';
        break;
      case ParserErrorCode.UNKNOWN_WINDOW_NAME:
        this.recoveryHint = 'Define the window before referencing it';
        break;
      case ParserErrorCode.INVALID_FRAME_SPEC:
        this.recoveryHint = 'Check the window frame specification syntax';
        break;
      case ParserErrorCode.UNKNOWN_FUNCTION:
        this.recoveryHint = 'Use a supported function';
        break;
      default:
        assertNever(this.code);
    }
  }

  isRetryable(): boolean {
    // Parser errors are not retryable as they indicate invalid input
    return false;
  }

  toUserMessage(): string {
    switch (this.code) {
      case ParserErrorCode.EMPTY_SQL:
        return 'No SQL statement was provided.';
      case ParserErrorCode.UNSUPPORTED_OPERATION:
        return 'This SQL operation is not supported.';
      case ParserErrorCode.INVALID_STATEMENT:
        return 'The SQL statement is invalid.';
      case ParserErrorCode.UNKNOWN_OPERATION:
        return 'Could not determine the SQL operation type.';
      case ParserErrorCode.INVALID_DATA_TYPE:
        return 'The specified data type is invalid.';
      case ParserErrorCode.INVALID_REFERENCE_ACTION:
        return 'The specified reference action is invalid.';
      case ParserErrorCode.UNKNOWN_WINDOW_NAME:
        return 'The specified window name is not defined.';
      case ParserErrorCode.INVALID_FRAME_SPEC:
        return 'The window frame specification is invalid.';
      case ParserErrorCode.UNKNOWN_FUNCTION:
        return 'The specified function is not recognized.';
      default:
        return assertNever(this.code);
    }
  }

  static fromJSON(json: SerializedError): ParserError {
    const error = new ParserError(
      json.code as ParserErrorCode,
      json.message,
      { context: json.context }
    );
    return error;
  }
}

registerErrorClass('ParserError', ParserError);

// =============================================================================
// STORAGE ERROR
// =============================================================================

/**
 * Error thrown during storage operations
 *
 * @example
 * ```typescript
 * throw new StorageError(
 *   StorageErrorCode.READ_FAILED,
 *   'Read failed: /data/page.bin',
 *   { cause: originalError, context: { metadata: { path: '/data/page.bin' } } }
 * );
 * ```
 */
export class StorageError extends DoSQLError {
  readonly code: StorageErrorCode;
  readonly category = ErrorCategory.RESOURCE;

  constructor(
    code: StorageErrorCode,
    message: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    super(message, options);
    this.name = 'StorageError';
    this.code = code;
    this.setRecoveryHint();
  }

  private setRecoveryHint(): void {
    switch (this.code) {
      case StorageErrorCode.READ_FAILED:
        this.recoveryHint = 'Check storage connectivity and retry';
        break;
      case StorageErrorCode.WRITE_FAILED:
        this.recoveryHint = 'Check storage connectivity and available space';
        break;
      case StorageErrorCode.INVALID_SNAPSHOT_ID:
        this.recoveryHint = 'Use a valid snapshot ID';
        break;
      case StorageErrorCode.INVALID_PAGE_ID:
        this.recoveryHint = 'Use a valid page ID';
        break;
      case StorageErrorCode.BUCKET_NOT_FOUND:
        this.recoveryHint = 'Check bucket configuration and bindings';
        break;
      case StorageErrorCode.CORRUPTION:
        this.recoveryHint = 'Data may be corrupted - consider restoring from backup';
        break;
      default:
        assertNever(this.code);
    }
  }

  isRetryable(): boolean {
    // Read and write failures may be transient
    return [
      StorageErrorCode.READ_FAILED,
      StorageErrorCode.WRITE_FAILED,
    ].includes(this.code);
  }

  toUserMessage(): string {
    switch (this.code) {
      case StorageErrorCode.READ_FAILED:
        return 'Failed to read data from storage. Please try again.';
      case StorageErrorCode.WRITE_FAILED:
        return 'Failed to write data to storage. Please try again.';
      case StorageErrorCode.INVALID_SNAPSHOT_ID:
        return 'The snapshot ID is invalid.';
      case StorageErrorCode.INVALID_PAGE_ID:
        return 'The page ID is invalid.';
      case StorageErrorCode.BUCKET_NOT_FOUND:
        return 'The storage bucket was not found.';
      case StorageErrorCode.CORRUPTION:
        return 'Data corruption was detected.';
      default:
        return assertNever(this.code);
    }
  }

  static fromJSON(json: SerializedError): StorageError {
    const error = new StorageError(
      json.code as StorageErrorCode,
      json.message,
      { context: json.context }
    );
    return error;
  }
}

registerErrorClass('StorageError', StorageError);

// =============================================================================
// FACTORY FUNCTIONS - PLANNER
// =============================================================================

/**
 * Create a PlannerError for no tables to join
 *
 * @example
 * ```typescript
 * if (tables.length === 0) {
 *   throw createNoTablesToJoinError('SELECT * FROM');
 * }
 * ```
 */
export function createNoTablesToJoinError(sql?: string): PlannerError {
  return new PlannerError(
    PlannerErrorCode.NO_TABLES,
    'No tables to join',
    { context: { sql } }
  );
}

/**
 * Create a PlannerError for failed join order optimization
 *
 * @example
 * ```typescript
 * if (!result) {
 *   throw createJoinOrderFailedError(['users', 'orders'], sql);
 * }
 * ```
 */
export function createJoinOrderFailedError(tables: string[], sql?: string): PlannerError {
  return new PlannerError(
    PlannerErrorCode.JOIN_ORDER_FAILED,
    `Failed to find join order for tables: ${tables.join(', ')}`,
    { context: { sql, metadata: { tables } } }
  );
}

// =============================================================================
// FACTORY FUNCTIONS - EXECUTOR
// =============================================================================

/**
 * Create an ExecutorError for unknown plan type
 *
 * @example
 * ```typescript
 * default:
 *   // Use type assertion to access type property in exhaustive switch default case
 *   throw createUnknownPlanTypeError((plan as { type: unknown }).type as string, sql);
 * ```
 */
export function createUnknownPlanTypeError(planType: string, sql?: string): ExecutorError {
  return new ExecutorError(
    ExecutorErrorCode.UNKNOWN_PLAN_TYPE,
    `Unknown plan type: ${planType}`,
    { context: { sql, metadata: { planType } } }
  );
}

/**
 * Create an ExecutorError for query timeout
 *
 * @example
 * ```typescript
 * if (elapsed > timeout) {
 *   throw createQueryTimeoutError(elapsed, timeout, sql);
 * }
 * ```
 */
export function createQueryTimeoutError(elapsed: number, timeout: number, sql?: string): ExecutorError {
  return new ExecutorError(
    ExecutorErrorCode.QUERY_TIMEOUT,
    `Query timeout after ${elapsed}ms (limit: ${timeout}ms)`,
    { context: { sql, metadata: { elapsed, timeout } } }
  );
}

/**
 * Create an ExecutorError for CTE not materialized
 */
export function createCteNotMaterializedError(cteName: string, sql?: string): ExecutorError {
  return new ExecutorError(
    ExecutorErrorCode.CTE_NOT_MATERIALIZED,
    `CTE '${cteName}' is not materialized. Ensure CTEs are executed before referencing them.`,
    { context: { sql, metadata: { cteName } } }
  );
}

/**
 * Create an ExecutorError for scalar subquery returning more than one row
 */
export function createScalarSubqueryError(sql?: string): ExecutorError {
  return new ExecutorError(
    ExecutorErrorCode.SUBQUERY_ERROR,
    'Scalar subquery returned more than one row',
    { context: { sql } }
  );
}

/**
 * Create an ExecutorError for unknown function
 */
export function createUnknownFunctionError(name: string, sql?: string): ExecutorError {
  return new ExecutorError(
    ExecutorErrorCode.OPERATOR_ERROR,
    `Unknown function: ${name}`,
    { context: { sql, metadata: { functionName: name } } }
  );
}

/**
 * Create an ExecutorError for unknown window function
 */
export function createUnknownWindowFunctionError(name: string, sql?: string): ExecutorError {
  return new ExecutorError(
    ExecutorErrorCode.WINDOW_FUNCTION_ERROR,
    `Unknown window function: ${name}`,
    { context: { sql, metadata: { functionName: name } } }
  );
}

/**
 * Create an ExecutorError for unknown set operation
 */
export function createUnknownSetOperationError(operator: string, sql?: string): ExecutorError {
  return new ExecutorError(
    ExecutorErrorCode.OPERATOR_ERROR,
    `Unknown set operation: ${operator}`,
    { context: { sql, metadata: { operator } } }
  );
}

/**
 * Create an ExecutorError for aggregate expression format error
 */
export function createAggregateExpressionError(sql?: string): ExecutorError {
  return new ExecutorError(
    ExecutorErrorCode.AGGREGATE_ERROR,
    'Invalid aggregate expression format',
    { context: { sql } }
  );
}

/**
 * Create an ExecutorError for transaction state error
 */
export function createTransactionStateError(
  txId: string,
  state: 'committed' | 'rolled_back',
  sql?: string
): ExecutorError {
  const message = state === 'committed'
    ? `Transaction ${txId} has already been committed`
    : `Transaction ${txId} has been rolled back`;
  return new ExecutorError(
    ExecutorErrorCode.TRANSACTION_ERROR,
    message,
    { context: { sql, transactionId: txId, metadata: { state } } }
  );
}

// =============================================================================
// FACTORY FUNCTIONS - PARSER
// =============================================================================

/**
 * Create a ParserError for empty SQL
 *
 * @example
 * ```typescript
 * if (!sql.trim()) {
 *   throw createEmptySqlError();
 * }
 * ```
 */
export function createEmptySqlError(): ParserError {
  return new ParserError(
    ParserErrorCode.EMPTY_SQL,
    'Empty SQL query'
  );
}

/**
 * Create a ParserError for unsupported operation
 *
 * @example
 * ```typescript
 * throw createUnsupportedOperationError('MERGE', sql);
 * ```
 */
export function createUnsupportedOperationError(operation: string, sql?: string): ParserError {
  return new ParserError(
    ParserErrorCode.UNSUPPORTED_OPERATION,
    `Unsupported operation: ${operation}`,
    { context: { sql, metadata: { operation } } }
  );
}

/**
 * Create a ParserError for unknown SQL operation
 */
export function createUnknownOperationError(sql?: string): ParserError {
  return new ParserError(
    ParserErrorCode.UNKNOWN_OPERATION,
    'Unknown SQL operation',
    { context: { sql } }
  );
}

/**
 * Create a ParserError for invalid statement structure
 */
export function createInvalidStatementError(statementType: string, reason?: string, sql?: string): ParserError {
  const message = reason
    ? `Invalid ${statementType} statement: ${reason}`
    : `Invalid ${statementType} statement`;
  return new ParserError(
    ParserErrorCode.INVALID_STATEMENT,
    message,
    { context: { sql, metadata: { statementType, reason } } }
  );
}

/**
 * Create a ParserError for invalid data type
 */
export function createInvalidDataTypeError(value: string, sql?: string): ParserError {
  return new ParserError(
    ParserErrorCode.INVALID_DATA_TYPE,
    `Expected data type but got ${value}`,
    { context: { sql, metadata: { value } } }
  );
}

/**
 * Create a ParserError for invalid reference action
 */
export function createInvalidReferenceActionError(value: string, sql?: string): ParserError {
  return new ParserError(
    ParserErrorCode.INVALID_REFERENCE_ACTION,
    `Expected reference action but got ${value}`,
    { context: { sql, metadata: { value } } }
  );
}

/**
 * Create a ParserError for unknown window name
 */
export function createUnknownWindowNameError(name: string, sql?: string): ParserError {
  return new ParserError(
    ParserErrorCode.UNKNOWN_WINDOW_NAME,
    `Unknown window name: ${name}`,
    { context: { sql, metadata: { windowName: name } } }
  );
}

/**
 * Create a ParserError for invalid frame specification
 */
export function createInvalidFrameSpecError(reason: string, sql?: string): ParserError {
  return new ParserError(
    ParserErrorCode.INVALID_FRAME_SPEC,
    reason,
    { context: { sql } }
  );
}

/**
 * Create a ParserError for unknown function
 */
export function createParserUnknownFunctionError(name: string, sql?: string): ParserError {
  return new ParserError(
    ParserErrorCode.UNKNOWN_FUNCTION,
    `Unknown function: ${name}`,
    { context: { sql, metadata: { functionName: name } } }
  );
}

// =============================================================================
// FACTORY FUNCTIONS - STORAGE
// =============================================================================

/**
 * Create a StorageError for invalid snapshot ID
 *
 * @example
 * ```typescript
 * throw createInvalidSnapshotIdError(id);
 * ```
 */
export function createInvalidSnapshotIdError(id: string): StorageError {
  return new StorageError(
    StorageErrorCode.INVALID_SNAPSHOT_ID,
    `Invalid snapshot ID: ${id}`,
    { context: { metadata: { snapshotId: id } } }
  );
}

/**
 * Create a StorageError for read failure
 *
 * @example
 * ```typescript
 * try {
 *   await storage.read(path);
 * } catch (e) {
 *   throw createStorageReadError(path, e);
 * }
 * ```
 */
export function createStorageReadError(path: string, cause?: Error): StorageError {
  return new StorageError(
    StorageErrorCode.READ_FAILED,
    `Read failed: ${path}`,
    { cause, context: { metadata: { path, operation: 'read' } } }
  );
}

/**
 * Create a StorageError for write failure
 *
 * @example
 * ```typescript
 * try {
 *   await storage.write(path, data);
 * } catch (e) {
 *   throw createStorageWriteError(path, e);
 * }
 * ```
 */
export function createStorageWriteError(path: string, cause?: Error): StorageError {
  return new StorageError(
    StorageErrorCode.WRITE_FAILED,
    `Write failed: ${path}`,
    { cause, context: { metadata: { path, operation: 'write' } } }
  );
}

/**
 * Create a StorageError for invalid page ID
 */
export function createInvalidPageIdError(value: number, reason: string): StorageError {
  return new StorageError(
    StorageErrorCode.INVALID_PAGE_ID,
    `PageId ${reason}: ${value}`,
    { context: { metadata: { pageId: value, reason } } }
  );
}

/**
 * Create a StorageError for bucket not found
 */
export function createBucketNotFoundError(bucketName?: string): StorageError {
  const message = bucketName
    ? `R2 bucket '${bucketName}' not found`
    : 'R2 bucket binding not found';
  return new StorageError(
    StorageErrorCode.BUCKET_NOT_FOUND,
    message,
    { context: { metadata: { bucketName } } }
  );
}
