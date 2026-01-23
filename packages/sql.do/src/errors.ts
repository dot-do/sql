/**
 * SQL Client Error Types
 *
 * Provides error classes for SQL operations, connection management, and timeouts.
 * All errors include machine-readable codes for programmatic error handling.
 *
 * @packageDocumentation
 */

import type { RPCError } from './types.js';

// =============================================================================
// SQL Error
// =============================================================================

/**
 * Error thrown by SQL operations when a query or command fails.
 *
 * Contains a machine-readable {@link code} for programmatic error handling and
 * optional {@link details} for debugging. Extends the standard JavaScript Error
 * class with additional context about the SQL failure.
 *
 * Common error codes:
 * - `SYNTAX_ERROR` - Invalid SQL syntax
 * - `CONSTRAINT_VIOLATION` - Unique constraint, foreign key, or check constraint failed
 * - `TABLE_NOT_FOUND` - Referenced table does not exist
 * - `TIMEOUT` - Query execution timed out
 * - `CONNECTION_CLOSED` - WebSocket connection was closed
 * - `TRANSACTION_CONFLICT` - Transaction was aborted due to conflict
 *
 * @example
 * ```typescript
 * try {
 *   await client.exec('INSERT INTO users (id, name) VALUES (?, ?)', [1, 'Alice']);
 * } catch (error) {
 *   if (error instanceof SQLError) {
 *     switch (error.code) {
 *       case 'CONSTRAINT_VIOLATION':
 *         console.log('User with this ID already exists');
 *         break;
 *       case 'TIMEOUT':
 *         console.log('Query timed out, retrying...');
 *         break;
 *       default:
 *         console.error(`SQL Error [${error.code}]: ${error.message}`);
 *     }
 *   }
 * }
 * ```
 *
 * @public
 * @since 0.1.0
 */
export class SQLError extends Error {
  /**
   * Machine-readable error code for programmatic error handling.
   *
   * Use this to implement different error handling strategies based on the
   * type of failure (e.g., retry for TIMEOUT, fail fast for SYNTAX_ERROR).
   */
  readonly code: string;

  /**
   * Optional additional details about the error.
   *
   * May contain structured information like the specific constraint that failed,
   * the position in the SQL where a syntax error occurred, etc.
   */
  readonly details?: unknown;

  /**
   * Creates a new SQLError from an RPC error response.
   *
   * @param error - The RPC error object from the server response
   * @internal
   */
  constructor(error: RPCError) {
    super(error.message);
    this.name = 'SQLError';
    this.code = error.code;
    this.details = error.details;
  }
}

// =============================================================================
// Connection Error
// =============================================================================

/**
 * Error thrown when a connection to the database fails.
 *
 * This error is typically retryable as it indicates a network or infrastructure
 * issue rather than a problem with the SQL statement itself.
 *
 * @example
 * ```typescript
 * try {
 *   await client.connect();
 * } catch (error) {
 *   if (error instanceof ConnectionError) {
 *     console.log(`Connection failed: ${error.message}`);
 *     console.log(`Retryable: ${error.retryable}`);
 *   }
 * }
 * ```
 *
 * @public
 * @since 0.2.0
 */
export class ConnectionError extends Error {
  /**
   * Error code indicating this is a connection-related error.
   */
  readonly code = 'CONNECTION_FAILED' as const;

  /**
   * Indicates whether this error is safe to retry.
   * Connection errors are typically retryable.
   */
  readonly retryable = true;

  /**
   * Creates a new ConnectionError.
   *
   * @param message - Description of the connection failure
   */
  constructor(message: string) {
    super(message);
    this.name = 'ConnectionError';
  }
}

// =============================================================================
// Timeout Error
// =============================================================================

/**
 * Error thrown when an operation times out.
 *
 * This error is typically retryable as the timeout may have been due to
 * transient network issues or server load.
 *
 * @example
 * ```typescript
 * try {
 *   await client.query('SELECT * FROM large_table');
 * } catch (error) {
 *   if (error instanceof TimeoutError) {
 *     console.log(`Query timed out after ${error.timeoutMs}ms`);
 *     console.log(`Retryable: ${error.retryable}`);
 *   }
 * }
 * ```
 *
 * @public
 * @since 0.2.0
 */
export class TimeoutError extends Error {
  /**
   * Error code indicating this is a timeout error.
   */
  readonly code = 'TIMEOUT' as const;

  /**
   * Indicates whether this error is safe to retry.
   * Timeout errors are typically retryable.
   */
  readonly retryable = true;

  /**
   * The timeout duration in milliseconds that was exceeded.
   */
  readonly timeoutMs: number;

  /**
   * Creates a new TimeoutError.
   *
   * @param message - Description of the timeout
   * @param timeoutMs - The timeout duration in milliseconds
   */
  constructor(message: string, timeoutMs: number) {
    super(message);
    this.name = 'TimeoutError';
    this.timeoutMs = timeoutMs;
  }
}

// =============================================================================
// Retryable Error Codes
// =============================================================================

/**
 * Error codes that indicate the error is retryable.
 * @internal
 */
export const RETRYABLE_ERROR_CODES = [
  'TIMEOUT',
  'CONNECTION_CLOSED',
  'NETWORK_ERROR',
  'UNAVAILABLE',
  'RESOURCE_EXHAUSTED',
] as const;

/**
 * Type for retryable error codes.
 * @public
 */
export type RetryableErrorCode = typeof RETRYABLE_ERROR_CODES[number];

/**
 * Set of retryable error codes for O(1) lookup.
 * @internal
 */
const RETRYABLE_ERROR_CODES_SET: ReadonlySet<string> = new Set(RETRYABLE_ERROR_CODES);

/**
 * Checks if a SQL error is retryable (connection issues, timeouts, etc.).
 *
 * @param error - The SQL error to check
 * @returns `true` if the error is retryable
 *
 * @example
 * ```typescript
 * try {
 *   await client.exec('INSERT INTO users VALUES (?)');
 * } catch (error) {
 *   if (error instanceof SQLError && isRetryableError(error)) {
 *     // Safe to retry
 *   }
 * }
 * ```
 *
 * @public
 */
export function isRetryableError(error: SQLError): boolean {
  return RETRYABLE_ERROR_CODES_SET.has(error.code);
}
