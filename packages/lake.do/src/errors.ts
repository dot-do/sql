/**
 * @dotdo/lake.do - Error Classes
 *
 * This module provides consolidated error classes for the lake.do client.
 * All error classes extend LakeError for consistent error handling.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

import type { LakeRPCError } from './types.js';
import { ErrorCode } from './constants.js';

// =============================================================================
// Base Error Class
// =============================================================================

/**
 * Base error thrown by Lake operations.
 *
 * @description Represents an error returned by the DoLake server. Contains a
 * machine-readable error code and optional details for debugging. All lake.do
 * API methods may throw this error type.
 *
 * Common error codes:
 * - `TABLE_NOT_FOUND`: The requested table does not exist
 * - `PARTITION_NOT_FOUND`: The requested partition does not exist
 * - `INVALID_SQL`: SQL syntax error or invalid query
 * - `UNAUTHORIZED`: Authentication failed or token expired
 * - `TIMEOUT`: Request exceeded the configured timeout
 * - `CONNECTION_CLOSED`: WebSocket connection was closed
 *
 * @example
 * ```typescript
 * try {
 *   const result = await client.query('SELECT * FROM nonexistent');
 * } catch (error) {
 *   if (error instanceof LakeError) {
 *     console.error(`Lake error [${error.code}]: ${error.message}`);
 *     if (error.code === 'TABLE_NOT_FOUND') {
 *       // Handle missing table
 *     }
 *     if (error.details) {
 *       console.error('Details:', error.details);
 *     }
 *   } else {
 *     throw error;
 *   }
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class LakeError extends Error {
  /** Machine-readable error code for programmatic error handling */
  readonly code: string;
  /** Additional error context (structure varies by error type) */
  readonly details?: unknown;

  /**
   * Creates a new LakeError instance.
   *
   * @param error - The RPC error response from the server
   */
  constructor(error: LakeRPCError) {
    super(error.message);
    this.name = 'LakeError';
    this.code = error.code;
    this.details = error.details;
  }
}

// =============================================================================
// Connection Errors
// =============================================================================

/**
 * Error thrown when a connection operation fails.
 *
 * @description Represents connection-related errors such as connection failures,
 * timeouts, or disconnections. Extends LakeError for consistent error handling.
 *
 * Common error codes:
 * - `CONNECTION_ERROR`: General connection failure
 * - `CONNECTION_CLOSED`: WebSocket connection was closed unexpectedly
 * - `CONNECTION_TIMEOUT`: Connection attempt timed out
 * - `NOT_CONNECTED`: Operation attempted while not connected
 *
 * @example
 * ```typescript
 * try {
 *   await client.connect();
 * } catch (error) {
 *   if (error instanceof ConnectionError) {
 *     console.error(`Connection failed [${error.code}]: ${error.message}`);
 *     if (error.code === ErrorCode.CONNECTION_TIMEOUT) {
 *       // Handle timeout specifically
 *     }
 *   }
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class ConnectionError extends LakeError {
  /**
   * Creates a new ConnectionError instance.
   *
   * @param error - The error details
   */
  constructor(error: LakeRPCError) {
    super(error);
    this.name = 'ConnectionError';
  }

  /**
   * Creates a ConnectionError from code and message.
   *
   * @param code - The error code
   * @param message - The error message
   * @param details - Optional additional details
   * @returns A new ConnectionError instance
   */
  static create(code: string, message: string, details?: unknown): ConnectionError {
    return new ConnectionError({ code, message, details });
  }

  /**
   * Creates a connection closed error.
   *
   * @param message - Optional custom message
   * @returns A new ConnectionError instance
   */
  static closed(message = 'Connection closed'): ConnectionError {
    return ConnectionError.create(ErrorCode.CONNECTION_CLOSED, message);
  }

  /**
   * Creates a not connected error.
   *
   * @param message - Optional custom message
   * @returns A new ConnectionError instance
   */
  static notConnected(message = 'WebSocket is not connected'): ConnectionError {
    return ConnectionError.create(ErrorCode.NOT_CONNECTED, message);
  }

  /**
   * Creates a connection error.
   *
   * @param message - Optional custom message
   * @returns A new ConnectionError instance
   */
  static failed(message = 'Connection failed'): ConnectionError {
    return ConnectionError.create(ErrorCode.CONNECTION_ERROR, message);
  }
}

// =============================================================================
// Query Errors
// =============================================================================

/**
 * Error thrown when a query operation fails.
 *
 * @description Represents query-related errors such as SQL syntax errors,
 * table not found, or query timeouts. Extends LakeError for consistent error handling.
 *
 * Common error codes:
 * - `INVALID_SQL`: SQL syntax error or invalid query
 * - `TABLE_NOT_FOUND`: The requested table does not exist
 * - `QUERY_TIMEOUT`: Query execution timed out
 *
 * @example
 * ```typescript
 * try {
 *   await client.query('INVALID SQL');
 * } catch (error) {
 *   if (error instanceof QueryError) {
 *     console.error(`Query failed [${error.code}]: ${error.message}`);
 *     if (error.code === ErrorCode.INVALID_SQL) {
 *       // Handle SQL syntax error
 *     }
 *   }
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class QueryError extends LakeError {
  /**
   * Creates a new QueryError instance.
   *
   * @param error - The error details
   */
  constructor(error: LakeRPCError) {
    super(error);
    this.name = 'QueryError';
  }

  /**
   * Creates a QueryError from code and message.
   *
   * @param code - The error code
   * @param message - The error message
   * @param details - Optional additional details
   * @returns A new QueryError instance
   */
  static create(code: string, message: string, details?: unknown): QueryError {
    return new QueryError({ code, message, details });
  }

  /**
   * Creates a table not found error.
   *
   * @param tableName - The name of the table that wasn't found
   * @returns A new QueryError instance
   */
  static tableNotFound(tableName: string): QueryError {
    return QueryError.create(
      ErrorCode.TABLE_NOT_FOUND,
      `Table "${tableName}" does not exist`,
      { tableName }
    );
  }

  /**
   * Creates an invalid SQL error.
   *
   * @param message - The error message describing the SQL issue
   * @param details - Optional additional details (e.g., position in query)
   * @returns A new QueryError instance
   */
  static invalidSql(message: string, details?: unknown): QueryError {
    return QueryError.create(ErrorCode.INVALID_SQL, message, details);
  }

  /**
   * Creates a query timeout error.
   *
   * @param message - Optional custom message
   * @returns A new QueryError instance
   */
  static timeout(message = 'Query execution timed out'): QueryError {
    return QueryError.create(ErrorCode.QUERY_TIMEOUT, message);
  }
}

// =============================================================================
// Timeout Errors
// =============================================================================

/**
 * Error thrown when a request times out.
 *
 * @description Represents timeout errors for any operation.
 * Extends LakeError for consistent error handling.
 *
 * @example
 * ```typescript
 * try {
 *   await client.query('SELECT * FROM large_table');
 * } catch (error) {
 *   if (error instanceof TimeoutError) {
 *     console.error(`Operation timed out: ${error.method}`);
 *   }
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class TimeoutError extends LakeError {
  /** The RPC method that timed out */
  readonly method: string;

  /**
   * Creates a new TimeoutError instance.
   *
   * @param method - The RPC method that timed out
   */
  constructor(method: string) {
    super({
      code: ErrorCode.TIMEOUT,
      message: `Request timeout: ${method}`,
      details: { method },
    });
    this.name = 'TimeoutError';
    this.method = method;
  }
}
