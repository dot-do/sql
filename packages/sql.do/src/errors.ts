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
// URL Masking Utility
// =============================================================================

/**
 * Masks sensitive data in a URL for safe logging and error messages.
 *
 * Removes or masks:
 * - Password in userinfo (user:password@host)
 * - Query parameters that may contain tokens (token, key, secret, password, auth, api_key)
 *
 * @param url - The URL to mask
 * @returns A masked version of the URL safe for logging
 * @internal
 */
export function maskUrl(url: string): string {
  try {
    const parsed = new URL(url);

    // Mask password in userinfo
    if (parsed.password) {
      parsed.password = '***';
    }

    // Mask sensitive query parameters
    const sensitiveParams = ['token', 'key', 'secret', 'password', 'auth', 'api_key', 'apikey', 'access_token'];
    for (const param of sensitiveParams) {
      if (parsed.searchParams.has(param)) {
        parsed.searchParams.set(param, '***');
      }
    }

    return parsed.toString();
  } catch {
    // If URL parsing fails, mask everything after :// except the host
    const match = url.match(/^(\w+:\/\/)([^/?#]+)/);
    if (match) {
      return `${match[1]}${match[2]}/***`;
    }
    // Fallback: return a generic masked version
    return '[invalid-url]';
  }
}

// =============================================================================
// SQL Error
// =============================================================================

/**
 * Error thrown by SQL operations when a query or command fails.
 *
 * Contains a machine-readable {@link code} for programmatic error handling,
 * optional {@link details} for debugging, and optional {@link suggestion} for
 * error recovery hints. Extends the standard JavaScript Error class with
 * additional context about the SQL failure.
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
 *       case 'SYNTAX_ERROR':
 *         console.error(`Syntax error: ${error.message}`);
 *         if (error.suggestion) {
 *           console.log(`Suggestion: ${error.suggestion}`);
 *         }
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
   * Optional suggestion for error recovery.
   *
   * Provides actionable hints to help developers fix the error, such as:
   * - Spelling corrections for typos (e.g., "Did you mean 'SELECT'?")
   * - Missing clause suggestions (e.g., "Add a WHERE clause to limit results")
   * - Syntax fixes (e.g., "Use single quotes for string literals")
   *
   * @example
   * ```typescript
   * if (error.suggestion) {
   *   console.log(`Hint: ${error.suggestion}`);
   * }
   * ```
   */
  readonly suggestion?: string;

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
    this.suggestion = error.suggestion;
  }
}

// =============================================================================
// Connection Error
// =============================================================================

/**
 * Error thrown when a connection to the database fails.
 *
 * This error is typically retryable as it indicates a network or infrastructure
 * issue rather than a problem with the SQL statement itself. The error message
 * includes the URL (with sensitive data masked) for debugging purposes.
 *
 * @example
 * ```typescript
 * try {
 *   await client.connect();
 * } catch (error) {
 *   if (error instanceof ConnectionError) {
 *     console.log(`Connection failed: ${error.message}`);
 *     console.log(`URL: ${error.url}`);
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
   * The URL that the connection was attempted to (masked for security).
   * May be undefined if no URL was provided.
   */
  readonly url?: string;

  /**
   * Creates a new ConnectionError.
   *
   * @param message - Description of the connection failure
   * @param url - Optional URL that the connection was attempted to (will be masked)
   */
  constructor(message: string, url?: string) {
    const maskedUrl = url ? maskUrl(url) : undefined;
    const fullMessage = maskedUrl ? `${message} (url: ${maskedUrl})` : message;
    super(fullMessage);
    this.name = 'ConnectionError';
    if (maskedUrl) {
      this.url = maskedUrl;
    }
  }
}

// =============================================================================
// Timeout Error
// =============================================================================

/**
 * The type of operation that timed out.
 * @public
 */
export type TimeoutOperationType = 'query' | 'exec' | 'transaction' | 'rpc';

/**
 * Error thrown when an operation times out.
 *
 * This error is typically retryable as the timeout may have been due to
 * transient network issues or server load. The error includes the operation
 * type for easier programmatic handling.
 *
 * @example
 * ```typescript
 * try {
 *   await client.query('SELECT * FROM large_table');
 * } catch (error) {
 *   if (error instanceof TimeoutError) {
 *     console.log(`${error.operationType} timed out after ${error.timeoutMs}ms`);
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
   * The type of operation that timed out (query, exec, transaction, or rpc).
   */
  readonly operationType: TimeoutOperationType;

  /**
   * Creates a new TimeoutError.
   *
   * @param operationType - The type of operation that timed out
   * @param timeoutMs - The timeout duration in milliseconds
   */
  constructor(operationType: TimeoutOperationType, timeoutMs: number) {
    super(`${operationType} timeout after ${timeoutMs}ms`);
    this.name = 'TimeoutError';
    this.timeoutMs = timeoutMs;
    this.operationType = operationType;
  }
}

// =============================================================================
// Retryable Error Codes
// =============================================================================

/**
 * Error codes that indicate the error is retryable.
 *
 * **Retry Backoff Algorithm:**
 * When retrying these errors, use exponential backoff with jitter to avoid
 * thundering herd problems. The recommended formula is:
 *
 *   delay = min(baseDelay * 2^attempt, maxDelay) + random(0, jitter)
 *
 * Example with baseDelay=100ms, maxDelay=10000ms:
 *   - Attempt 0: 100ms  (100 * 2^0 = 100)
 *   - Attempt 1: 200ms  (100 * 2^1 = 200)
 *   - Attempt 2: 400ms  (100 * 2^2 = 400)
 *   - Attempt 3: 800ms  (100 * 2^3 = 800)
 *   - Attempt 4: 1600ms (100 * 2^4 = 1600)
 *
 * **Rationale for each code:**
 * - TIMEOUT: Transient server/network overload; backing off allows recovery
 * - CONNECTION_CLOSED: WebSocket disconnected; reconnection may succeed
 * - NETWORK_ERROR: Intermittent network failure; retry after brief delay
 * - UNAVAILABLE: Server temporarily unavailable (e.g., during deployment)
 * - RESOURCE_EXHAUSTED: Rate limiting or quota; exponential backoff essential
 *
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

// =============================================================================
// Message Parse Error
// =============================================================================

/**
 * Error thrown when WebSocket message parsing fails.
 *
 * This error indicates a protocol-level failure when the client receives
 * malformed data from the server. This could be due to:
 * - Invalid JSON in the message
 * - Missing required fields in the RPC response
 * - Unexpected message format
 *
 * Unlike connection or timeout errors, message parse errors typically indicate
 * a bug or protocol mismatch and are generally not retryable.
 *
 * @example
 * ```typescript
 * client.on('error', (error) => {
 *   if (error instanceof MessageParseError) {
 *     console.error(`Protocol error: ${error.message}`);
 *     console.error(`Raw message: ${error.rawMessage?.substring(0, 100)}...`);
 *     // Report to monitoring service
 *   }
 * });
 * ```
 *
 * @public
 * @since 0.3.0
 */
export class MessageParseError extends Error {
  /**
   * Error code indicating this is a message parsing error.
   */
  readonly code = 'MESSAGE_PARSE_ERROR' as const;

  /**
   * Indicates whether this error is safe to retry.
   * Message parse errors are generally not retryable as they indicate protocol issues.
   */
  readonly retryable = false;

  /**
   * The raw message that failed to parse (truncated for safety).
   * May be undefined if the message couldn't be converted to string.
   */
  readonly rawMessage: string | undefined;

  /**
   * The underlying parsing error, if available.
   */
  readonly originalError: Error | undefined;

  /**
   * Creates a new MessageParseError.
   *
   * @param message - Description of the parsing failure
   * @param rawMessage - The raw message that failed to parse (will be truncated)
   * @param originalError - The underlying error that caused the parse failure
   */
  constructor(message: string, rawMessage?: string, originalError?: Error) {
    super(message);
    this.name = 'MessageParseError';
    // Truncate raw message to prevent memory issues with large malformed messages
    this.rawMessage = rawMessage !== undefined ? rawMessage.substring(0, 1000) : undefined;
    this.originalError = originalError;
  }
}
