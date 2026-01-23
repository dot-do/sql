/**
 * @dotdo/lake.do - Constants
 *
 * This module provides named constants for the lake.do client.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

// =============================================================================
// WebSocket Ready States
// =============================================================================

/**
 * WebSocket ready state constants.
 *
 * @description Named constants for WebSocket readyState values to improve code readability.
 * These mirror the standard WebSocket constants (CONNECTING=0, OPEN=1, CLOSING=2, CLOSED=3).
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const WebSocketState = {
  /** Connection is being established */
  CONNECTING: 0,
  /** Connection is open and ready to communicate */
  OPEN: 1,
  /** Connection is in the process of closing */
  CLOSING: 2,
  /** Connection is closed or couldn't be opened */
  CLOSED: 3,
} as const;

/**
 * Type representing valid WebSocket ready states.
 */
export type WebSocketState = typeof WebSocketState[keyof typeof WebSocketState];

// =============================================================================
// Default Configuration Values
// =============================================================================

/**
 * Default timeout for RPC requests in milliseconds.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const DEFAULT_TIMEOUT_MS = 30000;

/**
 * Default maximum queue size for CDC stream controller.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const DEFAULT_MAX_QUEUE_SIZE = 1000;

/**
 * Minimum allowed queue size for CDC stream controller.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const MIN_QUEUE_SIZE = 1;

// =============================================================================
// Error Codes
// =============================================================================

/**
 * Error codes for lake.do client errors.
 *
 * @description Named constants for error codes to ensure consistency
 * across error handling and make code more self-documenting.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const ErrorCode = {
  // Connection errors
  CONNECTION_ERROR: 'CONNECTION_ERROR',
  CONNECTION_CLOSED: 'CONNECTION_CLOSED',
  CONNECTION_TIMEOUT: 'CONNECTION_TIMEOUT',
  NOT_CONNECTED: 'NOT_CONNECTED',

  // Query errors
  INVALID_SQL: 'INVALID_SQL',
  QUERY_TIMEOUT: 'QUERY_TIMEOUT',

  // Table/Partition errors
  TABLE_NOT_FOUND: 'TABLE_NOT_FOUND',
  PARTITION_NOT_FOUND: 'PARTITION_NOT_FOUND',

  // Authentication errors
  UNAUTHORIZED: 'UNAUTHORIZED',
  TOKEN_EXPIRED: 'TOKEN_EXPIRED',

  // General errors
  TIMEOUT: 'TIMEOUT',
  INTERNAL_ERROR: 'INTERNAL_ERROR',
} as const;

/**
 * Type representing valid error codes.
 */
export type ErrorCode = typeof ErrorCode[keyof typeof ErrorCode];
