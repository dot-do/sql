/**
 * DoSQL Constants
 *
 * Centralized constants for configuration values, limits, and thresholds.
 * These replace magic numbers throughout the codebase for better maintainability.
 */

// =============================================================================
// Timeout Constants
// =============================================================================

/**
 * Timeout configuration values in milliseconds
 */
export const TIMEOUTS = {
  /** Default query timeout (30 seconds) */
  DEFAULT_QUERY_MS: 30000,
  /** Default transaction timeout (30 seconds) */
  DEFAULT_TRANSACTION_MS: 30000,
  /** Default procedure timeout (5 seconds) */
  DEFAULT_PROCEDURE_MS: 5000,
  /** Default busy timeout for database (5 seconds) */
  DEFAULT_BUSY_TIMEOUT_MS: 5000,
  /** Stream cleanup alarm buffer (1 minute) */
  STREAM_CLEANUP_BUFFER_MS: 60000,
  /** Default stream TTL (30 minutes) */
  DEFAULT_STREAM_TTL_MS: 30 * 60 * 1000,
  /** Default retry delay for procedures (100ms) */
  DEFAULT_RETRY_DELAY_MS: 100,
} as const;

// =============================================================================
// Size Limit Constants
// =============================================================================

/**
 * Size limits for buffers and caches
 */
export const SIZE_LIMITS = {
  /** Default statement cache size */
  DEFAULT_STATEMENT_CACHE_SIZE: 100,
  /** Default procedure memory limit (MB) */
  DEFAULT_PROCEDURE_MEMORY_MB: 128,
  /** Default chunk size for streaming */
  DEFAULT_CHUNK_SIZE: 1000,
} as const;

// =============================================================================
// CDC Constants
// =============================================================================

/**
 * CDC (Change Data Capture) configuration
 */
export const CDC = {
  /** Maximum events per poll */
  MAX_EVENTS_PER_POLL: 100,
  /** CDC poll timeout (100ms) */
  POLL_TIMEOUT_MS: 100,
} as const;

// =============================================================================
// Stream Constants
// =============================================================================

/**
 * Stream management configuration
 */
export const STREAMS = {
  /** Maximum concurrent streams per connection */
  MAX_CONCURRENT_STREAMS: 100,
} as const;

// =============================================================================
// Database Pragma Defaults
// =============================================================================

/**
 * Default pragma values for database configuration
 */
export const PRAGMA_DEFAULTS = {
  /** Default cache size (pages, negative means KB) */
  CACHE_SIZE: -2000,
  /** Default page size (bytes) */
  PAGE_SIZE: 4096,
  /** Default synchronous mode */
  SYNCHRONOUS: 2,
  /** Default foreign keys enabled */
  FOREIGN_KEYS: 1,
} as const;

// =============================================================================
// HTTP Status Codes
// =============================================================================

/**
 * HTTP status codes used in responses
 */
export const HTTP_STATUS = {
  /** OK */
  OK: 200,
  /** Not Found */
  NOT_FOUND: 404,
  /** Internal Server Error */
  INTERNAL_ERROR: 500,
} as const;
