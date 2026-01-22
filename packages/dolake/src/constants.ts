/**
 * DoLake Constants
 *
 * Centralized constants for configuration values, limits, and thresholds.
 * These replace magic numbers throughout the codebase for better maintainability.
 */

// =============================================================================
// Rate Limiting Constants
// =============================================================================

/**
 * Rate limit configuration constants
 */
export const RATE_LIMIT = {
  /** Maximum new connections per second */
  CONNECTIONS_PER_SECOND: 20,
  /** Maximum messages per connection per second */
  MESSAGES_PER_SECOND: 100,
  /** Token bucket capacity for burst handling */
  BURST_CAPACITY: 50,
  /** Token refill rate per second */
  REFILL_RATE: 10,
  /** Maximum connections per source/client ID */
  MAX_CONNECTIONS_PER_SOURCE: 5,
  /** Maximum connections per IP */
  MAX_CONNECTIONS_PER_IP: 30,
  /** Subnet-level rate limiting threshold for /24 subnet */
  SUBNET_RATE_LIMIT_THRESHOLD: 100,
  /** Rate limit window in milliseconds */
  WINDOW_MS: 1000,
  /** Buffer utilization threshold for backpressure signaling (0-1) */
  BACKPRESSURE_THRESHOLD: 0.8,
  /** Base retry delay for exponential backoff (ms) */
  BASE_RETRY_DELAY_MS: 100,
  /** Maximum retry delay for exponential backoff (ms) */
  MAX_RETRY_DELAY_MS: 30000,
  /** Number of size violations before connection close */
  MAX_SIZE_VIOLATIONS: 3,
  /** Maximum events per CDC poll */
  MAX_EVENTS_PER_POLL: 100,
  /** CDC poll timeout in milliseconds */
  CDC_POLL_TIMEOUT_MS: 100,
  /** Maximum retry exponential backoff power */
  MAX_EXPONENTIAL_BACKOFF_POWER: 10,
} as const;

// =============================================================================
// Size Limit Constants
// =============================================================================

/**
 * Size limits for payloads and buffers
 */
export const SIZE_LIMITS = {
  /** Maximum payload size in bytes (4MB) */
  MAX_PAYLOAD_SIZE: 4 * 1024 * 1024,
  /** Maximum individual event size in bytes (1MB) */
  MAX_EVENT_SIZE: 1 * 1024 * 1024,
  /** Maximum buffer size in bytes (128MB) */
  MAX_BUFFER_SIZE: 128 * 1024 * 1024,
  /** Flush threshold size in bytes (32MB) */
  FLUSH_THRESHOLD_BYTES: 32 * 1024 * 1024,
  /** Maximum fallback storage size (64MB) */
  MAX_FALLBACK_SIZE: 64 * 1024 * 1024,
  /** Kilobyte in bytes */
  KB: 1024,
  /** Megabyte in bytes */
  MB: 1024 * 1024,
} as const;

// =============================================================================
// Timeout Constants
// =============================================================================

/**
 * Timeout configuration values in milliseconds
 */
export const TIMEOUTS = {
  /** Default flush threshold time (60 seconds) */
  FLUSH_THRESHOLD_MS: 60_000,
  /** Default flush interval (30 seconds) */
  FLUSH_INTERVAL_MS: 30_000,
  /** Deduplication window (5 minutes) */
  DEDUPLICATION_WINDOW_MS: 300_000,
  /** Buffer full backpressure delay (1 second) */
  BUFFER_FULL_DELAY_MS: 1000,
  /** Backpressure multiplier base (500ms) */
  BACKPRESSURE_MULTIPLIER_MS: 500,
  /** Maximum rate limit value validation (24 hours) */
  MAX_RESET_TIME_OFFSET_SECONDS: 24 * 60 * 60,
} as const;

// =============================================================================
// Threshold Constants
// =============================================================================

/**
 * Event count and batch thresholds
 */
export const THRESHOLDS = {
  /** Maximum events before flush */
  FLUSH_THRESHOLD_EVENTS: 10000,
  /** Target Parquet row group size */
  PARQUET_ROW_GROUP_SIZE: 100_000,
  /** Maximum deduplication entries */
  MAX_DEDUP_ENTRIES: 100_000,
  /** Load level threshold - critical (0.9) */
  LOAD_LEVEL_CRITICAL: 0.9,
  /** Load level threshold - high (0.7) */
  LOAD_LEVEL_HIGH: 0.7,
  /** Load level threshold - elevated (0.5) */
  LOAD_LEVEL_ELEVATED: 0.5,
  /** Connection multiplier for load calculation */
  LOAD_CONNECTION_MULTIPLIER: 10,
} as const;

// =============================================================================
// Validation Constants
// =============================================================================

/**
 * Validation boundaries for rate limit values
 */
export const VALIDATION = {
  /** Minimum valid rate limit value */
  MIN_RATE_LIMIT_VALUE: 0,
  /** Maximum valid rate limit value */
  MAX_RATE_LIMIT_VALUE: 1_000_000,
  /** Minimum delay value (ms) */
  MIN_DELAY_MS: 100,
} as const;

// =============================================================================
// WebSocket Constants
// =============================================================================

/**
 * WebSocket configuration
 */
export const WEBSOCKET = {
  /** WebSocket close code for policy violation */
  CLOSE_CODE_POLICY_VIOLATION: 1008,
  /** Heartbeat bucket multiplier (more lenient than message bucket) */
  HEARTBEAT_BUCKET_MULTIPLIER: 2,
  /** Minimum jitter multiplier for retry delay (0.5x base delay) */
  JITTER_MIN: 0.5,
  /** Maximum jitter multiplier for retry delay (1.5x base delay) */
  JITTER_MAX: 1.5,
} as const;

// =============================================================================
// Buffer Constants
// =============================================================================

/**
 * Buffer estimation values
 */
export const BUFFER = {
  /** Base overhead for event size estimation (bytes) */
  EVENT_SIZE_BASE_OVERHEAD: 100,
} as const;

// =============================================================================
// Whitelisted Networks
// =============================================================================

/**
 * Default whitelisted IP ranges (private networks)
 */
export const WHITELISTED_NETWORKS = [
  '10.0.0.0/8',
  '172.16.0.0/12',
  '192.168.0.0/16',
  '127.0.0.1',
] as const;
