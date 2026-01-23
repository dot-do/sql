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
 * Rate limit configuration constants.
 *
 * These constants control DoLake's rate limiting behavior to protect against
 * abuse, ensure fair resource allocation, and maintain system stability under load.
 */
export const RATE_LIMIT = {
  /**
   * Maximum new WebSocket connections accepted per second.
   * Controls the rate at which new clients can establish connections.
   * Prevents connection flood attacks and ensures graceful scaling.
   */
  CONNECTIONS_PER_SECOND: 20,

  /**
   * Maximum messages a single connection can send per second.
   * Limits message throughput per client to prevent a single connection
   * from monopolizing server resources or overwhelming downstream systems.
   */
  MESSAGES_PER_SECOND: 100,

  /**
   * Token bucket capacity for handling message bursts.
   * Allows temporary spikes above the sustained rate limit.
   * Clients can send up to this many messages instantly before being throttled.
   */
  BURST_CAPACITY: 50,

  /**
   * Token refill rate per second for the token bucket algorithm.
   * Determines how quickly capacity is restored after a burst.
   * Lower values = stricter sustained rate limiting.
   */
  REFILL_RATE: 10,

  /**
   * Maximum concurrent connections allowed per source/client ID.
   * Prevents a single authenticated client from opening excessive connections.
   * Helps ensure fair connection distribution across all clients.
   */
  MAX_CONNECTIONS_PER_SOURCE: 5,

  /**
   * Maximum concurrent connections allowed per IP address.
   * Protects against connection exhaustion from a single IP.
   * Set higher than per-source limit to allow multiple clients behind NAT.
   */
  MAX_CONNECTIONS_PER_IP: 30,

  /**
   * Connection threshold for /24 subnet-level rate limiting.
   * When connections from a subnet exceed this threshold, stricter limits apply.
   * Defends against distributed attacks from similar IP ranges.
   */
  SUBNET_RATE_LIMIT_THRESHOLD: 100,

  /**
   * Sliding window duration for rate calculations in milliseconds.
   * Defines the time period over which rates are measured.
   * Shorter windows are more responsive but less stable.
   */
  WINDOW_MS: 1000,

  /**
   * Buffer utilization threshold (0-1) that triggers backpressure signaling.
   * When buffer usage exceeds 80%, clients receive backpressure signals
   * instructing them to slow down message transmission.
   */
  BACKPRESSURE_THRESHOLD: 0.8,

  /**
   * Base delay in milliseconds for exponential backoff retries.
   * Starting point for retry delays when operations fail.
   * Actual delay = BASE_RETRY_DELAY_MS * 2^attempt with jitter.
   */
  BASE_RETRY_DELAY_MS: 100,

  /**
   * Maximum delay in milliseconds for exponential backoff retries.
   * Caps retry delays to prevent excessively long waits.
   * Ensures clients eventually retry even after many failures.
   */
  MAX_RETRY_DELAY_MS: 30000,

  /**
   * Number of message size violations before forcibly closing a connection.
   * Allows for occasional accidental oversized messages while protecting
   * against clients that persistently send invalid payloads.
   */
  MAX_SIZE_VIOLATIONS: 3,

  /**
   * Maximum number of CDC events returned per poll operation.
   * Bounds the batch size for change data capture polling.
   * Balances latency (smaller batches) vs throughput (larger batches).
   */
  MAX_EVENTS_PER_POLL: 100,

  /**
   * Timeout in milliseconds for CDC poll operations.
   * How long to wait for events before returning an empty response.
   * Short timeout enables responsive long-polling behavior.
   */
  CDC_POLL_TIMEOUT_MS: 100,

  /**
   * Maximum exponent for exponential backoff calculations.
   * Limits delay = BASE_RETRY_DELAY_MS * 2^(min(attempt, MAX_EXPONENTIAL_BACKOFF_POWER)).
   * Prevents integer overflow and ensures MAX_RETRY_DELAY_MS is the effective cap.
   */
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
