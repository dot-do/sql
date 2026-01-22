/**
 * Rate Limiter for DoLake WebSocket connections
 *
 * Implements:
 * - Token bucket algorithm for burst handling
 * - Per-connection rate limiting
 * - Per-IP rate limiting
 * - Connection rate limiting
 * - Payload size limits
 * - Backpressure signaling
 */

import {
  RATE_LIMIT,
  SIZE_LIMITS,
  TIMEOUTS,
  THRESHOLDS,
  VALIDATION,
  WEBSOCKET,
  WHITELISTED_NETWORKS,
} from './constants.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Rate limit configuration
 */
export interface RateLimitConfig {
  /** Maximum new connections per second */
  connectionsPerSecond: number;

  /** Maximum messages per connection per second */
  messagesPerSecond: number;

  /** Token bucket capacity for burst handling */
  burstCapacity: number;

  /** Token refill rate per second */
  refillRate: number;

  /** Maximum payload size in bytes */
  maxPayloadSize: number;

  /** Maximum individual event size in bytes */
  maxEventSize: number;

  /** Maximum connections per source/client ID */
  maxConnectionsPerSource: number;

  /** Maximum connections per IP */
  maxConnectionsPerIp: number;

  /** Subnet-level rate limiting threshold for /24 subnet */
  subnetRateLimitThreshold: number;

  /** Whitelisted IPs (no rate limiting) */
  whitelistedIps: string[];

  /** Rate limit window in milliseconds */
  windowMs: number;

  /** Buffer utilization threshold for backpressure signaling */
  backpressureThreshold: number;

  /** High-priority header value */
  highPriorityHeader: string;

  /** Base retry delay for exponential backoff */
  baseRetryDelayMs: number;

  /** Max retry delay for exponential backoff */
  maxRetryDelayMs: number;

  /** Number of size violations before connection close */
  maxSizeViolations: number;
}

/**
 * Default rate limit configuration
 */
export const DEFAULT_RATE_LIMIT_CONFIG: RateLimitConfig = {
  connectionsPerSecond: RATE_LIMIT.CONNECTIONS_PER_SECOND,
  messagesPerSecond: RATE_LIMIT.MESSAGES_PER_SECOND,
  burstCapacity: RATE_LIMIT.BURST_CAPACITY,
  refillRate: RATE_LIMIT.REFILL_RATE,
  maxPayloadSize: SIZE_LIMITS.MAX_PAYLOAD_SIZE,
  maxEventSize: SIZE_LIMITS.MAX_EVENT_SIZE,
  maxConnectionsPerSource: RATE_LIMIT.MAX_CONNECTIONS_PER_SOURCE,
  maxConnectionsPerIp: RATE_LIMIT.MAX_CONNECTIONS_PER_IP,
  subnetRateLimitThreshold: RATE_LIMIT.SUBNET_RATE_LIMIT_THRESHOLD,
  whitelistedIps: [...WHITELISTED_NETWORKS],
  windowMs: RATE_LIMIT.WINDOW_MS,
  backpressureThreshold: RATE_LIMIT.BACKPRESSURE_THRESHOLD,
  highPriorityHeader: 'X-Priority',
  baseRetryDelayMs: RATE_LIMIT.BASE_RETRY_DELAY_MS,
  maxRetryDelayMs: RATE_LIMIT.MAX_RETRY_DELAY_MS,
  maxSizeViolations: RATE_LIMIT.MAX_SIZE_VIOLATIONS,
};

/**
 * Token bucket for rate limiting
 */
export interface TokenBucket {
  /** Current number of tokens */
  tokens: number;

  /** Maximum tokens (bucket capacity) */
  capacity: number;

  /** Refill rate (tokens per second) */
  refillRate: number;

  /** Last refill timestamp */
  lastRefillTime: number;
}

/**
 * Rate limit info for responses
 */
export interface RateLimitInfo {
  /** Maximum allowed requests */
  limit: number;

  /** Remaining requests in window */
  remaining: number;

  /** Reset timestamp (Unix seconds) */
  resetAt: number;
}

/**
 * Rate limit check result
 */
export interface RateLimitResult {
  /** Whether the request is allowed */
  allowed: boolean;

  /** Rate limit info */
  rateLimit: RateLimitInfo;

  /** Reason for rejection if not allowed */
  reason?:
    | 'rate_limited'
    | 'payload_too_large'
    | 'event_too_large'
    | 'connection_limit'
    | 'ip_limit'
    | 'buffer_full'
    | 'load_shedding'
    | undefined;

  /** Suggested retry delay in ms */
  retryDelayMs?: number | undefined;

  /** Remaining tokens in bucket */
  remainingTokens?: number | undefined;

  /** Bucket capacity */
  bucketCapacity?: number | undefined;

  /** Buffer utilization (0-1) */
  bufferUtilization?: number | undefined;

  /** Suggested delay for backpressure */
  suggestedDelayMs?: number | undefined;

  /** Max allowed size (for size violations) */
  maxSize?: number | undefined;

  /** Masked client IP (for logging) */
  clientIp?: string | undefined;
}

/**
 * Connection rate limiter state
 */
export interface ConnectionRateLimitState {
  /** Connection count in current window */
  count: number;

  /** Window start time */
  windowStart: number;
}

/**
 * Per-connection state
 */
export interface ConnectionState {
  /** Message token bucket */
  messageBucket: TokenBucket;

  /** Heartbeat token bucket (separate, more lenient) */
  heartbeatBucket: TokenBucket;

  /** Size violation count */
  sizeViolationCount: number;

  /** Last rate limit timestamp */
  lastRateLimitTime?: number;

  /** Consecutive rate limit count (for exponential backoff) */
  consecutiveRateLimits: number;

  /** Connection priority */
  priority: 'high' | 'normal' | 'low';

  /** Connected at timestamp */
  connectedAt: number;
}

/**
 * Per-IP state
 */
export interface IpState {
  /** Connection count */
  connectionCount: number;

  /** Message count in window */
  messageCount: number;

  /** Window start time */
  windowStart: number;
}

/**
 * Rate limiting metrics
 */
export interface RateLimitMetrics {
  /** Total connections rate limited */
  connectionsRateLimited: number;

  /** Total messages rate limited */
  messagesRateLimited: number;

  /** Total payload rejections */
  payloadRejections: number;

  /** Total event size rejections */
  eventSizeRejections: number;

  /** Total connections closed for violations */
  connectionsClosed: number;

  /** Total IP-based rejections */
  ipRejections: number;

  /** Total load shedding events */
  loadSheddingEvents: number;

  /** Current active connections */
  activeConnections: number;

  /** Peak connections */
  peakConnections: number;
}

// =============================================================================
// Rate Limiter Class
// =============================================================================

/**
 * Rate limiter for DoLake WebSocket connections
 */
export class RateLimiter {
  private readonly config: RateLimitConfig;
  private readonly connectionRateLimit: ConnectionRateLimitState;
  private readonly connections: Map<string, ConnectionState> = new Map();
  private readonly ipStates: Map<string, IpState> = new Map();
  private readonly subnetStates: Map<string, IpState> = new Map();
  private readonly sourceConnections: Map<string, Set<string>> = new Map();
  private readonly metrics: RateLimitMetrics = {
    connectionsRateLimited: 0,
    messagesRateLimited: 0,
    payloadRejections: 0,
    eventSizeRejections: 0,
    connectionsClosed: 0,
    ipRejections: 0,
    loadSheddingEvents: 0,
    activeConnections: 0,
    peakConnections: 0,
  };

  private degradedMode: boolean = false;
  private loadLevel: 'normal' | 'elevated' | 'high' | 'critical' = 'normal';

  constructor(config: Partial<RateLimitConfig> = {}) {
    this.config = { ...DEFAULT_RATE_LIMIT_CONFIG, ...config };
    this.connectionRateLimit = {
      count: 0,
      windowStart: Date.now(),
    };
  }

  // ===========================================================================
  // Connection Rate Limiting
  // ===========================================================================

  /**
   * Check if a new connection is allowed
   */
  checkConnection(
    clientId: string,
    ip?: string,
    priority?: string
  ): RateLimitResult {
    const now = Date.now();

    // Reset window if expired
    if (now - this.connectionRateLimit.windowStart >= this.config.windowMs) {
      this.connectionRateLimit.count = 0;
      this.connectionRateLimit.windowStart = now;
    }

    // Check IP whitelist
    if (ip && this.isWhitelisted(ip)) {
      return this.allowedResult(now);
    }

    // Check per-source limit
    const sourceConns = this.sourceConnections.get(clientId);
    if (
      sourceConns &&
      sourceConns.size >= this.config.maxConnectionsPerSource
    ) {
      this.metrics.connectionsRateLimited++;
      return this.rejectedResult(
        now,
        'connection_limit',
        'Maximum connections per source exceeded'
      );
    }

    // Check per-IP limit
    if (ip) {
      const ipState = this.getOrCreateIpState(ip);
      if (ipState.connectionCount >= this.config.maxConnectionsPerIp) {
        this.metrics.ipRejections++;
        return this.rejectedResult(
          now,
          'ip_limit',
          'Maximum connections per IP exceeded',
          this.maskIp(ip)
        );
      }

      // Check subnet limit
      const subnet = this.getSubnet(ip);
      if (subnet) {
        const subnetState = this.getOrCreateSubnetState(subnet);
        if (
          subnetState.connectionCount >= this.config.subnetRateLimitThreshold
        ) {
          this.metrics.ipRejections++;
          return this.rejectedResult(
            now,
            'ip_limit',
            'Subnet rate limit exceeded',
            this.maskIp(ip)
          );
        }
      }
    }

    // Check connection rate limit
    if (this.connectionRateLimit.count >= this.config.connectionsPerSecond) {
      this.metrics.connectionsRateLimited++;
      return this.rejectedResult(
        now,
        'rate_limited',
        'Connection rate limit exceeded',
        ip ? this.maskIp(ip) : undefined
      );
    }

    // Increment connection count
    this.connectionRateLimit.count++;

    return this.allowedResult(now);
  }

  /**
   * Register a new connection
   */
  registerConnection(
    connectionId: string,
    clientId: string,
    ip?: string,
    priority?: string
  ): void {
    const now = Date.now();
    const connPriority: 'high' | 'normal' | 'low' =
      priority === 'high' ? 'high' : 'normal';

    // Create connection state with token buckets
    const state: ConnectionState = {
      messageBucket: this.createBucket(
        this.config.burstCapacity,
        this.config.refillRate
      ),
      heartbeatBucket: this.createBucket(
        this.config.burstCapacity * WEBSOCKET.HEARTBEAT_BUCKET_MULTIPLIER,
        this.config.refillRate * WEBSOCKET.HEARTBEAT_BUCKET_MULTIPLIER
      ),
      sizeViolationCount: 0,
      consecutiveRateLimits: 0,
      priority: connPriority,
      connectedAt: now,
    };

    this.connections.set(connectionId, state);
    this.metrics.activeConnections++;

    if (this.metrics.activeConnections > this.metrics.peakConnections) {
      this.metrics.peakConnections = this.metrics.activeConnections;
    }

    // Track source connections
    let sourceConns = this.sourceConnections.get(clientId);
    if (!sourceConns) {
      sourceConns = new Set();
      this.sourceConnections.set(clientId, sourceConns);
    }
    sourceConns.add(connectionId);

    // Track IP connections
    if (ip) {
      const ipState = this.getOrCreateIpState(ip);
      ipState.connectionCount++;

      const subnet = this.getSubnet(ip);
      if (subnet) {
        const subnetState = this.getOrCreateSubnetState(subnet);
        subnetState.connectionCount++;
      }
    }

    this.updateLoadLevel();
  }

  /**
   * Unregister a connection
   */
  unregisterConnection(
    connectionId: string,
    clientId: string,
    ip?: string
  ): void {
    this.connections.delete(connectionId);
    this.metrics.activeConnections--;

    // Remove from source tracking
    const sourceConns = this.sourceConnections.get(clientId);
    if (sourceConns) {
      sourceConns.delete(connectionId);
      if (sourceConns.size === 0) {
        this.sourceConnections.delete(clientId);
      }
    }

    // Update IP tracking
    if (ip) {
      const ipState = this.ipStates.get(ip);
      if (ipState) {
        ipState.connectionCount--;
        if (ipState.connectionCount <= 0) {
          this.ipStates.delete(ip);
        }
      }

      const subnet = this.getSubnet(ip);
      if (subnet) {
        const subnetState = this.subnetStates.get(subnet);
        if (subnetState) {
          subnetState.connectionCount--;
          if (subnetState.connectionCount <= 0) {
            this.subnetStates.delete(subnet);
          }
        }
      }
    }

    this.updateLoadLevel();
  }

  // ===========================================================================
  // Message Rate Limiting
  // ===========================================================================

  /**
   * Check if a message is allowed
   */
  checkMessage(
    connectionId: string,
    messageType: string,
    payloadSize: number,
    eventSizes: number[] = [],
    bufferUtilization: number = 0
  ): RateLimitResult {
    const now = Date.now();
    const state = this.connections.get(connectionId);

    if (!state) {
      return this.rejectedResult(now, 'rate_limited', 'Unknown connection');
    }

    // Check payload size
    if (payloadSize > this.config.maxPayloadSize) {
      state.sizeViolationCount++;
      this.metrics.payloadRejections++;

      return {
        allowed: false,
        rateLimit: this.getRateLimitInfo(state, now),
        reason: 'payload_too_large',
        maxSize: this.config.maxPayloadSize,
        retryDelayMs: 0, // Don't retry with same payload
      };
    }

    // Check individual event sizes
    for (const eventSize of eventSizes) {
      if (eventSize > this.config.maxEventSize) {
        state.sizeViolationCount++;
        this.metrics.eventSizeRejections++;

        return {
          allowed: false,
          rateLimit: this.getRateLimitInfo(state, now),
          reason: 'event_too_large',
          maxSize: this.config.maxEventSize,
          retryDelayMs: 0,
        };
      }
    }

    // Check buffer backpressure
    if (bufferUtilization >= 1.0) {
      return {
        allowed: false,
        rateLimit: this.getRateLimitInfo(state, now),
        reason: 'buffer_full',
        bufferUtilization,
        suggestedDelayMs: TIMEOUTS.BUFFER_FULL_DELAY_MS,
        retryDelayMs: TIMEOUTS.BUFFER_FULL_DELAY_MS,
      };
    }

    // Check for load shedding under extreme load
    if (
      this.loadLevel === 'critical' &&
      state.priority !== 'high' &&
      messageType === 'cdc_batch'
    ) {
      this.metrics.loadSheddingEvents++;
      return {
        allowed: false,
        rateLimit: this.getRateLimitInfo(state, now),
        reason: 'load_shedding',
        suggestedDelayMs: this.calculateRetryDelay(state),
        retryDelayMs: this.calculateRetryDelay(state),
      };
    }

    // Select appropriate bucket based on message type
    const bucket =
      messageType === 'heartbeat' ? state.heartbeatBucket : state.messageBucket;

    // Refill bucket
    this.refillBucket(bucket, now);

    // Check if tokens available
    if (bucket.tokens < 1) {
      state.consecutiveRateLimits++;
      state.lastRateLimitTime = now;
      this.metrics.messagesRateLimited++;

      const retryDelay = this.calculateRetryDelay(state);

      return {
        allowed: false,
        rateLimit: this.getRateLimitInfo(state, now),
        reason: 'rate_limited',
        retryDelayMs: retryDelay,
        remainingTokens: 0,
        bucketCapacity: bucket.capacity,
        bufferUtilization,
        suggestedDelayMs:
          bufferUtilization > this.config.backpressureThreshold
            ? Math.floor(bufferUtilization * TIMEOUTS.BUFFER_FULL_DELAY_MS)
            : undefined,
      };
    }

    // Consume token
    bucket.tokens -= 1;
    state.consecutiveRateLimits = 0;

    return {
      allowed: true,
      rateLimit: this.getRateLimitInfo(state, now),
      remainingTokens: Math.floor(bucket.tokens),
      bucketCapacity: bucket.capacity,
      bufferUtilization,
      suggestedDelayMs:
        bufferUtilization > this.config.backpressureThreshold
          ? Math.floor(bufferUtilization * TIMEOUTS.BACKPRESSURE_MULTIPLIER_MS)
          : undefined,
    };
  }

  /**
   * Check if connection should be closed due to violations
   */
  shouldCloseConnection(connectionId: string): boolean {
    const state = this.connections.get(connectionId);
    if (!state) return false;

    if (state.sizeViolationCount >= this.config.maxSizeViolations) {
      this.metrics.connectionsClosed++;
      return true;
    }

    return false;
  }

  // ===========================================================================
  // Token Bucket Operations
  // ===========================================================================

  private createBucket(capacity: number, refillRate: number): TokenBucket {
    return {
      tokens: capacity,
      capacity,
      refillRate,
      lastRefillTime: Date.now(),
    };
  }

  private refillBucket(bucket: TokenBucket, now: number): void {
    const elapsed = (now - bucket.lastRefillTime) / 1000;
    const tokensToAdd = elapsed * bucket.refillRate;

    bucket.tokens = Math.min(bucket.capacity, bucket.tokens + tokensToAdd);
    bucket.lastRefillTime = now;
  }

  // ===========================================================================
  // IP Handling
  // ===========================================================================

  private getOrCreateIpState(ip: string): IpState {
    let state = this.ipStates.get(ip);
    if (!state) {
      state = {
        connectionCount: 0,
        messageCount: 0,
        windowStart: Date.now(),
      };
      this.ipStates.set(ip, state);
    }
    return state;
  }

  private getOrCreateSubnetState(subnet: string): IpState {
    let state = this.subnetStates.get(subnet);
    if (!state) {
      state = {
        connectionCount: 0,
        messageCount: 0,
        windowStart: Date.now(),
      };
      this.subnetStates.set(subnet, state);
    }
    return state;
  }

  private getSubnet(ip: string): string | null {
    // Extract /24 subnet from IPv4
    const parts = ip.split('.');
    if (parts.length === 4) {
      return `${parts[0]}.${parts[1]}.${parts[2]}.0/24`;
    }

    // For IPv6, use /64 subnet
    if (ip.includes(':')) {
      const colonParts = ip.split(':');
      if (colonParts.length >= 4) {
        return `${colonParts.slice(0, 4).join(':')}::/64`;
      }
    }

    return null;
  }

  private isWhitelisted(ip: string): boolean {
    for (const whitelist of this.config.whitelistedIps) {
      if (whitelist === ip) return true;

      // Check CIDR notation
      if (whitelist.includes('/')) {
        if (this.isInCidr(ip, whitelist)) {
          return true;
        }
      }
    }
    return false;
  }

  private isInCidr(ip: string, cidr: string): boolean {
    const parts = cidr.split('/');
    const network = parts[0];
    const maskBits = parts[1];
    if (!network || !maskBits) return false;
    const _mask = parseInt(maskBits, 10);

    // Simple check for private ranges
    if (cidr === '10.0.0.0/8' && ip.startsWith('10.')) return true;
    if (cidr === '172.16.0.0/12' && ip.startsWith('172.')) {
      const secondOctet = ip.split('.')[1];
      if (secondOctet) {
        const octetValue = parseInt(secondOctet, 10);
        if (octetValue >= 16 && octetValue <= 31) return true;
      }
    }
    if (cidr === '192.168.0.0/16' && ip.startsWith('192.168.')) return true;

    return false;
  }

  private maskIp(ip: string): string {
    // Mask last octet for privacy
    const parts = ip.split('.');
    if (parts.length === 4) {
      return `${parts[0]}.${parts[1]}.${parts[2]}.xxx`;
    }
    return ip.replace(/:[^:]+$/, ':xxxx');
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  private calculateRetryDelay(state: ConnectionState): number {
    // Exponential backoff
    const baseDelay = this.config.baseRetryDelayMs;
    const multiplier = Math.pow(2, Math.min(state.consecutiveRateLimits, RATE_LIMIT.MAX_EXPONENTIAL_BACKOFF_POWER));
    const delay = Math.min(
      baseDelay * multiplier,
      this.config.maxRetryDelayMs
    );
    // Add jitter
    return Math.floor(delay * (WEBSOCKET.JITTER_MIN + Math.random() * WEBSOCKET.JITTER_MIN));
  }

  private getRateLimitInfo(state: ConnectionState, now: number): RateLimitInfo {
    const windowEnd = Math.ceil((now + this.config.windowMs) / 1000);

    return {
      limit: this.config.messagesPerSecond,
      remaining: Math.floor(state.messageBucket.tokens),
      resetAt: windowEnd,
    };
  }

  private allowedResult(now: number): RateLimitResult {
    return {
      allowed: true,
      rateLimit: {
        limit: this.config.connectionsPerSecond,
        remaining:
          this.config.connectionsPerSecond - this.connectionRateLimit.count,
        resetAt: Math.ceil(
          (this.connectionRateLimit.windowStart + this.config.windowMs) / 1000
        ),
      },
    };
  }

  private rejectedResult(
    now: number,
    reason: RateLimitResult['reason'],
    _message: string,
    clientIp?: string
  ): RateLimitResult {
    const windowEnd = Math.ceil(
      (this.connectionRateLimit.windowStart + this.config.windowMs) / 1000
    );
    const retryDelay = Math.max(
      VALIDATION.MIN_DELAY_MS,
      (this.connectionRateLimit.windowStart +
        this.config.windowMs -
        now) as number
    );

    return {
      allowed: false,
      rateLimit: {
        limit: this.config.connectionsPerSecond,
        remaining: 0,
        resetAt: windowEnd,
      },
      reason,
      retryDelayMs: retryDelay,
      clientIp,
    };
  }

  private updateLoadLevel(): void {
    const utilizationRatio =
      this.metrics.activeConnections / (this.config.connectionsPerSecond * THRESHOLDS.LOAD_CONNECTION_MULTIPLIER);

    if (utilizationRatio > THRESHOLDS.LOAD_LEVEL_CRITICAL) {
      this.loadLevel = 'critical';
      this.degradedMode = true;
    } else if (utilizationRatio > THRESHOLDS.LOAD_LEVEL_HIGH) {
      this.loadLevel = 'high';
      this.degradedMode = true;
    } else if (utilizationRatio > THRESHOLDS.LOAD_LEVEL_ELEVATED) {
      this.loadLevel = 'elevated';
      this.degradedMode = false;
    } else {
      this.loadLevel = 'normal';
      this.degradedMode = false;
    }
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Get current metrics
   */
  getMetrics(): RateLimitMetrics {
    return { ...this.metrics };
  }

  /**
   * Get current configuration
   */
  getConfig(): RateLimitConfig {
    return { ...this.config };
  }

  /**
   * Check if in degraded mode
   */
  isDegraded(): boolean {
    return this.degradedMode;
  }

  /**
   * Get current load level
   */
  getLoadLevel(): string {
    return this.loadLevel;
  }

  /**
   * Validate that a value is a finite, non-negative number
   * Returns the validated integer value (clamped to bounds)
   * Throws if the value is invalid and cannot be sanitized
   */
  private validateRateLimitValue(
    value: unknown,
    fieldName: string,
    options: {
      minValue?: number;
      maxValue?: number;
      allowZero?: boolean;
    } = {}
  ): number {
    const { minValue = VALIDATION.MIN_RATE_LIMIT_VALUE, maxValue = VALIDATION.MAX_RATE_LIMIT_VALUE, allowZero = true } = options;

    // Type validation - must be a number
    if (typeof value !== 'number') {
      throw new Error(
        `Invalid rate limit value: ${fieldName} must be type number, got ${typeof value}`
      );
    }

    // NaN check
    if (Number.isNaN(value)) {
      throw new Error(
        `Invalid rate limit value: ${fieldName} is NaN`
      );
    }

    // Infinity check
    if (!Number.isFinite(value)) {
      throw new Error(
        `Invalid rate limit value: ${fieldName} is ${value > 0 ? 'Infinity' : '-Infinity'}`
      );
    }

    // Round floating point values to integers
    let intValue = Math.floor(value);

    // Clamp negative values to minValue (usually 0)
    if (intValue < minValue) {
      intValue = minValue;
    }

    // Clamp to max value
    if (intValue > maxValue) {
      intValue = maxValue;
    }

    // Check zero constraint
    if (!allowZero && intValue === 0) {
      intValue = 1; // Use minimum valid value
    }

    return intValue;
  }

  /**
   * Validate the rateLimit object exists and has required fields
   */
  private validateRateLimitResult(result: RateLimitResult): void {
    if (!result || typeof result !== 'object') {
      throw new Error('Invalid rate limit result: rateLimit result is required');
    }

    if (!result.rateLimit || typeof result.rateLimit !== 'object') {
      throw new Error('Invalid rate limit result: rateLimit object is required');
    }

    const { limit, remaining, resetAt } = result.rateLimit;

    // Validate all fields exist and are the right type
    if (limit === undefined || limit === null) {
      throw new Error('Invalid rate limit result: limit is required');
    }
    if (remaining === undefined || remaining === null) {
      throw new Error('Invalid rate limit result: remaining is required');
    }
    if (resetAt === undefined || resetAt === null) {
      throw new Error('Invalid rate limit result: resetAt is required');
    }
  }

  /**
   * Get HTTP headers for rate limit response
   *
   * Validates all numeric values before converting to strings:
   * - Ensures values are finite numbers (not NaN or Infinity)
   * - Clamps negative values to 0
   * - Ensures remaining <= limit
   * - Validates reset time is reasonable
   * - Validates retryDelayMs is positive when present
   */
  getRateLimitHeaders(result: RateLimitResult): Record<string, string> {
    // Validate the result structure first
    this.validateRateLimitResult(result);

    const now = Math.floor(Date.now() / 1000);
    const maxResetTime = now + TIMEOUTS.MAX_RESET_TIME_OFFSET_SECONDS;

    // Validate and sanitize limit (must be positive, max 1 million)
    const limit = this.validateRateLimitValue(
      result.rateLimit.limit,
      'limit',
      { minValue: 1, maxValue: VALIDATION.MAX_RATE_LIMIT_VALUE, allowZero: false }
    );

    // Validate and sanitize remaining (must be non-negative, capped at limit)
    let remaining = this.validateRateLimitValue(
      result.rateLimit.remaining,
      'remaining',
      { minValue: VALIDATION.MIN_RATE_LIMIT_VALUE, maxValue: VALIDATION.MAX_RATE_LIMIT_VALUE, allowZero: true }
    );

    // Ensure remaining does not exceed limit
    if (remaining > limit) {
      remaining = limit;
    }

    // Validate and sanitize resetAt (must be in the future or at least now)
    let resetAt = this.validateRateLimitValue(
      result.rateLimit.resetAt,
      'resetAt',
      { minValue: now, maxValue: maxResetTime, allowZero: false }
    );

    const headers: Record<string, string> = {
      'X-RateLimit-Limit': limit.toString(),
      'X-RateLimit-Remaining': remaining.toString(),
      'X-RateLimit-Reset': resetAt.toString(),
    };

    // Validate retryDelayMs if present
    if (!result.allowed && result.retryDelayMs !== undefined && result.retryDelayMs !== null) {
      // Explicit validation for invalid retryDelayMs
      if (typeof result.retryDelayMs !== 'number') {
        throw new Error('Invalid rate limit value: retryDelayMs must be type number');
      }

      if (Number.isNaN(result.retryDelayMs)) {
        // NaN is falsy in condition, but we explicitly skip
        // Don't add header for NaN
      } else if (!Number.isFinite(result.retryDelayMs)) {
        // Infinity - clamp to max retry delay
        const maxSeconds = Math.ceil(this.config.maxRetryDelayMs / 1000);
        headers['Retry-After'] = maxSeconds.toString();
      } else if (result.retryDelayMs < 0) {
        // Negative values - throw error for explicit validation
        throw new Error('Invalid rate limit value: retryDelayMs must be positive');
      } else if (result.retryDelayMs === 0) {
        // Zero is falsy, don't add header
      } else {
        // Valid positive value - cap to maxRetryDelayMs
        const delayMs = Math.min(result.retryDelayMs, this.config.maxRetryDelayMs);
        const retryAfterSeconds = Math.ceil(delayMs / 1000);
        if (retryAfterSeconds > 0) {
          headers['Retry-After'] = retryAfterSeconds.toString();
        }
      }
    }

    return headers;
  }

  /**
   * Reset all state (for testing)
   */
  reset(): void {
    this.connections.clear();
    this.ipStates.clear();
    this.subnetStates.clear();
    this.sourceConnections.clear();
    this.connectionRateLimit.count = 0;
    this.connectionRateLimit.windowStart = Date.now();
    this.metrics.activeConnections = 0;
    this.degradedMode = false;
    this.loadLevel = 'normal';
  }
}
