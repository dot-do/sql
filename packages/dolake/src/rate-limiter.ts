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
  connectionsPerSecond: 20,
  messagesPerSecond: 100,
  burstCapacity: 50,
  refillRate: 10,
  maxPayloadSize: 4 * 1024 * 1024, // 4MB
  maxEventSize: 1 * 1024 * 1024, // 1MB
  maxConnectionsPerSource: 5,
  maxConnectionsPerIp: 30,
  subnetRateLimitThreshold: 100,
  whitelistedIps: ['10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16', '127.0.0.1'],
  windowMs: 1000,
  backpressureThreshold: 0.8,
  highPriorityHeader: 'X-Priority',
  baseRetryDelayMs: 100,
  maxRetryDelayMs: 30000,
  maxSizeViolations: 3,
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
    | 'load_shedding';

  /** Suggested retry delay in ms */
  retryDelayMs?: number;

  /** Remaining tokens in bucket */
  remainingTokens?: number;

  /** Bucket capacity */
  bucketCapacity?: number;

  /** Buffer utilization (0-1) */
  bufferUtilization?: number;

  /** Suggested delay for backpressure */
  suggestedDelayMs?: number;

  /** Max allowed size (for size violations) */
  maxSize?: number;

  /** Masked client IP (for logging) */
  clientIp?: string;
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
        this.config.burstCapacity * 2, // More lenient for heartbeats
        this.config.refillRate * 2
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
        suggestedDelayMs: 1000,
        retryDelayMs: 1000,
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
            ? Math.floor(bufferUtilization * 1000)
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
          ? Math.floor(bufferUtilization * 500)
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
    const [network, maskBits] = cidr.split('/');
    const mask = parseInt(maskBits, 10);

    // Simple check for private ranges
    if (cidr === '10.0.0.0/8' && ip.startsWith('10.')) return true;
    if (
      cidr === '172.16.0.0/12' &&
      ip.startsWith('172.') &&
      parseInt(ip.split('.')[1]) >= 16 &&
      parseInt(ip.split('.')[1]) <= 31
    )
      return true;
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
    const multiplier = Math.pow(2, Math.min(state.consecutiveRateLimits, 10));
    const delay = Math.min(
      baseDelay * multiplier,
      this.config.maxRetryDelayMs
    );
    // Add jitter
    return Math.floor(delay * (0.5 + Math.random() * 0.5));
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
      100,
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
      this.metrics.activeConnections / (this.config.connectionsPerSecond * 10);

    if (utilizationRatio > 0.9) {
      this.loadLevel = 'critical';
      this.degradedMode = true;
    } else if (utilizationRatio > 0.7) {
      this.loadLevel = 'high';
      this.degradedMode = true;
    } else if (utilizationRatio > 0.5) {
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
   * Get HTTP headers for rate limit response
   */
  getRateLimitHeaders(result: RateLimitResult): Record<string, string> {
    const headers: Record<string, string> = {
      'X-RateLimit-Limit': result.rateLimit.limit.toString(),
      'X-RateLimit-Remaining': result.rateLimit.remaining.toString(),
      'X-RateLimit-Reset': result.rateLimit.resetAt.toString(),
    };

    if (!result.allowed && result.retryDelayMs) {
      headers['Retry-After'] = Math.ceil(result.retryDelayMs / 1000).toString();
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
