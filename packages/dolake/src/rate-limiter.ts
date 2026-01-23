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

  /**
   * Generic helper to get or create state in a map.
   * Creates a new IpState with default values if the key doesn't exist.
   */
  private getOrCreateState(map: Map<string, IpState>, key: string): IpState {
    let state = map.get(key);
    if (!state) {
      state = {
        connectionCount: 0,
        messageCount: 0,
        windowStart: Date.now(),
      };
      map.set(key, state);
    }
    return state;
  }

  private getOrCreateIpState(ip: string): IpState {
    return this.getOrCreateState(this.ipStates, ip);
  }

  private getOrCreateSubnetState(subnet: string): IpState {
    return this.getOrCreateState(this.subnetStates, subnet);
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

  /**
   * Check if an IP address is within a CIDR range using proper bitwise matching.
   *
   * CIDR (Classless Inter-Domain Routing) notation specifies a network prefix.
   * For example, '192.168.0.0/16' means:
   *   - Network address: 192.168.0.0
   *   - Prefix length: 16 bits (the first 16 bits identify the network)
   *   - Host bits: 32 - 16 = 16 bits (can vary within the network)
   *   - Range: 192.168.0.0 to 192.168.255.255 (65,536 addresses)
   *
   * The algorithm:
   * 1. Convert both IP addresses to 32-bit integers
   * 2. Create a bitmask with `prefix` leading 1s and `(32-prefix)` trailing 0s
   * 3. Apply the mask to both IPs using bitwise AND
   * 4. If the masked values are equal, the IP is in the CIDR range
   *
   * @param ip - The IP address to check (e.g., '192.168.1.100')
   * @param cidr - The CIDR notation (e.g., '192.168.0.0/16')
   * @returns true if the IP is within the CIDR range
   */
  private isInCidr(ip: string, cidr: string): boolean {
    // Split CIDR into network address and prefix length
    // Example: '192.168.0.0/16' -> network='192.168.0.0', maskBits='16'
    const parts = cidr.split('/');
    const network = parts[0];
    const maskBits = parts[1];
    if (!network || !maskBits) return false;

    const mask = parseInt(maskBits, 10);

    // Validate mask is within valid range (0-32 for IPv4)
    // /0 = all IPs, /32 = single IP (exact match)
    if (Number.isNaN(mask) || mask < 0 || mask > 32) return false;

    // Parse the network address to a 32-bit integer
    // Example: '192.168.0.0' -> 0xC0A80000 (3232235520)
    const networkInt = this.ipToInt(network);
    if (networkInt === null) return false;

    // Parse the IP address to check
    // Example: '192.168.1.100' -> 0xC0A80164 (3232235876)
    const ipInt = this.ipToInt(ip);
    if (ipInt === null) return false;

    // Special case: /0 matches all IPs (no bits to compare)
    if (mask === 0) return true;

    // Create the subnet mask by left-shifting 1s and filling with 0s on the right
    //
    // How it works:
    //   0xFFFFFFFF = 11111111 11111111 11111111 11111111 (all 32 bits set)
    //   (32 - mask) = number of host bits (bits to ignore)
    //   << (32 - mask) = shift left, pushing 0s into the low bits
    //
    // Examples:
    //   /24: 0xFFFFFFFF << 8  = 0xFFFFFF00 = 11111111.11111111.11111111.00000000
    //   /16: 0xFFFFFFFF << 16 = 0xFFFF0000 = 11111111.11111111.00000000.00000000
    //   /8:  0xFFFFFFFF << 24 = 0xFF000000 = 11111111.00000000.00000000.00000000
    //
    // The >>> 0 converts to unsigned 32-bit integer (JavaScript quirk:
    // bitwise ops return signed 32-bit ints, >>> 0 makes them unsigned)
    const subnetMask = mask === 32 ? 0xFFFFFFFF : ((0xFFFFFFFF << (32 - mask)) >>> 0);

    // Apply the mask to both addresses using bitwise AND, then compare
    //
    // The AND operation zeroes out the host bits, leaving only the network prefix.
    // If the network prefixes match, the IP is in the CIDR range.
    //
    // Example with 192.168.1.100 and 192.168.0.0/16:
    //   ipInt      = 0xC0A80164 = 11000000.10101000.00000001.01100100
    //   networkInt = 0xC0A80000 = 11000000.10101000.00000000.00000000
    //   subnetMask = 0xFFFF0000 = 11111111.11111111.00000000.00000000
    //
    //   ipInt & subnetMask      = 0xC0A80000 (network bits preserved, host bits zeroed)
    //   networkInt & subnetMask = 0xC0A80000
    //   Result: MATCH -> IP is in range
    //
    // >>> 0 ensures unsigned comparison (avoids signed int issues with high bit set)
    return ((ipInt & subnetMask) >>> 0) === ((networkInt & subnetMask) >>> 0);
  }

  /**
   * Convert an IPv4 address string to a 32-bit unsigned integer.
   *
   * IPv4 addresses are 4 octets (bytes), each 0-255. This function packs
   * them into a single 32-bit integer for efficient bitwise comparisons.
   *
   * Example: '192.168.1.100'
   *   Octet 1: 192 = 0xC0 -> shifted to bits 24-31
   *   Octet 2: 168 = 0xA8 -> shifted to bits 16-23
   *   Octet 3:   1 = 0x01 -> shifted to bits 8-15
   *   Octet 4: 100 = 0x64 -> bits 0-7
   *   Result: 0xC0A80164 = 3232235876
   *
   * @param ip - The IP address string (e.g., '192.168.1.100')
   * @returns The integer representation, or null if invalid
   */
  private ipToInt(ip: string): number | null {
    const octets = ip.split('.');
    if (octets.length !== 4) return null;

    let result = 0;
    for (const octet of octets) {
      const value = parseInt(octet, 10);
      // Validate octet is a valid number in range 0-255
      if (Number.isNaN(value) || value < 0 || value > 255) return null;

      // Shift existing bits left by 8 to make room for the new octet,
      // then OR in the new octet value.
      //
      // Iteration 1 (192): result = (0 << 8) | 192     = 192         = 0x000000C0
      // Iteration 2 (168): result = (192 << 8) | 168   = 49320       = 0x0000C0A8
      // Iteration 3 (1):   result = (49320 << 8) | 1   = 12625921    = 0x00C0A801
      // Iteration 4 (100): result = (12625921 << 8) | 100 = 3232235876 = 0xC0A80164
      //
      // >>> 0 converts to unsigned 32-bit integer (necessary because JavaScript
      // bitwise operations return signed 32-bit integers, and IPs like 255.x.x.x
      // would otherwise be interpreted as negative numbers)
      result = ((result << 8) | value) >>> 0;
    }

    return result;
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

  /**
   * Calculate the retry delay for a rate-limited connection using exponential backoff with jitter.
   *
   * Algorithm: Exponential Backoff with Decorrelated Jitter
   *
   * The delay is computed as:
   *   delay = min(baseDelay * 2^n, maxRetryDelay) * jitter
   *
   * Where:
   *   - baseDelay: The base retry delay from config (e.g., 100ms)
   *   - n: Number of consecutive rate limits, capped at MAX_EXPONENTIAL_BACKOFF_POWER (10)
   *        to prevent overflow (2^10 = 1024 max multiplier)
   *   - maxRetryDelay: Upper bound on delay before jitter (e.g., 30000ms)
   *   - jitter: Random multiplier in [JITTER_MIN, JITTER_MAX] (e.g., [0.5, 1.5])
   *
   * The jitter prevents "thundering herd" problems where many clients retry simultaneously
   * after being rate limited at the same time. By randomizing the retry delay, clients
   * spread their retries across a wider time window.
   *
   * Example progression (with baseDelay=100ms, maxRetryDelay=30000ms):
   *   - 1st rate limit: 100 * 2^1 = 200ms * jitter -> ~100-300ms
   *   - 2nd rate limit: 100 * 2^2 = 400ms * jitter -> ~200-600ms
   *   - 3rd rate limit: 100 * 2^3 = 800ms * jitter -> ~400-1200ms
   *   - ...
   *   - 10th+ rate limit: capped at 30000ms * jitter -> ~15000-45000ms
   *
   * @param state - The connection state containing consecutiveRateLimits counter
   * @returns The calculated retry delay in milliseconds
   */
  private calculateRetryDelay(state: ConnectionState): number {
    // Exponential backoff
    const baseDelay = this.config.baseRetryDelayMs;
    const multiplier = Math.pow(2, Math.min(state.consecutiveRateLimits, RATE_LIMIT.MAX_EXPONENTIAL_BACKOFF_POWER));
    const delay = Math.min(
      baseDelay * multiplier,
      this.config.maxRetryDelayMs
    );
    // Add jitter: random value in [JITTER_MIN, JITTER_MAX] range
    const jitterRange = WEBSOCKET.JITTER_MAX - WEBSOCKET.JITTER_MIN;
    return Math.floor(delay * (WEBSOCKET.JITTER_MIN + Math.random() * jitterRange));
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

  /**
   * Update the system load level based on current connection utilization.
   *
   * Algorithm: Threshold-Based Load Classification
   *
   * The load level is determined by comparing the utilization ratio against
   * predefined thresholds. The utilization ratio is calculated as:
   *
   *   utilizationRatio = activeConnections / (connectionsPerSecond * LOAD_CONNECTION_MULTIPLIER)
   *
   * This normalizes the active connection count against the expected capacity,
   * where LOAD_CONNECTION_MULTIPLIER scales the connectionsPerSecond config
   * to represent total expected connection capacity.
   *
   * Load Level Classification:
   *   - 'critical' (ratio > 0.9): System is at capacity. Enables degraded mode
   *      which triggers load shedding for non-priority traffic.
   *   - 'high' (ratio > 0.7): System under heavy load. Enables degraded mode
   *      as a precautionary measure.
   *   - 'elevated' (ratio > 0.5): System moderately loaded. No degraded mode,
   *      but monitoring is heightened.
   *   - 'normal' (ratio <= 0.5): System operating normally with ample capacity.
   *
   * The degraded mode flag affects rate limiting behavior:
   *   - When true: Load shedding is enabled for 'cdc_batch' messages from
   *     non-high-priority connections (see checkMessage method)
   *   - When false: All messages are processed normally
   *
   * This method is called whenever connections are registered or unregistered
   * to keep the load level current.
   */
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
