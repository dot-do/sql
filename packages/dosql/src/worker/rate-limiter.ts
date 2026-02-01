/**
 * In-memory Token Bucket Rate Limiter for DoSQL
 *
 * Implements a token bucket algorithm for rate limiting requests
 * to DoSQL query endpoints within a single Durable Object instance.
 *
 * - Each DO instance gets its own rate limiter (in-memory, no shared state)
 * - Tokens refill at a steady rate up to the burst capacity
 * - When tokens are exhausted, requests receive 429 Too Many Requests
 */

export interface RateLimiterConfig {
  /** Maximum sustained requests per second */
  requestsPerSecond: number;
  /** Maximum burst size (bucket capacity) */
  burstSize: number;
}

export const DEFAULT_RATE_LIMIT_CONFIG: RateLimiterConfig = {
  requestsPerSecond: 100,
  burstSize: 200,
};

export class RateLimiter {
  private tokens: number;
  private readonly capacity: number;
  private readonly refillRate: number; // tokens per millisecond
  private lastRefillTime: number;

  constructor(config: RateLimiterConfig = DEFAULT_RATE_LIMIT_CONFIG) {
    this.capacity = config.burstSize;
    this.tokens = config.burstSize;
    this.refillRate = config.requestsPerSecond / 1000;
    this.lastRefillTime = Date.now();
  }

  /**
   * Attempt to consume a token. Returns true if the request is allowed,
   * false if the rate limit has been exceeded.
   */
  consume(): boolean {
    this.refill();

    if (this.tokens >= 1) {
      this.tokens -= 1;
      return true;
    }

    return false;
  }

  /**
   * Check the current number of available tokens without consuming one.
   */
  availableTokens(): number {
    this.refill();
    return Math.floor(this.tokens);
  }

  /**
   * Refill tokens based on elapsed time since last refill.
   */
  private refill(): void {
    const now = Date.now();
    const elapsed = now - this.lastRefillTime;

    if (elapsed <= 0) return;

    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.refillRate);
    this.lastRefillTime = now;
  }

  /**
   * Build a 429 Too Many Requests response with appropriate headers.
   */
  static tooManyRequestsResponse(): Response {
    return new Response(
      JSON.stringify({
        success: false,
        error: 'Too Many Requests',
      }),
      {
        status: 429,
        headers: {
          'Content-Type': 'application/json',
          'Retry-After': '1',
        },
      }
    );
  }
}
