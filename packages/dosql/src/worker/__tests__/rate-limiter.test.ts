/**
 * Rate Limiter Tests
 *
 * Tests for the token bucket rate limiter used in DoSQL query endpoints.
 * These tests verify the algorithm logic directly without needing DO storage.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { RateLimiter, DEFAULT_RATE_LIMIT_CONFIG } from '../rate-limiter.js';

describe('RateLimiter', () => {
  describe('constructor', () => {
    it('should use default config when none provided', () => {
      const limiter = new RateLimiter();
      // Should have full burst capacity available
      expect(limiter.availableTokens()).toBe(DEFAULT_RATE_LIMIT_CONFIG.burstSize);
    });

    it('should accept custom config', () => {
      const limiter = new RateLimiter({ requestsPerSecond: 10, burstSize: 20 });
      expect(limiter.availableTokens()).toBe(20);
    });
  });

  describe('consume', () => {
    it('should allow requests within burst capacity', () => {
      const limiter = new RateLimiter({ requestsPerSecond: 10, burstSize: 5 });

      for (let i = 0; i < 5; i++) {
        expect(limiter.consume()).toBe(true);
      }
    });

    it('should reject requests when tokens are exhausted', () => {
      const limiter = new RateLimiter({ requestsPerSecond: 10, burstSize: 3 });

      // Consume all tokens
      expect(limiter.consume()).toBe(true);
      expect(limiter.consume()).toBe(true);
      expect(limiter.consume()).toBe(true);

      // Next request should be rejected
      expect(limiter.consume()).toBe(false);
    });

    it('should decrement available tokens on each consume', () => {
      const limiter = new RateLimiter({ requestsPerSecond: 10, burstSize: 5 });

      expect(limiter.availableTokens()).toBe(5);
      limiter.consume();
      expect(limiter.availableTokens()).toBe(4);
      limiter.consume();
      expect(limiter.availableTokens()).toBe(3);
    });
  });

  describe('token refill', () => {
    it('should refill tokens over time', async () => {
      const limiter = new RateLimiter({ requestsPerSecond: 1000, burstSize: 5 });

      // Consume all tokens
      for (let i = 0; i < 5; i++) {
        limiter.consume();
      }
      expect(limiter.availableTokens()).toBe(0);

      // Wait for refill (1000 req/s = 1 token per ms, wait 10ms for ~10 tokens)
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Should have some tokens back (at least 1, capped at 5)
      const tokens = limiter.availableTokens();
      expect(tokens).toBeGreaterThan(0);
      expect(tokens).toBeLessThanOrEqual(5);
    });

    it('should not exceed burst capacity when refilling', async () => {
      const limiter = new RateLimiter({ requestsPerSecond: 1000, burstSize: 5 });

      // Wait a long time - tokens should cap at burst size
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(limiter.availableTokens()).toBe(5);
    });
  });

  describe('tooManyRequestsResponse', () => {
    it('should return a 429 response', async () => {
      const response = RateLimiter.tooManyRequestsResponse();

      expect(response.status).toBe(429);
      expect(response.headers.get('Content-Type')).toBe('application/json');
      expect(response.headers.get('Retry-After')).toBe('1');

      const body = await response.json() as { success: boolean; error: string };
      expect(body.success).toBe(false);
      expect(body.error).toBe('Too Many Requests');
    });
  });

  describe('default config', () => {
    it('should have reasonable defaults', () => {
      expect(DEFAULT_RATE_LIMIT_CONFIG.requestsPerSecond).toBe(100);
      expect(DEFAULT_RATE_LIMIT_CONFIG.burstSize).toBe(200);
    });
  });

  describe('high volume', () => {
    it('should handle the default burst of 200 requests', () => {
      const limiter = new RateLimiter();
      let allowed = 0;

      for (let i = 0; i < 300; i++) {
        if (limiter.consume()) {
          allowed++;
        }
      }

      // Should allow exactly burstSize requests (200) when consumed instantly
      expect(allowed).toBe(200);
    });
  });
});
