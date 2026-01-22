/**
 * Jitter Calculation Tests
 *
 * Tests to verify that the jitter applied to retry delays produces
 * values within the expected range [JITTER_MIN, JITTER_MAX] and
 * follows a roughly uniform distribution.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { RateLimiter } from '../rate-limiter.js';
import { WEBSOCKET, RATE_LIMIT } from '../constants.js';

describe('Jitter Calculation', () => {
  let rateLimiter: RateLimiter;

  beforeEach(() => {
    rateLimiter = new RateLimiter({
      baseRetryDelayMs: 1000,
      maxRetryDelayMs: 30000,
    });
  });

  describe('Jitter constants', () => {
    it('should have JITTER_MIN constant defined', () => {
      expect(WEBSOCKET.JITTER_MIN).toBeDefined();
      expect(typeof WEBSOCKET.JITTER_MIN).toBe('number');
      expect(WEBSOCKET.JITTER_MIN).toBeGreaterThan(0);
      expect(WEBSOCKET.JITTER_MIN).toBeLessThan(1);
    });

    it('should have JITTER_MAX constant defined', () => {
      expect(WEBSOCKET.JITTER_MAX).toBeDefined();
      expect(typeof WEBSOCKET.JITTER_MAX).toBe('number');
      expect(WEBSOCKET.JITTER_MAX).toBeGreaterThan(WEBSOCKET.JITTER_MIN);
      expect(WEBSOCKET.JITTER_MAX).toBeLessThanOrEqual(2);
    });

    it('should have proper jitter range relationship', () => {
      // JITTER_MAX should be greater than JITTER_MIN
      expect(WEBSOCKET.JITTER_MAX).toBeGreaterThan(WEBSOCKET.JITTER_MIN);
      // The range should be reasonable (not more than 2x)
      expect(WEBSOCKET.JITTER_MAX - WEBSOCKET.JITTER_MIN).toBeLessThanOrEqual(1.0);
    });
  });

  describe('Jitter produces values in expected range', () => {
    it('should produce jittered delays within [JITTER_MIN * delay, JITTER_MAX * delay]', () => {
      // Use a rate limiter with known base delay and small burst capacity
      // to make it easier to trigger exactly one rate limit
      const customRateLimiter = new RateLimiter({
        baseRetryDelayMs: 100,
        maxRetryDelayMs: 30000,
        burstCapacity: 5,
        refillRate: 0, // No refill to ensure we hit rate limit
      });

      const connectionId = 'conn-jitter-test';
      const clientId = 'client-jitter-test';

      const baseDelay = 100;
      const jitterMin = WEBSOCKET.JITTER_MIN;
      const jitterMax = WEBSOCKET.JITTER_MAX;

      const minExpected = Math.floor(baseDelay * jitterMin);
      const maxExpected = Math.floor(baseDelay * jitterMax);

      // Sample multiple retry delays
      const samples: number[] = [];
      for (let i = 0; i < 100; i++) {
        // Reset and register fresh connection each iteration
        customRateLimiter.reset();
        customRateLimiter.registerConnection(connectionId, clientId);

        // Exhaust exactly the bucket capacity to trigger rate limit on next call
        for (let j = 0; j < 5; j++) {
          customRateLimiter.checkMessage(connectionId, 'cdc_batch', 100, [], 0);
        }

        // This should be the first rate-limited message (consecutiveRateLimits = 1)
        const result = customRateLimiter.checkMessage(connectionId, 'cdc_batch', 100, [], 0);
        if (!result.allowed && result.retryDelayMs !== undefined) {
          samples.push(result.retryDelayMs);
        }
      }

      expect(samples.length).toBeGreaterThan(0);

      // All values should be within the jitter range for first rate limit (multiplier = 2^1 = 2)
      // So expected range is baseDelay * 2 * [JITTER_MIN, JITTER_MAX]
      const firstRateLimitMultiplier = 2; // 2^1
      const adjustedMinExpected = Math.floor(baseDelay * firstRateLimitMultiplier * jitterMin);
      const adjustedMaxExpected = Math.floor(baseDelay * firstRateLimitMultiplier * jitterMax);

      for (const delay of samples) {
        expect(delay).toBeGreaterThanOrEqual(adjustedMinExpected);
        expect(delay).toBeLessThanOrEqual(adjustedMaxExpected);
      }
    });

    it('should never produce values below JITTER_MIN multiplier', () => {
      const customRateLimiter = new RateLimiter({
        baseRetryDelayMs: 100,
        maxRetryDelayMs: 30000,
        burstCapacity: 5,
        refillRate: 0,
      });

      const connectionId = 'conn-min-test';
      const clientId = 'client-min-test';

      const baseDelay = 100;
      // First rate limit has multiplier 2^1 = 2
      const firstRateLimitMultiplier = 2;
      const minExpected = Math.floor(baseDelay * firstRateLimitMultiplier * WEBSOCKET.JITTER_MIN);

      // Generate many samples
      for (let i = 0; i < 50; i++) {
        customRateLimiter.reset();
        customRateLimiter.registerConnection(connectionId, clientId);

        // Exhaust tokens
        for (let j = 0; j < 5; j++) {
          customRateLimiter.checkMessage(connectionId, 'cdc_batch', 100, [], 0);
        }

        const result = customRateLimiter.checkMessage(connectionId, 'cdc_batch', 100, [], 0);
        if (!result.allowed && result.retryDelayMs !== undefined) {
          expect(result.retryDelayMs).toBeGreaterThanOrEqual(minExpected);
        }
      }
    });

    it('should never produce values above JITTER_MAX multiplier', () => {
      const customRateLimiter = new RateLimiter({
        baseRetryDelayMs: 100,
        maxRetryDelayMs: 30000,
        burstCapacity: 5,
        refillRate: 0,
      });

      const connectionId = 'conn-max-test';
      const clientId = 'client-max-test';

      const baseDelay = 100;
      // First rate limit has multiplier 2^1 = 2
      const firstRateLimitMultiplier = 2;
      const maxExpected = Math.floor(baseDelay * firstRateLimitMultiplier * WEBSOCKET.JITTER_MAX);

      // Generate many samples
      for (let i = 0; i < 50; i++) {
        customRateLimiter.reset();
        customRateLimiter.registerConnection(connectionId, clientId);

        // Exhaust tokens
        for (let j = 0; j < 5; j++) {
          customRateLimiter.checkMessage(connectionId, 'cdc_batch', 100, [], 0);
        }

        const result = customRateLimiter.checkMessage(connectionId, 'cdc_batch', 100, [], 0);
        if (!result.allowed && result.retryDelayMs !== undefined) {
          expect(result.retryDelayMs).toBeLessThanOrEqual(maxExpected);
        }
      }
    });
  });

  describe('Jitter distribution is roughly uniform', () => {
    it('should produce a roughly uniform distribution across the jitter range', () => {
      const customRateLimiter = new RateLimiter({
        baseRetryDelayMs: 1000,
        maxRetryDelayMs: 60000,
        burstCapacity: 5,
        refillRate: 0,
      });

      const connectionId = 'conn-uniform-test';
      const clientId = 'client-uniform-test';

      const baseDelay = 1000;
      const jitterMin = WEBSOCKET.JITTER_MIN;
      const jitterMax = WEBSOCKET.JITTER_MAX;
      const range = jitterMax - jitterMin;

      // First rate limit multiplier is 2^1 = 2
      const firstRateLimitMultiplier = 2;
      const effectiveBaseDelay = baseDelay * firstRateLimitMultiplier;

      // Collect samples
      const samples: number[] = [];
      for (let i = 0; i < 200; i++) {
        customRateLimiter.reset();
        customRateLimiter.registerConnection(connectionId, clientId);

        // Exhaust tokens
        for (let j = 0; j < 5; j++) {
          customRateLimiter.checkMessage(connectionId, 'cdc_batch', 100, [], 0);
        }

        const result = customRateLimiter.checkMessage(connectionId, 'cdc_batch', 100, [], 0);
        if (!result.allowed && result.retryDelayMs !== undefined) {
          // Normalize to [0, 1] within the jitter range
          const normalized = (result.retryDelayMs / effectiveBaseDelay - jitterMin) / range;
          samples.push(normalized);
        }
      }

      expect(samples.length).toBeGreaterThan(100);

      // Divide into buckets and check distribution
      const buckets = [0, 0, 0, 0]; // 4 buckets for [0-0.25], [0.25-0.5], [0.5-0.75], [0.75-1]
      for (const sample of samples) {
        const bucketIndex = Math.min(3, Math.floor(sample * 4));
        if (bucketIndex >= 0 && bucketIndex < 4) {
          buckets[bucketIndex]++;
        }
      }

      // Each bucket should have roughly 25% of samples (with some tolerance)
      // For uniform distribution, we expect each bucket to have ~samples.length/4
      const expectedPerBucket = samples.length / 4;
      const tolerance = expectedPerBucket * 0.6; // Allow 60% deviation for random sampling

      for (let i = 0; i < buckets.length; i++) {
        expect(buckets[i]).toBeGreaterThan(expectedPerBucket - tolerance);
        expect(buckets[i]).toBeLessThan(expectedPerBucket + tolerance);
      }
    });
  });

  describe('Jitter is applied correctly to delay', () => {
    it('should multiply base delay by jitter factor', () => {
      const connectionId = 'conn-multiply-test';
      const clientId = 'client-multiply-test';

      // Use a specific base delay for easier verification
      const customRateLimiter = new RateLimiter({
        baseRetryDelayMs: 1000, // 1 second base
        maxRetryDelayMs: 60000,
        burstCapacity: 5,
        refillRate: 0,
      });

      customRateLimiter.registerConnection(connectionId, clientId);

      const baseDelay = 1000;
      const jitterMin = WEBSOCKET.JITTER_MIN;
      const jitterMax = WEBSOCKET.JITTER_MAX;

      // Exhaust tokens
      for (let j = 0; j < 5; j++) {
        customRateLimiter.checkMessage(connectionId, 'cdc_batch', 100, [], 0);
      }

      const result = customRateLimiter.checkMessage(connectionId, 'cdc_batch', 100, [], 0);

      expect(result.allowed).toBe(false);
      expect(result.retryDelayMs).toBeDefined();

      // First rate limit has multiplier 2^1 = 2
      const firstRateLimitMultiplier = 2;
      const effectiveBaseDelay = baseDelay * firstRateLimitMultiplier;

      // The delay should be effectiveBase * jitter_factor where jitter_factor is in [JITTER_MIN, JITTER_MAX]
      const expectedMin = Math.floor(effectiveBaseDelay * jitterMin);
      const expectedMax = Math.floor(effectiveBaseDelay * jitterMax);

      expect(result.retryDelayMs).toBeGreaterThanOrEqual(expectedMin);
      expect(result.retryDelayMs).toBeLessThanOrEqual(expectedMax);
    });

    it('should apply jitter to exponentially backed-off delays', () => {
      const connectionId = 'conn-backoff-test';
      const clientId = 'client-backoff-test';

      const customRateLimiter = new RateLimiter({
        baseRetryDelayMs: 100,
        maxRetryDelayMs: 30000,
        burstCapacity: 5,
        refillRate: 0,
      });

      customRateLimiter.registerConnection(connectionId, clientId);

      // First exhaust tokens
      for (let j = 0; j < 5; j++) {
        customRateLimiter.checkMessage(connectionId, 'cdc_batch', 100, [], 0);
      }

      const delays: number[] = [];

      // Trigger multiple consecutive rate limits to increase backoff
      for (let iteration = 0; iteration < 5; iteration++) {
        const result = customRateLimiter.checkMessage(connectionId, 'cdc_batch', 100, [], 0);
        if (!result.allowed && result.retryDelayMs !== undefined) {
          delays.push(result.retryDelayMs);
        }
      }

      // Each delay should still be within the proper jitter bounds for its backoff level
      const jitterMin = WEBSOCKET.JITTER_MIN;
      const jitterMax = WEBSOCKET.JITTER_MAX;

      for (let i = 0; i < delays.length; i++) {
        // consecutiveRateLimits starts at 0, increments before calculateRetryDelay
        // So first rate limit has consecutiveRateLimits=1, multiplier = 2^1 = 2
        // Second has consecutiveRateLimits=2, multiplier = 2^2 = 4, etc.
        const multiplier = Math.pow(2, i + 1);
        const expectedBaseDelay = Math.min(100 * multiplier, 30000);
        const expectedMin = Math.floor(expectedBaseDelay * jitterMin);
        const expectedMax = Math.floor(expectedBaseDelay * jitterMax);

        expect(delays[i]).toBeGreaterThanOrEqual(expectedMin);
        expect(delays[i]).toBeLessThanOrEqual(expectedMax);
      }
    });

    it('should cap delay at maxRetryDelayMs even with jitter', () => {
      const connectionId = 'conn-cap-test';
      const clientId = 'client-cap-test';

      const maxRetryDelay = 1000;
      const customRateLimiter = new RateLimiter({
        baseRetryDelayMs: 500,
        maxRetryDelayMs: maxRetryDelay,
        burstCapacity: 5,
        refillRate: 0,
      });

      customRateLimiter.registerConnection(connectionId, clientId);

      // Exhaust tokens
      for (let j = 0; j < 5; j++) {
        customRateLimiter.checkMessage(connectionId, 'cdc_batch', 100, [], 0);
      }

      // Trigger many consecutive rate limits to max out exponential backoff
      for (let i = 0; i < 15; i++) {
        const result = customRateLimiter.checkMessage(connectionId, 'cdc_batch', 100, [], 0);
        if (!result.allowed && result.retryDelayMs !== undefined) {
          // Even with jitter, should not exceed maxRetryDelay * JITTER_MAX
          const absoluteMax = Math.floor(maxRetryDelay * WEBSOCKET.JITTER_MAX);
          expect(result.retryDelayMs).toBeLessThanOrEqual(absoluteMax);
        }
      }
    });
  });
});
