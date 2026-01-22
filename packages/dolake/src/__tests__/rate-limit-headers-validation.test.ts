/**
 * Rate Limit Headers Validation Tests (TDD RED Phase)
 *
 * Issue: sql-zhy.13 - Tests for rate limit header validation issues
 *
 * VULNERABILITY: getRateLimitHeaders() trusts RateLimitResult without validation.
 *
 * In rate-limiter.ts getRateLimitHeaders():
 * ```typescript
 * getRateLimitHeaders(result: RateLimitResult): Record<string, string> {
 *   const headers: Record<string, string> = {
 *     'X-RateLimit-Limit': result.rateLimit.limit.toString(),
 *     'X-RateLimit-Remaining': result.rateLimit.remaining.toString(),
 *     'X-RateLimit-Reset': result.rateLimit.resetAt.toString(),
 *   };
 *
 *   if (!result.allowed && result.retryDelayMs) {
 *     headers['Retry-After'] = Math.ceil(result.retryDelayMs / 1000).toString();
 *   }
 *
 *   return headers;
 * }
 * ```
 *
 * Problems:
 * 1. No validation of negative remaining counts
 * 2. No validation of negative limits
 * 3. No validation that reset times are in the future
 * 4. No validation for non-integer values (NaN, Infinity)
 * 5. No bounds checking for rate limit values
 * 6. No validation of retryDelayMs before header generation
 *
 * The correct implementation should:
 * 1. Validate all numeric values are non-negative integers
 * 2. Ensure remaining <= limit
 * 3. Ensure resetAt is in the future (or at least current timestamp)
 * 4. Reject or sanitize NaN/Infinity values
 * 5. Validate retryDelayMs is positive when present
 * 6. Clamp values to reasonable bounds
 *
 * These tests use `it.fails()` pattern to document expected behavior that is currently MISSING.
 * When using it.fails():
 * - Test PASSES in vitest = the inner assertions FAILED = behavior is MISSING (RED phase)
 * - Test FAILS in vitest = the inner assertions PASSED = behavior already exists
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  RateLimiter,
  DEFAULT_RATE_LIMIT_CONFIG,
  type RateLimitResult,
  type RateLimitInfo,
} from '../rate-limiter.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a RateLimiter instance with default config
 */
function createRateLimiter(): RateLimiter {
  return new RateLimiter(DEFAULT_RATE_LIMIT_CONFIG);
}

/**
 * Create a valid RateLimitResult for testing
 */
function createValidRateLimitResult(overrides: Partial<RateLimitResult> = {}): RateLimitResult {
  const now = Math.floor(Date.now() / 1000);
  return {
    allowed: true,
    rateLimit: {
      limit: 100,
      remaining: 50,
      resetAt: now + 60, // 60 seconds in the future
    },
    ...overrides,
  };
}

/**
 * Create a RateLimitResult with invalid RateLimitInfo
 */
function createRateLimitResultWithInfo(info: Partial<RateLimitInfo>): RateLimitResult {
  const now = Math.floor(Date.now() / 1000);
  return {
    allowed: true,
    rateLimit: {
      limit: 100,
      remaining: 50,
      resetAt: now + 60,
      ...info,
    },
  };
}

// =============================================================================
// 1. Negative Value Validation Tests
// =============================================================================

describe('Rate Limit Headers Validation', () => {
  let rateLimiter: RateLimiter;

  beforeEach(() => {
    rateLimiter = createRateLimiter();
  });

  describe('Negative Value Validation', () => {
    /**
     * CRITICAL: Negative remaining counts should never appear in HTTP headers.
     * A negative X-RateLimit-Remaining header is semantically invalid and could
     * confuse clients about their rate limit status.
     */

    it.fails('should reject negative remaining count', () => {
      const result = createRateLimitResultWithInfo({ remaining: -5 });

      // EXPECTED: Should throw or sanitize negative remaining values
      // CURRENT BEHAVIOR: Directly converts to string, producing "-5" header
      expect(() => {
        const headers = rateLimiter.getRateLimitHeaders(result);
        // If we get here, check that the header is not negative
        expect(parseInt(headers['X-RateLimit-Remaining'], 10)).toBeGreaterThanOrEqual(0);
      }).not.toThrow();

      // Alternative: validation should throw
      // expect(() => rateLimiter.getRateLimitHeaders(result)).toThrow();
    });

    it.fails('should reject negative limit', () => {
      const result = createRateLimitResultWithInfo({ limit: -100 });

      // EXPECTED: Should throw or sanitize negative limit values
      // CURRENT BEHAVIOR: Directly converts to string, producing "-100" header
      const headers = rateLimiter.getRateLimitHeaders(result);
      expect(parseInt(headers['X-RateLimit-Limit'], 10)).toBeGreaterThan(0);
    });

    it.fails('should reject negative resetAt timestamp', () => {
      const result = createRateLimitResultWithInfo({ resetAt: -1000 });

      // EXPECTED: Should throw or sanitize negative reset times
      // CURRENT BEHAVIOR: Directly converts to string, producing "-1000" header
      const headers = rateLimiter.getRateLimitHeaders(result);
      expect(parseInt(headers['X-RateLimit-Reset'], 10)).toBeGreaterThan(0);
    });

    it.fails('should reject negative retryDelayMs', () => {
      const result: RateLimitResult = {
        ...createValidRateLimitResult(),
        allowed: false,
        retryDelayMs: -5000,
        reason: 'rate_limited',
      };

      // EXPECTED: Should throw or sanitize negative retry delay
      // CURRENT BEHAVIOR: Math.ceil(-5000/1000) = -5, producing "Retry-After: -5"
      const headers = rateLimiter.getRateLimitHeaders(result);
      if (headers['Retry-After']) {
        expect(parseInt(headers['Retry-After'], 10)).toBeGreaterThan(0);
      }
    });
  });

  // =============================================================================
  // 2. Non-Integer Value Validation Tests
  // =============================================================================

  describe('Non-Integer Value Validation', () => {
    /**
     * Rate limit header values should be valid integers.
     * NaN, Infinity, and floating point values are semantically invalid.
     */

    it.fails('should reject NaN remaining count', () => {
      const result = createRateLimitResultWithInfo({ remaining: NaN });

      // EXPECTED: Should throw or sanitize NaN values
      // CURRENT BEHAVIOR: NaN.toString() produces "NaN" header string
      const headers = rateLimiter.getRateLimitHeaders(result);
      expect(headers['X-RateLimit-Remaining']).not.toBe('NaN');
      expect(Number.isFinite(parseInt(headers['X-RateLimit-Remaining'], 10))).toBe(true);
    });

    it.fails('should reject NaN limit', () => {
      const result = createRateLimitResultWithInfo({ limit: NaN });

      // EXPECTED: Should throw or sanitize NaN values
      // CURRENT BEHAVIOR: NaN.toString() produces "NaN" header string
      const headers = rateLimiter.getRateLimitHeaders(result);
      expect(headers['X-RateLimit-Limit']).not.toBe('NaN');
      expect(Number.isFinite(parseInt(headers['X-RateLimit-Limit'], 10))).toBe(true);
    });

    it.fails('should reject NaN resetAt', () => {
      const result = createRateLimitResultWithInfo({ resetAt: NaN });

      // EXPECTED: Should throw or sanitize NaN values
      // CURRENT BEHAVIOR: NaN.toString() produces "NaN" header string
      const headers = rateLimiter.getRateLimitHeaders(result);
      expect(headers['X-RateLimit-Reset']).not.toBe('NaN');
      expect(Number.isFinite(parseInt(headers['X-RateLimit-Reset'], 10))).toBe(true);
    });

    it.fails('should reject Infinity remaining count', () => {
      const result = createRateLimitResultWithInfo({ remaining: Infinity });

      // EXPECTED: Should throw or sanitize Infinity values
      // CURRENT BEHAVIOR: Infinity.toString() produces "Infinity" header string
      const headers = rateLimiter.getRateLimitHeaders(result);
      expect(headers['X-RateLimit-Remaining']).not.toBe('Infinity');
      expect(Number.isFinite(parseInt(headers['X-RateLimit-Remaining'], 10))).toBe(true);
    });

    it.fails('should reject negative Infinity values', () => {
      const result = createRateLimitResultWithInfo({ remaining: -Infinity });

      // EXPECTED: Should throw or sanitize -Infinity values
      // CURRENT BEHAVIOR: (-Infinity).toString() produces "-Infinity" header string
      const headers = rateLimiter.getRateLimitHeaders(result);
      expect(headers['X-RateLimit-Remaining']).not.toBe('-Infinity');
      expect(Number.isFinite(parseInt(headers['X-RateLimit-Remaining'], 10))).toBe(true);
    });

    it.fails('should round or reject floating point remaining values', () => {
      const result = createRateLimitResultWithInfo({ remaining: 50.7 });

      // EXPECTED: Should round to integer or reject
      // CURRENT BEHAVIOR: (50.7).toString() produces "50.7" header string
      const headers = rateLimiter.getRateLimitHeaders(result);
      const remaining = headers['X-RateLimit-Remaining'];
      expect(remaining).not.toContain('.');
      expect(parseInt(remaining, 10)).toBe(parseFloat(remaining));
    });

    it.fails('should round or reject floating point limit values', () => {
      const result = createRateLimitResultWithInfo({ limit: 100.5 });

      // EXPECTED: Should round to integer or reject
      // CURRENT BEHAVIOR: (100.5).toString() produces "100.5" header string
      const headers = rateLimiter.getRateLimitHeaders(result);
      const limit = headers['X-RateLimit-Limit'];
      expect(limit).not.toContain('.');
      expect(parseInt(limit, 10)).toBe(parseFloat(limit));
    });
  });

  // =============================================================================
  // 3. Reset Time Validation Tests
  // =============================================================================

  describe('Reset Time Validation', () => {
    /**
     * The X-RateLimit-Reset header should be a timestamp in the future.
     * A reset time in the past is semantically invalid and confusing to clients.
     */

    it.fails('should reject reset time in the past', () => {
      const now = Math.floor(Date.now() / 1000);
      const pastTime = now - 3600; // 1 hour ago
      const result = createRateLimitResultWithInfo({ resetAt: pastTime });

      // EXPECTED: Should throw or use current time as minimum
      // CURRENT BEHAVIOR: Directly converts to string without time validation
      const headers = rateLimiter.getRateLimitHeaders(result);
      const resetAt = parseInt(headers['X-RateLimit-Reset'], 10);
      expect(resetAt).toBeGreaterThanOrEqual(now);
    });

    it.fails('should reject reset time of zero', () => {
      const result = createRateLimitResultWithInfo({ resetAt: 0 });

      // EXPECTED: Should throw or use a valid future timestamp
      // CURRENT BEHAVIOR: (0).toString() produces "0" header string (Unix epoch)
      const headers = rateLimiter.getRateLimitHeaders(result);
      const resetAt = parseInt(headers['X-RateLimit-Reset'], 10);
      const now = Math.floor(Date.now() / 1000);
      expect(resetAt).toBeGreaterThan(0);
      expect(resetAt).toBeGreaterThanOrEqual(now);
    });

    it.fails('should reject unreasonably far future reset times', () => {
      // 100 years in the future
      const farFuture = Math.floor(Date.now() / 1000) + 100 * 365 * 24 * 60 * 60;
      const result = createRateLimitResultWithInfo({ resetAt: farFuture });

      // EXPECTED: Should validate reasonable bounds (e.g., max 24 hours)
      // CURRENT BEHAVIOR: No upper bound validation
      const headers = rateLimiter.getRateLimitHeaders(result);
      const resetAt = parseInt(headers['X-RateLimit-Reset'], 10);
      const now = Math.floor(Date.now() / 1000);
      const maxReset = now + 24 * 60 * 60; // Max 24 hours from now

      expect(resetAt).toBeLessThanOrEqual(maxReset);
    });
  });

  // =============================================================================
  // 4. Bounds Validation Tests
  // =============================================================================

  describe('Bounds Validation', () => {
    /**
     * Rate limit values should have reasonable bounds to prevent
     * overflow issues and ensure semantic correctness.
     */

    it.fails('should reject remaining greater than limit', () => {
      const result = createRateLimitResultWithInfo({
        limit: 100,
        remaining: 150, // More remaining than the limit allows
      });

      // EXPECTED: remaining should never exceed limit
      // CURRENT BEHAVIOR: No relationship validation between remaining and limit
      const headers = rateLimiter.getRateLimitHeaders(result);
      const limit = parseInt(headers['X-RateLimit-Limit'], 10);
      const remaining = parseInt(headers['X-RateLimit-Remaining'], 10);

      expect(remaining).toBeLessThanOrEqual(limit);
    });

    it.fails('should reject extremely large limit values', () => {
      const result = createRateLimitResultWithInfo({
        limit: Number.MAX_SAFE_INTEGER,
      });

      // EXPECTED: Should have reasonable upper bound (e.g., 1 million)
      // CURRENT BEHAVIOR: No upper bound validation
      const headers = rateLimiter.getRateLimitHeaders(result);
      const limit = parseInt(headers['X-RateLimit-Limit'], 10);
      const maxReasonableLimit = 1_000_000;

      expect(limit).toBeLessThanOrEqual(maxReasonableLimit);
    });

    it.fails('should reject limit of zero', () => {
      const result = createRateLimitResultWithInfo({ limit: 0 });

      // EXPECTED: A limit of 0 is semantically invalid (would always block)
      // CURRENT BEHAVIOR: No zero validation
      const headers = rateLimiter.getRateLimitHeaders(result);
      const limit = parseInt(headers['X-RateLimit-Limit'], 10);

      expect(limit).toBeGreaterThan(0);
    });
  });

  // =============================================================================
  // 5. Retry-After Header Validation Tests
  // =============================================================================

  describe('Retry-After Header Validation', () => {
    /**
     * The Retry-After header should contain valid seconds values.
     */

    it('documents accidental handling of zero retryDelayMs via falsy check (no explicit validation)', () => {
      const result: RateLimitResult = {
        ...createValidRateLimitResult(),
        allowed: false,
        retryDelayMs: 0,
        reason: 'rate_limited',
      };

      // CURRENT BEHAVIOR: The conditional check (!result.allowed && result.retryDelayMs)
      // is falsy for 0, so header is accidentally not included.
      // This is ACCIDENTAL good behavior, not explicit validation.
      // The code should have explicit validation: if (retryDelayMs <= 0) throw/warn
      const headers = rateLimiter.getRateLimitHeaders(result);

      // Documents that 0 is accidentally filtered out by falsy check
      expect(headers['Retry-After']).toBeUndefined();
      // No explicit validation error is thrown
      expect(() => rateLimiter.getRateLimitHeaders(result)).not.toThrow();
    });

    it('documents accidental handling of NaN retryDelayMs via falsy check (no explicit validation)', () => {
      const result: RateLimitResult = {
        ...createValidRateLimitResult(),
        allowed: false,
        retryDelayMs: NaN,
        reason: 'rate_limited',
      };

      // CURRENT BEHAVIOR: NaN is falsy, so header is accidentally not included
      // But there's no explicit validation - this relies on JavaScript truthiness
      // Proper implementation would explicitly check: if (Number.isNaN(retryDelayMs)) throw
      const headers = rateLimiter.getRateLimitHeaders(result);

      // Documents that NaN is accidentally filtered out by falsy check
      expect(headers['Retry-After']).toBeUndefined();
    });

    it.fails('should provide explicit validation error for invalid retryDelayMs values', () => {
      const result: RateLimitResult = {
        ...createValidRateLimitResult(),
        allowed: false,
        retryDelayMs: -100, // Explicitly invalid
        reason: 'rate_limited',
      };

      // EXPECTED: Should throw with clear validation error for negative delay
      // CURRENT BEHAVIOR: -100 is truthy, so Math.ceil(-100/1000) = 0 is used
      // No validation, just produces "Retry-After: 0"
      expect(() => {
        rateLimiter.getRateLimitHeaders(result);
      }).toThrow(/retryDelay.*positive|invalid.*delay/i);
    });

    it.fails('should reject Infinity retryDelayMs', () => {
      const result: RateLimitResult = {
        ...createValidRateLimitResult(),
        allowed: false,
        retryDelayMs: Infinity,
        reason: 'rate_limited',
      };

      // EXPECTED: Should not produce "Retry-After: Infinity"
      // CURRENT BEHAVIOR: Math.ceil(Infinity/1000) = Infinity
      const headers = rateLimiter.getRateLimitHeaders(result);
      if (headers['Retry-After']) {
        expect(headers['Retry-After']).not.toBe('Infinity');
        expect(Number.isFinite(parseInt(headers['Retry-After'], 10))).toBe(true);
      }
    });

    it.fails('should cap extremely large retryDelayMs', () => {
      const result: RateLimitResult = {
        ...createValidRateLimitResult(),
        allowed: false,
        retryDelayMs: 1_000_000_000, // ~11.5 days in ms
        reason: 'rate_limited',
      };

      // EXPECTED: Should cap to reasonable maximum (e.g., maxRetryDelayMs from config)
      // CURRENT BEHAVIOR: No upper bound validation
      const headers = rateLimiter.getRateLimitHeaders(result);
      if (headers['Retry-After']) {
        const retryAfter = parseInt(headers['Retry-After'], 10);
        const maxSeconds = DEFAULT_RATE_LIMIT_CONFIG.maxRetryDelayMs / 1000;
        expect(retryAfter).toBeLessThanOrEqual(maxSeconds);
      }
    });
  });

  // =============================================================================
  // 6. Type Coercion Edge Cases
  // =============================================================================

  describe('Type Coercion Edge Cases', () => {
    /**
     * Even though TypeScript specifies numbers, runtime values could be
     * strings or other types due to JSON deserialization or bugs.
     */

    it.fails('should explicitly validate type of remaining value', () => {
      // This simulates what could happen with JSON deserialization bugs
      const result = createRateLimitResultWithInfo({
        remaining: '50' as unknown as number,
      });

      // EXPECTED: Should have explicit type validation that throws/warns for non-numbers
      // CURRENT BEHAVIOR: ('50').toString() works accidentally but no type checking
      // The code should use typeof validation before calling .toString()
      expect(() => {
        rateLimiter.getRateLimitHeaders(result);
      }).toThrow(/type.*number/i); // Should throw type validation error
    });

    it('should crash on undefined values due to lack of validation (documents current fragile behavior)', () => {
      const result = createRateLimitResultWithInfo({
        remaining: undefined as unknown as number,
      });

      // CURRENT BEHAVIOR: (undefined).toString() throws TypeError
      // This documents that the code crashes rather than handling gracefully
      // Proper implementation would validate upfront with clear error message
      expect(() => {
        rateLimiter.getRateLimitHeaders(result);
      }).toThrow(TypeError);
    });

    it.fails('should reject null values in rateLimit with explicit validation', () => {
      const result = createRateLimitResultWithInfo({
        limit: null as unknown as number,
      });

      // EXPECTED: Should throw with clear validation error
      // CURRENT BEHAVIOR: (null).toString() returns "null" string - no validation
      const headers = rateLimiter.getRateLimitHeaders(result);
      expect(headers['X-RateLimit-Limit']).not.toBe('null');
    });

    it.fails('should validate object values are not passed as numbers', () => {
      const result = createRateLimitResultWithInfo({
        remaining: { value: 50 } as unknown as number,
      });

      // EXPECTED: Should throw with clear validation error for object values
      // CURRENT BEHAVIOR: ({value: 50}).toString() returns "[object Object]"
      expect(() => {
        rateLimiter.getRateLimitHeaders(result);
      }).toThrow(/type.*number/i);
    });
  });

  // =============================================================================
  // 7. Header Format Validation Tests
  // =============================================================================

  describe('Header Format Validation', () => {
    /**
     * HTTP headers have specific format requirements that should be enforced.
     */

    it.fails('should explicitly validate and round floating point values', () => {
      const result = createRateLimitResultWithInfo({
        limit: 100.7,
        remaining: 50.3,
      });

      // EXPECTED: Should explicitly validate and round/floor floating point values
      // CURRENT BEHAVIOR: (100.7).toString() produces "100.7" - no rounding
      const headers = rateLimiter.getRateLimitHeaders(result);

      // Header values should be integers per HTTP conventions
      expect(headers['X-RateLimit-Limit']).not.toContain('.');
      expect(headers['X-RateLimit-Remaining']).not.toContain('.');
    });

    it.fails('should reject or clamp extremely large values that produce scientific notation', () => {
      const result = createRateLimitResultWithInfo({
        limit: 1e21, // Large enough to trigger scientific notation
      });

      // EXPECTED: Should either reject or format as regular integer within bounds
      // CURRENT BEHAVIOR: (1e21).toString() produces "1e+21"
      const headers = rateLimiter.getRateLimitHeaders(result);

      // Scientific notation is invalid for HTTP headers
      expect(headers['X-RateLimit-Limit']).not.toMatch(/e\+/i);
      expect(headers['X-RateLimit-Limit']).not.toMatch(/e-/i);
    });

    it.fails('should validate header values are ASCII-safe', () => {
      // Edge case: Number.prototype.toString can be overridden
      const maliciousNumber = {
        toString: () => 'Content-Type: text/html\r\n\r\n<script>alert(1)</script>',
        valueOf: () => 100,
      } as unknown as number;

      const result = createRateLimitResultWithInfo({
        limit: maliciousNumber,
      });

      // EXPECTED: Should validate header values are safe numeric strings
      // CURRENT BEHAVIOR: No sanitization of toString() output
      const headers = rateLimiter.getRateLimitHeaders(result);
      expect(headers['X-RateLimit-Limit']).toMatch(/^\d+$/);
    });
  });

  // =============================================================================
  // 8. Validation Error Handling Tests
  // =============================================================================

  describe('Validation Error Handling', () => {
    /**
     * When validation fails, the system should handle errors gracefully.
     */

    it.fails('should provide meaningful error for invalid input', () => {
      const invalidResult = {
        allowed: true,
        rateLimit: {
          limit: -1,
          remaining: NaN,
          resetAt: 0,
        },
      } as RateLimitResult;

      // EXPECTED: Should throw with descriptive error message
      // CURRENT BEHAVIOR: No validation, just produces invalid headers
      expect(() => {
        rateLimiter.getRateLimitHeaders(invalidResult);
      }).toThrow(/invalid.*rate.*limit/i);
    });

    it.fails('should validate entire RateLimitResult before header generation', () => {
      const partiallyInvalid = createRateLimitResultWithInfo({
        limit: 100,
        remaining: -10, // Invalid
        resetAt: Math.floor(Date.now() / 1000) + 60, // Valid
      });

      // EXPECTED: Should validate ALL fields before producing ANY headers
      // CURRENT BEHAVIOR: Each field is converted independently
      expect(() => {
        rateLimiter.getRateLimitHeaders(partiallyInvalid);
      }).toThrow();
    });
  });

  // =============================================================================
  // 9. Missing RateLimit Object Tests
  // =============================================================================

  describe('Missing RateLimit Object Handling', () => {
    /**
     * The rateLimit object could be undefined or malformed.
     */

    it('should crash on missing rateLimit object (documents fragile behavior)', () => {
      const result = {
        allowed: true,
      } as RateLimitResult;

      // CURRENT BEHAVIOR: result.rateLimit.limit throws TypeError
      // This documents that the code crashes without validation
      // Proper implementation would validate input and throw clear error
      expect(() => {
        rateLimiter.getRateLimitHeaders(result);
      }).toThrow(TypeError);
    });

    it('should crash on null rateLimit object (documents fragile behavior)', () => {
      const result = {
        allowed: true,
        rateLimit: null as unknown as RateLimitInfo,
      } as RateLimitResult;

      // CURRENT BEHAVIOR: Attempting to access null.limit throws TypeError
      // This documents that the code crashes without validation
      expect(() => {
        rateLimiter.getRateLimitHeaders(result);
      }).toThrow(TypeError);
    });

    it.fails('should provide clear validation error for missing rateLimit', () => {
      const result = {
        allowed: true,
      } as RateLimitResult;

      // EXPECTED: Should throw with descriptive error message about missing rateLimit
      // CURRENT BEHAVIOR: Generic TypeError "Cannot read properties of undefined"
      expect(() => {
        rateLimiter.getRateLimitHeaders(result);
      }).toThrow(/rateLimit.*required|missing.*rateLimit/i);
    });
  });
});
