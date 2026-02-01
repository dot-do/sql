/**
 * Retry Utilities Tests
 *
 * Tests for retry logic including exponential backoff,
 * retry predicates, contextual retry, and helper utilities.
 */

import { describe, it, expect, vi } from 'vitest';
import {
  withRetry,
  withRetryResult,
  withContextualRetry,
  sleep,
  createRetryWrapper,
  retryPredicates,
} from '../retry.js';

// =============================================================================
// withRetry
// =============================================================================

describe('withRetry', () => {
  it('should return result on first success', async () => {
    const result = await withRetry(async () => 42);
    expect(result).toBe(42);
  });

  it('should retry on failure and succeed', async () => {
    let attempts = 0;
    const result = await withRetry(
      async () => {
        attempts++;
        if (attempts < 3) throw new Error('fail');
        return 'success';
      },
      { maxAttempts: 3, initialDelayMs: 1 }
    );
    expect(result).toBe('success');
    expect(attempts).toBe(3);
  });

  it('should throw after exhausting all attempts', async () => {
    await expect(
      withRetry(
        async () => { throw new Error('always fails'); },
        { maxAttempts: 2, initialDelayMs: 1 }
      )
    ).rejects.toThrow('always fails');
  });

  it('should throw immediately for non-retryable errors', async () => {
    let attempts = 0;
    await expect(
      withRetry(
        async () => {
          attempts++;
          throw new Error('fatal');
        },
        {
          maxAttempts: 5,
          initialDelayMs: 1,
          isRetryable: () => false,
        }
      )
    ).rejects.toThrow('fatal');
    expect(attempts).toBe(1);
  });

  it('should call onRetry callback on each retry', async () => {
    const onRetry = vi.fn();
    let attempts = 0;
    await withRetry(
      async () => {
        attempts++;
        if (attempts < 3) throw new Error('fail');
        return 'ok';
      },
      { maxAttempts: 3, initialDelayMs: 1, onRetry }
    );
    expect(onRetry).toHaveBeenCalledTimes(2);
    expect(onRetry.mock.calls[0][1]).toBe(1); // attempt 1
    expect(onRetry.mock.calls[1][1]).toBe(2); // attempt 2
  });

  it('should respect maxDelayMs', async () => {
    const delays: number[] = [];
    let attempts = 0;
    try {
      await withRetry(
        async () => {
          attempts++;
          throw new Error('fail');
        },
        {
          maxAttempts: 4,
          initialDelayMs: 100,
          maxDelayMs: 150,
          backoffMultiplier: 10,
          onRetry: (_err, _attempt, nextDelay) => {
            delays.push(nextDelay);
          },
        }
      );
    } catch {
      // expected
    }
    // All delays should be capped at maxDelayMs
    for (const delay of delays) {
      expect(delay).toBeLessThanOrEqual(150);
    }
  });

  it('should convert non-Error throws to Error', async () => {
    await expect(
      withRetry(
        async () => { throw 'string error'; },
        { maxAttempts: 1 }
      )
    ).rejects.toThrow('string error');
  });
});

// =============================================================================
// withRetryResult
// =============================================================================

describe('withRetryResult', () => {
  it('should return success result', async () => {
    const result = await withRetryResult(async () => 42);
    expect(result.success).toBe(true);
    expect(result.result).toBe(42);
    expect(result.attempts).toBe(1);
    expect(result.totalTimeMs).toBeGreaterThanOrEqual(0);
  });

  it('should return failure result without throwing', async () => {
    const result = await withRetryResult(
      async () => { throw new Error('fail'); },
      { maxAttempts: 2, initialDelayMs: 1 }
    );
    expect(result.success).toBe(false);
    expect(result.error).toBeInstanceOf(Error);
    expect(result.error?.message).toBe('fail');
    expect(result.attempts).toBe(2);
  });

  it('should track total time', async () => {
    const result = await withRetryResult(async () => 'ok');
    expect(typeof result.totalTimeMs).toBe('number');
    expect(result.totalTimeMs).toBeGreaterThanOrEqual(0);
  });
});

// =============================================================================
// withContextualRetry
// =============================================================================

describe('withContextualRetry', () => {
  it('should pass context to function', async () => {
    const ctx = { count: 0 };
    await withContextualRetry(
      async (c) => {
        expect(c).toBe(ctx);
        return 'ok';
      },
      { context: ctx }
    );
  });

  it('should call onSuccess with context', async () => {
    const ctx = { successCalled: false };
    await withContextualRetry(
      async () => 'ok',
      {
        context: ctx,
        onSuccess: (c) => { c.successCalled = true; },
      }
    );
    expect(ctx.successCalled).toBe(true);
  });

  it('should call onFailure with context on error', async () => {
    const ctx = { failureCalled: false };
    let attempts = 0;
    await withContextualRetry(
      async () => {
        attempts++;
        if (attempts < 2) throw new Error('fail');
        return 'ok';
      },
      {
        context: ctx,
        onFailure: (c) => { c.failureCalled = true; },
        maxAttempts: 2,
        initialDelayMs: 1,
      }
    );
    expect(ctx.failureCalled).toBe(true);
  });

  it('should work without context', async () => {
    const result = await withContextualRetry(
      async (ctx) => {
        expect(ctx).toBeUndefined();
        return 42;
      }
    );
    expect(result).toBe(42);
  });
});

// =============================================================================
// sleep
// =============================================================================

describe('sleep', () => {
  it('should resolve after specified time', async () => {
    const start = performance.now();
    await sleep(10);
    const elapsed = performance.now() - start;
    expect(elapsed).toBeGreaterThanOrEqual(5); // some tolerance
  });

  it('should resolve immediately for 0ms', async () => {
    const start = performance.now();
    await sleep(0);
    const elapsed = performance.now() - start;
    expect(elapsed).toBeLessThan(50);
  });
});

// =============================================================================
// createRetryWrapper
// =============================================================================

describe('createRetryWrapper', () => {
  it('should create a reusable retry function with defaults', async () => {
    const retrier = createRetryWrapper({ maxAttempts: 2, initialDelayMs: 1 });
    const result = await retrier(async () => 'ok');
    expect(result).toBe('ok');
  });

  it('should allow overriding options', async () => {
    const retrier = createRetryWrapper({ maxAttempts: 1, initialDelayMs: 1 });
    let attempts = 0;
    const result = await retrier(
      async () => {
        attempts++;
        if (attempts < 3) throw new Error('fail');
        return 'ok';
      },
      { maxAttempts: 3, initialDelayMs: 1 }
    );
    expect(result).toBe('ok');
    expect(attempts).toBe(3);
  });
});

// =============================================================================
// retryPredicates
// =============================================================================

describe('retryPredicates', () => {
  describe('always', () => {
    it('should always return true', () => {
      expect(retryPredicates.always()).toBe(true);
    });
  });

  describe('never', () => {
    it('should always return false', () => {
      expect(retryPredicates.never()).toBe(false);
    });
  });

  describe('networkErrors', () => {
    it('should match network errors', () => {
      expect(retryPredicates.networkErrors(new Error('network error'))).toBe(true);
      expect(retryPredicates.networkErrors(new Error('connection refused'))).toBe(true);
      expect(retryPredicates.networkErrors(new Error('timeout occurred'))).toBe(true);
      expect(retryPredicates.networkErrors(new Error('ECONNRESET'))).toBe(true);
      expect(retryPredicates.networkErrors(new Error('ENOTFOUND'))).toBe(true);
    });

    it('should not match non-network errors', () => {
      expect(retryPredicates.networkErrors(new Error('syntax error'))).toBe(false);
      expect(retryPredicates.networkErrors(new Error('permission denied'))).toBe(false);
    });

    it('should handle non-Error objects', () => {
      expect(retryPredicates.networkErrors('string error')).toBe(false);
    });
  });

  describe('transientErrors', () => {
    it('should match 5xx status codes', () => {
      expect(retryPredicates.transientErrors({ status: 500 })).toBe(true);
      expect(retryPredicates.transientErrors({ status: 502 })).toBe(true);
      expect(retryPredicates.transientErrors({ status: 503 })).toBe(true);
      expect(retryPredicates.transientErrors({ status: 599 })).toBe(true);
    });

    it('should not match 4xx or other status codes', () => {
      expect(retryPredicates.transientErrors({ status: 400 })).toBe(false);
      expect(retryPredicates.transientErrors({ status: 404 })).toBe(false);
      expect(retryPredicates.transientErrors({ status: 200 })).toBe(false);
    });

    it('should return false for errors without status', () => {
      expect(retryPredicates.transientErrors(new Error('fail'))).toBe(false);
      expect(retryPredicates.transientErrors(null)).toBe(false);
    });
  });
});
