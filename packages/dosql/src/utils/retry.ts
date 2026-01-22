/**
 * Retry Utilities
 *
 * Shared retry patterns with exponential backoff for the DoSQL package.
 */

// =============================================================================
// TYPES
// =============================================================================

/**
 * Options for retry operations
 */
export interface RetryOptions {
  /** Maximum number of retry attempts (default: 3) */
  maxAttempts?: number;
  /** Initial delay between retries in ms (default: 100) */
  initialDelayMs?: number;
  /** Maximum delay between retries in ms (default: 10000) */
  maxDelayMs?: number;
  /** Exponential backoff multiplier (default: 2) */
  backoffMultiplier?: number;
  /** Predicate to determine if error is retryable (default: always true) */
  isRetryable?: (error: unknown) => boolean;
  /** Callback invoked on each retry (for logging/metrics) */
  onRetry?: (error: unknown, attempt: number, nextDelayMs: number) => void;
}

/**
 * Result of a retry operation
 */
export interface RetryResult<T> {
  success: boolean;
  result?: T;
  error?: Error;
  attempts: number;
  totalTimeMs: number;
}

// =============================================================================
// CORE RETRY FUNCTION
// =============================================================================

/**
 * Execute a function with automatic retry on failure.
 *
 * Uses exponential backoff between retries with configurable parameters.
 *
 * @param fn - The async function to execute
 * @param options - Retry configuration options
 * @returns Promise that resolves with the function result or rejects after all retries exhausted
 *
 * @example
 * ```typescript
 * // Simple retry with defaults
 * const result = await withRetry(() => fetchData(url));
 *
 * // Custom retry configuration
 * const result = await withRetry(
 *   () => unreliableApi.call(),
 *   {
 *     maxAttempts: 5,
 *     initialDelayMs: 200,
 *     isRetryable: (err) => err instanceof NetworkError,
 *   }
 * );
 * ```
 */
export async function withRetry<T>(
  fn: () => Promise<T>,
  options: RetryOptions = {}
): Promise<T> {
  const {
    maxAttempts = 3,
    initialDelayMs = 100,
    maxDelayMs = 10000,
    backoffMultiplier = 2,
    isRetryable = () => true,
    onRetry,
  } = options;

  let lastError: Error | undefined;
  let currentDelay = initialDelayMs;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));

      const isLast = attempt === maxAttempts;
      const shouldRetry = !isLast && isRetryable(error);

      if (!shouldRetry) {
        throw lastError;
      }

      // Calculate next delay with exponential backoff
      const nextDelay = Math.min(currentDelay, maxDelayMs);

      // Notify caller of retry
      onRetry?.(error, attempt, nextDelay);

      // Wait before retrying
      await sleep(nextDelay);
      currentDelay *= backoffMultiplier;
    }
  }

  throw lastError;
}

/**
 * Execute a function with retry and return detailed result.
 *
 * Unlike withRetry, this function never throws and instead returns
 * a result object with success/failure information.
 *
 * @param fn - The async function to execute
 * @param options - Retry configuration options
 * @returns RetryResult with success status, result/error, and metadata
 */
export async function withRetryResult<T>(
  fn: () => Promise<T>,
  options: RetryOptions = {}
): Promise<RetryResult<T>> {
  const startTime = performance.now();
  let attempts = 0;

  try {
    const result = await withRetry(async () => {
      attempts++;
      return await fn();
    }, options);

    return {
      success: true,
      result,
      attempts,
      totalTimeMs: performance.now() - startTime,
    };
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error : new Error(String(error)),
      attempts,
      totalTimeMs: performance.now() - startTime,
    };
  }
}

// =============================================================================
// CONTEXT-AWARE RETRY
// =============================================================================

/**
 * Options for contextual retry operations
 */
export interface ContextualRetryOptions<Ctx> extends RetryOptions {
  /** Context to pass through retry attempts */
  context?: Ctx;
  /** Update context on success */
  onSuccess?: (ctx: Ctx) => void;
  /** Update context on failure */
  onFailure?: (ctx: Ctx, error: unknown) => void;
}

/**
 * Execute with retry and track state in a context object.
 *
 * Useful for operations that need to track retry count, last success time, etc.
 *
 * @param fn - The async function to execute
 * @param options - Retry options with context handling
 * @returns Promise that resolves with the function result
 */
export async function withContextualRetry<T, Ctx>(
  fn: (ctx: Ctx | undefined) => Promise<T>,
  options: ContextualRetryOptions<Ctx> = {}
): Promise<T> {
  const { context, onSuccess, onFailure, ...retryOptions } = options;

  return withRetry(
    async () => {
      try {
        const result = await fn(context);
        if (context) onSuccess?.(context);
        return result;
      } catch (error) {
        if (context) onFailure?.(context, error);
        throw error;
      }
    },
    retryOptions
  );
}

// =============================================================================
// HELPERS
// =============================================================================

/**
 * Sleep for a specified duration.
 *
 * @param ms - Duration to sleep in milliseconds
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Create a reusable retry wrapper with preset options.
 *
 * @param defaultOptions - Default options for all retries
 * @returns A retry function with the preset options
 *
 * @example
 * ```typescript
 * const retryWithLogging = createRetryWrapper({
 *   maxAttempts: 5,
 *   onRetry: (err, attempt) => console.log(`Retry ${attempt}: ${err}`),
 * });
 *
 * const result = await retryWithLogging(() => fetchData());
 * ```
 */
export function createRetryWrapper(
  defaultOptions: RetryOptions
): <T>(fn: () => Promise<T>, overrideOptions?: RetryOptions) => Promise<T> {
  return async <T>(fn: () => Promise<T>, overrideOptions?: RetryOptions) => {
    return withRetry(fn, { ...defaultOptions, ...overrideOptions });
  };
}

/**
 * Common retryable error predicates
 */
export const retryPredicates = {
  /** Retry on any error */
  always: () => true,

  /** Never retry */
  never: () => false,

  /** Retry on network-related errors */
  networkErrors: (error: unknown): boolean => {
    const msg = error instanceof Error ? error.message.toLowerCase() : '';
    return (
      msg.includes('network') ||
      msg.includes('timeout') ||
      msg.includes('connection') ||
      msg.includes('econnreset') ||
      msg.includes('enotfound')
    );
  },

  /** Retry on transient errors (5xx status codes) */
  transientErrors: (error: unknown): boolean => {
    if (error && typeof error === 'object' && 'status' in error) {
      const status = (error as { status: number }).status;
      return status >= 500 && status < 600;
    }
    return false;
  },
};
