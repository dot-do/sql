/**
 * FlushStrategy - Handles flush timing and policies
 *
 * Single responsibility: Determining when to flush and how to
 * handle retry logic with exponential backoff.
 */

import { DurabilityTier } from './types.js';

/**
 * Configuration for flush strategy
 */
export interface FlushStrategyConfig {
  /** Maximum retries for P0 events (default: 100, effectively unlimited) */
  maxP0Retries: number;
  /** Maximum retries for P1 events (default: 3) */
  maxP1Retries: number;
  /** Base delay for exponential backoff in ms (default: 100) */
  baseRetryDelayMs: number;
  /** Maximum retry delay in ms (default: 30000) */
  maxRetryDelayMs: number;
  /** Batch size for VFS sync (default: 100) */
  vfsSyncBatchSize: number;
}

/**
 * Default flush strategy configuration
 */
export const DEFAULT_FLUSH_STRATEGY_CONFIG: FlushStrategyConfig = {
  maxP0Retries: 100,
  maxP1Retries: 3,
  baseRetryDelayMs: 100,
  maxRetryDelayMs: 30000,
  vfsSyncBatchSize: 100,
};

/**
 * Result of a flush decision
 */
export interface FlushDecision {
  /** Whether to proceed with flush */
  shouldFlush: boolean;
  /** Reason for the decision */
  reason: 'size_limit' | 'time_limit' | 'manual' | 'not_needed';
}

/**
 * Retry context for tracking retry state
 */
export interface RetryContext {
  /** Number of retries attempted */
  retryCount: number;
  /** Delays used for each retry */
  retryDelays: number[];
  /** Whether max retries has been reached */
  maxRetriesReached: boolean;
}

/**
 * FlushStrategy - Manages flush timing, retry logic, and backoff policies
 */
export class FlushStrategy {
  private readonly config: FlushStrategyConfig;

  constructor(config: Partial<FlushStrategyConfig> = {}) {
    this.config = { ...DEFAULT_FLUSH_STRATEGY_CONFIG, ...config };
  }

  /**
   * Get configuration
   */
  getConfig(): FlushStrategyConfig {
    return { ...this.config };
  }

  /**
   * Get maximum retries for a given tier
   */
  getMaxRetries(tier: DurabilityTier): number {
    switch (tier) {
      case DurabilityTier.P0:
        return this.config.maxP0Retries;
      case DurabilityTier.P1:
        return this.config.maxP1Retries;
      case DurabilityTier.P2:
        return 0; // P2 uses VFS fallback, no retry
      case DurabilityTier.P3:
        return 0; // P3 drops on failure
      default:
        return 0;
    }
  }

  /**
   * Calculate exponential backoff delay
   */
  calculateBackoff(retryCount: number): number {
    const delay = this.config.baseRetryDelayMs * Math.pow(2, retryCount);
    return Math.min(delay, this.config.maxRetryDelayMs);
  }

  /**
   * Check if retry should continue
   */
  shouldRetry(tier: DurabilityTier, retryCount: number): boolean {
    const maxRetries = this.getMaxRetries(tier);
    return retryCount < maxRetries;
  }

  /**
   * Create a new retry context
   */
  createRetryContext(): RetryContext {
    return {
      retryCount: 0,
      retryDelays: [],
      maxRetriesReached: false,
    };
  }

  /**
   * Record a retry attempt
   */
  recordRetry(context: RetryContext, tier: DurabilityTier): RetryContext {
    const delay = this.calculateBackoff(context.retryCount);
    const newCount = context.retryCount + 1;
    const maxRetries = this.getMaxRetries(tier);

    return {
      retryCount: newCount,
      retryDelays: [...context.retryDelays, delay],
      maxRetriesReached: newCount >= maxRetries,
    };
  }

  /**
   * Delay helper for implementing backoff
   */
  async delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Execute retry with backoff
   */
  async executeWithRetry<T>(
    tier: DurabilityTier,
    operation: () => Promise<T>,
    onRetry?: (retryCount: number, delay: number) => void
  ): Promise<{ result?: T; context: RetryContext; success: boolean; error?: string }> {
    let context = this.createRetryContext();

    while (true) {
      try {
        const result = await operation();
        return { result, context, success: true };
      } catch (e) {
        const error = String(e);

        if (!this.shouldRetry(tier, context.retryCount)) {
          context = { ...context, maxRetriesReached: true };
          return { context, success: false, error };
        }

        const delay = this.calculateBackoff(context.retryCount);
        context = this.recordRetry(context, tier);

        if (onRetry) {
          onRetry(context.retryCount, delay);
        }

        await this.delay(delay);
      }
    }
  }

  /**
   * Get VFS sync batch size
   */
  getVFSSyncBatchSize(): number {
    return this.config.vfsSyncBatchSize;
  }

  /**
   * Determine flush strategy based on tier
   */
  getFlushBehavior(tier: DurabilityTier): {
    useDualWrite: boolean;
    useFallback: boolean;
    fallbackType: 'KV' | 'VFS' | 'none';
    dropOnFailure: boolean;
  } {
    switch (tier) {
      case DurabilityTier.P0:
        return {
          useDualWrite: true,
          useFallback: false,
          fallbackType: 'none',
          dropOnFailure: false,
        };
      case DurabilityTier.P1:
        return {
          useDualWrite: false,
          useFallback: true,
          fallbackType: 'KV',
          dropOnFailure: false,
        };
      case DurabilityTier.P2:
        return {
          useDualWrite: false,
          useFallback: true,
          fallbackType: 'VFS',
          dropOnFailure: false,
        };
      case DurabilityTier.P3:
        return {
          useDualWrite: false,
          useFallback: false,
          fallbackType: 'none',
          dropOnFailure: true,
        };
      default:
        return {
          useDualWrite: false,
          useFallback: true,
          fallbackType: 'VFS',
          dropOnFailure: false,
        };
    }
  }
}
