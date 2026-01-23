/**
 * WAL Retention Scheduler Module
 *
 * Handles background cleanup scheduling, time window management,
 * and throttling for retention operations.
 *
 * @packageDocumentation
 */

import type {
  RetentionPolicy,
  ThrottleConfig,
  CleanupProgressEvent,
} from '../retention-types.js';
import { isInTimeWindow, getNextCronTime } from './policy.js';

// =============================================================================
// Scheduler State Interface
// =============================================================================

/**
 * Internal state for the retention scheduler
 */
export interface SchedulerState {
  /** Background cleanup timer */
  cleanupTimer: ReturnType<typeof setInterval> | null;
  /** Whether cleanup is currently in progress */
  cleanupInProgress: boolean;
  /** Distributed cleanup lock */
  cleanupLock: boolean;
  /** Throttle configuration */
  throttleConfig: ThrottleConfig;
}

/**
 * Cleanup function type
 */
export type CleanupFunction = (dryRun?: boolean) => Promise<void>;

// =============================================================================
// Scheduler Factory
// =============================================================================

/**
 * Create a retention scheduler
 *
 * @param policy - Retention policy configuration
 * @param cleanupFn - Function to call for cleanup
 * @param onProgress - Optional progress callback
 * @returns Scheduler instance
 */
export function createRetentionScheduler(
  policy: RetentionPolicy,
  cleanupFn: CleanupFunction,
  onProgress?: (event: CleanupProgressEvent) => void
): RetentionScheduler {
  const state: SchedulerState = {
    cleanupTimer: null,
    cleanupInProgress: false,
    cleanupLock: false,
    throttleConfig: {
      maxBatchSize: policy.maxCleanupBatchSize ?? 100,
      throttleMs: policy.cleanupThrottleMs ?? 0,
    },
  };

  return new RetentionSchedulerImpl(state, policy, cleanupFn, onProgress);
}

// =============================================================================
// Scheduler Interface
// =============================================================================

/**
 * Retention scheduler interface
 */
export interface RetentionScheduler {
  /**
   * Start background cleanup
   */
  start(): void;

  /**
   * Stop background cleanup
   */
  stop(): void;

  /**
   * Check if scheduler is running
   */
  isRunning(): boolean;

  /**
   * Get next scheduled cleanup time
   */
  getNextCleanupTime(): Date | null;

  /**
   * Set throttle configuration
   */
  setThrottleConfig(config: ThrottleConfig): void;

  /**
   * Get current throttle configuration
   */
  getThrottleConfig(): ThrottleConfig;

  /**
   * Check if currently in low activity window
   */
  isInLowActivityWindow(): boolean;

  /**
   * Acquire cleanup lock (for distributed coordination)
   */
  acquireCleanupLock(): Promise<boolean>;

  /**
   * Release cleanup lock
   */
  releaseCleanupLock(): void;

  /**
   * Check if cleanup is currently in progress
   */
  isCleanupInProgress(): boolean;

  /**
   * Update the policy
   */
  updatePolicy(policy: Partial<RetentionPolicy>): void;

  /**
   * Trigger an immediate cleanup (async)
   */
  triggerCleanup(): Promise<void>;
}

// =============================================================================
// Scheduler Implementation
// =============================================================================

/**
 * Retention scheduler implementation
 */
class RetentionSchedulerImpl implements RetentionScheduler {
  private state: SchedulerState;
  private policy: RetentionPolicy;
  private cleanupFn: CleanupFunction;
  private onProgress: ((event: CleanupProgressEvent) => void) | undefined;

  constructor(
    state: SchedulerState,
    policy: RetentionPolicy,
    cleanupFn: CleanupFunction,
    onProgress?: (event: CleanupProgressEvent) => void
  ) {
    this.state = state;
    this.policy = policy;
    this.cleanupFn = cleanupFn;
    this.onProgress = onProgress ?? undefined;
  }

  start(): void {
    if (this.state.cleanupTimer) return;

    const intervalMs = this.policy.cleanupIntervalMs ?? 60000;

    this.state.cleanupTimer = setInterval(async () => {
      await this.runScheduledCleanup();
    }, intervalMs);
  }

  stop(): void {
    if (this.state.cleanupTimer) {
      clearInterval(this.state.cleanupTimer);
      this.state.cleanupTimer = null;
    }
  }

  isRunning(): boolean {
    return this.state.cleanupTimer !== null;
  }

  getNextCleanupTime(): Date | null {
    if (this.policy.cleanupSchedule) {
      try {
        return getNextCronTime(this.policy.cleanupSchedule);
      } catch {
        return null;
      }
    }
    return null;
  }

  setThrottleConfig(config: ThrottleConfig): void {
    this.state.throttleConfig = { ...config };
  }

  getThrottleConfig(): ThrottleConfig {
    return { ...this.state.throttleConfig };
  }

  isInLowActivityWindow(): boolean {
    if (!this.policy.lowActivityWindow) return false;
    return isInTimeWindow(this.policy.lowActivityWindow);
  }

  async acquireCleanupLock(): Promise<boolean> {
    if (this.state.cleanupLock) return false;
    this.state.cleanupLock = true;
    return true;
  }

  releaseCleanupLock(): void {
    this.state.cleanupLock = false;
  }

  isCleanupInProgress(): boolean {
    return this.state.cleanupInProgress;
  }

  updatePolicy(policy: Partial<RetentionPolicy>): void {
    Object.assign(this.policy, policy);

    // Update throttle config if related fields changed
    if (policy.maxCleanupBatchSize !== undefined) {
      this.state.throttleConfig.maxBatchSize = policy.maxCleanupBatchSize;
    }
    if (policy.cleanupThrottleMs !== undefined) {
      this.state.throttleConfig.throttleMs = policy.cleanupThrottleMs;
    }
  }

  async triggerCleanup(): Promise<void> {
    if (this.state.cleanupInProgress) {
      return;
    }
    await this.runCleanup();
  }

  /**
   * Run scheduled cleanup with window checks
   */
  private async runScheduledCleanup(): Promise<void> {
    // Check low activity window if configured
    if (this.policy.preferLowActivityCleanup && this.policy.lowActivityWindow) {
      if (!isInTimeWindow(this.policy.lowActivityWindow)) {
        return;
      }
    }

    await this.runCleanup();
  }

  /**
   * Run cleanup operation
   */
  private async runCleanup(): Promise<void> {
    if (this.state.cleanupInProgress) {
      return;
    }

    this.state.cleanupInProgress = true;

    try {
      this.emitProgress({ type: 'start' });
      await this.cleanupFn(false);
      this.emitProgress({ type: 'complete' });
    } catch (error) {
      this.emitProgress({
        type: 'error',
        error: error instanceof Error ? error.message : String(error),
      });
    } finally {
      this.state.cleanupInProgress = false;
    }
  }

  /**
   * Emit progress event
   */
  private emitProgress(event: CleanupProgressEvent): void {
    if (this.onProgress) {
      this.onProgress(event);
    }
    if (this.policy.onCleanupProgress) {
      this.policy.onCleanupProgress(event);
    }
  }
}

// =============================================================================
// Throttle Utilities
// =============================================================================

/**
 * Create a throttled cleanup iterator
 *
 * @param items - Items to process
 * @param config - Throttle configuration
 * @param processor - Processing function
 * @returns Processed items count
 */
export async function throttledProcess<T>(
  items: T[],
  config: ThrottleConfig,
  processor: (item: T, index: number) => Promise<boolean>
): Promise<number> {
  let processed = 0;

  for (let i = 0; i < items.length; i++) {
    // Check batch size limit
    if (config.maxBatchSize > 0 && processed >= config.maxBatchSize) {
      break;
    }

    const item = items[i];
    if (item === undefined) continue;

    const success = await processor(item, i);
    if (success) {
      processed++;
    }

    // Apply throttle delay
    if (config.throttleMs > 0 && i < items.length - 1) {
      await sleep(config.throttleMs);
    }
  }

  return processed;
}

/**
 * Sleep for specified milliseconds
 *
 * @param ms - Milliseconds to sleep
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// =============================================================================
// Cron Scheduler
// =============================================================================

/**
 * Create a cron-based scheduler
 *
 * @param cronExpression - Cron expression
 * @param cleanupFn - Function to call on schedule
 * @returns Stop function
 */
export function createCronScheduler(
  cronExpression: string,
  cleanupFn: CleanupFunction
): { stop: () => void } {
  let running = true;
  let timeoutId: ReturnType<typeof setTimeout> | null = null;

  const scheduleNext = () => {
    if (!running) return;

    try {
      const nextTime = getNextCronTime(cronExpression);
      const delay = nextTime.getTime() - Date.now();

      timeoutId = setTimeout(async () => {
        if (!running) return;
        await cleanupFn(false);
        scheduleNext();
      }, Math.max(0, delay));
    } catch {
      // Invalid cron expression, stop scheduling
      running = false;
    }
  };

  scheduleNext();

  return {
    stop: () => {
      running = false;
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
    },
  };
}
