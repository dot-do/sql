/**
 * CDC Backpressure Controller for DoSQL
 *
 * Implements backpressure propagation from DoLake consumers back to DoSQL CDC producers.
 * This module provides:
 * - Backpressure signal handling (pause/slow_down/resume)
 * - Throttling of WAL capture rate based on downstream signals
 * - Adaptive rate limiting based on buffer utilization
 * - Integration with the CDC capture layer
 *
 * @packageDocumentation
 */

import type { BackpressureSignal } from './types.js';
import { assertNever } from '../utils/assert-never.js';

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * Configuration for the backpressure controller
 */
export interface BackpressureConfig {
  /** Minimum delay between batches (ms) - default: 0 */
  minDelayMs: number;
  /** Maximum delay between batches (ms) - default: 10000 */
  maxDelayMs: number;
  /** Buffer utilization threshold to start backpressure (0-1) - default: 0.7 */
  warningThreshold: number;
  /** Buffer utilization threshold for critical backpressure (0-1) - default: 0.9 */
  criticalThreshold: number;
  /** Base delay multiplier when slowing down - default: 1.5 */
  slowDownMultiplier: number;
  /** Delay decay rate when resuming (0-1) - default: 0.5 */
  resumeDecayRate: number;
  /** Minimum batch size when under backpressure - default: 10 */
  minBatchSize: number;
  /** Maximum batch size when healthy - default: 1000 */
  maxBatchSize: number;
  /** Enable adaptive batch sizing - default: true */
  adaptiveBatchSize: boolean;
}

/**
 * Default backpressure configuration
 */
export const DEFAULT_BACKPRESSURE_CONFIG: Readonly<BackpressureConfig> = {
  minDelayMs: 0,
  maxDelayMs: 10000,
  warningThreshold: 0.7,
  criticalThreshold: 0.9,
  slowDownMultiplier: 1.5,
  resumeDecayRate: 0.5,
  minBatchSize: 10,
  maxBatchSize: 1000,
  adaptiveBatchSize: true,
};

// =============================================================================
// Backpressure State Types
// =============================================================================

/**
 * Current state of backpressure
 */
export type BackpressureState = 'normal' | 'warning' | 'critical' | 'paused';

/**
 * Backpressure metrics for monitoring
 */
export interface BackpressureMetrics {
  /** Current state */
  state: BackpressureState;
  /** Current delay between batches (ms) */
  currentDelayMs: number;
  /** Current batch size limit */
  currentBatchSize: number;
  /** Last buffer utilization reported */
  lastBufferUtilization: number;
  /** Total pause signals received */
  totalPauseSignals: number;
  /** Total slow_down signals received */
  totalSlowDownSignals: number;
  /** Total resume signals received */
  totalResumeSignals: number;
  /** Time spent in paused state (ms) */
  totalPausedTimeMs: number;
  /** Time spent in warning state (ms) */
  totalWarningTimeMs: number;
  /** Time spent in critical state (ms) */
  totalCriticalTimeMs: number;
  /** Last signal received timestamp */
  lastSignalAt: number | null;
  /** Last state change timestamp */
  lastStateChangeAt: number;
}

/**
 * Callback for when backpressure state changes
 */
export type BackpressureStateChangeCallback = (
  newState: BackpressureState,
  previousState: BackpressureState,
  metrics: BackpressureMetrics
) => void;

/**
 * Callback for when batch parameters change
 */
export type BatchParameterChangeCallback = (
  delayMs: number,
  batchSize: number
) => void;

// =============================================================================
// Backpressure Controller
// =============================================================================

/**
 * Controller for managing backpressure from DoLake consumers
 *
 * This controller receives backpressure signals from DoLake and adjusts
 * the CDC capture rate accordingly. It provides:
 *
 * - Automatic delay adjustment based on signal type
 * - Adaptive batch sizing based on buffer utilization
 * - State tracking for monitoring and alerting
 * - Callbacks for integration with capture layer
 *
 * @example
 * ```typescript
 * // Create controller
 * const backpressure = new BackpressureController({
 *   maxDelayMs: 5000,
 *   criticalThreshold: 0.9,
 * });
 *
 * // Subscribe to state changes
 * backpressure.onStateChange((newState, oldState, metrics) => {
 *   console.log(`Backpressure: ${oldState} -> ${newState}`);
 * });
 *
 * // Handle signal from DoLake
 * backpressure.handleSignal({
 *   type: 'slow_down',
 *   bufferUtilization: 0.85,
 *   suggestedDelayMs: 500,
 *   reason: 'Buffer 85% full',
 * });
 *
 * // Get current batch parameters
 * const { delayMs, batchSize } = backpressure.getBatchParameters();
 *
 * // Wait before next batch
 * await backpressure.waitForDelay();
 * ```
 */
export class BackpressureController {
  private readonly config: BackpressureConfig;

  // Current state
  private state: BackpressureState = 'normal';
  private currentDelayMs = 0;
  private currentBatchSize: number;
  private lastBufferUtilization = 0;
  private pausedAt: number | null = null;

  // Metrics
  private totalPauseSignals = 0;
  private totalSlowDownSignals = 0;
  private totalResumeSignals = 0;
  private totalPausedTimeMs = 0;
  private totalWarningTimeMs = 0;
  private totalCriticalTimeMs = 0;
  private lastSignalAt: number | null = null;
  private lastStateChangeAt: number = Date.now();
  private stateEnteredAt: number = Date.now();

  // Callbacks
  private stateChangeCallbacks: Set<BackpressureStateChangeCallback> = new Set();
  private batchParameterCallbacks: Set<BatchParameterChangeCallback> = new Set();

  /**
   * Creates a new BackpressureController
   *
   * @param config - Configuration options
   */
  constructor(config: Partial<BackpressureConfig> = {}) {
    this.config = {
      ...DEFAULT_BACKPRESSURE_CONFIG,
      ...config,
    };
    this.currentBatchSize = this.config.maxBatchSize;
  }

  // ===========================================================================
  // Signal Handling
  // ===========================================================================

  /**
   * Handle a backpressure signal from DoLake
   *
   * @param signal - The backpressure signal
   */
  handleSignal(signal: BackpressureSignal): void {
    const now = Date.now();
    this.lastSignalAt = now;
    this.lastBufferUtilization = signal.bufferUtilization;

    const previousState = this.state;

    switch (signal.type) {
      case 'pause':
        this.handlePauseSignal(signal, now);
        break;
      case 'slow_down':
        this.handleSlowDownSignal(signal, now);
        break;
      case 'resume':
        this.handleResumeSignal(signal, now);
        break;
      default:
        assertNever(signal.type, `Unknown backpressure signal type: ${signal.type}`);
    }

    // Update state based on buffer utilization
    this.updateStateFromUtilization(signal.bufferUtilization);

    // Notify state change if applicable
    if (this.state !== previousState) {
      this.recordStateTime(previousState, now);
      this.lastStateChangeAt = now;
      this.stateEnteredAt = now;
      this.notifyStateChange(previousState);
    }

    // Notify batch parameter change
    this.notifyBatchParameterChange();
  }

  /**
   * Handle a pause signal
   */
  private handlePauseSignal(signal: BackpressureSignal, now: number): void {
    this.totalPauseSignals++;
    this.pausedAt = now;

    // Set maximum delay
    this.currentDelayMs = this.config.maxDelayMs;

    // Reduce batch size to minimum
    if (this.config.adaptiveBatchSize) {
      this.currentBatchSize = this.config.minBatchSize;
    }
  }

  /**
   * Handle a slow_down signal
   */
  private handleSlowDownSignal(signal: BackpressureSignal, _now: number): void {
    this.totalSlowDownSignals++;

    // Use suggested delay if provided, otherwise calculate based on utilization
    if (signal.suggestedDelayMs !== undefined) {
      this.currentDelayMs = Math.min(signal.suggestedDelayMs, this.config.maxDelayMs);
    } else {
      // Increase delay based on utilization
      const baseDelay = this.currentDelayMs || this.config.minDelayMs;
      const multiplier = 1 + (signal.bufferUtilization * this.config.slowDownMultiplier);
      this.currentDelayMs = Math.min(
        baseDelay * multiplier,
        this.config.maxDelayMs
      );
    }

    // Reduce batch size based on utilization
    if (this.config.adaptiveBatchSize) {
      const utilizationFactor = 1 - signal.bufferUtilization;
      this.currentBatchSize = Math.max(
        this.config.minBatchSize,
        Math.floor(this.config.maxBatchSize * utilizationFactor)
      );
    }
  }

  /**
   * Handle a resume signal
   */
  private handleResumeSignal(signal: BackpressureSignal, now: number): void {
    this.totalResumeSignals++;

    // Record paused time
    if (this.pausedAt !== null) {
      this.totalPausedTimeMs += now - this.pausedAt;
      this.pausedAt = null;
    }

    // Decay delay
    this.currentDelayMs = Math.floor(this.currentDelayMs * this.config.resumeDecayRate);
    if (this.currentDelayMs < this.config.minDelayMs) {
      this.currentDelayMs = this.config.minDelayMs;
    }

    // Increase batch size
    if (this.config.adaptiveBatchSize) {
      const utilizationFactor = 1 - signal.bufferUtilization;
      this.currentBatchSize = Math.min(
        this.config.maxBatchSize,
        Math.floor(this.currentBatchSize + (this.config.maxBatchSize * utilizationFactor * 0.2))
      );
    }
  }

  /**
   * Update state based on buffer utilization
   */
  private updateStateFromUtilization(utilization: number): void {
    if (this.pausedAt !== null) {
      this.state = 'paused';
    } else if (utilization >= this.config.criticalThreshold) {
      this.state = 'critical';
    } else if (utilization >= this.config.warningThreshold) {
      this.state = 'warning';
    } else {
      this.state = 'normal';
    }
  }

  /**
   * Record time spent in a state
   */
  private recordStateTime(state: BackpressureState, now: number): void {
    const duration = now - this.stateEnteredAt;

    switch (state) {
      case 'paused':
        this.totalPausedTimeMs += duration;
        break;
      case 'warning':
        this.totalWarningTimeMs += duration;
        break;
      case 'critical':
        this.totalCriticalTimeMs += duration;
        break;
    }
  }

  // ===========================================================================
  // Batch Parameters
  // ===========================================================================

  /**
   * Get current batch parameters based on backpressure state
   *
   * @returns Object containing delay and batch size
   */
  getBatchParameters(): { delayMs: number; batchSize: number } {
    return {
      delayMs: this.currentDelayMs,
      batchSize: this.currentBatchSize,
    };
  }

  /**
   * Wait for the current delay before proceeding
   *
   * @returns Promise that resolves after the delay
   */
  async waitForDelay(): Promise<void> {
    if (this.currentDelayMs > 0) {
      await new Promise(resolve => setTimeout(resolve, this.currentDelayMs));
    }
  }

  /**
   * Check if CDC capture should proceed
   *
   * @returns true if capture can proceed, false if paused
   */
  shouldProceed(): boolean {
    return this.state !== 'paused';
  }

  /**
   * Reset the controller to initial state
   */
  reset(): void {
    const previousState = this.state;
    const now = Date.now();

    this.recordStateTime(previousState, now);

    this.state = 'normal';
    this.currentDelayMs = this.config.minDelayMs;
    this.currentBatchSize = this.config.maxBatchSize;
    this.lastBufferUtilization = 0;
    this.pausedAt = null;
    this.lastStateChangeAt = now;
    this.stateEnteredAt = now;

    if (previousState !== 'normal') {
      this.notifyStateChange(previousState);
    }
    this.notifyBatchParameterChange();
  }

  // ===========================================================================
  // Metrics
  // ===========================================================================

  /**
   * Get current metrics
   */
  getMetrics(): BackpressureMetrics {
    const now = Date.now();

    // Update time tracking for current state
    const currentStateDuration = now - this.stateEnteredAt;
    let totalPausedTime = this.totalPausedTimeMs;
    let totalWarningTime = this.totalWarningTimeMs;
    let totalCriticalTime = this.totalCriticalTimeMs;

    switch (this.state) {
      case 'paused':
        totalPausedTime += currentStateDuration;
        break;
      case 'warning':
        totalWarningTime += currentStateDuration;
        break;
      case 'critical':
        totalCriticalTime += currentStateDuration;
        break;
    }

    return {
      state: this.state,
      currentDelayMs: this.currentDelayMs,
      currentBatchSize: this.currentBatchSize,
      lastBufferUtilization: this.lastBufferUtilization,
      totalPauseSignals: this.totalPauseSignals,
      totalSlowDownSignals: this.totalSlowDownSignals,
      totalResumeSignals: this.totalResumeSignals,
      totalPausedTimeMs: totalPausedTime,
      totalWarningTimeMs: totalWarningTime,
      totalCriticalTimeMs: totalCriticalTime,
      lastSignalAt: this.lastSignalAt,
      lastStateChangeAt: this.lastStateChangeAt,
    };
  }

  /**
   * Get current state
   */
  getState(): BackpressureState {
    return this.state;
  }

  // ===========================================================================
  // Event Handlers
  // ===========================================================================

  /**
   * Register a callback for state changes
   */
  onStateChange(callback: BackpressureStateChangeCallback): () => void {
    this.stateChangeCallbacks.add(callback);
    return () => this.stateChangeCallbacks.delete(callback);
  }

  /**
   * Register a callback for batch parameter changes
   */
  onBatchParameterChange(callback: BatchParameterChangeCallback): () => void {
    this.batchParameterCallbacks.add(callback);
    return () => this.batchParameterCallbacks.delete(callback);
  }

  /**
   * Notify state change listeners
   */
  private notifyStateChange(previousState: BackpressureState): void {
    const metrics = this.getMetrics();
    for (const callback of this.stateChangeCallbacks) {
      try {
        callback(this.state, previousState, metrics);
      } catch (error) {
        console.error('Error in backpressure state change callback:', error);
      }
    }
  }

  /**
   * Notify batch parameter change listeners
   */
  private notifyBatchParameterChange(): void {
    for (const callback of this.batchParameterCallbacks) {
      try {
        callback(this.currentDelayMs, this.currentBatchSize);
      } catch (error) {
        console.error('Error in batch parameter change callback:', error);
      }
    }
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a new backpressure controller
 *
 * @param config - Configuration options
 * @returns A new BackpressureController instance
 */
export function createBackpressureController(
  config: Partial<BackpressureConfig> = {}
): BackpressureController {
  return new BackpressureController(config);
}

// =============================================================================
// Lakehouse Integration
// =============================================================================

/**
 * Response from DoLake with backpressure information
 */
export interface LakehouseAckWithBackpressure {
  /** Acknowledgment status */
  status: 'ok' | 'buffered' | 'persisted' | 'duplicate';
  /** Buffer utilization (0-1) */
  bufferUtilization?: number;
  /** Suggested delay before next batch (ms) */
  suggestedDelayMs?: number;
  /** Circuit breaker state */
  circuitBreakerState?: 'closed' | 'open' | 'half-open';
  /** Remaining rate limit tokens */
  remainingTokens?: number;
  /** Token bucket capacity */
  bucketCapacity?: number;
}

/**
 * Convert a DoLake ACK response to a backpressure signal
 *
 * @param ack - The ACK response from DoLake
 * @returns A backpressure signal, or null if no backpressure needed
 */
export function ackToBackpressureSignal(ack: LakehouseAckWithBackpressure): BackpressureSignal | null {
  const utilization = ack.bufferUtilization ?? 0;

  // Check circuit breaker state
  if (ack.circuitBreakerState === 'open') {
    return {
      type: 'pause',
      bufferUtilization: utilization,
      suggestedDelayMs: 5000,
      reason: 'Circuit breaker open - downstream unavailable',
    };
  }

  // Check for high buffer utilization
  if (utilization >= 0.9) {
    return {
      type: 'pause',
      bufferUtilization: utilization,
      suggestedDelayMs: ack.suggestedDelayMs ?? 1000,
      reason: `Buffer ${Math.round(utilization * 100)}% full - critical`,
    };
  }

  if (utilization >= 0.7) {
    return {
      type: 'slow_down',
      bufferUtilization: utilization,
      suggestedDelayMs: ack.suggestedDelayMs ?? Math.floor(utilization * 1000),
      reason: `Buffer ${Math.round(utilization * 100)}% full - slowing down`,
    };
  }

  // Check rate limit tokens
  if (ack.remainingTokens !== undefined && ack.bucketCapacity !== undefined) {
    const tokenUtilization = 1 - (ack.remainingTokens / ack.bucketCapacity);
    if (tokenUtilization >= 0.9) {
      return {
        type: 'slow_down',
        bufferUtilization: utilization,
        suggestedDelayMs: ack.suggestedDelayMs ?? 500,
        reason: 'Rate limit tokens depleted',
      };
    }
  }

  // Buffer is healthy, resume if we were under backpressure
  if (utilization < 0.5) {
    return {
      type: 'resume',
      bufferUtilization: utilization,
      reason: 'Buffer drained',
    };
  }

  return null;
}

/**
 * Convert a DoLake NACK response to a backpressure signal
 *
 * @param reason - The NACK reason
 * @param retryDelayMs - Suggested retry delay
 * @returns A backpressure signal
 */
export function nackToBackpressureSignal(
  reason: string,
  retryDelayMs?: number
): BackpressureSignal {
  switch (reason) {
    case 'buffer_full':
      return {
        type: 'pause',
        bufferUtilization: 1.0,
        suggestedDelayMs: retryDelayMs ?? 5000,
        reason: 'Buffer full - pausing',
      };

    case 'rate_limited':
      return {
        type: 'slow_down',
        bufferUtilization: 0.8, // Assume moderate utilization
        suggestedDelayMs: retryDelayMs ?? 1000,
        reason: 'Rate limited - slowing down',
      };

    case 'load_shedding':
      return {
        type: 'slow_down',
        bufferUtilization: 0.9,
        suggestedDelayMs: retryDelayMs ?? 2000,
        reason: 'Load shedding - system under pressure',
      };

    default:
      return {
        type: 'slow_down',
        bufferUtilization: 0.7,
        suggestedDelayMs: retryDelayMs ?? 500,
        reason: `NACK received: ${reason}`,
      };
  }
}
