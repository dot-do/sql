/**
 * CDC Backpressure Controller for DoSQL
 *
 * Implements backpressure propagation from DoLake consumers back to DoSQL CDC producers.
 * This module provides:
 * - Backpressure signal handling (pause/slow_down/resume)
 * - Throttling of WAL capture rate based on downstream signals
 * - Adaptive rate limiting based on buffer utilization
 * - Integration with the CDC capture layer
 * - Watermark-based backpressure (high/low thresholds)
 * - Exactly-once delivery with sequence numbers and LSN deduplication
 * - Consumer acknowledgment tracking with checkpoint persistence
 *
 * @packageDocumentation
 */

import type { BackpressureSignal } from './types.js';
import type { LSN } from '../wal/types.js';
import { assertNever } from '../utils/assert-never.js';
import { createLSN, compareLSN } from '../engine/types.js';
import { createLogger } from '../logging/index.js';

const logger = createLogger({ defaultContext: { module: 'cdc-backpressure' } });

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
        logger.error('Error in backpressure state change callback', error instanceof Error ? error : new Error(String(error)), {
          component: 'BackpressureController',
          callbackType: 'stateChange',
          currentState: this.state,
          previousState,
        });
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
        logger.error('Error in batch parameter change callback', error instanceof Error ? error : new Error(String(error)), {
          component: 'BackpressureController',
          callbackType: 'batchParameterChange',
          currentDelayMs: this.currentDelayMs,
          currentBatchSize: this.currentBatchSize,
        });
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

// =============================================================================
// Watermark-Based Backpressure
// =============================================================================

/**
 * Configuration for watermark-based backpressure
 */
export interface WatermarkConfig {
  /** High watermark - pause capture when buffer reaches this level (default: 0.9) */
  highWatermark: number;
  /** Low watermark - resume capture when buffer drops to this level (default: 0.5) */
  lowWatermark: number;
  /** Maximum buffer size in events (default: 10000) */
  maxBufferSize: number;
  /** Minimum buffer size before any backpressure (default: 100) */
  minBufferSize: number;
}

/**
 * Default watermark configuration
 */
export const DEFAULT_WATERMARK_CONFIG: Readonly<WatermarkConfig> = {
  highWatermark: 0.9,
  lowWatermark: 0.5,
  maxBufferSize: 10000,
  minBufferSize: 100,
};

/**
 * Watermark state
 */
export type WatermarkState = 'flowing' | 'paused';

/**
 * Watermark-based backpressure controller for CDC streams
 */
export class WatermarkController {
  private readonly config: WatermarkConfig;
  private state: WatermarkState = 'flowing';
  private currentBufferSize = 0;
  private stateChangeCallbacks: Set<(state: WatermarkState, bufferUtilization: number) => void> = new Set();
  private totalPauses = 0;
  private totalResumes = 0;
  private lastStateChangeAt = Date.now();
  private totalPausedTimeMs = 0;

  constructor(config: Partial<WatermarkConfig> = {}) {
    this.config = { ...DEFAULT_WATERMARK_CONFIG, ...config };
    if (this.config.lowWatermark >= this.config.highWatermark) {
      throw new Error('Low watermark must be less than high watermark');
    }
  }

  getBufferUtilization(): number {
    return this.currentBufferSize / this.config.maxBufferSize;
  }

  getState(): WatermarkState {
    return this.state;
  }

  shouldCapture(): boolean {
    return this.state === 'flowing';
  }

  addEvents(count: number): boolean {
    this.currentBufferSize += count;
    if (this.state === 'flowing' && this.getBufferUtilization() >= this.config.highWatermark) {
      this.transitionTo('paused');
    }
    return this.state === 'flowing';
  }

  removeEvents(count: number): boolean {
    this.currentBufferSize = Math.max(0, this.currentBufferSize - count);
    if (this.state === 'paused' && this.getBufferUtilization() <= this.config.lowWatermark) {
      this.transitionTo('flowing');
      return true;
    }
    return false;
  }

  setBufferSize(size: number): void {
    const wasFlowing = this.state === 'flowing';
    this.currentBufferSize = Math.min(size, this.config.maxBufferSize);
    const utilization = this.getBufferUtilization();
    if (wasFlowing && utilization >= this.config.highWatermark) {
      this.transitionTo('paused');
    } else if (!wasFlowing && utilization <= this.config.lowWatermark) {
      this.transitionTo('flowing');
    }
  }

  onStateChange(callback: (state: WatermarkState, bufferUtilization: number) => void): () => void {
    this.stateChangeCallbacks.add(callback);
    return () => this.stateChangeCallbacks.delete(callback);
  }

  getMetrics(): {
    state: WatermarkState;
    bufferSize: number;
    bufferUtilization: number;
    totalPauses: number;
    totalResumes: number;
    totalPausedTimeMs: number;
    config: WatermarkConfig;
  } {
    const now = Date.now();
    let pausedTime = this.totalPausedTimeMs;
    if (this.state === 'paused') {
      pausedTime += now - this.lastStateChangeAt;
    }
    return {
      state: this.state,
      bufferSize: this.currentBufferSize,
      bufferUtilization: this.getBufferUtilization(),
      totalPauses: this.totalPauses,
      totalResumes: this.totalResumes,
      totalPausedTimeMs: pausedTime,
      config: { ...this.config },
    };
  }

  reset(): void {
    if (this.state === 'paused') {
      this.totalPausedTimeMs += Date.now() - this.lastStateChangeAt;
    }
    this.state = 'flowing';
    this.currentBufferSize = 0;
    this.lastStateChangeAt = Date.now();
  }

  private transitionTo(newState: WatermarkState): void {
    if (this.state === newState) return;
    const now = Date.now();
    if (this.state === 'paused') {
      this.totalPausedTimeMs += now - this.lastStateChangeAt;
    }
    this.state = newState;
    this.lastStateChangeAt = now;
    if (newState === 'paused') {
      this.totalPauses++;
    } else {
      this.totalResumes++;
    }
    const utilization = this.getBufferUtilization();
    for (const callback of this.stateChangeCallbacks) {
      try {
        callback(newState, utilization);
      } catch (error) {
        logger.error('Error in watermark state change callback', error instanceof Error ? error : new Error(String(error)), {
          component: 'WatermarkController',
          callbackType: 'stateChange',
          watermarkState: newState,
          bufferUtilization: utilization,
        });
      }
    }
  }
}

export function createWatermarkController(config: Partial<WatermarkConfig> = {}): WatermarkController {
  return new WatermarkController(config);
}

// =============================================================================
// Exactly-Once Delivery
// =============================================================================

export interface ExactlyOnceConfig {
  maxTrackedLSNs: number;
  lsnTTLMs: number;
  validateSequence: boolean;
  maxSequenceGap: number;
}

export const DEFAULT_EXACTLY_ONCE_CONFIG: Readonly<ExactlyOnceConfig> = {
  maxTrackedLSNs: 100000,
  lsnTTLMs: 3600000,
  validateSequence: true,
  maxSequenceGap: 1000,
};

interface TrackedLSN {
  lsn: LSN;
  timestamp: number;
  sequenceNumber: number;
}

export interface DeliveryResult {
  delivered: boolean;
  duplicate: boolean;
  sequenceNumber?: number;
  rejectionReason?: string;
}

export class ExactlyOnceController {
  private readonly config: ExactlyOnceConfig;
  private trackedLSNs: Map<string, TrackedLSN> = new Map();
  private highestLSN: LSN = createLSN(0n);
  private nextSequenceNumber = 1;
  private lastDeliveredSequence = 0;
  private pendingDeliveries: Map<string, { sequenceNumber: number; timestamp: number }> = new Map();
  private totalDelivered = 0;
  private totalDeduplicated = 0;
  private totalOutOfOrder = 0;

  constructor(config: Partial<ExactlyOnceConfig> = {}) {
    this.config = { ...DEFAULT_EXACTLY_ONCE_CONFIG, ...config };
  }

  checkAndTrack(lsn: LSN): DeliveryResult {
    const lsnKey = String(lsn);
    if (this.trackedLSNs.has(lsnKey)) {
      this.totalDeduplicated++;
      return { delivered: false, duplicate: true, rejectionReason: 'Duplicate LSN' };
    }
    if (this.pendingDeliveries.has(lsnKey)) {
      this.totalDeduplicated++;
      return { delivered: false, duplicate: true, rejectionReason: 'Delivery already pending' };
    }
    if (compareLSN(lsn, this.highestLSN) < 0) {
      this.totalOutOfOrder++;
    }
    const sequenceNumber = this.nextSequenceNumber++;
    if (this.config.validateSequence) {
      const gap = sequenceNumber - this.lastDeliveredSequence - 1;
      if (gap > this.config.maxSequenceGap) {
        return { delivered: false, duplicate: false, rejectionReason: `Sequence gap too large: ${gap}` };
      }
    }
    const now = Date.now();
    this.trackedLSNs.set(lsnKey, { lsn, timestamp: now, sequenceNumber });
    this.pendingDeliveries.set(lsnKey, { sequenceNumber, timestamp: now });
    if (compareLSN(lsn, this.highestLSN) > 0) {
      this.highestLSN = lsn;
    }
    this.lastDeliveredSequence = sequenceNumber;
    this.totalDelivered++;
    this.cleanupIfNeeded();
    return { delivered: true, duplicate: false, sequenceNumber };
  }

  isDuplicate(lsn: LSN): boolean {
    const lsnKey = String(lsn);
    return this.trackedLSNs.has(lsnKey) || this.pendingDeliveries.has(lsnKey);
  }

  acknowledge(lsn: LSN): void {
    this.pendingDeliveries.delete(String(lsn));
  }

  acknowledgeUpTo(upToLSN: LSN): number {
    let acknowledged = 0;
    for (const [lsnKey] of this.pendingDeliveries) {
      const lsn = createLSN(BigInt(lsnKey));
      if (compareLSN(lsn, upToLSN) <= 0) {
        this.pendingDeliveries.delete(lsnKey);
        acknowledged++;
      }
    }
    return acknowledged;
  }

  getHighestAcknowledgedLSN(): LSN {
    let highest = createLSN(0n);
    for (const [lsnKey] of this.trackedLSNs) {
      const lsn = createLSN(BigInt(lsnKey));
      if (!this.pendingDeliveries.has(lsnKey) && compareLSN(lsn, highest) > 0) {
        highest = lsn;
      }
    }
    return highest;
  }

  getPendingCount(): number {
    return this.pendingDeliveries.size;
  }

  getMetrics(): {
    totalDelivered: number;
    totalDeduplicated: number;
    totalOutOfOrder: number;
    pendingCount: number;
    trackedLSNCount: number;
    highestLSN: LSN;
    nextSequenceNumber: number;
  } {
    return {
      totalDelivered: this.totalDelivered,
      totalDeduplicated: this.totalDeduplicated,
      totalOutOfOrder: this.totalOutOfOrder,
      pendingCount: this.pendingDeliveries.size,
      trackedLSNCount: this.trackedLSNs.size,
      highestLSN: this.highestLSN,
      nextSequenceNumber: this.nextSequenceNumber,
    };
  }

  reset(fromLSN?: LSN, fromSequence?: number): void {
    this.trackedLSNs.clear();
    this.pendingDeliveries.clear();
    this.highestLSN = fromLSN ?? createLSN(0n);
    this.nextSequenceNumber = fromSequence ?? 1;
    this.lastDeliveredSequence = (fromSequence ?? 1) - 1;
  }

  exportState(): { highestLSN: string; nextSequenceNumber: number; pendingLSNs: string[] } {
    return {
      highestLSN: String(this.highestLSN),
      nextSequenceNumber: this.nextSequenceNumber,
      pendingLSNs: Array.from(this.pendingDeliveries.keys()),
    };
  }

  importState(state: { highestLSN: string; nextSequenceNumber: number; pendingLSNs: string[] }): void {
    this.highestLSN = createLSN(BigInt(state.highestLSN));
    this.nextSequenceNumber = state.nextSequenceNumber;
    this.lastDeliveredSequence = state.nextSequenceNumber - 1;
    const now = Date.now();
    for (const lsnKey of state.pendingLSNs) {
      this.pendingDeliveries.set(lsnKey, { sequenceNumber: 0, timestamp: now });
    }
  }

  private cleanupIfNeeded(): void {
    if (this.trackedLSNs.size <= this.config.maxTrackedLSNs) return;
    const now = Date.now();
    const expiredCutoff = now - this.config.lsnTTLMs;
    for (const [lsnKey, tracked] of this.trackedLSNs) {
      if (tracked.timestamp < expiredCutoff && !this.pendingDeliveries.has(lsnKey)) {
        this.trackedLSNs.delete(lsnKey);
      }
    }
    if (this.trackedLSNs.size > this.config.maxTrackedLSNs) {
      const sorted = Array.from(this.trackedLSNs.entries())
        .filter(([key]) => !this.pendingDeliveries.has(key))
        .sort((a, b) => a[1].timestamp - b[1].timestamp);
      const toRemove = sorted.slice(0, sorted.length - this.config.maxTrackedLSNs);
      for (const [key] of toRemove) {
        this.trackedLSNs.delete(key);
      }
    }
  }
}

export function createExactlyOnceController(config: Partial<ExactlyOnceConfig> = {}): ExactlyOnceController {
  return new ExactlyOnceController(config);
}

// =============================================================================
// Consumer Checkpoint Manager
// =============================================================================

export interface ConsumerCheckpoint {
  consumerId: string;
  acknowledgedLSN: LSN;
  acknowledgedSequence: number;
  checkpointedAt: number;
  pendingLSNs: string[];
  metadata?: Record<string, unknown>;
}

export interface CheckpointStorage {
  save(checkpoint: ConsumerCheckpoint): Promise<void>;
  load(consumerId: string): Promise<ConsumerCheckpoint | null>;
  delete(consumerId: string): Promise<void>;
  list(): Promise<string[]>;
}

export class InMemoryCheckpointStorage implements CheckpointStorage {
  private checkpoints: Map<string, ConsumerCheckpoint> = new Map();

  async save(checkpoint: ConsumerCheckpoint): Promise<void> {
    this.checkpoints.set(checkpoint.consumerId, { ...checkpoint });
  }

  async load(consumerId: string): Promise<ConsumerCheckpoint | null> {
    const checkpoint = this.checkpoints.get(consumerId);
    return checkpoint ? { ...checkpoint } : null;
  }

  async delete(consumerId: string): Promise<void> {
    this.checkpoints.delete(consumerId);
  }

  async list(): Promise<string[]> {
    return Array.from(this.checkpoints.keys());
  }
}

export class ConsumerCheckpointManager {
  private readonly storage: CheckpointStorage;
  private readonly checkpointInterval: number;
  private lastCheckpointAt: Map<string, number> = new Map();

  constructor(storage: CheckpointStorage, checkpointIntervalMs = 5000) {
    this.storage = storage;
    this.checkpointInterval = checkpointIntervalMs;
  }

  async checkpoint(
    consumerId: string,
    acknowledgedLSN: LSN,
    acknowledgedSequence: number,
    pendingLSNs: string[] = [],
    metadata?: Record<string, unknown>
  ): Promise<void> {
    const checkpoint: ConsumerCheckpoint = {
      consumerId,
      acknowledgedLSN,
      acknowledgedSequence,
      checkpointedAt: Date.now(),
      pendingLSNs,
      metadata,
    };
    await this.storage.save(checkpoint);
    this.lastCheckpointAt.set(consumerId, checkpoint.checkpointedAt);
  }

  shouldCheckpoint(consumerId: string): boolean {
    const lastCheckpoint = this.lastCheckpointAt.get(consumerId);
    if (!lastCheckpoint) return true;
    return Date.now() - lastCheckpoint >= this.checkpointInterval;
  }

  async recover(consumerId: string): Promise<ConsumerCheckpoint | null> {
    return this.storage.load(consumerId);
  }

  async deleteCheckpoint(consumerId: string): Promise<void> {
    await this.storage.delete(consumerId);
    this.lastCheckpointAt.delete(consumerId);
  }

  async listConsumers(): Promise<string[]> {
    return this.storage.list();
  }
}

export function createCheckpointManager(
  storage: CheckpointStorage,
  checkpointIntervalMs?: number
): ConsumerCheckpointManager {
  return new ConsumerCheckpointManager(storage, checkpointIntervalMs);
}

// =============================================================================
// Integrated Backpressure Stream Controller
// =============================================================================

export interface BackpressureStreamConfig {
  backpressure: Partial<BackpressureConfig>;
  watermark: Partial<WatermarkConfig>;
  exactlyOnce: Partial<ExactlyOnceConfig>;
  checkpointIntervalMs: number;
  consumerId: string;
}

export const DEFAULT_STREAM_CONFIG: Readonly<BackpressureStreamConfig> = {
  backpressure: {},
  watermark: {},
  exactlyOnce: {},
  checkpointIntervalMs: 5000,
  consumerId: '',
};

export class BackpressureStreamController {
  private readonly config: BackpressureStreamConfig;
  private readonly backpressure: BackpressureController;
  private readonly watermark: WatermarkController;
  private readonly exactlyOnce: ExactlyOnceController;
  private readonly checkpointManager: ConsumerCheckpointManager;
  private resumePromise: Promise<void> | null = null;
  private resumeResolve: (() => void) | null = null;

  constructor(config: Partial<BackpressureStreamConfig>, checkpointStorage: CheckpointStorage) {
    this.config = { ...DEFAULT_STREAM_CONFIG, ...config };
    this.backpressure = new BackpressureController(this.config.backpressure);
    this.watermark = new WatermarkController(this.config.watermark);
    this.exactlyOnce = new ExactlyOnceController(this.config.exactlyOnce);
    this.checkpointManager = new ConsumerCheckpointManager(
      checkpointStorage,
      this.config.checkpointIntervalMs
    );
    this.watermark.onStateChange((state, _utilization) => {
      if (state === 'paused') {
        this.backpressure.handleSignal({
          type: 'pause',
          bufferUtilization: this.watermark.getBufferUtilization(),
          reason: 'High watermark reached',
        });
      } else {
        this.backpressure.handleSignal({
          type: 'resume',
          bufferUtilization: this.watermark.getBufferUtilization(),
          reason: 'Low watermark reached',
        });
        this.notifyResume();
      }
    });
  }

  shouldCapture(): boolean {
    return this.watermark.shouldCapture() && this.backpressure.shouldProceed();
  }

  async waitForResume(): Promise<void> {
    if (this.shouldCapture()) return;
    if (!this.resumePromise) {
      this.resumePromise = new Promise<void>((resolve) => {
        this.resumeResolve = resolve;
      });
    }
    await this.resumePromise;
  }

  deliver(lsn: LSN): DeliveryResult {
    const result = this.exactlyOnce.checkAndTrack(lsn);
    if (result.delivered) {
      this.watermark.addEvents(1);
    }
    return result;
  }

  isDuplicate(lsn: LSN): boolean {
    return this.exactlyOnce.isDuplicate(lsn);
  }

  acknowledge(lsn: LSN): void {
    this.exactlyOnce.acknowledge(lsn);
    this.watermark.removeEvents(1);
  }

  acknowledgeUpTo(upToLSN: LSN): number {
    const count = this.exactlyOnce.acknowledgeUpTo(upToLSN);
    this.watermark.removeEvents(count);
    return count;
  }

  handleBackpressure(signal: BackpressureSignal): void {
    this.backpressure.handleSignal(signal);
  }

  async maybeCheckpoint(): Promise<boolean> {
    if (!this.config.consumerId) return false;
    if (!this.checkpointManager.shouldCheckpoint(this.config.consumerId)) return false;
    await this.checkpoint();
    return true;
  }

  async checkpoint(): Promise<void> {
    if (!this.config.consumerId) return;
    const state = this.exactlyOnce.exportState();
    await this.checkpointManager.checkpoint(
      this.config.consumerId,
      this.exactlyOnce.getHighestAcknowledgedLSN(),
      state.nextSequenceNumber - 1,
      state.pendingLSNs
    );
  }

  async recover(): Promise<ConsumerCheckpoint | null> {
    if (!this.config.consumerId) return null;
    const checkpoint = await this.checkpointManager.recover(this.config.consumerId);
    if (checkpoint) {
      this.exactlyOnce.importState({
        highestLSN: String(checkpoint.acknowledgedLSN),
        nextSequenceNumber: checkpoint.acknowledgedSequence + 1,
        pendingLSNs: checkpoint.pendingLSNs,
      });
    }
    return checkpoint;
  }

  getMetrics(): {
    backpressure: BackpressureMetrics;
    watermark: ReturnType<WatermarkController['getMetrics']>;
    exactlyOnce: ReturnType<ExactlyOnceController['getMetrics']>;
    shouldCapture: boolean;
  } {
    return {
      backpressure: this.backpressure.getMetrics(),
      watermark: this.watermark.getMetrics(),
      exactlyOnce: this.exactlyOnce.getMetrics(),
      shouldCapture: this.shouldCapture(),
    };
  }

  reset(): void {
    this.backpressure.reset();
    this.watermark.reset();
    this.exactlyOnce.reset();
    this.notifyResume();
  }

  private notifyResume(): void {
    if (this.resumeResolve) {
      this.resumeResolve();
      this.resumePromise = null;
      this.resumeResolve = null;
    }
  }
}

export function createBackpressureStreamController(
  config: Partial<BackpressureStreamConfig>,
  checkpointStorage: CheckpointStorage
): BackpressureStreamController {
  return new BackpressureStreamController(config, checkpointStorage);
}
