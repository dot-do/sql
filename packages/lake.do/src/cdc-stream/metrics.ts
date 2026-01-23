/**
 * lake.do - Queue Metrics Tracking
 *
 * This module provides metrics collection and water mark event handling
 * for queue monitoring.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Metrics about queue depth and throughput.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface QueueMetrics {
  /** Current number of items in the queue */
  currentDepth: number;
  /** Maximum configured queue size */
  maxDepth: number;
  /** Highest queue depth seen since creation or last reset */
  peakDepth: number;
  /** Number of items dropped due to backpressure */
  droppedCount: number;
  /** Total number of items pushed */
  totalPushed: number;
  /** Total number of items consumed */
  totalConsumed: number;
  /** Queue utilization as a percentage (0-100) */
  utilizationPercent: number;
}

/**
 * Water mark event payload.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface WaterMarkEvent {
  /** Current queue depth */
  currentDepth: number;
  /** Maximum queue size */
  maxDepth: number;
  /** Queue utilization as a percentage (0-100) */
  utilizationPercent: number;
}

/**
 * Configuration options for MetricsTracker.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface MetricsTrackerOptions {
  /** Maximum queue size for utilization calculation */
  maxDepth: number;
  /** High water mark threshold percentage (0-100) for emitting event */
  highWaterMark?: number;
  /** Low water mark threshold percentage (0-100) for emitting event */
  lowWaterMark?: number;
  /** Callback when queue reaches high water mark */
  onHighWaterMark?: (event: WaterMarkEvent) => void;
  /** Callback when queue falls below low water mark */
  onLowWaterMark?: (event: WaterMarkEvent) => void;
}

// =============================================================================
// MetricsTracker Implementation
// =============================================================================

/**
 * Tracks queue metrics and handles water mark events.
 *
 * @description Provides metrics collection for monitoring queue performance
 * and health. Supports high/low water mark thresholds for proactive alerting.
 *
 * @example
 * ```typescript
 * const tracker = new MetricsTracker({
 *   maxDepth: 100,
 *   highWaterMark: 80,
 *   lowWaterMark: 20,
 *   onHighWaterMark: (e) => console.warn('Queue filling up:', e),
 *   onLowWaterMark: (e) => console.log('Queue recovered:', e),
 * });
 *
 * // Track events
 * tracker.recordPush(currentQueueSize);
 * tracker.recordConsume(currentQueueSize);
 * tracker.recordDrop();
 *
 * // Get metrics
 * const metrics = tracker.getMetrics(currentQueueSize, droppedCount);
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class MetricsTracker {
  private _maxDepth: number;
  private _peakDepth = 0;
  private _totalPushed = 0;
  private _totalConsumed = 0;
  private _wasAboveHighWaterMark = false;

  private readonly _highWaterMark: number | undefined;
  private readonly _lowWaterMark: number | undefined;
  private readonly _onHighWaterMark: ((event: WaterMarkEvent) => void) | undefined;
  private readonly _onLowWaterMark: ((event: WaterMarkEvent) => void) | undefined;

  /**
   * Creates a new MetricsTracker.
   *
   * @param options - Configuration options
   */
  constructor(options: MetricsTrackerOptions) {
    this._maxDepth = options.maxDepth;
    this._highWaterMark = options.highWaterMark;
    this._lowWaterMark = options.lowWaterMark;
    this._onHighWaterMark = options.onHighWaterMark;
    this._onLowWaterMark = options.onLowWaterMark;
  }

  /**
   * Gets the current max depth for utilization calculation.
   */
  get maxDepth(): number {
    return this._maxDepth;
  }

  /**
   * Updates the max depth for utilization calculation.
   */
  set maxDepth(value: number) {
    this._maxDepth = value;
  }

  /**
   * Gets the peak depth seen since creation or last reset.
   */
  get peakDepth(): number {
    return this._peakDepth;
  }

  /**
   * Gets the total number of items pushed.
   */
  get totalPushed(): number {
    return this._totalPushed;
  }

  /**
   * Gets the total number of items consumed.
   */
  get totalConsumed(): number {
    return this._totalConsumed;
  }

  /**
   * Records a successful push operation.
   *
   * @param currentDepth - Current queue depth after the push
   */
  recordPush(currentDepth: number): void {
    this._totalPushed++;
    this.updatePeakDepth(currentDepth);
    this.checkWaterMarks(currentDepth);
  }

  /**
   * Records a consume operation.
   *
   * @param currentDepth - Current queue depth after the consume
   */
  recordConsume(currentDepth: number): void {
    this._totalConsumed++;
    this.checkWaterMarks(currentDepth);
  }

  /**
   * Records a dropped push (decrements total pushed count).
   */
  recordDroppedPush(): void {
    // Don't count dropped items in totalPushed
  }

  /**
   * Checks for water mark threshold events.
   *
   * @param currentDepth - Current queue depth
   */
  checkWaterMarks(currentDepth: number): void {
    const utilizationPercent = this.calculateUtilization(currentDepth);

    // Check high water mark
    if (this._highWaterMark !== undefined) {
      if (utilizationPercent >= this._highWaterMark && !this._wasAboveHighWaterMark) {
        this._wasAboveHighWaterMark = true;
        if (this._onHighWaterMark) {
          this._onHighWaterMark({
            currentDepth,
            maxDepth: this._maxDepth,
            utilizationPercent,
          });
        }
      }
    }

    // Check low water mark
    if (this._lowWaterMark !== undefined) {
      if (utilizationPercent <= this._lowWaterMark && this._wasAboveHighWaterMark) {
        this._wasAboveHighWaterMark = false;
        if (this._onLowWaterMark) {
          this._onLowWaterMark({
            currentDepth,
            maxDepth: this._maxDepth,
            utilizationPercent,
          });
        }
      }
    }
  }

  /**
   * Updates peak depth if current depth exceeds it.
   *
   * @param currentDepth - Current queue depth
   */
  private updatePeakDepth(currentDepth: number): void {
    if (currentDepth > this._peakDepth) {
      this._peakDepth = currentDepth;
    }
  }

  /**
   * Calculates utilization percentage.
   *
   * @param currentDepth - Current queue depth
   * @returns Utilization as a percentage (0-100)
   */
  calculateUtilization(currentDepth: number): number {
    return Math.round((currentDepth / this._maxDepth) * 100);
  }

  /**
   * Gets the current metrics.
   *
   * @param currentDepth - Current queue depth
   * @param droppedCount - Number of dropped items
   * @returns Current queue metrics
   */
  getMetrics(currentDepth: number, droppedCount: number): QueueMetrics {
    return {
      currentDepth,
      maxDepth: this._maxDepth,
      peakDepth: this._peakDepth,
      droppedCount,
      totalPushed: this._totalPushed,
      totalConsumed: this._totalConsumed,
      utilizationPercent: this.calculateUtilization(currentDepth),
    };
  }

  /**
   * Resets metrics counters.
   *
   * @param currentDepth - Current queue depth (becomes new peak)
   */
  reset(currentDepth: number): void {
    this._peakDepth = currentDepth;
    this._totalPushed = 0;
    this._totalConsumed = 0;
  }
}
