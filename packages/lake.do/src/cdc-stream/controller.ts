/**
 * lake.do - CDC Stream Controller
 *
 * This module provides the main CDCStreamController that coordinates
 * queue management, metrics tracking, and async iteration.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

import type { CDCBatch } from '../types.js';
import { BoundedQueue, type BackpressureStrategy } from './queue.js';
import { MetricsTracker, type QueueMetrics, type WaterMarkEvent } from './metrics.js';

// =============================================================================
// Re-exports for convenience
// =============================================================================

export type { BackpressureStrategy } from './queue.js';
export type { QueueMetrics as CDCStreamMetrics, WaterMarkEvent } from './metrics.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration options for CDCStreamController.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface CDCStreamControllerOptions {
  /** Maximum number of batches to buffer (default: 1000, minimum: 1) */
  maxQueueSize?: number;
  /** Strategy when queue is full (default: 'block') */
  backpressureStrategy?: BackpressureStrategy;
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
// CDCStreamController Implementation
// =============================================================================

/**
 * Controller for CDC stream iteration with bounded queue and backpressure support.
 *
 * @description Manages CDC event batches with configurable queue bounds,
 * backpressure strategies, and metrics for monitoring. The queue prevents
 * unbounded memory growth under high throughput scenarios.
 *
 * This implementation separates concerns into:
 * - Queue management (BoundedQueue)
 * - Metrics tracking (MetricsTracker)
 * - Async iteration coordination (this class)
 *
 * @example
 * ```typescript
 * const controller = new CDCStreamController({
 *   maxQueueSize: 100,
 *   backpressureStrategy: 'drop-oldest',
 *   highWaterMark: 80,
 *   onHighWaterMark: (event) => console.warn('Queue filling up:', event),
 * });
 *
 * // Check if push succeeded
 * if (!controller.push(batch)) {
 *   console.warn('Queue full, batch rejected');
 * }
 *
 * // Or wait for space
 * await controller.pushAsync(batch);
 *
 * // Monitor metrics
 * const metrics = controller.getMetrics();
 * console.log(`Queue utilization: ${metrics.utilizationPercent}%`);
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class CDCStreamController implements AsyncIterable<CDCBatch> {
  private readonly queue: BoundedQueue<CDCBatch>;
  private readonly metrics: MetricsTracker;
  private resolvers: Array<(value: IteratorResult<CDCBatch>) => void> = [];
  private closed = false;

  /**
   * Creates a new CDCStreamController instance.
   *
   * @param options - Configuration options
   * @throws {Error} When maxQueueSize is less than 1
   */
  constructor(options: CDCStreamControllerOptions = {}) {
    const maxQueueSize = options.maxQueueSize ?? 1000;

    // Validate maxQueueSize early with the expected error message
    if (maxQueueSize < 1) {
      throw new Error('maxQueueSize must be at least 1');
    }

    this.queue = new BoundedQueue<CDCBatch>({
      maxSize: maxQueueSize,
      backpressureStrategy: options.backpressureStrategy ?? 'block',
    });

    this.metrics = new MetricsTracker({
      maxDepth: maxQueueSize,
      highWaterMark: options.highWaterMark,
      lowWaterMark: options.lowWaterMark,
      onHighWaterMark: options.onHighWaterMark,
      onLowWaterMark: options.onLowWaterMark,
    });
  }

  // ===========================================================================
  // Public Properties
  // ===========================================================================

  /**
   * Maximum number of batches that can be buffered.
   */
  get maxQueueSize(): number {
    return this.queue.maxSize;
  }

  /**
   * Current number of batches in the queue.
   */
  get queueSize(): number {
    return this.queue.size;
  }

  /**
   * Number of batches dropped due to backpressure.
   */
  get droppedCount(): number {
    return this.queue.droppedCount;
  }

  /**
   * Current backpressure strategy.
   */
  get backpressureStrategy(): BackpressureStrategy {
    return this.queue.backpressureStrategy;
  }

  // ===========================================================================
  // Queue Size Management
  // ===========================================================================

  /**
   * Sets a new maximum queue size.
   *
   * @description If the new size is smaller than the current queue size,
   * excess batches are dropped according to the backpressure strategy.
   *
   * @param size - New maximum queue size (must be at least 1)
   * @throws {Error} When size is less than 1
   */
  setMaxQueueSize(size: number): void {
    this.queue.setMaxSize(size);
    this.metrics.maxDepth = size;
    this.metrics.checkWaterMarks(this.queue.size);
  }

  // ===========================================================================
  // Push Operations
  // ===========================================================================

  /**
   * Pushes a batch to the queue.
   *
   * @description Behavior depends on the backpressure strategy:
   * - `block`: Returns false when queue is full
   * - `drop-oldest`: Drops oldest batch, always returns true
   * - `drop-newest`: Returns false when full without modifying queue
   *
   * @param batch - The CDC batch to push
   * @returns true if the batch was accepted, false if rejected
   */
  push(batch: CDCBatch): boolean {
    if (this.closed) return false;

    // If there's a waiting consumer, deliver directly
    const resolver = this.resolvers.shift();
    if (resolver) {
      this.metrics.recordPush(this.queue.size);
      resolver({ value: batch, done: false });
      return true;
    }

    // Try to push to queue
    const result = this.queue.push(batch);

    if (result.accepted) {
      this.metrics.recordPush(this.queue.size);
    } else {
      this.metrics.recordDroppedPush();
    }

    this.metrics.checkWaterMarks(this.queue.size);
    return result.accepted;
  }

  /**
   * Pushes a batch to the queue, waiting for space if necessary.
   *
   * @description When the queue is full, this method waits until a batch
   * is consumed before adding the new batch. Only applies to 'block' strategy;
   * other strategies behave the same as push().
   *
   * @param batch - The CDC batch to push
   * @returns Promise that resolves to true when the batch is accepted
   */
  async pushAsync(batch: CDCBatch): Promise<boolean> {
    if (this.closed) return false;

    // If queue has space or not blocking, push immediately
    if (!this.queue.isFull || this.queue.backpressureStrategy !== 'block') {
      return this.push(batch);
    }

    // Wait for space via the queue's async mechanism
    await this.queue.pushAsync(batch);
    return true;
  }

  // ===========================================================================
  // Metrics
  // ===========================================================================

  /**
   * Returns current queue metrics.
   *
   * @returns Object containing queue depth statistics
   */
  getMetrics(): QueueMetrics {
    return this.metrics.getMetrics(this.queue.size, this.queue.droppedCount);
  }

  /**
   * Resets metrics counters while preserving the queue contents.
   *
   * @description Resets peakDepth, totalPushed, totalConsumed, and droppedCount.
   * The current queue depth becomes the new peak.
   */
  resetMetrics(): void {
    this.metrics.reset(this.queue.size);
    this.queue.resetDroppedCount();
  }

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  /**
   * Closes the controller, ending the async iteration.
   */
  close(): void {
    this.closed = true;

    // Resolve all waiting consumers with done
    // Using IteratorReturnResult pattern for type safety
    const doneResult: IteratorResult<CDCBatch> = { value: undefined, done: true } as IteratorReturnResult<undefined>;
    for (const resolver of this.resolvers) {
      resolver(doneResult);
    }
    this.resolvers = [];

    // Cancel any waiting async pushes
    this.queue.cancelAsyncPushes();
  }

  // ===========================================================================
  // Async Iterator Implementation
  // ===========================================================================

  [Symbol.asyncIterator](): AsyncIterator<CDCBatch> {
    // Pre-create the done result for reuse
    const doneResult: IteratorResult<CDCBatch> = { value: undefined, done: true } as IteratorReturnResult<undefined>;

    return {
      next: (): Promise<IteratorResult<CDCBatch>> => {
        if (this.closed && this.queue.isEmpty) {
          return Promise.resolve(doneResult);
        }

        const batch = this.queue.shift();
        if (batch) {
          this.metrics.recordConsume(this.queue.size);
          this.metrics.checkWaterMarks(this.queue.size);
          return Promise.resolve({ value: batch, done: false });
        }

        return new Promise((resolve) => {
          this.resolvers.push((result) => {
            if (!result.done) {
              this.metrics.recordConsume(this.queue.size);
              this.metrics.checkWaterMarks(this.queue.size);
            }
            resolve(result);
          });
        });
      },
    };
  }
}
