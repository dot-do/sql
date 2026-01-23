/**
 * lake.do - Bounded Queue Implementation
 *
 * This module provides a bounded queue with configurable backpressure strategies.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Backpressure strategy for handling queue overflow.
 *
 * @description Controls how the queue handles new items when full:
 * - `block`: push() returns false, pushAsync() waits for space
 * - `drop-oldest`: Removes oldest item to make room for new one
 * - `drop-newest`: Rejects new item without modifying queue
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type BackpressureStrategy = 'block' | 'drop-oldest' | 'drop-newest';

/**
 * Configuration options for BoundedQueue.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface BoundedQueueOptions {
  /** Maximum number of items to buffer (default: 1000, minimum: 1) */
  maxSize?: number;
  /** Strategy when queue is full (default: 'block') */
  backpressureStrategy?: BackpressureStrategy;
}

/**
 * Result of a push operation.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface PushResult {
  /** Whether the item was accepted into the queue */
  accepted: boolean;
  /** Whether an item was dropped (for drop-* strategies) */
  dropped: boolean;
}

// =============================================================================
// BoundedQueue Implementation
// =============================================================================

/**
 * A bounded queue with configurable backpressure strategies.
 *
 * @description Manages a fixed-size queue that prevents unbounded memory growth.
 * When the queue is full, behavior depends on the configured backpressure strategy.
 *
 * @typeParam T - The type of items stored in the queue
 *
 * @example
 * ```typescript
 * const queue = new BoundedQueue<string>({
 *   maxSize: 100,
 *   backpressureStrategy: 'drop-oldest',
 * });
 *
 * queue.push('item1');
 * const item = queue.shift(); // 'item1'
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class BoundedQueue<T> {
  private readonly items: T[] = [];
  private _maxSize: number;
  private readonly _backpressureStrategy: BackpressureStrategy;
  private _droppedCount = 0;
  private asyncPushResolvers: Array<() => void> = [];

  /**
   * Creates a new BoundedQueue.
   *
   * @param options - Configuration options
   * @throws {Error} When maxSize is less than 1
   */
  constructor(options: BoundedQueueOptions = {}) {
    const maxSize = options.maxSize ?? 1000;
    if (maxSize < 1) {
      throw new Error('maxSize must be at least 1');
    }
    this._maxSize = maxSize;
    this._backpressureStrategy = options.backpressureStrategy ?? 'block';
  }

  /**
   * Maximum number of items that can be buffered.
   */
  get maxSize(): number {
    return this._maxSize;
  }

  /**
   * Current number of items in the queue.
   */
  get size(): number {
    return this.items.length;
  }

  /**
   * Number of items dropped due to backpressure.
   */
  get droppedCount(): number {
    return this._droppedCount;
  }

  /**
   * Current backpressure strategy.
   */
  get backpressureStrategy(): BackpressureStrategy {
    return this._backpressureStrategy;
  }

  /**
   * Whether the queue is full.
   */
  get isFull(): boolean {
    return this.items.length >= this._maxSize;
  }

  /**
   * Whether the queue is empty.
   */
  get isEmpty(): boolean {
    return this.items.length === 0;
  }

  /**
   * Sets a new maximum queue size.
   *
   * @description If the new size is smaller than the current queue size,
   * excess items are dropped according to the backpressure strategy.
   *
   * @param size - New maximum queue size (must be at least 1)
   * @returns Number of items dropped
   * @throws {Error} When size is less than 1
   */
  setMaxSize(size: number): number {
    if (size < 1) {
      throw new Error('maxSize must be at least 1');
    }
    this._maxSize = size;

    let dropped = 0;
    while (this.items.length > this._maxSize) {
      if (this._backpressureStrategy === 'drop-oldest') {
        this.items.shift();
      } else {
        this.items.pop();
      }
      dropped++;
      this._droppedCount++;
    }

    return dropped;
  }

  /**
   * Pushes an item to the queue.
   *
   * @description Behavior depends on the backpressure strategy:
   * - `block`: Returns false when queue is full
   * - `drop-oldest`: Drops oldest item, always returns true
   * - `drop-newest`: Returns false when full without modifying queue
   *
   * @param item - The item to push
   * @returns Push result indicating acceptance and drop status
   */
  push(item: T): PushResult {
    if (this.items.length >= this._maxSize) {
      switch (this._backpressureStrategy) {
        case 'block':
        case 'drop-newest':
          this._droppedCount++;
          return { accepted: false, dropped: true };

        case 'drop-oldest':
          this.items.shift();
          this._droppedCount++;
          this.items.push(item);
          return { accepted: true, dropped: true };
      }
    }

    this.items.push(item);
    return { accepted: true, dropped: false };
  }

  /**
   * Pushes an item to the queue, waiting for space if necessary.
   *
   * @description When the queue is full, this method waits until an item
   * is consumed before adding the new item. Only applies to 'block' strategy;
   * other strategies behave the same as push().
   *
   * @param item - The item to push
   * @returns Promise that resolves to push result
   */
  async pushAsync(item: T): Promise<PushResult> {
    if (this.items.length < this._maxSize || this._backpressureStrategy !== 'block') {
      return this.push(item);
    }

    // Wait for space to become available
    await new Promise<void>((resolve) => {
      this.asyncPushResolvers.push(resolve);
    });

    return this.push(item);
  }

  /**
   * Removes and returns the first item from the queue.
   *
   * @returns The first item, or undefined if the queue is empty
   */
  shift(): T | undefined {
    const item = this.items.shift();
    if (item !== undefined) {
      this.notifyAsyncPushWaiters();
    }
    return item;
  }

  /**
   * Returns the first item without removing it.
   *
   * @returns The first item, or undefined if the queue is empty
   */
  peek(): T | undefined {
    return this.items[0];
  }

  /**
   * Clears all items from the queue.
   *
   * @returns The number of items cleared
   */
  clear(): number {
    const count = this.items.length;
    this.items.length = 0;
    return count;
  }

  /**
   * Resets the dropped count.
   */
  resetDroppedCount(): void {
    this._droppedCount = 0;
  }

  /**
   * Notifies waiting async push operations that space is available.
   */
  private notifyAsyncPushWaiters(): void {
    const waiter = this.asyncPushResolvers.shift();
    if (waiter) {
      waiter();
    }
  }

  /**
   * Cancels all waiting async push operations.
   */
  cancelAsyncPushes(): void {
    for (const waiter of this.asyncPushResolvers) {
      waiter();
    }
    this.asyncPushResolvers = [];
  }
}
