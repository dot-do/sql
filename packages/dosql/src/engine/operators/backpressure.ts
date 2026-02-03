/**
 * Backpressure Support for Engine Operators
 *
 * Provides streaming backpressure mechanism for large result sets.
 * Implements:
 * - Pause/resume for downstream flow control
 * - Memory-based watermarks for automatic backpressure
 * - Backpressure controller for pipeline coordination
 */

import {
  type BackpressureState,
  type BackpressureStats,
  type BackpressureSupport,
  type BackpressureController,
  type BackpressureOperator,
  type WatermarkConfig,
  type Operator,
  type Row,
  type ExecutionContext,
  DEFAULT_WATERMARKS,
} from '../types.js';

// =============================================================================
// BACKPRESSURE CONTROLLER IMPLEMENTATION
// =============================================================================

/**
 * Default backpressure controller implementation
 * Coordinates backpressure across a pipeline of operators
 */
export class DefaultBackpressureController implements BackpressureController {
  private watermarks: WatermarkConfig;
  private memoryUsage: Map<Operator, number> = new Map();
  private pauseRequests: Set<Operator> = new Set();
  private listeners: Array<(paused: boolean) => void> = [];

  constructor(watermarks: WatermarkConfig = DEFAULT_WATERMARKS) {
    this.watermarks = watermarks;
  }

  requestPause(source: Operator): void {
    this.pauseRequests.add(source);
    this.notifyListeners(true);
  }

  requestResume(source: Operator): void {
    this.pauseRequests.delete(source);
    if (this.pauseRequests.size === 0 && !this.shouldPause()) {
      this.notifyListeners(false);
    }
  }

  shouldPause(): boolean {
    return this.getTotalMemoryUsage() >= this.watermarks.highWatermark || this.pauseRequests.size > 0;
  }

  shouldResume(): boolean {
    return this.getTotalMemoryUsage() <= this.watermarks.lowWatermark && this.pauseRequests.size === 0;
  }

  reportMemoryUsage(source: Operator, bytes: number): void {
    this.memoryUsage.set(source, bytes);
  }

  getTotalMemoryUsage(): number {
    let total = 0;
    for (const usage of this.memoryUsage.values()) {
      total += usage;
    }
    return total;
  }

  getWatermarks(): WatermarkConfig {
    return { ...this.watermarks };
  }

  setWatermarks(config: WatermarkConfig): void {
    this.watermarks = { ...config };
  }

  onPauseChange(callback: (paused: boolean) => void): void {
    this.listeners.push(callback);
  }

  private notifyListeners(paused: boolean): void {
    for (const listener of this.listeners) {
      listener(paused);
    }
  }

  /**
   * Clear all state (for cleanup)
   */
  clear(): void {
    this.memoryUsage.clear();
    this.pauseRequests.clear();
    this.listeners = [];
  }
}

// =============================================================================
// BACKPRESSURE MIXIN
// =============================================================================

/**
 * Base class that provides backpressure support for operators.
 * Operators can extend this class to gain backpressure capabilities.
 */
export abstract class BackpressureOperatorBase implements BackpressureOperator {
  protected backpressureState: BackpressureState = 'flowing';
  protected watermarks: WatermarkConfig = DEFAULT_WATERMARKS;
  protected pauseCount = 0;
  protected resumeCount = 0;
  protected memoryUsage = 0;
  protected bufferedRows = 0;
  protected backpressureCallbacks: Array<(state: BackpressureState) => void> = [];
  protected pausePromise: Promise<void> | null = null;
  protected pauseResolve: (() => void) | null = null;
  protected controller: BackpressureController | null = null;

  // Abstract methods that subclasses must implement
  abstract open(ctx: ExecutionContext): Promise<void>;
  abstract next(): Promise<Row | null>;
  abstract close(): Promise<void>;
  abstract columns(): string[];
  abstract [Symbol.asyncIterator](): AsyncIterator<Row>;

  /**
   * Set the backpressure controller for this operator
   */
  setController(controller: BackpressureController): void {
    this.controller = controller;
  }

  async pause(): Promise<void> {
    if (this.backpressureState === 'paused') {
      return;
    }

    this.backpressureState = 'paused';
    this.pauseCount++;
    this.notifyBackpressureChange();

    // Create a promise that will be resolved when resume() is called
    this.pausePromise = new Promise<void>((resolve) => {
      this.pauseResolve = resolve;
    });

    if (this.controller) {
      this.controller.requestPause(this);
    }
  }

  async resume(): Promise<void> {
    if (this.backpressureState !== 'paused') {
      return;
    }

    this.backpressureState = 'flowing';
    this.resumeCount++;
    this.notifyBackpressureChange();

    // Resolve the pause promise to unblock waiting code
    if (this.pauseResolve) {
      this.pauseResolve();
      this.pausePromise = null;
      this.pauseResolve = null;
    }

    if (this.controller) {
      this.controller.requestResume(this);
    }
  }

  isPaused(): boolean {
    return this.backpressureState === 'paused';
  }

  getBackpressureState(): BackpressureState {
    return this.backpressureState;
  }

  getBackpressureStats(): BackpressureStats {
    return {
      memoryUsage: this.memoryUsage,
      pauseCount: this.pauseCount,
      resumeCount: this.resumeCount,
      state: this.backpressureState,
      bufferedRows: this.bufferedRows,
    };
  }

  setWatermarks(config: WatermarkConfig): void {
    this.watermarks = { ...config };
  }

  onBackpressure(callback: (state: BackpressureState) => void): void {
    this.backpressureCallbacks.push(callback);
  }

  /**
   * Wait if the operator is paused
   * Call this at the beginning of next() to respect backpressure
   */
  protected async waitIfPaused(): Promise<void> {
    if (this.pausePromise) {
      await this.pausePromise;
    }
  }

  /**
   * Check if backpressure should be applied based on memory usage
   * Call this after buffering data to auto-pause if needed
   */
  protected checkMemoryPressure(): void {
    if (this.memoryUsage >= this.watermarks.highWatermark && this.backpressureState === 'flowing') {
      // Don't await - this is a synchronous check that triggers async pause
      void this.pause();
    }
  }

  /**
   * Check if backpressure can be released based on memory usage
   * Call this after releasing data to auto-resume if needed
   */
  protected checkMemoryRelease(): void {
    if (this.memoryUsage <= this.watermarks.lowWatermark && this.backpressureState === 'paused') {
      void this.resume();
    }
  }

  /**
   * Estimate memory size of a row (rough approximation)
   */
  protected estimateRowSize(row: Row): number {
    let size = 0;
    for (const value of Object.values(row)) {
      if (value === null) {
        size += 8;
      } else if (typeof value === 'string') {
        size += value.length * 2; // UTF-16
      } else if (typeof value === 'number') {
        size += 8;
      } else if (typeof value === 'bigint') {
        size += 16;
      } else if (typeof value === 'boolean') {
        size += 4;
      } else if (value instanceof Date) {
        size += 8;
      } else if (value instanceof Uint8Array) {
        size += value.length;
      }
    }
    return size + 64; // Object overhead
  }

  /**
   * Report memory usage to the controller if present
   */
  protected reportMemory(): void {
    if (this.controller) {
      this.controller.reportMemoryUsage(this, this.memoryUsage);
    }
  }

  protected notifyBackpressureChange(): void {
    for (const callback of this.backpressureCallbacks) {
      callback(this.backpressureState);
    }
  }

  /**
   * Reset backpressure state (call in close())
   */
  protected resetBackpressureState(): void {
    this.backpressureState = 'flowing';
    this.pausePromise = null;
    this.pauseResolve = null;
    this.memoryUsage = 0;
    this.bufferedRows = 0;
    if (this.controller) {
      this.controller.reportMemoryUsage(this, 0);
    }
  }
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

/**
 * Check if an operator supports backpressure
 */
export function supportsBackpressure(operator: Operator): operator is BackpressureOperator {
  return (
    'pause' in operator &&
    'resume' in operator &&
    'isPaused' in operator &&
    'getBackpressureState' in operator
  );
}

/**
 * Create a backpressure-aware async iterator from an operator
 * This wraps any operator and adds backpressure support for iteration
 */
export async function* createBackpressureIterator(
  operator: Operator,
  controller?: BackpressureController
): AsyncGenerator<Row, void, unknown> {
  const bpController = controller ?? new DefaultBackpressureController();

  try {
    while (true) {
      // Check if we should pause before getting next row
      if (bpController.shouldPause()) {
        await new Promise<void>((resolve) => {
          const checkResume = (): void => {
            if (bpController.shouldResume()) {
              resolve();
            } else {
              setTimeout(checkResume, 10);
            }
          };
          checkResume();
        });
      }

      const row = await operator.next();
      if (row === null) break;

      yield row;
    }
  } finally {
    await operator.close();
  }
}

/**
 * Apply backpressure to a source operator based on downstream demand
 * Returns a function that can be called to request more rows
 */
export function createDemandController(
  source: BackpressureOperator,
  batchSize = 100
): { request: (count: number) => void; getRequested: () => number } {
  let requested = 0;

  return {
    request(count: number): void {
      requested += count;
      if (requested > 0 && source.isPaused()) {
        void source.resume();
      }
    },
    getRequested(): number {
      return requested;
    },
  };
}
