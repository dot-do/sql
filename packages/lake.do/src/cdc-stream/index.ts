/**
 * @dotdo/lake.do - CDC Stream Module
 *
 * This module exports CDC stream components with separated concerns:
 * - Queue management (BoundedQueue)
 * - Metrics tracking (MetricsTracker)
 * - Stream coordination (CDCStreamController)
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

// =============================================================================
// Controller (main export)
// =============================================================================

export {
  CDCStreamController,
  type CDCStreamControllerOptions,
  type CDCStreamMetrics,
  type BackpressureStrategy,
  type WaterMarkEvent,
} from './controller.js';

// =============================================================================
// Queue (for advanced usage)
// =============================================================================

export {
  BoundedQueue,
  type BoundedQueueOptions,
  type PushResult,
} from './queue.js';

// =============================================================================
// Metrics (for advanced usage)
// =============================================================================

export {
  MetricsTracker,
  type MetricsTrackerOptions,
  type QueueMetrics,
} from './metrics.js';
