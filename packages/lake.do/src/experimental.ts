/**
 * @dotdo/lake.do/experimental - Experimental exports
 *
 * This module contains experimental features that may change in any version.
 * Use with caution in production environments.
 *
 * ## Stability Warning
 *
 * All exports from this module are marked as experimental:
 * - They may be removed without notice
 * - Their API may change in breaking ways
 * - They may have bugs or incomplete implementations
 *
 * ## Feedback
 *
 * If you're using experimental features, please provide feedback:
 * - GitHub Issues: https://github.com/dot-do/sql/issues
 * - Include "experimental" in the issue title
 *
 * @example
 * ```typescript
 * // Import experimental features explicitly
 * import {
 *   CDCStreamOptions,
 *   CDCBatch,
 *   CompactionConfig,
 *   LakeMetrics,
 * } from '@dotdo/lake.do/experimental';
 * ```
 *
 * @packageDocumentation
 * @stability experimental
 */

// =============================================================================
// CDC Streaming Types
// =============================================================================

/**
 * CDC (Change Data Capture) streaming types - experimental.
 *
 * These types are used for real-time change data capture subscriptions.
 * The CDC streaming API is still evolving and may change significantly.
 *
 * @stability experimental
 * @since 0.1.0
 */
export type {
  CDCStreamOptions,
  CDCBatch,
  CDCStreamState,
} from './types.js';

// =============================================================================
// Compaction Types
// =============================================================================

/**
 * File compaction types - experimental.
 *
 * These types are used for triggering and monitoring file compaction jobs.
 * The compaction API is still evolving.
 *
 * @stability experimental
 * @since 0.1.0
 */
export type {
  CompactionConfig,
  CompactionJob,
} from './types.js';

// =============================================================================
// Metrics Types
// =============================================================================

/**
 * Lakehouse metrics types - experimental.
 *
 * These types are used for monitoring lakehouse operations.
 *
 * @stability experimental
 * @since 0.1.0
 */
export type {
  LakeMetrics,
} from './types.js';

// =============================================================================
// CDC Re-exports from sql.do
// =============================================================================

/**
 * CDC types re-exported from @dotdo/sql.do - experimental.
 *
 * @stability experimental
 * @since 0.1.0
 */
export type {
  CDCOperation,
  CDCEvent,
  ClientCDCOperation,
  ClientCapabilities,
} from './types.js';

/**
 * CDC constants and utilities - experimental.
 *
 * @stability experimental
 * @since 0.1.0
 */
export {
  CDCOperationCode,
  DEFAULT_CLIENT_CAPABILITIES,
  isServerCDCEvent,
  isClientCDCEvent,
  isDateTimestamp,
  isNumericTimestamp,
  serverToClientCDCEvent,
  clientToServerCDCEvent,
} from './types.js';
