/**
 * @dotdo/sql.do/experimental - Experimental exports
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
 * import { generateIdempotencyKey, isMutationQuery } from '@dotdo/sql.do/experimental';
 *
 * // Generate idempotency key for mutation requests
 * const key = await generateIdempotencyKey('INSERT INTO users (name) VALUES (?)', ['Alice']);
 * ```
 *
 * @packageDocumentation
 * @stability experimental
 */

// =============================================================================
// Idempotency Utilities
// =============================================================================

/**
 * Generate an idempotency key for mutation requests.
 *
 * @stability experimental
 * @since 0.1.0
 */
export { generateIdempotencyKey, isMutationQuery } from './client.js';

// =============================================================================
// CDC Types and Utilities
// =============================================================================

/**
 * CDC (Change Data Capture) types - experimental.
 *
 * @stability experimental
 * @since 0.1.0
 */
export type {
  CDCOperation,
  ClientCDCOperation,
  CDCEvent,
  CDCOperationCodeValue,
} from './types.js';

/**
 * CDC utilities and constants - experimental.
 *
 * @stability experimental
 * @since 0.1.0
 */
export {
  CDCOperationCode,
  isServerCDCEvent,
  isClientCDCEvent,
  isDateTimestamp,
  isNumericTimestamp,
  serverToClientCDCEvent,
  clientToServerCDCEvent,
} from './types.js';

// =============================================================================
// Sharding Types
// =============================================================================

/**
 * Sharding types - experimental.
 *
 * @stability experimental
 * @since 0.1.0
 */
export type {
  ShardConfig,
  ShardInfo,
} from './types.js';

// =============================================================================
// Client Capabilities
// =============================================================================

/**
 * Client capabilities types and defaults - experimental.
 *
 * @stability experimental
 * @since 0.1.0
 */
export type {
  ClientCapabilities,
} from './types.js';

export {
  DEFAULT_CLIENT_CAPABILITIES,
} from './types.js';

// =============================================================================
// Connection Types
// =============================================================================

/**
 * Connection types - experimental.
 *
 * @stability experimental
 * @since 0.1.0
 */
export type {
  ConnectionOptions,
  ConnectionStats,
} from './types.js';

// =============================================================================
// Response/Result Converters
// =============================================================================

/**
 * Response/Result converters - experimental.
 *
 * @stability experimental
 * @since 0.1.0
 */
export {
  responseToResult,
  resultToResponse,
} from './types.js';
