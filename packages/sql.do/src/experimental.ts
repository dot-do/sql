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
 * ## Feature Overview
 *
 * ### Idempotency Utilities
 *
 * Functions for generating and managing idempotency keys to ensure at-most-once
 * execution semantics for mutation operations. Useful for building reliable
 * distributed systems that can safely retry failed requests.
 *
 * - {@link generateIdempotencyKey} - Generate unique keys for mutation deduplication
 * - {@link isMutationQuery} - Detect INSERT/UPDATE/DELETE statements
 *
 * ### CDC (Change Data Capture) Types
 *
 * Types and utilities for working with real-time change data capture streams.
 * CDC enables applications to react to database changes as they happen, useful
 * for event sourcing, cache invalidation, and data synchronization.
 *
 * - {@link CDCOperation} - Operation types (INSERT, UPDATE, DELETE, TRUNCATE)
 * - {@link CDCEvent} - Change event with before/after row data
 * - {@link CDCOperationCode} - Numeric codes for efficient binary encoding
 * - Type guards: `isServerCDCEvent()`, `isClientCDCEvent()`
 * - Converters: `serverToClientCDCEvent()`, `clientToServerCDCEvent()`
 *
 * ### Sharding Types
 *
 * Types for configuring and querying sharded database deployments.
 * Sharding enables horizontal scaling by distributing data across multiple
 * database instances based on a partition key.
 *
 * - {@link ShardConfig} - Sharding strategy configuration
 * - {@link ShardInfo} - Runtime shard location information
 *
 * ### Client Capabilities
 *
 * Types for protocol negotiation between client and server, enabling
 * feature detection and graceful degradation.
 *
 * - {@link ClientCapabilities} - Client feature flags
 * - {@link DEFAULT_CLIENT_CAPABILITIES} - Default capability settings
 *
 * ### Connection Types
 *
 * Types for managing database connections and monitoring connection health.
 *
 * - {@link ConnectionOptions} - Connection configuration
 * - {@link ConnectionStats} - Connection pool statistics
 *
 * ### Response/Result Converters
 *
 * Utilities for converting between internal server response format and
 * client-facing result format.
 *
 * - {@link responseToResult} - Convert server response to client result
 * - {@link resultToResponse} - Convert client result to server response
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
 *   generateIdempotencyKey,
 *   isMutationQuery,
 *   CDCOperationCode,
 *   isServerCDCEvent,
 *   serverToClientCDCEvent,
 * } from '@dotdo/sql.do/experimental';
 *
 * // Generate idempotency key for mutation requests
 * const key = await generateIdempotencyKey('INSERT INTO users (name) VALUES (?)', ['Alice']);
 *
 * // Check if a query is a mutation
 * if (isMutationQuery(sql)) {
 *   const key = await generateIdempotencyKey(sql, params);
 *   // Include key in request for deduplication
 * }
 *
 * // Process CDC events from a stream
 * for await (const event of cdcStream) {
 *   if (isServerCDCEvent(event)) {
 *     const clientEvent = serverToClientCDCEvent(event);
 *     switch (clientEvent.operation) {
 *       case 'INSERT':
 *         handleInsert(clientEvent.after);
 *         break;
 *       case 'UPDATE':
 *         handleUpdate(clientEvent.before, clientEvent.after);
 *         break;
 *       case 'DELETE':
 *         handleDelete(clientEvent.before);
 *         break;
 *     }
 *   }
 * }
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
