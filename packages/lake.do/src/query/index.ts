/**
 * lake.do - Query Module
 *
 * This module provides query execution and response transformation.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

// =============================================================================
// Query Executor
// =============================================================================

/**
 * Query executor and related types.
 * @public
 * @stability stable
 */
export {
  QueryExecutor,
  type QueryExecutorConfig,
  type RPCSender,
  type RequestHandler,
} from './executor.js';
