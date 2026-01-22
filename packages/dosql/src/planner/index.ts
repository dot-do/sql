/**
 * DoSQL Cost-Based Query Planner
 *
 * A complete cost-based query optimizer supporting:
 * - Table and index statistics
 * - Histogram-based cardinality estimation
 * - Index selection based on selectivity
 * - Join ordering optimization
 * - Predicate pushdown
 * - Multiple EXPLAIN output formats
 *
 * @packageDocumentation
 */

// Planning context (request-scoped ID generation)
export * from './planning-context.js';

// Types
export * from './types.js';

// Statistics
export * from './stats.js';

// Cost estimation
export * from './cost.js';

// Optimizer
export * from './optimizer.js';

// EXPLAIN output
export * from './explain.js';
