/**
 * Utility Functions
 *
 * Shared utilities for the DoSQL package.
 */

export {
  // Percentile calculations
  calculatePercentile,
  calculatePercentileUnsorted,
  calculatePercentiles,

  // Statistical calculations
  calculateBasicStats,
  calculateStdDev,
  calculateLatencyStatistics,
  calculateLatencyHistogram,

  // Types
  type BasicStats,
  type LatencyStatistics,
  type LatencyHistogram,
} from './math.js';

export {
  // Table accessor utilities
  createSimpleTableAccessor,
  createSimpleTableAccessors,

  // Types
  type SimpleTableAccessorConfig,
} from './table-accessor.js';

export {
  // Retry utilities
  withRetry,
  withRetryResult,
  withContextualRetry,
  sleep,
  createRetryWrapper,
  retryPredicates,

  // Types
  type RetryOptions,
  type RetryResult,
  type ContextualRetryOptions,
} from './retry.js';

export {
  // Aggregate state functions
  createAggregateState,
  updateAggregateState,
  getAggregateResult,
  mergeAggregateStates,

  // Aggregate combine functions (for distributed aggregation)
  combineCount,
  combineSum,
  combineAvg,
  combineMin,
  combineMax,

  // Comparison utilities
  compareSqlValues,
  isSqlTruthy,

  // Types
  type AggregateFunction,
  type SqlValue as AggregateSqlValue,
  type AggregateState,
} from './aggregate.js';
