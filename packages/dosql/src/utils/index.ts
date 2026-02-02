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

export {
  // Sandbox module builders
  buildSandboxModule,
  buildProcedureModule,
  buildCustomSandboxModule,

  // Code generation helpers
  buildTableAccessorCode,
  buildDbCallCode,
  buildSqlCode,
  buildTransactionCode,

  // RPC handlers
  createOutboundRpcHandler,
  createExtendedOutboundRpcHandler,

  // Types
  type SandboxDatabaseContext,
  type SandboxModuleOptions,
  type SandboxExecutionOptions,
} from './sandbox.js';

export {
  // SQL expression evaluation
  evaluateWhenClause,
  sqlEquals,
  sqlLike,
  sqlTruthy,
} from './sql-eval.js';

export {
  // Exhaustiveness checking
  assertNever,
} from './assert-never.js';

export {
  // Safe LIKE/GLOB pattern matching (ReDoS-safe)
  safeLikeMatch,
  safeLikeMatchWithEscape,
  safeGlobMatch,
} from './safe-like.js';

export {
  // Hash functions
  fnv1a,
  fnv1aString,
  fnv1aNumber,
  fnv1aBigInt,
  fnv1aBytes,
  xxhash,
  getHashFunction,

  // Hash constants
  FNV_OFFSET_BASIS,
  FNV_PRIME,

  // Types
  type HashAlgorithm,
} from './hash.js';

export {
  // Type guards for Record<string, unknown>
  isObject,
  isObjectOrArray,
  isArray,
  isTypedArray,

  // Property type guards
  hasProperty,
  hasPropertyOfType,
  hasStringProperty,
  hasNumberProperty,
  hasBooleanProperty,
  hasArrayProperty,
  hasTypedArrayProperty,
  hasObjectProperty,
  hasFunctionProperty,
  hasStringProperties,
  hasProperties,

  // Optional property accessors
  getStringProperty,
  getNumberProperty,
  getBooleanProperty,
  getArrayProperty,
  getObjectProperty,
  getTypedProperty,

  // Common object shape guards
  hasErrorCode,
  hasTypeProperty,
  isErrorLike,
  hasTypeDiscriminator,

  // Record iteration helpers
  iterateRecord,
  getKeys,
  getValues,
  getEntries,

  // Primitive type guards
  isString,
  isNumber,
  isBoolean,
  isBigInt,
  isFunction,
  isNullish,
  isDefined,

  // Casting helpers
  asRecord,
  asRecordOrThrow,
  asArray,
  asTypedArray,

  // SQL value guards
  isSqlValue,
  isRow,

  // Utility types
  type RecordWithRequired,
  type RecordWith,
  type PartialRecord,
  type SqlValue as TypeGuardSqlValue,
  type Row,
} from './type-guards.js';
