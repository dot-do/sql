/**
 * SQLite Built-in Functions
 *
 * This module exports all SQLite-compatible built-in functions for DoSQL.
 * Functions are organized into categories:
 * - String functions: length, substr, upper, lower, trim, replace, etc.
 * - Math functions: abs, round, random, pow, sqrt, sin, cos, etc.
 * - Date/time functions: date, time, datetime, strftime, julianday, etc.
 * - Aggregate functions: count, sum, avg, min, max, group_concat, etc.
 * - JSON functions: json, json_extract, json_array, json_object, etc.
 *
 * @example
 * ```typescript
 * import { invokeFunction, hasFunction, isAggregate } from '@dotdo/dosql/functions';
 *
 * // Invoke a function
 * const result = invokeFunction('upper', ['hello']); // 'HELLO'
 *
 * // Check if function exists
 * if (hasFunction('custom_fn')) { ... }
 *
 * // Check if aggregate
 * if (isAggregate('count')) { ... }
 * ```
 *
 * @example Register custom function
 * ```typescript
 * import { defaultRegistry } from '@dotdo/dosql/functions';
 *
 * defaultRegistry.register('double', {
 *   fn: (x) => typeof x === 'number' ? x * 2 : null,
 *   minArgs: 1,
 *   maxArgs: 1,
 * });
 * ```
 */

// =============================================================================
// REGISTRY (Main API)
// =============================================================================

export {
  // Types
  type SqlFunction,
  type FunctionParam,
  type FunctionSignature,
  type UserDefinedFunction,
  type AggregateAccumulator,
  type AggregateFactory,

  // Registry class
  FunctionRegistry,

  // Default registry instance
  defaultRegistry,

  // Convenience functions
  invokeFunction,
  hasFunction,
  isAggregate,
  evaluateFunction,
} from './registry.js';

// =============================================================================
// STRING FUNCTIONS
// =============================================================================

export {
  // Functions
  length,
  substr,
  upper,
  lower,
  trim,
  ltrim,
  rtrim,
  replace,
  instr,
  printf,
  quote,
  hex,
  unhex,
  zeroblob,
  concat,
  concat_ws,
  char_fn,
  unicode,
  like,
  glob,

  // Registry data
  stringFunctions,
  stringSignatures,
} from './string.js';

// =============================================================================
// MATH FUNCTIONS
// =============================================================================

export {
  // Functions
  abs,
  round,
  random,
  max_scalar,
  min_scalar,
  sign,
  pow,
  sqrt,
  ceil,
  floor,
  trunc,
  mod,
  log,
  log10,
  log2,
  exp,
  sin,
  cos,
  tan,
  asin,
  acos,
  atan,
  atan2,
  pi,
  degrees,
  radians,
  nullif,
  ifnull,
  coalesce,
  iif,

  // Registry data
  mathFunctions,
  mathSignatures,
} from './math.js';

// =============================================================================
// DATE/TIME FUNCTIONS
// =============================================================================

export {
  // Functions
  date,
  time,
  datetime,
  julianday,
  strftime,
  unixepoch,
  current_date,
  current_time,
  current_timestamp,
  timediff,

  // Registry data
  dateFunctions,
  dateSignatures,
} from './date.js';

// =============================================================================
// AGGREGATE FUNCTIONS
// =============================================================================

export {
  // Types
  type AggregateAccumulator as AggregateAcc,

  // Factories
  aggregateFactories,
  createGroupConcat,
  createGroupConcatDistinct,

  // Utilities
  executeAggregate,
  isAggregateFunction,

  // Registry data
  aggregateSignatures,
} from './aggregate.js';

// =============================================================================
// JSON FUNCTIONS
// =============================================================================

export {
  // Functions
  json,
  json_valid,
  json_extract,
  json_type,
  json_array,
  json_object,
  json_array_length,
  json_insert,
  json_replace,
  json_set,
  json_remove,
  json_patch,
  json_quote,
  json_each,

  // Types
  type JsonEachRow,

  // Registry data
  jsonFunctions,
  jsonSignatures,
} from './json.js';

// =============================================================================
// VECTOR FUNCTIONS
// =============================================================================

export {
  // Vector creation and extraction
  vector,
  vector_extract,
  vector_dims,

  // Distance functions (SQL-compatible)
  sql_vector_distance_cos,
  sql_vector_distance_l2,
  sql_vector_distance_dot,

  // Vector operations
  sql_vector_norm,
  sql_vector_normalize,
  sql_vector_add,
  sql_vector_sub,
  sql_vector_scale,
  sql_vector_dot,

  // Helpers
  createSqlVector,
  extractVector,
  isSqlVector,

  // Registry data
  vectorFunctions,
  vectorSignatures,
} from './vector.js';

// =============================================================================
// WINDOW FUNCTIONS
// =============================================================================

export {
  // Types
  type WindowSpec,
  type WindowContext,
  type WindowFrame,
  type FrameBoundary,
  type FrameBoundaryType,
  type FrameMode,
  type WindowFunction,

  // Ranking functions
  rowNumber,
  rank,
  denseRank,
  ntile,
  percentRank,
  cumeDist,

  // Value access functions
  lagColumn,
  leadColumn,
  firstValue,
  lastValue,
  nthValue,

  // Aggregate window functions
  windowSum,
  windowAvg,
  windowCount,
  windowMin,
  windowMax,

  // Frame utilities
  calculateFrameBoundaries,
  getDefaultFrame,
  DEFAULT_FRAME_WITH_ORDER,
  DEFAULT_FRAME_WITHOUT_ORDER,

  // Registry data
  windowFunctionSignatures,
  windowFunctions,
  windowValueFunctions,
  windowAggregateFunctions,
  isWindowFunction,
} from './window.js';
