/**
 * Statement Module for DoSQL
 *
 * Provides prepared statements with parameter binding, caching,
 * and execution capabilities compatible with better-sqlite3/D1.
 */

// Types
export type {
  Statement,
  SqlValue,
  BindParameters,
  NamedParameters,
  PositionalParameters,
  RunResult,
  ColumnInfo,
  StatementOptions,
  TransactionMode,
  TransactionFunction,
  Database,
  DatabaseOptions,
  PragmaName,
  PragmaResult,
  AggregateOptions,
  TableInfoRow,
  IndexListRow,
  DatabaseListRow,
} from './types.js';

// Binding
export {
  parseParameters,
  bindParameters,
  validateParameters,
  coerceValue,
  isNamedParameters,
  escapeString,
  formatSqlWithValues,
  BindingError,
  type ParsedParameters,
  type ParameterToken,
  type ParameterType,
} from './binding.js';

// Cache
export {
  StatementCache,
  createStatementCache,
  hashString,
  estimateSqlSize,
  type CacheEntry,
  type CacheOptions,
  type CacheStats,
} from './cache.js';

// Statement
export {
  PreparedStatement,
  InMemoryEngine,
  createInMemoryStorage,
  createStatement,
  StatementError,
  type ExecutionEngine,
  type ExecutionResult,
  type InMemoryStorage,
  type InMemoryTable,
} from './statement.js';
