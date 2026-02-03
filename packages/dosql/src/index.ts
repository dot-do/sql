/**
 * DoSQL - Type-safe SQL for TypeScript
 *
 * A compile-time SQL parser that infers result types from SQL queries.
 * Case-sensitive for identifiers (like ClickHouse, not SQLite).
 *
 * ## Stability
 *
 * This package follows semantic versioning. Exports are marked with stability annotations:
 *
 * - **stable**: No breaking changes in minor versions. Safe for production use.
 * - **experimental**: May change in any version. Use with caution.
 * - **deprecated**: Scheduled for removal; see deprecation notice for migration path.
 *
 * See {@link APIStabilityEntry} and {@link DOSQL_STABLE_EXPORTS} for the formal manifest.
 *
 * @packageDocumentation
 */

// =============================================================================
// API STABILITY POLICY
// =============================================================================

/**
 * API stability definitions and validation utilities.
 * @public
 * @stable
 */
export {
  type StabilityLevel,
  type APIStabilityEntry,
  DOSQL_STABLE_EXPORTS,
  DOSQL_EXPERIMENTAL_EXPORTS,
  validateStableExports,
  getRuntimeExports,
  getExportsByStability,
} from './api-stability.js';

// =============================================================================
// CORE PARSER (stable)
// =============================================================================

/**
 * Core SQL type-level parser and runtime utilities.
 *
 * These are the foundational APIs for compile-time SQL type inference.
 *
 * @public
 * @stable
 */
export {
  // Type exports
  type ColumnType,
  type TableSchema,
  type DatabaseSchema,
  type QueryResult,
  type TypedDatabase,
  type SQL,

  // Runtime exports
  createDatabase,
  createQuery,
} from './parser.js';

// =============================================================================
// AGGREGATE AND EXPRESSION TYPES (experimental)
// =============================================================================

/**
 * Aggregate function and expression type-level utilities.
 *
 * These types power compile-time inference for GROUP BY, aggregate functions,
 * and arithmetic expressions. The type-level API surface is still evolving.
 *
 * @public
 * @experimental
 * @since 0.1.0
 */
export {
  type AggregateFunctionName,
  type IsAggregateFunction,
  type ExtractAggregateFn,
  type ExtractAggregateArg,
  type AggregateResultType,
  type ArithmeticOperator,
  type IsArithmeticExpression,
  type IsNumericLiteral,
  type ParseExpressionWithAlias,
  type GetExpressionOutputName,
  type ResolveExpressionType,
  type HasGroupBy,
  type ExtractGroupByColumns,
  type ParseSelectExpression,
  type ParseSelectExpressions,
  type ExpressionToResultType,
} from './aggregates.js';

// =============================================================================
// URL TABLE SOURCES (experimental)
// =============================================================================

/**
 * URL and R2 table source support (ClickHouse-style FROM 'url' syntax).
 *
 * Enables querying remote CSV, JSON, NDJSON, and Parquet data sources
 * directly from SQL. The wire format and options API are still evolving.
 *
 * @public
 * @experimental
 * @since 0.1.0
 */
export * from './sources/index.js';

// =============================================================================
// WAL - Write-Ahead Log (experimental)
// =============================================================================

/**
 * Write-ahead log for durability and recovery.
 *
 * Provides segment-based WAL with CRC32 checksums, checkpointing,
 * and retention management. The storage format may change between versions.
 *
 * @public
 * @experimental
 * @since 0.1.0
 */
export * from './wal/index.js';

// =============================================================================
// CDC - Change Data Capture (experimental)
// =============================================================================

/**
 * Change Data Capture streaming for real-time replication.
 *
 * Provides subscription-based CDC with replication slots, backpressure,
 * and lakehouse streaming integration. The protocol is versioned but
 * the API surface is still experimental.
 *
 * @public
 * @experimental
 * @since 0.1.0
 */
export * from './cdc/index.js';

// =============================================================================
// ESM STORED PROCEDURES (experimental)
// =============================================================================

/**
 * ESM-based stored procedure system.
 *
 * Allows defining, registering, and executing stored procedures as
 * ES modules within the Durable Object. The procedure API, context
 * shape, and registry interface are still evolving.
 *
 * @public
 * @experimental
 * @since 0.1.0
 */
export {
  // Re-exported schema types
  type DatabaseSchema as ProcDatabaseSchema,
  type TableSchema as ProcTableSchema,

  // Schema types
  type Schema,
  type ProcSchemaToType,

  // Table accessor types
  type Predicate,
  type QueryOptions,
  type TableAccessor,

  // Database context types
  type TableSchemaToRecord,
  type DatabaseAccessor,
  type SqlFunction,
  type TransactionFunction,
  type TransactionContext,
  type DatabaseContext,

  // Procedure context types
  type ProcedureEnv,
  type ProcedureContext,

  // Procedure definition types
  type ProcedureMetadata,
  type Procedure,
  type ProcedureHandler,
  type TypedProcedure,
  type InferProcedureResult,

  // Execution types
  type ExecutionOptions,
  type ExecutionResult,

  // Registry types
  type RegistryEntry,
  type ProcedureRegistry,

  // Parser types
  type ParsedProcedure,
  type ParseError,

  // Context exports
  type StorageAdapter,
  createTableAccessor,
  type SqlExecutor,
  createSqlFunction,
  type TransactionManager,
  createTransactionContext,
  createTransactionFunction,
  type DatabaseContextOptions,
  createDatabaseContext,
  createInMemoryAdapter,
  createInMemorySqlExecutor,
  createInMemoryTransactionManager,

  // Parser exports
  parseProcedure,
  tryParseProcedure,
  isCreateProcedure,
  validateModuleCode,
  sqlTypeToSchema,
  buildInputSchema,
  buildOutputSchema,

  // Registry exports
  type CatalogStorage,
  createInMemoryCatalogStorage,
  type RegistryOptions,
  createProcedureRegistry,
  type SqlProcedureManager,
  createSqlProcedureManager,
  ProcedureBuilder,
  procedure,
  type ListOptions,
  type ExtendedProcedureRegistry,
  createExtendedRegistry,

  // Executor exports
  type ProcedureExecutor,
  type ExecutorOptions,
  createProcedureExecutor,
  createSimpleExecutor,
  createProcedureCall,
  batchExecute,
  sequentialExecute,
  createMockExecutor,
} from './proc/index.js';

// =============================================================================
// SHARDING (experimental)
// =============================================================================

/**
 * Native sharding with vindexes, query routing, and distributed execution.
 *
 * Inspired by Vitess but with real SQL parsing, cost-based routing,
 * and native replica support. The sharding topology, vindex types,
 * and migration APIs are still experimental.
 *
 * @public
 * @experimental
 * @since 0.1.0
 */
export * from './sharding/index.js';

// =============================================================================
// TRANSACTIONS (stable)
// =============================================================================

/**
 * ACID transaction support with MVCC, savepoints, and isolation levels.
 *
 * Provides a complete transaction system including lock management,
 * multi-version concurrency control, and configurable isolation.
 * The core transaction APIs are stable.
 *
 * @public
 * @stable
 */
export {
  // State enums
  TransactionState,
  TransactionMode,
  IsolationLevel,
  LockType,

  // Error types
  TransactionError,
  TransactionErrorCode,

  // Core types
  type Savepoint,
  type SavepointStack,
  type TransactionLog,
  type TransactionLogEntry,
  type TransactionLogOperation,
  type TransactionContext as TxnContext,
  type TransactionOptions,
  type TransactionManager as TxnManager,
  type ApplyFunction,

  // MVCC types
  type Snapshot,
  type RowVersion,

  // Lock types
  type LockRequest,
  type LockResult,
  type HeldLock,

  // Stats
  type TransactionStats,

  // Factory functions
  createTransactionLog,
  createSavepointStack,

  // Manager
  createTransactionManager,
  executeInTransaction,
  executeWithSavepoint,
  executeReadOnly,
  createAutoCommitManager,
  type TransactionManagerOptions,
  type TransactionResult,

  // Isolation
  createLockManager,
  createMVCCStore,
  createIsolationEnforcer,
  createSnapshot,
  isVersionVisible,
  type LockManager,
  type LockManagerOptions,
  type MVCCStore,
  type IsolationEnforcer,
  type IsolationEnforcerOptions,
} from './transaction/index.js';

// =============================================================================
// VIRTUAL TABLES (experimental)
// =============================================================================

/**
 * Virtual table support for querying remote data sources via SQL.
 *
 * Enables SELECT from URLs, R2 objects, and other external sources
 * with automatic format detection and schema inference. The virtual
 * table interface and registry API are still evolving.
 *
 * @public
 * @experimental
 * @since 0.1.0
 */
export * from './virtual/index.js';
