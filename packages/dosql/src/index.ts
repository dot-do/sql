/**
 * DoSQL - Type-safe SQL for TypeScript
 *
 * A compile-time SQL parser that infers result types from SQL queries.
 * Case-sensitive for identifiers (like ClickHouse, not SQLite).
 *
 * @packageDocumentation
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

// Aggregate and expression type exports
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
// URL TABLE SOURCES
// =============================================================================

// Re-export all sources module exports
export * from './sources/index.js';

// =============================================================================
// WAL (Write-Ahead Log)
// =============================================================================

// Re-export WAL module
export * from './wal/index.js';

// =============================================================================
// CDC (Change Data Capture)
// =============================================================================

// Re-export CDC module
export * from './cdc/index.js';

// =============================================================================
// ESM STORED PROCEDURES
// =============================================================================

// Re-export procedure module (selective to avoid conflicts with sources SchemaToType)
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
// SHARDING
// =============================================================================

// Re-export sharding module
export * from './sharding/index.js';

// =============================================================================
// TRANSACTIONS
// =============================================================================

// Re-export transaction module (selective to avoid conflicts)
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
// VIRTUAL TABLES
// =============================================================================

// Re-export virtual table module
export * from './virtual/index.js';
