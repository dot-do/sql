/**
 * ESM Stored Procedures for DoSQL
 *
 * Provides PL/pgSQL-like stored procedures using ESM modules
 * executed in sandboxed V8 isolates via ai-evaluate.
 *
 * @packageDocumentation
 *
 * @example
 * ```typescript
 * import { createProcedureRegistry, createProcedureExecutor, parseProcedure } from 'dosql/proc';
 *
 * // Create registry and executor
 * const registry = createProcedureRegistry({
 *   storage: createInMemoryCatalogStorage(),
 * });
 *
 * // Register a procedure via SQL
 * const sql = `
 *   CREATE PROCEDURE calculate_total AS MODULE $$
 *     export default async ({ db }, userId) => {
 *       const orders = await db.orders.where({ userId });
 *       return orders.reduce((sum, o) => sum + o.total, 0);
 *     }
 *   $$;
 * `;
 *
 * const parsed = parseProcedure(sql);
 * await registry.register({
 *   name: parsed.name,
 *   code: parsed.code,
 * });
 *
 * // Execute the procedure
 * const executor = createProcedureExecutor({ db, registry });
 * const result = await executor.call('calculate_total', [userId]);
 * ```
 */

// =============================================================================
// TYPE EXPORTS
// =============================================================================

export type {
  // Re-export DatabaseSchema and TableSchema
  DatabaseSchema,
  TableSchema,

  // Schema types
  Schema,
  ProcSchemaToType,

  // Table accessor types
  Predicate,
  QueryOptions,
  TableAccessor,

  // Database context types
  TableSchemaToRecord,
  DatabaseAccessor,
  SqlFunction,
  TransactionFunction,
  TransactionContext,
  DatabaseContext,

  // Procedure context types
  ProcedureEnv,
  ProcedureContext,

  // Procedure definition types
  ProcedureMetadata,
  Procedure,
  ProcedureHandler,
  TypedProcedure,
  InferProcedureResult,

  // Execution types
  ExecutionOptions,
  ExecutionResult,

  // Registry types
  RegistryEntry,
  ProcedureRegistry,

  // Parser types
  ParsedProcedure,
  ParseError,
} from './types.js';

// =============================================================================
// CONTEXT EXPORTS
// =============================================================================

export {
  // Storage adapter
  type StorageAdapter,

  // Table accessor
  createTableAccessor,

  // SQL execution
  type SqlExecutor,
  createSqlFunction,

  // Transaction support
  type TransactionManager,
  createTransactionContext,
  createTransactionFunction,

  // Database context factory
  type DatabaseContextOptions,
  createDatabaseContext,

  // In-memory implementations (for testing)
  createInMemoryAdapter,
  createInMemorySqlExecutor,
  createInMemoryTransactionManager,
} from './context.js';

// =============================================================================
// PARSER EXPORTS
// =============================================================================

export {
  // Main parser
  parseProcedure,
  tryParseProcedure,
  isCreateProcedure,

  // Validation
  validateModuleCode,

  // Schema inference
  sqlTypeToSchema,
  buildInputSchema,
  buildOutputSchema,
} from './parser.js';

// =============================================================================
// REGISTRY EXPORTS
// =============================================================================

export {
  // Catalog storage
  type CatalogStorage,
  createInMemoryCatalogStorage,

  // Registry
  type RegistryOptions,
  createProcedureRegistry,

  // SQL interface
  type SqlProcedureManager,
  createSqlProcedureManager,

  // Builder
  ProcedureBuilder,
  procedure,

  // Extended registry
  type ListOptions,
  type ExtendedProcedureRegistry,
  createExtendedRegistry,
} from './registry.js';

// =============================================================================
// EXECUTOR EXPORTS
// =============================================================================

export {
  // Executor
  type ProcedureExecutor,
  type ExecutorOptions,
  createProcedureExecutor,

  // Simple executor for testing
  createSimpleExecutor,

  // Helpers
  createProcedureCall,
  batchExecute,
  sequentialExecute,

  // Mock executor for unit testing
  createMockExecutor,
} from './executor.js';

// =============================================================================
// FUNCTIONAL API EXPORTS
// =============================================================================

export {
  // Main functional API
  defineProcedures,
  defineProcedure,
  defineProceduresWithMiddleware,
  defineStreamingProcedure,

  // Wrappers
  withValidation,
  withRetry,

  // Types
  type FunctionalContext,
  type FunctionalDb,
  type ProcedureDef,
  type InferProcedureCall,
  type ProcedureDefinitions,
  type InferProcedures,
  type DefineProceduresOptions,
  type Proc,
  type VoidProc,
  type NoParamsProc,
  type StreamingProc,
  type ProcedureMiddleware,
  type DefineProceduresWithMiddlewareOptions,
  type Validator,
  type RetryOptions,
} from './functional.js';

// =============================================================================
// CONTEXT BUILDER EXPORTS
// =============================================================================

export {
  // Functional db creation
  createFunctionalDb,
  createInMemoryFunctionalDb,
  functionalDb,
  FunctionalDbBuilder,

  // SQL template helper
  sql,
  isSqlExpression,

  // Context extension
  extendContext,

  // Types
  type FunctionalDbOptions,
  type ExtendedContext,
  type ExtractSchema,
  type TypedTableAccessor,
} from './context-builder.js';
