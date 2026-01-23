/**
 * Procedure Executor
 *
 * Executes ESM stored procedures in sandboxed V8 isolates using ai-evaluate.
 * Provides secure execution with timeout protection and database context injection.
 */

import { evaluate, createEvaluator } from 'ai-evaluate';
import type { EvaluateOptions, EvaluateResult } from 'ai-evaluate';
import type {
  Procedure,
  ProcedureContext,
  DatabaseContext,
  DatabaseSchema,
  ExecutionOptions,
  ExecutionResult,
  ProcedureEnv,
  ProcedureRegistry,
} from './types.js';
import { createDatabaseContext, createInMemoryAdapter, createInMemorySqlExecutor, createInMemoryTransactionManager, type DatabaseContextOptions } from './context.js';
import {
  buildProcedureModule,
  createOutboundRpcHandler,
  type SandboxDatabaseContext,
} from '../utils/sandbox.js';

// =============================================================================
// EXECUTOR TYPES
// =============================================================================

/**
 * Procedure executor interface
 */
export interface ProcedureExecutor<DB extends DatabaseSchema = DatabaseSchema> {
  /**
   * Execute a procedure by name
   */
  call<Result = unknown>(
    name: string,
    params: unknown[],
    options?: ExecutionOptions
  ): Promise<ExecutionResult<Result>>;

  /**
   * Execute procedure code directly
   */
  execute<Result = unknown>(
    code: string,
    params: unknown[],
    options?: ExecutionOptions
  ): Promise<ExecutionResult<Result>>;

  /**
   * Execute a procedure definition
   */
  run<Result = unknown>(
    procedure: Procedure,
    params: unknown[],
    options?: ExecutionOptions
  ): Promise<ExecutionResult<Result>>;
}

// =============================================================================
// DATABASE CONTEXT SERIALIZATION
// =============================================================================

/**
 * Creates a serializable proxy for database context
 * This allows the context to be passed into the sandbox
 */
function createContextProxy<DB extends DatabaseSchema>(
  db: DatabaseContext<DB>,
  requestId: string
): Record<string, unknown> {
  // We can't directly pass the db context into the sandbox
  // Instead, we create RPC-style handlers that will be called via fetch
  return {
    tables: Object.keys(db.tables).reduce((acc, tableName) => {
      acc[tableName] = {
        __rpcTable: tableName,
      };
      return acc;
    }, {} as Record<string, unknown>),
    requestId,
  };
}

// Re-export buildProcedureModule as buildModuleWrapper for backward compatibility
export { buildProcedureModule as buildModuleWrapper } from '../utils/sandbox.js';

// =============================================================================
// EXECUTOR IMPLEMENTATION
// =============================================================================

/**
 * Options for creating a procedure executor
 */
export interface ExecutorOptions<DB extends DatabaseSchema> {
  /** Database context for procedure execution */
  db: DatabaseContext<DB>;

  /** Procedure registry for named procedure lookup */
  registry?: ProcedureRegistry;

  /** Default timeout in milliseconds */
  defaultTimeout?: number;

  /** Default memory limit in MB */
  defaultMemoryLimit?: number;

  /** Base environment variables */
  baseEnv?: ProcedureEnv;
}

/**
 * Create a procedure executor
 */
export function createProcedureExecutor<DB extends DatabaseSchema>(
  options: ExecutorOptions<DB>
): ProcedureExecutor<DB> {
  const {
    db,
    registry,
    defaultTimeout = 5000,
    defaultMemoryLimit = 128,
    baseEnv = {},
  } = options;

  // Get table names for context injection
  const tableNames = Object.keys(db.tables);

  // Create the shared outbound RPC handler for database operations
  const outboundRpc = createOutboundRpcHandler(db as unknown as SandboxDatabaseContext);

  /**
   * Execute procedure code
   */
  async function executeCode<Result>(
    code: string,
    params: unknown[],
    executionOptions: ExecutionOptions = {}
  ): Promise<ExecutionResult<Result>> {
    const startTime = Date.now();
    const requestId = `req_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;

    // Merge environment
    const env: ProcedureEnv = {
      ...baseEnv,
      ...executionOptions.env,
    };

    // Build the wrapped module code using shared utility
    const wrappedCode = buildProcedureModule(code, tableNames);

    // Create the script that calls the procedure
    const script = `
      const handler = exports.default;
      if (typeof handler !== 'function') {
        throw new Error('Procedure module must export a default function');
      }

      const ctx = {
        db,
        params: ${JSON.stringify(params)},
        env: ${JSON.stringify(env)},
        requestId: '${requestId}',
        timestamp: new Date('${new Date().toISOString()}'),
      };

      return handler(ctx, ...ctx.params);
    `;

    try {
      const evalOptions: EvaluateOptions = {
        module: wrappedCode,
        script,
        timeout: executionOptions.timeout ?? defaultTimeout,
        env,
        outboundRpc,
      };

      const evalResult: EvaluateResult = await evaluate(evalOptions);

      const duration = Date.now() - startTime;

      if (!evalResult.success) {
        return {
          success: false,
          error: evalResult.error ?? 'Unknown error',
          duration,
          requestId,
        };
      }

      return {
        success: true,
        result: evalResult.value as Result,
        duration,
        requestId,
      };
    } catch (error) {
      const duration = Date.now() - startTime;
      const message = error instanceof Error ? error.message : String(error);

      return {
        success: false,
        error: message,
        duration,
        requestId,
      };
    }
  }

  return {
    async call<Result>(
      name: string,
      params: unknown[],
      executionOptions?: ExecutionOptions
    ): Promise<ExecutionResult<Result>> {
      if (!registry) {
        return {
          success: false,
          error: 'Registry not configured - cannot call procedures by name',
          duration: 0,
          requestId: 'none',
        };
      }

      const procedure = await registry.get(name);
      if (!procedure) {
        return {
          success: false,
          error: `Procedure '${name}' not found`,
          duration: 0,
          requestId: 'none',
        };
      }

      return this.run<Result>(procedure, params, executionOptions);
    },

    async execute<Result>(
      code: string,
      params: unknown[],
      executionOptions?: ExecutionOptions
    ): Promise<ExecutionResult<Result>> {
      return executeCode<Result>(code, params, executionOptions);
    },

    async run<Result>(
      procedure: Procedure,
      params: unknown[],
      executionOptions?: ExecutionOptions
    ): Promise<ExecutionResult<Result>> {
      // Merge procedure-specific options with execution options
      const mergedOptions: ExecutionOptions = {
        timeout: procedure.timeout ?? executionOptions?.timeout ?? defaultTimeout,
        memoryLimit: procedure.memoryLimit ?? executionOptions?.memoryLimit ?? defaultMemoryLimit,
        ...executionOptions,
      };

      return executeCode<Result>(procedure.code, params, mergedOptions);
    },
  };
}

// =============================================================================
// SIMPLE EXECUTOR (FOR TESTING)
// =============================================================================

/**
 * Create a simple executor with in-memory storage for testing
 */
export function createSimpleExecutor<DB extends DatabaseSchema>(
  schema: { [K in keyof DB]: Array<Record<string, unknown>> }
): ProcedureExecutor<DB> {
  // Create in-memory adapters for each table
  // Type assertion: We're building adapters that match the DB schema at runtime
  // but TypeScript can't verify this because DB is generic and schema keys are dynamic
  type DBAdapters = {
    [K in keyof DB]: ReturnType<typeof createInMemoryAdapter<Record<string, unknown>>>
  };
  const adapters = {} as DBAdapters;

  for (const [tableName, data] of Object.entries(schema)) {
    (adapters as Record<string, ReturnType<typeof createInMemoryAdapter>>)[tableName] =
      createInMemoryAdapter(data as Array<Record<string, unknown>>);
  }

  // Create transaction manager
  const transactionManager = createInMemoryTransactionManager();

  // Create SQL executor
  const tableMap = new Map(Object.entries(adapters));
  const sqlExecutor = createInMemorySqlExecutor(tableMap as Map<string, ReturnType<typeof createInMemoryAdapter>>);

  // Create database context
  // Type assertion: adapters is DBAdapters which satisfies the constraint
  // when DB schema types align with the runtime data provided
  const db = createDatabaseContext<DB>({
    adapters: adapters as DatabaseContextOptions<DB>['adapters'],
    sqlExecutor,
    transactionManager,
  });

  return createProcedureExecutor({
    db,
  });
}

// =============================================================================
// PROCEDURE CALL HELPERS
// =============================================================================

/**
 * Helper to create a typed procedure call function
 */
export function createProcedureCall<
  DB extends DatabaseSchema,
  Params extends unknown[],
  Result
>(
  executor: ProcedureExecutor<DB>,
  name: string
): (...params: Params) => Promise<ExecutionResult<Result>> {
  return async (...params: Params) => {
    return executor.call<Result>(name, params);
  };
}

/**
 * Batch execute multiple procedures
 */
export async function batchExecute<DB extends DatabaseSchema>(
  executor: ProcedureExecutor<DB>,
  calls: Array<{ name: string; params: unknown[] }>
): Promise<ExecutionResult<unknown>[]> {
  return Promise.all(
    calls.map(call => executor.call(call.name, call.params))
  );
}

/**
 * Execute procedures sequentially
 */
export async function sequentialExecute<DB extends DatabaseSchema>(
  executor: ProcedureExecutor<DB>,
  calls: Array<{ name: string; params: unknown[] }>
): Promise<ExecutionResult<unknown>[]> {
  const results: ExecutionResult<unknown>[] = [];

  for (const call of calls) {
    const result = await executor.call(call.name, call.params);
    results.push(result);

    // Stop on first failure
    if (!result.success) {
      break;
    }
  }

  return results;
}

// =============================================================================
// MOCK EXECUTOR (FOR UNIT TESTING)
// =============================================================================

/**
 * Create a mock executor that returns predefined results
 */
export function createMockExecutor<DB extends DatabaseSchema>(
  mocks: Record<string, unknown>
): ProcedureExecutor<DB> {
  return {
    async call<Result>(name: string, params: unknown[]): Promise<ExecutionResult<Result>> {
      if (name in mocks) {
        const mockValue = mocks[name];
        const result = typeof mockValue === 'function'
          ? await (mockValue as (...args: unknown[]) => Promise<Result>)(...params)
          : mockValue as Result;

        return {
          success: true,
          result,
          duration: 0,
          requestId: 'mock',
        };
      }

      return {
        success: false,
        error: `Mock not found for procedure '${name}'`,
        duration: 0,
        requestId: 'mock',
      };
    },

    async execute<Result>(_code: string, _params: unknown[]): Promise<ExecutionResult<Result>> {
      return {
        success: false,
        error: 'Mock executor does not support execute()',
        duration: 0,
        requestId: 'mock',
      };
    },

    async run<Result>(procedure: Procedure, params: unknown[]): Promise<ExecutionResult<Result>> {
      return this.call<Result>(procedure.name, params);
    },
  };
}
