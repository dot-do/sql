/**
 * Functional Stored Procedure API
 *
 * Provides a TypeScript-native way to define stored procedures with full type inference.
 * Params come first, context comes last.
 *
 * @example
 * ```typescript
 * const procedures = defineProcedures({
 *   calculateScore: async (userId: string, { db }) => {
 *     const orders = await db.orders.where({ userId });
 *     return orders.reduce((sum, o) => sum + o.totalAmount, 0);
 *   },
 *
 *   transferFunds: async (fromId: string, toId: string, amount: number, { db }) => {
 *     return db.transaction(async (tx) => {
 *       await tx.accounts.update(fromId, { balance: sql`balance - ${amount}` });
 *       await tx.accounts.update(toId, { balance: sql`balance + ${amount}` });
 *       return { success: true, amount };
 *     });
 *   },
 * });
 *
 * // Call with full type safety
 * const score = await procedures.calculateScore('user-123');
 * // ^? number
 * ```
 *
 * @packageDocumentation
 */

import type {
  DatabaseSchema,
  DatabaseContext,
  TableAccessor,
  TableSchemaToRecord,
  TransactionContext,
  SqlFunction,
  ProcedureRegistry,
  Procedure,
} from './types.js';

// =============================================================================
// PROCEDURE CONTEXT TYPES
// =============================================================================

/**
 * The context object passed as the last argument to procedure functions.
 * Use destructuring: `{ db }` to access the database context.
 */
export interface FunctionalContext<DB extends DatabaseSchema = DatabaseSchema> {
  /** Database access interface with typed table accessors */
  db: FunctionalDb<DB>;

  /** Environment variables */
  env?: Record<string, string>;

  /** Request ID for tracing */
  requestId?: string;
}

/**
 * Database interface for functional procedures.
 * Tables are accessible directly on db (e.g., db.users instead of db.tables.users).
 */
export type FunctionalDb<DB extends DatabaseSchema> = {
  /** Table accessors keyed by table name */
  [K in keyof DB]: TableAccessor<TableSchemaToRecord<DB[K]>>
} & {
  /** Execute raw SQL queries via template literal */
  sql: SqlFunction;

  /** Execute within a transaction */
  transaction: <T>(callback: (tx: TransactionContext<DB>) => Promise<T>) => Promise<T>;
};

// =============================================================================
// TYPE INFERENCE UTILITIES
// =============================================================================

/**
 * Extracts all leading parameters from a function type, excluding the last context parameter.
 * Handles functions with 0 params (just context) up to many params.
 */
type ExtractParams<F> = F extends (ctx: FunctionalContext<any>) => any
  ? [] // No params, just context
  : F extends (arg1: infer A1, ctx: FunctionalContext<any>) => any
  ? [A1] // 1 param
  : F extends (arg1: infer A1, arg2: infer A2, ctx: FunctionalContext<any>) => any
  ? [A1, A2] // 2 params
  : F extends (arg1: infer A1, arg2: infer A2, arg3: infer A3, ctx: FunctionalContext<any>) => any
  ? [A1, A2, A3] // 3 params
  : F extends (arg1: infer A1, arg2: infer A2, arg3: infer A3, arg4: infer A4, ctx: FunctionalContext<any>) => any
  ? [A1, A2, A3, A4] // 4 params
  : F extends (arg1: infer A1, arg2: infer A2, arg3: infer A3, arg4: infer A4, arg5: infer A5, ctx: FunctionalContext<any>) => any
  ? [A1, A2, A3, A4, A5] // 5 params
  : F extends (arg1: infer A1, arg2: infer A2, arg3: infer A3, arg4: infer A4, arg5: infer A5, arg6: infer A6, ctx: FunctionalContext<any>) => any
  ? [A1, A2, A3, A4, A5, A6] // 6 params
  : unknown[];

/**
 * Extracts the return type from a procedure function.
 */
type ExtractReturn<F> = F extends (...args: any[]) => infer R
  ? R extends Promise<infer T> ? T : R
  : never;

/**
 * A procedure definition function - params first, context last.
 */
export type ProcedureDef<
  Args extends unknown[] = unknown[],
  R = unknown,
  DB extends DatabaseSchema = DatabaseSchema
> = (...args: [...Args, FunctionalContext<DB>]) => R | Promise<R>;

/**
 * Converts a procedure definition to its callable form (without context).
 */
export type InferProcedureCall<P, DB extends DatabaseSchema = DatabaseSchema> =
  P extends ProcedureDef<infer Args, infer R, DB>
    ? (...args: Args) => Promise<Awaited<R>>
    : never;

/**
 * Type for procedure definitions map.
 */
export type ProcedureDefinitions<DB extends DatabaseSchema = DatabaseSchema> = {
  [name: string]: ProcedureDef<any[], any, DB>;
};

/**
 * Converts procedure definitions to callable procedures.
 */
export type InferProcedures<
  Defs extends ProcedureDefinitions<DB>,
  DB extends DatabaseSchema = DatabaseSchema
> = {
  [K in keyof Defs]: InferProcedureCall<Defs[K], DB>;
};

// =============================================================================
// IMPLEMENTATION
// =============================================================================

/**
 * Options for defining procedures.
 */
export interface DefineProceduresOptions<DB extends DatabaseSchema = DatabaseSchema> {
  /** Database context to use for execution */
  db: FunctionalDb<DB>;

  /** Optional procedure registry for persistence */
  registry?: ProcedureRegistry;

  /** Base environment variables */
  env?: Record<string, string>;
}

/**
 * Define multiple procedures with full type inference.
 *
 * @example
 * ```typescript
 * interface MyDB extends DatabaseSchema {
 *   users: { id: 'number'; name: 'string'; email: 'string' };
 *   orders: { id: 'number'; userId: 'number'; totalAmount: 'number' };
 * }
 *
 * const procedures = defineProcedures<MyDB>(
 *   {
 *     getUser: async (id: number, { db }) => {
 *       return db.users.get(id);
 *     },
 *
 *     calculateTotal: async (userId: number, { db }) => {
 *       const orders = await db.orders.where({ userId });
 *       return orders.reduce((sum, o) => sum + o.totalAmount, 0);
 *     },
 *
 *     transferFunds: async (from: string, to: string, amount: number, { db }) => {
 *       return db.transaction(async (tx) => {
 *         // Transaction logic
 *         return { success: true };
 *       });
 *     },
 *   },
 *   { db }
 * );
 *
 * // Call with type safety
 * const user = await procedures.getUser(1);
 * // ^? { id: number; name: string; email: string } | undefined
 *
 * const total = await procedures.calculateTotal(1);
 * // ^? number
 * ```
 */
export function defineProcedures<
  DB extends DatabaseSchema,
  Defs extends ProcedureDefinitions<DB>
>(
  definitions: Defs,
  options: DefineProceduresOptions<DB>
): InferProcedures<Defs, DB> {
  const { db, registry, env = {} } = options;

  // Create the context that will be passed to each procedure
  const createContext = (): FunctionalContext<DB> => ({
    db,
    env,
    requestId: `req_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`,
  });

  // Build the callable procedures object
  const procedures: Record<string, (...args: unknown[]) => Promise<unknown>> = {};

  for (const [name, handler] of Object.entries(definitions)) {
    procedures[name] = async (...args: unknown[]) => {
      const ctx = createContext();
      // Call the handler with args followed by context
      return handler(...args, ctx);
    };

    // Optionally register with the procedure registry
    if (registry) {
      // Convert to internal Procedure format
      const code = generateProcedureCode(name, handler);
      registry.register({
        name,
        code,
        metadata: {
          description: `Functional procedure: ${name}`,
        },
      }).catch(err => {
        console.warn(`Failed to register procedure '${name}':`, err);
      });
    }
  }

  return procedures as InferProcedures<Defs, DB>;
}

/**
 * Generate ESM code representation of a functional procedure.
 * This is used when integrating with the existing registry system.
 */
function generateProcedureCode(name: string, handler: Function): string {
  // Get the function source and convert to ESM format
  const source = handler.toString();

  return `
// Generated from functional procedure: ${name}
// Original function signature is preserved for type safety

export default async function ${name}(ctx, ...args) {
  // The functional handler expects params first, context last
  const originalHandler = ${source};
  return originalHandler(...args, ctx);
}
`;
}

// =============================================================================
// SINGLE PROCEDURE HELPERS
// =============================================================================

/**
 * Define a single procedure with full type inference.
 *
 * @example
 * ```typescript
 * const getUser = defineProcedure<MyDB>()(
 *   'getUser',
 *   async (id: number, { db }) => db.users.get(id),
 *   { db }
 * );
 *
 * const user = await getUser(1);
 * ```
 */
export function defineProcedure<DB extends DatabaseSchema>() {
  return function <Args extends unknown[], R>(
    name: string,
    handler: (...args: [...Args, FunctionalContext<DB>]) => R | Promise<R>,
    options: DefineProceduresOptions<DB>
  ): (...args: Args) => Promise<Awaited<R>> {
    const { db, registry, env = {} } = options;

    // Create the context that will be passed to the procedure
    const createContext = (): FunctionalContext<DB> => ({
      db,
      env,
      requestId: `req_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`,
    });

    // Create the callable procedure function
    const procedure = async (...args: Args): Promise<Awaited<R>> => {
      const ctx = createContext();
      // Call the handler with args followed by context
      return handler(...args, ctx) as Promise<Awaited<R>>;
    };

    // Optionally register with the procedure registry
    if (registry) {
      const code = generateProcedureCode(name, handler);
      registry.register({
        name,
        code,
        metadata: {
          description: `Functional procedure: ${name}`,
        },
      }).catch(err => {
        console.warn(`Failed to register procedure '${name}':`, err);
      });
    }

    return procedure;
  };
}

// =============================================================================
// TYPE HELPERS FOR PROCEDURE DEFINITIONS
// =============================================================================

/**
 * Helper type to create a properly typed procedure definition.
 * Use this when you need to explicitly type a procedure function.
 *
 * @example
 * ```typescript
 * const handler: Proc<[string, number], boolean, MyDB> = async (
 *   userId,
 *   amount,
 *   { db }
 * ) => {
 *   // userId is string, amount is number
 *   return true;
 * };
 * ```
 */
export type Proc<
  Args extends unknown[],
  R,
  DB extends DatabaseSchema = DatabaseSchema
> = (...args: [...Args, FunctionalContext<DB>]) => R | Promise<R>;

/**
 * Helper type for procedures that return void.
 */
export type VoidProc<
  Args extends unknown[],
  DB extends DatabaseSchema = DatabaseSchema
> = Proc<Args, void, DB>;

/**
 * Helper type for procedures that take no parameters (only context).
 */
export type NoParamsProc<
  R,
  DB extends DatabaseSchema = DatabaseSchema
> = Proc<[], R, DB>;

// =============================================================================
// ASYNC ITERATION / STREAMING SUPPORT
// =============================================================================

/**
 * A procedure that returns an async iterable for streaming results.
 */
export type StreamingProc<
  Args extends unknown[],
  T,
  DB extends DatabaseSchema = DatabaseSchema
> = (...args: [...Args, FunctionalContext<DB>]) => AsyncIterable<T>;

/**
 * Define a streaming procedure that yields results over time.
 *
 * @example
 * ```typescript
 * const streamUsers = defineStreamingProcedure<MyDB>()(
 *   'streamUsers',
 *   async function* (batchSize: number, { db }) {
 *     let offset = 0;
 *     while (true) {
 *       const batch = await db.users.all({ limit: batchSize, offset });
 *       if (batch.length === 0) break;
 *       for (const user of batch) {
 *         yield user;
 *       }
 *       offset += batchSize;
 *     }
 *   },
 *   { db }
 * );
 *
 * for await (const user of streamUsers(100)) {
 *   console.log(user.name);
 * }
 * ```
 */
export function defineStreamingProcedure<DB extends DatabaseSchema>() {
  return function <Args extends unknown[], T>(
    _name: string,
    handler: (...args: [...Args, FunctionalContext<DB>]) => AsyncIterable<T>,
    options: DefineProceduresOptions<DB>
  ): (...args: Args) => AsyncIterable<T> {
    const { db, env = {} } = options;

    return (...args: Args): AsyncIterable<T> => {
      const ctx: FunctionalContext<DB> = {
        db,
        env,
        requestId: `req_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`,
      };

      // Return the async iterable from the handler directly
      return handler(...args, ctx);
    };
  };
}

// =============================================================================
// MIDDLEWARE SUPPORT
// =============================================================================

/**
 * Middleware function type for procedures.
 */
export type ProcedureMiddleware<DB extends DatabaseSchema = DatabaseSchema> = (
  ctx: FunctionalContext<DB>,
  next: () => Promise<unknown>
) => Promise<unknown>;

/**
 * Options for procedures with middleware.
 */
export interface DefineProceduresWithMiddlewareOptions<DB extends DatabaseSchema>
  extends DefineProceduresOptions<DB> {
  /** Middleware functions to wrap all procedures */
  middleware?: ProcedureMiddleware<DB>[];
}

/**
 * Define procedures with middleware support.
 *
 * @example
 * ```typescript
 * const procedures = defineProceduresWithMiddleware<MyDB>(
 *   {
 *     sensitiveOperation: async (id: string, { db }) => {
 *       // This will have logging and timing middleware applied
 *       return db.users.get(id);
 *     },
 *   },
 *   {
 *     db,
 *     middleware: [
 *       // Logging middleware
 *       async (ctx, next) => {
 *         console.log(`[${ctx.requestId}] Starting procedure`);
 *         const result = await next();
 *         console.log(`[${ctx.requestId}] Completed`);
 *         return result;
 *       },
 *       // Timing middleware
 *       async (ctx, next) => {
 *         const start = Date.now();
 *         const result = await next();
 *         console.log(`Duration: ${Date.now() - start}ms`);
 *         return result;
 *       },
 *     ],
 *   }
 * );
 * ```
 */
export function defineProceduresWithMiddleware<
  DB extends DatabaseSchema,
  Defs extends ProcedureDefinitions<DB>
>(
  definitions: Defs,
  options: DefineProceduresWithMiddlewareOptions<DB>
): InferProcedures<Defs, DB> {
  const { db, middleware = [], env = {} } = options;

  const createContext = (): FunctionalContext<DB> => ({
    db,
    env,
    requestId: `req_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`,
  });

  const procedures: Record<string, (...args: unknown[]) => Promise<unknown>> = {};

  for (const [name, handler] of Object.entries(definitions)) {
    procedures[name] = async (...args: unknown[]) => {
      const ctx = createContext();

      // Build the middleware chain
      let index = -1;

      const runNext = async (): Promise<unknown> => {
        index++;
        if (index < middleware.length) {
          return middleware[index](ctx, runNext);
        }
        // End of middleware chain - call the actual handler
        return handler(...args, ctx);
      };

      return runNext();
    };
  }

  return procedures as InferProcedures<Defs, DB>;
}

// =============================================================================
// VALIDATION HELPERS
// =============================================================================

/**
 * A validated procedure that throws if validation fails.
 */
export type ValidatedProc<
  Args extends unknown[],
  R,
  DB extends DatabaseSchema = DatabaseSchema
> = Proc<Args, R, DB>;

/**
 * Validation function type.
 */
export type Validator<T> = (value: unknown) => value is T;

/**
 * Create a validated procedure wrapper.
 *
 * @example
 * ```typescript
 * const validateUserId = (v: unknown): v is string =>
 *   typeof v === 'string' && v.length > 0;
 *
 * const getUser = withValidation(
 *   async (userId: string, { db }) => db.users.get(userId),
 *   [validateUserId]
 * );
 * ```
 */
export function withValidation<
  Args extends unknown[],
  R,
  DB extends DatabaseSchema
>(
  handler: Proc<Args, R, DB>,
  validators: Array<Validator<unknown> | undefined>
): Proc<Args, R, DB> {
  return (async (...args: [...Args, FunctionalContext<DB>]) => {
    // Extract context (last arg) and actual args
    const ctx = args[args.length - 1] as FunctionalContext<DB>;
    const actualArgs = args.slice(0, -1) as unknown[];

    // Validate each argument
    for (let i = 0; i < actualArgs.length; i++) {
      const validator = validators[i];
      if (validator && !validator(actualArgs[i])) {
        throw new Error(`Validation failed for argument at index ${i}`);
      }
    }

    return handler(...(actualArgs as Args), ctx);
  }) as Proc<Args, R, DB>;
}

// =============================================================================
// RETRY SUPPORT
// =============================================================================

/**
 * Options for retry wrapper.
 */
export interface RetryOptions {
  /** Maximum number of retry attempts */
  maxAttempts?: number;
  /** Delay between retries in ms */
  delayMs?: number;
  /** Exponential backoff multiplier */
  backoffMultiplier?: number;
  /** Predicate to determine if error is retryable */
  isRetryable?: (error: unknown) => boolean;
}

/**
 * Wrap a procedure with retry logic.
 *
 * @example
 * ```typescript
 * const reliableTransfer = withRetry(
 *   async (from: string, to: string, amount: number, { db }) => {
 *     // May fail due to transient errors
 *     return db.transaction(async (tx) => {...});
 *   },
 *   { maxAttempts: 3, delayMs: 100 }
 * );
 * ```
 */
export function withRetry<
  Args extends unknown[],
  R,
  DB extends DatabaseSchema
>(
  handler: Proc<Args, R, DB>,
  options: RetryOptions = {}
): Proc<Args, R, DB> {
  const {
    maxAttempts = 3,
    delayMs = 100,
    backoffMultiplier = 2,
    isRetryable = () => true,
  } = options;

  return (async (...args: [...Args, FunctionalContext<DB>]) => {
    let lastError: unknown;
    let currentDelay = delayMs;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        // handler expects [...Args, FunctionalContext<DB>] which matches args
        return await handler(...args);
      } catch (error) {
        lastError = error;

        if (attempt === maxAttempts || !isRetryable(error)) {
          throw error;
        }

        // Wait before retrying
        await new Promise(resolve => setTimeout(resolve, currentDelay));
        currentDelay *= backoffMultiplier;
      }
    }

    throw lastError;
  }) as Proc<Args, R, DB>;
}
