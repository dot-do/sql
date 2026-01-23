/**
 * ESM Stored Procedure Types
 *
 * Defines the type system for ESM modules executed as stored procedures
 * in sandboxed V8 isolates.
 */

// Re-export schema types from parser
export type { DatabaseSchema, TableSchema } from '../parser.js';
import type { DatabaseSchema, TableSchema } from '../parser.js';

// =============================================================================
// SCHEMA TYPES
// =============================================================================

/**
 * JSON Schema-like type for validating procedure inputs/outputs
 *
 * @example
 * ```ts
 * // Simple string schema
 * const nameSchema: Schema = { type: 'string', description: 'User name' };
 *
 * // Object schema with required fields
 * const userSchema: Schema = {
 *   type: 'object',
 *   properties: {
 *     id: { type: 'number' },
 *     name: { type: 'string' },
 *     email: { type: 'string', nullable: true }
 *   },
 *   required: ['id', 'name']
 * };
 *
 * // Array schema
 * const tagsSchema: Schema = {
 *   type: 'array',
 *   items: { type: 'string' }
 * };
 * ```
 */
export interface Schema {
  type: 'string' | 'number' | 'boolean' | 'object' | 'array' | 'null' | 'any';
  properties?: Record<string, Schema>;
  items?: Schema;
  required?: string[];
  nullable?: boolean;
  description?: string;
}

/**
 * Convert Schema to TypeScript type
 * Note: Renamed to ProcSchemaToType to avoid conflict with sources module
 */
export type ProcSchemaToType<S extends Schema> =
  S['type'] extends 'string' ? string :
  S['type'] extends 'number' ? number :
  S['type'] extends 'boolean' ? boolean :
  S['type'] extends 'null' ? null :
  S['type'] extends 'any' ? unknown :
  S['type'] extends 'array' ? S extends { items: infer I extends Schema } ? ProcSchemaToType<I>[] : unknown[] :
  S['type'] extends 'object' ? S extends { properties: infer P extends Record<string, Schema> }
    ? { [K in keyof P]: ProcSchemaToType<P[K]> }
    : Record<string, unknown>
  : unknown;

// =============================================================================
// TABLE ACCESSOR TYPES
// =============================================================================

/**
 * Predicate function for filtering records
 */
export type Predicate<T> = (record: T) => boolean;

/**
 * Query options for table operations
 *
 * @example
 * ```ts
 * // Paginated query with sorting
 * const options: QueryOptions = {
 *   limit: 10,
 *   offset: 20,
 *   orderBy: 'createdAt',
 *   orderDirection: 'desc'
 * };
 *
 * // Simple limit
 * const topTen: QueryOptions = { limit: 10 };
 * ```
 */
export interface QueryOptions {
  limit?: number;
  offset?: number;
  orderBy?: string;
  orderDirection?: 'asc' | 'desc';
}

/**
 * Table accessor interface - provides CRUD operations for a table
 *
 * @example
 * ```ts
 * interface User { id: number; name: string; email: string; }
 *
 * // Get by primary key
 * const user = await tables.users.get(123);
 *
 * // Query with predicate function
 * const admins = await tables.users.where(
 *   u => u.role === 'admin',
 *   { limit: 10, orderBy: 'name' }
 * );
 *
 * // Query with partial object filter
 * const activeUsers = await tables.users.where({ status: 'active' });
 *
 * // Insert new record
 * const newUser = await tables.users.insert({ name: 'Alice', email: 'alice@example.com' });
 *
 * // Update matching records
 * const updated = await tables.users.update({ status: 'inactive' }, { status: 'active' });
 *
 * // Delete matching records
 * const deleted = await tables.users.delete(u => u.deletedAt !== null);
 * ```
 */
export interface TableAccessor<T extends Record<string, unknown>> {
  /**
   * Get a single record by primary key
   */
  get(key: string | number): Promise<T | undefined>;

  /**
   * Get all records matching a predicate or filter object
   */
  where(predicate: Predicate<T> | Partial<T>, options?: QueryOptions): Promise<T[]>;

  /**
   * Get all records from the table
   */
  all(options?: QueryOptions): Promise<T[]>;

  /**
   * Count records matching a predicate or filter object
   */
  count(predicate?: Predicate<T> | Partial<T>): Promise<number>;

  /**
   * Insert a new record
   */
  insert(record: Omit<T, 'id'>): Promise<T>;

  /**
   * Update records matching a predicate
   */
  update(predicate: Predicate<T> | Partial<T>, changes: Partial<T>): Promise<number>;

  /**
   * Delete records matching a predicate
   */
  delete(predicate: Predicate<T> | Partial<T>): Promise<number>;
}

// =============================================================================
// DATABASE CONTEXT TYPES
// =============================================================================

/**
 * Convert table schema to record type
 */
export type TableSchemaToRecord<T extends TableSchema> = {
  [K in keyof T]: T[K] extends 'string' ? string :
                  T[K] extends 'number' ? number :
                  T[K] extends 'boolean' ? boolean :
                  T[K] extends 'Date' ? Date :
                  T[K] extends 'null' ? null :
                  unknown
};

/**
 * Database accessor type - provides table accessors for all tables
 */
export type DatabaseAccessor<DB extends DatabaseSchema> = {
  [K in keyof DB]: TableAccessor<TableSchemaToRecord<DB[K]>>
};

/**
 * SQL template literal function type
 */
export type SqlFunction = <Result = unknown>(
  strings: TemplateStringsArray,
  ...values: unknown[]
) => Promise<Result[]>;

/**
 * Transaction function type
 */
export type TransactionFunction<DB extends DatabaseSchema> = <T>(
  callback: (tx: TransactionContext<DB>) => Promise<T>
) => Promise<T>;

/**
 * Transaction context - provides database access within a transaction
 *
 * @example
 * ```ts
 * await ctx.db.transaction(async (tx) => {
 *   // Use table accessors within transaction
 *   const user = await tx.tables.users.get(userId);
 *
 *   // Execute raw SQL
 *   await tx.sql`UPDATE accounts SET balance = balance - ${amount} WHERE user_id = ${userId}`;
 *
 *   // Rollback on validation failure
 *   if (user.balance < amount) {
 *     await tx.rollback();
 *     return;
 *   }
 *
 *   // Auto-commits at end of callback
 * });
 * ```
 */
export interface TransactionContext<DB extends DatabaseSchema> {
  /** Table accessors within transaction */
  tables: DatabaseAccessor<DB>;
  /** Execute raw SQL within transaction */
  sql: SqlFunction;
  /** Commit the transaction (automatic at end of callback) */
  commit(): Promise<void>;
  /** Rollback the transaction */
  rollback(): Promise<void>;
}

/**
 * Database context provided to procedures
 *
 * @example
 * ```ts
 * // Access tables directly
 * const users = await ctx.db.tables.users.all({ limit: 100 });
 *
 * // Execute raw SQL with template literals
 * const results = await ctx.db.sql<{ count: number }>`
 *   SELECT COUNT(*) as count FROM orders WHERE status = ${'pending'}
 * `;
 *
 * // Use transactions for atomic operations
 * await ctx.db.transaction(async (tx) => {
 *   await tx.tables.orders.insert({ userId: 1, total: 99.99 });
 *   await tx.sql`UPDATE users SET order_count = order_count + 1 WHERE id = 1`;
 * });
 * ```
 */
export interface DatabaseContext<DB extends DatabaseSchema = DatabaseSchema> {
  /** Table accessors keyed by table name */
  tables: DatabaseAccessor<DB>;

  /** Execute raw SQL queries */
  sql: SqlFunction;

  /** Execute within a transaction */
  transaction: TransactionFunction<DB>;
}

// =============================================================================
// PROCEDURE CONTEXT
// =============================================================================

/**
 * Environment variables available to procedures
 */
export type ProcedureEnv = Record<string, string>;

/**
 * Full context provided to procedure execution
 *
 * @example
 * ```ts
 * // Procedure handler using context
 * export default async function handler(ctx: ProcedureContext<MyDB, [string, number]>) {
 *   const [userId, amount] = ctx.params;
 *
 *   // Access database
 *   const user = await ctx.db.tables.users.get(userId);
 *
 *   // Use environment variables
 *   const apiKey = ctx.env.EXTERNAL_API_KEY;
 *
 *   // Log with request tracing
 *   console.log(`[${ctx.requestId}] Processing at ${ctx.timestamp.toISOString()}`);
 *
 *   return { success: true, user };
 * }
 * ```
 */
export interface ProcedureContext<
  DB extends DatabaseSchema = DatabaseSchema,
  Params extends unknown[] = unknown[]
> {
  /** Database access interface */
  db: DatabaseContext<DB>;

  /** Input parameters passed to the procedure */
  params: Params;

  /** Environment variables */
  env: ProcedureEnv;

  /** Request ID for tracing */
  requestId: string;

  /** Execution timestamp */
  timestamp: Date;
}

// =============================================================================
// PROCEDURE DEFINITION
// =============================================================================

/**
 * Procedure metadata
 *
 * @example
 * ```ts
 * const metadata: ProcedureMetadata = {
 *   name: 'processOrder',
 *   version: 3,
 *   createdAt: new Date('2024-01-15'),
 *   updatedAt: new Date('2024-02-20'),
 *   description: 'Processes an order and updates inventory',
 *   author: 'team-orders',
 *   tags: ['orders', 'inventory', 'critical']
 * };
 * ```
 */
export interface ProcedureMetadata {
  /** Procedure name */
  name: string;

  /** Version number (auto-incremented) */
  version: number;

  /** Creation timestamp */
  createdAt: Date;

  /** Last modification timestamp */
  updatedAt: Date;

  /** Procedure description */
  description?: string;

  /** Author/owner */
  author?: string;

  /** Tags for categorization */
  tags?: string[];
}

/**
 * Procedure definition stored in the registry
 *
 * @example
 * ```ts
 * const procedure: Procedure = {
 *   name: 'getUserOrders',
 *   code: `
 *     export default async function(ctx) {
 *       const [userId] = ctx.params;
 *       return await ctx.db.tables.orders.where({ userId });
 *     }
 *   `,
 *   inputSchema: {
 *     type: 'array',
 *     items: { type: 'number' }
 *   },
 *   outputSchema: {
 *     type: 'array',
 *     items: { type: 'object' }
 *   },
 *   metadata: {
 *     name: 'getUserOrders',
 *     version: 1,
 *     createdAt: new Date(),
 *     updatedAt: new Date(),
 *     description: 'Fetches all orders for a user'
 *   },
 *   timeout: 5000,
 *   memoryLimit: 128
 * };
 * ```
 */
export interface Procedure<
  InputSchema extends Schema | undefined = Schema | undefined,
  OutputSchema extends Schema | undefined = Schema | undefined
> {
  /** Procedure name (unique identifier) */
  name: string;

  /** ESM module code */
  code: string;

  /** Schema for input parameters validation */
  inputSchema?: InputSchema;

  /** Schema for return value validation */
  outputSchema?: OutputSchema;

  /** Procedure metadata */
  metadata: ProcedureMetadata;

  /** Execution timeout in milliseconds */
  timeout?: number;

  /** Memory limit in MB */
  memoryLimit?: number;
}

/**
 * The signature of a procedure module's default export
 */
export type ProcedureHandler<
  DB extends DatabaseSchema = DatabaseSchema,
  Params extends unknown[] = unknown[],
  Result = unknown
> = (ctx: ProcedureContext<DB, Params>, ...args: Params) => Promise<Result>;

// =============================================================================
// PROCEDURE EXECUTION
// =============================================================================

/**
 * Options for procedure execution
 *
 * @example
 * ```ts
 * // Execute with custom timeout and memory limit
 * const options: ExecutionOptions = {
 *   timeout: 10000,      // 10 seconds
 *   memoryLimit: 256,    // 256 MB
 *   debug: true,
 *   env: {
 *     API_ENDPOINT: 'https://api.example.com',
 *     LOG_LEVEL: 'debug'
 *   }
 * };
 *
 * const result = await executor.run('myProcedure', [arg1, arg2], options);
 * ```
 */
export interface ExecutionOptions {
  /** Execution timeout in milliseconds */
  timeout?: number;

  /** Memory limit in MB */
  memoryLimit?: number;

  /** Enable debug logging */
  debug?: boolean;

  /** Additional environment variables */
  env?: ProcedureEnv;
}

/**
 * Result of procedure execution
 *
 * @example
 * ```ts
 * // Successful execution
 * const successResult: ExecutionResult<User[]> = {
 *   success: true,
 *   result: [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }],
 *   duration: 45,
 *   memoryUsed: 1024000,
 *   requestId: 'req-abc123'
 * };
 *
 * // Failed execution
 * const errorResult: ExecutionResult = {
 *   success: false,
 *   error: 'Timeout exceeded: procedure ran longer than 5000ms',
 *   duration: 5001,
 *   requestId: 'req-def456'
 * };
 *
 * // Handle result
 * if (result.success) {
 *   console.log('Got', result.result);
 * } else {
 *   console.error('Failed:', result.error);
 * }
 * ```
 */
export interface ExecutionResult<T = unknown> {
  /** Whether execution succeeded */
  success: boolean;

  /** Return value from procedure (if successful) */
  result?: T;

  /** Error message (if failed) */
  error?: string;

  /** Execution duration in milliseconds */
  duration: number;

  /** Memory used in bytes */
  memoryUsed?: number;

  /** Request ID for tracing */
  requestId: string;
}

// =============================================================================
// TYPE-LEVEL UTILITIES
// =============================================================================

/**
 * Extract the return type from procedure code (type-level approximation)
 * This is a simplified version - full inference requires actual code analysis
 */
export type InferProcedureResult<Code extends string> =
  // Check for explicit return type annotation
  Code extends `${string}): Promise<${infer ReturnType}>${string}`
    ? InferTypeFromString<ReturnType>
  : Code extends `${string}): ${infer ReturnType}${string}`
    ? InferTypeFromString<ReturnType>
  : unknown;

/**
 * Map type string to TypeScript type
 */
type InferTypeFromString<T extends string> =
  T extends 'string' ? string :
  T extends 'number' ? number :
  T extends 'boolean' ? boolean :
  T extends 'void' ? void :
  T extends 'null' ? null :
  T extends 'undefined' ? undefined :
  T extends `${infer U}[]` ? InferTypeFromString<U>[] :
  T extends `Array<${infer U}>` ? InferTypeFromString<U>[] :
  T extends `Promise<${infer U}>` ? InferTypeFromString<U> :
  unknown;

/**
 * Procedure definition with inferred types
 */
export type TypedProcedure<
  Code extends string,
  InputSchema extends Schema | undefined = undefined,
  OutputSchema extends Schema | undefined = undefined
> = Procedure<InputSchema, OutputSchema> & {
  code: Code;
  __inferredResult?: InferProcedureResult<Code>;
};

// =============================================================================
// PROCEDURE REGISTRY TYPES
// =============================================================================

/**
 * Registry entry for a stored procedure
 *
 * @example
 * ```ts
 * const entry: RegistryEntry = {
 *   current: {
 *     name: 'calculateTotal',
 *     code: 'export default async (ctx) => { ... }',
 *     metadata: { name: 'calculateTotal', version: 3, createdAt: new Date(), updatedAt: new Date() }
 *   },
 *   versions: [
 *     { version: 1, procedure: { ... }, timestamp: new Date('2024-01-01') },
 *     { version: 2, procedure: { ... }, timestamp: new Date('2024-01-15') },
 *     { version: 3, procedure: { ... }, timestamp: new Date('2024-02-01') }
 *   ]
 * };
 *
 * // Get previous version
 * const v2 = entry.versions.find(v => v.version === 2)?.procedure;
 * ```
 */
export interface RegistryEntry {
  /** Current procedure definition */
  current: Procedure;

  /** Version history */
  versions: Array<{
    version: number;
    procedure: Procedure;
    timestamp: Date;
  }>;
}

/**
 * Procedure registry interface
 *
 * @example
 * ```ts
 * // Register a new procedure
 * const proc = await registry.register({
 *   name: 'sendNotification',
 *   code: 'export default async (ctx) => { ... }',
 *   metadata: { description: 'Sends push notifications' }
 * });
 *
 * // Get procedure by name (latest version)
 * const latest = await registry.get('sendNotification');
 *
 * // Get specific version
 * const v2 = await registry.get('sendNotification', 2);
 *
 * // List all procedures
 * const allProcs = await registry.list();
 *
 * // Get version history
 * const history = await registry.history('sendNotification');
 * console.log(`${history.length} versions available`);
 *
 * // Delete procedure
 * await registry.delete('sendNotification');
 * ```
 */
export interface ProcedureRegistry {
  /**
   * Register a new procedure or update existing
   */
  register(procedure: Omit<Procedure, 'metadata'> & { metadata?: Partial<ProcedureMetadata> }): Promise<Procedure>;

  /**
   * Get a procedure by name
   */
  get(name: string, version?: number): Promise<Procedure | undefined>;

  /**
   * List all procedures
   */
  list(): Promise<Procedure[]>;

  /**
   * Delete a procedure
   */
  delete(name: string): Promise<boolean>;

  /**
   * Get version history for a procedure
   */
  history(name: string): Promise<RegistryEntry['versions']>;
}

// =============================================================================
// PARSED PROCEDURE TYPES
// =============================================================================

/**
 * Result of parsing CREATE PROCEDURE statement
 *
 * @example
 * ```ts
 * // Parsed from: CREATE PROCEDURE getUser(userId: number): User AS $$...$$
 * const parsed: ParsedProcedure = {
 *   name: 'getUser',
 *   code: 'export default async (ctx) => { return ctx.db.tables.users.get(ctx.params[0]); }',
 *   parameters: [
 *     { name: 'userId', type: 'number' }
 *   ],
 *   returnType: 'User',
 *   rawSql: 'CREATE PROCEDURE getUser(userId: number): User AS $$...$$'
 * };
 *
 * // Parsed with default values
 * const withDefaults: ParsedProcedure = {
 *   name: 'searchUsers',
 *   code: '...',
 *   parameters: [
 *     { name: 'query', type: 'string' },
 *     { name: 'limit', type: 'number', defaultValue: '10' }
 *   ],
 *   rawSql: '...'
 * };
 * ```
 */
export interface ParsedProcedure {
  /** Procedure name */
  name: string;

  /** ESM module code */
  code: string;

  /** Parsed parameter definitions */
  parameters?: Array<{
    name: string;
    type: string;
    defaultValue?: string;
  }>;

  /** Return type annotation */
  returnType?: string;

  /** Raw SQL statement */
  rawSql: string;
}

/**
 * Error from parsing CREATE PROCEDURE
 *
 * @example
 * ```ts
 * // Syntax error at specific location
 * const syntaxError: ParseError = {
 *   message: 'Unexpected token ";"',
 *   position: 45,
 *   line: 3,
 *   column: 12
 * };
 *
 * // General parse error
 * const generalError: ParseError = {
 *   message: 'Missing procedure body'
 * };
 *
 * // Format error message
 * function formatError(err: ParseError): string {
 *   if (err.line && err.column) {
 *     return `Error at line ${err.line}, column ${err.column}: ${err.message}`;
 *   }
 *   return err.message;
 * }
 * ```
 */
export interface ParseError {
  message: string;
  position?: number;
  line?: number;
  column?: number;
}
