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
 */
export interface QueryOptions {
  limit?: number;
  offset?: number;
  orderBy?: string;
  orderDirection?: 'asc' | 'desc';
}

/**
 * Table accessor interface - provides CRUD operations for a table
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
 */
export interface ParseError {
  message: string;
  position?: number;
  line?: number;
  column?: number;
}
