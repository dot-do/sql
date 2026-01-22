/**
 * DoSQL ATTACH/DETACH Database Support - Types
 *
 * Defines types for multi-database support following SQLite-style ATTACH semantics:
 * - ATTACH DATABASE 'path' AS alias
 * - DETACH DATABASE alias
 * - Cross-database queries: SELECT * FROM db1.table1 JOIN db2.table2
 * - Schema qualification: db.table.column
 */

import type { TableSchema, Schema } from '../engine/types.js';

// =============================================================================
// DATABASE ATTACHMENT TYPES
// =============================================================================

/**
 * Supported database connection types
 */
export type DatabaseConnectionType =
  | 'memory'      // In-memory database
  | 'file'        // Local file (for testing)
  | 'r2'          // R2 bucket storage
  | 'durable'     // Durable Object storage
  | 'kv'          // Workers KV
  | 'd1'          // Cloudflare D1
  | 'remote';     // Remote database connection

/**
 * Database connection options
 */
export interface DatabaseConnectionOptions {
  /** Connection type */
  type: DatabaseConnectionType;

  /** Connection path/URI */
  path: string;

  /** Read-only mode */
  readonly?: boolean;

  /** Connection timeout in milliseconds */
  timeout?: number;

  /** R2 bucket binding (for r2 type) */
  r2Bucket?: R2Bucket;

  /** Durable Object binding (for durable type) */
  durableObject?: DurableObjectNamespace;

  /** KV namespace binding (for kv type) */
  kvNamespace?: KVNamespace;

  /** D1 database binding (for d1 type) */
  d1Database?: D1Database;

  /** Custom headers for remote connections */
  headers?: Record<string, string>;

  /** Authentication credentials */
  credentials?: {
    username?: string;
    password?: string;
    token?: string;
  };
}

/**
 * Attached database info
 */
export interface AttachedDatabase {
  /** Database alias (e.g., 'db1', 'sales') */
  alias: string;

  /** Original connection path */
  path: string;

  /** Connection options */
  options: DatabaseConnectionOptions;

  /** Database schema (tables and their definitions) */
  schema: Schema;

  /** Whether this is the main database */
  isMain: boolean;

  /** Whether this is the temp database */
  isTemp: boolean;

  /** Sequence number (order of attachment) */
  seq: number;

  /** Attachment timestamp */
  attachedAt: Date;

  /** Is this database currently connected/available */
  isConnected: boolean;

  /** Is this database read-only */
  isReadOnly: boolean;
}

/**
 * Reserved database aliases that cannot be used for ATTACH
 */
export const RESERVED_DATABASE_ALIASES = ['main', 'temp'] as const;
export type ReservedDatabaseAlias = typeof RESERVED_DATABASE_ALIASES[number];

/**
 * Maximum number of attached databases (SQLite default is 10)
 */
export const MAX_ATTACHED_DATABASES = 10;

// =============================================================================
// SCHEMA QUALIFICATION TYPES
// =============================================================================

/**
 * Fully qualified table reference
 */
export interface QualifiedTableRef {
  /** Database alias (default: 'main') */
  database?: string;

  /** Table name */
  table: string;

  /** Optional alias for the reference in query */
  alias?: string;
}

/**
 * Fully qualified column reference
 */
export interface QualifiedColumnRef {
  /** Database alias (default: determined by context) */
  database?: string;

  /** Table name (if specified) */
  table?: string;

  /** Column name */
  column: string;
}

/**
 * Result of resolving a table reference
 */
export interface ResolvedTableRef {
  /** The database where the table was found */
  database: string;

  /** The table definition */
  tableDef: TableSchema;

  /** Full path for storage access */
  storagePath: string;

  /** Whether this requires cross-database access */
  isCrossDatabase: boolean;
}

/**
 * Result of resolving a column reference
 */
export interface ResolvedColumnRef {
  /** The database containing the column */
  database: string;

  /** The table containing the column */
  table: string;

  /** Column definition */
  columnDef: {
    name: string;
    type: string;
    nullable: boolean;
    primaryKey?: boolean;
  };

  /** Qualified name for output */
  qualifiedName: string;
}

// =============================================================================
// DATABASE MANAGER TYPES
// =============================================================================

/**
 * Options for attaching a database
 */
export interface AttachOptions {
  /** Database alias (required unless using AS clause) */
  alias?: string;

  /** Open in read-only mode */
  readonly?: boolean;

  /** Create if doesn't exist (default: false) */
  createIfNotExists?: boolean;

  /** Connection type override */
  type?: DatabaseConnectionType;

  /** Additional connection options */
  connectionOptions?: Partial<DatabaseConnectionOptions>;
}

/**
 * Result of ATTACH DATABASE operation
 */
export interface AttachResult {
  /** Success status */
  success: boolean;

  /** The attached database info */
  database?: AttachedDatabase;

  /** Error message if failed */
  error?: string;

  /** Warning messages */
  warnings?: string[];
}

/**
 * Result of DETACH DATABASE operation
 */
export interface DetachResult {
  /** Success status */
  success: boolean;

  /** Error message if failed */
  error?: string;

  /** Whether the database was in use by active transactions */
  hadActiveTransactions?: boolean;
}

/**
 * Database list entry (for PRAGMA database_list)
 */
export interface DatabaseListEntry {
  /** Sequence number */
  seq: number;

  /** Database alias */
  name: string;

  /** File path or connection string */
  file: string;
}

// =============================================================================
// TRANSACTION TYPES
// =============================================================================

/**
 * Multi-database transaction scope
 */
export interface MultiDatabaseTransaction {
  /** Transaction ID */
  id: string;

  /** Databases involved in this transaction */
  databases: string[];

  /** Transaction state */
  state: 'active' | 'committing' | 'rolling_back' | 'committed' | 'rolled_back';

  /** Start timestamp */
  startedAt: Date;

  /** Whether this transaction spans multiple databases */
  isCrossDatabase: boolean;

  /** Savepoints (for nested transactions) */
  savepoints: string[];
}

/**
 * Options for cross-database transactions
 */
export interface CrossDatabaseTransactionOptions {
  /** Isolation level */
  isolationLevel?: 'read_uncommitted' | 'read_committed' | 'repeatable_read' | 'serializable';

  /** Timeout for the transaction in milliseconds */
  timeout?: number;

  /** Whether to use two-phase commit for durability */
  twoPhaseCommit?: boolean;

  /** Databases to include in transaction (default: all modified) */
  databases?: string[];
}

// =============================================================================
// QUERY RESOLUTION TYPES
// =============================================================================

/**
 * Query scope for name resolution
 */
export interface QueryScope {
  /** Tables visible in current scope with their aliases */
  tables: Map<string, QualifiedTableRef>;

  /** Column aliases defined in SELECT clause */
  columnAliases: Map<string, QualifiedColumnRef>;

  /** Subquery aliases */
  subqueryAliases: Map<string, Schema>;

  /** Parent scope (for correlated subqueries) */
  parent?: QueryScope;
}

/**
 * Ambiguity detection result
 */
export interface AmbiguityCheck {
  /** Whether the reference is ambiguous */
  isAmbiguous: boolean;

  /** List of possible matches if ambiguous */
  candidates?: Array<{
    database: string;
    table: string;
  }>;

  /** Resolved reference if unambiguous */
  resolved?: ResolvedColumnRef;
}

// =============================================================================
// ERROR TYPES
// =============================================================================

/**
 * Error codes for attach/detach operations
 */
export type AttachErrorCode =
  | 'INVALID_ALIAS'           // Alias is reserved or invalid
  | 'ALIAS_EXISTS'            // Alias already in use
  | 'DATABASE_NOT_FOUND'      // Database path doesn't exist
  | 'MAX_ATTACHED'            // Maximum attached databases exceeded
  | 'CONNECTION_FAILED'       // Failed to connect to database
  | 'PERMISSION_DENIED'       // No permission to access database
  | 'CANNOT_DETACH_MAIN'      // Cannot detach main database
  | 'CANNOT_DETACH_TEMP'      // Cannot detach temp database
  | 'DATABASE_NOT_ATTACHED'   // Database alias not found
  | 'ACTIVE_TRANSACTION'      // Database has active transactions
  | 'SCHEMA_MISMATCH'         // Schema validation failed
  | 'READONLY_VIOLATION'      // Write attempted on readonly database
  | 'AMBIGUOUS_TABLE'         // Table name exists in multiple databases
  | 'AMBIGUOUS_COLUMN'        // Column name exists in multiple tables
  | 'TABLE_NOT_FOUND'         // Table doesn't exist in any database
  | 'COLUMN_NOT_FOUND';       // Column doesn't exist in table

/**
 * Attach/detach error
 */
export interface AttachError {
  code: AttachErrorCode;
  message: string;
  details?: {
    alias?: string;
    path?: string;
    databases?: string[];
    tables?: string[];
    columns?: string[];
  };
}

// =============================================================================
// MANAGER INTERFACE
// =============================================================================

/**
 * Database attachment manager interface
 */
export interface DatabaseManager {
  /**
   * Attach a database with the given alias
   */
  attach(path: string, options?: AttachOptions): Promise<AttachResult>;

  /**
   * Detach a database by alias
   */
  detach(alias: string): Promise<DetachResult>;

  /**
   * Get attached database by alias
   */
  get(alias: string): AttachedDatabase | undefined;

  /**
   * List all attached databases (for PRAGMA database_list)
   */
  list(): DatabaseListEntry[];

  /**
   * Get the main database
   */
  getMain(): AttachedDatabase;

  /**
   * Get the temp database
   */
  getTemp(): AttachedDatabase;

  /**
   * Check if a database alias exists
   */
  has(alias: string): boolean;

  /**
   * Get count of attached databases
   */
  count(): number;

  /**
   * Get all attached database aliases
   */
  aliases(): string[];

  /**
   * Get schema for a database
   */
  getSchema(alias: string): Schema | undefined;

  /**
   * Merge schemas from all databases for global view
   */
  getMergedSchema(): Schema;
}

/**
 * Schema resolver interface
 */
export interface SchemaResolver {
  /**
   * Resolve a table reference to its database location
   */
  resolveTable(ref: QualifiedTableRef): ResolvedTableRef | AttachError;

  /**
   * Resolve a column reference to its full definition
   */
  resolveColumn(ref: QualifiedColumnRef, scope: QueryScope): ResolvedColumnRef | AttachError;

  /**
   * Check for ambiguous references in a query
   */
  checkAmbiguity(tableName: string): AmbiguityCheck;

  /**
   * Get all tables across all databases
   */
  getAllTables(): Map<string, QualifiedTableRef[]>;

  /**
   * Build query scope from FROM clause
   */
  buildScope(fromTables: QualifiedTableRef[]): QueryScope;
}

// =============================================================================
// CLOUDFLARE TYPES (minimal declarations)
// =============================================================================

/**
 * Minimal R2Bucket interface
 */
export interface R2Bucket {
  get(key: string): Promise<R2Object | null>;
  head(key: string): Promise<R2Object | null>;
  list(options?: { prefix?: string }): Promise<{ objects: R2Object[] }>;
}

interface R2Object {
  key: string;
  size: number;
  body?: ReadableStream;
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
  json<T>(): Promise<T>;
}

/**
 * Minimal KVNamespace interface
 */
export interface KVNamespace {
  get(key: string, options?: { type?: 'text' | 'json' | 'arrayBuffer' }): Promise<string | object | ArrayBuffer | null>;
  put(key: string, value: string | ArrayBuffer): Promise<void>;
  delete(key: string): Promise<void>;
  list(options?: { prefix?: string }): Promise<{ keys: { name: string }[] }>;
}

/**
 * Minimal D1Database interface
 */
export interface D1Database {
  prepare(sql: string): D1PreparedStatement;
  batch<T>(statements: D1PreparedStatement[]): Promise<D1Result<T>[]>;
  exec(sql: string): Promise<D1ExecResult>;
}

interface D1PreparedStatement {
  bind(...values: unknown[]): D1PreparedStatement;
  first<T>(column?: string): Promise<T | null>;
  all<T>(): Promise<D1Result<T>>;
  run(): Promise<D1Result<unknown>>;
}

interface D1Result<T> {
  results: T[];
  success: boolean;
  meta?: {
    duration: number;
    changes: number;
    last_row_id: number;
  };
}

interface D1ExecResult {
  count: number;
  duration: number;
}

/**
 * Minimal DurableObjectNamespace interface
 */
export interface DurableObjectNamespace {
  idFromName(name: string): DurableObjectId;
  idFromString(id: string): DurableObjectId;
  get(id: DurableObjectId): DurableObjectStub;
  newUniqueId(): DurableObjectId;
}

interface DurableObjectId {
  toString(): string;
}

interface DurableObjectStub {
  fetch(request: Request): Promise<Response>;
}
