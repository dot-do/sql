/**
 * DoSQL CapnWeb RPC Types
 *
 * Type definitions for the CapnWeb RPC protocol used by DoSQL.
 * These types are shared between the server (Durable Object) and client.
 */

// =============================================================================
// Environment Types
// =============================================================================

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  R2_BUCKET?: R2Bucket;
}

// =============================================================================
// Query Types
// =============================================================================

/**
 * SQL execution request
 */
export interface ExecRequest {
  sql: string;
  params?: unknown[];
}

/**
 * SQL execution result
 */
export interface ExecResult {
  columns: string[];
  rows: unknown[][];
  rowCount: number;
  lastInsertRowId?: number;
  changes?: number;
}

/**
 * Prepared statement handle
 */
export interface PreparedStatementHandle {
  id: string;
  sql: string;
  paramCount: number;
}

/**
 * Transaction operation
 */
export interface TransactionOp {
  type: 'exec' | 'prepare' | 'run';
  sql?: string;
  params?: unknown[];
  stmtId?: string;
}

/**
 * Transaction result
 */
export interface TransactionResult {
  results: Array<ExecResult | { error: string }>;
  committed: boolean;
}

// =============================================================================
// DoSQL RPC API Interface
// =============================================================================

/**
 * DoSQL RPC API - methods exposed via CapnWeb
 *
 * This interface defines the complete API available through RPC.
 * Clients can use .map() chaining for pipelining multiple calls.
 */
export interface DoSQLRpcApi {
  /**
   * Execute SQL and return results
   *
   * @example
   * ```ts
   * const result = await rpc.exec({ sql: 'SELECT * FROM users' });
   * console.log(result.rows);
   * ```
   */
  exec(request: ExecRequest): Promise<ExecResult>;

  /**
   * Prepare a statement and return a handle
   *
   * @example
   * ```ts
   * const stmt = await rpc.prepare({ sql: 'SELECT * FROM users WHERE id = ?' });
   * const result = await rpc.run({ stmtId: stmt.id, params: [1] });
   * ```
   */
  prepare(request: { sql: string }): Promise<PreparedStatementHandle>;

  /**
   * Run a prepared statement
   */
  run(request: { stmtId: string; params?: unknown[] }): Promise<ExecResult>;

  /**
   * Finalize (close) a prepared statement
   */
  finalize(request: { stmtId: string }): Promise<{ success: boolean }>;

  /**
   * Execute multiple operations in a transaction
   *
   * All operations will be executed atomically. If any operation fails,
   * the entire transaction will be rolled back.
   *
   * @example
   * ```ts
   * const result = await rpc.transaction({
   *   ops: [
   *     { type: 'exec', sql: 'INSERT INTO users (name) VALUES (?)', params: ['Alice'] },
   *     { type: 'exec', sql: 'INSERT INTO logs (action) VALUES (?)', params: ['user_created'] },
   *   ],
   * });
   * ```
   */
  transaction(request: { ops: TransactionOp[] }): Promise<TransactionResult>;

  /**
   * Get current database status
   */
  status(): Promise<DatabaseStatus>;

  /**
   * Ping the database (for health checks)
   */
  ping(): Promise<{ pong: true; timestamp: number }>;

  /**
   * List all tables in the database
   */
  listTables(): Promise<{ tables: TableInfo[] }>;

  /**
   * Get schema for a specific table
   */
  describeTable(request: { table: string }): Promise<TableSchema>;
}

// =============================================================================
// Database Metadata Types
// =============================================================================

/**
 * Database status information
 */
export interface DatabaseStatus {
  initialized: boolean;
  tableCount: number;
  totalRows: number;
  storageBytes: number;
  preparedStatements: number;
  version: string;
}

/**
 * Table information (for listing)
 */
export interface TableInfo {
  name: string;
  rowCount: number;
}

/**
 * Full table schema
 */
export interface TableSchema {
  name: string;
  columns: ColumnSchema[];
  primaryKey: string[];
  indexes: IndexSchema[];
}

/**
 * Column schema
 */
export interface ColumnSchema {
  name: string;
  type: string;
  nullable: boolean;
  defaultValue?: string;
}

/**
 * Index schema
 */
export interface IndexSchema {
  name: string;
  columns: string[];
  unique: boolean;
}

// =============================================================================
// Streaming Types (for pipelining)
// =============================================================================

/**
 * Stream request for large result sets
 */
export interface StreamRequest {
  sql: string;
  params?: unknown[];
  chunkSize?: number;
}

/**
 * Stream chunk
 */
export interface StreamChunk {
  rows: unknown[][];
  chunkIndex: number;
  isLast: boolean;
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * RPC error codes
 */
export enum RpcErrorCode {
  UNKNOWN = 'UNKNOWN',
  INVALID_REQUEST = 'INVALID_REQUEST',
  SQL_ERROR = 'SQL_ERROR',
  NOT_FOUND = 'NOT_FOUND',
  TRANSACTION_FAILED = 'TRANSACTION_FAILED',
  TIMEOUT = 'TIMEOUT',
}

/**
 * RPC error structure
 */
export interface RpcError {
  code: RpcErrorCode;
  message: string;
  details?: Record<string, unknown>;
}
