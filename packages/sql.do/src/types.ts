/**
 * @dotdo/sql.do - Shared types for DoSQL client and server
 */

// =============================================================================
// Branded Types
// =============================================================================

declare const TransactionIdBrand: unique symbol;
declare const LSNBrand: unique symbol;
declare const StatementHashBrand: unique symbol;
declare const ShardIdBrand: unique symbol;

export type TransactionId = string & { readonly [TransactionIdBrand]: never };
export type LSN = bigint & { readonly [LSNBrand]: never };
export type StatementHash = string & { readonly [StatementHashBrand]: never };
export type ShardId = string & { readonly [ShardIdBrand]: never };

export function createTransactionId(id: string): TransactionId {
  return id as TransactionId;
}

export function createLSN(lsn: bigint): LSN {
  return lsn as LSN;
}

export function createStatementHash(hash: string): StatementHash {
  return hash as StatementHash;
}

export function createShardId(id: string): ShardId {
  return id as ShardId;
}

// =============================================================================
// Query Types
// =============================================================================

export type SQLValue = string | number | boolean | null | Uint8Array | bigint;

export interface QueryResult<T = Record<string, SQLValue>> {
  rows: T[];
  columns: string[];
  rowsAffected: number;
  lastInsertRowid?: bigint;
  duration: number;
}

export interface PreparedStatement {
  sql: string;
  hash: StatementHash;
}

export interface QueryOptions {
  /** Transaction ID for transactional queries */
  transactionId?: TransactionId;
  /** Read from a specific point in time */
  asOf?: Date | LSN;
  /** Timeout in milliseconds */
  timeout?: number;
  /** Target shard for sharded queries */
  shardId?: ShardId;
}

// =============================================================================
// Transaction Types
// =============================================================================

export type IsolationLevel =
  | 'READ_UNCOMMITTED'
  | 'READ_COMMITTED'
  | 'REPEATABLE_READ'
  | 'SERIALIZABLE'
  | 'SNAPSHOT';

export interface TransactionOptions {
  isolationLevel?: IsolationLevel;
  readOnly?: boolean;
  timeout?: number;
}

export interface TransactionState {
  id: TransactionId;
  isolationLevel: IsolationLevel;
  readOnly: boolean;
  startedAt: Date;
  snapshotLSN: LSN;
}

// =============================================================================
// Schema Types
// =============================================================================

export type ColumnType =
  | 'INTEGER'
  | 'REAL'
  | 'TEXT'
  | 'BLOB'
  | 'NULL'
  | 'BOOLEAN'
  | 'DATETIME'
  | 'JSON';

export interface ColumnDefinition {
  name: string;
  type: ColumnType;
  nullable: boolean;
  primaryKey: boolean;
  autoIncrement: boolean;
  defaultValue?: SQLValue;
  unique: boolean;
}

export interface TableSchema {
  name: string;
  columns: ColumnDefinition[];
  primaryKey: string[];
  indexes: IndexDefinition[];
}

export interface IndexDefinition {
  name: string;
  columns: string[];
  unique: boolean;
}

// =============================================================================
// RPC Message Types
// =============================================================================

export type RPCMethod =
  | 'exec'
  | 'query'
  | 'prepare'
  | 'execute'
  | 'beginTransaction'
  | 'commit'
  | 'rollback'
  | 'getSchema'
  | 'ping';

export interface RPCRequest {
  id: string;
  method: RPCMethod;
  params: unknown;
}

export interface RPCResponse<T = unknown> {
  id: string;
  result?: T;
  error?: RPCError;
}

export interface RPCError {
  code: string;
  message: string;
  details?: unknown;
}

// =============================================================================
// Client Interface
// =============================================================================

export interface SQLClient {
  /**
   * Execute a SQL statement (INSERT, UPDATE, DELETE, DDL)
   */
  exec(sql: string, params?: SQLValue[], options?: QueryOptions): Promise<QueryResult>;

  /**
   * Execute a SQL query (SELECT)
   */
  query<T = Record<string, SQLValue>>(
    sql: string,
    params?: SQLValue[],
    options?: QueryOptions
  ): Promise<QueryResult<T>>;

  /**
   * Prepare a SQL statement for repeated execution
   */
  prepare(sql: string): Promise<PreparedStatement>;

  /**
   * Execute a prepared statement
   */
  execute<T = Record<string, SQLValue>>(
    statement: PreparedStatement,
    params?: SQLValue[],
    options?: QueryOptions
  ): Promise<QueryResult<T>>;

  /**
   * Begin a transaction
   */
  beginTransaction(options?: TransactionOptions): Promise<TransactionState>;

  /**
   * Commit a transaction
   */
  commit(transactionId: TransactionId): Promise<LSN>;

  /**
   * Rollback a transaction
   */
  rollback(transactionId: TransactionId): Promise<void>;

  /**
   * Get table schema
   */
  getSchema(tableName: string): Promise<TableSchema | null>;

  /**
   * Check connection health
   */
  ping(): Promise<{ latency: number }>;

  /**
   * Close the connection
   */
  close(): Promise<void>;
}

// =============================================================================
// Sharding Types
// =============================================================================

export interface ShardConfig {
  shardCount: number;
  shardKey: string;
  shardFunction: 'hash' | 'range' | 'list';
}

export interface ShardInfo {
  shardId: ShardId;
  keyRange?: { min: SQLValue; max: SQLValue };
  rowCount?: number;
}

// =============================================================================
// CDC Types (for cross-package compatibility)
// =============================================================================

export type CDCOperation = 'INSERT' | 'UPDATE' | 'DELETE';

export interface CDCEvent {
  lsn: LSN;
  timestamp: Date;
  table: string;
  operation: CDCOperation;
  primaryKey: Record<string, SQLValue>;
  before?: Record<string, SQLValue>;
  after?: Record<string, SQLValue>;
  transactionId: TransactionId;
}
