/**
 * Standalone Query Executor Types
 *
 * Defines interfaces for the standalone query executor that can operate
 * without Durable Object infrastructure.
 */

// =============================================================================
// Storage Interface
// =============================================================================

/**
 * Minimal key-value storage interface for the executor.
 * Can be backed by B-tree, in-memory Map, or any other storage.
 */
export interface KVStorage {
  /** Get a value by key */
  get(key: string): Promise<Record<string, unknown> | undefined>;
  /** Set a value by key */
  set(key: string, value: Record<string, unknown>): Promise<void>;
  /** Delete a key */
  delete(key: string): Promise<boolean>;
  /** Iterate over a key range (inclusive start, exclusive end) */
  range(
    start: string,
    end: string
  ): AsyncIterable<[string, Record<string, unknown>]>;
}

// =============================================================================
// Schema Types
// =============================================================================

/**
 * Column definition in a table schema
 */
export interface ColumnDefinition {
  name: string;
  type: string;
  defaultValue?: string;
}

/**
 * Table schema definition
 */
export interface TableSchema {
  name: string;
  columns: ColumnDefinition[];
  primaryKey: string;
}

/**
 * Schema provider interface for looking up table schemas
 */
export interface SchemaProvider {
  /** Get schema for a table */
  getSchema(tableName: string): TableSchema | undefined;
  /** Get all table schemas */
  getAllSchemas(): TableSchema[];
  /** Get column names for a table */
  getSchemaColumns(tableName: string): string[];
}

/**
 * Schema manager interface that can persist schemas
 */
export interface SchemaManager extends SchemaProvider {
  /** Create a new table schema */
  createTable(schema: TableSchema): Promise<void>;
  /** Drop a table schema */
  dropTable(tableName: string): Promise<boolean>;
  /** Evaluate a DEFAULT value expression */
  evaluateDefaultValue(defaultValue: string): unknown;
  /** Get next auto-increment ID for a table */
  getNextId(tableName: string): Promise<number>;
  /** Update the max ID cache when an explicit ID is inserted */
  updateMaxIdCache(tableName: string, pkValue: unknown): void;
}

// =============================================================================
// WAL Types
// =============================================================================

/**
 * WAL entry for logging changes
 */
export interface WALEntry {
  timestamp: number;
  txnId: string;
  op: 'INSERT' | 'UPDATE' | 'DELETE';
  table: string;
  before?: Uint8Array;
  after?: Uint8Array;
}

/**
 * WAL writer interface
 */
export interface WALWriter {
  append(entry: WALEntry, options?: { sync?: boolean }): Promise<void>;
}

// =============================================================================
// Query Result Types
// =============================================================================

/**
 * Result of executing a SQL query
 */
export interface QueryResult {
  /** Rows returned (for SELECT) or affected rows info (for INSERT/UPDATE/DELETE with RETURNING) */
  rows: Record<string, unknown>[];
  /** Number of rows affected by the operation */
  rowsAffected: number;
}

// =============================================================================
// Executor Configuration
// =============================================================================

/**
 * Configuration for the standalone executor
 */
export interface ExecutorConfig {
  /** Key-value storage backend */
  storage: KVStorage;
  /** Schema provider/manager */
  schema: SchemaProvider | SchemaManager;
  /** Optional WAL writer for durability */
  wal?: WALWriter;
}
