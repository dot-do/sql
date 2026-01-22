/**
 * DoSQL PRAGMA Types
 *
 * TypeScript types for representing PRAGMA statements as an Abstract Syntax Tree.
 * Supports SQLite-compatible PRAGMA statements including:
 * - Schema introspection (table_info, table_list, index_list, etc.)
 * - Configuration pragmas (foreign_keys, journal_mode, synchronous, etc.)
 * - Application pragmas (user_version, application_id, data_version)
 * - Analysis pragmas (database_list, compile_options)
 */

// =============================================================================
// PRAGMA VALUE TYPES
// =============================================================================

/**
 * Possible PRAGMA value types
 */
export type PragmaValue =
  | string
  | number
  | boolean
  | null;

/**
 * PRAGMA operation mode
 */
export type PragmaMode =
  | 'get'     // PRAGMA name - returns current value
  | 'set'     // PRAGMA name = value - sets value
  | 'call';   // PRAGMA name(arg) - table-valued function style

// =============================================================================
// PRAGMA STATEMENT AST
// =============================================================================

/**
 * PRAGMA statement AST node
 */
export interface PragmaStatement {
  type: 'PRAGMA';
  /** PRAGMA name (e.g., 'table_info', 'foreign_keys') */
  name: string;
  /** Optional schema prefix (e.g., 'main', 'temp') */
  schema?: string;
  /** Operation mode */
  mode: PragmaMode;
  /** Value for set operations */
  value?: PragmaValue;
  /** Argument for call operations (e.g., table name in table_info) */
  argument?: string;
}

// =============================================================================
// PRAGMA RESULT TYPES - SCHEMA INTROSPECTION
// =============================================================================

/**
 * Column info returned by PRAGMA table_info(table)
 */
export interface TableInfoRow {
  /** Column index (0-based) */
  cid: number;
  /** Column name */
  name: string;
  /** Column data type */
  type: string;
  /** 1 if NOT NULL, 0 otherwise */
  notnull: number;
  /** Default value (null if none) */
  dflt_value: string | null;
  /** 1 if part of primary key, 0 otherwise */
  pk: number;
}

/**
 * Extended column info returned by PRAGMA table_xinfo(table)
 */
export interface TableXInfoRow extends TableInfoRow {
  /** 1 if hidden column (e.g., rowid in WITHOUT ROWID tables) */
  hidden: number;
}

/**
 * Table entry returned by PRAGMA table_list
 */
export interface TableListRow {
  /** Schema name (main, temp, etc.) */
  schema: string;
  /** Table name */
  name: string;
  /** Table type ('table', 'view', 'shadow', 'virtual') */
  type: string;
  /** Number of columns */
  ncol: number;
  /** 1 if WITHOUT ROWID table */
  wr: number;
  /** 1 if STRICT table */
  strict: number;
}

/**
 * Index entry returned by PRAGMA index_list(table)
 */
export interface IndexListRow {
  /** Index sequence number */
  seq: number;
  /** Index name */
  name: string;
  /** 1 if unique index */
  unique: number;
  /** Origin of the index ('c'=CREATE INDEX, 'u'=UNIQUE, 'pk'=PRIMARY KEY) */
  origin: 'c' | 'u' | 'pk';
  /** 1 if partial index */
  partial: number;
}

/**
 * Index column info returned by PRAGMA index_info(index)
 */
export interface IndexInfoRow {
  /** Position of column in index (0-based) */
  seqno: number;
  /** Column index in table (-1 for expressions) */
  cid: number;
  /** Column name (null for expressions) */
  name: string | null;
}

/**
 * Extended index column info returned by PRAGMA index_xinfo(index)
 */
export interface IndexXInfoRow extends IndexInfoRow {
  /** 1 if column is descending */
  desc: number;
  /** Collation name */
  coll: string;
  /** 1 if this is a key column */
  key: number;
}

/**
 * Foreign key info returned by PRAGMA foreign_key_list(table)
 */
export interface ForeignKeyListRow {
  /** Foreign key ID */
  id: number;
  /** Sequence number */
  seq: number;
  /** Referenced table name */
  table: string;
  /** Local column name */
  from: string;
  /** Referenced column name */
  to: string;
  /** ON UPDATE action */
  on_update: string;
  /** ON DELETE action */
  on_delete: string;
  /** MATCH clause */
  match: string;
}

/**
 * Foreign key check result returned by PRAGMA foreign_key_check
 */
export interface ForeignKeyCheckRow {
  /** Table name */
  table: string;
  /** Rowid of the violating row */
  rowid: number;
  /** Referenced table name */
  parent: string;
  /** Foreign key index (for multi-column keys) */
  fkid: number;
}

// =============================================================================
// PRAGMA RESULT TYPES - CONFIGURATION
// =============================================================================

/**
 * Database entry returned by PRAGMA database_list
 */
export interface DatabaseListRow {
  /** Database sequence number */
  seq: number;
  /** Database name (main, temp, or attached name) */
  name: string;
  /** Database file path */
  file: string;
}

/**
 * Compile option returned by PRAGMA compile_options
 */
export interface CompileOptionRow {
  /** Compile option name */
  compile_option: string;
}

// =============================================================================
// PRAGMA CONFIGURATION VALUES
// =============================================================================

/**
 * Journal mode values
 */
export type JournalMode =
  | 'DELETE'    // Default: journal file deleted after commit
  | 'TRUNCATE'  // Journal file truncated after commit
  | 'PERSIST'   // Journal file remains, header zeroed
  | 'MEMORY'    // Journal stored in memory
  | 'WAL'       // Write-Ahead Logging
  | 'OFF';      // No journaling (not crash-safe!)

/**
 * Synchronous mode values
 */
export type SynchronousMode =
  | 'OFF'       // 0: No syncs
  | 'NORMAL'    // 1: Sync at critical moments
  | 'FULL'      // 2: Sync after each transaction
  | 'EXTRA';    // 3: Extra syncing for durability

/**
 * Locking mode values
 */
export type LockingMode =
  | 'NORMAL'    // Default: release locks after transaction
  | 'EXCLUSIVE'; // Hold exclusive lock

/**
 * Auto vacuum mode values
 */
export type AutoVacuumMode =
  | 'NONE'       // 0: No auto vacuum
  | 'FULL'       // 1: Full auto vacuum
  | 'INCREMENTAL'; // 2: Incremental auto vacuum

/**
 * Temp store mode values
 */
export type TempStore =
  | 'DEFAULT'   // 0: Use compile-time default
  | 'FILE'      // 1: Use file for temp storage
  | 'MEMORY';   // 2: Use memory for temp storage

// =============================================================================
// PRAGMA RESULT UNION
// =============================================================================

/**
 * All possible PRAGMA result row types
 */
export type PragmaResultRow =
  | TableInfoRow
  | TableXInfoRow
  | TableListRow
  | IndexListRow
  | IndexInfoRow
  | IndexXInfoRow
  | ForeignKeyListRow
  | ForeignKeyCheckRow
  | DatabaseListRow
  | CompileOptionRow;

/**
 * PRAGMA execution result
 */
export interface PragmaResult<T = PragmaResultRow | PragmaValue> {
  /** Whether the PRAGMA execution succeeded */
  success: boolean;
  /** Result rows for table-valued pragmas */
  rows?: T[];
  /** Single value for get/set pragmas */
  value?: PragmaValue;
  /** Error message if failed */
  error?: string;
}

// =============================================================================
// PRAGMA NAME CONSTANTS
// =============================================================================

/**
 * Schema introspection pragmas that return table-valued results
 */
export const SCHEMA_INTROSPECTION_PRAGMAS = [
  'table_info',
  'table_xinfo',
  'table_list',
  'index_list',
  'index_info',
  'index_xinfo',
  'foreign_key_list',
  'foreign_key_check',
  'database_list',
  'compile_options',
] as const;

export type SchemaIntrospectionPragma = typeof SCHEMA_INTROSPECTION_PRAGMAS[number];

/**
 * Configuration pragmas that can be get/set
 */
export const CONFIGURATION_PRAGMAS = [
  'foreign_keys',
  'journal_mode',
  'synchronous',
  'cache_size',
  'page_size',
  'auto_vacuum',
  'locking_mode',
  'temp_store',
  'mmap_size',
  'max_page_count',
  'wal_autocheckpoint',
  'busy_timeout',
  'encoding',
  'recursive_triggers',
  'reverse_unordered_selects',
  'case_sensitive_like',
  'ignore_check_constraints',
  'defer_foreign_keys',
  'legacy_alter_table',
  'trusted_schema',
] as const;

export type ConfigurationPragma = typeof CONFIGURATION_PRAGMAS[number];

/**
 * Application pragmas for version tracking
 */
export const APPLICATION_PRAGMAS = [
  'user_version',
  'application_id',
  'data_version',
  'schema_version',
] as const;

export type ApplicationPragma = typeof APPLICATION_PRAGMAS[number];

/**
 * Read-only pragmas
 */
export const READ_ONLY_PRAGMAS = [
  'page_size',      // Read-only after database creation
  'data_version',   // Always read-only
  'schema_version', // Read-only (use VACUUM to change)
  'compile_options',
  'database_list',
  'table_list',
] as const;

export type ReadOnlyPragma = typeof READ_ONLY_PRAGMAS[number];

/**
 * All known PRAGMA names
 */
export type KnownPragma =
  | SchemaIntrospectionPragma
  | ConfigurationPragma
  | ApplicationPragma;

// =============================================================================
// PARSE RESULT
// =============================================================================

/**
 * Successful parse result
 */
export interface PragmaParseSuccess {
  success: true;
  statement: PragmaStatement;
}

/**
 * Parse error
 */
export interface PragmaParseError {
  success: false;
  error: string;
  position?: number;
}

/**
 * Parse result union
 */
export type PragmaParseResult = PragmaParseSuccess | PragmaParseError;

// =============================================================================
// TYPE GUARDS
// =============================================================================

/**
 * Check if a PRAGMA name is for schema introspection
 */
export function isSchemaIntrospectionPragma(
  name: string
): name is SchemaIntrospectionPragma {
  return (SCHEMA_INTROSPECTION_PRAGMAS as readonly string[]).includes(
    name.toLowerCase()
  );
}

/**
 * Check if a PRAGMA name is a configuration pragma
 */
export function isConfigurationPragma(
  name: string
): name is ConfigurationPragma {
  return (CONFIGURATION_PRAGMAS as readonly string[]).includes(
    name.toLowerCase()
  );
}

/**
 * Check if a PRAGMA name is an application pragma
 */
export function isApplicationPragma(
  name: string
): name is ApplicationPragma {
  return (APPLICATION_PRAGMAS as readonly string[]).includes(
    name.toLowerCase()
  );
}

/**
 * Check if a PRAGMA is read-only
 */
export function isReadOnlyPragma(name: string): boolean {
  return (READ_ONLY_PRAGMAS as readonly string[]).includes(
    name.toLowerCase()
  );
}

/**
 * Check if parse result is successful
 */
export function isPragmaParseSuccess(
  result: PragmaParseResult
): result is PragmaParseSuccess {
  return result.success === true;
}

/**
 * Check if parse result is an error
 */
export function isPragmaParseError(
  result: PragmaParseResult
): result is PragmaParseError {
  return result.success === false;
}

/**
 * Check if a result row is TableInfoRow
 */
export function isTableInfoRow(row: PragmaResultRow): row is TableInfoRow {
  return 'cid' in row && 'name' in row && 'type' in row && 'notnull' in row && 'pk' in row;
}

/**
 * Check if a result row is IndexListRow
 */
export function isIndexListRow(row: PragmaResultRow): row is IndexListRow {
  return 'seq' in row && 'name' in row && 'unique' in row && 'origin' in row;
}

/**
 * Check if a result row is ForeignKeyListRow
 */
export function isForeignKeyListRow(row: PragmaResultRow): row is ForeignKeyListRow {
  return 'id' in row && 'from' in row && 'to' in row && 'on_update' in row;
}
