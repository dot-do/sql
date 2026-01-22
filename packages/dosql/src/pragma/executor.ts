/**
 * DoSQL PRAGMA Executor
 *
 * Executes PRAGMA statements against a database context.
 * Supports SQLite-compatible pragmas including:
 *
 * Schema Introspection:
 * - table_info(table) - Returns column information
 * - table_list - List all tables
 * - index_list(table) - Indexes on a table
 * - index_info(index) - Index columns
 * - foreign_key_list(table) - Foreign keys
 *
 * Configuration:
 * - foreign_keys = ON/OFF
 * - journal_mode = WAL/DELETE/MEMORY
 * - synchronous = OFF/NORMAL/FULL
 * - cache_size = pages
 * - page_size (read-only after creation)
 *
 * Application:
 * - user_version = n
 * - application_id = n
 * - data_version (read-only)
 *
 * Analysis:
 * - database_list - Attached databases
 * - compile_options - Build flags
 */

import type {
  PragmaStatement,
  PragmaValue,
  PragmaResult,
  TableInfoRow,
  TableListRow,
  IndexListRow,
  IndexInfoRow,
  ForeignKeyListRow,
  DatabaseListRow,
  CompileOptionRow,
  JournalMode,
  SynchronousMode,
  AutoVacuumMode,
  TempStore,
  LockingMode,
} from './types.js';

import {
  isSchemaIntrospectionPragma,
  isReadOnlyPragma,
} from './types.js';

// =============================================================================
// DATABASE STATE INTERFACE
// =============================================================================

/**
 * Column definition in a table
 */
export interface ColumnInfo {
  name: string;
  type: string;
  notNull: boolean;
  defaultValue: string | null;
  primaryKey: number; // 0 for non-PK, 1+ for PK position
}

/**
 * Index definition
 */
export interface IndexInfo {
  name: string;
  tableName: string;
  unique: boolean;
  origin: 'c' | 'u' | 'pk';
  partial: boolean;
  columns: Array<{
    name: string | null;
    desc: boolean;
    collation: string;
  }>;
}

/**
 * Foreign key definition
 */
export interface ForeignKeyInfo {
  id: number;
  columns: Array<{
    from: string;
    to: string;
  }>;
  table: string;
  onUpdate: string;
  onDelete: string;
  match: string;
}

/**
 * Table definition
 */
export interface TableInfo {
  name: string;
  schema: string;
  type: 'table' | 'view' | 'virtual' | 'shadow';
  columns: ColumnInfo[];
  indexes: IndexInfo[];
  foreignKeys: ForeignKeyInfo[];
  withoutRowId: boolean;
  strict: boolean;
}

/**
 * Database schema state
 */
export interface DatabaseState {
  /** Schema name to tables map */
  schemas: Map<string, Map<string, TableInfo>>;
  /** Configuration values */
  config: DatabaseConfig;
  /** Application state */
  application: ApplicationState;
}

/**
 * Database configuration state
 */
export interface DatabaseConfig {
  foreignKeys: boolean;
  journalMode: JournalMode;
  synchronous: SynchronousMode;
  cacheSize: number;
  pageSize: number;
  autoVacuum: AutoVacuumMode;
  lockingMode: LockingMode;
  tempStore: TempStore;
  mmapSize: number;
  maxPageCount: number;
  walAutocheckpoint: number;
  busyTimeout: number;
  encoding: string;
  recursiveTriggers: boolean;
  reverseUnorderedSelects: boolean;
  caseSensitiveLike: boolean;
  ignoreCheckConstraints: boolean;
  deferForeignKeys: boolean;
  legacyAlterTable: boolean;
  trustedSchema: boolean;
}

/**
 * Application state
 */
export interface ApplicationState {
  userVersion: number;
  applicationId: number;
  dataVersion: number;
  schemaVersion: number;
}

/**
 * Compile options for this build
 */
export const COMPILE_OPTIONS: string[] = [
  'DOSQL_CLOUDFLARE_WORKERS',
  'ENABLE_FTS5',
  'ENABLE_JSON1',
  'ENABLE_RTREE',
  'OMIT_DEPRECATED',
  'THREADSAFE=0',
  'DEFAULT_WAL_SYNCHRONOUS=1',
  'LIKE_DOESNT_MATCH_BLOBS',
  'MAX_EXPR_DEPTH=100',
  'DEFAULT_CACHE_SIZE=-2000',
  'DEFAULT_PAGE_SIZE=4096',
];

// =============================================================================
// DEFAULT STATE FACTORY
// =============================================================================

/**
 * Create default database configuration
 */
export function createDefaultConfig(): DatabaseConfig {
  return {
    foreignKeys: false,         // SQLite default is OFF
    journalMode: 'DELETE',      // SQLite default
    synchronous: 'FULL',        // SQLite default
    cacheSize: -2000,           // Negative = KB, positive = pages
    pageSize: 4096,             // SQLite default
    autoVacuum: 'NONE',         // SQLite default
    lockingMode: 'NORMAL',      // SQLite default
    tempStore: 'DEFAULT',       // SQLite default
    mmapSize: 0,                // Disabled by default
    maxPageCount: 1073741823,   // SQLite default (2^30 - 1)
    walAutocheckpoint: 1000,    // SQLite default
    busyTimeout: 0,             // SQLite default
    encoding: 'UTF-8',          // SQLite default
    recursiveTriggers: false,   // SQLite default
    reverseUnorderedSelects: false,
    caseSensitiveLike: false,   // SQLite default
    ignoreCheckConstraints: false,
    deferForeignKeys: false,
    legacyAlterTable: false,
    trustedSchema: true,        // SQLite default
  };
}

/**
 * Create default application state
 */
export function createDefaultApplicationState(): ApplicationState {
  return {
    userVersion: 0,
    applicationId: 0,
    dataVersion: 1,
    schemaVersion: 0,
  };
}

/**
 * Create empty database state
 */
export function createDatabaseState(): DatabaseState {
  const schemas = new Map<string, Map<string, TableInfo>>();
  schemas.set('main', new Map());
  schemas.set('temp', new Map());

  return {
    schemas,
    config: createDefaultConfig(),
    application: createDefaultApplicationState(),
  };
}

// =============================================================================
// PRAGMA EXECUTOR
// =============================================================================

/**
 * PRAGMA executor interface
 */
export interface PragmaExecutor {
  /**
   * Execute a PRAGMA statement
   */
  execute(stmt: PragmaStatement): PragmaResult;

  /**
   * Get the current database state
   */
  getState(): DatabaseState;

  /**
   * Register a table in the schema
   */
  registerTable(table: TableInfo): void;

  /**
   * Unregister a table from the schema
   */
  unregisterTable(schema: string, name: string): void;
}

/**
 * Create a PRAGMA executor
 */
export function createPragmaExecutor(
  initialState?: Partial<DatabaseState>
): PragmaExecutor {
  const state = createDatabaseState();

  // Merge initial state if provided
  if (initialState) {
    if (initialState.config) {
      Object.assign(state.config, initialState.config);
    }
    if (initialState.application) {
      Object.assign(state.application, initialState.application);
    }
    if (initialState.schemas) {
      for (const [schemaName, tables] of initialState.schemas) {
        state.schemas.set(schemaName, new Map(tables));
      }
    }
  }

  return {
    execute(stmt: PragmaStatement): PragmaResult {
      return executePragma(stmt, state);
    },

    getState(): DatabaseState {
      return state;
    },

    registerTable(table: TableInfo): void {
      const schemaMap = state.schemas.get(table.schema) ?? new Map();
      schemaMap.set(table.name, table);
      state.schemas.set(table.schema, schemaMap);
    },

    unregisterTable(schema: string, name: string): void {
      const schemaMap = state.schemas.get(schema);
      if (schemaMap) {
        schemaMap.delete(name);
      }
    },
  };
}

// =============================================================================
// PRAGMA EXECUTION
// =============================================================================

/**
 * Execute a PRAGMA statement
 */
export function executePragma(
  stmt: PragmaStatement,
  state: DatabaseState
): PragmaResult {
  const name = stmt.name.toLowerCase();
  const schema = stmt.schema?.toLowerCase() ?? 'main';

  // Handle schema introspection pragmas
  if (isSchemaIntrospectionPragma(name)) {
    return executeSchemaIntrospectionPragma(stmt, state, schema);
  }

  // Handle configuration pragmas
  const configResult = executeConfigurationPragma(stmt, state);
  if (configResult) {
    return configResult;
  }

  // Handle application pragmas
  const appResult = executeApplicationPragma(stmt, state);
  if (appResult) {
    return appResult;
  }

  // Unknown pragma
  return {
    success: false,
    error: `Unknown PRAGMA: ${name}`,
  };
}

// =============================================================================
// SCHEMA INTROSPECTION PRAGMAS
// =============================================================================

/**
 * Execute a schema introspection pragma
 */
function executeSchemaIntrospectionPragma(
  stmt: PragmaStatement,
  state: DatabaseState,
  schema: string
): PragmaResult {
  const name = stmt.name.toLowerCase();

  switch (name) {
    case 'table_info':
    case 'table_xinfo':
      return executeTableInfo(stmt.argument, state, schema);

    case 'table_list':
      return executeTableList(state, schema, stmt.argument);

    case 'index_list':
      return executeIndexList(stmt.argument, state, schema);

    case 'index_info':
    case 'index_xinfo':
      return executeIndexInfo(stmt.argument, state, schema, name === 'index_xinfo');

    case 'foreign_key_list':
      return executeForeignKeyList(stmt.argument, state, schema);

    case 'foreign_key_check':
      return executeForeignKeyCheck(stmt.argument, state, schema);

    case 'database_list':
      return executeDatabaseList(state);

    case 'compile_options':
      return executeCompileOptions();

    default:
      return {
        success: false,
        error: `Unknown schema introspection pragma: ${name}`,
      };
  }
}

/**
 * Execute PRAGMA table_info(table)
 */
function executeTableInfo(
  tableName: string | undefined,
  state: DatabaseState,
  schema: string
): PragmaResult<TableInfoRow> {
  if (!tableName) {
    return {
      success: false,
      error: 'table_info requires a table name argument',
    };
  }

  const schemaMap = state.schemas.get(schema);
  const table = schemaMap?.get(tableName);

  if (!table) {
    // SQLite returns empty result for non-existent tables
    return {
      success: true,
      rows: [],
    };
  }

  const rows: TableInfoRow[] = table.columns.map((col, index) => ({
    cid: index,
    name: col.name,
    type: col.type,
    notnull: col.notNull ? 1 : 0,
    dflt_value: col.defaultValue,
    pk: col.primaryKey,
  }));

  return {
    success: true,
    rows,
  };
}

/**
 * Execute PRAGMA table_list
 */
function executeTableList(
  state: DatabaseState,
  schema: string,
  tableName?: string
): PragmaResult<TableListRow> {
  const rows: TableListRow[] = [];

  // If table name provided, filter to that table
  if (tableName) {
    const schemaMap = state.schemas.get(schema);
    const table = schemaMap?.get(tableName);
    if (table) {
      rows.push({
        schema: table.schema,
        name: table.name,
        type: table.type,
        ncol: table.columns.length,
        wr: table.withoutRowId ? 1 : 0,
        strict: table.strict ? 1 : 0,
      });
    }
  } else {
    // List all tables in all schemas
    for (const [schemaName, tables] of state.schemas) {
      for (const table of tables.values()) {
        rows.push({
          schema: schemaName,
          name: table.name,
          type: table.type,
          ncol: table.columns.length,
          wr: table.withoutRowId ? 1 : 0,
          strict: table.strict ? 1 : 0,
        });
      }
    }
  }

  return {
    success: true,
    rows,
  };
}

/**
 * Execute PRAGMA index_list(table)
 */
function executeIndexList(
  tableName: string | undefined,
  state: DatabaseState,
  schema: string
): PragmaResult<IndexListRow> {
  if (!tableName) {
    return {
      success: false,
      error: 'index_list requires a table name argument',
    };
  }

  const schemaMap = state.schemas.get(schema);
  const table = schemaMap?.get(tableName);

  if (!table) {
    return {
      success: true,
      rows: [],
    };
  }

  const rows: IndexListRow[] = table.indexes.map((idx, seq) => ({
    seq,
    name: idx.name,
    unique: idx.unique ? 1 : 0,
    origin: idx.origin,
    partial: idx.partial ? 1 : 0,
  }));

  return {
    success: true,
    rows,
  };
}

/**
 * Execute PRAGMA index_info(index)
 */
function executeIndexInfo(
  indexName: string | undefined,
  state: DatabaseState,
  schema: string,
  extended: boolean
): PragmaResult<IndexInfoRow> {
  if (!indexName) {
    return {
      success: false,
      error: 'index_info requires an index name argument',
    };
  }

  // Find the index across all tables in the schema
  const schemaMap = state.schemas.get(schema);
  if (!schemaMap) {
    return { success: true, rows: [] };
  }

  for (const table of schemaMap.values()) {
    const index = table.indexes.find((idx) => idx.name === indexName);
    if (index) {
      const rows: IndexInfoRow[] = index.columns.map((col, seqno) => {
        const cid = col.name
          ? table.columns.findIndex((c) => c.name === col.name)
          : -1;

        const base: IndexInfoRow = {
          seqno,
          cid,
          name: col.name,
        };

        if (extended) {
          return {
            ...base,
            desc: col.desc ? 1 : 0,
            coll: col.collation,
            key: 1, // All listed columns are key columns
          };
        }

        return base;
      });

      return { success: true, rows };
    }
  }

  return { success: true, rows: [] };
}

/**
 * Execute PRAGMA foreign_key_list(table)
 */
function executeForeignKeyList(
  tableName: string | undefined,
  state: DatabaseState,
  schema: string
): PragmaResult<ForeignKeyListRow> {
  if (!tableName) {
    return {
      success: false,
      error: 'foreign_key_list requires a table name argument',
    };
  }

  const schemaMap = state.schemas.get(schema);
  const table = schemaMap?.get(tableName);

  if (!table) {
    return { success: true, rows: [] };
  }

  const rows: ForeignKeyListRow[] = [];

  for (const fk of table.foreignKeys) {
    for (let seq = 0; seq < fk.columns.length; seq++) {
      rows.push({
        id: fk.id,
        seq,
        table: fk.table,
        from: fk.columns[seq].from,
        to: fk.columns[seq].to,
        on_update: fk.onUpdate,
        on_delete: fk.onDelete,
        match: fk.match,
      });
    }
  }

  return { success: true, rows };
}

/**
 * Execute PRAGMA foreign_key_check
 * Returns empty result (no violations) in this implementation
 */
function executeForeignKeyCheck(
  _tableName: string | undefined,
  _state: DatabaseState,
  _schema: string
): PragmaResult {
  // In a real implementation, this would check for FK violations
  // For now, return empty (no violations)
  return {
    success: true,
    rows: [],
  };
}

/**
 * Execute PRAGMA database_list
 */
function executeDatabaseList(state: DatabaseState): PragmaResult<DatabaseListRow> {
  const rows: DatabaseListRow[] = [];
  let seq = 0;

  for (const schemaName of state.schemas.keys()) {
    rows.push({
      seq: seq++,
      name: schemaName,
      file: schemaName === 'main' ? ':memory:' : '',
    });
  }

  return { success: true, rows };
}

/**
 * Execute PRAGMA compile_options
 */
function executeCompileOptions(): PragmaResult<CompileOptionRow> {
  const rows: CompileOptionRow[] = COMPILE_OPTIONS.map((option) => ({
    compile_option: option,
  }));

  return { success: true, rows };
}

// =============================================================================
// CONFIGURATION PRAGMAS
// =============================================================================

/**
 * Execute a configuration pragma
 */
function executeConfigurationPragma(
  stmt: PragmaStatement,
  state: DatabaseState
): PragmaResult | null {
  const name = stmt.name.toLowerCase();
  const config = state.config;

  // Map pragma names to config properties
  const configMap: Record<string, keyof DatabaseConfig> = {
    foreign_keys: 'foreignKeys',
    journal_mode: 'journalMode',
    synchronous: 'synchronous',
    cache_size: 'cacheSize',
    page_size: 'pageSize',
    auto_vacuum: 'autoVacuum',
    locking_mode: 'lockingMode',
    temp_store: 'tempStore',
    mmap_size: 'mmapSize',
    max_page_count: 'maxPageCount',
    wal_autocheckpoint: 'walAutocheckpoint',
    busy_timeout: 'busyTimeout',
    encoding: 'encoding',
    recursive_triggers: 'recursiveTriggers',
    reverse_unordered_selects: 'reverseUnorderedSelects',
    case_sensitive_like: 'caseSensitiveLike',
    ignore_check_constraints: 'ignoreCheckConstraints',
    defer_foreign_keys: 'deferForeignKeys',
    legacy_alter_table: 'legacyAlterTable',
    trusted_schema: 'trustedSchema',
  };

  const configKey = configMap[name];
  if (!configKey) {
    return null; // Not a configuration pragma
  }

  // Check for read-only pragmas
  if (stmt.mode === 'set' && isReadOnlyPragma(name)) {
    return {
      success: false,
      error: `PRAGMA ${name} is read-only`,
    };
  }

  // Get operation
  if (stmt.mode === 'get') {
    return {
      success: true,
      value: config[configKey] as PragmaValue,
    };
  }

  // Set operation
  if (stmt.mode === 'set' && stmt.value !== undefined) {
    const newValue = normalizeConfigValue(name, stmt.value);
    if (newValue === undefined) {
      return {
        success: false,
        error: `Invalid value for PRAGMA ${name}: ${stmt.value}`,
      };
    }

    (config as Record<string, unknown>)[configKey] = newValue;

    return {
      success: true,
      value: newValue as PragmaValue,
    };
  }

  return {
    success: true,
    value: config[configKey] as PragmaValue,
  };
}

/**
 * Normalize a configuration value to the appropriate type
 */
function normalizeConfigValue(
  name: string,
  value: PragmaValue
): unknown {
  switch (name) {
    // Boolean pragmas
    case 'foreign_keys':
    case 'recursive_triggers':
    case 'reverse_unordered_selects':
    case 'case_sensitive_like':
    case 'ignore_check_constraints':
    case 'defer_foreign_keys':
    case 'legacy_alter_table':
    case 'trusted_schema':
      return normalizeBoolean(value);

    // Journal mode
    case 'journal_mode':
      return normalizeJournalMode(value);

    // Synchronous mode
    case 'synchronous':
      return normalizeSynchronous(value);

    // Auto vacuum mode
    case 'auto_vacuum':
      return normalizeAutoVacuum(value);

    // Locking mode
    case 'locking_mode':
      return normalizeLockingMode(value);

    // Temp store
    case 'temp_store':
      return normalizeTempStore(value);

    // Numeric pragmas
    case 'cache_size':
    case 'page_size':
    case 'mmap_size':
    case 'max_page_count':
    case 'wal_autocheckpoint':
    case 'busy_timeout':
      return normalizeNumber(value);

    // String pragmas
    case 'encoding':
      return typeof value === 'string' ? value.toUpperCase() : undefined;

    default:
      return value;
  }
}

/**
 * Normalize a boolean value
 */
function normalizeBoolean(value: PragmaValue): boolean | undefined {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') {
    const upper = value.toUpperCase();
    if (upper === 'ON' || upper === 'TRUE' || upper === 'YES' || upper === '1') {
      return true;
    }
    if (upper === 'OFF' || upper === 'FALSE' || upper === 'NO' || upper === '0') {
      return false;
    }
  }
  return undefined;
}

/**
 * Normalize a number value
 */
function normalizeNumber(value: PragmaValue): number | undefined {
  if (typeof value === 'number') return value;
  if (typeof value === 'string') {
    const num = parseInt(value, 10);
    return isNaN(num) ? undefined : num;
  }
  return undefined;
}

/**
 * Normalize journal mode value
 */
function normalizeJournalMode(value: PragmaValue): JournalMode | undefined {
  if (typeof value !== 'string') return undefined;
  const upper = value.toUpperCase();
  const modes: JournalMode[] = ['DELETE', 'TRUNCATE', 'PERSIST', 'MEMORY', 'WAL', 'OFF'];
  return modes.includes(upper as JournalMode) ? (upper as JournalMode) : undefined;
}

/**
 * Normalize synchronous mode value
 */
function normalizeSynchronous(value: PragmaValue): SynchronousMode | undefined {
  if (typeof value === 'number') {
    const modes: SynchronousMode[] = ['OFF', 'NORMAL', 'FULL', 'EXTRA'];
    return modes[value];
  }
  // Handle boolean - OFF parsed as false, treat as 0
  if (typeof value === 'boolean') {
    return value ? 'NORMAL' : 'OFF';
  }
  if (typeof value === 'string') {
    const upper = value.toUpperCase();
    const modes: SynchronousMode[] = ['OFF', 'NORMAL', 'FULL', 'EXTRA'];
    return modes.includes(upper as SynchronousMode)
      ? (upper as SynchronousMode)
      : undefined;
  }
  return undefined;
}

/**
 * Normalize auto vacuum mode value
 */
function normalizeAutoVacuum(value: PragmaValue): AutoVacuumMode | undefined {
  if (typeof value === 'number') {
    const modes: AutoVacuumMode[] = ['NONE', 'FULL', 'INCREMENTAL'];
    return modes[value];
  }
  if (typeof value === 'string') {
    const upper = value.toUpperCase();
    const modes: AutoVacuumMode[] = ['NONE', 'FULL', 'INCREMENTAL'];
    return modes.includes(upper as AutoVacuumMode)
      ? (upper as AutoVacuumMode)
      : undefined;
  }
  return undefined;
}

/**
 * Normalize locking mode value
 */
function normalizeLockingMode(value: PragmaValue): LockingMode | undefined {
  if (typeof value !== 'string') return undefined;
  const upper = value.toUpperCase();
  const modes: LockingMode[] = ['NORMAL', 'EXCLUSIVE'];
  return modes.includes(upper as LockingMode) ? (upper as LockingMode) : undefined;
}

/**
 * Normalize temp store value
 */
function normalizeTempStore(value: PragmaValue): TempStore | undefined {
  if (typeof value === 'number') {
    const modes: TempStore[] = ['DEFAULT', 'FILE', 'MEMORY'];
    return modes[value];
  }
  if (typeof value === 'string') {
    const upper = value.toUpperCase();
    const modes: TempStore[] = ['DEFAULT', 'FILE', 'MEMORY'];
    return modes.includes(upper as TempStore) ? (upper as TempStore) : undefined;
  }
  return undefined;
}

// =============================================================================
// APPLICATION PRAGMAS
// =============================================================================

/**
 * Execute an application pragma
 */
function executeApplicationPragma(
  stmt: PragmaStatement,
  state: DatabaseState
): PragmaResult | null {
  const name = stmt.name.toLowerCase();
  const app = state.application;

  // Map pragma names to application properties
  const appMap: Record<string, keyof ApplicationState> = {
    user_version: 'userVersion',
    application_id: 'applicationId',
    data_version: 'dataVersion',
    schema_version: 'schemaVersion',
  };

  const appKey = appMap[name];
  if (!appKey) {
    return null; // Not an application pragma
  }

  // Check for read-only pragmas
  if (stmt.mode === 'set' && isReadOnlyPragma(name)) {
    return {
      success: false,
      error: `PRAGMA ${name} is read-only`,
    };
  }

  // Get operation
  if (stmt.mode === 'get') {
    return {
      success: true,
      value: app[appKey],
    };
  }

  // Set operation
  if (stmt.mode === 'set' && stmt.value !== undefined) {
    const numValue = normalizeNumber(stmt.value);
    if (numValue === undefined) {
      return {
        success: false,
        error: `Invalid value for PRAGMA ${name}: ${stmt.value}`,
      };
    }

    (app as Record<string, number>)[appKey] = numValue;

    return {
      success: true,
      value: numValue,
    };
  }

  return {
    success: true,
    value: app[appKey],
  };
}
