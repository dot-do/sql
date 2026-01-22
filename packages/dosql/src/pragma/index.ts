/**
 * DoSQL PRAGMA Module
 *
 * Exports PRAGMA parsing and execution functionality for SQLite compatibility.
 */

// Parser exports
export {
  parsePragma,
  tryParsePragma,
  isPragmaStatement,
  formatPragma,
} from './parser.js';

// Executor exports
export {
  createPragmaExecutor,
  createDatabaseState,
  createDefaultConfig,
  createDefaultApplicationState,
  executePragma,
  COMPILE_OPTIONS,
  type PragmaExecutor,
  type ColumnInfo,
  type IndexInfo,
  type ForeignKeyInfo,
  type TableInfo,
  type DatabaseState,
  type DatabaseConfig,
  type ApplicationState,
} from './executor.js';

// Type exports
export type {
  // Value types
  PragmaValue,
  PragmaMode,

  // Statement types
  PragmaStatement,

  // Result row types
  TableInfoRow,
  TableXInfoRow,
  TableListRow,
  IndexListRow,
  IndexInfoRow,
  IndexXInfoRow,
  ForeignKeyListRow,
  ForeignKeyCheckRow,
  DatabaseListRow,
  CompileOptionRow,
  PragmaResultRow,
  PragmaResult,

  // Configuration types
  JournalMode,
  SynchronousMode,
  LockingMode,
  AutoVacuumMode,
  TempStore,

  // Pragma name types
  SchemaIntrospectionPragma,
  ConfigurationPragma,
  ApplicationPragma,
  ReadOnlyPragma,
  KnownPragma,

  // Parse result types
  PragmaParseSuccess,
  PragmaParseError,
  PragmaParseResult,
} from './types.js';

// Type guard exports
export {
  isSchemaIntrospectionPragma,
  isConfigurationPragma,
  isApplicationPragma,
  isReadOnlyPragma,
  isPragmaParseSuccess,
  isPragmaParseError,
  isTableInfoRow,
  isIndexListRow,
  isForeignKeyListRow,

  // Constants
  SCHEMA_INTROSPECTION_PRAGMAS,
  CONFIGURATION_PRAGMAS,
  APPLICATION_PRAGMAS,
  READ_ONLY_PRAGMAS,
} from './types.js';
