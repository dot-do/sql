/**
 * DoSQL PRAGMA Module
 *
 * Exports PRAGMA parsing and execution functionality for SQLite compatibility.
 *
 * ## Stability
 *
 * This module follows semantic versioning. Exports are marked with stability annotations:
 *
 * - **stable**: No breaking changes in minor versions. Safe for production use.
 * - **experimental**: May change in any version. Use with caution.
 *
 * @packageDocumentation
 * @stability stable
 */

// Parser exports

/**
 * PRAGMA parser utilities.
 * @public
 * @stability stable
 */
export {
  parsePragma,
  tryParsePragma,
  isPragmaStatement,
  formatPragma,
} from './parser.js';

// Executor exports

/**
 * PRAGMA execution utilities.
 * @public
 * @stability stable
 */
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

/**
 * PRAGMA value and statement types.
 * @public
 * @stability stable
 */
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

/**
 * PRAGMA type guards and constants.
 * @public
 * @stability stable
 */
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
