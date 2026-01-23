/**
 * DoSQL Migrations Module
 *
 * Provides migration support for DoSQL with Drizzle Kit compatibility.
 * Designed for the DB-per-tenant model with clone-on-demand migrations.
 *
 * @module migrations
 *
 * @example
 * ```typescript
 * import {
 *   createMigrationRunner,
 *   createMigrations,
 *   createSchemaTracker,
 *   initializeWithMigrations,
 * } from 'dosql/migrations';
 *
 * // Define migrations
 * const migrations = createMigrations([
 *   { id: '20240101000000_init', sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY)' },
 *   { id: '20240101000001_add_email', sql: 'ALTER TABLE users ADD COLUMN email TEXT' },
 * ]);
 *
 * // Initialize database with migrations
 * const status = await initializeWithMigrations({
 *   migrations,
 *   storage: ctx.storage,
 *   db: database,
 *   autoMigrate: true,
 * });
 * ```
 */

// =============================================================================
// TYPE EXPORTS
// =============================================================================

export type {
  // Core migration types
  Migration,
  MigrationSnapshot,
  SnapshotTable,
  SnapshotColumn,
  SnapshotPrimaryKey,
  SnapshotIndex,
  SnapshotForeignKey,
  SnapshotUniqueConstraint,
  SnapshotCheckConstraint,
  SnapshotCompositePrimaryKey,
  SnapshotEnum,

  // Tracking types
  AppliedMigration,
  MigrationStatus,

  // Runner types
  MigrationRunnerOptions,
  MigrationLogger,
  MigrationResult,

  // DB integration types
  MigrationSource,
  DatabaseMigrationOptions,
  CloneMigrationContext,

  // Drizzle compat types
  DrizzleMigrationFolder,
  DrizzleJournalEntry,
  DrizzleJournal,
} from './types.js';

// =============================================================================
// TYPE GUARD EXPORTS
// =============================================================================

export {
  isMigration,
  isMigrationArray,
  isFolderSource,
  isDrizzleSource,
  isLoaderFunction,
} from './types.js';

// =============================================================================
// UTILITY FUNCTION EXPORTS
// =============================================================================

export {
  extractTimestamp,
  compareMigrationIds,
  generateMigrationId,
  calculateChecksum,
  calculateChecksumSync,
} from './types.js';

// =============================================================================
// RUNNER EXPORTS
// =============================================================================

export type { DatabaseExecutor } from './runner.js';

export type { DoMigrationsRunnerOptions } from './runner.js';

export {
  MigrationRunner,
  createMigrationRunner,
  createMigration,
  createMigrations,
  sortMigrations,
  getMigrationsBetween,
  detectOutOfOrderMigrations,
  batchMigrations,
  splitMigrationSql,
  runMigrationsFromSource,
} from './runner.js';

// =============================================================================
// DRIZZLE COMPATIBILITY EXPORTS
// =============================================================================

export type {
  MigrationFileSystem,
  DrizzleLoaderOptions,
  DrizzleConfig,
} from './drizzle-compat.js';

export {
  parseMigrationFolderName,
  parseSnapshotJson,
  parseJournalJson,
  loadDrizzleMigrations,
  createInMemoryFs,
  generateDownMigration,
  parseDrizzleConfig,
  drizzleIdToDoSqlId,
  toDoSqlMigration,
} from './drizzle-compat.js';

// =============================================================================
// SCHEMA TRACKER EXPORTS
// =============================================================================

export type {
  SchemaStorage,
  DBMigrationOptions,
} from './schema-tracker.js';

export {
  SchemaTracker,
  createSchemaTracker,
  createInMemoryStorage,
  initializeWithMigrations,
  prepareClone,
  isCloneReady,
  createMigrationFromSql,
  createReversibleMigration,
} from './schema-tracker.js';

// =============================================================================
// .DO/MIGRATIONS LOADER EXPORTS
// =============================================================================

export type {
  DoMigrationLoaderOptions,
  MigrationValidationError,
  MigrationValidationResult,
  ValidationOptions,
  ApplyMigrationsOptions,
} from './do-loader.js';

export {
  DEFAULT_DO_MIGRATIONS_FOLDER,
  loadMigrationsFromFolder,
  parseMigrationFile,
  sortMigrations as sortDoMigrations,
  extractMigrationNumber,
  validateMigrationSequence,
  parseSqlStatements,
  stripSqlComments,
  detectAppliedMigrations,
  getPendingMigrations,
  applyMigrationsInOrder,
} from './do-loader.js';
