/**
 * DoSQL Migrations - Type Definitions
 *
 * Defines migration types compatible with Drizzle Kit conventions
 * while supporting DoSQL's DB-per-tenant model with clone-on-demand.
 *
 * @module migrations/types
 */

// =============================================================================
// MIGRATION FILE TYPES
// =============================================================================

/**
 * A single migration file
 *
 * Follows Drizzle Kit convention: each migration has SQL and a snapshot.
 * The timestamp format is YYYYMMDDHHMMSS (e.g., 20240823160430).
 */
export interface Migration {
  /** Unique migration identifier (timestamp-based) */
  readonly id: string;

  /** SQL statements to apply (up migration) */
  readonly sql: string;

  /** Optional down migration SQL for rollback */
  readonly downSql?: string;

  /** Schema snapshot after this migration (JSON) */
  readonly snapshot?: MigrationSnapshot;

  /** Human-readable name (e.g., "add_users_table") */
  readonly name?: string;

  /** When this migration was created */
  readonly createdAt: Date;

  /** Checksum for integrity verification */
  readonly checksum: string;
}

/**
 * Schema snapshot format (Drizzle Kit compatible)
 *
 * Represents the complete schema state after a migration.
 */
export interface MigrationSnapshot {
  /** Schema version identifier */
  version: string;

  /** Dialect (postgresql, sqlite, mysql) */
  dialect: 'postgresql' | 'sqlite' | 'mysql';

  /** Tables in the schema */
  tables: Record<string, SnapshotTable>;

  /** Enums (PostgreSQL only) */
  enums?: Record<string, SnapshotEnum>;

  /** Indexes */
  indexes?: Record<string, SnapshotIndex>;

  /** Foreign keys */
  foreignKeys?: Record<string, SnapshotForeignKey>;

  /** Composite primary keys */
  compositePrimaryKeys?: Record<string, SnapshotCompositePrimaryKey>;
}

/**
 * Table definition in snapshot
 */
export interface SnapshotTable {
  name: string;
  columns: Record<string, SnapshotColumn>;
  primaryKey?: SnapshotPrimaryKey;
  indexes?: Record<string, SnapshotIndex>;
  foreignKeys?: Record<string, SnapshotForeignKey>;
  uniqueConstraints?: Record<string, SnapshotUniqueConstraint>;
  checkConstraints?: Record<string, SnapshotCheckConstraint>;
}

/**
 * Column definition in snapshot
 */
export interface SnapshotColumn {
  name: string;
  type: string;
  notNull: boolean;
  default?: string;
  primaryKey?: boolean;
  autoincrement?: boolean;
  generated?: {
    as: string;
    type: 'stored' | 'virtual';
  };
}

/**
 * Primary key definition
 */
export interface SnapshotPrimaryKey {
  name?: string;
  columns: string[];
}

/**
 * Index definition
 */
export interface SnapshotIndex {
  name: string;
  columns: string[];
  unique: boolean;
  where?: string;
}

/**
 * Foreign key definition
 */
export interface SnapshotForeignKey {
  name: string;
  columns: string[];
  referencedTable: string;
  referencedColumns: string[];
  onDelete?: 'cascade' | 'restrict' | 'no action' | 'set null' | 'set default';
  onUpdate?: 'cascade' | 'restrict' | 'no action' | 'set null' | 'set default';
}

/**
 * Unique constraint definition
 */
export interface SnapshotUniqueConstraint {
  name: string;
  columns: string[];
}

/**
 * Check constraint definition
 */
export interface SnapshotCheckConstraint {
  name: string;
  expression: string;
}

/**
 * Composite primary key definition
 */
export interface SnapshotCompositePrimaryKey {
  name: string;
  columns: string[];
}

/**
 * Enum definition (PostgreSQL)
 */
export interface SnapshotEnum {
  name: string;
  values: string[];
}

// =============================================================================
// MIGRATION TRACKING
// =============================================================================

/**
 * Record of an applied migration
 *
 * Stored in `__dosql_migrations` table (configurable).
 */
export interface AppliedMigration {
  /** Migration identifier */
  id: string;

  /** When the migration was applied */
  appliedAt: Date;

  /** Checksum at time of application */
  checksum: string;

  /** Duration in milliseconds */
  durationMs: number;

  /** Optional error message if migration failed */
  error?: string;
}

/**
 * Migration status for a given database
 */
export interface MigrationStatus {
  /** Current schema version (last applied migration ID) */
  currentVersion: string | null;

  /** All applied migrations */
  applied: AppliedMigration[];

  /** Pending migrations to apply */
  pending: Migration[];

  /** Whether database needs migration */
  needsMigration: boolean;
}

// =============================================================================
// MIGRATION RUNNER OPTIONS
// =============================================================================

/**
 * Options for migration runner
 */
export interface MigrationRunnerOptions {
  /** Table name for tracking migrations (default: __dosql_migrations) */
  migrationsTable?: string;

  /** Schema for migrations table (PostgreSQL, default: public) */
  migrationsSchema?: string;

  /** Allow running migrations in production */
  allowProduction?: boolean;

  /** Dry run mode - log SQL without executing */
  dryRun?: boolean;

  /** Wrap all migrations in a single transaction */
  transactional?: boolean;

  /** Continue on error (mark failed and continue) */
  continueOnError?: boolean;

  /** Timeout for each migration in milliseconds */
  timeout?: number;

  /** Log function */
  logger?: MigrationLogger;
}

/**
 * Logger interface for migrations
 */
export interface MigrationLogger {
  info(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
  debug(message: string, ...args: unknown[]): void;
}

// =============================================================================
// MIGRATION RESULT
// =============================================================================

/**
 * Result of running migrations
 */
export interface MigrationResult {
  /** Whether all migrations succeeded */
  success: boolean;

  /** Migrations that were applied */
  applied: AppliedMigration[];

  /** Migrations that failed */
  failed: Array<{
    migration: Migration;
    error: Error;
  }>;

  /** Total duration in milliseconds */
  totalDurationMs: number;

  /** Schema version after migration */
  newVersion: string | null;
}

// =============================================================================
// DB() FUNCTION INTEGRATION TYPES
// =============================================================================

/**
 * Migration source for DB() function
 *
 * Migrations can be provided inline or loaded from a folder.
 *
 * Convention: `.do/migrations/*.sql` folder structure
 * - Each file: `001_create_users.sql`, `002_add_email.sql`, etc.
 * - Files are applied in alphanumeric order
 * - Simple, portable, git-friendly
 */
export type MigrationSource =
  | Migration[]
  | { folder: string }          // Default: '.do/migrations'
  | { drizzle: string }         // Drizzle Kit compatibility: 'drizzle'
  | (() => Promise<Migration[]>);

/**
 * Default migration folder path
 */
export const DEFAULT_MIGRATIONS_FOLDER = '.do/migrations';

/**
 * Extended Database options with migration support
 */
export interface DatabaseMigrationOptions {
  /** Migration source */
  migrations?: MigrationSource;

  /** Auto-migrate on first access */
  autoMigrate?: boolean;

  /** Migration runner options */
  migrationOptions?: MigrationRunnerOptions;
}

/**
 * Clone migration context
 *
 * When a DB clone is created from a template, this tracks
 * the migration state transition needed.
 */
export interface CloneMigrationContext {
  /** Template database version */
  sourceVersion: string | null;

  /** Target version to migrate to */
  targetVersion: string;

  /** Migrations to apply (from source+1 to target) */
  pendingMigrations: Migration[];

  /** Whether clone needs migration */
  needsMigration: boolean;
}

// =============================================================================
// DRIZZLE KIT COMPATIBILITY
// =============================================================================

/**
 * Drizzle Kit migration folder structure
 *
 * Each migration folder contains:
 * - migration.sql: The SQL to execute
 * - snapshot.json: Schema state after migration
 */
export interface DrizzleMigrationFolder {
  /** Folder name (e.g., "20240823160430_add_users") */
  name: string;

  /** Full path to folder */
  path: string;

  /** Timestamp extracted from name */
  timestamp: string;

  /** Migration name extracted from folder name */
  migrationName: string;
}

/**
 * Drizzle Kit journal entry (legacy v2 format)
 *
 * Found in meta/_journal.json in older Drizzle projects.
 */
export interface DrizzleJournalEntry {
  idx: number;
  version: string;
  when: number;
  tag: string;
  breakpoints: boolean;
}

/**
 * Drizzle Kit journal file (legacy v2 format)
 */
export interface DrizzleJournal {
  version: string;
  dialect: string;
  entries: DrizzleJournalEntry[];
}

// =============================================================================
// TYPE GUARDS
// =============================================================================

/**
 * Check if value is a Migration
 */
export function isMigration(value: unknown): value is Migration {
  if (typeof value !== 'object' || value === null) return false;
  const m = value as Migration;
  return (
    typeof m.id === 'string' &&
    typeof m.sql === 'string' &&
    typeof m.checksum === 'string' &&
    m.createdAt instanceof Date
  );
}

/**
 * Check if migration source is an array
 */
export function isMigrationArray(source: MigrationSource): source is Migration[] {
  return Array.isArray(source);
}

/**
 * Check if migration source is a folder reference
 */
export function isFolderSource(
  source: MigrationSource
): source is { folder: string } {
  return (
    typeof source === 'object' &&
    source !== null &&
    'folder' in source &&
    typeof (source as { folder: string }).folder === 'string'
  );
}

/**
 * Check if migration source is a Drizzle folder reference
 */
export function isDrizzleSource(
  source: MigrationSource
): source is { drizzle: string } {
  return (
    typeof source === 'object' &&
    source !== null &&
    'drizzle' in source &&
    typeof (source as { drizzle: string }).drizzle === 'string'
  );
}

/**
 * Check if migration source is a loader function
 */
export function isLoaderFunction(
  source: MigrationSource
): source is () => Promise<Migration[]> {
  return typeof source === 'function';
}

// =============================================================================
// UTILITY TYPES
// =============================================================================

/**
 * Extract timestamp from migration ID
 */
export function extractTimestamp(migrationId: string): string {
  // Format: YYYYMMDDHHMMSS or YYYYMMDDHHMMSS_name
  const match = migrationId.match(/^(\d{14})/);
  return match ? match[1] : migrationId;
}

/**
 * Compare migration IDs for ordering
 */
export function compareMigrationIds(a: string, b: string): number {
  const timestampA = extractTimestamp(a);
  const timestampB = extractTimestamp(b);
  return timestampA.localeCompare(timestampB);
}

/**
 * Generate a migration ID from timestamp
 */
export function generateMigrationId(name?: string): string {
  const now = new Date();
  const timestamp = now
    .toISOString()
    .replace(/[-:T.Z]/g, '')
    .slice(0, 14);
  return name ? `${timestamp}_${name}` : timestamp;
}

/**
 * Calculate checksum for SQL content
 */
export async function calculateChecksum(sql: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(sql);

  // Use SubtleCrypto if available (Workers/Browser), otherwise simple hash
  if (typeof crypto !== 'undefined' && crypto.subtle) {
    const hashBuffer = await crypto.subtle.digest('SHA-256', data);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
  }

  // Fallback: simple hash for environments without SubtleCrypto
  let hash = 0;
  for (let i = 0; i < sql.length; i++) {
    const char = sql.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return Math.abs(hash).toString(16).padStart(8, '0');
}

/**
 * Synchronous checksum calculation (simple hash)
 */
export function calculateChecksumSync(sql: string): string {
  let hash = 0;
  for (let i = 0; i < sql.length; i++) {
    const char = sql.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash;
  }
  return Math.abs(hash).toString(16).padStart(8, '0');
}
