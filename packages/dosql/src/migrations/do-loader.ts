/**
 * DoSQL Migrations - .do/migrations Loader
 *
 * Loads migrations from the .do/migrations/*.sql folder structure.
 *
 * Convention:
 * ```
 * .do/
 *   migrations/
 *     001_create_users.sql
 *     002_add_posts.sql
 *     003_add_indexes.sql
 * ```
 *
 * Features:
 * - Numeric prefix ordering (001, 002, etc.)
 * - SQL file parsing with comment stripping
 * - Multi-statement file support
 * - Integration with SchemaTracker
 * - Out-of-order migration detection
 *
 * @module migrations/do-loader
 */

import {
  type Migration,
  type AppliedMigration,
  type MigrationResult,
  calculateChecksumSync,
} from './types.js';

import { type MigrationFileSystem } from './drizzle-compat.js';
import { type SchemaTracker } from './schema-tracker.js';
import { type DatabaseExecutor } from './runner.js';

// =============================================================================
// CONSTANTS
// =============================================================================

/**
 * Default path for .do/migrations folder
 */
export const DEFAULT_DO_MIGRATIONS_FOLDER = '.do/migrations';

/**
 * Regex for valid migration filename format: {number}_{name}.sql
 */
const MIGRATION_FILENAME_REGEX = /^(\d+)_(.+)\.sql$/;

// =============================================================================
// TYPES
// =============================================================================

/**
 * Options for loading migrations from .do/migrations folder
 */
export interface DoMigrationLoaderOptions {
  /** Base path to migrations folder */
  basePath: string;

  /** File system implementation */
  fs: MigrationFileSystem;

  /** Strip SQL comments before storing */
  stripComments?: boolean;
}

/**
 * Migration validation error
 */
export interface MigrationValidationError {
  type: 'gap' | 'duplicate' | 'out_of_order' | 'invalid_format';
  migrationId?: string;
  message: string;
  expected?: number;
  actual?: number;
}

/**
 * Validation result
 */
export interface MigrationValidationResult {
  valid: boolean;
  errors: MigrationValidationError[];
}

/**
 * Options for validation
 */
export interface ValidationOptions {
  /** Allow gaps in sequence (e.g., 001, 003) */
  allowGaps?: boolean;

  /** Allow applying migrations out of order */
  allowOutOfOrder?: boolean;

  /** Strict order enforcement */
  strictOrder?: boolean;

  /** Set of already applied migration IDs */
  appliedIds?: Set<string>;
}

// =============================================================================
// LOAD MIGRATIONS FROM FOLDER
// =============================================================================

/**
 * Load migrations from .do/migrations folder
 *
 * Reads all .sql files from the specified folder, parses them,
 * and returns them sorted by numeric prefix.
 *
 * @param options - Loader options
 * @returns Array of migrations sorted by numeric prefix
 */
export async function loadMigrationsFromFolder(
  options: DoMigrationLoaderOptions
): Promise<Migration[]> {
  const { basePath, fs, stripComments = false } = options;

  // Check if folder exists
  const folderExists = await fs.exists(basePath);
  if (!folderExists) {
    throw new Error(`Migration folder not found: ${basePath}`);
  }

  // List all files in the folder
  const entries = await fs.readdir(basePath);
  const migrations: Migration[] = [];

  for (const entry of entries) {
    // Skip non-.sql files
    if (!entry.endsWith('.sql')) {
      continue;
    }

    // Check if it's a valid migration filename
    const match = entry.match(MIGRATION_FILENAME_REGEX);
    if (!match) {
      continue;
    }

    // Check if it's a file (not a directory)
    const entryPath = `${basePath}/${entry}`;
    const isDir = await fs.isDirectory(entryPath);
    if (isDir) {
      continue;
    }

    // Read file content
    const content = await fs.readFile(entryPath);

    // Parse migration
    const migration = parseMigrationFile(
      entry,
      stripComments ? stripSqlComments(content) : content
    );

    migrations.push(migration);
  }

  // Sort by numeric prefix
  return sortMigrations(migrations);
}

// =============================================================================
// PARSE MIGRATION FILE
// =============================================================================

/**
 * Parse a migration file and create a Migration object
 *
 * @param filename - Migration filename (e.g., "001_create_users.sql")
 * @param content - SQL content of the file
 * @returns Migration object
 */
export function parseMigrationFile(
  filename: string,
  content: string
): Migration {
  const match = filename.match(MIGRATION_FILENAME_REGEX);

  if (!match) {
    throw new Error(
      `Invalid migration filename format: ${filename}. ` +
      `Expected format: {number}_{name}.sql (e.g., 001_create_users.sql)`
    );
  }

  const [, prefix, name] = match;
  const id = `${prefix}_${name}`;

  return {
    id,
    sql: content,
    name,
    createdAt: new Date(),
    checksum: calculateChecksumSync(content),
  };
}

// =============================================================================
// SORT MIGRATIONS
// =============================================================================

/**
 * Sort migrations by their numeric prefix
 *
 * @param migrations - Array of migrations to sort
 * @returns New sorted array
 */
export function sortMigrations(migrations: Migration[]): Migration[] {
  return [...migrations].sort((a, b) => {
    const numA = extractMigrationNumber(a.id);
    const numB = extractMigrationNumber(b.id);

    // Handle invalid IDs (shouldn't happen, but be safe)
    if (numA === null && numB === null) return 0;
    if (numA === null) return 1;
    if (numB === null) return -1;

    return numA - numB;
  });
}

/**
 * Extract the numeric prefix from a migration ID
 *
 * @param id - Migration ID (e.g., "001_create_users")
 * @returns Number or null if invalid
 */
export function extractMigrationNumber(id: string): number | null {
  if (!id) return null;

  const match = id.match(/^(\d+)_/);
  if (!match) return null;

  return parseInt(match[1], 10);
}

// =============================================================================
// VALIDATE MIGRATION SEQUENCE
// =============================================================================

/**
 * Validate migration sequence for gaps, duplicates, and order issues
 *
 * @param migrations - Array of migrations to validate
 * @param options - Validation options
 * @returns Validation result
 */
export function validateMigrationSequence(
  migrations: Migration[],
  options: ValidationOptions = {}
): MigrationValidationResult {
  const {
    allowGaps = false,
    allowOutOfOrder = false,
    strictOrder = false,
    appliedIds = new Set(),
  } = options;

  const errors: MigrationValidationError[] = [];

  if (migrations.length === 0) {
    return { valid: true, errors: [] };
  }

  const sorted = sortMigrations(migrations);
  const seenNumbers = new Map<number, string>();

  // Check for out-of-order in input (not sorted)
  for (let i = 1; i < migrations.length; i++) {
    const prevNum = extractMigrationNumber(migrations[i - 1].id);
    const currNum = extractMigrationNumber(migrations[i].id);

    if (prevNum !== null && currNum !== null && prevNum > currNum) {
      errors.push({
        type: 'out_of_order',
        migrationId: migrations[i].id,
        message: `Migration ${migrations[i].id} appears out of order (after ${migrations[i - 1].id})`,
        expected: prevNum + 1,
        actual: currNum,
      });
    }
  }

  // Check for strict order with applied migrations
  if (strictOrder && appliedIds.size > 0) {
    // Find the highest applied migration number
    let highestApplied = 0;
    for (const id of appliedIds) {
      const num = extractMigrationNumber(id);
      if (num !== null && num > highestApplied) {
        highestApplied = num;
      }
    }

    // Check for unapplied migrations before highest applied
    for (const migration of sorted) {
      const num = extractMigrationNumber(migration.id);
      if (num !== null && num < highestApplied && !appliedIds.has(migration.id)) {
        errors.push({
          type: 'out_of_order',
          migrationId: migration.id,
          message: `Migration ${migration.id} (${num}) is older than highest applied migration (${highestApplied}) but not applied`,
        });
      }
    }
  }

  // Check for duplicates and gaps
  let previousNumber: number | null = null;

  for (const migration of sorted) {
    const num = extractMigrationNumber(migration.id);

    if (num === null) {
      continue;
    }

    // Check for duplicates
    if (seenNumbers.has(num)) {
      errors.push({
        type: 'duplicate',
        migrationId: migration.id,
        message: `Duplicate migration number ${num}: ${seenNumbers.get(num)} and ${migration.id}`,
      });
    }
    seenNumbers.set(num, migration.id);

    // Check for gaps
    if (!allowGaps && previousNumber !== null && num !== previousNumber + 1) {
      // Allow gaps when out-of-order is allowed and some migrations are applied
      if (!allowOutOfOrder || appliedIds.size === 0) {
        errors.push({
          type: 'gap',
          migrationId: migration.id,
          message: `Gap in migration sequence: expected ${previousNumber + 1}, got ${num}`,
          expected: previousNumber + 1,
          actual: num,
        });
      }
    }

    previousNumber = num;
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

// =============================================================================
// SQL PARSING
// =============================================================================

/**
 * Parse SQL content into individual statements
 *
 * Handles semicolons inside strings correctly.
 *
 * @param sql - SQL content to parse
 * @returns Array of SQL statements
 */
export function parseSqlStatements(sql: string): string[] {
  const statements: string[] = [];
  let current = '';
  let inString = false;
  let stringChar = '';

  for (let i = 0; i < sql.length; i++) {
    const char = sql[i];
    const prevChar = i > 0 ? sql[i - 1] : '';

    // Handle string boundaries
    if ((char === "'" || char === '"') && prevChar !== '\\') {
      if (!inString) {
        inString = true;
        stringChar = char;
      } else if (char === stringChar) {
        // Check for escaped quote (doubled)
        if (i + 1 < sql.length && sql[i + 1] === char) {
          current += char;
          i++; // Skip next char
        } else {
          inString = false;
          stringChar = '';
        }
      }
    }

    // Handle statement end
    if (char === ';' && !inString) {
      const trimmed = current.trim();
      if (trimmed.length > 0) {
        statements.push(trimmed);
      }
      current = '';
      continue;
    }

    current += char;
  }

  // Add final statement if no trailing semicolon
  const trimmed = current.trim();
  if (trimmed.length > 0) {
    statements.push(trimmed);
  }

  return statements;
}

/**
 * Strip SQL comments from content
 *
 * Removes:
 * - Single-line comments (--)
 * - Multi-line comments (slash-star ... star-slash)
 *
 * Preserves comment-like content inside strings.
 *
 * @param sql - SQL content to strip
 * @returns SQL without comments
 */
export function stripSqlComments(sql: string): string {
  let result = '';
  let inString = false;
  let stringChar = '';
  let inSingleLineComment = false;
  let inMultiLineComment = false;

  for (let i = 0; i < sql.length; i++) {
    const char = sql[i];
    const nextChar = i + 1 < sql.length ? sql[i + 1] : '';
    const prevChar = i > 0 ? sql[i - 1] : '';

    // Handle end of single-line comment
    if (inSingleLineComment) {
      if (char === '\n') {
        inSingleLineComment = false;
        result += char;
      }
      continue;
    }

    // Handle multi-line comment
    if (inMultiLineComment) {
      if (char === '*' && nextChar === '/') {
        inMultiLineComment = false;
        i++; // Skip next char
      }
      continue;
    }

    // Check for string boundaries (not in comment)
    if ((char === "'" || char === '"') && prevChar !== '\\') {
      if (!inString) {
        inString = true;
        stringChar = char;
      } else if (char === stringChar) {
        // Check for escaped quote
        if (nextChar === char) {
          result += char + nextChar;
          i++;
          continue;
        }
        inString = false;
        stringChar = '';
      }
    }

    // Check for comment start (not in string)
    if (!inString) {
      // Single-line comment
      if (char === '-' && nextChar === '-') {
        inSingleLineComment = true;
        i++; // Skip next char
        continue;
      }

      // Multi-line comment
      if (char === '/' && nextChar === '*') {
        inMultiLineComment = true;
        i++; // Skip next char
        continue;
      }
    }

    result += char;
  }

  return result;
}

// =============================================================================
// DETECT APPLIED MIGRATIONS
// =============================================================================

/**
 * Get set of applied migration IDs from tracker
 *
 * @param tracker - Schema tracker instance
 * @returns Set of applied migration IDs
 */
export async function detectAppliedMigrations(
  tracker: SchemaTracker
): Promise<Set<string>> {
  const applied = await tracker.getAppliedMigrations();
  return new Set(applied.map(m => m.id));
}

/**
 * Get pending migrations that need to be applied
 *
 * @param migrations - All available migrations
 * @param appliedIds - Set of applied migration IDs
 * @returns Array of pending migrations, sorted
 */
export function getPendingMigrations(
  migrations: Migration[],
  appliedIds: Set<string>
): Migration[] {
  const pending = migrations.filter(m => !appliedIds.has(m.id));
  return sortMigrations(pending);
}

// =============================================================================
// APPLY MIGRATIONS IN ORDER
// =============================================================================

/**
 * Options for applying migrations
 */
export interface ApplyMigrationsOptions {
  /** Continue on error (mark failed and continue) */
  continueOnError?: boolean;

  /** Dry run mode - log SQL without executing */
  dryRun?: boolean;

  /** Log function */
  logger?: {
    info(message: string, ...args: unknown[]): void;
    error(message: string, ...args: unknown[]): void;
  };
}

/**
 * Apply migrations in order
 *
 * Applies all pending migrations to the database, recording
 * each successful application in the schema tracker.
 *
 * @param migrations - All available migrations
 * @param db - Database executor
 * @param tracker - Schema tracker
 * @param options - Apply options
 * @returns Migration result
 */
export async function applyMigrationsInOrder(
  migrations: Migration[],
  db: DatabaseExecutor,
  tracker: SchemaTracker,
  options: ApplyMigrationsOptions = {}
): Promise<MigrationResult> {
  const {
    continueOnError = false,
    dryRun = false,
    logger = { info: () => {}, error: () => {} },
  } = options;

  const startTime = Date.now();
  const applied: AppliedMigration[] = [];
  const failed: Array<{ migration: Migration; error: Error }> = [];

  // Get applied migrations
  const appliedIds = await detectAppliedMigrations(tracker);

  // Get pending migrations
  const pending = getPendingMigrations(migrations, appliedIds);

  if (pending.length === 0) {
    logger.info('No pending migrations');
    return {
      success: true,
      applied: [],
      failed: [],
      totalDurationMs: Date.now() - startTime,
      newVersion: await tracker.getCurrentVersion(),
    };
  }

  logger.info(`Found ${pending.length} pending migrations`);

  // Apply each pending migration
  for (const migration of pending) {
    const migrationStart = Date.now();

    try {
      if (!dryRun) {
        // Execute the SQL
        await db.exec(migration.sql);
      } else {
        logger.info(`DRY RUN: ${migration.id}`, migration.sql);
      }

      const appliedMigration: AppliedMigration = {
        id: migration.id,
        appliedAt: new Date(),
        checksum: migration.checksum,
        durationMs: Date.now() - migrationStart,
      };

      applied.push(appliedMigration);
      logger.info(`Applied migration: ${migration.id} (${appliedMigration.durationMs}ms)`);

      // Record in tracker
      if (!dryRun) {
        await tracker.recordMigration(appliedMigration);
      }
    } catch (error) {
      const migrationError = error instanceof Error ? error : new Error(String(error));
      logger.error(`Failed to apply migration: ${migration.id}`, migrationError);

      failed.push({ migration, error: migrationError });

      if (!continueOnError) {
        break;
      }
    }
  }

  const newVersion = applied.length > 0
    ? applied[applied.length - 1].id
    : await tracker.getCurrentVersion();

  return {
    success: failed.length === 0,
    applied,
    failed,
    totalDurationMs: Date.now() - startTime,
    newVersion,
  };
}
