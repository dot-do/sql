/**
 * DoSQL Migrations - Migration Runner
 *
 * Applies pending migrations to a database instance.
 * Supports transactional migrations, dry-run mode, and batched execution.
 *
 * @module migrations/runner
 */

import {
  type Migration,
  type AppliedMigration,
  type MigrationResult,
  type MigrationStatus,
  type MigrationRunnerOptions,
  type MigrationLogger,
  type MigrationSource,
  compareMigrationIds,
  calculateChecksumSync,
  isMigrationArray,
  isFolderSource,
} from './types.js';

import { type MigrationFileSystem } from './drizzle-compat.js';

// =============================================================================
// DEFAULT LOGGER
// =============================================================================

const noopLogger: MigrationLogger = {
  info: () => {},
  warn: () => {},
  error: () => {},
  debug: () => {},
};

const consoleLogger: MigrationLogger = {
  info: (msg, ...args) => console.log(`[migrations] ${msg}`, ...args),
  warn: (msg, ...args) => console.warn(`[migrations] ${msg}`, ...args),
  error: (msg, ...args) => console.error(`[migrations] ${msg}`, ...args),
  debug: (msg, ...args) => console.debug(`[migrations] ${msg}`, ...args),
};

// =============================================================================
// DATABASE EXECUTOR INTERFACE
// =============================================================================

/**
 * Minimal database interface for running migrations
 *
 * This abstracts over different database implementations (SQLite, PGLite, D1, etc.)
 */
export interface DatabaseExecutor {
  /** Execute SQL and return results */
  exec(sql: string): Promise<void> | void;

  /** Run a query and get results */
  query<T = unknown>(sql: string, params?: unknown[]): Promise<T[]> | T[];

  /** Run SQL that modifies data */
  run(sql: string, params?: unknown[]): Promise<{ changes: number }> | { changes: number };

  /** Begin transaction */
  beginTransaction?(): Promise<void> | void;

  /** Commit transaction */
  commit?(): Promise<void> | void;

  /** Rollback transaction */
  rollback?(): Promise<void> | void;
}

// =============================================================================
// MIGRATION RUNNER
// =============================================================================

/**
 * Migration Runner
 *
 * Manages applying migrations to a database.
 */
export class MigrationRunner {
  private readonly db: DatabaseExecutor;
  private readonly options: Required<MigrationRunnerOptions>;
  private readonly log: MigrationLogger;

  constructor(db: DatabaseExecutor, options: MigrationRunnerOptions = {}) {
    this.db = db;
    this.options = {
      migrationsTable: options.migrationsTable ?? '__dosql_migrations',
      migrationsSchema: options.migrationsSchema ?? 'public',
      allowProduction: options.allowProduction ?? false,
      dryRun: options.dryRun ?? false,
      transactional: options.transactional ?? true,
      continueOnError: options.continueOnError ?? false,
      timeout: options.timeout ?? 30000,
      logger: options.logger ?? consoleLogger,
    };
    this.log = this.options.logger;
  }

  // ===========================================================================
  // PUBLIC API
  // ===========================================================================

  /**
   * Get current migration status
   */
  async getStatus(migrations: Migration[]): Promise<MigrationStatus> {
    await this.ensureMigrationsTable();

    const applied = await this.getAppliedMigrations();
    const appliedIds = new Set(applied.map(m => m.id));

    // Sort migrations by ID
    const sortedMigrations = [...migrations].sort((a, b) =>
      compareMigrationIds(a.id, b.id)
    );

    const pending = sortedMigrations.filter(m => !appliedIds.has(m.id));
    const currentVersion = applied.length > 0
      ? applied[applied.length - 1].id
      : null;

    return {
      currentVersion,
      applied,
      pending,
      needsMigration: pending.length > 0,
    };
  }

  /**
   * Apply all pending migrations
   */
  async migrate(migrations: Migration[]): Promise<MigrationResult> {
    const startTime = Date.now();
    const applied: AppliedMigration[] = [];
    const failed: Array<{ migration: Migration; error: Error }> = [];

    // Get status
    const status = await this.getStatus(migrations);

    if (!status.needsMigration) {
      this.log.info('No pending migrations');
      return {
        success: true,
        applied: [],
        failed: [],
        totalDurationMs: Date.now() - startTime,
        newVersion: status.currentVersion,
      };
    }

    this.log.info(`Found ${status.pending.length} pending migrations`);

    // Apply each pending migration
    for (const migration of status.pending) {
      const migrationStart = Date.now();

      try {
        await this.applyMigration(migration);

        const appliedMigration: AppliedMigration = {
          id: migration.id,
          appliedAt: new Date(),
          checksum: migration.checksum,
          durationMs: Date.now() - migrationStart,
        };

        applied.push(appliedMigration);
        this.log.info(`Applied migration: ${migration.id} (${appliedMigration.durationMs}ms)`);

        // Record in migrations table
        if (!this.options.dryRun) {
          await this.recordMigration(appliedMigration);
        }
      } catch (error) {
        const migrationError = error instanceof Error ? error : new Error(String(error));
        this.log.error(`Failed to apply migration: ${migration.id}`, migrationError);

        failed.push({ migration, error: migrationError });

        if (!this.options.continueOnError) {
          break;
        }
      }
    }

    const newVersion = applied.length > 0
      ? applied[applied.length - 1].id
      : status.currentVersion;

    return {
      success: failed.length === 0,
      applied,
      failed,
      totalDurationMs: Date.now() - startTime,
      newVersion,
    };
  }

  /**
   * Rollback to a specific version
   */
  async rollbackTo(
    migrations: Migration[],
    targetVersion: string | null
  ): Promise<MigrationResult> {
    const startTime = Date.now();
    const applied: AppliedMigration[] = [];
    const failed: Array<{ migration: Migration; error: Error }> = [];

    const status = await this.getStatus(migrations);

    if (!status.currentVersion) {
      this.log.info('No migrations to rollback');
      return {
        success: true,
        applied: [],
        failed: [],
        totalDurationMs: Date.now() - startTime,
        newVersion: null,
      };
    }

    // Find migrations to rollback (in reverse order)
    const toRollback = status.applied
      .filter(m => targetVersion === null || compareMigrationIds(m.id, targetVersion) > 0)
      .reverse();

    if (toRollback.length === 0) {
      this.log.info('No migrations to rollback');
      return {
        success: true,
        applied: [],
        failed: [],
        totalDurationMs: Date.now() - startTime,
        newVersion: status.currentVersion,
      };
    }

    this.log.info(`Rolling back ${toRollback.length} migrations`);

    // Create migration map for down SQL lookup
    const migrationMap = new Map(migrations.map(m => [m.id, m]));

    for (const appliedMigration of toRollback) {
      const migration = migrationMap.get(appliedMigration.id);

      if (!migration?.downSql) {
        const error = new Error(`No down migration for: ${appliedMigration.id}`);
        this.log.error(error.message);
        failed.push({ migration: migration ?? { id: appliedMigration.id } as Migration, error });

        if (!this.options.continueOnError) {
          break;
        }
        continue;
      }

      const migrationStart = Date.now();

      try {
        await this.rollbackMigration(migration);

        const rolledBack: AppliedMigration = {
          id: migration.id,
          appliedAt: new Date(),
          checksum: migration.checksum,
          durationMs: Date.now() - migrationStart,
        };

        applied.push(rolledBack);
        this.log.info(`Rolled back migration: ${migration.id} (${rolledBack.durationMs}ms)`);

        // Remove from migrations table
        if (!this.options.dryRun) {
          await this.removeMigrationRecord(migration.id);
        }
      } catch (error) {
        const migrationError = error instanceof Error ? error : new Error(String(error));
        this.log.error(`Failed to rollback migration: ${migration.id}`, migrationError);

        failed.push({ migration, error: migrationError });

        if (!this.options.continueOnError) {
          break;
        }
      }
    }

    return {
      success: failed.length === 0,
      applied,
      failed,
      totalDurationMs: Date.now() - startTime,
      newVersion: targetVersion,
    };
  }

  /**
   * Validate migration checksums
   */
  async validateChecksums(migrations: Migration[]): Promise<{
    valid: boolean;
    mismatches: Array<{ id: string; expected: string; actual: string }>;
  }> {
    const status = await this.getStatus(migrations);
    const migrationMap = new Map(migrations.map(m => [m.id, m]));
    const mismatches: Array<{ id: string; expected: string; actual: string }> = [];

    for (const applied of status.applied) {
      const migration = migrationMap.get(applied.id);
      if (migration && migration.checksum !== applied.checksum) {
        mismatches.push({
          id: applied.id,
          expected: applied.checksum,
          actual: migration.checksum,
        });
      }
    }

    return {
      valid: mismatches.length === 0,
      mismatches,
    };
  }

  // ===========================================================================
  // PRIVATE METHODS
  // ===========================================================================

  /**
   * Ensure migrations table exists
   */
  private async ensureMigrationsTable(): Promise<void> {
    const tableName = this.options.migrationsTable;

    const createTableSql = `
      CREATE TABLE IF NOT EXISTS "${tableName}" (
        id TEXT PRIMARY KEY,
        applied_at TEXT NOT NULL,
        checksum TEXT NOT NULL,
        duration_ms INTEGER NOT NULL
      )
    `;

    if (this.options.dryRun) {
      this.log.debug('DRY RUN:', createTableSql);
      return;
    }

    await this.db.exec(createTableSql);
  }

  /**
   * Get list of applied migrations
   */
  private async getAppliedMigrations(): Promise<AppliedMigration[]> {
    const tableName = this.options.migrationsTable;

    try {
      const rows = await this.db.query<{
        id: string;
        applied_at: string;
        checksum: string;
        duration_ms: number;
      }>(`SELECT id, applied_at, checksum, duration_ms FROM "${tableName}" ORDER BY id ASC`);

      return rows.map(row => ({
        id: row.id,
        appliedAt: new Date(row.applied_at),
        checksum: row.checksum,
        durationMs: row.duration_ms,
      }));
    } catch {
      // Table doesn't exist yet
      return [];
    }
  }

  /**
   * Apply a single migration
   */
  private async applyMigration(migration: Migration): Promise<void> {
    this.log.debug(`Applying migration: ${migration.id}`);

    if (this.options.dryRun) {
      this.log.info('DRY RUN:', migration.sql);
      return;
    }

    if (this.options.transactional && this.db.beginTransaction) {
      try {
        await this.db.beginTransaction();
        await this.db.exec(migration.sql);
        await this.db.commit?.();
      } catch (error) {
        await this.db.rollback?.();
        throw error;
      }
    } else {
      await this.db.exec(migration.sql);
    }
  }

  /**
   * Rollback a single migration
   */
  private async rollbackMigration(migration: Migration): Promise<void> {
    if (!migration.downSql) {
      throw new Error(`No down migration for: ${migration.id}`);
    }

    this.log.debug(`Rolling back migration: ${migration.id}`);

    if (this.options.dryRun) {
      this.log.info('DRY RUN:', migration.downSql);
      return;
    }

    if (this.options.transactional && this.db.beginTransaction) {
      try {
        await this.db.beginTransaction();
        await this.db.exec(migration.downSql);
        await this.db.commit?.();
      } catch (error) {
        await this.db.rollback?.();
        throw error;
      }
    } else {
      await this.db.exec(migration.downSql);
    }
  }

  /**
   * Record a migration as applied
   */
  private async recordMigration(migration: AppliedMigration): Promise<void> {
    const tableName = this.options.migrationsTable;

    await this.db.run(
      `INSERT INTO "${tableName}" (id, applied_at, checksum, duration_ms) VALUES (?, ?, ?, ?)`,
      [
        migration.id,
        migration.appliedAt.toISOString(),
        migration.checksum,
        migration.durationMs,
      ]
    );
  }

  /**
   * Remove a migration record (for rollback)
   */
  private async removeMigrationRecord(id: string): Promise<void> {
    const tableName = this.options.migrationsTable;
    await this.db.run(`DELETE FROM "${tableName}" WHERE id = ?`, [id]);
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a migration runner
 */
export function createMigrationRunner(
  db: DatabaseExecutor,
  options?: MigrationRunnerOptions
): MigrationRunner {
  return new MigrationRunner(db, options);
}

/**
 * Create a migration from SQL
 */
export function createMigration(
  id: string,
  sql: string,
  options?: {
    downSql?: string;
    name?: string;
  }
): Migration {
  return {
    id,
    sql,
    downSql: options?.downSql,
    name: options?.name,
    createdAt: new Date(),
    checksum: calculateChecksumSync(sql),
  };
}

/**
 * Create migrations from an array of SQL strings
 */
export function createMigrations(
  migrations: Array<{
    id: string;
    sql: string;
    downSql?: string;
    name?: string;
  }>
): Migration[] {
  return migrations.map(m => createMigration(m.id, m.sql, {
    downSql: m.downSql,
    name: m.name,
  }));
}

// =============================================================================
// BATCH MIGRATION HELPERS
// =============================================================================

/**
 * Combine multiple SQL statements into a single migration
 */
export function batchMigrations(statements: string[]): string {
  return statements
    .map(s => s.trim())
    .filter(s => s.length > 0)
    .join(';\n\n');
}

/**
 * Split a migration SQL into individual statements
 */
export function splitMigrationSql(sql: string): string[] {
  // Simple split - doesn't handle strings containing semicolons
  // For production, use a proper SQL parser
  return sql
    .split(';')
    .map(s => s.trim())
    .filter(s => s.length > 0);
}

// =============================================================================
// MIGRATION ORDERING
// =============================================================================

/**
 * Sort migrations by ID (timestamp order)
 */
export function sortMigrations(migrations: Migration[]): Migration[] {
  return [...migrations].sort((a, b) => compareMigrationIds(a.id, b.id));
}

/**
 * Get migrations needed to go from one version to another
 */
export function getMigrationsBetween(
  migrations: Migration[],
  fromVersion: string | null,
  toVersion: string
): Migration[] {
  const sorted = sortMigrations(migrations);

  return sorted.filter(m => {
    const isAfterFrom = fromVersion === null || compareMigrationIds(m.id, fromVersion) > 0;
    const isBeforeOrAtTo = compareMigrationIds(m.id, toVersion) <= 0;
    return isAfterFrom && isBeforeOrAtTo;
  });
}

/**
 * Detect out-of-order migrations
 *
 * Returns migrations that were applied out of timestamp order,
 * which can happen in parallel branch development.
 */
export function detectOutOfOrderMigrations(
  migrations: Migration[],
  applied: AppliedMigration[]
): Migration[] {
  const appliedIds = new Set(applied.map(a => a.id));
  const sorted = sortMigrations(migrations);

  let lastAppliedIndex = -1;
  const outOfOrder: Migration[] = [];

  for (let i = 0; i < sorted.length; i++) {
    const migration = sorted[i];

    if (appliedIds.has(migration.id)) {
      if (lastAppliedIndex !== -1) {
        // Check if any migrations between lastAppliedIndex and i are not applied
        for (let j = lastAppliedIndex + 1; j < i; j++) {
          if (!appliedIds.has(sorted[j].id)) {
            outOfOrder.push(sorted[j]);
          }
        }
      }
      lastAppliedIndex = i;
    }
  }

  return outOfOrder;
}

// =============================================================================
// DO MIGRATIONS FOLDER INTEGRATION
// =============================================================================

/**
 * Options for running migrations from .do/migrations folder
 */
export interface DoMigrationsRunnerOptions extends MigrationRunnerOptions {
  /** File system implementation for loading migrations */
  fs?: MigrationFileSystem;

  /** Base path to migrations folder (default: .do/migrations) */
  migrationsPath?: string;
}

/**
 * Load and run migrations from a source
 *
 * Supports:
 * - Migration array (inline)
 * - Folder source ({ folder: '.do/migrations' })
 * - File system loading
 *
 * @param db - Database executor
 * @param source - Migration source
 * @param options - Runner options
 * @returns Migration result
 */
export async function runMigrationsFromSource(
  db: DatabaseExecutor,
  source: MigrationSource,
  options: DoMigrationsRunnerOptions = {}
): Promise<MigrationResult> {
  let migrations: Migration[];

  if (isMigrationArray(source)) {
    migrations = source;
  } else if (isFolderSource(source)) {
    if (!options.fs) {
      throw new Error('File system (fs) required for folder migrations');
    }

    // Dynamically import do-loader to avoid circular dependency
    const { loadMigrationsFromFolder } = await import('./do-loader.js');
    migrations = await loadMigrationsFromFolder({
      basePath: source.folder,
      fs: options.fs,
    });
  } else if (typeof source === 'function') {
    migrations = await source();
  } else {
    throw new Error('Unsupported migration source');
  }

  const runner = new MigrationRunner(db, options);
  return runner.migrate(migrations);
}
