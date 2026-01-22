/**
 * DoSQL Migrations - Schema Version Tracker
 *
 * Tracks schema versions in Durable Object storage for the
 * DB-per-tenant model with clone-on-demand migrations.
 *
 * @module migrations/schema-tracker
 */

import {
  type Migration,
  type MigrationStatus,
  type CloneMigrationContext,
  type MigrationSource,
  type AppliedMigration,
  isMigrationArray,
  isFolderSource,
  isDrizzleSource,
  isLoaderFunction,
  compareMigrationIds,
  calculateChecksumSync,
} from './types.js';
import {
  MigrationRunner,
  type DatabaseExecutor,
} from './runner.js';
import {
  loadDrizzleMigrations,
  type MigrationFileSystem,
} from './drizzle-compat.js';

// =============================================================================
// STORAGE INTERFACE
// =============================================================================

/**
 * Storage interface for schema tracking
 *
 * Compatible with Cloudflare Durable Object storage.
 */
export interface SchemaStorage {
  /** Get a value by key */
  get<T>(key: string): Promise<T | undefined>;

  /** Set a value */
  put<T>(key: string, value: T): Promise<void>;

  /** Delete a value */
  delete(key: string): Promise<boolean>;

  /** List keys with prefix */
  list(options?: { prefix?: string }): Promise<Map<string, unknown>>;
}

// =============================================================================
// SCHEMA TRACKER
// =============================================================================

/**
 * Schema version tracker
 *
 * Manages schema version state in DO storage and handles
 * migration-on-clone scenarios.
 */
export class SchemaTracker {
  private readonly storage: SchemaStorage;
  private readonly storagePrefix: string;

  constructor(storage: SchemaStorage, prefix = '_schema') {
    this.storage = storage;
    this.storagePrefix = prefix;
  }

  // ===========================================================================
  // VERSION TRACKING
  // ===========================================================================

  /**
   * Get current schema version
   */
  async getCurrentVersion(): Promise<string | null> {
    const version = await this.storage.get<string>(
      `${this.storagePrefix}:version`
    );
    return version ?? null;
  }

  /**
   * Set current schema version
   */
  async setCurrentVersion(version: string): Promise<void> {
    await this.storage.put(`${this.storagePrefix}:version`, version);
  }

  /**
   * Get all applied migrations
   */
  async getAppliedMigrations(): Promise<AppliedMigration[]> {
    const entries = await this.storage.list({
      prefix: `${this.storagePrefix}:migration:`,
    });

    const migrations: AppliedMigration[] = [];

    for (const [, value] of entries) {
      if (isAppliedMigration(value)) {
        migrations.push(value);
      }
    }

    return migrations.sort((a, b) => compareMigrationIds(a.id, b.id));
  }

  /**
   * Record a migration as applied
   */
  async recordMigration(migration: AppliedMigration): Promise<void> {
    await this.storage.put(
      `${this.storagePrefix}:migration:${migration.id}`,
      migration
    );
    await this.setCurrentVersion(migration.id);
  }

  /**
   * Remove a migration record (for rollback)
   */
  async removeMigrationRecord(id: string): Promise<void> {
    await this.storage.delete(`${this.storagePrefix}:migration:${id}`);

    // Update current version to previous
    const applied = await this.getAppliedMigrations();
    const filtered = applied.filter(m => m.id !== id);

    if (filtered.length > 0) {
      await this.setCurrentVersion(filtered[filtered.length - 1].id);
    } else {
      await this.storage.delete(`${this.storagePrefix}:version`);
    }
  }

  /**
   * Check if database needs migration
   */
  async needsMigration(targetVersion: string): Promise<boolean> {
    const currentVersion = await this.getCurrentVersion();

    if (currentVersion === null) {
      return true; // No migrations applied yet
    }

    return compareMigrationIds(currentVersion, targetVersion) < 0;
  }

  // ===========================================================================
  // CLONE MIGRATION CONTEXT
  // ===========================================================================

  /**
   * Get migration context for a cloned database
   *
   * When a DB is cloned from a template, this determines what
   * migrations need to be applied to bring it up to date.
   */
  async getCloneMigrationContext(
    allMigrations: Migration[],
    targetVersion?: string
  ): Promise<CloneMigrationContext> {
    const currentVersion = await this.getCurrentVersion();
    const sorted = [...allMigrations].sort((a, b) =>
      compareMigrationIds(a.id, b.id)
    );

    // Default target is latest migration
    const target = targetVersion ?? (sorted.length > 0 ? sorted[sorted.length - 1].id : '');

    // Find pending migrations
    const pending = sorted.filter(m => {
      const isAfterCurrent = currentVersion === null ||
        compareMigrationIds(m.id, currentVersion) > 0;
      const isBeforeOrAtTarget = compareMigrationIds(m.id, target) <= 0;
      return isAfterCurrent && isBeforeOrAtTarget;
    });

    return {
      sourceVersion: currentVersion,
      targetVersion: target,
      pendingMigrations: pending,
      needsMigration: pending.length > 0,
    };
  }

  /**
   * Store source version when cloning
   *
   * This marks the database as a clone that may need migration.
   */
  async markAsClone(sourceVersion: string | null): Promise<void> {
    await this.storage.put(`${this.storagePrefix}:clone_source`, sourceVersion);
  }

  /**
   * Check if database is a clone
   */
  async isClone(): Promise<boolean> {
    const source = await this.storage.get(`${this.storagePrefix}:clone_source`);
    return source !== undefined;
  }

  /**
   * Clear clone marker after migration
   */
  async clearCloneMarker(): Promise<void> {
    await this.storage.delete(`${this.storagePrefix}:clone_source`);
  }

  // ===========================================================================
  // SCHEMA SNAPSHOT
  // ===========================================================================

  /**
   * Store schema snapshot
   */
  async storeSnapshot(version: string, snapshot: unknown): Promise<void> {
    await this.storage.put(`${this.storagePrefix}:snapshot:${version}`, snapshot);
  }

  /**
   * Get schema snapshot for a version
   */
  async getSnapshot<T = unknown>(version: string): Promise<T | undefined> {
    return this.storage.get<T>(`${this.storagePrefix}:snapshot:${version}`);
  }

  // ===========================================================================
  // METADATA
  // ===========================================================================

  /**
   * Get migration metadata
   */
  async getMetadata(): Promise<{
    currentVersion: string | null;
    totalMigrations: number;
    isClone: boolean;
    lastMigrationAt: Date | null;
  }> {
    const currentVersion = await this.getCurrentVersion();
    const applied = await this.getAppliedMigrations();
    const isClone = await this.isClone();

    return {
      currentVersion,
      totalMigrations: applied.length,
      isClone,
      lastMigrationAt: applied.length > 0
        ? applied[applied.length - 1].appliedAt
        : null,
    };
  }

  /**
   * Clear all schema tracking data
   */
  async clear(): Promise<void> {
    const entries = await this.storage.list({
      prefix: `${this.storagePrefix}:`,
    });

    for (const [key] of entries) {
      await this.storage.delete(key);
    }
  }
}

// =============================================================================
// TYPE GUARD
// =============================================================================

function isAppliedMigration(value: unknown): value is AppliedMigration {
  if (typeof value !== 'object' || value === null) return false;
  const m = value as AppliedMigration;
  return (
    typeof m.id === 'string' &&
    (m.appliedAt instanceof Date || typeof m.appliedAt === 'string') &&
    typeof m.checksum === 'string' &&
    typeof m.durationMs === 'number'
  );
}

// =============================================================================
// DB() INTEGRATION
// =============================================================================

/**
 * Options for DB migration integration
 */
export interface DBMigrationOptions {
  /** Migration source (inline, folder, or Drizzle) */
  migrations: MigrationSource;

  /** Storage for tracking */
  storage: SchemaStorage;

  /** Database executor */
  db: DatabaseExecutor;

  /** File system for loading migrations (if using folder source) */
  fs?: MigrationFileSystem;

  /** Auto-migrate on first access */
  autoMigrate?: boolean;

  /** Target version (default: latest) */
  targetVersion?: string;

  /** Log function */
  logger?: {
    info: (msg: string, ...args: unknown[]) => void;
    error: (msg: string, ...args: unknown[]) => void;
  };
}

/**
 * Initialize database with migrations
 *
 * This is the main integration point for DB().
 * Call this when a tenant database is accessed.
 */
export async function initializeWithMigrations(
  options: DBMigrationOptions
): Promise<MigrationStatus> {
  const {
    migrations: source,
    storage,
    db,
    fs,
    autoMigrate = true,
    targetVersion,
    logger = { info: () => {}, error: () => {} },
  } = options;

  // Load migrations from source
  const migrations = await loadMigrations(source, fs);
  const tracker = new SchemaTracker(storage);

  // Get migration context
  const context = await tracker.getCloneMigrationContext(
    migrations,
    targetVersion
  );

  logger.info(
    `Migration status: ${context.needsMigration ? 'needs migration' : 'up to date'}`,
    {
      current: context.sourceVersion,
      target: context.targetVersion,
      pending: context.pendingMigrations.length,
    }
  );

  // Auto-migrate if enabled and needed
  if (autoMigrate && context.needsMigration) {
    logger.info(`Applying ${context.pendingMigrations.length} migrations...`);

    const runner = new MigrationRunner(db, {
      logger: {
        info: (msg) => logger.info(msg),
        warn: (msg) => logger.info(`WARN: ${msg}`),
        error: (msg) => logger.error(msg),
        debug: () => {},
      },
    });

    const result = await runner.migrate(context.pendingMigrations);

    if (!result.success) {
      logger.error('Migration failed', result.failed);
      throw new Error(
        `Migration failed: ${result.failed.map(f => f.error.message).join(', ')}`
      );
    }

    // Record migrations in tracker
    for (const applied of result.applied) {
      await tracker.recordMigration(applied);
    }

    // Clear clone marker if this was a clone
    if (await tracker.isClone()) {
      await tracker.clearCloneMarker();
    }

    logger.info(`Applied ${result.applied.length} migrations in ${result.totalDurationMs}ms`);
  }

  // Return final status
  const applied = await tracker.getAppliedMigrations();
  const currentVersion = await tracker.getCurrentVersion();

  return {
    currentVersion,
    applied,
    pending: context.pendingMigrations.filter(
      m => !applied.some(a => a.id === m.id)
    ),
    needsMigration: false,
  };
}

/**
 * Load migrations from a source
 */
async function loadMigrations(
  source: MigrationSource,
  fs?: MigrationFileSystem
): Promise<Migration[]> {
  if (isMigrationArray(source)) {
    return source;
  }

  if (isLoaderFunction(source)) {
    return source();
  }

  if (isDrizzleSource(source)) {
    if (!fs) {
      throw new Error('File system required for Drizzle migrations');
    }
    return loadDrizzleMigrations({
      basePath: source.drizzle,
      fs,
      includeSnapshots: true,
    });
  }

  if (isFolderSource(source)) {
    if (!fs) {
      throw new Error('File system required for folder migrations');
    }
    // Load from custom folder format
    return loadDrizzleMigrations({
      basePath: source.folder,
      fs,
      includeSnapshots: true,
    });
  }

  throw new Error('Invalid migration source');
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a schema tracker
 */
export function createSchemaTracker(
  storage: SchemaStorage,
  prefix?: string
): SchemaTracker {
  return new SchemaTracker(storage, prefix);
}

/**
 * Create an in-memory storage for testing
 */
export function createInMemoryStorage(): SchemaStorage {
  const data = new Map<string, unknown>();

  return {
    async get<T>(key: string): Promise<T | undefined> {
      return data.get(key) as T | undefined;
    },

    async put<T>(key: string, value: T): Promise<void> {
      data.set(key, value);
    },

    async delete(key: string): Promise<boolean> {
      return data.delete(key);
    },

    async list(options?: { prefix?: string }): Promise<Map<string, unknown>> {
      if (!options?.prefix) {
        return new Map(data);
      }

      const result = new Map<string, unknown>();
      for (const [key, value] of data) {
        if (key.startsWith(options.prefix)) {
          result.set(key, value);
        }
      }
      return result;
    },
  };
}

// =============================================================================
// CLONE WORKFLOW HELPERS
// =============================================================================

/**
 * Prepare a database clone for migration
 *
 * Call this when cloning a template database to a new tenant.
 */
export async function prepareClone(
  sourceStorage: SchemaStorage,
  targetStorage: SchemaStorage
): Promise<void> {
  const sourceTracker = new SchemaTracker(sourceStorage);
  const targetTracker = new SchemaTracker(targetStorage);

  // Get source version
  const sourceVersion = await sourceTracker.getCurrentVersion();

  // Mark target as a clone
  await targetTracker.markAsClone(sourceVersion);

  // Copy applied migrations from source
  const applied = await sourceTracker.getAppliedMigrations();
  for (const migration of applied) {
    await targetTracker.recordMigration(migration);
  }
}

/**
 * Check if a clone is ready (migrations complete)
 */
export async function isCloneReady(
  storage: SchemaStorage,
  migrations: Migration[]
): Promise<boolean> {
  const tracker = new SchemaTracker(storage);
  const context = await tracker.getCloneMigrationContext(migrations);
  return !context.needsMigration;
}

// =============================================================================
// MIGRATION GENERATION HELPERS
// =============================================================================

/**
 * Create a new migration from SQL
 *
 * Generates a properly formatted migration with timestamp ID.
 */
export function createMigrationFromSql(
  sql: string,
  name: string
): Migration {
  const now = new Date();
  const timestamp = now
    .toISOString()
    .replace(/[-:T.Z]/g, '')
    .slice(0, 14);

  return {
    id: `${timestamp}_${name}`,
    sql,
    name,
    createdAt: now,
    checksum: calculateChecksumSync(sql),
  };
}

/**
 * Create a migration with up/down SQL
 */
export function createReversibleMigration(
  upSql: string,
  downSql: string,
  name: string
): Migration {
  const migration = createMigrationFromSql(upSql, name);
  return {
    ...migration,
    downSql,
  };
}
