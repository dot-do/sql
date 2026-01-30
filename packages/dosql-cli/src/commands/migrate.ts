import { promises as fs } from 'node:fs';
import { join, basename } from 'node:path';

export interface Migration {
  name: string;
  content: string;
  path: string;
}

export interface MigrateOptions {
  migrations: Migration[];
  executor: (sql: string, name: string) => Promise<void>;
  appliedMigrations?: string[];
  dryRun?: boolean;
}

export interface MigrateResult {
  applied: string[];
  pending: string[];
  error?: {
    migration: string;
    message: string;
  };
}

export async function findMigrations(migrationsDir: string): Promise<Migration[]> {
  // Check if directory exists
  const exists = await fs.access(migrationsDir).then(() => true).catch(() => false);
  if (!exists) {
    throw new Error(
      `Migrations directory not found: ${migrationsDir}\n\n` +
      `To fix this, either:\n` +
      `  1. Run 'dosql init' to create the default project structure, or\n` +
      `  2. Create the migrations directory manually: mkdir -p ${migrationsDir}\n` +
      `  3. Use --directory to specify a different migrations path`
    );
  }

  const entries = await fs.readdir(migrationsDir, { withFileTypes: true });
  const migrations: Migration[] = [];

  for (const entry of entries) {
    if (entry.isFile() && entry.name.endsWith('.sql')) {
      const filePath = join(migrationsDir, entry.name);
      const content = await fs.readFile(filePath, 'utf-8');
      const name = basename(entry.name, '.sql');

      migrations.push({
        name,
        content,
        path: filePath,
      });
    }
  }

  // Sort by name (which should have numeric prefixes like 001_, 002_, etc.)
  migrations.sort((a, b) => a.name.localeCompare(b.name));

  return migrations;
}

export async function runMigrations(options: MigrateOptions): Promise<MigrateResult> {
  const { migrations, executor, appliedMigrations = [], dryRun = false } = options;

  const applied: string[] = [];
  const pending: string[] = [];

  for (const migration of migrations) {
    // Skip already applied migrations
    if (appliedMigrations.includes(migration.name)) {
      continue;
    }

    if (dryRun) {
      pending.push(migration.name);
      continue;
    }

    try {
      await executor(migration.content, migration.name);
      applied.push(migration.name);
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      return {
        applied,
        pending: [],
        error: {
          migration: migration.name,
          message: error.message,
        },
      };
    }
  }

  return {
    applied,
    pending,
  };
}

/**
 * Connection options for migrations
 */
export interface MigrationConnectionOptions {
  url: string;
  token?: string;
  timeout?: number;
}

/**
 * Migration connection interface
 */
export interface MigrationConnection {
  execute: (sql: string) => Promise<void>;
  close: () => Promise<void>;
}

/**
 * Create a connection for running migrations.
 * TODO: Implement actual database connection
 */
export async function createMigrationConnection(
  _options: MigrationConnectionOptions
): Promise<MigrationConnection> {
  // TODO: Implement WebSocket connection to DoSQL
  throw new Error('Remote migrations not yet implemented. Please run migrations locally.');
}

/**
 * Options for running migrations with a connection
 */
export interface RunMigrationsWithConnectionOptions {
  connection: MigrationConnection;
  migrationsDir: string;
  dryRun?: boolean;
}

/**
 * Run migrations with an established connection.
 * TODO: Implement migration tracking table
 */
export async function runMigrationsWithConnection(
  options: RunMigrationsWithConnectionOptions
): Promise<MigrateResult> {
  const migrations = await findMigrations(options.migrationsDir);

  // TODO: Query applied migrations from _dosql_migrations table
  const appliedMigrations: string[] = [];

  const migrateOptions = {
    migrations,
    executor: async (sql: string, _name: string) => {
      await options.connection.execute(sql);
    },
    appliedMigrations,
    ...(options.dryRun !== undefined && { dryRun: options.dryRun }),
  };
  return runMigrations(migrateOptions);
}
