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
    throw new Error(`Migrations directory not found: ${migrationsDir}`);
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
