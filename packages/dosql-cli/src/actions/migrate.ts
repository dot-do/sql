/**
 * CLI action handler for the migrate command.
 */

import { resolve } from 'node:path';
import {
  findMigrations,
  createMigrationConnection,
  runMigrationsWithConnection,
} from '../commands/migrate.js';
import { toError } from '../utils/errors.js';
import { logInfo, logError, logSuccess, logSection, logList } from '../utils/logger.js';

/**
 * Options for the migrate action.
 */
export interface MigrateActionOptions {
  directory: string;
  url?: string;
  token?: string;
  dryRun: boolean;
  timeout: string;
}

/**
 * Handles the migrate CLI command.
 * Finds and applies database migrations, with optional dry-run support.
 *
 * @param options - Command options from CLI
 */
export async function handleMigrateAction(options: MigrateActionOptions): Promise<void> {
  try {
    const migrationsDir = resolve(options.directory);
    const migrations = await findMigrations(migrationsDir);

    if (migrations.length === 0) {
      logInfo('No migrations found.');
      return;
    }

    logInfo(`Found ${migrations.length} migration(s)`);

    // Check if we have a database URL (from option or env var)
    const url = options.url ?? process.env['DOSQL_URL'];

    if (!url) {
      // No database connection - show local migrations only
      logSection('Local migrations');
      logList(migrations.map(m => m.name));
      logInfo('');
      logInfo('To apply migrations, provide a database URL:');
      logInfo('  dosql migrate --url wss://your-database.example.com');
      logInfo('  or set DOSQL_URL environment variable');
      return;
    }

    // Connect to database
    logInfo(`\nConnecting to ${url}...`);
    const connectionOptions = {
      url,
      timeout: parseInt(options.timeout, 10),
      ...(options.token !== undefined && { token: options.token }),
    };
    const connection = await createMigrationConnection(connectionOptions);

    try {
      // Run migrations with the connection
      const result = await runMigrationsWithConnection({
        connection,
        migrationsDir,
        dryRun: options.dryRun,
      });

      if (options.dryRun) {
        logInfo('\nDry run - no changes applied');
        if (result.pending.length > 0) {
          logSection('Pending migrations');
          logList(result.pending);
        } else {
          logInfo('No pending migrations.');
        }
      } else {
        if (result.applied.length > 0) {
          logSection('Applied migrations');
          logList(result.applied);
        }

        if (result.error) {
          logError(`Migration failed: ${result.error.migration}`);
          logError(result.error.message);
          process.exit(1);
        }

        if (result.applied.length === 0 && !result.error) {
          logInfo('\nNo pending migrations to apply.');
        } else {
          logSuccess(`\nSuccessfully applied ${result.applied.length} migration(s).`);
        }
      }
    } finally {
      await connection.close();
    }
  } catch (error) {
    logError(toError(error).message);
    process.exit(1);
  }
}
