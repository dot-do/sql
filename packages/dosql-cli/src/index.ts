#!/usr/bin/env node

/**
 * DoSQL CLI - Command-line interface for DoSQL database management.
 *
 * Provides commands for:
 * - `init` - Initialize a new DoSQL project
 * - `migrate` - Run database migrations
 * - `generate` - Generate TypeScript types from schema
 *
 * @module dosql-cli
 */

import { Command } from 'commander';
import { initProject } from './commands/init.js';
import { findMigrations, runMigrations } from './commands/migrate.js';
import { generateTypes } from './commands/generate.js';
import { resolve } from 'node:path';
import { logger } from './utils/logger.js';

/**
 * Creates and configures the DoSQL CLI program.
 *
 * Returns a Commander.js program instance with all commands registered.
 * Can be used to create custom CLI implementations or for testing.
 *
 * @returns Configured Commander.js Command instance
 *
 * @example
 * ```typescript
 * // Create and run the CLI
 * const program = createCLI();
 * program.parse(process.argv);
 *
 * // Or use programmatically
 * const program = createCLI();
 * await program.parseAsync(['node', 'dosql', 'init', '-d', './my-project']);
 * ```
 */
export function createCLI(): Command {
  const program = new Command();

  // Note: Version is hardcoded here and must be kept in sync with package.json.
  // When releasing a new version:
  //   1. Update version in package.json
  //   2. Update version in this .version() call
  //   3. Update CHANGELOG.md
  // TODO: Consider reading version from package.json at runtime to avoid duplication.
  program
    .name('dosql')
    .version('0.1.0')
    .description('DoSQL CLI - scaffold projects, run migrations, generate types');

  // Init command
  program
    .command('init')
    .description('Initialize a new DoSQL project')
    .option('-d, --directory <path>', 'Target directory', process.cwd())
    .option('-n, --name <name>', 'Project name')
    .option('-f, --force', 'Overwrite existing config', false)
    .action(async (options) => {
      try {
        const result = await initProject({
          directory: resolve(options.directory),
          name: options.name,
          force: options.force,
        });

        logger.success('DoSQL project initialized successfully!');
        logger.section('Created files');
        logger.list(result.createdFiles);
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.error(err.message);
        process.exit(1);
      }
    });

  // Migrate command
  program
    .command('migrate')
    .description('Run database migrations')
    .option('-d, --directory <path>', 'Migrations directory', './migrations')
    .option('--dry-run', 'Show pending migrations without applying', false)
    .action(async (options) => {
      try {
        const migrationsDir = resolve(options.directory);
        const migrations = await findMigrations(migrationsDir);

        if (migrations.length === 0) {
          logger.info('No migrations found.');
          return;
        }

        logger.info(`Found ${migrations.length} migration(s)`);

        if (options.dryRun) {
          logger.section('Pending migrations (dry run)');
          logger.list(migrations.map(m => m.name));
          return;
        }

        // TODO: Database connection not yet implemented in CLI
        //
        // The migrate command currently only lists available migrations without
        // actually applying them to a database. To run migrations, use the
        // programmatic API with a custom executor:
        //
        //   import { findMigrations, runMigrations } from 'dosql-cli';
        //
        //   const migrations = await findMigrations('./migrations');
        //   const result = await runMigrations({
        //     migrations,
        //     executor: async (sql, name) => {
        //       await yourDatabase.exec(sql);
        //     },
        //     appliedMigrations: [], // List of already-applied migration names
        //   });
        //
        // Future CLI options will include:
        //   --url <url>     Database connection URL
        //   --token <token> Authentication token
        //
        // See README.md for more details on programmatic usage.
        logger.newline();
        logger.warn('Database connection not yet implemented in CLI.');
        logger.section('Available migrations (not applied)');
        logger.list(migrations.map(m => m.name));
        logger.newline();
        logger.info('To apply migrations, use the programmatic API.');
        logger.info('See: https://github.com/dotdo/dosql-cli#programmatic-api');
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.error(err.message);
        process.exit(1);
      }
    });

  // Generate command
  program
    .command('generate')
    .description('Generate TypeScript types from schema')
    .option('-s, --schema <path>', 'Schema directory', './schema')
    .option('-o, --output <path>', 'Output directory', './generated')
    .action(async (options) => {
      try {
        const result = await generateTypes({
          schemaDir: resolve(options.schema),
          outputDir: resolve(options.output),
        });

        logger.success('Types generated successfully!');
        logger.section('Generated files');
        logger.list(result.generatedFiles);
        logger.section('Tables processed');
        logger.list(result.tablesProcessed);
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.error(err.message);
        process.exit(1);
      }
    });

  return program;
}

// Export commands for programmatic use
export { initProject, type InitOptions, type InitResult } from './commands/init.js';
export {
  findMigrations,
  runMigrations,
  type Migration,
  type MigrateOptions,
  type MigrateResult,
} from './commands/migrate.js';
export { generateTypes, type GenerateOptions, type GenerateResult } from './commands/generate.js';

// Run CLI if invoked directly
const isMainModule = import.meta.url === `file://${process.argv[1]}`;
if (isMainModule) {
  createCLI().parse();
}
