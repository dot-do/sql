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

        console.log('DoSQL project initialized successfully!');
        console.log('Created files:');
        result.createdFiles.forEach(file => console.log(`  - ${file}`));
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        console.error(`Error: ${err.message}`);
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
          console.log('No migrations found.');
          return;
        }

        console.log(`Found ${migrations.length} migration(s)`);

        if (options.dryRun) {
          console.log('Pending migrations (dry run):');
          migrations.forEach(m => console.log(`  - ${m.name}`));
          return;
        }

        // TODO: Database connection not yet implemented in CLI
        //
        // The migrate command currently only lists available migrations without
        // actually applying them to a database. To run migrations, use the
        // programmatic API with a custom executor:
        //
        //   import { findMigrations, runMigrations } from '@dotdo/dosql-cli';
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
        console.log('\nNote: Database connection not yet implemented in CLI.');
        console.log('Available migrations (not applied):');
        migrations.forEach(m => console.log(`  - ${m.name}`));
        console.log('\nTo apply migrations, use the programmatic API.');
        console.log('See: https://github.com/dotdo/dosql-cli#programmatic-api');
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        console.error(`Error: ${err.message}`);
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

        console.log('Types generated successfully!');
        console.log('Generated files:');
        result.generatedFiles.forEach(file => console.log(`  - ${file}`));
        console.log('Tables processed:');
        result.tablesProcessed.forEach(table => console.log(`  - ${table}`));
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        console.error(`Error: ${err.message}`);
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
