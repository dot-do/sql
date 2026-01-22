#!/usr/bin/env node

import { Command } from 'commander';
import { initProject } from './commands/init.js';
import { findMigrations, runMigrations } from './commands/migrate.js';
import { generateTypes } from './commands/generate.js';
import { resolve } from 'node:path';

export function createCLI(): Command {
  const program = new Command();

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

        // In real usage, this would connect to a database
        // For now, we just demonstrate the structure
        console.log('Note: Connect to a database to run migrations');
        console.log('Available migrations:');
        migrations.forEach(m => console.log(`  - ${m.name}`));
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
