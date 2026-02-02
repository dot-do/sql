#!/usr/bin/env node
/**
 * DoSQL CLI Binary Entry Point
 *
 * This file is the entry point for the `dosql` command.
 * It sets up the Node.js file system and runs the CLI.
 */

import * as fs from 'node:fs';
import * as readline from 'node:readline';
import { main, setFileSystem, loadConfig, query as runQuery, createCLILogger, type CLILogger } from './index.js';
import { Database } from '../database.js';

// Create CLI logger for the binary
const logger: CLILogger = createCLILogger();

// Set up Node.js file system
setFileSystem({
  existsSync: fs.existsSync,
  readFileSync: fs.readFileSync,
  writeFileSync: fs.writeFileSync,
  mkdirSync: fs.mkdirSync,
  readdirSync: fs.readdirSync,
});

/**
 * Interactive shell implementation for Node.js
 */
async function interactiveShell(configPath?: string): Promise<void> {
  const config = await loadConfig(configPath);

  const db = new Database(config.database.path ?? ':memory:');

  logger.output('DoSQL Shell');
  logger.output(`Connected to: ${config.database.path ?? ':memory:'}`);
  logger.output('Type ".exit" to quit, ".tables" to list tables, ".help" for help\n');

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: 'dosql> ',
  });

  rl.prompt();

  rl.on('line', (line) => {
    const input = line.trim();

    if (!input) {
      rl.prompt();
      return;
    }

    // Handle special commands
    if (input === '.exit' || input === '.quit') {
      db.close();
      rl.close();
      return;
    }

    if (input === '.help') {
      logger.output(`
Special commands:
  .exit      Exit the shell
  .quit      Exit the shell
  .tables    List all tables
  .schema    Show database schema
  .help      Show this help

SQL commands:
  Any valid SQL statement (SELECT, INSERT, CREATE TABLE, etc.)
`);
      rl.prompt();
      return;
    }

    if (input === '.tables') {
      try {
        const tables = db.getTables();
        if (tables.length === 0) {
          logger.info('No tables found', { operation: 'shell', command: '.tables' });
        } else {
          logger.output('Tables:');
          for (const table of tables) {
            logger.output(`  ${table}`);
          }
        }
      } catch (err) {
        logger.error('Error listing tables', err instanceof Error ? err : undefined, { operation: 'shell', command: '.tables' });
      }
      rl.prompt();
      return;
    }

    if (input === '.schema') {
      try {
        const tables = db.getTables();
        for (const table of tables) {
          const info = db.pragma('table_info', table);
          logger.output(`\nTable: ${table}`);
          if (Array.isArray(info)) {
            for (const col of info) {
              logger.output(`  ${col.name} ${col.type}${col.notnull ? ' NOT NULL' : ''}${col.pk ? ' PRIMARY KEY' : ''}`);
            }
          }
        }
      } catch (err) {
        logger.error('Error getting schema', err instanceof Error ? err : undefined, { operation: 'shell', command: '.schema' });
      }
      rl.prompt();
      return;
    }

    // Execute SQL
    try {
      if (input.toLowerCase().startsWith('select')) {
        const stmt = db.prepare(input);
        const results = stmt.all();
        if (results.length === 0) {
          logger.info('No results', { operation: 'shell', sql: input });
        } else {
          logger.outputData(results);
        }
      } else {
        const stmt = db.prepare(input);
        const result = stmt.run();
        logger.success(`OK, ${result.changes} row(s) affected`, { operation: 'shell', changes: result.changes });
      }
    } catch (err) {
      logger.error(err instanceof Error ? err.message : String(err), err instanceof Error ? err : undefined, { operation: 'shell', sql: input });
    }

    rl.prompt();
  });

  rl.on('close', () => {
    logger.output('\nGoodbye!');
    process.exit(0);
  });
}

// Check if shell command is being used - handle it specially
const args = process.argv.slice(2);
const isShellCommand = args[0] === 'shell';

if (isShellCommand) {
  // Find config path if specified
  let configPath: string | undefined;
  const configIndex = args.indexOf('--config');
  if (configIndex !== -1 && args[configIndex + 1]) {
    configPath = args[configIndex + 1];
  }

  interactiveShell(configPath).catch((err) => {
    logger.error(err instanceof Error ? err.message : String(err), err instanceof Error ? err : undefined, { operation: 'shell' });
    process.exit(1);
  });
} else {
  // Run main CLI
  main(args).catch((err) => {
    logger.error(err instanceof Error ? err.message : String(err), err instanceof Error ? err : undefined, { operation: 'cli' });
    process.exit(1);
  });
}
