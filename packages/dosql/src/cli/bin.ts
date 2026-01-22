#!/usr/bin/env node
/**
 * DoSQL CLI Binary Entry Point
 *
 * This file is the entry point for the `dosql` command.
 * It sets up the Node.js file system and runs the CLI.
 */

import * as fs from 'node:fs';
import * as readline from 'node:readline';
import { main, setFileSystem, loadConfig, query as runQuery } from './index.js';
import { Database } from '../database.js';

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

  console.log('DoSQL Shell');
  console.log(`Connected to: ${config.database.path ?? ':memory:'}`);
  console.log('Type ".exit" to quit, ".tables" to list tables, ".help" for help\n');

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
      console.log(`
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
          console.log('No tables found');
        } else {
          console.log('Tables:');
          for (const table of tables) {
            console.log(`  ${table}`);
          }
        }
      } catch (err) {
        console.error('Error listing tables:', err instanceof Error ? err.message : err);
      }
      rl.prompt();
      return;
    }

    if (input === '.schema') {
      try {
        const tables = db.getTables();
        for (const table of tables) {
          const info = db.pragma('table_info', table);
          console.log(`\nTable: ${table}`);
          if (Array.isArray(info)) {
            for (const col of info) {
              console.log(`  ${col.name} ${col.type}${col.notnull ? ' NOT NULL' : ''}${col.pk ? ' PRIMARY KEY' : ''}`);
            }
          }
        }
      } catch (err) {
        console.error('Error getting schema:', err instanceof Error ? err.message : err);
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
          console.log('No results');
        } else {
          console.table(results);
        }
      } else {
        const stmt = db.prepare(input);
        const result = stmt.run();
        console.log(`OK, ${result.changes} row(s) affected`);
      }
    } catch (err) {
      console.error('Error:', err instanceof Error ? err.message : err);
    }

    rl.prompt();
  });

  rl.on('close', () => {
    console.log('\nGoodbye!');
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
    console.error('Error:', err instanceof Error ? err.message : err);
    process.exit(1);
  });
} else {
  // Run main CLI
  main(args).catch((err) => {
    console.error('Error:', err instanceof Error ? err.message : err);
    process.exit(1);
  });
}
