/**
 * DoSQL CLI - Command Line Interface
 *
 * Provides commands for managing DoSQL databases:
 * - dosql init        : Initialize config file and project setup
 * - dosql migrate     : Run pending migrations
 * - dosql shell       : Open interactive SQL prompt
 * - dosql query       : Execute SQL and output results
 *
 * @module cli
 */

import { Database } from '../database.js';
import {
  MigrationRunner,
  createMigration,
  type DatabaseExecutor,
} from '../migrations/runner.js';
import {
  type Migration,
  type MigrationResult,
  calculateChecksumSync,
} from '../migrations/types.js';

// =============================================================================
// TYPES
// =============================================================================

/**
 * CLI Configuration
 */
export interface CLIConfig {
  database: {
    type: 'sqlite' | 'libsql' | 'do';
    path?: string;
    url?: string;
  };
  migrations?: {
    directory: string;
  };
}

/**
 * Parsed CLI arguments
 */
export interface ParsedArgs {
  command: string;
  options: Record<string, unknown>;
}

/**
 * Migration run result
 */
export interface MigrateResult {
  applied: string[];
  pending: string[];
}

// =============================================================================
// CONSTANTS
// =============================================================================

const DEFAULT_CONFIG_FILE = 'dosql.config.json';
const DEFAULT_MIGRATIONS_DIR = 'migrations';

const VERSION = '0.1.0';

const HELP_TEXT = `
DoSQL CLI v${VERSION}

Usage: dosql <command> [options]

Commands:
  init          Initialize a new DoSQL project
  migrate       Run pending migrations
  shell         Open interactive SQL prompt
  query <sql>   Execute a SQL query

Options:
  --config <path>   Path to config file (default: dosql.config.json)
  --dry-run         Show SQL without executing (migrate command)
  --format <fmt>    Output format: json, table, csv (query command)
  --help, -h        Show this help message
  --version, -v     Show version number

Examples:
  dosql init
  dosql migrate
  dosql migrate --dry-run
  dosql query "SELECT * FROM users"
  dosql query "SELECT * FROM users" --format csv
  dosql shell
`;

// =============================================================================
// FILE SYSTEM ABSTRACTION
// =============================================================================

/**
 * File system interface for Node.js compatibility
 */
interface FileSystem {
  existsSync(path: string): boolean;
  readFileSync(path: string, encoding: 'utf-8'): string;
  writeFileSync(path: string, content: string): void;
  mkdirSync(path: string, options?: { recursive?: boolean }): void;
  readdirSync(path: string): string[];
}

// Default to using globalThis for potential fs injection, or throw on use
let fs: FileSystem | null = null;

/**
 * Set the file system implementation (for Node.js)
 */
export function setFileSystem(fileSystem: FileSystem): void {
  fs = fileSystem;
}

/**
 * Get the file system implementation
 */
function getFs(): FileSystem {
  if (!fs) {
    // Try to detect Node.js and import fs
    throw new Error(
      'File system not available. Use setFileSystem() to provide an fs implementation.'
    );
  }
  return fs;
}

// =============================================================================
// CONFIG LOADING
// =============================================================================

/**
 * Load configuration from a file
 *
 * @param configPath - Path to config file (optional, defaults to dosql.config.json)
 * @returns Parsed configuration
 * @throws Error if config file not found or invalid
 */
export async function loadConfig(configPath?: string): Promise<CLIConfig> {
  const fsImpl = getFs();
  const path = configPath ?? DEFAULT_CONFIG_FILE;

  if (!fsImpl.existsSync(path)) {
    throw new Error(`Config file not found: ${path}`);
  }

  try {
    const content = fsImpl.readFileSync(path, 'utf-8');
    const config = JSON.parse(content) as CLIConfig;

    // Validate required fields
    if (!config.database) {
      throw new Error('Config missing required "database" field');
    }
    if (!config.database.type) {
      throw new Error('Config missing required "database.type" field');
    }

    return config;
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw new Error(`Invalid JSON in config file: ${path}`);
    }
    throw error;
  }
}

// =============================================================================
// ARGUMENT PARSING
// =============================================================================

/**
 * Parse CLI arguments
 *
 * @param args - Array of command line arguments
 * @returns Parsed command and options
 */
export function parseArgs(args: string[]): ParsedArgs {
  const result: ParsedArgs = {
    command: '',
    options: {},
  };

  let i = 0;

  // Handle global flags first
  while (i < args.length && args[i].startsWith('-')) {
    const arg = args[i];

    if (arg === '--help' || arg === '-h') {
      result.options.help = true;
      i++;
    } else if (arg === '--version' || arg === '-v') {
      result.options.version = true;
      i++;
    } else if (arg === '--config') {
      if (i + 1 >= args.length) {
        throw new Error('--config requires a path argument');
      }
      result.options.configPath = args[i + 1];
      i += 2;
    } else {
      // Unknown flag before command - skip for now
      break;
    }
  }

  // Get command
  if (i < args.length && !args[i].startsWith('-')) {
    result.command = args[i];
    i++;
  }

  // Parse command-specific arguments
  while (i < args.length) {
    const arg = args[i];

    if (arg === '--dry-run') {
      result.options.dryRun = true;
      i++;
    } else if (arg === '--format') {
      if (i + 1 >= args.length) {
        throw new Error('--format requires a format argument (json, table, csv)');
      }
      result.options.format = args[i + 1];
      i += 2;
    } else if (arg === '--config') {
      if (i + 1 >= args.length) {
        throw new Error('--config requires a path argument');
      }
      result.options.configPath = args[i + 1];
      i += 2;
    } else if (arg === '--help' || arg === '-h') {
      result.options.help = true;
      i++;
    } else if (arg === '--version' || arg === '-v') {
      result.options.version = true;
      i++;
    } else if (!arg.startsWith('-')) {
      // Positional argument - treat as SQL for query command
      if (result.command === 'query' && !result.options.sql) {
        result.options.sql = arg;
      }
      i++;
    } else {
      // Unknown flag
      i++;
    }
  }

  return result;
}

// =============================================================================
// DATABASE CONNECTION
// =============================================================================

/**
 * Create a database connection from config
 */
async function createDatabaseConnection(config: CLIConfig): Promise<Database> {
  const { database } = config;

  switch (database.type) {
    case 'sqlite':
      return new Database(database.path ?? ':memory:');

    case 'libsql':
      // For libsql, we'd need to integrate with @libsql/client
      // For now, fall back to in-memory
      if (database.url) {
        throw new Error(
          'libsql remote connections not yet implemented in CLI. Use sqlite type with a local path.'
        );
      }
      return new Database(database.path ?? ':memory:');

    case 'do':
      throw new Error(
        'Durable Object connections not available in CLI. Use sqlite or libsql type.'
      );

    default:
      throw new Error(`Unknown database type: ${database.type}`);
  }
}

/**
 * Create a database executor adapter for migrations
 */
function createExecutorAdapter(db: Database): DatabaseExecutor {
  return {
    exec(sql: string) {
      db.exec(sql);
    },
    query<T = unknown>(sql: string, params?: unknown[]): T[] {
      const stmt = db.prepare<T>(sql);
      if (params && params.length > 0) {
        return stmt.all(...params) as T[];
      }
      return stmt.all() as T[];
    },
    run(sql: string, params?: unknown[]) {
      const stmt = db.prepare(sql);
      if (params && params.length > 0) {
        const result = stmt.run(...params);
        return { changes: result.changes };
      }
      const result = stmt.run();
      return { changes: result.changes };
    },
  };
}

// =============================================================================
// MIGRATION LOADING
// =============================================================================

/**
 * Load migrations from the configured directory
 */
async function loadMigrations(config: CLIConfig): Promise<Migration[]> {
  const fsImpl = getFs();
  const migrationsDir = config.migrations?.directory ?? DEFAULT_MIGRATIONS_DIR;

  if (!fsImpl.existsSync(migrationsDir)) {
    return [];
  }

  const files = fsImpl.readdirSync(migrationsDir);
  const sqlFiles = files
    .filter(f => f.endsWith('.sql'))
    .sort();

  const migrations: Migration[] = [];

  for (const file of sqlFiles) {
    const filePath = `${migrationsDir}/${file}`;
    const sql = fsImpl.readFileSync(filePath, 'utf-8');
    const id = file.replace(/\.sql$/, '');

    migrations.push({
      id,
      sql,
      name: id,
      createdAt: new Date(),
      checksum: calculateChecksumSync(sql),
    });
  }

  return migrations;
}

// =============================================================================
// COMMANDS
// =============================================================================

/**
 * Initialize a new DoSQL project
 *
 * Creates dosql.config.json and migrations directory.
 *
 * @param options - Command options
 */
export async function init(options?: { configPath?: string }): Promise<void> {
  const fsImpl = getFs();
  const configPath = options?.configPath ?? DEFAULT_CONFIG_FILE;

  // Check if config already exists
  if (fsImpl.existsSync(configPath)) {
    throw new Error(`Config file already exists: ${configPath}`);
  }

  // Create default config
  const defaultConfig: CLIConfig = {
    database: {
      type: 'sqlite',
      path: 'dosql.db',
    },
    migrations: {
      directory: DEFAULT_MIGRATIONS_DIR,
    },
  };

  // Write config file
  fsImpl.writeFileSync(configPath, JSON.stringify(defaultConfig, null, 2));

  // Create migrations directory
  const migrationsDir = defaultConfig.migrations?.directory ?? DEFAULT_MIGRATIONS_DIR;
  if (!fsImpl.existsSync(migrationsDir)) {
    fsImpl.mkdirSync(migrationsDir, { recursive: true });
  }

  console.log(`Created ${configPath}`);
  console.log(`Created ${migrationsDir}/`);
}

/**
 * Run pending migrations
 *
 * @param options - Command options
 * @returns Migration result with applied and pending migrations
 */
export async function migrate(options?: {
  dryRun?: boolean;
  configPath?: string;
}): Promise<MigrateResult> {
  const config = await loadConfig(options?.configPath);
  const migrations = await loadMigrations(config);

  if (migrations.length === 0 && !options?.dryRun) {
    console.log('No migrations found');
    return { applied: [], pending: [] };
  }

  const db = await createDatabaseConnection(config);
  const executor = createExecutorAdapter(db);

  try {
    const runner = new MigrationRunner(executor, {
      dryRun: options?.dryRun ?? false,
      logger: {
        info: (msg) => console.log(msg),
        warn: (msg) => console.warn(msg),
        error: (msg) => console.error(msg),
        debug: () => {},
      },
    });

    if (options?.dryRun) {
      // In dry-run mode, just show what would be applied
      const status = await runner.getStatus(migrations);
      console.log('\nDry run - migrations that would be applied:');
      for (const m of status.pending) {
        console.log(`\n--- ${m.id} ---`);
        console.log(m.sql);
      }
      return {
        applied: [],
        pending: status.pending.map(m => m.id),
      };
    }

    const result = await runner.migrate(migrations);

    if (!result.success) {
      const errors = result.failed.map(f => f.error.message).join(', ');
      throw new Error(`Migration failed: ${errors}`);
    }

    return {
      applied: result.applied.map(m => m.id),
      pending: [],
    };
  } finally {
    db.close();
  }
}

/**
 * Open interactive SQL shell
 *
 * Note: This is a basic implementation. In a real CLI, this would
 * use readline for a proper interactive experience.
 *
 * @param options - Command options
 */
export async function shell(options?: { configPath?: string }): Promise<void> {
  const config = await loadConfig(options?.configPath);
  const db = await createDatabaseConnection(config);

  console.log('DoSQL Shell');
  console.log(`Connected to: ${config.database.path ?? ':memory:'}`);
  console.log('Type ".exit" to quit, ".tables" to list tables\n');

  // In Node.js environment, we'd use readline here
  // For now, this is a placeholder that would be implemented
  // with the actual readline interface in a real CLI

  // The shell function exists and can be called - the actual
  // interactive loop would be implemented in the bin entry point
  // using Node's readline module

  db.close();
}

/**
 * Execute a SQL query
 *
 * @param sql - SQL query to execute
 * @param options - Command options
 * @returns Query results
 */
export async function query(
  sql: string,
  options?: {
    configPath?: string;
    format?: 'json' | 'table' | 'csv';
  }
): Promise<unknown[]> {
  const config = await loadConfig(options?.configPath);
  const db = await createDatabaseConnection(config);

  try {
    const stmt = db.prepare(sql);
    const results = stmt.all();

    // Format output based on option
    const format = options?.format ?? 'json';

    switch (format) {
      case 'json':
        // JSON is the default - just return results
        break;

      case 'table':
        // Table format - print to console
        if (results.length > 0) {
          console.table(results);
        }
        break;

      case 'csv':
        // CSV format - print to console
        if (results.length > 0) {
          const headers = Object.keys(results[0] as object);
          console.log(headers.join(','));
          for (const row of results) {
            const values = headers.map(h => {
              const val = (row as Record<string, unknown>)[h];
              if (typeof val === 'string' && (val.includes(',') || val.includes('"'))) {
                return `"${val.replace(/"/g, '""')}"`;
              }
              return String(val ?? '');
            });
            console.log(values.join(','));
          }
        }
        break;
    }

    return results;
  } catch (error) {
    // Re-throw with more descriptive error messages
    if (error instanceof Error) {
      const msg = error.message.toLowerCase();
      if (msg.includes('syntax') || msg.includes('parse')) {
        throw new Error(`SQL syntax error: ${error.message}`);
      }
      if (msg.includes('no such table') || msg.includes('table') && msg.includes('not')) {
        throw new Error(`Table not found: ${error.message}`);
      }
      if (msg.includes('permission') || msg.includes('access')) {
        throw new Error(`Permission denied: ${error.message}`);
      }
      if (msg.includes('connection') || msg.includes('network')) {
        throw new Error(`Connection failed: ${error.message}`);
      }
    }
    throw error;
  } finally {
    db.close();
  }
}

// =============================================================================
// MAIN ENTRY POINT
// =============================================================================

/**
 * Main CLI entry point
 *
 * @param args - Command line arguments (without node and script path)
 */
export async function main(args: string[]): Promise<void> {
  try {
    const parsed = parseArgs(args);

    // Handle global flags
    if (parsed.options.help) {
      console.log(HELP_TEXT);
      return;
    }

    if (parsed.options.version) {
      console.log(`dosql v${VERSION}`);
      return;
    }

    // Handle commands
    switch (parsed.command) {
      case 'init':
        await init({ configPath: parsed.options.configPath as string | undefined });
        break;

      case 'migrate':
        await migrate({
          dryRun: parsed.options.dryRun as boolean | undefined,
          configPath: parsed.options.configPath as string | undefined,
        });
        break;

      case 'shell':
        await shell({ configPath: parsed.options.configPath as string | undefined });
        break;

      case 'query':
        if (!parsed.options.sql) {
          throw new Error('Query command requires a SQL argument');
        }
        const results = await query(parsed.options.sql as string, {
          configPath: parsed.options.configPath as string | undefined,
          format: parsed.options.format as 'json' | 'table' | 'csv' | undefined,
        });
        // Print JSON results to stdout
        if (parsed.options.format !== 'table' && parsed.options.format !== 'csv') {
          console.log(JSON.stringify(results, null, 2));
        }
        break;

      case '':
        console.log(HELP_TEXT);
        break;

      default:
        throw new Error(`Unknown command: ${parsed.command}`);
    }
  } catch (error) {
    if (error instanceof Error) {
      console.error(`Error: ${error.message}`);
    } else {
      console.error('An unknown error occurred');
    }
    process.exitCode = 1;
  }
}

// =============================================================================
// EXPORTS
// =============================================================================

export {
  VERSION,
  HELP_TEXT,
  DEFAULT_CONFIG_FILE,
  DEFAULT_MIGRATIONS_DIR,
};
