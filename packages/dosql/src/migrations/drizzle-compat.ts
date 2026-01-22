/**
 * DoSQL Migrations - Drizzle Kit Compatibility Layer
 *
 * Parses Drizzle Kit migration files and converts them to DoSQL Migration format.
 * Supports both v2 (journal-based) and v3 (folder-based) Drizzle structures.
 *
 * @module migrations/drizzle-compat
 */

import {
  type Migration,
  type MigrationSnapshot,
  type DrizzleMigrationFolder,
  type DrizzleJournal,
  type DrizzleJournalEntry,
  calculateChecksumSync,
  compareMigrationIds,
} from './types.js';

// =============================================================================
// FILE SYSTEM INTERFACE
// =============================================================================

/**
 * Minimal file system interface for reading Drizzle migrations
 *
 * This allows the module to work with different environments:
 * - Node.js fs
 * - Cloudflare Workers with R2
 * - In-memory for testing
 */
export interface MigrationFileSystem {
  /** Read file contents as string */
  readFile(path: string): Promise<string>;

  /** Check if file exists */
  exists(path: string): Promise<boolean>;

  /** List directory contents */
  readdir(path: string): Promise<string[]>;

  /** Check if path is a directory */
  isDirectory(path: string): Promise<boolean>;
}

// =============================================================================
// DRIZZLE FOLDER PARSER
// =============================================================================

/**
 * Parse a Drizzle migration folder name
 *
 * Extracts timestamp and name from folder names like:
 * - "20240823160430_add_users_table"
 * - "0000_warm_stone_men" (legacy format)
 */
export function parseMigrationFolderName(name: string): {
  timestamp: string;
  migrationName: string;
  isLegacy: boolean;
} | null {
  // New format: YYYYMMDDHHMMSS_name
  const newFormatMatch = name.match(/^(\d{14})_(.+)$/);
  if (newFormatMatch) {
    return {
      timestamp: newFormatMatch[1],
      migrationName: newFormatMatch[2],
      isLegacy: false,
    };
  }

  // Legacy format: 0000_name (sequential index)
  const legacyMatch = name.match(/^(\d{4})_(.+)$/);
  if (legacyMatch) {
    return {
      timestamp: legacyMatch[1].padStart(14, '0'),
      migrationName: legacyMatch[2],
      isLegacy: true,
    };
  }

  return null;
}

/**
 * Parse a Drizzle snapshot.json file
 */
export function parseSnapshotJson(content: string): MigrationSnapshot | null {
  try {
    const json = JSON.parse(content);

    // Drizzle snapshot format
    return {
      version: json.version ?? '5',
      dialect: json.dialect ?? 'sqlite',
      tables: json.tables ?? {},
      enums: json.enums,
      indexes: json.indexes,
      foreignKeys: json.foreignKeys,
      compositePrimaryKeys: json.compositePrimaryKeys,
    };
  } catch {
    return null;
  }
}

/**
 * Parse a Drizzle journal file (v2 format)
 */
export function parseJournalJson(content: string): DrizzleJournal | null {
  try {
    const json = JSON.parse(content);

    if (!json.entries || !Array.isArray(json.entries)) {
      return null;
    }

    return {
      version: json.version ?? '5',
      dialect: json.dialect ?? 'sqlite',
      entries: json.entries.map((entry: DrizzleJournalEntry) => ({
        idx: entry.idx,
        version: entry.version,
        when: entry.when,
        tag: entry.tag,
        breakpoints: entry.breakpoints ?? true,
      })),
    };
  } catch {
    return null;
  }
}

// =============================================================================
// DRIZZLE MIGRATION LOADER
// =============================================================================

/**
 * Options for loading Drizzle migrations
 */
export interface DrizzleLoaderOptions {
  /** Base path to drizzle folder */
  basePath: string;

  /** File system implementation */
  fs: MigrationFileSystem;

  /** Whether to include snapshot in migrations */
  includeSnapshots?: boolean;

  /** Generate down migrations from consecutive snapshots */
  generateDownMigrations?: boolean;
}

/**
 * Load Drizzle Kit migrations from a folder
 *
 * Supports both v2 (with meta/_journal.json) and v3 (folder-per-migration) formats.
 */
export async function loadDrizzleMigrations(
  options: DrizzleLoaderOptions
): Promise<Migration[]> {
  const { basePath, fs, includeSnapshots = true, generateDownMigrations = false } = options;

  // Check for v2 journal
  const journalPath = `${basePath}/meta/_journal.json`;
  const hasJournal = await fs.exists(journalPath);

  if (hasJournal) {
    return loadV2Migrations(options);
  }

  // Fall back to v3 folder-based format
  return loadV3Migrations(options);
}

/**
 * Load v2 format migrations (journal-based)
 */
async function loadV2Migrations(
  options: DrizzleLoaderOptions
): Promise<Migration[]> {
  const { basePath, fs, includeSnapshots } = options;

  // Read journal
  const journalContent = await fs.readFile(`${basePath}/meta/_journal.json`);
  const journal = parseJournalJson(journalContent);

  if (!journal) {
    throw new Error('Failed to parse Drizzle journal file');
  }

  const migrations: Migration[] = [];

  for (const entry of journal.entries) {
    // V2 folder format: {idx}_{tag}
    const folderName = `${String(entry.idx).padStart(4, '0')}_${entry.tag}`;
    const migrationPath = `${basePath}/${folderName}`;

    // Read migration.sql
    const sqlPath = `${migrationPath}/migration.sql`;
    let sql: string;

    try {
      sql = await fs.readFile(sqlPath);
    } catch {
      console.warn(`Migration file not found: ${sqlPath}`);
      continue;
    }

    // Read snapshot if requested
    let snapshot: MigrationSnapshot | undefined;
    if (includeSnapshots) {
      const snapshotPath = `${migrationPath}/snapshot.json`;
      try {
        const snapshotContent = await fs.readFile(snapshotPath);
        snapshot = parseSnapshotJson(snapshotContent) ?? undefined;
      } catch {
        // Snapshot is optional
      }
    }

    // Convert timestamp from journal
    const timestamp = String(entry.when).padStart(14, '0');

    migrations.push({
      id: `${timestamp}_${entry.tag}`,
      sql,
      snapshot,
      name: entry.tag,
      createdAt: new Date(entry.when),
      checksum: calculateChecksumSync(sql),
    });
  }

  return migrations.sort((a, b) => compareMigrationIds(a.id, b.id));
}

/**
 * Load v3 format migrations (folder-per-migration)
 */
async function loadV3Migrations(
  options: DrizzleLoaderOptions
): Promise<Migration[]> {
  const { basePath, fs, includeSnapshots, generateDownMigrations } = options;

  // List all directories in the drizzle folder
  const entries = await fs.readdir(basePath);
  const migrationFolders: DrizzleMigrationFolder[] = [];

  for (const entry of entries) {
    // Skip meta folder and non-directories
    if (entry === 'meta') continue;

    const entryPath = `${basePath}/${entry}`;
    const isDir = await fs.isDirectory(entryPath);

    if (!isDir) continue;

    const parsed = parseMigrationFolderName(entry);
    if (!parsed) continue;

    migrationFolders.push({
      name: entry,
      path: entryPath,
      timestamp: parsed.timestamp,
      migrationName: parsed.migrationName,
    });
  }

  // Sort by timestamp
  migrationFolders.sort((a, b) => a.timestamp.localeCompare(b.timestamp));

  const migrations: Migration[] = [];
  const snapshots: MigrationSnapshot[] = [];

  for (const folder of migrationFolders) {
    // Read migration.sql
    const sqlPath = `${folder.path}/migration.sql`;
    let sql: string;

    try {
      sql = await fs.readFile(sqlPath);
    } catch {
      console.warn(`Migration file not found: ${sqlPath}`);
      continue;
    }

    // Read snapshot if requested
    let snapshot: MigrationSnapshot | undefined;
    if (includeSnapshots || generateDownMigrations) {
      const snapshotPath = `${folder.path}/snapshot.json`;
      try {
        const snapshotContent = await fs.readFile(snapshotPath);
        snapshot = parseSnapshotJson(snapshotContent) ?? undefined;
        if (snapshot) {
          snapshots.push(snapshot);
        }
      } catch {
        // Snapshot is optional
      }
    }

    migrations.push({
      id: `${folder.timestamp}_${folder.migrationName}`,
      sql,
      snapshot: includeSnapshots ? snapshot : undefined,
      name: folder.migrationName,
      createdAt: new Date(
        parseInt(folder.timestamp.slice(0, 4)),
        parseInt(folder.timestamp.slice(4, 6)) - 1,
        parseInt(folder.timestamp.slice(6, 8)),
        parseInt(folder.timestamp.slice(8, 10)),
        parseInt(folder.timestamp.slice(10, 12)),
        parseInt(folder.timestamp.slice(12, 14))
      ),
      checksum: calculateChecksumSync(sql),
    });
  }

  // Generate down migrations if requested
  if (generateDownMigrations && snapshots.length > 1) {
    for (let i = 1; i < migrations.length; i++) {
      const prevSnapshot = snapshots[i - 1];
      const currSnapshot = snapshots[i];

      if (prevSnapshot && currSnapshot) {
        const downSql = generateDownMigration(currSnapshot, prevSnapshot);
        (migrations[i] as { downSql?: string }).downSql = downSql;
      }
    }
  }

  return migrations;
}

// =============================================================================
// DOWN MIGRATION GENERATOR
// =============================================================================

/**
 * Generate down migration SQL from two snapshots
 *
 * Compares the current snapshot to the previous and generates
 * SQL to revert the changes.
 */
export function generateDownMigration(
  current: MigrationSnapshot,
  previous: MigrationSnapshot
): string {
  const statements: string[] = [];

  // Find tables to drop (in current but not in previous)
  for (const tableName of Object.keys(current.tables)) {
    if (!previous.tables[tableName]) {
      statements.push(`DROP TABLE IF EXISTS "${tableName}"`);
    }
  }

  // Find tables to recreate (in previous but not in current)
  for (const tableName of Object.keys(previous.tables)) {
    if (!current.tables[tableName]) {
      const table = previous.tables[tableName];
      statements.push(generateCreateTable(tableName, table));
    }
  }

  // Find column changes
  for (const tableName of Object.keys(current.tables)) {
    const currentTable = current.tables[tableName];
    const previousTable = previous.tables[tableName];

    if (!previousTable) continue;

    // Columns to drop (added in current)
    for (const colName of Object.keys(currentTable.columns)) {
      if (!previousTable.columns[colName]) {
        statements.push(`ALTER TABLE "${tableName}" DROP COLUMN "${colName}"`);
      }
    }

    // Columns to add back (removed in current)
    for (const colName of Object.keys(previousTable.columns)) {
      if (!currentTable.columns[colName]) {
        const col = previousTable.columns[colName];
        statements.push(
          `ALTER TABLE "${tableName}" ADD COLUMN "${colName}" ${col.type}${col.notNull ? ' NOT NULL' : ''}${col.default ? ` DEFAULT ${col.default}` : ''}`
        );
      }
    }
  }

  return statements.join(';\n\n');
}

/**
 * Generate CREATE TABLE statement from snapshot
 */
function generateCreateTable(
  name: string,
  table: MigrationSnapshot['tables'][string]
): string {
  const columns: string[] = [];

  for (const [colName, col] of Object.entries(table.columns)) {
    let colDef = `"${colName}" ${col.type}`;

    if (col.primaryKey) {
      colDef += ' PRIMARY KEY';
    }
    if (col.autoincrement) {
      colDef += ' AUTOINCREMENT';
    }
    if (col.notNull) {
      colDef += ' NOT NULL';
    }
    if (col.default !== undefined) {
      colDef += ` DEFAULT ${col.default}`;
    }

    columns.push(colDef);
  }

  return `CREATE TABLE "${name}" (\n  ${columns.join(',\n  ')}\n)`;
}

// =============================================================================
// IN-MEMORY FILE SYSTEM (FOR TESTING)
// =============================================================================

/**
 * Create an in-memory file system for testing
 */
export function createInMemoryFs(
  files: Record<string, string>
): MigrationFileSystem {
  return {
    async readFile(path: string): Promise<string> {
      if (!(path in files)) {
        throw new Error(`File not found: ${path}`);
      }
      return files[path];
    },

    async exists(path: string): Promise<boolean> {
      // Check for exact file match
      if (path in files) return true;

      // Check if path is a directory (has children)
      const prefix = path.endsWith('/') ? path : `${path}/`;
      return Object.keys(files).some(f => f.startsWith(prefix));
    },

    async readdir(path: string): Promise<string[]> {
      const prefix = path.endsWith('/') ? path : `${path}/`;
      const entries = new Set<string>();

      for (const filePath of Object.keys(files)) {
        if (filePath.startsWith(prefix)) {
          const rest = filePath.slice(prefix.length);
          const firstPart = rest.split('/')[0];
          if (firstPart) {
            entries.add(firstPart);
          }
        }
      }

      return Array.from(entries);
    },

    async isDirectory(path: string): Promise<boolean> {
      // A path is a directory if it has children
      const prefix = path.endsWith('/') ? path : `${path}/`;
      return Object.keys(files).some(f => f.startsWith(prefix));
    },
  };
}

// =============================================================================
// DRIZZLE CONFIG PARSER
// =============================================================================

/**
 * Parsed Drizzle config
 */
export interface DrizzleConfig {
  dialect: 'postgresql' | 'sqlite' | 'mysql';
  schema: string | string[];
  out: string;
  migrations?: {
    table?: string;
    schema?: string;
  };
}

/**
 * Parse drizzle.config.ts content
 *
 * Note: This is a simple regex-based parser. For production,
 * use the actual TypeScript compiler or a proper AST parser.
 */
export function parseDrizzleConfig(content: string): Partial<DrizzleConfig> {
  const config: Partial<DrizzleConfig> = {};

  // Extract dialect
  const dialectMatch = content.match(/dialect:\s*["'](\w+)["']/);
  if (dialectMatch) {
    config.dialect = dialectMatch[1] as DrizzleConfig['dialect'];
  }

  // Extract out (output directory)
  const outMatch = content.match(/out:\s*["']([^"']+)["']/);
  if (outMatch) {
    config.out = outMatch[1];
  }

  // Extract schema
  const schemaMatch = content.match(/schema:\s*["']([^"']+)["']/);
  if (schemaMatch) {
    config.schema = schemaMatch[1];
  }

  // Extract migrations table
  const migrationsTableMatch = content.match(/table:\s*["']([^"']+)["']/);
  if (migrationsTableMatch) {
    config.migrations = config.migrations ?? {};
    config.migrations.table = migrationsTableMatch[1];
  }

  return config;
}

// =============================================================================
// CONVERSION UTILITIES
// =============================================================================

/**
 * Convert Drizzle migration ID format to DoSQL format
 */
export function drizzleIdToDoSqlId(drizzleId: string): string {
  // Drizzle v3: 20240823160430_name -> 20240823160430_name
  // Drizzle v2: 0000_name -> 00000000000000_name
  const parsed = parseMigrationFolderName(drizzleId);

  if (!parsed) {
    return drizzleId;
  }

  return `${parsed.timestamp}_${parsed.migrationName}`;
}

/**
 * Convert DoSQL migration to Drizzle-compatible format
 */
export function toDoSqlMigration(
  drizzleMigration: {
    tag: string;
    when: number;
    sql: string;
    snapshot?: unknown;
  }
): Migration {
  const timestamp = String(drizzleMigration.when).padStart(14, '0');

  return {
    id: `${timestamp}_${drizzleMigration.tag}`,
    sql: drizzleMigration.sql,
    name: drizzleMigration.tag,
    createdAt: new Date(drizzleMigration.when),
    checksum: calculateChecksumSync(drizzleMigration.sql),
    snapshot: drizzleMigration.snapshot as MigrationSnapshot | undefined,
  };
}
