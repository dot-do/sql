/**
 * SQLite File Backend for DoSQL
 *
 * Provides file-backed database functionality using better-sqlite3,
 * enabling export/import of SQLite files that are compatible with
 * standard SQLite tools (sqlite3 CLI, better-sqlite3, @libsql/client).
 *
 * @packageDocumentation
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import BetterSqlite3, { type Database as BetterSqlite3Database } from 'better-sqlite3';
import { Database } from '../database.js';
import type { SqlValue, BindParameters, Statement, RunResult } from '../statement/types.js';

// =============================================================================
// TYPES
// =============================================================================

/**
 * Journal modes supported by SQLite
 */
export type JournalMode = 'delete' | 'truncate' | 'persist' | 'memory' | 'wal' | 'off';

/**
 * Synchronous modes supported by SQLite
 */
export type SynchronousMode = 'off' | 'normal' | 'full' | 'extra';

/**
 * Checkpoint modes for WAL
 */
export type CheckpointMode = 'passive' | 'full' | 'restart' | 'truncate';

/**
 * File-backed database options
 */
export interface FileBackedDatabaseOptions {
  /** Path to the SQLite database file */
  filename: string;
  /** Open in read-only mode */
  readonly?: boolean;
  /** Create file if it doesn't exist (default: true) */
  create?: boolean;
  /** File mode for created file (default: 0o644) */
  mode?: number;
  /** Memory-mapped I/O size limit (0 = disabled) */
  mmapSize?: number;
  /** Journal mode */
  journalMode?: JournalMode;
  /** Synchronous mode */
  synchronous?: SynchronousMode;
  /** Page size in bytes (must be power of 2, 512-65536) */
  pageSize?: number;
  /** Lock timeout in milliseconds */
  busyTimeout?: number;
  /** Enable shared cache mode */
  sharedCache?: boolean;
}

/**
 * File export options
 */
export interface ExportOptions {
  /** Output file path */
  path: string;
  /** Overwrite existing file */
  overwrite?: boolean;
  /** Include schema only (no data) */
  schemaOnly?: boolean;
  /** Tables to include (default: all) */
  tables?: string[];
  /** Page size for exported file */
  pageSize?: number;
  /** Journal mode for exported file */
  journalMode?: JournalMode;
}

/**
 * File import options
 */
export interface ImportOptions {
  /** Source file path */
  path: string;
  /** Tables to import (default: all) */
  tables?: string[];
  /** Skip tables that already exist */
  skipExisting?: boolean;
  /** Drop and recreate existing tables */
  replace?: boolean;
}

/**
 * Database file statistics
 */
export interface DatabaseFileStats {
  /** File size in bytes */
  size: number;
  /** Page size in bytes */
  pageSize: number;
  /** Total page count */
  pageCount: number;
  /** Free page count */
  freePageCount: number;
  /** Schema version */
  schemaVersion: number;
  /** SQLite format version */
  formatVersion: number;
  /** Text encoding */
  encoding: 'UTF-8' | 'UTF-16le' | 'UTF-16be';
  /** Journal mode */
  journalMode: string;
  /** Whether WAL mode is active */
  isWalMode: boolean;
  /** WAL checkpoint size (if WAL mode) */
  walCheckpointSize?: number;
}

// =============================================================================
// FILE-BACKED DATABASE
// =============================================================================

/**
 * FileBackedDatabase extends the DoSQL Database with file persistence capabilities.
 *
 * This class uses better-sqlite3 under the hood to provide:
 * - Exporting in-memory databases to SQLite files
 * - Importing from existing SQLite files
 * - Opening databases directly from files
 * - WAL mode support with checkpoint control
 * - Serialization for database transfer
 *
 * @example
 * ```typescript
 * // Export an in-memory database
 * const db = new Database();
 * db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
 * db.exec("INSERT INTO users (name) VALUES ('Alice')");
 *
 * const fileDb = FileBackedDatabase.fromInMemory(db);
 * await fileDb.exportTo({ path: './data.sqlite' });
 *
 * // Open a file-backed database
 * const db2 = new FileBackedDatabase({ filename: './data.sqlite' });
 * const users = db2.prepare('SELECT * FROM users').all();
 * ```
 */
export class FileBackedDatabase extends Database {
  /** The underlying better-sqlite3 database (if file-backed) */
  private nativeDb: BetterSqlite3Database | null = null;

  /** Path to the database file (null if in-memory) */
  private _filename: string | null = null;

  /** Whether this is a file-backed database */
  private _isFileBacked = false;

  /**
   * Create a file-backed database
   *
   * @param options - File-backed database options
   */
  constructor(options?: FileBackedDatabaseOptions) {
    super(options?.filename ?? ':memory:', { readonly: options?.readonly });

    if (options?.filename && options.filename !== ':memory:') {
      this._filename = options.filename;
      this._isFileBacked = true;

      // Handle file existence check
      const fileExists = fs.existsSync(options.filename);
      if (!fileExists && options.create === false) {
        throw new Error(`SQLITE_CANTOPEN: unable to open database file - ${options.filename} not found`);
      }

      // Validate file is a valid SQLite database if it exists
      if (fileExists) {
        const buffer = Buffer.alloc(16);
        const fd = fs.openSync(options.filename, 'r');
        fs.readSync(fd, buffer, 0, 16, 0);
        fs.closeSync(fd);
        const header = buffer.toString('utf8', 0, 15);
        if (header !== 'SQLite format 3') {
          throw new Error('SQLITE_NOTADB: file is not a database');
        }
      }

      // Open with better-sqlite3
      const nativeOptions: BetterSqlite3.Options = {
        fileMustExist: options.create === false,
      };
      // Only set readonly if explicitly true (better-sqlite3 requires boolean)
      if (options.readonly === true) {
        nativeOptions.readonly = true;
      }

      this.nativeDb = new BetterSqlite3(options.filename, nativeOptions);

      // Apply options
      if (options.busyTimeout !== undefined) {
        this.nativeDb.pragma(`busy_timeout = ${options.busyTimeout}`);
      }
      if (options.journalMode) {
        this.nativeDb.pragma(`journal_mode = ${options.journalMode.toUpperCase()}`);
      }
      if (options.synchronous) {
        const syncMap: Record<SynchronousMode, number> = {
          off: 0,
          normal: 1,
          full: 2,
          extra: 3,
        };
        this.nativeDb.pragma(`synchronous = ${syncMap[options.synchronous]}`);
      }
      if (options.pageSize) {
        // Page size can only be set on empty databases
        this.nativeDb.pragma(`page_size = ${options.pageSize}`);
      }

      // Load schema and data from file into in-memory storage for query compatibility
      // Temporarily override readonly flag during loading
      const originalReadonly = this.readonly;
      Object.defineProperty(this, 'readonly', {
        value: false,
        writable: true,
        configurable: true,
      });
      try {
        this.loadFromNative();
      } finally {
        Object.defineProperty(this, 'readonly', {
          value: originalReadonly,
          writable: false,
          configurable: true,
        });
      }
    }
  }

  /**
   * Load schema and data from native database into in-memory storage
   * Note: This bypasses readonly check since we're just loading existing data
   */
  private loadFromNative(): void {
    if (!this.nativeDb) return;

    // Get all tables
    const tables = this.nativeDb.prepare(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).all() as { name: string }[];

    for (const { name: tableName } of tables) {
      // Get table schema
      const tableInfo = this.nativeDb.pragma(`table_info(${tableName})`);
      if (!tableInfo || !Array.isArray(tableInfo) || tableInfo.length === 0) continue;

      // Build CREATE TABLE statement
      const columns = tableInfo.map((col: any) => {
        let def = `${col.name} ${col.type || 'TEXT'}`;
        if (col.pk) def += ' PRIMARY KEY';
        if (col.notnull) def += ' NOT NULL';
        if (col.dflt_value !== null) def += ` DEFAULT ${col.dflt_value}`;
        return def;
      });
      const createSql = `CREATE TABLE IF NOT EXISTS ${tableName} (${columns.join(', ')})`;
      super.exec(createSql);

      // Copy data
      const rows = this.nativeDb.prepare(`SELECT * FROM "${tableName}"`).all();
      if (rows.length > 0) {
        const columnNames = tableInfo.map((col: any) => col.name);
        const placeholders = columnNames.map(() => '?').join(', ');
        const insertSql = `INSERT INTO ${tableName} (${columnNames.join(', ')}) VALUES (${placeholders})`;
        const insertStmt = super.prepare(insertSql);
        for (const row of rows) {
          const values = columnNames.map((col: string) => {
            const val = (row as any)[col];
            // Convert Buffer to Uint8Array for DoSQL
            if (Buffer.isBuffer(val)) {
              return new Uint8Array(val);
            }
            return val;
          });
          insertStmt.run(...values);
        }
      }
    }
  }


  /**
   * Get database file path (null if in-memory)
   */
  get filename(): string | null {
    return this._filename;
  }

  /**
   * Whether this is a file-backed database
   */
  get isFileBacked(): boolean {
    return this._isFileBacked;
  }

  /**
   * Create a FileBackedDatabase wrapper from an existing in-memory Database
   */
  static fromInMemory(db: Database): FileBackedDatabase {
    const fileDb = new FileBackedDatabase();
    // Copy data from the in-memory database
    fileDb.copyFrom(db);
    return fileDb;
  }

  /**
   * Copy schema and data from another Database instance
   */
  private copyFrom(source: Database): void {
    // Get all tables from source
    const tables = source.getTables();

    for (const tableName of tables) {
      // Get table schema
      const tableInfo = source.pragma('table_info', tableName);
      if (!tableInfo || !Array.isArray(tableInfo) || tableInfo.length === 0) continue;

      // Build CREATE TABLE statement
      // Note: Use unquoted column names for compatibility with DoSQL parser
      const columns = tableInfo.map((col: any) => {
        let def = `${col.name} ${col.type || 'TEXT'}`;
        if (col.pk) def += ' PRIMARY KEY';
        if (col.notnull) def += ' NOT NULL';
        if (col.dflt_value !== null) def += ` DEFAULT ${col.dflt_value}`;
        return def;
      });
      const createSql = `CREATE TABLE IF NOT EXISTS ${tableName} (${columns.join(', ')})`;
      this.exec(createSql);

      // Copy data
      const rows = source.prepare(`SELECT * FROM ${tableName}`).all();
      if (rows.length > 0) {
        const columnNames = tableInfo.map((col: any) => col.name);
        const placeholders = columnNames.map(() => '?').join(', ');
        const insertSql = `INSERT INTO ${tableName} (${columnNames.join(', ')}) VALUES (${placeholders})`;
        const insertStmt = this.prepare(insertSql);
        for (const row of rows) {
          const values = columnNames.map((col: string) => (row as any)[col]);
          insertStmt.run(...values);
        }
      }

      // Copy indexes - generate CREATE INDEX from index_list info
      const indexList = source.pragma('index_list', tableName);
      if (indexList && Array.isArray(indexList)) {
        for (const idx of indexList) {
          if (idx.origin === 'c') {
            // Build CREATE INDEX statement from index name and table
            // For DoSQL in-memory DB, we don't have sqlite_master, so skip
            // Index creation will be handled during export to file
          }
        }
      }
    }
  }

  /**
   * Export database to a file
   *
   * Creates a standard SQLite database file that can be read by
   * sqlite3 CLI, better-sqlite3, @libsql/client, and other SQLite tools.
   *
   * @param options - Export options
   */
  async exportTo(options: ExportOptions): Promise<void> {
    // Check if file exists and overwrite is not set
    if (fs.existsSync(options.path)) {
      if (!options.overwrite) {
        throw new Error(`SQLITE_ERROR: file exists and overwrite is false - ${options.path}`);
      }
      // Delete existing file to start fresh
      fs.unlinkSync(options.path);
    }

    // Ensure parent directory exists
    const dir = path.dirname(options.path);
    if (!fs.existsSync(dir)) {
      throw new Error(`SQLITE_CANTOPEN: unable to open database file - directory ${dir} not found`);
    }

    // Create target database
    const targetDb = new BetterSqlite3(options.path);

    try {
      // Set page size before any operations
      if (options.pageSize) {
        targetDb.pragma(`page_size = ${options.pageSize}`);
      }

      // Set journal mode
      if (options.journalMode) {
        targetDb.pragma(`journal_mode = ${options.journalMode.toUpperCase()}`);
      }

      // Determine tables to export
      let tablesToExport: string[];
      if (options.tables) {
        tablesToExport = options.tables;
      } else {
        tablesToExport = this.getTables();
      }

      // Export each table
      for (const tableName of tablesToExport) {
        // Get table schema
        const tableInfo = this.pragma('table_info', tableName);
        if (!tableInfo || !Array.isArray(tableInfo) || tableInfo.length === 0) continue;

        // Build CREATE TABLE statement with full schema
        const createSql = this.getCreateTableSql(tableName);
        if (createSql) {
          targetDb.exec(createSql);
        } else {
          // Fallback: build from table_info
          const columns = tableInfo.map((col: any) => {
            let def = `"${col.name}" ${col.type || 'TEXT'}`;
            if (col.pk) def += ' PRIMARY KEY';
            if (col.notnull) def += ' NOT NULL';
            if (col.dflt_value !== null) def += ` DEFAULT ${col.dflt_value}`;
            return def;
          });
          targetDb.exec(`CREATE TABLE "${tableName}" (${columns.join(', ')})`);
        }

        // Copy data unless schemaOnly
        if (!options.schemaOnly) {
          const rows = this.prepare(`SELECT * FROM ${tableName}`).all();
          if (rows.length > 0) {
            const columnNames = tableInfo.map((col: any) => col.name);
            const placeholders = columnNames.map(() => '?').join(', ');
            const insertStmt = targetDb.prepare(
              `INSERT INTO "${tableName}" (${columnNames.map(n => `"${n}"`).join(', ')}) VALUES (${placeholders})`
            );

            for (const row of rows) {
              const values = columnNames.map((col: string) => {
                const val = (row as any)[col];
                // Handle Uint8Array -> Buffer for better-sqlite3
                if (val instanceof Uint8Array) {
                  return Buffer.from(val);
                }
                return val;
              });
              insertStmt.run(...values);
            }
          }
        }

        // Copy indexes
        const indexList = this.pragma('index_list', tableName);
        if (indexList && Array.isArray(indexList)) {
          for (const idx of indexList) {
            if (idx.origin === 'c') {
              const indexSql = this.getIndexSql(idx.name);
              if (indexSql) {
                try {
                  targetDb.exec(indexSql);
                } catch {
                  // Index may already exist
                }
              }
            }
          }
        }
      }
    } finally {
      targetDb.close();
    }
  }

  /**
   * Get CREATE TABLE SQL from sqlite_master
   */
  private getCreateTableSql(tableName: string): string | null {
    if (this.nativeDb) {
      const result = this.nativeDb.prepare(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?"
      ).get(tableName) as { sql: string } | undefined;
      return result?.sql ?? null;
    }
    return null;
  }

  /**
   * Get CREATE INDEX SQL from sqlite_master
   */
  private getIndexSql(indexName: string): string | null {
    if (this.nativeDb) {
      const result = this.nativeDb.prepare(
        "SELECT sql FROM sqlite_master WHERE type='index' AND name=?"
      ).get(indexName) as { sql: string } | undefined;
      return result?.sql ?? null;
    }
    return null;
  }

  /**
   * Import tables from a SQLite file
   *
   * @param options - Import options
   */
  async importFrom(options: ImportOptions): Promise<void> {
    if (!fs.existsSync(options.path)) {
      throw new Error(`SQLITE_CANTOPEN: unable to open database file - ${options.path} not found`);
    }

    // Validate it's a SQLite database
    const buffer = Buffer.alloc(16);
    const fd = fs.openSync(options.path, 'r');
    fs.readSync(fd, buffer, 0, 16, 0);
    fs.closeSync(fd);
    if (buffer.toString('utf8', 0, 15) !== 'SQLite format 3') {
      throw new Error('SQLITE_NOTADB: file is not a database');
    }

    // Open source database
    const sourceDb = new BetterSqlite3(options.path, { readonly: true });

    try {
      // Get tables to import
      let tablesToImport: string[];
      if (options.tables) {
        tablesToImport = options.tables;
      } else {
        const tables = sourceDb.prepare(
          "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).all() as { name: string }[];
        tablesToImport = tables.map(t => t.name);
      }

      // Import each table
      for (const tableName of tablesToImport) {
        const existsInTarget = this.hasTable(tableName);

        if (existsInTarget) {
          if (options.skipExisting) {
            continue;
          }
          if (options.replace) {
            // For replace, we need to drop and recreate
            // In-memory engine doesn't support DROP TABLE directly
            // So we delete all rows and copy new data
            try {
              this.exec(`DELETE FROM ${tableName}`);
            } catch {
              // Table structure might differ, skip
            }
            // Get table schema for column info
            const tableInfo = sourceDb.pragma(`table_info(${tableName})`);
            // Copy data
            const rows = sourceDb.prepare(`SELECT * FROM "${tableName}"`).all();
            if (rows.length > 0 && tableInfo && tableInfo.length > 0) {
              const columnNames = tableInfo.map((col: any) => col.name);
              const placeholders = columnNames.map(() => '?').join(', ');
              const insertStmt = this.prepare(
                `INSERT INTO ${tableName} (${columnNames.join(', ')}) VALUES (${placeholders})`
              );

              for (const row of rows) {
                const values = columnNames.map((col: string) => {
                  const val = (row as any)[col];
                  if (Buffer.isBuffer(val)) {
                    return new Uint8Array(val);
                  }
                  return val;
                });
                insertStmt.run(...values);
              }
            }
            continue;
          } else {
            throw new Error(`Table ${tableName} already exists`);
          }
        }

        // Get table schema from pragma and build CREATE TABLE
        const tableInfo = sourceDb.pragma(`table_info(${tableName})`);
        if (!tableInfo || !Array.isArray(tableInfo) || tableInfo.length === 0) continue;

        // Build CREATE TABLE statement using unquoted identifiers for DoSQL compatibility
        const columns = tableInfo.map((col: any) => {
          let def = `${col.name} ${col.type || 'TEXT'}`;
          if (col.pk) def += ' PRIMARY KEY';
          if (col.notnull) def += ' NOT NULL';
          if (col.dflt_value !== null) def += ` DEFAULT ${col.dflt_value}`;
          return def;
        });
        const createSql = `CREATE TABLE IF NOT EXISTS ${tableName} (${columns.join(', ')})`;
        this.exec(createSql);

        // Copy data
        const rows = sourceDb.prepare(`SELECT * FROM "${tableName}"`).all();
        if (rows.length > 0) {
          const columnNames = tableInfo.map((col: any) => col.name);
          const placeholders = columnNames.map(() => '?').join(', ');
          const insertStmt = this.prepare(
            `INSERT INTO ${tableName} (${columnNames.join(', ')}) VALUES (${placeholders})`
          );

          for (const row of rows) {
            const values = columnNames.map((col: string) => {
              const val = (row as any)[col];
              // Handle Buffer -> Uint8Array for DoSQL
              if (Buffer.isBuffer(val)) {
                return new Uint8Array(val);
              }
              return val;
            });
            insertStmt.run(...values);
          }
        }

        // Copy indexes
        const indexes = sourceDb.prepare(
          "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL"
        ).all(tableName) as { sql: string }[];

        for (const idx of indexes) {
          try {
            this.exec(idx.sql);
          } catch {
            // Index may conflict
          }
        }
      }
    } finally {
      sourceDb.close();
    }
  }

  /**
   * Get file statistics
   *
   * @returns Database file statistics
   */
  getFileStats(): DatabaseFileStats {
    if (!this.nativeDb) {
      // For in-memory databases, return default stats
      return {
        size: 0,
        pageSize: 4096,
        pageCount: 0,
        freePageCount: 0,
        schemaVersion: 0,
        formatVersion: 1,
        encoding: 'UTF-8',
        journalMode: 'memory',
        isWalMode: false,
      };
    }

    const pageSize = this.nativeDb.pragma('page_size', { simple: true }) as number;
    const pageCount = this.nativeDb.pragma('page_count', { simple: true }) as number;
    const freePageCount = this.nativeDb.pragma('freelist_count', { simple: true }) as number;
    const schemaVersion = this.nativeDb.pragma('schema_version', { simple: true }) as number;
    const journalMode = this.nativeDb.pragma('journal_mode', { simple: true }) as string;
    const encoding = this.nativeDb.pragma('encoding', { simple: true }) as string;

    // Get file size
    let size = 0;
    if (this._filename && fs.existsSync(this._filename)) {
      size = fs.statSync(this._filename).size;
    }

    // Check WAL size if in WAL mode
    let walCheckpointSize: number | undefined;
    const isWalMode = journalMode.toLowerCase() === 'wal';
    if (isWalMode && this._filename) {
      const walPath = `${this._filename}-wal`;
      if (fs.existsSync(walPath)) {
        walCheckpointSize = fs.statSync(walPath).size;
      }
    }

    // Map encoding to our enum
    let encodingEnum: 'UTF-8' | 'UTF-16le' | 'UTF-16be' = 'UTF-8';
    if (encoding.toLowerCase().includes('utf-16le')) {
      encodingEnum = 'UTF-16le';
    } else if (encoding.toLowerCase().includes('utf-16be')) {
      encodingEnum = 'UTF-16be';
    }

    return {
      size,
      pageSize,
      pageCount,
      freePageCount,
      schemaVersion,
      formatVersion: 1,
      encoding: encodingEnum,
      journalMode,
      isWalMode,
      walCheckpointSize,
    };
  }

  /**
   * Checkpoint WAL (if in WAL mode)
   *
   * @param mode - Checkpoint mode
   */
  checkpoint(mode: CheckpointMode = 'passive'): void {
    if (!this.nativeDb) {
      throw new Error('Cannot checkpoint an in-memory database');
    }

    const modeMap: Record<CheckpointMode, string> = {
      passive: 'PASSIVE',
      full: 'FULL',
      restart: 'RESTART',
      truncate: 'TRUNCATE',
    };

    this.nativeDb.pragma(`wal_checkpoint(${modeMap[mode]})`);
  }

  /**
   * Vacuum the database file
   *
   * @param into - Optional path for VACUUM INTO
   */
  vacuum(into?: string): void {
    if (this.nativeDb) {
      if (into) {
        this.nativeDb.exec(`VACUUM INTO '${into.replace(/'/g, "''")}'`);
      } else {
        this.nativeDb.exec('VACUUM');
      }
    } else if (into) {
      // For in-memory database, export to file
      const targetDb = new BetterSqlite3(into);
      try {
        // Export schema and data
        const tables = this.getTables();
        for (const tableName of tables) {
          const tableInfo = this.pragma('table_info', tableName);
          if (!tableInfo || !Array.isArray(tableInfo) || tableInfo.length === 0) continue;

          const columns = tableInfo.map((col: any) => {
            let def = `"${col.name}" ${col.type || 'TEXT'}`;
            if (col.pk) def += ' PRIMARY KEY';
            if (col.notnull) def += ' NOT NULL';
            if (col.dflt_value !== null) def += ` DEFAULT ${col.dflt_value}`;
            return def;
          });
          targetDb.exec(`CREATE TABLE "${tableName}" (${columns.join(', ')})`);

          // Use unquoted identifiers for DoSQL compatibility
          const rows = this.prepare(`SELECT * FROM ${tableName}`).all();
          if (rows.length > 0) {
            const columnNames = tableInfo.map((col: any) => col.name);
            const placeholders = columnNames.map(() => '?').join(', ');
            const insertStmt = targetDb.prepare(
              `INSERT INTO "${tableName}" (${columnNames.map(n => `"${n}"`).join(', ')}) VALUES (${placeholders})`
            );
            for (const row of rows) {
              const values = columnNames.map((col: string) => {
                const val = (row as any)[col];
                if (val instanceof Uint8Array) {
                  return Buffer.from(val);
                }
                return val;
              });
              insertStmt.run(...values);
            }
          }
        }
      } finally {
        targetDb.close();
      }
    }
  }

  /**
   * Serialize database to buffer
   *
   * @returns Buffer containing the serialized database
   */
  serialize(): Buffer {
    if (this.nativeDb) {
      return this.nativeDb.serialize();
    }

    // For in-memory database, create a temporary file
    const tempPath = `/tmp/dosql-serialize-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`;
    try {
      const tempDb = new BetterSqlite3(tempPath);

      // Export schema and data
      const tables = this.getTables();
      for (const tableName of tables) {
        const tableInfo = this.pragma('table_info', tableName);
        if (!tableInfo || !Array.isArray(tableInfo) || tableInfo.length === 0) continue;

        const columns = tableInfo.map((col: any) => {
          let def = `"${col.name}" ${col.type || 'TEXT'}`;
          if (col.pk) def += ' PRIMARY KEY';
          if (col.notnull) def += ' NOT NULL';
          if (col.dflt_value !== null) def += ` DEFAULT ${col.dflt_value}`;
          return def;
        });
        tempDb.exec(`CREATE TABLE "${tableName}" (${columns.join(', ')})`);

        // Use unquoted identifiers for DoSQL compatibility
        const rows = this.prepare(`SELECT * FROM ${tableName}`).all();
        if (rows.length > 0) {
          const columnNames = tableInfo.map((col: any) => col.name);
          const placeholders = columnNames.map(() => '?').join(', ');
          const insertStmt = tempDb.prepare(
            `INSERT INTO "${tableName}" (${columnNames.map(n => `"${n}"`).join(', ')}) VALUES (${placeholders})`
          );
          for (const row of rows) {
            const values = columnNames.map((col: string) => {
              const val = (row as any)[col];
              if (val instanceof Uint8Array) {
                return Buffer.from(val);
              }
              return val;
            });
            insertStmt.run(...values);
          }
        }
      }

      const buffer = tempDb.serialize();
      tempDb.close();
      return buffer;
    } finally {
      if (fs.existsSync(tempPath)) {
        fs.unlinkSync(tempPath);
      }
    }
  }

  /**
   * Create from serialized buffer
   *
   * @param buffer - Serialized database buffer
   * @param options - Optional file-backed database options
   * @returns New FileBackedDatabase instance
   */
  static deserialize(buffer: Buffer, options?: Partial<FileBackedDatabaseOptions>): FileBackedDatabase {
    // Validate buffer is a SQLite database
    if (buffer.length < 16 || buffer.toString('utf8', 0, 15) !== 'SQLite format 3') {
      throw new Error('SQLITE_NOTADB: buffer is not a valid SQLite database');
    }

    // Create a temporary file to deserialize into
    const tempPath = `/tmp/dosql-deserialize-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`;
    try {
      fs.writeFileSync(tempPath, buffer);

      // Create file-backed database from temp file
      const sourceDb = new BetterSqlite3(tempPath, { readonly: true });

      // Create result database
      const result = new FileBackedDatabase();

      // Copy schema and data
      const tables = sourceDb.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
      ).all() as { name: string }[];

      for (const { name: tableName } of tables) {
        // Get table schema from pragma and build CREATE TABLE
        const tableInfo = sourceDb.pragma(`table_info(${tableName})`);
        if (!tableInfo || !Array.isArray(tableInfo) || tableInfo.length === 0) continue;

        // Build CREATE TABLE statement using unquoted identifiers for DoSQL compatibility
        const columns = tableInfo.map((col: any) => {
          let def = `${col.name} ${col.type || 'TEXT'}`;
          if (col.pk) def += ' PRIMARY KEY';
          if (col.notnull) def += ' NOT NULL';
          if (col.dflt_value !== null) def += ` DEFAULT ${col.dflt_value}`;
          return def;
        });
        const createSql = `CREATE TABLE IF NOT EXISTS ${tableName} (${columns.join(', ')})`;
        result.exec(createSql);

        // Copy data
        const rows = sourceDb.prepare(`SELECT * FROM "${tableName}"`).all();
        if (rows.length > 0) {
          const columnNames = tableInfo.map((col: any) => col.name);
          const placeholders = columnNames.map(() => '?').join(', ');
          const insertStmt = result.prepare(
            `INSERT INTO ${tableName} (${columnNames.join(', ')}) VALUES (${placeholders})`
          );

          for (const row of rows) {
            const values = columnNames.map((col: string) => {
              const val = (row as any)[col];
              if (Buffer.isBuffer(val)) {
                return new Uint8Array(val);
              }
              return val;
            });
            insertStmt.run(...values);
          }
        }
      }

      sourceDb.close();
      return result;
    } finally {
      if (fs.existsSync(tempPath)) {
        fs.unlinkSync(tempPath);
      }
    }
  }

  /**
   * Override prepare to use native db when available
   */
  override prepare<T = unknown, P extends BindParameters = BindParameters>(
    sql: string
  ): Statement<T, P> {
    // For file-backed databases with native db, we could delegate to native
    // but for consistency with DoSQL API, we use the parent implementation
    return super.prepare<T, P>(sql);
  }

  /**
   * Override close to also close native db
   */
  override close(): this {
    if (this.nativeDb) {
      this.nativeDb.close();
      this.nativeDb = null;
    }
    return super.close();
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a file-backed database
 *
 * @param options - File-backed database options
 * @returns FileBackedDatabase instance
 */
export function createFileBackedDatabase(options?: FileBackedDatabaseOptions): FileBackedDatabase {
  return new FileBackedDatabase(options);
}

/**
 * Open an existing SQLite file
 *
 * @param filename - Path to SQLite file
 * @param options - Additional options
 * @returns FileBackedDatabase instance
 */
export function openDatabase(
  filename: string,
  options?: Omit<FileBackedDatabaseOptions, 'filename'>
): FileBackedDatabase {
  return new FileBackedDatabase({ ...options, filename, create: false });
}

// =============================================================================
// EXPORTS
// =============================================================================

export type {
  SqlValue,
  BindParameters,
  Statement,
  RunResult,
};
