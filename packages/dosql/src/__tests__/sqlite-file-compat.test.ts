/**
 * SQLite File Compatibility Tests for DoSQL - RED Phase TDD
 *
 * Issue: sql-glij.1
 *
 * These tests document the MISSING file compatibility behavior.
 * DoSQL should produce SQLite database files that are readable by
 * standard SQLite tools (sqlite3 CLI, better-sqlite3, @libsql/client).
 *
 * Tests use `it.fails()` pattern to document expected behavior not yet implemented.
 *
 * Focus areas:
 * 1. Writing a DoSQL database that sqlite3 can read
 * 2. Reading an existing .sqlite file into DoSQL
 * 3. File format compatibility (page size, encoding, journaling)
 * 4. Concurrent access patterns
 * 5. WAL mode compatibility
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll, afterAll } from 'vitest';
import { Database } from '../database.js';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';

// =============================================================================
// EXTENDED TYPES FOR TDD (Expected Future API)
// =============================================================================

/**
 * File-backed database options
 * Expected future API for DoSQL file persistence
 */
interface FileBackedDatabaseOptions {
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
  /** Journal mode: 'delete' | 'truncate' | 'persist' | 'memory' | 'wal' | 'off' */
  journalMode?: 'delete' | 'truncate' | 'persist' | 'memory' | 'wal' | 'off';
  /** Synchronous mode: 'off' | 'normal' | 'full' | 'extra' */
  synchronous?: 'off' | 'normal' | 'full' | 'extra';
  /** Page size in bytes (must be power of 2, 512-65536) */
  pageSize?: number;
  /** Lock timeout in milliseconds */
  busyTimeout?: number;
}

/**
 * File export options
 */
interface ExportOptions {
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
  journalMode?: 'delete' | 'truncate' | 'persist' | 'memory' | 'wal' | 'off';
}

/**
 * File import options
 */
interface ImportOptions {
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
interface DatabaseFileStats {
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

/**
 * Extended Database interface for file operations
 */
interface FileBackedDatabase extends Database {
  /** Get database file path (null if in-memory) */
  readonly filename: string | null;
  /** Whether this is a file-backed database */
  readonly isFileBacked: boolean;
  /** Export database to a file */
  exportTo(options: ExportOptions): Promise<void>;
  /** Import from a SQLite file */
  importFrom(options: ImportOptions): Promise<void>;
  /** Get file statistics */
  getFileStats(): DatabaseFileStats;
  /** Checkpoint WAL (if in WAL mode) */
  checkpoint(mode?: 'passive' | 'full' | 'restart' | 'truncate'): void;
  /** Vacuum the database file */
  vacuum(into?: string): void;
  /** Serialize database to buffer (for transfer) */
  serialize(): Buffer;
  /** Create from serialized buffer */
  static deserialize(buffer: Buffer, options?: FileBackedDatabaseOptions): FileBackedDatabase;
}

/**
 * Factory function type for creating file-backed databases
 */
type CreateFileBackedDatabase = (options: FileBackedDatabaseOptions) => FileBackedDatabase;

// =============================================================================
// DOCUMENTED GAPS - Features that should be implemented
// =============================================================================
//
// 1. File persistence: Database constructor should accept filename option
// 2. Export API: exportTo() method for writing to .sqlite files
// 3. Import API: importFrom() method for reading from .sqlite files
// 4. Page size control: pageSize pragma support in file operations
// 5. Journal modes: Full journal_mode support including WAL
// 6. WAL compatibility: Proper WAL file handling (-wal, -shm files)
// 7. Concurrent reads: Multiple readers with single writer
// 8. Checkpoint API: Explicit WAL checkpoint control
// 9. Vacuum support: VACUUM and VACUUM INTO commands
// 10. Serialize/deserialize: Buffer-based database transfer
// 11. File stats: getFileStats() for file metadata
// 12. Cross-platform paths: Windows/Unix path normalization
//
// =============================================================================

// =============================================================================
// TEST UTILITIES
// =============================================================================

let tempDir: string;

/**
 * Create a temporary directory for test files
 */
function createTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'dosql-test-'));
}

/**
 * Clean up temporary directory
 */
function cleanupTempDir(dir: string): void {
  if (fs.existsSync(dir)) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

/**
 * Generate a temp file path
 */
function tempFilePath(name: string): string {
  return path.join(tempDir, name);
}

/**
 * Check if a file is a valid SQLite database
 */
function isValidSQLiteFile(filepath: string): boolean {
  if (!fs.existsSync(filepath)) {
    return false;
  }
  const buffer = Buffer.alloc(16);
  const fd = fs.openSync(filepath, 'r');
  fs.readSync(fd, buffer, 0, 16, 0);
  fs.closeSync(fd);
  // SQLite magic header: "SQLite format 3\0"
  return buffer.toString('utf8', 0, 15) === 'SQLite format 3';
}

/**
 * Read SQLite page size from file header
 */
function readPageSizeFromFile(filepath: string): number {
  const buffer = Buffer.alloc(100);
  const fd = fs.openSync(filepath, 'r');
  fs.readSync(fd, buffer, 0, 100, 0);
  fs.closeSync(fd);
  // Page size is at offset 16-17 (big-endian)
  const pageSize = buffer.readUInt16BE(16);
  // Page size of 1 means 65536
  return pageSize === 1 ? 65536 : pageSize;
}

/**
 * Read SQLite encoding from file header
 */
function readEncodingFromFile(filepath: string): number {
  const buffer = Buffer.alloc(100);
  const fd = fs.openSync(filepath, 'r');
  fs.readSync(fd, buffer, 0, 100, 0);
  fs.closeSync(fd);
  // Text encoding is at offset 56-59 (big-endian)
  return buffer.readUInt32BE(56);
}

// =============================================================================
// TEST SETUP
// =============================================================================

beforeAll(() => {
  tempDir = createTempDir();
});

afterAll(() => {
  cleanupTempDir(tempDir);
});

// =============================================================================
// 1. WRITING A DOSQL DATABASE THAT SQLITE3 CAN READ
// =============================================================================

describe('Write DoSQL database readable by sqlite3', () => {
  /**
   * GAP: DoSQL should be able to export to a standard SQLite file
   * Currently: No file export capability exists
   */
  it.fails('should export in-memory database to SQLite file', async () => {
    const db = new Database();
    db.exec(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE
      )
    `);
    db.exec("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')");
    db.exec("INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')");

    const filepath = tempFilePath('export-test.sqlite');

    // GAP: exportTo method should exist
    await (db as unknown as FileBackedDatabase).exportTo({
      path: filepath,
      overwrite: true,
    });

    // Verify file was created and is valid SQLite
    expect(fs.existsSync(filepath)).toBe(true);
    expect(isValidSQLiteFile(filepath)).toBe(true);
  });

  /**
   * GAP: Exported file should be readable by better-sqlite3
   */
  it.fails('should create file readable by better-sqlite3', async () => {
    const db = new Database();
    db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)');
    db.exec("INSERT INTO items (value) VALUES ('test1'), ('test2')");

    const filepath = tempFilePath('better-sqlite3-compat.sqlite');
    await (db as unknown as FileBackedDatabase).exportTo({ path: filepath, overwrite: true });

    // This would require better-sqlite3 to be available in test environment
    // The test documents the expectation that the file should be compatible
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const externalDb = new BetterSqlite3(filepath, { readonly: true });

    const rows = externalDb.prepare('SELECT * FROM items').all();
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ id: 1, value: 'test1' });

    externalDb.close();
  });

  /**
   * GAP: Exported file should preserve schema exactly
   */
  it.fails('should preserve table schema in exported file', async () => {
    const db = new Database();
    db.exec(`
      CREATE TABLE complex_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL DEFAULT 'unnamed',
        score REAL CHECK(score >= 0),
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        data BLOB
      )
    `);
    db.exec(`CREATE INDEX idx_name ON complex_table(name)`);
    db.exec(`CREATE UNIQUE INDEX idx_score ON complex_table(score) WHERE score > 100`);

    const filepath = tempFilePath('schema-preserve.sqlite');
    await (db as unknown as FileBackedDatabase).exportTo({ path: filepath, overwrite: true });

    // Read back with better-sqlite3 and verify schema
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const externalDb = new BetterSqlite3(filepath, { readonly: true });

    const tableInfo = externalDb.pragma('table_info(complex_table)');
    expect(tableInfo).toHaveLength(5);
    expect(tableInfo.find((c: any) => c.name === 'name').notnull).toBe(1);
    expect(tableInfo.find((c: any) => c.name === 'name').dflt_value).toBe("'unnamed'");

    const indexList = externalDb.pragma('index_list(complex_table)');
    expect(indexList).toHaveLength(2);

    externalDb.close();
  });

  /**
   * GAP: Should support exporting specific tables only
   */
  it.fails('should export only specified tables', async () => {
    const db = new Database();
    db.exec('CREATE TABLE table1 (id INTEGER PRIMARY KEY)');
    db.exec('CREATE TABLE table2 (id INTEGER PRIMARY KEY)');
    db.exec('CREATE TABLE table3 (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('selective-export.sqlite');
    await (db as unknown as FileBackedDatabase).exportTo({
      path: filepath,
      overwrite: true,
      tables: ['table1', 'table3'],
    });

    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const externalDb = new BetterSqlite3(filepath, { readonly: true });

    const tables = externalDb
      .prepare("SELECT name FROM sqlite_master WHERE type='table'")
      .pluck()
      .all();
    expect(tables).toContain('table1');
    expect(tables).toContain('table3');
    expect(tables).not.toContain('table2');

    externalDb.close();
  });

  /**
   * GAP: Should support schema-only export (no data)
   */
  it.fails('should export schema only without data', async () => {
    const db = new Database();
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Carol')");

    const filepath = tempFilePath('schema-only.sqlite');
    await (db as unknown as FileBackedDatabase).exportTo({
      path: filepath,
      overwrite: true,
      schemaOnly: true,
    });

    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const externalDb = new BetterSqlite3(filepath, { readonly: true });

    // Table should exist
    const tables = externalDb
      .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
      .pluck()
      .all();
    expect(tables).toHaveLength(1);

    // But no data
    const count = externalDb.prepare('SELECT COUNT(*) FROM users').pluck().get();
    expect(count).toBe(0);

    externalDb.close();
  });
});

// =============================================================================
// 2. READING AN EXISTING .SQLITE FILE INTO DOSQL
// =============================================================================

describe('Read existing .sqlite file into DoSQL', () => {
  let testDbPath: string;

  beforeEach(async () => {
    // Create a test SQLite file using better-sqlite3
    testDbPath = tempFilePath(`test-${Date.now()}.sqlite`);
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const externalDb = new BetterSqlite3(testDbPath);

    externalDb.exec(`
      CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        price REAL,
        stock INTEGER DEFAULT 0
      )
    `);
    externalDb.exec(`
      INSERT INTO products (name, price, stock) VALUES
        ('Widget', 9.99, 100),
        ('Gadget', 19.99, 50),
        ('Doohickey', 29.99, 25)
    `);
    externalDb.close();
  });

  /**
   * GAP: DoSQL should be able to import from existing SQLite file
   * Currently: No file import capability exists
   */
  it.fails('should import tables from SQLite file', async () => {
    const db = new Database();

    // GAP: importFrom method should exist
    await (db as unknown as FileBackedDatabase).importFrom({
      path: testDbPath,
    });

    // Verify data was imported
    const products = db.prepare('SELECT * FROM products ORDER BY id').all();
    expect(products).toHaveLength(3);
    expect(products[0]).toMatchObject({ id: 1, name: 'Widget', price: 9.99, stock: 100 });
  });

  /**
   * GAP: Should open file-backed database directly
   */
  it.fails('should open SQLite file directly as database', () => {
    // GAP: Database constructor should accept filename
    const db = new Database({ filename: testDbPath } as any);

    expect((db as unknown as FileBackedDatabase).isFileBacked).toBe(true);
    expect((db as unknown as FileBackedDatabase).filename).toBe(testDbPath);

    const count = db.prepare('SELECT COUNT(*) FROM products').pluck().get();
    expect(count).toBe(3);
  });

  /**
   * GAP: Should support read-only file access
   */
  it.fails('should open SQLite file in read-only mode', () => {
    const db = new Database({
      filename: testDbPath,
      readonly: true,
    } as any);

    expect(db.readonly).toBe(true);

    // Should be able to read
    const count = db.prepare('SELECT COUNT(*) FROM products').pluck().get();
    expect(count).toBe(3);

    // Should not be able to write
    expect(() => {
      db.exec("INSERT INTO products (name, price) VALUES ('Test', 1.00)");
    }).toThrow(/readonly/i);
  });

  /**
   * GAP: Should import only specified tables
   */
  it.fails('should import only specified tables', async () => {
    // Create source with multiple tables
    const sourcePath = tempFilePath('multi-table.sqlite');
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const sourceDb = new BetterSqlite3(sourcePath);
    sourceDb.exec('CREATE TABLE table_a (id INTEGER PRIMARY KEY)');
    sourceDb.exec('CREATE TABLE table_b (id INTEGER PRIMARY KEY)');
    sourceDb.exec('CREATE TABLE table_c (id INTEGER PRIMARY KEY)');
    sourceDb.close();

    const db = new Database();
    await (db as unknown as FileBackedDatabase).importFrom({
      path: sourcePath,
      tables: ['table_a', 'table_c'],
    });

    // Only specified tables should exist
    const stmt = db.prepare(
      "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    );
    const tables = stmt.pluck().all();
    expect(tables).toEqual(['table_a', 'table_c']);
  });

  /**
   * GAP: Should handle import with existing tables
   */
  it.fails('should skip existing tables during import', async () => {
    const db = new Database();
    db.exec('CREATE TABLE products (id INTEGER PRIMARY KEY, custom TEXT)');
    db.exec("INSERT INTO products (custom) VALUES ('existing')");

    await (db as unknown as FileBackedDatabase).importFrom({
      path: testDbPath,
      skipExisting: true,
    });

    // Original table should be preserved
    const products = db.prepare('SELECT * FROM products').all();
    expect(products).toHaveLength(1);
    expect((products[0] as any).custom).toBe('existing');
  });

  /**
   * GAP: Should replace existing tables during import
   */
  it.fails('should replace existing tables during import', async () => {
    const db = new Database();
    db.exec('CREATE TABLE products (id INTEGER PRIMARY KEY, custom TEXT)');
    db.exec("INSERT INTO products (custom) VALUES ('existing')");

    await (db as unknown as FileBackedDatabase).importFrom({
      path: testDbPath,
      replace: true,
    });

    // Table should have been replaced with imported data
    const products = db.prepare('SELECT * FROM products ORDER BY id').all();
    expect(products).toHaveLength(3);
    expect((products[0] as any).name).toBe('Widget');
  });
});

// =============================================================================
// 3. FILE FORMAT COMPATIBILITY (PAGE SIZE, ENCODING, JOURNALING)
// =============================================================================

describe('File format compatibility', () => {
  /**
   * GAP: Should support configurable page size
   */
  it.fails('should export with specified page size', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('pagesize-4096.sqlite');
    await (db as unknown as FileBackedDatabase).exportTo({
      path: filepath,
      overwrite: true,
      pageSize: 4096,
    });

    const pageSize = readPageSizeFromFile(filepath);
    expect(pageSize).toBe(4096);
  });

  /**
   * GAP: Should support different page sizes (1024, 2048, 4096, 8192, 16384, 32768, 65536)
   */
  it.fails('should support all valid SQLite page sizes', async () => {
    const validPageSizes = [1024, 2048, 4096, 8192, 16384, 32768, 65536];
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    for (const pageSize of validPageSizes) {
      const filepath = tempFilePath(`pagesize-${pageSize}.sqlite`);
      await (db as unknown as FileBackedDatabase).exportTo({
        path: filepath,
        overwrite: true,
        pageSize,
      });

      const readSize = readPageSizeFromFile(filepath);
      expect(readSize).toBe(pageSize);
    }
  });

  /**
   * GAP: Should export with UTF-8 encoding (SQLite default)
   */
  it.fails('should export with UTF-8 encoding', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, text TEXT)');
    db.exec("INSERT INTO test (text) VALUES ('Hello'), ('World')");

    const filepath = tempFilePath('utf8-encoding.sqlite');
    await (db as unknown as FileBackedDatabase).exportTo({
      path: filepath,
      overwrite: true,
    });

    // UTF-8 encoding value is 1
    const encoding = readEncodingFromFile(filepath);
    expect(encoding).toBe(1);
  });

  /**
   * GAP: Should export with specified journal mode
   */
  it.fails('should export with DELETE journal mode', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('journal-delete.sqlite');
    await (db as unknown as FileBackedDatabase).exportTo({
      path: filepath,
      overwrite: true,
      journalMode: 'delete',
    });

    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const externalDb = new BetterSqlite3(filepath);
    const mode = externalDb.pragma('journal_mode', { simple: true });
    expect(mode).toBe('delete');
    externalDb.close();
  });

  /**
   * GAP: Should export with WAL journal mode
   */
  it.fails('should export with WAL journal mode', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('journal-wal.sqlite');
    await (db as unknown as FileBackedDatabase).exportTo({
      path: filepath,
      overwrite: true,
      journalMode: 'wal',
    });

    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const externalDb = new BetterSqlite3(filepath);
    const mode = externalDb.pragma('journal_mode', { simple: true });
    expect(mode).toBe('wal');
    externalDb.close();
  });

  /**
   * GAP: Should provide file statistics
   */
  it.fails('should return file statistics for exported database', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)');
    for (let i = 0; i < 100; i++) {
      db.exec(`INSERT INTO test (data) VALUES ('${'x'.repeat(1000)}')`);
    }

    const filepath = tempFilePath('stats-test.sqlite');
    await (db as unknown as FileBackedDatabase).exportTo({
      path: filepath,
      overwrite: true,
      pageSize: 4096,
    });

    // Open as file-backed and get stats
    const fileDb = new Database({ filename: filepath } as any);
    const stats = (fileDb as unknown as FileBackedDatabase).getFileStats();

    expect(stats.pageSize).toBe(4096);
    expect(stats.pageCount).toBeGreaterThan(0);
    expect(stats.encoding).toBe('UTF-8');
    expect(stats.size).toBeGreaterThan(0);
  });
});

// =============================================================================
// 4. CONCURRENT ACCESS PATTERNS
// =============================================================================

describe('Concurrent access patterns', () => {
  let sharedDbPath: string;

  beforeEach(async () => {
    sharedDbPath = tempFilePath(`shared-${Date.now()}.sqlite`);
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const db = new BetterSqlite3(sharedDbPath);
    db.exec('CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)');
    db.exec('INSERT INTO counter (value) VALUES (0)');
    db.close();
  });

  /**
   * GAP: Should support multiple readers on same file
   */
  it.fails('should allow multiple readers on same database file', () => {
    const reader1 = new Database({
      filename: sharedDbPath,
      readonly: true,
    } as any);
    const reader2 = new Database({
      filename: sharedDbPath,
      readonly: true,
    } as any);

    // Both should be able to read simultaneously
    const value1 = reader1.prepare('SELECT value FROM counter').pluck().get();
    const value2 = reader2.prepare('SELECT value FROM counter').pluck().get();

    expect(value1).toBe(0);
    expect(value2).toBe(0);

    reader1.close();
    reader2.close();
  });

  /**
   * GAP: Should handle busy timeout for concurrent writes
   */
  it.fails('should respect busy timeout for concurrent access', async () => {
    // Writer 1 starts a long transaction
    const writer1 = new Database({
      filename: sharedDbPath,
      busyTimeout: 1000,
    } as any);

    // Writer 2 with short timeout
    const writer2 = new Database({
      filename: sharedDbPath,
      busyTimeout: 100,
    } as any);

    writer1.exec('BEGIN IMMEDIATE');

    // Writer 2 should timeout
    expect(() => {
      writer2.exec('BEGIN IMMEDIATE');
    }).toThrow(/busy|locked|timeout/i);

    writer1.exec('ROLLBACK');
    writer1.close();
    writer2.close();
  });

  /**
   * GAP: Should support shared-cache mode for concurrent readers
   */
  it.fails('should support shared-cache mode', () => {
    const db1 = new Database({
      filename: sharedDbPath,
      sharedCache: true,
    } as any);
    const db2 = new Database({
      filename: sharedDbPath,
      sharedCache: true,
    } as any);

    // Both should share the same cache
    db1.prepare('SELECT * FROM counter').get();
    db2.prepare('SELECT * FROM counter').get();

    // Changes in db1 should be visible in db2 within same process
    db1.exec('UPDATE counter SET value = 42 WHERE id = 1');
    const value = db2.prepare('SELECT value FROM counter WHERE id = 1').pluck().get();
    expect(value).toBe(42);

    db1.close();
    db2.close();
  });

  /**
   * GAP: Should handle reader during active write transaction
   */
  it.fails('should allow reads during write transactions in WAL mode', async () => {
    // Set up WAL mode database
    const walDbPath = tempFilePath('wal-concurrent.sqlite');
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const setupDb = new BetterSqlite3(walDbPath);
    setupDb.pragma('journal_mode = WAL');
    setupDb.exec('CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)');
    setupDb.exec("INSERT INTO data (value) VALUES ('original')");
    setupDb.close();

    // Writer with transaction
    const writer = new Database({
      filename: walDbPath,
    } as any);
    writer.exec('BEGIN');
    writer.exec("UPDATE data SET value = 'modified'");

    // Reader should see committed data (before transaction commits)
    const reader = new Database({
      filename: walDbPath,
      readonly: true,
    } as any);
    const value = reader.prepare('SELECT value FROM data WHERE id = 1').pluck().get();
    expect(value).toBe('original');

    writer.exec('COMMIT');

    // After commit, reader should see new value
    const newValue = reader.prepare('SELECT value FROM data WHERE id = 1').pluck().get();
    expect(newValue).toBe('modified');

    reader.close();
    writer.close();
  });
});

// =============================================================================
// 5. WAL MODE COMPATIBILITY
// =============================================================================

describe('WAL mode compatibility', () => {
  /**
   * GAP: Should create WAL files when using WAL journal mode
   */
  it.fails('should create -wal and -shm files in WAL mode', () => {
    const walDbPath = tempFilePath('wal-files.sqlite');

    const db = new Database({
      filename: walDbPath,
      journalMode: 'wal',
    } as any);
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');
    db.exec('INSERT INTO test (id) VALUES (1)');

    // WAL and SHM files should exist
    expect(fs.existsSync(`${walDbPath}-wal`)).toBe(true);
    expect(fs.existsSync(`${walDbPath}-shm`)).toBe(true);

    db.close();
  });

  /**
   * GAP: Should support manual checkpoint
   */
  it.fails('should support explicit WAL checkpoint', () => {
    const walDbPath = tempFilePath('wal-checkpoint.sqlite');

    const db = new Database({
      filename: walDbPath,
      journalMode: 'wal',
    } as any);
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)');

    // Insert data (goes to WAL)
    for (let i = 0; i < 100; i++) {
      db.exec(`INSERT INTO test (data) VALUES ('${'x'.repeat(1000)}')`);
    }

    // Checkpoint should move WAL to main database
    (db as unknown as FileBackedDatabase).checkpoint('full');

    // WAL file should be empty or very small after checkpoint
    const walStats = fs.statSync(`${walDbPath}-wal`);
    expect(walStats.size).toBeLessThan(4096); // Less than one page

    db.close();
  });

  /**
   * GAP: Should support TRUNCATE checkpoint mode
   */
  it.fails('should support truncate checkpoint mode', () => {
    const walDbPath = tempFilePath('wal-truncate.sqlite');

    const db = new Database({
      filename: walDbPath,
      journalMode: 'wal',
    } as any);
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');
    db.exec('INSERT INTO test (id) VALUES (1)');

    // Truncate checkpoint should reset WAL file
    (db as unknown as FileBackedDatabase).checkpoint('truncate');

    // WAL file should be truncated to 0
    const walStats = fs.statSync(`${walDbPath}-wal`);
    expect(walStats.size).toBe(0);

    db.close();
  });

  /**
   * GAP: Should support changing journal mode at runtime
   */
  it.fails('should allow changing journal mode', () => {
    const dbPath = tempFilePath('change-journal.sqlite');

    const db = new Database({
      filename: dbPath,
      journalMode: 'delete',
    } as any);
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    // Change to WAL mode
    db.exec('PRAGMA journal_mode = WAL');

    const mode = db.prepare('PRAGMA journal_mode').pluck().get();
    expect(mode).toBe('wal');

    // WAL files should now exist after a write
    db.exec('INSERT INTO test (id) VALUES (1)');
    expect(fs.existsSync(`${dbPath}-wal`)).toBe(true);

    db.close();
  });

  /**
   * GAP: Should report WAL stats
   */
  it.fails('should report WAL statistics', () => {
    const walDbPath = tempFilePath('wal-stats.sqlite');

    const db = new Database({
      filename: walDbPath,
      journalMode: 'wal',
    } as any);
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)');

    // Insert some data
    for (let i = 0; i < 50; i++) {
      db.exec(`INSERT INTO test (data) VALUES ('${'x'.repeat(500)}')`);
    }

    const stats = (db as unknown as FileBackedDatabase).getFileStats();
    expect(stats.isWalMode).toBe(true);
    expect(stats.walCheckpointSize).toBeGreaterThan(0);

    db.close();
  });

  /**
   * GAP: Should clean up WAL files on close (optional)
   */
  it.fails('should clean up WAL files after checkpoint and close', () => {
    const walDbPath = tempFilePath('wal-cleanup.sqlite');

    const db = new Database({
      filename: walDbPath,
      journalMode: 'wal',
    } as any);
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');
    db.exec('INSERT INTO test (id) VALUES (1)');

    // Checkpoint with truncate and close
    (db as unknown as FileBackedDatabase).checkpoint('truncate');
    db.close();

    // WAL file should be empty (or removed in some implementations)
    if (fs.existsSync(`${walDbPath}-wal`)) {
      const walStats = fs.statSync(`${walDbPath}-wal`);
      expect(walStats.size).toBe(0);
    }
  });
});

// =============================================================================
// 6. VACUUM AND OPTIMIZATION
// =============================================================================

describe('Vacuum and optimization', () => {
  /**
   * GAP: Should support VACUUM command
   */
  it.fails('should support vacuum to reclaim space', () => {
    const dbPath = tempFilePath('vacuum-test.sqlite');

    const db = new Database({
      filename: dbPath,
    } as any);
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)');

    // Insert then delete to create free space
    for (let i = 0; i < 1000; i++) {
      db.exec(`INSERT INTO test (data) VALUES ('${'x'.repeat(1000)}')`);
    }
    const sizeBeforeDelete = fs.statSync(dbPath).size;

    db.exec('DELETE FROM test');
    const sizeAfterDelete = fs.statSync(dbPath).size;

    // Size should be same (pages marked free but not reclaimed)
    expect(sizeAfterDelete).toBe(sizeBeforeDelete);

    // Vacuum should reclaim space
    (db as unknown as FileBackedDatabase).vacuum();
    const sizeAfterVacuum = fs.statSync(dbPath).size;

    expect(sizeAfterVacuum).toBeLessThan(sizeBeforeDelete);

    db.close();
  });

  /**
   * GAP: Should support VACUUM INTO
   */
  it.fails('should support vacuum into new file', () => {
    const dbPath = tempFilePath('vacuum-source.sqlite');
    const vacuumPath = tempFilePath('vacuum-dest.sqlite');

    const db = new Database({
      filename: dbPath,
    } as any);
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)');
    for (let i = 0; i < 100; i++) {
      db.exec(`INSERT INTO test (data) VALUES ('test${i}')`);
    }

    // VACUUM INTO creates a new optimized copy
    (db as unknown as FileBackedDatabase).vacuum(vacuumPath);

    expect(fs.existsSync(vacuumPath)).toBe(true);
    expect(isValidSQLiteFile(vacuumPath)).toBe(true);

    // New file should be valid and contain all data
    const destDb = new Database({ filename: vacuumPath } as any);
    const count = destDb.prepare('SELECT COUNT(*) FROM test').pluck().get();
    expect(count).toBe(100);

    db.close();
    destDb.close();
  });
});

// =============================================================================
// 7. SERIALIZATION AND BUFFER OPERATIONS
// =============================================================================

describe('Serialization and buffer operations', () => {
  /**
   * GAP: Should serialize database to buffer
   */
  it.fails('should serialize in-memory database to buffer', () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)');
    db.exec("INSERT INTO test (value) VALUES ('one'), ('two'), ('three')");

    const buffer = (db as unknown as FileBackedDatabase).serialize();

    expect(buffer).toBeInstanceOf(Buffer);
    expect(buffer.length).toBeGreaterThan(0);

    // Buffer should have SQLite magic header
    expect(buffer.toString('utf8', 0, 15)).toBe('SQLite format 3');
  });

  /**
   * GAP: Should deserialize buffer to database
   */
  it.fails('should deserialize buffer to new database', () => {
    // Create and serialize a database
    const originalDb = new Database();
    originalDb.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    originalDb.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')");
    const buffer = (originalDb as unknown as FileBackedDatabase).serialize();
    originalDb.close();

    // Deserialize to new database
    const FileBackedDatabase = Database as unknown as typeof FileBackedDatabase;
    const restoredDb = FileBackedDatabase.deserialize(buffer);

    const users = restoredDb.prepare('SELECT name FROM users ORDER BY id').pluck().all();
    expect(users).toEqual(['Alice', 'Bob']);

    restoredDb.close();
  });

  /**
   * GAP: Should preserve all data through serialize/deserialize cycle
   */
  it.fails('should preserve complex data through serialization', () => {
    const originalDb = new Database();
    originalDb.exec(`
      CREATE TABLE complex (
        id INTEGER PRIMARY KEY,
        int_val INTEGER,
        real_val REAL,
        text_val TEXT,
        blob_val BLOB
      )
    `);

    const blobData = Buffer.from([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd]);
    const insert = originalDb.prepare(
      'INSERT INTO complex (int_val, real_val, text_val, blob_val) VALUES (?, ?, ?, ?)'
    );
    insert.run(42, 3.14159, 'Hello, World!', blobData);
    insert.run(null, null, null, null);
    insert.run(-123456789, -0.000001, 'Unicode: ', Buffer.alloc(0));

    const buffer = (originalDb as unknown as FileBackedDatabase).serialize();

    const FileBackedDatabase = Database as unknown as typeof FileBackedDatabase;
    const restoredDb = FileBackedDatabase.deserialize(buffer);

    const rows = restoredDb.prepare('SELECT * FROM complex ORDER BY id').all();
    expect(rows).toHaveLength(3);

    expect((rows[0] as any).int_val).toBe(42);
    expect((rows[0] as any).real_val).toBeCloseTo(3.14159, 5);
    expect((rows[0] as any).text_val).toBe('Hello, World!');
    expect(Buffer.compare((rows[0] as any).blob_val, blobData)).toBe(0);

    expect((rows[1] as any).int_val).toBeNull();
    expect((rows[1] as any).text_val).toBeNull();

    originalDb.close();
    restoredDb.close();
  });
});

// =============================================================================
// 8. ERROR HANDLING
// =============================================================================

describe('File operation error handling', () => {
  /**
   * GAP: Should throw descriptive error for non-existent file
   */
  it.fails('should throw error when file does not exist and create is false', () => {
    expect(() => {
      new Database({
        filename: '/nonexistent/path/database.sqlite',
        create: false,
      } as any);
    }).toThrow(/not found|does not exist|ENOENT/i);
  });

  /**
   * GAP: Should throw error for invalid SQLite file
   */
  it.fails('should throw error for invalid SQLite file', () => {
    const invalidPath = tempFilePath('invalid.sqlite');
    fs.writeFileSync(invalidPath, 'This is not a SQLite database');

    expect(() => {
      new Database({ filename: invalidPath } as any);
    }).toThrow(/not a database|invalid|corrupt/i);
  });

  /**
   * GAP: Should throw error for permission denied
   */
  it.fails('should throw error when file is not readable', () => {
    const protectedPath = tempFilePath('protected.sqlite');
    fs.writeFileSync(protectedPath, '');
    fs.chmodSync(protectedPath, 0o000);

    expect(() => {
      new Database({ filename: protectedPath } as any);
    }).toThrow(/permission|access|EACCES/i);

    // Cleanup
    fs.chmodSync(protectedPath, 0o644);
  });

  /**
   * GAP: Should throw error for export to non-writable location
   */
  it.fails('should throw error when export destination is not writable', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    await expect(
      (db as unknown as FileBackedDatabase).exportTo({
        path: '/nonexistent/directory/database.sqlite',
        overwrite: true,
      })
    ).rejects.toThrow(/permission|access|ENOENT/i);
  });

  /**
   * GAP: Should throw error when file already exists without overwrite
   */
  it.fails('should throw error when export file exists without overwrite', async () => {
    const existingPath = tempFilePath('existing.sqlite');
    fs.writeFileSync(existingPath, 'existing content');

    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    await expect(
      (db as unknown as FileBackedDatabase).exportTo({
        path: existingPath,
        overwrite: false,
      })
    ).rejects.toThrow(/exists|overwrite/i);
  });
});
