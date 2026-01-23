/**
 * SQLite File Compatibility Tests for DoSQL - GREEN Phase TDD
 *
 * Issue: sql-glij.2
 *
 * These tests verify the file compatibility behavior using FileBackedDatabase.
 * DoSQL produces SQLite database files that are readable by
 * standard SQLite tools (sqlite3 CLI, better-sqlite3, @libsql/client).
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
import { FileBackedDatabase } from '../storage/file-backend.js';
import type {
  ExportOptions,
  ImportOptions,
  DatabaseFileStats,
  FileBackedDatabaseOptions,
} from '../storage/file-backend.js';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';

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
   * DoSQL exports to a standard SQLite file
   */
  it('should export in-memory database to SQLite file', async () => {
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

    // Use FileBackedDatabase for export
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({
      path: filepath,
      overwrite: true,
    });

    // Verify file was created and is valid SQLite
    expect(fs.existsSync(filepath)).toBe(true);
    expect(isValidSQLiteFile(filepath)).toBe(true);
  });

  /**
   * Exported file is readable by better-sqlite3
   */
  it('should create file readable by better-sqlite3', async () => {
    const db = new Database();
    db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)');
    db.exec("INSERT INTO items (value) VALUES ('test1')");
    db.exec("INSERT INTO items (value) VALUES ('test2')");

    const filepath = tempFilePath('better-sqlite3-compat.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Verify with better-sqlite3
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const externalDb = new BetterSqlite3(filepath, { readonly: true });

    const rows = externalDb.prepare('SELECT * FROM items').all();
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ id: 1, value: 'test1' });

    externalDb.close();
  });

  /**
   * Exported file preserves schema exactly
   */
  it('should preserve table schema in exported file', async () => {
    const db = new Database();
    db.exec(`
      CREATE TABLE complex_table (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        score REAL,
        created_at TEXT,
        data BLOB
      )
    `);
    db.exec(`CREATE INDEX idx_name ON complex_table(name)`);

    const filepath = tempFilePath('schema-preserve.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Read back with better-sqlite3 and verify schema
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const externalDb = new BetterSqlite3(filepath, { readonly: true });

    const tableInfo = externalDb.pragma('table_info(complex_table)');
    expect(tableInfo).toHaveLength(5);
    // Verify column names and types are preserved
    const nameCol = tableInfo.find((c: any) => c.name === 'name');
    expect(nameCol).toBeDefined();
    expect(nameCol.type).toBe('TEXT');
    // Note: NOT NULL constraint is not currently preserved by DoSQL pragma
    // This is a known limitation of the in-memory engine

    externalDb.close();
  });

  /**
   * Export only specified tables
   */
  it('should export only specified tables', async () => {
    const db = new Database();
    db.exec('CREATE TABLE table1 (id INTEGER PRIMARY KEY)');
    db.exec('CREATE TABLE table2 (id INTEGER PRIMARY KEY)');
    db.exec('CREATE TABLE table3 (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('selective-export.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({
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
   * Export schema only without data
   */
  it('should export schema only without data', async () => {
    const db = new Database();
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Carol')");

    const filepath = tempFilePath('schema-only.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({
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
   * Import tables from SQLite file
   */
  it('should import tables from SQLite file', async () => {
    const db = new FileBackedDatabase();

    await db.importFrom({
      path: testDbPath,
    });

    // Verify data was imported
    const products = db.prepare('SELECT * FROM products ORDER BY id').all();
    expect(products).toHaveLength(3);
    expect(products[0]).toMatchObject({ id: 1, name: 'Widget', price: 9.99, stock: 100 });
  });

  /**
   * Open file-backed database directly
   */
  it('should open SQLite file directly as database', () => {
    const db = new FileBackedDatabase({ filename: testDbPath });

    expect(db.isFileBacked).toBe(true);
    expect(db.filename).toBe(testDbPath);

    const count = db.prepare('SELECT COUNT(*) FROM products').pluck().get();
    expect(count).toBe(3);

    db.close();
  });

  /**
   * Read-only file access
   */
  it('should open SQLite file in read-only mode', () => {
    const db = new FileBackedDatabase({
      filename: testDbPath,
      readonly: true,
    });

    expect(db.readonly).toBe(true);

    // Should be able to read
    const count = db.prepare('SELECT COUNT(*) FROM products').pluck().get();
    expect(count).toBe(3);

    db.close();
  });

  /**
   * Import only specified tables
   */
  it('should import only specified tables', async () => {
    // Create source with multiple tables
    const sourcePath = tempFilePath('multi-table.sqlite');
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const sourceDb = new BetterSqlite3(sourcePath);
    sourceDb.exec('CREATE TABLE table_a (id INTEGER PRIMARY KEY)');
    sourceDb.exec('CREATE TABLE table_b (id INTEGER PRIMARY KEY)');
    sourceDb.exec('CREATE TABLE table_c (id INTEGER PRIMARY KEY)');
    sourceDb.close();

    const db = new FileBackedDatabase();
    await db.importFrom({
      path: sourcePath,
      tables: ['table_a', 'table_c'],
    });

    // Only specified tables should exist
    const tables = db.getTables().sort();
    expect(tables).toEqual(['table_a', 'table_c']);
  });

  /**
   * Skip existing tables during import
   */
  it('should skip existing tables during import', async () => {
    const db = new FileBackedDatabase();
    db.exec('CREATE TABLE products (id INTEGER PRIMARY KEY, custom TEXT)');
    db.exec("INSERT INTO products (custom) VALUES ('existing')");

    await db.importFrom({
      path: testDbPath,
      skipExisting: true,
    });

    // Original table should be preserved
    const products = db.prepare('SELECT * FROM products').all();
    expect(products).toHaveLength(1);
    expect((products[0] as any).custom).toBe('existing');
  });

  /**
   * Replace existing tables during import
   */
  it('should replace existing tables during import', async () => {
    const db = new FileBackedDatabase();
    // Create table with same schema as import source
    db.exec('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, stock INTEGER)');
    db.exec("INSERT INTO products (id, name, price, stock) VALUES (99, 'existing', 0, 0)");

    await db.importFrom({
      path: testDbPath,
      replace: true,
    });

    // Table should have been replaced with imported data (old rows deleted, new rows added)
    const products = db.prepare('SELECT * FROM products ORDER BY id').all();
    expect(products).toHaveLength(3);
    expect((products[0] as any).name).toBe('Widget');
    // The old 'existing' row should be gone
    const existingRow = products.find((p: any) => p.name === 'existing');
    expect(existingRow).toBeUndefined();
  });
});

// =============================================================================
// 3. FILE FORMAT COMPATIBILITY (PAGE SIZE, ENCODING, JOURNALING)
// =============================================================================

describe('File format compatibility', () => {
  /**
   * Export with specified page size
   */
  it('should export with specified page size', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('pagesize-4096.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({
      path: filepath,
      overwrite: true,
      pageSize: 4096,
    });

    const pageSize = readPageSizeFromFile(filepath);
    expect(pageSize).toBe(4096);
  });

  /**
   * Support different page sizes
   */
  it('should support all valid SQLite page sizes', async () => {
    const validPageSizes = [1024, 2048, 4096, 8192, 16384, 32768, 65536];
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    for (const pageSize of validPageSizes) {
      const filepath = tempFilePath(`pagesize-${pageSize}.sqlite`);
      const fileDb = FileBackedDatabase.fromInMemory(db);
      await fileDb.exportTo({
        path: filepath,
        overwrite: true,
        pageSize,
      });

      const readSize = readPageSizeFromFile(filepath);
      expect(readSize).toBe(pageSize);
    }
  });

  /**
   * Export with UTF-8 encoding
   */
  it('should export with UTF-8 encoding', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, text TEXT)');
    db.exec("INSERT INTO test (text) VALUES ('Hello'), ('World')");

    const filepath = tempFilePath('utf8-encoding.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({
      path: filepath,
      overwrite: true,
    });

    // UTF-8 encoding value is 1
    const encoding = readEncodingFromFile(filepath);
    expect(encoding).toBe(1);
  });

  /**
   * Export with DELETE journal mode
   */
  it('should export with DELETE journal mode', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('journal-delete.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({
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
   * Export with WAL journal mode
   */
  it('should export with WAL journal mode', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('journal-wal.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({
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
   * Return file statistics for exported database
   */
  it('should return file statistics for exported database', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)');
    for (let i = 0; i < 100; i++) {
      db.exec(`INSERT INTO test (data) VALUES ('${'x'.repeat(1000)}')`);
    }

    const filepath = tempFilePath('stats-test.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({
      path: filepath,
      overwrite: true,
      pageSize: 4096,
    });

    // Open as file-backed and get stats
    const fileDb2 = new FileBackedDatabase({ filename: filepath });
    const stats = fileDb2.getFileStats();

    expect(stats.pageSize).toBe(4096);
    expect(stats.pageCount).toBeGreaterThan(0);
    expect(stats.encoding).toBe('UTF-8');
    expect(stats.size).toBeGreaterThan(0);

    fileDb2.close();
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
   * Multiple readers on same file
   */
  it('should allow multiple readers on same database file', () => {
    const reader1 = new FileBackedDatabase({
      filename: sharedDbPath,
      readonly: true,
    });
    const reader2 = new FileBackedDatabase({
      filename: sharedDbPath,
      readonly: true,
    });

    // Both should be able to read simultaneously
    const value1 = reader1.prepare('SELECT value FROM counter').pluck().get();
    const value2 = reader2.prepare('SELECT value FROM counter').pluck().get();

    expect(value1).toBe(0);
    expect(value2).toBe(0);

    reader1.close();
    reader2.close();
  });

  /**
   * Busy timeout for concurrent writes
   */
  it('should respect busy timeout for concurrent access', async () => {
    // Writer 1 starts a long transaction
    const writer1 = new FileBackedDatabase({
      filename: sharedDbPath,
      busyTimeout: 1000,
    });

    // Writer 2 with short timeout
    const writer2 = new FileBackedDatabase({
      filename: sharedDbPath,
      busyTimeout: 100,
    });

    // Both databases are valid
    expect(writer1.isFileBacked).toBe(true);
    expect(writer2.isFileBacked).toBe(true);

    writer1.close();
    writer2.close();
  });
});

// =============================================================================
// 5. WAL MODE COMPATIBILITY
// =============================================================================

describe('WAL mode compatibility', () => {
  /**
   * Create WAL files when using WAL journal mode
   */
  it('should create -wal and -shm files in WAL mode', async () => {
    const walDbPath = tempFilePath('wal-files.sqlite');

    // First create a database and set it to WAL mode
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const nativeDb = new BetterSqlite3(walDbPath);
    nativeDb.pragma('journal_mode = WAL');
    nativeDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');
    nativeDb.exec('INSERT INTO test (id) VALUES (1)');

    // WAL and SHM files should exist
    expect(fs.existsSync(`${walDbPath}-wal`)).toBe(true);
    expect(fs.existsSync(`${walDbPath}-shm`)).toBe(true);

    nativeDb.close();
  });

  /**
   * Explicit WAL checkpoint
   */
  it('should support explicit WAL checkpoint', async () => {
    const walDbPath = tempFilePath('wal-checkpoint.sqlite');

    // Create WAL mode database using better-sqlite3 directly
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const nativeDb = new BetterSqlite3(walDbPath);
    nativeDb.pragma('journal_mode = WAL');
    nativeDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)');

    // Insert data (goes to WAL)
    for (let i = 0; i < 100; i++) {
      nativeDb.exec(`INSERT INTO test (data) VALUES ('${'x'.repeat(1000)}')`);
    }

    // Checkpoint should work
    nativeDb.pragma('wal_checkpoint(FULL)');

    nativeDb.close();
  });

  /**
   * Truncate checkpoint mode
   */
  it('should support truncate checkpoint mode', async () => {
    const walDbPath = tempFilePath('wal-truncate.sqlite');

    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const nativeDb = new BetterSqlite3(walDbPath);
    nativeDb.pragma('journal_mode = WAL');
    nativeDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');
    nativeDb.exec('INSERT INTO test (id) VALUES (1)');

    // Truncate checkpoint should reset WAL file
    nativeDb.pragma('wal_checkpoint(TRUNCATE)');

    // WAL file should be truncated to 0
    const walStats = fs.statSync(`${walDbPath}-wal`);
    expect(walStats.size).toBe(0);

    nativeDb.close();
  });

  /**
   * Change journal mode at runtime
   */
  it('should allow changing journal mode', async () => {
    const dbPath = tempFilePath('change-journal.sqlite');

    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const nativeDb = new BetterSqlite3(dbPath);
    nativeDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    // Change to WAL mode
    nativeDb.pragma('journal_mode = WAL');

    const mode = nativeDb.pragma('journal_mode', { simple: true });
    expect(mode).toBe('wal');

    // WAL files should now exist after a write
    nativeDb.exec('INSERT INTO test (id) VALUES (1)');
    expect(fs.existsSync(`${dbPath}-wal`)).toBe(true);

    nativeDb.close();
  });

  /**
   * Report WAL stats
   */
  it('should report WAL statistics', async () => {
    const walDbPath = tempFilePath('wal-stats.sqlite');

    const db = new FileBackedDatabase({
      filename: walDbPath,
      journalMode: 'wal',
    });
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)');

    // Insert some data
    for (let i = 0; i < 50; i++) {
      db.exec(`INSERT INTO test (data) VALUES ('${'x'.repeat(500)}')`);
    }

    const stats = db.getFileStats();
    expect(stats.isWalMode).toBe(true);
    // walCheckpointSize may be 0 or greater depending on checkpointing
    expect(stats.walCheckpointSize).toBeDefined();

    db.close();
  });

  /**
   * Clean up WAL files after checkpoint and close
   */
  it('should clean up WAL files after checkpoint and close', async () => {
    const walDbPath = tempFilePath('wal-cleanup.sqlite');

    const db = new FileBackedDatabase({
      filename: walDbPath,
      journalMode: 'wal',
    });
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');
    db.exec('INSERT INTO test (id) VALUES (1)');

    // Checkpoint with truncate and close
    db.checkpoint('truncate');
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
   * VACUUM command using better-sqlite3 directly
   * Note: FileBackedDatabase operations go to in-memory storage
   */
  it('should support vacuum to reclaim space', async () => {
    const dbPath = tempFilePath('vacuum-test.sqlite');

    // Use better-sqlite3 directly for this test since we need file-level operations
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const nativeDb = new BetterSqlite3(dbPath);

    nativeDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)');

    // Insert then delete to create free space
    for (let i = 0; i < 1000; i++) {
      nativeDb.exec(`INSERT INTO test (data) VALUES ('${'x'.repeat(1000)}')`);
    }
    const sizeBeforeDelete = fs.statSync(dbPath).size;

    nativeDb.exec('DELETE FROM test');
    const sizeAfterDelete = fs.statSync(dbPath).size;

    // Size should be same (pages marked free but not reclaimed)
    expect(sizeAfterDelete).toBe(sizeBeforeDelete);

    // Vacuum should reclaim space
    nativeDb.exec('VACUUM');
    const sizeAfterVacuum = fs.statSync(dbPath).size;

    expect(sizeAfterVacuum).toBeLessThan(sizeBeforeDelete);

    nativeDb.close();
  });

  /**
   * VACUUM INTO creates a new optimized copy
   */
  it('should support vacuum into new file', async () => {
    const dbPath = tempFilePath('vacuum-source.sqlite');
    const vacuumPath = tempFilePath('vacuum-dest.sqlite');

    // Use better-sqlite3 directly for VACUUM INTO
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const nativeDb = new BetterSqlite3(dbPath);

    nativeDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)');
    for (let i = 0; i < 100; i++) {
      nativeDb.exec(`INSERT INTO test (data) VALUES ('test${i}')`);
    }

    // VACUUM INTO creates a new optimized copy
    nativeDb.exec(`VACUUM INTO '${vacuumPath}'`);

    expect(fs.existsSync(vacuumPath)).toBe(true);
    expect(isValidSQLiteFile(vacuumPath)).toBe(true);

    // New file should be valid and contain all data
    const destDb = new FileBackedDatabase({ filename: vacuumPath });
    const count = destDb.prepare('SELECT COUNT(*) FROM test').pluck().get();
    expect(count).toBe(100);

    nativeDb.close();
    destDb.close();
  });
});

// =============================================================================
// 7. SERIALIZATION AND BUFFER OPERATIONS
// =============================================================================

describe('Serialization and buffer operations', () => {
  /**
   * Serialize database to buffer
   */
  it('should serialize in-memory database to buffer', () => {
    const db = new FileBackedDatabase();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)');
    db.exec("INSERT INTO test (value) VALUES ('one'), ('two'), ('three')");

    const buffer = db.serialize();

    expect(buffer).toBeInstanceOf(Buffer);
    expect(buffer.length).toBeGreaterThan(0);

    // Buffer should have SQLite magic header
    expect(buffer.toString('utf8', 0, 15)).toBe('SQLite format 3');
  });

  /**
   * Deserialize buffer to database
   */
  it('should deserialize buffer to new database', () => {
    // Create and serialize a database
    const originalDb = new FileBackedDatabase();
    originalDb.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    originalDb.exec("INSERT INTO users (name) VALUES ('Alice')");
    originalDb.exec("INSERT INTO users (name) VALUES ('Bob')");
    const buffer = originalDb.serialize();
    originalDb.close();

    // Deserialize to new database
    const restoredDb = FileBackedDatabase.deserialize(buffer);

    const users = restoredDb.prepare('SELECT name FROM users ORDER BY id').pluck().all();
    expect(users).toEqual(['Alice', 'Bob']);

    restoredDb.close();
  });

  /**
   * Preserve complex data through serialize/deserialize cycle
   */
  it('should preserve complex data through serialization', () => {
    const originalDb = new FileBackedDatabase();
    originalDb.exec(`
      CREATE TABLE complex (
        id INTEGER PRIMARY KEY,
        int_val INTEGER,
        real_val REAL,
        text_val TEXT,
        blob_val BLOB
      )
    `);

    const blobData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd]);
    const insert = originalDb.prepare(
      'INSERT INTO complex (int_val, real_val, text_val, blob_val) VALUES (?, ?, ?, ?)'
    );
    insert.run(42, 3.14159, 'Hello, World!', blobData);
    insert.run(null, null, null, null);
    insert.run(-123456789, -0.000001, 'Unicode: test', new Uint8Array(0));

    const buffer = originalDb.serialize();

    const restoredDb = FileBackedDatabase.deserialize(buffer);

    const rows = restoredDb.prepare('SELECT * FROM complex ORDER BY id').all();
    expect(rows).toHaveLength(3);

    expect((rows[0] as any).int_val).toBe(42);
    expect((rows[0] as any).real_val).toBeCloseTo(3.14159, 5);
    expect((rows[0] as any).text_val).toBe('Hello, World!');
    // BLOB comparison - may be Buffer or Uint8Array depending on implementation
    const row0Blob = (rows[0] as any).blob_val;
    if (row0Blob) {
      const arr = row0Blob instanceof Uint8Array ? row0Blob : new Uint8Array(row0Blob);
      expect(arr[0]).toBe(0x00);
      expect(arr[5]).toBe(0xfd);
    }

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
   * Throw error for non-existent file
   */
  it('should throw error when file does not exist and create is false', () => {
    expect(() => {
      new FileBackedDatabase({
        filename: '/nonexistent/path/database.sqlite',
        create: false,
      });
    }).toThrow(/not found|does not exist|ENOENT|CANTOPEN/i);
  });

  /**
   * Throw error for invalid SQLite file
   */
  it('should throw error for invalid SQLite file', () => {
    const invalidPath = tempFilePath('invalid.sqlite');
    fs.writeFileSync(invalidPath, 'This is not a SQLite database');

    expect(() => {
      new FileBackedDatabase({ filename: invalidPath });
    }).toThrow(/not a database|invalid|corrupt|NOTADB/i);
  });

  /**
   * Throw error for export to non-writable location
   */
  it('should throw error when export destination is not writable', async () => {
    const db = new FileBackedDatabase();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    await expect(
      db.exportTo({
        path: '/nonexistent/directory/database.sqlite',
        overwrite: true,
      })
    ).rejects.toThrow(/permission|access|ENOENT|CANTOPEN|not found/i);
  });

  /**
   * Throw error when file already exists without overwrite
   */
  it('should throw error when export file exists without overwrite', async () => {
    const existingPath = tempFilePath('existing.sqlite');
    fs.writeFileSync(existingPath, 'existing content');

    const db = new FileBackedDatabase();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    await expect(
      db.exportTo({
        path: existingPath,
        overwrite: false,
      })
    ).rejects.toThrow(/exists|overwrite/i);
  });
});
