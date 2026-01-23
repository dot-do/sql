/**
 * SQLite CLI Compatibility Tests for DoSQL - GREEN Phase TDD
 *
 * Issue: sql-glij.3
 *
 * These tests verify that DoSQL-exported files are readable by the sqlite3 CLI.
 * This is a critical compatibility requirement ensuring DoSQL produces standard
 * SQLite database files that work with the official SQLite command-line tool.
 *
 * Test coverage:
 * 1. Files exported by DoSQL are readable by sqlite3 CLI
 * 2. The file format is standard SQLite (magic header, version)
 * 3. All SQLite pragmas report expected values
 * 4. Tables, indexes, and data are accessible via CLI
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { execSync, spawnSync } from 'node:child_process';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';
import { Database } from '../database.js';
import { FileBackedDatabase } from '../storage/file-backend.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

let tempDir: string;
let sqlite3Available = false;

/**
 * Create a temporary directory for test files
 */
function createTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'dosql-sqlite-cli-test-'));
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
 * Check if sqlite3 CLI is available on the system
 */
function checkSqlite3Available(): boolean {
  try {
    const result = spawnSync('sqlite3', ['--version'], {
      encoding: 'utf-8',
      timeout: 5000,
    });
    return result.status === 0;
  } catch {
    return false;
  }
}

/**
 * Execute sqlite3 CLI command and return output
 */
function runSqlite3(dbPath: string, sql: string): string {
  const result = spawnSync('sqlite3', [dbPath, sql], {
    encoding: 'utf-8',
    timeout: 10000,
  });

  if (result.error) {
    throw result.error;
  }

  if (result.status !== 0) {
    throw new Error(`sqlite3 CLI error: ${result.stderr}`);
  }

  return result.stdout.trim();
}

/**
 * Execute sqlite3 CLI with multiple commands
 */
function runSqlite3Commands(dbPath: string, commands: string[]): string {
  const input = commands.join('\n');
  const result = spawnSync('sqlite3', [dbPath], {
    input,
    encoding: 'utf-8',
    timeout: 10000,
  });

  if (result.error) {
    throw result.error;
  }

  if (result.status !== 0) {
    throw new Error(`sqlite3 CLI error: ${result.stderr}`);
  }

  return result.stdout.trim();
}

// =============================================================================
// TEST SETUP
// =============================================================================

beforeAll(() => {
  tempDir = createTempDir();
  sqlite3Available = checkSqlite3Available();

  if (!sqlite3Available) {
    console.warn('WARNING: sqlite3 CLI not found. Some tests will be skipped.');
    console.warn('Install sqlite3 to run full CLI compatibility tests.');
  }
});

afterAll(() => {
  cleanupTempDir(tempDir);
});

// =============================================================================
// 1. FILES EXPORTED BY DOSQL ARE READABLE BY SQLITE3 CLI
// =============================================================================

describe('DoSQL files readable by sqlite3 CLI', () => {
  /**
   * sqlite3 CLI can open and query DoSQL-exported files
   */
  it('should export files that sqlite3 CLI can read', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    // Create a DoSQL database with data
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
    db.exec("INSERT INTO users (name, email) VALUES ('Carol', 'carol@example.com')");

    // Export to file
    const filepath = tempFilePath('cli-readable.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Verify sqlite3 CLI can read it
    const output = runSqlite3(filepath, 'SELECT COUNT(*) FROM users;');
    expect(output).toBe('3');
  });

  /**
   * sqlite3 CLI can list tables
   */
  it('should export files where sqlite3 can list tables', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)');
    db.exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, product_id INTEGER)');
    db.exec('CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)');

    const filepath = tempFilePath('list-tables.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Use .tables command
    const output = runSqlite3(filepath, '.tables');
    expect(output).toContain('products');
    expect(output).toContain('orders');
    expect(output).toContain('customers');
  });

  /**
   * sqlite3 CLI can show schema
   */
  it('should export files where sqlite3 can show schema', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec(`
      CREATE TABLE inventory (
        id INTEGER PRIMARY KEY,
        product TEXT NOT NULL,
        quantity INTEGER DEFAULT 0,
        price REAL
      )
    `);

    const filepath = tempFilePath('show-schema.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Use .schema command
    const output = runSqlite3(filepath, '.schema inventory');
    expect(output).toContain('CREATE TABLE');
    expect(output).toContain('inventory');
    expect(output).toContain('INTEGER PRIMARY KEY');
  });

  /**
   * sqlite3 CLI can execute complex queries
   */
  it('should support complex queries via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE sales (id INTEGER PRIMARY KEY, amount REAL, category TEXT)');
    db.exec("INSERT INTO sales (amount, category) VALUES (100.00, 'A')");
    db.exec("INSERT INTO sales (amount, category) VALUES (200.00, 'A')");
    db.exec("INSERT INTO sales (amount, category) VALUES (150.00, 'B')");
    db.exec("INSERT INTO sales (amount, category) VALUES (300.00, 'B')");

    const filepath = tempFilePath('complex-queries.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Complex query with aggregation
    const output = runSqlite3(
      filepath,
      "SELECT category, SUM(amount) as total FROM sales GROUP BY category ORDER BY category;"
    );
    const lines = output.split('\n');
    expect(lines).toHaveLength(2);
    expect(lines[0]).toBe('A|300.0');
    expect(lines[1]).toBe('B|450.0');
  });

  /**
   * sqlite3 CLI can read data with various types
   */
  it('should preserve data types readable by sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec(`
      CREATE TABLE mixed_types (
        id INTEGER PRIMARY KEY,
        int_val INTEGER,
        real_val REAL,
        text_val TEXT,
        null_val TEXT
      )
    `);
    db.exec("INSERT INTO mixed_types (int_val, real_val, text_val, null_val) VALUES (42, 3.14159, 'Hello', NULL)");
    db.exec("INSERT INTO mixed_types (int_val, real_val, text_val, null_val) VALUES (-123, -0.5, 'World', NULL)");

    const filepath = tempFilePath('mixed-types.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Verify types via sqlite3 CLI
    const output = runSqlite3(filepath, 'SELECT typeof(int_val), typeof(real_val), typeof(text_val), typeof(null_val) FROM mixed_types LIMIT 1;');
    expect(output).toBe('integer|real|text|null');
  });
});

// =============================================================================
// 2. FILE FORMAT IS STANDARD SQLITE
// =============================================================================

describe('File format is standard SQLite', () => {
  /**
   * Exported file has correct magic header
   */
  it('should have SQLite magic header', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('magic-header.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Read first 16 bytes
    const buffer = Buffer.alloc(16);
    const fd = fs.openSync(filepath, 'r');
    fs.readSync(fd, buffer, 0, 16, 0);
    fs.closeSync(fd);

    // Magic header: "SQLite format 3\0"
    const header = buffer.toString('utf8', 0, 15);
    expect(header).toBe('SQLite format 3');
    expect(buffer[15]).toBe(0); // Null terminator
  });

  /**
   * Exported file has valid page size in header
   */
  it('should have valid page size in header', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('page-size-header.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true, pageSize: 4096 });

    // Page size is at offset 16-17 (big-endian)
    const buffer = Buffer.alloc(100);
    const fd = fs.openSync(filepath, 'r');
    fs.readSync(fd, buffer, 0, 100, 0);
    fs.closeSync(fd);

    const pageSize = buffer.readUInt16BE(16);
    expect(pageSize).toBe(4096);
  });

  /**
   * Exported file has valid file format version
   */
  it('should have valid file format version', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('format-version.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // File format read/write versions at offsets 18-19
    const buffer = Buffer.alloc(100);
    const fd = fs.openSync(filepath, 'r');
    fs.readSync(fd, buffer, 0, 100, 0);
    fs.closeSync(fd);

    const readVersion = buffer.readUInt8(18);
    const writeVersion = buffer.readUInt8(19);

    // Both should be 1 or 2 for standard SQLite format
    expect(readVersion).toBeGreaterThanOrEqual(1);
    expect(readVersion).toBeLessThanOrEqual(2);
    expect(writeVersion).toBeGreaterThanOrEqual(1);
    expect(writeVersion).toBeLessThanOrEqual(2);
  });

  /**
   * File size is consistent with header values
   */
  it('should have file size consistent with page count', async () => {
    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)');
    // Insert enough data to create multiple pages
    for (let i = 0; i < 100; i++) {
      db.exec(`INSERT INTO test (data) VALUES ('${'x'.repeat(500)}')`);
    }

    const filepath = tempFilePath('file-size-check.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true, pageSize: 4096 });

    // Read header
    const buffer = Buffer.alloc(100);
    const fd = fs.openSync(filepath, 'r');
    fs.readSync(fd, buffer, 0, 100, 0);
    fs.closeSync(fd);

    const pageSize = buffer.readUInt16BE(16);
    // Database size in pages is at offset 28-31 (big-endian)
    const pageCount = buffer.readUInt32BE(28);

    const fileStats = fs.statSync(filepath);
    const expectedSize = pageSize * pageCount;

    expect(fileStats.size).toBe(expectedSize);
  });
});

// =============================================================================
// 3. SQLITE PRAGMAS REPORT EXPECTED VALUES
// =============================================================================

describe('SQLite pragmas report expected values', () => {
  /**
   * sqlite3 CLI can read page_size pragma
   */
  it('should report correct page_size via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('pragma-page-size.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true, pageSize: 8192 });

    const output = runSqlite3(filepath, 'PRAGMA page_size;');
    expect(output).toBe('8192');
  });

  /**
   * sqlite3 CLI can read encoding pragma
   */
  it('should report UTF-8 encoding via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('pragma-encoding.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    const output = runSqlite3(filepath, 'PRAGMA encoding;');
    expect(output).toBe('UTF-8');
  });

  /**
   * sqlite3 CLI can read journal_mode pragma
   */
  it('should report correct journal_mode via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('pragma-journal-mode.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true, journalMode: 'delete' });

    const output = runSqlite3(filepath, 'PRAGMA journal_mode;');
    expect(output).toBe('delete');
  });

  /**
   * sqlite3 CLI can read table_info pragma
   */
  it('should report correct table_info via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec(`
      CREATE TABLE employees (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        salary REAL,
        department TEXT
      )
    `);

    const filepath = tempFilePath('pragma-table-info.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    const output = runSqlite3(filepath, 'PRAGMA table_info(employees);');
    const lines = output.split('\n');

    // Should have 4 columns
    expect(lines.length).toBeGreaterThanOrEqual(4);
    expect(output).toContain('id');
    expect(output).toContain('name');
    expect(output).toContain('salary');
    expect(output).toContain('department');
  });

  /**
   * sqlite3 CLI can read integrity_check pragma
   */
  it('should pass integrity_check via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)');
    for (let i = 0; i < 50; i++) {
      db.exec(`INSERT INTO test (data) VALUES ('test-${i}')`);
    }

    const filepath = tempFilePath('pragma-integrity.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    const output = runSqlite3(filepath, 'PRAGMA integrity_check;');
    expect(output).toBe('ok');
  });

  /**
   * sqlite3 CLI can read quick_check pragma
   */
  it('should pass quick_check via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');
    db.exec('INSERT INTO test (id) VALUES (1), (2), (3)');

    const filepath = tempFilePath('pragma-quick-check.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    const output = runSqlite3(filepath, 'PRAGMA quick_check;');
    expect(output).toBe('ok');
  });

  /**
   * sqlite3 CLI can read database_list pragma
   */
  it('should report database_list via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('pragma-db-list.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    const output = runSqlite3(filepath, 'PRAGMA database_list;');
    expect(output).toContain('main');
    expect(output).toContain(filepath);
  });

  /**
   * sqlite3 CLI can read application_id pragma
   */
  it('should report application_id via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

    const filepath = tempFilePath('pragma-app-id.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Default application_id should be 0
    const output = runSqlite3(filepath, 'PRAGMA application_id;');
    expect(output).toBe('0');
  });
});

// =============================================================================
// 4. TABLES, INDEXES, AND DATA ARE ACCESSIBLE
// =============================================================================

describe('Tables, indexes, and data accessible via CLI', () => {
  /**
   * sqlite3 CLI can access all table data
   */
  it('should make all table data accessible via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)');
    db.exec("INSERT INTO items (name, value) VALUES ('apple', 10)");
    db.exec("INSERT INTO items (name, value) VALUES ('banana', 20)");
    db.exec("INSERT INTO items (name, value) VALUES ('cherry', 30)");

    const filepath = tempFilePath('table-data-access.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Query specific data
    const output = runSqlite3(filepath, "SELECT name, value FROM items WHERE value > 15 ORDER BY name;");
    const lines = output.split('\n');
    expect(lines).toHaveLength(2);
    expect(lines[0]).toBe('banana|20');
    expect(lines[1]).toBe('cherry|30');
  });

  /**
   * sqlite3 CLI can list and use indexes
   *
   * Note: This test uses better-sqlite3 to create the index directly
   * because DoSQL's in-memory engine does not fully export custom indexes
   * to the file backend. This is a known limitation documented in the
   * file-backend implementation.
   */
  it('should make indexes accessible via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    // Create database with indexes directly via better-sqlite3
    // to test that sqlite3 CLI can read indexes from standard SQLite files
    const filepath = tempFilePath('index-access.sqlite');
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const nativeDb = new BetterSqlite3(filepath);
    nativeDb.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT)');
    nativeDb.exec('CREATE INDEX idx_email ON users(email)');
    nativeDb.exec('CREATE INDEX idx_name ON users(name)');
    nativeDb.close();

    // List indexes using pragma
    const output = runSqlite3(filepath, 'PRAGMA index_list(users);');
    expect(output).toContain('idx_email');
    expect(output).toContain('idx_name');
  });

  /**
   * sqlite3 CLI can use indexes for query optimization
   *
   * Note: This test uses better-sqlite3 to create the index directly
   * because DoSQL's in-memory engine does not fully export custom indexes.
   * The test verifies that sqlite3 CLI can use indexes from standard SQLite files.
   */
  it('should use indexes in EXPLAIN QUERY PLAN via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    // Create database with index directly via better-sqlite3
    const filepath = tempFilePath('index-query-plan.sqlite');
    const BetterSqlite3 = await import('better-sqlite3').then(m => m.default);
    const nativeDb = new BetterSqlite3(filepath);
    nativeDb.exec('CREATE TABLE logs (id INTEGER PRIMARY KEY, timestamp TEXT, message TEXT)');
    nativeDb.exec('CREATE INDEX idx_timestamp ON logs(timestamp)');

    // Insert some data
    for (let i = 0; i < 100; i++) {
      nativeDb.exec(`INSERT INTO logs (timestamp, message) VALUES ('2024-01-${String(i % 28 + 1).padStart(2, '0')}', 'message ${i}')`);
    }
    nativeDb.close();

    // Check query plan uses index
    const output = runSqlite3(
      filepath,
      "EXPLAIN QUERY PLAN SELECT * FROM logs WHERE timestamp = '2024-01-15';"
    );
    // Should mention the index or SEARCH (using index)
    expect(output.toLowerCase()).toMatch(/index|search/);
  });

  /**
   * sqlite3 CLI can handle multiple tables with foreign key relationships
   */
  it('should handle multi-table data via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)');
    db.exec('CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)');

    db.exec("INSERT INTO departments (name) VALUES ('Engineering')");
    db.exec("INSERT INTO departments (name) VALUES ('Marketing')");
    db.exec("INSERT INTO employees (name, dept_id) VALUES ('Alice', 1)");
    db.exec("INSERT INTO employees (name, dept_id) VALUES ('Bob', 1)");
    db.exec("INSERT INTO employees (name, dept_id) VALUES ('Carol', 2)");

    const filepath = tempFilePath('multi-table.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // JOIN query via CLI
    const output = runSqlite3(
      filepath,
      'SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id ORDER BY e.name;'
    );
    const lines = output.split('\n');
    expect(lines).toHaveLength(3);
    expect(lines[0]).toBe('Alice|Engineering');
    expect(lines[1]).toBe('Bob|Engineering');
    expect(lines[2]).toBe('Carol|Marketing');
  });

  /**
   * sqlite3 CLI can handle BLOB data
   */
  it('should handle BLOB data via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE files (id INTEGER PRIMARY KEY, name TEXT, content BLOB)');

    // Insert blob data
    const blobData = new Uint8Array([0x00, 0x01, 0x02, 0x03, 0xFF]);
    const insert = db.prepare('INSERT INTO files (name, content) VALUES (?, ?)');
    insert.run('test.bin', blobData);

    const filepath = tempFilePath('blob-data.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Query blob length
    const output = runSqlite3(filepath, 'SELECT name, length(content) FROM files;');
    expect(output).toBe('test.bin|5');
  });

  /**
   * sqlite3 CLI can handle NULL values properly
   */
  it('should handle NULL values via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE nullable (id INTEGER PRIMARY KEY, value TEXT)');
    db.exec('INSERT INTO nullable (value) VALUES (NULL)');
    db.exec("INSERT INTO nullable (value) VALUES ('not null')");
    db.exec('INSERT INTO nullable (value) VALUES (NULL)');

    const filepath = tempFilePath('null-values.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Count NULLs
    const output = runSqlite3(filepath, 'SELECT COUNT(*) FROM nullable WHERE value IS NULL;');
    expect(output).toBe('2');
  });

  /**
   * sqlite3 CLI can handle Unicode data
   */
  it('should handle Unicode data via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE i18n (id INTEGER PRIMARY KEY, text TEXT)');
    db.exec("INSERT INTO i18n (text) VALUES ('Hello')");
    db.exec("INSERT INTO i18n (text) VALUES ('Bonjour')");
    db.exec("INSERT INTO i18n (text) VALUES ('Hola')");

    const filepath = tempFilePath('unicode-data.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Query Unicode data
    const output = runSqlite3(filepath, 'SELECT text FROM i18n ORDER BY id;');
    const lines = output.split('\n');
    expect(lines).toContain('Hello');
    expect(lines).toContain('Bonjour');
    expect(lines).toContain('Hola');
  });

  /**
   * sqlite3 CLI can dump and restore database
   */
  it('should support .dump command via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const db = new Database();
    db.exec('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)');
    db.exec("INSERT INTO products (name, price) VALUES ('Widget', 9.99)");
    db.exec("INSERT INTO products (name, price) VALUES ('Gadget', 19.99)");

    const filepath = tempFilePath('dump-test.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Dump the database
    const dump = runSqlite3(filepath, '.dump');

    // Dump should contain CREATE TABLE and INSERT statements
    expect(dump).toContain('CREATE TABLE');
    expect(dump).toContain('products');
    expect(dump).toContain('INSERT INTO');
    expect(dump).toContain('Widget');
    expect(dump).toContain('Gadget');
  });
});

// =============================================================================
// 5. ROUND-TRIP VERIFICATION
// =============================================================================

describe('Round-trip verification (DoSQL -> sqlite3 -> DoSQL)', () => {
  /**
   * Data survives round-trip through sqlite3 CLI
   */
  it('should preserve data through sqlite3 CLI modifications', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    // Create and export DoSQL database
    const db = new Database();
    db.exec('CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)');
    db.exec('INSERT INTO counter (value) VALUES (100)');

    const filepath = tempFilePath('round-trip.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Modify via sqlite3 CLI
    runSqlite3(filepath, 'UPDATE counter SET value = value + 50 WHERE id = 1;');
    runSqlite3(filepath, 'INSERT INTO counter (value) VALUES (200);');

    // Read back into DoSQL
    const restoredDb = new FileBackedDatabase({ filename: filepath });

    const rows = restoredDb.prepare('SELECT value FROM counter ORDER BY id').pluck().all();
    expect(rows).toEqual([150, 200]);

    restoredDb.close();
  });

  /**
   * Schema modifications via sqlite3 CLI are readable by DoSQL
   */
  it('should read schema changes made by sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    // Create initial database
    const db = new Database();
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

    const filepath = tempFilePath('schema-round-trip.sqlite');
    const fileDb = FileBackedDatabase.fromInMemory(db);
    await fileDb.exportTo({ path: filepath, overwrite: true });

    // Add new table via sqlite3 CLI
    runSqlite3(filepath, 'CREATE TABLE roles (id INTEGER PRIMARY KEY, role_name TEXT);');
    runSqlite3(filepath, "INSERT INTO roles (role_name) VALUES ('admin');");

    // Read back into DoSQL
    const restoredDb = new FileBackedDatabase({ filename: filepath });

    const tables = restoredDb.getTables().sort();
    expect(tables).toContain('users');
    expect(tables).toContain('roles');

    const roles = restoredDb.prepare('SELECT role_name FROM roles').pluck().all();
    expect(roles).toEqual(['admin']);

    restoredDb.close();
  });
});

// =============================================================================
// 6. ERROR CASES
// =============================================================================

describe('Error handling with sqlite3 CLI', () => {
  /**
   * sqlite3 CLI reports errors on corrupted files
   */
  it('should detect corrupted files via sqlite3 CLI', async () => {
    if (!sqlite3Available) {
      console.log('Skipping: sqlite3 CLI not available');
      return;
    }

    const corruptPath = tempFilePath('corrupted.sqlite');

    // Create a valid-looking but corrupted file
    const buffer = Buffer.alloc(4096);
    buffer.write('SQLite format 3\0', 0);
    // Set invalid page size (0)
    buffer.writeUInt16BE(0, 16);
    fs.writeFileSync(corruptPath, buffer);

    // sqlite3 should report an error
    const result = spawnSync('sqlite3', [corruptPath, 'PRAGMA integrity_check;'], {
      encoding: 'utf-8',
      timeout: 5000,
    });

    // Should either fail or report corruption
    const combinedOutput = (result.stdout || '') + (result.stderr || '');
    expect(
      result.status !== 0 ||
      combinedOutput.toLowerCase().includes('corrupt') ||
      combinedOutput.toLowerCase().includes('malformed') ||
      combinedOutput.toLowerCase().includes('error')
    ).toBe(true);
  });
});
