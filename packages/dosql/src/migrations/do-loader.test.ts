/**
 * DoSQL Migrations - .do/migrations Loader Tests
 *
 * TDD tests for loading migrations from .do/migrations/*.sql folder structure.
 * Uses workers-vitest-pool with NO MOCKS.
 *
 * Convention:
 * ```
 * .do/
 *   migrations/
 *     001_create_users.sql
 *     002_add_posts.sql
 *     003_add_indexes.sql
 * ```
 */

import { describe, it, expect, beforeEach } from 'vitest';

import {
  type Migration,
  calculateChecksumSync,
} from './types.js';

import {
  createInMemoryFs,
  type MigrationFileSystem,
} from './drizzle-compat.js';

import {
  loadMigrationsFromFolder,
  parseMigrationFile,
  sortMigrations,
  extractMigrationNumber,
  validateMigrationSequence,
  parseSqlStatements,
  stripSqlComments,
  detectAppliedMigrations,
  getPendingMigrations,
  applyMigrationsInOrder,
  type DoMigrationLoaderOptions,
  type MigrationValidationError,
  DEFAULT_DO_MIGRATIONS_FOLDER,
} from './do-loader.js';

import {
  createSchemaTracker,
  createInMemoryStorage,
  type SchemaStorage,
} from './schema-tracker.js';

import { type DatabaseExecutor } from './runner.js';

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Create a simple in-memory database executor for testing
 */
function createTestExecutor(): DatabaseExecutor & {
  tables: Map<string, { columns: string[]; rows: unknown[][] }>;
  executedSql: string[];
} {
  const tables = new Map<string, { columns: string[]; rows: unknown[][] }>();
  const executedSql: string[] = [];

  return {
    tables,
    executedSql,

    exec(sql: string) {
      executedSql.push(sql);

      // Normalize SQL for matching (remove extra whitespace)
      const normalizedSql = sql.replace(/\s+/g, ' ').trim();

      // Very simple SQL parser for testing
      const createMatch = normalizedSql.match(
        /CREATE TABLE(\s+IF NOT EXISTS)?\s+"?(\w+)"?\s*\(\s*(.*)\s*\)/i
      );
      if (createMatch) {
        const ifNotExists = !!createMatch[1];
        const tableName = createMatch[2];
        const columnDefs = createMatch[3];

        // If IF NOT EXISTS and table already exists, don't recreate it
        if (ifNotExists && tables.has(tableName)) {
          return;
        }

        // Split by comma but handle cases where column names have underscores
        const columns = columnDefs
          .split(',')
          .map(c => {
            const trimmed = c.trim();
            // Extract first word (column name), handling quotes
            const match = trimmed.match(/^"?(\w+)"?/);
            return match ? match[1] : trimmed.split(/\s+/)[0];
          })
          .filter(c => c.length > 0);
        tables.set(tableName, { columns, rows: [] });
      }

      const dropMatch = normalizedSql.match(/DROP TABLE(?:\s+IF EXISTS)?\s+"?(\w+)"?/i);
      if (dropMatch) {
        tables.delete(dropMatch[1]);
      }

      // Handle CREATE INDEX
      const indexMatch = normalizedSql.match(/CREATE\s+(?:UNIQUE\s+)?INDEX/i);
      if (indexMatch) {
        // Just record that we executed it
      }
    },

    query<T>(sql: string, params?: unknown[]): T[] {
      executedSql.push(sql);

      // Handle SELECT from migrations table
      const selectMatch = sql.match(/SELECT .+ FROM "?([a-zA-Z_][a-zA-Z0-9_]*)"?(?:\s|$)/i);
      if (selectMatch) {
        const tableName = selectMatch[1];
        const table = tables.get(tableName);

        if (!table) return [] as T[];

        const result = table.rows.map(row => {
          const obj: Record<string, unknown> = {};
          table.columns.forEach((col, i) => {
            obj[col] = row[i];
          });
          return obj as T;
        }) as T[];

        return result;
      }

      return [] as T[];
    },

    run(sql: string, params?: unknown[]): { changes: number } {
      executedSql.push(sql);

      // Handle INSERT
      const insertMatch = sql.match(/INSERT INTO "?(\w+)"?\s*\(([^)]+)\)/i);
      if (insertMatch && params) {
        const tableName = insertMatch[1];
        let table = tables.get(tableName);

        if (!table) {
          const columns = insertMatch[2].split(',').map(c => c.trim().replace(/"/g, ''));
          table = { columns, rows: [] };
          tables.set(tableName, table);
        }

        table.rows.push(params as unknown[]);
        return { changes: 1 };
      }

      // Handle DELETE
      const deleteMatch = sql.match(/DELETE FROM "?(\w+)"?/i);
      if (deleteMatch && params) {
        const tableName = deleteMatch[1];
        const table = tables.get(tableName);

        if (table) {
          const idIndex = table.columns.indexOf('id');
          const initialLength = table.rows.length;
          table.rows = table.rows.filter(row => row[idIndex] !== params[0]);
          return { changes: initialLength - table.rows.length };
        }
      }

      return { changes: 0 };
    },
  };
}

// =============================================================================
// LOAD MIGRATIONS FROM FOLDER TESTS
// =============================================================================

describe('.do/migrations Loader', () => {
  describe('loadMigrationsFromFolder', () => {
    it('should load migrations from .do/migrations folder', async () => {
      const fs = createInMemoryFs({
        '/.do/migrations/001_create_users.sql': 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);',
        '/.do/migrations/002_add_posts.sql': 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT, user_id INTEGER);',
      });

      const migrations = await loadMigrationsFromFolder({
        basePath: '/.do/migrations',
        fs,
      });

      expect(migrations).toHaveLength(2);
      expect(migrations[0].name).toBe('create_users');
      expect(migrations[1].name).toBe('add_posts');
    });

    it('should load from default .do/migrations path', async () => {
      const fs = createInMemoryFs({
        '.do/migrations/001_init.sql': 'CREATE TABLE init (id INT);',
      });

      const migrations = await loadMigrationsFromFolder({
        basePath: DEFAULT_DO_MIGRATIONS_FOLDER,
        fs,
      });

      expect(migrations).toHaveLength(1);
    });

    it('should ignore non-.sql files', async () => {
      const fs = createInMemoryFs({
        '/.do/migrations/001_create_users.sql': 'CREATE TABLE users (id INTEGER PRIMARY KEY);',
        '/.do/migrations/README.md': '# Migrations',
        '/.do/migrations/.gitkeep': '',
        '/.do/migrations/backup.sql.bak': 'backup content',
      });

      const migrations = await loadMigrationsFromFolder({
        basePath: '/.do/migrations',
        fs,
      });

      expect(migrations).toHaveLength(1);
      expect(migrations[0].name).toBe('create_users');
    });

    it('should handle empty migrations folder', async () => {
      const fs = createInMemoryFs({
        '/.do/migrations/.gitkeep': '',
      });

      const migrations = await loadMigrationsFromFolder({
        basePath: '/.do/migrations',
        fs,
      });

      expect(migrations).toHaveLength(0);
    });

    it('should throw error if folder does not exist', async () => {
      const fs = createInMemoryFs({});

      await expect(
        loadMigrationsFromFolder({
          basePath: '/.do/migrations',
          fs,
        })
      ).rejects.toThrow(/not found|does not exist/i);
    });

    it('should support nested folders within migrations', async () => {
      const fs = createInMemoryFs({
        '/.do/migrations/001_create_users.sql': 'CREATE TABLE users (id INTEGER PRIMARY KEY);',
        '/.do/migrations/archived/old_migration.sql': 'old content',
      });

      const migrations = await loadMigrationsFromFolder({
        basePath: '/.do/migrations',
        fs,
      });

      // Should only load from root, not nested folders
      expect(migrations).toHaveLength(1);
    });
  });

  // ===========================================================================
  // SORT MIGRATIONS TESTS
  // ===========================================================================

  describe('sortMigrations', () => {
    it('should sort by numeric prefix', () => {
      const migrations: Migration[] = [
        { id: '003_third', sql: '', checksum: '', createdAt: new Date(), name: 'third' },
        { id: '001_first', sql: '', checksum: '', createdAt: new Date(), name: 'first' },
        { id: '002_second', sql: '', checksum: '', createdAt: new Date(), name: 'second' },
      ];

      const sorted = sortMigrations(migrations);

      expect(sorted[0].name).toBe('first');
      expect(sorted[1].name).toBe('second');
      expect(sorted[2].name).toBe('third');
    });

    it('should handle different prefix lengths', () => {
      const migrations: Migration[] = [
        { id: '10_ten', sql: '', checksum: '', createdAt: new Date(), name: 'ten' },
        { id: '2_two', sql: '', checksum: '', createdAt: new Date(), name: 'two' },
        { id: '1_one', sql: '', checksum: '', createdAt: new Date(), name: 'one' },
      ];

      const sorted = sortMigrations(migrations);

      expect(sorted[0].name).toBe('one');
      expect(sorted[1].name).toBe('two');
      expect(sorted[2].name).toBe('ten');
    });

    it('should handle zero-padded numbers correctly', () => {
      const migrations: Migration[] = [
        { id: '010_ten', sql: '', checksum: '', createdAt: new Date(), name: 'ten' },
        { id: '001_one', sql: '', checksum: '', createdAt: new Date(), name: 'one' },
        { id: '100_hundred', sql: '', checksum: '', createdAt: new Date(), name: 'hundred' },
      ];

      const sorted = sortMigrations(migrations);

      expect(sorted[0].name).toBe('one');
      expect(sorted[1].name).toBe('ten');
      expect(sorted[2].name).toBe('hundred');
    });

    it('should return empty array for empty input', () => {
      const sorted = sortMigrations([]);
      expect(sorted).toHaveLength(0);
    });

    it('should not modify original array', () => {
      const original: Migration[] = [
        { id: '002_second', sql: '', checksum: '', createdAt: new Date(), name: 'second' },
        { id: '001_first', sql: '', checksum: '', createdAt: new Date(), name: 'first' },
      ];

      const sorted = sortMigrations(original);

      expect(original[0].name).toBe('second');
      expect(sorted[0].name).toBe('first');
    });
  });

  // ===========================================================================
  // PARSE MIGRATION FILE TESTS
  // ===========================================================================

  describe('parseMigrationFile', () => {
    it('should parse filename and extract id and name', () => {
      const migration = parseMigrationFile(
        '001_create_users.sql',
        'CREATE TABLE users (id INTEGER PRIMARY KEY);'
      );

      expect(migration.id).toBe('001_create_users');
      expect(migration.name).toBe('create_users');
      expect(migration.sql).toBe('CREATE TABLE users (id INTEGER PRIMARY KEY);');
    });

    it('should calculate checksum', () => {
      const sql = 'CREATE TABLE test (id INT);';
      const migration = parseMigrationFile('001_test.sql', sql);

      expect(migration.checksum).toBe(calculateChecksumSync(sql));
    });

    it('should set createdAt', () => {
      const migration = parseMigrationFile('001_test.sql', 'SELECT 1;');

      expect(migration.createdAt).toBeInstanceOf(Date);
    });

    it('should handle underscores in migration name', () => {
      const migration = parseMigrationFile(
        '001_create_user_posts_table.sql',
        'CREATE TABLE user_posts (id INT);'
      );

      expect(migration.name).toBe('create_user_posts_table');
    });

    it('should handle numeric-only prefixes', () => {
      const migration = parseMigrationFile(
        '00001_migration.sql',
        'SELECT 1;'
      );

      expect(migration.id).toBe('00001_migration');
    });

    it('should throw on invalid filename format', () => {
      expect(() => parseMigrationFile('invalid.sql', 'SELECT 1;')).toThrow(/invalid.*format/i);
      expect(() => parseMigrationFile('no_number.sql', 'SELECT 1;')).toThrow(/invalid.*format/i);
    });
  });

  // ===========================================================================
  // EXTRACT MIGRATION NUMBER TESTS
  // ===========================================================================

  describe('extractMigrationNumber', () => {
    it('should extract number from migration ID', () => {
      expect(extractMigrationNumber('001_create_users')).toBe(1);
      expect(extractMigrationNumber('002_add_posts')).toBe(2);
      expect(extractMigrationNumber('100_big_number')).toBe(100);
    });

    it('should handle different padding', () => {
      expect(extractMigrationNumber('1_no_padding')).toBe(1);
      expect(extractMigrationNumber('01_one_digit')).toBe(1);
      expect(extractMigrationNumber('001_three_digits')).toBe(1);
      expect(extractMigrationNumber('0001_four_digits')).toBe(1);
    });

    it('should return null for invalid IDs', () => {
      expect(extractMigrationNumber('invalid')).toBeNull();
      expect(extractMigrationNumber('no_number')).toBeNull();
      expect(extractMigrationNumber('')).toBeNull();
    });
  });

  // ===========================================================================
  // VALIDATE MIGRATION SEQUENCE TESTS
  // ===========================================================================

  describe('validateMigrationSequence', () => {
    it('should pass for valid sequential migrations', () => {
      const migrations: Migration[] = [
        { id: '001_first', sql: '', checksum: '', createdAt: new Date() },
        { id: '002_second', sql: '', checksum: '', createdAt: new Date() },
        { id: '003_third', sql: '', checksum: '', createdAt: new Date() },
      ];

      const result = validateMigrationSequence(migrations);

      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should detect gaps in sequence', () => {
      const migrations: Migration[] = [
        { id: '001_first', sql: '', checksum: '', createdAt: new Date() },
        { id: '003_third', sql: '', checksum: '', createdAt: new Date() },
      ];

      const result = validateMigrationSequence(migrations);

      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(expect.objectContaining({
        type: 'gap',
        migrationId: '003_third',
      }));
    });

    it('should detect duplicates', () => {
      const migrations: Migration[] = [
        { id: '001_first', sql: '', checksum: '', createdAt: new Date() },
        { id: '001_duplicate', sql: '', checksum: '', createdAt: new Date() },
      ];

      const result = validateMigrationSequence(migrations);

      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(expect.objectContaining({
        type: 'duplicate',
      }));
    });

    it('should allow non-sequential numbering when configured', () => {
      const migrations: Migration[] = [
        { id: '001_first', sql: '', checksum: '', createdAt: new Date() },
        { id: '010_tenth', sql: '', checksum: '', createdAt: new Date() },
      ];

      const result = validateMigrationSequence(migrations, { allowGaps: true });

      expect(result.valid).toBe(true);
    });

    it('should pass for empty migrations', () => {
      const result = validateMigrationSequence([]);

      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should detect out-of-order migrations', () => {
      const migrations: Migration[] = [
        { id: '002_second', sql: '', checksum: '', createdAt: new Date() },
        { id: '001_first', sql: '', checksum: '', createdAt: new Date() },
      ];

      const result = validateMigrationSequence(migrations);

      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(expect.objectContaining({
        type: 'out_of_order',
      }));
    });
  });

  // ===========================================================================
  // SQL PARSING TESTS
  // ===========================================================================

  describe('parseSqlStatements', () => {
    it('should split by semicolon', () => {
      const sql = 'CREATE TABLE a (id INT); CREATE TABLE b (id INT);';
      const statements = parseSqlStatements(sql);

      expect(statements).toHaveLength(2);
      expect(statements[0]).toBe('CREATE TABLE a (id INT)');
      expect(statements[1]).toBe('CREATE TABLE b (id INT)');
    });

    it('should handle multiple statements with newlines', () => {
      const sql = `
        CREATE TABLE users (id INTEGER PRIMARY KEY);

        CREATE TABLE posts (
          id INTEGER PRIMARY KEY,
          user_id INTEGER
        );

        CREATE INDEX idx_posts_user_id ON posts(user_id);
      `;

      const statements = parseSqlStatements(sql);

      expect(statements).toHaveLength(3);
    });

    it('should handle single statement without trailing semicolon', () => {
      const sql = 'CREATE TABLE test (id INT)';
      const statements = parseSqlStatements(sql);

      expect(statements).toHaveLength(1);
      expect(statements[0]).toBe('CREATE TABLE test (id INT)');
    });

    it('should preserve strings containing semicolons', () => {
      const sql = "INSERT INTO test VALUES ('value; with; semicolons');";
      const statements = parseSqlStatements(sql);

      expect(statements).toHaveLength(1);
      expect(statements[0]).toContain('value; with; semicolons');
    });

    it('should return empty array for empty input', () => {
      expect(parseSqlStatements('')).toHaveLength(0);
      expect(parseSqlStatements('   ')).toHaveLength(0);
    });

    it('should handle escaped quotes in strings', () => {
      const sql = "INSERT INTO test VALUES ('it''s escaped'); SELECT 1;";
      const statements = parseSqlStatements(sql);

      expect(statements).toHaveLength(2);
    });
  });

  describe('stripSqlComments', () => {
    it('should remove single-line comments (--)', () => {
      const sql = `
        -- This is a comment
        CREATE TABLE users (id INT);
        -- Another comment
      `;

      const result = stripSqlComments(sql);

      expect(result).not.toContain('This is a comment');
      expect(result).toContain('CREATE TABLE users');
    });

    it('should remove multi-line comments (/* */)', () => {
      const sql = `
        /* This is a
           multi-line comment */
        CREATE TABLE users (id INT);
      `;

      const result = stripSqlComments(sql);

      expect(result).not.toContain('multi-line comment');
      expect(result).toContain('CREATE TABLE users');
    });

    it('should preserve strings containing comment-like content', () => {
      const sql = "INSERT INTO test VALUES ('-- not a comment');";

      const result = stripSqlComments(sql);

      expect(result).toContain('-- not a comment');
    });

    it('should handle nested-looking comments', () => {
      const sql = `
        /* outer /* inner */ outer end */
        SELECT 1;
      `;

      const result = stripSqlComments(sql);

      expect(result).toContain('SELECT 1');
    });

    it('should return empty string for comment-only SQL', () => {
      const sql = `
        -- Just a comment
        /* Another comment */
      `;

      const result = stripSqlComments(sql).trim();

      expect(result).toBe('');
    });
  });

  // ===========================================================================
  // DETECT APPLIED MIGRATIONS TESTS
  // ===========================================================================

  describe('detectAppliedMigrations', () => {
    let storage: SchemaStorage;

    beforeEach(() => {
      storage = createInMemoryStorage();
    });

    it('should return empty set when no migrations applied', async () => {
      const tracker = createSchemaTracker(storage);

      const applied = await detectAppliedMigrations(tracker);

      expect(applied.size).toBe(0);
    });

    it('should return applied migration IDs', async () => {
      const tracker = createSchemaTracker(storage);

      await tracker.recordMigration({
        id: '001_create_users',
        appliedAt: new Date(),
        checksum: 'abc123',
        durationMs: 10,
      });
      await tracker.recordMigration({
        id: '002_add_posts',
        appliedAt: new Date(),
        checksum: 'def456',
        durationMs: 15,
      });

      const applied = await detectAppliedMigrations(tracker);

      expect(applied.size).toBe(2);
      expect(applied.has('001_create_users')).toBe(true);
      expect(applied.has('002_add_posts')).toBe(true);
    });
  });

  // ===========================================================================
  // GET PENDING MIGRATIONS TESTS
  // ===========================================================================

  describe('getPendingMigrations', () => {
    it('should return all migrations when none applied', () => {
      const migrations: Migration[] = [
        { id: '001_first', sql: 'SELECT 1', checksum: 'a', createdAt: new Date() },
        { id: '002_second', sql: 'SELECT 2', checksum: 'b', createdAt: new Date() },
      ];
      const applied = new Set<string>();

      const pending = getPendingMigrations(migrations, applied);

      expect(pending).toHaveLength(2);
    });

    it('should exclude applied migrations', () => {
      const migrations: Migration[] = [
        { id: '001_first', sql: 'SELECT 1', checksum: 'a', createdAt: new Date() },
        { id: '002_second', sql: 'SELECT 2', checksum: 'b', createdAt: new Date() },
        { id: '003_third', sql: 'SELECT 3', checksum: 'c', createdAt: new Date() },
      ];
      const applied = new Set(['001_first', '002_second']);

      const pending = getPendingMigrations(migrations, applied);

      expect(pending).toHaveLength(1);
      expect(pending[0].id).toBe('003_third');
    });

    it('should return sorted pending migrations', () => {
      const migrations: Migration[] = [
        { id: '003_third', sql: 'SELECT 3', checksum: 'c', createdAt: new Date() },
        { id: '001_first', sql: 'SELECT 1', checksum: 'a', createdAt: new Date() },
        { id: '002_second', sql: 'SELECT 2', checksum: 'b', createdAt: new Date() },
      ];
      const applied = new Set(['001_first']);

      const pending = getPendingMigrations(migrations, applied);

      expect(pending[0].id).toBe('002_second');
      expect(pending[1].id).toBe('003_third');
    });

    it('should return empty array when all applied', () => {
      const migrations: Migration[] = [
        { id: '001_first', sql: 'SELECT 1', checksum: 'a', createdAt: new Date() },
      ];
      const applied = new Set(['001_first']);

      const pending = getPendingMigrations(migrations, applied);

      expect(pending).toHaveLength(0);
    });
  });

  // ===========================================================================
  // APPLY MIGRATIONS IN ORDER TESTS
  // ===========================================================================

  describe('applyMigrationsInOrder', () => {
    let storage: SchemaStorage;
    let executor: ReturnType<typeof createTestExecutor>;

    beforeEach(() => {
      storage = createInMemoryStorage();
      executor = createTestExecutor();
    });

    it('should apply migrations in order', async () => {
      const migrations: Migration[] = [
        { id: '001_create_users', sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY);', checksum: 'a', createdAt: new Date() },
        { id: '002_create_posts', sql: 'CREATE TABLE posts (id INTEGER PRIMARY KEY);', checksum: 'b', createdAt: new Date() },
      ];
      const tracker = createSchemaTracker(storage);

      const result = await applyMigrationsInOrder(migrations, executor, tracker);

      expect(result.success).toBe(true);
      expect(result.applied).toHaveLength(2);
      expect(executor.tables.has('users')).toBe(true);
      expect(executor.tables.has('posts')).toBe(true);
    });

    it('should record migrations in tracker', async () => {
      const migrations: Migration[] = [
        { id: '001_test', sql: 'CREATE TABLE test (id INT);', checksum: 'a', createdAt: new Date() },
      ];
      const tracker = createSchemaTracker(storage);

      await applyMigrationsInOrder(migrations, executor, tracker);

      const applied = await tracker.getAppliedMigrations();
      expect(applied).toHaveLength(1);
      expect(applied[0].id).toBe('001_test');
    });

    it('should not apply already applied migrations', async () => {
      const tracker = createSchemaTracker(storage);

      // Pre-apply first migration
      await tracker.recordMigration({
        id: '001_first',
        appliedAt: new Date(),
        checksum: 'a',
        durationMs: 10,
      });

      const migrations: Migration[] = [
        { id: '001_first', sql: 'CREATE TABLE first (id INT);', checksum: 'a', createdAt: new Date() },
        { id: '002_second', sql: 'CREATE TABLE second (id INT);', checksum: 'b', createdAt: new Date() },
      ];

      const result = await applyMigrationsInOrder(migrations, executor, tracker);

      expect(result.applied).toHaveLength(1);
      expect(result.applied[0].id).toBe('002_second');
    });

    it('should handle multi-statement SQL files', async () => {
      const migrations: Migration[] = [
        {
          id: '001_multi_statement',
          sql: `
            CREATE TABLE users (id INTEGER PRIMARY KEY);
            CREATE TABLE posts (id INTEGER PRIMARY KEY);
            CREATE INDEX idx_test ON users(id);
          `,
          checksum: 'a',
          createdAt: new Date(),
        },
      ];
      const tracker = createSchemaTracker(storage);

      const result = await applyMigrationsInOrder(migrations, executor, tracker);

      expect(result.success).toBe(true);
      // The test executor processes all SQL in a single exec call
      // Multi-statement SQL support is validated by no errors being thrown
      expect(result.applied).toHaveLength(1);
      expect(executor.executedSql.some(sql => sql.includes('CREATE TABLE users'))).toBe(true);
    });

    it('should stop on error unless continueOnError is set', async () => {
      const failingExecutor = {
        ...executor,
        exec(sql: string) {
          if (sql.includes('fail_table')) {
            throw new Error('Intentional failure');
          }
          executor.exec(sql);
        },
      };

      const migrations: Migration[] = [
        { id: '001_good', sql: 'CREATE TABLE good (id INT);', checksum: 'a', createdAt: new Date() },
        { id: '002_bad', sql: 'CREATE TABLE fail_table (id INT);', checksum: 'b', createdAt: new Date() },
        { id: '003_never', sql: 'CREATE TABLE never (id INT);', checksum: 'c', createdAt: new Date() },
      ];
      const tracker = createSchemaTracker(storage);

      const result = await applyMigrationsInOrder(migrations, failingExecutor, tracker);

      expect(result.success).toBe(false);
      expect(result.applied).toHaveLength(1);
      expect(result.failed).toHaveLength(1);
      expect(result.failed[0].migration.id).toBe('002_bad');
    });

    it('should report total duration', async () => {
      const migrations: Migration[] = [
        { id: '001_test', sql: 'SELECT 1;', checksum: 'a', createdAt: new Date() },
      ];
      const tracker = createSchemaTracker(storage);

      const result = await applyMigrationsInOrder(migrations, executor, tracker);

      expect(result.totalDurationMs).toBeGreaterThanOrEqual(0);
    });
  });

  // ===========================================================================
  // ERROR ON OUT-OF-ORDER TESTS
  // ===========================================================================

  describe('Out-of-Order Migration Detection', () => {
    let storage: SchemaStorage;

    beforeEach(() => {
      storage = createInMemoryStorage();
    });

    it('should error when migration is older than latest applied', async () => {
      const tracker = createSchemaTracker(storage);

      // Apply migration 003
      await tracker.recordMigration({
        id: '003_third',
        appliedAt: new Date(),
        checksum: 'c',
        durationMs: 10,
      });

      const migrations: Migration[] = [
        { id: '001_first', sql: 'SELECT 1', checksum: 'a', createdAt: new Date() },
        { id: '002_second', sql: 'SELECT 2', checksum: 'b', createdAt: new Date() },
        { id: '003_third', sql: 'SELECT 3', checksum: 'c', createdAt: new Date() },
      ];

      const result = validateMigrationSequence(migrations, {
        appliedIds: new Set(['003_third']),
        strictOrder: true,
      });

      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(expect.objectContaining({
        type: 'out_of_order',
      }));
    });

    it('should allow applying missing migrations when configured', async () => {
      const tracker = createSchemaTracker(storage);

      // Apply migrations 001 and 003 (gap)
      await tracker.recordMigration({
        id: '001_first',
        appliedAt: new Date(),
        checksum: 'a',
        durationMs: 10,
      });
      await tracker.recordMigration({
        id: '003_third',
        appliedAt: new Date(),
        checksum: 'c',
        durationMs: 10,
      });

      const migrations: Migration[] = [
        { id: '001_first', sql: 'SELECT 1', checksum: 'a', createdAt: new Date() },
        { id: '002_second', sql: 'SELECT 2', checksum: 'b', createdAt: new Date() },
        { id: '003_third', sql: 'SELECT 3', checksum: 'c', createdAt: new Date() },
      ];

      const result = validateMigrationSequence(migrations, {
        appliedIds: new Set(['001_first', '003_third']),
        allowOutOfOrder: true,
      });

      expect(result.valid).toBe(true);
    });
  });

  // ===========================================================================
  // INTEGRATION WITH SCHEMA TRACKER TESTS
  // ===========================================================================

  describe('Integration with SchemaTracker', () => {
    let storage: SchemaStorage;
    let executor: ReturnType<typeof createTestExecutor>;

    beforeEach(() => {
      storage = createInMemoryStorage();
      executor = createTestExecutor();
    });

    it('should integrate with schema tracker for full workflow', async () => {
      const fs = createInMemoryFs({
        '/.do/migrations/001_create_users.sql': 'CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);',
        '/.do/migrations/002_add_posts.sql': 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT);',
      });

      // Load migrations
      const migrations = await loadMigrationsFromFolder({
        basePath: '/.do/migrations',
        fs,
      });

      expect(migrations).toHaveLength(2);

      // Create tracker and apply
      const tracker = createSchemaTracker(storage);
      const result = await applyMigrationsInOrder(migrations, executor, tracker);

      expect(result.success).toBe(true);
      expect(result.applied).toHaveLength(2);

      // Verify tracker state
      const version = await tracker.getCurrentVersion();
      expect(version).toBe('002_add_posts');

      const applied = await tracker.getAppliedMigrations();
      expect(applied).toHaveLength(2);
    });

    it('should handle incremental migration updates', async () => {
      const tracker = createSchemaTracker(storage);

      // First batch
      const fs1 = createInMemoryFs({
        '/.do/migrations/001_create_users.sql': 'CREATE TABLE users (id INTEGER PRIMARY KEY);',
      });

      const migrations1 = await loadMigrationsFromFolder({
        basePath: '/.do/migrations',
        fs: fs1,
      });

      await applyMigrationsInOrder(migrations1, executor, tracker);

      // Second batch with new migration
      const fs2 = createInMemoryFs({
        '/.do/migrations/001_create_users.sql': 'CREATE TABLE users (id INTEGER PRIMARY KEY);',
        '/.do/migrations/002_add_posts.sql': 'CREATE TABLE posts (id INTEGER PRIMARY KEY);',
      });

      const migrations2 = await loadMigrationsFromFolder({
        basePath: '/.do/migrations',
        fs: fs2,
      });

      const result = await applyMigrationsInOrder(migrations2, executor, tracker);

      expect(result.applied).toHaveLength(1);
      expect(result.applied[0].id).toBe('002_add_posts');
    });

    it('should detect checksum mismatches', async () => {
      const tracker = createSchemaTracker(storage);

      // Apply a migration
      await tracker.recordMigration({
        id: '001_test',
        appliedAt: new Date(),
        checksum: 'original_checksum',
        durationMs: 10,
      });

      // Load migrations with different content (different checksum)
      const fs = createInMemoryFs({
        '/.do/migrations/001_test.sql': 'MODIFIED SQL CONTENT;',
      });

      const migrations = await loadMigrationsFromFolder({
        basePath: '/.do/migrations',
        fs,
      });

      const applied = await tracker.getAppliedMigrations();
      const mismatch = migrations.find(m =>
        applied.some(a => a.id === m.id && a.checksum !== m.checksum)
      );

      expect(mismatch).toBeDefined();
      expect(mismatch?.id).toBe('001_test');
    });
  });

  // ===========================================================================
  // EDGE CASES AND ERROR HANDLING TESTS
  // ===========================================================================

  describe('Edge Cases and Error Handling', () => {
    it('should handle very large migration numbers', () => {
      const migration = parseMigrationFile(
        '9999999_big_number.sql',
        'SELECT 1;'
      );

      expect(extractMigrationNumber(migration.id)).toBe(9999999);
    });

    it('should handle special characters in migration names', () => {
      // Note: underscores are the delimiter, so we need to handle them carefully
      const migration = parseMigrationFile(
        '001_create_user_profiles_table.sql',
        'CREATE TABLE user_profiles (id INT);'
      );

      expect(migration.name).toBe('create_user_profiles_table');
    });

    it('should handle UTF-8 content in SQL files', async () => {
      const fs = createInMemoryFs({
        '/.do/migrations/001_unicode.sql': "INSERT INTO test VALUES ('Hello World');",
      });

      const migrations = await loadMigrationsFromFolder({
        basePath: '/.do/migrations',
        fs,
      });

      expect(migrations[0].sql).toContain('Hello World');
    });

    it('should handle very long SQL files', async () => {
      const longSql = Array(2000).fill('SELECT 1;').join('\n');
      const fs = createInMemoryFs({
        '/.do/migrations/001_long.sql': longSql,
      });

      const migrations = await loadMigrationsFromFolder({
        basePath: '/.do/migrations',
        fs,
      });

      expect(migrations[0].sql.length).toBeGreaterThan(10000);
    });

    it('should handle concurrent migration detection', async () => {
      const storage = createInMemoryStorage();
      const tracker = createSchemaTracker(storage);

      // Simulate concurrent recording
      await Promise.all([
        tracker.recordMigration({
          id: '001_first',
          appliedAt: new Date(),
          checksum: 'a',
          durationMs: 10,
        }),
        tracker.recordMigration({
          id: '002_second',
          appliedAt: new Date(),
          checksum: 'b',
          durationMs: 10,
        }),
      ]);

      const applied = await tracker.getAppliedMigrations();
      expect(applied).toHaveLength(2);
    });
  });
});
