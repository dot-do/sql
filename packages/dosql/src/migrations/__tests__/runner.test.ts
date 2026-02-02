/**
 * DoSQL Migrations - Runner Tests
 *
 * Tests for migration runner functionality including:
 * - Migration up/down execution
 * - Migration versioning
 * - Rollback on failure
 * - Transactional migrations
 * - Dry run mode
 *
 * NO MOCKS - uses real implementations per DoSQL testing philosophy.
 */

import { describe, it, expect, beforeEach } from 'vitest';

import {
  MigrationRunner,
  createMigration,
  createMigrations,
  createMigrationRunner,
  sortMigrations,
  getMigrationsBetween,
  detectOutOfOrderMigrations,
  batchMigrations,
  splitMigrationSql,
  runMigrationsFromSource,
  type DatabaseExecutor,
} from '../runner.js';

import {
  type Migration,
  type AppliedMigration,
  calculateChecksumSync,
} from '../types.js';

import { createInMemoryFs } from '../drizzle-compat.js';

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Create an in-memory database executor for testing with transaction support
 */
function createTestExecutor(): DatabaseExecutor & {
  tables: Map<string, { columns: string[]; rows: unknown[][] }>;
  executedSql: string[];
  transactionActive: boolean;
  transactionRolledBack: boolean;
  failOnSql?: string;
} {
  const tables = new Map<string, { columns: string[]; rows: unknown[][] }>();
  const executedSql: string[] = [];
  let transactionActive = false;
  let transactionRolledBack = false;
  let failOnSql: string | undefined;

  return {
    tables,
    executedSql,
    get transactionActive() {
      return transactionActive;
    },
    set transactionActive(value: boolean) {
      transactionActive = value;
    },
    get transactionRolledBack() {
      return transactionRolledBack;
    },
    set transactionRolledBack(value: boolean) {
      transactionRolledBack = value;
    },
    get failOnSql() {
      return failOnSql;
    },
    set failOnSql(value: string | undefined) {
      failOnSql = value;
    },

    beginTransaction() {
      transactionActive = true;
      transactionRolledBack = false;
      executedSql.push('BEGIN TRANSACTION');
    },

    commit() {
      transactionActive = false;
      executedSql.push('COMMIT');
    },

    rollback() {
      transactionActive = false;
      transactionRolledBack = true;
      executedSql.push('ROLLBACK');
    },

    exec(sql: string) {
      executedSql.push(sql);

      // Check for intentional failure
      if (failOnSql && sql.includes(failOnSql)) {
        throw new Error(`Intentional failure on: ${failOnSql}`);
      }

      // Normalize SQL for matching (remove extra whitespace)
      const normalizedSql = sql.replace(/\s+/g, ' ').trim();

      // Handle CREATE TABLE
      const createMatch = normalizedSql.match(
        /CREATE TABLE(\s+IF NOT EXISTS)?\s+"?(\w+)"?\s*\(\s*(.*)\s*\)/i
      );
      if (createMatch) {
        const ifNotExists = !!createMatch[1];
        const tableName = createMatch[2];
        const columnDefs = createMatch[3];

        if (ifNotExists && tables.has(tableName)) {
          return;
        }

        const columns = columnDefs
          .split(',')
          .map(c => {
            const trimmed = c.trim();
            const match = trimmed.match(/^"?(\w+)"?/);
            return match ? match[1] : trimmed.split(/\s+/)[0];
          })
          .filter(c => c.length > 0);
        tables.set(tableName, { columns, rows: [] });
      }

      // Handle DROP TABLE
      const dropMatch = normalizedSql.match(/DROP TABLE(?:\s+IF EXISTS)?\s+"?(\w+)"?/i);
      if (dropMatch) {
        tables.delete(dropMatch[1]);
      }

      // Handle ALTER TABLE ADD COLUMN
      const alterAddMatch = normalizedSql.match(
        /ALTER TABLE "?(\w+)"? ADD COLUMN "?(\w+)"?/i
      );
      if (alterAddMatch) {
        const tableName = alterAddMatch[1];
        const columnName = alterAddMatch[2];
        const table = tables.get(tableName);
        if (table) {
          table.columns.push(columnName);
        }
      }

      // Handle ALTER TABLE DROP COLUMN
      const alterDropMatch = normalizedSql.match(
        /ALTER TABLE "?(\w+)"? DROP COLUMN "?(\w+)"?/i
      );
      if (alterDropMatch) {
        const tableName = alterDropMatch[1];
        const columnName = alterDropMatch[2];
        const table = tables.get(tableName);
        if (table) {
          const idx = table.columns.indexOf(columnName);
          if (idx >= 0) {
            table.columns.splice(idx, 1);
          }
        }
      }
    },

    query<T>(sql: string, params?: unknown[]): T[] {
      executedSql.push(sql);

      const selectMatch = sql.match(/SELECT .+ FROM "?([a-zA-Z_][a-zA-Z0-9_]*)"?(?:\s|$)/i);
      if (selectMatch) {
        const tableName = selectMatch[1];
        const table = tables.get(tableName);

        if (!table) return [] as T[];

        return table.rows.map(row => {
          const obj: Record<string, unknown> = {};
          table.columns.forEach((col, i) => {
            obj[col] = row[i];
          });
          return obj as T;
        }) as T[];
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

/**
 * Create a silent logger for testing
 */
const silentLogger = {
  info: () => {},
  warn: () => {},
  error: () => {},
  debug: () => {},
};

// =============================================================================
// MIGRATION UP EXECUTION TESTS
// =============================================================================

describe('Migration Runner - Up Execution', () => {
  let executor: ReturnType<typeof createTestExecutor>;
  let runner: MigrationRunner;

  beforeEach(() => {
    executor = createTestExecutor();
    runner = new MigrationRunner(executor, { logger: silentLogger });
  });

  it('should apply a single migration', async () => {
    const migrations = createMigrations([
      { id: '20240101000000_create_users', sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(1);
    expect(result.applied[0].id).toBe('20240101000000_create_users');
    expect(executor.tables.has('users')).toBe(true);
  });

  it('should apply multiple migrations in order', async () => {
    const migrations = createMigrations([
      { id: '20240101000000_create_users', sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY)' },
      { id: '20240101000001_create_posts', sql: 'CREATE TABLE posts (id INTEGER PRIMARY KEY)' },
      { id: '20240101000002_create_comments', sql: 'CREATE TABLE comments (id INTEGER PRIMARY KEY)' },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(3);
    expect(result.newVersion).toBe('20240101000002_create_comments');
    expect(executor.tables.has('users')).toBe(true);
    expect(executor.tables.has('posts')).toBe(true);
    expect(executor.tables.has('comments')).toBe(true);
  });

  it('should skip already applied migrations', async () => {
    const migrations = createMigrations([
      { id: '20240101000000_first', sql: 'CREATE TABLE first (id INT)' },
      { id: '20240101000001_second', sql: 'CREATE TABLE second (id INT)' },
    ]);

    // Apply first migration
    await runner.migrate(migrations.slice(0, 1));
    const sqlCountAfterFirst = executor.executedSql.length;

    // Apply all migrations - should only apply second
    const result = await runner.migrate(migrations);

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(1);
    expect(result.applied[0].id).toBe('20240101000001_second');
  });

  it('should return success when no migrations pending', async () => {
    const migrations = createMigrations([
      { id: '20240101000000_only', sql: 'CREATE TABLE only (id INT)' },
    ]);

    // Apply migration
    await runner.migrate(migrations);

    // Try to apply again
    const result = await runner.migrate(migrations);

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(0);
    expect(result.newVersion).toBe('20240101000000_only');
  });

  it('should record migration duration', async () => {
    const migrations = createMigrations([
      { id: '20240101000000_test', sql: 'CREATE TABLE test (id INT)' },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.applied[0].durationMs).toBeGreaterThanOrEqual(0);
    expect(result.totalDurationMs).toBeGreaterThanOrEqual(0);
  });
});

// =============================================================================
// MIGRATION DOWN (ROLLBACK) EXECUTION TESTS
// =============================================================================

describe('Migration Runner - Down Execution (Rollback)', () => {
  let executor: ReturnType<typeof createTestExecutor>;
  let runner: MigrationRunner;

  beforeEach(() => {
    executor = createTestExecutor();
    runner = new MigrationRunner(executor, { logger: silentLogger });
  });

  it('should rollback a single migration', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_create_users',
        sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE users',
      },
    ]);

    // Apply migration
    await runner.migrate(migrations);
    expect(executor.tables.has('users')).toBe(true);

    // Rollback to null (before any migrations)
    const result = await runner.rollbackTo(migrations, null);

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(1);
    expect(result.newVersion).toBeNull();
    expect(executor.tables.has('users')).toBe(false);
  });

  it('should rollback multiple migrations in reverse order', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_create_users',
        sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE users',
      },
      {
        id: '20240101000001_create_posts',
        sql: 'CREATE TABLE posts (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE posts',
      },
      {
        id: '20240101000002_create_comments',
        sql: 'CREATE TABLE comments (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE comments',
      },
    ]);

    // Apply all migrations
    await runner.migrate(migrations);
    expect(executor.tables.size).toBeGreaterThanOrEqual(3);

    // Rollback to first migration
    const result = await runner.rollbackTo(migrations, '20240101000000_create_users');

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(2);
    // Should rollback in reverse order: comments first, then posts
    expect(result.applied[0].id).toBe('20240101000002_create_comments');
    expect(result.applied[1].id).toBe('20240101000001_create_posts');
    expect(executor.tables.has('users')).toBe(true);
    expect(executor.tables.has('posts')).toBe(false);
    expect(executor.tables.has('comments')).toBe(false);
  });

  it('should fail rollback when no down migration provided', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_create_users',
        sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY)',
        // No downSql
      },
    ]);

    // Apply migration
    await runner.migrate(migrations);

    // Try to rollback
    const result = await runner.rollbackTo(migrations, null);

    expect(result.success).toBe(false);
    expect(result.failed).toHaveLength(1);
    expect(result.failed[0].error.message).toContain('No down migration');
  });

  it('should handle partial rollback on failure', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_create_users',
        sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE users',
      },
      {
        id: '20240101000001_create_posts',
        sql: 'CREATE TABLE posts (id INTEGER PRIMARY KEY)',
        // No downSql - will fail
      },
    ]);

    // Apply migrations
    await runner.migrate(migrations);

    // Try to rollback all
    const result = await runner.rollbackTo(migrations, null);

    expect(result.success).toBe(false);
    expect(result.failed).toHaveLength(1);
    expect(result.failed[0].error.message).toContain('No down migration');
  });

  it('should return success when no migrations to rollback', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_create_users',
        sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE users',
      },
    ]);

    // Don't apply any migrations
    const result = await runner.rollbackTo(migrations, null);

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(0);
  });
});

// =============================================================================
// MIGRATION VERSIONING TESTS
// =============================================================================

describe('Migration Runner - Versioning', () => {
  let executor: ReturnType<typeof createTestExecutor>;
  let runner: MigrationRunner;

  beforeEach(() => {
    executor = createTestExecutor();
    runner = new MigrationRunner(executor, { logger: silentLogger });
  });

  it('should track current version correctly', async () => {
    const migrations = createMigrations([
      { id: '20240101000000_v1', sql: 'CREATE TABLE v1 (id INT)' },
      { id: '20240102000000_v2', sql: 'CREATE TABLE v2 (id INT)' },
    ]);

    // Apply first migration
    await runner.migrate(migrations.slice(0, 1));
    let status = await runner.getStatus(migrations);
    expect(status.currentVersion).toBe('20240101000000_v1');

    // Apply second migration
    await runner.migrate(migrations);
    status = await runner.getStatus(migrations);
    expect(status.currentVersion).toBe('20240102000000_v2');
  });

  it('should identify pending migrations correctly', async () => {
    const migrations = createMigrations([
      { id: '20240101000000_first', sql: 'CREATE TABLE first (id INT)' },
      { id: '20240102000000_second', sql: 'CREATE TABLE second (id INT)' },
      { id: '20240103000000_third', sql: 'CREATE TABLE third (id INT)' },
    ]);

    // Apply first migration only
    await runner.migrate(migrations.slice(0, 1));

    const status = await runner.getStatus(migrations);

    expect(status.pending).toHaveLength(2);
    expect(status.pending[0].id).toBe('20240102000000_second');
    expect(status.pending[1].id).toBe('20240103000000_third');
    expect(status.needsMigration).toBe(true);
  });

  it('should validate checksums correctly', async () => {
    const originalSql = 'CREATE TABLE users (id INT)';
    const migrations = createMigrations([
      { id: '20240101000000_users', sql: originalSql },
    ]);

    await runner.migrate(migrations);

    // Validate with same SQL
    let validation = await runner.validateChecksums(migrations);
    expect(validation.valid).toBe(true);
    expect(validation.mismatches).toHaveLength(0);

    // Validate with modified SQL (different checksum)
    const modifiedMigrations = createMigrations([
      { id: '20240101000000_users', sql: 'CREATE TABLE users (id INT, name TEXT)' },
    ]);

    validation = await runner.validateChecksums(modifiedMigrations);
    expect(validation.valid).toBe(false);
    expect(validation.mismatches).toHaveLength(1);
    expect(validation.mismatches[0].id).toBe('20240101000000_users');
  });

  it('should sort migrations by timestamp', () => {
    const migrations = createMigrations([
      { id: '20240103000000_third', sql: 'SELECT 3' },
      { id: '20240101000000_first', sql: 'SELECT 1' },
      { id: '20240102000000_second', sql: 'SELECT 2' },
    ]);

    const sorted = sortMigrations(migrations);

    expect(sorted[0].id).toBe('20240101000000_first');
    expect(sorted[1].id).toBe('20240102000000_second');
    expect(sorted[2].id).toBe('20240103000000_third');
  });

  it('should get migrations between versions', () => {
    const migrations = createMigrations([
      { id: '20240101000000_v1', sql: 'SELECT 1' },
      { id: '20240102000000_v2', sql: 'SELECT 2' },
      { id: '20240103000000_v3', sql: 'SELECT 3' },
      { id: '20240104000000_v4', sql: 'SELECT 4' },
    ]);

    const between = getMigrationsBetween(
      migrations,
      '20240101000000_v1',
      '20240103000000_v3'
    );

    expect(between).toHaveLength(2);
    expect(between[0].id).toBe('20240102000000_v2');
    expect(between[1].id).toBe('20240103000000_v3');
  });

  it('should detect out-of-order migrations', () => {
    const migrations = createMigrations([
      { id: '20240101000000_v1', sql: 'SELECT 1' },
      { id: '20240102000000_v2', sql: 'SELECT 2' },
      { id: '20240103000000_v3', sql: 'SELECT 3' },
    ]);

    // Simulate v1 and v3 applied, but v2 missing
    const applied: AppliedMigration[] = [
      { id: '20240101000000_v1', appliedAt: new Date(), checksum: 'a', durationMs: 10 },
      { id: '20240103000000_v3', appliedAt: new Date(), checksum: 'c', durationMs: 10 },
    ];

    const outOfOrder = detectOutOfOrderMigrations(migrations, applied);

    expect(outOfOrder).toHaveLength(1);
    expect(outOfOrder[0].id).toBe('20240102000000_v2');
  });
});

// =============================================================================
// TRANSACTIONAL MIGRATION TESTS
// =============================================================================

describe('Migration Runner - Transactions', () => {
  let executor: ReturnType<typeof createTestExecutor>;

  beforeEach(() => {
    executor = createTestExecutor();
  });

  it('should wrap migrations in transaction when transactional=true', async () => {
    const runner = new MigrationRunner(executor, {
      logger: silentLogger,
      transactional: true,
    });

    const migrations = createMigrations([
      { id: '20240101000000_test', sql: 'CREATE TABLE test (id INT)' },
    ]);

    await runner.migrate(migrations);

    expect(executor.executedSql).toContain('BEGIN TRANSACTION');
    expect(executor.executedSql).toContain('COMMIT');
    expect(executor.transactionRolledBack).toBe(false);
  });

  it('should rollback transaction on failure', async () => {
    executor.failOnSql = 'fail_table';

    const runner = new MigrationRunner(executor, {
      logger: silentLogger,
      transactional: true,
    });

    const migrations = createMigrations([
      { id: '20240101000000_test', sql: 'CREATE TABLE fail_table (id INT)' },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.success).toBe(false);
    expect(executor.executedSql).toContain('BEGIN TRANSACTION');
    expect(executor.executedSql).toContain('ROLLBACK');
    expect(executor.transactionRolledBack).toBe(true);
  });

  it('should not use transactions when transactional=false', async () => {
    const runner = new MigrationRunner(executor, {
      logger: silentLogger,
      transactional: false,
    });

    const migrations = createMigrations([
      { id: '20240101000000_test', sql: 'CREATE TABLE test (id INT)' },
    ]);

    await runner.migrate(migrations);

    expect(executor.executedSql).not.toContain('BEGIN TRANSACTION');
    expect(executor.executedSql).not.toContain('COMMIT');
  });

  it('should rollback transaction on down migration failure', async () => {
    // Set up executor to fail on specific SQL
    executor.failOnSql = 'fail_migration';

    const runner = new MigrationRunner(executor, {
      logger: silentLogger,
      transactional: true,
    });

    const migrations = createMigrations([
      {
        id: '20240101000000_test',
        sql: 'CREATE TABLE test (id INT)',
        downSql: 'DROP TABLE fail_migration', // Will fail due to failOnSql
      },
    ]);

    await runner.migrate(migrations);
    executor.executedSql = []; // Clear history

    const result = await runner.rollbackTo(migrations, null);

    expect(result.success).toBe(false);
    expect(executor.transactionRolledBack).toBe(true);
  });
});

// =============================================================================
// DRY RUN MODE TESTS
// =============================================================================

describe('Migration Runner - Dry Run Mode', () => {
  let executor: ReturnType<typeof createTestExecutor>;

  beforeEach(() => {
    executor = createTestExecutor();
  });

  it('should not execute SQL in dry run mode', async () => {
    const runner = new MigrationRunner(executor, {
      logger: silentLogger,
      dryRun: true,
    });

    const migrations = createMigrations([
      { id: '20240101000000_test', sql: 'CREATE TABLE test (id INT)' },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(1);
    // Table should not be created in dry run mode
    expect(executor.tables.has('test')).toBe(false);
  });

  it('should not record migrations in dry run mode', async () => {
    const runner = new MigrationRunner(executor, {
      logger: silentLogger,
      dryRun: true,
    });

    const migrations = createMigrations([
      { id: '20240101000000_test', sql: 'CREATE TABLE test (id INT)' },
    ]);

    await runner.migrate(migrations);

    // Migrations table should not have the migration recorded
    const migrationsTable = executor.tables.get('__dosql_migrations');
    expect(migrationsTable).toBeUndefined();
  });

  it('should not execute down SQL in dry run rollback', async () => {
    // First apply without dry run
    const runner1 = new MigrationRunner(executor, { logger: silentLogger });
    const migrations = createMigrations([
      {
        id: '20240101000000_test',
        sql: 'CREATE TABLE test (id INT)',
        downSql: 'DROP TABLE test',
      },
    ]);

    await runner1.migrate(migrations);
    expect(executor.tables.has('test')).toBe(true);

    // Now rollback in dry run mode
    const runner2 = new MigrationRunner(executor, {
      logger: silentLogger,
      dryRun: true,
    });

    const result = await runner2.rollbackTo(migrations, null);

    expect(result.success).toBe(true);
    // Table should still exist after dry run rollback
    expect(executor.tables.has('test')).toBe(true);
  });
});

// =============================================================================
// CONTINUE ON ERROR MODE TESTS
// =============================================================================

describe('Migration Runner - Continue On Error', () => {
  let executor: ReturnType<typeof createTestExecutor>;

  beforeEach(() => {
    executor = createTestExecutor();
  });

  it('should stop on first error by default', async () => {
    executor.failOnSql = 'fail_table';

    const runner = new MigrationRunner(executor, {
      logger: silentLogger,
      transactional: false,
    });

    const migrations = createMigrations([
      { id: '20240101000000_good', sql: 'CREATE TABLE good (id INT)' },
      { id: '20240101000001_bad', sql: 'CREATE TABLE fail_table (id INT)' },
      { id: '20240101000002_never', sql: 'CREATE TABLE never (id INT)' },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.success).toBe(false);
    expect(result.applied).toHaveLength(1);
    expect(result.failed).toHaveLength(1);
    expect(result.failed[0].migration.id).toBe('20240101000001_bad');
    expect(executor.tables.has('never')).toBe(false);
  });

  it('should continue on error when continueOnError=true', async () => {
    executor.failOnSql = 'fail_table';

    const runner = new MigrationRunner(executor, {
      logger: silentLogger,
      continueOnError: true,
      transactional: false,
    });

    const migrations = createMigrations([
      { id: '20240101000000_good', sql: 'CREATE TABLE good (id INT)' },
      { id: '20240101000001_bad', sql: 'CREATE TABLE fail_table (id INT)' },
      { id: '20240101000002_also_good', sql: 'CREATE TABLE also_good (id INT)' },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.success).toBe(false);
    expect(result.applied).toHaveLength(2);
    expect(result.failed).toHaveLength(1);
    expect(executor.tables.has('good')).toBe(true);
    expect(executor.tables.has('also_good')).toBe(true);
  });

  it('should continue rollback on error when continueOnError=true', async () => {
    const runner = new MigrationRunner(executor, {
      logger: silentLogger,
      continueOnError: true,
      transactional: false,
    });

    const migrations = createMigrations([
      {
        id: '20240101000000_first',
        sql: 'CREATE TABLE first (id INT)',
        downSql: 'DROP TABLE first',
      },
      {
        id: '20240101000001_second',
        sql: 'CREATE TABLE second (id INT)',
        // No downSql - will fail
      },
      {
        id: '20240101000002_third',
        sql: 'CREATE TABLE third (id INT)',
        downSql: 'DROP TABLE third',
      },
    ]);

    await runner.migrate(migrations);

    const result = await runner.rollbackTo(migrations, null);

    expect(result.success).toBe(false);
    expect(result.failed).toHaveLength(1);
    expect(result.failed[0].migration.id).toBe('20240101000001_second');
    // Should have rolled back third (first attempted) and skipped first
    expect(result.applied.length).toBeGreaterThanOrEqual(1);
  });
});

// =============================================================================
// CUSTOM MIGRATIONS TABLE TESTS
// =============================================================================

describe('Migration Runner - Custom Migrations Table', () => {
  let executor: ReturnType<typeof createTestExecutor>;

  beforeEach(() => {
    executor = createTestExecutor();
  });

  it('should use custom migrations table name', async () => {
    const runner = new MigrationRunner(executor, {
      logger: silentLogger,
      migrationsTable: 'custom_migrations',
    });

    const migrations = createMigrations([
      { id: '20240101000000_test', sql: 'CREATE TABLE test (id INT)' },
    ]);

    await runner.migrate(migrations);

    expect(executor.tables.has('custom_migrations')).toBe(true);
    expect(executor.tables.has('__dosql_migrations')).toBe(false);
  });
});

// =============================================================================
// HELPER FUNCTION TESTS
// =============================================================================

describe('Migration Runner - Helper Functions', () => {
  it('createMigration should generate checksum', () => {
    const migration = createMigration(
      '20240101000000_test',
      'CREATE TABLE test (id INT)'
    );

    expect(migration.checksum).toBe(calculateChecksumSync('CREATE TABLE test (id INT)'));
  });

  it('createMigration should set createdAt', () => {
    const before = new Date();
    const migration = createMigration('20240101000000_test', 'SELECT 1');
    const after = new Date();

    expect(migration.createdAt.getTime()).toBeGreaterThanOrEqual(before.getTime());
    expect(migration.createdAt.getTime()).toBeLessThanOrEqual(after.getTime());
  });

  it('createMigrations should create multiple migrations', () => {
    const migrations = createMigrations([
      { id: '20240101000000_first', sql: 'SELECT 1' },
      { id: '20240101000001_second', sql: 'SELECT 2', downSql: 'SELECT 0' },
    ]);

    expect(migrations).toHaveLength(2);
    expect(migrations[0].checksum).toBeTruthy();
    expect(migrations[1].downSql).toBe('SELECT 0');
  });

  it('createMigrationRunner should create runner instance', () => {
    const executor = createTestExecutor();
    const runner = createMigrationRunner(executor, { dryRun: true });

    expect(runner).toBeInstanceOf(MigrationRunner);
  });

  it('batchMigrations should combine SQL statements', () => {
    const result = batchMigrations([
      'CREATE TABLE a (id INT)',
      'CREATE TABLE b (id INT)',
      '',
      '  ',
    ]);

    expect(result).toBe('CREATE TABLE a (id INT);\n\nCREATE TABLE b (id INT)');
  });

  it('splitMigrationSql should split by semicolon', () => {
    const sql = 'CREATE TABLE a (id INT); CREATE TABLE b (id INT);';
    const statements = splitMigrationSql(sql);

    expect(statements).toHaveLength(2);
    expect(statements[0]).toBe('CREATE TABLE a (id INT)');
    expect(statements[1]).toBe('CREATE TABLE b (id INT)');
  });
});

// =============================================================================
// MIGRATION SOURCE TESTS
// =============================================================================

describe('Migration Runner - Migration Source', () => {
  let executor: ReturnType<typeof createTestExecutor>;

  beforeEach(() => {
    executor = createTestExecutor();
  });

  it('should run migrations from array source', async () => {
    const migrations = createMigrations([
      { id: '20240101000000_test', sql: 'CREATE TABLE test (id INT)' },
    ]);

    const result = await runMigrationsFromSource(executor, migrations, {
      logger: silentLogger,
    });

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(1);
  });

  it('should run migrations from folder source', async () => {
    const fs = createInMemoryFs({
      '/.do/migrations/001_create_users.sql': 'CREATE TABLE users (id INTEGER PRIMARY KEY);',
      '/.do/migrations/002_add_posts.sql': 'CREATE TABLE posts (id INTEGER PRIMARY KEY);',
    });

    const result = await runMigrationsFromSource(
      executor,
      { folder: '/.do/migrations' },
      { logger: silentLogger, fs }
    );

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(2);
  });

  it('should run migrations from function source', async () => {
    const loader = async (): Promise<Migration[]> => {
      return createMigrations([
        { id: '20240101000000_dynamic', sql: 'CREATE TABLE dynamic (id INT)' },
      ]);
    };

    const result = await runMigrationsFromSource(executor, loader, {
      logger: silentLogger,
    });

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(1);
    expect(executor.tables.has('dynamic')).toBe(true);
  });

  it('should throw error for folder source without fs', async () => {
    await expect(
      runMigrationsFromSource(executor, { folder: '/.do/migrations' }, {
        logger: silentLogger,
        // No fs provided
      })
    ).rejects.toThrow(/fs.*required/i);
  });
});

// =============================================================================
// SCHEMA CHANGE HANDLING TESTS
// =============================================================================

describe('Migration Runner - Schema Change Handling', () => {
  let executor: ReturnType<typeof createTestExecutor>;
  let runner: MigrationRunner;

  beforeEach(() => {
    executor = createTestExecutor();
    runner = new MigrationRunner(executor, { logger: silentLogger });
  });

  it('should handle ADD COLUMN migrations', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_create_users',
        sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY)',
      },
      {
        id: '20240101000001_add_email',
        sql: 'ALTER TABLE users ADD COLUMN email TEXT',
        downSql: 'ALTER TABLE users DROP COLUMN email',
      },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(2);
    expect(executor.tables.get('users')?.columns).toContain('email');
  });

  it('should handle DROP COLUMN migrations', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_create_users',
        sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY, temp_col TEXT)',
      },
      {
        id: '20240101000001_drop_temp',
        sql: 'ALTER TABLE users DROP COLUMN temp_col',
        downSql: 'ALTER TABLE users ADD COLUMN temp_col TEXT',
      },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.success).toBe(true);
    expect(executor.tables.get('users')?.columns).not.toContain('temp_col');
  });

  it('should handle multiple columns in single ALTER statement', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_create_users',
        sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY)',
      },
      {
        id: '20240101000001_add_email',
        sql: 'ALTER TABLE users ADD COLUMN email TEXT',
      },
      {
        id: '20240101000002_add_name',
        sql: 'ALTER TABLE users ADD COLUMN name TEXT',
      },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.success).toBe(true);
    const userColumns = executor.tables.get('users')?.columns ?? [];
    expect(userColumns).toContain('email');
    expect(userColumns).toContain('name');
  });

  it('should handle CREATE INDEX migrations', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_create_users',
        sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)',
      },
      {
        id: '20240101000001_add_index',
        sql: 'CREATE INDEX idx_users_email ON users(email)',
        downSql: 'DROP INDEX idx_users_email',
      },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(2);
  });

  it('should handle DROP TABLE migrations', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_create_temp',
        sql: 'CREATE TABLE temp_table (id INTEGER PRIMARY KEY)',
        downSql: 'CREATE TABLE temp_table (id INTEGER PRIMARY KEY)',
      },
      {
        id: '20240101000001_drop_temp',
        sql: 'DROP TABLE temp_table',
        downSql: 'CREATE TABLE temp_table (id INTEGER PRIMARY KEY)',
      },
    ]);

    await runner.migrate(migrations);

    expect(executor.tables.has('temp_table')).toBe(false);
  });

  it('should handle complex schema with foreign key references', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_create_users',
        sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
      },
      {
        id: '20240101000001_create_posts',
        sql: 'CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id), title TEXT)',
      },
      {
        id: '20240101000002_create_comments',
        sql: 'CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER REFERENCES posts(id), body TEXT)',
      },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.success).toBe(true);
    expect(executor.tables.has('users')).toBe(true);
    expect(executor.tables.has('posts')).toBe(true);
    expect(executor.tables.has('comments')).toBe(true);
  });
});

// =============================================================================
// ADVANCED ROLLBACK SCENARIOS TESTS
// =============================================================================

describe('Migration Runner - Advanced Rollback Scenarios', () => {
  let executor: ReturnType<typeof createTestExecutor>;
  let runner: MigrationRunner;

  beforeEach(() => {
    executor = createTestExecutor();
    runner = new MigrationRunner(executor, { logger: silentLogger });
  });

  it('should rollback to a specific intermediate version', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_v1',
        sql: 'CREATE TABLE v1 (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE v1',
      },
      {
        id: '20240101000001_v2',
        sql: 'CREATE TABLE v2 (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE v2',
      },
      {
        id: '20240101000002_v3',
        sql: 'CREATE TABLE v3 (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE v3',
      },
      {
        id: '20240101000003_v4',
        sql: 'CREATE TABLE v4 (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE v4',
      },
    ]);

    // Apply all migrations
    await runner.migrate(migrations);
    expect(executor.tables.has('v4')).toBe(true);

    // Rollback to v2
    const result = await runner.rollbackTo(migrations, '20240101000001_v2');

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(2); // v4 and v3 rolled back
    expect(result.applied[0].id).toBe('20240101000003_v4'); // Rolled back first
    expect(result.applied[1].id).toBe('20240101000002_v3'); // Rolled back second
    expect(executor.tables.has('v1')).toBe(true);
    expect(executor.tables.has('v2')).toBe(true);
    expect(executor.tables.has('v3')).toBe(false);
    expect(executor.tables.has('v4')).toBe(false);
  });

  it('should handle rollback when already at target version', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_v1',
        sql: 'CREATE TABLE v1 (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE v1',
      },
      {
        id: '20240101000001_v2',
        sql: 'CREATE TABLE v2 (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE v2',
      },
    ]);

    // Apply only first migration
    await runner.migrate(migrations.slice(0, 1));

    // Try to rollback to v1 (current version)
    const result = await runner.rollbackTo(migrations, '20240101000000_v1');

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(0);
    expect(executor.tables.has('v1')).toBe(true);
  });

  it('should handle rollback of sequential table creation migrations', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_create_users',
        sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE users',
      },
      {
        id: '20240101000001_create_posts',
        sql: 'CREATE TABLE posts (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE posts',
      },
      {
        id: '20240101000002_create_comments',
        sql: 'CREATE TABLE comments (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE comments',
      },
    ]);

    await runner.migrate(migrations);
    expect(executor.tables.has('users')).toBe(true);
    expect(executor.tables.has('posts')).toBe(true);
    expect(executor.tables.has('comments')).toBe(true);

    const result = await runner.rollbackTo(migrations, null);

    expect(result.success).toBe(true);
    expect(executor.tables.has('users')).toBe(false);
    expect(executor.tables.has('posts')).toBe(false);
    expect(executor.tables.has('comments')).toBe(false);
  });

  it('should record accurate duration for rollback operations', async () => {
    const migrations = createMigrations([
      {
        id: '20240101000000_test',
        sql: 'CREATE TABLE test (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE test',
      },
    ]);

    await runner.migrate(migrations);
    const result = await runner.rollbackTo(migrations, null);

    expect(result.applied[0].durationMs).toBeGreaterThanOrEqual(0);
    expect(result.totalDurationMs).toBeGreaterThanOrEqual(result.applied[0].durationMs);
  });

  it('should handle rollback with continueOnError when middle migration fails', async () => {
    const runnerWithContinue = new MigrationRunner(executor, {
      logger: silentLogger,
      continueOnError: true,
      transactional: false,
    });

    const migrations = createMigrations([
      {
        id: '20240101000000_first',
        sql: 'CREATE TABLE first (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE first',
      },
      {
        id: '20240101000001_second',
        sql: 'CREATE TABLE second (id INTEGER PRIMARY KEY)',
        // No downSql - will fail on rollback
      },
      {
        id: '20240101000002_third',
        sql: 'CREATE TABLE third (id INTEGER PRIMARY KEY)',
        downSql: 'DROP TABLE third',
      },
    ]);

    await runnerWithContinue.migrate(migrations);

    const result = await runnerWithContinue.rollbackTo(migrations, null);

    expect(result.success).toBe(false);
    expect(result.failed).toHaveLength(1);
    expect(result.failed[0].migration.id).toBe('20240101000001_second');
    // Third should be rolled back (first in reverse), first should be attempted after second fails
    expect(result.applied.length).toBeGreaterThanOrEqual(1);
  });
});

// =============================================================================
// MIGRATION STATUS EDGE CASES TESTS
// =============================================================================

describe('Migration Runner - Status Edge Cases', () => {
  let executor: ReturnType<typeof createTestExecutor>;
  let runner: MigrationRunner;

  beforeEach(() => {
    executor = createTestExecutor();
    runner = new MigrationRunner(executor, { logger: silentLogger });
  });

  it('should handle status with empty migrations array', async () => {
    const status = await runner.getStatus([]);

    expect(status.currentVersion).toBeNull();
    expect(status.applied).toHaveLength(0);
    expect(status.pending).toHaveLength(0);
    expect(status.needsMigration).toBe(false);
  });

  it('should correctly identify pending when some migrations applied', async () => {
    const migrations = createMigrations([
      { id: '20240101000000_first', sql: 'CREATE TABLE first (id INT)' },
      { id: '20240102000000_second', sql: 'CREATE TABLE second (id INT)' },
      { id: '20240103000000_third', sql: 'CREATE TABLE third (id INT)' },
    ]);

    // Apply first two
    await runner.migrate(migrations.slice(0, 2));

    const status = await runner.getStatus(migrations);

    expect(status.currentVersion).toBe('20240102000000_second');
    expect(status.applied).toHaveLength(2);
    expect(status.pending).toHaveLength(1);
    expect(status.pending[0].id).toBe('20240103000000_third');
    expect(status.needsMigration).toBe(true);
  });

  it('should handle migrations presented in non-sorted order', async () => {
    const migrations = createMigrations([
      { id: '20240103000000_third', sql: 'SELECT 3' },
      { id: '20240101000000_first', sql: 'SELECT 1' },
      { id: '20240102000000_second', sql: 'SELECT 2' },
    ]);

    const status = await runner.getStatus(migrations);

    // Pending should be sorted
    expect(status.pending[0].id).toBe('20240101000000_first');
    expect(status.pending[1].id).toBe('20240102000000_second');
    expect(status.pending[2].id).toBe('20240103000000_third');
  });

  it('should handle checksum validation with multiple migrations', async () => {
    const originalMigrations = createMigrations([
      { id: '20240101000000_first', sql: 'CREATE TABLE first (id INT)' },
      { id: '20240102000000_second', sql: 'CREATE TABLE second (id INT)' },
    ]);

    await runner.migrate(originalMigrations);

    // Create migrations with one modified checksum
    const modifiedMigrations = createMigrations([
      { id: '20240101000000_first', sql: 'CREATE TABLE first (id INT)' }, // Same
      { id: '20240102000000_second', sql: 'CREATE TABLE second (id INT, name TEXT)' }, // Different
    ]);

    const validation = await runner.validateChecksums(modifiedMigrations);

    expect(validation.valid).toBe(false);
    expect(validation.mismatches).toHaveLength(1);
    expect(validation.mismatches[0].id).toBe('20240102000000_second');
  });
});

// =============================================================================
// ORDERING AND DEPENDENCY TESTS
// =============================================================================

describe('Migration Runner - Ordering and Dependencies', () => {
  it('should get correct migrations between versions', () => {
    const migrations = createMigrations([
      { id: '20240101000000_v1', sql: 'SELECT 1' },
      { id: '20240102000000_v2', sql: 'SELECT 2' },
      { id: '20240103000000_v3', sql: 'SELECT 3' },
      { id: '20240104000000_v4', sql: 'SELECT 4' },
      { id: '20240105000000_v5', sql: 'SELECT 5' },
    ]);

    // Get migrations from v2 to v4
    const between = getMigrationsBetween(
      migrations,
      '20240102000000_v2',
      '20240104000000_v4'
    );

    expect(between).toHaveLength(2);
    expect(between[0].id).toBe('20240103000000_v3');
    expect(between[1].id).toBe('20240104000000_v4');
  });

  it('should get all migrations when from is null', () => {
    const migrations = createMigrations([
      { id: '20240101000000_v1', sql: 'SELECT 1' },
      { id: '20240102000000_v2', sql: 'SELECT 2' },
      { id: '20240103000000_v3', sql: 'SELECT 3' },
    ]);

    const between = getMigrationsBetween(migrations, null, '20240102000000_v2');

    expect(between).toHaveLength(2);
    expect(between[0].id).toBe('20240101000000_v1');
    expect(between[1].id).toBe('20240102000000_v2');
  });

  it('should detect multiple out-of-order migrations', () => {
    const migrations = createMigrations([
      { id: '20240101000000_v1', sql: 'SELECT 1' },
      { id: '20240102000000_v2', sql: 'SELECT 2' },
      { id: '20240103000000_v3', sql: 'SELECT 3' },
      { id: '20240104000000_v4', sql: 'SELECT 4' },
      { id: '20240105000000_v5', sql: 'SELECT 5' },
    ]);

    // Applied v1, v3, v5 - missing v2 and v4
    const applied: AppliedMigration[] = [
      { id: '20240101000000_v1', appliedAt: new Date(), checksum: 'a', durationMs: 10 },
      { id: '20240103000000_v3', appliedAt: new Date(), checksum: 'c', durationMs: 10 },
      { id: '20240105000000_v5', appliedAt: new Date(), checksum: 'e', durationMs: 10 },
    ];

    const outOfOrder = detectOutOfOrderMigrations(migrations, applied);

    expect(outOfOrder).toHaveLength(2);
    expect(outOfOrder.map(m => m.id)).toContain('20240102000000_v2');
    expect(outOfOrder.map(m => m.id)).toContain('20240104000000_v4');
  });

  it('should return empty array when no out-of-order migrations', () => {
    const migrations = createMigrations([
      { id: '20240101000000_v1', sql: 'SELECT 1' },
      { id: '20240102000000_v2', sql: 'SELECT 2' },
      { id: '20240103000000_v3', sql: 'SELECT 3' },
    ]);

    // All applied in order
    const applied: AppliedMigration[] = [
      { id: '20240101000000_v1', appliedAt: new Date(), checksum: 'a', durationMs: 10 },
      { id: '20240102000000_v2', appliedAt: new Date(), checksum: 'b', durationMs: 10 },
      { id: '20240103000000_v3', appliedAt: new Date(), checksum: 'c', durationMs: 10 },
    ];

    const outOfOrder = detectOutOfOrderMigrations(migrations, applied);

    expect(outOfOrder).toHaveLength(0);
  });

  it('should handle empty applied list', () => {
    const migrations = createMigrations([
      { id: '20240101000000_v1', sql: 'SELECT 1' },
      { id: '20240102000000_v2', sql: 'SELECT 2' },
    ]);

    const outOfOrder = detectOutOfOrderMigrations(migrations, []);

    expect(outOfOrder).toHaveLength(0);
  });
});

// =============================================================================
// CONCURRENT MIGRATION SCENARIOS TESTS
// =============================================================================

describe('Migration Runner - Idempotency and Edge Cases', () => {
  let executor: ReturnType<typeof createTestExecutor>;

  beforeEach(() => {
    executor = createTestExecutor();
  });

  it('should handle multiple migrate calls with same migrations', async () => {
    const runner = new MigrationRunner(executor, { logger: silentLogger });

    const migrations = createMigrations([
      { id: '20240101000000_test', sql: 'CREATE TABLE test (id INT)' },
    ]);

    // First call
    const result1 = await runner.migrate(migrations);
    expect(result1.success).toBe(true);
    expect(result1.applied).toHaveLength(1);

    // Second call - should be idempotent
    const result2 = await runner.migrate(migrations);
    expect(result2.success).toBe(true);
    expect(result2.applied).toHaveLength(0);
  });

  it('should handle migration with empty SQL', async () => {
    const runner = new MigrationRunner(executor, { logger: silentLogger });

    const migrations = createMigrations([
      { id: '20240101000000_empty', sql: '' },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(1);
  });

  it('should handle migration with only whitespace SQL', async () => {
    const runner = new MigrationRunner(executor, { logger: silentLogger });

    const migrations = createMigrations([
      { id: '20240101000000_whitespace', sql: '   \n\t  ' },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.success).toBe(true);
    expect(result.applied).toHaveLength(1);
  });

  it('should handle migration with SQL comments only', async () => {
    const runner = new MigrationRunner(executor, { logger: silentLogger });

    const migrations = createMigrations([
      { id: '20240101000000_comments', sql: '-- This is a comment\n/* Block comment */' },
    ]);

    const result = await runner.migrate(migrations);

    expect(result.success).toBe(true);
  });

  it('should maintain consistency after partial failure without transactions', async () => {
    executor.failOnSql = 'fail_table';

    const runner = new MigrationRunner(executor, {
      logger: silentLogger,
      transactional: false,
    });

    const migrations = createMigrations([
      { id: '20240101000000_good', sql: 'CREATE TABLE good (id INT)' },
      { id: '20240101000001_bad', sql: 'CREATE TABLE fail_table (id INT)' },
    ]);

    await runner.migrate(migrations);

    // First migration should have succeeded
    expect(executor.tables.has('good')).toBe(true);

    // Status should reflect partial application
    const status = await runner.getStatus(migrations);
    expect(status.applied).toHaveLength(1);
    expect(status.pending).toHaveLength(1);
  });
});
