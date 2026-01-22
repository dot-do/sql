/**
 * DoSQL Migrations - Test Suite
 *
 * Tests for migration types, runner, Drizzle compatibility,
 * and schema tracking using workers-vitest-pool.
 *
 * NO MOCKS - all tests use real implementations.
 */

import { describe, it, expect, beforeEach } from 'vitest';

import {
  type Migration,
  type MigrationSnapshot,
  calculateChecksumSync,
  compareMigrationIds,
  extractTimestamp,
  generateMigrationId,
  isMigration,
  isMigrationArray,
  isFolderSource,
  isDrizzleSource,
  isLoaderFunction,
} from './types.js';

import {
  MigrationRunner,
  createMigration,
  createMigrations,
  sortMigrations,
  getMigrationsBetween,
  detectOutOfOrderMigrations,
  batchMigrations,
  splitMigrationSql,
  type DatabaseExecutor,
} from './runner.js';

import {
  parseMigrationFolderName,
  parseSnapshotJson,
  parseJournalJson,
  loadDrizzleMigrations,
  createInMemoryFs,
  generateDownMigration,
  parseDrizzleConfig,
  drizzleIdToDoSqlId,
} from './drizzle-compat.js';

import {
  SchemaTracker,
  createSchemaTracker,
  createInMemoryStorage,
  initializeWithMigrations,
  prepareClone,
  isCloneReady,
  createMigrationFromSql,
  createReversibleMigration,
  type SchemaStorage,
} from './schema-tracker.js';

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
    },

    query<T>(sql: string, params?: unknown[]): T[] {
      executedSql.push(sql);

      // Handle SELECT from migrations table
      // The regex needs to handle table names like __dosql_migrations
      // Also handle ORDER BY, WHERE, etc. after table name
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
// TYPES TESTS
// =============================================================================

describe('Migration Types', () => {
  describe('calculateChecksumSync', () => {
    it('should generate consistent checksums', () => {
      const sql = 'CREATE TABLE users (id INTEGER PRIMARY KEY)';
      const checksum1 = calculateChecksumSync(sql);
      const checksum2 = calculateChecksumSync(sql);

      expect(checksum1).toBe(checksum2);
      expect(checksum1.length).toBeGreaterThan(0);
    });

    it('should generate different checksums for different SQL', () => {
      const sql1 = 'CREATE TABLE users (id INTEGER)';
      const sql2 = 'CREATE TABLE posts (id INTEGER)';

      expect(calculateChecksumSync(sql1)).not.toBe(calculateChecksumSync(sql2));
    });
  });

  describe('compareMigrationIds', () => {
    it('should compare timestamp-based IDs correctly', () => {
      const id1 = '20240101120000_first';
      const id2 = '20240101120001_second';
      const id3 = '20240102000000_third';

      expect(compareMigrationIds(id1, id2)).toBeLessThan(0);
      expect(compareMigrationIds(id2, id1)).toBeGreaterThan(0);
      expect(compareMigrationIds(id1, id3)).toBeLessThan(0);
      expect(compareMigrationIds(id1, id1)).toBe(0);
    });
  });

  describe('extractTimestamp', () => {
    it('should extract timestamp from migration ID', () => {
      expect(extractTimestamp('20240101120000_migration_name')).toBe('20240101120000');
      expect(extractTimestamp('20240101120000')).toBe('20240101120000');
    });
  });

  describe('generateMigrationId', () => {
    it('should generate valid migration ID', () => {
      const id = generateMigrationId('test_migration');

      expect(id).toMatch(/^\d{14}_test_migration$/);
    });

    it('should generate ID without name', () => {
      const id = generateMigrationId();

      expect(id).toMatch(/^\d{14}$/);
    });
  });

  describe('Type Guards', () => {
    it('should identify Migration objects', () => {
      const migration: Migration = {
        id: '20240101120000_test',
        sql: 'CREATE TABLE test (id INT)',
        createdAt: new Date(),
        checksum: 'abc123',
      };

      expect(isMigration(migration)).toBe(true);
      expect(isMigration({})).toBe(false);
      expect(isMigration(null)).toBe(false);
    });

    it('should identify migration array sources', () => {
      const migrations: Migration[] = [];
      expect(isMigrationArray(migrations)).toBe(true);
      expect(isMigrationArray({ folder: '/path' })).toBe(false);
    });

    it('should identify folder sources', () => {
      expect(isFolderSource({ folder: '/path' })).toBe(true);
      expect(isFolderSource({ drizzle: '/path' })).toBe(false);
    });

    it('should identify Drizzle sources', () => {
      expect(isDrizzleSource({ drizzle: '/path' })).toBe(true);
      expect(isDrizzleSource({ folder: '/path' })).toBe(false);
    });

    it('should identify loader functions', () => {
      expect(isLoaderFunction(async () => [])).toBe(true);
      expect(isLoaderFunction({ folder: '/path' })).toBe(false);
    });
  });
});

// =============================================================================
// RUNNER TESTS
// =============================================================================

describe('Migration Runner', () => {
  let executor: ReturnType<typeof createTestExecutor>;
  let runner: MigrationRunner;

  beforeEach(() => {
    executor = createTestExecutor();
    runner = new MigrationRunner(executor, {
      logger: {
        info: () => {},
        warn: () => {},
        error: () => {},
        debug: () => {},
      },
    });
  });

  describe('createMigration', () => {
    it('should create a valid migration', () => {
      const migration = createMigration(
        '20240101120000_create_users',
        'CREATE TABLE users (id INTEGER PRIMARY KEY)'
      );

      expect(migration.id).toBe('20240101120000_create_users');
      expect(migration.sql).toContain('CREATE TABLE');
      expect(migration.checksum).toBeTruthy();
      expect(migration.createdAt).toBeInstanceOf(Date);
    });

    it('should create migration with down SQL', () => {
      const migration = createMigration(
        '20240101120000_create_users',
        'CREATE TABLE users (id INTEGER)',
        { downSql: 'DROP TABLE users' }
      );

      expect(migration.downSql).toBe('DROP TABLE users');
    });
  });

  describe('createMigrations', () => {
    it('should create multiple migrations', () => {
      const migrations = createMigrations([
        { id: '20240101000000_first', sql: 'CREATE TABLE a (id INT)' },
        { id: '20240101000001_second', sql: 'CREATE TABLE b (id INT)' },
      ]);

      expect(migrations).toHaveLength(2);
      expect(migrations[0].id).toBe('20240101000000_first');
      expect(migrations[1].id).toBe('20240101000001_second');
    });
  });

  describe('sortMigrations', () => {
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
  });

  describe('getMigrationsBetween', () => {
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

    it('should get all migrations from null', () => {
      const migrations = createMigrations([
        { id: '20240101000000_v1', sql: 'SELECT 1' },
        { id: '20240102000000_v2', sql: 'SELECT 2' },
      ]);

      const between = getMigrationsBetween(migrations, null, '20240102000000_v2');

      expect(between).toHaveLength(2);
    });
  });

  describe('getStatus', () => {
    it('should return pending migrations when none applied', async () => {
      const migrations = createMigrations([
        { id: '20240101000000_first', sql: 'CREATE TABLE test (id INT)' },
      ]);

      const status = await runner.getStatus(migrations);

      expect(status.currentVersion).toBeNull();
      expect(status.applied).toHaveLength(0);
      expect(status.pending).toHaveLength(1);
      expect(status.needsMigration).toBe(true);
    });
  });

  describe('migrate', () => {
    it('should apply pending migrations', async () => {
      const migrations = createMigrations([
        { id: '20240101000000_create_users', sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
        { id: '20240101000001_create_posts', sql: 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)' },
      ]);

      const result = await runner.migrate(migrations);

      expect(result.success).toBe(true);
      expect(result.applied).toHaveLength(2);
      expect(result.failed).toHaveLength(0);
      expect(executor.tables.has('users')).toBe(true);
      expect(executor.tables.has('posts')).toBe(true);
    });

    it('should not reapply already applied migrations', async () => {
      const migrations = createMigrations([
        { id: '20240101000000_test', sql: 'CREATE TABLE test (id INT)' },
      ]);

      // First run - applies migration and records it
      const firstResult = await runner.migrate(migrations);
      expect(firstResult.success).toBe(true);
      expect(firstResult.applied).toHaveLength(1);

      // Verify migration table has data
      const migrationsTable = executor.tables.get('__dosql_migrations');
      expect(migrationsTable).toBeDefined();
      expect(migrationsTable?.rows).toHaveLength(1);

      // Verify migration was recorded in tracking table
      const status = await runner.getStatus(migrations);
      expect(status.applied).toHaveLength(1);
      expect(status.pending).toHaveLength(0);
      expect(status.needsMigration).toBe(false);

      // Second run with same runner - should skip because already applied
      const result = await runner.migrate(migrations);

      expect(result.success).toBe(true);
      expect(result.applied).toHaveLength(0);
    });
  });

  describe('batchMigrations', () => {
    it('should combine SQL statements', () => {
      const result = batchMigrations([
        'CREATE TABLE a (id INT)',
        'CREATE TABLE b (id INT)',
        '',
        '  ',
      ]);

      expect(result).toBe('CREATE TABLE a (id INT);\n\nCREATE TABLE b (id INT)');
    });
  });

  describe('splitMigrationSql', () => {
    it('should split SQL by semicolon', () => {
      const sql = 'CREATE TABLE a (id INT); CREATE TABLE b (id INT);';
      const statements = splitMigrationSql(sql);

      expect(statements).toHaveLength(2);
      expect(statements[0]).toBe('CREATE TABLE a (id INT)');
    });
  });

  describe('detectOutOfOrderMigrations', () => {
    it('should detect out-of-order migrations', () => {
      const migrations = createMigrations([
        { id: '20240101000000_v1', sql: 'SELECT 1' },
        { id: '20240102000000_v2', sql: 'SELECT 2' },
        { id: '20240103000000_v3', sql: 'SELECT 3' },
      ]);

      // v1 and v3 applied, but v2 missing (out of order)
      const applied = [
        { id: '20240101000000_v1', appliedAt: new Date(), checksum: 'a', durationMs: 10 },
        { id: '20240103000000_v3', appliedAt: new Date(), checksum: 'c', durationMs: 10 },
      ];

      const outOfOrder = detectOutOfOrderMigrations(migrations, applied);

      expect(outOfOrder).toHaveLength(1);
      expect(outOfOrder[0].id).toBe('20240102000000_v2');
    });
  });
});

// =============================================================================
// DRIZZLE COMPATIBILITY TESTS
// =============================================================================

describe('Drizzle Compatibility', () => {
  describe('parseMigrationFolderName', () => {
    it('should parse v3 format (timestamp)', () => {
      const result = parseMigrationFolderName('20240823160430_add_users_table');

      expect(result).toEqual({
        timestamp: '20240823160430',
        migrationName: 'add_users_table',
        isLegacy: false,
      });
    });

    it('should parse legacy v2 format (sequential)', () => {
      const result = parseMigrationFolderName('0001_warm_stone_men');

      expect(result).toEqual({
        timestamp: '00000000000001',
        migrationName: 'warm_stone_men',
        isLegacy: true,
      });
    });

    it('should return null for invalid format', () => {
      expect(parseMigrationFolderName('invalid')).toBeNull();
      expect(parseMigrationFolderName('meta')).toBeNull();
    });
  });

  describe('parseSnapshotJson', () => {
    it('should parse Drizzle snapshot format', () => {
      const content = JSON.stringify({
        version: '5',
        dialect: 'sqlite',
        tables: {
          users: {
            name: 'users',
            columns: {
              id: { name: 'id', type: 'integer', notNull: true, primaryKey: true },
              name: { name: 'name', type: 'text', notNull: false },
            },
          },
        },
      });

      const snapshot = parseSnapshotJson(content);

      expect(snapshot).not.toBeNull();
      expect(snapshot?.dialect).toBe('sqlite');
      expect(snapshot?.tables.users).toBeDefined();
    });

    it('should return null for invalid JSON', () => {
      expect(parseSnapshotJson('invalid')).toBeNull();
    });
  });

  describe('parseJournalJson', () => {
    it('should parse Drizzle journal format', () => {
      const content = JSON.stringify({
        version: '5',
        dialect: 'sqlite',
        entries: [
          { idx: 0, version: '5', when: 1692806670430, tag: 'init', breakpoints: true },
          { idx: 1, version: '5', when: 1692806670431, tag: 'add_users', breakpoints: true },
        ],
      });

      const journal = parseJournalJson(content);

      expect(journal).not.toBeNull();
      expect(journal?.entries).toHaveLength(2);
      expect(journal?.entries[0].tag).toBe('init');
    });

    it('should return null for missing entries', () => {
      expect(parseJournalJson('{}')).toBeNull();
    });
  });

  describe('loadDrizzleMigrations with in-memory FS', () => {
    it('should load v3 format migrations', async () => {
      const fs = createInMemoryFs({
        '/drizzle/20240101000000_create_users/migration.sql':
          'CREATE TABLE users (id INTEGER PRIMARY KEY)',
        '/drizzle/20240101000000_create_users/snapshot.json': JSON.stringify({
          version: '5',
          dialect: 'sqlite',
          tables: {},
        }),
        '/drizzle/20240101000001_create_posts/migration.sql':
          'CREATE TABLE posts (id INTEGER PRIMARY KEY)',
        '/drizzle/20240101000001_create_posts/snapshot.json': JSON.stringify({
          version: '5',
          dialect: 'sqlite',
          tables: {},
        }),
      });

      const migrations = await loadDrizzleMigrations({
        basePath: '/drizzle',
        fs,
        includeSnapshots: true,
      });

      expect(migrations).toHaveLength(2);
      expect(migrations[0].id).toBe('20240101000000_create_users');
      expect(migrations[1].id).toBe('20240101000001_create_posts');
    });

    it('should load v2 format migrations with journal', async () => {
      const fs = createInMemoryFs({
        '/drizzle/meta/_journal.json': JSON.stringify({
          version: '5',
          dialect: 'sqlite',
          entries: [
            { idx: 0, version: '5', when: 1704067200000, tag: 'init', breakpoints: true },
          ],
        }),
        '/drizzle/0000_init/migration.sql': 'CREATE TABLE init (id INT)',
        '/drizzle/0000_init/snapshot.json': JSON.stringify({
          version: '5',
          dialect: 'sqlite',
          tables: {},
        }),
      });

      const migrations = await loadDrizzleMigrations({
        basePath: '/drizzle',
        fs,
        includeSnapshots: true,
      });

      expect(migrations).toHaveLength(1);
      expect(migrations[0].name).toBe('init');
    });
  });

  describe('generateDownMigration', () => {
    it('should generate DROP TABLE for added tables', () => {
      const previous: MigrationSnapshot = {
        version: '5',
        dialect: 'sqlite',
        tables: {},
      };

      const current: MigrationSnapshot = {
        version: '5',
        dialect: 'sqlite',
        tables: {
          users: {
            name: 'users',
            columns: {
              id: { name: 'id', type: 'integer', notNull: true },
            },
          },
        },
      };

      const downSql = generateDownMigration(current, previous);

      expect(downSql).toContain('DROP TABLE IF EXISTS "users"');
    });

    it('should generate CREATE TABLE for removed tables', () => {
      const previous: MigrationSnapshot = {
        version: '5',
        dialect: 'sqlite',
        tables: {
          users: {
            name: 'users',
            columns: {
              id: { name: 'id', type: 'integer', notNull: true, primaryKey: true },
            },
          },
        },
      };

      const current: MigrationSnapshot = {
        version: '5',
        dialect: 'sqlite',
        tables: {},
      };

      const downSql = generateDownMigration(current, previous);

      expect(downSql).toContain('CREATE TABLE "users"');
      expect(downSql).toContain('"id" integer PRIMARY KEY NOT NULL');
    });
  });

  describe('parseDrizzleConfig', () => {
    it('should parse drizzle config content', () => {
      const content = `
        export default defineConfig({
          dialect: "sqlite",
          schema: "./src/schema.ts",
          out: "./drizzle",
        });
      `;

      const config = parseDrizzleConfig(content);

      expect(config.dialect).toBe('sqlite');
      expect(config.out).toBe('./drizzle');
      expect(config.schema).toBe('./src/schema.ts');
    });
  });

  describe('drizzleIdToDoSqlId', () => {
    it('should convert v3 format (no change needed)', () => {
      expect(drizzleIdToDoSqlId('20240823160430_add_users')).toBe(
        '20240823160430_add_users'
      );
    });

    it('should convert legacy format', () => {
      expect(drizzleIdToDoSqlId('0001_init')).toBe('00000000000001_init');
    });
  });
});

// =============================================================================
// SCHEMA TRACKER TESTS
// =============================================================================

describe('Schema Tracker', () => {
  let storage: SchemaStorage;
  let tracker: SchemaTracker;

  beforeEach(() => {
    storage = createInMemoryStorage();
    tracker = createSchemaTracker(storage);
  });

  describe('Version Tracking', () => {
    it('should start with null version', async () => {
      const version = await tracker.getCurrentVersion();
      expect(version).toBeNull();
    });

    it('should set and get version', async () => {
      await tracker.setCurrentVersion('20240101000000_init');

      const version = await tracker.getCurrentVersion();
      expect(version).toBe('20240101000000_init');
    });

    it('should track applied migrations', async () => {
      await tracker.recordMigration({
        id: '20240101000000_first',
        appliedAt: new Date(),
        checksum: 'abc',
        durationMs: 100,
      });

      const applied = await tracker.getAppliedMigrations();
      expect(applied).toHaveLength(1);
      expect(applied[0].id).toBe('20240101000000_first');
    });

    it('should remove migration records', async () => {
      await tracker.recordMigration({
        id: '20240101000000_first',
        appliedAt: new Date(),
        checksum: 'abc',
        durationMs: 100,
      });
      await tracker.recordMigration({
        id: '20240101000001_second',
        appliedAt: new Date(),
        checksum: 'def',
        durationMs: 100,
      });

      await tracker.removeMigrationRecord('20240101000001_second');

      const applied = await tracker.getAppliedMigrations();
      expect(applied).toHaveLength(1);
      expect(applied[0].id).toBe('20240101000000_first');

      const version = await tracker.getCurrentVersion();
      expect(version).toBe('20240101000000_first');
    });
  });

  describe('needsMigration', () => {
    it('should return true when no version set', async () => {
      expect(await tracker.needsMigration('20240101000000_init')).toBe(true);
    });

    it('should return true when target is newer', async () => {
      await tracker.setCurrentVersion('20240101000000_v1');
      expect(await tracker.needsMigration('20240102000000_v2')).toBe(true);
    });

    it('should return false when at target', async () => {
      await tracker.setCurrentVersion('20240101000000_v1');
      expect(await tracker.needsMigration('20240101000000_v1')).toBe(false);
    });
  });

  describe('Clone Migration Context', () => {
    it('should identify pending migrations for clone', async () => {
      // Set up source state
      await tracker.recordMigration({
        id: '20240101000000_v1',
        appliedAt: new Date(),
        checksum: 'a',
        durationMs: 10,
      });

      const migrations = createMigrations([
        { id: '20240101000000_v1', sql: 'SELECT 1' },
        { id: '20240102000000_v2', sql: 'SELECT 2' },
        { id: '20240103000000_v3', sql: 'SELECT 3' },
      ]);

      const context = await tracker.getCloneMigrationContext(migrations);

      expect(context.sourceVersion).toBe('20240101000000_v1');
      expect(context.targetVersion).toBe('20240103000000_v3');
      expect(context.pendingMigrations).toHaveLength(2);
      expect(context.needsMigration).toBe(true);
    });

    it('should handle clone marker', async () => {
      expect(await tracker.isClone()).toBe(false);

      await tracker.markAsClone('20240101000000_v1');
      expect(await tracker.isClone()).toBe(true);

      await tracker.clearCloneMarker();
      expect(await tracker.isClone()).toBe(false);
    });
  });

  describe('Metadata', () => {
    it('should return metadata', async () => {
      await tracker.recordMigration({
        id: '20240101000000_first',
        appliedAt: new Date('2024-01-01'),
        checksum: 'abc',
        durationMs: 100,
      });

      const metadata = await tracker.getMetadata();

      expect(metadata.currentVersion).toBe('20240101000000_first');
      expect(metadata.totalMigrations).toBe(1);
      expect(metadata.isClone).toBe(false);
      expect(metadata.lastMigrationAt).toBeInstanceOf(Date);
    });
  });

  describe('Clear', () => {
    it('should clear all tracking data', async () => {
      await tracker.recordMigration({
        id: '20240101000000_test',
        appliedAt: new Date(),
        checksum: 'abc',
        durationMs: 100,
      });

      await tracker.clear();

      expect(await tracker.getCurrentVersion()).toBeNull();
      expect(await tracker.getAppliedMigrations()).toHaveLength(0);
    });
  });
});

// =============================================================================
// DB() INTEGRATION TESTS
// =============================================================================

describe('DB() Integration', () => {
  describe('initializeWithMigrations', () => {
    it('should auto-migrate on initialization', async () => {
      const storage = createInMemoryStorage();
      const executor = createTestExecutor();

      const migrations = createMigrations([
        { id: '20240101000000_init', sql: 'CREATE TABLE test (id INTEGER)' },
      ]);

      const status = await initializeWithMigrations({
        migrations,
        storage,
        db: executor,
        autoMigrate: true,
      });

      expect(status.needsMigration).toBe(false);
      expect(status.applied).toHaveLength(1);
      expect(executor.tables.has('test')).toBe(true);
    });

    it('should skip migration when already up to date', async () => {
      const storage = createInMemoryStorage();
      const executor = createTestExecutor();
      const tracker = createSchemaTracker(storage);

      // Pre-apply migration
      await tracker.recordMigration({
        id: '20240101000000_init',
        appliedAt: new Date(),
        checksum: calculateChecksumSync('CREATE TABLE test (id INTEGER)'),
        durationMs: 10,
      });

      const migrations = createMigrations([
        { id: '20240101000000_init', sql: 'CREATE TABLE test (id INTEGER)' },
      ]);

      const status = await initializeWithMigrations({
        migrations,
        storage,
        db: executor,
        autoMigrate: true,
      });

      expect(status.needsMigration).toBe(false);
      expect(status.applied).toHaveLength(1);
      // Table should not be created (migration was already recorded)
      expect(executor.executedSql).not.toContain('CREATE TABLE test (id INTEGER)');
    });
  });

  describe('prepareClone', () => {
    it('should copy migration state to new clone', async () => {
      const sourceStorage = createInMemoryStorage();
      const targetStorage = createInMemoryStorage();

      const sourceTracker = createSchemaTracker(sourceStorage);
      await sourceTracker.recordMigration({
        id: '20240101000000_init',
        appliedAt: new Date(),
        checksum: 'abc',
        durationMs: 10,
      });

      await prepareClone(sourceStorage, targetStorage);

      const targetTracker = createSchemaTracker(targetStorage);
      expect(await targetTracker.isClone()).toBe(true);
      expect(await targetTracker.getCurrentVersion()).toBe('20240101000000_init');
    });
  });

  describe('isCloneReady', () => {
    it('should return true when clone is up to date', async () => {
      const storage = createInMemoryStorage();
      const tracker = createSchemaTracker(storage);

      await tracker.recordMigration({
        id: '20240101000000_v1',
        appliedAt: new Date(),
        checksum: 'a',
        durationMs: 10,
      });

      const migrations = createMigrations([
        { id: '20240101000000_v1', sql: 'SELECT 1' },
      ]);

      expect(await isCloneReady(storage, migrations)).toBe(true);
    });

    it('should return false when clone needs migration', async () => {
      const storage = createInMemoryStorage();

      const migrations = createMigrations([
        { id: '20240101000000_v1', sql: 'SELECT 1' },
      ]);

      expect(await isCloneReady(storage, migrations)).toBe(false);
    });
  });

  describe('createMigrationFromSql', () => {
    it('should create properly formatted migration', () => {
      const migration = createMigrationFromSql(
        'CREATE TABLE users (id INTEGER)',
        'create_users'
      );

      expect(migration.id).toMatch(/^\d{14}_create_users$/);
      expect(migration.sql).toBe('CREATE TABLE users (id INTEGER)');
      expect(migration.name).toBe('create_users');
      expect(migration.checksum).toBeTruthy();
    });
  });

  describe('createReversibleMigration', () => {
    it('should create migration with up and down SQL', () => {
      const migration = createReversibleMigration(
        'CREATE TABLE users (id INTEGER)',
        'DROP TABLE users',
        'create_users'
      );

      expect(migration.sql).toBe('CREATE TABLE users (id INTEGER)');
      expect(migration.downSql).toBe('DROP TABLE users');
    });
  });
});
