/**
 * DoSQL Migrations - Drizzle Compatibility Tests
 *
 * Tests for Drizzle Kit migration compatibility including:
 * - Parsing Drizzle folder structures (v2 and v3)
 * - Loading migrations from Drizzle format
 * - Generating down migrations from snapshots
 * - Config parsing
 * - ID format conversion
 *
 * NO MOCKS - uses real implementations per DoSQL testing philosophy.
 */

import { describe, it, expect } from 'vitest';

import {
  parseMigrationFolderName,
  parseSnapshotJson,
  parseJournalJson,
  loadDrizzleMigrations,
  createInMemoryFs,
  generateDownMigration,
  parseDrizzleConfig,
  drizzleIdToDoSqlId,
  toDoSqlMigration,
  type MigrationFileSystem,
  type DrizzleLoaderOptions,
} from '../drizzle-compat.js';

import { type MigrationSnapshot } from '../types.js';

// =============================================================================
// PARSE MIGRATION FOLDER NAME TESTS
// =============================================================================

describe('Drizzle Compatibility - parseMigrationFolderName', () => {
  describe('v3 format (timestamp-based)', () => {
    it('should parse standard v3 format', () => {
      const result = parseMigrationFolderName('20240823160430_add_users_table');

      expect(result).toEqual({
        timestamp: '20240823160430',
        migrationName: 'add_users_table',
        isLegacy: false,
      });
    });

    it('should parse v3 format with single word name', () => {
      const result = parseMigrationFolderName('20240101000000_init');

      expect(result).toEqual({
        timestamp: '20240101000000',
        migrationName: 'init',
        isLegacy: false,
      });
    });

    it('should parse v3 format with complex name', () => {
      const result = parseMigrationFolderName('20241231235959_add_user_profiles_and_settings');

      expect(result).toEqual({
        timestamp: '20241231235959',
        migrationName: 'add_user_profiles_and_settings',
        isLegacy: false,
      });
    });

    it('should parse v3 format with numbers in name', () => {
      const result = parseMigrationFolderName('20240101120000_add_v2_api_endpoints');

      expect(result).toEqual({
        timestamp: '20240101120000',
        migrationName: 'add_v2_api_endpoints',
        isLegacy: false,
      });
    });
  });

  describe('v2 format (legacy sequential)', () => {
    it('should parse 4-digit legacy format', () => {
      const result = parseMigrationFolderName('0001_warm_stone_men');

      expect(result).toEqual({
        timestamp: '00000000000001',
        migrationName: 'warm_stone_men',
        isLegacy: true,
      });
    });

    it('should parse 4-digit legacy format with larger number', () => {
      const result = parseMigrationFolderName('0042_add_indexes');

      expect(result).toEqual({
        timestamp: '00000000000042',
        migrationName: 'add_indexes',
        isLegacy: true,
      });
    });

    it('should parse 4-digit legacy format starting with 0000', () => {
      const result = parseMigrationFolderName('0000_initial_schema');

      expect(result).toEqual({
        timestamp: '00000000000000',
        migrationName: 'initial_schema',
        isLegacy: true,
      });
    });
  });

  describe('invalid formats', () => {
    it('should return null for invalid format without number prefix', () => {
      expect(parseMigrationFolderName('invalid')).toBeNull();
      expect(parseMigrationFolderName('no_number')).toBeNull();
      expect(parseMigrationFolderName('meta')).toBeNull();
    });

    it('should return null for empty string', () => {
      expect(parseMigrationFolderName('')).toBeNull();
    });

    it('should return null for folder with only number', () => {
      expect(parseMigrationFolderName('20240101000000')).toBeNull();
    });

    it('should return null for folder with number but no underscore', () => {
      expect(parseMigrationFolderName('20240101000000migration')).toBeNull();
    });
  });
});

// =============================================================================
// PARSE SNAPSHOT JSON TESTS
// =============================================================================

describe('Drizzle Compatibility - parseSnapshotJson', () => {
  it('should parse valid snapshot with tables', () => {
    const content = JSON.stringify({
      version: '5',
      dialect: 'sqlite',
      tables: {
        users: {
          name: 'users',
          columns: {
            id: { name: 'id', type: 'integer', notNull: true, primaryKey: true },
            name: { name: 'name', type: 'text', notNull: false },
            email: { name: 'email', type: 'text', notNull: true },
          },
        },
      },
    });

    const snapshot = parseSnapshotJson(content);

    expect(snapshot).not.toBeNull();
    expect(snapshot?.version).toBe('5');
    expect(snapshot?.dialect).toBe('sqlite');
    expect(snapshot?.tables.users).toBeDefined();
    expect(snapshot?.tables.users.columns.id.primaryKey).toBe(true);
  });

  it('should parse snapshot with enums', () => {
    const content = JSON.stringify({
      version: '5',
      dialect: 'postgresql',
      tables: {},
      enums: {
        status: { name: 'status', values: ['pending', 'active', 'completed'] },
      },
    });

    const snapshot = parseSnapshotJson(content);

    expect(snapshot?.enums?.status?.values).toEqual(['pending', 'active', 'completed']);
  });

  it('should parse snapshot with indexes', () => {
    const content = JSON.stringify({
      version: '5',
      dialect: 'sqlite',
      tables: {},
      indexes: {
        idx_users_email: {
          name: 'idx_users_email',
          columns: ['email'],
          unique: true,
        },
      },
    });

    const snapshot = parseSnapshotJson(content);

    expect(snapshot?.indexes?.idx_users_email?.unique).toBe(true);
  });

  it('should parse snapshot with foreign keys', () => {
    const content = JSON.stringify({
      version: '5',
      dialect: 'sqlite',
      tables: {},
      foreignKeys: {
        fk_posts_user: {
          name: 'fk_posts_user',
          columns: ['user_id'],
          referencedTable: 'users',
          referencedColumns: ['id'],
          onDelete: 'cascade',
        },
      },
    });

    const snapshot = parseSnapshotJson(content);

    expect(snapshot?.foreignKeys?.fk_posts_user?.onDelete).toBe('cascade');
  });

  it('should use defaults for missing fields', () => {
    const content = JSON.stringify({
      tables: {},
    });

    const snapshot = parseSnapshotJson(content);

    expect(snapshot?.version).toBe('5');
    expect(snapshot?.dialect).toBe('sqlite');
  });

  it('should return null for invalid JSON', () => {
    expect(parseSnapshotJson('invalid json {')).toBeNull();
    expect(parseSnapshotJson('')).toBeNull();
  });
});

// =============================================================================
// PARSE JOURNAL JSON TESTS
// =============================================================================

describe('Drizzle Compatibility - parseJournalJson', () => {
  it('should parse valid journal file', () => {
    const content = JSON.stringify({
      version: '5',
      dialect: 'sqlite',
      entries: [
        { idx: 0, version: '5', when: 1692806670430, tag: 'init', breakpoints: true },
        { idx: 1, version: '5', when: 1692806670431, tag: 'add_users', breakpoints: true },
        { idx: 2, version: '5', when: 1692806670432, tag: 'add_posts', breakpoints: false },
      ],
    });

    const journal = parseJournalJson(content);

    expect(journal).not.toBeNull();
    expect(journal?.version).toBe('5');
    expect(journal?.dialect).toBe('sqlite');
    expect(journal?.entries).toHaveLength(3);
    expect(journal?.entries[0].tag).toBe('init');
    expect(journal?.entries[2].breakpoints).toBe(false);
  });

  it('should use defaults for breakpoints', () => {
    const content = JSON.stringify({
      version: '5',
      dialect: 'sqlite',
      entries: [
        { idx: 0, version: '5', when: 1692806670430, tag: 'test' },
      ],
    });

    const journal = parseJournalJson(content);

    expect(journal?.entries[0].breakpoints).toBe(true);
  });

  it('should return null for missing entries', () => {
    expect(parseJournalJson('{}')).toBeNull();
    expect(parseJournalJson('{"version": "5"}')).toBeNull();
  });

  it('should return null for non-array entries', () => {
    expect(parseJournalJson('{"entries": "not an array"}')).toBeNull();
    expect(parseJournalJson('{"entries": {}}')).toBeNull();
  });

  it('should return null for invalid JSON', () => {
    expect(parseJournalJson('invalid')).toBeNull();
  });
});

// =============================================================================
// LOAD DRIZZLE MIGRATIONS (V3 FORMAT) TESTS
// =============================================================================

describe('Drizzle Compatibility - loadDrizzleMigrations (v3)', () => {
  it('should load v3 format migrations', async () => {
    const fs = createInMemoryFs({
      '/drizzle/20240101000000_create_users/migration.sql':
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);',
      '/drizzle/20240101000000_create_users/snapshot.json': JSON.stringify({
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
      }),
      '/drizzle/20240101000001_create_posts/migration.sql':
        'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT);',
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
    expect(migrations[0].name).toBe('create_users');
    expect(migrations[0].sql).toContain('CREATE TABLE users');
    expect(migrations[0].snapshot).toBeDefined();
    expect(migrations[1].id).toBe('20240101000001_create_posts');
  });

  it('should sort v3 migrations by timestamp', async () => {
    const fs = createInMemoryFs({
      '/drizzle/20240103000000_third/migration.sql': 'SELECT 3;',
      '/drizzle/20240101000000_first/migration.sql': 'SELECT 1;',
      '/drizzle/20240102000000_second/migration.sql': 'SELECT 2;',
    });

    const migrations = await loadDrizzleMigrations({
      basePath: '/drizzle',
      fs,
    });

    expect(migrations[0].name).toBe('first');
    expect(migrations[1].name).toBe('second');
    expect(migrations[2].name).toBe('third');
  });

  it('should skip meta folder', async () => {
    const fs = createInMemoryFs({
      '/drizzle/20240101000000_create_users/migration.sql': 'CREATE TABLE users (id INT);',
      '/drizzle/meta/_journal.json': '{}', // Should be ignored as a directory
      '/drizzle/meta/0000_snapshot.json': '{}',
    });

    const migrations = await loadDrizzleMigrations({
      basePath: '/drizzle',
      fs,
    });

    expect(migrations).toHaveLength(1);
    expect(migrations[0].name).toBe('create_users');
  });

  it('should handle missing snapshot files', async () => {
    const fs = createInMemoryFs({
      '/drizzle/20240101000000_test/migration.sql': 'CREATE TABLE test (id INT);',
      // No snapshot.json
    });

    const migrations = await loadDrizzleMigrations({
      basePath: '/drizzle',
      fs,
      includeSnapshots: true,
    });

    expect(migrations).toHaveLength(1);
    expect(migrations[0].snapshot).toBeUndefined();
  });

  it('should exclude snapshots when includeSnapshots is false', async () => {
    const fs = createInMemoryFs({
      '/drizzle/20240101000000_test/migration.sql': 'CREATE TABLE test (id INT);',
      '/drizzle/20240101000000_test/snapshot.json': JSON.stringify({
        version: '5',
        dialect: 'sqlite',
        tables: {},
      }),
    });

    const migrations = await loadDrizzleMigrations({
      basePath: '/drizzle',
      fs,
      includeSnapshots: false,
    });

    expect(migrations[0].snapshot).toBeUndefined();
  });

  it('should calculate checksums for migrations', async () => {
    const sql = 'CREATE TABLE users (id INTEGER PRIMARY KEY);';
    const fs = createInMemoryFs({
      '/drizzle/20240101000000_create_users/migration.sql': sql,
    });

    const migrations = await loadDrizzleMigrations({
      basePath: '/drizzle',
      fs,
    });

    expect(migrations[0].checksum).toBeTruthy();
    expect(migrations[0].checksum.length).toBeGreaterThan(0);
  });
});

// =============================================================================
// LOAD DRIZZLE MIGRATIONS (V2 FORMAT) TESTS
// =============================================================================

describe('Drizzle Compatibility - loadDrizzleMigrations (v2)', () => {
  it('should load v2 format migrations with journal', async () => {
    const fs = createInMemoryFs({
      '/drizzle/meta/_journal.json': JSON.stringify({
        version: '5',
        dialect: 'sqlite',
        entries: [
          { idx: 0, version: '5', when: 1704067200000, tag: 'init', breakpoints: true },
          { idx: 1, version: '5', when: 1704067200001, tag: 'add_users', breakpoints: true },
        ],
      }),
      '/drizzle/0000_init/migration.sql': 'CREATE TABLE init (id INT);',
      '/drizzle/0000_init/snapshot.json': JSON.stringify({
        version: '5',
        dialect: 'sqlite',
        tables: {},
      }),
      '/drizzle/0001_add_users/migration.sql': 'CREATE TABLE users (id INT);',
      '/drizzle/0001_add_users/snapshot.json': JSON.stringify({
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
    expect(migrations[0].name).toBe('init');
    expect(migrations[1].name).toBe('add_users');
  });

  it('should use journal timestamps for v2 migrations', async () => {
    const timestamp = 1704067200000; // 2024-01-01T00:00:00.000Z
    const fs = createInMemoryFs({
      '/drizzle/meta/_journal.json': JSON.stringify({
        version: '5',
        dialect: 'sqlite',
        entries: [
          { idx: 0, version: '5', when: timestamp, tag: 'test', breakpoints: true },
        ],
      }),
      '/drizzle/0000_test/migration.sql': 'SELECT 1;',
    });

    const migrations = await loadDrizzleMigrations({
      basePath: '/drizzle',
      fs,
    });

    expect(migrations[0].createdAt.getTime()).toBe(timestamp);
  });

  it('should throw error for invalid journal file', async () => {
    const fs = createInMemoryFs({
      '/drizzle/meta/_journal.json': 'invalid json',
    });

    await expect(
      loadDrizzleMigrations({ basePath: '/drizzle', fs })
    ).rejects.toThrow(/journal/i);
  });

  it('should skip migrations not in folder', async () => {
    const fs = createInMemoryFs({
      '/drizzle/meta/_journal.json': JSON.stringify({
        version: '5',
        dialect: 'sqlite',
        entries: [
          { idx: 0, version: '5', when: 1704067200000, tag: 'exists', breakpoints: true },
          { idx: 1, version: '5', when: 1704067200001, tag: 'missing', breakpoints: true },
        ],
      }),
      '/drizzle/0000_exists/migration.sql': 'SELECT 1;',
      // 0001_missing folder does not exist
    });

    const migrations = await loadDrizzleMigrations({
      basePath: '/drizzle',
      fs,
    });

    expect(migrations).toHaveLength(1);
    expect(migrations[0].name).toBe('exists');
  });
});

// =============================================================================
// GENERATE DOWN MIGRATION TESTS
// =============================================================================

describe('Drizzle Compatibility - generateDownMigration', () => {
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
            name: { name: 'name', type: 'text', notNull: false },
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
    expect(downSql).toContain('"name" text');
  });

  it('should generate DROP COLUMN for added columns', () => {
    const previous: MigrationSnapshot = {
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

    const current: MigrationSnapshot = {
      version: '5',
      dialect: 'sqlite',
      tables: {
        users: {
          name: 'users',
          columns: {
            id: { name: 'id', type: 'integer', notNull: true },
            email: { name: 'email', type: 'text', notNull: true },
          },
        },
      },
    };

    const downSql = generateDownMigration(current, previous);

    expect(downSql).toContain('ALTER TABLE "users" DROP COLUMN "email"');
  });

  it('should generate ADD COLUMN for removed columns', () => {
    const previous: MigrationSnapshot = {
      version: '5',
      dialect: 'sqlite',
      tables: {
        users: {
          name: 'users',
          columns: {
            id: { name: 'id', type: 'integer', notNull: true },
            email: { name: 'email', type: 'text', notNull: true },
          },
        },
      },
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

    expect(downSql).toContain('ALTER TABLE "users" ADD COLUMN "email" text NOT NULL');
  });

  it('should handle column with default value', () => {
    const previous: MigrationSnapshot = {
      version: '5',
      dialect: 'sqlite',
      tables: {
        users: {
          name: 'users',
          columns: {
            id: { name: 'id', type: 'integer', notNull: true },
            status: { name: 'status', type: 'text', notNull: true, default: "'active'" },
          },
        },
      },
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

    expect(downSql).toContain("DEFAULT 'active'");
  });

  it('should generate CREATE TABLE with autoincrement', () => {
    const previous: MigrationSnapshot = {
      version: '5',
      dialect: 'sqlite',
      tables: {
        users: {
          name: 'users',
          columns: {
            id: { name: 'id', type: 'integer', notNull: true, primaryKey: true, autoincrement: true },
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

    expect(downSql).toContain('AUTOINCREMENT');
  });

  it('should handle multiple table changes', () => {
    const previous: MigrationSnapshot = {
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
        posts: {
          name: 'posts',
          columns: {
            id: { name: 'id', type: 'integer', notNull: true },
          },
        },
        comments: {
          name: 'comments',
          columns: {
            id: { name: 'id', type: 'integer', notNull: true },
          },
        },
      },
    };

    const downSql = generateDownMigration(current, previous);

    expect(downSql).toContain('DROP TABLE IF EXISTS "posts"');
    expect(downSql).toContain('DROP TABLE IF EXISTS "comments"');
    expect(downSql).not.toContain('DROP TABLE IF EXISTS "users"');
  });

  it('should return empty string for identical snapshots', () => {
    const snapshot: MigrationSnapshot = {
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

    const downSql = generateDownMigration(snapshot, snapshot);

    expect(downSql.trim()).toBe('');
  });
});

// =============================================================================
// GENERATE DOWN MIGRATIONS WITH SNAPSHOTS TESTS
// =============================================================================

describe('Drizzle Compatibility - generateDownMigrations option', () => {
  it('should generate down migrations when enabled', async () => {
    const fs = createInMemoryFs({
      '/drizzle/20240101000000_create_users/migration.sql':
        'CREATE TABLE users (id INTEGER PRIMARY KEY);',
      '/drizzle/20240101000000_create_users/snapshot.json': JSON.stringify({
        version: '5',
        dialect: 'sqlite',
        tables: {},
      }),
      '/drizzle/20240101000001_add_email/migration.sql':
        'ALTER TABLE users ADD COLUMN email TEXT;',
      '/drizzle/20240101000001_add_email/snapshot.json': JSON.stringify({
        version: '5',
        dialect: 'sqlite',
        tables: {
          users: {
            name: 'users',
            columns: {
              id: { name: 'id', type: 'integer', notNull: true, primaryKey: true },
              email: { name: 'email', type: 'text', notNull: false },
            },
          },
        },
      }),
    });

    const migrations = await loadDrizzleMigrations({
      basePath: '/drizzle',
      fs,
      generateDownMigrations: true,
    });

    expect(migrations).toHaveLength(2);
    // First migration won't have down since there's no previous snapshot
    expect(migrations[0].downSql).toBeUndefined();
    // Second migration should have generated down SQL
    expect(migrations[1].downSql).toBeDefined();
  });
});

// =============================================================================
// PARSE DRIZZLE CONFIG TESTS
// =============================================================================

describe('Drizzle Compatibility - parseDrizzleConfig', () => {
  it('should parse complete drizzle config', () => {
    const content = `
      import { defineConfig } from 'drizzle-kit';

      export default defineConfig({
        dialect: "sqlite",
        schema: "./src/schema.ts",
        out: "./drizzle",
        migrations: {
          table: "custom_migrations",
          schema: "main",
        },
      });
    `;

    const config = parseDrizzleConfig(content);

    expect(config.dialect).toBe('sqlite');
    expect(config.out).toBe('./drizzle');
    expect(config.schema).toBe('./src/schema.ts');
    expect(config.migrations?.table).toBe('custom_migrations');
  });

  it('should parse postgresql dialect', () => {
    const content = `
      export default defineConfig({
        dialect: "postgresql",
        schema: "./schema.ts",
        out: "./migrations",
      });
    `;

    const config = parseDrizzleConfig(content);

    expect(config.dialect).toBe('postgresql');
  });

  it('should parse mysql dialect', () => {
    const content = `
      export default defineConfig({
        dialect: "mysql",
        out: "./drizzle",
      });
    `;

    const config = parseDrizzleConfig(content);

    expect(config.dialect).toBe('mysql');
  });

  it('should handle missing fields', () => {
    const content = `
      export default {
        dialect: "sqlite",
      };
    `;

    const config = parseDrizzleConfig(content);

    expect(config.dialect).toBe('sqlite');
    expect(config.out).toBeUndefined();
    expect(config.schema).toBeUndefined();
  });

  it('should handle empty content', () => {
    const config = parseDrizzleConfig('');

    expect(config.dialect).toBeUndefined();
    expect(config.out).toBeUndefined();
  });
});

// =============================================================================
// ID FORMAT CONVERSION TESTS
// =============================================================================

describe('Drizzle Compatibility - drizzleIdToDoSqlId', () => {
  it('should pass through v3 format unchanged', () => {
    expect(drizzleIdToDoSqlId('20240823160430_add_users')).toBe(
      '20240823160430_add_users'
    );
  });

  it('should convert legacy 4-digit format', () => {
    expect(drizzleIdToDoSqlId('0001_init')).toBe('00000000000001_init');
    expect(drizzleIdToDoSqlId('0042_add_users')).toBe('00000000000042_add_users');
    expect(drizzleIdToDoSqlId('0000_first')).toBe('00000000000000_first');
  });

  it('should handle invalid format by returning as-is', () => {
    expect(drizzleIdToDoSqlId('invalid')).toBe('invalid');
    expect(drizzleIdToDoSqlId('no_number')).toBe('no_number');
  });
});

// =============================================================================
// TO DOSQL MIGRATION TESTS
// =============================================================================

describe('Drizzle Compatibility - toDoSqlMigration', () => {
  it('should convert drizzle migration to DoSQL format', () => {
    const drizzleMigration = {
      tag: 'add_users',
      when: 1704067200000,
      sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY);',
    };

    const migration = toDoSqlMigration(drizzleMigration);

    expect(migration.id).toContain('add_users');
    expect(migration.sql).toBe('CREATE TABLE users (id INTEGER PRIMARY KEY);');
    expect(migration.name).toBe('add_users');
    expect(migration.checksum).toBeTruthy();
    expect(migration.createdAt.getTime()).toBe(1704067200000);
  });

  it('should include snapshot if provided', () => {
    const snapshot = {
      version: '5',
      dialect: 'sqlite',
      tables: {},
    };

    const drizzleMigration = {
      tag: 'init',
      when: 1704067200000,
      sql: 'SELECT 1;',
      snapshot,
    };

    const migration = toDoSqlMigration(drizzleMigration);

    expect(migration.snapshot).toBeDefined();
    expect(migration.snapshot?.dialect).toBe('sqlite');
  });

  it('should pad timestamp correctly', () => {
    const drizzleMigration = {
      tag: 'test',
      when: 123,
      sql: 'SELECT 1;',
    };

    const migration = toDoSqlMigration(drizzleMigration);

    expect(migration.id).toMatch(/^0+123_test$/);
  });
});

// =============================================================================
// IN-MEMORY FILE SYSTEM TESTS
// =============================================================================

describe('Drizzle Compatibility - createInMemoryFs', () => {
  it('should read file contents', async () => {
    const fs = createInMemoryFs({
      '/path/to/file.txt': 'Hello, World!',
    });

    const content = await fs.readFile('/path/to/file.txt');

    expect(content).toBe('Hello, World!');
  });

  it('should throw error for missing file', async () => {
    const fs = createInMemoryFs({});

    await expect(fs.readFile('/missing.txt')).rejects.toThrow(/not found/i);
  });

  it('should check file existence', async () => {
    const fs = createInMemoryFs({
      '/exists.txt': 'content',
    });

    expect(await fs.exists('/exists.txt')).toBe(true);
    expect(await fs.exists('/missing.txt')).toBe(false);
  });

  it('should check directory existence', async () => {
    const fs = createInMemoryFs({
      '/dir/file.txt': 'content',
    });

    expect(await fs.exists('/dir')).toBe(true);
    expect(await fs.exists('/dir/')).toBe(true);
    expect(await fs.exists('/other')).toBe(false);
  });

  it('should list directory contents', async () => {
    const fs = createInMemoryFs({
      '/dir/file1.txt': 'content1',
      '/dir/file2.txt': 'content2',
      '/dir/subdir/file3.txt': 'content3',
    });

    const entries = await fs.readdir('/dir');

    expect(entries).toContain('file1.txt');
    expect(entries).toContain('file2.txt');
    expect(entries).toContain('subdir');
    expect(entries).toHaveLength(3);
  });

  it('should check if path is directory', async () => {
    const fs = createInMemoryFs({
      '/dir/file.txt': 'content',
    });

    expect(await fs.isDirectory('/dir')).toBe(true);
    expect(await fs.isDirectory('/dir/file.txt')).toBe(false);
  });

  it('should handle empty directory', async () => {
    const fs = createInMemoryFs({});

    const entries = await fs.readdir('/empty');

    expect(entries).toHaveLength(0);
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('Drizzle Compatibility - Integration', () => {
  it('should load and process complete v3 drizzle structure', async () => {
    const fs = createInMemoryFs({
      '/drizzle/20240101000000_init/migration.sql': `
        CREATE TABLE users (
          id INTEGER PRIMARY KEY,
          email TEXT NOT NULL UNIQUE
        );
      `,
      '/drizzle/20240101000000_init/snapshot.json': JSON.stringify({
        version: '5',
        dialect: 'sqlite',
        tables: {
          users: {
            name: 'users',
            columns: {
              id: { name: 'id', type: 'integer', notNull: true, primaryKey: true },
              email: { name: 'email', type: 'text', notNull: true },
            },
          },
        },
      }),
      '/drizzle/20240102000000_add_posts/migration.sql': `
        CREATE TABLE posts (
          id INTEGER PRIMARY KEY,
          title TEXT NOT NULL,
          user_id INTEGER REFERENCES users(id)
        );
      `,
      '/drizzle/20240102000000_add_posts/snapshot.json': JSON.stringify({
        version: '5',
        dialect: 'sqlite',
        tables: {
          users: {
            name: 'users',
            columns: {
              id: { name: 'id', type: 'integer', notNull: true, primaryKey: true },
              email: { name: 'email', type: 'text', notNull: true },
            },
          },
          posts: {
            name: 'posts',
            columns: {
              id: { name: 'id', type: 'integer', notNull: true, primaryKey: true },
              title: { name: 'title', type: 'text', notNull: true },
              user_id: { name: 'user_id', type: 'integer', notNull: false },
            },
          },
        },
      }),
    });

    const migrations = await loadDrizzleMigrations({
      basePath: '/drizzle',
      fs,
      includeSnapshots: true,
      generateDownMigrations: true,
    });

    expect(migrations).toHaveLength(2);

    // First migration
    expect(migrations[0].id).toBe('20240101000000_init');
    expect(migrations[0].sql).toContain('CREATE TABLE users');
    expect(migrations[0].snapshot?.tables.users).toBeDefined();

    // Second migration
    expect(migrations[1].id).toBe('20240102000000_add_posts');
    expect(migrations[1].sql).toContain('CREATE TABLE posts');
    expect(migrations[1].snapshot?.tables.posts).toBeDefined();

    // Down migration should be generated for second migration
    expect(migrations[1].downSql).toContain('DROP TABLE IF EXISTS "posts"');
  });

  it('should load and process complete v2 drizzle structure', async () => {
    const fs = createInMemoryFs({
      '/drizzle/meta/_journal.json': JSON.stringify({
        version: '5',
        dialect: 'sqlite',
        entries: [
          { idx: 0, version: '5', when: 1704067200000, tag: 'init', breakpoints: true },
          { idx: 1, version: '5', when: 1704153600000, tag: 'add_posts', breakpoints: true },
        ],
      }),
      '/drizzle/0000_init/migration.sql': 'CREATE TABLE users (id INTEGER PRIMARY KEY);',
      '/drizzle/0000_init/snapshot.json': JSON.stringify({
        version: '5',
        dialect: 'sqlite',
        tables: { users: { name: 'users', columns: {} } },
      }),
      '/drizzle/0001_add_posts/migration.sql': 'CREATE TABLE posts (id INTEGER PRIMARY KEY);',
      '/drizzle/0001_add_posts/snapshot.json': JSON.stringify({
        version: '5',
        dialect: 'sqlite',
        tables: { users: { name: 'users', columns: {} }, posts: { name: 'posts', columns: {} } },
      }),
    });

    const migrations = await loadDrizzleMigrations({
      basePath: '/drizzle',
      fs,
      includeSnapshots: true,
    });

    expect(migrations).toHaveLength(2);
    expect(migrations[0].name).toBe('init');
    expect(migrations[1].name).toBe('add_posts');

    // Verify sorting is correct (by journal timestamp, not folder name)
    expect(migrations[0].createdAt.getTime()).toBeLessThan(
      migrations[1].createdAt.getTime()
    );
  });
});
