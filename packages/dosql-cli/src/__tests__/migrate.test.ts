import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { promises as fs } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import {
  findMigrations,
  runMigrations,
  type Migration,
  type MigrateOptions,
} from '../commands/migrate.js';

describe('dosql migrate', () => {
  let testDir: string;
  let migrationsDir: string;

  beforeEach(async () => {
    testDir = join(tmpdir(), `dosql-test-${Date.now()}-${Math.random().toString(36).slice(2)}`);
    migrationsDir = join(testDir, 'migrations');
    await fs.mkdir(migrationsDir, { recursive: true });
  });

  afterEach(async () => {
    await fs.rm(testDir, { recursive: true, force: true });
  });

  describe('findMigrations', () => {
    it('should find migration files in directory', async () => {
      await fs.writeFile(join(migrationsDir, '001_initial.sql'), 'CREATE TABLE users (id INTEGER);');
      await fs.writeFile(join(migrationsDir, '002_add_email.sql'), 'ALTER TABLE users ADD COLUMN email TEXT;');

      const migrations = await findMigrations(migrationsDir);

      expect(migrations).toHaveLength(2);
      expect(migrations[0]?.name).toBe('001_initial');
      expect(migrations[1]?.name).toBe('002_add_email');
    });

    it('should sort migrations by name', async () => {
      await fs.writeFile(join(migrationsDir, '003_third.sql'), 'SELECT 3;');
      await fs.writeFile(join(migrationsDir, '001_first.sql'), 'SELECT 1;');
      await fs.writeFile(join(migrationsDir, '002_second.sql'), 'SELECT 2;');

      const migrations = await findMigrations(migrationsDir);

      expect(migrations[0]?.name).toBe('001_first');
      expect(migrations[1]?.name).toBe('002_second');
      expect(migrations[2]?.name).toBe('003_third');
    });

    it('should ignore non-sql files', async () => {
      await fs.writeFile(join(migrationsDir, '001_migration.sql'), 'CREATE TABLE t;');
      await fs.writeFile(join(migrationsDir, 'readme.md'), '# Migrations');
      await fs.writeFile(join(migrationsDir, 'helper.ts'), 'export const x = 1;');

      const migrations = await findMigrations(migrationsDir);

      expect(migrations).toHaveLength(1);
      expect(migrations[0]?.name).toBe('001_migration');
    });

    it('should return empty array if no migrations exist', async () => {
      const migrations = await findMigrations(migrationsDir);
      expect(migrations).toEqual([]);
    });

    it('should throw if migrations directory does not exist', async () => {
      const nonExistent = join(testDir, 'nonexistent');
      await expect(findMigrations(nonExistent)).rejects.toThrow(/not found|does not exist/i);
    });

    it('should include file content in migration object', async () => {
      const sql = 'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL);';
      await fs.writeFile(join(migrationsDir, '001_products.sql'), sql);

      const migrations = await findMigrations(migrationsDir);

      expect(migrations[0]?.content).toBe(sql);
    });
  });

  describe('runMigrations', () => {
    it('should return list of applied migrations', async () => {
      const migrations: Migration[] = [
        { name: '001_initial', content: 'CREATE TABLE users (id INTEGER);', path: '' },
        { name: '002_posts', content: 'CREATE TABLE posts (id INTEGER);', path: '' },
      ];

      // Mock executor that just records what was run
      const executed: string[] = [];
      const mockExecutor = async (sql: string, name: string) => {
        executed.push(name);
      };

      const result = await runMigrations({
        migrations,
        executor: mockExecutor,
      });

      expect(result.applied).toHaveLength(2);
      expect(result.applied[0]).toBe('001_initial');
      expect(result.applied[1]).toBe('002_posts');
    });

    it('should skip already applied migrations', async () => {
      const migrations: Migration[] = [
        { name: '001_initial', content: 'CREATE TABLE users (id INTEGER);', path: '' },
        { name: '002_posts', content: 'CREATE TABLE posts (id INTEGER);', path: '' },
      ];

      const executed: string[] = [];
      const mockExecutor = async (sql: string, name: string) => {
        executed.push(name);
      };

      const result = await runMigrations({
        migrations,
        executor: mockExecutor,
        appliedMigrations: ['001_initial'],
      });

      expect(result.applied).toEqual(['002_posts']);
      expect(executed).toEqual(['002_posts']);
    });

    it('should handle dry run mode', async () => {
      const migrations: Migration[] = [
        { name: '001_initial', content: 'CREATE TABLE users (id INTEGER);', path: '' },
      ];

      const executed: string[] = [];
      const mockExecutor = async (sql: string, name: string) => {
        executed.push(name);
      };

      const result = await runMigrations({
        migrations,
        executor: mockExecutor,
        dryRun: true,
      });

      expect(result.applied).toEqual([]);
      expect(result.pending).toEqual(['001_initial']);
      expect(executed).toEqual([]);
    });

    it('should stop on first error if not using force', async () => {
      const migrations: Migration[] = [
        { name: '001_initial', content: 'CREATE TABLE users (id INTEGER);', path: '' },
        { name: '002_broken', content: 'INVALID SQL', path: '' },
        { name: '003_posts', content: 'CREATE TABLE posts (id INTEGER);', path: '' },
      ];

      const mockExecutor = async (sql: string, name: string) => {
        if (name === '002_broken') {
          throw new Error('SQL syntax error');
        }
      };

      const result = await runMigrations({
        migrations,
        executor: mockExecutor,
      });

      expect(result.applied).toEqual(['001_initial']);
      expect(result.error?.migration).toBe('002_broken');
      expect(result.error?.message).toContain('SQL syntax error');
    });
  });
});
