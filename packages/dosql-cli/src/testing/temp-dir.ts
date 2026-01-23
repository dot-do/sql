/**
 * Shared test fixtures for temporary directory management.
 * Provides consistent setup/teardown patterns across tests.
 */

import { promises as fs } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { beforeEach, afterEach } from 'vitest';

/**
 * Options for creating a temp directory fixture.
 */
export interface TempDirFixtureOptions {
  /**
   * Prefix for the temp directory name.
   * @default 'dosql-test'
   */
  prefix?: string;

  /**
   * Subdirectories to create within the temp directory.
   * @default []
   */
  subdirs?: string[];
}

/**
 * Context object returned by useTempDir hook.
 * Contains the test directory path and helper functions.
 */
export interface TempDirContext {
  /**
   * Root path of the temporary test directory.
   */
  testDir: string;

  /**
   * Gets a path within the test directory.
   * @param parts - Path segments to join
   */
  getPath: (...parts: string[]) => string;

  /**
   * Writes a file within the test directory.
   * @param relativePath - Path relative to testDir
   * @param content - File content to write
   */
  writeFile: (relativePath: string, content: string) => Promise<void>;

  /**
   * Reads a file within the test directory.
   * @param relativePath - Path relative to testDir
   */
  readFile: (relativePath: string) => Promise<string>;

  /**
   * Creates a subdirectory within the test directory.
   * @param relativePath - Path relative to testDir
   */
  mkdir: (relativePath: string) => Promise<void>;
}

/**
 * Generates a unique temp directory path.
 * Uses timestamp and random suffix to ensure uniqueness.
 */
export function generateTempDirPath(prefix = 'dosql-test'): string {
  return join(tmpdir(), `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2)}`);
}

/**
 * Creates a temporary test directory and returns its path.
 * Remember to clean up with cleanupTempDir when done.
 *
 * @param options - Options for directory creation
 * @returns Path to the created directory
 */
export async function createTempDir(options: TempDirFixtureOptions = {}): Promise<string> {
  const { prefix = 'dosql-test', subdirs = [] } = options;

  const testDir = generateTempDirPath(prefix);
  await fs.mkdir(testDir, { recursive: true });

  for (const subdir of subdirs) {
    await fs.mkdir(join(testDir, subdir), { recursive: true });
  }

  return testDir;
}

/**
 * Removes a temporary test directory and all its contents.
 *
 * @param testDir - Path to the directory to remove
 */
export async function cleanupTempDir(testDir: string): Promise<void> {
  await fs.rm(testDir, { recursive: true, force: true });
}

/**
 * Vitest hook that creates and manages a temp directory for tests.
 * Automatically cleans up after each test.
 *
 * @param options - Options for directory creation
 * @returns TempDirContext object with testDir path and helpers
 *
 * @example
 * ```ts
 * describe('my tests', () => {
 *   const ctx = useTempDir({ subdirs: ['migrations', 'schema'] });
 *
 *   it('should work', async () => {
 *     await ctx.writeFile('config.ts', 'export default {}');
 *     const content = await ctx.readFile('config.ts');
 *     expect(content).toContain('export default');
 *   });
 * });
 * ```
 */
export function useTempDir(options: TempDirFixtureOptions = {}): TempDirContext {
  const ctx: TempDirContext = {
    testDir: '',
    getPath: (...parts: string[]) => join(ctx.testDir, ...parts),
    writeFile: async (relativePath: string, content: string) => {
      const fullPath = join(ctx.testDir, relativePath);
      const dir = join(fullPath, '..');
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(fullPath, content);
    },
    readFile: async (relativePath: string) => {
      return fs.readFile(join(ctx.testDir, relativePath), 'utf-8');
    },
    mkdir: async (relativePath: string) => {
      await fs.mkdir(join(ctx.testDir, relativePath), { recursive: true });
    },
  };

  beforeEach(async () => {
    ctx.testDir = await createTempDir(options);
  });

  afterEach(async () => {
    if (ctx.testDir) {
      await cleanupTempDir(ctx.testDir);
    }
  });

  return ctx;
}
