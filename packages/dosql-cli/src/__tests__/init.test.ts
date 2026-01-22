import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { promises as fs } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { initProject, type InitOptions } from '../commands/init.js';

describe('dosql init', () => {
  let testDir: string;

  beforeEach(async () => {
    // Create a unique temp directory for each test
    testDir = join(tmpdir(), `dosql-test-${Date.now()}-${Math.random().toString(36).slice(2)}`);
    await fs.mkdir(testDir, { recursive: true });
  });

  afterEach(async () => {
    // Clean up test directory
    await fs.rm(testDir, { recursive: true, force: true });
  });

  it('should create dosql.config.ts file', async () => {
    await initProject({ directory: testDir });

    const configPath = join(testDir, 'dosql.config.ts');
    const exists = await fs.access(configPath).then(() => true).catch(() => false);
    expect(exists).toBe(true);
  });

  it('should create migrations directory', async () => {
    await initProject({ directory: testDir });

    const migrationsPath = join(testDir, 'migrations');
    const stat = await fs.stat(migrationsPath);
    expect(stat.isDirectory()).toBe(true);
  });

  it('should create schema directory', async () => {
    await initProject({ directory: testDir });

    const schemaPath = join(testDir, 'schema');
    const stat = await fs.stat(schemaPath);
    expect(stat.isDirectory()).toBe(true);
  });

  it('should create initial schema.ts file', async () => {
    await initProject({ directory: testDir });

    const schemaPath = join(testDir, 'schema', 'schema.ts');
    const exists = await fs.access(schemaPath).then(() => true).catch(() => false);
    expect(exists).toBe(true);
  });

  it('should populate config with sensible defaults', async () => {
    await initProject({ directory: testDir });

    const configPath = join(testDir, 'dosql.config.ts');
    const content = await fs.readFile(configPath, 'utf-8');

    expect(content).toContain('export default');
    expect(content).toContain('migrationsDir');
    expect(content).toContain('schemaDir');
  });

  it('should not overwrite existing config without force flag', async () => {
    const configPath = join(testDir, 'dosql.config.ts');
    await fs.writeFile(configPath, '// existing config');

    await expect(initProject({ directory: testDir })).rejects.toThrow(/already exists/);
  });

  it('should overwrite existing config with force flag', async () => {
    const configPath = join(testDir, 'dosql.config.ts');
    await fs.writeFile(configPath, '// existing config');

    await initProject({ directory: testDir, force: true });

    const content = await fs.readFile(configPath, 'utf-8');
    expect(content).not.toContain('// existing config');
    expect(content).toContain('export default');
  });

  it('should use custom project name if provided', async () => {
    await initProject({ directory: testDir, name: 'my-awesome-db' });

    const configPath = join(testDir, 'dosql.config.ts');
    const content = await fs.readFile(configPath, 'utf-8');
    expect(content).toContain('my-awesome-db');
  });

  it('should return success result with created files', async () => {
    const result = await initProject({ directory: testDir });

    expect(result.success).toBe(true);
    expect(result.createdFiles).toContain('dosql.config.ts');
    expect(result.createdFiles).toContain('migrations');
    expect(result.createdFiles).toContain('schema/schema.ts');
  });
});
