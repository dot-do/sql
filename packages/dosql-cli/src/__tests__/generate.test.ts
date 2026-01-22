import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { promises as fs } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { generateTypes, type GenerateOptions } from '../commands/generate.js';

describe('dosql generate', () => {
  let testDir: string;
  let schemaDir: string;
  let outputDir: string;

  beforeEach(async () => {
    testDir = join(tmpdir(), `dosql-test-${Date.now()}-${Math.random().toString(36).slice(2)}`);
    schemaDir = join(testDir, 'schema');
    outputDir = join(testDir, 'generated');
    await fs.mkdir(schemaDir, { recursive: true });
  });

  afterEach(async () => {
    await fs.rm(testDir, { recursive: true, force: true });
  });

  it('should generate types from schema file', async () => {
    const schemaContent = `
export const users = {
  tableName: 'users',
  columns: {
    id: { type: 'integer', primaryKey: true },
    name: { type: 'text', nullable: false },
    email: { type: 'text', nullable: true },
  },
} as const;
`;
    await fs.writeFile(join(schemaDir, 'users.ts'), schemaContent);

    await generateTypes({
      schemaDir,
      outputDir,
    });

    const outputPath = join(outputDir, 'types.ts');
    const exists = await fs.access(outputPath).then(() => true).catch(() => false);
    expect(exists).toBe(true);
  });

  it('should generate correct TypeScript interfaces', async () => {
    const schemaContent = `
export const users = {
  tableName: 'users',
  columns: {
    id: { type: 'integer', primaryKey: true },
    name: { type: 'text', nullable: false },
    email: { type: 'text', nullable: true },
  },
} as const;
`;
    await fs.writeFile(join(schemaDir, 'users.ts'), schemaContent);

    await generateTypes({
      schemaDir,
      outputDir,
    });

    const outputPath = join(outputDir, 'types.ts');
    const content = await fs.readFile(outputPath, 'utf-8');

    expect(content).toContain('interface User');
    expect(content).toContain('id: number');
    expect(content).toContain('name: string');
    expect(content).toContain('email: string | null');
  });

  it('should handle multiple schema files', async () => {
    await fs.writeFile(
      join(schemaDir, 'users.ts'),
      `export const users = {
        tableName: 'users',
        columns: { id: { type: 'integer', primaryKey: true } },
      } as const;`
    );
    await fs.writeFile(
      join(schemaDir, 'posts.ts'),
      `export const posts = {
        tableName: 'posts',
        columns: { id: { type: 'integer', primaryKey: true } },
      } as const;`
    );

    await generateTypes({
      schemaDir,
      outputDir,
    });

    const outputPath = join(outputDir, 'types.ts');
    const content = await fs.readFile(outputPath, 'utf-8');

    expect(content).toContain('interface User');
    expect(content).toContain('interface Post');
  });

  it('should create output directory if it does not exist', async () => {
    const schemaContent = `
export const items = {
  tableName: 'items',
  columns: { id: { type: 'integer', primaryKey: true } },
} as const;
`;
    await fs.writeFile(join(schemaDir, 'items.ts'), schemaContent);

    const deepOutputDir = join(testDir, 'deep', 'nested', 'generated');

    await generateTypes({
      schemaDir,
      outputDir: deepOutputDir,
    });

    const outputPath = join(deepOutputDir, 'types.ts');
    const exists = await fs.access(outputPath).then(() => true).catch(() => false);
    expect(exists).toBe(true);
  });

  it('should throw if schema directory does not exist', async () => {
    const nonExistent = join(testDir, 'nonexistent');

    await expect(
      generateTypes({
        schemaDir: nonExistent,
        outputDir,
      })
    ).rejects.toThrow(/not found|does not exist/i);
  });

  it('should return result with generated file info', async () => {
    const schemaContent = `
export const users = {
  tableName: 'users',
  columns: { id: { type: 'integer', primaryKey: true } },
} as const;
`;
    await fs.writeFile(join(schemaDir, 'users.ts'), schemaContent);

    const result = await generateTypes({
      schemaDir,
      outputDir,
    });

    expect(result.success).toBe(true);
    expect(result.generatedFiles).toContain('types.ts');
    expect(result.tablesProcessed).toContain('users');
  });
});
