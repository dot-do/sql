import { promises as fs } from 'node:fs';
import { join, basename } from 'node:path';

export interface InitOptions {
  directory: string;
  name?: string;
  force?: boolean;
}

export interface InitResult {
  success: boolean;
  createdFiles: string[];
}

const CONFIG_TEMPLATE = (name: string) => `import { defineConfig } from '@dotdo/dosql';

export default defineConfig({
  name: '${name}',
  migrationsDir: './migrations',
  schemaDir: './schema',
  outputDir: './generated',
});
`;

const SCHEMA_TEMPLATE = `// Define your database schema here
// Each table should export a schema definition

export const example = {
  tableName: 'example',
  columns: {
    id: { type: 'integer', primaryKey: true },
    name: { type: 'text', nullable: false },
    created_at: { type: 'text', nullable: false },
  },
} as const;
`;

export async function initProject(options: InitOptions): Promise<InitResult> {
  const { directory, name, force = false } = options;
  const projectName = name ?? basename(directory) ?? 'dosql-project';
  const createdFiles: string[] = [];

  // Check if config already exists
  const configPath = join(directory, 'dosql.config.ts');
  const configExists = await fs.access(configPath).then(() => true).catch(() => false);

  if (configExists && !force) {
    throw new Error(`dosql.config.ts already exists in ${directory}. Use --force to overwrite.`);
  }

  // Create migrations directory
  const migrationsPath = join(directory, 'migrations');
  await fs.mkdir(migrationsPath, { recursive: true });
  createdFiles.push('migrations');

  // Create schema directory
  const schemaPath = join(directory, 'schema');
  await fs.mkdir(schemaPath, { recursive: true });

  // Create initial schema file
  const schemaFilePath = join(schemaPath, 'schema.ts');
  await fs.writeFile(schemaFilePath, SCHEMA_TEMPLATE);
  createdFiles.push('schema/schema.ts');

  // Create config file
  await fs.writeFile(configPath, CONFIG_TEMPLATE(projectName));
  createdFiles.push('dosql.config.ts');

  return {
    success: true,
    createdFiles,
  };
}
