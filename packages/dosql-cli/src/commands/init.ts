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

// Configuration file template.
// Note: This imports defineConfig from 'dosql', which is the DoSQL runtime library.
// Users must install this package separately: npm install dosql
// If dosql is not available, users can remove the import and defineConfig wrapper,
// or create their own defineConfig function that returns the config object unchanged.
const CONFIG_TEMPLATE = (name: string) => `// DoSQL configuration file
// Requires: npm install dosql (or remove defineConfig wrapper if not using)
import { defineConfig } from 'dosql';

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
    throw new Error(
      `dosql.config.ts already exists in ${directory}.\n\n` +
      `To overwrite the existing configuration, use the --force flag:\n` +
      `  dosql init --force\n\n` +
      `Note: --force will overwrite dosql.config.ts and schema/schema.ts,\n` +
      `but will NOT delete existing migrations or other schema files.`
    );
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
