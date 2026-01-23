import { promises as fs } from 'node:fs';
import { join } from 'node:path';

export interface GenerateOptions {
  schemaDir: string;
  outputDir: string;
}

export interface GenerateResult {
  success: boolean;
  generatedFiles: string[];
  tablesProcessed: string[];
}

interface ColumnDefinition {
  type: 'integer' | 'text' | 'real' | 'blob' | 'boolean';
  primaryKey?: boolean;
  nullable?: boolean;
}

interface TableSchema {
  tableName: string;
  columns: Record<string, ColumnDefinition>;
}

function mapSqlTypeToTs(type: string, nullable: boolean): string {
  const baseType = (() => {
    switch (type) {
      case 'integer':
        return 'number';
      case 'real':
        return 'number';
      case 'boolean':
        return 'boolean';
      case 'blob':
        return 'Uint8Array';
      case 'text':
      default:
        return 'string';
    }
  })();

  return nullable ? `${baseType} | null` : baseType;
}

function tableNameToInterfaceName(tableName: string): string {
  // Convert snake_case to PascalCase: split on underscores and capitalize each part
  // Example: "user_profiles" -> "UserProfiles"
  const pascal = tableName
    .split('_')
    .map(part => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join('');

  // Simple singularization: remove trailing 's' if present and name is longer than 1 char.
  // This handles common cases: "Users" -> "User", "Posts" -> "Post"
  //
  // Known limitations (intentionally kept simple for predictability):
  // - "address" -> "addres" (incorrect - ends in 's' but not plural)
  // - "status" -> "statu" (incorrect - ends in 's' but not plural)
  // - "data" -> "Data" (correct - doesn't end in 's')
  // - "people" -> "People" (incorrect - irregular plural)
  // - "children" -> "Children" (incorrect - irregular plural)
  //
  // Rationale: A simple approach avoids complex inflection libraries and provides
  // predictable output. Users can rename interfaces if needed.
  if (pascal.endsWith('s') && pascal.length > 1) {
    return pascal.slice(0, -1);
  }
  return pascal;
}

async function parseSchemaFile(filePath: string): Promise<TableSchema[]> {
  const content = await fs.readFile(filePath, 'utf-8');
  const schemas: TableSchema[] = [];

  // Regex to match exported table schema definitions.
  // Pattern breakdown:
  //   export\s+const\s+(\w+)     - "export const <varName>" (captures variable name in $1)
  //   \s*=\s*\{                  - " = {" with optional whitespace
  //   [\s\S]*?                   - any characters (non-greedy) before tableName
  //   tableName:\s*['"]([^'"]+)['"]  - "tableName: '<name>'" (captures table name in $2)
  //   [\s\S]*?                   - any characters (non-greedy) before columns
  //   columns:\s*\{([\s\S]*?)\}  - "columns: { ... }" (captures columns block in $3)
  //   \s*,?\s*\}                 - optional trailing comma and closing brace
  //   \s*as\s+const              - "as const" type assertion
  //
  // Limitations:
  // - Requires "as const" suffix (won't match plain objects)
  // - tableName must come before columns in the object
  // - Doesn't handle nested objects or computed property names
  // - Single-line and multi-line strings only (no template literals)
  const exportRegex = /export\s+const\s+(\w+)\s*=\s*\{[\s\S]*?tableName:\s*['"]([^'"]+)['"][\s\S]*?columns:\s*\{([\s\S]*?)\}\s*,?\s*\}\s*as\s+const/g;

  let match;
  while ((match = exportRegex.exec(content)) !== null) {
    const tableName = match[2];
    const columnsBlock = match[3];

    if (!tableName || !columnsBlock) continue;

    const columns: Record<string, ColumnDefinition> = {};

    // Regex to parse individual column definitions within the columns block.
    // Pattern breakdown:
    //   (\w+):                       - column name followed by colon (captures in $1)
    //   \s*\{\s*                     - opening brace with optional whitespace
    //   type:\s*['"](\w+)['"]        - "type: '<type>'" (captures type in $2)
    //   (?:,\s*primaryKey:\s*(true|false))?  - optional "primaryKey: true/false" ($3)
    //   (?:,\s*nullable:\s*(true|false))?    - optional "nullable: true/false" ($4)
    //   \s*\}                        - closing brace
    //
    // Limitations:
    // - Properties must be in order: type, then primaryKey, then nullable
    // - Doesn't handle other column properties (e.g., default, unique)
    const columnRegex = /(\w+):\s*\{\s*type:\s*['"](\w+)['"](?:,\s*primaryKey:\s*(true|false))?(?:,\s*nullable:\s*(true|false))?\s*\}/g;
    let colMatch;
    while ((colMatch = columnRegex.exec(columnsBlock)) !== null) {
      const colName = colMatch[1];
      const colType = colMatch[2] as ColumnDefinition['type'];
      const isPrimaryKey = colMatch[3] === 'true';
      const isNullable = colMatch[4] === 'true';

      if (colName) {
        columns[colName] = {
          type: colType,
          primaryKey: isPrimaryKey,
          nullable: isNullable,
        };
      }
    }

    schemas.push({
      tableName,
      columns,
    });
  }

  return schemas;
}

function generateInterface(schema: TableSchema): string {
  const interfaceName = tableNameToInterfaceName(schema.tableName);
  const lines: string[] = [`export interface ${interfaceName} {`];

  for (const [colName, colDef] of Object.entries(schema.columns)) {
    const tsType = mapSqlTypeToTs(colDef.type, colDef.nullable ?? false);
    lines.push(`  ${colName}: ${tsType};`);
  }

  lines.push('}');
  return lines.join('\n');
}

export async function generateTypes(options: GenerateOptions): Promise<GenerateResult> {
  const { schemaDir, outputDir } = options;

  // Check if schema directory exists
  const exists = await fs.access(schemaDir).then(() => true).catch(() => false);
  if (!exists) {
    throw new Error(
      `Schema directory not found: ${schemaDir}\n\n` +
      `To fix this, either:\n` +
      `  1. Run 'dosql init' to create the default project structure with schema files, or\n` +
      `  2. Create the schema directory manually: mkdir -p ${schemaDir}\n` +
      `  3. Use --schema to specify a different schema directory path\n\n` +
      `Schema files should be TypeScript files (.ts) containing table definitions.\n` +
      `See 'dosql init' output for an example schema file format.`
    );
  }

  // Ensure output directory exists
  await fs.mkdir(outputDir, { recursive: true });

  // Find all schema files
  const entries = await fs.readdir(schemaDir, { withFileTypes: true });
  const schemaFiles = entries
    .filter(e => e.isFile() && e.name.endsWith('.ts'))
    .map(e => join(schemaDir, e.name));

  // Parse all schemas
  const allSchemas: TableSchema[] = [];
  for (const file of schemaFiles) {
    const schemas = await parseSchemaFile(file);
    allSchemas.push(...schemas);
  }

  // Generate TypeScript interfaces
  const interfaces = allSchemas.map(generateInterface);
  const header = `// This file is auto-generated by dosql generate
// Do not edit manually

`;
  const output = header + interfaces.join('\n\n') + '\n';

  // Write output file
  const outputPath = join(outputDir, 'types.ts');
  await fs.writeFile(outputPath, output);

  return {
    success: true,
    generatedFiles: ['types.ts'],
    tablesProcessed: allSchemas.map(s => s.tableName),
  };
}
