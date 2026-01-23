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

/**
 * Context information for schema parsing errors.
 */
export interface SchemaParseContext {
  filePath?: string | undefined;
  variableName?: string | undefined;
  tableName?: string | undefined;
}

/**
 * Custom error class for schema parsing errors.
 * Provides user-friendly error messages with context about where the error occurred.
 */
export class SchemaParseError extends Error {
  constructor(
    message: string,
    public readonly context?: SchemaParseContext
  ) {
    const contextParts: string[] = [];
    if (context?.filePath) contextParts.push(`file: ${context.filePath}`);
    if (context?.variableName) contextParts.push(`variable: ${context.variableName}`);
    if (context?.tableName) contextParts.push(`table: ${context.tableName}`);

    const contextStr = contextParts.length > 0 ? ` (${contextParts.join(', ')})` : '';
    super(`${message}${contextStr}`);
    this.name = 'SchemaParseError';
  }
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

// Common irregular plurals mapping (lowercase)
const IRREGULAR_PLURALS: Record<string, string> = {
  people: 'person',
  children: 'child',
  men: 'man',
  women: 'woman',
  feet: 'foot',
  teeth: 'tooth',
  geese: 'goose',
  mice: 'mouse',
  indices: 'index',
  vertices: 'vertex',
  matrices: 'matrix',
  analyses: 'analysis',
  crises: 'crisis',
  theses: 'thesis',
  phenomena: 'phenomenon',
  criteria: 'criterion',
  data: 'datum',
  media: 'medium',
};

// Words that end in 's' but are not plural (should not be singularized)
const NON_PLURAL_S_WORDS = new Set([
  'status',
  'bus',
  'campus',
  'virus',
  'genus',
  'radius',
  'focus',
  'corpus',
  'census',
  'consensus',
  'axis',
  'basis',
  'series',
  'species',
  'news',
  'atlas',
  'alias',
  'canvas',
  'gas',
  'class',
  'glass',
  'grass',
  'mass',
  'pass',
  'process',
  'address',
  'success',
  'access',
  'progress',
  'congress',
  'express',
  'business',
  'witness',
  'chess',
  'dress',
  'stress',
  'boss',
  'loss',
  'cross',
  'moss',
]);

/**
 * Singularizes a word using common English rules.
 * Handles irregular plurals, words ending in -ies, -es, and regular -s plurals.
 * Also avoids incorrectly singularizing words that end in 's' but aren't plural.
 */
function singularize(word: string): string {
  const lower = word.toLowerCase();

  // Check irregular plurals first
  if (IRREGULAR_PLURALS[lower]) {
    // Preserve original casing (PascalCase)
    const singular = IRREGULAR_PLURALS[lower];
    return singular.charAt(0).toUpperCase() + singular.slice(1);
  }

  // Check if word ends in 's' but is not a plural
  if (NON_PLURAL_S_WORDS.has(lower)) {
    return word;
  }

  // Handle -ies -> -y (e.g., "Categories" -> "Category")
  if (word.endsWith('ies') && word.length > 3) {
    return word.slice(0, -3) + 'y';
  }

  // Handle -ses -> -s (e.g., "Buses" -> "Bus", "Statuses" -> "Status")
  if (word.endsWith('ses') && word.length > 3) {
    return word.slice(0, -2);
  }

  // Handle -xes -> -x (e.g., "Boxes" -> "Box")
  if (word.endsWith('xes') && word.length > 3) {
    return word.slice(0, -2);
  }

  // Handle -ches -> -ch (e.g., "Batches" -> "Batch")
  if (word.endsWith('ches') && word.length > 4) {
    return word.slice(0, -2);
  }

  // Handle -shes -> -sh (e.g., "Dishes" -> "Dish")
  if (word.endsWith('shes') && word.length > 4) {
    return word.slice(0, -2);
  }

  // Handle -oes -> -o (e.g., "Heroes" -> "Hero", "Potatoes" -> "Potato")
  if (word.endsWith('oes') && word.length > 3) {
    return word.slice(0, -2);
  }

  // Handle -ves -> -f or -fe (e.g., "Leaves" -> "Leaf", "Knives" -> "Knife")
  if (word.endsWith('ves') && word.length > 3) {
    const stem = word.slice(0, -3);
    // Common -ife words: knife, wife, life
    if (lower.endsWith('ives') && ['kn', 'w', 'l'].some(prefix => lower.slice(0, -4).endsWith(prefix))) {
      return stem + 'ife';
    }
    return stem + 'f';
  }

  // Handle regular -s plurals (e.g., "Users" -> "User")
  if (word.endsWith('s') && word.length > 1) {
    return word.slice(0, -1);
  }

  return word;
}

function tableNameToInterfaceName(tableName: string): string {
  // Convert snake_case to PascalCase: split on underscores and capitalize each part
  // Example: "user_profiles" -> "UserProfiles"
  const pascal = tableName
    .split('_')
    .map(part => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join('');

  // Apply singularization with improved rules
  return singularize(pascal);
}

/**
 * Regex to match exported table schema definitions.
 * Pattern breakdown:
 *   export\s+const\s+(\w+)     - "export const <varName>" (captures variable name in $1)
 *   \s*=\s*\{                  - " = {" with optional whitespace
 *   [\s\S]*?                   - any characters (non-greedy) before tableName
 *   tableName:\s*['"]([^'"]+)['"]  - "tableName: '<name>'" (captures table name in $2)
 *   [\s\S]*?                   - any characters (non-greedy) before columns
 *   columns:\s*\{([\s\S]*?)\}  - "columns: { ... }" (captures columns block in $3)
 *   \s*,?\s*\}                 - optional trailing comma and closing brace
 *   \s*as\s+const              - "as const" type assertion
 *
 * Limitations:
 * - Requires "as const" suffix (won't match plain objects)
 * - tableName must come before columns in the object
 * - Doesn't handle nested objects or computed property names
 * - Single-line and multi-line strings only (no template literals)
 */
const TABLE_EXPORT_REGEX = /export\s+const\s+(\w+)\s*=\s*\{[\s\S]*?tableName:\s*['"]([^'"]+)['"][\s\S]*?columns:\s*\{([\s\S]*?)\}\s*,?\s*\}\s*as\s+const/g;

/**
 * Regex to parse individual column definitions within the columns block.
 * Pattern breakdown:
 *   (\w+):                       - column name followed by colon (captures in $1)
 *   \s*\{\s*                     - opening brace with optional whitespace
 *   type:\s*['"](\w+)['"]        - "type: '<type>'" (captures type in $2)
 *   (?:,\s*primaryKey:\s*(true|false))?  - optional "primaryKey: true/false" ($3)
 *   (?:,\s*nullable:\s*(true|false))?    - optional "nullable: true/false" ($4)
 *   \s*\}                        - closing brace
 *
 * Limitations:
 * - Properties must be in order: type, then primaryKey, then nullable
 * - Doesn't handle other column properties (e.g., default, unique)
 */
const COLUMN_DEFINITION_REGEX = /(\w+):\s*\{\s*type:\s*['"](\w+)['"](?:,\s*primaryKey:\s*(true|false))?(?:,\s*nullable:\s*(true|false))?\s*\}/g;

/**
 * Named indices for COLUMN_DEFINITION_REGEX capture groups.
 * Provides type-safe access to regex match results.
 */
const ColumnCaptureGroups = {
  /** The full matched string (index 0) */
  FULL_MATCH: 0,
  /** Column name (e.g., "id", "email") */
  COLUMN_NAME: 1,
  /** Column type (e.g., "integer", "text") */
  COLUMN_TYPE: 2,
  /** Primary key flag ("true" | "false" | undefined) */
  PRIMARY_KEY: 3,
  /** Nullable flag ("true" | "false" | undefined) */
  NULLABLE: 4,
} as const;

/**
 * Named indices for TABLE_EXPORT_REGEX capture groups.
 * Provides type-safe access to regex match results.
 */
const TableCaptureGroups = {
  /** The full matched string (index 0) */
  FULL_MATCH: 0,
  /** Variable name (e.g., "usersSchema") */
  VARIABLE_NAME: 1,
  /** Table name (e.g., "users") */
  TABLE_NAME: 2,
  /** Columns block content (the string between "columns: {" and "}") */
  COLUMNS_BLOCK: 3,
} as const;

/**
 * Type-safe helper to extract a required capture group from a regex match.
 * @param match - The regex match array
 * @param index - The capture group index
 * @param groupName - Human-readable name for error messages
 * @returns The captured string value
 * @throws Error if the capture group is undefined or empty
 */
function getRequiredCaptureGroup(
  match: RegExpExecArray,
  index: number,
  groupName: string
): string {
  const value = match[index];
  if (value === undefined || value === '') {
    throw new Error(`Missing required capture group: ${groupName} (index ${index})`);
  }
  return value;
}

/**
 * Type-safe helper to extract an optional capture group from a regex match.
 * @param match - The regex match array
 * @param index - The capture group index
 * @returns The captured string value or undefined
 */
function getOptionalCaptureGroup(
  match: RegExpExecArray,
  index: number
): string | undefined {
  return match[index];
}

/**
 * Type-safe helper to extract a boolean capture group from a regex match.
 * @param match - The regex match array
 * @param index - The capture group index
 * @returns true if the captured value is "true", false otherwise
 */
function getBooleanCaptureGroup(
  match: RegExpExecArray,
  index: number
): boolean {
  return match[index] === 'true';
}

/**
 * Parses a single column definition from a regex match result.
 * Uses type-safe capture group extraction via named indices.
 * @param match - The regex match array from COLUMN_DEFINITION_REGEX
 * @returns A tuple of [columnName, columnDefinition] or null if invalid
 */
function parseColumnDefinition(match: RegExpExecArray): [string, ColumnDefinition] | null {
  // Extract column name (required) - if missing, return null
  const colName = getOptionalCaptureGroup(match, ColumnCaptureGroups.COLUMN_NAME);
  if (!colName) {
    return null;
  }

  // Extract column type (required for valid match, but use optional for safety)
  const rawType = getOptionalCaptureGroup(match, ColumnCaptureGroups.COLUMN_TYPE);
  if (!rawType) {
    return null;
  }
  const colType = rawType as ColumnDefinition['type'];

  // Extract optional boolean flags using type-safe boolean helper
  const isPrimaryKey = getBooleanCaptureGroup(match, ColumnCaptureGroups.PRIMARY_KEY);
  const isNullable = getBooleanCaptureGroup(match, ColumnCaptureGroups.NULLABLE);

  return [
    colName,
    {
      type: colType,
      primaryKey: isPrimaryKey,
      nullable: isNullable,
    },
  ];
}

/**
 * Parses the columns block from a table schema definition.
 * @param columnsBlock - The raw string content between "columns: {" and "}"
 * @param context - Optional context for error messages
 * @returns A record mapping column names to their definitions
 * @throws SchemaParseError if no valid columns are found
 */
function parseColumnsBlock(
  columnsBlock: string,
  context?: SchemaParseContext
): Record<string, ColumnDefinition> {
  const columns: Record<string, ColumnDefinition> = {};

  // Create a fresh regex instance to avoid lastIndex issues with global regex
  const regex = new RegExp(COLUMN_DEFINITION_REGEX.source, COLUMN_DEFINITION_REGEX.flags);
  let colMatch;

  while ((colMatch = regex.exec(columnsBlock)) !== null) {
    const parsed = parseColumnDefinition(colMatch);
    if (parsed) {
      const [colName, colDef] = parsed;
      columns[colName] = colDef;
    }
  }

  // Validate that at least one column was found
  if (Object.keys(columns).length === 0) {
    throw new SchemaParseError(
      'No valid columns found in table schema. Each column must have a "type" property with a valid SQL type (integer, text, real, blob, boolean).\n\n' +
      'Example column definition:\n' +
      '  columns: {\n' +
      '    id: { type: "integer", primaryKey: true },\n' +
      '    name: { type: "text" },\n' +
      '  }',
      context
    );
  }

  return columns;
}

/**
 * Parses a single table definition from a regex match result.
 * Uses type-safe capture group extraction via named indices.
 * @param match - The regex match array from TABLE_EXPORT_REGEX
 * @param filePath - Optional file path for error context
 * @returns A TableSchema object
 * @throws SchemaParseError if tableName or columns block is missing/invalid
 */
function parseTableDefinition(match: RegExpExecArray, filePath?: string): TableSchema {
  // Extract capture groups using type-safe named indices
  const variableName = getOptionalCaptureGroup(match, TableCaptureGroups.VARIABLE_NAME);
  const tableName = getOptionalCaptureGroup(match, TableCaptureGroups.TABLE_NAME);
  const columnsBlock = getOptionalCaptureGroup(match, TableCaptureGroups.COLUMNS_BLOCK);

  const context = { filePath, variableName };

  // Validate tableName is present and not empty
  if (!tableName) {
    throw new SchemaParseError(
      'Missing "tableName" property in table schema. The tableName property is required and must be a non-empty string.\n\n' +
      'Example:\n' +
      '  export const usersSchema = {\n' +
      '    tableName: "users",\n' +
      '    columns: { ... }\n' +
      '  } as const;',
      context
    );
  }

  // Validate tableName is a valid identifier (not just whitespace)
  const trimmedTableName = tableName.trim();
  if (trimmedTableName.length === 0) {
    throw new SchemaParseError(
      'The "tableName" property cannot be empty or contain only whitespace. Please provide a valid table name.',
      { ...context, tableName }
    );
  }

  // Validate columns block is present
  if (!columnsBlock) {
    throw new SchemaParseError(
      'Missing "columns" property in table schema. The columns property is required and must define at least one column.\n\n' +
      'Example:\n' +
      '  export const usersSchema = {\n' +
      '    tableName: "users",\n' +
      '    columns: {\n' +
      '      id: { type: "integer", primaryKey: true },\n' +
      '      email: { type: "text" },\n' +
      '    }\n' +
      '  } as const;',
      { ...context, tableName: trimmedTableName }
    );
  }

  const columns = parseColumnsBlock(columnsBlock, { ...context, tableName: trimmedTableName });

  return {
    tableName: trimmedTableName,
    columns,
  };
}

/**
 * Extracts all table schema definitions from file content.
 * @param content - The raw file content to parse
 * @param filePath - Optional file path for error context
 * @returns An array of TableSchema objects found in the content
 * @throws SchemaParseError if any schema definition is invalid
 */
function extractTableSchemas(content: string, filePath?: string): TableSchema[] {
  const schemas: TableSchema[] = [];

  // Create a fresh regex instance to avoid lastIndex issues with global regex
  const regex = new RegExp(TABLE_EXPORT_REGEX.source, TABLE_EXPORT_REGEX.flags);
  let match;

  while ((match = regex.exec(content)) !== null) {
    const schema = parseTableDefinition(match, filePath);
    schemas.push(schema);
  }

  return schemas;
}

/**
 * Reads and parses a schema file to extract table definitions.
 * @param filePath - The path to the schema file
 * @returns An array of TableSchema objects found in the file
 * @throws SchemaParseError if any schema definition is invalid
 */
async function parseSchemaFile(filePath: string): Promise<TableSchema[]> {
  const content = await fs.readFile(filePath, 'utf-8');
  return extractTableSchemas(content, filePath);
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
