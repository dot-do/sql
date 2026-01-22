/**
 * DoSQL Schema Code Generator
 *
 * Generates:
 * - TypeScript interfaces from schema definitions
 * - SQL CREATE TABLE statements
 * - Validation functions
 */

import type {
  SchemaDefinition,
  TableDefinition,
  GeneratedInterface,
  GeneratedSql,
  CodegenOutput,
  ParsedField,
} from './types.js';

import { parseField, parseSchema } from './parser.js';

// =============================================================================
// TYPE MAPPINGS
// =============================================================================

/**
 * Map base SQL types to TypeScript types
 */
const SQL_TO_TS: Record<string, string> = {
  string: 'string',
  text: 'string',
  uuid: 'string',
  int: 'number',
  integer: 'number',
  bigint: 'number',
  float: 'number',
  double: 'number',
  decimal: 'number',
  number: 'number',
  boolean: 'boolean',
  bool: 'boolean',
  timestamp: 'Date',
  datetime: 'Date',
  date: 'Date',
  time: 'string',
  json: 'unknown',
  jsonb: 'unknown',
  blob: 'Uint8Array',
  binary: 'Uint8Array',
};

/**
 * Map base SQL types to SQL column types
 */
const SQL_COLUMN_TYPES: Record<string, string> = {
  string: 'VARCHAR(255)',
  text: 'TEXT',
  uuid: 'UUID',
  int: 'INTEGER',
  integer: 'INTEGER',
  bigint: 'BIGINT',
  float: 'REAL',
  double: 'DOUBLE PRECISION',
  decimal: 'DECIMAL',
  number: 'NUMERIC',
  boolean: 'BOOLEAN',
  bool: 'BOOLEAN',
  timestamp: 'TIMESTAMP',
  datetime: 'TIMESTAMP',
  date: 'DATE',
  time: 'TIME',
  json: 'JSONB',
  jsonb: 'JSONB',
  blob: 'BYTEA',
  binary: 'BYTEA',
};

// =============================================================================
// TYPESCRIPT GENERATION
// =============================================================================

/**
 * Convert a field to TypeScript type string
 */
function fieldToTsType(field: ParsedField, schema: SchemaDefinition): string {
  let tsType: string;

  if (field.relation) {
    // For relations, use the target table name as the type
    const targetTable = field.relation.target;
    tsType = pascalCase(targetTable);

    if (field.relation.isMany || field.modifiers.isArray) {
      tsType = `${tsType}[]`;
    }
  } else {
    // Get the base TypeScript type
    const baseType = field.baseType.toLowerCase();

    // Handle decimal with precision
    if (baseType.startsWith('decimal(')) {
      tsType = 'number';
    } else {
      tsType = SQL_TO_TS[baseType] || 'unknown';
    }

    // Handle arrays
    if (field.modifiers.isArray) {
      tsType = `${tsType}[]`;
    }
  }

  return tsType;
}

/**
 * Convert string to PascalCase
 */
function pascalCase(str: string): string {
  return str
    .replace(/[-_](.)/g, (_, c) => c.toUpperCase())
    .replace(/^(.)/, (_, c) => c.toUpperCase());
}

/**
 * Convert string to camelCase
 */
function camelCase(str: string): string {
  return str
    .replace(/[-_](.)/g, (_, c) => c.toUpperCase())
    .replace(/^(.)/, (_, c) => c.toLowerCase());
}

/**
 * Generate TypeScript interface for a table
 */
function generateInterface(
  tableName: string,
  table: TableDefinition,
  schema: SchemaDefinition
): GeneratedInterface {
  const interfaceName = pascalCase(tableName);
  const fields: GeneratedInterface['fields'] = [];
  const lines: string[] = [];

  lines.push(`export interface ${interfaceName} {`);

  for (const [fieldName, fieldDef] of Object.entries(table)) {
    const parsed = parseField(fieldDef);
    const tsType = fieldToTsType(parsed, schema);
    const optional = parsed.modifiers.nullable;

    // Build comment
    const comments: string[] = [];
    if (parsed.modifiers.primaryKey) comments.push('@primaryKey');
    if (parsed.modifiers.indexed) comments.push('@indexed');
    if (parsed.relation) {
      comments.push(`@relation ${parsed.relation.type} -> ${parsed.relation.target}`);
    }
    if (parsed.modifiers.defaultValue) {
      comments.push(`@default ${parsed.modifiers.defaultValue}`);
    }

    const comment = comments.length > 0
      ? `  /** ${comments.join(' | ')} */\n`
      : '';

    const optionalMark = optional ? '?' : '';
    const line = `${comment}  ${fieldName}${optionalMark}: ${tsType};`;
    lines.push(line);

    fields.push({
      name: fieldName,
      type: tsType,
      optional,
      comment: comments.length > 0 ? comments.join(' | ') : undefined,
    });
  }

  lines.push('}');

  return {
    name: interfaceName,
    code: lines.join('\n'),
    fields,
  };
}

/**
 * Generate all TypeScript interfaces for a schema
 */
export function generateTypeScript(schema: SchemaDefinition): {
  interfaces: GeneratedInterface[];
  fullCode: string;
} {
  const interfaces: GeneratedInterface[] = [];
  const parsedSchema = parseSchema(schema);

  for (const [tableName] of parsedSchema) {
    const table = schema[tableName] as TableDefinition;
    interfaces.push(generateInterface(tableName, table, schema));
  }

  // Generate the full code with imports and type helpers
  const lines: string[] = [
    '/**',
    ' * Generated TypeScript interfaces from DoSQL schema',
    ' * DO NOT EDIT - This file is auto-generated',
    ' */',
    '',
    '// =============================================================================',
    '// ENTITY INTERFACES',
    '// =============================================================================',
    '',
  ];

  for (const iface of interfaces) {
    lines.push(iface.code);
    lines.push('');
  }

  // Add type helpers
  lines.push(
    '// =============================================================================',
    '// TYPE HELPERS',
    '// =============================================================================',
    '',
    '/** Extract only non-relation fields (for inserts) */',
    `export type InsertData<T> = {`,
    '  [K in keyof T as T[K] extends (infer _)[] ? never :',
    '    T[K] extends { id: string } ? never : K]: T[K];',
    '};',
    '',
    '/** Make primary key fields required, others optional (for updates) */',
    'export type UpdateData<T> = Partial<T>;',
    '',
    '/** Extract primary key type */',
    'export type PrimaryKey<T extends { id: unknown }> = T["id"];',
    ''
  );

  return {
    interfaces,
    fullCode: lines.join('\n'),
  };
}

// =============================================================================
// SQL GENERATION
// =============================================================================

/**
 * Convert a field to SQL column definition
 */
function fieldToSqlColumn(
  fieldName: string,
  field: ParsedField,
  tableName: string
): {
  definition: string;
  constraints: string[];
  foreignKey?: string;
  index?: string;
} {
  const constraints: string[] = [];
  let foreignKey: string | undefined;
  let index: string | undefined;

  // Handle relations
  if (field.relation) {
    if (field.relation.type === 'backward') {
      // Backward relation creates a foreign key column
      const targetTable = field.relation.target;
      const sqlType = 'UUID'; // Assume UUID for foreign keys

      if (!field.modifiers.nullable) {
        constraints.push('NOT NULL');
      }

      foreignKey = `FOREIGN KEY ("${fieldName}") REFERENCES "${targetTable}"("id")`;

      if (field.modifiers.indexed) {
        index = `CREATE INDEX "idx_${tableName}_${fieldName}" ON "${tableName}"("${fieldName}");`;
      }

      return {
        definition: `"${fieldName}" ${sqlType}`,
        constraints,
        foreignKey,
        index,
      };
    } else {
      // Forward relations don't create columns (they're virtual)
      return {
        definition: '',
        constraints: [],
      };
    }
  }

  // Get SQL column type
  let sqlType: string;
  const baseType = field.baseType.toLowerCase();

  // Handle decimal with precision
  if (baseType.startsWith('decimal(')) {
    sqlType = baseType.toUpperCase();
  } else {
    sqlType = SQL_COLUMN_TYPES[baseType] || 'TEXT';
  }

  // Handle arrays
  if (field.modifiers.isArray) {
    sqlType = `${sqlType}[]`;
  }

  // Add constraints
  if (field.modifiers.primaryKey) {
    constraints.push('PRIMARY KEY');
  }

  if (!field.modifiers.nullable && !field.modifiers.primaryKey) {
    constraints.push('NOT NULL');
  }

  if (field.modifiers.defaultValue) {
    const defaultVal = field.modifiers.defaultValue;
    // Handle special defaults like now()
    if (defaultVal === 'now()') {
      constraints.push('DEFAULT CURRENT_TIMESTAMP');
    } else if (/^['"].*['"]$/.test(defaultVal)) {
      // Already quoted
      constraints.push(`DEFAULT ${defaultVal}`);
    } else if (/^\d+$/.test(defaultVal)) {
      // Numeric
      constraints.push(`DEFAULT ${defaultVal}`);
    } else if (defaultVal === 'true' || defaultVal === 'false') {
      // Boolean
      constraints.push(`DEFAULT ${defaultVal.toUpperCase()}`);
    } else {
      // String default
      constraints.push(`DEFAULT '${defaultVal}'`);
    }
  }

  // Handle indexes
  if (field.modifiers.indexed && !field.modifiers.primaryKey) {
    index = `CREATE INDEX "idx_${tableName}_${fieldName}" ON "${tableName}"("${fieldName}");`;
  }

  return {
    definition: `"${fieldName}" ${sqlType}`,
    constraints,
    index,
  };
}

/**
 * Generate SQL CREATE TABLE statement for a table
 */
function generateCreateTable(
  tableName: string,
  table: TableDefinition
): GeneratedSql {
  const columns: GeneratedSql['columns'] = [];
  const columnDefs: string[] = [];
  const indexes: string[] = [];
  const foreignKeys: string[] = [];
  const primaryKeyFields: string[] = [];

  for (const [fieldName, fieldDef] of Object.entries(table)) {
    const parsed = parseField(fieldDef);

    // Skip forward relations (they don't create columns)
    if (parsed.relation?.type === 'forward') {
      continue;
    }

    const column = fieldToSqlColumn(fieldName, parsed, tableName);

    if (column.definition) {
      const fullDef = column.constraints.length > 0
        ? `${column.definition} ${column.constraints.join(' ')}`
        : column.definition;

      columnDefs.push(`  ${fullDef}`);

      columns.push({
        name: fieldName,
        type: parsed.baseType,
        constraints: column.constraints,
      });
    }

    if (column.foreignKey) {
      foreignKeys.push(`  ${column.foreignKey}`);
    }

    if (column.index) {
      indexes.push(column.index);
    }

    if (parsed.modifiers.primaryKey) {
      primaryKeyFields.push(fieldName);
    }
  }

  // Build CREATE TABLE statement
  const allTableDefs = [...columnDefs, ...foreignKeys];
  const sql = [
    `CREATE TABLE "${tableName}" (`,
    allTableDefs.join(',\n'),
    ');',
  ].join('\n');

  return {
    tableName,
    sql,
    columns,
    indexes,
    foreignKeys: foreignKeys.map((fk) => fk.trim()),
  };
}

/**
 * Generate all SQL CREATE TABLE statements for a schema
 */
export function generateSql(schema: SchemaDefinition): {
  tables: GeneratedSql[];
  fullScript: string;
} {
  const tables: GeneratedSql[] = [];
  const parsedSchema = parseSchema(schema);

  // Sort tables to handle dependencies (simple topological sort)
  const tableOrder = topologicalSort(parsedSchema);

  for (const tableName of tableOrder) {
    const table = schema[tableName] as TableDefinition;
    tables.push(generateCreateTable(tableName, table));
  }

  // Generate full SQL script
  const lines: string[] = [
    '-- Generated SQL from DoSQL schema',
    '-- DO NOT EDIT - This file is auto-generated',
    '',
    '-- =============================================================================',
    '-- TABLES',
    '-- =============================================================================',
    '',
  ];

  for (const table of tables) {
    lines.push(table.sql);
    lines.push('');
  }

  // Add indexes
  const allIndexes = tables.flatMap((t) => t.indexes);
  if (allIndexes.length > 0) {
    lines.push(
      '-- =============================================================================',
      '-- INDEXES',
      '-- =============================================================================',
      ''
    );
    for (const index of allIndexes) {
      lines.push(index);
    }
    lines.push('');
  }

  return {
    tables,
    fullScript: lines.join('\n'),
  };
}

/**
 * Topological sort for table creation order (handle foreign key dependencies)
 */
function topologicalSort(
  parsedSchema: Map<string, Map<string, ParsedField>>
): string[] {
  const visited = new Set<string>();
  const result: string[] = [];

  function visit(tableName: string): void {
    if (visited.has(tableName)) return;
    visited.add(tableName);

    const fields = parsedSchema.get(tableName);
    if (fields) {
      // Visit dependencies first (backward relations)
      for (const [, field] of fields) {
        if (field.relation?.type === 'backward') {
          const target = field.relation.target;
          if (parsedSchema.has(target)) {
            visit(target);
          }
        }
      }
    }

    result.push(tableName);
  }

  for (const tableName of parsedSchema.keys()) {
    visit(tableName);
  }

  return result;
}

// =============================================================================
// VALIDATION FUNCTION GENERATION
// =============================================================================

/**
 * Generate a runtime validation function for a table
 */
function generateValidationFunction(
  tableName: string,
  table: TableDefinition
): string {
  const interfaceName = pascalCase(tableName);
  const lines: string[] = [];

  lines.push(`export function validate${interfaceName}(data: unknown): data is ${interfaceName} {`);
  lines.push('  if (typeof data !== "object" || data === null) return false;');
  lines.push('  const obj = data as Record<string, unknown>;');
  lines.push('');

  for (const [fieldName, fieldDef] of Object.entries(table)) {
    const parsed = parseField(fieldDef);

    // Skip relation fields for basic validation
    if (parsed.relation) continue;

    const checkName = `obj["${fieldName}"]`;

    if (parsed.modifiers.nullable) {
      lines.push(`  // ${fieldName} is optional`);
      lines.push(`  if (${checkName} !== undefined && ${checkName} !== null) {`);
      lines.push(`    ${generateTypeCheck(checkName, parsed, '    ')}`);
      lines.push('  }');
    } else {
      lines.push(`  // ${fieldName} is required`);
      lines.push(`  if (${checkName} === undefined || ${checkName} === null) return false;`);
      lines.push(`  ${generateTypeCheck(checkName, parsed, '  ')}`);
    }
    lines.push('');
  }

  lines.push('  return true;');
  lines.push('}');

  return lines.join('\n');
}

/**
 * Generate type check for a field
 */
function generateTypeCheck(varName: string, field: ParsedField, indent: string): string {
  const baseType = field.baseType.toLowerCase();

  if (field.modifiers.isArray) {
    return `${indent}if (!Array.isArray(${varName})) return false;`;
  }

  switch (baseType) {
    case 'string':
    case 'text':
    case 'uuid':
    case 'time':
      return `${indent}if (typeof ${varName} !== "string") return false;`;

    case 'int':
    case 'integer':
    case 'bigint':
    case 'float':
    case 'double':
    case 'number':
      return `${indent}if (typeof ${varName} !== "number") return false;`;

    case 'boolean':
    case 'bool':
      return `${indent}if (typeof ${varName} !== "boolean") return false;`;

    case 'timestamp':
    case 'datetime':
    case 'date':
      return `${indent}if (!(${varName} instanceof Date) && typeof ${varName} !== "string") return false;`;

    case 'json':
    case 'jsonb':
      return `${indent}// JSON can be any type`;

    default:
      if (baseType.startsWith('decimal')) {
        return `${indent}if (typeof ${varName} !== "number") return false;`;
      }
      return `${indent}// Unknown type: ${baseType}`;
  }
}

/**
 * Generate all validation functions for a schema
 */
export function generateValidationFunctions(schema: SchemaDefinition): string {
  const lines: string[] = [
    '/**',
    ' * Generated validation functions from DoSQL schema',
    ' * DO NOT EDIT - This file is auto-generated',
    ' */',
    '',
  ];

  const parsedSchema = parseSchema(schema);

  for (const [tableName] of parsedSchema) {
    const table = schema[tableName] as TableDefinition;
    lines.push(generateValidationFunction(tableName, table));
    lines.push('');
  }

  return lines.join('\n');
}

// =============================================================================
// MAIN CODEGEN FUNCTION
// =============================================================================

/**
 * Code generation options
 */
export interface CodegenOptions {
  /** Include validation functions */
  includeValidation?: boolean;
  /** SQL dialect (default: 'postgres') */
  sqlDialect?: 'postgres' | 'sqlite' | 'mysql';
  /** Add timestamps if missing */
  addTimestamps?: boolean;
}

/**
 * Generate all code from a schema definition
 */
export function generateCode(
  schema: SchemaDefinition,
  options: CodegenOptions = {}
): CodegenOutput {
  const typescript = generateTypeScript(schema);
  const sql = generateSql(schema);

  // Add validation functions if requested
  if (options.includeValidation) {
    typescript.fullCode += '\n' + generateValidationFunctions(schema);
  }

  return {
    typescript,
    sql,
  };
}

/**
 * Generate TypeScript types only
 */
export function codegen(schema: SchemaDefinition): string {
  return generateTypeScript(schema).fullCode;
}

/**
 * Generate SQL only
 */
export function codegenSql(schema: SchemaDefinition): string {
  return generateSql(schema).fullScript;
}
