/**
 * DoSQL TypeScript Schema Inference from SQL DDL
 *
 * Generates TypeScript types from SQL CREATE TABLE statements.
 * This utility parses CREATE TABLE DDL and produces type-safe interfaces
 * and query helpers for use with DoSQL.
 *
 * @example
 * ```sql
 * CREATE TABLE users (
 *   id INTEGER PRIMARY KEY,
 *   name TEXT NOT NULL,
 *   email TEXT,
 *   created_at TEXT
 * );
 * ```
 *
 * Generates:
 * ```typescript
 * interface UsersRow {
 *   id: number;
 *   name: string;
 *   email: string | null;
 *   created_at: string | null;
 * }
 * ```
 */

import {
  parseDDL,
  parseCreateTable,
  type CreateTableStatement,
  type ColumnDefinition,
  type ColumnConstraint,
  type ColumnDataType,
} from '../parser/ddl.js';

// =============================================================================
// TYPES
// =============================================================================

/**
 * SQL type to TypeScript type mapping entry
 */
export interface TypeMapping {
  /** The TypeScript type string */
  tsType: string;
  /** Whether the type is nullable by default */
  nullableByDefault?: boolean;
}

/**
 * Configuration options for schema inference
 */
export interface SchemaInferenceOptions {
  /** Custom type mappings to override defaults */
  customTypeMappings?: Record<string, TypeMapping>;
  /** Whether to use branded types for specific SQL types */
  useBrandedTypes?: boolean;
  /** Whether to generate insert/update helper types */
  generateHelperTypes?: boolean;
  /** Whether to generate validation functions */
  generateValidation?: boolean;
  /** Prefix for generated interface names */
  interfacePrefix?: string;
  /** Suffix for generated interface names (default: 'Row') */
  interfaceSuffix?: string;
  /** Whether to include JSDoc comments */
  includeComments?: boolean;
  /** Whether to use strict null checks */
  strictNullChecks?: boolean;
}

/**
 * Generated TypeScript field definition
 */
export interface GeneratedField {
  /** Column name */
  name: string;
  /** TypeScript type */
  type: string;
  /** Whether the field is nullable */
  nullable: boolean;
  /** Whether this is a primary key */
  isPrimaryKey: boolean;
  /** Whether this field has a default value */
  hasDefault: boolean;
  /** The default value if any */
  defaultValue?: string | number | boolean | null;
  /** JSDoc comment for the field */
  comment?: string;
}

/**
 * Generated TypeScript interface definition
 */
export interface GeneratedInterface {
  /** Table name (original) */
  tableName: string;
  /** Interface name (PascalCase) */
  interfaceName: string;
  /** Field definitions */
  fields: GeneratedField[];
  /** Full TypeScript code for the interface */
  code: string;
}

/**
 * Generated helper types (Insert, Update, etc.)
 */
export interface GeneratedHelperTypes {
  /** Insert type code (excludes auto-generated fields) */
  insertType?: string;
  /** Update type code (all fields optional) */
  updateType?: string;
  /** Primary key type code */
  primaryKeyType?: string;
}

/**
 * Result of schema inference
 */
export interface SchemaInferenceResult {
  /** Generated interfaces */
  interfaces: GeneratedInterface[];
  /** Helper types */
  helpers: GeneratedHelperTypes;
  /** Full generated TypeScript code */
  fullCode: string;
  /** Any warnings during inference */
  warnings: string[];
}

// =============================================================================
// TYPE MAPPINGS
// =============================================================================

/**
 * Default SQL type to TypeScript type mappings
 */
const DEFAULT_TYPE_MAPPINGS: Record<string, TypeMapping> = {
  // Integer types
  INTEGER: { tsType: 'number' },
  INT: { tsType: 'number' },
  SMALLINT: { tsType: 'number' },
  MEDIUMINT: { tsType: 'number' },
  BIGINT: { tsType: 'number' },
  TINYINT: { tsType: 'number' },

  // Floating point types
  REAL: { tsType: 'number' },
  DOUBLE: { tsType: 'number' },
  'DOUBLE PRECISION': { tsType: 'number' },
  FLOAT: { tsType: 'number' },
  NUMERIC: { tsType: 'number' },
  DECIMAL: { tsType: 'number' },

  // Text types
  TEXT: { tsType: 'string' },
  VARCHAR: { tsType: 'string' },
  CHAR: { tsType: 'string' },
  NCHAR: { tsType: 'string' },
  NVARCHAR: { tsType: 'string' },
  CLOB: { tsType: 'string' },

  // Binary types
  BLOB: { tsType: 'Uint8Array' },
  NONE: { tsType: 'unknown' },

  // Date/time types (SQLite stores as TEXT, REAL, or INTEGER)
  DATE: { tsType: 'string' },
  DATETIME: { tsType: 'string' },
  TIMESTAMP: { tsType: 'string' },
  TIME: { tsType: 'string' },

  // Boolean (stored as INTEGER 0/1 in SQLite)
  BOOLEAN: { tsType: 'boolean' },
  BOOL: { tsType: 'boolean' },

  // JSON (stored as TEXT in SQLite)
  JSON: { tsType: 'unknown' },
  JSONB: { tsType: 'unknown' },

  // UUID (stored as TEXT in SQLite)
  UUID: { tsType: 'string' },
};

/**
 * Branded type mappings for enhanced type safety
 */
const BRANDED_TYPE_MAPPINGS: Record<string, TypeMapping> = {
  UUID: { tsType: 'UUID' },
  TIMESTAMP: { tsType: 'Timestamp' },
  DATETIME: { tsType: 'Timestamp' },
  DATE: { tsType: 'DateString' },
  JSON: { tsType: 'JsonValue' },
  JSONB: { tsType: 'JsonValue' },
};

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

/**
 * Convert a table name to PascalCase interface name
 */
export function toPascalCase(str: string): string {
  return str
    .split(/[_-]/)
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join('');
}

/**
 * Convert a column name to camelCase
 */
export function toCamelCase(str: string): string {
  const pascal = toPascalCase(str);
  return pascal.charAt(0).toLowerCase() + pascal.slice(1);
}

/**
 * Check if a column has NOT NULL constraint
 */
function hasNotNullConstraint(constraints: ColumnConstraint[]): boolean {
  return constraints.some(c => c.type === 'NOT NULL');
}

/**
 * Check if a column has PRIMARY KEY constraint
 */
function hasPrimaryKeyConstraint(constraints: ColumnConstraint[]): boolean {
  return constraints.some(c => c.type === 'PRIMARY KEY');
}

/**
 * Check if a column has DEFAULT constraint
 */
function hasDefaultConstraint(constraints: ColumnConstraint[]): { hasDefault: boolean; value?: string | number | boolean | null } {
  const defaultConstraint = constraints.find(c => c.type === 'DEFAULT');
  if (defaultConstraint && defaultConstraint.type === 'DEFAULT') {
    return { hasDefault: true, value: defaultConstraint.value };
  }
  return { hasDefault: false };
}

/**
 * Get the TypeScript type for a SQL data type
 */
function getSqlToTsType(
  dataType: ColumnDataType,
  options: SchemaInferenceOptions
): string {
  const typeName = dataType.name.toUpperCase();
  const customMapping = options.customTypeMappings?.[typeName];

  if (customMapping) {
    return customMapping.tsType;
  }

  if (options.useBrandedTypes && BRANDED_TYPE_MAPPINGS[typeName]) {
    return BRANDED_TYPE_MAPPINGS[typeName].tsType;
  }

  const mapping = DEFAULT_TYPE_MAPPINGS[typeName];
  return mapping?.tsType ?? 'unknown';
}

// =============================================================================
// CORE INFERENCE FUNCTIONS
// =============================================================================

/**
 * Infer a TypeScript field from a SQL column definition
 */
function inferField(
  column: ColumnDefinition,
  options: SchemaInferenceOptions
): GeneratedField {
  const tsType = getSqlToTsType(column.dataType, options);
  const isPrimaryKey = hasPrimaryKeyConstraint(column.constraints);
  const isNotNull = hasNotNullConstraint(column.constraints);
  const { hasDefault, value: defaultValue } = hasDefaultConstraint(column.constraints);

  // Determine nullability:
  // - NOT NULL columns are not nullable
  // - PRIMARY KEY columns are not nullable
  // - Columns with DEFAULT can be nullable in the select but not required in insert
  // - Other columns are nullable by default in SQLite
  const nullable = !isNotNull && !isPrimaryKey;

  let comment: string | undefined;
  if (options.includeComments) {
    const parts: string[] = [];
    if (isPrimaryKey) parts.push('@primaryKey');
    if (hasDefault) parts.push(`@default ${JSON.stringify(defaultValue)}`);
    if (column.dataType.precision !== undefined) {
      parts.push(`@precision ${column.dataType.precision}`);
    }
    if (column.dataType.scale !== undefined) {
      parts.push(`@scale ${column.dataType.scale}`);
    }
    if (parts.length > 0) {
      comment = parts.join(' ');
    }
  }

  return {
    name: column.name,
    type: tsType,
    nullable,
    isPrimaryKey,
    hasDefault,
    defaultValue,
    comment,
  };
}

/**
 * Generate TypeScript interface from a CREATE TABLE statement
 */
function generateInterfaceFromTable(
  table: CreateTableStatement,
  options: SchemaInferenceOptions
): GeneratedInterface {
  const prefix = options.interfacePrefix ?? '';
  const suffix = options.interfaceSuffix ?? 'Row';
  const interfaceName = `${prefix}${toPascalCase(table.name)}${suffix}`;

  const fields = table.columns.map(col => inferField(col, options));

  // Generate the interface code
  const lines: string[] = [];

  if (options.includeComments) {
    lines.push(`/**`);
    lines.push(` * Row type for the "${table.name}" table`);
    lines.push(` * Generated from SQL DDL`);
    lines.push(` */`);
  }

  lines.push(`export interface ${interfaceName} {`);

  for (const field of fields) {
    if (field.comment && options.includeComments) {
      lines.push(`  /** ${field.comment} */`);
    }

    const nullSuffix = field.nullable ? ' | null' : '';
    lines.push(`  ${field.name}: ${field.type}${nullSuffix};`);
  }

  lines.push(`}`);

  return {
    tableName: table.name,
    interfaceName,
    fields,
    code: lines.join('\n'),
  };
}

/**
 * Generate helper types (Insert, Update, PrimaryKey)
 */
function generateHelperTypes(
  interfaces: GeneratedInterface[],
  options: SchemaInferenceOptions
): GeneratedHelperTypes {
  if (!options.generateHelperTypes) {
    return {};
  }

  const lines: string[] = [];

  for (const iface of interfaces) {
    const baseName = iface.interfaceName.replace(/Row$/, '');

    // Generate Insert type (excludes auto-generated fields)
    const insertFields = iface.fields.filter(f => {
      // Exclude primary keys with autoincrement-like defaults
      if (f.isPrimaryKey && f.hasDefault) return false;
      return true;
    });

    const requiredInsertFields = insertFields.filter(f => !f.nullable && !f.hasDefault);
    const optionalInsertFields = insertFields.filter(f => f.nullable || f.hasDefault);

    if (options.includeComments) {
      lines.push(`/**`);
      lines.push(` * Insert data type for "${iface.tableName}"`);
      lines.push(` */`);
    }
    lines.push(`export type ${baseName}InsertData = {`);
    for (const field of requiredInsertFields) {
      lines.push(`  ${field.name}: ${field.type};`);
    }
    for (const field of optionalInsertFields) {
      const nullSuffix = field.nullable ? ' | null' : '';
      lines.push(`  ${field.name}?: ${field.type}${nullSuffix};`);
    }
    lines.push(`};`);
    lines.push('');

    // Generate Update type (all fields optional)
    if (options.includeComments) {
      lines.push(`/**`);
      lines.push(` * Update data type for "${iface.tableName}"`);
      lines.push(` */`);
    }
    lines.push(`export type ${baseName}UpdateData = Partial<${iface.interfaceName}>;`);
    lines.push('');

    // Generate PrimaryKey type
    const pkFields = iface.fields.filter(f => f.isPrimaryKey);
    if (pkFields.length > 0) {
      if (options.includeComments) {
        lines.push(`/**`);
        lines.push(` * Primary key type for "${iface.tableName}"`);
        lines.push(` */`);
      }
      if (pkFields.length === 1) {
        lines.push(`export type ${baseName}PrimaryKey = ${pkFields[0].type};`);
      } else {
        lines.push(`export type ${baseName}PrimaryKey = {`);
        for (const field of pkFields) {
          lines.push(`  ${field.name}: ${field.type};`);
        }
        lines.push(`};`);
      }
      lines.push('');
    }
  }

  return {
    insertType: lines.join('\n'),
  };
}

/**
 * Generate validation functions for the interfaces
 */
function generateValidationFunctions(
  interfaces: GeneratedInterface[],
  options: SchemaInferenceOptions
): string {
  if (!options.generateValidation) {
    return '';
  }

  const lines: string[] = [];

  lines.push(`// =============================================================================`);
  lines.push(`// VALIDATION FUNCTIONS`);
  lines.push(`// =============================================================================`);
  lines.push('');

  for (const iface of interfaces) {
    const fnName = `validate${iface.interfaceName.replace(/Row$/, '')}`;

    if (options.includeComments) {
      lines.push(`/**`);
      lines.push(` * Validate data for the "${iface.tableName}" table`);
      lines.push(` */`);
    }
    lines.push(`export function ${fnName}(data: unknown): data is ${iface.interfaceName} {`);
    lines.push(`  if (typeof data !== 'object' || data === null) return false;`);
    lines.push(`  const obj = data as Record<string, unknown>;`);

    for (const field of iface.fields) {
      const typeCheck = getTypeCheck(field.type, `obj.${field.name}`);

      if (field.nullable) {
        lines.push(`  // ${field.name} is optional`);
        lines.push(`  if (obj.${field.name} !== undefined && obj.${field.name} !== null && !(${typeCheck})) return false;`);
      } else {
        lines.push(`  // ${field.name} is required`);
        lines.push(`  if (!(${typeCheck})) return false;`);
      }
    }

    lines.push(`  return true;`);
    lines.push(`}`);
    lines.push('');
  }

  return lines.join('\n');
}

/**
 * Get the runtime type check expression for a TypeScript type
 */
function getTypeCheck(tsType: string, varName: string): string {
  switch (tsType) {
    case 'string':
      return `typeof ${varName} === 'string'`;
    case 'number':
      return `typeof ${varName} === 'number'`;
    case 'boolean':
      return `typeof ${varName} === 'boolean'`;
    case 'Uint8Array':
      return `${varName} instanceof Uint8Array`;
    case 'unknown':
      return `true`;
    default:
      // Handle branded types
      if (tsType === 'UUID' || tsType === 'DateString' || tsType === 'Timestamp') {
        return `typeof ${varName} === 'string'`;
      }
      if (tsType === 'JsonValue') {
        return `true`;
      }
      return `true`;
  }
}

// =============================================================================
// MAIN API
// =============================================================================

/**
 * Infer TypeScript types from SQL CREATE TABLE statements
 *
 * @param sql - One or more CREATE TABLE statements
 * @param options - Configuration options
 * @returns Generated TypeScript code and metadata
 *
 * @example
 * ```typescript
 * const sql = `
 *   CREATE TABLE users (
 *     id INTEGER PRIMARY KEY,
 *     name TEXT NOT NULL,
 *     email TEXT
 *   );
 * `;
 *
 * const result = inferSchemaFromDDL(sql);
 * console.log(result.fullCode);
 * // interface UsersRow {
 * //   id: number;
 * //   name: string;
 * //   email: string | null;
 * // }
 * ```
 */
export function inferSchemaFromDDL(
  sql: string,
  options: SchemaInferenceOptions = {}
): SchemaInferenceResult {
  const warnings: string[] = [];
  const interfaces: GeneratedInterface[] = [];

  // Split SQL into statements and parse each CREATE TABLE
  const statements = sql
    .split(';')
    .map(s => s.trim())
    .filter(s => s.length > 0);

  for (const stmt of statements) {
    const result = parseDDL(stmt);

    if (!result.success) {
      warnings.push(`Failed to parse statement: ${result.error}`);
      continue;
    }

    if (result.statement.type === 'CREATE TABLE') {
      const iface = generateInterfaceFromTable(result.statement, options);
      interfaces.push(iface);
    } else {
      // Skip non-CREATE TABLE statements (like CREATE INDEX)
    }
  }

  // Generate helper types
  const helpers = generateHelperTypes(interfaces, options);

  // Generate validation functions
  const validation = generateValidationFunctions(interfaces, options);

  // Assemble full code
  const fullCodeParts: string[] = [];

  fullCodeParts.push(`/**`);
  fullCodeParts.push(` * Generated TypeScript types from SQL DDL`);
  fullCodeParts.push(` * DO NOT EDIT - This file is auto-generated`);
  fullCodeParts.push(` */`);
  fullCodeParts.push('');

  // Add branded type definitions if needed
  if (options.useBrandedTypes) {
    fullCodeParts.push(`// =============================================================================`);
    fullCodeParts.push(`// BRANDED TYPES`);
    fullCodeParts.push(`// =============================================================================`);
    fullCodeParts.push('');
    fullCodeParts.push(`/** Branded type helper */`);
    fullCodeParts.push(`type Brand<T, B> = T & { readonly __brand: B };`);
    fullCodeParts.push('');
    fullCodeParts.push(`/** UUID string type */`);
    fullCodeParts.push(`export type UUID = Brand<string, 'UUID'>;`);
    fullCodeParts.push('');
    fullCodeParts.push(`/** Timestamp string type (ISO 8601) */`);
    fullCodeParts.push(`export type Timestamp = Brand<string, 'Timestamp'>;`);
    fullCodeParts.push('');
    fullCodeParts.push(`/** Date string type (YYYY-MM-DD) */`);
    fullCodeParts.push(`export type DateString = Brand<string, 'DateString'>;`);
    fullCodeParts.push('');
    fullCodeParts.push(`/** JSON value type */`);
    fullCodeParts.push(`export type JsonValue = string | number | boolean | null | JsonValue[] | Record<string, JsonValue>;`);
    fullCodeParts.push('');
  }

  // Add interfaces
  fullCodeParts.push(`// =============================================================================`);
  fullCodeParts.push(`// ROW TYPES`);
  fullCodeParts.push(`// =============================================================================`);
  fullCodeParts.push('');

  for (const iface of interfaces) {
    fullCodeParts.push(iface.code);
    fullCodeParts.push('');
  }

  // Add helper types
  if (helpers.insertType) {
    fullCodeParts.push(`// =============================================================================`);
    fullCodeParts.push(`// HELPER TYPES`);
    fullCodeParts.push(`// =============================================================================`);
    fullCodeParts.push('');
    fullCodeParts.push(helpers.insertType);
  }

  // Add validation functions
  if (validation) {
    fullCodeParts.push(validation);
  }

  return {
    interfaces,
    helpers,
    fullCode: fullCodeParts.join('\n'),
    warnings,
  };
}

/**
 * Generate TypeScript types from a single CREATE TABLE statement
 *
 * @param sql - A single CREATE TABLE statement
 * @param options - Configuration options
 * @returns Generated interface or null if parsing fails
 */
export function inferTableType(
  sql: string,
  options: SchemaInferenceOptions = {}
): GeneratedInterface | null {
  const result = parseCreateTable(sql);

  if (!result.success) {
    return null;
  }

  return generateInterfaceFromTable(result.statement, options);
}

/**
 * Quick helper to generate TypeScript code from SQL DDL
 *
 * @param sql - SQL DDL statements
 * @returns Generated TypeScript code string
 */
export function ddlToTypeScript(sql: string): string {
  const result = inferSchemaFromDDL(sql, {
    includeComments: true,
    generateHelperTypes: true,
  });
  return result.fullCode;
}

/**
 * Generate TypeScript types with query helpers
 *
 * @param sql - SQL DDL statements
 * @returns Generated TypeScript code with query helpers
 */
export function ddlToTypeScriptWithHelpers(sql: string): string {
  const result = inferSchemaFromDDL(sql, {
    includeComments: true,
    generateHelperTypes: true,
    generateValidation: true,
    useBrandedTypes: true,
  });
  return result.fullCode;
}
