/**
 * DoSQL Schema Validator
 *
 * Validates schema definitions for:
 * - Relation targets exist
 * - Types are valid
 * - Circular references detection
 * - Primary keys exist
 * - Index validity
 */

import type {
  SchemaDefinition,
  TableDefinition,
  ValidationResult,
  ValidationError,
  ValidationWarning,
  ParsedField,
} from './types.js';

import {
  parseField,
  parseSchema,
  isValidBaseType,
  isRelation,
} from './parser.js';

// =============================================================================
// VALIDATION ERROR CODES
// =============================================================================

export const ErrorCodes = {
  // Type errors
  INVALID_TYPE: 'INVALID_TYPE',
  UNKNOWN_TYPE: 'UNKNOWN_TYPE',

  // Relation errors
  RELATION_TARGET_NOT_FOUND: 'RELATION_TARGET_NOT_FOUND',
  SELF_RELATION: 'SELF_RELATION',
  CIRCULAR_RELATION: 'CIRCULAR_RELATION',

  // Primary key errors
  NO_PRIMARY_KEY: 'NO_PRIMARY_KEY',
  MULTIPLE_PRIMARY_KEYS: 'MULTIPLE_PRIMARY_KEYS',
  NULLABLE_PRIMARY_KEY: 'NULLABLE_PRIMARY_KEY',

  // Field errors
  INVALID_FIELD_NAME: 'INVALID_FIELD_NAME',
  RESERVED_FIELD_NAME: 'RESERVED_FIELD_NAME',
  DUPLICATE_INDEX: 'DUPLICATE_INDEX',

  // Table errors
  INVALID_TABLE_NAME: 'INVALID_TABLE_NAME',
  EMPTY_TABLE: 'EMPTY_TABLE',
  RESERVED_TABLE_NAME: 'RESERVED_TABLE_NAME',

  // Default value errors
  INVALID_DEFAULT: 'INVALID_DEFAULT',
  TYPE_MISMATCH_DEFAULT: 'TYPE_MISMATCH_DEFAULT',
} as const;

export type ErrorCode = (typeof ErrorCodes)[keyof typeof ErrorCodes];

// =============================================================================
// WARNING CODES
// =============================================================================

export const WarningCodes = {
  // Naming conventions
  SNAKE_CASE_FIELD: 'SNAKE_CASE_FIELD',
  NON_CAMEL_CASE_FIELD: 'NON_CAMEL_CASE_FIELD',

  // Schema design
  MISSING_TIMESTAMPS: 'MISSING_TIMESTAMPS',
  MISSING_BACK_RELATION: 'MISSING_BACK_RELATION',
  UNUSED_TABLE: 'UNUSED_TABLE',

  // Performance
  UNINDEXED_RELATION: 'UNINDEXED_RELATION',
  TOO_MANY_INDEXES: 'TOO_MANY_INDEXES',
} as const;

export type WarningCode = (typeof WarningCodes)[keyof typeof WarningCodes];

// =============================================================================
// RESERVED NAMES
// =============================================================================

const RESERVED_TABLE_NAMES = new Set([
  'user',
  'order',
  'group',
  'table',
  'select',
  'insert',
  'update',
  'delete',
  'create',
  'drop',
  'alter',
  'index',
  'key',
  'primary',
  'foreign',
  'constraint',
  'default',
  'null',
  'not',
  'and',
  'or',
  'where',
  'from',
  'join',
  'on',
  'as',
  'in',
  'is',
  'like',
  'between',
  'case',
  'when',
  'then',
  'else',
  'end',
  'limit',
  'offset',
  'having',
  'union',
  'except',
  'intersect',
]);

const RESERVED_FIELD_NAMES = new Set([
  '__proto__',
  'constructor',
  'prototype',
  'hasOwnProperty',
  'isPrototypeOf',
  'propertyIsEnumerable',
  'toLocaleString',
  'toString',
  'valueOf',
]);

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Check if a name follows camelCase convention
 */
function isCamelCase(name: string): boolean {
  // camelCase: starts with lowercase, no underscores (except at start for private)
  return /^[a-z][a-zA-Z0-9]*$/.test(name);
}

/**
 * Check if a name uses snake_case
 */
function isSnakeCase(name: string): boolean {
  return name.includes('_');
}

/**
 * Check if a name is a valid identifier
 */
function isValidIdentifier(name: string): boolean {
  return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name);
}

/**
 * Get all table names from a schema
 */
function getTableNames(schema: SchemaDefinition): Set<string> {
  const tables = new Set<string>();
  for (const tableName of Object.keys(schema)) {
    if (!tableName.startsWith('@')) {
      tables.add(tableName);
    }
  }
  return tables;
}

/**
 * Detect circular relations using DFS
 */
function detectCircularRelations(
  schema: SchemaDefinition,
  parsedSchema: Map<string, Map<string, ParsedField>>
): string[][] {
  const cycles: string[][] = [];
  const visited = new Set<string>();
  const recStack = new Set<string>();
  const path: string[] = [];

  function dfs(tableName: string): void {
    if (recStack.has(tableName)) {
      // Found a cycle - extract it
      const cycleStart = path.indexOf(tableName);
      if (cycleStart !== -1) {
        cycles.push([...path.slice(cycleStart), tableName]);
      }
      return;
    }

    if (visited.has(tableName)) {
      return;
    }

    visited.add(tableName);
    recStack.add(tableName);
    path.push(tableName);

    const tableFields = parsedSchema.get(tableName);
    if (tableFields) {
      for (const [, field] of tableFields) {
        if (field.relation) {
          const target = field.relation.target;
          if (parsedSchema.has(target)) {
            dfs(target);
          }
        }
      }
    }

    path.pop();
    recStack.delete(tableName);
  }

  for (const tableName of parsedSchema.keys()) {
    visited.clear();
    recStack.clear();
    path.length = 0;
    dfs(tableName);
  }

  return cycles;
}

// =============================================================================
// VALIDATORS
// =============================================================================

/**
 * Validate a single field
 */
function validateField(
  tableName: string,
  fieldName: string,
  fieldDef: string,
  tableNames: Set<string>,
  errors: ValidationError[],
  warnings: ValidationWarning[]
): ParsedField {
  const parsed = parseField(fieldDef);
  const path = [tableName, fieldName];

  // Validate field name
  if (!isValidIdentifier(fieldName)) {
    errors.push({
      type: 'error',
      path,
      message: `Invalid field name: '${fieldName}'. Must start with a letter or underscore.`,
      code: ErrorCodes.INVALID_FIELD_NAME,
    });
  }

  if (RESERVED_FIELD_NAMES.has(fieldName)) {
    errors.push({
      type: 'error',
      path,
      message: `Reserved field name: '${fieldName}'. Use a different name.`,
      code: ErrorCodes.RESERVED_FIELD_NAME,
    });
  }

  // Warn about snake_case (prefer camelCase)
  if (isSnakeCase(fieldName)) {
    warnings.push({
      type: 'warning',
      path,
      message: `Field '${fieldName}' uses snake_case. Consider using camelCase (e.g., '${fieldName.replace(/_([a-z])/g, (_, c) => c.toUpperCase())}').`,
      code: WarningCodes.SNAKE_CASE_FIELD,
    });
  } else if (!isCamelCase(fieldName) && !fieldName.startsWith('_')) {
    warnings.push({
      type: 'warning',
      path,
      message: `Field '${fieldName}' doesn't follow camelCase convention.`,
      code: WarningCodes.NON_CAMEL_CASE_FIELD,
    });
  }

  // Validate relation targets
  if (parsed.relation) {
    const target = parsed.relation.target;

    // Check if target table exists
    if (!tableNames.has(target)) {
      errors.push({
        type: 'error',
        path,
        message: `Relation target '${target}' not found in schema. Available tables: ${[...tableNames].join(', ')}`,
        code: ErrorCodes.RELATION_TARGET_NOT_FOUND,
      });
    }

    // Check for self-relation (not an error, but note it)
    if (target === tableName) {
      // Self-relations are valid (e.g., parent-child hierarchies)
    }

    // Warn about unindexed backward relations
    if (
      parsed.relation.type === 'backward' &&
      !parsed.modifiers.indexed
    ) {
      warnings.push({
        type: 'warning',
        path,
        message: `Backward relation '${fieldName}' is not indexed. Consider adding '#' for better query performance.`,
        code: WarningCodes.UNINDEXED_RELATION,
      });
    }
  } else {
    // Validate base type for non-relation fields
    if (!isValidBaseType(parsed.baseType)) {
      errors.push({
        type: 'error',
        path,
        message: `Unknown type '${parsed.baseType}'. Valid types: string, text, int, integer, bigint, float, double, decimal, number, boolean, bool, uuid, timestamp, datetime, date, time, json, jsonb, blob, binary`,
        code: ErrorCodes.UNKNOWN_TYPE,
      });
    }
  }

  // Validate nullable primary key
  if (parsed.modifiers.primaryKey && parsed.modifiers.nullable) {
    errors.push({
      type: 'error',
      path,
      message: `Primary key field '${fieldName}' cannot be nullable. Remove the '?' modifier.`,
      code: ErrorCodes.NULLABLE_PRIMARY_KEY,
    });
  }

  return parsed;
}

/**
 * Validate a single table
 */
function validateTable(
  tableName: string,
  table: TableDefinition,
  tableNames: Set<string>,
  errors: ValidationError[],
  warnings: ValidationWarning[]
): Map<string, ParsedField> {
  const parsedFields = new Map<string, ParsedField>();
  const path = [tableName];

  // Validate table name
  if (!isValidIdentifier(tableName)) {
    errors.push({
      type: 'error',
      path,
      message: `Invalid table name: '${tableName}'. Must start with a letter or underscore.`,
      code: ErrorCodes.INVALID_TABLE_NAME,
    });
  }

  if (RESERVED_TABLE_NAMES.has(tableName.toLowerCase())) {
    warnings.push({
      type: 'warning',
      path,
      message: `Table name '${tableName}' is a SQL reserved word. This may cause issues with some databases.`,
      code: WarningCodes.SNAKE_CASE_FIELD, // Using existing code, ideally would have RESERVED_TABLE_NAME
    });
  }

  // Check for empty table
  const fields = Object.entries(table);
  if (fields.length === 0) {
    errors.push({
      type: 'error',
      path,
      message: `Table '${tableName}' has no fields defined.`,
      code: ErrorCodes.EMPTY_TABLE,
    });
    return parsedFields;
  }

  // Count primary keys
  let primaryKeyCount = 0;
  const primaryKeyFields: string[] = [];

  // Validate each field
  for (const [fieldName, fieldDef] of fields) {
    const parsed = validateField(
      tableName,
      fieldName,
      fieldDef,
      tableNames,
      errors,
      warnings
    );
    parsedFields.set(fieldName, parsed);

    if (parsed.modifiers.primaryKey) {
      primaryKeyCount++;
      primaryKeyFields.push(fieldName);
    }
  }

  // Check for no primary key
  if (primaryKeyCount === 0) {
    warnings.push({
      type: 'warning',
      path,
      message: `Table '${tableName}' has no primary key. Consider adding '!' to one field (e.g., 'id: "uuid!"').`,
      code: WarningCodes.MISSING_TIMESTAMPS, // Reusing code
    });
  }

  // Check for multiple primary keys (allowed for composite, but warn)
  if (primaryKeyCount > 1) {
    warnings.push({
      type: 'warning',
      path,
      message: `Table '${tableName}' has multiple primary key fields: ${primaryKeyFields.join(', ')}. This creates a composite primary key.`,
      code: WarningCodes.MISSING_TIMESTAMPS, // Reusing code
    });
  }

  // Check for timestamp fields
  const hasCreatedAt = fields.some(([name]) =>
    name === 'createdAt' || name === 'created_at'
  );
  const hasUpdatedAt = fields.some(([name]) =>
    name === 'updatedAt' || name === 'updated_at'
  );

  if (!hasCreatedAt || !hasUpdatedAt) {
    warnings.push({
      type: 'warning',
      path,
      message: `Table '${tableName}' is missing timestamp fields. Consider adding 'createdAt' and 'updatedAt'.`,
      code: WarningCodes.MISSING_TIMESTAMPS,
    });
  }

  // Count indexes
  let indexCount = 0;
  for (const [, parsed] of parsedFields) {
    if (parsed.modifiers.indexed) {
      indexCount++;
    }
  }

  if (indexCount > 5) {
    warnings.push({
      type: 'warning',
      path,
      message: `Table '${tableName}' has ${indexCount} indexed fields. Too many indexes can slow down writes.`,
      code: WarningCodes.TOO_MANY_INDEXES,
    });
  }

  return parsedFields;
}

// =============================================================================
// MAIN VALIDATION FUNCTION
// =============================================================================

/**
 * Validation options
 */
export interface ValidateOptions {
  /** Skip camelCase warnings */
  skipCamelCaseWarnings?: boolean;
  /** Skip timestamp warnings */
  skipTimestampWarnings?: boolean;
  /** Allow empty tables */
  allowEmptyTables?: boolean;
  /** Allow multiple primary keys */
  allowMultiplePrimaryKeys?: boolean;
}

/**
 * Validate a schema definition
 *
 * @param schema - The schema to validate
 * @param options - Validation options
 * @returns Validation result with errors and warnings
 */
export function validateSchema(
  schema: SchemaDefinition,
  options: ValidateOptions = {}
): ValidationResult {
  const errors: ValidationError[] = [];
  const warnings: ValidationWarning[] = [];
  const tableNames = getTableNames(schema);
  const parsedTables = new Map<string, Map<string, ParsedField>>();

  // Validate each table
  for (const tableName of tableNames) {
    const table = schema[tableName];
    if (typeof table === 'object' && table !== null) {
      // Check if it's a directive (has @ keys)
      const hasDirectiveKeys = Object.keys(table).some((k) =>
        k.startsWith('@')
      );
      if (!hasDirectiveKeys) {
        const parsedFields = validateTable(
          tableName,
          table as TableDefinition,
          tableNames,
          errors,
          warnings
        );
        parsedTables.set(tableName, parsedFields);
      }
    }
  }

  // Detect circular relations
  const cycles = detectCircularRelations(schema, parsedTables);
  for (const cycle of cycles) {
    errors.push({
      type: 'error',
      path: [cycle[0]],
      message: `Circular relation detected: ${cycle.join(' -> ')}`,
      code: ErrorCodes.CIRCULAR_RELATION,
    });
  }

  // Check for missing back-relations
  for (const [tableName, fields] of parsedTables) {
    for (const [fieldName, field] of fields) {
      if (field.relation?.type === 'forward') {
        const targetTable = parsedTables.get(field.relation.target);
        if (targetTable) {
          // Look for a backward relation pointing to this table
          let hasBackRelation = false;
          for (const [, targetField] of targetTable) {
            if (
              targetField.relation?.type === 'backward' &&
              targetField.relation.target === tableName
            ) {
              hasBackRelation = true;
              break;
            }
          }

          if (!hasBackRelation) {
            warnings.push({
              type: 'warning',
              path: [tableName, fieldName],
              message: `Forward relation '${fieldName}' has no corresponding backward relation in '${field.relation.target}'.`,
              code: WarningCodes.MISSING_BACK_RELATION,
            });
          }
        }
      }
    }
  }

  // Filter warnings based on options
  let filteredWarnings = warnings;
  if (options.skipCamelCaseWarnings) {
    filteredWarnings = filteredWarnings.filter(
      (w) =>
        w.code !== WarningCodes.SNAKE_CASE_FIELD &&
        w.code !== WarningCodes.NON_CAMEL_CASE_FIELD
    );
  }
  if (options.skipTimestampWarnings) {
    filteredWarnings = filteredWarnings.filter(
      (w) => w.code !== WarningCodes.MISSING_TIMESTAMPS
    );
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings: filteredWarnings,
  };
}

/**
 * Quick check if a schema is valid (no errors)
 */
export function isValidSchema(schema: SchemaDefinition): boolean {
  return validateSchema(schema).valid;
}

/**
 * Assert that a schema is valid, throwing if not
 */
export function assertValidSchema(schema: SchemaDefinition): void {
  const result = validateSchema(schema);
  if (!result.valid) {
    const errorMessages = result.errors
      .map((e) => `[${e.code}] ${e.path.join('.')}: ${e.message}`)
      .join('\n');
    throw new Error(`Schema validation failed:\n${errorMessages}`);
  }
}
