/**
 * DoSQL IceType-inspired Schema DSL - Core Types
 *
 * Field type notation:
 * - `!` = required/unique (primary key when combined with base type like `uuid!`)
 * - `#` = indexed
 * - `?` = optional/nullable
 * - `->` = forward relation (has-many)
 * - `<-` = backward relation (belongs-to)
 * - `[]` = array
 * - `= "value"` = default value
 *
 * Examples:
 * - `'string'` - required string
 * - `'int!'` - required integer (primary key candidate)
 * - `'uuid!#'` - required UUID, indexed (primary key)
 * - `'timestamp?'` - optional timestamp (nullable)
 * - `'-> Order[]'` - forward relation to many Orders
 * - `'<- User'` - backward relation to User
 * - `'string = "pending"'` - string with default value
 */

// =============================================================================
// BASE SQL TYPES
// =============================================================================

/**
 * Supported SQL/data types
 */
export type SqlBaseType =
  | 'string'
  | 'text'
  | 'int'
  | 'integer'
  | 'bigint'
  | 'float'
  | 'double'
  | 'decimal'
  | 'number'
  | 'boolean'
  | 'bool'
  | 'uuid'
  | 'timestamp'
  | 'datetime'
  | 'date'
  | 'time'
  | 'json'
  | 'jsonb'
  | 'blob'
  | 'binary';

/**
 * Type mappings from SQL types to TypeScript types
 */
export type SqlTypeToTs<T extends string> =
  T extends 'string' | 'text' | 'uuid' ? string :
  T extends 'int' | 'integer' | 'bigint' | 'float' | 'double' | 'number' ? number :
  T extends `decimal${string}` ? number :
  T extends 'boolean' | 'bool' ? boolean :
  T extends 'timestamp' | 'datetime' | 'date' | 'time' ? Date :
  T extends 'json' | 'jsonb' ? unknown :
  T extends 'blob' | 'binary' ? Uint8Array :
  unknown;

// =============================================================================
// FIELD MODIFIERS
// =============================================================================

/**
 * Field modifiers that can be applied to base types
 */
export interface FieldModifiers {
  /** Is this field required (non-nullable)? */
  required: boolean;
  /** Is this a primary key? (! modifier) */
  primaryKey: boolean;
  /** Is this field indexed? (# modifier) */
  indexed: boolean;
  /** Is this field optional/nullable? (? modifier) */
  nullable: boolean;
  /** Is this an array type? ([] modifier) */
  isArray: boolean;
  /** Default value if specified */
  defaultValue?: string | undefined;
}

/**
 * Relation types
 */
export type RelationType = 'forward' | 'backward' | 'none';

/**
 * Parsed relation info
 */
export interface RelationInfo {
  type: RelationType;
  /** Target table name */
  target: string;
  /** Is this a to-many relation? */
  isMany: boolean;
}

// =============================================================================
// PARSED FIELD
// =============================================================================

/**
 * Fully parsed field definition
 */
export interface ParsedField {
  /** Original field definition string */
  raw: string;
  /** Base SQL type (e.g., 'string', 'int', 'uuid') */
  baseType: string;
  /** Field modifiers */
  modifiers: FieldModifiers;
  /** Relation info (if this is a relation field) */
  relation?: RelationInfo | undefined;
}

// =============================================================================
// SCHEMA DEFINITION TYPES
// =============================================================================

/**
 * A single field type definition (the DSL string)
 *
 * Examples:
 * - `'string'`
 * - `'int!'`
 * - `'uuid!#'`
 * - `'timestamp?'`
 * - `'-> Order[]'`
 * - `'<- User'`
 * - `'string = "pending"'`
 */
export type FieldType = string;

/**
 * Table definition: mapping of field names to field types
 */
export type TableDefinition = {
  [fieldName: string]: FieldType;
};

/**
 * Schema directive (for metadata like @index, @constraint, etc.)
 * These start with @ and provide table-level or schema-level configuration
 */
export type SchemaDirective = {
  '@index'?: string[];
  '@unique'?: string[];
  '@check'?: string;
  '@comment'?: string;
};

/**
 * Full schema definition: mapping of table names to table definitions
 */
export type SchemaDefinition = {
  [tableName: string]: TableDefinition | SchemaDirective;
};

// =============================================================================
// TYPE-LEVEL UTILITIES
// =============================================================================

/**
 * Check if a string starts with a given prefix
 */
export type StartsWith<S extends string, Prefix extends string> =
  S extends `${Prefix}${string}` ? true : false;

/**
 * Check if a string ends with a given suffix
 */
export type EndsWith<S extends string, Suffix extends string> =
  S extends `${string}${Suffix}` ? true : false;

/**
 * Check if a string contains a substring
 */
export type Contains<S extends string, Sub extends string> =
  S extends `${string}${Sub}${string}` ? true : false;

/**
 * Trim whitespace from both ends of a string
 */
export type Trim<S extends string> =
  S extends ` ${infer R}` ? Trim<R> :
  S extends `${infer R} ` ? Trim<R> :
  S extends `\t${infer R}` ? Trim<R> :
  S extends `${infer R}\t` ? Trim<R> :
  S extends `\n${infer R}` ? Trim<R> :
  S extends `${infer R}\n` ? Trim<R> :
  S;

/**
 * Extract everything before a pattern
 */
export type Before<S extends string, Pattern extends string> =
  S extends `${infer Head}${Pattern}${string}` ? Head : S;

/**
 * Extract everything after a pattern
 */
export type After<S extends string, Pattern extends string> =
  S extends `${string}${Pattern}${infer Tail}` ? Tail : '';

// =============================================================================
// VALIDATION RESULT TYPES
// =============================================================================

/**
 * Validation error
 */
export interface ValidationError {
  type: 'error';
  path: string[];
  message: string;
  code: string;
}

/**
 * Validation warning
 */
export interface ValidationWarning {
  type: 'warning';
  path: string[];
  message: string;
  code: string;
}

/**
 * Validation result
 */
export interface ValidationResult {
  valid: boolean;
  errors: ValidationError[];
  warnings: ValidationWarning[];
}

// =============================================================================
// CODEGEN OUTPUT TYPES
// =============================================================================

/**
 * Generated TypeScript interface
 */
export interface GeneratedInterface {
  name: string;
  code: string;
  fields: Array<{
    name: string;
    type: string;
    optional: boolean;
    comment?: string;
  }>;
}

/**
 * Generated SQL CREATE TABLE statement
 */
export interface GeneratedSql {
  tableName: string;
  sql: string;
  columns: Array<{
    name: string;
    type: string;
    constraints: string[];
  }>;
  indexes: string[];
  foreignKeys: string[];
}

/**
 * Full codegen output
 */
export interface CodegenOutput {
  typescript: {
    interfaces: GeneratedInterface[];
    fullCode: string;
  };
  sql: {
    tables: GeneratedSql[];
    fullScript: string;
  };
}
