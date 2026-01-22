/**
 * DoSQL Schema DSL Parser
 *
 * Parses field definitions in the IceType-inspired DSL format.
 *
 * Field type notation:
 * - `!` = required/unique (primary key)
 * - `#` = indexed
 * - `?` = optional/nullable
 * - `->` = forward relation (has-many)
 * - `<-` = backward relation (belongs-to)
 * - `[]` = array
 * - `= "value"` = default value
 */

import type {
  FieldType,
  ParsedField,
  FieldModifiers,
  RelationInfo,
  RelationType,
  SqlBaseType,
  TableDefinition,
  SchemaDefinition,
} from './types.js';

// =============================================================================
// REGEX PATTERNS
// =============================================================================

/** Pattern to match relation prefixes */
const FORWARD_RELATION_PATTERN = /^->\s*/;
const BACKWARD_RELATION_PATTERN = /^<-\s*/;

/** Pattern to match array suffix */
const ARRAY_PATTERN = /\[\]$/;

/** Pattern to match modifiers at the end of type (!, #, ?) */
const MODIFIERS_PATTERN = /([!#?]+)$/;

/** Pattern to match default value */
const DEFAULT_PATTERN = /\s*=\s*(?:"([^"]*)"|([\w()]+))\s*$/;

/** Pattern to match decimal with precision: decimal(10,2) */
const DECIMAL_PRECISION_PATTERN = /^decimal\(\d+(?:,\s*\d+)?\)/i;

/** Valid SQL base types */
const VALID_BASE_TYPES: Set<string> = new Set([
  'string',
  'text',
  'int',
  'integer',
  'bigint',
  'float',
  'double',
  'decimal',
  'number',
  'boolean',
  'bool',
  'uuid',
  'timestamp',
  'datetime',
  'date',
  'time',
  'json',
  'jsonb',
  'blob',
  'binary',
]);

// =============================================================================
// FIELD PARSER
// =============================================================================

/**
 * Create default modifiers
 */
function createDefaultModifiers(): FieldModifiers {
  return {
    required: true, // By default, fields are required (non-nullable)
    primaryKey: false,
    indexed: false,
    nullable: false,
    isArray: false,
    defaultValue: undefined,
  };
}

/**
 * Parse a field type definition string
 *
 * @param fieldDef - The field definition string (e.g., 'uuid!#', '-> Order[]')
 * @returns ParsedField object with all extracted information
 */
export function parseField(fieldDef: FieldType): ParsedField {
  let remaining = fieldDef.trim();
  const modifiers = createDefaultModifiers();
  let relation: RelationInfo | undefined;

  // 1. Extract default value first (if present)
  const defaultMatch = remaining.match(DEFAULT_PATTERN);
  if (defaultMatch) {
    modifiers.defaultValue = defaultMatch[1] ?? defaultMatch[2];
    remaining = remaining.replace(DEFAULT_PATTERN, '').trim();
  }

  // 2. Check for forward relation (->)
  if (FORWARD_RELATION_PATTERN.test(remaining)) {
    remaining = remaining.replace(FORWARD_RELATION_PATTERN, '').trim();

    // Check for array (many relation)
    const isMany = ARRAY_PATTERN.test(remaining);
    if (isMany) {
      remaining = remaining.replace(ARRAY_PATTERN, '').trim();
      modifiers.isArray = true;
    }

    relation = {
      type: 'forward',
      target: remaining,
      isMany,
    };

    return {
      raw: fieldDef,
      baseType: remaining, // The target table name is the "type"
      modifiers,
      relation,
    };
  }

  // 3. Check for backward relation (<-)
  if (BACKWARD_RELATION_PATTERN.test(remaining)) {
    remaining = remaining.replace(BACKWARD_RELATION_PATTERN, '').trim();

    // Check for array (many relation - unusual but supported)
    const isMany = ARRAY_PATTERN.test(remaining);
    if (isMany) {
      remaining = remaining.replace(ARRAY_PATTERN, '').trim();
      modifiers.isArray = true;
    }

    relation = {
      type: 'backward',
      target: remaining,
      isMany,
    };

    return {
      raw: fieldDef,
      baseType: remaining, // The target table name is the "type"
      modifiers,
      relation,
    };
  }

  // 4. Check for array type (before modifiers)
  if (ARRAY_PATTERN.test(remaining)) {
    remaining = remaining.replace(ARRAY_PATTERN, '').trim();
    modifiers.isArray = true;
  }

  // 5. Extract modifiers (!, #, ?)
  const modifiersMatch = remaining.match(MODIFIERS_PATTERN);
  if (modifiersMatch) {
    const modifierChars = modifiersMatch[1];
    remaining = remaining.slice(0, -modifierChars.length).trim();

    // Parse each modifier
    if (modifierChars.includes('!')) {
      modifiers.primaryKey = true;
      modifiers.required = true;
      modifiers.nullable = false;
    }
    if (modifierChars.includes('#')) {
      modifiers.indexed = true;
    }
    if (modifierChars.includes('?')) {
      modifiers.nullable = true;
      modifiers.required = false;
    }
  }

  // 6. The remaining string is the base type
  let baseType = remaining.toLowerCase();

  // Handle decimal with precision (keep original case for precision)
  const decimalMatch = remaining.match(DECIMAL_PRECISION_PATTERN);
  if (decimalMatch) {
    baseType = decimalMatch[0].toLowerCase();
  }

  return {
    raw: fieldDef,
    baseType,
    modifiers,
    relation,
  };
}

/**
 * Parse all fields in a table definition
 *
 * @param table - Table definition object
 * @returns Map of field names to parsed fields
 */
export function parseTable(table: TableDefinition): Map<string, ParsedField> {
  const result = new Map<string, ParsedField>();

  for (const [fieldName, fieldDef] of Object.entries(table)) {
    result.set(fieldName, parseField(fieldDef));
  }

  return result;
}

/**
 * Parse an entire schema definition
 *
 * @param schema - Schema definition object
 * @returns Map of table names to maps of field names to parsed fields
 */
export function parseSchema(
  schema: SchemaDefinition
): Map<string, Map<string, ParsedField>> {
  const result = new Map<string, Map<string, ParsedField>>();

  for (const [tableName, tableDef] of Object.entries(schema)) {
    // Skip schema directives (start with @)
    if (tableName.startsWith('@')) {
      continue;
    }

    // Type guard: check if it's a table definition (not a directive)
    if (typeof tableDef === 'object' && tableDef !== null) {
      // Check if it's a directive object (has @ keys)
      const hasDirectiveKeys = Object.keys(tableDef).some((k) =>
        k.startsWith('@')
      );
      if (!hasDirectiveKeys) {
        result.set(tableName, parseTable(tableDef as TableDefinition));
      }
    }
  }

  return result;
}

// =============================================================================
// TYPE CHECKING UTILITIES
// =============================================================================

/**
 * Check if a base type is a valid SQL type
 */
export function isValidBaseType(type: string): type is SqlBaseType {
  // Check for decimal with precision
  if (/^decimal\(\d+(?:,\s*\d+)?\)$/i.test(type)) {
    return true;
  }
  return VALID_BASE_TYPES.has(type.toLowerCase());
}

/**
 * Check if a field definition represents a relation
 */
export function isRelation(fieldDef: FieldType): boolean {
  const trimmed = fieldDef.trim();
  return (
    FORWARD_RELATION_PATTERN.test(trimmed) ||
    BACKWARD_RELATION_PATTERN.test(trimmed)
  );
}

/**
 * Check if a field definition is a forward relation (->)
 */
export function isForwardRelation(fieldDef: FieldType): boolean {
  return FORWARD_RELATION_PATTERN.test(fieldDef.trim());
}

/**
 * Check if a field definition is a backward relation (<-)
 */
export function isBackwardRelation(fieldDef: FieldType): boolean {
  return BACKWARD_RELATION_PATTERN.test(fieldDef.trim());
}

/**
 * Get the relation target from a field definition
 */
export function getRelationTarget(fieldDef: FieldType): string | null {
  const parsed = parseField(fieldDef);
  return parsed.relation?.target ?? null;
}

/**
 * Check if a field is nullable
 */
export function isNullable(fieldDef: FieldType): boolean {
  return parseField(fieldDef).modifiers.nullable;
}

/**
 * Check if a field is a primary key
 */
export function isPrimaryKey(fieldDef: FieldType): boolean {
  return parseField(fieldDef).modifiers.primaryKey;
}

/**
 * Check if a field is indexed
 */
export function isIndexed(fieldDef: FieldType): boolean {
  return parseField(fieldDef).modifiers.indexed;
}

/**
 * Check if a field is an array type
 */
export function isArrayType(fieldDef: FieldType): boolean {
  return parseField(fieldDef).modifiers.isArray;
}

/**
 * Get the default value of a field (if any)
 */
export function getDefaultValue(fieldDef: FieldType): string | undefined {
  return parseField(fieldDef).modifiers.defaultValue;
}

// =============================================================================
// TYPE-LEVEL PARSER (for compile-time inference)
// =============================================================================

/**
 * Type-level: Extract default value from field definition
 */
export type ExtractDefault<F extends string> =
  F extends `${string}= "${infer D}"` ? D :
  F extends `${string}= ${infer D}` ? D :
  never;

/**
 * Type-level: Remove default value from field definition
 */
export type RemoveDefault<F extends string> =
  F extends `${infer Base} = "${string}"` ? Trim<Base> :
  F extends `${infer Base} = ${string}` ? Trim<Base> :
  F extends `${infer Base}= "${string}"` ? Trim<Base> :
  F extends `${infer Base}=${string}` ? Trim<Base> :
  F;

/**
 * Type-level: Check if field is a forward relation
 */
export type IsForwardRelation<F extends string> =
  RemoveDefault<F> extends `-> ${string}` ? true :
  RemoveDefault<F> extends `->${string}` ? true :
  false;

/**
 * Type-level: Check if field is a backward relation
 */
export type IsBackwardRelation<F extends string> =
  RemoveDefault<F> extends `<- ${string}` ? true :
  RemoveDefault<F> extends `<-${string}` ? true :
  false;

/**
 * Type-level: Check if field is any relation
 */
export type IsRelation<F extends string> =
  IsForwardRelation<F> extends true ? true :
  IsBackwardRelation<F> extends true ? true :
  false;

/**
 * Type-level: Extract relation target
 */
export type ExtractRelationTarget<F extends string> =
  RemoveDefault<F> extends `-> ${infer T}[]` ? T :
  RemoveDefault<F> extends `->${infer T}[]` ? T :
  RemoveDefault<F> extends `-> ${infer T}` ? T :
  RemoveDefault<F> extends `->${infer T}` ? T :
  RemoveDefault<F> extends `<- ${infer T}[]` ? T :
  RemoveDefault<F> extends `<-${infer T}[]` ? T :
  RemoveDefault<F> extends `<- ${infer T}` ? T :
  RemoveDefault<F> extends `<-${infer T}` ? T :
  never;

/**
 * Type-level: Check if relation is to-many (array)
 */
export type IsToManyRelation<F extends string> =
  RemoveDefault<F> extends `${string}[]` ? true : false;

/**
 * Type-level: Check if field is nullable (has ?)
 */
export type IsNullable<F extends string> =
  RemoveDefault<F> extends `${string}?${string}` ? true :
  RemoveDefault<F> extends `${string}?` ? true :
  false;

/**
 * Type-level: Check if field is primary key (has !)
 */
export type IsPrimaryKey<F extends string> =
  RemoveDefault<F> extends `${string}!${string}` ? true :
  RemoveDefault<F> extends `${string}!` ? true :
  false;

/**
 * Type-level: Check if field is indexed (has #)
 */
export type IsIndexed<F extends string> =
  RemoveDefault<F> extends `${string}#${string}` ? true :
  RemoveDefault<F> extends `${string}#` ? true :
  false;

/**
 * Type-level: Check if field is an array type (ends with [])
 */
export type IsArrayType<F extends string> =
  RemoveDefault<F> extends `${string}[]` ? true : false;

/**
 * Type-level: Extract base type from field definition
 */
export type ExtractBaseType<F extends string> =
  // First remove default value
  RemoveDefault<F> extends infer NoDefault extends string ?
    // Handle relations
    NoDefault extends `-> ${infer T}[]` ? T :
    NoDefault extends `->${infer T}[]` ? T :
    NoDefault extends `-> ${infer T}` ? T :
    NoDefault extends `->${infer T}` ? T :
    NoDefault extends `<- ${infer T}[]` ? T :
    NoDefault extends `<-${infer T}[]` ? T :
    NoDefault extends `<- ${infer T}` ? T :
    NoDefault extends `<-${infer T}` ? T :
    // Handle array types
    NoDefault extends `${infer T}[]` ?
      // Remove modifiers from T
      T extends `${infer Base}!#` ? Base :
      T extends `${infer Base}#!` ? Base :
      T extends `${infer Base}!?` ? Base :
      T extends `${infer Base}?!` ? Base :
      T extends `${infer Base}#?` ? Base :
      T extends `${infer Base}?#` ? Base :
      T extends `${infer Base}!` ? Base :
      T extends `${infer Base}#` ? Base :
      T extends `${infer Base}?` ? Base :
      T :
    // Handle non-array types with modifiers
    NoDefault extends `${infer Base}!#?` ? Base :
    NoDefault extends `${infer Base}!?#` ? Base :
    NoDefault extends `${infer Base}#!?` ? Base :
    NoDefault extends `${infer Base}#?!` ? Base :
    NoDefault extends `${infer Base}?!#` ? Base :
    NoDefault extends `${infer Base}?#!` ? Base :
    NoDefault extends `${infer Base}!#` ? Base :
    NoDefault extends `${infer Base}#!` ? Base :
    NoDefault extends `${infer Base}!?` ? Base :
    NoDefault extends `${infer Base}?!` ? Base :
    NoDefault extends `${infer Base}#?` ? Base :
    NoDefault extends `${infer Base}?#` ? Base :
    NoDefault extends `${infer Base}!` ? Base :
    NoDefault extends `${infer Base}#` ? Base :
    NoDefault extends `${infer Base}?` ? Base :
    NoDefault :
  never;

// Helper type for trimming
type Trim<S extends string> =
  S extends ` ${infer R}` ? Trim<R> :
  S extends `${infer R} ` ? Trim<R> :
  S;
