/**
 * DoSQL Schema DSL Parser
 *
 * Parses field definitions in the IceType-inspired DSL format.
 * Uses a state machine-based approach instead of regex for more accurate parsing
 * and better error messages.
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
// VALID BASE TYPES
// =============================================================================

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
// FIELD PARSER (State Machine Based)
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
 * Parse a field type definition string using a state machine approach
 * This provides more accurate parsing than regex and better error messages.
 *
 * @param fieldDef - The field definition string (e.g., 'uuid!#', '-> Order[]')
 * @returns ParsedField object with all extracted information
 */
export function parseField(fieldDef: FieldType): ParsedField {
  const modifiers: FieldModifiers = {
    required: true,
    primaryKey: false,
    indexed: false,
    nullable: false,
    isArray: false,
  };
  let relation: RelationInfo | undefined;
  let baseType = '';

  const input = fieldDef.trim();
  let index = 0;

  // Helper functions for parsing
  const skipWhitespace = () => {
    while (index < input.length && /\s/.test(input[index]!)) {
      index++;
    }
  };

  const peek = (offset = 0): string | undefined => input[index + offset];
  const consume = (): string => input[index++]!;
  const atEnd = () => index >= input.length;

  const isIdentifierChar = (ch: string | undefined): boolean =>
    ch !== undefined && /[a-zA-Z0-9_]/.test(ch);

  skipWhitespace();

  // Check for forward relation (->)
  if (peek() === '-' && peek(1) === '>') {
    index += 2;
    skipWhitespace();

    // Parse target table name
    let target = '';
    while (!atEnd() && isIdentifierChar(peek())) {
      target += consume();
    }
    skipWhitespace();

    // Check for array notation
    let isMany = false;
    if (peek() === '[' && peek(1) === ']') {
      index += 2;
      isMany = true;
      modifiers.isArray = true;
    }

    return {
      raw: fieldDef,
      baseType: target,
      modifiers,
      relation: {
        type: 'forward',
        target,
        isMany,
      },
    };
  }

  // Check for backward relation (<-)
  if (peek() === '<' && peek(1) === '-') {
    index += 2;
    skipWhitespace();

    // Parse target table name
    let target = '';
    while (!atEnd() && isIdentifierChar(peek())) {
      target += consume();
    }
    skipWhitespace();

    // Check for array notation
    let isMany = false;
    if (peek() === '[' && peek(1) === ']') {
      index += 2;
      isMany = true;
      modifiers.isArray = true;
    }

    return {
      raw: fieldDef,
      baseType: target,
      modifiers,
      relation: {
        type: 'backward',
        target,
        isMany,
      },
    };
  }

  // Handle decimal with precision: decimal(10,2)
  if (input.slice(index).toLowerCase().startsWith('decimal(')) {
    let depth = 0;
    while (!atEnd()) {
      const ch = consume();
      baseType += ch;
      if (ch === '(') depth++;
      if (ch === ')') {
        depth--;
        if (depth === 0) break;
      }
    }
  } else {
    // Regular type name
    while (!atEnd() && isIdentifierChar(peek())) {
      baseType += consume();
    }
  }

  // Parse modifiers, array notation, and default values
  while (!atEnd()) {
    skipWhitespace();
    const ch = peek();

    if (ch === '!') {
      consume();
      modifiers.primaryKey = true;
      modifiers.required = true;
      modifiers.nullable = false;
    } else if (ch === '#') {
      consume();
      modifiers.indexed = true;
    } else if (ch === '?') {
      consume();
      modifiers.nullable = true;
      modifiers.required = false;
    } else if (ch === '[' && peek(1) === ']') {
      index += 2;
      modifiers.isArray = true;
    } else if (ch === '=') {
      // Default value
      consume();
      skipWhitespace();

      let defaultValue = '';
      if (peek() === '"') {
        // Quoted string default
        consume(); // Opening quote
        while (!atEnd() && peek() !== '"') {
          if (peek() === '\\' && peek(1) === '"') {
            consume(); // Skip escape character
            defaultValue += consume();
          } else {
            defaultValue += consume();
          }
        }
        if (peek() === '"') consume(); // Closing quote
      } else if (peek() === "'") {
        // Single-quoted string default
        consume(); // Opening quote
        while (!atEnd() && peek() !== "'") {
          if (peek() === '\\' && peek(1) === "'") {
            consume(); // Skip escape character
            defaultValue += consume();
          } else {
            defaultValue += consume();
          }
        }
        if (peek() === "'") consume(); // Closing quote
      } else {
        // Unquoted value (function call like now(), or identifier, or number)
        while (!atEnd() && peek() !== undefined && /[a-zA-Z0-9_().]/.test(peek()!)) {
          defaultValue += consume();
        }
      }
      modifiers.defaultValue = defaultValue;
    } else {
      // Unknown character, stop parsing
      break;
    }
  }

  return {
    raw: fieldDef,
    baseType: baseType.toLowerCase(),
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
  const parsed = parseField(fieldDef);
  return parsed.relation !== undefined;
}

/**
 * Check if a field definition is a forward relation (->)
 */
export function isForwardRelation(fieldDef: FieldType): boolean {
  const parsed = parseField(fieldDef);
  return parsed.relation?.type === 'forward';
}

/**
 * Check if a field definition is a backward relation (<-)
 */
export function isBackwardRelation(fieldDef: FieldType): boolean {
  const parsed = parseField(fieldDef);
  return parsed.relation?.type === 'backward';
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
