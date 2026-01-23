/**
 * DoSQL IceType-inspired Schema DSL
 *
 * A type-safe schema definition language with compile-time inference.
 *
 * ## Stability
 *
 * This module follows semantic versioning. Exports are marked with stability annotations:
 *
 * - **stable**: No breaking changes in minor versions. Safe for production use.
 * - **experimental**: May change in any version. Use with caution.
 *
 * @packageDocumentation
 * @stability stable
 *
 * @example
 * ```typescript
 * import { defineSchema, type InferTable } from 'dosql/schema';
 *
 * const schema = defineSchema({
 *   users: {
 *     id: 'uuid!',
 *     email: 'string!#',
 *     displayName: 'string',
 *     avatarUrl: 'string?',
 *     createdAt: 'timestamp = now()',
 *     orders: '-> Order[]',
 *   },
 *   orders: {
 *     id: 'uuid!',
 *     userId: '<- users',
 *     totalAmount: 'decimal(10,2)',
 *     lineItems: 'json[]',
 *     status: 'string = "pending"',
 *   },
 * });
 *
 * // TypeScript types automatically inferred
 * type User = InferTable<typeof schema, 'users'>;
 * // { id: string; email: string; displayName: string; avatarUrl?: string | null; ... }
 * ```
 *
 * ## Field Type Notation
 *
 * | Modifier | Meaning | Example |
 * |----------|---------|---------|
 * | `!` | Required/Primary key | `'uuid!'` |
 * | `#` | Indexed | `'string#'`, `'uuid!#'` |
 * | `?` | Optional/Nullable | `'string?'` |
 * | `->` | Forward relation (has-many) | `'-> Order[]'` |
 * | `<-` | Backward relation (belongs-to) | `'<- users'` |
 * | `[]` | Array | `'string[]'`, `'-> Order[]'` |
 * | `= "value"` | Default value | `'string = "pending"'` |
 *
 * ## Supported Base Types
 *
 * - **String**: `string`, `text`, `uuid`
 * - **Number**: `int`, `integer`, `bigint`, `float`, `double`, `decimal(p,s)`, `number`
 * - **Boolean**: `boolean`, `bool`
 * - **Date/Time**: `timestamp`, `datetime`, `date`, `time`
 * - **JSON**: `json`, `jsonb`
 * - **Binary**: `blob`, `binary`
 */

// Import types for local use in function signatures
import type { SchemaDefinition, TableDefinition } from './types.js';

// =============================================================================
// RE-EXPORTS: TYPES
// =============================================================================

/**
 * Core schema types and definitions.
 * @public
 * @stability stable
 */
export type {
  // Core types
  SchemaDefinition,
  TableDefinition,
  FieldType,
  SchemaDirective,

  // SQL types
  SqlBaseType,
  SqlTypeToTs,

  // Parsed types
  ParsedField,
  FieldModifiers,
  RelationInfo,
  RelationType,

  // Validation types
  ValidationResult,
  ValidationError,
  ValidationWarning,

  // Codegen types
  GeneratedInterface,
  GeneratedSql,
  CodegenOutput,

  // Utility types
  Trim,
  StartsWith,
  EndsWith,
  Contains,
  Before,
  After,
} from './types.js';

// =============================================================================
// RE-EXPORTS: PARSER
// =============================================================================

/**
 * Schema parsing utilities.
 * @public
 * @stability stable
 */
export {
  // Runtime parser functions
  parseField,
  parseTable,
  parseSchema,

  // Type checking utilities
  isValidBaseType,
  isRelation,
  isForwardRelation,
  isBackwardRelation,
  getRelationTarget,
  isNullable,
  isPrimaryKey,
  isIndexed,
  isArrayType,
  getDefaultValue,
} from './parser.js';

/**
 * Type-level parser types.
 * @public
 * @stability stable
 */
export type {
  // Type-level parser types
  ExtractDefault,
  RemoveDefault,
  IsForwardRelation as TypeIsForwardRelation,
  IsBackwardRelation as TypeIsBackwardRelation,
  IsRelation as TypeIsRelation,
  ExtractRelationTarget as TypeExtractRelationTarget,
  IsToManyRelation,
  IsNullable as TypeIsNullable,
  IsPrimaryKey as TypeIsPrimaryKey,
  IsIndexed as TypeIsIndexed,
  IsArrayType as TypeIsArrayType,
  ExtractBaseType as TypeExtractBaseType,
} from './parser.js';

// =============================================================================
// RE-EXPORTS: AST PARSER
// =============================================================================

/**
 * AST-based schema parsing utilities using TypeScript Compiler API.
 * @public
 * @stability stable
 */
export {
  // AST-based parsing functions
  parseSchemaSource,
  parseFieldDefinition,
  parseObjectTableDefinition,

  // AST utilities
  getPosition,
  unwrapExpression,
  getJsDocComment,
  evaluateExpression,
  getPropertyName,
} from './ast-parser.js';

/**
 * AST parser types.
 * @public
 * @stability stable
 */
export type {
  SourcePosition,
  ParseError,
  ParseResult,
  ParsedSchemaSource,
  AstParseOptions,
} from './ast-parser.js';

// =============================================================================
// RE-EXPORTS: VALIDATOR
// =============================================================================

/**
 * Schema validation utilities.
 * @public
 * @stability stable
 */
export {
  // Validation functions
  validateSchema,
  isValidSchema,
  assertValidSchema,

  // Error/warning codes
  ErrorCodes,
  WarningCodes,
} from './validator.js';

/**
 * Validation option types.
 * @public
 * @stability stable
 */
export type {
  ErrorCode,
  WarningCode,
  ValidateOptions,
} from './validator.js';

// =============================================================================
// RE-EXPORTS: CODEGEN
// =============================================================================

/**
 * Code generation utilities.
 * @public
 * @stability stable
 */
export {
  // Main codegen functions
  generateCode,
  codegen,
  codegenSql,

  // Individual generators
  generateTypeScript,
  generateSql,
  generateValidationFunctions,
} from './codegen.js';

/**
 * Codegen option types.
 * @public
 * @stability stable
 */
export type {
  CodegenOptions,
} from './codegen.js';

// =============================================================================
// RE-EXPORTS: INFERENCE
// =============================================================================

/**
 * Type inference utilities.
 * @public
 * @stability stable
 */
export type {
  // Field inference
  InferField,
  SqlToTs,
  ExtractBaseType,

  // Modifier detection
  HasNullable,
  HasRequired,
  HasIndexed,
  HasArray,
  IsForwardRelation,
  IsBackwardRelation,
  IsRelation,
  ExtractRelationTarget,

  // Table inference
  InferTableDef,
  InferTable,
  InferSchema,

  // Insert/Update types
  InferInsert,
  InferUpdate,
  InferSelect,

  // Utility types
  PrimaryKeyField,
  IndexedFields,
  RelationFields,
  DataFields,

  // Branded types
  Brand,
  UUID,
  Email,
  Timestamp,

  // Type testing helpers
  Expect,
  Equal,
  Extends,
} from './inference.js';

// =============================================================================
// MAIN API
// =============================================================================

/**
 * Define a type-safe schema with compile-time inference.
 *
 * @public
 * @stability stable
 *
 * This is the main entry point for creating schemas. It validates the schema
 * structure and returns a strongly-typed schema object.
 *
 * @param schema - The schema definition object
 * @returns The validated schema with preserved types
 *
 * @example
 * ```typescript
 * const schema = defineSchema({
 *   users: {
 *     id: 'uuid!',
 *     email: 'string!#',
 *     displayName: 'string',
 *     avatarUrl: 'string?',
 *     createdAt: 'timestamp = now()',
 *     orders: '-> orders[]',
 *   },
 *   orders: {
 *     id: 'uuid!',
 *     userId: '<- users',
 *     totalAmount: 'decimal(10,2)',
 *     status: 'string = "pending"',
 *   },
 * } as const);
 *
 * // Access inferred types
 * type User = InferTable<typeof schema, 'users'>;
 * type Order = InferTable<typeof schema, 'orders'>;
 * ```
 */
export function defineSchema<S extends SchemaDefinition>(schema: S): S {
  // Runtime validation (optional - can be disabled in production)
  if (process.env.NODE_ENV !== 'production') {
    const { validateSchema } = require('./validator.js');
    const result = validateSchema(schema);

    if (!result.valid) {
      const errorMessages = result.errors
        .map((e: { code: string; path: string[]; message: string }) =>
          `[${e.code}] ${e.path.join('.')}: ${e.message}`
        )
        .join('\n');
      throw new Error(`Schema validation failed:\n${errorMessages}`);
    }

    // Log warnings in development
    if (result.warnings.length > 0) {
      console.warn(
        'Schema warnings:',
        result.warnings.map((w: { path: string[]; message: string }) =>
          `${w.path.join('.')}: ${w.message}`
        )
      );
    }
  }

  return schema;
}

/**
 * Define a schema without runtime validation.
 *
 * Use this for better performance when you trust the schema is valid
 * or when you've already validated it separately.
 *
 * @param schema - The schema definition object
 * @returns The schema with preserved types
 *
 * @public
 * @stability stable
 */
export function unsafeDefineSchema<S extends SchemaDefinition>(schema: S): S {
  return schema;
}

/**
 * Create a table definition helper (for use outside defineSchema).
 *
 * @public
 * @stability stable
 *
 * @example
 * ```typescript
 * const usersTable = table({
 *   id: 'uuid!',
 *   email: 'string!#',
 *   name: 'string',
 * });
 * ```
 */
export function table<T extends TableDefinition>(definition: T): T {
  return definition;
}

// =============================================================================
// SCHEMA UTILITIES
// =============================================================================

import type { SchemaDefinition as SD, TableDefinition as TD } from './types.js';
import { parseSchema as ps } from './parser.js';
import { generateTypeScript as gts, generateSql as gsql } from './codegen.js';
import { validateSchema as vs } from './validator.js';

/**
 * Get all table names from a schema.
 * @public
 * @stability stable
 */
export function getTableNames<S extends SD>(schema: S): (keyof S)[] {
  return Object.keys(schema).filter((k) => !k.startsWith('@')) as (keyof S)[];
}

/**
 * Get a specific table definition from a schema.
 * @public
 * @stability stable
 */
export function getTable<S extends SD, T extends keyof S>(
  schema: S,
  tableName: T
): S[T] {
  return schema[tableName];
}

/**
 * Get field names from a table definition.
 * @public
 * @stability stable
 */
export function getFieldNames<T extends TD>(table: T): (keyof T)[] {
  return Object.keys(table) as (keyof T)[];
}

/**
 * Generate TypeScript code from a schema.
 * @public
 * @stability stable
 */
export function toTypeScript(schema: SD): string {
  return gts(schema).fullCode;
}

/**
 * Generate SQL from a schema.
 * @public
 * @stability stable
 */
export function toSql(schema: SD): string {
  return gsql(schema).fullScript;
}

/**
 * Validate a schema and return the result.
 * @public
 * @stability stable
 */
export function validate(schema: SD) {
  return vs(schema);
}

/**
 * Parse a schema into structured format.
 * @public
 * @stability stable
 */
export function parse(schema: SD) {
  return ps(schema);
}

// =============================================================================
// SCHEMA BUILDER (FLUENT API)
// =============================================================================

/**
 * Schema builder for fluent API construction.
 * @public
 * @stability stable
 */
export class SchemaBuilder<S extends SD = {}> {
  private schema: S;

  constructor(schema: S = {} as S) {
    this.schema = schema;
  }

  /**
   * Add a table to the schema
   */
  addTable<N extends string, T extends TD>(
    name: N,
    definition: T
  ): SchemaBuilder<S & { [K in N]: T }> {
    return new SchemaBuilder({
      ...this.schema,
      [name]: definition,
    } as S & { [K in N]: T });
  }

  /**
   * Build and validate the schema
   */
  build(): S {
    return defineSchema(this.schema as SD) as S;
  }

  /**
   * Build without validation
   */
  buildUnsafe(): S {
    return this.schema;
  }
}

/**
 * Create a new schema builder.
 * @public
 * @stability stable
 */
export function createSchemaBuilder(): SchemaBuilder<{}> {
  return new SchemaBuilder();
}

// =============================================================================
// TYPE HELPERS FOR CONSUMERS
// =============================================================================

/**
 * Helper type to get the inferred schema type from a defineSchema call.
 *
 * @public
 * @stability stable
 *
 * @example
 * ```typescript
 * const schema = defineSchema({ ... });
 * type Schema = SchemaOf<typeof schema>;
 * ```
 */
export type SchemaOf<S extends SD> = {
  [K in keyof S]: S[K] extends TD ? import('./inference.js').InferTableDef<S[K], S> : never;
};

/**
 * Helper type to extract a single table type.
 *
 * @public
 * @stability stable
 *
 * @example
 * ```typescript
 * type User = TableOf<typeof schema, 'users'>;
 * ```
 */
export type TableOf<S extends SD, K extends keyof S> =
  S[K] extends TD ? import('./inference.js').InferTableDef<S[K], S> : never;
