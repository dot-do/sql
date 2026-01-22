/**
 * DoSQL Schema Type Inference
 *
 * Compile-time type inference for the IceType-inspired schema DSL.
 * This module provides type-level utilities to infer TypeScript types
 * from schema definitions.
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
 *     orders: '-> Order[]',
 *   },
 * });
 *
 * // TypeScript type automatically inferred
 * type User = InferTable<typeof schema, 'users'>;
 * // { id: string; email: string; displayName: string; avatarUrl?: string; ... }
 * ```
 */

import type {
  SchemaDefinition,
  TableDefinition,
  Trim,
} from './types.js';

// =============================================================================
// TYPE-LEVEL STRING UTILITIES
// =============================================================================

/**
 * Remove default value from field definition
 * 'string = "pending"' -> 'string'
 */
type RemoveDefault<F extends string> =
  F extends `${infer Base} = "${string}"` ? TrimInternal<Base> :
  F extends `${infer Base} = ${string}` ? TrimInternal<Base> :
  F extends `${infer Base}= "${string}"` ? TrimInternal<Base> :
  F extends `${infer Base}=${string}` ? TrimInternal<Base> :
  F;

/**
 * Internal trim helper
 */
type TrimInternal<S extends string> =
  S extends ` ${infer R}` ? TrimInternal<R> :
  S extends `${infer R} ` ? TrimInternal<R> :
  S;

// =============================================================================
// MODIFIER EXTRACTION
// =============================================================================

/**
 * Check if field has ? modifier (nullable)
 */
export type HasNullable<F extends string> =
  RemoveDefault<F> extends `${string}?${string}` ? true :
  RemoveDefault<F> extends `${string}?` ? true :
  false;

/**
 * Check if field has ! modifier (required/primary key)
 */
export type HasRequired<F extends string> =
  RemoveDefault<F> extends `${string}!${string}` ? true :
  RemoveDefault<F> extends `${string}!` ? true :
  false;

/**
 * Check if field has # modifier (indexed)
 */
export type HasIndexed<F extends string> =
  RemoveDefault<F> extends `${string}#${string}` ? true :
  RemoveDefault<F> extends `${string}#` ? true :
  false;

/**
 * Check if field is an array type
 */
export type HasArray<F extends string> =
  RemoveDefault<F> extends `${string}[]` ? true : false;

// =============================================================================
// RELATION DETECTION
// =============================================================================

/**
 * Check if field is a forward relation (->)
 */
export type IsForwardRelation<F extends string> =
  RemoveDefault<F> extends `-> ${string}` ? true :
  RemoveDefault<F> extends `->${string}` ? true :
  false;

/**
 * Check if field is a backward relation (<-)
 */
export type IsBackwardRelation<F extends string> =
  RemoveDefault<F> extends `<- ${string}` ? true :
  RemoveDefault<F> extends `<-${string}` ? true :
  false;

/**
 * Check if field is any relation
 */
export type IsRelation<F extends string> =
  IsForwardRelation<F> extends true ? true :
  IsBackwardRelation<F> extends true ? true :
  false;

/**
 * Extract relation target table name
 */
export type ExtractRelationTarget<F extends string> =
  RemoveDefault<F> extends `-> ${infer T}[]` ? TrimInternal<T> :
  RemoveDefault<F> extends `->${infer T}[]` ? TrimInternal<T> :
  RemoveDefault<F> extends `-> ${infer T}` ? TrimInternal<T> :
  RemoveDefault<F> extends `->${infer T}` ? TrimInternal<T> :
  RemoveDefault<F> extends `<- ${infer T}[]` ? TrimInternal<T> :
  RemoveDefault<F> extends `<-${infer T}[]` ? TrimInternal<T> :
  RemoveDefault<F> extends `<- ${infer T}` ? TrimInternal<T> :
  RemoveDefault<F> extends `<-${infer T}` ? TrimInternal<T> :
  never;

// =============================================================================
// BASE TYPE EXTRACTION
// =============================================================================

/**
 * Remove all modifiers from type string
 */
type RemoveModifiers<T extends string> =
  T extends `${infer Base}!#?` ? Base :
  T extends `${infer Base}!?#` ? Base :
  T extends `${infer Base}#!?` ? Base :
  T extends `${infer Base}#?!` ? Base :
  T extends `${infer Base}?!#` ? Base :
  T extends `${infer Base}?#!` ? Base :
  T extends `${infer Base}!#` ? Base :
  T extends `${infer Base}#!` ? Base :
  T extends `${infer Base}!?` ? Base :
  T extends `${infer Base}?!` ? Base :
  T extends `${infer Base}#?` ? Base :
  T extends `${infer Base}?#` ? Base :
  T extends `${infer Base}!` ? Base :
  T extends `${infer Base}#` ? Base :
  T extends `${infer Base}?` ? Base :
  T;

/**
 * Extract base type from field definition
 */
export type ExtractBaseType<F extends string> =
  RemoveDefault<F> extends infer NoDefault extends string ?
    // Handle forward relations
    NoDefault extends `-> ${infer T}[]` ? T :
    NoDefault extends `->${infer T}[]` ? T :
    NoDefault extends `-> ${infer T}` ? T :
    NoDefault extends `->${infer T}` ? T :
    // Handle backward relations
    NoDefault extends `<- ${infer T}[]` ? T :
    NoDefault extends `<-${infer T}[]` ? T :
    NoDefault extends `<- ${infer T}` ? T :
    NoDefault extends `<-${infer T}` ? T :
    // Handle array types
    NoDefault extends `${infer T}[]` ? RemoveModifiers<T> :
    // Handle non-array types
    RemoveModifiers<NoDefault> :
  never;

// =============================================================================
// SQL TYPE TO TYPESCRIPT TYPE MAPPING
// =============================================================================

/**
 * Map SQL base type to TypeScript type
 */
export type SqlToTs<T extends string> =
  // String types
  Lowercase<T> extends 'string' ? string :
  Lowercase<T> extends 'text' ? string :
  Lowercase<T> extends 'uuid' ? string :
  Lowercase<T> extends 'time' ? string :
  // Numeric types
  Lowercase<T> extends 'int' ? number :
  Lowercase<T> extends 'integer' ? number :
  Lowercase<T> extends 'bigint' ? number :
  Lowercase<T> extends 'float' ? number :
  Lowercase<T> extends 'double' ? number :
  Lowercase<T> extends 'number' ? number :
  // Decimal with precision
  Lowercase<T> extends `decimal${string}` ? number :
  // Boolean
  Lowercase<T> extends 'boolean' ? boolean :
  Lowercase<T> extends 'bool' ? boolean :
  // Date/Time
  Lowercase<T> extends 'timestamp' ? Date :
  Lowercase<T> extends 'datetime' ? Date :
  Lowercase<T> extends 'date' ? Date :
  // JSON
  Lowercase<T> extends 'json' ? unknown :
  Lowercase<T> extends 'jsonb' ? unknown :
  // Binary
  Lowercase<T> extends 'blob' ? Uint8Array :
  Lowercase<T> extends 'binary' ? Uint8Array :
  // Unknown type
  unknown;

// =============================================================================
// FIELD TYPE INFERENCE
// =============================================================================

/**
 * Infer TypeScript type for a single field definition
 *
 * @example
 * InferField<'string'> = string
 * InferField<'int!'> = number
 * InferField<'uuid!#'> = string
 * InferField<'timestamp?'> = Date | null
 * InferField<'json[]'> = unknown[]
 * InferField<'string = "pending"'> = string
 */
export type InferField<F extends string, Schema = never> =
  // Handle relations with schema lookup
  IsForwardRelation<F> extends true ?
    HasArray<F> extends true ?
      [Schema] extends [never] ? unknown[] :
      ExtractRelationTarget<F> extends keyof Schema ?
        InferTable<Schema, ExtractRelationTarget<F>>[] :
        unknown[] :
    [Schema] extends [never] ? unknown :
    ExtractRelationTarget<F> extends keyof Schema ?
      InferTable<Schema, ExtractRelationTarget<F>> :
      unknown :

  IsBackwardRelation<F> extends true ?
    // Backward relations reference the primary key (usually string for UUID)
    HasArray<F> extends true ? string[] : string :

  // Handle array types
  HasArray<F> extends true ?
    SqlToTs<ExtractBaseType<F>>[] :

  // Handle nullable types
  HasNullable<F> extends true ?
    SqlToTs<ExtractBaseType<F>> | null :

  // Handle required types (same as base)
  SqlToTs<ExtractBaseType<F>>;

// =============================================================================
// TABLE TYPE INFERENCE
// =============================================================================

/**
 * Get required field keys from table definition
 */
type RequiredKeys<T extends TableDefinition> = {
  [K in keyof T]: HasNullable<T[K] & string> extends true ? never : K;
}[keyof T];

/**
 * Get optional field keys from table definition
 */
type OptionalKeys<T extends TableDefinition> = {
  [K in keyof T]: HasNullable<T[K] & string> extends true ? K : never;
}[keyof T];

/**
 * Infer TypeScript interface for a table definition
 *
 * @example
 * type UserTable = {
 *   id: 'uuid!';
 *   email: 'string!#';
 *   name: 'string';
 *   bio: 'text?';
 * };
 *
 * type User = InferTableDef<UserTable>;
 * // { id: string; email: string; name: string; bio?: string | null; }
 */
export type InferTableDef<T extends TableDefinition, Schema = never> = {
  // Required fields
  [K in RequiredKeys<T> & string]: InferField<T[K] & string, Schema>;
} & {
  // Optional fields (nullable)
  [K in OptionalKeys<T> & string]?: InferField<T[K] & string, Schema>;
};

/**
 * Infer TypeScript interface for a table by name from schema
 *
 * @example
 * const schema = {
 *   users: { id: 'uuid!', name: 'string' },
 *   orders: { id: 'uuid!', userId: '<- users' },
 * } as const;
 *
 * type User = InferTable<typeof schema, 'users'>;
 * // { id: string; name: string; }
 */
export type InferTable<S, T extends keyof S> =
  S[T] extends TableDefinition ?
    InferTableDef<S[T], S> :
  never;

// =============================================================================
// SCHEMA TYPE INFERENCE
// =============================================================================

/**
 * Filter out schema directives (keys starting with @)
 */
type TableKeys<S> = {
  [K in keyof S]: K extends `@${string}` ? never : K;
}[keyof S];

/**
 * Infer all tables from a schema as a union type
 *
 * @example
 * const schema = {
 *   users: { id: 'uuid!', name: 'string' },
 *   orders: { id: 'uuid!', total: 'decimal(10,2)' },
 * } as const;
 *
 * type AllEntities = InferSchema<typeof schema>;
 * // {
 * //   users: { id: string; name: string; };
 * //   orders: { id: string; total: number; };
 * // }
 */
export type InferSchema<S extends SchemaDefinition> = {
  [K in TableKeys<S> & string]: S[K] extends TableDefinition ?
    InferTableDef<S[K], S> :
    never;
};

// =============================================================================
// INSERT/UPDATE TYPE HELPERS
// =============================================================================

/**
 * Get fields that are auto-generated (have defaults)
 */
type AutoGeneratedKeys<T extends TableDefinition> = {
  [K in keyof T]: T[K] extends `${string}= ${string}` ? K :
                  T[K] extends `${string} = ${string}` ? K : never;
}[keyof T];

/**
 * Get fields that are relations (not real columns)
 */
type RelationKeys<T extends TableDefinition> = {
  [K in keyof T]: IsRelation<T[K] & string> extends true ? K : never;
}[keyof T];

/**
 * Infer the insert type for a table (excludes auto-generated and relation fields)
 */
export type InferInsert<T extends TableDefinition, Schema = never> = {
  // Required fields (not nullable, not auto-generated, not relations)
  [K in Exclude<RequiredKeys<T>, AutoGeneratedKeys<T> | RelationKeys<T>> & string]:
    InferField<T[K] & string, Schema>;
} & {
  // Optional fields (nullable or auto-generated, but not relations)
  [K in Exclude<OptionalKeys<T> | AutoGeneratedKeys<T>, RelationKeys<T>> & string]?:
    InferField<T[K] & string, Schema> | undefined;
};

/**
 * Infer the update type for a table (all fields optional except primary key)
 */
export type InferUpdate<T extends TableDefinition, Schema = never> = Partial<{
  [K in Exclude<keyof T, RelationKeys<T>> & string]: InferField<T[K] & string, Schema>;
}>;

/**
 * Infer the select type for a table (includes all fields including relations)
 */
export type InferSelect<T extends TableDefinition, Schema = never> = InferTableDef<T, Schema>;

// =============================================================================
// UTILITY TYPES
// =============================================================================

/**
 * Extract primary key field name from table definition
 */
export type PrimaryKeyField<T extends TableDefinition> = {
  [K in keyof T]: HasRequired<T[K] & string> extends true ? K : never;
}[keyof T];

/**
 * Extract indexed field names from table definition
 */
export type IndexedFields<T extends TableDefinition> = {
  [K in keyof T]: HasIndexed<T[K] & string> extends true ? K : never;
}[keyof T];

/**
 * Extract relation field names from table definition
 */
export type RelationFields<T extends TableDefinition> = {
  [K in keyof T]: IsRelation<T[K] & string> extends true ? K : never;
}[keyof T];

/**
 * Extract non-relation field names from table definition
 */
export type DataFields<T extends TableDefinition> = {
  [K in keyof T]: IsRelation<T[K] & string> extends true ? never : K;
}[keyof T];

// =============================================================================
// BRANDED TYPES FOR BETTER TYPE SAFETY
// =============================================================================

/**
 * Brand a type with a unique identifier
 */
declare const brand: unique symbol;

export type Brand<T, B extends string> = T & { readonly [brand]: B };

/**
 * UUID type (branded string)
 */
export type UUID = Brand<string, 'UUID'>;

/**
 * Email type (branded string)
 */
export type Email = Brand<string, 'Email'>;

/**
 * Timestamp type (branded Date)
 */
export type Timestamp = Brand<Date, 'Timestamp'>;

// =============================================================================
// TYPE ASSERTION HELPERS
// =============================================================================

/**
 * Helper for testing type equality
 */
export type Expect<T extends true> = T;

/**
 * Check if two types are equal
 */
export type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2)
    ? true
    : false;

/**
 * Check if type A extends type B
 */
export type Extends<A, B> = A extends B ? true : false;
