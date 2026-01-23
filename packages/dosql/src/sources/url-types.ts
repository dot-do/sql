/**
 * Type-Level URL Source Parsing
 *
 * Provides compile-time parsing and type inference for URL table sources:
 * - SELECT * FROM 'https://example.com/data.csv'
 * - SELECT * FROM 'r2://bucket/path.parquet'
 * - SELECT * FROM url('https://api.example.com/data', 'JSON')
 *
 * These types integrate with the main DoSQL parser to provide type-safe
 * URL table source queries.
 */

import type { ColumnType } from '../parser.js';
import type { Format } from './types.js';

// =============================================================================
// UTILITY TYPES
// =============================================================================

type Whitespace = ' ' | '\n' | '\t' | '\r';

type Trim<S extends string> =
  S extends `${Whitespace}${infer Rest}` ? Trim<Rest> :
  S extends `${infer Rest}${Whitespace}` ? Trim<Rest> :
  S;

// =============================================================================
// URL SCHEME PARSING
// =============================================================================

/**
 * Supported URI schemes
 */
export type SupportedScheme = 'https' | 'http' | 'r2' | 'kv' | 'd1';

/**
 * Parse URL scheme from a string literal at compile time
 */
export type ParseUrlScheme<S extends string> =
  S extends `https://${string}` ? 'https' :
  S extends `http://${string}` ? 'http' :
  S extends `r2://${string}` ? 'r2' :
  S extends `kv://${string}` ? 'kv' :
  S extends `d1://${string}` ? 'd1' :
  never;

/**
 * Check if URL scheme is supported
 */
export type IsSupportedScheme<S extends SupportedScheme | never> =
  S extends SupportedScheme ? true : false;

// =============================================================================
// URL PATH AND HOST PARSING
// =============================================================================

/**
 * Extract host from URL at compile time
 * For r2://bucket/path, host is "bucket"
 * For https://example.com/path, host is "example.com"
 */
export type ParseUrlHost<S extends string> =
  S extends `${string}://${infer Host}/${string}` ? ExtractHostPart<Host> :
  S extends `${string}://${infer Host}` ? ExtractHostPart<Host> :
  never;

type ExtractHostPart<S extends string> =
  S extends `${infer Host}:${string}` ? Host : S;

/**
 * Extract path from URL at compile time
 */
export type ParseUrlPath<S extends string> =
  S extends `${string}://${string}/${infer Path}` ? `/${Path}` :
  S extends `${string}://${string}` ? '/' :
  never;

/**
 * Extract port from URL at compile time (if present)
 */
export type ParseUrlPort<S extends string> =
  S extends `${string}://${string}:${infer Port}/${string}` ? Port :
  S extends `${string}://${string}:${infer Port}` ? Port :
  never;

// =============================================================================
// FORMAT INFERENCE FROM URL
// =============================================================================

/**
 * Extract file extension from URL at compile time
 */
export type ParseUrlExtension<S extends string> =
  // Handle query strings: file.json?query=value -> json
  S extends `${string}.json?${string}` ? 'json' :
  S extends `${string}.ndjson?${string}` ? 'ndjson' :
  S extends `${string}.jsonl?${string}` ? 'ndjson' :
  S extends `${string}.csv?${string}` ? 'csv' :
  S extends `${string}.parquet?${string}` ? 'parquet' :
  // Handle fragments: file.json#section -> json
  S extends `${string}.json#${string}` ? 'json' :
  S extends `${string}.ndjson#${string}` ? 'ndjson' :
  S extends `${string}.jsonl#${string}` ? 'ndjson' :
  S extends `${string}.csv#${string}` ? 'csv' :
  S extends `${string}.parquet#${string}` ? 'parquet' :
  // Plain extensions
  S extends `${string}.json` ? 'json' :
  S extends `${string}.ndjson` ? 'ndjson' :
  S extends `${string}.jsonl` ? 'ndjson' :
  S extends `${string}.csv` ? 'csv' :
  S extends `${string}.parquet` ? 'parquet' :
  // Uppercase variants
  S extends `${string}.JSON` ? 'json' :
  S extends `${string}.NDJSON` ? 'ndjson' :
  S extends `${string}.JSONL` ? 'ndjson' :
  S extends `${string}.CSV` ? 'csv' :
  S extends `${string}.PARQUET` ? 'parquet' :
  never;

/**
 * Infer format from URL extension at compile time
 * Returns 'json' as default if no extension is detected
 */
export type InferFormatFromUrl<S extends string> =
  ParseUrlExtension<S> extends infer F extends Format ? F : 'json';

// =============================================================================
// URL LITERAL DETECTION
// =============================================================================

/**
 * Check if string is a quoted URL literal
 */
export type IsUrlLiteral<S extends string> =
  S extends `'${infer Url}'` ? ParseUrlScheme<Url> extends never ? false : true :
  S extends `"${infer Url}"` ? ParseUrlScheme<Url> extends never ? false : true :
  false;

/**
 * Extract URL from quoted literal
 */
export type ExtractUrlFromLiteral<S extends string> =
  S extends `'${infer Url}'` ? Url :
  S extends `"${infer Url}"` ? Url :
  never;

// =============================================================================
// URL FUNCTION PARSING
// =============================================================================

/**
 * Check if string is a url() function call
 */
export type IsUrlFunction<S extends string> =
  Uppercase<Trim<S>> extends `URL(${string})` ? true : false;

/**
 * Parse url() function call at compile time
 * Format: url('https://...', 'FORMAT') or url('https://...')
 */
export type ParseUrlFunction<S extends string> =
  IsUrlFunction<S> extends true ? ParseUrlFunctionArgs<Trim<S>> : never;

type ParseUrlFunctionArgs<S extends string> =
  S extends `${infer _}(${infer Args})` ? ParseArgs<Trim<Args>> :
  S extends `${infer _}(${infer Args})` ? ParseArgs<Trim<Args>> :
  never;

type ParseArgs<S extends string> =
  // Two arguments with single quotes
  S extends `'${infer Url}',${infer Rest}` ? ParseArgsWithFormat<Url, Trim<Rest>> :
  // Two arguments with double quotes
  S extends `"${infer Url}",${infer Rest}` ? ParseArgsWithFormat<Url, Trim<Rest>> :
  // Single argument with single quotes
  S extends `'${infer Url}'` ? { url: Url; format: InferFormatFromUrl<Url> } :
  // Single argument with double quotes
  S extends `"${infer Url}"` ? { url: Url; format: InferFormatFromUrl<Url> } :
  never;

type ParseArgsWithFormat<Url extends string, Rest extends string> =
  Rest extends `'${infer Fmt}'` ? { url: Url; format: ParseFormatString<Fmt> } :
  Rest extends `"${infer Fmt}"` ? { url: Url; format: ParseFormatString<Fmt> } :
  { url: Url; format: InferFormatFromUrl<Url> };

type ParseFormatString<S extends string> =
  Lowercase<S> extends 'json' ? 'json' :
  Lowercase<S> extends 'ndjson' ? 'ndjson' :
  Lowercase<S> extends 'jsonl' ? 'ndjson' :
  Lowercase<S> extends 'csv' ? 'csv' :
  Lowercase<S> extends 'parquet' ? 'parquet' :
  'json';

// =============================================================================
// URL SOURCE PARSING RESULT
// =============================================================================

/**
 * Parsed URL source information
 */
export interface ParsedUrlSource<
  Url extends string,
  Fmt extends Format,
  Scheme extends SupportedScheme
> {
  url: Url;
  format: Fmt;
  scheme: Scheme;
  host: ParseUrlHost<Url>;
  path: ParseUrlPath<Url>;
}

/**
 * Parse URL source from FROM clause value
 */
export type ParseUrlSource<S extends string> =
  // Check for url() function
  IsUrlFunction<S> extends true
    ? ParseUrlFunction<S> extends { url: infer U extends string; format: infer F extends Format }
      ? ParseUrlScheme<U> extends infer Scheme extends SupportedScheme
        ? ParsedUrlSource<U, F, Scheme>
        : { error: `Unsupported URL scheme in: ${U}` }
      : { error: `Failed to parse url() function: ${S}` }
  // Check for quoted URL literal
  : IsUrlLiteral<S> extends true
    ? ExtractUrlFromLiteral<S> extends infer U extends string
      ? ParseUrlScheme<U> extends infer Scheme extends SupportedScheme
        ? ParsedUrlSource<U, InferFormatFromUrl<U>, Scheme>
        : { error: `Unsupported URL scheme in: ${U}` }
      : { error: `Failed to extract URL from: ${S}` }
  // Not a URL source
  : never;

// =============================================================================
// URL SOURCE SCHEMA TYPES
// =============================================================================

/**
 * Dynamic schema for URL sources
 * Since URL sources don't have compile-time schemas, we use a flexible type
 */
export type UrlSourceSchema = Record<string, ColumnType>;

/**
 * Type-safe schema definition for known URL sources
 */
export type DefineUrlSchema<T extends Record<string, ColumnType>> = T;

/**
 * URL source with known schema
 */
export interface TypedUrlSource<
  Url extends string,
  Schema extends Record<string, ColumnType>
> {
  url: Url;
  schema: Schema;
}

// =============================================================================
// QUERY RESULT TYPES FOR URL SOURCES
// =============================================================================

/**
 * Convert schema to TypeScript type
 */
export type SchemaToType<S extends Record<string, ColumnType>> = {
  [K in keyof S]: S[K] extends 'string' ? string :
    S[K] extends 'number' ? number :
    S[K] extends 'boolean' ? boolean :
    S[K] extends 'Date' ? Date :
    S[K] extends 'null' ? null :
    unknown
};

/**
 * Result type for URL source query with known schema
 */
export type UrlQueryResult<
  Schema extends Record<string, ColumnType>,
  Columns extends keyof Schema | '*' = '*'
> = Columns extends '*'
  ? SchemaToType<Schema>[]
  : { [K in Extract<Columns, keyof Schema>]: SchemaToType<Schema>[K] }[];

// =============================================================================
// SQL INTEGRATION TYPES
// =============================================================================

/**
 * Check if FROM clause value is a URL source
 */
export type IsUrlSourceFrom<S extends string> =
  IsUrlLiteral<S> extends true ? true :
  IsUrlFunction<S> extends true ? true :
  false;

/**
 * Extract URL from FROM clause
 */
export type ExtractUrlFromFrom<S extends string> =
  IsUrlFunction<S> extends true
    ? ParseUrlFunction<S> extends { url: infer U } ? U : never
  : IsUrlLiteral<S> extends true
    ? ExtractUrlFromLiteral<S>
  : never;

// =============================================================================
// URL SOURCE REGISTRY (for schema lookups)
// =============================================================================

/**
 * Registry of known URL sources and their schemas
 * Users can extend this interface to add compile-time schema support
 *
 * @example
 * declare module 'dosql' {
 *   interface UrlSourceRegistry {
 *     'https://api.example.com/users.json': {
 *       id: 'number';
 *       name: 'string';
 *       email: 'string';
 *     };
 *   }
 * }
 */
export interface UrlSourceRegistry {
  // Users can extend this interface
}

/**
 * Get schema from registry if available
 */
export type GetRegisteredSchema<Url extends string> =
  Url extends keyof UrlSourceRegistry
    ? UrlSourceRegistry[Url]
    : Record<string, 'unknown'>;

/**
 * Check if URL has registered schema
 */
export type HasRegisteredSchema<Url extends string> =
  Url extends keyof UrlSourceRegistry ? true : false;
