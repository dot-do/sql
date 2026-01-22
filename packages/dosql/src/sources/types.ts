/**
 * Core types for URL Table Sources
 *
 * Provides ClickHouse-style URL table source support:
 * - SELECT * FROM 'https://example.com/data.csv'
 * - SELECT * FROM 'r2://bucket/path.parquet'
 * - SELECT * FROM url('https://api.example.com/data', 'JSON')
 */

import type { ColumnType } from '../parser.js';

// =============================================================================
// FORMAT TYPES
// =============================================================================

/**
 * Supported data formats for URL table sources
 */
export type Format = 'json' | 'ndjson' | 'csv' | 'parquet';

/**
 * Format detection result
 */
export interface FormatDetectionResult {
  format: Format;
  confidence: 'high' | 'medium' | 'low';
  source: 'extension' | 'content-type' | 'content' | 'default';
}

// =============================================================================
// EXPRESSION TYPES
// =============================================================================

/**
 * SQL expression for filtering (simplified)
 */
export interface Expression {
  type: 'comparison' | 'and' | 'or' | 'not' | 'literal' | 'column';
}

export interface ComparisonExpression extends Expression {
  type: 'comparison';
  operator: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'LIKE' | 'IN';
  left: Expression;
  right: Expression;
}

export interface AndExpression extends Expression {
  type: 'and';
  conditions: Expression[];
}

export interface OrExpression extends Expression {
  type: 'or';
  conditions: Expression[];
}

export interface NotExpression extends Expression {
  type: 'not';
  expression: Expression;
}

export interface LiteralExpression extends Expression {
  type: 'literal';
  value: string | number | boolean | null;
}

export interface ColumnExpression extends Expression {
  type: 'column';
  name: string;
  table?: string;
}

// =============================================================================
// SCAN OPTIONS
// =============================================================================

/**
 * Options for scanning a table source
 */
export interface ScanOptions {
  /** Columns to read (projection pushdown) */
  projection?: string[];

  /** Filter predicate (filter pushdown) */
  predicate?: Expression;

  /** Maximum rows to return */
  limit?: number;

  /** Number of rows to skip */
  offset?: number;
}

// =============================================================================
// TABLE SOURCE INTERFACE
// =============================================================================

/**
 * A table source that can be queried
 *
 * Implements a pull-based iterator model for streaming large datasets.
 */
export interface TableSource {
  /**
   * Get the URI of this source
   */
  readonly uri: string;

  /**
   * Scan the source and return rows
   *
   * @param options - Scan options for projection, filtering, and limiting
   * @returns AsyncIterator of records
   */
  scan(options?: ScanOptions): AsyncIterableIterator<Record<string, unknown>>;

  /**
   * Get the schema of the source
   *
   * May require reading the source to infer schema if not explicitly provided.
   */
  schema(): Promise<Record<string, ColumnType>>;

  /**
   * Close and release resources
   */
  close?(): Promise<void>;
}

// =============================================================================
// SOURCE OPTIONS
// =============================================================================

/**
 * Options for creating a table source
 */
export interface SourceOptions {
  /** Data format (auto-detected if not specified) */
  format?: Format;

  /** HTTP headers for URL sources */
  headers?: Record<string, string>;

  /** Explicit schema definition */
  schema?: Record<string, ColumnType>;

  /** Timeout in milliseconds */
  timeout?: number;

  /** Enable caching */
  cache?: boolean;

  /** Cache TTL in seconds */
  cacheTtl?: number;
}

/**
 * Options specific to R2 sources
 */
export interface R2SourceOptions extends SourceOptions {
  /** R2 bucket binding name */
  bucket?: R2Bucket;

  /** Enable range reads for efficient partial access */
  rangeReads?: boolean;

  /** Row group indices to read (for Parquet) */
  rowGroups?: number[];
}

/**
 * Options specific to HTTP URL sources
 */
export interface UrlSourceOptions extends SourceOptions {
  /** HTTP method (default: GET) */
  method?: 'GET' | 'POST';

  /** Request body (for POST) */
  body?: string | object;

  /** Follow redirects */
  redirect?: 'follow' | 'error' | 'manual';

  /** Credentials mode */
  credentials?: 'omit' | 'same-origin' | 'include';
}

// =============================================================================
// URI SCHEME TYPES
// =============================================================================

/**
 * Supported URI schemes
 */
export type UriScheme = 'https' | 'http' | 'r2' | 'kv' | 'd1';

/**
 * Parsed URI components
 */
export interface ParsedUri {
  scheme: UriScheme;
  host?: string;
  port?: number;
  path: string;
  query?: Record<string, string>;
  fragment?: string;
}

// =============================================================================
// TABLE SOURCE FACTORY
// =============================================================================

/**
 * Factory interface for creating table sources
 */
export interface TableSourceFactory {
  /**
   * Create a table source from a URI
   */
  create(uri: string, options?: SourceOptions): Promise<TableSource>;

  /**
   * Check if this factory supports the given URI scheme
   */
  supports(scheme: UriScheme): boolean;
}

// =============================================================================
// TYPE-LEVEL URL PARSING
// =============================================================================

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
 * Extract path from URL at compile time
 */
export type ParseUrlPath<S extends string> =
  S extends `${string}://${infer Host}/${infer Path}` ? `/${Path}` :
  S extends `${string}://${infer Host}` ? '/' :
  never;

/**
 * Extract host from URL at compile time
 */
export type ParseUrlHost<S extends string> =
  S extends `${string}://${infer Host}/${string}` ? Host :
  S extends `${string}://${infer Host}` ? Host :
  never;

/**
 * Extract file extension from URL at compile time
 */
export type ParseUrlExtension<S extends string> =
  S extends `${string}.json${string}` ? 'json' :
  S extends `${string}.ndjson${string}` ? 'ndjson' :
  S extends `${string}.jsonl${string}` ? 'ndjson' :
  S extends `${string}.csv${string}` ? 'csv' :
  S extends `${string}.parquet${string}` ? 'parquet' :
  S extends `${string}.JSON${string}` ? 'json' :
  S extends `${string}.CSV${string}` ? 'csv' :
  S extends `${string}.PARQUET${string}` ? 'parquet' :
  never;

/**
 * Infer format from URL extension at compile time
 */
export type InferFormatFromUrl<S extends string> =
  ParseUrlExtension<S> extends infer F extends Format ? F : 'json';

/**
 * Check if string is a valid URL literal for SQL FROM clause
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
 * Parse url() function call at compile time
 * Format: url('https://...', 'FORMAT')
 */
export type ParseUrlFunction<S extends string> =
  Uppercase<S> extends `URL(${string})` ? ParseUrlFunctionArgs<S> : never;

type ParseUrlFunctionArgs<S extends string> =
  S extends `${infer _}(${infer Args})` ? ParseArgs<Args> : never;

type ParseArgs<S extends string> =
  S extends `'${infer Url}',${infer Rest}` ? ParseArgsWithFormat<Url, Rest> :
  S extends `'${infer Url}'` ? { url: Url; format: 'json' } :
  S extends `"${infer Url}",${infer Rest}` ? ParseArgsWithFormat<Url, Rest> :
  S extends `"${infer Url}"` ? { url: Url; format: 'json' } :
  never;

type ParseArgsWithFormat<Url extends string, Rest extends string> =
  Trim<Rest> extends `'${infer Format}'` ? { url: Url; format: ParseFormatLiteral<Format> } :
  Trim<Rest> extends `"${infer Format}"` ? { url: Url; format: ParseFormatLiteral<Format> } :
  { url: Url; format: 'json' };

type Trim<S extends string> =
  S extends ` ${infer Rest}` ? Trim<Rest> :
  S extends `${infer Rest} ` ? Trim<Rest> :
  S;

type ParseFormatLiteral<S extends string> =
  Lowercase<S> extends 'json' ? 'json' :
  Lowercase<S> extends 'ndjson' ? 'ndjson' :
  Lowercase<S> extends 'csv' ? 'csv' :
  Lowercase<S> extends 'parquet' ? 'parquet' :
  'json';

// =============================================================================
// CLOUDFLARE WORKERS TYPES (minimal declarations)
// =============================================================================

/**
 * Minimal R2Bucket interface for type compatibility
 * Full types come from @cloudflare/workers-types
 */
export interface R2Bucket {
  get(key: string, options?: R2GetOptions): Promise<R2ObjectBody | null>;
  head(key: string): Promise<R2Object | null>;
  list(options?: R2ListOptions): Promise<R2Objects>;
}

export interface R2GetOptions {
  range?: R2Range;
}

export interface R2Range {
  offset?: number;
  length?: number;
  suffix?: number;
}

export interface R2Object {
  key: string;
  size: number;
  httpEtag: string;
  httpMetadata?: R2HTTPMetadata;
  customMetadata?: Record<string, string>;
}

export interface R2ObjectBody extends R2Object {
  body: ReadableStream<Uint8Array>;
  bodyUsed: boolean;
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
  json<T = unknown>(): Promise<T>;
  blob(): Promise<Blob>;
}

export interface R2HTTPMetadata {
  contentType?: string;
  contentLanguage?: string;
  contentDisposition?: string;
  contentEncoding?: string;
  cacheControl?: string;
  cacheExpiry?: Date;
}

export interface R2ListOptions {
  prefix?: string;
  delimiter?: string;
  cursor?: string;
  limit?: number;
}

export interface R2Objects {
  objects: R2Object[];
  truncated: boolean;
  cursor?: string;
  delimitedPrefixes: string[];
}
