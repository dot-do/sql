/**
 * Virtual Table Types for DoSQL
 *
 * Provides type definitions for virtual tables that query remote data sources.
 * This is a KEY DoSQL differentiator - query remote data sources directly:
 *
 * - SELECT * FROM 'https://api.example.com/users.json'
 * - SELECT * FROM 'r2://mybucket/data/sales.parquet' WHERE year = 2024
 * - SELECT * FROM 'https://data.gov/dataset.csv' WITH (headers=true, delimiter=',')
 *
 * @packageDocumentation
 */

import type { ColumnType } from '../parser.js';
import type { Format, ScanOptions, Expression } from '../sources/types.js';

// =============================================================================
// SUPPORTED FORMATS
// =============================================================================

/**
 * Data formats supported by virtual tables
 */
export type VirtualTableFormat = Format;

// =============================================================================
// URL SOURCE OPTIONS
// =============================================================================

/**
 * Authentication options for URL sources.
 *
 * Supports multiple authentication methods for accessing protected data sources.
 *
 * @example
 * ```typescript
 * // HTTP Basic authentication
 * const basicAuth: AuthOptions = {
 *   basic: { username: 'admin', password: 'secret123' }
 * };
 *
 * // Bearer token (OAuth, JWT)
 * const bearerAuth: AuthOptions = {
 *   bearer: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...'
 * };
 *
 * // API key in custom header
 * const apiKeyAuth: AuthOptions = {
 *   apiKey: { header: 'X-API-Key', value: 'sk_live_abc123' }
 * };
 *
 * // Custom headers for non-standard auth
 * const customAuth: AuthOptions = {
 *   headers: { 'X-Custom-Token': 'token123', 'X-Org-Id': 'org456' }
 * };
 * ```
 */
export interface AuthOptions {
  /** HTTP Basic authentication */
  basic?: {
    username: string;
    password: string;
  };
  /** Bearer token authentication */
  bearer?: string;
  /** API key authentication */
  apiKey?: {
    header: string;
    value: string;
  };
  /** Custom headers for authentication */
  headers?: Record<string, string>;
}

/**
 * Options for URL-based virtual tables.
 *
 * Configure how DoSQL fetches and parses remote data sources.
 *
 * @example
 * ```typescript
 * // Basic JSON API with caching
 * const jsonOptions: URLSourceOptions = {
 *   format: 'json',
 *   cache: true,
 *   cacheTtl: 600, // 10 minutes
 *   timeout: 5000
 * };
 *
 * // CSV file with custom parsing
 * const csvOptions: URLSourceOptions = {
 *   format: 'csv',
 *   delimiter: ';',
 *   csvHeaders: true,
 *   schema: { id: 'integer', name: 'text', price: 'real' }
 * };
 *
 * // Authenticated API with bearer token
 * const authOptions: URLSourceOptions = {
 *   auth: { bearer: 'your-jwt-token' },
 *   headers: { 'Accept': 'application/json' },
 *   timeout: 10000
 * };
 *
 * // POST request with body
 * const postOptions: URLSourceOptions = {
 *   method: 'POST',
 *   body: { query: 'active=true', limit: 100 },
 *   headers: { 'Content-Type': 'application/json' }
 * };
 * ```
 */
export interface URLSourceOptions {
  /** Data format (auto-detected if not specified) */
  format?: VirtualTableFormat;

  /** HTTP headers to include in requests */
  headers?: Record<string, string>;

  /** Authentication configuration */
  auth?: AuthOptions;

  /** Request timeout in milliseconds */
  timeout?: number;

  /** HTTP method (default: GET) */
  method?: 'GET' | 'POST';

  /** Request body for POST requests */
  body?: string | Record<string, unknown>;

  /** Follow redirects (default: follow) */
  redirect?: 'follow' | 'error' | 'manual';

  /** Credentials mode */
  credentials?: 'omit' | 'same-origin' | 'include';

  // CSV-specific options
  /** CSV delimiter (default: ',') */
  delimiter?: string;

  /** CSV quote character (default: '"') */
  quote?: string;

  /** Whether CSV has header row (default: true) */
  csvHeaders?: boolean;

  // Caching options
  /** Enable caching with Cache API */
  cache?: boolean;

  /** Cache TTL in seconds (default: 300) */
  cacheTtl?: number;

  /** Cache key prefix */
  cacheKeyPrefix?: string;

  // Schema options
  /** Explicit schema definition */
  schema?: Record<string, ColumnType>;

  /** Maximum samples for schema inference */
  schemaSampleSize?: number;
}

/**
 * Options parsed from SQL WITH clause.
 *
 * These options are extracted from the WITH clause in SQL queries
 * and converted into URLSourceOptions for execution.
 *
 * @example
 * ```sql
 * -- CSV with custom delimiter and headers
 * SELECT * FROM 'https://api.example.com/data.csv'
 * WITH (format='csv', delimiter=';', headers=true)
 *
 * -- JSON with caching enabled
 * SELECT * FROM 'https://api.example.com/users.json'
 * WITH (cache=true, cacheTtl=300, timeout=5000)
 *
 * -- API with bearer token authentication
 * SELECT * FROM 'https://api.example.com/private/data.json'
 * WITH (auth='bearer', token='eyJhbGc...')
 *
 * -- API with basic authentication
 * SELECT * FROM 'https://api.example.com/secure/data.json'
 * WITH (auth='basic', username='admin', password='secret')
 * ```
 */
export interface WithClauseOptions {
  /** Data format */
  format?: VirtualTableFormat;

  /** Whether data has headers (CSV) */
  headers?: boolean;

  /** Field delimiter (CSV) */
  delimiter?: string;

  /** Quote character (CSV) */
  quote?: string;

  /** Request timeout in milliseconds */
  timeout?: number;

  /** Enable caching */
  cache?: boolean;

  /** Cache TTL in seconds */
  cacheTtl?: number;

  /** Authentication type */
  auth?: 'basic' | 'bearer' | 'api-key';

  /** Auth username (for basic auth) */
  username?: string;

  /** Auth password (for basic auth) */
  password?: string;

  /** Auth token (for bearer auth) */
  token?: string;

  /** API key header name */
  apiKeyHeader?: string;

  /** API key value */
  apiKeyValue?: string;
}

// =============================================================================
// VIRTUAL TABLE INTERFACE
// =============================================================================

/**
 * Schema definition for a virtual table.
 *
 * Describes the structure of data in a virtual table, used for
 * type checking, query validation, and query planning.
 *
 * @example
 * ```typescript
 * // User table schema
 * const userSchema: VirtualTableSchema = {
 *   columns: {
 *     id: 'integer',
 *     email: 'text',
 *     name: 'text',
 *     created_at: 'text',
 *     is_active: 'integer' // SQLite boolean
 *   },
 *   primaryKey: ['id'],
 *   estimatedRows: 10000
 * };
 *
 * // Product catalog schema
 * const productSchema: VirtualTableSchema = {
 *   columns: {
 *     sku: 'text',
 *     name: 'text',
 *     price: 'real',
 *     quantity: 'integer',
 *     metadata: 'blob' // JSON stored as blob
 *   },
 *   primaryKey: ['sku'],
 *   estimatedRows: 50000
 * };
 * ```
 */
export interface VirtualTableSchema {
  /** Column definitions */
  columns: Record<string, ColumnType>;

  /** Primary key columns (if any) */
  primaryKey?: string[];

  /** Estimated row count (for query planning) */
  estimatedRows?: number;
}

/**
 * Statistics for a virtual table.
 *
 * Provides metadata about the data source for query optimization
 * and monitoring purposes.
 *
 * @example
 * ```typescript
 * // Exact stats from a known data source
 * const exactStats: VirtualTableStats = {
 *   rowCount: 15420,
 *   sizeBytes: 2_340_000,
 *   lastModified: new Date('2024-01-15T10:30:00Z'),
 *   isEstimate: false
 * };
 *
 * // Estimated stats (e.g., from sampling)
 * const estimatedStats: VirtualTableStats = {
 *   rowCount: 50000,
 *   isEstimate: true
 * };
 *
 * // Using stats for query decisions
 * const stats = await virtualTable.getStats();
 * if (stats.rowCount && stats.rowCount > 100000) {
 *   console.log('Large dataset, consider adding LIMIT');
 * }
 * ```
 */
export interface VirtualTableStats {
  /** Total rows (if known) */
  rowCount?: number;

  /** Size in bytes (if known) */
  sizeBytes?: number;

  /** Last modified time */
  lastModified?: Date;

  /** Whether stats are estimated or exact */
  isEstimate: boolean;
}

/**
 * A virtual table represents a remote data source that can be queried.
 *
 * Virtual tables are the core abstraction that enables DoSQL to query
 * remote URLs, R2 buckets, and other external data sources directly.
 *
 * @example
 * ```typescript
 * // Create and use a virtual table
 * const table = await factory.create('https://api.example.com/users.json');
 *
 * // Check availability before querying
 * if (await table.isAvailable()) {
 *   // Get schema for type information
 *   const schema = await table.getSchema();
 *   console.log('Columns:', Object.keys(schema.columns));
 *
 *   // Scan with filtering and projection
 *   const rows = table.scan({
 *     columns: ['id', 'name', 'email'],
 *     predicate: { column: 'status', op: '=', value: 'active' },
 *     limit: 100
 *   });
 *
 *   for await (const row of rows) {
 *     console.log(row);
 *   }
 * }
 *
 * // Always close when done
 * await table.close();
 * ```
 *
 * @example
 * ```typescript
 * // Query R2 bucket data
 * const parquetTable = await factory.create('r2://mybucket/sales/2024.parquet');
 * const stats = await parquetTable.getStats();
 * console.log(`Dataset size: ${stats.sizeBytes} bytes, ${stats.rowCount} rows`);
 * ```
 */
export interface VirtualTable {
  /**
   * The URI of this virtual table.
   *
   * @example
   * ```typescript
   * console.log(table.uri); // 'https://api.example.com/data.json'
   * ```
   */
  readonly uri: string;

  /**
   * The resolved format of the data.
   *
   * @example
   * ```typescript
   * if (table.format === 'parquet') {
   *   console.log('Using columnar format for efficient queries');
   * }
   * ```
   */
  readonly format: VirtualTableFormat;

  /**
   * Get the schema of the virtual table.
   * May require fetching a sample to infer the schema.
   *
   * @example
   * ```typescript
   * const schema = await table.getSchema();
   * for (const [column, type] of Object.entries(schema.columns)) {
   *   console.log(`${column}: ${type}`);
   * }
   * ```
   */
  getSchema(): Promise<VirtualTableSchema>;

  /**
   * Get statistics about the virtual table.
   *
   * @example
   * ```typescript
   * const stats = await table.getStats();
   * if (!stats.isEstimate && stats.rowCount) {
   *   console.log(`Exact row count: ${stats.rowCount}`);
   * }
   * ```
   */
  getStats(): Promise<VirtualTableStats>;

  /**
   * Scan the virtual table and return matching rows.
   *
   * @param options - Scan options including projection, predicate, limit, offset
   * @returns AsyncIterator of records
   *
   * @example
   * ```typescript
   * // Full scan with limit
   * for await (const row of table.scan({ limit: 10 })) {
   *   console.log(row);
   * }
   *
   * // Filtered scan with projection
   * const options: VirtualTableScanOptions = {
   *   columns: ['id', 'name'],
   *   predicate: { column: 'active', op: '=', value: true },
   *   limit: 100,
   *   offset: 0
   * };
   * for await (const row of table.scan(options)) {
   *   console.log(row.id, row.name);
   * }
   * ```
   */
  scan(options?: VirtualTableScanOptions): AsyncIterableIterator<Record<string, unknown>>;

  /**
   * Check if the virtual table is available.
   * (e.g., URL is reachable, file exists)
   *
   * @example
   * ```typescript
   * if (await table.isAvailable()) {
   *   // Safe to query
   * } else {
   *   console.error('Data source unavailable');
   * }
   * ```
   */
  isAvailable(): Promise<boolean>;

  /**
   * Close and release any resources.
   *
   * @example
   * ```typescript
   * try {
   *   for await (const row of table.scan()) {
   *     // process rows
   *   }
   * } finally {
   *   await table.close();
   * }
   * ```
   */
  close(): Promise<void>;
}

/**
 * Scan options for virtual tables.
 *
 * Extends base ScanOptions with virtual-table-specific options
 * for cache control and parallel execution.
 *
 * @example
 * ```typescript
 * // Basic scan with limit
 * const basicOptions: VirtualTableScanOptions = {
 *   limit: 100,
 *   offset: 0
 * };
 *
 * // Force fresh data (skip cache)
 * const freshOptions: VirtualTableScanOptions = {
 *   noCache: true,
 *   limit: 50
 * };
 *
 * // Parallel scan for partitioned Parquet files
 * const parallelOptions: VirtualTableScanOptions = {
 *   concurrency: 4,
 *   columns: ['id', 'amount', 'date']
 * };
 *
 * // Full options example
 * const fullOptions: VirtualTableScanOptions = {
 *   columns: ['user_id', 'action', 'timestamp'],
 *   predicate: { column: 'timestamp', op: '>', value: '2024-01-01' },
 *   limit: 1000,
 *   offset: 0,
 *   noCache: false,
 *   concurrency: 2
 * };
 * ```
 */
export interface VirtualTableScanOptions extends ScanOptions {
  /** Force refresh cache */
  noCache?: boolean;

  /** Maximum concurrent requests (for partitioned data) */
  concurrency?: number;
}

// =============================================================================
// VIRTUAL TABLE RESULT
// =============================================================================

/**
 * Result of a virtual table scan.
 *
 * Contains the scanned rows along with schema information and
 * performance statistics for monitoring and debugging.
 *
 * @example
 * ```typescript
 * // Type-safe result with custom row type
 * interface User {
 *   id: number;
 *   name: string;
 *   email: string;
 * }
 *
 * const result: VirtualTableResult<User> = {
 *   rows: [
 *     { id: 1, name: 'Alice', email: 'alice@example.com' },
 *     { id: 2, name: 'Bob', email: 'bob@example.com' }
 *   ],
 *   schema: {
 *     columns: { id: 'integer', name: 'text', email: 'text' },
 *     primaryKey: ['id']
 *   },
 *   stats: {
 *     rowCount: 2,
 *     timeToFirstRow: 45,
 *     totalTime: 120,
 *     fromCache: false,
 *     bytesRead: 1024
 *   }
 * };
 *
 * // Log performance metrics
 * console.log(`Fetched ${result.stats.rowCount} rows in ${result.stats.totalTime}ms`);
 * if (result.stats.fromCache) {
 *   console.log('Result served from cache');
 * }
 * ```
 */
export interface VirtualTableResult<T = Record<string, unknown>> {
  /** The scanned rows */
  rows: T[];

  /** Schema of the result */
  schema: VirtualTableSchema;

  /** Statistics about the scan */
  stats: {
    /** Number of rows returned */
    rowCount: number;

    /** Time to first row in milliseconds */
    timeToFirstRow?: number;

    /** Total scan time in milliseconds */
    totalTime: number;

    /** Whether result was from cache */
    fromCache: boolean;

    /** Bytes read */
    bytesRead?: number;
  };
}

// =============================================================================
// URL SCHEMES
// =============================================================================

/**
 * Supported URL schemes for virtual tables.
 *
 * DoSQL supports multiple protocols for accessing remote data:
 * - `https`/`http`: Standard web URLs for APIs and hosted files
 * - `r2`: Cloudflare R2 object storage
 * - `s3`: AWS S3 buckets
 * - `file`: Local filesystem (for development/testing)
 *
 * @example
 * ```typescript
 * // HTTPS - most common for APIs
 * const scheme1: VirtualTableScheme = 'https';
 * // SELECT * FROM 'https://api.example.com/data.json'
 *
 * // R2 - Cloudflare's object storage
 * const scheme2: VirtualTableScheme = 'r2';
 * // SELECT * FROM 'r2://mybucket/path/to/data.parquet'
 *
 * // S3 - AWS object storage
 * const scheme3: VirtualTableScheme = 's3';
 * // SELECT * FROM 's3://mybucket/analytics/events.csv'
 * ```
 */
export type VirtualTableScheme = 'https' | 'http' | 'r2' | 's3' | 'file';

/**
 * Parsed virtual table URL.
 *
 * Represents a URL broken down into its components for processing
 * by the appropriate scheme handler.
 *
 * @example
 * ```typescript
 * // Parsed HTTPS URL
 * const httpsUrl: ParsedVirtualTableUrl = {
 *   scheme: 'https',
 *   host: 'api.example.com',
 *   path: '/v1/users.json',
 *   query: { page: '1', limit: '100' },
 *   detectedFormat: 'json'
 * };
 *
 * // Parsed R2 URL
 * const r2Url: ParsedVirtualTableUrl = {
 *   scheme: 'r2',
 *   host: 'my-data-bucket',
 *   path: '/analytics/sales/2024-01.parquet',
 *   detectedFormat: 'parquet'
 * };
 *
 * // Parsed S3 URL with port
 * const s3Url: ParsedVirtualTableUrl = {
 *   scheme: 'http',
 *   host: 'localhost',
 *   port: 9000,
 *   path: '/testbucket/data.csv',
 *   detectedFormat: 'csv'
 * };
 * ```
 */
export interface ParsedVirtualTableUrl {
  /** URL scheme */
  scheme: VirtualTableScheme;

  /** Host (for http/https) or bucket name (for r2/s3) */
  host: string;

  /** Port number (if specified) */
  port?: number;

  /** Path within the host/bucket */
  path: string;

  /** Query parameters */
  query?: Record<string, string>;

  /** Fragment (hash) */
  fragment?: string;

  /** Detected format from extension */
  detectedFormat?: VirtualTableFormat;
}

// =============================================================================
// TYPE-LEVEL URL PARSING
// =============================================================================

/**
 * Parse URL scheme at compile time.
 *
 * A utility type that extracts the scheme from a URL string literal,
 * enabling type-safe URL handling at compile time.
 *
 * @example
 * ```typescript
 * // Type-level scheme extraction
 * type Scheme1 = ParseVirtualTableScheme<'https://api.example.com/data.json'>;
 * // Result: 'https'
 *
 * type Scheme2 = ParseVirtualTableScheme<'r2://bucket/file.parquet'>;
 * // Result: 'r2'
 *
 * type Scheme3 = ParseVirtualTableScheme<'ftp://invalid.com/file'>;
 * // Result: never (unsupported scheme)
 *
 * // Use in conditional types
 * type IsSecure<U extends string> =
 *   ParseVirtualTableScheme<U> extends 'https' ? true : false;
 *
 * type Test1 = IsSecure<'https://secure.com/data.json'>; // true
 * type Test2 = IsSecure<'http://insecure.com/data.json'>; // false
 * ```
 */
export type ParseVirtualTableScheme<S extends string> =
  S extends `https://${string}` ? 'https' :
  S extends `http://${string}` ? 'http' :
  S extends `r2://${string}` ? 'r2' :
  S extends `s3://${string}` ? 's3' :
  S extends `file://${string}` ? 'file' :
  never;

/**
 * Check if string is a URL literal (quoted string with valid scheme).
 *
 * Validates that a string represents a virtual table URL, handling
 * both quoted SQL literals and raw URL strings.
 *
 * @example
 * ```typescript
 * // Single-quoted SQL literal
 * type Test1 = IsVirtualTableUrl<"'https://api.example.com/data.json'">;
 * // Result: true
 *
 * // Double-quoted SQL literal
 * type Test2 = IsVirtualTableUrl<'"r2://bucket/file.parquet"'>;
 * // Result: true
 *
 * // Raw URL string
 * type Test3 = IsVirtualTableUrl<'https://api.example.com/data.json'>;
 * // Result: true
 *
 * // Invalid scheme
 * type Test4 = IsVirtualTableUrl<"'ftp://invalid.com/file'">;
 * // Result: false
 *
 * // Regular table name (not a URL)
 * type Test5 = IsVirtualTableUrl<'users'>;
 * // Result: false
 * ```
 */
export type IsVirtualTableUrl<S extends string> =
  S extends `'${infer Url}'` ? ParseVirtualTableScheme<Url> extends never ? false : true :
  S extends `"${infer Url}"` ? ParseVirtualTableScheme<Url> extends never ? false : true :
  ParseVirtualTableScheme<S> extends never ? false : true;

/**
 * Extract URL from quoted literal.
 *
 * Removes surrounding quotes from SQL string literals to get the raw URL.
 *
 * @example
 * ```typescript
 * // Single quotes
 * type Url1 = ExtractVirtualTableUrl<"'https://api.example.com/data.json'">;
 * // Result: 'https://api.example.com/data.json'
 *
 * // Double quotes
 * type Url2 = ExtractVirtualTableUrl<'"r2://bucket/file.parquet"'>;
 * // Result: 'r2://bucket/file.parquet'
 *
 * // Already unquoted (passthrough)
 * type Url3 = ExtractVirtualTableUrl<'https://api.example.com/data.json'>;
 * // Result: 'https://api.example.com/data.json'
 *
 * // Use case: Parse SQL FROM clause
 * type FromClause = "'https://api.example.com/users.json'";
 * type ActualUrl = ExtractVirtualTableUrl<FromClause>;
 * // ActualUrl = 'https://api.example.com/users.json'
 * ```
 */
export type ExtractVirtualTableUrl<S extends string> =
  S extends `'${infer Url}'` ? Url :
  S extends `"${infer Url}"` ? Url :
  S;

/**
 * Infer format from URL extension.
 *
 * Automatically detects the data format based on file extension,
 * supporting both lowercase and uppercase extensions.
 * Defaults to 'json' when format cannot be determined.
 *
 * @example
 * ```typescript
 * // JSON detection
 * type Format1 = InferVirtualTableFormat<'https://api.example.com/users.json'>;
 * // Result: 'json'
 *
 * // CSV detection
 * type Format2 = InferVirtualTableFormat<'r2://bucket/data.csv'>;
 * // Result: 'csv'
 *
 * // Parquet detection
 * type Format3 = InferVirtualTableFormat<'s3://bucket/analytics.parquet'>;
 * // Result: 'parquet'
 *
 * // NDJSON/JSONL detection
 * type Format4 = InferVirtualTableFormat<'https://stream.example.com/events.ndjson'>;
 * // Result: 'ndjson'
 * type Format5 = InferVirtualTableFormat<'https://stream.example.com/logs.jsonl'>;
 * // Result: 'ndjson'
 *
 * // Case-insensitive
 * type Format6 = InferVirtualTableFormat<'https://example.com/DATA.CSV'>;
 * // Result: 'csv'
 *
 * // Default to JSON for unknown extensions
 * type Format7 = InferVirtualTableFormat<'https://api.example.com/endpoint'>;
 * // Result: 'json'
 * ```
 */
export type InferVirtualTableFormat<S extends string> =
  S extends `${string}.json${string}` ? 'json' :
  S extends `${string}.ndjson${string}` ? 'ndjson' :
  S extends `${string}.jsonl${string}` ? 'ndjson' :
  S extends `${string}.csv${string}` ? 'csv' :
  S extends `${string}.parquet${string}` ? 'parquet' :
  S extends `${string}.JSON${string}` ? 'json' :
  S extends `${string}.CSV${string}` ? 'csv' :
  S extends `${string}.PARQUET${string}` ? 'parquet' :
  'json'; // Default to JSON

// =============================================================================
// FACTORY TYPES
// =============================================================================

/**
 * Factory for creating virtual tables from URLs.
 *
 * The central entry point for instantiating virtual tables from
 * various URL schemes and data formats.
 *
 * @example
 * ```typescript
 * // Basic factory usage
 * const factory: VirtualTableFactory = createVirtualTableFactory();
 *
 * // Create virtual table from JSON API
 * const usersTable = await factory.create(
 *   'https://api.example.com/users.json',
 *   { cache: true, cacheTtl: 300 }
 * );
 *
 * // Create virtual table from R2 Parquet file
 * const salesTable = await factory.create(
 *   'r2://analytics-bucket/sales/2024.parquet'
 * );
 *
 * // Create virtual table from CSV with auth
 * const privateData = await factory.create(
 *   'https://api.example.com/private/data.csv',
 *   {
 *     format: 'csv',
 *     auth: { bearer: 'your-token' },
 *     csvHeaders: true
 *   }
 * );
 *
 * // Check scheme support before creating
 * if (factory.supports('r2')) {
 *   const table = await factory.create('r2://bucket/file.json');
 * }
 * ```
 */
export interface VirtualTableFactory {
  /**
   * Create a virtual table from a URL.
   *
   * @param url - The URL to create a virtual table from
   * @param options - Options for the virtual table
   * @returns A virtual table instance
   *
   * @example
   * ```typescript
   * const table = await factory.create(
   *   'https://api.example.com/data.json',
   *   { timeout: 5000, cache: true }
   * );
   * ```
   */
  create(url: string, options?: URLSourceOptions): Promise<VirtualTable>;

  /**
   * Check if this factory supports the given URL scheme.
   *
   * @example
   * ```typescript
   * factory.supports('https'); // true
   * factory.supports('r2');    // true (if R2 handler registered)
   * factory.supports('ftp');   // false
   * ```
   */
  supports(scheme: VirtualTableScheme): boolean;
}

/**
 * Handler for a specific URL scheme.
 *
 * Implement this interface to add support for custom URL schemes
 * (e.g., custom storage backends, internal APIs).
 *
 * @example
 * ```typescript
 * // Custom R2 scheme handler
 * const r2Handler: SchemeHandler = {
 *   scheme: 'r2',
 *
 *   async create(url: string, options?: URLSourceOptions): Promise<VirtualTable> {
 *     const parsed = this.parse(url);
 *     const bucket = env.R2_BUCKETS[parsed.host];
 *     return new R2VirtualTable(bucket, parsed.path, options);
 *   },
 *
 *   parse(url: string): ParsedVirtualTableUrl {
 *     const match = url.match(/^r2:\/\/([^/]+)(.*)$/);
 *     if (!match) throw new Error('Invalid R2 URL');
 *     return {
 *       scheme: 'r2',
 *       host: match[1],      // bucket name
 *       path: match[2],      // object key
 *       detectedFormat: inferFormat(match[2])
 *     };
 *   }
 * };
 *
 * // Register the handler
 * factory.registerHandler(r2Handler);
 *
 * // Now R2 URLs work
 * const table = await factory.create('r2://mybucket/data.parquet');
 * ```
 */
export interface SchemeHandler {
  /** The scheme this handler supports */
  scheme: VirtualTableScheme;

  /**
   * Create a virtual table for this scheme.
   *
   * @example
   * ```typescript
   * const table = await handler.create(
   *   'r2://mybucket/data.json',
   *   { cache: true }
   * );
   * ```
   */
  create(url: string, options?: URLSourceOptions): Promise<VirtualTable>;

  /**
   * Parse a URL with this scheme.
   *
   * @example
   * ```typescript
   * const parsed = handler.parse('r2://mybucket/path/to/file.parquet');
   * console.log(parsed.host); // 'mybucket'
   * console.log(parsed.path); // '/path/to/file.parquet'
   * console.log(parsed.detectedFormat); // 'parquet'
   * ```
   */
  parse(url: string): ParsedVirtualTableUrl;
}
