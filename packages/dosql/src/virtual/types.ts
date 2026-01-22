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
 * Authentication options for URL sources
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
 * Options for URL-based virtual tables
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
 * Options parsed from WITH clause
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
 * Schema definition for a virtual table
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
 * Statistics for a virtual table
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
 * A virtual table represents a remote data source that can be queried
 */
export interface VirtualTable {
  /**
   * The URI of this virtual table
   */
  readonly uri: string;

  /**
   * The resolved format of the data
   */
  readonly format: VirtualTableFormat;

  /**
   * Get the schema of the virtual table
   * May require fetching a sample to infer the schema
   */
  getSchema(): Promise<VirtualTableSchema>;

  /**
   * Get statistics about the virtual table
   */
  getStats(): Promise<VirtualTableStats>;

  /**
   * Scan the virtual table and return matching rows
   *
   * @param options - Scan options including projection, predicate, limit, offset
   * @returns AsyncIterator of records
   */
  scan(options?: VirtualTableScanOptions): AsyncIterableIterator<Record<string, unknown>>;

  /**
   * Check if the virtual table is available
   * (e.g., URL is reachable, file exists)
   */
  isAvailable(): Promise<boolean>;

  /**
   * Close and release any resources
   */
  close(): Promise<void>;
}

/**
 * Scan options for virtual tables
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
 * Result of a virtual table scan
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
 * Supported URL schemes for virtual tables
 */
export type VirtualTableScheme = 'https' | 'http' | 'r2' | 's3' | 'file';

/**
 * Parsed virtual table URL
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
 * Parse URL scheme at compile time
 */
export type ParseVirtualTableScheme<S extends string> =
  S extends `https://${string}` ? 'https' :
  S extends `http://${string}` ? 'http' :
  S extends `r2://${string}` ? 'r2' :
  S extends `s3://${string}` ? 's3' :
  S extends `file://${string}` ? 'file' :
  never;

/**
 * Check if string is a URL literal (quoted string with valid scheme)
 */
export type IsVirtualTableUrl<S extends string> =
  S extends `'${infer Url}'` ? ParseVirtualTableScheme<Url> extends never ? false : true :
  S extends `"${infer Url}"` ? ParseVirtualTableScheme<Url> extends never ? false : true :
  ParseVirtualTableScheme<S> extends never ? false : true;

/**
 * Extract URL from quoted literal
 */
export type ExtractVirtualTableUrl<S extends string> =
  S extends `'${infer Url}'` ? Url :
  S extends `"${infer Url}"` ? Url :
  S;

/**
 * Infer format from URL extension
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
 * Factory for creating virtual tables from URLs
 */
export interface VirtualTableFactory {
  /**
   * Create a virtual table from a URL
   *
   * @param url - The URL to create a virtual table from
   * @param options - Options for the virtual table
   * @returns A virtual table instance
   */
  create(url: string, options?: URLSourceOptions): Promise<VirtualTable>;

  /**
   * Check if this factory supports the given URL scheme
   */
  supports(scheme: VirtualTableScheme): boolean;
}

/**
 * Handler for a specific URL scheme
 */
export interface SchemeHandler {
  /** The scheme this handler supports */
  scheme: VirtualTableScheme;

  /**
   * Create a virtual table for this scheme
   */
  create(url: string, options?: URLSourceOptions): Promise<VirtualTable>;

  /**
   * Parse a URL with this scheme
   */
  parse(url: string): ParsedVirtualTableUrl;
}
