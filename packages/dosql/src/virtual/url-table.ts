/**
 * URL-based Virtual Table Implementation
 *
 * Provides virtual table support for HTTP/HTTPS URLs with:
 * - Format auto-detection from extension and Content-Type
 * - Support for JSON, CSV, Parquet, NDJSON
 * - Caching with Cache API
 * - Authentication support (Basic, Bearer, API Key)
 *
 * @packageDocumentation
 */

import type { ColumnType } from '../parser.js';
import type {
  VirtualTable,
  VirtualTableSchema,
  VirtualTableStats,
  VirtualTableScanOptions,
  URLSourceOptions,
  VirtualTableFormat,
  ParsedVirtualTableUrl,
  AuthOptions,
} from './types.js';
import {
  detectFormat,
  createFormatIterator,
  inferSchema,
  type CsvParseOptions,
} from '../sources/parser.js';
import type { Expression } from '../sources/types.js';

// =============================================================================
// URL PARSING
// =============================================================================

/**
 * Parse a URL into its components
 */
export function parseVirtualTableUrl(url: string): ParsedVirtualTableUrl {
  // Handle r2:// scheme
  if (url.startsWith('r2://')) {
    const match = url.match(/^r2:\/\/([^/]+)(?:\/(.*))?$/);
    if (!match) {
      throw new Error(`Invalid r2:// URL: ${url}`);
    }
    const path = match[2] ? `/${match[2]}` : '/';
    return {
      scheme: 'r2',
      host: match[1],
      path,
      detectedFormat: detectFormatFromPath(path),
    };
  }

  // Handle s3:// scheme
  if (url.startsWith('s3://')) {
    const match = url.match(/^s3:\/\/([^/]+)(?:\/(.*))?$/);
    if (!match) {
      throw new Error(`Invalid s3:// URL: ${url}`);
    }
    const path = match[2] ? `/${match[2]}` : '/';
    return {
      scheme: 's3',
      host: match[1],
      path,
      detectedFormat: detectFormatFromPath(path),
    };
  }

  // Handle file:// scheme
  if (url.startsWith('file://')) {
    const path = url.slice(7);
    return {
      scheme: 'file',
      host: 'localhost',
      path: path.startsWith('/') ? path : `/${path}`,
      detectedFormat: detectFormatFromPath(path),
    };
  }

  // Use standard URL parser for http/https
  try {
    const parsed = new URL(url);
    const scheme = parsed.protocol.slice(0, -1);

    if (scheme !== 'http' && scheme !== 'https') {
      throw new Error(`Unsupported URL scheme: ${scheme}`);
    }

    // Parse query parameters
    const query: Record<string, string> = {};
    parsed.searchParams.forEach((value, key) => {
      query[key] = value;
    });

    return {
      scheme: scheme as 'http' | 'https',
      host: parsed.hostname,
      port: parsed.port ? parseInt(parsed.port, 10) : undefined,
      path: parsed.pathname,
      query: Object.keys(query).length > 0 ? query : undefined,
      fragment: parsed.hash ? parsed.hash.slice(1) : undefined,
      detectedFormat: detectFormatFromPath(parsed.pathname),
    };
  } catch (e) {
    throw new Error(`Invalid URL: ${url}`);
  }
}

/**
 * Detect format from file path extension
 */
function detectFormatFromPath(path: string): VirtualTableFormat | undefined {
  // Remove query string and fragment
  const cleanPath = path.split(/[?#]/)[0];
  const ext = cleanPath.split('.').pop()?.toLowerCase();

  switch (ext) {
    case 'json':
      return 'json';
    case 'ndjson':
    case 'jsonl':
      return 'ndjson';
    case 'csv':
      return 'csv';
    case 'parquet':
      return 'parquet';
    default:
      return undefined;
  }
}

// =============================================================================
// CACHING
// =============================================================================

/**
 * Cache key generator for virtual table URLs
 */
function generateCacheKey(url: string, prefix?: string): string {
  const hash = simpleHash(url);
  return prefix ? `${prefix}:${hash}` : `vtable:${hash}`;
}

/**
 * Simple string hash function
 */
function simpleHash(str: string): string {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return Math.abs(hash).toString(36);
}

/**
 * Virtual table cache interface
 */
interface VirtualTableCache {
  get(key: string): Promise<Response | undefined>;
  put(key: string, response: Response, ttl: number): Promise<void>;
  delete(key: string): Promise<void>;
}

/**
 * Create a cache adapter using the Cache API (Cloudflare Workers compatible)
 */
function createCacheAdapter(): VirtualTableCache | null {
  // Check if Cache API is available
  if (typeof caches === 'undefined') {
    return null;
  }

  return {
    async get(key: string): Promise<Response | undefined> {
      try {
        const cache = await caches.open('dosql-virtual-tables');
        const response = await cache.match(new Request(`https://cache.dosql/${key}`));
        return response || undefined;
      } catch {
        return undefined;
      }
    },

    async put(key: string, response: Response, ttl: number): Promise<void> {
      try {
        const cache = await caches.open('dosql-virtual-tables');
        const clonedResponse = new Response(response.clone().body, {
          status: response.status,
          statusText: response.statusText,
          headers: {
            ...Object.fromEntries(response.headers.entries()),
            'Cache-Control': `max-age=${ttl}`,
          },
        });
        await cache.put(new Request(`https://cache.dosql/${key}`), clonedResponse);
      } catch {
        // Ignore cache errors
      }
    },

    async delete(key: string): Promise<void> {
      try {
        const cache = await caches.open('dosql-virtual-tables');
        await cache.delete(new Request(`https://cache.dosql/${key}`));
      } catch {
        // Ignore cache errors
      }
    },
  };
}

// =============================================================================
// URL VIRTUAL TABLE
// =============================================================================

/**
 * Virtual table backed by HTTP/HTTPS URL
 */
export class URLVirtualTable implements VirtualTable {
  readonly uri: string;
  readonly format: VirtualTableFormat;

  private readonly options: URLSourceOptions;
  private readonly parsedUrl: ParsedVirtualTableUrl;
  private cachedSchema: VirtualTableSchema | null = null;
  private cache: VirtualTableCache | null = null;

  constructor(url: string, options: URLSourceOptions = {}) {
    this.uri = url;
    this.options = options;
    this.parsedUrl = parseVirtualTableUrl(url);

    // Determine format
    this.format = this.determineFormat();

    // Initialize cache if enabled
    if (options.cache !== false) {
      this.cache = createCacheAdapter();
    }
  }

  /**
   * Determine the data format
   */
  private determineFormat(): VirtualTableFormat {
    // Explicit format takes precedence
    if (this.options.format) {
      return this.options.format;
    }

    // Use detected format from URL
    if (this.parsedUrl.detectedFormat) {
      return this.parsedUrl.detectedFormat;
    }

    // Default to JSON
    return 'json';
  }

  /**
   * Build authentication headers
   */
  private buildAuthHeaders(): Record<string, string> {
    const headers: Record<string, string> = {};
    const auth = this.options.auth;

    if (!auth) return headers;

    if (auth.basic) {
      const credentials = btoa(`${auth.basic.username}:${auth.basic.password}`);
      headers['Authorization'] = `Basic ${credentials}`;
    } else if (auth.bearer) {
      headers['Authorization'] = `Bearer ${auth.bearer}`;
    } else if (auth.apiKey) {
      headers[auth.apiKey.header] = auth.apiKey.value;
    }

    // Add any custom auth headers
    if (auth.headers) {
      Object.assign(headers, auth.headers);
    }

    return headers;
  }

  /**
   * Fetch the URL with configured options
   */
  private async fetch(noCache: boolean = false): Promise<Response> {
    const cacheKey = generateCacheKey(this.uri, this.options.cacheKeyPrefix);

    // Try cache first (if enabled and not bypassed)
    if (!noCache && this.cache && this.options.cache !== false) {
      const cachedResponse = await this.cache.get(cacheKey);
      if (cachedResponse) {
        return cachedResponse;
      }
    }

    // Build headers
    const headers: Record<string, string> = {
      ...this.options.headers,
      ...this.buildAuthHeaders(),
    };

    // Build request options
    const { method = 'GET', body, redirect = 'follow', credentials, timeout } = this.options;

    const controller = new AbortController();
    let timeoutId: ReturnType<typeof setTimeout> | undefined;

    if (timeout) {
      timeoutId = setTimeout(() => controller.abort(), timeout);
    }

    try {
      const response = await fetch(this.uri, {
        method,
        headers,
        body: body ? (typeof body === 'string' ? body : JSON.stringify(body)) : undefined,
        redirect,
        credentials,
        signal: controller.signal,
      });

      // Cache successful responses (only if response supports clone)
      if (response.ok && this.cache && this.options.cache !== false && typeof response.clone === 'function') {
        const ttl = this.options.cacheTtl ?? 300; // Default 5 minutes
        await this.cache.put(cacheKey, response.clone(), ttl);
      }

      return response;
    } finally {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    }
  }

  /**
   * Get or infer the schema
   */
  async getSchema(): Promise<VirtualTableSchema> {
    if (this.cachedSchema) {
      return this.cachedSchema;
    }

    // Use explicit schema if provided
    if (this.options.schema) {
      this.cachedSchema = {
        columns: this.options.schema,
      };
      return this.cachedSchema;
    }

    // Infer schema from sample
    const sampleSize = this.options.schemaSampleSize ?? 100;
    const records: Record<string, unknown>[] = [];

    for await (const record of this.scan({ limit: sampleSize })) {
      records.push(record);
      if (records.length >= sampleSize) break;
    }

    const inferredColumns = inferSchema(records);
    this.cachedSchema = {
      columns: inferredColumns,
      estimatedRows: records.length < sampleSize ? records.length : undefined,
    };

    return this.cachedSchema;
  }

  /**
   * Get statistics about the virtual table
   */
  async getStats(): Promise<VirtualTableStats> {
    try {
      // Try HEAD request for metadata
      const response = await fetch(this.uri, {
        method: 'HEAD',
        headers: this.buildAuthHeaders(),
      });

      if (!response.ok) {
        return { isEstimate: true };
      }

      const contentLength = response.headers.get('Content-Length');
      const lastModified = response.headers.get('Last-Modified');

      return {
        sizeBytes: contentLength ? parseInt(contentLength, 10) : undefined,
        lastModified: lastModified ? new Date(lastModified) : undefined,
        isEstimate: false,
      };
    } catch {
      return { isEstimate: true };
    }
  }

  /**
   * Scan the virtual table
   */
  async *scan(options: VirtualTableScanOptions = {}): AsyncIterableIterator<Record<string, unknown>> {
    const { projection, predicate, limit, offset = 0, noCache = false } = options;

    // Fetch the URL
    const response = await this.fetch(noCache);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText} for ${this.uri}`);
    }

    // Determine format from response if needed
    let format = this.format;
    const contentType = response.headers.get('Content-Type');
    if (!this.options.format && contentType) {
      const detected = detectFormat(this.uri, contentType);
      if (detected.confidence === 'high') {
        format = detected.format;
      }
    }

    // Create format-specific iterator
    const iterator = this.createIterator(response, format);

    // Apply scan operations
    let count = 0;
    let skipped = 0;

    for await (const record of iterator) {
      // Apply offset
      if (skipped < offset) {
        skipped++;
        continue;
      }

      // Apply predicate filter
      if (predicate && !this.matchesPredicate(record, predicate)) {
        continue;
      }

      // Apply projection
      const projected = projection ? this.project(record, projection) : record;

      yield projected;

      // Apply limit
      count++;
      if (limit !== undefined && count >= limit) {
        break;
      }
    }
  }

  /**
   * Create format-specific iterator
   */
  private createIterator(
    response: Response,
    format: VirtualTableFormat
  ): AsyncIterableIterator<Record<string, unknown>> {
    // Build CSV options
    const csvOptions: CsvParseOptions = {
      delimiter: this.options.delimiter,
      quote: this.options.quote,
      header: this.options.csvHeaders ?? true,
    };

    // For streaming formats (NDJSON), use the body stream
    if (format === 'ndjson' && response.body) {
      return createFormatIterator(format, response.body, csvOptions);
    }

    // For CSV with streaming support
    if (format === 'csv' && response.body) {
      return createFormatIterator(format, response.body, csvOptions);
    }

    // For other formats, read the full response
    const self = this;
    return (async function* () {
      const text = await response.text();
      yield* createFormatIterator(format, text, csvOptions);
    })();
  }

  /**
   * Apply projection to select specific columns
   */
  private project(
    record: Record<string, unknown>,
    columns: string[]
  ): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    for (const col of columns) {
      if (col in record) {
        result[col] = record[col];
      }
    }
    return result;
  }

  /**
   * Check if record matches predicate
   */
  private matchesPredicate(
    record: Record<string, unknown>,
    predicate: Expression
  ): boolean {
    return evaluatePredicate(record, predicate);
  }

  /**
   * Check if the URL is available
   */
  async isAvailable(): Promise<boolean> {
    try {
      const response = await fetch(this.uri, {
        method: 'HEAD',
        headers: this.buildAuthHeaders(),
      });
      return response.ok;
    } catch {
      return false;
    }
  }

  /**
   * Close the virtual table
   */
  async close(): Promise<void> {
    // Clear cached schema
    this.cachedSchema = null;
  }
}

// =============================================================================
// PREDICATE EVALUATION
// =============================================================================

/**
 * Evaluate a predicate expression against a record
 */
function evaluatePredicate(
  record: Record<string, unknown>,
  expr: Expression
): boolean {
  switch (expr.type) {
    case 'literal':
      return Boolean((expr as unknown as { value: unknown }).value);

    case 'column': {
      const colExpr = expr as unknown as { name: string; table?: string };
      return Boolean(record[colExpr.name]);
    }

    case 'comparison': {
      const cmp = expr as unknown as {
        operator: string;
        left: Expression;
        right: Expression;
      };
      const leftVal = evaluateValue(record, cmp.left);
      const rightVal = evaluateValue(record, cmp.right);
      return compareValues(cmp.operator, leftVal, rightVal);
    }

    case 'and': {
      const andExpr = expr as unknown as { conditions: Expression[] };
      return andExpr.conditions.every(c => evaluatePredicate(record, c));
    }

    case 'or': {
      const orExpr = expr as unknown as { conditions: Expression[] };
      return orExpr.conditions.some(c => evaluatePredicate(record, c));
    }

    case 'not': {
      const notExpr = expr as unknown as { expression: Expression };
      return !evaluatePredicate(record, notExpr.expression);
    }

    default:
      return true;
  }
}

/**
 * Evaluate an expression to get its value
 */
function evaluateValue(
  record: Record<string, unknown>,
  expr: Expression
): unknown {
  switch (expr.type) {
    case 'literal':
      return (expr as unknown as { value: unknown }).value;
    case 'column': {
      const colExpr = expr as unknown as { name: string };
      return record[colExpr.name];
    }
    default:
      return null;
  }
}

/**
 * Compare two values with an operator
 */
function compareValues(
  operator: string,
  left: unknown,
  right: unknown
): boolean {
  switch (operator) {
    case '=':
    case '==':
      return left === right;
    case '!=':
    case '<>':
      return left !== right;
    case '<':
      return (left as number) < (right as number);
    case '>':
      return (left as number) > (right as number);
    case '<=':
      return (left as number) <= (right as number);
    case '>=':
      return (left as number) >= (right as number);
    case 'LIKE':
    case 'like':
      return matchLike(String(left), String(right));
    case 'IN':
    case 'in':
      return Array.isArray(right) && right.includes(left);
    default:
      return false;
  }
}

/**
 * Match SQL LIKE pattern
 */
function matchLike(value: string, pattern: string): boolean {
  const regexPattern = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    .replace(/%/g, '.*')
    .replace(/_/g, '.');
  const regex = new RegExp(`^${regexPattern}$`, 'i');
  return regex.test(value);
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a URL virtual table
 *
 * @param url - The URL to create a virtual table from
 * @param options - Options for the virtual table
 * @returns A URLVirtualTable instance
 *
 * @example
 * ```typescript
 * const table = createURLVirtualTable('https://api.example.com/users.json');
 * for await (const row of table.scan()) {
 *   console.log(row);
 * }
 * ```
 */
export function createURLVirtualTable(
  url: string,
  options?: URLSourceOptions
): URLVirtualTable {
  // Validate URL scheme
  const parsedUrl = parseVirtualTableUrl(url);

  if (parsedUrl.scheme !== 'http' && parsedUrl.scheme !== 'https') {
    throw new Error(
      `createURLVirtualTable only supports http:// and https:// URLs. ` +
      `For ${parsedUrl.scheme}:// URLs, use the appropriate factory.`
    );
  }

  return new URLVirtualTable(url, options);
}

/**
 * Fetch URL and return all records (convenience function)
 *
 * @param url - The URL to fetch
 * @param options - Options including scan options
 * @returns Array of records
 */
export async function fetchVirtualTable(
  url: string,
  options?: URLSourceOptions & VirtualTableScanOptions
): Promise<Record<string, unknown>[]> {
  const table = createURLVirtualTable(url, options);
  const records: Record<string, unknown>[] = [];

  for await (const record of table.scan(options)) {
    records.push(record);
  }

  await table.close();
  return records;
}
