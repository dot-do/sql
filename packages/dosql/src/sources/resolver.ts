/**
 * URI Scheme Resolver
 *
 * Resolves URI strings to appropriate TableSource implementations.
 * Supports:
 * - https:// and http:// - HTTP URL sources
 * - r2:// - Cloudflare R2 storage
 * - kv:// - Cloudflare KV (future)
 * - d1:// - Cloudflare D1 (future)
 */

import type {
  TableSource,
  SourceOptions,
  UriScheme,
  ParsedUri,
  R2Bucket,
  R2SourceOptions,
  UrlSourceOptions,
} from './types.js';
import { createUrlSource, UrlTableSource } from './url.js';
import { createR2Source, R2TableSource } from './r2.js';

// =============================================================================
// URI PARSING
// =============================================================================

/**
 * Parse a URI string into components
 */
export function parseUri(uri: string): ParsedUri {
  // Handle r2:// scheme specially (no standard URL parser support)
  if (uri.startsWith('r2://')) {
    const match = uri.match(/^r2:\/\/([^/]+)\/(.*)$/);
    if (!match) {
      throw new Error(`Invalid r2:// URI: ${uri}`);
    }
    return {
      scheme: 'r2',
      host: match[1],
      path: '/' + match[2],
    };
  }

  // Handle kv:// scheme
  if (uri.startsWith('kv://')) {
    const match = uri.match(/^kv:\/\/([^/]+)\/(.*)$/);
    if (!match) {
      throw new Error(`Invalid kv:// URI: ${uri}`);
    }
    return {
      scheme: 'kv',
      host: match[1],
      path: '/' + match[2],
    };
  }

  // Handle d1:// scheme
  if (uri.startsWith('d1://')) {
    const match = uri.match(/^d1:\/\/([^/]+)\/(.*)$/);
    if (!match) {
      throw new Error(`Invalid d1:// URI: ${uri}`);
    }
    return {
      scheme: 'd1',
      host: match[1],
      path: '/' + match[2],
    };
  }

  // Use standard URL parser for http/https
  try {
    const url = new URL(uri);
    const scheme = url.protocol.slice(0, -1) as UriScheme; // Remove trailing ':'

    if (scheme !== 'http' && scheme !== 'https') {
      throw new Error(`Unsupported URI scheme: ${scheme}`);
    }

    // Parse query string
    const query: Record<string, string> = {};
    url.searchParams.forEach((value, key) => {
      query[key] = value;
    });

    return {
      scheme,
      host: url.hostname,
      port: url.port ? parseInt(url.port, 10) : undefined,
      path: url.pathname,
      query: Object.keys(query).length > 0 ? query : undefined,
      fragment: url.hash ? url.hash.slice(1) : undefined,
    };
  } catch (e) {
    throw new Error(`Invalid URI: ${uri}`);
  }
}

/**
 * Get the scheme from a URI without full parsing
 */
export function getUriScheme(uri: string): UriScheme {
  if (uri.startsWith('https://')) return 'https';
  if (uri.startsWith('http://')) return 'http';
  if (uri.startsWith('r2://')) return 'r2';
  if (uri.startsWith('kv://')) return 'kv';
  if (uri.startsWith('d1://')) return 'd1';
  throw new Error(`Unknown URI scheme: ${uri}`);
}

// =============================================================================
// RESOLVER OPTIONS
// =============================================================================

/**
 * Bindings for Cloudflare resources
 */
export interface Bindings {
  /** R2 bucket bindings by name */
  r2?: Record<string, R2Bucket>;

  /** KV namespace bindings by name (future) */
  kv?: Record<string, unknown>;

  /** D1 database bindings by name (future) */
  d1?: Record<string, unknown>;
}

/**
 * Options for the resolver
 */
export interface ResolverOptions {
  /** Cloudflare resource bindings */
  bindings?: Bindings;

  /** Default options for URL sources */
  urlDefaults?: UrlSourceOptions;

  /** Default options for R2 sources */
  r2Defaults?: R2SourceOptions;
}

// =============================================================================
// TABLE SOURCE RESOLVER
// =============================================================================

/**
 * Resolver that creates TableSource instances from URIs
 */
export class TableSourceResolver {
  private bindings: Bindings;
  private urlDefaults: UrlSourceOptions;
  private r2Defaults: R2SourceOptions;

  constructor(options: ResolverOptions = {}) {
    this.bindings = options.bindings || {};
    this.urlDefaults = options.urlDefaults || {};
    this.r2Defaults = options.r2Defaults || {};
  }

  /**
   * Resolve a URI to a TableSource
   */
  async resolve(uri: string, options?: SourceOptions): Promise<TableSource> {
    const scheme = getUriScheme(uri);

    switch (scheme) {
      case 'https':
      case 'http':
        return this.resolveUrl(uri, options as UrlSourceOptions);

      case 'r2':
        return this.resolveR2(uri, options as R2SourceOptions);

      case 'kv':
        return this.resolveKv(uri, options);

      case 'd1':
        return this.resolveD1(uri, options);

      default:
        throw new Error(`Unsupported URI scheme: ${scheme}`);
    }
  }

  /**
   * Resolve HTTP/HTTPS URL
   */
  private resolveUrl(uri: string, options?: UrlSourceOptions): UrlTableSource {
    const mergedOptions = { ...this.urlDefaults, ...options };
    return createUrlSource(uri, mergedOptions);
  }

  /**
   * Resolve R2 URI
   */
  private resolveR2(uri: string, options?: R2SourceOptions): R2TableSource {
    const parsed = parseUri(uri);

    // Get bucket from options or bindings
    let bucket = options?.bucket;

    if (!bucket && parsed.host && this.bindings.r2) {
      bucket = this.bindings.r2[parsed.host];
    }

    if (!bucket) {
      throw new Error(
        `R2 bucket not found for URI: ${uri}. ` +
        `Provide bucket in options or configure bindings.`
      );
    }

    const mergedOptions = { ...this.r2Defaults, ...options, bucket };
    return createR2Source(uri, bucket, mergedOptions);
  }

  /**
   * Resolve KV URI (stub for future implementation)
   */
  private resolveKv(_uri: string, _options?: SourceOptions): TableSource {
    throw new Error('KV table source not yet implemented');
  }

  /**
   * Resolve D1 URI (stub for future implementation)
   */
  private resolveD1(_uri: string, _options?: SourceOptions): TableSource {
    throw new Error('D1 table source not yet implemented');
  }

  /**
   * Check if a URI scheme is supported
   */
  supports(scheme: UriScheme): boolean {
    switch (scheme) {
      case 'https':
      case 'http':
      case 'r2':
        return true;
      case 'kv':
      case 'd1':
        return false; // Not yet implemented
      default:
        return false;
    }
  }

  /**
   * Add R2 bucket binding
   */
  addR2Bucket(name: string, bucket: R2Bucket): void {
    if (!this.bindings.r2) {
      this.bindings.r2 = {};
    }
    this.bindings.r2[name] = bucket;
  }
}

// =============================================================================
// CONVENIENCE FUNCTIONS
// =============================================================================

/**
 * Create a resolver with the given options
 */
export function createResolver(options?: ResolverOptions): TableSourceResolver {
  return new TableSourceResolver(options);
}

/**
 * Resolve a URI to a TableSource using default resolver
 */
export async function resolveSource(
  uri: string,
  options?: SourceOptions & ResolverOptions
): Promise<TableSource> {
  const resolver = new TableSourceResolver({
    bindings: options?.bindings,
    urlDefaults: options?.urlDefaults,
    r2Defaults: options?.r2Defaults,
  });

  return resolver.resolve(uri, options);
}

// =============================================================================
// SQL URL LITERAL PARSING
// =============================================================================

/**
 * Check if a SQL FROM clause value is a URL literal
 * URL literals are quoted strings starting with a scheme
 */
export function isUrlLiteral(value: string): boolean {
  const trimmed = value.trim();

  // Check for single quotes
  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    const inner = trimmed.slice(1, -1);
    return isValidUrl(inner);
  }

  // Check for double quotes
  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    const inner = trimmed.slice(1, -1);
    return isValidUrl(inner);
  }

  return false;
}

/**
 * Check if a string is a valid URL
 */
function isValidUrl(str: string): boolean {
  return (
    str.startsWith('https://') ||
    str.startsWith('http://') ||
    str.startsWith('r2://') ||
    str.startsWith('kv://') ||
    str.startsWith('d1://')
  );
}

/**
 * Extract URL from a quoted literal
 */
export function extractUrlFromLiteral(literal: string): string {
  const trimmed = literal.trim();

  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    return trimmed.slice(1, -1);
  }

  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    return trimmed.slice(1, -1);
  }

  throw new Error(`Not a quoted literal: ${literal}`);
}

/**
 * Parse url() function call from SQL
 * Format: url('https://...', 'FORMAT') or url('https://...')
 */
export function parseUrlFunction(
  expr: string
): { url: string; format?: string } | null {
  const match = expr
    .trim()
    .match(/^url\s*\(\s*(['"])([^'"]+)\1(?:\s*,\s*(['"])([^'"]+)\3)?\s*\)$/i);

  if (!match) {
    return null;
  }

  return {
    url: match[2],
    format: match[4]?.toLowerCase(),
  };
}

/**
 * Resolve a SQL FROM clause value to a TableSource
 * Handles both URL literals and url() function calls
 */
export async function resolveFromClause(
  fromValue: string,
  resolver: TableSourceResolver
): Promise<TableSource | null> {
  // Check for url() function
  const urlFunc = parseUrlFunction(fromValue);
  if (urlFunc) {
    return resolver.resolve(urlFunc.url, {
      format: urlFunc.format as 'json' | 'ndjson' | 'csv' | 'parquet' | undefined,
    });
  }

  // Check for URL literal
  if (isUrlLiteral(fromValue)) {
    const url = extractUrlFromLiteral(fromValue);
    return resolver.resolve(url);
  }

  // Not a URL source
  return null;
}
