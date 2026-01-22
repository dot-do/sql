/**
 * Virtual Table Registry
 *
 * Provides a registry for virtual table handlers by URL scheme.
 * Supports:
 * - Built-in schemes: https, http, r2, s3
 * - Custom virtual table registration
 * - WITH clause option parsing
 *
 * @packageDocumentation
 */

import type {
  VirtualTable,
  VirtualTableFactory,
  VirtualTableScheme,
  URLSourceOptions,
  WithClauseOptions,
  SchemeHandler,
  AuthOptions,
} from './types.js';
import { URLVirtualTable, parseVirtualTableUrl } from './url-table.js';
import type { R2Bucket, R2SourceOptions } from '../sources/types.js';
import { R2TableSource, createR2Source } from '../sources/r2.js';

// =============================================================================
// REGISTRY BINDINGS
// =============================================================================

/**
 * Cloudflare resource bindings
 */
export interface VirtualTableBindings {
  /** R2 bucket bindings by name */
  r2?: Record<string, R2Bucket>;

  /** S3 client bindings (future) */
  s3?: Record<string, unknown>;
}

/**
 * Options for the virtual table registry
 */
export interface VirtualTableRegistryOptions {
  /** Cloudflare resource bindings */
  bindings?: VirtualTableBindings;

  /** Default options for HTTP/HTTPS sources */
  httpDefaults?: URLSourceOptions;

  /** Default options for R2 sources */
  r2Defaults?: R2SourceOptions;

  /** Custom scheme handlers */
  customHandlers?: SchemeHandler[];
}

// =============================================================================
// VIRTUAL TABLE REGISTRY
// =============================================================================

/**
 * Registry that manages virtual table handlers by URL scheme
 */
export class VirtualTableRegistry implements VirtualTableFactory {
  private readonly bindings: VirtualTableBindings;
  private readonly httpDefaults: URLSourceOptions;
  private readonly r2Defaults: R2SourceOptions;
  private readonly customHandlers: Map<VirtualTableScheme, SchemeHandler>;

  constructor(options: VirtualTableRegistryOptions = {}) {
    this.bindings = options.bindings || {};
    this.httpDefaults = options.httpDefaults || {};
    this.r2Defaults = options.r2Defaults || {};
    this.customHandlers = new Map();

    // Register custom handlers
    if (options.customHandlers) {
      for (const handler of options.customHandlers) {
        this.customHandlers.set(handler.scheme, handler);
      }
    }
  }

  /**
   * Create a virtual table from a URL
   */
  async create(url: string, options?: URLSourceOptions): Promise<VirtualTable> {
    const parsed = parseVirtualTableUrl(url);
    const scheme = parsed.scheme;

    // Check for custom handler first
    const customHandler = this.customHandlers.get(scheme);
    if (customHandler) {
      return customHandler.create(url, options);
    }

    // Built-in handlers
    switch (scheme) {
      case 'https':
      case 'http':
        return this.createHttpTable(url, options);

      case 'r2':
        return this.createR2Table(url, options);

      case 's3':
        return this.createS3Table(url, options);

      case 'file':
        throw new Error('file:// scheme is not supported in Cloudflare Workers environment');

      default:
        throw new Error(`Unsupported URL scheme: ${scheme}`);
    }
  }

  /**
   * Check if a scheme is supported
   */
  supports(scheme: VirtualTableScheme): boolean {
    // Custom handlers
    if (this.customHandlers.has(scheme)) {
      return true;
    }

    // Built-in handlers
    switch (scheme) {
      case 'https':
      case 'http':
        return true;
      case 'r2':
        return true; // Requires bindings at runtime
      case 's3':
        return false; // Not yet implemented
      case 'file':
        return false; // Not supported in Workers
      default:
        return false;
    }
  }

  /**
   * Create HTTP/HTTPS virtual table
   */
  private createHttpTable(url: string, options?: URLSourceOptions): VirtualTable {
    const mergedOptions = { ...this.httpDefaults, ...options };
    return new URLVirtualTable(url, mergedOptions);
  }

  /**
   * Create R2 virtual table
   */
  private createR2Table(url: string, options?: URLSourceOptions): VirtualTable {
    const parsed = parseVirtualTableUrl(url);

    // Get bucket from options or bindings
    let bucket: R2Bucket | undefined;

    const r2Options = options as R2SourceOptions | undefined;
    if (r2Options?.bucket) {
      bucket = r2Options.bucket;
    } else if (parsed.host && this.bindings.r2) {
      bucket = this.bindings.r2[parsed.host];
    }

    if (!bucket) {
      throw new Error(
        `R2 bucket not found for URL: ${url}. ` +
        `Provide bucket in options or configure bindings.`
      );
    }

    const mergedOptions: R2SourceOptions = { ...this.r2Defaults, ...r2Options, bucket };

    // R2TableSource implements TableSource which is compatible with VirtualTable
    return createR2Source(url, bucket, mergedOptions) as unknown as VirtualTable;
  }

  /**
   * Create S3 virtual table (stub for future implementation)
   */
  private createS3Table(_url: string, _options?: URLSourceOptions): VirtualTable {
    throw new Error(
      'S3 virtual table support not yet implemented. ' +
      'Consider using R2 which provides S3-compatible API.'
    );
  }

  /**
   * Register a custom scheme handler
   */
  registerHandler(handler: SchemeHandler): void {
    this.customHandlers.set(handler.scheme, handler);
  }

  /**
   * Unregister a custom scheme handler
   */
  unregisterHandler(scheme: VirtualTableScheme): boolean {
    return this.customHandlers.delete(scheme);
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

  /**
   * Get list of supported schemes
   */
  getSupportedSchemes(): VirtualTableScheme[] {
    const schemes = new Set<VirtualTableScheme>(['https', 'http', 'r2']);

    for (const scheme of this.customHandlers.keys()) {
      schemes.add(scheme);
    }

    return Array.from(schemes);
  }
}

// =============================================================================
// WITH CLAUSE PARSING
// =============================================================================

/**
 * Parse WITH clause options from SQL
 *
 * Supports syntax like:
 * - WITH (format='csv', headers=true)
 * - WITH (delimiter=';', quote="'")
 * - WITH (auth='bearer', token='...')
 *
 * @param withClause - The WITH clause string (without the WITH keyword)
 * @returns Parsed options
 */
export function parseWithClause(withClause: string): WithClauseOptions {
  const options: WithClauseOptions = {};

  // Remove parentheses if present
  let content = withClause.trim();
  if (content.startsWith('(') && content.endsWith(')')) {
    content = content.slice(1, -1);
  }

  // Parse key=value pairs
  const pairs = splitWithPairs(content);

  for (const pair of pairs) {
    const [key, value] = parseKeyValue(pair);

    switch (key.toLowerCase()) {
      case 'format':
        options.format = parseFormatValue(value);
        break;
      case 'headers':
        options.headers = parseBooleanValue(value);
        break;
      case 'delimiter':
        options.delimiter = parseStringValue(value);
        break;
      case 'quote':
        options.quote = parseStringValue(value);
        break;
      case 'timeout':
        options.timeout = parseInt(value, 10);
        break;
      case 'cache':
        options.cache = parseBooleanValue(value);
        break;
      case 'cachettl':
      case 'cache_ttl':
        options.cacheTtl = parseInt(value, 10);
        break;
      case 'auth':
        options.auth = parseStringValue(value).toLowerCase() as 'basic' | 'bearer' | 'api-key';
        break;
      case 'username':
        options.username = parseStringValue(value);
        break;
      case 'password':
        options.password = parseStringValue(value);
        break;
      case 'token':
        options.token = parseStringValue(value);
        break;
      case 'apikeyheader':
      case 'api_key_header':
        options.apiKeyHeader = parseStringValue(value);
        break;
      case 'apikeyvalue':
      case 'api_key_value':
      case 'apikey':
      case 'api_key':
        options.apiKeyValue = parseStringValue(value);
        break;
    }
  }

  return options;
}

/**
 * Split WITH clause content into key=value pairs
 * Handles quoted values containing commas
 */
function splitWithPairs(content: string): string[] {
  const pairs: string[] = [];
  let current = '';
  let inQuotes = false;
  let quoteChar = '';

  for (let i = 0; i < content.length; i++) {
    const char = content[i];

    if (!inQuotes && (char === "'" || char === '"')) {
      inQuotes = true;
      quoteChar = char;
      current += char;
    } else if (inQuotes && char === quoteChar) {
      inQuotes = false;
      current += char;
    } else if (!inQuotes && char === ',') {
      if (current.trim()) {
        pairs.push(current.trim());
      }
      current = '';
    } else {
      current += char;
    }
  }

  if (current.trim()) {
    pairs.push(current.trim());
  }

  return pairs;
}

/**
 * Parse a key=value pair
 */
function parseKeyValue(pair: string): [string, string] {
  const eqIndex = pair.indexOf('=');
  if (eqIndex === -1) {
    return [pair.trim(), 'true'];
  }

  const key = pair.slice(0, eqIndex).trim();
  const value = pair.slice(eqIndex + 1).trim();

  return [key, value];
}

/**
 * Parse a string value (remove quotes)
 */
function parseStringValue(value: string): string {
  if ((value.startsWith("'") && value.endsWith("'")) ||
      (value.startsWith('"') && value.endsWith('"'))) {
    return value.slice(1, -1);
  }
  return value;
}

/**
 * Parse a boolean value
 */
function parseBooleanValue(value: string): boolean {
  const lower = value.toLowerCase();
  return lower === 'true' || lower === '1' || lower === 'yes';
}

/**
 * Parse a format value
 */
function parseFormatValue(value: string): 'json' | 'ndjson' | 'csv' | 'parquet' | undefined {
  const lower = parseStringValue(value).toLowerCase();
  switch (lower) {
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

/**
 * Convert WithClauseOptions to URLSourceOptions
 */
export function withClauseToOptions(withOptions: WithClauseOptions): URLSourceOptions {
  const options: URLSourceOptions = {};

  if (withOptions.format) {
    options.format = withOptions.format;
  }

  if (withOptions.headers !== undefined) {
    options.csvHeaders = withOptions.headers;
  }

  if (withOptions.delimiter) {
    options.delimiter = withOptions.delimiter;
  }

  if (withOptions.quote) {
    options.quote = withOptions.quote;
  }

  if (withOptions.timeout) {
    options.timeout = withOptions.timeout;
  }

  if (withOptions.cache !== undefined) {
    options.cache = withOptions.cache;
  }

  if (withOptions.cacheTtl) {
    options.cacheTtl = withOptions.cacheTtl;
  }

  // Build auth options
  if (withOptions.auth) {
    const auth: AuthOptions = {};

    switch (withOptions.auth) {
      case 'basic':
        if (withOptions.username && withOptions.password) {
          auth.basic = {
            username: withOptions.username,
            password: withOptions.password,
          };
        }
        break;

      case 'bearer':
        if (withOptions.token) {
          auth.bearer = withOptions.token;
        }
        break;

      case 'api-key':
        if (withOptions.apiKeyHeader && withOptions.apiKeyValue) {
          auth.apiKey = {
            header: withOptions.apiKeyHeader,
            value: withOptions.apiKeyValue,
          };
        }
        break;
    }

    if (Object.keys(auth).length > 0) {
      options.auth = auth;
    }
  }

  return options;
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a virtual table registry
 */
export function createVirtualTableRegistry(
  options?: VirtualTableRegistryOptions
): VirtualTableRegistry {
  return new VirtualTableRegistry(options);
}

/**
 * Create a virtual table from a URL using default registry
 */
export async function createVirtualTable(
  url: string,
  options?: URLSourceOptions & { bindings?: VirtualTableBindings }
): Promise<VirtualTable> {
  const registry = new VirtualTableRegistry({
    bindings: options?.bindings,
  });

  return registry.create(url, options);
}

/**
 * Resolve a FROM clause to a virtual table
 *
 * Handles:
 * - Quoted URL literals: 'https://...' or "https://..."
 * - url() function: url('https://...', 'JSON')
 * - WITH clause: ... WITH (format='csv', headers=true)
 *
 * @param fromClause - The FROM clause value
 * @param registry - Virtual table registry
 * @param withClause - Optional WITH clause options
 * @returns Virtual table or null if not a URL source
 */
export async function resolveVirtualTableFromClause(
  fromClause: string,
  registry: VirtualTableRegistry,
  withClause?: string
): Promise<VirtualTable | null> {
  // Parse WITH clause options
  let withOptions: URLSourceOptions = {};
  if (withClause) {
    withOptions = withClauseToOptions(parseWithClause(withClause));
  }

  // Check for url() function
  const urlFuncMatch = fromClause
    .trim()
    .match(/^url\s*\(\s*(['"])([^'"]+)\1(?:\s*,\s*(['"])([^'"]+)\3)?\s*\)$/i);

  if (urlFuncMatch) {
    const url = urlFuncMatch[2];
    const format = urlFuncMatch[4]?.toLowerCase() as 'json' | 'ndjson' | 'csv' | 'parquet' | undefined;

    const options: URLSourceOptions = {
      ...withOptions,
      ...(format && { format }),
    };

    return registry.create(url, options);
  }

  // Check for quoted URL literal
  const trimmed = fromClause.trim();

  // Single quoted
  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    const url = trimmed.slice(1, -1);
    if (isValidUrlScheme(url)) {
      return registry.create(url, withOptions);
    }
  }

  // Double quoted
  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    const url = trimmed.slice(1, -1);
    if (isValidUrlScheme(url)) {
      return registry.create(url, withOptions);
    }
  }

  // Not a URL source
  return null;
}

/**
 * Check if a string starts with a valid URL scheme
 */
function isValidUrlScheme(str: string): boolean {
  return (
    str.startsWith('https://') ||
    str.startsWith('http://') ||
    str.startsWith('r2://') ||
    str.startsWith('s3://') ||
    str.startsWith('file://')
  );
}
