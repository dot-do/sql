/**
 * HTTP URL Table Source
 *
 * Fetches data from HTTP/HTTPS URLs and provides it as a table source.
 * Supports:
 * - Auto-detection of format from extension and Content-Type
 * - Streaming for large NDJSON responses
 * - Projection and filter pushdown (applied after fetch)
 */

import type {
  TableSource,
  ScanOptions,
  UrlSourceOptions,
  Format,
  Expression,
} from './types.js';
import type { ColumnType } from '../parser.js';
import {
  detectFormat,
  createFormatIterator,
  inferSchema,
} from './parser.js';

// =============================================================================
// URL TABLE SOURCE
// =============================================================================

/**
 * Table source backed by HTTP URL
 */
export class UrlTableSource implements TableSource {
  readonly uri: string;
  private options: UrlSourceOptions;
  private cachedSchema: Record<string, ColumnType> | null = null;

  constructor(url: string, options: UrlSourceOptions = {}) {
    this.uri = url;
    this.options = options;
  }

  /**
   * Scan the URL and return matching records
   */
  async *scan(options: ScanOptions = {}): AsyncIterableIterator<Record<string, unknown>> {
    const { projection, predicate, limit, offset = 0 } = options;

    // Fetch the URL
    const response = await this.fetch();

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText} for ${this.uri}`);
    }

    // Determine format
    const format = this.determineFormat(response);

    // Create iterator based on format
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
   * Get or infer the schema
   */
  async schema(): Promise<Record<string, ColumnType>> {
    if (this.cachedSchema) {
      return this.cachedSchema;
    }

    // If schema provided in options, use it
    if (this.options.schema) {
      this.cachedSchema = this.options.schema;
      return this.cachedSchema;
    }

    // Otherwise, fetch a sample and infer
    const records: Record<string, unknown>[] = [];
    let count = 0;
    const maxSamples = 100;

    for await (const record of this.scan({ limit: maxSamples })) {
      records.push(record);
      count++;
      if (count >= maxSamples) break;
    }

    this.cachedSchema = inferSchema(records);
    return this.cachedSchema;
  }

  /**
   * Close the source (no-op for URL source)
   */
  async close(): Promise<void> {
    // URL sources don't hold persistent resources
  }

  // ==========================================================================
  // PRIVATE METHODS
  // ==========================================================================

  /**
   * Fetch the URL with configured options
   */
  private async fetch(): Promise<Response> {
    const { headers, method = 'GET', body, redirect = 'follow', credentials, timeout } = this.options;

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

      return response;
    } finally {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    }
  }

  /**
   * Determine format from options, extension, or Content-Type
   */
  private determineFormat(response: Response): Format {
    // Explicit format takes precedence
    if (this.options.format) {
      return this.options.format;
    }

    const contentType = response.headers.get('Content-Type') || undefined;
    const result = detectFormat(this.uri, contentType);
    return result.format;
  }

  /**
   * Create format-specific iterator
   */
  private createIterator(
    response: Response,
    format: Format
  ): AsyncIterableIterator<Record<string, unknown>> {
    // For streaming formats (NDJSON), use the body stream
    if (format === 'ndjson' && response.body) {
      return createFormatIterator(format, response.body);
    }

    // For other formats, read the full response
    return (async function* (self: UrlTableSource) {
      const text = await response.text();
      yield* createFormatIterator(format, text);
    })(this);
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
   * Note: This is a simplified implementation
   */
  private matchesPredicate(
    record: Record<string, unknown>,
    predicate: Expression
  ): boolean {
    return evaluatePredicate(record, predicate);
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
  // Convert SQL LIKE pattern to regex
  // % matches any sequence, _ matches single character
  const regexPattern = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, '\\$&') // Escape regex special chars
    .replace(/%/g, '.*')
    .replace(/_/g, '.');
  const regex = new RegExp(`^${regexPattern}$`, 'i');
  return regex.test(value);
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a URL table source
 */
export function createUrlSource(
  url: string,
  options?: UrlSourceOptions
): UrlTableSource {
  // Validate URL
  if (!url.startsWith('http://') && !url.startsWith('https://')) {
    throw new Error(`Invalid URL scheme. Expected http:// or https://, got: ${url}`);
  }

  return new UrlTableSource(url, options);
}

/**
 * Fetch URL and return records directly (convenience function)
 */
export async function fetchUrl(
  url: string,
  options?: UrlSourceOptions & ScanOptions
): Promise<Record<string, unknown>[]> {
  const source = createUrlSource(url, options);
  const records: Record<string, unknown>[] = [];

  for await (const record of source.scan(options)) {
    records.push(record);
  }

  return records;
}
