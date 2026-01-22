/**
 * R2 Table Source
 *
 * Reads data from Cloudflare R2 storage buckets.
 * Supports:
 * - Range reads for efficient partial access
 * - Parquet row group filtering (when metadata is available)
 * - Auto-detection of format from extension
 */

import type {
  TableSource,
  ScanOptions,
  R2SourceOptions,
  R2Bucket,
  Format,
  Expression,
} from './types.js';
import type { ColumnType } from '../parser.js';
import {
  detectFormat,
  createFormatIterator,
  inferSchema,
  isParquet,
} from './parser.js';

// =============================================================================
// R2 TABLE SOURCE
// =============================================================================

/**
 * Table source backed by R2 object storage
 */
export class R2TableSource implements TableSource {
  readonly uri: string;
  private bucket: R2Bucket;
  private key: string;
  private options: R2SourceOptions;
  private cachedSchema: Record<string, ColumnType> | null = null;
  private cachedObject: {
    body: ArrayBuffer;
    contentType?: string;
  } | null = null;

  constructor(uri: string, bucket: R2Bucket, options: R2SourceOptions = {}) {
    this.uri = uri;
    this.bucket = bucket;
    this.options = options;

    // Parse r2://bucket/path format
    const match = uri.match(/^r2:\/\/([^/]+)\/(.+)$/);
    if (!match) {
      throw new Error(`Invalid R2 URI format: ${uri}. Expected r2://bucket/path`);
    }
    this.key = match[2];
  }

  /**
   * Scan the R2 object and return matching records
   */
  async *scan(options: ScanOptions = {}): AsyncIterableIterator<Record<string, unknown>> {
    const { projection, predicate, limit, offset = 0 } = options;

    // Get the object content
    const { body, contentType } = await this.getObjectContent();

    // Determine format
    const format = this.determineFormat(contentType, body);

    // For Parquet with range reads enabled, we could optimize here
    // For now, read full content and filter in memory
    if (format === 'parquet' && this.options.rangeReads && this.options.rowGroups) {
      // Future: implement row group filtering with range reads
      // This would require parsing Parquet footer to get row group offsets
    }

    // Create iterator based on format
    const iterator = createFormatIterator(format, body);

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
   * Close the source
   */
  async close(): Promise<void> {
    this.cachedObject = null;
    this.cachedSchema = null;
  }

  // ==========================================================================
  // PRIVATE METHODS
  // ==========================================================================

  /**
   * Get the object content from R2
   */
  private async getObjectContent(): Promise<{
    body: ArrayBuffer;
    contentType?: string;
  }> {
    if (this.cachedObject && this.options.cache !== false) {
      return this.cachedObject;
    }

    const object = await this.bucket.get(this.key);

    if (!object) {
      throw new Error(`R2 object not found: ${this.key}`);
    }

    const body = await object.arrayBuffer();
    const contentType = object.httpMetadata?.contentType;

    this.cachedObject = { body, contentType };
    return this.cachedObject;
  }

  /**
   * Read a range of bytes from the object
   */
  private async getObjectRange(
    offset: number,
    length: number
  ): Promise<ArrayBuffer> {
    const object = await this.bucket.get(this.key, {
      range: { offset, length },
    });

    if (!object) {
      throw new Error(`R2 object not found: ${this.key}`);
    }

    return object.arrayBuffer();
  }

  /**
   * Get object metadata (HEAD request)
   */
  private async getObjectMetadata(): Promise<{
    size: number;
    contentType?: string;
  }> {
    const head = await this.bucket.head(this.key);

    if (!head) {
      throw new Error(`R2 object not found: ${this.key}`);
    }

    return {
      size: head.size,
      contentType: head.httpMetadata?.contentType,
    };
  }

  /**
   * Determine format from options, extension, or content
   */
  private determineFormat(contentType?: string, content?: ArrayBuffer): Format {
    // Explicit format takes precedence
    if (this.options.format) {
      return this.options.format;
    }

    // Check for Parquet magic bytes
    if (content && isParquet(content)) {
      return 'parquet';
    }

    // Use extension/content-type detection
    const result = detectFormat(this.key, contentType);
    return result.format;
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
}

// =============================================================================
// PREDICATE EVALUATION (shared with url.ts, could be extracted)
// =============================================================================

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

function matchLike(value: string, pattern: string): boolean {
  const regexPattern = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    .replace(/%/g, '.*')
    .replace(/_/g, '.');
  const regex = new RegExp(`^${regexPattern}$`, 'i');
  return regex.test(value);
}

// =============================================================================
// PARQUET RANGE READ HELPERS (for future optimization)
// =============================================================================

/**
 * Parquet file footer is at the end of the file
 * Footer size is stored in last 4 bytes before PAR1 magic
 */
export async function readParquetFooterSize(
  bucket: R2Bucket,
  key: string,
  fileSize: number
): Promise<number> {
  // Read last 8 bytes: 4 bytes footer length + 4 bytes PAR1 magic
  const tailBytes = await bucket.get(key, {
    range: { offset: fileSize - 8, length: 8 },
  });

  if (!tailBytes) {
    throw new Error('Failed to read Parquet footer');
  }

  const buffer = await tailBytes.arrayBuffer();
  const view = new DataView(buffer);

  // Verify magic bytes
  const magic = String.fromCharCode(
    view.getUint8(4),
    view.getUint8(5),
    view.getUint8(6),
    view.getUint8(7)
  );

  if (magic !== 'PAR1') {
    throw new Error('Invalid Parquet file: missing PAR1 magic at end');
  }

  // Footer length is little-endian uint32
  return view.getUint32(0, true);
}

/**
 * Read Parquet footer metadata
 * Returns raw bytes - actual parsing requires Thrift deserialization
 */
export async function readParquetFooter(
  bucket: R2Bucket,
  key: string,
  fileSize: number,
  footerSize: number
): Promise<ArrayBuffer> {
  // Footer is right before the last 8 bytes
  const footerOffset = fileSize - 8 - footerSize;

  const footer = await bucket.get(key, {
    range: { offset: footerOffset, length: footerSize },
  });

  if (!footer) {
    throw new Error('Failed to read Parquet footer');
  }

  return footer.arrayBuffer();
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create an R2 table source
 */
export function createR2Source(
  uri: string,
  bucket: R2Bucket,
  options?: R2SourceOptions
): R2TableSource {
  return new R2TableSource(uri, bucket, options);
}

/**
 * Parse R2 URI into bucket name and key
 */
export function parseR2Uri(uri: string): { bucketName: string; key: string } {
  const match = uri.match(/^r2:\/\/([^/]+)\/(.+)$/);
  if (!match) {
    throw new Error(`Invalid R2 URI format: ${uri}. Expected r2://bucket/path`);
  }
  return {
    bucketName: match[1],
    key: match[2],
  };
}

/**
 * List objects in R2 bucket matching a prefix
 */
export async function listR2Objects(
  bucket: R2Bucket,
  prefix?: string
): Promise<string[]> {
  const result = await bucket.list({ prefix });
  return result.objects.map(obj => obj.key);
}
