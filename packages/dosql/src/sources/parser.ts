/**
 * Format Parsers for URL Table Sources
 *
 * Provides parsers for different data formats:
 * - JSON: Parse JSON arrays
 * - NDJSON: Stream newline-delimited JSON
 * - CSV: Parse CSV with simple implementation (no external library)
 * - Parquet: Stub for future integration
 */

import type { Format, FormatDetectionResult } from './types.js';
import type { ColumnType } from '../parser.js';

// =============================================================================
// FORMAT DETECTION
// =============================================================================

/**
 * Detect format from file extension
 */
export function detectFormatFromExtension(path: string): FormatDetectionResult | null {
  const ext = path.split('.').pop()?.toLowerCase();

  switch (ext) {
    case 'json':
      return { format: 'json', confidence: 'high', source: 'extension' };
    case 'ndjson':
    case 'jsonl':
      return { format: 'ndjson', confidence: 'high', source: 'extension' };
    case 'csv':
      return { format: 'csv', confidence: 'high', source: 'extension' };
    case 'parquet':
      return { format: 'parquet', confidence: 'high', source: 'extension' };
    default:
      return null;
  }
}

/**
 * Detect format from Content-Type header
 */
export function detectFormatFromContentType(contentType: string): FormatDetectionResult | null {
  const type = contentType.split(';')[0].trim().toLowerCase();

  switch (type) {
    case 'application/json':
      return { format: 'json', confidence: 'high', source: 'content-type' };
    case 'application/x-ndjson':
    case 'application/x-jsonlines':
    case 'application/jsonl':
      return { format: 'ndjson', confidence: 'high', source: 'content-type' };
    case 'text/csv':
    case 'application/csv':
      return { format: 'csv', confidence: 'high', source: 'content-type' };
    case 'application/vnd.apache.parquet':
    case 'application/x-parquet':
      return { format: 'parquet', confidence: 'high', source: 'content-type' };
    case 'text/plain':
      // Could be CSV or NDJSON - low confidence
      return { format: 'csv', confidence: 'low', source: 'content-type' };
    default:
      return null;
  }
}

/**
 * Detect format from content preview
 */
export function detectFormatFromContent(preview: string): FormatDetectionResult | null {
  const trimmed = preview.trim();

  // JSON array starts with [
  if (trimmed.startsWith('[')) {
    return { format: 'json', confidence: 'medium', source: 'content' };
  }

  // JSON object starts with { - could be single object or NDJSON
  if (trimmed.startsWith('{')) {
    // Check if there are multiple lines with { at the start
    const lines = trimmed.split('\n').filter(l => l.trim());
    if (lines.length > 1 && lines.every(l => l.trim().startsWith('{'))) {
      return { format: 'ndjson', confidence: 'medium', source: 'content' };
    }
    return { format: 'json', confidence: 'low', source: 'content' };
  }

  // Parquet magic bytes: PAR1
  if (trimmed.startsWith('PAR1')) {
    return { format: 'parquet', confidence: 'high', source: 'content' };
  }

  // Assume CSV for comma-separated text
  if (trimmed.includes(',') && !trimmed.startsWith('{') && !trimmed.startsWith('[')) {
    return { format: 'csv', confidence: 'low', source: 'content' };
  }

  return null;
}

/**
 * Detect format using all available methods
 */
export function detectFormat(
  path: string,
  contentType?: string,
  contentPreview?: string
): FormatDetectionResult {
  // 1. Try extension first (highest priority)
  const fromExtension = detectFormatFromExtension(path);
  if (fromExtension?.confidence === 'high') {
    return fromExtension;
  }

  // 2. Try Content-Type
  if (contentType) {
    const fromContentType = detectFormatFromContentType(contentType);
    if (fromContentType?.confidence === 'high') {
      return fromContentType;
    }
  }

  // 3. Try content preview
  if (contentPreview) {
    const fromContent = detectFormatFromContent(contentPreview);
    if (fromContent) {
      return fromContent;
    }
  }

  // 4. Return lower confidence results
  if (fromExtension) return fromExtension;

  // 5. Default to JSON
  return { format: 'json', confidence: 'low', source: 'default' };
}

// =============================================================================
// JSON PARSER
// =============================================================================

/**
 * Parse JSON array from string
 */
export function parseJsonArray(content: string): Record<string, unknown>[] {
  const parsed = JSON.parse(content);

  if (!Array.isArray(parsed)) {
    // If it's a single object, wrap in array
    if (typeof parsed === 'object' && parsed !== null) {
      return [parsed as Record<string, unknown>];
    }
    throw new Error('Expected JSON array or object');
  }

  return parsed as Record<string, unknown>[];
}

/**
 * Create async iterator from JSON array
 */
export async function* parseJsonArrayIterator(
  content: string | Promise<string>
): AsyncIterableIterator<Record<string, unknown>> {
  const resolvedContent = await content;
  const records = parseJsonArray(resolvedContent);
  for (const record of records) {
    yield record;
  }
}

// =============================================================================
// NDJSON PARSER (Streaming)
// =============================================================================

/**
 * Parse single NDJSON line
 */
export function parseNdjsonLine(line: string): Record<string, unknown> | null {
  const trimmed = line.trim();
  if (!trimmed) return null;

  try {
    const parsed = JSON.parse(trimmed);
    if (typeof parsed === 'object' && parsed !== null) {
      return parsed as Record<string, unknown>;
    }
    return null;
  } catch {
    return null;
  }
}

/**
 * Parse NDJSON string
 */
export function parseNdjson(content: string): Record<string, unknown>[] {
  const lines = content.split('\n');
  const records: Record<string, unknown>[] = [];

  for (const line of lines) {
    const record = parseNdjsonLine(line);
    if (record) {
      records.push(record);
    }
  }

  return records;
}

/**
 * Create async iterator from NDJSON stream
 */
export async function* parseNdjsonStream(
  stream: ReadableStream<Uint8Array>
): AsyncIterableIterator<Record<string, unknown>> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  try {
    while (true) {
      const { done, value } = await reader.read();

      if (done) {
        // Process remaining buffer
        if (buffer.trim()) {
          const record = parseNdjsonLine(buffer);
          if (record) yield record;
        }
        break;
      }

      buffer += decoder.decode(value, { stream: true });

      // Process complete lines
      const lines = buffer.split('\n');
      buffer = lines.pop() || ''; // Keep incomplete line in buffer

      for (const line of lines) {
        const record = parseNdjsonLine(line);
        if (record) yield record;
      }
    }
  } finally {
    reader.releaseLock();
  }
}

/**
 * Create async iterator from NDJSON string
 */
export async function* parseNdjsonIterator(
  content: string | Promise<string>
): AsyncIterableIterator<Record<string, unknown>> {
  const resolvedContent = await content;
  const records = parseNdjson(resolvedContent);
  for (const record of records) {
    yield record;
  }
}

// =============================================================================
// CSV PARSER (Simple implementation without external libraries)
// =============================================================================

/**
 * CSV parse options
 */
export interface CsvParseOptions {
  /** Field delimiter (default: ,) */
  delimiter?: string;
  /** Quote character (default: ") */
  quote?: string;
  /** Whether first row is header (default: true) */
  header?: boolean;
  /** Custom column names (used if header is false) */
  columns?: string[];
  /** Skip empty lines (default: true) */
  skipEmpty?: boolean;
}

/**
 * Parse a single CSV field, handling quotes
 */
function parseCsvField(field: string, quote: string): string {
  const trimmed = field.trim();

  // If field is quoted, remove quotes and unescape
  if (trimmed.startsWith(quote) && trimmed.endsWith(quote)) {
    const inner = trimmed.slice(1, -1);
    // Unescape doubled quotes
    return inner.replace(new RegExp(`${quote}${quote}`, 'g'), quote);
  }

  return trimmed;
}

/**
 * Parse a CSV line into fields, handling quoted fields with delimiters
 */
export function parseCsvLine(
  line: string,
  delimiter: string = ',',
  quote: string = '"'
): string[] {
  const fields: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const nextChar = line[i + 1];

    if (!inQuotes) {
      if (char === delimiter) {
        fields.push(parseCsvField(current, quote));
        current = '';
      } else if (char === quote) {
        inQuotes = true;
        current += char;
      } else {
        current += char;
      }
    } else {
      // Inside quotes
      if (char === quote) {
        if (nextChar === quote) {
          // Escaped quote
          current += char + nextChar;
          i++; // Skip next quote
        } else {
          // End of quoted field
          inQuotes = false;
          current += char;
        }
      } else {
        current += char;
      }
    }
  }

  // Add last field
  fields.push(parseCsvField(current, quote));

  return fields;
}

/**
 * Parse CSV string into records
 */
export function parseCsv(
  content: string,
  options: CsvParseOptions = {}
): Record<string, unknown>[] {
  const {
    delimiter = ',',
    quote = '"',
    header = true,
    columns,
    skipEmpty = true,
  } = options;

  const lines = content.split(/\r?\n/);
  const records: Record<string, unknown>[] = [];

  let columnNames: string[];
  let startLine = 0;

  if (header && lines.length > 0) {
    columnNames = parseCsvLine(lines[0], delimiter, quote);
    startLine = 1;
  } else if (columns) {
    columnNames = columns;
  } else {
    // Auto-generate column names from first row field count
    const firstLine = lines.find(l => l.trim());
    if (!firstLine) return [];
    const fieldCount = parseCsvLine(firstLine, delimiter, quote).length;
    columnNames = Array.from({ length: fieldCount }, (_, i) => `col${i + 1}`);
  }

  for (let i = startLine; i < lines.length; i++) {
    const line = lines[i];
    if (skipEmpty && !line.trim()) continue;

    const fields = parseCsvLine(line, delimiter, quote);
    const record: Record<string, unknown> = {};

    for (let j = 0; j < columnNames.length; j++) {
      const value = fields[j] ?? '';
      // Try to parse numbers
      const numValue = parseFloat(value);
      if (!isNaN(numValue) && value.trim() !== '') {
        record[columnNames[j]] = numValue;
      } else if (value.toLowerCase() === 'true') {
        record[columnNames[j]] = true;
      } else if (value.toLowerCase() === 'false') {
        record[columnNames[j]] = false;
      } else if (value === '' || value.toLowerCase() === 'null') {
        record[columnNames[j]] = null;
      } else {
        record[columnNames[j]] = value;
      }
    }

    records.push(record);
  }

  return records;
}

/**
 * Create async iterator from CSV string
 */
export async function* parseCsvIterator(
  content: string | Promise<string>,
  options?: CsvParseOptions
): AsyncIterableIterator<Record<string, unknown>> {
  const resolvedContent = await content;
  const records = parseCsv(resolvedContent, options);
  for (const record of records) {
    yield record;
  }
}

/**
 * Create async iterator from CSV stream
 */
export async function* parseCsvStream(
  stream: ReadableStream<Uint8Array>,
  options: CsvParseOptions = {}
): AsyncIterableIterator<Record<string, unknown>> {
  const {
    delimiter = ',',
    quote = '"',
    header = true,
    columns,
    skipEmpty = true,
  } = options;

  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let columnNames: string[] | null = columns || null;
  let isFirstRow = header;

  try {
    while (true) {
      const { done, value } = await reader.read();

      if (done) {
        // Process remaining buffer
        if (buffer.trim()) {
          if (isFirstRow && !columnNames) {
            columnNames = parseCsvLine(buffer, delimiter, quote);
          } else if (columnNames) {
            const fields = parseCsvLine(buffer, delimiter, quote);
            const record: Record<string, unknown> = {};
            for (let j = 0; j < columnNames.length; j++) {
              record[columnNames[j]] = fields[j] ?? '';
            }
            yield record;
          }
        }
        break;
      }

      buffer += decoder.decode(value, { stream: true });

      // Process complete lines
      const lines = buffer.split(/\r?\n/);
      buffer = lines.pop() || '';

      for (const line of lines) {
        if (skipEmpty && !line.trim()) continue;

        if (isFirstRow && !columnNames) {
          columnNames = parseCsvLine(line, delimiter, quote);
          isFirstRow = false;
        } else if (columnNames) {
          const fields = parseCsvLine(line, delimiter, quote);
          const record: Record<string, unknown> = {};
          for (let j = 0; j < columnNames.length; j++) {
            const value = fields[j] ?? '';
            const numValue = parseFloat(value);
            if (!isNaN(numValue) && value.trim() !== '') {
              record[columnNames[j]] = numValue;
            } else {
              record[columnNames[j]] = value;
            }
          }
          yield record;
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}

// =============================================================================
// PARQUET PARSER (Stub - complex format, integrate later)
// =============================================================================

/**
 * Parquet is a complex binary format that requires specialized libraries.
 * This stub provides the interface but throws for now.
 */
export async function* parseParquetIterator(
  _content: ArrayBuffer
): AsyncIterableIterator<Record<string, unknown>> {
  throw new Error(
    'Parquet parsing not yet implemented. ' +
    'Consider using @duckdb/duckdb-wasm or parquet-wasm for Parquet support.'
  );
}

/**
 * Check if content is Parquet (magic bytes)
 */
export function isParquet(content: ArrayBuffer): boolean {
  const view = new DataView(content);
  // PAR1 magic bytes at start
  if (content.byteLength < 4) return false;
  const magic = String.fromCharCode(
    view.getUint8(0),
    view.getUint8(1),
    view.getUint8(2),
    view.getUint8(3)
  );
  return magic === 'PAR1';
}

// =============================================================================
// SCHEMA INFERENCE
// =============================================================================

/**
 * Infer column type from a value
 */
function inferTypeFromValue(value: unknown): ColumnType {
  if (value === null || value === undefined) return 'null';
  if (typeof value === 'string') return 'string';
  if (typeof value === 'number') return 'number';
  if (typeof value === 'boolean') return 'boolean';
  if (value instanceof Date) return 'Date';
  return 'unknown';
}

/**
 * Merge two column types (for schema inference)
 * Returns the more general type
 */
function mergeTypes(type1: ColumnType, type2: ColumnType): ColumnType {
  if (type1 === type2) return type1;
  if (type1 === 'null') return type2;
  if (type2 === 'null') return type1;
  if (type1 === 'unknown' || type2 === 'unknown') return 'unknown';
  // Different non-null types - could be string (most general)
  return 'string';
}

/**
 * Infer schema from a sample of records
 */
export function inferSchema(
  records: Record<string, unknown>[],
  maxSamples: number = 100
): Record<string, ColumnType> {
  const schema: Record<string, ColumnType> = {};
  const sampled = records.slice(0, maxSamples);

  for (const record of sampled) {
    for (const [key, value] of Object.entries(record)) {
      const inferredType = inferTypeFromValue(value);
      if (key in schema) {
        schema[key] = mergeTypes(schema[key], inferredType);
      } else {
        schema[key] = inferredType;
      }
    }
  }

  return schema;
}

/**
 * Infer schema from CSV header row (all columns as string initially)
 */
export function inferSchemaFromCsvHeader(
  headerRow: string,
  options?: CsvParseOptions
): Record<string, ColumnType> {
  const columns = parseCsvLine(headerRow, options?.delimiter, options?.quote);
  const schema: Record<string, ColumnType> = {};

  for (const col of columns) {
    schema[col] = 'string'; // CSV columns start as string, refined by data
  }

  return schema;
}

// =============================================================================
// UNIFIED PARSER
// =============================================================================

/**
 * Create async iterator for any format
 */
export function createFormatIterator(
  format: Format,
  content: string | ArrayBuffer | ReadableStream<Uint8Array>,
  options?: CsvParseOptions
): AsyncIterableIterator<Record<string, unknown>> {
  switch (format) {
    case 'json':
      if (content instanceof ReadableStream) {
        // Read stream to string first (JSON requires full content)
        return (async function* () {
          const reader = content.getReader();
          const decoder = new TextDecoder();
          let text = '';
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            text += decoder.decode(value, { stream: true });
          }
          text += decoder.decode(); // Flush remaining
          yield* parseJsonArrayIterator(text);
        })();
      }
      if (content instanceof ArrayBuffer) {
        return parseJsonArrayIterator(new TextDecoder().decode(content));
      }
      return parseJsonArrayIterator(content);

    case 'ndjson':
      if (content instanceof ReadableStream) {
        return parseNdjsonStream(content);
      }
      if (content instanceof ArrayBuffer) {
        return parseNdjsonIterator(new TextDecoder().decode(content));
      }
      return parseNdjsonIterator(content);

    case 'csv':
      if (content instanceof ReadableStream) {
        return parseCsvStream(content, options);
      }
      if (content instanceof ArrayBuffer) {
        return parseCsvIterator(new TextDecoder().decode(content), options);
      }
      return parseCsvIterator(content, options);

    case 'parquet':
      if (content instanceof ArrayBuffer) {
        return parseParquetIterator(content);
      }
      throw new Error('Parquet requires ArrayBuffer content');

    default:
      throw new Error(`Unsupported format: ${format}`);
  }
}
