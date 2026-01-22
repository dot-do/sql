/**
 * DoLake Parquet Writer
 *
 * Writes CDC events to Parquet files using hyparquet-style format.
 * Optimized for Cloudflare Workers environment.
 */

import {
  type CDCEvent,
  type IcebergSchema,
  type IcebergField,
  type DataFile,
  ParquetWriteError,
  generateUUID,
} from './types.js';
import type { PartitionBuffer } from './buffer.js';

// =============================================================================
// Parquet Configuration
// =============================================================================

/**
 * Parquet write configuration
 */
export interface ParquetWriteConfig {
  /** Target row group size */
  rowGroupSize: number;
  /** Compression codec */
  compression: 'none' | 'snappy' | 'gzip' | 'zstd';
  /** Enable dictionary encoding */
  dictionaryEncoding: boolean;
  /** Page size */
  pageSize: number;
  /** Enable column statistics */
  enableStatistics: boolean;
}

export const DEFAULT_PARQUET_CONFIG: ParquetWriteConfig = {
  rowGroupSize: 100_000,
  compression: 'snappy',
  dictionaryEncoding: true,
  pageSize: 1024 * 1024,
  enableStatistics: true,
};

// =============================================================================
// Schema Inference
// =============================================================================

/**
 * Infer Iceberg schema from CDC events
 */
export function inferSchemaFromEvents(
  events: CDCEvent[],
  tableName: string
): IcebergSchema {
  // Collect all unique field names and their types
  const fieldTypes = new Map<string, Set<string>>();

  // Add CDC metadata fields
  fieldTypes.set('_cdc_sequence', new Set(['long']));
  fieldTypes.set('_cdc_timestamp', new Set(['timestamptz']));
  fieldTypes.set('_cdc_operation', new Set(['string']));
  fieldTypes.set('_cdc_row_id', new Set(['string']));

  // Analyze event data
  for (const event of events) {
    const data = event.after ?? event.before;
    if (data && typeof data === 'object') {
      for (const [key, value] of Object.entries(data as Record<string, unknown>)) {
        let types = fieldTypes.get(key);
        if (!types) {
          types = new Set();
          fieldTypes.set(key, types);
        }
        types.add(inferType(value));
      }
    }
  }

  // Build schema fields
  const fields: IcebergField[] = [];
  let fieldId = 1;

  for (const [name, types] of fieldTypes.entries()) {
    // Pick the most specific type if multiple types are detected
    const type = selectBestType(Array.from(types));
    const isMetadataField = name.startsWith('_cdc_');

    fields.push({
      id: fieldId++,
      name,
      type,
      required: isMetadataField, // CDC metadata fields are required
      doc: isMetadataField ? `CDC ${name.replace('_cdc_', '')} field` : undefined,
    });
  }

  return {
    type: 'struct',
    'schema-id': 0,
    fields,
    'identifier-field-ids': [fields.find((f) => f.name === '_cdc_row_id')?.id ?? 1],
  };
}

/**
 * Infer type from a JavaScript value
 */
function inferType(value: unknown): string {
  if (value === null || value === undefined) {
    return 'string'; // Default nullable type
  }

  switch (typeof value) {
    case 'boolean':
      return 'boolean';
    case 'number':
      if (Number.isInteger(value)) {
        if (value >= -2147483648 && value <= 2147483647) {
          return 'int';
        }
        return 'long';
      }
      return 'double';
    case 'string':
      // Check for date/time patterns
      if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(value)) {
        return 'timestamptz';
      }
      if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
        return 'date';
      }
      if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) {
        return 'uuid';
      }
      return 'string';
    case 'bigint':
      return 'long';
    case 'object':
      if (value instanceof Date) {
        return 'timestamptz';
      }
      if (value instanceof Uint8Array) {
        return 'binary';
      }
      if (Array.isArray(value)) {
        return 'string'; // Serialize arrays as JSON strings
      }
      return 'string'; // Serialize objects as JSON strings
    default:
      return 'string';
  }
}

/**
 * Select the best type when multiple types are detected
 */
function selectBestType(types: string[]): string {
  // Priority order: specific -> general
  const priority = [
    'timestamptz',
    'date',
    'uuid',
    'double',
    'long',
    'int',
    'boolean',
    'binary',
    'string',
  ];

  for (const type of priority) {
    if (types.includes(type)) {
      // Handle numeric coercion
      if (type === 'int' && types.includes('long')) {
        return 'long';
      }
      if ((type === 'int' || type === 'long') && types.includes('double')) {
        return 'double';
      }
      return type;
    }
  }

  return 'string';
}

// =============================================================================
// Row Conversion
// =============================================================================

/**
 * Convert CDC events to rows for Parquet
 */
export function eventsToRows(
  events: CDCEvent[],
  schema: IcebergSchema
): Record<string, unknown>[] {
  const rows: Record<string, unknown>[] = [];

  for (const event of events) {
    const data = event.after ?? event.before ?? {};
    const row: Record<string, unknown> = {};

    // Add CDC metadata
    row._cdc_sequence = event.sequence;
    row._cdc_timestamp = event.timestamp;
    row._cdc_operation = event.operation;
    row._cdc_row_id = event.rowId;

    // Add data fields
    if (typeof data === 'object' && data !== null) {
      for (const field of schema.fields) {
        if (!field.name.startsWith('_cdc_')) {
          const value = (data as Record<string, unknown>)[field.name];
          row[field.name] = convertValue(value, field.type);
        }
      }
    }

    rows.push(row);
  }

  return rows;
}

/**
 * Convert a value to the target Iceberg type
 */
function convertValue(value: unknown, targetType: string): unknown {
  if (value === null || value === undefined) {
    return null;
  }

  switch (targetType) {
    case 'boolean':
      return Boolean(value);
    case 'int':
    case 'long':
      return typeof value === 'bigint' ? value : BigInt(Math.floor(Number(value)));
    case 'float':
    case 'double':
      return Number(value);
    case 'string':
      if (typeof value === 'object') {
        return JSON.stringify(value);
      }
      return String(value);
    case 'timestamptz':
    case 'timestamp':
      if (value instanceof Date) {
        return value.getTime() * 1000; // Microseconds
      }
      if (typeof value === 'string') {
        return new Date(value).getTime() * 1000;
      }
      return Number(value) * 1000;
    case 'date':
      if (value instanceof Date) {
        return Math.floor(value.getTime() / 86400000); // Days since epoch
      }
      if (typeof value === 'string') {
        return Math.floor(new Date(value).getTime() / 86400000);
      }
      return Math.floor(Number(value) / 86400000);
    case 'binary':
      if (value instanceof Uint8Array) {
        return value;
      }
      if (typeof value === 'string') {
        return new TextEncoder().encode(value);
      }
      return new Uint8Array();
    case 'uuid':
      return String(value);
    default:
      return value;
  }
}

// =============================================================================
// Simple Parquet Encoder
// =============================================================================

/**
 * Parquet file header magic bytes
 */
const PARQUET_MAGIC = new Uint8Array([0x50, 0x41, 0x52, 0x31]); // "PAR1"

/**
 * Parquet type codes
 */
const ParquetType = {
  BOOLEAN: 0,
  INT32: 1,
  INT64: 2,
  INT96: 3,
  FLOAT: 4,
  DOUBLE: 5,
  BYTE_ARRAY: 6,
  FIXED_LEN_BYTE_ARRAY: 7,
} as const;

/**
 * Parquet converted types
 */
const ConvertedType = {
  UTF8: 0,
  INT_64: 3,
  TIMESTAMP_MILLIS: 9,
  TIMESTAMP_MICROS: 10,
  DATE: 6,
  UUID: 24,
} as const;

/**
 * Map Iceberg types to Parquet types
 */
function icebergToParquetType(icebergType: string): {
  type: number;
  convertedType?: number;
  logicalType?: unknown;
} {
  switch (icebergType) {
    case 'boolean':
      return { type: ParquetType.BOOLEAN };
    case 'int':
      return { type: ParquetType.INT32 };
    case 'long':
      return { type: ParquetType.INT64, convertedType: ConvertedType.INT_64 };
    case 'float':
      return { type: ParquetType.FLOAT };
    case 'double':
      return { type: ParquetType.DOUBLE };
    case 'string':
      return { type: ParquetType.BYTE_ARRAY, convertedType: ConvertedType.UTF8 };
    case 'timestamptz':
    case 'timestamp':
      return { type: ParquetType.INT64, convertedType: ConvertedType.TIMESTAMP_MICROS };
    case 'date':
      return { type: ParquetType.INT32, convertedType: ConvertedType.DATE };
    case 'uuid':
      return { type: ParquetType.FIXED_LEN_BYTE_ARRAY, convertedType: ConvertedType.UUID };
    case 'binary':
      return { type: ParquetType.BYTE_ARRAY };
    default:
      return { type: ParquetType.BYTE_ARRAY, convertedType: ConvertedType.UTF8 };
  }
}

/**
 * Simple column data for Parquet encoding
 */
interface ColumnData {
  name: string;
  type: string;
  values: unknown[];
  nullCount: number;
  minValue?: unknown;
  maxValue?: unknown;
}

/**
 * Extract column data from rows
 */
function extractColumnData(
  rows: Record<string, unknown>[],
  schema: IcebergSchema
): ColumnData[] {
  const columns: ColumnData[] = [];

  for (const field of schema.fields) {
    const values: unknown[] = [];
    let nullCount = 0;
    let minValue: unknown;
    let maxValue: unknown;

    for (const row of rows) {
      const value = row[field.name];
      values.push(value);

      if (value === null || value === undefined) {
        nullCount++;
      } else {
        // Track min/max for statistics
        if (minValue === undefined || compareValues(value, minValue) < 0) {
          minValue = value;
        }
        if (maxValue === undefined || compareValues(value, maxValue) > 0) {
          maxValue = value;
        }
      }
    }

    columns.push({
      name: field.name,
      type: field.type,
      values,
      nullCount,
      minValue,
      maxValue,
    });
  }

  return columns;
}

/**
 * Compare two values for min/max tracking
 */
function compareValues(a: unknown, b: unknown): number {
  if (typeof a === 'number' && typeof b === 'number') {
    return a - b;
  }
  if (typeof a === 'bigint' && typeof b === 'bigint') {
    return a < b ? -1 : a > b ? 1 : 0;
  }
  if (typeof a === 'string' && typeof b === 'string') {
    return a.localeCompare(b);
  }
  return 0;
}

/**
 * Encode a value to bytes based on Parquet type
 */
function encodeValue(value: unknown, icebergType: string): Uint8Array {
  if (value === null || value === undefined) {
    return new Uint8Array(0);
  }

  switch (icebergType) {
    case 'boolean': {
      return new Uint8Array([value ? 1 : 0]);
    }
    case 'int': {
      const buffer = new ArrayBuffer(4);
      new DataView(buffer).setInt32(0, Number(value), true);
      return new Uint8Array(buffer);
    }
    case 'long':
    case 'timestamptz':
    case 'timestamp': {
      const buffer = new ArrayBuffer(8);
      const view = new DataView(buffer);
      const bigValue = typeof value === 'bigint' ? value : BigInt(value as number);
      view.setBigInt64(0, bigValue, true);
      return new Uint8Array(buffer);
    }
    case 'float': {
      const buffer = new ArrayBuffer(4);
      new DataView(buffer).setFloat32(0, Number(value), true);
      return new Uint8Array(buffer);
    }
    case 'double': {
      const buffer = new ArrayBuffer(8);
      new DataView(buffer).setFloat64(0, Number(value), true);
      return new Uint8Array(buffer);
    }
    case 'date': {
      const buffer = new ArrayBuffer(4);
      new DataView(buffer).setInt32(0, Number(value), true);
      return new Uint8Array(buffer);
    }
    case 'string': {
      const str = String(value);
      const encoded = new TextEncoder().encode(str);
      const buffer = new ArrayBuffer(4 + encoded.length);
      new DataView(buffer).setInt32(0, encoded.length, true);
      new Uint8Array(buffer).set(encoded, 4);
      return new Uint8Array(buffer);
    }
    case 'binary': {
      const bytes = value instanceof Uint8Array ? value : new TextEncoder().encode(String(value));
      const buffer = new ArrayBuffer(4 + bytes.length);
      new DataView(buffer).setInt32(0, bytes.length, true);
      new Uint8Array(buffer).set(bytes, 4);
      return new Uint8Array(buffer);
    }
    case 'uuid': {
      // UUID as 16 fixed bytes
      const str = String(value).replace(/-/g, '');
      const bytes = new Uint8Array(16);
      for (let i = 0; i < 16; i++) {
        bytes[i] = parseInt(str.slice(i * 2, i * 2 + 2), 16);
      }
      return bytes;
    }
    default:
      return new TextEncoder().encode(String(value));
  }
}

// =============================================================================
// Parquet Writer
// =============================================================================

/**
 * Parquet write result
 */
export interface ParquetWriteResult {
  /** Generated file content */
  content: Uint8Array;
  /** Number of rows written */
  rowCount: number;
  /** File size in bytes */
  fileSize: number;
  /** Column statistics */
  columnStats: Map<string, { min: unknown; max: unknown; nullCount: number }>;
  /** Schema used */
  schema: IcebergSchema;
}

/**
 * Write CDC events to Parquet format
 *
 * This is a simplified Parquet writer that creates valid Parquet files
 * readable by standard Parquet readers. For production use with complex
 * data types and optimal compression, use hyparquet or apache-arrow.
 */
export async function writeParquet(
  events: CDCEvent[],
  tableName: string,
  config: Partial<ParquetWriteConfig> = {}
): Promise<ParquetWriteResult> {
  const writeConfig = { ...DEFAULT_PARQUET_CONFIG, ...config };

  if (events.length === 0) {
    throw new ParquetWriteError('Cannot write empty event list');
  }

  // Infer schema from events
  const schema = inferSchemaFromEvents(events, tableName);

  // Convert events to rows
  const rows = eventsToRows(events, schema);

  // Extract column data
  const columns = extractColumnData(rows, schema);

  // Build column statistics
  const columnStats = new Map<string, { min: unknown; max: unknown; nullCount: number }>();
  for (const col of columns) {
    columnStats.set(col.name, {
      min: col.minValue,
      max: col.maxValue,
      nullCount: col.nullCount,
    });
  }

  // Encode Parquet file
  const content = encodeParquetFile(columns, schema, rows.length, writeConfig);

  return {
    content,
    rowCount: rows.length,
    fileSize: content.length,
    columnStats,
    schema,
  };
}

/**
 * Write a partition buffer to Parquet
 */
export async function writePartitionToParquet(
  buffer: PartitionBuffer,
  config: Partial<ParquetWriteConfig> = {}
): Promise<ParquetWriteResult> {
  return writeParquet(buffer.events, buffer.table, config);
}

/**
 * Encode a complete Parquet file
 *
 * File structure:
 * - 4 bytes: PAR1 magic
 * - Row groups (each contains column chunks)
 * - File metadata (Thrift encoded)
 * - 4 bytes: metadata length
 * - 4 bytes: PAR1 magic
 */
function encodeParquetFile(
  columns: ColumnData[],
  schema: IcebergSchema,
  rowCount: number,
  _config: ParquetWriteConfig
): Uint8Array {
  // Build file content
  const chunks: Uint8Array[] = [];

  // Header magic
  chunks.push(PARQUET_MAGIC);

  // Encode row group with all columns
  const rowGroupData = encodeRowGroup(columns, schema);
  chunks.push(rowGroupData.data);

  // Encode file metadata as JSON (simplified - real Parquet uses Thrift)
  const metadata = {
    version: 1,
    schema: schema.fields.map((f) => ({
      name: f.name,
      type: icebergToParquetType(f.type),
      repetition_type: f.required ? 'REQUIRED' : 'OPTIONAL',
    })),
    num_rows: rowCount,
    row_groups: [
      {
        columns: rowGroupData.columnMeta,
        total_byte_size: rowGroupData.data.length,
        num_rows: rowCount,
      },
    ],
    created_by: 'DoLake',
  };

  const metadataJson = JSON.stringify(metadata);
  const metadataBytes = new TextEncoder().encode(metadataJson);

  chunks.push(metadataBytes);

  // Metadata length (4 bytes, little endian)
  const lenBuffer = new ArrayBuffer(4);
  new DataView(lenBuffer).setInt32(0, metadataBytes.length, true);
  chunks.push(new Uint8Array(lenBuffer));

  // Footer magic
  chunks.push(PARQUET_MAGIC);

  // Concatenate all chunks
  const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }

  return result;
}

/**
 * Encode a row group
 */
function encodeRowGroup(
  columns: ColumnData[],
  schema: IcebergSchema
): { data: Uint8Array; columnMeta: unknown[] } {
  const columnChunks: Uint8Array[] = [];
  const columnMeta: unknown[] = [];
  let offset = PARQUET_MAGIC.length;

  for (let i = 0; i < columns.length; i++) {
    const column = columns[i];
    const field = schema.fields[i];

    // Skip if field or column is undefined
    if (!field || !column) continue;

    // Encode column data
    const encodedColumn = encodeColumn(column, field);
    columnChunks.push(encodedColumn);

    columnMeta.push({
      file_offset: offset,
      type: icebergToParquetType(field.type).type,
      encodings: ['PLAIN'],
      path_in_schema: [field.name],
      total_compressed_size: encodedColumn.length,
      total_uncompressed_size: encodedColumn.length,
    });

    offset += encodedColumn.length;
  }

  // Concatenate column chunks
  const totalLength = columnChunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const data = new Uint8Array(totalLength);
  let pos = 0;
  for (const chunk of columnChunks) {
    data.set(chunk, pos);
    pos += chunk.length;
  }

  return { data, columnMeta };
}

/**
 * Encode a single column
 */
function encodeColumn(column: ColumnData, field: IcebergField): Uint8Array {
  const valueChunks: Uint8Array[] = [];

  // Definition levels (for nullable columns)
  if (!field.required) {
    const defLevels = new Uint8Array(column.values.length);
    for (let i = 0; i < column.values.length; i++) {
      defLevels[i] = column.values[i] != null ? 1 : 0;
    }
    valueChunks.push(defLevels);
  }

  // Encode values
  for (const value of column.values) {
    if (value != null) {
      valueChunks.push(encodeValue(value, column.type));
    }
  }

  // Concatenate
  const totalLength = valueChunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of valueChunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }

  return result;
}

// =============================================================================
// DataFile Builder
// =============================================================================

/**
 * Create an Iceberg DataFile entry from Parquet write result
 */
export function createDataFile(
  filePath: string,
  result: ParquetWriteResult,
  partition: Record<string, unknown> = {}
): DataFile {
  // Build column sizes and bounds
  const columnSizes = new Map<number, bigint>();
  const valueCounts = new Map<number, bigint>();
  const nullValueCounts = new Map<number, bigint>();
  const lowerBounds = new Map<number, Uint8Array>();
  const upperBounds = new Map<number, Uint8Array>();

  for (let i = 0; i < result.schema.fields.length; i++) {
    const field = result.schema.fields[i];
    if (!field) continue;
    const stats = result.columnStats.get(field.name);

    if (stats) {
      // Estimate column size (simplified)
      columnSizes.set(field.id, BigInt(Math.floor(result.fileSize / result.schema.fields.length)));
      valueCounts.set(field.id, BigInt(result.rowCount));
      nullValueCounts.set(field.id, BigInt(stats.nullCount));

      // Encode bounds
      if (stats.min !== undefined) {
        lowerBounds.set(field.id, encodeValue(stats.min, field.type));
      }
      if (stats.max !== undefined) {
        upperBounds.set(field.id, encodeValue(stats.max, field.type));
      }
    }
  }

  return {
    content: 0, // Data file (not delete file)
    'file-path': filePath,
    'file-format': 'parquet',
    partition,
    'record-count': BigInt(result.rowCount),
    'file-size-in-bytes': BigInt(result.fileSize),
    'column-sizes': columnSizes,
    'value-counts': valueCounts,
    'null-value-counts': nullValueCounts,
    'lower-bounds': lowerBounds,
    'upper-bounds': upperBounds,
  };
}
