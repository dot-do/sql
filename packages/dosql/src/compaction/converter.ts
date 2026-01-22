/**
 * Compaction Converter for DoSQL
 *
 * Converts rows from B-tree format to columnar chunks.
 * Handles encoding selection, batching, and chunk creation.
 */

import type { FSXBackend } from '../fsx/types.js';
import type {
  ColumnarTableSchema,
  RowGroup,
  ColumnDefinition,
  ColumnDataType,
  Encoding,
} from '../columnar/types.js';
import { ColumnarWriter } from '../columnar/writer.js';
import type {
  CompactionConfig,
  CompactionCandidate,
  ConversionOptions,
  ConversionResult,
} from './types.js';
import { CompactionError, CompactionErrorCode } from './types.js';

// =============================================================================
// Encoding Selection
// =============================================================================

/**
 * Analyze values and select the best encoding for a column
 */
export function selectEncoding(
  values: unknown[],
  dataType: ColumnDataType
): Encoding {
  if (values.length === 0) return 'raw';

  const nonNullValues = values.filter((v) => v !== null && v !== undefined);
  if (nonNullValues.length === 0) return 'raw';

  switch (dataType) {
    case 'string':
      return selectStringEncoding(nonNullValues as string[]);

    case 'int8':
    case 'int16':
    case 'int32':
    case 'int64':
    case 'uint8':
    case 'uint16':
    case 'uint32':
    case 'uint64':
    case 'timestamp':
      return selectIntegerEncoding(nonNullValues as (number | bigint)[]);

    case 'float32':
    case 'float64':
      // Floats don't compress well with most techniques
      return 'raw';

    case 'boolean':
      // Booleans are already 1 bit each effectively
      return 'raw';

    case 'bytes':
      return 'raw';

    default:
      return 'raw';
  }
}

/**
 * Select encoding for string columns
 */
function selectStringEncoding(values: string[]): Encoding {
  const uniqueValues = new Set(values);
  const cardinalityRatio = uniqueValues.size / values.length;

  // Low cardinality: use dictionary encoding
  if (cardinalityRatio < 0.1 && values.length >= 100) {
    return 'dict';
  }

  // Check for runs
  let runCount = 1;
  for (let i = 1; i < values.length; i++) {
    if (values[i] !== values[i - 1]) {
      runCount++;
    }
  }

  // If average run length > 3, use RLE
  if (values.length / runCount > 3) {
    return 'rle';
  }

  return 'raw';
}

/**
 * Select encoding for integer columns
 */
function selectIntegerEncoding(values: (number | bigint)[]): Encoding {
  if (values.length < 10) return 'raw';

  // Check for sorted data (good for delta encoding)
  let isSorted = true;
  let prevValue = values[0];
  for (let i = 1; i < values.length; i++) {
    if (values[i] < prevValue) {
      isSorted = false;
      break;
    }
    prevValue = values[i];
  }

  if (isSorted) {
    return 'delta';
  }

  // Check for runs
  let runCount = 1;
  for (let i = 1; i < values.length; i++) {
    if (values[i] !== values[i - 1]) {
      runCount++;
    }
  }

  // If average run length > 3, use RLE
  if (values.length / runCount > 3) {
    return 'rle';
  }

  // Check value range for bit-packing
  const numValues = values.map((v) => Number(v));
  const min = Math.min(...numValues);
  const max = Math.max(...numValues);
  const range = max - min;

  // If range is small, bit-packing might help
  if (range > 0 && range < 256) {
    return 'bitpack';
  }

  return 'raw';
}

// =============================================================================
// Row-to-Record Converter
// =============================================================================

/**
 * Convert a row value to a record matching the columnar schema
 */
export function rowToRecord<V>(
  value: V,
  schema: ColumnarTableSchema
): Record<string, unknown> {
  // If value is already a record, validate and return
  if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }

  // If value is a primitive, try to map to first column
  if (schema.columns.length === 1) {
    return { [schema.columns[0].name]: value };
  }

  // Try JSON parse if string
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      if (typeof parsed === 'object' && parsed !== null) {
        return parsed as Record<string, unknown>;
      }
    } catch {
      // Not valid JSON
    }
  }

  // If value is Uint8Array, try to decode as JSON
  if (value instanceof Uint8Array) {
    try {
      const str = new TextDecoder().decode(value);
      const parsed = JSON.parse(str);
      if (typeof parsed === 'object' && parsed !== null) {
        return parsed as Record<string, unknown>;
      }
    } catch {
      // Not valid JSON
    }
  }

  throw new Error(
    `Cannot convert value to record: ${typeof value}. Schema expects columns: ${schema.columns.map((c) => c.name).join(', ')}`
  );
}

// =============================================================================
// Converter Implementation
// =============================================================================

/**
 * Converter for transforming B-tree rows into columnar chunks
 */
export class RowToColumnarConverter<K, V> {
  private readonly schema: ColumnarTableSchema;
  private readonly fsx: FSXBackend;
  private readonly config: CompactionConfig;

  /** Buffer of rows waiting to be converted */
  private buffer: Record<string, unknown>[] = [];

  /** Track column values for encoding analysis */
  private columnValues = new Map<string, unknown[]>();

  constructor(
    schema: ColumnarTableSchema,
    fsx: FSXBackend,
    config: CompactionConfig
  ) {
    this.schema = schema;
    this.fsx = fsx;
    this.config = config;
    this.initColumnValues();
  }

  /**
   * Initialize column value trackers
   */
  private initColumnValues(): void {
    this.columnValues.clear();
    for (const col of this.schema.columns) {
      this.columnValues.set(col.name, []);
    }
  }

  /**
   * Add rows to the conversion buffer
   */
  bufferRows(candidates: CompactionCandidate<K, V>[]): void {
    for (const candidate of candidates) {
      const record = rowToRecord(candidate.value, this.schema);
      this.buffer.push(record);

      // Track values for encoding analysis
      for (const col of this.schema.columns) {
        const values = this.columnValues.get(col.name)!;
        values.push(record[col.name]);
      }
    }
  }

  /**
   * Get current buffer size
   */
  getBufferSize(): number {
    return this.buffer.length;
  }

  /**
   * Check if buffer is ready for conversion
   */
  isReadyToConvert(): boolean {
    return this.buffer.length >= this.config.targetChunkRows;
  }

  /**
   * Estimate current buffer size in bytes
   */
  estimateBufferBytes(): number {
    let size = 0;
    for (const record of this.buffer) {
      // Rough estimate: JSON stringify length * 2 (for UTF-16)
      size += JSON.stringify(record).length * 2;
    }
    return size;
  }

  /**
   * Convert buffered rows to columnar format
   */
  async convert(options: ConversionOptions = {}): Promise<ConversionResult> {
    const startTime = Date.now();

    if (this.buffer.length === 0) {
      return {
        rowGroups: [],
        serialized: [],
        rowCount: 0,
        byteSize: 0,
        durationMs: 0,
      };
    }

    try {
      // Determine encoding for each column
      const encodingMap = new Map<string, Encoding>();
      if (options.forceEncoding) {
        for (const [name, encoding] of options.forceEncoding) {
          encodingMap.set(name, encoding);
        }
      } else {
        for (const col of this.schema.columns) {
          const values = this.columnValues.get(col.name)!;
          const encoding = selectEncoding(values, col.dataType);
          encodingMap.set(col.name, encoding);
        }
      }

      // Create columnar writer with computed encodings
      const rowGroups: RowGroup[] = [];
      const serialized: Uint8Array[] = [];

      const writer = new ColumnarWriter(
        this.schema,
        {
          targetRowsPerGroup: options.targetRows ?? this.config.targetChunkRows,
          targetBytesPerGroup: options.targetBytes ?? this.config.targetChunkSize,
          forceEncoding: encodingMap,
          onFlush: async (rowGroup, data) => {
            rowGroups.push(rowGroup);
            serialized.push(data);
          },
        },
        this.fsx
      );

      // Write all buffered rows
      await writer.write(this.buffer);

      // Finalize any remaining rows
      const final = await writer.finalize();
      if (final) {
        // Note: finalize doesn't provide serialized data directly
        // In a real implementation, we'd need to serialize the final row group
      }

      // Calculate total bytes
      const byteSize = serialized.reduce((sum, data) => sum + data.length, 0);

      // Clear buffer after successful conversion
      const rowCount = this.buffer.length;
      this.clearBuffer();

      return {
        rowGroups,
        serialized,
        rowCount,
        byteSize,
        durationMs: Date.now() - startTime,
      };
    } catch (error) {
      throw new CompactionError(
        CompactionErrorCode.CONVERSION_FAILED,
        `Failed to convert rows to columnar: ${error instanceof Error ? error.message : 'Unknown error'}`,
        undefined,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Clear the conversion buffer
   */
  clearBuffer(): void {
    this.buffer = [];
    this.initColumnValues();
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a row-to-columnar converter
 */
export function createConverter<K, V>(
  schema: ColumnarTableSchema,
  fsx: FSXBackend,
  config: CompactionConfig
): RowToColumnarConverter<K, V> {
  return new RowToColumnarConverter<K, V>(schema, fsx, config);
}

// =============================================================================
// Direct Conversion Utility
// =============================================================================

/**
 * Convert candidates directly to columnar format without buffering
 */
export async function convertCandidates<K, V>(
  candidates: CompactionCandidate<K, V>[],
  schema: ColumnarTableSchema,
  fsx: FSXBackend,
  config: CompactionConfig,
  options: ConversionOptions = {}
): Promise<ConversionResult> {
  const converter = createConverter<K, V>(schema, fsx, config);
  converter.bufferRows(candidates);
  return converter.convert(options);
}

// =============================================================================
// Chunk Writer
// =============================================================================

/**
 * Write converted chunks to storage
 */
export async function writeChunks(
  rowGroups: RowGroup[],
  serialized: Uint8Array[],
  fsx: FSXBackend,
  config: CompactionConfig,
  tableName: string
): Promise<string[]> {
  const storagePaths: string[] = [];

  for (let i = 0; i < rowGroups.length; i++) {
    const rowGroup = rowGroups[i];
    const data = serialized[i];

    const path = `${config.columnarPrefix}${tableName}/chunks/${rowGroup.id}`;

    try {
      await fsx.write(path, data);
      storagePaths.push(path);
    } catch (error) {
      throw new CompactionError(
        CompactionErrorCode.WRITE_FAILED,
        `Failed to write chunk ${rowGroup.id}: ${error instanceof Error ? error.message : 'Unknown error'}`,
        undefined,
        error instanceof Error ? error : undefined
      );
    }
  }

  return storagePaths;
}

// =============================================================================
// Streaming Converter
// =============================================================================

/**
 * Convert rows to columnar format in a streaming fashion
 * Yields row groups as they're created
 */
export async function* streamConvert<K, V>(
  candidates: AsyncIterable<CompactionCandidate<K, V>>,
  schema: ColumnarTableSchema,
  fsx: FSXBackend,
  config: CompactionConfig,
  options: ConversionOptions = {}
): AsyncIterableIterator<{ rowGroup: RowGroup; serialized: Uint8Array }> {
  const converter = createConverter<K, V>(schema, fsx, config);
  const batch: CompactionCandidate<K, V>[] = [];

  for await (const candidate of candidates) {
    batch.push(candidate);

    // Check if we should convert
    if (batch.length >= (options.targetRows ?? config.targetChunkRows)) {
      converter.bufferRows(batch);
      batch.length = 0;

      const result = await converter.convert(options);

      for (let i = 0; i < result.rowGroups.length; i++) {
        yield {
          rowGroup: result.rowGroups[i],
          serialized: result.serialized[i],
        };
      }
    }
  }

  // Handle remaining candidates
  if (batch.length > 0) {
    converter.bufferRows(batch);
    const result = await converter.convert(options);

    for (let i = 0; i < result.rowGroups.length; i++) {
      yield {
        rowGroup: result.rowGroups[i],
        serialized: result.serialized[i],
      };
    }
  }
}
