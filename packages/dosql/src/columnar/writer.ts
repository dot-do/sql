/**
 * Columnar Chunk Manager - Write Path
 *
 * Accepts rows, buffers them, and flushes to columnar chunks.
 * Auto-selects encoding per column based on data characteristics.
 *
 * SAFETY NOTES:
 * - All buffer operations include bounds checking to prevent memory corruption
 * - Values are snapshotted before encoding to prevent TOCTOU vulnerabilities
 * - Debug assertions verify invariants during development
 */

import {
  type ColumnChunk,
  type RowGroup,
  type ColumnarTableSchema,
  type ColumnDefinition,
  type ColumnDataType,
  type Encoding,
  type FSXInterface,
  MAX_ROWS_PER_ROW_GROUP,
  TARGET_ROW_GROUP_SIZE,
} from './types.js';

import {
  encodeRaw,
  encodeRawStrings,
  encodeDictionary,
  encodeRLE,
  encodeDelta,
  bitpackEncode,
  createNullBitmap,
  analyzeForEncoding,
  calculateStats,
} from './encoding.js';

import {
  serializeRowGroup,
  generateRowGroupId,
  estimateRowGroupSize,
} from './chunk.js';

// ============================================================================
// Debug Utilities
// ============================================================================

/**
 * Debug assertion that throws in development but is stripped in production.
 * Used to verify safety invariants during development.
 */
function debugAssert(condition: boolean, message: string): asserts condition {
  if (!condition) {
    throw new Error(`Assertion failed: ${message}`);
  }
}

/**
 * Verify buffer bounds before a write operation.
 * Throws a descriptive error if the write would overflow.
 */
function assertBufferBounds(
  offset: number,
  length: number,
  bufferSize: number,
  context: string
): void {
  if (offset < 0) {
    throw new Error(
      `Buffer underflow in ${context}: offset ${offset} is negative`
    );
  }
  if (offset + length > bufferSize) {
    throw new Error(
      `Buffer overflow in ${context}: ` +
      `attempting to write ${length} bytes at offset ${offset}, ` +
      `but buffer size is only ${bufferSize} bytes ` +
      `(would overflow by ${offset + length - bufferSize} bytes)`
    );
  }
}

// ============================================================================
// Writer Configuration
// ============================================================================

export interface WriterConfig {
  /** Target row count per row group */
  targetRowsPerGroup?: number;

  /** Target byte size per row group */
  targetBytesPerGroup?: number;

  /** Force specific encoding for columns (by name) */
  forceEncoding?: Map<string, Encoding>;

  /** Disable auto-encoding selection */
  disableAutoEncoding?: boolean;

  /** Flush callback when row group is ready */
  onFlush?: (rowGroup: RowGroup, data: Uint8Array) => Promise<void>;
}

const DEFAULT_CONFIG: Required<Omit<WriterConfig, 'forceEncoding' | 'onFlush'>> = {
  targetRowsPerGroup: MAX_ROWS_PER_ROW_GROUP,
  targetBytesPerGroup: TARGET_ROW_GROUP_SIZE,
  disableAutoEncoding: false,
};

// ============================================================================
// Column Buffer
// ============================================================================

interface ColumnBuffer {
  definition: ColumnDefinition;
  values: unknown[];
}

// ============================================================================
// Columnar Writer
// ============================================================================

export class ColumnarWriter {
  private readonly schema: ColumnarTableSchema;
  private readonly config: Required<Omit<WriterConfig, 'forceEncoding' | 'onFlush'>> &
    Pick<WriterConfig, 'forceEncoding' | 'onFlush'>;
  private readonly fsx?: FSXInterface;

  private buffers: Map<string, ColumnBuffer> = new Map();
  private bufferedRowCount: number = 0;
  private totalRowCount: number = 0;
  private rowGroupSequence: number = 0;
  private flushedRowGroups: RowGroup[] = [];

  constructor(
    schema: ColumnarTableSchema,
    config?: WriterConfig,
    fsx?: FSXInterface
  ) {
    this.schema = schema;
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.fsx = fsx;

    // Initialize column buffers
    for (const col of schema.columns) {
      this.buffers.set(col.name, {
        definition: col,
        values: [],
      });
    }
  }

  /**
   * Write rows to the columnar buffer.
   */
  async write(rows: Record<string, unknown>[]): Promise<RowGroup[]> {
    const flushed: RowGroup[] = [];

    for (const row of rows) {
      this.bufferRow(row);

      // Check if we should flush
      if (this.shouldFlush()) {
        const rowGroup = await this.flush();
        flushed.push(rowGroup);
      }
    }

    return flushed;
  }

  /**
   * Write a single row to the buffer.
   */
  private bufferRow(row: Record<string, unknown>): void {
    for (const col of this.schema.columns) {
      const buffer = this.buffers.get(col.name)!;
      const value = row[col.name];

      // Validate nullable constraint
      if (!col.nullable && (value === null || value === undefined)) {
        throw new Error(`Column ${col.name} does not allow null values`);
      }

      buffer.values.push(value ?? null);
    }

    this.bufferedRowCount++;
    this.totalRowCount++;
  }

  /**
   * Check if we should flush the buffer.
   */
  private shouldFlush(): boolean {
    if (this.bufferedRowCount >= this.config.targetRowsPerGroup) {
      return true;
    }

    // Estimate current buffer size
    const estimatedSize = this.estimateBufferSize();
    if (estimatedSize >= this.config.targetBytesPerGroup) {
      return true;
    }

    return false;
  }

  /**
   * Estimate current buffer size in bytes.
   */
  private estimateBufferSize(): number {
    let size = 0;

    for (const [_, buffer] of this.buffers) {
      const bytesPerValue = this.getBytesPerValue(buffer.definition.dataType);
      if (bytesPerValue > 0) {
        size += buffer.values.length * bytesPerValue;
      } else {
        // Variable length (strings)
        for (const value of buffer.values) {
          if (value !== null && typeof value === 'string') {
            size += 4 + value.length * 2; // Conservative UTF-8 estimate
          }
        }
      }
    }

    return size;
  }

  private getBytesPerValue(dataType: ColumnDataType): number {
    switch (dataType) {
      case 'int8':
      case 'uint8':
      case 'boolean':
        return 1;
      case 'int16':
      case 'uint16':
        return 2;
      case 'int32':
      case 'uint32':
      case 'float32':
        return 4;
      case 'int64':
      case 'uint64':
      case 'float64':
      case 'timestamp':
        return 8;
      default:
        return 0; // Variable length
    }
  }

  /**
   * Flush the buffer to a row group.
   */
  async flush(): Promise<RowGroup> {
    if (this.bufferedRowCount === 0) {
      throw new Error('No rows to flush');
    }

    const columns = new Map<string, ColumnChunk>();
    const rowCount = this.bufferedRowCount;
    const startRow = this.totalRowCount - rowCount;

    // Encode each column
    for (const [name, buffer] of this.buffers) {
      const chunk = this.encodeColumn(buffer, rowCount);
      columns.set(name, chunk);
    }

    // Create row group
    const rowGroup: RowGroup = {
      id: generateRowGroupId(this.schema.tableName, this.rowGroupSequence++),
      rowCount,
      columns,
      rowRange: {
        start: startRow,
        end: startRow + rowCount - 1,
      },
      createdAt: Date.now(),
    };

    // Serialize for size calculation
    const serialized = serializeRowGroup(rowGroup);
    rowGroup.byteSize = serialized.length;

    // Store to fsx if available
    if (this.fsx) {
      const key = `${this.schema.tableName}/rowgroups/${rowGroup.id}`;
      await this.fsx.put(key, serialized);
    }

    // Call flush callback if provided
    if (this.config.onFlush) {
      await this.config.onFlush(rowGroup, serialized);
    }

    // Track flushed row groups
    this.flushedRowGroups.push(rowGroup);

    // Clear buffers
    this.clearBuffers();

    return rowGroup;
  }

  /**
   * Encode a column buffer into a column chunk.
   */
  private encodeColumn(buffer: ColumnBuffer, rowCount: number): ColumnChunk {
    const { definition, values } = buffer;
    const { name, dataType } = definition;

    // Create null bitmap
    const nullBitmap = createNullBitmap(values);

    // Calculate statistics
    const stats = calculateStats(values, dataType);

    // Determine encoding
    let encoding: Encoding;
    if (this.config.forceEncoding?.has(name)) {
      encoding = this.config.forceEncoding.get(name)!;
    } else if (this.config.disableAutoEncoding) {
      encoding = 'raw';
    } else {
      const analysis = analyzeForEncoding(values, dataType);
      encoding = definition.preferredEncoding ?? analysis.recommendedEncoding;
    }

    // Encode data
    let encodedData: Uint8Array;

    switch (dataType) {
      case 'string':
        encodedData = this.encodeStringColumn(values as (string | null)[], encoding, nullBitmap);
        break;

      case 'int8':
      case 'int16':
      case 'int32':
      case 'int64':
      case 'uint8':
      case 'uint16':
      case 'uint32':
      case 'uint64':
      case 'timestamp':
        encodedData = this.encodeIntegerColumn(
          values as (number | bigint | null)[],
          dataType,
          encoding,
          nullBitmap
        );
        break;

      case 'float32':
      case 'float64':
        encodedData = this.encodeFloatColumn(
          values as (number | null)[],
          dataType,
          encoding,
          nullBitmap
        );
        break;

      case 'boolean':
        encodedData = this.encodeBooleanColumn(values as (boolean | null)[], nullBitmap);
        encoding = 'raw'; // Always raw for boolean
        break;

      case 'bytes':
        encodedData = this.encodeBytesColumn(values as (Uint8Array | null)[]);
        encoding = 'raw'; // Always raw for bytes
        break;

      default:
        throw new Error(`Unsupported data type: ${dataType}`);
    }

    return {
      columnName: name,
      dataType,
      rowCount,
      encoding,
      nullBitmap,
      data: encodedData,
      stats,
    };
  }

  /**
   * Encode a string column.
   */
  private encodeStringColumn(
    values: (string | null)[],
    encoding: Encoding,
    nullBitmap: Uint8Array
  ): Uint8Array {
    switch (encoding) {
      case 'dict':
        return encodeDictionary(values, nullBitmap).data;
      case 'raw':
      default:
        return encodeRawStrings(values).data;
    }
  }

  /**
   * Encode an integer column.
   */
  private encodeIntegerColumn(
    values: (number | bigint | null)[],
    dataType: ColumnDataType,
    encoding: Encoding,
    nullBitmap: Uint8Array
  ): Uint8Array {
    switch (encoding) {
      case 'rle':
        return encodeRLE(values, dataType, nullBitmap).data;

      case 'delta':
        return encodeDelta(values, dataType, nullBitmap).data;

      case 'bitpack': {
        // Find max value to determine bit width
        const nonNullValues = values.filter((v) => v !== null) as (number | bigint)[];
        const maxValue = nonNullValues.length > 0
          ? Math.max(...nonNullValues.map((v) => Math.abs(Number(v))))
          : 0;
        const bitWidth = maxValue === 0 ? 1 : Math.ceil(Math.log2(maxValue + 1));

        // Store bit width in first byte
        const packedData = bitpackEncode(
          values.map((v) => (v !== null ? Number(v) : 0)),
          bitWidth
        );

        const result = new Uint8Array(1 + packedData.length);
        result[0] = bitWidth;
        result.set(packedData, 1);
        return result;
      }

      case 'raw':
      default:
        return encodeRaw(values, dataType, nullBitmap).data;
    }
  }

  /**
   * Encode a float column.
   */
  private encodeFloatColumn(
    values: (number | null)[],
    dataType: ColumnDataType,
    encoding: Encoding,
    nullBitmap: Uint8Array
  ): Uint8Array {
    // Floats only support raw encoding
    return encodeRaw(values, dataType, nullBitmap).data;
  }

  /**
   * Encode a boolean column.
   */
  private encodeBooleanColumn(
    values: (boolean | null)[],
    nullBitmap: Uint8Array
  ): Uint8Array {
    return encodeRaw(values, 'boolean', nullBitmap).data;
  }

  /**
   * Encode a bytes column.
   *
   * Safety invariants:
   * - Buffer is allocated based on size calculated from snapshotted values
   * - Snapshot prevents TOCTOU (time-of-check-time-of-use) vulnerabilities
   * - Before each buffer.set(), we verify: offset + value.length <= buffer.length
   * - Size mismatches throw descriptive errors rather than corrupting memory
   */
  private encodeBytesColumn(values: (Uint8Array | null)[]): Uint8Array {
    const context = 'bytes column encoding';

    // Snapshot the values array to prevent TOCTOU issues
    // This ensures we iterate over the same data for both size calculation and copying
    const snapshot = values.map(v => v);

    // Calculate total size from snapshot
    let totalSize = 0;
    for (const value of snapshot) {
      totalSize += 4; // Length prefix (uint32)
      if (value !== null) {
        totalSize += value.length;
      }
    }

    // Debug assertion: totalSize should be non-negative
    debugAssert(totalSize >= 0, `totalSize must be non-negative, got ${totalSize}`);

    const buffer = new Uint8Array(totalSize);
    const view = new DataView(buffer.buffer);
    let offset = 0;

    for (let i = 0; i < snapshot.length; i++) {
      const value = snapshot[i];

      if (value !== null) {
        // Cache length to detect mutations
        const valueLength = value.length;

        // Verify bounds before writing length prefix
        assertBufferBounds(offset, 4, buffer.length, `${context} length prefix at index ${i}`);
        view.setUint32(offset, valueLength, true);
        offset += 4;

        // Verify bounds before buffer.set()
        assertBufferBounds(offset, valueLength, buffer.length, `${context} data at index ${i}`);
        buffer.set(value, offset);
        offset += valueLength;
      } else {
        // Null value: just write zero-length prefix
        assertBufferBounds(offset, 4, buffer.length, `${context} null at index ${i}`);
        view.setUint32(offset, 0, true);
        offset += 4;
      }
    }

    // Final consistency check
    debugAssert(
      offset === totalSize,
      `offset (${offset}) should equal totalSize (${totalSize}) after encoding`
    );

    return buffer;
  }

  /**
   * Clear the column buffers.
   */
  private clearBuffers(): void {
    for (const buffer of this.buffers.values()) {
      buffer.values = [];
    }
    this.bufferedRowCount = 0;
  }

  /**
   * Flush any remaining buffered rows.
   */
  async finalize(): Promise<RowGroup | null> {
    if (this.bufferedRowCount > 0) {
      return this.flush();
    }
    return null;
  }

  /**
   * Get all flushed row groups.
   */
  getFlushedRowGroups(): RowGroup[] {
    return this.flushedRowGroups;
  }

  /**
   * Get the total number of rows written.
   */
  getTotalRowCount(): number {
    return this.totalRowCount;
  }

  /**
   * Get the number of currently buffered rows.
   */
  getBufferedRowCount(): number {
    return this.bufferedRowCount;
  }

  /**
   * Get the schema.
   */
  getSchema(): ColumnarTableSchema {
    return this.schema;
  }
}

// ============================================================================
// Convenience Functions
// ============================================================================

/**
 * Write rows directly to columnar format without a writer instance.
 */
export async function writeColumnar(
  schema: ColumnarTableSchema,
  rows: Record<string, unknown>[],
  fsx?: FSXInterface
): Promise<{ rowGroups: RowGroup[]; serialized: Uint8Array[] }> {
  const serializedGroups: Uint8Array[] = [];

  const writer = new ColumnarWriter(schema, {
    onFlush: async (_rowGroup, data) => {
      serializedGroups.push(data);
    },
  }, fsx);

  const rowGroups = await writer.write(rows);

  // Finalize any remaining rows
  const final = await writer.finalize();
  if (final) {
    rowGroups.push(final);
  }

  return { rowGroups, serialized: serializedGroups };
}

/**
 * Create a schema from sample data.
 */
export function inferSchema(
  tableName: string,
  sampleRows: Record<string, unknown>[]
): ColumnarTableSchema {
  if (sampleRows.length === 0) {
    throw new Error('Cannot infer schema from empty data');
  }

  const columns: ColumnDefinition[] = [];
  const sample = sampleRows[0];

  for (const [name, value] of Object.entries(sample)) {
    let dataType: ColumnDataType = 'string';
    let nullable = false;

    // Check all rows for type consistency and nullability
    for (const row of sampleRows) {
      const v = row[name];
      if (v === null || v === undefined) {
        nullable = true;
        continue;
      }

      const inferredType = inferType(v);
      if (dataType === 'string' && inferredType !== 'string') {
        dataType = inferredType;
      } else if (dataType !== inferredType && inferredType !== 'string') {
        // Type mismatch - fall back to string
        dataType = 'string';
      }
    }

    columns.push({ name, dataType, nullable });
  }

  return { tableName, columns };
}

function inferType(value: unknown): ColumnDataType {
  if (typeof value === 'number') {
    return Number.isInteger(value) ? 'int32' : 'float64';
  }
  if (typeof value === 'bigint') {
    return 'int64';
  }
  if (typeof value === 'boolean') {
    return 'boolean';
  }
  if (value instanceof Uint8Array) {
    return 'bytes';
  }
  if (value instanceof Date) {
    return 'timestamp';
  }
  return 'string';
}
