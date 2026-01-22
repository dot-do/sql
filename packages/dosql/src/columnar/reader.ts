/**
 * Columnar Chunk Manager - Read Path
 *
 * Supports:
 * - Projection pushdown (only read needed columns)
 * - Predicate pushdown (use zone maps to skip chunks)
 * - Efficient batch scanning
 */

import {
  type ColumnChunk,
  type RowGroup,
  type RowGroupMetadata,
  type ColumnStats,
  type ColumnDataType,
  type Encoding,
  type Predicate,
  type ReadRequest,
  type FSXInterface,
  isNumericType,
} from './types.js';

import {
  decodeRaw,
  decodeRawStrings,
  decodeDictionary,
  decodeRLE,
  decodeDelta,
  bitpackDecode,
  isNull,
} from './encoding.js';

import {
  deserializeRowGroup,
  extractMetadataFast,
  canSkipRowGroup,
} from './chunk.js';

// ============================================================================
// Reader Configuration
// ============================================================================

export interface ReaderConfig {
  /** Enable predicate pushdown using zone maps */
  enablePredicatePushdown?: boolean;

  /** Maximum number of row groups to scan in parallel */
  parallelScan?: number;

  /** Prefetch metadata for all row groups */
  prefetchMetadata?: boolean;
}

const DEFAULT_CONFIG: Required<ReaderConfig> = {
  enablePredicatePushdown: true,
  parallelScan: 4,
  prefetchMetadata: true,
};

// ============================================================================
// Read Result Types
// ============================================================================

export interface ReadResult {
  /** Column values by column name */
  columns: Map<string, (unknown | null)[]>;

  /** Total rows returned */
  rowCount: number;

  /** Row groups scanned */
  rowGroupsScanned: number;

  /** Row groups skipped by predicate pushdown */
  rowGroupsSkipped: number;
}

export interface ScanResult {
  /** Rows as objects */
  rows: Record<string, unknown>[];

  /** Total rows returned */
  rowCount: number;

  /** Statistics about the scan */
  stats: {
    rowGroupsScanned: number;
    rowGroupsSkipped: number;
    totalBytes: number;
  };
}

// ============================================================================
// Columnar Reader
// ============================================================================

export class ColumnarReader {
  private fsx: FSXInterface;
  private config: Required<ReaderConfig>;
  private metadataCache: Map<string, RowGroupMetadata> = new Map();

  constructor(fsx: FSXInterface, config?: ReaderConfig) {
    this.fsx = fsx;
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Read columnar data matching the request.
   */
  async read(request: ReadRequest): Promise<ReadResult> {
    const { table, projection, predicates, limit, offset } = request;

    // List available row groups
    const rowGroupKeys = await this.fsx.list(`${table}/rowgroups/`);

    // Load metadata and filter by predicates
    const eligibleGroups: { key: string; metadata: RowGroupMetadata }[] = [];
    let skippedCount = 0;

    for (const key of rowGroupKeys) {
      const metadata = await this.getMetadata(key);

      if (
        this.config.enablePredicatePushdown &&
        predicates &&
        predicates.length > 0 &&
        canSkipRowGroup(metadata, predicates)
      ) {
        skippedCount++;
        continue;
      }

      eligibleGroups.push({ key, metadata });
    }

    // Sort by row range
    eligibleGroups.sort((a, b) => a.metadata.rowRange.start - b.metadata.rowRange.start);

    // Read row groups
    const columns = new Map<string, unknown[]>();
    const columnNames = projection?.columns ?? [];
    let totalRows = 0;
    let currentOffset = offset ?? 0;
    let remaining = limit ?? Infinity;

    for (const { key, metadata } of eligibleGroups) {
      if (remaining <= 0) break;

      // Check if we need to skip rows for offset
      if (currentOffset >= metadata.rowCount) {
        currentOffset -= metadata.rowCount;
        continue;
      }

      // Read the row group
      const data = await this.fsx.get(key);
      if (!data) continue;

      const rowGroup = deserializeRowGroup(data);

      // Decode requested columns
      const decodedColumns = this.decodeColumns(
        rowGroup,
        columnNames.length > 0 ? columnNames : Array.from(rowGroup.columns.keys())
      );

      // Apply predicates to filter rows
      const matchingIndices = predicates
        ? this.filterRows(decodedColumns, predicates, rowGroup.rowCount)
        : Array.from({ length: rowGroup.rowCount }, (_, i) => i);

      // Apply offset and limit
      const startIndex = currentOffset;
      const endIndex = Math.min(matchingIndices.length, startIndex + remaining);

      for (let i = startIndex; i < endIndex; i++) {
        const rowIndex = matchingIndices[i];

        for (const [colName, values] of decodedColumns) {
          if (!columns.has(colName)) {
            columns.set(colName, []);
          }
          columns.get(colName)!.push(values[rowIndex]);
        }
        totalRows++;
        remaining--;
      }

      currentOffset = 0; // Offset only applies to first group
    }

    return {
      columns,
      rowCount: totalRows,
      rowGroupsScanned: eligibleGroups.length,
      rowGroupsSkipped: skippedCount,
    };
  }

  /**
   * Scan and return rows as objects.
   */
  async scan(request: ReadRequest): Promise<ScanResult> {
    const result = await this.read(request);

    // Convert columns to rows
    const rows: Record<string, unknown>[] = [];
    const columnNames = Array.from(result.columns.keys());

    for (let i = 0; i < result.rowCount; i++) {
      const row: Record<string, unknown> = {};
      for (const colName of columnNames) {
        row[colName] = result.columns.get(colName)![i];
      }
      rows.push(row);
    }

    return {
      rows,
      rowCount: result.rowCount,
      stats: {
        rowGroupsScanned: result.rowGroupsScanned,
        rowGroupsSkipped: result.rowGroupsSkipped,
        totalBytes: 0, // TODO: track bytes read
      },
    };
  }

  /**
   * Read a single row group directly.
   */
  async readRowGroup(
    data: Uint8Array,
    projection?: string[]
  ): Promise<Map<string, unknown[]>> {
    const rowGroup = deserializeRowGroup(data);
    return this.decodeColumns(
      rowGroup,
      projection ?? Array.from(rowGroup.columns.keys())
    );
  }

  /**
   * Get metadata for a row group (with caching).
   */
  private async getMetadata(key: string): Promise<RowGroupMetadata> {
    if (this.metadataCache.has(key)) {
      return this.metadataCache.get(key)!;
    }

    const data = await this.fsx.get(key);
    if (!data) {
      throw new Error(`Row group not found: ${key}`);
    }

    const metadata = extractMetadataFast(data);
    this.metadataCache.set(key, metadata);

    return metadata;
  }

  /**
   * Decode columns from a row group.
   */
  private decodeColumns(
    rowGroup: RowGroup,
    columnNames: string[]
  ): Map<string, unknown[]> {
    const result = new Map<string, unknown[]>();

    for (const name of columnNames) {
      const chunk = rowGroup.columns.get(name);
      if (!chunk) continue;

      const values = this.decodeColumn(chunk);
      result.set(name, values);
    }

    return result;
  }

  /**
   * Decode a single column chunk.
   */
  private decodeColumn(chunk: ColumnChunk): unknown[] {
    const { dataType, encoding, data, nullBitmap, rowCount } = chunk;

    switch (dataType) {
      case 'string':
        return this.decodeStringColumn(data, encoding, rowCount, nullBitmap);

      case 'int8':
      case 'int16':
      case 'int32':
      case 'int64':
      case 'uint8':
      case 'uint16':
      case 'uint32':
      case 'uint64':
      case 'timestamp':
        return this.decodeIntegerColumn(data, dataType, encoding, rowCount, nullBitmap);

      case 'float32':
      case 'float64':
        return this.decodeFloatColumn(data, dataType, rowCount, nullBitmap);

      case 'boolean':
        return this.decodeBooleanColumn(data, rowCount, nullBitmap);

      case 'bytes':
        return this.decodeBytesColumn(data, rowCount, nullBitmap);

      default:
        throw new Error(`Unsupported data type: ${dataType}`);
    }
  }

  /**
   * Decode a string column.
   */
  private decodeStringColumn(
    data: Uint8Array,
    encoding: Encoding,
    rowCount: number,
    nullBitmap: Uint8Array
  ): (string | null)[] {
    switch (encoding) {
      case 'dict':
        return decodeDictionary(data, rowCount, nullBitmap).values as (string | null)[];
      case 'raw':
      default:
        return decodeRawStrings(data, rowCount, nullBitmap).values as (string | null)[];
    }
  }

  /**
   * Decode an integer column.
   */
  private decodeIntegerColumn(
    data: Uint8Array,
    dataType: ColumnDataType,
    encoding: Encoding,
    rowCount: number,
    nullBitmap: Uint8Array
  ): (number | bigint | null)[] {
    switch (encoding) {
      case 'rle':
        return decodeRLE(data, dataType, rowCount, nullBitmap).values as (number | bigint | null)[];

      case 'delta':
        return decodeDelta(data, dataType, rowCount, nullBitmap).values as (number | bigint | null)[];

      case 'bitpack': {
        const bitWidth = data[0];
        const packedData = data.subarray(1);
        const unpacked = bitpackDecode(packedData, rowCount, bitWidth);

        const values: (number | null)[] = [];
        for (let i = 0; i < rowCount; i++) {
          values.push(isNull(nullBitmap, i) ? null : unpacked[i]);
        }
        return values;
      }

      case 'raw':
      default:
        return decodeRaw(data, dataType, rowCount, nullBitmap).values as (number | bigint | null)[];
    }
  }

  /**
   * Decode a float column.
   */
  private decodeFloatColumn(
    data: Uint8Array,
    dataType: ColumnDataType,
    rowCount: number,
    nullBitmap: Uint8Array
  ): (number | null)[] {
    return decodeRaw(data, dataType, rowCount, nullBitmap).values as (number | null)[];
  }

  /**
   * Decode a boolean column.
   */
  private decodeBooleanColumn(
    data: Uint8Array,
    rowCount: number,
    nullBitmap: Uint8Array
  ): (boolean | null)[] {
    return decodeRaw(data, 'boolean', rowCount, nullBitmap).values as (boolean | null)[];
  }

  /**
   * Decode a bytes column.
   */
  private decodeBytesColumn(
    data: Uint8Array,
    rowCount: number,
    nullBitmap: Uint8Array
  ): (Uint8Array | null)[] {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const values: (Uint8Array | null)[] = [];
    let offset = 0;

    for (let i = 0; i < rowCount; i++) {
      const len = view.getUint32(offset, true);
      offset += 4;

      if (isNull(nullBitmap, i) || len === 0) {
        values.push(null);
        offset += len;
      } else {
        values.push(data.slice(offset, offset + len));
        offset += len;
      }
    }

    return values;
  }

  /**
   * Filter rows based on predicates.
   */
  private filterRows(
    columns: Map<string, unknown[]>,
    predicates: Predicate[],
    rowCount: number
  ): number[] {
    const matchingIndices: number[] = [];

    for (let i = 0; i < rowCount; i++) {
      let matches = true;

      for (const predicate of predicates) {
        const colValues = columns.get(predicate.column);
        if (!colValues) {
          matches = false;
          break;
        }

        const value = colValues[i];
        if (!this.evaluatePredicate(value, predicate)) {
          matches = false;
          break;
        }
      }

      if (matches) {
        matchingIndices.push(i);
      }
    }

    return matchingIndices;
  }

  /**
   * Evaluate a predicate against a value.
   */
  private evaluatePredicate(value: unknown, predicate: Predicate): boolean {
    const { op, value: predicateValue, value2 } = predicate;

    // Handle null comparisons
    if (value === null) {
      return op === 'eq' && predicateValue === null;
    }

    if (predicateValue === null && op !== 'ne') {
      return false;
    }

    switch (op) {
      case 'eq':
        return value === predicateValue;

      case 'ne':
        return value !== predicateValue;

      case 'lt':
        return (value as number | string | bigint) < (predicateValue as number | string | bigint);

      case 'le':
        return (value as number | string | bigint) <= (predicateValue as number | string | bigint);

      case 'gt':
        return (value as number | string | bigint) > (predicateValue as number | string | bigint);

      case 'ge':
        return (value as number | string | bigint) >= (predicateValue as number | string | bigint);

      case 'between':
        return (
          value2 !== undefined &&
          (value as number | string | bigint) >= (predicateValue as number | string | bigint) &&
          (value as number | string | bigint) <= (value2 as number | string | bigint)
        );

      case 'in':
        return Array.isArray(predicateValue) && predicateValue.includes(value as number | bigint | string);

      default:
        return false;
    }
  }

  /**
   * Clear the metadata cache.
   */
  clearCache(): void {
    this.metadataCache.clear();
  }
}

// ============================================================================
// Convenience Functions
// ============================================================================

/**
 * Read columnar data from a single serialized row group.
 */
export function readColumnarChunk(
  data: Uint8Array,
  projection?: string[]
): Map<string, unknown[]> {
  const rowGroup = deserializeRowGroup(data);
  const reader = new ColumnarReader({
    get: async () => data,
    put: async () => {},
    delete: async () => {},
    list: async () => [],
  });

  const columnNames = projection ?? Array.from(rowGroup.columns.keys());
  const result = new Map<string, unknown[]>();

  for (const name of columnNames) {
    const chunk = rowGroup.columns.get(name);
    if (!chunk) continue;

    const values = (reader as any).decodeColumn(chunk);
    result.set(name, values);
  }

  return result;
}

/**
 * Get statistics for a column from serialized data.
 */
export function getColumnStats(data: Uint8Array, columnName: string): ColumnStats | null {
  const metadata = extractMetadataFast(data);
  return metadata.columnStats.get(columnName) ?? null;
}

/**
 * Check if predicates would match any rows based on zone maps.
 */
export function mightMatchPredicates(
  data: Uint8Array,
  predicates: Predicate[]
): boolean {
  const metadata = extractMetadataFast(data);
  return !canSkipRowGroup(metadata, predicates);
}

// ============================================================================
// Aggregation Support
// ============================================================================

/**
 * Calculate sum directly from column stats (no data scan needed).
 */
export function aggregateSumFromStats(
  metadataList: RowGroupMetadata[],
  columnName: string
): number | bigint | null {
  let sum: number | bigint = 0;
  let isBigInt = false;

  for (const metadata of metadataList) {
    const stats = metadata.columnStats.get(columnName);
    if (!stats || stats.sum === undefined) {
      return null; // Can't use stats for this aggregation
    }

    if (typeof stats.sum === 'bigint') {
      isBigInt = true;
      sum = BigInt(sum) + stats.sum;
    } else {
      if (isBigInt) {
        sum = (sum as bigint) + BigInt(stats.sum);
      } else {
        sum = (sum as number) + stats.sum;
      }
    }
  }

  return sum;
}

/**
 * Calculate count directly from metadata (no data scan needed).
 */
export function aggregateCountFromStats(
  metadataList: RowGroupMetadata[],
  columnName: string,
  countNulls: boolean = true
): number {
  let count = 0;

  for (const metadata of metadataList) {
    if (countNulls) {
      count += metadata.rowCount;
    } else {
      const stats = metadata.columnStats.get(columnName);
      count += metadata.rowCount - (stats?.nullCount ?? 0);
    }
  }

  return count;
}

/**
 * Calculate min/max directly from column stats.
 */
export function aggregateMinMaxFromStats(
  metadataList: RowGroupMetadata[],
  columnName: string
): { min: number | bigint | string | null; max: number | bigint | string | null } {
  let min: number | bigint | string | null = null;
  let max: number | bigint | string | null = null;

  for (const metadata of metadataList) {
    const stats = metadata.columnStats.get(columnName);
    if (!stats) continue;

    if (stats.min !== null) {
      if (min === null || stats.min < min) {
        min = stats.min;
      }
    }

    if (stats.max !== null) {
      if (max === null || stats.max > max) {
        max = stats.max;
      }
    }
  }

  return { min, max };
}
