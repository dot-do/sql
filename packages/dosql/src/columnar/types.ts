/**
 * Columnar Chunk Manager - Core Types
 *
 * Defines the types for columnar OLAP storage in DoSQL.
 * Supports multiple encodings and zone map filtering for efficient analytics queries.
 */

// ============================================================================
// Encoding Types
// ============================================================================

/**
 * Supported column encodings for compression and efficient storage.
 * - raw: Direct typed array storage (no compression)
 * - dict: Dictionary encoding for low-cardinality strings
 * - rle: Run-length encoding for repeated values
 * - delta: Delta encoding for sorted integers
 * - bitpack: Bit-packing for small integers
 */
export type Encoding = 'raw' | 'dict' | 'rle' | 'delta' | 'bitpack';

/**
 * Supported column data types.
 */
export type ColumnDataType =
  | 'int8'
  | 'int16'
  | 'int32'
  | 'int64'
  | 'uint8'
  | 'uint16'
  | 'uint32'
  | 'uint64'
  | 'float32'
  | 'float64'
  | 'boolean'
  | 'string'
  | 'bytes'
  | 'timestamp';

// ============================================================================
// Column Statistics
// ============================================================================

/**
 * Statistics for a column chunk, used for zone map filtering.
 * These stats enable predicate pushdown to skip entire chunks.
 */
export interface ColumnStats {
  /** Minimum value in the chunk (null if all values are null) */
  min: number | bigint | string | null;

  /** Maximum value in the chunk (null if all values are null) */
  max: number | bigint | string | null;

  /** Number of null values in the chunk */
  nullCount: number;

  /** Approximate distinct count (optional, for query optimization) */
  distinctCount?: number;

  /** Sum for numeric columns (optional, for aggregation pushdown) */
  sum?: number | bigint;
}

// ============================================================================
// Column Chunk
// ============================================================================

/**
 * A single column chunk containing encoded data for one column.
 * This is the atomic unit of columnar storage.
 */
export interface ColumnChunk {
  /** Column name in the schema */
  columnName: string;

  /** Data type of the column */
  dataType: ColumnDataType;

  /** Number of rows in this chunk */
  rowCount: number;

  /** Encoding used for the data */
  encoding: Encoding;

  /**
   * Null bitmap (1 bit per row).
   * Bit is 1 if value is NOT null, 0 if null.
   * Uses little-endian bit order within each byte.
   */
  nullBitmap: Uint8Array;

  /** Encoded column data */
  data: Uint8Array;

  /** Column statistics for zone map filtering */
  stats: ColumnStats;
}

/**
 * Dictionary metadata for dictionary-encoded columns.
 */
export interface DictionaryMetadata {
  /** Number of unique values in dictionary */
  size: number;

  /** Byte size of dictionary data */
  byteSize: number;

  /** Encoding used for indices (typically bitpack or raw) */
  indexEncoding: 'raw' | 'bitpack';

  /** Bits per index (for bitpack encoding) */
  bitsPerIndex?: number;
}

/**
 * RLE metadata for run-length encoded columns.
 */
export interface RLEMetadata {
  /** Number of runs */
  runCount: number;

  /** Encoding used for run lengths */
  lengthEncoding: 'raw' | 'bitpack';
}

/**
 * Delta metadata for delta-encoded columns.
 */
export interface DeltaMetadata {
  /** First value in the sequence */
  firstValue: number | bigint;

  /** Bit width for delta values */
  bitWidth: number;

  /** Whether deltas can be negative */
  signed: boolean;
}

/**
 * Extended column chunk with encoding-specific metadata.
 */
export interface ExtendedColumnChunk extends ColumnChunk {
  /** Dictionary metadata (when encoding = 'dict') */
  dictionaryMeta?: DictionaryMetadata;

  /** RLE metadata (when encoding = 'rle') */
  rleMeta?: RLEMetadata;

  /** Delta metadata (when encoding = 'delta') */
  deltaMeta?: DeltaMetadata;
}

// ============================================================================
// Row Group
// ============================================================================

/**
 * A row group is a horizontal partition of a table.
 * Contains all columns for a subset of rows.
 * Target size: ~1MB to fit within 2MB fsx blob limit with overhead.
 */
export interface RowGroup {
  /** Unique identifier for this row group */
  id: string;

  /** Number of rows in this row group */
  rowCount: number;

  /** Map of column name to column chunk */
  columns: Map<string, ColumnChunk>;

  /** Row range within the table (inclusive) */
  rowRange: {
    start: number;
    end: number;
  };

  /** Creation timestamp */
  createdAt: number;

  /** Byte size of the serialized row group */
  byteSize?: number;
}

/**
 * Metadata for a row group (stored separately for efficient scanning).
 */
export interface RowGroupMetadata {
  /** Row group ID */
  id: string;

  /** Number of rows */
  rowCount: number;

  /** Row range */
  rowRange: { start: number; end: number };

  /** Per-column statistics for zone map filtering */
  columnStats: Map<string, ColumnStats>;

  /** Byte size of the row group */
  byteSize: number;

  /** Creation timestamp */
  createdAt: number;
}

// ============================================================================
// Table Schema
// ============================================================================

/**
 * Column definition in the schema.
 */
export interface ColumnDefinition {
  /** Column name */
  name: string;

  /** Data type */
  dataType: ColumnDataType;

  /** Whether the column can contain nulls */
  nullable: boolean;

  /** Preferred encoding (auto-selected if not specified) */
  preferredEncoding?: Encoding;
}

/**
 * Table schema for columnar storage.
 */
export interface ColumnarTableSchema {
  /** Table name */
  tableName: string;

  /** Column definitions */
  columns: ColumnDefinition[];

  /** Primary key columns (for sorting within chunks) */
  primaryKey?: string[];

  /** Partition key columns */
  partitionBy?: string[];
}

// ============================================================================
// FSX Interface (assumed from context)
// ============================================================================

/**
 * Simplified FSX interface for blob storage.
 * Matches the fsx API used elsewhere in the codebase.
 */
export interface FSXInterface {
  /** Read a blob by key */
  get(key: string): Promise<Uint8Array | null>;

  /** Write a blob by key (max 2MB) */
  put(key: string, data: Uint8Array): Promise<void>;

  /** Delete a blob by key */
  delete(key: string): Promise<void>;

  /** List blobs by prefix */
  list(prefix: string): Promise<string[]>;
}

// ============================================================================
// Query Types
// ============================================================================

/**
 * Column projection for read operations.
 */
export interface ColumnProjection {
  /** Column names to read (empty = all columns) */
  columns: string[];
}

/**
 * Predicate for zone map filtering.
 */
export type PredicateOp = 'eq' | 'ne' | 'lt' | 'le' | 'gt' | 'ge' | 'in' | 'between';

export interface Predicate {
  /** Column name */
  column: string;

  /** Comparison operator */
  op: PredicateOp;

  /** Value(s) to compare against */
  value: number | bigint | string | null | (number | bigint | string)[];

  /** Second value for 'between' operator */
  value2?: number | bigint | string;
}

/**
 * Read request for columnar data.
 */
export interface ReadRequest {
  /** Table name */
  table: string;

  /** Column projection (optional, null = all columns) */
  projection?: ColumnProjection;

  /** Predicates for filtering (ANDed together) */
  predicates?: Predicate[];

  /** Row limit */
  limit?: number;

  /** Row offset */
  offset?: number;
}

/**
 * Write request for columnar data.
 */
export interface WriteRequest {
  /** Table name */
  table: string;

  /** Rows to write (array of objects) */
  rows: Record<string, unknown>[];
}

// ============================================================================
// Constants
// ============================================================================

/** Maximum size for a single fsx blob (2MB - some overhead) */
export const MAX_BLOB_SIZE = 2 * 1024 * 1024 - 1024; // 2MB - 1KB overhead

/** Target size for row groups */
export const TARGET_ROW_GROUP_SIZE = 1 * 1024 * 1024; // 1MB

/** Maximum rows per row group (for predictable chunk sizes) */
export const MAX_ROWS_PER_ROW_GROUP = 65536;

/** Minimum rows for dictionary encoding to be worthwhile */
export const MIN_ROWS_FOR_DICT = 100;

/** Dictionary encoding cardinality threshold (max unique/total ratio) */
export const DICT_CARDINALITY_THRESHOLD = 0.1;

// ============================================================================
// Type Guards
// ============================================================================

export function isNumericType(dataType: ColumnDataType): boolean {
  return [
    'int8', 'int16', 'int32', 'int64',
    'uint8', 'uint16', 'uint32', 'uint64',
    'float32', 'float64',
  ].includes(dataType);
}

export function isIntegerType(dataType: ColumnDataType): boolean {
  return [
    'int8', 'int16', 'int32', 'int64',
    'uint8', 'uint16', 'uint32', 'uint64',
  ].includes(dataType);
}

export function isSignedType(dataType: ColumnDataType): boolean {
  return ['int8', 'int16', 'int32', 'int64', 'float32', 'float64'].includes(dataType);
}

export function getBytesPerElement(dataType: ColumnDataType): number {
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
    case 'string':
    case 'bytes':
      return -1; // Variable length
    default:
      return -1;
  }
}
