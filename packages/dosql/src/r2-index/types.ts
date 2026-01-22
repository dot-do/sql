/**
 * R2 Single-File Index Format - Type Definitions
 *
 * A single-file index format optimized for R2 byte-range requests.
 * Minimizes round-trips by embedding everything needed for queries:
 * - Schema definition
 * - B-tree indexes with inline pages
 * - Bloom filters for negative lookups
 * - Column statistics (min/max/count/nulls) per chunk
 * - Data file pointers with byte offsets
 *
 * File Layout:
 * +-----------------------+
 * | Magic (8 bytes)       |  "DOSQLIDX"
 * +-----------------------+
 * | Version (4 bytes)     |  Format version (uint32 LE)
 * +-----------------------+
 * | Flags (4 bytes)       |  Feature flags (uint32 LE)
 * +-----------------------+
 * | Header Size (4 bytes) |  Total header + offset table size
 * +-----------------------+
 * | Schema Offset (8 B)   |  Offset to schema section
 * | Schema Size (4 B)     |  Size of schema section
 * +-----------------------+
 * | Index Offset (8 B)    |  Offset to B-tree index section
 * | Index Size (4 B)      |  Size of index section
 * +-----------------------+
 * | Bloom Offset (8 B)    |  Offset to bloom filter section
 * | Bloom Size (4 B)      |  Size of bloom section
 * +-----------------------+
 * | Stats Offset (8 B)    |  Offset to statistics section
 * | Stats Size (4 B)      |  Size of stats section
 * +-----------------------+
 * | Data Offset (8 B)     |  Offset to data pointers section
 * | Data Size (4 B)       |  Size of data section
 * +-----------------------+
 * | Reserved (32 bytes)   |  Future expansion
 * +-----------------------+
 * | Schema Section        |  JSON-encoded schema
 * +-----------------------+
 * | Index Section         |  B-tree pages with entries
 * +-----------------------+
 * | Bloom Section         |  Bloom filters per column
 * +-----------------------+
 * | Stats Section         |  Column statistics per chunk
 * +-----------------------+
 * | Data Section          |  Data file pointers
 * +-----------------------+
 * | Footer (8 bytes)      |  "DOSQLIDX" (for validation)
 * +-----------------------+
 *
 * All multi-byte integers are little-endian.
 * Offsets are absolute from file start.
 */

// =============================================================================
// MAGIC & VERSION
// =============================================================================

/** Magic bytes identifying a DoSQL R2 Index file */
export const R2_INDEX_MAGIC = 'DOSQLIDX';

/** Current format version */
export const R2_INDEX_VERSION = 1;

/** Minimum supported format version for reading */
export const R2_INDEX_MIN_VERSION = 1;

// =============================================================================
// FEATURE FLAGS
// =============================================================================

/**
 * Feature flags indicating which sections are present
 */
export const enum R2IndexFlags {
  /** No special features */
  NONE = 0,
  /** Bloom filters are present */
  HAS_BLOOM = 1 << 0,
  /** Column statistics are present */
  HAS_STATS = 1 << 1,
  /** B-tree indexes are present */
  HAS_BTREE = 1 << 2,
  /** Data is compressed with zstd */
  COMPRESSED_ZSTD = 1 << 3,
  /** Data is compressed with lz4 */
  COMPRESSED_LZ4 = 1 << 4,
  /** Checksums are present for sections */
  HAS_CHECKSUMS = 1 << 5,
}

// =============================================================================
// HEADER STRUCTURE
// =============================================================================

/**
 * Fixed-size header at the start of the index file.
 * Total size: 128 bytes (allows reading in a single small range request)
 */
export interface R2IndexHeader {
  /** Magic bytes: "DOSQLIDX" */
  magic: string;

  /** Format version number */
  version: number;

  /** Feature flags */
  flags: number;

  /** Total size of header (including offset table) */
  headerSize: number;

  /** Offset table for each section */
  sections: R2IndexSectionOffsets;

  /** Total file size (for validation) */
  totalSize: bigint;

  /** Creation timestamp (Unix ms) */
  createdAt: bigint;

  /** Number of indexed rows */
  rowCount: bigint;

  /** CRC32 checksum of header (excluding this field) */
  headerChecksum: number;
}

/**
 * Offset table for quick section lookup.
 * Each section has an offset (8 bytes) and size (4 bytes).
 */
export interface R2IndexSectionOffsets {
  /** Schema section */
  schema: SectionPointer;

  /** B-tree index section */
  index: SectionPointer;

  /** Bloom filter section */
  bloom: SectionPointer;

  /** Statistics section */
  stats: SectionPointer;

  /** Data pointers section */
  data: SectionPointer;
}

/**
 * Pointer to a section within the file
 */
export interface SectionPointer {
  /** Absolute byte offset from file start */
  offset: bigint;

  /** Size in bytes */
  size: number;

  /** CRC32 checksum of section (0 if HAS_CHECKSUMS not set) */
  checksum: number;
}

// =============================================================================
// SCHEMA SECTION
// =============================================================================

/**
 * Schema definition stored in the index.
 * JSON-encoded for simplicity and forward compatibility.
 */
export interface R2IndexSchema {
  /** Table name */
  tableName: string;

  /** Column definitions */
  columns: R2IndexColumn[];

  /** Primary key columns (for composite keys) */
  primaryKey: string[];

  /** Partition key columns (for partition pruning) */
  partitionBy?: string[];

  /** Sort key columns (for range query optimization) */
  sortBy?: SortColumn[];
}

/**
 * Column definition in the schema
 */
export interface R2IndexColumn {
  /** Column name */
  name: string;

  /** Data type */
  type: R2IndexDataType;

  /** Whether the column can contain nulls */
  nullable: boolean;

  /** Column ordinal position (0-based) */
  ordinal: number;
}

/**
 * Supported data types
 */
export type R2IndexDataType =
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
  | 'timestamp'
  | 'date'
  | 'uuid';

/**
 * Sort column specification
 */
export interface SortColumn {
  /** Column name */
  name: string;

  /** Sort direction */
  direction: 'asc' | 'desc';

  /** Nulls ordering */
  nulls?: 'first' | 'last';
}

// =============================================================================
// B-TREE INDEX SECTION
// =============================================================================

/**
 * B-tree index section containing one or more indexes.
 * Each index has its own set of pages stored contiguously.
 */
export interface R2IndexBTreeSection {
  /** Index definitions */
  indexes: R2BTreeIndex[];
}

/**
 * A single B-tree index
 */
export interface R2BTreeIndex {
  /** Index name */
  name: string;

  /** Indexed columns */
  columns: string[];

  /** Whether this is a unique index */
  unique: boolean;

  /** Root page offset (relative to index section start) */
  rootOffset: number;

  /** Total number of pages in this index */
  pageCount: number;

  /** Height of the B-tree */
  height: number;

  /** Total number of entries */
  entryCount: bigint;

  /** Page size for this index */
  pageSize: number;

  /** Page directory: array of page offsets for O(1) lookup */
  pageDirectory: R2BTreePageInfo[];
}

/**
 * Information about a single B-tree page
 */
export interface R2BTreePageInfo {
  /** Page ID */
  pageId: number;

  /** Offset relative to index section start */
  offset: number;

  /** Size of page in bytes */
  size: number;

  /** Page type */
  type: R2BTreePageType;

  /** First key in page (for binary search) */
  firstKey?: Uint8Array;

  /** Last key in page (for binary search) */
  lastKey?: Uint8Array;
}

/**
 * B-tree page types
 */
export const enum R2BTreePageType {
  /** Internal node with keys and child pointers */
  INTERNAL = 0x01,
  /** Leaf node with keys and values */
  LEAF = 0x02,
  /** Overflow page for large values */
  OVERFLOW = 0x03,
}

/**
 * B-tree page structure (serialized format)
 *
 * Page Layout:
 * +------------------------+
 * | Page Type (1 byte)     |
 * | Entry Count (2 bytes)  |
 * | Free Space (2 bytes)   |
 * | Next Leaf (4 bytes)    |  -1 if none (for leaf pages)
 * | Prev Leaf (4 bytes)    |  -1 if none (for leaf pages)
 * +------------------------+
 * | Key Offsets (2B each)  |  Array of offsets to keys
 * +------------------------+
 * | Value/Child Offsets    |  Array of value offsets (leaf) or child page IDs (internal)
 * +------------------------+
 * | Key/Value Data         |  Actual key and value bytes
 * +------------------------+
 */
export interface R2BTreePage {
  /** Page type */
  type: R2BTreePageType;

  /** Number of entries in the page */
  entryCount: number;

  /** Free space in bytes */
  freeSpace: number;

  /** Next leaf page ID (-1 if none) */
  nextLeaf: number;

  /** Previous leaf page ID (-1 if none) */
  prevLeaf: number;

  /** Keys in the page */
  keys: Uint8Array[];

  /** Values (leaf) or child page IDs (internal) */
  values: Uint8Array[];

  /** Child page IDs (internal pages only) */
  children: number[];
}

/**
 * B-tree entry (key-value pair)
 */
export interface R2BTreeEntry {
  /** Encoded key */
  key: Uint8Array;

  /** Encoded value (for leaf pages) */
  value: Uint8Array;
}

// =============================================================================
// BLOOM FILTER SECTION
// =============================================================================

/**
 * Bloom filter section for quick negative lookups.
 * One bloom filter per indexed column.
 */
export interface R2IndexBloomSection {
  /** Bloom filters by column name */
  filters: R2BloomFilter[];
}

/**
 * A single bloom filter
 */
export interface R2BloomFilter {
  /** Column name this filter is for */
  columnName: string;

  /** Number of hash functions (k) */
  numHashes: number;

  /** Size of bit array in bits (m) */
  numBits: number;

  /** Expected number of elements (n) */
  expectedElements: number;

  /** Actual number of elements inserted */
  insertedElements: number;

  /** False positive rate (calculated from actual fill) */
  falsePositiveRate: number;

  /** Bit array (packed as Uint8Array) */
  bits: Uint8Array;

  /** Hash seed for reproducibility */
  hashSeed: number;
}

/**
 * Bloom filter parameters for construction
 */
export interface BloomFilterParams {
  /** Expected number of elements */
  expectedElements: number;

  /** Desired false positive rate (0.0 - 1.0) */
  falsePositiveRate: number;

  /** Optional hash seed */
  hashSeed?: number;
}

// =============================================================================
// STATISTICS SECTION
// =============================================================================

/**
 * Statistics section containing column stats per data chunk.
 * Enables predicate pushdown and partition pruning.
 */
export interface R2IndexStatsSection {
  /** Global statistics for the entire dataset */
  global: R2GlobalStats;

  /** Per-chunk statistics */
  chunks: R2ChunkStats[];
}

/**
 * Global statistics across all data
 */
export interface R2GlobalStats {
  /** Total row count */
  rowCount: bigint;

  /** Total size in bytes */
  totalBytes: bigint;

  /** Number of data chunks */
  chunkCount: number;

  /** Per-column global statistics */
  columns: Map<string, R2ColumnStats>;
}

/**
 * Statistics for a single data chunk
 */
export interface R2ChunkStats {
  /** Chunk ID */
  chunkId: string;

  /** Row range in this chunk */
  rowRange: {
    start: bigint;
    end: bigint;
  };

  /** Byte range in the data file */
  byteRange: {
    offset: bigint;
    size: number;
  };

  /** Per-column statistics for this chunk */
  columns: Map<string, R2ColumnStats>;

  /** Partition values (if partitioned) */
  partitionValues?: Map<string, R2IndexValue>;
}

/**
 * Statistics for a single column
 */
export interface R2ColumnStats {
  /** Minimum value (null if all values are null) */
  min: R2IndexValue | null;

  /** Maximum value (null if all values are null) */
  max: R2IndexValue | null;

  /** Number of null values */
  nullCount: bigint;

  /** Number of distinct values (approximate) */
  distinctCount?: bigint;

  /** Sum for numeric columns */
  sum?: number | bigint;

  /** Average for numeric columns */
  avg?: number;

  /** Whether all values are the same */
  allSame?: boolean;

  /** Whether the column is sorted */
  sorted?: boolean;

  /** Sort direction if sorted */
  sortDirection?: 'asc' | 'desc';
}

/**
 * A value that can be stored in statistics.
 * Supports all R2IndexDataType values.
 */
export type R2IndexValue =
  | { type: 'null' }
  | { type: 'boolean'; value: boolean }
  | { type: 'int'; value: bigint }
  | { type: 'float'; value: number }
  | { type: 'string'; value: string }
  | { type: 'bytes'; value: Uint8Array }
  | { type: 'timestamp'; value: bigint }
  | { type: 'date'; value: number }
  | { type: 'uuid'; value: string };

// =============================================================================
// DATA POINTERS SECTION
// =============================================================================

/**
 * Data pointers section containing references to actual data files.
 * Supports both inline data and external R2 object references.
 */
export interface R2IndexDataSection {
  /** Data storage type */
  storageType: R2DataStorageType;

  /** Data files/chunks */
  files: R2DataFile[];

  /** Total data size across all files */
  totalSize: bigint;
}

/**
 * Data storage types
 */
export type R2DataStorageType =
  | 'inline'       // Data embedded in the index file
  | 'external-r2'  // Data in separate R2 objects
  | 'external-url'; // Data at external URLs

/**
 * Reference to a data file
 */
export interface R2DataFile {
  /** File/chunk ID */
  id: string;

  /** Storage location */
  location: R2DataLocation;

  /** Format of the data */
  format: R2DataFormat;

  /** Row range in this file */
  rowRange: {
    start: bigint;
    end: bigint;
  };

  /** File size in bytes */
  size: bigint;

  /** Compression used */
  compression?: R2Compression;

  /** Optional checksum */
  checksum?: {
    algorithm: 'crc32' | 'md5' | 'sha256';
    value: string;
  };
}

/**
 * Location of data
 */
export type R2DataLocation =
  | { type: 'inline'; offset: bigint; size: number }
  | { type: 'r2'; bucket: string; key: string }
  | { type: 'url'; url: string };

/**
 * Data format
 */
export type R2DataFormat =
  | 'parquet'
  | 'json'
  | 'ndjson'
  | 'csv'
  | 'avro'
  | 'arrow';

/**
 * Compression algorithms
 */
export type R2Compression =
  | 'none'
  | 'zstd'
  | 'lz4'
  | 'snappy'
  | 'gzip';

// =============================================================================
// QUERY TYPES
// =============================================================================

/**
 * Range request specification for reading index sections
 */
export interface R2RangeRequest {
  /** Start byte offset */
  start: bigint;

  /** End byte offset (exclusive) */
  end: bigint;
}

/**
 * Result of a header read operation
 */
export interface R2IndexHeaderReadResult {
  /** Parsed header */
  header: R2IndexHeader;

  /** Raw header bytes (for validation) */
  rawBytes: Uint8Array;

  /** Whether the header is valid */
  valid: boolean;

  /** Validation errors if any */
  errors?: string[];
}

/**
 * Result of a section read operation
 */
export interface R2IndexSectionReadResult<T> {
  /** Parsed section data */
  data: T;

  /** Bytes read */
  bytesRead: number;

  /** Whether checksum matched (if present) */
  checksumValid?: boolean;
}

/**
 * Query predicate for index lookup
 */
export interface R2IndexPredicate {
  /** Column name */
  column: string;

  /** Operator */
  op: R2PredicateOp;

  /** Value(s) to compare */
  value: R2IndexValue | R2IndexValue[];

  /** Second value for BETWEEN */
  value2?: R2IndexValue;
}

/**
 * Predicate operators
 */
export type R2PredicateOp =
  | 'eq'       // =
  | 'ne'       // !=
  | 'lt'       // <
  | 'le'       // <=
  | 'gt'       // >
  | 'ge'       // >=
  | 'in'       // IN (value1, value2, ...)
  | 'between'  // BETWEEN value AND value2
  | 'like'     // LIKE pattern
  | 'is_null'  // IS NULL
  | 'is_not_null'; // IS NOT NULL

/**
 * Result of an index query
 */
export interface R2IndexQueryResult {
  /** Matching row IDs or data file references */
  matches: R2IndexMatch[];

  /** Whether more results are available */
  hasMore: boolean;

  /** Continuation token for pagination */
  cursor?: string;

  /** Query statistics */
  stats: R2QueryStats;
}

/**
 * A single match from index lookup
 */
export interface R2IndexMatch {
  /** Row ID within the dataset */
  rowId: bigint;

  /** Data file containing this row */
  dataFile: R2DataFile;

  /** Byte offset within the data file (if known) */
  byteOffset?: bigint;

  /** Matched key value (for debugging) */
  key?: R2IndexValue;
}

/**
 * Query execution statistics
 */
export interface R2QueryStats {
  /** Number of index pages read */
  pagesRead: number;

  /** Total bytes read from R2 */
  bytesRead: bigint;

  /** Number of bloom filter checks */
  bloomChecks: number;

  /** Number of bloom filter hits (potential matches) */
  bloomHits: number;

  /** Number of chunks pruned by statistics */
  chunksPruned: number;

  /** Total execution time in milliseconds */
  executionTimeMs: number;

  /** Number of range requests made */
  rangeRequests: number;
}

// =============================================================================
// BUILDER OPTIONS
// =============================================================================

/**
 * Options for building an R2 index file
 */
export interface R2IndexBuildOptions {
  /** Table name */
  tableName: string;

  /** Schema columns */
  columns: R2IndexColumn[];

  /** Primary key columns */
  primaryKey: string[];

  /** Columns to build B-tree indexes on */
  indexedColumns?: string[];

  /** Columns to build bloom filters on */
  bloomColumns?: string[];

  /** Target B-tree page size */
  pageSize?: number;

  /** Bloom filter false positive rate */
  bloomFalsePositiveRate?: number;

  /** Whether to compute statistics */
  computeStats?: boolean;

  /** Compression for sections */
  compression?: R2Compression;

  /** Whether to compute checksums */
  computeChecksums?: boolean;
}

/**
 * Options for reading an R2 index file
 */
export interface R2IndexReadOptions {
  /** R2 bucket for range requests */
  bucket?: R2Bucket;

  /** R2 object key */
  key?: string;

  /** Pre-loaded file content (for testing/small files) */
  content?: Uint8Array;

  /** Whether to verify checksums */
  verifyChecksums?: boolean;

  /** Cache control */
  cache?: {
    /** Cache parsed sections */
    enabled: boolean;
    /** Maximum cache size in bytes */
    maxSize?: number;
  };
}

// =============================================================================
// R2 BUCKET INTERFACE (from Cloudflare Workers)
// =============================================================================

/**
 * R2 bucket interface (matches Cloudflare Workers R2)
 */
export interface R2Bucket {
  get(key: string, options?: R2GetOptions): Promise<R2Object | null>;
  head(key: string): Promise<R2Object | null>;
  put(key: string, value: ArrayBuffer | Uint8Array | string, options?: R2PutOptions): Promise<R2Object>;
  delete(key: string): Promise<void>;
  list(options?: R2ListOptions): Promise<R2Objects>;
}

/**
 * R2 get options
 */
export interface R2GetOptions {
  range?: {
    offset: number;
    length?: number;
  } | {
    suffix: number;
  };
}

/**
 * R2 put options
 */
export interface R2PutOptions {
  httpMetadata?: {
    contentType?: string;
    contentEncoding?: string;
  };
  customMetadata?: Record<string, string>;
}

/**
 * R2 list options
 */
export interface R2ListOptions {
  prefix?: string;
  limit?: number;
  cursor?: string;
}

/**
 * R2 object
 */
export interface R2Object {
  key: string;
  size: number;
  httpMetadata?: {
    contentType?: string;
    contentEncoding?: string;
  };
  customMetadata?: Record<string, string>;
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
}

/**
 * R2 list result
 */
export interface R2Objects {
  objects: R2Object[];
  truncated: boolean;
  cursor?: string;
}

// =============================================================================
// SIZE CONSTANTS
// =============================================================================

/** Header fixed size (128 bytes) */
export const R2_INDEX_HEADER_SIZE = 128;

/** Default B-tree page size (16KB) */
export const R2_DEFAULT_PAGE_SIZE = 16 * 1024;

/** Maximum B-tree page size (256KB) */
export const R2_MAX_PAGE_SIZE = 256 * 1024;

/** Default bloom filter false positive rate */
export const R2_DEFAULT_BLOOM_FP_RATE = 0.01;

/** Maximum inline value size in B-tree */
export const R2_MAX_INLINE_VALUE_SIZE = 4096;

// =============================================================================
// TYPE GUARDS
// =============================================================================

/**
 * Check if a value is a numeric type
 */
export function isNumericType(type: R2IndexDataType): boolean {
  return [
    'int8', 'int16', 'int32', 'int64',
    'uint8', 'uint16', 'uint32', 'uint64',
    'float32', 'float64',
  ].includes(type);
}

/**
 * Check if a value is an integer type
 */
export function isIntegerType(type: R2IndexDataType): boolean {
  return [
    'int8', 'int16', 'int32', 'int64',
    'uint8', 'uint16', 'uint32', 'uint64',
  ].includes(type);
}

/**
 * Get the byte size for a fixed-size type
 */
export function getTypeSize(type: R2IndexDataType): number {
  switch (type) {
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
    case 'date':
      return 4;
    case 'int64':
    case 'uint64':
    case 'float64':
    case 'timestamp':
      return 8;
    case 'uuid':
      return 16;
    case 'string':
    case 'bytes':
      return -1; // Variable length
    default:
      return -1;
  }
}

/**
 * Compare two R2IndexValue instances
 */
export function compareValues(a: R2IndexValue, b: R2IndexValue): number {
  if (a.type === 'null' && b.type === 'null') return 0;
  if (a.type === 'null') return -1;
  if (b.type === 'null') return 1;

  if (a.type !== b.type) {
    // Type mismatch - compare type names for stable ordering
    return a.type.localeCompare(b.type);
  }

  switch (a.type) {
    case 'boolean':
      return (a.value ? 1 : 0) - ((b as typeof a).value ? 1 : 0);
    case 'int':
      return Number((a.value) - ((b as typeof a).value));
    case 'float':
      return a.value - (b as typeof a).value;
    case 'string':
    case 'uuid':
      return a.value.localeCompare((b as typeof a).value);
    case 'timestamp':
      return Number(a.value - (b as typeof a).value);
    case 'date':
      return a.value - (b as typeof a).value;
    case 'bytes': {
      const aBytes = a.value;
      const bBytes = (b as typeof a).value;
      const minLen = Math.min(aBytes.length, bBytes.length);
      for (let i = 0; i < minLen; i++) {
        if (aBytes[i] !== bBytes[i]) {
          return aBytes[i] - bBytes[i];
        }
      }
      return aBytes.length - bBytes.length;
    }
    default:
      return 0;
  }
}

/**
 * Encode a value to bytes for index storage
 */
export function encodeValue(value: R2IndexValue): Uint8Array {
  const encoder = new TextEncoder();

  switch (value.type) {
    case 'null':
      return new Uint8Array([0]);
    case 'boolean':
      return new Uint8Array([1, value.value ? 1 : 0]);
    case 'int': {
      const buffer = new ArrayBuffer(9);
      const view = new DataView(buffer);
      view.setUint8(0, 2);
      view.setBigInt64(1, value.value, true);
      return new Uint8Array(buffer);
    }
    case 'float': {
      const buffer = new ArrayBuffer(9);
      const view = new DataView(buffer);
      view.setUint8(0, 3);
      view.setFloat64(1, value.value, true);
      return new Uint8Array(buffer);
    }
    case 'string': {
      const strBytes = encoder.encode(value.value);
      const result = new Uint8Array(5 + strBytes.length);
      const view = new DataView(result.buffer);
      view.setUint8(0, 4);
      view.setUint32(1, strBytes.length, true);
      result.set(strBytes, 5);
      return result;
    }
    case 'bytes': {
      const result = new Uint8Array(5 + value.value.length);
      const view = new DataView(result.buffer);
      view.setUint8(0, 5);
      view.setUint32(1, value.value.length, true);
      result.set(value.value, 5);
      return result;
    }
    case 'timestamp': {
      const buffer = new ArrayBuffer(9);
      const view = new DataView(buffer);
      view.setUint8(0, 6);
      view.setBigInt64(1, value.value, true);
      return new Uint8Array(buffer);
    }
    case 'date': {
      const buffer = new ArrayBuffer(5);
      const view = new DataView(buffer);
      view.setUint8(0, 7);
      view.setInt32(1, value.value, true);
      return new Uint8Array(buffer);
    }
    case 'uuid': {
      const uuidBytes = encoder.encode(value.value);
      const result = new Uint8Array(1 + uuidBytes.length);
      result[0] = 8;
      result.set(uuidBytes, 1);
      return result;
    }
    default:
      throw new Error(`Unknown value type: ${(value as R2IndexValue).type}`);
  }
}

/**
 * Decode bytes to a value
 */
export function decodeValue(bytes: Uint8Array): R2IndexValue {
  const decoder = new TextDecoder();
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const typeTag = view.getUint8(0);

  switch (typeTag) {
    case 0:
      return { type: 'null' };
    case 1:
      return { type: 'boolean', value: view.getUint8(1) === 1 };
    case 2:
      return { type: 'int', value: view.getBigInt64(1, true) };
    case 3:
      return { type: 'float', value: view.getFloat64(1, true) };
    case 4: {
      const len = view.getUint32(1, true);
      return { type: 'string', value: decoder.decode(bytes.slice(5, 5 + len)) };
    }
    case 5: {
      const len = view.getUint32(1, true);
      return { type: 'bytes', value: bytes.slice(5, 5 + len) };
    }
    case 6:
      return { type: 'timestamp', value: view.getBigInt64(1, true) };
    case 7:
      return { type: 'date', value: view.getInt32(1, true) };
    case 8:
      return { type: 'uuid', value: decoder.decode(bytes.slice(1)) };
    default:
      throw new Error(`Unknown type tag: ${typeTag}`);
  }
}
