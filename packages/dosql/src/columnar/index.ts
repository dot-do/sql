/**
 * DoSQL Columnar Chunk Manager
 *
 * A columnar OLAP storage engine for analytics queries.
 * Supports multiple encodings, zone map filtering, and projection/predicate pushdown.
 *
 * @packageDocumentation
 */

// ============================================================================
// Types
// ============================================================================

export {
  // Encoding types
  type Encoding,
  type ColumnDataType,

  // Column and stats types
  type ColumnStats,
  type ColumnChunk,
  type ExtendedColumnChunk,
  type DictionaryMetadata,
  type RLEMetadata,
  type DeltaMetadata,

  // Row group types
  type RowGroup,
  type RowGroupMetadata,

  // Schema types
  type ColumnDefinition,
  type ColumnarTableSchema,

  // Query types
  type ColumnProjection,
  type PredicateOp,
  type Predicate,
  type ReadRequest,
  type WriteRequest,

  // FSX interface
  type FSXInterface,

  // Constants
  MAX_BLOB_SIZE,
  TARGET_ROW_GROUP_SIZE,
  MAX_ROWS_PER_ROW_GROUP,
  MIN_ROWS_FOR_DICT,
  DICT_CARDINALITY_THRESHOLD,

  // Type guards
  isNumericType,
  isIntegerType,
  isSignedType,
  getBytesPerElement,
} from './types.js';

// ============================================================================
// Encoding
// ============================================================================

export {
  // Encoding result types
  type EncodingResult,
  type DecodingResult,
  type DictionaryEncodingMetadata,
  type RLERun,

  // Raw encoding
  encodeRaw,
  decodeRaw,
  encodeRawStrings,
  decodeRawStrings,

  // Dictionary encoding
  encodeDictionary,
  decodeDictionary,

  // RLE encoding
  encodeRLE,
  decodeRLE,

  // Delta encoding
  encodeDelta,
  decodeDelta,

  // Bit-packing
  bitpackEncode,
  bitpackDecode,

  // Null bitmap
  createNullBitmap,
  isNull,
  setNull,
  setNotNull,

  // Encoding analysis
  type EncodingAnalysis,
  analyzeForEncoding,
  calculateStats,
} from './encoding.js';

// ============================================================================
// Chunk Operations
// ============================================================================

export {
  // Serialization
  serializeRowGroup,
  deserializeRowGroup,

  // Zone map filtering
  canSkipChunk,
  canSkipRowGroup,

  // Metadata
  extractMetadata,
  extractMetadataFast,

  // Size utilities
  estimateRowGroupSize,
  wouldExceedTargetSize,

  // ID generation
  generateRowGroupId,
} from './chunk.js';

// ============================================================================
// Writer
// ============================================================================

export {
  // Configuration
  type WriterConfig,

  // Writer class
  ColumnarWriter,

  // Convenience functions
  writeColumnar,
  inferSchema,
} from './writer.js';

// ============================================================================
// Reader
// ============================================================================

export {
  // Configuration
  type ReaderConfig,

  // Result types
  type ReadResult,
  type ScanResult,

  // Reader class
  ColumnarReader,

  // Convenience functions
  readColumnarChunk,
  getColumnStats,
  mightMatchPredicates,

  // Aggregation from stats
  aggregateSumFromStats,
  aggregateCountFromStats,
  aggregateMinMaxFromStats,
} from './reader.js';
