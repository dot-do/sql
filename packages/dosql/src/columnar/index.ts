/**
 * DoSQL Columnar Chunk Manager
 *
 * A columnar OLAP storage engine for analytics queries.
 * Supports multiple encodings, zone map filtering, and projection/predicate pushdown.
 *
 * @example OLAP Query with Projection Pushdown
 * ```typescript
 * import {
 *   ColumnarWriter,
 *   ColumnarReader,
 *   type ColumnarTableSchema,
 *   type ReadRequest,
 * } from '@dotdo/dosql/columnar';
 *
 * // Define schema for sales analytics
 * const schema: ColumnarTableSchema = {
 *   columns: [
 *     { name: 'order_id', type: 'int64' },
 *     { name: 'product_id', type: 'int32' },
 *     { name: 'customer_id', type: 'int32' },
 *     { name: 'quantity', type: 'int32' },
 *     { name: 'unit_price', type: 'float64' },
 *     { name: 'region', type: 'string' },
 *     { name: 'order_date', type: 'int64' }, // Unix timestamp
 *   ],
 * };
 *
 * // Write columnar data with automatic encoding selection
 * const writer = new ColumnarWriter(fsx, schema, {
 *   targetRowGroupSize: 64 * 1024, // 64KB row groups
 *   autoSelectEncoding: true,       // Dictionary, RLE, or Delta based on data
 * });
 *
 * await writer.write({
 *   tableName: 'sales',
 *   rows: salesData,
 * });
 *
 * // OLAP query with projection pushdown - only reads 'region' and 'quantity' columns
 * // Predicate pushdown uses zone maps to skip row groups where region != 'WEST'
 * const reader = new ColumnarReader(fsx, schema);
 *
 * const request: ReadRequest = {
 *   tableName: 'sales',
 *   // Projection pushdown: only load these columns from storage
 *   projection: ['region', 'quantity'],
 *   // Predicate pushdown: skip row groups using zone map min/max stats
 *   predicates: [
 *     { column: 'region', op: 'eq', value: 'WEST' },
 *     { column: 'quantity', op: 'gte', value: 10 },
 *   ],
 * };
 *
 * const result = await reader.scan(request);
 *
 * // Aggregate directly from column stats when possible (no row scan needed)
 * const totalQuantity = aggregateSumFromStats(result.metadata, 'quantity');
 * const orderCount = aggregateCountFromStats(result.metadata);
 * const { min: minQty, max: maxQty } = aggregateMinMaxFromStats(result.metadata, 'quantity');
 *
 * console.log(`WEST region: ${orderCount} orders, ${totalQuantity} units (${minQty}-${maxQty} per order)`);
 * ```
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
