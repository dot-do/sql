/**
 * R2 Single-File Index Format
 *
 * A single-file index format optimized for R2 byte-range requests.
 * Minimizes round-trips by embedding schema, indexes, bloom filters,
 * statistics, and data pointers in one file with offset-based access.
 *
 * @example
 * ```typescript
 * import { createIndexWriter, createR2IndexReader } from 'dosql/r2-index';
 *
 * // Build an index
 * const writer = createIndexWriter({
 *   tableName: 'users',
 *   columns: [
 *     { name: 'id', type: 'int64', nullable: false, ordinal: 0 },
 *     { name: 'email', type: 'string', nullable: false, ordinal: 1 },
 *   ],
 *   primaryKey: ['id'],
 *   indexedColumns: ['id', 'email'],
 *   bloomColumns: ['email'],
 * });
 *
 * for (const row of rows) {
 *   writer.addRow({
 *     id: { type: 'int', value: BigInt(row.id) },
 *     email: { type: 'string', value: row.email },
 *   }, BigInt(row.id));
 * }
 *
 * const indexBytes = writer.build();
 * await bucket.put('indexes/users.idx', indexBytes);
 *
 * // Read an index
 * const reader = createR2IndexReader(bucket, 'indexes/users.idx');
 * const header = await reader.getHeader();
 * const result = await reader.lookup('email', { type: 'string', value: 'user@example.com' });
 * ```
 */

// Types
export {
  // Constants
  R2_INDEX_MAGIC,
  R2_INDEX_VERSION,
  R2_INDEX_MIN_VERSION,
  R2_INDEX_HEADER_SIZE,
  R2_DEFAULT_PAGE_SIZE,
  R2_MAX_PAGE_SIZE,
  R2_DEFAULT_BLOOM_FP_RATE,
  R2_MAX_INLINE_VALUE_SIZE,

  // Enums
  R2IndexFlags,
  R2BTreePageType,

  // Header types
  type R2IndexHeader,
  type R2IndexSectionOffsets,
  type SectionPointer,

  // Schema types
  type R2IndexSchema,
  type R2IndexColumn,
  type R2IndexDataType,
  type SortColumn,

  // B-tree types
  type R2IndexBTreeSection,
  type R2BTreeIndex,
  type R2BTreePageInfo,
  type R2BTreePage,
  type R2BTreeEntry,

  // Bloom filter types
  type R2IndexBloomSection,
  type R2BloomFilter,
  type BloomFilterParams,

  // Statistics types
  type R2IndexStatsSection,
  type R2GlobalStats,
  type R2ChunkStats,
  type R2ColumnStats,

  // Data section types
  type R2IndexDataSection,
  type R2DataStorageType,
  type R2DataFile,
  type R2DataLocation,
  type R2DataFormat,
  type R2Compression,

  // Value types
  type R2IndexValue,

  // Query types
  type R2RangeRequest,
  type R2IndexHeaderReadResult,
  type R2IndexSectionReadResult,
  type R2IndexPredicate,
  type R2PredicateOp,
  type R2IndexQueryResult,
  type R2IndexMatch,
  type R2QueryStats,

  // Options types
  type R2IndexBuildOptions,
  type R2IndexReadOptions,

  // R2 interface types
  type R2Bucket,
  type R2GetOptions,
  type R2PutOptions,
  type R2ListOptions,
  type R2Object,
  type R2Objects,

  // Type guards and utilities
  isNumericType,
  isIntegerType,
  getTypeSize,
  compareValues,
  encodeValue,
  decodeValue,
} from './types.js';

// Writer
export {
  R2IndexWriter,
  createIndexWriter,
  buildIndex,
  BloomFilterBuilder,
  BTreePageBuilder,
  ColumnStatsBuilder,
  crc32,
  murmur3_32,
  calculateBloomParams,
} from './writer.js';

// Reader
export {
  R2IndexReader,
  createR2IndexReader,
  createIndexReaderFromContent,
  readIndexHeader,
  validateIndex,
} from './reader.js';
