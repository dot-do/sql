/**
 * Columnar Chunk Manager - Chunk Operations
 *
 * Handles serialization/deserialization of row groups.
 * Ensures chunks fit within the 2MB fsx blob limit.
 * Supports zone map filtering using min/max statistics.
 */

import {
  type ColumnChunk,
  type RowGroup,
  type RowGroupMetadata,
  type ColumnStats,
  type ColumnDataType,
  type Encoding,
  type Predicate,
  type PredicateOp,
  MAX_BLOB_SIZE,
  TARGET_ROW_GROUP_SIZE,
} from './types.js';

// ============================================================================
// Serialization Format
// ============================================================================

/**
 * Row Group Binary Format:
 *
 * Header (fixed size):
 *   [magic:4]           "DSCG" (DoSQL Columnar Group)
 *   [version:2]         Format version (1)
 *   [flags:2]           Reserved flags
 *   [rowCount:4]        Number of rows
 *   [columnCount:4]     Number of columns
 *   [rowStart:4]        Start row index
 *   [rowEnd:4]          End row index
 *   [createdAt:8]       Creation timestamp (ms since epoch)
 *   [idLength:2]        Length of ID string
 *   [id:var]            Row group ID (UTF-8)
 *
 * Column Directory (per column):
 *   [nameLength:2]      Column name length
 *   [name:var]          Column name (UTF-8)
 *   [dataType:1]        Data type enum
 *   [encoding:1]        Encoding enum
 *   [dataOffset:4]      Offset to column data
 *   [dataLength:4]      Length of column data
 *   [nullBitmapOffset:4] Offset to null bitmap
 *   [nullBitmapLength:4] Length of null bitmap
 *   [statsOffset:4]     Offset to stats
 *   [statsLength:4]     Length of stats
 *
 * Column Data (per column):
 *   [nullBitmap:var]    Null bitmap
 *   [data:var]          Encoded column data
 *   [stats:var]         Serialized stats
 */

const MAGIC = new TextEncoder().encode('DSCG');
const FORMAT_VERSION = 1;

// Data type to byte mapping
const DATA_TYPE_MAP: Record<ColumnDataType, number> = {
  int8: 0,
  int16: 1,
  int32: 2,
  int64: 3,
  uint8: 4,
  uint16: 5,
  uint32: 6,
  uint64: 7,
  float32: 8,
  float64: 9,
  boolean: 10,
  string: 11,
  bytes: 12,
  timestamp: 13,
};

const DATA_TYPE_REVERSE: ColumnDataType[] = [
  'int8', 'int16', 'int32', 'int64',
  'uint8', 'uint16', 'uint32', 'uint64',
  'float32', 'float64', 'boolean', 'string', 'bytes', 'timestamp',
];

const ENCODING_MAP: Record<Encoding, number> = {
  raw: 0,
  dict: 1,
  rle: 2,
  delta: 3,
  bitpack: 4,
};

const ENCODING_REVERSE: Encoding[] = ['raw', 'dict', 'rle', 'delta', 'bitpack'];

// ============================================================================
// Serialization
// ============================================================================

/**
 * Serialize a row group to binary format.
 */
export function serializeRowGroup(rowGroup: RowGroup): Uint8Array {
  const encoder = new TextEncoder();
  const idBytes = encoder.encode(rowGroup.id);
  const columnNames = Array.from(rowGroup.columns.keys());

  // Calculate header size
  let headerSize = 4 + 2 + 2 + 4 + 4 + 4 + 4 + 8 + 2 + idBytes.length;

  // Calculate column directory size
  const columnDirEntries: {
    nameBytes: Uint8Array;
    chunk: ColumnChunk;
  }[] = [];

  let columnDirSize = 0;
  for (const name of columnNames) {
    const chunk = rowGroup.columns.get(name)!;
    const nameBytes = encoder.encode(name);
    columnDirEntries.push({ nameBytes, chunk });
    columnDirSize += 2 + nameBytes.length + 1 + 1 + 4 + 4 + 4 + 4 + 4 + 4;
  }

  // Calculate data size and offsets
  let dataOffset = headerSize + columnDirSize;
  const columnOffsets: {
    dataOffset: number;
    dataLength: number;
    nullBitmapOffset: number;
    nullBitmapLength: number;
    statsOffset: number;
    statsLength: number;
  }[] = [];

  for (const { chunk } of columnDirEntries) {
    const statsBytes = serializeStats(chunk.stats);

    const entry = {
      nullBitmapOffset: dataOffset,
      nullBitmapLength: chunk.nullBitmap.length,
      dataOffset: dataOffset + chunk.nullBitmap.length,
      dataLength: chunk.data.length,
      statsOffset: dataOffset + chunk.nullBitmap.length + chunk.data.length,
      statsLength: statsBytes.length,
    };
    columnOffsets.push(entry);

    dataOffset += chunk.nullBitmap.length + chunk.data.length + statsBytes.length;
  }

  const totalSize = dataOffset;

  // Check size limit
  if (totalSize > MAX_BLOB_SIZE) {
    throw new Error(
      `Row group size ${totalSize} exceeds maximum blob size ${MAX_BLOB_SIZE}`
    );
  }

  // Allocate buffer
  const buffer = new Uint8Array(totalSize);
  const view = new DataView(buffer.buffer);
  let offset = 0;

  // Write header
  buffer.set(MAGIC, offset);
  offset += 4;

  view.setUint16(offset, FORMAT_VERSION, true);
  offset += 2;

  view.setUint16(offset, 0, true); // flags
  offset += 2;

  view.setUint32(offset, rowGroup.rowCount, true);
  offset += 4;

  view.setUint32(offset, columnNames.length, true);
  offset += 4;

  view.setUint32(offset, rowGroup.rowRange.start, true);
  offset += 4;

  view.setUint32(offset, rowGroup.rowRange.end, true);
  offset += 4;

  // Write timestamp as two 32-bit values (for compatibility)
  const timestamp = rowGroup.createdAt;
  view.setUint32(offset, timestamp & 0xffffffff, true);
  offset += 4;
  view.setUint32(offset, Math.floor(timestamp / 0x100000000), true);
  offset += 4;

  view.setUint16(offset, idBytes.length, true);
  offset += 2;

  buffer.set(idBytes, offset);
  offset += idBytes.length;

  // Write column directory
  for (let i = 0; i < columnDirEntries.length; i++) {
    const { nameBytes, chunk } = columnDirEntries[i];
    const offsets = columnOffsets[i];

    view.setUint16(offset, nameBytes.length, true);
    offset += 2;

    buffer.set(nameBytes, offset);
    offset += nameBytes.length;

    view.setUint8(offset, DATA_TYPE_MAP[chunk.dataType]);
    offset += 1;

    view.setUint8(offset, ENCODING_MAP[chunk.encoding]);
    offset += 1;

    view.setUint32(offset, offsets.dataOffset, true);
    offset += 4;

    view.setUint32(offset, offsets.dataLength, true);
    offset += 4;

    view.setUint32(offset, offsets.nullBitmapOffset, true);
    offset += 4;

    view.setUint32(offset, offsets.nullBitmapLength, true);
    offset += 4;

    view.setUint32(offset, offsets.statsOffset, true);
    offset += 4;

    view.setUint32(offset, offsets.statsLength, true);
    offset += 4;
  }

  // Write column data
  for (let i = 0; i < columnDirEntries.length; i++) {
    const { chunk } = columnDirEntries[i];
    const statsBytes = serializeStats(chunk.stats);

    buffer.set(chunk.nullBitmap, columnOffsets[i].nullBitmapOffset);
    buffer.set(chunk.data, columnOffsets[i].dataOffset);
    buffer.set(statsBytes, columnOffsets[i].statsOffset);
  }

  return buffer;
}

/**
 * Deserialize a row group from binary format.
 */
export function deserializeRowGroup(data: Uint8Array): RowGroup {
  const decoder = new TextDecoder();
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let offset = 0;

  // Verify magic
  const magic = data.subarray(offset, offset + 4);
  if (
    magic[0] !== MAGIC[0] ||
    magic[1] !== MAGIC[1] ||
    magic[2] !== MAGIC[2] ||
    magic[3] !== MAGIC[3]
  ) {
    throw new Error('Invalid row group format: bad magic bytes');
  }
  offset += 4;

  // Read version
  const version = view.getUint16(offset, true);
  if (version !== FORMAT_VERSION) {
    throw new Error(`Unsupported row group format version: ${version}`);
  }
  offset += 2;

  // Skip flags
  offset += 2;

  // Read header
  const rowCount = view.getUint32(offset, true);
  offset += 4;

  const columnCount = view.getUint32(offset, true);
  offset += 4;

  const rowStart = view.getUint32(offset, true);
  offset += 4;

  const rowEnd = view.getUint32(offset, true);
  offset += 4;

  // Read timestamp
  const timestampLow = view.getUint32(offset, true);
  offset += 4;
  const timestampHigh = view.getUint32(offset, true);
  offset += 4;
  const createdAt = timestampLow + timestampHigh * 0x100000000;

  // Read ID
  const idLength = view.getUint16(offset, true);
  offset += 2;

  const id = decoder.decode(data.subarray(offset, offset + idLength));
  offset += idLength;

  // Read column directory
  const columns = new Map<string, ColumnChunk>();

  for (let i = 0; i < columnCount; i++) {
    const nameLength = view.getUint16(offset, true);
    offset += 2;

    const columnName = decoder.decode(data.subarray(offset, offset + nameLength));
    offset += nameLength;

    const dataType = DATA_TYPE_REVERSE[view.getUint8(offset)];
    offset += 1;

    const encoding = ENCODING_REVERSE[view.getUint8(offset)];
    offset += 1;

    const dataOffset = view.getUint32(offset, true);
    offset += 4;

    const dataLength = view.getUint32(offset, true);
    offset += 4;

    const nullBitmapOffset = view.getUint32(offset, true);
    offset += 4;

    const nullBitmapLength = view.getUint32(offset, true);
    offset += 4;

    const statsOffset = view.getUint32(offset, true);
    offset += 4;

    const statsLength = view.getUint32(offset, true);
    offset += 4;

    // Read column data
    const nullBitmap = data.slice(nullBitmapOffset, nullBitmapOffset + nullBitmapLength);
    const columnData = data.slice(dataOffset, dataOffset + dataLength);
    const statsData = data.subarray(statsOffset, statsOffset + statsLength);
    const stats = deserializeStats(statsData);

    columns.set(columnName, {
      columnName,
      dataType,
      rowCount,
      encoding,
      nullBitmap,
      data: columnData,
      stats,
    });
  }

  return {
    id,
    rowCount,
    columns,
    rowRange: { start: rowStart, end: rowEnd },
    createdAt,
    byteSize: data.length,
  };
}

// ============================================================================
// Stats Serialization
// ============================================================================

/**
 * Serialize column statistics.
 */
function serializeStats(stats: ColumnStats): Uint8Array {
  const encoder = new TextEncoder();

  // Determine types
  const minType = typeof stats.min;
  const maxType = typeof stats.max;

  // Calculate size
  let size = 1 + 1 + 4 + 4; // flags + reserved + nullCount + distinctCount

  // Min/max sizes
  if (minType === 'number') size += 8;
  else if (minType === 'bigint') size += 8;
  else if (minType === 'string') size += 4 + encoder.encode(stats.min as string).length;
  else size += 0; // null

  if (maxType === 'number') size += 8;
  else if (maxType === 'bigint') size += 8;
  else if (maxType === 'string') size += 4 + encoder.encode(stats.max as string).length;
  else size += 0; // null

  // Sum (optional)
  if (stats.sum !== undefined) {
    size += 1 + 8; // flag + value
  }

  const buffer = new Uint8Array(size);
  const view = new DataView(buffer.buffer);
  let offset = 0;

  // Flags byte: bits 0-1 = min type, bits 2-3 = max type, bit 4 = has sum
  const typeCode = (t: string | null) => {
    if (t === 'number') return 1;
    if (t === 'bigint') return 2;
    if (t === 'string') return 3;
    return 0; // null
  };

  let flags =
    typeCode(minType === 'object' ? null : minType) |
    (typeCode(maxType === 'object' ? null : maxType) << 2);
  if (stats.sum !== undefined) flags |= 0x10;

  view.setUint8(offset, flags);
  offset += 1;

  view.setUint8(offset, 0); // reserved
  offset += 1;

  view.setUint32(offset, stats.nullCount, true);
  offset += 4;

  view.setUint32(offset, stats.distinctCount ?? 0, true);
  offset += 4;

  // Write min
  if (minType === 'number') {
    view.setFloat64(offset, stats.min as number, true);
    offset += 8;
  } else if (minType === 'bigint') {
    view.setBigInt64(offset, stats.min as bigint, true);
    offset += 8;
  } else if (minType === 'string') {
    const bytes = encoder.encode(stats.min as string);
    view.setUint32(offset, bytes.length, true);
    offset += 4;
    buffer.set(bytes, offset);
    offset += bytes.length;
  }

  // Write max
  if (maxType === 'number') {
    view.setFloat64(offset, stats.max as number, true);
    offset += 8;
  } else if (maxType === 'bigint') {
    view.setBigInt64(offset, stats.max as bigint, true);
    offset += 8;
  } else if (maxType === 'string') {
    const bytes = encoder.encode(stats.max as string);
    view.setUint32(offset, bytes.length, true);
    offset += 4;
    buffer.set(bytes, offset);
    offset += bytes.length;
  }

  // Write sum
  if (stats.sum !== undefined) {
    view.setUint8(offset, typeof stats.sum === 'bigint' ? 1 : 0);
    offset += 1;
    if (typeof stats.sum === 'bigint') {
      view.setBigInt64(offset, stats.sum, true);
    } else {
      view.setFloat64(offset, stats.sum, true);
    }
    offset += 8;
  }

  return buffer;
}

/**
 * Deserialize column statistics.
 */
function deserializeStats(data: Uint8Array): ColumnStats {
  const decoder = new TextDecoder();
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let offset = 0;

  const flags = view.getUint8(offset);
  offset += 1;

  offset += 1; // skip reserved

  const nullCount = view.getUint32(offset, true);
  offset += 4;

  const distinctCount = view.getUint32(offset, true);
  offset += 4;

  const minType = flags & 0x03;
  const maxType = (flags >> 2) & 0x03;
  const hasSum = (flags & 0x10) !== 0;

  // Read min
  let min: number | bigint | string | null = null;
  if (minType === 1) {
    min = view.getFloat64(offset, true);
    offset += 8;
  } else if (minType === 2) {
    min = view.getBigInt64(offset, true);
    offset += 8;
  } else if (minType === 3) {
    const len = view.getUint32(offset, true);
    offset += 4;
    min = decoder.decode(data.subarray(offset, offset + len));
    offset += len;
  }

  // Read max
  let max: number | bigint | string | null = null;
  if (maxType === 1) {
    max = view.getFloat64(offset, true);
    offset += 8;
  } else if (maxType === 2) {
    max = view.getBigInt64(offset, true);
    offset += 8;
  } else if (maxType === 3) {
    const len = view.getUint32(offset, true);
    offset += 4;
    max = decoder.decode(data.subarray(offset, offset + len));
    offset += len;
  }

  const stats: ColumnStats = { min, max, nullCount, distinctCount };

  // Read sum
  if (hasSum) {
    const sumType = view.getUint8(offset);
    offset += 1;
    if (sumType === 1) {
      stats.sum = view.getBigInt64(offset, true);
    } else {
      stats.sum = view.getFloat64(offset, true);
    }
  }

  return stats;
}

// ============================================================================
// Zone Map Filtering
// ============================================================================

/**
 * Check if a chunk can be skipped based on zone map statistics.
 * Returns true if the chunk CANNOT contain matching rows.
 */
export function canSkipChunk(stats: ColumnStats, predicate: Predicate): boolean {
  const { min, max } = stats;

  // Can't skip if min/max are null (unknown range)
  if (min === null || max === null) {
    return false;
  }

  const { op, value, value2 } = predicate;

  switch (op) {
    case 'eq':
      // Skip if value is outside [min, max]
      return value !== null && (value < min || value > max);

    case 'ne':
      // Skip only if all values are equal to the predicate value
      return min === max && min === value;

    case 'lt':
      // Skip if all values >= predicate value
      return value !== null && min >= value;

    case 'le':
      // Skip if all values > predicate value
      return value !== null && min > value;

    case 'gt':
      // Skip if all values <= predicate value
      return value !== null && max <= value;

    case 'ge':
      // Skip if all values < predicate value
      return value !== null && max < value;

    case 'between':
      // Skip if range doesn't overlap with [value, value2]
      if (value === null || value2 === undefined) return false;
      return max < value || min > value2;

    case 'in':
      // Skip if no value in the list overlaps with [min, max]
      if (!Array.isArray(value)) return false;
      return value.every((v) => v < min || v > max);

    default:
      return false;
  }
}

/**
 * Check if a row group can be skipped based on predicates.
 */
export function canSkipRowGroup(
  metadata: RowGroupMetadata,
  predicates: Predicate[]
): boolean {
  for (const predicate of predicates) {
    const stats = metadata.columnStats.get(predicate.column);
    if (stats && canSkipChunk(stats, predicate)) {
      return true;
    }
  }
  return false;
}

// ============================================================================
// Metadata Extraction
// ============================================================================

/**
 * Extract metadata from a row group (for efficient scanning without full deserialize).
 */
export function extractMetadata(rowGroup: RowGroup): RowGroupMetadata {
  const columnStats = new Map<string, ColumnStats>();

  for (const [name, chunk] of rowGroup.columns) {
    columnStats.set(name, chunk.stats);
  }

  return {
    id: rowGroup.id,
    rowCount: rowGroup.rowCount,
    rowRange: rowGroup.rowRange,
    columnStats,
    byteSize: rowGroup.byteSize ?? 0,
    createdAt: rowGroup.createdAt,
  };
}

/**
 * Quickly extract metadata from serialized row group without full deserialize.
 * Reads only the header and column stats.
 */
export function extractMetadataFast(data: Uint8Array): RowGroupMetadata {
  const decoder = new TextDecoder();
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let offset = 0;

  // Skip magic and version checks for speed
  offset += 4 + 2 + 2;

  const rowCount = view.getUint32(offset, true);
  offset += 4;

  const columnCount = view.getUint32(offset, true);
  offset += 4;

  const rowStart = view.getUint32(offset, true);
  offset += 4;

  const rowEnd = view.getUint32(offset, true);
  offset += 4;

  const timestampLow = view.getUint32(offset, true);
  offset += 4;
  const timestampHigh = view.getUint32(offset, true);
  offset += 4;
  const createdAt = timestampLow + timestampHigh * 0x100000000;

  const idLength = view.getUint16(offset, true);
  offset += 2;

  const id = decoder.decode(data.subarray(offset, offset + idLength));
  offset += idLength;

  // Read column stats
  const columnStats = new Map<string, ColumnStats>();

  for (let i = 0; i < columnCount; i++) {
    const nameLength = view.getUint16(offset, true);
    offset += 2;

    const columnName = decoder.decode(data.subarray(offset, offset + nameLength));
    offset += nameLength;

    // Skip dataType, encoding, offsets to get to stats
    offset += 1 + 1 + 4 + 4 + 4 + 4;

    const statsOffset = view.getUint32(offset, true);
    offset += 4;

    const statsLength = view.getUint32(offset, true);
    offset += 4;

    const statsData = data.subarray(statsOffset, statsOffset + statsLength);
    columnStats.set(columnName, deserializeStats(statsData));
  }

  return {
    id,
    rowCount,
    rowRange: { start: rowStart, end: rowEnd },
    columnStats,
    byteSize: data.length,
    createdAt,
  };
}

// ============================================================================
// Size Estimation
// ============================================================================

/**
 * Estimate the serialized size of a row group.
 */
export function estimateRowGroupSize(rowGroup: RowGroup): number {
  const encoder = new TextEncoder();
  const idBytes = encoder.encode(rowGroup.id);

  // Header size
  let size = 4 + 2 + 2 + 4 + 4 + 4 + 4 + 8 + 2 + idBytes.length;

  // Column directory and data
  for (const [name, chunk] of rowGroup.columns) {
    const nameBytes = encoder.encode(name);

    // Directory entry
    size += 2 + nameBytes.length + 1 + 1 + 4 + 4 + 4 + 4 + 4 + 4;

    // Data
    size += chunk.nullBitmap.length + chunk.data.length;

    // Stats (estimate)
    size += 50; // Approximate stats size
  }

  return size;
}

/**
 * Check if adding more rows would exceed the target size.
 */
export function wouldExceedTargetSize(
  currentSize: number,
  additionalRows: number,
  avgBytesPerRow: number
): boolean {
  const estimatedNewSize = currentSize + additionalRows * avgBytesPerRow;
  return estimatedNewSize > TARGET_ROW_GROUP_SIZE;
}

// ============================================================================
// Row Group ID Generation
// ============================================================================

/**
 * Generate a unique row group ID.
 */
export function generateRowGroupId(tableName: string, sequence: number): string {
  const timestamp = Date.now().toString(36);
  const seq = sequence.toString(36).padStart(4, '0');
  return `${tableName}_${timestamp}_${seq}`;
}
