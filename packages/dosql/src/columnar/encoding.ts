/**
 * Columnar Chunk Manager - Column Encodings
 *
 * Implements various encoding strategies for columnar data:
 * - Raw: Direct typed array storage
 * - Dictionary: For low-cardinality strings
 * - RLE: Run-length for repeated values
 * - Delta: For sorted/sequential integers
 * - Bitpack: For small integers
 */

import {
  type ColumnDataType,
  type Encoding,
  type ColumnStats,
  DICT_CARDINALITY_THRESHOLD,
  MIN_ROWS_FOR_DICT,
  isIntegerType,
  isNumericType,
  getBytesPerElement,
} from './types.js';

// ============================================================================
// Encoding Result Types
// ============================================================================

export interface EncodingResult {
  encoding: Encoding;
  data: Uint8Array;
  metadata?: Record<string, unknown>;
}

export interface DecodingResult {
  values: (number | bigint | string | boolean | null)[];
}

// ============================================================================
// Raw Encoding
// ============================================================================

/**
 * Encode numeric values as raw typed array.
 */
export function encodeRaw(
  values: (number | bigint | boolean | null)[],
  dataType: ColumnDataType,
  nullBitmap: Uint8Array
): EncodingResult {
  const nonNullCount = values.filter((v) => v !== null).length;

  switch (dataType) {
    case 'int8': {
      const buffer = new Int8Array(values.length);
      for (let i = 0; i < values.length; i++) {
        buffer[i] = values[i] !== null ? Number(values[i]) : 0;
      }
      return { encoding: 'raw', data: new Uint8Array(buffer.buffer) };
    }
    case 'uint8': {
      const buffer = new Uint8Array(values.length);
      for (let i = 0; i < values.length; i++) {
        buffer[i] = values[i] !== null ? Number(values[i]) : 0;
      }
      return { encoding: 'raw', data: buffer };
    }
    case 'int16': {
      const buffer = new Int16Array(values.length);
      for (let i = 0; i < values.length; i++) {
        buffer[i] = values[i] !== null ? Number(values[i]) : 0;
      }
      return { encoding: 'raw', data: new Uint8Array(buffer.buffer) };
    }
    case 'uint16': {
      const buffer = new Uint16Array(values.length);
      for (let i = 0; i < values.length; i++) {
        buffer[i] = values[i] !== null ? Number(values[i]) : 0;
      }
      return { encoding: 'raw', data: new Uint8Array(buffer.buffer) };
    }
    case 'int32': {
      const buffer = new Int32Array(values.length);
      for (let i = 0; i < values.length; i++) {
        buffer[i] = values[i] !== null ? Number(values[i]) : 0;
      }
      return { encoding: 'raw', data: new Uint8Array(buffer.buffer) };
    }
    case 'uint32': {
      const buffer = new Uint32Array(values.length);
      for (let i = 0; i < values.length; i++) {
        buffer[i] = values[i] !== null ? Number(values[i]) : 0;
      }
      return { encoding: 'raw', data: new Uint8Array(buffer.buffer) };
    }
    case 'int64':
    case 'timestamp': {
      const buffer = new BigInt64Array(values.length);
      for (let i = 0; i < values.length; i++) {
        buffer[i] = values[i] !== null ? BigInt(values[i] as number | bigint) : 0n;
      }
      return { encoding: 'raw', data: new Uint8Array(buffer.buffer) };
    }
    case 'uint64': {
      const buffer = new BigUint64Array(values.length);
      for (let i = 0; i < values.length; i++) {
        buffer[i] = values[i] !== null ? BigInt(values[i] as number | bigint) : 0n;
      }
      return { encoding: 'raw', data: new Uint8Array(buffer.buffer) };
    }
    case 'float32': {
      const buffer = new Float32Array(values.length);
      for (let i = 0; i < values.length; i++) {
        buffer[i] = values[i] !== null ? Number(values[i]) : 0;
      }
      return { encoding: 'raw', data: new Uint8Array(buffer.buffer) };
    }
    case 'float64': {
      const buffer = new Float64Array(values.length);
      for (let i = 0; i < values.length; i++) {
        buffer[i] = values[i] !== null ? Number(values[i]) : 0;
      }
      return { encoding: 'raw', data: new Uint8Array(buffer.buffer) };
    }
    case 'boolean': {
      const buffer = new Uint8Array(values.length);
      for (let i = 0; i < values.length; i++) {
        buffer[i] = values[i] !== null && values[i] ? 1 : 0;
      }
      return { encoding: 'raw', data: buffer };
    }
    default:
      throw new Error(`Unsupported data type for raw encoding: ${dataType}`);
  }
}

/**
 * Decode raw typed array to values.
 */
export function decodeRaw(
  data: Uint8Array,
  dataType: ColumnDataType,
  rowCount: number,
  nullBitmap: Uint8Array
): DecodingResult {
  const values: (number | bigint | boolean | null)[] = [];

  switch (dataType) {
    case 'int8': {
      const buffer = new Int8Array(data.buffer, data.byteOffset, rowCount);
      for (let i = 0; i < rowCount; i++) {
        values.push(isNull(nullBitmap, i) ? null : buffer[i]);
      }
      break;
    }
    case 'uint8': {
      for (let i = 0; i < rowCount; i++) {
        values.push(isNull(nullBitmap, i) ? null : data[i]);
      }
      break;
    }
    case 'int16': {
      const buffer = new Int16Array(data.buffer, data.byteOffset, rowCount);
      for (let i = 0; i < rowCount; i++) {
        values.push(isNull(nullBitmap, i) ? null : buffer[i]);
      }
      break;
    }
    case 'uint16': {
      const buffer = new Uint16Array(data.buffer, data.byteOffset, rowCount);
      for (let i = 0; i < rowCount; i++) {
        values.push(isNull(nullBitmap, i) ? null : buffer[i]);
      }
      break;
    }
    case 'int32': {
      const buffer = new Int32Array(data.buffer, data.byteOffset, rowCount);
      for (let i = 0; i < rowCount; i++) {
        values.push(isNull(nullBitmap, i) ? null : buffer[i]);
      }
      break;
    }
    case 'uint32': {
      const buffer = new Uint32Array(data.buffer, data.byteOffset, rowCount);
      for (let i = 0; i < rowCount; i++) {
        values.push(isNull(nullBitmap, i) ? null : buffer[i]);
      }
      break;
    }
    case 'int64':
    case 'timestamp': {
      const buffer = new BigInt64Array(data.buffer, data.byteOffset, rowCount);
      for (let i = 0; i < rowCount; i++) {
        values.push(isNull(nullBitmap, i) ? null : buffer[i]);
      }
      break;
    }
    case 'uint64': {
      const buffer = new BigUint64Array(data.buffer, data.byteOffset, rowCount);
      for (let i = 0; i < rowCount; i++) {
        values.push(isNull(nullBitmap, i) ? null : buffer[i]);
      }
      break;
    }
    case 'float32': {
      const buffer = new Float32Array(data.buffer, data.byteOffset, rowCount);
      for (let i = 0; i < rowCount; i++) {
        values.push(isNull(nullBitmap, i) ? null : buffer[i]);
      }
      break;
    }
    case 'float64': {
      const buffer = new Float64Array(data.buffer, data.byteOffset, rowCount);
      for (let i = 0; i < rowCount; i++) {
        values.push(isNull(nullBitmap, i) ? null : buffer[i]);
      }
      break;
    }
    case 'boolean': {
      for (let i = 0; i < rowCount; i++) {
        values.push(isNull(nullBitmap, i) ? null : data[i] !== 0);
      }
      break;
    }
    default:
      throw new Error(`Unsupported data type for raw decoding: ${dataType}`);
  }

  return { values };
}

// ============================================================================
// String Encoding (Raw with length prefixes)
// ============================================================================

/**
 * Encode strings as length-prefixed UTF-8.
 */
export function encodeRawStrings(values: (string | null)[]): EncodingResult {
  const encoder = new TextEncoder();

  // First pass: calculate total size
  let totalSize = 0;
  const encoded: Uint8Array[] = [];

  for (const value of values) {
    if (value !== null) {
      const bytes = encoder.encode(value);
      encoded.push(bytes);
      totalSize += 4 + bytes.length; // 4 bytes for length prefix
    } else {
      encoded.push(new Uint8Array(0));
      totalSize += 4; // Still need length prefix (0)
    }
  }

  // Second pass: write to buffer
  const buffer = new Uint8Array(totalSize);
  const view = new DataView(buffer.buffer);
  let offset = 0;

  for (let i = 0; i < values.length; i++) {
    const bytes = encoded[i];
    view.setUint32(offset, bytes.length, true); // Little-endian
    offset += 4;
    buffer.set(bytes, offset);
    offset += bytes.length;
  }

  return { encoding: 'raw', data: buffer };
}

/**
 * Decode length-prefixed strings.
 */
export function decodeRawStrings(
  data: Uint8Array,
  rowCount: number,
  nullBitmap: Uint8Array
): DecodingResult {
  const decoder = new TextDecoder();
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const values: (string | null)[] = [];
  let offset = 0;

  for (let i = 0; i < rowCount; i++) {
    if (isNull(nullBitmap, i)) {
      values.push(null);
      // Skip the length prefix
      const len = view.getUint32(offset, true);
      offset += 4 + len;
    } else {
      const len = view.getUint32(offset, true);
      offset += 4;
      const bytes = data.subarray(offset, offset + len);
      values.push(decoder.decode(bytes));
      offset += len;
    }
  }

  return { values };
}

// ============================================================================
// Dictionary Encoding
// ============================================================================

export interface DictionaryEncodingMetadata {
  dictionary: string[];
  bitsPerIndex: number;
}

/**
 * Encode strings using dictionary encoding.
 * Good for low-cardinality columns (many repeated values).
 */
export function encodeDictionary(
  values: (string | null)[],
  nullBitmap: Uint8Array
): EncodingResult {
  // Build dictionary
  const dict: string[] = [];
  const dictMap = new Map<string, number>();

  for (const value of values) {
    if (value !== null && !dictMap.has(value)) {
      dictMap.set(value, dict.length);
      dict.push(value);
    }
  }

  // Calculate bits needed for indices
  const bitsPerIndex = dict.length <= 1 ? 1 : Math.ceil(Math.log2(dict.length));

  // Encode dictionary
  const encoder = new TextEncoder();
  const encodedDict: Uint8Array[] = dict.map((s) => encoder.encode(s));
  const dictSize = encodedDict.reduce((sum, bytes) => sum + 4 + bytes.length, 0);

  // Encode indices using bit-packing
  const indicesBuffer = bitpackEncode(
    values.map((v) => (v !== null ? dictMap.get(v)! : 0)),
    bitsPerIndex
  );

  // Combine: [dictCount:4][dict...][indices...]
  const totalSize = 4 + dictSize + indicesBuffer.length;
  const buffer = new Uint8Array(totalSize);
  const view = new DataView(buffer.buffer);

  let offset = 0;
  view.setUint32(offset, dict.length, true);
  offset += 4;

  for (const bytes of encodedDict) {
    view.setUint32(offset, bytes.length, true);
    offset += 4;
    buffer.set(bytes, offset);
    offset += bytes.length;
  }

  buffer.set(indicesBuffer, offset);

  return {
    encoding: 'dict',
    data: buffer,
    metadata: { dictionary: dict, bitsPerIndex },
  };
}

/**
 * Decode dictionary-encoded strings.
 */
export function decodeDictionary(
  data: Uint8Array,
  rowCount: number,
  nullBitmap: Uint8Array
): DecodingResult {
  const decoder = new TextDecoder();
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  let offset = 0;

  // Read dictionary
  const dictCount = view.getUint32(offset, true);
  offset += 4;

  const dict: string[] = [];
  for (let i = 0; i < dictCount; i++) {
    const len = view.getUint32(offset, true);
    offset += 4;
    const bytes = data.subarray(offset, offset + len);
    dict.push(decoder.decode(bytes));
    offset += len;
  }

  // Calculate bits per index
  const bitsPerIndex = dictCount <= 1 ? 1 : Math.ceil(Math.log2(dictCount));

  // Decode indices
  const indicesData = data.subarray(offset);
  const indices = bitpackDecode(indicesData, rowCount, bitsPerIndex);

  // Map indices to values
  const values: (string | null)[] = [];
  for (let i = 0; i < rowCount; i++) {
    if (isNull(nullBitmap, i)) {
      values.push(null);
    } else {
      values.push(dict[indices[i]]);
    }
  }

  return { values };
}

// ============================================================================
// Run-Length Encoding (RLE)
// ============================================================================

export interface RLERun {
  value: number | bigint | string;
  count: number;
}

/**
 * Encode values using run-length encoding.
 * Good for sorted or repeated consecutive values.
 */
export function encodeRLE(
  values: (number | bigint | null)[],
  dataType: ColumnDataType,
  nullBitmap: Uint8Array
): EncodingResult {
  if (values.length === 0) {
    return { encoding: 'rle', data: new Uint8Array(0) };
  }

  // Build runs
  const runs: { value: number | bigint; count: number }[] = [];
  let currentValue = values[0];
  let currentCount = 1;

  for (let i = 1; i < values.length; i++) {
    const value = values[i];
    if (
      value === currentValue ||
      (isNull(nullBitmap, i) && isNull(nullBitmap, i - 1))
    ) {
      currentCount++;
    } else {
      runs.push({ value: currentValue ?? 0, count: currentCount });
      currentValue = value;
      currentCount = 1;
    }
  }
  runs.push({ value: currentValue ?? 0, count: currentCount });

  // Encode runs: [runCount:4][value:N][count:4]...
  const bytesPerValue = getBytesPerElement(dataType);
  const runDataSize = runs.length * (bytesPerValue + 4);
  const buffer = new Uint8Array(4 + runDataSize);
  const view = new DataView(buffer.buffer);

  let offset = 0;
  view.setUint32(offset, runs.length, true);
  offset += 4;

  for (const run of runs) {
    // Write value
    if (dataType === 'int64' || dataType === 'timestamp') {
      view.setBigInt64(offset, BigInt(run.value), true);
    } else if (dataType === 'uint64') {
      view.setBigUint64(offset, BigInt(run.value), true);
    } else if (dataType === 'float64') {
      view.setFloat64(offset, Number(run.value), true);
    } else if (dataType === 'float32') {
      view.setFloat32(offset, Number(run.value), true);
    } else if (dataType === 'int32') {
      view.setInt32(offset, Number(run.value), true);
    } else if (dataType === 'uint32') {
      view.setUint32(offset, Number(run.value), true);
    } else if (dataType === 'int16') {
      view.setInt16(offset, Number(run.value), true);
    } else if (dataType === 'uint16') {
      view.setUint16(offset, Number(run.value), true);
    } else if (dataType === 'int8') {
      view.setInt8(offset, Number(run.value));
    } else {
      view.setUint8(offset, Number(run.value));
    }
    offset += bytesPerValue;

    // Write count
    view.setUint32(offset, run.count, true);
    offset += 4;
  }

  return {
    encoding: 'rle',
    data: buffer,
    metadata: { runCount: runs.length },
  };
}

/**
 * Decode RLE-encoded values.
 */
export function decodeRLE(
  data: Uint8Array,
  dataType: ColumnDataType,
  rowCount: number,
  nullBitmap: Uint8Array
): DecodingResult {
  if (data.length === 0) {
    return { values: [] };
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const bytesPerValue = getBytesPerElement(dataType);

  let offset = 0;
  const runCount = view.getUint32(offset, true);
  offset += 4;

  const values: (number | bigint | null)[] = [];
  let rowIndex = 0;

  for (let r = 0; r < runCount; r++) {
    // Read value
    let value: number | bigint;
    if (dataType === 'int64' || dataType === 'timestamp') {
      value = view.getBigInt64(offset, true);
    } else if (dataType === 'uint64') {
      value = view.getBigUint64(offset, true);
    } else if (dataType === 'float64') {
      value = view.getFloat64(offset, true);
    } else if (dataType === 'float32') {
      value = view.getFloat32(offset, true);
    } else if (dataType === 'int32') {
      value = view.getInt32(offset, true);
    } else if (dataType === 'uint32') {
      value = view.getUint32(offset, true);
    } else if (dataType === 'int16') {
      value = view.getInt16(offset, true);
    } else if (dataType === 'uint16') {
      value = view.getUint16(offset, true);
    } else if (dataType === 'int8') {
      value = view.getInt8(offset);
    } else {
      value = view.getUint8(offset);
    }
    offset += bytesPerValue;

    // Read count
    const count = view.getUint32(offset, true);
    offset += 4;

    // Expand run
    for (let i = 0; i < count && rowIndex < rowCount; i++, rowIndex++) {
      values.push(isNull(nullBitmap, rowIndex) ? null : value);
    }
  }

  return { values };
}

// ============================================================================
// Delta Encoding
// ============================================================================

/**
 * Encode sorted integers using delta encoding.
 * Stores first value + deltas, which can then be bit-packed.
 */
export function encodeDelta(
  values: (number | bigint | null)[],
  dataType: ColumnDataType,
  nullBitmap: Uint8Array
): EncodingResult {
  if (values.length === 0) {
    return { encoding: 'delta', data: new Uint8Array(0) };
  }

  // Extract non-null values for delta calculation
  const nonNullValues: (number | bigint)[] = [];
  for (let i = 0; i < values.length; i++) {
    if (!isNull(nullBitmap, i) && values[i] !== null) {
      nonNullValues.push(values[i]!);
    }
  }

  if (nonNullValues.length === 0) {
    return { encoding: 'delta', data: new Uint8Array(0) };
  }

  // Calculate deltas
  const firstValue = nonNullValues[0];
  const deltas: number[] = [];
  let minDelta = 0;
  let maxDelta = 0;

  for (let i = 1; i < nonNullValues.length; i++) {
    const delta = Number(nonNullValues[i]) - Number(nonNullValues[i - 1]);
    deltas.push(delta);
    minDelta = Math.min(minDelta, delta);
    maxDelta = Math.max(maxDelta, delta);
  }

  // Determine if we need signed deltas
  const signed = minDelta < 0;
  const range = maxDelta - minDelta;
  const bitWidth = range === 0 ? 1 : Math.ceil(Math.log2(range + 1));

  // Encode: [firstValue:8][signed:1][bitWidth:1][minDelta:4][deltas...]
  const deltasBuffer = bitpackEncode(
    deltas.map((d) => d - minDelta), // Normalize to positive
    bitWidth
  );

  const buffer = new Uint8Array(14 + deltasBuffer.length);
  const view = new DataView(buffer.buffer);

  view.setBigInt64(0, BigInt(firstValue), true);
  view.setUint8(8, signed ? 1 : 0);
  view.setUint8(9, bitWidth);
  view.setInt32(10, minDelta, true);
  buffer.set(deltasBuffer, 14);

  return {
    encoding: 'delta',
    data: buffer,
    metadata: { firstValue, bitWidth, signed, minDelta },
  };
}

/**
 * Decode delta-encoded values.
 */
export function decodeDelta(
  data: Uint8Array,
  dataType: ColumnDataType,
  rowCount: number,
  nullBitmap: Uint8Array
): DecodingResult {
  if (data.length === 0) {
    return { values: new Array(rowCount).fill(null) };
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  const firstValue = view.getBigInt64(0, true);
  const signed = view.getUint8(8) === 1;
  const bitWidth = view.getUint8(9);
  const minDelta = view.getInt32(10, true);

  // Count non-null values
  let nonNullCount = 0;
  for (let i = 0; i < rowCount; i++) {
    if (!isNull(nullBitmap, i)) nonNullCount++;
  }

  // Decode deltas
  const deltasData = data.subarray(14);
  const normalizedDeltas = bitpackDecode(deltasData, nonNullCount - 1, bitWidth);
  const deltas = normalizedDeltas.map((d) => d + minDelta);

  // Reconstruct values
  const nonNullValues: bigint[] = [firstValue];
  let current = firstValue;
  for (const delta of deltas) {
    current += BigInt(delta);
    nonNullValues.push(current);
  }

  // Map back with nulls
  const values: (number | bigint | null)[] = [];
  let nonNullIndex = 0;
  for (let i = 0; i < rowCount; i++) {
    if (isNull(nullBitmap, i)) {
      values.push(null);
    } else {
      const val = nonNullValues[nonNullIndex++];
      // Convert to appropriate type
      if (dataType === 'int64' || dataType === 'timestamp' || dataType === 'uint64') {
        values.push(val);
      } else {
        values.push(Number(val));
      }
    }
  }

  return { values };
}

// ============================================================================
// Bit-Packing
// ============================================================================

/**
 * Pack integers into minimal bits.
 */
export function bitpackEncode(values: number[], bitsPerValue: number): Uint8Array {
  if (values.length === 0) {
    return new Uint8Array(0);
  }

  const totalBits = values.length * bitsPerValue;
  const totalBytes = Math.ceil(totalBits / 8);
  const buffer = new Uint8Array(totalBytes);

  let bitOffset = 0;
  for (const value of values) {
    writeBits(buffer, bitOffset, value, bitsPerValue);
    bitOffset += bitsPerValue;
  }

  return buffer;
}

/**
 * Unpack bit-packed integers.
 */
export function bitpackDecode(
  data: Uint8Array,
  count: number,
  bitsPerValue: number
): number[] {
  const values: number[] = [];
  let bitOffset = 0;

  for (let i = 0; i < count; i++) {
    values.push(readBits(data, bitOffset, bitsPerValue));
    bitOffset += bitsPerValue;
  }

  return values;
}

// ============================================================================
// Bit Manipulation Helpers
// ============================================================================

function writeBits(
  buffer: Uint8Array,
  bitOffset: number,
  value: number,
  numBits: number
): void {
  let remaining = numBits;
  let val = value;

  while (remaining > 0) {
    const byteIndex = Math.floor(bitOffset / 8);
    const bitInByte = bitOffset % 8;
    const bitsToWrite = Math.min(remaining, 8 - bitInByte);
    const mask = (1 << bitsToWrite) - 1;

    buffer[byteIndex] |= (val & mask) << bitInByte;

    val >>>= bitsToWrite;
    bitOffset += bitsToWrite;
    remaining -= bitsToWrite;
  }
}

function readBits(buffer: Uint8Array, bitOffset: number, numBits: number): number {
  let value = 0;
  let remaining = numBits;
  let shift = 0;

  while (remaining > 0) {
    const byteIndex = Math.floor(bitOffset / 8);
    const bitInByte = bitOffset % 8;
    const bitsToRead = Math.min(remaining, 8 - bitInByte);
    const mask = (1 << bitsToRead) - 1;

    value |= ((buffer[byteIndex] >>> bitInByte) & mask) << shift;

    shift += bitsToRead;
    bitOffset += bitsToRead;
    remaining -= bitsToRead;
  }

  return value;
}

// ============================================================================
// Null Bitmap Helpers
// ============================================================================

/**
 * Create a null bitmap from values.
 * Bit is 1 if value is NOT null, 0 if null.
 */
export function createNullBitmap(values: unknown[]): Uint8Array {
  const byteCount = Math.ceil(values.length / 8);
  const bitmap = new Uint8Array(byteCount);

  for (let i = 0; i < values.length; i++) {
    if (values[i] !== null && values[i] !== undefined) {
      const byteIndex = Math.floor(i / 8);
      const bitIndex = i % 8;
      bitmap[byteIndex] |= 1 << bitIndex;
    }
  }

  return bitmap;
}

/**
 * Check if a value is null based on the null bitmap.
 */
export function isNull(nullBitmap: Uint8Array, index: number): boolean {
  const byteIndex = Math.floor(index / 8);
  const bitIndex = index % 8;
  return (nullBitmap[byteIndex] & (1 << bitIndex)) === 0;
}

/**
 * Set a value as null in the bitmap.
 */
export function setNull(nullBitmap: Uint8Array, index: number): void {
  const byteIndex = Math.floor(index / 8);
  const bitIndex = index % 8;
  nullBitmap[byteIndex] &= ~(1 << bitIndex);
}

/**
 * Set a value as not null in the bitmap.
 */
export function setNotNull(nullBitmap: Uint8Array, index: number): void {
  const byteIndex = Math.floor(index / 8);
  const bitIndex = index % 8;
  nullBitmap[byteIndex] |= 1 << bitIndex;
}

// ============================================================================
// Encoding Selection
// ============================================================================

export interface EncodingAnalysis {
  recommendedEncoding: Encoding;
  estimatedSize: number;
  cardinality?: number;
  runCount?: number;
  isSorted?: boolean;
}

/**
 * Analyze column data and recommend the best encoding.
 */
export function analyzeForEncoding(
  values: unknown[],
  dataType: ColumnDataType
): EncodingAnalysis {
  const nonNullValues = values.filter((v) => v !== null && v !== undefined);
  const rowCount = values.length;

  // For strings, check dictionary encoding
  if (dataType === 'string') {
    const uniqueValues = new Set(nonNullValues as string[]);
    const cardinality = uniqueValues.size;
    const cardinalityRatio = cardinality / Math.max(rowCount, 1);

    if (rowCount >= MIN_ROWS_FOR_DICT && cardinalityRatio <= DICT_CARDINALITY_THRESHOLD) {
      const bitsPerIndex = cardinality <= 1 ? 1 : Math.ceil(Math.log2(cardinality));
      const indexSize = Math.ceil((rowCount * bitsPerIndex) / 8);
      const dictSize = Array.from(uniqueValues).reduce(
        (sum, s) => sum + 4 + new TextEncoder().encode(s).length,
        0
      );
      return {
        recommendedEncoding: 'dict',
        estimatedSize: 4 + dictSize + indexSize,
        cardinality,
      };
    }

    // Fall back to raw strings
    const rawSize = (nonNullValues as string[]).reduce(
      (sum, s) => sum + 4 + new TextEncoder().encode(s).length,
      0
    );
    return { recommendedEncoding: 'raw', estimatedSize: rawSize };
  }

  // For numeric types, check RLE and delta encoding
  if (isNumericType(dataType)) {
    const numericValues = nonNullValues as (number | bigint)[];
    const bytesPerValue = getBytesPerElement(dataType);

    // Check if sorted (for delta encoding)
    let isSorted = true;
    for (let i = 1; i < numericValues.length; i++) {
      if (numericValues[i] < numericValues[i - 1]) {
        isSorted = false;
        break;
      }
    }

    // Count runs (for RLE)
    let runCount = 1;
    for (let i = 1; i < values.length; i++) {
      if (values[i] !== values[i - 1]) runCount++;
    }

    // Estimate sizes
    const rawSize = rowCount * bytesPerValue;
    const rleSize = runCount * (bytesPerValue + 4) + 4;

    // Delta encoding estimate (if sorted integers)
    let deltaSize = Infinity;
    if (isSorted && isIntegerType(dataType) && numericValues.length > 1) {
      const deltas: number[] = [];
      for (let i = 1; i < numericValues.length; i++) {
        deltas.push(Number(numericValues[i]) - Number(numericValues[i - 1]));
      }
      const maxDelta = Math.max(...deltas.map(Math.abs));
      const bitWidth = maxDelta === 0 ? 1 : Math.ceil(Math.log2(maxDelta * 2 + 1));
      deltaSize = 14 + Math.ceil((deltas.length * bitWidth) / 8);
    }

    // Bitpack estimate (for small integers)
    let bitpackSize = Infinity;
    if (isIntegerType(dataType)) {
      const max = Math.max(...numericValues.map((v) => Math.abs(Number(v))));
      if (max > 0) {
        const bitsNeeded = Math.ceil(Math.log2(max + 1));
        if (bitsNeeded < bytesPerValue * 8) {
          bitpackSize = Math.ceil((rowCount * bitsNeeded) / 8);
        }
      }
    }

    // Choose best encoding
    const sizes: [Encoding, number][] = [
      ['raw', rawSize],
      ['rle', rleSize],
      ['delta', deltaSize],
      ['bitpack', bitpackSize],
    ];

    const [bestEncoding, bestSize] = sizes.reduce((best, current) =>
      current[1] < best[1] ? current : best
    );

    return {
      recommendedEncoding: bestEncoding,
      estimatedSize: bestSize,
      runCount,
      isSorted,
    };
  }

  // Default to raw for other types
  const bytesPerValue = getBytesPerElement(dataType);
  return {
    recommendedEncoding: 'raw',
    estimatedSize: bytesPerValue > 0 ? rowCount * bytesPerValue : rowCount,
  };
}

// ============================================================================
// Statistics Calculation
// ============================================================================

/**
 * Calculate column statistics for zone map filtering.
 */
export function calculateStats(
  values: unknown[],
  dataType: ColumnDataType
): ColumnStats {
  let min: number | bigint | string | null = null;
  let max: number | bigint | string | null = null;
  let nullCount = 0;
  let sum: number | bigint = 0;
  const uniqueValues = new Set<unknown>();

  for (const value of values) {
    if (value === null || value === undefined) {
      nullCount++;
      continue;
    }

    uniqueValues.add(value);

    if (dataType === 'string') {
      const strValue = value as string;
      if (min === null || strValue < (min as string)) min = strValue;
      if (max === null || strValue > (max as string)) max = strValue;
    } else if (isNumericType(dataType)) {
      const numValue = value as number | bigint;
      if (min === null || numValue < (min as number | bigint)) min = numValue;
      if (max === null || numValue > (max as number | bigint)) max = numValue;

      if (typeof sum === 'bigint' || typeof numValue === 'bigint') {
        sum = BigInt(sum) + BigInt(numValue);
      } else {
        sum = (sum as number) + (numValue as number);
      }
    } else if (dataType === 'boolean') {
      const boolValue = value ? 1 : 0;
      if (min === null || boolValue < (min as number)) min = boolValue;
      if (max === null || boolValue > (max as number)) max = boolValue;
    }
  }

  const stats: ColumnStats = {
    min,
    max,
    nullCount,
    distinctCount: uniqueValues.size,
  };

  if (isNumericType(dataType)) {
    stats.sum = sum;
  }

  return stats;
}
