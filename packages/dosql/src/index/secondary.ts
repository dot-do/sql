/**
 * Secondary Index Implementation for DoSQL
 *
 * Uses the existing B-tree implementation for storage.
 * Supports:
 * - Single and composite column indexes
 * - Unique index enforcement
 * - Index maintenance on INSERT/UPDATE/DELETE
 * - Range and point lookups
 */

import type { FSXBackend } from '../fsx/types.js';
import type { SqlValue, Row } from '../engine/types.js';
import {
  type IndexDefinition,
  type IndexEntry,
  type IndexScanRange,
  type IndexMetadata,
  type IndexMaintenanceOp,
  type IndexMaintenanceResult,
  type UniqueViolation,
  type IndexStatistics,
  type EncodedIndexKey,
} from './types.js';
import { BTreeImpl, type KeyCodec, type ValueCodec, type BTreeConfig } from '../btree/index.js';

// =============================================================================
// KEY ENCODING
// =============================================================================

/**
 * Encode a SqlValue to bytes for B-tree storage
 * Uses a type prefix to maintain correct sort order across types
 */
function encodeValue(value: SqlValue): Uint8Array {
  if (value === null) {
    // NULL sorts first (0x00 prefix)
    return new Uint8Array([0x00]);
  }

  if (typeof value === 'boolean') {
    // Boolean: 0x01 prefix, then 0x00 or 0x01
    return new Uint8Array([0x01, value ? 0x01 : 0x00]);
  }

  if (typeof value === 'number') {
    // Number: 0x02 prefix, then IEEE 754 double (big-endian, with sign bit flipped)
    const buffer = new ArrayBuffer(9);
    const view = new DataView(buffer);
    view.setUint8(0, 0x02);
    view.setFloat64(1, value, false);

    // Flip sign bit and flip all bits if negative for correct sort order
    const bytes = new Uint8Array(buffer);
    if (value >= 0) {
      bytes[1] ^= 0x80; // Flip sign bit
    } else {
      for (let i = 1; i < 9; i++) {
        bytes[i] ^= 0xff; // Flip all bits
      }
    }
    return bytes;
  }

  if (typeof value === 'bigint') {
    // BigInt: 0x03 prefix, then 8-byte signed big-endian with sign adjustment
    const buffer = new ArrayBuffer(9);
    const view = new DataView(buffer);
    view.setUint8(0, 0x03);
    view.setBigInt64(1, value, false);

    // Flip sign bit for correct sort order
    const bytes = new Uint8Array(buffer);
    bytes[1] ^= 0x80;
    return bytes;
  }

  if (typeof value === 'string') {
    // String: 0x04 prefix, then UTF-8 bytes with null terminator
    const encoded = new TextEncoder().encode(value);
    const result = new Uint8Array(1 + encoded.length + 1);
    result[0] = 0x04;
    result.set(encoded, 1);
    result[result.length - 1] = 0x00; // Null terminator
    return result;
  }

  if (value instanceof Date) {
    // Date: 0x05 prefix, then milliseconds as 64-bit signed
    const buffer = new ArrayBuffer(9);
    const view = new DataView(buffer);
    view.setUint8(0, 0x05);
    view.setBigInt64(1, BigInt(value.getTime()), false);
    const bytes = new Uint8Array(buffer);
    bytes[1] ^= 0x80; // Flip sign bit for correct sort order
    return bytes;
  }

  if (value instanceof Uint8Array) {
    // Binary: 0x06 prefix, then raw bytes
    const result = new Uint8Array(1 + value.length);
    result[0] = 0x06;
    result.set(value, 1);
    return result;
  }

  throw new Error(`Unsupported value type: ${typeof value}`);
}

/**
 * Decode a SqlValue from bytes
 */
function decodeValue(bytes: Uint8Array): SqlValue {
  if (bytes.length === 0) {
    throw new Error('Cannot decode empty bytes');
  }

  const type = bytes[0];

  switch (type) {
    case 0x00: // NULL
      return null;

    case 0x01: // Boolean
      return bytes[1] === 0x01;

    case 0x02: { // Number
      const buffer = new ArrayBuffer(8);
      const view = new DataView(buffer);
      const valueBytes = bytes.slice(1);

      // Reverse the encoding transformation
      if (valueBytes[0] & 0x80) {
        // Positive: just flip sign bit
        valueBytes[0] ^= 0x80;
      } else {
        // Negative: flip all bits
        for (let i = 0; i < 8; i++) {
          valueBytes[i] ^= 0xff;
        }
      }

      new Uint8Array(buffer).set(valueBytes);
      return view.getFloat64(0, false);
    }

    case 0x03: { // BigInt
      const buffer = new ArrayBuffer(8);
      const valueBytes = bytes.slice(1);
      valueBytes[0] ^= 0x80; // Reverse sign bit flip
      new Uint8Array(buffer).set(valueBytes);
      return new DataView(buffer).getBigInt64(0, false);
    }

    case 0x04: { // String
      // Skip prefix (0x04) and null terminator
      const stringBytes = bytes.slice(1, -1);
      return new TextDecoder().decode(stringBytes);
    }

    case 0x05: { // Date
      const buffer = new ArrayBuffer(8);
      const valueBytes = bytes.slice(1);
      valueBytes[0] ^= 0x80; // Reverse sign bit flip
      new Uint8Array(buffer).set(valueBytes);
      const ms = new DataView(buffer).getBigInt64(0, false);
      return new Date(Number(ms));
    }

    case 0x06: // Binary
      return bytes.slice(1);

    default:
      throw new Error(`Unknown value type: ${type}`);
  }
}

/**
 * Encode a composite key (multiple column values) to bytes
 */
function encodeCompositeKey(values: SqlValue[]): Uint8Array {
  const encodedParts = values.map(encodeValue);
  const totalLength = encodedParts.reduce((sum, part) => sum + part.length, 0);

  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const part of encodedParts) {
    result.set(part, offset);
    offset += part.length;
  }

  return result;
}

/**
 * Decode a composite key from bytes
 */
function decodeCompositeKey(bytes: Uint8Array, columnCount: number): SqlValue[] {
  const values: SqlValue[] = [];
  let offset = 0;

  for (let i = 0; i < columnCount && offset < bytes.length; i++) {
    const type = bytes[offset];
    let length: number;

    switch (type) {
      case 0x00: // NULL
        length = 1;
        break;
      case 0x01: // Boolean
        length = 2;
        break;
      case 0x02: // Number
      case 0x03: // BigInt
      case 0x05: // Date
        length = 9;
        break;
      case 0x04: { // String - find null terminator
        let end = offset + 1;
        while (end < bytes.length && bytes[end] !== 0x00) {
          end++;
        }
        length = end - offset + 1; // Include null terminator
        break;
      }
      case 0x06: // Binary - rest of data (only works if last column)
        length = bytes.length - offset;
        break;
      default:
        throw new Error(`Unknown type prefix: ${type}`);
    }

    values.push(decodeValue(bytes.slice(offset, offset + length)));
    offset += length;
  }

  return values;
}

/**
 * Key codec for composite index keys
 */
function createIndexKeyCodec(columnCount: number): KeyCodec<SqlValue[]> {
  return {
    encode(key: SqlValue[]): Uint8Array {
      return encodeCompositeKey(key);
    },
    decode(bytes: Uint8Array): SqlValue[] {
      return decodeCompositeKey(bytes, columnCount);
    },
    compare(a: SqlValue[], b: SqlValue[]): number {
      const aBytes = encodeCompositeKey(a);
      const bBytes = encodeCompositeKey(b);

      const minLen = Math.min(aBytes.length, bBytes.length);
      for (let i = 0; i < minLen; i++) {
        if (aBytes[i] < bBytes[i]) return -1;
        if (aBytes[i] > bBytes[i]) return 1;
      }
      return aBytes.length - bBytes.length;
    },
  };
}

/**
 * Value codec for row IDs (primary keys)
 */
const rowIdCodec: ValueCodec<SqlValue> = {
  encode(rowId: SqlValue): Uint8Array {
    return encodeValue(rowId);
  },
  decode(bytes: Uint8Array): SqlValue {
    return decodeValue(bytes);
  },
};

/**
 * Value codec for arrays of row IDs (for non-unique indexes)
 */
const rowIdArrayCodec: ValueCodec<SqlValue[]> = {
  encode(rowIds: SqlValue[]): Uint8Array {
    const encoded = rowIds.map(encodeValue);
    // Prefix with count, then length-prefixed encoded values
    const countBytes = new Uint8Array(4);
    new DataView(countBytes.buffer).setUint32(0, rowIds.length, false);

    const totalLength = 4 + encoded.reduce((sum, e) => sum + 4 + e.length, 0);
    const result = new Uint8Array(totalLength);
    result.set(countBytes, 0);

    let offset = 4;
    for (const enc of encoded) {
      new DataView(result.buffer).setUint32(offset, enc.length, false);
      result.set(enc, offset + 4);
      offset += 4 + enc.length;
    }

    return result;
  },
  decode(bytes: Uint8Array): SqlValue[] {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const count = view.getUint32(0, false);
    const result: SqlValue[] = [];

    let offset = 4;
    for (let i = 0; i < count; i++) {
      const len = view.getUint32(offset, false);
      const encoded = bytes.slice(offset + 4, offset + 4 + len);
      result.push(decodeValue(encoded));
      offset += 4 + len;
    }

    return result;
  },
};

// =============================================================================
// SECONDARY INDEX CLASS
// =============================================================================

/**
 * Secondary index implementation using B-tree
 */
export class SecondaryIndex {
  private readonly definition: IndexDefinition;
  private readonly btree: BTreeImpl<SqlValue[], SqlValue[]>;
  private readonly keyCodec: KeyCodec<SqlValue[]>;
  private metadata: IndexMetadata | null = null;

  constructor(
    private readonly fsx: FSXBackend,
    definition: IndexDefinition,
    config?: Partial<BTreeConfig>
  ) {
    this.definition = definition;
    this.keyCodec = createIndexKeyCodec(definition.columns.length);

    const btreeConfig: Partial<BTreeConfig> = {
      ...config,
      pagePrefix: `index/${definition.table}/${definition.name}/`,
    };

    // For unique indexes, each key maps to exactly one row ID
    // For non-unique indexes, each key maps to an array of row IDs
    this.btree = new BTreeImpl(
      fsx,
      this.keyCodec,
      rowIdArrayCodec,
      btreeConfig
    );
  }

  /**
   * Initialize the index
   */
  async init(): Promise<void> {
    await this.btree.init();
    await this.loadMetadata();
  }

  /**
   * Get the index definition
   */
  getDefinition(): IndexDefinition {
    return this.definition;
  }

  /**
   * Get index metadata
   */
  getMetadata(): IndexMetadata | null {
    return this.metadata;
  }

  /**
   * Load or create index metadata
   */
  private async loadMetadata(): Promise<void> {
    const metaKey = `index/${this.definition.table}/${this.definition.name}/_meta`;
    const data = await this.fsx.read(metaKey);

    if (data) {
      this.metadata = JSON.parse(new TextDecoder().decode(data));
    } else {
      this.metadata = {
        definition: this.definition,
        entryCount: 0,
        distinctKeys: 0,
        height: 1,
        statistics: {
          avgKeySize: 0,
          avgRowIdSize: 8,
          totalBytes: 0,
          pageCount: 1,
        },
      };
      await this.saveMetadata();
    }
  }

  /**
   * Save index metadata
   */
  private async saveMetadata(): Promise<void> {
    if (!this.metadata) return;
    const metaKey = `index/${this.definition.table}/${this.definition.name}/_meta`;
    const data = new TextEncoder().encode(JSON.stringify(this.metadata));
    await this.fsx.write(metaKey, data);
  }

  /**
   * Extract index key values from a row
   */
  extractKey(row: Row): SqlValue[] {
    return this.definition.columns.map(col => {
      const value = row[col.name];
      if (value === undefined) {
        throw new Error(`Column ${col.name} not found in row`);
      }
      return value;
    });
  }

  /**
   * Insert an entry into the index
   *
   * @returns UniqueViolation if unique constraint is violated
   */
  async insert(key: SqlValue[], rowId: SqlValue): Promise<UniqueViolation | null> {
    const existing = await this.btree.get(key);

    if (existing) {
      if (this.definition.unique) {
        return {
          index: this.definition.name,
          key,
          existingRowId: existing[0],
          newRowId: rowId,
        };
      }
      // Non-unique: append to existing array
      existing.push(rowId);
      await this.btree.set(key, existing);
    } else {
      // New key
      await this.btree.set(key, [rowId]);
      if (this.metadata) {
        this.metadata.distinctKeys++;
      }
    }

    if (this.metadata) {
      this.metadata.entryCount++;
    }
    await this.saveMetadata();

    return null;
  }

  /**
   * Delete an entry from the index
   */
  async delete(key: SqlValue[], rowId: SqlValue): Promise<boolean> {
    const existing = await this.btree.get(key);

    if (!existing) {
      return false;
    }

    if (this.definition.unique) {
      // Unique index: just delete the key
      await this.btree.delete(key);
      if (this.metadata) {
        this.metadata.entryCount--;
        this.metadata.distinctKeys--;
      }
    } else {
      // Non-unique: remove rowId from array
      const index = existing.findIndex(id => this.compareValues(id, rowId) === 0);
      if (index === -1) {
        return false;
      }

      existing.splice(index, 1);

      if (existing.length === 0) {
        await this.btree.delete(key);
        if (this.metadata) {
          this.metadata.distinctKeys--;
        }
      } else {
        await this.btree.set(key, existing);
      }

      if (this.metadata) {
        this.metadata.entryCount--;
      }
    }

    await this.saveMetadata();
    return true;
  }

  /**
   * Update an entry in the index (delete old, insert new)
   */
  async update(
    oldKey: SqlValue[],
    newKey: SqlValue[],
    rowId: SqlValue
  ): Promise<UniqueViolation | null> {
    // Check if key changed
    if (this.compareKeys(oldKey, newKey) === 0) {
      return null; // No change needed
    }

    // Delete old entry
    await this.delete(oldKey, rowId);

    // Insert new entry
    return this.insert(newKey, rowId);
  }

  /**
   * Perform a point lookup (exact key match)
   */
  async lookup(key: SqlValue[]): Promise<SqlValue[]> {
    const rowIds = await this.btree.get(key);
    return rowIds ?? [];
  }

  /**
   * Perform a range scan
   */
  async *scan(range: IndexScanRange): AsyncIterableIterator<IndexEntry> {
    // Determine iteration
    if (range.reverse) {
      // Reverse iteration not yet implemented
      throw new Error('Reverse index scan not yet implemented');
    }

    let count = 0;
    const limit = range.limit ?? Infinity;

    // Get iterator based on range
    let iterator: AsyncIterableIterator<[SqlValue[], SqlValue[]]>;

    if (range.start && range.end) {
      // Range scan
      iterator = this.btree.range(range.start.key, range.end.key);
    } else {
      // Full scan
      iterator = this.btree.entries();
    }

    for await (const [key, rowIds] of iterator) {
      // Check start bound
      if (range.start) {
        const cmp = this.compareKeys(key, range.start.key);
        if (cmp < 0 || (cmp === 0 && !range.start.inclusive)) {
          continue;
        }
      }

      // Check end bound
      if (range.end) {
        const cmp = this.compareKeys(key, range.end.key);
        if (cmp > 0 || (cmp === 0 && !range.end.inclusive)) {
          break;
        }
      }

      // Emit entries
      for (const rowId of rowIds) {
        yield { key, rowId };
        count++;
        if (count >= limit) {
          return;
        }
      }
    }
  }

  /**
   * Get count of entries matching a range
   */
  async count(range?: IndexScanRange): Promise<number> {
    if (!range) {
      return this.metadata?.entryCount ?? 0;
    }

    let count = 0;
    for await (const _entry of this.scan(range)) {
      count++;
    }
    return count;
  }

  /**
   * Clear all entries from the index
   */
  async clear(): Promise<void> {
    await this.btree.clear();
    if (this.metadata) {
      this.metadata.entryCount = 0;
      this.metadata.distinctKeys = 0;
    }
    await this.saveMetadata();
  }

  /**
   * Rebuild the index from table data
   */
  async rebuild(
    rowIterator: AsyncIterableIterator<{ rowId: SqlValue; row: Row }>
  ): Promise<IndexMaintenanceResult> {
    const startTime = performance.now();
    await this.clear();

    let inserted = 0;
    const violations: UniqueViolation[] = [];

    for await (const { rowId, row } of rowIterator) {
      const key = this.extractKey(row);
      const violation = await this.insert(key, rowId);
      if (violation) {
        violations.push(violation);
      } else {
        inserted++;
      }
    }

    if (this.metadata) {
      this.metadata.lastRebuild = Date.now();
    }
    await this.saveMetadata();

    return {
      inserted,
      deleted: 0,
      violations,
      durationMs: performance.now() - startTime,
    };
  }

  /**
   * Compare two keys
   */
  private compareKeys(a: SqlValue[], b: SqlValue[]): number {
    return this.keyCodec.compare(a, b);
  }

  /**
   * Compare two values
   */
  private compareValues(a: SqlValue, b: SqlValue): number {
    const aBytes = encodeValue(a);
    const bBytes = encodeValue(b);

    const minLen = Math.min(aBytes.length, bBytes.length);
    for (let i = 0; i < minLen; i++) {
      if (aBytes[i] < bBytes[i]) return -1;
      if (aBytes[i] > bBytes[i]) return 1;
    }
    return aBytes.length - bBytes.length;
  }
}

// =============================================================================
// INDEX MANAGER
// =============================================================================

/**
 * Manages all secondary indexes for a table
 */
export class IndexManager {
  private readonly fsx: FSXBackend;
  private readonly indexes = new Map<string, SecondaryIndex>();
  private readonly tableIndexes = new Map<string, Set<string>>();

  constructor(fsx: FSXBackend) {
    this.fsx = fsx;
  }

  /**
   * Create and register a new index
   */
  async createIndex(definition: IndexDefinition): Promise<SecondaryIndex> {
    const key = `${definition.table}.${definition.name}`;

    if (this.indexes.has(key)) {
      throw new Error(`Index ${key} already exists`);
    }

    const index = new SecondaryIndex(this.fsx, definition);
    await index.init();

    this.indexes.set(key, index);

    // Track indexes by table
    let tableSet = this.tableIndexes.get(definition.table);
    if (!tableSet) {
      tableSet = new Set();
      this.tableIndexes.set(definition.table, tableSet);
    }
    tableSet.add(definition.name);

    return index;
  }

  /**
   * Get an index by name
   */
  getIndex(table: string, name: string): SecondaryIndex | undefined {
    return this.indexes.get(`${table}.${name}`);
  }

  /**
   * Get all indexes for a table
   */
  getTableIndexes(table: string): SecondaryIndex[] {
    const names = this.tableIndexes.get(table);
    if (!names) return [];

    return Array.from(names)
      .map(name => this.indexes.get(`${table}.${name}`))
      .filter((idx): idx is SecondaryIndex => idx !== undefined);
  }

  /**
   * Drop an index
   */
  async dropIndex(table: string, name: string): Promise<boolean> {
    const key = `${table}.${name}`;
    const index = this.indexes.get(key);

    if (!index) {
      return false;
    }

    await index.clear();
    this.indexes.delete(key);

    const tableSet = this.tableIndexes.get(table);
    if (tableSet) {
      tableSet.delete(name);
    }

    return true;
  }

  /**
   * Maintain indexes after row insert
   */
  async onInsert(
    table: string,
    rowId: SqlValue,
    row: Row
  ): Promise<IndexMaintenanceResult> {
    const startTime = performance.now();
    const indexes = this.getTableIndexes(table);
    const violations: UniqueViolation[] = [];
    let inserted = 0;

    for (const index of indexes) {
      const key = index.extractKey(row);
      const violation = await index.insert(key, rowId);
      if (violation) {
        violations.push(violation);
      } else {
        inserted++;
      }
    }

    return {
      inserted,
      deleted: 0,
      violations,
      durationMs: performance.now() - startTime,
    };
  }

  /**
   * Maintain indexes after row delete
   */
  async onDelete(
    table: string,
    rowId: SqlValue,
    row: Row
  ): Promise<IndexMaintenanceResult> {
    const startTime = performance.now();
    const indexes = this.getTableIndexes(table);
    let deleted = 0;

    for (const index of indexes) {
      const key = index.extractKey(row);
      if (await index.delete(key, rowId)) {
        deleted++;
      }
    }

    return {
      inserted: 0,
      deleted,
      violations: [],
      durationMs: performance.now() - startTime,
    };
  }

  /**
   * Maintain indexes after row update
   */
  async onUpdate(
    table: string,
    rowId: SqlValue,
    oldRow: Row,
    newRow: Row
  ): Promise<IndexMaintenanceResult> {
    const startTime = performance.now();
    const indexes = this.getTableIndexes(table);
    const violations: UniqueViolation[] = [];
    let inserted = 0;
    let deleted = 0;

    for (const index of indexes) {
      const oldKey = index.extractKey(oldRow);
      const newKey = index.extractKey(newRow);

      const violation = await index.update(oldKey, newKey, rowId);
      if (violation) {
        violations.push(violation);
      } else {
        // Count as both delete and insert if key changed
        const def = index.getDefinition();
        const keyChanged = oldKey.some((v, i) => v !== newKey[i]);
        if (keyChanged) {
          deleted++;
          inserted++;
        }
      }
    }

    return {
      inserted,
      deleted,
      violations,
      durationMs: performance.now() - startTime,
    };
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a secondary index
 */
export function createSecondaryIndex(
  fsx: FSXBackend,
  definition: IndexDefinition,
  config?: Partial<BTreeConfig>
): SecondaryIndex {
  return new SecondaryIndex(fsx, definition, config);
}

/**
 * Create an index manager
 */
export function createIndexManager(fsx: FSXBackend): IndexManager {
  return new IndexManager(fsx);
}
