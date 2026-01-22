/**
 * R2 Index Reader
 *
 * Reads single-file R2 index using byte-range requests.
 * Optimized for minimal round-trips - header read gives all offsets,
 * then individual sections can be fetched as needed.
 */

import {
  R2_INDEX_MAGIC,
  R2_INDEX_VERSION,
  R2_INDEX_MIN_VERSION,
  R2_INDEX_HEADER_SIZE,
  R2IndexFlags,
  R2BTreePageType,
  type R2IndexHeader,
  type R2IndexSectionOffsets,
  type SectionPointer,
  type R2IndexSchema,
  type R2IndexBTreeSection,
  type R2BTreeIndex,
  type R2BTreePage,
  type R2BTreePageInfo,
  type R2IndexBloomSection,
  type R2BloomFilter,
  type R2IndexStatsSection,
  type R2GlobalStats,
  type R2ChunkStats,
  type R2ColumnStats,
  type R2IndexDataSection,
  type R2DataFile,
  type R2IndexReadOptions,
  type R2IndexHeaderReadResult,
  type R2IndexSectionReadResult,
  type R2IndexPredicate,
  type R2IndexQueryResult,
  type R2IndexMatch,
  type R2QueryStats,
  type R2IndexValue,
  type R2Bucket,
  type R2RangeRequest,
  encodeValue,
  decodeValue,
  compareValues,
} from './types.js';
import { crc32, murmur3_32 } from './writer.js';

// =============================================================================
// R2 INDEX READER
// =============================================================================

/**
 * R2 Index file reader with byte-range request support
 */
export class R2IndexReader {
  private bucket?: R2Bucket;
  private key?: string;
  private content?: Uint8Array;
  private header?: R2IndexHeader;
  private options: R2IndexReadOptions;

  // Cached sections
  private schemaCache?: R2IndexSchema;
  private indexCache?: R2IndexBTreeSection;
  private bloomCache?: R2IndexBloomSection;
  private statsCache?: R2IndexStatsSection;
  private dataCache?: R2IndexDataSection;

  // Query statistics
  private queryStats: R2QueryStats = this.resetStats();

  constructor(options: R2IndexReadOptions) {
    this.options = {
      verifyChecksums: true,
      ...options,
    };
    this.bucket = options.bucket;
    this.key = options.key;
    this.content = options.content;
  }

  private resetStats(): R2QueryStats {
    return {
      pagesRead: 0,
      bytesRead: 0n,
      bloomChecks: 0,
      bloomHits: 0,
      chunksPruned: 0,
      executionTimeMs: 0,
      rangeRequests: 0,
    };
  }

  // ===========================================================================
  // BYTE RANGE FETCHING
  // ===========================================================================

  /**
   * Fetch bytes from R2 using range request
   */
  private async fetchRange(start: bigint, length: number): Promise<Uint8Array> {
    this.queryStats.rangeRequests++;
    this.queryStats.bytesRead += BigInt(length);

    if (this.content) {
      // Use pre-loaded content
      return this.content.slice(Number(start), Number(start) + length);
    }

    if (!this.bucket || !this.key) {
      throw new Error('R2 bucket and key are required for range requests');
    }

    const object = await this.bucket.get(this.key, {
      range: { offset: Number(start), length },
    });

    if (!object) {
      throw new Error(`R2 object not found: ${this.key}`);
    }

    const buffer = await object.arrayBuffer();
    return new Uint8Array(buffer);
  }

  // ===========================================================================
  // HEADER READING
  // ===========================================================================

  /**
   * Read and parse the index header (first 128 bytes)
   */
  async readHeader(): Promise<R2IndexHeaderReadResult> {
    const rawBytes = await this.fetchRange(0n, R2_INDEX_HEADER_SIZE);
    const view = new DataView(rawBytes.buffer, rawBytes.byteOffset, rawBytes.byteLength);
    const decoder = new TextDecoder();
    const errors: string[] = [];

    let pos = 0;

    // Magic (8 bytes)
    const magic = decoder.decode(rawBytes.slice(pos, pos + 8));
    pos += 8;
    if (magic !== R2_INDEX_MAGIC) {
      errors.push(`Invalid magic: expected "${R2_INDEX_MAGIC}", got "${magic}"`);
    }

    // Version (4 bytes)
    const version = view.getUint32(pos, true);
    pos += 4;
    if (version < R2_INDEX_MIN_VERSION || version > R2_INDEX_VERSION) {
      errors.push(`Unsupported version: ${version}`);
    }

    // Flags (4 bytes)
    const flags = view.getUint32(pos, true);
    pos += 4;

    // Header size (4 bytes)
    const headerSize = view.getUint32(pos, true);
    pos += 4;

    // Section offsets (5 sections * 16 bytes = 80 bytes)
    const readSectionPointer = (): SectionPointer => {
      const offset = view.getBigUint64(pos, true);
      pos += 8;
      const size = view.getUint32(pos, true);
      pos += 4;
      const checksum = view.getUint32(pos, true);
      pos += 4;
      return { offset, size, checksum };
    };

    const sections: R2IndexSectionOffsets = {
      schema: readSectionPointer(),
      index: readSectionPointer(),
      bloom: readSectionPointer(),
      stats: readSectionPointer(),
      data: readSectionPointer(),
    };

    // Total size (8 bytes)
    const totalSize = view.getBigUint64(pos, true);
    pos += 8;

    // Created at (8 bytes)
    const createdAt = view.getBigUint64(pos, true);
    pos += 8;

    // Row count (8 bytes)
    const rowCount = view.getBigUint64(pos, true);
    pos += 8;

    // Header checksum (4 bytes)
    const headerChecksum = view.getUint32(pos, true);
    pos += 4;

    // Verify checksum
    if (this.options.verifyChecksums) {
      const checksumData = rawBytes.slice(0, pos - 4);
      const calculatedChecksum = crc32(checksumData);
      if (calculatedChecksum !== headerChecksum) {
        errors.push(`Header checksum mismatch: expected ${headerChecksum}, got ${calculatedChecksum}`);
      }
    }

    const header: R2IndexHeader = {
      magic,
      version,
      flags,
      headerSize,
      sections,
      totalSize,
      createdAt,
      rowCount,
      headerChecksum,
    };

    this.header = header;

    return {
      header,
      rawBytes,
      valid: errors.length === 0,
      errors: errors.length > 0 ? errors : undefined,
    };
  }

  /**
   * Get the cached header, reading it if necessary
   */
  async getHeader(): Promise<R2IndexHeader> {
    if (!this.header) {
      const result = await this.readHeader();
      if (!result.valid) {
        throw new Error(`Invalid index header: ${result.errors?.join(', ')}`);
      }
    }
    return this.header!;
  }

  // ===========================================================================
  // SECTION READING
  // ===========================================================================

  /**
   * Read a section and optionally verify its checksum
   */
  private async readSection(pointer: SectionPointer): Promise<Uint8Array> {
    if (pointer.size === 0) {
      return new Uint8Array(0);
    }

    const data = await this.fetchRange(pointer.offset, pointer.size);

    if (this.options.verifyChecksums && pointer.checksum !== 0) {
      const calculatedChecksum = crc32(data);
      if (calculatedChecksum !== pointer.checksum) {
        throw new Error(`Section checksum mismatch: expected ${pointer.checksum}, got ${calculatedChecksum}`);
      }
    }

    return data;
  }

  /**
   * Read and parse the schema section
   */
  async readSchema(): Promise<R2IndexSectionReadResult<R2IndexSchema>> {
    if (this.schemaCache) {
      return { data: this.schemaCache, bytesRead: 0 };
    }

    const header = await this.getHeader();
    const data = await this.readSection(header.sections.schema);
    const decoder = new TextDecoder();
    const schema = JSON.parse(decoder.decode(data)) as R2IndexSchema;

    this.schemaCache = schema;

    return {
      data: schema,
      bytesRead: data.length,
      checksumValid: true,
    };
  }

  /**
   * Read and parse the B-tree index section
   */
  async readIndexSection(): Promise<R2IndexSectionReadResult<R2IndexBTreeSection>> {
    if (this.indexCache) {
      return { data: this.indexCache, bytesRead: 0 };
    }

    const header = await this.getHeader();
    if (!(header.flags & R2IndexFlags.HAS_BTREE)) {
      return { data: { indexes: [] }, bytesRead: 0 };
    }

    const data = await this.readSection(header.sections.index);
    if (data.length === 0) {
      return { data: { indexes: [] }, bytesRead: 0 };
    }

    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const decoder = new TextDecoder();

    // Read metadata size
    const metadataSize = view.getUint32(0, true);
    const metadataBytes = data.slice(4, 4 + metadataSize);
    const metadata = JSON.parse(decoder.decode(metadataBytes));

    // Parse indexes
    const indexes: R2BTreeIndex[] = metadata.indexes.map((idx: Record<string, unknown>) => ({
      ...idx,
      entryCount: BigInt(idx.entryCount as string),
      pageDirectory: (idx.pageDirectory as Array<Record<string, unknown>>).map((p) => ({
        ...p,
        firstKey: p.firstKey ? new Uint8Array(p.firstKey as number[]) : undefined,
        lastKey: p.lastKey ? new Uint8Array(p.lastKey as number[]) : undefined,
      })),
    }));

    const section: R2IndexBTreeSection = { indexes };
    this.indexCache = section;

    return {
      data: section,
      bytesRead: data.length,
      checksumValid: true,
    };
  }

  /**
   * Read and parse the bloom filter section
   */
  async readBloomSection(): Promise<R2IndexSectionReadResult<R2IndexBloomSection>> {
    if (this.bloomCache) {
      return { data: this.bloomCache, bytesRead: 0 };
    }

    const header = await this.getHeader();
    if (!(header.flags & R2IndexFlags.HAS_BLOOM)) {
      return { data: { filters: [] }, bytesRead: 0 };
    }

    const data = await this.readSection(header.sections.bloom);
    if (data.length === 0) {
      return { data: { filters: [] }, bytesRead: 0 };
    }

    const decoder = new TextDecoder();
    const parsed = JSON.parse(decoder.decode(data));

    const filters: R2BloomFilter[] = parsed.filters.map((f: Record<string, unknown>) => ({
      ...f,
      bits: new Uint8Array(f.bits as number[]),
    }));

    const section: R2IndexBloomSection = { filters };
    this.bloomCache = section;

    return {
      data: section,
      bytesRead: data.length,
      checksumValid: true,
    };
  }

  /**
   * Read and parse the statistics section
   */
  async readStatsSection(): Promise<R2IndexSectionReadResult<R2IndexStatsSection>> {
    if (this.statsCache) {
      return { data: this.statsCache, bytesRead: 0 };
    }

    const header = await this.getHeader();
    if (!(header.flags & R2IndexFlags.HAS_STATS)) {
      return {
        data: {
          global: {
            rowCount: 0n,
            totalBytes: 0n,
            chunkCount: 0,
            columns: new Map(),
          },
          chunks: [],
        },
        bytesRead: 0,
      };
    }

    const data = await this.readSection(header.sections.stats);
    if (data.length === 0) {
      return {
        data: {
          global: {
            rowCount: 0n,
            totalBytes: 0n,
            chunkCount: 0,
            columns: new Map(),
          },
          chunks: [],
        },
        bytesRead: 0,
      };
    }

    const decoder = new TextDecoder();
    const parsed = JSON.parse(decoder.decode(data));

    const parseValue = (v: Record<string, unknown> | null): R2IndexValue | null => {
      if (v === null) return null;
      switch (v.type as string) {
        case 'null':
          return { type: 'null' };
        case 'boolean':
          return { type: 'boolean', value: v.value as boolean };
        case 'int':
          return { type: 'int', value: BigInt(v.value as string) };
        case 'float':
          return { type: 'float', value: v.value as number };
        case 'string':
          return { type: 'string', value: v.value as string };
        case 'bytes':
          return { type: 'bytes', value: new Uint8Array(v.value as number[]) };
        case 'timestamp':
          return { type: 'timestamp', value: BigInt(v.value as string) };
        case 'date':
          return { type: 'date', value: v.value as number };
        case 'uuid':
          return { type: 'uuid', value: v.value as string };
        default:
          return null;
      }
    };

    const parseColumnStats = (obj: Record<string, unknown>): R2ColumnStats => ({
      min: parseValue(obj.min as Record<string, unknown> | null),
      max: parseValue(obj.max as Record<string, unknown> | null),
      nullCount: BigInt(obj.nullCount as string),
      distinctCount: obj.distinctCount ? BigInt(obj.distinctCount as string) : undefined,
      sum: obj.sum !== undefined
        ? (typeof obj.sum === 'string' ? BigInt(obj.sum) : obj.sum as number)
        : undefined,
      avg: obj.avg as number | undefined,
      allSame: obj.allSame as boolean | undefined,
      sorted: obj.sorted as boolean | undefined,
      sortDirection: obj.sortDirection as 'asc' | 'desc' | undefined,
    });

    const globalStats: R2GlobalStats = {
      rowCount: BigInt(parsed.global.rowCount),
      totalBytes: BigInt(parsed.global.totalBytes),
      chunkCount: parsed.global.chunkCount,
      columns: new Map(
        Object.entries(parsed.global.columns).map(([k, v]) => [
          k,
          parseColumnStats(v as Record<string, unknown>),
        ])
      ),
    };

    const chunks: R2ChunkStats[] = parsed.chunks.map((c: Record<string, unknown>) => ({
      ...c,
      rowRange: {
        start: BigInt((c.rowRange as Record<string, string>).start),
        end: BigInt((c.rowRange as Record<string, string>).end),
      },
      byteRange: {
        offset: BigInt((c.byteRange as Record<string, unknown>).offset as string),
        size: (c.byteRange as Record<string, unknown>).size as number,
      },
      columns: new Map(
        Object.entries(c.columns as Record<string, unknown>).map(([k, v]) => [
          k,
          parseColumnStats(v as Record<string, unknown>),
        ])
      ),
    }));

    const section: R2IndexStatsSection = { global: globalStats, chunks };
    this.statsCache = section;

    return {
      data: section,
      bytesRead: data.length,
      checksumValid: true,
    };
  }

  /**
   * Read and parse the data pointers section
   */
  async readDataSection(): Promise<R2IndexSectionReadResult<R2IndexDataSection>> {
    if (this.dataCache) {
      return { data: this.dataCache, bytesRead: 0 };
    }

    const header = await this.getHeader();
    const data = await this.readSection(header.sections.data);

    if (data.length === 0) {
      return {
        data: { storageType: 'inline', files: [], totalSize: 0n },
        bytesRead: 0,
      };
    }

    const decoder = new TextDecoder();
    const parsed = JSON.parse(decoder.decode(data));

    const files: R2DataFile[] = parsed.files.map((f: Record<string, unknown>) => ({
      ...f,
      rowRange: {
        start: BigInt((f.rowRange as Record<string, string>).start),
        end: BigInt((f.rowRange as Record<string, string>).end),
      },
      size: BigInt(f.size as string),
      location: (f.location as Record<string, unknown>).type === 'inline'
        ? {
            type: 'inline' as const,
            offset: BigInt((f.location as Record<string, string>).offset),
            size: (f.location as Record<string, unknown>).size as number,
          }
        : f.location,
    }));

    const section: R2IndexDataSection = {
      storageType: parsed.storageType,
      files,
      totalSize: BigInt(parsed.totalSize),
    };
    this.dataCache = section;

    return {
      data: section,
      bytesRead: data.length,
      checksumValid: true,
    };
  }

  // ===========================================================================
  // B-TREE PAGE READING
  // ===========================================================================

  /**
   * Read a specific B-tree page by page ID
   */
  async readBTreePage(indexName: string, pageId: number): Promise<R2BTreePage> {
    const indexSection = await this.readIndexSection();
    const index = indexSection.data.indexes.find(i => i.name === indexName);
    if (!index) {
      throw new Error(`Index not found: ${indexName}`);
    }

    const pageInfo = index.pageDirectory.find(p => p.pageId === pageId);
    if (!pageInfo) {
      throw new Error(`Page not found: ${pageId}`);
    }

    this.queryStats.pagesRead++;

    const header = await this.getHeader();
    const pageOffset = header.sections.index.offset + BigInt(4 + pageInfo.offset);
    // Calculate metadata size to skip
    const indexData = await this.readSection(header.sections.index);
    const metadataSize = new DataView(indexData.buffer).getUint32(0, true);

    const pageData = await this.fetchRange(
      header.sections.index.offset + BigInt(4 + metadataSize + pageInfo.offset),
      pageInfo.size
    );

    return this.parseBTreePage(pageData);
  }

  /**
   * Parse a B-tree page from bytes
   */
  private parseBTreePage(data: Uint8Array): R2BTreePage {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    let pos = 0;

    const type = view.getUint8(pos++) as R2BTreePageType;
    const entryCount = view.getUint16(pos, true);
    pos += 2;
    const freeSpace = view.getUint16(pos, true);
    pos += 2;
    const nextLeaf = view.getInt32(pos, true);
    pos += 4;
    const prevLeaf = view.getInt32(pos, true);
    pos += 4;

    // Read key offsets
    const keyOffsets: number[] = [];
    for (let i = 0; i < entryCount; i++) {
      keyOffsets.push(view.getUint32(pos, true));
      pos += 4;
    }

    // Read value/child offsets
    const valueOffsets: number[] = [];
    const children: number[] = [];
    if (type === R2BTreePageType.LEAF) {
      for (let i = 0; i < entryCount; i++) {
        valueOffsets.push(view.getUint32(pos, true));
        pos += 4;
      }
    } else {
      // Internal node has entryCount + 1 children
      for (let i = 0; i <= entryCount; i++) {
        children.push(view.getUint32(pos, true));
        pos += 4;
      }
    }

    // Extract keys
    const keys: Uint8Array[] = [];
    for (let i = 0; i < entryCount; i++) {
      const start = keyOffsets[i];
      const end = i < entryCount - 1 ? keyOffsets[i + 1] : (valueOffsets[0] || data.length);
      keys.push(data.slice(start, end));
    }

    // Extract values (leaf only)
    const values: Uint8Array[] = [];
    if (type === R2BTreePageType.LEAF) {
      for (let i = 0; i < entryCount; i++) {
        const start = valueOffsets[i];
        const end = i < entryCount - 1 ? valueOffsets[i + 1] : data.length;
        values.push(data.slice(start, end));
      }
    }

    return {
      type,
      entryCount,
      freeSpace,
      nextLeaf,
      prevLeaf,
      keys,
      values,
      children,
    };
  }

  // ===========================================================================
  // BLOOM FILTER QUERIES
  // ===========================================================================

  /**
   * Check if a value might exist using bloom filter
   */
  async bloomMightContain(columnName: string, value: R2IndexValue): Promise<boolean> {
    const bloomSection = await this.readBloomSection();
    const filter = bloomSection.data.filters.find(f => f.columnName === columnName);

    if (!filter) {
      // No bloom filter for this column - assume it might contain
      return true;
    }

    this.queryStats.bloomChecks++;

    const encodedValue = encodeValue(value);
    const hash1 = murmur3_32(encodedValue, filter.hashSeed);
    const hash2 = murmur3_32(encodedValue, hash1);

    for (let i = 0; i < filter.numHashes; i++) {
      const bit = (hash1 + i * hash2) % filter.numBits;
      if ((filter.bits[Math.floor(bit / 8)] & (1 << (bit % 8))) === 0) {
        return false;
      }
    }

    this.queryStats.bloomHits++;
    return true;
  }

  // ===========================================================================
  // STATISTICS QUERIES
  // ===========================================================================

  /**
   * Get column statistics
   */
  async getColumnStats(columnName: string): Promise<R2ColumnStats | undefined> {
    const statsSection = await this.readStatsSection();
    return statsSection.data.global.columns.get(columnName);
  }

  /**
   * Get chunks that might match a predicate based on statistics
   */
  async getMatchingChunks(predicate: R2IndexPredicate): Promise<R2ChunkStats[]> {
    const statsSection = await this.readStatsSection();
    const matchingChunks: R2ChunkStats[] = [];

    for (const chunk of statsSection.data.chunks) {
      const colStats = chunk.columns.get(predicate.column);
      if (!colStats) {
        // No stats for this column - include chunk
        matchingChunks.push(chunk);
        continue;
      }

      if (this.chunkMightMatch(colStats, predicate)) {
        matchingChunks.push(chunk);
      } else {
        this.queryStats.chunksPruned++;
      }
    }

    return matchingChunks;
  }

  /**
   * Check if a chunk might match a predicate based on column stats
   */
  private chunkMightMatch(stats: R2ColumnStats, predicate: R2IndexPredicate): boolean {
    const { min, max } = stats;
    const value = predicate.value as R2IndexValue;

    if (min === null || max === null) {
      // All nulls - only matches IS NULL
      return predicate.op === 'is_null';
    }

    switch (predicate.op) {
      case 'eq':
        // Value must be within [min, max]
        return compareValues(value, min) >= 0 && compareValues(value, max) <= 0;

      case 'ne':
        // Always might match unless all values are the same
        return !stats.allSame || compareValues(value, min) !== 0;

      case 'lt':
        // Value must be > min
        return compareValues(value, min) > 0;

      case 'le':
        // Value must be >= min
        return compareValues(value, min) >= 0;

      case 'gt':
        // Value must be < max
        return compareValues(value, max) < 0;

      case 'ge':
        // Value must be <= max
        return compareValues(value, max) <= 0;

      case 'between': {
        const value2 = predicate.value2 as R2IndexValue;
        // Range [value, value2] must overlap [min, max]
        return compareValues(value, max) <= 0 && compareValues(value2, min) >= 0;
      }

      case 'in': {
        // At least one value must be within [min, max]
        const values = predicate.value as R2IndexValue[];
        return values.some(v => compareValues(v, min) >= 0 && compareValues(v, max) <= 0);
      }

      case 'is_null':
        return stats.nullCount > 0n;

      case 'is_not_null':
        return stats.nullCount < (stats.distinctCount || 0n);

      case 'like':
        // Can't efficiently prune for LIKE - always include
        return true;

      default:
        return true;
    }
  }

  // ===========================================================================
  // INDEX LOOKUP
  // ===========================================================================

  /**
   * Perform a point lookup in the index
   */
  async lookup(columnName: string, value: R2IndexValue): Promise<R2IndexQueryResult> {
    const startTime = performance.now();
    this.queryStats = this.resetStats();

    // First check bloom filter
    const mightExist = await this.bloomMightContain(columnName, value);
    if (!mightExist) {
      return {
        matches: [],
        hasMore: false,
        stats: {
          ...this.queryStats,
          executionTimeMs: performance.now() - startTime,
        },
      };
    }

    // Find the index for this column
    const indexSection = await this.readIndexSection();
    const index = indexSection.data.indexes.find(i => i.columns.includes(columnName));
    if (!index) {
      throw new Error(`No index found for column: ${columnName}`);
    }

    // Search the B-tree
    const encodedKey = encodeValue(value);
    const matches = await this.searchBTree(index, encodedKey);

    // Load data section for file references
    const dataSection = await this.readDataSection();

    const result: R2IndexMatch[] = matches.map(rowId => {
      // Find the data file containing this row
      const dataFile = dataSection.data.files.find(
        f => rowId >= f.rowRange.start && rowId < f.rowRange.end
      );
      return {
        rowId,
        dataFile: dataFile!,
        key: value,
      };
    });

    return {
      matches: result,
      hasMore: false,
      stats: {
        ...this.queryStats,
        executionTimeMs: performance.now() - startTime,
      },
    };
  }

  /**
   * Perform a range scan in the index
   */
  async rangeScan(
    columnName: string,
    start: R2IndexValue | undefined,
    end: R2IndexValue | undefined,
    options?: { limit?: number; reverse?: boolean }
  ): Promise<R2IndexQueryResult> {
    const startTime = performance.now();
    this.queryStats = this.resetStats();

    // Find the index for this column
    const indexSection = await this.readIndexSection();
    const index = indexSection.data.indexes.find(i => i.columns.includes(columnName));
    if (!index) {
      throw new Error(`No index found for column: ${columnName}`);
    }

    const startKey = start ? encodeValue(start) : undefined;
    const endKey = end ? encodeValue(end) : undefined;

    const matches = await this.scanBTreeRange(
      index,
      startKey,
      endKey,
      options?.limit,
      options?.reverse
    );

    // Load data section for file references
    const dataSection = await this.readDataSection();

    const result: R2IndexMatch[] = matches.map(rowId => {
      const dataFile = dataSection.data.files.find(
        f => rowId >= f.rowRange.start && rowId < f.rowRange.end
      );
      return {
        rowId,
        dataFile: dataFile!,
      };
    });

    return {
      matches: result,
      hasMore: options?.limit !== undefined && result.length >= options.limit,
      stats: {
        ...this.queryStats,
        executionTimeMs: performance.now() - startTime,
      },
    };
  }

  /**
   * Search B-tree for exact key match
   */
  private async searchBTree(index: R2BTreeIndex, key: Uint8Array): Promise<bigint[]> {
    if (index.pageCount === 0) {
      return [];
    }

    // Find the root page (last page in directory for this index)
    let currentPageId = index.pageDirectory[index.pageDirectory.length - 1].pageId;

    while (true) {
      const page = await this.readBTreePage(index.name, currentPageId);

      // Binary search for key position
      const pos = this.binarySearchKeys(page.keys, key);

      if (page.type === R2BTreePageType.LEAF) {
        // Check if we found an exact match
        if (pos < page.keys.length && this.compareKeys(page.keys[pos], key) === 0) {
          // Found! Return the row ID
          const view = new DataView(
            page.values[pos].buffer,
            page.values[pos].byteOffset,
            page.values[pos].byteLength
          );
          return [view.getBigInt64(0, true)];
        }
        return [];
      } else {
        // Internal node - descend to appropriate child
        currentPageId = page.children[pos];
      }
    }
  }

  /**
   * Scan B-tree for a range of keys
   */
  private async scanBTreeRange(
    index: R2BTreeIndex,
    startKey: Uint8Array | undefined,
    endKey: Uint8Array | undefined,
    limit?: number,
    reverse?: boolean
  ): Promise<bigint[]> {
    if (index.pageCount === 0) {
      return [];
    }

    const results: bigint[] = [];

    // Find starting leaf page
    let currentPageId = await this.findLeafPage(index, startKey, reverse);

    while (currentPageId !== -1 && (limit === undefined || results.length < limit)) {
      const page = await this.readBTreePage(index.name, currentPageId);

      // Find starting position in this page
      let startPos = 0;
      let endPos = page.keys.length;

      if (startKey && !reverse) {
        startPos = this.binarySearchKeys(page.keys, startKey);
      }
      if (endKey && !reverse) {
        endPos = this.binarySearchKeys(page.keys, endKey);
      }

      // Collect matching entries
      const iterStart = reverse ? endPos - 1 : startPos;
      const iterEnd = reverse ? startPos - 1 : endPos;
      const iterStep = reverse ? -1 : 1;

      for (let i = iterStart; reverse ? i > iterEnd : i < iterEnd; i += iterStep) {
        const key = page.keys[i];

        // Check range bounds
        if (startKey && this.compareKeys(key, startKey) < 0) continue;
        if (endKey && this.compareKeys(key, endKey) >= 0) break;

        const view = new DataView(
          page.values[i].buffer,
          page.values[i].byteOffset,
          page.values[i].byteLength
        );
        results.push(view.getBigInt64(0, true));

        if (limit !== undefined && results.length >= limit) break;
      }

      // Move to next/prev leaf
      currentPageId = reverse ? page.prevLeaf : page.nextLeaf;
    }

    return results;
  }

  /**
   * Find the leaf page that would contain the given key
   */
  private async findLeafPage(
    index: R2BTreeIndex,
    key: Uint8Array | undefined,
    reverse?: boolean
  ): Promise<number> {
    if (index.pageCount === 0) {
      return -1;
    }

    // Start from root
    let currentPageId = index.pageDirectory[index.pageDirectory.length - 1].pageId;

    while (true) {
      const page = await this.readBTreePage(index.name, currentPageId);

      if (page.type === R2BTreePageType.LEAF) {
        return currentPageId;
      }

      // Find child to descend to
      let childIndex: number;
      if (key === undefined) {
        childIndex = reverse ? page.children.length - 1 : 0;
      } else {
        childIndex = this.binarySearchKeys(page.keys, key);
      }

      currentPageId = page.children[childIndex];
    }
  }

  /**
   * Binary search for key position in sorted keys array
   */
  private binarySearchKeys(keys: Uint8Array[], target: Uint8Array): number {
    let lo = 0;
    let hi = keys.length;

    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.compareKeys(keys[mid], target) < 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    return lo;
  }

  /**
   * Compare two encoded keys
   */
  private compareKeys(a: Uint8Array, b: Uint8Array): number {
    const minLen = Math.min(a.length, b.length);
    for (let i = 0; i < minLen; i++) {
      if (a[i] !== b[i]) {
        return a[i] - b[i];
      }
    }
    return a.length - b.length;
  }

  // ===========================================================================
  // UTILITY METHODS
  // ===========================================================================

  /**
   * Get query statistics from the last operation
   */
  getQueryStats(): R2QueryStats {
    return { ...this.queryStats };
  }

  /**
   * Clear all caches
   */
  clearCache(): void {
    this.schemaCache = undefined;
    this.indexCache = undefined;
    this.bloomCache = undefined;
    this.statsCache = undefined;
    this.dataCache = undefined;
  }

  /**
   * Check if the index file is valid
   */
  async validate(): Promise<{ valid: boolean; errors: string[] }> {
    const errors: string[] = [];

    try {
      const headerResult = await this.readHeader();
      if (!headerResult.valid) {
        errors.push(...(headerResult.errors || []));
      }

      // Verify footer
      const header = headerResult.header;
      const footer = await this.fetchRange(header.totalSize - 8n, 8);
      const decoder = new TextDecoder();
      const footerMagic = decoder.decode(footer);
      if (footerMagic !== R2_INDEX_MAGIC) {
        errors.push(`Invalid footer magic: expected "${R2_INDEX_MAGIC}", got "${footerMagic}"`);
      }

      // Try reading each section
      await this.readSchema();
      await this.readIndexSection();
      await this.readBloomSection();
      await this.readStatsSection();
      await this.readDataSection();
    } catch (e) {
      errors.push(`Validation error: ${e instanceof Error ? e.message : String(e)}`);
    }

    return { valid: errors.length === 0, errors };
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create an R2 index reader for an R2 object
 */
export function createR2IndexReader(bucket: R2Bucket, key: string, options?: Partial<R2IndexReadOptions>): R2IndexReader {
  return new R2IndexReader({ bucket, key, ...options });
}

/**
 * Create an R2 index reader from pre-loaded content
 */
export function createIndexReaderFromContent(content: Uint8Array, options?: Partial<R2IndexReadOptions>): R2IndexReader {
  return new R2IndexReader({ content, ...options });
}

/**
 * Quick helper to read just the header
 */
export async function readIndexHeader(bucket: R2Bucket, key: string): Promise<R2IndexHeader> {
  const reader = createR2IndexReader(bucket, key);
  return reader.getHeader();
}

/**
 * Quick helper to validate an index file
 */
export async function validateIndex(bucket: R2Bucket, key: string): Promise<{ valid: boolean; errors: string[] }> {
  const reader = createR2IndexReader(bucket, key);
  return reader.validate();
}
