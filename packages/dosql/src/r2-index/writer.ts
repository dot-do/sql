/**
 * R2 Index Writer
 *
 * Builds single-file R2 index optimized for byte-range requests.
 * Writes all sections in a single pass to produce the final index file.
 */

import {
  R2_INDEX_MAGIC,
  R2_INDEX_VERSION,
  R2_INDEX_HEADER_SIZE,
  R2_DEFAULT_PAGE_SIZE,
  R2_DEFAULT_BLOOM_FP_RATE,
  R2_MAX_INLINE_VALUE_SIZE,
  R2IndexFlags,
  R2BTreePageType,
  type R2IndexHeader,
  type R2IndexSectionOffsets,
  type SectionPointer,
  type R2IndexSchema,
  type R2IndexColumn,
  type R2IndexBTreeSection,
  type R2BTreeIndex,
  type R2BTreePage,
  type R2BTreePageInfo,
  type R2IndexBloomSection,
  type R2BloomFilter,
  type BloomFilterParams,
  type R2IndexStatsSection,
  type R2GlobalStats,
  type R2ChunkStats,
  type R2ColumnStats,
  type R2IndexDataSection,
  type R2DataFile,
  type R2DataStorageType,
  type R2IndexBuildOptions,
  type R2IndexValue,
  type R2IndexDataType,
  encodeValue,
  compareValues,
} from './types.js';

// =============================================================================
// BIGINT-SAFE SERIALIZATION
// =============================================================================

/**
 * Serialize a value to a string key for tracking distinct values.
 * Handles BigInt which JSON.stringify cannot serialize.
 */
function serializeValueKey(value: R2IndexValue): string {
  switch (value.type) {
    case 'null':
      return 'null';
    case 'boolean':
      return `b:${value.value}`;
    case 'int':
      return `i:${value.value.toString()}`;
    case 'float':
      return `f:${value.value}`;
    case 'string':
      return `s:${value.value}`;
    case 'bytes':
      return `y:${Array.from(value.value).join(',')}`;
    case 'timestamp':
      return `t:${value.value.toString()}`;
    case 'date':
      return `d:${value.value}`;
    case 'uuid':
      return `u:${value.value}`;
    default:
      return `?:${String((value as R2IndexValue))}`;
  }
}

// =============================================================================
// CRC32 CHECKSUM
// =============================================================================

/**
 * CRC32 lookup table
 */
const CRC32_TABLE: Uint32Array = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) {
      c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    }
    table[i] = c;
  }
  return table;
})();

/**
 * Calculate CRC32 checksum
 */
export function crc32(data: Uint8Array): number {
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < data.length; i++) {
    crc = CRC32_TABLE[(crc ^ data[i]) & 0xFF] ^ (crc >>> 8);
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

// =============================================================================
// BLOOM FILTER
// =============================================================================

/**
 * Create optimal bloom filter parameters
 */
export function calculateBloomParams(
  expectedElements: number,
  falsePositiveRate: number
): { numBits: number; numHashes: number } {
  // m = -n * ln(p) / (ln(2)^2)
  const numBits = Math.ceil(
    -expectedElements * Math.log(falsePositiveRate) / (Math.log(2) ** 2)
  );
  // k = m/n * ln(2)
  const numHashes = Math.ceil((numBits / expectedElements) * Math.log(2));
  return { numBits, numHashes };
}

/**
 * MurmurHash3 32-bit for bloom filter
 */
export function murmur3_32(key: Uint8Array, seed: number): number {
  const c1 = 0xcc9e2d51;
  const c2 = 0x1b873593;
  const r1 = 15;
  const r2 = 13;
  const m = 5;
  const n = 0xe6546b64;

  let hash = seed >>> 0;
  const len = key.length;
  const blocks = Math.floor(len / 4);

  // Body
  for (let i = 0; i < blocks; i++) {
    let k = (key[i * 4] |
      (key[i * 4 + 1] << 8) |
      (key[i * 4 + 2] << 16) |
      (key[i * 4 + 3] << 24)) >>> 0;

    k = Math.imul(k, c1) >>> 0;
    k = ((k << r1) | (k >>> (32 - r1))) >>> 0;
    k = Math.imul(k, c2) >>> 0;

    hash ^= k;
    hash = ((hash << r2) | (hash >>> (32 - r2))) >>> 0;
    hash = (Math.imul(hash, m) + n) >>> 0;
  }

  // Tail
  let k = 0;
  const tailIdx = blocks * 4;
  switch (len & 3) {
    case 3:
      k ^= key[tailIdx + 2] << 16;
    // falls through
    case 2:
      k ^= key[tailIdx + 1] << 8;
    // falls through
    case 1:
      k ^= key[tailIdx];
      k = Math.imul(k, c1) >>> 0;
      k = ((k << r1) | (k >>> (32 - r1))) >>> 0;
      k = Math.imul(k, c2) >>> 0;
      hash ^= k;
  }

  // Finalization
  hash ^= len;
  hash ^= hash >>> 16;
  hash = Math.imul(hash, 0x85ebca6b) >>> 0;
  hash ^= hash >>> 13;
  hash = Math.imul(hash, 0xc2b2ae35) >>> 0;
  hash ^= hash >>> 16;

  return hash >>> 0;
}

/**
 * Bloom filter builder
 */
export class BloomFilterBuilder {
  private bits: Uint8Array;
  private numBits: number;
  private numHashes: number;
  private hashSeed: number;
  private insertedElements: number = 0;

  constructor(params: BloomFilterParams) {
    const { numBits, numHashes } = calculateBloomParams(
      params.expectedElements,
      params.falsePositiveRate
    );
    this.numBits = numBits;
    this.numHashes = numHashes;
    this.hashSeed = params.hashSeed ?? 0x9747b28c;
    this.bits = new Uint8Array(Math.ceil(numBits / 8));
  }

  /**
   * Add a value to the bloom filter
   */
  add(value: Uint8Array): void {
    const hash1 = murmur3_32(value, this.hashSeed);
    const hash2 = murmur3_32(value, hash1);

    for (let i = 0; i < this.numHashes; i++) {
      const bit = (hash1 + i * hash2) % this.numBits;
      this.bits[Math.floor(bit / 8)] |= (1 << (bit % 8));
    }
    this.insertedElements++;
  }

  /**
   * Check if a value might be in the set
   */
  mightContain(value: Uint8Array): boolean {
    const hash1 = murmur3_32(value, this.hashSeed);
    const hash2 = murmur3_32(value, hash1);

    for (let i = 0; i < this.numHashes; i++) {
      const bit = (hash1 + i * hash2) % this.numBits;
      if ((this.bits[Math.floor(bit / 8)] & (1 << (bit % 8))) === 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Build the bloom filter structure
   */
  build(columnName: string, expectedElements: number): R2BloomFilter {
    // Calculate actual false positive rate
    const bitsSet = this.countBitsSet();
    const fillRatio = bitsSet / this.numBits;
    const actualFpRate = Math.pow(fillRatio, this.numHashes);

    return {
      columnName,
      numHashes: this.numHashes,
      numBits: this.numBits,
      expectedElements,
      insertedElements: this.insertedElements,
      falsePositiveRate: actualFpRate,
      bits: this.bits,
      hashSeed: this.hashSeed,
    };
  }

  private countBitsSet(): number {
    let count = 0;
    for (let i = 0; i < this.bits.length; i++) {
      let b = this.bits[i];
      while (b) {
        count += b & 1;
        b >>>= 1;
      }
    }
    return count;
  }
}

// =============================================================================
// B-TREE PAGE BUILDER
// =============================================================================

/**
 * B-tree page builder
 */
export class BTreePageBuilder {
  private pageSize: number;
  private pages: R2BTreePage[] = [];
  private currentPage: R2BTreePage;
  private currentSize: number = 0;
  private pageIdCounter: number = 0;

  constructor(pageSize: number = R2_DEFAULT_PAGE_SIZE) {
    this.pageSize = pageSize;
    this.currentPage = this.createEmptyPage(R2BTreePageType.LEAF);
  }

  private createEmptyPage(type: R2BTreePageType): R2BTreePage {
    return {
      type,
      entryCount: 0,
      freeSpace: this.pageSize - 13, // Header size
      nextLeaf: -1,
      prevLeaf: -1,
      keys: [],
      values: [],
      children: [],
    };
  }

  /**
   * Add a key-value entry to the B-tree
   */
  addEntry(key: Uint8Array, value: Uint8Array): void {
    const entrySize = 4 + key.length + 4 + value.length; // 4 bytes for each offset

    if (this.currentSize + entrySize > this.pageSize - 13) {
      // Page is full, create new page
      this.finalizePage();
      this.currentPage = this.createEmptyPage(R2BTreePageType.LEAF);
      this.currentSize = 0;
    }

    this.currentPage.keys.push(key);
    this.currentPage.values.push(value);
    this.currentPage.entryCount++;
    this.currentSize += entrySize;
  }

  /**
   * Finalize current page and add to list
   */
  private finalizePage(): void {
    if (this.currentPage.entryCount > 0) {
      // Set up leaf chain
      if (this.pages.length > 0) {
        const prevPage = this.pages[this.pages.length - 1];
        prevPage.nextLeaf = this.pageIdCounter;
        this.currentPage.prevLeaf = this.pageIdCounter - 1;
      }
      this.currentPage.freeSpace = this.pageSize - this.currentSize - 13;
      this.pages.push(this.currentPage);
      this.pageIdCounter++;
    }
  }

  /**
   * Build internal nodes to create the B-tree
   */
  private buildInternalNodes(): void {
    if (this.pages.length <= 1) {
      return; // No internal nodes needed
    }

    let currentLevel = this.pages.filter(p => p.type === R2BTreePageType.LEAF);

    while (currentLevel.length > 1) {
      const nextLevel: R2BTreePage[] = [];
      let internalPage = this.createEmptyPage(R2BTreePageType.INTERNAL);
      let currentInternalSize = 0;

      for (let i = 0; i < currentLevel.length; i++) {
        const childPageId = this.pages.indexOf(currentLevel[i]);
        const childFirstKey = currentLevel[i].keys[0] || new Uint8Array(0);
        const entrySize = 4 + childFirstKey.length + 4; // key offset + key + child pointer

        if (currentInternalSize + entrySize > this.pageSize - 13 && internalPage.children.length > 0) {
          this.pages.push(internalPage);
          nextLevel.push(internalPage);
          internalPage = this.createEmptyPage(R2BTreePageType.INTERNAL);
          currentInternalSize = 0;
        }

        if (i > 0) {
          internalPage.keys.push(childFirstKey);
        }
        internalPage.children.push(childPageId);
        internalPage.entryCount = internalPage.keys.length;
        currentInternalSize += entrySize;
      }

      if (internalPage.children.length > 0) {
        this.pages.push(internalPage);
        nextLevel.push(internalPage);
      }

      currentLevel = nextLevel;
    }
  }

  /**
   * Build the complete B-tree index
   */
  build(name: string, columns: string[], unique: boolean): R2BTreeIndex {
    // Finalize any remaining entries
    this.finalizePage();

    // Build internal nodes
    this.buildInternalNodes();

    // Calculate height
    let height = 1;
    let currentPageId = this.pages.length - 1;
    while (currentPageId >= 0 && this.pages[currentPageId].type === R2BTreePageType.INTERNAL) {
      height++;
      const children = this.pages[currentPageId].children;
      if (children.length > 0) {
        currentPageId = children[0];
      } else {
        break;
      }
    }

    // Count total entries
    let entryCount = 0n;
    for (const page of this.pages) {
      if (page.type === R2BTreePageType.LEAF) {
        entryCount += BigInt(page.entryCount);
      }
    }

    // Build page directory
    let offset = 0;
    const pageDirectory: R2BTreePageInfo[] = this.pages.map((page, idx) => {
      const serialized = this.serializePage(page);
      const info: R2BTreePageInfo = {
        pageId: idx,
        offset,
        size: serialized.length,
        type: page.type,
        firstKey: page.keys[0],
        lastKey: page.keys[page.keys.length - 1],
      };
      offset += serialized.length;
      return info;
    });

    return {
      name,
      columns,
      unique,
      rootOffset: pageDirectory.length > 0 ? pageDirectory[pageDirectory.length - 1].offset : 0,
      pageCount: this.pages.length,
      height,
      entryCount,
      pageSize: this.pageSize,
      pageDirectory,
    };
  }

  /**
   * Serialize a page to bytes
   */
  serializePage(page: R2BTreePage): Uint8Array {
    const keyData: Uint8Array[] = [];
    const valueData: Uint8Array[] = [];
    let totalKeySize = 0;
    let totalValueSize = 0;

    for (const key of page.keys) {
      keyData.push(key);
      totalKeySize += key.length;
    }

    for (const value of page.values) {
      valueData.push(value);
      totalValueSize += value.length;
    }

    // Calculate buffer size
    const headerSize = 13; // type(1) + count(2) + freeSpace(2) + nextLeaf(4) + prevLeaf(4)
    const keyOffsetsSize = page.keys.length * 4;
    const valueOffsetsSize = page.type === R2BTreePageType.LEAF
      ? page.values.length * 4
      : page.children.length * 4;
    const dataSize = totalKeySize + totalValueSize;
    const totalSize = headerSize + keyOffsetsSize + valueOffsetsSize + dataSize;

    const buffer = new ArrayBuffer(totalSize);
    const view = new DataView(buffer);
    const result = new Uint8Array(buffer);
    let pos = 0;

    // Header
    view.setUint8(pos++, page.type);
    view.setUint16(pos, page.entryCount, true);
    pos += 2;
    view.setUint16(pos, page.freeSpace, true);
    pos += 2;
    view.setInt32(pos, page.nextLeaf, true);
    pos += 4;
    view.setInt32(pos, page.prevLeaf, true);
    pos += 4;

    // Key offsets
    let dataOffset = headerSize + keyOffsetsSize + valueOffsetsSize;
    for (const key of keyData) {
      view.setUint32(pos, dataOffset, true);
      pos += 4;
      dataOffset += key.length;
    }

    // Value/child offsets
    if (page.type === R2BTreePageType.LEAF) {
      for (const value of valueData) {
        view.setUint32(pos, dataOffset, true);
        pos += 4;
        dataOffset += value.length;
      }
    } else {
      for (const childId of page.children) {
        view.setUint32(pos, childId, true);
        pos += 4;
      }
    }

    // Key data
    for (const key of keyData) {
      result.set(key, pos);
      pos += key.length;
    }

    // Value data
    for (const value of valueData) {
      result.set(value, pos);
      pos += value.length;
    }

    return result;
  }

  /**
   * Get all serialized pages
   */
  getSerializedPages(): Uint8Array[] {
    // Make sure to finalize the current page if it has entries
    if (this.currentPage.entryCount > 0 && !this.pages.includes(this.currentPage)) {
      this.finalizePage();
    }
    return this.pages.map(p => this.serializePage(p));
  }
}

// =============================================================================
// STATISTICS BUILDER
// =============================================================================

/**
 * Column statistics builder
 */
export class ColumnStatsBuilder {
  private min: R2IndexValue | null = null;
  private max: R2IndexValue | null = null;
  private nullCount: bigint = 0n;
  private distinctValues: Set<string> = new Set();
  private sum: number | bigint = 0;
  private count: bigint = 0n;
  private dataType: R2IndexDataType;
  private firstValue: R2IndexValue | null = null;
  private lastValue: R2IndexValue | null = null;
  private sorted: boolean = true;
  private sortDirection: 'asc' | 'desc' | null = null;

  constructor(dataType: R2IndexDataType) {
    this.dataType = dataType;
  }

  /**
   * Add a value to the statistics
   */
  add(value: R2IndexValue): void {
    if (value.type === 'null') {
      this.nullCount++;
      return;
    }

    this.count++;

    // Update min/max
    if (this.min === null || compareValues(value, this.min) < 0) {
      this.min = value;
    }
    if (this.max === null || compareValues(value, this.max) > 0) {
      this.max = value;
    }

    // Track distinct values (up to a limit for memory efficiency)
    if (this.distinctValues.size < 10000) {
      // Use a custom serialization to handle BigInt
      const key = serializeValueKey(value);
      this.distinctValues.add(key);
    }

    // Sum for numeric types
    if (value.type === 'int' || value.type === 'float') {
      if (typeof this.sum === 'bigint' && value.type === 'int') {
        this.sum = this.sum + value.value;
      } else {
        this.sum = Number(this.sum) + (value.type === 'int' ? Number(value.value) : value.value);
      }
    }

    // Track sort order
    if (this.lastValue !== null && this.sorted) {
      const cmp = compareValues(this.lastValue, value);
      if (this.sortDirection === null) {
        this.sortDirection = cmp <= 0 ? 'asc' : 'desc';
      } else if (
        (this.sortDirection === 'asc' && cmp > 0) ||
        (this.sortDirection === 'desc' && cmp < 0)
      ) {
        this.sorted = false;
      }
    }

    if (this.firstValue === null) {
      this.firstValue = value;
    }
    this.lastValue = value;
  }

  /**
   * Build the column statistics
   */
  build(): R2ColumnStats {
    const total = this.count + this.nullCount;
    const avg = this.count > 0n && typeof this.sum === 'number'
      ? this.sum / Number(this.count)
      : undefined;

    return {
      min: this.min,
      max: this.max,
      nullCount: this.nullCount,
      distinctCount: BigInt(this.distinctValues.size),
      sum: this.count > 0n ? this.sum : undefined,
      avg,
      allSame: this.min !== null && this.max !== null && compareValues(this.min, this.max) === 0,
      sorted: this.sorted && this.count > 1n,
      sortDirection: this.sorted ? this.sortDirection || undefined : undefined,
    };
  }
}

// =============================================================================
// R2 INDEX WRITER
// =============================================================================

/**
 * R2 Index file writer
 */
export class R2IndexWriter {
  private options: R2IndexBuildOptions;
  private schema: R2IndexSchema;
  private btreeBuilders: Map<string, BTreePageBuilder> = new Map();
  private bloomBuilders: Map<string, BloomFilterBuilder> = new Map();
  private statsBuilders: Map<string, ColumnStatsBuilder> = new Map();
  private chunkStats: R2ChunkStats[] = [];
  private dataFiles: R2DataFile[] = [];
  private rowCount: bigint = 0n;

  constructor(options: R2IndexBuildOptions) {
    this.options = {
      pageSize: R2_DEFAULT_PAGE_SIZE,
      bloomFalsePositiveRate: R2_DEFAULT_BLOOM_FP_RATE,
      computeStats: true,
      computeChecksums: true,
      ...options,
    };

    this.schema = {
      tableName: options.tableName,
      columns: options.columns,
      primaryKey: options.primaryKey,
    };

    // Initialize builders for indexed columns
    for (const col of options.indexedColumns || options.primaryKey) {
      this.btreeBuilders.set(col, new BTreePageBuilder(this.options.pageSize));
    }

    // Initialize bloom filter builders
    for (const col of options.bloomColumns || []) {
      const columnDef = options.columns.find(c => c.name === col);
      if (columnDef) {
        this.bloomBuilders.set(col, new BloomFilterBuilder({
          expectedElements: 100000, // Will be updated as we add data
          falsePositiveRate: this.options.bloomFalsePositiveRate!,
        }));
      }
    }

    // Initialize stats builders for all columns
    if (this.options.computeStats) {
      for (const col of options.columns) {
        this.statsBuilders.set(col.name, new ColumnStatsBuilder(col.type));
      }
    }
  }

  /**
   * Add a row to the index
   */
  addRow(row: Record<string, R2IndexValue>, rowId: bigint): void {
    this.rowCount++;

    // Add to B-tree indexes
    for (const [colName, builder] of this.btreeBuilders) {
      const value = row[colName];
      if (value) {
        const key = encodeValue(value);
        const rowIdBytes = new ArrayBuffer(8);
        new DataView(rowIdBytes).setBigInt64(0, rowId, true);
        builder.addEntry(key, new Uint8Array(rowIdBytes));
      }
    }

    // Add to bloom filters
    for (const [colName, builder] of this.bloomBuilders) {
      const value = row[colName];
      if (value && value.type !== 'null') {
        builder.add(encodeValue(value));
      }
    }

    // Update statistics
    if (this.options.computeStats) {
      for (const [colName, value] of Object.entries(row)) {
        const statsBuilder = this.statsBuilders.get(colName);
        if (statsBuilder) {
          statsBuilder.add(value);
        }
      }
    }
  }

  /**
   * Add a data file reference
   */
  addDataFile(file: R2DataFile): void {
    this.dataFiles.push(file);
  }

  /**
   * Add chunk statistics
   */
  addChunkStats(stats: R2ChunkStats): void {
    this.chunkStats.push(stats);
  }

  /**
   * Build and return the complete index file as bytes
   */
  build(): Uint8Array {
    // Build all sections
    const schemaSection = this.buildSchemaSection();
    const indexSection = this.buildIndexSection();
    const bloomSection = this.buildBloomSection();
    const statsSection = this.buildStatsSection();
    const dataSection = this.buildDataSection();

    // Calculate flags
    let flags = R2IndexFlags.NONE;
    if (bloomSection.length > 0) flags |= R2IndexFlags.HAS_BLOOM;
    if (statsSection.length > 0) flags |= R2IndexFlags.HAS_STATS;
    if (indexSection.length > 0) flags |= R2IndexFlags.HAS_BTREE;
    if (this.options.computeChecksums) flags |= R2IndexFlags.HAS_CHECKSUMS;

    // Calculate offsets
    const headerSize = R2_INDEX_HEADER_SIZE;
    let currentOffset = BigInt(headerSize);

    const schemaOffset = currentOffset;
    currentOffset += BigInt(schemaSection.length);

    const indexOffset = currentOffset;
    currentOffset += BigInt(indexSection.length);

    const bloomOffset = currentOffset;
    currentOffset += BigInt(bloomSection.length);

    const statsOffset = currentOffset;
    currentOffset += BigInt(statsSection.length);

    const dataOffset = currentOffset;
    currentOffset += BigInt(dataSection.length);

    const footerOffset = currentOffset;
    const totalSize = currentOffset + 8n; // Footer is 8 bytes

    // Build section offsets
    const sections: R2IndexSectionOffsets = {
      schema: {
        offset: schemaOffset,
        size: schemaSection.length,
        checksum: this.options.computeChecksums ? crc32(schemaSection) : 0,
      },
      index: {
        offset: indexOffset,
        size: indexSection.length,
        checksum: this.options.computeChecksums ? crc32(indexSection) : 0,
      },
      bloom: {
        offset: bloomOffset,
        size: bloomSection.length,
        checksum: this.options.computeChecksums ? crc32(bloomSection) : 0,
      },
      stats: {
        offset: statsOffset,
        size: statsSection.length,
        checksum: this.options.computeChecksums ? crc32(statsSection) : 0,
      },
      data: {
        offset: dataOffset,
        size: dataSection.length,
        checksum: this.options.computeChecksums ? crc32(dataSection) : 0,
      },
    };

    // Build header
    const header = this.buildHeader(flags, sections, totalSize);

    // Combine all sections
    const result = new Uint8Array(Number(totalSize));
    let pos = 0;

    result.set(header, pos);
    pos += header.length;

    result.set(schemaSection, pos);
    pos += schemaSection.length;

    result.set(indexSection, pos);
    pos += indexSection.length;

    result.set(bloomSection, pos);
    pos += bloomSection.length;

    result.set(statsSection, pos);
    pos += statsSection.length;

    result.set(dataSection, pos);
    pos += dataSection.length;

    // Footer (magic bytes)
    const encoder = new TextEncoder();
    result.set(encoder.encode(R2_INDEX_MAGIC), pos);

    return result;
  }

  /**
   * Build the header section
   */
  private buildHeader(
    flags: number,
    sections: R2IndexSectionOffsets,
    totalSize: bigint
  ): Uint8Array {
    const buffer = new ArrayBuffer(R2_INDEX_HEADER_SIZE);
    const view = new DataView(buffer);
    const result = new Uint8Array(buffer);
    const encoder = new TextEncoder();

    let pos = 0;

    // Magic (8 bytes)
    result.set(encoder.encode(R2_INDEX_MAGIC), pos);
    pos += 8;

    // Version (4 bytes)
    view.setUint32(pos, R2_INDEX_VERSION, true);
    pos += 4;

    // Flags (4 bytes)
    view.setUint32(pos, flags, true);
    pos += 4;

    // Header size (4 bytes)
    view.setUint32(pos, R2_INDEX_HEADER_SIZE, true);
    pos += 4;

    // Section offsets (5 sections * 16 bytes each = 80 bytes)
    for (const section of [
      sections.schema,
      sections.index,
      sections.bloom,
      sections.stats,
      sections.data,
    ]) {
      view.setBigUint64(pos, section.offset, true);
      pos += 8;
      view.setUint32(pos, section.size, true);
      pos += 4;
      view.setUint32(pos, section.checksum, true);
      pos += 4;
    }

    // Total size (8 bytes)
    view.setBigUint64(pos, totalSize, true);
    pos += 8;

    // Created at (8 bytes)
    view.setBigUint64(pos, BigInt(Date.now()), true);
    pos += 8;

    // Row count (8 bytes)
    view.setBigUint64(pos, this.rowCount, true);
    pos += 8;

    // Header checksum (4 bytes) - calculate over all previous bytes
    const checksumData = result.slice(0, pos);
    view.setUint32(pos, crc32(checksumData), true);
    pos += 4;

    // Remaining bytes are reserved (padding to 128 bytes)

    return result;
  }

  /**
   * Build the schema section (JSON-encoded)
   */
  private buildSchemaSection(): Uint8Array {
    const encoder = new TextEncoder();
    return encoder.encode(JSON.stringify(this.schema));
  }

  /**
   * Build the B-tree index section
   */
  private buildIndexSection(): Uint8Array {
    const indexes: R2BTreeIndex[] = [];
    const allPageData: Uint8Array[] = [];

    for (const [colName, builder] of this.btreeBuilders) {
      const index = builder.build(
        `idx_${this.schema.tableName}_${colName}`,
        [colName],
        this.options.primaryKey.includes(colName)
      );
      indexes.push(index);
      allPageData.push(...builder.getSerializedPages());
    }

    // Serialize index metadata
    const encoder = new TextEncoder();
    const metadataJson = JSON.stringify({
      indexes: indexes.map(idx => ({
        ...idx,
        entryCount: idx.entryCount.toString(),
        pageDirectory: idx.pageDirectory.map(p => ({
          ...p,
          firstKey: p.firstKey ? Array.from(p.firstKey) : undefined,
          lastKey: p.lastKey ? Array.from(p.lastKey) : undefined,
        })),
      })),
    });
    const metadataBytes = encoder.encode(metadataJson);

    // Calculate total size
    const metadataSizeBytes = 4;
    let pagesSize = 0;
    for (const page of allPageData) {
      pagesSize += page.length;
    }

    const result = new Uint8Array(metadataSizeBytes + metadataBytes.length + pagesSize);
    const view = new DataView(result.buffer);
    let pos = 0;

    // Metadata size
    view.setUint32(pos, metadataBytes.length, true);
    pos += 4;

    // Metadata
    result.set(metadataBytes, pos);
    pos += metadataBytes.length;

    // Page data
    for (const page of allPageData) {
      result.set(page, pos);
      pos += page.length;
    }

    return result;
  }

  /**
   * Build the bloom filter section
   */
  private buildBloomSection(): Uint8Array {
    if (this.bloomBuilders.size === 0) {
      return new Uint8Array(0);
    }

    const filters: R2BloomFilter[] = [];
    for (const [colName, builder] of this.bloomBuilders) {
      filters.push(builder.build(colName, Number(this.rowCount)));
    }

    const encoder = new TextEncoder();
    const filtersJson = JSON.stringify({
      filters: filters.map(f => ({
        ...f,
        bits: Array.from(f.bits),
      })),
    });

    return encoder.encode(filtersJson);
  }

  /**
   * Build the statistics section
   */
  private buildStatsSection(): Uint8Array {
    if (!this.options.computeStats) {
      return new Uint8Array(0);
    }

    const columnStats: Record<string, R2ColumnStats> = {};
    for (const [colName, builder] of this.statsBuilders) {
      columnStats[colName] = builder.build();
    }

    const globalStats: R2GlobalStats = {
      rowCount: this.rowCount,
      totalBytes: 0n, // Will be updated
      chunkCount: this.chunkStats.length,
      columns: new Map(Object.entries(columnStats)),
    };

    const encoder = new TextEncoder();
    const statsJson = JSON.stringify({
      global: {
        rowCount: globalStats.rowCount.toString(),
        totalBytes: globalStats.totalBytes.toString(),
        chunkCount: globalStats.chunkCount,
        columns: Object.fromEntries(
          Array.from(globalStats.columns.entries()).map(([k, v]) => [
            k,
            this.serializeColumnStats(v),
          ])
        ),
      },
      chunks: this.chunkStats.map(c => ({
        ...c,
        rowRange: {
          start: c.rowRange.start.toString(),
          end: c.rowRange.end.toString(),
        },
        byteRange: {
          offset: c.byteRange.offset.toString(),
          size: c.byteRange.size,
        },
        columns: Object.fromEntries(
          Array.from(c.columns.entries()).map(([k, v]) => [k, this.serializeColumnStats(v)])
        ),
      })),
    });

    return encoder.encode(statsJson);
  }

  /**
   * Serialize column stats to JSON-safe format
   */
  private serializeColumnStats(stats: R2ColumnStats): Record<string, unknown> {
    return {
      min: this.serializeValue(stats.min),
      max: this.serializeValue(stats.max),
      nullCount: stats.nullCount.toString(),
      distinctCount: stats.distinctCount?.toString(),
      sum: typeof stats.sum === 'bigint' ? stats.sum.toString() : stats.sum,
      avg: stats.avg,
      allSame: stats.allSame,
      sorted: stats.sorted,
      sortDirection: stats.sortDirection,
    };
  }

  /**
   * Serialize an R2IndexValue to JSON-safe format
   */
  private serializeValue(value: R2IndexValue | null): Record<string, unknown> | null {
    if (value === null) return null;
    switch (value.type) {
      case 'null':
        return { type: 'null' };
      case 'boolean':
        return { type: 'boolean', value: value.value };
      case 'int':
        return { type: 'int', value: value.value.toString() };
      case 'float':
        return { type: 'float', value: value.value };
      case 'string':
        return { type: 'string', value: value.value };
      case 'bytes':
        return { type: 'bytes', value: Array.from(value.value) };
      case 'timestamp':
        return { type: 'timestamp', value: value.value.toString() };
      case 'date':
        return { type: 'date', value: value.value };
      case 'uuid':
        return { type: 'uuid', value: value.value };
      default:
        return null;
    }
  }

  /**
   * Build the data pointers section
   */
  private buildDataSection(): Uint8Array {
    const section: R2IndexDataSection = {
      storageType: this.dataFiles.length > 0
        ? (this.dataFiles[0].location.type === 'r2' ? 'external-r2' : 'external-url')
        : 'inline',
      files: this.dataFiles,
      totalSize: this.dataFiles.reduce((sum, f) => sum + f.size, 0n),
    };

    const encoder = new TextEncoder();
    const dataJson = JSON.stringify({
      storageType: section.storageType,
      totalSize: section.totalSize.toString(),
      files: section.files.map(f => ({
        ...f,
        rowRange: {
          start: f.rowRange.start.toString(),
          end: f.rowRange.end.toString(),
        },
        size: f.size.toString(),
        location: f.location.type === 'inline'
          ? { type: 'inline', offset: f.location.offset.toString(), size: f.location.size }
          : f.location,
      })),
    });

    return encoder.encode(dataJson);
  }
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Create an R2 index writer with the given options
 */
export function createIndexWriter(options: R2IndexBuildOptions): R2IndexWriter {
  return new R2IndexWriter(options);
}

/**
 * Build an index from an array of rows
 */
export function buildIndex(
  options: R2IndexBuildOptions,
  rows: Array<Record<string, R2IndexValue>>
): Uint8Array {
  const writer = createIndexWriter(options);

  let rowId = 0n;
  for (const row of rows) {
    writer.addRow(row, rowId++);
  }

  return writer.build();
}
