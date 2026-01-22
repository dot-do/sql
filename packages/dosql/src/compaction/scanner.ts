/**
 * Compaction Scanner for DoSQL
 *
 * Scans the B-tree to find rows eligible for compaction.
 * Supports filtering by age, LSN, and batched scanning.
 */

import type { BTree, KeyCodec, ValueCodec } from '../btree/types.js';
import type { FSXBackend } from '../fsx/types.js';
import type {
  CompactionConfig,
  CompactionCandidate,
  ScanResult,
  ScanOptions,
} from './types.js';
import { CompactionError, CompactionErrorCode } from './types.js';

// =============================================================================
// Row Metadata Tracking
// =============================================================================

/**
 * Metadata tracked for each row for compaction decisions
 */
interface RowMetadata {
  /** When the row was last modified (ms timestamp) */
  lastModifiedAt: number;

  /** LSN of the last modification */
  lsn?: bigint;

  /** Estimated size in bytes */
  sizeBytes: number;
}

/**
 * Storage key for row metadata
 */
function metadataKey(config: CompactionConfig, key: string): string {
  return `${config.metadataPrefix}meta/${key}`;
}

// =============================================================================
// Scanner Implementation
// =============================================================================

/**
 * Scanner for finding compaction candidates in the B-tree
 */
export class CompactionScanner<K, V> {
  private readonly btree: BTree<K, V>;
  private readonly fsx: FSXBackend;
  private readonly keyCodec: KeyCodec<K>;
  private readonly valueCodec: ValueCodec<V>;
  private readonly config: CompactionConfig;

  /** Cache of row metadata */
  private metadataCache = new Map<string, RowMetadata>();

  /** Last scan cursor for pagination */
  private lastCursor?: K;

  constructor(
    btree: BTree<K, V>,
    fsx: FSXBackend,
    keyCodec: KeyCodec<K>,
    valueCodec: ValueCodec<V>,
    config: CompactionConfig
  ) {
    this.btree = btree;
    this.fsx = fsx;
    this.keyCodec = keyCodec;
    this.valueCodec = valueCodec;
    this.config = config;
  }

  /**
   * Scan the B-tree for compaction candidates
   */
  async scan(options: ScanOptions<K> = {}): Promise<ScanResult<K, V>> {
    const {
      afterKey,
      limit = this.config.scanBatchSize,
      minAge = this.config.ageThreshold,
      maxLSN,
    } = options;

    const candidates: CompactionCandidate<K, V>[] = [];
    let scanned = 0;
    let totalSize = 0;
    let cursor: K | undefined;
    let minLSN: bigint | undefined;
    let maxFoundLSN: bigint | undefined;

    const now = Date.now();

    try {
      // Iterate through B-tree entries
      const iterator = afterKey
        ? this.btree.range(afterKey, this.maxKey())
        : this.btree.entries();

      for await (const [key, value] of iterator) {
        // Skip the afterKey itself (it was already processed)
        if (afterKey !== undefined && this.keyCodec.compare(key, afterKey) === 0) {
          continue;
        }

        scanned++;
        cursor = key;

        // Get metadata for this row
        const metadata = await this.getRowMetadata(key, value);

        // Check age threshold
        const age = now - metadata.lastModifiedAt;
        if (age < minAge) {
          continue;
        }

        // Check LSN threshold if specified
        if (maxLSN !== undefined && metadata.lsn !== undefined) {
          if (metadata.lsn > maxLSN) {
            continue;
          }
        }

        // Track LSN range
        if (metadata.lsn !== undefined) {
          if (minLSN === undefined || metadata.lsn < minLSN) {
            minLSN = metadata.lsn;
          }
          if (maxFoundLSN === undefined || metadata.lsn > maxFoundLSN) {
            maxFoundLSN = metadata.lsn;
          }
        }

        // Add as candidate
        candidates.push({
          key,
          value,
          age,
          size: metadata.sizeBytes,
          lsn: metadata.lsn,
        });

        totalSize += metadata.sizeBytes;

        // Check if we've hit the limit
        if (candidates.length >= limit) {
          break;
        }
      }

      // Determine if there are more candidates
      const hasMore = scanned > 0 && candidates.length >= limit;

      this.lastCursor = cursor;

      return {
        candidates,
        hasMore,
        cursor,
        scanned,
        totalSize,
        lsnRange: minLSN !== undefined && maxFoundLSN !== undefined
          ? [minLSN, maxFoundLSN]
          : undefined,
      };
    } catch (error) {
      throw new CompactionError(
        CompactionErrorCode.SCAN_FAILED,
        `Failed to scan B-tree: ${error instanceof Error ? error.message : 'Unknown error'}`,
        undefined,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Scan all eligible candidates (paginated internally)
   * Returns an async iterator for memory efficiency
   */
  async *scanAll(options: Omit<ScanOptions<K>, 'afterKey'> = {}): AsyncIterableIterator<CompactionCandidate<K, V>> {
    let afterKey: K | undefined;
    let hasMore = true;

    while (hasMore) {
      const result = await this.scan({ ...options, afterKey });

      for (const candidate of result.candidates) {
        yield candidate;
      }

      hasMore = result.hasMore;
      afterKey = result.cursor;
    }
  }

  /**
   * Count eligible rows without returning them
   */
  async countEligible(minAge?: number): Promise<{ count: number; totalSize: number }> {
    let count = 0;
    let totalSize = 0;
    const threshold = minAge ?? this.config.ageThreshold;
    const now = Date.now();

    for await (const [key, value] of this.btree.entries()) {
      const metadata = await this.getRowMetadata(key, value);
      const age = now - metadata.lastModifiedAt;

      if (age >= threshold) {
        count++;
        totalSize += metadata.sizeBytes;
      }
    }

    return { count, totalSize };
  }

  /**
   * Get the total number of rows in the B-tree
   */
  async getTotalRows(): Promise<number> {
    return this.btree.count();
  }

  /**
   * Estimate total B-tree size
   */
  async estimateTotalSize(): Promise<number> {
    let totalSize = 0;

    for await (const [key, value] of this.btree.entries()) {
      const metadata = await this.getRowMetadata(key, value);
      totalSize += metadata.sizeBytes;
    }

    return totalSize;
  }

  /**
   * Check if compaction thresholds are met
   */
  async checkThresholds(): Promise<{
    rowThresholdMet: boolean;
    ageThresholdMet: boolean;
    sizeThresholdMet: boolean;
    rowCount: number;
    eligibleCount: number;
    totalSize: number;
  }> {
    const rowCount = await this.getTotalRows();
    const { count: eligibleCount, totalSize } = await this.countEligible();

    return {
      rowThresholdMet: rowCount >= this.config.rowThreshold,
      ageThresholdMet: eligibleCount > 0,
      sizeThresholdMet: totalSize >= this.config.sizeThreshold,
      rowCount,
      eligibleCount,
      totalSize,
    };
  }

  /**
   * Track metadata for a row (call this when rows are inserted/updated)
   */
  async trackRow(key: K, value: V, lsn?: bigint): Promise<void> {
    const keyStr = this.keyToString(key);
    const sizeBytes = this.estimateRowSize(key, value);

    const metadata: RowMetadata = {
      lastModifiedAt: Date.now(),
      lsn,
      sizeBytes,
    };

    // Store in cache
    this.metadataCache.set(keyStr, metadata);

    // Persist to storage
    await this.fsx.write(
      metadataKey(this.config, keyStr),
      new TextEncoder().encode(JSON.stringify(metadata))
    );
  }

  /**
   * Remove metadata for deleted rows
   */
  async untrackRow(key: K): Promise<void> {
    const keyStr = this.keyToString(key);
    this.metadataCache.delete(keyStr);
    await this.fsx.delete(metadataKey(this.config, keyStr));
  }

  /**
   * Batch remove metadata for multiple rows
   */
  async untrackRows(keys: K[]): Promise<void> {
    await Promise.all(keys.map((key) => this.untrackRow(key)));
  }

  /**
   * Clear the metadata cache
   */
  clearCache(): void {
    this.metadataCache.clear();
  }

  /**
   * Reset the scan cursor
   */
  resetCursor(): void {
    this.lastCursor = undefined;
  }

  // =============================================================================
  // Private Methods
  // =============================================================================

  /**
   * Get or compute row metadata
   */
  private async getRowMetadata(key: K, value: V): Promise<RowMetadata> {
    const keyStr = this.keyToString(key);

    // Check cache first
    if (this.metadataCache.has(keyStr)) {
      return this.metadataCache.get(keyStr)!;
    }

    // Try to load from storage
    const storedData = await this.fsx.read(metadataKey(this.config, keyStr));
    if (storedData) {
      try {
        const metadata = JSON.parse(new TextDecoder().decode(storedData)) as RowMetadata;
        // Convert LSN back to bigint if present
        if (metadata.lsn !== undefined) {
          metadata.lsn = BigInt(metadata.lsn as unknown as string);
        }
        this.metadataCache.set(keyStr, metadata);
        return metadata;
      } catch {
        // Invalid stored metadata, fall through to estimation
      }
    }

    // Estimate metadata for rows without tracking
    const metadata: RowMetadata = {
      // Assume row was created recently if no metadata exists
      // In practice, you'd want a more sophisticated approach
      lastModifiedAt: Date.now() - this.config.ageThreshold - 1, // Assume old enough
      sizeBytes: this.estimateRowSize(key, value),
    };

    this.metadataCache.set(keyStr, metadata);
    return metadata;
  }

  /**
   * Convert a key to a string for metadata lookup
   */
  private keyToString(key: K): string {
    const encoded = this.keyCodec.encode(key);
    return btoa(String.fromCharCode(...encoded));
  }

  /**
   * Estimate the size of a row in bytes
   */
  private estimateRowSize(key: K, value: V): number {
    const keyBytes = this.keyCodec.encode(key);
    const valueBytes = this.valueCodec.encode(value);
    // Add overhead for B-tree node storage
    return keyBytes.length + valueBytes.length + 16;
  }

  /**
   * Get a "maximum" key for range scanning
   * This is a hack - ideally the B-tree would support open-ended ranges
   */
  private maxKey(): K {
    // Return the current key with modified bytes to be "larger"
    // This is codec-specific and may need customization
    return undefined as unknown as K;
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a compaction scanner
 */
export function createScanner<K, V>(
  btree: BTree<K, V>,
  fsx: FSXBackend,
  keyCodec: KeyCodec<K>,
  valueCodec: ValueCodec<V>,
  config: CompactionConfig
): CompactionScanner<K, V> {
  return new CompactionScanner(btree, fsx, keyCodec, valueCodec, config);
}

// =============================================================================
// Batch Scanner
// =============================================================================

/**
 * Collect candidates into batches for efficient processing
 */
export async function collectBatch<K, V>(
  scanner: CompactionScanner<K, V>,
  targetRows: number,
  targetBytes: number,
  maxLSN?: bigint
): Promise<{
  candidates: CompactionCandidate<K, V>[];
  totalSize: number;
  lsnRange?: [bigint, bigint];
}> {
  const candidates: CompactionCandidate<K, V>[] = [];
  let totalSize = 0;
  let minLSN: bigint | undefined;
  let maxFoundLSN: bigint | undefined;

  for await (const candidate of scanner.scanAll({ maxLSN })) {
    candidates.push(candidate);
    totalSize += candidate.size;

    // Track LSN range
    if (candidate.lsn !== undefined) {
      if (minLSN === undefined || candidate.lsn < minLSN) {
        minLSN = candidate.lsn;
      }
      if (maxFoundLSN === undefined || candidate.lsn > maxFoundLSN) {
        maxFoundLSN = candidate.lsn;
      }
    }

    // Check if we've hit targets
    if (candidates.length >= targetRows || totalSize >= targetBytes) {
      break;
    }
  }

  return {
    candidates,
    totalSize,
    lsnRange: minLSN !== undefined && maxFoundLSN !== undefined
      ? [minLSN, maxFoundLSN]
      : undefined,
  };
}
