/**
 * WAL Reader for DoSQL
 *
 * Reads WAL segments from fsx storage with support for:
 * - Range queries (fromLSN, toLSN)
 * - Replay from checkpoint
 * - Filtering by table/operation
 * - Async iteration
 */

import type { FSXBackend } from '../fsx/types.js';
import {
  type WALEntry,
  type WALSegment,
  type WALConfig,
  type WALReader,
  type WALEncoder,
  type ReadOptions,
  type WALOperation,
  DEFAULT_WAL_CONFIG,
  WALError,
  WALErrorCode,
} from './types.js';
import { DefaultWALEncoder } from './writer.js';

// =============================================================================
// WAL Reader Implementation
// =============================================================================

/**
 * Creates a new WAL Reader
 */
export function createWALReader(
  backend: FSXBackend,
  config: Partial<WALConfig> = {},
  encoder?: WALEncoder
): WALReader {
  const fullConfig: WALConfig = { ...DEFAULT_WAL_CONFIG, ...config };
  const walEncoder = encoder ?? new DefaultWALEncoder();

  /**
   * Parse segment ID to extract start LSN
   */
  function parseSegmentLSN(segmentId: string): bigint {
    // Format: seg_00000000000000000000
    const match = segmentId.match(/seg_(\d+)/);
    if (!match) {
      throw new WALError(
        WALErrorCode.SEGMENT_CORRUPTED,
        `Invalid segment ID format: ${segmentId}`,
        undefined,
        segmentId
      );
    }
    return BigInt(match[1]);
  }

  /**
   * Load and decode a segment from storage
   */
  async function loadSegment(segmentId: string): Promise<WALSegment | null> {
    const path = `${fullConfig.segmentPrefix}${segmentId}`;
    const data = await backend.read(path);

    if (!data) {
      return null;
    }

    try {
      const segment = walEncoder.decodeSegment(data);

      // Verify checksum if enabled
      if (fullConfig.verifyChecksums) {
        // Recalculate checksum (set checksum to 0 for calculation)
        const verifyData = walEncoder.encodeSegment({
          ...segment,
          checksum: 0,
        });
        const calculatedChecksum = walEncoder.calculateChecksum(verifyData);

        if (calculatedChecksum !== segment.checksum) {
          throw new WALError(
            WALErrorCode.CHECKSUM_MISMATCH,
            `Checksum mismatch for segment ${segmentId}: expected ${segment.checksum}, got ${calculatedChecksum}`,
            segment.startLSN,
            segmentId
          );
        }
      }

      return segment;
    } catch (error) {
      if (error instanceof WALError) {
        throw error;
      }
      throw new WALError(
        WALErrorCode.SEGMENT_CORRUPTED,
        `Failed to decode segment ${segmentId}`,
        undefined,
        segmentId,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Check if entry matches filter criteria
   */
  function entryMatchesFilter(entry: WALEntry, options: ReadOptions): boolean {
    // LSN range check
    if (options.fromLSN !== undefined && entry.lsn < options.fromLSN) {
      return false;
    }
    if (options.toLSN !== undefined && entry.lsn > options.toLSN) {
      return false;
    }

    // Table filter
    if (options.table !== undefined && entry.table !== options.table) {
      return false;
    }

    // Operation filter
    if (
      options.operations !== undefined &&
      options.operations.length > 0 &&
      !options.operations.includes(entry.op)
    ) {
      return false;
    }

    // Transaction filter
    if (options.txnId !== undefined && entry.txnId !== options.txnId) {
      return false;
    }

    return true;
  }

  /**
   * Find segments that may contain entries in the LSN range
   */
  async function findRelevantSegments(
    options: ReadOptions,
    includeArchived: boolean
  ): Promise<string[]> {
    const allSegments = await reader.listSegments(includeArchived);

    if (allSegments.length === 0) {
      return [];
    }

    // If no LSN range specified, return all segments
    if (options.fromLSN === undefined && options.toLSN === undefined) {
      return allSegments;
    }

    // Filter segments based on LSN range
    const relevant: string[] = [];

    for (const segmentId of allSegments) {
      const startLSN = parseSegmentLSN(segmentId);

      // Quick check: if toLSN is before segment start, skip
      if (options.toLSN !== undefined && options.toLSN < startLSN) {
        continue;
      }

      // We need to load segment to check end LSN for more precise filtering
      // For efficiency, we include segments that might contain data
      relevant.push(segmentId);
    }

    return relevant;
  }

  // Public interface
  const reader: WALReader = {
    async readSegment(segmentId: string): Promise<WALSegment | null> {
      return loadSegment(segmentId);
    },

    async readEntries(options: ReadOptions): Promise<WALEntry[]> {
      const segments = await findRelevantSegments(options, false);
      const entries: WALEntry[] = [];
      let count = 0;
      const limit = options.limit ?? Infinity;

      for (const segmentId of segments) {
        if (count >= limit) break;

        const segment = await loadSegment(segmentId);
        if (!segment) continue;

        // Check if segment is entirely before our range
        if (options.fromLSN !== undefined && segment.endLSN < options.fromLSN) {
          continue;
        }

        // Check if segment is entirely after our range
        if (options.toLSN !== undefined && segment.startLSN > options.toLSN) {
          break; // Segments are ordered, so we can stop
        }

        for (const entry of segment.entries) {
          if (count >= limit) break;

          if (entryMatchesFilter(entry, options)) {
            entries.push(entry);
            count++;
          }
        }
      }

      return entries;
    },

    async listSegments(includeArchived = false): Promise<string[]> {
      const segments: string[] = [];

      // List active segments
      const activePaths = await backend.list(fullConfig.segmentPrefix);
      for (const path of activePaths) {
        const segmentId = path.replace(fullConfig.segmentPrefix, '');
        if (segmentId.startsWith('seg_')) {
          segments.push(segmentId);
        }
      }

      // Optionally include archived segments
      if (includeArchived) {
        const archivePaths = await backend.list(fullConfig.archivePrefix);
        for (const path of archivePaths) {
          const segmentId = path.replace(fullConfig.archivePrefix, '');
          if (segmentId.startsWith('seg_')) {
            segments.push(segmentId);
          }
        }
      }

      // Sort by LSN (lexicographic works because of zero-padding)
      segments.sort();

      return segments;
    },

    async getEntry(lsn: bigint): Promise<WALEntry | null> {
      // Find the segment that should contain this LSN
      const segments = await reader.listSegments(true);

      for (const segmentId of segments) {
        const segment = await loadSegment(segmentId);
        if (!segment) continue;

        // Check if LSN is in this segment
        if (lsn >= segment.startLSN && lsn <= segment.endLSN) {
          // Binary search within segment
          const entry = segment.entries.find((e) => e.lsn === lsn);
          return entry ?? null;
        }

        // If segment start is past our LSN, entry doesn't exist
        if (segment.startLSN > lsn) {
          return null;
        }
      }

      return null;
    },

    async *iterate(options: ReadOptions): AsyncIterableIterator<WALEntry> {
      const segments = await findRelevantSegments(options, false);
      let count = 0;
      const limit = options.limit ?? Infinity;

      for (const segmentId of segments) {
        if (count >= limit) return;

        const segment = await loadSegment(segmentId);
        if (!segment) continue;

        // Check if segment is entirely before our range
        if (options.fromLSN !== undefined && segment.endLSN < options.fromLSN) {
          continue;
        }

        // Check if segment is entirely after our range
        if (options.toLSN !== undefined && segment.startLSN > options.toLSN) {
          return; // Segments are ordered, so we can stop
        }

        for (const entry of segment.entries) {
          if (count >= limit) return;

          if (entryMatchesFilter(entry, options)) {
            yield entry;
            count++;
          }
        }
      }
    },
  };

  return reader;
}

// =============================================================================
// Extended Reader with Live Tailing
// =============================================================================

/**
 * Options for tailing the WAL
 */
export interface TailOptions extends ReadOptions {
  /** Poll interval in milliseconds (default: 100) */
  pollInterval?: number;
  /** Stop after this many entries (default: Infinity) */
  maxEntries?: number;
  /** Timeout in milliseconds (default: Infinity) */
  timeout?: number;
}

/**
 * Create a tailing iterator that follows new WAL entries
 */
export async function* tailWAL(
  reader: WALReader,
  options: TailOptions = {}
): AsyncIterableIterator<WALEntry> {
  const pollInterval = options.pollInterval ?? 100;
  const maxEntries = options.maxEntries ?? Infinity;
  const timeout = options.timeout ?? Infinity;

  let lastLSN = options.fromLSN ?? -1n;
  let count = 0;
  const startTime = Date.now();

  while (count < maxEntries) {
    // Check timeout
    if (Date.now() - startTime > timeout) {
      return;
    }

    // Read new entries
    const entries = await reader.readEntries({
      ...options,
      fromLSN: lastLSN + 1n,
      limit: Math.min(1000, maxEntries - count),
    });

    // Yield new entries
    for (const entry of entries) {
      yield entry;
      lastLSN = entry.lsn;
      count++;

      if (count >= maxEntries) {
        return;
      }
    }

    // If no new entries, wait before polling again
    if (entries.length === 0) {
      await new Promise((resolve) => setTimeout(resolve, pollInterval));
    }
  }
}

// =============================================================================
// Batch Reader for Efficient Replay
// =============================================================================

/**
 * Options for batch reading
 */
export interface BatchReadOptions {
  /** Batch size (number of entries per batch) */
  batchSize?: number;
  /** Process function called for each batch */
  onBatch?: (entries: WALEntry[], batchNumber: number) => Promise<void>;
}

/**
 * Read WAL entries in batches for efficient replay
 */
export async function readWALBatched(
  reader: WALReader,
  readOptions: ReadOptions,
  batchOptions: BatchReadOptions = {}
): Promise<{
  totalEntries: number;
  totalBatches: number;
  lastLSN: bigint | null;
}> {
  const batchSize = batchOptions.batchSize ?? 1000;
  const onBatch = batchOptions.onBatch;

  let totalEntries = 0;
  let totalBatches = 0;
  let lastLSN: bigint | null = null;
  let currentBatch: WALEntry[] = [];

  for await (const entry of reader.iterate(readOptions)) {
    currentBatch.push(entry);
    lastLSN = entry.lsn;

    if (currentBatch.length >= batchSize) {
      totalBatches++;
      if (onBatch) {
        await onBatch(currentBatch, totalBatches);
      }
      totalEntries += currentBatch.length;
      currentBatch = [];
    }
  }

  // Process remaining entries
  if (currentBatch.length > 0) {
    totalBatches++;
    if (onBatch) {
      await onBatch(currentBatch, totalBatches);
    }
    totalEntries += currentBatch.length;
  }

  return {
    totalEntries,
    totalBatches,
    lastLSN,
  };
}

// =============================================================================
// Transaction Reconstruction
// =============================================================================

/**
 * A complete transaction as read from the WAL
 */
export interface ReconstructedTransaction {
  txnId: string;
  status: 'committed' | 'rolledBack' | 'incomplete';
  startLSN: bigint;
  endLSN: bigint;
  entries: WALEntry[];
}

/**
 * Reconstruct complete transactions from WAL entries
 */
export async function reconstructTransactions(
  reader: WALReader,
  options: ReadOptions = {}
): Promise<ReconstructedTransaction[]> {
  const txnMap = new Map<
    string,
    {
      startLSN: bigint;
      entries: WALEntry[];
      status: 'committed' | 'rolledBack' | 'incomplete';
    }
  >();

  for await (const entry of reader.iterate(options)) {
    let txn = txnMap.get(entry.txnId);

    if (!txn) {
      txn = {
        startLSN: entry.lsn,
        entries: [],
        status: 'incomplete',
      };
      txnMap.set(entry.txnId, txn);
    }

    txn.entries.push(entry);

    if (entry.op === 'COMMIT') {
      txn.status = 'committed';
    } else if (entry.op === 'ROLLBACK') {
      txn.status = 'rolledBack';
    }
  }

  // Convert to array and sort by start LSN
  const transactions: ReconstructedTransaction[] = [];

  for (const [txnId, txn] of txnMap) {
    transactions.push({
      txnId,
      status: txn.status,
      startLSN: txn.startLSN,
      endLSN: txn.entries[txn.entries.length - 1].lsn,
      entries: txn.entries,
    });
  }

  transactions.sort((a, b) =>
    a.startLSN < b.startLSN ? -1 : a.startLSN > b.startLSN ? 1 : 0
  );

  return transactions;
}
