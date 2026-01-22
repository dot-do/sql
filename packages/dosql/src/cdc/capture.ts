/**
 * CDC Capture Module for DoSQL
 *
 * Captures changes from the WAL for CDC streaming to lakehouse.
 * Implements:
 * - Real-time change capture from WAL entries
 * - Batching for efficient transfer
 * - Position tracking for resume capability
 * - Backpressure handling
 */

import type { WALEntry, WALReader, WALOperation, LSN } from '../wal/types.js';
import { tailWAL } from '../wal/reader.js';
import type { ChangeEvent, CDCFilter, TransactionEvent } from './types.js';
import { createLSN, incrementLSN } from '../engine/types.js';

// =============================================================================
// Capture Types
// =============================================================================

/**
 * Options for CDC capture
 */
export interface CaptureOptions {
  /** Starting LSN (exclusive, branded type) */
  fromLSN?: LSN;
  /** Filter criteria */
  filter?: CDCFilter;
  /** Poll interval in ms */
  pollInterval?: number;
  /** Maximum batch size */
  maxBatchSize?: number;
  /** Maximum batch age in ms before forced flush */
  maxBatchAge?: number;
  /** Include transaction boundaries */
  includeTransactions?: boolean;
}

/**
 * A batch of captured changes
 */
export interface CaptureBatch {
  /** Batch ID for deduplication */
  batchId: string;
  /** First LSN in batch (branded type) */
  startLSN: LSN;
  /** Last LSN in batch (branded type) */
  endLSN: LSN;
  /** Captured entries */
  entries: WALEntry[];
  /** Batch creation time */
  createdAt: number;
  /** Size in bytes (estimated) */
  sizeBytes: number;
}

/**
 * Capture state for tracking position
 */
export interface CaptureState {
  /** Last captured LSN (branded type) */
  lastLSN: LSN;
  /** Total entries captured */
  totalEntries: number;
  /** Total batches created */
  totalBatches: number;
  /** Current batch entries count */
  currentBatchSize: number;
  /** Whether capture is active */
  active: boolean;
  /** Last capture timestamp */
  lastCaptureAt?: number;
}

/**
 * Capture result from a single capture operation
 */
export interface CaptureResult {
  /** Whether entries were captured */
  hasEntries: boolean;
  /** Number of entries captured */
  entryCount: number;
  /** Last LSN captured (branded type) */
  lastLSN: LSN;
  /** Batch if threshold reached */
  batch?: CaptureBatch;
}

// =============================================================================
// WAL Change Capturer
// =============================================================================

/**
 * Creates a WAL change capturer that reads from WAL and batches changes
 */
export function createWALCapturer(
  reader: WALReader,
  options: CaptureOptions = {}
): WALCapturer {
  const {
    fromLSN = createLSN(0n),
    filter,
    pollInterval = 100,
    maxBatchSize = 1000,
    maxBatchAge = 5000,
    includeTransactions = false,
  } = options;

  let currentLSN: LSN = fromLSN;
  let totalEntries = 0;
  let totalBatches = 0;
  let active = false;
  let lastCaptureAt: number | undefined;
  let batchStartTime = Date.now();
  let pendingEntries: WALEntry[] = [];
  let batchStartLSN: LSN | null = null;

  /**
   * Generate unique batch ID
   */
  function generateBatchId(): string {
    return `batch_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
  }

  /**
   * Estimate entry size in bytes
   */
  function estimateEntrySize(entry: WALEntry): number {
    let size = 100; // Base overhead
    size += entry.txnId.length;
    size += entry.table.length;
    if (entry.key) size += entry.key.length;
    if (entry.before) size += entry.before.length;
    if (entry.after) size += entry.after.length;
    return size;
  }

  /**
   * Check if entry matches filter
   */
  function matchesFilter(entry: WALEntry): boolean {
    if (!filter) return true;

    // Table filter
    if (filter.tables && filter.tables.length > 0) {
      if (!filter.tables.includes(entry.table)) {
        return false;
      }
    }

    // Operation filter
    if (filter.operations && filter.operations.length > 0) {
      if (!filter.operations.includes(entry.op)) {
        return false;
      }
    }

    // Transaction ID filter
    if (filter.txnIds && filter.txnIds.length > 0) {
      if (!filter.txnIds.includes(entry.txnId)) {
        return false;
      }
    }

    // Custom predicate
    if (filter.predicate && !filter.predicate(entry)) {
      return false;
    }

    return true;
  }

  /**
   * Check if we should flush the current batch
   */
  function shouldFlush(): boolean {
    if (pendingEntries.length === 0) return false;
    if (pendingEntries.length >= maxBatchSize) return true;
    if (Date.now() - batchStartTime >= maxBatchAge) return true;
    return false;
  }

  /**
   * Flush pending entries to a batch
   */
  function flushBatch(): CaptureBatch | null {
    if (pendingEntries.length === 0 || batchStartLSN === null) {
      return null;
    }

    const batch: CaptureBatch = {
      batchId: generateBatchId(),
      startLSN: batchStartLSN,
      endLSN: currentLSN,
      entries: [...pendingEntries],
      createdAt: Date.now(),
      sizeBytes: pendingEntries.reduce((sum, e) => sum + estimateEntrySize(e), 0),
    };

    totalBatches++;
    pendingEntries = [];
    batchStartLSN = null;
    batchStartTime = Date.now();

    return batch;
  }

  const capturer: WALCapturer = {
    async capture(): Promise<CaptureResult> {
      active = true;

      // Read new entries from WAL
      const entries = await reader.readEntries({
        fromLSN: incrementLSN(currentLSN),
        limit: maxBatchSize,
      });

      let captured = 0;

      for (const entry of entries) {
        // Skip transaction control if not requested
        if (
          !includeTransactions &&
          (entry.op === 'BEGIN' || entry.op === 'COMMIT' || entry.op === 'ROLLBACK')
        ) {
          currentLSN = entry.lsn;
          continue;
        }

        // Apply filter
        if (!matchesFilter(entry)) {
          currentLSN = entry.lsn;
          continue;
        }

        // Add to pending batch
        if (batchStartLSN === null) {
          batchStartLSN = entry.lsn;
        }
        pendingEntries.push(entry);
        currentLSN = entry.lsn;
        totalEntries++;
        captured++;
        lastCaptureAt = Date.now();
      }

      // Check if we should flush
      const batch = shouldFlush() ? flushBatch() : undefined;

      return {
        hasEntries: captured > 0,
        entryCount: captured,
        lastLSN: currentLSN,
        batch,
      };
    },

    async *captureStream(): AsyncIterableIterator<CaptureBatch> {
      active = true;

      try {
        for await (const entry of tailWAL(reader, {
          fromLSN: currentLSN,
          pollInterval,
        })) {
          // Skip transaction control if not requested
          if (
            !includeTransactions &&
            (entry.op === 'BEGIN' || entry.op === 'COMMIT' || entry.op === 'ROLLBACK')
          ) {
            currentLSN = entry.lsn;
            continue;
          }

          // Apply filter
          if (!matchesFilter(entry)) {
            currentLSN = entry.lsn;
            continue;
          }

          // Add to pending batch
          if (batchStartLSN === null) {
            batchStartLSN = entry.lsn;
          }
          pendingEntries.push(entry);
          currentLSN = entry.lsn;
          totalEntries++;
          lastCaptureAt = Date.now();

          // Yield batch if ready
          if (shouldFlush()) {
            const batch = flushBatch();
            if (batch) {
              yield batch;
            }
          }
        }
      } finally {
        active = false;
      }

      // Yield any remaining entries
      const finalBatch = flushBatch();
      if (finalBatch) {
        yield finalBatch;
      }
    },

    flush(): CaptureBatch | null {
      return flushBatch();
    },

    getState(): CaptureState {
      return {
        lastLSN: currentLSN,
        totalEntries,
        totalBatches,
        currentBatchSize: pendingEntries.length,
        active,
        lastCaptureAt,
      };
    },

    setPosition(lsn: LSN): void {
      currentLSN = lsn;
      // Clear pending entries when position changes
      pendingEntries = [];
      batchStartLSN = null;
      batchStartTime = Date.now();
    },

    stop(): void {
      active = false;
    },
  };

  return capturer;
}

/**
 * WAL Capturer interface
 */
export interface WALCapturer {
  /** Capture available entries (single pass) */
  capture(): Promise<CaptureResult>;
  /** Continuous capture stream yielding batches */
  captureStream(): AsyncIterableIterator<CaptureBatch>;
  /** Force flush current batch */
  flush(): CaptureBatch | null;
  /** Get current capture state */
  getState(): CaptureState;
  /** Set capture position (for resuming) */
  setPosition(lsn: bigint): void;
  /** Stop capturing */
  stop(): void;
}

// =============================================================================
// Change Event Converter
// =============================================================================

/**
 * Convert WAL entry to change event
 */
export function walEntryToChangeEvent<T = unknown>(
  entry: WALEntry,
  decoder?: (data: Uint8Array) => T
): ChangeEvent<T> | TransactionEvent | null {
  const decode = decoder ?? ((d: Uint8Array) => d as unknown as T);

  // Transaction control events
  if (entry.op === 'BEGIN' || entry.op === 'COMMIT' || entry.op === 'ROLLBACK') {
    const txnEvent: TransactionEvent = {
      type: entry.op.toLowerCase() as 'begin' | 'commit' | 'rollback',
      txnId: entry.txnId,
      timestamp: new Date(entry.timestamp),
      lsn: entry.lsn,
    };
    return txnEvent;
  }

  // Data change events
  const typeMap: Record<WALOperation, ChangeEvent['type'] | null> = {
    INSERT: 'insert',
    UPDATE: 'update',
    DELETE: 'delete',
    BEGIN: null,
    COMMIT: null,
    ROLLBACK: null,
  };

  const type = typeMap[entry.op];
  if (!type) return null;

  const changeEvent: ChangeEvent<T> = {
    id: entry.lsn.toString(),
    type,
    table: entry.table,
    txnId: entry.txnId,
    timestamp: new Date(entry.timestamp),
    lsn: entry.lsn,
    key: entry.key,
  };

  if (entry.after) {
    changeEvent.data = decode(entry.after);
  }
  if (entry.before) {
    changeEvent.oldData = decode(entry.before);
  }

  return changeEvent;
}

/**
 * Convert a batch of WAL entries to change events
 */
export function batchToChangeEvents<T = unknown>(
  batch: CaptureBatch,
  decoder?: (data: Uint8Array) => T
): Array<ChangeEvent<T> | TransactionEvent> {
  const events: Array<ChangeEvent<T> | TransactionEvent> = [];

  for (const entry of batch.entries) {
    const event = walEntryToChangeEvent(entry, decoder);
    if (event) {
      events.push(event);
    }
  }

  return events;
}
