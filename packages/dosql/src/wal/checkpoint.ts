/**
 * WAL Checkpoint Manager for DoSQL
 *
 * Handles checkpointing and crash recovery:
 * - Persist checkpoint LSN to durable storage
 * - Archive old WAL segments after checkpoint
 * - Replay from last checkpoint on recovery
 */

import type { FSXBackend } from '../fsx/types.js';
import {
  type WALEntry,
  type WALSegment,
  type WALConfig,
  type WALReader,
  type Checkpoint,
  type CheckpointManager,
  type RecoveryState,
  DEFAULT_WAL_CONFIG,
  WALError,
  WALErrorCode,
} from './types.js';
import { createWALReader } from './reader.js';

// =============================================================================
// Checkpoint Encoder
// =============================================================================

/**
 * Encode checkpoint to JSON bytes
 */
function encodeCheckpoint(checkpoint: Checkpoint): Uint8Array {
  const obj = {
    lsn: checkpoint.lsn.toString(),
    timestamp: checkpoint.timestamp,
    segmentId: checkpoint.segmentId,
    activeTransactions: checkpoint.activeTransactions,
    schemaVersion: checkpoint.schemaVersion,
  };
  return new TextEncoder().encode(JSON.stringify(obj));
}

/**
 * Decode checkpoint from JSON bytes
 */
function decodeCheckpoint(data: Uint8Array): Checkpoint {
  const json = new TextDecoder().decode(data);
  const obj = JSON.parse(json);
  return {
    lsn: BigInt(obj.lsn),
    timestamp: obj.timestamp,
    segmentId: obj.segmentId,
    activeTransactions: obj.activeTransactions,
    schemaVersion: obj.schemaVersion,
  };
}

// =============================================================================
// Checkpoint Manager Implementation
// =============================================================================

/**
 * Creates a checkpoint manager
 */
export function createCheckpointManager(
  backend: FSXBackend,
  reader: WALReader,
  config: Partial<WALConfig> = {}
): CheckpointManager {
  const fullConfig: WALConfig = { ...DEFAULT_WAL_CONFIG, ...config };

  /**
   * Find segment containing an LSN
   */
  async function findSegmentForLSN(lsn: bigint): Promise<string | null> {
    const segments = await reader.listSegments(false);

    // First pass: exact match (LSN is within segment range)
    for (const segmentId of segments) {
      const segment = await reader.readSegment(segmentId);
      if (segment && segment.startLSN <= lsn && segment.endLSN >= lsn) {
        return segmentId;
      }
    }

    // Second pass: find the closest segment whose endLSN is <= lsn
    // This handles the common case where getCurrentLSN() returns the NEXT LSN
    // (one past the last written entry), which won't be inside any segment.
    let bestSegmentId: string | null = null;
    let bestEndLSN: bigint = -1n;
    for (const segmentId of segments) {
      const segment = await reader.readSegment(segmentId);
      if (segment && segment.endLSN <= lsn && segment.endLSN > bestEndLSN) {
        bestEndLSN = segment.endLSN;
        bestSegmentId = segmentId;
      }
    }

    return bestSegmentId;
  }

  /**
   * Parse segment ID to get start LSN
   */
  function parseSegmentLSN(segmentId: string): bigint {
    const match = segmentId.match(/seg_(\d+)/);
    return match ? BigInt(match[1]) : 0n;
  }

  // Public interface
  const manager: CheckpointManager = {
    async getCheckpoint(): Promise<Checkpoint | null> {
      const data = await backend.read(fullConfig.checkpointPath);
      if (!data) {
        return null;
      }

      try {
        return decodeCheckpoint(data);
      } catch (error) {
        throw new WALError(
          WALErrorCode.CHECKPOINT_FAILED,
          'Failed to decode checkpoint',
          undefined,
          undefined,
          error instanceof Error ? error : undefined
        );
      }
    },

    async createCheckpoint(
      lsn: bigint,
      activeTransactions: string[]
    ): Promise<Checkpoint> {
      // Find segment containing this LSN
      const segmentId = await findSegmentForLSN(lsn);

      if (!segmentId) {
        throw new WALError(
          WALErrorCode.CHECKPOINT_FAILED,
          `No segment found containing LSN ${lsn}`,
          lsn
        );
      }

      const checkpoint: Checkpoint = {
        lsn,
        timestamp: Date.now(),
        segmentId,
        activeTransactions,
      };

      // Write checkpoint to storage
      const data = encodeCheckpoint(checkpoint);
      try {
        await backend.write(fullConfig.checkpointPath, data);
      } catch (error) {
        throw new WALError(
          WALErrorCode.CHECKPOINT_FAILED,
          'Failed to write checkpoint',
          lsn,
          undefined,
          error instanceof Error ? error : undefined
        );
      }

      return checkpoint;
    },

    async archiveOldSegments(retainCount = 10): Promise<number> {
      const checkpoint = await manager.getCheckpoint();
      if (!checkpoint) {
        return 0;
      }

      const segments = await reader.listSegments(false);
      const checkpointLSN = checkpoint.lsn;
      let archivedCount = 0;

      // Find segments that are entirely before the checkpoint
      const toArchive: string[] = [];

      for (const segmentId of segments) {
        const segment = await reader.readSegment(segmentId);
        if (segment && segment.endLSN < checkpointLSN) {
          toArchive.push(segmentId);
        }
      }

      // Archive segments
      for (const segmentId of toArchive) {
        const sourcePath = `${fullConfig.segmentPrefix}${segmentId}`;
        const archivePath = `${fullConfig.archivePrefix}${segmentId}`;

        try {
          // Read segment data
          const data = await backend.read(sourcePath);
          if (data) {
            // Write to archive
            await backend.write(archivePath, data);
            // Delete from active
            await backend.delete(sourcePath);
            archivedCount++;
          }
        } catch (error) {
          console.error(
            `Failed to archive segment ${segmentId}:`,
            error
          );
        }
      }

      // Clean up old archives (keep only retainCount most recent)
      if (fullConfig.autoArchive && retainCount > 0) {
        const archivedSegments = await backend.list(fullConfig.archivePrefix);
        const sortedArchives = archivedSegments
          .map((path) => path.replace(fullConfig.archivePrefix, ''))
          .filter((id) => id.startsWith('seg_'))
          .sort();

        if (sortedArchives.length > retainCount) {
          const toDelete = sortedArchives.slice(
            0,
            sortedArchives.length - retainCount
          );
          for (const segmentId of toDelete) {
            try {
              await backend.delete(
                `${fullConfig.archivePrefix}${segmentId}`
              );
            } catch (error) {
              console.error(
                `Failed to delete archived segment ${segmentId}:`,
                error
              );
            }
          }
        }
      }

      return archivedCount;
    },

    async recover(
      applyFn: (entry: WALEntry) => Promise<void>
    ): Promise<RecoveryState> {
      const state: RecoveryState = {
        lastLSN: 0n,
        entriesReplayed: 0,
        rolledBackTransactions: [],
        startedAt: Date.now(),
        errors: [],
      };

      // Get last checkpoint
      const checkpoint = await manager.getCheckpoint();

      // Track transaction state during replay
      const activeTransactions = new Set<string>(
        checkpoint?.activeTransactions ?? []
      );
      const committedTransactions = new Set<string>();
      const rolledBackTransactions = new Set<string>();

      // Entries buffered per transaction (for rollback)
      const txnEntries = new Map<string, WALEntry[]>();

      // Determine starting LSN for replay
      const startLSN = checkpoint?.lsn ?? 0n;
      state.lastLSN = startLSN;

      // First pass: identify committed and rolled back transactions
      const allEntries: WALEntry[] = [];

      for await (const entry of reader.iterate({
        fromLSN: startLSN > 0n ? startLSN + 1n : 0n,
      })) {
        allEntries.push(entry);

        if (entry.op === 'BEGIN') {
          activeTransactions.add(entry.txnId);
        } else if (entry.op === 'COMMIT') {
          activeTransactions.delete(entry.txnId);
          committedTransactions.add(entry.txnId);
        } else if (entry.op === 'ROLLBACK') {
          activeTransactions.delete(entry.txnId);
          rolledBackTransactions.add(entry.txnId);
        }
      }

      // Transactions that were active at checkpoint and never completed are rolled back
      for (const txnId of activeTransactions) {
        rolledBackTransactions.add(txnId);
      }

      state.rolledBackTransactions = Array.from(rolledBackTransactions);

      // Second pass: apply only committed transaction entries
      for (const entry of allEntries) {
        state.lastLSN = entry.lsn;

        // Skip transaction control entries
        if (
          entry.op === 'BEGIN' ||
          entry.op === 'COMMIT' ||
          entry.op === 'ROLLBACK'
        ) {
          continue;
        }

        // Skip rolled back transactions
        if (rolledBackTransactions.has(entry.txnId)) {
          continue;
        }

        // Buffer entries for transactions not yet known to be committed
        if (!committedTransactions.has(entry.txnId)) {
          let entries = txnEntries.get(entry.txnId);
          if (!entries) {
            entries = [];
            txnEntries.set(entry.txnId, entries);
          }
          entries.push(entry);
          continue;
        }

        // Apply committed entry
        try {
          await applyFn(entry);
          state.entriesReplayed++;
        } catch (error) {
          state.errors.push({
            lsn: entry.lsn,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }

      // Apply buffered entries for transactions that were committed
      for (const [txnId, entries] of txnEntries) {
        if (committedTransactions.has(txnId)) {
          for (const entry of entries) {
            try {
              await applyFn(entry);
              state.entriesReplayed++;
            } catch (error) {
              state.errors.push({
                lsn: entry.lsn,
                error: error instanceof Error ? error.message : String(error),
              });
            }
          }
        }
      }

      state.completedAt = Date.now();

      // Throw if there were critical errors
      if (state.errors.length > 0) {
        console.warn(
          `Recovery completed with ${state.errors.length} errors`
        );
      }

      return state;
    },
  };

  return manager;
}

// =============================================================================
// Recovery Helpers
// =============================================================================

/**
 * Options for full recovery
 */
export interface FullRecoveryOptions {
  /** Backend for storage */
  backend: FSXBackend;
  /** WAL configuration */
  config?: Partial<WALConfig>;
  /** Function to apply each entry */
  applyFn: (entry: WALEntry) => Promise<void>;
  /** Whether to archive old segments after recovery */
  archiveAfterRecovery?: boolean;
  /** Number of archived segments to retain */
  archiveRetainCount?: number;
}

/**
 * Perform full recovery from WAL
 */
export async function performRecovery(
  options: FullRecoveryOptions
): Promise<RecoveryState> {
  const { backend, config, applyFn } = options;
  const archiveAfterRecovery = options.archiveAfterRecovery ?? true;
  const archiveRetainCount = options.archiveRetainCount ?? 10;

  // Create reader and checkpoint manager
  const reader = createWALReader(backend, config);
  const checkpointMgr = createCheckpointManager(backend, reader, config);

  // Perform recovery
  const state = await checkpointMgr.recover(applyFn);

  // Archive old segments if requested
  if (archiveAfterRecovery && state.entriesReplayed > 0) {
    await checkpointMgr.archiveOldSegments(archiveRetainCount);
  }

  return state;
}

/**
 * Check if recovery is needed (checkpoint exists but there are entries after it)
 */
export async function needsRecovery(
  backend: FSXBackend,
  config: Partial<WALConfig> = {}
): Promise<{ needsRecovery: boolean; checkpointLSN: bigint | null }> {
  const reader = createWALReader(backend, config);
  const checkpointMgr = createCheckpointManager(backend, reader, config);

  const checkpoint = await checkpointMgr.getCheckpoint();

  if (!checkpoint) {
    // No checkpoint means we need to replay everything
    const segments = await reader.listSegments(false);
    return {
      needsRecovery: segments.length > 0,
      checkpointLSN: null,
    };
  }

  // Check if there are entries after the checkpoint
  const entries = await reader.readEntries({
    fromLSN: checkpoint.lsn + 1n,
    limit: 1,
  });

  return {
    needsRecovery: entries.length > 0,
    checkpointLSN: checkpoint.lsn,
  };
}

// =============================================================================
// Automatic Checkpointing
// =============================================================================

/**
 * Options for automatic checkpointing
 */
export interface AutoCheckpointOptions {
  /** Checkpoint every N entries (default: 10000) */
  entryInterval?: number;
  /** Checkpoint every N milliseconds (default: 60000 = 1 minute) */
  timeInterval?: number;
  /** Get currently active transaction IDs */
  getActiveTransactions: () => string[];
  /** Called after each checkpoint */
  onCheckpoint?: (checkpoint: Checkpoint) => void;
}

/**
 * Create an automatic checkpointing controller
 */
export function createAutoCheckpointer(
  checkpointMgr: CheckpointManager,
  getCurrentLSN: () => bigint,
  options: AutoCheckpointOptions
) {
  const entryInterval = options.entryInterval ?? 10000;
  const timeInterval = options.timeInterval ?? 60000;
  const { getActiveTransactions, onCheckpoint } = options;

  let lastCheckpointLSN = -1n;
  let lastCheckpointTime = Date.now();
  let entriesSinceCheckpoint = 0;
  let timer: ReturnType<typeof setInterval> | null = null;

  /**
   * Check if checkpoint is needed and create one if so
   */
  async function maybeCheckpoint(): Promise<Checkpoint | null> {
    const currentLSN = getCurrentLSN();
    const now = Date.now();

    const entriesExceeded = entriesSinceCheckpoint >= entryInterval;
    const timeExceeded = now - lastCheckpointTime >= timeInterval;
    const hasNewEntries = currentLSN > lastCheckpointLSN;

    if ((entriesExceeded || timeExceeded) && hasNewEntries) {
      try {
        const checkpoint = await checkpointMgr.createCheckpoint(
          currentLSN,
          getActiveTransactions()
        );

        lastCheckpointLSN = currentLSN;
        lastCheckpointTime = now;
        entriesSinceCheckpoint = 0;

        if (onCheckpoint) {
          onCheckpoint(checkpoint);
        }

        return checkpoint;
      } catch (error) {
        console.error('Auto-checkpoint failed:', error);
      }
    }

    return null;
  }

  return {
    /**
     * Notify that an entry was written
     */
    onEntryWritten(): void {
      entriesSinceCheckpoint++;
    },

    /**
     * Force a checkpoint now
     */
    async forceCheckpoint(): Promise<Checkpoint | null> {
      const currentLSN = getCurrentLSN();
      if (currentLSN <= lastCheckpointLSN) {
        return null;
      }

      const checkpoint = await checkpointMgr.createCheckpoint(
        currentLSN,
        getActiveTransactions()
      );

      lastCheckpointLSN = currentLSN;
      lastCheckpointTime = Date.now();
      entriesSinceCheckpoint = 0;

      if (onCheckpoint) {
        onCheckpoint(checkpoint);
      }

      return checkpoint;
    },

    /**
     * Start periodic checkpoint timer
     */
    start(): void {
      if (timer) return;
      timer = setInterval(() => {
        maybeCheckpoint().catch(console.error);
      }, timeInterval);
    },

    /**
     * Stop periodic checkpoint timer
     */
    stop(): void {
      if (timer) {
        clearInterval(timer);
        timer = null;
      }
    },

    /**
     * Check if checkpoint is needed (call after writes)
     */
    maybeCheckpoint,
  };
}
