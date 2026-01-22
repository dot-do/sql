/**
 * Replica Durable Object for Multi-Region Replication
 *
 * Handles:
 * - Read operations with consistency levels
 * - WAL streaming from primary
 * - Snapshot-based catch-up
 * - Write forwarding to primary
 * - Failover promotion/demotion
 *
 * @packageDocumentation
 */

import type { FSXBackend } from '../fsx/types.js';
import type { WALEntry, WALWriter } from '../wal/types.js';
import { crc32 } from '../wal/writer.js';
import {
  type ReplicaDO,
  type ReplicaId,
  type ReplicaInfo,
  type ReplicaStatus,
  type WALBatch,
  type WALAck,
  type WALApplyError,
  WALApplyErrorCode,
  type SnapshotInfo,
  type SnapshotChunk,
  type ConsistencyLevel,
  type SessionState,
  type ReplicationConfig,
  DEFAULT_REPLICATION_CONFIG,
  ReplicationError,
  ReplicationErrorCode,
  serializeReplicaId,
} from './types.js';

// =============================================================================
// REPLICA STATE
// =============================================================================

/**
 * Internal state for the replica
 */
interface ReplicaState {
  /** Replica info */
  info: ReplicaInfo;
  /** Primary DO URL for RPC */
  primaryUrl: string;
  /** Current LSN */
  currentLSN: bigint;
  /** Is streaming active */
  streamingActive: boolean;
  /** Streaming interval handle */
  streamingInterval?: ReturnType<typeof setInterval>;
  /** Snapshot in progress */
  snapshotInProgress?: {
    info: SnapshotInfo;
    receivedChunks: Set<number>;
    data: Map<number, Uint8Array>;
  };
  /** Applied entries (for deduplication) */
  appliedLSNs: Set<string>;
  /** Session states for read-your-writes */
  sessions: Map<string, SessionState>;
}

// =============================================================================
// REPLICA IMPLEMENTATION
// =============================================================================

/**
 * Create a Replica Durable Object handler
 */
export function createReplicaDO(
  backend: FSXBackend,
  walWriter: WALWriter,
  config: Partial<ReplicationConfig> = {},
  executeSql?: (sql: string) => Promise<unknown>
): ReplicaDO {
  const fullConfig: ReplicationConfig = { ...DEFAULT_REPLICATION_CONFIG, ...config };

  const textEncoder = new TextEncoder();
  const textDecoder = new TextDecoder();

  // State
  let state: ReplicaState | null = null;

  // ==========================================================================
  // INITIALIZATION
  // ==========================================================================

  async function initialize(
    primaryUrl: string,
    replicaInfo: Omit<ReplicaInfo, 'registeredAt'>
  ): Promise<void> {
    state = {
      info: {
        ...replicaInfo,
        registeredAt: Date.now(),
        status: 'registering',
      },
      primaryUrl,
      currentLSN: replicaInfo.lastLSN,
      streamingActive: false,
      appliedLSNs: new Set(),
      sessions: new Map(),
    };

    // Persist state
    await persistState();

    // Register with primary
    await registerWithPrimary();
  }

  async function registerWithPrimary(): Promise<void> {
    if (!state) throw new Error('Replica not initialized');

    try {
      // In real implementation, this would make an RPC call to primary
      // For now, we just update the status
      state.info.status = 'syncing';
      await persistState();
    } catch (error) {
      throw new ReplicationError(
        ReplicationErrorCode.REGISTRATION_FAILED,
        'Failed to register with primary',
        state.info.id,
        undefined,
        error instanceof Error ? error : undefined
      );
    }
  }

  // ==========================================================================
  // WAL STREAMING
  // ==========================================================================

  async function startStreaming(): Promise<void> {
    if (!state) throw new Error('Replica not initialized');
    if (state.streamingActive) return;

    state.streamingActive = true;
    state.info.status = 'syncing';

    // Start streaming loop
    state.streamingInterval = setInterval(async () => {
      try {
        await pullAndApplyWAL();
      } catch (error) {
        console.error('WAL streaming error:', error);
        state!.info.status = 'lagging';
      }
    }, fullConfig.heartbeatIntervalMs);

    // Initial pull
    await pullAndApplyWAL();
  }

  async function stopStreaming(): Promise<void> {
    if (!state) throw new Error('Replica not initialized');

    state.streamingActive = false;
    if (state.streamingInterval) {
      clearInterval(state.streamingInterval);
      state.streamingInterval = undefined;
    }
  }

  async function pullAndApplyWAL(): Promise<void> {
    if (!state) return;

    // In real implementation, this would call primary.pullWAL()
    // For now, we simulate with a no-op
    state.info.lastHeartbeat = Date.now();

    // Update status if caught up
    const primaryLSN = walWriter.getCurrentLSN();
    if (state.currentLSN >= primaryLSN) {
      state.info.status = 'active';
    }
  }

  async function applyWALBatch(batch: WALBatch): Promise<WALAck> {
    if (!state) throw new Error('Replica not initialized');

    const errors: WALApplyError[] = [];
    let lastAppliedLSN = state.currentLSN;
    const startTime = performance.now();

    // Verify checksum
    const entriesJson = JSON.stringify(batch.entries.map(e => ({
      ...e,
      lsn: e.lsn.toString(),
    })));
    const expectedChecksum = crc32(textEncoder.encode(entriesJson));

    if (batch.checksum !== expectedChecksum) {
      errors.push({
        lsn: batch.startLSN,
        code: WALApplyErrorCode.CHECKSUM_MISMATCH,
        message: 'Batch checksum mismatch',
        retryable: true,
      });

      return {
        replicaId: state.info.id,
        appliedLSN: lastAppliedLSN,
        processingTimeMs: performance.now() - startTime,
        errors,
      };
    }

    // Apply entries in order
    for (const entry of batch.entries) {
      // Check for duplicates
      const lsnKey = entry.lsn.toString();
      if (state.appliedLSNs.has(lsnKey)) {
        errors.push({
          lsn: entry.lsn,
          code: WALApplyErrorCode.DUPLICATE,
          message: 'Entry already applied',
          retryable: false,
        });
        continue;
      }

      // Check for ordering
      if (entry.lsn !== lastAppliedLSN + 1n && entry.lsn !== lastAppliedLSN) {
        errors.push({
          lsn: entry.lsn,
          code: WALApplyErrorCode.MISSING_PREREQUISITE,
          message: `Expected LSN ${lastAppliedLSN + 1n}, got ${entry.lsn}`,
          retryable: true,
        });
        break; // Stop processing on gap
      }

      // Apply the entry
      try {
        await applyEntry(entry);
        lastAppliedLSN = entry.lsn;
        state.appliedLSNs.add(lsnKey);

        // Keep applied LSN set bounded
        if (state.appliedLSNs.size > 10000) {
          const oldestLSN = entry.lsn - 10000n;
          for (const key of state.appliedLSNs) {
            if (BigInt(key) < oldestLSN) {
              state.appliedLSNs.delete(key);
            }
          }
        }
      } catch (error) {
        errors.push({
          lsn: entry.lsn,
          code: WALApplyErrorCode.STORAGE_ERROR,
          message: error instanceof Error ? error.message : 'Unknown error',
          retryable: true,
        });
        break;
      }
    }

    // Update state
    state.currentLSN = lastAppliedLSN;
    state.info.lastLSN = lastAppliedLSN;
    state.info.lastHeartbeat = Date.now();

    // Update status
    if (errors.length === 0) {
      state.info.status = 'active';
    } else if (errors.some(e => !e.retryable)) {
      state.info.status = 'lagging';
    }

    await persistState();

    return {
      replicaId: state.info.id,
      appliedLSN: lastAppliedLSN,
      processingTimeMs: performance.now() - startTime,
      errors: errors.length > 0 ? errors : undefined,
    };
  }

  async function applyEntry(entry: WALEntry): Promise<void> {
    // In real implementation, this would apply the entry to local storage
    // For now, we write to WAL for durability
    await walWriter.append({
      timestamp: entry.timestamp,
      txnId: entry.txnId,
      op: entry.op,
      table: entry.table,
      key: entry.key,
      before: entry.before,
      after: entry.after,
    });
  }

  // ==========================================================================
  // SNAPSHOT CATCH-UP
  // ==========================================================================

  async function catchUpFromSnapshot(snapshotInfo: SnapshotInfo): Promise<void> {
    if (!state) throw new Error('Replica not initialized');

    state.snapshotInProgress = {
      info: snapshotInfo,
      receivedChunks: new Set(),
      data: new Map(),
    };

    state.info.status = 'syncing';
  }

  async function applySnapshotChunk(chunk: SnapshotChunk): Promise<void> {
    if (!state) throw new Error('Replica not initialized');
    if (!state.snapshotInProgress) {
      throw new ReplicationError(
        ReplicationErrorCode.SNAPSHOT_NOT_FOUND,
        'No snapshot in progress'
      );
    }

    if (chunk.snapshotId !== state.snapshotInProgress.info.id) {
      throw new ReplicationError(
        ReplicationErrorCode.SNAPSHOT_NOT_FOUND,
        'Chunk does not match current snapshot'
      );
    }

    // Verify chunk checksum
    const expectedChecksum = crc32(chunk.data);
    if (chunk.checksum !== expectedChecksum) {
      throw new ReplicationError(
        ReplicationErrorCode.STREAMING_ERROR,
        'Chunk checksum mismatch'
      );
    }

    // Store chunk
    state.snapshotInProgress.receivedChunks.add(chunk.chunkIndex);
    state.snapshotInProgress.data.set(chunk.chunkIndex, chunk.data);

    // Check if all chunks received
    if (state.snapshotInProgress.receivedChunks.size === chunk.totalChunks) {
      await finalizeSnapshot();
    }
  }

  async function finalizeSnapshot(): Promise<void> {
    if (!state || !state.snapshotInProgress) return;

    // Combine all chunks
    let totalSize = 0;
    for (const chunk of state.snapshotInProgress.data.values()) {
      totalSize += chunk.length;
    }

    const combinedData = new Uint8Array(totalSize);
    let offset = 0;
    for (let i = 0; i < state.snapshotInProgress.data.size; i++) {
      const chunk = state.snapshotInProgress.data.get(i);
      if (chunk) {
        combinedData.set(chunk, offset);
        offset += chunk.length;
      }
    }

    // Verify overall checksum
    const checksum = crc32(combinedData);
    if (checksum !== state.snapshotInProgress.info.checksum) {
      throw new ReplicationError(
        ReplicationErrorCode.STREAMING_ERROR,
        'Snapshot checksum mismatch'
      );
    }

    // Apply snapshot data
    // In real implementation, this would restore all data
    const snapshotData = JSON.parse(textDecoder.decode(combinedData));

    // Update LSN to snapshot LSN
    state.currentLSN = state.snapshotInProgress.info.lsn;
    state.info.lastLSN = state.snapshotInProgress.info.lsn;

    // Clear snapshot state
    state.snapshotInProgress = undefined;

    // Ready to start streaming
    state.info.status = 'syncing';
    await persistState();
  }

  // ==========================================================================
  // QUERY HANDLING
  // ==========================================================================

  async function handleQuery(
    sql: string,
    consistency: ConsistencyLevel,
    session?: SessionState
  ): Promise<unknown> {
    if (!state) throw new Error('Replica not initialized');

    // Check consistency requirements
    if (consistency === 'strong') {
      // Must forward to primary
      return forwardWrite(sql);
    }

    if (consistency === 'session' && session) {
      // Check if we've caught up to session's last write
      if (state.currentLSN < session.lastWriteLSN) {
        // Wait for catch-up or forward to primary
        const waitMs = 100;
        const maxWait = fullConfig.boundedStalenessMs;
        let waited = 0;

        while (state.currentLSN < session.lastWriteLSN && waited < maxWait) {
          await new Promise(resolve => setTimeout(resolve, waitMs));
          waited += waitMs;
        }

        if (state.currentLSN < session.lastWriteLSN) {
          // Still behind, forward to primary
          return forwardWrite(sql);
        }
      }
    }

    if (consistency === 'bounded') {
      // Check if lag is within bounds
      const lag = Date.now() - state.info.lastHeartbeat;
      if (lag > fullConfig.boundedStalenessMs) {
        throw new ReplicationError(
          ReplicationErrorCode.CONSISTENCY_VIOLATION,
          `Replica lag ${lag}ms exceeds bounded staleness ${fullConfig.boundedStalenessMs}ms`,
          state.info.id
        );
      }
    }

    // Execute locally
    if (executeSql) {
      return executeSql(sql);
    }

    // Default: return empty result
    return { rows: [], rowCount: 0 };
  }

  async function forwardWrite(sql: string): Promise<unknown> {
    if (!state) throw new Error('Replica not initialized');

    // In real implementation, this would forward to primary via RPC
    // For now, we throw an error indicating write forwarding is needed
    throw new ReplicationError(
      ReplicationErrorCode.PRIMARY_UNAVAILABLE,
      'Write forwarding not implemented - contact primary directly',
      state.info.id
    );
  }

  // ==========================================================================
  // STATUS & HEARTBEAT
  // ==========================================================================

  async function getStatus(): Promise<ReplicaInfo> {
    if (!state) throw new Error('Replica not initialized');
    return state.info;
  }

  async function getCurrentLSN(): Promise<bigint> {
    if (!state) throw new Error('Replica not initialized');
    return state.currentLSN;
  }

  async function sendHeartbeat(): Promise<void> {
    if (!state) throw new Error('Replica not initialized');

    state.info.lastHeartbeat = Date.now();

    // In real implementation, this would send heartbeat to primary
    // and receive acknowledgment with current primary LSN

    await persistState();
  }

  // ==========================================================================
  // FAILOVER
  // ==========================================================================

  async function promoteToPrimary(): Promise<void> {
    if (!state) throw new Error('Replica not initialized');

    // Stop streaming
    await stopStreaming();

    // Change role
    state.info.role = 'primary';
    state.info.status = 'active';

    await persistState();
  }

  async function demoteToReplica(newPrimaryUrl: string): Promise<void> {
    if (!state) throw new Error('Replica not initialized');

    // Update primary URL
    state.primaryUrl = newPrimaryUrl;

    // Change role
    state.info.role = 'replica';
    state.info.status = 'syncing';

    await persistState();

    // Re-register with new primary
    await registerWithPrimary();

    // Start streaming
    await startStreaming();
  }

  // ==========================================================================
  // PERSISTENCE
  // ==========================================================================

  async function persistState(): Promise<void> {
    if (!state) return;

    const persistedState = {
      info: {
        ...state.info,
        lastLSN: state.info.lastLSN.toString(),
      },
      primaryUrl: state.primaryUrl,
      currentLSN: state.currentLSN.toString(),
      streamingActive: state.streamingActive,
    };

    await backend.write(
      '_replica/state.json',
      textEncoder.encode(JSON.stringify(persistedState))
    );
  }

  async function loadState(): Promise<void> {
    const data = await backend.read('_replica/state.json');
    if (!data) return;

    try {
      const persistedState = JSON.parse(textDecoder.decode(data));

      state = {
        info: {
          ...persistedState.info,
          lastLSN: BigInt(persistedState.info.lastLSN),
        },
        primaryUrl: persistedState.primaryUrl,
        currentLSN: BigInt(persistedState.currentLSN),
        streamingActive: false, // Don't auto-start streaming
        appliedLSNs: new Set(),
        sessions: new Map(),
      };
    } catch (e) {
      // Ignore corrupted state
    }
  }

  // Load state on creation
  loadState().catch(console.error);

  // Return interface
  return {
    initialize,
    startStreaming,
    stopStreaming,
    applyWALBatch,
    catchUpFromSnapshot,
    applySnapshotChunk,
    getStatus,
    getCurrentLSN,
    sendHeartbeat,
    handleQuery,
    forwardWrite,
    promoteToPrimary,
    demoteToReplica,
  };
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Configuration for creating a replica DO
 */
export interface CreateReplicaOptions {
  backend: FSXBackend;
  walWriter: WALWriter;
  config?: Partial<ReplicationConfig>;
  executeSql?: (sql: string) => Promise<unknown>;
}

/**
 * Create a replica DO instance
 */
export function createReplica(options: CreateReplicaOptions): ReplicaDO {
  return createReplicaDO(
    options.backend,
    options.walWriter,
    options.config,
    options.executeSql
  );
}
