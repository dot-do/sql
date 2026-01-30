/**
 * Primary Durable Object for Multi-Region Replication
 *
 * Handles:
 * - Write operations
 * - Replica registration/deregistration
 * - WAL streaming to replicas
 * - Snapshot generation for catch-up
 * - Failover coordination
 *
 * @packageDocumentation
 */

import type { FSXBackend } from '../fsx/types.js';
import type { WALWriter, WALReader, WALEntry, WALSegment } from '../wal/types.js';
import { crc32 } from '../wal/writer.js';
import {
  type PrimaryDO,
  type ReplicaId,
  type ReplicaInfo,
  type ReplicaStatus,
  type StreamPosition,
  type WALBatch,
  type WALAck,
  type SnapshotInfo,
  type SnapshotChunk,
  type SnapshotRequest,
  type ReplicationHealth,
  type ReplicationLag,
  type FailoverReason,
  type FailoverDecision,
  type FailoverState,
  type ReplicationConfig,
  DEFAULT_REPLICATION_CONFIG,
  ReplicationError,
  ReplicationErrorCode,
  serializeReplicaId,
  replicaIdsEqual,
} from './types.js';

// =============================================================================
// PRIMARY STATE
// =============================================================================

/**
 * Internal state for tracking replicas
 */
interface ReplicaTrackingState {
  info: ReplicaInfo;
  /** Last acknowledged LSN */
  ackedLSN: bigint;
  /** Pending WAL batches awaiting acknowledgment */
  pendingBatches: WALBatch[];
  /** Stream position */
  streamPosition: StreamPosition;
  /** Consecutive failed heartbeats */
  failedHeartbeats: number;
}

/**
 * Snapshot state
 */
interface ActiveSnapshot {
  info: SnapshotInfo;
  chunks: Map<number, SnapshotChunk>;
  expiresAt: number;
}

// =============================================================================
// PRIMARY IMPLEMENTATION
// =============================================================================

/**
 * Create a Primary Durable Object handler
 */
export function createPrimaryDO(
  backend: FSXBackend,
  walWriter: WALWriter,
  walReader: WALReader,
  config: Partial<ReplicationConfig> = {}
): PrimaryDO {
  const fullConfig: ReplicationConfig = { ...DEFAULT_REPLICATION_CONFIG, ...config };

  // State
  const replicas = new Map<string, ReplicaTrackingState>();
  const snapshots = new Map<string, ActiveSnapshot>();
  let failoverState: FailoverState = { status: 'idle' };
  let currentPrimaryId: ReplicaId = { region: 'primary', instanceId: 'main' };

  // Encoder for serialization
  const textEncoder = new TextEncoder();
  const textDecoder = new TextDecoder();

  // ==========================================================================
  // REPLICA MANAGEMENT
  // ==========================================================================

  async function registerReplica(info: Omit<ReplicaInfo, 'registeredAt'>): Promise<void> {
    const key = serializeReplicaId(info.id);

    if (replicas.has(key)) {
      // Update existing replica
      const existing = replicas.get(key)!;
      existing.info = {
        ...info,
        registeredAt: existing.info.registeredAt,
      };
      // Preserve explicitly provided status (e.g. 'offline'), otherwise default to 'syncing'
      existing.info.status = info.status === 'offline' ? 'offline' : 'syncing';
      return;
    }

    // Create new replica tracking state
    const replicaInfo: ReplicaInfo = {
      ...info,
      registeredAt: Date.now(),
      // Preserve explicitly provided status (e.g. 'offline'), otherwise default to 'registering'
      status: info.status === 'offline' ? 'offline' : 'registering',
    };

    const trackingState: ReplicaTrackingState = {
      info: replicaInfo,
      ackedLSN: info.lastLSN,
      pendingBatches: [],
      streamPosition: {
        lsn: info.lastLSN,
        segmentId: '',
        offset: 0,
      },
      failedHeartbeats: 0,
    };

    replicas.set(key, trackingState);

    // Persist replica info
    await persistReplicaState();

    // Update status to syncing (unless explicitly registered as offline)
    if (info.status !== 'offline') {
      trackingState.info.status = 'syncing';
    }
  }

  async function deregisterReplica(replicaId: ReplicaId): Promise<void> {
    const key = serializeReplicaId(replicaId);
    const replica = replicas.get(key);

    if (!replica) {
      throw new ReplicationError(
        ReplicationErrorCode.REPLICA_NOT_FOUND,
        `Replica ${key} not found`,
        replicaId
      );
    }

    // Mark as deregistering
    replica.info.status = 'deregistering';

    // Remove from tracking
    replicas.delete(key);

    // Persist state
    await persistReplicaState();
  }

  // ==========================================================================
  // WAL STREAMING
  // ==========================================================================

  async function getStreamPosition(replicaId: ReplicaId): Promise<StreamPosition> {
    const key = serializeReplicaId(replicaId);
    const replica = replicas.get(key);

    if (!replica) {
      throw new ReplicationError(
        ReplicationErrorCode.REPLICA_NOT_FOUND,
        `Replica ${key} not found`,
        replicaId
      );
    }

    return replica.streamPosition;
  }

  async function pullWAL(
    replicaId: ReplicaId,
    fromLSN: bigint,
    limit: number = fullConfig.walBatchSize
  ): Promise<WALBatch> {
    const key = serializeReplicaId(replicaId);
    const replica = replicas.get(key);

    if (!replica) {
      throw new ReplicationError(
        ReplicationErrorCode.REPLICA_NOT_FOUND,
        `Replica ${key} not found`,
        replicaId
      );
    }

    // Read entries from WAL
    const entries = await walReader.readEntries({
      fromLSN,
      limit,
    });

    if (entries.length === 0) {
      return {
        startLSN: fromLSN,
        endLSN: fromLSN,
        entries: [],
        checksum: 0,
        timestamp: Date.now(),
      };
    }

    const startLSN = entries[0].lsn;
    const endLSN = entries[entries.length - 1].lsn;

    // Calculate checksum
    const entriesJson = JSON.stringify(entries.map(e => ({
      ...e,
      lsn: e.lsn.toString(),
    })));
    const checksum = crc32(textEncoder.encode(entriesJson));

    const batch: WALBatch = {
      startLSN,
      endLSN,
      entries,
      checksum,
      timestamp: Date.now(),
    };

    // Track pending batch
    replica.pendingBatches.push(batch);

    // Update heartbeat
    replica.info.lastHeartbeat = Date.now();
    replica.failedHeartbeats = 0;

    return batch;
  }

  async function acknowledgeWAL(ack: WALAck): Promise<void> {
    const key = serializeReplicaId(ack.replicaId);
    const replica = replicas.get(key);

    if (!replica) {
      throw new ReplicationError(
        ReplicationErrorCode.REPLICA_NOT_FOUND,
        `Replica ${key} not found`,
        ack.replicaId
      );
    }

    // Update acknowledged LSN
    if (ack.appliedLSN > replica.ackedLSN) {
      replica.ackedLSN = ack.appliedLSN;
      replica.info.lastLSN = ack.appliedLSN;
    }

    // Remove acknowledged batches from pending
    replica.pendingBatches = replica.pendingBatches.filter(
      batch => batch.endLSN > ack.appliedLSN
    );

    // Update stream position
    replica.streamPosition.lsn = ack.appliedLSN;

    // Update replica status based on lag
    const primaryLSN = walWriter.getCurrentLSN();
    const lag = primaryLSN - ack.appliedLSN;

    if (lag === 0n) {
      replica.info.status = 'active';
    } else if (lag > BigInt(fullConfig.walBatchSize * 10)) {
      replica.info.status = 'lagging';
    } else if (replica.info.status === 'syncing') {
      replica.info.status = 'active';
    }

    // Update heartbeat
    replica.info.lastHeartbeat = Date.now();
    replica.failedHeartbeats = 0;

    // Persist state periodically
    await persistReplicaState();
  }

  // ==========================================================================
  // SNAPSHOT MANAGEMENT
  // ==========================================================================

  async function requestSnapshot(request: SnapshotRequest): Promise<SnapshotInfo> {
    const snapshotId = `snap_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
    const currentLSN = walWriter.getCurrentLSN();

    // Get all data from backend for snapshot
    const tables: string[] = [];
    const dataChunks: Uint8Array[] = [];

    // Read metadata to get tables
    const schemaData = await backend.read('_meta/schemas');
    if (schemaData) {
      const schemas = JSON.parse(textDecoder.decode(schemaData));
      for (const schema of schemas) {
        tables.push(schema.name);
      }
    }

    // Create snapshot data
    const snapshotData = {
      lsn: currentLSN.toString(),
      tables,
      timestamp: Date.now(),
    };

    // For now, create a simple snapshot with metadata
    // In production, this would iterate through all B-tree data
    const snapshotBytes = textEncoder.encode(JSON.stringify(snapshotData));
    dataChunks.push(snapshotBytes);

    // Calculate total size
    let totalSize = 0;
    for (const chunk of dataChunks) {
      totalSize += chunk.length;
    }

    // Create chunks
    const chunkSize = request.preferredChunkSize ?? fullConfig.snapshotChunkSize;
    const chunks = new Map<number, SnapshotChunk>();

    let chunkIndex = 0;
    let offset = 0;

    for (const data of dataChunks) {
      let dataOffset = 0;
      while (dataOffset < data.length) {
        const chunkData = data.slice(dataOffset, dataOffset + chunkSize);
        const chunk: SnapshotChunk = {
          snapshotId,
          chunkIndex,
          totalChunks: 0, // Will be updated
          data: chunkData,
          checksum: crc32(chunkData),
        };
        chunks.set(chunkIndex, chunk);
        chunkIndex++;
        dataOffset += chunkSize;
      }
    }

    // Update total chunks
    for (const chunk of chunks.values()) {
      chunk.totalChunks = chunks.size;
    }

    // Calculate overall checksum
    let combinedData = new Uint8Array(totalSize);
    let combinedOffset = 0;
    for (const data of dataChunks) {
      combinedData.set(data, combinedOffset);
      combinedOffset += data.length;
    }
    const checksum = crc32(combinedData);

    const snapshotInfo: SnapshotInfo = {
      id: snapshotId,
      lsn: currentLSN,
      createdAt: Date.now(),
      sizeBytes: totalSize,
      chunkCount: chunks.size,
      schemaVersion: 1,
      tables,
      checksum,
    };

    // Store snapshot with expiration
    snapshots.set(snapshotId, {
      info: snapshotInfo,
      chunks,
      expiresAt: Date.now() + 3600000, // 1 hour expiration
    });

    return snapshotInfo;
  }

  async function getSnapshotChunk(snapshotId: string, chunkIndex: number): Promise<SnapshotChunk> {
    const snapshot = snapshots.get(snapshotId);

    if (!snapshot) {
      throw new ReplicationError(
        ReplicationErrorCode.SNAPSHOT_NOT_FOUND,
        `Snapshot ${snapshotId} not found`
      );
    }

    const chunk = snapshot.chunks.get(chunkIndex);

    if (!chunk) {
      throw new ReplicationError(
        ReplicationErrorCode.CHUNK_NOT_FOUND,
        `Chunk ${chunkIndex} not found in snapshot ${snapshotId}`
      );
    }

    return chunk;
  }

  // ==========================================================================
  // HEALTH MONITORING
  // ==========================================================================

  async function getReplicationHealth(): Promise<ReplicationHealth> {
    const primaryLSN = walWriter.getCurrentLSN();
    const now = Date.now();

    const replicasWithLag: Array<{ info: ReplicaInfo; lag: ReplicationLag }> = [];
    let healthyCount = 0;
    let laggingCount = 0;

    for (const [key, replica] of replicas) {
      const lagEntries = primaryLSN - replica.ackedLSN;
      const lagMs = replica.info.lastHeartbeat > 0
        ? now - replica.info.lastHeartbeat
        : 0;

      const lag: ReplicationLag = {
        replicaId: replica.info.id,
        primaryLSN,
        replicaLSN: replica.ackedLSN,
        lagEntries,
        lagMs,
        measuredAt: now,
      };

      // Update status based on heartbeat timeout
      if (now - replica.info.lastHeartbeat > fullConfig.heartbeatTimeoutMs) {
        replica.info.status = 'offline';
      } else if (lagMs > fullConfig.maxLagMs) {
        replica.info.status = 'lagging';
        laggingCount++;
      } else if (replica.info.status === 'active') {
        healthyCount++;
      }

      replicasWithLag.push({ info: replica.info, lag });
    }

    // Determine overall health
    let status: 'healthy' | 'degraded' | 'critical' = 'healthy';
    if (healthyCount < fullConfig.minReplicas) {
      status = 'critical';
    } else if (laggingCount > 0) {
      status = 'degraded';
    }

    return {
      primary: {
        id: currentPrimaryId,
        status: 'active',
        role: 'primary',
        lastLSN: primaryLSN,
        lastHeartbeat: now,
        registeredAt: 0,
        doUrl: '',
      },
      replicas: replicasWithLag,
      status,
      checkedAt: now,
    };
  }

  async function getReplicas(): Promise<ReplicaInfo[]> {
    return Array.from(replicas.values()).map(r => r.info);
  }

  // ==========================================================================
  // FAILOVER
  // ==========================================================================

  async function initiateFailover(
    reason: FailoverReason,
    candidateId?: ReplicaId
  ): Promise<FailoverDecision> {
    if (failoverState.status !== 'idle') {
      return {
        proceed: false,
        reason: `Failover already in progress: ${failoverState.status}`,
      };
    }

    // Find best candidate
    let bestCandidate: ReplicaTrackingState | null = null;
    let minLag = BigInt(Number.MAX_SAFE_INTEGER);

    for (const replica of replicas.values()) {
      if (replica.info.status === 'offline') continue;
      if (candidateId && !replicaIdsEqual(replica.info.id, candidateId)) continue;

      const lag = walWriter.getCurrentLSN() - replica.ackedLSN;
      if (lag < minLag) {
        minLag = lag;
        bestCandidate = replica;
      }
    }

    if (!bestCandidate) {
      return {
        proceed: false,
        reason: 'No suitable candidate found for failover',
      };
    }

    // Check if automatic failover is allowed
    if (!fullConfig.autoFailover && reason !== 'manual_trigger') {
      return {
        proceed: false,
        candidate: bestCandidate.info.id,
        reason: 'Automatic failover is disabled',
        dataLossEstimate: minLag,
      };
    }

    return {
      proceed: true,
      candidate: bestCandidate.info.id,
      reason: `Best candidate found with ${minLag} entries lag`,
      dataLossEstimate: minLag,
    };
  }

  async function executeFailover(decision: FailoverDecision): Promise<FailoverState> {
    if (!decision.proceed || !decision.candidate) {
      return {
        status: 'failed',
        errors: ['Invalid failover decision'],
      };
    }

    failoverState = {
      status: 'initiating',
      oldPrimary: currentPrimaryId,
      newPrimary: decision.candidate,
      reason: 'manual_trigger',
      startedAt: Date.now(),
    };

    try {
      // Mark failover as in progress
      failoverState.status = 'in_progress';

      // Flush any pending WAL
      await walWriter.flush();

      // Wait for candidate to catch up (in real implementation)
      // Here we just simulate the process

      // Update primary ID
      currentPrimaryId = decision.candidate;

      failoverState.status = 'completed';
      failoverState.completedAt = Date.now();

      return failoverState;
    } catch (error) {
      failoverState.status = 'failed';
      failoverState.errors = [error instanceof Error ? error.message : 'Unknown error'];
      return failoverState;
    }
  }

  // ==========================================================================
  // PERSISTENCE
  // ==========================================================================

  async function persistReplicaState(): Promise<void> {
    const state = {
      replicas: Array.from(replicas.entries()).map(([key, tracking]) => ({
        key,
        info: {
          ...tracking.info,
          lastLSN: tracking.info.lastLSN.toString(),
        },
        ackedLSN: tracking.ackedLSN.toString(),
      })),
      primaryId: currentPrimaryId,
    };

    await backend.write(
      '_replication/state.json',
      textEncoder.encode(JSON.stringify(state))
    );
  }

  async function loadReplicaState(): Promise<void> {
    const data = await backend.read('_replication/state.json');
    if (!data) return;

    try {
      const state = JSON.parse(textDecoder.decode(data));

      for (const entry of state.replicas ?? []) {
        const info: ReplicaInfo = {
          ...entry.info,
          lastLSN: BigInt(entry.info.lastLSN),
        };

        replicas.set(entry.key, {
          info,
          ackedLSN: BigInt(entry.ackedLSN),
          pendingBatches: [],
          streamPosition: { lsn: BigInt(entry.ackedLSN), segmentId: '', offset: 0 },
          failedHeartbeats: 0,
        });
      }

      if (state.primaryId) {
        currentPrimaryId = state.primaryId;
      }
    } catch (e) {
      // Ignore corrupted state, start fresh
    }
  }

  // Load state on creation
  loadReplicaState().catch(console.error);

  // Return interface
  return {
    registerReplica,
    deregisterReplica,
    getStreamPosition,
    pullWAL,
    acknowledgeWAL,
    requestSnapshot,
    getSnapshotChunk,
    getReplicationHealth,
    getReplicas,
    initiateFailover,
    executeFailover,
  };
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Configuration for creating a primary DO
 */
export interface CreatePrimaryOptions {
  backend: FSXBackend;
  walWriter: WALWriter;
  walReader: WALReader;
  config?: Partial<ReplicationConfig>;
}

/**
 * Create a primary DO instance
 */
export function createPrimary(options: CreatePrimaryOptions): PrimaryDO {
  return createPrimaryDO(
    options.backend,
    options.walWriter,
    options.walReader,
    options.config
  );
}
