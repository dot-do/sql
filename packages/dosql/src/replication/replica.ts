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

import { createLogger } from '../logging/index.js';
import type { FSXBackend } from '../fsx/types.js';

const logger = createLogger({ defaultContext: { module: 'replication-replica' } });
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
  replicaIdsEqual,
  type FencingToken,
  type LeaderElectionState,
  type VoteRequest,
  type VoteResponse,
  type LeaderHeartbeat,
  type SplitBrainDetection,
  type PromotionEligibility,
} from './types.js';
import {
  LeaderElectionStateMachine,
  SplitBrainResolver,
  generateFencingToken,
  validateFencingTokenSignature,
  compareFencingTokens,
} from './leader-election.js';

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
  /** Election check interval handle */
  electionCheckInterval?: ReturnType<typeof setInterval>;
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
  /** Last known primary LSN (for eligibility calculation) */
  lastKnownPrimaryLSN: bigint;
  /** Known replicas for quorum */
  knownReplicas: Map<string, ReplicaInfo>;
  /** Observed leaders (for split-brain detection) */
  observedLeaders: ReplicaId[];
  /** Current fencing token (for split-brain prevention) */
  currentFencingToken: FencingToken | null;
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

  // Leader election state machine (initialized when replica initializes)
  let electionStateMachine: LeaderElectionStateMachine | null = null;
  const splitBrainResolver = new SplitBrainResolver(fullConfig);

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
      lastKnownPrimaryLSN: replicaInfo.lastLSN,
      knownReplicas: new Map(),
      observedLeaders: [],
      currentFencingToken: null,
    };

    // Initialize leader election state machine
    electionStateMachine = new LeaderElectionStateMachine(replicaInfo.id, fullConfig);

    // Persist state
    await persistState();

    // Register with primary
    await registerWithPrimary();
  }

  async function registerWithPrimary(): Promise<void> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');

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
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');
    if (state.streamingActive) return;

    state.streamingActive = true;
    state.info.status = 'syncing';

    // Start streaming loop
    state.streamingInterval = setInterval(async () => {
      try {
        await pullAndApplyWAL();
      } catch (error) {
        logger.error('WAL streaming error', error instanceof Error ? error : new Error(String(error)));
        state!.info.status = 'lagging';
      }
    }, fullConfig.heartbeatIntervalMs);

    // Start election check loop (if auto-failover is enabled)
    if (fullConfig.autoFailover && electionStateMachine) {
      state.electionCheckInterval = setInterval(async () => {
        try {
          await checkAndStartElection();
        } catch (error) {
          logger.error('Election check error', error instanceof Error ? error : new Error(String(error)));
        }
      }, fullConfig.heartbeatIntervalMs);
    }

    // Initial pull
    await pullAndApplyWAL();
  }

  /**
   * Check if we should start an election and initiate if needed
   */
  async function checkAndStartElection(): Promise<void> {
    if (!state || !electionStateMachine) return;
    if (state.info.role === 'primary') return;

    // Check if we should start election
    if (electionStateMachine.shouldStartElection()) {
      logger.info('Primary timeout detected, checking promotion eligibility', {
        replicaId: serializeReplicaId(state.info.id),
      });

      const eligibility = await checkPromotionEligibility();
      if (eligibility.eligible) {
        logger.info('Eligible for promotion, starting election', {
          replicaId: serializeReplicaId(state.info.id),
          priority: eligibility.priority,
        });

        await startElection();
      }
    }
  }

  async function stopStreaming(): Promise<void> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');

    state.streamingActive = false;
    if (state.streamingInterval) {
      clearInterval(state.streamingInterval);
      state.streamingInterval = undefined;
    }
    if (state.electionCheckInterval) {
      clearInterval(state.electionCheckInterval);
      state.electionCheckInterval = undefined;
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
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');

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
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');

    state.snapshotInProgress = {
      info: snapshotInfo,
      receivedChunks: new Set(),
      data: new Map(),
    };

    state.info.status = 'syncing';
  }

  async function applySnapshotChunk(chunk: SnapshotChunk): Promise<void> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');
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
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');

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
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');

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
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');
    return state.info;
  }

  async function getCurrentLSN(): Promise<bigint> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');
    return state.currentLSN;
  }

  async function sendHeartbeat(): Promise<void> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');

    state.info.lastHeartbeat = Date.now();

    // In real implementation, this would send heartbeat to primary
    // and receive acknowledgment with current primary LSN

    await persistState();
  }

  // ==========================================================================
  // FAILOVER
  // ==========================================================================

  async function promoteToPrimary(): Promise<void> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');

    // Stop streaming
    await stopStreaming();

    // Change role
    state.info.role = 'primary';
    state.info.status = 'active';

    await persistState();
  }

  async function demoteToReplica(newPrimaryUrl: string): Promise<void> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');

    // Update primary URL
    state.primaryUrl = newPrimaryUrl;

    // Change role
    state.info.role = 'replica';
    state.info.status = 'syncing';

    // Reset fencing token on demotion
    state.currentFencingToken = null;

    // Reset election state machine
    if (electionStateMachine) {
      // Create a new state machine to reset state
      electionStateMachine = new LeaderElectionStateMachine(state.info.id, fullConfig);
    }

    await persistState();

    // Re-register with new primary
    await registerWithPrimary();

    // Start streaming
    await startStreaming();
  }

  // ==========================================================================
  // AUTO-PROMOTION & LEADER ELECTION
  // ==========================================================================

  /**
   * Check if this replica is eligible for auto-promotion
   */
  async function checkPromotionEligibility(): Promise<PromotionEligibility> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');
    if (!electionStateMachine) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Election state machine not initialized');

    return electionStateMachine.checkPromotionEligibility(state.currentLSN, state.lastKnownPrimaryLSN);
  }

  /**
   * Start leader election process
   */
  async function startElection(): Promise<LeaderElectionState> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');
    if (!electionStateMachine) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Election state machine not initialized');

    // Check if we're already in an election
    const currentRole = electionStateMachine.getRole();
    if (currentRole === 'candidate') {
      throw new ReplicationError(
        ReplicationErrorCode.ELECTION_IN_PROGRESS,
        'Election already in progress',
        state.info.id
      );
    }

    // Check eligibility before starting
    const eligibility = await checkPromotionEligibility();
    if (!eligibility.eligible) {
      throw new ReplicationError(
        ReplicationErrorCode.NOT_ELIGIBLE_FOR_PROMOTION,
        eligibility.reason,
        state.info.id
      );
    }

    // Start the election
    const voteRequest = electionStateMachine.startElection(state.currentLSN);

    logger.info('Started election', {
      replicaId: serializeReplicaId(state.info.id),
      term: voteRequest.term.toString(),
      currentLSN: state.currentLSN.toString(),
    });

    // Update state with new fencing token
    state.currentFencingToken = voteRequest.fencingToken;

    return electionStateMachine.getState();
  }

  /**
   * Handle vote request from another candidate
   */
  async function handleVoteRequest(request: VoteRequest): Promise<VoteResponse> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');
    if (!electionStateMachine) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Election state machine not initialized');

    const response = electionStateMachine.handleVoteRequest(request, state.currentLSN);

    // If we granted the vote, update our observed leaders
    if (response.voteGranted) {
      // Track the candidate as a potential leader
      const candidateKey = serializeReplicaId(request.candidateId);
      if (!state.observedLeaders.some(l => serializeReplicaId(l) === candidateKey)) {
        state.observedLeaders.push(request.candidateId);
      }
    }

    return response;
  }

  /**
   * Handle leader heartbeat
   */
  async function handleLeaderHeartbeat(heartbeat: LeaderHeartbeat): Promise<void> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');
    if (!electionStateMachine) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Election state machine not initialized');

    electionStateMachine.handleLeaderHeartbeat(heartbeat);

    // Update our tracked state
    state.lastKnownPrimaryLSN = heartbeat.currentLSN;
    state.currentFencingToken = heartbeat.fencingToken;
    state.info.lastHeartbeat = Date.now();

    // Update observed leaders for split-brain detection
    const leaderKey = serializeReplicaId(heartbeat.leaderId);
    state.observedLeaders = state.observedLeaders.filter(l => serializeReplicaId(l) !== leaderKey);
    state.observedLeaders.push(heartbeat.leaderId);

    // Keep only recent leaders (last 5 for split-brain detection)
    if (state.observedLeaders.length > 5) {
      state.observedLeaders = state.observedLeaders.slice(-5);
    }

    await persistState();
  }

  /**
   * Get current leader election state
   */
  async function getElectionState(): Promise<LeaderElectionState> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');
    if (!electionStateMachine) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Election state machine not initialized');

    return electionStateMachine.getState();
  }

  /**
   * Validate fencing token
   */
  async function validateFencingToken(token: FencingToken): Promise<boolean> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');
    if (!electionStateMachine) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Election state machine not initialized');

    return electionStateMachine.validateFencingToken(token);
  }

  /**
   * Detect split-brain scenario
   */
  async function detectSplitBrain(): Promise<SplitBrainDetection> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');
    if (!electionStateMachine) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Election state machine not initialized');

    return electionStateMachine.detectSplitBrain(state.observedLeaders);
  }

  /**
   * Auto-promote with split-brain detection
   * This is the main entry point for automatic failover
   */
  async function autoPromote(): Promise<{ success: boolean; fencingToken: FencingToken | null; error?: string }> {
    if (!state) {
      return { success: false, fencingToken: null, error: 'Replica not initialized' };
    }
    if (!electionStateMachine) {
      return { success: false, fencingToken: null, error: 'Election state machine not initialized' };
    }

    // Step 1: Check promotion eligibility
    const eligibility = await checkPromotionEligibility();
    if (!eligibility.eligible) {
      return { success: false, fencingToken: null, error: eligibility.reason };
    }

    logger.info('Auto-promotion started', {
      replicaId: serializeReplicaId(state.info.id),
      priority: eligibility.priority,
      lsnLag: eligibility.lsnLag.toString(),
    });

    // Step 2: Detect potential split-brain before proceeding
    const splitBrainCheck = await detectSplitBrain();
    if (splitBrainCheck.detected) {
      logger.warn('Split-brain detected during auto-promotion', {
        replicaId: serializeReplicaId(state.info.id),
        conflictingLeaders: splitBrainCheck.conflictingLeaders.map(l => serializeReplicaId(l)),
      });

      // Use the split-brain resolver to determine if we should proceed
      const leaders = splitBrainCheck.conflictingLeaders.map(id => ({
        id,
        token: state.currentFencingToken ?? generateFencingToken(0n, id),
        lsn: state.currentLSN,
      }));

      // Add ourselves as a potential leader
      const ourToken = generateFencingToken(electionStateMachine.getState().term + 1n, state.info.id);
      leaders.push({
        id: state.info.id,
        token: ourToken,
        lsn: state.currentLSN,
      });

      const resolution = splitBrainResolver.resolveConflict(leaders);
      if (!resolution) {
        return { success: false, fencingToken: null, error: 'Failed to resolve split-brain conflict' };
      }

      // Only proceed if we are the winner
      if (!replicaIdsEqual(resolution.winner, state.info.id)) {
        return {
          success: false,
          fencingToken: null,
          error: `Lost split-brain resolution to ${serializeReplicaId(resolution.winner)}: ${resolution.reason}`,
        };
      }

      logger.info('Won split-brain resolution', {
        replicaId: serializeReplicaId(state.info.id),
        reason: resolution.reason,
      });
    }

    // Step 3: Start election and attempt to become leader
    try {
      await startElection();
    } catch (error) {
      if (error instanceof ReplicationError && error.code === ReplicationErrorCode.NOT_ELIGIBLE_FOR_PROMOTION) {
        return { success: false, fencingToken: null, error: error.message };
      }
      throw error;
    }

    // Step 4: In a real distributed system, we would wait for votes from other replicas
    // For now, we simulate single-node promotion where we have quorum of 1
    // In production, this would involve RPC calls to other replicas

    const electionState = electionStateMachine.getState();

    // If auto-failover with quorum size 1 (or single replica), we can become leader immediately
    // The self-vote from startElection() should be enough for quorum
    if (fullConfig.quorumSize === 1 || state.knownReplicas.size === 0) {
      // With quorum size 1, the self-vote should give us quorum
      // Simulate receiving our own vote response to trigger leader election
      const selfVoteResponse: VoteResponse = {
        voterId: state.info.id,
        term: electionState.term,
        voteGranted: true,
        reason: 'Self-vote for single-node election',
      };

      // This will call becomeLeader() if we have quorum
      const wonElection = electionStateMachine.handleVoteResponse(selfVoteResponse);

      if (wonElection || electionStateMachine.getRole() === 'leader') {
        const newToken = electionStateMachine.getFencingToken();
        state.currentFencingToken = newToken;

        // Promote to primary
        await promoteToPrimary();

        logger.info('Auto-promotion completed successfully', {
          replicaId: serializeReplicaId(state.info.id),
          fencingToken: newToken?.epoch.toString() ?? 'unknown',
        });

        return { success: true, fencingToken: newToken };
      }

      // Shouldn't reach here with quorum size 1
      return { success: false, fencingToken: null, error: 'Failed to achieve quorum with single node' };
    }

    // For multi-replica setup, we need to collect votes
    // This is a simplified version - in production, you'd make RPC calls to other replicas
    // and wait for responses before deciding if we have quorum

    // Generate vote request for other replicas
    const voteRequest: VoteRequest = {
      candidateId: state.info.id,
      term: electionState.term,
      lastLSN: state.currentLSN,
      fencingToken: electionState.fencingToken!,
    };

    // Simulate self-vote (already done in startElection)
    // In production: broadcast voteRequest to all known replicas and collect responses

    // Check if we won the election (have quorum)
    if (electionStateMachine.getRole() === 'leader') {
      const newToken = electionStateMachine.getFencingToken();
      state.currentFencingToken = newToken;

      // Promote to primary
      await promoteToPrimary();

      logger.info('Auto-promotion completed with quorum', {
        replicaId: serializeReplicaId(state.info.id),
        term: electionState.term.toString(),
      });

      return { success: true, fencingToken: newToken };
    }

    // Election started but not yet won - waiting for votes
    return {
      success: false,
      fencingToken: electionState.fencingToken,
      error: 'Election started, waiting for votes from other replicas',
    };
  }

  /**
   * Register a known replica for quorum calculation
   */
  function registerKnownReplica(info: ReplicaInfo): void {
    if (!state) return;
    state.knownReplicas.set(serializeReplicaId(info.id), info);
    if (electionStateMachine) {
      electionStateMachine.registerReplica(info);
    }
  }

  /**
   * Unregister a known replica
   */
  function unregisterKnownReplica(replicaId: ReplicaId): void {
    if (!state) return;
    state.knownReplicas.delete(serializeReplicaId(replicaId));
    if (electionStateMachine) {
      electionStateMachine.unregisterReplica(replicaId);
    }
  }

  /**
   * Handle vote response from another replica (for election process)
   */
  async function handleVoteResponse(response: VoteResponse): Promise<boolean> {
    if (!state) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Replica not initialized');
    if (!electionStateMachine) throw new ReplicationError(ReplicationErrorCode.NOT_INITIALIZED, 'Election state machine not initialized');

    const wonElection = electionStateMachine.handleVoteResponse(response);

    if (wonElection) {
      const newToken = electionStateMachine.getFencingToken();
      state.currentFencingToken = newToken;

      // Auto-promote to primary on winning election
      await promoteToPrimary();

      logger.info('Won election and promoted to primary', {
        replicaId: serializeReplicaId(state.info.id),
        term: electionStateMachine.getState().term.toString(),
      });
    }

    return wonElection;
  }

  /**
   * Update last known primary LSN (called when receiving heartbeats or WAL batches)
   */
  function updateLastKnownPrimaryLSN(lsn: bigint): void {
    if (!state) return;
    if (lsn > state.lastKnownPrimaryLSN) {
      state.lastKnownPrimaryLSN = lsn;
    }
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
        lastKnownPrimaryLSN: BigInt(persistedState.currentLSN),
        knownReplicas: new Map(),
        observedLeaders: [],
        currentFencingToken: null,
      };

      // Initialize election state machine if we have state
      if (state.info.id) {
        electionStateMachine = new LeaderElectionStateMachine(state.info.id, fullConfig);
      }
    } catch (e) {
      // Ignore corrupted state
    }
  }

  // Load state on creation
  loadState().catch((err: unknown) => logger.error('Failed to load replica state', err instanceof Error ? err : new Error(String(err))));

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
    // Auto-promotion & leader election
    checkPromotionEligibility,
    startElection,
    handleVoteRequest,
    handleLeaderHeartbeat,
    getElectionState,
    validateFencingToken,
    detectSplitBrain,
    autoPromote,
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
