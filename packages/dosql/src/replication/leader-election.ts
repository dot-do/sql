/**
 * Leader Election and Split-Brain Detection
 *
 * Implements:
 * - Raft-inspired leader election
 * - Fencing tokens for split-brain prevention
 * - Quorum-based consensus
 * - Auto-promotion when primary fails
 *
 * @packageDocumentation
 */

import { createLogger } from '../logging/index.js';
import {
  type ReplicaId,
  type ReplicaInfo,
  type FencingToken,
  type LeaderElectionState,
  type VoteRequest,
  type VoteResponse,
  type LeaderHeartbeat,
  type SplitBrainDetection,
  type PromotionEligibility,
  type ReplicationConfig,
  DEFAULT_REPLICATION_CONFIG,
  ReplicationError,
  ReplicationErrorCode,
  serializeReplicaId,
  replicaIdsEqual,
} from './types.js';

const logger = createLogger({ defaultContext: { module: 'leader-election' } });

// =============================================================================
// FENCING TOKEN UTILITIES
// =============================================================================

/**
 * Generate a new fencing token with cryptographically secure signature
 */
export async function generateFencingToken(
  epoch: bigint,
  generatedBy: ReplicaId
): Promise<FencingToken> {
  const timestamp = Date.now();
  const data = `${epoch}:${timestamp}:${serializeReplicaId(generatedBy)}`;

  // Use SHA-256 for cryptographically secure signing
  const signature = await sha256Hash(data);

  return {
    epoch,
    generatedAt: timestamp,
    generatedBy,
    signature,
  };
}

/**
 * Validate a fencing token signature using SHA-256
 */
export async function validateFencingTokenSignature(token: FencingToken): Promise<boolean> {
  const data = `${token.epoch}:${token.generatedAt}:${serializeReplicaId(token.generatedBy)}`;
  const expectedSignature = await sha256Hash(data);
  return token.signature === expectedSignature;
}

/**
 * Compare two fencing tokens - returns positive if a > b, negative if a < b, 0 if equal
 */
export function compareFencingTokens(a: FencingToken | null, b: FencingToken | null): number {
  if (!a && !b) return 0;
  if (!a) return -1;
  if (!b) return 1;

  // Compare epochs first
  if (a.epoch !== b.epoch) {
    return a.epoch > b.epoch ? 1 : -1;
  }

  // If epochs are equal, compare timestamps
  return a.generatedAt - b.generatedAt;
}

/**
 * Check if a fencing token is expired
 */
export function isFencingTokenExpired(token: FencingToken, ttlMs: number): boolean {
  return Date.now() - token.generatedAt > ttlMs;
}

/**
 * Compute SHA-256 hash of data string
 * Uses Web Crypto API for cryptographically secure hashing
 */
async function sha256Hash(data: string): Promise<string> {
  const encoder = new TextEncoder();
  const dataBuffer = encoder.encode(data);
  const hashBuffer = await crypto.subtle.digest('SHA-256', dataBuffer);
  const hashArray = new Uint8Array(hashBuffer);
  return Array.from(hashArray)
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}

// =============================================================================
// LEADER ELECTION STATE MACHINE
// =============================================================================

export type ElectionRole = 'follower' | 'candidate' | 'leader';

/**
 * Leader Election State Machine
 */
export class LeaderElectionStateMachine {
  private readonly config: ReplicationConfig;
  private readonly selfId: ReplicaId;

  private role: ElectionRole = 'follower';
  private currentTerm: bigint = 0n;
  private votedFor: ReplicaId | null = null;
  private currentLeader: ReplicaId | null = null;
  private fencingToken: FencingToken | null = null;
  private votesReceived = new Map<string, boolean>();
  private lastLeaderHeartbeat: number = Date.now();
  private electionStartedAt: number | null = null;
  private electionTimeout: number;
  private knownReplicas: Map<string, ReplicaInfo> = new Map();

  constructor(
    selfId: ReplicaId,
    config: Partial<ReplicationConfig> = {}
  ) {
    this.selfId = selfId;
    this.config = { ...DEFAULT_REPLICATION_CONFIG, ...config };
    this.electionTimeout = this.calculateElectionTimeout();
  }

  // ==========================================================================
  // PUBLIC METHODS
  // ==========================================================================

  /**
   * Get current election state
   */
  getState(): LeaderElectionState {
    return {
      leader: this.currentLeader,
      fencingToken: this.fencingToken,
      term: this.currentTerm,
      votedFor: this.votedFor,
      votes: new Map(this.votesReceived),
      electionStartedAt: this.electionStartedAt,
      lastLeaderHeartbeat: this.lastLeaderHeartbeat,
    };
  }

  /**
   * Get current role
   */
  getRole(): ElectionRole {
    return this.role;
  }

  /**
   * Get current fencing token
   */
  getFencingToken(): FencingToken | null {
    return this.fencingToken;
  }

  /**
   * Register a known replica for quorum calculation
   */
  registerReplica(info: ReplicaInfo): void {
    this.knownReplicas.set(serializeReplicaId(info.id), info);
  }

  /**
   * Unregister a replica
   */
  unregisterReplica(replicaId: ReplicaId): void {
    this.knownReplicas.delete(serializeReplicaId(replicaId));
  }

  /**
   * Check if election timeout has elapsed
   */
  shouldStartElection(): boolean {
    if (this.role === 'leader') {
      return false;
    }

    const timeSinceHeartbeat = Date.now() - this.lastLeaderHeartbeat;
    return timeSinceHeartbeat > this.electionTimeout;
  }

  /**
   * Start a new election
   */
  async startElection(currentLSN: bigint): Promise<VoteRequest> {
    // Increment term
    this.currentTerm += 1n;

    // Transition to candidate
    this.role = 'candidate';
    this.votedFor = this.selfId;
    this.votesReceived.clear();
    this.votesReceived.set(serializeReplicaId(this.selfId), true);
    this.electionStartedAt = Date.now();
    this.currentLeader = null;

    // Generate new fencing token
    this.fencingToken = await generateFencingToken(this.currentTerm, this.selfId);

    // Reset election timeout with jitter
    this.electionTimeout = this.calculateElectionTimeout();

    logger.info('Starting election', {
      term: this.currentTerm.toString(),
      selfId: serializeReplicaId(this.selfId),
    });

    return {
      candidateId: this.selfId,
      term: this.currentTerm,
      lastLSN: currentLSN,
      fencingToken: this.fencingToken,
    };
  }

  /**
   * Handle vote request from another candidate
   */
  async handleVoteRequest(
    request: VoteRequest,
    ourLSN: bigint
  ): Promise<VoteResponse> {
    const reason: string[] = [];
    let voteGranted = false;

    // Step down if we see a higher term
    if (request.term > this.currentTerm) {
      this.stepDown(request.term);
    }

    // Don't vote if request is from an older term
    if (request.term < this.currentTerm) {
      reason.push(`Stale term: ${request.term} < ${this.currentTerm}`);
    }
    // Don't vote if we already voted for someone else in this term
    else if (this.votedFor !== null && !replicaIdsEqual(this.votedFor, request.candidateId)) {
      reason.push(`Already voted for ${serializeReplicaId(this.votedFor)}`);
    }
    // Don't vote if candidate's log is behind ours
    else if (request.lastLSN < ourLSN) {
      reason.push(`Candidate LSN ${request.lastLSN} < our LSN ${ourLSN}`);
    }
    // Validate fencing token
    else if (!(await validateFencingTokenSignature(request.fencingToken))) {
      reason.push('Invalid fencing token signature');
    }
    // Grant vote
    else {
      voteGranted = true;
      this.votedFor = request.candidateId;
      this.lastLeaderHeartbeat = Date.now(); // Reset timeout
      reason.push('Vote granted');

      logger.info('Granting vote', {
        to: serializeReplicaId(request.candidateId),
        term: request.term.toString(),
      });
    }

    return {
      voterId: this.selfId,
      term: this.currentTerm,
      voteGranted,
      reason: reason.join('; '),
    };
  }

  /**
   * Handle vote response
   * Returns true if we have won the election
   */
  async handleVoteResponse(response: VoteResponse): Promise<boolean> {
    // Ignore if we're not a candidate anymore
    if (this.role !== 'candidate') {
      return false;
    }

    // Step down if we see a higher term
    if (response.term > this.currentTerm) {
      this.stepDown(response.term);
      return false;
    }

    // Ignore responses from old terms
    if (response.term !== this.currentTerm) {
      return false;
    }

    // Record vote
    this.votesReceived.set(serializeReplicaId(response.voterId), response.voteGranted);

    // Check if we have quorum
    if (this.hasQuorum()) {
      await this.becomeLeader();
      return true;
    }

    return false;
  }

  /**
   * Handle heartbeat from leader
   */
  handleLeaderHeartbeat(heartbeat: LeaderHeartbeat): void {
    // Step down if we see a higher term
    if (heartbeat.term > this.currentTerm) {
      this.stepDown(heartbeat.term);
    }

    // Ignore heartbeats from old terms
    if (heartbeat.term < this.currentTerm) {
      return;
    }

    // Accept leader
    this.currentLeader = heartbeat.leaderId;
    this.fencingToken = heartbeat.fencingToken;
    this.lastLeaderHeartbeat = Date.now();

    // If we were a candidate, step down
    if (this.role === 'candidate') {
      this.role = 'follower';
      this.electionStartedAt = null;
    }
  }

  /**
   * Generate leader heartbeat (only valid if we are leader)
   */
  generateHeartbeat(currentLSN: bigint): LeaderHeartbeat | null {
    if (this.role !== 'leader' || !this.fencingToken) {
      return null;
    }

    return {
      leaderId: this.selfId,
      term: this.currentTerm,
      fencingToken: this.fencingToken,
      currentLSN,
      timestamp: Date.now(),
    };
  }

  /**
   * Validate a fencing token against our current state
   */
  async validateFencingToken(token: FencingToken): Promise<boolean> {
    // Check signature
    if (!(await validateFencingTokenSignature(token))) {
      return false;
    }

    // Check if expired
    if (isFencingTokenExpired(token, this.config.fencingTokenTtlMs)) {
      return false;
    }

    // Check if token is from current or future term
    if (token.epoch < this.currentTerm) {
      return false;
    }

    return true;
  }

  /**
   * Check promotion eligibility
   */
  checkPromotionEligibility(
    currentLSN: bigint,
    lastKnownPrimaryLSN: bigint
  ): PromotionEligibility {
    const timeSinceHeartbeat = Date.now() - this.lastLeaderHeartbeat;
    const lsnLag = lastKnownPrimaryLSN - currentLSN;

    // Not eligible if we're already the leader
    if (this.role === 'leader') {
      return {
        eligible: false,
        priority: 0,
        reason: 'Already leader',
        lsnLag,
        timeSinceLastHeartbeat: timeSinceHeartbeat,
      };
    }

    // Not eligible if auto-failover is disabled
    if (!this.config.autoFailover) {
      return {
        eligible: false,
        priority: 0,
        reason: 'Auto-failover disabled',
        lsnLag,
        timeSinceLastHeartbeat: timeSinceHeartbeat,
      };
    }

    // Not eligible if we recently heard from leader
    if (timeSinceHeartbeat < this.config.heartbeatTimeoutMs) {
      return {
        eligible: false,
        priority: 0,
        reason: `Leader still active (last heartbeat ${timeSinceHeartbeat}ms ago)`,
        lsnLag,
        timeSinceLastHeartbeat: timeSinceHeartbeat,
      };
    }

    // Calculate priority based on LSN lag and other factors
    // Higher priority = more suitable for promotion
    let priority = 100;

    // Penalize for LSN lag
    priority -= Number(lsnLag) * 10;

    // Ensure priority is not negative
    priority = Math.max(0, priority);

    return {
      eligible: true,
      priority,
      reason: `Eligible for promotion (primary timeout: ${timeSinceHeartbeat}ms, lag: ${lsnLag} entries)`,
      lsnLag,
      timeSinceLastHeartbeat: timeSinceHeartbeat,
    };
  }

  /**
   * Detect split-brain scenario
   */
  detectSplitBrain(observedLeaders: ReplicaId[]): SplitBrainDetection {
    // Filter to unique leaders
    const uniqueLeaders = observedLeaders.filter((leader, index) => {
      return observedLeaders.findIndex(l => replicaIdsEqual(l, leader)) === index;
    });

    if (uniqueLeaders.length <= 1) {
      return {
        detected: false,
        conflictingLeaders: [],
        resolution: 'none',
        details: 'No split-brain detected',
      };
    }

    logger.warn('Split-brain detected', {
      leaders: uniqueLeaders.map(l => serializeReplicaId(l)),
    });

    return {
      detected: true,
      conflictingLeaders: uniqueLeaders,
      resolution: 'fencing',
      details: `Multiple leaders detected: ${uniqueLeaders.map(l => serializeReplicaId(l)).join(', ')}`,
    };
  }

  // ==========================================================================
  // PRIVATE METHODS
  // ==========================================================================

  private calculateElectionTimeout(): number {
    const jitter = Math.random() * this.config.electionTimeoutJitterMs;
    return this.config.electionTimeoutMs + jitter;
  }

  private stepDown(newTerm: bigint): void {
    this.currentTerm = newTerm;
    this.role = 'follower';
    this.votedFor = null;
    this.electionStartedAt = null;

    logger.info('Stepping down', {
      newTerm: newTerm.toString(),
      selfId: serializeReplicaId(this.selfId),
    });
  }

  private hasQuorum(): boolean {
    const yesVotes = Array.from(this.votesReceived.values()).filter(v => v).length;
    const totalNodes = this.knownReplicas.size + 1; // Include self

    // Calculate required quorum
    const quorumSize = this.config.quorumSize > 0
      ? this.config.quorumSize
      : Math.floor(totalNodes / 2) + 1;

    return yesVotes >= quorumSize;
  }

  private async becomeLeader(): Promise<void> {
    this.role = 'leader';
    this.currentLeader = this.selfId;
    this.electionStartedAt = null;

    // Generate fresh fencing token as new leader
    this.fencingToken = await generateFencingToken(this.currentTerm, this.selfId);

    logger.info('Became leader', {
      term: this.currentTerm.toString(),
      selfId: serializeReplicaId(this.selfId),
      fencingToken: this.fencingToken.epoch.toString(),
    });
  }
}

// =============================================================================
// SPLIT-BRAIN RESOLVER
// =============================================================================

/**
 * Resolves split-brain scenarios using fencing tokens
 */
export class SplitBrainResolver {
  private readonly config: ReplicationConfig;

  constructor(config: Partial<ReplicationConfig> = {}) {
    this.config = { ...DEFAULT_REPLICATION_CONFIG, ...config };
  }

  /**
   * Determine the legitimate leader in a split-brain scenario
   */
  resolveConflict(
    leaders: Array<{ id: ReplicaId; token: FencingToken; lsn: bigint }>
  ): { winner: ReplicaId; losers: ReplicaId[]; reason: string } | null {
    if (leaders.length === 0) {
      return null;
    }

    if (leaders.length === 1) {
      return {
        winner: leaders[0].id,
        losers: [],
        reason: 'Single leader',
      };
    }

    // Sort by fencing token epoch (descending), then by LSN (descending)
    const sorted = [...leaders].sort((a, b) => {
      const tokenComparison = compareFencingTokens(b.token, a.token);
      if (tokenComparison !== 0) return tokenComparison;

      if (b.lsn > a.lsn) return 1;
      if (b.lsn < a.lsn) return -1;
      return 0;
    });

    const winner = sorted[0];
    const losers = sorted.slice(1).map(l => l.id);

    logger.info('Resolved split-brain', {
      winner: serializeReplicaId(winner.id),
      losers: losers.map(l => serializeReplicaId(l)),
      winnerEpoch: winner.token.epoch.toString(),
    });

    return {
      winner: winner.id,
      losers,
      reason: `Winner has highest fencing token epoch (${winner.token.epoch})`,
    };
  }

  /**
   * Fence a stale leader
   */
  shouldFenceLeader(
    leaderToken: FencingToken,
    newToken: FencingToken
  ): boolean {
    // Leader should be fenced if new token has higher epoch
    return compareFencingTokens(newToken, leaderToken) > 0;
  }
}

// Functions are already exported with `export function` declarations above
