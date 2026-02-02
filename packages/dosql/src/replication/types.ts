/**
 * Multi-Region Replication Types for DoSQL
 *
 * Defines the core types for:
 * - Primary DO handling writes
 * - Replica DOs handling reads
 * - WAL streaming between primary and replicas
 * - Conflict resolution for eventual multi-master
 *
 * @packageDocumentation
 */

import type { WALEntry, WALSegment } from '../wal/types.js';

// =============================================================================
// REPLICA IDENTITY
// =============================================================================

/**
 * Unique identifier for a replica
 */
export interface ReplicaId {
  /** Region identifier (e.g., 'us-west-1', 'eu-central-1') */
  region: string;
  /** Unique replica instance ID within the region */
  instanceId: string;
}

/**
 * Replica registration status
 */
export type ReplicaStatus =
  | 'registering'
  | 'syncing'
  | 'active'
  | 'lagging'
  | 'offline'
  | 'deregistering';

/**
 * Replica role in the replication topology
 */
export type ReplicaRole = 'primary' | 'replica' | 'witness';

/**
 * Information about a registered replica
 */
export interface ReplicaInfo {
  /** Replica identifier */
  id: ReplicaId;
  /** Current status */
  status: ReplicaStatus;
  /** Role in replication */
  role: ReplicaRole;
  /** Last known LSN */
  lastLSN: bigint;
  /** Last heartbeat timestamp (Unix ms) */
  lastHeartbeat: number;
  /** Registration timestamp */
  registeredAt: number;
  /** Durable Object stub URL for RPC */
  doUrl: string;
  /** Region-specific metadata */
  metadata?: Record<string, unknown>;
}

// =============================================================================
// WAL STREAMING TYPES
// =============================================================================

/**
 * WAL stream position
 */
export interface StreamPosition {
  /** Current LSN position */
  lsn: bigint;
  /** Segment ID containing the LSN */
  segmentId: string;
  /** Offset within the segment */
  offset: number;
}

/**
 * WAL batch for streaming to replicas
 */
export interface WALBatch {
  /** Start LSN of this batch */
  startLSN: bigint;
  /** End LSN of this batch */
  endLSN: bigint;
  /** Entries in this batch */
  entries: WALEntry[];
  /** Batch checksum */
  checksum: number;
  /** Timestamp when batch was created */
  timestamp: number;
  /** Whether this is a compressed batch */
  compressed?: boolean;
}

/**
 * Acknowledgment from replica after applying WAL batch
 */
export interface WALAck {
  /** Replica ID */
  replicaId: ReplicaId;
  /** Last LSN successfully applied */
  appliedLSN: bigint;
  /** Processing time in milliseconds */
  processingTimeMs: number;
  /** Any errors encountered */
  errors?: WALApplyError[];
}

/**
 * Error that occurred while applying WAL entry
 */
export interface WALApplyError {
  /** LSN of the failed entry */
  lsn: bigint;
  /** Error code */
  code: WALApplyErrorCode;
  /** Error message */
  message: string;
  /** Whether this error is retryable */
  retryable: boolean;
}

/**
 * WAL apply error codes
 */
export enum WALApplyErrorCode {
  /** Entry already applied (duplicate) */
  DUPLICATE = 'DUPLICATE',
  /** Missing prerequisite entry */
  MISSING_PREREQUISITE = 'MISSING_PREREQUISITE',
  /** Schema mismatch */
  SCHEMA_MISMATCH = 'SCHEMA_MISMATCH',
  /** Conflict detected */
  CONFLICT = 'CONFLICT',
  /** Storage error */
  STORAGE_ERROR = 'STORAGE_ERROR',
  /** Checksum mismatch */
  CHECKSUM_MISMATCH = 'CHECKSUM_MISMATCH',
}

// =============================================================================
// SNAPSHOT TYPES
// =============================================================================

/**
 * Snapshot metadata
 */
export interface SnapshotInfo {
  /** Unique snapshot identifier */
  id: string;
  /** LSN at the time of snapshot */
  lsn: bigint;
  /** Snapshot creation timestamp */
  createdAt: number;
  /** Total size in bytes */
  sizeBytes: number;
  /** Number of chunks */
  chunkCount: number;
  /** Schema version */
  schemaVersion: number;
  /** Tables included in snapshot */
  tables: string[];
  /** Checksum of entire snapshot */
  checksum: number;
}

/**
 * Snapshot chunk for incremental transfer
 */
export interface SnapshotChunk {
  /** Snapshot ID */
  snapshotId: string;
  /** Chunk index (0-based) */
  chunkIndex: number;
  /** Total chunks */
  totalChunks: number;
  /** Chunk data */
  data: Uint8Array;
  /** Chunk checksum */
  checksum: number;
}

/**
 * Snapshot request from replica
 */
export interface SnapshotRequest {
  /** Requesting replica ID */
  replicaId: ReplicaId;
  /** Current LSN of the replica (for incremental sync) */
  currentLSN?: bigint;
  /** Preferred chunk size */
  preferredChunkSize?: number;
}

// =============================================================================
// ROUTING TYPES
// =============================================================================

/**
 * Consistency level for read operations
 */
export type ConsistencyLevel =
  | 'eventual'      // Read from any replica
  | 'session'       // Read your writes within session
  | 'bounded'       // Read with bounded staleness
  | 'strong';       // Read from primary only

/**
 * Read preference for routing
 */
export type ReadPreference =
  | 'primary'           // Always read from primary
  | 'primaryPreferred'  // Primary if available, else replica
  | 'replica'           // Always read from replica
  | 'replicaPreferred'  // Replica if available, else primary
  | 'nearest';          // Lowest latency

/**
 * Routing decision for a query
 */
export interface RoutingDecision {
  /** Target replica ID */
  target: ReplicaId;
  /** Chosen consistency level */
  consistency: ConsistencyLevel;
  /** Reason for routing decision */
  reason: string;
  /** Estimated latency */
  estimatedLatencyMs?: number;
  /** Whether this is a fallback decision */
  fallback: boolean;
}

/**
 * Session state for read-your-writes consistency
 */
export interface SessionState {
  /** Session ID */
  sessionId: string;
  /** Last write LSN in this session */
  lastWriteLSN: bigint;
  /** Session start time */
  startedAt: number;
  /** Preferred region */
  preferredRegion?: string;
}

// =============================================================================
// REPLICATION LAG MONITORING
// =============================================================================

/**
 * Replication lag metrics for a replica
 */
export interface ReplicationLag {
  /** Replica ID */
  replicaId: ReplicaId;
  /** Primary LSN at measurement time */
  primaryLSN: bigint;
  /** Replica LSN at measurement time */
  replicaLSN: bigint;
  /** LSN difference */
  lagEntries: bigint;
  /** Estimated lag in milliseconds */
  lagMs: number;
  /** Measurement timestamp */
  measuredAt: number;
}

/**
 * Replication health status
 */
export interface ReplicationHealth {
  /** Primary replica info */
  primary: ReplicaInfo;
  /** All replicas with their lag */
  replicas: Array<{
    info: ReplicaInfo;
    lag: ReplicationLag;
  }>;
  /** Overall health status */
  status: 'healthy' | 'degraded' | 'critical';
  /** Health check timestamp */
  checkedAt: number;
}

// =============================================================================
// FAILOVER TYPES
// =============================================================================

/**
 * Failover reason
 */
export type FailoverReason =
  | 'primary_unreachable'
  | 'primary_lagging'
  | 'manual_trigger'
  | 'scheduled_maintenance'
  | 'split_brain_resolution';

/**
 * Failover state
 */
export interface FailoverState {
  /** Current failover status */
  status: 'idle' | 'initiating' | 'in_progress' | 'completed' | 'failed' | 'rolled_back';
  /** Old primary */
  oldPrimary?: ReplicaId;
  /** New primary */
  newPrimary?: ReplicaId;
  /** Failover reason */
  reason?: FailoverReason;
  /** Failover start time */
  startedAt?: number;
  /** Failover completion time */
  completedAt?: number;
  /** Any errors during failover */
  errors?: string[];
}

/**
 * Failover decision
 */
export interface FailoverDecision {
  /** Whether to proceed with failover */
  proceed: boolean;
  /** Candidate for new primary */
  candidate?: ReplicaId;
  /** Reason for decision */
  reason: string;
  /** Data loss estimate (LSN difference) */
  dataLossEstimate?: bigint;
}

// =============================================================================
// CONFLICT RESOLUTION (for multi-master)
// =============================================================================

/**
 * Conflict type
 */
export type ConflictType =
  | 'write_write'   // Same row modified on multiple replicas
  | 'delete_update' // Row deleted on one, updated on another
  | 'insert_insert' // Same key inserted on multiple replicas
  | 'constraint';   // Constraint violation

/**
 * Conflict record
 */
export interface Conflict {
  /** Unique conflict ID */
  id: string;
  /** Conflict type */
  type: ConflictType;
  /** Table name */
  table: string;
  /** Row key */
  key: Uint8Array;
  /** Entries involved in conflict */
  entries: WALEntry[];
  /** Replica origins */
  origins: ReplicaId[];
  /** Conflict detection timestamp */
  detectedAt: number;
  /** Resolution (if resolved) */
  resolution?: ConflictResolution;
}

/**
 * Conflict resolution strategy
 */
export type ConflictResolutionStrategy =
  | 'last_write_wins'      // Use entry with highest timestamp
  | 'first_write_wins'     // Use entry with lowest timestamp
  | 'primary_wins'         // Primary's version always wins
  | 'custom'               // Custom resolution function
  | 'manual';              // Require manual resolution

/**
 * Resolved conflict
 */
export interface ConflictResolution {
  /** Resolution strategy used */
  strategy: ConflictResolutionStrategy;
  /** Winning entry */
  winner: WALEntry;
  /** Resolution timestamp */
  resolvedAt: number;
  /** Who/what resolved it */
  resolvedBy: string;
}

// =============================================================================
// PRIMARY INTERFACE
// =============================================================================

/**
 * Primary Durable Object interface
 */
export interface PrimaryDO {
  /**
   * Register a new replica
   */
  registerReplica(info: Omit<ReplicaInfo, 'registeredAt'>): Promise<void>;

  /**
   * Deregister a replica
   */
  deregisterReplica(replicaId: ReplicaId): Promise<void>;

  /**
   * Get current stream position for a replica
   */
  getStreamPosition(replicaId: ReplicaId): Promise<StreamPosition>;

  /**
   * Pull WAL entries for a replica
   */
  pullWAL(replicaId: ReplicaId, fromLSN: bigint, limit?: number): Promise<WALBatch>;

  /**
   * Acknowledge received WAL entries
   */
  acknowledgeWAL(ack: WALAck): Promise<void>;

  /**
   * Request a snapshot for catch-up
   */
  requestSnapshot(request: SnapshotRequest): Promise<SnapshotInfo>;

  /**
   * Get snapshot chunk
   */
  getSnapshotChunk(snapshotId: string, chunkIndex: number): Promise<SnapshotChunk>;

  /**
   * Get replication health
   */
  getReplicationHealth(): Promise<ReplicationHealth>;

  /**
   * Get all registered replicas
   */
  getReplicas(): Promise<ReplicaInfo[]>;

  /**
   * Initiate failover to a replica
   */
  initiateFailover(reason: FailoverReason, candidateId?: ReplicaId): Promise<FailoverDecision>;

  /**
   * Execute failover
   */
  executeFailover(decision: FailoverDecision): Promise<FailoverState>;
}

// =============================================================================
// REPLICA INTERFACE
// =============================================================================

/**
 * Replica Durable Object interface
 */
export interface ReplicaDO {
  /**
   * Initialize replica with primary URL
   */
  initialize(primaryUrl: string, replicaInfo: Omit<ReplicaInfo, 'registeredAt'>): Promise<void>;

  /**
   * Start WAL streaming from primary
   */
  startStreaming(): Promise<void>;

  /**
   * Stop WAL streaming
   */
  stopStreaming(): Promise<void>;

  /**
   * Apply WAL batch from primary
   */
  applyWALBatch(batch: WALBatch): Promise<WALAck>;

  /**
   * Catch up from snapshot
   */
  catchUpFromSnapshot(snapshotInfo: SnapshotInfo): Promise<void>;

  /**
   * Apply snapshot chunk
   */
  applySnapshotChunk(chunk: SnapshotChunk): Promise<void>;

  /**
   * Get current replica status
   */
  getStatus(): Promise<ReplicaInfo>;

  /**
   * Get current LSN
   */
  getCurrentLSN(): Promise<bigint>;

  /**
   * Heartbeat to primary
   */
  sendHeartbeat(): Promise<void>;

  /**
   * Handle query with consistency level
   */
  handleQuery(sql: string, consistency: ConsistencyLevel, session?: SessionState): Promise<unknown>;

  /**
   * Forward write to primary
   */
  forwardWrite(sql: string): Promise<unknown>;

  /**
   * Promote to primary (during failover)
   */
  promoteToPrimary(): Promise<void>;

  /**
   * Demote to replica (during failover)
   */
  demoteToReplica(newPrimaryUrl: string): Promise<void>;

  // ==========================================================================
  // AUTO-PROMOTION & LEADER ELECTION
  // ==========================================================================

  /**
   * Check if this replica is eligible for auto-promotion
   */
  checkPromotionEligibility(): Promise<PromotionEligibility>;

  /**
   * Start leader election process
   */
  startElection(): Promise<LeaderElectionState>;

  /**
   * Handle vote request from another candidate
   */
  handleVoteRequest(request: VoteRequest): Promise<VoteResponse>;

  /**
   * Handle leader heartbeat
   */
  handleLeaderHeartbeat(heartbeat: LeaderHeartbeat): Promise<void>;

  /**
   * Get current leader election state
   */
  getElectionState(): Promise<LeaderElectionState>;

  /**
   * Validate fencing token
   */
  validateFencingToken(token: FencingToken): Promise<boolean>;

  /**
   * Detect split-brain scenario
   */
  detectSplitBrain(): Promise<SplitBrainDetection>;

  /**
   * Auto-promote with split-brain detection
   */
  autoPromote(): Promise<{ success: boolean; fencingToken: FencingToken | null; error?: string }>;
}

// =============================================================================
// ROUTER INTERFACE
// =============================================================================

/**
 * Replication router interface
 */
export interface ReplicationRouter {
  /**
   * Route a read query
   */
  routeRead(
    sql: string,
    consistency: ConsistencyLevel,
    session?: SessionState
  ): Promise<RoutingDecision>;

  /**
   * Route a write query
   */
  routeWrite(sql: string): Promise<RoutingDecision>;

  /**
   * Get nearest replica for a region
   */
  getNearestReplica(region: string): Promise<ReplicaId | null>;

  /**
   * Update replica status
   */
  updateReplicaStatus(replicaId: ReplicaId, status: ReplicaStatus, lag?: ReplicationLag): void;

  /**
   * Get routing metrics
   */
  getMetrics(): RouterMetrics;
}

/**
 * Router metrics
 */
export interface RouterMetrics {
  /** Total reads routed */
  totalReads: number;
  /** Reads to primary */
  primaryReads: number;
  /** Reads to replicas */
  replicaReads: number;
  /** Total writes routed */
  totalWrites: number;
  /** Failover count */
  failovers: number;
  /** Average routing latency */
  avgRoutingLatencyMs: number;
}

// =============================================================================
// FENCING TOKENS & LEADER ELECTION
// =============================================================================

/**
 * Fencing token for split-brain prevention
 * Monotonically increasing token that invalidates stale leaders
 */
export interface FencingToken {
  /** Token epoch - increments on each leadership change */
  epoch: bigint;
  /** Token generation timestamp */
  generatedAt: number;
  /** Node ID that generated this token */
  generatedBy: ReplicaId;
  /** Cryptographic signature for token validation */
  signature: string;
}

/**
 * Leader election state
 */
export interface LeaderElectionState {
  /** Current leader */
  leader: ReplicaId | null;
  /** Current fencing token */
  fencingToken: FencingToken | null;
  /** Election term (increments with each election) */
  term: bigint;
  /** Nodes that voted in current term */
  votedFor: ReplicaId | null;
  /** Votes received in current election */
  votes: Map<string, boolean>;
  /** Election start timestamp */
  electionStartedAt: number | null;
  /** Last heartbeat from leader */
  lastLeaderHeartbeat: number;
}

/**
 * Vote request for leader election
 */
export interface VoteRequest {
  /** Candidate requesting votes */
  candidateId: ReplicaId;
  /** Election term */
  term: bigint;
  /** Candidate's last LSN */
  lastLSN: bigint;
  /** Candidate's fencing token */
  fencingToken: FencingToken;
}

/**
 * Vote response
 */
export interface VoteResponse {
  /** Voter's replica ID */
  voterId: ReplicaId;
  /** Election term */
  term: bigint;
  /** Whether vote was granted */
  voteGranted: boolean;
  /** Reason for vote decision */
  reason: string;
}

/**
 * Leader heartbeat message
 */
export interface LeaderHeartbeat {
  /** Leader's replica ID */
  leaderId: ReplicaId;
  /** Current term */
  term: bigint;
  /** Current fencing token */
  fencingToken: FencingToken;
  /** Leader's current LSN */
  currentLSN: bigint;
  /** Timestamp */
  timestamp: number;
}

/**
 * Split-brain detection result
 */
export interface SplitBrainDetection {
  /** Whether split-brain was detected */
  detected: boolean;
  /** Conflicting leaders (if detected) */
  conflictingLeaders: ReplicaId[];
  /** Resolution action taken */
  resolution: 'none' | 'fencing' | 'rollback' | 'manual';
  /** Details about the detection */
  details: string;
}

/**
 * Auto-promotion eligibility
 */
export interface PromotionEligibility {
  /** Whether this replica is eligible for promotion */
  eligible: boolean;
  /** Priority score (higher is better) */
  priority: number;
  /** Reason for eligibility decision */
  reason: string;
  /** Current LSN lag from last known primary LSN */
  lsnLag: bigint;
  /** Time since last primary heartbeat */
  timeSinceLastHeartbeat: number;
}

// =============================================================================
// CONFIGURATION
// =============================================================================

/**
 * Replication configuration
 */
export interface ReplicationConfig {
  /** Minimum replicas for quorum */
  minReplicas: number;
  /** WAL batch size for streaming */
  walBatchSize: number;
  /** Heartbeat interval (ms) */
  heartbeatIntervalMs: number;
  /** Heartbeat timeout (ms) */
  heartbeatTimeoutMs: number;
  /** Maximum replication lag before marking unhealthy (ms) */
  maxLagMs: number;
  /** Snapshot chunk size (bytes) */
  snapshotChunkSize: number;
  /** Enable automatic failover */
  autoFailover: boolean;
  /** Conflict resolution strategy */
  conflictStrategy: ConflictResolutionStrategy;
  /** Bounded staleness window (ms) for 'bounded' consistency */
  boundedStalenessMs: number;
  /** Election timeout (ms) - time to wait before starting new election */
  electionTimeoutMs: number;
  /** Election timeout randomization range (ms) */
  electionTimeoutJitterMs: number;
  /** Minimum votes required for quorum (0 = automatic based on replica count) */
  quorumSize: number;
  /** Maximum allowed clock skew between nodes (ms) */
  maxClockSkewMs: number;
  /** Enable split-brain detection */
  splitBrainDetection: boolean;
  /** Fencing token TTL (ms) */
  fencingTokenTtlMs: number;
}

/**
 * Default replication configuration
 */
export const DEFAULT_REPLICATION_CONFIG: Readonly<ReplicationConfig> = {
  minReplicas: 1,
  walBatchSize: 100,
  heartbeatIntervalMs: 5000,
  heartbeatTimeoutMs: 15000,
  maxLagMs: 10000,
  snapshotChunkSize: 1024 * 1024, // 1MB
  autoFailover: true,
  conflictStrategy: 'last_write_wins',
  boundedStalenessMs: 5000,
  electionTimeoutMs: 10000,
  electionTimeoutJitterMs: 5000,
  quorumSize: 0, // Auto-calculate based on replica count
  maxClockSkewMs: 5000,
  splitBrainDetection: true,
  fencingTokenTtlMs: 60000, // 1 minute
};

// =============================================================================
// ERROR TYPES
// =============================================================================

/**
 * Replication error codes
 */
export enum ReplicationErrorCode {
  /** Replica not initialized */
  NOT_INITIALIZED = 'REPL_NOT_INITIALIZED',
  /** Replica not found */
  REPLICA_NOT_FOUND = 'REPL_REPLICA_NOT_FOUND',
  /** Primary not available */
  PRIMARY_UNAVAILABLE = 'REPL_PRIMARY_UNAVAILABLE',
  /** Replication lag too high */
  LAG_TOO_HIGH = 'REPL_LAG_TOO_HIGH',
  /** Snapshot not found */
  SNAPSHOT_NOT_FOUND = 'REPL_SNAPSHOT_NOT_FOUND',
  /** Chunk not found */
  CHUNK_NOT_FOUND = 'REPL_CHUNK_NOT_FOUND',
  /** Failover in progress */
  FAILOVER_IN_PROGRESS = 'REPL_FAILOVER_IN_PROGRESS',
  /** Conflict unresolved */
  CONFLICT_UNRESOLVED = 'REPL_CONFLICT_UNRESOLVED',
  /** Consistency violation */
  CONSISTENCY_VIOLATION = 'REPL_CONSISTENCY_VIOLATION',
  /** Registration failed */
  REGISTRATION_FAILED = 'REPL_REGISTRATION_FAILED',
  /** Streaming error */
  STREAMING_ERROR = 'REPL_STREAMING_ERROR',
  /** Split-brain detected */
  SPLIT_BRAIN_DETECTED = 'REPL_SPLIT_BRAIN_DETECTED',
  /** Invalid fencing token */
  INVALID_FENCING_TOKEN = 'REPL_INVALID_FENCING_TOKEN',
  /** Election in progress */
  ELECTION_IN_PROGRESS = 'REPL_ELECTION_IN_PROGRESS',
  /** Not eligible for promotion */
  NOT_ELIGIBLE_FOR_PROMOTION = 'REPL_NOT_ELIGIBLE_FOR_PROMOTION',
  /** Stale leader detected */
  STALE_LEADER = 'REPL_STALE_LEADER',
  /** Quorum not reached */
  QUORUM_NOT_REACHED = 'REPL_QUORUM_NOT_REACHED',
}

/**
 * Replication error class
 */
export class ReplicationError extends Error {
  constructor(
    public readonly code: ReplicationErrorCode,
    message: string,
    public readonly replicaId?: ReplicaId,
    public readonly lsn?: bigint,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'ReplicationError';
  }
}

// =============================================================================
// UTILITY TYPES
// =============================================================================

/**
 * Serialize ReplicaId to string
 */
export function serializeReplicaId(id: ReplicaId): string {
  return `${id.region}:${id.instanceId}`;
}

/**
 * Deserialize ReplicaId from string
 */
export function deserializeReplicaId(str: string): ReplicaId {
  const [region, instanceId] = str.split(':');
  return { region, instanceId };
}

/**
 * Check if two ReplicaIds are equal
 */
export function replicaIdsEqual(a: ReplicaId, b: ReplicaId): boolean {
  return a.region === b.region && a.instanceId === b.instanceId;
}
