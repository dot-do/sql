/**
 * Multi-Level Time Travel Types for DoSQL
 *
 * Provides types for querying data at specific points in time across
 * multiple levels of the storage hierarchy:
 * - Local (DO): fsx COW snapshots + WAL replay
 * - Branch: Branch history and fork points
 * - Global (Lakehouse): Cross-shard consistent views
 *
 * @packageDocumentation
 */

import type { SnapshotId, Snapshot, Branch } from '../fsx/cow-types.js';
import type { WALEntry, Checkpoint } from '../wal/types.js';

// =============================================================================
// Time Point Types
// =============================================================================

/**
 * Reference a point in time by LSN (Log Sequence Number)
 * Most precise - refers to exact WAL position
 */
export interface LSNTimePoint {
  type: 'lsn';
  /** The log sequence number */
  lsn: bigint;
}

/**
 * Reference a point in time by timestamp
 * Will resolve to nearest LSN at or before the timestamp
 */
export interface TimestampTimePoint {
  type: 'timestamp';
  /** The timestamp to query as of */
  timestamp: Date;
}

/**
 * Reference a point in time by snapshot ID
 * Exact snapshot - immutable and consistent
 */
export interface SnapshotTimePoint {
  type: 'snapshot';
  /** The snapshot ID (branch@version format) */
  snapshotId: SnapshotId;
}

/**
 * Reference a point in time within a branch
 * Can optionally specify a point within that branch
 */
export interface BranchTimePoint {
  type: 'branch';
  /** The branch name */
  branch: string;
  /** Optional point within the branch (defaults to HEAD) */
  point?: TimePoint;
}

/**
 * Reference a relative point in time
 * Useful for "N versions ago" or "N seconds ago" queries
 */
export interface RelativeTimePoint {
  type: 'relative';
  /** The anchor point (defaults to HEAD) */
  anchor?: TimePoint;
  /** Offset in LSN (negative = earlier) */
  lsnOffset?: bigint;
  /** Offset in time (negative = earlier) */
  timeOffset?: number;
  /** Offset in versions/snapshots (negative = earlier) */
  versionOffset?: number;
}

/**
 * Union type for all time point references
 */
export type TimePoint =
  | LSNTimePoint
  | TimestampTimePoint
  | SnapshotTimePoint
  | BranchTimePoint
  | RelativeTimePoint;

// =============================================================================
// Time Travel Query Types
// =============================================================================

/**
 * Scope of time travel query
 */
export type TimeTravelScope =
  | 'local'   // DO-level only (fsx + WAL)
  | 'branch'  // Branch-level (includes branch history)
  | 'global'; // Lakehouse-level (cross-shard)

/**
 * A time travel query
 */
export interface TimeTravelQuery {
  /** SQL query to execute */
  sql: string;
  /** Point in time to query as of */
  asOf: TimePoint;
  /** Scope of the query */
  scope: TimeTravelScope;
  /** Optional table-specific time points */
  tableOverrides?: Map<string, TimePoint>;
}

/**
 * Parsed AS OF clause from SQL
 */
export interface AsOfClause {
  /** Type of time reference */
  type: 'timestamp' | 'lsn' | 'snapshot' | 'branch' | 'version';
  /** The value (string for timestamp/snapshot/branch, bigint for LSN, number for version) */
  value: string | bigint | number;
  /** Optional branch context */
  branch?: string;
}

// =============================================================================
// Resolution Types
// =============================================================================

/**
 * Result of resolving a time point
 */
export interface ResolvedTimePoint {
  /** The resolved LSN */
  lsn: bigint;
  /** The resolved timestamp */
  timestamp: Date;
  /** The snapshot ID if available */
  snapshotId?: SnapshotId;
  /** The branch context */
  branch: string;
  /** The snapshot containing this point (may be earlier than resolved point) */
  baseSnapshot?: Snapshot;
  /** WAL entries to replay from snapshot to reach this point */
  walReplayRange?: {
    fromLSN: bigint;
    toLSN: bigint;
  };
  /** Resolution metadata */
  resolution: {
    /** Whether the exact point was found */
    exact: boolean;
    /** The original time point requested */
    original: TimePoint;
    /** Notes about the resolution */
    notes?: string;
  };
}

/**
 * A range of time for history queries
 */
export interface TimeRange {
  /** Start time point (inclusive) */
  from: TimePoint;
  /** End time point (inclusive) */
  to: TimePoint;
}

/**
 * Result of resolving a time range
 */
export interface ResolvedTimeRange {
  /** Resolved start point */
  from: ResolvedTimePoint;
  /** Resolved end point */
  to: ResolvedTimePoint;
  /** Snapshots within this range */
  snapshots: Snapshot[];
  /** WAL entry count in this range */
  entryCount: number;
}

// =============================================================================
// Local Time Travel Types (DO-level)
// =============================================================================

/**
 * State for local time travel at a specific point
 */
export interface LocalTimeTravelState {
  /** The resolved time point */
  point: ResolvedTimePoint;
  /** The base snapshot to start from */
  snapshot: Snapshot | null;
  /** WAL entries to apply on top of snapshot */
  walEntries: WALEntry[];
  /** Checkpoint state */
  checkpoint: Checkpoint | null;
  /** Branch context */
  branch: string;
}

/**
 * Result of a local time travel read operation
 */
export interface LocalReadResult {
  /** The data read */
  data: Uint8Array | null;
  /** The state used for the read */
  state: LocalTimeTravelState;
  /** Whether the read was from snapshot (true) or WAL replay (false) */
  fromSnapshot: boolean;
  /** The effective LSN of the read */
  effectiveLSN: bigint;
}

// =============================================================================
// Branch Time Travel Types
// =============================================================================

/**
 * A node in the branch history tree
 */
export interface BranchHistoryNode {
  /** Branch metadata */
  branch: Branch;
  /** Snapshots on this branch */
  snapshots: Snapshot[];
  /** Child branches (forked from this branch) */
  children: BranchHistoryNode[];
  /** Fork point (if this is a child branch) */
  forkPoint?: {
    parentBranch: string;
    parentSnapshot: SnapshotId;
    forkLSN: bigint;
    forkTimestamp: Date;
  };
}

/**
 * Result of finding merge base between branches
 */
export interface MergeBaseResult {
  /** The common ancestor snapshot */
  snapshot: Snapshot;
  /** LSN of the common ancestor */
  lsn: bigint;
  /** Timestamp of the common ancestor */
  timestamp: Date;
  /** Distance (in commits) from source branch */
  sourceDistance: number;
  /** Distance (in commits) from target branch */
  targetDistance: number;
}

/**
 * Fork point information
 */
export interface ForkPoint {
  /** Parent branch name */
  parentBranch: string;
  /** Child branch name */
  childBranch: string;
  /** Snapshot at fork */
  snapshot: Snapshot;
  /** LSN at fork */
  lsn: bigint;
  /** Timestamp of fork */
  timestamp: Date;
}

// =============================================================================
// Global Time Travel Types (Lakehouse-level)
// =============================================================================

/**
 * A lakehouse snapshot - consistent view across all tables
 */
export interface LakehouseSnapshot {
  /** Unique snapshot ID */
  id: string;
  /** Creation timestamp */
  timestamp: Date;
  /** High-water mark LSN across all shards */
  hwmLSN: bigint;
  /** Per-table snapshot references */
  tables: Map<string, TableSnapshot>;
  /** Schema version at snapshot time */
  schemaVersion: number;
  /** Optional description */
  description?: string;
}

/**
 * Per-table snapshot within a lakehouse snapshot
 */
export interface TableSnapshot {
  /** Table name */
  table: string;
  /** Table-specific snapshot ID */
  snapshotId: SnapshotId;
  /** Row count at snapshot time */
  rowCount: number;
  /** Size in bytes */
  sizeBytes: number;
  /** Partition information if applicable */
  partitions?: PartitionSnapshot[];
}

/**
 * Partition-level snapshot
 */
export interface PartitionSnapshot {
  /** Partition key value(s) */
  partitionKey: Record<string, unknown>;
  /** Shard location */
  shard: string;
  /** Snapshot ID for this partition */
  snapshotId: SnapshotId;
  /** Row count in partition */
  rowCount: number;
}

/**
 * Options for global time travel queries
 */
export interface GlobalTimeTravelOptions {
  /** The time point to query as of */
  asOf: TimePoint;
  /** Whether to include data not yet in lakehouse (from DOs) */
  includePendingData?: boolean;
  /** Consistency level */
  consistency: 'snapshot' | 'eventual' | 'strong';
  /** Tables to query (empty = all) */
  tables?: string[];
  /** Timeout for cross-shard coordination */
  timeoutMs?: number;
}

/**
 * Result of a global time travel query
 */
export interface GlobalTimeTravelResult {
  /** The lakehouse snapshot used */
  lakehouseSnapshot: LakehouseSnapshot;
  /** Data from DOs not yet in lakehouse */
  pendingData?: PendingDataResult[];
  /** The effective time point */
  effectivePoint: ResolvedTimePoint;
  /** Per-shard status */
  shardStatus: Map<string, ShardQueryStatus>;
}

/**
 * Data pending in DOs not yet flushed to lakehouse
 */
export interface PendingDataResult {
  /** The DO/shard that has pending data */
  shard: string;
  /** Table name */
  table: string;
  /** LSN range of pending data */
  lsnRange: { from: bigint; to: bigint };
  /** Whether this data was included in the result */
  included: boolean;
  /** Entries if included */
  entries?: WALEntry[];
}

/**
 * Query status for a single shard
 */
export interface ShardQueryStatus {
  /** Shard identifier */
  shard: string;
  /** Whether query succeeded */
  success: boolean;
  /** LSN used for this shard */
  lsn: bigint;
  /** Latency in ms */
  latencyMs: number;
  /** Error if failed */
  error?: string;
}

// =============================================================================
// Coordinator Types
// =============================================================================

/**
 * Time travel coordinator state
 */
export interface CoordinatorState {
  /** Currently active time travel sessions */
  activeSessions: Map<string, TimeTravelSession>;
  /** Cache of resolved time points */
  resolvedCache: Map<string, ResolvedTimePoint>;
  /** Lakehouse snapshot metadata */
  lakehouseSnapshots: LakehouseSnapshot[];
}

/**
 * A time travel session for a query
 */
export interface TimeTravelSession {
  /** Session ID */
  id: string;
  /** The query being executed */
  query: TimeTravelQuery;
  /** Resolved time point */
  resolved: ResolvedTimePoint;
  /** State per table involved */
  tableStates: Map<string, LocalTimeTravelState>;
  /** Session start time */
  startedAt: Date;
  /** Session timeout */
  timeout: number;
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * Time travel specific error codes
 */
export enum TimeTravelErrorCode {
  /** The requested time point could not be resolved */
  POINT_NOT_FOUND = 'TT_POINT_NOT_FOUND',
  /** The requested LSN is not available (too old, archived) */
  LSN_NOT_AVAILABLE = 'TT_LSN_NOT_AVAILABLE',
  /** The requested snapshot doesn't exist */
  SNAPSHOT_NOT_FOUND = 'TT_SNAPSHOT_NOT_FOUND',
  /** The requested branch doesn't exist */
  BRANCH_NOT_FOUND = 'TT_BRANCH_NOT_FOUND',
  /** Time travel query timed out */
  TIMEOUT = 'TT_TIMEOUT',
  /** Gap in data between snapshot and requested point */
  DATA_GAP = 'TT_DATA_GAP',
  /** Cross-shard consistency couldn't be achieved */
  CONSISTENCY_ERROR = 'TT_CONSISTENCY_ERROR',
  /** Invalid AS OF syntax */
  INVALID_SYNTAX = 'TT_INVALID_SYNTAX',
  /** Time point is in the future */
  FUTURE_POINT = 'TT_FUTURE_POINT',
  /** Session expired */
  SESSION_EXPIRED = 'TT_SESSION_EXPIRED',
}

/**
 * Time travel specific error
 */
export class TimeTravelError extends Error {
  constructor(
    public readonly code: TimeTravelErrorCode,
    message: string,
    public readonly timePoint?: TimePoint,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'TimeTravelError';
  }
}

// =============================================================================
// Utility Types
// =============================================================================

/**
 * Serialization format for time points (for caching/transport)
 */
export interface SerializedTimePoint {
  type: TimePoint['type'];
  lsn?: string; // bigint serialized as string
  timestamp?: string; // ISO date string
  snapshotId?: SnapshotId;
  branch?: string;
  point?: SerializedTimePoint;
  lsnOffset?: string;
  timeOffset?: number;
  versionOffset?: number;
  anchor?: SerializedTimePoint;
}

/**
 * Serialize a time point for storage/transport
 */
export function serializeTimePoint(point: TimePoint): SerializedTimePoint {
  switch (point.type) {
    case 'lsn':
      return { type: 'lsn', lsn: point.lsn.toString() };
    case 'timestamp':
      return { type: 'timestamp', timestamp: point.timestamp.toISOString() };
    case 'snapshot':
      return { type: 'snapshot', snapshotId: point.snapshotId };
    case 'branch':
      return {
        type: 'branch',
        branch: point.branch,
        point: point.point ? serializeTimePoint(point.point) : undefined,
      };
    case 'relative':
      return {
        type: 'relative',
        anchor: point.anchor ? serializeTimePoint(point.anchor) : undefined,
        lsnOffset: point.lsnOffset?.toString(),
        timeOffset: point.timeOffset,
        versionOffset: point.versionOffset,
      };
  }
}

/**
 * Deserialize a time point from storage/transport
 */
export function deserializeTimePoint(data: SerializedTimePoint): TimePoint {
  switch (data.type) {
    case 'lsn':
      return { type: 'lsn', lsn: BigInt(data.lsn!) };
    case 'timestamp':
      return { type: 'timestamp', timestamp: new Date(data.timestamp!) };
    case 'snapshot':
      return { type: 'snapshot', snapshotId: data.snapshotId! };
    case 'branch':
      return {
        type: 'branch',
        branch: data.branch!,
        point: data.point ? deserializeTimePoint(data.point) : undefined,
      };
    case 'relative':
      return {
        type: 'relative',
        anchor: data.anchor ? deserializeTimePoint(data.anchor) : undefined,
        lsnOffset: data.lsnOffset ? BigInt(data.lsnOffset) : undefined,
        timeOffset: data.timeOffset,
        versionOffset: data.versionOffset,
      };
    default:
      throw new TimeTravelError(
        TimeTravelErrorCode.INVALID_SYNTAX,
        `Unknown time point type: ${(data as { type: string }).type}`
      );
  }
}

/**
 * Create a time point from LSN
 */
export function lsn(value: bigint): LSNTimePoint {
  return { type: 'lsn', lsn: value };
}

/**
 * Create a time point from timestamp
 */
export function timestamp(value: Date | string | number): TimestampTimePoint {
  return {
    type: 'timestamp',
    timestamp: value instanceof Date ? value : new Date(value),
  };
}

/**
 * Create a time point from snapshot ID
 */
export function snapshot(id: SnapshotId): SnapshotTimePoint {
  return { type: 'snapshot', snapshotId: id };
}

/**
 * Create a time point from branch name
 */
export function branch(name: string, point?: TimePoint): BranchTimePoint {
  return { type: 'branch', branch: name, point };
}

/**
 * Create a relative time point
 */
export function relative(
  options: Omit<RelativeTimePoint, 'type'>
): RelativeTimePoint {
  return { type: 'relative', ...options };
}
