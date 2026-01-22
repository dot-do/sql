/**
 * DoSQL Distributed Transaction Types
 *
 * Shared types for distributed transaction handling including state machines,
 * locks, operations, and transaction contexts.
 *
 * @packageDocumentation
 */

import type { ShardId } from '../sharding/types.js';
import type { IsolationLevel } from '../transaction/types.js';

// =============================================================================
// DISTRIBUTED TRANSACTION TYPES
// =============================================================================

/**
 * Distributed transaction state machine states
 */
export type DistributedTransactionState =
  | 'INITIATED'
  | 'PREPARING'
  | 'PREPARED'
  | 'COMMITTING'
  | 'COMMITTED'
  | 'ABORTING'
  | 'ABORTED';

/**
 * Participant vote in 2PC prepare phase
 */
export type ParticipantVote = 'YES' | 'NO' | 'TIMEOUT';

/**
 * Decision made by coordinator
 */
export type CoordinatorDecision = 'COMMIT' | 'ABORT' | 'PENDING';

/**
 * Lock type for distributed transactions
 */
export type DistributedLockType = 'SHARED' | 'EXCLUSIVE';

/**
 * Distributed lock info
 */
export interface DistributedLock {
  /** Resource being locked (table:key) */
  resource: string;
  /** Lock type */
  lockType: DistributedLockType;
  /** When lock was acquired */
  acquiredAt: number;
  /** Transaction holding the lock */
  txnId: string;
}

/**
 * Operation to be executed in distributed transaction
 */
export interface DistributedOperation {
  /** Target shard */
  shard: ShardId;
  /** SQL statement */
  sql: string;
  /** Parameters */
  params?: unknown[];
  /** Operation type */
  type: 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE';
  /** Affected rows (for undo) */
  affectedKeys?: string[];
}

/**
 * Distributed transaction context
 */
export interface DistributedTransactionContext {
  /** Unique transaction ID */
  txnId: string;
  /** Coordinator shard/node ID */
  coordinatorId: string;
  /** Participating shards */
  participants: ShardId[];
  /** Current state */
  state: DistributedTransactionState;
  /** When transaction started */
  startedAt: number;
  /** Transaction timeout in ms */
  timeout: number;
  /** Votes received from participants */
  prepareVotes: Map<string, ParticipantVote>;
  /** Locked rows per shard */
  lockedRows: Map<string, Set<string>>;
  /** Pending operations */
  operations: DistributedOperation[];
  /** Read set (for read-your-writes) */
  readSet: Map<string, Map<string, unknown>>;
  /** Write set (uncommitted writes) */
  writeSet: Map<string, Map<string, unknown>>;
  /** Isolation level */
  isolationLevel: IsolationLevel;
  /** Coordinator decision */
  decision: CoordinatorDecision;
  /** Prepare timestamp */
  preparedAt?: number;
  /** Commit timestamp */
  committedAt?: number;
}

/**
 * Transaction log entry for durability
 */
export interface TransactionLogRecord {
  /** Transaction ID */
  txnId: string;
  /** Record type */
  type: 'BEGIN' | 'PREPARE' | 'COMMIT' | 'ABORT' | 'PREPARE_ACK' | 'COMMIT_ACK';
  /** Participants */
  participants: string[];
  /** Timestamp */
  timestamp: number;
  /** Decision (for PREPARE/COMMIT records) */
  decision?: CoordinatorDecision;
  /** Vote (for PREPARE_ACK records) */
  vote?: ParticipantVote;
  /** Shard ID (for participant records) */
  shardId?: string;
}

/**
 * Participant state for 2PC
 */
export interface ParticipantState {
  /** Shard ID */
  shardId: ShardId;
  /** Current vote */
  vote?: ParticipantVote;
  /** Prepared data (for commit) */
  preparedData?: unknown;
  /** Last communication timestamp */
  lastSeen: number;
  /** Number of retries */
  retryCount: number;
  /** Held locks */
  locks: DistributedLock[];
}

// =============================================================================
// RPC INTERFACE FOR SHARD COMMUNICATION
// =============================================================================

/**
 * RPC interface for shard communication in 2PC
 */
export interface ShardParticipantRPC {
  /** Send PREPARE to a participant */
  prepare(
    shardId: string,
    txnId: string,
    operations: DistributedOperation[]
  ): Promise<{ vote: ParticipantVote; preparedData?: unknown }>;

  /** Send COMMIT to a participant */
  commit(shardId: string, txnId: string): Promise<void>;

  /** Send ABORT/ROLLBACK to a participant */
  abort(shardId: string, txnId: string): Promise<void>;

  /** Query decision from coordinator (for participant recovery) */
  queryDecision(coordinatorId: string, txnId: string): Promise<CoordinatorDecision>;

  /** Execute operations (for non-transactional queries) */
  execute(
    shardId: string,
    sql: string,
    params?: unknown[]
  ): Promise<{ rows: unknown[]; rowsAffected: number }>;
}

// =============================================================================
// DISTRIBUTED TRANSACTION LOG
// =============================================================================

/**
 * Durable transaction log for 2PC recovery
 */
export interface DistributedTransactionLog {
  /** Write a log record */
  write(record: TransactionLogRecord): Promise<void>;

  /** Read all records for a transaction */
  read(txnId: string): Promise<TransactionLogRecord[]>;

  /** Get all pending (in-doubt) transactions */
  getPending(): Promise<TransactionLogRecord[]>;

  /** Delete logs for a completed transaction */
  delete(txnId: string): Promise<void>;

  /** Sync log to durable storage */
  sync(): Promise<void>;
}

/**
 * In-memory transaction log implementation
 */
export class InMemoryTransactionLog implements DistributedTransactionLog {
  private logs: Map<string, TransactionLogRecord[]> = new Map();

  async write(record: TransactionLogRecord): Promise<void> {
    const txnLogs = this.logs.get(record.txnId) || [];
    txnLogs.push(record);
    this.logs.set(record.txnId, txnLogs);
  }

  async read(txnId: string): Promise<TransactionLogRecord[]> {
    return this.logs.get(txnId) || [];
  }

  async getPending(): Promise<TransactionLogRecord[]> {
    const pending: TransactionLogRecord[] = [];
    for (const [txnId, records] of this.logs) {
      const lastRecord = records[records.length - 1];
      if (lastRecord && lastRecord.type !== 'COMMIT' && lastRecord.type !== 'ABORT') {
        pending.push(lastRecord);
      }
    }
    return pending;
  }

  async delete(txnId: string): Promise<void> {
    this.logs.delete(txnId);
  }

  async sync(): Promise<void> {
    // No-op for in-memory
  }
}

// =============================================================================
// OPTIONS AND CONFIG TYPES
// =============================================================================

/**
 * Configuration for the distributed transaction coordinator
 */
export interface CoordinatorConfig {
  /** Coordinator ID */
  coordinatorId: string;
  /** Default timeout for prepare phase (ms) */
  prepareTimeoutMs?: number;
  /** Default timeout for commit phase (ms) */
  commitTimeoutMs?: number;
  /** Overall transaction timeout (ms) */
  transactionTimeoutMs?: number;
  /** Maximum retries for participant communication */
  maxRetries?: number;
  /** Retry delay (ms) */
  retryDelayMs?: number;
  /** Default isolation level */
  defaultIsolationLevel?: IsolationLevel;
}

/**
 * Options for distributed transaction
 */
export interface DistributedTransactionOptions {
  /** Transaction timeout in ms */
  timeout?: number;
  /** Isolation level */
  isolationLevel?: IsolationLevel;
  /** Read-only transaction */
  readOnly?: boolean;
}

/**
 * Configuration for transaction participant
 */
export interface ParticipantConfig {
  /** This shard's ID */
  shardId: string;
  /** Lock timeout in ms */
  lockTimeoutMs?: number;
  /** How long to wait before querying coordinator */
  uncertaintyWindowMs?: number;
}

/**
 * Local executor interface for participant
 */
export interface LocalExecutor {
  /** Execute SQL locally */
  execute(sql: string, params?: unknown[]): Promise<{ rows: unknown[]; rowsAffected: number }>;

  /** Acquire lock on a resource */
  acquireLock(resource: string, lockType: DistributedLockType, txnId: string, timeout: number): Promise<boolean>;

  /** Release lock */
  releaseLock(resource: string, txnId: string): void;

  /** Release all locks for a transaction */
  releaseAllLocks(txnId: string): void;

  /** Begin local transaction */
  beginLocal(): Promise<void>;

  /** Commit local transaction */
  commitLocal(): Promise<void>;

  /** Rollback local transaction */
  rollbackLocal(): Promise<void>;
}

/**
 * Transaction participant state at shard level
 */
export interface TransactionParticipantContext {
  /** Transaction ID */
  txnId: string;
  /** Coordinator ID */
  coordinatorId: string;
  /** Current state */
  state: 'ACTIVE' | 'PREPARED' | 'COMMITTED' | 'ABORTED';
  /** Held locks */
  locks: DistributedLock[];
  /** Pending operations */
  operations: DistributedOperation[];
  /** Undo log for rollback */
  undoLog: Array<{ sql: string; params: unknown[] }>;
  /** Prepare timestamp */
  preparedAt?: number;
}

/**
 * Cross-shard execution result
 */
export interface CrossShardExecutionResult<T = unknown> {
  /** Whether the transaction succeeded */
  success: boolean;
  /** Results from each operation */
  results: T[];
  /** Error if failed */
  error?: Error;
}
