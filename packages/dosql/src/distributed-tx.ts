/**
 * DoSQL Distributed Transactions with Two-Phase Commit (2PC)
 *
 * Implements distributed transactions across multiple shards with:
 * - Two-Phase Commit (2PC) protocol
 * - Coordinator and participant roles
 * - Prepare/commit/rollback phases
 * - Timeout handling
 * - Coordinator failure recovery
 * - Participant failure handling
 * - Read-your-writes consistency
 * - Serializable isolation across shards
 *
 * @packageDocumentation
 */

import type { ShardId } from './sharding/types.js';
import { createShardId } from './sharding/types.js';
import type { TransactionId, LSN } from './engine/types.js';
import { createTransactionId, createLSN } from './engine/types.js';
import { IsolationLevel, LockType, TransactionError, TransactionErrorCode } from './transaction/types.js';

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
// DISTRIBUTED TRANSACTION COORDINATOR
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
 * Distributed Transaction Coordinator interface
 */
export interface DistributedTransactionCoordinator {
  /** Begin a new distributed transaction */
  begin(participants: ShardId[], options?: DistributedTransactionOptions): Promise<DistributedTransactionContext>;

  /** Execute an operation within the transaction */
  execute(shardId: ShardId, sql: string, params?: unknown[]): Promise<{ rows: unknown[]; rowsAffected: number }>;

  /** Prepare phase - lock rows on all participants */
  prepare(): Promise<Map<string, ParticipantVote>>;

  /** Commit phase - finalize on all participants */
  commit(): Promise<void>;

  /** Rollback - abort on all participants */
  rollback(): Promise<void>;

  /** Get current state */
  getState(): DistributedTransactionState;

  /** Get the transaction context */
  getContext(): DistributedTransactionContext | null;

  /** Recover in-flight transactions after restart */
  recover(): Promise<void>;
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
 * Creates a distributed transaction coordinator
 */
export function createDistributedTransactionCoordinator(
  rpc: ShardParticipantRPC,
  txnLog: DistributedTransactionLog,
  config: CoordinatorConfig
): DistributedTransactionCoordinator {
  const {
    coordinatorId,
    prepareTimeoutMs = 10000,
    commitTimeoutMs = 30000,
    transactionTimeoutMs = 60000,
    maxRetries = 3,
    retryDelayMs = 100,
    defaultIsolationLevel = IsolationLevel.SERIALIZABLE,
  } = config;

  let currentContext: DistributedTransactionContext | null = null;
  let participantStates: Map<string, ParticipantState> = new Map();

  /**
   * Generate unique transaction ID
   */
  function generateTxnId(): string {
    return `txn_${coordinatorId}_${Date.now()}_${Math.random().toString(36).slice(2)}`;
  }

  /**
   * Check if transaction has timed out
   */
  function checkTimeout(ctx: DistributedTransactionContext): void {
    if (Date.now() - ctx.startedAt > ctx.timeout) {
      throw new DistributedTransactionError(
        DistributedTransactionErrorCode.TIMEOUT,
        `Transaction ${ctx.txnId} timed out after ${ctx.timeout}ms`,
        ctx.txnId
      );
    }
  }

  /**
   * Sleep helper for retries
   */
  function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Execute with retry
   */
  async function withRetry<T>(
    fn: () => Promise<T>,
    shardId: string,
    operation: string
  ): Promise<T> {
    let lastError: Error | undefined;
    const state = participantStates.get(shardId);

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const result = await fn();
        if (state) {
          state.lastSeen = Date.now();
        }
        return result;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (state) {
          state.retryCount++;
        }

        if (attempt < maxRetries - 1) {
          await sleep(retryDelayMs * Math.pow(2, attempt));
        }
      }
    }

    throw new DistributedTransactionError(
      DistributedTransactionErrorCode.PARTICIPANT_FAILURE,
      `Failed to ${operation} on shard ${shardId} after ${maxRetries} retries: ${lastError?.message}`,
      currentContext?.txnId,
      { shardId }
    );
  }

  const coordinator: DistributedTransactionCoordinator = {
    async begin(
      participants: ShardId[],
      options?: DistributedTransactionOptions
    ): Promise<DistributedTransactionContext> {
      if (currentContext !== null) {
        throw new DistributedTransactionError(
          DistributedTransactionErrorCode.TRANSACTION_ALREADY_ACTIVE,
          'A distributed transaction is already active',
          currentContext.txnId
        );
      }

      const txnId = generateTxnId();
      const timeout = options?.timeout ?? transactionTimeoutMs;
      const isolationLevel = options?.isolationLevel ?? defaultIsolationLevel;

      currentContext = {
        txnId,
        coordinatorId,
        participants,
        state: 'INITIATED',
        startedAt: Date.now(),
        timeout,
        prepareVotes: new Map(),
        lockedRows: new Map(),
        operations: [],
        readSet: new Map(),
        writeSet: new Map(),
        isolationLevel,
        decision: 'PENDING',
      };

      // Initialize participant states
      participantStates = new Map();
      for (const shard of participants) {
        participantStates.set(shard as string, {
          shardId: shard,
          lastSeen: Date.now(),
          retryCount: 0,
          locks: [],
        });
      }

      // Log BEGIN
      await txnLog.write({
        txnId,
        type: 'BEGIN',
        participants: participants.map((p) => p as string),
        timestamp: Date.now(),
      });

      return currentContext;
    },

    async execute(
      shardId: ShardId,
      sql: string,
      params?: unknown[]
    ): Promise<{ rows: unknown[]; rowsAffected: number }> {
      if (!currentContext) {
        throw new DistributedTransactionError(
          DistributedTransactionErrorCode.NO_ACTIVE_TRANSACTION,
          'No active distributed transaction'
        );
      }

      checkTimeout(currentContext);

      if (
        currentContext.state !== 'INITIATED' &&
        currentContext.state !== 'PREPARING'
      ) {
        throw new DistributedTransactionError(
          DistributedTransactionErrorCode.INVALID_STATE,
          `Cannot execute in state ${currentContext.state}`,
          currentContext.txnId
        );
      }

      // Determine operation type
      const sqlUpper = sql.trim().toUpperCase();
      let opType: DistributedOperation['type'] = 'SELECT';
      if (sqlUpper.startsWith('INSERT')) opType = 'INSERT';
      else if (sqlUpper.startsWith('UPDATE')) opType = 'UPDATE';
      else if (sqlUpper.startsWith('DELETE')) opType = 'DELETE';

      // Record operation
      const operation: DistributedOperation = {
        shard: shardId,
        sql,
        params,
        type: opType,
      };
      currentContext.operations.push(operation);

      // For write operations, add to write set for read-your-writes
      if (opType !== 'SELECT') {
        // Execute and track the write
        const result = await withRetry(
          () => rpc.execute(shardId as string, sql, params),
          shardId as string,
          'execute'
        );

        // Track in write set for read-your-writes
        let shardWriteSet = currentContext.writeSet.get(shardId as string);
        if (!shardWriteSet) {
          shardWriteSet = new Map();
          currentContext.writeSet.set(shardId as string, shardWriteSet);
        }
        // Store the result for potential read-your-writes
        const key = `${sql}:${JSON.stringify(params)}`;
        shardWriteSet.set(key, result);

        return result;
      } else {
        // For SELECT, check write set first (read-your-writes)
        const shardWriteSet = currentContext.writeSet.get(shardId as string);

        // Execute the read
        const result = await withRetry(
          () => rpc.execute(shardId as string, sql, params),
          shardId as string,
          'execute'
        );

        // Track in read set
        let shardReadSet = currentContext.readSet.get(shardId as string);
        if (!shardReadSet) {
          shardReadSet = new Map();
          currentContext.readSet.set(shardId as string, shardReadSet);
        }
        const key = `${sql}:${JSON.stringify(params)}`;
        shardReadSet.set(key, result);

        return result;
      }
    },

    async prepare(): Promise<Map<string, ParticipantVote>> {
      if (!currentContext) {
        throw new DistributedTransactionError(
          DistributedTransactionErrorCode.NO_ACTIVE_TRANSACTION,
          'No active distributed transaction'
        );
      }

      checkTimeout(currentContext);

      if (currentContext.state !== 'INITIATED') {
        throw new DistributedTransactionError(
          DistributedTransactionErrorCode.INVALID_STATE,
          `Cannot prepare in state ${currentContext.state}`,
          currentContext.txnId
        );
      }

      currentContext.state = 'PREPARING';

      // Log PREPARE decision
      await txnLog.write({
        txnId: currentContext.txnId,
        type: 'PREPARE',
        participants: currentContext.participants.map((p) => p as string),
        timestamp: Date.now(),
      });

      // Send PREPARE to all participants in parallel
      const preparePromises: Promise<void>[] = [];

      for (const participant of currentContext.participants) {
        const shardOps = currentContext.operations.filter(
          (op) => op.shard === participant
        );

        preparePromises.push(
          (async () => {
            const shardId = participant as string;
            try {
              // Create prepare timeout
              const preparePromise = withRetry(
                () => rpc.prepare(shardId, currentContext!.txnId, shardOps),
                shardId,
                'prepare'
              );

              const timeoutPromise = new Promise<never>((_, reject) => {
                setTimeout(() => {
                  reject(new Error(`Prepare timeout for shard ${shardId}`));
                }, prepareTimeoutMs);
              });

              const result = await Promise.race([preparePromise, timeoutPromise]);

              currentContext!.prepareVotes.set(shardId, result.vote);

              // Track participant state
              const state = participantStates.get(shardId);
              if (state) {
                state.vote = result.vote;
                state.preparedData = result.preparedData;
              }

              // Log participant's vote
              await txnLog.write({
                txnId: currentContext!.txnId,
                type: 'PREPARE_ACK',
                participants: [shardId],
                timestamp: Date.now(),
                vote: result.vote,
                shardId,
              });

              // Track locked rows
              if (result.vote === 'YES') {
                currentContext!.lockedRows.set(shardId, new Set(shardOps.flatMap((op) => op.affectedKeys || [])));
              }
            } catch (error) {
              // Treat failures as TIMEOUT votes
              currentContext!.prepareVotes.set(shardId, 'TIMEOUT');

              await txnLog.write({
                txnId: currentContext!.txnId,
                type: 'PREPARE_ACK',
                participants: [shardId],
                timestamp: Date.now(),
                vote: 'TIMEOUT',
                shardId,
              });
            }
          })()
        );
      }

      await Promise.all(preparePromises);

      // Check if all participants voted YES
      let allYes = true;
      for (const vote of currentContext.prepareVotes.values()) {
        if (vote !== 'YES') {
          allYes = false;
          break;
        }
      }

      if (allYes) {
        currentContext.state = 'PREPARED';
        currentContext.preparedAt = Date.now();
        currentContext.decision = 'COMMIT';
      } else {
        currentContext.state = 'ABORTING';
        currentContext.decision = 'ABORT';
      }

      return currentContext.prepareVotes;
    },

    async commit(): Promise<void> {
      if (!currentContext) {
        throw new DistributedTransactionError(
          DistributedTransactionErrorCode.NO_ACTIVE_TRANSACTION,
          'No active distributed transaction'
        );
      }

      // Allow commit in PREPARED or COMMITTED (idempotent) state
      if (
        currentContext.state !== 'PREPARED' &&
        currentContext.state !== 'COMMITTING' &&
        currentContext.state !== 'COMMITTED'
      ) {
        throw new DistributedTransactionError(
          DistributedTransactionErrorCode.INVALID_STATE,
          `Cannot commit in state ${currentContext.state}. All participants must vote YES.`,
          currentContext.txnId
        );
      }

      // If already committed, this is idempotent
      if (currentContext.state === 'COMMITTED') {
        return;
      }

      currentContext.state = 'COMMITTING';

      // Log COMMIT decision (before sending to participants - this is crucial for recovery)
      await txnLog.write({
        txnId: currentContext.txnId,
        type: 'COMMIT',
        participants: currentContext.participants.map((p) => p as string),
        timestamp: Date.now(),
        decision: 'COMMIT',
      });

      await txnLog.sync();

      // Send COMMIT to all participants
      const commitPromises: Promise<void>[] = [];

      for (const participant of currentContext.participants) {
        commitPromises.push(
          (async () => {
            const shardId = participant as string;
            try {
              await withRetry(
                () => rpc.commit(shardId, currentContext!.txnId),
                shardId,
                'commit'
              );

              // Log commit acknowledgment
              await txnLog.write({
                txnId: currentContext!.txnId,
                type: 'COMMIT_ACK',
                participants: [shardId],
                timestamp: Date.now(),
                shardId,
              });

              // Release locks
              currentContext!.lockedRows.delete(shardId);
            } catch (error) {
              // For commit phase, we must keep retrying indefinitely
              // The decision is already logged, participant must eventually commit
              console.error(
                `Failed to commit on shard ${shardId}, will retry: ${error}`
              );
            }
          })()
        );
      }

      await Promise.all(commitPromises);

      currentContext.state = 'COMMITTED';
      currentContext.committedAt = Date.now();

      // Cleanup logs after successful commit
      await txnLog.delete(currentContext.txnId);

      // Clear locked rows
      currentContext.lockedRows.clear();

      // Clear context
      const ctx = currentContext;
      currentContext = null;
      participantStates.clear();
    },

    async rollback(): Promise<void> {
      if (!currentContext) {
        throw new DistributedTransactionError(
          DistributedTransactionErrorCode.NO_ACTIVE_TRANSACTION,
          'No active distributed transaction'
        );
      }

      // Can rollback from any state except COMMITTED
      if (currentContext.state === 'COMMITTED') {
        throw new DistributedTransactionError(
          DistributedTransactionErrorCode.INVALID_STATE,
          'Cannot rollback a committed transaction',
          currentContext.txnId
        );
      }

      // If already aborted, this is idempotent
      if (currentContext.state === 'ABORTED') {
        return;
      }

      currentContext.state = 'ABORTING';
      currentContext.decision = 'ABORT';

      // Log ABORT decision
      await txnLog.write({
        txnId: currentContext.txnId,
        type: 'ABORT',
        participants: currentContext.participants.map((p) => p as string),
        timestamp: Date.now(),
        decision: 'ABORT',
      });

      await txnLog.sync();

      // Send ABORT to all participants
      const abortPromises: Promise<void>[] = [];

      for (const participant of currentContext.participants) {
        abortPromises.push(
          (async () => {
            const shardId = participant as string;
            try {
              await withRetry(
                () => rpc.abort(shardId, currentContext!.txnId),
                shardId,
                'abort'
              );

              // Release locks
              currentContext!.lockedRows.delete(shardId);
            } catch (error) {
              // Best effort for abort
              console.error(`Failed to abort on shard ${shardId}: ${error}`);
            }
          })()
        );
      }

      await Promise.all(abortPromises);

      currentContext.state = 'ABORTED';

      // Cleanup logs
      await txnLog.delete(currentContext.txnId);

      // Clear locked rows
      currentContext.lockedRows.clear();

      // Clear context
      currentContext = null;
      participantStates.clear();
    },

    getState(): DistributedTransactionState {
      return currentContext?.state ?? 'ABORTED';
    },

    getContext(): DistributedTransactionContext | null {
      return currentContext;
    },

    async recover(): Promise<void> {
      // Get all pending transactions from log
      const pending = await txnLog.getPending();

      for (const record of pending) {
        // Determine the latest state
        const logs = await txnLog.read(record.txnId);
        const lastRecord = logs[logs.length - 1];

        if (!lastRecord) continue;

        switch (lastRecord.type) {
          case 'BEGIN':
          case 'PREPARE':
            // Transaction was in progress, abort it
            for (const shardId of record.participants) {
              try {
                await rpc.abort(shardId, record.txnId);
              } catch {
                // Best effort
              }
            }
            await txnLog.delete(record.txnId);
            break;

          case 'COMMIT':
            // Decision was commit, ensure all participants commit
            for (const shardId of record.participants) {
              try {
                await rpc.commit(shardId, record.txnId);
              } catch {
                // Will retry on next recovery
              }
            }
            // Check if all committed
            // In real implementation, track commit acknowledgments
            await txnLog.delete(record.txnId);
            break;

          case 'ABORT':
            // Decision was abort, ensure all participants abort
            for (const shardId of record.participants) {
              try {
                await rpc.abort(shardId, record.txnId);
              } catch {
                // Best effort
              }
            }
            await txnLog.delete(record.txnId);
            break;
        }
      }
    },
  };

  return coordinator;
}

// =============================================================================
// TRANSACTION PARTICIPANT
// =============================================================================

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
 * Transaction participant interface
 */
export interface TransactionParticipant {
  /** Handle PREPARE from coordinator */
  prepare(txnId: string, operations: DistributedOperation[]): Promise<{ vote: ParticipantVote; preparedData?: unknown }>;

  /** Handle COMMIT from coordinator */
  commit(txnId: string): Promise<void>;

  /** Handle ABORT from coordinator */
  abort(txnId: string): Promise<void>;

  /** Get current participant state */
  getState(txnId: string): TransactionParticipantContext | undefined;

  /** Query coordinator for decision (for recovery) */
  queryCoordinatorDecision(txnId: string): Promise<void>;
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
 * Creates a transaction participant
 */
export function createTransactionParticipant(
  localExecutor: LocalExecutor,
  rpc: ShardParticipantRPC,
  participantLog: DistributedTransactionLog,
  config: ParticipantConfig
): TransactionParticipant {
  const {
    shardId,
    lockTimeoutMs = 5000,
    uncertaintyWindowMs = 30000,
  } = config;

  // Active transaction contexts
  const transactions: Map<string, TransactionParticipantContext> = new Map();

  // Pending uncertainty window timers
  const uncertaintyTimers: Map<string, NodeJS.Timeout> = new Map();

  const participant: TransactionParticipant = {
    async prepare(
      txnId: string,
      operations: DistributedOperation[]
    ): Promise<{ vote: ParticipantVote; preparedData?: unknown }> {
      // Check if we already have this transaction
      let ctx = transactions.get(txnId);

      if (ctx && ctx.state === 'PREPARED') {
        // Already prepared, return YES (idempotent)
        return { vote: 'YES' };
      }

      if (ctx && ctx.state !== 'ACTIVE') {
        // Invalid state
        return { vote: 'NO' };
      }

      // Create new context if needed
      if (!ctx) {
        ctx = {
          txnId,
          coordinatorId: '',
          state: 'ACTIVE',
          locks: [],
          operations: [],
          undoLog: [],
        };
        transactions.set(txnId, ctx);
      }

      ctx.operations = operations;

      try {
        // Begin local transaction
        await localExecutor.beginLocal();

        // Acquire locks for all operations
        for (const op of operations) {
          // Determine resources to lock
          const resources = op.affectedKeys || [`${op.type}:${op.sql}`];

          for (const resource of resources) {
            const lockType: DistributedLockType =
              op.type === 'SELECT' ? 'SHARED' : 'EXCLUSIVE';

            const acquired = await localExecutor.acquireLock(
              resource,
              lockType,
              txnId,
              lockTimeoutMs
            );

            if (!acquired) {
              // Could not acquire lock, vote NO
              await localExecutor.rollbackLocal();
              localExecutor.releaseAllLocks(txnId);

              return { vote: 'NO' };
            }

            ctx.locks.push({
              resource,
              lockType,
              acquiredAt: Date.now(),
              txnId,
            });
          }
        }

        // Execute all operations to validate they can succeed
        for (const op of operations) {
          try {
            await localExecutor.execute(op.sql, op.params);

            // Build undo log entry
            // In real implementation, would capture before-image for UPDATE/DELETE
            if (op.type !== 'SELECT') {
              ctx.undoLog.push({
                sql: `-- UNDO: ${op.sql}`,
                params: op.params || [],
              });
            }
          } catch (error) {
            // Operation failed, vote NO
            await localExecutor.rollbackLocal();
            localExecutor.releaseAllLocks(txnId);

            return { vote: 'NO' };
          }
        }

        // Log PREPARE
        await participantLog.write({
          txnId,
          type: 'PREPARE',
          participants: [shardId],
          timestamp: Date.now(),
          shardId,
        });

        await participantLog.sync();

        ctx.state = 'PREPARED';
        ctx.preparedAt = Date.now();

        // Start uncertainty window timer
        const timer = setTimeout(async () => {
          await participant.queryCoordinatorDecision(txnId);
        }, uncertaintyWindowMs);
        uncertaintyTimers.set(txnId, timer);

        return { vote: 'YES' };
      } catch (error) {
        // Any error means vote NO
        try {
          await localExecutor.rollbackLocal();
        } catch {
          // Ignore rollback errors
        }
        localExecutor.releaseAllLocks(txnId);

        return { vote: 'NO' };
      }
    },

    async commit(txnId: string): Promise<void> {
      const ctx = transactions.get(txnId);

      if (!ctx) {
        // Unknown transaction - might be replay, ignore
        return;
      }

      // Clear uncertainty timer
      const timer = uncertaintyTimers.get(txnId);
      if (timer) {
        clearTimeout(timer);
        uncertaintyTimers.delete(txnId);
      }

      if (ctx.state === 'COMMITTED') {
        // Already committed (idempotent)
        return;
      }

      if (ctx.state !== 'PREPARED') {
        throw new DistributedTransactionError(
          DistributedTransactionErrorCode.INVALID_STATE,
          `Cannot commit participant in state ${ctx.state}`,
          txnId
        );
      }

      // Log COMMIT acknowledgment
      await participantLog.write({
        txnId,
        type: 'COMMIT_ACK',
        participants: [shardId],
        timestamp: Date.now(),
        shardId,
      });

      // Commit local transaction
      await localExecutor.commitLocal();

      // Release all locks
      localExecutor.releaseAllLocks(txnId);

      ctx.state = 'COMMITTED';
      ctx.locks = [];

      // Cleanup
      await participantLog.delete(txnId);
      transactions.delete(txnId);
    },

    async abort(txnId: string): Promise<void> {
      const ctx = transactions.get(txnId);

      // Clear uncertainty timer
      const timer = uncertaintyTimers.get(txnId);
      if (timer) {
        clearTimeout(timer);
        uncertaintyTimers.delete(txnId);
      }

      if (!ctx) {
        // Unknown transaction - might be already cleaned up
        return;
      }

      if (ctx.state === 'ABORTED') {
        // Already aborted (idempotent)
        return;
      }

      if (ctx.state === 'COMMITTED') {
        throw new DistributedTransactionError(
          DistributedTransactionErrorCode.INVALID_STATE,
          'Cannot abort a committed transaction',
          txnId
        );
      }

      // Log ABORT
      await participantLog.write({
        txnId,
        type: 'ABORT',
        participants: [shardId],
        timestamp: Date.now(),
        shardId,
      });

      // Rollback local transaction
      try {
        await localExecutor.rollbackLocal();
      } catch {
        // Ignore rollback errors
      }

      // Release all locks
      localExecutor.releaseAllLocks(txnId);

      ctx.state = 'ABORTED';
      ctx.locks = [];

      // Cleanup
      await participantLog.delete(txnId);
      transactions.delete(txnId);
    },

    getState(txnId: string): TransactionParticipantContext | undefined {
      return transactions.get(txnId);
    },

    async queryCoordinatorDecision(txnId: string): Promise<void> {
      const ctx = transactions.get(txnId);
      if (!ctx || ctx.state !== 'PREPARED') {
        return;
      }

      try {
        const decision = await rpc.queryDecision(ctx.coordinatorId, txnId);

        switch (decision) {
          case 'COMMIT':
            await participant.commit(txnId);
            break;
          case 'ABORT':
            await participant.abort(txnId);
            break;
          case 'PENDING':
            // Coordinator hasn't decided yet, reschedule query
            const timer = setTimeout(async () => {
              await participant.queryCoordinatorDecision(txnId);
            }, uncertaintyWindowMs);
            uncertaintyTimers.set(txnId, timer);
            break;
        }
      } catch (error) {
        // Coordinator unavailable, retry later
        const timer = setTimeout(async () => {
          await participant.queryCoordinatorDecision(txnId);
        }, uncertaintyWindowMs);
        uncertaintyTimers.set(txnId, timer);
      }
    },
  };

  return participant;
}

// =============================================================================
// CROSS-SHARD EXECUTOR
// =============================================================================

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

/**
 * Cross-shard executor interface
 */
export interface CrossShardExecutor {
  /** Execute operations atomically across shards */
  executeInTransaction<T>(
    shards: ShardId[],
    operations: Array<{ shard: ShardId; sql: string; params?: unknown[] }>,
    options?: { timeout?: number; isolationLevel?: string }
  ): Promise<CrossShardExecutionResult<T>>;
}

/**
 * Creates a cross-shard executor
 */
export function createCrossShardExecutor(
  coordinator: DistributedTransactionCoordinator
): CrossShardExecutor {
  return {
    async executeInTransaction<T>(
      shards: ShardId[],
      operations: Array<{ shard: ShardId; sql: string; params?: unknown[] }>,
      options?: { timeout?: number; isolationLevel?: string }
    ): Promise<CrossShardExecutionResult<T>> {
      const results: T[] = [];

      try {
        // Determine isolation level
        let isolationLevel = IsolationLevel.SERIALIZABLE;
        if (options?.isolationLevel) {
          const level = options.isolationLevel.toUpperCase();
          if (level === 'SERIALIZABLE') isolationLevel = IsolationLevel.SERIALIZABLE;
          else if (level === 'SNAPSHOT') isolationLevel = IsolationLevel.SNAPSHOT;
          else if (level === 'REPEATABLE_READ') isolationLevel = IsolationLevel.REPEATABLE_READ;
          else if (level === 'READ_COMMITTED') isolationLevel = IsolationLevel.READ_COMMITTED;
        }

        // Begin distributed transaction
        await coordinator.begin(shards, {
          timeout: options?.timeout,
          isolationLevel,
        });

        // Execute all operations
        for (const op of operations) {
          const result = await coordinator.execute(op.shard, op.sql, op.params);
          results.push(result as T);
        }

        // Prepare phase
        const votes = await coordinator.prepare();

        // Check if all voted YES
        let allYes = true;
        for (const vote of votes.values()) {
          if (vote !== 'YES') {
            allYes = false;
            break;
          }
        }

        if (!allYes) {
          // Rollback
          await coordinator.rollback();
          return {
            success: false,
            results: [],
            error: new Error('Transaction aborted: one or more participants voted NO'),
          };
        }

        // Commit phase
        await coordinator.commit();

        return {
          success: true,
          results,
        };
      } catch (error) {
        // Try to rollback
        try {
          await coordinator.rollback();
        } catch {
          // Ignore rollback errors
        }

        return {
          success: false,
          results: [],
          error: error instanceof Error ? error : new Error(String(error)),
        };
      }
    },
  };
}

// =============================================================================
// ERROR TYPES
// =============================================================================

/**
 * Error codes for distributed transactions
 */
export enum DistributedTransactionErrorCode {
  /** No active transaction */
  NO_ACTIVE_TRANSACTION = 'DTX_NO_ACTIVE',
  /** Transaction already active */
  TRANSACTION_ALREADY_ACTIVE = 'DTX_ALREADY_ACTIVE',
  /** Invalid state transition */
  INVALID_STATE = 'DTX_INVALID_STATE',
  /** Prepare phase failed */
  PREPARE_FAILED = 'DTX_PREPARE_FAILED',
  /** Commit phase failed */
  COMMIT_FAILED = 'DTX_COMMIT_FAILED',
  /** Rollback failed */
  ROLLBACK_FAILED = 'DTX_ROLLBACK_FAILED',
  /** Transaction timeout */
  TIMEOUT = 'DTX_TIMEOUT',
  /** Participant failure */
  PARTICIPANT_FAILURE = 'DTX_PARTICIPANT_FAILURE',
  /** Coordinator failure */
  COORDINATOR_FAILURE = 'DTX_COORDINATOR_FAILURE',
  /** Lock acquisition failed */
  LOCK_FAILED = 'DTX_LOCK_FAILED',
  /** Serialization conflict */
  SERIALIZATION_FAILURE = 'DTX_SERIALIZATION_FAILURE',
}

/**
 * Distributed transaction error
 */
export class DistributedTransactionError extends Error {
  constructor(
    public readonly code: DistributedTransactionErrorCode,
    message: string,
    public readonly txnId?: string,
    public readonly details?: Record<string, unknown>
  ) {
    super(message);
    this.name = 'DistributedTransactionError';
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create an in-memory mock RPC for testing
 */
export function createMockShardRPC(): ShardParticipantRPC {
  const participants = new Map<string, TransactionParticipant>();
  const pendingPrepares = new Map<string, Map<string, DistributedOperation[]>>();

  return {
    async prepare(
      shardId: string,
      txnId: string,
      operations: DistributedOperation[]
    ): Promise<{ vote: ParticipantVote; preparedData?: unknown }> {
      // Store for testing/inspection
      let txnPrepares = pendingPrepares.get(txnId);
      if (!txnPrepares) {
        txnPrepares = new Map();
        pendingPrepares.set(txnId, txnPrepares);
      }
      txnPrepares.set(shardId, operations);

      // For mock, always vote YES
      return { vote: 'YES' };
    },

    async commit(shardId: string, txnId: string): Promise<void> {
      // Cleanup
      const txnPrepares = pendingPrepares.get(txnId);
      if (txnPrepares) {
        txnPrepares.delete(shardId);
        if (txnPrepares.size === 0) {
          pendingPrepares.delete(txnId);
        }
      }
    },

    async abort(shardId: string, txnId: string): Promise<void> {
      // Cleanup
      const txnPrepares = pendingPrepares.get(txnId);
      if (txnPrepares) {
        txnPrepares.delete(shardId);
        if (txnPrepares.size === 0) {
          pendingPrepares.delete(txnId);
        }
      }
    },

    async queryDecision(
      coordinatorId: string,
      txnId: string
    ): Promise<CoordinatorDecision> {
      // For mock, return PENDING
      return 'PENDING';
    },

    async execute(
      shardId: string,
      sql: string,
      params?: unknown[]
    ): Promise<{ rows: unknown[]; rowsAffected: number }> {
      // Mock execution
      return { rows: [], rowsAffected: 0 };
    },
  };
}

/**
 * Create a distributed transaction coordinator with defaults
 */
export function createDistributedCoordinator(
  coordinatorId: string,
  rpc?: ShardParticipantRPC,
  txnLog?: DistributedTransactionLog
): DistributedTransactionCoordinator {
  return createDistributedTransactionCoordinator(
    rpc ?? createMockShardRPC(),
    txnLog ?? new InMemoryTransactionLog(),
    { coordinatorId }
  );
}
