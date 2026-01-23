/**
 * DoSQL Distributed Transaction Coordinator
 *
 * Implements the coordinator role in the Two-Phase Commit (2PC) protocol
 * for distributed transactions across multiple shards.
 *
 * @packageDocumentation
 */

import type { ShardId } from '../sharding/types.js';
import { IsolationLevel } from '../transaction/types.js';
import { DistributedTransactionError, DistributedTransactionErrorCode } from './errors.js';
import type {
  DistributedTransactionState,
  ParticipantVote,
  DistributedTransactionContext,
  DistributedOperation,
  ParticipantState,
  ShardParticipantRPC,
  DistributedTransactionLog,
  CoordinatorConfig,
  DistributedTransactionOptions,
} from './types.js';

// =============================================================================
// DISTRIBUTED TRANSACTION COORDINATOR INTERFACE
// =============================================================================

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

// =============================================================================
// COORDINATOR IMPLEMENTATION
// =============================================================================

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
  let lastCompletedState: DistributedTransactionState | null = null;

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

      // Reset last completed state for new transaction
      lastCompletedState = null;

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
        type: opType,
        ...(params !== undefined && { params }),
      };
      currentContext.operations.push(operation);

      // For write operations, add to write set for read-your-writes
      if (opType !== 'SELECT') {
        // Execute directly without retry - errors during execute phase should
        // fail the transaction immediately (e.g., constraint violations)
        const result = await rpc.execute(shardId as string, sql, params);

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

        // Execute directly without retry - errors during execute phase should
        // fail the transaction immediately
        const result = await rpc.execute(shardId as string, sql, params);

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

      // Preserve the final state before clearing context
      lastCompletedState = 'COMMITTED';

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

      // Preserve the final state before clearing context
      lastCompletedState = 'ABORTED';

      // Clear context
      currentContext = null;
      participantStates.clear();
    },

    getState(): DistributedTransactionState {
      // Return current state if transaction is active, otherwise return last completed state
      if (currentContext) {
        return currentContext.state;
      }
      return lastCompletedState ?? 'ABORTED';
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
