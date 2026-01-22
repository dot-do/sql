/**
 * DoSQL Transaction Participant
 *
 * Implements the participant role in the Two-Phase Commit (2PC) protocol.
 * Each shard acts as a participant that responds to coordinator requests.
 *
 * @packageDocumentation
 */

import { DistributedTransactionError, DistributedTransactionErrorCode } from './errors.js';
import type {
  ParticipantVote,
  DistributedLockType,
  DistributedOperation,
  TransactionParticipantContext,
  ShardParticipantRPC,
  DistributedTransactionLog,
  ParticipantConfig,
  LocalExecutor,
} from './types.js';

// =============================================================================
// TRANSACTION PARTICIPANT INTERFACE
// =============================================================================

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

// =============================================================================
// PARTICIPANT IMPLEMENTATION
// =============================================================================

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
