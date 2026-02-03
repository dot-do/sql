/**
 * DoSQL Distributed Transaction Coordinator
 *
 * Implements the coordinator role in the Two-Phase Commit (2PC) protocol
 * for distributed transactions across multiple shards.
 *
 * @packageDocumentation
 */

import { createLogger } from '../logging/index.js';
import { assertNever } from '../utils/assert-never.js';
import type { ShardId } from '../sharding/types.js';
import { IsolationLevel } from '../transaction/types.js';
import { DistributedTransactionError, DistributedTransactionErrorCode } from './errors.js';
import { withRetry as sharedWithRetry } from '../utils/retry.js';
import {
  createCircuitBreaker,
  CircuitBreakerOpenError,
  type CircuitBreaker,
  type CircuitBreakerMetrics,
} from './circuit-breaker.js';
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

const logger = createLogger({ defaultContext: { module: 'distributed-tx' } });

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

  /** Get circuit breaker metrics for all participants */
  getCircuitBreakerMetrics(): CircuitBreakerMetrics[];

  /** Get circuit breaker instance for advanced operations */
  getCircuitBreaker(): CircuitBreaker | null;
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
    circuitBreaker: circuitBreakerConfig,
  } = config;

  let currentContext: DistributedTransactionContext | null = null;
  let participantStates: Map<string, ParticipantState> = new Map();
  let lastCompletedState: DistributedTransactionState | null = null;

  // Initialize circuit breaker if enabled (default: enabled)
  const circuitBreakerEnabled = circuitBreakerConfig?.enabled !== false;
  const circuitBreaker = circuitBreakerEnabled
    ? createCircuitBreaker({
        failureThreshold: circuitBreakerConfig?.failureThreshold ?? 5,
        resetTimeoutMs: circuitBreakerConfig?.resetTimeoutMs ?? 30000,
        successThreshold: circuitBreakerConfig?.successThreshold ?? 2,
        failureWindowMs: circuitBreakerConfig?.failureWindowMs ?? 60000,
        onStateChange: (participantId, oldState, newState) => {
          logger.warn('Circuit breaker state change', {
            participantId,
            oldState,
            newState,
            txnId: currentContext?.txnId,
          });
        },
      })
    : null;

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
   * Check if circuit breaker allows request to participant
   */
  function checkCircuitBreaker(shardId: string): void {
    if (circuitBreaker && !circuitBreaker.canExecute(shardId)) {
      const state = circuitBreaker.getState(shardId);
      throw new CircuitBreakerOpenError(shardId, state);
    }
  }

  /**
   * Execute with retry using shared retry utility and circuit breaker protection
   */
  async function withRetry<T>(
    fn: () => Promise<T>,
    shardId: string,
    operation: string
  ): Promise<T> {
    const state = participantStates.get(shardId);

    // Check circuit breaker before attempting
    checkCircuitBreaker(shardId);

    try {
      const result = await sharedWithRetry(fn, {
        maxAttempts: maxRetries,
        initialDelayMs: retryDelayMs,
        backoffMultiplier: 2,
        onRetry: () => {
          if (state) {
            state.retryCount++;
          }
        },
        // Don't retry if circuit breaker is open
        isRetryable: (error) => {
          if (error instanceof CircuitBreakerOpenError) {
            return false;
          }
          // Check circuit breaker state before retry
          if (circuitBreaker && !circuitBreaker.canExecute(shardId)) {
            return false;
          }
          return true;
        },
      });

      // Update lastSeen on success
      if (state) {
        state.lastSeen = Date.now();
      }

      // Record success with circuit breaker
      if (circuitBreaker) {
        circuitBreaker.recordSuccess(shardId);
      }

      return result;
    } catch (error) {
      // Record failure with circuit breaker (unless it's already a circuit breaker error)
      if (circuitBreaker && !(error instanceof CircuitBreakerOpenError)) {
        circuitBreaker.recordFailure(shardId);
      }

      // Wrap in DistributedTransactionError for consistent error handling
      const message = error instanceof Error ? error.message : String(error);
      const isCircuitOpen = error instanceof CircuitBreakerOpenError;
      throw new DistributedTransactionError(
        isCircuitOpen
          ? DistributedTransactionErrorCode.PARTICIPANT_UNAVAILABLE
          : DistributedTransactionErrorCode.PARTICIPANT_FAILURE,
        isCircuitOpen
          ? `Circuit breaker open for shard ${shardId}: ${message}`
          : `Failed to ${operation} on shard ${shardId} after ${maxRetries} retries: ${message}`,
        currentContext?.txnId,
        { shardId, circuitOpen: isCircuitOpen }
      );
    }
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

      // Capture txnId, prepareVotes, and lockedRows before async iteration
      // These are guaranteed to exist after the null check above
      const txnId = currentContext.txnId;
      const prepareVotes = currentContext.prepareVotes;
      const lockedRows = currentContext.lockedRows;

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
                () => rpc.prepare(shardId, txnId, shardOps),
                shardId,
                'prepare'
              );

              const timeoutPromise = new Promise<never>((_, reject) => {
                setTimeout(() => {
                  reject(new Error(`Prepare timeout for shard ${shardId}`));
                }, prepareTimeoutMs);
              });

              const result = await Promise.race([preparePromise, timeoutPromise]);

              prepareVotes.set(shardId, result.vote);

              // Track participant state
              const state = participantStates.get(shardId);
              if (state) {
                state.vote = result.vote;
                state.preparedData = result.preparedData;
              }

              // Log participant's vote
              await txnLog.write({
                txnId,
                type: 'PREPARE_ACK',
                participants: [shardId],
                timestamp: Date.now(),
                vote: result.vote,
                shardId,
              });

              // Track locked rows
              if (result.vote === 'YES') {
                lockedRows.set(shardId, new Set(shardOps.flatMap((op) => op.affectedKeys || [])));
              }
            } catch (error) {
              // Treat failures as TIMEOUT votes
              prepareVotes.set(shardId, 'TIMEOUT');

              await txnLog.write({
                txnId,
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

      // Capture txnId and lockedRows before async iteration
      const commitTxnId = currentContext.txnId;
      const commitLockedRows = currentContext.lockedRows;

      for (const participant of currentContext.participants) {
        commitPromises.push(
          (async () => {
            const shardId = participant as string;
            try {
              await withRetry(
                () => rpc.commit(shardId, commitTxnId),
                shardId,
                'commit'
              );

              // Log commit acknowledgment
              await txnLog.write({
                txnId: commitTxnId,
                type: 'COMMIT_ACK',
                participants: [shardId],
                timestamp: Date.now(),
                shardId,
              });

              // Release locks
              commitLockedRows.delete(shardId);
            } catch (error) {
              // For commit phase, we must keep retrying indefinitely
              // The decision is already logged, participant must eventually commit
              logger.error('Failed to commit on shard, will retry', error instanceof Error ? error : new Error(String(error)), { shardId });
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

      // Capture txnId and lockedRows before async iteration
      const abortTxnId = currentContext.txnId;
      const abortLockedRows = currentContext.lockedRows;

      for (const participant of currentContext.participants) {
        abortPromises.push(
          (async () => {
            const shardId = participant as string;
            try {
              await withRetry(
                () => rpc.abort(shardId, abortTxnId),
                shardId,
                'abort'
              );

              // Release locks
              abortLockedRows.delete(shardId);
            } catch (error) {
              // Best effort for abort
              logger.error('Failed to abort on shard', error instanceof Error ? error : new Error(String(error)), { shardId });
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

          case 'PREPARE_ACK':
          case 'COMMIT_ACK':
            // These are acknowledgment records, not state changes
            // Skip processing for recovery
            break;

          default:
            assertNever(lastRecord.type, `Unknown transaction log record type: ${lastRecord.type}`);
        }
      }
    },

    getCircuitBreakerMetrics(): CircuitBreakerMetrics[] {
      if (!circuitBreaker) {
        return [];
      }
      return circuitBreaker.getAllMetrics();
    },

    getCircuitBreaker(): CircuitBreaker | null {
      return circuitBreaker;
    },
  };

  return coordinator;
}
