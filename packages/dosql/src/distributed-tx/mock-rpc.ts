/**
 * DoSQL Mock Shard RPC
 *
 * In-memory mock implementation of the ShardParticipantRPC interface
 * for testing distributed transactions without actual network communication.
 *
 * @packageDocumentation
 */

import type {
  ParticipantVote,
  CoordinatorDecision,
  DistributedOperation,
  ShardParticipantRPC,
  DistributedTransactionLog,
} from './types.js';
import { InMemoryTransactionLog } from './types.js';
import type { DistributedTransactionCoordinator } from './coordinator.js';
import { createDistributedTransactionCoordinator } from './coordinator.js';
import type { TransactionParticipant } from './participant.js';

// =============================================================================
// MOCK RPC IMPLEMENTATION
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

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

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
