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

// =============================================================================
// ERROR TYPES
// =============================================================================

export {
  DistributedTransactionErrorCode,
  DistributedTransactionError,
} from './errors.js';

// =============================================================================
// SHARED TYPES
// =============================================================================

export type {
  DistributedTransactionState,
  ParticipantVote,
  CoordinatorDecision,
  DistributedLockType,
  DistributedLock,
  DistributedOperation,
  DistributedTransactionContext,
  TransactionLogRecord,
  ParticipantState,
  ShardParticipantRPC,
  DistributedTransactionLog,
  CoordinatorConfig,
  DistributedTransactionOptions,
  ParticipantConfig,
  LocalExecutor,
  TransactionParticipantContext,
  CrossShardExecutionResult,
} from './types.js';

export { InMemoryTransactionLog } from './types.js';

// =============================================================================
// COORDINATOR
// =============================================================================

export type { DistributedTransactionCoordinator } from './coordinator.js';
export { createDistributedTransactionCoordinator } from './coordinator.js';

// =============================================================================
// PARTICIPANT
// =============================================================================

export type { TransactionParticipant } from './participant.js';
export { createTransactionParticipant } from './participant.js';

// =============================================================================
// CROSS-SHARD EXECUTOR
// =============================================================================

export type { CrossShardExecutor } from './cross-shard-executor.js';
export { createCrossShardExecutor } from './cross-shard-executor.js';

// =============================================================================
// MOCK RPC FOR TESTING
// =============================================================================

export {
  createMockShardRPC,
  createDistributedCoordinator,
} from './mock-rpc.js';
