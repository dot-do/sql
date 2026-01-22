/**
 * Multi-Region Replication Module for DoSQL
 *
 * Provides:
 * - Primary DO for handling writes and coordinating replication
 * - Replica DO for handling reads and streaming WAL from primary
 * - Router for intelligent read/write routing based on consistency levels
 * - Session management for read-your-writes consistency
 *
 * @example
 * ```typescript
 * import {
 *   createPrimary,
 *   createReplica,
 *   createExtendedRouter,
 *   SessionManager,
 * } from '@dotdo/dosql/replication';
 *
 * // Create primary
 * const primary = createPrimary({ backend, walWriter, walReader });
 *
 * // Register replicas
 * await primary.registerReplica({
 *   id: { region: 'us-west', instanceId: 'replica-1' },
 *   status: 'syncing',
 *   role: 'replica',
 *   lastLSN: 0n,
 *   lastHeartbeat: Date.now(),
 *   doUrl: 'https://replica.do',
 * });
 *
 * // Create router for routing decisions
 * const router = createExtendedRouter({ region: 'us-east', instanceId: 'primary-1' });
 *
 * // Route reads based on consistency
 * const decision = await router.routeRead('SELECT * FROM users', 'eventual');
 * console.log(decision.target); // Routes to nearest healthy replica
 *
 * // Use session manager for read-your-writes
 * const sessionManager = new SessionManager();
 * const session = sessionManager.createSession('us-west');
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// TYPES
// =============================================================================

export {
  // Replica identity types
  type ReplicaId,
  type ReplicaStatus,
  type ReplicaRole,
  type ReplicaInfo,

  // WAL streaming types
  type StreamPosition,
  type WALBatch,
  type WALAck,
  type WALApplyError,
  WALApplyErrorCode,

  // Snapshot types
  type SnapshotInfo,
  type SnapshotChunk,
  type SnapshotRequest,

  // Routing types
  type ConsistencyLevel,
  type ReadPreference,
  type RoutingDecision,
  type SessionState,

  // Health monitoring types
  type ReplicationLag,
  type ReplicationHealth,

  // Failover types
  type FailoverReason,
  type FailoverState,
  type FailoverDecision,

  // Conflict resolution types
  type ConflictType,
  type Conflict,
  type ConflictResolutionStrategy,
  type ConflictResolution,

  // Interface types
  type PrimaryDO,
  type ReplicaDO,
  type ReplicationRouter,
  type RouterMetrics,

  // Configuration types
  type ReplicationConfig,
  DEFAULT_REPLICATION_CONFIG,

  // Error types
  ReplicationError,
  ReplicationErrorCode,

  // Utility functions
  serializeReplicaId,
  deserializeReplicaId,
  replicaIdsEqual,
} from './types.js';

// =============================================================================
// PRIMARY
// =============================================================================

export {
  createPrimaryDO,
  createPrimary,
  type CreatePrimaryOptions,
} from './primary.js';

// =============================================================================
// REPLICA
// =============================================================================

export {
  createReplicaDO,
  createReplica,
  type CreateReplicaOptions,
} from './replica.js';

// =============================================================================
// ROUTER
// =============================================================================

export {
  createReplicationRouter,
  createExtendedRouter,
  createLoadBalancedRouter,
  createRouterWithSessions,
  SessionManager,
  type ExtendedReplicationRouter,
  type LoadBalancingStrategy,
} from './router.js';
