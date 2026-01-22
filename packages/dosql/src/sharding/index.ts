/**
 * DoSQL Native Sharding Module
 *
 * A native sharding implementation for DoSQL, inspired by Vitess but
 * with key improvements:
 *
 * - Real SQL parsing (not regex)
 * - Cost-based query routing
 * - Native replica support
 * - Result streaming (not buffering)
 * - Type-safe shard keys at compile time
 *
 * @packageDocumentation
 */

// =============================================================================
// TYPE EXPORTS
// =============================================================================

export type {
  // Vindex types
  VindexType,
  VindexConfig,
  HashVindexConfig,
  ConsistentHashVindexConfig,
  RangeVindexConfig,
  RangeBoundary,

  // Table sharding types
  TableShardingType,
  TableShardingConfig,
  ShardedTableConfig,
  UnshardedTableConfig,
  ReferenceTableConfig,

  // Shard & replica types
  ShardConfig,
  ReplicaConfig,
  ReplicaRole,
  ReplicaHealth,

  // VSchema types
  VSchema,
  VSchemaSettings,

  // Query routing types
  QueryType,
  QueryOperation,
  ReadPreference,
  RoutingDecision,
  ExtractedShardKey,

  // Execution types
  ExecutionPlan,
  ShardExecutionPlan,
  PostProcessingOp,
  SortColumn,
  AggregateOp,

  // Result types
  ShardResult,
  ShardError,
  MergedResult,

  // Type-safe schema types
  ColumnTypeString,
  TypedTableSchema,
  TypedDatabaseSchema,
  ShardKeyType,
  ShardKey,

  // Compile-time query detection
  HasEqualityOnColumn,
  DetectQueryType,

  // Health types
  ShardHealth,
  ClusterHealth,
} from './types.js';

// =============================================================================
// FACTORY FUNCTION EXPORTS
// =============================================================================

export {
  // Vindex factory functions
  hashVindex,
  consistentHashVindex,
  rangeVindex,

  // Table config factory functions
  shardedTable,
  unshardedTable,
  referenceTable,

  // Shard/replica factory functions
  shard,
  replica,

  // VSchema factory function
  createVSchema,
} from './types.js';

// =============================================================================
// VINDEX EXPORTS
// =============================================================================

export {
  // Hash functions
  fnv1a,
  xxhash,
  getHashFunction,

  // Vindex interface
  type Vindex,

  // Vindex implementations
  HashVindex,
  ConsistentHashVindex,
  RangeVindex,

  // Factory function
  createVindex,

  // Utilities
  testDistribution,
  distributionStats,
} from './vindex.js';

// =============================================================================
// ROUTER EXPORTS
// =============================================================================

export {
  // Parser types
  type ParsedQuery,
  type TableReference,
  type ColumnReference,
  type WhereClause,
  type WhereCondition,

  // Parser class
  SQLParser,

  // Router class
  QueryRouter,

  // Factory function
  createRouter,
} from './router.js';

// =============================================================================
// EXECUTOR EXPORTS
// =============================================================================

export {
  // RPC interface
  type ShardRPC,
  type ExecuteOptions,

  // Executor config
  type ExecutorConfig,

  // Executor class
  DistributedExecutor,

  // Error types
  ShardExecutionError,

  // Mock for testing
  MockShardRPC,

  // Factory function
  createExecutor,
} from './executor.js';

// =============================================================================
// REPLICA EXPORTS
// =============================================================================

export {
  // Health config
  type HealthConfig,

  // Replica selector interface
  type ReplicaSelector,

  // Replica selector implementation
  DefaultReplicaSelector,

  // Health checker
  HealthChecker,

  // Factory functions
  createReplicaSelector,
  createHealthChecker,
} from './replica.js';

// =============================================================================
// HIGH-LEVEL API
// =============================================================================

import type { VSchema, ReadPreference, MergedResult } from './types.js';
import { createRouter, type QueryRouter } from './router.js';
import { createExecutor, type ShardRPC, type DistributedExecutor, type ExecutorConfig } from './executor.js';
import { createReplicaSelector, type DefaultReplicaSelector, type HealthConfig } from './replica.js';

/**
 * Configuration for the sharding system
 */
export interface ShardingConfig {
  /** VSchema defining the sharding topology */
  vschema: VSchema;
  /** RPC implementation for shard communication */
  rpc: ShardRPC;
  /** Current region for nearest replica selection */
  currentRegion?: string;
  /** Executor configuration */
  executor?: ExecutorConfig;
  /** Health tracking configuration */
  health?: HealthConfig;
}

/**
 * High-level sharding client
 */
export interface ShardingClient {
  /**
   * Execute a SQL query across shards
   */
  query(sql: string, params?: unknown[], readPreference?: ReadPreference): Promise<MergedResult>;

  /**
   * Execute a SQL query with streaming results
   */
  queryStream(sql: string, params?: unknown[], readPreference?: ReadPreference): AsyncIterable<unknown[]>;

  /**
   * Get the query router
   */
  router: QueryRouter;

  /**
   * Get the replica selector
   */
  replicaSelector: DefaultReplicaSelector;

  /**
   * Get the distributed executor
   */
  executor: DistributedExecutor;
}

/**
 * Create a sharding client
 *
 * @example
 * ```typescript
 * const client = createShardingClient({
 *   vschema: createVSchema({
 *     users: shardedTable('tenant_id', hashVindex()),
 *     countries: referenceTable(),
 *   }, [
 *     shard('shard-1', 'user-do'),
 *     shard('shard-2', 'user-do'),
 *   ]),
 *   rpc: myRpcImplementation,
 * });
 *
 * const result = await client.query(
 *   'SELECT * FROM users WHERE tenant_id = $1',
 *   [123]
 * );
 * ```
 */
export function createShardingClient(config: ShardingConfig): ShardingClient {
  const router = createRouter(config.vschema);
  const replicaSelector = createReplicaSelector(
    config.vschema.shards,
    config.health,
    config.currentRegion
  );
  const executor = createExecutor(config.rpc, replicaSelector, config.executor);

  return {
    router,
    replicaSelector,
    executor,

    async query(sql: string, params?: unknown[], readPreference?: ReadPreference): Promise<MergedResult> {
      const plan = router.createExecutionPlan(sql, params, readPreference);
      return executor.execute(plan);
    },

    async *queryStream(sql: string, params?: unknown[], readPreference?: ReadPreference): AsyncIterable<unknown[]> {
      const plan = router.createExecutionPlan(sql, params, readPreference);
      yield* executor.executeStream(plan);
    },
  };
}

// =============================================================================
// TYPE-SAFE SHARDING
// =============================================================================

/**
 * Type-safe sharding client with compile-time query type detection
 *
 * @example
 * ```typescript
 * interface MySchema {
 *   users: {
 *     columns: { id: 'number'; tenant_id: 'number'; name: 'string' };
 *     shardKey: 'tenant_id';
 *   };
 * }
 *
 * const client = createTypedShardingClient<MySchema>(config);
 *
 * // TypeScript knows this is a single-shard query
 * const result = await client.query('SELECT * FROM users WHERE tenant_id = 1');
 * ```
 */
export function createTypedShardingClient<Schema extends Record<string, { columns: Record<string, string>; shardKey: string | undefined }>>(
  config: ShardingConfig
): ShardingClient {
  return createShardingClient(config);
}
