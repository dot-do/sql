/**
 * DoSQL Native Sharding Types
 *
 * Core type definitions for sharding configuration, inspired by Vitess VSchema
 * but with improvements:
 * - Type-safe shard keys at compile time
 * - Native replica support
 * - Cost-based routing hints
 *
 * @packageDocumentation
 */

import type { ShardId } from '../engine/types.js';

// Re-export branded types for convenience
export type { ShardId } from '../engine/types.js';
export { createShardId } from '../engine/types.js';

// =============================================================================
// VINDEX TYPES
// =============================================================================

/**
 * Vindex (Virtual Index) type - determines how rows are distributed across shards
 *
 * - hash: FNV-1a or xxhash for uniform distribution
 * - consistent-hash: Virtual nodes for seamless rebalancing
 * - range: Boundary-based partitioning for range queries
 */
export type VindexType = 'hash' | 'consistent-hash' | 'range';

/**
 * Configuration for hash-based vindex
 */
export interface HashVindexConfig {
  type: 'hash';
  /** Hash algorithm: 'fnv1a' (default) or 'xxhash' */
  algorithm?: 'fnv1a' | 'xxhash';
}

/**
 * Configuration for consistent hash vindex
 * Uses virtual nodes for better distribution and rebalancing
 */
export interface ConsistentHashVindexConfig {
  type: 'consistent-hash';
  /** Number of virtual nodes per physical shard (default: 150) */
  virtualNodes?: number;
  /** Hash algorithm for consistent hash ring */
  algorithm?: 'fnv1a' | 'xxhash';
}

/**
 * Range boundary definition for range-based sharding
 */
export interface RangeBoundary<T = unknown> {
  /** Target shard ID (branded type) */
  shard: ShardId;
  /** Minimum value (inclusive) */
  min: T;
  /** Maximum value (exclusive, null for unbounded) */
  max: T | null;
}

/**
 * Configuration for range-based vindex
 */
export interface RangeVindexConfig<T = unknown> {
  type: 'range';
  /** Ordered list of range boundaries */
  boundaries: RangeBoundary<T>[];
}

/**
 * Union type for all vindex configurations
 */
export type VindexConfig<T = unknown> =
  | HashVindexConfig
  | ConsistentHashVindexConfig
  | RangeVindexConfig<T>;

// =============================================================================
// TABLE SHARDING TYPES
// =============================================================================

/**
 * Table sharding type
 *
 * - sharded: Data is distributed across shards using vindex
 * - unsharded: All data lives in a single designated shard
 * - reference: Data is replicated to all shards (lookup tables)
 */
export type TableShardingType = 'sharded' | 'unsharded' | 'reference';

/**
 * Configuration for a sharded table
 */
export interface ShardedTableConfig<T = unknown> {
  type: 'sharded';
  /** Column used as shard key */
  shardKey: string;
  /** Vindex configuration for routing */
  vindex: VindexConfig<T>;
  /** Secondary vindexes for alternate routing paths */
  secondaryVindexes?: Record<string, VindexConfig>;
}

/**
 * Configuration for an unsharded table
 */
export interface UnshardedTableConfig {
  type: 'unsharded';
  /** Specific shard where data resides (optional, uses primary shard if not set, branded type) */
  shard?: ShardId;
}

/**
 * Configuration for a reference table
 * Reference tables are replicated to all shards for efficient local joins
 */
export interface ReferenceTableConfig {
  type: 'reference';
  /** Whether to allow writes (goes to all shards) or read-only */
  readOnly?: boolean;
}

/**
 * Union type for all table sharding configurations
 */
export type TableShardingConfig<T = unknown> =
  | ShardedTableConfig<T>
  | UnshardedTableConfig
  | ReferenceTableConfig;

// =============================================================================
// SHARD & REPLICA CONFIGURATION
// =============================================================================

/**
 * Replica role in the shard
 */
export type ReplicaRole = 'primary' | 'replica' | 'analytics';

/**
 * Replica health status
 */
export type ReplicaHealth = 'healthy' | 'degraded' | 'unhealthy' | 'unknown';

/**
 * Configuration for a single replica
 */
export interface ReplicaConfig {
  /** Unique replica identifier */
  id: string;
  /** Durable Object namespace for this replica */
  doNamespace: string;
  /** Durable Object ID (optional, derived from shard ID if not set) */
  doId?: string;
  /** Role of this replica */
  role: ReplicaRole;
  /** Geographic region hint (e.g., 'us-west', 'eu-central') */
  region?: string;
  /** Weight for load balancing (higher = more traffic) */
  weight?: number;
}

/**
 * Configuration for a single shard
 */
export interface ShardConfig {
  /** Unique shard identifier (branded type) */
  id: ShardId;
  /** Primary Durable Object namespace */
  doNamespace: string;
  /** Primary Durable Object ID (optional, derived from shard ID if not set) */
  doId?: string;
  /** Replica configurations */
  replicas?: ReplicaConfig[];
  /** Shard metadata */
  metadata?: Record<string, unknown>;
  /** Whether this shard is read-only (for maintenance) */
  readOnly?: boolean;
}

// =============================================================================
// VSCHEMA - COMPLETE SHARDING CONFIGURATION
// =============================================================================

/**
 * VSchema - Virtual Schema defining the sharding topology
 *
 * The VSchema is the complete configuration that tells DoSQL how to:
 * 1. Route queries to the correct shard(s)
 * 2. Distribute data across shards
 * 3. Handle joins between tables
 * 4. Manage replicas for read scaling
 */
export interface VSchema<Tables extends Record<string, TableShardingConfig> = Record<string, TableShardingConfig>> {
  /** Table sharding configurations */
  tables: Tables;
  /** Shard definitions */
  shards: ShardConfig[];
  /** Default shard for unsharded tables (branded type) */
  defaultShard?: ShardId;
  /** Global settings */
  settings?: VSchemaSettings;
}

/**
 * Global VSchema settings
 */
export interface VSchemaSettings {
  /** Default vindex type for new sharded tables */
  defaultVindexType?: VindexType;
  /** Enable automatic shard key detection from schema */
  autoDetectShardKey?: boolean;
  /** Maximum parallel shard requests */
  maxParallelShards?: number;
  /** Default timeout for shard requests (ms) */
  shardTimeoutMs?: number;
  /** Enable query result caching */
  enableCaching?: boolean;
}

// =============================================================================
// QUERY ROUTING TYPES
// =============================================================================

/**
 * Query type classification for routing
 */
export type QueryType = 'single-shard' | 'scatter' | 'scatter-gather';

/**
 * Query operation type
 */
export type QueryOperation = 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE';

/**
 * Read preference for replica selection
 */
export type ReadPreference =
  | 'primary'           // Always read from primary
  | 'primaryPreferred'  // Primary if available, else replica
  | 'replica'           // Always read from replica
  | 'replicaPreferred'  // Replica if available, else primary
  | 'nearest'           // Lowest latency
  | 'analytics';        // Analytics replica for heavy queries

/**
 * Routing decision made by the query router
 */
export interface RoutingDecision {
  /** Type of query routing */
  queryType: QueryType;
  /** Target shard IDs (branded type) */
  targetShards: ShardId[];
  /** Shard key value (if single-shard) */
  shardKeyValue?: unknown;
  /** Read preference for this query */
  readPreference: ReadPreference;
  /** Whether query can use replica */
  canUseReplica: boolean;
  /** Cost estimate (lower is better) */
  costEstimate: number;
  /** Reason for routing decision */
  reason: string;
}

/**
 * Extracted shard key information from a query
 */
export interface ExtractedShardKey {
  /** Column name of the shard key */
  column: string;
  /** Extracted value(s) */
  values: unknown[];
  /** Extraction method */
  method: 'equality' | 'in-list' | 'range' | 'none';
}

// =============================================================================
// EXECUTION TYPES
// =============================================================================

/**
 * Execution plan for a distributed query
 */
export interface ExecutionPlan {
  /** Original SQL query */
  sql: string;
  /** Routing decision */
  routing: RoutingDecision;
  /** Per-shard execution plans */
  shardPlans: ShardExecutionPlan[];
  /** Post-processing operations */
  postProcessing?: PostProcessingOp[];
  /** Estimated total cost */
  totalCost: number;
}

/**
 * Execution plan for a single shard
 */
export interface ShardExecutionPlan {
  /** Target shard ID (branded type) */
  shardId: ShardId;
  /** Target replica ID (if using replica) */
  replicaId?: string;
  /** SQL to execute on this shard */
  sql: string;
  /** Parameters for the query */
  params?: unknown[];
  /** Whether this is the final result (no merging needed) */
  isFinal: boolean;
}

/**
 * Post-processing operations for scatter-gather queries
 */
export type PostProcessingOp =
  | { type: 'merge' }
  | { type: 'sort'; columns: SortColumn[] }
  | { type: 'limit'; count: number; offset?: number }
  | { type: 'aggregate'; aggregates: AggregateOp[] }
  | { type: 'distinct'; columns: string[] };

/**
 * Sort column specification
 */
export interface SortColumn {
  column: string;
  direction: 'ASC' | 'DESC';
  nulls?: 'FIRST' | 'LAST';
}

/**
 * Aggregate operation for two-phase aggregation
 */
export interface AggregateOp {
  /** Aggregate function */
  function: 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX';
  /** Source column */
  column: string;
  /** Output alias */
  alias: string;
  /** For AVG: track sum and count separately */
  isPartial?: boolean;
}

// =============================================================================
// SHARD RESULT TYPES
// =============================================================================

/**
 * Result from a single shard execution
 */
export interface ShardResult {
  /** Source shard ID (branded type) */
  shardId: ShardId;
  /** Source replica ID (if applicable) */
  replicaId?: string;
  /** Result rows */
  rows: unknown[][];
  /** Column names */
  columns: string[];
  /** Row count */
  rowCount: number;
  /** Execution time (ms) */
  executionTimeMs: number;
  /** Error (if failed) */
  error?: ShardError;
  /** Partial aggregates (for two-phase aggregation) */
  partialAggregates?: Record<string, number>;
}

/**
 * Error from shard execution
 */
export interface ShardError {
  code: string;
  message: string;
  isRetryable: boolean;
}

/**
 * Merged result from scatter-gather query
 */
export interface MergedResult {
  /** Final result rows */
  rows: unknown[][];
  /** Column names */
  columns: string[];
  /** Total row count */
  totalRowCount: number;
  /** Shards that contributed to the result (branded type) */
  contributingShards: ShardId[];
  /** Total execution time (ms) */
  totalExecutionTimeMs: number;
  /** Per-shard timing (keyed by ShardId as string) */
  shardTiming: Record<string, number>;
  /** Any partial failures */
  partialFailures?: ShardError[];
}

// =============================================================================
// TYPE-SAFE SCHEMA TYPES
// =============================================================================

/**
 * Column type string literals (matches DoSQL parser)
 */
export type ColumnTypeString = 'string' | 'number' | 'boolean' | 'Date' | 'null' | 'unknown';

/**
 * Type-safe table schema with column definitions
 */
export interface TypedTableSchema<
  Columns extends Record<string, ColumnTypeString> = Record<string, ColumnTypeString>,
  ShardKey extends keyof Columns | undefined = undefined
> {
  columns: Columns;
  shardKey: ShardKey;
}

/**
 * Type-safe database schema
 */
export type TypedDatabaseSchema = Record<string, TypedTableSchema>;

/**
 * Extract shard key type from a typed table schema
 */
export type ShardKeyType<T extends TypedTableSchema> =
  T['shardKey'] extends keyof T['columns']
    ? T['columns'][T['shardKey']]
    : never;

/**
 * Type-safe shard key extraction helper
 */
export type ShardKey<T, K extends keyof T> = T[K];

// =============================================================================
// COMPILE-TIME QUERY TYPE DETECTION
// =============================================================================

/**
 * Check if a WHERE clause contains an equality condition on a specific column
 * This is a simplified type-level check
 */
export type HasEqualityOnColumn<
  SQL extends string,
  Column extends string
> = Uppercase<SQL> extends `${string}WHERE${string}${Uppercase<Column>}${string}=${string}` ? true : false;

/**
 * Detect query type at compile time based on schema and SQL
 *
 * @template SQL - The SQL query string
 * @template Schema - The database schema with shard key information
 * @template Table - The table being queried
 */
export type DetectQueryType<
  SQL extends string,
  Schema extends TypedDatabaseSchema,
  Table extends keyof Schema
> = Schema[Table]['shardKey'] extends string
  ? HasEqualityOnColumn<SQL, Schema[Table]['shardKey']> extends true
    ? 'single-shard'
    : 'scatter'
  : 'scatter';

// =============================================================================
// HEALTH & METRICS TYPES
// =============================================================================

/**
 * Shard health metrics
 */
export interface ShardHealth {
  /** Shard ID (branded type) */
  shardId: ShardId;
  status: ReplicaHealth;
  primaryHealth: ReplicaHealth;
  replicaHealths: Record<string, ReplicaHealth>;
  lastChecked: number;
  latencyMs?: number;
  errorRate?: number;
}

/**
 * Cluster-wide health summary
 */
export interface ClusterHealth {
  shards: ShardHealth[];
  healthyShards: number;
  totalShards: number;
  healthyReplicas: number;
  totalReplicas: number;
  lastUpdated: number;
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a hash vindex configuration
 */
export function hashVindex(algorithm: 'fnv1a' | 'xxhash' = 'fnv1a'): HashVindexConfig {
  return { type: 'hash', algorithm };
}

/**
 * Create a consistent hash vindex configuration
 */
export function consistentHashVindex(
  virtualNodes: number = 150,
  algorithm: 'fnv1a' | 'xxhash' = 'fnv1a'
): ConsistentHashVindexConfig {
  return { type: 'consistent-hash', virtualNodes, algorithm };
}

/**
 * Create a range vindex configuration
 */
export function rangeVindex<T>(boundaries: RangeBoundary<T>[]): RangeVindexConfig<T> {
  return { type: 'range', boundaries };
}

/**
 * Create a sharded table configuration
 */
export function shardedTable<T>(
  shardKey: string,
  vindex: VindexConfig<T>
): ShardedTableConfig<T> {
  return { type: 'sharded', shardKey, vindex };
}

/**
 * Create an unsharded table configuration
 */
export function unshardedTable(shard?: ShardId): UnshardedTableConfig {
  return { type: 'unsharded', shard };
}

/**
 * Create a reference table configuration
 */
export function referenceTable(readOnly: boolean = false): ReferenceTableConfig {
  return { type: 'reference', readOnly };
}

/**
 * Create a shard configuration
 */
export function shard(
  id: ShardId,
  doNamespace: string,
  options?: Partial<Omit<ShardConfig, 'id' | 'doNamespace'>>
): ShardConfig {
  return { id, doNamespace, ...options };
}

/**
 * Create a replica configuration
 */
export function replica(
  id: string,
  doNamespace: string,
  role: ReplicaRole,
  options?: Partial<Omit<ReplicaConfig, 'id' | 'doNamespace' | 'role'>>
): ReplicaConfig {
  return { id, doNamespace, role, ...options };
}

/**
 * Create a VSchema configuration
 */
export function createVSchema<Tables extends Record<string, TableShardingConfig>>(
  tables: Tables,
  shards: ShardConfig[],
  options?: { defaultShard?: ShardId; settings?: VSchemaSettings }
): VSchema<Tables> {
  return {
    tables,
    shards,
    defaultShard: options?.defaultShard,
    settings: options?.settings,
  };
}
