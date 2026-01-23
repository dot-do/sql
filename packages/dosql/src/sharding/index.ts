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
 * @example Basic sharding setup
 * ```typescript
 * import {
 *   createShardingClient,
 *   createVSchema,
 *   shardedTable,
 *   referenceTable,
 *   hashVindex,
 *   shard,
 *   createShardId,
 * } from '@dosql/sharding';
 *
 * // Define your VSchema
 * const vschema = createVSchema({
 *   users: shardedTable('tenant_id', hashVindex()),
 *   countries: referenceTable(),
 * }, [
 *   shard(createShardId('shard-1'), 'user-do'),
 *   shard(createShardId('shard-2'), 'user-do'),
 * ]);
 *
 * // Create a sharding client
 * const client = createShardingClient({
 *   vschema,
 *   rpc: myRpcImplementation,
 * });
 *
 * // Execute queries - automatically routed to correct shard
 * const result = await client.query(
 *   'SELECT * FROM users WHERE tenant_id = $1',
 *   [123]
 * );
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// TYPE EXPORTS
// =============================================================================

/**
 * Vindex (Virtual Index) types for controlling data distribution across shards.
 *
 * @example Using different vindex types
 * ```typescript
 * // Hash vindex for uniform distribution
 * const userTable = shardedTable('user_id', hashVindex('xxhash'));
 *
 * // Consistent hash for easier rebalancing
 * const orderTable = shardedTable('order_id', consistentHashVindex(150));
 *
 * // Range vindex for time-series data
 * const eventTable = shardedTable('created_at', rangeVindex([
 *   { shard: createShardId('shard-2024'), min: new Date('2024-01-01'), max: new Date('2025-01-01') },
 *   { shard: createShardId('shard-2025'), min: new Date('2025-01-01'), max: null },
 * ]));
 * ```
 */
export type {
  /**
   * Vindex type identifier: 'hash', 'consistent-hash', or 'range'.
   * Determines the algorithm used to map shard keys to shards.
   */
  VindexType,
  /**
   * Union type for all vindex configuration options.
   * Use factory functions (hashVindex, consistentHashVindex, rangeVindex) to create these.
   */
  VindexConfig,
  /**
   * Configuration for hash-based vindex using FNV-1a or xxhash algorithms.
   * Best for uniform distribution when you don't need range queries.
   */
  HashVindexConfig,
  /**
   * Configuration for consistent hash vindex with virtual nodes.
   * Best when you anticipate adding/removing shards frequently.
   */
  ConsistentHashVindexConfig,
  /**
   * Configuration for range-based vindex with explicit boundaries.
   * Best for time-series data or natural range partitioning.
   */
  RangeVindexConfig,
  /**
   * Defines a range boundary mapping values to a specific shard.
   * Used with RangeVindexConfig.
   */
  RangeBoundary,

  /**
   * Table sharding strategy: 'sharded', 'unsharded', or 'reference'.
   */
  TableShardingType,
  /**
   * Union type for all table sharding configurations.
   */
  TableShardingConfig,
  /**
   * Configuration for a table distributed across shards using a vindex.
   */
  ShardedTableConfig,
  /**
   * Configuration for a table residing on a single shard.
   */
  UnshardedTableConfig,
  /**
   * Configuration for a reference table replicated to all shards.
   * Ideal for lookup tables that are frequently joined.
   */
  ReferenceTableConfig,

  /**
   * Configuration for a single shard in the cluster.
   */
  ShardConfig,
  /**
   * Configuration for a replica within a shard.
   */
  ReplicaConfig,
  /**
   * Role of a replica: 'primary', 'replica', or 'analytics'.
   */
  ReplicaRole,
  /**
   * Health status of a replica: 'healthy', 'degraded', 'unhealthy', or 'unknown'.
   */
  ReplicaHealth,

  /**
   * Complete sharding topology configuration (Virtual Schema).
   * Defines tables, shards, and routing rules.
   */
  VSchema,
  /**
   * Global settings for VSchema behavior.
   */
  VSchemaSettings,

  /**
   * Query routing classification: 'single-shard', 'scatter', or 'scatter-gather'.
   */
  QueryType,
  /**
   * SQL operation type: 'SELECT', 'INSERT', 'UPDATE', or 'DELETE'.
   */
  QueryOperation,
  /**
   * Read preference for replica selection.
   * Controls whether reads go to primary, replicas, or nearest node.
   */
  ReadPreference,
  /**
   * Result of query routing analysis including target shards and cost estimate.
   */
  RoutingDecision,
  /**
   * Information about shard key extraction from a query's WHERE clause.
   */
  ExtractedShardKey,

  /**
   * Complete execution plan for a distributed query.
   */
  ExecutionPlan,
  /**
   * Execution plan for a single shard.
   */
  ShardExecutionPlan,
  /**
   * Post-processing operation for scatter-gather queries (merge, sort, limit, etc.).
   */
  PostProcessingOp,
  /**
   * Sort column specification for ORDER BY post-processing.
   */
  SortColumn,
  /**
   * Aggregate operation for two-phase aggregation (SUM, COUNT, AVG, etc.).
   */
  AggregateOp,

  /**
   * Result from executing a query on a single shard.
   */
  ShardResult,
  /**
   * Error information from a failed shard execution.
   */
  ShardError,
  /**
   * Merged result from scatter-gather query execution.
   */
  MergedResult,

  /**
   * String literal for column types used in typed schemas.
   */
  ColumnTypeString,
  /**
   * Type-safe table schema with column type definitions.
   */
  TypedTableSchema,
  /**
   * Type-safe database schema mapping table names to typed table schemas.
   */
  TypedDatabaseSchema,
  /**
   * Utility type to extract the shard key type from a typed table schema.
   */
  ShardKeyType,
  /**
   * Utility type for type-safe shard key access.
   */
  ShardKey,

  /**
   * Compile-time type to check if SQL has equality on a specific column.
   */
  HasEqualityOnColumn,
  /**
   * Compile-time type to detect query routing type based on schema and SQL.
   */
  DetectQueryType,

  /**
   * Health metrics for a single shard including primary and replica status.
   */
  ShardHealth,
  /**
   * Cluster-wide health summary across all shards and replicas.
   */
  ClusterHealth,
} from './types.js';

// =============================================================================
// FACTORY FUNCTION EXPORTS
// =============================================================================

/**
 * Factory functions for creating sharding configurations.
 *
 * @example Creating a complete VSchema
 * ```typescript
 * import {
 *   createVSchema,
 *   shardedTable,
 *   unshardedTable,
 *   referenceTable,
 *   hashVindex,
 *   consistentHashVindex,
 *   rangeVindex,
 *   shard,
 *   replica,
 *   createShardId,
 * } from '@dosql/sharding';
 *
 * const vschema = createVSchema({
 *   // Sharded by tenant_id using hash
 *   users: shardedTable('tenant_id', hashVindex()),
 *
 *   // Sharded by order_id using consistent hash (better for rebalancing)
 *   orders: shardedTable('order_id', consistentHashVindex(150)),
 *
 *   // All config data on a single shard
 *   app_config: unshardedTable(createShardId('shard-1')),
 *
 *   // Lookup table replicated everywhere
 *   countries: referenceTable(),
 * }, [
 *   shard(createShardId('shard-1'), 'user-do', {
 *     replicas: [
 *       replica('replica-1a', 'user-do-replica', 'replica', { region: 'us-west' }),
 *       replica('replica-1b', 'user-do-replica', 'replica', { region: 'us-east' }),
 *     ],
 *   }),
 *   shard(createShardId('shard-2'), 'user-do'),
 * ]);
 * ```
 */
export {
  /**
   * Creates a hash vindex configuration for uniform data distribution.
   *
   * @param algorithm - Hash algorithm: 'fnv1a' (default) or 'xxhash'
   * @returns Hash vindex configuration
   *
   * @example
   * ```typescript
   * // Using default FNV-1a algorithm
   * const table1 = shardedTable('user_id', hashVindex());
   *
   * // Using xxhash for better performance with longer keys
   * const table2 = shardedTable('email', hashVindex('xxhash'));
   * ```
   */
  hashVindex,
  /**
   * Creates a consistent hash vindex for seamless shard rebalancing.
   *
   * @param virtualNodes - Number of virtual nodes per shard (default: 150)
   * @param algorithm - Hash algorithm: 'fnv1a' (default) or 'xxhash'
   * @returns Consistent hash vindex configuration
   *
   * @example
   * ```typescript
   * // Default 150 virtual nodes
   * const table1 = shardedTable('session_id', consistentHashVindex());
   *
   * // More virtual nodes for better distribution
   * const table2 = shardedTable('cache_key', consistentHashVindex(300, 'xxhash'));
   * ```
   */
  consistentHashVindex,
  /**
   * Creates a range vindex for boundary-based partitioning.
   *
   * @param boundaries - Array of range boundaries mapping to shards
   * @returns Range vindex configuration
   *
   * @example
   * ```typescript
   * // Time-based range sharding
   * const events = shardedTable('created_at', rangeVindex([
   *   { shard: createShardId('archive'), min: new Date('2020-01-01'), max: new Date('2024-01-01') },
   *   { shard: createShardId('current'), min: new Date('2024-01-01'), max: null },
   * ]));
   *
   * // ID-based range sharding
   * const users = shardedTable('user_id', rangeVindex([
   *   { shard: createShardId('shard-1'), min: 0, max: 1000000 },
   *   { shard: createShardId('shard-2'), min: 1000000, max: 2000000 },
   *   { shard: createShardId('shard-3'), min: 2000000, max: null },
   * ]));
   * ```
   */
  rangeVindex,

  /**
   * Creates a sharded table configuration.
   *
   * @param shardKey - Column name used for shard routing
   * @param vindex - Vindex configuration for data distribution
   * @returns Sharded table configuration
   *
   * @example
   * ```typescript
   * // Simple sharded table
   * const users = shardedTable('tenant_id', hashVindex());
   *
   * // With typed shard key
   * const orders = shardedTable<number>('customer_id', hashVindex());
   * ```
   */
  shardedTable,
  /**
   * Creates an unsharded table configuration (single shard).
   *
   * @param shard - Optional specific shard ID (uses default if not provided)
   * @returns Unsharded table configuration
   *
   * @example
   * ```typescript
   * // Uses default shard
   * const globalConfig = unshardedTable();
   *
   * // Explicit shard assignment
   * const adminData = unshardedTable(createShardId('admin-shard'));
   * ```
   */
  unshardedTable,
  /**
   * Creates a reference table configuration (replicated to all shards).
   *
   * @param readOnly - Whether the table is read-only (default: false)
   * @returns Reference table configuration
   *
   * @example
   * ```typescript
   * // Writable reference table (writes go to all shards)
   * const countries = referenceTable();
   *
   * // Read-only reference table
   * const staticLookup = referenceTable(true);
   * ```
   */
  referenceTable,

  /**
   * Creates a shard configuration.
   *
   * @param id - Unique shard identifier (ShardId branded type)
   * @param doNamespace - Durable Object namespace name
   * @param options - Optional shard configuration (replicas, metadata, readOnly)
   * @returns Shard configuration
   *
   * @example
   * ```typescript
   * // Simple shard
   * const shard1 = shard(createShardId('shard-1'), 'user-do');
   *
   * // Shard with replicas and metadata
   * const shard2 = shard(createShardId('shard-2'), 'user-do', {
   *   replicas: [
   *     replica('r2a', 'user-replica', 'replica', { region: 'eu-west' }),
   *   ],
   *   metadata: { datacenter: 'eu-1' },
   * });
   * ```
   */
  shard,
  /**
   * Creates a replica configuration.
   *
   * @param id - Unique replica identifier
   * @param doNamespace - Durable Object namespace name
   * @param role - Replica role: 'primary', 'replica', or 'analytics'
   * @param options - Optional replica configuration (region, weight)
   * @returns Replica configuration
   *
   * @example
   * ```typescript
   * // Read replica in specific region
   * const r1 = replica('replica-1', 'user-replica', 'replica', {
   *   region: 'us-west',
   *   weight: 2, // Higher weight = more traffic
   * });
   *
   * // Analytics replica for heavy queries
   * const r2 = replica('analytics-1', 'user-analytics', 'analytics');
   * ```
   */
  replica,

  /**
   * Creates a complete VSchema configuration.
   *
   * @param tables - Table sharding configurations keyed by table name
   * @param shards - Array of shard configurations
   * @param options - Optional settings (defaultShard, global settings)
   * @returns Complete VSchema configuration
   *
   * @example
   * ```typescript
   * const vschema = createVSchema(
   *   {
   *     users: shardedTable('tenant_id', hashVindex()),
   *     countries: referenceTable(),
   *   },
   *   [
   *     shard(createShardId('shard-1'), 'user-do'),
   *     shard(createShardId('shard-2'), 'user-do'),
   *   ],
   *   {
   *     defaultShard: createShardId('shard-1'),
   *     settings: {
   *       maxParallelShards: 16,
   *       shardTimeoutMs: 5000,
   *     },
   *   }
   * );
   * ```
   */
  createVSchema,
} from './types.js';

// =============================================================================
// VINDEX EXPORTS
// =============================================================================

/**
 * Vindex (Virtual Index) implementations and utilities.
 *
 * Vindexes determine how data is distributed across shards. Each vindex
 * type has different characteristics:
 *
 * - **HashVindex**: Fast, uniform distribution. Cannot optimize range queries.
 * - **ConsistentHashVindex**: Better for adding/removing shards. Cannot optimize range queries.
 * - **RangeVindex**: Supports range query optimization. Requires careful boundary planning.
 *
 * @example Using vindexes directly
 * ```typescript
 * import { HashVindex, createVindex, testDistribution, distributionStats } from '@dosql/sharding';
 *
 * // Create vindex from config
 * const vindex = createVindex(shards, { type: 'hash', algorithm: 'fnv1a' });
 *
 * // Route a key to a shard
 * const shardId = vindex.getShard(12345);
 *
 * // Route multiple keys (for IN queries)
 * const shardIds = vindex.getShardsForKeys([1, 2, 3, 4, 5]);
 *
 * // Test distribution quality
 * const distribution = testDistribution(vindex, () => Math.random(), 10000);
 * const stats = distributionStats(distribution);
 * console.log('Standard deviation:', stats.stdDev);
 * ```
 */
export {
  /**
   * FNV-1a hash function implementation.
   * Fast 32-bit hash with good distribution for sharding.
   *
   * @param input - String, number, or bigint to hash
   * @returns 32-bit unsigned integer hash
   *
   * @example
   * ```typescript
   * const hash1 = fnv1a('user-123');
   * const hash2 = fnv1a(12345);
   * const hash3 = fnv1a(BigInt('9007199254740993'));
   * ```
   */
  fnv1a,
  /**
   * xxHash-inspired fast hash implementation.
   * Better performance than FNV-1a for longer strings.
   *
   * @param input - String, number, or bigint to hash
   * @returns 32-bit unsigned integer hash
   *
   * @example
   * ```typescript
   * const hash = xxhash('long-email@example.com');
   * ```
   */
  xxhash,
  /**
   * Get hash function by algorithm name.
   *
   * @param algorithm - 'fnv1a' or 'xxhash'
   * @returns Hash function
   *
   * @example
   * ```typescript
   * const hashFn = getHashFunction('xxhash');
   * const hash = hashFn('my-key');
   * ```
   */
  getHashFunction,

  /**
   * Vindex interface for shard routing.
   * All vindex implementations conform to this interface.
   */
  type Vindex,

  /**
   * Hash-based vindex using modulo arithmetic.
   * Provides uniform distribution but cannot optimize range queries.
   *
   * @example
   * ```typescript
   * const vindex = new HashVindex(shards, { type: 'hash', algorithm: 'fnv1a' });
   * const shardId = vindex.getShard('user-123');
   * ```
   */
  HashVindex,
  /**
   * Consistent hash vindex using virtual nodes.
   * Better for dynamic shard addition/removal with minimal data movement.
   *
   * @example
   * ```typescript
   * const vindex = new ConsistentHashVindex(shards, {
   *   type: 'consistent-hash',
   *   virtualNodes: 150,
   * });
   *
   * // Debug ring state
   * const { totalNodes, nodesPerShard } = vindex.getRingState();
   * ```
   */
  ConsistentHashVindex,
  /**
   * Range-based vindex with explicit boundaries.
   * Supports efficient range query optimization.
   *
   * @example
   * ```typescript
   * const vindex = new RangeVindex(shards, {
   *   type: 'range',
   *   boundaries: [
   *     { shard: createShardId('old'), min: 0, max: 1000000 },
   *     { shard: createShardId('new'), min: 1000000, max: null },
   *   ],
   * });
   *
   * // Efficient range query routing
   * const shards = vindex.getShardsForRange(500000, 1500000);
   * ```
   */
  RangeVindex,

  /**
   * Factory function to create a vindex from configuration.
   *
   * @param shards - Shard configurations
   * @param config - Vindex configuration
   * @returns Vindex instance
   *
   * @example
   * ```typescript
   * const hashVindex = createVindex(shards, { type: 'hash' });
   * const consistentVindex = createVindex(shards, { type: 'consistent-hash', virtualNodes: 200 });
   * const rangeVindex = createVindex(shards, { type: 'range', boundaries: [...] });
   * ```
   */
  createVindex,

  /**
   * Test distribution quality of a vindex.
   * Generates sample keys and measures distribution across shards.
   *
   * @param vindex - Vindex to test
   * @param keyGenerator - Function that generates random keys
   * @param sampleSize - Number of samples (default: 10000)
   * @returns Map of shard ID to count
   *
   * @example
   * ```typescript
   * const distribution = testDistribution(
   *   vindex,
   *   () => `user-${Math.floor(Math.random() * 1000000)}`,
   *   50000
   * );
   *
   * for (const [shardId, count] of distribution) {
   *   console.log(`${shardId}: ${count} keys`);
   * }
   * ```
   */
  testDistribution,
  /**
   * Calculate statistics for a distribution.
   *
   * @param distribution - Map of shard ID to count from testDistribution
   * @returns Statistics object with min, max, mean, stdDev, and skew
   *
   * @example
   * ```typescript
   * const stats = distributionStats(distribution);
   *
   * // Good distribution: low stdDev, skew near 0
   * if (stats.stdDev < stats.mean * 0.1) {
   *   console.log('Distribution is well-balanced');
   * }
   * ```
   */
  distributionStats,
} from './vindex.js';

// =============================================================================
// ROUTER EXPORTS
// =============================================================================

/**
 * SQL parsing and query routing components.
 *
 * The router analyzes SQL queries and determines optimal shard routing:
 * - Extracts shard keys from WHERE clauses
 * - Creates execution plans for single-shard and scatter queries
 * - Handles query rewriting for distributed aggregation
 *
 * @example Query routing
 * ```typescript
 * import { createRouter, SQLParser } from '@dosql/sharding';
 *
 * const router = createRouter(vschema);
 *
 * // Single-shard query (efficient)
 * const decision1 = router.route(
 *   'SELECT * FROM users WHERE tenant_id = $1',
 *   [123]
 * );
 * console.log(decision1.queryType); // 'single-shard'
 * console.log(decision1.targetShards); // ['shard-2']
 *
 * // Scatter query (hits all shards)
 * const decision2 = router.route('SELECT COUNT(*) FROM users');
 * console.log(decision2.queryType); // 'scatter'
 *
 * // Create full execution plan
 * const plan = router.createExecutionPlan(
 *   'SELECT * FROM users WHERE tenant_id IN ($1, $2)',
 *   [100, 200]
 * );
 * ```
 */
export {
  /**
   * Parsed SQL query structure containing operation, tables, columns, WHERE, etc.
   */
  type ParsedQuery,
  /**
   * Table reference in a SQL query (name and optional alias).
   */
  type TableReference,
  /**
   * Column reference in a SQL query (name, alias, table, aggregate).
   */
  type ColumnReference,
  /**
   * WHERE clause structure with conditions and logical operator (AND/OR).
   */
  type WhereClause,
  /**
   * Single WHERE condition (column, operator, value(s)).
   */
  type WhereCondition,

  /**
   * SQL parser using state-aware tokenization.
   * Properly handles comments, string literals, and edge cases.
   *
   * @example
   * ```typescript
   * const parser = new SQLParser();
   *
   * const query = parser.parse(`
   *   SELECT u.name, COUNT(*) as order_count
   *   FROM users u
   *   JOIN orders o ON u.id = o.user_id
   *   WHERE u.tenant_id = 123
   *   GROUP BY u.name
   *   ORDER BY order_count DESC
   *   LIMIT 10
   * `);
   *
   * console.log(query.operation); // 'SELECT'
   * console.log(query.tables); // [{ name: 'users', alias: 'u' }]
   * console.log(query.aggregates); // [{ function: 'COUNT', column: '*', alias: 'order_count' }]
   * ```
   */
  SQLParser,

  /**
   * Query router that determines shard routing based on VSchema and SQL.
   *
   * @example
   * ```typescript
   * const router = new QueryRouter(vschema);
   *
   * // Route with read preference
   * const decision = router.route(
   *   'SELECT * FROM users WHERE tenant_id = $1',
   *   [123],
   *   'replicaPreferred'
   * );
   *
   * // Get table configuration
   * const config = router.getTableConfig('users');
   *
   * // Get all shards
   * const shards = router.getShards();
   * ```
   */
  QueryRouter,

  /**
   * Factory function to create a query router.
   *
   * @param vschema - VSchema configuration
   * @returns QueryRouter instance
   *
   * @example
   * ```typescript
   * const router = createRouter(vschema);
   * const plan = router.createExecutionPlan('SELECT * FROM users WHERE id = 1');
   * ```
   */
  createRouter,
} from './router.js';

// =============================================================================
// EXECUTOR EXPORTS
// =============================================================================

/**
 * Distributed query execution components.
 *
 * The executor handles parallel shard execution, result merging, and error handling:
 * - Parallel execution with configurable concurrency
 * - Circuit breaker for failing shards
 * - Two-phase aggregation for distributed queries
 * - Streaming results for memory efficiency
 *
 * @example Distributed execution
 * ```typescript
 * import { createExecutor, MockShardRPC, DistributedExecutor } from '@dosql/sharding';
 *
 * // Create executor with custom config
 * const executor = createExecutor(rpc, replicaSelector, {
 *   maxParallelShards: 16,
 *   defaultTimeoutMs: 5000,
 *   failFast: true,
 *   retry: {
 *     maxAttempts: 3,
 *     backoffMs: 100,
 *     maxBackoffMs: 2000,
 *   },
 *   circuitBreaker: {
 *     failureThreshold: 5,
 *     resetTimeoutMs: 30000,
 *   },
 * });
 *
 * // Execute a plan
 * const plan = router.createExecutionPlan('SELECT * FROM users');
 * const result = await executor.execute(plan);
 *
 * // Stream results
 * for await (const row of executor.executeStream(plan)) {
 *   console.log(row);
 * }
 * ```
 */
export {
  /**
   * Interface for shard RPC communication.
   * Implement this to connect to your Durable Objects or other backends.
   *
   * @example
   * ```typescript
   * class MyShardRPC implements ShardRPC {
   *   async execute(shardId, replicaId, sql, params, options) {
   *     const stub = this.env.USER_DO.get(this.env.USER_DO.idFromName(shardId));
   *     const response = await stub.fetch('/query', {
   *       method: 'POST',
   *       body: JSON.stringify({ sql, params }),
   *     });
   *     return response.json();
   *   }
   *
   *   async *executeStream(shardId, replicaId, sql, params, options) {
   *     // Implement streaming if supported
   *     yield await this.execute(shardId, replicaId, sql, params, options);
   *   }
   * }
   * ```
   */
  type ShardRPC,
  /**
   * Options for query execution (timeout, max rows, column types).
   */
  type ExecuteOptions,

  /**
   * Configuration for the distributed executor.
   */
  type ExecutorConfig,

  /**
   * Distributed query executor with parallel execution and circuit breaker.
   *
   * @example
   * ```typescript
   * const executor = new DistributedExecutor(rpc, replicaSelector, config);
   *
   * // Listen for circuit breaker events
   * executor.on('circuitStateChange', (event) => {
   *   console.log(`Shard ${event.shardId}: ${event.oldState} -> ${event.newState}`);
   * });
   *
   * // Check circuit state
   * const state = executor.getCircuitState('shard-1');
   * if (state.state === 'OPEN') {
   *   console.log('Shard is unavailable');
   * }
   *
   * // Force circuit management (admin)
   * executor.forceCircuitOpen('shard-1'); // Mark shard as down
   * executor.forceCircuitClose('shard-1'); // Reset shard health
   * ```
   */
  DistributedExecutor,

  /**
   * Error thrown when one or more shards fail during execution.
   *
   * @example
   * ```typescript
   * try {
   *   await executor.execute(plan);
   * } catch (err) {
   *   if (err instanceof ShardExecutionError) {
   *     for (const shardErr of err.shardErrors) {
   *       console.log(`Shard error: ${shardErr.message}`);
   *       if (shardErr.isRetryable) {
   *         // Could retry this operation
   *       }
   *     }
   *   }
   * }
   * ```
   */
  ShardExecutionError,

  /**
   * Mock RPC implementation for testing.
   *
   * @example
   * ```typescript
   * const mockRpc = new MockShardRPC();
   *
   * // Set up test data
   * mockRpc.setShardData('shard-1', ['id', 'name'], [
   *   [1, 'Alice'],
   *   [2, 'Bob'],
   * ]);
   * mockRpc.setShardData('shard-2', ['id', 'name'], [
   *   [3, 'Charlie'],
   * ]);
   *
   * // Use in tests
   * const executor = createExecutor(mockRpc, replicaSelector);
   * const result = await executor.execute(plan);
   * expect(result.totalRowCount).toBe(3);
   * ```
   */
  MockShardRPC,

  /**
   * Factory function to create a distributed executor.
   *
   * @param rpc - ShardRPC implementation for shard communication
   * @param replicaSelector - Replica selector for read routing
   * @param config - Optional executor configuration
   * @returns DistributedExecutor instance
   *
   * @example
   * ```typescript
   * const executor = createExecutor(rpc, replicaSelector, {
   *   maxParallelShards: 8,
   *   failFast: true,
   * });
   * ```
   */
  createExecutor,
} from './executor.js';

// =============================================================================
// REPLICA EXPORTS
// =============================================================================

/**
 * Replica management and health tracking components.
 *
 * Handles replica selection for read scaling and automatic health tracking:
 * - Multiple read preferences (primary, replica, nearest, analytics)
 * - Automatic health degradation based on failures
 * - Circuit breaker for unhealthy replicas
 * - Latency-based nearest replica selection
 *
 * @example Replica management
 * ```typescript
 * import {
 *   createReplicaSelector,
 *   createHealthChecker,
 *   DefaultReplicaSelector,
 * } from '@dosql/sharding';
 *
 * // Create replica selector with region awareness
 * const selector = createReplicaSelector(shards, {
 *   failureThreshold: 3,
 *   circuitResetMs: 30000,
 * }, 'us-west');
 *
 * // Select replica based on preference
 * const replicaId = selector.select('shard-1', 'nearest');
 *
 * // Record success/failure for health tracking
 * selector.recordSuccess('shard-1', 'replica-1', 15); // 15ms latency
 * selector.recordFailure('shard-1', 'replica-1');
 *
 * // Get health information
 * const shardHealth = selector.getShardHealth('shard-1');
 * const clusterHealth = selector.getClusterHealth();
 * ```
 */
export {
  /**
   * Configuration for health tracking behavior.
   *
   * @example
   * ```typescript
   * const healthConfig: HealthConfig = {
   *   latencyWindowSize: 100,    // Track last 100 latencies
   *   failureThreshold: 5,       // 5 failures = unhealthy
   *   successThreshold: 3,       // 3 successes = healthy again
   *   failureWindowMs: 60000,    // Reset failure count after 1 minute
   *   circuitResetMs: 30000,     // Try again after 30 seconds
   * };
   * ```
   */
  type HealthConfig,

  /**
   * Interface for replica selection and health tracking.
   */
  type ReplicaSelector,

  /**
   * Default replica selector implementation with health tracking.
   *
   * @example
   * ```typescript
   * const selector = new DefaultReplicaSelector(shards, healthConfig, 'us-west');
   *
   * // Select with different preferences
   * const primary = selector.select('shard-1', 'primary');
   * const replica = selector.select('shard-1', 'replicaPreferred');
   * const nearest = selector.select('shard-1', 'nearest');
   * const analytics = selector.select('shard-1', 'analytics');
   *
   * // Debug health state
   * const debugState = selector.getHealthStateDebug('shard-1', 'replica-1');
   * console.log(debugState.circuitState); // 'closed', 'open', or 'half-open'
   * console.log(debugState.latencyMs); // Recent latency samples
   *
   * // Reset health after manual intervention
   * selector.resetHealth('shard-1', 'replica-1');
   * ```
   */
  DefaultReplicaSelector,

  /**
   * Active health checker that periodically pings replicas.
   *
   * @example
   * ```typescript
   * // Create health check function
   * async function checkReplica(shardId: string, replicaId: string): Promise<number> {
   *   const start = Date.now();
   *   const stub = env.USER_DO.get(env.USER_DO.idFromName(replicaId));
   *   await stub.fetch('/health');
   *   return Date.now() - start; // Return latency in ms
   * }
   *
   * const checker = createHealthChecker(selector, shards, checkReplica, 10000);
   *
   * // Start periodic checks
   * checker.start();
   *
   * // Manual check
   * await checker.checkAll();
   *
   * // Stop when done
   * checker.stop();
   * ```
   */
  HealthChecker,

  /**
   * Factory function to create a replica selector.
   *
   * @param shards - Shard configurations with replica definitions
   * @param config - Optional health tracking configuration
   * @param currentRegion - Optional current region for nearest selection
   * @returns DefaultReplicaSelector instance
   *
   * @example
   * ```typescript
   * const selector = createReplicaSelector(shards, {
   *   failureThreshold: 3,
   * }, request.cf?.colo);
   * ```
   */
  createReplicaSelector,
  /**
   * Factory function to create a health checker.
   *
   * @param selector - Replica selector to update with health results
   * @param shards - Shard configurations
   * @param checkFn - Function to check replica health (returns latency in ms)
   * @param intervalMs - Check interval (default: 10000ms)
   * @returns HealthChecker instance
   *
   * @example
   * ```typescript
   * const checker = createHealthChecker(
   *   selector,
   *   shards,
   *   async (shardId, replicaId) => {
   *     const start = performance.now();
   *     await pingReplica(replicaId);
   *     return performance.now() - start;
   *   },
   *   5000 // Check every 5 seconds
   * );
   * ```
   */
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
 * Configuration for the sharding system.
 *
 * Provides all necessary configuration to create a sharding client including
 * the VSchema topology, RPC implementation, and optional tuning parameters.
 *
 * @example
 * ```typescript
 * const config: ShardingConfig = {
 *   vschema: createVSchema({
 *     users: shardedTable('tenant_id', hashVindex()),
 *     countries: referenceTable(),
 *   }, [
 *     shard(createShardId('shard-1'), 'user-do'),
 *     shard(createShardId('shard-2'), 'user-do'),
 *   ]),
 *   rpc: new MyShardRPC(env),
 *   currentRegion: request.cf?.colo,
 *   executor: {
 *     maxParallelShards: 16,
 *     defaultTimeoutMs: 5000,
 *     failFast: false,
 *   },
 *   health: {
 *     failureThreshold: 5,
 *     circuitResetMs: 30000,
 *   },
 * };
 * ```
 */
export interface ShardingConfig {
  /**
   * VSchema defining the sharding topology.
   * Contains table configurations, shard definitions, and routing rules.
   */
  vschema: VSchema;
  /**
   * RPC implementation for shard communication.
   * Must implement the ShardRPC interface to communicate with Durable Objects.
   */
  rpc: ShardRPC;
  /**
   * Current region for nearest replica selection.
   * Typically from `request.cf?.colo` in Cloudflare Workers.
   */
  currentRegion?: string;
  /**
   * Executor configuration for parallel execution and retry behavior.
   */
  executor?: ExecutorConfig;
  /**
   * Health tracking configuration for replica management.
   */
  health?: HealthConfig;
}

/**
 * High-level sharding client for distributed SQL queries.
 *
 * Provides a simple interface for executing SQL queries across a sharded database
 * with automatic routing, execution, and result merging.
 *
 * @example Basic usage
 * ```typescript
 * const client = createShardingClient(config);
 *
 * // Single-shard query (routed efficiently)
 * const users = await client.query(
 *   'SELECT * FROM users WHERE tenant_id = $1',
 *   [123]
 * );
 *
 * // Scatter query with read preference
 * const counts = await client.query(
 *   'SELECT tenant_id, COUNT(*) FROM users GROUP BY tenant_id',
 *   [],
 *   'replicaPreferred'
 * );
 *
 * // Streaming results for large datasets
 * for await (const row of client.queryStream('SELECT * FROM users')) {
 *   processRow(row);
 * }
 * ```
 *
 * @example Accessing internal components
 * ```typescript
 * // Get routing decision without executing
 * const decision = client.router.route('SELECT * FROM users WHERE id = 1');
 *
 * // Check cluster health
 * const health = client.replicaSelector.getClusterHealth();
 *
 * // Access circuit breaker state
 * const circuitState = client.executor.getCircuitState('shard-1');
 * ```
 */
export interface ShardingClient {
  /**
   * Execute a SQL query across shards.
   *
   * The query is automatically analyzed to determine optimal routing:
   * - Single-shard queries are routed directly
   * - Scatter queries execute in parallel with result merging
   * - Aggregates use two-phase aggregation for correctness
   *
   * @param sql - SQL query string (supports $1, $2, etc. placeholders)
   * @param params - Optional query parameters
   * @param readPreference - Optional read preference for replica selection
   * @returns Merged result from all contributing shards
   *
   * @example
   * ```typescript
   * // Simple query
   * const result = await client.query('SELECT * FROM users WHERE id = $1', [123]);
   *
   * // With read preference
   * const analytics = await client.query(
   *   'SELECT COUNT(*) FROM large_table',
   *   [],
   *   'analytics'
   * );
   * ```
   */
  query(sql: string, params?: unknown[], readPreference?: ReadPreference): Promise<MergedResult>;

  /**
   * Execute a SQL query with streaming results.
   *
   * For single-shard queries, streams directly from the shard.
   * For scatter queries, collects all results before streaming.
   *
   * @param sql - SQL query string
   * @param params - Optional query parameters
   * @param readPreference - Optional read preference
   * @yields Individual result rows
   *
   * @example
   * ```typescript
   * // Stream large result set
   * let count = 0;
   * for await (const row of client.queryStream('SELECT * FROM events')) {
   *   processEvent(row);
   *   count++;
   *   if (count % 1000 === 0) {
   *     console.log(`Processed ${count} rows`);
   *   }
   * }
   * ```
   */
  queryStream(sql: string, params?: unknown[], readPreference?: ReadPreference): AsyncIterable<unknown[]>;

  /**
   * The query router for analyzing queries and creating execution plans.
   * Use for advanced scenarios like getting routing decisions without executing.
   */
  router: QueryRouter;

  /**
   * The replica selector for health tracking and replica selection.
   * Use for monitoring cluster health or manual health management.
   */
  replicaSelector: DefaultReplicaSelector;

  /**
   * The distributed executor for parallel query execution.
   * Use for advanced scenarios like circuit breaker management.
   */
  executor: DistributedExecutor;
}

/**
 * Create a sharding client for distributed SQL queries.
 *
 * This is the main entry point for the sharding module. It creates a fully
 * configured client that can execute SQL queries across a sharded database
 * with automatic routing, replica selection, and result merging.
 *
 * @param config - Sharding configuration including VSchema, RPC, and options
 * @returns A ShardingClient instance ready to execute queries
 *
 * @example Basic setup
 * ```typescript
 * import {
 *   createShardingClient,
 *   createVSchema,
 *   shardedTable,
 *   referenceTable,
 *   hashVindex,
 *   shard,
 *   createShardId,
 * } from '@dosql/sharding';
 *
 * const client = createShardingClient({
 *   vschema: createVSchema({
 *     users: shardedTable('tenant_id', hashVindex()),
 *     countries: referenceTable(),
 *   }, [
 *     shard(createShardId('shard-1'), 'user-do'),
 *     shard(createShardId('shard-2'), 'user-do'),
 *   ]),
 *   rpc: new MyShardRPC(env),
 * });
 *
 * // Execute queries
 * const result = await client.query(
 *   'SELECT * FROM users WHERE tenant_id = $1',
 *   [123]
 * );
 * ```
 *
 * @example Full configuration in Cloudflare Worker
 * ```typescript
 * export default {
 *   async fetch(request: Request, env: Env): Promise<Response> {
 *     const client = createShardingClient({
 *       vschema: getVSchema(),
 *       rpc: new DurableObjectRPC(env),
 *       currentRegion: request.cf?.colo as string,
 *       executor: {
 *         maxParallelShards: 8,
 *         defaultTimeoutMs: 5000,
 *         failFast: false,
 *         retry: { maxAttempts: 3, backoffMs: 100, maxBackoffMs: 2000 },
 *         circuitBreaker: { failureThreshold: 5, resetTimeoutMs: 30000 },
 *       },
 *       health: {
 *         failureThreshold: 3,
 *         successThreshold: 2,
 *       },
 *     });
 *
 *     const tenantId = getTenantFromRequest(request);
 *     const users = await client.query(
 *       'SELECT * FROM users WHERE tenant_id = $1 LIMIT 100',
 *       [tenantId],
 *       'replicaPreferred'
 *     );
 *
 *     return Response.json(users.rows);
 *   },
 * };
 * ```
 *
 * @example Error handling
 * ```typescript
 * try {
 *   const result = await client.query('SELECT * FROM users');
 *
 *   // Check for partial failures
 *   if (result.partialFailures) {
 *     console.warn('Some shards failed:', result.partialFailures);
 *   }
 *
 *   return result.rows;
 * } catch (err) {
 *   if (err instanceof ShardExecutionError) {
 *     // All shards failed (when failFast: true)
 *     console.error('Query failed:', err.shardErrors);
 *   }
 *   throw err;
 * }
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
 * Create a type-safe sharding client with compile-time query type detection.
 *
 * This advanced API provides TypeScript type inference for your database schema,
 * enabling compile-time detection of query routing types. While the runtime
 * behavior is identical to `createShardingClient`, this version carries schema
 * type information that can be used for advanced type checking.
 *
 * @template Schema - Database schema type mapping table names to typed table schemas
 * @param config - Sharding configuration
 * @returns A ShardingClient with schema type information
 *
 * @example Type-safe schema definition
 * ```typescript
 * import { createTypedShardingClient, TypedTableSchema } from '@dosql/sharding';
 *
 * // Define your schema with TypeScript types
 * interface MySchema {
 *   users: {
 *     columns: {
 *       id: 'number';
 *       tenant_id: 'number';
 *       name: 'string';
 *       email: 'string';
 *       created_at: 'Date';
 *     };
 *     shardKey: 'tenant_id';
 *   };
 *   orders: {
 *     columns: {
 *       id: 'number';
 *       user_id: 'number';
 *       total: 'number';
 *     };
 *     shardKey: 'user_id';
 *   };
 *   countries: {
 *     columns: { code: 'string'; name: 'string' };
 *     shardKey: undefined; // Reference table, no shard key
 *   };
 * }
 *
 * const client = createTypedShardingClient<MySchema>(config);
 *
 * // TypeScript can infer that this is a single-shard query
 * // because it includes equality on the shard key (tenant_id)
 * const singleShardResult = await client.query(
 *   'SELECT * FROM users WHERE tenant_id = 1'
 * );
 *
 * // TypeScript knows this will be a scatter query
 * // because there's no shard key filter
 * const scatterResult = await client.query(
 *   'SELECT COUNT(*) FROM users'
 * );
 * ```
 *
 * @example Compile-time query type detection
 * ```typescript
 * import { DetectQueryType, HasEqualityOnColumn } from '@dosql/sharding';
 *
 * // These types are computed at compile time
 * type Query1Type = DetectQueryType<
 *   'SELECT * FROM users WHERE tenant_id = 1',
 *   MySchema,
 *   'users'
 * >; // Type: 'single-shard'
 *
 * type Query2Type = DetectQueryType<
 *   'SELECT * FROM users WHERE name = "Alice"',
 *   MySchema,
 *   'users'
 * >; // Type: 'scatter' (name is not the shard key)
 *
 * // Check if a query has equality on a specific column
 * type HasTenantFilter = HasEqualityOnColumn<
 *   'SELECT * FROM users WHERE tenant_id = 1 AND active = true',
 *   'tenant_id'
 * >; // Type: true
 * ```
 */
export function createTypedShardingClient<Schema extends Record<string, { columns: Record<string, string>; shardKey: string | undefined }>>(
  config: ShardingConfig
): ShardingClient {
  return createShardingClient(config);
}
