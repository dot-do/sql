/**
 * Sharded Aggregator for DoLake
 *
 * Solves the single-aggregator scalability bottleneck by partitioning tables
 * across multiple DoLake Durable Object instances. Each shard handles a subset
 * of tables, allowing DoLake to scale beyond ~100K events/sec.
 *
 * Architecture:
 * ```
 * ┌────────────────────────────────────────────────────────────────┐
 * │                    CDC Event Sources                           │
 * │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐          │
 * │  │ DoSQL 1 │  │ DoSQL 2 │  │ DoSQL 3 │  │ DoSQL N │          │
 * │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘          │
 * │       │            │            │            │                │
 * │       ▼            ▼            ▼            ▼                │
 * │  ┌───────────────────────────────────────────────────┐        │
 * │  │          ShardedAggregatorRouter                   │        │
 * │  │  - Hashes table name → shard index                 │        │
 * │  │  - Routes CDC events to correct shard              │        │
 * │  │  - Broadcasts cross-shard queries                  │        │
 * │  └──────┬──────────┬──────────┬──────────┬───────────┘        │
 * │         │          │          │          │                    │
 * │         ▼          ▼          ▼          ▼                    │
 * │  ┌──────────┐┌──────────┐┌──────────┐┌──────────┐            │
 * │  │ Shard 0  ││ Shard 1  ││ Shard 2  ││ Shard N  │            │
 * │  │ (DoLake) ││ (DoLake) ││ (DoLake) ││ (DoLake) │            │
 * │  │ tables:  ││ tables:  ││ tables:  ││ tables:  │            │
 * │  │ users,   ││ orders,  ││ events,  ││ metrics, │            │
 * │  │ profiles ││ payments ││ logs     ││ sessions │            │
 * │  └──────────┘└──────────┘└──────────┘└──────────┘            │
 * └────────────────────────────────────────────────────────────────┘
 * ```
 *
 * Key features:
 * - Consistent hashing of table names to shard indices
 * - Backward compatible: shardCount=1 behaves identically to single DO
 * - Supports explicit table-to-shard pinning for co-location
 * - Cross-shard status aggregation
 * - Graceful shard count changes with table reassignment tracking
 *
 * @module sharded-aggregator
 */

import type { CDCEvent, FlushResult, FlushTrigger } from './types.js';
import { fnv1a } from '@dotdo/sql-types';

// =============================================================================
// Configuration
// =============================================================================

/**
 * Configuration for sharded aggregation.
 */
export interface ShardedAggregatorConfig {
  /** Number of shards to distribute tables across (default: 1 for backward compat) */
  shardCount: number;

  /** Maximum number of shards allowed */
  maxShards: number;

  /**
   * Explicit table-to-shard assignments for co-location.
   * Tables not listed here are routed via consistent hashing.
   */
  tablePinning: Record<string, number>;

  /**
   * Prefix used when generating shard DO names.
   * Shard names are: `${prefix}-${shardIndex}`
   */
  shardNamePrefix: string;
}

/**
 * Default sharded aggregator configuration.
 * shardCount=1 maintains full backward compatibility with single-DO deployments.
 */
export const DEFAULT_SHARDED_AGGREGATOR_CONFIG: Readonly<ShardedAggregatorConfig> = {
  shardCount: 1,
  maxShards: 32,
  tablePinning: {},
  shardNamePrefix: 'dolake-shard',
};

// =============================================================================
// Shard Info
// =============================================================================

/**
 * Information about a specific shard.
 */
export interface ShardInfo {
  /** Shard index (0-based) */
  index: number;

  /** Generated shard name for DO identification */
  name: string;

  /** Tables currently assigned to this shard */
  tables: Set<string>;

  /** Number of events routed to this shard */
  eventsRouted: number;

  /** Last time an event was routed to this shard */
  lastEventTime: number;
}

/**
 * Result of routing a batch of CDC events across shards.
 */
export interface ShardRoutingResult {
  /** Map of shard index to the events routed to that shard */
  shardEvents: Map<number, CDCEvent[]>;

  /** Number of distinct shards that received events */
  shardsUsed: number;

  /** Total events routed */
  totalEventsRouted: number;

  /** Per-table shard assignment used */
  tableShardMap: Map<string, number>;
}

/**
 * Aggregated status from all shards.
 */
export interface AggregatedShardStatus {
  /** Total number of shards */
  shardCount: number;

  /** Number of shards with assigned tables */
  activeShardsCount: number;

  /** Total tables across all shards */
  totalTables: number;

  /** Total events routed across all shards */
  totalEventsRouted: number;

  /** Per-shard breakdown */
  shards: Array<{
    index: number;
    name: string;
    tableCount: number;
    eventsRouted: number;
    lastEventTime: number;
  }>;

  /** Whether running in single-shard (backward-compatible) mode */
  isSingleShardMode: boolean;
}

// =============================================================================
// Hash Function
// =============================================================================

/**
 * Compute a deterministic hash for a string.
 * Uses FNV-1a for fast, well-distributed hashing.
 *
 * Re-exported from @dotdo/sql-types for backward compatibility.
 */
export const fnv1aHash = fnv1a;

/**
 * Map a table name to a shard index using consistent hashing.
 *
 * @param tableName - The table name to hash
 * @param shardCount - Number of available shards
 * @returns Shard index (0-based)
 */
export function tableToShardIndex(tableName: string, shardCount: number): number {
  if (shardCount <= 1) return 0;
  return fnv1a(tableName) % shardCount;
}

// =============================================================================
// ShardedAggregatorRouter
// =============================================================================

/**
 * Routes CDC events to the appropriate DoLake shard based on table name.
 *
 * In single-shard mode (shardCount=1), all events go to shard 0, which is
 * equivalent to the existing single-DO behavior.
 *
 * @example
 * ```typescript
 * // Single-shard (backward compatible)
 * const router = new ShardedAggregatorRouter({ shardCount: 1 });
 *
 * // Multi-shard (scaled)
 * const router = new ShardedAggregatorRouter({
 *   shardCount: 4,
 *   tablePinning: { 'users': 0, 'profiles': 0 }, // co-locate related tables
 * });
 *
 * const result = router.routeEvents(cdcEvents);
 * for (const [shardIndex, events] of result.shardEvents) {
 *   const shardName = router.getShardName(shardIndex);
 *   const doId = env.DOLAKE.idFromName(shardName);
 *   // Forward events to shard DO...
 * }
 * ```
 */
export class ShardedAggregatorRouter {
  private readonly config: ShardedAggregatorConfig;
  private readonly shards: Map<number, ShardInfo>;
  private readonly tableToShard: Map<string, number>;

  constructor(config: Partial<ShardedAggregatorConfig> = {}) {
    this.config = { ...DEFAULT_SHARDED_AGGREGATOR_CONFIG, ...config };

    // Clamp shard count
    if (this.config.shardCount < 1) {
      this.config.shardCount = 1;
    }
    if (this.config.shardCount > this.config.maxShards) {
      this.config.shardCount = this.config.maxShards;
    }

    this.shards = new Map();
    this.tableToShard = new Map();

    // Initialize shard info
    for (let i = 0; i < this.config.shardCount; i++) {
      this.shards.set(i, {
        index: i,
        name: `${this.config.shardNamePrefix}-${i}`,
        tables: new Set(),
        eventsRouted: 0,
        lastEventTime: 0,
      });
    }

    // Apply explicit table pinning
    for (const [table, shardIndex] of Object.entries(this.config.tablePinning)) {
      if (shardIndex >= 0 && shardIndex < this.config.shardCount) {
        this.tableToShard.set(table, shardIndex);
        this.shards.get(shardIndex)?.tables.add(table);
      }
    }
  }

  /**
   * Get the shard index for a given table name.
   *
   * First checks explicit pinning, then falls back to consistent hashing.
   */
  getShardForTable(tableName: string): number {
    // Check explicit pinning first
    const pinned = this.tableToShard.get(tableName);
    if (pinned !== undefined) {
      return pinned;
    }

    // Hash-based routing
    const shardIndex = tableToShardIndex(tableName, this.config.shardCount);

    // Cache the assignment
    this.tableToShard.set(tableName, shardIndex);
    this.shards.get(shardIndex)?.tables.add(tableName);

    return shardIndex;
  }

  /**
   * Route a batch of CDC events to appropriate shards.
   *
   * Events are grouped by their table's shard assignment.
   */
  routeEvents(events: CDCEvent[]): ShardRoutingResult {
    const shardEvents = new Map<number, CDCEvent[]>();
    const tableShardMap = new Map<string, number>();
    const now = Date.now();

    for (const event of events) {
      const shardIndex = this.getShardForTable(event.table);
      tableShardMap.set(event.table, shardIndex);

      let shardBatch = shardEvents.get(shardIndex);
      if (!shardBatch) {
        shardBatch = [];
        shardEvents.set(shardIndex, shardBatch);
      }
      shardBatch.push(event);

      // Update shard stats
      const shard = this.shards.get(shardIndex);
      if (shard) {
        shard.eventsRouted++;
        shard.lastEventTime = now;
      }
    }

    return {
      shardEvents,
      shardsUsed: shardEvents.size,
      totalEventsRouted: events.length,
      tableShardMap,
    };
  }

  /**
   * Get the DO name for a shard index.
   */
  getShardName(shardIndex: number): string {
    const shard = this.shards.get(shardIndex);
    if (!shard) {
      throw new Error(`Invalid shard index: ${shardIndex}. Valid range: 0-${this.config.shardCount - 1}`);
    }
    return shard.name;
  }

  /**
   * Get information about a specific shard.
   */
  getShardInfo(shardIndex: number): ShardInfo | undefined {
    return this.shards.get(shardIndex);
  }

  /**
   * Get aggregated status across all shards.
   */
  getStatus(): AggregatedShardStatus {
    const shardStatuses: AggregatedShardStatus['shards'] = [];
    let totalTables = 0;
    let totalEventsRouted = 0;
    let activeShardsCount = 0;

    for (const [index, shard] of this.shards) {
      const tableCount = shard.tables.size;
      totalTables += tableCount;
      totalEventsRouted += shard.eventsRouted;
      if (tableCount > 0) {
        activeShardsCount++;
      }

      shardStatuses.push({
        index,
        name: shard.name,
        tableCount,
        eventsRouted: shard.eventsRouted,
        lastEventTime: shard.lastEventTime,
      });
    }

    return {
      shardCount: this.config.shardCount,
      activeShardsCount,
      totalTables,
      totalEventsRouted,
      shards: shardStatuses,
      isSingleShardMode: this.config.shardCount === 1,
    };
  }

  /**
   * Get the current shard count.
   */
  getShardCount(): number {
    return this.config.shardCount;
  }

  /**
   * Check whether this router is in single-shard (backward-compatible) mode.
   */
  isSingleShardMode(): boolean {
    return this.config.shardCount === 1;
  }

  /**
   * Get all table-to-shard assignments (both pinned and hash-derived).
   */
  getTableAssignments(): Map<string, number> {
    return new Map(this.tableToShard);
  }

  /**
   * Get the configuration.
   */
  getConfig(): ShardedAggregatorConfig {
    return { ...this.config };
  }

  /**
   * Pin a table to a specific shard (for co-location).
   * This overrides hash-based routing for the specified table.
   */
  pinTable(tableName: string, shardIndex: number): void {
    if (shardIndex < 0 || shardIndex >= this.config.shardCount) {
      throw new Error(
        `Invalid shard index ${shardIndex}. Valid range: 0-${this.config.shardCount - 1}`
      );
    }

    // Remove from old shard if previously assigned
    const oldShard = this.tableToShard.get(tableName);
    if (oldShard !== undefined && oldShard !== shardIndex) {
      this.shards.get(oldShard)?.tables.delete(tableName);
    }

    this.tableToShard.set(tableName, shardIndex);
    this.shards.get(shardIndex)?.tables.add(tableName);
    this.config.tablePinning[tableName] = shardIndex;
  }

  /**
   * Unpin a table (revert to hash-based routing).
   */
  unpinTable(tableName: string): void {
    const oldShard = this.tableToShard.get(tableName);
    if (oldShard !== undefined) {
      this.shards.get(oldShard)?.tables.delete(tableName);
    }
    this.tableToShard.delete(tableName);
    delete this.config.tablePinning[tableName];
  }

  /**
   * Reset all routing statistics (useful for testing).
   */
  resetStats(): void {
    for (const shard of this.shards.values()) {
      shard.eventsRouted = 0;
      shard.lastEventTime = 0;
    }
  }
}

// =============================================================================
// ShardedCDCCoordinator
// =============================================================================

/**
 * Callback type for forwarding events to a shard.
 * Implementations should send events to the actual DoLake DO instance.
 */
export type ShardForwardFn = (
  shardIndex: number,
  shardName: string,
  events: CDCEvent[]
) => Promise<{ success: boolean; eventsAccepted: number; error?: string }>;

/**
 * Result of coordinating a CDC batch across shards.
 */
export interface CoordinatorResult {
  /** Whether all shard forwards succeeded */
  success: boolean;

  /** Total events accepted across all shards */
  totalEventsAccepted: number;

  /** Total events in the original batch */
  totalEvents: number;

  /** Number of shards that received events */
  shardsUsed: number;

  /** Per-shard results */
  shardResults: Array<{
    shardIndex: number;
    shardName: string;
    eventsForwarded: number;
    success: boolean;
    error?: string | undefined;
  }>;

  /** Whether we are in single-shard mode */
  isSingleShardMode: boolean;
}

/**
 * Coordinates forwarding of CDC events to sharded DoLake instances.
 *
 * This is the main entry point for sharded CDC ingestion. It:
 * 1. Routes events by table name to appropriate shards
 * 2. Forwards events to each shard in parallel
 * 3. Aggregates results
 *
 * For single-shard mode, this adds minimal overhead (no hashing needed,
 * all events go to shard 0).
 *
 * @example
 * ```typescript
 * const coordinator = new ShardedCDCCoordinator({ shardCount: 4 });
 *
 * const result = await coordinator.ingestBatch(cdcEvents, async (shardIndex, shardName, events) => {
 *   const doId = env.DOLAKE.idFromName(shardName);
 *   const stub = env.DOLAKE.get(doId);
 *   const response = await stub.fetch('https://dolake/cdc', {
 *     method: 'POST',
 *     body: JSON.stringify({ events }),
 *   });
 *   const body = await response.json();
 *   return { success: body.success, eventsAccepted: body.eventsAccepted };
 * });
 * ```
 */
export class ShardedCDCCoordinator {
  private readonly router: ShardedAggregatorRouter;

  constructor(config: Partial<ShardedAggregatorConfig> = {}) {
    this.router = new ShardedAggregatorRouter(config);
  }

  /**
   * Ingest a batch of CDC events, routing them to appropriate shards.
   *
   * In single-shard mode, this calls forwardFn once with all events.
   * In multi-shard mode, events are grouped by table and forwarded
   * to appropriate shards in parallel.
   */
  async ingestBatch(
    events: CDCEvent[],
    forwardFn: ShardForwardFn
  ): Promise<CoordinatorResult> {
    if (events.length === 0) {
      return {
        success: true,
        totalEventsAccepted: 0,
        totalEvents: 0,
        shardsUsed: 0,
        shardResults: [],
        isSingleShardMode: this.router.isSingleShardMode(),
      };
    }

    // Route events to shards
    const routing = this.router.routeEvents(events);
    const shardResults: CoordinatorResult['shardResults'] = [];
    let totalEventsAccepted = 0;
    let allSuccess = true;

    // Forward to each shard in parallel
    const forwardPromises = Array.from(routing.shardEvents.entries()).map(
      async ([shardIndex, shardEvents]) => {
        const shardName = this.router.getShardName(shardIndex);
        try {
          const result = await forwardFn(shardIndex, shardName, shardEvents);
          return {
            shardIndex,
            shardName,
            eventsForwarded: shardEvents.length,
            success: result.success,
            eventsAccepted: result.eventsAccepted,
            error: result.error,
          };
        } catch (error) {
          return {
            shardIndex,
            shardName,
            eventsForwarded: shardEvents.length,
            success: false,
            eventsAccepted: 0,
            error: String(error),
          };
        }
      }
    );

    const results = await Promise.all(forwardPromises);

    for (const result of results) {
      shardResults.push({
        shardIndex: result.shardIndex,
        shardName: result.shardName,
        eventsForwarded: result.eventsForwarded,
        success: result.success,
        error: result.error,
      });
      totalEventsAccepted += result.eventsAccepted;
      if (!result.success) {
        allSuccess = false;
      }
    }

    return {
      success: allSuccess,
      totalEventsAccepted,
      totalEvents: events.length,
      shardsUsed: routing.shardsUsed,
      shardResults,
      isSingleShardMode: this.router.isSingleShardMode(),
    };
  }

  /**
   * Get the underlying router for direct access.
   */
  getRouter(): ShardedAggregatorRouter {
    return this.router;
  }

  /**
   * Get aggregated status across all shards.
   */
  getStatus(): AggregatedShardStatus {
    return this.router.getStatus();
  }
}
