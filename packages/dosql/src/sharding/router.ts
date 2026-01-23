/**
 * DoSQL Query Router
 *
 * Routes SQL queries to the appropriate shard(s) based on:
 * - Shard key extraction from WHERE clause
 * - VSchema configuration
 * - Cost-based decision making
 *
 * Key improvements over Vitess:
 * - Real SQL parsing (not regex)
 * - Cost-based query routing
 * - Type-safe shard keys
 *
 * @packageDocumentation
 */

import type {
  VSchema,
  TableShardingConfig,
  ShardConfig,
  RoutingDecision,
  ExtractedShardKey,
  QueryType,
  QueryOperation,
  ReadPreference,
  ExecutionPlan,
  ShardExecutionPlan,
  PostProcessingOp,
  SortColumn,
  AggregateOp,
} from './types.js';

import { createVindex, type Vindex } from './vindex.js';
// Import from the dedicated parser module
import {
  SQLParser,
  type ParsedQuery,
  type TableReference,
  type ColumnReference,
  type WhereClause,
  type WhereCondition,
  tokenizeSQL,
  stripComments,
  extractShardKeyFromTokens,
} from './parser/index.js';

// =============================================================================
// RE-EXPORT SQL PARSING TYPES AND CLASS FROM PARSER MODULE
// =============================================================================

// Re-export types for backward compatibility
export type { ParsedQuery, TableReference, ColumnReference, WhereClause, WhereCondition } from './parser/index.js';

// Re-export SQLParser class for backward compatibility
export { SQLParser } from './parser/index.js';

// =============================================================================
// QUERY ROUTER
// =============================================================================

/**
 * Query router - routes SQL queries to appropriate shards
 */
export class QueryRouter {
  private readonly vschema: VSchema;
  private readonly vindexes: Map<string, Vindex>;
  private readonly parser: SQLParser;

  constructor(vschema: VSchema) {
    this.vschema = vschema;
    this.parser = new SQLParser();
    this.vindexes = new Map();

    // Pre-create vindexes for sharded tables
    for (const [tableName, config] of Object.entries(vschema.tables)) {
      if (config.type === 'sharded') {
        this.vindexes.set(tableName, createVindex(vschema.shards, config.vindex));
      }
    }
  }

  /**
   * Route a SQL query to target shards
   */
  route(sql: string, params?: unknown[], readPreference?: ReadPreference): RoutingDecision {
    const parsed = this.parser.parse(sql);
    const tableName = parsed.tables[0]?.name;

    if (!tableName) {
      throw new Error('Could not determine target table from query');
    }

    const tableConfig = this.vschema.tables[tableName];
    if (!tableConfig) {
      throw new Error(`Table '${tableName}' not found in VSchema`);
    }

    return this.routeForTable(parsed, tableConfig, tableName, params, readPreference);
  }

  private routeForTable(
    parsed: ParsedQuery,
    config: TableShardingConfig,
    tableName: string,
    params?: unknown[],
    readPreference?: ReadPreference
  ): RoutingDecision {
    // Determine read preference based on operation
    const isWrite = parsed.operation !== 'SELECT';
    const effectiveReadPref = isWrite ? 'primary' : (readPreference ?? 'primaryPreferred');
    const canUseReplica = !isWrite && effectiveReadPref !== 'primary';

    switch (config.type) {
      case 'unsharded':
        return this.routeUnsharded(config, effectiveReadPref, canUseReplica);

      case 'reference':
        return this.routeReference(parsed, config, effectiveReadPref);

      case 'sharded':
        return this.routeSharded(parsed, config, tableName, params, effectiveReadPref, canUseReplica);

      default:
        throw new Error(`Unknown table type: ${(config as TableShardingConfig).type}`);
    }
  }

  private routeUnsharded(
    config: { type: 'unsharded'; shard?: string },
    readPreference: ReadPreference,
    canUseReplica: boolean
  ): RoutingDecision {
    const targetShard = config.shard ?? this.vschema.defaultShard ?? this.vschema.shards[0].id;

    return {
      queryType: 'single-shard',
      targetShards: [targetShard],
      readPreference,
      canUseReplica,
      costEstimate: 1,
      reason: 'Unsharded table routes to single shard',
    };
  }

  private routeReference(
    parsed: ParsedQuery,
    _config: { type: 'reference'; readOnly?: boolean },
    readPreference: ReadPreference
  ): RoutingDecision {
    if (parsed.operation !== 'SELECT') {
      // Writes go to all shards for reference tables
      return {
        queryType: 'scatter',
        targetShards: this.vschema.shards.map(s => s.id),
        readPreference: 'primary',
        canUseReplica: false,
        costEstimate: this.vschema.shards.length,
        reason: 'Reference table write broadcasts to all shards',
      };
    }

    // Reads can go to any single shard
    const targetShard = this.selectNearestShard();

    return {
      queryType: 'single-shard',
      targetShards: [targetShard],
      readPreference,
      canUseReplica: true,
      costEstimate: 1,
      reason: 'Reference table read uses nearest shard',
    };
  }

  private routeSharded(
    parsed: ParsedQuery,
    config: { type: 'sharded'; shardKey: string; vindex: unknown },
    tableName: string,
    params: unknown[] | undefined,
    readPreference: ReadPreference,
    canUseReplica: boolean
  ): RoutingDecision {
    const vindex = this.vindexes.get(tableName);
    if (!vindex) {
      throw new Error(`No vindex found for table '${tableName}'`);
    }

    // Extract shard key from WHERE clause
    const extracted = this.extractShardKey(parsed, config.shardKey, params);

    if (extracted.method === 'equality' && extracted.values.length === 1) {
      // Single shard key value - route to single shard
      const targetShard = vindex.getShard(extracted.values[0]);

      return {
        queryType: 'single-shard',
        targetShards: [targetShard],
        shardKeyValue: extracted.values[0],
        readPreference,
        canUseReplica,
        costEstimate: 1,
        reason: `Shard key equality on '${config.shardKey}'`,
      };
    }

    if (extracted.method === 'in-list' && extracted.values.length > 0) {
      // Multiple shard key values - route to subset of shards
      const targetShards = vindex.getShardsForKeys(extracted.values);

      return {
        queryType: targetShards.length === 1 ? 'single-shard' : 'scatter-gather',
        targetShards,
        shardKeyValue: extracted.values,
        readPreference,
        canUseReplica,
        costEstimate: targetShards.length,
        reason: `IN list on shard key '${config.shardKey}' targets ${targetShards.length} shard(s)`,
      };
    }

    if (extracted.method === 'range') {
      // Range query on shard key
      const targetShards = vindex.getShardsForRange(
        extracted.values[0],
        extracted.values[1]
      );

      return {
        queryType: targetShards.length === 1 ? 'single-shard' : 'scatter-gather',
        targetShards,
        readPreference,
        canUseReplica,
        costEstimate: targetShards.length,
        reason: `Range query on shard key '${config.shardKey}' targets ${targetShards.length} shard(s)`,
      };
    }

    // No shard key in WHERE - must scatter to all shards
    const allShards = vindex.getAllShards();

    return {
      queryType: 'scatter',
      targetShards: allShards,
      readPreference,
      canUseReplica,
      costEstimate: allShards.length * 10, // Higher cost for scatter queries
      reason: 'No shard key in WHERE clause - scatter query required',
    };
  }

  private extractShardKey(
    parsed: ParsedQuery,
    shardKeyColumn: string,
    params?: unknown[],
    originalSql?: string
  ): ExtractedShardKey {
    // If we have the original SQL, use tokenizer-based extraction for more accuracy
    if (originalSql) {
      const tokens = stripComments(tokenizeSQL(originalSql));
      const result = extractShardKeyFromTokens(tokens, shardKeyColumn);

      if (result) {
        const resolveValue = (val: unknown): unknown => {
          if (typeof val === 'object' && val !== null && 'placeholder' in val) {
            const placeholder = (val as { placeholder: string }).placeholder;
            if (placeholder.startsWith('$')) {
              const index = parseInt(placeholder.slice(1), 10) - 1;
              return params?.[index];
            }
          }
          return val;
        };

        if (result.method === 'equality') {
          const resolvedValue = resolveValue(result.value);
          if (resolvedValue !== undefined) {
            return {
              column: shardKeyColumn,
              values: [resolvedValue],
              method: 'equality',
            };
          }
        } else if (result.method === 'in-list' && result.values) {
          const resolvedValues = result.values.map(resolveValue).filter(v => v !== undefined);
          if (resolvedValues.length > 0) {
            return {
              column: shardKeyColumn,
              values: resolvedValues,
              method: 'in-list',
            };
          }
        }
      }
    }

    // Fallback to parsed WHERE clause
    if (!parsed.where) {
      return { column: shardKeyColumn, values: [], method: 'none' };
    }

    // Only consider AND conditions for shard key extraction
    if (parsed.where.operator !== 'AND') {
      // OR conditions cannot be optimized for single shard
      return { column: shardKeyColumn, values: [], method: 'none' };
    }

    for (const condition of parsed.where.conditions) {
      if (condition.column.toLowerCase() !== shardKeyColumn.toLowerCase()) {
        continue;
      }

      // Resolve placeholders to actual values
      const resolveValue = (val: unknown): unknown => {
        if (typeof val === 'object' && val !== null && 'placeholder' in val) {
          const placeholder = (val as { placeholder: string }).placeholder;
          if (placeholder.startsWith('$')) {
            const index = parseInt(placeholder.slice(1), 10) - 1;
            return params?.[index];
          }
          // Handle other placeholder types as needed
        }
        return val;
      };

      switch (condition.operator) {
        case '=':
          const eqValue = resolveValue(condition.value);
          if (eqValue !== undefined) {
            return {
              column: shardKeyColumn,
              values: [eqValue],
              method: 'equality',
            };
          }
          break;

        case 'IN':
          if (condition.values && condition.values.length > 0) {
            const resolvedValues = condition.values.map(resolveValue).filter(v => v !== undefined);
            if (resolvedValues.length > 0) {
              return {
                column: shardKeyColumn,
                values: resolvedValues,
                method: 'in-list',
              };
            }
          }
          break;

        case 'BETWEEN':
          const minVal = resolveValue(condition.minValue);
          const maxVal = resolveValue(condition.maxValue);
          if (minVal !== undefined && maxVal !== undefined) {
            return {
              column: shardKeyColumn,
              values: [minVal, maxVal],
              method: 'range',
            };
          }
          break;
      }
    }

    return { column: shardKeyColumn, values: [], method: 'none' };
  }

  /**
   * Select the nearest shard based on available information.
   *
   * Selection strategy (in order of preference):
   * 1. If request region is known and a shard has a matching region, use it
   * 2. If shards have weights defined, use weighted random selection for load balancing
   * 3. Fall back to random selection across all shards for even distribution
   *
   * @param requestRegion - Optional region hint from the incoming request (e.g., from cf.colo)
   * @returns The selected shard ID
   */
  private selectNearestShard(requestRegion?: string): string {
    const shards = this.vschema.shards;

    if (shards.length === 0) {
      throw new Error('No shards configured in VSchema');
    }

    if (shards.length === 1) {
      return shards[0].id;
    }

    // Strategy 1: Match by region if request region is available
    if (requestRegion) {
      const regionLower = requestRegion.toLowerCase();

      // Check for exact region match in shard metadata or replicas
      for (const shard of shards) {
        // Check shard metadata for region
        if (shard.metadata?.['region'] && String(shard.metadata['region']).toLowerCase() === regionLower) {
          return shard.id;
        }

        // Check replicas for region match
        if (shard.replicas) {
          for (const replica of shard.replicas) {
            if (replica.region && replica.region.toLowerCase() === regionLower) {
              return shard.id;
            }
          }
        }
      }

      // Check for partial region match (e.g., 'us' matches 'us-west', 'us-east')
      for (const shard of shards) {
        if (shard.metadata?.['region'] && String(shard.metadata['region']).toLowerCase().startsWith(regionLower.slice(0, 2))) {
          return shard.id;
        }

        if (shard.replicas) {
          for (const replica of shard.replicas) {
            if (replica.region && replica.region.toLowerCase().startsWith(regionLower.slice(0, 2))) {
              return shard.id;
            }
          }
        }
      }
    }

    // Strategy 2: Weighted random selection if weights are defined
    const shardsWithWeights = shards.filter(s => s.replicas?.some(r => r.weight !== undefined && r.weight > 0));
    if (shardsWithWeights.length > 0) {
      const totalWeight = shardsWithWeights.reduce(
        (sum, s) => sum + (s.replicas?.reduce((w, r) => w + (r.weight ?? 1), 0) ?? 1),
        0
      );
      let random = Math.random() * totalWeight;

      for (const shard of shardsWithWeights) {
        const shardWeight = shard.replicas?.reduce((w, r) => w + (r.weight ?? 1), 0) ?? 1;
        random -= shardWeight;
        if (random <= 0) {
          return shard.id;
        }
      }
    }

    // Strategy 3: Random selection for even distribution
    const randomIndex = Math.floor(Math.random() * shards.length);
    return shards[randomIndex].id;
  }

  /**
   * Create a complete execution plan for a query
   */
  createExecutionPlan(sql: string, params?: unknown[], readPreference?: ReadPreference): ExecutionPlan {
    const parsed = this.parser.parse(sql);
    const routing = this.route(sql, params, readPreference);

    const shardPlans: ShardExecutionPlan[] = routing.targetShards.map(shardId => ({
      shardId,
      sql: this.rewriteQueryForShard(sql, parsed, routing),
      params,
      isFinal: routing.queryType === 'single-shard',
    }));

    const postProcessing = this.determinePostProcessing(parsed, routing);

    return {
      sql,
      routing,
      shardPlans,
      postProcessing,
      totalCost: routing.costEstimate,
    };
  }

  private rewriteQueryForShard(
    sql: string,
    parsed: ParsedQuery,
    routing: RoutingDecision
  ): string {
    // For scatter-gather queries with aggregates, rewrite to compute partial aggregates
    if (
      routing.queryType === 'scatter' ||
      routing.queryType === 'scatter-gather'
    ) {
      if (parsed.aggregates && parsed.aggregates.length > 0) {
        return this.rewriteForPartialAggregation(sql, parsed);
      }

      // For ORDER BY + LIMIT, each shard needs to return enough rows
      if (parsed.orderBy && parsed.limit) {
        const totalNeeded = (parsed.limit ?? 0) + (parsed.offset ?? 0);
        return sql.replace(
          /LIMIT\s+\d+/i,
          `LIMIT ${totalNeeded}`
        ).replace(
          /OFFSET\s+\d+/i,
          ''
        );
      }
    }

    return sql;
  }

  private rewriteForPartialAggregation(sql: string, parsed: ParsedQuery): string {
    // For AVG, we need to compute SUM and COUNT separately
    // This is a simplified rewrite - a real implementation would be more sophisticated

    let rewritten = sql;

    for (const agg of parsed.aggregates ?? []) {
      if (agg.function === 'AVG') {
        // Replace AVG(col) with SUM(col) AS _sum_col, COUNT(col) AS _count_col
        const avgPattern = new RegExp(`AVG\\s*\\(\\s*${agg.column}\\s*\\)`, 'gi');
        rewritten = rewritten.replace(
          avgPattern,
          `SUM(${agg.column}) AS _sum_${agg.column}, COUNT(${agg.column}) AS _count_${agg.column}`
        );
      }
    }

    return rewritten;
  }

  private determinePostProcessing(
    parsed: ParsedQuery,
    routing: RoutingDecision
  ): PostProcessingOp[] | undefined {
    if (routing.queryType === 'single-shard') {
      return undefined;
    }

    const ops: PostProcessingOp[] = [{ type: 'merge' }];

    // Add aggregate post-processing
    if (parsed.aggregates && parsed.aggregates.length > 0) {
      ops.push({ type: 'aggregate', aggregates: parsed.aggregates });
    }

    // Add distinct post-processing
    if (parsed.distinct && parsed.columns) {
      ops.push({
        type: 'distinct',
        columns: parsed.columns.map(c => c.alias ?? c.name),
      });
    }

    // Add sort post-processing
    if (parsed.orderBy) {
      ops.push({ type: 'sort', columns: parsed.orderBy });
    }

    // Add limit post-processing
    if (parsed.limit) {
      ops.push({ type: 'limit', count: parsed.limit, offset: parsed.offset });
    }

    return ops.length > 1 ? ops : undefined;
  }

  /**
   * Get the table configuration for a table
   */
  getTableConfig(tableName: string): TableShardingConfig | undefined {
    return this.vschema.tables[tableName];
  }

  /**
   * Get all shard configurations
   */
  getShards(): readonly ShardConfig[] {
    return this.vschema.shards;
  }

  /**
   * Get shard configuration by ID
   */
  getShard(shardId: string): ShardConfig | undefined {
    return this.vschema.shards.find(s => s.id === shardId);
  }
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a query router from a VSchema
 */
export function createRouter(vschema: VSchema): QueryRouter {
  return new QueryRouter(vschema);
}
