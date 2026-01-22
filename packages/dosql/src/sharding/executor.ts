/**
 * DoSQL Distributed Executor
 *
 * Executes queries across multiple shards with:
 * - Parallel shard execution
 * - Result streaming (not buffering)
 * - Two-phase aggregation for SUM/COUNT/AVG
 * - ORDER BY + LIMIT post-processing
 *
 * @packageDocumentation
 */

import type {
  ExecutionPlan,
  ShardExecutionPlan,
  ShardResult,
  MergedResult,
  ShardError,
  PostProcessingOp,
  SortColumn,
  AggregateOp,
  ShardConfig,
  ReplicaConfig,
  ReadPreference,
} from './types.js';

import type { ReplicaSelector } from './replica.js';

// =============================================================================
// RPC INTERFACE
// =============================================================================

/**
 * Interface for shard RPC communication
 * Implementations can use CapnWeb, HTTP, or WebSocket
 */
export interface ShardRPC {
  /**
   * Execute a query on a specific shard/replica
   */
  execute(
    shardId: string,
    replicaId: string | undefined,
    sql: string,
    params?: unknown[],
    options?: ExecuteOptions
  ): Promise<ShardResult>;

  /**
   * Execute a query and stream results
   */
  executeStream(
    shardId: string,
    replicaId: string | undefined,
    sql: string,
    params?: unknown[],
    options?: ExecuteOptions
  ): AsyncIterable<ShardResult>;
}

/**
 * Options for query execution
 */
export interface ExecuteOptions {
  /** Query timeout in milliseconds */
  timeoutMs?: number;
  /** Maximum rows to return */
  maxRows?: number;
  /** Whether to include column types in result */
  includeColumnTypes?: boolean;
}

// =============================================================================
// DISTRIBUTED EXECUTOR
// =============================================================================

/**
 * Configuration for the distributed executor
 */
export interface ExecutorConfig {
  /** Maximum parallel shard requests */
  maxParallelShards?: number;
  /** Default timeout for shard requests (ms) */
  defaultTimeoutMs?: number;
  /** Whether to fail fast on first shard error */
  failFast?: boolean;
  /** Retry configuration */
  retry?: {
    maxAttempts: number;
    backoffMs: number;
    maxBackoffMs: number;
  };
}

/**
 * Distributed query executor
 * Handles parallel execution across shards and result aggregation
 */
export class DistributedExecutor {
  private readonly rpc: ShardRPC;
  private readonly replicaSelector: ReplicaSelector;
  private readonly config: Required<ExecutorConfig>;

  constructor(
    rpc: ShardRPC,
    replicaSelector: ReplicaSelector,
    config?: ExecutorConfig
  ) {
    this.rpc = rpc;
    this.replicaSelector = replicaSelector;
    this.config = {
      maxParallelShards: config?.maxParallelShards ?? 32,
      defaultTimeoutMs: config?.defaultTimeoutMs ?? 30000,
      failFast: config?.failFast ?? false,
      retry: config?.retry ?? {
        maxAttempts: 3,
        backoffMs: 100,
        maxBackoffMs: 5000,
      },
    };
  }

  /**
   * Execute a query plan
   */
  async execute(plan: ExecutionPlan): Promise<MergedResult> {
    const startTime = performance.now();

    // Execute all shard plans in parallel
    const shardResults = await this.executeShardPlans(plan.shardPlans, plan.routing.readPreference);

    // Check for errors
    const errors = shardResults
      .filter(r => r.error)
      .map(r => r.error!);

    if (errors.length > 0 && this.config.failFast) {
      throw new ShardExecutionError(errors);
    }

    // Merge and post-process results
    const merged = this.mergeResults(shardResults, plan.postProcessing);

    return {
      ...merged,
      totalExecutionTimeMs: performance.now() - startTime,
      partialFailures: errors.length > 0 ? errors : undefined,
    };
  }

  /**
   * Execute a query plan with streaming results
   */
  async *executeStream(plan: ExecutionPlan): AsyncIterable<unknown[]> {
    // For single-shard queries, stream directly
    if (plan.routing.queryType === 'single-shard') {
      const shardPlan = plan.shardPlans[0];
      const replicaId = this.selectReplica(shardPlan.shardId, plan.routing.readPreference);

      for await (const result of this.rpc.executeStream(
        shardPlan.shardId,
        replicaId,
        shardPlan.sql,
        shardPlan.params,
        { timeoutMs: this.config.defaultTimeoutMs }
      )) {
        for (const row of result.rows) {
          yield row;
        }
      }
      return;
    }

    // For scatter queries, we need to collect all results first for proper ordering
    const allResults = await this.execute(plan);

    for (const row of allResults.rows) {
      yield row;
    }
  }

  private async executeShardPlans(
    plans: ShardExecutionPlan[],
    readPreference: ReadPreference
  ): Promise<ShardResult[]> {
    // Batch plans into groups respecting maxParallelShards
    const batches: ShardExecutionPlan[][] = [];
    for (let i = 0; i < plans.length; i += this.config.maxParallelShards) {
      batches.push(plans.slice(i, i + this.config.maxParallelShards));
    }

    const results: ShardResult[] = [];

    for (const batch of batches) {
      const batchResults = await Promise.all(
        batch.map(plan => this.executeWithRetry(plan, readPreference))
      );
      results.push(...batchResults);
    }

    return results;
  }

  private async executeWithRetry(
    plan: ShardExecutionPlan,
    readPreference: ReadPreference
  ): Promise<ShardResult> {
    let lastError: ShardError | undefined;
    let backoff = this.config.retry.backoffMs;

    for (let attempt = 0; attempt < this.config.retry.maxAttempts; attempt++) {
      try {
        const replicaId = plan.replicaId ?? this.selectReplica(plan.shardId, readPreference);

        const result = await this.rpc.execute(
          plan.shardId,
          replicaId,
          plan.sql,
          plan.params,
          { timeoutMs: this.config.defaultTimeoutMs }
        );

        // Update replica health on success
        if (replicaId) {
          this.replicaSelector.recordSuccess(plan.shardId, replicaId);
        }

        return result;
      } catch (err) {
        lastError = {
          code: 'EXECUTION_ERROR',
          message: err instanceof Error ? err.message : String(err),
          isRetryable: this.isRetryableError(err),
        };

        // Update replica health on failure
        const replicaId = plan.replicaId ?? this.selectReplica(plan.shardId, readPreference);
        if (replicaId) {
          this.replicaSelector.recordFailure(plan.shardId, replicaId);
        }

        if (!lastError.isRetryable) {
          break;
        }

        // Exponential backoff
        await this.sleep(backoff);
        backoff = Math.min(backoff * 2, this.config.retry.maxBackoffMs);
      }
    }

    return {
      shardId: plan.shardId,
      rows: [],
      columns: [],
      rowCount: 0,
      executionTimeMs: 0,
      error: lastError,
    };
  }

  private selectReplica(shardId: string, readPreference: ReadPreference): string | undefined {
    return this.replicaSelector.select(shardId, readPreference);
  }

  private isRetryableError(err: unknown): boolean {
    if (err instanceof Error) {
      const message = err.message.toLowerCase();
      return (
        message.includes('timeout') ||
        message.includes('connection') ||
        message.includes('unavailable') ||
        message.includes('retry')
      );
    }
    return false;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  private mergeResults(
    results: ShardResult[],
    postProcessing?: PostProcessingOp[]
  ): Omit<MergedResult, 'totalExecutionTimeMs' | 'partialFailures'> {
    // Start with basic merge
    let rows: unknown[][] = [];
    const columns = results[0]?.columns ?? [];
    const contributingShards: string[] = [];
    const shardTiming: Record<string, number> = {};

    for (const result of results) {
      if (!result.error) {
        rows.push(...result.rows);
        contributingShards.push(result.shardId);
      }
      shardTiming[result.shardId] = result.executionTimeMs;
    }

    // Apply post-processing operations in order
    if (postProcessing) {
      for (const op of postProcessing) {
        rows = this.applyPostProcessing(rows, columns, op, results);
      }
    }

    return {
      rows,
      columns,
      totalRowCount: rows.length,
      contributingShards,
      shardTiming,
    };
  }

  private applyPostProcessing(
    rows: unknown[][],
    columns: string[],
    op: PostProcessingOp,
    shardResults: ShardResult[]
  ): unknown[][] {
    switch (op.type) {
      case 'merge':
        return rows; // Already merged

      case 'aggregate':
        return this.applyAggregation(rows, columns, op.aggregates, shardResults);

      case 'distinct':
        return this.applyDistinct(rows, columns, op.columns);

      case 'sort':
        return this.applySort(rows, columns, op.columns);

      case 'limit':
        return this.applyLimit(rows, op.count, op.offset);

      default:
        return rows;
    }
  }

  private applyAggregation(
    rows: unknown[][],
    columns: string[],
    aggregates: AggregateOp[],
    shardResults: ShardResult[]
  ): unknown[][] {
    // Two-phase aggregation: combine partial results from shards

    // Create column index map
    const colIndex = new Map<string, number>();
    columns.forEach((col, idx) => colIndex.set(col, idx));

    // For each aggregate, compute final result
    const finalRow: unknown[] = [];

    for (const agg of aggregates) {
      const colIdx = colIndex.get(agg.alias) ?? colIndex.get(agg.column);

      switch (agg.function) {
        case 'COUNT': {
          // Sum all partial counts
          const total = rows.reduce((sum, row) => {
            const val = colIdx !== undefined ? row[colIdx] : 0;
            return sum + (typeof val === 'number' ? val : 0);
          }, 0);
          finalRow.push(total);
          break;
        }

        case 'SUM': {
          // Sum all partial sums
          const total = rows.reduce((sum, row) => {
            const val = colIdx !== undefined ? row[colIdx] : 0;
            return sum + (typeof val === 'number' ? val : 0);
          }, 0);
          finalRow.push(total);
          break;
        }

        case 'AVG': {
          // Combine partial sums and counts
          // Look for _sum_col and _count_col columns
          const sumColIdx = colIndex.get(`_sum_${agg.column}`);
          const countColIdx = colIndex.get(`_count_${agg.column}`);

          if (sumColIdx !== undefined && countColIdx !== undefined) {
            let totalSum = 0;
            let totalCount = 0;

            for (const row of rows) {
              const sum = row[sumColIdx];
              const count = row[countColIdx];
              if (typeof sum === 'number' && typeof count === 'number') {
                totalSum += sum;
                totalCount += count;
              }
            }

            finalRow.push(totalCount > 0 ? totalSum / totalCount : null);
          } else {
            // Fallback: simple average (less accurate)
            const vals = rows
              .map(row => colIdx !== undefined ? row[colIdx] : null)
              .filter((v): v is number => typeof v === 'number');
            finalRow.push(vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : null);
          }
          break;
        }

        case 'MIN': {
          // Find minimum across all shards
          let min: unknown = undefined;
          for (const row of rows) {
            const val = colIdx !== undefined ? row[colIdx] : undefined;
            if (val !== null && val !== undefined) {
              if (min === undefined || this.compareValues(val, min) < 0) {
                min = val;
              }
            }
          }
          finalRow.push(min ?? null);
          break;
        }

        case 'MAX': {
          // Find maximum across all shards
          let max: unknown = undefined;
          for (const row of rows) {
            const val = colIdx !== undefined ? row[colIdx] : undefined;
            if (val !== null && val !== undefined) {
              if (max === undefined || this.compareValues(val, max) > 0) {
                max = val;
              }
            }
          }
          finalRow.push(max ?? null);
          break;
        }
      }
    }

    return finalRow.length > 0 ? [finalRow] : [];
  }

  private applyDistinct(
    rows: unknown[][],
    columns: string[],
    distinctColumns: string[]
  ): unknown[][] {
    const colIndices = distinctColumns.map(col =>
      columns.findIndex(c => c === col)
    );

    const seen = new Set<string>();
    const result: unknown[][] = [];

    for (const row of rows) {
      const key = colIndices.map(idx => JSON.stringify(row[idx])).join('|');
      if (!seen.has(key)) {
        seen.add(key);
        result.push(row);
      }
    }

    return result;
  }

  private applySort(
    rows: unknown[][],
    columns: string[],
    sortColumns: SortColumn[]
  ): unknown[][] {
    const colIndices = sortColumns.map(sc => ({
      index: columns.findIndex(c => c === sc.column),
      direction: sc.direction,
      nulls: sc.nulls,
    }));

    return [...rows].sort((a, b) => {
      for (const col of colIndices) {
        const aVal = a[col.index];
        const bVal = b[col.index];

        // Handle nulls
        if (aVal === null || aVal === undefined) {
          if (bVal === null || bVal === undefined) continue;
          return col.nulls === 'FIRST' ? -1 : 1;
        }
        if (bVal === null || bVal === undefined) {
          return col.nulls === 'FIRST' ? 1 : -1;
        }

        const cmp = this.compareValues(aVal, bVal);
        if (cmp !== 0) {
          return col.direction === 'DESC' ? -cmp : cmp;
        }
      }
      return 0;
    });
  }

  private applyLimit(rows: unknown[][], count: number, offset?: number): unknown[][] {
    const start = offset ?? 0;
    return rows.slice(start, start + count);
  }

  private compareValues(a: unknown, b: unknown): number {
    if (typeof a === 'number' && typeof b === 'number') {
      return a - b;
    }
    if (typeof a === 'string' && typeof b === 'string') {
      return a.localeCompare(b);
    }
    if (typeof a === 'bigint' && typeof b === 'bigint') {
      return a < b ? -1 : a > b ? 1 : 0;
    }
    if (a instanceof Date && b instanceof Date) {
      return a.getTime() - b.getTime();
    }
    return String(a).localeCompare(String(b));
  }
}

// =============================================================================
// ERROR TYPES
// =============================================================================

/**
 * Error thrown when shard execution fails
 */
export class ShardExecutionError extends Error {
  readonly shardErrors: ShardError[];

  constructor(errors: ShardError[]) {
    super(`Shard execution failed: ${errors.map(e => e.message).join(', ')}`);
    this.name = 'ShardExecutionError';
    this.shardErrors = errors;
  }
}

// =============================================================================
// MOCK RPC FOR TESTING
// =============================================================================

/**
 * Mock RPC implementation for testing
 */
export class MockShardRPC implements ShardRPC {
  private readonly shardData: Map<string, { columns: string[]; rows: unknown[][] }>;

  constructor() {
    this.shardData = new Map();
  }

  /**
   * Set data for a shard (for testing)
   */
  setShardData(shardId: string, columns: string[], rows: unknown[][]): void {
    this.shardData.set(shardId, { columns, rows });
  }

  async execute(
    shardId: string,
    _replicaId: string | undefined,
    _sql: string,
    _params?: unknown[],
    _options?: ExecuteOptions
  ): Promise<ShardResult> {
    const data = this.shardData.get(shardId) ?? { columns: [], rows: [] };

    return {
      shardId,
      columns: data.columns,
      rows: data.rows,
      rowCount: data.rows.length,
      executionTimeMs: Math.random() * 10,
    };
  }

  async *executeStream(
    shardId: string,
    replicaId: string | undefined,
    sql: string,
    params?: unknown[],
    options?: ExecuteOptions
  ): AsyncIterable<ShardResult> {
    yield await this.execute(shardId, replicaId, sql, params, options);
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a distributed executor
 */
export function createExecutor(
  rpc: ShardRPC,
  replicaSelector: ReplicaSelector,
  config?: ExecutorConfig
): DistributedExecutor {
  return new DistributedExecutor(rpc, replicaSelector, config);
}
