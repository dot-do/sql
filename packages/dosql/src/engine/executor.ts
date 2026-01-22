/**
 * DoSQL Query Executor
 *
 * Executes query plans using a pull-based iterator model.
 * Each operator reads from its child(ren) and produces rows on demand.
 */

import {
  type QueryPlan,
  type ExecutionContext,
  type Operator,
  type Row,
  type QueryResult,
  type ExecutionStats,
  type SqlValue,
} from './types.js';

import { ScanOperator } from './operators/scan.js';
import { FilterOperator } from './operators/filter.js';
import { ProjectOperator } from './operators/project.js';
import { JoinOperator } from './operators/join.js';
import { AggregateOperator } from './operators/aggregate.js';
import { SortOperator } from './operators/sort.js';
import { LimitOperator } from './operators/limit.js';

// =============================================================================
// OPERATOR FACTORY
// =============================================================================

/**
 * Create an operator for a query plan node
 */
export function createOperator(plan: QueryPlan, ctx: ExecutionContext): Operator {
  switch (plan.type) {
    case 'scan':
      return new ScanOperator(plan, ctx);

    case 'indexLookup':
      // Index lookup is handled as a specialized scan
      return new ScanOperator({
        id: plan.id,
        type: 'scan',
        table: plan.table,
        alias: plan.alias,
        source: 'btree',
        columns: plan.columns,
      }, ctx);

    case 'filter':
      return new FilterOperator(plan, createOperator(plan.input, ctx), ctx);

    case 'project':
      return new ProjectOperator(plan, createOperator(plan.input, ctx), ctx);

    case 'join':
      return new JoinOperator(
        plan,
        createOperator(plan.left, ctx),
        createOperator(plan.right, ctx),
        ctx
      );

    case 'aggregate':
      return new AggregateOperator(plan, createOperator(plan.input, ctx), ctx);

    case 'sort':
      return new SortOperator(plan, createOperator(plan.input, ctx), ctx);

    case 'limit':
      return new LimitOperator(plan, createOperator(plan.input, ctx), ctx);

    case 'distinct':
      // Distinct is implemented as a specialized aggregate with grouping
      return new AggregateOperator(
        {
          id: plan.id,
          type: 'aggregate',
          input: plan.input,
          groupBy: plan.columns?.map(c => ({ type: 'columnRef', column: c })) || [],
          aggregates: [],
        },
        createOperator(plan.input, ctx),
        ctx
      );

    case 'union':
      return new UnionOperator(
        plan,
        plan.inputs.map(input => createOperator(input, ctx)),
        ctx
      );

    case 'merge':
      return new MergeOperator(
        plan,
        plan.inputs.map(input => createOperator(input, ctx)),
        ctx
      );

    default:
      throw new Error(`Unknown plan type: ${(plan as any).type}`);
  }
}

// =============================================================================
// UNION OPERATOR
// =============================================================================

/**
 * Union operator - combines results from multiple inputs
 */
class UnionOperator implements Operator {
  private inputs: Operator[];
  private currentIndex = 0;
  private all: boolean;
  private seen: Set<string> | null = null;
  private outputColumns: string[] = [];

  constructor(
    plan: { all: boolean },
    inputs: Operator[],
    private ctx: ExecutionContext
  ) {
    this.inputs = inputs;
    this.all = plan.all;
    if (!this.all) {
      this.seen = new Set();
    }
  }

  async open(): Promise<void> {
    for (const input of this.inputs) {
      await input.open(this.ctx);
    }
    if (this.inputs.length > 0) {
      this.outputColumns = this.inputs[0].columns();
    }
  }

  async next(): Promise<Row | null> {
    while (this.currentIndex < this.inputs.length) {
      const row = await this.inputs[this.currentIndex].next();

      if (row === null) {
        this.currentIndex++;
        continue;
      }

      // For UNION (not UNION ALL), deduplicate
      if (!this.all && this.seen) {
        const key = JSON.stringify(row);
        if (this.seen.has(key)) {
          continue;
        }
        this.seen.add(key);
      }

      return row;
    }

    return null;
  }

  async close(): Promise<void> {
    for (const input of this.inputs) {
      await input.close();
    }
  }

  columns(): string[] {
    return this.outputColumns;
  }
}

// =============================================================================
// MERGE OPERATOR
// =============================================================================

/**
 * Merge operator - combines hot (B-tree) and cold (columnar) data
 * If orderBy is specified, performs a merge-sort
 */
class MergeOperator implements Operator {
  private inputs: Operator[];
  private buffers: (Row | null)[] = [];
  private outputColumns: string[] = [];

  constructor(
    private plan: { orderBy?: { expr: any; direction: 'asc' | 'desc' }[] },
    inputs: Operator[],
    private ctx: ExecutionContext
  ) {
    this.inputs = inputs;
  }

  async open(): Promise<void> {
    for (const input of this.inputs) {
      await input.open(this.ctx);
    }

    // Initialize buffers
    this.buffers = await Promise.all(this.inputs.map(input => input.next()));

    if (this.inputs.length > 0) {
      this.outputColumns = this.inputs[0].columns();
    }
  }

  async next(): Promise<Row | null> {
    // Find the next row to return (smallest if sorted, any if not)
    let minIndex = -1;
    let minRow: Row | null = null;

    for (let i = 0; i < this.buffers.length; i++) {
      const row = this.buffers[i];
      if (row === null) continue;

      if (minRow === null) {
        minIndex = i;
        minRow = row;
      } else if (this.plan.orderBy) {
        // Compare based on order by
        if (this.compareRows(row, minRow) < 0) {
          minIndex = i;
          minRow = row;
        }
      } else {
        // No ordering, just pick the first non-null
        minIndex = i;
        minRow = row;
        break;
      }
    }

    if (minIndex === -1) {
      return null;
    }

    // Refill the buffer
    this.buffers[minIndex] = await this.inputs[minIndex].next();

    return minRow;
  }

  async close(): Promise<void> {
    for (const input of this.inputs) {
      await input.close();
    }
  }

  columns(): string[] {
    return this.outputColumns;
  }

  private compareRows(a: Row, b: Row): number {
    if (!this.plan.orderBy) return 0;

    for (const spec of this.plan.orderBy) {
      const colName = spec.expr.type === 'columnRef' ? spec.expr.column : 'unknown';
      const aVal = a[colName];
      const bVal = b[colName];

      let cmp = 0;
      if (aVal === null && bVal === null) cmp = 0;
      else if (aVal === null) cmp = 1;
      else if (bVal === null) cmp = -1;
      else if (aVal < bVal) cmp = -1;
      else if (aVal > bVal) cmp = 1;

      if (cmp !== 0) {
        return spec.direction === 'desc' ? -cmp : cmp;
      }
    }

    return 0;
  }
}

// =============================================================================
// QUERY EXECUTOR
// =============================================================================

/**
 * Execute a query plan and collect results
 */
export async function executePlan<T = Row>(
  plan: QueryPlan,
  ctx: ExecutionContext
): Promise<QueryResult<T>> {
  const startTime = performance.now();

  const operator = createOperator(plan, ctx);
  const rows: T[] = [];
  let rowsScanned = 0;

  await operator.open(ctx);

  try {
    while (true) {
      const row = await operator.next();
      if (row === null) break;

      rows.push(row as T);
      rowsScanned++;

      // Check for row limit
      if (ctx.options?.maxRows && rows.length >= ctx.options.maxRows) {
        break;
      }

      // Check for timeout
      if (ctx.options?.timeout) {
        const elapsed = performance.now() - startTime;
        if (elapsed > ctx.options.timeout) {
          throw new Error(`Query timeout after ${elapsed}ms`);
        }
      }
    }
  } finally {
    await operator.close();
  }

  const endTime = performance.now();

  return {
    rows,
    columns: operator.columns().map(name => ({ name, type: 'unknown' })),
    stats: {
      planningTime: 0, // Filled in by caller
      executionTime: endTime - startTime,
      rowsScanned,
      rowsReturned: rows.length,
    },
  };
}

/**
 * Execute a query plan as an async iterator
 */
export async function* executePlanIterator<T = Row>(
  plan: QueryPlan,
  ctx: ExecutionContext
): AsyncIterableIterator<T> {
  const operator = createOperator(plan, ctx);
  await operator.open(ctx);

  try {
    while (true) {
      const row = await operator.next();
      if (row === null) break;
      yield row as T;
    }
  } finally {
    await operator.close();
  }
}

// =============================================================================
// EXECUTOR CLASS
// =============================================================================

/**
 * Query executor that manages execution context and statistics
 */
export class QueryExecutor {
  private ctx: ExecutionContext;

  constructor(ctx: ExecutionContext) {
    this.ctx = ctx;
  }

  /**
   * Execute a query plan
   */
  async execute<T = Row>(plan: QueryPlan): Promise<QueryResult<T>> {
    return executePlan<T>(plan, this.ctx);
  }

  /**
   * Execute a query plan as an iterator
   */
  executeIterator<T = Row>(plan: QueryPlan): AsyncIterableIterator<T> {
    return executePlanIterator<T>(plan, this.ctx);
  }

  /**
   * Execute and return just the rows
   */
  async query<T = Row>(plan: QueryPlan): Promise<T[]> {
    const result = await this.execute<T>(plan);
    return result.rows;
  }

  /**
   * Execute and return the first row
   */
  async queryOne<T = Row>(plan: QueryPlan): Promise<T | null> {
    const iterator = this.executeIterator<T>(plan);
    const result = await iterator.next();
    return result.done ? null : result.value;
  }

  /**
   * Get the execution context
   */
  getContext(): ExecutionContext {
    return this.ctx;
  }
}

/**
 * Create a query executor
 */
export function createExecutor(ctx: ExecutionContext): QueryExecutor {
  return new QueryExecutor(ctx);
}
