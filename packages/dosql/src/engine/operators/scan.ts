/**
 * Scan Operator
 *
 * Reads rows from B-tree or columnar storage.
 * Supports predicate pushdown to filter at the storage level.
 * Supports backpressure for large result sets.
 */

import {
  type ScanPlan,
  type ExecutionContext,
  type Row,
  type Predicate,
  type SqlValue,
  type BackpressureState,
  type BackpressureStats,
  type BackpressureOperator,
  type WatermarkConfig,
  type BackpressureController,
  DEFAULT_WATERMARKS,
} from '../types.js';
import { evaluatePredicate } from './filter.js';
import type { Predicate as ColumnarPredicate } from '../../columnar/index.js';

/**
 * Convert our predicate format to columnar predicate format
 */
function toColumnarPredicates(predicate: Predicate | undefined): ColumnarPredicate[] | undefined {
  if (!predicate) return undefined;

  const predicates: ColumnarPredicate[] = [];

  function extract(p: Predicate): void {
    switch (p.type) {
      case 'comparison': {
        // Only handle simple column = literal comparisons
        if (p.left.type === 'columnRef' && p.right.type === 'literal') {
          const opMap: Record<string, ColumnarPredicate['op']> = {
            eq: 'eq', ne: 'ne', lt: 'lt', le: 'le', gt: 'gt', ge: 'ge',
          };
          const mappedOp = opMap[p.op];
          if (mappedOp) {
            predicates.push({
              column: p.left.column,
              op: mappedOp,
              value: p.right.value as number | bigint | string | null,
            });
          }
        }
        break;
      }
      case 'logical': {
        if (p.op === 'and') {
          for (const operand of p.operands) {
            extract(operand);
          }
        }
        // OR and NOT are harder to push down, skip
        break;
      }
      case 'between': {
        if (p.expr.type === 'columnRef' && p.low.type === 'literal' && p.high.type === 'literal') {
          predicates.push({
            column: p.expr.column,
            op: 'between',
            value: p.low.value as number | bigint | string,
            value2: p.high.value as number | bigint | string,
          });
        }
        break;
      }
      case 'in': {
        if (p.expr.type === 'columnRef' && Array.isArray(p.values)) {
          const values = p.values
            .filter(v => v.type === 'literal')
            .map(v => (v as { type: 'literal'; value: SqlValue }).value as number | bigint | string);
          if (values.length > 0) {
            predicates.push({
              column: p.expr.column,
              op: 'in',
              value: values,
            });
          }
        }
        break;
      }
      case 'isNull': {
        if (p.expr.type === 'columnRef') {
          predicates.push({
            column: p.expr.column,
            op: p.isNot ? 'ne' : 'eq',
            value: null,
          });
        }
        break;
      }
    }
  }

  extract(predicate);
  return predicates.length > 0 ? predicates : undefined;
}

/**
 * Scan operator implementation with backpressure support
 */
export class ScanOperator implements BackpressureOperator {
  private plan: ScanPlan;
  private ctx!: ExecutionContext;
  private iterator: AsyncIterator<Row> | null = null;
  private outputColumns: string[];

  // Backpressure state
  private backpressureState: BackpressureState = 'flowing';
  private watermarks: WatermarkConfig = DEFAULT_WATERMARKS;
  private pauseCount = 0;
  private resumeCount = 0;
  private memoryUsage = 0;
  private bufferedRows = 0;
  private backpressureCallbacks: Array<(state: BackpressureState) => void> = [];
  private pausePromise: Promise<void> | null = null;
  private pauseResolve: (() => void) | null = null;
  private controller: BackpressureController | null = null;

  constructor(plan: ScanPlan, ctx: ExecutionContext) {
    this.plan = plan;
    this.ctx = ctx;
    this.outputColumns = plan.columns;
  }

  /**
   * Set the backpressure controller for this operator
   */
  setController(controller: BackpressureController): void {
    this.controller = controller;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;
    const { table, source, predicate, columns } = this.plan;

    // Reset backpressure state
    this.backpressureState = 'flowing';
    this.pausePromise = null;
    this.pauseResolve = null;

    // Choose data source
    if (source === 'columnar') {
      // Use columnar storage
      const columnarPredicates = toColumnarPredicates(predicate);
      this.iterator = this.ctx.columnar.scan(table, {
        columns,
        predicates: columnarPredicates,
      });
    } else if (source === 'btree') {
      // Use B-tree storage
      this.iterator = this.ctx.btree.scan(table);
    } else {
      // 'both' - merge hot and cold data
      // For now, just use B-tree as primary and skip columnar
      // A full implementation would merge both sources
      this.iterator = this.ctx.btree.scan(table);
    }
  }

  async next(): Promise<Row | null> {
    if (!this.iterator) return null;

    // Wait if paused (backpressure)
    if (this.pausePromise) {
      await this.pausePromise;
    }

    while (true) {
      // Check if we need to pause again after each iteration
      if (this.pausePromise) {
        await this.pausePromise;
      }

      const result = await this.iterator.next();
      if (result.done) return null;

      const row = result.value;

      // Apply predicate if not pushed down
      if (this.plan.predicate && this.plan.source !== 'columnar') {
        if (!evaluatePredicate(this.plan.predicate, row)) {
          continue;
        }
      }

      // Project columns if specified
      if (this.plan.columns.length > 0 && !this.plan.columns.includes('*')) {
        const projected: Row = {};
        for (const col of this.plan.columns) {
          if (col in row) {
            projected[col] = row[col] ?? null;
          }
        }
        return projected;
      }

      return row;
    }
  }

  async close(): Promise<void> {
    // Cleanup iterator if needed
    if (this.iterator && 'return' in this.iterator && typeof this.iterator.return === 'function') {
      await this.iterator.return();
    }
    this.iterator = null;

    // Reset backpressure state
    this.backpressureState = 'flowing';
    this.memoryUsage = 0;
    this.bufferedRows = 0;
    this.pausePromise = null;
    this.pauseResolve = null;
    if (this.controller) {
      this.controller.reportMemoryUsage(this, 0);
    }
  }

  columns(): string[] {
    return this.outputColumns;
  }

  // ==========================================================================
  // BACKPRESSURE SUPPORT
  // ==========================================================================

  async pause(): Promise<void> {
    if (this.backpressureState === 'paused') {
      return;
    }

    this.backpressureState = 'paused';
    this.pauseCount++;
    this.notifyBackpressureChange();

    // Create a promise that will be resolved when resume() is called
    this.pausePromise = new Promise<void>((resolve) => {
      this.pauseResolve = resolve;
    });

    if (this.controller) {
      this.controller.requestPause(this);
    }
  }

  async resume(): Promise<void> {
    if (this.backpressureState !== 'paused') {
      return;
    }

    this.backpressureState = 'flowing';
    this.resumeCount++;
    this.notifyBackpressureChange();

    // Resolve the pause promise to unblock waiting code
    if (this.pauseResolve) {
      this.pauseResolve();
      this.pausePromise = null;
      this.pauseResolve = null;
    }

    if (this.controller) {
      this.controller.requestResume(this);
    }
  }

  isPaused(): boolean {
    return this.backpressureState === 'paused';
  }

  getBackpressureState(): BackpressureState {
    return this.backpressureState;
  }

  getBackpressureStats(): BackpressureStats {
    return {
      memoryUsage: this.memoryUsage,
      pauseCount: this.pauseCount,
      resumeCount: this.resumeCount,
      state: this.backpressureState,
      bufferedRows: this.bufferedRows,
    };
  }

  setWatermarks(config: WatermarkConfig): void {
    this.watermarks = { ...config };
  }

  onBackpressure(callback: (state: BackpressureState) => void): void {
    this.backpressureCallbacks.push(callback);
  }

  private notifyBackpressureChange(): void {
    for (const callback of this.backpressureCallbacks) {
      callback(this.backpressureState);
    }
  }

  /**
   * Async iterator support - enables `for await (const row of operator)`
   * Note: The operator must be opened before iterating
   */
  async *[Symbol.asyncIterator](): AsyncIterator<Row> {
    try {
      let row: Row | null;
      while ((row = await this.next()) !== null) {
        yield row;
      }
    } finally {
      await this.close();
    }
  }
}
