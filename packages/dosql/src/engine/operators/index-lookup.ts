/**
 * Index Lookup Operator
 *
 * Performs efficient point lookups using B-tree's get() method.
 * This is O(log n) instead of O(n) for full table scans.
 */

import {
  type IndexLookupPlan,
  type ExecutionContext,
  type Operator,
  type Row,
  type SqlValue,
} from '../types.js';
import { evaluateExpression } from './filter.js';

/**
 * Index lookup operator implementation
 *
 * This operator uses the B-tree's get() method for efficient point lookups.
 * It extracts the lookup key from the plan and performs a single B-tree lookup
 * instead of scanning the entire table.
 */
export class IndexLookupOperator implements Operator {
  private plan: IndexLookupPlan;
  private ctx!: ExecutionContext;
  private result: Row | null = null;
  private consumed = false;
  private outputColumns: string[];

  constructor(plan: IndexLookupPlan, ctx: ExecutionContext) {
    this.plan = plan;
    this.ctx = ctx;
    this.outputColumns = plan.columns;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;
    this.consumed = false;

    // Evaluate the lookup key expressions
    // Note: For a primary key lookup, the key is typically a single value
    // but could be composite (multiple expressions for composite keys)
    const keyValues: SqlValue[] = this.plan.lookupKey.map((expr) =>
      evaluateExpression(expr, {})
    );

    // For single-column primary key, use the first value
    // For composite keys, we'd need to combine them appropriately
    const lookupKey = keyValues.length === 1 ? keyValues[0] : keyValues;

    // Perform the point lookup using B-tree's get() method
    // This is O(log n) instead of O(n) for full table scan
    this.result = (await this.ctx.btree.get(
      this.plan.table,
      lookupKey as SqlValue
    )) ?? null;
  }

  async next(): Promise<Row | null> {
    // Point lookup returns at most one row
    if (this.consumed || this.result === null) {
      return null;
    }

    this.consumed = true;

    const row = this.result;

    // Project only the requested columns if specified
    if (
      this.plan.columns.length > 0 &&
      !this.plan.columns.includes('*')
    ) {
      const projected: Row = {};
      for (const col of this.plan.columns) {
        if (col in row) {
          const value = row[col];
          if (value !== undefined) {
            projected[col] = value;
          }
        }
      }
      return projected;
    }

    return row;
  }

  async close(): Promise<void> {
    this.result = null;
    this.consumed = false;
  }

  columns(): string[] {
    return this.outputColumns;
  }
}
