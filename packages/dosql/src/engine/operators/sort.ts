/**
 * Sort Operator
 *
 * Materializes all input rows and sorts them according to ORDER BY specification.
 */

import {
  type SortPlan,
  type ExecutionContext,
  type Operator,
  type Row,
  type SqlValue,
} from '../types.js';
import { evaluateExpression } from './filter.js';

// =============================================================================
// SORT OPERATOR
// =============================================================================

/**
 * Sort operator implementation
 *
 * Materializes all input rows, sorts them, and emits in sorted order.
 */
export class SortOperator implements Operator {
  private plan: SortPlan;
  private input: Operator;
  private ctx!: ExecutionContext;
  private sortedRows: Row[] = [];
  private index = 0;

  constructor(plan: SortPlan, input: Operator, ctx: ExecutionContext) {
    this.plan = plan;
    this.input = input;
    this.ctx = ctx;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;
    this.sortedRows = [];
    this.index = 0;

    await this.input.open(ctx);

    // Materialize all input rows
    let row: Row | null;
    while ((row = await this.input.next()) !== null) {
      this.sortedRows.push(row);
    }

    // Sort rows according to ORDER BY specification
    this.sortedRows.sort((a, b) => {
      for (const spec of this.plan.orderBy) {
        const aVal = evaluateExpression(spec.expr, a);
        const bVal = evaluateExpression(spec.expr, b);

        if (aVal === bVal) continue;

        // Handle nulls
        if (aVal === null) {
          return spec.nullsFirst ? -1 : 1;
        }
        if (bVal === null) {
          return spec.nullsFirst ? 1 : -1;
        }

        // Compare values
        const cmp = aVal < bVal ? -1 : 1;
        return spec.direction === 'desc' ? -cmp : cmp;
      }
      return 0;
    });
  }

  async next(): Promise<Row | null> {
    if (this.index >= this.sortedRows.length) return null;
    return this.sortedRows[this.index++];
  }

  async close(): Promise<void> {
    await this.input.close();
    this.sortedRows = [];
    this.index = 0;
  }

  columns(): string[] {
    return this.input.columns();
  }
}
