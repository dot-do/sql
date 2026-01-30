/**
 * Limit Operator
 *
 * Limits the number of rows returned from the input operator.
 * Supports optional OFFSET for pagination.
 */

import {
  type LimitPlan,
  type ExecutionContext,
  type Operator,
  type Row,
} from '../types.js';

// =============================================================================
// LIMIT OPERATOR
// =============================================================================

/**
 * Limit operator implementation
 *
 * Returns at most `limit` rows from the input, optionally skipping `offset` rows.
 */
export class LimitOperator implements Operator {
  private plan: LimitPlan;
  private input: Operator;
  private ctx!: ExecutionContext;
  private emitted = 0;
  private skipped = 0;

  constructor(plan: LimitPlan, input: Operator, ctx: ExecutionContext) {
    this.plan = plan;
    this.input = input;
    this.ctx = ctx;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;
    this.emitted = 0;
    this.skipped = 0;
    await this.input.open(ctx);
  }

  async next(): Promise<Row | null> {
    // Check if we've emitted enough rows
    if (this.emitted >= this.plan.limit) return null;

    // Skip offset rows
    const offset = this.plan.offset ?? 0;
    while (this.skipped < offset) {
      const row = await this.input.next();
      if (row === null) return null;
      this.skipped++;
    }

    const row = await this.input.next();
    if (row === null) return null;

    this.emitted++;
    return row;
  }

  async close(): Promise<void> {
    await this.input.close();
    this.emitted = 0;
    this.skipped = 0;
  }

  columns(): string[] {
    return this.input.columns();
  }
}
