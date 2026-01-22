/**
 * Project Operator
 *
 * Selects specific columns from input rows and applies aliases.
 * Supports:
 * - Column selection (SELECT id, name FROM ...)
 * - Column aliases (SELECT id AS user_id ...)
 * - Computed expressions (SELECT amount * 1.1 AS amount_with_tax ...)
 * - Literal values (SELECT 'VIP' AS status ...)
 */

import {
  type ProjectPlan,
  type ExecutionContext,
  type Operator,
  type Row,
  type Expression,
  type SqlValue,
} from '../types.js';
import { evaluateExpression } from './filter.js';

// =============================================================================
// PROJECT OPERATOR
// =============================================================================

/**
 * Project operator implementation
 *
 * Transforms input rows by selecting specific columns/expressions
 * and applying aliases to the output.
 */
export class ProjectOperator implements Operator {
  private plan: ProjectPlan;
  private input: Operator;
  private ctx!: ExecutionContext;
  private outputColumns: string[];

  constructor(plan: ProjectPlan, input: Operator, ctx: ExecutionContext) {
    this.plan = plan;
    this.input = input;
    this.ctx = ctx;
    // Output columns are the aliases from expressions
    this.outputColumns = plan.expressions.map((e) => e.alias);
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;
    await this.input.open(ctx);
  }

  async next(): Promise<Row | null> {
    const inputRow = await this.input.next();
    if (inputRow === null) return null;

    // Build output row by evaluating each expression
    const outputRow: Row = {};

    for (const { expr, alias } of this.plan.expressions) {
      outputRow[alias] = evaluateExpression(expr, inputRow);
    }

    return outputRow;
  }

  async close(): Promise<void> {
    await this.input.close();
  }

  columns(): string[] {
    return this.outputColumns;
  }
}
