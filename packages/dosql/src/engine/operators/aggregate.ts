/**
 * Aggregate Operator
 *
 * Implements SQL aggregation operations.
 * Supports:
 * - Aggregate functions: COUNT, SUM, AVG, MIN, MAX
 * - GROUP BY: single and multiple columns
 * - HAVING: filter groups after aggregation
 *
 * The operator materializes all input rows, groups them, computes
 * aggregates, and then emits one row per group.
 */

import {
  type AggregatePlan,
  type ExecutionContext,
  type Operator,
  type Row,
  type SqlValue,
  type Expression,
  type AggregateExpr,
  type Predicate,
} from '../types.js';
import { evaluateExpression, evaluatePredicate } from './filter.js';

// =============================================================================
// AGGREGATE STATE
// =============================================================================

/**
 * State for a single aggregate computation
 */
interface AggregateState {
  func: string;
  count: number;
  sum: number | bigint | null;
  min: SqlValue;
  max: SqlValue;
}

/**
 * Create initial state for an aggregate
 */
function createAggregateState(func: string): AggregateState {
  return {
    func,
    count: 0,
    sum: null,
    min: null,
    max: null,
  };
}

/**
 * Update aggregate state with a new value
 */
function updateAggregateState(state: AggregateState, value: SqlValue): void {
  // For COUNT(*), we count all rows regardless of value
  // For COUNT(column), we only count non-null values
  // The distinction is handled by the caller

  if (state.func === 'count') {
    state.count++;
    return;
  }

  // Skip null values for other aggregates
  if (value === null || value === undefined) {
    return;
  }

  state.count++;

  // SUM and AVG
  if (state.func === 'sum' || state.func === 'avg') {
    if (typeof value === 'number') {
      state.sum = ((state.sum as number) || 0) + value;
    } else if (typeof value === 'bigint') {
      state.sum = ((state.sum as bigint) || 0n) + value;
    }
  }

  // MIN
  if (state.func === 'min') {
    if (state.min === null || (value !== null && value < state.min)) {
      state.min = value;
    }
  }

  // MAX
  if (state.func === 'max') {
    if (state.max === null || (value !== null && value > state.max)) {
      state.max = value;
    }
  }
}

/**
 * Get final result from aggregate state
 */
function getAggregateResult(state: AggregateState): SqlValue {
  switch (state.func) {
    case 'count':
      return state.count;
    case 'sum':
      return state.sum;
    case 'avg':
      if (state.count === 0) return null;
      if (typeof state.sum === 'bigint') {
        return Number(state.sum) / state.count;
      }
      return ((state.sum as number) || 0) / state.count;
    case 'min':
      return state.min;
    case 'max':
      return state.max;
    default:
      return null;
  }
}

// =============================================================================
// GROUP STATE
// =============================================================================

/**
 * State for a single group
 */
interface GroupState {
  key: string;
  keyValues: Record<string, SqlValue>;
  aggregateStates: AggregateState[];
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Compute a group key from a row based on GROUP BY expressions
 */
function computeGroupKey(row: Row, groupBy: Expression[]): string {
  if (groupBy.length === 0) {
    return '__all__';
  }

  const keyParts: string[] = [];
  for (const expr of groupBy) {
    const value = evaluateExpression(expr, row);
    keyParts.push(JSON.stringify(value));
  }
  return keyParts.join('|');
}

/**
 * Extract key values from a row based on GROUP BY expressions
 */
function extractKeyValues(row: Row, groupBy: Expression[]): Record<string, SqlValue> {
  const result: Record<string, SqlValue> = {};
  for (const expr of groupBy) {
    if (expr.type === 'columnRef') {
      const colName = expr.table ? `${expr.table}.${expr.column}` : expr.column;
      result[expr.column] = evaluateExpression(expr, row);
    }
  }
  return result;
}

/**
 * Extract aggregate function info from an aggregate expression
 * Handles both the standard format and the test format
 */
function getAggregateInfo(aggExpr: AggregateExpr): { func: string; argExpr: Expression | '*' } {
  // Standard format: { type: 'aggregate', function: '...', arg: ... }
  if ('function' in aggExpr && aggExpr.function) {
    return {
      func: aggExpr.function.toLowerCase(),
      argExpr: aggExpr.arg,
    };
  }

  // Test format: { type: 'aggregate', func: '...', args: [...] }
  const testFormat = aggExpr as unknown as {
    func?: string;
    args?: Expression[];
  };

  if (testFormat.func) {
    const func = testFormat.func.toLowerCase();
    // For COUNT(*), args[0] might be { type: 'literal', value: '*' }
    if (testFormat.args && testFormat.args.length > 0) {
      const firstArg = testFormat.args[0];
      if (firstArg.type === 'literal' && firstArg.value === '*') {
        return { func, argExpr: '*' };
      }
      return { func, argExpr: firstArg };
    }
    return { func, argExpr: '*' };
  }

  throw new Error('Invalid aggregate expression format');
}

/**
 * Evaluate an aggregate expression against accumulated group values
 * Used for HAVING clause evaluation
 */
function evaluateAggregateInHaving(
  aggExpr: AggregateExpr,
  aggregateStates: AggregateState[],
  aggregates: { expr: AggregateExpr; alias: string }[]
): SqlValue {
  const { func, argExpr } = getAggregateInfo(aggExpr);

  // Find matching aggregate state
  for (let i = 0; i < aggregates.length; i++) {
    const { func: stateFunc } = getAggregateInfo(aggregates[i].expr);
    if (stateFunc === func) {
      // This is a simplification - in a full implementation we'd match
      // the exact expression
      return getAggregateResult(aggregateStates[i]);
    }
  }

  return null;
}

/**
 * Evaluate a HAVING predicate against a group
 */
function evaluateHaving(
  predicate: Predicate,
  keyValues: Record<string, SqlValue>,
  aggregateStates: AggregateState[],
  aggregates: { expr: AggregateExpr; alias: string }[]
): boolean {
  // Build a row with group key values and aggregate results
  const row: Row = { ...keyValues };

  // Add aggregate results to row for predicate evaluation
  for (let i = 0; i < aggregates.length; i++) {
    row[aggregates[i].alias] = getAggregateResult(aggregateStates[i]);
  }

  // Handle aggregate expressions in the predicate
  return evaluateHavingPredicate(predicate, row, aggregateStates, aggregates);
}

/**
 * Evaluate HAVING predicate, handling aggregate expressions specially
 */
function evaluateHavingPredicate(
  predicate: Predicate,
  row: Row,
  aggregateStates: AggregateState[],
  aggregates: { expr: AggregateExpr; alias: string }[]
): boolean {
  switch (predicate.type) {
    case 'comparison': {
      const leftValue = evaluateHavingExpression(predicate.left, row, aggregateStates, aggregates);
      const rightValue = evaluateHavingExpression(predicate.right, row, aggregateStates, aggregates);

      if (leftValue === null || rightValue === null) {
        return predicate.op === 'ne';
      }

      switch (predicate.op) {
        case 'eq': return leftValue === rightValue;
        case 'ne': return leftValue !== rightValue;
        case 'lt': return leftValue < rightValue;
        case 'le': return leftValue <= rightValue;
        case 'gt': return leftValue > rightValue;
        case 'ge': return leftValue >= rightValue;
        default: return false;
      }
    }

    case 'logical': {
      switch (predicate.op) {
        case 'and':
          return predicate.operands.every((op) =>
            evaluateHavingPredicate(op, row, aggregateStates, aggregates)
          );
        case 'or':
          return predicate.operands.some((op) =>
            evaluateHavingPredicate(op, row, aggregateStates, aggregates)
          );
        case 'not':
          return !evaluateHavingPredicate(predicate.operands[0], row, aggregateStates, aggregates);
        default:
          return false;
      }
    }

    default:
      return evaluatePredicate(predicate, row);
  }
}

/**
 * Evaluate an expression in HAVING context, handling aggregates
 */
function evaluateHavingExpression(
  expr: Expression,
  row: Row,
  aggregateStates: AggregateState[],
  aggregates: { expr: AggregateExpr; alias: string }[]
): SqlValue {
  if (expr.type === 'aggregate') {
    return evaluateAggregateInHaving(expr as AggregateExpr, aggregateStates, aggregates);
  }
  return evaluateExpression(expr, row);
}

// =============================================================================
// AGGREGATE OPERATOR
// =============================================================================

/**
 * Aggregate operator implementation
 *
 * Materializes all input, groups rows, computes aggregates, and emits results.
 */
export class AggregateOperator implements Operator {
  private plan: AggregatePlan;
  private input: Operator;
  private ctx!: ExecutionContext;

  // Aggregation state
  private groups: Map<string, GroupState> = new Map();
  private resultIterator: Iterator<GroupState> | null = null;
  private outputColumns: string[];

  constructor(plan: AggregatePlan, input: Operator, ctx: ExecutionContext) {
    this.plan = plan;
    this.input = input;
    this.ctx = ctx;

    // Compute output columns: group by columns + aggregate aliases
    const groupCols = plan.groupBy
      .filter((e) => e.type === 'columnRef')
      .map((e) => (e as { column: string }).column);
    const aggCols = plan.aggregates.map((a) => a.alias);
    this.outputColumns = [...groupCols, ...aggCols];
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;
    this.groups.clear();
    this.resultIterator = null;

    await this.input.open(ctx);

    // Materialize and aggregate all input rows
    let row: Row | null;
    while ((row = await this.input.next()) !== null) {
      this.processRow(row);
    }

    // Prepare iterator for emitting results
    this.resultIterator = this.groups.values();
  }

  /**
   * Process a single input row, updating aggregate state
   */
  private processRow(row: Row): void {
    const key = computeGroupKey(row, this.plan.groupBy);

    let group = this.groups.get(key);
    if (!group) {
      // Create new group
      group = {
        key,
        keyValues: extractKeyValues(row, this.plan.groupBy),
        aggregateStates: this.plan.aggregates.map((agg) => {
          const { func } = getAggregateInfo(agg.expr);
          return createAggregateState(func);
        }),
      };
      this.groups.set(key, group);
    }

    // Update aggregate states
    for (let i = 0; i < this.plan.aggregates.length; i++) {
      const aggExpr = this.plan.aggregates[i].expr;
      const { func, argExpr } = getAggregateInfo(aggExpr);

      if (func === 'count' && argExpr === '*') {
        // COUNT(*) counts all rows
        updateAggregateState(group.aggregateStates[i], 1);
      } else if (func === 'count') {
        // COUNT(column) counts non-null values
        const value = evaluateExpression(argExpr as Expression, row);
        if (value !== null && value !== undefined) {
          updateAggregateState(group.aggregateStates[i], 1);
        }
      } else {
        // Other aggregates evaluate the expression
        const value = evaluateExpression(argExpr as Expression, row);
        updateAggregateState(group.aggregateStates[i], value);
      }
    }
  }

  async next(): Promise<Row | null> {
    if (!this.resultIterator) return null;

    while (true) {
      const result = this.resultIterator.next();
      if (result.done) return null;

      const group = result.value;

      // Apply HAVING filter if present
      if (this.plan.having) {
        const passes = evaluateHaving(
          this.plan.having,
          group.keyValues,
          group.aggregateStates,
          this.plan.aggregates
        );
        if (!passes) continue;
      }

      // Build output row
      const outputRow: Row = {};

      // Add group key columns
      for (const [col, value] of Object.entries(group.keyValues)) {
        outputRow[col] = value;
      }

      // Add aggregate results
      for (let i = 0; i < this.plan.aggregates.length; i++) {
        const alias = this.plan.aggregates[i].alias;
        outputRow[alias] = getAggregateResult(group.aggregateStates[i]);
      }

      return outputRow;
    }
  }

  async close(): Promise<void> {
    await this.input.close();
    this.groups.clear();
    this.resultIterator = null;
  }

  columns(): string[] {
    return this.outputColumns;
  }
}
