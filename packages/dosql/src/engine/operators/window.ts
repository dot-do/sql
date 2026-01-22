/**
 * Window Operator
 *
 * Executes window functions over partitioned and sorted data.
 * The operator:
 * 1. Partitions rows into groups based on PARTITION BY
 * 2. Sorts within partitions based on ORDER BY
 * 3. Calculates frame boundaries for each row
 * 4. Evaluates window functions for each row
 */

import {
  type ExecutionContext,
  type Operator,
  type Row,
  type SqlValue,
  type BasePlanNode,
  type Expression,
  type SortSpec,
} from '../types.js';
import { evaluateExpression } from './filter.js';
import {
  type WindowSpec,
  type WindowFrame,
  type WindowContext,
  calculateFrameBoundaries,
  getDefaultFrame,
  rowNumber,
  rank,
  denseRank,
  ntile,
  percentRank,
  cumeDist,
  lagColumn,
  leadColumn,
  firstValue,
  lastValue,
  nthValue,
  windowSum,
  windowAvg,
  windowCount,
  windowMin,
  windowMax,
} from '../../functions/window.js';

// =============================================================================
// WINDOW PLAN NODE
// =============================================================================

/**
 * Window function definition in a plan
 */
export interface WindowFunctionDef {
  /** Function name (row_number, rank, etc.) */
  name: string;
  /** Arguments to the function */
  args: Expression[];
  /** Window specification */
  windowSpec: WindowSpec;
  /** Output column alias */
  alias: string;
}

/**
 * Window plan node
 */
export interface WindowPlan extends BasePlanNode {
  type: 'window';
  input: any; // QueryPlan (avoid circular import)
  /** Window functions to evaluate */
  windowFunctions: WindowFunctionDef[];
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Compare two SQL values for sorting
 */
function compareValues(a: SqlValue, b: SqlValue, direction: 'asc' | 'desc', nullsFirst?: boolean): number {
  // Handle null comparisons
  if (a === null && b === null) return 0;
  if (a === null) return nullsFirst ? -1 : 1;
  if (b === null) return nullsFirst ? 1 : -1;

  // Compare by type
  let result: number;
  if (typeof a === 'number' && typeof b === 'number') {
    result = a - b;
  } else if (typeof a === 'bigint' && typeof b === 'bigint') {
    result = a < b ? -1 : a > b ? 1 : 0;
  } else if (a instanceof Date && b instanceof Date) {
    result = a.getTime() - b.getTime();
  } else if (typeof a === 'string' && typeof b === 'string') {
    result = a.localeCompare(b);
  } else if (a instanceof Uint8Array && b instanceof Uint8Array) {
    const minLen = Math.min(a.length, b.length);
    for (let i = 0; i < minLen; i++) {
      if (a[i] !== b[i]) {
        result = a[i] - b[i];
        break;
      }
    }
    result = result! ?? a.length - b.length;
  } else {
    result = String(a).localeCompare(String(b));
  }

  return direction === 'desc' ? -result : result;
}

/**
 * Get key for partitioning
 */
function getPartitionKey(row: Row, partitionBy: string[]): string {
  if (partitionBy.length === 0) return '';
  return partitionBy.map(col => JSON.stringify(row[col])).join('\0');
}

/**
 * Sort rows within a partition
 */
function sortPartition(
  rows: Row[],
  orderBy: { column: string; direction: 'asc' | 'desc'; nulls?: 'first' | 'last' }[]
): Row[] {
  if (orderBy.length === 0) return rows;

  return [...rows].sort((a, b) => {
    for (const spec of orderBy) {
      const nullsFirst = spec.nulls === 'first' ||
        (spec.nulls === undefined && spec.direction === 'desc');
      const cmp = compareValues(a[spec.column], b[spec.column], spec.direction, nullsFirst);
      if (cmp !== 0) return cmp;
    }
    return 0;
  });
}

/**
 * Evaluate a window function
 */
function evaluateWindowFunction(
  name: string,
  args: Expression[],
  ctx: WindowContext,
  currentRow: Row
): SqlValue {
  const lowerName = name.toLowerCase();

  switch (lowerName) {
    // Ranking functions
    case 'row_number':
      return rowNumber(ctx);
    case 'rank':
      return rank(ctx);
    case 'dense_rank':
      return denseRank(ctx);
    case 'ntile': {
      const n = args.length > 0 ? evaluateExpression(args[0], currentRow) : null;
      return ntile(ctx, n);
    }
    case 'percent_rank':
      return percentRank(ctx);
    case 'cume_dist':
      return cumeDist(ctx);

    // Value access functions
    case 'lag': {
      if (args.length === 0) return null;
      const columnExpr = args[0];
      if (columnExpr.type !== 'columnRef') return null;
      const offset = args.length > 1 ? evaluateExpression(args[1], currentRow) : 1;
      const defaultVal = args.length > 2 ? evaluateExpression(args[2], currentRow) : null;
      return lagColumn(ctx, columnExpr.column, typeof offset === 'number' ? offset : 1, defaultVal);
    }
    case 'lead': {
      if (args.length === 0) return null;
      const columnExpr = args[0];
      if (columnExpr.type !== 'columnRef') return null;
      const offset = args.length > 1 ? evaluateExpression(args[1], currentRow) : 1;
      const defaultVal = args.length > 2 ? evaluateExpression(args[2], currentRow) : null;
      return leadColumn(ctx, columnExpr.column, typeof offset === 'number' ? offset : 1, defaultVal);
    }
    case 'first_value': {
      if (args.length === 0) return null;
      const columnExpr = args[0];
      if (columnExpr.type !== 'columnRef') return null;
      return firstValue(ctx, columnExpr.column);
    }
    case 'last_value': {
      if (args.length === 0) return null;
      const columnExpr = args[0];
      if (columnExpr.type !== 'columnRef') return null;
      return lastValue(ctx, columnExpr.column);
    }
    case 'nth_value': {
      if (args.length < 2) return null;
      const columnExpr = args[0];
      if (columnExpr.type !== 'columnRef') return null;
      const n = evaluateExpression(args[1], currentRow);
      return nthValue(ctx, columnExpr.column, n);
    }

    // Aggregate window functions
    case 'sum': {
      if (args.length === 0) return null;
      const columnExpr = args[0];
      if (columnExpr.type !== 'columnRef') {
        // Try to evaluate expression for each row in frame
        return evaluateAggregateOverFrame(ctx, 'sum', args[0]);
      }
      return windowSum(ctx, columnExpr.column);
    }
    case 'avg': {
      if (args.length === 0) return null;
      const columnExpr = args[0];
      if (columnExpr.type !== 'columnRef') {
        return evaluateAggregateOverFrame(ctx, 'avg', args[0]);
      }
      return windowAvg(ctx, columnExpr.column);
    }
    case 'count': {
      if (args.length === 0 || (args[0].type === 'columnRef' && args[0].column === '*')) {
        return windowCount(ctx);
      }
      const columnExpr = args[0];
      if (columnExpr.type !== 'columnRef') {
        return evaluateAggregateOverFrame(ctx, 'count', args[0]);
      }
      return windowCount(ctx, columnExpr.column);
    }
    case 'min': {
      if (args.length === 0) return null;
      const columnExpr = args[0];
      if (columnExpr.type !== 'columnRef') {
        return evaluateAggregateOverFrame(ctx, 'min', args[0]);
      }
      return windowMin(ctx, columnExpr.column);
    }
    case 'max': {
      if (args.length === 0) return null;
      const columnExpr = args[0];
      if (columnExpr.type !== 'columnRef') {
        return evaluateAggregateOverFrame(ctx, 'max', args[0]);
      }
      return windowMax(ctx, columnExpr.column);
    }

    default:
      throw new Error(`Unknown window function: ${name}`);
  }
}

/**
 * Evaluate an aggregate function over the window frame
 * (for expressions, not just column references)
 */
function evaluateAggregateOverFrame(
  ctx: WindowContext,
  aggName: string,
  expr: Expression
): SqlValue {
  const { partitionRows, frameStart, frameEnd } = ctx;
  const values: SqlValue[] = [];

  for (let i = frameStart; i <= frameEnd && i < partitionRows.length; i++) {
    const val = evaluateExpression(expr, partitionRows[i]);
    values.push(val);
  }

  switch (aggName) {
    case 'sum': {
      let sum = 0;
      let hasValue = false;
      for (const v of values) {
        if (v !== null && typeof v === 'number') {
          sum += v;
          hasValue = true;
        }
      }
      return hasValue ? sum : null;
    }
    case 'avg': {
      let sum = 0;
      let count = 0;
      for (const v of values) {
        if (v !== null && typeof v === 'number') {
          sum += v;
          count++;
        }
      }
      return count > 0 ? sum / count : null;
    }
    case 'count': {
      return values.filter(v => v !== null).length;
    }
    case 'min': {
      let min: SqlValue = null;
      for (const v of values) {
        if (v !== null && (min === null || v < min)) {
          min = v;
        }
      }
      return min;
    }
    case 'max': {
      let max: SqlValue = null;
      for (const v of values) {
        if (v !== null && (max === null || v > max)) {
          max = v;
        }
      }
      return max;
    }
    default:
      return null;
  }
}

// =============================================================================
// WINDOW OPERATOR
// =============================================================================

/**
 * Window operator implementation
 *
 * Processes all input rows, partitions them, sorts them,
 * and evaluates window functions.
 */
export class WindowOperator implements Operator {
  private plan: WindowPlan;
  private input: Operator;
  private ctx!: ExecutionContext;
  private outputRows: Row[] = [];
  private outputIndex = 0;
  private processed = false;

  constructor(plan: WindowPlan, input: Operator) {
    this.plan = plan;
    this.input = input;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;
    await this.input.open(ctx);
    this.outputRows = [];
    this.outputIndex = 0;
    this.processed = false;
  }

  async next(): Promise<Row | null> {
    // First time: collect all input and process
    if (!this.processed) {
      await this.processAllRows();
      this.processed = true;
    }

    // Return next output row
    if (this.outputIndex >= this.outputRows.length) {
      return null;
    }
    return this.outputRows[this.outputIndex++];
  }

  /**
   * Collect all input rows and process window functions
   */
  private async processAllRows(): Promise<void> {
    // Collect all input rows
    const allRows: Row[] = [];
    let row: Row | null;
    while ((row = await this.input.next()) !== null) {
      allRows.push(row);
    }

    if (allRows.length === 0) return;

    // For each window function, we may need different partitioning/sorting
    // Group functions by their window spec
    const specGroups = this.groupByWindowSpec();

    // Process each group
    for (const [specKey, funcDefs] of specGroups) {
      const spec = funcDefs[0].windowSpec;
      const partitionBy = spec.partitionBy?.map(col => col) || [];
      const orderBy = spec.orderBy?.map(o => ({
        column: o.column,
        direction: o.direction,
        nulls: o.nulls,
      })) || [];

      // Partition rows
      const partitions = this.partitionRows(allRows, partitionBy);

      // Process each partition
      for (const partition of partitions) {
        // Sort partition
        const sortedPartition = sortPartition(partition, orderBy);

        // Get frame spec (use default if not specified)
        const frame = spec.frame || getDefaultFrame(orderBy.length > 0);
        const orderByColumns = orderBy.map(o => o.column);

        // Evaluate window functions for each row
        for (let i = 0; i < sortedPartition.length; i++) {
          const currentRow = sortedPartition[i];

          // Calculate frame boundaries
          const { start, end } = calculateFrameBoundaries(
            sortedPartition,
            i,
            frame,
            orderByColumns
          );

          // Create window context
          const windowCtx: WindowContext = {
            partitionRows: sortedPartition,
            currentIndex: i,
            frameStart: start,
            frameEnd: end,
            spec,
          };

          // Evaluate each window function
          for (const funcDef of funcDefs) {
            const result = evaluateWindowFunction(
              funcDef.name,
              funcDef.args,
              windowCtx,
              currentRow
            );

            // Add result to row
            currentRow[funcDef.alias] = result;
          }
        }

        // Add processed rows to output
        this.outputRows.push(...sortedPartition);
      }
    }
  }

  /**
   * Group window functions by their window specification
   */
  private groupByWindowSpec(): Map<string, WindowFunctionDef[]> {
    const groups = new Map<string, WindowFunctionDef[]>();

    for (const funcDef of this.plan.windowFunctions) {
      const key = this.windowSpecKey(funcDef.windowSpec);
      const existing = groups.get(key);
      if (existing) {
        existing.push(funcDef);
      } else {
        groups.set(key, [funcDef]);
      }
    }

    return groups;
  }

  /**
   * Create a key for window specification (for grouping)
   */
  private windowSpecKey(spec: WindowSpec): string {
    const parts: string[] = [];

    if (spec.partitionBy) {
      parts.push('P:' + spec.partitionBy.map(p => p).join(','));
    }
    if (spec.orderBy) {
      parts.push('O:' + spec.orderBy.map(o =>
        `${o.column}:${o.direction}:${o.nulls || 'default'}`
      ).join(','));
    }
    if (spec.frame) {
      parts.push(`F:${spec.frame.mode}:${JSON.stringify(spec.frame.start)}:${JSON.stringify(spec.frame.end)}`);
    }

    return parts.join('|');
  }

  /**
   * Partition rows by PARTITION BY columns
   */
  private partitionRows(rows: Row[], partitionBy: string[]): Row[][] {
    if (partitionBy.length === 0) {
      // No partitioning - all rows in one partition
      return [rows];
    }

    const partitions = new Map<string, Row[]>();

    for (const row of rows) {
      const key = getPartitionKey(row, partitionBy);
      const existing = partitions.get(key);
      if (existing) {
        existing.push(row);
      } else {
        partitions.set(key, [row]);
      }
    }

    return Array.from(partitions.values());
  }

  async close(): Promise<void> {
    await this.input.close();
    this.outputRows = [];
    this.outputIndex = 0;
  }

  columns(): string[] {
    // Original columns plus window function aliases
    const inputCols = this.input.columns();
    const windowCols = this.plan.windowFunctions.map(f => f.alias);
    return [...inputCols, ...windowCols];
  }
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a window operator
 */
export function createWindowOperator(
  plan: WindowPlan,
  input: Operator
): WindowOperator {
  return new WindowOperator(plan, input);
}

// =============================================================================
// UTILITY FUNCTIONS FOR QUERY PLANNING
// =============================================================================

/**
 * Check if an expression contains a window function
 */
export function containsWindowFunction(expr: Expression): boolean {
  if (expr.type === 'function') {
    const fnExpr = expr as any;
    if (fnExpr.over !== undefined) {
      return true;
    }
  }

  // Check children
  switch (expr.type) {
    case 'binary': {
      const binExpr = expr as any;
      return containsWindowFunction(binExpr.left) || containsWindowFunction(binExpr.right);
    }
    case 'unary': {
      const unaryExpr = expr as any;
      return containsWindowFunction(unaryExpr.operand);
    }
    case 'case': {
      const caseExpr = expr as any;
      for (const { condition, result } of caseExpr.when || []) {
        if (containsWindowFunction(condition) || containsWindowFunction(result)) {
          return true;
        }
      }
      if (caseExpr.else && containsWindowFunction(caseExpr.else)) {
        return true;
      }
      return false;
    }
    default:
      return false;
  }
}

/**
 * Extract window function definitions from expressions
 */
export function extractWindowFunctions(
  expressions: { expr: Expression; alias: string }[]
): WindowFunctionDef[] {
  const windowFunctions: WindowFunctionDef[] = [];

  for (const { expr, alias } of expressions) {
    if (expr.type === 'function') {
      const fnExpr = expr as any;
      if (fnExpr.over !== undefined) {
        windowFunctions.push({
          name: fnExpr.name,
          args: fnExpr.args || [],
          windowSpec: typeof fnExpr.over === 'string'
            ? { baseName: fnExpr.over }
            : fnExpr.over,
          alias,
        });
      }
    }
  }

  return windowFunctions;
}
