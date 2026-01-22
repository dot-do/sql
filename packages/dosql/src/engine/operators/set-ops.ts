/**
 * Set Operations Operators
 *
 * Implements UNION, INTERSECT, EXCEPT set operations at the execution level.
 *
 * Supports:
 * - UNION: Combines results, removes duplicates (default)
 * - UNION ALL: Combines results, keeps duplicates
 * - INTERSECT: Returns common rows, removes duplicates
 * - INTERSECT ALL: Returns common rows, keeps duplicates (bag semantics)
 * - EXCEPT: Returns rows in left but not right, removes duplicates
 * - EXCEPT ALL: Returns rows in left but not right, keeps duplicates
 */

import {
  type ExecutionContext,
  type Operator,
  type Row,
  type QueryPlan,
  type BasePlanNode,
  type SortSpec,
} from '../types.js';

// =============================================================================
// PLAN NODE TYPES
// =============================================================================

/**
 * Set operation type
 */
export type SetOperationType = 'UNION' | 'INTERSECT' | 'EXCEPT';

/**
 * Plan node for set operations
 */
export interface SetOperationPlan extends BasePlanNode {
  type: 'setOperation';
  operator: SetOperationType;
  all: boolean;
  left: QueryPlan;
  right: QueryPlan;
}

/**
 * Plan node for compound SELECT with multiple set operations
 */
export interface CompoundSelectPlan extends BasePlanNode {
  type: 'compoundSelect';
  base: QueryPlan;
  operations: {
    operator: SetOperationType;
    all: boolean;
    right: QueryPlan;
  }[];
  orderBy?: SortSpec[];
  limit?: number;
  offset?: number;
}

// =============================================================================
// ROW HASHING
// =============================================================================

/**
 * Create a hash key for a row (for deduplication)
 * Uses JSON.stringify for simplicity, could be optimized with FNV or xxHash
 */
function rowKey(row: Row): string {
  // Sort keys for consistent ordering
  const sorted = Object.keys(row).sort();
  const values = sorted.map(k => row[k]);
  return JSON.stringify(values);
}

/**
 * Create a hash key for specific columns only
 */
function rowKeyColumns(row: Row, columns: string[]): string {
  const values = columns.map(k => row[k]);
  return JSON.stringify(values);
}

// =============================================================================
// UNION OPERATOR
// =============================================================================

/**
 * Union operator - combines results from two inputs
 *
 * UNION: removes duplicates using hash set
 * UNION ALL: keeps all rows
 */
export class UnionOperator implements Operator {
  private left: Operator;
  private right: Operator;
  private all: boolean;
  private ctx!: ExecutionContext;
  private outputColumns: string[] = [];
  private readingLeft = true;
  private seen: Set<string> | null = null;

  constructor(left: Operator, right: Operator, all: boolean) {
    this.left = left;
    this.right = right;
    this.all = all;
    if (!this.all) {
      this.seen = new Set();
    }
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;
    await this.left.open(ctx);
    await this.right.open(ctx);

    // Output columns come from left operand
    this.outputColumns = this.left.columns();
    this.readingLeft = true;
  }

  async next(): Promise<Row | null> {
    while (true) {
      // Read from current side
      const source = this.readingLeft ? this.left : this.right;
      const row = await source.next();

      if (row === null) {
        if (this.readingLeft) {
          // Switch to right side
          this.readingLeft = false;
          continue;
        }
        // Both sides exhausted
        return null;
      }

      // For UNION (not ALL), deduplicate
      if (!this.all && this.seen) {
        const key = rowKey(row);
        if (this.seen.has(key)) {
          continue;
        }
        this.seen.add(key);
      }

      return row;
    }
  }

  async close(): Promise<void> {
    await this.left.close();
    await this.right.close();
    this.seen = null;
  }

  columns(): string[] {
    return this.outputColumns;
  }
}

// =============================================================================
// INTERSECT OPERATOR
// =============================================================================

/**
 * Intersect operator - returns rows common to both inputs
 *
 * INTERSECT: removes duplicates, returns each matching row once
 * INTERSECT ALL: uses bag semantics (count-based intersection)
 */
export class IntersectOperator implements Operator {
  private left: Operator;
  private right: Operator;
  private all: boolean;
  private ctx!: ExecutionContext;
  private outputColumns: string[] = [];

  // For INTERSECT: set of right-side row keys
  private rightSet: Set<string> | null = null;
  private leftSeen: Set<string> | null = null;

  // For INTERSECT ALL: count of right-side rows
  private rightCounts: Map<string, number> | null = null;

  constructor(left: Operator, right: Operator, all: boolean) {
    this.left = left;
    this.right = right;
    this.all = all;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;
    await this.left.open(ctx);
    await this.right.open(ctx);

    this.outputColumns = this.left.columns();

    // Materialize right side into hash structure
    if (this.all) {
      // INTERSECT ALL: count occurrences
      this.rightCounts = new Map();
      while (true) {
        const row = await this.right.next();
        if (row === null) break;
        const key = rowKey(row);
        this.rightCounts.set(key, (this.rightCounts.get(key) || 0) + 1);
      }
    } else {
      // INTERSECT: just track presence
      this.rightSet = new Set();
      this.leftSeen = new Set();
      while (true) {
        const row = await this.right.next();
        if (row === null) break;
        this.rightSet.add(rowKey(row));
      }
    }
  }

  async next(): Promise<Row | null> {
    while (true) {
      const row = await this.left.next();
      if (row === null) return null;

      const key = rowKey(row);

      if (this.all && this.rightCounts) {
        // INTERSECT ALL: decrement count if present
        const count = this.rightCounts.get(key);
        if (count && count > 0) {
          this.rightCounts.set(key, count - 1);
          return row;
        }
      } else if (this.rightSet && this.leftSeen) {
        // INTERSECT: return if in right set and not already returned
        if (this.rightSet.has(key) && !this.leftSeen.has(key)) {
          this.leftSeen.add(key);
          return row;
        }
      }
    }
  }

  async close(): Promise<void> {
    await this.left.close();
    await this.right.close();
    this.rightSet = null;
    this.rightCounts = null;
    this.leftSeen = null;
  }

  columns(): string[] {
    return this.outputColumns;
  }
}

// =============================================================================
// EXCEPT OPERATOR
// =============================================================================

/**
 * Except operator - returns rows in left but not in right
 *
 * EXCEPT: removes duplicates, each row appears at most once
 * EXCEPT ALL: uses bag semantics (count-based difference)
 */
export class ExceptOperator implements Operator {
  private left: Operator;
  private right: Operator;
  private all: boolean;
  private ctx!: ExecutionContext;
  private outputColumns: string[] = [];

  // For EXCEPT: set of right-side row keys
  private rightSet: Set<string> | null = null;
  private leftSeen: Set<string> | null = null;

  // For EXCEPT ALL: count of right-side rows
  private rightCounts: Map<string, number> | null = null;

  constructor(left: Operator, right: Operator, all: boolean) {
    this.left = left;
    this.right = right;
    this.all = all;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;
    await this.left.open(ctx);
    await this.right.open(ctx);

    this.outputColumns = this.left.columns();

    // Materialize right side into hash structure
    if (this.all) {
      // EXCEPT ALL: count occurrences
      this.rightCounts = new Map();
      while (true) {
        const row = await this.right.next();
        if (row === null) break;
        const key = rowKey(row);
        this.rightCounts.set(key, (this.rightCounts.get(key) || 0) + 1);
      }
    } else {
      // EXCEPT: just track presence
      this.rightSet = new Set();
      this.leftSeen = new Set();
      while (true) {
        const row = await this.right.next();
        if (row === null) break;
        this.rightSet.add(rowKey(row));
      }
    }
  }

  async next(): Promise<Row | null> {
    while (true) {
      const row = await this.left.next();
      if (row === null) return null;

      const key = rowKey(row);

      if (this.all && this.rightCounts) {
        // EXCEPT ALL: decrement count if present in right, skip if count > 0
        const count = this.rightCounts.get(key);
        if (count && count > 0) {
          this.rightCounts.set(key, count - 1);
          continue; // Skip this row
        }
        return row;
      } else if (this.rightSet !== null && this.leftSeen !== null) {
        // EXCEPT: return if not in right set and not already returned
        if (!this.rightSet.has(key) && !this.leftSeen.has(key)) {
          this.leftSeen.add(key);
          return row;
        }
      }
    }
  }

  async close(): Promise<void> {
    await this.left.close();
    await this.right.close();
    this.rightSet = null;
    this.rightCounts = null;
    this.leftSeen = null;
  }

  columns(): string[] {
    return this.outputColumns;
  }
}

// =============================================================================
// SET OPERATION FACTORY
// =============================================================================

/**
 * Create a set operation operator based on type
 */
export function createSetOperationOperator(
  operator: SetOperationType,
  left: Operator,
  right: Operator,
  all: boolean,
): Operator {
  switch (operator) {
    case 'UNION':
      return new UnionOperator(left, right, all);
    case 'INTERSECT':
      return new IntersectOperator(left, right, all);
    case 'EXCEPT':
      return new ExceptOperator(left, right, all);
    default:
      throw new Error(`Unknown set operation: ${operator}`);
  }
}

// =============================================================================
// COMPOUND SELECT OPERATOR
// =============================================================================

/**
 * Compound select operator - handles multiple chained set operations
 * with proper precedence and final ORDER BY/LIMIT
 */
export class CompoundSelectOperator implements Operator {
  private plan: CompoundSelectPlan;
  private baseOperator: Operator;
  private createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator;
  private ctx!: ExecutionContext;
  private result: Operator | null = null;
  private buffer: Row[] = [];
  private bufferIndex = 0;
  private outputColumns: string[] = [];

  constructor(
    plan: CompoundSelectPlan,
    createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator,
  ) {
    this.plan = plan;
    this.baseOperator = createOperator(plan.base, {} as ExecutionContext);
    this.createOperator = createOperator;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;

    // Build the operator chain
    let current: Operator = this.createOperator(this.plan.base, ctx);

    for (const op of this.plan.operations) {
      const rightOp = this.createOperator(op.right, ctx);
      current = createSetOperationOperator(op.operator, current, rightOp, op.all);
    }

    this.result = current;
    await this.result.open(ctx);
    this.outputColumns = this.result.columns();

    // If ORDER BY or LIMIT, we need to buffer and sort
    if (this.plan.orderBy || this.plan.limit !== undefined) {
      await this.materializeAndSort();
    }
  }

  private async materializeAndSort(): Promise<void> {
    // Collect all rows
    const rows: Row[] = [];
    while (true) {
      const row = await this.result!.next();
      if (row === null) break;
      rows.push(row);
    }

    // Sort if ORDER BY specified
    if (this.plan.orderBy && this.plan.orderBy.length > 0) {
      rows.sort((a, b) => this.compareRows(a, b));
    }

    // Apply OFFSET and LIMIT
    let start = this.plan.offset ?? 0;
    let end = this.plan.limit !== undefined ? start + this.plan.limit : rows.length;

    this.buffer = rows.slice(start, end);
    this.bufferIndex = 0;
  }

  private compareRows(a: Row, b: Row): number {
    if (!this.plan.orderBy) return 0;

    for (const spec of this.plan.orderBy) {
      const colName = spec.expr.type === 'columnRef' ? spec.expr.column : 'unknown';
      const aVal = a[colName];
      const bVal = b[colName];

      let cmp = 0;
      if (aVal === null && bVal === null) {
        cmp = 0;
      } else if (aVal === null) {
        // NULLS handling
        cmp = spec.nullsFirst ? -1 : 1;
      } else if (bVal === null) {
        cmp = spec.nullsFirst ? 1 : -1;
      } else if (aVal < bVal) {
        cmp = -1;
      } else if (aVal > bVal) {
        cmp = 1;
      }

      if (cmp !== 0) {
        return spec.direction === 'desc' ? -cmp : cmp;
      }
    }

    return 0;
  }

  async next(): Promise<Row | null> {
    // If we buffered results (for ORDER BY/LIMIT)
    if (this.buffer.length > 0) {
      if (this.bufferIndex < this.buffer.length) {
        return this.buffer[this.bufferIndex++];
      }
      return null;
    }

    // Otherwise, stream from result
    return this.result!.next();
  }

  async close(): Promise<void> {
    if (this.result) {
      await this.result.close();
    }
    this.buffer = [];
  }

  columns(): string[] {
    return this.outputColumns;
  }
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Check if a plan is a set operation
 */
export function isSetOperationPlan(plan: QueryPlan): plan is SetOperationPlan {
  return (plan as any).type === 'setOperation';
}

/**
 * Check if a plan is a compound select
 */
export function isCompoundSelectPlan(plan: QueryPlan): plan is CompoundSelectPlan {
  return (plan as any).type === 'compoundSelect';
}

/**
 * Get precedence for a set operation (for parsing)
 * INTERSECT binds tighter than UNION/EXCEPT
 */
export function getSetOperationPrecedence(op: SetOperationType): number {
  switch (op) {
    case 'INTERSECT':
      return 2;
    case 'UNION':
    case 'EXCEPT':
      return 1;
    default:
      return 0;
  }
}

// =============================================================================
// EXECUTION HELPERS
// =============================================================================

/**
 * Execute a set operation plan directly (for testing)
 */
export async function executeSetOperation(
  operator: SetOperationType,
  leftRows: Row[],
  rightRows: Row[],
  all: boolean,
): Promise<Row[]> {
  // Create mock operators for testing
  const leftOp = createArrayOperator(leftRows);
  const rightOp = createArrayOperator(rightRows);

  const setOp = createSetOperationOperator(operator, leftOp, rightOp, all);

  // Mock context
  const ctx = {} as ExecutionContext;
  await setOp.open(ctx);

  const result: Row[] = [];
  while (true) {
    const row = await setOp.next();
    if (row === null) break;
    result.push(row);
  }

  await setOp.close();
  return result;
}

/**
 * Create an operator from an array of rows (for testing)
 */
function createArrayOperator(rows: Row[]): Operator {
  let index = 0;
  const cols = rows.length > 0 ? Object.keys(rows[0]) : [];

  return {
    async open() { index = 0; },
    async next() { return index < rows.length ? rows[index++] : null; },
    async close() {},
    columns() { return cols; },
  };
}
