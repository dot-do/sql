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
  type SqlValue,
} from '../types.js';
import { assertNever } from '../../utils/assert-never.js';
import {
  FNV_OFFSET_BASIS,
  FNV_PRIME,
  fnv1aString,
  fnv1aNumber,
  fnv1aBigInt,
  fnv1aBytes,
} from '../../utils/hash.js';

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
 * Hash a single SQL value
 */
function hashValue(value: SqlValue, hash: number): number {
  if (value === null) {
    hash ^= 0x00; // null marker
    return Math.imul(hash, FNV_PRIME) >>> 0;
  }

  switch (typeof value) {
    case 'string':
      hash ^= 0x53; // 'S' for string
      hash = Math.imul(hash, FNV_PRIME) >>> 0;
      return fnv1aString(value, hash);

    case 'number':
      return fnv1aNumber(value, hash);

    case 'bigint':
      return fnv1aBigInt(value, hash);

    case 'boolean':
      hash ^= 0x4C; // 'L' for logical/boolean
      hash = Math.imul(hash, FNV_PRIME) >>> 0;
      hash ^= value ? 1 : 0;
      return Math.imul(hash, FNV_PRIME) >>> 0;

    default:
      // Date or Uint8Array
      if (value instanceof Date) {
        hash ^= 0x44; // 'D' for date
        hash = Math.imul(hash, FNV_PRIME) >>> 0;
        return fnv1aNumber(value.getTime(), hash);
      }
      if (value instanceof Uint8Array) {
        return fnv1aBytes(value, hash);
      }
      // Fallback: shouldn't happen with proper types
      hash ^= 0x55; // 'U' for unknown
      hash = Math.imul(hash, FNV_PRIME) >>> 0;
      return fnv1aString(String(value), hash);
  }
}

/**
 * Row identity type for deduplication.
 * Combines a numeric hash for fast comparison with a canonical string key
 * to handle hash collisions.
 */
interface RowIdentity {
  hash: number;
  key: string;
}

/**
 * Build a canonical string key for a row (for collision handling)
 * This is only used when there's a hash collision
 */
function buildCanonicalKey(row: Row, sortedKeys: string[]): string {
  const parts: string[] = [];
  for (const k of sortedKeys) {
    const v = row[k];
    if (v === null) {
      parts.push('N');
    } else if (typeof v === 'string') {
      // Escape special chars to avoid ambiguity
      parts.push('S' + v.length + ':' + v);
    } else if (typeof v === 'number') {
      parts.push('n' + v);
    } else if (typeof v === 'bigint') {
      parts.push('B' + v.toString());
    } else if (typeof v === 'boolean') {
      parts.push(v ? 'T' : 'F');
    } else if (v instanceof Date) {
      parts.push('D' + v.getTime());
    } else if (v instanceof Uint8Array) {
      // Base64-like encoding would be better but for now use hex
      parts.push('Y' + Array.from(v).map(b => b.toString(16).padStart(2, '0')).join(''));
    }
    parts.push('|');
  }
  return parts.join('');
}

/**
 * Create a row identity for deduplication
 * Uses FNV-1a hash for fast comparison with collision fallback
 */
function rowIdentity(row: Row): RowIdentity {
  // Sort keys for consistent ordering
  const sortedKeys = Object.keys(row).sort();

  // Compute hash
  let hash = FNV_OFFSET_BASIS;
  for (const k of sortedKeys) {
    // Hash the key name
    hash = fnv1aString(k, hash);
    // Hash the value
    hash = hashValue(row[k], hash);
  }

  return {
    hash,
    key: buildCanonicalKey(row, sortedKeys),
  };
}

/**
 * Hash set with collision handling for row deduplication
 */
class RowHashSet {
  private buckets = new Map<number, string[]>();

  has(identity: RowIdentity): boolean {
    const bucket = this.buckets.get(identity.hash);
    if (!bucket) return false;
    return bucket.includes(identity.key);
  }

  add(identity: RowIdentity): void {
    const bucket = this.buckets.get(identity.hash);
    if (bucket) {
      if (!bucket.includes(identity.key)) {
        bucket.push(identity.key);
      }
    } else {
      this.buckets.set(identity.hash, [identity.key]);
    }
  }

  clear(): void {
    this.buckets.clear();
  }
}

/**
 * Hash map with collision handling for row counts
 */
class RowHashMap {
  private buckets = new Map<number, Map<string, number>>();

  get(identity: RowIdentity): number | undefined {
    const bucket = this.buckets.get(identity.hash);
    if (!bucket) return undefined;
    return bucket.get(identity.key);
  }

  set(identity: RowIdentity, count: number): void {
    let bucket = this.buckets.get(identity.hash);
    if (!bucket) {
      bucket = new Map();
      this.buckets.set(identity.hash, bucket);
    }
    bucket.set(identity.key, count);
  }

  clear(): void {
    this.buckets.clear();
  }
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
  private seen: RowHashSet | null = null;

  constructor(left: Operator, right: Operator, all: boolean) {
    this.left = left;
    this.right = right;
    this.all = all;
    if (!this.all) {
      this.seen = new RowHashSet();
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

      // For UNION (not ALL), deduplicate using hash-based set
      if (!this.all && this.seen) {
        const identity = rowIdentity(row);
        if (this.seen.has(identity)) {
          continue;
        }
        this.seen.add(identity);
      }

      return row;
    }
  }

  async close(): Promise<void> {
    await this.left.close();
    await this.right.close();
    if (this.seen) {
      this.seen.clear();
      this.seen = null;
    }
  }

  columns(): string[] {
    return this.outputColumns;
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
  private rightSet: RowHashSet | null = null;
  private leftSeen: RowHashSet | null = null;

  // For INTERSECT ALL: count of right-side rows
  private rightCounts: RowHashMap | null = null;

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
      this.rightCounts = new RowHashMap();
      while (true) {
        const row = await this.right.next();
        if (row === null) break;
        const identity = rowIdentity(row);
        this.rightCounts.set(identity, (this.rightCounts.get(identity) || 0) + 1);
      }
    } else {
      // INTERSECT: just track presence
      this.rightSet = new RowHashSet();
      this.leftSeen = new RowHashSet();
      while (true) {
        const row = await this.right.next();
        if (row === null) break;
        this.rightSet.add(rowIdentity(row));
      }
    }
  }

  async next(): Promise<Row | null> {
    while (true) {
      const row = await this.left.next();
      if (row === null) return null;

      const identity = rowIdentity(row);

      if (this.all && this.rightCounts) {
        // INTERSECT ALL: decrement count if present
        const count = this.rightCounts.get(identity);
        if (count && count > 0) {
          this.rightCounts.set(identity, count - 1);
          return row;
        }
      } else if (this.rightSet && this.leftSeen) {
        // INTERSECT: return if in right set and not already returned
        if (this.rightSet.has(identity) && !this.leftSeen.has(identity)) {
          this.leftSeen.add(identity);
          return row;
        }
      }
    }
  }

  async close(): Promise<void> {
    await this.left.close();
    await this.right.close();
    if (this.rightSet) {
      this.rightSet.clear();
      this.rightSet = null;
    }
    if (this.rightCounts) {
      this.rightCounts.clear();
      this.rightCounts = null;
    }
    if (this.leftSeen) {
      this.leftSeen.clear();
      this.leftSeen = null;
    }
  }

  columns(): string[] {
    return this.outputColumns;
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
  private rightSet: RowHashSet | null = null;
  private leftSeen: RowHashSet | null = null;

  // For EXCEPT ALL: count of right-side rows
  private rightCounts: RowHashMap | null = null;

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
      this.rightCounts = new RowHashMap();
      while (true) {
        const row = await this.right.next();
        if (row === null) break;
        const identity = rowIdentity(row);
        this.rightCounts.set(identity, (this.rightCounts.get(identity) || 0) + 1);
      }
    } else {
      // EXCEPT: just track presence
      this.rightSet = new RowHashSet();
      this.leftSeen = new RowHashSet();
      while (true) {
        const row = await this.right.next();
        if (row === null) break;
        this.rightSet.add(rowIdentity(row));
      }
    }
  }

  async next(): Promise<Row | null> {
    while (true) {
      const row = await this.left.next();
      if (row === null) return null;

      const identity = rowIdentity(row);

      if (this.all && this.rightCounts) {
        // EXCEPT ALL: decrement count if present in right, skip if count > 0
        const count = this.rightCounts.get(identity);
        if (count && count > 0) {
          this.rightCounts.set(identity, count - 1);
          continue; // Skip this row
        }
        return row;
      } else if (this.rightSet !== null && this.leftSeen !== null) {
        // EXCEPT: return if not in right set and not already returned
        if (!this.rightSet.has(identity) && !this.leftSeen.has(identity)) {
          this.leftSeen.add(identity);
          return row;
        }
      }
    }
  }

  async close(): Promise<void> {
    await this.left.close();
    await this.right.close();
    if (this.rightSet) {
      this.rightSet.clear();
      this.rightSet = null;
    }
    if (this.rightCounts) {
      this.rightCounts.clear();
      this.rightCounts = null;
    }
    if (this.leftSeen) {
      this.leftSeen.clear();
      this.leftSeen = null;
    }
  }

  columns(): string[] {
    return this.outputColumns;
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
      return assertNever(operator, `Unknown set operation: ${operator}`);
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
        return this.buffer[this.bufferIndex++] ?? null;
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

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Check if a plan is a set operation
 */
export function isSetOperationPlan(plan: QueryPlan | SetOperationPlan | CompoundSelectPlan): plan is SetOperationPlan {
  return (plan as { type: string }).type === 'setOperation';
}

/**
 * Check if a plan is a compound select
 */
export function isCompoundSelectPlan(plan: QueryPlan | SetOperationPlan | CompoundSelectPlan): plan is CompoundSelectPlan {
  return (plan as { type: string }).type === 'compoundSelect';
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
      return assertNever(op, `Unknown set operation: ${op}`);
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
    async next(): Promise<Row | null> { return index < rows.length ? (rows[index++] ?? null) : null; },
    async close() {},
    columns() { return cols; },
  };
}
