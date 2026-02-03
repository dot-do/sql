/**
 * Join Operator
 *
 * Implements SQL JOIN operations between two input relations.
 * Supports:
 * - INNER JOIN (only matching rows)
 * - LEFT OUTER JOIN (all left rows, matching right or nulls)
 * - RIGHT OUTER JOIN (all right rows, matching left or nulls)
 * - FULL OUTER JOIN (all rows from both sides)
 * - CROSS JOIN (cartesian product)
 *
 * Uses nested loop join by default, with hash join optimization
 * for equality conditions.
 *
 * Supports backpressure with memory limits for hash table builds.
 */

import {
  type JoinPlan,
  type ExecutionContext,
  type Operator,
  type Row,
  type SqlValue,
  type Expression,
  type Predicate,
  type BackpressureState,
  type BackpressureStats,
  type BackpressureOperator,
  type WatermarkConfig,
  type BackpressureController,
  DEFAULT_WATERMARKS,
} from '../types.js';
import { evaluatePredicate, evaluateExpression } from './filter.js';

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Prefix all keys in a row with a table name
 */
function prefixRow(row: Row, tableName: string): Row {
  const result: Row = {};
  for (const [key, value] of Object.entries(row)) {
    result[`${tableName}.${key}`] = value;
  }
  return result;
}

/**
 * Create a null-filled row with prefixed column names
 */
function createNullRow(columns: string[], tableName: string): Row {
  const result: Row = {};
  for (const col of columns) {
    result[`${tableName}.${col}`] = null;
  }
  return result;
}

/**
 * Extract table name from a plan node
 */
function getTableName(plan: JoinPlan['left'] | JoinPlan['right']): string {
  if (plan.type === 'scan') {
    return plan.alias || plan.table;
  }
  // For nested plans, generate a synthetic name
  return `_t${plan.id}`;
}

/**
 * Get columns from a plan node
 */
function getColumns(plan: JoinPlan['left'] | JoinPlan['right']): string[] {
  if (plan.type === 'scan') {
    return plan.columns;
  }
  // For other plan types, we'd need to derive columns
  // For now, return empty - the operator will use input.columns()
  return [];
}

/**
 * Extract join key from an equality condition
 * Returns [leftKey, rightKey] if the condition is an equality comparison
 */
function extractEqualityKeys(
  condition: Predicate | undefined,
  leftTable: string,
  rightTable: string
): { leftKey: string; rightKey: string } | null {
  if (!condition || condition.type !== 'comparison' || condition.op !== 'eq') {
    return null;
  }

  const left = condition.left;
  const right = condition.right;

  if (left.type !== 'columnRef' || right.type !== 'columnRef') {
    return null;
  }

  // Determine which side is which
  const leftCol = left.table === leftTable || !left.table ? left.column : null;
  const rightCol = right.table === rightTable || !right.table ? right.column : null;

  if (leftCol && rightCol) {
    return { leftKey: leftCol, rightKey: rightCol };
  }

  // Try the other direction
  const leftColAlt = right.table === leftTable || !right.table ? right.column : null;
  const rightColAlt = left.table === rightTable || !left.table ? left.column : null;

  if (leftColAlt && rightColAlt) {
    return { leftKey: leftColAlt, rightKey: rightColAlt };
  }

  return null;
}

// =============================================================================
// JOIN OPERATOR
// =============================================================================

/**
 * Join operator implementation with backpressure support
 *
 * Implements various join types using either nested loop or hash join.
 * Supports memory limits for hash table builds to prevent OOM.
 */
export class JoinOperator implements BackpressureOperator {
  private plan: JoinPlan;
  private leftInput: Operator;
  private rightInput: Operator;
  private ctx!: ExecutionContext;

  // State for nested loop join
  private currentLeftRow: Row | null = null;
  private rightRows: Row[] = [];
  private rightIndex = 0;
  private leftMatched = false;

  // State for hash join
  private useHashJoin = false;
  private hashTable: Map<string, Row[]> = new Map();
  private hashKey: { leftKey: string; rightKey: string } | null = null;

  // State for tracking unmatched rows (for outer joins)
  private matchedRightIndices: Set<number> = new Set();
  private leftExhausted = false;
  private emittingUnmatchedRight = false;
  private unmatchedRightIndex = 0;

  // Table names for column prefixing
  private leftTableName: string;
  private rightTableName: string;

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
  private memoryLimitExceeded = false;

  constructor(
    plan: JoinPlan,
    leftInput: Operator,
    rightInput: Operator,
    ctx: ExecutionContext
  ) {
    this.plan = plan;
    this.leftInput = leftInput;
    this.rightInput = rightInput;
    this.ctx = ctx;

    this.leftTableName = getTableName(plan.left);
    this.rightTableName = getTableName(plan.right);

    // Determine if we can use hash join
    this.hashKey = extractEqualityKeys(plan.condition, this.leftTableName, this.rightTableName);
    this.useHashJoin = plan.algorithm === 'hash' || (this.hashKey !== null && plan.joinType === 'inner');
  }

  /**
   * Set the backpressure controller for this operator
   */
  setController(controller: BackpressureController): void {
    this.controller = controller;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;

    // Reset state
    this.currentLeftRow = null;
    this.rightRows = [];
    this.rightIndex = 0;
    this.leftMatched = false;
    this.hashTable.clear();
    this.matchedRightIndices.clear();
    this.leftExhausted = false;
    this.emittingUnmatchedRight = false;
    this.unmatchedRightIndex = 0;

    // Reset backpressure state
    this.backpressureState = 'flowing';
    this.memoryUsage = 0;
    this.bufferedRows = 0;
    this.pausePromise = null;
    this.pauseResolve = null;
    this.memoryLimitExceeded = false;

    await this.leftInput.open(ctx);
    await this.rightInput.open(ctx);

    // For hash join or outer joins, materialize the right side
    if (this.useHashJoin || this.plan.joinType === 'right' || this.plan.joinType === 'full') {
      await this.materializeRight();
    }

    // Build hash table if using hash join
    if (this.useHashJoin && this.hashKey) {
      this.buildHashTable();
    }
  }

  /**
   * Materialize all right-side rows into memory with memory limits
   */
  private async materializeRight(): Promise<void> {
    this.rightRows = [];
    this.memoryUsage = 0;
    this.bufferedRows = 0;
    let row: Row | null;

    while ((row = await this.rightInput.next()) !== null) {
      // Estimate memory for this row
      const rowSize = this.estimateRowSize(row);
      this.memoryUsage += rowSize;
      this.bufferedRows++;

      this.rightRows.push(row);

      // Check memory limits - notify via backpressure state change
      if (this.memoryUsage >= this.watermarks.highWatermark) {
        this.memoryLimitExceeded = true;
        if (this.backpressureState === 'flowing') {
          this.backpressureState = 'draining';
          this.notifyBackpressureChange();
        }
      }

      // Report memory usage to controller
      if (this.controller) {
        this.controller.reportMemoryUsage(this, this.memoryUsage);
      }
    }
  }

  /**
   * Estimate memory size of a row (rough approximation)
   */
  private estimateRowSize(row: Row): number {
    let size = 0;
    for (const value of Object.values(row)) {
      if (value === null) {
        size += 8;
      } else if (typeof value === 'string') {
        size += value.length * 2; // UTF-16
      } else if (typeof value === 'number') {
        size += 8;
      } else if (typeof value === 'bigint') {
        size += 16;
      } else if (typeof value === 'boolean') {
        size += 4;
      } else if (value instanceof Date) {
        size += 8;
      } else if (value instanceof Uint8Array) {
        size += value.length;
      }
    }
    return size + 64; // Object overhead
  }

  /**
   * Build hash table from right rows
   */
  private buildHashTable(): void {
    if (!this.hashKey) return;

    for (const row of this.rightRows) {
      const keyValue = row[this.hashKey.rightKey];
      const key = String(keyValue);
      const existing = this.hashTable.get(key);
      if (existing) {
        existing.push(row);
      } else {
        this.hashTable.set(key, [row]);
      }
    }
  }

  async next(): Promise<Row | null> {
    // Handle emitting unmatched right rows for FULL OUTER JOIN
    if (this.emittingUnmatchedRight) {
      return this.nextUnmatchedRight();
    }

    // Use hash join if applicable
    if (this.useHashJoin) {
      return this.nextHashJoin();
    }

    // Otherwise use nested loop join
    return this.nextNestedLoop();
  }

  /**
   * Hash join implementation
   */
  private async nextHashJoin(): Promise<Row | null> {
    if (!this.hashKey) return null;

    while (true) {
      // Get next left row
      if (this.currentLeftRow === null) {
        this.currentLeftRow = await this.leftInput.next();
        this.rightIndex = 0;
        this.leftMatched = false;

        if (this.currentLeftRow === null) {
          this.leftExhausted = true;
          // For FULL OUTER, emit unmatched right rows
          if (this.plan.joinType === 'full') {
            this.emittingUnmatchedRight = true;
            return this.nextUnmatchedRight();
          }
          return null;
        }
      }

      // Look up matching right rows in hash table
      const leftKeyValue = this.currentLeftRow[this.hashKey.leftKey];
      const key = String(leftKeyValue);
      const matchingRightRows = this.hashTable.get(key) || [];

      // Emit matching rows
      while (this.rightIndex < matchingRightRows.length) {
        const rightRow = matchingRightRows[this.rightIndex];
        const rightOriginalIndex = this.rightRows.indexOf(rightRow);
        this.rightIndex++;
        this.leftMatched = true;

        if (rightOriginalIndex >= 0) {
          this.matchedRightIndices.add(rightOriginalIndex);
        }

        return this.combineRows(this.currentLeftRow, rightRow);
      }

      // Handle LEFT OUTER: emit null-padded row if no matches
      if (!this.leftMatched && (this.plan.joinType === 'left' || this.plan.joinType === 'full')) {
        const result = this.combineRows(this.currentLeftRow, null);
        this.currentLeftRow = null;
        return result;
      }

      // Move to next left row
      this.currentLeftRow = null;
    }
  }

  /**
   * Nested loop join implementation
   */
  private async nextNestedLoop(): Promise<Row | null> {
    while (true) {
      // Get next left row if needed
      if (this.currentLeftRow === null) {
        this.currentLeftRow = await this.leftInput.next();
        this.rightIndex = 0;
        this.leftMatched = false;

        if (this.currentLeftRow === null) {
          this.leftExhausted = true;
          // For FULL/RIGHT OUTER, emit unmatched right rows
          if (this.plan.joinType === 'full' || this.plan.joinType === 'right') {
            this.emittingUnmatchedRight = true;
            return this.nextUnmatchedRight();
          }
          return null;
        }

        // For first left row (or CROSS/INNER without materialization), materialize right
        if (this.rightRows.length === 0 && this.plan.joinType !== 'right' && this.plan.joinType !== 'full') {
          await this.materializeRight();
        }
      }

      // Iterate through right rows
      while (this.rightIndex < this.rightRows.length) {
        const rightRow = this.rightRows[this.rightIndex];
        this.rightIndex++;

        // Check join condition
        if (this.matchesCondition(this.currentLeftRow, rightRow)) {
          this.leftMatched = true;
          this.matchedRightIndices.add(this.rightIndex - 1);
          return this.combineRows(this.currentLeftRow, rightRow);
        }
      }

      // All right rows checked for current left row
      // Handle LEFT OUTER: emit null-padded row if no matches
      if (!this.leftMatched && (this.plan.joinType === 'left' || this.plan.joinType === 'full')) {
        const result = this.combineRows(this.currentLeftRow, null);
        this.currentLeftRow = null;
        return result;
      }

      // Move to next left row
      this.currentLeftRow = null;
    }
  }

  /**
   * Emit unmatched right rows for RIGHT/FULL OUTER joins
   */
  private nextUnmatchedRight(): Row | null {
    while (this.unmatchedRightIndex < this.rightRows.length) {
      const idx = this.unmatchedRightIndex;
      this.unmatchedRightIndex++;

      if (!this.matchedRightIndices.has(idx)) {
        return this.combineRows(null, this.rightRows[idx]);
      }
    }
    return null;
  }

  /**
   * Check if a pair of rows matches the join condition
   */
  private matchesCondition(leftRow: Row, rightRow: Row): boolean {
    if (!this.plan.condition) {
      // CROSS JOIN: always matches
      return true;
    }

    // Combine rows for predicate evaluation
    const combinedRow = this.combineRows(leftRow, rightRow);
    return evaluatePredicate(this.plan.condition, combinedRow);
  }

  /**
   * Combine a left and right row into a single row with prefixed column names
   */
  private combineRows(leftRow: Row | null, rightRow: Row | null): Row {
    const result: Row = {};

    if (leftRow) {
      // Prefix left columns
      for (const [key, value] of Object.entries(leftRow)) {
        // Check if already prefixed
        if (key.includes('.')) {
          result[key] = value;
        } else {
          result[`${this.leftTableName}.${key}`] = value;
        }
      }
    } else {
      // Null row for left side
      const leftColumns = this.leftInput.columns();
      for (const col of leftColumns) {
        if (col.includes('.')) {
          result[col] = null;
        } else {
          result[`${this.leftTableName}.${col}`] = null;
        }
      }
    }

    if (rightRow) {
      // Prefix right columns
      for (const [key, value] of Object.entries(rightRow)) {
        // Check if already prefixed
        if (key.includes('.')) {
          result[key] = value;
        } else {
          result[`${this.rightTableName}.${key}`] = value;
        }
      }
    } else {
      // Null row for right side
      const rightColumns = this.rightInput.columns();
      for (const col of rightColumns) {
        if (col.includes('.')) {
          result[col] = null;
        } else {
          result[`${this.rightTableName}.${col}`] = null;
        }
      }
    }

    return result;
  }

  async close(): Promise<void> {
    await this.leftInput.close();
    await this.rightInput.close();
    this.rightRows = [];
    this.hashTable.clear();

    // Reset backpressure state
    this.backpressureState = 'flowing';
    this.memoryUsage = 0;
    this.bufferedRows = 0;
    this.pausePromise = null;
    this.pauseResolve = null;
    this.memoryLimitExceeded = false;
    if (this.controller) {
      this.controller.reportMemoryUsage(this, 0);
    }
  }

  columns(): string[] {
    // Combine columns from both inputs with table prefixes
    const leftCols = this.leftInput.columns().map((c) =>
      c.includes('.') ? c : `${this.leftTableName}.${c}`
    );
    const rightCols = this.rightInput.columns().map((c) =>
      c.includes('.') ? c : `${this.rightTableName}.${c}`
    );
    return [...leftCols, ...rightCols];
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

  /**
   * Check if memory limit was exceeded during hash build
   */
  isMemoryLimitExceeded(): boolean {
    return this.memoryLimitExceeded;
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
