/**
 * TopN Operator - Optimized Sort with Limit
 *
 * Uses a bounded heap to efficiently find the top N rows without fully sorting.
 * Time complexity: O(n log k) where n = input rows, k = limit
 * Space complexity: O(k) - only stores k rows in memory
 *
 * This is dramatically faster than Sort + Limit for large datasets with small limits.
 * For example, finding top 10 from 1M rows:
 * - Sort + Limit: O(n log n) = ~20M comparisons
 * - TopN heap: O(n log k) = ~3.3M comparisons (6x faster)
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
// MIN/MAX HEAP IMPLEMENTATION
// =============================================================================

/**
 * A min-heap that maintains the top-k elements for a given sort order.
 *
 * Strategy: We use a min-heap where "min" means the WORST element in our
 * desired output order. This way, when a new element comes in that's BETTER
 * than the worst element in the heap, we replace it.
 *
 * For ORDER BY x ASC (smallest first):
 * - The WORST element is the LARGEST, so the heap root should be the max
 * - We want a MAX-heap of the k smallest values
 *
 * For ORDER BY x DESC (largest first):
 * - The WORST element is the SMALLEST, so the heap root should be the min
 * - We want a MIN-heap of the k largest values
 */
class BoundedHeap {
  private heap: Row[] = [];
  private readonly maxSize: number;
  /** Original comparison for sorting: negative if a should come BEFORE b */
  private readonly sortCompare: (a: Row, b: Row) => number;

  /**
   * @param maxSize Maximum number of elements to keep
   * @param sortCompare Sort comparator: returns negative if a should come BEFORE b in final output
   */
  constructor(maxSize: number, sortCompare: (a: Row, b: Row) => number) {
    this.maxSize = maxSize;
    this.sortCompare = sortCompare;
  }

  push(row: Row): void {
    if (this.heap.length < this.maxSize) {
      // Heap not full, just add and restore heap property
      this.heap.push(row);
      this.siftUp(this.heap.length - 1);
    } else if (this.heap.length > 0 && this.sortCompare(row, this.heap[0]) < 0) {
      // New row is BETTER than the worst (root) in the heap, replace
      // sortCompare(row, heap[0]) < 0 means row should come BEFORE heap[0]
      this.heap[0] = row;
      this.siftDown(0);
    }
    // Otherwise, row is worse than everything in heap, discard it
  }

  /**
   * Extract all elements in sorted order (best to worst)
   */
  extractSorted(): Row[] {
    // Sort the heap contents using the original sort comparator
    return [...this.heap].sort(this.sortCompare);
  }

  size(): number {
    return this.heap.length;
  }

  /**
   * Sift up: The element at index may be WORSE than its parent.
   * In our inverted heap (worst at root), worse elements should move up.
   */
  private siftUp(index: number): void {
    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2);
      // If current is WORSE than parent (should come AFTER parent in sort order),
      // then current should be higher in the heap
      if (this.sortCompare(this.heap[index], this.heap[parentIndex]) >= 0) {
        // Current is worse or equal, swap with parent
        this.swap(index, parentIndex);
        index = parentIndex;
      } else {
        break;
      }
    }
  }

  /**
   * Sift down: The element at index may be BETTER than its children.
   * In our inverted heap (worst at root), the worst child should move up.
   */
  private siftDown(index: number): void {
    while (true) {
      const leftChild = 2 * index + 1;
      const rightChild = 2 * index + 2;
      let worst = index;

      // Find the worst among current, left child, right child
      if (leftChild < this.heap.length &&
          this.sortCompare(this.heap[leftChild], this.heap[worst]) > 0) {
        // Left child is worse (comes after in sort order)
        worst = leftChild;
      }

      if (rightChild < this.heap.length &&
          this.sortCompare(this.heap[rightChild], this.heap[worst]) > 0) {
        // Right child is worse
        worst = rightChild;
      }

      if (worst === index) break;

      this.swap(index, worst);
      index = worst;
    }
  }

  private swap(i: number, j: number): void {
    const temp = this.heap[i];
    this.heap[i] = this.heap[j];
    this.heap[j] = temp;
  }
}

// =============================================================================
// TOPN OPERATOR
// =============================================================================

/**
 * TopN operator - efficiently finds top N rows using a bounded heap.
 *
 * Instead of sorting all rows and taking the first N, we maintain a heap
 * of size N. This is O(n log k) instead of O(n log n).
 */
export class TopNOperator implements Operator {
  private plan: SortPlan;
  private input: Operator;
  private ctx!: ExecutionContext;
  private limit: number;
  private offset: number;
  private sortedRows: Row[] = [];
  private index = 0;

  constructor(
    plan: SortPlan,
    input: Operator,
    ctx: ExecutionContext,
    limit: number,
    offset: number = 0
  ) {
    this.plan = plan;
    this.input = input;
    this.ctx = ctx;
    this.limit = limit;
    this.offset = offset;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;
    this.sortedRows = [];
    this.index = 0;

    await this.input.open(ctx);

    // Create comparison function for ORDER BY
    const compare = this.createCompareFunction();

    // Use a bounded heap to find top (limit + offset) rows
    const heapSize = this.limit + this.offset;
    const heap = new BoundedHeap(heapSize, compare);

    // Stream all input rows through the heap
    let row: Row | null;
    while ((row = await this.input.next()) !== null) {
      heap.push(row);
    }

    // Extract sorted results
    const allResults = heap.extractSorted();

    // Apply offset
    this.sortedRows = allResults.slice(this.offset);
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

  /**
   * Create a comparison function based on ORDER BY specification.
   * Returns negative if a should come BEFORE b in the sorted output.
   */
  private createCompareFunction(): (a: Row, b: Row) => number {
    return (a: Row, b: Row) => {
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
    };
  }
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a TopN operator for SORT + LIMIT optimization.
 *
 * @param plan The sort plan
 * @param input The input operator
 * @param ctx Execution context
 * @param limit Maximum rows to return
 * @param offset Rows to skip before returning
 */
export function createTopNOperator(
  plan: SortPlan,
  input: Operator,
  ctx: ExecutionContext,
  limit: number,
  offset: number = 0
): TopNOperator {
  return new TopNOperator(plan, input, ctx, limit, offset);
}

/**
 * Determine if TopN optimization should be used.
 *
 * TopN is beneficial when:
 * - There's a LIMIT clause
 * - The limit is reasonably small (< 10% of estimated rows, or absolute threshold)
 *
 * @param limit The LIMIT value
 * @param estimatedRows Estimated number of input rows (if known)
 */
export function shouldUseTopN(limit: number, estimatedRows?: number): boolean {
  // Always use TopN for small limits
  if (limit <= 1000) {
    return true;
  }

  // If we know row count, use TopN if limit is < 10% of rows
  if (estimatedRows !== undefined && limit < estimatedRows * 0.1) {
    return true;
  }

  // For large limits with unknown row count, still use TopN
  // since it's never worse than sort (and often much better)
  return true;
}
