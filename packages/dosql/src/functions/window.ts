/**
 * SQL Window Functions
 *
 * Implements SQL-compatible window functions:
 * - ROW_NUMBER() - Sequential row number within partition
 * - RANK() - Rank with gaps for ties
 * - DENSE_RANK() - Rank without gaps
 * - NTILE(n) - Divide into n buckets
 * - LAG(expr, offset, default) - Value from previous row
 * - LEAD(expr, offset, default) - Value from next row
 * - FIRST_VALUE(expr) - First value in frame
 * - LAST_VALUE(expr) - Last value in frame
 * - NTH_VALUE(expr, n) - Nth value in frame
 *
 * Window functions operate over a "window" of rows defined by:
 * - PARTITION BY: Groups rows into partitions
 * - ORDER BY: Orders rows within each partition
 * - Frame specification: Defines which rows are included
 */

import type { SqlValue } from '../engine/types.js';
import type { FunctionSignature } from './registry.js';

// =============================================================================
// WINDOW FUNCTION TYPES
// =============================================================================

/**
 * Window frame boundary types
 */
export type FrameBoundaryType =
  | 'unboundedPreceding'
  | 'unboundedFollowing'
  | 'currentRow'
  | 'preceding'
  | 'following';

/**
 * Window frame boundary specification
 */
export interface FrameBoundary {
  type: FrameBoundaryType;
  offset?: number; // For 'preceding' and 'following'
}

/**
 * Window frame mode
 */
export type FrameMode = 'rows' | 'range' | 'groups';

/**
 * Window frame specification
 */
export interface WindowFrame {
  mode: FrameMode;
  start: FrameBoundary;
  end: FrameBoundary;
  exclusion?: 'currentRow' | 'group' | 'ties' | 'noOthers';
}

/**
 * Window specification for a function call
 */
export interface WindowSpec {
  partitionBy?: string[];
  orderBy?: { column: string; direction: 'asc' | 'desc'; nulls?: 'first' | 'last' }[];
  frame?: WindowFrame;
}

/**
 * A row with its window context
 */
export interface WindowRow {
  row: Record<string, SqlValue>;
  partitionIndex: number;
  rowIndexInPartition: number;
}

/**
 * Window function context passed to function implementations
 */
export interface WindowContext {
  /** All rows in the current partition (ordered) */
  partitionRows: Record<string, SqlValue>[];
  /** Index of current row within partition */
  currentIndex: number;
  /** Frame boundaries for current row */
  frameStart: number;
  frameEnd: number;
  /** The full window specification */
  spec: WindowSpec;
}

/**
 * Window function implementation signature
 */
export type WindowFunction = (
  ctx: WindowContext,
  ...args: SqlValue[]
) => SqlValue;

// =============================================================================
// DEFAULT FRAME SPECIFICATIONS
// =============================================================================

/**
 * Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 * (Standard SQL default when ORDER BY is specified)
 */
export const DEFAULT_FRAME_WITH_ORDER: WindowFrame = {
  mode: 'range',
  start: { type: 'unboundedPreceding' },
  end: { type: 'currentRow' },
};

/**
 * Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 * (When no ORDER BY is specified)
 */
export const DEFAULT_FRAME_WITHOUT_ORDER: WindowFrame = {
  mode: 'range',
  start: { type: 'unboundedPreceding' },
  end: { type: 'unboundedFollowing' },
};

/**
 * Get the default frame based on whether ORDER BY is specified
 */
export function getDefaultFrame(hasOrderBy: boolean): WindowFrame {
  return hasOrderBy ? DEFAULT_FRAME_WITH_ORDER : DEFAULT_FRAME_WITHOUT_ORDER;
}

// =============================================================================
// FRAME BOUNDARY CALCULATION
// =============================================================================

/**
 * Calculate frame boundaries for a given row
 */
export function calculateFrameBoundaries(
  partitionRows: Record<string, SqlValue>[],
  currentIndex: number,
  frame: WindowFrame,
  orderByColumns?: string[]
): { start: number; end: number } {
  const partitionSize = partitionRows.length;

  let start: number;
  let end: number;

  if (frame.mode === 'rows') {
    // ROWS mode: physical row offsets
    start = calculateRowBoundary(currentIndex, frame.start, partitionSize);
    end = calculateRowBoundary(currentIndex, frame.end, partitionSize);
  } else if (frame.mode === 'range') {
    // RANGE mode: logical value-based boundaries
    start = calculateRangeBoundary(
      partitionRows,
      currentIndex,
      frame.start,
      orderByColumns || [],
      'start'
    );
    end = calculateRangeBoundary(
      partitionRows,
      currentIndex,
      frame.end,
      orderByColumns || [],
      'end'
    );
  } else {
    // GROUPS mode: peer group boundaries
    start = calculateGroupBoundary(
      partitionRows,
      currentIndex,
      frame.start,
      orderByColumns || [],
      'start'
    );
    end = calculateGroupBoundary(
      partitionRows,
      currentIndex,
      frame.end,
      orderByColumns || [],
      'end'
    );
  }

  // Clamp to valid indices
  start = Math.max(0, start);
  end = Math.min(partitionSize - 1, end);

  return { start, end };
}

/**
 * Calculate row-based boundary (ROWS mode)
 */
function calculateRowBoundary(
  currentIndex: number,
  boundary: FrameBoundary,
  partitionSize: number
): number {
  switch (boundary.type) {
    case 'unboundedPreceding':
      return 0;
    case 'unboundedFollowing':
      return partitionSize - 1;
    case 'currentRow':
      return currentIndex;
    case 'preceding':
      return currentIndex - (boundary.offset ?? 0);
    case 'following':
      return currentIndex + (boundary.offset ?? 0);
    default:
      return currentIndex;
  }
}

/**
 * Calculate range-based boundary (RANGE mode)
 */
function calculateRangeBoundary(
  rows: Record<string, SqlValue>[],
  currentIndex: number,
  boundary: FrameBoundary,
  orderByColumns: string[],
  side: 'start' | 'end'
): number {
  switch (boundary.type) {
    case 'unboundedPreceding':
      return 0;
    case 'unboundedFollowing':
      return rows.length - 1;
    case 'currentRow': {
      // For RANGE, currentRow includes all peers with same value
      if (orderByColumns.length === 0) {
        return side === 'start' ? 0 : rows.length - 1;
      }
      return findPeerBoundary(rows, currentIndex, orderByColumns, side);
    }
    case 'preceding':
    case 'following': {
      // Value-based offset (for numeric ORDER BY columns)
      if (orderByColumns.length === 0) {
        return side === 'start' ? 0 : rows.length - 1;
      }
      return findValueBoundary(
        rows,
        currentIndex,
        orderByColumns,
        boundary.offset ?? 0,
        boundary.type === 'preceding',
        side
      );
    }
    default:
      return currentIndex;
  }
}

/**
 * Calculate group-based boundary (GROUPS mode)
 */
function calculateGroupBoundary(
  rows: Record<string, SqlValue>[],
  currentIndex: number,
  boundary: FrameBoundary,
  orderByColumns: string[],
  side: 'start' | 'end'
): number {
  if (orderByColumns.length === 0) {
    // Without ORDER BY, all rows are in the same group
    return side === 'start' ? 0 : rows.length - 1;
  }

  // Find all group boundaries
  const groups = findPeerGroups(rows, orderByColumns);
  const currentGroup = groups.findIndex(g =>
    currentIndex >= g.start && currentIndex <= g.end
  );

  switch (boundary.type) {
    case 'unboundedPreceding':
      return 0;
    case 'unboundedFollowing':
      return rows.length - 1;
    case 'currentRow': {
      const group = groups[currentGroup];
      return side === 'start' ? group.start : group.end;
    }
    case 'preceding': {
      const targetGroup = Math.max(0, currentGroup - (boundary.offset ?? 0));
      const group = groups[targetGroup];
      return side === 'start' ? group.start : groups[currentGroup].end;
    }
    case 'following': {
      const targetGroup = Math.min(groups.length - 1, currentGroup + (boundary.offset ?? 0));
      const group = groups[targetGroup];
      return side === 'start' ? groups[currentGroup].start : group.end;
    }
    default:
      return currentIndex;
  }
}

/**
 * Find peer group boundaries
 */
function findPeerGroups(
  rows: Record<string, SqlValue>[],
  orderByColumns: string[]
): { start: number; end: number }[] {
  const groups: { start: number; end: number }[] = [];
  let groupStart = 0;

  for (let i = 1; i <= rows.length; i++) {
    if (i === rows.length || !arePeers(rows[i - 1], rows[i], orderByColumns)) {
      groups.push({ start: groupStart, end: i - 1 });
      groupStart = i;
    }
  }

  return groups;
}

/**
 * Find the boundary of peer rows (same ORDER BY values)
 */
function findPeerBoundary(
  rows: Record<string, SqlValue>[],
  currentIndex: number,
  orderByColumns: string[],
  side: 'start' | 'end'
): number {
  const currentRow = rows[currentIndex];

  if (side === 'start') {
    let i = currentIndex;
    while (i > 0 && arePeers(rows[i - 1], currentRow, orderByColumns)) {
      i--;
    }
    return i;
  } else {
    let i = currentIndex;
    while (i < rows.length - 1 && arePeers(rows[i + 1], currentRow, orderByColumns)) {
      i++;
    }
    return i;
  }
}

/**
 * Check if two rows are peers (same ORDER BY values)
 */
function arePeers(
  row1: Record<string, SqlValue>,
  row2: Record<string, SqlValue>,
  orderByColumns: string[]
): boolean {
  for (const col of orderByColumns) {
    if (!compareValues(row1[col], row2[col])) {
      return false;
    }
  }
  return true;
}

/**
 * Compare two SQL values for equality
 */
function compareValues(a: SqlValue, b: SqlValue): boolean {
  if (a === null && b === null) return true;
  if (a === null || b === null) return false;
  if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime();
  if (a instanceof Uint8Array && b instanceof Uint8Array) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }
  return a === b;
}

/**
 * Find value-based boundary for RANGE with offset
 */
function findValueBoundary(
  rows: Record<string, SqlValue>[],
  currentIndex: number,
  orderByColumns: string[],
  offset: number,
  isPreceding: boolean,
  side: 'start' | 'end'
): number {
  // This is a simplified implementation that works for numeric columns
  const col = orderByColumns[0];
  const currentValue = rows[currentIndex][col];

  if (typeof currentValue !== 'number' && typeof currentValue !== 'bigint') {
    // For non-numeric values, fall back to row-based behavior
    return isPreceding
      ? currentIndex - offset
      : currentIndex + offset;
  }

  const targetValue = isPreceding
    ? (currentValue as number) - offset
    : (currentValue as number) + offset;

  if (isPreceding) {
    // Find first row with value >= targetValue
    for (let i = 0; i < rows.length; i++) {
      const val = rows[i][col];
      if (typeof val === 'number' && val >= targetValue) {
        return side === 'start' ? i : currentIndex;
      }
    }
    return 0;
  } else {
    // Find last row with value <= targetValue
    for (let i = rows.length - 1; i >= 0; i--) {
      const val = rows[i][col];
      if (typeof val === 'number' && val <= targetValue) {
        return side === 'start' ? currentIndex : i;
      }
    }
    return rows.length - 1;
  }
}

// =============================================================================
// RANKING WINDOW FUNCTIONS
// =============================================================================

/**
 * ROW_NUMBER() - Assigns sequential numbers starting from 1
 */
export function rowNumber(ctx: WindowContext): SqlValue {
  return ctx.currentIndex + 1;
}

/**
 * RANK() - Assigns rank with gaps for ties
 * Rows with equal ORDER BY values get the same rank
 */
export function rank(ctx: WindowContext): SqlValue {
  const { partitionRows, currentIndex, spec } = ctx;
  const orderByColumns = spec.orderBy?.map(o => o.column) || [];

  if (orderByColumns.length === 0 || currentIndex === 0) {
    return 1;
  }

  // Count how many rows are strictly before this row (not peers)
  let rank = 1;
  const currentRow = partitionRows[currentIndex];

  for (let i = 0; i < currentIndex; i++) {
    if (!arePeers(partitionRows[i], currentRow, orderByColumns)) {
      rank = i + 2; // Will be overwritten until we hit a peer
    }
  }

  // Find the first peer
  for (let i = 0; i < currentIndex; i++) {
    if (arePeers(partitionRows[i], currentRow, orderByColumns)) {
      return i + 1;
    }
  }

  return currentIndex + 1;
}

/**
 * DENSE_RANK() - Assigns rank without gaps for ties
 */
export function denseRank(ctx: WindowContext): SqlValue {
  const { partitionRows, currentIndex, spec } = ctx;
  const orderByColumns = spec.orderBy?.map(o => o.column) || [];

  if (orderByColumns.length === 0 || currentIndex === 0) {
    return 1;
  }

  // Count distinct preceding values
  let denseRank = 1;
  let prevRow = partitionRows[0];

  for (let i = 1; i <= currentIndex; i++) {
    const currentRow = partitionRows[i];
    if (!arePeers(prevRow, currentRow, orderByColumns)) {
      denseRank++;
      prevRow = currentRow;
    }
    if (i === currentIndex) break;
  }

  return denseRank;
}

/**
 * NTILE(n) - Divides partition into n buckets
 */
export function ntile(ctx: WindowContext, n: SqlValue): SqlValue {
  if (n === null || typeof n !== 'number' || n <= 0) {
    return null;
  }

  const { partitionRows, currentIndex } = ctx;
  const buckets = Math.floor(n);
  const rowCount = partitionRows.length;

  // Calculate bucket sizes
  // If rowCount doesn't divide evenly, first (rowCount % n) buckets get one extra row
  const baseSize = Math.floor(rowCount / buckets);
  const extraRows = rowCount % buckets;

  // Determine which bucket this row falls into
  let bucket = 0;
  let rowsAssigned = 0;

  for (let b = 0; b < buckets; b++) {
    const bucketSize = baseSize + (b < extraRows ? 1 : 0);
    if (currentIndex < rowsAssigned + bucketSize) {
      bucket = b + 1;
      break;
    }
    rowsAssigned += bucketSize;
  }

  return bucket;
}

/**
 * PERCENT_RANK() - Relative rank as percentage (0 to 1)
 */
export function percentRank(ctx: WindowContext): SqlValue {
  const { partitionRows, currentIndex, spec } = ctx;
  const orderByColumns = spec.orderBy?.map(o => o.column) || [];

  if (partitionRows.length <= 1) {
    return 0;
  }

  // Get the rank
  const r = rank(ctx) as number;

  return (r - 1) / (partitionRows.length - 1);
}

/**
 * CUME_DIST() - Cumulative distribution (0 to 1)
 */
export function cumeDist(ctx: WindowContext): SqlValue {
  const { partitionRows, currentIndex, spec } = ctx;
  const orderByColumns = spec.orderBy?.map(o => o.column) || [];

  if (partitionRows.length === 0) {
    return null;
  }

  // Count rows with values less than or equal to current row
  const currentRow = partitionRows[currentIndex];
  let countLE = 0;

  for (const row of partitionRows) {
    if (arePeers(row, currentRow, orderByColumns) ||
        compareRowsLE(row, currentRow, orderByColumns, spec.orderBy || [])) {
      countLE++;
    }
  }

  return countLE / partitionRows.length;
}

/**
 * Compare if row1 <= row2 based on ORDER BY columns
 */
function compareRowsLE(
  row1: Record<string, SqlValue>,
  row2: Record<string, SqlValue>,
  orderByColumns: string[],
  orderSpec: { column: string; direction: 'asc' | 'desc' }[]
): boolean {
  for (let i = 0; i < orderByColumns.length; i++) {
    const col = orderByColumns[i];
    const v1 = row1[col];
    const v2 = row2[col];
    const dir = orderSpec[i]?.direction || 'asc';

    if (v1 === null && v2 === null) continue;
    if (v1 === null) return dir === 'asc'; // NULL is treated as smallest in ASC
    if (v2 === null) return dir !== 'asc';

    if (v1 < v2) return dir === 'asc';
    if (v1 > v2) return dir !== 'asc';
  }
  return true; // Equal
}

// =============================================================================
// VALUE ACCESS WINDOW FUNCTIONS
// =============================================================================

/**
 * LAG(expr, offset, default) - Access value from previous row
 */
export function lag(
  ctx: WindowContext,
  value: SqlValue,
  offset: SqlValue = 1,
  defaultValue: SqlValue = null
): SqlValue {
  const { partitionRows, currentIndex } = ctx;
  const off = typeof offset === 'number' ? offset : 1;
  const targetIndex = currentIndex - off;

  if (targetIndex < 0 || targetIndex >= partitionRows.length) {
    return defaultValue;
  }

  // The value parameter is already the expression evaluated at current row
  // We need to evaluate it at the target row, but since we receive pre-evaluated
  // values, we work with column references. This is handled by the executor.
  // For now, we return the value at the target position from a column.
  return value; // This gets handled specially in the executor
}

/**
 * LAG implementation that works with column name
 */
export function lagColumn(
  ctx: WindowContext,
  column: string,
  offset: number = 1,
  defaultValue: SqlValue = null
): SqlValue {
  const { partitionRows, currentIndex } = ctx;
  const targetIndex = currentIndex - offset;

  if (targetIndex < 0 || targetIndex >= partitionRows.length) {
    return defaultValue;
  }

  return partitionRows[targetIndex][column] ?? defaultValue;
}

/**
 * LEAD(expr, offset, default) - Access value from following row
 */
export function lead(
  ctx: WindowContext,
  value: SqlValue,
  offset: SqlValue = 1,
  defaultValue: SqlValue = null
): SqlValue {
  const { partitionRows, currentIndex } = ctx;
  const off = typeof offset === 'number' ? offset : 1;
  const targetIndex = currentIndex + off;

  if (targetIndex < 0 || targetIndex >= partitionRows.length) {
    return defaultValue;
  }

  return value;
}

/**
 * LEAD implementation that works with column name
 */
export function leadColumn(
  ctx: WindowContext,
  column: string,
  offset: number = 1,
  defaultValue: SqlValue = null
): SqlValue {
  const { partitionRows, currentIndex } = ctx;
  const targetIndex = currentIndex + offset;

  if (targetIndex < 0 || targetIndex >= partitionRows.length) {
    return defaultValue;
  }

  return partitionRows[targetIndex][column] ?? defaultValue;
}

/**
 * FIRST_VALUE(expr) - First value in the window frame
 */
export function firstValue(ctx: WindowContext, column: string): SqlValue {
  const { partitionRows, frameStart } = ctx;

  if (frameStart < 0 || frameStart >= partitionRows.length) {
    return null;
  }

  return partitionRows[frameStart][column] ?? null;
}

/**
 * LAST_VALUE(expr) - Last value in the window frame
 */
export function lastValue(ctx: WindowContext, column: string): SqlValue {
  const { partitionRows, frameEnd } = ctx;

  if (frameEnd < 0 || frameEnd >= partitionRows.length) {
    return null;
  }

  return partitionRows[frameEnd][column] ?? null;
}

/**
 * NTH_VALUE(expr, n) - Nth value in the window frame
 */
export function nthValue(ctx: WindowContext, column: string, n: SqlValue): SqlValue {
  if (n === null || typeof n !== 'number' || n <= 0) {
    return null;
  }

  const { partitionRows, frameStart, frameEnd } = ctx;
  const targetIndex = frameStart + Math.floor(n) - 1;

  if (targetIndex < frameStart || targetIndex > frameEnd || targetIndex >= partitionRows.length) {
    return null;
  }

  return partitionRows[targetIndex][column] ?? null;
}

// =============================================================================
// AGGREGATE WINDOW FUNCTIONS
// =============================================================================

/**
 * SUM() over window frame
 */
export function windowSum(ctx: WindowContext, column: string): SqlValue {
  const { partitionRows, frameStart, frameEnd } = ctx;
  let sum = 0;
  let hasValue = false;

  for (let i = frameStart; i <= frameEnd && i < partitionRows.length; i++) {
    const val = partitionRows[i][column];
    if (val !== null && typeof val === 'number') {
      sum += val;
      hasValue = true;
    } else if (val !== null && typeof val === 'bigint') {
      sum += Number(val);
      hasValue = true;
    }
  }

  return hasValue ? sum : null;
}

/**
 * AVG() over window frame
 */
export function windowAvg(ctx: WindowContext, column: string): SqlValue {
  const { partitionRows, frameStart, frameEnd } = ctx;
  let sum = 0;
  let count = 0;

  for (let i = frameStart; i <= frameEnd && i < partitionRows.length; i++) {
    const val = partitionRows[i][column];
    if (val !== null && typeof val === 'number') {
      sum += val;
      count++;
    } else if (val !== null && typeof val === 'bigint') {
      sum += Number(val);
      count++;
    }
  }

  return count > 0 ? sum / count : null;
}

/**
 * COUNT() over window frame
 */
export function windowCount(ctx: WindowContext, column?: string): SqlValue {
  const { partitionRows, frameStart, frameEnd } = ctx;
  let count = 0;

  for (let i = frameStart; i <= frameEnd && i < partitionRows.length; i++) {
    if (column === undefined || column === '*') {
      count++;
    } else {
      const val = partitionRows[i][column];
      if (val !== null) {
        count++;
      }
    }
  }

  return count;
}

/**
 * MIN() over window frame
 */
export function windowMin(ctx: WindowContext, column: string): SqlValue {
  const { partitionRows, frameStart, frameEnd } = ctx;
  let min: SqlValue = null;

  for (let i = frameStart; i <= frameEnd && i < partitionRows.length; i++) {
    const val = partitionRows[i][column];
    if (val !== null) {
      if (min === null || val < min) {
        min = val;
      }
    }
  }

  return min;
}

/**
 * MAX() over window frame
 */
export function windowMax(ctx: WindowContext, column: string): SqlValue {
  const { partitionRows, frameStart, frameEnd } = ctx;
  let max: SqlValue = null;

  for (let i = frameStart; i <= frameEnd && i < partitionRows.length; i++) {
    const val = partitionRows[i][column];
    if (val !== null) {
      if (max === null || val > max) {
        max = val;
      }
    }
  }

  return max;
}

// =============================================================================
// FUNCTION SIGNATURES FOR REGISTRY
// =============================================================================

export const windowFunctionSignatures: Record<string, FunctionSignature> = {
  row_number: {
    name: 'row_number',
    params: [],
    returnType: 'number',
    description: 'Returns sequential row number starting from 1',
    isAggregate: false,
  },
  rank: {
    name: 'rank',
    params: [],
    returnType: 'number',
    description: 'Returns rank with gaps for ties',
    isAggregate: false,
  },
  dense_rank: {
    name: 'dense_rank',
    params: [],
    returnType: 'number',
    description: 'Returns rank without gaps for ties',
    isAggregate: false,
  },
  ntile: {
    name: 'ntile',
    params: [{ name: 'n', type: 'number' }],
    returnType: 'number',
    description: 'Divides partition into n buckets',
    isAggregate: false,
  },
  percent_rank: {
    name: 'percent_rank',
    params: [],
    returnType: 'number',
    description: 'Returns relative rank as percentage (0 to 1)',
    isAggregate: false,
  },
  cume_dist: {
    name: 'cume_dist',
    params: [],
    returnType: 'number',
    description: 'Returns cumulative distribution (0 to 1)',
    isAggregate: false,
  },
  lag: {
    name: 'lag',
    params: [
      { name: 'expr', type: 'any' },
      { name: 'offset', type: 'number', optional: true },
      { name: 'default', type: 'any', optional: true },
    ],
    returnType: 'any',
    description: 'Returns value from a previous row',
    isAggregate: false,
  },
  lead: {
    name: 'lead',
    params: [
      { name: 'expr', type: 'any' },
      { name: 'offset', type: 'number', optional: true },
      { name: 'default', type: 'any', optional: true },
    ],
    returnType: 'any',
    description: 'Returns value from a following row',
    isAggregate: false,
  },
  first_value: {
    name: 'first_value',
    params: [{ name: 'expr', type: 'any' }],
    returnType: 'any',
    description: 'Returns first value in the window frame',
    isAggregate: false,
  },
  last_value: {
    name: 'last_value',
    params: [{ name: 'expr', type: 'any' }],
    returnType: 'any',
    description: 'Returns last value in the window frame',
    isAggregate: false,
  },
  nth_value: {
    name: 'nth_value',
    params: [
      { name: 'expr', type: 'any' },
      { name: 'n', type: 'number' },
    ],
    returnType: 'any',
    description: 'Returns nth value in the window frame',
    isAggregate: false,
  },
};

/**
 * Check if a function name is a window function
 */
export function isWindowFunction(name: string): boolean {
  return name.toLowerCase() in windowFunctionSignatures;
}

/**
 * Window functions registry
 */
export const windowFunctions: Record<string, WindowFunction> = {
  row_number: rowNumber,
  rank: rank,
  dense_rank: denseRank,
  ntile: (ctx, n) => ntile(ctx, n),
  percent_rank: percentRank,
  cume_dist: cumeDist,
};

// Value access functions need special handling in the executor
// since they need access to specific columns
export const windowValueFunctions = {
  lag: lagColumn,
  lead: leadColumn,
  first_value: firstValue,
  last_value: lastValue,
  nth_value: nthValue,
};

// Aggregate functions over window frames
export const windowAggregateFunctions = {
  sum: windowSum,
  avg: windowAvg,
  count: windowCount,
  min: windowMin,
  max: windowMax,
};
