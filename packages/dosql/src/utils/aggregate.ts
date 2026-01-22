/**
 * Aggregate Utilities
 *
 * Shared aggregation functions for both single-node and distributed execution.
 */

// =============================================================================
// TYPES
// =============================================================================

/**
 * Supported aggregate function types
 */
export type AggregateFunction = 'count' | 'sum' | 'avg' | 'min' | 'max';

/**
 * SQL-compatible value type
 */
export type SqlValue = string | number | boolean | bigint | null | undefined;

/**
 * State for tracking aggregation computation
 */
export interface AggregateState {
  func: AggregateFunction;
  count: number;
  sum: number | bigint | null;
  min: SqlValue;
  max: SqlValue;
}

// =============================================================================
// STATE MANAGEMENT
// =============================================================================

/**
 * Create initial state for an aggregate function
 *
 * @param func - The aggregate function type
 * @returns Initial aggregate state
 */
export function createAggregateState(func: AggregateFunction | string): AggregateState {
  const normalizedFunc = func.toLowerCase() as AggregateFunction;
  return {
    func: normalizedFunc,
    count: 0,
    sum: null,
    min: null,
    max: null,
  };
}

/**
 * Update aggregate state with a new value
 *
 * @param state - Current aggregate state
 * @param value - New value to incorporate
 */
export function updateAggregateState(state: AggregateState, value: SqlValue): void {
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
    if (state.min === null || (value !== null && compareSqlValues(value, state.min) < 0)) {
      state.min = value;
    }
  }

  // MAX
  if (state.func === 'max') {
    if (state.max === null || (value !== null && compareSqlValues(value, state.max) > 0)) {
      state.max = value;
    }
  }
}

/**
 * Get final result from aggregate state
 *
 * @param state - Final aggregate state
 * @returns The computed aggregate value
 */
export function getAggregateResult(state: AggregateState): SqlValue {
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
// MERGE OPERATIONS (For distributed aggregation)
// =============================================================================

/**
 * Merge two aggregate states (for combining partial results)
 *
 * @param state1 - First aggregate state
 * @param state2 - Second aggregate state
 * @returns Merged aggregate state
 */
export function mergeAggregateStates(
  state1: AggregateState,
  state2: AggregateState
): AggregateState {
  if (state1.func !== state2.func) {
    throw new Error(`Cannot merge different aggregate functions: ${state1.func} vs ${state2.func}`);
  }

  const merged: AggregateState = {
    func: state1.func,
    count: state1.count + state2.count,
    sum: null,
    min: null,
    max: null,
  };

  // Merge SUM
  if (state1.sum !== null && state2.sum !== null) {
    if (typeof state1.sum === 'bigint' || typeof state2.sum === 'bigint') {
      merged.sum = BigInt(state1.sum ?? 0) + BigInt(state2.sum ?? 0);
    } else {
      merged.sum = ((state1.sum as number) || 0) + ((state2.sum as number) || 0);
    }
  } else {
    merged.sum = state1.sum ?? state2.sum;
  }

  // Merge MIN
  if (state1.min !== null && state2.min !== null) {
    merged.min = compareSqlValues(state1.min, state2.min) <= 0 ? state1.min : state2.min;
  } else {
    merged.min = state1.min ?? state2.min;
  }

  // Merge MAX
  if (state1.max !== null && state2.max !== null) {
    merged.max = compareSqlValues(state1.max, state2.max) >= 0 ? state1.max : state2.max;
  } else {
    merged.max = state1.max ?? state2.max;
  }

  return merged;
}

/**
 * Combine COUNT results from multiple partial counts
 *
 * @param partialCounts - Array of partial count values
 * @returns Total count
 */
export function combineCount(partialCounts: number[]): number {
  return partialCounts.reduce((sum, count) => sum + count, 0);
}

/**
 * Combine SUM results from multiple partial sums
 *
 * @param partialSums - Array of partial sum values
 * @returns Total sum
 */
export function combineSum(partialSums: (number | bigint | null)[]): number | bigint | null {
  const validSums = partialSums.filter((v): v is number | bigint => v !== null);
  if (validSums.length === 0) return null;

  const hasBigInt = validSums.some((v) => typeof v === 'bigint');
  if (hasBigInt) {
    return validSums.reduce((acc, val) => {
      const a = typeof acc === 'bigint' ? acc : BigInt(acc);
      const b = typeof val === 'bigint' ? val : BigInt(val);
      return a + b;
    }, 0n as bigint);
  }

  return (validSums as number[]).reduce((acc, val) => acc + val, 0);
}

/**
 * Combine AVG results using partial sums and counts
 *
 * @param partialSums - Array of partial sum values
 * @param partialCounts - Array of partial count values
 * @returns Combined average
 */
export function combineAvg(
  partialSums: (number | null)[],
  partialCounts: number[]
): number | null {
  let totalSum = 0;
  let totalCount = 0;

  for (let i = 0; i < partialSums.length; i++) {
    const sum = partialSums[i];
    const count = partialCounts[i];
    if (sum !== null && count > 0) {
      totalSum += sum;
      totalCount += count;
    }
  }

  return totalCount > 0 ? totalSum / totalCount : null;
}

/**
 * Find MIN across multiple partial MIN values
 *
 * @param partialMins - Array of partial min values
 * @returns Global minimum
 */
export function combineMin<T extends SqlValue>(partialMins: T[]): T | null {
  const validMins = partialMins.filter((v): v is NonNullable<T> => v !== null && v !== undefined);
  if (validMins.length === 0) return null;

  return validMins.reduce((min, val) => {
    return compareSqlValues(val, min) < 0 ? val : min;
  });
}

/**
 * Find MAX across multiple partial MAX values
 *
 * @param partialMaxes - Array of partial max values
 * @returns Global maximum
 */
export function combineMax<T extends SqlValue>(partialMaxes: T[]): T | null {
  const validMaxes = partialMaxes.filter((v): v is NonNullable<T> => v !== null && v !== undefined);
  if (validMaxes.length === 0) return null;

  return validMaxes.reduce((max, val) => {
    return compareSqlValues(val, max) > 0 ? val : max;
  });
}

// =============================================================================
// COMPARISON UTILITIES
// =============================================================================

/**
 * Compare two SQL values
 *
 * @param a - First value
 * @param b - Second value
 * @returns -1 if a < b, 0 if equal, 1 if a > b
 */
export function compareSqlValues(a: SqlValue, b: SqlValue): number {
  // Nulls sort to end
  if (a === null || a === undefined) return b === null || b === undefined ? 0 : 1;
  if (b === null || b === undefined) return -1;

  // Type-specific comparison
  if (typeof a === 'number' && typeof b === 'number') {
    return a - b;
  }

  if (typeof a === 'bigint' && typeof b === 'bigint') {
    return a < b ? -1 : a > b ? 1 : 0;
  }

  if (typeof a === 'string' && typeof b === 'string') {
    return a.localeCompare(b);
  }

  if (typeof a === 'boolean' && typeof b === 'boolean') {
    return a === b ? 0 : a ? 1 : -1;
  }

  // Mixed types: convert to string and compare
  return String(a).localeCompare(String(b));
}

/**
 * Check if a value is truthy for SQL WHERE/HAVING conditions
 *
 * @param value - Value to check
 * @returns true if the value is truthy in SQL semantics
 */
export function isSqlTruthy(value: SqlValue): boolean {
  if (value === null || value === undefined) return false;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'bigint') return value !== 0n;
  if (typeof value === 'string') return value.length > 0;
  return true;
}
