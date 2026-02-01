/**
 * Aggregate Utilities Tests
 *
 * Tests for SQL aggregation functions including state management,
 * merge operations, and comparison utilities.
 */

import { describe, it, expect } from 'vitest';
import {
  createAggregateState,
  updateAggregateState,
  getAggregateResult,
  mergeAggregateStates,
  combineCount,
  combineSum,
  combineAvg,
  combineMin,
  combineMax,
  compareSqlValues,
  isSqlTruthy,
  type AggregateState,
} from '../aggregate.js';

// =============================================================================
// STATE MANAGEMENT
// =============================================================================

describe('createAggregateState', () => {
  it('should create initial state for count', () => {
    const state = createAggregateState('count');
    expect(state.func).toBe('count');
    expect(state.count).toBe(0);
    expect(state.sum).toBeNull();
    expect(state.min).toBeNull();
    expect(state.max).toBeNull();
  });

  it('should normalize function name to lowercase', () => {
    const state = createAggregateState('COUNT');
    expect(state.func).toBe('count');
  });

  it('should create initial state for all aggregate functions', () => {
    for (const func of ['count', 'sum', 'avg', 'min', 'max'] as const) {
      const state = createAggregateState(func);
      expect(state.func).toBe(func);
      expect(state.count).toBe(0);
    }
  });
});

describe('updateAggregateState', () => {
  describe('COUNT', () => {
    it('should increment count for every value including null', () => {
      const state = createAggregateState('count');
      updateAggregateState(state, 1);
      updateAggregateState(state, null);
      updateAggregateState(state, 'hello');
      expect(state.count).toBe(3);
    });
  });

  describe('SUM', () => {
    it('should sum numeric values', () => {
      const state = createAggregateState('sum');
      updateAggregateState(state, 10);
      updateAggregateState(state, 20);
      updateAggregateState(state, 30);
      expect(state.sum).toBe(60);
      expect(state.count).toBe(3);
    });

    it('should skip null values', () => {
      const state = createAggregateState('sum');
      updateAggregateState(state, 10);
      updateAggregateState(state, null);
      updateAggregateState(state, 20);
      expect(state.sum).toBe(30);
      expect(state.count).toBe(2);
    });

    it('should skip undefined values', () => {
      const state = createAggregateState('sum');
      updateAggregateState(state, 10);
      updateAggregateState(state, undefined);
      expect(state.sum).toBe(10);
      expect(state.count).toBe(1);
    });

    it('should handle bigint values', () => {
      const state = createAggregateState('sum');
      updateAggregateState(state, 10n);
      updateAggregateState(state, 20n);
      expect(state.sum).toBe(30n);
    });
  });

  describe('AVG', () => {
    it('should track sum and count for average calculation', () => {
      const state = createAggregateState('avg');
      updateAggregateState(state, 10);
      updateAggregateState(state, 20);
      updateAggregateState(state, 30);
      expect(state.sum).toBe(60);
      expect(state.count).toBe(3);
    });
  });

  describe('MIN', () => {
    it('should track minimum value', () => {
      const state = createAggregateState('min');
      updateAggregateState(state, 30);
      updateAggregateState(state, 10);
      updateAggregateState(state, 20);
      expect(state.min).toBe(10);
    });

    it('should handle string values', () => {
      const state = createAggregateState('min');
      updateAggregateState(state, 'charlie');
      updateAggregateState(state, 'alice');
      updateAggregateState(state, 'bob');
      expect(state.min).toBe('alice');
    });

    it('should skip null values', () => {
      const state = createAggregateState('min');
      updateAggregateState(state, null);
      updateAggregateState(state, 5);
      updateAggregateState(state, null);
      expect(state.min).toBe(5);
    });
  });

  describe('MAX', () => {
    it('should track maximum value', () => {
      const state = createAggregateState('max');
      updateAggregateState(state, 10);
      updateAggregateState(state, 30);
      updateAggregateState(state, 20);
      expect(state.max).toBe(30);
    });

    it('should handle string values', () => {
      const state = createAggregateState('max');
      updateAggregateState(state, 'alice');
      updateAggregateState(state, 'charlie');
      updateAggregateState(state, 'bob');
      expect(state.max).toBe('charlie');
    });
  });
});

describe('getAggregateResult', () => {
  it('should return count', () => {
    const state = createAggregateState('count');
    updateAggregateState(state, 1);
    updateAggregateState(state, 2);
    expect(getAggregateResult(state)).toBe(2);
  });

  it('should return sum', () => {
    const state = createAggregateState('sum');
    updateAggregateState(state, 10);
    updateAggregateState(state, 20);
    expect(getAggregateResult(state)).toBe(30);
  });

  it('should return null for sum with no values', () => {
    const state = createAggregateState('sum');
    expect(getAggregateResult(state)).toBeNull();
  });

  it('should return average', () => {
    const state = createAggregateState('avg');
    updateAggregateState(state, 10);
    updateAggregateState(state, 20);
    updateAggregateState(state, 30);
    expect(getAggregateResult(state)).toBe(20);
  });

  it('should return null for avg with no values', () => {
    const state = createAggregateState('avg');
    expect(getAggregateResult(state)).toBeNull();
  });

  it('should return avg with bigint sum', () => {
    const state = createAggregateState('avg');
    updateAggregateState(state, 10n);
    updateAggregateState(state, 20n);
    expect(getAggregateResult(state)).toBe(15);
  });

  it('should return min', () => {
    const state = createAggregateState('min');
    updateAggregateState(state, 5);
    updateAggregateState(state, 3);
    expect(getAggregateResult(state)).toBe(3);
  });

  it('should return max', () => {
    const state = createAggregateState('max');
    updateAggregateState(state, 5);
    updateAggregateState(state, 8);
    expect(getAggregateResult(state)).toBe(8);
  });

  it('should return null for unknown function', () => {
    const state = { func: 'unknown' as never, count: 0, sum: null, min: null, max: null };
    expect(getAggregateResult(state)).toBeNull();
  });
});

// =============================================================================
// MERGE OPERATIONS
// =============================================================================

describe('mergeAggregateStates', () => {
  it('should merge two count states', () => {
    const s1 = createAggregateState('count');
    updateAggregateState(s1, 1);
    updateAggregateState(s1, 2);

    const s2 = createAggregateState('count');
    updateAggregateState(s2, 3);

    const merged = mergeAggregateStates(s1, s2);
    expect(merged.count).toBe(3);
  });

  it('should merge two sum states with numbers', () => {
    const s1 = createAggregateState('sum');
    updateAggregateState(s1, 10);
    updateAggregateState(s1, 20);

    const s2 = createAggregateState('sum');
    updateAggregateState(s2, 30);

    const merged = mergeAggregateStates(s1, s2);
    expect(merged.sum).toBe(60);
    expect(merged.count).toBe(3);
  });

  it('should merge sum states when one is null', () => {
    const s1 = createAggregateState('sum');
    updateAggregateState(s1, 10);

    const s2 = createAggregateState('sum'); // no values, sum is null

    const merged = mergeAggregateStates(s1, s2);
    expect(merged.sum).toBe(10);
  });

  it('should merge min states', () => {
    const s1 = createAggregateState('min');
    updateAggregateState(s1, 5);

    const s2 = createAggregateState('min');
    updateAggregateState(s2, 3);

    const merged = mergeAggregateStates(s1, s2);
    expect(merged.min).toBe(3);
  });

  it('should merge max states', () => {
    const s1 = createAggregateState('max');
    updateAggregateState(s1, 5);

    const s2 = createAggregateState('max');
    updateAggregateState(s2, 8);

    const merged = mergeAggregateStates(s1, s2);
    expect(merged.max).toBe(8);
  });

  it('should merge min states when one is null', () => {
    const s1 = createAggregateState('min');
    updateAggregateState(s1, 5);

    const s2 = createAggregateState('min'); // no values, min is null

    const merged = mergeAggregateStates(s1, s2);
    expect(merged.min).toBe(5);
  });

  it('should throw when merging different functions', () => {
    const s1 = createAggregateState('count');
    const s2 = createAggregateState('sum');
    expect(() => mergeAggregateStates(s1, s2)).toThrow('Cannot merge different aggregate functions');
  });

  it('should merge bigint sums', () => {
    const s1 = createAggregateState('sum');
    updateAggregateState(s1, 10n);

    const s2 = createAggregateState('sum');
    updateAggregateState(s2, 20n);

    const merged = mergeAggregateStates(s1, s2);
    expect(merged.sum).toBe(30n);
  });
});

// =============================================================================
// COMBINE FUNCTIONS (Distributed aggregation)
// =============================================================================

describe('combineCount', () => {
  it('should sum partial counts', () => {
    expect(combineCount([10, 20, 30])).toBe(60);
  });

  it('should return 0 for empty array', () => {
    expect(combineCount([])).toBe(0);
  });
});

describe('combineSum', () => {
  it('should sum partial sums', () => {
    expect(combineSum([10, 20, 30])).toBe(60);
  });

  it('should handle null values', () => {
    expect(combineSum([10, null, 30])).toBe(40);
  });

  it('should return null for all nulls', () => {
    expect(combineSum([null, null])).toBeNull();
  });

  it('should handle bigint values', () => {
    expect(combineSum([10n, 20n])).toBe(30n);
  });

  it('should return null for empty array', () => {
    expect(combineSum([])).toBeNull();
  });
});

describe('combineAvg', () => {
  it('should compute weighted average from partial sums and counts', () => {
    // Partition 1: sum=30, count=3 (avg=10)
    // Partition 2: sum=60, count=2 (avg=30)
    // Global: sum=90, count=5, avg=18
    expect(combineAvg([30, 60], [3, 2])).toBe(18);
  });

  it('should handle null sums', () => {
    expect(combineAvg([30, null], [3, 0])).toBe(10);
  });

  it('should return null when all counts are zero', () => {
    expect(combineAvg([null, null], [0, 0])).toBeNull();
  });
});

describe('combineMin', () => {
  it('should find global minimum', () => {
    expect(combineMin([5, 3, 8])).toBe(3);
  });

  it('should handle null values', () => {
    expect(combineMin([5, null, 3])).toBe(3);
  });

  it('should return null for all nulls', () => {
    expect(combineMin([null, null])).toBeNull();
  });

  it('should return null for empty array', () => {
    expect(combineMin([])).toBeNull();
  });

  it('should handle string values', () => {
    expect(combineMin(['charlie', 'alice', 'bob'])).toBe('alice');
  });
});

describe('combineMax', () => {
  it('should find global maximum', () => {
    expect(combineMax([5, 3, 8])).toBe(8);
  });

  it('should handle null values', () => {
    expect(combineMax([5, null, 8])).toBe(8);
  });

  it('should return null for all nulls', () => {
    expect(combineMax([null, null])).toBeNull();
  });

  it('should handle string values', () => {
    expect(combineMax(['alice', 'charlie', 'bob'])).toBe('charlie');
  });
});

// =============================================================================
// COMPARISON UTILITIES
// =============================================================================

describe('compareSqlValues', () => {
  it('should compare numbers', () => {
    expect(compareSqlValues(1, 2)).toBeLessThan(0);
    expect(compareSqlValues(2, 1)).toBeGreaterThan(0);
    expect(compareSqlValues(5, 5)).toBe(0);
  });

  it('should compare strings', () => {
    expect(compareSqlValues('alice', 'bob')).toBeLessThan(0);
    expect(compareSqlValues('bob', 'alice')).toBeGreaterThan(0);
    expect(compareSqlValues('abc', 'abc')).toBe(0);
  });

  it('should compare bigints', () => {
    expect(compareSqlValues(1n, 2n)).toBeLessThan(0);
    expect(compareSqlValues(2n, 1n)).toBeGreaterThan(0);
    expect(compareSqlValues(5n, 5n)).toBe(0);
  });

  it('should compare booleans', () => {
    expect(compareSqlValues(false, true)).toBeLessThan(0);
    expect(compareSqlValues(true, false)).toBeGreaterThan(0);
    expect(compareSqlValues(true, true)).toBe(0);
  });

  it('should sort nulls to end', () => {
    expect(compareSqlValues(null, 1)).toBeGreaterThan(0);
    expect(compareSqlValues(1, null)).toBeLessThan(0);
    expect(compareSqlValues(null, null)).toBe(0);
    expect(compareSqlValues(undefined, undefined)).toBe(0);
  });

  it('should handle mixed types via string comparison', () => {
    // Mixed types convert to string
    const result = compareSqlValues(1, 'abc');
    expect(typeof result).toBe('number');
  });
});

describe('isSqlTruthy', () => {
  it('should return false for null and undefined', () => {
    expect(isSqlTruthy(null)).toBe(false);
    expect(isSqlTruthy(undefined)).toBe(false);
  });

  it('should handle booleans', () => {
    expect(isSqlTruthy(true)).toBe(true);
    expect(isSqlTruthy(false)).toBe(false);
  });

  it('should handle numbers', () => {
    expect(isSqlTruthy(0)).toBe(false);
    expect(isSqlTruthy(1)).toBe(true);
    expect(isSqlTruthy(-1)).toBe(true);
  });

  it('should handle bigints', () => {
    expect(isSqlTruthy(0n)).toBe(false);
    expect(isSqlTruthy(1n)).toBe(true);
  });

  it('should handle strings', () => {
    expect(isSqlTruthy('')).toBe(false);
    expect(isSqlTruthy('hello')).toBe(true);
  });
});
