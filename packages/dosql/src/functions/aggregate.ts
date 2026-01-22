/**
 * SQLite Aggregate Functions
 *
 * Implements SQLite-compatible aggregate functions:
 * - count(*) - Count of all rows
 * - count(x) - Count of non-NULL values
 * - sum(x) - Sum of all non-NULL values
 * - avg(x) - Average of all non-NULL values
 * - min(x) - Minimum value
 * - max(x) - Maximum value
 * - total(x) - Sum that returns 0.0 instead of NULL for empty set
 * - group_concat(x, separator) - Concatenate values with separator
 *
 * Each aggregate function has two parts:
 * 1. An accumulator class that maintains state during aggregation
 * 2. A factory function that creates new accumulators
 */

import type { SqlValue } from '../engine/types.js';
import type { FunctionSignature } from './registry.js';

// =============================================================================
// AGGREGATE ACCUMULATOR INTERFACE
// =============================================================================

/**
 * Interface for aggregate function accumulators
 */
export interface AggregateAccumulator {
  /** Process a single value */
  step(value: SqlValue): void;
  /** Get the final result */
  finalize(): SqlValue;
  /** Reset the accumulator for reuse */
  reset(): void;
}

/**
 * Factory function type for creating accumulators
 */
export type AggregateFactory = () => AggregateAccumulator;

// =============================================================================
// COUNT AGGREGATE
// =============================================================================

/**
 * COUNT(*) accumulator - counts all rows
 */
class CountStarAccumulator implements AggregateAccumulator {
  private count = 0;

  step(_value: SqlValue): void {
    this.count++;
  }

  finalize(): SqlValue {
    return this.count;
  }

  reset(): void {
    this.count = 0;
  }
}

/**
 * COUNT(x) accumulator - counts non-NULL values
 */
class CountAccumulator implements AggregateAccumulator {
  private count = 0;

  step(value: SqlValue): void {
    if (value !== null) {
      this.count++;
    }
  }

  finalize(): SqlValue {
    return this.count;
  }

  reset(): void {
    this.count = 0;
  }
}

/**
 * COUNT(DISTINCT x) accumulator - counts unique non-NULL values
 */
class CountDistinctAccumulator implements AggregateAccumulator {
  private values = new Set<string>();

  step(value: SqlValue): void {
    if (value !== null) {
      // Convert to string for Set comparison
      this.values.add(JSON.stringify(value));
    }
  }

  finalize(): SqlValue {
    return this.values.size;
  }

  reset(): void {
    this.values.clear();
  }
}

// =============================================================================
// SUM AGGREGATE
// =============================================================================

/**
 * SUM(x) accumulator - sums non-NULL values
 * Returns NULL if all values are NULL
 */
class SumAccumulator implements AggregateAccumulator {
  private sum: number | bigint = 0;
  private hasValue = false;
  private useBigInt = false;

  step(value: SqlValue): void {
    if (value === null) return;

    if (typeof value === 'bigint') {
      this.useBigInt = true;
      if (!this.hasValue) {
        this.sum = 0n;
      }
      this.sum = (this.sum as bigint) + value;
      this.hasValue = true;
    } else {
      const num = Number(value);
      if (!isNaN(num)) {
        if (this.useBigInt) {
          this.sum = (this.sum as bigint) + BigInt(Math.floor(num));
        } else {
          this.sum = (this.sum as number) + num;
        }
        this.hasValue = true;
      }
    }
  }

  finalize(): SqlValue {
    if (!this.hasValue) return null;
    return this.sum;
  }

  reset(): void {
    this.sum = 0;
    this.hasValue = false;
    this.useBigInt = false;
  }
}

// =============================================================================
// TOTAL AGGREGATE
// =============================================================================

/**
 * TOTAL(x) accumulator - like SUM but returns 0.0 instead of NULL
 */
class TotalAccumulator implements AggregateAccumulator {
  private sum = 0;

  step(value: SqlValue): void {
    if (value === null) return;

    const num = Number(value);
    if (!isNaN(num)) {
      this.sum += num;
    }
  }

  finalize(): SqlValue {
    return this.sum;
  }

  reset(): void {
    this.sum = 0;
  }
}

// =============================================================================
// AVG AGGREGATE
// =============================================================================

/**
 * AVG(x) accumulator - average of non-NULL values
 * Returns NULL if all values are NULL
 */
class AvgAccumulator implements AggregateAccumulator {
  private sum = 0;
  private count = 0;

  step(value: SqlValue): void {
    if (value === null) return;

    const num = Number(value);
    if (!isNaN(num)) {
      this.sum += num;
      this.count++;
    }
  }

  finalize(): SqlValue {
    if (this.count === 0) return null;
    return this.sum / this.count;
  }

  reset(): void {
    this.sum = 0;
    this.count = 0;
  }
}

// =============================================================================
// MIN AGGREGATE
// =============================================================================

/**
 * MIN(x) accumulator - minimum non-NULL value
 */
class MinAccumulator implements AggregateAccumulator {
  private min: SqlValue = null;

  step(value: SqlValue): void {
    if (value === null) return;

    if (this.min === null || compare(value, this.min) < 0) {
      this.min = value;
    }
  }

  finalize(): SqlValue {
    return this.min;
  }

  reset(): void {
    this.min = null;
  }
}

// =============================================================================
// MAX AGGREGATE
// =============================================================================

/**
 * MAX(x) accumulator - maximum non-NULL value
 */
class MaxAccumulator implements AggregateAccumulator {
  private max: SqlValue = null;

  step(value: SqlValue): void {
    if (value === null) return;

    if (this.max === null || compare(value, this.max) > 0) {
      this.max = value;
    }
  }

  finalize(): SqlValue {
    return this.max;
  }

  reset(): void {
    this.max = null;
  }
}

// =============================================================================
// GROUP_CONCAT AGGREGATE
// =============================================================================

/**
 * GROUP_CONCAT(x, separator) accumulator
 * Concatenates non-NULL values with separator (default: ",")
 */
class GroupConcatAccumulator implements AggregateAccumulator {
  private values: string[] = [];
  private separator: string;

  constructor(separator: string = ',') {
    this.separator = separator;
  }

  step(value: SqlValue): void {
    if (value === null) return;
    this.values.push(String(value));
  }

  finalize(): SqlValue {
    if (this.values.length === 0) return null;
    return this.values.join(this.separator);
  }

  reset(): void {
    this.values = [];
  }
}

/**
 * GROUP_CONCAT(DISTINCT x, separator) accumulator
 */
class GroupConcatDistinctAccumulator implements AggregateAccumulator {
  private values = new Set<string>();
  private separator: string;

  constructor(separator: string = ',') {
    this.separator = separator;
  }

  step(value: SqlValue): void {
    if (value === null) return;
    this.values.add(String(value));
  }

  finalize(): SqlValue {
    if (this.values.size === 0) return null;
    return Array.from(this.values).join(this.separator);
  }

  reset(): void {
    this.values.clear();
  }
}

// =============================================================================
// JSON GROUP AGGREGATES
// =============================================================================

/**
 * JSON_GROUP_ARRAY(x) accumulator
 * Aggregates values into a JSON array
 */
class JsonGroupArrayAccumulator implements AggregateAccumulator {
  private values: unknown[] = [];

  step(value: SqlValue): void {
    this.values.push(value);
  }

  finalize(): SqlValue {
    return JSON.stringify(this.values);
  }

  reset(): void {
    this.values = [];
  }
}

/**
 * JSON_GROUP_OBJECT(key, value) accumulator
 * Aggregates key-value pairs into a JSON object
 */
class JsonGroupObjectAccumulator implements AggregateAccumulator {
  private obj: Record<string, unknown> = {};

  step(key: SqlValue, value?: SqlValue): void {
    // If called with two args (key, value)
    if (key !== null && key !== undefined) {
      const keyStr = String(key);
      this.obj[keyStr] = value;
    }
  }

  finalize(): SqlValue {
    return JSON.stringify(this.obj);
  }

  reset(): void {
    this.obj = {};
  }
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Compare two SQL values
 * SQLite type affinity order: NULL < INTEGER/REAL < TEXT < BLOB
 */
function compare(a: SqlValue, b: SqlValue): number {
  // NULL is less than everything
  if (a === null && b === null) return 0;
  if (a === null) return -1;
  if (b === null) return 1;

  // Get type category
  const typeA = getTypeCategory(a);
  const typeB = getTypeCategory(b);

  if (typeA !== typeB) {
    return typeA - typeB;
  }

  // Same type category
  switch (typeA) {
    case 1: // Number
      return Number(a) - Number(b);
    case 2: // String
      return String(a).localeCompare(String(b));
    case 3: // Blob
      return compareBlobs(a as Uint8Array, b as Uint8Array);
    default:
      return 0;
  }
}

function getTypeCategory(value: SqlValue): number {
  if (value === null) return 0;
  if (typeof value === 'number' || typeof value === 'bigint') return 1;
  if (typeof value === 'boolean') return 1; // Treated as number
  if (value instanceof Date) return 1; // Treated as number (timestamp)
  if (value instanceof Uint8Array) return 3;
  return 2; // String
}

function compareBlobs(a: Uint8Array, b: Uint8Array): number {
  const minLen = Math.min(a.length, b.length);
  for (let i = 0; i < minLen; i++) {
    if (a[i] !== b[i]) return a[i] - b[i];
  }
  return a.length - b.length;
}

// =============================================================================
// AGGREGATE FACTORIES
// =============================================================================

export const aggregateFactories: Record<string, AggregateFactory> = {
  count_star: () => new CountStarAccumulator(),
  count: () => new CountAccumulator(),
  count_distinct: () => new CountDistinctAccumulator(),
  sum: () => new SumAccumulator(),
  total: () => new TotalAccumulator(),
  avg: () => new AvgAccumulator(),
  min: () => new MinAccumulator(),
  max: () => new MaxAccumulator(),
  group_concat: () => new GroupConcatAccumulator(),
  group_concat_distinct: () => new GroupConcatDistinctAccumulator(),
  json_group_array: () => new JsonGroupArrayAccumulator(),
  json_group_object: () => new JsonGroupObjectAccumulator(),
};

/**
 * Create a group_concat accumulator with custom separator
 */
export function createGroupConcat(separator: string = ','): AggregateAccumulator {
  return new GroupConcatAccumulator(separator);
}

/**
 * Create a group_concat distinct accumulator with custom separator
 */
export function createGroupConcatDistinct(separator: string = ','): AggregateAccumulator {
  return new GroupConcatDistinctAccumulator(separator);
}

/**
 * Create a json_group_array accumulator
 */
export function createJsonGroupArrayAccumulator(): AggregateAccumulator {
  return new JsonGroupArrayAccumulator();
}

/**
 * Create a json_group_object accumulator
 */
export function createJsonGroupObjectAccumulator(): AggregateAccumulator {
  return new JsonGroupObjectAccumulator();
}

// =============================================================================
// AGGREGATE SIGNATURES FOR REGISTRY
// =============================================================================

export const aggregateSignatures: Record<string, FunctionSignature> = {
  count: {
    name: 'count',
    params: [{ name: 'x', type: 'any', optional: true }],
    returnType: 'number',
    description: 'Count of rows or non-NULL values',
    isAggregate: true,
  },
  sum: {
    name: 'sum',
    params: [{ name: 'x', type: 'number' }],
    returnType: 'number',
    description: 'Sum of all non-NULL values',
    isAggregate: true,
  },
  total: {
    name: 'total',
    params: [{ name: 'x', type: 'number' }],
    returnType: 'number',
    description: 'Sum of all values (returns 0.0 for empty set)',
    isAggregate: true,
  },
  avg: {
    name: 'avg',
    params: [{ name: 'x', type: 'number' }],
    returnType: 'number',
    description: 'Average of all non-NULL values',
    isAggregate: true,
  },
  min: {
    name: 'min',
    params: [{ name: 'x', type: 'any' }],
    returnType: 'any',
    description: 'Minimum value',
    isAggregate: true,
  },
  max: {
    name: 'max',
    params: [{ name: 'x', type: 'any' }],
    returnType: 'any',
    description: 'Maximum value',
    isAggregate: true,
  },
  group_concat: {
    name: 'group_concat',
    params: [
      { name: 'x', type: 'any' },
      { name: 'separator', type: 'string', optional: true },
    ],
    returnType: 'string',
    description: 'Concatenate values with separator',
    isAggregate: true,
  },
  json_group_array: {
    name: 'json_group_array',
    params: [{ name: 'x', type: 'any' }],
    returnType: 'string',
    description: 'Aggregate values into a JSON array',
    isAggregate: true,
  },
  json_group_object: {
    name: 'json_group_object',
    params: [
      { name: 'key', type: 'string' },
      { name: 'value', type: 'any' },
    ],
    returnType: 'string',
    description: 'Aggregate key-value pairs into a JSON object',
    isAggregate: true,
  },
};

// =============================================================================
// UTILITY FOR EXECUTING AGGREGATES
// =============================================================================

/**
 * Execute an aggregate function over an array of values
 */
export function executeAggregate(
  name: string,
  values: SqlValue[],
  options?: { distinct?: boolean; separator?: string }
): SqlValue {
  const distinct = options?.distinct ?? false;
  const separator = options?.separator ?? ',';

  let factory: AggregateFactory;

  const lowerName = name.toLowerCase();

  switch (lowerName) {
    case 'count':
      factory = distinct ? aggregateFactories.count_distinct : aggregateFactories.count;
      break;
    case 'sum':
      factory = aggregateFactories.sum;
      break;
    case 'total':
      factory = aggregateFactories.total;
      break;
    case 'avg':
      factory = aggregateFactories.avg;
      break;
    case 'min':
      factory = aggregateFactories.min;
      break;
    case 'max':
      factory = aggregateFactories.max;
      break;
    case 'group_concat':
      return (() => {
        const acc = distinct
          ? new GroupConcatDistinctAccumulator(separator)
          : new GroupConcatAccumulator(separator);
        for (const v of values) {
          acc.step(v);
        }
        return acc.finalize();
      })();
    default:
      throw new Error(`Unknown aggregate function: ${name}`);
  }

  const acc = factory();
  for (const value of values) {
    acc.step(value);
  }
  return acc.finalize();
}

/**
 * Check if a function name is an aggregate function
 */
export function isAggregateFunction(name: string): boolean {
  const lowerName = name.toLowerCase();
  return lowerName in aggregateSignatures || lowerName === 'count_star';
}
