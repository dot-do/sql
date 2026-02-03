/**
 * Tests for WHERE evaluator recursion depth limits
 */

import { describe, it, expect } from 'vitest';
import {
  evaluateWhereCondition,
  MAX_WHERE_DEPTH,
  type WhereEvaluatorDeps,
} from '../where-evaluator.js';
import type { SqlValue } from '../types.js';

const mockDeps: WhereEvaluatorDeps = {
  parseValueList: (valueList: string) => {
    const values = valueList
      .split(',')
      .map((v) => v.trim())
      .filter(Boolean)
      .map((v) => {
        if (v.startsWith("'") && v.endsWith("'")) return v.slice(1, -1);
        if (v.toUpperCase() === 'NULL') return null;
        return Number(v);
      });
    return { values, paramIndex: 0 };
  },
  valuesEqual: (a: SqlValue, b: SqlValue) => a === b,
  getColumnValue: (row: Record<string, SqlValue>, colRef: string) => {
    return row[colRef] ?? null;
  },
};

describe('WHERE evaluator depth limits', () => {
  it('should evaluate moderately nested conditions', () => {
    // 5 levels of nesting - should work fine
    const condition = '(((((x = 1)))))';
    const row = { x: 1 };
    const pIdx = { value: 0 };

    const result = evaluateWhereCondition(condition, row, [], pIdx, mockDeps);
    expect(result).toBe(true);
  });

  it('should reject deeply nested parenthesized expressions', () => {
    // Build a condition with > MAX_WHERE_DEPTH levels of parentheses
    const depth = MAX_WHERE_DEPTH + 10;
    const open = '('.repeat(depth);
    const close = ')'.repeat(depth);
    const condition = `${open}x = 1${close}`;
    const row = { x: 1 };
    const pIdx = { value: 0 };

    expect(() => {
      evaluateWhereCondition(condition, row, [], pIdx, mockDeps);
    }).toThrow(`WHERE clause exceeds maximum nesting depth of ${MAX_WHERE_DEPTH}`);
  });

  it('should reject deeply nested NOT expressions', () => {
    // Build NOT NOT NOT ... (x = 1)
    const depth = MAX_WHERE_DEPTH + 10;
    let condition = 'x = 1';
    for (let i = 0; i < depth; i++) {
      condition = `NOT (${condition})`;
    }
    const row = { x: 1 };
    const pIdx = { value: 0 };

    expect(() => {
      evaluateWhereCondition(condition, row, [], pIdx, mockDeps);
    }).toThrow(`WHERE clause exceeds maximum nesting depth of ${MAX_WHERE_DEPTH}`);
  });

  it('should handle normal AND/OR chains without hitting depth limit', () => {
    // AND/OR chains split into parts, each evaluated at depth+1
    // A chain of 10 ANDs should be fine
    const parts = Array.from({ length: 10 }, (_, i) => `x = ${i}`);
    const condition = parts.join(' OR ');
    const row = { x: 5 };
    const pIdx = { value: 0 };

    const result = evaluateWhereCondition(condition, row, [], pIdx, mockDeps);
    expect(result).toBe(true);
  });
});
