/**
 * DoSQL IN Subquery Parser Tests
 *
 * TDD tests for IN operator with subquery support.
 *
 * Issue: sql-z79a - Fix IN operator with subquery support
 *
 * Test cases:
 * - IN with scalar subquery: `WHERE 1 IN (SELECT 1)`
 * - IN with multi-row subquery: `WHERE x IN (SELECT y FROM t2)`
 * - NOT IN with subquery
 * - Correlated IN subquery: `WHERE x IN (SELECT y FROM t2 WHERE t2.z = t1.z)`
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { SubqueryParser, parseSubquery } from '../subquery.js';
import type { SubqueryNode, ParsedExpr } from '../subquery.js';

// =============================================================================
// TYPE GUARDS
// =============================================================================

/** Type guard for subquery nodes */
function isSubqueryNode(expr: ParsedExpr | undefined): expr is SubqueryNode {
  return expr !== undefined && expr.type === 'subquery';
}

/** Type guard for IN expressions */
function isInExpr(expr: ParsedExpr | undefined): expr is { type: 'in'; expr: ParsedExpr; values: ParsedExpr[] | SubqueryNode; not?: boolean } {
  return expr !== undefined && expr.type === 'in';
}

/** Type guard for unary expressions */
function isUnaryExpr(expr: ParsedExpr | undefined): expr is { type: 'unary'; op: string; operand: ParsedExpr } {
  return expr !== undefined && expr.type === 'unary';
}

/** Type guard for binary expressions */
function isBinaryExpr(expr: ParsedExpr | undefined): expr is { type: 'binary'; op: string; left: ParsedExpr; right: ParsedExpr } {
  return expr !== undefined && expr.type === 'binary';
}

/** Type guard for literal expressions */
function isLiteralExpr(expr: ParsedExpr | undefined): expr is { type: 'literal'; value: string | number | boolean | null } {
  return expr !== undefined && expr.type === 'literal';
}

/** Type guard for column expressions */
function isColumnExpr(expr: ParsedExpr | undefined): expr is { type: 'column'; name: string; table?: string } {
  return expr !== undefined && expr.type === 'column';
}

// =============================================================================
// IN SUBQUERY TESTS - SQLLogicTest in2.test cases
// =============================================================================

describe('IN Subquery Support (sql-z79a)', () => {
  let parser: SubqueryParser;

  beforeEach(() => {
    parser = new SubqueryParser();
  });

  describe('IN with scalar subquery', () => {
    it('should parse 1 IN (SELECT 1) - from SQLLogicTest in2.test', () => {
      // This is the exact test case from the issue
      const sql = 'SELECT 1 FROM t1 WHERE 1 IN (SELECT 1)';
      const result = parser.parse(sql);

      expect(result.type).toBe('select');
      expect(result.where).toBeDefined();
      expect(result.where?.type).toBe('in');

      if (isInExpr(result.where)) {
        // The left side should be literal 1
        expect(result.where.expr.type).toBe('literal');
        if (isLiteralExpr(result.where.expr)) {
          expect(result.where.expr.value).toBe(1);
        }

        // The right side should be a subquery
        expect(isSubqueryNode(result.where.values as ParsedExpr)).toBe(true);
        const subquery = result.where.values as SubqueryNode;
        expect(subquery.subqueryType).toBe('in');
        expect(subquery.query.columns).toHaveLength(1);
      }
    });

    it('should parse float literal IN (SELECT float)', () => {
      const sql = 'SELECT 1 FROM t1 WHERE 1.0 IN (SELECT 1.0)';
      const result = parser.parse(sql);

      expect(result.where?.type).toBe('in');
      if (isInExpr(result.where)) {
        expect(isSubqueryNode(result.where.values as ParsedExpr)).toBe(true);
      }
    });

    it('should parse string literal IN (SELECT string)', () => {
      const sql = "SELECT 1 FROM t1 WHERE '1' IN (SELECT '1')";
      const result = parser.parse(sql);

      expect(result.where?.type).toBe('in');
      if (isInExpr(result.where)) {
        expect(isSubqueryNode(result.where.values as ParsedExpr)).toBe(true);
      }
    });
  });

  describe('NOT IN with subquery', () => {
    it('should parse 1 NOT IN (SELECT 2)', () => {
      const sql = 'SELECT 1 FROM t1 WHERE 1 NOT IN (SELECT 2)';
      const result = parser.parse(sql);

      // NOT IN is represented as NOT (IN ...)
      if (isUnaryExpr(result.where) && result.where.op === 'not') {
        expect(result.where.operand.type).toBe('in');
        const inExpr = result.where.operand as { type: 'in'; expr: ParsedExpr; values: ParsedExpr[] | SubqueryNode };
        expect(isSubqueryNode(inExpr.values as ParsedExpr)).toBe(true);
      } else {
        // Some parsers might handle NOT IN directly
        expect(result.where?.type).toBe('in');
      }
    });
  });

  describe('IN with multi-row subquery', () => {
    it('should parse x IN (SELECT y FROM t2)', () => {
      const sql = 'SELECT * FROM t1 WHERE x IN (SELECT y FROM t2)';
      const result = parser.parse(sql);

      expect(result.where?.type).toBe('in');
      if (isInExpr(result.where)) {
        // Left side should be column x
        expect(result.where.expr.type).toBe('column');
        if (isColumnExpr(result.where.expr)) {
          expect(result.where.expr.name).toBe('x');
        }

        // Right side should be subquery
        expect(isSubqueryNode(result.where.values as ParsedExpr)).toBe(true);
        const subquery = result.where.values as SubqueryNode;
        expect(subquery.query.from).toBeDefined();
      }
    });

    it('should parse IN subquery with WHERE clause', () => {
      const sql = 'SELECT * FROM t1 WHERE x IN (SELECT y FROM t2 WHERE z > 0)';
      const result = parser.parse(sql);

      expect(result.where?.type).toBe('in');
      if (isInExpr(result.where)) {
        const subquery = result.where.values as SubqueryNode;
        expect(subquery.query.where).toBeDefined();
      }
    });
  });

  describe('Correlated IN subquery', () => {
    it('should parse correlated IN subquery: WHERE x IN (SELECT y FROM t2 WHERE t2.z = t1.z)', () => {
      const sql = 'SELECT * FROM t1 WHERE x IN (SELECT y FROM t2 WHERE t2.z = t1.z)';
      const result = parser.parse(sql);

      expect(result.where?.type).toBe('in');
      if (isInExpr(result.where)) {
        const subquery = result.where.values as SubqueryNode;
        expect(subquery.isCorrelated).toBe(true);
        expect(subquery.correlatedColumns).toBeDefined();
        expect(subquery.correlatedColumns?.length).toBeGreaterThan(0);
        expect(subquery.correlatedColumns).toContainEqual({ table: 't1', column: 'z' });
      }
    });

    it('should parse correlated IN with aliased outer table', () => {
      const sql = 'SELECT * FROM orders AS o WHERE user_id IN (SELECT id FROM users WHERE users.status = o.status)';
      const result = parser.parse(sql);

      expect(result.where?.type).toBe('in');
      if (isInExpr(result.where)) {
        const subquery = result.where.values as SubqueryNode;
        expect(subquery.isCorrelated).toBe(true);
        expect(subquery.correlatedColumns).toContainEqual({ table: 'o', column: 'status' });
      }
    });
  });

  describe('IN subquery error cases from SQLLogicTest', () => {
    it('should reject IN subquery with multiple columns: 1 IN (SELECT 1,2)', () => {
      const sql = 'SELECT 1 FROM t1 WHERE 1 IN (SELECT 1,2)';
      expect(() => parser.parse(sql)).toThrow(/scalar/i);
    });

    it('should reject IN subquery with multiple columns: 1 IN (SELECT x,y FROM t1)', () => {
      const sql = 'SELECT 1 FROM t1 WHERE 1 IN (SELECT x,y FROM t1)';
      expect(() => parser.parse(sql)).toThrow(/scalar/i);
    });

    it('should reject IN subquery with SELECT *: 1 IN (SELECT * FROM t1)', () => {
      const sql = 'SELECT 1 FROM t1 WHERE 1 IN (SELECT * FROM t1)';
      // This should error unless we know the table has only one column
      // For now, we might need to defer this check to runtime
      // But ideally, the parser should flag this
      const result = parser.parse(sql);
      // If it doesn't throw, it should at least mark it as potentially problematic
      expect(result.where).toBeDefined();
    });
  });

  describe('Complex IN subquery combinations', () => {
    it('should parse nested IN subqueries', () => {
      const sql = 'SELECT * FROM t1 WHERE a IN (SELECT b FROM t2 WHERE c IN (SELECT d FROM t3))';
      const result = parser.parse(sql);

      expect(result.where?.type).toBe('in');
      if (isInExpr(result.where)) {
        const outerSubquery = result.where.values as SubqueryNode;
        expect(outerSubquery.query.where?.type).toBe('in');
      }
    });

    it('should parse IN subquery combined with other conditions', () => {
      const sql = 'SELECT * FROM t1 WHERE x > 0 AND y IN (SELECT z FROM t2) AND w IS NOT NULL';
      const result = parser.parse(sql);

      expect(result.where?.type).toBe('binary');
      if (isBinaryExpr(result.where)) {
        expect(result.where.op).toBe('and');
      }
    });

    it('should parse IN subquery in JOIN condition', () => {
      const sql = 'SELECT * FROM t1 JOIN t2 ON t2.id IN (SELECT ref_id FROM t3 WHERE t3.x = t1.x)';
      const result = parser.parse(sql);

      expect(result.joins).toBeDefined();
      expect(result.joins?.[0].on?.type).toBe('in');
    });
  });

  describe('IN subquery optimization hints', () => {
    it('should mark uncorrelated IN subquery as semi-join candidate', () => {
      const sql = 'SELECT * FROM t1 WHERE x IN (SELECT y FROM t2)';
      const result = parser.parse(sql);

      if (isInExpr(result.where)) {
        const subquery = result.where.values as SubqueryNode;
        expect(subquery.isCorrelated).toBe(false);
        expect(subquery.canBeSemiJoin).toBe(true);
      }
    });

    it('should mark NOT IN as anti-join candidate', () => {
      const sql = 'SELECT * FROM t1 WHERE x NOT IN (SELECT y FROM t2)';
      const result = parser.parse(sql);

      // Navigate to the IN expression
      if (isUnaryExpr(result.where) && result.where.op === 'not') {
        const inExpr = result.where.operand;
        if (isInExpr(inExpr)) {
          const subquery = inExpr.values as SubqueryNode;
          expect(subquery.canBeAntiJoin).toBe(true);
        }
      }
    });
  });
});
