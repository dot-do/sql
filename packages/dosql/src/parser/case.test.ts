/**
 * DoSQL CASE Expression Tests
 *
 * Comprehensive test suite for CASE expression support including:
 * - Simple CASE expressions (CASE x WHEN y THEN z END)
 * - Searched CASE expressions (CASE WHEN condition THEN value END)
 * - CASE in SELECT, WHERE, ORDER BY clauses
 * - Nested CASE expressions
 * - CASE with NULL handling
 * - CASE with subqueries
 * - CASE with aggregates
 * - COALESCE as CASE shorthand
 * - NULLIF as CASE shorthand
 * - IIF (condition, true_value, false_value)
 *
 * Following TDD approach - tests are written first, implementation follows.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  CaseExpressionParser,
  parseCaseExpression,
  evaluateCaseExpression,
  type CaseExpression,
  type SimpleCaseExpression,
  type SearchedCaseExpression,
} from './case.js';

describe('DoSQL CASE Expression Parser', () => {
  let parser: CaseExpressionParser;

  beforeEach(() => {
    parser = new CaseExpressionParser();
  });

  // ===========================================================================
  // SIMPLE CASE EXPRESSIONS
  // ===========================================================================

  describe('Simple CASE Expressions', () => {
    it('should parse simple CASE with single WHEN clause', () => {
      const sql = "SELECT CASE status WHEN 'active' THEN 1 END FROM users";
      const result = parser.parse(sql);

      expect(result.type).toBe('select');
      const caseExpr = result.columns[0].expr as SimpleCaseExpression;
      expect(caseExpr.type).toBe('case');
      expect(caseExpr.caseType).toBe('simple');
      expect(caseExpr.operand).toBeDefined();
      expect(caseExpr.whenClauses.length).toBe(1);
    });

    it('should parse simple CASE with multiple WHEN clauses', () => {
      const sql = `SELECT CASE status
        WHEN 'active' THEN 1
        WHEN 'inactive' THEN 2
        WHEN 'pending' THEN 3
        END FROM users`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SimpleCaseExpression;
      expect(caseExpr.whenClauses.length).toBe(3);
    });

    it('should parse simple CASE with ELSE clause', () => {
      const sql = `SELECT CASE status
        WHEN 'active' THEN 'Active'
        ELSE 'Unknown'
        END FROM users`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SimpleCaseExpression;
      expect(caseExpr.elseClause).toBeDefined();
    });

    it('should parse simple CASE with numeric operand', () => {
      const sql = `SELECT CASE priority
        WHEN 1 THEN 'high'
        WHEN 2 THEN 'medium'
        WHEN 3 THEN 'low'
        END FROM tasks`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SimpleCaseExpression;
      expect(caseExpr.operand.type).toBe('column');
      expect((caseExpr.operand as any).name).toBe('priority');
    });

    it('should parse simple CASE with expression as operand', () => {
      const sql = `SELECT CASE upper(status)
        WHEN 'ACTIVE' THEN 1
        WHEN 'INACTIVE' THEN 0
        END FROM users`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SimpleCaseExpression;
      expect(caseExpr.operand.type).toBe('function');
    });

    it('should parse simple CASE with table-qualified column', () => {
      const sql = `SELECT CASE u.status
        WHEN 'active' THEN 1
        ELSE 0
        END FROM users u`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SimpleCaseExpression;
      expect(caseExpr.operand.type).toBe('column');
      expect((caseExpr.operand as any).table).toBe('u');
    });

    it('should parse simple CASE with alias', () => {
      const sql = `SELECT CASE status
        WHEN 'active' THEN 1
        ELSE 0
        END AS is_active FROM users`;
      const result = parser.parse(sql);

      expect(result.columns[0].alias).toBe('is_active');
    });
  });

  // ===========================================================================
  // SEARCHED CASE EXPRESSIONS
  // ===========================================================================

  describe('Searched CASE Expressions', () => {
    it('should parse searched CASE with single condition', () => {
      const sql = "SELECT CASE WHEN x > 0 THEN 'positive' END FROM numbers";
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.type).toBe('case');
      expect(caseExpr.caseType).toBe('searched');
      expect(caseExpr.operand).toBeUndefined();
      expect(caseExpr.whenClauses.length).toBe(1);
    });

    it('should parse searched CASE with multiple conditions', () => {
      const sql = `SELECT CASE
        WHEN x > 0 THEN 'positive'
        WHEN x < 0 THEN 'negative'
        ELSE 'zero'
        END FROM numbers`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.whenClauses.length).toBe(2);
      expect(caseExpr.elseClause).toBeDefined();
    });

    it('should parse searched CASE with complex conditions', () => {
      const sql = `SELECT CASE
        WHEN age >= 18 AND age < 65 THEN 'adult'
        WHEN age >= 65 THEN 'senior'
        ELSE 'minor'
        END FROM persons`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.whenClauses[0].condition.type).toBe('binary');
    });

    it('should parse searched CASE with OR conditions', () => {
      const sql = `SELECT CASE
        WHEN status = 'active' OR status = 'pending' THEN 'valid'
        ELSE 'invalid'
        END FROM accounts`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.whenClauses[0].condition.type).toBe('binary');
      expect((caseExpr.whenClauses[0].condition as any).op).toBe('or');
    });

    it('should parse searched CASE with BETWEEN condition', () => {
      const sql = `SELECT CASE
        WHEN price BETWEEN 0 AND 100 THEN 'cheap'
        WHEN price BETWEEN 100 AND 500 THEN 'moderate'
        ELSE 'expensive'
        END FROM products`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.whenClauses[0].condition.type).toBe('between');
    });

    it('should parse searched CASE with IN condition', () => {
      const sql = `SELECT CASE
        WHEN category IN ('electronics', 'computers') THEN 'tech'
        ELSE 'other'
        END FROM products`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.whenClauses[0].condition.type).toBe('in');
    });

    it('should parse searched CASE with LIKE condition', () => {
      const sql = `SELECT CASE
        WHEN name LIKE 'A%' THEN 'starts_with_A'
        WHEN name LIKE '%z' THEN 'ends_with_z'
        ELSE 'other'
        END FROM items`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.whenClauses.length).toBe(2);
    });
  });

  // ===========================================================================
  // CASE IN SELECT CLAUSE
  // ===========================================================================

  describe('CASE in SELECT Clause', () => {
    it('should parse CASE alongside other columns', () => {
      const sql = `SELECT
        id,
        CASE status WHEN 'active' THEN 1 ELSE 0 END AS is_active,
        name
        FROM users`;
      const result = parser.parse(sql);

      expect(result.columns.length).toBe(3);
      expect(result.columns[1].expr.type).toBe('case');
    });

    it('should parse multiple CASE expressions in SELECT', () => {
      const sql = `SELECT
        CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END AS age_group,
        CASE WHEN income > 50000 THEN 'high' ELSE 'low' END AS income_bracket
        FROM persons`;
      const result = parser.parse(sql);

      expect(result.columns.length).toBe(2);
      expect(result.columns[0].expr.type).toBe('case');
      expect(result.columns[1].expr.type).toBe('case');
    });

    it('should parse CASE in arithmetic expression', () => {
      const sql = `SELECT price * CASE WHEN discount > 0 THEN (1 - discount/100) ELSE 1 END AS final_price
        FROM products`;
      const result = parser.parse(sql);

      const binary = result.columns[0].expr as any;
      expect(binary.type).toBe('binary');
      expect(binary.right.type).toBe('case');
    });

    it('should parse CASE with aggregate result', () => {
      const sql = `SELECT CASE
        WHEN count(*) > 100 THEN 'many'
        WHEN count(*) > 10 THEN 'some'
        ELSE 'few'
        END AS quantity FROM orders GROUP BY category`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.whenClauses[0].condition.left.type).toBe('aggregate');
    });

    it('should parse CASE in function argument', () => {
      const sql = `SELECT concat(
        CASE gender WHEN 'M' THEN 'Mr.' ELSE 'Ms.' END,
        ' ',
        name
      ) AS title FROM persons`;
      const result = parser.parse(sql);

      const funcExpr = result.columns[0].expr as any;
      expect(funcExpr.type).toBe('function');
      expect(funcExpr.args[0].type).toBe('case');
    });
  });

  // ===========================================================================
  // CASE IN WHERE CLAUSE
  // ===========================================================================

  describe('CASE in WHERE Clause', () => {
    it('should parse CASE in WHERE comparison', () => {
      const sql = `SELECT * FROM orders
        WHERE CASE WHEN vip THEN amount * 0.9 ELSE amount END > 100`;
      const result = parser.parse(sql);

      const where = result.where as any;
      expect(where.type).toBe('binary');
      expect(where.left.type).toBe('case');
    });

    it('should parse CASE in WHERE equality', () => {
      const sql = `SELECT * FROM users
        WHERE CASE status WHEN 'active' THEN 1 ELSE 0 END = 1`;
      const result = parser.parse(sql);

      const where = result.where as any;
      expect(where.left.type).toBe('case');
    });

    it('should parse CASE in WHERE with AND', () => {
      const sql = `SELECT * FROM products
        WHERE CASE WHEN discounted THEN price * 0.8 ELSE price END < 100
        AND CASE category WHEN 'electronics' THEN 1 ELSE 0 END = 1`;
      const result = parser.parse(sql);

      const where = result.where as any;
      expect(where.type).toBe('binary');
      expect(where.op).toBe('and');
    });

    it('should parse CASE result in IN clause', () => {
      const sql = `SELECT * FROM orders
        WHERE CASE priority
          WHEN 'urgent' THEN 1
          WHEN 'high' THEN 2
          ELSE 3
        END IN (1, 2)`;
      const result = parser.parse(sql);

      const inExpr = result.where as any;
      expect(inExpr.type).toBe('in');
      expect(inExpr.expr.type).toBe('case');
    });

    it('should parse boolean CASE in WHERE', () => {
      const sql = `SELECT * FROM users
        WHERE CASE WHEN premium AND active THEN true ELSE false END`;
      const result = parser.parse(sql);

      expect(result.where?.type).toBe('case');
    });
  });

  // ===========================================================================
  // CASE IN ORDER BY
  // ===========================================================================

  describe('CASE in ORDER BY', () => {
    it('should parse CASE in ORDER BY', () => {
      const sql = `SELECT * FROM products
        ORDER BY CASE WHEN featured THEN 0 ELSE 1 END, price`;
      const result = parser.parse(sql);

      expect(result.orderBy?.length).toBe(2);
      expect(result.orderBy![0].expr.type).toBe('case');
    });

    it('should parse CASE in ORDER BY with direction', () => {
      const sql = `SELECT * FROM tasks
        ORDER BY CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END ASC`;
      const result = parser.parse(sql);

      expect(result.orderBy![0].direction).toBe('asc');
      expect(result.orderBy![0].expr.type).toBe('case');
    });

    it('should parse multiple CASE in ORDER BY', () => {
      const sql = `SELECT * FROM items
        ORDER BY
          CASE WHEN pinned THEN 0 ELSE 1 END,
          CASE category WHEN 'A' THEN 1 WHEN 'B' THEN 2 ELSE 3 END`;
      const result = parser.parse(sql);

      expect(result.orderBy?.length).toBe(2);
      expect(result.orderBy![0].expr.type).toBe('case');
      expect(result.orderBy![1].expr.type).toBe('case');
    });

    it('should parse CASE with NULLS FIRST in ORDER BY', () => {
      const sql = `SELECT * FROM data
        ORDER BY CASE WHEN important THEN value ELSE NULL END NULLS LAST`;
      const result = parser.parse(sql);

      expect(result.orderBy![0].nullsFirst).toBe(false);
    });
  });

  // ===========================================================================
  // NESTED CASE EXPRESSIONS
  // ===========================================================================

  describe('Nested CASE Expressions', () => {
    it('should parse CASE nested in THEN clause', () => {
      const sql = `SELECT CASE type
        WHEN 'A' THEN CASE size WHEN 'large' THEN 100 ELSE 50 END
        ELSE 0
        END FROM items`;
      const result = parser.parse(sql);

      const outerCase = result.columns[0].expr as SimpleCaseExpression;
      expect(outerCase.whenClauses[0].result.type).toBe('case');
    });

    it('should parse CASE nested in ELSE clause', () => {
      const sql = `SELECT CASE WHEN premium THEN 'VIP'
        ELSE CASE level WHEN 1 THEN 'Basic' ELSE 'Standard' END
        END FROM users`;
      const result = parser.parse(sql);

      const outerCase = result.columns[0].expr as SearchedCaseExpression;
      expect(outerCase.elseClause?.type).toBe('case');
    });

    it('should parse CASE nested in WHEN condition', () => {
      const sql = `SELECT CASE
        WHEN CASE type WHEN 'A' THEN 1 ELSE 0 END = 1 THEN 'Type A'
        ELSE 'Other'
        END FROM items`;
      const result = parser.parse(sql);

      const outerCase = result.columns[0].expr as SearchedCaseExpression;
      expect(outerCase.whenClauses[0].condition.left.type).toBe('case');
    });

    it('should parse deeply nested CASE expressions', () => {
      const sql = `SELECT CASE
        WHEN a > 0 THEN CASE
          WHEN b > 0 THEN CASE WHEN c > 0 THEN 'all_positive' ELSE 'c_not_positive' END
          ELSE 'b_not_positive'
        END
        ELSE 'a_not_positive'
        END FROM numbers`;
      const result = parser.parse(sql);

      const level1 = result.columns[0].expr as SearchedCaseExpression;
      const level2 = level1.whenClauses[0].result as SearchedCaseExpression;
      const level3 = level2.whenClauses[0].result as SearchedCaseExpression;
      expect(level3.type).toBe('case');
    });

    it('should parse mixed simple and searched CASE nesting', () => {
      const sql = `SELECT CASE category
        WHEN 'tech' THEN CASE WHEN price > 1000 THEN 'expensive_tech' ELSE 'cheap_tech' END
        ELSE 'other'
        END FROM products`;
      const result = parser.parse(sql);

      const outerCase = result.columns[0].expr as SimpleCaseExpression;
      const innerCase = outerCase.whenClauses[0].result as SearchedCaseExpression;
      expect(outerCase.caseType).toBe('simple');
      expect(innerCase.caseType).toBe('searched');
    });
  });

  // ===========================================================================
  // CASE WITH NULL HANDLING
  // ===========================================================================

  describe('CASE with NULL Handling', () => {
    it('should parse CASE with NULL result', () => {
      const sql = `SELECT CASE WHEN condition THEN value ELSE NULL END FROM data`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.elseClause?.type).toBe('literal');
      expect((caseExpr.elseClause as any).value).toBeNull();
    });

    it('should parse CASE with IS NULL condition', () => {
      const sql = `SELECT CASE
        WHEN value IS NULL THEN 'missing'
        ELSE 'present'
        END FROM data`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.whenClauses[0].condition.type).toBe('isNull');
    });

    it('should parse CASE with IS NOT NULL condition', () => {
      const sql = `SELECT CASE
        WHEN email IS NOT NULL THEN email
        ELSE 'no_email'
        END FROM users`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      const condition = caseExpr.whenClauses[0].condition as any;
      expect(condition.type).toBe('isNull');
      expect(condition.isNot).toBe(true);
    });

    it('should parse simple CASE comparing to NULL', () => {
      const sql = `SELECT CASE nullable_col WHEN NULL THEN 'was_null' ELSE 'not_null' END FROM data`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SimpleCaseExpression;
      expect((caseExpr.whenClauses[0].value as any).value).toBeNull();
    });

    it('should parse CASE with COALESCE in condition', () => {
      const sql = `SELECT CASE WHEN coalesce(nullable_val, 0) > 10 THEN 'high' ELSE 'low' END FROM data`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.whenClauses[0].condition.left.type).toBe('function');
    });
  });

  // ===========================================================================
  // CASE WITH SUBQUERIES
  // ===========================================================================

  describe('CASE with Subqueries', () => {
    it('should parse CASE with scalar subquery in condition', () => {
      const sql = `SELECT CASE
        WHEN (SELECT count(*) FROM orders WHERE user_id = u.id) > 10 THEN 'frequent'
        ELSE 'occasional'
        END FROM users u`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.whenClauses[0].condition.left.type).toBe('subquery');
    });

    it('should parse CASE with scalar subquery in result', () => {
      const sql = `SELECT CASE WHEN premium THEN
        (SELECT max(discount) FROM premium_discounts)
        ELSE 0
        END FROM users`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.whenClauses[0].result.type).toBe('subquery');
    });

    it('should parse CASE with EXISTS in condition', () => {
      const sql = `SELECT CASE
        WHEN EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) THEN 'has_orders'
        ELSE 'no_orders'
        END FROM users`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.whenClauses[0].condition.type).toBe('exists');
    });

    it('should parse CASE with IN subquery in condition', () => {
      const sql = `SELECT CASE
        WHEN category_id IN (SELECT id FROM featured_categories) THEN 'featured'
        ELSE 'regular'
        END FROM products`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect(caseExpr.whenClauses[0].condition.type).toBe('in');
    });

    it('should parse simple CASE comparing to subquery result', () => {
      const sql = `SELECT CASE status
        WHEN (SELECT default_status FROM settings) THEN 'default'
        ELSE 'custom'
        END FROM items`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SimpleCaseExpression;
      expect(caseExpr.whenClauses[0].value.type).toBe('subquery');
    });
  });

  // ===========================================================================
  // CASE WITH AGGREGATES
  // ===========================================================================

  describe('CASE with Aggregates', () => {
    it('should parse aggregate containing CASE', () => {
      const sql = `SELECT sum(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS total
        FROM orders`;
      const result = parser.parse(sql);

      const aggExpr = result.columns[0].expr as any;
      expect(aggExpr.type).toBe('aggregate');
      expect(aggExpr.arg.type).toBe('case');
    });

    it('should parse COUNT with CASE (conditional count)', () => {
      const sql = `SELECT count(CASE WHEN active THEN 1 END) AS active_count FROM users`;
      const result = parser.parse(sql);

      const aggExpr = result.columns[0].expr as any;
      expect(aggExpr.name).toBe('count');
      expect(aggExpr.arg.type).toBe('case');
    });

    it('should parse AVG with CASE', () => {
      const sql = `SELECT avg(CASE WHEN type = 'A' THEN value END) AS avg_a FROM data`;
      const result = parser.parse(sql);

      const aggExpr = result.columns[0].expr as any;
      expect(aggExpr.name).toBe('avg');
    });

    it('should parse multiple aggregates with CASE', () => {
      const sql = `SELECT
        sum(CASE WHEN type = 'revenue' THEN amount ELSE 0 END) AS total_revenue,
        sum(CASE WHEN type = 'expense' THEN amount ELSE 0 END) AS total_expense
        FROM transactions`;
      const result = parser.parse(sql);

      expect(result.columns.length).toBe(2);
      expect(result.columns[0].expr.type).toBe('aggregate');
      expect(result.columns[1].expr.type).toBe('aggregate');
    });

    it('should parse CASE with aggregate in condition', () => {
      const sql = `SELECT category,
        CASE
          WHEN count(*) > 100 THEN 'large'
          WHEN count(*) > 10 THEN 'medium'
          ELSE 'small'
        END AS size
        FROM products GROUP BY category`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[1].expr as SearchedCaseExpression;
      expect(caseExpr.whenClauses[0].condition.left.type).toBe('aggregate');
    });

    it('should parse CASE in HAVING clause', () => {
      const sql = `SELECT category, count(*) AS cnt FROM products
        GROUP BY category
        HAVING CASE WHEN category = 'special' THEN count(*) > 5 ELSE count(*) > 10 END`;
      const result = parser.parse(sql);

      expect(result.having?.type).toBe('case');
    });
  });

  // ===========================================================================
  // COALESCE (CASE Shorthand)
  // ===========================================================================

  describe('COALESCE Function (CASE Shorthand)', () => {
    it('should parse COALESCE with two arguments', () => {
      const sql = `SELECT coalesce(nullable_col, 'default') FROM data`;
      const result = parser.parse(sql);

      const funcExpr = result.columns[0].expr as any;
      expect(funcExpr.type).toBe('function');
      expect(funcExpr.name.toLowerCase()).toBe('coalesce');
      expect(funcExpr.args.length).toBe(2);
    });

    it('should parse COALESCE with multiple arguments', () => {
      const sql = `SELECT coalesce(col1, col2, col3, 'fallback') FROM data`;
      const result = parser.parse(sql);

      const funcExpr = result.columns[0].expr as any;
      expect(funcExpr.args.length).toBe(4);
    });

    it('should parse nested COALESCE', () => {
      const sql = `SELECT coalesce(coalesce(a, b), c) FROM data`;
      const result = parser.parse(sql);

      const outerCoalesce = result.columns[0].expr as any;
      expect(outerCoalesce.args[0].type).toBe('function');
      expect(outerCoalesce.args[0].name.toLowerCase()).toBe('coalesce');
    });

    it('should parse COALESCE in WHERE clause', () => {
      const sql = `SELECT * FROM users WHERE coalesce(status, 'inactive') = 'active'`;
      const result = parser.parse(sql);

      const where = result.where as any;
      expect(where.left.type).toBe('function');
    });

    it('should parse COALESCE with expressions', () => {
      const sql = `SELECT coalesce(price * discount, price, 0) AS final_price FROM products`;
      const result = parser.parse(sql);

      const funcExpr = result.columns[0].expr as any;
      expect(funcExpr.args[0].type).toBe('binary');
    });
  });

  // ===========================================================================
  // NULLIF (CASE Shorthand)
  // ===========================================================================

  describe('NULLIF Function (CASE Shorthand)', () => {
    it('should parse NULLIF with two arguments', () => {
      const sql = `SELECT nullif(value, 0) FROM data`;
      const result = parser.parse(sql);

      const funcExpr = result.columns[0].expr as any;
      expect(funcExpr.type).toBe('function');
      expect(funcExpr.name.toLowerCase()).toBe('nullif');
      expect(funcExpr.args.length).toBe(2);
    });

    it('should parse NULLIF with string comparison', () => {
      const sql = `SELECT nullif(status, 'unknown') FROM items`;
      const result = parser.parse(sql);

      const funcExpr = result.columns[0].expr as any;
      expect(funcExpr.args[1].value).toBe('unknown');
    });

    it('should parse NULLIF in division (avoid divide by zero)', () => {
      const sql = `SELECT total / nullif(cnt, 0) AS average FROM stats`;
      const result = parser.parse(sql);

      const binary = result.columns[0].expr as any;
      expect(binary.type).toBe('binary');
      expect(binary.right.type).toBe('function');
      expect(binary.right.name.toLowerCase()).toBe('nullif');
    });

    it('should parse nested NULLIF and COALESCE', () => {
      const sql = `SELECT coalesce(nullif(value, 0), default_value) FROM data`;
      const result = parser.parse(sql);

      const coalesce = result.columns[0].expr as any;
      expect(coalesce.args[0].name.toLowerCase()).toBe('nullif');
    });
  });

  // ===========================================================================
  // IIF (Ternary CASE Shorthand)
  // ===========================================================================

  describe('IIF Function (Ternary CASE Shorthand)', () => {
    it('should parse IIF with three arguments', () => {
      const sql = `SELECT iif(condition, true_value, false_value) FROM data`;
      const result = parser.parse(sql);

      const funcExpr = result.columns[0].expr as any;
      expect(funcExpr.type).toBe('function');
      expect(funcExpr.name.toLowerCase()).toBe('iif');
      expect(funcExpr.args.length).toBe(3);
    });

    it('should parse IIF with comparison condition', () => {
      const sql = `SELECT iif(age >= 18, 'adult', 'minor') AS age_group FROM persons`;
      const result = parser.parse(sql);

      const funcExpr = result.columns[0].expr as any;
      expect(funcExpr.args[0].type).toBe('binary');
    });

    it('should parse nested IIF', () => {
      const sql = `SELECT iif(x > 0, 'positive', iif(x < 0, 'negative', 'zero')) FROM numbers`;
      const result = parser.parse(sql);

      const outerIif = result.columns[0].expr as any;
      expect(outerIif.args[2].type).toBe('function');
      expect(outerIif.args[2].name.toLowerCase()).toBe('iif');
    });

    it('should parse IIF in WHERE clause', () => {
      const sql = `SELECT * FROM orders WHERE iif(vip, amount * 0.9, amount) > 100`;
      const result = parser.parse(sql);

      const where = result.where as any;
      expect(where.left.type).toBe('function');
    });

    it('should parse IIF with NULL values', () => {
      const sql = `SELECT iif(flag, value, NULL) FROM data`;
      const result = parser.parse(sql);

      const funcExpr = result.columns[0].expr as any;
      expect(funcExpr.args[2].value).toBeNull();
    });
  });

  // ===========================================================================
  // CASE EVALUATION
  // ===========================================================================

  describe('CASE Expression Evaluation', () => {
    const testRow = {
      status: 'active',
      priority: 2,
      amount: 150,
      vip: true,
      nullable_col: null,
      x: 5,
      y: -3,
    };

    it('should evaluate simple CASE returning matching value', () => {
      const caseExpr: SimpleCaseExpression = {
        type: 'case',
        caseType: 'simple',
        operand: { type: 'column', name: 'status' },
        whenClauses: [
          { value: { type: 'literal', value: 'active' }, result: { type: 'literal', value: 1 } },
          { value: { type: 'literal', value: 'inactive' }, result: { type: 'literal', value: 0 } },
        ],
        elseClause: { type: 'literal', value: -1 },
      };

      const result = evaluateCaseExpression(caseExpr, testRow);
      expect(result).toBe(1);
    });

    it('should evaluate simple CASE returning ELSE', () => {
      const caseExpr: SimpleCaseExpression = {
        type: 'case',
        caseType: 'simple',
        operand: { type: 'column', name: 'status' },
        whenClauses: [
          { value: { type: 'literal', value: 'pending' }, result: { type: 'literal', value: 1 } },
        ],
        elseClause: { type: 'literal', value: 0 },
      };

      const result = evaluateCaseExpression(caseExpr, testRow);
      expect(result).toBe(0);
    });

    it('should evaluate simple CASE returning NULL when no ELSE', () => {
      const caseExpr: SimpleCaseExpression = {
        type: 'case',
        caseType: 'simple',
        operand: { type: 'column', name: 'status' },
        whenClauses: [
          { value: { type: 'literal', value: 'pending' }, result: { type: 'literal', value: 1 } },
        ],
      };

      const result = evaluateCaseExpression(caseExpr, testRow);
      expect(result).toBeNull();
    });

    it('should evaluate searched CASE with true condition', () => {
      const caseExpr: SearchedCaseExpression = {
        type: 'case',
        caseType: 'searched',
        whenClauses: [
          {
            condition: { type: 'binary', op: 'gt', left: { type: 'column', name: 'x' }, right: { type: 'literal', value: 0 } },
            result: { type: 'literal', value: 'positive' },
          },
        ],
        elseClause: { type: 'literal', value: 'non-positive' },
      };

      const result = evaluateCaseExpression(caseExpr, testRow);
      expect(result).toBe('positive');
    });

    it('should evaluate searched CASE with first matching condition', () => {
      const caseExpr: SearchedCaseExpression = {
        type: 'case',
        caseType: 'searched',
        whenClauses: [
          {
            condition: { type: 'binary', op: 'gt', left: { type: 'column', name: 'x' }, right: { type: 'literal', value: 10 } },
            result: { type: 'literal', value: 'very_high' },
          },
          {
            condition: { type: 'binary', op: 'gt', left: { type: 'column', name: 'x' }, right: { type: 'literal', value: 0 } },
            result: { type: 'literal', value: 'positive' },
          },
        ],
        elseClause: { type: 'literal', value: 'other' },
      };

      const result = evaluateCaseExpression(caseExpr, testRow);
      expect(result).toBe('positive');
    });

    it('should evaluate CASE with expression result', () => {
      const caseExpr: SearchedCaseExpression = {
        type: 'case',
        caseType: 'searched',
        whenClauses: [
          {
            condition: { type: 'column', name: 'vip' },
            result: { type: 'binary', op: 'mul', left: { type: 'column', name: 'amount' }, right: { type: 'literal', value: 0.9 } },
          },
        ],
        elseClause: { type: 'column', name: 'amount' },
      };

      const result = evaluateCaseExpression(caseExpr, testRow);
      expect(result).toBe(135); // 150 * 0.9
    });

    it('should evaluate nested CASE expressions', () => {
      const innerCase: SearchedCaseExpression = {
        type: 'case',
        caseType: 'searched',
        whenClauses: [
          {
            condition: { type: 'binary', op: 'gt', left: { type: 'column', name: 'priority' }, right: { type: 'literal', value: 1 } },
            result: { type: 'literal', value: 'high_priority' },
          },
        ],
        elseClause: { type: 'literal', value: 'low_priority' },
      };

      const outerCase: SearchedCaseExpression = {
        type: 'case',
        caseType: 'searched',
        whenClauses: [
          {
            condition: { type: 'column', name: 'vip' },
            result: innerCase,
          },
        ],
        elseClause: { type: 'literal', value: 'regular' },
      };

      const result = evaluateCaseExpression(outerCase, testRow);
      expect(result).toBe('high_priority');
    });

    it('should handle NULL in CASE operand correctly', () => {
      const caseExpr: SimpleCaseExpression = {
        type: 'case',
        caseType: 'simple',
        operand: { type: 'column', name: 'nullable_col' },
        whenClauses: [
          { value: { type: 'literal', value: 'test' }, result: { type: 'literal', value: 1 } },
        ],
        elseClause: { type: 'literal', value: 0 },
      };

      const result = evaluateCaseExpression(caseExpr, testRow);
      expect(result).toBe(0); // NULL doesn't match 'test', falls to ELSE
    });
  });

  // ===========================================================================
  // EDGE CASES AND ERROR HANDLING
  // ===========================================================================

  describe('Edge Cases and Error Handling', () => {
    it('should reject CASE without any WHEN clauses', () => {
      const sql = `SELECT CASE status END FROM users`;
      expect(() => parser.parse(sql)).toThrow(/expected WHEN/i);
    });

    it('should reject CASE with WHEN but no THEN', () => {
      const sql = `SELECT CASE WHEN x > 0 END FROM numbers`;
      expect(() => parser.parse(sql)).toThrow(/expected THEN/i);
    });

    it('should reject CASE without END keyword', () => {
      const sql = `SELECT CASE WHEN x > 0 THEN 1 FROM numbers`;
      expect(() => parser.parse(sql)).toThrow(/expected END/i);
    });

    it('should reject simple CASE with condition after WHEN', () => {
      const sql = `SELECT CASE status WHEN > 0 THEN 1 END FROM data`;
      expect(() => parser.parse(sql)).toThrow(/unexpected/i);
    });

    it('should handle CASE with only ELSE (edge case)', () => {
      // Some SQL dialects allow this, others don't
      const sql = `SELECT CASE ELSE 'default' END FROM data`;
      // This should either parse with empty whenClauses or throw
      expect(() => parser.parse(sql)).toThrow(/expected WHEN/i);
    });

    it('should handle empty string in CASE', () => {
      const sql = `SELECT CASE WHEN name = '' THEN 'empty' ELSE 'not_empty' END FROM data`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SearchedCaseExpression;
      expect((caseExpr.whenClauses[0].condition as any).right.value).toBe('');
    });

    it('should handle CASE-insensitive keywords', () => {
      const sql = `SELECT case WHEN x > 0 then 'positive' ELSE 'other' End FROM numbers`;
      const result = parser.parse(sql);

      expect(result.columns[0].expr.type).toBe('case');
    });

    it('should track location information for errors', () => {
      const sql = `SELECT CASE status
        WHEN 'active' THEN 1
        WHEN
      FROM users`;
      try {
        parser.parse(sql);
        expect.fail('Should have thrown');
      } catch (e: any) {
        expect(e.location).toBeDefined();
        expect(e.location.line).toBeGreaterThan(1);
      }
    });

    it('should handle very long CASE chains', () => {
      const whenClauses = Array.from({ length: 50 }, (_, i) =>
        `WHEN ${i} THEN 'val${i}'`
      ).join(' ');
      const sql = `SELECT CASE x ${whenClauses} ELSE 'other' END FROM data`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as SimpleCaseExpression;
      expect(caseExpr.whenClauses.length).toBe(50);
    });
  });

  // ===========================================================================
  // TYPE INFERENCE
  // ===========================================================================

  describe('CASE Type Inference', () => {
    it('should infer string type from string results', () => {
      const sql = `SELECT CASE WHEN true THEN 'yes' ELSE 'no' END FROM dual`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as CaseExpression;
      expect(caseExpr.inferredType).toBe('string');
    });

    it('should infer number type from numeric results', () => {
      const sql = `SELECT CASE status WHEN 'active' THEN 1 ELSE 0 END FROM users`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as CaseExpression;
      expect(caseExpr.inferredType).toBe('number');
    });

    it('should infer nullable type when ELSE is missing', () => {
      const sql = `SELECT CASE WHEN condition THEN value END FROM data`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as CaseExpression;
      expect(caseExpr.nullable).toBe(true);
    });

    it('should infer nullable type when NULL is a possible result', () => {
      const sql = `SELECT CASE WHEN condition THEN value ELSE NULL END FROM data`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr as CaseExpression;
      expect(caseExpr.nullable).toBe(true);
    });
  });
});
