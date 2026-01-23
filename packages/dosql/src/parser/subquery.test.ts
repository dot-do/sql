/**
 * DoSQL Subquery Tests
 *
 * Comprehensive test suite for subquery support including:
 * - Scalar subqueries
 * - IN subqueries
 * - EXISTS subqueries
 * - Correlated subqueries
 * - Derived tables (subqueries in FROM clause)
 * - ANY/ALL/SOME operators
 *
 * Following TDD approach - tests are written first, implementation follows.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  SubqueryParser,
  parseSubquery,
  type SubqueryNode,
  type ParsedSelect,
  type ParsedExpr,
  type ExistsExpression,
  type QuantifiedComparison,
  type TupleExpression,
  type DerivedTable,
  type ParsedJoin,
} from './subquery.js';

// =============================================================================
// TYPE GUARDS FOR EXPRESSION NARROWING
// =============================================================================

/** Type guard for subquery nodes */
function isSubqueryNode(expr: ParsedExpr | undefined): expr is SubqueryNode {
  return expr !== undefined && expr.type === 'subquery';
}

/** Type guard for exists expressions */
function isExistsExpr(expr: ParsedExpr | undefined): expr is ExistsExpression {
  return expr !== undefined && expr.type === 'exists';
}

/** Type guard for binary expressions */
function isBinaryExpr(expr: ParsedExpr | undefined): expr is { type: 'binary'; op: string; left: ParsedExpr; right: ParsedExpr } {
  return expr !== undefined && expr.type === 'binary';
}

/** Type guard for unary expressions */
function isUnaryExpr(expr: ParsedExpr | undefined): expr is { type: 'unary'; op: string; operand: ParsedExpr } {
  return expr !== undefined && expr.type === 'unary';
}

/** Type guard for IN expressions */
function isInExpr(expr: ParsedExpr | undefined): expr is { type: 'in'; expr: ParsedExpr; values: ParsedExpr[] | SubqueryNode; not?: boolean } {
  return expr !== undefined && expr.type === 'in';
}

/** Type guard for tuple expressions */
function isTupleExpr(expr: ParsedExpr | undefined): expr is TupleExpression {
  return expr !== undefined && expr.type === 'tuple';
}

/** Type guard for quantified comparisons (ANY/ALL/SOME) */
function isQuantifiedComparison(expr: ParsedExpr | undefined): expr is QuantifiedComparison {
  return expr !== undefined && expr.type === 'comparison' && 'quantifier' in expr;
}

/** Type guard for case expressions */
function isCaseExpr(expr: ParsedExpr | undefined): expr is { type: 'case'; operand?: ParsedExpr; whens: { condition: ParsedExpr; result: ParsedExpr }[]; else_?: ParsedExpr } {
  return expr !== undefined && expr.type === 'case';
}

/** Type guard for literal expressions */
function isLiteralExpr(expr: ParsedExpr | undefined): expr is { type: 'literal'; value: string | number | boolean | null } {
  return expr !== undefined && expr.type === 'literal';
}

/** Type guard for star expressions */
function isStarExpr(expr: ParsedExpr | undefined): expr is { type: 'star' } {
  return expr !== undefined && expr.type === 'star';
}

/** Type guard for aggregate expressions */
function isAggregateExpr(expr: ParsedExpr | undefined): expr is { type: 'aggregate'; name: string; arg: ParsedExpr | '*'; distinct?: boolean } {
  return expr !== undefined && expr.type === 'aggregate';
}

/** Type guard for derived tables */
function isDerivedTable(from: { type: string } | undefined): from is DerivedTable {
  return from !== undefined && from.type === 'derived';
}

/** Type guard for table references */
function isTableRef(from: { type: string } | undefined): from is { type: 'table'; table: string; alias?: string } {
  return from !== undefined && from.type === 'table';
}

describe('DoSQL Subquery Parser', () => {
  let parser: SubqueryParser;

  beforeEach(() => {
    parser = new SubqueryParser();
  });

  // ===========================================================================
  // SCALAR SUBQUERIES
  // ===========================================================================

  describe('Scalar Subqueries', () => {
    it('should parse scalar subquery in SELECT clause', () => {
      const sql = 'SELECT (SELECT max(x) FROM t) FROM dual';
      const result = parser.parse(sql);

      expect(result.type).toBe('select');
      expect(result.columns[0].expr.type).toBe('subquery');
      expect((result.columns[0].expr as SubqueryNode).subqueryType).toBe('scalar');
    });

    it('should parse scalar subquery with alias', () => {
      const sql = 'SELECT (SELECT count(*) FROM orders) AS total_orders FROM dual';
      const result = parser.parse(sql);

      expect(result.columns[0].alias).toBe('total_orders');
      expect(result.columns[0].expr.type).toBe('subquery');
    });

    it('should parse multiple scalar subqueries in SELECT', () => {
      const sql = `SELECT
        (SELECT count(*) FROM users) AS user_count,
        (SELECT count(*) FROM orders) AS order_count
        FROM dual`;
      const result = parser.parse(sql);

      expect(result.columns.length).toBe(2);
      expect(result.columns[0].expr.type).toBe('subquery');
      expect(result.columns[1].expr.type).toBe('subquery');
    });

    it('should parse scalar subquery in arithmetic expression', () => {
      const sql = 'SELECT price - (SELECT avg(price) FROM products) AS diff FROM products';
      const result = parser.parse(sql);

      expect(result.columns[0].expr.type).toBe('binary');
      const binary = result.columns[0].expr;
      if (isBinaryExpr(binary)) {
        expect(binary.right.type).toBe('subquery');
      } else {
        expect.fail('Expected binary expression');
      }
    });

    it('should parse nested scalar subqueries', () => {
      const sql = 'SELECT (SELECT (SELECT max(id) FROM t1) FROM t2) FROM t3';
      const result = parser.parse(sql);

      const outerSubquery = result.columns[0].expr as SubqueryNode;
      expect(outerSubquery.type).toBe('subquery');
      expect(outerSubquery.query.columns[0].expr.type).toBe('subquery');
    });

    it('should parse scalar subquery in WHERE comparison', () => {
      const sql = 'SELECT * FROM orders WHERE amount > (SELECT avg(amount) FROM orders)';
      const result = parser.parse(sql);

      expect(result.where).toBeDefined();
      if (isBinaryExpr(result.where)) {
        expect(result.where.right.type).toBe('subquery');
      } else {
        expect.fail('Expected binary expression in WHERE');
      }
    });

    it('should parse scalar subquery with LIMIT 1', () => {
      const sql = 'SELECT * FROM users WHERE id = (SELECT user_id FROM orders ORDER BY created_at DESC LIMIT 1)';
      const result = parser.parse(sql);

      if (isBinaryExpr(result.where) && isSubqueryNode(result.where.right)) {
        expect(result.where.right.query.limit).toBe(1);
      } else {
        expect.fail('Expected binary expression with subquery on right');
      }
    });

    it('should parse scalar subquery with aggregates', () => {
      const sql = 'SELECT (SELECT sum(quantity) FROM order_items WHERE order_id = orders.id) AS total_qty FROM orders';
      const result = parser.parse(sql);

      const subquery = result.columns[0].expr as SubqueryNode;
      expect(subquery.query.columns[0].expr.type).toBe('aggregate');
    });
  });

  // ===========================================================================
  // IN SUBQUERIES
  // ===========================================================================

  describe('IN Subqueries', () => {
    it('should parse simple IN subquery', () => {
      const sql = 'SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)';
      const result = parser.parse(sql);

      expect(result.where?.type).toBe('in');
      if (isInExpr(result.where) && isSubqueryNode(result.where.values as ParsedExpr)) {
        const subquery = result.where.values as SubqueryNode;
        expect(subquery.type).toBe('subquery');
        expect(subquery.subqueryType).toBe('in');
      } else {
        expect.fail('Expected IN expression with subquery values');
      }
    });

    it('should parse NOT IN subquery', () => {
      const sql = 'SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM blocked_users)';
      const result = parser.parse(sql);

      if (isUnaryExpr(result.where)) {
        expect(result.where.type).toBe('unary');
        expect(result.where.op).toBe('not');
        expect(result.where.operand.type).toBe('in');
      } else {
        expect.fail('Expected unary NOT expression');
      }
    });

    it('should parse IN subquery with WHERE clause', () => {
      const sql = `SELECT * FROM products
        WHERE category_id IN (SELECT id FROM categories WHERE active = true)`;
      const result = parser.parse(sql);

      if (isInExpr(result.where) && isSubqueryNode(result.where.values as ParsedExpr)) {
        const subquery = result.where.values as SubqueryNode;
        expect(subquery.query.where).toBeDefined();
      } else {
        expect.fail('Expected IN expression with subquery');
      }
    });

    it('should parse IN subquery with multiple columns', () => {
      const sql = 'SELECT * FROM orders WHERE (user_id, status) IN (SELECT user_id, status FROM priority_orders)';
      const result = parser.parse(sql);

      if (isInExpr(result.where)) {
        expect(result.where.expr.type).toBe('tuple');
      } else {
        expect.fail('Expected IN expression');
      }
    });

    it('should parse nested IN subqueries', () => {
      const sql = `SELECT * FROM t1
        WHERE x IN (SELECT y FROM t2 WHERE z IN (SELECT z FROM t3))`;
      const result = parser.parse(sql);

      if (isInExpr(result.where) && isSubqueryNode(result.where.values as ParsedExpr)) {
        const subquery = result.where.values as SubqueryNode;
        expect(subquery.query.where?.type).toBe('in');
      } else {
        expect.fail('Expected IN expression with subquery');
      }
    });

    it('should parse IN subquery in JOIN condition', () => {
      const sql = `SELECT * FROM orders o
        JOIN users u ON u.id IN (SELECT user_id FROM premium_users WHERE active = true)`;
      const result = parser.parse(sql);

      expect(result.joins).toBeDefined();
      const joinCondition = result.joins![0].on;
      expect(joinCondition?.type).toBe('in');
    });
  });

  // ===========================================================================
  // EXISTS SUBQUERIES
  // ===========================================================================

  describe('EXISTS Subqueries', () => {
    it('should parse simple EXISTS subquery', () => {
      const sql = 'SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)';
      const result = parser.parse(sql);

      expect(result.where?.type).toBe('exists');
      if (isExistsExpr(result.where)) {
        expect(result.where.subqueryType).toBe('exists');
      } else {
        expect.fail('Expected EXISTS expression');
      }
    });

    it('should parse NOT EXISTS subquery', () => {
      const sql = 'SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM blocked_users WHERE blocked_users.user_id = users.id)';
      const result = parser.parse(sql);

      if (isUnaryExpr(result.where)) {
        expect(result.where.type).toBe('unary');
        expect(result.where.op).toBe('not');
        expect(result.where.operand.type).toBe('exists');
      } else {
        expect.fail('Expected unary NOT expression');
      }
    });

    it('should parse EXISTS with correlated subquery', () => {
      const sql = `SELECT * FROM departments d
        WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.salary > 100000)`;
      const result = parser.parse(sql);

      if (isExistsExpr(result.where)) {
        // The WHERE clause references outer table (d.id)
        expect(result.where.query.where).toBeDefined();
      } else {
        expect.fail('Expected EXISTS expression');
      }
    });

    it('should parse multiple EXISTS in AND condition', () => {
      const sql = `SELECT * FROM products p
        WHERE EXISTS (SELECT 1 FROM inventory WHERE product_id = p.id)
        AND EXISTS (SELECT 1 FROM prices WHERE product_id = p.id)`;
      const result = parser.parse(sql);

      if (isBinaryExpr(result.where)) {
        expect(result.where.type).toBe('binary');
        expect(result.where.op).toBe('and');
      } else {
        expect.fail('Expected binary AND expression');
      }
    });

    it('should parse EXISTS with SELECT 1', () => {
      const sql = 'SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2)';
      const result = parser.parse(sql);

      if (isExistsExpr(result.where)) {
        const firstColExpr = result.where.query.columns[0].expr;
        if (isLiteralExpr(firstColExpr)) {
          expect(firstColExpr.type).toBe('literal');
          expect(firstColExpr.value).toBe(1);
        } else {
          expect.fail('Expected literal expression');
        }
      } else {
        expect.fail('Expected EXISTS expression');
      }
    });

    it('should parse EXISTS with SELECT *', () => {
      const sql = 'SELECT * FROM t WHERE EXISTS (SELECT * FROM t2 WHERE t2.x = t.x)';
      const result = parser.parse(sql);

      if (isExistsExpr(result.where)) {
        expect(result.where.query.columns[0].expr.type).toBe('star');
      } else {
        expect.fail('Expected EXISTS expression');
      }
    });
  });

  // ===========================================================================
  // CORRELATED SUBQUERIES
  // ===========================================================================

  describe('Correlated Subqueries', () => {
    it('should identify outer column reference in scalar subquery', () => {
      const sql = `SELECT id, (SELECT count(*) FROM orders o WHERE o.user_id = u.id) AS order_count
        FROM users u`;
      const result = parser.parse(sql);

      if (isSubqueryNode(result.columns[1].expr)) {
        const subquery = result.columns[1].expr;
        expect(subquery.correlatedColumns).toBeDefined();
        expect(subquery.correlatedColumns).toContainEqual({ table: 'u', column: 'id' });
      } else {
        expect.fail('Expected subquery node');
      }
    });

    it('should identify multiple outer references', () => {
      const sql = `SELECT * FROM orders o
        WHERE amount > (SELECT avg(amount) FROM orders o2 WHERE o2.user_id = o.user_id AND o2.status = o.status)`;
      const result = parser.parse(sql);

      if (isBinaryExpr(result.where) && isSubqueryNode(result.where.right)) {
        expect(result.where.right.correlatedColumns?.length).toBe(2);
      } else {
        expect.fail('Expected binary expression with subquery');
      }
    });

    it('should parse deeply nested correlated subquery', () => {
      const sql = `SELECT * FROM t1
        WHERE x IN (
          SELECT y FROM t2
          WHERE z = (SELECT max(z) FROM t3 WHERE t3.id = t1.id)
        )`;
      const result = parser.parse(sql);

      if (isInExpr(result.where) && isSubqueryNode(result.where.values as ParsedExpr)) {
        const middleSubquery = result.where.values as SubqueryNode;
        if (isBinaryExpr(middleSubquery.query.where) && isSubqueryNode(middleSubquery.query.where.right)) {
          const innerScalar = middleSubquery.query.where.right;
          expect(innerScalar.correlatedColumns).toContainEqual({ table: 't1', column: 'id' });
        } else {
          expect.fail('Expected binary expression with subquery in nested query');
        }
      } else {
        expect.fail('Expected IN expression with subquery');
      }
    });

    it('should track correlation depth', () => {
      const sql = `SELECT * FROM t1
        WHERE EXISTS (
          SELECT 1 FROM t2
          WHERE t2.a = t1.a
          AND EXISTS (SELECT 1 FROM t3 WHERE t3.b = t2.b AND t3.c = t1.c)
        )`;
      const result = parser.parse(sql);

      if (isExistsExpr(result.where)) {
        const outerQuery = result.where.query;
        // Navigate to inner EXISTS - may be accessed via binary AND expression
        if (isBinaryExpr(outerQuery.where)) {
          const rightSide = outerQuery.where.right;
          if (isExistsExpr(rightSide)) {
            // Inner subquery should reference both t1 and t2
            expect(rightSide.correlatedColumns?.length).toBeGreaterThanOrEqual(2);
          }
        }
      }
    });

    it('should handle aliased table references', () => {
      const sql = `SELECT * FROM users AS u
        WHERE (SELECT count(*) FROM orders WHERE orders.user_id = u.id) > 5`;
      const result = parser.parse(sql);

      if (isBinaryExpr(result.where) && isSubqueryNode(result.where.left)) {
        expect(result.where.left.correlatedColumns).toContainEqual({ table: 'u', column: 'id' });
      } else {
        expect.fail('Expected binary expression with subquery on left');
      }
    });
  });

  // ===========================================================================
  // DERIVED TABLES (Subqueries in FROM clause)
  // ===========================================================================

  describe('Derived Tables (FROM clause subqueries)', () => {
    it('should parse simple derived table', () => {
      const sql = 'SELECT * FROM (SELECT id, name FROM users) AS u';
      const result = parser.parse(sql);

      expect(result.from.type).toBe('derived');
      expect(result.from.alias).toBe('u');
    });

    it('should parse derived table without alias', () => {
      // Some SQL dialects require alias, but parser should handle both
      const sql = 'SELECT * FROM (SELECT id FROM users)';
      expect(() => parser.parse(sql)).toThrow(); // Should require alias
    });

    it('should parse derived table with column aliases', () => {
      const sql = 'SELECT * FROM (SELECT id AS user_id, name AS user_name FROM users) AS u';
      const result = parser.parse(sql);

      if (isDerivedTable(result.from)) {
        expect(result.from.query.columns[0].alias).toBe('user_id');
      } else {
        expect.fail('Expected derived table');
      }
    });

    it('should parse derived table in JOIN', () => {
      const sql = `SELECT * FROM orders o
        JOIN (SELECT user_id, count(*) as order_count FROM orders GROUP BY user_id) AS user_orders
        ON o.user_id = user_orders.user_id`;
      const result = parser.parse(sql);

      expect(result.joins![0].table.type).toBe('derived');
    });

    it('should parse multiple derived tables', () => {
      const sql = `SELECT *
        FROM (SELECT * FROM users WHERE active = true) AS active_users
        JOIN (SELECT * FROM orders WHERE status = 'completed') AS completed_orders
        ON active_users.id = completed_orders.user_id`;
      const result = parser.parse(sql);

      expect(result.from.type).toBe('derived');
      expect(result.joins![0].table.type).toBe('derived');
    });

    it('should parse nested derived tables', () => {
      const sql = `SELECT * FROM (
        SELECT * FROM (SELECT * FROM users) AS inner_u
      ) AS outer_u`;
      const result = parser.parse(sql);

      if (isDerivedTable(result.from)) {
        expect(result.from.type).toBe('derived');
        expect(result.from.query.from.type).toBe('derived');
      } else {
        expect.fail('Expected derived table');
      }
    });

    it('should parse derived table with aggregation', () => {
      const sql = `SELECT category, max_price
        FROM (
          SELECT category, max(price) AS max_price
          FROM products
          GROUP BY category
        ) AS category_maxes
        WHERE max_price > 100`;
      const result = parser.parse(sql);

      if (isDerivedTable(result.from)) {
        expect(result.from.query.groupBy).toBeDefined();
      } else {
        expect.fail('Expected derived table');
      }
    });

    it('should parse lateral derived table (LATERAL join)', () => {
      const sql = `SELECT * FROM users u
        CROSS JOIN LATERAL (
          SELECT * FROM orders o WHERE o.user_id = u.id ORDER BY created_at DESC LIMIT 3
        ) AS recent_orders`;
      const result = parser.parse(sql);

      const join = result.joins![0];
      expect(join.lateral).toBe(true);
      expect(join.table.type).toBe('derived');
    });
  });

  // ===========================================================================
  // ANY/ALL/SOME OPERATORS
  // ===========================================================================

  describe('ANY/ALL/SOME Operators', () => {
    it('should parse = ANY subquery', () => {
      const sql = 'SELECT * FROM products WHERE category = ANY (SELECT category FROM featured_categories)';
      const result = parser.parse(sql);

      if (isQuantifiedComparison(result.where)) {
        expect(result.where.type).toBe('comparison');
        expect(result.where.quantifier).toBe('any');
        expect(result.where.right.type).toBe('subquery');
      } else {
        expect.fail('Expected quantified comparison');
      }
    });

    it('should parse > ALL subquery', () => {
      const sql = 'SELECT * FROM products WHERE price > ALL (SELECT price FROM competitor_prices)';
      const result = parser.parse(sql);

      if (isQuantifiedComparison(result.where)) {
        expect(result.where.quantifier).toBe('all');
      } else {
        expect.fail('Expected quantified comparison');
      }
    });

    it('should parse < SOME subquery', () => {
      const sql = 'SELECT * FROM employees WHERE salary < SOME (SELECT salary FROM managers)';
      const result = parser.parse(sql);

      if (isQuantifiedComparison(result.where)) {
        expect(result.where.quantifier).toBe('some'); // SOME is alias for ANY
      } else {
        expect.fail('Expected quantified comparison');
      }
    });

    it('should parse <> ALL subquery', () => {
      const sql = 'SELECT * FROM users WHERE status <> ALL (SELECT status FROM banned_statuses)';
      const result = parser.parse(sql);

      if (isQuantifiedComparison(result.where)) {
        expect(result.where.op).toBe('ne');
        expect(result.where.quantifier).toBe('all');
      } else {
        expect.fail('Expected quantified comparison');
      }
    });

    it('should parse >= ANY with correlated subquery', () => {
      const sql = `SELECT * FROM orders o
        WHERE o.amount >= ANY (SELECT avg_amount FROM customer_stats WHERE customer_id = o.customer_id)`;
      const result = parser.parse(sql);

      if (isQuantifiedComparison(result.where)) {
        expect(result.where.quantifier).toBe('any');
        expect(result.where.right.correlatedColumns).toBeDefined();
      } else {
        expect.fail('Expected quantified comparison');
      }
    });

    it('should parse nested ANY/ALL', () => {
      const sql = `SELECT * FROM products
        WHERE price > ALL (SELECT price FROM products WHERE category = ANY (SELECT id FROM premium_categories))`;
      const result = parser.parse(sql);

      if (isQuantifiedComparison(result.where)) {
        expect(result.where.quantifier).toBe('all');
        const innerWhere = result.where.right.query.where;
        if (isQuantifiedComparison(innerWhere)) {
          expect(innerWhere.quantifier).toBe('any');
        }
      } else {
        expect.fail('Expected quantified comparison');
      }
    });
  });

  // ===========================================================================
  // COMPLEX COMBINATIONS
  // ===========================================================================

  describe('Complex Subquery Combinations', () => {
    it('should parse subquery with CTE (WITH clause)', () => {
      const sql = `WITH active_users AS (SELECT * FROM users WHERE active = true)
        SELECT * FROM orders WHERE user_id IN (SELECT id FROM active_users)`;
      const result = parser.parse(sql);

      expect(result.cte).toBeDefined();
      expect(result.cte![0].name).toBe('active_users');
    });

    it('should parse subquery in CASE expression', () => {
      const sql = `SELECT
        CASE
          WHEN (SELECT count(*) FROM orders WHERE user_id = u.id) > 10 THEN 'premium'
          ELSE 'standard'
        END AS user_tier
        FROM users u`;
      const result = parser.parse(sql);

      const caseExpr = result.columns[0].expr;
      if (isCaseExpr(caseExpr)) {
        expect(caseExpr.type).toBe('case');
        const condition = caseExpr.whens[0].condition;
        if (isBinaryExpr(condition)) {
          expect(condition.left.type).toBe('subquery');
        }
      } else {
        expect.fail('Expected CASE expression');
      }
    });

    it('should parse subquery in HAVING clause', () => {
      const sql = `SELECT user_id, count(*) as order_count
        FROM orders
        GROUP BY user_id
        HAVING count(*) > (SELECT avg(order_count) FROM (SELECT count(*) as order_count FROM orders GROUP BY user_id) AS counts)`;
      const result = parser.parse(sql);

      expect(result.having).toBeDefined();
      if (isBinaryExpr(result.having)) {
        expect(result.having.right.type).toBe('subquery');
      } else {
        expect.fail('Expected binary expression in HAVING');
      }
    });

    it('should parse subquery in ORDER BY', () => {
      const sql = `SELECT * FROM products p
        ORDER BY (SELECT count(*) FROM order_items WHERE product_id = p.id) DESC`;
      const result = parser.parse(sql);

      expect(result.orderBy![0].expr.type).toBe('subquery');
    });

    it('should parse multiple subquery types in single query', () => {
      const sql = `SELECT
        u.id,
        (SELECT count(*) FROM orders WHERE user_id = u.id) AS order_count
        FROM users u
        WHERE EXISTS (SELECT 1 FROM premium_users WHERE user_id = u.id)
        AND u.category IN (SELECT category FROM active_categories)`;
      const result = parser.parse(sql);

      // Scalar subquery in SELECT
      expect(result.columns[1].expr.type).toBe('subquery');
      // EXISTS in WHERE
      if (isBinaryExpr(result.where)) {
        expect(result.where.op).toBe('and');
      } else {
        expect.fail('Expected binary AND expression');
      }
    });

    it('should parse UNION inside subquery', () => {
      const sql = `SELECT * FROM products
        WHERE category_id IN (
          SELECT id FROM category_a
          UNION
          SELECT id FROM category_b
        )`;
      const result = parser.parse(sql);

      if (isInExpr(result.where) && isSubqueryNode(result.where.values as ParsedExpr)) {
        const subquery = result.where.values as SubqueryNode;
        expect(subquery.query.union).toBeDefined();
      } else {
        expect.fail('Expected IN expression with subquery');
      }
    });

    it('should handle deeply nested mixed subqueries', () => {
      const sql = `SELECT * FROM t1
        WHERE a IN (
          SELECT b FROM t2
          WHERE c = (
            SELECT d FROM t3
            WHERE EXISTS (
              SELECT 1 FROM t4 WHERE t4.e = t1.a
            )
          )
        )`;
      const result = parser.parse(sql);

      // Verify the structure parsed correctly
      if (isInExpr(result.where) && isSubqueryNode(result.where.values as ParsedExpr)) {
        const inSubquery = result.where.values as SubqueryNode;
        expect(inSubquery.type).toBe('subquery');
        if (isBinaryExpr(inSubquery.query.where) && isSubqueryNode(inSubquery.query.where.right)) {
          const scalarSubquery = inSubquery.query.where.right;
          expect(scalarSubquery.type).toBe('subquery');
          expect(scalarSubquery.query.where?.type).toBe('exists');
        }
      } else {
        expect.fail('Expected IN expression with subquery');
      }
    });
  });

  // ===========================================================================
  // EDGE CASES AND ERROR HANDLING
  // ===========================================================================

  describe('Edge Cases and Error Handling', () => {
    it('should reject subquery returning multiple columns in scalar context', () => {
      const sql = 'SELECT * FROM t WHERE x = (SELECT a, b FROM t2)';
      expect(() => parser.parse(sql)).toThrow(/scalar subquery.*single column/i);
    });

    it('should handle empty parentheses gracefully', () => {
      const sql = 'SELECT * FROM t WHERE x IN ()';
      expect(() => parser.parse(sql)).toThrow(/expected/i);
    });

    it('should parse subquery with all clauses', () => {
      const sql = `SELECT * FROM t WHERE x IN (
        SELECT DISTINCT a
        FROM t2
        WHERE b > 0
        GROUP BY a
        HAVING count(*) > 1
        ORDER BY a
        LIMIT 10
        OFFSET 5
      )`;
      const result = parser.parse(sql);

      if (isInExpr(result.where) && isSubqueryNode(result.where.values as ParsedExpr)) {
        const subquery = result.where.values as SubqueryNode;
        const sq = subquery.query;
        expect(sq.distinct).toBe(true);
        expect(sq.where).toBeDefined();
        expect(sq.groupBy).toBeDefined();
        expect(sq.having).toBeDefined();
        expect(sq.orderBy).toBeDefined();
        expect(sq.limit).toBe(10);
        expect(sq.offset).toBe(5);
      } else {
        expect.fail('Expected IN expression with subquery');
      }
    });

    it('should handle subquery with table alias collision', () => {
      // Inner t should shadow outer t
      const sql = 'SELECT * FROM t WHERE x IN (SELECT x FROM t WHERE y > 0)';
      const result = parser.parse(sql);

      if (isInExpr(result.where) && isSubqueryNode(result.where.values as ParsedExpr)) {
        const subquery = result.where.values as SubqueryNode;
        if (isTableRef(subquery.query.from)) {
          expect(subquery.query.from.table).toBe('t');
        }
      } else {
        expect.fail('Expected IN expression with subquery');
      }
    });

    it('should reject reserved word as unquoted alias', () => {
      const sql = 'SELECT * FROM (SELECT 1 AS select) AS sub';
      // Reserved words as aliases require quoting in strict SQL
      expect(() => parser.parse(sql)).toThrow(/expected identifier/i);
    });

    it('should track line and column numbers for subquery errors', () => {
      const sql = `SELECT * FROM t
        WHERE x IN (
          SELECT
        )`;
      try {
        parser.parse(sql);
        expect.fail('Should have thrown');
      } catch (e: unknown) {
        const error = e as { location?: { line: number } };
        expect(error.location).toBeDefined();
        expect(error.location?.line).toBeGreaterThan(1);
      }
    });
  });

  // ===========================================================================
  // SUBQUERY OPTIMIZATION HINTS
  // ===========================================================================

  describe('Subquery Optimization Metadata', () => {
    it('should mark uncorrelated subqueries', () => {
      const sql = 'SELECT * FROM t WHERE x IN (SELECT y FROM t2)';
      const result = parser.parse(sql);

      if (isInExpr(result.where) && isSubqueryNode(result.where.values as ParsedExpr)) {
        const subquery = result.where.values as SubqueryNode;
        expect(subquery.isCorrelated).toBe(false);
      } else {
        expect.fail('Expected IN expression with subquery');
      }
    });

    it('should mark correlated subqueries', () => {
      const sql = 'SELECT * FROM t WHERE x IN (SELECT y FROM t2 WHERE t2.z = t.z)';
      const result = parser.parse(sql);

      if (isInExpr(result.where) && isSubqueryNode(result.where.values as ParsedExpr)) {
        const subquery = result.where.values as SubqueryNode;
        expect(subquery.isCorrelated).toBe(true);
      } else {
        expect.fail('Expected IN expression with subquery');
      }
    });

    it('should identify semi-join candidate (IN)', () => {
      const sql = 'SELECT * FROM orders WHERE user_id IN (SELECT id FROM active_users)';
      const result = parser.parse(sql);

      if (isInExpr(result.where) && isSubqueryNode(result.where.values as ParsedExpr)) {
        const subquery = result.where.values as SubqueryNode;
        expect(subquery.canBeSemiJoin).toBe(true);
      } else {
        expect.fail('Expected IN expression with subquery');
      }
    });

    it('should identify anti-join candidate (NOT IN)', () => {
      const sql = 'SELECT * FROM orders WHERE user_id NOT IN (SELECT id FROM blocked_users)';
      const result = parser.parse(sql);

      if (isUnaryExpr(result.where) && isInExpr(result.where.operand)) {
        const inExpr = result.where.operand;
        if (isSubqueryNode(inExpr.values as ParsedExpr)) {
          const subquery = inExpr.values as SubqueryNode;
          expect(subquery.canBeAntiJoin).toBe(true);
        }
      } else {
        expect.fail('Expected NOT IN expression');
      }
    });

    it('should identify EXISTS that can be converted to semi-join', () => {
      const sql = 'SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)';
      const result = parser.parse(sql);

      if (isExistsExpr(result.where)) {
        expect(result.where.canBeSemiJoin).toBe(true);
      } else {
        expect.fail('Expected EXISTS expression');
      }
    });
  });
});
