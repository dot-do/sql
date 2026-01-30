/**
 * Correlated Subquery Tests (TDD RED Phase)
 *
 * These tests document the expected behavior of correlated subqueries
 * referencing outer table columns. They focus on the execution behavior
 * where the subquery correctly accesses values from the outer query's
 * current row.
 *
 * Test cases:
 * - Simple correlated: SELECT (SELECT count(*) FROM t2 WHERE t2.x = t1.x) FROM t1
 * - Correlated with alias: SELECT (SELECT count(*) FROM t1 AS x WHERE x.b < t1.b) FROM t1
 * - Correlated in WHERE: SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.x = t1.x)
 * - Multiple correlated subqueries
 *
 * Issue: sql-aayr
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Database } from '../../database.js';

describe('Correlated Subquery Evaluation', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  // ===========================================================================
  // SIMPLE CORRELATED SUBQUERIES
  // ===========================================================================

  describe('Simple Correlated Subqueries', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (x INTEGER)');
      db.exec('CREATE TABLE t2 (x INTEGER)');

      db.exec('INSERT INTO t1 (x) VALUES (1)');
      db.exec('INSERT INTO t1 (x) VALUES (2)');
      db.exec('INSERT INTO t1 (x) VALUES (3)');

      db.exec('INSERT INTO t2 (x) VALUES (1)');
      db.exec('INSERT INTO t2 (x) VALUES (1)');
      db.exec('INSERT INTO t2 (x) VALUES (2)');
    });

    /**
     * Core test: Correlated subquery counting matching rows
     *
     * For each row in t1, count rows in t2 where t2.x = t1.x
     * - t1.x = 1: 2 rows in t2 have x = 1
     * - t1.x = 2: 1 row in t2 has x = 2
     * - t1.x = 3: 0 rows in t2 have x = 3
     */
    it('should support correlated subquery with count', () => {
      const result = db.prepare(`
        SELECT t1.x, (SELECT count(*) FROM t2 WHERE t2.x = t1.x) AS cnt
        FROM t1
      `).all();

      expect(result.length).toBe(3);
      const sorted = (result as { x: number; cnt: number }[]).sort((a, b) => a.x - b.x);

      expect(sorted[0]).toEqual({ x: 1, cnt: 2 });
      expect(sorted[1]).toEqual({ x: 2, cnt: 1 });
      expect(sorted[2]).toEqual({ x: 3, cnt: 0 });
    });

    /**
     * Correlated subquery with max aggregate
     */
    it('should support correlated subquery with max', () => {
      // Create a separate table for this test
      db.exec('CREATE TABLE t3 (x INTEGER, val INTEGER)');
      db.exec('INSERT INTO t3 (x, val) VALUES (1, 100)');
      db.exec('INSERT INTO t3 (x, val) VALUES (1, 200)');
      db.exec('INSERT INTO t3 (x, val) VALUES (2, 300)');

      const result = db.prepare(`
        SELECT t1.x, (SELECT max(t3.val) FROM t3 WHERE t3.x = t1.x) AS max_val
        FROM t1
      `).all();

      expect(result.length).toBe(3);
      const sorted = (result as { x: number; max_val: number | null }[]).sort((a, b) => a.x - b.x);

      expect(sorted[0]).toEqual({ x: 1, max_val: 200 });
      expect(sorted[1]).toEqual({ x: 2, max_val: 300 });
      expect(sorted[2]).toEqual({ x: 3, max_val: null });
    });
  });

  // ===========================================================================
  // CORRELATED SUBQUERY WITH ALIAS (Same table referenced twice)
  // ===========================================================================

  describe('Correlated Subquery with Alias', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)');
      db.exec('INSERT INTO t1 (a, b, c) VALUES (1, 10, 100)');
      db.exec('INSERT INTO t1 (a, b, c) VALUES (2, 20, 200)');
      db.exec('INSERT INTO t1 (a, b, c) VALUES (3, 30, 300)');
      db.exec('INSERT INTO t1 (a, b, c) VALUES (4, 40, 400)');
      db.exec('INSERT INTO t1 (a, b, c) VALUES (5, 50, 500)');
    });

    /**
     * SQLLogicTest case: SELECT (SELECT count(*) FROM t1 AS x WHERE x.b < t1.b) FROM t1
     *
     * This counts, for each row, how many rows have smaller b values.
     * - For a=1, b=10: 0 rows have b < 10
     * - For a=2, b=20: 1 row has b < 20 (b=10)
     * - For a=3, b=30: 2 rows have b < 30 (b=10, b=20)
     * - For a=4, b=40: 3 rows have b < 40 (b=10, b=20, b=30)
     * - For a=5, b=50: 4 rows have b < 50 (b=10, b=20, b=30, b=40)
     */
    it('should support correlated subquery counting rows with smaller values', () => {
      const result = db.prepare(`
        SELECT a, (SELECT count(*) FROM t1 AS x WHERE x.b < t1.b) AS cnt
        FROM t1
      `).all();

      expect(result.length).toBe(5);
      const sorted = (result as { a: number; cnt: number }[]).sort((a, b) => a.a - b.a);

      expect(sorted).toEqual([
        { a: 1, cnt: 0 },
        { a: 2, cnt: 1 },
        { a: 3, cnt: 2 },
        { a: 4, cnt: 3 },
        { a: 5, cnt: 4 },
      ]);
    });

    /**
     * Correlated subquery with max of smaller values
     */
    it('should support correlated subquery with max of smaller values', () => {
      const result = db.prepare(`
        SELECT a, (SELECT max(x.b) FROM t1 AS x WHERE x.b < t1.b) AS max_smaller
        FROM t1
      `).all();

      expect(result.length).toBe(5);
      const sorted = (result as { a: number; max_smaller: number | null }[]).sort((a, b) => a.a - b.a);

      expect(sorted).toEqual([
        { a: 1, max_smaller: null },
        { a: 2, max_smaller: 10 },
        { a: 3, max_smaller: 20 },
        { a: 4, max_smaller: 30 },
        { a: 5, max_smaller: 40 },
      ]);
    });

    /**
     * Correlated subquery with sum (running total pattern)
     */
    it('should support correlated subquery with running sum', () => {
      const result = db.prepare(`
        SELECT a, (SELECT sum(x.c) FROM t1 AS x WHERE x.a <= t1.a) AS running_sum
        FROM t1
      `).all();

      expect(result.length).toBe(5);
      const sorted = (result as { a: number; running_sum: number }[]).sort((a, b) => a.a - b.a);

      // Running sum of c values:
      // a=1: sum(100) = 100
      // a=2: sum(100, 200) = 300
      // a=3: sum(100, 200, 300) = 600
      // a=4: sum(100, 200, 300, 400) = 1000
      // a=5: sum(100, 200, 300, 400, 500) = 1500
      expect(sorted).toEqual([
        { a: 1, running_sum: 100 },
        { a: 2, running_sum: 300 },
        { a: 3, running_sum: 600 },
        { a: 4, running_sum: 1000 },
        { a: 5, running_sum: 1500 },
      ]);
    });
  });

  // ===========================================================================
  // CORRELATED EXISTS IN WHERE CLAUSE
  // ===========================================================================

  describe('Correlated EXISTS in WHERE', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE customers (id INTEGER, name TEXT)');
      db.exec('CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER)');

      db.exec(`INSERT INTO customers (id, name) VALUES (1, 'Alice')`);
      db.exec(`INSERT INTO customers (id, name) VALUES (2, 'Bob')`);
      db.exec(`INSERT INTO customers (id, name) VALUES (3, 'Charlie')`);

      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (1, 1, 100)`);
      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (2, 1, 200)`);
      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (3, 2, 150)`);
      // Charlie (id=3) has no orders
    });

    /**
     * EXISTS with correlated subquery
     */
    it('should support correlated EXISTS subquery', () => {
      const result = db.prepare(`
        SELECT * FROM customers c
        WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
      `).all();

      expect(result.length).toBe(2);
      const sorted = (result as { id: number; name: string }[]).sort((a, b) => a.id - b.id);
      expect(sorted).toEqual([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]);
    });

    /**
     * NOT EXISTS with correlated subquery
     */
    it('should support correlated NOT EXISTS subquery', () => {
      const result = db.prepare(`
        SELECT * FROM customers c
        WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
      `).all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 3, name: 'Charlie' });
    });

    /**
     * EXISTS with additional conditions
     */
    it('should support correlated EXISTS with additional conditions', () => {
      const result = db.prepare(`
        SELECT * FROM customers c
        WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 100)
      `).all();

      expect(result.length).toBe(2);
      const sorted = (result as { id: number; name: string }[]).sort((a, b) => a.id - b.id);
      expect(sorted).toEqual([
        { id: 1, name: 'Alice' },  // Has order of 200
        { id: 2, name: 'Bob' },    // Has order of 150
      ]);
    });
  });

  // ===========================================================================
  // MULTIPLE CORRELATED SUBQUERIES
  // ===========================================================================

  describe('Multiple Correlated Subqueries', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE products (id INTEGER, name TEXT, category_id INTEGER)');
      db.exec('CREATE TABLE sales (id INTEGER, product_id INTEGER, quantity INTEGER)');
      db.exec('CREATE TABLE reviews (id INTEGER, product_id INTEGER, rating INTEGER)');

      db.exec(`INSERT INTO products (id, name, category_id) VALUES (1, 'Widget', 1)`);
      db.exec(`INSERT INTO products (id, name, category_id) VALUES (2, 'Gadget', 1)`);
      db.exec(`INSERT INTO products (id, name, category_id) VALUES (3, 'Gizmo', 2)`);

      db.exec(`INSERT INTO sales (id, product_id, quantity) VALUES (1, 1, 10)`);
      db.exec(`INSERT INTO sales (id, product_id, quantity) VALUES (2, 1, 20)`);
      db.exec(`INSERT INTO sales (id, product_id, quantity) VALUES (3, 2, 5)`);

      db.exec(`INSERT INTO reviews (id, product_id, rating) VALUES (1, 1, 5)`);
      db.exec(`INSERT INTO reviews (id, product_id, rating) VALUES (2, 1, 4)`);
      db.exec(`INSERT INTO reviews (id, product_id, rating) VALUES (3, 2, 3)`);
    });

    /**
     * Multiple correlated subqueries in SELECT
     */
    it('should support multiple correlated subqueries in SELECT', () => {
      const result = db.prepare(`
        SELECT
          p.id,
          p.name,
          (SELECT sum(s.quantity) FROM sales s WHERE s.product_id = p.id) AS total_sales,
          (SELECT avg(r.rating) FROM reviews r WHERE r.product_id = p.id) AS avg_rating
        FROM products p
      `).all();

      expect(result.length).toBe(3);
      const sorted = (result as { id: number; name: string; total_sales: number | null; avg_rating: number | null }[])
        .sort((a, b) => a.id - b.id);

      expect(sorted[0].id).toBe(1);
      expect(sorted[0].total_sales).toBe(30);  // 10 + 20
      expect(sorted[0].avg_rating).toBe(4.5);  // (5 + 4) / 2

      expect(sorted[1].id).toBe(2);
      expect(sorted[1].total_sales).toBe(5);
      expect(sorted[1].avg_rating).toBe(3);

      expect(sorted[2].id).toBe(3);
      expect(sorted[2].total_sales).toBeNull();  // No sales
      expect(sorted[2].avg_rating).toBeNull();   // No reviews
    });
  });

  // ===========================================================================
  // CORRELATED SUBQUERY IN WHERE COMPARISON
  // ===========================================================================

  describe('Correlated Subquery in WHERE Comparison', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE employees (id INTEGER, name TEXT, salary INTEGER, dept_id INTEGER)');
      db.exec(`INSERT INTO employees (id, name, salary, dept_id) VALUES (1, 'Alice', 50000, 1)`);
      db.exec(`INSERT INTO employees (id, name, salary, dept_id) VALUES (2, 'Bob', 60000, 1)`);
      db.exec(`INSERT INTO employees (id, name, salary, dept_id) VALUES (3, 'Charlie', 55000, 1)`);
      db.exec(`INSERT INTO employees (id, name, salary, dept_id) VALUES (4, 'Diana', 70000, 2)`);
      db.exec(`INSERT INTO employees (id, name, salary, dept_id) VALUES (5, 'Eve', 80000, 2)`);
    });

    /**
     * Correlated subquery: employees earning above their department average
     */
    it('should find employees earning above department average', () => {
      const result = db.prepare(`
        SELECT e.name, e.salary
        FROM employees e
        WHERE e.salary > (
          SELECT avg(e2.salary)
          FROM employees e2
          WHERE e2.dept_id = e.dept_id
        )
      `).all();

      // Dept 1 avg: (50000 + 60000 + 55000) / 3 = 55000
      // Dept 2 avg: (70000 + 80000) / 2 = 75000
      // Above avg: Bob (60000 > 55000), Eve (80000 > 75000)
      expect(result.length).toBe(2);
      const names = (result as { name: string; salary: number }[]).map(r => r.name).sort();
      expect(names).toEqual(['Bob', 'Eve']);
    });
  });

  // ===========================================================================
  // EDGE CASES
  // ===========================================================================

  describe('Edge Cases', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (a INTEGER, b INTEGER)');
      db.exec('INSERT INTO t1 (a, b) VALUES (1, NULL)');
      db.exec('INSERT INTO t1 (a, b) VALUES (2, 10)');
      db.exec('INSERT INTO t1 (a, b) VALUES (3, 20)');
    });

    /**
     * Correlated subquery with NULL values
     */
    it('should handle NULL values in correlated subqueries', () => {
      const result = db.prepare(`
        SELECT a, (SELECT count(*) FROM t1 AS x WHERE x.b < t1.b) AS cnt
        FROM t1
      `).all();

      expect(result.length).toBe(3);
      const sorted = (result as { a: number; cnt: number }[]).sort((a, b) => a.a - b.a);

      // a=1, b=NULL: no valid comparisons (NULL < anything is false)
      // a=2, b=10: 0 rows have b < 10 (NULL doesn't count)
      // a=3, b=20: 1 row has b < 20 (b=10)
      expect(sorted[0].a).toBe(1);
      expect(sorted[0].cnt).toBe(0);
      expect(sorted[1].a).toBe(2);
      expect(sorted[1].cnt).toBe(0);
      expect(sorted[2].a).toBe(3);
      expect(sorted[2].cnt).toBe(1);
    });

    /**
     * Empty subquery result
     */
    it('should handle empty subquery results', () => {
      db.exec('CREATE TABLE t2 (x INTEGER)');
      // t2 is empty

      const result = db.prepare(`
        SELECT a, (SELECT count(*) FROM t2 WHERE t2.x = t1.a) AS cnt
        FROM t1
      `).all();

      expect(result.length).toBe(3);
      for (const row of result as { a: number; cnt: number }[]) {
        expect(row.cnt).toBe(0);
      }
    });
  });
});
