/**
 * SQLLogicTest Subquery Compatibility Tests (RED Phase TDD)
 *
 * These tests document expected subquery behavior that is currently NOT implemented
 * in the DoSQL InMemoryEngine. They represent the RED phase of TDD - all tests
 * are expected to fail until the subquery functionality is implemented.
 *
 * Subquery types covered:
 * - Scalar subqueries in SELECT clause
 * - Scalar subqueries in WHERE clause
 * - Correlated subqueries (referencing outer table)
 * - Subqueries with aggregate functions
 * - EXISTS subqueries
 * - NOT EXISTS subqueries
 *
 * SQLLogicTest failures that motivated these tests:
 * - SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b) FROM t1
 * - SELECT a, (SELECT max(b) FROM t1) FROM t1
 * - SELECT * FROM t1 WHERE a > (SELECT avg(a) FROM t1)
 * - SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1
 *
 * Run with: npx vitest run src/__tests__/sqllogictest-subqueries.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Database } from '../database.js';
import {
  InMemoryEngine,
  createInMemoryStorage,
  type InMemoryStorage,
} from '../statement/statement.js';

// =============================================================================
// TEST SETUP
// =============================================================================

describe('SQLLogicTest Subqueries', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  // ===========================================================================
  // SCALAR SUBQUERIES IN SELECT CLAUSE
  // ===========================================================================

  describe('Scalar Subqueries in SELECT Clause', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)
      `);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (1, 10, 100)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (2, 20, 200)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (3, 30, 300)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (4, 40, 400)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (5, 50, 500)`);
    });

    /**
     * Scalar subquery in SELECT clause - Testing implementation
     *
     * SQLLogicTest: SELECT a, (SELECT max(b) FROM t1) FROM t1
     * Expected: Each row should have the max(b) value (50) as the second column
     */
    it('should support scalar subquery returning max value in SELECT', () => {
      const result = db.prepare('SELECT a, (SELECT max(b) FROM t1) AS max_b FROM t1').all();

      expect(result.length).toBe(5);
      expect(result).toEqual([
        { a: 1, max_b: 50 },
        { a: 2, max_b: 50 },
        { a: 3, max_b: 50 },
        { a: 4, max_b: 50 },
        { a: 5, max_b: 50 },
      ]);
    });

    /**
     * KNOWN FAILURE: Scalar subquery with min() in SELECT
     */
    it('should support scalar subquery returning min value in SELECT', () => {
      const result = db.prepare('SELECT a, (SELECT min(c) FROM t1) AS min_c FROM t1').all();

      expect(result.length).toBe(5);
      // All rows should have min_c = 100 (the minimum value of c)
      for (const row of result as { a: number; min_c: number }[]) {
        expect(row.min_c).toBe(100);
      }
    });

    /**
     * KNOWN FAILURE: Scalar subquery with count() in SELECT
     */
    it('should support scalar subquery returning count in SELECT', () => {
      const result = db.prepare('SELECT a, (SELECT count(*) FROM t1) AS total FROM t1').all();

      expect(result.length).toBe(5);
      // All rows should have total = 5 (count of rows in t1)
      for (const row of result as { a: number; total: number }[]) {
        expect(row.total).toBe(5);
      }
    });

    /**
     * KNOWN FAILURE: Scalar subquery with avg() in SELECT
     */
    it('should support scalar subquery returning avg in SELECT', () => {
      const result = db.prepare('SELECT a, (SELECT avg(b) FROM t1) AS avg_b FROM t1').all();

      expect(result.length).toBe(5);
      // avg(b) = (10 + 20 + 30 + 40 + 50) / 5 = 30
      for (const row of result as { a: number; avg_b: number }[]) {
        expect(row.avg_b).toBe(30);
      }
    });

    /**
     * KNOWN FAILURE: Scalar subquery with sum() in SELECT
     */
    it('should support scalar subquery returning sum in SELECT', () => {
      const result = db.prepare('SELECT a, (SELECT sum(b) FROM t1) AS sum_b FROM t1').all();

      expect(result.length).toBe(5);
      // sum(b) = 10 + 20 + 30 + 40 + 50 = 150
      for (const row of result as { a: number; sum_b: number }[]) {
        expect(row.sum_b).toBe(150);
      }
    });

    /**
     * KNOWN FAILURE: Multiple scalar subqueries in SELECT
     */
    it('should support multiple scalar subqueries in SELECT', () => {
      const result = db.prepare(`
        SELECT
          a,
          (SELECT min(b) FROM t1) AS min_b,
          (SELECT max(b) FROM t1) AS max_b
        FROM t1
      `).all();

      expect(result.length).toBe(5);
      for (const row of result as { a: number; min_b: number; max_b: number }[]) {
        expect(row.min_b).toBe(10);
        expect(row.max_b).toBe(50);
      }
    });

    /**
     * KNOWN FAILURE: Scalar subquery from different table
     */
    it('should support scalar subquery from a different table', () => {
      db.exec('CREATE TABLE t2 (x INTEGER)');
      db.exec('INSERT INTO t2 (x) VALUES (999)');

      const result = db.prepare('SELECT a, (SELECT x FROM t2) AS x FROM t1').all();

      expect(result.length).toBe(5);
      for (const row of result as { a: number; x: number }[]) {
        expect(row.x).toBe(999);
      }
    });
  });

  // ===========================================================================
  // SCALAR SUBQUERIES IN WHERE CLAUSE
  // ===========================================================================

  describe('Scalar Subqueries in WHERE Clause', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)
      `);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (1, 10, 100)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (2, 20, 200)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (3, 30, 300)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (4, 40, 400)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (5, 50, 500)`);
    });

    /**
     * KNOWN FAILURE: Subquery with avg() in WHERE clause
     *
     * SQLLogicTest: SELECT * FROM t1 WHERE a > (SELECT avg(a) FROM t1)
     * Expected: Rows where a > 3 (avg of 1,2,3,4,5 = 3)
     */
    it('should support subquery with avg() in WHERE clause', () => {
      // avg(a) = (1 + 2 + 3 + 4 + 5) / 5 = 3
      // Rows with a > 3 are: a=4, a=5
      const result = db.prepare('SELECT * FROM t1 WHERE a > (SELECT avg(a) FROM t1)').all();

      expect(result.length).toBe(2);
      const sorted = (result as { a: number; b: number; c: number }[]).sort((x, y) => x.a - y.a);
      expect(sorted[0]).toEqual({ a: 4, b: 40, c: 400 });
      expect(sorted[1]).toEqual({ a: 5, b: 50, c: 500 });
    });

    /**
     * KNOWN FAILURE: Subquery with max() in WHERE clause
     */
    it('should support subquery with max() in WHERE clause', () => {
      const result = db.prepare('SELECT * FROM t1 WHERE b = (SELECT max(b) FROM t1)').all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 5, b: 50, c: 500 });
    });

    /**
     * KNOWN FAILURE: Subquery with min() in WHERE clause
     */
    it('should support subquery with min() in WHERE clause', () => {
      const result = db.prepare('SELECT * FROM t1 WHERE c = (SELECT min(c) FROM t1)').all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 1, b: 10, c: 100 });
    });

    /**
     * KNOWN FAILURE: Subquery comparison with >= in WHERE clause
     */
    it('should support subquery with >= comparison in WHERE', () => {
      // avg(b) = 30, so b >= 30 means rows with b = 30, 40, 50
      const result = db.prepare('SELECT * FROM t1 WHERE b >= (SELECT avg(b) FROM t1)').all();

      expect(result.length).toBe(3);
      const sorted = (result as { a: number; b: number; c: number }[]).sort((x, y) => x.a - y.a);
      expect(sorted.map(r => r.a)).toEqual([3, 4, 5]);
    });

    /**
     * KNOWN FAILURE: Subquery comparison with <= in WHERE clause
     */
    it('should support subquery with <= comparison in WHERE', () => {
      // avg(b) = 30, so b <= 30 means rows with b = 10, 20, 30
      const result = db.prepare('SELECT * FROM t1 WHERE b <= (SELECT avg(b) FROM t1)').all();

      expect(result.length).toBe(3);
      const sorted = (result as { a: number; b: number; c: number }[]).sort((x, y) => x.a - y.a);
      expect(sorted.map(r => r.a)).toEqual([1, 2, 3]);
    });

    /**
     * KNOWN FAILURE: Subquery with count() in WHERE clause
     */
    it('should support subquery with count() in WHERE clause', () => {
      // count(*) = 5, so a < 5 means rows with a = 1, 2, 3, 4
      const result = db.prepare('SELECT * FROM t1 WHERE a < (SELECT count(*) FROM t1)').all();

      expect(result.length).toBe(4);
      const sorted = (result as { a: number; b: number; c: number }[]).sort((x, y) => x.a - y.a);
      expect(sorted.map(r => r.a)).toEqual([1, 2, 3, 4]);
    });

    /**
     * KNOWN FAILURE: Subquery from different table in WHERE with compound conditions
     * TODO: Compound AND conditions with multiple subqueries not supported
     */
    it.fails('should support subquery from different table in WHERE', () => {
      db.exec('CREATE TABLE thresholds (min_val INTEGER, max_val INTEGER)');
      db.exec('INSERT INTO thresholds (min_val, max_val) VALUES (20, 40)');

      const result = db.prepare(`
        SELECT * FROM t1
        WHERE b >= (SELECT min_val FROM thresholds)
          AND b <= (SELECT max_val FROM thresholds)
      `).all();

      expect(result.length).toBe(3);
      const sorted = (result as { a: number; b: number; c: number }[]).sort((x, y) => x.a - y.a);
      expect(sorted.map(r => r.b)).toEqual([20, 30, 40]);
    });
  });

  // ===========================================================================
  // CORRELATED SUBQUERIES
  // ===========================================================================

  describe('Correlated Subqueries', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)
      `);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (1, 10, 100)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (2, 20, 200)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (3, 30, 300)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (4, 40, 400)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (5, 50, 500)`);
    });

    /**
     * Correlated subquery with count - FIXED in sql-aayr
     *
     * SQLLogicTest: SELECT (SELECT count(*) FROM t1 AS x WHERE x.b < t1.b) FROM t1
     * For each row, count how many rows have smaller b values
     */
    it('should support correlated subquery counting rows with smaller values', () => {
      const result = db.prepare(`
        SELECT a, (SELECT count(*) FROM t1 AS x WHERE x.b < t1.b) AS cnt FROM t1
      `).all();

      expect(result.length).toBe(5);
      const sorted = (result as { a: number; cnt: number }[]).sort((x, y) => x.a - y.a);

      // For a=1, b=10: 0 rows have b < 10
      // For a=2, b=20: 1 row has b < 20 (b=10)
      // For a=3, b=30: 2 rows have b < 30 (b=10, b=20)
      // For a=4, b=40: 3 rows have b < 40 (b=10, b=20, b=30)
      // For a=5, b=50: 4 rows have b < 50 (b=10, b=20, b=30, b=40)
      expect(sorted).toEqual([
        { a: 1, cnt: 0 },
        { a: 2, cnt: 1 },
        { a: 3, cnt: 2 },
        { a: 4, cnt: 3 },
        { a: 5, cnt: 4 },
      ]);
    });

    /**
     * Correlated subquery with max - FIXED in sql-aayr
     */
    it('should support correlated subquery with max of smaller values', () => {
      const result = db.prepare(`
        SELECT a, (SELECT max(x.b) FROM t1 AS x WHERE x.b < t1.b) AS max_smaller FROM t1
      `).all();

      expect(result.length).toBe(5);
      const sorted = (result as { a: number; max_smaller: number | null }[]).sort((x, y) => x.a - y.a);

      // For a=1, b=10: no rows have b < 10, so NULL
      // For a=2, b=20: max of b < 20 is 10
      // For a=3, b=30: max of b < 30 is 20
      // etc.
      expect(sorted).toEqual([
        { a: 1, max_smaller: null },
        { a: 2, max_smaller: 10 },
        { a: 3, max_smaller: 20 },
        { a: 4, max_smaller: 30 },
        { a: 5, max_smaller: 40 },
      ]);
    });

    /**
     * Correlated subquery in WHERE clause
     */
    it('should support correlated subquery in WHERE clause', () => {
      // Select rows where a is greater than the average a of rows with smaller b
      const result = db.prepare(`
        SELECT * FROM t1
        WHERE a > (SELECT avg(x.a) FROM t1 AS x WHERE x.b < t1.b)
      `).all();

      // For a=2, b=20: avg of a where b < 20 = avg(1) = 1. 2 > 1, so included.
      // For a=3, b=30: avg of a where b < 30 = avg(1,2) = 1.5. 3 > 1.5, so included.
      // For a=4, b=40: avg of a where b < 40 = avg(1,2,3) = 2. 4 > 2, so included.
      // For a=5, b=50: avg of a where b < 50 = avg(1,2,3,4) = 2.5. 5 > 2.5, so included.
      // a=1 has no rows with b < 10, so subquery returns NULL, comparison fails
      expect(result.length).toBe(4);
    });

    /**
     * Correlated subquery with sum - FIXED
     */
    it('should support correlated subquery with sum', () => {
      const result = db.prepare(`
        SELECT a, (SELECT sum(x.c) FROM t1 AS x WHERE x.a <= t1.a) AS running_sum FROM t1
      `).all();

      expect(result.length).toBe(5);
      const sorted = (result as { a: number; running_sum: number }[]).sort((x, y) => x.a - y.a);

      // Running sum of c values
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
  // SUBQUERIES WITH CASE EXPRESSIONS
  // ===========================================================================

  describe('Subqueries with CASE Expressions', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)
      `);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (1, 10, 100)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (2, 20, 200)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (3, 30, 300)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (4, 40, 400)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (5, 50, 500)`);
    });

    /**
     * Subquery in CASE WHEN condition - FIXED
     *
     * SQLLogicTest: SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1
     */
    it('should support subquery in CASE WHEN condition', () => {
      // avg(c) = (100 + 200 + 300 + 400 + 500) / 5 = 300
      // For c > 300: a * 2 (rows with c = 400, 500 -> 4*2=8, 5*2=10)
      // For c <= 300: b * 10 (rows with c = 100, 200, 300 -> 10*10=100, 20*10=200, 30*10=300)
      const result = db.prepare(`
        SELECT a, CASE WHEN c > (SELECT avg(c) FROM t1) THEN a * 2 ELSE b * 10 END AS result FROM t1
      `).all();

      expect(result.length).toBe(5);
      const sorted = (result as { a: number; result: number }[]).sort((x, y) => x.a - y.a);

      expect(sorted).toEqual([
        { a: 1, result: 100 },  // c=100 <= 300, so b*10 = 10*10 = 100
        { a: 2, result: 200 },  // c=200 <= 300, so b*10 = 20*10 = 200
        { a: 3, result: 300 },  // c=300 <= 300, so b*10 = 30*10 = 300
        { a: 4, result: 8 },    // c=400 > 300, so a*2 = 4*2 = 8
        { a: 5, result: 10 },   // c=500 > 300, so a*2 = 5*2 = 10
      ]);
    });

    /**
     * Subquery in CASE THEN branch - FIXED
     */
    it('should support subquery in CASE THEN branch', () => {
      const result = db.prepare(`
        SELECT a, CASE WHEN a > 3 THEN (SELECT max(b) FROM t1) ELSE b END AS result FROM t1
      `).all();

      expect(result.length).toBe(5);
      const sorted = (result as { a: number; result: number }[]).sort((x, y) => x.a - y.a);

      expect(sorted).toEqual([
        { a: 1, result: 10 },   // a <= 3, so b = 10
        { a: 2, result: 20 },   // a <= 3, so b = 20
        { a: 3, result: 30 },   // a <= 3, so b = 30
        { a: 4, result: 50 },   // a > 3, so max(b) = 50
        { a: 5, result: 50 },   // a > 3, so max(b) = 50
      ]);
    });

    /**
     * Subquery in CASE ELSE branch - FIXED
     */
    it('should support subquery in CASE ELSE branch', () => {
      const result = db.prepare(`
        SELECT a, CASE WHEN a < 3 THEN b ELSE (SELECT min(c) FROM t1) END AS result FROM t1
      `).all();

      expect(result.length).toBe(5);
      const sorted = (result as { a: number; result: number }[]).sort((x, y) => x.a - y.a);

      expect(sorted).toEqual([
        { a: 1, result: 10 },   // a < 3, so b = 10
        { a: 2, result: 20 },   // a < 3, so b = 20
        { a: 3, result: 100 },  // a >= 3, so min(c) = 100
        { a: 4, result: 100 },  // a >= 3, so min(c) = 100
        { a: 5, result: 100 },  // a >= 3, so min(c) = 100
      ]);
    });
  });

  // ===========================================================================
  // EXISTS SUBQUERIES
  // ===========================================================================

  describe('EXISTS Subqueries', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER)');
      db.exec('CREATE TABLE customers (id INTEGER, name TEXT)');

      db.exec(`INSERT INTO customers (id, name) VALUES (1, 'Alice')`);
      db.exec(`INSERT INTO customers (id, name) VALUES (2, 'Bob')`);
      db.exec(`INSERT INTO customers (id, name) VALUES (3, 'Charlie')`);

      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (1, 1, 100)`);
      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (2, 1, 200)`);
      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (3, 2, 150)`);
      // Note: Charlie (id=3) has no orders
    });

    /**
     * EXISTS with correlated subquery
     */
    it('should support EXISTS subquery', () => {
      // Select customers who have at least one order
      const result = db.prepare(`
        SELECT * FROM customers c
        WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
      `).all();

      expect(result.length).toBe(2);
      const sorted = (result as { id: number; name: string }[]).sort((x, y) => x.id - y.id);
      expect(sorted).toEqual([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]);
    });

    /**
     * EXISTS with correlated conditions
     */
    it('should support EXISTS with additional conditions in subquery', () => {
      // Select customers who have orders over 100
      const result = db.prepare(`
        SELECT * FROM customers c
        WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 100)
      `).all();

      expect(result.length).toBe(2);
      const sorted = (result as { id: number; name: string }[]).sort((x, y) => x.id - y.id);
      expect(sorted).toEqual([
        { id: 1, name: 'Alice' },  // Has order of 200
        { id: 2, name: 'Bob' },    // Has order of 150
      ]);
    });

    /**
     * KNOWN FAILURE: EXISTS with aggregate correlated condition
     * TODO: EXISTS with GROUP BY/HAVING in subquery not supported
     */
    it.fails('should support EXISTS with aggregate in subquery', () => {
      // Select customers who have more than 1 order
      const result = db.prepare(`
        SELECT * FROM customers c
        WHERE EXISTS (
          SELECT 1 FROM orders o
          WHERE o.customer_id = c.id
          GROUP BY o.customer_id
          HAVING count(*) > 1
        )
      `).all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 1, name: 'Alice' });  // Only Alice has 2 orders
    });
  });

  // ===========================================================================
  // NOT EXISTS SUBQUERIES
  // ===========================================================================

  describe('NOT EXISTS Subqueries', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER)');
      db.exec('CREATE TABLE customers (id INTEGER, name TEXT)');

      db.exec(`INSERT INTO customers (id, name) VALUES (1, 'Alice')`);
      db.exec(`INSERT INTO customers (id, name) VALUES (2, 'Bob')`);
      db.exec(`INSERT INTO customers (id, name) VALUES (3, 'Charlie')`);

      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (1, 1, 100)`);
      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (2, 1, 200)`);
      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (3, 2, 150)`);
      // Note: Charlie (id=3) has no orders
    });

    /**
     * NOT EXISTS with correlated subquery
     */
    it('should support NOT EXISTS subquery', () => {
      // Select customers who have NO orders
      const result = db.prepare(`
        SELECT * FROM customers c
        WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
      `).all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 3, name: 'Charlie' });
    });

    /**
     * NOT EXISTS with correlated condition
     */
    it('should support NOT EXISTS with additional conditions', () => {
      // Select customers who have no orders over 175
      const result = db.prepare(`
        SELECT * FROM customers c
        WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 175)
      `).all();

      expect(result.length).toBe(2);
      const sorted = (result as { id: number; name: string }[]).sort((x, y) => x.id - y.id);
      expect(sorted).toEqual([
        { id: 2, name: 'Bob' },      // Has order of 150 only
        { id: 3, name: 'Charlie' },  // Has no orders
      ]);
    });

    /**
     * NOT EXISTS for correlated anti-join pattern
     */
    it('should support NOT EXISTS for anti-join pattern', () => {
      db.exec('CREATE TABLE premium_customers (customer_id INTEGER)');
      db.exec('INSERT INTO premium_customers (customer_id) VALUES (1)');

      // Select non-premium customers
      const result = db.prepare(`
        SELECT * FROM customers c
        WHERE NOT EXISTS (SELECT 1 FROM premium_customers p WHERE p.customer_id = c.id)
      `).all();

      expect(result.length).toBe(2);
      const sorted = (result as { id: number; name: string }[]).sort((x, y) => x.id - y.id);
      expect(sorted).toEqual([
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
      ]);
    });
  });

  // ===========================================================================
  // IN SUBQUERIES
  // ===========================================================================

  describe('IN Subqueries', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER)');
      db.exec('CREATE TABLE customers (id INTEGER, name TEXT)');

      db.exec(`INSERT INTO customers (id, name) VALUES (1, 'Alice')`);
      db.exec(`INSERT INTO customers (id, name) VALUES (2, 'Bob')`);
      db.exec(`INSERT INTO customers (id, name) VALUES (3, 'Charlie')`);

      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (1, 1, 100)`);
      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (2, 1, 200)`);
      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (3, 2, 150)`);
    });

    /**
     * IN subquery support - IMPLEMENTED
     */
    it('should support IN subquery', () => {
      // Select customers who have at least one order
      const result = db.prepare(`
        SELECT * FROM customers
        WHERE id IN (SELECT customer_id FROM orders)
      `).all();

      expect(result.length).toBe(2);
      const sorted = (result as { id: number; name: string }[]).sort((x, y) => x.id - y.id);
      expect(sorted).toEqual([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]);
    });

    /**
     * NOT IN subquery support - IMPLEMENTED
     */
    it('should support NOT IN subquery', () => {
      // Select customers who have no orders
      const result = db.prepare(`
        SELECT * FROM customers
        WHERE id NOT IN (SELECT customer_id FROM orders)
      `).all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 3, name: 'Charlie' });
    });

    /**
     * IN subquery with condition - IMPLEMENTED
     */
    it('should support IN subquery with condition', () => {
      // Select customers who have orders over 100
      const result = db.prepare(`
        SELECT * FROM customers
        WHERE id IN (SELECT customer_id FROM orders WHERE amount > 100)
      `).all();

      expect(result.length).toBe(2);
      const sorted = (result as { id: number; name: string }[]).sort((x, y) => x.id - y.id);
      expect(sorted).toEqual([
        { id: 1, name: 'Alice' },  // Has order of 200
        { id: 2, name: 'Bob' },    // Has order of 150
      ]);
    });
  });

  // ===========================================================================
  // SUBQUERIES IN ARITHMETIC EXPRESSIONS
  // ===========================================================================

  describe('Subqueries in Arithmetic Expressions', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (a INTEGER, b INTEGER)');
      db.exec('INSERT INTO t1 (a, b) VALUES (10, 100)');
      db.exec('INSERT INTO t1 (a, b) VALUES (20, 200)');
      db.exec('INSERT INTO t1 (a, b) VALUES (30, 300)');
      db.exec('INSERT INTO t1 (a, b) VALUES (40, 400)');
      db.exec('INSERT INTO t1 (a, b) VALUES (50, 500)');
    });

    /**
     * Subquery in arithmetic expression - FIXED
     */
    it('should support subquery in arithmetic expression', () => {
      // Calculate each a as a percentage of the total
      // sum(a) = 150, so percentage = a * 100 / 150
      const result = db.prepare(`
        SELECT a, a * 100 / (SELECT sum(a) FROM t1) AS percentage FROM t1
      `).all();

      expect(result.length).toBe(5);
      const sorted = (result as { a: number; percentage: number }[]).sort((x, y) => x.a - y.a);

      // Integer division in SQL
      expect(sorted).toEqual([
        { a: 10, percentage: 6 },   // 10 * 100 / 150 = 6 (integer)
        { a: 20, percentage: 13 },  // 20 * 100 / 150 = 13 (integer)
        { a: 30, percentage: 20 },  // 30 * 100 / 150 = 20
        { a: 40, percentage: 26 },  // 40 * 100 / 150 = 26 (integer)
        { a: 50, percentage: 33 },  // 50 * 100 / 150 = 33 (integer)
      ]);
    });

    /**
     * Subquery with subtraction - FIXED
     */
    it('should support subquery in subtraction expression', () => {
      // Calculate deviation from average
      // avg(a) = 30
      const result = db.prepare(`
        SELECT a, a - (SELECT avg(a) FROM t1) AS deviation FROM t1
      `).all();

      expect(result.length).toBe(5);
      const sorted = (result as { a: number; deviation: number }[]).sort((x, y) => x.a - y.a);

      expect(sorted).toEqual([
        { a: 10, deviation: -20 },
        { a: 20, deviation: -10 },
        { a: 30, deviation: 0 },
        { a: 40, deviation: 10 },
        { a: 50, deviation: 20 },
      ]);
    });
  });

  // ===========================================================================
  // NESTED SUBQUERIES
  // ===========================================================================

  describe('Nested Subqueries', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (a INTEGER, b INTEGER)');
      db.exec('INSERT INTO t1 (a, b) VALUES (1, 10)');
      db.exec('INSERT INTO t1 (a, b) VALUES (2, 20)');
      db.exec('INSERT INTO t1 (a, b) VALUES (3, 30)');
      db.exec('INSERT INTO t1 (a, b) VALUES (4, 40)');
      db.exec('INSERT INTO t1 (a, b) VALUES (5, 50)');
    });

    /**
     * KNOWN FAILURE: Nested subqueries
     */
    it('should support nested subqueries', () => {
      // Select rows where a is greater than the count of rows where b < avg(b)
      // avg(b) = 30, rows with b < 30: 2 (b=10, b=20)
      // So select where a > 2: a=3, a=4, a=5
      const result = db.prepare(`
        SELECT * FROM t1
        WHERE a > (
          SELECT count(*) FROM t1
          WHERE b < (SELECT avg(b) FROM t1)
        )
      `).all();

      expect(result.length).toBe(3);
      const sorted = (result as { a: number; b: number }[]).sort((x, y) => x.a - y.a);
      expect(sorted.map(r => r.a)).toEqual([3, 4, 5]);
    });

    /**
     * KNOWN FAILURE: Deeply nested subqueries
     */
    it('should support deeply nested subqueries', () => {
      const result = db.prepare(`
        SELECT a FROM t1
        WHERE a = (
          SELECT max(a) FROM t1
          WHERE a < (
            SELECT max(a) FROM t1
            WHERE a < (
              SELECT max(a) FROM t1
            )
          )
        )
      `).all();

      // max(a) = 5
      // max(a) where a < 5 = 4
      // max(a) where a < 4 = 3
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 3 });
    });
  });
});

// =============================================================================
// LOW-LEVEL INMEMORYENGINE SUBQUERY TESTS
// =============================================================================

describe('InMemoryEngine Direct Subquery Tests', () => {
  let engine: InMemoryEngine;
  let storage: InMemoryStorage;

  beforeEach(() => {
    storage = createInMemoryStorage();
    engine = new InMemoryEngine(storage);

    // Set up test tables
    engine.execute('CREATE TABLE t1 (a INTEGER, b INTEGER)', []);
    engine.execute('INSERT INTO t1 (a, b) VALUES (?, ?)', [1, 10]);
    engine.execute('INSERT INTO t1 (a, b) VALUES (?, ?)', [2, 20]);
    engine.execute('INSERT INTO t1 (a, b) VALUES (?, ?)', [3, 30]);
  });

  /**
   * KNOWN FAILURE: Engine should parse scalar subquery
   */
  it('should parse and execute scalar subquery in SELECT', () => {
    const result = engine.execute('SELECT a, (SELECT max(b) FROM t1) AS max_b FROM t1', []);

    expect(result.rows.length).toBe(3);
    expect(result.rows[0].max_b).toBe(30);
  });

  /**
   * KNOWN FAILURE: Engine should parse subquery in WHERE
   */
  it('should parse and execute subquery in WHERE clause', () => {
    const result = engine.execute('SELECT * FROM t1 WHERE a > (SELECT avg(a) FROM t1)', []);

    // avg(a) = 2, so a > 2 means a = 3
    expect(result.rows.length).toBe(1);
    expect(result.rows[0].a).toBe(3);
  });

  /**
   * Engine should parse correlated EXISTS subquery
   */
  it('should parse and execute EXISTS subquery', () => {
    engine.execute('CREATE TABLE t2 (x INTEGER)', []);
    engine.execute('INSERT INTO t2 (x) VALUES (?)', [1]);

    const result = engine.execute(
      'SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.x = t1.a)',
      []
    );

    expect(result.rows.length).toBe(1);
    expect(result.rows[0].a).toBe(1);
  });

  /**
   * IN subquery parsing - IMPLEMENTED
   */
  it('should parse and execute IN subquery', () => {
    engine.execute('CREATE TABLE t2 (x INTEGER)', []);
    engine.execute('INSERT INTO t2 (x) VALUES (?)', [1]);
    engine.execute('INSERT INTO t2 (x) VALUES (?)', [3]);

    const result = engine.execute('SELECT * FROM t1 WHERE a IN (SELECT x FROM t2)', []);

    expect(result.rows.length).toBe(2);
    const sorted = result.rows.sort((x: { a: number }, y: { a: number }) => x.a - y.a);
    expect(sorted[0].a).toBe(1);
    expect(sorted[1].a).toBe(3);
  });
});
