/**
 * SQLLogicTest Extended Coverage
 *
 * Expands SQLLogicTest compatibility with tests for common SQL patterns
 * not yet covered in the existing test files:
 *
 * 1. GROUP BY with HAVING
 * 2. Subqueries (additional IN, EXISTS, scalar patterns)
 * 3. UNION / INTERSECT / EXCEPT set operations
 * 4. CASE WHEN expressions (additional edge cases)
 * 5. Date/time functions
 *
 * Run with: npx vitest run src/__tests__/sqllogictest-extended.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Database } from '../database.js';
import {
  InMemoryEngine,
  createInMemoryStorage,
  type InMemoryStorage,
} from '../statement/statement.js';

// =============================================================================
// GROUP BY WITH HAVING
// =============================================================================

describe('SQLLogicTest: GROUP BY with HAVING', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  describe('Basic HAVING clause', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE sales (id INTEGER, product TEXT, amount INTEGER, region TEXT)');
      db.exec("INSERT INTO sales (id, product, amount, region) VALUES (1, 'Widget', 100, 'North')");
      db.exec("INSERT INTO sales (id, product, amount, region) VALUES (2, 'Widget', 200, 'South')");
      db.exec("INSERT INTO sales (id, product, amount, region) VALUES (3, 'Widget', 150, 'North')");
      db.exec("INSERT INTO sales (id, product, amount, region) VALUES (4, 'Gadget', 300, 'South')");
      db.exec("INSERT INTO sales (id, product, amount, region) VALUES (5, 'Gadget', 50, 'North')");
      db.exec("INSERT INTO sales (id, product, amount, region) VALUES (6, 'Doohickey', 75, 'South')");
    });

    /**
     * SQLLogicTest: SELECT product, SUM(amount) FROM sales GROUP BY product HAVING SUM(amount) > 200
     * Expected: Groups where total exceeds 200
     */
    it('should filter groups using HAVING with SUM', () => {
      const result = db.prepare(
        'SELECT product, SUM(amount) AS total FROM sales GROUP BY product HAVING SUM(amount) > 200'
      ).all();

      const sorted = (result as { product: string; total: number }[])
        .sort((a, b) => a.product.localeCompare(b.product));

      // Gadget: 300+50=350 > 200
      // Widget: 100+200+150=450 > 200
      // Doohickey: 75 < 200, excluded
      expect(sorted.length).toBe(2);
      expect(sorted[0]).toEqual({ product: 'Gadget', total: 350 });
      expect(sorted[1]).toEqual({ product: 'Widget', total: 450 });
    });

    /**
     * SQLLogicTest: SELECT product, COUNT(*) FROM sales GROUP BY product HAVING COUNT(*) > 1
     * Expected: Groups with more than 1 row
     */
    it('should filter groups using HAVING with COUNT', () => {
      const result = db.prepare(
        'SELECT product, COUNT(*) AS cnt FROM sales GROUP BY product HAVING COUNT(*) > 1'
      ).all();

      const sorted = (result as { product: string; cnt: number }[])
        .sort((a, b) => a.product.localeCompare(b.product));

      // Gadget: 2 rows
      // Widget: 3 rows
      // Doohickey: 1 row, excluded
      expect(sorted.length).toBe(2);
      expect(sorted[0]).toEqual({ product: 'Gadget', cnt: 2 });
      expect(sorted[1]).toEqual({ product: 'Widget', cnt: 3 });
    });

    /**
     * SQLLogicTest: SELECT product, AVG(amount) FROM sales GROUP BY product HAVING AVG(amount) >= 100
     * Expected: Groups with average >= 100
     */
    it('should filter groups using HAVING with AVG', () => {
      const result = db.prepare(
        'SELECT product, AVG(amount) AS avg_amount FROM sales GROUP BY product HAVING AVG(amount) >= 100'
      ).all();

      const sorted = (result as { product: string; avg_amount: number }[])
        .sort((a, b) => a.product.localeCompare(b.product));

      // Gadget: avg = 175
      // Widget: avg = 150
      // Doohickey: avg = 75, excluded
      expect(sorted.length).toBe(2);
      expect(sorted[0]).toEqual({ product: 'Gadget', avg_amount: 175 });
      expect(sorted[1]).toEqual({ product: 'Widget', avg_amount: 150 });
    });

    /**
     * SQLLogicTest: SELECT region, MAX(amount) FROM sales GROUP BY region HAVING MAX(amount) > 200
     * Expected: Regions where max amount exceeds 200
     */
    it('should filter groups using HAVING with MAX', () => {
      const result = db.prepare(
        'SELECT region, MAX(amount) AS max_amount FROM sales GROUP BY region HAVING MAX(amount) > 200'
      ).all();

      // North max: 150 (excluded)
      // South max: 300
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ region: 'South', max_amount: 300 });
    });

    /**
     * SQLLogicTest: SELECT region, MIN(amount) FROM sales GROUP BY region HAVING MIN(amount) < 100
     * Expected: Regions where min amount is below 100
     */
    it('should filter groups using HAVING with MIN', () => {
      const result = db.prepare(
        'SELECT region, MIN(amount) AS min_amount FROM sales GROUP BY region HAVING MIN(amount) < 100'
      ).all();

      const sorted = (result as { region: string; min_amount: number }[])
        .sort((a, b) => a.region.localeCompare(b.region));

      // North min: 50
      // South min: 75
      expect(sorted.length).toBe(2);
      expect(sorted[0]).toEqual({ region: 'North', min_amount: 50 });
      expect(sorted[1]).toEqual({ region: 'South', min_amount: 75 });
    });
  });

  describe('HAVING with compound conditions', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE orders (id INTEGER, customer TEXT, total INTEGER)');
      db.exec("INSERT INTO orders (id, customer, total) VALUES (1, 'Alice', 100)");
      db.exec("INSERT INTO orders (id, customer, total) VALUES (2, 'Alice', 200)");
      db.exec("INSERT INTO orders (id, customer, total) VALUES (3, 'Alice', 50)");
      db.exec("INSERT INTO orders (id, customer, total) VALUES (4, 'Bob', 300)");
      db.exec("INSERT INTO orders (id, customer, total) VALUES (5, 'Bob', 400)");
      db.exec("INSERT INTO orders (id, customer, total) VALUES (6, 'Charlie', 150)");
    });

    /**
     * KNOWN FAILURE: Compound HAVING with AND not supported
     *
     * Bug: The HAVING clause parser does not support compound conditions
     * with AND/OR. Only a single aggregate condition is supported.
     *
     * SQLLogicTest: SELECT customer, SUM(total), COUNT(*) FROM orders
     *   GROUP BY customer HAVING SUM(total) > 200 AND COUNT(*) > 1
     * Expected: customers with total > 200 AND more than 1 order
     * Actual: Returns 0 rows (AND not parsed in HAVING)
     */
    it('should support compound HAVING with AND', () => {
      const result = db.prepare(
        'SELECT customer, SUM(total) AS sum_total, COUNT(*) AS cnt FROM orders GROUP BY customer HAVING SUM(total) > 200 AND COUNT(*) > 1'
      ).all();

      const sorted = (result as { customer: string; sum_total: number; cnt: number }[])
        .sort((a, b) => a.customer.localeCompare(b.customer));

      // Alice: sum=350, cnt=3 -> matches
      // Bob: sum=700, cnt=2 -> matches
      // Charlie: sum=150, cnt=1 -> excluded (both conditions fail)
      expect(sorted.length).toBe(2);
      expect(sorted[0]).toEqual({ customer: 'Alice', sum_total: 350, cnt: 3 });
      expect(sorted[1]).toEqual({ customer: 'Bob', sum_total: 700, cnt: 2 });
    });

    /**
     * SQLLogicTest: SELECT customer, COUNT(*) FROM orders
     *   GROUP BY customer HAVING COUNT(*) >= 2
     *   ORDER BY customer
     */
    it('should support HAVING with ORDER BY', () => {
      const result = db.prepare(
        'SELECT customer, COUNT(*) AS cnt FROM orders GROUP BY customer HAVING COUNT(*) >= 2 ORDER BY customer'
      ).all();

      expect(result.length).toBe(2);
      expect(result[0]).toEqual({ customer: 'Alice', cnt: 3 });
      expect(result[1]).toEqual({ customer: 'Bob', cnt: 2 });
    });
  });

  describe('HAVING edge cases', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE data (id INTEGER, grp TEXT, val INTEGER)');
      db.exec("INSERT INTO data (id, grp, val) VALUES (1, 'A', 10)");
      db.exec("INSERT INTO data (id, grp, val) VALUES (2, 'A', NULL)");
      db.exec("INSERT INTO data (id, grp, val) VALUES (3, 'B', 20)");
      db.exec("INSERT INTO data (id, grp, val) VALUES (4, 'B', 30)");
      db.exec("INSERT INTO data (id, grp, val) VALUES (5, 'C', NULL)");
    });

    /**
     * HAVING with NULL values in aggregates
     * COUNT(val) excludes NULLs, COUNT(*) includes all rows
     */
    it('should handle NULL values in HAVING conditions', () => {
      const result = db.prepare(
        'SELECT grp, COUNT(val) AS non_null_cnt FROM data GROUP BY grp HAVING COUNT(val) > 0'
      ).all();

      const sorted = (result as { grp: string; non_null_cnt: number }[])
        .sort((a, b) => a.grp.localeCompare(b.grp));

      // A: COUNT(val)=1 (NULL excluded)
      // B: COUNT(val)=2
      // C: COUNT(val)=0, excluded
      expect(sorted.length).toBe(2);
      expect(sorted[0]).toEqual({ grp: 'A', non_null_cnt: 1 });
      expect(sorted[1]).toEqual({ grp: 'B', non_null_cnt: 2 });
    });

    /**
     * HAVING with no matching groups should return empty result
     */
    it('should return empty result when no groups match HAVING', () => {
      const result = db.prepare(
        'SELECT grp, SUM(val) AS total FROM data GROUP BY grp HAVING SUM(val) > 1000'
      ).all();

      expect(result.length).toBe(0);
    });
  });
});

// =============================================================================
// UNION / INTERSECT / EXCEPT SET OPERATIONS
// =============================================================================

describe('SQLLogicTest: UNION / INTERSECT / EXCEPT', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  describe('UNION', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (id INTEGER, name TEXT)');
      db.exec('CREATE TABLE t2 (id INTEGER, name TEXT)');

      db.exec("INSERT INTO t1 (id, name) VALUES (1, 'Alice')");
      db.exec("INSERT INTO t1 (id, name) VALUES (2, 'Bob')");
      db.exec("INSERT INTO t1 (id, name) VALUES (3, 'Charlie')");

      db.exec("INSERT INTO t2 (id, name) VALUES (2, 'Bob')");
      db.exec("INSERT INTO t2 (id, name) VALUES (3, 'Charlie')");
      db.exec("INSERT INTO t2 (id, name) VALUES (4, 'Diana')");
      db.exec("INSERT INTO t2 (id, name) VALUES (5, 'Eve')");
    });

    /**
     * KNOWN FAILURE: UNION not supported in parser
     *
     * Bug: The InMemoryEngine parser does not recognize UNION syntax.
     * It treats "t1 UNION SELECT id" as a table name, resulting in
     * a "table not found" error.
     *
     * SQLLogicTest: SELECT id, name FROM t1 UNION SELECT id, name FROM t2
     * Expected: All distinct rows from both tables (5 rows)
     * Actual: StatementError: Table "t1 UNION SELECT id" does not exist
     */
    it('should combine results removing duplicates', () => {
      const result = db.prepare(
        'SELECT id, name FROM t1 UNION SELECT id, name FROM t2'
      ).all();

      const sorted = (result as { id: number; name: string }[])
        .sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(5);
      expect(sorted[0]).toEqual({ id: 1, name: 'Alice' });
      expect(sorted[1]).toEqual({ id: 2, name: 'Bob' });
      expect(sorted[2]).toEqual({ id: 3, name: 'Charlie' });
      expect(sorted[3]).toEqual({ id: 4, name: 'Diana' });
      expect(sorted[4]).toEqual({ id: 5, name: 'Eve' });
    });

    /**
     * KNOWN FAILURE: UNION ALL not supported in parser
     *
     * Bug: Same parser limitation as UNION.
     *
     * SQLLogicTest: SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2
     * Expected: All rows from both tables including duplicates (7 rows)
     * Actual: StatementError: Table "t1 UNION ALL SELECT id" does not exist
     */
    it('should combine results keeping duplicates with UNION ALL', () => {
      const result = db.prepare(
        'SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2'
      ).all();

      // t1 has 3 rows, t2 has 4 rows = 7 total
      expect(result.length).toBe(7);
    });

    /**
     * KNOWN FAILURE: UNION with single column not supported in parser
     *
     * SQLLogicTest: SELECT id FROM t1 UNION SELECT id FROM t2
     * Expected: Distinct ids from both tables (5 rows)
     */
    it('should handle UNION with single column', () => {
      const result = db.prepare(
        'SELECT id FROM t1 UNION SELECT id FROM t2'
      ).all();

      const sorted = (result as { id: number }[]).sort((a, b) => a.id - b.id);
      expect(sorted.length).toBe(5);
      expect(sorted.map(r => r.id)).toEqual([1, 2, 3, 4, 5]);
    });

    /**
     * KNOWN FAILURE: UNION with WHERE clauses not supported in parser
     *
     * Bug: The parser consumes the WHERE clause of the first SELECT
     * but fails to parse UNION. Returns empty result instead of error.
     *
     * SQLLogicTest: SELECT ... WHERE ... UNION SELECT ... WHERE ...
     * Expected: 4 rows
     * Actual: 0 rows (UNION not parsed)
     */
    it('should handle UNION with WHERE clauses', () => {
      const result = db.prepare(
        "SELECT id, name FROM t1 WHERE id <= 2 UNION SELECT id, name FROM t2 WHERE id >= 4"
      ).all();

      const sorted = (result as { id: number; name: string }[])
        .sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(4);
      expect(sorted.map(r => r.id)).toEqual([1, 2, 4, 5]);
    });
  });

  describe('INTERSECT', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (id INTEGER, name TEXT)');
      db.exec('CREATE TABLE t2 (id INTEGER, name TEXT)');

      db.exec("INSERT INTO t1 (id, name) VALUES (1, 'Alice')");
      db.exec("INSERT INTO t1 (id, name) VALUES (2, 'Bob')");
      db.exec("INSERT INTO t1 (id, name) VALUES (3, 'Charlie')");

      db.exec("INSERT INTO t2 (id, name) VALUES (2, 'Bob')");
      db.exec("INSERT INTO t2 (id, name) VALUES (3, 'Charlie')");
      db.exec("INSERT INTO t2 (id, name) VALUES (4, 'Diana')");
    });

    /**
     * KNOWN FAILURE: INTERSECT not supported in parser
     *
     * Bug: Same parser limitation as UNION. The InMemoryEngine parser
     * does not recognize INTERSECT syntax.
     *
     * SQLLogicTest: SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t2
     * Expected: Rows common to both tables (2 rows: Bob, Charlie)
     * Actual: StatementError: Table not found
     */
    it('should return rows common to both queries', () => {
      const result = db.prepare(
        'SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t2'
      ).all();

      const sorted = (result as { id: number; name: string }[])
        .sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(2);
      expect(sorted[0]).toEqual({ id: 2, name: 'Bob' });
      expect(sorted[1]).toEqual({ id: 3, name: 'Charlie' });
    });

    /**
     * KNOWN FAILURE: INTERSECT with no common rows not supported
     */
    it('should return empty when no common rows exist', () => {
      const result = db.prepare(
        "SELECT id, name FROM t1 WHERE id = 1 INTERSECT SELECT id, name FROM t2 WHERE id = 4"
      ).all();

      expect(result.length).toBe(0);
    });
  });

  describe('EXCEPT', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (id INTEGER, name TEXT)');
      db.exec('CREATE TABLE t2 (id INTEGER, name TEXT)');

      db.exec("INSERT INTO t1 (id, name) VALUES (1, 'Alice')");
      db.exec("INSERT INTO t1 (id, name) VALUES (2, 'Bob')");
      db.exec("INSERT INTO t1 (id, name) VALUES (3, 'Charlie')");

      db.exec("INSERT INTO t2 (id, name) VALUES (2, 'Bob')");
      db.exec("INSERT INTO t2 (id, name) VALUES (3, 'Charlie')");
      db.exec("INSERT INTO t2 (id, name) VALUES (4, 'Diana')");
    });

    /**
     * KNOWN FAILURE: EXCEPT not supported in parser
     *
     * Bug: Same parser limitation as UNION/INTERSECT.
     *
     * SQLLogicTest: SELECT id, name FROM t1 EXCEPT SELECT id, name FROM t2
     * Expected: Rows in t1 but not in t2 (1 row: Alice)
     * Actual: StatementError: Table not found
     */
    it('should return rows in left but not in right', () => {
      const result = db.prepare(
        'SELECT id, name FROM t1 EXCEPT SELECT id, name FROM t2'
      ).all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 1, name: 'Alice' });
    });

    /**
     * KNOWN FAILURE: EXCEPT in reverse direction not supported
     */
    it('should return rows in right but not in left (reversed)', () => {
      const result = db.prepare(
        'SELECT id, name FROM t2 EXCEPT SELECT id, name FROM t1'
      ).all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 4, name: 'Diana' });
    });

    /**
     * KNOWN FAILURE: EXCEPT where all rows are in both not supported
     */
    it('should return empty when left is subset of right', () => {
      const result = db.prepare(
        "SELECT id, name FROM t1 WHERE id IN (2, 3) EXCEPT SELECT id, name FROM t2"
      ).all();

      expect(result.length).toBe(0);
    });
  });

  describe('Chained set operations', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE a (x INTEGER)');
      db.exec('CREATE TABLE b (x INTEGER)');
      db.exec('CREATE TABLE c (x INTEGER)');

      db.exec('INSERT INTO a (x) VALUES (1)');
      db.exec('INSERT INTO a (x) VALUES (2)');
      db.exec('INSERT INTO a (x) VALUES (3)');

      db.exec('INSERT INTO b (x) VALUES (2)');
      db.exec('INSERT INTO b (x) VALUES (3)');
      db.exec('INSERT INTO b (x) VALUES (4)');

      db.exec('INSERT INTO c (x) VALUES (3)');
      db.exec('INSERT INTO c (x) VALUES (4)');
      db.exec('INSERT INTO c (x) VALUES (5)');
    });

    /**
     * KNOWN FAILURE: Chained UNION of three tables not supported
     *
     * Bug: Parser does not recognize UNION keyword.
     *
     * SQLLogicTest: SELECT x FROM a UNION SELECT x FROM b UNION SELECT x FROM c
     * Expected: {1, 2, 3, 4, 5}
     */
    it('should handle UNION of three tables', () => {
      const result = db.prepare(
        'SELECT x FROM a UNION SELECT x FROM b UNION SELECT x FROM c'
      ).all();

      const sorted = (result as { x: number }[]).sort((a, b) => a.x - b.x);
      expect(sorted.length).toBe(5);
      expect(sorted.map(r => r.x)).toEqual([1, 2, 3, 4, 5]);
    });

    /**
     * KNOWN FAILURE: INTERSECT precedence over UNION not supported
     *
     * Bug: Parser does not recognize set operation keywords.
     *
     * SQLLogicTest: INTERSECT binds tighter than UNION
     * a UNION b INTERSECT c = a UNION (b INTERSECT c) = a UNION {3,4} = {1,2,3,4}
     */
    it('should respect INTERSECT precedence over UNION', () => {
      const result = db.prepare(
        'SELECT x FROM a UNION SELECT x FROM b INTERSECT SELECT x FROM c'
      ).all();

      const sorted = (result as { x: number }[]).sort((a, b) => a.x - b.x);
      expect(sorted.length).toBe(4);
      expect(sorted.map(r => r.x)).toEqual([1, 2, 3, 4]);
    });
  });
});

// =============================================================================
// ADDITIONAL SUBQUERY PATTERNS
// =============================================================================

describe('SQLLogicTest: Additional Subquery Patterns', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  describe('IN with literal list', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (id INTEGER, name TEXT, category TEXT)');
      db.exec("INSERT INTO t1 (id, name, category) VALUES (1, 'Apple', 'fruit')");
      db.exec("INSERT INTO t1 (id, name, category) VALUES (2, 'Carrot', 'vegetable')");
      db.exec("INSERT INTO t1 (id, name, category) VALUES (3, 'Banana', 'fruit')");
      db.exec("INSERT INTO t1 (id, name, category) VALUES (4, 'Potato', 'vegetable')");
      db.exec("INSERT INTO t1 (id, name, category) VALUES (5, 'Salmon', 'fish')");
    });

    /**
     * SQLLogicTest: SELECT * FROM t1 WHERE id IN (1, 3, 5)
     */
    it('should support IN with integer literal list', () => {
      const result = db.prepare(
        'SELECT id, name FROM t1 WHERE id IN (1, 3, 5) ORDER BY id'
      ).all();

      expect(result.length).toBe(3);
      expect((result as { id: number; name: string }[]).map(r => r.id)).toEqual([1, 3, 5]);
    });

    /**
     * SQLLogicTest: SELECT * FROM t1 WHERE category IN ('fruit', 'fish')
     */
    it('should support IN with string literal list', () => {
      const result = db.prepare(
        "SELECT id, name FROM t1 WHERE category IN ('fruit', 'fish') ORDER BY id"
      ).all();

      expect(result.length).toBe(3);
      expect((result as { id: number; name: string }[]).map(r => r.id)).toEqual([1, 3, 5]);
    });

    /**
     * SQLLogicTest: SELECT * FROM t1 WHERE id NOT IN (1, 3, 5)
     */
    it('should support NOT IN with literal list', () => {
      const result = db.prepare(
        'SELECT id, name FROM t1 WHERE id NOT IN (1, 3, 5) ORDER BY id'
      ).all();

      expect(result.length).toBe(2);
      expect((result as { id: number; name: string }[]).map(r => r.id)).toEqual([2, 4]);
    });
  });

  describe('Scalar subquery in expression', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE items (id INTEGER, price INTEGER)');
      db.exec('INSERT INTO items (id, price) VALUES (1, 10)');
      db.exec('INSERT INTO items (id, price) VALUES (2, 20)');
      db.exec('INSERT INTO items (id, price) VALUES (3, 30)');
      db.exec('INSERT INTO items (id, price) VALUES (4, 40)');
    });

    /**
     * SQLLogicTest: Compare with average via subquery + CASE WHEN
     */
    it('should classify values relative to average using subquery', () => {
      const result = db.prepare(`
        SELECT id, price,
          CASE WHEN price > (SELECT AVG(price) FROM items)
            THEN 'above'
            ELSE 'at_or_below'
          END AS classification
        FROM items
        ORDER BY id
      `).all();

      expect(result.length).toBe(4);
      // avg(price) = 25
      const rows = result as { id: number; price: number; classification: string }[];
      expect(rows[0].classification).toBe('at_or_below'); // 10
      expect(rows[1].classification).toBe('at_or_below'); // 20
      expect(rows[2].classification).toBe('above');       // 30
      expect(rows[3].classification).toBe('above');       // 40
    });
  });

  describe('Multi-table subqueries', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE departments (id INTEGER, name TEXT)');
      db.exec('CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER, salary INTEGER)');

      db.exec("INSERT INTO departments (id, name) VALUES (1, 'Engineering')");
      db.exec("INSERT INTO departments (id, name) VALUES (2, 'Marketing')");
      db.exec("INSERT INTO departments (id, name) VALUES (3, 'HR')");

      db.exec("INSERT INTO employees (id, name, dept_id, salary) VALUES (1, 'Alice', 1, 90000)");
      db.exec("INSERT INTO employees (id, name, dept_id, salary) VALUES (2, 'Bob', 1, 85000)");
      db.exec("INSERT INTO employees (id, name, dept_id, salary) VALUES (3, 'Charlie', 2, 70000)");
      db.exec("INSERT INTO employees (id, name, dept_id, salary) VALUES (4, 'Diana', 2, 75000)");
      // No employees in HR (dept 3)
    });

    /**
     * SQLLogicTest: Find departments with no employees using NOT IN
     */
    it('should find departments with no employees using NOT IN', () => {
      const result = db.prepare(`
        SELECT id, name FROM departments
        WHERE id NOT IN (SELECT DISTINCT dept_id FROM employees)
        ORDER BY id
      `).all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 3, name: 'HR' });
    });

    /**
     * SQLLogicTest: Find departments with no employees using NOT EXISTS
     */
    it('should find departments with no employees using NOT EXISTS', () => {
      const result = db.prepare(`
        SELECT d.id, d.name FROM departments d
        WHERE NOT EXISTS (
          SELECT 1 FROM employees e WHERE e.dept_id = d.id
        )
        ORDER BY d.id
      `).all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 3, name: 'HR' });
    });

    /**
     * SQLLogicTest: Find highest paid employee per department
     * Uses correlated subquery with MAX
     */
    it('should find employees with max salary in their department', () => {
      const result = db.prepare(`
        SELECT e.name, e.salary FROM employees e
        WHERE e.salary = (
          SELECT MAX(e2.salary) FROM employees e2 WHERE e2.dept_id = e.dept_id
        )
        ORDER BY e.dept_id
      `).all();

      expect(result.length).toBe(2);
      const rows = result as { name: string; salary: number }[];
      expect(rows[0]).toEqual({ name: 'Alice', salary: 90000 });
      expect(rows[1]).toEqual({ name: 'Diana', salary: 75000 });
    });
  });
});

// =============================================================================
// CASE WHEN ADDITIONAL PATTERNS
// =============================================================================

describe('SQLLogicTest: Additional CASE WHEN Patterns', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  describe('CASE in aggregate context', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE events (id INTEGER, type TEXT, value INTEGER)');
      db.exec("INSERT INTO events (id, type, value) VALUES (1, 'sale', 100)");
      db.exec("INSERT INTO events (id, type, value) VALUES (2, 'return', 30)");
      db.exec("INSERT INTO events (id, type, value) VALUES (3, 'sale', 200)");
      db.exec("INSERT INTO events (id, type, value) VALUES (4, 'return', 50)");
      db.exec("INSERT INTO events (id, type, value) VALUES (5, 'sale', 150)");
    });

    /**
     * SQLLogicTest: Conditional SUM using CASE WHEN
     */
    it('should compute conditional sums using CASE WHEN', () => {
      const result = db.prepare(`
        SELECT
          SUM(CASE WHEN type = 'sale' THEN value ELSE 0 END) AS total_sales,
          SUM(CASE WHEN type = 'return' THEN value ELSE 0 END) AS total_returns
        FROM events
      `).all();

      expect(result.length).toBe(1);
      const row = result[0] as { total_sales: number; total_returns: number };
      expect(row.total_sales).toBe(450);   // 100 + 200 + 150
      expect(row.total_returns).toBe(80);   // 30 + 50
    });

    /**
     * SQLLogicTest: Conditional COUNT using CASE WHEN
     */
    it('should compute conditional counts using CASE WHEN', () => {
      const result = db.prepare(`
        SELECT
          COUNT(CASE WHEN type = 'sale' THEN 1 END) AS sale_count,
          COUNT(CASE WHEN type = 'return' THEN 1 END) AS return_count,
          COUNT(*) AS total_count
        FROM events
      `).all();

      expect(result.length).toBe(1);
      const row = result[0] as { sale_count: number; return_count: number; total_count: number };
      expect(row.sale_count).toBe(3);
      expect(row.return_count).toBe(2);
      expect(row.total_count).toBe(5);
    });
  });

  describe('CASE with boolean logic', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE users (id INTEGER, age INTEGER, active INTEGER)');
      db.exec('INSERT INTO users (id, age, active) VALUES (1, 25, 1)');
      db.exec('INSERT INTO users (id, age, active) VALUES (2, 17, 1)');
      db.exec('INSERT INTO users (id, age, active) VALUES (3, 30, 0)');
      db.exec('INSERT INTO users (id, age, active) VALUES (4, 16, 0)');
    });

    /**
     * KNOWN FAILURE: CASE with AND in WHEN condition
     *
     * Bug: The CASE WHEN evaluation does not properly handle compound
     * boolean expressions with AND. It appears to evaluate only the first
     * condition and ignores the second, returning the first WHEN branch
     * for all rows.
     *
     * SQLLogicTest: CASE WHEN age >= 18 AND active = 1 THEN ...
     * Expected: Different status per row based on both conditions
     * Actual: All rows return 'active_adult' (first branch always matches)
     */
    it('should handle AND in WHEN condition', () => {
      const result = db.prepare(`
        SELECT id,
          CASE WHEN age >= 18 AND active = 1 THEN 'active_adult'
               WHEN age >= 18 AND active = 0 THEN 'inactive_adult'
               WHEN age < 18 AND active = 1 THEN 'active_minor'
               ELSE 'inactive_minor'
          END AS status
        FROM users
        ORDER BY id
      `).all();

      expect(result.length).toBe(4);
      const rows = result as { id: number; status: string }[];
      expect(rows[0].status).toBe('active_adult');    // 25, active
      expect(rows[1].status).toBe('active_minor');    // 17, active
      expect(rows[2].status).toBe('inactive_adult');  // 30, not active
      expect(rows[3].status).toBe('inactive_minor');  // 16, not active
    });

    /**
     * KNOWN FAILURE: CASE with OR in WHEN condition
     *
     * Bug: The CASE WHEN evaluation does not properly handle compound
     * boolean expressions with OR. Similar to AND, the second operand
     * of the OR is not evaluated correctly.
     *
     * SQLLogicTest: CASE WHEN age < 18 OR active = 0 THEN 'restricted' ELSE 'full_access'
     * Expected: Only id=1 gets 'full_access'; rest get 'restricted'
     * Actual: All rows return 'full_access' or incorrect results
     */
    it('should handle OR in WHEN condition', () => {
      const result = db.prepare(`
        SELECT id,
          CASE WHEN age < 18 OR active = 0 THEN 'restricted' ELSE 'full_access' END AS access
        FROM users
        ORDER BY id
      `).all();

      expect(result.length).toBe(4);
      const rows = result as { id: number; access: string }[];
      expect(rows[0].access).toBe('full_access'); // 25, active
      expect(rows[1].access).toBe('restricted');  // 17 (minor)
      expect(rows[2].access).toBe('restricted');  // 30, inactive
      expect(rows[3].access).toBe('restricted');  // 16, inactive
    });
  });

  describe('CASE returning computed values', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE products (id INTEGER, price INTEGER, category TEXT)');
      db.exec("INSERT INTO products (id, price, category) VALUES (1, 100, 'electronics')");
      db.exec("INSERT INTO products (id, price, category) VALUES (2, 50, 'clothing')");
      db.exec("INSERT INTO products (id, price, category) VALUES (3, 200, 'electronics')");
      db.exec("INSERT INTO products (id, price, category) VALUES (4, 30, 'food')");
    });

    /**
     * SQLLogicTest: CASE returning different computed expressions per branch
     */
    it('should return computed values from CASE branches', () => {
      const result = db.prepare(`
        SELECT id, price,
          CASE category
            WHEN 'electronics' THEN price * 110 / 100
            WHEN 'clothing' THEN price * 120 / 100
            ELSE price
          END AS taxed_price
        FROM products
        ORDER BY id
      `).all();

      expect(result.length).toBe(4);
      const rows = result as { id: number; price: number; taxed_price: number }[];
      expect(rows[0].taxed_price).toBe(110);  // 100 * 110 / 100 = 110
      expect(rows[1].taxed_price).toBe(60);   // 50 * 120 / 100 = 60
      expect(rows[2].taxed_price).toBe(220);  // 200 * 110 / 100 = 220
      expect(rows[3].taxed_price).toBe(30);   // 30 (ELSE)
    });
  });
});

// =============================================================================
// DATE/TIME FUNCTIONS
// =============================================================================

describe('SQLLogicTest: Date/Time Functions', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  describe('Date literals and comparisons', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE events (id INTEGER, name TEXT, event_date TEXT)');
      db.exec("INSERT INTO events (id, name, event_date) VALUES (1, 'Launch', '2024-01-15')");
      db.exec("INSERT INTO events (id, name, event_date) VALUES (2, 'Update', '2024-03-20')");
      db.exec("INSERT INTO events (id, name, event_date) VALUES (3, 'Release', '2024-06-10')");
      db.exec("INSERT INTO events (id, name, event_date) VALUES (4, 'Patch', '2024-09-05')");
      db.exec("INSERT INTO events (id, name, event_date) VALUES (5, 'EOL', '2025-01-01')");
    });

    /**
     * SQLLogicTest: String-based date comparison (ISO 8601 format)
     * Since dates are stored as TEXT in ISO format, lexicographic comparison works
     */
    it('should compare date strings lexicographically', () => {
      const result = db.prepare(
        "SELECT id, name FROM events WHERE event_date >= '2024-06-01' ORDER BY id"
      ).all();

      expect(result.length).toBe(3);
      expect((result as { id: number; name: string }[]).map(r => r.id)).toEqual([3, 4, 5]);
    });

    /**
     * SQLLogicTest: Date range query using BETWEEN-like pattern
     */
    it('should support date range queries', () => {
      const result = db.prepare(
        "SELECT id, name FROM events WHERE event_date >= '2024-03-01' AND event_date <= '2024-09-30' ORDER BY id"
      ).all();

      expect(result.length).toBe(3);
      expect((result as { id: number; name: string }[]).map(r => r.id)).toEqual([2, 3, 4]);
    });

    /**
     * SQLLogicTest: Ordering by date column
     */
    it('should order by date column correctly', () => {
      const result = db.prepare(
        'SELECT id, event_date FROM events ORDER BY event_date DESC'
      ).all();

      expect(result.length).toBe(5);
      const rows = result as { id: number; event_date: string }[];
      expect(rows[0].event_date).toBe('2025-01-01');
      expect(rows[4].event_date).toBe('2024-01-15');
    });
  });

  describe('Date extraction via string functions', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE logs (id INTEGER, timestamp TEXT)');
      db.exec("INSERT INTO logs (id, timestamp) VALUES (1, '2024-01-15 10:30:00')");
      db.exec("INSERT INTO logs (id, timestamp) VALUES (2, '2024-01-15 14:45:00')");
      db.exec("INSERT INTO logs (id, timestamp) VALUES (3, '2024-02-20 09:15:00')");
      db.exec("INSERT INTO logs (id, timestamp) VALUES (4, '2024-02-20 16:00:00')");
    });

    /**
     * KNOWN FAILURE: GROUP BY with SUBSTR expression
     *
     * Bug: GROUP BY with expression (SUBSTR) does not properly group rows.
     * The engine groups by the raw expression string instead of the computed
     * value, resulting in incorrect group counts.
     *
     * SQLLogicTest: SELECT SUBSTR(timestamp, 1, 7) AS month, COUNT(*) FROM logs
     *   GROUP BY SUBSTR(timestamp, 1, 7)
     * Expected: 2 groups (2024-01: 2, 2024-02: 2)
     * Actual: 1 group or incorrect grouping
     */
    it('should extract date parts using SUBSTR for grouping', () => {
      const result = db.prepare(`
        SELECT SUBSTR(timestamp, 1, 7) AS month, COUNT(*) AS cnt
        FROM logs
        GROUP BY SUBSTR(timestamp, 1, 7)
        ORDER BY month
      `).all();

      expect(result.length).toBe(2);
      const rows = result as { month: string; cnt: number }[];
      expect(rows[0]).toEqual({ month: '2024-01', cnt: 2 });
      expect(rows[1]).toEqual({ month: '2024-02', cnt: 2 });
    });

    /**
     * SQLLogicTest: Filter by date prefix
     */
    it('should filter by date prefix using LIKE', () => {
      const result = db.prepare(
        "SELECT id FROM logs WHERE timestamp LIKE '2024-01%' ORDER BY id"
      ).all();

      expect(result.length).toBe(2);
      expect((result as { id: number }[]).map(r => r.id)).toEqual([1, 2]);
    });
  });

  describe('Timestamp comparisons', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE tasks (id INTEGER, title TEXT, created_at TEXT, due_at TEXT)');
      db.exec("INSERT INTO tasks (id, title, created_at, due_at) VALUES (1, 'Task A', '2024-01-01', '2024-02-01')");
      db.exec("INSERT INTO tasks (id, title, created_at, due_at) VALUES (2, 'Task B', '2024-01-15', '2024-01-20')");
      db.exec("INSERT INTO tasks (id, title, created_at, due_at) VALUES (3, 'Task C', '2024-02-01', '2024-03-01')");
    });

    /**
     * SQLLogicTest: Compare two date columns
     */
    it('should compare two date text columns', () => {
      const result = db.prepare(
        "SELECT id, title FROM tasks WHERE due_at > '2024-02-01' ORDER BY id"
      ).all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 3, title: 'Task C' });
    });

    /**
     * SQLLogicTest: Order by date column
     */
    it('should support ordering by date column', () => {
      const result = db.prepare(
        'SELECT id, title FROM tasks ORDER BY due_at ASC'
      ).all();

      expect(result.length).toBe(3);
      const rows = result as { id: number; title: string }[];
      expect(rows[0].title).toBe('Task B');  // due 2024-01-20
      expect(rows[1].title).toBe('Task A');  // due 2024-02-01
      expect(rows[2].title).toBe('Task C');  // due 2024-03-01
    });
  });

  describe('Date with GROUP BY and HAVING', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE transactions (id INTEGER, date TEXT, amount INTEGER)');
      db.exec("INSERT INTO transactions (id, date, amount) VALUES (1, '2024-01-05', 100)");
      db.exec("INSERT INTO transactions (id, date, amount) VALUES (2, '2024-01-15', 200)");
      db.exec("INSERT INTO transactions (id, date, amount) VALUES (3, '2024-01-25', 50)");
      db.exec("INSERT INTO transactions (id, date, amount) VALUES (4, '2024-02-10', 300)");
      db.exec("INSERT INTO transactions (id, date, amount) VALUES (5, '2024-02-20', 100)");
      db.exec("INSERT INTO transactions (id, date, amount) VALUES (6, '2024-03-01', 75)");
    });

    /**
     * KNOWN FAILURE: GROUP BY with SUBSTR expression and HAVING
     *
     * Bug: GROUP BY with computed expression (SUBSTR) does not properly
     * group rows. Combined with HAVING, this produces incorrect results.
     *
     * SQLLogicTest: GROUP BY SUBSTR(date, 1, 7) HAVING SUM(amount) >= 350
     * Expected: Jan (350) and Feb (400) match; Mar (75) excluded
     * Actual: Incorrect grouping due to SUBSTR limitation in GROUP BY
     */
    it('should group by month and filter with HAVING', () => {
      const result = db.prepare(`
        SELECT SUBSTR(date, 1, 7) AS month, SUM(amount) AS total
        FROM transactions
        GROUP BY SUBSTR(date, 1, 7)
        HAVING SUM(amount) >= 350
      `).all();

      const sorted = (result as { month: string; total: number }[])
        .sort((a, b) => a.month.localeCompare(b.month));

      // Jan: 100+200+50=350 >= 350
      // Feb: 300+100=400 >= 350
      // Mar: 75 < 350, excluded
      expect(sorted.length).toBe(2);
      expect(sorted[0]).toEqual({ month: '2024-01', total: 350 });
      expect(sorted[1]).toEqual({ month: '2024-02', total: 400 });
    });
  });
});

// =============================================================================
// LOW-LEVEL INMEMORYENGINE TESTS FOR NEW PATTERNS
// =============================================================================

describe('InMemoryEngine Direct Tests for Extended Coverage', () => {
  let engine: InMemoryEngine;
  let storage: InMemoryStorage;

  beforeEach(() => {
    storage = createInMemoryStorage();
    engine = new InMemoryEngine(storage);
  });

  describe('GROUP BY with HAVING at engine level', () => {
    beforeEach(() => {
      engine.execute('CREATE TABLE sales (product TEXT, amount INTEGER)', []);
      engine.execute('INSERT INTO sales (product, amount) VALUES (?, ?)', ['A', 100]);
      engine.execute('INSERT INTO sales (product, amount) VALUES (?, ?)', ['A', 200]);
      engine.execute('INSERT INTO sales (product, amount) VALUES (?, ?)', ['B', 50]);
      engine.execute('INSERT INTO sales (product, amount) VALUES (?, ?)', ['B', 75]);
      engine.execute('INSERT INTO sales (product, amount) VALUES (?, ?)', ['C', 300]);
    });

    it('should execute GROUP BY with HAVING SUM filter', () => {
      const result = engine.execute(
        'SELECT product, SUM(amount) AS total FROM sales GROUP BY product HAVING SUM(amount) > 100',
        []
      );

      const sorted = result.rows.sort(
        (a: { product: string }, b: { product: string }) => a.product.localeCompare(b.product)
      );

      // A: 300 > 100 yes, B: 125 > 100 yes, C: 300 > 100 yes
      expect(sorted.length).toBe(3);
      expect(sorted[0].total).toBe(300); // A
      expect(sorted[1].total).toBe(125); // B
      expect(sorted[2].total).toBe(300); // C
    });

    it('should execute GROUP BY with HAVING COUNT filter', () => {
      const result = engine.execute(
        'SELECT product, COUNT(*) AS cnt FROM sales GROUP BY product HAVING COUNT(*) > 1',
        []
      );

      const sorted = result.rows.sort(
        (a: { product: string }, b: { product: string }) => a.product.localeCompare(b.product)
      );

      // A: 2 > 1 yes, B: 2 > 1 yes, C: 1 > 1 no
      expect(sorted.length).toBe(2);
      expect(sorted[0].product).toBe('A');
      expect(sorted[0].cnt).toBe(2);
      expect(sorted[1].product).toBe('B');
      expect(sorted[1].cnt).toBe(2);
    });
  });

  describe('UNION at engine level', () => {
    beforeEach(() => {
      engine.execute('CREATE TABLE t1 (x INTEGER)', []);
      engine.execute('CREATE TABLE t2 (x INTEGER)', []);
      engine.execute('INSERT INTO t1 (x) VALUES (?)', [1]);
      engine.execute('INSERT INTO t1 (x) VALUES (?)', [2]);
      engine.execute('INSERT INTO t2 (x) VALUES (?)', [2]);
      engine.execute('INSERT INTO t2 (x) VALUES (?)', [3]);
    });

    /**
     * KNOWN FAILURE: UNION not supported in InMemoryEngine parser
     *
     * Bug: The InMemoryEngine parser does not recognize UNION syntax.
     * Set operations (UNION, INTERSECT, EXCEPT) have execution operators
     * implemented in engine/operators/set-ops.ts, but the InMemoryEngine
     * parser does not produce AST nodes for them.
     *
     * SQLLogicTest: SELECT x FROM t1 UNION SELECT x FROM t2
     * Expected: {1, 2, 3}
     * Actual: StatementError: Table "t1 UNION SELECT x FROM t2" does not exist
     */
    it('should execute UNION removing duplicates', () => {
      const result = engine.execute('SELECT x FROM t1 UNION SELECT x FROM t2', []);

      const sorted = result.rows.sort(
        (a: { x: number }, b: { x: number }) => a.x - b.x
      );
      expect(sorted.length).toBe(3);
      expect(sorted.map((r: { x: number }) => r.x)).toEqual([1, 2, 3]);
    });

    /**
     * KNOWN FAILURE: UNION ALL not supported in InMemoryEngine parser
     */
    it('should execute UNION ALL keeping duplicates', () => {
      const result = engine.execute('SELECT x FROM t1 UNION ALL SELECT x FROM t2', []);
      expect(result.rows.length).toBe(4);
    });
  });

  describe('CASE with LIKE in condition', () => {
    beforeEach(() => {
      engine.execute('CREATE TABLE files (name TEXT)', []);
      engine.execute('INSERT INTO files (name) VALUES (?)', ['report.pdf']);
      engine.execute('INSERT INTO files (name) VALUES (?)', ['image.png']);
      engine.execute('INSERT INTO files (name) VALUES (?)', ['data.csv']);
      engine.execute('INSERT INTO files (name) VALUES (?)', ['notes.txt']);
    });

    /**
     * KNOWN FAILURE: LIKE not supported inside CASE WHEN conditions
     *
     * Bug: The CASE WHEN evaluator does not properly handle LIKE
     * operators in conditions. The LIKE pattern matching returns
     * true for all rows (always matching the first branch).
     *
     * SQLLogicTest: CASE WHEN name LIKE '%.pdf' THEN 'document' ...
     * Expected: Each file classified by extension
     * Actual: All rows return 'document' (first LIKE branch always matches)
     */
    it('should handle LIKE inside CASE WHEN condition', () => {
      const result = engine.execute(`
        SELECT name,
          CASE
            WHEN name LIKE '%.pdf' THEN 'document'
            WHEN name LIKE '%.png' THEN 'image'
            WHEN name LIKE '%.csv' THEN 'data'
            ELSE 'other'
          END AS file_type
        FROM files
        ORDER BY name
      `, []);

      expect(result.rows.length).toBe(4);
      const sorted = result.rows.sort(
        (a: { name: string }, b: { name: string }) => a.name.localeCompare(b.name)
      );
      expect(sorted[0].file_type).toBe('data');     // data.csv
      expect(sorted[1].file_type).toBe('image');     // image.png
      expect(sorted[2].file_type).toBe('other');     // notes.txt
      expect(sorted[3].file_type).toBe('document');  // report.pdf
    });
  });
});
