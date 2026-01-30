/**
 * Scalar Subquery Evaluation Tests for DoSQL
 *
 * TDD tests for fixing scalar subqueries in SELECT expressions.
 *
 * Issue: sql-jf6l - Fix scalar subqueries in SELECT expressions
 *
 * Problem: Scalar subqueries in CASE WHEN produce wrong results:
 * SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1
 *
 * Test cases:
 * 1. Scalar subquery returning single value: SELECT (SELECT 1)
 * 2. Scalar subquery with aggregate: SELECT (SELECT avg(x) FROM t1)
 * 3. Scalar subquery in CASE condition
 * 4. Multiple scalar subqueries in same SELECT
 * 5. Verify uncorrelated subquery is evaluated ONCE, not per row
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Database } from '../../database.js';

describe('Scalar Subquery Evaluation', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  describe('Basic scalar subqueries', () => {
    it('should evaluate scalar subquery returning a constant: SELECT (SELECT 1)', () => {
      // Create a simple table to have a FROM clause
      db.exec('CREATE TABLE dual (x INTEGER)');
      db.exec('INSERT INTO dual (x) VALUES (1)');

      const result = db.prepare('SELECT (SELECT 1) AS val FROM dual').all();

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({ val: 1 });
    });

    it('should evaluate scalar subquery with aggregate: SELECT (SELECT avg(x) FROM t1)', () => {
      db.exec('CREATE TABLE t1 (x INTEGER)');
      db.exec('INSERT INTO t1 (x) VALUES (10)');
      db.exec('INSERT INTO t1 (x) VALUES (20)');
      db.exec('INSERT INTO t1 (x) VALUES (30)');

      const result = db.prepare('SELECT (SELECT avg(x) FROM t1) AS avg_x FROM t1').all();

      // avg(x) = (10 + 20 + 30) / 3 = 20
      expect(result).toHaveLength(3);
      // All rows should have the same value since it's uncorrelated
      expect(result[0]).toHaveProperty('avg_x');
      expect(result[0].avg_x).toBe(20);
      expect(result[1].avg_x).toBe(20);
      expect(result[2].avg_x).toBe(20);
    });

    it('should return NULL for scalar subquery with no results', () => {
      db.exec('CREATE TABLE t1 (x INTEGER)');
      db.exec('CREATE TABLE t2 (y INTEGER)');
      db.exec('INSERT INTO t1 (x) VALUES (1)');
      // t2 is empty

      const result = db.prepare('SELECT (SELECT y FROM t2) AS val FROM t1').all();

      expect(result).toHaveLength(1);
      expect(result[0].val).toBeNull();
    });
  });

  describe('Scalar subquery in CASE conditions', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)');
      db.exec('INSERT INTO t1 (a, b, c) VALUES (1, 100, 10)');  // c < avg(c), so ELSE
      db.exec('INSERT INTO t1 (a, b, c) VALUES (2, 105, 20)');  // c < avg(c), so ELSE
      db.exec('INSERT INTO t1 (a, b, c) VALUES (3, 110, 30)');  // c = avg(c), so ELSE
      db.exec('INSERT INTO t1 (a, b, c) VALUES (4, 115, 40)');  // c > avg(c), so THEN
      db.exec('INSERT INTO t1 (a, b, c) VALUES (5, 120, 50)');  // c > avg(c), so THEN
      // avg(c) = (10 + 20 + 30 + 40 + 50) / 5 = 30
    });

    it('should evaluate scalar subquery in CASE WHEN condition', () => {
      // avg(c) = 30
      // Row 1: c=10 <= 30, so ELSE -> b*10 = 1000
      // Row 2: c=20 <= 30, so ELSE -> b*10 = 1050
      // Row 3: c=30 <= 30, so ELSE -> b*10 = 1100
      // Row 4: c=40 > 30, so THEN -> a*2 = 8
      // Row 5: c=50 > 30, so THEN -> a*2 = 10
      const result = db.prepare(`
        SELECT CASE WHEN c > (SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END AS result
        FROM t1
        ORDER BY a
      `).all();

      expect(result).toHaveLength(5);
      expect(result[0].result).toBe(1000); // c=10 <= 30, ELSE: 100*10
      expect(result[1].result).toBe(1050); // c=20 <= 30, ELSE: 105*10
      expect(result[2].result).toBe(1100); // c=30 <= 30, ELSE: 110*10
      expect(result[3].result).toBe(8);    // c=40 > 30, THEN: 4*2
      expect(result[4].result).toBe(10);   // c=50 > 30, THEN: 5*2
    });

    it('should use cached value for uncorrelated subquery in CASE', () => {
      // The subquery (SELECT avg(c) FROM t1) should be evaluated exactly ONCE
      // for all rows, since it's uncorrelated

      const result = db.prepare(`
        SELECT c,
               (SELECT avg(c) FROM t1) AS the_avg,
               CASE WHEN c > (SELECT avg(c) FROM t1) THEN 'above' ELSE 'not above' END AS status
        FROM t1
        ORDER BY c
      `).all();

      expect(result).toHaveLength(5);
      // All rows should have the same avg value
      const avgValue = result[0].the_avg;
      expect(avgValue).toBe(30);
      for (const row of result) {
        expect(row.the_avg).toBe(avgValue);
      }

      // Check the CASE results
      expect(result[0].status).toBe('not above'); // c=10
      expect(result[1].status).toBe('not above'); // c=20
      expect(result[2].status).toBe('not above'); // c=30 (not > 30)
      expect(result[3].status).toBe('above');     // c=40
      expect(result[4].status).toBe('above');     // c=50
    });
  });

  describe('Multiple scalar subqueries in same SELECT', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (x INTEGER)');
      db.exec('INSERT INTO t1 (x) VALUES (10)');
      db.exec('INSERT INTO t1 (x) VALUES (20)');
      db.exec('INSERT INTO t1 (x) VALUES (30)');
    });

    it('should evaluate multiple scalar subqueries independently', () => {
      const result = db.prepare(`
        SELECT
          (SELECT min(x) FROM t1) AS min_val,
          (SELECT max(x) FROM t1) AS max_val,
          (SELECT avg(x) FROM t1) AS avg_val,
          (SELECT sum(x) FROM t1) AS sum_val
        FROM t1
      `).all();

      expect(result).toHaveLength(3);
      for (const row of result) {
        expect(row.min_val).toBe(10);
        expect(row.max_val).toBe(30);
        expect(row.avg_val).toBe(20);
        expect(row.sum_val).toBe(60);
      }
    });

    it('should evaluate multiple subqueries in expressions', () => {
      const result = db.prepare(`
        SELECT
          x,
          x - (SELECT avg(x) FROM t1) AS diff_from_avg,
          CASE
            WHEN x < (SELECT avg(x) FROM t1) THEN 'below'
            WHEN x > (SELECT avg(x) FROM t1) THEN 'above'
            ELSE 'at average'
          END AS position
        FROM t1
        ORDER BY x
      `).all();

      expect(result).toHaveLength(3);

      // x=10: diff = 10 - 20 = -10, below
      expect(result[0].x).toBe(10);
      expect(result[0].diff_from_avg).toBe(-10);
      expect(result[0].position).toBe('below');

      // x=20: diff = 20 - 20 = 0, at average
      expect(result[1].x).toBe(20);
      expect(result[1].diff_from_avg).toBe(0);
      expect(result[1].position).toBe('at average');

      // x=30: diff = 30 - 20 = 10, above
      expect(result[2].x).toBe(30);
      expect(result[2].diff_from_avg).toBe(10);
      expect(result[2].position).toBe('above');
    });
  });

  describe('Correlated scalar subqueries', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (id INTEGER, a INTEGER, b INTEGER)');
      db.exec('INSERT INTO t1 (id, a, b) VALUES (1, 10, 100)');
      db.exec('INSERT INTO t1 (id, a, b) VALUES (2, 20, 200)');
      db.exec('INSERT INTO t1 (id, a, b) VALUES (3, 30, 300)');
    });

    it('should evaluate correlated scalar subquery for each row', () => {
      // Count how many rows have a smaller 'a' value
      const result = db.prepare(`
        SELECT t1.id,
               t1.a,
               (SELECT count(*) FROM t1 AS x WHERE x.a < t1.a) AS count_smaller
        FROM t1
        ORDER BY t1.id
      `).all();

      expect(result).toHaveLength(3);
      expect(result[0].count_smaller).toBe(0); // no rows have a < 10
      expect(result[1].count_smaller).toBe(1); // 1 row has a < 20
      expect(result[2].count_smaller).toBe(2); // 2 rows have a < 30
    });

    it('should evaluate correlated subquery in CASE condition', () => {
      const result = db.prepare(`
        SELECT t1.id,
               CASE
                 WHEN (SELECT count(*) FROM t1 AS x WHERE x.a < t1.a) > 0
                 THEN 'has smaller'
                 ELSE 'is smallest'
               END AS status
        FROM t1
        ORDER BY t1.id
      `).all();

      expect(result).toHaveLength(3);
      expect(result[0].status).toBe('is smallest');  // id=1, a=10
      expect(result[1].status).toBe('has smaller');  // id=2, a=20
      expect(result[2].status).toBe('has smaller');  // id=3, a=30
    });
  });

  describe('Scalar subquery caching behavior', () => {
    it('should evaluate uncorrelated subquery only once', () => {
      // We test this by checking that the same value is returned for all rows
      // and that the computation is correct

      db.exec('CREATE TABLE nums (n INTEGER)');
      for (let i = 1; i <= 10; i++) {
        db.exec(`INSERT INTO nums (n) VALUES (${i})`);
      }

      // The subquery computes sum(n) = 1+2+...+10 = 55
      // This should be evaluated ONCE and reused for all 10 rows
      const result = db.prepare(`
        SELECT n,
               (SELECT sum(n) FROM nums) AS total
        FROM nums
      `).all();

      expect(result).toHaveLength(10);
      // All rows should have the same total
      for (const row of result) {
        expect(row.total).toBe(55);
      }
    });
  });

  describe('Edge cases', () => {
    it('should handle scalar subquery returning NULL', () => {
      db.exec('CREATE TABLE t1 (x INTEGER)');
      db.exec('INSERT INTO t1 (x) VALUES (1)');
      db.exec('INSERT INTO t1 (x) VALUES (NULL)');

      // avg of (1, NULL) = 1 (NULL is ignored in aggregates)
      const result = db.prepare(`
        SELECT (SELECT avg(x) FROM t1) AS avg_x FROM t1
      `).all();

      expect(result).toHaveLength(2);
      expect(result[0].avg_x).toBe(1);
    });

    it('should handle nested scalar subqueries', () => {
      db.exec('CREATE TABLE t1 (x INTEGER)');
      db.exec('INSERT INTO t1 (x) VALUES (10)');
      db.exec('INSERT INTO t1 (x) VALUES (20)');
      db.exec('INSERT INTO t1 (x) VALUES (30)');

      // Nested: (SELECT (SELECT max(x) FROM t1) FROM dual)
      db.exec('CREATE TABLE dual (d INTEGER)');
      db.exec('INSERT INTO dual (d) VALUES (1)');

      const result = db.prepare(`
        SELECT (SELECT (SELECT max(x) FROM t1) FROM dual) AS nested_max
        FROM dual
      `).all();

      expect(result).toHaveLength(1);
      expect(result[0].nested_max).toBe(30);
    });

    it('should error when scalar subquery returns multiple rows', () => {
      db.exec('CREATE TABLE t1 (x INTEGER)');
      db.exec('INSERT INTO t1 (x) VALUES (1)');
      db.exec('INSERT INTO t1 (x) VALUES (2)');
      db.exec('CREATE TABLE dual (d INTEGER)');
      db.exec('INSERT INTO dual (d) VALUES (1)');

      // This should error because (SELECT x FROM t1) returns 2 rows
      expect(() => {
        db.prepare('SELECT (SELECT x FROM t1) AS val FROM dual').all();
      }).toThrow(/more than one row|single row/i);
    });
  });
});
