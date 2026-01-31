/**
 * Complex Correlated Subquery Tests
 *
 * Tests for correlated subqueries with complex WHERE clauses matching SQLLogicTest patterns.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Database } from '../database.js';

describe('Complex Correlated Subqueries', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
    // Create table matching SQLLogicTest select1.test (all 30 rows)
    db.exec('CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)');
    db.exec('INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)');
    db.exec('INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105)');
    db.exec('INSERT INTO t1(e,d,b,a,c) VALUES(110,114,112,111,113)');
    db.exec('INSERT INTO t1(d,c,e,a,b) VALUES(116,119,117,115,118)');
    db.exec('INSERT INTO t1(c,d,b,e,a) VALUES(123,122,124,120,121)');
    db.exec('INSERT INTO t1(a,d,b,e,c) VALUES(127,128,129,126,125)');
    db.exec('INSERT INTO t1(e,c,a,d,b) VALUES(132,134,131,133,130)');
    db.exec('INSERT INTO t1(a,d,b,e,c) VALUES(138,136,139,135,137)');
    db.exec('INSERT INTO t1(e,c,d,a,b) VALUES(144,141,140,142,143)');
    db.exec('INSERT INTO t1(b,a,e,d,c) VALUES(145,149,146,148,147)');
    db.exec('INSERT INTO t1(b,c,a,d,e) VALUES(151,150,153,154,152)');
    db.exec('INSERT INTO t1(c,e,a,d,b) VALUES(155,157,159,156,158)');
    db.exec('INSERT INTO t1(c,b,a,d,e) VALUES(161,160,163,164,162)');
    db.exec('INSERT INTO t1(b,d,a,e,c) VALUES(167,169,168,165,166)');
    db.exec('INSERT INTO t1(d,b,c,e,a) VALUES(171,170,172,173,174)');
    db.exec('INSERT INTO t1(e,c,a,d,b) VALUES(177,176,179,178,175)');
    db.exec('INSERT INTO t1(b,e,a,d,c) VALUES(181,180,182,183,184)');
    db.exec('INSERT INTO t1(c,a,b,e,d) VALUES(187,188,186,189,185)');
    db.exec('INSERT INTO t1(d,b,c,e,a) VALUES(190,194,193,192,191)');
    db.exec('INSERT INTO t1(a,e,b,d,c) VALUES(199,197,198,196,195)');
    db.exec('INSERT INTO t1(b,c,d,a,e) VALUES(200,202,203,201,204)');
    db.exec('INSERT INTO t1(c,e,a,b,d) VALUES(208,209,205,206,207)');
    db.exec('INSERT INTO t1(c,e,a,d,b) VALUES(214,210,213,212,211)');
    db.exec('INSERT INTO t1(b,c,a,d,e) VALUES(218,215,216,217,219)');
    db.exec('INSERT INTO t1(b,e,d,a,c) VALUES(223,221,222,220,224)');
    db.exec('INSERT INTO t1(d,e,b,a,c) VALUES(226,227,228,229,225)');
    db.exec('INSERT INTO t1(a,c,b,e,d) VALUES(234,231,232,230,233)');
    db.exec('INSERT INTO t1(e,b,a,c,d) VALUES(237,236,239,235,238)');
    db.exec('INSERT INTO t1(e,c,b,a,d) VALUES(242,244,240,243,241)');
    db.exec('INSERT INTO t1(e,d,c,b,a) VALUES(246,248,247,249,245)');
  });

  describe('WHERE clause evaluation', () => {
    it('should correctly parse complex WHERE clause with OR', () => {
      // Simple OR condition
      const result = db.prepare('SELECT a, b, c, d FROM t1 WHERE a > 110 OR c > 118').all();
      console.log('a > 110 OR c > 118:', result);
      expect(result.length).toBeGreaterThan(0);
    });

    it('should correctly parse complex WHERE clause with AND in parens followed by OR', () => {
      // Test: (a>b-2 AND a<b+2) OR c>d
      // First let's understand the data
      const all = db.prepare('SELECT a, b, c, d, (a>b-2 AND a<b+2) AS cond1, (c>d) AS cond2 FROM t1').all();
      console.log('All rows with conditions:', all);

      // Now the actual filter
      const result = db.prepare('SELECT a, b, c, d FROM t1 WHERE (a>b-2 AND a<b+2) OR c>d').all();
      console.log('WHERE (a>b-2 AND a<b+2) OR c>d:', result);
      expect(result.length).toBeGreaterThan(0);
    });

    it('should correctly handle c>d condition', () => {
      const result = db.prepare('SELECT a, b, c, d FROM t1 WHERE c > d').all();
      console.log('WHERE c > d:', result);
      expect(result.length).toBeGreaterThan(0);
    });

    it('should correctly handle a>b-2 AND a<b+2 condition', () => {
      const result = db.prepare('SELECT a, b, c, d FROM t1 WHERE a>b-2 AND a<b+2').all();
      console.log('WHERE a>b-2 AND a<b+2:', result);
      // This might be 0 rows depending on data, but let's check
    });
  });

  describe('Correlated subquery in SELECT with complex WHERE', () => {
    it('should return correct count for correlated subquery with OR in outer WHERE', () => {
      // Test the exact SQLLogicTest case that fails
      const result = db.prepare(`
        SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b) AS cnt
        FROM t1
        WHERE (a>b-2 AND a<b+2) OR c>d
        ORDER BY 1
      `).all();

      console.log('Correlated subquery result:', result);
      // Should not be empty
      expect(result.length).toBeGreaterThan(0);
    });

    it('should return correct count for correlated subquery with simple c>d WHERE', () => {
      const result = db.prepare(`
        SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b) AS cnt
        FROM t1
        WHERE c>d
        ORDER BY 1
      `).all();

      console.log('Correlated subquery with c>d:', result);
      expect(result.length).toBeGreaterThan(0);
    });
  });

  describe('Correlated subquery with two conditions (x.c>t1.c AND x.d<t1.d)', () => {
    it('should correctly count rows matching complex subquery condition', () => {
      const result = db.prepare(`
        SELECT a,
               (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d) AS cnt
        FROM t1
      `).all();

      console.log('Correlated with x.c>t1.c AND x.d<t1.d:', result);
      expect(result.length).toBe(30);
    });

    it('should return correct count values for compound WHERE in correlated subquery', () => {
      // Test with a smaller dataset for verifiable results
      const testDb = new Database(':memory:');
      testDb.exec('CREATE TABLE t1(a INTEGER, c INTEGER, d INTEGER)');
      // c=10, d=5 : rows with c>10 AND d<5 = 0
      testDb.exec('INSERT INTO t1 VALUES(1, 10, 5)');
      // c=20, d=15: rows with c>20 AND d<15 = row 1 (10>20=F)? no. row 3 (30>20=T, 10<15=T)=1
      testDb.exec('INSERT INTO t1 VALUES(2, 20, 15)');
      // c=30, d=10: rows with c>30 AND d<10 = row 1 (10>30=F, 5<10=T) = 0
      testDb.exec('INSERT INTO t1 VALUES(3, 30, 10)');
      // c=5, d=25: rows with c>5 AND d<25 = row 1 (10>5=T, 5<25=T), row 2 (20>5=T, 15<25=T), row 3 (30>5=T, 10<25=T) = 3
      testDb.exec('INSERT INTO t1 VALUES(4, 5, 25)');

      const result = testDb.prepare(`
        SELECT a, c, d,
               (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d) AS cnt
        FROM t1
        ORDER BY a
      `).all() as { a: number; c: number; d: number; cnt: number }[];

      // For a=1, c=10, d=5: count rows where x.c>10 AND x.d<5 = none
      expect(result[0].cnt).toBe(0);

      // For a=2, c=20, d=15: count rows where x.c>20 AND x.d<15
      //   row 1: 10>20=F, skip
      //   row 2: 20>20=F, skip
      //   row 3: 30>20=T, 10<15=T = 1
      //   row 4: 5>20=F, skip
      expect(result[1].cnt).toBe(1);

      // For a=3, c=30, d=10: count rows where x.c>30 AND x.d<10
      //   row 1: 10>30=F, skip
      //   row 2: 20>30=F, skip
      //   row 3: 30>30=F, skip
      //   row 4: 5>30=F, skip
      expect(result[2].cnt).toBe(0);

      // For a=4, c=5, d=25: count rows where x.c>5 AND x.d<25
      //   row 1: 10>5=T, 5<25=T = 1
      //   row 2: 20>5=T, 15<25=T = 1
      //   row 3: 30>5=T, 10<25=T = 1
      //   row 4: 5>5=F, skip
      expect(result[3].cnt).toBe(3);
    });
  });

  describe('EXISTS correlated subquery in WHERE', () => {
    it('should correctly filter with EXISTS and complex boolean conditions', () => {
      // First check the boolean conditions alone
      const boolResult = db.prepare(`
        SELECT a, b, c, d, e,
               (e>c OR e<d) AS cond1,
               (d>e) AS cond2
        FROM t1
        WHERE (e>c OR e<d) AND d>e
      `).all();
      console.log('Boolean conditions alone:', boolResult.length, 'rows');

      // Now check with EXISTS
      const existsResult = db.prepare(`
        SELECT a, b, c, d, e
        FROM t1
        WHERE (e>c OR e<d)
          AND d>e
          AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
      `).all();
      console.log('With EXISTS:', existsResult.length, 'rows');
      console.log('EXISTS result:', existsResult.slice(0, 5));
    });

    it('should correctly filter with EXISTS alone', () => {
      const result = db.prepare(`
        SELECT a, b
        FROM t1
        WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
      `).all();
      console.log('EXISTS alone:', result.length, 'rows');
      // Should be all rows except the one with the minimum b value
      expect(result.length).toBe(29); // 30 - 1 = 29
    });

    it('should match SQLLogicTest line 109 query', () => {
      const result = db.prepare(`
        SELECT a+b*2+c*3+d*4+e*5,
               CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
                WHEN a<b+3 THEN 333 ELSE 444 END,
               abs(b-c),
               (a+b+c+d+e)/5,
               a+b*2+c*3
        FROM t1
        WHERE (e>c OR e<d)
          AND d>e
          AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
        ORDER BY 4,2,1,3,5
      `).all();
      console.log('SQLLogicTest line 109 result:', result.length, 'rows');
      console.log('First 5:', result.slice(0, 5));
      // Expected: 80 values = 16 rows * 5 columns
      expect(result.length).toBe(16);
    });

    it('should match SQLLogicTest line 634 query - EXISTS OR boolean condition', () => {
      // First test EXISTS alone
      const existsResult = db.prepare(`
        SELECT a, b FROM t1
        WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
      `).all();
      console.log('EXISTS alone:', existsResult.length, 'rows');
      expect(existsResult.length).toBe(29);

      // Test the boolean condition alone
      const boolResult = db.prepare(`
        SELECT a, b FROM t1
        WHERE (a>b-2 AND a<b+2)
      `).all();
      console.log('Boolean alone:', boolResult.length, 'rows');

      // Test EXISTS OR boolean - this is failing
      const orResult = db.prepare(`
        SELECT a, b FROM t1
        WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
           OR (a>b-2 AND a<b+2)
      `).all();
      console.log('EXISTS OR boolean:', orResult.length, 'rows');
      expect(orResult.length).toBeGreaterThan(0);

      // Full query
      const result = db.prepare(`
        SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
                WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
               (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
               d-e,
               b,
               (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
               a+b*2
        FROM t1
        WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
           OR (a>b-2 AND a<b+2)
        ORDER BY 2,6,3,5,4,1
      `).all();
      console.log('SQLLogicTest line 634 result:', result.length, 'rows');
      console.log('First 5:', result.slice(0, 5));
      // Expected: 174 values = 29 rows * 6 columns
      expect(result.length).toBe(29);
    });
  });
});
