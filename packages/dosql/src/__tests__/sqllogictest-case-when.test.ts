/**
 * RED Phase TDD Tests for CASE WHEN Expressions
 *
 * SQLLogicTest shows many failures for CASE WHEN expressions. This file documents
 * the expected behavior that needs to be implemented.
 *
 * Failing queries from SQLLogicTest:
 * - SELECT CASE WHEN a<b THEN 111 WHEN a=b THEN 222 ELSE 333 END FROM t1
 * - SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222 WHEN a<b+3 THEN 333 ELSE 444 END FROM t1
 * - SELECT CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t1
 *
 * SQL CASE Expression Syntax:
 *
 * Searched CASE:
 *   CASE
 *     WHEN condition1 THEN result1
 *     WHEN condition2 THEN result2
 *     ...
 *     ELSE default_result
 *   END
 *
 * Simple CASE:
 *   CASE expression
 *     WHEN value1 THEN result1
 *     WHEN value2 THEN result2
 *     ...
 *     ELSE default_result
 *   END
 *
 * Run with: npx vitest run src/__tests__/sqllogictest-case-when.test.ts
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

describe('SQLLogicTest CASE WHEN Compatibility', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  // ===========================================================================
  // BASIC SEARCHED CASE EXPRESSIONS
  // ===========================================================================

  describe('Searched CASE Expressions (CASE WHEN...THEN...ELSE...END)', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)
      `);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (1, 5, 10)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (5, 5, 15)`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (10, 5, 20)`);
    });

    /**
     * KNOWN FAILURE: Basic CASE WHEN expression not implemented
     *
     * SQLLogicTest query: SELECT CASE WHEN a<b THEN 111 WHEN a=b THEN 222 ELSE 333 END FROM t1
     *
     * Expected results for rows (a=1,b=5), (a=5,b=5), (a=10,b=5):
     * - Row 1: a=1, b=5 -> a<b is true -> 111
     * - Row 2: a=5, b=5 -> a=b is true -> 222
     * - Row 3: a=10, b=5 -> else -> 333
     */
    it('should evaluate simple CASE WHEN with comparison operators', () => {
      const result = db.prepare(`
        SELECT CASE WHEN a < b THEN 111 WHEN a = b THEN 222 ELSE 333 END as result
        FROM t1
        ORDER BY a
      `).all();

      expect(result.length).toBe(3);
      expect((result[0] as { result: number }).result).toBe(111); // a=1 < b=5
      expect((result[1] as { result: number }).result).toBe(222); // a=5 = b=5
      expect((result[2] as { result: number }).result).toBe(333); // a=10 > b=5 (else)
    });

    /**
     * KNOWN FAILURE: CASE WHEN without ELSE clause
     *
     * When no ELSE is provided and no WHEN condition matches, result should be NULL
     */
    it('should return NULL when no WHEN matches and no ELSE provided', () => {
      const result = db.prepare(`
        SELECT CASE WHEN a > 100 THEN 'big' WHEN a > 50 THEN 'medium' END as result
        FROM t1
        ORDER BY a
      `).all();

      expect(result.length).toBe(3);
      expect((result[0] as { result: string | null }).result).toBeNull();
      expect((result[1] as { result: string | null }).result).toBeNull();
      expect((result[2] as { result: string | null }).result).toBeNull();
    });

    /**
     * KNOWN FAILURE: CASE WHEN with single condition
     */
    it('should handle CASE WHEN with single WHEN clause', () => {
      const result = db.prepare(`
        SELECT CASE WHEN a = 5 THEN 'match' ELSE 'no match' END as result
        FROM t1
        ORDER BY a
      `).all();

      expect(result.length).toBe(3);
      expect((result[0] as { result: string }).result).toBe('no match'); // a=1
      expect((result[1] as { result: string }).result).toBe('match');    // a=5
      expect((result[2] as { result: string }).result).toBe('no match'); // a=10
    });
  });

  // ===========================================================================
  // MULTIPLE WHEN CLAUSES
  // ===========================================================================

  describe('Multiple WHEN Clauses', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE scores (id INTEGER, score INTEGER)
      `);
      db.exec(`INSERT INTO scores (id, score) VALUES (1, 95)`);
      db.exec(`INSERT INTO scores (id, score) VALUES (2, 85)`);
      db.exec(`INSERT INTO scores (id, score) VALUES (3, 75)`);
      db.exec(`INSERT INTO scores (id, score) VALUES (4, 65)`);
      db.exec(`INSERT INTO scores (id, score) VALUES (5, 55)`);
    });

    /**
     * KNOWN FAILURE: Multiple WHEN clauses with numeric comparisons
     *
     * This is a common grading pattern where first matching WHEN wins
     */
    it('should evaluate multiple WHEN clauses in order (first match wins)', () => {
      const result = db.prepare(`
        SELECT id, score,
          CASE
            WHEN score >= 90 THEN 'A'
            WHEN score >= 80 THEN 'B'
            WHEN score >= 70 THEN 'C'
            WHEN score >= 60 THEN 'D'
            ELSE 'F'
          END as grade
        FROM scores
        ORDER BY id
      `).all();

      expect(result.length).toBe(5);
      expect((result[0] as { grade: string }).grade).toBe('A'); // 95
      expect((result[1] as { grade: string }).grade).toBe('B'); // 85
      expect((result[2] as { grade: string }).grade).toBe('C'); // 75
      expect((result[3] as { grade: string }).grade).toBe('D'); // 65
      expect((result[4] as { grade: string }).grade).toBe('F'); // 55
    });

    /**
     * KNOWN FAILURE: Many WHEN clauses (stress test)
     */
    it('should handle many WHEN clauses', () => {
      db.exec('CREATE TABLE nums (n INTEGER)');
      db.exec('INSERT INTO nums (n) VALUES (1)');
      db.exec('INSERT INTO nums (n) VALUES (5)');
      db.exec('INSERT INTO nums (n) VALUES (10)');

      const result = db.prepare(`
        SELECT n,
          CASE
            WHEN n = 1 THEN 'one'
            WHEN n = 2 THEN 'two'
            WHEN n = 3 THEN 'three'
            WHEN n = 4 THEN 'four'
            WHEN n = 5 THEN 'five'
            WHEN n = 6 THEN 'six'
            WHEN n = 7 THEN 'seven'
            WHEN n = 8 THEN 'eight'
            WHEN n = 9 THEN 'nine'
            WHEN n = 10 THEN 'ten'
            ELSE 'other'
          END as word
        FROM nums
        ORDER BY n
      `).all();

      expect(result.length).toBe(3);
      expect((result[0] as { word: string }).word).toBe('one');   // n=1
      expect((result[1] as { word: string }).word).toBe('five');  // n=5
      expect((result[2] as { word: string }).word).toBe('ten');   // n=10
    });
  });

  // ===========================================================================
  // COMPARISON OPERATORS IN WHEN CONDITIONS
  // ===========================================================================

  describe('Comparison Operators in WHEN Conditions', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (a INTEGER, b INTEGER)
      `);
      db.exec(`INSERT INTO t1 (a, b) VALUES (1, 10)`);
      db.exec(`INSERT INTO t1 (a, b) VALUES (5, 10)`);
      db.exec(`INSERT INTO t1 (a, b) VALUES (10, 10)`);
      db.exec(`INSERT INTO t1 (a, b) VALUES (15, 10)`);
    });

    /**
     * KNOWN FAILURE: CASE WHEN with < operator
     */
    it('should handle less than (<) in WHEN condition', () => {
      const result = db.prepare(`
        SELECT a, CASE WHEN a < b THEN 'less' ELSE 'not less' END as cmp
        FROM t1
        ORDER BY a
      `).all();

      expect(result.length).toBe(4);
      expect((result[0] as { cmp: string }).cmp).toBe('less');     // 1 < 10
      expect((result[1] as { cmp: string }).cmp).toBe('less');     // 5 < 10
      expect((result[2] as { cmp: string }).cmp).toBe('not less'); // 10 < 10 is false
      expect((result[3] as { cmp: string }).cmp).toBe('not less'); // 15 < 10 is false
    });

    /**
     * KNOWN FAILURE: CASE WHEN with > operator
     */
    it('should handle greater than (>) in WHEN condition', () => {
      const result = db.prepare(`
        SELECT a, CASE WHEN a > b THEN 'greater' ELSE 'not greater' END as cmp
        FROM t1
        ORDER BY a
      `).all();

      expect(result.length).toBe(4);
      expect((result[0] as { cmp: string }).cmp).toBe('not greater'); // 1 > 10 is false
      expect((result[1] as { cmp: string }).cmp).toBe('not greater'); // 5 > 10 is false
      expect((result[2] as { cmp: string }).cmp).toBe('not greater'); // 10 > 10 is false
      expect((result[3] as { cmp: string }).cmp).toBe('greater');     // 15 > 10
    });

    /**
     * KNOWN FAILURE: CASE WHEN with <= operator
     */
    it('should handle less than or equal (<=) in WHEN condition', () => {
      const result = db.prepare(`
        SELECT a, CASE WHEN a <= b THEN 'lte' ELSE 'gt' END as cmp
        FROM t1
        ORDER BY a
      `).all();

      expect(result.length).toBe(4);
      expect((result[0] as { cmp: string }).cmp).toBe('lte'); // 1 <= 10
      expect((result[1] as { cmp: string }).cmp).toBe('lte'); // 5 <= 10
      expect((result[2] as { cmp: string }).cmp).toBe('lte'); // 10 <= 10
      expect((result[3] as { cmp: string }).cmp).toBe('gt');  // 15 <= 10 is false
    });

    /**
     * KNOWN FAILURE: CASE WHEN with >= operator
     */
    it('should handle greater than or equal (>=) in WHEN condition', () => {
      const result = db.prepare(`
        SELECT a, CASE WHEN a >= b THEN 'gte' ELSE 'lt' END as cmp
        FROM t1
        ORDER BY a
      `).all();

      expect(result.length).toBe(4);
      expect((result[0] as { cmp: string }).cmp).toBe('lt');  // 1 >= 10 is false
      expect((result[1] as { cmp: string }).cmp).toBe('lt');  // 5 >= 10 is false
      expect((result[2] as { cmp: string }).cmp).toBe('gte'); // 10 >= 10
      expect((result[3] as { cmp: string }).cmp).toBe('gte'); // 15 >= 10
    });

    /**
     * KNOWN FAILURE: CASE WHEN with = operator
     */
    it('should handle equal (=) in WHEN condition', () => {
      const result = db.prepare(`
        SELECT a, CASE WHEN a = b THEN 'equal' ELSE 'not equal' END as cmp
        FROM t1
        ORDER BY a
      `).all();

      expect(result.length).toBe(4);
      expect((result[0] as { cmp: string }).cmp).toBe('not equal'); // 1 = 10 is false
      expect((result[1] as { cmp: string }).cmp).toBe('not equal'); // 5 = 10 is false
      expect((result[2] as { cmp: string }).cmp).toBe('equal');     // 10 = 10
      expect((result[3] as { cmp: string }).cmp).toBe('not equal'); // 15 = 10 is false
    });

    /**
     * KNOWN FAILURE: CASE WHEN with <> (not equal) operator
     */
    it('should handle not equal (<>) in WHEN condition', () => {
      const result = db.prepare(`
        SELECT a, CASE WHEN a <> b THEN 'different' ELSE 'same' END as cmp
        FROM t1
        ORDER BY a
      `).all();

      expect(result.length).toBe(4);
      expect((result[0] as { cmp: string }).cmp).toBe('different'); // 1 <> 10
      expect((result[1] as { cmp: string }).cmp).toBe('different'); // 5 <> 10
      expect((result[2] as { cmp: string }).cmp).toBe('same');      // 10 <> 10 is false
      expect((result[3] as { cmp: string }).cmp).toBe('different'); // 15 <> 10
    });
  });

  // ===========================================================================
  // ARITHMETIC IN CONDITIONS
  // ===========================================================================

  describe('Arithmetic Expressions in WHEN Conditions', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (a INTEGER, b INTEGER)
      `);
      db.exec(`INSERT INTO t1 (a, b) VALUES (5, 10)`);
      db.exec(`INSERT INTO t1 (a, b) VALUES (7, 10)`);
      db.exec(`INSERT INTO t1 (a, b) VALUES (10, 10)`);
      db.exec(`INSERT INTO t1 (a, b) VALUES (13, 10)`);
      db.exec(`INSERT INTO t1 (a, b) VALUES (15, 10)`);
    });

    /**
     * KNOWN FAILURE: SQLLogicTest query with arithmetic in CASE WHEN
     *
     * Query: SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222 WHEN a<b+3 THEN 333 ELSE 444 END FROM t1
     *
     * For b=10:
     * - b-3 = 7, b+3 = 13
     * - a=5:  5 < 7 (true)  -> 111
     * - a=7:  7 < 7 (false), 7 <= 10 (true) -> 222
     * - a=10: 10 < 7 (false), 10 <= 10 (true) -> 222
     * - a=13: 13 < 7 (false), 13 <= 10 (false), 13 < 13 (false) -> 444
     * - a=15: 15 < 7 (false), 15 <= 10 (false), 15 < 13 (false) -> 444
     */
    it('should handle arithmetic subtraction in WHEN condition', () => {
      const result = db.prepare(`
        SELECT a, CASE WHEN a < b - 3 THEN 111 WHEN a <= b THEN 222 WHEN a < b + 3 THEN 333 ELSE 444 END as result
        FROM t1
        ORDER BY a
      `).all();

      expect(result.length).toBe(5);
      expect((result[0] as { result: number }).result).toBe(111); // a=5 < b-3=7
      expect((result[1] as { result: number }).result).toBe(222); // a=7 <= b=10
      expect((result[2] as { result: number }).result).toBe(222); // a=10 <= b=10
      expect((result[3] as { result: number }).result).toBe(444); // a=13, no match (13 is not < 13)
      expect((result[4] as { result: number }).result).toBe(444); // a=15 else
    });

    /**
     * KNOWN FAILURE: Arithmetic addition in WHEN condition
     */
    it('should handle arithmetic addition in WHEN condition', () => {
      const result = db.prepare(`
        SELECT a, CASE WHEN a > b + 2 THEN 'over' ELSE 'under' END as result
        FROM t1
        ORDER BY a
      `).all();

      expect(result.length).toBe(5);
      expect((result[0] as { result: string }).result).toBe('under'); // 5 > 12 is false
      expect((result[1] as { result: string }).result).toBe('under'); // 7 > 12 is false
      expect((result[2] as { result: string }).result).toBe('under'); // 10 > 12 is false
      expect((result[3] as { result: string }).result).toBe('over');  // 13 > 12
      expect((result[4] as { result: string }).result).toBe('over');  // 15 > 12
    });

    /**
     * KNOWN FAILURE: Arithmetic multiplication in WHEN condition
     */
    it('should handle arithmetic multiplication in WHEN condition', () => {
      const result = db.prepare(`
        SELECT a, CASE WHEN a < b * 2 THEN 'small' ELSE 'large' END as result
        FROM t1
        ORDER BY a
      `).all();

      expect(result.length).toBe(5);
      // b * 2 = 20
      expect((result[0] as { result: string }).result).toBe('small'); // 5 < 20
      expect((result[1] as { result: string }).result).toBe('small'); // 7 < 20
      expect((result[2] as { result: string }).result).toBe('small'); // 10 < 20
      expect((result[3] as { result: string }).result).toBe('small'); // 13 < 20
      expect((result[4] as { result: string }).result).toBe('small'); // 15 < 20
    });

    /**
     * KNOWN FAILURE: Arithmetic division in WHEN condition
     */
    it('should handle arithmetic division in WHEN condition', () => {
      const result = db.prepare(`
        SELECT a, CASE WHEN a > b / 2 THEN 'above half' ELSE 'at or below half' END as result
        FROM t1
        ORDER BY a
      `).all();

      expect(result.length).toBe(5);
      // b / 2 = 5
      expect((result[0] as { result: string }).result).toBe('at or below half'); // 5 > 5 is false
      expect((result[1] as { result: string }).result).toBe('above half');       // 7 > 5
      expect((result[2] as { result: string }).result).toBe('above half');       // 10 > 5
      expect((result[3] as { result: string }).result).toBe('above half');       // 13 > 5
      expect((result[4] as { result: string }).result).toBe('above half');       // 15 > 5
    });

    /**
     * KNOWN FAILURE: Complex arithmetic expression in WHEN condition
     */
    it('should handle complex arithmetic expressions', () => {
      const result = db.prepare(`
        SELECT a, CASE WHEN a * 2 > b + 5 THEN 'yes' ELSE 'no' END as result
        FROM t1
        ORDER BY a
      `).all();

      expect(result.length).toBe(5);
      // b + 5 = 15
      // a * 2: 10, 14, 20, 26, 30
      expect((result[0] as { result: string }).result).toBe('no');  // 10 > 15 is false
      expect((result[1] as { result: string }).result).toBe('no');  // 14 > 15 is false
      expect((result[2] as { result: string }).result).toBe('yes'); // 20 > 15
      expect((result[3] as { result: string }).result).toBe('yes'); // 26 > 15
      expect((result[4] as { result: string }).result).toBe('yes'); // 30 > 15
    });
  });

  // ===========================================================================
  // SIMPLE CASE EXPRESSIONS
  // ===========================================================================

  describe('Simple CASE Expressions (CASE x WHEN value THEN...)', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (x INTEGER, label TEXT)
      `);
      db.exec(`INSERT INTO t1 (x, label) VALUES (1, 'first')`);
      db.exec(`INSERT INTO t1 (x, label) VALUES (2, 'second')`);
      db.exec(`INSERT INTO t1 (x, label) VALUES (3, 'third')`);
      db.exec(`INSERT INTO t1 (x, label) VALUES (4, 'fourth')`);
    });

    /**
     * KNOWN FAILURE: SQLLogicTest simple CASE expression
     *
     * Query: SELECT CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t1
     *
     * This is the "simple CASE" form where the expression after CASE is compared
     * to each WHEN value for equality.
     */
    it('should evaluate simple CASE with numeric comparison', () => {
      const result = db.prepare(`
        SELECT x, CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END as word
        FROM t1
        ORDER BY x
      `).all();

      expect(result.length).toBe(4);
      expect((result[0] as { word: string }).word).toBe('one');   // x=1
      expect((result[1] as { word: string }).word).toBe('two');   // x=2
      expect((result[2] as { word: string }).word).toBe('other'); // x=3
      expect((result[3] as { word: string }).word).toBe('other'); // x=4
    });

    /**
     * KNOWN FAILURE: Simple CASE with string expression
     */
    it('should evaluate simple CASE with string comparison', () => {
      const result = db.prepare(`
        SELECT label, CASE label WHEN 'first' THEN 1 WHEN 'second' THEN 2 ELSE 0 END as num
        FROM t1
        ORDER BY x
      `).all();

      expect(result.length).toBe(4);
      expect((result[0] as { num: number }).num).toBe(1); // 'first'
      expect((result[1] as { num: number }).num).toBe(2); // 'second'
      expect((result[2] as { num: number }).num).toBe(0); // 'third'
      expect((result[3] as { num: number }).num).toBe(0); // 'fourth'
    });

    /**
     * KNOWN FAILURE: Simple CASE without ELSE (should return NULL)
     */
    it('should return NULL for simple CASE when no WHEN matches and no ELSE', () => {
      const result = db.prepare(`
        SELECT x, CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' END as word
        FROM t1
        ORDER BY x
      `).all();

      expect(result.length).toBe(4);
      expect((result[0] as { word: string | null }).word).toBe('one');
      expect((result[1] as { word: string | null }).word).toBe('two');
      expect((result[2] as { word: string | null }).word).toBeNull(); // x=3, no match
      expect((result[3] as { word: string | null }).word).toBeNull(); // x=4, no match
    });

    /**
     * KNOWN FAILURE: Simple CASE with many WHEN values
     */
    it('should handle simple CASE with many WHEN values', () => {
      const result = db.prepare(`
        SELECT x,
          CASE x
            WHEN 1 THEN 'one'
            WHEN 2 THEN 'two'
            WHEN 3 THEN 'three'
            WHEN 4 THEN 'four'
            WHEN 5 THEN 'five'
          END as word
        FROM t1
        ORDER BY x
      `).all();

      expect(result.length).toBe(4);
      expect((result[0] as { word: string }).word).toBe('one');
      expect((result[1] as { word: string }).word).toBe('two');
      expect((result[2] as { word: string }).word).toBe('three');
      expect((result[3] as { word: string }).word).toBe('four');
    });
  });

  // ===========================================================================
  // NESTED CASE EXPRESSIONS
  // ===========================================================================

  describe('Nested CASE Expressions', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE data (category TEXT, value INTEGER)
      `);
      db.exec(`INSERT INTO data (category, value) VALUES ('A', 10)`);
      db.exec(`INSERT INTO data (category, value) VALUES ('A', 50)`);
      db.exec(`INSERT INTO data (category, value) VALUES ('B', 30)`);
      db.exec(`INSERT INTO data (category, value) VALUES ('B', 70)`);
    });

    /**
     * KNOWN FAILURE: Nested CASE in THEN clause
     *
     * Outer CASE determines category, inner CASE determines size
     */
    it('should handle CASE nested inside THEN clause', () => {
      const result = db.prepare(`
        SELECT category, value,
          CASE category
            WHEN 'A' THEN
              CASE WHEN value < 30 THEN 'A-small' ELSE 'A-large' END
            WHEN 'B' THEN
              CASE WHEN value < 50 THEN 'B-small' ELSE 'B-large' END
            ELSE 'unknown'
          END as label
        FROM data
        ORDER BY category, value
      `).all();

      expect(result.length).toBe(4);
      expect((result[0] as { label: string }).label).toBe('A-small'); // A, 10
      expect((result[1] as { label: string }).label).toBe('A-large'); // A, 50
      expect((result[2] as { label: string }).label).toBe('B-small'); // B, 30
      expect((result[3] as { label: string }).label).toBe('B-large'); // B, 70
    });

    /**
     * KNOWN FAILURE: Nested CASE in WHEN condition
     */
    it('should handle CASE nested inside WHEN condition', () => {
      const result = db.prepare(`
        SELECT category, value,
          CASE
            WHEN (CASE category WHEN 'A' THEN 25 ELSE 50 END) < value THEN 'above threshold'
            ELSE 'at or below threshold'
          END as status
        FROM data
        ORDER BY category, value
      `).all();

      expect(result.length).toBe(4);
      // Category A threshold: 25
      // Category B threshold: 50
      expect((result[0] as { status: string }).status).toBe('at or below threshold'); // A, 10 < 25 is false
      expect((result[1] as { status: string }).status).toBe('above threshold');       // A, 50 > 25
      expect((result[2] as { status: string }).status).toBe('at or below threshold'); // B, 30 < 50 is false
      expect((result[3] as { status: string }).status).toBe('above threshold');       // B, 70 > 50
    });

    /**
     * KNOWN FAILURE: Deeply nested CASE (3 levels)
     */
    it('should handle deeply nested CASE expressions', () => {
      const result = db.prepare(`
        SELECT category, value,
          CASE
            WHEN value < 25 THEN
              CASE category
                WHEN 'A' THEN 'A-tier1'
                ELSE 'B-tier1'
              END
            WHEN value < 60 THEN
              CASE category
                WHEN 'A' THEN 'A-tier2'
                ELSE
                  CASE WHEN value < 40 THEN 'B-tier2a' ELSE 'B-tier2b' END
              END
            ELSE 'tier3'
          END as tier
        FROM data
        ORDER BY category, value
      `).all();

      expect(result.length).toBe(4);
      expect((result[0] as { tier: string }).tier).toBe('A-tier1');  // A, 10 < 25
      expect((result[1] as { tier: string }).tier).toBe('A-tier2');  // A, 50 in [25,60)
      expect((result[2] as { tier: string }).tier).toBe('B-tier2a'); // B, 30 in [25,60), 30 < 40
      expect((result[3] as { tier: string }).tier).toBe('tier3');    // B, 70 >= 60
    });
  });

  // ===========================================================================
  // NULL HANDLING IN CASE EXPRESSIONS
  // ===========================================================================

  describe('NULL Handling in CASE Expressions', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE nullable (id INTEGER, value INTEGER)
      `);
      db.exec(`INSERT INTO nullable (id, value) VALUES (1, 10)`);
      db.exec(`INSERT INTO nullable (id, value) VALUES (2, NULL)`);
      db.exec(`INSERT INTO nullable (id, value) VALUES (3, 30)`);
    });

    /**
     * KNOWN FAILURE: CASE WHEN with NULL comparison
     *
     * In SQL, NULL comparisons (NULL = anything, NULL < anything) are always NULL/false
     * So NULL values should fall through to ELSE
     */
    it('should handle NULL in WHEN condition (comparison always fails)', () => {
      const result = db.prepare(`
        SELECT id, value,
          CASE WHEN value > 15 THEN 'high' ELSE 'low or null' END as category
        FROM nullable
        ORDER BY id
      `).all();

      expect(result.length).toBe(3);
      expect((result[0] as { category: string }).category).toBe('low or null'); // 10 > 15 is false
      expect((result[1] as { category: string }).category).toBe('low or null'); // NULL > 15 is NULL -> ELSE
      expect((result[2] as { category: string }).category).toBe('high');        // 30 > 15
    });

    /**
     * KNOWN FAILURE: Simple CASE with NULL value
     *
     * CASE x WHEN NULL THEN ... does NOT match NULL because NULL = NULL is NULL
     */
    it('should not match NULL with simple CASE (NULL = NULL is NULL)', () => {
      const result = db.prepare(`
        SELECT id, value,
          CASE value WHEN 10 THEN 'ten' WHEN NULL THEN 'null-matched' ELSE 'other' END as label
        FROM nullable
        ORDER BY id
      `).all();

      expect(result.length).toBe(3);
      expect((result[0] as { label: string }).label).toBe('ten');
      expect((result[1] as { label: string }).label).toBe('other'); // NULL doesn't match WHEN NULL
      expect((result[2] as { label: string }).label).toBe('other');
    });

    /**
     * KNOWN FAILURE: Using IS NULL in WHEN condition
     */
    it('should handle IS NULL in WHEN condition', () => {
      const result = db.prepare(`
        SELECT id, value,
          CASE WHEN value IS NULL THEN 'missing' ELSE 'present' END as status
        FROM nullable
        ORDER BY id
      `).all();

      expect(result.length).toBe(3);
      expect((result[0] as { status: string }).status).toBe('present');
      expect((result[1] as { status: string }).status).toBe('missing');
      expect((result[2] as { status: string }).status).toBe('present');
    });

    /**
     * KNOWN FAILURE: COALESCE-like behavior with CASE
     */
    it('should implement COALESCE-like behavior with CASE', () => {
      const result = db.prepare(`
        SELECT id, value,
          CASE WHEN value IS NOT NULL THEN value ELSE -1 END as safe_value
        FROM nullable
        ORDER BY id
      `).all();

      expect(result.length).toBe(3);
      expect((result[0] as { safe_value: number }).safe_value).toBe(10);
      expect((result[1] as { safe_value: number }).safe_value).toBe(-1);
      expect((result[2] as { safe_value: number }).safe_value).toBe(30);
    });
  });

  // ===========================================================================
  // CASE IN DIFFERENT CONTEXTS
  // ===========================================================================

  describe('CASE in Different SQL Contexts', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE orders (id INTEGER, customer TEXT, amount INTEGER, status TEXT)
      `);
      db.exec(`INSERT INTO orders (id, customer, amount, status) VALUES (1, 'Alice', 100, 'completed')`);
      db.exec(`INSERT INTO orders (id, customer, amount, status) VALUES (2, 'Alice', 200, 'pending')`);
      db.exec(`INSERT INTO orders (id, customer, amount, status) VALUES (3, 'Bob', 150, 'completed')`);
      db.exec(`INSERT INTO orders (id, customer, amount, status) VALUES (4, 'Bob', 300, 'cancelled')`);
    });

    /**
     * KNOWN FAILURE: CASE in ORDER BY
     * Requires ORDER BY expression support (not implemented in InMemoryEngine)
     */
    it.fails('should handle CASE in ORDER BY clause', () => {
      const result = db.prepare(`
        SELECT id, customer, amount, status
        FROM orders
        ORDER BY CASE status WHEN 'pending' THEN 1 WHEN 'completed' THEN 2 ELSE 3 END, amount
      `).all();

      expect(result.length).toBe(4);
      // First pending (priority 1), then completed (priority 2), then cancelled (priority 3)
      expect((result[0] as { status: string }).status).toBe('pending');
      expect((result[1] as { status: string }).status).toBe('completed');
      expect((result[2] as { status: string }).status).toBe('completed');
      expect((result[3] as { status: string }).status).toBe('cancelled');
    });

    /**
     * KNOWN FAILURE: CASE in WHERE clause
     * Requires WHERE expression support (not implemented in InMemoryEngine)
     */
    it.fails('should handle CASE in WHERE clause', () => {
      const result = db.prepare(`
        SELECT id, customer, amount
        FROM orders
        WHERE CASE WHEN amount > 150 THEN 1 ELSE 0 END = 1
        ORDER BY id
      `).all();

      expect(result.length).toBe(2);
      expect((result[0] as { id: number }).id).toBe(2); // amount = 200
      expect((result[1] as { id: number }).id).toBe(4); // amount = 300
    });

    /**
     * KNOWN FAILURE: CASE with aggregate functions
     * Requires GROUP BY support (not implemented in InMemoryEngine)
     */
    it.fails('should handle CASE inside aggregate functions', () => {
      const result = db.prepare(`
        SELECT customer,
          SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as completed_total,
          SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) as pending_total
        FROM orders
        GROUP BY customer
        ORDER BY customer
      `).all();

      expect(result.length).toBe(2);
      expect((result[0] as { customer: string; completed_total: number; pending_total: number }))
        .toEqual({ customer: 'Alice', completed_total: 100, pending_total: 200 });
      expect((result[1] as { customer: string; completed_total: number; pending_total: number }))
        .toEqual({ customer: 'Bob', completed_total: 150, pending_total: 0 });
    });

    /**
     * KNOWN FAILURE: COUNT with CASE (conditional counting)
     * Requires GROUP BY support (not implemented in InMemoryEngine)
     */
    it.fails('should handle COUNT with CASE for conditional counting', () => {
      const result = db.prepare(`
        SELECT customer,
          COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_count,
          COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_count,
          COUNT(*) as total_count
        FROM orders
        GROUP BY customer
        ORDER BY customer
      `).all();

      expect(result.length).toBe(2);
      expect((result[0] as { completed_count: number; pending_count: number; total_count: number }))
        .toEqual({ customer: 'Alice', completed_count: 1, pending_count: 1, total_count: 2 });
      expect((result[1] as { completed_count: number; pending_count: number; total_count: number }))
        .toEqual({ customer: 'Bob', completed_count: 1, pending_count: 0, total_count: 2 });
    });
  });

  // ===========================================================================
  // TYPE COERCION IN CASE EXPRESSIONS
  // ===========================================================================

  describe('Type Coercion in CASE Expressions', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE mixed (id INTEGER, num_val INTEGER, str_val TEXT)
      `);
      db.exec(`INSERT INTO mixed (id, num_val, str_val) VALUES (1, 10, '10')`);
      db.exec(`INSERT INTO mixed (id, num_val, str_val) VALUES (2, 20, '20')`);
    });

    /**
     * KNOWN FAILURE: Mixed types in THEN branches
     *
     * All THEN branches should return compatible types
     */
    it('should handle mixed numeric and string results', () => {
      const result = db.prepare(`
        SELECT id,
          CASE WHEN num_val < 15 THEN 'small' ELSE 'large' END as size
        FROM mixed
        ORDER BY id
      `).all();

      expect(result.length).toBe(2);
      expect((result[0] as { size: string }).size).toBe('small');
      expect((result[1] as { size: string }).size).toBe('large');
    });

    /**
     * KNOWN FAILURE: Integer results from CASE
     */
    it('should return integer from CASE when all branches are integers', () => {
      const result = db.prepare(`
        SELECT id,
          CASE WHEN num_val < 15 THEN 1 ELSE 2 END as category
        FROM mixed
        ORDER BY id
      `).all();

      expect(result.length).toBe(2);
      expect(typeof (result[0] as { category: number }).category).toBe('number');
      expect((result[0] as { category: number }).category).toBe(1);
      expect((result[1] as { category: number }).category).toBe(2);
    });
  });
});

// =============================================================================
// LOW-LEVEL INMEMORYENGINE CASE WHEN TESTS
// =============================================================================

describe('InMemoryEngine CASE WHEN Tests', () => {
  let engine: InMemoryEngine;
  let storage: InMemoryStorage;

  beforeEach(() => {
    storage = createInMemoryStorage();
    engine = new InMemoryEngine(storage);

    // Set up test table
    engine.execute('CREATE TABLE test (id INTEGER, a INTEGER, b INTEGER)', []);
    engine.execute('INSERT INTO test (id, a, b) VALUES (?, ?, ?)', [1, 5, 10]);
    engine.execute('INSERT INTO test (id, a, b) VALUES (?, ?, ?)', [2, 10, 10]);
    engine.execute('INSERT INTO test (id, a, b) VALUES (?, ?, ?)', [3, 15, 10]);
  });

  /**
   * KNOWN FAILURE: Basic CASE WHEN at engine level
   */
  it('should execute basic CASE WHEN expression', () => {
    const result = engine.execute(
      'SELECT id, CASE WHEN a < b THEN 1 WHEN a = b THEN 2 ELSE 3 END as cmp FROM test ORDER BY id',
      []
    );

    expect(result.rows.length).toBe(3);
    expect(result.rows[0].cmp).toBe(1); // 5 < 10
    expect(result.rows[1].cmp).toBe(2); // 10 = 10
    expect(result.rows[2].cmp).toBe(3); // 15 > 10 -> else
  });

  /**
   * KNOWN FAILURE: Simple CASE at engine level
   */
  it('should execute simple CASE expression', () => {
    const result = engine.execute(
      "SELECT id, CASE a WHEN 5 THEN 'five' WHEN 10 THEN 'ten' ELSE 'other' END as word FROM test ORDER BY id",
      []
    );

    expect(result.rows.length).toBe(3);
    expect(result.rows[0].word).toBe('five');
    expect(result.rows[1].word).toBe('ten');
    expect(result.rows[2].word).toBe('other');
  });

  /**
   * KNOWN FAILURE: CASE with parameter binding
   */
  it('should handle CASE with parameter binding', () => {
    const result = engine.execute(
      'SELECT id, CASE WHEN a > ? THEN 1 ELSE 0 END as above FROM test ORDER BY id',
      [7]
    );

    expect(result.rows.length).toBe(3);
    expect(result.rows[0].above).toBe(0); // 5 > 7 is false
    expect(result.rows[1].above).toBe(1); // 10 > 7
    expect(result.rows[2].above).toBe(1); // 15 > 7
  });
});
