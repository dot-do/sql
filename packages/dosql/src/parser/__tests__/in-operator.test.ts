/**
 * IN Operator Tests
 *
 * Tests for IN operator with literal value lists in DoSQL.
 *
 * Issue: sql-gr7o - Fix IN operator with literal value lists (P1)
 *
 * Problem:
 * - IN operator with literal lists returns empty instead of matching rows
 * - `SELECT 1 FROM t1 WHERE 1 IN (1)` returns nothing but should return rows
 * - NULL handling in IN lists needs proper SQL three-valued logic
 *
 * Run with: cd packages/dosql && pnpm test src/parser/__tests__/in-operator.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Database } from '../../database.js';

describe('IN Operator with Literal Value Lists', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
    db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');
    db.exec('INSERT INTO t1 (a, b) VALUES (1, \'one\')');
    db.exec('INSERT INTO t1 (a, b) VALUES (2, \'two\')');
    db.exec('INSERT INTO t1 (a, b) VALUES (3, \'three\')');
    db.exec('INSERT INTO t1 (a, b) VALUES (NULL, \'null\')');
  });

  // ===========================================================================
  // LITERAL IN WITH SINGLE VALUE
  // ===========================================================================

  describe('Literal IN with single value', () => {
    it('should match when literal equals single value in list: WHERE 1 IN (1)', () => {
      // This is the main failing case from the issue
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 1 IN (1)').all();

      // Should return 4 rows (one for each row in t1) because 1 IN (1) is TRUE
      expect(result.length).toBe(4);
      expect(result[0]).toEqual({ result: 1 });
    });

    it('should NOT match when literal does not equal single value: WHERE 1 IN (2)', () => {
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 1 IN (2)').all();

      // Should return 0 rows because 1 IN (2) is FALSE
      expect(result.length).toBe(0);
    });

    it('should match string literal in list: WHERE \'a\' IN (\'a\')', () => {
      const result = db.prepare("SELECT 1 AS result FROM t1 WHERE 'a' IN ('a')").all();

      expect(result.length).toBe(4);
    });

    it('should NOT match different string literals: WHERE \'a\' IN (\'b\')', () => {
      const result = db.prepare("SELECT 1 AS result FROM t1 WHERE 'a' IN ('b')").all();

      expect(result.length).toBe(0);
    });
  });

  // ===========================================================================
  // LITERAL IN WITH MULTIPLE VALUES
  // ===========================================================================

  describe('Literal IN with multiple values', () => {
    it('should match when literal is first in list: WHERE 1 IN (1, 2, 3)', () => {
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 1 IN (1, 2, 3)').all();

      expect(result.length).toBe(4);
    });

    it('should match when literal is middle in list: WHERE 2 IN (1, 2, 3)', () => {
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 2 IN (1, 2, 3)').all();

      expect(result.length).toBe(4);
    });

    it('should match when literal is last in list: WHERE 3 IN (1, 2, 3)', () => {
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 3 IN (1, 2, 3)').all();

      expect(result.length).toBe(4);
    });

    it('should NOT match when literal not in list: WHERE 4 IN (1, 2, 3)', () => {
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 4 IN (1, 2, 3)').all();

      expect(result.length).toBe(0);
    });

    it('should match string literal in list of strings', () => {
      const result = db.prepare("SELECT 1 AS result FROM t1 WHERE 'b' IN ('a', 'b', 'c')").all();

      expect(result.length).toBe(4);
    });
  });

  // ===========================================================================
  // NULL HANDLING IN IN LISTS
  // ===========================================================================

  describe('IN with NULL in list', () => {
    it('should match when value found regardless of NULL: WHERE 1 IN (NULL, 1)', () => {
      // SQL standard: if value is found in list, result is TRUE regardless of NULL
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 1 IN (NULL, 1)').all();

      expect(result.length).toBe(4);
    });

    it('should return NULL (falsy) when value not found and NULL in list: WHERE 2 IN (NULL, 1)', () => {
      // SQL standard: if value not found and NULL is in list, result is NULL (falsy in WHERE)
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 2 IN (NULL, 1)').all();

      expect(result.length).toBe(0);
    });

    it('should return NULL (falsy) for NULL literal: WHERE NULL IN (1, 2)', () => {
      // NULL IN (...) is always NULL
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE NULL IN (1, 2)').all();

      expect(result.length).toBe(0);
    });

    it('should handle column IN with NULL in list', () => {
      // a IN (NULL, 1) should return row where a=1
      const result = db.prepare('SELECT a FROM t1 WHERE a IN (NULL, 1)').all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 1 });
    });
  });

  // ===========================================================================
  // COLUMN IN WITH LITERAL LIST
  // ===========================================================================

  describe('Column IN with literal list', () => {
    it('should filter rows where column matches value in list: WHERE a IN (1, 2)', () => {
      const result = db.prepare('SELECT a FROM t1 WHERE a IN (1, 2)').all();

      const sorted = (result as { a: number }[]).sort((a, b) => a.a - b.a);
      expect(sorted.length).toBe(2);
      expect(sorted[0]).toEqual({ a: 1 });
      expect(sorted[1]).toEqual({ a: 2 });
    });

    it('should filter rows with single value: WHERE a IN (2)', () => {
      const result = db.prepare('SELECT a FROM t1 WHERE a IN (2)').all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 2 });
    });

    it('should return no rows when no match: WHERE a IN (10, 20)', () => {
      const result = db.prepare('SELECT a FROM t1 WHERE a IN (10, 20)').all();

      expect(result.length).toBe(0);
    });

    it('should filter string column: WHERE b IN (\'one\', \'two\')', () => {
      const result = db.prepare("SELECT b FROM t1 WHERE b IN ('one', 'two')").all();

      const values = (result as { b: string }[]).map(r => r.b).sort();
      expect(values).toEqual(['one', 'two']);
    });
  });

  // ===========================================================================
  // NOT IN WITH LITERAL VALUES
  // ===========================================================================

  describe('NOT IN with literals', () => {
    it('should match when literal NOT in list: WHERE 1 NOT IN (2, 3)', () => {
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 1 NOT IN (2, 3)').all();

      expect(result.length).toBe(4);
    });

    it('should NOT match when literal IN list: WHERE 1 NOT IN (1, 2)', () => {
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 1 NOT IN (1, 2)').all();

      expect(result.length).toBe(0);
    });

    it('should return NULL (falsy) when NOT IN list contains NULL: WHERE 2 NOT IN (NULL, 1)', () => {
      // SQL standard: NOT IN with NULL in list returns NULL if value not found
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 2 NOT IN (NULL, 1)').all();

      expect(result.length).toBe(0);
    });
  });

  // ===========================================================================
  // TYPE COERCION
  // ===========================================================================

  describe('Type coercion in IN', () => {
    it('should coerce integer to string for comparison: WHERE 1 IN (\'1\')', () => {
      // SQLite performs type coercion - 1 should match '1'
      const result = db.prepare("SELECT 1 AS result FROM t1 WHERE 1 IN ('1')").all();

      expect(result.length).toBe(4);
    });

    it('should coerce string to integer for comparison: WHERE \'1\' IN (1)', () => {
      const result = db.prepare("SELECT 1 AS result FROM t1 WHERE '1' IN (1)").all();

      expect(result.length).toBe(4);
    });

    it('should handle mixed types in list: WHERE 1 IN (1, \'2\', 3)', () => {
      const result = db.prepare("SELECT 1 AS result FROM t1 WHERE 1 IN (1, '2', 3)").all();

      expect(result.length).toBe(4);
    });

    it('should handle float comparison: WHERE 1.0 IN (1)', () => {
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 1.0 IN (1)').all();

      expect(result.length).toBe(4);
    });

    it('should handle column with float list: WHERE a IN (1.0, 2.0)', () => {
      const result = db.prepare('SELECT a FROM t1 WHERE a IN (1.0, 2.0)').all();

      const sorted = (result as { a: number }[]).sort((a, b) => a.a - b.a);
      expect(sorted.length).toBe(2);
    });
  });

  // ===========================================================================
  // EMPTY LIST
  // ===========================================================================

  describe('IN with empty list', () => {
    it('should return FALSE for IN with empty list: WHERE 1 IN ()', () => {
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 1 IN ()').all();

      expect(result.length).toBe(0);
    });

    it('should return TRUE for NOT IN with empty list: WHERE 1 NOT IN ()', () => {
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 1 NOT IN ()').all();

      expect(result.length).toBe(4);
    });

    it('should return FALSE for column IN empty list: WHERE a IN ()', () => {
      const result = db.prepare('SELECT a FROM t1 WHERE a IN ()').all();

      expect(result.length).toBe(0);
    });
  });

  // ===========================================================================
  // COMBINED CONDITIONS
  // ===========================================================================

  describe('IN combined with other conditions', () => {
    it('should work with AND: WHERE 1 IN (1) AND a = 2', () => {
      const result = db.prepare('SELECT a FROM t1 WHERE 1 IN (1) AND a = 2').all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 2 });
    });

    it('should work with OR: WHERE 1 IN (2) OR a = 1', () => {
      const result = db.prepare('SELECT a FROM t1 WHERE 1 IN (2) OR a = 1').all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 1 });
    });

    it('should work with multiple IN conditions', () => {
      const result = db.prepare('SELECT a, b FROM t1 WHERE a IN (1, 2) AND b IN (\'one\')').all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 1, b: 'one' });
    });
  });

  // ===========================================================================
  // NEGATIVE NUMBERS
  // ===========================================================================

  describe('IN with negative numbers', () => {
    beforeEach(() => {
      db.exec('INSERT INTO t1 (a, b) VALUES (-1, \'neg\')');
    });

    it('should handle negative literal: WHERE -1 IN (-1)', () => {
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE -1 IN (-1)').all();

      expect(result.length).toBe(5); // 4 original + 1 new row
    });

    it('should filter negative column values: WHERE a IN (-1)', () => {
      const result = db.prepare('SELECT a FROM t1 WHERE a IN (-1)').all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: -1 });
    });

    it('should handle mixed positive and negative: WHERE a IN (-1, 1, 2)', () => {
      const result = db.prepare('SELECT a FROM t1 WHERE a IN (-1, 1, 2)').all();

      const sorted = (result as { a: number }[]).sort((a, b) => a.a - b.a);
      expect(sorted.length).toBe(3);
      expect(sorted.map(r => r.a)).toEqual([-1, 1, 2]);
    });
  });
});
