/**
 * SQLLogicTest in2.test Compatibility Tests
 *
 * Tests for specific edge cases from SQLLogicTest in2.test:
 * - Line 139: NULL NOT IN () - empty list with NULL should return TRUE
 * - Line 296: 1 IN (SELECT 1) - IN with scalar subquery
 *
 * Run with: cd packages/dosql && pnpm test src/__tests__/sqllogictest-in2.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Database } from '../database.js';

describe('SQLLogicTest in2.test - IN Edge Cases', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
    // Create table t1 exactly as in the SQLLogicTest
    db.exec('CREATE TABLE t1 (x INTEGER, y TEXT)');
    db.exec("INSERT INTO t1 VALUES (1, 'true')");
    db.exec("INSERT INTO t1 VALUES (0, 'false')");
    db.exec("INSERT INTO t1 VALUES (NULL, 'NULL')");
  });

  // ===========================================================================
  // EMPTY LIST EDGE CASES (Lines 82-144 of in2.test)
  // ===========================================================================

  describe('Empty list behavior (R-52275-55503)', () => {
    // R-52275-55503: When the right operand is an empty set, the result of IN is
    // false and the result of NOT IN is true, regardless of the left operand and
    // even if the left operand is NULL.

    it('should return false for 1 IN () - line 82', () => {
      const result = db.prepare('SELECT 1 FROM t1 WHERE 1 IN ()').all();
      expect(result.length).toBe(0);
    });

    it('should return false for 1.0 IN () - line 89', () => {
      const result = db.prepare('SELECT 1 FROM t1 WHERE 1.0 IN ()').all();
      expect(result.length).toBe(0);
    });

    it("should return false for '1' IN () - line 96", () => {
      const result = db.prepare("SELECT 1 FROM t1 WHERE '1' IN ()").all();
      expect(result.length).toBe(0);
    });

    it('should return false for NULL IN () - line 103', () => {
      // Critical edge case: NULL IN empty set returns FALSE (not NULL)
      const result = db.prepare('SELECT 1 FROM t1 WHERE NULL IN ()').all();
      expect(result.length).toBe(0);
    });

    it('should return true for 1 NOT IN () - line 110 (3 rows)', () => {
      const result = db.prepare('SELECT 1 FROM t1 WHERE 1 NOT IN ()').all();
      expect(result.length).toBe(3);
    });

    it('should return true for 1.0 NOT IN () - line 119 (3 rows)', () => {
      const result = db.prepare('SELECT 1 FROM t1 WHERE 1.0 NOT IN ()').all();
      expect(result.length).toBe(3);
    });

    it("should return true for '1' NOT IN () - line 129 (3 rows)", () => {
      const result = db.prepare("SELECT 1 FROM t1 WHERE '1' NOT IN ()").all();
      expect(result.length).toBe(3);
    });

    it('should return true for NULL NOT IN () - line 139 (3 rows)', () => {
      // CRITICAL EDGE CASE: NULL NOT IN empty set returns TRUE
      // This is the key fix - empty list behavior overrides NULL semantics
      const result = db.prepare('SELECT 1 FROM t1 WHERE NULL NOT IN ()').all();
      expect(result.length).toBe(3);
    });
  });

  // ===========================================================================
  // IN SUBQUERY SUPPORT (Lines 296-313 of in2.test)
  // ===========================================================================

  describe('IN with scalar subquery (R-35033-20570)', () => {
    // R-35033-20570: The subquery on the right of an IN or NOT IN operator must
    // be a scalar subquery if the left expression is not a row value expression.
    //
    // NOTE: Literal IN with subquery (e.g., 1 IN (SELECT 1)) is not yet fully supported
    // in InMemoryEngine. The engine currently handles column IN (SELECT ...) for cases
    // where the left operand is a column reference. Literal IN with subquery support
    // requires additional work to handle IN expressions where the left side is a
    // constant value.
    //
    // The subquery parser (SubqueryParser) correctly parses these queries.
    // The limitation is in the InMemoryEngine's WHERE clause evaluation.

    it.skip('should support 1 IN (SELECT 1) - line 296 (3 rows)', () => {
      // This requires subquery materialization and IN evaluation for literal operands
      // TODO: Implement literal IN (SELECT ...) support in InMemoryEngine
      const result = db.prepare('SELECT 1 FROM t1 WHERE 1 IN (SELECT 1)').all();
      expect(result.length).toBe(3);
    });

    it.skip('should error on 1 IN (SELECT 1, 2) - line 304', () => {
      // Scalar subquery must return single column
      // TODO: Validate subquery column count at parse/prepare time
      expect(() => {
        db.prepare('SELECT 1 FROM t1 WHERE 1 IN (SELECT 1, 2)');
      }).toThrow();
    });

    it.skip('should error on 1 IN (SELECT x, y FROM t1) - line 307', () => {
      // Multi-column subquery not allowed for scalar IN
      // TODO: Validate subquery column count at parse/prepare time
      expect(() => {
        db.prepare('SELECT 1 FROM t1 WHERE 1 IN (SELECT x, y FROM t1)');
      }).toThrow();
    });

    it.skip('should error on 1 IN (SELECT * FROM t1) - line 310', () => {
      // SELECT * returns multiple columns
      // TODO: Validate subquery column count at parse/prepare time
      expect(() => {
        db.prepare('SELECT 1 FROM t1 WHERE 1 IN (SELECT * FROM t1)');
      }).toThrow();
    });
  });

  // ===========================================================================
  // IN WITH NULL IN LIST (Lines 149-239 of in2.test)
  // ===========================================================================

  describe('IN with NULL in list', () => {
    it('should return TRUE when value found (NULL, 1) - line 149', () => {
      const result = db.prepare('SELECT 1 FROM t1 WHERE 1 IN (NULL, 1)').all();
      expect(result.length).toBe(3);
    });

    it('should return FALSE for 1 NOT IN (NULL, 1) - line 170', () => {
      // When value IS in list, NOT IN returns FALSE
      const result = db.prepare('SELECT 1 FROM t1 WHERE 1 NOT IN (NULL, 1)').all();
      expect(result.length).toBe(0);
    });

    it('should return FALSE (NULL) when value not in list with NULL - line 218', () => {
      // When value not found but NULL in list, result is NULL (falsy)
      const result = db.prepare('SELECT 1 FROM t1 WHERE 1 IN (NULL, 2)').all();
      expect(result.length).toBe(0);
    });

    it('should return FALSE (NULL) for NOT IN with NULL when not matched - line 230', () => {
      // NOT IN with NULL returns NULL (falsy) when value not definitively in list
      const result = db.prepare('SELECT 1 FROM t1 WHERE 1 NOT IN (NULL, 2)').all();
      expect(result.length).toBe(0);
    });
  });

  // ===========================================================================
  // NULL LEFT OPERAND (Lines 244-289 of in2.test)
  // ===========================================================================

  describe('NULL as left operand', () => {
    it('should return FALSE (NULL) for NULL IN (1) - line 245', () => {
      const result = db.prepare('SELECT 1 FROM t1 WHERE NULL IN (1)').all();
      expect(result.length).toBe(0);
    });

    it('should return FALSE (NULL) for NULL NOT IN (1) - line 257', () => {
      // NULL NOT IN non-empty list is NULL (falsy)
      const result = db.prepare('SELECT 1 FROM t1 WHERE NULL NOT IN (1)').all();
      expect(result.length).toBe(0);
    });

    it('should return FALSE (NULL) for NULL IN (NULL, 1) - line 269', () => {
      const result = db.prepare('SELECT 1 FROM t1 WHERE NULL IN (NULL, 1)').all();
      expect(result.length).toBe(0);
    });

    it('should return FALSE (NULL) for NULL NOT IN (NULL, 1) - line 281', () => {
      const result = db.prepare('SELECT 1 FROM t1 WHERE NULL NOT IN (NULL, 1)').all();
      expect(result.length).toBe(0);
    });
  });
});
