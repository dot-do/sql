/**
 * SQLLogicTest NOT IN Operator Compatibility Tests
 *
 * Tests for NOT IN operator in DoSQL's InMemoryEngine.
 *
 * ALL FEATURES WORKING:
 * - Column references: SELECT * FROM t1 WHERE a NOT IN (1, 2, 3)
 * - Literal as left operand: SELECT 1 FROM t WHERE 1 NOT IN (2)
 * - Empty list: NOT IN ()
 * - Case-sensitive strings and special characters (escaped quotes)
 * - NULL handling in columns and lists (proper SQL NULL semantics)
 * - Subquery support: NOT IN (SELECT ...)
 * - Combined with AND/OR/BETWEEN/LIKE
 * - NOT (expr IN (...)) syntax (parenthesized negation)
 * - NOT IN in SELECT expression (non-WHERE usage)
 * - Edge cases (zero, negative, large lists, duplicates)
 * - Parameter binding support
 * - Float and mixed integer/float comparisons
 *
 * Run with: npx vitest run src/__tests__/sqllogictest-not-in.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Database } from '../database.js';

// =============================================================================
// TEST SETUP
// =============================================================================

describe('SQLLogicTest NOT IN Operator', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  // ===========================================================================
  // NOT IN WITH INTEGER VALUES
  // ===========================================================================

  describe('NOT IN with Integer Values', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (a INTEGER)');
      db.exec('INSERT INTO t1 (a) VALUES (1)');
      db.exec('INSERT INTO t1 (a) VALUES (2)');
      db.exec('INSERT INTO t1 (a) VALUES (3)');
      db.exec('INSERT INTO t1 (a) VALUES (4)');
      db.exec('INSERT INTO t1 (a) VALUES (5)');
    });

    /**
     * KNOWN FAILURE: Basic NOT IN with integer literal
     *
     * SQLLogicTest: SELECT 1 FROM t1 WHERE 1 NOT IN (2)
     * Expected: Returns 1 for each row since 1 is NOT IN (2)
     * Current: NOT IN filtering works, but SELECT expression returns null for literal
     */
    it('should handle NOT IN with single integer value - literal comparison', () => {
      // SELECT 1 FROM t1 WHERE 1 NOT IN (2)
      // 1 is not equal to 2, so condition is true for all rows
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 1 NOT IN (2)').all();

      // Should return 5 rows (one for each row in t1)
      expect(result.length).toBe(5);
      expect(result[0]).toEqual({ result: 1 });
    });

    /**
     * KNOWN FAILURE: NOT IN with column reference
     *
     * SQLLogicTest: SELECT * FROM t1 WHERE a NOT IN (1, 2, 3)
     * Expected: Returns rows where a = 4 or a = 5
     * Current: NOT IN operator not implemented
     */
    it('should handle NOT IN with column reference', () => {
      // SELECT * FROM t1 WHERE a NOT IN (1, 2, 3)
      const result = db.prepare('SELECT a FROM t1 WHERE a NOT IN (1, 2, 3)').all();

      const sorted = (result as { a: number }[]).sort((a, b) => a.a - b.a);

      expect(sorted.length).toBe(2);
      expect(sorted[0]).toEqual({ a: 4 });
      expect(sorted[1]).toEqual({ a: 5 });
    });

    /**
     * KNOWN FAILURE: NOT IN with single value in list
     */
    it('should handle NOT IN with single value in list', () => {
      // SELECT a FROM t1 WHERE a NOT IN (3)
      const result = db.prepare('SELECT a FROM t1 WHERE a NOT IN (3)').all();

      const sorted = (result as { a: number }[]).sort((a, b) => a.a - b.a);

      expect(sorted.length).toBe(4);
      expect(sorted.map((r) => r.a)).toEqual([1, 2, 4, 5]);
    });

    /**
     * KNOWN FAILURE: NOT IN with all values excluded
     */
    it('should handle NOT IN excluding all values', () => {
      // SELECT a FROM t1 WHERE a NOT IN (1, 2, 3, 4, 5)
      const result = db.prepare('SELECT a FROM t1 WHERE a NOT IN (1, 2, 3, 4, 5)').all();

      expect(result.length).toBe(0);
    });

    /**
     * WORKING: NOT IN with no matching exclusions
     * This test passes - NOT IN works when no values match the exclusion list
     */
    it('should handle NOT IN with no matching exclusions', () => {
      // SELECT a FROM t1 WHERE a NOT IN (10, 20, 30)
      const result = db.prepare('SELECT a FROM t1 WHERE a NOT IN (10, 20, 30)').all();

      expect(result.length).toBe(5);
    });

    /**
     * KNOWN FAILURE: NOT IN with parameter binding
     */
    it('should handle NOT IN with parameter binding', () => {
      // Use parameter binding for NOT IN values
      const result = db.prepare('SELECT a FROM t1 WHERE a NOT IN (?, ?, ?)').all(1, 2, 3);

      const sorted = (result as { a: number }[]).sort((a, b) => a.a - b.a);

      expect(sorted.length).toBe(2);
      expect(sorted.map((r) => r.a)).toEqual([4, 5]);
    });
  });

  // ===========================================================================
  // NOT IN WITH FLOAT VALUES
  // ===========================================================================

  describe('NOT IN with Float Values', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE floats (id INTEGER, value REAL)');
      db.exec('INSERT INTO floats (id, value) VALUES (1, 1.0)');
      db.exec('INSERT INTO floats (id, value) VALUES (2, 2.0)');
      db.exec('INSERT INTO floats (id, value) VALUES (3, 3.0)');
      db.exec('INSERT INTO floats (id, value) VALUES (4, 4.5)');
      db.exec('INSERT INTO floats (id, value) VALUES (5, 5.5)');
    });

    /**
     * WORKING: NOT IN with float literals
     *
     * SQLLogicTest: SELECT 1 FROM t1 WHERE 1.0 NOT IN (2.0)
     * This test passes - float literal comparison works
     */
    it('should handle NOT IN with float literal comparison', () => {
      // SELECT 1 FROM floats WHERE 1.0 NOT IN (2.0)
      const result = db.prepare('SELECT 1 AS result FROM floats WHERE 1.0 NOT IN (2.0)').all();

      expect(result.length).toBe(5);
    });

    /**
     * KNOWN FAILURE: NOT IN with REAL column
     */
    it('should handle NOT IN with REAL column', () => {
      // SELECT * FROM floats WHERE value NOT IN (1.0, 2.0, 3.0)
      const result = db.prepare('SELECT id, value FROM floats WHERE value NOT IN (1.0, 2.0, 3.0)').all();

      const sorted = (result as { id: number; value: number }[]).sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(2);
      expect(sorted[0]).toEqual({ id: 4, value: 4.5 });
      expect(sorted[1]).toEqual({ id: 5, value: 5.5 });
    });

    /**
     * KNOWN FAILURE: NOT IN with mixed integer and float
     */
    it('should handle NOT IN with mixed integer and float', () => {
      // SELECT * FROM floats WHERE value NOT IN (1, 2, 3)
      // Integer literals should compare correctly with REAL column
      const result = db.prepare('SELECT id, value FROM floats WHERE value NOT IN (1, 2, 3)').all();

      const sorted = (result as { id: number; value: number }[]).sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(2);
      expect(sorted.map((r) => r.value)).toEqual([4.5, 5.5]);
    });

    /**
     * WORKING: NOT IN with float precision edge case
     * This test passes - float precision is handled correctly
     */
    it('should handle NOT IN with float precision', () => {
      db.exec('INSERT INTO floats (id, value) VALUES (6, 1.0000001)');

      // 1.0000001 should NOT be in (1.0) due to precision difference
      const result = db.prepare('SELECT id FROM floats WHERE value NOT IN (1.0)').all();

      const ids = (result as { id: number }[]).map((r) => r.id).sort((a, b) => a - b);

      // All rows except id=1 (value=1.0)
      expect(ids).toContain(6); // 1.0000001 is not exactly 1.0
    });
  });

  // ===========================================================================
  // NOT IN WITH STRING VALUES
  // ===========================================================================

  describe('NOT IN with String Values', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE strings (id INTEGER, name TEXT)');
      db.exec("INSERT INTO strings (id, name) VALUES (1, 'apple')");
      db.exec("INSERT INTO strings (id, name) VALUES (2, 'banana')");
      db.exec("INSERT INTO strings (id, name) VALUES (3, 'cherry')");
      db.exec("INSERT INTO strings (id, name) VALUES (4, 'date')");
      db.exec("INSERT INTO strings (id, name) VALUES (5, 'elderberry')");
    });

    /**
     * WORKING: NOT IN with string literals
     *
     * SQLLogicTest: SELECT 1 FROM t1 WHERE '1' NOT IN ('2')
     * This test passes - string literal comparison works
     */
    it('should handle NOT IN with string literal comparison', () => {
      // SELECT 1 FROM strings WHERE '1' NOT IN ('2')
      const result = db.prepare("SELECT 1 AS result FROM strings WHERE '1' NOT IN ('2')").all();

      expect(result.length).toBe(5);
    });

    /**
     * KNOWN FAILURE: NOT IN with TEXT column
     */
    it('should handle NOT IN with TEXT column', () => {
      // SELECT * FROM strings WHERE name NOT IN ('apple', 'banana')
      const result = db.prepare("SELECT id, name FROM strings WHERE name NOT IN ('apple', 'banana')").all();

      const sorted = (result as { id: number; name: string }[]).sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(3);
      expect(sorted.map((r) => r.name)).toEqual(['cherry', 'date', 'elderberry']);
    });

    /**
     * WORKING: NOT IN with case-sensitive string comparison
     * This test passes - case-sensitive comparison works correctly
     */
    it('should handle NOT IN with case-sensitive strings', () => {
      // 'Apple' is different from 'apple' in case-sensitive comparison
      const result = db.prepare("SELECT id, name FROM strings WHERE name NOT IN ('Apple', 'Banana')").all();

      // All 5 rows should be returned since 'Apple' != 'apple'
      expect(result.length).toBe(5);
    });

    /**
     * KNOWN FAILURE: NOT IN with empty string
     */
    it('should handle NOT IN with empty string', () => {
      db.exec("INSERT INTO strings (id, name) VALUES (6, '')");

      const result = db.prepare("SELECT id FROM strings WHERE name NOT IN ('')").all();

      // All rows except id=6 (empty string)
      expect(result.length).toBe(5);
    });

    /**
     * KNOWN FAILURE: NOT IN with special characters
     */
    it('should handle NOT IN with special characters', () => {
      db.exec("INSERT INTO strings (id, name) VALUES (6, 'it''s')");

      const result = db.prepare("SELECT id FROM strings WHERE name NOT IN ('it''s')").all();

      expect(result.length).toBe(5);
    });
  });

  // ===========================================================================
  // NOT IN WITH EMPTY LIST
  // ===========================================================================

  describe('NOT IN with Empty List', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (a INTEGER)');
      db.exec('INSERT INTO t1 (a) VALUES (1)');
      db.exec('INSERT INTO t1 (a) VALUES (2)');
      db.exec('INSERT INTO t1 (a) VALUES (3)');
    });

    /**
     * WORKING: NOT IN with empty list
     *
     * SQLLogicTest: SELECT 1 FROM t1 WHERE 1 NOT IN ()
     * This test passes - empty NOT IN () returns TRUE for all rows (SQLite compatible)
     */
    it('should handle NOT IN with empty list (SQLite compatible)', () => {
      // SELECT * FROM t1 WHERE a NOT IN ()
      // In SQLite, empty NOT IN () returns TRUE for all rows
      const result = db.prepare('SELECT a FROM t1 WHERE a NOT IN ()').all();

      // All 3 rows should be returned
      expect(result.length).toBe(3);
    });

    /**
     * WORKING: Literal NOT IN empty list
     * This test passes - literal NOT IN () works correctly
     */
    it('should handle literal NOT IN empty list', () => {
      // SELECT 1 FROM t1 WHERE 1 NOT IN ()
      const result = db.prepare('SELECT 1 AS result FROM t1 WHERE 1 NOT IN ()').all();

      expect(result.length).toBe(3);
    });
  });

  // ===========================================================================
  // NOT IN WITH NULL VALUES
  // ===========================================================================

  describe('NOT IN with NULL Values', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE nullable (id INTEGER, value INTEGER)');
      db.exec('INSERT INTO nullable (id, value) VALUES (1, 10)');
      db.exec('INSERT INTO nullable (id, value) VALUES (2, 20)');
      db.exec('INSERT INTO nullable (id, value) VALUES (3, NULL)');
      db.exec('INSERT INTO nullable (id, value) VALUES (4, 30)');
      db.exec('INSERT INTO nullable (id, value) VALUES (5, NULL)');
    });

    /**
     * KNOWN FAILURE: NOT IN with NULL in column
     *
     * SQL Standard: value NOT IN (list) returns NULL when:
     * - value is NULL, OR
     * - value is not in list AND list contains NULL
     *
     * Only returns TRUE when value is definitely not in list
     * and list does not contain NULL.
     */
    it('should return rows where non-NULL value is NOT IN list without NULL', () => {
      // SELECT * FROM nullable WHERE value NOT IN (10, 20)
      // Should return row 4 (value=30)
      // Row 3 and 5 have NULL values, which produce NULL in comparison
      const result = db.prepare('SELECT id, value FROM nullable WHERE value NOT IN (10, 20)').all();

      // Only row 4 should be returned (value=30 is not in list)
      // Rows with NULL values should not match (NULL NOT IN (10,20) is NULL, not TRUE)
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 4, value: 30 });
    });

    /**
     * KNOWN FAILURE: NOT IN with NULL in list
     *
     * When NULL is in the list, NOT IN always returns NULL or FALSE
     * because we cannot determine if a value is definitely not in a list
     * that contains an unknown (NULL) value.
     */
    it('should handle NOT IN with NULL in the list', () => {
      // SELECT * FROM nullable WHERE value NOT IN (10, NULL)
      // This should return 0 rows because:
      // - For non-10 values: we can't know if they're NOT IN because NULL is unknown
      // - For 10: it's IN the list
      // - For NULL values: NULL comparison with NULL in list is NULL
      const result = db.prepare('SELECT id, value FROM nullable WHERE value NOT IN (10, NULL)').all();

      // Standard SQL: no rows should be returned
      expect(result.length).toBe(0);
    });

    /**
     * KNOWN FAILURE: NOT IN excluding NULL from results
     */
    it('should not return NULL values in NOT IN results', () => {
      // SELECT * FROM nullable WHERE value NOT IN (10)
      // NULL values should not match because NULL NOT IN (anything) is NULL
      const result = db.prepare('SELECT id, value FROM nullable WHERE value NOT IN (10)').all();

      const sorted = (result as { id: number; value: number | null }[]).sort((a, b) => a.id - b.id);

      // Only non-NULL values that are not 10
      expect(sorted.length).toBe(2);
      expect(sorted.map((r) => r.id)).toEqual([2, 4]);
      expect(sorted.map((r) => r.value)).toEqual([20, 30]);
    });

    /**
     * KNOWN FAILURE: NOT IN with explicit NULL comparison
     */
    it('should handle NOT IN with explicit NULL in list', () => {
      db.exec('CREATE TABLE t2 (a INTEGER)');
      db.exec('INSERT INTO t2 (a) VALUES (1)');
      db.exec('INSERT INTO t2 (a) VALUES (2)');
      db.exec('INSERT INTO t2 (a) VALUES (3)');

      // SELECT * FROM t2 WHERE a NOT IN (1, NULL)
      // No rows should match due to NULL in list
      const result = db.prepare('SELECT a FROM t2 WHERE a NOT IN (1, NULL)').all();

      expect(result.length).toBe(0);
    });

    /**
     * KNOWN FAILURE: IS NOT NULL combined with NOT IN
     */
    it('should handle IS NOT NULL combined with NOT IN', () => {
      // SELECT * FROM nullable WHERE value IS NOT NULL AND value NOT IN (10, 20)
      const result = db.prepare('SELECT id, value FROM nullable WHERE value IS NOT NULL AND value NOT IN (10, 20)').all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 4, value: 30 });
    });
  });

  // ===========================================================================
  // NOT IN WITH SUBQUERY
  // ===========================================================================

  describe('NOT IN with Subquery', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE users (id INTEGER, name TEXT, department_id INTEGER)');
      db.exec("INSERT INTO users (id, name, department_id) VALUES (1, 'Alice', 1)");
      db.exec("INSERT INTO users (id, name, department_id) VALUES (2, 'Bob', 2)");
      db.exec("INSERT INTO users (id, name, department_id) VALUES (3, 'Charlie', 1)");
      db.exec("INSERT INTO users (id, name, department_id) VALUES (4, 'Diana', 3)");
      db.exec("INSERT INTO users (id, name, department_id) VALUES (5, 'Eve', NULL)");

      db.exec('CREATE TABLE active_departments (dept_id INTEGER)');
      db.exec('INSERT INTO active_departments (dept_id) VALUES (1)');
      db.exec('INSERT INTO active_departments (dept_id) VALUES (2)');
    });

    /**
     * KNOWN FAILURE: NOT IN with subquery
     *
     * Subquery support for NOT IN requires the query engine to:
     * 1. Execute the subquery
     * 2. Collect results into a set
     * 3. Check each row against the set
     */
    it('should handle NOT IN with subquery', () => {
      // SELECT * FROM users WHERE department_id NOT IN (SELECT dept_id FROM active_departments)
      const result = db
        .prepare('SELECT id, name, department_id FROM users WHERE department_id NOT IN (SELECT dept_id FROM active_departments)')
        .all();

      // Should return Diana (dept 3) - Eve has NULL department_id
      // Users in dept 1 (Alice, Charlie) and dept 2 (Bob) should be excluded
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 4, name: 'Diana', department_id: 3 });
    });

    /**
     * WORKING: NOT IN with correlated subquery (subquery support added)
     */
    it('should handle NOT IN with correlated subquery', () => {
      db.exec('CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)');
      db.exec('INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)');
      db.exec('INSERT INTO orders (id, user_id, amount) VALUES (2, 1, 200)');
      db.exec('INSERT INTO orders (id, user_id, amount) VALUES (3, 3, 150)');

      // Users with no orders
      // SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)
      const result = db.prepare('SELECT id, name FROM users WHERE id NOT IN (SELECT user_id FROM orders)').all();

      const sorted = (result as { id: number; name: string }[]).sort((a, b) => a.id - b.id);

      // Bob (2), Diana (4), Eve (5) have no orders
      expect(sorted.length).toBe(3);
      expect(sorted.map((r) => r.name)).toEqual(['Bob', 'Diana', 'Eve']);
    });

    /**
     * KNOWN FAILURE: NOT IN with subquery returning NULL
     */
    it('should handle NOT IN with subquery containing NULL', () => {
      db.exec('CREATE TABLE dept_list (dept_id INTEGER)');
      db.exec('INSERT INTO dept_list (dept_id) VALUES (1)');
      db.exec('INSERT INTO dept_list (dept_id) VALUES (NULL)');

      // When subquery returns NULL, NOT IN should return no rows
      // (or only rows where column IS NULL in some interpretations)
      const result = db.prepare('SELECT id, name FROM users WHERE department_id NOT IN (SELECT dept_id FROM dept_list)').all();

      // Standard SQL behavior: no rows should match because NULL is in subquery result
      expect(result.length).toBe(0);
    });

    /**
     * WORKING: NOT IN with empty subquery result
     * Note: When subquery is empty, NOT IN () returns TRUE for all rows (including NULL)
     */
    it('should handle NOT IN with empty subquery result', () => {
      db.exec('CREATE TABLE empty_table (dept_id INTEGER)');

      // SELECT * FROM users WHERE department_id NOT IN (SELECT dept_id FROM empty_table)
      const result = db.prepare('SELECT id, name FROM users WHERE department_id NOT IN (SELECT dept_id FROM empty_table)').all();

      // All rows match because empty list: x NOT IN () is TRUE for all x
      expect(result.length).toBe(5);
    });
  });

  // ===========================================================================
  // NOT IN COMBINED WITH OTHER OPERATORS
  // ===========================================================================

  describe('NOT IN Combined with Other Operators', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE products (id INTEGER, name TEXT, category TEXT, price INTEGER)');
      db.exec("INSERT INTO products (id, name, category, price) VALUES (1, 'Widget', 'A', 100)");
      db.exec("INSERT INTO products (id, name, category, price) VALUES (2, 'Gadget', 'B', 200)");
      db.exec("INSERT INTO products (id, name, category, price) VALUES (3, 'Gizmo', 'A', 150)");
      db.exec("INSERT INTO products (id, name, category, price) VALUES (4, 'Thing', 'C', 300)");
      db.exec("INSERT INTO products (id, name, category, price) VALUES (5, 'Stuff', 'B', 250)");
    });

    /**
     * KNOWN FAILURE: NOT IN with AND condition
     */
    it('should handle NOT IN combined with AND', () => {
      // SELECT * FROM products WHERE category NOT IN ('A', 'B') AND price > 100
      const result = db.prepare("SELECT id, name FROM products WHERE category NOT IN ('A', 'B') AND price > 100").all();

      // Only category C with price > 100 = Thing
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 4, name: 'Thing' });
    });

    /**
     * KNOWN FAILURE: NOT IN with OR condition
     */
    it('should handle NOT IN combined with OR', () => {
      // SELECT * FROM products WHERE category NOT IN ('A') OR price > 250
      const result = db.prepare("SELECT id, name FROM products WHERE category NOT IN ('A') OR price > 250").all();

      const sorted = (result as { id: number; name: string }[]).sort((a, b) => a.id - b.id);

      // category not A: Gadget, Thing, Stuff
      // OR price > 250: Thing
      // Union: Gadget, Thing, Stuff
      expect(sorted.length).toBe(3);
      expect(sorted.map((r) => r.name)).toEqual(['Gadget', 'Thing', 'Stuff']);
    });

    /**
     * KNOWN FAILURE: Nested NOT IN conditions
     */
    it('should handle nested NOT and IN conditions', () => {
      // SELECT * FROM products WHERE NOT (category IN ('A', 'B'))
      // This is equivalent to NOT IN
      const result = db.prepare("SELECT id, name FROM products WHERE NOT (category IN ('A', 'B'))").all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 4, name: 'Thing' });
    });

    /**
     * KNOWN FAILURE: NOT IN with BETWEEN
     */
    it('should handle NOT IN combined with BETWEEN', () => {
      // SELECT * FROM products WHERE category NOT IN ('C') AND price BETWEEN 100 AND 200
      const result = db
        .prepare("SELECT id, name, price FROM products WHERE category NOT IN ('C') AND price BETWEEN 100 AND 200")
        .all();

      const sorted = (result as { id: number; name: string; price: number }[]).sort((a, b) => a.id - b.id);

      // Category A or B, price 100-200: Widget, Gadget, Gizmo
      expect(sorted.length).toBe(3);
      expect(sorted.map((r) => r.name)).toEqual(['Widget', 'Gadget', 'Gizmo']);
    });

    /**
     * WORKING: NOT IN with LIKE
     * This test passes - NOT IN combined with LIKE works correctly
     */
    it('should handle NOT IN combined with LIKE', () => {
      // SELECT * FROM products WHERE category NOT IN ('C') AND name LIKE 'G%'
      const result = db.prepare("SELECT id, name FROM products WHERE category NOT IN ('C') AND name LIKE 'G%'").all();

      const sorted = (result as { id: number; name: string }[]).sort((a, b) => a.id - b.id);

      // Category not C, name starts with G: Gadget, Gizmo
      expect(sorted.length).toBe(2);
      expect(sorted.map((r) => r.name)).toEqual(['Gadget', 'Gizmo']);
    });
  });

  // ===========================================================================
  // NOT IN EDGE CASES
  // ===========================================================================

  describe('NOT IN Edge Cases', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE edge (id INTEGER, value INTEGER)');
      db.exec('INSERT INTO edge (id, value) VALUES (1, 0)');
      db.exec('INSERT INTO edge (id, value) VALUES (2, -1)');
      db.exec('INSERT INTO edge (id, value) VALUES (3, 1)');
    });

    /**
     * KNOWN FAILURE: NOT IN with zero
     */
    it('should handle NOT IN with zero value', () => {
      const result = db.prepare('SELECT id FROM edge WHERE value NOT IN (0)').all();

      const ids = (result as { id: number }[]).map((r) => r.id).sort((a, b) => a - b);

      expect(ids).toEqual([2, 3]);
    });

    /**
     * KNOWN FAILURE: NOT IN with negative numbers
     */
    it('should handle NOT IN with negative numbers', () => {
      const result = db.prepare('SELECT id FROM edge WHERE value NOT IN (-1, 0)').all();

      expect(result.length).toBe(1);
      expect((result[0] as { id: number }).id).toBe(3);
    });

    /**
     * KNOWN FAILURE: NOT IN with large list
     */
    it('should handle NOT IN with large list of values', () => {
      // Create a larger dataset
      for (let i = 4; i <= 100; i++) {
        db.exec(`INSERT INTO edge (id, value) VALUES (${i}, ${i})`);
      }

      // Exclude values 1-50
      const excludeList = Array.from({ length: 50 }, (_, i) => i + 1).join(', ');
      const result = db.prepare(`SELECT id FROM edge WHERE value NOT IN (${excludeList})`).all();

      // Should return: 0, -1, and 51-100
      // id 1 has value 0 (not in list)
      // id 2 has value -1 (not in list)
      // ids 51-100 have values 51-100 (not in list)
      expect(result.length).toBe(52);
    });

    /**
     * KNOWN FAILURE: NOT IN with duplicate values in list
     */
    it('should handle NOT IN with duplicate values in list', () => {
      const result = db.prepare('SELECT id FROM edge WHERE value NOT IN (0, 0, 0)').all();

      const ids = (result as { id: number }[]).map((r) => r.id).sort((a, b) => a - b);

      // Same as NOT IN (0)
      expect(ids).toEqual([2, 3]);
    });

    /**
     * KNOWN FAILURE: NOT IN in SELECT expression (not WHERE)
     */
    it('should handle NOT IN in SELECT expression', () => {
      const result = db.prepare('SELECT id, value NOT IN (0, 1) AS not_in_result FROM edge').all();

      const sorted = (result as { id: number; not_in_result: number }[]).sort((a, b) => a.id - b.id);

      // id 1: value=0, 0 NOT IN (0,1) = FALSE (0)
      // id 2: value=-1, -1 NOT IN (0,1) = TRUE (1)
      // id 3: value=1, 1 NOT IN (0,1) = FALSE (0)
      expect(sorted[0]).toEqual({ id: 1, not_in_result: 0 });
      expect(sorted[1]).toEqual({ id: 2, not_in_result: 1 });
      expect(sorted[2]).toEqual({ id: 3, not_in_result: 0 });
    });
  });

  // ===========================================================================
  // NOT BETWEEN and BETWEEN WITH EXPRESSIONS
  // ===========================================================================

  describe('NOT BETWEEN and BETWEEN with Expressions', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)');
      db.exec('INSERT INTO t1 (a, b, c, d, e) VALUES (100, 50, 60, 120, 55)');
      db.exec('INSERT INTO t1 (a, b, c, d, e) VALUES (110, 50, 60, 130, 65)');
      db.exec('INSERT INTO t1 (a, b, c, d, e) VALUES (140, 50, 60, 100, 70)');
      db.exec('INSERT INTO t1 (a, b, c, d, e) VALUES (160, 50, 60, 110, 80)');
    });

    it('should handle NOT BETWEEN with literal bounds', () => {
      // d NOT BETWEEN 110 AND 150 means d < 110 OR d > 150
      const result = db.prepare('SELECT a, d FROM t1 WHERE d NOT BETWEEN 110 AND 150').all();

      const sorted = (result as { a: number; d: number }[]).sort((a, b) => a.a - b.a);

      // d=100 (< 110, so NOT BETWEEN), d=110 (in range, not matching)
      expect(sorted.length).toBe(1);
      expect(sorted[0]).toEqual({ a: 140, d: 100 });
    });

    it('should handle BETWEEN with expression bounds', () => {
      // c BETWEEN b-2 AND d+2 where b=50 means c BETWEEN 48 AND d+2
      // With b=50, c=60, d=120: 60 BETWEEN 48 AND 122 = true
      // With b=50, c=60, d=130: 60 BETWEEN 48 AND 132 = true
      // With b=50, c=60, d=100: 60 BETWEEN 48 AND 102 = true
      // With b=50, c=60, d=110: 60 BETWEEN 48 AND 112 = true
      const result = db.prepare('SELECT a, c, d FROM t1 WHERE c BETWEEN b-2 AND d+2').all();

      // All rows should match since 60 is between 48 and at least 102
      expect(result.length).toBe(4);
    });

    it('should handle complex OR with NOT BETWEEN', () => {
      // d NOT BETWEEN 110 AND 150 OR c BETWEEN b-2 AND d+2
      // First condition: d < 110 OR d > 150 (row with d=100 matches)
      // Second condition: c BETWEEN 48 AND d+2 (all rows match since c=60)
      const result = db.prepare(
        'SELECT a, d FROM t1 WHERE d NOT BETWEEN 110 AND 150 OR c BETWEEN b-2 AND d+2'
      ).all();

      // All rows should match due to the second OR condition
      expect(result.length).toBe(4);
    });

    it('should handle OR with multiple conditions', () => {
      // d NOT BETWEEN 110 AND 150 OR e < d
      // Row 1: d=120 (in range, NOT true), e=55 < d=120 TRUE -> TRUE
      // Row 2: d=130 (in range, NOT true), e=65 < d=130 TRUE -> TRUE
      // Row 3: d=100 (NOT in range) -> TRUE
      // Row 4: d=110 (in range, NOT true), e=80 < d=110 TRUE -> TRUE
      const result = db.prepare(
        'SELECT a, d, e FROM t1 WHERE d NOT BETWEEN 110 AND 150 OR e < d'
      ).all();

      // All rows should match
      expect(result.length).toBe(4);
    });

    it('should handle NOT BETWEEN in AND condition', () => {
      // d NOT BETWEEN 110 AND 150 AND e > 60
      // d=100 (NOT BETWEEN) AND e=70 > 60 -> TRUE (row with a=140)
      const result = db.prepare('SELECT a, d, e FROM t1 WHERE d NOT BETWEEN 110 AND 150 AND e > 60').all();

      expect(result.length).toBe(1);
      expect((result[0] as { a: number }).a).toBe(140);
    });
  });
});
