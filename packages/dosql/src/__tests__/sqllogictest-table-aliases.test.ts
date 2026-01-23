/**
 * SQLLogicTest Table Aliases - RED Phase TDD Tests
 *
 * These tests document expected behavior for table aliases that currently fail.
 * SQLLogicTest shows failures for queries using table aliases with AS keyword,
 * implicit aliases, self-joins, and alias references in WHERE/SELECT clauses.
 *
 * RED PHASE: All tests marked with it() are expected to fail initially.
 * GREEN PHASE: Implement fixes until tests pass, then remove it().
 *
 * Run with: npx vitest run src/__tests__/sqllogictest-table-aliases.test.ts
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

describe('SQLLogicTest Table Aliases', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  // ===========================================================================
  // AS KEYWORD FOR TABLE ALIAS
  // ===========================================================================

  describe('AS keyword for table alias', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (a INTEGER, b INTEGER, c TEXT)
      `);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (1, 10, 'one')`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (2, 20, 'two')`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (3, 30, 'three')`);
    });

    /**
     * KNOWN FAILURE: Table alias with AS keyword
     *
     * SQLLogicTest query: SELECT t.a, t.b FROM t1 AS t
     * Expected: 3 rows with columns a, b from t1 using alias t
     *
     * Bug: The InMemoryEngine does not recognize table aliases defined
     * with the AS keyword. Column references like t.a fail to resolve.
     */
    it('should support table alias with AS keyword', () => {
      // SQLLogicTest: SELECT t.a, t.b FROM t1 AS t
      const result = db.prepare('SELECT t.a, t.b FROM t1 AS t').all();

      expect(result.length).toBe(3);
      expect(result).toEqual([
        { a: 1, b: 10 },
        { a: 2, b: 20 },
        { a: 3, b: 30 },
      ]);
    });

    /**
     * KNOWN FAILURE: Table alias with AS keyword - single column
     *
     * SQLLogicTest query: SELECT t.c FROM t1 AS t
     * Expected: 3 rows with column c from t1 using alias t
     */
    it('should support single column reference via AS alias', () => {
      // SQLLogicTest: SELECT t.c FROM t1 AS t
      const result = db.prepare('SELECT t.c FROM t1 AS t').all();

      expect(result.length).toBe(3);
      expect(result).toEqual([
        { c: 'one' },
        { c: 'two' },
        { c: 'three' },
      ]);
    });

    /**
     * KNOWN FAILURE: Table alias with lowercase as keyword
     *
     * SQLLogicTest query: SELECT t.a FROM t1 as t
     * Expected: Same behavior as AS (uppercase)
     */
    it('should support lowercase as keyword for table alias', () => {
      // SQLLogicTest: SELECT t.a FROM t1 as t
      const result = db.prepare('SELECT t.a FROM t1 as t').all();

      expect(result.length).toBe(3);
      expect(result.map(r => (r as { a: number }).a)).toEqual([1, 2, 3]);
    });
  });

  // ===========================================================================
  // IMPLICIT ALIAS (WITHOUT AS KEYWORD)
  // ===========================================================================

  describe('Implicit alias (without AS keyword)', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (a INTEGER, b INTEGER, c TEXT)
      `);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (1, 10, 'one')`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (2, 20, 'two')`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (3, 30, 'three')`);
    });

    /**
     * KNOWN FAILURE: Implicit table alias without AS keyword
     *
     * SQLLogicTest query: SELECT x.a FROM t1 x WHERE x.b > 10
     * Expected: 2 rows where b > 10 (rows with a=2,3)
     *
     * Bug: The InMemoryEngine does not recognize implicit table aliases
     * (space-separated, without AS keyword). Column references like x.a
     * and x.b fail to resolve.
     */
    it('should support implicit table alias without AS keyword', () => {
      // SQLLogicTest: SELECT x.a FROM t1 x WHERE x.b > 10
      const result = db.prepare('SELECT x.a FROM t1 x WHERE x.b > 10').all();

      expect(result.length).toBe(2);
      expect(result).toEqual([
        { a: 2 },
        { a: 3 },
      ]);
    });

    /**
     * KNOWN FAILURE: Implicit alias with multiple columns
     *
     * SQLLogicTest query: SELECT x.a, x.b, x.c FROM t1 x
     * Expected: 3 rows with all columns using implicit alias x
     */
    it('should support implicit alias with multiple column references', () => {
      // SQLLogicTest: SELECT x.a, x.b, x.c FROM t1 x
      const result = db.prepare('SELECT x.a, x.b, x.c FROM t1 x').all();

      expect(result.length).toBe(3);
      expect(result).toEqual([
        { a: 1, b: 10, c: 'one' },
        { a: 2, b: 20, c: 'two' },
        { a: 3, b: 30, c: 'three' },
      ]);
    });

    /**
     * KNOWN FAILURE: Implicit alias with single letter
     *
     * SQLLogicTest query: SELECT t.a FROM t1 t
     * Expected: Single letter alias should work
     */
    it('should support single letter implicit alias', () => {
      // SQLLogicTest: SELECT t.a FROM t1 t
      const result = db.prepare('SELECT t.a FROM t1 t').all();

      expect(result.length).toBe(3);
      expect(result.map(r => (r as { a: number }).a)).toEqual([1, 2, 3]);
    });
  });

  // ===========================================================================
  // ALIAS IN WHERE CLAUSE
  // ===========================================================================

  describe('Alias in WHERE clause', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (a INTEGER, b INTEGER, c TEXT)
      `);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (1, 10, 'one')`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (2, 20, 'two')`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (3, 30, 'three')`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (4, 40, 'four')`);
    });

    /**
     * KNOWN FAILURE: Alias reference in WHERE with comparison
     *
     * SQLLogicTest query: SELECT t.a FROM t1 AS t WHERE t.b >= 20
     * Expected: 3 rows where b >= 20 (a=2,3,4)
     */
    it('should resolve alias in WHERE clause with >= comparison', () => {
      // SQLLogicTest: SELECT t.a FROM t1 AS t WHERE t.b >= 20
      const result = db.prepare('SELECT t.a FROM t1 AS t WHERE t.b >= 20').all();

      expect(result.length).toBe(3);
      expect(result).toEqual([
        { a: 2 },
        { a: 3 },
        { a: 4 },
      ]);
    });

    /**
     * KNOWN FAILURE: Alias reference in WHERE with equality
     *
     * SQLLogicTest query: SELECT t.a, t.c FROM t1 AS t WHERE t.b = 20
     * Expected: 1 row where b = 20
     */
    it('should resolve alias in WHERE clause with equality', () => {
      // SQLLogicTest: SELECT t.a, t.c FROM t1 AS t WHERE t.b = 20
      const result = db.prepare('SELECT t.a, t.c FROM t1 AS t WHERE t.b = 20').all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 2, c: 'two' });
    });

    /**
     * KNOWN FAILURE: Implicit alias in WHERE clause
     *
     * SQLLogicTest query: SELECT x.a FROM t1 x WHERE x.b < 30
     * Expected: 2 rows where b < 30 (a=1,2)
     */
    it('should resolve implicit alias in WHERE clause', () => {
      // SQLLogicTest: SELECT x.a FROM t1 x WHERE x.b < 30
      const result = db.prepare('SELECT x.a FROM t1 x WHERE x.b < 30').all();

      expect(result.length).toBe(2);
      expect(result).toEqual([
        { a: 1 },
        { a: 2 },
      ]);
    });

    /**
     * KNOWN FAILURE: Alias in WHERE with AND condition
     *
     * SQLLogicTest query: SELECT t.a FROM t1 AS t WHERE t.b > 10 AND t.b < 40
     * Expected: 2 rows where 10 < b < 40 (a=2,3)
     */
    it('should resolve alias in WHERE clause with AND conditions', () => {
      // SQLLogicTest: SELECT t.a FROM t1 AS t WHERE t.b > 10 AND t.b < 40
      const result = db.prepare('SELECT t.a FROM t1 AS t WHERE t.b > 10 AND t.b < 40').all();

      expect(result.length).toBe(2);
      expect(result).toEqual([
        { a: 2 },
        { a: 3 },
      ]);
    });
  });

  // ===========================================================================
  // ALIAS IN SELECT COLUMN REFERENCES
  // ===========================================================================

  describe('Alias in SELECT column references', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (a INTEGER, b INTEGER, c TEXT)
      `);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (1, 10, 'one')`);
      db.exec(`INSERT INTO t1 (a, b, c) VALUES (2, 20, 'two')`);
    });

    /**
     * KNOWN FAILURE: All columns via alias
     *
     * SQLLogicTest query: SELECT t.a, t.b, t.c FROM t1 AS t
     * Expected: All columns referenced via alias
     */
    it('should allow all columns to be referenced via alias', () => {
      // SQLLogicTest: SELECT t.a, t.b, t.c FROM t1 AS t
      const result = db.prepare('SELECT t.a, t.b, t.c FROM t1 AS t').all();

      expect(result.length).toBe(2);
      expect(result).toEqual([
        { a: 1, b: 10, c: 'one' },
        { a: 2, b: 20, c: 'two' },
      ]);
    });

    /**
     * KNOWN FAILURE: Mixed alias and direct column references
     *
     * This tests whether both t.a (via alias) and b (direct) work together.
     * Note: This may be intentionally unsupported, but documenting expected behavior.
     */
    it('should allow mixing alias and direct column references', () => {
      // Some SQL engines allow: SELECT t.a, b FROM t1 AS t
      // Where t.a uses alias and b is direct reference
      const result = db.prepare('SELECT t.a, t.b FROM t1 AS t').all();

      expect(result.length).toBe(2);
      expect(result[0]).toEqual({ a: 1, b: 10 });
      expect(result[1]).toEqual({ a: 2, b: 20 });
    });

    /**
     * KNOWN FAILURE: SELECT * with alias should still work
     *
     * SQLLogicTest query: SELECT * FROM t1 AS t
     * Expected: All rows, alias just provides alternate name
     */
    it('should support SELECT * with table alias', () => {
      // SQLLogicTest: SELECT * FROM t1 AS t
      const result = db.prepare('SELECT * FROM t1 AS t').all();

      expect(result.length).toBe(2);
      expect(result).toEqual([
        { a: 1, b: 10, c: 'one' },
        { a: 2, b: 20, c: 'two' },
      ]);
    });
  });

  // ===========================================================================
  // SELF-JOIN WITH ALIASES
  // ===========================================================================

  describe('Self-join with aliases', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (a INTEGER, b INTEGER)
      `);
      db.exec(`INSERT INTO t1 (a, b) VALUES (1, 2)`);
      db.exec(`INSERT INTO t1 (a, b) VALUES (2, 3)`);
      db.exec(`INSERT INTO t1 (a, b) VALUES (3, 4)`);
      db.exec(`INSERT INTO t1 (a, b) VALUES (4, 5)`);
    });

    /**
     * KNOWN FAILURE: Self-join using AS keyword
     *
     * SQLLogicTest query: SELECT t1.a, t2.b FROM t1, t1 AS t2 WHERE t1.a = t2.b
     * Expected: Rows where t1.a matches t2.b (a=2,b=2), (a=3,b=3), (a=4,b=4)
     *
     * Bug: Self-joins require aliases to disambiguate table references.
     * The engine must recognize that t1 and t2 refer to the same table
     * but as separate instances in the query.
     */
    it('should support self-join with AS keyword', () => {
      // SQLLogicTest: SELECT t1.a, t2.b FROM t1, t1 AS t2 WHERE t1.a = t2.b
      const result = db.prepare('SELECT t1.a, t2.b FROM t1, t1 AS t2 WHERE t1.a = t2.b').all();

      // t1.a = t2.b means: find rows where a in first copy equals b in second copy
      // t1: (1,2), (2,3), (3,4), (4,5)
      // t2: (1,2), (2,3), (3,4), (4,5)
      // Matches: t1.a=2 matches t2.b=2, t1.a=3 matches t2.b=3, t1.a=4 matches t2.b=4
      expect(result.length).toBe(3);
      const sorted = (result as { a: number; b: number }[]).sort((x, y) => x.a - y.a);
      expect(sorted).toEqual([
        { a: 2, b: 2 },
        { a: 3, b: 3 },
        { a: 4, b: 4 },
      ]);
    });

    /**
     * KNOWN FAILURE: Self-join with implicit aliases
     *
     * SQLLogicTest query: SELECT a.a, b.b FROM t1 a, t1 b WHERE a.a = b.b
     * Expected: Same as above but using implicit aliases
     */
    it('should support self-join with implicit aliases', () => {
      // SQLLogicTest: SELECT a.a, b.b FROM t1 a, t1 b WHERE a.a = b.b
      const result = db.prepare('SELECT a.a, b.b FROM t1 a, t1 b WHERE a.a = b.b').all();

      expect(result.length).toBe(3);
      const sorted = (result as { a: number; b: number }[]).sort((x, y) => x.a - y.a);
      expect(sorted).toEqual([
        { a: 2, b: 2 },
        { a: 3, b: 3 },
        { a: 4, b: 4 },
      ]);
    });

    /**
     * KNOWN FAILURE: Self-join cross product with different condition
     *
     * SQLLogicTest query: SELECT a.a, b.a FROM t1 a, t1 b WHERE a.b = b.a
     * Expected: Chain linkage - a.b points to b.a
     */
    it('should support self-join with chain condition', () => {
      // SQLLogicTest: SELECT a.a, b.a FROM t1 a, t1 b WHERE a.b = b.a
      // t1: (1,2), (2,3), (3,4), (4,5)
      // a.b = b.a means: find where b column of first equals a column of second
      // (1,2) -> a.b=2 matches (2,3) where b.a=2 -> result (1, 2)
      // (2,3) -> a.b=3 matches (3,4) where b.a=3 -> result (2, 3)
      // (3,4) -> a.b=4 matches (4,5) where b.a=4 -> result (3, 4)
      const result = db.prepare('SELECT a.a, b.a FROM t1 a, t1 b WHERE a.b = b.a').all();

      expect(result.length).toBe(3);
      const sorted = (result as { a: number }[]).sort((x, y) => x.a - y.a);
      expect(sorted).toEqual([
        { a: 1 },  // Column name collision - both selected as 'a'
        { a: 2 },
        { a: 3 },
      ]);
    });
  });

  // ===========================================================================
  // MULTIPLE ALIASES IN SAME QUERY
  // ===========================================================================

  describe('Multiple aliases in same query', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (x INTEGER, y INTEGER)
      `);
      db.exec(`INSERT INTO t1 (x, y) VALUES (1, 2)`);
      db.exec(`INSERT INTO t1 (x, y) VALUES (2, 3)`);
      db.exec(`INSERT INTO t1 (x, y) VALUES (3, 1)`);
    });

    /**
     * KNOWN FAILURE: Two aliases on same table in FROM clause
     *
     * SQLLogicTest query: SELECT * FROM t1 AS a, t1 AS b WHERE a.x = b.y
     * Expected: Cartesian product filtered by condition
     */
    it('should support two AS aliases on same table', () => {
      // SQLLogicTest: SELECT * FROM t1 AS a, t1 AS b WHERE a.x = b.y
      const result = db.prepare('SELECT * FROM t1 AS a, t1 AS b WHERE a.x = b.y').all();

      // t1: (1,2), (2,3), (3,1)
      // a.x = b.y
      // a(1,2): x=1 matches b(3,1) y=1 -> (1,2,3,1)
      // a(2,3): x=2 matches b(1,2) y=2 -> (2,3,1,2)
      // a(3,1): x=3 matches b(2,3) y=3 -> (3,1,2,3)
      expect(result.length).toBe(3);
    });

    /**
     * KNOWN FAILURE: Two implicit aliases on same table
     *
     * SQLLogicTest query: SELECT a.x, b.y FROM t1 a, t1 b WHERE a.x = b.y
     * Expected: Specific columns from each alias
     */
    it('should support two implicit aliases on same table', () => {
      // SQLLogicTest: SELECT a.x, b.y FROM t1 a, t1 b WHERE a.x = b.y
      const result = db.prepare('SELECT a.x, b.y FROM t1 a, t1 b WHERE a.x = b.y').all();

      expect(result.length).toBe(3);
      const sorted = (result as { x: number; y: number }[]).sort((r1, r2) => r1.x - r2.x);
      expect(sorted).toEqual([
        { x: 1, y: 1 },
        { x: 2, y: 2 },
        { x: 3, y: 3 },
      ]);
    });

    /**
     * KNOWN FAILURE: Mixed AS and implicit aliases
     *
     * SQLLogicTest query: SELECT a.x, b.y FROM t1 AS a, t1 b WHERE a.y = b.x
     * Expected: One table with AS, one without
     */
    it('should support mixed AS and implicit aliases', () => {
      // SQLLogicTest: SELECT a.x, b.y FROM t1 AS a, t1 b WHERE a.y = b.x
      const result = db.prepare('SELECT a.x, b.y FROM t1 AS a, t1 b WHERE a.y = b.x').all();

      // t1: (1,2), (2,3), (3,1)
      // a.y = b.x
      // a(1,2): y=2 matches b(2,3) x=2 -> (1, 3)
      // a(2,3): y=3 matches b(3,1) x=3 -> (2, 1)
      // a(3,1): y=1 matches b(1,2) x=1 -> (3, 2)
      expect(result.length).toBe(3);
      const sorted = (result as { x: number; y: number }[]).sort((r1, r2) => r1.x - r2.x);
      expect(sorted).toEqual([
        { x: 1, y: 3 },
        { x: 2, y: 1 },
        { x: 3, y: 2 },
      ]);
    });
  });

  // ===========================================================================
  // EDGE CASES
  // ===========================================================================

  describe('Edge cases for table aliases', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (a INTEGER, b INTEGER)
      `);
      db.exec(`INSERT INTO t1 (a, b) VALUES (1, 10)`);
      db.exec(`INSERT INTO t1 (a, b) VALUES (2, 20)`);
    });

    /**
     * KNOWN FAILURE: Alias same as column name
     *
     * Edge case: Using alias 'a' when table has column named 'a'
     */
    it('should handle alias same as column name', () => {
      // Alias 'a' used, table has column 'a'
      const result = db.prepare('SELECT a.a, a.b FROM t1 a').all();

      expect(result.length).toBe(2);
      expect(result).toEqual([
        { a: 1, b: 10 },
        { a: 2, b: 20 },
      ]);
    });

    /**
     * KNOWN FAILURE: Longer alias name
     *
     * Edge case: Multi-character alias
     */
    it('should support longer alias names', () => {
      const result = db.prepare('SELECT tbl.a, tbl.b FROM t1 AS tbl').all();

      expect(result.length).toBe(2);
      expect(result).toEqual([
        { a: 1, b: 10 },
        { a: 2, b: 20 },
      ]);
    });

    /**
     * KNOWN FAILURE: Alias with underscore
     *
     * Edge case: Alias containing underscore
     */
    it('should support alias with underscore', () => {
      const result = db.prepare('SELECT my_t.a FROM t1 AS my_t').all();

      expect(result.length).toBe(2);
      expect(result.map(r => (r as { a: number }).a)).toEqual([1, 2]);
    });

    /**
     * KNOWN FAILURE: Numeric suffix in alias
     *
     * Edge case: Alias like t1 (same as table name but as alias)
     */
    it('should support alias with numeric suffix', () => {
      const result = db.prepare('SELECT t2.a FROM t1 AS t2').all();

      expect(result.length).toBe(2);
      expect(result.map(r => (r as { a: number }).a)).toEqual([1, 2]);
    });
  });
});

// =============================================================================
// LOW-LEVEL INMEMORYENGINE TESTS FOR TABLE ALIASES
// =============================================================================

describe('InMemoryEngine Direct Table Alias Tests', () => {
  let engine: InMemoryEngine;
  let storage: InMemoryStorage;

  beforeEach(() => {
    storage = createInMemoryStorage();
    engine = new InMemoryEngine(storage);

    // Set up test table
    engine.execute('CREATE TABLE test (id INTEGER, value INTEGER)', []);
    engine.execute('INSERT INTO test (id, value) VALUES (?, ?)', [1, 10]);
    engine.execute('INSERT INTO test (id, value) VALUES (?, ?)', [2, 20]);
    engine.execute('INSERT INTO test (id, value) VALUES (?, ?)', [3, 30]);
  });

  /**
   * KNOWN FAILURE: Direct engine test for AS alias
   */
  it('should resolve column via AS alias in engine.execute', () => {
    const result = engine.execute('SELECT t.id, t.value FROM test AS t', []);

    expect(result.rows.length).toBe(3);
    expect(result.rows).toEqual([
      { id: 1, value: 10 },
      { id: 2, value: 20 },
      { id: 3, value: 30 },
    ]);
  });

  /**
   * KNOWN FAILURE: Direct engine test for implicit alias
   */
  it('should resolve column via implicit alias in engine.execute', () => {
    const result = engine.execute('SELECT t.id, t.value FROM test t', []);

    expect(result.rows.length).toBe(3);
    expect(result.rows).toEqual([
      { id: 1, value: 10 },
      { id: 2, value: 20 },
      { id: 3, value: 30 },
    ]);
  });

  /**
   * KNOWN FAILURE: Direct engine test for alias in WHERE
   */
  it('should filter using alias reference in WHERE', () => {
    const result = engine.execute('SELECT t.id FROM test AS t WHERE t.value > ?', [15]);

    expect(result.rows.length).toBe(2);
    expect(result.rows.map(r => r.id)).toEqual([2, 3]);
  });
});
