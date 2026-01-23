/**
 * SQLLogicTest Compatibility Tests - Aggregate Function Edge Cases
 *
 * RED Phase TDD tests documenting aggregate function edge cases from SQLLogicTest.
 * These tests capture expected SQL behavior that is not yet implemented or has bugs.
 *
 * SQLLogicTest reference patterns:
 * - SELECT count(*), sum(a), avg(a), min(a), max(a) FROM t1
 * - SELECT count(DISTINCT a) FROM t1
 * - SELECT sum(a+b) FROM t1
 * - SELECT avg(CASE WHEN a>0 THEN a END) FROM t1
 * - SELECT group_concat(a) FROM t1
 * - SELECT total(a) FROM t1
 *
 * Run with: npx vitest run src/__tests__/sqllogictest-aggregates.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Database } from '../database.js';

// =============================================================================
// TEST SETUP
// =============================================================================

describe('SQLLogicTest Aggregate Function Edge Cases', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  // ===========================================================================
  // COUNT(DISTINCT column) - Known Unsupported Feature
  // ===========================================================================

  describe('COUNT(DISTINCT column)', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (id INTEGER, category TEXT, value INTEGER)
      `);
      db.exec(`INSERT INTO t1 (id, category, value) VALUES (1, 'A', 10)`);
      db.exec(`INSERT INTO t1 (id, category, value) VALUES (2, 'B', 20)`);
      db.exec(`INSERT INTO t1 (id, category, value) VALUES (3, 'A', 30)`);
      db.exec(`INSERT INTO t1 (id, category, value) VALUES (4, 'B', 40)`);
      db.exec(`INSERT INTO t1 (id, category, value) VALUES (5, 'A', 50)`);
      db.exec(`INSERT INTO t1 (id, category, value) VALUES (6, 'C', 60)`);
    });

    /**
     * KNOWN FAILURE: COUNT(DISTINCT column) not supported
     *
     * SQLLogicTest: SELECT COUNT(DISTINCT category) FROM t1
     * Expected: 3 (distinct values: 'A', 'B', 'C')
     *
     * The DISTINCT modifier inside aggregate functions is a standard SQL feature
     * that should be supported for counting unique values.
     */
    it('should count distinct values in a column', () => {
      const result = db.prepare('SELECT COUNT(DISTINCT category) AS cnt FROM t1').all();

      expect(result.length).toBe(1);
      expect((result[0] as { cnt: number }).cnt).toBe(3);
    });

    /**
     * KNOWN FAILURE: COUNT(DISTINCT column) with multiple distinct values
     *
     * SQLLogicTest: SELECT COUNT(DISTINCT value) FROM t1
     * Expected: 6 (all values are unique: 10, 20, 30, 40, 50, 60)
     */
    it('should count all distinct values when all are unique', () => {
      const result = db.prepare('SELECT COUNT(DISTINCT value) AS cnt FROM t1').all();

      expect(result.length).toBe(1);
      expect((result[0] as { cnt: number }).cnt).toBe(6);
    });

    /**
     * KNOWN FAILURE: COUNT(DISTINCT column) with GROUP BY
     *
     * SQLLogicTest: SELECT category, COUNT(DISTINCT value) FROM t1 GROUP BY category
     * Expected: A->3, B->2, C->1
     */
    it('should count distinct values per group', () => {
      const result = db.prepare(
        'SELECT category, COUNT(DISTINCT value) AS cnt FROM t1 GROUP BY category'
      ).all();

      const sorted = (result as { category: string; cnt: number }[])
        .sort((a, b) => a.category.localeCompare(b.category));

      expect(sorted.length).toBe(3);
      expect(sorted[0]).toEqual({ category: 'A', cnt: 3 });
      expect(sorted[1]).toEqual({ category: 'B', cnt: 2 });
      expect(sorted[2]).toEqual({ category: 'C', cnt: 1 });
    });
  });

  // ===========================================================================
  // AGGREGATE WITH EXPRESSION (sum(a+b))
  // ===========================================================================

  describe('Aggregate with Expression', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t2 (id INTEGER, a INTEGER, b INTEGER)
      `);
      db.exec(`INSERT INTO t2 (id, a, b) VALUES (1, 10, 5)`);
      db.exec(`INSERT INTO t2 (id, a, b) VALUES (2, 20, 10)`);
      db.exec(`INSERT INTO t2 (id, a, b) VALUES (3, 30, 15)`);
      db.exec(`INSERT INTO t2 (id, a, b) VALUES (4, 40, 20)`);
    });

    /**
     * KNOWN FAILURE: SUM of expression (a+b)
     *
     * SQLLogicTest: SELECT SUM(a+b) FROM t2
     * Expected: (10+5) + (20+10) + (30+15) + (40+20) = 15 + 30 + 45 + 60 = 150
     *
     * Aggregate functions should accept arbitrary expressions, not just column names.
     */
    it('should sum an arithmetic expression', () => {
      const result = db.prepare('SELECT SUM(a+b) AS total FROM t2').all();

      expect(result.length).toBe(1);
      expect((result[0] as { total: number }).total).toBe(150);
    });

    /**
     * KNOWN FAILURE: AVG of expression (a*b)
     *
     * SQLLogicTest: SELECT AVG(a*b) FROM t2
     * Expected: (50 + 200 + 450 + 800) / 4 = 1500 / 4 = 375
     */
    it('should average a multiplication expression', () => {
      const result = db.prepare('SELECT AVG(a*b) AS avg_val FROM t2').all();

      expect(result.length).toBe(1);
      expect((result[0] as { avg_val: number }).avg_val).toBe(375);
    });

    /**
     * KNOWN FAILURE: MIN of expression (a-b)
     *
     * SQLLogicTest: SELECT MIN(a-b) FROM t2
     * Expected: min(5, 10, 15, 20) = 5
     */
    it('should find minimum of subtraction expression', () => {
      const result = db.prepare('SELECT MIN(a-b) AS min_val FROM t2').all();

      expect(result.length).toBe(1);
      expect((result[0] as { min_val: number }).min_val).toBe(5);
    });

    /**
     * KNOWN FAILURE: MAX of expression (a/b)
     *
     * SQLLogicTest: SELECT MAX(a/b) FROM t2
     * Expected: max(2, 2, 2, 2) = 2 (integer division)
     */
    it('should find maximum of division expression', () => {
      const result = db.prepare('SELECT MAX(a/b) AS max_val FROM t2').all();

      expect(result.length).toBe(1);
      expect((result[0] as { max_val: number }).max_val).toBe(2);
    });

    /**
     * KNOWN FAILURE: COUNT with expression
     *
     * SQLLogicTest: SELECT COUNT(a+b) FROM t2
     * Expected: 4 (counts non-null results of the expression)
     */
    it('should count results of expression', () => {
      const result = db.prepare('SELECT COUNT(a+b) AS cnt FROM t2').all();

      expect(result.length).toBe(1);
      expect((result[0] as { cnt: number }).cnt).toBe(4);
    });
  });

  // ===========================================================================
  // AGGREGATE WITH CASE WHEN
  // ===========================================================================

  describe('Aggregate with CASE WHEN', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t3 (id INTEGER, value INTEGER, status TEXT)
      `);
      db.exec(`INSERT INTO t3 (id, value, status) VALUES (1, -10, 'negative')`);
      db.exec(`INSERT INTO t3 (id, value, status) VALUES (2, 0, 'zero')`);
      db.exec(`INSERT INTO t3 (id, value, status) VALUES (3, 20, 'positive')`);
      db.exec(`INSERT INTO t3 (id, value, status) VALUES (4, 30, 'positive')`);
      db.exec(`INSERT INTO t3 (id, value, status) VALUES (5, -5, 'negative')`);
      db.exec(`INSERT INTO t3 (id, value, status) VALUES (6, 40, 'positive')`);
    });

    /**
     * KNOWN FAILURE: AVG with CASE WHEN filtering positive values
     *
     * SQLLogicTest: SELECT AVG(CASE WHEN value > 0 THEN value END) FROM t3
     * Expected: (20 + 30 + 40) / 3 = 90 / 3 = 30
     *
     * The CASE WHEN should return NULL for non-matching rows,
     * and AVG should exclude NULLs from calculation.
     */
    it('should average only positive values using CASE WHEN', () => {
      const result = db.prepare(
        'SELECT AVG(CASE WHEN value > 0 THEN value END) AS avg_positive FROM t3'
      ).all();

      expect(result.length).toBe(1);
      expect((result[0] as { avg_positive: number }).avg_positive).toBe(30);
    });

    /**
     * KNOWN FAILURE: SUM with CASE WHEN
     *
     * SQLLogicTest: SELECT SUM(CASE WHEN value >= 0 THEN value ELSE 0 END) FROM t3
     * Expected: 0 + 0 + 20 + 30 + 0 + 40 = 90
     */
    it('should sum non-negative values with CASE WHEN ELSE', () => {
      const result = db.prepare(
        'SELECT SUM(CASE WHEN value >= 0 THEN value ELSE 0 END) AS sum_nonneg FROM t3'
      ).all();

      expect(result.length).toBe(1);
      expect((result[0] as { sum_nonneg: number }).sum_nonneg).toBe(90);
    });

    /**
     * KNOWN FAILURE: COUNT with CASE WHEN
     *
     * SQLLogicTest: SELECT COUNT(CASE WHEN value > 0 THEN 1 END) FROM t3
     * Expected: 3 (counts non-null results: rows with positive values)
     */
    it('should count matching rows using CASE WHEN', () => {
      const result = db.prepare(
        'SELECT COUNT(CASE WHEN value > 0 THEN 1 END) AS cnt FROM t3'
      ).all();

      expect(result.length).toBe(1);
      expect((result[0] as { cnt: number }).cnt).toBe(3);
    });

    /**
     * KNOWN FAILURE: MAX with CASE WHEN on text column
     *
     * SQLLogicTest: SELECT MAX(CASE WHEN status = 'positive' THEN value END) FROM t3
     * Expected: 40
     */
    it('should find max of filtered values using CASE WHEN', () => {
      const result = db.prepare(
        "SELECT MAX(CASE WHEN status = 'positive' THEN value END) AS max_pos FROM t3"
      ).all();

      expect(result.length).toBe(1);
      expect((result[0] as { max_pos: number }).max_pos).toBe(40);
    });
  });

  // ===========================================================================
  // GROUP_CONCAT Function (SQLite-specific)
  // ===========================================================================

  describe('GROUP_CONCAT Function', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t4 (id INTEGER, category TEXT, name TEXT)
      `);
      db.exec(`INSERT INTO t4 (id, category, name) VALUES (1, 'fruit', 'apple')`);
      db.exec(`INSERT INTO t4 (id, category, name) VALUES (2, 'fruit', 'banana')`);
      db.exec(`INSERT INTO t4 (id, category, name) VALUES (3, 'fruit', 'cherry')`);
      db.exec(`INSERT INTO t4 (id, category, name) VALUES (4, 'vegetable', 'carrot')`);
      db.exec(`INSERT INTO t4 (id, category, name) VALUES (5, 'vegetable', 'potato')`);
    });

    /**
     * KNOWN FAILURE: GROUP_CONCAT without GROUP BY
     *
     * SQLLogicTest: SELECT GROUP_CONCAT(name) FROM t4
     * Expected: 'apple,banana,cherry,carrot,potato' (or similar, order may vary)
     *
     * GROUP_CONCAT concatenates values from multiple rows into a single string.
     */
    it('should concatenate all values into a comma-separated string', () => {
      const result = db.prepare('SELECT GROUP_CONCAT(name) AS names FROM t4').all();

      expect(result.length).toBe(1);
      const names = (result[0] as { names: string }).names;
      // Check that all names are present (order may vary)
      expect(names).toContain('apple');
      expect(names).toContain('banana');
      expect(names).toContain('cherry');
      expect(names).toContain('carrot');
      expect(names).toContain('potato');
      expect(names.split(',').length).toBe(5);
    });

    /**
     * KNOWN FAILURE: GROUP_CONCAT with GROUP BY
     *
     * SQLLogicTest: SELECT category, GROUP_CONCAT(name) FROM t4 GROUP BY category
     * Expected:
     *   fruit -> 'apple,banana,cherry'
     *   vegetable -> 'carrot,potato'
     */
    it('should concatenate values within each group', () => {
      const result = db.prepare(
        'SELECT category, GROUP_CONCAT(name) AS names FROM t4 GROUP BY category'
      ).all();

      const sorted = (result as { category: string; names: string }[])
        .sort((a, b) => a.category.localeCompare(b.category));

      expect(sorted.length).toBe(2);
      expect(sorted[0].category).toBe('fruit');
      expect(sorted[0].names.split(',').sort()).toEqual(['apple', 'banana', 'cherry']);
      expect(sorted[1].category).toBe('vegetable');
      expect(sorted[1].names.split(',').sort()).toEqual(['carrot', 'potato']);
    });

    /**
     * KNOWN FAILURE: GROUP_CONCAT with custom separator
     *
     * SQLLogicTest: SELECT GROUP_CONCAT(name, ' | ') FROM t4 WHERE category = 'fruit'
     * Expected: 'apple | banana | cherry' (or similar, order may vary)
     */
    it('should use custom separator in GROUP_CONCAT', () => {
      const result = db.prepare(
        "SELECT GROUP_CONCAT(name, ' | ') AS names FROM t4 WHERE category = 'fruit'"
      ).all();

      expect(result.length).toBe(1);
      const names = (result[0] as { names: string }).names;
      expect(names.split(' | ').sort()).toEqual(['apple', 'banana', 'cherry']);
    });

    /**
     * KNOWN FAILURE: GROUP_CONCAT(DISTINCT column)
     *
     * SQLLogicTest: SELECT GROUP_CONCAT(DISTINCT category) FROM t4
     * Expected: 'fruit,vegetable' (or 'vegetable,fruit')
     */
    it('should concatenate distinct values only', () => {
      const result = db.prepare('SELECT GROUP_CONCAT(DISTINCT category) AS cats FROM t4').all();

      expect(result.length).toBe(1);
      const cats = (result[0] as { cats: string }).cats.split(',').sort();
      expect(cats).toEqual(['fruit', 'vegetable']);
    });
  });

  // ===========================================================================
  // TOTAL Function (SQLite-specific)
  // ===========================================================================

  describe('TOTAL Function', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t5 (id INTEGER, amount REAL)
      `);
      db.exec(`INSERT INTO t5 (id, amount) VALUES (1, 10.5)`);
      db.exec(`INSERT INTO t5 (id, amount) VALUES (2, 20.5)`);
      db.exec(`INSERT INTO t5 (id, amount) VALUES (3, 30.0)`);
    });

    /**
     * KNOWN FAILURE: TOTAL function basic usage
     *
     * SQLLogicTest: SELECT TOTAL(amount) FROM t5
     * Expected: 61.0
     *
     * TOTAL is a SQLite-specific function that always returns a floating point
     * value (0.0 for empty sets), unlike SUM which can return NULL or integer.
     */
    it('should return total as floating point', () => {
      const result = db.prepare('SELECT TOTAL(amount) AS total FROM t5').all();

      expect(result.length).toBe(1);
      expect((result[0] as { total: number }).total).toBe(61.0);
    });

    /**
     * KNOWN FAILURE: TOTAL vs SUM difference on empty table
     *
     * SQLLogicTest: SELECT TOTAL(amount), SUM(amount) FROM empty_table
     * Expected: TOTAL returns 0.0, SUM returns NULL
     */
    it('should return 0.0 for empty table (unlike SUM which returns NULL)', () => {
      db.exec('CREATE TABLE empty_t5 (id INTEGER, amount REAL)');

      const result = db.prepare('SELECT TOTAL(amount) AS total FROM empty_t5').all();

      expect(result.length).toBe(1);
      expect((result[0] as { total: number }).total).toBe(0.0);
    });

    /**
     * KNOWN FAILURE: TOTAL with integer column
     *
     * SQLLogicTest: SELECT TOTAL(id) FROM t5
     * Expected: 6.0 (1 + 2 + 3 as float)
     */
    it('should return floating point even for integer column', () => {
      const result = db.prepare('SELECT TOTAL(id) AS total FROM t5').all();

      expect(result.length).toBe(1);
      expect((result[0] as { total: number }).total).toBe(6.0);
      expect(typeof (result[0] as { total: number }).total).toBe('number');
    });
  });

  // ===========================================================================
  // NULL HANDLING IN AGGREGATES
  // ===========================================================================

  describe('NULL Handling in Aggregates', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t6 (id INTEGER, value INTEGER)
      `);
      db.exec(`INSERT INTO t6 (id, value) VALUES (1, 10)`);
      db.exec(`INSERT INTO t6 (id, value) VALUES (2, NULL)`);
      db.exec(`INSERT INTO t6 (id, value) VALUES (3, 30)`);
      db.exec(`INSERT INTO t6 (id, value) VALUES (4, NULL)`);
      db.exec(`INSERT INTO t6 (id, value) VALUES (5, 50)`);
    });

    /**
     * KNOWN FAILURE: COUNT(*) not returning single aggregated row
     *
     * Bug: The engine returns all rows instead of a single aggregated result.
     * Standard behavior: COUNT(*) counts all rows including NULLs
     * Expected: single row with cnt=5
     */
    it('should count all rows with COUNT(*)', () => {
      const result = db.prepare('SELECT COUNT(*) AS cnt FROM t6').all();

      expect(result.length).toBe(1);
      expect((result[0] as { cnt: number }).cnt).toBe(5);
    });

    /**
     * KNOWN FAILURE: COUNT(column) not returning single aggregated row
     *
     * Bug: The engine returns all rows instead of a single aggregated result.
     * Standard behavior: COUNT(column) excludes NULL values
     * Expected: single row with cnt=3
     */
    it('should exclude NULL values with COUNT(column)', () => {
      const result = db.prepare('SELECT COUNT(value) AS cnt FROM t6').all();

      expect(result.length).toBe(1);
      expect((result[0] as { cnt: number }).cnt).toBe(3);
    });

    /**
     * KNOWN FAILURE: SUM not returning single aggregated row
     *
     * Bug: The engine returns all rows instead of a single aggregated result.
     * Standard behavior: SUM excludes NULL values
     * Expected: single row with total=90 (10 + 30 + 50)
     */
    it('should exclude NULL values in SUM', () => {
      const result = db.prepare('SELECT SUM(value) AS total FROM t6').all();

      expect(result.length).toBe(1);
      expect((result[0] as { total: number }).total).toBe(90);
    });

    /**
     * KNOWN FAILURE: AVG not returning single aggregated row
     *
     * Bug: The engine returns all rows instead of a single aggregated result.
     * Standard behavior: AVG excludes NULL values from both sum and count
     * Expected: single row with avg_val=30 ((10 + 30 + 50) / 3)
     */
    it('should exclude NULL values in AVG calculation', () => {
      const result = db.prepare('SELECT AVG(value) AS avg_val FROM t6').all();

      expect(result.length).toBe(1);
      expect((result[0] as { avg_val: number }).avg_val).toBe(30);
    });

    /**
     * KNOWN FAILURE: MIN not returning single aggregated row
     *
     * Bug: The engine returns all rows instead of a single aggregated result.
     * Standard behavior: MIN excludes NULL values
     * Expected: single row with min_val=10
     */
    it('should exclude NULL values in MIN', () => {
      const result = db.prepare('SELECT MIN(value) AS min_val FROM t6').all();

      expect(result.length).toBe(1);
      expect((result[0] as { min_val: number }).min_val).toBe(10);
    });

    /**
     * KNOWN FAILURE: MAX not returning single aggregated row
     *
     * Bug: The engine returns all rows instead of a single aggregated result.
     * Standard behavior: MAX excludes NULL values
     * Expected: single row with max_val=50
     */
    it('should exclude NULL values in MAX', () => {
      const result = db.prepare('SELECT MAX(value) AS max_val FROM t6').all();

      expect(result.length).toBe(1);
      expect((result[0] as { max_val: number }).max_val).toBe(50);
    });

    /**
     * KNOWN FAILURE: SUM of all NULL values should return NULL
     *
     * Bug: The engine returns multiple rows instead of a single aggregated result.
     * Standard behavior: SUM of only NULL values returns NULL (not 0)
     */
    it('should return NULL when SUM of all NULL values', () => {
      db.exec('CREATE TABLE all_nulls (id INTEGER, value INTEGER)');
      db.exec('INSERT INTO all_nulls (id, value) VALUES (1, NULL)');
      db.exec('INSERT INTO all_nulls (id, value) VALUES (2, NULL)');

      const result = db.prepare('SELECT SUM(value) AS total FROM all_nulls').all();

      expect(result.length).toBe(1);
      expect((result[0] as { total: number | null }).total).toBeNull();
    });

    /**
     * KNOWN FAILURE: AVG of all NULL values should return NULL
     *
     * Bug: The engine returns multiple rows instead of a single aggregated result.
     * Standard behavior: AVG of only NULL values returns NULL
     */
    it('should return NULL when AVG of all NULL values', () => {
      db.exec('CREATE TABLE all_nulls_avg (id INTEGER, value INTEGER)');
      db.exec('INSERT INTO all_nulls_avg (id, value) VALUES (1, NULL)');
      db.exec('INSERT INTO all_nulls_avg (id, value) VALUES (2, NULL)');

      const result = db.prepare('SELECT AVG(value) AS avg_val FROM all_nulls_avg').all();

      expect(result.length).toBe(1);
      expect((result[0] as { avg_val: number | null }).avg_val).toBeNull();
    });
  });

  // ===========================================================================
  // AGGREGATES ON EMPTY TABLE
  // ===========================================================================

  describe('Aggregates on Empty Table', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE empty_table (id INTEGER, value INTEGER, name TEXT)
      `);
      // No inserts - table is empty
    });

    /**
     * KNOWN FAILURE: COUNT(*) on empty table should return single row with 0
     *
     * Bug: Empty table returns 0 rows, but aggregate should return 1 row with cnt=0
     */
    it('should return 0 for COUNT(*) on empty table', () => {
      const result = db.prepare('SELECT COUNT(*) AS cnt FROM empty_table').all();

      expect(result.length).toBe(1);
      expect((result[0] as { cnt: number }).cnt).toBe(0);
    });

    /**
     * KNOWN FAILURE: COUNT(column) on empty table should return single row with 0
     *
     * Bug: Empty table returns 0 rows, but aggregate should return 1 row with cnt=0
     */
    it('should return 0 for COUNT(column) on empty table', () => {
      const result = db.prepare('SELECT COUNT(value) AS cnt FROM empty_table').all();

      expect(result.length).toBe(1);
      expect((result[0] as { cnt: number }).cnt).toBe(0);
    });

    /**
     * KNOWN FAILURE: SUM on empty table should return single row with NULL
     *
     * Bug: Empty table returns 0 rows, but aggregate should return 1 row with NULL
     */
    it('should return NULL for SUM on empty table', () => {
      const result = db.prepare('SELECT SUM(value) AS total FROM empty_table').all();

      expect(result.length).toBe(1);
      expect((result[0] as { total: number | null }).total).toBeNull();
    });

    /**
     * KNOWN FAILURE: AVG on empty table should return single row with NULL
     *
     * Bug: Empty table returns 0 rows, but aggregate should return 1 row with NULL
     */
    it('should return NULL for AVG on empty table', () => {
      const result = db.prepare('SELECT AVG(value) AS avg_val FROM empty_table').all();

      expect(result.length).toBe(1);
      expect((result[0] as { avg_val: number | null }).avg_val).toBeNull();
    });

    /**
     * KNOWN FAILURE: MIN on empty table should return single row with NULL
     *
     * Bug: Empty table returns 0 rows, but aggregate should return 1 row with NULL
     */
    it('should return NULL for MIN on empty table', () => {
      const result = db.prepare('SELECT MIN(value) AS min_val FROM empty_table').all();

      expect(result.length).toBe(1);
      expect((result[0] as { min_val: number | null }).min_val).toBeNull();
    });

    /**
     * KNOWN FAILURE: MAX on empty table should return single row with NULL
     *
     * Bug: Empty table returns 0 rows, but aggregate should return 1 row with NULL
     */
    it('should return NULL for MAX on empty table', () => {
      const result = db.prepare('SELECT MAX(value) AS max_val FROM empty_table').all();

      expect(result.length).toBe(1);
      expect((result[0] as { max_val: number | null }).max_val).toBeNull();
    });

    /**
     * KNOWN FAILURE: Multiple aggregates on empty table
     *
     * Bug: Parser does not support multiple aggregates in single SELECT
     * Standard behavior: Should return single row with all aggregate results
     */
    it('should return correct values for multiple aggregates on empty table', () => {
      const result = db.prepare(`
        SELECT
          COUNT(*) AS cnt_all,
          COUNT(value) AS cnt_val,
          SUM(value) AS sum_val,
          AVG(value) AS avg_val,
          MIN(value) AS min_val,
          MAX(value) AS max_val
        FROM empty_table
      `).all();

      expect(result.length).toBe(1);
      const row = result[0] as {
        cnt_all: number;
        cnt_val: number;
        sum_val: number | null;
        avg_val: number | null;
        min_val: number | null;
        max_val: number | null;
      };

      expect(row.cnt_all).toBe(0);
      expect(row.cnt_val).toBe(0);
      expect(row.sum_val).toBeNull();
      expect(row.avg_val).toBeNull();
      expect(row.min_val).toBeNull();
      expect(row.max_val).toBeNull();
    });

    /**
     * KNOWN FAILURE: GROUP BY on empty table should return no rows
     *
     * Bug: Parser does not support GROUP BY properly
     * Standard behavior: GROUP BY on empty table returns 0 rows (no groups)
     */
    it('should return no rows for GROUP BY on empty table', () => {
      const result = db.prepare(
        'SELECT name, COUNT(*) AS cnt FROM empty_table GROUP BY name'
      ).all();

      expect(result.length).toBe(0);
      expect(result).toEqual([]);
    });
  });

  // ===========================================================================
  // COMBINED AGGREGATE QUERIES (Multiple aggregates in single SELECT)
  // ===========================================================================

  describe('Combined Aggregate Queries', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t7 (id INTEGER, category TEXT, value INTEGER)
      `);
      db.exec(`INSERT INTO t7 (id, category, value) VALUES (1, 'A', 10)`);
      db.exec(`INSERT INTO t7 (id, category, value) VALUES (2, 'A', 20)`);
      db.exec(`INSERT INTO t7 (id, category, value) VALUES (3, 'B', 30)`);
      db.exec(`INSERT INTO t7 (id, category, value) VALUES (4, 'B', 40)`);
      db.exec(`INSERT INTO t7 (id, category, value) VALUES (5, 'B', 50)`);
    });

    /**
     * KNOWN FAILURE: Multiple aggregates in single SELECT
     *
     * Bug: Parser throws "Invalid SELECT syntax" for multiple aggregates
     * Standard behavior: Multiple aggregates without GROUP BY should return single row
     */
    it('should compute multiple aggregates in single query', () => {
      const result = db.prepare(`
        SELECT
          COUNT(*) AS cnt,
          SUM(value) AS sum_val,
          AVG(value) AS avg_val,
          MIN(value) AS min_val,
          MAX(value) AS max_val
        FROM t7
      `).all();

      expect(result.length).toBe(1);
      const row = result[0] as {
        cnt: number;
        sum_val: number;
        avg_val: number;
        min_val: number;
        max_val: number;
      };

      expect(row.cnt).toBe(5);
      expect(row.sum_val).toBe(150);
      expect(row.avg_val).toBe(30);
      expect(row.min_val).toBe(10);
      expect(row.max_val).toBe(50);
    });

    /**
     * KNOWN FAILURE: Multiple aggregates with GROUP BY
     *
     * Bug: Parser throws "Invalid SELECT syntax" for multiple aggregates
     * Standard behavior: Should compute all aggregates per group
     */
    it('should compute multiple aggregates per group', () => {
      const result = db.prepare(`
        SELECT
          category,
          COUNT(*) AS cnt,
          SUM(value) AS sum_val,
          AVG(value) AS avg_val,
          MIN(value) AS min_val,
          MAX(value) AS max_val
        FROM t7
        GROUP BY category
      `).all();

      const sorted = (result as {
        category: string;
        cnt: number;
        sum_val: number;
        avg_val: number;
        min_val: number;
        max_val: number;
      }[]).sort((a, b) => a.category.localeCompare(b.category));

      expect(sorted.length).toBe(2);

      // Category A
      expect(sorted[0].category).toBe('A');
      expect(sorted[0].cnt).toBe(2);
      expect(sorted[0].sum_val).toBe(30);
      expect(sorted[0].avg_val).toBe(15);
      expect(sorted[0].min_val).toBe(10);
      expect(sorted[0].max_val).toBe(20);

      // Category B
      expect(sorted[1].category).toBe('B');
      expect(sorted[1].cnt).toBe(3);
      expect(sorted[1].sum_val).toBe(120);
      expect(sorted[1].avg_val).toBe(40);
      expect(sorted[1].min_val).toBe(30);
      expect(sorted[1].max_val).toBe(50);
    });
  });

  // ===========================================================================
  // EDGE CASES: Single Row Tables
  // ===========================================================================

  describe('Aggregates on Single Row Table', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE single_row (id INTEGER, value INTEGER)
      `);
      db.exec(`INSERT INTO single_row (id, value) VALUES (1, 42)`);
    });

    /**
     * KNOWN FAILURE: COUNT(*) on single row table
     *
     * Bug: Returns null instead of aggregated count
     */
    it('should handle COUNT(*) on single row', () => {
      const result = db.prepare('SELECT COUNT(*) AS cnt FROM single_row').all();
      expect(result.length).toBe(1);
      expect((result[0] as { cnt: number }).cnt).toBe(1);
    });

    /**
     * KNOWN FAILURE: SUM on single row table
     *
     * Bug: Returns null instead of aggregated sum
     */
    it('should handle SUM on single row', () => {
      const result = db.prepare('SELECT SUM(value) AS total FROM single_row').all();
      expect(result.length).toBe(1);
      expect((result[0] as { total: number }).total).toBe(42);
    });

    /**
     * KNOWN FAILURE: AVG on single row table
     *
     * Bug: Returns null instead of aggregated average
     */
    it('should handle AVG on single row', () => {
      const result = db.prepare('SELECT AVG(value) AS avg_val FROM single_row').all();
      expect(result.length).toBe(1);
      expect((result[0] as { avg_val: number }).avg_val).toBe(42);
    });

    /**
     * KNOWN FAILURE: MIN on single row table
     *
     * Bug: Returns null instead of aggregated minimum
     */
    it('should handle MIN on single row', () => {
      const result = db.prepare('SELECT MIN(value) AS min_val FROM single_row').all();
      expect(result.length).toBe(1);
      expect((result[0] as { min_val: number }).min_val).toBe(42);
    });

    /**
     * KNOWN FAILURE: MAX on single row table
     *
     * Bug: Returns null instead of aggregated maximum
     */
    it('should handle MAX on single row', () => {
      const result = db.prepare('SELECT MAX(value) AS max_val FROM single_row').all();
      expect(result.length).toBe(1);
      expect((result[0] as { max_val: number }).max_val).toBe(42);
    });
  });

  // ===========================================================================
  // EDGE CASES: Large Numbers and Overflow
  // ===========================================================================

  describe('Large Numbers in Aggregates', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE large_nums (id INTEGER, value INTEGER)
      `);
      // Use large but safe integers
      db.exec(`INSERT INTO large_nums (id, value) VALUES (1, 1000000000)`);
      db.exec(`INSERT INTO large_nums (id, value) VALUES (2, 2000000000)`);
      db.exec(`INSERT INTO large_nums (id, value) VALUES (3, 3000000000)`);
    });

    /**
     * KNOWN FAILURE: SUM of large numbers
     *
     * Bug: Returns null instead of aggregated sum
     */
    it('should handle SUM of large numbers', () => {
      const result = db.prepare('SELECT SUM(value) AS total FROM large_nums').all();
      expect(result.length).toBe(1);
      expect((result[0] as { total: number }).total).toBe(6000000000);
    });

    /**
     * KNOWN FAILURE: AVG of large numbers
     *
     * Bug: Returns null instead of aggregated average
     */
    it('should handle AVG of large numbers', () => {
      const result = db.prepare('SELECT AVG(value) AS avg_val FROM large_nums').all();
      expect(result.length).toBe(1);
      expect((result[0] as { avg_val: number }).avg_val).toBe(2000000000);
    });
  });

  // ===========================================================================
  // EDGE CASES: Floating Point Precision
  // ===========================================================================

  describe('Floating Point Precision in Aggregates', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE floats (id INTEGER, value REAL)
      `);
      db.exec(`INSERT INTO floats (id, value) VALUES (1, 0.1)`);
      db.exec(`INSERT INTO floats (id, value) VALUES (2, 0.2)`);
      db.exec(`INSERT INTO floats (id, value) VALUES (3, 0.3)`);
    });

    /**
     * KNOWN FAILURE: SUM of floating point numbers
     *
     * Bug: Returns null instead of aggregated sum
     */
    it('should handle SUM of floating point numbers', () => {
      const result = db.prepare('SELECT SUM(value) AS total FROM floats').all();
      expect(result.length).toBe(1);
      // 0.1 + 0.2 + 0.3 should be close to 0.6 (floating point imprecision possible)
      const total = (result[0] as { total: number }).total;
      expect(total).toBeCloseTo(0.6, 10);
    });

    /**
     * KNOWN FAILURE: AVG of floating point numbers
     *
     * Bug: Returns null instead of aggregated average
     */
    it('should handle AVG of floating point numbers', () => {
      const result = db.prepare('SELECT AVG(value) AS avg_val FROM floats').all();
      expect(result.length).toBe(1);
      const avg = (result[0] as { avg_val: number }).avg_val;
      expect(avg).toBeCloseTo(0.2, 10);
    });
  });
});
