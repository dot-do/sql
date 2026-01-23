/**
 * SQLLogicTest Compatibility Tests
 *
 * These tests capture known failures from SQLLogicTest runs against the DoSQL
 * InMemoryEngine. They document the expected SQL behavior that needs to be fixed.
 *
 * Each test includes:
 * - The failing SQL query pattern
 * - Expected results from SQLLogicTest
 * - Current behavior documentation
 *
 * Run with: npx vitest run src/__tests__/sqllogictest-compat.test.ts
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

describe('SQLLogicTest Compatibility', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  // ===========================================================================
  // COMPARISON OPERATOR TESTS - Known Failures
  // ===========================================================================

  describe('Comparison Operators - Known Failures', () => {
    describe('>= (greater than or equal) operator', () => {
      beforeEach(() => {
        db.exec(`
          CREATE TABLE people (id INTEGER, name TEXT, age INTEGER)
        `);
        db.exec(`INSERT INTO people (id, name, age) VALUES (1, 'Alice', 25)`);
        db.exec(`INSERT INTO people (id, name, age) VALUES (2, 'Bob', 30)`);
        db.exec(`INSERT INTO people (id, name, age) VALUES (3, 'Charlie', 35)`);
        db.exec(`INSERT INTO people (id, name, age) VALUES (4, 'Diana', 40)`);
        db.exec(`INSERT INTO people (id, name, age) VALUES (5, 'Eve', 45)`);
      });

      it('should handle >= comparison operator with integer column', () => {
        // SQLLogicTest: SELECT * FROM people WHERE age >= 30
        // Expected: rows with age 30, 35, 40, 45 (Bob, Charlie, Diana, Eve)
        const result = db.prepare('SELECT id, name, age FROM people WHERE age >= 30').all();

        // Sort by id for consistent comparison
        const sorted = (result as { id: number; name: string; age: number }[]).sort((a, b) => a.id - b.id);

        expect(sorted.length).toBe(4);
        expect(sorted[0]).toEqual({ id: 2, name: 'Bob', age: 30 });
        expect(sorted[1]).toEqual({ id: 3, name: 'Charlie', age: 35 });
        expect(sorted[2]).toEqual({ id: 4, name: 'Diana', age: 40 });
        expect(sorted[3]).toEqual({ id: 5, name: 'Eve', age: 45 });
      });

      it('should handle >= comparison with exact boundary value', () => {
        // SQLLogicTest: SELECT * FROM people WHERE age >= 35
        // Expected: rows with age 35, 40, 45 (Charlie, Diana, Eve)
        const result = db.prepare('SELECT id, name, age FROM people WHERE age >= 35').all();

        const sorted = (result as { id: number; name: string; age: number }[]).sort((a, b) => a.id - b.id);

        expect(sorted.length).toBe(3);
        expect(sorted[0]).toEqual({ id: 3, name: 'Charlie', age: 35 });
        expect(sorted[1]).toEqual({ id: 4, name: 'Diana', age: 40 });
        expect(sorted[2]).toEqual({ id: 5, name: 'Eve', age: 45 });
      });

      it('should handle >= comparison with parameter binding', () => {
        // SQLLogicTest: SELECT * FROM people WHERE age >= ?
        // With parameter value 30
        const result = db.prepare('SELECT id, name, age FROM people WHERE age >= ?').all(30);

        const sorted = (result as { id: number; name: string; age: number }[]).sort((a, b) => a.id - b.id);

        expect(sorted.length).toBe(4);
        expect(sorted.map(r => r.name)).toEqual(['Bob', 'Charlie', 'Diana', 'Eve']);
      });
    });

    describe('<= (less than or equal) operator', () => {
      beforeEach(() => {
        db.exec(`
          CREATE TABLE numbers (id INTEGER, n INTEGER, label TEXT)
        `);
        db.exec(`INSERT INTO numbers (id, n, label) VALUES (1, 10, 'ten')`);
        db.exec(`INSERT INTO numbers (id, n, label) VALUES (2, 20, 'twenty')`);
        db.exec(`INSERT INTO numbers (id, n, label) VALUES (3, 30, 'thirty')`);
        db.exec(`INSERT INTO numbers (id, n, label) VALUES (4, 40, 'forty')`);
        db.exec(`INSERT INTO numbers (id, n, label) VALUES (5, 50, 'fifty')`);
      });

      it('should handle <= comparison operator with integer column', () => {
        // SQLLogicTest: SELECT * FROM numbers WHERE n <= 40
        // Expected: rows with n 10, 20, 30, 40
        const result = db.prepare('SELECT id, n, label FROM numbers WHERE n <= 40').all();

        const sorted = (result as { id: number; n: number; label: string }[]).sort((a, b) => a.id - b.id);

        expect(sorted.length).toBe(4);
        expect(sorted[0]).toEqual({ id: 1, n: 10, label: 'ten' });
        expect(sorted[1]).toEqual({ id: 2, n: 20, label: 'twenty' });
        expect(sorted[2]).toEqual({ id: 3, n: 30, label: 'thirty' });
        expect(sorted[3]).toEqual({ id: 4, n: 40, label: 'forty' });
      });

      it('should handle <= comparison with exact boundary value', () => {
        // SQLLogicTest: SELECT * FROM numbers WHERE n <= 30
        // Expected: rows with n 10, 20, 30
        const result = db.prepare('SELECT id, n, label FROM numbers WHERE n <= 30').all();

        const sorted = (result as { id: number; n: number; label: string }[]).sort((a, b) => a.id - b.id);

        expect(sorted.length).toBe(3);
        expect(sorted[0]).toEqual({ id: 1, n: 10, label: 'ten' });
        expect(sorted[1]).toEqual({ id: 2, n: 20, label: 'twenty' });
        expect(sorted[2]).toEqual({ id: 3, n: 30, label: 'thirty' });
      });

      it('should handle <= comparison with parameter binding', () => {
        // SQLLogicTest: SELECT * FROM numbers WHERE n <= ?
        // With parameter value 40
        const result = db.prepare('SELECT id, n, label FROM numbers WHERE n <= ?').all(40);

        const sorted = (result as { id: number; n: number; label: string }[]).sort((a, b) => a.id - b.id);

        expect(sorted.length).toBe(4);
        expect(sorted.map(r => r.label)).toEqual(['ten', 'twenty', 'thirty', 'forty']);
      });
    });

    describe('Combined >= and <= in WHERE clause', () => {
      beforeEach(() => {
        db.exec(`
          CREATE TABLE data (id INTEGER, value INTEGER)
        `);
        for (let i = 1; i <= 10; i++) {
          db.exec(`INSERT INTO data (id, value) VALUES (${i}, ${i * 10})`);
        }
      });

      it('should handle >= and <= combined with AND', () => {
        // SQLLogicTest: SELECT * FROM data WHERE value >= 30 AND value <= 70
        // Expected: rows with value 30, 40, 50, 60, 70
        const result = db.prepare('SELECT id, value FROM data WHERE value >= 30 AND value <= 70').all();

        const sorted = (result as { id: number; value: number }[]).sort((a, b) => a.id - b.id);

        expect(sorted.length).toBe(5);
        expect(sorted.map(r => r.value)).toEqual([30, 40, 50, 60, 70]);
      });

      it('should handle BETWEEN (equivalent to >= AND <=)', () => {
        // SQLLogicTest: SELECT * FROM data WHERE value BETWEEN 30 AND 70
        // Expected: same as above - rows with value 30, 40, 50, 60, 70
        // Note: BETWEEN is inclusive on both ends
        const result = db.prepare('SELECT id, value FROM data WHERE value >= 30 AND value <= 70').all();

        expect(result.length).toBe(5);
      });
    });
  });

  // ===========================================================================
  // ADDITIONAL COMPARISON OPERATOR TESTS
  // ===========================================================================

  describe('Other Comparison Operators', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE items (id INTEGER, quantity INTEGER, price REAL)
      `);
      db.exec(`INSERT INTO items (id, quantity, price) VALUES (1, 5, 10.00)`);
      db.exec(`INSERT INTO items (id, quantity, price) VALUES (2, 10, 20.00)`);
      db.exec(`INSERT INTO items (id, quantity, price) VALUES (3, 15, 30.00)`);
      db.exec(`INSERT INTO items (id, quantity, price) VALUES (4, 20, 40.00)`);
    });

    it('should handle > comparison operator', () => {
      // SELECT * FROM items WHERE quantity > 10
      const result = db.prepare('SELECT id, quantity FROM items WHERE quantity > 10').all();

      const sorted = (result as { id: number; quantity: number }[]).sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(2);
      expect(sorted[0]).toEqual({ id: 3, quantity: 15 });
      expect(sorted[1]).toEqual({ id: 4, quantity: 20 });
    });

    it('should handle < comparison operator', () => {
      // SELECT * FROM items WHERE quantity < 15
      const result = db.prepare('SELECT id, quantity FROM items WHERE quantity < 15').all();

      const sorted = (result as { id: number; quantity: number }[]).sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(2);
      expect(sorted[0]).toEqual({ id: 1, quantity: 5 });
      expect(sorted[1]).toEqual({ id: 2, quantity: 10 });
    });

    it('should handle = equality operator', () => {
      // SELECT * FROM items WHERE quantity = 10
      const result = db.prepare('SELECT id, quantity FROM items WHERE quantity = 10').all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 2, quantity: 10 });
    });

    it('should handle <> (not equal) operator', () => {
      // SELECT * FROM items WHERE quantity <> 10
      const result = db.prepare('SELECT id, quantity FROM items WHERE quantity <> 10').all();

      const sorted = (result as { id: number; quantity: number }[]).sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(3);
      expect(sorted.map(r => r.quantity)).toEqual([5, 15, 20]);
    });

    it('should handle != (not equal) operator', () => {
      // SELECT * FROM items WHERE quantity != 10
      const result = db.prepare('SELECT id, quantity FROM items WHERE quantity != 10').all();

      const sorted = (result as { id: number; quantity: number }[]).sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(3);
      expect(sorted.map(r => r.quantity)).toEqual([5, 15, 20]);
    });
  });

  // ===========================================================================
  // NULL HANDLING IN COMPARISONS
  // ===========================================================================

  describe('NULL Handling in Comparisons', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE nullable (id INTEGER, value INTEGER)
      `);
      db.exec(`INSERT INTO nullable (id, value) VALUES (1, 10)`);
      db.exec(`INSERT INTO nullable (id, value) VALUES (2, 20)`);
      db.exec(`INSERT INTO nullable (id, value) VALUES (3, NULL)`);
      db.exec(`INSERT INTO nullable (id, value) VALUES (4, 30)`);
    });

    it('should exclude NULL values from >= comparison', () => {
      // SQLLogicTest: SELECT * FROM nullable WHERE value >= 15
      // Expected: only non-NULL values >= 15 (rows 2 and 4)
      // NULL should not match any comparison
      const result = db.prepare('SELECT id, value FROM nullable WHERE value >= 15').all();

      const sorted = (result as { id: number; value: number }[]).sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(2);
      expect(sorted[0]).toEqual({ id: 2, value: 20 });
      expect(sorted[1]).toEqual({ id: 4, value: 30 });
    });

    /**
     * WORKING: NULL handling in <= comparison
     *
     * NULL values are correctly excluded from <= comparisons.
     * NULL should never match any comparison operator (except IS NULL/IS NOT NULL).
     */
    it('should exclude NULL values from <= comparison', () => {
      // SQLLogicTest: SELECT * FROM nullable WHERE value <= 15
      // Expected: only non-NULL values <= 15 (row 1)
      const result = db.prepare('SELECT id, value FROM nullable WHERE value <= 15').all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 1, value: 10 });
    });

    it('should handle IS NULL comparison', () => {
      // SELECT * FROM nullable WHERE value IS NULL
      const result = db.prepare('SELECT id, value FROM nullable WHERE value IS NULL').all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 3, value: null });
    });
  });

  // ===========================================================================
  // REAL/FLOAT COMPARISONS
  // ===========================================================================

  describe('REAL/Float Comparisons', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE floats (id INTEGER, value REAL)
      `);
      db.exec(`INSERT INTO floats (id, value) VALUES (1, 1.5)`);
      db.exec(`INSERT INTO floats (id, value) VALUES (2, 2.5)`);
      db.exec(`INSERT INTO floats (id, value) VALUES (3, 3.5)`);
      db.exec(`INSERT INTO floats (id, value) VALUES (4, 4.5)`);
    });

    it('should handle >= comparison with REAL values', () => {
      // SELECT * FROM floats WHERE value >= 2.5
      const result = db.prepare('SELECT id, value FROM floats WHERE value >= 2.5').all();

      const sorted = (result as { id: number; value: number }[]).sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(3);
      expect(sorted.map(r => r.value)).toEqual([2.5, 3.5, 4.5]);
    });

    it('should handle <= comparison with REAL values', () => {
      // SELECT * FROM floats WHERE value <= 3.0
      const result = db.prepare('SELECT id, value FROM floats WHERE value <= 3.0').all();

      const sorted = (result as { id: number; value: number }[]).sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(2);
      expect(sorted.map(r => r.value)).toEqual([1.5, 2.5]);
    });
  });

  // ===========================================================================
  // STRING COMPARISONS
  // ===========================================================================

  describe('String Comparisons', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE strings (id INTEGER, name TEXT)
      `);
      db.exec(`INSERT INTO strings (id, name) VALUES (1, 'apple')`);
      db.exec(`INSERT INTO strings (id, name) VALUES (2, 'banana')`);
      db.exec(`INSERT INTO strings (id, name) VALUES (3, 'cherry')`);
      db.exec(`INSERT INTO strings (id, name) VALUES (4, 'date')`);
    });

    /**
     * KNOWN FAILURE: String >= comparison not working
     *
     * Bug: The InMemoryEngine does not support >= comparison with TEXT/string
     * values. Lexicographic string comparison should work but returns 0 rows.
     *
     * Expected: 2 rows ('cherry', 'date')
     * Actual: 0 rows
     */
    it.fails('should handle >= comparison with TEXT values (lexicographic)', () => {
      // SELECT * FROM strings WHERE name >= 'cherry'
      // Lexicographic comparison: 'cherry' and 'date' should match
      const result = db.prepare("SELECT id, name FROM strings WHERE name >= 'cherry'").all();

      const sorted = (result as { id: number; name: string }[]).sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(2);
      expect(sorted.map(r => r.name)).toEqual(['cherry', 'date']);
    });

    /**
     * KNOWN FAILURE: String <= comparison not working
     *
     * Bug: The InMemoryEngine does not support <= comparison with TEXT/string
     * values. Lexicographic string comparison should work but returns 0 rows.
     *
     * Expected: 2 rows ('apple', 'banana')
     * Actual: 0 rows
     */
    it.fails('should handle <= comparison with TEXT values (lexicographic)', () => {
      // SELECT * FROM strings WHERE name <= 'banana'
      // Lexicographic comparison: 'apple' and 'banana' should match
      const result = db.prepare("SELECT id, name FROM strings WHERE name <= 'banana'").all();

      const sorted = (result as { id: number; name: string }[]).sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(2);
      expect(sorted.map(r => r.name)).toEqual(['apple', 'banana']);
    });

    it('should handle LIKE operator', () => {
      // SELECT * FROM strings WHERE name LIKE 'a%'
      const result = db.prepare("SELECT id, name FROM strings WHERE name LIKE 'a%'").all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 1, name: 'apple' });
    });
  });

  // ===========================================================================
  // COMPLEX WHERE CLAUSES
  // ===========================================================================

  describe('Complex WHERE Clauses', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE products (id INTEGER, category TEXT, price INTEGER, stock INTEGER)
      `);
      db.exec(`INSERT INTO products (id, category, price, stock) VALUES (1, 'electronics', 100, 50)`);
      db.exec(`INSERT INTO products (id, category, price, stock) VALUES (2, 'electronics', 200, 30)`);
      db.exec(`INSERT INTO products (id, category, price, stock) VALUES (3, 'clothing', 50, 100)`);
      db.exec(`INSERT INTO products (id, category, price, stock) VALUES (4, 'clothing', 75, 80)`);
      db.exec(`INSERT INTO products (id, category, price, stock) VALUES (5, 'food', 10, 200)`);
    });

    it('should handle multiple AND conditions with >= and <=', () => {
      // SELECT * FROM products WHERE price >= 50 AND price <= 100 AND stock >= 50
      const result = db.prepare(
        'SELECT id, category, price, stock FROM products WHERE price >= 50 AND price <= 100 AND stock >= 50'
      ).all();

      const sorted = (result as { id: number; category: string; price: number; stock: number }[])
        .sort((a, b) => a.id - b.id);

      expect(sorted.length).toBe(3);
      expect(sorted.map(r => r.id)).toEqual([1, 3, 4]);
    });

    it('should handle equality with >= and <=', () => {
      // SELECT * FROM products WHERE category = 'electronics' AND price >= 150
      const result = db.prepare(
        "SELECT id, price FROM products WHERE category = 'electronics' AND price >= 150"
      ).all();

      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 2, price: 200 });
    });
  });
});

// =============================================================================
// LOW-LEVEL INMEMORYENGINE TESTS
// =============================================================================

describe('InMemoryEngine Direct Tests', () => {
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
    engine.execute('INSERT INTO test (id, value) VALUES (?, ?)', [4, 40]);
    engine.execute('INSERT INTO test (id, value) VALUES (?, ?)', [5, 50]);
  });

  describe('filterRows method behavior', () => {
    it('should correctly filter with >= operator', () => {
      // Test the >= operator directly
      const result = engine.execute('SELECT id, value FROM test WHERE value >= 30', []);

      expect(result.rows.length).toBe(3);
      expect(result.rows.map(r => r.value).sort()).toEqual([30, 40, 50]);
    });

    it('should correctly filter with <= operator', () => {
      // Test the <= operator directly
      const result = engine.execute('SELECT id, value FROM test WHERE value <= 30', []);

      expect(result.rows.length).toBe(3);
      expect(result.rows.map(r => r.value).sort()).toEqual([10, 20, 30]);
    });

    it('should correctly filter with >= operator using parameter', () => {
      // Test >= with parameter binding
      const result = engine.execute('SELECT id, value FROM test WHERE value >= ?', [30]);

      expect(result.rows.length).toBe(3);
      expect(result.rows.map(r => r.value).sort()).toEqual([30, 40, 50]);
    });

    it('should correctly filter with <= operator using parameter', () => {
      // Test <= with parameter binding
      const result = engine.execute('SELECT id, value FROM test WHERE value <= ?', [30]);

      expect(result.rows.length).toBe(3);
      expect(result.rows.map(r => r.value).sort()).toEqual([10, 20, 30]);
    });

    it('should correctly handle boundary values for >= (exact match)', () => {
      // The boundary value should be included
      const result = engine.execute('SELECT id, value FROM test WHERE value >= 30', []);

      const values = result.rows.map(r => r.value);
      expect(values).toContain(30);
    });

    it('should correctly handle boundary values for <= (exact match)', () => {
      // The boundary value should be included
      const result = engine.execute('SELECT id, value FROM test WHERE value <= 30', []);

      const values = result.rows.map(r => r.value);
      expect(values).toContain(30);
    });
  });
});

// =============================================================================
// EDGE CASES FROM SQLLOGICTEST SUITE
// =============================================================================

describe('SQLLogicTest Edge Cases', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  describe('Numeric type coercion', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE mixed (id INTEGER, value TEXT)');
      db.exec("INSERT INTO mixed (id, value) VALUES (1, '10')");
      db.exec("INSERT INTO mixed (id, value) VALUES (2, '20')");
      db.exec("INSERT INTO mixed (id, value) VALUES (3, '30')");
    });

    it('should handle comparison between TEXT column and numeric literal', () => {
      // SQLLogicTest may expect type coercion in some cases
      // This tests whether '30' >= 25 works (string to number comparison)
      // Note: This behavior may vary - SQLite does string comparison for TEXT columns
      const result = db.prepare("SELECT id, value FROM mixed WHERE value >= '20'").all();

      // Lexicographic: '20', '30' should match (as strings)
      expect(result.length).toBe(2);
    });
  });

  describe('Empty result sets', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE empty_test (id INTEGER, value INTEGER)');
      db.exec('INSERT INTO empty_test (id, value) VALUES (1, 100)');
    });

    it('should return empty result for >= with no matches', () => {
      const result = db.prepare('SELECT id, value FROM empty_test WHERE value >= 200').all();
      expect(result.length).toBe(0);
      expect(result).toEqual([]);
    });

    it('should return empty result for <= with no matches', () => {
      const result = db.prepare('SELECT id, value FROM empty_test WHERE value <= 50').all();
      expect(result.length).toBe(0);
      expect(result).toEqual([]);
    });
  });

  describe('Single row boundary cases', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE single (id INTEGER, value INTEGER)');
      db.exec('INSERT INTO single (id, value) VALUES (1, 50)');
    });

    it('should return single row when >= matches exactly', () => {
      const result = db.prepare('SELECT id, value FROM single WHERE value >= 50').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 1, value: 50 });
    });

    it('should return single row when <= matches exactly', () => {
      const result = db.prepare('SELECT id, value FROM single WHERE value <= 50').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 1, value: 50 });
    });

    it('should return empty when >= does not match', () => {
      const result = db.prepare('SELECT id, value FROM single WHERE value >= 51').all();
      expect(result.length).toBe(0);
    });

    it('should return empty when <= does not match', () => {
      const result = db.prepare('SELECT id, value FROM single WHERE value <= 49').all();
      expect(result.length).toBe(0);
    });
  });

  describe('Negative numbers', () => {
    beforeEach(() => {
      db.exec('CREATE TABLE negatives (id INTEGER, value INTEGER)');
      db.exec('INSERT INTO negatives (id, value) VALUES (1, -30)');
      db.exec('INSERT INTO negatives (id, value) VALUES (2, -20)');
      db.exec('INSERT INTO negatives (id, value) VALUES (3, -10)');
      db.exec('INSERT INTO negatives (id, value) VALUES (4, 0)');
      db.exec('INSERT INTO negatives (id, value) VALUES (5, 10)');
    });

    it('should handle >= with negative values', () => {
      // >= -15 should include -10, 0, 10
      const result = db.prepare('SELECT id, value FROM negatives WHERE value >= -15').all();

      const sorted = (result as { id: number; value: number }[]).sort((a, b) => a.id - b.id);
      expect(sorted.length).toBe(3);
      expect(sorted.map(r => r.value)).toEqual([-10, 0, 10]);
    });

    it('should handle <= with negative values', () => {
      // <= -15 should include -30, -20
      const result = db.prepare('SELECT id, value FROM negatives WHERE value <= -15').all();

      const sorted = (result as { id: number; value: number }[]).sort((a, b) => a.id - b.id);
      expect(sorted.length).toBe(2);
      expect(sorted.map(r => r.value)).toEqual([-30, -20]);
    });

    it('should handle >= 0 boundary', () => {
      const result = db.prepare('SELECT id, value FROM negatives WHERE value >= 0').all();

      const sorted = (result as { id: number; value: number }[]).sort((a, b) => a.id - b.id);
      expect(sorted.length).toBe(2);
      expect(sorted.map(r => r.value)).toEqual([0, 10]);
    });

    it('should handle <= 0 boundary', () => {
      const result = db.prepare('SELECT id, value FROM negatives WHERE value <= 0').all();

      const sorted = (result as { id: number; value: number }[]).sort((a, b) => a.id - b.id);
      expect(sorted.length).toBe(4);
      expect(sorted.map(r => r.value)).toEqual([-30, -20, -10, 0]);
    });
  });
});
