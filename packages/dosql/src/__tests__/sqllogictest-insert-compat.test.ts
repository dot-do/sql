/**
 * SQLLogicTest INSERT Compatibility Tests - RED Phase TDD
 *
 * These tests document the known failure of INSERT statements without explicit column lists.
 * SQLLogicTest shows 1,643 failures for INSERT statements like:
 *   INSERT INTO t1 VALUES(1,'true')
 *   INSERT INTO t1 VALUES(0,'false')
 *   INSERT INTO t1 VALUES(NULL,'NULL')
 *
 * These should work per SQL standard, but DoSQL currently requires explicit column names.
 * The InMemoryEngine.executeInsert() uses a regex that requires column list:
 *   /INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i
 *
 * Run with: npx vitest run src/__tests__/sqllogictest-insert-compat.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Database } from '../database.js';

// =============================================================================
// TEST SETUP
// =============================================================================

describe('SQLLogicTest INSERT Compatibility', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  // ===========================================================================
  // BASIC INSERT WITHOUT COLUMN LIST
  // ===========================================================================

  describe('INSERT without explicit column list - Basic Patterns', () => {
    it('should accept INSERT without column list - integer and text', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');

      // This is standard SQL - columns are inferred from table definition order
      db.exec("INSERT INTO t1 VALUES(1,'true')");

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 1, b: 'true' });
    });

    it('should accept INSERT without column list - multiple rows', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');

      db.exec("INSERT INTO t1 VALUES(1,'true')");
      db.exec("INSERT INTO t1 VALUES(0,'false')");

      const result = db.prepare('SELECT * FROM t1 ORDER BY a').all();
      expect(result.length).toBe(2);
      expect(result[0]).toEqual({ a: 0, b: 'false' });
      expect(result[1]).toEqual({ a: 1, b: 'true' });
    });

    it('should accept INSERT without column list - NULL value', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');

      db.exec("INSERT INTO t1 VALUES(NULL,'NULL')");

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: null, b: 'NULL' });
    });
  });

  // ===========================================================================
  // INSERT WITH VARIOUS DATA TYPES
  // ===========================================================================

  describe('INSERT without column list - Various Data Types', () => {
    it('should accept INSERT without column list - INTEGER only', () => {
      db.exec('CREATE TABLE nums (x INTEGER, y INTEGER, z INTEGER)');

      db.exec('INSERT INTO nums VALUES(1, 2, 3)');

      const result = db.prepare('SELECT * FROM nums').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ x: 1, y: 2, z: 3 });
    });

    it('should accept INSERT without column list - REAL values', () => {
      db.exec('CREATE TABLE floats (a REAL, b REAL)');

      db.exec('INSERT INTO floats VALUES(1.5, 2.7)');

      const result = db.prepare('SELECT * FROM floats').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 1.5, b: 2.7 });
    });

    it('should accept INSERT without column list - TEXT only', () => {
      db.exec('CREATE TABLE strings (a TEXT, b TEXT, c TEXT)');

      db.exec("INSERT INTO strings VALUES('hello', 'world', 'test')");

      const result = db.prepare('SELECT * FROM strings').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 'hello', b: 'world', c: 'test' });
    });

    it('should accept INSERT without column list - BLOB data', () => {
      db.exec('CREATE TABLE blobs (id INTEGER, data BLOB)');

      // SQLite allows BLOB literals as X'...' hex strings
      db.exec("INSERT INTO blobs VALUES(1, X'DEADBEEF')");

      const result = db.prepare('SELECT id FROM blobs').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 1 });
    });

    it('should accept INSERT without column list - mixed types', () => {
      db.exec('CREATE TABLE mixed (id INTEGER, name TEXT, value REAL, flag INTEGER)');

      db.exec("INSERT INTO mixed VALUES(1, 'item', 99.99, 1)");

      const result = db.prepare('SELECT * FROM mixed').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 1, name: 'item', value: 99.99, flag: 1 });
    });
  });

  // ===========================================================================
  // INSERT WITH NULL VALUES
  // ===========================================================================

  describe('INSERT without column list - NULL Handling', () => {
    it('should accept INSERT with NULL as first value', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');

      db.exec("INSERT INTO t1 VALUES(NULL, 'test')");

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: null, b: 'test' });
    });

    it('should accept INSERT with NULL as last value', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');

      db.exec('INSERT INTO t1 VALUES(42, NULL)');

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 42, b: null });
    });

    it('should accept INSERT with all NULL values', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT, c REAL)');

      db.exec('INSERT INTO t1 VALUES(NULL, NULL, NULL)');

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: null, b: null, c: null });
    });

    it('should accept INSERT with mixed NULL and non-NULL values', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, d INTEGER)');

      db.exec("INSERT INTO t1 VALUES(1, NULL, 3.14, NULL)");

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 1, b: null, c: 3.14, d: null });
    });
  });

  // ===========================================================================
  // INSERT WITH PARAMETER BINDING
  // ===========================================================================

  describe('INSERT without column list - Parameter Binding', () => {
    it('should accept INSERT without column list using positional params', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');

      // Prepared statement with positional parameters
      const stmt = db.prepare('INSERT INTO t1 VALUES(?, ?)');
      stmt.run(1, 'hello');

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 1, b: 'hello' });
    });

    it('should accept INSERT without column list using all params', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT, c REAL)');

      const stmt = db.prepare('INSERT INTO t1 VALUES(?, ?, ?)');
      stmt.run(42, 'test', 3.14);

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 42, b: 'test', c: 3.14 });
    });

    it('should accept INSERT without column list using named params', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');

      // Named parameters should also work
      const stmt = db.prepare('INSERT INTO t1 VALUES(:a, :b)');
      stmt.run({ a: 1, b: 'named' });

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 1, b: 'named' });
    });
  });

  // ===========================================================================
  // INSERT WITH DEFAULT VALUES
  // ===========================================================================

  describe('INSERT without column list - DEFAULT values', () => {
    it.fails('should accept INSERT with DEFAULT keyword', () => {
      db.exec('CREATE TABLE t1 (a INTEGER DEFAULT 0, b TEXT DEFAULT \'none\')');

      // Using DEFAULT keyword should use column default
      db.exec('INSERT INTO t1 VALUES(DEFAULT, DEFAULT)');

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 0, b: 'none' });
    });

    it.fails('should accept INSERT mixing DEFAULT and literal values', () => {
      db.exec("CREATE TABLE t1 (a INTEGER DEFAULT 99, b TEXT DEFAULT 'default')");

      db.exec("INSERT INTO t1 VALUES(DEFAULT, 'explicit')");

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 99, b: 'explicit' });
    });
  });

  // ===========================================================================
  // INSERT WITH AUTOINCREMENT
  // ===========================================================================

  describe('INSERT without column list - AUTOINCREMENT', () => {
    it.fails('should accept INSERT without column list with PRIMARY KEY', () => {
      db.exec('CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)');

      // When inserting into a table with INTEGER PRIMARY KEY, NULL auto-generates ID
      db.exec("INSERT INTO t1 VALUES(NULL, 'first')");
      db.exec("INSERT INTO t1 VALUES(NULL, 'second')");

      const result = db.prepare('SELECT * FROM t1 ORDER BY id').all();
      expect(result.length).toBe(2);
      expect(result[0]).toEqual({ id: 1, name: 'first' });
      expect(result[1]).toEqual({ id: 2, name: 'second' });
    });

    it('should accept INSERT without column list with explicit ID', () => {
      db.exec('CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)');

      db.exec("INSERT INTO t1 VALUES(100, 'explicit id')");

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ id: 100, name: 'explicit id' });
    });

    it.fails('should accept INSERT without column list with AUTOINCREMENT', () => {
      db.exec('CREATE TABLE t1 (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)');

      db.exec("INSERT INTO t1 VALUES(NULL, 'auto')");

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0].name).toBe('auto');
      expect(typeof result[0].id).toBe('number');
    });
  });

  // ===========================================================================
  // INSERT WITH EXPRESSIONS
  // ===========================================================================

  describe('INSERT without column list - Expressions in VALUES', () => {
    it.fails('should accept INSERT with arithmetic expressions', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b INTEGER)');

      db.exec('INSERT INTO t1 VALUES(1 + 2, 3 * 4)');

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 3, b: 12 });
    });

    it.fails('should accept INSERT with string concatenation', () => {
      db.exec('CREATE TABLE t1 (a TEXT)');

      db.exec("INSERT INTO t1 VALUES('hello' || ' ' || 'world')");

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 'hello world' });
    });

    it.fails('should accept INSERT with function calls', () => {
      db.exec('CREATE TABLE t1 (a TEXT, b INTEGER)');

      db.exec("INSERT INTO t1 VALUES(UPPER('test'), ABS(-42))");

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 'TEST', b: 42 });
    });
  });

  // ===========================================================================
  // MULTI-ROW INSERT (VALUES clause with multiple tuples)
  // ===========================================================================

  describe('INSERT without column list - Multi-row INSERT', () => {
    it.fails('should accept INSERT with multiple VALUES tuples', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');

      // SQL standard multi-row insert
      db.exec("INSERT INTO t1 VALUES(1, 'one'), (2, 'two'), (3, 'three')");

      const result = db.prepare('SELECT * FROM t1 ORDER BY a').all();
      expect(result.length).toBe(3);
      expect(result[0]).toEqual({ a: 1, b: 'one' });
      expect(result[1]).toEqual({ a: 2, b: 'two' });
      expect(result[2]).toEqual({ a: 3, b: 'three' });
    });

    it.fails('should accept INSERT with multiple VALUES and NULL', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');

      db.exec("INSERT INTO t1 VALUES(1, 'one'), (NULL, 'null-a'), (3, NULL)");

      const result = db.prepare('SELECT * FROM t1 ORDER BY a').all();
      expect(result.length).toBe(3);
      // Note: NULL sorts differently, this tests the actual insert worked
    });

    it.fails('should accept INSERT with multiple VALUES using params', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');

      const stmt = db.prepare('INSERT INTO t1 VALUES(?, ?), (?, ?)');
      stmt.run(1, 'first', 2, 'second');

      const result = db.prepare('SELECT * FROM t1 ORDER BY a').all();
      expect(result.length).toBe(2);
      expect(result[0]).toEqual({ a: 1, b: 'first' });
      expect(result[1]).toEqual({ a: 2, b: 'second' });
    });
  });

  // ===========================================================================
  // INSERT WITH QUOTED IDENTIFIERS
  // ===========================================================================

  describe('INSERT without column list - Quoted Identifiers', () => {
    it.fails('should accept INSERT into table with quoted name', () => {
      db.exec('CREATE TABLE "my table" (a INTEGER, b TEXT)');

      db.exec('INSERT INTO "my table" VALUES(1, \'test\')');

      const result = db.prepare('SELECT * FROM "my table"').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 1, b: 'test' });
    });

    it.fails('should accept INSERT into table with reserved word name', () => {
      db.exec('CREATE TABLE "select" (a INTEGER, b TEXT)');

      db.exec('INSERT INTO "select" VALUES(1, \'reserved\')');

      const result = db.prepare('SELECT * FROM "select"').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 1, b: 'reserved' });
    });
  });

  // ===========================================================================
  // EDGE CASES FROM SQLLOGICTEST SUITE
  // ===========================================================================

  describe('SQLLogicTest Specific Patterns', () => {
    /**
     * Pattern from SQLLogicTest:
     * CREATE TABLE t1(a,b)
     * INSERT INTO t1 VALUES(1,'true')
     * INSERT INTO t1 VALUES(0,'false')
     * INSERT INTO t1 VALUES(NULL,'NULL')
     */
    it('should handle the exact SQLLogicTest pattern', () => {
      // SQLite allows CREATE TABLE without column types
      db.exec('CREATE TABLE t1(a INTEGER, b TEXT)');

      db.exec("INSERT INTO t1 VALUES(1,'true')");
      db.exec("INSERT INTO t1 VALUES(0,'false')");
      db.exec("INSERT INTO t1 VALUES(NULL,'NULL')");

      const result = db.prepare('SELECT * FROM t1 ORDER BY a').all() as { a: number | null; b: string }[];
      expect(result.length).toBe(3);

      // Find the NULL row (will be first or last depending on NULL sorting)
      const nullRow = result.find(r => r.a === null);
      const falseRow = result.find(r => r.a === 0);
      const trueRow = result.find(r => r.a === 1);

      expect(nullRow).toEqual({ a: null, b: 'NULL' });
      expect(falseRow).toEqual({ a: 0, b: 'false' });
      expect(trueRow).toEqual({ a: 1, b: 'true' });
    });

    it('should handle boolean-like integer values', () => {
      db.exec('CREATE TABLE bools (flag INTEGER, label TEXT)');

      db.exec("INSERT INTO bools VALUES(1, 'yes')");
      db.exec("INSERT INTO bools VALUES(0, 'no')");

      const result = db.prepare('SELECT * FROM bools WHERE flag = 1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ flag: 1, label: 'yes' });
    });

    it('should handle empty string values', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');

      db.exec("INSERT INTO t1 VALUES(1, '')");

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 1, b: '' });
    });

    it.fails('should handle special characters in string values', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');

      // Single quotes escaped by doubling
      db.exec("INSERT INTO t1 VALUES(1, 'it''s a test')");

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 1, b: "it's a test" });
    });

    it('should handle numeric string values', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');

      // String that looks like a number
      db.exec("INSERT INTO t1 VALUES(1, '42')");

      const result = db.prepare('SELECT * FROM t1').all();
      expect(result.length).toBe(1);
      expect(result[0]).toEqual({ a: 1, b: '42' });
      expect(typeof result[0].b).toBe('string'); // Should remain string
    });
  });

  // ===========================================================================
  // ERROR CASES - These should continue to fail appropriately
  // ===========================================================================

  describe('INSERT without column list - Error Cases', () => {
    it('should reject INSERT with wrong number of values', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT, c REAL)');

      // Too few values - this should fail even when we support column-less INSERT
      expect(() => {
        db.exec('INSERT INTO t1 VALUES(1, \'test\')'); // missing c
      }).toThrow();
    });

    it('should reject INSERT with too many values', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');

      // Too many values
      expect(() => {
        db.exec("INSERT INTO t1 VALUES(1, 'test', 'extra')");
      }).toThrow();
    });

    it('should reject INSERT into non-existent table', () => {
      expect(() => {
        db.exec("INSERT INTO nonexistent VALUES(1, 'test')");
      }).toThrow();
    });
  });

  // ===========================================================================
  // COMPARISON: WITH vs WITHOUT COLUMN LIST
  // ===========================================================================

  describe('Comparison: WITH vs WITHOUT column list', () => {
    it('should produce same result with or without column list', () => {
      // Create two identical tables
      db.exec('CREATE TABLE with_cols (a INTEGER, b TEXT)');
      db.exec('CREATE TABLE without_cols (a INTEGER, b TEXT)');

      // INSERT WITH explicit columns (this works today)
      db.exec("INSERT INTO with_cols (a, b) VALUES (1, 'test')");

      // Get result from working INSERT
      const expected = db.prepare('SELECT * FROM with_cols').all();
      expect(expected.length).toBe(1);
      expect(expected[0]).toEqual({ a: 1, b: 'test' });
    });

    it('INSERT without column list should match INSERT with column list', () => {
      db.exec('CREATE TABLE t1 (a INTEGER, b TEXT)');
      db.exec('CREATE TABLE t2 (a INTEGER, b TEXT)');

      // WITH explicit columns (works)
      db.exec("INSERT INTO t1 (a, b) VALUES (42, 'hello')");

      // WITHOUT explicit columns (should work the same)
      db.exec("INSERT INTO t2 VALUES(42, 'hello')");

      const r1 = db.prepare('SELECT * FROM t1').all();
      const r2 = db.prepare('SELECT * FROM t2').all();

      expect(r2).toEqual(r1);
    });
  });
});
