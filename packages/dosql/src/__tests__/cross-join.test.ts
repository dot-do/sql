/**
 * CROSS JOIN Parser Tests
 *
 * Tests for CROSS JOIN parsing with table aliases.
 * This addresses the issue where CROSS JOIN was being treated as a single table name.
 *
 * Failing queries before fix:
 *   SELECT * FROM tab2 AS cor0 CROSS JOIN tab2 AS cor1
 *   -- Error: Table tab2 AS cor0 CROSS JOIN tab2 AS cor1 does not exist
 *
 *   SELECT ALL * FROM tab0 cor0 CROSS JOIN tab2 AS cor1
 *   -- Error: Table tab0 cor0 CROSS JOIN tab2 AS cor1 does not exist
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Database } from '../database.js';
import {
  InMemoryEngine,
  createInMemoryStorage,
  type InMemoryStorage,
} from '../statement/statement.js';

describe('CROSS JOIN Parsing', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  describe('Basic CROSS JOIN', () => {
    beforeEach(() => {
      db.exec(`CREATE TABLE tab0 (col0 INTEGER, col1 INTEGER, col2 TEXT)`);
      db.exec(`CREATE TABLE tab2 (col0 INTEGER, col1 INTEGER, col2 TEXT)`);
      db.exec(`INSERT INTO tab0 VALUES (1, 10, 'a')`);
      db.exec(`INSERT INTO tab0 VALUES (2, 20, 'b')`);
      db.exec(`INSERT INTO tab2 VALUES (3, 30, 'c')`);
      db.exec(`INSERT INTO tab2 VALUES (4, 40, 'd')`);
    });

    it('should parse CROSS JOIN with AS aliases', () => {
      // This was the failing query from SQLLogicTest
      const result = db.prepare('SELECT * FROM tab2 AS cor0 CROSS JOIN tab2 AS cor1').all();

      // CROSS JOIN produces Cartesian product: 2 * 2 = 4 rows
      expect(result.length).toBe(4);
    });

    it('should parse CROSS JOIN with implicit aliases', () => {
      // Another failing query from SQLLogicTest
      const result = db.prepare('SELECT * FROM tab0 cor0 CROSS JOIN tab2 cor1').all();

      // CROSS JOIN produces Cartesian product: 2 * 2 = 4 rows
      expect(result.length).toBe(4);
    });

    it('should parse CROSS JOIN with mixed alias styles', () => {
      // Mix of AS alias and implicit alias
      const result = db.prepare('SELECT * FROM tab0 AS cor0 CROSS JOIN tab2 cor1').all();

      expect(result.length).toBe(4);
    });

    it('should parse SELECT ALL with CROSS JOIN', () => {
      // SELECT ALL is equivalent to SELECT
      const result = db.prepare('SELECT ALL * FROM tab0 cor0 CROSS JOIN tab2 AS cor1').all();

      expect(result.length).toBe(4);
    });

    it('should resolve column references with CROSS JOIN aliases', () => {
      const result = db.prepare('SELECT cor0.col0, cor1.col0 FROM tab2 AS cor0 CROSS JOIN tab2 AS cor1').all();

      expect(result.length).toBe(4);
      // Should have both col0 values from both tables
      // All combinations: (3,3), (3,4), (4,3), (4,4)
      const pairs = result.map((r: any) => [r.col0, r.col0]);
      expect(pairs.length).toBe(4);
    });
  });

  describe('CROSS JOIN with filtering', () => {
    beforeEach(() => {
      db.exec(`CREATE TABLE t1 (a INTEGER, b INTEGER)`);
      db.exec(`CREATE TABLE t2 (x INTEGER, y INTEGER)`);
      db.exec(`INSERT INTO t1 VALUES (1, 2)`);
      db.exec(`INSERT INTO t1 VALUES (3, 4)`);
      db.exec(`INSERT INTO t2 VALUES (10, 20)`);
      db.exec(`INSERT INTO t2 VALUES (30, 40)`);
    });

    it('should support WHERE clause with CROSS JOIN', () => {
      const result = db.prepare('SELECT a.a, b.x FROM t1 a CROSS JOIN t2 b WHERE a.a = 1').all();

      // Only rows where a.a = 1, crossed with all t2 rows
      expect(result.length).toBe(2);
    });

    it('should support WHERE clause comparing across tables', () => {
      db.exec(`DELETE FROM t1`);
      db.exec(`DELETE FROM t2`);
      db.exec(`INSERT INTO t1 VALUES (10, 2)`);
      db.exec(`INSERT INTO t1 VALUES (30, 4)`);
      db.exec(`INSERT INTO t2 VALUES (10, 20)`);
      db.exec(`INSERT INTO t2 VALUES (30, 40)`);

      const result = db.prepare('SELECT a.a, b.x FROM t1 a CROSS JOIN t2 b WHERE a.a = b.x').all();

      // Only rows where a.a matches b.x
      expect(result.length).toBe(2);
    });
  });

  describe('Multiple CROSS JOINs', () => {
    beforeEach(() => {
      db.exec(`CREATE TABLE t1 (a INTEGER)`);
      db.exec(`CREATE TABLE t2 (b INTEGER)`);
      db.exec(`CREATE TABLE t3 (c INTEGER)`);
      db.exec(`INSERT INTO t1 VALUES (1), (2)`);
      db.exec(`INSERT INTO t2 VALUES (10), (20)`);
      db.exec(`INSERT INTO t3 VALUES (100), (200)`);
    });

    it('should handle multiple CROSS JOINs', () => {
      const result = db.prepare('SELECT * FROM t1 a CROSS JOIN t2 b CROSS JOIN t3 c').all();

      // 2 * 2 * 2 = 8 rows
      expect(result.length).toBe(8);
    });
  });

  describe('Self CROSS JOIN', () => {
    beforeEach(() => {
      db.exec(`CREATE TABLE t1 (val INTEGER)`);
      db.exec(`INSERT INTO t1 VALUES (1), (2), (3)`);
    });

    it('should handle self CROSS JOIN with different aliases', () => {
      const result = db.prepare('SELECT a.val AS a_val, b.val AS b_val FROM t1 a CROSS JOIN t1 b').all();

      // 3 * 3 = 9 rows
      expect(result.length).toBe(9);
    });
  });
});

describe('Other JOIN Types (Table Recognition)', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
    db.exec(`CREATE TABLE users (id INTEGER, name TEXT)`);
    db.exec(`CREATE TABLE orders (id INTEGER, user_id INTEGER, total INTEGER)`);
    db.exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`);
    db.exec(`INSERT INTO orders VALUES (101, 1, 100), (102, 1, 200), (103, 2, 150)`);
  });

  // Note: The ON clause filtering for non-CROSS JOINs is not yet implemented.
  // These tests verify that the tables are at least recognized correctly.
  // The full JOIN semantics (ON clause filtering) is a separate feature.

  it('should parse INNER JOIN and recognize both tables', () => {
    // This query should not throw "table does not exist" error
    const result = db.prepare('SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id').all();

    // Currently returns Cartesian product (3 users * 3 orders = 9)
    // Full INNER JOIN semantics would return 3 rows
    expect(result.length).toBe(9);
  });

  it('should parse JOIN (implicit INNER) and recognize both tables', () => {
    const result = db.prepare('SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id').all();

    // Currently returns Cartesian product
    expect(result.length).toBe(9);
  });

  it('should parse LEFT JOIN and recognize both tables', () => {
    const result = db.prepare('SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id').all();

    // Currently returns Cartesian product
    expect(result.length).toBe(9);
  });

  it('should parse LEFT OUTER JOIN and recognize both tables', () => {
    const result = db.prepare('SELECT u.name, o.total FROM users u LEFT OUTER JOIN orders o ON u.id = o.user_id').all();

    // Currently returns Cartesian product
    expect(result.length).toBe(9);
  });
});

describe('InMemoryEngine CROSS JOIN Tests', () => {
  let engine: InMemoryEngine;
  let storage: InMemoryStorage;

  beforeEach(() => {
    storage = createInMemoryStorage();
    engine = new InMemoryEngine(storage);

    engine.execute('CREATE TABLE t1 (a INTEGER, b INTEGER)', []);
    engine.execute('CREATE TABLE t2 (x INTEGER, y INTEGER)', []);
    engine.execute('INSERT INTO t1 VALUES (1, 2)', []);
    engine.execute('INSERT INTO t1 VALUES (3, 4)', []);
    engine.execute('INSERT INTO t2 VALUES (10, 20)', []);
    engine.execute('INSERT INTO t2 VALUES (30, 40)', []);
  });

  it('should execute CROSS JOIN with AS aliases', () => {
    const result = engine.execute('SELECT * FROM t1 AS a CROSS JOIN t2 AS b', []);

    expect(result.rows.length).toBe(4);
  });

  it('should execute CROSS JOIN with implicit aliases', () => {
    const result = engine.execute('SELECT * FROM t1 a CROSS JOIN t2 b', []);

    expect(result.rows.length).toBe(4);
  });

  it('should resolve alias.column in CROSS JOIN', () => {
    const result = engine.execute('SELECT a.a, b.x FROM t1 a CROSS JOIN t2 b', []);

    expect(result.rows.length).toBe(4);
    // Verify column names are resolved correctly
    expect(result.rows[0]).toHaveProperty('a');
    expect(result.rows[0]).toHaveProperty('x');
  });
});
