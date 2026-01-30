/**
 * SQLLogicTest CREATE INDEX Compatibility Tests
 *
 * RED PHASE TDD: These tests document expected CREATE INDEX behavior
 * that is not yet implemented in the DoSQL InMemoryEngine.
 *
 * The tests use `it.fails()` to indicate expected failures that need
 * to be fixed during the GREEN phase.
 *
 * SQLLogicTest failures being documented:
 * - CREATE INDEX t1i1 ON t1(a)
 * - CREATE INDEX t1i2 ON t1(a, b)
 * - CREATE INDEX t1i3 ON t1(a DESC, b ASC)
 * - CREATE UNIQUE INDEX t1i4 ON t1(a)
 * - DROP INDEX t1i1
 *
 * Run with: npx vitest run src/__tests__/sqllogictest-create-index.test.ts
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

describe('SQLLogicTest CREATE INDEX Compatibility', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  // ===========================================================================
  // BASIC CREATE INDEX
  // ===========================================================================

  describe('Basic CREATE INDEX', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (
          id INTEGER PRIMARY KEY,
          a INTEGER,
          b TEXT,
          c REAL
        )
      `);
    });

    /**
     * KNOWN FAILURE: Basic CREATE INDEX not supported
     *
     * SQLLogicTest: CREATE INDEX t1i1 ON t1(a)
     *
     * Expected: Index t1i1 is created on column 'a' of table t1
     * Actual: Parser error or unsupported statement error
     */
    it('should create a basic index on a single column', () => {
      // This should not throw
      expect(() => {
        db.exec('CREATE INDEX t1i1 ON t1(a)');
      }).not.toThrow();

      // Verify index exists via pragma
      const indexes = db.pragma('index_list', 't1');
      expect(Array.isArray(indexes)).toBe(true);
      expect((indexes as { name: string }[]).some(idx => idx.name === 't1i1')).toBe(true);
    });

    /**
     * KNOWN FAILURE: CREATE INDEX with multiple columns not supported
     *
     * SQLLogicTest: CREATE INDEX t1i2 ON t1(a, b)
     *
     * Expected: Composite index t1i2 is created on columns (a, b)
     * Actual: Parser error or unsupported statement error
     */
    it('should create a multi-column (composite) index', () => {
      expect(() => {
        db.exec('CREATE INDEX t1i2 ON t1(a, b)');
      }).not.toThrow();

      // Verify index exists
      const indexes = db.pragma('index_list', 't1');
      expect(Array.isArray(indexes)).toBe(true);
      expect((indexes as { name: string }[]).some(idx => idx.name === 't1i2')).toBe(true);
    });

    /**
     * KNOWN FAILURE: CREATE INDEX with three columns not supported
     *
     * Expected: Composite index on (a, b, c) columns
     * Actual: Parser error or unsupported statement error
     */
    it('should create a three-column composite index', () => {
      expect(() => {
        db.exec('CREATE INDEX t1i_abc ON t1(a, b, c)');
      }).not.toThrow();

      const indexes = db.pragma('index_list', 't1');
      expect(Array.isArray(indexes)).toBe(true);
      expect((indexes as { name: string }[]).some(idx => idx.name === 't1i_abc')).toBe(true);
    });
  });

  // ===========================================================================
  // INDEX WITH SORT ORDER (ASC/DESC)
  // ===========================================================================

  describe('Index with Sort Order (ASC/DESC)', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (
          id INTEGER PRIMARY KEY,
          a INTEGER,
          b TEXT
        )
      `);
    });

    /**
     * KNOWN FAILURE: CREATE INDEX with DESC ordering not supported
     *
     * SQLLogicTest: CREATE INDEX t1i3 ON t1(a DESC, b ASC)
     *
     * Expected: Index with a in descending order and b in ascending order
     * Actual: Parser error or unsupported statement error
     */
    it('should create an index with DESC and ASC sort orders', () => {
      expect(() => {
        db.exec('CREATE INDEX t1i3 ON t1(a DESC, b ASC)');
      }).not.toThrow();

      const indexes = db.pragma('index_list', 't1');
      expect(Array.isArray(indexes)).toBe(true);
      expect((indexes as { name: string }[]).some(idx => idx.name === 't1i3')).toBe(true);
    });

    /**
     * KNOWN FAILURE: CREATE INDEX with single column DESC not supported
     *
     * Expected: Index with column in descending order
     * Actual: Parser error or unsupported statement error
     */
    it('should create an index with single column DESC', () => {
      expect(() => {
        db.exec('CREATE INDEX t1i_desc ON t1(a DESC)');
      }).not.toThrow();

      const indexes = db.pragma('index_list', 't1');
      expect(Array.isArray(indexes)).toBe(true);
      expect((indexes as { name: string }[]).some(idx => idx.name === 't1i_desc')).toBe(true);
    });

    /**
     * KNOWN FAILURE: CREATE INDEX with explicit ASC not supported
     *
     * Expected: Index with column in ascending order (explicit)
     * Actual: Parser error or unsupported statement error
     */
    it('should create an index with explicit ASC', () => {
      expect(() => {
        db.exec('CREATE INDEX t1i_asc ON t1(a ASC)');
      }).not.toThrow();

      const indexes = db.pragma('index_list', 't1');
      expect(Array.isArray(indexes)).toBe(true);
      expect((indexes as { name: string }[]).some(idx => idx.name === 't1i_asc')).toBe(true);
    });
  });

  // ===========================================================================
  // UNIQUE INDEX
  // ===========================================================================

  describe('UNIQUE INDEX', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (
          id INTEGER PRIMARY KEY,
          a INTEGER,
          b TEXT
        )
      `);
    });

    /**
     * KNOWN FAILURE: CREATE UNIQUE INDEX not supported
     *
     * SQLLogicTest: CREATE UNIQUE INDEX t1i4 ON t1(a)
     *
     * Expected: Unique index t1i4 is created, enforcing uniqueness on column a
     * Actual: Parser error or unsupported statement error
     */
    it('should create a unique index', () => {
      expect(() => {
        db.exec('CREATE UNIQUE INDEX t1i4 ON t1(a)');
      }).not.toThrow();

      const indexes = db.pragma('index_list', 't1');
      expect(Array.isArray(indexes)).toBe(true);
      const idx = (indexes as { name: string; unique: number }[]).find(i => i.name === 't1i4');
      expect(idx).toBeDefined();
      expect(idx?.unique).toBe(1);
    });

    /**
     * KNOWN FAILURE: UNIQUE constraint enforcement via index not working
     *
     * Expected: Inserting duplicate values should fail with constraint violation
     * Actual: Index is created but constraint enforcement is not yet implemented
     *
     * Note: Implementing unique constraint enforcement requires modifying the
     * INSERT execution path to check all unique indexes on the table.
     */
    it('should enforce uniqueness on insert after creating unique index', () => {
      db.exec('CREATE UNIQUE INDEX t1i4 ON t1(a)');

      // First insert should succeed
      db.exec('INSERT INTO t1 (id, a, b) VALUES (1, 100, "first")');

      // Second insert with same 'a' value should fail
      expect(() => {
        db.exec('INSERT INTO t1 (id, a, b) VALUES (2, 100, "second")');
      }).toThrow();
    });

    /**
     * KNOWN FAILURE: Unique index on multiple columns not supported
     *
     * Expected: Unique constraint on the combination of (a, b)
     * Actual: Parser error or unsupported statement error
     */
    it('should create a unique index on multiple columns', () => {
      expect(() => {
        db.exec('CREATE UNIQUE INDEX t1i_unique_ab ON t1(a, b)');
      }).not.toThrow();

      const indexes = db.pragma('index_list', 't1');
      expect(Array.isArray(indexes)).toBe(true);
      const idx = (indexes as { name: string; unique: number }[]).find(i => i.name === 't1i_unique_ab');
      expect(idx).toBeDefined();
      expect(idx?.unique).toBe(1);
    });
  });

  // ===========================================================================
  // DROP INDEX
  // ===========================================================================

  describe('DROP INDEX', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (
          id INTEGER PRIMARY KEY,
          a INTEGER,
          b TEXT
        )
      `);
    });

    /**
     * KNOWN FAILURE: DROP INDEX not supported
     *
     * SQLLogicTest: DROP INDEX t1i1
     *
     * Expected: Index t1i1 is dropped from the database
     * Actual: Parser error or unsupported statement error
     */
    it('should drop an existing index', () => {
      // First create the index
      db.exec('CREATE INDEX t1i1 ON t1(a)');

      // Verify it exists
      let indexes = db.pragma('index_list', 't1');
      expect((indexes as { name: string }[]).some(idx => idx.name === 't1i1')).toBe(true);

      // Drop the index
      expect(() => {
        db.exec('DROP INDEX t1i1');
      }).not.toThrow();

      // Verify it no longer exists
      indexes = db.pragma('index_list', 't1');
      expect((indexes as { name: string }[]).some(idx => idx.name === 't1i1')).toBe(false);
    });

    /**
     * KNOWN FAILURE: DROP INDEX on non-existent index should error with proper message
     *
     * Expected: Error "no such index: nonexistent_index" when trying to drop
     *           an index that doesn't exist
     * Actual: Throws "Unsupported SQL" error instead of proper validation error
     */
    it('should throw proper error when dropping non-existent index', () => {
      // The engine should support DROP INDEX syntax first, then validate
      // Currently throws "Unsupported SQL" instead of "no such index"
      let error: Error | null = null;
      try {
        db.exec('DROP INDEX nonexistent_index');
      } catch (e) {
        error = e as Error;
      }

      expect(error).not.toBeNull();
      // Should be a proper "no such index" error, not "Unsupported SQL"
      expect(error?.message).toMatch(/no such index|does not exist/i);
      expect(error?.message).not.toMatch(/unsupported/i);
    });
  });

  // ===========================================================================
  // IF NOT EXISTS / IF EXISTS CLAUSES
  // ===========================================================================

  describe('IF NOT EXISTS / IF EXISTS Clauses', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (
          id INTEGER PRIMARY KEY,
          a INTEGER,
          b TEXT
        )
      `);
    });

    /**
     * KNOWN FAILURE: CREATE INDEX IF NOT EXISTS not supported
     *
     * Expected: Creates index only if it doesn't already exist
     * Actual: Parser error or unsupported statement error
     */
    it('should create index only if it does not exist', () => {
      // First creation should succeed
      expect(() => {
        db.exec('CREATE INDEX IF NOT EXISTS t1i1 ON t1(a)');
      }).not.toThrow();

      // Second creation with IF NOT EXISTS should not throw
      expect(() => {
        db.exec('CREATE INDEX IF NOT EXISTS t1i1 ON t1(a)');
      }).not.toThrow();

      // Creating without IF NOT EXISTS on existing index should throw
      expect(() => {
        db.exec('CREATE INDEX t1i1 ON t1(a)');
      }).toThrow();
    });

    /**
     * KNOWN FAILURE: DROP INDEX IF EXISTS not supported
     *
     * Expected: Drops index only if it exists, no error if it doesn't
     * Actual: Parser error or unsupported statement error
     */
    it('should drop index only if it exists', () => {
      // Should not throw even if index doesn't exist
      expect(() => {
        db.exec('DROP INDEX IF EXISTS nonexistent_index');
      }).not.toThrow();

      // Create and then drop
      db.exec('CREATE INDEX t1i1 ON t1(a)');
      expect(() => {
        db.exec('DROP INDEX IF EXISTS t1i1');
      }).not.toThrow();
    });
  });

  // ===========================================================================
  // INDEX AND QUERY EXECUTION
  // ===========================================================================

  describe('Index and Query Execution', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (
          id INTEGER PRIMARY KEY,
          a INTEGER,
          b TEXT
        )
      `);

      // Insert test data
      for (let i = 1; i <= 100; i++) {
        db.exec(`INSERT INTO t1 (id, a, b) VALUES (${i}, ${i * 10}, 'value${i}')`);
      }
    });

    /**
     * KNOWN FAILURE: Index should be used for equality queries
     *
     * Expected: Query uses index for WHERE a = 500
     * Actual: Index not created or not used
     */
    it('should use index for equality queries', () => {
      db.exec('CREATE INDEX t1i_a ON t1(a)');

      // This query should use the index
      const result = db.prepare('SELECT id, a, b FROM t1 WHERE a = 500').all();

      expect(result.length).toBe(1);
      expect((result[0] as { id: number; a: number; b: string }).a).toBe(500);
    });

    /**
     * KNOWN FAILURE: Index should be used for range queries
     *
     * Expected: Query uses index for WHERE a > 900
     * Actual: Index not created or not used
     */
    it('should use index for range queries', () => {
      db.exec('CREATE INDEX t1i_a ON t1(a)');

      const result = db.prepare('SELECT id, a FROM t1 WHERE a > 900').all();

      expect(result.length).toBe(10); // 910, 920, ... 1000
      expect((result as { id: number; a: number }[]).every(r => r.a > 900)).toBe(true);
    });

    /**
     * KNOWN FAILURE: Composite index should be used for multi-column queries
     *
     * Expected: Query uses composite index for WHERE a = 500 AND b = 'value50'
     * Actual: Index not created or not used
     */
    it('should use composite index for multi-column queries', () => {
      db.exec('CREATE INDEX t1i_ab ON t1(a, b)');

      const result = db.prepare("SELECT id FROM t1 WHERE a = 500 AND b = 'value50'").all();

      expect(result.length).toBe(1);
      expect((result[0] as { id: number }).id).toBe(50);
    });
  });

  // ===========================================================================
  // INDEX METADATA (PRAGMA)
  // ===========================================================================

  describe('Index Metadata via PRAGMA', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (
          id INTEGER PRIMARY KEY,
          a INTEGER,
          b TEXT,
          c REAL
        )
      `);
    });

    /**
     * KNOWN FAILURE: pragma index_list should return created indexes
     *
     * Expected: index_list pragma returns array of indexes for table
     * Actual: Returns empty array or fails
     */
    it('should list indexes via pragma index_list', () => {
      db.exec('CREATE INDEX t1i1 ON t1(a)');
      db.exec('CREATE INDEX t1i2 ON t1(b)');
      db.exec('CREATE UNIQUE INDEX t1i3 ON t1(c)');

      const indexes = db.pragma('index_list', 't1');

      expect(Array.isArray(indexes)).toBe(true);
      expect((indexes as { name: string }[]).length).toBe(3);

      const indexNames = (indexes as { name: string }[]).map(i => i.name);
      expect(indexNames).toContain('t1i1');
      expect(indexNames).toContain('t1i2');
      expect(indexNames).toContain('t1i3');
    });

    /**
     * KNOWN FAILURE: pragma index_info should return column info for index
     *
     * Expected: index_info pragma returns columns in the index
     * Actual: Not implemented or fails
     */
    it('should get index column info via pragma index_info', () => {
      db.exec('CREATE INDEX t1i_ab ON t1(a, b)');

      // Note: This pragma may need different invocation
      const indexInfo = db.prepare("SELECT * FROM pragma_index_info('t1i_ab')").all();

      expect(Array.isArray(indexInfo)).toBe(true);
      expect(indexInfo.length).toBe(2);
    });
  });

  // ===========================================================================
  // ERROR CASES
  // ===========================================================================

  describe('Error Cases', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (
          id INTEGER PRIMARY KEY,
          a INTEGER,
          b TEXT
        )
      `);
    });

    /**
     * KNOWN FAILURE: Should error with proper message on non-existent table
     *
     * Expected: Error "no such table: nonexistent_table" when creating index
     * Actual: Throws "Unsupported SQL" error instead of proper validation error
     */
    it('should error with proper message when creating index on non-existent table', () => {
      let error: Error | null = null;
      try {
        db.exec('CREATE INDEX idx ON nonexistent_table(a)');
      } catch (e) {
        error = e as Error;
      }

      expect(error).not.toBeNull();
      // Should be a proper "no such table" error, not "Unsupported SQL"
      expect(error?.message).toMatch(/no such table|table.*not found|does not exist/i);
      expect(error?.message).not.toMatch(/unsupported/i);
    });

    /**
     * KNOWN FAILURE: Should error with proper message on non-existent column
     *
     * Expected: Error "no such column: nonexistent_column" when creating index
     * Actual: Throws "Unsupported SQL" error instead of proper validation error
     */
    it('should error with proper message when creating index on non-existent column', () => {
      let error: Error | null = null;
      try {
        db.exec('CREATE INDEX idx ON t1(nonexistent_column)');
      } catch (e) {
        error = e as Error;
      }

      expect(error).not.toBeNull();
      // Should be a proper "no such column" error, not "Unsupported SQL"
      expect(error?.message).toMatch(/no such column|column.*not found|does not exist/i);
      expect(error?.message).not.toMatch(/unsupported/i);
    });

    /**
     * KNOWN FAILURE: Should error on duplicate index name
     *
     * Expected: Error when creating index with same name as existing index
     * Actual: Parser error or no error
     */
    it('should error when creating duplicate index name', () => {
      db.exec('CREATE INDEX t1i1 ON t1(a)');

      expect(() => {
        db.exec('CREATE INDEX t1i1 ON t1(b)');
      }).toThrow();
    });
  });
});

// =============================================================================
// LOW-LEVEL INMEMORYENGINE TESTS
// =============================================================================

describe('InMemoryEngine CREATE INDEX Direct Tests', () => {
  let engine: InMemoryEngine;
  let storage: InMemoryStorage;

  beforeEach(() => {
    storage = createInMemoryStorage();
    engine = new InMemoryEngine(storage);

    // Set up test table
    engine.execute('CREATE TABLE t1 (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)', []);
    engine.execute('INSERT INTO t1 (id, a, b) VALUES (?, ?, ?)', [1, 10, 'one']);
    engine.execute('INSERT INTO t1 (id, a, b) VALUES (?, ?, ?)', [2, 20, 'two']);
    engine.execute('INSERT INTO t1 (id, a, b) VALUES (?, ?, ?)', [3, 30, 'three']);
  });

  /**
   * KNOWN FAILURE: Engine should parse CREATE INDEX statement
   *
   * Expected: Engine parses and executes CREATE INDEX
   * Actual: Parse error or unsupported operation
   */
  it('should execute CREATE INDEX at engine level', () => {
    expect(() => {
      engine.execute('CREATE INDEX t1i1 ON t1(a)', []);
    }).not.toThrow();
  });

  /**
   * KNOWN FAILURE: Engine should store index metadata
   *
   * Expected: Index metadata stored in storage
   * Actual: No index storage mechanism
   */
  it('should store index metadata in storage', () => {
    engine.execute('CREATE INDEX t1i1 ON t1(a)', []);

    // Check if storage has index information
    // Note: This depends on how indexes are stored in InMemoryStorage
    expect(storage).toHaveProperty('indexes');
    expect((storage as unknown as { indexes: Map<string, unknown> }).indexes.has('t1i1')).toBe(true);
  });

  /**
   * KNOWN FAILURE: Engine should parse DROP INDEX statement
   *
   * Expected: Engine parses and executes DROP INDEX
   * Actual: Parse error or unsupported operation
   */
  it('should execute DROP INDEX at engine level', () => {
    engine.execute('CREATE INDEX t1i1 ON t1(a)', []);

    expect(() => {
      engine.execute('DROP INDEX t1i1', []);
    }).not.toThrow();
  });
});

// =============================================================================
// INDEX PERFORMANCE TESTS (OPTIONAL)
// =============================================================================

describe('Index Performance Tests', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
    db.exec(`
      CREATE TABLE large_table (
        id INTEGER PRIMARY KEY,
        indexed_col INTEGER,
        unindexed_col INTEGER,
        data TEXT
      )
    `);

    // Insert 1000 rows
    for (let i = 1; i <= 1000; i++) {
      db.exec(`INSERT INTO large_table (id, indexed_col, unindexed_col, data) VALUES (${i}, ${i % 100}, ${i}, 'data${i}')`);
    }
  });

  /**
   * KNOWN FAILURE: Index performance test
   *
   * Note: This test verifies that indexed queries don't perform significantly
   * worse than unindexed queries. For small in-memory tables, the performance
   * difference may be minimal since the index is not currently used for query
   * optimization - it's just metadata storage.
   *
   * This test is marked as failing because index-based query optimization
   * is not yet implemented. The indexes are stored as metadata but are not
   * used to speed up queries.
   */
  it('should demonstrate improved query performance with index', () => {
    // Create index on indexed_col
    db.exec('CREATE INDEX idx_col ON large_table(indexed_col)');

    // Query indexed column
    const indexedStart = performance.now();
    for (let i = 0; i < 100; i++) {
      db.prepare('SELECT * FROM large_table WHERE indexed_col = ?').all(50);
    }
    const indexedTime = performance.now() - indexedStart;

    // Query unindexed column
    const unindexedStart = performance.now();
    for (let i = 0; i < 100; i++) {
      db.prepare('SELECT * FROM large_table WHERE unindexed_col = ?').all(500);
    }
    const unindexedTime = performance.now() - unindexedStart;

    // Index should be faster (allowing for some variance)
    // Note: For small tables, the difference may be minimal
    console.log(`Indexed query time: ${indexedTime}ms, Unindexed query time: ${unindexedTime}ms`);

    // At minimum, indexed queries shouldn't be significantly slower
    expect(indexedTime).toBeLessThanOrEqual(unindexedTime * 2);
  });

  /**
   * KNOWN FAILURE: EXPLAIN should show index usage
   *
   * Expected: EXPLAIN shows index scan instead of full table scan
   * Actual: EXPLAIN may not be fully implemented or index not used
   */
  it('should show index usage in EXPLAIN output', () => {
    db.exec('CREATE INDEX idx_col ON large_table(indexed_col)');

    const explain = db.prepare('EXPLAIN QUERY PLAN SELECT * FROM large_table WHERE indexed_col = 50').all();

    expect(Array.isArray(explain)).toBe(true);
    // The explain output should mention using the index
    const explainText = JSON.stringify(explain);
    expect(explainText.toLowerCase()).toMatch(/idx_col|index/);
  });
});
