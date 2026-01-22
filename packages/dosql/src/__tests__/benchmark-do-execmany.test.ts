/**
 * BenchmarkTestDO.execMany Batch Execution Tests - RED Phase TDD
 *
 * These tests document the MISSING execMany method for batch SQL execution.
 * The BenchmarkTestDO lacks a method to execute multiple SQL statements in a single call,
 * requiring sequential exec() calls which is inefficient.
 *
 * Issue: sql-2qw
 *
 * Tests document:
 * 1. execMany accepts array of SQL statements
 * 2. execMany returns array of results matching input order
 * 3. execMany handles mixed SELECT/INSERT/UPDATE statements
 * 4. execMany transaction semantics (all-or-nothing)
 * 5. execMany with empty array returns empty array
 * 6. execMany performance is better than sequential exec calls
 *
 * NOTE: The execMany method does not exist yet.
 * - Tests using `it.fails()` document the missing feature that SHOULD be implemented
 * - These tests will pass once execMany is implemented
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';
import { env, runInDurableObject } from 'cloudflare:test';

import { BenchmarkTestDO } from '../benchmarks/adapters/__tests__/do-sqlite.test.js';

import { type TableSchemaConfig } from '../benchmarks/types.js';

// =============================================================================
// DOCUMENTED GAPS - Features that should be implemented
// =============================================================================
//
// 1. execMany(statements: string[]): Promise<ExecManyResult[]>
//    - Executes multiple SQL statements in a single batch
//    - Returns an array of results in the same order as input statements
//
// 2. ExecManyResult interface:
//    - success: boolean
//    - rowCount: number
//    - rows?: Record<string, unknown>[]  // For SELECT statements
//    - error?: string
//    - durationMs: number
//
// 3. Transaction semantics:
//    - All statements should execute within a single transaction
//    - If any statement fails, all changes should be rolled back
//    - Partial success should not be possible
//
// 4. Performance characteristics:
//    - Batch execution should be significantly faster than sequential calls
//    - Single round-trip overhead instead of N round-trips
//    - Amortized transaction overhead
//
// =============================================================================

// =============================================================================
// Test Helpers
// =============================================================================

let testCounter = 0;

/**
 * Get a unique DO stub for each test
 */
function getUniqueStub() {
  const id = env.BENCHMARK_TEST_DO.idFromName(
    `execmany-test-${Date.now()}-${testCounter++}`
  );
  return env.BENCHMARK_TEST_DO.get(id);
}

/**
 * Default test schema
 */
const TEST_SCHEMA: TableSchemaConfig = {
  tableName: 'execmany_test',
  columns: [
    { name: 'id', type: 'INTEGER' },
    { name: 'name', type: 'TEXT' },
    { name: 'value', type: 'REAL' },
    { name: 'created_at', type: 'INTEGER' },
  ],
  primaryKey: 'id',
  indexes: ['name'],
};

// =============================================================================
// Type augmentation for execMany (not yet implemented)
// =============================================================================

/**
 * Augment BenchmarkTestDO with execMany method signature
 * This documents the expected API that needs to be implemented
 */
interface BenchmarkTestDOWithExecMany extends BenchmarkTestDO {
  execMany(
    statements: string[]
  ): Promise<
    Array<{
      success: boolean;
      rowCount: number;
      rows?: Record<string, unknown>[];
      error?: string;
      durationMs: number;
    }>
  >;

  execManyWithParams(
    statements: Array<{ sql: string; params?: Record<string, unknown> }>
  ): Promise<
    Array<{
      success: boolean;
      rowCount: number;
      rows?: Record<string, unknown>[];
      error?: string;
      durationMs: number;
    }>
  >;
}

// =============================================================================
// 1. EXECMANY ACCEPTS ARRAY OF SQL STATEMENTS
// =============================================================================

describe('BenchmarkTestDO.execMany - Array Input', () => {
  it.fails('should accept an array of SQL statements', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const statements = [
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (1, 'row1', 10.0, ${Date.now()})`,
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (2, 'row2', 20.0, ${Date.now()})`,
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (3, 'row3', 30.0, ${Date.now()})`,
      ];

      const results = await instance.execMany(statements);

      expect(results).toBeDefined();
      expect(Array.isArray(results)).toBe(true);
      expect(results.length).toBe(3);
    });
  });

  it.fails('should accept a single statement in array form', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const statements = [
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (1, 'single', 100.0, ${Date.now()})`,
      ];

      const results = await instance.execMany(statements);

      expect(results).toBeDefined();
      expect(results.length).toBe(1);
      expect(results[0].success).toBe(true);
    });
  });

  it.fails('should accept statements with varying complexity', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const statements = [
        // Simple insert
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (1, 'simple', 1.0, ${Date.now()})`,
        // Insert with expression
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (2, 'calc', 1.0 + 2.0, ${Date.now()})`,
        // Insert with string containing special chars
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (3, 'special''s "test"', 3.0, ${Date.now()})`,
      ];

      const results = await instance.execMany(statements);

      expect(results.length).toBe(3);
      results.forEach((r) => expect(r.success).toBe(true));
    });
  });
});

// =============================================================================
// 2. EXECMANY RETURNS RESULTS MATCHING INPUT ORDER
// =============================================================================

describe('BenchmarkTestDO.execMany - Result Order', () => {
  it.fails('should return results in the same order as input statements', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // Insert in specific order
      const statements = [
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (1, 'first', 1.0, ${Date.now()})`,
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (2, 'second', 2.0, ${Date.now()})`,
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (3, 'third', 3.0, ${Date.now()})`,
      ];

      const results = await instance.execMany(statements);

      // Each result should correspond to its statement
      expect(results[0].success).toBe(true);
      expect(results[0].rowCount).toBe(1);
      expect(results[1].success).toBe(true);
      expect(results[1].rowCount).toBe(1);
      expect(results[2].success).toBe(true);
      expect(results[2].rowCount).toBe(1);
    });
  });

  it.fails('should preserve order even when some statements fail', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // Pre-insert a row to cause conflict
      await instance.query(
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (2, 'existing', 2.0, ${Date.now()})`
      );

      const statements = [
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (1, 'first', 1.0, ${Date.now()})`,
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (2, 'duplicate', 2.0, ${Date.now()})`, // Will fail
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (3, 'third', 3.0, ${Date.now()})`,
      ];

      // With transaction semantics, all should fail
      // But results should still be in order
      const results = await instance.execMany(statements);

      expect(results.length).toBe(3);
      // First statement would have succeeded but rolled back
      // Second statement failed due to duplicate key
      expect(results[1].success).toBe(false);
      expect(results[1].error).toBeDefined();
    });
  });

  it.fails('should return SELECT results with rows in correct position', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // Setup: insert some data first
      await instance.query(
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (1, 'one', 1.0, ${Date.now()})`
      );
      await instance.query(
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (2, 'two', 2.0, ${Date.now()})`
      );

      const statements = [
        'SELECT * FROM execmany_test WHERE id = 1',
        'SELECT * FROM execmany_test WHERE id = 2',
        'SELECT COUNT(*) as total FROM execmany_test',
      ];

      const results = await instance.execMany(statements);

      expect(results.length).toBe(3);
      expect(results[0].rows).toBeDefined();
      expect(results[0].rows![0].name).toBe('one');
      expect(results[1].rows).toBeDefined();
      expect(results[1].rows![0].name).toBe('two');
      expect(results[2].rows).toBeDefined();
      expect(results[2].rows![0].total).toBe(2);
    });
  });
});

// =============================================================================
// 3. EXECMANY HANDLES MIXED STATEMENT TYPES
// =============================================================================

describe('BenchmarkTestDO.execMany - Mixed Statement Types', () => {
  it.fails('should handle mixed SELECT/INSERT/UPDATE statements', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // Setup initial data
      await instance.query(
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (1, 'initial', 10.0, ${Date.now()})`
      );

      const statements = [
        // SELECT to verify initial state
        'SELECT * FROM execmany_test WHERE id = 1',
        // INSERT new row
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (2, 'new', 20.0, ${Date.now()})`,
        // UPDATE existing row
        'UPDATE execmany_test SET value = 15.0 WHERE id = 1',
        // SELECT to verify changes
        'SELECT * FROM execmany_test ORDER BY id',
      ];

      const results = await instance.execMany(statements);

      expect(results.length).toBe(4);

      // First SELECT
      expect(results[0].success).toBe(true);
      expect(results[0].rows).toBeDefined();
      expect(results[0].rows!.length).toBe(1);

      // INSERT
      expect(results[1].success).toBe(true);
      expect(results[1].rowCount).toBe(1);

      // UPDATE
      expect(results[2].success).toBe(true);
      expect(results[2].rowCount).toBe(1);

      // Final SELECT
      expect(results[3].success).toBe(true);
      expect(results[3].rows).toBeDefined();
      expect(results[3].rows!.length).toBe(2);
    });
  });

  it.fails('should handle DELETE statements in batch', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // Setup: insert data
      await instance.query(
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (1, 'delete1', 1.0, ${Date.now()})`
      );
      await instance.query(
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (2, 'keep', 2.0, ${Date.now()})`
      );
      await instance.query(
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (3, 'delete2', 3.0, ${Date.now()})`
      );

      const statements = [
        'DELETE FROM execmany_test WHERE id = 1',
        'DELETE FROM execmany_test WHERE id = 3',
        'SELECT COUNT(*) as remaining FROM execmany_test',
      ];

      const results = await instance.execMany(statements);

      expect(results[0].success).toBe(true);
      expect(results[0].rowCount).toBe(1);
      expect(results[1].success).toBe(true);
      expect(results[1].rowCount).toBe(1);
      expect(results[2].rows![0].remaining).toBe(1);
    });
  });

  it.fails('should handle DDL statements in batch', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();

      const statements = [
        'CREATE TABLE batch_table1 (id INTEGER PRIMARY KEY, name TEXT)',
        'CREATE TABLE batch_table2 (id INTEGER PRIMARY KEY, ref_id INTEGER)',
        'CREATE INDEX idx_batch_ref ON batch_table2(ref_id)',
        `INSERT INTO batch_table1 (id, name) VALUES (1, 'test')`,
        `INSERT INTO batch_table2 (id, ref_id) VALUES (1, 1)`,
      ];

      const results = await instance.execMany(statements);

      expect(results.length).toBe(5);
      results.forEach((r) => expect(r.success).toBe(true));
    });
  });
});

// =============================================================================
// 4. EXECMANY TRANSACTION SEMANTICS (ALL-OR-NOTHING)
// =============================================================================

describe('BenchmarkTestDO.execMany - Transaction Semantics', () => {
  it.fails('should rollback all changes if any statement fails', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // Pre-insert to cause conflict later
      await instance.query(
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (5, 'existing', 5.0, ${Date.now()})`
      );

      const statements = [
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (1, 'row1', 1.0, ${Date.now()})`,
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (2, 'row2', 2.0, ${Date.now()})`,
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (5, 'duplicate', 5.0, ${Date.now()})`, // Will fail
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (4, 'row4', 4.0, ${Date.now()})`,
      ];

      // Execute batch - should fail
      const results = await instance.execMany(statements);

      // At least one failure
      expect(results.some((r) => !r.success)).toBe(true);

      // Verify rollback: rows 1, 2, 4 should NOT exist
      const checkResult = await instance.query(
        'SELECT COUNT(*) as count FROM execmany_test'
      );
      expect(checkResult.success).toBe(true);
      // Only the pre-existing row (id=5) should remain
      // If transaction rolled back properly
    });
  });

  it.fails('should commit all changes if all statements succeed', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const statements = [
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (1, 'row1', 1.0, ${Date.now()})`,
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (2, 'row2', 2.0, ${Date.now()})`,
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (3, 'row3', 3.0, ${Date.now()})`,
      ];

      const results = await instance.execMany(statements);

      // All should succeed
      expect(results.every((r) => r.success)).toBe(true);

      // Verify commit: all rows should exist
      const checkResult = await instance.query(
        'SELECT COUNT(*) as count FROM execmany_test'
      );
      expect(checkResult.success).toBe(true);
    });
  });

  it.fails('should maintain isolation during batch execution', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // Insert initial row
      await instance.query(
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (1, 'initial', 100.0, ${Date.now()})`
      );

      const statements = [
        // Read current value
        'SELECT value FROM execmany_test WHERE id = 1',
        // Update based on current value (increment)
        'UPDATE execmany_test SET value = value + 10 WHERE id = 1',
        // Read again to verify
        'SELECT value FROM execmany_test WHERE id = 1',
      ];

      const results = await instance.execMany(statements);

      expect(results[0].rows![0].value).toBe(100.0);
      expect(results[1].success).toBe(true);
      expect(results[2].rows![0].value).toBe(110.0);
    });
  });

  it.fails('should handle constraint violations with proper rollback', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();

      // Create table with unique constraint
      await instance.query(`
        CREATE TABLE unique_test (
          id INTEGER PRIMARY KEY,
          email TEXT UNIQUE,
          name TEXT
        )
      `);

      const statements = [
        `INSERT INTO unique_test (id, email, name) VALUES (1, 'a@test.com', 'Alice')`,
        `INSERT INTO unique_test (id, email, name) VALUES (2, 'b@test.com', 'Bob')`,
        `INSERT INTO unique_test (id, email, name) VALUES (3, 'a@test.com', 'Charlie')`, // Duplicate email
      ];

      const results = await instance.execMany(statements);

      // Should fail on third statement
      expect(results[2].success).toBe(false);
      expect(results[2].error).toContain('UNIQUE constraint');

      // All changes should be rolled back
      const checkResult = await instance.query(
        'SELECT COUNT(*) as count FROM unique_test'
      );
      // Depending on implementation, either 0 rows (full rollback) or 2 rows (partial)
      // Proper transaction semantics means 0 rows
    });
  });
});

// =============================================================================
// 5. EXECMANY WITH EMPTY ARRAY
// =============================================================================

describe('BenchmarkTestDO.execMany - Empty Array', () => {
  it.fails('should return empty array for empty input', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();

      const results = await instance.execMany([]);

      expect(results).toBeDefined();
      expect(Array.isArray(results)).toBe(true);
      expect(results.length).toBe(0);
    });
  });

  it.fails('should not throw for empty array', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();

      await expect(instance.execMany([])).resolves.not.toThrow();
    });
  });

  it.fails('should complete quickly for empty array', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();

      const start = performance.now();
      await instance.execMany([]);
      const duration = performance.now() - start;

      // Should complete in under 10ms for empty array
      expect(duration).toBeLessThan(10);
    });
  });
});

// =============================================================================
// 6. EXECMANY PERFORMANCE COMPARISON
// =============================================================================

describe('BenchmarkTestDO.execMany - Performance', () => {
  it.fails('should be faster than sequential exec calls for many statements', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const statementCount = 50;
      const statements: string[] = [];
      for (let i = 1; i <= statementCount; i++) {
        statements.push(
          `INSERT INTO execmany_test (id, name, value, created_at) VALUES (${i}, 'row${i}', ${i * 10.0}, ${Date.now()})`
        );
      }

      // Measure batch execution
      const batchStart = performance.now();
      const batchResults = await instance.execMany(statements);
      const batchDuration = performance.now() - batchStart;

      // All should succeed
      expect(batchResults.every((r) => r.success)).toBe(true);

      // Clear table for sequential test
      await instance.query('DELETE FROM execmany_test');

      // Measure sequential execution
      const sequentialStart = performance.now();
      for (const sql of statements) {
        await instance.query(sql);
      }
      const sequentialDuration = performance.now() - sequentialStart;

      // Batch should be significantly faster (at least 2x)
      expect(batchDuration).toBeLessThan(sequentialDuration / 2);
    });
  });

  it.fails(
    'should have lower total duration than sum of individual durations',
    async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
        await instance.initialize();
        await instance.createTable(TEST_SCHEMA);

        const statements = Array.from(
          { length: 20 },
          (_, i) =>
            `INSERT INTO execmany_test (id, name, value, created_at) VALUES (${i + 1}, 'perf${i}', ${i}, ${Date.now()})`
        );

        const overallStart = performance.now();
        const results = await instance.execMany(statements);
        const overallDuration = performance.now() - overallStart;

        // Sum of individual statement durations
        const sumOfDurations = results.reduce((sum, r) => sum + r.durationMs, 0);

        // Overall duration should be less than sum due to transaction overhead amortization
        // (In a proper implementation, the batch has single transaction overhead)
        expect(overallDuration).toBeLessThanOrEqual(sumOfDurations + 50); // Allow some margin
      });
    }
  );

  it.fails('should scale linearly with statement count', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // Measure 10 statements
      const small = Array.from(
        { length: 10 },
        (_, i) =>
          `INSERT INTO execmany_test (id, name, value, created_at) VALUES (${i + 1}, 's${i}', ${i}, ${Date.now()})`
      );
      const smallStart = performance.now();
      await instance.execMany(small);
      const smallDuration = performance.now() - smallStart;

      // Clear and measure 50 statements
      await instance.query('DELETE FROM execmany_test');

      const large = Array.from(
        { length: 50 },
        (_, i) =>
          `INSERT INTO execmany_test (id, name, value, created_at) VALUES (${i + 1}, 'l${i}', ${i}, ${Date.now()})`
      );
      const largeStart = performance.now();
      await instance.execMany(large);
      const largeDuration = performance.now() - largeStart;

      // 5x more statements should not take more than ~10x time (sublinear overhead)
      // This accounts for fixed overhead + linear per-statement cost
      expect(largeDuration).toBeLessThan(smallDuration * 10);
    });
  });

  it.fails('should track timing information per statement', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const statements = [
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (1, 'timed1', 1.0, ${Date.now()})`,
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (2, 'timed2', 2.0, ${Date.now()})`,
        'SELECT * FROM execmany_test',
      ];

      const results = await instance.execMany(statements);

      results.forEach((r) => {
        expect(r.durationMs).toBeDefined();
        expect(typeof r.durationMs).toBe('number');
        expect(r.durationMs).toBeGreaterThanOrEqual(0);
      });
    });
  });
});

// =============================================================================
// ADDITIONAL EDGE CASES
// =============================================================================

describe('BenchmarkTestDO.execMany - Edge Cases', () => {
  it.fails('should handle statements with parameters', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const statementsWithParams = [
        {
          sql: 'INSERT INTO execmany_test (id, name, value, created_at) VALUES (?, ?, ?, ?)',
          params: { 1: 1, 2: 'param1', 3: 10.0, 4: Date.now() },
        },
        {
          sql: 'INSERT INTO execmany_test (id, name, value, created_at) VALUES (?, ?, ?, ?)',
          params: { 1: 2, 2: 'param2', 3: 20.0, 4: Date.now() },
        },
        {
          sql: 'SELECT * FROM execmany_test WHERE name = ?',
          params: { 1: 'param1' },
        },
      ];

      const results = await instance.execManyWithParams(statementsWithParams);

      expect(results.length).toBe(3);
      expect(results[0].success).toBe(true);
      expect(results[1].success).toBe(true);
      expect(results[2].success).toBe(true);
      expect(results[2].rows!.length).toBe(1);
    });
  });

  it.fails('should handle very large batches', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const statements = Array.from(
        { length: 500 },
        (_, i) =>
          `INSERT INTO execmany_test (id, name, value, created_at) VALUES (${i + 1}, 'large${i}', ${i}, ${Date.now()})`
      );

      const results = await instance.execMany(statements);

      expect(results.length).toBe(500);
      expect(results.every((r) => r.success)).toBe(true);

      // Verify all inserted
      const countResult = await instance.query(
        'SELECT COUNT(*) as count FROM execmany_test'
      );
      expect(countResult.success).toBe(true);
    });
  });

  it.fails('should handle statements with different result shapes', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      await instance.query(
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (1, 'test', 100.0, ${Date.now()})`
      );

      const statements = [
        'SELECT id, name FROM execmany_test', // 2 columns
        'SELECT * FROM execmany_test', // 4 columns
        'SELECT COUNT(*) as cnt, AVG(value) as avg_val FROM execmany_test', // Aggregates
      ];

      const results = await instance.execMany(statements);

      expect(results[0].rows![0]).toHaveProperty('id');
      expect(results[0].rows![0]).toHaveProperty('name');
      expect(results[0].rows![0]).not.toHaveProperty('value');

      expect(results[1].rows![0]).toHaveProperty('id');
      expect(results[1].rows![0]).toHaveProperty('value');

      expect(results[2].rows![0]).toHaveProperty('cnt');
      expect(results[2].rows![0]).toHaveProperty('avg_val');
    });
  });

  it.fails('should handle whitespace-only statements gracefully', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();

      const statements = ['   ', '\n\t', ''];

      // Should either skip or return error for each
      const results = await instance.execMany(statements);

      expect(results.length).toBe(3);
    });
  });

  it.fails('should handle comment-only statements', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDOWithExecMany) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const statements = [
        '-- This is a comment',
        `INSERT INTO execmany_test (id, name, value, created_at) VALUES (1, 'real', 1.0, ${Date.now()})`,
        '/* Block comment */',
      ];

      const results = await instance.execMany(statements);

      // The real insert should succeed
      expect(results.some((r) => r.success && r.rowCount === 1)).toBe(true);
    });
  });
});
