/**
 * SQLite OG Benchmark Adapter Tests
 *
 * Tests for the original SQLite implementation using sql.js (WASM).
 *
 * NOTE: These tests are SKIPPED because they require the cloudflare-worker-sqlite-wasm
 * package which cannot be properly imported in the vitest-pool-workers environment
 * due to WASM module resolution limitations.
 *
 * See: https://developers.cloudflare.com/workers/testing/vitest-integration/known-issues/#module-resolution
 *
 * To run these tests, you would need:
 * 1. A proper Cloudflare Workers deployment environment
 * 2. Vite configuration to bundle the WASM module
 *
 * @module benchmarks/adapters/__tests__/sqlite-og.test
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  SQLiteOGAdapter,
  createSQLiteOGAdapter,
  type SQLiteOGAdapterConfig,
} from '../sqlite-og.js';
import { type TableSchemaConfig } from '../../types.js';

// Skip all tests - WASM module imports are not supported in vitest-pool-workers
// The cloudflare-worker-sqlite-wasm package requires special bundling that
// isn't available in the test environment.
const SKIP_REASON = 'cloudflare-worker-sqlite-wasm WASM imports not supported in vitest-pool-workers';

// =============================================================================
// Test Setup (placeholder - actual adapter creation would require WASM)
// =============================================================================

/**
 * Create a Workers-compatible adapter using the cloudflare-worker-sqlite-wasm package.
 * This function is a placeholder since the WASM module cannot be imported in tests.
 */
async function createTestAdapter(): Promise<SQLiteOGAdapter> {
  // This would require:
  // import wasm from 'cloudflare-worker-sqlite-wasm/dist/sql-cf-wasm.wasm';
  // import initSqlJs from 'cloudflare-worker-sqlite-wasm/dist/sql-cf-wasm.js';
  throw new Error('WASM module not available in test environment');
}

describe.skip('SQLiteOGAdapter', () => {
  let adapter: SQLiteOGAdapter;

  const testSchema: TableSchemaConfig = {
    tableName: 'test_users',
    columns: [
      { name: 'id', type: 'INTEGER' },
      { name: 'name', type: 'TEXT', nullable: false },
      { name: 'email', type: 'TEXT', nullable: false },
      { name: 'age', type: 'INTEGER', nullable: true },
      { name: 'balance', type: 'REAL', nullable: true },
      { name: 'created_at', type: 'INTEGER', nullable: true },
    ],
    primaryKey: 'id',
    indexes: ['email', 'name'],
  };

  beforeEach(async () => {
    adapter = await createTestAdapter();
  });

  afterEach(async () => {
    await adapter.cleanup();
  });

  // ===========================================================================
  // Initialization Tests
  // ===========================================================================

  describe('initialization', () => {
    it('should initialize successfully', async () => {
      const freshAdapter = await createTestAdapter();
      expect(freshAdapter.name).toBe('sqlite-og');
      await freshAdapter.cleanup();
    });

    it('should have correct name and version', () => {
      expect(adapter.name).toBe('sqlite-og');
      expect(adapter.version).toBe('1.0.0');
    });

    it('should throw if operations attempted before initialization', async () => {
      const uninitAdapter = new SQLiteOGAdapter();
      await expect(uninitAdapter.createTable(testSchema)).rejects.toThrow(
        'SQLite OG adapter not initialized'
      );
    });

    it('should allow re-initialization after cleanup', async () => {
      await adapter.cleanup();
      // Re-create using Workers-compatible method
      adapter = await createTestAdapter();
      expect(adapter.name).toBe('sqlite-og');
    });
  });

  // ===========================================================================
  // Table Management Tests
  // ===========================================================================

  describe('table management', () => {
    it('should create a table with schema', async () => {
      await expect(adapter.createTable(testSchema)).resolves.not.toThrow();
    });

    it('should create table with indexes', async () => {
      await adapter.createTable(testSchema);

      // Verify table exists by inserting a row
      const result = await adapter.insert('test_users', {
        id: 1,
        name: 'Alice',
        email: 'alice@example.com',
      });
      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(1);
    });

    it('should drop a table', async () => {
      await adapter.createTable(testSchema);
      await adapter.insert('test_users', {
        id: 1,
        name: 'Alice',
        email: 'alice@example.com',
      });

      await expect(adapter.dropTable('test_users')).resolves.not.toThrow();

      // Verify table is gone by trying to insert
      const result = await adapter.insert('test_users', {
        id: 2,
        name: 'Bob',
        email: 'bob@example.com',
      });
      expect(result.success).toBe(false);
    });

    it('should handle dropping non-existent table gracefully', async () => {
      await expect(adapter.dropTable('non_existent')).resolves.not.toThrow();
    });
  });

  // ===========================================================================
  // INSERT (Create) Tests
  // ===========================================================================

  describe('INSERT operations', () => {
    beforeEach(async () => {
      await adapter.createTable(testSchema);
    });

    it('should insert a single row', async () => {
      const result = await adapter.insert('test_users', {
        id: 1,
        name: 'Alice',
        email: 'alice@example.com',
        age: 30,
        balance: 100.50,
      });

      expect(result.type).toBe('create');
      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(1);
      expect(result.durationMs).toBeGreaterThanOrEqual(0);
      expect(result.startedAt).toBeGreaterThan(0);
    });

    it('should handle null values', async () => {
      const result = await adapter.insert('test_users', {
        id: 1,
        name: 'Bob',
        email: 'bob@example.com',
        age: null,
        balance: null,
      });

      expect(result.success).toBe(true);
    });

    it('should return error for duplicate primary key', async () => {
      await adapter.insert('test_users', {
        id: 1,
        name: 'Alice',
        email: 'alice@example.com',
      });

      const result = await adapter.insert('test_users', {
        id: 1,
        name: 'Bob',
        email: 'bob@example.com',
      });

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });

    it('should insert batch of rows', async () => {
      const rows = Array.from({ length: 100 }, (_, i) => ({
        id: i + 1,
        name: `User${i}`,
        email: `user${i}@example.com`,
        age: 20 + (i % 50),
        balance: Math.random() * 1000,
      }));

      const result = await adapter.insertBatch('test_users', rows);

      expect(result.type).toBe('batch');
      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(100);
    });

    it('should handle empty batch', async () => {
      const result = await adapter.insertBatch('test_users', []);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(0);
    });

    it('should rollback batch on error', async () => {
      // Insert one row first
      await adapter.insert('test_users', {
        id: 50,
        name: 'Existing',
        email: 'existing@example.com',
      });

      // Try to insert batch with duplicate
      const rows = Array.from({ length: 100 }, (_, i) => ({
        id: i + 1, // id 50 will conflict
        name: `User${i}`,
        email: `user${i}@example.com`,
      }));

      const result = await adapter.insertBatch('test_users', rows);

      expect(result.success).toBe(false);
    });

    it('should serialize Date objects to integers', async () => {
      const now = new Date();
      const result = await adapter.insert('test_users', {
        id: 1,
        name: 'DateTime Test',
        email: 'dt@example.com',
        created_at: now,
      });

      expect(result.success).toBe(true);
    });
  });

  // ===========================================================================
  // SELECT (Read) Tests
  // ===========================================================================

  describe('SELECT operations', () => {
    beforeEach(async () => {
      await adapter.createTable(testSchema);

      // Insert test data
      const rows = [
        { id: 1, name: 'Alice', email: 'alice@example.com', age: 25, balance: 100.0 },
        { id: 2, name: 'Bob', email: 'bob@example.com', age: 30, balance: 200.0 },
        { id: 3, name: 'Carol', email: 'carol@example.com', age: 35, balance: 300.0 },
        { id: 4, name: 'Dave', email: 'dave@example.com', age: 40, balance: 400.0 },
        { id: 5, name: 'Eve', email: 'eve@example.com', age: 45, balance: 500.0 },
      ];
      await adapter.insertBatch('test_users', rows);
    });

    it('should read a row by primary key', async () => {
      const result = await adapter.read('test_users', 'id', 1);

      expect(result.type).toBe('read');
      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(1);
    });

    it('should return 0 rows for non-existent key', async () => {
      const result = await adapter.read('test_users', 'id', 999);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(0);
    });

    it('should read multiple rows with WHERE clause', async () => {
      const result = await adapter.readMany('test_users', 'age >= 30');

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(4); // Bob, Carol, Dave, Eve
    });

    it('should read with parameterized WHERE clause', async () => {
      const result = await adapter.readMany(
        'test_users',
        'age >= :minAge AND age <= :maxAge',
        { minAge: 30, maxAge: 40 }
      );

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(3); // Bob, Carol, Dave
    });
  });

  // ===========================================================================
  // UPDATE Tests
  // ===========================================================================

  describe('UPDATE operations', () => {
    beforeEach(async () => {
      await adapter.createTable(testSchema);
      await adapter.insert('test_users', {
        id: 1,
        name: 'Alice',
        email: 'alice@example.com',
        age: 25,
        balance: 100.0,
      });
    });

    it('should update a row', async () => {
      const result = await adapter.update('test_users', 'id', 1, {
        name: 'Alice Updated',
        balance: 150.0,
      });

      expect(result.type).toBe('update');
      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(1);
    });

    it('should return 0 rows for non-existent key', async () => {
      const result = await adapter.update('test_users', 'id', 999, {
        name: 'Nobody',
      });

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(0);
    });

    it('should update multiple fields at once', async () => {
      const result = await adapter.update('test_users', 'id', 1, {
        name: 'New Name',
        email: 'newemail@example.com',
        age: 26,
        balance: 200.0,
      });

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(1);
    });
  });

  // ===========================================================================
  // DELETE Tests
  // ===========================================================================

  describe('DELETE operations', () => {
    beforeEach(async () => {
      await adapter.createTable(testSchema);
      const rows = [
        { id: 1, name: 'Alice', email: 'alice@example.com' },
        { id: 2, name: 'Bob', email: 'bob@example.com' },
        { id: 3, name: 'Carol', email: 'carol@example.com' },
      ];
      await adapter.insertBatch('test_users', rows);
    });

    it('should delete a row by primary key', async () => {
      const result = await adapter.delete('test_users', 'id', 1);

      expect(result.type).toBe('delete');
      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(1);
    });

    it('should return 0 rows for non-existent key', async () => {
      const result = await adapter.delete('test_users', 'id', 999);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(0);
    });

    it('should verify row is actually deleted', async () => {
      await adapter.delete('test_users', 'id', 1);

      const readResult = await adapter.read('test_users', 'id', 1);
      expect(readResult.rowCount).toBe(0);
    });
  });

  // ===========================================================================
  // Raw Query Tests
  // ===========================================================================

  describe('raw query operations', () => {
    beforeEach(async () => {
      await adapter.createTable(testSchema);
      const rows = [
        { id: 1, name: 'Alice', email: 'alice@example.com', age: 25, balance: 100.0 },
        { id: 2, name: 'Bob', email: 'bob@example.com', age: 30, balance: 200.0 },
        { id: 3, name: 'Carol', email: 'carol@example.com', age: 35, balance: 300.0 },
      ];
      await adapter.insertBatch('test_users', rows);
    });

    it('should execute SELECT query', async () => {
      const result = await adapter.query('SELECT * FROM test_users');

      expect(result.type).toBe('read');
      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(3);
    });

    it('should execute parameterized query', async () => {
      const result = await adapter.query(
        'SELECT * FROM test_users WHERE age > :minAge',
        { minAge: 25 }
      );

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(2);
    });

    it('should execute INSERT query', async () => {
      const result = await adapter.query(
        'INSERT INTO test_users (id, name, email) VALUES (:id, :name, :email)',
        { id: 4, name: 'Dave', email: 'dave@example.com' }
      );

      expect(result.type).toBe('create');
      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(1);
    });

    it('should execute UPDATE query', async () => {
      const result = await adapter.query(
        'UPDATE test_users SET balance = balance + 50 WHERE age >= 30'
      );

      expect(result.type).toBe('update');
      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(2);
    });

    it('should execute DELETE query', async () => {
      const result = await adapter.query('DELETE FROM test_users WHERE age < 30');

      expect(result.type).toBe('delete');
      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(1);
    });

    it('should execute aggregation query', async () => {
      const result = await adapter.query(
        'SELECT COUNT(*) as count, AVG(age) as avg_age, SUM(balance) as total_balance FROM test_users'
      );

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(1);
    });

    it('should execute complex JOIN query', async () => {
      // Create orders table
      await adapter.createTable({
        tableName: 'test_orders',
        columns: [
          { name: 'id', type: 'INTEGER' },
          { name: 'user_id', type: 'INTEGER' },
          { name: 'amount', type: 'REAL' },
        ],
        primaryKey: 'id',
      });

      await adapter.insertBatch('test_orders', [
        { id: 1, user_id: 1, amount: 50.0 },
        { id: 2, user_id: 1, amount: 75.0 },
        { id: 3, user_id: 2, amount: 100.0 },
      ]);

      const result = await adapter.query(`
        SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total_amount
        FROM test_users u
        LEFT JOIN test_orders o ON u.id = o.user_id
        GROUP BY u.id
        ORDER BY total_amount DESC
      `);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(3);
    });
  });

  // ===========================================================================
  // Transaction Tests
  // ===========================================================================

  describe('transaction operations', () => {
    beforeEach(async () => {
      await adapter.createTable(testSchema);
    });

    it('should execute multiple operations in a transaction', async () => {
      const result = await adapter.transaction([
        { type: 'insert', tableName: 'test_users', data: { id: 1, name: 'Alice', email: 'alice@example.com' } },
        { type: 'insert', tableName: 'test_users', data: { id: 2, name: 'Bob', email: 'bob@example.com' } },
        { type: 'update', tableName: 'test_users', data: { id: 1, name: 'Alice Updated' } },
      ]);

      expect(result.type).toBe('transaction');
      expect(result.success).toBe(true);
      expect(result.rowCount).toBeGreaterThan(0);
    });

    it('should rollback transaction on error', async () => {
      // Insert a row first
      await adapter.insert('test_users', { id: 1, name: 'Existing', email: 'existing@example.com' });

      const result = await adapter.transaction([
        { type: 'insert', tableName: 'test_users', data: { id: 2, name: 'New', email: 'new@example.com' } },
        { type: 'insert', tableName: 'test_users', data: { id: 1, name: 'Duplicate', email: 'dup@example.com' } }, // Should fail
      ]);

      expect(result.success).toBe(false);

      // Verify rollback - id: 2 should not exist
      const checkResult = await adapter.read('test_users', 'id', 2);
      expect(checkResult.rowCount).toBe(0);
    });

    it('should handle delete in transaction', async () => {
      await adapter.insert('test_users', { id: 1, name: 'ToDelete', email: 'delete@example.com' });

      const result = await adapter.transaction([
        { type: 'delete', tableName: 'test_users', data: { id: 1 } },
      ]);

      expect(result.success).toBe(true);

      const checkResult = await adapter.read('test_users', 'id', 1);
      expect(checkResult.rowCount).toBe(0);
    });
  });

  // ===========================================================================
  // Storage Metrics Tests
  // ===========================================================================

  describe('storage metrics', () => {
    it('should return storage metrics', async () => {
      await adapter.createTable(testSchema);

      const rows = Array.from({ length: 100 }, (_, i) => ({
        id: i + 1,
        name: `User${i}`,
        email: `user${i}@example.com`,
        age: 20 + i,
        balance: i * 10.5,
      }));
      await adapter.insertBatch('test_users', rows);

      const metrics = await adapter.getStorageMetrics();

      expect(metrics.totalBytes).toBeGreaterThan(0);
      expect(metrics.rowCount).toBe(100);
      expect(metrics.tableCount).toBe(1);
      expect(metrics.limitUtilization).toBeGreaterThanOrEqual(0);
      expect(metrics.limitUtilization).toBeLessThan(1); // Should be tiny for test data
    });

    it('should handle empty database metrics', async () => {
      const metrics = await adapter.getStorageMetrics();

      expect(metrics.totalBytes).toBeGreaterThanOrEqual(0);
      expect(metrics.rowCount).toBe(0);
      expect(metrics.tableCount).toBe(0);
    });
  });

  // ===========================================================================
  // Cold Start Metrics Tests
  // ===========================================================================

  describe('cold start metrics', () => {
    it('should measure initialization timing', async () => {
      // Instead of using measureColdStart which re-initializes,
      // we measure by creating a fresh adapter
      const startTime = performance.now();
      const freshAdapter = await createTestAdapter();
      const initTime = performance.now() - startTime;

      // Run a query to measure total time to first query
      const queryStart = performance.now();
      await freshAdapter.query('SELECT 1');
      const queryTime = performance.now() - queryStart;

      // Init time might be 0 in Workers due to timing precision
      // Just verify we got numbers back
      expect(typeof initTime).toBe('number');
      expect(initTime).toBeGreaterThanOrEqual(0);
      expect(typeof queryTime).toBe('number');
      expect(queryTime).toBeGreaterThanOrEqual(0);

      await freshAdapter.cleanup();
    });
  });

  // ===========================================================================
  // Full SQL Compatibility Tests
  // ===========================================================================

  describe('full SQL compatibility', () => {
    beforeEach(async () => {
      await adapter.createTable(testSchema);
      await adapter.insertBatch('test_users', [
        { id: 1, name: 'Alice', email: 'alice@example.com', age: 25, balance: 100.0 },
        { id: 2, name: 'Bob', email: 'bob@example.com', age: 30, balance: 200.0 },
        { id: 3, name: 'Carol', email: 'carol@example.com', age: 35, balance: 300.0 },
        { id: 4, name: 'Dave', email: 'dave@example.com', age: 40, balance: 400.0 },
        { id: 5, name: 'Eve', email: 'eve@example.com', age: 45, balance: 500.0 },
      ]);
    });

    it('should support ORDER BY', async () => {
      const result = await adapter.query('SELECT * FROM test_users ORDER BY age DESC');
      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(5);
    });

    it('should support LIMIT and OFFSET', async () => {
      const result = await adapter.query('SELECT * FROM test_users LIMIT 2 OFFSET 1');
      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(2);
    });

    it('should support GROUP BY with HAVING', async () => {
      // Create data for grouping
      await adapter.createTable({
        tableName: 'test_sales',
        columns: [
          { name: 'id', type: 'INTEGER' },
          { name: 'category', type: 'TEXT' },
          { name: 'amount', type: 'REAL' },
        ],
        primaryKey: 'id',
      });

      await adapter.insertBatch('test_sales', [
        { id: 1, category: 'A', amount: 100 },
        { id: 2, category: 'A', amount: 200 },
        { id: 3, category: 'B', amount: 50 },  // B total = 75
        { id: 4, category: 'B', amount: 25 },
        { id: 5, category: 'C', amount: 500 },
      ]);

      const result = await adapter.query(`
        SELECT category, SUM(amount) as total
        FROM test_sales
        GROUP BY category
        HAVING SUM(amount) > 100
      `);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(2); // A (300) and C (500), B is only 75
    });

    it('should support subqueries', async () => {
      const result = await adapter.query(`
        SELECT * FROM test_users
        WHERE age > (SELECT AVG(age) FROM test_users)
      `);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(2); // Dave and Eve (above avg of 35)
    });

    it('should support CASE expressions', async () => {
      const result = await adapter.query(`
        SELECT name,
          CASE
            WHEN age < 30 THEN 'young'
            WHEN age < 40 THEN 'middle'
            ELSE 'senior'
          END as age_group
        FROM test_users
      `);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(5);
    });

    it('should support window functions', async () => {
      const result = await adapter.query(`
        SELECT name, age,
          ROW_NUMBER() OVER (ORDER BY age) as row_num,
          SUM(balance) OVER (ORDER BY age) as running_total
        FROM test_users
      `);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(5);
    });

    it('should support CTEs (Common Table Expressions)', async () => {
      const result = await adapter.query(`
        WITH high_balance AS (
          SELECT * FROM test_users WHERE balance > 200
        )
        SELECT COUNT(*) as count FROM high_balance
      `);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(1);
    });

    it('should support COALESCE and NULLIF', async () => {
      await adapter.insert('test_users', {
        id: 6,
        name: 'Frank',
        email: 'frank@example.com',
        age: null,
        balance: null,
      });

      const result = await adapter.query(`
        SELECT name, COALESCE(age, 0) as age_or_zero, NULLIF(balance, 0) as balance_or_null
        FROM test_users
        WHERE id = 6
      `);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(1);
    });

    it('should support string functions', async () => {
      const result = await adapter.query(`
        SELECT
          UPPER(name) as upper_name,
          LOWER(email) as lower_email,
          LENGTH(name) as name_len,
          SUBSTR(email, 1, 5) as email_prefix
        FROM test_users
        WHERE id = 1
      `);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(1);
    });

    it('should support date functions', async () => {
      const result = await adapter.query(`
        SELECT
          date('now') as today,
          datetime('now') as now,
          strftime('%Y-%m-%d', 'now') as formatted
      `);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(1);
    });

    it('should support LIKE pattern matching', async () => {
      const result = await adapter.query(`
        SELECT * FROM test_users WHERE name LIKE 'A%'
      `);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(1); // Alice
    });

    it('should support IN clause', async () => {
      const result = await adapter.query(`
        SELECT * FROM test_users WHERE id IN (1, 3, 5)
      `);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(3);
    });

    it('should support BETWEEN', async () => {
      const result = await adapter.query(`
        SELECT * FROM test_users WHERE age BETWEEN 30 AND 40
      `);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(3); // Bob, Carol, Dave
    });

    it('should support UNION', async () => {
      const result = await adapter.query(`
        SELECT name FROM test_users WHERE age < 30
        UNION
        SELECT name FROM test_users WHERE balance > 400
      `);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBeGreaterThanOrEqual(2);
    });

    it('should support EXISTS', async () => {
      // Create orders table
      await adapter.createTable({
        tableName: 'test_orders',
        columns: [
          { name: 'id', type: 'INTEGER' },
          { name: 'user_id', type: 'INTEGER' },
          { name: 'amount', type: 'REAL' },
        ],
        primaryKey: 'id',
      });

      await adapter.insertBatch('test_orders', [
        { id: 1, user_id: 1, amount: 50 },
        { id: 2, user_id: 3, amount: 100 },
      ]);

      const result = await adapter.query(`
        SELECT * FROM test_users u
        WHERE EXISTS (
          SELECT 1 FROM test_orders o WHERE o.user_id = u.id
        )
      `);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(2); // Alice and Carol
    });
  });

  // ===========================================================================
  // Edge Cases and Error Handling
  // ===========================================================================

  describe('edge cases and error handling', () => {
    beforeEach(async () => {
      await adapter.createTable(testSchema);
    });

    it('should handle empty strings', async () => {
      const result = await adapter.insert('test_users', {
        id: 1,
        name: '',
        email: '',
      });

      expect(result.success).toBe(true);
    });

    it('should handle very long strings', async () => {
      const longName = 'A'.repeat(10000);
      const result = await adapter.insert('test_users', {
        id: 1,
        name: longName,
        email: 'long@example.com',
      });

      expect(result.success).toBe(true);
    });

    it('should handle special characters', async () => {
      const result = await adapter.insert('test_users', {
        id: 1,
        name: "O'Brien \"Bob\" <test>",
        email: 'special@example.com',
      });

      expect(result.success).toBe(true);
    });

    it('should handle unicode characters', async () => {
      const result = await adapter.insert('test_users', {
        id: 1,
        name: 'Test Unicode',
        email: 'unicode@example.com',
      });

      expect(result.success).toBe(true);
    });

    it('should handle negative numbers', async () => {
      const result = await adapter.insert('test_users', {
        id: 1,
        name: 'Negative',
        email: 'neg@example.com',
        balance: -100.50,
      });

      expect(result.success).toBe(true);
    });

    it('should handle very large numbers', async () => {
      const result = await adapter.insert('test_users', {
        id: 1,
        name: 'Large',
        email: 'large@example.com',
        balance: 999999999999.99,
      });

      expect(result.success).toBe(true);
    });

    it('should handle zero values', async () => {
      const result = await adapter.insert('test_users', {
        id: 1,
        name: 'Zero',
        email: 'zero@example.com',
        age: 0,
        balance: 0.0,
      });

      expect(result.success).toBe(true);
    });

    it('should return error details on invalid SQL', async () => {
      const result = await adapter.query('SELECT * FROM nonexistent_table');

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
      expect(result.error).toContain('nonexistent_table');
    });

    it('should return error details on constraint violation', async () => {
      await adapter.insert('test_users', {
        id: 1,
        name: 'First',
        email: 'first@example.com',
      });

      const result = await adapter.insert('test_users', {
        id: 1,
        name: 'Duplicate',
        email: 'dup@example.com',
      });

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });
  });

  // ===========================================================================
  // Benchmark Operation Format Tests
  // ===========================================================================

  describe('benchmark operation format', () => {
    beforeEach(async () => {
      await adapter.createTable(testSchema);
    });

    it('should return complete BenchmarkOperation for insert', async () => {
      const result = await adapter.insert('test_users', {
        id: 1,
        name: 'Test',
        email: 'test@example.com',
      });

      expect(result).toMatchObject({
        type: expect.any(String),
        sql: expect.any(String),
        durationMs: expect.any(Number),
        rowCount: expect.any(Number),
        success: expect.any(Boolean),
        startedAt: expect.any(Number),
      });
    });

    it('should include params in operation result', async () => {
      const params = { id: 1, name: 'Test', email: 'test@example.com' };
      const result = await adapter.insert('test_users', params);

      expect(result.params).toBeDefined();
    });

    it('should include error message on failure', async () => {
      const result = await adapter.query('INVALID SQL SYNTAX');

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
      expect(typeof result.error).toBe('string');
    });

    it('should have positive duration for operations', async () => {
      const result = await adapter.insert('test_users', {
        id: 1,
        name: 'Test',
        email: 'test@example.com',
      });

      expect(result.durationMs).toBeGreaterThanOrEqual(0);
    });

    it('should have realistic startedAt timestamp', async () => {
      const before = performance.now();
      const result = await adapter.insert('test_users', {
        id: 1,
        name: 'Test',
        email: 'test@example.com',
      });
      const after = performance.now();

      expect(result.startedAt).toBeGreaterThanOrEqual(before);
      expect(result.startedAt).toBeLessThanOrEqual(after);
    });
  });
});

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('createSQLiteOGAdapter', () => {
  it('should create adapter with default config', () => {
    const adapter = createSQLiteOGAdapter();
    expect(adapter).toBeInstanceOf(SQLiteOGAdapter);
    expect(adapter.name).toBe('sqlite-og');
  });

  it('should create adapter with custom config', () => {
    const adapter = createSQLiteOGAdapter({
      inMemory: true,
      wasmUrl: 'https://example.com/sql-wasm.wasm',
    });
    expect(adapter).toBeInstanceOf(SQLiteOGAdapter);
  });
});
