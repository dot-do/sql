/**
 * D1 Benchmark Adapter Tests
 *
 * Tests the D1Adapter implementation using workers-vitest-pool.
 * NO MOCKS - all tests run against real D1 in miniflare.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { env } from 'cloudflare:test';
import { D1Adapter, createD1Adapter } from '../d1.js';
import { TableSchemaConfig, DEFAULT_BENCHMARK_CONFIG } from '../../types.js';

// =============================================================================
// Test Schema
// =============================================================================

const testSchema: TableSchemaConfig = {
  tableName: 'benchmark_test',
  columns: [
    { name: 'id', type: 'INTEGER' },
    { name: 'name', type: 'TEXT', nullable: false },
    { name: 'value', type: 'REAL' },
    { name: 'active', type: 'INTEGER' },
    { name: 'created_at', type: 'INTEGER' },
  ],
  primaryKey: 'id',
  indexes: ['name', 'created_at'],
};

// =============================================================================
// Test Suite
// =============================================================================

describe('D1Adapter', () => {
  let adapter: D1Adapter;

  beforeEach(async () => {
    adapter = createD1Adapter({ db: env.TEST_D1 });
    await adapter.initialize();
    await adapter.createTable(testSchema);
  });

  afterEach(async () => {
    await adapter.cleanup();
  });

  // ===========================================================================
  // Initialization Tests
  // ===========================================================================

  describe('initialization', () => {
    it('should have correct name and version', () => {
      expect(adapter.name).toBe('d1');
      expect(adapter.version).toBe('1.0.0');
    });

    it('should initialize successfully', async () => {
      const newAdapter = createD1Adapter({ db: env.TEST_D1 });
      await expect(newAdapter.initialize()).resolves.not.toThrow();
      await newAdapter.cleanup();
    });

    it('should be idempotent when initialized multiple times', async () => {
      await expect(adapter.initialize()).resolves.not.toThrow();
      await expect(adapter.initialize()).resolves.not.toThrow();
    });
  });

  // ===========================================================================
  // Table Operations Tests
  // ===========================================================================

  describe('table operations', () => {
    it('should create table with schema', async () => {
      // Table was created in beforeEach, verify it exists
      const result = await env.TEST_D1
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='benchmark_test'")
        .first();
      expect(result).toBeDefined();
      expect(result?.name).toBe('benchmark_test');
    });

    it('should create indexes', async () => {
      const indexes = await env.TEST_D1
        .prepare("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='benchmark_test'")
        .all();

      const indexNames = indexes.results?.map((r: any) => r.name) ?? [];
      expect(indexNames).toContain('idx_benchmark_test_name');
      expect(indexNames).toContain('idx_benchmark_test_created_at');
    });

    it('should drop table', async () => {
      await adapter.dropTable('benchmark_test');

      const result = await env.TEST_D1
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='benchmark_test'")
        .first();
      expect(result).toBeNull();
    });

    it('should handle dropping non-existent table gracefully', async () => {
      await expect(adapter.dropTable('non_existent_table')).resolves.not.toThrow();
    });
  });

  // ===========================================================================
  // Insert (Create) Tests
  // ===========================================================================

  describe('insert', () => {
    it('should insert a single row', async () => {
      const row = {
        id: 1,
        name: 'Test Item',
        value: 42.5,
        active: 1,
        created_at: Date.now(),
      };

      const op = await adapter.insert('benchmark_test', row);

      expect(op.type).toBe('create');
      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(1);
      expect(op.durationMs).toBeGreaterThanOrEqual(0);
      expect(op.error).toBeUndefined();
    });

    it('should track timing for insert operation', async () => {
      const row = { id: 1, name: 'Timed', value: 1.0, active: 1, created_at: Date.now() };

      const op = await adapter.insert('benchmark_test', row);

      expect(op.startedAt).toBeGreaterThan(0);
      expect(op.durationMs).toBeGreaterThanOrEqual(0);
      expect(op.durationMs).toBeLessThan(10000); // Should complete in under 10s
    });

    it('should handle insert error gracefully', async () => {
      // Insert same ID twice to trigger unique constraint
      await adapter.insert('benchmark_test', { id: 1, name: 'First', value: 1, active: 1, created_at: 0 });
      const op = await adapter.insert('benchmark_test', { id: 1, name: 'Duplicate', value: 2, active: 1, created_at: 0 });

      expect(op.success).toBe(false);
      expect(op.error).toBeDefined();
      expect(op.error).toContain('UNIQUE');
    });
  });

  // ===========================================================================
  // Batch Insert Tests
  // ===========================================================================

  describe('insertBatch', () => {
    it('should insert multiple rows in a batch', async () => {
      const rows = [
        { id: 1, name: 'Item 1', value: 10.0, active: 1, created_at: Date.now() },
        { id: 2, name: 'Item 2', value: 20.0, active: 1, created_at: Date.now() },
        { id: 3, name: 'Item 3', value: 30.0, active: 0, created_at: Date.now() },
      ];

      const op = await adapter.insertBatch('benchmark_test', rows);

      expect(op.type).toBe('batch');
      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(3);
    });

    it('should handle empty batch', async () => {
      const op = await adapter.insertBatch('benchmark_test', []);

      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(0);
    });

    it('should handle large batches by chunking', async () => {
      // Create more than 100 rows to test chunking
      const rows = Array.from({ length: 150 }, (_, i) => ({
        id: i + 1,
        name: `Item ${i + 1}`,
        value: i * 1.5,
        active: i % 2,
        created_at: Date.now(),
      }));

      const op = await adapter.insertBatch('benchmark_test', rows);

      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(150);

      // Verify all rows were inserted
      const count = await env.TEST_D1
        .prepare('SELECT COUNT(*) as count FROM benchmark_test')
        .first<{ count: number }>();
      expect(count?.count).toBe(150);
    });
  });

  // ===========================================================================
  // Read Tests
  // ===========================================================================

  describe('read', () => {
    beforeEach(async () => {
      await adapter.insertBatch('benchmark_test', [
        { id: 1, name: 'Alice', value: 100.0, active: 1, created_at: 1000 },
        { id: 2, name: 'Bob', value: 200.0, active: 1, created_at: 2000 },
        { id: 3, name: 'Carol', value: 300.0, active: 0, created_at: 3000 },
      ]);
    });

    it('should read a single row by primary key', async () => {
      const op = await adapter.read('benchmark_test', 'id', 1);

      expect(op.type).toBe('read');
      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(1);
    });

    it('should return 0 rows for non-existent key', async () => {
      const op = await adapter.read('benchmark_test', 'id', 999);

      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(0);
    });

    it('should track timing for read operation', async () => {
      const op = await adapter.read('benchmark_test', 'id', 1);

      expect(op.durationMs).toBeGreaterThanOrEqual(0);
      expect(op.startedAt).toBeGreaterThan(0);
    });
  });

  // ===========================================================================
  // Read Many Tests
  // ===========================================================================

  describe('readMany', () => {
    beforeEach(async () => {
      await adapter.insertBatch('benchmark_test', [
        { id: 1, name: 'Alice', value: 100.0, active: 1, created_at: 1000 },
        { id: 2, name: 'Bob', value: 200.0, active: 1, created_at: 2000 },
        { id: 3, name: 'Carol', value: 300.0, active: 0, created_at: 3000 },
        { id: 4, name: 'Dave', value: 400.0, active: 1, created_at: 4000 },
      ]);
    });

    it('should read multiple rows with WHERE clause', async () => {
      const op = await adapter.readMany('benchmark_test', 'active = ?', { active: 1 });

      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(3);
    });

    it('should read rows with range query', async () => {
      const op = await adapter.readMany('benchmark_test', 'value > ?', { minValue: 150.0 });

      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(3); // Bob, Carol, Dave
    });

    it('should return 0 rows when no match', async () => {
      const op = await adapter.readMany('benchmark_test', 'value > ?', { minValue: 1000.0 });

      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(0);
    });
  });

  // ===========================================================================
  // Update Tests
  // ===========================================================================

  describe('update', () => {
    beforeEach(async () => {
      await adapter.insert('benchmark_test', {
        id: 1,
        name: 'Original',
        value: 100.0,
        active: 1,
        created_at: Date.now(),
      });
    });

    it('should update a single row', async () => {
      const op = await adapter.update('benchmark_test', 'id', 1, {
        name: 'Updated',
        value: 200.0,
      });

      expect(op.type).toBe('update');
      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(1);

      // Verify the update
      const result = await env.TEST_D1
        .prepare('SELECT name, value FROM benchmark_test WHERE id = 1')
        .first();
      expect(result?.name).toBe('Updated');
      expect(result?.value).toBe(200.0);
    });

    it('should return 0 rows when updating non-existent record', async () => {
      const op = await adapter.update('benchmark_test', 'id', 999, { name: 'NotFound' });

      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(0);
    });

    it('should track timing for update operation', async () => {
      const op = await adapter.update('benchmark_test', 'id', 1, { name: 'Timed' });

      expect(op.durationMs).toBeGreaterThanOrEqual(0);
      expect(op.startedAt).toBeGreaterThan(0);
    });
  });

  // ===========================================================================
  // Delete Tests
  // ===========================================================================

  describe('delete', () => {
    beforeEach(async () => {
      await adapter.insertBatch('benchmark_test', [
        { id: 1, name: 'ToDelete', value: 1.0, active: 1, created_at: 0 },
        { id: 2, name: 'ToKeep', value: 2.0, active: 1, created_at: 0 },
      ]);
    });

    it('should delete a single row', async () => {
      const op = await adapter.delete('benchmark_test', 'id', 1);

      expect(op.type).toBe('delete');
      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(1);

      // Verify deletion
      const result = await env.TEST_D1
        .prepare('SELECT id FROM benchmark_test WHERE id = 1')
        .first();
      expect(result).toBeNull();
    });

    it('should return 0 rows when deleting non-existent record', async () => {
      const op = await adapter.delete('benchmark_test', 'id', 999);

      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(0);
    });

    it('should preserve other rows after delete', async () => {
      await adapter.delete('benchmark_test', 'id', 1);

      const result = await env.TEST_D1
        .prepare('SELECT id FROM benchmark_test WHERE id = 2')
        .first();
      expect(result?.id).toBe(2);
    });
  });

  // ===========================================================================
  // Query Tests
  // ===========================================================================

  describe('query', () => {
    beforeEach(async () => {
      await adapter.insertBatch('benchmark_test', [
        { id: 1, name: 'Alice', value: 100.0, active: 1, created_at: 1000 },
        { id: 2, name: 'Bob', value: 200.0, active: 1, created_at: 2000 },
        { id: 3, name: 'Carol', value: 300.0, active: 0, created_at: 3000 },
      ]);
    });

    it('should execute SELECT query', async () => {
      const op = await adapter.query('SELECT * FROM benchmark_test WHERE active = ?', { active: 1 });

      expect(op.type).toBe('query');
      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(2);
    });

    it('should execute aggregate query', async () => {
      const op = await adapter.query('SELECT SUM(value) as total FROM benchmark_test');

      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(1);
    });

    it('should execute UPDATE query', async () => {
      const op = await adapter.query('UPDATE benchmark_test SET active = ? WHERE id = ?', { newActive: 0, id: 1 });

      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(1);
    });

    it('should handle query errors gracefully', async () => {
      const op = await adapter.query('SELECT * FROM non_existent_table');

      expect(op.success).toBe(false);
      expect(op.error).toBeDefined();
    });
  });

  // ===========================================================================
  // Transaction Tests
  // ===========================================================================

  describe('transaction', () => {
    it('should execute multiple operations atomically', async () => {
      const op = await adapter.transaction([
        { type: 'insert', tableName: 'benchmark_test', data: { id: 1, name: 'First', value: 1.0, active: 1, created_at: 0 } },
        { type: 'insert', tableName: 'benchmark_test', data: { id: 2, name: 'Second', value: 2.0, active: 1, created_at: 0 } },
        { type: 'update', tableName: 'benchmark_test', data: { id: 1, name: 'Updated First' } },
      ]);

      expect(op.type).toBe('transaction');
      expect(op.success).toBe(true);
      expect(op.rowCount).toBe(3); // 2 inserts + 1 update

      // Verify operations
      const result = await env.TEST_D1
        .prepare('SELECT name FROM benchmark_test WHERE id = 1')
        .first();
      expect(result?.name).toBe('Updated First');
    });

    it('should support delete in transaction', async () => {
      // First insert a row
      await adapter.insert('benchmark_test', { id: 1, name: 'ToDelete', value: 1.0, active: 1, created_at: 0 });

      const op = await adapter.transaction([
        { type: 'insert', tableName: 'benchmark_test', data: { id: 2, name: 'New', value: 2.0, active: 1, created_at: 0 } },
        { type: 'delete', tableName: 'benchmark_test', data: { id: 1 } },
      ]);

      expect(op.success).toBe(true);

      // Verify delete
      const deleted = await env.TEST_D1
        .prepare('SELECT id FROM benchmark_test WHERE id = 1')
        .first();
      expect(deleted).toBeNull();

      // Verify insert
      const inserted = await env.TEST_D1
        .prepare('SELECT id FROM benchmark_test WHERE id = 2')
        .first();
      expect(inserted?.id).toBe(2);
    });

    it('should track timing for transaction', async () => {
      const op = await adapter.transaction([
        { type: 'insert', tableName: 'benchmark_test', data: { id: 1, name: 'Test', value: 1.0, active: 1, created_at: 0 } },
      ]);

      expect(op.durationMs).toBeGreaterThanOrEqual(0);
      expect(op.startedAt).toBeGreaterThan(0);
    });
  });

  // ===========================================================================
  // Metrics Tests
  // ===========================================================================

  describe('getStorageMetrics', () => {
    it('should return storage metrics', async () => {
      await adapter.insertBatch('benchmark_test', [
        { id: 1, name: 'Row1', value: 1.0, active: 1, created_at: 0 },
        { id: 2, name: 'Row2', value: 2.0, active: 1, created_at: 0 },
        { id: 3, name: 'Row3', value: 3.0, active: 1, created_at: 0 },
      ]);

      const metrics = await adapter.getStorageMetrics();

      expect(metrics.rowCount).toBeGreaterThanOrEqual(3);
      expect(metrics.tableCount).toBeGreaterThanOrEqual(1);
      expect(metrics.totalBytes).toBeGreaterThanOrEqual(0);
      expect(metrics.limitUtilization).toBeGreaterThanOrEqual(0);
      expect(metrics.limitUtilization).toBeLessThan(100);
    });

    it('should count tables correctly', async () => {
      // Create additional table
      await adapter.createTable({
        tableName: 'another_table',
        columns: [{ name: 'id', type: 'INTEGER' }],
        primaryKey: 'id',
      });

      const metrics = await adapter.getStorageMetrics();

      expect(metrics.tableCount).toBeGreaterThanOrEqual(2);
    });
  });

  // ===========================================================================
  // Cold Start Tests
  // ===========================================================================

  describe('measureColdStart', () => {
    it('should measure cold start time', async () => {
      const metrics = await adapter.measureColdStart();

      expect(metrics.timeToFirstQuery).toBeGreaterThanOrEqual(0);
      expect(metrics.initializationTime).toBe(0); // D1 doesn't have separate init
      expect(metrics.connectionTime).toBeGreaterThanOrEqual(0);
    });

    it('should return consistent metrics on repeated calls', async () => {
      const metrics1 = await adapter.measureColdStart();
      const metrics2 = await adapter.measureColdStart();

      // After first initialization, times should be similar (cached)
      expect(metrics1.timeToFirstQuery).toBe(metrics2.timeToFirstQuery);
    });

    it('should measure fresh cold start on new adapter', async () => {
      const freshAdapter = createD1Adapter({ db: env.TEST_D1 });

      const metrics = await freshAdapter.measureColdStart();

      expect(metrics.timeToFirstQuery).toBeGreaterThanOrEqual(0);
      expect(metrics.connectionTime).toBeGreaterThanOrEqual(0);

      await freshAdapter.cleanup();
    });
  });

  // ===========================================================================
  // Timing Measurement Tests
  // ===========================================================================

  describe('timing measurement', () => {
    it('should measure insert timing accurately', async () => {
      const start = performance.now();
      const op = await adapter.insert('benchmark_test', {
        id: 1, name: 'Timing', value: 1.0, active: 1, created_at: 0
      });
      const totalTime = performance.now() - start;

      // Operation duration should be close to total time (within overhead)
      expect(op.durationMs).toBeLessThanOrEqual(totalTime + 1);
      expect(op.durationMs).toBeGreaterThanOrEqual(0);
    });

    it('should track startedAt timestamp', async () => {
      const beforeOp = performance.now();
      const op = await adapter.insert('benchmark_test', {
        id: 1, name: 'Timestamp', value: 1.0, active: 1, created_at: 0
      });
      const afterOp = performance.now();

      expect(op.startedAt).toBeGreaterThanOrEqual(beforeOp);
      expect(op.startedAt).toBeLessThanOrEqual(afterOp);
    });

    it('should track batch operation timing', async () => {
      const rows = Array.from({ length: 50 }, (_, i) => ({
        id: i + 1, name: `Item${i}`, value: i, active: 1, created_at: 0
      }));

      const op = await adapter.insertBatch('benchmark_test', rows);

      // Batch should take some time (may be 0ms for fast operations)
      expect(op.durationMs).toBeGreaterThanOrEqual(0);
    });
  });

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('should handle special characters in strings', async () => {
      const row = {
        id: 1,
        name: "O'Reilly's \"Book\" -- test",
        value: 1.0,
        active: 1,
        created_at: 0,
      };

      const op = await adapter.insert('benchmark_test', row);
      expect(op.success).toBe(true);

      const result = await env.TEST_D1
        .prepare('SELECT name FROM benchmark_test WHERE id = 1')
        .first();
      expect(result?.name).toBe("O'Reilly's \"Book\" -- test");
    });

    it('should handle NULL values', async () => {
      // Create table with nullable column
      await adapter.createTable({
        tableName: 'nullable_test',
        columns: [
          { name: 'id', type: 'INTEGER' },
          { name: 'optional_value', type: 'TEXT', nullable: true },
        ],
        primaryKey: 'id',
      });

      const op = await adapter.insert('nullable_test', { id: 1, optional_value: null });
      expect(op.success).toBe(true);

      const result = await env.TEST_D1
        .prepare('SELECT optional_value FROM nullable_test WHERE id = 1')
        .first();
      expect(result?.optional_value).toBeNull();
    });

    it('should handle very long strings', async () => {
      const longString = 'x'.repeat(10000);
      const row = { id: 1, name: longString, value: 1.0, active: 1, created_at: 0 };

      const op = await adapter.insert('benchmark_test', row);
      expect(op.success).toBe(true);

      const result = await env.TEST_D1
        .prepare('SELECT name FROM benchmark_test WHERE id = 1')
        .first();
      expect((result?.name as string).length).toBe(10000);
    });

    it('should handle floating point precision', async () => {
      const preciseValue = 123.456789012345;
      const row = { id: 1, name: 'Precise', value: preciseValue, active: 1, created_at: 0 };

      const op = await adapter.insert('benchmark_test', row);
      expect(op.success).toBe(true);

      const result = await env.TEST_D1
        .prepare('SELECT value FROM benchmark_test WHERE id = 1')
        .first();
      expect(result?.value).toBeCloseTo(preciseValue, 10);
    });
  });
});
