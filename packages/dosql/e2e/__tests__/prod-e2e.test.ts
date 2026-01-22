/**
 * DoSQL Production E2E Tests
 *
 * Full CRUD operations, transaction semantics, cold start latency,
 * connection pooling, error recovery, and concurrent request handling.
 *
 * These tests run against real Cloudflare Workers in production.
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import {
  DoSQLE2EClient,
  createTestClient,
  assert,
  assertEqual,
  assertLength,
  assertWithinTime,
  retry,
} from '../client.js';
import { getE2EEndpoint, waitForReady } from '../setup.js';

// =============================================================================
// Test Configuration
// =============================================================================

const E2E_TIMEOUT = 60_000; // 60 seconds per test
const COLD_START_THRESHOLD_MS = 5_000; // Cold start should be under 5s
const WARM_LATENCY_THRESHOLD_MS = 500; // Warm requests under 500ms

// Skip E2E tests if no endpoint is configured
const E2E_ENDPOINT = process.env.DOSQL_E2E_ENDPOINT || process.env.DOSQL_E2E_SKIP
  ? null
  : (() => {
      try {
        return getE2EEndpoint();
      } catch {
        return null;
      }
    })();

const describeE2E = E2E_ENDPOINT ? describe : describe.skip;

// =============================================================================
// Test Suite
// =============================================================================

describeE2E('DoSQL Production E2E Tests', () => {
  let baseUrl: string;
  let testRunId: string;

  beforeAll(async () => {
    baseUrl = E2E_ENDPOINT!;
    testRunId = `e2e-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;

    console.log(`[E2E] Running tests against: ${baseUrl}`);
    console.log(`[E2E] Test run ID: ${testRunId}`);

    // Wait for worker to be ready
    await waitForReady(baseUrl, { timeoutMs: 30_000 });
  }, E2E_TIMEOUT);

  // ===========================================================================
  // Health Check Tests
  // ===========================================================================

  describe('Health Checks', () => {
    it('worker health endpoint responds', async () => {
      const client = createTestClient(baseUrl, 'health');

      const response = await client.health();

      expect(response.status).toBe('ok');
      expect(response.service).toBe('dosql-test');
    }, E2E_TIMEOUT);

    it('database health endpoint responds after initialization', async () => {
      const client = createTestClient(baseUrl, 'db-health');

      await client.waitForReady(10_000);
      const response = await client.dbHealth();

      expect(response.status).toBe('ok');
      expect(response.initialized).toBe(true);
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // CRUD Operations
  // ===========================================================================

  describe('CRUD Operations', () => {
    let client: DoSQLE2EClient;

    beforeEach(async () => {
      // Create a fresh client with unique database for each test
      client = createTestClient(baseUrl, `crud-${Date.now()}`);
      await client.waitForReady();
    });

    it('CREATE TABLE creates a new table', async () => {
      const result = await client.createTable(
        'users',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'name', type: 'TEXT' },
          { name: 'email', type: 'TEXT' },
        ],
        'id'
      );

      expect(result.success).toBe(true);

      // Verify table exists
      const tables = await client.listTables();
      expect(tables.tables.some((t) => t.name === 'users')).toBe(true);
    }, E2E_TIMEOUT);

    it('INSERT adds a row to the table', async () => {
      // Setup
      await client.createTable(
        'products',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'name', type: 'TEXT' },
          { name: 'price', type: 'REAL' },
        ],
        'id'
      );

      // Insert
      const result = await client.insert('products', {
        id: 1,
        name: 'Widget',
        price: 19.99,
      });

      expect(result.success).toBe(true);
      expect(result.stats?.rowsAffected).toBe(1);
    }, E2E_TIMEOUT);

    it('SELECT retrieves inserted rows', async () => {
      // Setup
      await client.createTable(
        'items',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'value', type: 'TEXT' },
        ],
        'id'
      );

      // Insert multiple rows
      await client.insert('items', { id: 1, value: 'first' });
      await client.insert('items', { id: 2, value: 'second' });
      await client.insert('items', { id: 3, value: 'third' });

      // Select all
      const result = await client.selectAll('items');

      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(3);
    }, E2E_TIMEOUT);

    it('SELECT with WHERE filters correctly', async () => {
      // Setup
      await client.createTable(
        'orders',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'status', type: 'TEXT' },
          { name: 'total', type: 'REAL' },
        ],
        'id'
      );

      await client.insert('orders', { id: 1, status: 'pending', total: 100 });
      await client.insert('orders', { id: 2, status: 'completed', total: 200 });
      await client.insert('orders', { id: 3, status: 'pending', total: 150 });

      // Filter
      const result = await client.selectWhere('orders', { status: 'pending' });

      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(2);
    }, E2E_TIMEOUT);

    it('UPDATE modifies existing rows', async () => {
      // Setup
      await client.createTable(
        'accounts',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'balance', type: 'REAL' },
        ],
        'id'
      );

      await client.insert('accounts', { id: 1, balance: 1000 });

      // Update
      const updateResult = await client.update(
        'accounts',
        { balance: 1500 },
        { id: 1 }
      );

      expect(updateResult.success).toBe(true);
      expect(updateResult.stats?.rowsAffected).toBe(1);

      // Verify
      const selectResult = await client.selectWhere('accounts', { id: 1 });
      expect(selectResult.rows?.[0]?.balance).toBe(1500);
    }, E2E_TIMEOUT);

    it('DELETE removes rows', async () => {
      // Setup
      await client.createTable(
        'logs',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'message', type: 'TEXT' },
        ],
        'id'
      );

      await client.insert('logs', { id: 1, message: 'info' });
      await client.insert('logs', { id: 2, message: 'error' });
      await client.insert('logs', { id: 3, message: 'info' });

      // Delete
      const deleteResult = await client.delete('logs', { id: 2 });

      expect(deleteResult.success).toBe(true);
      expect(deleteResult.stats?.rowsAffected).toBe(1);

      // Verify
      const selectResult = await client.selectAll('logs');
      expect(selectResult.rows).toHaveLength(2);
    }, E2E_TIMEOUT);

    it('handles invalid SQL gracefully', async () => {
      const result = await client.query('SELECT * FORM invalid_syntax');

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    }, E2E_TIMEOUT);

    it('handles non-existent table gracefully', async () => {
      const result = await client.query('SELECT * FROM nonexistent_table');

      expect(result.success).toBe(false);
      expect(result.error).toContain('not found');
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Data Persistence
  // ===========================================================================

  describe('Data Persistence', () => {
    it('data persists across requests', async () => {
      const dbName = `persistence-${Date.now()}`;
      const client1 = new DoSQLE2EClient({ baseUrl, dbName });

      await client1.waitForReady();

      // Create and insert
      await client1.createTable(
        'persistent',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'data', type: 'TEXT' },
        ],
        'id'
      );

      await client1.insert('persistent', { id: 1, data: 'should persist' });

      // Create new client instance (simulates new connection)
      const client2 = new DoSQLE2EClient({ baseUrl, dbName });

      // Data should still be there
      const result = await client2.selectAll('persistent');

      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(1);
      expect(result.rows?.[0]?.data).toBe('should persist');
    }, E2E_TIMEOUT);

    it('schema persists across requests', async () => {
      const dbName = `schema-persistence-${Date.now()}`;
      const client1 = new DoSQLE2EClient({ baseUrl, dbName });

      await client1.waitForReady();

      // Create table
      await client1.createTable(
        'schema_test',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'field1', type: 'TEXT' },
          { name: 'field2', type: 'REAL' },
        ],
        'id'
      );

      // New client should see the table
      const client2 = new DoSQLE2EClient({ baseUrl, dbName });
      const tables = await client2.listTables();

      expect(tables.tables.some((t) => t.name === 'schema_test')).toBe(true);
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Cold Start Latency
  // ===========================================================================

  describe('Cold Start Latency', () => {
    it('cold start completes within threshold', async () => {
      const client = createTestClient(baseUrl, 'cold-start');

      // Measure cold start (first request to new database)
      const latency = await client.measureColdStart(`cold-test-${Date.now()}`, 3);

      console.log(`[E2E] Cold start latency: p50=${latency.p50.toFixed(2)}ms, p99=${latency.p99.toFixed(2)}ms`);

      // Cold start should complete within threshold
      expect(latency.p99).toBeLessThan(COLD_START_THRESHOLD_MS);
    }, E2E_TIMEOUT);

    it('warm requests are significantly faster', async () => {
      const client = createTestClient(baseUrl, 'warm-latency');

      await client.waitForReady();

      // Setup
      await client.createTable(
        'latency_test',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'value', type: 'TEXT' },
        ],
        'id'
      );

      await client.insert('latency_test', { id: 1, value: 'test' });

      // Measure warm request latency
      const latency = await client.measureLatency(
        () => client.selectAll('latency_test'),
        50
      );

      console.log(`[E2E] Warm latency: p50=${latency.p50.toFixed(2)}ms, p99=${latency.p99.toFixed(2)}ms`);

      // Warm requests should be fast
      expect(latency.p50).toBeLessThan(WARM_LATENCY_THRESHOLD_MS);
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Concurrent Request Handling
  // ===========================================================================

  describe('Concurrent Request Handling', () => {
    it('handles multiple concurrent reads', async () => {
      const client = createTestClient(baseUrl, 'concurrent-reads');

      await client.waitForReady();

      // Setup
      await client.createTable(
        'concurrent_read',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'value', type: 'TEXT' },
        ],
        'id'
      );

      for (let i = 0; i < 10; i++) {
        await client.insert('concurrent_read', { id: i, value: `value-${i}` });
      }

      // Execute concurrent reads
      const concurrentReads = Array.from({ length: 20 }, () =>
        client.selectAll('concurrent_read')
      );

      const results = await Promise.all(concurrentReads);

      // All reads should succeed
      expect(results.every((r) => r.success)).toBe(true);
      expect(results.every((r) => r.rows?.length === 10)).toBe(true);
    }, E2E_TIMEOUT);

    it('handles concurrent writes with proper serialization', async () => {
      const client = createTestClient(baseUrl, 'concurrent-writes');

      await client.waitForReady();

      // Setup
      await client.createTable(
        'counter',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'value', type: 'INTEGER' },
        ],
        'id'
      );

      await client.insert('counter', { id: 1, value: 0 });

      // Execute concurrent increments (simulated via insert)
      const writes = Array.from({ length: 10 }, (_, i) =>
        client.insert('counter', { id: i + 2, value: i + 1 })
      );

      const results = await Promise.all(writes);

      // All writes should succeed
      expect(results.every((r) => r.success)).toBe(true);

      // Verify all rows are present
      const selectResult = await client.selectAll('counter');
      expect(selectResult.rows).toHaveLength(11); // 1 initial + 10 concurrent
    }, E2E_TIMEOUT);

    it('handles mixed read/write workload', async () => {
      const client = createTestClient(baseUrl, 'mixed-workload');

      await client.waitForReady();

      // Setup
      await client.createTable(
        'mixed',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'data', type: 'TEXT' },
        ],
        'id'
      );

      for (let i = 0; i < 5; i++) {
        await client.insert('mixed', { id: i, data: `initial-${i}` });
      }

      // Mixed workload
      const operations = [
        // Reads
        client.selectAll('mixed'),
        client.selectAll('mixed'),
        client.selectWhere('mixed', { id: 1 }),
        // Writes
        client.insert('mixed', { id: 100, data: 'new-1' }),
        client.insert('mixed', { id: 101, data: 'new-2' }),
        // More reads
        client.selectAll('mixed'),
      ];

      const results = await Promise.all(operations);

      // All operations should succeed
      expect(results.every((r) => r.success)).toBe(true);
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Error Recovery
  // ===========================================================================

  describe('Error Recovery', () => {
    it('recovers from transient errors with retry', async () => {
      const client = createTestClient(baseUrl, 'retry-test');

      await client.waitForReady();

      // Setup
      await client.createTable(
        'retry_test',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'value', type: 'TEXT' },
        ],
        'id'
      );

      // Operation with retry
      const result = await retry(
        () => client.insert('retry_test', { id: 1, value: 'retried' }),
        { maxAttempts: 3 }
      );

      expect(result.success).toBe(true);
    }, E2E_TIMEOUT);

    it('maintains consistency after error recovery', async () => {
      const client = createTestClient(baseUrl, 'error-consistency');

      await client.waitForReady();

      // Setup
      await client.createTable(
        'consistency_test',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'value', type: 'INTEGER' },
        ],
        'id'
      );

      // Initial state
      await client.insert('consistency_test', { id: 1, value: 100 });

      // Attempt operations that may fail
      try {
        // This should fail (duplicate key)
        await client.insert('consistency_test', { id: 1, value: 200 });
      } catch {
        // Expected
      }

      // State should be unchanged
      const result = await client.selectWhere('consistency_test', { id: 1 });
      expect(result.rows?.[0]?.value).toBe(100);
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Connection Behavior
  // ===========================================================================

  describe('Connection Behavior', () => {
    it('handles request after idle period', async () => {
      const client = createTestClient(baseUrl, 'idle-recovery');

      await client.waitForReady();

      // Setup
      await client.createTable(
        'idle_test',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'value', type: 'TEXT' },
        ],
        'id'
      );

      await client.insert('idle_test', { id: 1, value: 'before-idle' });

      // Wait for a bit (simulating idle)
      await new Promise((resolve) => setTimeout(resolve, 2000));

      // Request should still work
      const result = await client.selectAll('idle_test');
      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(1);
    }, E2E_TIMEOUT);

    it('multiple databases are isolated', async () => {
      const db1 = `isolation-a-${Date.now()}`;
      const db2 = `isolation-b-${Date.now()}`;

      const client1 = new DoSQLE2EClient({ baseUrl, dbName: db1 });
      const client2 = new DoSQLE2EClient({ baseUrl, dbName: db2 });

      await Promise.all([client1.waitForReady(), client2.waitForReady()]);

      // Create same table in both DBs
      await client1.createTable(
        'isolated',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'db', type: 'TEXT' },
        ],
        'id'
      );

      await client2.createTable(
        'isolated',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'db', type: 'TEXT' },
        ],
        'id'
      );

      // Insert different data
      await client1.insert('isolated', { id: 1, db: 'db1' });
      await client2.insert('isolated', { id: 1, db: 'db2' });

      // Data should be isolated
      const result1 = await client1.selectAll('isolated');
      const result2 = await client2.selectAll('isolated');

      expect(result1.rows?.[0]?.db).toBe('db1');
      expect(result2.rows?.[0]?.db).toBe('db2');
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('Edge Cases', () => {
    it('handles large text values', async () => {
      const client = createTestClient(baseUrl, 'large-text');

      await client.waitForReady();

      await client.createTable(
        'large_text',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'content', type: 'TEXT' },
        ],
        'id'
      );

      // Insert large text (100KB)
      const largeText = 'x'.repeat(100 * 1024);
      const result = await client.insert('large_text', { id: 1, content: largeText });

      expect(result.success).toBe(true);

      // Retrieve and verify
      const selectResult = await client.selectAll('large_text');
      expect((selectResult.rows?.[0]?.content as string).length).toBe(largeText.length);
    }, E2E_TIMEOUT);

    it('handles special characters in text', async () => {
      const client = createTestClient(baseUrl, 'special-chars');

      await client.waitForReady();

      await client.createTable(
        'special_chars',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'text', type: 'TEXT' },
        ],
        'id'
      );

      const specialText = "Hello 'World' with \"quotes\" and \\ backslashes";
      // Note: For SQL, we need to escape single quotes
      const escapedText = specialText.replace(/'/g, "''");

      const result = await client.execute(
        `INSERT INTO special_chars (id, text) VALUES (1, '${escapedText}')`
      );

      expect(result.success).toBe(true);
    }, E2E_TIMEOUT);

    it('handles empty table correctly', async () => {
      const client = createTestClient(baseUrl, 'empty-table');

      await client.waitForReady();

      await client.createTable(
        'empty',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'value', type: 'TEXT' },
        ],
        'id'
      );

      const result = await client.selectAll('empty');

      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(0);
    }, E2E_TIMEOUT);

    it('handles NULL values', async () => {
      const client = createTestClient(baseUrl, 'null-values');

      await client.waitForReady();

      await client.createTable(
        'nullable',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'optional', type: 'TEXT' },
        ],
        'id'
      );

      const result = await client.execute(
        "INSERT INTO nullable (id, optional) VALUES (1, NULL)"
      );

      expect(result.success).toBe(true);

      const selectResult = await client.selectAll('nullable');
      expect(selectResult.rows?.[0]?.optional).toBeNull();
    }, E2E_TIMEOUT);
  });
});
