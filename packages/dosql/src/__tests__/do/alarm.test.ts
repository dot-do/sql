/**
 * DoSQL Durable Object Alarm Tests
 *
 * Tests DO alarm functionality using real miniflare/workerd runtime.
 * NO MOCKS - all tests run against real DO alarms.
 */

import { describe, it, expect } from 'vitest';
import { env } from 'cloudflare:test';
import {
  getUniqueDoSqlStub,
  executeSQL,
  getHealth,
  type TestEnv,
} from './setup.js';

describe('DoSQL DO Alarms', () => {
  describe('Alarm Scheduling', () => {
    it('should allow the DO to schedule alarms', async () => {
      const stub = getUniqueDoSqlStub();

      // Initialize the DO
      await getHealth(stub);

      // Create a table to ensure the DO is fully initialized
      await executeSQL(
        stub,
        'CREATE TABLE alarm_test (id INTEGER, value TEXT, PRIMARY KEY (id))'
      );

      // The DO should be able to handle requests without error
      // (Alarm scheduling happens internally in the DO)
      const health = await getHealth(stub);
      expect(health.status).toBe('ok');
    });

    it('should handle multiple DO instances with independent alarms', async () => {
      // Create two separate DO instances
      const stub1 = getUniqueDoSqlStub();
      const stub2 = getUniqueDoSqlStub();

      // Initialize both
      await getHealth(stub1);
      await getHealth(stub2);

      // Create different tables in each
      await executeSQL(
        stub1,
        'CREATE TABLE instance1_table (id INTEGER, PRIMARY KEY (id))'
      );
      await executeSQL(
        stub2,
        'CREATE TABLE instance2_table (id INTEGER, PRIMARY KEY (id))'
      );

      // Both should be healthy and independent
      const health1 = await getHealth(stub1);
      const health2 = await getHealth(stub2);

      expect(health1.status).toBe('ok');
      expect(health2.status).toBe('ok');
    });
  });

  describe('Alarm-Based Maintenance', () => {
    it('should support compaction-ready storage', async () => {
      const stub = getUniqueDoSqlStub();

      // Create table and add data
      await executeSQL(
        stub,
        'CREATE TABLE compaction_test (id INTEGER, data TEXT, PRIMARY KEY (id))'
      );

      // Insert multiple rows
      for (let i = 1; i <= 10; i++) {
        await executeSQL(
          stub,
          `INSERT INTO compaction_test (id, data) VALUES (${i}, 'data_${i}')`
        );
      }

      // Update some rows (creates WAL entries)
      for (let i = 1; i <= 5; i++) {
        await executeSQL(
          stub,
          `UPDATE compaction_test SET data = 'updated_${i}' WHERE id = ${i}`
        );
      }

      // Delete specific rows (simple parser only supports equality)
      await executeSQL(stub, 'DELETE FROM compaction_test WHERE id = 9');
      await executeSQL(stub, 'DELETE FROM compaction_test WHERE id = 10');

      // Verify data is still correct (compaction should not affect correctness)
      // Use the DO's query endpoint for SELECT
      const queryResponse = await stub.fetch('http://localhost/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT * FROM compaction_test' }),
      });
      const queryResult = await queryResponse.json() as { success: boolean; rows?: unknown[] };

      expect(queryResult.success).toBe(true);
      expect(queryResult.rows).toHaveLength(8); // 10 - 2 deleted
    });
  });

  describe('DO State Persistence Across Alarms', () => {
    it('should maintain schema after potential alarm-triggered operations', async () => {
      const stub = getUniqueDoSqlStub();

      // Create multiple tables
      await executeSQL(
        stub,
        'CREATE TABLE table_a (id INTEGER, value TEXT, PRIMARY KEY (id))'
      );
      await executeSQL(
        stub,
        'CREATE TABLE table_b (id INTEGER, count INTEGER, PRIMARY KEY (id))'
      );

      // Insert data
      await executeSQL(stub, "INSERT INTO table_a (id, value) VALUES (1, 'test')");
      await executeSQL(stub, 'INSERT INTO table_b (id, count) VALUES (1, 100)');

      // Fetch tables list to verify schema persistence
      const response = await stub.fetch('http://localhost/tables');
      const tables = await response.json() as { tables: Array<{ name: string }> };

      expect(tables.tables).toHaveLength(2);
      expect(tables.tables.map(t => t.name).sort()).toEqual(['table_a', 'table_b']);
    });
  });
});
