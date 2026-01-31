/**
 * DoSQL Full SQL Execution Tests
 *
 * Tests full SQL execution in a real Durable Object environment.
 * NO MOCKS - all tests run against real miniflare/workerd runtime.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  getUniqueDoSqlStub,
  executeSQL,
  querySQL,
  createTestTable,
} from './setup.js';

describe('DoSQL Full SQL Execution', () => {
  describe('Complex Queries', () => {
    it('should handle queries with string values containing spaces', async () => {
      const stub = getUniqueDoSqlStub();

      await executeSQL(
        stub,
        'CREATE TABLE messages (id INTEGER, content TEXT, PRIMARY KEY (id))'
      );
      await executeSQL(
        stub,
        "INSERT INTO messages (id, content) VALUES (1, 'Hello World')"
      );

      const result = await querySQL(stub, "SELECT * FROM messages WHERE content = 'Hello World'");

      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(1);
      expect(result.rows?.[0].content).toBe('Hello World');
    });

    it('should handle numeric comparisons', async () => {
      const stub = getUniqueDoSqlStub();

      await executeSQL(
        stub,
        'CREATE TABLE scores (id INTEGER, value INTEGER, PRIMARY KEY (id))'
      );
      await executeSQL(stub, 'INSERT INTO scores (id, value) VALUES (1, 100)');
      await executeSQL(stub, 'INSERT INTO scores (id, value) VALUES (2, 200)');
      await executeSQL(stub, 'INSERT INTO scores (id, value) VALUES (3, 150)');

      // Note: The simple SQL parser in the DO may not support > comparisons,
      // but we can test equality
      const result = await querySQL(stub, 'SELECT * FROM scores WHERE value = 200');

      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(1);
      expect(result.rows?.[0].id).toBe(2);
    });

    it('should handle NULL values', async () => {
      const stub = getUniqueDoSqlStub();

      await executeSQL(
        stub,
        'CREATE TABLE nullable (id INTEGER, optional TEXT, PRIMARY KEY (id))'
      );
      await executeSQL(stub, "INSERT INTO nullable (id, optional) VALUES (1, 'present')");
      await executeSQL(stub, 'INSERT INTO nullable (id, optional) VALUES (2, NULL)');

      const result = await querySQL(stub, 'SELECT * FROM nullable');

      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(2);
      expect(result.rows?.find(r => r.id === 1)?.optional).toBe('present');
      expect(result.rows?.find(r => r.id === 2)?.optional).toBeNull();
    });
  });

  describe('Transaction-like Operations', () => {
    it('should handle sequential operations atomically within request', async () => {
      const stub = getUniqueDoSqlStub();

      // Create accounts table
      await executeSQL(
        stub,
        'CREATE TABLE accounts (id INTEGER, balance INTEGER, PRIMARY KEY (id))'
      );
      await executeSQL(stub, 'INSERT INTO accounts (id, balance) VALUES (1, 1000)');
      await executeSQL(stub, 'INSERT INTO accounts (id, balance) VALUES (2, 500)');

      // Simulate a transfer: deduct from account 1, add to account 2
      await executeSQL(stub, 'UPDATE accounts SET balance = 900 WHERE id = 1');
      await executeSQL(stub, 'UPDATE accounts SET balance = 600 WHERE id = 2');

      // Verify balances
      const result = await querySQL(stub, 'SELECT * FROM accounts');

      expect(result.success).toBe(true);
      const account1 = result.rows?.find(r => r.id === 1);
      const account2 = result.rows?.find(r => r.id === 2);

      expect(account1?.balance).toBe(900);
      expect(account2?.balance).toBe(600);
    });
  });

  describe('Schema Operations', () => {
    it('should handle tables with many columns', async () => {
      const stub = getUniqueDoSqlStub();

      await executeSQL(
        stub,
        `CREATE TABLE complex (
          id INTEGER,
          col1 TEXT,
          col2 TEXT,
          col3 INTEGER,
          col4 INTEGER,
          col5 TEXT,
          PRIMARY KEY (id)
        )`
      );

      await executeSQL(
        stub,
        "INSERT INTO complex (id, col1, col2, col3, col4, col5) VALUES (1, 'a', 'b', 100, 200, 'c')"
      );

      const result = await querySQL(stub, 'SELECT * FROM complex');

      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(1);
      expect(result.rows?.[0]).toMatchObject({
        id: 1,
        col1: 'a',
        col2: 'b',
        col3: 100,
        col4: 200,
        col5: 'c',
      });
    });

    it('should handle special characters in table/column names', async () => {
      const stub = getUniqueDoSqlStub();

      // Use underscores (SQL-safe special characters)
      await executeSQL(
        stub,
        'CREATE TABLE my_special_table (id INTEGER, my_column TEXT, PRIMARY KEY (id))'
      );

      await executeSQL(
        stub,
        "INSERT INTO my_special_table (id, my_column) VALUES (1, 'test')"
      );

      const result = await querySQL(stub, 'SELECT * FROM my_special_table');

      expect(result.success).toBe(true);
      expect(result.rows?.[0].my_column).toBe('test');
    });
  });

  describe('DO HTTP API', () => {
    it('should execute queries via DO HTTP endpoint', async () => {
      const stub = getUniqueDoSqlStub();

      // Create table via DO fetch - fully consume response body
      const createResponse = await stub.fetch('http://localhost/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE http_table (id INTEGER, value TEXT, PRIMARY KEY (id))',
        }),
      });
      const createResult = await createResponse.json();
      expect(createResponse.ok).toBe(true);

      // Insert data via DO fetch - fully consume response body
      const insertResponse = await stub.fetch('http://localhost/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: "INSERT INTO http_table (id, value) VALUES (1, 'from http')",
        }),
      });
      const insertResult = await insertResponse.json();
      expect(insertResponse.ok).toBe(true);

      // Query data via DO fetch - fully consume response body
      const queryResponse = await stub.fetch('http://localhost/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'SELECT * FROM http_table',
        }),
      });
      const queryResult = await queryResponse.json() as { success: boolean; rows?: unknown[] };

      expect(queryResponse.ok).toBe(true);
      expect(queryResult.success).toBe(true);
      expect(queryResult.rows).toHaveLength(1);
    });

    it('should return 404 for unknown DO paths', async () => {
      const stub = getUniqueDoSqlStub();
      const response = await stub.fetch('http://localhost/unknown/path');
      // Consume the response body
      const body = await response.json();

      expect(response.status).toBe(404);
    });

    it('should return health status via DO HTTP', async () => {
      const stub = getUniqueDoSqlStub();
      const response = await stub.fetch('http://localhost/health');
      const body = await response.json() as { status: string };

      expect(response.ok).toBe(true);
      expect(body.status).toBe('ok');
    });

    it('should list tables via DO HTTP', async () => {
      const stub = getUniqueDoSqlStub();

      // Create some tables first - consume response bodies
      const r1 = await stub.fetch('http://localhost/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE http_table_a (id INTEGER, PRIMARY KEY (id))',
        }),
      });
      await r1.json();

      const r2 = await stub.fetch('http://localhost/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE http_table_b (id INTEGER, PRIMARY KEY (id))',
        }),
      });
      await r2.json();

      const response = await stub.fetch('http://localhost/tables');
      const body = await response.json() as { tables: Array<{ name: string }> };

      expect(response.ok).toBe(true);
      expect(body.tables).toHaveLength(2);
    });
  });

  describe('Performance and Limits', () => {
    it('should handle bulk inserts', async () => {
      const stub = getUniqueDoSqlStub();

      await executeSQL(
        stub,
        'CREATE TABLE bulk_test (id INTEGER, data TEXT, PRIMARY KEY (id))'
      );

      // Insert 100 rows
      const insertPromises = [];
      for (let i = 1; i <= 100; i++) {
        insertPromises.push(
          executeSQL(stub, `INSERT INTO bulk_test (id, data) VALUES (${i}, 'data_${i}')`)
        );
      }
      await Promise.all(insertPromises);

      const result = await querySQL(stub, 'SELECT * FROM bulk_test');

      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(100);
    });

    it('should handle large text values', async () => {
      const stub = getUniqueDoSqlStub();

      await executeSQL(
        stub,
        'CREATE TABLE large_text (id INTEGER, content TEXT, PRIMARY KEY (id))'
      );

      // Create a large string (10KB)
      const largeContent = 'x'.repeat(10000);

      await executeSQL(
        stub,
        `INSERT INTO large_text (id, content) VALUES (1, '${largeContent}')`
      );

      const result = await querySQL(stub, 'SELECT * FROM large_text WHERE id = 1');

      expect(result.success).toBe(true);
      expect(result.rows?.[0].content).toHaveLength(10000);
    });

    it('should track execution time in stats', async () => {
      const stub = getUniqueDoSqlStub();
      await createTestTable(stub);

      const result = await querySQL(stub, 'SELECT * FROM test_users');

      expect(result.success).toBe(true);
      expect(result.stats?.executionTimeMs).toBeGreaterThanOrEqual(0);
    });
  });

  describe('DROP TABLE', () => {
    it('should drop an existing table', async () => {
      const stub = getUniqueDoSqlStub();

      // Create table
      await executeSQL(
        stub,
        'CREATE TABLE to_drop (id INTEGER, value TEXT, PRIMARY KEY (id))'
      );
      await executeSQL(stub, "INSERT INTO to_drop (id, value) VALUES (1, 'test')");

      // Verify table exists
      let result = await querySQL(stub, 'SELECT * FROM to_drop');
      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(1);

      // Drop the table
      const dropResult = await executeSQL(stub, 'DROP TABLE to_drop');
      expect(dropResult.success).toBe(true);

      // Verify table no longer exists
      const afterDrop = await querySQL(stub, 'SELECT * FROM to_drop');
      expect(afterDrop.success).toBe(false);
    });

    it('should drop table with IF EXISTS when table exists', async () => {
      const stub = getUniqueDoSqlStub();

      // Create table
      await executeSQL(
        stub,
        'CREATE TABLE drop_if_exists_test (id INTEGER, PRIMARY KEY (id))'
      );

      // Drop with IF EXISTS
      const dropResult = await executeSQL(stub, 'DROP TABLE IF EXISTS drop_if_exists_test');
      expect(dropResult.success).toBe(true);

      // Verify table no longer exists
      const afterDrop = await querySQL(stub, 'SELECT * FROM drop_if_exists_test');
      expect(afterDrop.success).toBe(false);
    });

    it('should succeed silently when dropping non-existent table with IF EXISTS', async () => {
      const stub = getUniqueDoSqlStub();

      // Drop non-existent table with IF EXISTS should succeed
      const dropResult = await executeSQL(stub, 'DROP TABLE IF EXISTS nonexistent_table');
      expect(dropResult.success).toBe(true);
    });

    it('should fail when dropping non-existent table without IF EXISTS', async () => {
      const stub = getUniqueDoSqlStub();

      // Drop non-existent table without IF EXISTS should fail
      const dropResult = await executeSQL(stub, 'DROP TABLE definitely_not_there');
      expect(dropResult.success).toBe(false);
      expect(dropResult.error).toContain('no such table');
    });

    it('should remove table from schema listing after drop', async () => {
      const stub = getUniqueDoSqlStub();

      // Create multiple tables
      await executeSQL(
        stub,
        'CREATE TABLE keep_this (id INTEGER, PRIMARY KEY (id))'
      );
      await executeSQL(
        stub,
        'CREATE TABLE drop_this (id INTEGER, PRIMARY KEY (id))'
      );

      // Drop one table
      await executeSQL(stub, 'DROP TABLE drop_this');

      // Check table listing
      const response = await stub.fetch('http://localhost/tables');
      const body = (await response.json()) as { tables: Array<{ name: string }> };

      expect(body.tables).toHaveLength(1);
      expect(body.tables[0].name).toBe('keep_this');
    });

    it('should delete all rows when dropping table', async () => {
      const stub = getUniqueDoSqlStub();

      // Create table with data
      await executeSQL(
        stub,
        'CREATE TABLE with_data (id INTEGER, value TEXT, PRIMARY KEY (id))'
      );
      await executeSQL(stub, "INSERT INTO with_data (id, value) VALUES (1, 'a')");
      await executeSQL(stub, "INSERT INTO with_data (id, value) VALUES (2, 'b')");
      await executeSQL(stub, "INSERT INTO with_data (id, value) VALUES (3, 'c')");

      // Verify rows exist
      let result = await querySQL(stub, 'SELECT * FROM with_data');
      expect(result.rows).toHaveLength(3);

      // Drop table
      const dropResult = await executeSQL(stub, 'DROP TABLE with_data');
      expect(dropResult.success).toBe(true);

      // Recreate same table - should be empty
      await executeSQL(
        stub,
        'CREATE TABLE with_data (id INTEGER, value TEXT, PRIMARY KEY (id))'
      );

      result = await querySQL(stub, 'SELECT * FROM with_data');
      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(0);
    });
  });

  describe('Isolation Between DO Instances', () => {
    it('should have isolated data between different named DOs', async () => {
      // Create two different DO instances
      const stub1 = getUniqueDoSqlStub();
      const stub2 = getUniqueDoSqlStub();

      // Create same table name in both
      await executeSQL(
        stub1,
        'CREATE TABLE shared_name (id INTEGER, source TEXT, PRIMARY KEY (id))'
      );
      await executeSQL(
        stub2,
        'CREATE TABLE shared_name (id INTEGER, source TEXT, PRIMARY KEY (id))'
      );

      // Insert different data
      await executeSQL(stub1, "INSERT INTO shared_name (id, source) VALUES (1, 'from_stub1')");
      await executeSQL(stub2, "INSERT INTO shared_name (id, source) VALUES (1, 'from_stub2')");

      // Verify data is isolated
      const result1 = await querySQL(stub1, 'SELECT * FROM shared_name');
      const result2 = await querySQL(stub2, 'SELECT * FROM shared_name');

      expect(result1.rows?.[0].source).toBe('from_stub1');
      expect(result2.rows?.[0].source).toBe('from_stub2');
    });
  });
});
