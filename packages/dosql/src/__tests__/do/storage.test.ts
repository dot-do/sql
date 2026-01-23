/**
 * DoSQL Durable Object Storage Tests
 *
 * Tests DO storage persistence using real miniflare/workerd runtime.
 * NO MOCKS - all tests run against real DO storage.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  getUniqueDoSqlStub,
  executeSQL,
  querySQL,
  getHealth,
  listTables,
  createTestTable,
} from './setup.js';

describe('DoSQL DO Storage', () => {
  describe('Health Check', () => {
    it('should return healthy status', async () => {
      const stub = getUniqueDoSqlStub();
      const health = await getHealth(stub);

      expect(health.status).toBe('ok');
    });

    it('should initialize on first request', async () => {
      const stub = getUniqueDoSqlStub();
      const health = await getHealth(stub);

      expect(health.initialized).toBe(true);
    });
  });

  describe('CREATE TABLE', () => {
    it('should create a table successfully', async () => {
      const stub = getUniqueDoSqlStub();

      const result = await executeSQL(
        stub,
        'CREATE TABLE users (id INTEGER, name TEXT, email TEXT, PRIMARY KEY (id))'
      );

      expect(result.success).toBe(true);
    });

    it('should persist table schema', async () => {
      const stub = getUniqueDoSqlStub();

      await executeSQL(
        stub,
        'CREATE TABLE products (id INTEGER, name TEXT, price INTEGER, PRIMARY KEY (id))'
      );

      const tables = await listTables(stub);

      expect(tables.tables).toHaveLength(1);
      expect(tables.tables[0].name).toBe('products');
      expect(tables.tables[0].primaryKey).toBe('id');
      expect(tables.tables[0].columns).toContainEqual({ name: 'id', type: 'INTEGER' });
      expect(tables.tables[0].columns).toContainEqual({ name: 'name', type: 'TEXT' });
      expect(tables.tables[0].columns).toContainEqual({ name: 'price', type: 'INTEGER' });
    });

    it('should create multiple tables', async () => {
      const stub = getUniqueDoSqlStub();

      await executeSQL(
        stub,
        'CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))'
      );
      await executeSQL(
        stub,
        'CREATE TABLE orders (id INTEGER, user_id INTEGER, total INTEGER, PRIMARY KEY (id))'
      );

      const tables = await listTables(stub);

      expect(tables.tables).toHaveLength(2);
      expect(tables.tables.map(t => t.name).sort()).toEqual(['orders', 'users']);
    });
  });

  describe('INSERT', () => {
    it('should insert a row successfully', async () => {
      const stub = getUniqueDoSqlStub();

      await executeSQL(
        stub,
        'CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))'
      );

      const result = await executeSQL(
        stub,
        "INSERT INTO users (id, name) VALUES (1, 'Alice')"
      );

      expect(result.success).toBe(true);
      expect(result.stats?.rowsAffected).toBe(1);
    });

    it('should insert multiple rows', async () => {
      const stub = getUniqueDoSqlStub();

      await executeSQL(
        stub,
        'CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))'
      );

      await executeSQL(stub, "INSERT INTO users (id, name) VALUES (1, 'Alice')");
      await executeSQL(stub, "INSERT INTO users (id, name) VALUES (2, 'Bob')");
      await executeSQL(stub, "INSERT INTO users (id, name) VALUES (3, 'Charlie')");

      const result = await querySQL(stub, 'SELECT * FROM users');

      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(3);
    });

    it('should auto-generate primary key when not provided', async () => {
      const stub = getUniqueDoSqlStub();

      await executeSQL(
        stub,
        'CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))'
      );

      // Insert without providing primary key - should auto-generate one
      const result = await executeSQL(
        stub,
        "INSERT INTO users (name) VALUES ('Alice')"
      );

      expect(result.success).toBe(true);
      expect(result.stats?.rowsAffected).toBe(1);

      // Verify the row was inserted with an auto-generated ID
      const selectResult = await querySQL(stub, 'SELECT * FROM users');
      expect(selectResult.rows).toHaveLength(1);
      expect(selectResult.rows?.[0].name).toBe('Alice');
      expect(selectResult.rows?.[0].id).toBeDefined();
    });

    it('should fail to insert into non-existent table', async () => {
      const stub = getUniqueDoSqlStub();

      const result = await executeSQL(
        stub,
        "INSERT INTO nonexistent (id, name) VALUES (1, 'Alice')"
      );

      expect(result.success).toBe(false);
      expect(result.error).toContain('not found');
    });
  });

  describe('SELECT', () => {
    it('should select all rows', async () => {
      const stub = getUniqueDoSqlStub();
      await createTestTable(stub, 'users');

      const result = await querySQL(stub, 'SELECT * FROM users');

      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(3);
    });

    it('should filter rows with WHERE clause', async () => {
      const stub = getUniqueDoSqlStub();
      await createTestTable(stub, 'users');

      const result = await querySQL(stub, "SELECT * FROM users WHERE name = 'Alice'");

      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(1);
      expect(result.rows?.[0].name).toBe('Alice');
    });

    it('should return empty result for no matches', async () => {
      const stub = getUniqueDoSqlStub();
      await createTestTable(stub, 'users');

      const result = await querySQL(stub, "SELECT * FROM users WHERE name = 'Nobody'");

      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(0);
    });

    it('should fail to select from non-existent table', async () => {
      const stub = getUniqueDoSqlStub();

      const result = await querySQL(stub, 'SELECT * FROM nonexistent');

      expect(result.success).toBe(false);
      expect(result.error).toContain('not found');
    });
  });

  describe('UPDATE', () => {
    it('should update a row', async () => {
      const stub = getUniqueDoSqlStub();
      await createTestTable(stub, 'users');

      const updateResult = await executeSQL(
        stub,
        "UPDATE users SET email = 'alice.new@example.com' WHERE name = 'Alice'"
      );

      expect(updateResult.success).toBe(true);
      expect(updateResult.stats?.rowsAffected).toBe(1);

      // Verify the update
      const selectResult = await querySQL(stub, "SELECT * FROM users WHERE name = 'Alice'");
      expect(selectResult.rows?.[0].email).toBe('alice.new@example.com');
    });

    it('should update multiple rows', async () => {
      const stub = getUniqueDoSqlStub();

      await executeSQL(
        stub,
        'CREATE TABLE products (id INTEGER, name TEXT, status TEXT, PRIMARY KEY (id))'
      );
      await executeSQL(stub, "INSERT INTO products (id, name, status) VALUES (1, 'A', 'draft')");
      await executeSQL(stub, "INSERT INTO products (id, name, status) VALUES (2, 'B', 'draft')");
      await executeSQL(stub, "INSERT INTO products (id, name, status) VALUES (3, 'C', 'active')");

      const updateResult = await executeSQL(
        stub,
        "UPDATE products SET status = 'archived' WHERE status = 'draft'"
      );

      expect(updateResult.success).toBe(true);
      expect(updateResult.stats?.rowsAffected).toBe(2);
    });

    it('should handle update with no matches', async () => {
      const stub = getUniqueDoSqlStub();
      await createTestTable(stub, 'users');

      const result = await executeSQL(
        stub,
        "UPDATE users SET email = 'new@example.com' WHERE name = 'Nobody'"
      );

      expect(result.success).toBe(true);
      expect(result.stats?.rowsAffected).toBe(0);
    });
  });

  describe('DELETE', () => {
    it('should delete a row', async () => {
      const stub = getUniqueDoSqlStub();
      await createTestTable(stub, 'users');

      const deleteResult = await executeSQL(
        stub,
        "DELETE FROM users WHERE name = 'Alice'"
      );

      expect(deleteResult.success).toBe(true);
      expect(deleteResult.stats?.rowsAffected).toBe(1);

      // Verify the delete
      const selectResult = await querySQL(stub, 'SELECT * FROM users');
      expect(selectResult.rows).toHaveLength(2);
      expect(selectResult.rows?.find(r => r.name === 'Alice')).toBeUndefined();
    });

    it('should delete multiple rows', async () => {
      const stub = getUniqueDoSqlStub();

      await executeSQL(
        stub,
        'CREATE TABLE items (id INTEGER, category TEXT, PRIMARY KEY (id))'
      );
      await executeSQL(stub, "INSERT INTO items (id, category) VALUES (1, 'A')");
      await executeSQL(stub, "INSERT INTO items (id, category) VALUES (2, 'A')");
      await executeSQL(stub, "INSERT INTO items (id, category) VALUES (3, 'B')");

      const deleteResult = await executeSQL(
        stub,
        "DELETE FROM items WHERE category = 'A'"
      );

      expect(deleteResult.success).toBe(true);
      expect(deleteResult.stats?.rowsAffected).toBe(2);

      const selectResult = await querySQL(stub, 'SELECT * FROM items');
      expect(selectResult.rows).toHaveLength(1);
    });

    it('should handle delete with no matches', async () => {
      const stub = getUniqueDoSqlStub();
      await createTestTable(stub, 'users');

      const result = await executeSQL(
        stub,
        "DELETE FROM users WHERE name = 'Nobody'"
      );

      expect(result.success).toBe(true);
      expect(result.stats?.rowsAffected).toBe(0);
    });
  });

  describe('Data Persistence', () => {
    it('should persist data across multiple requests', async () => {
      const stub = getUniqueDoSqlStub();

      // First request: create and insert
      await executeSQL(
        stub,
        'CREATE TABLE persistent (id INTEGER, value TEXT, PRIMARY KEY (id))'
      );
      await executeSQL(
        stub,
        "INSERT INTO persistent (id, value) VALUES (1, 'test')"
      );

      // Second request: verify data exists
      const result = await querySQL(stub, 'SELECT * FROM persistent');

      expect(result.rows).toHaveLength(1);
      expect(result.rows?.[0].value).toBe('test');
    });

    it('should maintain data integrity after update', async () => {
      const stub = getUniqueDoSqlStub();
      await createTestTable(stub, 'users');

      // Update
      await executeSQL(
        stub,
        "UPDATE users SET email = 'updated@example.com' WHERE id = 1"
      );

      // Verify other rows unchanged
      const result = await querySQL(stub, 'SELECT * FROM users ORDER BY id');

      expect(result.rows).toHaveLength(3);
      expect(result.rows?.[0].email).toBe('updated@example.com');
      expect(result.rows?.[1].email).toBe('bob@example.com');
      expect(result.rows?.[2].email).toBe('charlie@example.com');
    });
  });

  describe('Error Handling', () => {
    it('should return error for unsupported SQL', async () => {
      const stub = getUniqueDoSqlStub();

      const result = await executeSQL(stub, 'DROP TABLE users');

      expect(result.success).toBe(false);
      expect(result.error).toContain('Unsupported');
    });

    it('should return error for invalid syntax', async () => {
      const stub = getUniqueDoSqlStub();

      const result = await executeSQL(stub, 'INVALID SQL SYNTAX HERE');

      expect(result.success).toBe(false);
    });
  });
});
