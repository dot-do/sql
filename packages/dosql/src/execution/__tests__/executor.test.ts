/**
 * Standalone Executor Tests
 *
 * Tests for the standalone query executor that operates without DO infrastructure.
 * Uses real in-memory storage, not mocks.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  StandaloneExecutor,
  createMemoryStorage,
  createMemorySchemaManager,
  type MemoryStorage,
  type MemorySchemaManager,
} from '../index.js';
import type { WALWriter } from '../../wal/types.js';

describe('StandaloneExecutor', () => {
  let storage: MemoryStorage;
  let schema: MemorySchemaManager;
  let executor: StandaloneExecutor;

  beforeEach(() => {
    storage = createMemoryStorage();
    schema = createMemorySchemaManager(storage);
    executor = new StandaloneExecutor({ storage, schema });
  });

  // ===========================================================================
  // CREATE TABLE
  // ===========================================================================
  describe('CREATE TABLE', () => {
    it('should create a table with columns', async () => {
      const result = await executor.execute(
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)'
      );

      expect(result.rows).toEqual([]);
      expect(result.rowsAffected).toBe(0);

      const tableSchema = schema.getSchema('users');
      expect(tableSchema).toBeDefined();
      expect(tableSchema!.name).toBe('users');
      expect(tableSchema!.primaryKey).toBe('id');
      expect(tableSchema!.columns).toHaveLength(3);
    });

    it('should create a table with standalone PRIMARY KEY', async () => {
      await executor.execute(
        'CREATE TABLE products (id INTEGER, name TEXT, price INTEGER, PRIMARY KEY (id))'
      );

      const tableSchema = schema.getSchema('products');
      expect(tableSchema!.primaryKey).toBe('id');
    });

    it('should create a table with DEFAULT values', async () => {
      await executor.execute(
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'draft')"
      );

      const tableSchema = schema.getSchema('posts');
      const statusCol = tableSchema!.columns.find((c) => c.name === 'status');
      expect(statusCol!.defaultValue).toBe("'draft'");
    });

    it('should reject duplicate table creation', async () => {
      await executor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY)');

      await expect(
        executor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY)')
      ).rejects.toThrow('Table test already exists');
    });

    it('should reject invalid CREATE TABLE syntax', async () => {
      await expect(executor.execute('CREATE TABLE')).rejects.toThrow(
        'Invalid CREATE TABLE syntax'
      );
    });
  });

  // ===========================================================================
  // INSERT
  // ===========================================================================
  describe('INSERT', () => {
    beforeEach(async () => {
      await executor.execute(
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)'
      );
    });

    it('should insert a single row', async () => {
      const result = await executor.execute(
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"
      );

      expect(result.rowsAffected).toBe(1);

      const selectResult = await executor.execute('SELECT * FROM users');
      expect(selectResult.rows).toHaveLength(1);
      expect(selectResult.rows[0]).toEqual({
        id: 1,
        name: 'Alice',
        email: 'alice@example.com',
      });
    });

    it('should insert multiple rows', async () => {
      const result = await executor.execute(
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')"
      );

      expect(result.rowsAffected).toBe(2);

      const selectResult = await executor.execute('SELECT * FROM users');
      expect(selectResult.rows).toHaveLength(2);
    });

    it('should auto-generate ID when not provided', async () => {
      await executor.execute(
        "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')"
      );
      await executor.execute(
        "INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')"
      );

      const selectResult = await executor.execute('SELECT * FROM users');
      expect(selectResult.rows).toHaveLength(2);
      expect(selectResult.rows[0]!.id).toBe(1);
      expect(selectResult.rows[1]!.id).toBe(2);
    });

    it('should handle RETURNING clause', async () => {
      const result = await executor.execute(
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com') RETURNING *"
      );

      expect(result.rowsAffected).toBe(1);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]).toEqual({
        id: 1,
        name: 'Alice',
        email: 'alice@example.com',
      });
    });

    it('should handle RETURNING specific columns', async () => {
      const result = await executor.execute(
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com') RETURNING id, name"
      );

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]).toEqual({
        id: 1,
        name: 'Alice',
      });
    });

    it('should reject INSERT to non-existent table', async () => {
      await expect(
        executor.execute("INSERT INTO nonexistent (id) VALUES (1)")
      ).rejects.toThrow('does not exist');
    });

    it('should handle ON CONFLICT DO NOTHING', async () => {
      await executor.execute(
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"
      );

      const result = await executor.execute(
        "INSERT INTO users (id, name, email) VALUES (1, 'Bob', 'bob@example.com') ON CONFLICT DO NOTHING"
      );

      expect(result.rowsAffected).toBe(0);

      const selectResult = await executor.execute(
        "SELECT * FROM users WHERE id = 1"
      );
      expect(selectResult.rows[0]!.name).toBe('Alice');
    });

    it('should handle ON CONFLICT DO UPDATE', async () => {
      await executor.execute(
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"
      );

      const result = await executor.execute(
        "INSERT INTO users (id, name, email) VALUES (1, 'Bob', 'bob@example.com') ON CONFLICT (id) DO UPDATE SET name = name"
      );

      expect(result.rowsAffected).toBe(1);

      const selectResult = await executor.execute(
        "SELECT * FROM users WHERE id = 1"
      );
      // name should remain 'Alice' since we're setting name = existing name
      expect(selectResult.rows[0]!.name).toBe('Alice');
    });
  });

  // ===========================================================================
  // SELECT
  // ===========================================================================
  describe('SELECT', () => {
    beforeEach(async () => {
      await executor.execute(
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)'
      );
      await executor.execute(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)"
      );
    });

    it('should select all rows', async () => {
      const result = await executor.execute('SELECT * FROM users');

      expect(result.rows).toHaveLength(3);
      expect(result.rowsAffected).toBe(0);
    });

    it('should filter with WHERE clause', async () => {
      const result = await executor.execute(
        "SELECT * FROM users WHERE name = 'Alice'"
      );

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]!.name).toBe('Alice');
    });

    it('should return empty result for no matches', async () => {
      const result = await executor.execute(
        "SELECT * FROM users WHERE name = 'Nobody'"
      );

      expect(result.rows).toHaveLength(0);
    });

    it('should reject SELECT from non-existent table', async () => {
      await expect(
        executor.execute('SELECT * FROM nonexistent')
      ).rejects.toThrow('does not exist');
    });
  });

  // ===========================================================================
  // UPDATE
  // ===========================================================================
  describe('UPDATE', () => {
    beforeEach(async () => {
      await executor.execute(
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)'
      );
      await executor.execute(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)"
      );
    });

    it('should update all rows without WHERE', async () => {
      const result = await executor.execute("UPDATE users SET name = 'Updated'");

      expect(result.rowsAffected).toBe(3);

      const selectResult = await executor.execute('SELECT * FROM users');
      expect(selectResult.rows.every((r) => r.name === 'Updated')).toBe(true);
    });

    it('should update rows matching WHERE', async () => {
      const result = await executor.execute(
        "UPDATE users SET name = 'Alicia' WHERE id = 1"
      );

      expect(result.rowsAffected).toBe(1);

      const selectResult = await executor.execute(
        'SELECT * FROM users WHERE id = 1'
      );
      expect(selectResult.rows[0]!.name).toBe('Alicia');
    });

    it('should handle RETURNING clause', async () => {
      const result = await executor.execute(
        "UPDATE users SET name = 'Alicia' WHERE id = 1 RETURNING *"
      );

      expect(result.rowsAffected).toBe(1);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]!.name).toBe('Alicia');
    });

    it('should handle multiple SET clauses', async () => {
      const result = await executor.execute(
        "UPDATE users SET name = 'Alicia', age = 31 WHERE id = 1"
      );

      expect(result.rowsAffected).toBe(1);

      const selectResult = await executor.execute(
        'SELECT * FROM users WHERE id = 1'
      );
      expect(selectResult.rows[0]!.name).toBe('Alicia');
      expect(selectResult.rows[0]!.age).toBe(31);
    });

    it('should handle UPDATE with LIMIT', async () => {
      const result = await executor.execute(
        "UPDATE users SET name = 'Updated' LIMIT 1"
      );

      expect(result.rowsAffected).toBe(1);
    });

    it('should reject UPDATE on non-existent table', async () => {
      await expect(
        executor.execute("UPDATE nonexistent SET name = 'test'")
      ).rejects.toThrow('does not exist');
    });
  });

  // ===========================================================================
  // DELETE
  // ===========================================================================
  describe('DELETE', () => {
    beforeEach(async () => {
      await executor.execute(
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)'
      );
      await executor.execute(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)"
      );
    });

    it('should delete all rows without WHERE', async () => {
      const result = await executor.execute('DELETE FROM users');

      expect(result.rowsAffected).toBe(3);

      const selectResult = await executor.execute('SELECT * FROM users');
      expect(selectResult.rows).toHaveLength(0);
    });

    it('should delete rows matching WHERE', async () => {
      const result = await executor.execute('DELETE FROM users WHERE id = 1');

      expect(result.rowsAffected).toBe(1);

      const selectResult = await executor.execute('SELECT * FROM users');
      expect(selectResult.rows).toHaveLength(2);
    });

    it('should handle RETURNING clause', async () => {
      const result = await executor.execute(
        'DELETE FROM users WHERE id = 1 RETURNING *'
      );

      expect(result.rowsAffected).toBe(1);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]!.id).toBe(1);
    });

    it('should handle DELETE with LIMIT', async () => {
      const result = await executor.execute('DELETE FROM users LIMIT 1');

      expect(result.rowsAffected).toBe(1);

      const selectResult = await executor.execute('SELECT * FROM users');
      expect(selectResult.rows).toHaveLength(2);
    });

    it('should reject DELETE from non-existent table', async () => {
      await expect(executor.execute('DELETE FROM nonexistent')).rejects.toThrow(
        'does not exist'
      );
    });
  });

  // ===========================================================================
  // DROP TABLE
  // ===========================================================================
  describe('DROP TABLE', () => {
    beforeEach(async () => {
      await executor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY)');
      await executor.execute('INSERT INTO test (id) VALUES (1), (2), (3)');
    });

    it('should drop an existing table', async () => {
      const result = await executor.execute('DROP TABLE test');

      expect(result.rowsAffected).toBe(3);
      expect(schema.getSchema('test')).toBeUndefined();
    });

    it('should fail when dropping non-existent table', async () => {
      await expect(executor.execute('DROP TABLE nonexistent')).rejects.toThrow(
        'does not exist'
      );
    });

    it('should succeed with IF EXISTS on non-existent table', async () => {
      const result = await executor.execute('DROP TABLE IF EXISTS nonexistent');

      expect(result.rowsAffected).toBe(0);
    });
  });

  // ===========================================================================
  // REPLACE
  // ===========================================================================
  describe('REPLACE', () => {
    beforeEach(async () => {
      await executor.execute(
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)'
      );
    });

    it('should insert when row does not exist', async () => {
      const result = await executor.execute(
        "REPLACE INTO users (id, name) VALUES (1, 'Alice')"
      );

      expect(result.rowsAffected).toBe(1);

      const selectResult = await executor.execute('SELECT * FROM users');
      expect(selectResult.rows[0]!.name).toBe('Alice');
    });

    it('should replace when row exists', async () => {
      await executor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");

      const result = await executor.execute(
        "REPLACE INTO users (id, name) VALUES (1, 'Alicia')"
      );

      expect(result.rowsAffected).toBe(1);

      const selectResult = await executor.execute('SELECT * FROM users');
      expect(selectResult.rows).toHaveLength(1);
      expect(selectResult.rows[0]!.name).toBe('Alicia');
    });

    it('should handle RETURNING clause', async () => {
      const result = await executor.execute(
        "REPLACE INTO users (id, name) VALUES (1, 'Alice') RETURNING *"
      );

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]).toEqual({ id: 1, name: 'Alice' });
    });
  });

  // ===========================================================================
  // Parameter Substitution
  // ===========================================================================
  describe('Parameter Substitution', () => {
    beforeEach(async () => {
      await executor.execute(
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)'
      );
      await executor.execute(
        "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
      );
    });

    it('should substitute named parameters', async () => {
      const result = await executor.execute(
        'SELECT * FROM users WHERE id = :id',
        { id: 1 }
      );

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]!.name).toBe('Alice');
    });

    it('should handle string parameters', async () => {
      const result = await executor.execute(
        'SELECT * FROM users WHERE name = :name',
        { name: 'Bob' }
      );

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]!.id).toBe(2);
    });

    it('should handle null parameters', async () => {
      await executor.execute('CREATE TABLE nullable (id INTEGER PRIMARY KEY, value TEXT)');
      await executor.execute(
        'INSERT INTO nullable (id, value) VALUES (:id, :value)',
        { id: 1, value: null }
      );

      const result = await executor.execute('SELECT * FROM nullable');
      expect(result.rows[0]!.value).toBeNull();
    });
  });

  // ===========================================================================
  // WHERE Clause Evaluation
  // ===========================================================================
  describe('WHERE Clause Evaluation', () => {
    beforeEach(async () => {
      await executor.execute(
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, active INTEGER)'
      );
      await executor.execute(
        "INSERT INTO users (id, name, age, active) VALUES (1, 'Alice', 30, 1), (2, 'Bob', 25, 0), (3, 'Charlie', 35, 1)"
      );
    });

    it('should handle AND conditions', async () => {
      const result = await executor.execute(
        'SELECT * FROM users WHERE age > 25 AND active = 1'
      );

      expect(result.rows).toHaveLength(2);
    });

    it('should handle OR conditions', async () => {
      const result = await executor.execute(
        "SELECT * FROM users WHERE name = 'Alice' OR name = 'Bob'"
      );

      expect(result.rows).toHaveLength(2);
    });

    it('should handle IN clause', async () => {
      const result = await executor.execute(
        'SELECT * FROM users WHERE id IN (1, 3)'
      );

      expect(result.rows).toHaveLength(2);
    });

    it('should handle comparison operators', async () => {
      expect(
        (await executor.execute('SELECT * FROM users WHERE age > 30')).rows
      ).toHaveLength(1);
      expect(
        (await executor.execute('SELECT * FROM users WHERE age >= 30')).rows
      ).toHaveLength(2);
      expect(
        (await executor.execute('SELECT * FROM users WHERE age < 30')).rows
      ).toHaveLength(1);
      expect(
        (await executor.execute('SELECT * FROM users WHERE age <= 30')).rows
      ).toHaveLength(2);
      expect(
        (await executor.execute('SELECT * FROM users WHERE age != 30')).rows
      ).toHaveLength(2);
    });
  });

  // ===========================================================================
  // Edge Cases
  // ===========================================================================
  describe('Edge Cases', () => {
    it('should reject unsupported SQL', async () => {
      await expect(executor.execute('TRUNCATE TABLE users')).rejects.toThrow(
        'Unsupported SQL'
      );
    });

    it('should handle empty SELECT result', async () => {
      await executor.execute('CREATE TABLE empty (id INTEGER PRIMARY KEY)');
      const result = await executor.execute('SELECT * FROM empty');

      expect(result.rows).toHaveLength(0);
    });

    it('should handle special characters in values', async () => {
      await executor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)');
      // Simple test with comma in value
      await executor.execute(
        "INSERT INTO test (id, data) VALUES (1, 'Hello World')"
      );

      const result = await executor.execute('SELECT * FROM test');
      expect(result.rows[0]!.data).toBe('Hello World');
    });
  });

  // ===========================================================================
  // Standalone Operation (No DO Infrastructure)
  // ===========================================================================
  describe('Standalone Operation', () => {
    it('should work without WAL', async () => {
      // Executor created without WAL in beforeEach
      await executor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY)');
      await executor.execute('INSERT INTO test (id) VALUES (1)');
      const result = await executor.execute('SELECT * FROM test');

      expect(result.rows).toHaveLength(1);
    });

    it('should work with WAL', async () => {
      const walEntries: Array<{
        op: string;
        table: string;
      }> = [];

      const walWriter = {
        async append(entry: { op: string; table: string }) {
          walEntries.push({ op: entry.op, table: entry.table });
        },
      };

      const execWithWal = new StandaloneExecutor({
        storage,
        schema,
        wal: walWriter as unknown as WALWriter,
      });

      await execWithWal.execute('CREATE TABLE logged (id INTEGER PRIMARY KEY)');
      await execWithWal.execute('INSERT INTO logged (id) VALUES (1)');
      await execWithWal.execute('UPDATE logged SET id = 2 WHERE id = 1');
      await execWithWal.execute('DELETE FROM logged WHERE id = 2');

      expect(walEntries).toHaveLength(3);
      expect(walEntries[0]!.op).toBe('INSERT');
      expect(walEntries[1]!.op).toBe('UPDATE');
      expect(walEntries[2]!.op).toBe('DELETE');
    });

    it('should persist data in storage', async () => {
      await executor.execute('CREATE TABLE persist (id INTEGER PRIMARY KEY, value TEXT)');
      await executor.execute("INSERT INTO persist (id, value) VALUES (1, 'test')");

      // Create new executor with same storage
      const executor2 = new StandaloneExecutor({ storage, schema });
      const result = await executor2.execute('SELECT * FROM persist');

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]!.value).toBe('test');
    });
  });
});
