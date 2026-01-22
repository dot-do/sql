/**
 * DoSQL Knex Integration Tests
 *
 * Tests the Knex query builder integration with DoSQL backend.
 * This spike demonstrates using Knex's SQL generation with DoSQL's execution engine.
 *
 * Architecture:
 * - Knex generates SQL and bindings using its fluent API
 * - DoSQL backend executes the generated SQL
 * - Results are returned in Knex-compatible format
 */

import { describe, it, expect, beforeEach } from 'vitest';
import knex, { type Knex } from 'knex';
import { createInMemoryBackend, type DoSQLBackend } from '../index.js';

// =============================================================================
// TEST SETUP
// =============================================================================

describe('DoSQL Knex Integration', () => {
  let backend: DoSQLBackend;
  let knexBuilder: Knex;

  beforeEach(async () => {
    // Create fresh in-memory backend for each test
    backend = createInMemoryBackend();

    // Create Knex instance for SQL generation only (no actual connection)
    // This uses Knex as a query builder while DoSQL handles execution
    knexBuilder = knex({
      client: 'better-sqlite3',
      connection: ':memory:',
      useNullAsDefault: true,
      pool: { min: 0, max: 0 }, // Disable pool - we use DoSQL backend
    });

    // Create test tables
    await backend.exec(`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, active INTEGER DEFAULT 1)`);
    await backend.exec(`CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, status TEXT DEFAULT 'pending')`);
    await backend.exec(`CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)`);
  });

  // ===========================================================================
  // HELPER: Execute Knex-generated SQL through DoSQL
  // ===========================================================================

  async function executeKnex<T>(builder: Knex.QueryBuilder | Knex.Raw): Promise<T[]> {
    const compiled = builder.toSQL();
    const result = await backend.query(compiled.sql, compiled.bindings as any[]);
    return result.rows as T[];
  }

  async function executeKnexFirst<T>(builder: Knex.QueryBuilder): Promise<T | undefined> {
    const rows = await executeKnex<T>(builder);
    return rows[0];
  }

  async function executeKnexModify(builder: Knex.QueryBuilder | Knex.Raw): Promise<number> {
    const compiled = builder.toSQL();
    const result = await backend.exec(compiled.sql, compiled.bindings as any[]);
    return result.rowsAffected ?? 0;
  }

  // ===========================================================================
  // QUERY BUILDING TESTS
  // ===========================================================================

  describe('Query Building with Knex -> DoSQL Execution', () => {
    it('should generate and execute simple SELECT', async () => {
      // Seed data
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [1, 'Alice', 'alice@example.com', 25]);

      // Build query with Knex
      const query = knexBuilder('users').select('*');

      // Execute through DoSQL
      const result = await executeKnex(query);

      expect(result).toHaveLength(1);
      expect(result[0]).toMatchObject({
        id: 1,
        name: 'Alice',
        email: 'alice@example.com',
        age: 25,
      });
    });

    it('should generate SELECT with specific columns', async () => {
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [1, 'Bob', 'bob@example.com', 30]);

      const query = knexBuilder('users').select('name', 'email');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('select');
      expect(compiled.sql).toContain('name');
      expect(compiled.sql).toContain('email');

      const result = await executeKnex(query);
      expect(result[0]).toHaveProperty('name', 'Bob');
      expect(result[0]).toHaveProperty('email', 'bob@example.com');
    });

    it('should generate WHERE clause with equality', async () => {
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [1, 'Alice', 'alice@example.com', 25]);
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [2, 'Bob', 'bob@example.com', 30]);

      const query = knexBuilder('users').where('name', 'Alice').select('*');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('where');
      expect(compiled.bindings).toContain('Alice');

      const result = await executeKnex(query);
      expect(result).toHaveLength(1);
      expect(result[0]).toHaveProperty('name', 'Alice');
    });

    it('should generate WHERE clause with comparison operators', async () => {
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [1, 'Alice', 'alice@example.com', 25]);
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [2, 'Bob', 'bob@example.com', 30]);
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [3, 'Carol', 'carol@example.com', 35]);

      const query = knexBuilder('users').where('age', '>', 27).select('*');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('>');
      expect(compiled.bindings).toContain(27);

      const result = await executeKnex(query);
      expect(result).toHaveLength(2);
    });

    it('should generate multiple WHERE conditions', async () => {
      await backend.exec('INSERT INTO users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)', [1, 'Alice', 'alice@example.com', 25, 1]);
      await backend.exec('INSERT INTO users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)', [2, 'Bob', 'bob@example.com', 30, 0]);
      await backend.exec('INSERT INTO users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)', [3, 'Carol', 'carol@example.com', 35, 1]);

      const query = knexBuilder('users')
        .where('age', '>', 20)
        .where('active', 1)
        .select('*');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('and');
      expect(compiled.bindings).toContain(20);
      expect(compiled.bindings).toContain(1);
    });

    it('should generate ORDER BY clause', async () => {
      const query = knexBuilder('users').orderBy('name', 'asc').select('*');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('order by');
      expect(compiled.sql).toContain('asc');
    });

    it('should generate LIMIT and OFFSET clauses', async () => {
      const query = knexBuilder('users').limit(10).offset(5).select('*');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('limit');
      expect(compiled.sql).toContain('offset');
    });
  });

  // ===========================================================================
  // SELECT TESTS
  // ===========================================================================

  describe('SELECT Operations', () => {
    beforeEach(async () => {
      await backend.exec('INSERT INTO users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)', [1, 'Alice', 'alice@example.com', 25, 1]);
      await backend.exec('INSERT INTO users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)', [2, 'Bob', 'bob@example.com', 30, 1]);
      await backend.exec('INSERT INTO users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)', [3, 'Carol', 'carol@example.com', 35, 0]);
    });

    it('should select all records', async () => {
      const query = knexBuilder('users').select('*');
      const result = await executeKnex(query);

      expect(result).toHaveLength(3);
    });

    it('should select first record', async () => {
      const query = knexBuilder('users').select('*');
      const result = await executeKnexFirst(query);

      expect(result).toBeDefined();
      expect(result).toHaveProperty('id');
      expect(result).toHaveProperty('name');
    });

    it('should return undefined for empty result', async () => {
      const query = knexBuilder('users').where('id', 999).select('*');
      const result = await executeKnexFirst(query);

      expect(result).toBeUndefined();
    });

    it('should generate whereIn clause', async () => {
      const query = knexBuilder('users').whereIn('id', [1, 3]).select('*');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('in');
      expect(compiled.bindings).toEqual(expect.arrayContaining([1, 3]));
    });

    it('should generate whereNull clause', async () => {
      const query = knexBuilder('users').whereNull('email').select('*');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('is null');
    });

    it('should generate whereBetween clause', async () => {
      const query = knexBuilder('users').whereBetween('age', [25, 32]).select('*');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('between');
      expect(compiled.bindings).toEqual(expect.arrayContaining([25, 32]));
    });
  });

  // ===========================================================================
  // INSERT TESTS
  // ===========================================================================

  describe('INSERT Operations', () => {
    it('should generate INSERT statement', async () => {
      const query = knexBuilder('users').insert({
        name: 'David',
        email: 'david@example.com',
        age: 40,
      });
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('insert into');
      expect(compiled.sql).toContain('users');
      expect(compiled.bindings).toContain('David');
      expect(compiled.bindings).toContain('david@example.com');
      expect(compiled.bindings).toContain(40);
    });

    it('should generate INSERT with multiple rows', async () => {
      const query = knexBuilder('users').insert([
        { name: 'Eve', email: 'eve@example.com', age: 28 },
        { name: 'Frank', email: 'frank@example.com', age: 32 },
      ]);
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('insert into');
      expect(compiled.bindings).toContain('Eve');
      expect(compiled.bindings).toContain('Frank');
    });

    it('should execute INSERT and retrieve data', async () => {
      const insertQuery = knexBuilder('users').insert({
        name: 'Grace',
        email: 'grace@example.com',
        age: 29,
      });

      await executeKnexModify(insertQuery);

      // Verify insertion
      const selectQuery = knexBuilder('users').where('name', 'Grace').select('*');
      const result = await executeKnex(selectQuery);

      expect(result).toHaveLength(1);
      expect(result[0]).toHaveProperty('name', 'Grace');
    });
  });

  // ===========================================================================
  // UPDATE TESTS
  // ===========================================================================

  describe('UPDATE Operations', () => {
    beforeEach(async () => {
      await backend.exec('INSERT INTO users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)', [1, 'Alice', 'alice@example.com', 25, 1]);
      await backend.exec('INSERT INTO users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)', [2, 'Bob', 'bob@example.com', 30, 1]);
    });

    it('should generate UPDATE statement', async () => {
      const query = knexBuilder('users')
        .where('id', 1)
        .update({ name: 'Alice Updated' });
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('update');
      expect(compiled.sql).toContain('set');
      expect(compiled.bindings).toContain('Alice Updated');
      expect(compiled.bindings).toContain(1);
    });

    it('should generate UPDATE with multiple fields', async () => {
      const query = knexBuilder('users')
        .where('id', 1)
        .update({
          name: 'Alice Smith',
          age: 26,
          active: 0,
        });
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('update');
      expect(compiled.bindings).toContain('Alice Smith');
      expect(compiled.bindings).toContain(26);
    });

    it('should execute UPDATE and verify change', async () => {
      const updateQuery = knexBuilder('users')
        .where('id', 1)
        .update({ name: 'AliceUpdated' });

      await executeKnexModify(updateQuery);

      // Verify update
      const selectQuery = knexBuilder('users').where('id', 1).select('*');
      const result = await executeKnex(selectQuery);

      expect(result[0]).toHaveProperty('name', 'AliceUpdated');
    });
  });

  // ===========================================================================
  // DELETE TESTS
  // ===========================================================================

  describe('DELETE Operations', () => {
    beforeEach(async () => {
      await backend.exec('INSERT INTO users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)', [1, 'Alice', 'alice@example.com', 25, 1]);
      await backend.exec('INSERT INTO users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)', [2, 'Bob', 'bob@example.com', 30, 1]);
      await backend.exec('INSERT INTO users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)', [3, 'Carol', 'carol@example.com', 35, 0]);
    });

    it('should generate DELETE statement', async () => {
      const query = knexBuilder('users').where('id', 1).delete();
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('delete from');
      expect(compiled.sql).toContain('users');
      expect(compiled.bindings).toContain(1);
    });

    it('should execute DELETE and verify removal', async () => {
      const deleteQuery = knexBuilder('users').where('id', 1).delete();
      await executeKnexModify(deleteQuery);

      // Verify deletion
      const selectQuery = knexBuilder('users').where('id', 1).select('*');
      const result = await executeKnex(selectQuery);

      expect(result).toHaveLength(0);
    });

    it('should generate DELETE with conditions', async () => {
      const query = knexBuilder('users').where('active', 0).delete();
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('where');
      expect(compiled.bindings).toContain(0);
    });
  });

  // ===========================================================================
  // JOIN TESTS
  // ===========================================================================

  describe('JOIN Operations', () => {
    beforeEach(async () => {
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [1, 'Alice', 'alice@example.com', 25]);
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [2, 'Bob', 'bob@example.com', 30]);
      await backend.exec('INSERT INTO orders (id, user_id, total, status) VALUES (?, ?, ?, ?)', [1, 1, 100.0, 'completed']);
      await backend.exec('INSERT INTO orders (id, user_id, total, status) VALUES (?, ?, ?, ?)', [2, 1, 50.0, 'pending']);
      await backend.exec('INSERT INTO orders (id, user_id, total, status) VALUES (?, ?, ?, ?)', [3, 2, 200.0, 'completed']);
    });

    it('should generate INNER JOIN', async () => {
      const query = knexBuilder('orders')
        .join('users', 'orders.user_id', 'users.id')
        .select('orders.*', 'users.name');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('inner join');
      expect(compiled.sql).toContain('user_id');
      expect(compiled.sql).toContain('users');
    });

    it('should generate LEFT JOIN', async () => {
      const query = knexBuilder('users')
        .leftJoin('orders', 'users.id', 'orders.user_id')
        .select('users.name', 'orders.total');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('left join');
    });

    it('should generate multiple JOINs', async () => {
      const query = knexBuilder('orders')
        .join('users', 'orders.user_id', 'users.id')
        .leftJoin('products', 'orders.id', 'products.id')
        .select('orders.id', 'users.name');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('inner join');
      expect(compiled.sql).toContain('left join');
    });
  });

  // ===========================================================================
  // TRANSACTION TESTS
  // ===========================================================================

  describe('Transaction Operations', () => {
    it('should support transaction begin/commit via backend', async () => {
      const txnId = await backend.beginTransaction!();
      expect(txnId).toBeDefined();

      // Insert within transaction
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [1, 'TxnUser', 'txn@example.com', 28]);

      await backend.commit!(txnId);

      // Verify persisted
      const result = await backend.query('SELECT * FROM users WHERE name = ?', ['TxnUser']);
      expect(result.rows).toHaveLength(1);
    });

    it('should support transaction rollback via backend', async () => {
      const txnId = await backend.beginTransaction!();

      // Insert within transaction
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [1, 'WillRollback', 'rollback@example.com', 33]);

      await backend.rollback!(txnId);

      // Verify not persisted (rolled back)
      const result = await backend.query('SELECT * FROM users WHERE name = ?', ['WillRollback']);
      expect(result.rows).toHaveLength(0);
    });
  });

  // ===========================================================================
  // RAW QUERY TESTS
  // ===========================================================================

  describe('Raw Query Operations', () => {
    beforeEach(async () => {
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [1, 'Alice', 'alice@example.com', 25]);
    });

    it('should generate raw SQL', async () => {
      const query = knexBuilder.raw('SELECT * FROM users');
      const compiled = query.toSQL();

      expect(compiled.sql).toBe('SELECT * FROM users');
    });

    it('should generate raw SQL with bindings', async () => {
      const query = knexBuilder.raw('SELECT * FROM users WHERE id = ?', [1]);
      const compiled = query.toSQL();

      expect(compiled.sql).toBe('SELECT * FROM users WHERE id = ?');
      expect(compiled.bindings).toEqual([1]);
    });

    it('should execute raw query via backend', async () => {
      const result = await backend.query('SELECT * FROM users WHERE id = ?', [1]);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]).toHaveProperty('name', 'Alice');
    });

    it('should use raw in where clause', async () => {
      const query = knexBuilder('users')
        .where(knexBuilder.raw('age > ?', [20]))
        .select('*');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('age > ?');
      expect(compiled.bindings).toContain(20);
    });
  });

  // ===========================================================================
  // AGGREGATE TESTS
  // ===========================================================================

  describe('Aggregate Operations', () => {
    beforeEach(async () => {
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [1, 'Alice', 'alice@example.com', 25]);
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [2, 'Bob', 'bob@example.com', 30]);
      await backend.exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)', [3, 'Carol', 'carol@example.com', 35]);
    });

    it('should generate COUNT query', async () => {
      const query = knexBuilder('users').count('* as total');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('count(*)');
    });

    it('should generate SUM query', async () => {
      const query = knexBuilder('users').sum('age as totalAge');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('sum');
    });

    it('should generate AVG query', async () => {
      const query = knexBuilder('users').avg('age as avgAge');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('avg');
    });

    it('should generate MIN/MAX queries', async () => {
      const minQuery = knexBuilder('users').min('age as minAge');
      const maxQuery = knexBuilder('users').max('age as maxAge');

      expect(minQuery.toSQL().sql).toContain('min');
      expect(maxQuery.toSQL().sql).toContain('max');
    });

    it('should generate GROUP BY', async () => {
      await backend.exec('INSERT INTO orders (id, user_id, total, status) VALUES (?, ?, ?, ?)', [1, 1, 100, 'completed']);
      await backend.exec('INSERT INTO orders (id, user_id, total, status) VALUES (?, ?, ?, ?)', [2, 1, 50, 'pending']);
      await backend.exec('INSERT INTO orders (id, user_id, total, status) VALUES (?, ?, ?, ?)', [3, 2, 200, 'completed']);

      const query = knexBuilder('orders')
        .select('status')
        .count('* as count')
        .groupBy('status');
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('group by');
    });

    it('should generate GROUP BY with HAVING', async () => {
      const query = knexBuilder('orders')
        .select('user_id')
        .sum('total as totalSpent')
        .groupBy('user_id')
        .having('totalSpent', '>', 100);
      const compiled = query.toSQL();

      expect(compiled.sql).toContain('group by');
      expect(compiled.sql).toContain('having');
    });
  });

  // ===========================================================================
  // SCHEMA GENERATION TESTS
  // ===========================================================================

  describe('Schema Operations', () => {
    it('should generate CREATE TABLE SQL', async () => {
      const createSql = knexBuilder.schema.createTable('test_table', (table) => {
        table.increments('id');
        table.string('name');
        table.integer('value');
      }).toSQL();

      expect(createSql[0].sql).toContain('create table');
      expect(createSql[0].sql).toContain('test_table');
    });

    it('should generate DROP TABLE SQL', async () => {
      const dropSql = knexBuilder.schema.dropTable('test_table').toSQL();

      expect(dropSql[0].sql).toContain('drop table');
      expect(dropSql[0].sql).toContain('test_table');
    });

    it('should generate ALTER TABLE SQL', async () => {
      const alterSql = knexBuilder.schema.alterTable('users', (table) => {
        table.string('phone');
      }).toSQL();

      expect(alterSql[0].sql).toContain('alter table');
    });

    it('should generate CREATE INDEX SQL', async () => {
      const indexSql = knexBuilder.schema.alterTable('users', (table) => {
        table.index(['name'], 'idx_users_name');
      }).toSQL();

      expect(indexSql[0].sql).toContain('create index');
    });
  });

  // ===========================================================================
  // EDGE CASES
  // ===========================================================================

  describe('Edge Cases', () => {
    it('should handle empty result set', async () => {
      const query = knexBuilder('users').where('id', 999999).select('*');
      const result = await executeKnex(query);

      expect(result).toHaveLength(0);
    });

    it('should handle null values', async () => {
      const query = knexBuilder('users').insert({
        name: 'NullTest',
        email: null,
        age: 30,
      });
      const compiled = query.toSQL();

      expect(compiled.bindings).toContain(null);
    });

    it('should handle special characters', async () => {
      const specialName = "O'Brien \"The Great\"";
      const query = knexBuilder('users').insert({
        name: specialName,
        email: 'obrien@example.com',
        age: 40,
      });
      const compiled = query.toSQL();

      expect(compiled.bindings).toContain(specialName);
    });

    it('should handle unicode characters', async () => {
      const unicodeName = '';
      const query = knexBuilder('users').insert({
        name: unicodeName,
        email: 'unicode@example.com',
        age: 30,
      });
      const compiled = query.toSQL();

      expect(compiled.bindings).toContain(unicodeName);
    });

    it('should handle very large numbers', async () => {
      const query = knexBuilder('orders').insert({
        user_id: 1,
        total: 999999999.99,
        status: 'large',
      });
      const compiled = query.toSQL();

      expect(compiled.bindings).toContain(999999999.99);
    });
  });

  // ===========================================================================
  // INTEGRATION EXAMPLE
  // ===========================================================================

  describe('Full Integration Example', () => {
    it('should demonstrate complete CRUD workflow', async () => {
      // CREATE - Insert users
      const insertUser = knexBuilder('users').insert({
        name: 'Integration Test User',
        email: 'integration@test.com',
        age: 35,
      });
      await executeKnexModify(insertUser);

      // READ - Select user
      const selectUser = knexBuilder('users')
        .where('email', 'integration@test.com')
        .select('*');
      const users = await executeKnex(selectUser);
      expect(users).toHaveLength(1);
      expect(users[0]).toHaveProperty('name', 'Integration Test User');

      // UPDATE - Modify user
      const userId = users[0].id;
      const updateUser = knexBuilder('users')
        .where('id', userId)
        .update({ age: 36 });
      await executeKnexModify(updateUser);

      // Verify update
      const verifyUpdate = knexBuilder('users').where('id', userId).select('*');
      const updated = await executeKnex(verifyUpdate);
      expect(updated[0]).toHaveProperty('age', 36);

      // DELETE - Remove user
      const deleteUser = knexBuilder('users').where('id', userId).delete();
      await executeKnexModify(deleteUser);

      // Verify deletion
      const verifyDelete = knexBuilder('users').where('id', userId).select('*');
      const deleted = await executeKnex(verifyDelete);
      expect(deleted).toHaveLength(0);
    });
  });
});
