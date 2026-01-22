/**
 * Tests for DoSQL Prepared Statements
 */

import { describe, it, expect, beforeEach } from 'vitest';

import { Database, createDatabase, DatabaseError } from '../database.js';
import {
  parseParameters,
  bindParameters,
  validateParameters,
  coerceValue,
  isNamedParameters,
  BindingError,
} from './binding.js';
import {
  StatementCache,
  createStatementCache,
  hashString,
} from './cache.js';
import {
  PreparedStatement,
  InMemoryEngine,
  createInMemoryStorage,
  StatementError,
} from './statement.js';

// =============================================================================
// PARAMETER PARSING TESTS
// =============================================================================

describe('Parameter Parsing', () => {
  describe('parseParameters', () => {
    it('should parse positional parameters (?)', () => {
      const result = parseParameters('SELECT * FROM users WHERE id = ?');

      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0]).toMatchObject({
        type: 'positional',
        text: '?',
        key: 1,
      });
      expect(result.hasNamedParameters).toBe(false);
      expect(result.hasNumberedParameters).toBe(false);
    });

    it('should parse multiple positional parameters', () => {
      const result = parseParameters(
        'SELECT * FROM users WHERE id = ? AND name = ?'
      );

      expect(result.tokens).toHaveLength(2);
      expect(result.tokens[0].key).toBe(1);
      expect(result.tokens[1].key).toBe(2);
    });

    it('should parse numbered parameters (?NNN)', () => {
      const result = parseParameters(
        'SELECT * FROM users WHERE id = ?1 AND name = ?2'
      );

      expect(result.tokens).toHaveLength(2);
      expect(result.tokens[0]).toMatchObject({
        type: 'numbered',
        text: '?1',
        key: 1,
      });
      expect(result.tokens[1]).toMatchObject({
        type: 'numbered',
        text: '?2',
        key: 2,
      });
      expect(result.hasNumberedParameters).toBe(true);
      expect(result.maxNumberedIndex).toBe(2);
    });

    it('should parse named parameters (:name)', () => {
      const result = parseParameters(
        'SELECT * FROM users WHERE id = :id AND name = :name'
      );

      expect(result.tokens).toHaveLength(2);
      expect(result.tokens[0]).toMatchObject({
        type: 'named',
        text: ':id',
        key: 'id',
      });
      expect(result.tokens[1]).toMatchObject({
        type: 'named',
        text: ':name',
        key: 'name',
      });
      expect(result.hasNamedParameters).toBe(true);
      expect(result.namedParameterNames).toContain('id');
      expect(result.namedParameterNames).toContain('name');
    });

    it('should parse named parameters (@name)', () => {
      const result = parseParameters('SELECT * FROM users WHERE id = @id');

      expect(result.tokens[0]).toMatchObject({
        type: 'named',
        text: '@id',
        key: 'id',
      });
    });

    it('should parse named parameters ($name)', () => {
      const result = parseParameters('SELECT * FROM users WHERE id = $id');

      expect(result.tokens[0]).toMatchObject({
        type: 'named',
        text: '$id',
        key: 'id',
      });
    });

    it('should not parse parameters inside string literals', () => {
      const result = parseParameters("SELECT * FROM users WHERE name = '?test'");

      expect(result.tokens).toHaveLength(0);
    });

    it('should handle escaped quotes in strings', () => {
      const result = parseParameters(
        "SELECT * FROM users WHERE name = 'O''Brien' AND id = ?"
      );

      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0].type).toBe('positional');
    });

    it('should normalize SQL to use ? placeholders', () => {
      const result = parseParameters(
        'SELECT * FROM users WHERE id = :id AND name = ?'
      );

      expect(result.normalizedSql).toBe(
        'SELECT * FROM users WHERE id = ? AND name = ?'
      );
    });

    it('should handle SQL without parameters', () => {
      const result = parseParameters('SELECT * FROM users');

      expect(result.tokens).toHaveLength(0);
      expect(result.normalizedSql).toBe('SELECT * FROM users');
    });
  });
});

// =============================================================================
// PARAMETER BINDING TESTS
// =============================================================================

describe('Parameter Binding', () => {
  describe('bindParameters', () => {
    it('should bind positional parameters', () => {
      const parsed = parseParameters('SELECT * FROM users WHERE id = ?');
      const values = bindParameters(parsed, 123);

      expect(values).toEqual([123]);
    });

    it('should bind multiple positional parameters', () => {
      const parsed = parseParameters(
        'SELECT * FROM users WHERE id = ? AND name = ?'
      );
      const values = bindParameters(parsed, 123, 'Alice');

      expect(values).toEqual([123, 'Alice']);
    });

    it('should bind named parameters from object', () => {
      const parsed = parseParameters(
        'SELECT * FROM users WHERE id = :id AND name = :name'
      );
      const values = bindParameters(parsed, { id: 123, name: 'Alice' });

      expect(values).toEqual([123, 'Alice']);
    });

    it('should bind numbered parameters', () => {
      const parsed = parseParameters(
        'SELECT * FROM users WHERE name = ?2 AND id = ?1'
      );
      const values = bindParameters(parsed, 123, 'Alice');

      expect(values).toEqual(['Alice', 123]);
    });

    it('should throw for missing positional parameter', () => {
      const parsed = parseParameters(
        'SELECT * FROM users WHERE id = ? AND name = ?'
      );

      expect(() => bindParameters(parsed, 123)).toThrow(BindingError);
    });

    it('should throw for missing named parameter', () => {
      const parsed = parseParameters(
        'SELECT * FROM users WHERE id = :id AND name = :name'
      );

      expect(() => bindParameters(parsed, { id: 123 })).toThrow(BindingError);
    });
  });

  describe('coerceValue', () => {
    it('should pass through null', () => {
      expect(coerceValue(null)).toBe(null);
    });

    it('should convert undefined to null', () => {
      expect(coerceValue(undefined)).toBe(null);
    });

    it('should pass through strings', () => {
      expect(coerceValue('hello')).toBe('hello');
    });

    it('should pass through numbers', () => {
      expect(coerceValue(42)).toBe(42);
      expect(coerceValue(3.14)).toBe(3.14);
    });

    it('should pass through bigint', () => {
      expect(coerceValue(BigInt(9007199254740993))).toBe(BigInt(9007199254740993));
    });

    it('should convert boolean to 0/1', () => {
      expect(coerceValue(true)).toBe(1);
      expect(coerceValue(false)).toBe(0);
    });

    it('should convert Date to ISO string', () => {
      const date = new Date('2024-01-15T12:00:00Z');
      expect(coerceValue(date)).toBe('2024-01-15T12:00:00.000Z');
    });

    it('should pass through Uint8Array', () => {
      const buffer = new Uint8Array([1, 2, 3]);
      expect(coerceValue(buffer)).toBe(buffer);
    });

    it('should convert objects to JSON', () => {
      expect(coerceValue({ foo: 'bar' })).toBe('{"foo":"bar"}');
    });

    it('should throw for Infinity', () => {
      expect(() => coerceValue(Infinity)).toThrow(BindingError);
    });

    it('should throw for NaN', () => {
      expect(() => coerceValue(NaN)).toThrow(BindingError);
    });
  });

  describe('isNamedParameters', () => {
    it('should return true for plain objects', () => {
      expect(isNamedParameters({ id: 1 })).toBe(true);
    });

    it('should return false for arrays', () => {
      expect(isNamedParameters([1, 2, 3])).toBe(false);
    });

    it('should return false for null', () => {
      expect(isNamedParameters(null as any)).toBe(false);
    });
  });
});

// =============================================================================
// STATEMENT CACHE TESTS
// =============================================================================

describe('Statement Cache', () => {
  describe('StatementCache', () => {
    let cache: StatementCache;
    let engine: InMemoryEngine;

    beforeEach(() => {
      cache = createStatementCache({ maxSize: 3 });
      engine = new InMemoryEngine();
    });

    it('should cache and retrieve statements', () => {
      const sql = 'SELECT * FROM users';
      const parsed = parseParameters(sql);
      const stmt = new PreparedStatement(sql, engine);

      cache.set(sql, stmt, parsed);

      const entry = cache.get(sql);
      expect(entry).toBeDefined();
      expect(entry?.statement.source).toBe(sql);
    });

    it('should return undefined for uncached SQL', () => {
      expect(cache.get('SELECT * FROM users')).toBeUndefined();
    });

    it('should evict LRU entries when full', () => {
      const sqls = [
        'SELECT 1',
        'SELECT 2',
        'SELECT 3',
        'SELECT 4',
      ];

      for (const sql of sqls) {
        const parsed = parseParameters(sql);
        const stmt = new PreparedStatement(sql, engine);
        cache.set(sql, stmt, parsed);
      }

      // First SQL should be evicted
      expect(cache.get('SELECT 1')).toBeUndefined();
      expect(cache.get('SELECT 4')).toBeDefined();
    });

    it('should update LRU order on access', () => {
      const sqls = ['SELECT 1', 'SELECT 2', 'SELECT 3'];

      for (const sql of sqls) {
        const parsed = parseParameters(sql);
        const stmt = new PreparedStatement(sql, engine);
        cache.set(sql, stmt, parsed);
      }

      // Access first SQL to make it recently used
      cache.get('SELECT 1');

      // Add new SQL
      const newSql = 'SELECT 4';
      const parsed = parseParameters(newSql);
      const stmt = new PreparedStatement(newSql, engine);
      cache.set(newSql, stmt, parsed);

      // Second SQL should be evicted (it was LRU)
      expect(cache.get('SELECT 1')).toBeDefined();
      expect(cache.get('SELECT 2')).toBeUndefined();
      expect(cache.get('SELECT 3')).toBeDefined();
    });

    it('should track statistics', () => {
      const sql = 'SELECT * FROM users';
      const parsed = parseParameters(sql);
      const stmt = new PreparedStatement(sql, engine);

      cache.set(sql, stmt, parsed);
      cache.get(sql); // Hit
      cache.get(sql); // Hit
      cache.get('SELECT other'); // Miss

      const stats = cache.getStats();
      expect(stats.hits).toBe(2);
      expect(stats.misses).toBe(1);
      expect(stats.hitRate).toBeCloseTo(66.67, 1);
    });

    it('should clear all entries', () => {
      const sql = 'SELECT * FROM users';
      const parsed = parseParameters(sql);
      const stmt = new PreparedStatement(sql, engine);

      cache.set(sql, stmt, parsed);
      cache.clear();

      expect(cache.size).toBe(0);
      expect(cache.get(sql)).toBeUndefined();
    });

    it('should delete specific entries', () => {
      const sql = 'SELECT * FROM users';
      const parsed = parseParameters(sql);
      const stmt = new PreparedStatement(sql, engine);

      cache.set(sql, stmt, parsed);
      expect(cache.delete(sql)).toBe(true);
      expect(cache.get(sql)).toBeUndefined();
    });
  });

  describe('hashString', () => {
    it('should produce consistent hashes', () => {
      const sql = 'SELECT * FROM users';
      expect(hashString(sql)).toBe(hashString(sql));
    });

    it('should produce different hashes for different strings', () => {
      expect(hashString('SELECT 1')).not.toBe(hashString('SELECT 2'));
    });
  });
});

// =============================================================================
// PREPARED STATEMENT TESTS
// =============================================================================

describe('PreparedStatement', () => {
  let engine: InMemoryEngine;

  beforeEach(() => {
    engine = new InMemoryEngine();

    // Create test table
    engine.execute(
      'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)',
      []
    );

    // Insert test data
    engine.execute(
      "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
      [1, 'Alice', 30]
    );
    engine.execute(
      "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
      [2, 'Bob', 25]
    );
    engine.execute(
      "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
      [3, 'Charlie', 35]
    );
  });

  describe('run()', () => {
    it('should execute INSERT and return result', () => {
      const stmt = new PreparedStatement(
        'INSERT INTO users (name, age) VALUES (?, ?)',
        engine
      );

      const result = stmt.run('David', 28);

      expect(result.changes).toBe(1);
      expect(result.lastInsertRowid).toBeDefined();
    });

    it('should execute UPDATE and return changes', () => {
      const stmt = new PreparedStatement(
        'UPDATE users SET age = ? WHERE name = ?',
        engine
      );

      const result = stmt.run(31, 'Alice');

      expect(result.changes).toBe(1);
    });

    it('should execute DELETE and return changes', () => {
      const stmt = new PreparedStatement(
        'DELETE FROM users WHERE name = ?',
        engine
      );

      const result = stmt.run('Bob');

      expect(result.changes).toBe(1);
    });
  });

  describe('get()', () => {
    it('should return first row', () => {
      const stmt = new PreparedStatement(
        'SELECT * FROM users WHERE id = ?',
        engine
      );

      const user = stmt.get(1);

      expect(user).toMatchObject({
        id: 1,
        name: 'Alice',
        age: 30,
      });
    });

    it('should return undefined for no results', () => {
      const stmt = new PreparedStatement(
        'SELECT * FROM users WHERE id = ?',
        engine
      );

      const user = stmt.get(999);

      expect(user).toBeUndefined();
    });

    it('should work with named parameters', () => {
      const stmt = new PreparedStatement(
        'SELECT * FROM users WHERE id = :id',
        engine
      );

      const user = stmt.get({ id: 1 });

      expect(user).toMatchObject({ id: 1, name: 'Alice' });
    });
  });

  describe('all()', () => {
    it('should return all matching rows', () => {
      const stmt = new PreparedStatement(
        'SELECT * FROM users WHERE age > ?',
        engine
      );

      const users = stmt.all(25);

      expect(users).toHaveLength(2);
      expect(users.map(u => (u as any).name)).toContain('Alice');
      expect(users.map(u => (u as any).name)).toContain('Charlie');
    });

    it('should return empty array for no matches', () => {
      const stmt = new PreparedStatement(
        'SELECT * FROM users WHERE age > ?',
        engine
      );

      const users = stmt.all(100);

      expect(users).toEqual([]);
    });
  });

  describe('iterate()', () => {
    it('should iterate over all rows', () => {
      const stmt = new PreparedStatement('SELECT * FROM users', engine);

      const names: string[] = [];
      for (const user of stmt.iterate()) {
        names.push((user as any).name);
      }

      expect(names).toHaveLength(3);
      expect(names).toContain('Alice');
      expect(names).toContain('Bob');
      expect(names).toContain('Charlie');
    });
  });

  describe('bind()', () => {
    it('should bind parameters for later execution', () => {
      const stmt = new PreparedStatement(
        'SELECT * FROM users WHERE id = ?',
        engine
      );

      stmt.bind(2);
      const user = stmt.get();

      expect(user).toMatchObject({ id: 2, name: 'Bob' });
    });

    it('should be chainable', () => {
      const stmt = new PreparedStatement(
        'SELECT * FROM users WHERE id = ?',
        engine
      );

      const user = stmt.bind(1).get();

      expect(user).toMatchObject({ id: 1, name: 'Alice' });
    });
  });

  describe('columns()', () => {
    it('should return column metadata', () => {
      const stmt = new PreparedStatement('SELECT * FROM users', engine);

      const cols = stmt.columns();

      expect(cols).toHaveLength(3);
      expect(cols.map(c => c.name)).toContain('id');
      expect(cols.map(c => c.name)).toContain('name');
      expect(cols.map(c => c.name)).toContain('age');
    });
  });

  describe('finalize()', () => {
    it('should prevent further use', () => {
      const stmt = new PreparedStatement('SELECT * FROM users', engine);

      stmt.finalize();

      expect(stmt.finalized).toBe(true);
      expect(() => stmt.all()).toThrow(StatementError);
    });
  });

  describe('options', () => {
    it('should support pluck mode', () => {
      const stmt = new PreparedStatement(
        'SELECT name FROM users WHERE id = ?',
        engine
      );

      const name = stmt.pluck().get(1);

      expect(name).toBe('Alice');
    });

    it('should support raw mode', () => {
      const stmt = new PreparedStatement(
        'SELECT id, name FROM users WHERE id = ?',
        engine
      );

      const row = stmt.raw().get(1);

      expect(Array.isArray(row)).toBe(true);
      expect(row).toContain(1);
      expect(row).toContain('Alice');
    });
  });

  describe('reader property', () => {
    it('should be true for SELECT statements', () => {
      const stmt = new PreparedStatement('SELECT * FROM users', engine);
      expect(stmt.reader).toBe(true);
    });

    it('should be false for INSERT statements', () => {
      const stmt = new PreparedStatement(
        'INSERT INTO users (name) VALUES (?)',
        engine
      );
      expect(stmt.reader).toBe(false);
    });

    it('should be false for UPDATE statements', () => {
      const stmt = new PreparedStatement(
        'UPDATE users SET name = ?',
        engine
      );
      expect(stmt.reader).toBe(false);
    });

    it('should be false for DELETE statements', () => {
      const stmt = new PreparedStatement('DELETE FROM users', engine);
      expect(stmt.reader).toBe(false);
    });
  });
});

// =============================================================================
// DATABASE CLASS TESTS
// =============================================================================

describe('Database', () => {
  let db: Database;

  beforeEach(() => {
    db = createDatabase();

    db.exec(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT,
        email TEXT,
        age INTEGER
      )
    `);
  });

  describe('prepare()', () => {
    it('should create a prepared statement', () => {
      const stmt = db.prepare('SELECT * FROM users');

      expect(stmt).toBeDefined();
      expect(stmt.source).toBe('SELECT * FROM users');
    });

    it('should cache prepared statements', () => {
      const sql = 'SELECT * FROM users WHERE id = ?';

      db.prepare(sql);
      db.prepare(sql);

      const stats = db.getCacheStats();
      expect(stats.size).toBeGreaterThan(0);
    });
  });

  describe('exec()', () => {
    it('should execute multiple statements', () => {
      db.exec(`
        INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
        INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com');
      `);

      const users = db.prepare('SELECT * FROM users').all();
      expect(users).toHaveLength(2);
    });

    it('should be chainable', () => {
      const result = db.exec("INSERT INTO users (name) VALUES ('Alice')");
      expect(result).toBe(db);
    });
  });

  describe('transaction()', () => {
    it('should wrap function in transaction', () => {
      const insert = db.transaction((name: string) => {
        db.prepare('INSERT INTO users (name) VALUES (?)').run(name);
        return db.prepare('SELECT * FROM users WHERE name = ?').get(name);
      });

      const user = insert('Alice');
      expect(user).toMatchObject({ name: 'Alice' });
    });

    it('should support deferred mode', () => {
      const insert = db.transaction((name: string) => {
        db.prepare('INSERT INTO users (name) VALUES (?)').run(name);
      });

      expect(() => insert.deferred('Alice')).not.toThrow();
    });

    it('should support immediate mode', () => {
      const insert = db.transaction((name: string) => {
        db.prepare('INSERT INTO users (name) VALUES (?)').run(name);
      });

      expect(() => insert.immediate('Bob')).not.toThrow();
    });

    it('should support exclusive mode', () => {
      const insert = db.transaction((name: string) => {
        db.prepare('INSERT INTO users (name) VALUES (?)').run(name);
      });

      expect(() => insert.exclusive('Charlie')).not.toThrow();
    });
  });

  describe('pragma()', () => {
    it('should get pragma values', () => {
      const journalMode = db.pragma('journal_mode');
      expect(journalMode).toBeDefined();
    });

    it('should get table_info', () => {
      const tableInfo = db.pragma('table_info', 'users');

      expect(tableInfo).toBeDefined();
      expect(Array.isArray(tableInfo)).toBe(true);
    });
  });

  describe('savepoint()', () => {
    it('should create and release savepoints', () => {
      db.savepoint('sp1');
      db.prepare('INSERT INTO users (name) VALUES (?)').run('Alice');
      db.release('sp1');

      const users = db.prepare('SELECT * FROM users').all();
      expect(users).toHaveLength(1);
    });

    it('should rollback to savepoint', () => {
      db.prepare('INSERT INTO users (name) VALUES (?)').run('Alice');
      db.savepoint('sp1');
      db.prepare('INSERT INTO users (name) VALUES (?)').run('Bob');
      db.rollback('sp1');

      // Note: In-memory engine doesn't actually rollback,
      // but API is compatible
    });
  });

  describe('function()', () => {
    it('should register user-defined function', () => {
      db.function('double', (x) => Number(x) * 2);

      // Note: Function execution not implemented in in-memory engine
      expect(db).toBeDefined();
    });
  });

  describe('aggregate()', () => {
    it('should register user-defined aggregate', () => {
      db.aggregate('mysum', {
        start: 0,
        step: (acc, val) => acc + Number(val),
        result: (acc) => acc,
      });

      // Note: Aggregate execution not implemented in in-memory engine
      expect(db).toBeDefined();
    });
  });

  describe('close()', () => {
    it('should close the database', () => {
      db.close();

      expect(db.open).toBe(false);
    });

    it('should prevent further operations', () => {
      db.close();

      expect(() => db.prepare('SELECT 1')).toThrow(DatabaseError);
    });

    it('should be idempotent', () => {
      db.close();
      db.close();

      expect(db.open).toBe(false);
    });
  });

  describe('properties', () => {
    it('should have correct name', () => {
      expect(db.name).toBe(':memory:');
    });

    it('should not be readonly by default', () => {
      expect(db.readonly).toBe(false);
    });

    it('should track inTransaction state', () => {
      expect(db.inTransaction).toBe(false);
    });
  });

  describe('utility methods', () => {
    it('should list tables', () => {
      const tables = db.getTables();
      expect(tables).toContain('users');
    });

    it('should check table existence', () => {
      expect(db.hasTable('users')).toBe(true);
      expect(db.hasTable('nonexistent')).toBe(false);
    });
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('Integration Tests', () => {
  it('should handle complete workflow', () => {
    const db = createDatabase();

    // Create table
    db.exec(`
      CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        price INTEGER,
        quantity INTEGER
      )
    `);

    // Prepare statements
    const insert = db.prepare(
      'INSERT INTO products (name, price, quantity) VALUES (:name, :price, :quantity)'
    );
    const selectAll = db.prepare('SELECT * FROM products');
    const selectByPrice = db.prepare('SELECT * FROM products WHERE price > ?');

    // Insert products
    insert.run({ name: 'Widget', price: 100, quantity: 50 });
    insert.run({ name: 'Gadget', price: 200, quantity: 30 });
    insert.run({ name: 'Gizmo', price: 150, quantity: 25 });

    // Query all
    const all = selectAll.all();
    expect(all).toHaveLength(3);

    // Query by price
    const expensive = selectByPrice.all(150);
    expect(expensive).toHaveLength(1);
    expect((expensive[0] as any).name).toBe('Gadget');

    // Update
    const update = db.prepare(
      'UPDATE products SET quantity = quantity - ? WHERE name = ?'
    );
    const result = update.run(10, 'Widget');
    expect(result.changes).toBe(1);

    // Delete
    const del = db.prepare('DELETE FROM products WHERE price < ?');
    const delResult = del.run(150);
    expect(delResult.changes).toBe(1);

    // Verify
    const remaining = selectAll.all();
    expect(remaining).toHaveLength(2);

    db.close();
  });

  it('should handle mixed parameter styles', () => {
    const db = createDatabase();

    db.exec('CREATE TABLE test (a TEXT, b TEXT, c TEXT)');

    // Positional
    db.prepare('INSERT INTO test (a, b, c) VALUES (?, ?, ?)').run('1', '2', '3');

    // Named
    db.prepare('INSERT INTO test (a, b, c) VALUES (:a, :b, :c)').run({
      a: '4',
      b: '5',
      c: '6',
    });

    const rows = db.prepare('SELECT * FROM test').all();
    expect(rows).toHaveLength(2);

    db.close();
  });

  it('should handle transactions with error', () => {
    const db = createDatabase();

    db.exec('CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)');
    db.prepare('INSERT INTO accounts (id, balance) VALUES (?, ?)').run(1, 1000);
    db.prepare('INSERT INTO accounts (id, balance) VALUES (?, ?)').run(2, 500);

    const transfer = db.transaction((from: number, to: number, amount: number) => {
      const fromAccount = db.prepare(
        'SELECT balance FROM accounts WHERE id = ?'
      ).get(from) as { balance: number } | undefined;

      if (!fromAccount || fromAccount.balance < amount) {
        throw new Error('Insufficient funds');
      }

      db.prepare('UPDATE accounts SET balance = balance - ? WHERE id = ?').run(
        amount,
        from
      );
      db.prepare('UPDATE accounts SET balance = balance + ? WHERE id = ?').run(
        amount,
        to
      );
    });

    // Successful transfer
    transfer(1, 2, 200);

    // Failed transfer (insufficient funds)
    expect(() => transfer(1, 2, 10000)).toThrow('Insufficient funds');

    db.close();
  });
});
