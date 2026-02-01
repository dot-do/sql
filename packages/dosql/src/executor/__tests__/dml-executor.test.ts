/**
 * DoSQL DML Executor Tests
 *
 * Comprehensive tests for the DML executor covering INSERT, UPDATE, DELETE,
 * and REPLACE operations with RETURNING clause support, WHERE predicates,
 * ORDER BY, LIMIT, parameterized queries, and edge cases.
 *
 * Uses the InMemoryDMLStorage provided by the module for isolated testing
 * without mocks (real storage, real parser, real executor).
 *
 * @module executor/__tests__/dml-executor
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  DMLExecutor,
  InMemoryDMLStorage,
  createInMemoryDMLExecutor,
  createDMLExecutor,
} from '../dml-executor.js';

// =============================================================================
// TEST SETUP
// =============================================================================

let executor: DMLExecutor;
let storage: InMemoryDMLStorage;

beforeEach(() => {
  const ctx = createInMemoryDMLExecutor();
  executor = ctx.executor;
  storage = ctx.storage;
  storage.createTable('users', ['id', 'name', 'email', 'age']);
});

// =============================================================================
// INSERT TESTS
// =============================================================================

describe('DMLExecutor: INSERT', () => {
  it('inserts a single row with literal values', async () => {
    const result = await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)"
    );

    expect(result.statementType).toBe('insert');
    expect(result.changes).toBe(1);
    expect(result.lastInsertRowid).toBe(1);

    const rows = await storage.getAll('users');
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ name: 'Alice', email: 'alice@test.com', age: 30 });
  });

  it('inserts multiple rows in a single statement', async () => {
    const result = await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30), ('Bob', 'bob@test.com', 25)"
    );

    expect(result.changes).toBe(2);

    const rows = await storage.getAll('users');
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ name: 'Alice' });
    expect(rows[1]).toMatchObject({ name: 'Bob' });
  });

  it('inserts with NULL values', async () => {
    const result = await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Charlie', NULL, NULL)"
    );

    expect(result.changes).toBe(1);

    const rows = await storage.getAll('users');
    expect(rows[0]).toMatchObject({ name: 'Charlie', email: null, age: null });
  });

  it('inserts with numeric literal values', async () => {
    const result = await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Diana', 'diana@test.com', 42)"
    );

    const rows = await storage.getAll('users');
    expect(rows[0].age).toBe(42);
  });

  it('auto-increments id on each insert', async () => {
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Alice', 'a@t.com', 20)"
    );
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Bob', 'b@t.com', 25)"
    );

    const rows = await storage.getAll('users');
    expect(rows[0].id).toBe(1);
    expect(rows[1].id).toBe(2);
  });

  it('returns empty rows array when no RETURNING clause', async () => {
    const result = await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Alice', 'a@t.com', 20)"
    );

    expect(result.rows).toEqual([]);
    expect(result.columns).toEqual([]);
  });

  it('inserts into a table that does not yet exist (auto-creates)', async () => {
    const result = await executor.execute(
      "INSERT INTO products (name, price) VALUES ('Widget', 9.99)"
    );

    expect(result.changes).toBe(1);
    const rows = await storage.getAll('products');
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ name: 'Widget', price: 9.99 });
  });

  it('throws on invalid SQL', async () => {
    await expect(
      executor.execute('INSERT INVALID')
    ).rejects.toThrow();
  });
});

// =============================================================================
// INSERT WITH RETURNING TESTS
// =============================================================================

describe('DMLExecutor: INSERT with RETURNING', () => {
  it('returns all columns with RETURNING *', async () => {
    const result = await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30) RETURNING *"
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({
      name: 'Alice',
      email: 'alice@test.com',
      age: 30,
    });
    expect(result.columns).toContain('id');
    expect(result.columns).toContain('name');
    expect(result.columns).toContain('email');
    expect(result.columns).toContain('age');
  });

  it('returns specific columns with RETURNING col1, col2', async () => {
    const result = await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30) RETURNING name, email"
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toHaveProperty('name', 'Alice');
    expect(result.rows[0]).toHaveProperty('email', 'alice@test.com');
    expect(result.columns).toEqual(['name', 'email']);
  });

  it('returns rows for multi-row insert with RETURNING', async () => {
    const result = await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Alice', 'a@t.com', 30), ('Bob', 'b@t.com', 25) RETURNING name, age"
    );

    expect(result.rows).toHaveLength(2);
    expect(result.rows[0]).toMatchObject({ name: 'Alice', age: 30 });
    expect(result.rows[1]).toMatchObject({ name: 'Bob', age: 25 });
  });
});

// =============================================================================
// UPDATE TESTS
// =============================================================================

describe('DMLExecutor: UPDATE', () => {
  beforeEach(async () => {
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)"
    );
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)"
    );
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@test.com', 35)"
    );
  });

  it('updates all rows when no WHERE clause', async () => {
    const result = await executor.execute(
      "UPDATE users SET age = 99"
    );

    expect(result.statementType).toBe('update');
    expect(result.changes).toBe(3);

    const rows = await storage.getAll('users');
    for (const row of rows) {
      expect(row.age).toBe(99);
    }
  });

  it('updates rows matching WHERE clause', async () => {
    const result = await executor.execute(
      "UPDATE users SET age = 31 WHERE name = 'Alice'"
    );

    expect(result.changes).toBe(1);

    const rows = await storage.getAll('users');
    const alice = rows.find(r => r.name === 'Alice');
    expect(alice?.age).toBe(31);

    const bob = rows.find(r => r.name === 'Bob');
    expect(bob?.age).toBe(25);
  });

  it('updates multiple columns', async () => {
    const result = await executor.execute(
      "UPDATE users SET name = 'Alicia', age = 31 WHERE name = 'Alice'"
    );

    expect(result.changes).toBe(1);

    const rows = await storage.getAll('users');
    const updated = rows.find(r => r.name === 'Alicia');
    expect(updated).toBeDefined();
    expect(updated?.age).toBe(31);
  });

  it('updates with numeric comparison in WHERE', async () => {
    const result = await executor.execute(
      "UPDATE users SET email = 'senior@test.com' WHERE age > 30"
    );

    // Charlie (35) matches
    expect(result.changes).toBe(1);

    const rows = await storage.getAll('users');
    const charlie = rows.find(r => r.name === 'Charlie');
    expect(charlie?.email).toBe('senior@test.com');
  });

  it('returns 0 changes when no rows match WHERE', async () => {
    const result = await executor.execute(
      "UPDATE users SET age = 100 WHERE name = 'NonExistent'"
    );

    expect(result.changes).toBe(0);
  });

  it('returns empty rows array when no RETURNING clause', async () => {
    const result = await executor.execute(
      "UPDATE users SET age = 99"
    );

    expect(result.rows).toEqual([]);
    expect(result.columns).toEqual([]);
  });
});

// =============================================================================
// UPDATE WITH RETURNING TESTS
// =============================================================================

describe('DMLExecutor: UPDATE with RETURNING', () => {
  beforeEach(async () => {
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)"
    );
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)"
    );
  });

  it('returns updated rows with RETURNING *', async () => {
    const result = await executor.execute(
      "UPDATE users SET age = 99 WHERE name = 'Alice' RETURNING *"
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({
      name: 'Alice',
      age: 99,
    });
  });

  it('returns specific columns with RETURNING', async () => {
    const result = await executor.execute(
      "UPDATE users SET age = 40 WHERE name = 'Bob' RETURNING name, age"
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({ name: 'Bob', age: 40 });
    expect(result.columns).toEqual(['name', 'age']);
  });
});

// =============================================================================
// DELETE TESTS
// =============================================================================

describe('DMLExecutor: DELETE', () => {
  beforeEach(async () => {
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)"
    );
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)"
    );
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@test.com', 35)"
    );
  });

  it('deletes all rows when no WHERE clause', async () => {
    const result = await executor.execute('DELETE FROM users');

    expect(result.statementType).toBe('delete');
    expect(result.changes).toBe(3);

    const rows = await storage.getAll('users');
    expect(rows).toHaveLength(0);
  });

  it('deletes rows matching WHERE clause', async () => {
    const result = await executor.execute(
      "DELETE FROM users WHERE name = 'Alice'"
    );

    expect(result.changes).toBe(1);

    const rows = await storage.getAll('users');
    expect(rows).toHaveLength(2);
    expect(rows.find(r => r.name === 'Alice')).toBeUndefined();
  });

  it('deletes rows matching numeric WHERE', async () => {
    const result = await executor.execute(
      "DELETE FROM users WHERE age < 30"
    );

    expect(result.changes).toBe(1);

    const rows = await storage.getAll('users');
    expect(rows).toHaveLength(2);
    expect(rows.find(r => r.name === 'Bob')).toBeUndefined();
  });

  it('returns 0 changes when no rows match', async () => {
    const result = await executor.execute(
      "DELETE FROM users WHERE name = 'NonExistent'"
    );

    expect(result.changes).toBe(0);
    const rows = await storage.getAll('users');
    expect(rows).toHaveLength(3);
  });

  it('returns empty rows when no RETURNING clause', async () => {
    const result = await executor.execute('DELETE FROM users');

    expect(result.rows).toEqual([]);
    expect(result.columns).toEqual([]);
  });
});

// =============================================================================
// DELETE WITH RETURNING TESTS
// =============================================================================

describe('DMLExecutor: DELETE with RETURNING', () => {
  beforeEach(async () => {
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)"
    );
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)"
    );
  });

  it('returns deleted rows with RETURNING *', async () => {
    const result = await executor.execute(
      "DELETE FROM users WHERE name = 'Alice' RETURNING *"
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({
      name: 'Alice',
      email: 'alice@test.com',
      age: 30,
    });
  });

  it('returns specific columns from deleted rows', async () => {
    const result = await executor.execute(
      "DELETE FROM users WHERE name = 'Bob' RETURNING name, age"
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({ name: 'Bob', age: 25 });
    expect(result.columns).toEqual(['name', 'age']);
  });

  it('returns all deleted rows with RETURNING', async () => {
    const result = await executor.execute(
      'DELETE FROM users RETURNING name'
    );

    expect(result.rows).toHaveLength(2);
    const names = result.rows.map((r: Record<string, unknown>) => r.name);
    expect(names).toContain('Alice');
    expect(names).toContain('Bob');
  });
});

// =============================================================================
// REPLACE TESTS
// =============================================================================

describe('DMLExecutor: REPLACE', () => {
  it('inserts via REPLACE INTO', async () => {
    const result = await executor.execute(
      "REPLACE INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)"
    );

    expect(result.statementType).toBe('replace');
    expect(result.changes).toBe(1);

    const rows = await storage.getAll('users');
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ name: 'Alice' });
  });

  it('REPLACE with RETURNING returns inserted row', async () => {
    const result = await executor.execute(
      "REPLACE INTO users (name, email, age) VALUES ('Alice', 'a@t.com', 30) RETURNING name, age"
    );

    expect(result.statementType).toBe('replace');
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({ name: 'Alice', age: 30 });
  });
});

// =============================================================================
// WHERE CLAUSE OPERATORS
// =============================================================================

describe('DMLExecutor: WHERE clause operators', () => {
  beforeEach(async () => {
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)"
    );
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)"
    );
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@test.com', 35)"
    );
  });

  it('supports = operator', async () => {
    const result = await executor.execute(
      "DELETE FROM users WHERE name = 'Bob'"
    );
    expect(result.changes).toBe(1);
  });

  it('supports != operator', async () => {
    const result = await executor.execute(
      "DELETE FROM users WHERE name != 'Alice'"
    );
    expect(result.changes).toBe(2);
  });

  it('supports < operator', async () => {
    const result = await executor.execute(
      'DELETE FROM users WHERE age < 30'
    );
    expect(result.changes).toBe(1);
  });

  it('supports <= operator', async () => {
    const result = await executor.execute(
      'DELETE FROM users WHERE age <= 30'
    );
    expect(result.changes).toBe(2);
  });

  it('supports > operator', async () => {
    const result = await executor.execute(
      'DELETE FROM users WHERE age > 30'
    );
    expect(result.changes).toBe(1);
  });

  it('supports >= operator', async () => {
    const result = await executor.execute(
      'DELETE FROM users WHERE age >= 30'
    );
    expect(result.changes).toBe(2);
  });

  it('supports AND in WHERE', async () => {
    const result = await executor.execute(
      "DELETE FROM users WHERE age > 20 AND name = 'Bob'"
    );
    expect(result.changes).toBe(1);
  });

  it('supports OR in WHERE', async () => {
    const result = await executor.execute(
      "DELETE FROM users WHERE name = 'Alice' OR name = 'Charlie'"
    );
    expect(result.changes).toBe(2);
  });
});

// =============================================================================
// PARAMETERIZED QUERIES
// =============================================================================

describe('DMLExecutor: parameterized queries', () => {
  it('INSERT with parameter placeholders', async () => {
    const result = await executor.execute(
      'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
      { params: ['Alice', 'alice@test.com', 30] }
    );

    expect(result.changes).toBe(1);
    const rows = await storage.getAll('users');
    expect(rows[0]).toMatchObject({ name: 'Alice', email: 'alice@test.com', age: 30 });
  });

  it('INSERT with multiple rows and parameters', async () => {
    const result = await executor.execute(
      'INSERT INTO users (name, email, age) VALUES (?, ?, ?), (?, ?, ?)',
      { params: ['Alice', 'a@t.com', 30, 'Bob', 'b@t.com', 25] }
    );

    expect(result.changes).toBe(2);
    const rows = await storage.getAll('users');
    expect(rows).toHaveLength(2);
  });
});

// =============================================================================
// BINARY OPERATIONS IN UPDATE
// =============================================================================

describe('DMLExecutor: binary operations in UPDATE SET', () => {
  beforeEach(async () => {
    await executor.execute(
      "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)"
    );
  });

  it('supports addition in SET clause (age = age + 1)', async () => {
    const result = await executor.execute(
      "UPDATE users SET age = age + 1 WHERE name = 'Alice'"
    );

    expect(result.changes).toBe(1);
    const rows = await storage.getAll('users');
    expect(rows[0].age).toBe(31);
  });

  it('supports subtraction in SET clause', async () => {
    await executor.execute(
      "UPDATE users SET age = age - 5 WHERE name = 'Alice'"
    );

    const rows = await storage.getAll('users');
    expect(rows[0].age).toBe(25);
  });

  it('supports multiplication in SET clause', async () => {
    await executor.execute(
      "UPDATE users SET age = age * 2 WHERE name = 'Alice'"
    );

    const rows = await storage.getAll('users');
    expect(rows[0].age).toBe(60);
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('DMLExecutor: edge cases', () => {
  it('handles empty table for DELETE', async () => {
    const result = await executor.execute('DELETE FROM users');
    expect(result.changes).toBe(0);
  });

  it('handles empty table for UPDATE', async () => {
    const result = await executor.execute("UPDATE users SET name = 'X'");
    expect(result.changes).toBe(0);
  });

  it('handles INSERT with DEFAULT VALUES', async () => {
    const result = await executor.execute(
      'INSERT INTO users DEFAULT VALUES'
    );

    expect(result.changes).toBe(1);
    const rows = await storage.getAll('users');
    expect(rows).toHaveLength(1);
    expect(rows[0].id).toBeDefined();
  });

  it('throws on INSERT ... SELECT (not yet supported)', async () => {
    await expect(
      executor.execute('INSERT INTO users SELECT * FROM other')
    ).rejects.toThrow('INSERT ... SELECT not yet supported');
  });

  it('parse error produces meaningful message', async () => {
    await expect(
      executor.execute('NOT SQL AT ALL')
    ).rejects.toThrow('Parse error');
  });
});

// =============================================================================
// FUNCTION EVALUATOR
// =============================================================================

describe('DMLExecutor: custom function evaluator', () => {
  it('uses custom function in INSERT values', async () => {
    const result = await executor.execute(
      "INSERT INTO users (name, email, age) VALUES (UPPER('alice'), 'a@t.com', 30)",
      {
        functionEvaluator: (name, args) => {
          if (name === 'UPPER' || name === 'upper') {
            return String(args[0]).toUpperCase();
          }
          return null;
        },
      }
    );

    expect(result.changes).toBe(1);
    const rows = await storage.getAll('users');
    expect(rows[0].name).toBe('ALICE');
  });
});

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

describe('DMLExecutor: factory functions', () => {
  it('createDMLExecutor creates executor with custom storage', () => {
    const customStorage = new InMemoryDMLStorage();
    const exec = createDMLExecutor(customStorage);
    expect(exec).toBeInstanceOf(DMLExecutor);
  });

  it('createInMemoryDMLExecutor returns executor and storage', () => {
    const { executor: exec, storage: stor } = createInMemoryDMLExecutor();
    expect(exec).toBeInstanceOf(DMLExecutor);
    expect(stor).toBeInstanceOf(InMemoryDMLStorage);
  });
});

// =============================================================================
// InMemoryDMLStorage TESTS
// =============================================================================

describe('InMemoryDMLStorage', () => {
  it('clear() removes all tables and data', async () => {
    storage.createTable('products', ['id', 'name']);
    await storage.insert('users', { name: 'Alice' });
    await storage.insert('products', { name: 'Widget' });

    storage.clear();

    const users = await storage.getAll('users');
    const products = await storage.getAll('products');
    expect(users).toEqual([]);
    expect(products).toEqual([]);
  });

  it('getAll returns empty array for unknown table', async () => {
    const rows = await storage.getAll('nonexistent');
    expect(rows).toEqual([]);
  });

  it('query returns empty array for unknown table', async () => {
    const rows = await storage.query('nonexistent', () => true);
    expect(rows).toEqual([]);
  });

  it('delete returns empty array for unknown table', async () => {
    const deleted = await storage.delete('nonexistent', () => true);
    expect(deleted).toEqual([]);
  });

  it('update returns empty array for unknown table', async () => {
    const updated = await storage.update('nonexistent', { x: 1 }, () => true);
    expect(updated).toEqual([]);
  });

  it('getColumns returns empty array for unknown table', async () => {
    const cols = await storage.getColumns('nonexistent');
    expect(cols).toEqual([]);
  });

  it('nextId returns 1 for unknown table', async () => {
    const id = await storage.nextId('nonexistent');
    expect(id).toBe(1);
  });

  it('insertMany inserts all rows', async () => {
    const rows = await storage.insertMany('users', [
      { name: 'Alice' },
      { name: 'Bob' },
    ]);

    expect(rows).toHaveLength(2);
    expect(rows[0].id).toBe(1);
    expect(rows[1].id).toBe(2);

    const allRows = await storage.getAll('users');
    expect(allRows).toHaveLength(2);
  });
});
