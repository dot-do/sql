/**
 * DoSQL RETURNING Clause Execution Tests
 *
 * Tests that INSERT, UPDATE, DELETE statements with RETURNING clauses
 * actually return the affected rows through the DOQueryEngine.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { DOQueryEngine, createDOEngine } from '../do-engine.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Create a mock DurableObjectStorage for testing.
 */
function createMockStorage() {
  const data = new Map<string, unknown>();

  return {
    get: async <T>(key: string | string[]): Promise<T | Map<string, T> | undefined> => {
      if (Array.isArray(key)) {
        const result = new Map<string, T>();
        for (const k of key) {
          const value = data.get(k);
          if (value !== undefined) {
            result.set(k, value as T);
          }
        }
        return result;
      }
      return data.get(key) as T | undefined;
    },
    put: async <T>(key: string | Record<string, T>, value?: T) => {
      if (typeof key === 'string') {
        data.set(key, value);
      } else {
        for (const [k, v] of Object.entries(key)) {
          data.set(k, v);
        }
      }
    },
    delete: async (key: string | string[]): Promise<boolean | number> => {
      if (Array.isArray(key)) {
        let count = 0;
        for (const k of key) {
          if (data.delete(k)) count++;
        }
        return count;
      }
      return data.delete(key);
    },
    list: async <T>(options?: { prefix?: string }): Promise<Map<string, T>> => {
      const result = new Map<string, T>();
      for (const [key, value] of data) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          result.set(key, value as T);
        }
      }
      return result;
    },
    transaction: async <T>(closure: (txn: unknown) => Promise<T>): Promise<T> => {
      return closure({
        get: async (k: string) => data.get(k),
        put: async (k: string, v: unknown) => data.set(k, v),
        delete: async (k: string) => data.delete(k),
      });
    },
    deleteAll: async () => { data.clear(); },
    getAlarm: async () => null,
    setAlarm: async () => {},
    deleteAlarm: async () => {},
  };
}

/**
 * Helper to create engine with a users table pre-populated.
 */
async function createEngineWithUsers() {
  const storage = createMockStorage();
  const engine = new DOQueryEngine({ storage });

  await engine.execute('CREATE TABLE users (id INTEGER, name TEXT, email TEXT, PRIMARY KEY (id))');

  return engine;
}

// =============================================================================
// INSERT ... RETURNING TESTS
// =============================================================================

describe('INSERT ... RETURNING', () => {
  it('should return all columns with RETURNING *', async () => {
    const engine = await createEngineWithUsers();

    const result = await engine.execute(
      "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com') RETURNING *"
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toEqual({
      id: 1,
      name: 'Alice',
      email: 'alice@example.com',
    });
  });

  it('should return specific columns with RETURNING col1, col2', async () => {
    const engine = await createEngineWithUsers();

    const result = await engine.execute(
      "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com') RETURNING id, name"
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({
      id: 1,
      name: 'Alice',
    });
    // Should not contain email since we only asked for id and name
    expect(result.rows[0]).not.toHaveProperty('email');
  });

  it('should return single column with RETURNING id', async () => {
    const engine = await createEngineWithUsers();

    const result = await engine.execute(
      "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com') RETURNING id"
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({ id: 1 });
    expect(result.rows[0]).not.toHaveProperty('name');
    expect(result.rows[0]).not.toHaveProperty('email');
  });

  it('should still insert the row when RETURNING is used', async () => {
    const engine = await createEngineWithUsers();

    await engine.execute(
      "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com') RETURNING *"
    );

    // Verify the row was actually inserted
    const users = await engine.query('SELECT * FROM users');
    expect(users).toHaveLength(1);
    expect(users[0]).toMatchObject({ id: 1, name: 'Alice' });
  });

  it('should return empty rows for INSERT without RETURNING', async () => {
    const engine = await createEngineWithUsers();

    const result = await engine.execute(
      "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"
    );

    expect(result.rows).toHaveLength(0);
  });
});

// =============================================================================
// UPDATE ... RETURNING TESTS
// =============================================================================

describe('UPDATE ... RETURNING', () => {
  it('should return updated rows with RETURNING *', async () => {
    const engine = await createEngineWithUsers();

    await engine.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

    const result = await engine.execute(
      "UPDATE users SET name = 'Alicia' WHERE id = 1 RETURNING *"
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({
      id: 1,
      name: 'Alicia',
      email: 'alice@example.com',
    });
  });

  it('should return specific columns with RETURNING col1, col2', async () => {
    const engine = await createEngineWithUsers();

    await engine.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

    const result = await engine.execute(
      "UPDATE users SET name = 'Alicia' WHERE id = 1 RETURNING id, name"
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({
      id: 1,
      name: 'Alicia',
    });
    expect(result.rows[0]).not.toHaveProperty('email');
  });

  it('should return empty when no rows match', async () => {
    const engine = await createEngineWithUsers();

    await engine.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

    const result = await engine.execute(
      "UPDATE users SET name = 'Nobody' WHERE id = 999 RETURNING *"
    );

    expect(result.rows).toHaveLength(0);
  });

  it('should return empty rows for UPDATE without RETURNING', async () => {
    const engine = await createEngineWithUsers();

    await engine.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

    const result = await engine.execute(
      "UPDATE users SET name = 'Alicia' WHERE id = 1"
    );

    expect(result.rows).toHaveLength(0);
    expect(result.rowsAffected).toBe(1);
  });
});

// =============================================================================
// DELETE ... RETURNING TESTS
// =============================================================================

describe('DELETE ... RETURNING', () => {
  it('should return deleted rows with RETURNING *', async () => {
    const engine = await createEngineWithUsers();

    await engine.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

    const result = await engine.execute(
      'DELETE FROM users WHERE id = 1 RETURNING *'
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({
      id: 1,
      name: 'Alice',
      email: 'alice@example.com',
    });
  });

  it('should return specific columns with RETURNING id, name', async () => {
    const engine = await createEngineWithUsers();

    await engine.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

    const result = await engine.execute(
      'DELETE FROM users WHERE id = 1 RETURNING id, name'
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({
      id: 1,
      name: 'Alice',
    });
    expect(result.rows[0]).not.toHaveProperty('email');
  });

  it('should actually delete the row when RETURNING is used', async () => {
    const engine = await createEngineWithUsers();

    await engine.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

    await engine.execute('DELETE FROM users WHERE id = 1 RETURNING *');

    // Verify the row was actually deleted
    const users = await engine.query('SELECT * FROM users');
    expect(users).toHaveLength(0);
  });

  it('should return empty when no rows match', async () => {
    const engine = await createEngineWithUsers();

    const result = await engine.execute(
      'DELETE FROM users WHERE id = 999 RETURNING *'
    );

    expect(result.rows).toHaveLength(0);
  });

  it('should return empty rows for DELETE without RETURNING', async () => {
    const engine = await createEngineWithUsers();

    await engine.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

    const result = await engine.execute('DELETE FROM users WHERE id = 1');

    expect(result.rows).toHaveLength(0);
    expect(result.rowsAffected).toBe(1);
  });
});
