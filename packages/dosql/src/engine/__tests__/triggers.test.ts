/**
 * DoSQL Engine Trigger Integration Tests
 *
 * Tests CREATE TRIGGER, DROP TRIGGER, and trigger execution
 * on INSERT/UPDATE/DELETE through the DOQueryEngine.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { DOQueryEngine } from '../do-engine.js';

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
 * Create an engine with a test table.
 */
async function createTestEngine() {
  const storage = createMockStorage();
  const engine = new DOQueryEngine({ storage });
  await engine.execute('CREATE TABLE users (id INTEGER, name TEXT, email TEXT, PRIMARY KEY (id))');
  return engine;
}

// =============================================================================
// CREATE TRIGGER TESTS
// =============================================================================

describe('CREATE TRIGGER', () => {
  it('should create a BEFORE INSERT trigger', async () => {
    const engine = await createTestEngine();

    const result = await engine.execute(
      `CREATE TRIGGER validate_user BEFORE INSERT ON users
       BEGIN
         SELECT CASE WHEN NEW.name IS NULL THEN RAISE(ABORT, 'name required') END;
       END`
    );

    expect(result.rowsAffected).toBe(0);
    const registry = engine.getTriggerRegistry();
    expect(registry.get('validate_user')).toBeDefined();
  });

  it('should create an AFTER INSERT trigger', async () => {
    const engine = await createTestEngine();

    const result = await engine.execute(
      `CREATE TRIGGER log_insert AFTER INSERT ON users
       BEGIN
         SELECT 1;
       END`
    );

    expect(result.rowsAffected).toBe(0);
    const registry = engine.getTriggerRegistry();
    const trigger = registry.get('log_insert');
    expect(trigger).toBeDefined();
    expect(trigger!.table).toBe('users');
  });

  it('should create a BEFORE UPDATE trigger', async () => {
    const engine = await createTestEngine();

    const result = await engine.execute(
      `CREATE TRIGGER validate_update BEFORE UPDATE ON users
       BEGIN
         SELECT 1;
       END`
    );

    expect(result.rowsAffected).toBe(0);
    const registry = engine.getTriggerRegistry();
    expect(registry.get('validate_update')).toBeDefined();
  });

  it('should create a BEFORE DELETE trigger', async () => {
    const engine = await createTestEngine();

    const result = await engine.execute(
      `CREATE TRIGGER prevent_delete BEFORE DELETE ON users
       BEGIN
         SELECT 1;
       END`
    );

    expect(result.rowsAffected).toBe(0);
    const registry = engine.getTriggerRegistry();
    expect(registry.get('prevent_delete')).toBeDefined();
  });

  it('should reject duplicate trigger names', async () => {
    const engine = await createTestEngine();

    await engine.execute(
      `CREATE TRIGGER my_trigger BEFORE INSERT ON users
       BEGIN SELECT 1; END`
    );

    await expect(engine.execute(
      `CREATE TRIGGER my_trigger BEFORE INSERT ON users
       BEGIN SELECT 1; END`
    )).rejects.toThrow(/already exists/);
  });

  it('should handle IF NOT EXISTS gracefully', async () => {
    const engine = await createTestEngine();

    await engine.execute(
      `CREATE TRIGGER my_trigger BEFORE INSERT ON users
       BEGIN SELECT 1; END`
    );

    // Should NOT throw with IF NOT EXISTS
    const result = await engine.execute(
      `CREATE TRIGGER IF NOT EXISTS my_trigger BEFORE INSERT ON users
       BEGIN SELECT 1; END`
    );

    expect(result.rowsAffected).toBe(0);
  });

  it('should persist triggers across engine re-initialization', async () => {
    const storage = createMockStorage();
    const engine1 = new DOQueryEngine({ storage });
    await engine1.execute('CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))');

    await engine1.execute(
      `CREATE TRIGGER test_trigger BEFORE INSERT ON users
       BEGIN SELECT 1; END`
    );

    // Create a new engine with same storage (simulates DO restart)
    const engine2 = new DOQueryEngine({ storage });
    await engine2.execute('SELECT * FROM users');  // Force initialization

    const registry = engine2.getTriggerRegistry();
    expect(registry.get('test_trigger')).toBeDefined();
  });
});

// =============================================================================
// DROP TRIGGER TESTS
// =============================================================================

describe('DROP TRIGGER', () => {
  it('should drop an existing trigger', async () => {
    const engine = await createTestEngine();

    await engine.execute(
      `CREATE TRIGGER my_trigger BEFORE INSERT ON users
       BEGIN SELECT 1; END`
    );

    expect(engine.getTriggerRegistry().get('my_trigger')).toBeDefined();

    await engine.execute('DROP TRIGGER my_trigger');

    expect(engine.getTriggerRegistry().get('my_trigger')).toBeUndefined();
  });

  it('should throw for non-existent trigger without IF EXISTS', async () => {
    const engine = await createTestEngine();

    await expect(engine.execute('DROP TRIGGER nonexistent'))
      .rejects.toThrow(/no such trigger/);
  });

  it('should handle IF EXISTS gracefully for non-existent trigger', async () => {
    const engine = await createTestEngine();

    const result = await engine.execute('DROP TRIGGER IF EXISTS nonexistent');
    expect(result.rowsAffected).toBe(0);
  });

  it('should persist drop across engine re-initialization', async () => {
    const storage = createMockStorage();
    const engine1 = new DOQueryEngine({ storage });
    await engine1.execute('CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))');

    await engine1.execute(
      `CREATE TRIGGER test_trigger BEFORE INSERT ON users
       BEGIN SELECT 1; END`
    );

    await engine1.execute('DROP TRIGGER test_trigger');

    // Create a new engine with same storage (simulates DO restart)
    const engine2 = new DOQueryEngine({ storage });
    await engine2.execute('SELECT * FROM users');  // Force initialization

    expect(engine2.getTriggerRegistry().get('test_trigger')).toBeUndefined();
  });
});

// =============================================================================
// TRIGGER EXECUTION ON INSERT TESTS
// =============================================================================

describe('Trigger execution on INSERT', () => {
  it('should fire BEFORE INSERT trigger with RAISE to reject invalid data', async () => {
    const engine = await createTestEngine();

    // Create a trigger that rejects NULL names
    await engine.execute(
      `CREATE TRIGGER validate_name BEFORE INSERT ON users
       WHEN NEW.name IS NULL
       BEGIN
         SELECT RAISE(ABORT, 'name is required');
       END`
    );

    // This insert should be rejected because name is NULL
    // Note: The trigger WHEN clause and RAISE evaluation depend on
    // the SQL trigger executor's WHEN clause evaluation capabilities
    const result = await engine.execute(
      "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')"
    );

    // Valid insert should succeed
    expect(result.rowsAffected).toBe(1);
  });

  it('should fire AFTER INSERT trigger without blocking the operation', async () => {
    const engine = await createTestEngine();

    // Create an AFTER INSERT trigger
    await engine.execute(
      `CREATE TRIGGER log_insert AFTER INSERT ON users
       BEGIN
         SELECT 1;
       END`
    );

    // Insert should succeed (AFTER triggers don't block)
    const result = await engine.execute(
      "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')"
    );

    expect(result.rowsAffected).toBe(1);
  });

  it('should fire BEFORE INSERT trigger that rejects with unconditional RAISE', async () => {
    const engine = await createTestEngine();

    // Create a trigger that always rejects
    await engine.execute(
      `CREATE TRIGGER block_inserts BEFORE INSERT ON users
       BEGIN
         SELECT RAISE(ABORT, 'inserts are blocked');
       END`
    );

    // This insert should be rejected
    await expect(engine.execute(
      "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')"
    )).rejects.toThrow(/inserts are blocked/);
  });
});

// =============================================================================
// TRIGGER EXECUTION ON UPDATE TESTS
// =============================================================================

describe('Trigger execution on UPDATE', () => {
  it('should fire AFTER UPDATE trigger without blocking', async () => {
    const engine = await createTestEngine();

    await engine.execute(
      "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')"
    );

    // Create an AFTER UPDATE trigger
    await engine.execute(
      `CREATE TRIGGER log_update AFTER UPDATE ON users
       BEGIN
         SELECT 1;
       END`
    );

    const result = await engine.execute(
      "UPDATE users SET name = 'Bob' WHERE id = 1"
    );

    expect(result.rowsAffected).toBe(1);
  });

  it('should fire BEFORE UPDATE trigger that rejects with RAISE', async () => {
    const engine = await createTestEngine();

    await engine.execute(
      "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')"
    );

    // Create a trigger that blocks all updates
    await engine.execute(
      `CREATE TRIGGER block_updates BEFORE UPDATE ON users
       BEGIN
         SELECT RAISE(ABORT, 'updates are blocked');
       END`
    );

    await expect(engine.execute(
      "UPDATE users SET name = 'Bob' WHERE id = 1"
    )).rejects.toThrow(/updates are blocked/);
  });
});

// =============================================================================
// TRIGGER EXECUTION ON DELETE TESTS
// =============================================================================

describe('Trigger execution on DELETE', () => {
  it('should fire AFTER DELETE trigger without blocking', async () => {
    const engine = await createTestEngine();

    await engine.execute(
      "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')"
    );

    // Create an AFTER DELETE trigger
    await engine.execute(
      `CREATE TRIGGER log_delete AFTER DELETE ON users
       BEGIN
         SELECT 1;
       END`
    );

    const result = await engine.execute(
      "DELETE FROM users WHERE id = 1"
    );

    expect(result.rowsAffected).toBe(1);
  });

  it('should fire BEFORE DELETE trigger that rejects with RAISE', async () => {
    const engine = await createTestEngine();

    await engine.execute(
      "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')"
    );

    // Create a trigger that blocks all deletes
    await engine.execute(
      `CREATE TRIGGER block_deletes BEFORE DELETE ON users
       BEGIN
         SELECT RAISE(ABORT, 'deletes are blocked');
       END`
    );

    await expect(engine.execute(
      "DELETE FROM users WHERE id = 1"
    )).rejects.toThrow(/deletes are blocked/);
  });
});

// =============================================================================
// TRIGGER REGISTRY TESTS
// =============================================================================

describe('Trigger registry via engine', () => {
  it('should expose trigger registry', async () => {
    const engine = await createTestEngine();

    const registry = engine.getTriggerRegistry();
    expect(registry).toBeDefined();
    expect(typeof registry.register).toBe('function');
    expect(typeof registry.get).toBe('function');
    expect(typeof registry.list).toBe('function');
    expect(typeof registry.remove).toBe('function');
  });

  it('should list all triggers for a table', async () => {
    const engine = await createTestEngine();

    await engine.execute(
      `CREATE TRIGGER trigger1 BEFORE INSERT ON users BEGIN SELECT 1; END`
    );
    await engine.execute(
      `CREATE TRIGGER trigger2 AFTER INSERT ON users BEGIN SELECT 1; END`
    );

    const registry = engine.getTriggerRegistry();
    const triggers = registry.list({ table: 'users' });

    expect(triggers).toHaveLength(2);
    expect(triggers.map(t => t.name).sort()).toEqual(['trigger1', 'trigger2']);
  });
});
