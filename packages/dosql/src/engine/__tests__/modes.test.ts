/**
 * DoSQL Engine Mode Separation Tests
 *
 * Tests for the read/write mode separation:
 * - Mode enforcement
 * - Read-only rejection of writes
 * - Write routing to DO
 *
 * Uses workers-vitest-pool for real Workers environment testing.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  QueryMode,
  ReadOnlyError,
  isWriteOperation,
  extractOperation,
  ModeEnforcer,
  isReadOnlyError,
  createModeEnforcer,
} from '../modes.js';
import { WorkerQueryEngine, createWorkerEngine } from '../worker-engine.js';
import { DOQueryEngine, createDOEngine } from '../do-engine.js';
import {
  createQueryEngine,
  createReadOnlyEngine,
  createReadWriteEngine,
  isWorkerEngine,
  isDOEngine,
  isReadOnlyOptions,
  isReadWriteOptions,
} from '../factory.js';

// =============================================================================
// TEST UTILITIES - Mock R2 Types
// =============================================================================

/**
 * Minimal mock R2Object for testing
 */
interface MockR2Object {
  key: string;
  version: string;
  size: number;
  etag: string;
  httpEtag: string;
  checksums: Record<string, unknown>;
  uploaded: Date;
  httpMetadata?: Record<string, string>;
  customMetadata?: Record<string, string>;
  body?: ReadableStream;
  bodyUsed: boolean;
  arrayBuffer: () => Promise<ArrayBuffer>;
  text: () => Promise<string>;
  json: <T>() => Promise<T>;
  blob: () => Promise<Blob>;
  writeHttpMetadata: (headers: Headers) => void;
}

/**
 * Create a minimal mock R2Object
 */
function createMockR2Object(): MockR2Object {
  return {
    key: 'test-key',
    version: '1',
    size: 0,
    etag: 'test-etag',
    httpEtag: '"test-etag"',
    checksums: {},
    uploaded: new Date(),
    bodyUsed: false,
    arrayBuffer: async () => new ArrayBuffer(0),
    text: async () => '',
    json: async <T>() => ({} as T),
    blob: async () => new Blob(),
    writeHttpMetadata: () => {},
  };
}

/**
 * Minimal mock storage bucket for testing
 */
interface MockStorageBucket {
  get: (key: string) => Promise<null>;
  head: (key: string) => Promise<null>;
  put: (key: string, value: unknown) => Promise<MockR2Object>;
  delete: (keys: string | string[]) => Promise<void>;
  list: () => Promise<{ objects: unknown[]; truncated: boolean }>;
}

/**
 * Create a minimal mock storage bucket
 */
function createMockStorageBucket(): MockStorageBucket {
  return {
    get: async () => null,
    head: async () => null,
    put: async () => createMockR2Object(),
    delete: async () => {},
    list: async () => ({ objects: [], truncated: false }),
  };
}

// =============================================================================
// MODE DETECTION TESTS
// =============================================================================

describe('isWriteOperation', () => {
  describe('should detect write operations', () => {
    const writeOperations = [
      'INSERT INTO users (name) VALUES (\'Alice\')',
      'insert into users (name) values (\'Alice\')',
      'UPDATE users SET name = \'Bob\' WHERE id = 1',
      'update users set name = \'Bob\' where id = 1',
      'DELETE FROM users WHERE id = 1',
      'delete from users where id = 1',
      'CREATE TABLE users (id INT, name TEXT)',
      'create table users (id int, name text)',
      'ALTER TABLE users ADD COLUMN email TEXT',
      'alter table users add column email text',
      'DROP TABLE users',
      'drop table users',
      'TRUNCATE TABLE users',
      'truncate table users',
      'REPLACE INTO users (id, name) VALUES (1, \'Alice\')',
      'replace into users (id, name) values (1, \'Alice\')',
      'UPSERT INTO users (id, name) VALUES (1, \'Alice\')',
      'MERGE INTO users USING source ON users.id = source.id',
    ];

    for (const sql of writeOperations) {
      it(`should detect "${sql.substring(0, 30)}..." as a write operation`, () => {
        expect(isWriteOperation(sql)).toBe(true);
      });
    }
  });

  describe('should detect read operations', () => {
    const readOperations = [
      'SELECT * FROM users',
      'select * from users',
      'SELECT id, name FROM users WHERE id = 1',
      'SELECT COUNT(*) FROM users',
      'SELECT * FROM users JOIN orders ON users.id = orders.user_id',
      'WITH cte AS (SELECT * FROM users) SELECT * FROM cte',
      'EXPLAIN SELECT * FROM users',
      'SHOW TABLES',
      'DESCRIBE users',
      'PRAGMA table_info(users)',
    ];

    for (const sql of readOperations) {
      it(`should not detect "${sql.substring(0, 30)}..." as a write operation`, () => {
        expect(isWriteOperation(sql)).toBe(false);
      });
    }
  });

  describe('should handle edge cases', () => {
    it('should handle leading whitespace', () => {
      expect(isWriteOperation('  INSERT INTO users (name) VALUES (\'Alice\')')).toBe(true);
      expect(isWriteOperation('\nINSERT INTO users (name) VALUES (\'Alice\')')).toBe(true);
      expect(isWriteOperation('\t  INSERT INTO users (name) VALUES (\'Alice\')')).toBe(true);
    });

    it('should handle SQL comments', () => {
      expect(isWriteOperation('-- comment\nSELECT * FROM users')).toBe(false);
      expect(isWriteOperation('-- comment\nINSERT INTO users (name) VALUES (\'Alice\')')).toBe(true);
    });

    it('should not be fooled by keywords in column names', () => {
      expect(isWriteOperation('SELECT insert_date FROM users')).toBe(false);
      expect(isWriteOperation('SELECT update_count FROM users')).toBe(false);
      expect(isWriteOperation('SELECT delete_flag FROM users')).toBe(false);
    });
  });
});

describe('extractOperation', () => {
  it('should extract operation type from SQL', () => {
    expect(extractOperation('SELECT * FROM users')).toBe('SELECT');
    expect(extractOperation('INSERT INTO users (name) VALUES (\'Alice\')')).toBe('INSERT');
    expect(extractOperation('UPDATE users SET name = \'Bob\'')).toBe('UPDATE');
    expect(extractOperation('DELETE FROM users WHERE id = 1')).toBe('DELETE');
    expect(extractOperation('CREATE TABLE users (id INT)')).toBe('CREATE');
    expect(extractOperation('ALTER TABLE users ADD COLUMN email TEXT')).toBe('ALTER');
    expect(extractOperation('DROP TABLE users')).toBe('DROP');
  });

  it('should handle lowercase operations', () => {
    expect(extractOperation('select * from users')).toBe('SELECT');
    expect(extractOperation('insert into users (name) values (\'Alice\')')).toBe('INSERT');
  });

  it('should return null for invalid SQL', () => {
    expect(extractOperation('')).toBe(null);
    expect(extractOperation('   ')).toBe(null);
  });
});

// =============================================================================
// READ ONLY ERROR TESTS
// =============================================================================

describe('ReadOnlyError', () => {
  it('should create error with correct message', () => {
    const error = new ReadOnlyError('INSERT');
    expect(error.message).toBe(
      'Cannot execute INSERT in read-only mode. Writes must go through a Durable Object.'
    );
    expect(error.operation).toBe('INSERT');
    expect(error.name).toBe('ReadOnlyError');
  });

  it('should be serializable to JSON', () => {
    const error = new ReadOnlyError('UPDATE');
    const json = error.toJSON();
    expect(json.name).toBe('ReadOnlyError');
    expect(json.operation).toBe('UPDATE');
    expect(json.message).toContain('UPDATE');
  });

  it('should be identifiable with type guard', () => {
    const readOnlyError = new ReadOnlyError('DELETE');
    const regularError = new Error('test');

    expect(isReadOnlyError(readOnlyError)).toBe(true);
    expect(isReadOnlyError(regularError)).toBe(false);
    expect(isReadOnlyError(null)).toBe(false);
    expect(isReadOnlyError('string')).toBe(false);
  });
});

// =============================================================================
// MODE ENFORCER TESTS
// =============================================================================

describe('ModeEnforcer', () => {
  describe('READ_ONLY mode', () => {
    let enforcer: ModeEnforcer;

    beforeEach(() => {
      enforcer = new ModeEnforcer(QueryMode.READ_ONLY);
    });

    it('should report mode correctly', () => {
      expect(enforcer.getMode()).toBe(QueryMode.READ_ONLY);
      expect(enforcer.isReadOnly()).toBe(true);
    });

    it('should allow read operations', () => {
      expect(() => enforcer.enforce('SELECT * FROM users')).not.toThrow();
      expect(() => enforcer.enforce('SELECT id FROM users WHERE name = ?')).not.toThrow();
      expect(() => enforcer.enforce('EXPLAIN SELECT * FROM users')).not.toThrow();
    });

    it('should reject write operations', () => {
      expect(() => enforcer.enforce('INSERT INTO users (name) VALUES (?)')).toThrow(ReadOnlyError);
      expect(() => enforcer.enforce('UPDATE users SET name = ?')).toThrow(ReadOnlyError);
      expect(() => enforcer.enforce('DELETE FROM users WHERE id = ?')).toThrow(ReadOnlyError);
      expect(() => enforcer.enforce('CREATE TABLE users (id INT)')).toThrow(ReadOnlyError);
      expect(() => enforcer.enforce('DROP TABLE users')).toThrow(ReadOnlyError);
    });

    it('should throw ReadOnlyError with correct operation', () => {
      try {
        enforcer.enforce('INSERT INTO users (name) VALUES (\'Alice\')');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ReadOnlyError);
        expect((error as ReadOnlyError).operation).toBe('INSERT');
      }
    });

    it('should check if operations are allowed', () => {
      expect(enforcer.isAllowed('SELECT * FROM users')).toBe(true);
      expect(enforcer.isAllowed('INSERT INTO users (name) VALUES (?)')).toBe(false);
    });

    it('should classify and enforce operations', () => {
      const readResult = enforcer.enforceAndClassify('SELECT * FROM users');
      expect(readResult.isWrite).toBe(false);
      expect(readResult.operation).toBe('SELECT');

      expect(() => enforcer.enforceAndClassify('INSERT INTO users (name) VALUES (?)'))
        .toThrow(ReadOnlyError);
    });
  });

  describe('READ_WRITE mode', () => {
    let enforcer: ModeEnforcer;

    beforeEach(() => {
      enforcer = new ModeEnforcer(QueryMode.READ_WRITE);
    });

    it('should report mode correctly', () => {
      expect(enforcer.getMode()).toBe(QueryMode.READ_WRITE);
      expect(enforcer.isReadOnly()).toBe(false);
    });

    it('should allow all operations', () => {
      expect(() => enforcer.enforce('SELECT * FROM users')).not.toThrow();
      expect(() => enforcer.enforce('INSERT INTO users (name) VALUES (?)')).not.toThrow();
      expect(() => enforcer.enforce('UPDATE users SET name = ?')).not.toThrow();
      expect(() => enforcer.enforce('DELETE FROM users WHERE id = ?')).not.toThrow();
      expect(() => enforcer.enforce('CREATE TABLE users (id INT)')).not.toThrow();
      expect(() => enforcer.enforce('DROP TABLE users')).not.toThrow();
    });

    it('should classify operations correctly', () => {
      const readResult = enforcer.enforceAndClassify('SELECT * FROM users');
      expect(readResult.isWrite).toBe(false);
      expect(readResult.operation).toBe('SELECT');

      const writeResult = enforcer.enforceAndClassify('INSERT INTO users (name) VALUES (?)');
      expect(writeResult.isWrite).toBe(true);
      expect(writeResult.operation).toBe('INSERT');
    });
  });
});

describe('createModeEnforcer', () => {
  it('should create enforcer with options', () => {
    const enforcer = createModeEnforcer({ mode: QueryMode.READ_ONLY });
    expect(enforcer.getMode()).toBe(QueryMode.READ_ONLY);
  });
});

// =============================================================================
// WORKER QUERY ENGINE TESTS
// =============================================================================

describe('WorkerQueryEngine', () => {
  describe('mode enforcement', () => {
    it('should report read-only mode', () => {
      // Create a minimal mock R2Bucket
      const mockBucket = createMockStorageBucket();

      const engine = new WorkerQueryEngine({
        storage: mockBucket,
      });

      expect(engine.getMode()).toBe(QueryMode.READ_ONLY);
      expect(engine.hasDoStub()).toBe(false);
    });

    it('should reject write operations', async () => {
      const mockBucket = createMockStorageBucket();

      const engine = new WorkerQueryEngine({
        storage: mockBucket,
      });

      await expect(engine.query('INSERT INTO users (name) VALUES (?)', ['Alice']))
        .rejects.toThrow(ReadOnlyError);

      await expect(engine.query('UPDATE users SET name = ?', ['Bob']))
        .rejects.toThrow(ReadOnlyError);

      await expect(engine.query('DELETE FROM users WHERE id = ?', [1]))
        .rejects.toThrow(ReadOnlyError);
    });

    it('should allow read operations', async () => {
      const mockBucket = createMockStorageBucket();

      const engine = new WorkerQueryEngine({
        storage: mockBucket,
      });

      // Should not throw
      const result = await engine.query('SELECT * FROM users');
      expect(result.rows).toEqual([]);
    });
  });

  describe('write routing', () => {
    it('should fail without DO stub', async () => {
      const mockBucket = createMockStorageBucket();

      const engine = new WorkerQueryEngine({
        storage: mockBucket,
      });

      await expect(engine.write('INSERT INTO users (name) VALUES (?)', ['Alice']))
        .rejects.toThrow('No Durable Object stub configured for writes');
    });

    it('should fail if write() called with non-write operation', async () => {
      const mockBucket = createMockStorageBucket();

      // Create a mock DO stub
      const mockDoStub = {
        id: { toString: () => 'test-id' },
        fetch: async () => new Response(JSON.stringify({ success: true, rowsAffected: 1 })),
      };

      const engine = new WorkerQueryEngine({
        storage: mockBucket,
        doStub: mockDoStub,
      });

      await expect(engine.write('SELECT * FROM users'))
        .rejects.toThrow('Expected a write operation');
    });

    it('should route writes to DO stub', async () => {
      const mockBucket = createMockStorageBucket();

      let receivedRequest: Request | null = null;

      const mockDoStub = {
        id: { toString: () => 'test-id' },
        fetch: async (request: Request) => {
          receivedRequest = request;
          return new Response(JSON.stringify({ success: true, rowsAffected: 1 }));
        },
      };

      const engine = new WorkerQueryEngine({
        storage: mockBucket,
        doStub: mockDoStub,
      });

      const result = await engine.write('INSERT INTO users (name) VALUES (?)', ['Alice']);

      expect(result.success).toBe(true);
      expect(result.rowsAffected).toBe(1);
      expect(receivedRequest).not.toBeNull();
      expect(receivedRequest!.method).toBe('POST');
    });

    it('should handle DO stub errors', async () => {
      const mockBucket = createMockStorageBucket();

      const mockDoStub = {
        id: { toString: () => 'test-id' },
        fetch: async () => new Response('Internal error', { status: 500 }),
      };

      const engine = new WorkerQueryEngine({
        storage: mockBucket,
        doStub: mockDoStub,
      });

      const result = await engine.write('INSERT INTO users (name) VALUES (?)', ['Alice']);

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });
  });

  describe('cache', () => {
    it('should cache query results', async () => {
      let queryCount = 0;

      const mockIndexReader = {
        query: async <T>() => {
          queryCount++;
          return { rows: [{ id: 1, name: 'Alice' }] as T[], columns: [] };
        },
        getSchema: async () => [],
        close: async () => {},
      };

      const engine = new WorkerQueryEngine({
        storage: mockIndexReader,
        cache: { enabled: true, ttlMs: 10000 },
      });

      // First query should hit storage
      await engine.query('SELECT * FROM users');
      expect(queryCount).toBe(1);

      // Second query should hit cache
      await engine.query('SELECT * FROM users');
      expect(queryCount).toBe(1); // Still 1

      // Different query should hit storage
      await engine.query('SELECT * FROM orders');
      expect(queryCount).toBe(2);
    });

    it('should respect cache TTL', async () => {
      let queryCount = 0;

      const mockIndexReader = {
        query: async <T>() => {
          queryCount++;
          return { rows: [{ id: 1 }] as T[], columns: [] };
        },
        getSchema: async () => [],
        close: async () => {},
      };

      const engine = new WorkerQueryEngine({
        storage: mockIndexReader,
        cache: { enabled: true, ttlMs: 1 }, // 1ms TTL
      });

      await engine.query('SELECT * FROM users');
      expect(queryCount).toBe(1);

      // Wait for TTL to expire
      await new Promise(resolve => setTimeout(resolve, 5));

      await engine.query('SELECT * FROM users');
      expect(queryCount).toBe(2); // Should have hit storage again
    });

    it('should provide cache stats', () => {
      const mockBucket = createMockStorageBucket();

      const engine = new WorkerQueryEngine({
        storage: mockBucket,
        cache: { enabled: true, maxEntries: 100 },
      });

      const stats = engine.getCacheStats();
      expect(stats.size).toBe(0);
      expect(stats.maxSize).toBe(100);
    });
  });

  describe('factory function', () => {
    it('createWorkerEngine should create WorkerQueryEngine', () => {
      const mockBucket = createMockStorageBucket();

      const engine = createWorkerEngine({ storage: mockBucket });
      expect(engine).toBeInstanceOf(WorkerQueryEngine);
    });
  });
});

// =============================================================================
// DO QUERY ENGINE TESTS
// =============================================================================

describe('DOQueryEngine', () => {
  /**
   * Create a mock DurableObjectStorage for testing.
   */
  function createMockStorage() {
    const data = new Map<string, any>();

    return {
      get: async <T>(key: string | string[]): Promise<T | Map<string, T> | undefined> => {
        if (Array.isArray(key)) {
          const result = new Map<string, T>();
          for (const k of key) {
            const value = data.get(k);
            if (value !== undefined) {
              result.set(k, value);
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
            result.set(key, value);
          }
        }
        return result;
      },
      transaction: async <T>(closure: (txn: any) => Promise<T>): Promise<T> => {
        // Simplified - just execute the closure
        return closure({
          get: async (k: string) => data.get(k),
          put: async (k: string, v: any) => data.set(k, v),
          delete: async (k: string) => data.delete(k),
        });
      },
      deleteAll: async () => { data.clear(); },
      getAlarm: async () => null,
      setAlarm: async () => {},
      deleteAlarm: async () => {},
    };
  }

  describe('mode', () => {
    it('should report read-write mode', async () => {
      const storage = createMockStorage();
      const engine = new DOQueryEngine({ storage });

      expect(engine.getMode()).toBe(QueryMode.READ_WRITE);
    });
  });

  describe('CRUD operations', () => {
    it('should create tables', async () => {
      const storage = createMockStorage();
      const engine = new DOQueryEngine({ storage });

      const result = await engine.execute('CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))');

      expect(result.rows).toEqual([]);
      expect(result.rowsAffected).toBe(0);

      const schema = engine.getSchema();
      expect(schema.tables.has('users')).toBe(true);
    });

    it('should insert rows', async () => {
      const storage = createMockStorage();
      const engine = new DOQueryEngine({ storage });

      await engine.execute('CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))');
      const result = await engine.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");

      expect(result.rowsAffected).toBe(1);
    });

    it('should select rows', async () => {
      const storage = createMockStorage();
      const engine = new DOQueryEngine({ storage });

      await engine.execute('CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))');
      await engine.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
      await engine.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')");

      const users = await engine.query('SELECT * FROM users');

      expect(users.length).toBe(2);
    });

    it('should update rows', async () => {
      const storage = createMockStorage();
      const engine = new DOQueryEngine({ storage });

      await engine.execute('CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))');
      await engine.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");

      const result = await engine.execute("UPDATE users SET name = 'Alicia' WHERE id = 1");

      expect(result.rowsAffected).toBe(1);

      const users = await engine.query("SELECT * FROM users WHERE id = 1");
      expect(users[0]).toEqual({ id: 1, name: 'Alicia' });
    });

    it('should delete rows', async () => {
      const storage = createMockStorage();
      const engine = new DOQueryEngine({ storage });

      await engine.execute('CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))');
      await engine.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
      await engine.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')");

      const result = await engine.execute('DELETE FROM users WHERE id = 1');

      expect(result.rowsAffected).toBe(1);

      const users = await engine.query('SELECT * FROM users');
      expect(users.length).toBe(1);
      expect(users[0]).toEqual({ id: 2, name: 'Bob' });
    });

    it('should use queryOne to get single row', async () => {
      const storage = createMockStorage();
      const engine = new DOQueryEngine({ storage });

      await engine.execute('CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))');
      await engine.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");

      const user = await engine.queryOne('SELECT * FROM users WHERE id = 1');

      expect(user).toEqual({ id: 1, name: 'Alice' });
    });

    it('should return null from queryOne when no match', async () => {
      const storage = createMockStorage();
      const engine = new DOQueryEngine({ storage });

      await engine.execute('CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))');

      const user = await engine.queryOne('SELECT * FROM users WHERE id = 999');

      expect(user).toBeNull();
    });
  });

  describe('transactions', () => {
    it('should execute transactions', async () => {
      const storage = createMockStorage();
      const engine = new DOQueryEngine({ storage });

      await engine.execute('CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))');
      await engine.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");

      // Execute a transaction that updates the user
      const result = await engine.transaction(async (tx) => {
        await tx.execute("UPDATE users SET name = 'Alicia' WHERE id = 1");
        // Query after update within same transaction
        const user = await tx.queryOne('SELECT * FROM users WHERE id = 1');
        return user;
      });

      // The transaction should have updated the name
      expect(result).toEqual({ id: 1, name: 'Alicia' });

      // Verify the update persisted outside the transaction
      const updated = await engine.queryOne('SELECT * FROM users WHERE id = 1');
      expect(updated).toEqual({ id: 1, name: 'Alicia' });
    });

    it('should track active transactions', async () => {
      const storage = createMockStorage();
      const engine = new DOQueryEngine({ storage });

      expect(engine.getActiveTransactionCount()).toBe(0);
    });
  });

  describe('factory function', () => {
    it('createDOEngine should create DOQueryEngine', () => {
      const storage = createMockStorage();
      const engine = createDOEngine({ storage });
      expect(engine).toBeInstanceOf(DOQueryEngine);
    });
  });
});

// =============================================================================
// FACTORY TESTS
// =============================================================================

describe('createQueryEngine', () => {
  describe('read-only mode', () => {
    it('should create WorkerQueryEngine', () => {
      const mockBucket = createMockStorageBucket();

      const engine = createQueryEngine({
        mode: 'read-only',
        storage: mockBucket,
      });

      expect(engine).toBeInstanceOf(WorkerQueryEngine);
      expect(isWorkerEngine(engine)).toBe(true);
      expect(isDOEngine(engine)).toBe(false);
    });
  });

  describe('read-write mode', () => {
    it('should create DOQueryEngine', () => {
      const mockStorage = {
        get: async () => undefined,
        put: async () => {},
        delete: async () => false,
        list: async () => new Map(),
        transaction: async <T>(fn: any) => fn({}),
        deleteAll: async () => {},
        getAlarm: async () => null,
        setAlarm: async () => {},
        deleteAlarm: async () => {},
      };

      const engine = createQueryEngine({
        mode: 'read-write',
        storage: mockStorage,
      });

      expect(engine).toBeInstanceOf(DOQueryEngine);
      expect(isDOEngine(engine)).toBe(true);
      expect(isWorkerEngine(engine)).toBe(false);
    });
  });
});

describe('createReadOnlyEngine', () => {
  it('should create WorkerQueryEngine', () => {
    const mockBucket = createMockStorageBucket();

    const engine = createReadOnlyEngine(mockBucket);

    expect(engine).toBeInstanceOf(WorkerQueryEngine);
  });
});

/**
 * Minimal mock DO storage for testing
 */
interface MockDOStorage {
  get: (key: string) => Promise<unknown>;
  put: (key: string, value: unknown) => Promise<void>;
  delete: (keys: string | string[]) => Promise<boolean>;
  list: () => Promise<Map<string, unknown>>;
  transaction: <T>(fn: (txn: MockDOStorage) => Promise<T>) => Promise<T>;
  deleteAll: () => Promise<void>;
  getAlarm: () => Promise<number | null>;
  setAlarm: (time: number) => Promise<void>;
  deleteAlarm: () => Promise<void>;
}

function createMockDOStorage(): MockDOStorage {
  return {
    get: async () => undefined,
    put: async () => {},
    delete: async () => false,
    list: async () => new Map(),
    transaction: async <T>(fn: (txn: MockDOStorage) => Promise<T>) => fn({} as MockDOStorage),
    deleteAll: async () => {},
    getAlarm: async () => null,
    setAlarm: async () => {},
    deleteAlarm: async () => {},
  };
}

describe('createReadWriteEngine', () => {
  it('should create DOQueryEngine', () => {
    const mockStorage = createMockDOStorage();

    const engine = createReadWriteEngine(mockStorage);

    expect(engine).toBeInstanceOf(DOQueryEngine);
  });
});

describe('type guards for options', () => {
  it('isReadOnlyOptions should identify read-only options', () => {
    const mockBucket = createMockStorageBucket();

    expect(isReadOnlyOptions({ mode: 'read-only', storage: mockBucket })).toBe(true);
    expect(isReadOnlyOptions({ mode: 'read-write', storage: createMockDOStorage() })).toBe(false);
  });

  it('isReadWriteOptions should identify read-write options', () => {
    const mockStorage = createMockDOStorage();

    expect(isReadWriteOptions({ mode: 'read-write', storage: mockStorage })).toBe(true);
    expect(isReadWriteOptions({ mode: 'read-only', storage: createMockStorageBucket() })).toBe(false);
  });
});
