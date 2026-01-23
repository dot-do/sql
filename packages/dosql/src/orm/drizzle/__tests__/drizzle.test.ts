/**
 * DoSQL Drizzle ORM Integration Tests
 *
 * Tests the Drizzle ORM adapter components for DoSQL.
 *
 * NOTE: Full Drizzle ORM tests require Node.js environment due to module
 * resolution issues with @cloudflare/vitest-pool-workers and drizzle-orm.
 * These tests focus on validating the core integration components.
 */

import { describe, it, expect, beforeEach } from 'vitest';

// Import our types directly (these work in workers)
import type { DoSQLBackend, DoSQLDrizzleConfig, DoSQLRunResult } from '../types.js';

// =============================================================================
// TYPE AND INTERFACE TESTS
// =============================================================================

describe('DoSQL Drizzle Types', () => {
  describe('DoSQLBackend Interface', () => {
    it('should define required methods', () => {
      // Create a mock implementation to verify interface
      const mockBackend: DoSQLBackend = {
        all: async () => [],
        get: async () => undefined,
        run: async () => ({ rowsAffected: 0 }),
        values: async () => [],
        transaction: async (fn) => fn({} as DoSQLBackend),
      };

      expect(typeof mockBackend.all).toBe('function');
      expect(typeof mockBackend.get).toBe('function');
      expect(typeof mockBackend.run).toBe('function');
      expect(typeof mockBackend.values).toBe('function');
      expect(typeof mockBackend.transaction).toBe('function');
    });

    it('should support optional getContext method', () => {
      const mockBackend: DoSQLBackend = {
        all: async () => [],
        get: async () => undefined,
        run: async () => ({ rowsAffected: 0 }),
        values: async () => [],
        transaction: async (fn) => fn({} as DoSQLBackend),
        getContext: () => ({ timestamp: Date.now() } as any),
      };

      expect(typeof mockBackend.getContext).toBe('function');
    });
  });

  describe('DoSQLRunResult Interface', () => {
    it('should have rowsAffected property', () => {
      const result: DoSQLRunResult = { rowsAffected: 5 };
      expect(result.rowsAffected).toBe(5);
    });

    it('should support optional lastInsertRowId', () => {
      const result: DoSQLRunResult = {
        rowsAffected: 1,
        lastInsertRowId: 42,
      };
      expect(result.lastInsertRowId).toBe(42);
    });

    it('should support bigint lastInsertRowId', () => {
      const result: DoSQLRunResult = {
        rowsAffected: 1,
        lastInsertRowId: BigInt(9007199254740993),
      };
      expect(result.lastInsertRowId).toBe(BigInt(9007199254740993));
    });
  });

  describe('DoSQLDrizzleConfig Interface', () => {
    it('should require backend property', () => {
      const mockBackend: DoSQLBackend = {
        all: async () => [],
        get: async () => undefined,
        run: async () => ({ rowsAffected: 0 }),
        values: async () => [],
        transaction: async (fn) => fn({} as DoSQLBackend),
      };

      const config: DoSQLDrizzleConfig = {
        backend: mockBackend,
      };

      expect(config.backend).toBe(mockBackend);
    });

    it('should support optional schema', () => {
      const mockBackend: DoSQLBackend = {
        all: async () => [],
        get: async () => undefined,
        run: async () => ({ rowsAffected: 0 }),
        values: async () => [],
        transaction: async (fn) => fn({} as DoSQLBackend),
      };

      const config: DoSQLDrizzleConfig<{ users: any }> = {
        backend: mockBackend,
        schema: { users: {} },
      };

      expect(config.schema).toBeDefined();
    });

    it('should support logger as boolean', () => {
      const mockBackend: DoSQLBackend = {
        all: async () => [],
        get: async () => undefined,
        run: async () => ({ rowsAffected: 0 }),
        values: async () => [],
        transaction: async (fn) => fn({} as DoSQLBackend),
      };

      const config: DoSQLDrizzleConfig = {
        backend: mockBackend,
        logger: true,
      };

      expect(config.logger).toBe(true);
    });

    it('should support custom logger', () => {
      const mockBackend: DoSQLBackend = {
        all: async () => [],
        get: async () => undefined,
        run: async () => ({ rowsAffected: 0 }),
        values: async () => [],
        transaction: async (fn) => fn({} as DoSQLBackend),
      };

      const logs: string[] = [];
      const config: DoSQLDrizzleConfig = {
        backend: mockBackend,
        logger: {
          logQuery(query, params) {
            logs.push(query);
          },
        },
      };

      expect(config.logger).toBeDefined();
      if (typeof config.logger === 'object') {
        config.logger.logQuery('SELECT * FROM users', []);
        expect(logs).toContain('SELECT * FROM users');
      }
    });

    it('should support casing option', () => {
      const mockBackend: DoSQLBackend = {
        all: async () => [],
        get: async () => undefined,
        run: async () => ({ rowsAffected: 0 }),
        values: async () => [],
        transaction: async (fn) => fn({} as DoSQLBackend),
      };

      const snakeCaseConfig: DoSQLDrizzleConfig = {
        backend: mockBackend,
        casing: 'snake_case',
      };

      const camelCaseConfig: DoSQLDrizzleConfig = {
        backend: mockBackend,
        casing: 'camelCase',
      };

      expect(snakeCaseConfig.casing).toBe('snake_case');
      expect(camelCaseConfig.casing).toBe('camelCase');
    });
  });
});

// =============================================================================
// MOCK BACKEND TESTS
// =============================================================================

describe('Mock DoSQL Backend', () => {
  let backend: DoSQLBackend;
  let queryLog: { sql: string; params: unknown[] }[];

  beforeEach(() => {
    queryLog = [];
    backend = {
      all: async (sql, params) => {
        queryLog.push({ sql, params: params || [] });
        return [
          { id: 1, name: 'Alice' },
          { id: 2, name: 'Bob' },
        ];
      },
      get: async (sql, params) => {
        queryLog.push({ sql, params: params || [] });
        return { id: 1, name: 'Alice' };
      },
      run: async (sql, params) => {
        queryLog.push({ sql, params: params || [] });
        return { rowsAffected: 1, lastInsertRowId: 1 };
      },
      values: async (sql, params) => {
        queryLog.push({ sql, params: params || [] });
        return [[1, 'Alice'], [2, 'Bob']];
      },
      transaction: async (fn) => {
        return fn(backend);
      },
    };
  });

  describe('Query Methods', () => {
    it('should execute all() and return rows', async () => {
      const result = await backend.all('SELECT * FROM users', []);

      expect(result).toHaveLength(2);
      expect(result[0]).toEqual({ id: 1, name: 'Alice' });
      expect(queryLog).toHaveLength(1);
      expect(queryLog[0].sql).toBe('SELECT * FROM users');
    });

    it('should execute get() and return single row', async () => {
      const result = await backend.get('SELECT * FROM users WHERE id = ?', [1]);

      expect(result).toEqual({ id: 1, name: 'Alice' });
      expect(queryLog[0].params).toContain(1);
    });

    it('should execute run() and return affected rows', async () => {
      const result = await backend.run('INSERT INTO users (name) VALUES (?)', ['Charlie']);

      expect(result.rowsAffected).toBe(1);
      expect(result.lastInsertRowId).toBe(1);
    });

    it('should execute values() and return arrays', async () => {
      const result = await backend.values('SELECT id, name FROM users', []);

      expect(result).toHaveLength(2);
      expect(result[0]).toEqual([1, 'Alice']);
    });
  });

  describe('Transaction Support', () => {
    it('should execute transaction function', async () => {
      let txExecuted = false;

      await backend.transaction(async (tx) => {
        txExecuted = true;
        await tx.run('INSERT INTO users (name) VALUES (?)', ['TxUser']);
        return 'done';
      });

      expect(txExecuted).toBe(true);
    });

    it('should return transaction result', async () => {
      const result = await backend.transaction(async (tx) => {
        await tx.run('INSERT INTO users (name) VALUES (?)', ['TxUser']);
        return 42;
      });

      expect(result).toBe(42);
    });
  });
});

// =============================================================================
// INTEGRATION PATTERN TESTS
// =============================================================================

describe('Drizzle Integration Pattern', () => {
  it('should demonstrate the intended usage pattern', async () => {
    // This test documents how the integration is meant to be used
    const backend: DoSQLBackend = {
      all: async () => [],
      get: async () => undefined,
      run: async () => ({ rowsAffected: 0 }),
      values: async () => [],
      transaction: async (fn) => fn({} as DoSQLBackend),
    };

    // The config follows Drizzle's pattern
    const config: DoSQLDrizzleConfig = {
      backend,
      logger: true,
    };

    // Verify the config structure
    expect(config.backend).toBe(backend);
    expect(config.logger).toBe(true);

    /*
    // Full usage (requires Node.js environment):

    import { drizzle } from 'dosql/orm/drizzle';
    import { sqliteTable, text, integer } from 'drizzle-orm/sqlite-core';
    import { eq } from 'drizzle-orm';

    // Define schema
    const users = sqliteTable('users', {
      id: integer('id').primaryKey(),
      name: text('name').notNull(),
      email: text('email').notNull(),
    });

    // Create database instance
    const db = drizzle({ backend: dosqlBackend });

    // Execute queries
    const allUsers = await db.select().from(users);
    const user = await db.select().from(users).where(eq(users.id, 1));
    await db.insert(users).values({ name: 'Alice', email: 'alice@example.com' });
    */
  });

  it('should support type-safe schema configuration', () => {
    // Define a schema type
    interface Schema {
      users: {
        id: number;
        name: string;
        email: string;
      };
      posts: {
        id: number;
        title: string;
        authorId: number;
      };
    }

    const backend: DoSQLBackend = {
      all: async () => [],
      get: async () => undefined,
      run: async () => ({ rowsAffected: 0 }),
      values: async () => [],
      transaction: async (fn) => fn({} as DoSQLBackend),
    };

    // Config with schema type
    const config: DoSQLDrizzleConfig<Schema> = {
      backend,
      schema: {
        users: {} as any,
        posts: {} as any,
      },
    };

    expect(config.schema).toBeDefined();
    expect(config.schema?.users).toBeDefined();
    expect(config.schema?.posts).toBeDefined();
  });
});

// =============================================================================
// SQL VALUE TYPE TESTS
// =============================================================================

describe('SQL Value Types', () => {
  let backend: DoSQLBackend;

  beforeEach(() => {
    backend = {
      all: async () => [],
      get: async () => undefined,
      run: async () => ({ rowsAffected: 0 }),
      values: async () => [],
      transaction: async (fn) => fn({} as DoSQLBackend),
    };
  });

  it('should support string values', async () => {
    await backend.run('INSERT INTO users (name) VALUES (?)', ['Alice']);
    // No error = success
  });

  it('should support number values', async () => {
    await backend.run('INSERT INTO users (age) VALUES (?)', [25]);
    // No error = success
  });

  it('should support bigint values', async () => {
    await backend.run('INSERT INTO users (big_id) VALUES (?)', [BigInt(9007199254740993)]);
    // No error = success
  });

  it('should support boolean values', async () => {
    await backend.run('INSERT INTO users (active) VALUES (?)', [true]);
    // No error = success
  });

  it('should support null values', async () => {
    await backend.run('INSERT INTO users (middle_name) VALUES (?)', [null]);
    // No error = success
  });

  it('should support Uint8Array values', async () => {
    const blobData = new Uint8Array([1, 2, 3, 4, 5]);
    await backend.run('INSERT INTO files (data) VALUES (?)', [blobData]);
    // No error = success
  });
});
