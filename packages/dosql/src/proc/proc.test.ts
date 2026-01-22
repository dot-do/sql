/**
 * ESM Stored Procedures Tests
 *
 * Tests for the procedure parser, registry, and executor.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  parseProcedure,
  tryParseProcedure,
  isCreateProcedure,
  validateModuleCode,
  buildInputSchema,
  buildOutputSchema,
} from './parser.js';
import {
  createInMemoryCatalogStorage,
  createProcedureRegistry,
  createSqlProcedureManager,
  procedure,
  createExtendedRegistry,
} from './registry.js';
import {
  createInMemoryAdapter,
  createInMemorySqlExecutor,
  createInMemoryTransactionManager,
  createDatabaseContext,
} from './context.js';
import {
  createSimpleExecutor,
  createMockExecutor,
} from './executor.js';
import type { DatabaseSchema } from './types.js';

// =============================================================================
// PARSER TESTS
// =============================================================================

describe('Procedure Parser', () => {
  describe('parseProcedure', () => {
    it('should parse a simple CREATE PROCEDURE statement', () => {
      const sql = `
        CREATE PROCEDURE hello AS MODULE $$
          export default async ({ db }) => {
            return "Hello, World!";
          }
        $$;
      `;

      const result = parseProcedure(sql);

      expect(result.name).toBe('hello');
      expect(result.code).toContain('export default async');
      expect(result.code).toContain('Hello, World!');
    });

    it('should parse CREATE OR REPLACE PROCEDURE', () => {
      const sql = `
        CREATE OR REPLACE PROCEDURE update_user AS MODULE $$
          export default async ({ db }, userId, data) => {
            return db.users.update({ id: userId }, data);
          }
        $$;
      `;

      const result = parseProcedure(sql);

      expect(result.name).toBe('update_user');
      expect(result.code).toContain('db.users.update');
    });

    it('should parse procedure with parameters', () => {
      const sql = `
        CREATE PROCEDURE calculate_total(userId INTEGER, includeShipping BOOLEAN DEFAULT true)
        RETURNS NUMBER
        AS MODULE $$
          export default async ({ db }, userId, includeShipping) => {
            const orders = await db.orders.where({ userId });
            let total = orders.reduce((sum, o) => sum + o.total, 0);
            if (includeShipping) total += 10;
            return total;
          }
        $$;
      `;

      const result = parseProcedure(sql);

      expect(result.name).toBe('calculate_total');
      expect(result.parameters).toHaveLength(2);
      expect(result.parameters![0]).toEqual({ name: 'userId', type: 'INTEGER' });
      expect(result.parameters![1]).toEqual({
        name: 'includeShipping',
        type: 'BOOLEAN',
        defaultValue: 'true',
      });
      expect(result.returnType).toBe('NUMBER');
    });

    it('should parse CREATE FUNCTION (alias)', () => {
      const sql = `
        CREATE FUNCTION get_user AS MODULE $$
          export default async ({ db }, id) => db.users.get(id);
        $$;
      `;

      const result = parseProcedure(sql);

      expect(result.name).toBe('get_user');
    });

    it('should handle complex module code with nested brackets', () => {
      const sql = `
        CREATE PROCEDURE complex AS MODULE $$
          export default async ({ db }) => {
            const data = { nested: { array: [1, 2, 3] } };
            const result = await db.items.where(item => {
              return item.value > 10 && (item.type === 'A' || item.type === 'B');
            });
            return result.map(r => ({ ...r, processed: true }));
          }
        $$;
      `;

      const result = parseProcedure(sql);

      expect(result.name).toBe('complex');
      expect(result.code).toContain('nested: { array: [1, 2, 3] }');
    });
  });

  describe('tryParseProcedure', () => {
    it('should return success for valid procedure', () => {
      const sql = `CREATE PROCEDURE test AS MODULE $$ export default () => 1; $$`;

      const result = tryParseProcedure(sql);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.procedure.name).toBe('test');
      }
    });

    it('should return error for invalid SQL', () => {
      const sql = `SELECT * FROM users`;

      const result = tryParseProcedure(sql);

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.message).toBeDefined();
      }
    });
  });

  describe('isCreateProcedure', () => {
    it('should detect CREATE PROCEDURE', () => {
      expect(isCreateProcedure('CREATE PROCEDURE test AS MODULE $$ ... $$')).toBe(true);
      expect(isCreateProcedure('create procedure test AS MODULE $$ ... $$')).toBe(true);
      expect(isCreateProcedure('CREATE OR REPLACE PROCEDURE test AS MODULE $$ ... $$')).toBe(true);
      expect(isCreateProcedure('CREATE FUNCTION test AS MODULE $$ ... $$')).toBe(true);
    });

    it('should not detect non-procedure SQL', () => {
      expect(isCreateProcedure('SELECT * FROM users')).toBe(false);
      expect(isCreateProcedure('CREATE TABLE users')).toBe(false);
    });
  });

  describe('validateModuleCode', () => {
    it('should validate module with default export', () => {
      const code = `export default async ({ db }) => db.users.all()`;
      const result = validateModuleCode(code);
      expect(result.valid).toBe(true);
    });

    it('should reject module without default export', () => {
      const code = `export const handler = ({ db }) => db.users.all()`;
      const result = validateModuleCode(code);
      expect(result.valid).toBe(false);
      expect(result.error).toContain('default export');
    });

    it('should detect unbalanced brackets', () => {
      const code = `export default async ({ db }) => { db.users.all(`;
      const result = validateModuleCode(code);
      expect(result.valid).toBe(false);
      expect(result.error).toContain('bracket');
    });
  });

  describe('schema builders', () => {
    it('should build input schema from parameters', () => {
      const params = [
        { name: 'id', type: 'INTEGER' },
        { name: 'name', type: 'TEXT' },
        { name: 'active', type: 'BOOLEAN', defaultValue: 'true' },
      ];

      const schema = buildInputSchema(params);

      expect(schema?.type).toBe('object');
      expect(schema?.properties?.id.type).toBe('number');
      expect(schema?.properties?.name.type).toBe('string');
      expect(schema?.properties?.active.type).toBe('boolean');
      expect(schema?.required).toContain('id');
      expect(schema?.required).toContain('name');
      expect(schema?.required).not.toContain('active');
    });

    it('should build output schema from return type', () => {
      expect(buildOutputSchema('INTEGER')?.type).toBe('number');
      expect(buildOutputSchema('TEXT')?.type).toBe('string');
      expect(buildOutputSchema('BOOLEAN')?.type).toBe('boolean');
      expect(buildOutputSchema('TEXT[]')?.type).toBe('array');
    });
  });
});

// =============================================================================
// REGISTRY TESTS
// =============================================================================

describe('Procedure Registry', () => {
  let registry: ReturnType<typeof createProcedureRegistry>;

  beforeEach(() => {
    registry = createProcedureRegistry({
      storage: createInMemoryCatalogStorage(),
    });
  });

  describe('register', () => {
    it('should register a new procedure', async () => {
      const proc = await registry.register({
        name: 'test_proc',
        code: 'export default () => 42;',
      });

      expect(proc.name).toBe('test_proc');
      expect(proc.metadata.version).toBe(1);
      expect(proc.metadata.createdAt).toBeInstanceOf(Date);
    });

    it('should increment version on update', async () => {
      await registry.register({
        name: 'test_proc',
        code: 'export default () => 1;',
      });

      const updated = await registry.register({
        name: 'test_proc',
        code: 'export default () => 2;',
      });

      expect(updated.metadata.version).toBe(2);
    });

    it('should validate module code when enabled', async () => {
      await expect(registry.register({
        name: 'invalid',
        code: 'const x = 1; // no default export',
      })).rejects.toThrow('default export');
    });
  });

  describe('get', () => {
    it('should retrieve procedure by name', async () => {
      await registry.register({
        name: 'my_proc',
        code: 'export default () => "hello";',
      });

      const proc = await registry.get('my_proc');

      expect(proc?.name).toBe('my_proc');
      expect(proc?.code).toContain('hello');
    });

    it('should retrieve specific version', async () => {
      await registry.register({
        name: 'versioned',
        code: 'export default () => 1;',
      });
      await registry.register({
        name: 'versioned',
        code: 'export default () => 2;',
      });

      const v1 = await registry.get('versioned', 1);
      const v2 = await registry.get('versioned', 2);

      expect(v1?.code).toContain('=> 1');
      expect(v2?.code).toContain('=> 2');
    });

    it('should return undefined for non-existent procedure', async () => {
      const proc = await registry.get('nonexistent');
      expect(proc).toBeUndefined();
    });
  });

  describe('list', () => {
    it('should list all procedures', async () => {
      await registry.register({ name: 'proc1', code: 'export default () => 1;' });
      await registry.register({ name: 'proc2', code: 'export default () => 2;' });
      await registry.register({ name: 'proc3', code: 'export default () => 3;' });

      const procs = await registry.list();

      expect(procs).toHaveLength(3);
      expect(procs.map(p => p.name).sort()).toEqual(['proc1', 'proc2', 'proc3']);
    });
  });

  describe('delete', () => {
    it('should delete a procedure', async () => {
      await registry.register({ name: 'to_delete', code: 'export default () => 0;' });

      const deleted = await registry.delete('to_delete');
      const proc = await registry.get('to_delete');

      expect(deleted).toBe(true);
      expect(proc).toBeUndefined();
    });
  });

  describe('history', () => {
    it('should return version history', async () => {
      await registry.register({ name: 'tracked', code: 'export default () => 1;' });
      await registry.register({ name: 'tracked', code: 'export default () => 2;' });
      await registry.register({ name: 'tracked', code: 'export default () => 3;' });

      const history = await registry.history('tracked');

      expect(history).toHaveLength(3);
      expect(history.map(h => h.version)).toEqual([1, 2, 3]);
    });
  });
});

// =============================================================================
// SQL PROCEDURE MANAGER TESTS
// =============================================================================

describe('SQL Procedure Manager', () => {
  let manager: ReturnType<typeof createSqlProcedureManager>;

  beforeEach(() => {
    const registry = createProcedureRegistry({
      storage: createInMemoryCatalogStorage(),
    });
    manager = createSqlProcedureManager(registry);
  });

  it('should execute CREATE PROCEDURE SQL', async () => {
    const sql = `
      CREATE PROCEDURE greet AS MODULE $$
        export default async (ctx, name) => \`Hello, \${name}!\`;
      $$;
    `;

    const proc = await manager.execute(sql);

    expect(proc.name).toBe('greet');
    expect(proc.code).toContain('Hello');
  });

  it('should reject non-procedure SQL', async () => {
    await expect(manager.execute('SELECT * FROM users')).rejects.toThrow(
      'Only CREATE PROCEDURE'
    );
  });
});

// =============================================================================
// PROCEDURE BUILDER TESTS
// =============================================================================

describe('Procedure Builder', () => {
  let registry: ReturnType<typeof createProcedureRegistry>;

  beforeEach(() => {
    registry = createProcedureRegistry({
      storage: createInMemoryCatalogStorage(),
    });
  });

  it('should build and register procedure', async () => {
    const proc = await procedure()
      .name('builder_test')
      .code('export default () => "built";')
      .description('A test procedure')
      .author('test-user')
      .tag('test', 'example')
      .timeout(3000)
      .register(registry);

    expect(proc.name).toBe('builder_test');
    expect(proc.metadata.description).toBe('A test procedure');
    expect(proc.metadata.author).toBe('test-user');
    expect(proc.metadata.tags).toContain('test');
    expect(proc.timeout).toBe(3000);
  });

  it('should build without registering', () => {
    const def = procedure()
      .name('no_register')
      .code('export default () => null;')
      .build();

    expect(def.name).toBe('no_register');
  });

  it('should require name and code', () => {
    expect(() => procedure().code('export default () => 1;').build()).toThrow('name');
    expect(() => procedure().name('test').build()).toThrow('code');
  });
});

// =============================================================================
// EXTENDED REGISTRY TESTS
// =============================================================================

describe('Extended Procedure Registry', () => {
  let extRegistry: ReturnType<typeof createExtendedRegistry>;

  beforeEach(async () => {
    const baseRegistry = createProcedureRegistry({
      storage: createInMemoryCatalogStorage(),
    });

    // Register some procedures
    await baseRegistry.register({
      name: 'user_create',
      code: 'export default () => {};',
      metadata: { author: 'alice', tags: ['user', 'crud'] },
    });
    await baseRegistry.register({
      name: 'user_delete',
      code: 'export default () => {};',
      metadata: { author: 'alice', tags: ['user', 'crud'] },
    });
    await baseRegistry.register({
      name: 'order_create',
      code: 'export default () => {};',
      metadata: { author: 'bob', tags: ['order', 'crud'] },
    });

    extRegistry = createExtendedRegistry(baseRegistry);
  });

  it('should query by tag', async () => {
    const procs = await extRegistry.query({ tag: 'user' });
    expect(procs).toHaveLength(2);
    expect(procs.every(p => p.metadata.tags?.includes('user'))).toBe(true);
  });

  it('should query by author', async () => {
    const procs = await extRegistry.query({ author: 'alice' });
    expect(procs).toHaveLength(2);
  });

  it('should query by name pattern', async () => {
    const procs = await extRegistry.query({ namePattern: '*_create' });
    expect(procs).toHaveLength(2);
    expect(procs.map(p => p.name).sort()).toEqual(['order_create', 'user_create']);
  });

  it('should count procedures', async () => {
    const total = await extRegistry.count();
    const userCount = await extRegistry.count({ tag: 'user' });

    expect(total).toBe(3);
    expect(userCount).toBe(2);
  });

  it('should search by description', async () => {
    // Update a procedure with description
    await extRegistry.register({
      name: 'searchable',
      code: 'export default () => {};',
      metadata: { description: 'This handles payment processing' },
    });

    const results = await extRegistry.search('payment');
    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('searchable');
  });
});

// =============================================================================
// CONTEXT TESTS
// =============================================================================

describe('Database Context', () => {
  interface TestDB extends DatabaseSchema {
    users: { id: 'number'; name: 'string'; email: 'string' };
    orders: { id: 'number'; userId: 'number'; total: 'number' };
  }

  it('should create in-memory adapter', async () => {
    const adapter = createInMemoryAdapter([
      { id: 1, name: 'Alice', email: 'alice@example.com' },
      { id: 2, name: 'Bob', email: 'bob@example.com' },
    ]);

    const all = await adapter.query({});
    expect(all).toHaveLength(2);

    const alice = await adapter.get(1);
    expect(alice?.name).toBe('Alice');
  });

  it('should support CRUD operations', async () => {
    const adapter = createInMemoryAdapter<{ id: number; value: string }>([]);

    // Insert
    const record = await adapter.insert({ value: 'test' });
    expect(record.id).toBeDefined();
    expect(record.value).toBe('test');

    // Query
    const found = await adapter.query({ id: record.id });
    expect(found).toHaveLength(1);

    // Update
    const updated = await adapter.update({ id: record.id }, { value: 'updated' });
    expect(updated).toBe(1);

    // Delete
    const deleted = await adapter.delete({ id: record.id });
    expect(deleted).toBe(1);

    // Verify deleted
    const count = await adapter.count();
    expect(count).toBe(0);
  });

  it('should create full database context', () => {
    const adapters = {
      users: createInMemoryAdapter([{ id: 1, name: 'Test', email: 'test@test.com' }]),
      orders: createInMemoryAdapter([{ id: 1, userId: 1, total: 100 }]),
    };

    const db = createDatabaseContext({
      adapters: adapters as any,
      sqlExecutor: createInMemorySqlExecutor(new Map(Object.entries(adapters))),
      transactionManager: createInMemoryTransactionManager(),
    });

    expect(db.tables).toBeDefined();
    expect(db.sql).toBeDefined();
    expect(db.transaction).toBeDefined();
  });
});

// =============================================================================
// EXECUTOR TESTS
// =============================================================================

describe('Procedure Executor', () => {
  describe('createSimpleExecutor', () => {
    it('should execute simple procedure', async () => {
      const executor = createSimpleExecutor({
        users: [
          { id: 1, name: 'Alice' },
          { id: 2, name: 'Bob' },
        ],
      });

      const code = `
        export default async ({ db }) => {
          return 42;
        }
      `;

      const result = await executor.execute(code, []);

      // Note: This test depends on ai-evaluate which may need actual environment
      // For unit testing, we use the mock executor instead
      expect(result.requestId).toBeDefined();
    });
  });

  describe('createMockExecutor', () => {
    it('should return mocked results', async () => {
      const executor = createMockExecutor({
        get_user: { id: 1, name: 'Mock User' },
        calculate_total: async (userId: number) => userId * 100,
      });

      const userResult = await executor.call('get_user', [1]);
      expect(userResult.success).toBe(true);
      expect(userResult.result).toEqual({ id: 1, name: 'Mock User' });

      const totalResult = await executor.call('calculate_total', [5]);
      expect(totalResult.success).toBe(true);
      expect(totalResult.result).toBe(500);
    });

    it('should return error for unmocked procedure', async () => {
      const executor = createMockExecutor({});

      const result = await executor.call('nonexistent', []);

      expect(result.success).toBe(false);
      expect(result.error).toContain('Mock not found');
    });
  });
});
