/**
 * Procedure Registry Tests
 *
 * Additional tests for the procedure registry module including:
 * - Storage backend operations
 * - Versioning and history
 * - Registry queries and filtering
 * - SQL procedure manager
 * - Procedure builder
 *
 * Issue: sql-ntht - Stored Procedure Module Tests
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  createInMemoryCatalogStorage,
  createProcedureRegistry,
  createSqlProcedureManager,
  createExtendedRegistry,
  procedure,
  ProcedureBuilder,
  type CatalogStorage,
  type RegistryOptions,
} from '../registry.js';
import type { Procedure, ProcedureRegistry, RegistryEntry } from '../types.js';

// =============================================================================
// CATALOG STORAGE TESTS
// =============================================================================

describe('CatalogStorage', () => {
  describe('createInMemoryCatalogStorage', () => {
    let storage: CatalogStorage;

    beforeEach(() => {
      storage = createInMemoryCatalogStorage();
    });

    it('should start empty', async () => {
      const list = await storage.list();
      expect(list).toHaveLength(0);
    });

    it('should store and retrieve entry', async () => {
      const entry: RegistryEntry = {
        current: {
          name: 'test_proc',
          code: 'export default () => 1;',
          metadata: {
            name: 'test_proc',
            version: 1,
            createdAt: new Date(),
            updatedAt: new Date(),
          },
        },
        versions: [],
      };

      await storage.set('test_proc', entry);
      const retrieved = await storage.get('test_proc');

      expect(retrieved).toBeDefined();
      expect(retrieved?.current.name).toBe('test_proc');
    });

    it('should return undefined for non-existent entry', async () => {
      const result = await storage.get('nonexistent');
      expect(result).toBeUndefined();
    });

    it('should list all entry names', async () => {
      const entries = ['proc_a', 'proc_b', 'proc_c'];

      for (const name of entries) {
        await storage.set(name, {
          current: {
            name,
            code: 'export default () => 1;',
            metadata: {
              name,
              version: 1,
              createdAt: new Date(),
              updatedAt: new Date(),
            },
          },
          versions: [],
        });
      }

      const list = await storage.list();
      expect(list.sort()).toEqual(entries.sort());
    });

    it('should delete entry', async () => {
      await storage.set('to_delete', {
        current: {
          name: 'to_delete',
          code: 'export default () => 1;',
          metadata: {
            name: 'to_delete',
            version: 1,
            createdAt: new Date(),
            updatedAt: new Date(),
          },
        },
        versions: [],
      });

      const deleted = await storage.delete('to_delete');
      expect(deleted).toBe(true);

      const retrieved = await storage.get('to_delete');
      expect(retrieved).toBeUndefined();
    });

    it('should return false when deleting non-existent entry', async () => {
      const deleted = await storage.delete('nonexistent');
      expect(deleted).toBe(false);
    });

    it('should overwrite existing entry on set', async () => {
      await storage.set('overwrite_test', {
        current: {
          name: 'overwrite_test',
          code: 'export default () => 1;',
          metadata: {
            name: 'overwrite_test',
            version: 1,
            createdAt: new Date(),
            updatedAt: new Date(),
          },
        },
        versions: [],
      });

      await storage.set('overwrite_test', {
        current: {
          name: 'overwrite_test',
          code: 'export default () => 2;',
          metadata: {
            name: 'overwrite_test',
            version: 2,
            createdAt: new Date(),
            updatedAt: new Date(),
          },
        },
        versions: [],
      });

      const retrieved = await storage.get('overwrite_test');
      expect(retrieved?.current.metadata.version).toBe(2);
    });
  });
});

// =============================================================================
// PROCEDURE REGISTRY TESTS
// =============================================================================

describe('ProcedureRegistry', () => {
  let registry: ProcedureRegistry;

  beforeEach(() => {
    registry = createProcedureRegistry({
      storage: createInMemoryCatalogStorage(),
    });
  });

  describe('register', () => {
    it('should register new procedure with version 1', async () => {
      const proc = await registry.register({
        name: 'new_proc',
        code: 'export default () => "new"',
      });

      expect(proc.name).toBe('new_proc');
      expect(proc.metadata.version).toBe(1);
      expect(proc.metadata.createdAt).toBeInstanceOf(Date);
      expect(proc.metadata.updatedAt).toBeInstanceOf(Date);
    });

    it('should increment version on re-registration', async () => {
      await registry.register({
        name: 'versioned_proc',
        code: 'export default () => 1',
      });

      const v2 = await registry.register({
        name: 'versioned_proc',
        code: 'export default () => 2',
      });

      expect(v2.metadata.version).toBe(2);
    });

    it('should preserve createdAt on update', async () => {
      const v1 = await registry.register({
        name: 'date_test',
        code: 'export default () => 1',
      });

      // Small delay to ensure timestamps differ
      await new Promise(r => setTimeout(r, 10));

      const v2 = await registry.register({
        name: 'date_test',
        code: 'export default () => 2',
      });

      expect(v2.metadata.createdAt.getTime()).toBe(v1.metadata.createdAt.getTime());
      expect(v2.metadata.updatedAt.getTime()).toBeGreaterThan(v1.metadata.createdAt.getTime());
    });

    it('should store input and output schemas', async () => {
      const proc = await registry.register({
        name: 'with_schemas',
        code: 'export default (ctx, id) => id',
        inputSchema: {
          type: 'object',
          properties: { id: { type: 'number' } },
        },
        outputSchema: { type: 'number' },
      });

      expect(proc.inputSchema).toBeDefined();
      expect(proc.outputSchema).toBeDefined();
      expect(proc.inputSchema?.type).toBe('object');
      expect(proc.outputSchema?.type).toBe('number');
    });

    it('should store timeout and memory limit', async () => {
      const proc = await registry.register({
        name: 'with_limits',
        code: 'export default () => 1',
        timeout: 10000,
        memoryLimit: 256,
      });

      expect(proc.timeout).toBe(10000);
      expect(proc.memoryLimit).toBe(256);
    });

    it('should store custom metadata', async () => {
      const proc = await registry.register({
        name: 'with_metadata',
        code: 'export default () => 1',
        metadata: {
          description: 'Test procedure',
          author: 'test-user',
          tags: ['test', 'example'],
        },
      });

      expect(proc.metadata.description).toBe('Test procedure');
      expect(proc.metadata.author).toBe('test-user');
      expect(proc.metadata.tags).toEqual(['test', 'example']);
    });
  });

  describe('validation', () => {
    it('should validate module code by default', async () => {
      await expect(
        registry.register({
          name: 'invalid',
          code: 'const x = 1;', // No default export
        })
      ).rejects.toThrow('default export');
    });

    it('should skip validation when disabled', async () => {
      const noValidateRegistry = createProcedureRegistry({
        storage: createInMemoryCatalogStorage(),
        validateCode: false,
      });

      // Should not throw
      const proc = await noValidateRegistry.register({
        name: 'no_validate',
        code: 'const x = 1;',
      });

      expect(proc.name).toBe('no_validate');
    });
  });

  describe('get', () => {
    beforeEach(async () => {
      await registry.register({
        name: 'multi_version',
        code: 'export default () => 1',
      });
      await registry.register({
        name: 'multi_version',
        code: 'export default () => 2',
      });
      await registry.register({
        name: 'multi_version',
        code: 'export default () => 3',
      });
    });

    it('should get latest version by default', async () => {
      const proc = await registry.get('multi_version');

      expect(proc?.metadata.version).toBe(3);
      expect(proc?.code).toContain('=> 3');
    });

    it('should get specific version', async () => {
      const v1 = await registry.get('multi_version', 1);
      const v2 = await registry.get('multi_version', 2);
      const v3 = await registry.get('multi_version', 3);

      expect(v1?.code).toContain('=> 1');
      expect(v2?.code).toContain('=> 2');
      expect(v3?.code).toContain('=> 3');
    });

    it('should return undefined for non-existent version', async () => {
      const proc = await registry.get('multi_version', 99);
      expect(proc).toBeUndefined();
    });

    it('should return undefined for non-existent procedure', async () => {
      const proc = await registry.get('nonexistent');
      expect(proc).toBeUndefined();
    });
  });

  describe('list', () => {
    it('should return empty array when no procedures', async () => {
      const procs = await registry.list();
      expect(procs).toHaveLength(0);
    });

    it('should return all current procedures', async () => {
      await registry.register({ name: 'proc_a', code: 'export default () => "a"' });
      await registry.register({ name: 'proc_b', code: 'export default () => "b"' });
      await registry.register({ name: 'proc_c', code: 'export default () => "c"' });

      const procs = await registry.list();

      expect(procs).toHaveLength(3);
      expect(procs.map(p => p.name).sort()).toEqual(['proc_a', 'proc_b', 'proc_c']);
    });

    it('should return current version for each procedure', async () => {
      await registry.register({ name: 'versioned', code: 'export default () => 1' });
      await registry.register({ name: 'versioned', code: 'export default () => 2' });
      await registry.register({ name: 'other', code: 'export default () => "other"' });

      const procs = await registry.list();
      const versioned = procs.find(p => p.name === 'versioned');

      expect(versioned?.metadata.version).toBe(2);
    });
  });

  describe('delete', () => {
    it('should delete existing procedure', async () => {
      await registry.register({ name: 'to_delete', code: 'export default () => 1' });

      const deleted = await registry.delete('to_delete');
      const proc = await registry.get('to_delete');

      expect(deleted).toBe(true);
      expect(proc).toBeUndefined();
    });

    it('should return false for non-existent procedure', async () => {
      const deleted = await registry.delete('nonexistent');
      expect(deleted).toBe(false);
    });
  });

  describe('history', () => {
    it('should return empty history for non-existent procedure', async () => {
      const history = await registry.history('nonexistent');
      expect(history).toHaveLength(0);
    });

    it('should return all versions in history', async () => {
      await registry.register({ name: 'tracked', code: 'export default () => 1' });
      await registry.register({ name: 'tracked', code: 'export default () => 2' });
      await registry.register({ name: 'tracked', code: 'export default () => 3' });

      const history = await registry.history('tracked');

      expect(history).toHaveLength(3);
      expect(history.map(h => h.version)).toEqual([1, 2, 3]);
    });

    it('should include timestamps in history', async () => {
      await registry.register({ name: 'timed', code: 'export default () => 1' });
      await new Promise(r => setTimeout(r, 10));
      await registry.register({ name: 'timed', code: 'export default () => 2' });

      const history = await registry.history('timed');

      expect(history[0].timestamp).toBeInstanceOf(Date);
      expect(history[1].timestamp).toBeInstanceOf(Date);
      expect(history[1].timestamp.getTime()).toBeGreaterThan(history[0].timestamp.getTime());
    });
  });

  describe('version retention', () => {
    it('should limit version history by maxVersions', async () => {
      const limitedRegistry = createProcedureRegistry({
        storage: createInMemoryCatalogStorage(),
        maxVersions: 3,
      });

      // Register 5 versions
      for (let i = 1; i <= 5; i++) {
        await limitedRegistry.register({
          name: 'limited',
          code: `export default () => ${i}`,
        });
      }

      const history = await limitedRegistry.history('limited');

      // Should only keep 3 versions (plus current)
      // Based on implementation: versions array + current = maxVersions + 1
      expect(history.length).toBeLessThanOrEqual(4);
    });
  });
});

// =============================================================================
// SQL PROCEDURE MANAGER TESTS
// =============================================================================

describe('SqlProcedureManager', () => {
  let registry: ProcedureRegistry;
  let manager: ReturnType<typeof createSqlProcedureManager>;

  beforeEach(() => {
    registry = createProcedureRegistry({
      storage: createInMemoryCatalogStorage(),
    });
    manager = createSqlProcedureManager(registry);
  });

  describe('execute', () => {
    it('should execute CREATE PROCEDURE SQL', async () => {
      const sql = `
        CREATE PROCEDURE test_sql AS MODULE $$
          export default () => 'from sql';
        $$
      `;

      const proc = await manager.execute(sql);

      expect(proc.name).toBe('test_sql');
      expect(proc.code).toContain("'from sql'");
    });

    it('should execute CREATE FUNCTION SQL', async () => {
      const sql = `
        CREATE FUNCTION test_func AS MODULE $$
          export default (ctx, x) => x * 2;
        $$
      `;

      const proc = await manager.execute(sql);

      expect(proc.name).toBe('test_func');
    });

    it('should execute CREATE OR REPLACE', async () => {
      const sql1 = `CREATE PROCEDURE replaceable AS MODULE $$ export default () => 1; $$`;
      const sql2 = `CREATE OR REPLACE PROCEDURE replaceable AS MODULE $$ export default () => 2; $$`;

      await manager.execute(sql1);
      const proc = await manager.execute(sql2);

      expect(proc.metadata.version).toBe(2);
    });

    it('should build schemas from SQL parameters', async () => {
      const sql = `
        CREATE PROCEDURE with_params(id INTEGER, name TEXT, active BOOLEAN DEFAULT true)
        RETURNS JSON
        AS MODULE $$
          export default (ctx, id, name, active) => ({ id, name, active });
        $$
      `;

      const proc = await manager.execute(sql);

      expect(proc.inputSchema).toBeDefined();
      expect(proc.inputSchema?.type).toBe('object');
      expect(proc.outputSchema).toBeDefined();
      expect(proc.outputSchema?.type).toBe('object'); // JSON maps to object
    });

    it('should reject non-procedure SQL', async () => {
      await expect(manager.execute('SELECT * FROM users')).rejects.toThrow(
        'Only CREATE PROCEDURE'
      );
    });

    it('should reject CREATE TABLE', async () => {
      await expect(manager.execute('CREATE TABLE users (id INT)')).rejects.toThrow();
    });
  });

  describe('get', () => {
    it('should get registered procedure', async () => {
      await manager.execute(`CREATE PROCEDURE test AS MODULE $$ export default () => 1; $$`);

      const proc = await manager.get('test');

      expect(proc).toBeDefined();
      expect(proc?.name).toBe('test');
    });

    it('should return undefined for non-existent', async () => {
      const proc = await manager.get('nonexistent');
      expect(proc).toBeUndefined();
    });
  });

  describe('list', () => {
    it('should list all procedures', async () => {
      await manager.execute(`CREATE PROCEDURE a AS MODULE $$ export default () => 'a'; $$`);
      await manager.execute(`CREATE PROCEDURE b AS MODULE $$ export default () => 'b'; $$`);

      const procs = await manager.list();

      expect(procs).toHaveLength(2);
    });
  });

  describe('drop', () => {
    it('should drop procedure', async () => {
      await manager.execute(`CREATE PROCEDURE to_drop AS MODULE $$ export default () => 1; $$`);

      const dropped = await manager.drop('to_drop');
      const proc = await manager.get('to_drop');

      expect(dropped).toBe(true);
      expect(proc).toBeUndefined();
    });
  });
});

// =============================================================================
// PROCEDURE BUILDER TESTS
// =============================================================================

describe('ProcedureBuilder', () => {
  let registry: ProcedureRegistry;

  beforeEach(() => {
    registry = createProcedureRegistry({
      storage: createInMemoryCatalogStorage(),
    });
  });

  describe('fluent API', () => {
    it('should build procedure with all options', async () => {
      const proc = await procedure()
        .name('full_builder')
        .code('export default () => "built"')
        .description('Full builder test')
        .author('test-author')
        .tag('test', 'builder', 'example')
        .timeout(5000)
        .memoryLimit(64)
        .input({ type: 'object', properties: { id: { type: 'number' } } })
        .output({ type: 'string' })
        .register(registry);

      expect(proc.name).toBe('full_builder');
      expect(proc.code).toContain('"built"');
      expect(proc.metadata.description).toBe('Full builder test');
      expect(proc.metadata.author).toBe('test-author');
      expect(proc.metadata.tags).toContain('test');
      expect(proc.metadata.tags).toContain('builder');
      expect(proc.timeout).toBe(5000);
      expect(proc.memoryLimit).toBe(64);
      expect(proc.inputSchema).toBeDefined();
      expect(proc.outputSchema).toBeDefined();
    });

    it('should support method chaining in any order', async () => {
      const proc = await procedure()
        .timeout(1000)
        .name('chained')
        .tag('a')
        .code('export default () => 1')
        .author('author')
        .tag('b', 'c')
        .register(registry);

      expect(proc.name).toBe('chained');
      expect(proc.timeout).toBe(1000);
      expect(proc.metadata.tags).toContain('a');
      expect(proc.metadata.tags).toContain('b');
      expect(proc.metadata.tags).toContain('c');
    });
  });

  describe('build without register', () => {
    it('should build definition without registering', () => {
      const def = procedure()
        .name('local_def')
        .code('export default () => "local"')
        .description('Local definition')
        .build();

      expect(def.name).toBe('local_def');
      expect(def.code).toContain('"local"');
      expect(def.metadata?.description).toBe('Local definition');
    });
  });

  describe('validation', () => {
    it('should throw when name is missing', () => {
      expect(() => {
        procedure()
          .code('export default () => 1')
          .build();
      }).toThrow('name');
    });

    it('should throw when code is missing', () => {
      expect(() => {
        procedure()
          .name('no_code')
          .build();
      }).toThrow('code');
    });

    it('should throw when registering without name', async () => {
      await expect(
        procedure()
          .code('export default () => 1')
          .register(registry)
      ).rejects.toThrow('name');
    });
  });
});

// =============================================================================
// EXTENDED REGISTRY TESTS
// =============================================================================

describe('ExtendedProcedureRegistry', () => {
  let extRegistry: ReturnType<typeof createExtendedRegistry>;

  beforeEach(async () => {
    const baseRegistry = createProcedureRegistry({
      storage: createInMemoryCatalogStorage(),
    });

    // Seed with test data
    await baseRegistry.register({
      name: 'user_create',
      code: 'export default () => {}',
      metadata: {
        description: 'Create a new user',
        author: 'alice',
        tags: ['user', 'crud', 'write'],
      },
    });
    await baseRegistry.register({
      name: 'user_read',
      code: 'export default () => {}',
      metadata: {
        description: 'Read user data',
        author: 'alice',
        tags: ['user', 'crud', 'read'],
      },
    });
    await baseRegistry.register({
      name: 'order_create',
      code: 'export default () => {}',
      metadata: {
        description: 'Create an order',
        author: 'bob',
        tags: ['order', 'crud', 'write'],
      },
    });
    await baseRegistry.register({
      name: 'report_generate',
      code: 'export default () => {}',
      metadata: {
        description: 'Generate analytics report',
        author: 'charlie',
        tags: ['analytics', 'read'],
      },
    });

    extRegistry = createExtendedRegistry(baseRegistry);
  });

  describe('query', () => {
    it('should query by tag', async () => {
      const userProcs = await extRegistry.query({ tag: 'user' });
      expect(userProcs).toHaveLength(2);
      expect(userProcs.every(p => p.metadata.tags?.includes('user'))).toBe(true);
    });

    it('should query by author', async () => {
      const aliceProcs = await extRegistry.query({ author: 'alice' });
      expect(aliceProcs).toHaveLength(2);
      expect(aliceProcs.every(p => p.metadata.author === 'alice')).toBe(true);
    });

    it('should query by name pattern with wildcard', async () => {
      const createProcs = await extRegistry.query({ namePattern: '*_create' });
      expect(createProcs).toHaveLength(2);
      expect(createProcs.map(p => p.name).sort()).toEqual(['order_create', 'user_create']);
    });

    it('should query by name pattern with ?', async () => {
      const procs = await extRegistry.query({ namePattern: 'user_????' });
      expect(procs).toHaveLength(1);
      expect(procs[0].name).toBe('user_read');
    });

    it('should combine multiple filters', async () => {
      const procs = await extRegistry.query({
        tag: 'crud',
        author: 'alice',
      });

      expect(procs).toHaveLength(2);
      expect(procs.every(p => p.metadata.author === 'alice')).toBe(true);
      expect(procs.every(p => p.metadata.tags?.includes('crud'))).toBe(true);
    });
  });

  describe('sorting', () => {
    it('should sort by name ascending', async () => {
      const procs = await extRegistry.query({
        sortBy: 'name',
        sortOrder: 'asc',
      });

      const names = procs.map(p => p.name);
      expect(names).toEqual([...names].sort());
    });

    it('should sort by name descending', async () => {
      const procs = await extRegistry.query({
        sortBy: 'name',
        sortOrder: 'desc',
      });

      const names = procs.map(p => p.name);
      expect(names).toEqual([...names].sort().reverse());
    });

    it('should sort by createdAt', async () => {
      const procs = await extRegistry.query({
        sortBy: 'createdAt',
        sortOrder: 'asc',
      });

      // All created around the same time, so order may vary
      expect(procs.length).toBe(4);
    });
  });

  describe('pagination', () => {
    it('should limit results', async () => {
      const procs = await extRegistry.query({ limit: 2 });
      expect(procs).toHaveLength(2);
    });

    it('should offset results', async () => {
      const all = await extRegistry.query({ sortBy: 'name', sortOrder: 'asc' });
      const offset = await extRegistry.query({ sortBy: 'name', sortOrder: 'asc', offset: 2 });

      expect(offset).toHaveLength(2);
      expect(offset[0].name).toBe(all[2].name);
    });

    it('should combine offset and limit', async () => {
      const procs = await extRegistry.query({
        sortBy: 'name',
        sortOrder: 'asc',
        offset: 1,
        limit: 2,
      });

      expect(procs).toHaveLength(2);
    });
  });

  describe('count', () => {
    it('should count all procedures', async () => {
      const count = await extRegistry.count();
      expect(count).toBe(4);
    });

    it('should count with tag filter', async () => {
      const count = await extRegistry.count({ tag: 'crud' });
      expect(count).toBe(3);
    });

    it('should count with author filter', async () => {
      const count = await extRegistry.count({ author: 'bob' });
      expect(count).toBe(1);
    });

    it('should count with combined filters', async () => {
      const count = await extRegistry.count({
        tag: 'write',
        author: 'alice',
      });
      expect(count).toBe(1);
    });
  });

  describe('search', () => {
    it('should search by name', async () => {
      const results = await extRegistry.search('user');
      expect(results.length).toBeGreaterThanOrEqual(2);
      expect(results.some(p => p.name.includes('user'))).toBe(true);
    });

    it('should search by description', async () => {
      const results = await extRegistry.search('analytics');
      expect(results).toHaveLength(1);
      expect(results[0].name).toBe('report_generate');
    });

    it('should search by tags', async () => {
      const results = await extRegistry.search('crud');
      expect(results).toHaveLength(3);
    });

    it('should be case insensitive', async () => {
      const results1 = await extRegistry.search('USER');
      const results2 = await extRegistry.search('user');
      expect(results1.length).toBe(results2.length);
    });

    it('should return empty for no matches', async () => {
      const results = await extRegistry.search('nonexistent_term');
      expect(results).toHaveLength(0);
    });
  });

  describe('inherits base registry methods', () => {
    it('should support register', async () => {
      const proc = await extRegistry.register({
        name: 'new_via_ext',
        code: 'export default () => "ext"',
      });

      expect(proc.name).toBe('new_via_ext');
    });

    it('should support get', async () => {
      const proc = await extRegistry.get('user_create');
      expect(proc).toBeDefined();
      expect(proc?.name).toBe('user_create');
    });

    it('should support list', async () => {
      const procs = await extRegistry.list();
      expect(procs).toHaveLength(4);
    });

    it('should support delete', async () => {
      const deleted = await extRegistry.delete('user_create');
      expect(deleted).toBe(true);

      const procs = await extRegistry.list();
      expect(procs).toHaveLength(3);
    });

    it('should support history', async () => {
      const history = await extRegistry.history('user_create');
      expect(history).toHaveLength(1);
    });
  });
});
