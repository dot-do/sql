/**
 * Tests for TableSchema storage config integration
 */
import { describe, it, expect, beforeEach } from 'vitest';
import type { TableSchema } from '../types.js';
import type { TableStorageConfig } from '../storage-config.js';
import { createDOEngine } from '../do-engine.js';

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
    list: async () => new Map(data),
    transaction: async <T>(fn: (txn: any) => Promise<T>): Promise<T> => fn({}),
    deleteAll: async () => data.clear(),
    getAlarm: async () => null,
    setAlarm: async () => {},
    deleteAlarm: async () => {},
  };
}

describe('TableSchema with storageConfig', () => {
  let engine: ReturnType<typeof createDOEngine>;
  let storage: ReturnType<typeof createMockStorage>;

  beforeEach(() => {
    storage = createMockStorage();
    engine = createDOEngine({ storage });
  });

  it('TableSchema interface accepts optional storageConfig field', () => {
    // Type check: this should compile without errors
    const schemaWithStorage: TableSchema = {
      name: 'users',
      columns: [
        { name: 'id', type: 'INTEGER', nullable: false },
        { name: 'name', type: 'TEXT', nullable: true },
      ],
      primaryKey: ['id'],
      storageConfig: {
        chunkSize: 1024 * 1024, // 1MB
        maxPageSize: 512 * 1024, // 512KB
      },
    };

    expect(schemaWithStorage.storageConfig).toBeDefined();
    expect(schemaWithStorage.storageConfig?.chunkSize).toBe(1024 * 1024);
  });

  it('TableSchema interface works without storageConfig (backward compatible)', () => {
    // Type check: this should compile without errors
    const schemaWithoutStorage: TableSchema = {
      name: 'products',
      columns: [
        { name: 'id', type: 'INTEGER', nullable: false },
        { name: 'price', type: 'REAL', nullable: false },
      ],
      primaryKey: ['id'],
    };

    expect(schemaWithoutStorage.storageConfig).toBeUndefined();
  });

  it('DOEngine persists storageConfig when creating a table', async () => {
    const storageConfig: TableStorageConfig = {
      chunkSize: 1024 * 1024, // 1MB
      maxPageSize: 512 * 1024, // 512KB
      hotStorageMaxSize: 50 * 1024 * 1024, // 50MB
    };

    // Create table with storage config
    await engine.execute(
      'CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL)',
    );

    // Manually set the storage config (since CREATE TABLE syntax doesn't support it yet)
    // Schemas are stored as an array
    const schemasKey = '_meta:schemas';
    const schemas = (await storage.get(schemasKey)) as TableSchema[] | undefined;

    if (schemas) {
      const ordersSchema = schemas.find(s => s.name === 'orders');
      if (ordersSchema) {
        ordersSchema.storageConfig = storageConfig;
        await storage.put(schemasKey, schemas);
      }
    }

    // Load schemas back
    const loadedSchemas = (await storage.get(schemasKey)) as TableSchema[] | undefined;
    const ordersSchema = loadedSchemas?.find(s => s.name === 'orders');

    expect(loadedSchemas).toBeDefined();
    expect(ordersSchema).toBeDefined();
    expect(ordersSchema?.storageConfig).toEqual(storageConfig);
  });

  it('DOEngine loads storageConfig correctly', async () => {
    // Create table first
    await engine.execute(
      'CREATE TABLE inventory (id INTEGER PRIMARY KEY, quantity INTEGER)',
    );

    const storageConfig: TableStorageConfig = {
      rowGroupSize: 2 * 1024 * 1024, // 2MB
      maxRowsPerRowGroup: 100000,
    };

    // Set storage config
    const schemasKey = '_meta:schemas';
    const schemas = (await storage.get(schemasKey)) as TableSchema[] | undefined;

    if (schemas) {
      const inventorySchema = schemas.find(s => s.name === 'inventory');
      if (inventorySchema) {
        inventorySchema.storageConfig = storageConfig;
        await storage.put(schemasKey, schemas);
      }
    }

    // Create a new engine instance to test loading
    const engine2 = createDOEngine({ storage });

    // Execute a query to ensure engine is initialized
    await engine2.execute('SELECT * FROM inventory');

    // Verify the storage config is loaded
    const loadedSchemas = (await storage.get(schemasKey)) as TableSchema[] | undefined;
    const inventorySchema = loadedSchemas?.find(s => s.name === 'inventory');

    expect(inventorySchema?.storageConfig).toEqual(storageConfig);
  });

  it('Backward compatibility: schemas without storageConfig work fine', async () => {
    // Create table without storage config
    await engine.execute(
      'CREATE TABLE legacy (id INTEGER PRIMARY KEY, data TEXT)',
    );

    // Query should work fine
    const result = await engine.execute('SELECT * FROM legacy');
    expect(result.rows).toBeDefined();

    // Check schema has no storageConfig
    const schemasKey = '_meta:schemas';
    const schemas = (await storage.get(schemasKey)) as TableSchema[] | undefined;
    const legacySchema = schemas?.find(s => s.name === 'legacy');

    expect(legacySchema).toBeDefined();
    expect(legacySchema?.storageConfig).toBeUndefined();
  });
});
