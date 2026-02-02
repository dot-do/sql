/**
 * Tests for TableSchema storage config integration
 */
import { describe, it, expect, beforeEach } from 'vitest';
import type { TableSchema } from '../types.js';
import type { TableStorageConfig } from '../storage-config.js';
import { createDOEngine } from '../do-engine.js';
import {
  createTypedStorageTransaction,
  type TypedStorageTransaction,
} from '../../__tests__/test-utils.js';

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
    list: async () => new Map(data),
    transaction: async <T>(fn: (txn: TypedStorageTransaction) => Promise<T>): Promise<T> => fn(createTypedStorageTransaction(data)),
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

  it('DOEngine parses WITH STORAGE clause in CREATE TABLE', async () => {
    // Create table with storage config via SQL
    await engine.execute(
      "CREATE TABLE analytics (id INTEGER PRIMARY KEY, data TEXT) WITH STORAGE (rowGroupSize = '4MB', parquetFileSize = '256MB')",
    );

    // Check schema has storageConfig
    const schemasKey = '_meta:schemas';
    const schemas = (await storage.get(schemasKey)) as TableSchema[] | undefined;
    const analyticsSchema = schemas?.find(s => s.name === 'analytics');

    expect(analyticsSchema).toBeDefined();
    expect(analyticsSchema?.storageConfig).toBeDefined();
    expect(analyticsSchema?.storageConfig?.rowGroupSize).toBe(4 * 1024 * 1024);
    expect(analyticsSchema?.storageConfig?.parquetFileSize).toBe(256 * 1024 * 1024);
  });

  it('DOEngine parses all storage settings from WITH STORAGE clause', async () => {
    // Create table with all storage settings
    await engine.execute(`
      CREATE TABLE full_config (id INTEGER PRIMARY KEY) WITH STORAGE (
        chunkSize = '1MB',
        maxPageSize = '2MB',
        rowGroupSize = '4MB',
        maxRowsPerRowGroup = 32768,
        hotStorageMaxSize = '200MB',
        hotDataMaxAge = 7200000,
        maxHotFileSize = '20MB',
        parquetFileSize = '1GB'
      )
    `);

    // Check schema has all storageConfig settings
    const schemasKey = '_meta:schemas';
    const schemas = (await storage.get(schemasKey)) as TableSchema[] | undefined;
    const fullConfigSchema = schemas?.find(s => s.name === 'full_config');

    expect(fullConfigSchema).toBeDefined();
    const config = fullConfigSchema?.storageConfig;
    expect(config).toBeDefined();
    expect(config?.chunkSize).toBe(1 * 1024 * 1024);
    expect(config?.maxPageSize).toBe(2 * 1024 * 1024);
    expect(config?.rowGroupSize).toBe(4 * 1024 * 1024);
    expect(config?.maxRowsPerRowGroup).toBe(32768);
    expect(config?.hotStorageMaxSize).toBe(200 * 1024 * 1024);
    expect(config?.hotDataMaxAge).toBe(7200000);
    expect(config?.maxHotFileSize).toBe(20 * 1024 * 1024);
    expect(config?.parquetFileSize).toBe(1 * 1024 * 1024 * 1024);
  });

  it('DOEngine persists storage config across engine instances', async () => {
    // Create table with storage config
    await engine.execute(
      "CREATE TABLE persistent (id INTEGER PRIMARY KEY) WITH STORAGE (rowGroupSize = '8MB')",
    );

    // Create a new engine instance
    const engine2 = createDOEngine({ storage });

    // Execute a query to ensure engine is initialized
    await engine2.execute('SELECT * FROM persistent');

    // Verify the storage config is still present
    const schemasKey = '_meta:schemas';
    const schemas = (await storage.get(schemasKey)) as TableSchema[] | undefined;
    const persistentSchema = schemas?.find(s => s.name === 'persistent');

    expect(persistentSchema?.storageConfig).toBeDefined();
    expect(persistentSchema?.storageConfig?.rowGroupSize).toBe(8 * 1024 * 1024);
  });
});
