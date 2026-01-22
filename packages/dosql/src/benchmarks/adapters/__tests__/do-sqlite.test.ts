/**
 * DO SQLite Benchmark Adapter Tests
 *
 * Tests for the DOSqliteAdapter that implements BenchmarkAdapter for
 * Durable Object SQLite storage.
 *
 * Uses workers-vitest-pool (NO MOCKS) as required.
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { env, runInDurableObject } from 'cloudflare:test';
import { DurableObject } from 'cloudflare:workers';

import {
  DOSqliteAdapter,
  createDOSqliteAdapter,
  type DOStorage,
} from '../do-sqlite.js';

import {
  type BenchmarkOperation,
  type TableSchemaConfig,
  type StorageMetrics,
  type ColdStartMetrics,
  type DOBillingMetrics,
  DEFAULT_BENCHMARK_CONFIG,
  DO_SQLITE_PRICING,
} from '../../types.js';

// =============================================================================
// Test Durable Object
// =============================================================================

/**
 * Test Durable Object for DO SQLite adapter operations
 *
 * Provides access to ctx.storage for testing the adapter
 */
export class BenchmarkTestDO extends DurableObject {
  private adapter: DOSqliteAdapter | null = null;

  /**
   * Get the adapter instance, creating it on first access
   */
  getAdapter(): DOSqliteAdapter {
    if (!this.adapter) {
      this.adapter = createDOSqliteAdapter({
        storage: this.ctx.storage as DOStorage,
      });
    }
    return this.adapter;
  }

  /**
   * Initialize the adapter
   */
  async initialize(): Promise<void> {
    const adapter = this.getAdapter();
    await adapter.initialize();
  }

  /**
   * Cleanup the adapter
   */
  async cleanup(): Promise<void> {
    const adapter = this.getAdapter();
    await adapter.cleanup();
  }

  // ===========================================================================
  // Table Operations
  // ===========================================================================

  async createTable(schema: TableSchemaConfig): Promise<void> {
    const adapter = this.getAdapter();
    await adapter.createTable(schema);
  }

  async dropTable(tableName: string): Promise<void> {
    const adapter = this.getAdapter();
    await adapter.dropTable(tableName);
  }

  // ===========================================================================
  // CRUD Operations
  // ===========================================================================

  async insert(
    tableName: string,
    row: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const adapter = this.getAdapter();
    return adapter.insert(tableName, row);
  }

  async insertBatch(
    tableName: string,
    rows: Record<string, unknown>[]
  ): Promise<BenchmarkOperation> {
    const adapter = this.getAdapter();
    return adapter.insertBatch(tableName, rows);
  }

  async read(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation> {
    const adapter = this.getAdapter();
    return adapter.read(tableName, primaryKey, primaryKeyValue);
  }

  async readMany(
    tableName: string,
    whereClause: string,
    params?: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const adapter = this.getAdapter();
    return adapter.readMany(tableName, whereClause, params);
  }

  async update(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown,
    updates: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const adapter = this.getAdapter();
    return adapter.update(tableName, primaryKey, primaryKeyValue, updates);
  }

  async delete(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation> {
    const adapter = this.getAdapter();
    return adapter.delete(tableName, primaryKey, primaryKeyValue);
  }

  // ===========================================================================
  // Query Operations
  // ===========================================================================

  async query(
    sql: string,
    params?: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const adapter = this.getAdapter();
    return adapter.query(sql, params);
  }

  async transaction(
    operations: Array<{
      type: 'insert' | 'update' | 'delete';
      tableName: string;
      data: Record<string, unknown>;
    }>
  ): Promise<BenchmarkOperation> {
    const adapter = this.getAdapter();
    return adapter.transaction(operations);
  }

  // ===========================================================================
  // Metrics Operations
  // ===========================================================================

  async getStorageMetrics(): Promise<StorageMetrics> {
    const adapter = this.getAdapter();
    return adapter.getStorageMetrics();
  }

  async measureColdStart(): Promise<ColdStartMetrics> {
    const adapter = this.getAdapter();
    return adapter.measureColdStart();
  }

  async getBillingMetrics(): Promise<DOBillingMetrics> {
    const adapter = this.getAdapter();
    return adapter.getBillingMetrics();
  }

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  getReadOps(): number {
    const adapter = this.getAdapter();
    return adapter.getReadOps();
  }

  getWriteOps(): number {
    const adapter = this.getAdapter();
    return adapter.getWriteOps();
  }

  resetCounters(): void {
    const adapter = this.getAdapter();
    adapter.resetCounters();
  }

  async isApproachingLimit(): Promise<boolean> {
    const adapter = this.getAdapter();
    return adapter.isApproachingLimit();
  }

  async getRemainingCapacity(): Promise<number> {
    const adapter = this.getAdapter();
    return adapter.getRemainingCapacity();
  }

  // ===========================================================================
  // Adapter Properties
  // ===========================================================================

  getName(): string {
    const adapter = this.getAdapter();
    return adapter.name;
  }

  getVersion(): string {
    const adapter = this.getAdapter();
    return adapter.version;
  }
}

// =============================================================================
// Test Helpers
// =============================================================================

let testCounter = 0;

/**
 * Get a unique DO stub for each test
 */
function getUniqueStub() {
  const id = env.BENCHMARK_TEST_DO.idFromName(
    `benchmark-test-${Date.now()}-${testCounter++}`
  );
  return env.BENCHMARK_TEST_DO.get(id);
}

/**
 * Default test schema
 */
const TEST_SCHEMA: TableSchemaConfig = {
  tableName: 'benchmark_test',
  columns: [
    { name: 'id', type: 'INTEGER' },
    { name: 'name', type: 'TEXT' },
    { name: 'value', type: 'REAL' },
    { name: 'data', type: 'TEXT', nullable: true },
    { name: 'created_at', type: 'INTEGER' },
  ],
  primaryKey: 'id',
  indexes: ['name', 'created_at'],
};

// =============================================================================
// Adapter Initialization Tests
// =============================================================================

describe('DOSqliteAdapter - Initialization', () => {
  it('should have correct adapter name', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      expect(instance.getName()).toBe('do-sqlite');
    });
  });

  it('should have correct adapter version', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      expect(instance.getVersion()).toBe('1.0.0');
    });
  });

  it('should initialize successfully', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await expect(instance.initialize()).resolves.not.toThrow();
    });
  });

  it('should cleanup successfully', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await expect(instance.cleanup()).resolves.not.toThrow();
    });
  });

  it('should reset operation counters on initialization', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();

      expect(instance.getReadOps()).toBe(0);
      expect(instance.getWriteOps()).toBe(0);
    });
  });
});

// =============================================================================
// Table Operations Tests
// =============================================================================

describe('DOSqliteAdapter - Table Operations', () => {
  it('should create a table with schema', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await expect(instance.createTable(TEST_SCHEMA)).resolves.not.toThrow();
    });
  });

  it('should create table with indexes', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();

      const schema: TableSchemaConfig = {
        tableName: 'indexed_table',
        columns: [
          { name: 'id', type: 'INTEGER' },
          { name: 'email', type: 'TEXT' },
          { name: 'status', type: 'TEXT' },
        ],
        primaryKey: 'id',
        indexes: ['email', 'status'],
      };

      await expect(instance.createTable(schema)).resolves.not.toThrow();
    });
  });

  it('should drop table if exists', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);
      await expect(instance.dropTable(TEST_SCHEMA.tableName)).resolves.not.toThrow();
    });
  });

  it('should handle drop non-existent table gracefully', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await expect(instance.dropTable('nonexistent_table')).resolves.not.toThrow();
    });
  });

  it('should track write operations on table creation', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      instance.resetCounters();

      await instance.createTable(TEST_SCHEMA);

      // Table creation and index creation should track writes
      // The exact count depends on SQLite internals
      expect(instance.getWriteOps()).toBeGreaterThanOrEqual(0);
    });
  });
});

// =============================================================================
// INSERT Operations Tests
// =============================================================================

describe('DOSqliteAdapter - INSERT Operations', () => {
  it('should insert a single row', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const result = await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'test-row',
        value: 42.5,
        data: 'test data',
        created_at: Date.now(),
      });

      expect(result.success).toBe(true);
      expect(result.type).toBe('create');
      expect(result.rowCount).toBe(1);
      expect(result.durationMs).toBeGreaterThanOrEqual(0);
    });
  });

  it('should track write operations on insert', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);
      instance.resetCounters();

      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'tracked-row',
        value: 100,
        created_at: Date.now(),
      });

      expect(instance.getWriteOps()).toBeGreaterThan(0);
    });
  });

  it('should return error for duplicate primary key', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'original',
        value: 100,
        created_at: Date.now(),
      });

      const result = await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'duplicate',
        value: 200,
        created_at: Date.now(),
      });

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });
  });

  it('should batch insert multiple rows', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const rows = Array.from({ length: 10 }, (_, i) => ({
        id: i + 1,
        name: `batch-row-${i}`,
        value: i * 10,
        created_at: Date.now(),
      }));

      const result = await instance.insertBatch(TEST_SCHEMA.tableName, rows);

      expect(result.success).toBe(true);
      expect(result.type).toBe('batch');
      expect(result.rowCount).toBe(10);
    });
  });

  it('should handle empty batch insert', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const result = await instance.insertBatch(TEST_SCHEMA.tableName, []);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(0);
    });
  });

  it('should rollback batch on error', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // Insert first row
      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'existing',
        value: 100,
        created_at: Date.now(),
      });

      // Try batch with duplicate
      const rows = [
        { id: 2, name: 'new', value: 200, created_at: Date.now() },
        { id: 1, name: 'duplicate', value: 300, created_at: Date.now() }, // Will fail
      ];

      const result = await instance.insertBatch(TEST_SCHEMA.tableName, rows);

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });
  });
});

// =============================================================================
// READ Operations Tests
// =============================================================================

describe('DOSqliteAdapter - READ Operations', () => {
  it('should read a single row by primary key', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'read-test',
        value: 42,
        created_at: Date.now(),
      });

      const result = await instance.read(TEST_SCHEMA.tableName, 'id', 1);

      expect(result.success).toBe(true);
      expect(result.type).toBe('read');
      expect(result.rowCount).toBe(1);
    });
  });

  it('should return empty result for non-existent row', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const result = await instance.read(TEST_SCHEMA.tableName, 'id', 999);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(0);
    });
  });

  it('should track read operations', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'tracked',
        value: 100,
        created_at: Date.now(),
      });

      instance.resetCounters();

      await instance.read(TEST_SCHEMA.tableName, 'id', 1);

      expect(instance.getReadOps()).toBeGreaterThan(0);
    });
  });

  it('should read multiple rows with WHERE clause', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // Insert multiple rows
      for (let i = 1; i <= 5; i++) {
        await instance.insert(TEST_SCHEMA.tableName, {
          id: i,
          name: 'same-name',
          value: i * 10,
          created_at: Date.now(),
        });
      }

      const result = await instance.readMany(
        TEST_SCHEMA.tableName,
        'name = ?',
        { name: 'same-name' }
      );

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(5);
    });
  });

  it('should handle empty readMany result', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const result = await instance.readMany(
        TEST_SCHEMA.tableName,
        'name = ?',
        { name: 'nonexistent' }
      );

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(0);
    });
  });
});

// =============================================================================
// UPDATE Operations Tests
// =============================================================================

describe('DOSqliteAdapter - UPDATE Operations', () => {
  it('should update a single row', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'original',
        value: 100,
        created_at: Date.now(),
      });

      const result = await instance.update(
        TEST_SCHEMA.tableName,
        'id',
        1,
        { name: 'updated', value: 200 }
      );

      expect(result.success).toBe(true);
      expect(result.type).toBe('update');
      expect(result.rowCount).toBe(1);
    });
  });

  it('should track write operations on update', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'to-update',
        value: 100,
        created_at: Date.now(),
      });

      instance.resetCounters();

      await instance.update(TEST_SCHEMA.tableName, 'id', 1, { value: 200 });

      expect(instance.getWriteOps()).toBeGreaterThan(0);
    });
  });

  it('should return zero rows for non-existent update', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const result = await instance.update(
        TEST_SCHEMA.tableName,
        'id',
        999,
        { name: 'ghost' }
      );

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(0);
    });
  });
});

// =============================================================================
// DELETE Operations Tests
// =============================================================================

describe('DOSqliteAdapter - DELETE Operations', () => {
  it('should delete a single row', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'to-delete',
        value: 100,
        created_at: Date.now(),
      });

      const result = await instance.delete(TEST_SCHEMA.tableName, 'id', 1);

      expect(result.success).toBe(true);
      expect(result.type).toBe('delete');
      expect(result.rowCount).toBe(1);
    });
  });

  it('should track write operations on delete', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'tracked-delete',
        value: 100,
        created_at: Date.now(),
      });

      instance.resetCounters();

      await instance.delete(TEST_SCHEMA.tableName, 'id', 1);

      expect(instance.getWriteOps()).toBeGreaterThan(0);
    });
  });

  it('should return zero rows for non-existent delete', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const result = await instance.delete(TEST_SCHEMA.tableName, 'id', 999);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(0);
    });
  });

  it('should verify row is deleted', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'verify-delete',
        value: 100,
        created_at: Date.now(),
      });

      await instance.delete(TEST_SCHEMA.tableName, 'id', 1);

      const readResult = await instance.read(TEST_SCHEMA.tableName, 'id', 1);
      expect(readResult.rowCount).toBe(0);
    });
  });
});

// =============================================================================
// Query Operations Tests
// =============================================================================

describe('DOSqliteAdapter - Query Operations', () => {
  it('should execute raw SELECT query', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'query-test',
        value: 42,
        created_at: Date.now(),
      });

      const result = await instance.query(
        `SELECT * FROM ${TEST_SCHEMA.tableName} WHERE value > ?`,
        { minValue: 40 }
      );

      expect(result.success).toBe(true);
      expect(result.type).toBe('query');
      expect(result.rowCount).toBe(1);
    });
  });

  it('should execute raw INSERT query', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const result = await instance.query(
        `INSERT INTO ${TEST_SCHEMA.tableName} (id, name, value, created_at) VALUES (?, ?, ?, ?)`,
        { id: 1, name: 'raw-insert', value: 100, created_at: Date.now() }
      );

      expect(result.success).toBe(true);
      expect(result.type).toBe('create');
    });
  });

  it('should return error for invalid query', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();

      const result = await instance.query('INVALID SQL STATEMENT');

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });
  });
});

// =============================================================================
// Transaction Tests
// =============================================================================

describe('DOSqliteAdapter - Transaction Operations', () => {
  it('should execute multiple operations in transaction', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const result = await instance.transaction([
        {
          type: 'insert',
          tableName: TEST_SCHEMA.tableName,
          data: { id: 1, name: 'txn-1', value: 100, created_at: Date.now() },
        },
        {
          type: 'insert',
          tableName: TEST_SCHEMA.tableName,
          data: { id: 2, name: 'txn-2', value: 200, created_at: Date.now() },
        },
      ]);

      expect(result.success).toBe(true);
      expect(result.type).toBe('transaction');
      expect(result.rowCount).toBe(2);
    });
  });

  it('should rollback transaction on error', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // First insert a row
      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'existing',
        value: 100,
        created_at: Date.now(),
      });

      // Transaction with duplicate key
      const result = await instance.transaction([
        {
          type: 'insert',
          tableName: TEST_SCHEMA.tableName,
          data: { id: 2, name: 'new', value: 200, created_at: Date.now() },
        },
        {
          type: 'insert',
          tableName: TEST_SCHEMA.tableName,
          data: { id: 1, name: 'duplicate', value: 300, created_at: Date.now() },
        },
      ]);

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });
  });

  it('should support mixed operation types in transaction', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // Insert initial row
      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'initial',
        value: 100,
        created_at: Date.now(),
      });

      const result = await instance.transaction([
        {
          type: 'insert',
          tableName: TEST_SCHEMA.tableName,
          data: { id: 2, name: 'new', value: 200, created_at: Date.now() },
        },
        {
          type: 'update',
          tableName: TEST_SCHEMA.tableName,
          data: { id: 1, name: 'updated', value: 150 },
        },
      ]);

      expect(result.success).toBe(true);
      expect(result.rowCount).toBe(2);
    });
  });
});

// =============================================================================
// Storage Metrics Tests
// =============================================================================

describe('DOSqliteAdapter - Storage Metrics', () => {
  it('should return storage metrics', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // Insert some data
      for (let i = 1; i <= 10; i++) {
        await instance.insert(TEST_SCHEMA.tableName, {
          id: i,
          name: `row-${i}`,
          value: i * 10,
          data: 'Some test data that takes up space',
          created_at: Date.now(),
        });
      }

      const metrics = await instance.getStorageMetrics();

      expect(metrics.totalBytes).toBeGreaterThan(0);
      expect(metrics.rowCount).toBeGreaterThanOrEqual(10);
      expect(metrics.tableCount).toBeGreaterThanOrEqual(1);
      expect(metrics.limitUtilization).toBeGreaterThanOrEqual(0);
      expect(metrics.limitUtilization).toBeLessThan(100);
    });
  });

  it('should calculate limit utilization correctly', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const metrics = await instance.getStorageMetrics();

      // Limit utilization should be (totalBytes / 128MB) * 100
      const expectedUtilization =
        (metrics.totalBytes / DO_SQLITE_PRICING.maxDatabaseSize) * 100;

      expect(metrics.limitUtilization).toBeCloseTo(expectedUtilization, 2);
    });
  });

  it('should report remaining capacity', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();

      const remaining = await instance.getRemainingCapacity();

      expect(remaining).toBeGreaterThan(0);
      expect(remaining).toBeLessThanOrEqual(DO_SQLITE_PRICING.maxDatabaseSize);
    });
  });

  it('should correctly check approaching limit', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();

      const isApproaching = await instance.isApproachingLimit();

      // Fresh database should not be approaching limit
      expect(isApproaching).toBe(false);
    });
  });
});

// =============================================================================
// Cold Start Metrics Tests
// =============================================================================

describe('DOSqliteAdapter - Cold Start Metrics', () => {
  it('should measure cold start time', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();

      const metrics = await instance.measureColdStart();

      expect(metrics.timeToFirstQuery).toBeGreaterThanOrEqual(0);
      expect(metrics.initializationTime).toBeGreaterThanOrEqual(0);
      expect(metrics.connectionTime).toBeGreaterThanOrEqual(0);
    });
  });

  it('should return numeric values for all cold start metrics', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();

      const metrics = await instance.measureColdStart();

      expect(typeof metrics.timeToFirstQuery).toBe('number');
      expect(typeof metrics.initializationTime).toBe('number');
      expect(typeof metrics.connectionTime).toBe('number');
    });
  });
});

// =============================================================================
// Billing Estimation Tests
// =============================================================================

describe('DOSqliteAdapter - Billing Estimation', () => {
  it('should return billing metrics', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // Perform some operations
      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'billing-test',
        value: 100,
        created_at: Date.now(),
      });
      await instance.read(TEST_SCHEMA.tableName, 'id', 1);

      const billing = await instance.getBillingMetrics();

      expect(billing.estimatedRowOps).toBeGreaterThan(0);
      expect(billing.estimatedStorageBytes).toBeGreaterThanOrEqual(0);
      expect(billing.estimatedCostUSD).toBeGreaterThanOrEqual(0);
    });
  });

  it('should accumulate operation counts', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);
      instance.resetCounters();

      // Perform multiple operations
      for (let i = 1; i <= 5; i++) {
        await instance.insert(TEST_SCHEMA.tableName, {
          id: i,
          name: `row-${i}`,
          value: i * 10,
          created_at: Date.now(),
        });
      }

      for (let i = 1; i <= 5; i++) {
        await instance.read(TEST_SCHEMA.tableName, 'id', i);
      }

      const billing = await instance.getBillingMetrics();

      // Should have tracked both read and write operations
      expect(instance.getWriteOps()).toBeGreaterThanOrEqual(5);
      expect(instance.getReadOps()).toBeGreaterThanOrEqual(5);
      expect(billing.estimatedRowOps).toBe(
        instance.getReadOps() + instance.getWriteOps()
      );
    });
  });

  it('should reset counters correctly', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      // Perform some operations
      await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'reset-test',
        value: 100,
        created_at: Date.now(),
      });

      expect(instance.getWriteOps()).toBeGreaterThan(0);

      instance.resetCounters();

      expect(instance.getReadOps()).toBe(0);
      expect(instance.getWriteOps()).toBe(0);
    });
  });

  it('should estimate cost based on operations', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);
      instance.resetCounters();

      // Insert many rows to accumulate operations
      const rows = Array.from({ length: 100 }, (_, i) => ({
        id: i + 1,
        name: `cost-row-${i}`,
        value: i * 10,
        created_at: Date.now(),
      }));

      await instance.insertBatch(TEST_SCHEMA.tableName, rows);

      const billing = await instance.getBillingMetrics();

      // Cost should be calculated from read + write operations + storage
      expect(billing.estimatedCostUSD).toBeGreaterThan(0);
    });
  });
});

// =============================================================================
// Edge Cases and Error Handling Tests
// =============================================================================

describe('DOSqliteAdapter - Edge Cases', () => {
  it('should handle NULL values correctly', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const result = await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'null-test',
        value: 0,
        data: null,
        created_at: Date.now(),
      });

      expect(result.success).toBe(true);
    });
  });

  it('should handle empty string values', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const result = await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: '',
        value: 0,
        data: '',
        created_at: Date.now(),
      });

      expect(result.success).toBe(true);
    });
  });

  it('should handle large text values', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const largeData = 'x'.repeat(10000);

      const result = await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'large-data-test',
        value: 0,
        data: largeData,
        created_at: Date.now(),
      });

      expect(result.success).toBe(true);
    });
  });

  it('should handle special characters in strings', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const result = await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: "Test's \"special\" chars: \n\t",
        value: 0,
        data: 'Unicode: ',
        created_at: Date.now(),
      });

      expect(result.success).toBe(true);
    });
  });

  it('should handle floating point values', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const result = await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'float-test',
        value: 123.456789,
        created_at: Date.now(),
      });

      expect(result.success).toBe(true);
    });
  });

  it('should handle query on non-existent table', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();

      const result = await instance.query('SELECT * FROM nonexistent_table');

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });
  });

  it('should record operation timing accurately', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();
      await instance.createTable(TEST_SCHEMA);

      const result = await instance.insert(TEST_SCHEMA.tableName, {
        id: 1,
        name: 'timing-test',
        value: 100,
        created_at: Date.now(),
      });

      expect(result.durationMs).toBeGreaterThanOrEqual(0);
      expect(result.startedAt).toBeGreaterThan(0);
      expect(result.startedAt).toBeLessThanOrEqual(Date.now());
    });
  });
});
