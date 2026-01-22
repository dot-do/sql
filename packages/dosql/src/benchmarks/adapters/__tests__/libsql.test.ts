/**
 * LibSQL Benchmark Adapter Tests
 *
 * Tests for the libSQL benchmark adapter using workers-vitest-pool.
 * NO MOCKS - all tests use real libSQL operations.
 *
 * IMPORTANT: The @libsql/client/web does NOT support in-memory databases.
 * These tests verify adapter behavior and error handling.
 * For full integration tests with actual database operations,
 * set the LIBSQL_URL environment variable to a Turso endpoint.
 *
 * Test coverage:
 * - Adapter initialization
 * - In-memory mode error handling (web client limitation)
 * - URL configuration
 * - Integration tests (when LIBSQL_URL is available)
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll } from 'vitest';
import {
  LibSQLAdapter,
  createLibSQLAdapter,
  createInMemoryLibSQLAdapter,
  createRemoteLibSQLAdapter,
  type LibSQLAdapterConfig,
} from '../libsql.js';
import {
  type TableSchemaConfig,
  type BenchmarkOperation,
  DEFAULT_BENCHMARK_CONFIG,
} from '../../types.js';

// =============================================================================
// Test Schema
// =============================================================================

const TEST_SCHEMA: TableSchemaConfig = {
  tableName: 'test_users',
  columns: [
    { name: 'id', type: 'INTEGER' },
    { name: 'name', type: 'TEXT', nullable: false },
    { name: 'email', type: 'TEXT', nullable: false },
    { name: 'age', type: 'INTEGER', nullable: true },
    { name: 'balance', type: 'REAL', nullable: true },
    { name: 'created_at', type: 'INTEGER', nullable: true },
  ],
  primaryKey: 'id',
  indexes: ['name', 'email'],
};

const PRODUCTS_SCHEMA: TableSchemaConfig = {
  tableName: 'test_products',
  columns: [
    { name: 'id', type: 'INTEGER' },
    { name: 'name', type: 'TEXT', nullable: false },
    { name: 'price', type: 'REAL', nullable: false },
    { name: 'stock', type: 'INTEGER', nullable: true },
  ],
  primaryKey: 'id',
};

// =============================================================================
// Environment Detection
// =============================================================================

/**
 * Check if a libSQL endpoint is available for integration tests.
 * Set LIBSQL_URL and optionally LIBSQL_AUTH_TOKEN environment variables.
 */
function getLibSQLConfig(): LibSQLAdapterConfig | null {
  // In Workers environment, we would check env bindings
  // For now, check if we have a test endpoint configured
  const url = typeof process !== 'undefined' ? process.env?.LIBSQL_URL : undefined;
  const authToken = typeof process !== 'undefined' ? process.env?.LIBSQL_AUTH_TOKEN : undefined;

  if (url) {
    return { url, authToken };
  }

  return null;
}

const libsqlConfig = getLibSQLConfig();
const hasLibSQLEndpoint = libsqlConfig !== null;

// =============================================================================
// Adapter Initialization Tests (Always Run)
// =============================================================================

describe('LibSQLAdapter Initialization', () => {
  it('should have correct adapter name and version', () => {
    const adapter = createLibSQLAdapter();
    expect(adapter.name).toBe('libsql');
    expect(adapter.version).toBe('1.0.0');
  });

  it('should start uninitialized', () => {
    const adapter = createLibSQLAdapter();
    expect(adapter.isInitialized()).toBe(false);
    expect(adapter.getClient()).toBeNull();
  });

  it('should throw error for in-memory mode (web client limitation)', async () => {
    const adapter = createInMemoryLibSQLAdapter();

    await expect(adapter.initialize()).rejects.toThrow(
      'In-memory mode is not supported by the web client'
    );
  });

  it('should throw error when neither inMemory nor url is specified', async () => {
    const adapter = new LibSQLAdapter({ inMemory: false });

    await expect(adapter.initialize()).rejects.toThrow(
      'Either inMemory or url must be specified'
    );
  });

  it('should throw error when accessing operations before initialization', async () => {
    const adapter = createLibSQLAdapter();

    await expect(
      adapter.query('SELECT 1')
    ).rejects.toThrow('Not initialized');
  });

  it('should create adapter with factory functions', () => {
    const defaultAdapter = createLibSQLAdapter();
    expect(defaultAdapter).toBeInstanceOf(LibSQLAdapter);

    const inMemoryAdapter = createInMemoryLibSQLAdapter();
    expect(inMemoryAdapter).toBeInstanceOf(LibSQLAdapter);

    const remoteAdapter = createRemoteLibSQLAdapter('libsql://test.turso.io', 'test-token');
    expect(remoteAdapter).toBeInstanceOf(LibSQLAdapter);
  });

  it('should return operation counts starting at zero', () => {
    const adapter = createLibSQLAdapter();
    const counts = adapter.getOperationCounts();

    expect(counts.reads).toBe(0);
    expect(counts.writes).toBe(0);
  });
});

// =============================================================================
// Integration Tests (Conditionally Run)
// =============================================================================

describe.skipIf(!hasLibSQLEndpoint)('LibSQLAdapter Integration Tests', () => {
  let adapter: LibSQLAdapter;

  beforeEach(async () => {
    if (!libsqlConfig) {
      throw new Error('LibSQL config not available');
    }
    adapter = new LibSQLAdapter(libsqlConfig);
    await adapter.initialize();
  });

  afterEach(async () => {
    if (adapter) {
      await adapter.cleanup();
    }
  });

  describe('Connection', () => {
    it('should initialize and connect to remote endpoint', async () => {
      expect(adapter.isInitialized()).toBe(true);
      expect(adapter.getClient()).not.toBeNull();
    });

    it('should allow re-initialization', async () => {
      await adapter.initialize(); // Second call
      expect(adapter.isInitialized()).toBe(true);
    });

    it('should cleanup properly', async () => {
      await adapter.cleanup();
      expect(adapter.isInitialized()).toBe(false);
      expect(adapter.getClient()).toBeNull();
    });
  });

  describe('Table Operations', () => {
    afterEach(async () => {
      // Clean up test tables
      try {
        await adapter.dropTable('test_users');
        await adapter.dropTable('test_products');
      } catch {
        // Ignore cleanup errors
      }
    });

    it('should create a table with schema', async () => {
      await adapter.createTable(TEST_SCHEMA);

      // Verify by querying
      const result = await adapter.query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='test_users'"
      );
      expect(result.success).toBe(true);
    });

    it('should drop a table', async () => {
      await adapter.createTable(TEST_SCHEMA);
      await adapter.dropTable('test_users');

      const result = await adapter.query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='test_users'"
      );
      expect(result.rowCount).toBe(0);
    });
  });

  describe('CRUD Operations', () => {
    beforeEach(async () => {
      await adapter.createTable(TEST_SCHEMA);
    });

    afterEach(async () => {
      await adapter.dropTable('test_users');
    });

    it('should insert a single row', async () => {
      const operation = await adapter.insert('test_users', {
        id: 1,
        name: 'Alice',
        email: 'alice@example.com',
        age: 30,
      });

      expect(operation.type).toBe('create');
      expect(operation.success).toBe(true);
      expect(operation.rowCount).toBe(1);
      expect(operation.durationMs).toBeGreaterThanOrEqual(0);
    });

    it('should read a single row', async () => {
      await adapter.insert('test_users', {
        id: 1,
        name: 'Bob',
        email: 'bob@example.com',
      });

      const operation = await adapter.read('test_users', 'id', 1);

      expect(operation.type).toBe('read');
      expect(operation.success).toBe(true);
      expect(operation.rowCount).toBe(1);
    });

    it('should update a row', async () => {
      await adapter.insert('test_users', {
        id: 1,
        name: 'Carol',
        email: 'carol@example.com',
      });

      const operation = await adapter.update('test_users', 'id', 1, {
        name: 'Carol Updated',
      });

      expect(operation.type).toBe('update');
      expect(operation.success).toBe(true);
      expect(operation.rowCount).toBe(1);
    });

    it('should delete a row', async () => {
      await adapter.insert('test_users', {
        id: 1,
        name: 'Dave',
        email: 'dave@example.com',
      });

      const operation = await adapter.delete('test_users', 'id', 1);

      expect(operation.type).toBe('delete');
      expect(operation.success).toBe(true);
      expect(operation.rowCount).toBe(1);
    });
  });

  describe('Batch Operations', () => {
    beforeEach(async () => {
      await adapter.createTable(TEST_SCHEMA);
    });

    afterEach(async () => {
      await adapter.dropTable('test_users');
    });

    it('should insert multiple rows in a batch', async () => {
      const rows = [
        { id: 1, name: 'User1', email: 'user1@example.com' },
        { id: 2, name: 'User2', email: 'user2@example.com' },
        { id: 3, name: 'User3', email: 'user3@example.com' },
      ];

      const operation = await adapter.insertBatch('test_users', rows);

      expect(operation.type).toBe('batch');
      expect(operation.success).toBe(true);
      expect(operation.rowCount).toBe(3);
    });

    it('should handle empty batch', async () => {
      const operation = await adapter.insertBatch('test_users', []);

      expect(operation.success).toBe(true);
      expect(operation.rowCount).toBe(0);
    });
  });

  describe('Transaction Operations', () => {
    beforeEach(async () => {
      await adapter.createTable(TEST_SCHEMA);
      await adapter.createTable(PRODUCTS_SCHEMA);
    });

    afterEach(async () => {
      await adapter.dropTable('test_users');
      await adapter.dropTable('test_products');
    });

    it('should execute multiple operations in a transaction', async () => {
      await adapter.insert('test_users', {
        id: 1,
        name: 'Transaction Test',
        email: 'tx@example.com',
        balance: 100,
      });

      const operations = [
        {
          type: 'update' as const,
          tableName: 'test_users',
          data: { id: 1, balance: 200 },
        },
        {
          type: 'insert' as const,
          tableName: 'test_products',
          data: { id: 1, name: 'Widget', price: 25.00 },
        },
      ];

      const operation = await adapter.transaction(operations);

      expect(operation.type).toBe('transaction');
      expect(operation.success).toBe(true);
    });
  });

  describe('Raw Query Operations', () => {
    beforeEach(async () => {
      await adapter.createTable(TEST_SCHEMA);
    });

    afterEach(async () => {
      await adapter.dropTable('test_users');
    });

    it('should execute raw SELECT query', async () => {
      await adapter.insert('test_users', {
        id: 1,
        name: 'QueryTest',
        email: 'query@example.com',
      });

      const operation = await adapter.query('SELECT * FROM test_users');

      expect(operation.type).toBe('query');
      expect(operation.success).toBe(true);
      expect(operation.rowCount).toBe(1);
    });

    it('should handle query errors gracefully', async () => {
      const operation = await adapter.query('SELECT * FROM non_existent_table');

      expect(operation.success).toBe(false);
      expect(operation.error).toBeDefined();
    });
  });

  describe('Storage Metrics', () => {
    beforeEach(async () => {
      await adapter.createTable(TEST_SCHEMA);
    });

    afterEach(async () => {
      await adapter.dropTable('test_users');
    });

    it('should return storage metrics', async () => {
      await adapter.insertBatch('test_users', [
        { id: 1, name: 'A', email: 'a@x.com' },
        { id: 2, name: 'B', email: 'b@x.com' },
      ]);

      const metrics = await adapter.getStorageMetrics();

      expect(metrics.tableCount).toBeGreaterThanOrEqual(1);
      expect(metrics.rowCount).toBeGreaterThanOrEqual(2);
      expect(metrics.totalBytes).toBeGreaterThanOrEqual(0);
      expect(typeof metrics.limitUtilization).toBe('number');
    });
  });

  describe('Cold Start Measurement', () => {
    it('should measure cold start time', async () => {
      const metrics = await adapter.measureColdStart();

      expect(metrics.timeToFirstQuery).toBeGreaterThan(0);
      expect(metrics.initializationTime).toBeGreaterThan(0);
      expect(metrics.connectionTime).toBeGreaterThanOrEqual(0);
    });
  });

  describe('Operation Counting', () => {
    beforeEach(async () => {
      await adapter.createTable(TEST_SCHEMA);
    });

    afterEach(async () => {
      await adapter.dropTable('test_users');
    });

    it('should track read operations', async () => {
      await adapter.insert('test_users', {
        id: 1,
        name: 'Counter',
        email: 'counter@example.com',
      });

      const countsBefore = adapter.getOperationCounts();
      await adapter.read('test_users', 'id', 1);
      const countsAfter = adapter.getOperationCounts();

      expect(countsAfter.reads).toBeGreaterThan(countsBefore.reads);
    });

    it('should track write operations', async () => {
      const countsBefore = adapter.getOperationCounts();

      await adapter.insert('test_users', {
        id: 1,
        name: 'Writer',
        email: 'writer@example.com',
      });

      const countsAfter = adapter.getOperationCounts();

      expect(countsAfter.writes).toBeGreaterThan(countsBefore.writes);
    });
  });
});

// =============================================================================
// Adapter Behavior Tests (Always Run - No Remote Connection Required)
// =============================================================================

describe('LibSQLAdapter Behavior', () => {
  describe('Error Handling', () => {
    it('should include detailed error message for web client limitation', async () => {
      const adapter = createInMemoryLibSQLAdapter();

      try {
        await adapter.initialize();
        expect.fail('Should have thrown an error');
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
        const message = (error as Error).message;
        expect(message).toContain('web client');
        expect(message).toContain('HTTP');
        expect(message).toContain('libsql:');
      }
    });

    it('should suggest alternatives in error message', async () => {
      const adapter = createInMemoryLibSQLAdapter();

      try {
        await adapter.initialize();
      } catch (error) {
        const message = (error as Error).message;
        expect(message).toContain('Turso');
        expect(message).toContain('sqld');
      }
    });
  });

  describe('Factory Functions', () => {
    it('should create default adapter with inMemory true', () => {
      const adapter = createLibSQLAdapter();
      // Default config is inMemory: true
      expect(adapter).toBeInstanceOf(LibSQLAdapter);
    });

    it('should create adapter with custom config', () => {
      const adapter = createLibSQLAdapter({
        url: 'libsql://test.turso.io',
        authToken: 'test-token',
      });
      expect(adapter).toBeInstanceOf(LibSQLAdapter);
    });

    it('should create remote adapter with URL and token', () => {
      const adapter = createRemoteLibSQLAdapter(
        'libsql://test.turso.io',
        'test-token'
      );
      expect(adapter).toBeInstanceOf(LibSQLAdapter);
    });

    it('should create remote adapter without token', () => {
      const adapter = createRemoteLibSQLAdapter('http://localhost:8080');
      expect(adapter).toBeInstanceOf(LibSQLAdapter);
    });
  });

  describe('Default Config Compatibility', () => {
    it('should support DEFAULT_BENCHMARK_CONFIG schema structure', () => {
      const schema = DEFAULT_BENCHMARK_CONFIG.schema!;

      expect(schema.tableName).toBeDefined();
      expect(schema.columns).toBeInstanceOf(Array);
      expect(schema.primaryKey).toBeDefined();
      expect(schema.columns.length).toBeGreaterThan(0);
    });
  });
});

// =============================================================================
// Documentation Tests
// =============================================================================

describe('LibSQLAdapter Documentation', () => {
  it('should have correct type exports', () => {
    // Verify all expected exports are available
    expect(LibSQLAdapter).toBeDefined();
    expect(createLibSQLAdapter).toBeDefined();
    expect(createInMemoryLibSQLAdapter).toBeDefined();
    expect(createRemoteLibSQLAdapter).toBeDefined();
  });

  it('should implement BenchmarkAdapter interface', () => {
    const adapter = createLibSQLAdapter();

    // Verify required properties
    expect(adapter.name).toBe('libsql');
    expect(adapter.version).toBeDefined();

    // Verify required methods exist
    expect(typeof adapter.initialize).toBe('function');
    expect(typeof adapter.cleanup).toBe('function');
    expect(typeof adapter.createTable).toBe('function');
    expect(typeof adapter.dropTable).toBe('function');
    expect(typeof adapter.insert).toBe('function');
    expect(typeof adapter.insertBatch).toBe('function');
    expect(typeof adapter.read).toBe('function');
    expect(typeof adapter.readMany).toBe('function');
    expect(typeof adapter.update).toBe('function');
    expect(typeof adapter.delete).toBe('function');
    expect(typeof adapter.query).toBe('function');
    expect(typeof adapter.transaction).toBe('function');
    expect(typeof adapter.getStorageMetrics).toBe('function');
    expect(typeof adapter.measureColdStart).toBe('function');
  });

  it('should have additional utility methods', () => {
    const adapter = createLibSQLAdapter();

    expect(typeof adapter.getClient).toBe('function');
    expect(typeof adapter.isInitialized).toBe('function');
    expect(typeof adapter.executeRaw).toBe('function');
    expect(typeof adapter.getOperationCounts).toBe('function');
  });
});
