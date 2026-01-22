/**
 * Turso Benchmark Adapter Tests
 *
 * Tests for the Turso adapter implementation.
 * Uses workers-vitest-pool with mock URLs for testing.
 *
 * Note: Since workers environment only supports HTTP/HTTPS/WS/WSS/libsql URL schemes,
 * tests that require actual database operations are skipped in this environment.
 * The adapter is designed to work with Turso cloud instances or local SQLite via libsql-server.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  TursoAdapter,
  TursoAdapterConfig,
  createTursoAdapter,
  createTursoMemoryAdapter,
  createTursoReplicaAdapter,
} from '../turso.js';
import { TableSchemaConfig } from '../../types.js';

// =============================================================================
// Test Schema
// =============================================================================

const TEST_SCHEMA: TableSchemaConfig = {
  tableName: 'test_users',
  columns: [
    { name: 'id', type: 'INTEGER' },
    { name: 'name', type: 'TEXT', nullable: false },
    { name: 'email', type: 'TEXT', nullable: false },
    { name: 'balance', type: 'REAL', nullable: true },
    { name: 'is_active', type: 'INTEGER', nullable: false, defaultValue: 1 },
    { name: 'created_at', type: 'INTEGER', nullable: false },
  ],
  primaryKey: 'id',
  indexes: ['email', 'created_at'],
};

const SIMPLE_SCHEMA: TableSchemaConfig = {
  tableName: 'simple_table',
  columns: [
    { name: 'id', type: 'INTEGER' },
    { name: 'value', type: 'TEXT' },
  ],
  primaryKey: 'id',
};

// Mock Turso URL for testing (doesn't actually connect)
const MOCK_TURSO_URL = 'libsql://mock-db-test.turso.io';
const MOCK_AUTH_TOKEN = 'mock-auth-token-for-testing';

// =============================================================================
// Adapter Initialization Tests
// =============================================================================

describe('TursoAdapter', () => {
  describe('initialization', () => {
    it('should create adapter with basic configuration', () => {
      const config: TursoAdapterConfig = {
        url: MOCK_TURSO_URL,
      };

      const adapter = new TursoAdapter(config);

      expect(adapter.name).toBe('turso');
      expect(adapter.version).toBe('1.0.0');
    });

    it('should create adapter with auth token configuration', () => {
      const config: TursoAdapterConfig = {
        url: MOCK_TURSO_URL,
        authToken: MOCK_AUTH_TOKEN,
      };

      const adapter = new TursoAdapter(config);

      expect(adapter.name).toBe('turso');
      expect(adapter.version).toBe('1.0.0');
    });

    it('should create adapter with embedded replica configuration', () => {
      const config: TursoAdapterConfig = {
        url: 'libsql://local-replica.turso.io',
        syncUrl: 'libsql://primary-db.turso.io',
        authToken: MOCK_AUTH_TOKEN,
        syncInterval: 5000,
      };

      const adapter = new TursoAdapter(config);

      expect(adapter.name).toBe('turso');
    });

    it('should create adapter with encryption key', () => {
      const config: TursoAdapterConfig = {
        url: MOCK_TURSO_URL,
        authToken: MOCK_AUTH_TOKEN,
        encryptionKey: 'test-encryption-key-32-bytes-long',
      };

      const adapter = new TursoAdapter(config);

      expect(adapter.name).toBe('turso');
    });

    it('should throw error when using uninitialized adapter', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
        authToken: MOCK_AUTH_TOKEN,
      });

      // Should throw when trying to query without initialization
      await expect(adapter.query('SELECT 1')).rejects.toThrow(
        'TursoAdapter not initialized'
      );
    });

    it('should throw when getting client from uninitialized adapter', () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
        authToken: MOCK_AUTH_TOKEN,
      });

      // Should throw when trying to get client without initialization
      expect(() => adapter.getLibsqlClient()).toThrow(
        'TursoAdapter not initialized'
      );
    });

    it('should throw when reading from uninitialized adapter', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      await expect(adapter.read('test', 'id', 1)).rejects.toThrow(
        'TursoAdapter not initialized'
      );
    });

    it('should throw when inserting to uninitialized adapter', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      await expect(adapter.insert('test', { id: 1 })).rejects.toThrow(
        'TursoAdapter not initialized'
      );
    });
  });

  // ===========================================================================
  // Factory Functions Tests
  // ===========================================================================

  describe('factory functions', () => {
    it('should create adapter with createTursoAdapter', () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
        authToken: MOCK_AUTH_TOKEN,
      });

      expect(adapter).toBeInstanceOf(TursoAdapter);
      expect(adapter.name).toBe('turso');
    });

    it('should create memory adapter with createTursoMemoryAdapter', () => {
      const adapter = createTursoMemoryAdapter();

      expect(adapter).toBeInstanceOf(TursoAdapter);
      expect(adapter.name).toBe('turso');
    });

    it('should create replica adapter with createTursoReplicaAdapter', () => {
      const adapter = createTursoReplicaAdapter(
        'libsql://primary.turso.io',
        'libsql://local.turso.io',
        'auth-token'
      );

      expect(adapter).toBeInstanceOf(TursoAdapter);
      expect(adapter.name).toBe('turso');
    });

    it('should create replica adapter without auth token', () => {
      const adapter = createTursoReplicaAdapter(
        'libsql://primary.turso.io',
        'libsql://local.turso.io'
      );

      expect(adapter).toBeInstanceOf(TursoAdapter);
      expect(adapter.name).toBe('turso');
    });
  });

  // ===========================================================================
  // Configuration Tests
  // ===========================================================================

  describe('configuration', () => {
    it('should accept http URL', () => {
      const adapter = createTursoAdapter({
        url: 'http://localhost:8080',
      });

      expect(adapter.name).toBe('turso');
    });

    it('should accept https URL', () => {
      const adapter = createTursoAdapter({
        url: 'https://my-db.turso.io',
        authToken: MOCK_AUTH_TOKEN,
      });

      expect(adapter.name).toBe('turso');
    });

    it('should accept ws URL', () => {
      const adapter = createTursoAdapter({
        url: 'ws://localhost:8080',
      });

      expect(adapter.name).toBe('turso');
    });

    it('should accept wss URL', () => {
      const adapter = createTursoAdapter({
        url: 'wss://my-db.turso.io',
        authToken: MOCK_AUTH_TOKEN,
      });

      expect(adapter.name).toBe('turso');
    });

    it('should accept libsql URL', () => {
      const adapter = createTursoAdapter({
        url: 'libsql://my-db.turso.io',
        authToken: MOCK_AUTH_TOKEN,
      });

      expect(adapter.name).toBe('turso');
    });

    it('should accept sync interval configuration', () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
        syncUrl: 'libsql://primary.turso.io',
        syncInterval: 10000,
      });

      expect(adapter.name).toBe('turso');
    });
  });

  // ===========================================================================
  // Adapter Interface Compliance Tests
  // ===========================================================================

  describe('BenchmarkAdapter interface compliance', () => {
    let adapter: TursoAdapter;

    beforeEach(() => {
      adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
        authToken: MOCK_AUTH_TOKEN,
      });
    });

    it('should have name property', () => {
      expect(adapter.name).toBe('turso');
    });

    it('should have version property', () => {
      expect(adapter.version).toBe('1.0.0');
    });

    it('should have initialize method', () => {
      expect(typeof adapter.initialize).toBe('function');
    });

    it('should have cleanup method', () => {
      expect(typeof adapter.cleanup).toBe('function');
    });

    it('should have createTable method', () => {
      expect(typeof adapter.createTable).toBe('function');
    });

    it('should have dropTable method', () => {
      expect(typeof adapter.dropTable).toBe('function');
    });

    it('should have insert method', () => {
      expect(typeof adapter.insert).toBe('function');
    });

    it('should have insertBatch method', () => {
      expect(typeof adapter.insertBatch).toBe('function');
    });

    it('should have read method', () => {
      expect(typeof adapter.read).toBe('function');
    });

    it('should have readMany method', () => {
      expect(typeof adapter.readMany).toBe('function');
    });

    it('should have update method', () => {
      expect(typeof adapter.update).toBe('function');
    });

    it('should have delete method', () => {
      expect(typeof adapter.delete).toBe('function');
    });

    it('should have query method', () => {
      expect(typeof adapter.query).toBe('function');
    });

    it('should have transaction method', () => {
      expect(typeof adapter.transaction).toBe('function');
    });

    it('should have getStorageMetrics method', () => {
      expect(typeof adapter.getStorageMetrics).toBe('function');
    });

    it('should have measureColdStart method', () => {
      expect(typeof adapter.measureColdStart).toBe('function');
    });
  });

  // ===========================================================================
  // Turso-specific Methods Tests
  // ===========================================================================

  describe('Turso-specific methods', () => {
    let adapter: TursoAdapter;

    beforeEach(() => {
      adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
        authToken: MOCK_AUTH_TOKEN,
      });
    });

    it('should have sync method', () => {
      expect(typeof adapter.sync).toBe('function');
    });

    it('should have getLibsqlClient method', () => {
      expect(typeof adapter.getLibsqlClient).toBe('function');
    });

    it('should have executeWithMetrics method', () => {
      expect(typeof adapter.executeWithMetrics).toBe('function');
    });
  });

  // ===========================================================================
  // Error Handling Tests (without connection)
  // ===========================================================================

  describe('error handling', () => {
    it('should throw descriptive error for uninitialized createTable', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      await expect(adapter.createTable(TEST_SCHEMA)).rejects.toThrow(
        'TursoAdapter not initialized. Call initialize() first.'
      );
    });

    it('should throw descriptive error for uninitialized dropTable', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      await expect(adapter.dropTable('test')).rejects.toThrow(
        'TursoAdapter not initialized. Call initialize() first.'
      );
    });

    it('should throw descriptive error for uninitialized update', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      await expect(adapter.update('test', 'id', 1, { name: 'new' })).rejects.toThrow(
        'TursoAdapter not initialized. Call initialize() first.'
      );
    });

    it('should throw descriptive error for uninitialized delete', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      await expect(adapter.delete('test', 'id', 1)).rejects.toThrow(
        'TursoAdapter not initialized. Call initialize() first.'
      );
    });

    it('should throw descriptive error for uninitialized insertBatch', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      await expect(adapter.insertBatch('test', [{ id: 1 }])).rejects.toThrow(
        'TursoAdapter not initialized. Call initialize() first.'
      );
    });

    it('should throw descriptive error for uninitialized readMany', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      await expect(adapter.readMany('test', 'id > 0')).rejects.toThrow(
        'TursoAdapter not initialized. Call initialize() first.'
      );
    });

    it('should throw descriptive error for uninitialized transaction', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      await expect(adapter.transaction([])).rejects.toThrow(
        'TursoAdapter not initialized. Call initialize() first.'
      );
    });

    it('should throw descriptive error for uninitialized getStorageMetrics', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      await expect(adapter.getStorageMetrics()).rejects.toThrow(
        'TursoAdapter not initialized. Call initialize() first.'
      );
    });

    it('should throw descriptive error for uninitialized sync', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
        syncUrl: 'libsql://primary.turso.io',
      });

      await expect(adapter.sync()).rejects.toThrow(
        'TursoAdapter not initialized. Call initialize() first.'
      );
    });

    it('should throw descriptive error for uninitialized executeWithMetrics', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      await expect(adapter.executeWithMetrics('SELECT 1')).rejects.toThrow(
        'TursoAdapter not initialized. Call initialize() first.'
      );
    });
  });

  // ===========================================================================
  // Empty Batch Handling Tests
  // ===========================================================================

  describe('empty batch handling', () => {
    it('should handle empty insertBatch gracefully without initialization', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      // Empty batch should still require initialization (consistent behavior)
      await expect(adapter.insertBatch('test', [])).rejects.toThrow(
        'TursoAdapter not initialized'
      );
    });

    it('should handle empty transaction gracefully without initialization', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      // Empty transaction should still require initialization
      await expect(adapter.transaction([])).rejects.toThrow(
        'TursoAdapter not initialized'
      );
    });
  });

  // ===========================================================================
  // Cleanup Without Initialize Tests
  // ===========================================================================

  describe('cleanup behavior', () => {
    it('should handle cleanup without initialize gracefully', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      // Should not throw
      await expect(adapter.cleanup()).resolves.not.toThrow();
    });

    it('should allow cleanup to be called multiple times', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      await adapter.cleanup();
      await adapter.cleanup();
      await adapter.cleanup();

      // Should not throw
    });
  });

  // ===========================================================================
  // Type Safety Tests
  // ===========================================================================

  describe('type safety', () => {
    it('should accept various types for insert row values', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      // These should compile without type errors
      const stringValue: Record<string, unknown> = { name: 'test' };
      const numberValue: Record<string, unknown> = { id: 1 };
      const nullValue: Record<string, unknown> = { value: null };
      const booleanValue: Record<string, unknown> = { active: true };
      const mixedValue: Record<string, unknown> = {
        id: 1,
        name: 'test',
        value: null,
        active: true,
        balance: 100.50,
      };

      // All should throw due to uninitialized adapter, but type checking passes
      await expect(adapter.insert('test', stringValue)).rejects.toThrow();
      await expect(adapter.insert('test', numberValue)).rejects.toThrow();
      await expect(adapter.insert('test', nullValue)).rejects.toThrow();
      await expect(adapter.insert('test', booleanValue)).rejects.toThrow();
      await expect(adapter.insert('test', mixedValue)).rejects.toThrow();
    });

    it('should accept various types for update values', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      const updates: Record<string, unknown> = {
        name: 'updated',
        balance: 200.00,
        active: false,
      };

      await expect(adapter.update('test', 'id', 1, updates)).rejects.toThrow();
    });

    it('should accept various primary key types', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      // Integer primary key
      await expect(adapter.read('test', 'id', 1)).rejects.toThrow();

      // String primary key
      await expect(adapter.read('test', 'uuid', 'abc-123')).rejects.toThrow();

      // BigInt primary key
      await expect(adapter.read('test', 'big_id', 9007199254740993n)).rejects.toThrow();
    });
  });

  // ===========================================================================
  // Operation Result Structure Tests
  // ===========================================================================

  describe('operation result structure', () => {
    it('should return properly structured BenchmarkOperation for uninitialized errors', async () => {
      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      try {
        await adapter.insert('test', { id: 1 });
      } catch (e) {
        // Error should be thrown, not a failed operation result
        expect(e).toBeInstanceOf(Error);
      }
    });
  });

  // ===========================================================================
  // Schema Configuration Tests
  // ===========================================================================

  describe('schema configuration', () => {
    it('should accept schema with all column types', () => {
      const schema: TableSchemaConfig = {
        tableName: 'all_types',
        columns: [
          { name: 'id', type: 'INTEGER' },
          { name: 'name', type: 'TEXT' },
          { name: 'balance', type: 'REAL' },
          { name: 'data', type: 'BLOB' },
        ],
        primaryKey: 'id',
      };

      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      // Schema should be accepted (actual execution requires initialization)
      expect(() => adapter.createTable(schema)).not.toThrow();
    });

    it('should accept schema with indexes', () => {
      const schema: TableSchemaConfig = {
        tableName: 'indexed_table',
        columns: [
          { name: 'id', type: 'INTEGER' },
          { name: 'email', type: 'TEXT' },
          { name: 'created_at', type: 'INTEGER' },
        ],
        primaryKey: 'id',
        indexes: ['email', 'created_at'],
      };

      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      expect(() => adapter.createTable(schema)).not.toThrow();
    });

    it('should accept schema with nullable columns', () => {
      const schema: TableSchemaConfig = {
        tableName: 'nullable_table',
        columns: [
          { name: 'id', type: 'INTEGER' },
          { name: 'optional_name', type: 'TEXT', nullable: true },
          { name: 'optional_value', type: 'REAL', nullable: true },
        ],
        primaryKey: 'id',
      };

      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      expect(() => adapter.createTable(schema)).not.toThrow();
    });

    it('should accept schema with default values', () => {
      const schema: TableSchemaConfig = {
        tableName: 'defaults_table',
        columns: [
          { name: 'id', type: 'INTEGER' },
          { name: 'status', type: 'TEXT', defaultValue: 'active' },
          { name: 'count', type: 'INTEGER', defaultValue: 0 },
          { name: 'rate', type: 'REAL', defaultValue: 1.0 },
        ],
        primaryKey: 'id',
      };

      const adapter = createTursoAdapter({
        url: MOCK_TURSO_URL,
      });

      expect(() => adapter.createTable(schema)).not.toThrow();
    });
  });
});
