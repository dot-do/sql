/**
 * DoSQL RPC Tests
 *
 * Tests for the CapnWeb-based RPC implementation.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  DoSQLTarget,
  MockQueryExecutor,
  isRPCError,
  createRPCError,
  RPCErrorCode,
  type QueryRequest,
  type BatchRequest,
} from './index.js';

describe('DoSQL RPC', () => {
  describe('MockQueryExecutor', () => {
    let executor: MockQueryExecutor;

    beforeEach(() => {
      executor = new MockQueryExecutor();
    });

    it('should execute SELECT queries', async () => {
      executor.addTable('users', ['id', 'name'], ['number', 'string'], [
        [1, 'Alice'],
        [2, 'Bob'],
      ]);

      const result = await executor.execute('SELECT * FROM users');

      expect(result.columns).toEqual(['id', 'name']);
      expect(result.rows).toHaveLength(2);
      expect(result.rowCount).toBe(2);
      expect(result.lsn).toBeGreaterThan(0n);
    });

    it('should apply limit and offset', async () => {
      executor.addTable('items', ['id'], ['number'], [
        [1], [2], [3], [4], [5],
      ]);

      const result = await executor.execute('SELECT * FROM items', undefined, {
        limit: 2,
        offset: 1,
      });

      expect(result.rows).toEqual([[2], [3]]);
      expect(result.rowCount).toBe(2);
    });

    it('should track LSN across queries', async () => {
      executor.addTable('test', ['id'], ['number'], [[1]]);

      const result1 = await executor.execute('SELECT * FROM test');
      const result2 = await executor.execute('SELECT * FROM test');
      const result3 = await executor.execute('SELECT * FROM test');

      expect(result1.lsn).toBeLessThan(result2.lsn);
      expect(result2.lsn).toBeLessThan(result3.lsn);
    });

    it('should return empty result for unknown tables', async () => {
      const result = await executor.execute('SELECT * FROM nonexistent');

      expect(result.columns).toEqual([]);
      expect(result.rows).toEqual([]);
      expect(result.rowCount).toBe(0);
    });

    it('should handle INSERT statements', async () => {
      const result = await executor.execute("INSERT INTO users (name) VALUES ('test')");

      expect(result.rowCount).toBe(1);
    });

    it('should handle CREATE TABLE statements', async () => {
      await executor.execute('CREATE TABLE new_table (id INTEGER)');

      const schema = await executor.getSchema(['new_table']);
      expect(schema).toHaveLength(1);
      expect(schema[0].name).toBe('new_table');
    });

    it('should throw on unsupported SQL', async () => {
      await expect(executor.execute('DROP TABLE users')).rejects.toThrow('Unsupported SQL');
    });
  });

  describe('DoSQLTarget', () => {
    let executor: MockQueryExecutor;
    let target: DoSQLTarget;

    beforeEach(() => {
      executor = new MockQueryExecutor();
      executor.addTable('users', ['id', 'name', 'email'], ['number', 'string', 'string'], [
        [1, 'Alice', 'alice@example.com'],
        [2, 'Bob', 'bob@example.com'],
        [3, 'Charlie', 'charlie@example.com'],
      ]);
      target = new DoSQLTarget(executor);
    });

    describe('query()', () => {
      it('should execute a simple query', async () => {
        const response = await target.query({
          sql: 'SELECT * FROM users',
        });

        expect(response.columns).toEqual(['id', 'name', 'email']);
        expect(response.rows).toHaveLength(3);
        expect(response.rowCount).toBe(3);
        expect(response.executionTimeMs).toBeGreaterThanOrEqual(0);
        expect(typeof response.lsn).toBe('bigint');
      });

      it('should handle parameterized queries', async () => {
        const response = await target.query({
          sql: 'SELECT * FROM users WHERE id = $1',
          params: [1],
        });

        // Mock executor doesn't actually filter, but params are passed through
        expect(response.rows).toBeDefined();
      });

      it('should respect limit option', async () => {
        const response = await target.query({
          sql: 'SELECT * FROM users',
          limit: 2,
        });

        expect(response.rows).toHaveLength(2);
        expect(response.hasMore).toBe(true);
      });
    });

    describe('batch()', () => {
      it('should execute multiple queries', async () => {
        const request: BatchRequest = {
          queries: [
            { sql: 'SELECT * FROM users' },
            { sql: 'SELECT * FROM users' },
          ],
        };

        const response = await target.batch(request);

        expect(response.successCount).toBe(2);
        expect(response.errorCount).toBe(0);
        expect(response.results).toHaveLength(2);
      });

      it('should continue on error when specified', async () => {
        const request: BatchRequest = {
          queries: [
            { sql: 'SELECT * FROM users' },
            { sql: 'DROP TABLE users' }, // This will fail
            { sql: 'SELECT * FROM users' },
          ],
          continueOnError: true,
        };

        const response = await target.batch(request);

        expect(response.successCount).toBe(2);
        expect(response.errorCount).toBe(1);
        expect(response.results).toHaveLength(3);
      });

      it('should stop on error by default', async () => {
        const request: BatchRequest = {
          queries: [
            { sql: 'SELECT * FROM users' },
            { sql: 'DROP TABLE users' }, // This will fail
            { sql: 'SELECT * FROM users' },
          ],
        };

        const response = await target.batch(request);

        expect(response.successCount).toBe(1);
        expect(response.errorCount).toBe(1); // Only the actual error is counted
        expect(response.results).toHaveLength(3); // All results including skipped
        // The third query should be skipped
        const thirdResult = response.results[2] as { index: number; error: string };
        expect(thirdResult.error).toContain('Skipped');
      });
    });

    describe('transactions', () => {
      it('should begin and commit a transaction', async () => {
        const handle = await target.beginTransaction({});

        expect(handle.txId).toBeTruthy();
        expect(typeof handle.startLSN).toBe('bigint');
        expect(handle.expiresAt).toBeGreaterThan(Date.now());

        const result = await target.commit({ txId: handle.txId });

        expect(result.success).toBe(true);
        expect(typeof result.lsn).toBe('bigint');
      });

      it('should begin and rollback a transaction', async () => {
        const handle = await target.beginTransaction({});
        const result = await target.rollback({ txId: handle.txId });

        expect(result.success).toBe(true);
      });

      it('should fail to commit unknown transaction', async () => {
        const result = await target.commit({ txId: 'unknown_tx' });

        expect(result.success).toBe(false);
        expect(result.error).toBeTruthy();
      });
    });

    describe('schema', () => {
      it('should return schema information', async () => {
        const response = await target.getSchema({});

        expect(response.tables).toHaveLength(1);
        expect(response.tables[0].name).toBe('users');
        expect(response.tables[0].columns).toHaveLength(3);
        expect(typeof response.lastModifiedLSN).toBe('bigint');
      });

      it('should filter tables by name', async () => {
        executor.addTable('orders', ['id'], ['number'], []);

        const response = await target.getSchema({ tables: ['users'] });

        expect(response.tables).toHaveLength(1);
        expect(response.tables[0].name).toBe('users');
      });
    });

    describe('ping', () => {
      it('should return pong with LSN and timestamp', async () => {
        const response = await target.ping();

        expect(response.pong).toBe(true);
        expect(typeof response.lsn).toBe('bigint');
        expect(response.timestamp).toBeLessThanOrEqual(Date.now());
      });
    });

    describe('getStats', () => {
      it('should return connection stats', async () => {
        const stats = await target.getStats();

        expect(stats.connected).toBe(true);
        expect(typeof stats.currentLSN).toBe('bigint');
      });
    });

    describe('getServerStats', () => {
      it('should track server statistics', async () => {
        // Execute some queries
        await target.query({ sql: 'SELECT * FROM users' });
        await target.query({ sql: 'SELECT * FROM users' });
        await target.beginTransaction({});

        const stats = target.getServerStats();

        expect(stats.totalQueries).toBe(2);
        expect(stats.totalTransactions).toBe(1);
        expect(stats.activeTransactions).toBe(1);
      });
    });
  });

  describe('Error utilities', () => {
    it('isRPCError should identify RPC errors', () => {
      const rpcError = { code: 'UNKNOWN', message: 'test' };
      const regularError = new Error('test');

      expect(isRPCError(rpcError)).toBe(true);
      expect(isRPCError(regularError)).toBe(false);
      expect(isRPCError(null)).toBe(false);
      expect(isRPCError('string')).toBe(false);
    });

    it('createRPCError should create properly structured errors', () => {
      const error = createRPCError(
        RPCErrorCode.SYNTAX_ERROR,
        'Invalid SQL syntax',
        { position: 42 }
      );

      expect(error.code).toBe(RPCErrorCode.SYNTAX_ERROR);
      expect(error.message).toBe('Invalid SQL syntax');
      expect(error.details).toEqual({ position: 42 });
    });
  });

  describe('Type definitions', () => {
    it('should have all required types exported', () => {
      // This test verifies that types are exported correctly
      // If any type is missing, TypeScript will fail to compile this file

      type _QueryRequest = QueryRequest;
      type _BatchRequest = BatchRequest;

      // The fact that this compiles means the types are exported
      expect(true).toBe(true);
    });
  });
});

describe('RPC Types', () => {
  it('RPCErrorCode should have all expected values', () => {
    expect(RPCErrorCode.UNKNOWN).toBe('UNKNOWN');
    expect(RPCErrorCode.SYNTAX_ERROR).toBe('SYNTAX_ERROR');
    expect(RPCErrorCode.TABLE_NOT_FOUND).toBe('TABLE_NOT_FOUND');
    expect(RPCErrorCode.COLUMN_NOT_FOUND).toBe('COLUMN_NOT_FOUND');
    expect(RPCErrorCode.CONSTRAINT_VIOLATION).toBe('CONSTRAINT_VIOLATION');
    expect(RPCErrorCode.TIMEOUT).toBe('TIMEOUT');
    expect(RPCErrorCode.DEADLOCK_DETECTED).toBe('DEADLOCK_DETECTED');
    expect(RPCErrorCode.SERIALIZATION_FAILURE).toBe('SERIALIZATION_FAILURE');
    expect(RPCErrorCode.UNAUTHORIZED).toBe('UNAUTHORIZED');
    expect(RPCErrorCode.FORBIDDEN).toBe('FORBIDDEN');
    expect(RPCErrorCode.RESOURCE_EXHAUSTED).toBe('RESOURCE_EXHAUSTED');
    expect(RPCErrorCode.QUOTA_EXCEEDED).toBe('QUOTA_EXCEEDED');
  });
});
