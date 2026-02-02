/**
 * Tests for lake.do QueryExecutor
 *
 * @module lake.do/tests/query-executor
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { QueryExecutor } from '../query/executor.js';
import { LakeError, TimeoutError } from '../errors.js';

describe('QueryExecutor', () => {
  let executor: QueryExecutor;
  let mockSender: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    executor = new QueryExecutor({ timeout: 1000 });
    mockSender = vi.fn();
    executor.setSender(mockSender);
  });

  describe('constructor', () => {
    it('creates executor with timeout config', () => {
      const exec = new QueryExecutor({ timeout: 5000 });
      expect(exec.pendingRequestCount).toBe(0);
    });
  });

  describe('setSender', () => {
    it('sets the RPC sender function', async () => {
      const sender = vi.fn();
      const exec = new QueryExecutor({ timeout: 1000 });

      exec.setSender(sender);

      // Attempt to make RPC call to verify sender is set
      const promise = exec.ping();

      expect(sender).toHaveBeenCalled();

      // Clean up by rejecting the pending request and catching
      exec.cancelAllRequests(new Error('test cleanup'));
      await promise.catch(() => {}); // Swallow the rejection
    });
  });

  describe('pendingRequestCount', () => {
    it('returns 0 when no pending requests', () => {
      expect(executor.pendingRequestCount).toBe(0);
    });

    it('increments on RPC call', async () => {
      // Start a request
      const promise = executor.ping();

      expect(executor.pendingRequestCount).toBe(1);

      // Clean up
      executor.cancelAllRequests(new Error('cleanup'));
      await promise.catch(() => {}); // Swallow the rejection
    });

    it('decrements when request completes', async () => {
      const promise = executor.query('SELECT 1');

      expect(executor.pendingRequestCount).toBe(1);

      // Get the request ID from the sender call
      const request = mockSender.mock.calls[0][0];

      // Simulate response
      executor.handleResponse({
        id: request.id,
        result: { rows: [{ '1': 1 }], rowCount: 1 },
      });

      await promise;

      expect(executor.pendingRequestCount).toBe(0);
    });
  });

  describe('rpc', () => {
    it('throws when sender not configured', async () => {
      const exec = new QueryExecutor({ timeout: 1000 });

      await expect(exec.ping()).rejects.toThrow('RPC sender not configured');
    });

    it('sends request with correct format', async () => {
      const promise = executor.query('SELECT * FROM users');

      expect(mockSender).toHaveBeenCalledOnce();
      const request = mockSender.mock.calls[0][0];

      expect(request.id).toBeDefined();
      expect(request.method).toBe('query');
      expect(request.params).toEqual({ sql: 'SELECT * FROM users' });

      // Clean up
      executor.cancelAllRequests(new Error('cleanup'));
      await promise.catch(() => {}); // Swallow the rejection
    });

    it('generates unique request IDs', async () => {
      const promise1 = executor.query('SELECT 1');
      const promise2 = executor.query('SELECT 2');
      const promise3 = executor.query('SELECT 3');

      const id1 = mockSender.mock.calls[0][0].id;
      const id2 = mockSender.mock.calls[1][0].id;
      const id3 = mockSender.mock.calls[2][0].id;

      expect(id1).not.toBe(id2);
      expect(id2).not.toBe(id3);

      // Clean up
      executor.cancelAllRequests(new Error('cleanup'));
      await Promise.allSettled([promise1, promise2, promise3]); // Swallow rejections
    });

    it('times out after configured duration', async () => {
      const exec = new QueryExecutor({ timeout: 50 });
      exec.setSender(vi.fn());

      await expect(exec.ping()).rejects.toThrow(TimeoutError);
    });
  });

  describe('handleResponse', () => {
    it('resolves pending request with result', async () => {
      const promise = executor.query<{ count: number }>('SELECT COUNT(*) as count FROM users');
      const request = mockSender.mock.calls[0][0];

      executor.handleResponse({
        id: request.id,
        result: { rows: [{ count: 42 }], rowCount: 1 },
      });

      const result = await promise;

      expect(result.rows).toEqual([{ count: 42 }]);
      expect(result.rowCount).toBe(1);
    });

    it('rejects pending request with error', async () => {
      const promise = executor.query('SELECT * FROM nonexistent');
      const request = mockSender.mock.calls[0][0];

      executor.handleResponse({
        id: request.id,
        error: {
          code: 'TABLE_NOT_FOUND',
          message: 'Table "nonexistent" does not exist',
        },
      });

      await expect(promise).rejects.toThrow(LakeError);
    });

    it('ignores response for unknown request ID', () => {
      // Should not throw
      expect(() => {
        executor.handleResponse({
          id: 'unknown-id',
          result: { data: 'test' },
        });
      }).not.toThrow();
    });
  });

  describe('cancelAllRequests', () => {
    it('rejects all pending requests with given error', async () => {
      const promise1 = executor.query('SELECT 1');
      const promise2 = executor.query('SELECT 2');
      const promise3 = executor.query('SELECT 3');

      expect(executor.pendingRequestCount).toBe(3);

      const cancelError = new Error('Connection closed');
      executor.cancelAllRequests(cancelError);

      expect(executor.pendingRequestCount).toBe(0);

      await expect(promise1).rejects.toThrow('Connection closed');
      await expect(promise2).rejects.toThrow('Connection closed');
      await expect(promise3).rejects.toThrow('Connection closed');
    });
  });

  describe('query', () => {
    it('sends query request', async () => {
      const promise = executor.query('SELECT * FROM orders', { limit: 10 });

      const request = mockSender.mock.calls[0][0];

      expect(request.method).toBe('query');
      expect(request.params).toEqual({
        sql: 'SELECT * FROM orders',
        limit: 10,
      });

      executor.cancelAllRequests(new Error('cleanup'));
      await promise.catch(() => {}); // Swallow the rejection
    });

    it('supports time travel options', async () => {
      const asOf = new Date('2024-01-15');
      const promise = executor.query('SELECT * FROM orders', { asOf });

      const request = mockSender.mock.calls[0][0];

      expect(request.params.asOf).toEqual(asOf);

      executor.cancelAllRequests(new Error('cleanup'));
      await promise.catch(() => {}); // Swallow the rejection
    });
  });

  describe('getMetadata', () => {
    it('sends getMetadata request', async () => {
      const promise = executor.getMetadata('orders');

      const request = mockSender.mock.calls[0][0];

      expect(request.method).toBe('getMetadata');
      expect(request.params).toEqual({ tableName: 'orders' });

      executor.cancelAllRequests(new Error('cleanup'));
      await promise.catch(() => {}); // Swallow the rejection
    });

    it('transforms response snapshots', async () => {
      const promise = executor.getMetadata('orders');
      const request = mockSender.mock.calls[0][0];

      executor.handleResponse({
        id: request.id,
        result: {
          tableId: 'orders',
          schema: { columns: [] },
          partitionSpec: { column: 'date', strategy: 'time', granularity: 'day' },
          currentSnapshotId: 'snap_123',
          snapshots: [
            {
              id: 'snap_123',
              timestamp: '2024-01-15T00:00:00Z',
              summary: { addedFiles: 1, deletedFiles: 0, addedRows: 10, deletedRows: 0 },
              manifestList: '/manifest.json',
            },
          ],
          properties: {},
        },
      });

      const metadata = await promise;

      expect(metadata.tableId).toBe('orders');
      expect(typeof metadata.currentSnapshotId).toBe('string');
      expect(metadata.snapshots[0].timestamp).toBeInstanceOf(Date);
    });
  });

  describe('listSnapshots', () => {
    it('sends listSnapshots request', async () => {
      const promise = executor.listSnapshots('orders');

      const request = mockSender.mock.calls[0][0];

      expect(request.method).toBe('listSnapshots');
      expect(request.params).toEqual({ tableName: 'orders' });

      executor.cancelAllRequests(new Error('cleanup'));
      await promise.catch(() => {}); // Swallow the rejection
    });

    it('transforms response snapshots', async () => {
      const promise = executor.listSnapshots('orders');
      const request = mockSender.mock.calls[0][0];

      executor.handleResponse({
        id: request.id,
        result: [
          {
            id: 'snap_1',
            timestamp: '2024-01-15T00:00:00Z',
            summary: { addedFiles: 1, deletedFiles: 0, addedRows: 10, deletedRows: 0 },
            manifestList: '/manifest1.json',
          },
          {
            id: 'snap_2',
            timestamp: '2024-01-16T00:00:00Z',
            parentId: 'snap_1',
            summary: { addedFiles: 2, deletedFiles: 0, addedRows: 20, deletedRows: 0 },
            manifestList: '/manifest2.json',
          },
        ],
      });

      const snapshots = await promise;

      expect(snapshots).toHaveLength(2);
      expect(snapshots[0].timestamp).toBeInstanceOf(Date);
      expect(snapshots[1].parentId).toBe('snap_1');
    });
  });

  describe('listPartitions', () => {
    it('sends listPartitions request', async () => {
      const promise = executor.listPartitions('orders');

      const request = mockSender.mock.calls[0][0];

      expect(request.method).toBe('listPartitions');
      expect(request.params).toEqual({ tableName: 'orders' });

      executor.cancelAllRequests(new Error('cleanup'));
      await promise.catch(() => {}); // Swallow the rejection
    });

    it('transforms response partitions', async () => {
      const promise = executor.listPartitions('orders');
      const request = mockSender.mock.calls[0][0];

      executor.handleResponse({
        id: request.id,
        result: [
          {
            key: 'date=2024-01-15',
            strategy: 'time',
            fileCount: 5,
            rowCount: 5000,
            sizeBytes: 1024 * 1024,
            lastModified: '2024-01-15T12:00:00Z',
          },
        ],
      });

      const partitions = await promise;

      expect(partitions).toHaveLength(1);
      expect(typeof partitions[0].key).toBe('string');
      expect(partitions[0].lastModified).toBeInstanceOf(Date);
    });
  });

  describe('compact', () => {
    it('sends compact request', async () => {
      const promise = executor.compact('date=2024-01-15' as any);

      const request = mockSender.mock.calls[0][0];

      expect(request.method).toBe('compact');
      expect(request.params.partition).toBe('date=2024-01-15');

      executor.cancelAllRequests(new Error('cleanup'));
      await promise.catch(() => {}); // Swallow the rejection
    });

    it('includes config when provided', async () => {
      const promise = executor.compact('date=2024-01-15' as any, {
        targetFileSize: 128 * 1024 * 1024,
        maxFiles: 50,
      });

      const request = mockSender.mock.calls[0][0];

      expect(request.params.config).toEqual({
        targetFileSize: 128 * 1024 * 1024,
        maxFiles: 50,
      });

      executor.cancelAllRequests(new Error('cleanup'));
      await promise.catch(() => {}); // Swallow the rejection
    });

    it('transforms response job', async () => {
      const promise = executor.compact('date=2024-01-15' as any);
      const request = mockSender.mock.calls[0][0];

      executor.handleResponse({
        id: request.id,
        result: {
          id: 'compact_123',
          partition: 'date=2024-01-15',
          status: 'pending',
          inputFiles: ['file1.parquet', 'file2.parquet'],
        },
      });

      const job = await promise;

      expect(typeof job.id).toBe('string');
      expect(job.status).toBe('pending');
    });
  });

  describe('getCompactionStatus', () => {
    it('sends getCompactionStatus request', async () => {
      const promise = executor.getCompactionStatus('compact_123' as any);

      const request = mockSender.mock.calls[0][0];

      expect(request.method).toBe('getCompactionStatus');
      expect(request.params).toEqual({ jobId: 'compact_123' });

      executor.cancelAllRequests(new Error('cleanup'));
      await promise.catch(() => {}); // Swallow the rejection
    });

    it('transforms completed job response', async () => {
      const promise = executor.getCompactionStatus('compact_123' as any);
      const request = mockSender.mock.calls[0][0];

      executor.handleResponse({
        id: request.id,
        result: {
          id: 'compact_123',
          partition: 'date=2024-01-15',
          status: 'completed',
          inputFiles: ['file1.parquet', 'file2.parquet'],
          outputFiles: ['merged.parquet'],
          bytesRead: 1000000,
          bytesWritten: 900000,
          startedAt: '2024-01-15T12:00:00Z',
          completedAt: '2024-01-15T12:05:00Z',
        },
      });

      const job = await promise;

      expect(job.status).toBe('completed');
      expect(job.startedAt).toBeInstanceOf(Date);
      expect(job.completedAt).toBeInstanceOf(Date);
    });
  });

  describe('subscribe', () => {
    it('sends subscribe request', async () => {
      const promise = executor.subscribe({
        tables: ['orders'],
        operations: ['INSERT', 'UPDATE'],
      });

      const request = mockSender.mock.calls[0][0];

      expect(request.method).toBe('subscribe');
      expect(request.params).toEqual({
        tables: ['orders'],
        operations: ['INSERT', 'UPDATE'],
      });

      executor.cancelAllRequests(new Error('cleanup'));
      await promise.catch(() => {}); // Swallow the rejection
    });
  });

  describe('unsubscribe', () => {
    it('sends unsubscribe request', async () => {
      const promise = executor.unsubscribe();

      const request = mockSender.mock.calls[0][0];

      expect(request.method).toBe('unsubscribe');
      expect(request.params).toEqual({});

      // Simulate success
      executor.handleResponse({ id: request.id, result: undefined });

      await promise;
    });

    it('suppresses errors', async () => {
      const promise = executor.unsubscribe();
      const request = mockSender.mock.calls[0][0];

      // Simulate error
      executor.handleResponse({
        id: request.id,
        error: { code: 'NOT_SUBSCRIBED', message: 'Not subscribed' },
      });

      // Should not throw
      await expect(promise).resolves.toBeUndefined();
    });
  });

  describe('ping', () => {
    it('sends ping request', async () => {
      const promise = executor.ping();

      const request = mockSender.mock.calls[0][0];

      expect(request.method).toBe('ping');
      expect(request.params).toEqual({});

      executor.cancelAllRequests(new Error('cleanup'));
      await promise.catch(() => {}); // Swallow the rejection
    });

    it('resolves on success', async () => {
      const promise = executor.ping();
      const request = mockSender.mock.calls[0][0];

      executor.handleResponse({ id: request.id, result: undefined });

      await expect(promise).resolves.toBeUndefined();
    });
  });
});
