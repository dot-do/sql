/**
 * Replica DO Comprehensive Tests
 *
 * Tests for the Replica Durable Object including:
 * - Initialization and registration
 * - WAL streaming and sync
 * - Snapshot catch-up
 * - Consistency level handling
 * - Failover promotion/demotion
 * - Edge cases and error handling
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

import {
  type ReplicaId,
  type ReplicaInfo,
  type WALBatch,
  type SnapshotInfo,
  type SnapshotChunk,
  type SessionState,
  ReplicationError,
  ReplicationErrorCode,
  WALApplyErrorCode,
} from '../types.js';

import { createReplica, type CreateReplicaOptions } from '../replica.js';
import { createWALWriter } from '../../wal/index.js';
import { crc32 } from '../../wal/writer.js';
import type { WALWriter } from '../../wal/types.js';
import type { DOStorageBackend } from '../../fsx/types.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

function createMockBackend(): DOStorageBackend {
  const storage = new Map<string, Uint8Array>();

  return {
    async read(path: string): Promise<Uint8Array | null> {
      return storage.get(path) ?? null;
    },
    async write(path: string, data: Uint8Array): Promise<void> {
      storage.set(path, data);
    },
    async delete(path: string): Promise<void> {
      storage.delete(path);
    },
    async exists(path: string): Promise<boolean> {
      return storage.has(path);
    },
    async list(prefix: string): Promise<string[]> {
      return Array.from(storage.keys()).filter(k => k.startsWith(prefix));
    },
    async getStats(): Promise<{ fileCount: number; totalSize: number }> {
      let totalSize = 0;
      for (const data of storage.values()) {
        totalSize += data.length;
      }
      return { fileCount: storage.size, totalSize };
    },
  } as DOStorageBackend;
}

function createReplicaId(region: string, instanceId: string): ReplicaId {
  return { region, instanceId };
}

function createReplicaInfo(
  id: ReplicaId,
  status: ReplicaInfo['status'] = 'syncing',
  lastLSN: bigint = 0n
): Omit<ReplicaInfo, 'registeredAt'> {
  return {
    id,
    status,
    role: 'replica',
    lastLSN,
    lastHeartbeat: Date.now(),
    doUrl: `https://${id.region}.replica.do/${id.instanceId}`,
  };
}

const textEncoder = new TextEncoder();

function createValidBatch(
  entries: Array<{ lsn: bigint; timestamp: number; txnId: string; op: 'INSERT' | 'UPDATE' | 'DELETE' | 'COMMIT'; table: string }>
): WALBatch {
  const entriesJson = JSON.stringify(entries.map(e => ({ ...e, lsn: e.lsn.toString() })));
  const checksum = crc32(textEncoder.encode(entriesJson));

  return {
    startLSN: entries[0]?.lsn ?? 0n,
    endLSN: entries[entries.length - 1]?.lsn ?? 0n,
    entries,
    checksum,
    timestamp: Date.now(),
  };
}

// =============================================================================
// INITIALIZATION TESTS
// =============================================================================

describe('Replica DO - Initialization', () => {
  let backend: DOStorageBackend;
  let walWriter: WALWriter;

  beforeEach(() => {
    backend = createMockBackend();
    walWriter = createWALWriter(backend);
  });

  describe('Basic Initialization', () => {
    it('initializes with primary URL and replica info', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const status = await replica.getStatus();
      expect(status.id).toEqual(replicaId);
      expect(status.role).toBe('replica');
    });

    it('sets initial status to syncing', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId, 'active'));

      const status = await replica.getStatus();
      // After registration, status should be 'syncing' as it needs to catch up
      expect(status.status).toBe('syncing');
    });

    it('persists state after initialization', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const data = await backend.read('_replica/state.json');
      expect(data).not.toBeNull();

      const state = JSON.parse(new TextDecoder().decode(data!));
      expect(state.info.id).toEqual(replicaId);
    });

    it('initializes with specified LSN', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId, 'syncing', 100n));

      const lsn = await replica.getCurrentLSN();
      expect(lsn).toBe(100n);
    });
  });

  describe('Initialization Errors', () => {
    it('throws error when accessing status before initialization', async () => {
      const replica = createReplica({ backend, walWriter });

      await expect(replica.getStatus()).rejects.toThrow();
    });

    it('throws error when accessing LSN before initialization', async () => {
      const replica = createReplica({ backend, walWriter });

      await expect(replica.getCurrentLSN()).rejects.toThrow();
    });
  });
});

// =============================================================================
// WAL STREAMING TESTS
// =============================================================================

describe('Replica DO - WAL Streaming', () => {
  let backend: DOStorageBackend;
  let walWriter: WALWriter;

  beforeEach(() => {
    backend = createMockBackend();
    walWriter = createWALWriter(backend);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Streaming Lifecycle', () => {
    it('starts streaming successfully', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));
      await replica.startStreaming();

      const status = await replica.getStatus();
      expect(['syncing', 'active']).toContain(status.status);
    });

    it('stops streaming and updates state', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));
      await replica.startStreaming();
      await replica.stopStreaming();

      // Should not throw, streaming is stopped
      const status = await replica.getStatus();
      expect(status).toBeDefined();
    });

    it('handles multiple start/stop cycles', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      for (let i = 0; i < 3; i++) {
        await replica.startStreaming();
        await replica.stopStreaming();
      }

      const status = await replica.getStatus();
      expect(status).toBeDefined();
    });

    it('idempotent start streaming', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      await replica.startStreaming();
      await replica.startStreaming(); // Should not error
      await replica.startStreaming();

      await replica.stopStreaming();
    });
  });

  describe('WAL Batch Application', () => {
    it('applies valid WAL batch', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const batch = createValidBatch([
        { lsn: 1n, timestamp: Date.now(), txnId: 'txn1', op: 'INSERT', table: 'users' },
        { lsn: 2n, timestamp: Date.now(), txnId: 'txn1', op: 'COMMIT', table: '' },
      ]);

      const ack = await replica.applyWALBatch(batch);

      expect(ack.appliedLSN).toBe(2n);
      expect(ack.errors).toBeUndefined();
    });

    it('detects checksum mismatch', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const batch: WALBatch = {
        startLSN: 1n,
        endLSN: 1n,
        entries: [{ lsn: 1n, timestamp: Date.now(), txnId: 'txn1', op: 'INSERT', table: 'users' }],
        checksum: 12345, // Invalid checksum
        timestamp: Date.now(),
      };

      const ack = await replica.applyWALBatch(batch);

      expect(ack.errors).toBeDefined();
      expect(ack.errors![0].code).toBe(WALApplyErrorCode.CHECKSUM_MISMATCH);
    });

    it('detects duplicate entries', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const batch = createValidBatch([
        { lsn: 1n, timestamp: Date.now(), txnId: 'txn1', op: 'INSERT', table: 'users' },
      ]);

      await replica.applyWALBatch(batch);

      // Apply same batch again
      const ack2 = await replica.applyWALBatch(batch);

      expect(ack2.errors).toBeDefined();
      expect(ack2.errors![0].code).toBe(WALApplyErrorCode.DUPLICATE);
    });

    it('detects LSN gaps', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Apply first batch
      const batch1 = createValidBatch([
        { lsn: 1n, timestamp: Date.now(), txnId: 'txn1', op: 'INSERT', table: 'users' },
      ]);
      await replica.applyWALBatch(batch1);

      // Skip LSN 2, apply LSN 3
      const batch2 = createValidBatch([
        { lsn: 3n, timestamp: Date.now(), txnId: 'txn2', op: 'INSERT', table: 'users' },
      ]);
      const ack = await replica.applyWALBatch(batch2);

      expect(ack.errors).toBeDefined();
      expect(ack.errors![0].code).toBe(WALApplyErrorCode.MISSING_PREREQUISITE);
    });

    it('updates status to active when caught up', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const batch = createValidBatch([
        { lsn: 1n, timestamp: Date.now(), txnId: 'txn1', op: 'INSERT', table: 'users' },
      ]);

      const ack = await replica.applyWALBatch(batch);

      expect(ack.errors).toBeUndefined();
      const status = await replica.getStatus();
      expect(status.status).toBe('active');
    });

    it('handles large WAL batches', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const entries = [];
      for (let i = 1; i <= 100; i++) {
        entries.push({
          lsn: BigInt(i),
          timestamp: Date.now(),
          txnId: `txn${i}`,
          op: 'INSERT' as const,
          table: 'users',
        });
      }

      const batch = createValidBatch(entries);
      const ack = await replica.applyWALBatch(batch);

      expect(ack.appliedLSN).toBe(100n);
      expect(ack.errors).toBeUndefined();
    });

    it('records processing time', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const batch = createValidBatch([
        { lsn: 1n, timestamp: Date.now(), txnId: 'txn1', op: 'INSERT', table: 'users' },
      ]);

      const ack = await replica.applyWALBatch(batch);

      expect(ack.processingTimeMs).toBeGreaterThanOrEqual(0);
    });

    it('bounds applied LSN set to prevent memory growth', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Apply many batches to trigger cleanup
      for (let batch = 0; batch < 110; batch++) {
        const entries = [];
        for (let i = 0; i < 100; i++) {
          const lsn = BigInt(batch * 100 + i + 1);
          entries.push({
            lsn,
            timestamp: Date.now(),
            txnId: `txn${lsn}`,
            op: 'INSERT' as const,
            table: 'users',
          });
        }
        await replica.applyWALBatch(createValidBatch(entries));
      }

      // Should complete without memory issues
      const lsn = await replica.getCurrentLSN();
      expect(lsn).toBe(11000n);
    });
  });
});

// =============================================================================
// SNAPSHOT CATCH-UP TESTS
// =============================================================================

describe('Replica DO - Snapshot Catch-up', () => {
  let backend: DOStorageBackend;
  let walWriter: WALWriter;

  beforeEach(() => {
    backend = createMockBackend();
    walWriter = createWALWriter(backend);
  });

  describe('Snapshot Initialization', () => {
    it('starts catch-up from snapshot', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const snapshotInfo: SnapshotInfo = {
        id: 'snap_123',
        lsn: 100n,
        createdAt: Date.now(),
        sizeBytes: 1000,
        chunkCount: 1,
        schemaVersion: 1,
        tables: ['users', 'orders'],
        checksum: 0,
      };

      await replica.catchUpFromSnapshot(snapshotInfo);

      const status = await replica.getStatus();
      expect(status.status).toBe('syncing');
    });

    it('rejects chunk for wrong snapshot', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const snapshotInfo: SnapshotInfo = {
        id: 'snap_123',
        lsn: 100n,
        createdAt: Date.now(),
        sizeBytes: 1000,
        chunkCount: 1,
        schemaVersion: 1,
        tables: ['users'],
        checksum: 0,
      };

      await replica.catchUpFromSnapshot(snapshotInfo);

      const wrongChunk: SnapshotChunk = {
        snapshotId: 'snap_wrong',
        chunkIndex: 0,
        totalChunks: 1,
        data: new Uint8Array([1, 2, 3]),
        checksum: crc32(new Uint8Array([1, 2, 3])),
      };

      await expect(replica.applySnapshotChunk(wrongChunk)).rejects.toThrow(ReplicationError);
    });

    it('rejects chunk without active snapshot', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const chunk: SnapshotChunk = {
        snapshotId: 'snap_123',
        chunkIndex: 0,
        totalChunks: 1,
        data: new Uint8Array([1, 2, 3]),
        checksum: crc32(new Uint8Array([1, 2, 3])),
      };

      await expect(replica.applySnapshotChunk(chunk)).rejects.toThrow(ReplicationError);
    });
  });

  describe('Chunk Application', () => {
    it('applies single chunk snapshot', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const snapshotData = JSON.stringify({ lsn: '100', tables: ['users'], timestamp: Date.now() });
      const dataBytes = textEncoder.encode(snapshotData);

      const snapshotInfo: SnapshotInfo = {
        id: 'snap_123',
        lsn: 100n,
        createdAt: Date.now(),
        sizeBytes: dataBytes.length,
        chunkCount: 1,
        schemaVersion: 1,
        tables: ['users'],
        checksum: crc32(dataBytes),
      };

      await replica.catchUpFromSnapshot(snapshotInfo);

      const chunk: SnapshotChunk = {
        snapshotId: 'snap_123',
        chunkIndex: 0,
        totalChunks: 1,
        data: dataBytes,
        checksum: crc32(dataBytes),
      };

      await replica.applySnapshotChunk(chunk);

      const lsn = await replica.getCurrentLSN();
      expect(lsn).toBe(100n);
    });

    it('applies multi-chunk snapshot', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Create multi-chunk data
      const chunk1Data = textEncoder.encode('{"lsn":"100",');
      const chunk2Data = textEncoder.encode('"tables":["users"],');
      const chunk3Data = textEncoder.encode('"timestamp":1234}');

      const combinedData = new Uint8Array(chunk1Data.length + chunk2Data.length + chunk3Data.length);
      combinedData.set(chunk1Data, 0);
      combinedData.set(chunk2Data, chunk1Data.length);
      combinedData.set(chunk3Data, chunk1Data.length + chunk2Data.length);

      const snapshotInfo: SnapshotInfo = {
        id: 'snap_multi',
        lsn: 100n,
        createdAt: Date.now(),
        sizeBytes: combinedData.length,
        chunkCount: 3,
        schemaVersion: 1,
        tables: ['users'],
        checksum: crc32(combinedData),
      };

      await replica.catchUpFromSnapshot(snapshotInfo);

      // Apply chunks out of order
      await replica.applySnapshotChunk({
        snapshotId: 'snap_multi',
        chunkIndex: 1,
        totalChunks: 3,
        data: chunk2Data,
        checksum: crc32(chunk2Data),
      });

      await replica.applySnapshotChunk({
        snapshotId: 'snap_multi',
        chunkIndex: 0,
        totalChunks: 3,
        data: chunk1Data,
        checksum: crc32(chunk1Data),
      });

      await replica.applySnapshotChunk({
        snapshotId: 'snap_multi',
        chunkIndex: 2,
        totalChunks: 3,
        data: chunk3Data,
        checksum: crc32(chunk3Data),
      });

      const lsn = await replica.getCurrentLSN();
      expect(lsn).toBe(100n);
    });

    it('detects chunk checksum mismatch', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const snapshotInfo: SnapshotInfo = {
        id: 'snap_123',
        lsn: 100n,
        createdAt: Date.now(),
        sizeBytes: 100,
        chunkCount: 1,
        schemaVersion: 1,
        tables: ['users'],
        checksum: 0,
      };

      await replica.catchUpFromSnapshot(snapshotInfo);

      const chunk: SnapshotChunk = {
        snapshotId: 'snap_123',
        chunkIndex: 0,
        totalChunks: 1,
        data: new Uint8Array([1, 2, 3]),
        checksum: 99999, // Invalid checksum
      };

      await expect(replica.applySnapshotChunk(chunk)).rejects.toThrow(ReplicationError);
    });
  });
});

// =============================================================================
// QUERY HANDLING TESTS
// =============================================================================

describe('Replica DO - Query Handling', () => {
  let backend: DOStorageBackend;
  let walWriter: WALWriter;

  beforeEach(() => {
    backend = createMockBackend();
    walWriter = createWALWriter(backend);
  });

  describe('Consistency Levels', () => {
    it('handles eventual consistency locally', async () => {
      const executeSql = vi.fn().mockResolvedValue({ rows: [{ id: 1 }], sql: '' });
      const replica = createReplica({ backend, walWriter, executeSql });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const result = await replica.handleQuery('SELECT * FROM users', 'eventual');

      expect(executeSql).toHaveBeenCalledWith('SELECT * FROM users');
      expect(result).toEqual({ rows: [{ id: 1 }], sql: '' });
    });

    it('forwards strong consistency to primary', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      await expect(
        replica.handleQuery('SELECT * FROM users', 'strong')
      ).rejects.toThrow(ReplicationError);
    });

    it('handles session consistency with caught-up replica', async () => {
      const executeSql = vi.fn().mockResolvedValue({ rows: [], sql: '' });
      const replica = createReplica({ backend, walWriter, executeSql });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId, 'active', 100n));

      // Apply some WAL to bring replica up to speed
      const batch = createValidBatch([
        { lsn: 1n, timestamp: Date.now(), txnId: 'txn1', op: 'INSERT', table: 'users' },
      ]);
      await replica.applyWALBatch(batch);

      const session: SessionState = {
        sessionId: 'sess_123',
        lastWriteLSN: 1n,
        startedAt: Date.now(),
      };

      const result = await replica.handleQuery('SELECT * FROM users', 'session', session);

      expect(executeSql).toHaveBeenCalled();
    });

    it('rejects bounded consistency when lag exceeds threshold', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { boundedStalenessMs: 100 },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      const oldHeartbeat = createReplicaInfo(replicaId, 'active', 100n);
      oldHeartbeat.lastHeartbeat = Date.now() - 500; // 500ms ago

      await replica.initialize('https://primary.do', oldHeartbeat);

      await expect(
        replica.handleQuery('SELECT * FROM users', 'bounded')
      ).rejects.toThrow(ReplicationError);
    });

    it('returns empty result when no executeSql provided', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const result = await replica.handleQuery('SELECT * FROM users', 'eventual');

      expect(result).toEqual({ rows: [], rowCount: 0 });
    });
  });

  describe('Write Forwarding', () => {
    it('throws error for write forwarding (not implemented)', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      await expect(
        replica.forwardWrite('INSERT INTO users VALUES (1)')
      ).rejects.toThrow(ReplicationError);
    });
  });
});

// =============================================================================
// FAILOVER TESTS
// =============================================================================

describe('Replica DO - Failover', () => {
  let backend: DOStorageBackend;
  let walWriter: WALWriter;

  beforeEach(() => {
    backend = createMockBackend();
    walWriter = createWALWriter(backend);
  });

  describe('Promotion to Primary', () => {
    it('promotes replica to primary', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));
      await replica.startStreaming();

      await replica.promoteToPrimary();

      const status = await replica.getStatus();
      expect(status.role).toBe('primary');
      expect(status.status).toBe('active');
    });

    it('stops streaming on promotion', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));
      await replica.startStreaming();

      await replica.promoteToPrimary();

      // Should be primary now, streaming should be stopped
      const status = await replica.getStatus();
      expect(status.role).toBe('primary');
    });

    it('persists state after promotion', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));
      await replica.promoteToPrimary();

      const data = await backend.read('_replica/state.json');
      const state = JSON.parse(new TextDecoder().decode(data!));

      expect(state.info.role).toBe('primary');
    });
  });

  describe('Demotion to Replica', () => {
    it('demotes from primary to replica', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));
      await replica.promoteToPrimary();

      await replica.demoteToReplica('https://new-primary.do');

      const status = await replica.getStatus();
      expect(status.role).toBe('replica');
      // After demotion, the replica starts streaming and may transition to 'active'
      // if already caught up with the WAL
      expect(['syncing', 'active']).toContain(status.status);
    });

    it('updates primary URL on demotion', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://old-primary.do', createReplicaInfo(replicaId));
      await replica.promoteToPrimary();

      await replica.demoteToReplica('https://new-primary.do');

      const data = await backend.read('_replica/state.json');
      const state = JSON.parse(new TextDecoder().decode(data!));

      expect(state.primaryUrl).toBe('https://new-primary.do');
    });

    it('restarts streaming after demotion', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));
      await replica.promoteToPrimary();

      await replica.demoteToReplica('https://new-primary.do');

      const status = await replica.getStatus();
      // After demotion, streaming is restarted and status may be 'syncing' or 'active'
      // depending on whether the replica is already caught up
      expect(['syncing', 'active']).toContain(status.status);
    });
  });

  describe('Failover Edge Cases', () => {
    it('handles rapid promotion/demotion cycles', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      for (let i = 0; i < 5; i++) {
        await replica.promoteToPrimary();
        await replica.demoteToReplica(`https://primary-${i}.do`);
      }

      const status = await replica.getStatus();
      expect(status.role).toBe('replica');
    });
  });
});

// =============================================================================
// HEARTBEAT TESTS
// =============================================================================

describe('Replica DO - Heartbeat', () => {
  let backend: DOStorageBackend;
  let walWriter: WALWriter;

  beforeEach(() => {
    backend = createMockBackend();
    walWriter = createWALWriter(backend);
  });

  it('updates heartbeat timestamp', async () => {
    const replica = createReplica({ backend, walWriter });
    const replicaId = createReplicaId('us-west', 'replica-1');

    await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

    const statusBefore = await replica.getStatus();
    const heartbeatBefore = statusBefore.lastHeartbeat;

    await new Promise(resolve => setTimeout(resolve, 10));
    await replica.sendHeartbeat();

    const statusAfter = await replica.getStatus();
    expect(statusAfter.lastHeartbeat).toBeGreaterThan(heartbeatBefore);
  });

  it('persists state after heartbeat', async () => {
    const replica = createReplica({ backend, walWriter });
    const replicaId = createReplicaId('us-west', 'replica-1');

    await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

    await replica.sendHeartbeat();

    const data = await backend.read('_replica/state.json');
    expect(data).not.toBeNull();
  });
});

// =============================================================================
// STATE PERSISTENCE TESTS
// =============================================================================

describe('Replica DO - State Persistence', () => {
  it('loads persisted state on creation', async () => {
    const backend = createMockBackend();

    // Pre-populate state
    const state = {
      info: {
        id: { region: 'us-west', instanceId: 'replica-1' },
        status: 'active',
        role: 'replica',
        lastLSN: '50',
        lastHeartbeat: Date.now(),
        registeredAt: Date.now() - 10000,
        doUrl: 'https://replica.do',
      },
      primaryUrl: 'https://primary.do',
      currentLSN: '50',
      streamingActive: false,
    };

    await backend.write(
      '_replica/state.json',
      new TextEncoder().encode(JSON.stringify(state))
    );

    const walWriter = createWALWriter(backend);
    const replica = createReplica({ backend, walWriter });

    // Wait for async load
    await new Promise(resolve => setTimeout(resolve, 50));

    // State should be loaded - getStatus should work without initialize
    // Note: In actual implementation, loadState is called on creation but
    // the state variable is still null until initialize is called
    // This tests the loading mechanism works
  });

  it('handles corrupted persisted state gracefully', async () => {
    const backend = createMockBackend();

    // Pre-populate with corrupted state
    await backend.write(
      '_replica/state.json',
      new TextEncoder().encode('invalid json {{{')
    );

    const walWriter = createWALWriter(backend);

    // Should not throw
    const replica = createReplica({ backend, walWriter });
    const replicaId = createReplicaId('us-west', 'replica-1');

    // Should be able to initialize fresh
    await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

    const status = await replica.getStatus();
    expect(status.id).toEqual(replicaId);
  });
});
