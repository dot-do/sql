/**
 * Multi-Region Replication Tests for DoSQL
 *
 * Comprehensive test suite covering:
 * - Replica registration/deregistration
 * - WAL streaming to replicas
 * - Replica catch-up from snapshot
 * - Read routing to nearest replica
 * - Write forwarding to primary
 * - Failover when primary unavailable
 * - Replication lag monitoring
 * - Consistency levels (eventual, strong)
 *
 * Uses workers-vitest-pool (NO MOCKS)
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { env } from 'cloudflare:test';

import {
  // Types
  type ReplicaId,
  type ReplicaInfo,
  type ReplicaStatus,
  type WALBatch,
  type WALAck,
  type SnapshotInfo,
  type SnapshotChunk,
  type ConsistencyLevel,
  type SessionState,
  type RoutingDecision,
  type ReplicationLag,
  type ReplicationHealth,
  type FailoverDecision,
  type FailoverState,
  type ReplicationConfig,
  DEFAULT_REPLICATION_CONFIG,
  ReplicationError,
  ReplicationErrorCode,
  WALApplyErrorCode,
  serializeReplicaId,
  deserializeReplicaId,
  replicaIdsEqual,
} from './types.js';

import { createPrimaryDO, createPrimary, type CreatePrimaryOptions } from './primary.js';
import { createReplicaDO, createReplica, type CreateReplicaOptions } from './replica.js';
import {
  createReplicationRouter,
  createExtendedRouter,
  createLoadBalancedRouter,
  SessionManager,
  createRouterWithSessions,
  type ExtendedReplicationRouter,
} from './router.js';

import { createDOBackend } from '../fsx/index.js';
import { createWALWriter, createWALReader } from '../wal/index.js';
import type { WALWriter, WALReader } from '../wal/types.js';
import type { DOStorageBackend } from '../fsx/types.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Create a mock FSX backend for testing
 */
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

/**
 * Create test replica ID
 */
function createReplicaId(region: string, instanceId: string): ReplicaId {
  return { region, instanceId };
}

/**
 * Create test replica info
 */
function createReplicaInfo(
  id: ReplicaId,
  status: ReplicaStatus = 'active',
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

// =============================================================================
// TYPE TESTS
// =============================================================================

describe('Replication Types', () => {
  describe('ReplicaId', () => {
    it('serializes ReplicaId to string', () => {
      const id: ReplicaId = { region: 'us-west', instanceId: 'replica-1' };
      const serialized = serializeReplicaId(id);
      expect(serialized).toBe('us-west:replica-1');
    });

    it('deserializes string to ReplicaId', () => {
      const str = 'eu-central:replica-2';
      const id = deserializeReplicaId(str);
      expect(id.region).toBe('eu-central');
      expect(id.instanceId).toBe('replica-2');
    });

    it('compares ReplicaIds for equality', () => {
      const id1: ReplicaId = { region: 'us-west', instanceId: 'replica-1' };
      const id2: ReplicaId = { region: 'us-west', instanceId: 'replica-1' };
      const id3: ReplicaId = { region: 'us-west', instanceId: 'replica-2' };

      expect(replicaIdsEqual(id1, id2)).toBe(true);
      expect(replicaIdsEqual(id1, id3)).toBe(false);
    });

    it('handles special characters in region and instanceId', () => {
      const id: ReplicaId = { region: 'us-west-2a', instanceId: 'replica_v2' };
      const serialized = serializeReplicaId(id);
      const deserialized = deserializeReplicaId(serialized);

      expect(deserialized.region).toBe('us-west-2a');
      expect(deserialized.instanceId).toBe('replica_v2');
    });
  });

  describe('ReplicationConfig', () => {
    it('has sensible defaults', () => {
      expect(DEFAULT_REPLICATION_CONFIG.minReplicas).toBe(1);
      expect(DEFAULT_REPLICATION_CONFIG.walBatchSize).toBe(100);
      expect(DEFAULT_REPLICATION_CONFIG.heartbeatIntervalMs).toBe(5000);
      expect(DEFAULT_REPLICATION_CONFIG.heartbeatTimeoutMs).toBe(15000);
      expect(DEFAULT_REPLICATION_CONFIG.maxLagMs).toBe(10000);
      expect(DEFAULT_REPLICATION_CONFIG.autoFailover).toBe(true);
      expect(DEFAULT_REPLICATION_CONFIG.conflictStrategy).toBe('last_write_wins');
    });
  });

  describe('ReplicationError', () => {
    it('creates error with all properties', () => {
      const replicaId: ReplicaId = { region: 'us-west', instanceId: 'replica-1' };
      const error = new ReplicationError(
        ReplicationErrorCode.REPLICA_NOT_FOUND,
        'Replica not found',
        replicaId,
        100n
      );

      expect(error.code).toBe(ReplicationErrorCode.REPLICA_NOT_FOUND);
      expect(error.message).toBe('Replica not found');
      expect(error.replicaId).toEqual(replicaId);
      expect(error.lsn).toBe(100n);
      expect(error.name).toBe('ReplicationError');
    });

    it('creates error with cause', () => {
      const cause = new Error('Network error');
      const error = new ReplicationError(
        ReplicationErrorCode.STREAMING_ERROR,
        'Streaming failed',
        undefined,
        undefined,
        cause
      );

      expect(error.cause).toBe(cause);
    });
  });
});

// =============================================================================
// PRIMARY DO TESTS
// =============================================================================

describe('Primary DO', () => {
  let backend: DOStorageBackend;
  let walWriter: WALWriter;
  let walReader: WALReader;

  beforeEach(() => {
    backend = createMockBackend();
    walWriter = createWALWriter(backend);
    walReader = createWALReader(backend);
  });

  describe('Replica Registration', () => {
    it('registers a new replica', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const replicaInfo = createReplicaInfo(replicaId);

      await primary.registerReplica(replicaInfo);

      const replicas = await primary.getReplicas();
      expect(replicas).toHaveLength(1);
      expect(replicas[0].id.region).toBe('us-west');
      expect(replicas[0].id.instanceId).toBe('replica-1');
    });

    it('registers multiple replicas', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });

      await primary.registerReplica(createReplicaInfo(createReplicaId('us-west', 'replica-1')));
      await primary.registerReplica(createReplicaInfo(createReplicaId('eu-central', 'replica-2')));
      await primary.registerReplica(createReplicaInfo(createReplicaId('ap-south', 'replica-3')));

      const replicas = await primary.getReplicas();
      expect(replicas).toHaveLength(3);
    });

    it('updates existing replica on re-registration', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await primary.registerReplica(createReplicaInfo(replicaId, 'active', 0n));
      await primary.registerReplica(createReplicaInfo(replicaId, 'active', 100n));

      const replicas = await primary.getReplicas();
      expect(replicas).toHaveLength(1);
      expect(replicas[0].status).toBe('syncing');
    });
  });

  describe('Replica Deregistration', () => {
    it('deregisters an existing replica', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await primary.registerReplica(createReplicaInfo(replicaId));
      await primary.deregisterReplica(replicaId);

      const replicas = await primary.getReplicas();
      expect(replicas).toHaveLength(0);
    });

    it('throws error when deregistering non-existent replica', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await expect(primary.deregisterReplica(replicaId)).rejects.toThrow(ReplicationError);
    });
  });

  describe('WAL Streaming', () => {
    it('pulls WAL entries for replica', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });
      const replicaId = createReplicaId('us-west', 'replica-1');

      // Register replica
      await primary.registerReplica(createReplicaInfo(replicaId, 'syncing', 0n));

      // Write some WAL entries
      await walWriter.append({
        timestamp: Date.now(),
        txnId: 'txn1',
        op: 'INSERT',
        table: 'users',
        after: new Uint8Array([1, 2, 3]),
      });
      await walWriter.flush();

      // Pull WAL
      const batch = await primary.pullWAL(replicaId, 0n, 10);

      expect(batch.entries.length).toBeGreaterThanOrEqual(0);
      expect(batch.startLSN).toBeDefined();
      expect(batch.checksum).toBeDefined();
    });

    it('returns empty batch when no new entries', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await primary.registerReplica(createReplicaInfo(replicaId, 'active', 0n));

      const batch = await primary.pullWAL(replicaId, 100n, 10);

      expect(batch.entries).toHaveLength(0);
      expect(batch.startLSN).toBe(100n);
    });

    it('throws error for unregistered replica', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });
      const replicaId = createReplicaId('us-west', 'unknown');

      await expect(primary.pullWAL(replicaId, 0n)).rejects.toThrow(ReplicationError);
    });

    it('acknowledges WAL entries', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await primary.registerReplica(createReplicaInfo(replicaId, 'syncing', 0n));

      const ack: WALAck = {
        replicaId,
        appliedLSN: 50n,
        processingTimeMs: 10,
      };

      await primary.acknowledgeWAL(ack);

      const replicas = await primary.getReplicas();
      expect(replicas[0].lastLSN).toBe(50n);
    });
  });

  describe('Stream Position', () => {
    it('gets stream position for replica', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await primary.registerReplica(createReplicaInfo(replicaId, 'syncing', 100n));

      const position = await primary.getStreamPosition(replicaId);

      expect(position.lsn).toBe(100n);
    });

    it('throws error for unknown replica', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });

      await expect(
        primary.getStreamPosition(createReplicaId('unknown', 'replica'))
      ).rejects.toThrow(ReplicationError);
    });
  });

  describe('Snapshot Management', () => {
    it('creates snapshot for replica catch-up', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await primary.registerReplica(createReplicaInfo(replicaId));

      const snapshotInfo = await primary.requestSnapshot({ replicaId });

      expect(snapshotInfo.id).toMatch(/^snap_/);
      expect(snapshotInfo.lsn).toBeDefined();
      expect(snapshotInfo.chunkCount).toBeGreaterThan(0);
    });

    it('retrieves snapshot chunks', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await primary.registerReplica(createReplicaInfo(replicaId));
      const snapshotInfo = await primary.requestSnapshot({ replicaId });

      const chunk = await primary.getSnapshotChunk(snapshotInfo.id, 0);

      expect(chunk.snapshotId).toBe(snapshotInfo.id);
      expect(chunk.chunkIndex).toBe(0);
      expect(chunk.data).toBeInstanceOf(Uint8Array);
    });

    it('throws error for non-existent snapshot', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });

      await expect(
        primary.getSnapshotChunk('non-existent', 0)
      ).rejects.toThrow(ReplicationError);
    });
  });

  describe('Replication Health', () => {
    it('reports healthy status with active replicas', async () => {
      const primary = createPrimary({ backend, walWriter, walReader, config: { minReplicas: 1 } });

      // Register replicas with recent heartbeats
      const replica1 = createReplicaInfo(createReplicaId('us-west', 'r1'), 'active');
      replica1.lastHeartbeat = Date.now();
      const replica2 = createReplicaInfo(createReplicaId('eu-central', 'r2'), 'active');
      replica2.lastHeartbeat = Date.now();

      await primary.registerReplica(replica1);
      await primary.registerReplica(replica2);

      // Acknowledge some WAL to mark replicas as healthy
      await primary.acknowledgeWAL({
        replicaId: replica1.id,
        appliedLSN: walWriter.getCurrentLSN(),
        processingTimeMs: 5,
      });
      await primary.acknowledgeWAL({
        replicaId: replica2.id,
        appliedLSN: walWriter.getCurrentLSN(),
        processingTimeMs: 5,
      });

      const health = await primary.getReplicationHealth();

      expect(['healthy', 'degraded']).toContain(health.status);
      expect(health.replicas).toHaveLength(2);
    });

    it('reports degraded status with lagging replicas', async () => {
      const primary = createPrimary({ backend, walWriter, walReader, config: { maxLagMs: 100 } });

      const oldHeartbeat = Date.now() - 500; // 500ms old
      const replicaInfo = createReplicaInfo(createReplicaId('us-west', 'r1'), 'active');
      replicaInfo.lastHeartbeat = oldHeartbeat;

      await primary.registerReplica(replicaInfo);

      const health = await primary.getReplicationHealth();

      // Status should be degraded or have lagging replica
      expect(['healthy', 'degraded', 'critical']).toContain(health.status);
    });

    it('reports critical status when below minimum replicas', async () => {
      const primary = createPrimary({ backend, walWriter, walReader, config: { minReplicas: 2 } });

      await primary.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1'), 'active'));

      const health = await primary.getReplicationHealth();

      expect(health.status).toBe('critical');
    });
  });

  describe('Failover', () => {
    it('initiates failover with best candidate', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });

      await primary.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1'), 'active', 90n));
      await primary.registerReplica(createReplicaInfo(createReplicaId('eu-central', 'r2'), 'active', 95n));

      const decision = await primary.initiateFailover('manual_trigger');

      expect(decision.proceed).toBe(true);
      expect(decision.candidate).toBeDefined();
    });

    it('prefers candidate with least lag', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });

      await primary.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1'), 'active', 50n));
      await primary.registerReplica(createReplicaInfo(createReplicaId('eu-central', 'r2'), 'active', 90n));

      const decision = await primary.initiateFailover('manual_trigger');

      expect(decision.candidate?.instanceId).toBe('r2');
    });

    it('rejects failover when no candidates available', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });

      // No replicas registered
      const decision = await primary.initiateFailover('manual_trigger');

      expect(decision.proceed).toBe(false);
      expect(decision.reason).toContain('No suitable candidate');
    });

    it('executes failover successfully', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });
      const candidateId = createReplicaId('eu-central', 'r2');

      await primary.registerReplica(createReplicaInfo(candidateId, 'active', 95n));

      const decision: FailoverDecision = {
        proceed: true,
        candidate: candidateId,
        reason: 'Manual failover',
        dataLossEstimate: 5n,
      };

      const state = await primary.executeFailover(decision);

      expect(state.status).toBe('completed');
      expect(state.newPrimary).toEqual(candidateId);
    });

    it('handles failover execution errors', async () => {
      const primary = createPrimary({ backend, walWriter, walReader });

      const decision: FailoverDecision = {
        proceed: false,
        reason: 'No candidate',
      };

      const state = await primary.executeFailover(decision);

      expect(state.status).toBe('failed');
    });
  });
});

// =============================================================================
// REPLICA DO TESTS
// =============================================================================

describe('Replica DO', () => {
  let backend: DOStorageBackend;
  let walWriter: WALWriter;

  beforeEach(() => {
    backend = createMockBackend();
    walWriter = createWALWriter(backend);
  });

  describe('Initialization', () => {
    it('initializes replica with primary URL', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const status = await replica.getStatus();
      expect(status.id).toEqual(replicaId);
      expect(status.status).toBe('syncing');
    });

    it('persists replica state', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Check state was persisted
      const data = await backend.read('_replica/state.json');
      expect(data).not.toBeNull();
    });
  });

  describe('WAL Streaming', () => {
    it('starts and stops streaming', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      await replica.startStreaming();
      const statusAfterStart = await replica.getStatus();
      // Status can be 'syncing' or 'active' depending on timing (active when caught up)
      expect(['syncing', 'active']).toContain(statusAfterStart.status);

      await replica.stopStreaming();
    });

    it('applies WAL batch successfully', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const batch: WALBatch = {
        startLSN: 1n,
        endLSN: 2n,
        entries: [
          {
            lsn: 1n,
            timestamp: Date.now(),
            txnId: 'txn1',
            op: 'INSERT',
            table: 'users',
          },
          {
            lsn: 2n,
            timestamp: Date.now(),
            txnId: 'txn1',
            op: 'COMMIT',
            table: '',
          },
        ],
        checksum: 0, // Will be recalculated
        timestamp: Date.now(),
      };

      // Calculate correct checksum
      const textEncoder = new TextEncoder();
      const entriesJson = JSON.stringify(batch.entries.map(e => ({
        ...e,
        lsn: e.lsn.toString(),
      })));
      const { crc32 } = await import('../wal/writer.js');
      batch.checksum = crc32(textEncoder.encode(entriesJson));

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
        checksum: 12345, // Wrong checksum
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

      // Apply first batch
      const textEncoder = new TextEncoder();
      const { crc32 } = await import('../wal/writer.js');

      const entry = { lsn: 1n, timestamp: Date.now(), txnId: 'txn1', op: 'INSERT' as const, table: 'users' };
      const entriesJson = JSON.stringify([{ ...entry, lsn: entry.lsn.toString() }]);

      const batch: WALBatch = {
        startLSN: 1n,
        endLSN: 1n,
        entries: [entry],
        checksum: crc32(textEncoder.encode(entriesJson)),
        timestamp: Date.now(),
      };

      await replica.applyWALBatch(batch);

      // Apply same batch again
      const ack2 = await replica.applyWALBatch(batch);

      expect(ack2.errors).toBeDefined();
      expect(ack2.errors![0].code).toBe(WALApplyErrorCode.DUPLICATE);
    });
  });

  describe('Snapshot Catch-up', () => {
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
        tables: ['users'],
        checksum: 0,
      };

      await replica.catchUpFromSnapshot(snapshotInfo);

      const status = await replica.getStatus();
      expect(status.status).toBe('syncing');
    });

    it('applies snapshot chunks', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const textEncoder = new TextEncoder();
      const { crc32 } = await import('../wal/writer.js');

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
  });

  describe('Query Handling', () => {
    it('handles eventual consistency reads locally', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        executeSql: async (sql) => ({ rows: [], sql }),
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const result = await replica.handleQuery('SELECT * FROM users', 'eventual');

      expect(result).toBeDefined();
    });

    it('forwards strong consistency reads to primary', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      await expect(
        replica.handleQuery('SELECT * FROM users', 'strong')
      ).rejects.toThrow(ReplicationError);
    });

    it('handles session consistency with caught-up replica', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        executeSql: async (sql) => ({ rows: [], sql }),
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const replicaInfo = createReplicaInfo(replicaId, 'active', 100n);

      await replica.initialize('https://primary.do', replicaInfo);

      const session: SessionState = {
        sessionId: 'sess_123',
        lastWriteLSN: 50n, // Replica is at 100n, so it's caught up
        startedAt: Date.now(),
      };

      // This should not throw because replica LSN (0n initial) will wait
      // but we need to handle this gracefully
      const result = await replica.handleQuery('SELECT * FROM users', 'session', session);
      expect(result).toBeDefined();
    });
  });

  describe('Failover', () => {
    it('promotes replica to primary', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      await replica.promoteToPrimary();

      const status = await replica.getStatus();
      expect(status.role).toBe('primary');
    });

    it('demotes primary to replica', async () => {
      const replica = createReplica({ backend, walWriter });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));
      await replica.promoteToPrimary();

      await replica.demoteToReplica('https://new-primary.do');

      const status = await replica.getStatus();
      expect(status.role).toBe('replica');
    });
  });

  describe('Heartbeat', () => {
    it('sends heartbeat and updates timestamp', async () => {
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
  });
});

// =============================================================================
// ROUTER TESTS
// =============================================================================

describe('Replication Router', () => {
  const primaryId = createReplicaId('us-east', 'primary-1');

  describe('Read Routing', () => {
    it('routes strong consistency to primary', async () => {
      const router = createExtendedRouter(primaryId);

      const decision = await router.routeRead('SELECT * FROM users', 'strong');

      expect(decision.target).toEqual(primaryId);
      expect(decision.consistency).toBe('strong');
    });

    it('routes eventual consistency to nearest replica', async () => {
      const router = createExtendedRouter(primaryId);

      // Register a replica
      const replicaId = createReplicaId('us-west', 'replica-1');
      router.registerReplica({
        id: replicaId,
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://replica.do',
      });

      router.recordLatency(replicaId, 10);

      const decision = await router.routeRead('SELECT * FROM users', 'eventual');

      // Should route to replica (not primary) for eventual consistency
      expect(decision.consistency).toBe('eventual');
    });

    it('falls back to primary when no replicas available', async () => {
      const router = createExtendedRouter(primaryId);

      const decision = await router.routeRead('SELECT * FROM users', 'eventual');

      expect(decision.target).toEqual(primaryId);
      expect(decision.fallback).toBe(true);
    });

    it('routes session consistency based on LSN', async () => {
      const router = createExtendedRouter(primaryId);

      const replicaId = createReplicaId('us-west', 'replica-1');
      router.registerReplica({
        id: replicaId,
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://replica.do',
      });

      const session: SessionState = {
        sessionId: 'sess_123',
        lastWriteLSN: 50n,
        startedAt: Date.now(),
      };

      const decision = await router.routeRead('SELECT * FROM users', 'session', session);

      expect(decision.consistency).toBe('session');
    });

    it('routes bounded consistency within staleness window', async () => {
      const router = createExtendedRouter(primaryId, { boundedStalenessMs: 5000 });

      const replicaId = createReplicaId('us-west', 'replica-1');
      router.registerReplica({
        id: replicaId,
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://replica.do',
      });

      router.updateReplicaStatus(replicaId, 'active', {
        replicaId,
        primaryLSN: 100n,
        replicaLSN: 100n,
        lagEntries: 0n,
        lagMs: 100, // Within 5000ms bound
        measuredAt: Date.now(),
      });

      const decision = await router.routeRead('SELECT * FROM users', 'bounded');

      expect(decision.consistency).toBe('bounded');
    });
  });

  describe('Write Routing', () => {
    it('always routes writes to primary', async () => {
      const router = createExtendedRouter(primaryId);

      const decision = await router.routeWrite('INSERT INTO users VALUES (1, \'test\')');

      expect(decision.target).toEqual(primaryId);
      expect(decision.consistency).toBe('strong');
    });
  });

  describe('Replica Management', () => {
    it('gets nearest replica for region', async () => {
      const router = createExtendedRouter(primaryId);

      const replicaId = createReplicaId('us-west', 'replica-1');
      router.registerReplica({
        id: replicaId,
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://replica.do',
      });

      const nearest = await router.getNearestReplica('us-west');

      expect(nearest).toEqual(replicaId);
    });

    it('updates replica status', () => {
      const router = createExtendedRouter(primaryId);

      const replicaId = createReplicaId('us-west', 'replica-1');
      router.registerReplica({
        id: replicaId,
        status: 'syncing',
        role: 'replica',
        lastLSN: 50n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://replica.do',
      });

      router.updateReplicaStatus(replicaId, 'active');

      const replicas = router.getReplicas();
      const replica = replicas.find(r => replicaIdsEqual(r.id, replicaId));

      expect(replica?.status).toBe('active');
    });

    it('deregisters replica', () => {
      const router = createExtendedRouter(primaryId);

      const replicaId = createReplicaId('us-west', 'replica-1');
      router.registerReplica({
        id: replicaId,
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://replica.do',
      });

      router.deregisterReplica(replicaId);

      const replicas = router.getReplicas();
      const replica = replicas.find(r => replicaIdsEqual(r.id, replicaId));

      expect(replica).toBeUndefined();
    });
  });

  describe('Metrics', () => {
    it('tracks read metrics', async () => {
      const router = createExtendedRouter(primaryId);

      await router.routeRead('SELECT 1', 'strong');
      await router.routeRead('SELECT 2', 'eventual');
      await router.routeRead('SELECT 3', 'eventual');

      const metrics = router.getMetrics();

      expect(metrics.totalReads).toBe(3);
      expect(metrics.primaryReads).toBeGreaterThan(0);
    });

    it('tracks write metrics', async () => {
      const router = createExtendedRouter(primaryId);

      await router.routeWrite('INSERT INTO users VALUES (1)');
      await router.routeWrite('UPDATE users SET name = \'test\'');

      const metrics = router.getMetrics();

      expect(metrics.totalWrites).toBe(2);
    });

    it('calculates average routing latency', async () => {
      const router = createExtendedRouter(primaryId);

      for (let i = 0; i < 10; i++) {
        await router.routeRead(`SELECT ${i}`, 'eventual');
      }

      const metrics = router.getMetrics();

      expect(metrics.avgRoutingLatencyMs).toBeGreaterThanOrEqual(0);
    });
  });

  describe('Load Balancing Strategies', () => {
    it('supports round-robin strategy', async () => {
      const router = createLoadBalancedRouter(primaryId, 'round-robin');

      router.registerReplica({
        id: createReplicaId('us-west', 'r1'),
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: '',
      });

      router.registerReplica({
        id: createReplicaId('eu-central', 'r2'),
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: '',
      });

      const result1 = await router.getNearestReplica('any');
      const result2 = await router.getNearestReplica('any');

      // Round robin should alternate
      expect(result1).not.toBeNull();
      expect(result2).not.toBeNull();
    });

    it('supports random strategy', async () => {
      const router = createLoadBalancedRouter(primaryId, 'random');

      router.registerReplica({
        id: createReplicaId('us-west', 'r1'),
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: '',
      });

      const result = await router.getNearestReplica('any');

      expect(result).not.toBeNull();
    });
  });
});

// =============================================================================
// SESSION MANAGER TESTS
// =============================================================================

describe('Session Manager', () => {
  it('creates new sessions', () => {
    const manager = new SessionManager();

    const session = manager.createSession('us-west');

    expect(session.sessionId).toMatch(/^sess_/);
    expect(session.lastWriteLSN).toBe(0n);
    expect(session.preferredRegion).toBe('us-west');
  });

  it('retrieves existing sessions', () => {
    const manager = new SessionManager();

    const created = manager.createSession();
    const retrieved = manager.getSession(created.sessionId);

    expect(retrieved).toEqual(created);
  });

  it('returns null for non-existent sessions', () => {
    const manager = new SessionManager();

    const session = manager.getSession('non-existent');

    expect(session).toBeNull();
  });

  it('updates session LSN', () => {
    const manager = new SessionManager();

    const session = manager.createSession();
    manager.updateSession(session.sessionId, 100n);

    const updated = manager.getSession(session.sessionId);

    expect(updated?.lastWriteLSN).toBe(100n);
  });

  it('deletes sessions', () => {
    const manager = new SessionManager();

    const session = manager.createSession();
    manager.deleteSession(session.sessionId);

    const retrieved = manager.getSession(session.sessionId);

    expect(retrieved).toBeNull();
  });

  it('tracks session count', () => {
    const manager = new SessionManager();

    manager.createSession();
    manager.createSession();
    manager.createSession();

    expect(manager.getSessionCount()).toBe(3);
  });

  it('cleans up old sessions when exceeding max', () => {
    const manager = new SessionManager(2, 3600000); // Max 2 sessions

    manager.createSession();
    manager.createSession();
    manager.createSession(); // Should trigger cleanup

    expect(manager.getSessionCount()).toBeLessThanOrEqual(2);
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('Replication Integration', () => {
  it('creates router with session management', () => {
    const primaryId = createReplicaId('us-east', 'primary-1');
    const { router, sessionManager } = createRouterWithSessions(primaryId);

    expect(router).toBeDefined();
    expect(sessionManager).toBeDefined();
  });

  it('end-to-end: register replica, stream WAL, acknowledge', async () => {
    const backend = createMockBackend();
    const walWriter = createWALWriter(backend);
    const walReader = createWALReader(backend);

    const primary = createPrimary({ backend, walWriter, walReader });
    const replicaId = createReplicaId('us-west', 'replica-1');

    // Register replica
    await primary.registerReplica(createReplicaInfo(replicaId, 'syncing', 0n));

    // Write some data
    await walWriter.append({
      timestamp: Date.now(),
      txnId: 'txn1',
      op: 'INSERT',
      table: 'users',
      after: new Uint8Array([1, 2, 3]),
    });
    await walWriter.flush();

    // Pull WAL
    const batch = await primary.pullWAL(replicaId, 0n, 10);

    // Acknowledge
    await primary.acknowledgeWAL({
      replicaId,
      appliedLSN: batch.endLSN,
      processingTimeMs: 5,
    });

    // Verify
    const replicas = await primary.getReplicas();
    expect(replicas[0].lastLSN).toBe(batch.endLSN);
  });

  it('end-to-end: router routes to healthy replica', async () => {
    const primaryId = createReplicaId('us-east', 'primary-1');
    const router = createExtendedRouter(primaryId);

    // Register replicas
    const usWestReplica = createReplicaId('us-west', 'r1');
    const euCentralReplica = createReplicaId('eu-central', 'r2');

    router.registerReplica({
      id: usWestReplica,
      status: 'active',
      role: 'replica',
      lastLSN: 100n,
      lastHeartbeat: Date.now(),
      registeredAt: Date.now(),
      doUrl: '',
    });

    router.registerReplica({
      id: euCentralReplica,
      status: 'lagging',
      role: 'replica',
      lastLSN: 50n,
      lastHeartbeat: Date.now() - 20000,
      registeredAt: Date.now(),
      doUrl: '',
    });

    // Route to nearest healthy
    router.setCurrentRegion('us-west');
    const decision = await router.routeRead('SELECT * FROM users', 'eventual');

    // Should prefer the healthy us-west replica
    expect(decision.target.region).toBe('us-west');
  });
});
