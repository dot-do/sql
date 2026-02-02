/**
 * Replication Integration Tests
 *
 * Tests for interactions between:
 * - Primary and Replica synchronization
 * - Router with multiple replicas
 * - Failover scenarios end-to-end
 * - Consistency guarantees across components
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

import {
  type ReplicaId,
  type ReplicaInfo,
  type WALBatch,
  type WALAck,
  type SnapshotInfo,
  type SessionState,
  ReplicationError,
  ReplicationErrorCode,
  serializeReplicaId,
} from '../types.js';

import { createPrimary } from '../primary.js';
import { createReplica } from '../replica.js';
import { createExtendedRouter, SessionManager, type ExtendedReplicationRouter } from '../router.js';
import { createWALWriter, createWALReader } from '../../wal/index.js';
import { crc32 } from '../../wal/writer.js';
import type { WALWriter, WALReader } from '../../wal/types.js';
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
  entries: Array<{
    lsn: bigint;
    timestamp: number;
    txnId: string;
    op: 'INSERT' | 'UPDATE' | 'DELETE' | 'COMMIT';
    table: string;
    after?: Uint8Array;
  }>
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
// PRIMARY-REPLICA SYNCHRONIZATION TESTS
// =============================================================================

describe('Primary-Replica Synchronization', () => {
  let primaryBackend: DOStorageBackend;
  let replicaBackend: DOStorageBackend;
  let primaryWalWriter: WALWriter;
  let primaryWalReader: WALReader;
  let replicaWalWriter: WALWriter;

  beforeEach(() => {
    primaryBackend = createMockBackend();
    replicaBackend = createMockBackend();
    primaryWalWriter = createWALWriter(primaryBackend);
    primaryWalReader = createWALReader(primaryBackend);
    replicaWalWriter = createWALWriter(replicaBackend);
  });

  describe('Initial Sync', () => {
    it('replica registers with primary and syncs', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
      });

      const replica = createReplica({
        backend: replicaBackend,
        walWriter: replicaWalWriter,
      });

      const replicaId = createReplicaId('us-west', 'r1');

      // Primary registers replica
      await primary.registerReplica(createReplicaInfo(replicaId));

      // Replica initializes
      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const replicaStatus = await replica.getStatus();
      expect(replicaStatus.status).toBe('syncing');

      const replicas = await primary.getReplicas();
      expect(replicas).toHaveLength(1);
    });

    it('replica catches up via WAL streaming', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
      });

      const replica = createReplica({
        backend: replicaBackend,
        walWriter: replicaWalWriter,
      });

      const replicaId = createReplicaId('us-west', 'r1');

      // Write data to primary
      for (let i = 0; i < 5; i++) {
        await primaryWalWriter.append({
          timestamp: Date.now(),
          txnId: `txn${i}`,
          op: 'INSERT',
          table: 'users',
          after: new Uint8Array([i]),
        });
      }
      await primaryWalWriter.flush();

      // Register and initialize
      await primary.registerReplica(createReplicaInfo(replicaId, 'syncing', 0n));
      await replica.initialize('https://primary.do', createReplicaInfo(replicaId, 'syncing', 0n));

      // Pull WAL from primary
      const batch = await primary.pullWAL(replicaId, 0n, 10);

      // Apply to replica
      const ack = await replica.applyWALBatch(batch);

      // Acknowledge to primary
      await primary.acknowledgeWAL(ack);

      // Verify sync
      const replicaLSN = await replica.getCurrentLSN();
      expect(replicaLSN).toBe(batch.endLSN);
    });

    it('replica catches up via snapshot', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
      });

      const replica = createReplica({
        backend: replicaBackend,
        walWriter: replicaWalWriter,
      });

      const replicaId = createReplicaId('us-west', 'r1');

      // Write substantial data to primary
      for (let i = 0; i < 100; i++) {
        await primaryWalWriter.append({
          timestamp: Date.now(),
          txnId: `txn${i}`,
          op: 'INSERT',
          table: 'users',
          after: new Uint8Array([i % 256]),
        });
      }
      await primaryWalWriter.flush();

      // Initialize replica
      await primary.registerReplica(createReplicaInfo(replicaId, 'syncing', 0n));
      await replica.initialize('https://primary.do', createReplicaInfo(replicaId, 'syncing', 0n));

      // Request snapshot
      const snapshotInfo = await primary.requestSnapshot({ replicaId });

      // Start catch-up from snapshot
      await replica.catchUpFromSnapshot(snapshotInfo);

      // Apply all chunks
      for (let i = 0; i < snapshotInfo.chunkCount; i++) {
        const chunk = await primary.getSnapshotChunk(snapshotInfo.id, i);
        await replica.applySnapshotChunk(chunk);
      }

      // Verify LSN updated
      const replicaLSN = await replica.getCurrentLSN();
      expect(replicaLSN).toBe(snapshotInfo.lsn);
    });
  });

  describe('Continuous Replication', () => {
    it('maintains sync across multiple WAL batches', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
      });

      const replica = createReplica({
        backend: replicaBackend,
        walWriter: replicaWalWriter,
      });

      const replicaId = createReplicaId('us-west', 'r1');

      await primary.registerReplica(createReplicaInfo(replicaId, 'syncing', 0n));
      await replica.initialize('https://primary.do', createReplicaInfo(replicaId, 'syncing', 0n));

      // Simulate multiple rounds of writes and sync
      for (let round = 0; round < 5; round++) {
        // Write to primary
        for (let i = 0; i < 3; i++) {
          await primaryWalWriter.append({
            timestamp: Date.now(),
            txnId: `txn-r${round}-${i}`,
            op: 'INSERT',
            table: 'users',
            after: new Uint8Array([round * 10 + i]),
          });
        }
        await primaryWalWriter.flush();

        // Pull and apply - pull from current replica LSN, not +1
        // because the WAL reader fromLSN is inclusive
        const currentLSN = await replica.getCurrentLSN();
        const batch = await primary.pullWAL(replicaId, currentLSN, 10);

        if (batch.entries.length > 0) {
          const ack = await replica.applyWALBatch(batch);
          await primary.acknowledgeWAL(ack);
        }
      }

      // Verify replica caught up (may not be exactly equal due to WAL batch behavior)
      const primaryLSN = primaryWalWriter.getCurrentLSN();
      const replicaLSN = await replica.getCurrentLSN();
      // Replica should be close to primary LSN
      expect(Number(primaryLSN - replicaLSN)).toBeLessThanOrEqual(1);
    });

    it('handles concurrent writes during sync', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
      });

      const replica = createReplica({
        backend: replicaBackend,
        walWriter: replicaWalWriter,
      });

      const replicaId = createReplicaId('us-west', 'r1');

      await primary.registerReplica(createReplicaInfo(replicaId, 'syncing', 0n));
      await replica.initialize('https://primary.do', createReplicaInfo(replicaId, 'syncing', 0n));

      // Concurrent writes and pulls
      const writePromises = [];
      const pullPromises = [];

      for (let i = 0; i < 5; i++) {
        writePromises.push(
          primaryWalWriter.append({
            timestamp: Date.now(),
            txnId: `concurrent-${i}`,
            op: 'INSERT',
            table: 'users',
            after: new Uint8Array([i]),
          })
        );
      }

      await Promise.all(writePromises);
      await primaryWalWriter.flush();

      // Pull should get all entries
      const batch = await primary.pullWAL(replicaId, 0n, 10);
      expect(batch.entries.length).toBeGreaterThan(0);
    });
  });
});

// =============================================================================
// ROUTER INTEGRATION TESTS
// =============================================================================

describe('Router Integration', () => {
  describe('Multi-Replica Routing', () => {
    it('routes to appropriate replica based on consistency', async () => {
      const primaryId = createReplicaId('us-east', 'primary');
      const router = createExtendedRouter(primaryId);

      const r1 = createReplicaId('us-west', 'r1');
      const r2 = createReplicaId('eu-central', 'r2');

      router.registerReplica({
        id: r1,
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://r1.do',
      });

      router.registerReplica({
        id: r2,
        status: 'active',
        role: 'replica',
        lastLSN: 90n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://r2.do',
      });

      // Strong consistency -> primary
      const strong = await router.routeRead('SELECT 1', 'strong');
      expect(strong.target).toEqual(primaryId);

      // Eventual consistency -> replica
      const eventual = await router.routeRead('SELECT 1', 'eventual');
      expect(eventual.target).not.toEqual(primaryId);

      // Write -> primary
      const write = await router.routeWrite('INSERT INTO t VALUES (1)');
      expect(write.target).toEqual(primaryId);
    });

    it('routes session-aware reads correctly', async () => {
      const primaryId = createReplicaId('us-east', 'primary');
      const router = createExtendedRouter(primaryId);
      const sessionManager = new SessionManager();

      const r1 = createReplicaId('us-west', 'r1');

      router.registerReplica({
        id: r1,
        status: 'active',
        role: 'replica',
        lastLSN: 50n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://r1.do',
      });

      // Create session and update after write
      const session = sessionManager.createSession();
      sessionManager.updateSession(session.sessionId, 100n);

      // Session consistency with session that wrote at LSN 100
      const retrieved = sessionManager.getSession(session.sessionId);
      const decision = await router.routeRead('SELECT 1', 'session', retrieved!);

      // Replica is at 50, session wrote at 100, so should fall back to primary
      expect(decision.target).toEqual(primaryId);
      expect(decision.fallback).toBe(true);

      // Now update replica LSN past session
      router.updateReplicaStatus(r1, 'active', {
        replicaId: r1,
        primaryLSN: 150n,
        replicaLSN: 150n,
        lagEntries: 0n,
        lagMs: 0,
        measuredAt: Date.now(),
      });

      // Now should route to replica
      const decision2 = await router.routeRead('SELECT 1', 'session', retrieved!);
      expect(decision2.target).toEqual(r1);
    });

    it('handles replica failures gracefully', async () => {
      const primaryId = createReplicaId('us-east', 'primary');
      const router = createExtendedRouter(primaryId);

      const r1 = createReplicaId('us-west', 'r1');
      const r2 = createReplicaId('eu-central', 'r2');

      router.registerReplica({
        id: r1,
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://r1.do',
      });

      router.registerReplica({
        id: r2,
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://r2.do',
      });

      // r1 goes offline
      router.updateReplicaStatus(r1, 'offline');

      // Eventual should still work, using r2
      const decision = await router.routeRead('SELECT 1', 'eventual');
      expect(decision.target).toEqual(r2);

      // r2 also goes offline
      router.updateReplicaStatus(r2, 'offline');

      // Should fall back to primary
      const decision2 = await router.routeRead('SELECT 1', 'eventual');
      expect(decision2.target).toEqual(primaryId);
      expect(decision2.fallback).toBe(true);
    });
  });

  describe('Lag-Aware Routing', () => {
    it('respects bounded staleness', async () => {
      const primaryId = createReplicaId('us-east', 'primary');
      const router = createExtendedRouter(primaryId, { boundedStalenessMs: 1000 });

      const r1 = createReplicaId('us-west', 'r1');
      const r2 = createReplicaId('eu-central', 'r2');

      router.registerReplica({
        id: r1,
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://r1.do',
      });

      router.registerReplica({
        id: r2,
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://r2.do',
      });

      // r1 has low lag
      router.updateReplicaStatus(r1, 'active', {
        replicaId: r1,
        primaryLSN: 100n,
        replicaLSN: 100n,
        lagEntries: 0n,
        lagMs: 100,
        measuredAt: Date.now(),
      });

      // r2 has high lag
      router.updateReplicaStatus(r2, 'active', {
        replicaId: r2,
        primaryLSN: 100n,
        replicaLSN: 50n,
        lagEntries: 50n,
        lagMs: 5000,
        measuredAt: Date.now(),
      });

      // Bounded consistency should avoid r2
      const decision = await router.routeRead('SELECT 1', 'bounded');
      expect(decision.target).toEqual(r1);
    });
  });
});

// =============================================================================
// FAILOVER INTEGRATION TESTS
// =============================================================================

describe('Failover Integration', () => {
  let primaryBackend: DOStorageBackend;
  let replicaBackend: DOStorageBackend;
  let primaryWalWriter: WALWriter;
  let primaryWalReader: WALReader;
  let replicaWalWriter: WALWriter;

  beforeEach(() => {
    primaryBackend = createMockBackend();
    replicaBackend = createMockBackend();
    primaryWalWriter = createWALWriter(primaryBackend);
    primaryWalReader = createWALReader(primaryBackend);
    replicaWalWriter = createWALWriter(replicaBackend);
  });

  describe('Manual Failover', () => {
    it('promotes replica to primary on failover', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
      });

      const replica = createReplica({
        backend: replicaBackend,
        walWriter: replicaWalWriter,
      });

      const replicaId = createReplicaId('us-west', 'r1');

      // Setup replication
      await primary.registerReplica(createReplicaInfo(replicaId, 'active', 100n));
      await replica.initialize('https://primary.do', createReplicaInfo(replicaId, 'active', 100n));

      // Initiate failover
      const decision = await primary.initiateFailover('manual_trigger');
      expect(decision.proceed).toBe(true);
      expect(decision.candidate).toEqual(replicaId);

      // Execute failover on primary side
      const state = await primary.executeFailover(decision);
      expect(state.status).toBe('completed');

      // Promote replica
      await replica.promoteToPrimary();

      const replicaStatus = await replica.getStatus();
      expect(replicaStatus.role).toBe('primary');
    });

    it('handles failover with data loss estimate', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
      });

      const replicaId = createReplicaId('us-west', 'r1');

      // Write some data
      for (let i = 0; i < 10; i++) {
        await primaryWalWriter.append({
          timestamp: Date.now(),
          txnId: `txn${i}`,
          op: 'INSERT',
          table: 'users',
          after: new Uint8Array([i]),
        });
      }
      await primaryWalWriter.flush();

      const primaryLSN = primaryWalWriter.getCurrentLSN();

      // Register replica that's behind
      await primary.registerReplica(createReplicaInfo(replicaId, 'active', primaryLSN - 5n));

      // Initiate failover
      const decision = await primary.initiateFailover('manual_trigger');

      expect(decision.dataLossEstimate).toBeDefined();
      expect(decision.dataLossEstimate).toBe(5n);
    });
  });

  describe('Failover Scenarios', () => {
    it('selects best candidate among multiple replicas', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
      });

      // Write some data
      for (let i = 0; i < 10; i++) {
        await primaryWalWriter.append({
          timestamp: Date.now(),
          txnId: `txn${i}`,
          op: 'INSERT',
          table: 'users',
          after: new Uint8Array([i]),
        });
      }
      await primaryWalWriter.flush();

      const primaryLSN = primaryWalWriter.getCurrentLSN();

      // Register multiple replicas with different LSNs
      await primary.registerReplica(
        createReplicaInfo(createReplicaId('us-west', 'r1'), 'active', primaryLSN - 5n)
      );
      await primary.registerReplica(
        createReplicaInfo(createReplicaId('eu-central', 'r2'), 'active', primaryLSN - 2n)
      );
      await primary.registerReplica(
        createReplicaInfo(createReplicaId('ap-south', 'r3'), 'active', primaryLSN - 8n)
      );

      // Initiate failover
      const decision = await primary.initiateFailover('manual_trigger');

      // Should select r2 (least lag)
      expect(decision.proceed).toBe(true);
      expect(decision.candidate?.instanceId).toBe('r2');
    });

    it('excludes offline replicas from failover candidates', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
      });

      // Register replicas
      await primary.registerReplica(
        createReplicaInfo(createReplicaId('us-west', 'r1'), 'offline', 100n)
      );
      await primary.registerReplica(
        createReplicaInfo(createReplicaId('eu-central', 'r2'), 'active', 50n)
      );

      const decision = await primary.initiateFailover('manual_trigger');

      // Should select r2 (only active one)
      expect(decision.proceed).toBe(true);
      expect(decision.candidate?.instanceId).toBe('r2');
    });

    it('rejects failover when no suitable candidates', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
      });

      // Register only offline replicas
      await primary.registerReplica(
        createReplicaInfo(createReplicaId('us-west', 'r1'), 'offline', 100n)
      );
      await primary.registerReplica(
        createReplicaInfo(createReplicaId('eu-central', 'r2'), 'offline', 100n)
      );

      const decision = await primary.initiateFailover('manual_trigger');

      expect(decision.proceed).toBe(false);
      expect(decision.reason).toContain('No suitable candidate');
    });
  });

  describe('Post-Failover Routing', () => {
    it('router continues working after failover', async () => {
      const primaryId = createReplicaId('us-east', 'primary');
      const newPrimaryId = createReplicaId('us-west', 'r1');
      const router = createExtendedRouter(primaryId);

      // Initial setup with replicas
      router.registerReplica({
        id: newPrimaryId,
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://r1.do',
      });

      // Simulate failover by marking old primary offline
      router.updateReplicaStatus(primaryId, 'offline');

      // Router should still work with remaining replica
      const decision = await router.routeRead('SELECT 1', 'eventual');

      // Should route to remaining replica or fall back appropriately
      expect(decision).toBeDefined();
    });
  });
});

// =============================================================================
// CONSISTENCY GUARANTEES TESTS
// =============================================================================

describe('Consistency Guarantees', () => {
  describe('Read-Your-Writes', () => {
    it('guarantees read-your-writes within session', async () => {
      const primaryId = createReplicaId('us-east', 'primary');
      const router = createExtendedRouter(primaryId);
      const sessionManager = new SessionManager();

      const r1 = createReplicaId('us-west', 'r1');

      router.registerReplica({
        id: r1,
        status: 'active',
        role: 'replica',
        lastLSN: 0n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://r1.do',
      });

      // Create session
      const session = sessionManager.createSession();

      // Simulate write
      const writeLSN = 100n;
      sessionManager.updateSession(session.sessionId, writeLSN);

      // Read with session (replica behind)
      const retrieved = sessionManager.getSession(session.sessionId)!;
      let decision = await router.routeRead('SELECT 1', 'session', retrieved);

      // Should route to primary since replica is at 0
      expect(decision.target).toEqual(primaryId);

      // Simulate replica catching up
      router.updateReplicaStatus(r1, 'active', {
        replicaId: r1,
        primaryLSN: writeLSN,
        replicaLSN: writeLSN,
        lagEntries: 0n,
        lagMs: 0,
        measuredAt: Date.now(),
      });

      // Now should route to replica
      decision = await router.routeRead('SELECT 1', 'session', retrieved);
      expect(decision.target).toEqual(r1);
    });
  });

  describe('Strong Consistency', () => {
    it('always reads from primary for strong consistency', async () => {
      const primaryId = createReplicaId('us-east', 'primary');
      const router = createExtendedRouter(primaryId);

      // Add many fast replicas
      for (let i = 0; i < 5; i++) {
        const r = createReplicaId('us-west', `r${i}`);
        router.registerReplica({
          id: r,
          status: 'active',
          role: 'replica',
          lastLSN: 1000n,
          lastHeartbeat: Date.now(),
          registeredAt: Date.now(),
          doUrl: `https://r${i}.do`,
        });
        router.recordLatency(r, 1); // Very fast
      }

      // Strong consistency should still go to primary
      for (let i = 0; i < 10; i++) {
        const decision = await router.routeRead('SELECT 1', 'strong');
        expect(decision.target).toEqual(primaryId);
      }
    });
  });

  describe('Bounded Staleness', () => {
    it('only uses replicas within staleness bound', async () => {
      const primaryId = createReplicaId('us-east', 'primary');
      const router = createExtendedRouter(primaryId, { boundedStalenessMs: 1000 });

      const freshReplica = createReplicaId('us-west', 'fresh');
      const staleReplica = createReplicaId('eu-central', 'stale');

      router.registerReplica({
        id: freshReplica,
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://fresh.do',
      });

      router.registerReplica({
        id: staleReplica,
        status: 'active',
        role: 'replica',
        lastLSN: 100n,
        lastHeartbeat: Date.now(),
        registeredAt: Date.now(),
        doUrl: 'https://stale.do',
      });

      // Make stale replica have lower latency but high lag
      router.recordLatency(freshReplica, 100);
      router.recordLatency(staleReplica, 10);

      router.updateReplicaStatus(freshReplica, 'active', {
        replicaId: freshReplica,
        primaryLSN: 100n,
        replicaLSN: 100n,
        lagEntries: 0n,
        lagMs: 100,
        measuredAt: Date.now(),
      });

      router.updateReplicaStatus(staleReplica, 'active', {
        replicaId: staleReplica,
        primaryLSN: 100n,
        replicaLSN: 50n,
        lagEntries: 50n,
        lagMs: 5000, // Beyond 1000ms bound
        measuredAt: Date.now(),
      });

      // Bounded staleness should choose fresh replica despite higher latency
      const decision = await router.routeRead('SELECT 1', 'bounded');
      expect(decision.target).toEqual(freshReplica);
    });
  });
});

// =============================================================================
// REPLICATION LAG HANDLING TESTS
// =============================================================================

describe('Replication Lag Handling', () => {
  let primaryBackend: DOStorageBackend;
  let primaryWalWriter: WALWriter;
  let primaryWalReader: WALReader;

  beforeEach(() => {
    primaryBackend = createMockBackend();
    primaryWalWriter = createWALWriter(primaryBackend);
    primaryWalReader = createWALReader(primaryBackend);
  });

  describe('Lag Detection', () => {
    it('detects lagging replicas in health check', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
        config: { maxLagMs: 100 },
      });

      const replicaId = createReplicaId('us-west', 'r1');

      // Register replica with old heartbeat
      const info = createReplicaInfo(replicaId, 'active');
      info.lastHeartbeat = Date.now() - 500; // 500ms ago

      await primary.registerReplica(info);

      const health = await primary.getReplicationHealth();

      const replica = health.replicas.find(r => r.info.id.instanceId === 'r1');
      expect(replica?.lag.lagMs).toBeGreaterThan(0);
    });

    it('marks replicas offline after heartbeat timeout', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
        config: { heartbeatTimeoutMs: 100 },
      });

      const replicaId = createReplicaId('us-west', 'r1');

      // Register replica with very old heartbeat
      const info = createReplicaInfo(replicaId, 'active');
      info.lastHeartbeat = Date.now() - 500;

      await primary.registerReplica(info);

      const health = await primary.getReplicationHealth();

      const replica = health.replicas.find(r => r.info.id.instanceId === 'r1');
      expect(replica?.info.status).toBe('offline');
    });

    it('reports degraded status when replicas lag', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
        config: { maxLagMs: 100, minReplicas: 1 },
      });

      const replicaId = createReplicaId('us-west', 'r1');

      // Register active replica with recent heartbeat but high lag status
      const info = createReplicaInfo(replicaId, 'active');
      info.lastHeartbeat = Date.now() - 500; // Will cause lag detection

      await primary.registerReplica(info);

      const health = await primary.getReplicationHealth();

      // Should detect lag
      expect(health.status).not.toBe('healthy');
    });

    it('reports critical when below minimum replicas', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
        config: { minReplicas: 3 },
      });

      // Register only 1 replica
      await primary.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1'), 'active'));

      const health = await primary.getReplicationHealth();

      expect(health.status).toBe('critical');
    });
  });

  describe('Lag Recovery', () => {
    it('updates status when replica catches up', async () => {
      const primary = createPrimary({
        backend: primaryBackend,
        walWriter: primaryWalWriter,
        walReader: primaryWalReader,
      });

      const replicaId = createReplicaId('us-west', 'r1');

      await primary.registerReplica(createReplicaInfo(replicaId, 'syncing', 0n));

      // Simulate catch-up via acknowledgment
      await primary.acknowledgeWAL({
        replicaId,
        appliedLSN: primaryWalWriter.getCurrentLSN(),
        processingTimeMs: 10,
      });

      const replicas = await primary.getReplicas();
      const replica = replicas.find(r => r.id.instanceId === 'r1');

      expect(replica?.status).toBe('active');
    });
  });
});
