/**
 * Auto-Promotion and Split-Brain Detection Tests
 *
 * Tests for:
 * - Auto-promotion when primary becomes unavailable
 * - Split-brain detection using fencing tokens
 * - Leader election protocol
 * - Promotion eligibility checks (replication lag, data freshness)
 *
 * TIMING NOTE: Tests use real timers with 100ms delays for heartbeat timeout testing.
 * Real timers are required because:
 * - The replica tracks lastHeartbeat using Date.now()
 * - Heartbeat timeout detection compares timestamps against current time
 * - The auto-promotion logic depends on actual elapsed time
 *
 * Timeout values are kept small (50-100ms) to minimize test duration while still
 * validating the time-based promotion eligibility logic. These delays are
 * acceptable as they test real temporal behavior rather than mocked time.
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

import {
  type ReplicaId,
  type ReplicaInfo,
  type WALBatch,
  type VoteRequest,
  type VoteResponse,
  type LeaderHeartbeat,
  type FencingToken,
  ReplicationError,
  ReplicationErrorCode,
  serializeReplicaId,
} from '../types.js';

import { createReplica, type CreateReplicaOptions } from '../replica.js';
import { createWALWriter } from '../../wal/index.js';
import { crc32 } from '../../wal/writer.js';
import type { WALWriter } from '../../wal/types.js';
import type { DOStorageBackend } from '../../fsx/types.js';
import {
  generateFencingToken,
  validateFencingTokenSignature,
} from '../leader-election.js';

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
// PROMOTION ELIGIBILITY TESTS
// =============================================================================

describe('Auto-Promotion - Eligibility Checks', () => {
  let backend: DOStorageBackend;
  let walWriter: WALWriter;

  beforeEach(() => {
    backend = createMockBackend();
    walWriter = createWALWriter(backend);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Basic Eligibility', () => {
    it('is not eligible immediately after initialization', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const eligibility = await replica.checkPromotionEligibility();

      // Recently initialized - leader should still be considered active
      expect(eligibility.eligible).toBe(false);
      expect(eligibility.reason).toContain('Leader still active');
    });

    it('is eligible after primary timeout', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Wait for timeout
      await new Promise(resolve => setTimeout(resolve, 100));

      const eligibility = await replica.checkPromotionEligibility();

      expect(eligibility.eligible).toBe(true);
    });

    it('is not eligible when auto-failover is disabled', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { autoFailover: false },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const eligibility = await replica.checkPromotionEligibility();

      expect(eligibility.eligible).toBe(false);
      expect(eligibility.reason).toContain('Auto-failover disabled');
    });

    it('is not eligible when already primary', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true, quorumSize: 1 },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Wait for timeout and auto-promote (this properly becomes leader in election state machine)
      await new Promise(resolve => setTimeout(resolve, 100));
      const result = await replica.autoPromote();
      expect(result.success).toBe(true);

      const eligibility = await replica.checkPromotionEligibility();

      expect(eligibility.eligible).toBe(false);
      expect(eligibility.reason).toContain('Already leader');
    });
  });

  describe('Replication Lag', () => {
    it('reports LSN lag correctly', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId, 'syncing', 50n));

      // Simulate receiving a heartbeat with higher LSN
      const leaderId = createReplicaId('us-east', 'primary');
      const token = generateFencingToken(1n, leaderId);
      const heartbeat: LeaderHeartbeat = {
        leaderId,
        term: 1n,
        fencingToken: token,
        currentLSN: 100n, // Primary is at 100, replica at 50 = 50 entries lag
        timestamp: Date.now(),
      };

      await replica.handleLeaderHeartbeat(heartbeat);

      // Wait for timeout
      await new Promise(resolve => setTimeout(resolve, 100));

      const eligibility = await replica.checkPromotionEligibility();

      expect(eligibility.lsnLag).toBe(50n);
    });

    it('calculates priority based on lag', async () => {
      const replica1Backend = createMockBackend();
      const replica2Backend = createMockBackend();
      const walWriter1 = createWALWriter(replica1Backend);
      const walWriter2 = createWALWriter(replica2Backend);

      const replica1 = createReplica({
        backend: replica1Backend,
        walWriter: walWriter1,
        config: { heartbeatTimeoutMs: 50, autoFailover: true },
      });
      const replica2 = createReplica({
        backend: replica2Backend,
        walWriter: walWriter2,
        config: { heartbeatTimeoutMs: 50, autoFailover: true },
      });

      await replica1.initialize('https://primary.do', createReplicaInfo(createReplicaId('us-west', 'r1'), 'syncing', 95n));
      await replica2.initialize('https://primary.do', createReplicaInfo(createReplicaId('us-east', 'r2'), 'syncing', 50n));

      // Both receive heartbeat from same primary with LSN 100
      const leaderId = createReplicaId('us-central', 'primary');
      const token = generateFencingToken(1n, leaderId);
      const heartbeat: LeaderHeartbeat = {
        leaderId,
        term: 1n,
        fencingToken: token,
        currentLSN: 100n,
        timestamp: Date.now(),
      };

      await replica1.handleLeaderHeartbeat(heartbeat);
      await replica2.handleLeaderHeartbeat(heartbeat);

      // Wait for timeout
      await new Promise(resolve => setTimeout(resolve, 100));

      const eligibility1 = await replica1.checkPromotionEligibility();
      const eligibility2 = await replica2.checkPromotionEligibility();

      // Replica 1 at LSN 95 has lag of 5, Replica 2 at LSN 50 has lag of 50
      // Replica 1 should have higher priority (less lag)
      expect(eligibility1.lsnLag).toBe(5n);
      expect(eligibility2.lsnLag).toBe(50n);
      expect(eligibility1.priority).toBeGreaterThan(eligibility2.priority);
    });
  });
});

// =============================================================================
// LEADER ELECTION TESTS
// =============================================================================

describe('Auto-Promotion - Leader Election', () => {
  let backend: DOStorageBackend;
  let walWriter: WALWriter;

  beforeEach(() => {
    backend = createMockBackend();
    walWriter = createWALWriter(backend);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Starting Election', () => {
    it('starts election when eligible', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Wait for timeout
      await new Promise(resolve => setTimeout(resolve, 100));

      const electionState = await replica.startElection();

      expect(electionState.term).toBe(1n);
      expect(electionState.votedFor).toEqual(replicaId);
      expect(electionState.fencingToken).not.toBeNull();
    });

    it('throws error when not eligible', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Don't wait - should not be eligible
      await expect(replica.startElection()).rejects.toThrow(ReplicationError);
    });

    it('throws error when election already in progress', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Wait for timeout
      await new Promise(resolve => setTimeout(resolve, 100));

      // Start first election
      await replica.startElection();

      // Try to start another - should fail
      await expect(replica.startElection()).rejects.toThrow(ReplicationError);
    });

    it('generates valid fencing token', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Wait for timeout
      await new Promise(resolve => setTimeout(resolve, 100));

      const electionState = await replica.startElection();

      expect(electionState.fencingToken).not.toBeNull();
      expect(validateFencingTokenSignature(electionState.fencingToken!)).toBe(true);
      expect(electionState.fencingToken!.generatedBy).toEqual(replicaId);
    });
  });

  describe('Vote Handling', () => {
    it('grants vote to valid candidate', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const candidateId = createReplicaId('us-east', 'replica-2');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId, 'syncing', 50n));

      const token = generateFencingToken(1n, candidateId);
      const voteRequest: VoteRequest = {
        candidateId,
        term: 1n,
        lastLSN: 100n, // Candidate is ahead
        fencingToken: token,
      };

      const response = await replica.handleVoteRequest(voteRequest);

      expect(response.voteGranted).toBe(true);
      expect(response.voterId).toEqual(replicaId);
    });

    it('rejects vote from candidate with lower LSN', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const candidateId = createReplicaId('us-east', 'replica-2');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId, 'syncing', 100n));

      const token = generateFencingToken(1n, candidateId);
      const voteRequest: VoteRequest = {
        candidateId,
        term: 1n,
        lastLSN: 50n, // Candidate is behind
        fencingToken: token,
      };

      const response = await replica.handleVoteRequest(voteRequest);

      expect(response.voteGranted).toBe(false);
      expect(response.reason).toContain('Candidate LSN');
    });

    it('rejects vote from old term', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const candidateId = createReplicaId('us-east', 'replica-2');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Wait and start our own election to advance term
      await new Promise(resolve => setTimeout(resolve, 100));
      await replica.startElection();

      // Now try to handle a vote request from an older term
      const token = generateFencingToken(0n, candidateId);
      const voteRequest: VoteRequest = {
        candidateId,
        term: 0n,
        lastLSN: 100n,
        fencingToken: token,
      };

      const response = await replica.handleVoteRequest(voteRequest);

      expect(response.voteGranted).toBe(false);
      expect(response.reason).toContain('Stale term');
    });

    it('rejects vote with invalid fencing token', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const candidateId = createReplicaId('us-east', 'replica-2');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const invalidToken: FencingToken = {
        epoch: 1n,
        generatedAt: Date.now(),
        generatedBy: candidateId,
        signature: 'invalid-signature',
      };

      const voteRequest: VoteRequest = {
        candidateId,
        term: 1n,
        lastLSN: 100n,
        fencingToken: invalidToken,
      };

      const response = await replica.handleVoteRequest(voteRequest);

      expect(response.voteGranted).toBe(false);
      expect(response.reason).toContain('Invalid fencing token');
    });
  });

  describe('Leader Heartbeat Handling', () => {
    it('updates state on valid heartbeat', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const leaderId = createReplicaId('us-east', 'primary');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId, 'syncing', 50n));

      const token = generateFencingToken(1n, leaderId);
      const heartbeat: LeaderHeartbeat = {
        leaderId,
        term: 1n,
        fencingToken: token,
        currentLSN: 100n,
        timestamp: Date.now(),
      };

      await replica.handleLeaderHeartbeat(heartbeat);

      const electionState = await replica.getElectionState();
      expect(electionState.leader).toEqual(leaderId);
      expect(electionState.fencingToken).toEqual(token);
    });

    it('steps down from candidate on receiving heartbeat from valid leader', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const leaderId = createReplicaId('us-east', 'primary');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Wait for timeout and start election
      await new Promise(resolve => setTimeout(resolve, 100));
      await replica.startElection();

      // Now receive heartbeat from leader with higher term
      const token = generateFencingToken(5n, leaderId);
      const heartbeat: LeaderHeartbeat = {
        leaderId,
        term: 5n,
        fencingToken: token,
        currentLSN: 100n,
        timestamp: Date.now(),
      };

      await replica.handleLeaderHeartbeat(heartbeat);

      // Should no longer be eligible (leader is active)
      const eligibility = await replica.checkPromotionEligibility();
      expect(eligibility.eligible).toBe(false);
      expect(eligibility.reason).toContain('Leader still active');
    });

    it('ignores heartbeat from old term', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const oldLeader = createReplicaId('us-east', 'old-primary');
      const newLeader = createReplicaId('eu-central', 'new-primary');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Accept new leader first
      const newToken = generateFencingToken(5n, newLeader);
      await replica.handleLeaderHeartbeat({
        leaderId: newLeader,
        term: 5n,
        fencingToken: newToken,
        currentLSN: 100n,
        timestamp: Date.now(),
      });

      // Now try to accept heartbeat from old leader
      const oldToken = generateFencingToken(1n, oldLeader);
      await replica.handleLeaderHeartbeat({
        leaderId: oldLeader,
        term: 1n,
        fencingToken: oldToken,
        currentLSN: 50n,
        timestamp: Date.now(),
      });

      // Should still have new leader
      const electionState = await replica.getElectionState();
      expect(electionState.leader).toEqual(newLeader);
      expect(electionState.term).toBe(5n);
    });
  });
});

// =============================================================================
// SPLIT-BRAIN DETECTION TESTS
// =============================================================================

describe('Auto-Promotion - Split-Brain Detection', () => {
  let backend: DOStorageBackend;
  let walWriter: WALWriter;

  beforeEach(() => {
    backend = createMockBackend();
    walWriter = createWALWriter(backend);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Detection', () => {
    it('detects no split-brain with single leader', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const leaderId = createReplicaId('us-east', 'primary');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Receive heartbeat from single leader
      const token = generateFencingToken(1n, leaderId);
      await replica.handleLeaderHeartbeat({
        leaderId,
        term: 1n,
        fencingToken: token,
        currentLSN: 100n,
        timestamp: Date.now(),
      });

      const detection = await replica.detectSplitBrain();

      expect(detection.detected).toBe(false);
      expect(detection.resolution).toBe('none');
    });

    it('detects split-brain with multiple leaders', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const leader1 = createReplicaId('us-east', 'primary-1');
      const leader2 = createReplicaId('eu-central', 'primary-2');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Receive heartbeat from first leader
      const token1 = generateFencingToken(1n, leader1);
      await replica.handleLeaderHeartbeat({
        leaderId: leader1,
        term: 1n,
        fencingToken: token1,
        currentLSN: 100n,
        timestamp: Date.now(),
      });

      // Receive heartbeat from second leader (simulating split-brain)
      const token2 = generateFencingToken(1n, leader2);
      await replica.handleLeaderHeartbeat({
        leaderId: leader2,
        term: 1n,
        fencingToken: token2,
        currentLSN: 95n,
        timestamp: Date.now(),
      });

      const detection = await replica.detectSplitBrain();

      expect(detection.detected).toBe(true);
      expect(detection.conflictingLeaders.length).toBe(2);
      expect(detection.resolution).toBe('fencing');
    });

    it('deduplicates same leader', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const leaderId = createReplicaId('us-east', 'primary');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Receive multiple heartbeats from same leader
      for (let i = 0; i < 5; i++) {
        const token = generateFencingToken(1n, leaderId);
        await replica.handleLeaderHeartbeat({
          leaderId,
          term: 1n,
          fencingToken: token,
          currentLSN: BigInt(100 + i),
          timestamp: Date.now(),
        });
      }

      const detection = await replica.detectSplitBrain();

      expect(detection.detected).toBe(false);
    });
  });

  describe('Fencing Token Validation', () => {
    it('validates current token', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true, fencingTokenTtlMs: 60000 },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const leaderId = createReplicaId('us-east', 'primary');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Accept leader with token
      const token = generateFencingToken(1n, leaderId);
      await replica.handleLeaderHeartbeat({
        leaderId,
        term: 1n,
        fencingToken: token,
        currentLSN: 100n,
        timestamp: Date.now(),
      });

      const isValid = await replica.validateFencingToken(token);
      expect(isValid).toBe(true);
    });

    it('rejects token from old term', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const leaderId = createReplicaId('us-east', 'primary');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Accept leader at term 5
      const currentToken = generateFencingToken(5n, leaderId);
      await replica.handleLeaderHeartbeat({
        leaderId,
        term: 5n,
        fencingToken: currentToken,
        currentLSN: 100n,
        timestamp: Date.now(),
      });

      // Try to validate old token
      const oldToken = generateFencingToken(1n, leaderId);
      const isValid = await replica.validateFencingToken(oldToken);

      expect(isValid).toBe(false);
    });

    it('rejects token with invalid signature', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const invalidToken: FencingToken = {
        epoch: 1n,
        generatedAt: Date.now(),
        generatedBy: createReplicaId('attacker', 'x'),
        signature: 'fake-signature',
      };

      const isValid = await replica.validateFencingToken(invalidToken);
      expect(isValid).toBe(false);
    });
  });
});

// =============================================================================
// AUTO-PROMOTE INTEGRATION TESTS
// =============================================================================

describe('Auto-Promotion - autoPromote()', () => {
  let backend: DOStorageBackend;
  let walWriter: WALWriter;

  beforeEach(() => {
    backend = createMockBackend();
    walWriter = createWALWriter(backend);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Successful Promotion', () => {
    it('auto-promotes with quorum size 1', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true, quorumSize: 1 },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Wait for timeout
      await new Promise(resolve => setTimeout(resolve, 100));

      const result = await replica.autoPromote();

      expect(result.success).toBe(true);
      expect(result.fencingToken).not.toBeNull();
      expect(validateFencingTokenSignature(result.fencingToken!)).toBe(true);

      // Verify we're now primary
      const status = await replica.getStatus();
      expect(status.role).toBe('primary');
    });

    it('generates fencing token on promotion', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true, quorumSize: 1 },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Wait for timeout
      await new Promise(resolve => setTimeout(resolve, 100));

      const result = await replica.autoPromote();

      expect(result.success).toBe(true);
      expect(result.fencingToken).not.toBeNull();
      expect(result.fencingToken!.generatedBy).toEqual(replicaId);
      expect(result.fencingToken!.epoch).toBeGreaterThan(0n);
    });
  });

  describe('Failed Promotion', () => {
    it('fails when not eligible', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Don't wait for timeout
      const result = await replica.autoPromote();

      expect(result.success).toBe(false);
      expect(result.fencingToken).toBeNull();
      expect(result.error).toContain('Leader still active');

      // Verify we're still replica
      const status = await replica.getStatus();
      expect(status.role).toBe('replica');
    });

    it('fails when auto-failover is disabled', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { autoFailover: false },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const result = await replica.autoPromote();

      expect(result.success).toBe(false);
      expect(result.error).toContain('Auto-failover disabled');
    });

    it('returns error when not initialized', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { autoFailover: true },
      });

      const result = await replica.autoPromote();

      expect(result.success).toBe(false);
      expect(result.error).toContain('not initialized');
    });
  });

  describe('Split-Brain Handling', () => {
    it('handles split-brain during auto-promotion', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true, quorumSize: 1 },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const conflictingLeader = createReplicaId('eu-central', 'conflicting');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId, 'syncing', 100n));

      // Simulate receiving heartbeats from multiple "leaders"
      const token1 = generateFencingToken(1n, conflictingLeader);
      await replica.handleLeaderHeartbeat({
        leaderId: conflictingLeader,
        term: 1n,
        fencingToken: token1,
        currentLSN: 50n, // Lower LSN than us
        timestamp: Date.now(),
      });

      // Wait for timeout
      await new Promise(resolve => setTimeout(resolve, 100));

      // Try to auto-promote
      const result = await replica.autoPromote();

      // Should succeed or fail based on split-brain resolution
      // Our replica has higher LSN, so should win or proceed
      // In this test, we only have one conflicting leader with lower data
      expect(result.success).toBe(true);
    });
  });
});

// =============================================================================
// ELECTION STATE TESTS
// =============================================================================

describe('Auto-Promotion - Election State', () => {
  let backend: DOStorageBackend;
  let walWriter: WALWriter;

  beforeEach(() => {
    backend = createMockBackend();
    walWriter = createWALWriter(backend);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('getElectionState()', () => {
    it('returns initial state', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const state = await replica.getElectionState();

      expect(state.term).toBe(0n);
      expect(state.leader).toBeNull();
      expect(state.votedFor).toBeNull();
    });

    it('updates after starting election', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Wait for timeout
      await new Promise(resolve => setTimeout(resolve, 100));

      await replica.startElection();

      const state = await replica.getElectionState();

      expect(state.term).toBe(1n);
      expect(state.votedFor).toEqual(replicaId);
      expect(state.votes.get(serializeReplicaId(replicaId))).toBe(true);
    });

    it('updates after receiving heartbeat', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const leaderId = createReplicaId('us-east', 'primary');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      const token = generateFencingToken(3n, leaderId);
      await replica.handleLeaderHeartbeat({
        leaderId,
        term: 3n,
        fencingToken: token,
        currentLSN: 100n,
        timestamp: Date.now(),
      });

      const state = await replica.getElectionState();

      expect(state.term).toBe(3n);
      expect(state.leader).toEqual(leaderId);
      expect(state.fencingToken).toEqual(token);
    });
  });

  describe('State Reset on Demotion', () => {
    it('resets election state on demotion', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true, quorumSize: 1 },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Wait and promote
      await new Promise(resolve => setTimeout(resolve, 100));
      await replica.autoPromote();

      // Verify we're primary
      let status = await replica.getStatus();
      expect(status.role).toBe('primary');

      // Now demote
      await replica.demoteToReplica('https://new-primary.do');

      // Verify role changed
      status = await replica.getStatus();
      expect(status.role).toBe('replica');

      // Election state should be reset
      const electionState = await replica.getElectionState();
      expect(electionState.term).toBe(0n);
      expect(electionState.leader).toBeNull();
    });
  });
});

// =============================================================================
// EDGE CASES AND ERROR HANDLING
// =============================================================================

describe('Auto-Promotion - Edge Cases', () => {
  let backend: DOStorageBackend;
  let walWriter: WALWriter;

  beforeEach(() => {
    backend = createMockBackend();
    walWriter = createWALWriter(backend);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Rapid State Changes', () => {
    it('handles rapid promotion/demotion cycles', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 50, autoFailover: true, quorumSize: 1 },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      for (let i = 0; i < 3; i++) {
        // Wait for timeout
        await new Promise(resolve => setTimeout(resolve, 60));

        // Promote
        await replica.autoPromote();

        const promotedStatus = await replica.getStatus();
        expect(promotedStatus.role).toBe('primary');

        // Demote
        await replica.demoteToReplica(`https://primary-${i}.do`);

        const demotedStatus = await replica.getStatus();
        expect(demotedStatus.role).toBe('replica');
      }
    });

    it('handles multiple heartbeats from different leaders', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Receive heartbeats from multiple leaders with increasing terms
      for (let i = 1; i <= 5; i++) {
        const leaderId = createReplicaId(`region-${i}`, `leader-${i}`);
        const token = generateFencingToken(BigInt(i), leaderId);
        await replica.handleLeaderHeartbeat({
          leaderId,
          term: BigInt(i),
          fencingToken: token,
          currentLSN: BigInt(100 + i),
          timestamp: Date.now(),
        });
      }

      // Should have the latest leader
      const electionState = await replica.getElectionState();
      expect(electionState.term).toBe(5n);
      expect(electionState.leader?.region).toBe('region-5');
    });
  });

  describe('Concurrent Operations', () => {
    it('handles concurrent heartbeats', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { heartbeatTimeoutMs: 15000, autoFailover: true },
      });
      const replicaId = createReplicaId('us-west', 'replica-1');
      const leaderId = createReplicaId('us-east', 'primary');

      await replica.initialize('https://primary.do', createReplicaInfo(replicaId));

      // Send multiple heartbeats concurrently
      const heartbeatPromises = [];
      for (let i = 0; i < 10; i++) {
        const token = generateFencingToken(1n, leaderId);
        heartbeatPromises.push(replica.handleLeaderHeartbeat({
          leaderId,
          term: 1n,
          fencingToken: token,
          currentLSN: BigInt(100 + i),
          timestamp: Date.now(),
        }));
      }

      // Should complete without errors
      await Promise.all(heartbeatPromises);

      // Should still have valid state
      const electionState = await replica.getElectionState();
      expect(electionState.leader).toEqual(leaderId);
    });
  });

  describe('Error States', () => {
    it('throws when checking eligibility before initialization', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { autoFailover: true },
      });

      await expect(replica.checkPromotionEligibility()).rejects.toThrow(ReplicationError);
    });

    it('throws when starting election before initialization', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { autoFailover: true },
      });

      await expect(replica.startElection()).rejects.toThrow(ReplicationError);
    });

    it('throws when handling vote before initialization', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { autoFailover: true },
      });

      const candidateId = createReplicaId('us-east', 'candidate');
      const token = generateFencingToken(1n, candidateId);

      await expect(replica.handleVoteRequest({
        candidateId,
        term: 1n,
        lastLSN: 100n,
        fencingToken: token,
      })).rejects.toThrow(ReplicationError);
    });

    it('throws when detecting split-brain before initialization', async () => {
      const replica = createReplica({
        backend,
        walWriter,
        config: { autoFailover: true },
      });

      await expect(replica.detectSplitBrain()).rejects.toThrow(ReplicationError);
    });
  });
});
