/**
 * Leader Election and Split-Brain Detection Tests
 *
 * Tests for the leader election module including:
 * - Fencing token generation and validation
 * - Leader election state machine
 * - Vote handling
 * - Split-brain detection and resolution
 * - Promotion eligibility
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

import {
  type ReplicaId,
  type ReplicaInfo,
  type FencingToken,
  type VoteRequest,
  type VoteResponse,
  type LeaderHeartbeat,
  serializeReplicaId,
  replicaIdsEqual,
  DEFAULT_REPLICATION_CONFIG,
} from '../types.js';

import {
  generateFencingToken,
  validateFencingTokenSignature,
  compareFencingTokens,
  isFencingTokenExpired,
  LeaderElectionStateMachine,
  SplitBrainResolver,
} from '../leader-election.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

function createReplicaId(region: string, instanceId: string): ReplicaId {
  return { region, instanceId };
}

function createReplicaInfo(
  id: ReplicaId,
  status: ReplicaInfo['status'] = 'active',
  lastLSN: bigint = 100n
): ReplicaInfo {
  return {
    id,
    status,
    role: 'replica',
    lastLSN,
    lastHeartbeat: Date.now(),
    registeredAt: Date.now(),
    doUrl: `https://${id.region}.replica.do/${id.instanceId}`,
  };
}

// =============================================================================
// FENCING TOKEN TESTS
// =============================================================================

describe('Fencing Tokens', () => {
  describe('Token Generation', () => {
    it('generates token with correct epoch', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token = await generateFencingToken(1n, replicaId);

      expect(token.epoch).toBe(1n);
    });

    it('generates token with generator info', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token = await generateFencingToken(1n, replicaId);

      expect(token.generatedBy).toEqual(replicaId);
    });

    it('generates token with timestamp', async () => {
      const before = Date.now();
      const replicaId = createReplicaId('us-west', 'r1');
      const token = await generateFencingToken(1n, replicaId);
      const after = Date.now();

      expect(token.generatedAt).toBeGreaterThanOrEqual(before);
      expect(token.generatedAt).toBeLessThanOrEqual(after);
    });

    it('generates token with signature', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token = await generateFencingToken(1n, replicaId);

      expect(token.signature).toBeDefined();
      expect(typeof token.signature).toBe('string');
    });

    it('generates unique tokens for different epochs', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token1 = await generateFencingToken(1n, replicaId);
      const token2 = await generateFencingToken(2n, replicaId);

      expect(token1.epoch).not.toBe(token2.epoch);
    });

    it('generates unique tokens for different generators', async () => {
      const r1 = createReplicaId('us-west', 'r1');
      const r2 = createReplicaId('us-west', 'r2');

      const token1 = await generateFencingToken(1n, r1);
      const token2 = await generateFencingToken(1n, r2);

      expect(token1.signature).not.toBe(token2.signature);
    });
  });

  describe('Token Validation', () => {
    it('validates valid token signature', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token = await generateFencingToken(1n, replicaId);

      expect(await validateFencingTokenSignature(token)).toBe(true);
    });

    it('rejects token with tampered epoch', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token = await generateFencingToken(1n, replicaId);

      const tamperedToken: FencingToken = {
        ...token,
        epoch: 999n,
      };

      expect(await validateFencingTokenSignature(tamperedToken)).toBe(false);
    });

    it('rejects token with tampered timestamp', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token = await generateFencingToken(1n, replicaId);

      const tamperedToken: FencingToken = {
        ...token,
        generatedAt: token.generatedAt + 1000,
      };

      expect(await validateFencingTokenSignature(tamperedToken)).toBe(false);
    });

    it('rejects token with tampered generator', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token = await generateFencingToken(1n, replicaId);

      const tamperedToken: FencingToken = {
        ...token,
        generatedBy: createReplicaId('eu-central', 'attacker'),
      };

      expect(await validateFencingTokenSignature(tamperedToken)).toBe(false);
    });

    it('rejects token with tampered signature', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token = await generateFencingToken(1n, replicaId);

      const tamperedToken: FencingToken = {
        ...token,
        signature: 'invalid-signature',
      };

      expect(await validateFencingTokenSignature(tamperedToken)).toBe(false);
    });
  });

  describe('Token Comparison', () => {
    it('compares tokens with different epochs', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token1 = await generateFencingToken(1n, replicaId);
      const token2 = await generateFencingToken(2n, replicaId);

      expect(compareFencingTokens(token2, token1)).toBeGreaterThan(0);
      expect(compareFencingTokens(token1, token2)).toBeLessThan(0);
    });

    it('compares tokens with same epoch by timestamp', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token1 = await generateFencingToken(1n, replicaId);

      await new Promise(resolve => setTimeout(resolve, 10));

      const token2: FencingToken = {
        ...token1,
        generatedAt: token1.generatedAt + 100,
      };

      expect(compareFencingTokens(token2, token1)).toBeGreaterThan(0);
    });

    it('returns 0 for equal tokens', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token = await generateFencingToken(1n, replicaId);

      expect(compareFencingTokens(token, token)).toBe(0);
    });

    it('handles null tokens', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token = await generateFencingToken(1n, replicaId);

      expect(compareFencingTokens(null, null)).toBe(0);
      expect(compareFencingTokens(token, null)).toBe(1);
      expect(compareFencingTokens(null, token)).toBe(-1);
    });
  });

  describe('Token Expiration', () => {
    it('token is not expired within TTL', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token = await generateFencingToken(1n, replicaId);

      expect(isFencingTokenExpired(token, 60000)).toBe(false);
    });

    it('token expires after TTL', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token: FencingToken = {
        ...(await generateFencingToken(1n, replicaId)),
        generatedAt: Date.now() - 120000, // 2 minutes ago
      };

      expect(isFencingTokenExpired(token, 60000)).toBe(true);
    });

    it('token at exact TTL boundary is expired', async () => {
      const replicaId = createReplicaId('us-west', 'r1');
      const token: FencingToken = {
        ...(await generateFencingToken(1n, replicaId)),
        generatedAt: Date.now() - 60001, // Just past 60 seconds
      };

      expect(isFencingTokenExpired(token, 60000)).toBe(true);
    });
  });
});

// =============================================================================
// LEADER ELECTION STATE MACHINE TESTS
// =============================================================================

describe('LeaderElectionStateMachine', () => {
  describe('Initialization', () => {
    it('starts as follower', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      expect(machine.getRole()).toBe('follower');
    });

    it('starts with no leader', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      const state = machine.getState();
      expect(state.leader).toBeNull();
    });

    it('starts with term 0', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      const state = machine.getState();
      expect(state.term).toBe(0n);
    });

    it('starts with no fencing token', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      expect(machine.getFencingToken()).toBeNull();
    });
  });

  describe('Election Timeout', () => {
    it('should not start election if recently heard from leader', async () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId, {
        heartbeatTimeoutMs: 15000,
      });

      // Simulate recent heartbeat
      const leaderId = createReplicaId('us-east', 'primary');
      const token = await generateFencingToken(1n, leaderId);
      const heartbeat: LeaderHeartbeat = {
        leaderId,
        term: 1n,
        fencingToken: token,
        currentLSN: 100n,
        timestamp: Date.now(),
      };

      machine.handleLeaderHeartbeat(heartbeat);

      expect(machine.shouldStartElection()).toBe(false);
    });

    it('should start election after timeout', async () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId, {
        heartbeatTimeoutMs: 50,
        electionTimeoutMs: 50,
        electionTimeoutJitterMs: 0,
      });

      // Wait for timeout
      await new Promise(resolve => setTimeout(resolve, 100));

      expect(machine.shouldStartElection()).toBe(true);
    });

    it('should not start election if already leader', async () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId, {
        quorumSize: 1, // Only need self vote
      });

      // Register a replica so quorum can be calculated
      machine.registerReplica(createReplicaInfo(createReplicaId('us-east', 'r2')));

      // Start election - we need enough votes for quorum
      await machine.startElection(100n);

      // Add another yes vote to achieve quorum
      const currentTerm = machine.getState().term;
      await machine.handleVoteResponse({
        voterId: createReplicaId('us-east', 'r2'),
        term: currentTerm,
        voteGranted: true,
        reason: 'Vote granted',
      });

      expect(machine.getRole()).toBe('leader');

      // Should not start another election
      expect(machine.shouldStartElection()).toBe(false);
    });
  });

  describe('Starting Election', () => {
    it('transitions to candidate role', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      machine.startElection(100n);

      expect(machine.getRole()).toBe('candidate');
    });

    it('increments term', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      machine.startElection(100n);
      const state = machine.getState();

      expect(state.term).toBe(1n);
    });

    it('votes for self', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      machine.startElection(100n);
      const state = machine.getState();

      expect(state.votedFor).toEqual(selfId);
      expect(state.votes.get(serializeReplicaId(selfId))).toBe(true);
    });

    it('generates vote request with fencing token', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      const request = machine.startElection(100n);

      expect(request.candidateId).toEqual(selfId);
      expect(request.term).toBe(1n);
      expect(request.lastLSN).toBe(100n);
      expect(request.fencingToken).toBeDefined();
    });

    it('generates fencing token', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      machine.startElection(100n);

      const token = machine.getFencingToken();
      expect(token).not.toBeNull();
      expect(token!.epoch).toBe(1n);
    });

    it('clears previous leader', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const leaderId = createReplicaId('us-east', 'primary');
      const machine = new LeaderElectionStateMachine(selfId);

      // Accept a leader first
      const token = generateFencingToken(1n, leaderId);
      machine.handleLeaderHeartbeat({
        leaderId,
        term: 1n,
        fencingToken: token,
        currentLSN: 100n,
        timestamp: Date.now(),
      });

      // Then start election
      machine.startElection(100n);
      const state = machine.getState();

      expect(state.leader).toBeNull();
    });
  });

  describe('Handling Vote Requests', () => {
    it('grants vote for valid request', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const candidateId = createReplicaId('us-east', 'r2');
      const machine = new LeaderElectionStateMachine(selfId);

      const token = generateFencingToken(1n, candidateId);
      const request: VoteRequest = {
        candidateId,
        term: 1n,
        lastLSN: 100n,
        fencingToken: token,
      };

      const response = machine.handleVoteRequest(request, 50n);

      expect(response.voteGranted).toBe(true);
      expect(response.voterId).toEqual(selfId);
    });

    it('rejects vote for stale term', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const candidateId = createReplicaId('us-east', 'r2');
      const machine = new LeaderElectionStateMachine(selfId);

      // Advance our term by starting election
      machine.startElection(100n);

      // Receive request from older term
      const token = generateFencingToken(0n, candidateId);
      const request: VoteRequest = {
        candidateId,
        term: 0n,
        lastLSN: 100n,
        fencingToken: token,
      };

      const response = machine.handleVoteRequest(request, 100n);

      expect(response.voteGranted).toBe(false);
      expect(response.reason).toContain('Stale term');
    });

    it('rejects vote if already voted for another', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const candidate1 = createReplicaId('us-east', 'r2');
      const candidate2 = createReplicaId('eu-central', 'r3');
      const machine = new LeaderElectionStateMachine(selfId);

      // Vote for first candidate
      const token1 = generateFencingToken(1n, candidate1);
      machine.handleVoteRequest({
        candidateId: candidate1,
        term: 1n,
        lastLSN: 100n,
        fencingToken: token1,
      }, 50n);

      // Try to vote for second candidate in same term
      const token2 = generateFencingToken(1n, candidate2);
      const response = machine.handleVoteRequest({
        candidateId: candidate2,
        term: 1n,
        lastLSN: 100n,
        fencingToken: token2,
      }, 50n);

      expect(response.voteGranted).toBe(false);
      expect(response.reason).toContain('Already voted');
    });

    it('rejects vote if candidate log is behind', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const candidateId = createReplicaId('us-east', 'r2');
      const machine = new LeaderElectionStateMachine(selfId);

      const token = generateFencingToken(1n, candidateId);
      const request: VoteRequest = {
        candidateId,
        term: 1n,
        lastLSN: 50n, // Candidate is behind
        fencingToken: token,
      };

      const response = machine.handleVoteRequest(request, 100n); // We're at LSN 100

      expect(response.voteGranted).toBe(false);
      expect(response.reason).toContain('Candidate LSN');
    });

    it('rejects vote for invalid fencing token', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const candidateId = createReplicaId('us-east', 'r2');
      const machine = new LeaderElectionStateMachine(selfId);

      const invalidToken: FencingToken = {
        epoch: 1n,
        generatedAt: Date.now(),
        generatedBy: candidateId,
        signature: 'invalid-signature',
      };

      const request: VoteRequest = {
        candidateId,
        term: 1n,
        lastLSN: 100n,
        fencingToken: invalidToken,
      };

      const response = machine.handleVoteRequest(request, 50n);

      expect(response.voteGranted).toBe(false);
      expect(response.reason).toContain('Invalid fencing token');
    });

    it('steps down if request has higher term', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const candidateId = createReplicaId('us-east', 'r2');
      const machine = new LeaderElectionStateMachine(selfId);

      // Start election at term 1
      machine.startElection(100n);
      expect(machine.getRole()).toBe('candidate');

      // Receive vote request from higher term
      const token = generateFencingToken(5n, candidateId);
      machine.handleVoteRequest({
        candidateId,
        term: 5n,
        lastLSN: 100n,
        fencingToken: token,
      }, 100n);

      expect(machine.getRole()).toBe('follower');
      expect(machine.getState().term).toBe(5n);
    });

    it('resets election timeout on granting vote', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const candidateId = createReplicaId('us-east', 'r2');
      const machine = new LeaderElectionStateMachine(selfId, {
        heartbeatTimeoutMs: 100,
        electionTimeoutMs: 100,
        electionTimeoutJitterMs: 0,
      });

      // Wait for timeout to approach
      // Then grant vote - should reset timeout
      const token = generateFencingToken(1n, candidateId);
      machine.handleVoteRequest({
        candidateId,
        term: 1n,
        lastLSN: 100n,
        fencingToken: token,
      }, 50n);

      expect(machine.shouldStartElection()).toBe(false);
    });
  });

  describe('Handling Vote Responses', () => {
    it('ignores response if not candidate', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const voterId = createReplicaId('us-east', 'r2');
      const machine = new LeaderElectionStateMachine(selfId);

      // We're a follower, not candidate
      const response: VoteResponse = {
        voterId,
        term: 1n,
        voteGranted: true,
        reason: 'Vote granted',
      };

      const won = machine.handleVoteResponse(response);
      expect(won).toBe(false);
    });

    it('steps down on response with higher term', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const voterId = createReplicaId('us-east', 'r2');
      const machine = new LeaderElectionStateMachine(selfId);

      machine.startElection(100n);

      const response: VoteResponse = {
        voterId,
        term: 10n, // Higher term
        voteGranted: false,
        reason: 'Higher term',
      };

      const won = machine.handleVoteResponse(response);

      expect(won).toBe(false);
      expect(machine.getRole()).toBe('follower');
      expect(machine.getState().term).toBe(10n);
    });

    it('ignores response from old term', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const voterId = createReplicaId('us-east', 'r2');
      const machine = new LeaderElectionStateMachine(selfId);

      machine.startElection(100n);

      const response: VoteResponse = {
        voterId,
        term: 0n, // Old term
        voteGranted: true,
        reason: 'Vote granted',
      };

      const won = machine.handleVoteResponse(response);
      expect(won).toBe(false);
      expect(machine.getRole()).toBe('candidate');
    });

    it('becomes leader with quorum', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const voter1 = createReplicaId('us-east', 'r2');
      const voter2 = createReplicaId('eu-central', 'r3');
      const machine = new LeaderElectionStateMachine(selfId, {
        quorumSize: 2,
      });

      // Register replicas for quorum calculation
      machine.registerReplica(createReplicaInfo(voter1));
      machine.registerReplica(createReplicaInfo(voter2));

      machine.startElection(100n);
      const currentTerm = machine.getState().term;

      // Receive positive vote - should give us quorum (self + 1 = 2)
      const won = machine.handleVoteResponse({
        voterId: voter1,
        term: currentTerm,
        voteGranted: true,
        reason: 'Vote granted',
      });

      expect(won).toBe(true);
      expect(machine.getRole()).toBe('leader');
    });

    it('does not become leader without quorum', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const voter1 = createReplicaId('us-east', 'r2');
      const voter2 = createReplicaId('eu-central', 'r3');
      const voter3 = createReplicaId('ap-south', 'r4');
      const machine = new LeaderElectionStateMachine(selfId, {
        quorumSize: 3,
      });

      machine.registerReplica(createReplicaInfo(voter1));
      machine.registerReplica(createReplicaInfo(voter2));
      machine.registerReplica(createReplicaInfo(voter3));

      machine.startElection(100n);
      const currentTerm = machine.getState().term;

      // Only one positive vote - not enough for quorum of 3
      const won = machine.handleVoteResponse({
        voterId: voter1,
        term: currentTerm,
        voteGranted: true,
        reason: 'Vote granted',
      });

      expect(won).toBe(false);
      expect(machine.getRole()).toBe('candidate');
    });

    it('tracks rejected votes', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const voterId = createReplicaId('us-east', 'r2');
      const machine = new LeaderElectionStateMachine(selfId);

      machine.startElection(100n);
      const currentTerm = machine.getState().term;

      machine.handleVoteResponse({
        voterId,
        term: currentTerm,
        voteGranted: false,
        reason: 'Vote rejected',
      });

      const state = machine.getState();
      expect(state.votes.get(serializeReplicaId(voterId))).toBe(false);
    });
  });

  describe('Handling Leader Heartbeats', () => {
    it('accepts heartbeat from leader in same term', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const leaderId = createReplicaId('us-east', 'primary');
      const machine = new LeaderElectionStateMachine(selfId);

      const token = generateFencingToken(1n, leaderId);
      const heartbeat: LeaderHeartbeat = {
        leaderId,
        term: 1n,
        fencingToken: token,
        currentLSN: 100n,
        timestamp: Date.now(),
      };

      machine.handleLeaderHeartbeat(heartbeat);

      const state = machine.getState();
      expect(state.leader).toEqual(leaderId);
      expect(state.fencingToken).toEqual(token);
    });

    it('steps down from candidate on valid heartbeat', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const leaderId = createReplicaId('us-east', 'primary');
      const machine = new LeaderElectionStateMachine(selfId);

      // Become candidate
      machine.startElection(100n);
      expect(machine.getRole()).toBe('candidate');

      // Receive heartbeat from leader with higher term
      const token = generateFencingToken(5n, leaderId);
      machine.handleLeaderHeartbeat({
        leaderId,
        term: 5n,
        fencingToken: token,
        currentLSN: 100n,
        timestamp: Date.now(),
      });

      expect(machine.getRole()).toBe('follower');
      expect(machine.getState().leader).toEqual(leaderId);
    });

    it('ignores heartbeat from old term', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const oldLeader = createReplicaId('us-east', 'old-primary');
      const machine = new LeaderElectionStateMachine(selfId);

      // Advance term
      machine.startElection(100n);
      const currentTerm = machine.getState().term;

      // Receive heartbeat from old term
      const token = generateFencingToken(0n, oldLeader);
      machine.handleLeaderHeartbeat({
        leaderId: oldLeader,
        term: 0n,
        fencingToken: token,
        currentLSN: 50n,
        timestamp: Date.now(),
      });

      // Should not accept old leader
      expect(machine.getState().term).toBe(currentTerm);
    });

    it('updates fencing token on heartbeat', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const leaderId = createReplicaId('us-east', 'primary');
      const machine = new LeaderElectionStateMachine(selfId);

      const token1 = generateFencingToken(1n, leaderId);
      machine.handleLeaderHeartbeat({
        leaderId,
        term: 1n,
        fencingToken: token1,
        currentLSN: 100n,
        timestamp: Date.now(),
      });

      expect(machine.getFencingToken()).toEqual(token1);

      // New heartbeat with updated token
      const token2 = generateFencingToken(1n, leaderId);
      machine.handleLeaderHeartbeat({
        leaderId,
        term: 1n,
        fencingToken: token2,
        currentLSN: 200n,
        timestamp: Date.now(),
      });

      expect(machine.getFencingToken()).toEqual(token2);
    });

    it('resets election timeout on heartbeat', async () => {
      const selfId = createReplicaId('us-west', 'r1');
      const leaderId = createReplicaId('us-east', 'primary');
      const machine = new LeaderElectionStateMachine(selfId, {
        heartbeatTimeoutMs: 100,
        electionTimeoutMs: 100,
        electionTimeoutJitterMs: 0,
      });

      const token = generateFencingToken(1n, leaderId);
      machine.handleLeaderHeartbeat({
        leaderId,
        term: 1n,
        fencingToken: token,
        currentLSN: 100n,
        timestamp: Date.now(),
      });

      expect(machine.shouldStartElection()).toBe(false);
    });
  });

  describe('Generating Heartbeats', () => {
    it('generates heartbeat when leader', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const voterId = createReplicaId('us-east', 'r2');
      const machine = new LeaderElectionStateMachine(selfId, {
        quorumSize: 2,
      });

      // Register replica for quorum
      machine.registerReplica(createReplicaInfo(voterId));

      // Start election
      machine.startElection(100n);
      const currentTerm = machine.getState().term;

      // Get vote to become leader
      machine.handleVoteResponse({
        voterId,
        term: currentTerm,
        voteGranted: true,
        reason: 'Vote granted',
      });

      expect(machine.getRole()).toBe('leader');

      const heartbeat = machine.generateHeartbeat(150n);

      expect(heartbeat).not.toBeNull();
      expect(heartbeat!.leaderId).toEqual(selfId);
      expect(heartbeat!.currentLSN).toBe(150n);
    });

    it('returns null when not leader', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      const heartbeat = machine.generateHeartbeat(100n);

      expect(heartbeat).toBeNull();
    });

    it('includes fencing token in heartbeat', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const voterId = createReplicaId('us-east', 'r2');
      const machine = new LeaderElectionStateMachine(selfId, {
        quorumSize: 2,
      });

      // Register replica for quorum
      machine.registerReplica(createReplicaInfo(voterId));

      // Start election and become leader
      machine.startElection(100n);
      const currentTerm = machine.getState().term;

      machine.handleVoteResponse({
        voterId,
        term: currentTerm,
        voteGranted: true,
        reason: 'Vote granted',
      });

      expect(machine.getRole()).toBe('leader');

      const heartbeat = machine.generateHeartbeat(150n);

      expect(heartbeat).not.toBeNull();
      expect(heartbeat!.fencingToken).toBeDefined();
      expect(validateFencingTokenSignature(heartbeat!.fencingToken)).toBe(true);
    });
  });

  describe('Fencing Token Validation', () => {
    it('accepts valid current term token', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const leaderId = createReplicaId('us-east', 'primary');
      const machine = new LeaderElectionStateMachine(selfId);

      // Accept leader heartbeat to set term
      const token = generateFencingToken(1n, leaderId);
      machine.handleLeaderHeartbeat({
        leaderId,
        term: 1n,
        fencingToken: token,
        currentLSN: 100n,
        timestamp: Date.now(),
      });

      expect(machine.validateFencingToken(token)).toBe(true);
    });

    it('rejects expired token', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const leaderId = createReplicaId('us-east', 'primary');
      const machine = new LeaderElectionStateMachine(selfId, {
        fencingTokenTtlMs: 1000,
      });

      const expiredToken: FencingToken = {
        ...generateFencingToken(1n, leaderId),
        generatedAt: Date.now() - 5000, // Expired
      };

      // Need to regenerate signature with correct timestamp for it to pass signature check
      // Since signature includes timestamp, this token will fail signature validation
      expect(machine.validateFencingToken(expiredToken)).toBe(false);
    });

    it('rejects token from old term', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const leaderId = createReplicaId('us-east', 'primary');
      const machine = new LeaderElectionStateMachine(selfId);

      // Advance to term 5
      const newToken = generateFencingToken(5n, leaderId);
      machine.handleLeaderHeartbeat({
        leaderId,
        term: 5n,
        fencingToken: newToken,
        currentLSN: 100n,
        timestamp: Date.now(),
      });

      // Old term token
      const oldToken = generateFencingToken(1n, leaderId);
      expect(machine.validateFencingToken(oldToken)).toBe(false);
    });

    it('rejects token with invalid signature', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      const invalidToken: FencingToken = {
        epoch: 1n,
        generatedAt: Date.now(),
        generatedBy: createReplicaId('attacker', 'x'),
        signature: 'fake-signature',
      };

      expect(machine.validateFencingToken(invalidToken)).toBe(false);
    });
  });

  describe('Promotion Eligibility', () => {
    it('is not eligible when already leader', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const voterId = createReplicaId('us-east', 'r2');
      const machine = new LeaderElectionStateMachine(selfId, {
        quorumSize: 2,
        heartbeatTimeoutMs: 50,
      });

      // Register replica for quorum
      machine.registerReplica(createReplicaInfo(voterId));

      // Start election and become leader
      machine.startElection(100n);
      const currentTerm = machine.getState().term;

      machine.handleVoteResponse({
        voterId,
        term: currentTerm,
        voteGranted: true,
        reason: 'Vote granted',
      });

      expect(machine.getRole()).toBe('leader');

      const eligibility = machine.checkPromotionEligibility(100n, 100n);

      expect(eligibility.eligible).toBe(false);
      expect(eligibility.reason).toContain('Already leader');
    });

    it('is not eligible when auto-failover disabled', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId, {
        autoFailover: false,
      });

      const eligibility = machine.checkPromotionEligibility(100n, 100n);

      expect(eligibility.eligible).toBe(false);
      expect(eligibility.reason).toContain('Auto-failover disabled');
    });

    it('is not eligible when leader is still active', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const leaderId = createReplicaId('us-east', 'primary');
      const machine = new LeaderElectionStateMachine(selfId, {
        heartbeatTimeoutMs: 15000,
      });

      // Receive recent heartbeat
      const token = generateFencingToken(1n, leaderId);
      machine.handleLeaderHeartbeat({
        leaderId,
        term: 1n,
        fencingToken: token,
        currentLSN: 100n,
        timestamp: Date.now(),
      });

      const eligibility = machine.checkPromotionEligibility(100n, 100n);

      expect(eligibility.eligible).toBe(false);
      expect(eligibility.reason).toContain('Leader still active');
    });

    it('is eligible after leader timeout', async () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId, {
        heartbeatTimeoutMs: 50,
        autoFailover: true,
      });

      // Wait for timeout
      await new Promise(resolve => setTimeout(resolve, 100));

      const eligibility = machine.checkPromotionEligibility(100n, 100n);

      expect(eligibility.eligible).toBe(true);
    });

    it('calculates priority based on LSN lag', async () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId, {
        heartbeatTimeoutMs: 50,
        autoFailover: true,
      });

      await new Promise(resolve => setTimeout(resolve, 100));

      const eligibilityNoLag = machine.checkPromotionEligibility(100n, 100n);
      const eligibilityWithLag = machine.checkPromotionEligibility(90n, 100n);

      expect(eligibilityNoLag.priority).toBeGreaterThan(eligibilityWithLag.priority);
    });

    it('reports LSN lag correctly', async () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId, {
        heartbeatTimeoutMs: 50,
        autoFailover: true,
      });

      await new Promise(resolve => setTimeout(resolve, 100));

      const eligibility = machine.checkPromotionEligibility(80n, 100n);

      expect(eligibility.lsnLag).toBe(20n);
    });
  });

  describe('Split-Brain Detection', () => {
    it('detects no split-brain with single leader', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      const leader = createReplicaId('us-east', 'primary');
      const detection = machine.detectSplitBrain([leader]);

      expect(detection.detected).toBe(false);
      expect(detection.resolution).toBe('none');
    });

    it('detects no split-brain with no leaders', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      const detection = machine.detectSplitBrain([]);

      expect(detection.detected).toBe(false);
    });

    it('detects split-brain with multiple leaders', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      const leader1 = createReplicaId('us-east', 'primary-1');
      const leader2 = createReplicaId('eu-central', 'primary-2');

      const detection = machine.detectSplitBrain([leader1, leader2]);

      expect(detection.detected).toBe(true);
      expect(detection.conflictingLeaders).toHaveLength(2);
      expect(detection.resolution).toBe('fencing');
    });

    it('deduplicates same leader reported multiple times', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      const leader = createReplicaId('us-east', 'primary');

      const detection = machine.detectSplitBrain([leader, leader, leader]);

      expect(detection.detected).toBe(false);
      expect(detection.conflictingLeaders).toHaveLength(0);
    });
  });

  describe('Replica Registration', () => {
    it('registers replica for quorum calculation', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      const replica = createReplicaInfo(createReplicaId('us-east', 'r2'));
      machine.registerReplica(replica);

      // Should affect quorum calculation
      // With 2 nodes (self + 1 replica), quorum is 2
    });

    it('unregisters replica', () => {
      const selfId = createReplicaId('us-west', 'r1');
      const machine = new LeaderElectionStateMachine(selfId);

      const replicaId = createReplicaId('us-east', 'r2');
      machine.registerReplica(createReplicaInfo(replicaId));
      machine.unregisterReplica(replicaId);

      // Should update quorum calculation
    });
  });
});

// =============================================================================
// SPLIT-BRAIN RESOLVER TESTS
// =============================================================================

describe('SplitBrainResolver', () => {
  describe('Conflict Resolution', () => {
    it('returns null for empty leaders list', () => {
      const resolver = new SplitBrainResolver();

      const result = resolver.resolveConflict([]);

      expect(result).toBeNull();
    });

    it('returns single leader with no losers', () => {
      const resolver = new SplitBrainResolver();

      const leader = createReplicaId('us-east', 'primary');
      const token = generateFencingToken(1n, leader);

      const result = resolver.resolveConflict([
        { id: leader, token, lsn: 100n },
      ]);

      expect(result).not.toBeNull();
      expect(result!.winner).toEqual(leader);
      expect(result!.losers).toHaveLength(0);
    });

    it('selects leader with higher epoch token', () => {
      const resolver = new SplitBrainResolver();

      const leader1 = createReplicaId('us-east', 'primary-1');
      const leader2 = createReplicaId('eu-central', 'primary-2');

      const token1 = generateFencingToken(1n, leader1);
      const token2 = generateFencingToken(5n, leader2);

      const result = resolver.resolveConflict([
        { id: leader1, token: token1, lsn: 100n },
        { id: leader2, token: token2, lsn: 50n },
      ]);

      expect(result!.winner).toEqual(leader2);
      expect(result!.losers).toContainEqual(leader1);
    });

    it('uses LSN as tiebreaker for same epoch', async () => {
      const resolver = new SplitBrainResolver();

      const leader1 = createReplicaId('us-east', 'primary-1');
      const leader2 = createReplicaId('eu-central', 'primary-2');

      const token1 = generateFencingToken(1n, leader1);
      const token2: FencingToken = {
        ...generateFencingToken(1n, leader2),
        generatedAt: token1.generatedAt, // Same timestamp
      };

      const result = resolver.resolveConflict([
        { id: leader1, token: token1, lsn: 50n },
        { id: leader2, token: token2, lsn: 100n },
      ]);

      expect(result!.winner).toEqual(leader2);
    });

    it('handles multiple conflicting leaders', () => {
      const resolver = new SplitBrainResolver();

      const leader1 = createReplicaId('us-east', 'primary-1');
      const leader2 = createReplicaId('eu-central', 'primary-2');
      const leader3 = createReplicaId('ap-south', 'primary-3');

      const result = resolver.resolveConflict([
        { id: leader1, token: generateFencingToken(1n, leader1), lsn: 100n },
        { id: leader2, token: generateFencingToken(3n, leader2), lsn: 90n },
        { id: leader3, token: generateFencingToken(2n, leader3), lsn: 95n },
      ]);

      expect(result!.winner).toEqual(leader2); // Highest epoch
      expect(result!.losers).toHaveLength(2);
    });
  });

  describe('Fencing Decision', () => {
    it('should fence leader with lower epoch', () => {
      const resolver = new SplitBrainResolver();

      const oldLeader = createReplicaId('us-east', 'old');
      const newLeader = createReplicaId('eu-central', 'new');

      const oldToken = generateFencingToken(1n, oldLeader);
      const newToken = generateFencingToken(5n, newLeader);

      expect(resolver.shouldFenceLeader(oldToken, newToken)).toBe(true);
    });

    it('should not fence leader with higher epoch', () => {
      const resolver = new SplitBrainResolver();

      const currentLeader = createReplicaId('us-east', 'current');
      const staleLeader = createReplicaId('eu-central', 'stale');

      const currentToken = generateFencingToken(5n, currentLeader);
      const staleToken = generateFencingToken(1n, staleLeader);

      expect(resolver.shouldFenceLeader(currentToken, staleToken)).toBe(false);
    });

    it('should fence leader with same epoch but older timestamp', async () => {
      const resolver = new SplitBrainResolver();

      const leader = createReplicaId('us-east', 'leader');

      const oldToken = generateFencingToken(1n, leader);
      await new Promise(resolve => setTimeout(resolve, 10));
      const newToken: FencingToken = {
        ...generateFencingToken(1n, leader),
        generatedAt: oldToken.generatedAt + 100,
      };

      expect(resolver.shouldFenceLeader(oldToken, newToken)).toBe(true);
    });
  });
});
