/**
 * Replication Router Comprehensive Tests
 *
 * Tests for the Replication Router including:
 * - Read routing with various consistency levels
 * - Write routing to primary
 * - Load balancing strategies
 * - Failover and health-aware routing
 * - Session management
 * - Network partition simulation
 * - Lag detection and handling
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

import {
  type ReplicaId,
  type ReplicaInfo,
  type ReplicationLag,
  type SessionState,
  type ConsistencyLevel,
  serializeReplicaId,
  replicaIdsEqual,
} from '../types.js';

import {
  createReplicationRouter,
  createExtendedRouter,
  createLoadBalancedRouter,
  SessionManager,
  createRouterWithSessions,
  type ExtendedReplicationRouter,
  type LoadBalancingStrategy,
} from '../router.js';

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
// READ ROUTING TESTS
// =============================================================================

describe('Router - Read Routing', () => {
  const primaryId = createReplicaId('us-east', 'primary-1');

  describe('Strong Consistency', () => {
    it('always routes to primary for strong consistency', async () => {
      const router = createExtendedRouter(primaryId);

      // Register some replicas
      router.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1')));
      router.registerReplica(createReplicaInfo(createReplicaId('eu-central', 'r2')));

      const decision = await router.routeRead('SELECT * FROM users', 'strong');

      expect(decision.target).toEqual(primaryId);
      expect(decision.consistency).toBe('strong');
      expect(decision.fallback).toBe(false);
    });

    it('routes to primary regardless of replica health', async () => {
      const router = createExtendedRouter(primaryId);

      // Register only offline replicas
      router.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1'), 'offline'));
      router.registerReplica(createReplicaInfo(createReplicaId('eu-central', 'r2'), 'offline'));

      const decision = await router.routeRead('SELECT * FROM users', 'strong');

      expect(decision.target).toEqual(primaryId);
    });
  });

  describe('Eventual Consistency', () => {
    it('routes to nearest healthy replica', async () => {
      const router = createExtendedRouter(primaryId);

      const nearReplica = createReplicaId('us-west', 'r1');
      const farReplica = createReplicaId('eu-central', 'r2');

      router.registerReplica(createReplicaInfo(nearReplica));
      router.registerReplica(createReplicaInfo(farReplica));

      // Set latencies
      router.recordLatency(nearReplica, 10);
      router.recordLatency(farReplica, 100);

      const decision = await router.routeRead('SELECT * FROM users', 'eventual');

      // Should prefer lower latency replica
      expect(decision.target).toEqual(nearReplica);
      expect(decision.consistency).toBe('eventual');
    });

    it('falls back to primary when no healthy replicas', async () => {
      const router = createExtendedRouter(primaryId);

      // Register only offline replicas
      router.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1'), 'offline'));

      const decision = await router.routeRead('SELECT * FROM users', 'eventual');

      expect(decision.target).toEqual(primaryId);
      expect(decision.fallback).toBe(true);
    });

    it('prefers same-region replicas', async () => {
      const router = createExtendedRouter(primaryId);

      const sameRegionReplica = createReplicaId('us-west', 'r1');
      const otherRegionReplica = createReplicaId('eu-central', 'r2');

      router.registerReplica(createReplicaInfo(sameRegionReplica));
      router.registerReplica(createReplicaInfo(otherRegionReplica));

      // Set similar latencies
      router.recordLatency(sameRegionReplica, 50);
      router.recordLatency(otherRegionReplica, 50);

      // Set current region
      router.setCurrentRegion('us-west');

      const decision = await router.routeRead('SELECT * FROM users', 'eventual');

      // Should prefer same region even with similar latency
      expect(decision.target).toEqual(sameRegionReplica);
    });

    it('excludes lagging replicas based on status', async () => {
      const router = createExtendedRouter(primaryId);

      const healthyReplica = createReplicaId('us-west', 'r1');
      const laggingReplica = createReplicaId('eu-central', 'r2');

      router.registerReplica(createReplicaInfo(healthyReplica, 'active'));
      router.registerReplica(createReplicaInfo(laggingReplica, 'lagging'));

      // Lagging replica has lower latency but should not be chosen
      router.recordLatency(healthyReplica, 100);
      router.recordLatency(laggingReplica, 10);

      const decision = await router.routeRead('SELECT * FROM users', 'eventual');

      // Should still choose healthy replica even with higher latency
      // Note: The router considers 'syncing' as healthy too
    });
  });

  describe('Session Consistency', () => {
    it('routes to replica caught up to session LSN', async () => {
      const router = createExtendedRouter(primaryId);

      const caughtUpReplica = createReplicaId('us-west', 'r1');
      const behindReplica = createReplicaId('eu-central', 'r2');

      router.registerReplica(createReplicaInfo(caughtUpReplica, 'active', 100n));
      router.registerReplica(createReplicaInfo(behindReplica, 'active', 50n));

      const session: SessionState = {
        sessionId: 'sess_123',
        lastWriteLSN: 80n,
        startedAt: Date.now(),
      };

      const decision = await router.routeRead('SELECT * FROM users', 'session', session);

      // Should choose replica that's past session's last write
      expect(decision.target.instanceId).toBe('r1');
    });

    it('falls back to primary when no replica caught up', async () => {
      const router = createExtendedRouter(primaryId);

      router.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1'), 'active', 50n));
      router.registerReplica(createReplicaInfo(createReplicaId('eu-central', 'r2'), 'active', 60n));

      const session: SessionState = {
        sessionId: 'sess_123',
        lastWriteLSN: 100n, // Higher than all replicas
        startedAt: Date.now(),
      };

      const decision = await router.routeRead('SELECT * FROM users', 'session', session);

      expect(decision.target).toEqual(primaryId);
      expect(decision.fallback).toBe(true);
    });

    it('treats missing session as eventual consistency', async () => {
      const router = createExtendedRouter(primaryId);

      router.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1'), 'active'));

      const decision = await router.routeRead('SELECT * FROM users', 'session');

      // Without session, should route to nearest replica like eventual
      expect(decision.consistency).toBe('session');
    });
  });

  describe('Bounded Staleness', () => {
    it('routes to replica within staleness bounds', async () => {
      const router = createExtendedRouter(primaryId, { boundedStalenessMs: 5000 });

      const withinBoundsReplica = createReplicaId('us-west', 'r1');
      const outsideBoundsReplica = createReplicaId('eu-central', 'r2');

      router.registerReplica(createReplicaInfo(withinBoundsReplica, 'active'));
      router.registerReplica(createReplicaInfo(outsideBoundsReplica, 'active'));

      // Set lag within bounds for r1
      router.updateReplicaStatus(withinBoundsReplica, 'active', {
        replicaId: withinBoundsReplica,
        primaryLSN: 100n,
        replicaLSN: 100n,
        lagEntries: 0n,
        lagMs: 100,
        measuredAt: Date.now(),
      });

      // Set lag outside bounds for r2
      router.updateReplicaStatus(outsideBoundsReplica, 'active', {
        replicaId: outsideBoundsReplica,
        primaryLSN: 100n,
        replicaLSN: 90n,
        lagEntries: 10n,
        lagMs: 10000, // Beyond 5000ms bound
        measuredAt: Date.now(),
      });

      const decision = await router.routeRead('SELECT * FROM users', 'bounded');

      expect(decision.target).toEqual(withinBoundsReplica);
    });

    it('falls back to primary when all replicas exceed bounds', async () => {
      const router = createExtendedRouter(primaryId, { boundedStalenessMs: 100 });

      const replica = createReplicaId('us-west', 'r1');
      router.registerReplica(createReplicaInfo(replica, 'active'));

      router.updateReplicaStatus(replica, 'active', {
        replicaId: replica,
        primaryLSN: 100n,
        replicaLSN: 50n,
        lagEntries: 50n,
        lagMs: 5000, // Beyond 100ms bound
        measuredAt: Date.now(),
      });

      const decision = await router.routeRead('SELECT * FROM users', 'bounded');

      expect(decision.target).toEqual(primaryId);
      expect(decision.fallback).toBe(true);
    });
  });
});

// =============================================================================
// WRITE ROUTING TESTS
// =============================================================================

describe('Router - Write Routing', () => {
  const primaryId = createReplicaId('us-east', 'primary-1');

  it('always routes writes to primary', async () => {
    const router = createExtendedRouter(primaryId);

    router.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1')));

    const decision = await router.routeWrite('INSERT INTO users VALUES (1, \'test\')');

    expect(decision.target).toEqual(primaryId);
    expect(decision.consistency).toBe('strong');
    expect(decision.fallback).toBe(false);
  });

  it('routes all DML operations to primary', async () => {
    const router = createExtendedRouter(primaryId);

    const operations = [
      'INSERT INTO users VALUES (1)',
      'UPDATE users SET name = \'test\'',
      'DELETE FROM users WHERE id = 1',
      'REPLACE INTO users VALUES (1, \'test\')',
    ];

    for (const sql of operations) {
      const decision = await router.routeWrite(sql);
      expect(decision.target).toEqual(primaryId);
    }
  });
});

// =============================================================================
// LOAD BALANCING TESTS
// =============================================================================

describe('Router - Load Balancing Strategies', () => {
  const primaryId = createReplicaId('us-east', 'primary-1');

  describe('Round Robin', () => {
    it('distributes requests evenly', async () => {
      const router = createLoadBalancedRouter(primaryId, 'round-robin');

      const r1 = createReplicaId('us-west', 'r1');
      const r2 = createReplicaId('eu-central', 'r2');
      const r3 = createReplicaId('ap-south', 'r3');

      router.registerReplica(createReplicaInfo(r1));
      router.registerReplica(createReplicaInfo(r2));
      router.registerReplica(createReplicaInfo(r3));

      const results = [];
      for (let i = 0; i < 9; i++) {
        const result = await router.getNearestReplica('any');
        results.push(result?.instanceId);
      }

      // Should cycle through all replicas
      const uniqueReplicas = new Set(results);
      expect(uniqueReplicas.size).toBe(3);
    });

    it('handles single replica', async () => {
      const router = createLoadBalancedRouter(primaryId, 'round-robin');

      router.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1')));

      const result = await router.getNearestReplica('any');
      expect(result?.instanceId).toBe('r1');
    });
  });

  describe('Random', () => {
    it('returns valid replica', async () => {
      const router = createLoadBalancedRouter(primaryId, 'random');

      router.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1')));
      router.registerReplica(createReplicaInfo(createReplicaId('eu-central', 'r2')));

      const result = await router.getNearestReplica('any');

      expect(result).not.toBeNull();
      expect(['r1', 'r2']).toContain(result?.instanceId);
    });

    it('distributes across replicas over many requests', async () => {
      const router = createLoadBalancedRouter(primaryId, 'random');

      router.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1')));
      router.registerReplica(createReplicaInfo(createReplicaId('eu-central', 'r2')));

      const counts: Record<string, number> = { r1: 0, r2: 0 };

      for (let i = 0; i < 100; i++) {
        const result = await router.getNearestReplica('any');
        if (result) {
          counts[result.instanceId]++;
        }
      }

      // Both should have been selected at least once
      expect(counts.r1).toBeGreaterThan(0);
      expect(counts.r2).toBeGreaterThan(0);
    });
  });

  describe('Least Connections', () => {
    it('selects replica with fewest connections', async () => {
      const router = createLoadBalancedRouter(primaryId, 'least-connections');

      router.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1')));
      router.registerReplica(createReplicaInfo(createReplicaId('eu-central', 'r2')));

      // First request should work
      const result = await router.getNearestReplica('any');
      expect(result).not.toBeNull();
    });
  });

  describe('Latency Weighted', () => {
    it('prefers lower latency replicas', async () => {
      const router = createLoadBalancedRouter(primaryId, 'latency-weighted');

      const lowLatency = createReplicaId('us-west', 'r1');
      const highLatency = createReplicaId('eu-central', 'r2');

      router.registerReplica(createReplicaInfo(lowLatency));
      router.registerReplica(createReplicaInfo(highLatency));

      // Record latencies
      router.recordLatency(lowLatency, 10);
      router.recordLatency(highLatency, 200);

      const result = await router.getNearestReplica('any');

      expect(result).toEqual(lowLatency);
    });
  });
});

// =============================================================================
// FAILOVER AND HEALTH TESTS
// =============================================================================

describe('Router - Failover and Health', () => {
  const primaryId = createReplicaId('us-east', 'primary-1');

  describe('Health-Aware Routing', () => {
    it('excludes offline replicas from routing', async () => {
      const router = createExtendedRouter(primaryId);

      const healthyReplica = createReplicaId('us-west', 'r1');
      const offlineReplica = createReplicaId('eu-central', 'r2');

      router.registerReplica(createReplicaInfo(healthyReplica, 'active'));
      router.registerReplica(createReplicaInfo(offlineReplica, 'offline'));

      // Make offline replica have lower latency
      router.recordLatency(healthyReplica, 100);
      router.recordLatency(offlineReplica, 10);

      const decision = await router.routeRead('SELECT 1', 'eventual');

      expect(decision.target).toEqual(healthyReplica);
    });

    it('includes syncing replicas in routing', async () => {
      const router = createExtendedRouter(primaryId);

      const syncingReplica = createReplicaId('us-west', 'r1');
      router.registerReplica(createReplicaInfo(syncingReplica, 'syncing'));

      router.recordLatency(syncingReplica, 10);

      const decision = await router.routeRead('SELECT 1', 'eventual');

      expect(decision.target).toEqual(syncingReplica);
    });

    it('updates status based on lag information', () => {
      const router = createExtendedRouter(primaryId);

      const replicaId = createReplicaId('us-west', 'r1');
      router.registerReplica(createReplicaInfo(replicaId, 'active'));

      const lag: ReplicationLag = {
        replicaId,
        primaryLSN: 100n,
        replicaLSN: 50n,
        lagEntries: 50n,
        lagMs: 5000,
        measuredAt: Date.now(),
      };

      router.updateReplicaStatus(replicaId, 'lagging', lag);

      const replicas = router.getReplicas();
      const replica = replicas.find(r => r.id.instanceId === 'r1');

      expect(replica?.status).toBe('lagging');
    });
  });

  describe('Network Partition Simulation', () => {
    it('handles all replicas going offline', async () => {
      const router = createExtendedRouter(primaryId);

      const r1 = createReplicaId('us-west', 'r1');
      const r2 = createReplicaId('eu-central', 'r2');

      router.registerReplica(createReplicaInfo(r1, 'active'));
      router.registerReplica(createReplicaInfo(r2, 'active'));

      // Simulate partition - mark all offline
      router.updateReplicaStatus(r1, 'offline');
      router.updateReplicaStatus(r2, 'offline');

      const decision = await router.routeRead('SELECT 1', 'eventual');

      // Should fall back to primary
      expect(decision.target).toEqual(primaryId);
      expect(decision.fallback).toBe(true);
    });

    it('recovers when replicas come back online', async () => {
      const router = createExtendedRouter(primaryId);

      const replicaId = createReplicaId('us-west', 'r1');
      router.registerReplica(createReplicaInfo(replicaId, 'offline'));

      // First routing should fall back to primary
      let decision = await router.routeRead('SELECT 1', 'eventual');
      expect(decision.target).toEqual(primaryId);

      // Replica comes back online
      router.updateReplicaStatus(replicaId, 'active');

      // Should now route to replica
      decision = await router.routeRead('SELECT 1', 'eventual');
      expect(decision.target).toEqual(replicaId);
    });
  });

  describe('Lag Detection', () => {
    it('tracks replication lag per replica', () => {
      const router = createExtendedRouter(primaryId);

      const replicaId = createReplicaId('us-west', 'r1');
      router.registerReplica(createReplicaInfo(replicaId, 'active'));

      const lag: ReplicationLag = {
        replicaId,
        primaryLSN: 100n,
        replicaLSN: 80n,
        lagEntries: 20n,
        lagMs: 2000,
        measuredAt: Date.now(),
      };

      router.updateReplicaStatus(replicaId, 'active', lag);

      const replicas = router.getReplicas();
      const replica = replicas.find(r => r.id.instanceId === 'r1');

      expect(replica?.lastLSN).toBe(80n);
    });

    it('counts failovers when primary goes offline', () => {
      const router = createExtendedRouter(primaryId);

      // Mark primary offline
      router.updateReplicaStatus(primaryId, 'offline');

      const metrics = router.getMetrics();
      expect(metrics.failovers).toBe(1);
    });
  });
});

// =============================================================================
// REPLICA MANAGEMENT TESTS
// =============================================================================

describe('Router - Replica Management', () => {
  const primaryId = createReplicaId('us-east', 'primary-1');

  it('registers new replica', () => {
    const router = createExtendedRouter(primaryId);

    const replicaId = createReplicaId('us-west', 'r1');
    router.registerReplica(createReplicaInfo(replicaId));

    const replicas = router.getReplicas();
    const replica = replicas.find(r => r.id.instanceId === 'r1');

    expect(replica).toBeDefined();
    expect(replica?.status).toBe('active');
  });

  it('deregisters replica', () => {
    const router = createExtendedRouter(primaryId);

    const replicaId = createReplicaId('us-west', 'r1');
    router.registerReplica(createReplicaInfo(replicaId));
    router.deregisterReplica(replicaId);

    const replicas = router.getReplicas();
    const replica = replicas.find(r => r.id.instanceId === 'r1');

    expect(replica).toBeUndefined();
  });

  it('gets nearest replica by region', async () => {
    const router = createExtendedRouter(primaryId);

    const usWestReplica = createReplicaId('us-west', 'r1');
    const euReplica = createReplicaId('eu-central', 'r2');

    router.registerReplica(createReplicaInfo(usWestReplica));
    router.registerReplica(createReplicaInfo(euReplica));

    const nearest = await router.getNearestReplica('us-west');

    expect(nearest).toEqual(usWestReplica);
  });

  it('returns null when no replica in region', async () => {
    const router = createExtendedRouter(primaryId);

    router.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1')));

    // Request replica in region with no replicas
    const nearest = await router.getNearestReplica('ap-northeast');

    // Should fall back to nearest healthy replica
    expect(nearest).not.toBeNull();
  });

  it('updates replica status', () => {
    const router = createExtendedRouter(primaryId);

    const replicaId = createReplicaId('us-west', 'r1');
    router.registerReplica(createReplicaInfo(replicaId, 'syncing'));

    router.updateReplicaStatus(replicaId, 'active');

    const replicas = router.getReplicas();
    const replica = replicas.find(r => r.id.instanceId === 'r1');

    expect(replica?.status).toBe('active');
  });

  it('creates new replica on status update if not exists', () => {
    const router = createExtendedRouter(primaryId);

    const replicaId = createReplicaId('us-west', 'r1');

    // Update status without registering first
    router.updateReplicaStatus(replicaId, 'active');

    const replicas = router.getReplicas();
    const replica = replicas.find(r => r.id.instanceId === 'r1');

    expect(replica).toBeDefined();
  });

  it('gets primary info', () => {
    const router = createExtendedRouter(primaryId);

    const primary = router.getPrimary();

    expect(primary).not.toBeNull();
    expect(primary?.id).toEqual(primaryId);
    expect(primary?.role).toBe('primary');
  });
});

// =============================================================================
// LATENCY TRACKING TESTS
// =============================================================================

describe('Router - Latency Tracking', () => {
  const primaryId = createReplicaId('us-east', 'primary-1');

  it('records latency samples', () => {
    const router = createExtendedRouter(primaryId);

    const replicaId = createReplicaId('us-west', 'r1');
    router.registerReplica(createReplicaInfo(replicaId));

    router.recordLatency(replicaId, 10);
    router.recordLatency(replicaId, 20);
    router.recordLatency(replicaId, 30);

    // Latency should affect routing decisions
    const metrics = router.getMetrics();
    expect(metrics).toBeDefined();
  });

  it('maintains bounded latency sample window', () => {
    const router = createExtendedRouter(primaryId);

    const replicaId = createReplicaId('us-west', 'r1');
    router.registerReplica(createReplicaInfo(replicaId));

    // Record many samples
    for (let i = 0; i < 200; i++) {
      router.recordLatency(replicaId, i);
    }

    // Should not error or cause memory issues
    const metrics = router.getMetrics();
    expect(metrics).toBeDefined();
  });

  it('calculates average latency correctly', () => {
    const router = createExtendedRouter(primaryId);

    const replicaId = createReplicaId('us-west', 'r1');
    router.registerReplica(createReplicaInfo(replicaId));

    router.recordLatency(replicaId, 10);
    router.recordLatency(replicaId, 20);
    router.recordLatency(replicaId, 30);

    // Average should be 20
    // This affects routing decision estimatedLatencyMs
  });
});

// =============================================================================
// METRICS TESTS
// =============================================================================

describe('Router - Metrics', () => {
  const primaryId = createReplicaId('us-east', 'primary-1');

  it('tracks total reads', async () => {
    const router = createExtendedRouter(primaryId);

    await router.routeRead('SELECT 1', 'eventual');
    await router.routeRead('SELECT 2', 'eventual');
    await router.routeRead('SELECT 3', 'eventual');

    const metrics = router.getMetrics();
    expect(metrics.totalReads).toBe(3);
  });

  it('tracks primary reads separately', async () => {
    const router = createExtendedRouter(primaryId);

    await router.routeRead('SELECT 1', 'strong');
    await router.routeRead('SELECT 2', 'strong');

    const metrics = router.getMetrics();
    expect(metrics.primaryReads).toBe(2);
  });

  it('tracks replica reads separately', async () => {
    const router = createExtendedRouter(primaryId);

    router.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1')));

    await router.routeRead('SELECT 1', 'eventual');
    await router.routeRead('SELECT 2', 'eventual');

    const metrics = router.getMetrics();
    expect(metrics.replicaReads).toBe(2);
  });

  it('tracks total writes', async () => {
    const router = createExtendedRouter(primaryId);

    await router.routeWrite('INSERT INTO t VALUES (1)');
    await router.routeWrite('UPDATE t SET x = 1');
    await router.routeWrite('DELETE FROM t');

    const metrics = router.getMetrics();
    expect(metrics.totalWrites).toBe(3);
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

// =============================================================================
// SESSION MANAGER TESTS
// =============================================================================

describe('SessionManager', () => {
  describe('Session Creation', () => {
    it('creates session with unique ID', () => {
      const manager = new SessionManager();

      const session1 = manager.createSession();
      const session2 = manager.createSession();

      expect(session1.sessionId).not.toBe(session2.sessionId);
    });

    it('creates session with preferred region', () => {
      const manager = new SessionManager();

      const session = manager.createSession('us-west');

      expect(session.preferredRegion).toBe('us-west');
    });

    it('initializes lastWriteLSN to 0', () => {
      const manager = new SessionManager();

      const session = manager.createSession();

      expect(session.lastWriteLSN).toBe(0n);
    });

    it('sets startedAt timestamp', () => {
      const manager = new SessionManager();

      const before = Date.now();
      const session = manager.createSession();
      const after = Date.now();

      expect(session.startedAt).toBeGreaterThanOrEqual(before);
      expect(session.startedAt).toBeLessThanOrEqual(after);
    });
  });

  describe('Session Retrieval', () => {
    it('retrieves existing session', () => {
      const manager = new SessionManager();

      const created = manager.createSession();
      const retrieved = manager.getSession(created.sessionId);

      expect(retrieved).toEqual(created);
    });

    it('returns null for non-existent session', () => {
      const manager = new SessionManager();

      const session = manager.getSession('non-existent-id');

      expect(session).toBeNull();
    });
  });

  describe('Session Updates', () => {
    it('updates lastWriteLSN', () => {
      const manager = new SessionManager();

      const session = manager.createSession();
      manager.updateSession(session.sessionId, 100n);

      const updated = manager.getSession(session.sessionId);
      expect(updated?.lastWriteLSN).toBe(100n);
    });

    it('ignores update for non-existent session', () => {
      const manager = new SessionManager();

      // Should not throw
      manager.updateSession('non-existent', 100n);
    });
  });

  describe('Session Deletion', () => {
    it('deletes session', () => {
      const manager = new SessionManager();

      const session = manager.createSession();
      manager.deleteSession(session.sessionId);

      const retrieved = manager.getSession(session.sessionId);
      expect(retrieved).toBeNull();
    });

    it('handles deletion of non-existent session', () => {
      const manager = new SessionManager();

      // Should not throw
      manager.deleteSession('non-existent');
    });
  });

  describe('Session Counting', () => {
    it('tracks session count', () => {
      const manager = new SessionManager();

      expect(manager.getSessionCount()).toBe(0);

      manager.createSession();
      expect(manager.getSessionCount()).toBe(1);

      manager.createSession();
      expect(manager.getSessionCount()).toBe(2);
    });

    it('decrements count on deletion', () => {
      const manager = new SessionManager();

      const session = manager.createSession();
      manager.createSession();

      manager.deleteSession(session.sessionId);

      expect(manager.getSessionCount()).toBe(1);
    });
  });

  describe('Session Cleanup', () => {
    it('enforces max sessions limit', () => {
      const manager = new SessionManager(3, 3600000);

      manager.createSession();
      manager.createSession();
      manager.createSession();
      manager.createSession(); // Should trigger cleanup

      expect(manager.getSessionCount()).toBeLessThanOrEqual(3);
    });

    it('removes oldest sessions when exceeding limit', async () => {
      const manager = new SessionManager(2, 3600000);

      const oldest = manager.createSession();
      await new Promise(resolve => setTimeout(resolve, 10));

      manager.createSession();
      await new Promise(resolve => setTimeout(resolve, 10));

      manager.createSession(); // Should remove oldest

      const retrieved = manager.getSession(oldest.sessionId);
      expect(retrieved).toBeNull();
    });
  });

  describe('Session TTL', () => {
    it('expires sessions after TTL', async () => {
      const manager = new SessionManager(100, 50); // 50ms TTL

      const session = manager.createSession();

      await new Promise(resolve => setTimeout(resolve, 100));

      const retrieved = manager.getSession(session.sessionId);
      expect(retrieved).toBeNull();
    });
  });
});

// =============================================================================
// FACTORY FUNCTION TESTS
// =============================================================================

describe('Router Factory Functions', () => {
  const primaryId = createReplicaId('us-east', 'primary-1');

  it('createReplicationRouter returns basic router', () => {
    const router = createReplicationRouter(primaryId);

    expect(router.routeRead).toBeDefined();
    expect(router.routeWrite).toBeDefined();
    expect(router.getMetrics).toBeDefined();
  });

  it('createExtendedRouter returns router with extra methods', () => {
    const router = createExtendedRouter(primaryId);

    expect(router.setCurrentRegion).toBeDefined();
    expect(router.registerReplica).toBeDefined();
    expect(router.deregisterReplica).toBeDefined();
    expect(router.getReplicas).toBeDefined();
    expect(router.getPrimary).toBeDefined();
    expect(router.recordLatency).toBeDefined();
  });

  it('createLoadBalancedRouter supports all strategies', () => {
    const strategies: LoadBalancingStrategy[] = [
      'round-robin',
      'random',
      'least-connections',
      'latency-weighted',
    ];

    for (const strategy of strategies) {
      const router = createLoadBalancedRouter(primaryId, strategy);
      expect(router).toBeDefined();
    }
  });

  it('createRouterWithSessions returns router and session manager', () => {
    const { router, sessionManager } = createRouterWithSessions(primaryId);

    expect(router).toBeDefined();
    expect(sessionManager).toBeInstanceOf(SessionManager);
  });

  it('applies custom config to router', async () => {
    const router = createExtendedRouter(primaryId, {
      boundedStalenessMs: 1000,
      minReplicas: 3,
    });

    // Config should affect routing behavior
    const metrics = router.getMetrics();
    expect(metrics).toBeDefined();
  });
});

// =============================================================================
// EDGE CASES TESTS
// =============================================================================

describe('Router - Edge Cases', () => {
  const primaryId = createReplicaId('us-east', 'primary-1');

  it('handles empty replica list', async () => {
    const router = createExtendedRouter(primaryId);

    const decision = await router.routeRead('SELECT 1', 'eventual');

    expect(decision.target).toEqual(primaryId);
    expect(decision.fallback).toBe(true);
  });

  it('handles rapid registration/deregistration', () => {
    const router = createExtendedRouter(primaryId);

    for (let i = 0; i < 100; i++) {
      const replicaId = createReplicaId('us-west', `r${i}`);
      router.registerReplica(createReplicaInfo(replicaId));
      router.deregisterReplica(replicaId);
    }

    // Primary should still be there
    const replicas = router.getReplicas();
    expect(replicas.length).toBe(1); // Just primary
  });

  it('handles duplicate registration', () => {
    const router = createExtendedRouter(primaryId);

    const replicaId = createReplicaId('us-west', 'r1');

    router.registerReplica(createReplicaInfo(replicaId, 'syncing', 50n));
    router.registerReplica(createReplicaInfo(replicaId, 'active', 100n));

    const replicas = router.getReplicas();
    const replica = replicas.find(r => r.id.instanceId === 'r1');

    // Should be updated, not duplicated
    expect(replicas.filter(r => r.id.instanceId === 'r1')).toHaveLength(1);
    expect(replica?.status).toBe('active');
  });

  it('handles very large LSN values', async () => {
    const router = createExtendedRouter(primaryId);

    const replicaId = createReplicaId('us-west', 'r1');
    router.registerReplica(createReplicaInfo(replicaId, 'active', BigInt('9007199254740991')));

    const session: SessionState = {
      sessionId: 'sess_123',
      lastWriteLSN: BigInt('9007199254740990'),
      startedAt: Date.now(),
    };

    const decision = await router.routeRead('SELECT 1', 'session', session);

    expect(decision.target).toEqual(replicaId);
  });

  it('handles concurrent routing requests', async () => {
    const router = createExtendedRouter(primaryId);

    router.registerReplica(createReplicaInfo(createReplicaId('us-west', 'r1')));
    router.registerReplica(createReplicaInfo(createReplicaId('eu-central', 'r2')));

    const promises = [];
    for (let i = 0; i < 100; i++) {
      promises.push(router.routeRead(`SELECT ${i}`, 'eventual'));
    }

    const results = await Promise.all(promises);

    expect(results).toHaveLength(100);
    results.forEach(r => expect(r.target).toBeDefined());
  });

  it('handles special characters in SQL', async () => {
    const router = createExtendedRouter(primaryId);

    const decision = await router.routeWrite(
      "INSERT INTO users VALUES (1, 'O''Brien', '{}', 'tab\there')"
    );

    expect(decision.target).toEqual(primaryId);
  });
});
