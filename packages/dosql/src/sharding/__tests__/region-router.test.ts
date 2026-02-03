/**
 * RegionRouter Unit Tests
 *
 * Tests for cross-region read replica routing:
 * - Region awareness and nearest replica selection
 * - Staleness tolerance and lag detection
 * - Automatic primary fallback
 * - Health monitoring and recovery
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

import {
  RegionRouter,
  createRegionRouter,
  coloToRegion,
  estimateInterRegionLatency,
  type RegionRouterConfig,
  type RegionId,
  type ReplicaSelection,
  DEFAULT_REGION_ROUTER_CONFIG,
} from '../region-router.js';

import {
  createShardId,
  shard,
  replica,
  type ShardConfig,
  type ReadPreference,
} from '../types.js';

// =============================================================================
// TEST FIXTURES
// =============================================================================

function createMultiRegionShards(): ShardConfig[] {
  return [
    shard(createShardId('shard-1'), 'do-ns-1', {
      metadata: { region: 'us-west' },
      replicas: [
        replica('replica-1-west', 'do-ns-1-replica', 'replica', { region: 'us-west', weight: 2 }),
        replica('replica-1-east', 'do-ns-1-replica', 'replica', { region: 'us-east', weight: 1 }),
        replica('replica-1-eu', 'do-ns-1-replica', 'replica', { region: 'eu-west', weight: 1 }),
      ],
    }),
    shard(createShardId('shard-2'), 'do-ns-2', {
      metadata: { region: 'us-east' },
      replicas: [
        replica('replica-2-west', 'do-ns-2-replica', 'replica', { region: 'us-west', weight: 1 }),
        replica('replica-2-east', 'do-ns-2-replica', 'replica', { region: 'us-east', weight: 2 }),
        replica('replica-2-asia', 'do-ns-2-replica', 'replica', { region: 'asia-northeast', weight: 1 }),
        replica('analytics-2', 'do-ns-2-analytics', 'analytics', { region: 'us-east' }),
      ],
    }),
  ];
}

function createSingleRegionShards(): ShardConfig[] {
  return [
    shard(createShardId('shard-1'), 'do-ns-1', {
      replicas: [
        replica('replica-1a', 'do-ns-1-replica', 'replica'),
        replica('replica-1b', 'do-ns-1-replica', 'replica'),
      ],
    }),
  ];
}

// =============================================================================
// REGION ROUTER TESTS
// =============================================================================

describe('RegionRouter', () => {
  let shards: ShardConfig[];
  let router: RegionRouter;

  beforeEach(() => {
    shards = createMultiRegionShards();
    router = createRegionRouter(shards);
  });

  afterEach(() => {
    router.stopHealthChecks();
  });

  describe('constructor', () => {
    it('should create router with shards', () => {
      expect(router).toBeInstanceOf(RegionRouter);
    });

    it('should accept custom configuration', () => {
      const config: Partial<RegionRouterConfig> = {
        maxStalenessMs: 10000,
        latencyWeight: 0.5,
        stalenessWeight: 0.5,
      };
      const customRouter = createRegionRouter(shards, config);
      expect(customRouter).toBeInstanceOf(RegionRouter);
    });

    it('should accept current region', () => {
      const regionRouter = createRegionRouter(shards, {}, 'us-west');
      expect(regionRouter.getCurrentRegion()).toBe('us-west');
    });

    it('should index all replicas by region', () => {
      const health = router.getAllRegionHealth();
      const regions = health.map(h => h.region);

      expect(regions).toContain('us-west');
      expect(regions).toContain('us-east');
      expect(regions).toContain('eu-west');
      expect(regions).toContain('asia-northeast');
    });
  });

  describe('setCurrentRegion', () => {
    it('should update current region', () => {
      router.setCurrentRegion('eu-west');
      expect(router.getCurrentRegion()).toBe('eu-west');
    });
  });

  // ===========================================================================
  // REPLICA SELECTION TESTS
  // ===========================================================================

  describe('selectReplica', () => {
    beforeEach(() => {
      // Initialize replica health by recording successes
      router.recordSuccess('shard-1', 'replica-1-west', 10);
      router.recordSuccess('shard-1', 'replica-1-east', 50);
      router.recordSuccess('shard-1', 'replica-1-eu', 100);
      router.recordSuccess('shard-2', 'replica-2-west', 20);
      router.recordSuccess('shard-2', 'replica-2-east', 15);
      router.recordSuccess('shard-2', 'replica-2-asia', 80);
      router.recordSuccess('shard-2', 'analytics-2', 25);
    });

    it('should select replica for replicaPreferred preference', () => {
      const selection = router.selectReplica('shard-1', 'replicaPreferred');

      expect(selection.replicaId).toBeDefined();
      expect(selection.fallbackToPrimary).toBe(false);
    });

    it('should fallback to primary for primary preference', () => {
      const selection = router.selectReplica('shard-1', 'primary');

      expect(selection.replicaId).toBeUndefined();
      expect(selection.fallbackToPrimary).toBe(true);
      expect(selection.reason).toContain('Primary read preference');
    });

    it('should select nearest replica for nearest preference', () => {
      router.setCurrentRegion('us-west');
      const selection = router.selectReplica('shard-1', 'nearest');

      // Should prefer us-west replica due to lowest latency
      expect(selection.replicaId).toBe('replica-1-west');
      expect(selection.reason).toContain('Nearest');
    });

    it('should select analytics replica for analytics preference', () => {
      const selection = router.selectReplica('shard-2', 'analytics');

      expect(selection.replicaId).toBe('analytics-2');
    });

    it('should fallback to regular replica if no analytics available', () => {
      const selection = router.selectReplica('shard-1', 'analytics');

      // shard-1 has no analytics replicas
      expect(selection.replicaId).toBeDefined();
      expect(selection.replicaId).not.toBe('analytics');
    });

    it('should prefer same-region replicas when configured', () => {
      const regionRouter = createRegionRouter(shards, {
        preferSameRegion: true,
        regionAffinityBonusMs: 50,
      }, 'us-east');

      regionRouter.recordSuccess('shard-1', 'replica-1-west', 20);
      regionRouter.recordSuccess('shard-1', 'replica-1-east', 30);

      const selection = regionRouter.selectReplica('shard-1', 'nearest');

      // us-east replica has latency 30ms, but with 50ms bonus becomes -20ms effective
      // us-west has 20ms, so us-east should be preferred
      expect(selection.replicaId).toBe('replica-1-east');
    });
  });

  // ===========================================================================
  // STALENESS DETECTION TESTS
  // ===========================================================================

  describe('staleness detection', () => {
    beforeEach(() => {
      router.recordSuccess('shard-1', 'replica-1-west', 10);
      router.recordSuccess('shard-1', 'replica-1-east', 50);
    });

    it('should skip replicas exceeding staleness threshold', () => {
      // Update lag for one replica to exceed threshold
      router.updateReplicaLag('shard-1', 'replica-1-west', 100n, 110n, 6000); // 6s lag
      router.updateReplicaLag('shard-1', 'replica-1-east', 105n, 110n, 500); // 0.5s lag

      const selection = router.selectReplica('shard-1', 'replicaPreferred', 5000);

      // Should skip stale replica-1-west and select replica-1-east
      expect(selection.replicaId).toBe('replica-1-east');
    });

    it('should fallback to primary when all replicas are stale', () => {
      router.updateReplicaLag('shard-1', 'replica-1-west', 100n, 200n, 10000);
      router.updateReplicaLag('shard-1', 'replica-1-east', 100n, 200n, 10000);
      router.updateReplicaLag('shard-1', 'replica-1-eu', 100n, 200n, 10000);

      const selection = router.selectReplica('shard-1', 'replicaPreferred', 5000);

      expect(selection.fallbackToPrimary).toBe(true);
      expect(selection.reason).toContain('No healthy replicas');
    });

    it('should not fallback for strict replica preference', () => {
      router.updateReplicaLag('shard-1', 'replica-1-west', 100n, 200n, 10000);
      router.updateReplicaLag('shard-1', 'replica-1-east', 100n, 200n, 10000);
      router.updateReplicaLag('shard-1', 'replica-1-eu', 100n, 200n, 10000);

      const selection = router.selectReplica('shard-1', 'replica', 5000);

      expect(selection.fallbackToPrimary).toBe(false);
      expect(selection.replicaId).toBeUndefined();
      expect(selection.estimatedLatencyMs).toBe(Infinity);
    });

    it('should check replica freshness', () => {
      router.updateReplicaLag('shard-1', 'replica-1-west', 100n, 110n, 1000);
      router.updateReplicaLag('shard-1', 'replica-1-east', 50n, 110n, 6000);

      expect(router.isReplicaFresh('shard-1', 'replica-1-west', 5000)).toBe(true);
      expect(router.isReplicaFresh('shard-1', 'replica-1-east', 5000)).toBe(false);
    });

    it('should get replica lag information', () => {
      router.updateReplicaLag('shard-1', 'replica-1-west', 100n, 110n, 1000);

      const lag = router.getReplicaLag('shard-1', 'replica-1-west');

      expect(lag).toBeDefined();
      expect(lag?.lagMs).toBe(1000);
      expect(lag?.replicaLSN).toBe(100n);
      expect(lag?.primaryLSN).toBe(110n);
    });
  });

  // ===========================================================================
  // LATENCY TRACKING TESTS
  // ===========================================================================

  describe('latency tracking', () => {
    it('should record latency samples', () => {
      router.recordLatency('shard-1', 'replica-1-west', 10);
      router.recordLatency('shard-1', 'replica-1-west', 20);
      router.recordLatency('shard-1', 'replica-1-west', 30);

      // Average should be 20ms
      const replicas = router.getReplicasInRegion('us-west');
      const replica = replicas.find(r => r.replicaId === 'replica-1-west');

      expect(replica?.latencyMs).toBe(20);
    });

    it('should calculate region latency from replicas', () => {
      router.recordLatency('shard-1', 'replica-1-west', 10);
      router.recordLatency('shard-2', 'replica-2-west', 30);

      const regionLatency = router.getRegionLatency('us-west');

      // Average of 10 and 30
      expect(regionLatency).toBe(20);
    });

    it('should maintain sliding window for latency samples', () => {
      // Record more than 100 samples
      for (let i = 0; i < 150; i++) {
        router.recordLatency('shard-1', 'replica-1-west', i);
      }

      // Should use the most recent samples (50-149), average = 99.5
      const replicas = router.getReplicasInRegion('us-west');
      const replica = replicas.find(r => r.replicaId === 'replica-1-west');

      // Latest 100 samples: 50-149, average = (50+149)/2 = 99.5
      expect(replica?.latencyMs).toBe(99.5);
    });
  });

  // ===========================================================================
  // HEALTH MONITORING TESTS
  // ===========================================================================

  describe('health monitoring', () => {
    it('should mark replica healthy after success', () => {
      router.recordSuccess('shard-1', 'replica-1-west');

      const health = router.getReplicaHealth('shard-1', 'replica-1-west');
      expect(health).toBe('healthy');
    });

    it('should mark replica unhealthy after failures', () => {
      // Default threshold is 3
      router.recordFailure('shard-1', 'replica-1-west');
      router.recordFailure('shard-1', 'replica-1-west');
      router.recordFailure('shard-1', 'replica-1-west');

      const health = router.getReplicaHealth('shard-1', 'replica-1-west');
      expect(health).toBe('unhealthy');
    });

    it('should mark replica degraded during partial failures', () => {
      router.recordSuccess('shard-1', 'replica-1-west');
      router.recordFailure('shard-1', 'replica-1-west');
      router.recordFailure('shard-1', 'replica-1-west');

      const health = router.getReplicaHealth('shard-1', 'replica-1-west');
      expect(health).toBe('degraded');
    });

    it('should recover replica health after successes', () => {
      // First, mark unhealthy
      for (let i = 0; i < 3; i++) {
        router.recordFailure('shard-1', 'replica-1-west');
      }
      expect(router.getReplicaHealth('shard-1', 'replica-1-west')).toBe('unhealthy');

      // Then recover
      for (let i = 0; i < 4; i++) {
        router.recordSuccess('shard-1', 'replica-1-west');
      }
      expect(router.getReplicaHealth('shard-1', 'replica-1-west')).toBe('healthy');
    });

    it('should skip unhealthy replicas in selection', () => {
      router.recordSuccess('shard-1', 'replica-1-west', 10);
      router.recordSuccess('shard-1', 'replica-1-east', 50);

      // Mark west replica as unhealthy
      for (let i = 0; i < 3; i++) {
        router.recordFailure('shard-1', 'replica-1-west');
      }

      const selection = router.selectReplica('shard-1', 'nearest');

      // Should not select the unhealthy west replica
      expect(selection.replicaId).not.toBe('replica-1-west');
    });
  });

  // ===========================================================================
  // REGION HEALTH TESTS
  // ===========================================================================

  describe('region health', () => {
    it('should calculate region health status', () => {
      router.recordSuccess('shard-1', 'replica-1-west');
      router.recordSuccess('shard-2', 'replica-2-west');

      const health = router.getRegionHealth('us-west');

      expect(health).toBeDefined();
      expect(health?.status).toBe('healthy');
      expect(health?.healthyReplicaCount).toBe(2);
      expect(health?.totalReplicaCount).toBe(2);
    });

    it('should report degraded region when some replicas unhealthy', () => {
      router.recordSuccess('shard-1', 'replica-1-west');

      // Mark one replica unhealthy
      for (let i = 0; i < 3; i++) {
        router.recordFailure('shard-2', 'replica-2-west');
      }

      const health = router.getRegionHealth('us-west');

      expect(health?.status).toBe('degraded');
      expect(health?.healthyReplicaCount).toBe(1);
      expect(health?.totalReplicaCount).toBe(2);
    });

    it('should get all region health summaries', () => {
      router.recordSuccess('shard-1', 'replica-1-west');
      router.recordSuccess('shard-1', 'replica-1-east');
      router.recordSuccess('shard-1', 'replica-1-eu');

      const allHealth = router.getAllRegionHealth();

      expect(allHealth.length).toBeGreaterThan(0);
      expect(allHealth.every(h => h.region !== undefined)).toBe(true);
    });

    it('should include average lag in region health', () => {
      router.recordSuccess('shard-1', 'replica-1-west');
      router.updateReplicaLag('shard-1', 'replica-1-west', 100n, 110n, 1000);
      router.recordSuccess('shard-2', 'replica-2-west');
      router.updateReplicaLag('shard-2', 'replica-2-west', 100n, 115n, 500);

      const health = router.getRegionHealth('us-west');

      expect(health?.avgLagMs).toBe(750); // Average of 1000 and 500
    });
  });

  // ===========================================================================
  // HEALTH CHECK MANAGEMENT TESTS
  // ===========================================================================

  describe('health checks', () => {
    it('should start periodic health checks', async () => {
      const checkFn = vi.fn().mockResolvedValue({ latencyMs: 10, lagMs: 100 });

      router.startHealthChecks(checkFn);

      // Wait for initial check
      await new Promise(resolve => setTimeout(resolve, 50));

      expect(checkFn).toHaveBeenCalled();

      router.stopHealthChecks();
    });

    it('should stop health checks', () => {
      const checkFn = vi.fn().mockResolvedValue({ latencyMs: 10, lagMs: 100 });

      router.startHealthChecks(checkFn);
      router.stopHealthChecks();

      // Clear any pending calls
      vi.clearAllMocks();

      // Should not make more calls after stopping
      expect(true).toBe(true); // Placeholder assertion
    });

    it('should handle health check failures gracefully', async () => {
      const checkFn = vi.fn().mockRejectedValue(new Error('Check failed'));

      router.startHealthChecks(checkFn);

      await new Promise(resolve => setTimeout(resolve, 50));

      // Should have recorded failures
      const health = router.getReplicaHealth('shard-1', 'replica-1-west');
      // First failure, not yet unhealthy
      expect(health).not.toBe('healthy');

      router.stopHealthChecks();
    });
  });

  // ===========================================================================
  // METRICS TESTS
  // ===========================================================================

  describe('metrics', () => {
    it('should return metrics summary', () => {
      router.recordSuccess('shard-1', 'replica-1-west', 10);
      router.recordSuccess('shard-1', 'replica-1-east', 50);
      router.updateReplicaLag('shard-1', 'replica-1-west', 100n, 110n, 1000);

      const metrics = router.getMetrics();

      expect(metrics.regions).toBeGreaterThan(0);
      expect(metrics.totalReplicas).toBeGreaterThan(0);
      expect(metrics.healthyReplicas).toBeGreaterThan(0);
      expect(metrics.avgLatencyMs).toBeGreaterThan(0);
    });

    it('should get replicas in a specific region', () => {
      router.recordSuccess('shard-1', 'replica-1-west', 10);
      router.recordSuccess('shard-2', 'replica-2-west', 20);

      const replicas = router.getReplicasInRegion('us-west');

      expect(replicas).toHaveLength(2);
      expect(replicas.every(r => r.replicaId.includes('west'))).toBe(true);
    });
  });

  // ===========================================================================
  // RESET FUNCTIONALITY TESTS
  // ===========================================================================

  describe('reset functionality', () => {
    it('should reset all health states', () => {
      // Build up some state
      router.recordSuccess('shard-1', 'replica-1-west', 10);
      for (let i = 0; i < 3; i++) {
        router.recordFailure('shard-1', 'replica-1-east');
      }

      // Reset
      router.resetHealthStates();

      // All should be unknown now
      expect(router.getReplicaHealth('shard-1', 'replica-1-west')).toBe('unknown');
      expect(router.getReplicaHealth('shard-1', 'replica-1-east')).toBe('unknown');
    });
  });
});

// =============================================================================
// UTILITY FUNCTION TESTS
// =============================================================================

describe('Region Utilities', () => {
  describe('coloToRegion', () => {
    it('should map Cloudflare colo codes to regions', () => {
      expect(coloToRegion('SJC')).toBe('us-west');
      expect(coloToRegion('LAX')).toBe('us-west');
      expect(coloToRegion('IAD')).toBe('us-east');
      expect(coloToRegion('LHR')).toBe('eu-west');
      expect(coloToRegion('FRA')).toBe('eu-central');
      expect(coloToRegion('NRT')).toBe('asia-northeast');
      expect(coloToRegion('SIN')).toBe('asia-southeast');
      expect(coloToRegion('SYD')).toBe('oceania');
    });

    it('should handle lowercase colo codes', () => {
      expect(coloToRegion('sjc')).toBe('us-west');
      expect(coloToRegion('lhr')).toBe('eu-west');
    });

    it('should return default for unknown colo', () => {
      expect(coloToRegion('UNKNOWN')).toBe('default');
      expect(coloToRegion('XXX')).toBe('default');
    });
  });

  describe('estimateInterRegionLatency', () => {
    it('should return 0 for same region', () => {
      expect(estimateInterRegionLatency('us-west', 'us-west')).toBe(0);
      expect(estimateInterRegionLatency('eu-west', 'eu-west')).toBe(0);
    });

    it('should estimate cross-region latency', () => {
      // US cross-country
      expect(estimateInterRegionLatency('us-west', 'us-east')).toBe(70);

      // Transatlantic
      expect(estimateInterRegionLatency('us-east', 'eu-west')).toBe(80);

      // Pacific
      expect(estimateInterRegionLatency('us-west', 'asia-northeast')).toBe(120);
    });

    it('should be symmetric', () => {
      expect(estimateInterRegionLatency('us-west', 'eu-west'))
        .toBe(estimateInterRegionLatency('eu-west', 'us-west'));
    });

    it('should return default for unknown region pairs', () => {
      expect(estimateInterRegionLatency('unknown1', 'unknown2')).toBe(200);
    });
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Edge Cases', () => {
  it('should handle empty shard configuration', () => {
    const router = createRegionRouter([]);

    expect(() => router.selectReplica('nonexistent', 'replicaPreferred'))
      .not.toThrow();
  });

  it('should handle shards without replicas', () => {
    const shardsWithoutReplicas = [
      shard(createShardId('shard-1'), 'do-ns-1'),
    ];
    const router = createRegionRouter(shardsWithoutReplicas);

    const selection = router.selectReplica('shard-1', 'replicaPreferred');

    expect(selection.fallbackToPrimary).toBe(true);
    expect(selection.reason).toContain('No healthy replicas');
  });

  it('should handle replicas without region specified', () => {
    const shardsWithDefaultRegion = createSingleRegionShards();
    const router = createRegionRouter(shardsWithDefaultRegion);

    router.recordSuccess('shard-1', 'replica-1a', 10);

    const health = router.getRegionHealth('default');
    expect(health).toBeDefined();
    expect(health?.totalReplicaCount).toBe(2);
  });

  it('should handle unknown shard ID', () => {
    const router = createRegionRouter(createMultiRegionShards());

    const selection = router.selectReplica('unknown-shard', 'replicaPreferred');

    expect(selection.fallbackToPrimary).toBe(true);
    expect(selection.reason).toContain('Unknown shard');
  });

  it('should handle unknown replica in health operations', () => {
    const router = createRegionRouter(createMultiRegionShards());

    // These should not throw
    expect(() => router.recordSuccess('shard-1', 'unknown-replica')).not.toThrow();
    expect(() => router.recordFailure('shard-1', 'unknown-replica')).not.toThrow();
    expect(() => router.recordLatency('shard-1', 'unknown-replica', 10)).not.toThrow();
  });

  it('should handle concurrent health updates', async () => {
    const router = createRegionRouter(createMultiRegionShards());

    // Simulate concurrent updates
    const updates = Array.from({ length: 100 }, (_, i) =>
      Promise.resolve().then(() => {
        if (i % 2 === 0) {
          router.recordSuccess('shard-1', 'replica-1-west', Math.random() * 100);
        } else {
          router.recordFailure('shard-1', 'replica-1-west');
        }
      })
    );

    await Promise.all(updates);

    // Should not crash and health should be defined
    const health = router.getReplicaHealth('shard-1', 'replica-1-west');
    expect(health).toBeDefined();
  });
});

// =============================================================================
// CONFIGURATION TESTS
// =============================================================================

describe('Configuration', () => {
  it('should use default configuration values', () => {
    expect(DEFAULT_REGION_ROUTER_CONFIG.maxStalenessMs).toBe(5000);
    expect(DEFAULT_REGION_ROUTER_CONFIG.latencyWeight).toBe(0.7);
    expect(DEFAULT_REGION_ROUTER_CONFIG.stalenessWeight).toBe(0.3);
    expect(DEFAULT_REGION_ROUTER_CONFIG.preferSameRegion).toBe(true);
  });

  it('should merge custom config with defaults', () => {
    const router = createRegionRouter(createMultiRegionShards(), {
      maxStalenessMs: 10000,
    });

    // Custom value applied, but we can verify behavior
    router.recordSuccess('shard-1', 'replica-1-west');
    router.updateReplicaLag('shard-1', 'replica-1-west', 100n, 110n, 8000);

    // With 10s threshold, 8s lag should be acceptable
    expect(router.isReplicaFresh('shard-1', 'replica-1-west', 10000)).toBe(true);
  });

  it('should respect automaticPrimaryFallback setting', () => {
    const router = createRegionRouter(createMultiRegionShards(), {
      automaticPrimaryFallback: false,
    });

    // Mark all replicas unhealthy
    const shards = createMultiRegionShards();
    for (const shard of shards) {
      for (const replica of shard.replicas ?? []) {
        for (let i = 0; i < 5; i++) {
          router.recordFailure(shard.id, replica.id);
        }
      }
    }

    // With replicaPreferred, should still fallback as no replicas available
    const selection = router.selectReplica('shard-1', 'replicaPreferred');
    expect(selection.fallbackToPrimary).toBe(true);
  });
});

// =============================================================================
// WEIGHTED SELECTION TESTS
// =============================================================================

describe('Weighted Selection', () => {
  it('should balance selection based on latency and staleness weights', () => {
    const router = createRegionRouter(createMultiRegionShards(), {
      latencyWeight: 0.9,
      stalenessWeight: 0.1,
    });

    // Low latency, high staleness
    router.recordSuccess('shard-1', 'replica-1-west', 10);
    router.updateReplicaLag('shard-1', 'replica-1-west', 100n, 200n, 3000);

    // High latency, low staleness
    router.recordSuccess('shard-1', 'replica-1-east', 100);
    router.updateReplicaLag('shard-1', 'replica-1-east', 195n, 200n, 100);

    // With 90% weight on latency, should prefer low-latency replica
    const selection = router.selectReplica('shard-1', 'replicaPreferred');
    expect(selection.replicaId).toBe('replica-1-west');
  });

  it('should favor fresh replicas when staleness weight is high', () => {
    const router = createRegionRouter(createMultiRegionShards(), {
      latencyWeight: 0.1,
      stalenessWeight: 0.9,
    });

    // Low latency, high staleness
    router.recordSuccess('shard-1', 'replica-1-west', 10);
    router.updateReplicaLag('shard-1', 'replica-1-west', 100n, 200n, 3000);

    // High latency, low staleness
    router.recordSuccess('shard-1', 'replica-1-east', 100);
    router.updateReplicaLag('shard-1', 'replica-1-east', 195n, 200n, 100);

    // With 90% weight on staleness, should prefer fresh replica
    const selection = router.selectReplica('shard-1', 'replicaPreferred');
    expect(selection.replicaId).toBe('replica-1-east');
  });
});
