/**
 * Replica Selector Unit Tests
 *
 * Tests for replica management and selection:
 * - Read preference handling
 * - Health tracking
 * - Circuit breaker behavior
 * - Region-aware selection
 * - Load balancing
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

import {
  DefaultReplicaSelector,
  HealthChecker,
  createReplicaSelector,
  createHealthChecker,
  type HealthConfig,
} from '../replica.js';

import {
  createShardId,
  shard,
  replica,
  type ShardConfig,
  type ReadPreference,
  type ReplicaHealth,
} from '../types.js';

// =============================================================================
// TEST FIXTURES
// =============================================================================

function createTestShards(
  options: {
    withReplicas?: boolean;
    withAnalytics?: boolean;
    withRegions?: boolean;
    withWeights?: boolean;
  } = {}
): ShardConfig[] {
  const { withReplicas = true, withAnalytics = false, withRegions = false, withWeights = false } = options;

  return Array.from({ length: 3 }, (_, i) => {
    const replicas = [];

    if (withReplicas) {
      replicas.push(
        replica(
          `replica-${i + 1}-a`,
          `do-ns-${i + 1}-replica-a`,
          'replica',
          withRegions ? { region: i === 0 ? 'us-west' : i === 1 ? 'us-east' : 'eu-central' } : undefined
        ),
        replica(
          `replica-${i + 1}-b`,
          `do-ns-${i + 1}-replica-b`,
          'replica',
          withRegions
            ? { region: i === 0 ? 'us-west' : i === 1 ? 'us-east' : 'eu-central', weight: withWeights ? (i + 1) * 10 : undefined }
            : { weight: withWeights ? (i + 1) * 10 : undefined }
        )
      );
    }

    if (withAnalytics) {
      replicas.push(
        replica(`analytics-${i + 1}`, `do-ns-${i + 1}-analytics`, 'analytics')
      );
    }

    return shard(createShardId(`shard-${i + 1}`), `do-ns-${i + 1}`, {
      replicas: replicas.length > 0 ? replicas : undefined,
    });
  });
}

// =============================================================================
// REPLICA SELECTOR TESTS
// =============================================================================

describe('DefaultReplicaSelector', () => {
  describe('constructor', () => {
    it('should create selector with shards', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      expect(selector).toBeInstanceOf(DefaultReplicaSelector);
    });

    it('should create selector with custom health config', () => {
      const shards = createTestShards();
      const config: HealthConfig = {
        failureThreshold: 10,
        successThreshold: 5,
        circuitResetMs: 60000,
      };
      const selector = createReplicaSelector(shards, config);

      expect(selector).toBeInstanceOf(DefaultReplicaSelector);
    });

    it('should create selector with region awareness', () => {
      const shards = createTestShards({ withRegions: true });
      const selector = createReplicaSelector(shards, undefined, 'us-west');

      expect(selector).toBeInstanceOf(DefaultReplicaSelector);
    });

    it('should initialize health states for all replicas', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      // Check cluster health to verify initialization
      const health = selector.getClusterHealth();
      expect(health.totalShards).toBe(3);
    });
  });

  describe('read preference: primary', () => {
    it('should always return primary for primary preference', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      const result = selector.select('shard-1', 'primary');

      expect(result).toBe('shard-1'); // Primary DO ID
    });

    it('should return primary even with healthy replicas', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      // Mark replicas as healthy
      selector.recordSuccess('shard-1', 'replica-1-a', 5);
      selector.recordSuccess('shard-1', 'replica-1-b', 5);

      const result = selector.select('shard-1', 'primary');

      expect(result).toBe('shard-1');
    });
  });

  describe('read preference: primaryPreferred', () => {
    it('should return primary when healthy', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      // Mark primary as healthy
      selector.recordSuccess('shard-1', 'shard-1', 5);

      const result = selector.select('shard-1', 'primaryPreferred');

      expect(result).toBe('shard-1');
    });

    it('should fall back to replica when primary is unhealthy', () => {
      const shards = createTestShards();
      const config: HealthConfig = { failureThreshold: 2 };
      const selector = createReplicaSelector(shards, config);

      // Mark primary as unhealthy
      selector.recordFailure('shard-1', 'shard-1');
      selector.recordFailure('shard-1', 'shard-1');

      const result = selector.select('shard-1', 'primaryPreferred');

      // Should return a replica
      expect(result).toMatch(/replica-1-[ab]/);
    });
  });

  describe('read preference: replica', () => {
    it('should return a replica', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      const result = selector.select('shard-1', 'replica');

      expect(result).toMatch(/replica-1-[ab]/);
    });

    it('should round-robin across replicas', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      const results = new Set<string>();
      for (let i = 0; i < 10; i++) {
        results.add(selector.select('shard-1', 'replica')!);
      }

      // Should have seen both replicas
      expect(results.size).toBe(2);
    });

    it('should return primary if no replicas configured', () => {
      const shards = createTestShards({ withReplicas: false });
      const selector = createReplicaSelector(shards);

      const result = selector.select('shard-1', 'replica');

      expect(result).toBe('shard-1');
    });
  });

  describe('read preference: replicaPreferred', () => {
    it('should return replica when available', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      // Mark replica as healthy
      selector.recordSuccess('shard-1', 'replica-1-a', 5);

      const result = selector.select('shard-1', 'replicaPreferred');

      expect(result).toMatch(/replica-1-[ab]/);
    });

    it('should fall back to primary when all replicas unhealthy', () => {
      const shards = createTestShards();
      const config: HealthConfig = { failureThreshold: 2 };
      const selector = createReplicaSelector(shards, config);

      // Mark all replicas as unhealthy
      for (let i = 0; i < 2; i++) {
        selector.recordFailure('shard-1', 'replica-1-a');
        selector.recordFailure('shard-1', 'replica-1-b');
      }

      const result = selector.select('shard-1', 'replicaPreferred');

      expect(result).toBe('shard-1');
    });
  });

  describe('read preference: nearest', () => {
    it('should prefer same-region replicas', () => {
      const shards = createTestShards({ withReplicas: true, withRegions: true });
      const selector = createReplicaSelector(shards, undefined, 'us-west');

      // Mark replicas as healthy
      selector.recordSuccess('shard-1', 'replica-1-a', 5);
      selector.recordSuccess('shard-1', 'replica-1-b', 5);

      const result = selector.select('shard-1', 'nearest');

      // Should prefer us-west region replica
      expect(result).toMatch(/replica-1-[ab]/);
    });

    it('should prefer lower latency replicas', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      // Record different latencies
      for (let i = 0; i < 10; i++) {
        selector.recordSuccess('shard-1', 'replica-1-a', 50); // Higher latency
        selector.recordSuccess('shard-1', 'replica-1-b', 10); // Lower latency
      }

      // Make multiple selections
      const results: string[] = [];
      for (let i = 0; i < 10; i++) {
        results.push(selector.select('shard-1', 'nearest')!);
      }

      // Should prefer lower latency replica
      const preferredCount = results.filter(r => r === 'replica-1-b').length;
      expect(preferredCount).toBeGreaterThan(0);
    });
  });

  describe('read preference: analytics', () => {
    it('should prefer analytics replicas', () => {
      const shards = createTestShards({ withReplicas: true, withAnalytics: true });
      const selector = createReplicaSelector(shards);

      const result = selector.select('shard-1', 'analytics');

      expect(result).toBe('analytics-1');
    });

    it('should fall back to regular replicas if no analytics', () => {
      const shards = createTestShards({ withReplicas: true, withAnalytics: false });
      const selector = createReplicaSelector(shards);

      const result = selector.select('shard-1', 'analytics');

      expect(result).toMatch(/replica-1-[ab]/);
    });
  });

  describe('unknown shard handling', () => {
    it('should return undefined for unknown shard', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      const result = selector.select('unknown-shard', 'primary');

      expect(result).toBeUndefined();
    });
  });
});

// =============================================================================
// HEALTH TRACKING TESTS
// =============================================================================

describe('Health Tracking', () => {
  describe('recordSuccess', () => {
    it('should mark replica as healthy after success threshold', () => {
      const shards = createTestShards();
      const config: HealthConfig = { successThreshold: 3 };
      const selector = createReplicaSelector(shards, config);

      // Record successes
      for (let i = 0; i < 3; i++) {
        selector.recordSuccess('shard-1', 'replica-1-a', 10);
      }

      const health = selector.getShardHealth('shard-1');
      expect(health.replicaHealths['replica-1-a']).toBe('healthy');
    });

    it('should track latency', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      selector.recordSuccess('shard-1', 'replica-1-a', 50);
      selector.recordSuccess('shard-1', 'replica-1-a', 60);
      selector.recordSuccess('shard-1', 'replica-1-a', 40);

      const health = selector.getShardHealth('shard-1');
      expect(health.latencyMs).toBeDefined();
    });

    it('should reset failure count on success', () => {
      const shards = createTestShards();
      const config: HealthConfig = { failureThreshold: 5, successThreshold: 1 };
      const selector = createReplicaSelector(shards, config);

      // Record some failures
      selector.recordFailure('shard-1', 'replica-1-a');
      selector.recordFailure('shard-1', 'replica-1-a');

      // Record success
      selector.recordSuccess('shard-1', 'replica-1-a', 10);

      const state = selector.getHealthStateDebug('shard-1', 'replica-1-a');
      expect(state.health).toBe('healthy');
      expect(state.failureCount).toBe(0);
    });
  });

  describe('recordFailure', () => {
    it('should mark replica as unhealthy after failure threshold', () => {
      const shards = createTestShards();
      const config: HealthConfig = { failureThreshold: 3 };
      const selector = createReplicaSelector(shards, config);

      // Record failures
      for (let i = 0; i < 3; i++) {
        selector.recordFailure('shard-1', 'replica-1-a');
      }

      const health = selector.getShardHealth('shard-1');
      expect(health.replicaHealths['replica-1-a']).toBe('unhealthy');
    });

    it('should mark as degraded before unhealthy threshold', () => {
      const shards = createTestShards();
      const config: HealthConfig = { failureThreshold: 4 };
      const selector = createReplicaSelector(shards, config);

      // Record failures (half of threshold)
      selector.recordFailure('shard-1', 'replica-1-a');
      selector.recordFailure('shard-1', 'replica-1-a');

      const health = selector.getShardHealth('shard-1');
      expect(health.replicaHealths['replica-1-a']).toBe('degraded');
    });

    it('should reset success count on failure', () => {
      const shards = createTestShards();
      const config: HealthConfig = { successThreshold: 3 };
      const selector = createReplicaSelector(shards, config);

      // Build up some successes
      selector.recordSuccess('shard-1', 'replica-1-a', 10);
      selector.recordSuccess('shard-1', 'replica-1-a', 10);

      // Fail
      selector.recordFailure('shard-1', 'replica-1-a');

      const state = selector.getHealthStateDebug('shard-1', 'replica-1-a');
      expect(state.successCount).toBe(0);
    });
  });

  describe('circuit breaker', () => {
    it('should open circuit after failure threshold', () => {
      const shards = createTestShards();
      const config: HealthConfig = { failureThreshold: 3 };
      const selector = createReplicaSelector(shards, config);

      for (let i = 0; i < 3; i++) {
        selector.recordFailure('shard-1', 'replica-1-a');
      }

      const state = selector.getHealthStateDebug('shard-1', 'replica-1-a');
      expect(state.circuitState).toBe('open');
    });

    it('should transition to half-open after reset timeout', () => {
      const shards = createTestShards();
      const config: HealthConfig = { failureThreshold: 3, circuitResetMs: 100 };
      const selector = createReplicaSelector(shards, config);

      // Open circuit
      for (let i = 0; i < 3; i++) {
        selector.recordFailure('shard-1', 'replica-1-a');
      }

      // Wait for reset timeout
      return new Promise<void>(resolve => {
        setTimeout(() => {
          // Trigger state check via selection
          selector.select('shard-1', 'replica');

          const state = selector.getHealthStateDebug('shard-1', 'replica-1-a');
          expect(state.circuitState).toBe('half-open');
          resolve();
        }, 150);
      });
    });

    it('should close circuit after success in half-open state', async () => {
      const shards = createTestShards();
      const config: HealthConfig = { failureThreshold: 3, circuitResetMs: 50, successThreshold: 1 };
      const selector = createReplicaSelector(shards, config);

      // Open circuit
      for (let i = 0; i < 3; i++) {
        selector.recordFailure('shard-1', 'replica-1-a');
      }

      // Wait for reset timeout
      await new Promise(resolve => setTimeout(resolve, 100));

      // Trigger half-open
      selector.select('shard-1', 'replica');

      // Record success
      selector.recordSuccess('shard-1', 'replica-1-a', 10);

      const state = selector.getHealthStateDebug('shard-1', 'replica-1-a');
      expect(state.circuitState).toBe('closed');
    });
  });

  describe('resetHealth', () => {
    it('should reset health state to unknown', () => {
      const shards = createTestShards();
      const config: HealthConfig = { failureThreshold: 2 };
      const selector = createReplicaSelector(shards, config);

      // Mark as unhealthy
      selector.recordFailure('shard-1', 'replica-1-a');
      selector.recordFailure('shard-1', 'replica-1-a');

      // Reset
      selector.resetHealth('shard-1', 'replica-1-a');

      const state = selector.getHealthStateDebug('shard-1', 'replica-1-a');
      expect(state.health).toBe('unknown');
      expect(state.failureCount).toBe(0);
      expect(state.circuitState).toBe('closed');
    });
  });
});

// =============================================================================
// SHARD HEALTH TESTS
// =============================================================================

describe('Shard Health', () => {
  describe('getShardHealth', () => {
    it('should return health for existing shard', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      const health = selector.getShardHealth('shard-1');

      expect(health.shardId).toBe('shard-1');
      expect(health.status).toBeDefined();
      expect(health.primaryHealth).toBeDefined();
      expect(health.replicaHealths).toBeDefined();
    });

    it('should return unknown status for non-existent shard', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      const health = selector.getShardHealth('unknown-shard');

      expect(health.status).toBe('unknown');
    });

    it('should reflect overall shard status based on primary health', () => {
      const shards = createTestShards();
      const config: HealthConfig = { failureThreshold: 2 };
      const selector = createReplicaSelector(shards, config);

      // Mark primary as unhealthy
      selector.recordFailure('shard-1', 'shard-1');
      selector.recordFailure('shard-1', 'shard-1');

      const health = selector.getShardHealth('shard-1');
      expect(health.status).toBe('unhealthy');
    });

    it('should include last checked timestamp', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      const before = Date.now();
      const health = selector.getShardHealth('shard-1');
      const after = Date.now();

      expect(health.lastChecked).toBeGreaterThanOrEqual(before);
      expect(health.lastChecked).toBeLessThanOrEqual(after);
    });
  });
});

// =============================================================================
// CLUSTER HEALTH TESTS
// =============================================================================

describe('Cluster Health', () => {
  describe('getClusterHealth', () => {
    it('should return aggregate health stats', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      const health = selector.getClusterHealth();

      expect(health.totalShards).toBe(3);
      expect(health.shards).toHaveLength(3);
    });

    it('should count healthy shards', () => {
      const shards = createTestShards();
      const config: HealthConfig = { failureThreshold: 2, successThreshold: 1 };
      const selector = createReplicaSelector(shards, config);

      // Mark shard-1 and shard-2 primaries as healthy
      selector.recordSuccess('shard-1', 'shard-1', 10);
      selector.recordSuccess('shard-2', 'shard-2', 10);

      const health = selector.getClusterHealth();

      // Unknown counts as healthy for availability purposes
      expect(health.healthyShards).toBe(3);
    });

    it('should count unhealthy shards', () => {
      const shards = createTestShards();
      const config: HealthConfig = { failureThreshold: 2 };
      const selector = createReplicaSelector(shards, config);

      // Mark one primary as unhealthy
      selector.recordFailure('shard-1', 'shard-1');
      selector.recordFailure('shard-1', 'shard-1');

      const health = selector.getClusterHealth();

      expect(health.healthyShards).toBe(2);
    });

    it('should count replica health', () => {
      const shards = createTestShards();
      const config: HealthConfig = { successThreshold: 1 };
      const selector = createReplicaSelector(shards, config);

      // Mark some replicas as healthy
      selector.recordSuccess('shard-1', 'replica-1-a', 10);
      selector.recordSuccess('shard-2', 'replica-2-a', 10);

      const health = selector.getClusterHealth();

      // 3 shards * 2 replicas = 6 total replicas
      expect(health.totalReplicas).toBe(6);
      expect(health.healthyReplicas).toBeGreaterThanOrEqual(2);
    });

    it('should include lastUpdated timestamp', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      const before = Date.now();
      const health = selector.getClusterHealth();
      const after = Date.now();

      expect(health.lastUpdated).toBeGreaterThanOrEqual(before);
      expect(health.lastUpdated).toBeLessThanOrEqual(after);
    });
  });
});

// =============================================================================
// HEALTH CHECKER TESTS
// =============================================================================

describe('HealthChecker', () => {
  describe('constructor', () => {
    it('should create health checker', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);
      const checkFn = vi.fn().mockResolvedValue(10);

      const checker = createHealthChecker(selector, shards, checkFn);

      expect(checker).toBeInstanceOf(HealthChecker);
    });
  });

  describe('start and stop', () => {
    it('should start and run initial check', async () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);
      const checkFn = vi.fn().mockResolvedValue(10);

      const checker = createHealthChecker(selector, shards, checkFn, 10000);
      checker.start();

      // Wait for initial check
      await new Promise(resolve => setTimeout(resolve, 50));

      checker.stop();

      // Should have called check for primary + all replicas
      // 3 primaries + 6 replicas = 9 total
      expect(checkFn).toHaveBeenCalledTimes(9);
    });

    it('should stop periodic checks', async () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);
      const checkFn = vi.fn().mockResolvedValue(10);

      const checker = createHealthChecker(selector, shards, checkFn, 50);
      checker.start();

      // Let it run one cycle
      await new Promise(resolve => setTimeout(resolve, 30));

      const callCountAfterFirst = checkFn.mock.calls.length;

      // Stop immediately
      checker.stop();

      // Wait longer than interval
      await new Promise(resolve => setTimeout(resolve, 100));

      // Should not have made more calls
      expect(checkFn.mock.calls.length).toBe(callCountAfterFirst);
    });

    it('should not start twice', () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);
      const checkFn = vi.fn().mockResolvedValue(10);

      const checker = createHealthChecker(selector, shards, checkFn, 10000);
      checker.start();
      checker.start(); // Should be no-op

      checker.stop();
    });
  });

  describe('checkAll', () => {
    it('should check all replicas in parallel', async () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);
      const checkFn = vi.fn().mockResolvedValue(10);

      const checker = createHealthChecker(selector, shards, checkFn);
      await checker.checkAll();

      // 3 primaries + 6 replicas = 9 checks
      expect(checkFn).toHaveBeenCalledTimes(9);
    });

    it('should record success on successful check', async () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);
      const checkFn = vi.fn().mockResolvedValue(10);

      const checker = createHealthChecker(selector, shards, checkFn);
      await checker.checkAll();

      // All replicas should have been marked with success
      const state = selector.getHealthStateDebug('shard-1', 'replica-1-a');
      expect(state.lastSuccessTime).toBeGreaterThan(0);
    });

    it('should record failure on failed check', async () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);
      const checkFn = vi.fn().mockRejectedValue(new Error('Connection failed'));

      const checker = createHealthChecker(selector, shards, checkFn);
      await checker.checkAll();

      // All replicas should have been marked with failure
      const state = selector.getHealthStateDebug('shard-1', 'replica-1-a');
      expect(state.lastFailureTime).toBeGreaterThan(0);
    });

    it('should handle mixed success and failure', async () => {
      const shards = createTestShards();
      const selector = createReplicaSelector(shards);

      let callCount = 0;
      const checkFn = vi.fn().mockImplementation(() => {
        callCount++;
        if (callCount % 2 === 0) {
          return Promise.reject(new Error('Failed'));
        }
        return Promise.resolve(10);
      });

      const checker = createHealthChecker(selector, shards, checkFn);
      await checker.checkAll();

      // Should complete without throwing
      expect(checkFn).toHaveBeenCalledTimes(9);
    });
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Edge Cases', () => {
  it('should handle shard with no replicas', () => {
    const shards = createTestShards({ withReplicas: false });
    const selector = createReplicaSelector(shards);

    const result = selector.select('shard-1', 'replicaPreferred');

    // Should fall back to primary
    expect(result).toBe('shard-1');
  });

  it('should handle empty shard list', () => {
    const selector = createReplicaSelector([]);
    const health = selector.getClusterHealth();

    expect(health.totalShards).toBe(0);
  });

  it('should handle rapid health state changes', () => {
    const shards = createTestShards();
    const config: HealthConfig = { failureThreshold: 2, successThreshold: 2 };
    const selector = createReplicaSelector(shards, config);

    // Rapid alternating
    for (let i = 0; i < 100; i++) {
      if (i % 2 === 0) {
        selector.recordSuccess('shard-1', 'replica-1-a', 10);
      } else {
        selector.recordFailure('shard-1', 'replica-1-a');
      }
    }

    // Should not crash and should have a valid state
    const state = selector.getHealthStateDebug('shard-1', 'replica-1-a');
    expect(['healthy', 'degraded', 'unhealthy', 'unknown']).toContain(state.health);
  });

  it('should handle very high latency values', () => {
    const shards = createTestShards();
    const selector = createReplicaSelector(shards);

    selector.recordSuccess('shard-1', 'replica-1-a', 999999);

    const health = selector.getShardHealth('shard-1');
    expect(health.latencyMs).toBeGreaterThan(0);
  });

  it('should maintain latency window limit', () => {
    const shards = createTestShards();
    const config: HealthConfig = { latencyWindowSize: 5 };
    const selector = createReplicaSelector(shards, config);

    // Record more latencies than window size
    for (let i = 0; i < 100; i++) {
      selector.recordSuccess('shard-1', 'replica-1-a', i);
    }

    const state = selector.getHealthStateDebug('shard-1', 'replica-1-a');
    expect(state.latencyMs.length).toBe(5);
    // Should only have the last 5 values
    expect(state.latencyMs).toEqual([95, 96, 97, 98, 99]);
  });
});
