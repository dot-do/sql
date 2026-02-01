/**
 * Tests for the Circuit Breaker
 *
 * Covers:
 * - State transitions (CLOSED -> OPEN -> HALF_OPEN -> CLOSED)
 * - Failure threshold behavior
 * - Recovery probing in half-open state
 * - Success threshold for closing circuit
 * - Failure window cleanup
 * - Metrics tracking
 * - Force open/close operations
 * - Integration with coordinator
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  createCircuitBreaker,
  CircuitBreakerOpenError,
  type CircuitBreaker,
} from '../circuit-breaker.js';
import { createDistributedTransactionCoordinator } from '../coordinator.js';
import { InMemoryTransactionLog } from '../types.js';
import type {
  ShardParticipantRPC,
  ParticipantVote,
  CoordinatorDecision,
} from '../types.js';
import type { ShardId } from '../../sharding/types.js';

// =============================================================================
// HELPERS
// =============================================================================

function shardId(id: string): ShardId {
  return id as unknown as ShardId;
}

function createTestRPC(overrides?: Partial<ShardParticipantRPC>): ShardParticipantRPC {
  return {
    async prepare(_shardId, _txnId, _ops) {
      return { vote: 'YES' as ParticipantVote };
    },
    async commit(_shardId, _txnId) {},
    async abort(_shardId, _txnId) {},
    async queryDecision(_coordinatorId, _txnId) {
      return 'PENDING' as CoordinatorDecision;
    },
    async execute(_shardId, _sql, _params) {
      return { rows: [], rowsAffected: 1 };
    },
    ...overrides,
  };
}

// =============================================================================
// CIRCUIT BREAKER UNIT TESTS
// =============================================================================

describe('CircuitBreaker', () => {
  let circuitBreaker: CircuitBreaker;

  beforeEach(() => {
    circuitBreaker = createCircuitBreaker({
      failureThreshold: 3,
      resetTimeoutMs: 1000,
      successThreshold: 2,
      failureWindowMs: 5000,
    });
  });

  describe('initial state', () => {
    it('should start in CLOSED state', () => {
      expect(circuitBreaker.getState('shard-a')).toBe('CLOSED');
    });

    it('should allow execution in initial state', () => {
      expect(circuitBreaker.canExecute('shard-a')).toBe(true);
    });

    it('should have zero metrics initially', () => {
      const metrics = circuitBreaker.getMetrics('shard-a');
      expect(metrics.totalFailures).toBe(0);
      expect(metrics.totalSuccesses).toBe(0);
      expect(metrics.recentFailures).toBe(0);
      expect(metrics.timeSinceOpenMs).toBeNull();
    });
  });

  describe('CLOSED -> OPEN transition', () => {
    it('should open circuit after failure threshold exceeded', () => {
      // Record failures up to threshold
      circuitBreaker.recordFailure('shard-a');
      expect(circuitBreaker.getState('shard-a')).toBe('CLOSED');

      circuitBreaker.recordFailure('shard-a');
      expect(circuitBreaker.getState('shard-a')).toBe('CLOSED');

      circuitBreaker.recordFailure('shard-a');
      expect(circuitBreaker.getState('shard-a')).toBe('OPEN');
    });

    it('should block execution when circuit is open', () => {
      // Trip the circuit
      for (let i = 0; i < 3; i++) {
        circuitBreaker.recordFailure('shard-a');
      }

      expect(circuitBreaker.canExecute('shard-a')).toBe(false);
    });

    it('should track timeSinceOpenMs when open', () => {
      // Trip the circuit
      for (let i = 0; i < 3; i++) {
        circuitBreaker.recordFailure('shard-a');
      }

      const metrics = circuitBreaker.getMetrics('shard-a');
      expect(metrics.timeSinceOpenMs).not.toBeNull();
      expect(metrics.timeSinceOpenMs).toBeGreaterThanOrEqual(0);
    });

    it('should isolate circuits per participant', () => {
      // Trip circuit for shard-a
      for (let i = 0; i < 3; i++) {
        circuitBreaker.recordFailure('shard-a');
      }

      expect(circuitBreaker.getState('shard-a')).toBe('OPEN');
      expect(circuitBreaker.getState('shard-b')).toBe('CLOSED');
      expect(circuitBreaker.canExecute('shard-b')).toBe(true);
    });
  });

  describe('OPEN -> HALF_OPEN transition', () => {
    it('should transition to HALF_OPEN after reset timeout', async () => {
      // Trip the circuit
      for (let i = 0; i < 3; i++) {
        circuitBreaker.recordFailure('shard-a');
      }
      expect(circuitBreaker.getState('shard-a')).toBe('OPEN');

      // Wait for reset timeout
      await new Promise((resolve) => setTimeout(resolve, 1100));

      // State should transition on next check
      expect(circuitBreaker.getState('shard-a')).toBe('HALF_OPEN');
    });

    it('should allow execution in HALF_OPEN state', async () => {
      // Trip the circuit
      for (let i = 0; i < 3; i++) {
        circuitBreaker.recordFailure('shard-a');
      }

      // Wait for reset timeout
      await new Promise((resolve) => setTimeout(resolve, 1100));

      expect(circuitBreaker.canExecute('shard-a')).toBe(true);
    });
  });

  describe('HALF_OPEN -> CLOSED transition', () => {
    it('should close circuit after success threshold in half-open', async () => {
      // Trip the circuit
      for (let i = 0; i < 3; i++) {
        circuitBreaker.recordFailure('shard-a');
      }

      // Wait for reset timeout
      await new Promise((resolve) => setTimeout(resolve, 1100));
      expect(circuitBreaker.getState('shard-a')).toBe('HALF_OPEN');

      // Record successes
      circuitBreaker.recordSuccess('shard-a');
      expect(circuitBreaker.getState('shard-a')).toBe('HALF_OPEN');

      circuitBreaker.recordSuccess('shard-a');
      expect(circuitBreaker.getState('shard-a')).toBe('CLOSED');
    });
  });

  describe('HALF_OPEN -> OPEN transition', () => {
    it('should reopen circuit on failure in half-open state', async () => {
      // Trip the circuit
      for (let i = 0; i < 3; i++) {
        circuitBreaker.recordFailure('shard-a');
      }

      // Wait for reset timeout
      await new Promise((resolve) => setTimeout(resolve, 1100));
      expect(circuitBreaker.getState('shard-a')).toBe('HALF_OPEN');

      // Any failure in half-open goes back to open
      circuitBreaker.recordFailure('shard-a');
      expect(circuitBreaker.getState('shard-a')).toBe('OPEN');
    });
  });

  describe('failure window', () => {
    it('should clear old failures outside window', async () => {
      // Create circuit breaker with short failure window
      const shortWindowBreaker = createCircuitBreaker({
        failureThreshold: 3,
        resetTimeoutMs: 1000,
        successThreshold: 2,
        failureWindowMs: 100, // Very short window
      });

      // Record 2 failures
      shortWindowBreaker.recordFailure('shard-a');
      shortWindowBreaker.recordFailure('shard-a');

      // Wait for failures to expire
      await new Promise((resolve) => setTimeout(resolve, 150));

      // This failure should not trigger open (previous ones expired)
      shortWindowBreaker.recordFailure('shard-a');
      expect(shortWindowBreaker.getState('shard-a')).toBe('CLOSED');
    });
  });

  describe('execute method', () => {
    it('should execute function and record success', async () => {
      const fn = vi.fn().mockResolvedValue('result');

      const result = await circuitBreaker.execute('shard-a', fn);

      expect(result.success).toBe(true);
      expect(result.result).toBe('result');
      expect(result.circuitOpen).toBe(false);
      expect(fn).toHaveBeenCalled();

      const metrics = circuitBreaker.getMetrics('shard-a');
      expect(metrics.totalSuccesses).toBe(1);
    });

    it('should execute function and record failure', async () => {
      const fn = vi.fn().mockRejectedValue(new Error('test error'));

      const result = await circuitBreaker.execute('shard-a', fn);

      expect(result.success).toBe(false);
      expect(result.error?.message).toBe('test error');
      expect(result.circuitOpen).toBe(false);

      const metrics = circuitBreaker.getMetrics('shard-a');
      expect(metrics.totalFailures).toBe(1);
    });

    it('should fail fast when circuit is open', async () => {
      // Trip the circuit
      for (let i = 0; i < 3; i++) {
        circuitBreaker.recordFailure('shard-a');
      }

      const fn = vi.fn().mockResolvedValue('result');
      const result = await circuitBreaker.execute('shard-a', fn);

      expect(result.success).toBe(false);
      expect(result.circuitOpen).toBe(true);
      expect(result.error).toBeInstanceOf(CircuitBreakerOpenError);
      expect(fn).not.toHaveBeenCalled();
    });
  });

  describe('force operations', () => {
    it('should force open a circuit', () => {
      expect(circuitBreaker.getState('shard-a')).toBe('CLOSED');

      circuitBreaker.forceOpen('shard-a');

      expect(circuitBreaker.getState('shard-a')).toBe('OPEN');
      expect(circuitBreaker.canExecute('shard-a')).toBe(false);
    });

    it('should force close a circuit', () => {
      // Trip the circuit
      for (let i = 0; i < 3; i++) {
        circuitBreaker.recordFailure('shard-a');
      }
      expect(circuitBreaker.getState('shard-a')).toBe('OPEN');

      circuitBreaker.forceClose('shard-a');

      expect(circuitBreaker.getState('shard-a')).toBe('CLOSED');
      expect(circuitBreaker.canExecute('shard-a')).toBe(true);
    });

    it('should reset a single circuit', () => {
      // Trip the circuit
      for (let i = 0; i < 3; i++) {
        circuitBreaker.recordFailure('shard-a');
      }

      circuitBreaker.reset('shard-a');

      expect(circuitBreaker.getState('shard-a')).toBe('CLOSED');
      const metrics = circuitBreaker.getMetrics('shard-a');
      expect(metrics.recentFailures).toBe(0);
    });

    it('should reset all circuits', () => {
      // Trip circuits for multiple shards
      for (let i = 0; i < 3; i++) {
        circuitBreaker.recordFailure('shard-a');
        circuitBreaker.recordFailure('shard-b');
      }

      circuitBreaker.resetAll();

      expect(circuitBreaker.getState('shard-a')).toBe('CLOSED');
      expect(circuitBreaker.getState('shard-b')).toBe('CLOSED');
    });
  });

  describe('state change callback', () => {
    it('should call onStateChange when state changes', () => {
      const onStateChange = vi.fn();
      const cbBreaker = createCircuitBreaker({
        failureThreshold: 2,
        onStateChange,
      });

      cbBreaker.recordFailure('shard-a');
      expect(onStateChange).not.toHaveBeenCalled();

      cbBreaker.recordFailure('shard-a');
      expect(onStateChange).toHaveBeenCalledWith('shard-a', 'CLOSED', 'OPEN');
    });
  });

  describe('metrics', () => {
    it('should track metrics correctly', () => {
      circuitBreaker.recordSuccess('shard-a');
      circuitBreaker.recordSuccess('shard-a');
      circuitBreaker.recordFailure('shard-a');

      const metrics = circuitBreaker.getMetrics('shard-a');
      expect(metrics.participantId).toBe('shard-a');
      expect(metrics.state).toBe('CLOSED');
      expect(metrics.totalSuccesses).toBe(2);
      expect(metrics.totalFailures).toBe(1);
      expect(metrics.recentFailures).toBe(1);
    });

    it('should return all metrics', () => {
      circuitBreaker.recordSuccess('shard-a');
      circuitBreaker.recordFailure('shard-b');

      const allMetrics = circuitBreaker.getAllMetrics();
      expect(allMetrics).toHaveLength(2);
      expect(allMetrics.map((m) => m.participantId).sort()).toEqual(['shard-a', 'shard-b']);
    });
  });
});

// =============================================================================
// COORDINATOR INTEGRATION TESTS
// =============================================================================

describe('DistributedTransactionCoordinator with CircuitBreaker', () => {
  let rpc: ShardParticipantRPC;
  let txnLog: InMemoryTransactionLog;

  const shardA = shardId('shard-a');
  const shardB = shardId('shard-b');

  beforeEach(() => {
    rpc = createTestRPC();
    txnLog = new InMemoryTransactionLog();
  });

  describe('circuit breaker integration', () => {
    it('should have circuit breaker enabled by default', () => {
      const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
        coordinatorId: 'coord-1',
      });

      expect(coordinator.getCircuitBreaker()).not.toBeNull();
    });

    it('should disable circuit breaker when configured', () => {
      const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
        coordinatorId: 'coord-1',
        circuitBreaker: { enabled: false },
      });

      expect(coordinator.getCircuitBreaker()).toBeNull();
    });

    it('should return empty metrics when disabled', () => {
      const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
        coordinatorId: 'coord-1',
        circuitBreaker: { enabled: false },
      });

      expect(coordinator.getCircuitBreakerMetrics()).toEqual([]);
    });

    it('should return metrics when enabled', async () => {
      const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
        coordinatorId: 'coord-1',
      });

      await coordinator.begin([shardA]);
      await coordinator.prepare();
      await coordinator.commit();

      const metrics = coordinator.getCircuitBreakerMetrics();
      expect(metrics.length).toBeGreaterThan(0);
    });

    it('should record success on successful prepare', async () => {
      const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
        coordinatorId: 'coord-1',
      });

      await coordinator.begin([shardA]);
      await coordinator.prepare();

      const cb = coordinator.getCircuitBreaker()!;
      const metrics = cb.getMetrics('shard-a');
      expect(metrics.totalSuccesses).toBeGreaterThan(0);
    });

    it('should record failure on failed prepare', async () => {
      let failCount = 0;
      const failingRpc = createTestRPC({
        async prepare() {
          failCount++;
          throw new Error('Shard unavailable');
        },
      });

      const coordinator = createDistributedTransactionCoordinator(failingRpc, txnLog, {
        coordinatorId: 'coord-1',
        maxRetries: 1,
        retryDelayMs: 1,
        prepareTimeoutMs: 5000,
      });

      await coordinator.begin([shardA]);
      const votes = await coordinator.prepare();

      expect(votes.get('shard-a')).toBe('TIMEOUT');

      const cb = coordinator.getCircuitBreaker()!;
      const metrics = cb.getMetrics('shard-a');
      expect(metrics.totalFailures).toBeGreaterThan(0);
    });

    it('should fail fast with TIMEOUT vote when circuit is open', async () => {
      const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
        coordinatorId: 'coord-1',
        circuitBreaker: {
          failureThreshold: 2,
          resetTimeoutMs: 30000,
        },
      });

      // Force open the circuit
      const cb = coordinator.getCircuitBreaker()!;
      cb.forceOpen('shard-a');

      await coordinator.begin([shardA]);
      const votes = await coordinator.prepare();

      // Should get TIMEOUT vote due to open circuit
      expect(votes.get('shard-a')).toBe('TIMEOUT');
    });

    it('should configure circuit breaker with custom settings', () => {
      const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
        coordinatorId: 'coord-1',
        circuitBreaker: {
          failureThreshold: 10,
          resetTimeoutMs: 60000,
          successThreshold: 5,
          failureWindowMs: 120000,
        },
      });

      // Can't directly verify config, but can verify circuit breaker exists
      expect(coordinator.getCircuitBreaker()).not.toBeNull();
    });

    it('should allow transaction to proceed after circuit recovers', async () => {
      const coordinator = createDistributedTransactionCoordinator(rpc, txnLog, {
        coordinatorId: 'coord-1',
        circuitBreaker: {
          failureThreshold: 2,
          resetTimeoutMs: 50, // Very short for test
          successThreshold: 1,
        },
      });

      const cb = coordinator.getCircuitBreaker()!;

      // Trip the circuit
      cb.forceOpen('shard-a');
      expect(cb.getState('shard-a')).toBe('OPEN');

      // Wait for reset timeout
      await new Promise((resolve) => setTimeout(resolve, 60));

      // Now should be half-open and allow request
      await coordinator.begin([shardA]);
      const votes = await coordinator.prepare();

      // Should succeed and close circuit
      expect(votes.get('shard-a')).toBe('YES');
      expect(cb.getState('shard-a')).toBe('CLOSED');
    });

    it('should maintain separate circuits for each shard', async () => {
      const failingRpc = createTestRPC({
        async prepare(shardIdStr) {
          if (shardIdStr === 'shard-a') {
            throw new Error('Shard A unavailable');
          }
          return { vote: 'YES' as ParticipantVote };
        },
      });

      const coordinator = createDistributedTransactionCoordinator(failingRpc, txnLog, {
        coordinatorId: 'coord-1',
        maxRetries: 1,
        retryDelayMs: 1,
        circuitBreaker: {
          failureThreshold: 2,
        },
      });

      const cb = coordinator.getCircuitBreaker()!;

      // Run multiple transactions to trip circuit for shard-a
      for (let i = 0; i < 2; i++) {
        await coordinator.begin([shardA, shardB]);
        await coordinator.prepare();
        await coordinator.rollback();
      }

      // Shard A should have high failure count, shard B should be healthy
      const metricsA = cb.getMetrics('shard-a');
      const metricsB = cb.getMetrics('shard-b');

      expect(metricsA.totalFailures).toBeGreaterThan(0);
      expect(metricsB.totalSuccesses).toBeGreaterThan(0);
    });
  });
});
