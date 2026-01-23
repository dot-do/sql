/**
 * TDD RED Phase: Circuit Breaker for DistributedExecutor
 *
 * These tests document the expected circuit breaker functionality that should be
 * added to the DistributedExecutor. All tests are marked with it() because
 * the circuit breaker implementation does not exist yet.
 *
 * Circuit Breaker Pattern:
 * - CLOSED: Normal operation, requests pass through
 * - OPEN: After N consecutive failures, rejects requests immediately
 * - HALF-OPEN: After timeout, allows a single test request
 *
 * Requirements:
 * 1. DistributedExecutor should have per-shard circuit breaker
 * 2. Circuit opens after N consecutive failures
 * 3. Circuit half-opens after timeout
 * 4. Circuit closes after successful request
 * 5. Failed shard doesn't cascade to others
 * 6. Circuit state is per-shard, not global
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

import {
  DistributedExecutor,
  MockShardRPC,
  createExecutor,
  type ShardRPC,
  type ExecutorConfig,
  type ExecuteOptions,
  type CircuitBreakerConfig,
  type CircuitBreakerState,
  type CircuitStateChangeEvent,
} from '../executor.js';

import {
  createReplicaSelector,
  type DefaultReplicaSelector,
} from '../replica.js';

import {
  createVSchema,
  hashVindex,
  shardedTable,
  shard,
  createShardId,
  type ShardConfig,
  type ExecutionPlan,
  type ShardResult,
  type ShardId,
} from '../types.js';

import { createRouter, type QueryRouter } from '../router.js';

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Extended executor interface for testing internal state.
 * The DistributedExecutor class exposes these methods publicly,
 * but TypeScript sometimes needs explicit typing for test assertions.
 */
interface TestableExecutor extends DistributedExecutor {
  getCircuitState(shardId: string): CircuitBreakerState;
  getCircuitStates(): Map<string, CircuitBreakerState>;
  initializeCircuitStates(shardIds: string[]): void;
  forceCircuitOpen(shardId: string): void;
  forceCircuitClose(shardId: string): void;
}

/**
 * Interface for accessing internal config (for test assertions only)
 */
interface ExecutorInternals {
  config: Required<Omit<ExecutorConfig, 'circuitBreaker'>> & { circuitBreaker?: CircuitBreakerConfig };
}

/**
 * Mock RPC that can be configured to fail for specific shards
 */
class FailingMockShardRPC implements ShardRPC {
  private readonly failingShards = new Set<string>();
  private readonly shardData = new Map<string, { columns: string[]; rows: unknown[][] }>();
  public callCount = new Map<string, number>();

  setShardData(shardId: string, columns: string[], rows: unknown[][]): void {
    this.shardData.set(shardId, { columns, rows });
  }

  setShardToFail(shardId: string): void {
    this.failingShards.add(shardId);
  }

  clearShardFailure(shardId: string): void {
    this.failingShards.delete(shardId);
  }

  async execute(
    shardId: string,
    replicaId: string | undefined,
    sql: string,
    params?: unknown[],
    options?: ExecuteOptions
  ): Promise<ShardResult> {
    // Track call count
    const count = this.callCount.get(shardId) ?? 0;
    this.callCount.set(shardId, count + 1);

    if (this.failingShards.has(shardId)) {
      throw new Error(`Shard ${shardId} is unavailable`);
    }

    const data = this.shardData.get(shardId) ?? { columns: [], rows: [] };
    return {
      shardId: createShardId(shardId),
      columns: data.columns,
      rows: data.rows,
      rowCount: data.rows.length,
      executionTimeMs: 5,
    };
  }

  async *executeStream(
    shardId: string,
    replicaId: string | undefined,
    sql: string,
    params?: unknown[],
    options?: ExecuteOptions
  ): AsyncIterable<ShardResult> {
    yield await this.execute(shardId, replicaId, sql, params, options);
  }
}

/**
 * Creates a test setup with 3 shards
 */
function createTestSetup() {
  const shards: ShardConfig[] = [
    shard(createShardId('shard-1'), 'do-ns-1'),
    shard(createShardId('shard-2'), 'do-ns-2'),
    shard(createShardId('shard-3'), 'do-ns-3'),
  ];

  const vschema = createVSchema({
    users: shardedTable('tenant_id', hashVindex()),
  }, shards);

  const rpc = new FailingMockShardRPC();
  rpc.setShardData('shard-1', ['id', 'name'], [[1, 'Alice']]);
  rpc.setShardData('shard-2', ['id', 'name'], [[2, 'Bob']]);
  rpc.setShardData('shard-3', ['id', 'name'], [[3, 'Charlie']]);

  const selector = createReplicaSelector(shards);
  const router = createRouter(vschema);

  return { shards, vschema, rpc, selector, router };
}

// =============================================================================
// TEST SUITE: Circuit Breaker for DistributedExecutor
// =============================================================================

describe('Circuit Breaker for DistributedExecutor', () => {
  // -------------------------------------------------------------------------
  // 1. DistributedExecutor should have per-shard circuit breaker
  // -------------------------------------------------------------------------
  describe('1. Per-shard circuit breaker existence', () => {
    it('should expose circuit breaker configuration in ExecutorConfig', () => {
      const { rpc, selector } = createTestSetup();

      // Expected: ExecutorConfig should have circuitBreaker property defined in its type
      // This test verifies the type exists and is properly configured
      const config: ExecutorConfig = {
        maxParallelShards: 10,
        defaultTimeoutMs: 5000,
        circuitBreaker: {
          failureThreshold: 5,
          resetTimeoutMs: 30000,
        },
      };

      const executor = createExecutor(rpc, selector, config) as TestableExecutor;

      // Executor should store and use the circuit breaker config
      // Access the internal config to verify it was stored
      const internals = executor as unknown as ExecutorInternals;
      expect(internals.config.circuitBreaker).toBeDefined();
      expect(internals.config.circuitBreaker?.failureThreshold).toBe(5);
      expect(internals.config.circuitBreaker?.resetTimeoutMs).toBe(30000);
    });

    it('should have getCircuitState method for shard introspection', () => {
      const { rpc, selector } = createTestSetup();

      const executor = createExecutor(rpc, selector, {
        circuitBreaker: {
          failureThreshold: 5,
          resetTimeoutMs: 30000,
        },
      }) as TestableExecutor;

      // Expected: executor should have getCircuitState method
      const state = executor.getCircuitState('shard-1');
      expect(state).toBeDefined();
      expect(state.state).toBe('CLOSED');
    });

    it('should initialize circuit breakers for all shards in CLOSED state', () => {
      const { rpc, selector, shards } = createTestSetup();

      const executor = createExecutor(rpc, selector, {
        circuitBreaker: {
          failureThreshold: 5,
          resetTimeoutMs: 30000,
        },
      }) as TestableExecutor;

      // Each shard should have its own circuit breaker initialized
      for (const shardConfig of shards) {
        const state = executor.getCircuitState(shardConfig.id);
        expect(state.state).toBe('CLOSED');
        expect(state.failureCount).toBe(0);
      }
    });
  });

  // -------------------------------------------------------------------------
  // 2. Circuit opens after N consecutive failures
  // -------------------------------------------------------------------------
  describe('2. Circuit opens after N consecutive failures', () => {
    it('should open circuit after failureThreshold consecutive failures', async () => {
      const { rpc, selector, router } = createTestSetup();
      const failureThreshold = 3;

      const executor = createExecutor(rpc, selector, {
        failFast: true,
        retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
        circuitBreaker: {
          failureThreshold,
          resetTimeoutMs: 30000,
        },
      }) as TestableExecutor;

      // Configure shard-1 to fail
      rpc.setShardToFail('shard-1');

      // Make failureThreshold requests that will fail
      for (let i = 0; i < failureThreshold; i++) {
        const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
        // Force the plan to target shard-1
        plan.shardPlans[0].shardId = createShardId('shard-1');

        try {
          await executor.execute(plan);
        } catch {
          // Expected to fail
        }
      }

      // Circuit should now be OPEN
      const state = executor.getCircuitState('shard-1');
      expect(state.state).toBe('OPEN');
      expect(state.failureCount).toBe(failureThreshold);
    });

    it('should reject requests immediately when circuit is OPEN', async () => {
      const { rpc, selector, router } = createTestSetup();

      const executor = createExecutor(rpc, selector, {
        failFast: true,
        retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
        circuitBreaker: {
          failureThreshold: 2,
          resetTimeoutMs: 30000,
        },
      }) as TestableExecutor;

      // Open the circuit for shard-1
      rpc.setShardToFail('shard-1');
      for (let i = 0; i < 2; i++) {
        const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
        plan.shardPlans[0].shardId = createShardId('shard-1');
        try {
          await executor.execute(plan);
        } catch {
          // Expected
        }
      }

      // Reset call count to verify no RPC calls are made
      rpc.callCount.clear();

      // Next request should be rejected immediately without calling RPC
      const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
      plan.shardPlans[0].shardId = createShardId('shard-1');

      await expect(executor.execute(plan)).rejects.toThrow(/circuit.*open/i);

      // Verify no RPC call was made
      expect(rpc.callCount.get('shard-1') ?? 0).toBe(0);
    });

    it('should track failures independently per shard', async () => {
      const { rpc, selector, router } = createTestSetup();

      const executor = createExecutor(rpc, selector, {
        failFast: true,
        retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
        circuitBreaker: {
          failureThreshold: 3,
          resetTimeoutMs: 30000,
        },
      }) as TestableExecutor;

      // Fail shard-1 twice
      rpc.setShardToFail('shard-1');
      for (let i = 0; i < 2; i++) {
        const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
        plan.shardPlans[0].shardId = createShardId('shard-1');
        try {
          await executor.execute(plan);
        } catch {
          // Expected
        }
      }

      // Fail shard-2 once
      rpc.setShardToFail('shard-2');
      const plan2 = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 2');
      plan2.shardPlans[0].shardId = createShardId('shard-2');
      try {
        await executor.execute(plan2);
      } catch {
        // Expected
      }

      // Both circuits should still be CLOSED (under threshold)
      expect(executor.getCircuitState('shard-1').state).toBe('CLOSED');
      expect(executor.getCircuitState('shard-1').failureCount).toBe(2);
      expect(executor.getCircuitState('shard-2').state).toBe('CLOSED');
      expect(executor.getCircuitState('shard-2').failureCount).toBe(1);
    });

    it('should reset failure count on successful request', async () => {
      const { rpc, selector, router } = createTestSetup();

      const executor = createExecutor(rpc, selector, {
        failFast: true,
        retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
        circuitBreaker: {
          failureThreshold: 5,
          resetTimeoutMs: 30000,
        },
      }) as TestableExecutor;

      // Fail shard-1 twice
      rpc.setShardToFail('shard-1');
      for (let i = 0; i < 2; i++) {
        const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
        plan.shardPlans[0].shardId = createShardId('shard-1');
        try {
          await executor.execute(plan);
        } catch {
          // Expected
        }
      }

      expect(executor.getCircuitState('shard-1').failureCount).toBe(2);

      // Now succeed
      rpc.clearShardFailure('shard-1');
      const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
      plan.shardPlans[0].shardId = createShardId('shard-1');
      await executor.execute(plan);

      // Failure count should be reset
      expect(executor.getCircuitState('shard-1').failureCount).toBe(0);
      expect(executor.getCircuitState('shard-1').state).toBe('CLOSED');
    });
  });

  // -------------------------------------------------------------------------
  // 3. Circuit half-opens after timeout
  // -------------------------------------------------------------------------
  describe('3. Circuit half-opens after timeout', () => {
    it('should transition to HALF_OPEN after resetTimeoutMs', async () => {
      const { rpc, selector, router } = createTestSetup();
      const resetTimeoutMs = 5000;

      // Mock Date.now to control time
      let currentTime = 1000;
      const originalDateNow = Date.now;
      Date.now = vi.fn(() => currentTime);

      try {
        const executor = createExecutor(rpc, selector, {
          failFast: true,
          retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
          circuitBreaker: {
            failureThreshold: 2,
            resetTimeoutMs,
          },
        }) as TestableExecutor;

        // Open the circuit
        rpc.setShardToFail('shard-1');
        for (let i = 0; i < 2; i++) {
          const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
          plan.shardPlans[0].shardId = createShardId('shard-1');
          try {
            await executor.execute(plan);
          } catch {
            // Expected
          }
        }

        expect(executor.getCircuitState('shard-1').state).toBe('OPEN');

        // Advance time past reset timeout
        currentTime += resetTimeoutMs + 100;

        // Circuit should be HALF_OPEN (or ready to transition on next request)
        const state = executor.getCircuitState('shard-1');
        expect(state.state).toBe('HALF_OPEN');
      } finally {
        Date.now = originalDateNow;
      }
    });

    it('should allow single test request in HALF_OPEN state', async () => {
      const { rpc, selector, router } = createTestSetup();
      const resetTimeoutMs = 5000;

      // Mock Date.now to control time
      let currentTime = 1000;
      const originalDateNow = Date.now;
      Date.now = vi.fn(() => currentTime);

      try {
        const executor = createExecutor(rpc, selector, {
          failFast: true,
          retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
          circuitBreaker: {
            failureThreshold: 2,
            resetTimeoutMs,
          },
        }) as TestableExecutor;

        // Open the circuit
        rpc.setShardToFail('shard-1');
        for (let i = 0; i < 2; i++) {
          const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
          plan.shardPlans[0].shardId = createShardId('shard-1');
          try {
            await executor.execute(plan);
          } catch {
            // Expected
          }
        }

        // Advance time past reset timeout
        currentTime += resetTimeoutMs + 100;

        // Clear the failure so the test request succeeds
        rpc.clearShardFailure('shard-1');
        rpc.callCount.clear();

        // Make a request - it should be allowed through
        const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
        plan.shardPlans[0].shardId = createShardId('shard-1');
        await executor.execute(plan);

        // Verify the RPC was called
        expect(rpc.callCount.get('shard-1')).toBe(1);
      } finally {
        Date.now = originalDateNow;
      }
    });
  });

  // -------------------------------------------------------------------------
  // 4. Circuit closes after successful request
  // -------------------------------------------------------------------------
  describe('4. Circuit closes after successful request', () => {
    it('should close circuit after successful request in HALF_OPEN state', async () => {
      const { rpc, selector, router } = createTestSetup();
      const resetTimeoutMs = 5000;

      // Mock Date.now to control time
      let currentTime = 1000;
      const originalDateNow = Date.now;
      Date.now = vi.fn(() => currentTime);

      try {
        const executor = createExecutor(rpc, selector, {
          failFast: true,
          retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
          circuitBreaker: {
            failureThreshold: 2,
            resetTimeoutMs,
          },
        }) as TestableExecutor;

        // Open the circuit
        rpc.setShardToFail('shard-1');
        for (let i = 0; i < 2; i++) {
          const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
          plan.shardPlans[0].shardId = createShardId('shard-1');
          try {
            await executor.execute(plan);
          } catch {
            // Expected
          }
        }

        // Advance time past reset timeout to get to HALF_OPEN
        currentTime += resetTimeoutMs + 100;

        // Service recovers - clear the failure
        rpc.clearShardFailure('shard-1');

        // Make a successful request
        const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
        plan.shardPlans[0].shardId = createShardId('shard-1');
        await executor.execute(plan);

        // Circuit should be CLOSED
        const state = executor.getCircuitState('shard-1');
        expect(state.state).toBe('CLOSED');
        expect(state.failureCount).toBe(0);
      } finally {
        Date.now = originalDateNow;
      }
    });

    it('should re-open circuit if test request fails in HALF_OPEN state', async () => {
      const { rpc, selector, router } = createTestSetup();
      const resetTimeoutMs = 5000;

      // Mock Date.now to control time
      let currentTime = 1000;
      const originalDateNow = Date.now;
      Date.now = vi.fn(() => currentTime);

      try {
        const executor = createExecutor(rpc, selector, {
          failFast: true,
          retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
          circuitBreaker: {
            failureThreshold: 2,
            resetTimeoutMs,
          },
        }) as TestableExecutor;

        // Open the circuit
        rpc.setShardToFail('shard-1');
        for (let i = 0; i < 2; i++) {
          const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
          plan.shardPlans[0].shardId = createShardId('shard-1');
          try {
            await executor.execute(plan);
          } catch {
            // Expected
          }
        }

        // Advance time past reset timeout to get to HALF_OPEN
        currentTime += resetTimeoutMs + 100;

        // Service still failing - don't clear the failure
        // Make a request that will fail
        const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
        plan.shardPlans[0].shardId = createShardId('shard-1');
        try {
          await executor.execute(plan);
        } catch {
          // Expected
        }

        // Circuit should be OPEN again
        const state = executor.getCircuitState('shard-1');
        expect(state.state).toBe('OPEN');
      } finally {
        Date.now = originalDateNow;
      }
    });

    it('should support successThreshold for closing from HALF_OPEN', async () => {
      const { rpc, selector, router } = createTestSetup();
      const resetTimeoutMs = 5000;
      const successThreshold = 3;

      // Mock Date.now to control time
      let currentTime = 1000;
      const originalDateNow = Date.now;
      Date.now = vi.fn(() => currentTime);

      try {
        const executor = createExecutor(rpc, selector, {
          failFast: true,
          retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
          circuitBreaker: {
            failureThreshold: 2,
            resetTimeoutMs,
            successThreshold,
          },
        }) as TestableExecutor;

        // Open the circuit
        rpc.setShardToFail('shard-1');
        for (let i = 0; i < 2; i++) {
          const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
          plan.shardPlans[0].shardId = createShardId('shard-1');
          try {
            await executor.execute(plan);
          } catch {
            // Expected
          }
        }

        // Advance time past reset timeout
        currentTime += resetTimeoutMs + 100;

        // Service recovers
        rpc.clearShardFailure('shard-1');

        // Make successThreshold - 1 successful requests
        for (let i = 0; i < successThreshold - 1; i++) {
          const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
          plan.shardPlans[0].shardId = createShardId('shard-1');
          await executor.execute(plan);
        }

        // Still in HALF_OPEN
        expect(executor.getCircuitState('shard-1').state).toBe('HALF_OPEN');

        // One more success
        const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
        plan.shardPlans[0].shardId = createShardId('shard-1');
        await executor.execute(plan);

        // Now CLOSED
        expect(executor.getCircuitState('shard-1').state).toBe('CLOSED');
      } finally {
        Date.now = originalDateNow;
      }
    });
  });

  // -------------------------------------------------------------------------
  // 5. Failed shard doesn't cascade to others
  // -------------------------------------------------------------------------
  describe('5. Failed shard does not cascade to others', () => {
    it('should continue serving requests to healthy shards when one is OPEN', async () => {
      const { rpc, selector, router } = createTestSetup();

      const executor = createExecutor(rpc, selector, {
        failFast: false, // Don't fail fast - allow partial results
        retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
        circuitBreaker: {
          failureThreshold: 2,
          resetTimeoutMs: 30000,
        },
      }) as TestableExecutor;

      // Open circuit for shard-1
      rpc.setShardToFail('shard-1');
      for (let i = 0; i < 2; i++) {
        const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
        plan.shardPlans[0].shardId = createShardId('shard-1');
        try {
          await executor.execute(plan);
        } catch {
          // Expected
        }
      }

      expect(executor.getCircuitState('shard-1').state).toBe('OPEN');

      // Scatter query across all shards should still return results from healthy shards
      const plan = router.createExecutionPlan('SELECT * FROM users');
      const result = await executor.execute(plan);

      // Should have results from shard-2 and shard-3
      expect(result.contributingShards).toContain('shard-2');
      expect(result.contributingShards).toContain('shard-3');
      expect(result.contributingShards).not.toContain('shard-1');

      // Results should include data from healthy shards
      expect(result.rows.length).toBeGreaterThan(0);
    });

    it('should report partial failures when circuit is OPEN for some shards', async () => {
      const { rpc, selector, router } = createTestSetup();

      const executor = createExecutor(rpc, selector, {
        failFast: false,
        retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
        circuitBreaker: {
          failureThreshold: 2,
          resetTimeoutMs: 30000,
        },
      }) as TestableExecutor;

      // Open circuit for shard-1
      rpc.setShardToFail('shard-1');
      for (let i = 0; i < 2; i++) {
        const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
        plan.shardPlans[0].shardId = createShardId('shard-1');
        try {
          await executor.execute(plan);
        } catch {
          // Expected
        }
      }

      // Scatter query
      const plan = router.createExecutionPlan('SELECT * FROM users');
      const result = await executor.execute(plan);

      // Should report partial failure for shard-1
      expect(result.partialFailures).toBeDefined();
      expect(result.partialFailures?.some(e => e.message.includes('circuit') || e.message.includes('open'))).toBe(true);
    });

    it('should not increment failure count for other shards when one fails', async () => {
      const { rpc, selector, router } = createTestSetup();

      const executor = createExecutor(rpc, selector, {
        failFast: false,
        retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
        circuitBreaker: {
          failureThreshold: 10,
          resetTimeoutMs: 30000,
        },
      }) as TestableExecutor;

      // Make shard-1 fail
      rpc.setShardToFail('shard-1');

      // Execute multiple scatter queries
      for (let i = 0; i < 5; i++) {
        const plan = router.createExecutionPlan('SELECT * FROM users');
        await executor.execute(plan);
      }

      // shard-1 should have accumulated failures
      expect(executor.getCircuitState('shard-1').failureCount).toBe(5);

      // shard-2 and shard-3 should have no failures
      expect(executor.getCircuitState('shard-2').failureCount).toBe(0);
      expect(executor.getCircuitState('shard-3').failureCount).toBe(0);
    });
  });

  // -------------------------------------------------------------------------
  // 6. Circuit state is per-shard, not global
  // -------------------------------------------------------------------------
  describe('6. Circuit state is per-shard, not global', () => {
    it('should maintain independent circuit states for each shard', async () => {
      const { rpc, selector, router } = createTestSetup();

      const executor = createExecutor(rpc, selector, {
        failFast: true,
        retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
        circuitBreaker: {
          failureThreshold: 2,
          resetTimeoutMs: 30000,
        },
      }) as TestableExecutor;

      // Open circuit for shard-1
      rpc.setShardToFail('shard-1');
      for (let i = 0; i < 2; i++) {
        const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
        plan.shardPlans[0].shardId = createShardId('shard-1');
        try {
          await executor.execute(plan);
        } catch {
          // Expected
        }
      }

      // Verify states
      expect(executor.getCircuitState('shard-1').state).toBe('OPEN');
      expect(executor.getCircuitState('shard-2').state).toBe('CLOSED');
      expect(executor.getCircuitState('shard-3').state).toBe('CLOSED');
    });

    it('should allow queries to shard-2 when shard-1 circuit is OPEN', async () => {
      const { rpc, selector, router } = createTestSetup();

      const executor = createExecutor(rpc, selector, {
        failFast: true,
        retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
        circuitBreaker: {
          failureThreshold: 2,
          resetTimeoutMs: 30000,
        },
      }) as TestableExecutor;

      // Open circuit for shard-1
      rpc.setShardToFail('shard-1');
      for (let i = 0; i < 2; i++) {
        const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
        plan.shardPlans[0].shardId = createShardId('shard-1');
        try {
          await executor.execute(plan);
        } catch {
          // Expected
        }
      }

      // Queries to shard-2 should work
      const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 2');
      plan.shardPlans[0].shardId = createShardId('shard-2');
      const result = await executor.execute(plan);

      expect(result.rows.length).toBeGreaterThan(0);
      expect(result.contributingShards).toContain('shard-2');
    });

    it('should expose all circuit states via getCircuitStates method', () => {
      const { rpc, selector, shards } = createTestSetup();

      const executor = createExecutor(rpc, selector, {
        circuitBreaker: {
          failureThreshold: 5,
          resetTimeoutMs: 30000,
        },
      }) as TestableExecutor;

      // Initialize circuit states for all shards
      executor.initializeCircuitStates(shards.map(s => s.id));

      // Expected: executor should have getCircuitStates method returning all states
      const states = executor.getCircuitStates();

      expect(states).toBeInstanceOf(Map);
      expect(states.size).toBe(shards.length);

      for (const shardConfig of shards) {
        expect(states.has(shardConfig.id)).toBe(true);
        expect(states.get(shardConfig.id)?.state).toBe('CLOSED');
      }
    });

    it('should support manual circuit manipulation for testing/admin', () => {
      const { rpc, selector } = createTestSetup();

      const executor = createExecutor(rpc, selector, {
        circuitBreaker: {
          failureThreshold: 5,
          resetTimeoutMs: 30000,
        },
      }) as TestableExecutor;

      // Expected: executor should have forceCircuitOpen and forceCircuitClose methods
      executor.forceCircuitOpen('shard-1');
      expect(executor.getCircuitState('shard-1').state).toBe('OPEN');

      executor.forceCircuitClose('shard-1');
      expect(executor.getCircuitState('shard-1').state).toBe('CLOSED');
    });

    it('should emit circuit state change events', async () => {
      const { rpc, selector, router } = createTestSetup();

      const executor = createExecutor(rpc, selector, {
        failFast: true,
        retry: { maxAttempts: 1, backoffMs: 0, maxBackoffMs: 0 },
        circuitBreaker: {
          failureThreshold: 2,
          resetTimeoutMs: 30000,
        },
      }) as TestableExecutor;

      const events: CircuitStateChangeEvent[] = [];

      // Expected: executor should emit 'circuitStateChange' events
      executor.on('circuitStateChange', (event: CircuitStateChangeEvent) => {
        events.push(event);
      });

      // Open circuit for shard-1
      rpc.setShardToFail('shard-1');
      for (let i = 0; i < 2; i++) {
        const plan = router.createExecutionPlan('SELECT * FROM users WHERE tenant_id = 1');
        plan.shardPlans[0].shardId = createShardId('shard-1');
        try {
          await executor.execute(plan);
        } catch {
          // Expected
        }
      }

      // Should have received event for CLOSED -> OPEN transition
      expect(events.length).toBe(1);
      expect(events[0]).toEqual({
        shardId: 'shard-1',
        oldState: 'CLOSED',
        newState: 'OPEN',
      });
    });
  });
});
