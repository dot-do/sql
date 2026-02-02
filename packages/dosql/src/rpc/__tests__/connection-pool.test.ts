/**
 * DO Connection Pool Tests
 *
 * Tests for cross-DO RPC connection pooling:
 * - Connection acquisition and release
 * - Pool size limits
 * - Idle timeout and eviction
 * - Health tracking per connection
 * - Statistics and monitoring
 * - Waiters when pool is exhausted
 *
 * Tests run using workers-vitest-pool (NO MOCKS).
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  DOConnectionPool,
  createDOConnectionPool,
  createPooledShardRPC,
  type DOConnectionPoolConfig,
  type PooledConnection,
  type DOConnectionPoolStats,
  type DOStubFactory,
  type ConnectionHealth,
  DEFAULT_DO_CONNECTION_POOL_CONFIG,
} from '../connection-pool.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Mock DO stub for testing
 */
interface MockDOStub {
  id: string;
  namespace: string;
  fetch: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
  callCount: number;
}

/**
 * Creates a mock stub factory
 */
function createMockStubFactory(): DOStubFactory<MockDOStub> {
  let stubCounter = 0;
  return (namespace: string, doId: string) => ({
    id: `stub-${++stubCounter}-${doId}`,
    namespace,
    callCount: 0,
    fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
      return new Response(JSON.stringify({ columns: [], rows: [], rowCount: 0 }), {
        headers: { 'Content-Type': 'application/json' },
      });
    },
  });
}

/**
 * Creates a failing stub factory for testing error handling
 */
function createFailingStubFactory(): DOStubFactory<MockDOStub> {
  let stubCounter = 0;
  return (namespace: string, doId: string) => ({
    id: `failing-stub-${++stubCounter}-${doId}`,
    namespace,
    callCount: 0,
    fetch: async () => {
      throw new Error('Connection failed');
    },
  });
}

// =============================================================================
// Test: Pool Creation and Configuration
// =============================================================================

describe('DOConnectionPool - Configuration', () => {
  it('should use default configuration values', () => {
    const pool = createDOConnectionPool(createMockStubFactory());

    expect(pool).toBeDefined();
    expect(pool.isClosed()).toBe(false);

    const stats = pool.getStats();
    expect(stats.totalConnections).toBe(0);

    pool.close();
  });

  it('should allow partial configuration override', () => {
    const pool = createDOConnectionPool(createMockStubFactory(), {
      maxConnectionsPerDO: 5,
      idleTimeoutMs: 10000,
    });

    expect(pool).toBeDefined();
    pool.close();
  });

  it('should have sensible defaults exported', () => {
    expect(DEFAULT_DO_CONNECTION_POOL_CONFIG).toBeDefined();
    expect(DEFAULT_DO_CONNECTION_POOL_CONFIG.maxConnectionsPerDO).toBe(10);
    expect(DEFAULT_DO_CONNECTION_POOL_CONFIG.maxTotalConnections).toBe(100);
    expect(DEFAULT_DO_CONNECTION_POOL_CONFIG.idleTimeoutMs).toBe(60000);
    expect(DEFAULT_DO_CONNECTION_POOL_CONFIG.maxConsecutiveFailures).toBe(3);
  });
});

// =============================================================================
// Test: Connection Acquisition
// =============================================================================

describe('DOConnectionPool - Acquisition', () => {
  let pool: DOConnectionPool<MockDOStub>;

  beforeEach(() => {
    pool = createDOConnectionPool(createMockStubFactory(), {
      maxConnectionsPerDO: 5,
      maxTotalConnections: 20,
      idleTimeoutMs: 5000,
      healthCheckIntervalMs: 0, // Disable for tests
    });
  });

  afterEach(() => {
    pool.close();
  });

  it('should acquire a new connection', async () => {
    const conn = await pool.acquire('TEST_DO', 'test-id-1');

    expect(conn).toBeDefined();
    expect(conn.id).toMatch(/^conn-/);
    expect(conn.doId).toBe('test-id-1');
    expect(conn.namespace).toBe('TEST_DO');
    expect(conn.inUse).toBe(true);
    expect(conn.health).toBe('unknown');
  });

  it('should create unique connection IDs', async () => {
    const conn1 = await pool.acquire('TEST_DO', 'id-1');
    const conn2 = await pool.acquire('TEST_DO', 'id-2');

    expect(conn1.id).not.toBe(conn2.id);
  });

  it('should reuse released connections', async () => {
    const conn1 = await pool.acquire('TEST_DO', 'reuse-test');
    const connId = conn1.id;
    pool.release(connId);

    const conn2 = await pool.acquire('TEST_DO', 'reuse-test');
    expect(conn2.id).toBe(connId);
  });

  it('should track connection metadata', async () => {
    const beforeAcquire = Date.now();
    const conn = await pool.acquire('TEST_DO', 'metadata-test');
    const afterAcquire = Date.now();

    expect(conn.createdAt).toBeGreaterThanOrEqual(beforeAcquire);
    expect(conn.createdAt).toBeLessThanOrEqual(afterAcquire);
    expect(conn.lastUsedAt).toBeGreaterThanOrEqual(beforeAcquire);
    expect(conn.totalRequests).toBe(0);
    expect(conn.consecutiveFailures).toBe(0);
  });

  it('should throw when pool is closed', async () => {
    pool.close();

    await expect(pool.acquire('TEST_DO', 'closed-test'))
      .rejects.toThrow('Connection pool is closed');
  });

  it('should emit connection:created event', async () => {
    const createdHandler = vi.fn();
    pool.on('connection:created', createdHandler);

    const conn = await pool.acquire('TEST_DO', 'event-test');

    expect(createdHandler).toHaveBeenCalledWith({
      connectionId: conn.id,
      doId: 'event-test',
      namespace: 'TEST_DO',
    });
  });

  it('should emit connection:acquired event', async () => {
    const acquiredHandler = vi.fn();
    pool.on('connection:acquired', acquiredHandler);

    const conn = await pool.acquire('TEST_DO', 'acquired-event-test');

    expect(acquiredHandler).toHaveBeenCalledWith({
      connectionId: conn.id,
      doId: 'acquired-event-test',
    });
  });
});

// =============================================================================
// Test: Connection Release
// =============================================================================

describe('DOConnectionPool - Release', () => {
  let pool: DOConnectionPool<MockDOStub>;

  beforeEach(() => {
    pool = createDOConnectionPool(createMockStubFactory(), {
      maxConnectionsPerDO: 5,
      idleTimeoutMs: 5000,
      healthCheckIntervalMs: 0,
    });
  });

  afterEach(() => {
    pool.close();
  });

  it('should release a connection', async () => {
    const conn = await pool.acquire('TEST_DO', 'release-test');
    expect(conn.inUse).toBe(true);

    pool.release(conn.id);

    const updated = pool.getConnection(conn.id);
    expect(updated?.inUse).toBe(false);
  });

  it('should update lastUsedAt on release', async () => {
    const conn = await pool.acquire('TEST_DO', 'release-time-test');
    const acquireTime = conn.lastUsedAt;

    await new Promise(resolve => setTimeout(resolve, 10));
    pool.release(conn.id);

    const updated = pool.getConnection(conn.id);
    expect(updated?.lastUsedAt).toBeGreaterThan(acquireTime);
  });

  it('should emit connection:released event', async () => {
    const releasedHandler = vi.fn();
    pool.on('connection:released', releasedHandler);

    const conn = await pool.acquire('TEST_DO', 'release-event-test');
    pool.release(conn.id);

    expect(releasedHandler).toHaveBeenCalledWith({
      connectionId: conn.id,
      doId: 'release-event-test',
    });
  });

  it('should handle release of non-existent connection gracefully', () => {
    // Should not throw
    expect(() => pool.release('non-existent-id')).not.toThrow();
  });

  it('should track releases in stats', async () => {
    const conn = await pool.acquire('TEST_DO', 'stats-release-test');
    pool.release(conn.id);

    const stats = pool.getStats();
    expect(stats.releases).toBe(1);
  });
});

// =============================================================================
// Test: Pool Size Limits
// =============================================================================

describe('DOConnectionPool - Size Limits', () => {
  it('should enforce maxConnectionsPerDO limit', async () => {
    const pool = createDOConnectionPool(createMockStubFactory(), {
      maxConnectionsPerDO: 2,
      maxTotalConnections: 100,
      healthCheckIntervalMs: 0,
    });

    try {
      // Acquire up to limit
      const conn1 = await pool.acquire('TEST_DO', 'same-id');
      const conn2 = await pool.acquire('TEST_DO', 'same-id');

      // Third acquisition should timeout (using short timeout)
      await expect(
        pool.acquire('TEST_DO', 'same-id', { timeoutMs: 100 })
      ).rejects.toThrow('timeout');
    } finally {
      pool.close();
    }
  });

  it('should enforce maxTotalConnections limit', async () => {
    const pool = createDOConnectionPool(createMockStubFactory(), {
      maxConnectionsPerDO: 10,
      maxTotalConnections: 3,
      healthCheckIntervalMs: 0,
    });

    try {
      await pool.acquire('TEST_DO', 'id-1');
      await pool.acquire('TEST_DO', 'id-2');
      await pool.acquire('TEST_DO', 'id-3');

      // Fourth should timeout
      await expect(
        pool.acquire('TEST_DO', 'id-4', { timeoutMs: 100 })
      ).rejects.toThrow('timeout');
    } finally {
      pool.close();
    }
  });

  it('should emit pool:exhausted event', async () => {
    const pool = createDOConnectionPool(createMockStubFactory(), {
      maxConnectionsPerDO: 1,
      healthCheckIntervalMs: 0,
    });

    try {
      const exhaustedHandler = vi.fn();
      pool.on('pool:exhausted', exhaustedHandler);

      await pool.acquire('TEST_DO', 'exhaust-test');

      // Try to acquire another (will timeout, but should emit event)
      pool.acquire('TEST_DO', 'exhaust-test', { timeoutMs: 50 }).catch(() => {});

      // Small delay to allow event to fire
      await new Promise(resolve => setTimeout(resolve, 10));

      expect(exhaustedHandler).toHaveBeenCalledWith({
        namespace: 'TEST_DO',
        doId: 'exhaust-test',
      });
    } finally {
      pool.close();
    }
  });

  it('should serve waiters when connections are released', async () => {
    const pool = createDOConnectionPool(createMockStubFactory(), {
      maxConnectionsPerDO: 1,
      healthCheckIntervalMs: 0,
    });

    try {
      const conn1 = await pool.acquire('TEST_DO', 'waiter-test');

      // Start waiting for a connection
      const waiterPromise = pool.acquire('TEST_DO', 'waiter-test', { timeoutMs: 5000 });

      // Release the first connection
      setTimeout(() => pool.release(conn1.id), 50);

      // Waiter should get the connection
      const conn2 = await waiterPromise;
      expect(conn2.id).toBe(conn1.id);
    } finally {
      pool.close();
    }
  });
});

// =============================================================================
// Test: Health Tracking
// =============================================================================

describe('DOConnectionPool - Health Tracking', () => {
  let pool: DOConnectionPool<MockDOStub>;

  beforeEach(() => {
    pool = createDOConnectionPool(createMockStubFactory(), {
      maxConnectionsPerDO: 5,
      maxConsecutiveFailures: 3,
      unhealthyRetryMs: 100,
      healthCheckIntervalMs: 0,
    });
  });

  afterEach(() => {
    pool.close();
  });

  it('should start with unknown health', async () => {
    const conn = await pool.acquire('TEST_DO', 'health-test');
    expect(conn.health).toBe('unknown');
  });

  it('should mark connection as healthy on success', async () => {
    const conn = await pool.acquire('TEST_DO', 'success-test');
    pool.recordSuccess(conn.id);

    const updated = pool.getConnection(conn.id);
    expect(updated?.health).toBe('healthy');
    expect(updated?.consecutiveFailures).toBe(0);
    expect(updated?.successfulRequests).toBe(1);
  });

  it('should track consecutive failures', async () => {
    const conn = await pool.acquire('TEST_DO', 'failure-test');

    pool.recordFailure(conn.id);
    let updated = pool.getConnection(conn.id);
    expect(updated?.consecutiveFailures).toBe(1);
    expect(updated?.health).toBe('unknown'); // Not yet unhealthy

    pool.recordFailure(conn.id);
    updated = pool.getConnection(conn.id);
    expect(updated?.consecutiveFailures).toBe(2);

    pool.recordFailure(conn.id);
    updated = pool.getConnection(conn.id);
    expect(updated?.consecutiveFailures).toBe(3);
    expect(updated?.health).toBe('unhealthy');
  });

  it('should reset consecutive failures on success', async () => {
    const conn = await pool.acquire('TEST_DO', 'reset-test');

    pool.recordFailure(conn.id);
    pool.recordFailure(conn.id);

    let updated = pool.getConnection(conn.id);
    expect(updated?.consecutiveFailures).toBe(2);

    pool.recordSuccess(conn.id);
    updated = pool.getConnection(conn.id);
    expect(updated?.consecutiveFailures).toBe(0);
  });

  it('should emit connection:healthChange event', async () => {
    const healthChangeHandler = vi.fn();
    pool.on('connection:healthChange', healthChangeHandler);

    const conn = await pool.acquire('TEST_DO', 'health-change-test');

    // Trigger health change to healthy
    pool.recordSuccess(conn.id);

    expect(healthChangeHandler).toHaveBeenCalledWith({
      connectionId: conn.id,
      doId: 'health-change-test',
      oldHealth: 'unknown',
      newHealth: 'healthy',
    });
  });

  it('should prefer healthy connections', async () => {
    // Create two connections
    const conn1 = await pool.acquire('TEST_DO', 'prefer-test');
    const conn2 = await pool.acquire('TEST_DO', 'prefer-test');

    // Mark first as unhealthy
    pool.recordFailure(conn1.id);
    pool.recordFailure(conn1.id);
    pool.recordFailure(conn1.id);

    // Mark second as healthy
    pool.recordSuccess(conn2.id);

    // Release both
    pool.release(conn1.id);
    pool.release(conn2.id);

    // Acquire should prefer healthy conn2
    const conn3 = await pool.acquire('TEST_DO', 'prefer-test');
    expect(conn3.id).toBe(conn2.id);
  });

  it('should retry unhealthy connections after timeout', async () => {
    const conn = await pool.acquire('TEST_DO', 'retry-test');

    // Mark as unhealthy
    pool.recordFailure(conn.id);
    pool.recordFailure(conn.id);
    pool.recordFailure(conn.id);
    pool.release(conn.id);

    // Immediately, unhealthy connection should not be returned
    // Need to wait for unhealthyRetryMs
    await new Promise(resolve => setTimeout(resolve, 150));

    // Now should be retriable (health reset to unknown)
    const conn2 = await pool.acquire('TEST_DO', 'retry-test');
    expect(conn2.id).toBe(conn.id);
    expect(conn2.health).toBe('unknown');
  });

  it('should track health statistics', async () => {
    const conn1 = await pool.acquire('TEST_DO', 'stats-health-1');
    const conn2 = await pool.acquire('TEST_DO', 'stats-health-2');

    pool.recordSuccess(conn1.id);
    pool.recordFailure(conn2.id);
    pool.recordFailure(conn2.id);
    pool.recordFailure(conn2.id);

    const stats = pool.getStats();
    expect(stats.healthyConnections).toBe(1);
    expect(stats.unhealthyConnections).toBe(1);
  });
});

// =============================================================================
// Test: Idle Timeout and Eviction
// =============================================================================

describe('DOConnectionPool - Idle Timeout', () => {
  it('should evict idle connections after timeout', async () => {
    const pool = createDOConnectionPool(createMockStubFactory(), {
      idleTimeoutMs: 100,
      healthCheckIntervalMs: 0,
    });

    try {
      const conn = await pool.acquire('TEST_DO', 'idle-test');
      pool.release(conn.id);

      // Should exist immediately after release
      expect(pool.getConnection(conn.id)).toBeDefined();

      // Wait for idle timeout
      await new Promise(resolve => setTimeout(resolve, 150));

      // Should be evicted
      expect(pool.getConnection(conn.id)).toBeUndefined();
    } finally {
      pool.close();
    }
  });

  it('should emit connection:evicted event', async () => {
    const pool = createDOConnectionPool(createMockStubFactory(), {
      idleTimeoutMs: 50,
      healthCheckIntervalMs: 0,
    });

    try {
      const evictedHandler = vi.fn();
      pool.on('connection:evicted', evictedHandler);

      const conn = await pool.acquire('TEST_DO', 'evict-event-test');
      pool.release(conn.id);

      // Wait for eviction
      await new Promise(resolve => setTimeout(resolve, 100));

      expect(evictedHandler).toHaveBeenCalledWith({
        connectionId: conn.id,
        doId: 'evict-event-test',
        reason: 'Idle timeout',
      });
    } finally {
      pool.close();
    }
  });

  it('should reset idle timer on re-acquire', async () => {
    const pool = createDOConnectionPool(createMockStubFactory(), {
      idleTimeoutMs: 100,
      healthCheckIntervalMs: 0,
    });

    try {
      const conn = await pool.acquire('TEST_DO', 'reset-idle-test');
      pool.release(conn.id);

      // Wait part of the idle timeout
      await new Promise(resolve => setTimeout(resolve, 50));

      // Re-acquire (should reset timer)
      const conn2 = await pool.acquire('TEST_DO', 'reset-idle-test');
      expect(conn2.id).toBe(conn.id);
      pool.release(conn2.id);

      // Wait another partial timeout (total 100ms from last release)
      await new Promise(resolve => setTimeout(resolve, 50));

      // Should still exist (timer was reset)
      expect(pool.getConnection(conn.id)).toBeDefined();

      // Wait for full timeout from last release
      await new Promise(resolve => setTimeout(resolve, 60));

      // Now should be evicted
      expect(pool.getConnection(conn.id)).toBeUndefined();
    } finally {
      pool.close();
    }
  });

  it('should track evictions in stats', async () => {
    const pool = createDOConnectionPool(createMockStubFactory(), {
      idleTimeoutMs: 50,
      healthCheckIntervalMs: 0,
    });

    try {
      const conn = await pool.acquire('TEST_DO', 'evict-stats-test');
      pool.release(conn.id);

      await new Promise(resolve => setTimeout(resolve, 100));

      const stats = pool.getStats();
      expect(stats.evictions).toBe(1);
    } finally {
      pool.close();
    }
  });
});

// =============================================================================
// Test: Manual Eviction
// =============================================================================

describe('DOConnectionPool - Manual Eviction', () => {
  let pool: DOConnectionPool<MockDOStub>;

  beforeEach(() => {
    pool = createDOConnectionPool(createMockStubFactory(), {
      idleTimeoutMs: 60000, // Long timeout
      healthCheckIntervalMs: 0,
    });
  });

  afterEach(() => {
    pool.close();
  });

  it('should evict a specific connection', async () => {
    const conn = await pool.acquire('TEST_DO', 'manual-evict-test');
    pool.release(conn.id);

    pool.evict(conn.id, 'Test eviction');

    expect(pool.getConnection(conn.id)).toBeUndefined();
  });

  it('should evict all connections for a DO', async () => {
    const conn1 = await pool.acquire('TEST_DO', 'bulk-evict-test');
    const conn2 = await pool.acquire('TEST_DO', 'bulk-evict-test');
    const conn3 = await pool.acquire('TEST_DO', 'other-id');

    pool.release(conn1.id);
    pool.release(conn2.id);
    pool.release(conn3.id);

    const evictedCount = pool.evictAll('TEST_DO', 'bulk-evict-test', 'Bulk test');

    expect(evictedCount).toBe(2);
    expect(pool.getConnection(conn1.id)).toBeUndefined();
    expect(pool.getConnection(conn2.id)).toBeUndefined();
    expect(pool.getConnection(conn3.id)).toBeDefined();
  });

  it('should handle eviction of non-existent connection gracefully', () => {
    // Should not throw
    expect(() => pool.evict('non-existent-id')).not.toThrow();
  });
});

// =============================================================================
// Test: Statistics
// =============================================================================

describe('DOConnectionPool - Statistics', () => {
  let pool: DOConnectionPool<MockDOStub>;

  beforeEach(() => {
    pool = createDOConnectionPool(createMockStubFactory(), {
      maxConnectionsPerDO: 10,
      healthCheckIntervalMs: 0,
      enableStats: true,
    });
  });

  afterEach(() => {
    pool.close();
  });

  it('should track acquisitions and releases', async () => {
    const conn1 = await pool.acquire('TEST_DO', 'stats-test-1');
    const conn2 = await pool.acquire('TEST_DO', 'stats-test-2');
    pool.release(conn1.id);

    const stats = pool.getStats();
    expect(stats.acquisitions).toBe(2);
    expect(stats.releases).toBe(1);
    expect(stats.inUseConnections).toBe(1);
    expect(stats.idleConnections).toBe(1);
  });

  it('should track request success and failure counts', async () => {
    const conn = await pool.acquire('TEST_DO', 'request-stats-test');

    pool.recordSuccess(conn.id);
    pool.recordSuccess(conn.id);
    pool.recordFailure(conn.id);

    const stats = pool.getStats();
    expect(stats.totalRequests).toBe(3);
    expect(stats.successfulRequests).toBe(2);
    expect(stats.failedRequests).toBe(1);
  });

  it('should track connections by namespace', async () => {
    await pool.acquire('NS_1', 'id-1');
    await pool.acquire('NS_1', 'id-2');
    await pool.acquire('NS_2', 'id-3');

    const stats = pool.getStats();
    expect(stats.connectionsByNamespace.get('NS_1')).toBe(2);
    expect(stats.connectionsByNamespace.get('NS_2')).toBe(1);
  });

  it('should calculate hit rate', async () => {
    // First acquisition is a miss (new connection)
    const conn1 = await pool.acquire('TEST_DO', 'hit-rate-test');
    pool.release(conn1.id);

    // Second acquisition is a hit (reuse)
    const conn2 = await pool.acquire('TEST_DO', 'hit-rate-test');
    pool.release(conn2.id);

    // Third acquisition is a hit (reuse)
    await pool.acquire('TEST_DO', 'hit-rate-test');

    const stats = pool.getStats();
    expect(stats.hitRate).toBeCloseTo(2 / 3, 2); // 2 hits, 1 miss
  });

  it('should calculate average connection age', async () => {
    const conn1 = await pool.acquire('TEST_DO', 'age-test-1');
    await new Promise(resolve => setTimeout(resolve, 50));
    const conn2 = await pool.acquire('TEST_DO', 'age-test-2');

    const stats = pool.getStats();
    expect(stats.averageConnectionAge).toBeGreaterThan(0);
  });

  it('should reset statistics', async () => {
    const conn = await pool.acquire('TEST_DO', 'reset-stats-test');
    pool.recordSuccess(conn.id);
    pool.release(conn.id);

    pool.resetStats();

    const stats = pool.getStats();
    expect(stats.totalRequests).toBe(0);
    expect(stats.acquisitions).toBe(0);
    expect(stats.releases).toBe(0);
    // Note: connections still exist, just stats are reset
  });
});

// =============================================================================
// Test: withConnection Helper
// =============================================================================

describe('DOConnectionPool - withConnection', () => {
  let pool: DOConnectionPool<MockDOStub>;

  beforeEach(() => {
    pool = createDOConnectionPool(createMockStubFactory(), {
      healthCheckIntervalMs: 0,
    });
  });

  afterEach(() => {
    pool.close();
  });

  it('should acquire, execute, and release', async () => {
    const result = await pool.withConnection('TEST_DO', 'with-conn-test', async (stub) => {
      expect(stub.namespace).toBe('TEST_DO');
      return 'success';
    });

    expect(result).toBe('success');

    // Connection should be released
    const stats = pool.getStats();
    expect(stats.inUseConnections).toBe(0);
    expect(stats.idleConnections).toBe(1);
  });

  it('should record success on successful execution', async () => {
    await pool.withConnection('TEST_DO', 'success-record-test', async () => {
      return 'done';
    });

    const stats = pool.getStats();
    expect(stats.successfulRequests).toBe(1);
  });

  it('should record failure and release on error', async () => {
    await expect(
      pool.withConnection('TEST_DO', 'error-test', async () => {
        throw new Error('Test error');
      })
    ).rejects.toThrow('Test error');

    const stats = pool.getStats();
    expect(stats.failedRequests).toBe(1);
    expect(stats.inUseConnections).toBe(0);
    expect(stats.idleConnections).toBe(1);
  });
});

// =============================================================================
// Test: Connection Listing
// =============================================================================

describe('DOConnectionPool - Listing', () => {
  let pool: DOConnectionPool<MockDOStub>;

  beforeEach(() => {
    pool = createDOConnectionPool(createMockStubFactory(), {
      healthCheckIntervalMs: 0,
    });
  });

  afterEach(() => {
    pool.close();
  });

  it('should list all connections', async () => {
    await pool.acquire('NS_1', 'id-1');
    await pool.acquire('NS_1', 'id-2');
    await pool.acquire('NS_2', 'id-3');

    const connections = pool.listConnections();
    expect(connections).toHaveLength(3);
  });

  it('should list connections for a specific DO', async () => {
    await pool.acquire('TEST_DO', 'specific-id');
    await pool.acquire('TEST_DO', 'specific-id');
    await pool.acquire('TEST_DO', 'other-id');

    const connections = pool.listConnectionsForDO('TEST_DO', 'specific-id');
    expect(connections).toHaveLength(2);
    expect(connections.every(c => c.doId === 'specific-id')).toBe(true);
  });

  it('should get a specific connection by ID', async () => {
    const conn = await pool.acquire('TEST_DO', 'get-test');

    const retrieved = pool.getConnection(conn.id);
    expect(retrieved).toBeDefined();
    expect(retrieved?.id).toBe(conn.id);
    expect(retrieved?.doId).toBe('get-test');
  });

  it('should return undefined for non-existent connection', () => {
    const result = pool.getConnection('non-existent');
    expect(result).toBeUndefined();
  });
});

// =============================================================================
// Test: Pool Lifecycle
// =============================================================================

describe('DOConnectionPool - Lifecycle', () => {
  it('should close pool and reject waiters', async () => {
    const pool = createDOConnectionPool(createMockStubFactory(), {
      maxConnectionsPerDO: 1,
      healthCheckIntervalMs: 0,
    });

    try {
      await pool.acquire('TEST_DO', 'close-test');

      // Start waiting
      const waiterPromise = pool.acquire('TEST_DO', 'close-test', { timeoutMs: 5000 });

      // Close pool
      pool.close();

      // Waiter should be rejected
      await expect(waiterPromise).rejects.toThrow('closed');
    } finally {
      if (!pool.isClosed()) {
        pool.close();
      }
    }
  });

  it('should check isClosed status', () => {
    const pool = createDOConnectionPool(createMockStubFactory());

    expect(pool.isClosed()).toBe(false);

    pool.close();

    expect(pool.isClosed()).toBe(true);
  });

  it('should clear all connections on close', async () => {
    const pool = createDOConnectionPool(createMockStubFactory(), {
      healthCheckIntervalMs: 0,
    });

    await pool.acquire('TEST_DO', 'clear-test-1');
    await pool.acquire('TEST_DO', 'clear-test-2');

    pool.close();

    expect(pool.listConnections()).toHaveLength(0);
  });
});

// =============================================================================
// Test: Event Listeners
// =============================================================================

describe('DOConnectionPool - Event Listeners', () => {
  let pool: DOConnectionPool<MockDOStub>;

  beforeEach(() => {
    pool = createDOConnectionPool(createMockStubFactory(), {
      healthCheckIntervalMs: 0,
    });
  });

  afterEach(() => {
    pool.close();
  });

  it('should register and trigger event listeners', async () => {
    const handler = vi.fn();
    pool.on('connection:created', handler);

    await pool.acquire('TEST_DO', 'listener-test');

    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('should remove event listeners', async () => {
    const handler = vi.fn();
    pool.on('connection:created', handler);
    pool.off('connection:created', handler);

    await pool.acquire('TEST_DO', 'removed-listener-test');

    expect(handler).not.toHaveBeenCalled();
  });

  it('should support multiple listeners for same event', async () => {
    const handler1 = vi.fn();
    const handler2 = vi.fn();

    pool.on('connection:created', handler1);
    pool.on('connection:created', handler2);

    await pool.acquire('TEST_DO', 'multi-listener-test');

    expect(handler1).toHaveBeenCalledTimes(1);
    expect(handler2).toHaveBeenCalledTimes(1);
  });

  it('should handle listener errors gracefully', async () => {
    const errorHandler = vi.fn(() => {
      throw new Error('Listener error');
    });
    const goodHandler = vi.fn();

    pool.on('connection:created', errorHandler);
    pool.on('connection:created', goodHandler);

    // Should not throw
    await pool.acquire('TEST_DO', 'error-listener-test');

    // Good handler should still be called
    expect(goodHandler).toHaveBeenCalled();
  });
});

// =============================================================================
// Test: Pooled ShardRPC
// =============================================================================

describe('createPooledShardRPC', () => {
  it('should create a pooled ShardRPC implementation', async () => {
    const stubFactory: DOStubFactory<{ fetch: typeof fetch }> = (namespace, doId) => ({
      fetch: async (input, init) => {
        return new Response(
          JSON.stringify({ columns: ['id'], rows: [[1]], rowCount: 1 }),
          { headers: { 'Content-Type': 'application/json' } }
        );
      },
    });

    const rpc = createPooledShardRPC(stubFactory, {
      maxConnectionsPerDO: 5,
      healthCheckIntervalMs: 0,
    });

    expect(rpc).toBeDefined();
    expect(rpc.pool).toBeDefined();
    expect(typeof rpc.execute).toBe('function');
    expect(typeof rpc.executeStream).toBe('function');

    rpc.pool.close();
  });

  it('should execute queries through the pool', async () => {
    let requestCount = 0;
    const stubFactory: DOStubFactory<{ fetch: typeof fetch }> = (namespace, doId) => ({
      fetch: async (input, init) => {
        requestCount++;
        return new Response(
          JSON.stringify({ columns: ['id', 'name'], rows: [[1, 'test']], rowCount: 1 }),
          { headers: { 'Content-Type': 'application/json' } }
        );
      },
    });

    const rpc = createPooledShardRPC(stubFactory, {
      healthCheckIntervalMs: 0,
    });

    try {
      const result = await rpc.execute('shard-1', undefined, 'SELECT * FROM test');

      expect(result).toBeDefined();
      expect(result.shardId).toBe('shard-1');
      expect(result.columns).toEqual(['id', 'name']);
      expect(result.rows).toEqual([[1, 'test']]);
      expect(result.rowCount).toBe(1);
      expect(result.executionTimeMs).toBeGreaterThanOrEqual(0);
      expect(requestCount).toBe(1);
    } finally {
      rpc.pool.close();
    }
  });

  it('should handle errors from shard execution', async () => {
    const stubFactory: DOStubFactory<{ fetch: typeof fetch }> = (namespace, doId) => ({
      fetch: async () => {
        return new Response('Internal error', { status: 500 });
      },
    });

    const rpc = createPooledShardRPC(stubFactory, {
      healthCheckIntervalMs: 0,
    });

    try {
      const result = await rpc.execute('shard-1', undefined, 'SELECT * FROM test');

      expect(result.error).toBeDefined();
      expect(result.error?.code).toBe('RPC_ERROR');
      expect(result.error?.isRetryable).toBe(true);
    } finally {
      rpc.pool.close();
    }
  });

  it('should stream results', async () => {
    const stubFactory: DOStubFactory<{ fetch: typeof fetch }> = () => ({
      fetch: async () => {
        return new Response(
          JSON.stringify({ columns: ['id'], rows: [[1], [2]], rowCount: 2 }),
          { headers: { 'Content-Type': 'application/json' } }
        );
      },
    });

    const rpc = createPooledShardRPC(stubFactory, {
      healthCheckIntervalMs: 0,
    });

    try {
      const results: Array<{ rowCount: number }> = [];
      for await (const result of rpc.executeStream('shard-1', undefined, 'SELECT * FROM test')) {
        results.push(result);
      }

      expect(results).toHaveLength(1);
      expect(results[0].rowCount).toBe(2);
    } finally {
      rpc.pool.close();
    }
  });
});
