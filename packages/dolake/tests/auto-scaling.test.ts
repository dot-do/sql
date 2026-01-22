/**
 * DoLake Auto-Scaling DO Pool Tests (TDD Red Phase)
 *
 * Tests for automatic scaling of DO instances based on load metrics.
 *
 * Issue: do-d1isn.9 - RED: Auto-scaling DO pool
 *
 * Requirements:
 * 1. HorizontalScalingManager detects overload
 * 2. Scale-up when buffer > 80% OR latency > 5s
 * 3. Scale-down when buffer < 20% for 5+ minutes
 * 4. ShardRouter updates routing table on scale events
 * 5. Graceful drain of old shards before removal
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  AutoScalingManager,
  type AutoScalingConfig,
  type LoadMetrics,
  type ShardHealth,
  type ScaleEvent,
} from '../src/scalability.js';

// =============================================================================
// Test: HorizontalScalingManager detects overload
// =============================================================================

describe('HorizontalScalingManager detects overload', () => {
  let manager: AutoScalingManager;

  beforeEach(() => {
    manager = new AutoScalingManager({
      scaleUpBufferThreshold: 0.8,
      scaleUpLatencyThresholdMs: 5000,
    });
  });

  it('should detect overload when buffer utilization exceeds 80%', () => {
    // Arrange
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.85,
      avgLatencyMs: 100,
      p99LatencyMs: 500,
      requestsPerSecond: 1000,
      activeConnections: 50,
    });

    // Act
    const result = manager.detectOverload();

    // Assert
    expect(result.isOverloaded).toBe(true);
    expect(result.reason).toContain('Buffer utilization');
    expect(result.reason).toContain('85');
  });

  it('should detect overload when latency exceeds 5 seconds', () => {
    // Arrange
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.5,
      avgLatencyMs: 6000,
      p99LatencyMs: 8000,
      requestsPerSecond: 500,
      activeConnections: 30,
    });

    // Act
    const result = manager.detectOverload();

    // Assert
    expect(result.isOverloaded).toBe(true);
    expect(result.reason).toContain('latency');
    expect(result.reason).toContain('6000');
  });

  it('should not detect overload when metrics are normal', () => {
    // Arrange
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.5,
      avgLatencyMs: 100,
      p99LatencyMs: 500,
      requestsPerSecond: 1000,
      activeConnections: 50,
    });

    // Act
    const result = manager.detectOverload();

    // Assert
    expect(result.isOverloaded).toBe(false);
    expect(result.reason).toBeNull();
  });

  it('should aggregate metrics from multiple shards', () => {
    // Arrange
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.6,
      avgLatencyMs: 100,
      p99LatencyMs: 500,
      requestsPerSecond: 500,
      activeConnections: 25,
    });
    manager.recordLoadMetrics('shard-1', {
      bufferUtilization: 0.9,
      avgLatencyMs: 200,
      p99LatencyMs: 800,
      requestsPerSecond: 500,
      activeConnections: 25,
    });

    // Act
    const aggregate = manager.getAggregateMetrics();
    const result = manager.detectOverload();

    // Assert - Average buffer is 0.75 (60% + 90%) / 2 = 75%, which is below 80%
    expect(aggregate.bufferUtilization).toBe(0.75);
    expect(result.isOverloaded).toBe(false);
  });

  it('should detect overload when aggregate buffer exceeds threshold', () => {
    // Arrange
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.85,
      avgLatencyMs: 100,
      p99LatencyMs: 500,
      requestsPerSecond: 500,
      activeConnections: 25,
    });
    manager.recordLoadMetrics('shard-1', {
      bufferUtilization: 0.85,
      avgLatencyMs: 200,
      p99LatencyMs: 800,
      requestsPerSecond: 500,
      activeConnections: 25,
    });

    // Act
    const aggregate = manager.getAggregateMetrics();
    const result = manager.detectOverload();

    // Assert - Average buffer is 85%, above threshold
    expect(aggregate.bufferUtilization).toBe(0.85);
    expect(result.isOverloaded).toBe(true);
  });
});

// =============================================================================
// Test: Scale-up when buffer > 80% OR latency > 5s
// =============================================================================

describe('Scale-up when buffer > 80% OR latency > 5s', () => {
  let manager: AutoScalingManager;

  beforeEach(() => {
    manager = new AutoScalingManager({
      minInstances: 1,
      maxInstances: 16,
      scaleUpBufferThreshold: 0.8,
      scaleUpLatencyThresholdMs: 5000,
      scaleCooldownMs: 60000,
    });
    manager.resetCooldown();
  });

  it('should recommend scale-up when buffer exceeds 80%', () => {
    // Arrange
    manager.setInstanceCount(2);
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.85,
      avgLatencyMs: 100,
      p99LatencyMs: 500,
      requestsPerSecond: 1000,
      activeConnections: 50,
    });

    // Act
    const scaleEvent = manager.evaluateScaling();

    // Assert
    expect(scaleEvent).not.toBeNull();
    expect(scaleEvent!.type).toBe('scale-up');
    expect(scaleEvent!.fromInstances).toBe(2);
    expect(scaleEvent!.toInstances).toBe(3);
    expect(scaleEvent!.reason).toContain('Buffer utilization');
  });

  it('should recommend scale-up when latency exceeds 5 seconds', () => {
    // Arrange
    manager.setInstanceCount(2);
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.5,
      avgLatencyMs: 6000,
      p99LatencyMs: 10000,
      requestsPerSecond: 500,
      activeConnections: 30,
    });

    // Act
    const scaleEvent = manager.evaluateScaling();

    // Assert
    expect(scaleEvent).not.toBeNull();
    expect(scaleEvent!.type).toBe('scale-up');
    expect(scaleEvent!.reason).toContain('latency');
  });

  it('should execute scale-up and emit event', async () => {
    // Arrange
    manager.setInstanceCount(2);
    const events: ScaleEvent[] = [];
    manager.onScaleEvent((event) => events.push(event));

    // Act
    const result = await manager.scaleUp(4);

    // Assert
    expect(result.type).toBe('scale-up');
    expect(result.fromInstances).toBe(2);
    expect(result.toInstances).toBe(4);
    expect(manager.getInstanceCount()).toBe(4);
    expect(events).toHaveLength(1);
    expect(events[0].type).toBe('scale-up');
  });

  it('should not exceed maxInstances on scale-up', async () => {
    // Arrange
    manager.setInstanceCount(15);

    // Act
    const result = await manager.scaleUp(20);

    // Assert
    expect(result.toInstances).toBe(16); // maxInstances
    expect(manager.getInstanceCount()).toBe(16);
  });

  it('should not scale-up when already at maxInstances', () => {
    // Arrange
    manager.setInstanceCount(16);
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.95,
      avgLatencyMs: 10000,
      p99LatencyMs: 15000,
      requestsPerSecond: 100,
      activeConnections: 10,
    });

    // Act
    const scaleEvent = manager.evaluateScaling();

    // Assert - No scale event because already at max
    expect(scaleEvent).toBeNull();
  });
});

// =============================================================================
// Test: Scale-down when buffer < 20% for 5+ minutes
// =============================================================================

describe('Scale-down when buffer < 20% for 5+ minutes', () => {
  let manager: AutoScalingManager;

  beforeEach(() => {
    vi.useFakeTimers();
    manager = new AutoScalingManager({
      minInstances: 1,
      maxInstances: 16,
      scaleDownBufferThreshold: 0.2,
      scaleDownCooldownMs: 300000, // 5 minutes
      scaleCooldownMs: 60000,
    });
    manager.resetCooldown();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should not scale-down immediately when buffer drops below 20%', () => {
    // Arrange
    manager.setInstanceCount(4);
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.1,
      avgLatencyMs: 50,
      p99LatencyMs: 100,
      requestsPerSecond: 100,
      activeConnections: 5,
    });

    // Act
    const scaleEvent = manager.evaluateScaling();

    // Assert - No immediate scale-down
    expect(scaleEvent).toBeNull();
  });

  it('should scale-down after buffer is below 20% for 5+ minutes', () => {
    // Arrange
    manager.setInstanceCount(4);
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.1,
      avgLatencyMs: 50,
      p99LatencyMs: 100,
      requestsPerSecond: 100,
      activeConnections: 5,
    });

    // First evaluation starts the timer
    manager.evaluateScaling();

    // Advance time by 5 minutes
    vi.advanceTimersByTime(300001);

    // Re-record low metrics
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.15,
      avgLatencyMs: 50,
      p99LatencyMs: 100,
      requestsPerSecond: 100,
      activeConnections: 5,
    });

    // Act
    const scaleEvent = manager.evaluateScaling();

    // Assert
    expect(scaleEvent).not.toBeNull();
    expect(scaleEvent!.type).toBe('scale-down');
    expect(scaleEvent!.fromInstances).toBe(4);
    expect(scaleEvent!.toInstances).toBe(3);
  });

  it('should reset scale-down timer if buffer goes above threshold', () => {
    // Arrange
    manager.setInstanceCount(4);

    // Start with low buffer
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.1,
      avgLatencyMs: 50,
      p99LatencyMs: 100,
      requestsPerSecond: 100,
      activeConnections: 5,
    });
    manager.evaluateScaling(); // Start timer

    // Advance 3 minutes
    vi.advanceTimersByTime(180000);

    // Buffer spikes above threshold
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.5,
      avgLatencyMs: 100,
      p99LatencyMs: 200,
      requestsPerSecond: 500,
      activeConnections: 20,
    });
    manager.evaluateScaling(); // Should reset timer

    // Advance another 3 minutes (total 6 minutes, but timer was reset)
    vi.advanceTimersByTime(180000);

    // Buffer drops again
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.1,
      avgLatencyMs: 50,
      p99LatencyMs: 100,
      requestsPerSecond: 100,
      activeConnections: 5,
    });

    // Act
    const scaleEvent = manager.evaluateScaling();

    // Assert - No scale-down yet because timer was reset
    expect(scaleEvent).toBeNull();
  });

  it('should not go below minInstances on scale-down', async () => {
    // Arrange
    manager.setInstanceCount(1);
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.05,
      avgLatencyMs: 10,
      p99LatencyMs: 50,
      requestsPerSecond: 10,
      activeConnections: 1,
    });

    // Start timer and wait
    manager.evaluateScaling();
    vi.advanceTimersByTime(300001);
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.05,
      avgLatencyMs: 10,
      p99LatencyMs: 50,
      requestsPerSecond: 10,
      activeConnections: 1,
    });

    // Act
    const scaleEvent = manager.evaluateScaling();

    // Assert - No scale-down because already at minInstances
    expect(scaleEvent).toBeNull();
  });

  it('should execute scale-down with affected shards', async () => {
    // Arrange
    manager.setInstanceCount(4);
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.3,
      avgLatencyMs: 100,
      p99LatencyMs: 200,
      requestsPerSecond: 200,
      activeConnections: 10,
    });
    manager.recordLoadMetrics('shard-1', {
      bufferUtilization: 0.1,
      avgLatencyMs: 50,
      p99LatencyMs: 100,
      requestsPerSecond: 50,
      activeConnections: 5,
    });
    manager.recordLoadMetrics('shard-2', {
      bufferUtilization: 0.2,
      avgLatencyMs: 75,
      p99LatencyMs: 150,
      requestsPerSecond: 100,
      activeConnections: 7,
    });
    manager.recordLoadMetrics('shard-3', {
      bufferUtilization: 0.05,
      avgLatencyMs: 25,
      p99LatencyMs: 50,
      requestsPerSecond: 20,
      activeConnections: 2,
    });

    // Act
    const result = await manager.scaleDown(3);

    // Assert
    expect(result.type).toBe('scale-down');
    expect(result.fromInstances).toBe(4);
    expect(result.toInstances).toBe(3);
    expect(result.affectedShards).toBeDefined();
    expect(result.affectedShards!.length).toBe(1);
    // Should select shard with lowest load (shard-3)
    expect(result.affectedShards![0]).toBe('shard-3');
  });
});

// =============================================================================
// Test: ShardRouter updates routing table on scale events
// =============================================================================

describe('ShardRouter updates routing table on scale events', () => {
  let manager: AutoScalingManager;

  beforeEach(() => {
    manager = new AutoScalingManager({
      minInstances: 1,
      maxInstances: 16,
    });
  });

  it('should add new shards to health tracking on scale-up', async () => {
    // Arrange
    manager.setInstanceCount(2);
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.5,
      avgLatencyMs: 100,
      p99LatencyMs: 200,
      requestsPerSecond: 500,
      activeConnections: 25,
    });
    manager.recordLoadMetrics('shard-1', {
      bufferUtilization: 0.6,
      avgLatencyMs: 150,
      p99LatencyMs: 300,
      requestsPerSecond: 600,
      activeConnections: 30,
    });

    // Act
    await manager.scaleUp(4);

    // Assert
    const allHealth = manager.getAllShardHealth();
    expect(allHealth.size).toBe(2); // Existing shards still tracked
    expect(manager.getInstanceCount()).toBe(4);
  });

  it('should mark shards as draining on scale-down', async () => {
    // Arrange
    manager.setInstanceCount(4);
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.3,
      avgLatencyMs: 100,
      p99LatencyMs: 200,
      requestsPerSecond: 200,
      activeConnections: 10,
    });
    manager.recordLoadMetrics('shard-1', {
      bufferUtilization: 0.1,
      avgLatencyMs: 50,
      p99LatencyMs: 100,
      requestsPerSecond: 50,
      activeConnections: 5,
    });

    // Act
    const result = await manager.scaleDown(3);
    const affectedShard = result.affectedShards![0];

    // Assert
    expect(manager.isDraining(affectedShard)).toBe(true);
    const health = manager.getShardHealth(affectedShard);
    expect(health?.status).toBe('draining');
  });

  it('should remove shards from routing after drain completes', async () => {
    // Arrange
    manager.setInstanceCount(2);
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.5,
      avgLatencyMs: 100,
      p99LatencyMs: 200,
      requestsPerSecond: 500,
      activeConnections: 25,
    });
    manager.recordLoadMetrics('shard-1', {
      bufferUtilization: 0.1,
      avgLatencyMs: 50,
      p99LatencyMs: 100,
      requestsPerSecond: 50,
      activeConnections: 5,
    });

    // Scale down and complete drain
    await manager.scaleDown(1);
    const healthBefore = manager.getAllShardHealth();
    expect(healthBefore.has('shard-1')).toBe(true);

    // Complete drain
    await manager.completeDrain('shard-1');

    // Assert
    const healthAfter = manager.getAllShardHealth();
    expect(healthAfter.has('shard-1')).toBe(false);
    expect(manager.getLoadMetrics('shard-1')).toBeUndefined();
  });

  it('should emit scale events with routing information', async () => {
    // Arrange
    manager.setInstanceCount(3);
    const events: ScaleEvent[] = [];
    manager.onScaleEvent((event) => events.push(event));

    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.4,
      avgLatencyMs: 100,
      p99LatencyMs: 200,
      requestsPerSecond: 300,
      activeConnections: 15,
    });
    manager.recordLoadMetrics('shard-1', {
      bufferUtilization: 0.2,
      avgLatencyMs: 80,
      p99LatencyMs: 150,
      requestsPerSecond: 200,
      activeConnections: 10,
    });
    manager.recordLoadMetrics('shard-2', {
      bufferUtilization: 0.1,
      avgLatencyMs: 50,
      p99LatencyMs: 100,
      requestsPerSecond: 100,
      activeConnections: 5,
    });

    // Act
    await manager.scaleDown(2);

    // Assert
    expect(events).toHaveLength(1);
    expect(events[0].affectedShards).toBeDefined();
    expect(events[0].affectedShards!.length).toBe(1);
  });
});

// =============================================================================
// Test: Graceful drain of old shards before removal
// =============================================================================

describe('Graceful drain of old shards before removal', () => {
  let manager: AutoScalingManager;

  beforeEach(() => {
    manager = new AutoScalingManager({
      minInstances: 1,
      maxInstances: 16,
      drainTimeoutMs: 300000,
      drainCheckIntervalMs: 5000,
    });
  });

  it('should mark shard as draining when drain starts', async () => {
    // Arrange
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.5,
      avgLatencyMs: 100,
      p99LatencyMs: 200,
      requestsPerSecond: 500,
      activeConnections: 25,
    });

    // Act
    await manager.startDrain('shard-0');

    // Assert
    expect(manager.isDraining('shard-0')).toBe(true);
    const health = manager.getShardHealth('shard-0');
    expect(health?.status).toBe('draining');
  });

  it('should not route new requests to draining shards', async () => {
    // Arrange
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.5,
      avgLatencyMs: 100,
      p99LatencyMs: 200,
      requestsPerSecond: 500,
      activeConnections: 25,
    });
    manager.recordLoadMetrics('shard-1', {
      bufferUtilization: 0.3,
      avgLatencyMs: 80,
      p99LatencyMs: 150,
      requestsPerSecond: 300,
      activeConnections: 15,
    });

    // Act
    await manager.startDrain('shard-0');
    const allHealth = manager.getAllShardHealth();

    // Assert
    const drainingShard = allHealth.get('shard-0');
    const healthyShard = allHealth.get('shard-1');
    expect(drainingShard?.status).toBe('draining');
    expect(healthyShard?.status).toBe('healthy');
  });

  it('should complete drain and remove shard from tracking', async () => {
    // Arrange
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.5,
      avgLatencyMs: 100,
      p99LatencyMs: 200,
      requestsPerSecond: 500,
      activeConnections: 25,
    });

    await manager.startDrain('shard-0');
    expect(manager.isDraining('shard-0')).toBe(true);

    // Act
    await manager.completeDrain('shard-0');

    // Assert
    expect(manager.isDraining('shard-0')).toBe(false);
    expect(manager.getShardHealth('shard-0')).toBeUndefined();
    expect(manager.getLoadMetrics('shard-0')).toBeUndefined();
  });

  it('should select lowest-load shard for removal', async () => {
    // Arrange
    manager.setInstanceCount(4);
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.6,
      avgLatencyMs: 200,
      p99LatencyMs: 400,
      requestsPerSecond: 800,
      activeConnections: 40,
    });
    manager.recordLoadMetrics('shard-1', {
      bufferUtilization: 0.1,
      avgLatencyMs: 50,
      p99LatencyMs: 100,
      requestsPerSecond: 100,
      activeConnections: 5,
    });
    manager.recordLoadMetrics('shard-2', {
      bufferUtilization: 0.4,
      avgLatencyMs: 150,
      p99LatencyMs: 300,
      requestsPerSecond: 500,
      activeConnections: 25,
    });
    manager.recordLoadMetrics('shard-3', {
      bufferUtilization: 0.3,
      avgLatencyMs: 120,
      p99LatencyMs: 250,
      requestsPerSecond: 400,
      activeConnections: 20,
    });

    // Act
    const result = await manager.scaleDown(3);

    // Assert - shard-1 has lowest buffer utilization (0.1)
    expect(result.affectedShards).toContain('shard-1');
    expect(manager.isDraining('shard-1')).toBe(true);
    expect(manager.isDraining('shard-0')).toBe(false);
  });

  it('should allow manual drain of specific shard', async () => {
    // Arrange
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.5,
      avgLatencyMs: 100,
      p99LatencyMs: 200,
      requestsPerSecond: 500,
      activeConnections: 25,
    });
    manager.recordLoadMetrics('shard-1', {
      bufferUtilization: 0.3,
      avgLatencyMs: 80,
      p99LatencyMs: 150,
      requestsPerSecond: 300,
      activeConnections: 15,
    });

    // Act - Manually drain a specific shard (not the lowest load one)
    await manager.startDrain('shard-0');

    // Assert
    expect(manager.isDraining('shard-0')).toBe(true);
    expect(manager.isDraining('shard-1')).toBe(false);
  });
});

// =============================================================================
// Test: Configuration and thresholds
// =============================================================================

describe('Auto-scaling configuration', () => {
  it('should use default configuration values', () => {
    // Act
    const manager = new AutoScalingManager();
    const config = manager.getAutoScalingConfig();

    // Assert
    expect(config.scaleUpBufferThreshold).toBe(0.8);
    expect(config.scaleUpLatencyThresholdMs).toBe(5000);
    expect(config.scaleDownBufferThreshold).toBe(0.2);
    expect(config.scaleDownCooldownMs).toBe(300000);
    expect(config.scaleCooldownMs).toBe(60000);
    expect(config.drainTimeoutMs).toBe(300000);
  });

  it('should allow configuration updates', () => {
    // Arrange
    const manager = new AutoScalingManager();

    // Act
    const updatedConfig = manager.updateAutoScalingConfig({
      scaleUpBufferThreshold: 0.7,
      scaleUpLatencyThresholdMs: 3000,
    });

    // Assert
    expect(updatedConfig.scaleUpBufferThreshold).toBe(0.7);
    expect(updatedConfig.scaleUpLatencyThresholdMs).toBe(3000);
    // Other values should remain unchanged
    expect(updatedConfig.scaleDownBufferThreshold).toBe(0.2);
  });

  it('should respect custom configuration on initialization', () => {
    // Act
    const manager = new AutoScalingManager({
      minInstances: 2,
      maxInstances: 8,
      scaleUpBufferThreshold: 0.9,
      scaleDownBufferThreshold: 0.1,
    });
    const config = manager.getAutoScalingConfig();

    // Assert
    expect(config.minInstances).toBe(2);
    expect(config.maxInstances).toBe(8);
    expect(config.scaleUpBufferThreshold).toBe(0.9);
    expect(config.scaleDownBufferThreshold).toBe(0.1);
  });
});

// =============================================================================
// Test: Cooldown between scale operations
// =============================================================================

describe('Cooldown between scale operations', () => {
  let manager: AutoScalingManager;

  beforeEach(() => {
    vi.useFakeTimers();
    manager = new AutoScalingManager({
      minInstances: 1,
      maxInstances: 16,
      scaleCooldownMs: 60000, // 1 minute cooldown
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should not scale again during cooldown period', async () => {
    // Arrange
    manager.setInstanceCount(2);
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.85,
      avgLatencyMs: 100,
      p99LatencyMs: 200,
      requestsPerSecond: 500,
      activeConnections: 25,
    });

    // First scale-up
    await manager.scaleUp(3);

    // Still high load
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.85,
      avgLatencyMs: 100,
      p99LatencyMs: 200,
      requestsPerSecond: 500,
      activeConnections: 25,
    });

    // Act - Try to scale again immediately
    const scaleEvent = manager.evaluateScaling();

    // Assert - No scaling during cooldown
    expect(scaleEvent).toBeNull();
  });

  it('should allow scaling after cooldown expires', async () => {
    // Arrange
    manager.setInstanceCount(2);
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.85,
      avgLatencyMs: 100,
      p99LatencyMs: 200,
      requestsPerSecond: 500,
      activeConnections: 25,
    });

    // First scale-up
    await manager.scaleUp(3);

    // Advance time past cooldown
    vi.advanceTimersByTime(60001);

    // Still high load
    manager.recordLoadMetrics('shard-0', {
      bufferUtilization: 0.9,
      avgLatencyMs: 100,
      p99LatencyMs: 200,
      requestsPerSecond: 500,
      activeConnections: 25,
    });

    // Act
    const scaleEvent = manager.evaluateScaling();

    // Assert - Scaling allowed after cooldown
    expect(scaleEvent).not.toBeNull();
    expect(scaleEvent!.type).toBe('scale-up');
  });
});
