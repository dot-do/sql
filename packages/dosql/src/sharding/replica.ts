/**
 * DoSQL Replica Management
 *
 * Manages replica selection and health tracking:
 * - Primary for writes
 * - Replica selection for reads (nearest, random, round-robin)
 * - Read preference hints
 * - Health tracking and circuit breaker
 *
 * @packageDocumentation
 */

import type {
  ShardConfig,
  ReplicaConfig,
  ReplicaRole,
  ReplicaHealth,
  ReadPreference,
  ShardHealth,
  ClusterHealth,
} from './types.js';

// =============================================================================
// HEALTH TRACKING
// =============================================================================

/**
 * Health state for a single replica
 */
interface ReplicaHealthState {
  health: ReplicaHealth;
  lastSuccessTime: number;
  lastFailureTime: number;
  successCount: number;
  failureCount: number;
  latencyMs: number[];
  circuitState: 'closed' | 'open' | 'half-open';
  circuitOpenTime: number;
}

/**
 * Configuration for health tracking
 */
export interface HealthConfig {
  /** Window size for latency tracking */
  latencyWindowSize?: number;
  /** Failure threshold before marking unhealthy */
  failureThreshold?: number;
  /** Success threshold before marking healthy again */
  successThreshold?: number;
  /** Time window for failure counting (ms) */
  failureWindowMs?: number;
  /** Circuit breaker reset time (ms) */
  circuitResetMs?: number;
  /** Health check interval (ms) */
  healthCheckIntervalMs?: number;
}

/**
 * Default health configuration
 */
const DEFAULT_HEALTH_CONFIG: Required<HealthConfig> = {
  latencyWindowSize: 100,
  failureThreshold: 5,
  successThreshold: 3,
  failureWindowMs: 60000,
  circuitResetMs: 30000,
  healthCheckIntervalMs: 10000,
};

// =============================================================================
// REPLICA SELECTOR
// =============================================================================

/**
 * Replica selector interface
 */
export interface ReplicaSelector {
  /**
   * Select a replica for a given shard and read preference
   */
  select(shardId: string, preference: ReadPreference): string | undefined;

  /**
   * Record a successful request
   */
  recordSuccess(shardId: string, replicaId: string, latencyMs?: number): void;

  /**
   * Record a failed request
   */
  recordFailure(shardId: string, replicaId: string): void;

  /**
   * Get health for a specific shard
   */
  getShardHealth(shardId: string): ShardHealth;

  /**
   * Get overall cluster health
   */
  getClusterHealth(): ClusterHealth;
}

/**
 * Replica selector implementation
 * Handles replica selection based on read preference and health
 */
export class DefaultReplicaSelector implements ReplicaSelector {
  private readonly shards: Map<string, ShardConfig>;
  private readonly healthStates: Map<string, ReplicaHealthState>;
  private readonly config: Required<HealthConfig>;
  private roundRobinCounters: Map<string, number>;
  private readonly currentRegion?: string;

  constructor(
    shards: ShardConfig[],
    config?: HealthConfig,
    currentRegion?: string
  ) {
    this.shards = new Map(shards.map(s => [s.id, s]));
    this.healthStates = new Map();
    this.config = { ...DEFAULT_HEALTH_CONFIG, ...config };
    this.roundRobinCounters = new Map();
    this.currentRegion = currentRegion;

    // Initialize health states for all replicas
    for (const shard of shards) {
      this.initializeHealthState(shard.id, shard.doId ?? shard.id, 'primary');
      for (const replica of shard.replicas ?? []) {
        this.initializeHealthState(shard.id, replica.id, replica.role);
      }
    }
  }

  private initializeHealthState(shardId: string, replicaId: string, _role: ReplicaRole): void {
    const key = `${shardId}:${replicaId}`;
    this.healthStates.set(key, {
      health: 'unknown',
      lastSuccessTime: 0,
      lastFailureTime: 0,
      successCount: 0,
      failureCount: 0,
      latencyMs: [],
      circuitState: 'closed',
      circuitOpenTime: 0,
    });
  }

  private getHealthState(shardId: string, replicaId: string): ReplicaHealthState {
    const key = `${shardId}:${replicaId}`;
    let state = this.healthStates.get(key);
    if (!state) {
      state = {
        health: 'unknown',
        lastSuccessTime: 0,
        lastFailureTime: 0,
        successCount: 0,
        failureCount: 0,
        latencyMs: [],
        circuitState: 'closed',
        circuitOpenTime: 0,
      };
      this.healthStates.set(key, state);
    }
    return state;
  }

  select(shardId: string, preference: ReadPreference): string | undefined {
    const shard = this.shards.get(shardId);
    if (!shard) {
      return undefined;
    }

    // Build list of available replicas based on preference
    const candidates = this.getCandidates(shard, preference);

    if (candidates.length === 0) {
      // Fallback to primary if no candidates
      return shard.doId ?? shard.id;
    }

    // Select based on strategy
    switch (preference) {
      case 'primary':
        return shard.doId ?? shard.id;

      case 'primaryPreferred':
        // Check if primary is healthy
        const primaryState = this.getHealthState(shardId, shard.doId ?? shard.id);
        if (this.isHealthy(primaryState)) {
          return shard.doId ?? shard.id;
        }
        return this.selectRoundRobin(shardId, candidates);

      case 'replica':
        return this.selectRoundRobin(shardId, candidates);

      case 'replicaPreferred':
        // Try to find a healthy replica first
        const healthyCandidates = candidates.filter(c =>
          this.isHealthy(this.getHealthState(shardId, c.id))
        );
        if (healthyCandidates.length > 0) {
          return this.selectRoundRobin(shardId, healthyCandidates);
        }
        // Fall back to primary
        return shard.doId ?? shard.id;

      case 'nearest':
        return this.selectNearest(shardId, candidates);

      case 'analytics':
        // Prefer analytics replicas
        const analyticsReplicas = candidates.filter(c => c.role === 'analytics');
        if (analyticsReplicas.length > 0) {
          return this.selectRoundRobin(shardId, analyticsReplicas);
        }
        return this.selectRoundRobin(shardId, candidates);

      default:
        return shard.doId ?? shard.id;
    }
  }

  private getCandidates(shard: ShardConfig, preference: ReadPreference): ReplicaConfig[] {
    if (!shard.replicas || shard.replicas.length === 0) {
      return [];
    }

    // Filter based on preference
    switch (preference) {
      case 'primary':
        return [];

      case 'replica':
      case 'replicaPreferred':
        return shard.replicas.filter(r => r.role === 'replica');

      case 'analytics':
        // Include analytics replicas, fall back to regular replicas
        const analytics = shard.replicas.filter(r => r.role === 'analytics');
        if (analytics.length > 0) return analytics;
        return shard.replicas.filter(r => r.role === 'replica');

      case 'primaryPreferred':
      case 'nearest':
      default:
        return shard.replicas.filter(r => r.role !== 'primary');
    }
  }

  private selectRoundRobin(shardId: string, candidates: ReplicaConfig[]): string {
    const counter = (this.roundRobinCounters.get(shardId) ?? 0) + 1;
    this.roundRobinCounters.set(shardId, counter);
    return candidates[counter % candidates.length].id;
  }

  private selectNearest(shardId: string, candidates: ReplicaConfig[]): string {
    // Priority 1: Same region replicas
    if (this.currentRegion) {
      const sameRegion = candidates.filter(c => c.region === this.currentRegion);
      if (sameRegion.length > 0) {
        return this.selectByLatency(shardId, sameRegion);
      }
    }

    // Priority 2: Lowest latency
    return this.selectByLatency(shardId, candidates);
  }

  private selectByLatency(shardId: string, candidates: ReplicaConfig[]): string {
    let bestReplica: ReplicaConfig = candidates[0];
    let bestLatency = Infinity;

    for (const candidate of candidates) {
      const state = this.getHealthState(shardId, candidate.id);

      // Skip unhealthy replicas unless it's the only option
      if (!this.isHealthy(state) && candidates.length > 1) {
        continue;
      }

      // Calculate average latency
      const avgLatency = state.latencyMs.length > 0
        ? state.latencyMs.reduce((a, b) => a + b, 0) / state.latencyMs.length
        : Infinity;

      // Weight by replica weight
      const weightedLatency = avgLatency / (candidate.weight ?? 1);

      if (weightedLatency < bestLatency) {
        bestLatency = weightedLatency;
        bestReplica = candidate;
      }
    }

    return bestReplica.id;
  }

  private isHealthy(state: ReplicaHealthState): boolean {
    // Check circuit breaker
    if (state.circuitState === 'open') {
      const now = Date.now();
      if (now - state.circuitOpenTime < this.config.circuitResetMs) {
        return false;
      }
      // Transition to half-open
      state.circuitState = 'half-open';
    }

    return state.health === 'healthy' || state.health === 'unknown';
  }

  recordSuccess(shardId: string, replicaId: string, latencyMs?: number): void {
    const state = this.getHealthState(shardId, replicaId);
    const now = Date.now();

    state.lastSuccessTime = now;
    state.successCount++;

    // Track latency
    if (latencyMs !== undefined) {
      state.latencyMs.push(latencyMs);
      if (state.latencyMs.length > this.config.latencyWindowSize) {
        state.latencyMs.shift();
      }
    }

    // Update health based on success threshold
    if (state.successCount >= this.config.successThreshold) {
      state.health = 'healthy';
      state.failureCount = 0;

      // Close circuit breaker if it was half-open
      if (state.circuitState === 'half-open') {
        state.circuitState = 'closed';
      }
    }
  }

  recordFailure(shardId: string, replicaId: string): void {
    const state = this.getHealthState(shardId, replicaId);
    const now = Date.now();

    state.lastFailureTime = now;
    state.failureCount++;
    state.successCount = 0;

    // Check if we should mark as unhealthy
    if (state.failureCount >= this.config.failureThreshold) {
      state.health = 'unhealthy';

      // Open circuit breaker
      if (state.circuitState === 'closed') {
        state.circuitState = 'open';
        state.circuitOpenTime = now;
      }
    } else if (state.failureCount >= Math.ceil(this.config.failureThreshold / 2)) {
      state.health = 'degraded';
    }
  }

  getShardHealth(shardId: string): ShardHealth {
    const shard = this.shards.get(shardId);
    if (!shard) {
      return {
        shardId,
        status: 'unknown',
        primaryHealth: 'unknown',
        replicaHealths: {},
        lastChecked: Date.now(),
      };
    }

    const primaryState = this.getHealthState(shardId, shard.doId ?? shard.id);
    const replicaHealths: Record<string, ReplicaHealth> = {};

    for (const replica of shard.replicas ?? []) {
      const state = this.getHealthState(shardId, replica.id);
      replicaHealths[replica.id] = state.health;
    }

    // Determine overall shard status
    let status: ReplicaHealth = 'healthy';
    if (primaryState.health === 'unhealthy') {
      status = 'unhealthy';
    } else if (primaryState.health === 'degraded') {
      status = 'degraded';
    } else {
      // Check replicas
      const replicaStatuses = Object.values(replicaHealths);
      const unhealthyCount = replicaStatuses.filter(h => h === 'unhealthy').length;
      if (unhealthyCount === replicaStatuses.length && replicaStatuses.length > 0) {
        status = 'degraded';
      }
    }

    // Calculate average latency
    const avgLatency = primaryState.latencyMs.length > 0
      ? primaryState.latencyMs.reduce((a, b) => a + b, 0) / primaryState.latencyMs.length
      : undefined;

    return {
      shardId,
      status,
      primaryHealth: primaryState.health,
      replicaHealths,
      lastChecked: Date.now(),
      latencyMs: avgLatency,
    };
  }

  getClusterHealth(): ClusterHealth {
    const shardHealths: ShardHealth[] = [];
    let healthyShards = 0;
    let totalShards = 0;
    let healthyReplicas = 0;
    let totalReplicas = 0;

    for (const shardId of this.shards.keys()) {
      const health = this.getShardHealth(shardId);
      shardHealths.push(health);
      totalShards++;

      if (health.status === 'healthy' || health.status === 'unknown') {
        healthyShards++;
      }

      for (const replicaHealth of Object.values(health.replicaHealths)) {
        totalReplicas++;
        if (replicaHealth === 'healthy' || replicaHealth === 'unknown') {
          healthyReplicas++;
        }
      }
    }

    return {
      shards: shardHealths,
      healthyShards,
      totalShards,
      healthyReplicas,
      totalReplicas,
      lastUpdated: Date.now(),
    };
  }

  /**
   * Reset health state for a replica (e.g., after manual intervention)
   */
  resetHealth(shardId: string, replicaId: string): void {
    const state = this.getHealthState(shardId, replicaId);
    state.health = 'unknown';
    state.successCount = 0;
    state.failureCount = 0;
    state.circuitState = 'closed';
    state.circuitOpenTime = 0;
  }

  /**
   * Get raw health state for debugging
   */
  getHealthStateDebug(shardId: string, replicaId: string): ReplicaHealthState {
    return this.getHealthState(shardId, replicaId);
  }
}

// =============================================================================
// HEALTH CHECKER
// =============================================================================

/**
 * Active health checker that periodically checks replica health
 */
export class HealthChecker {
  private readonly selector: DefaultReplicaSelector;
  private readonly shards: ShardConfig[];
  private readonly checkFn: (shardId: string, replicaId: string) => Promise<number>;
  private readonly intervalMs: number;
  private intervalHandle?: ReturnType<typeof setInterval>;

  constructor(
    selector: DefaultReplicaSelector,
    shards: ShardConfig[],
    checkFn: (shardId: string, replicaId: string) => Promise<number>,
    intervalMs: number = 10000
  ) {
    this.selector = selector;
    this.shards = shards;
    this.checkFn = checkFn;
    this.intervalMs = intervalMs;
  }

  /**
   * Start periodic health checks
   */
  start(): void {
    if (this.intervalHandle) return;

    this.intervalHandle = setInterval(() => {
      this.checkAll().catch(console.error);
    }, this.intervalMs);

    // Run initial check
    this.checkAll().catch(console.error);
  }

  /**
   * Stop periodic health checks
   */
  stop(): void {
    if (this.intervalHandle) {
      clearInterval(this.intervalHandle);
      this.intervalHandle = undefined;
    }
  }

  /**
   * Run health check for all replicas
   */
  async checkAll(): Promise<void> {
    const checks: Promise<void>[] = [];

    for (const shard of this.shards) {
      // Check primary
      checks.push(this.checkReplica(shard.id, shard.doId ?? shard.id));

      // Check replicas
      for (const replica of shard.replicas ?? []) {
        checks.push(this.checkReplica(shard.id, replica.id));
      }
    }

    await Promise.allSettled(checks);
  }

  private async checkReplica(shardId: string, replicaId: string): Promise<void> {
    try {
      const latencyMs = await this.checkFn(shardId, replicaId);
      this.selector.recordSuccess(shardId, replicaId, latencyMs);
    } catch {
      this.selector.recordFailure(shardId, replicaId);
    }
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a replica selector
 */
export function createReplicaSelector(
  shards: ShardConfig[],
  config?: HealthConfig,
  currentRegion?: string
): DefaultReplicaSelector {
  return new DefaultReplicaSelector(shards, config, currentRegion);
}

/**
 * Create a health checker
 */
export function createHealthChecker(
  selector: DefaultReplicaSelector,
  shards: ShardConfig[],
  checkFn: (shardId: string, replicaId: string) => Promise<number>,
  intervalMs?: number
): HealthChecker {
  return new HealthChecker(selector, shards, checkFn, intervalMs);
}
