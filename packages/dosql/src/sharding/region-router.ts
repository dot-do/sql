/**
 * Cross-Region Read Replica Router for DoSQL
 *
 * Implements region-aware routing for read replicas with:
 * - Route reads to nearest replica based on geographic region
 * - Configurable staleness tolerance for eventual consistency
 * - Automatic primary fallback when replicas are stale
 * - Region health monitoring and failover
 *
 * @packageDocumentation
 */

import { createLogger } from '../logging/index.js';
import type { ShardId } from '../engine/types.js';
import type {
  ShardConfig,
  ReplicaConfig,
  ReplicaHealth,
  ReadPreference,
} from './types.js';

const logger = createLogger({ defaultContext: { module: 'region-router' } });

// =============================================================================
// REGION TYPES
// =============================================================================

/**
 * Geographic region identifier
 * Matches Cloudflare colo codes or custom region identifiers
 */
export type RegionId = string;

/**
 * Region latency information
 */
export interface RegionLatency {
  /** Target region */
  region: RegionId;
  /** Round-trip time in milliseconds */
  rttMs: number;
  /** Last updated timestamp */
  lastUpdated: number;
  /** Sample count for averaging */
  sampleCount: number;
}

/**
 * Replica lag information for staleness detection
 */
export interface ReplicaLag {
  /** Replica identifier */
  replicaId: string;
  /** Shard this replica belongs to */
  shardId: ShardId;
  /** Current LSN of the replica */
  replicaLSN: bigint;
  /** Primary LSN at the time of measurement */
  primaryLSN: bigint;
  /** Estimated lag in milliseconds */
  lagMs: number;
  /** Last measured timestamp */
  measuredAt: number;
}

/**
 * Region health status
 */
export interface RegionHealth {
  /** Region identifier */
  region: RegionId;
  /** Overall health status */
  status: 'healthy' | 'degraded' | 'unhealthy' | 'unknown';
  /** Number of healthy replicas in this region */
  healthyReplicaCount: number;
  /** Total number of replicas in this region */
  totalReplicaCount: number;
  /** Average latency to this region */
  avgLatencyMs: number;
  /** Average replica lag in this region */
  avgLagMs: number;
  /** Last health check timestamp */
  lastChecked: number;
}

/**
 * Replica selection result
 */
export interface ReplicaSelection {
  /** Selected shard ID */
  shardId: ShardId;
  /** Selected replica ID (undefined if using primary) */
  replicaId: string | undefined;
  /** Region of the selected replica */
  region: RegionId | undefined;
  /** Whether this is a fallback to primary */
  fallbackToPrimary: boolean;
  /** Reason for selection */
  reason: string;
  /** Estimated latency */
  estimatedLatencyMs: number;
  /** Estimated staleness */
  estimatedStalenessMs: number;
}

// =============================================================================
// REGION ROUTER CONFIGURATION
// =============================================================================

/**
 * Configuration for region-aware routing
 */
export interface RegionRouterConfig {
  /**
   * Maximum acceptable staleness in milliseconds
   * Replicas with higher lag will be skipped
   * @default 5000
   */
  maxStalenessMs: number;

  /**
   * Latency threshold for "near" replica selection (ms)
   * Replicas within this latency are considered equally near
   * @default 50
   */
  nearLatencyThresholdMs: number;

  /**
   * Weight factor for latency in replica selection (0-1)
   * Higher values prioritize lower latency over lower staleness
   * @default 0.7
   */
  latencyWeight: number;

  /**
   * Weight factor for staleness in replica selection (0-1)
   * Higher values prioritize lower staleness over lower latency
   * @default 0.3
   */
  stalenessWeight: number;

  /**
   * Enable automatic primary fallback when no healthy replicas available
   * @default true
   */
  automaticPrimaryFallback: boolean;

  /**
   * Health check interval in milliseconds
   * @default 10000
   */
  healthCheckIntervalMs: number;

  /**
   * Number of consecutive failures before marking region unhealthy
   * @default 3
   */
  failureThreshold: number;

  /**
   * Number of consecutive successes to recover from unhealthy
   * @default 2
   */
  recoveryThreshold: number;

  /**
   * Region latency cache TTL in milliseconds
   * @default 60000
   */
  latencyCacheTtlMs: number;

  /**
   * Prefer same-region replicas even if slightly slower
   * @default true
   */
  preferSameRegion: boolean;

  /**
   * Region affinity bonus in milliseconds
   * Reduces effective latency for same-region replicas
   * @default 20
   */
  regionAffinityBonusMs: number;
}

/**
 * Default region router configuration
 */
export const DEFAULT_REGION_ROUTER_CONFIG: Readonly<RegionRouterConfig> = {
  maxStalenessMs: 5000,
  nearLatencyThresholdMs: 50,
  latencyWeight: 0.7,
  stalenessWeight: 0.3,
  automaticPrimaryFallback: true,
  healthCheckIntervalMs: 10000,
  failureThreshold: 3,
  recoveryThreshold: 2,
  latencyCacheTtlMs: 60000,
  preferSameRegion: true,
  regionAffinityBonusMs: 20,
};

// =============================================================================
// REGION ROUTER IMPLEMENTATION
// =============================================================================

/**
 * Internal state for a tracked replica
 */
interface TrackedReplica {
  config: ReplicaConfig;
  shardId: ShardId;
  health: ReplicaHealth;
  lag: ReplicaLag | null;
  latencyMs: number;
  latencySamples: number[];
  consecutiveFailures: number;
  consecutiveSuccesses: number;
  lastSuccess: number;
  lastFailure: number;
}

/**
 * Internal state for a tracked region
 */
interface TrackedRegion {
  replicas: Map<string, TrackedReplica>;
  health: RegionHealth;
  latency: RegionLatency;
}

/**
 * Cross-Region Read Replica Router
 *
 * Provides intelligent routing to read replicas across geographic regions
 * with support for staleness tolerance, latency-based selection, and
 * automatic failover to primary.
 */
export class RegionRouter {
  private readonly config: RegionRouterConfig;
  private readonly shards: Map<ShardId, ShardConfig>;
  private readonly regions: Map<RegionId, TrackedRegion>;
  private readonly replicasByRegion: Map<RegionId, Set<string>>;
  private currentRegion: RegionId | undefined;
  private healthCheckInterval: ReturnType<typeof setInterval> | null = null;

  constructor(
    shards: ShardConfig[],
    config: Partial<RegionRouterConfig> = {},
    currentRegion?: RegionId
  ) {
    this.config = { ...DEFAULT_REGION_ROUTER_CONFIG, ...config };
    this.shards = new Map(shards.map(s => [s.id, s]));
    this.regions = new Map();
    this.replicasByRegion = new Map();
    this.currentRegion = currentRegion;

    // Index all replicas by region
    this.indexReplicas(shards);
  }

  /**
   * Index replicas from shard configurations by region
   */
  private indexReplicas(shards: ShardConfig[]): void {
    for (const shard of shards) {
      // Index primary as a special replica
      const primaryRegion = (shard.metadata?.['region'] as RegionId) ?? 'default';
      this.ensureRegion(primaryRegion);

      // Index shard replicas
      if (shard.replicas) {
        for (const replica of shard.replicas) {
          const region = replica.region ?? 'default';
          this.ensureRegion(region);

          const replicaKey = `${shard.id}:${replica.id}`;

          // Add to region index
          const regionReplicas = this.replicasByRegion.get(region) ?? new Set();
          regionReplicas.add(replicaKey);
          this.replicasByRegion.set(region, regionReplicas);

          // Create tracked replica
          const trackedRegion = this.regions.get(region)!;
          trackedRegion.replicas.set(replicaKey, {
            config: replica,
            shardId: shard.id,
            health: 'unknown',
            lag: null,
            latencyMs: Infinity,
            latencySamples: [],
            consecutiveFailures: 0,
            consecutiveSuccesses: 0,
            lastSuccess: 0,
            lastFailure: 0,
          });
        }
      }
    }

    // Update region health summaries
    this.updateAllRegionHealth();
  }

  /**
   * Ensure a region exists in the tracking structures
   */
  private ensureRegion(region: RegionId): void {
    if (this.regions.has(region)) return;

    this.regions.set(region, {
      replicas: new Map(),
      health: {
        region,
        status: 'unknown',
        healthyReplicaCount: 0,
        totalReplicaCount: 0,
        avgLatencyMs: Infinity,
        avgLagMs: 0,
        lastChecked: 0,
      },
      latency: {
        region,
        rttMs: Infinity,
        lastUpdated: 0,
        sampleCount: 0,
      },
    });

    this.replicasByRegion.set(region, new Set());
  }

  /**
   * Set the current region for routing decisions
   */
  setCurrentRegion(region: RegionId): void {
    this.currentRegion = region;
    logger.info('Current region updated', { region });
  }

  /**
   * Get the current region
   */
  getCurrentRegion(): RegionId | undefined {
    return this.currentRegion;
  }

  // ==========================================================================
  // REPLICA SELECTION
  // ==========================================================================

  /**
   * Select the best replica for a read operation
   *
   * @param shardId - Target shard ID
   * @param readPreference - Read preference (replica, nearest, etc.)
   * @param maxStalenessMs - Override max staleness (optional)
   * @returns Replica selection result
   */
  selectReplica(
    shardId: ShardId,
    readPreference: ReadPreference = 'replicaPreferred',
    maxStalenessMs?: number
  ): ReplicaSelection {
    const maxStaleness = maxStalenessMs ?? this.config.maxStalenessMs;
    const shard = this.shards.get(shardId);

    if (!shard) {
      logger.warn('Unknown shard requested', { shardId });
      return this.createPrimaryFallback(shardId, 'Unknown shard');
    }

    // Handle primary-only preferences
    if (readPreference === 'primary') {
      return this.createPrimaryFallback(shardId, 'Primary read preference');
    }

    // Collect candidate replicas
    const candidates = this.collectCandidates(shard, maxStaleness);

    if (candidates.length === 0) {
      // No valid replicas, check if we should fallback to primary
      if (readPreference === 'replica') {
        logger.warn('No healthy replicas available and replica preference set', { shardId });
        // Strict replica preference - return error-like result
        return {
          shardId,
          replicaId: undefined,
          region: undefined,
          fallbackToPrimary: false,
          reason: 'No healthy replicas available',
          estimatedLatencyMs: Infinity,
          estimatedStalenessMs: Infinity,
        };
      }

      return this.createPrimaryFallback(shardId, 'No healthy replicas available');
    }

    // Select based on preference
    switch (readPreference) {
      case 'nearest':
        return this.selectNearest(shardId, candidates);
      case 'replica':
      case 'replicaPreferred':
        return this.selectBestReplica(shardId, candidates);
      case 'primaryPreferred':
        return this.selectPrimaryPreferred(shardId, candidates);
      case 'analytics':
        return this.selectAnalytics(shardId, candidates);
      default:
        return this.selectBestReplica(shardId, candidates);
    }
  }

  /**
   * Collect valid candidate replicas for a shard
   */
  private collectCandidates(shard: ShardConfig, maxStaleness: number): TrackedReplica[] {
    const candidates: TrackedReplica[] = [];

    if (!shard.replicas) return candidates;

    for (const replica of shard.replicas) {
      // Skip primary role replicas
      if (replica.role === 'primary') continue;

      const region = replica.region ?? 'default';
      const trackedRegion = this.regions.get(region);
      if (!trackedRegion) continue;

      const replicaKey = `${shard.id}:${replica.id}`;
      const tracked = trackedRegion.replicas.get(replicaKey);
      if (!tracked) continue;

      // Check health
      if (tracked.health === 'unhealthy') continue;

      // Check staleness
      const staleness = tracked.lag?.lagMs ?? 0;
      if (staleness > maxStaleness) {
        logger.debug('Replica skipped due to staleness', {
          replicaId: replica.id,
          staleness,
          maxStaleness,
        });
        continue;
      }

      candidates.push(tracked);
    }

    return candidates;
  }

  /**
   * Select the nearest replica by latency
   */
  private selectNearest(shardId: ShardId, candidates: TrackedReplica[]): ReplicaSelection {
    let best: TrackedReplica | null = null;
    let bestScore = Infinity;

    for (const candidate of candidates) {
      let effectiveLatency = candidate.latencyMs;

      // Apply region affinity bonus
      if (this.config.preferSameRegion &&
          this.currentRegion &&
          candidate.config.region === this.currentRegion) {
        effectiveLatency -= this.config.regionAffinityBonusMs;
      }

      if (effectiveLatency < bestScore) {
        bestScore = effectiveLatency;
        best = candidate;
      }
    }

    if (!best) {
      return this.createPrimaryFallback(shardId, 'No candidates passed selection');
    }

    return {
      shardId,
      replicaId: best.config.id,
      region: best.config.region,
      fallbackToPrimary: false,
      reason: 'Nearest replica by latency',
      estimatedLatencyMs: best.latencyMs,
      estimatedStalenessMs: best.lag?.lagMs ?? 0,
    };
  }

  /**
   * Select the best replica considering both latency and staleness
   */
  private selectBestReplica(shardId: ShardId, candidates: TrackedReplica[]): ReplicaSelection {
    let best: TrackedReplica | null = null;
    let bestScore = Infinity;

    // Normalize latencies and staleness for scoring
    const maxLatency = Math.max(...candidates.map(c => c.latencyMs), 1);
    const maxStaleness = Math.max(...candidates.map(c => c.lag?.lagMs ?? 0), 1);

    for (const candidate of candidates) {
      // Calculate weighted score (lower is better)
      const normalizedLatency = candidate.latencyMs / maxLatency;
      const normalizedStaleness = (candidate.lag?.lagMs ?? 0) / maxStaleness;

      let score =
        normalizedLatency * this.config.latencyWeight +
        normalizedStaleness * this.config.stalenessWeight;

      // Apply region affinity bonus
      if (this.config.preferSameRegion &&
          this.currentRegion &&
          candidate.config.region === this.currentRegion) {
        score *= 0.8; // 20% bonus for same region
      }

      if (score < bestScore) {
        bestScore = score;
        best = candidate;
      }
    }

    if (!best) {
      return this.createPrimaryFallback(shardId, 'No candidates passed selection');
    }

    return {
      shardId,
      replicaId: best.config.id,
      region: best.config.region,
      fallbackToPrimary: false,
      reason: 'Best replica by weighted score',
      estimatedLatencyMs: best.latencyMs,
      estimatedStalenessMs: best.lag?.lagMs ?? 0,
    };
  }

  /**
   * Select with primary preference (use replica if primary unavailable)
   */
  private selectPrimaryPreferred(shardId: ShardId, candidates: TrackedReplica[]): ReplicaSelection {
    // Check if primary is healthy (not tracked as replica, so always prefer it)
    const shard = this.shards.get(shardId);
    if (shard && !shard.readOnly) {
      return this.createPrimaryFallback(shardId, 'Primary preferred and available');
    }

    // Primary not available or read-only, use best replica
    return this.selectBestReplica(shardId, candidates);
  }

  /**
   * Select analytics replica
   */
  private selectAnalytics(shardId: ShardId, candidates: TrackedReplica[]): ReplicaSelection {
    // Filter for analytics replicas
    const analyticsCandidates = candidates.filter(c => c.config.role === 'analytics');

    if (analyticsCandidates.length > 0) {
      return this.selectBestReplica(shardId, analyticsCandidates);
    }

    // Fallback to regular replicas if no analytics replicas available
    return this.selectBestReplica(shardId, candidates);
  }

  /**
   * Create a primary fallback result
   */
  private createPrimaryFallback(shardId: ShardId, reason: string): ReplicaSelection {
    const shard = this.shards.get(shardId);
    const primaryRegion = (shard?.metadata?.['region'] as RegionId) ?? undefined;

    return {
      shardId,
      replicaId: undefined,
      region: primaryRegion,
      fallbackToPrimary: true,
      reason,
      estimatedLatencyMs: 0, // Primary is assumed to be authoritative
      estimatedStalenessMs: 0,
    };
  }

  // ==========================================================================
  // REPLICA LAG TRACKING
  // ==========================================================================

  /**
   * Update replica lag information
   */
  updateReplicaLag(
    shardId: ShardId,
    replicaId: string,
    replicaLSN: bigint,
    primaryLSN: bigint,
    lagMs: number
  ): void {
    const shard = this.shards.get(shardId);
    if (!shard) return;

    const replica = shard.replicas?.find(r => r.id === replicaId);
    if (!replica) return;

    const region = replica.region ?? 'default';
    const trackedRegion = this.regions.get(region);
    if (!trackedRegion) return;

    const replicaKey = `${shardId}:${replicaId}`;
    const tracked = trackedRegion.replicas.get(replicaKey);
    if (!tracked) return;

    tracked.lag = {
      replicaId,
      shardId,
      replicaLSN,
      primaryLSN,
      lagMs,
      measuredAt: Date.now(),
    };

    // Check if lag exceeds threshold
    if (lagMs > this.config.maxStalenessMs) {
      logger.warn('Replica lag exceeds threshold', {
        replicaId,
        shardId,
        lagMs,
        threshold: this.config.maxStalenessMs,
      });
    }

    // Update region health
    this.updateRegionHealth(region);
  }

  /**
   * Get current lag for a replica
   */
  getReplicaLag(shardId: ShardId, replicaId: string): ReplicaLag | null {
    const shard = this.shards.get(shardId);
    if (!shard) return null;

    const replica = shard.replicas?.find(r => r.id === replicaId);
    if (!replica) return null;

    const region = replica.region ?? 'default';
    const trackedRegion = this.regions.get(region);
    if (!trackedRegion) return null;

    const replicaKey = `${shardId}:${replicaId}`;
    const tracked = trackedRegion.replicas.get(replicaKey);
    return tracked?.lag ?? null;
  }

  /**
   * Check if a replica is within staleness bounds
   */
  isReplicaFresh(shardId: ShardId, replicaId: string, maxStalenessMs?: number): boolean {
    const lag = this.getReplicaLag(shardId, replicaId);
    if (!lag) return false;

    const maxStaleness = maxStalenessMs ?? this.config.maxStalenessMs;
    return lag.lagMs <= maxStaleness;
  }

  // ==========================================================================
  // LATENCY TRACKING
  // ==========================================================================

  /**
   * Record a latency sample for a replica
   */
  recordLatency(shardId: ShardId, replicaId: string, latencyMs: number): void {
    const shard = this.shards.get(shardId);
    if (!shard) return;

    const replica = shard.replicas?.find(r => r.id === replicaId);
    if (!replica) return;

    const region = replica.region ?? 'default';
    const trackedRegion = this.regions.get(region);
    if (!trackedRegion) return;

    const replicaKey = `${shardId}:${replicaId}`;
    const tracked = trackedRegion.replicas.get(replicaKey);
    if (!tracked) return;

    // Add sample with sliding window
    tracked.latencySamples.push(latencyMs);
    if (tracked.latencySamples.length > 100) {
      tracked.latencySamples.shift();
    }

    // Update average
    tracked.latencyMs = tracked.latencySamples.reduce((a, b) => a + b, 0) /
                        tracked.latencySamples.length;

    // Update region latency
    this.updateRegionLatency(region);
  }

  /**
   * Get average latency for a region
   */
  getRegionLatency(region: RegionId): number {
    const trackedRegion = this.regions.get(region);
    return trackedRegion?.latency.rttMs ?? Infinity;
  }

  /**
   * Update region latency from replica latencies
   */
  private updateRegionLatency(region: RegionId): void {
    const trackedRegion = this.regions.get(region);
    if (!trackedRegion) return;

    const latencies: number[] = [];
    for (const replica of trackedRegion.replicas.values()) {
      if (replica.latencyMs < Infinity) {
        latencies.push(replica.latencyMs);
      }
    }

    if (latencies.length > 0) {
      trackedRegion.latency.rttMs = latencies.reduce((a, b) => a + b, 0) / latencies.length;
      trackedRegion.latency.lastUpdated = Date.now();
      trackedRegion.latency.sampleCount = latencies.length;
    }
  }

  // ==========================================================================
  // HEALTH MONITORING
  // ==========================================================================

  /**
   * Record a successful request to a replica
   */
  recordSuccess(shardId: ShardId, replicaId: string, latencyMs?: number): void {
    const shard = this.shards.get(shardId);
    if (!shard) return;

    const replica = shard.replicas?.find(r => r.id === replicaId);
    if (!replica) return;

    const region = replica.region ?? 'default';
    const trackedRegion = this.regions.get(region);
    if (!trackedRegion) return;

    const replicaKey = `${shardId}:${replicaId}`;
    const tracked = trackedRegion.replicas.get(replicaKey);
    if (!tracked) return;

    tracked.consecutiveSuccesses++;
    tracked.consecutiveFailures = 0;
    tracked.lastSuccess = Date.now();

    // Update health status
    if (tracked.health === 'unhealthy' &&
        tracked.consecutiveSuccesses >= this.config.recoveryThreshold) {
      tracked.health = 'degraded';
      logger.info('Replica recovering from unhealthy', { replicaId, shardId });
    } else if (tracked.health === 'degraded' &&
               tracked.consecutiveSuccesses >= this.config.recoveryThreshold * 2) {
      tracked.health = 'healthy';
      logger.info('Replica recovered to healthy', { replicaId, shardId });
    } else if (tracked.health === 'unknown' && tracked.consecutiveSuccesses >= 1) {
      tracked.health = 'healthy';
    }

    if (latencyMs !== undefined) {
      this.recordLatency(shardId, replicaId, latencyMs);
    }

    this.updateRegionHealth(region);
  }

  /**
   * Record a failed request to a replica
   */
  recordFailure(shardId: ShardId, replicaId: string): void {
    const shard = this.shards.get(shardId);
    if (!shard) return;

    const replica = shard.replicas?.find(r => r.id === replicaId);
    if (!replica) return;

    const region = replica.region ?? 'default';
    const trackedRegion = this.regions.get(region);
    if (!trackedRegion) return;

    const replicaKey = `${shardId}:${replicaId}`;
    const tracked = trackedRegion.replicas.get(replicaKey);
    if (!tracked) return;

    tracked.consecutiveFailures++;
    tracked.consecutiveSuccesses = 0;
    tracked.lastFailure = Date.now();

    // Update health status
    if (tracked.consecutiveFailures >= this.config.failureThreshold) {
      tracked.health = 'unhealthy';
      logger.warn('Replica marked unhealthy', {
        replicaId,
        shardId,
        consecutiveFailures: tracked.consecutiveFailures
      });
    } else if (tracked.consecutiveFailures >= Math.ceil(this.config.failureThreshold / 2)) {
      tracked.health = 'degraded';
    }

    this.updateRegionHealth(region);
  }

  /**
   * Update health summary for a region
   */
  private updateRegionHealth(region: RegionId): void {
    const trackedRegion = this.regions.get(region);
    if (!trackedRegion) return;

    let healthyCount = 0;
    let totalCount = 0;
    let totalLatency = 0;
    let totalLag = 0;
    let lagCount = 0;

    for (const replica of trackedRegion.replicas.values()) {
      totalCount++;
      if (replica.health === 'healthy' || replica.health === 'unknown') {
        healthyCount++;
      }
      if (replica.latencyMs < Infinity) {
        totalLatency += replica.latencyMs;
      }
      if (replica.lag) {
        totalLag += replica.lag.lagMs;
        lagCount++;
      }
    }

    trackedRegion.health = {
      region,
      status: this.calculateRegionStatus(healthyCount, totalCount),
      healthyReplicaCount: healthyCount,
      totalReplicaCount: totalCount,
      avgLatencyMs: totalCount > 0 ? totalLatency / totalCount : Infinity,
      avgLagMs: lagCount > 0 ? totalLag / lagCount : 0,
      lastChecked: Date.now(),
    };
  }

  /**
   * Calculate region status from replica health counts
   */
  private calculateRegionStatus(
    healthyCount: number,
    totalCount: number
  ): 'healthy' | 'degraded' | 'unhealthy' | 'unknown' {
    if (totalCount === 0) return 'unknown';

    const healthRatio = healthyCount / totalCount;

    if (healthRatio >= 0.8) return 'healthy';
    if (healthRatio >= 0.5) return 'degraded';
    if (healthRatio > 0) return 'degraded';
    return 'unhealthy';
  }

  /**
   * Update all region health summaries
   */
  private updateAllRegionHealth(): void {
    for (const region of this.regions.keys()) {
      this.updateRegionHealth(region);
    }
  }

  /**
   * Get health status for a region
   */
  getRegionHealth(region: RegionId): RegionHealth | null {
    return this.regions.get(region)?.health ?? null;
  }

  /**
   * Get all regions with their health status
   */
  getAllRegionHealth(): RegionHealth[] {
    return Array.from(this.regions.values()).map(r => r.health);
  }

  /**
   * Get health status for a specific replica
   */
  getReplicaHealth(shardId: ShardId, replicaId: string): ReplicaHealth {
    const shard = this.shards.get(shardId);
    if (!shard) return 'unknown';

    const replica = shard.replicas?.find(r => r.id === replicaId);
    if (!replica) return 'unknown';

    const region = replica.region ?? 'default';
    const trackedRegion = this.regions.get(region);
    if (!trackedRegion) return 'unknown';

    const replicaKey = `${shardId}:${replicaId}`;
    const tracked = trackedRegion.replicas.get(replicaKey);
    return tracked?.health ?? 'unknown';
  }

  // ==========================================================================
  // HEALTH CHECK MANAGEMENT
  // ==========================================================================

  /**
   * Start periodic health checks
   */
  startHealthChecks(
    checkFn: (shardId: ShardId, replicaId: string) => Promise<{ latencyMs: number; lagMs: number }>
  ): void {
    if (this.healthCheckInterval) {
      this.stopHealthChecks();
    }

    this.healthCheckInterval = setInterval(async () => {
      await this.runHealthChecks(checkFn);
    }, this.config.healthCheckIntervalMs);

    // Run initial check
    this.runHealthChecks(checkFn).catch(err => {
      logger.error('Initial health check failed', err);
    });
  }

  /**
   * Stop periodic health checks
   */
  stopHealthChecks(): void {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
    }
  }

  /**
   * Run health checks on all replicas
   */
  private async runHealthChecks(
    checkFn: (shardId: ShardId, replicaId: string) => Promise<{ latencyMs: number; lagMs: number }>
  ): Promise<void> {
    const checks: Promise<void>[] = [];

    for (const [, trackedRegion] of this.regions) {
      for (const [, tracked] of trackedRegion.replicas) {
        checks.push(
          this.checkReplica(tracked, checkFn)
        );
      }
    }

    await Promise.allSettled(checks);
    this.updateAllRegionHealth();
  }

  /**
   * Check a single replica's health
   */
  private async checkReplica(
    tracked: TrackedReplica,
    checkFn: (shardId: ShardId, replicaId: string) => Promise<{ latencyMs: number; lagMs: number }>
  ): Promise<void> {
    try {
      const result = await checkFn(tracked.shardId, tracked.config.id);
      this.recordSuccess(tracked.shardId, tracked.config.id, result.latencyMs);

      // Update lag with a synthetic LSN difference based on lag time
      // In real usage, the checkFn should return actual LSN values
      this.updateReplicaLag(
        tracked.shardId,
        tracked.config.id,
        0n, // Placeholder - should come from actual replication state
        BigInt(result.lagMs), // Placeholder
        result.lagMs
      );
    } catch {
      this.recordFailure(tracked.shardId, tracked.config.id);
    }
  }

  // ==========================================================================
  // METRICS AND DEBUGGING
  // ==========================================================================

  /**
   * Get routing metrics summary
   */
  getMetrics(): {
    regions: number;
    totalReplicas: number;
    healthyReplicas: number;
    avgLatencyMs: number;
    avgLagMs: number;
  } {
    let totalReplicas = 0;
    let healthyReplicas = 0;
    let totalLatency = 0;
    let totalLag = 0;
    let latencyCount = 0;
    let lagCount = 0;

    for (const trackedRegion of this.regions.values()) {
      for (const replica of trackedRegion.replicas.values()) {
        totalReplicas++;
        if (replica.health === 'healthy' || replica.health === 'unknown') {
          healthyReplicas++;
        }
        if (replica.latencyMs < Infinity) {
          totalLatency += replica.latencyMs;
          latencyCount++;
        }
        if (replica.lag) {
          totalLag += replica.lag.lagMs;
          lagCount++;
        }
      }
    }

    return {
      regions: this.regions.size,
      totalReplicas,
      healthyReplicas,
      avgLatencyMs: latencyCount > 0 ? totalLatency / latencyCount : 0,
      avgLagMs: lagCount > 0 ? totalLag / lagCount : 0,
    };
  }

  /**
   * Get all replicas for a region
   */
  getReplicasInRegion(region: RegionId): Array<{
    shardId: ShardId;
    replicaId: string;
    health: ReplicaHealth;
    latencyMs: number;
    lagMs: number;
  }> {
    const trackedRegion = this.regions.get(region);
    if (!trackedRegion) return [];

    return Array.from(trackedRegion.replicas.values()).map(r => ({
      shardId: r.shardId,
      replicaId: r.config.id,
      health: r.health,
      latencyMs: r.latencyMs,
      lagMs: r.lag?.lagMs ?? 0,
    }));
  }

  /**
   * Reset all health states (useful for testing or manual recovery)
   */
  resetHealthStates(): void {
    for (const trackedRegion of this.regions.values()) {
      for (const replica of trackedRegion.replicas.values()) {
        replica.health = 'unknown';
        replica.consecutiveFailures = 0;
        replica.consecutiveSuccesses = 0;
        replica.latencySamples = [];
        replica.latencyMs = Infinity;
        replica.lag = null;
      }
    }

    this.updateAllRegionHealth();
    logger.info('All health states reset');
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a region router
 */
export function createRegionRouter(
  shards: ShardConfig[],
  config?: Partial<RegionRouterConfig>,
  currentRegion?: RegionId
): RegionRouter {
  return new RegionRouter(shards, config, currentRegion);
}

// =============================================================================
// REGION UTILITIES
// =============================================================================

/**
 * Common region identifiers based on Cloudflare colo codes
 * Maps to broader geographic regions for grouping
 */
export const REGION_MAPPINGS: Readonly<Record<string, RegionId>> = {
  // North America
  'SJC': 'us-west',
  'LAX': 'us-west',
  'SEA': 'us-west',
  'DFW': 'us-central',
  'ORD': 'us-central',
  'IAD': 'us-east',
  'EWR': 'us-east',
  'ATL': 'us-east',
  'MIA': 'us-east',
  'YYZ': 'us-east',

  // Europe
  'LHR': 'eu-west',
  'CDG': 'eu-west',
  'AMS': 'eu-west',
  'FRA': 'eu-central',
  'ARN': 'eu-north',

  // Asia Pacific
  'NRT': 'asia-northeast',
  'HND': 'asia-northeast',
  'ICN': 'asia-northeast',
  'SIN': 'asia-southeast',
  'HKG': 'asia-east',
  'SYD': 'oceania',
  'MEL': 'oceania',

  // South America
  'GRU': 'south-america',
  'EZE': 'south-america',
};

/**
 * Map a Cloudflare colo code to a region identifier
 */
export function coloToRegion(colo: string): RegionId {
  return REGION_MAPPINGS[colo.toUpperCase()] ?? 'default';
}

/**
 * Calculate distance-based latency estimate between regions
 * Returns estimated RTT in milliseconds
 */
export function estimateInterRegionLatency(from: RegionId, to: RegionId): number {
  if (from === to) return 0;

  // Simplified latency estimates based on typical network paths
  const latencyMatrix: Record<string, Record<string, number>> = {
    'us-west': { 'us-east': 70, 'us-central': 40, 'eu-west': 140, 'asia-northeast': 120 },
    'us-east': { 'us-west': 70, 'us-central': 30, 'eu-west': 80, 'asia-northeast': 180 },
    'us-central': { 'us-west': 40, 'us-east': 30, 'eu-west': 100, 'asia-northeast': 150 },
    'eu-west': { 'us-east': 80, 'us-west': 140, 'eu-central': 20, 'asia-northeast': 200 },
    'eu-central': { 'eu-west': 20, 'us-east': 90, 'asia-northeast': 180 },
    'asia-northeast': { 'us-west': 120, 'asia-southeast': 60, 'asia-east': 30, 'oceania': 100 },
    'asia-southeast': { 'asia-northeast': 60, 'asia-east': 40, 'oceania': 100 },
    'asia-east': { 'asia-northeast': 30, 'asia-southeast': 40 },
    'oceania': { 'asia-northeast': 100, 'us-west': 140 },
    'south-america': { 'us-east': 120, 'eu-west': 200 },
  };

  return latencyMatrix[from]?.[to] ?? latencyMatrix[to]?.[from] ?? 200;
}
