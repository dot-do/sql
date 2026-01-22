/**
 * Replication Router for Multi-Region Read/Write Routing
 *
 * Handles:
 * - Read routing based on consistency levels
 * - Write routing to primary
 * - Nearest replica selection
 * - Health-aware routing
 * - Session-based routing for read-your-writes
 *
 * @packageDocumentation
 */

import {
  type ReplicationRouter,
  type ReplicaId,
  type ReplicaInfo,
  type ReplicaStatus,
  type ConsistencyLevel,
  type ReadPreference,
  type SessionState,
  type RoutingDecision,
  type ReplicationLag,
  type RouterMetrics,
  type ReplicationConfig,
  DEFAULT_REPLICATION_CONFIG,
  replicaIdsEqual,
  serializeReplicaId,
} from './types.js';

// =============================================================================
// ROUTER STATE
// =============================================================================

/**
 * Internal replica state for routing
 */
interface RoutableReplica {
  info: ReplicaInfo;
  lag?: ReplicationLag;
  avgLatencyMs: number;
  latencySamples: number[];
  lastUpdated: number;
}

// =============================================================================
// ROUTER IMPLEMENTATION
// =============================================================================

/**
 * Create a replication router
 */
export function createReplicationRouter(
  primaryId: ReplicaId,
  config: Partial<ReplicationConfig> = {}
): ReplicationRouter {
  const fullConfig: ReplicationConfig = { ...DEFAULT_REPLICATION_CONFIG, ...config };

  // State
  const replicas = new Map<string, RoutableReplica>();
  let primaryInfo: RoutableReplica | null = null;
  let currentRegion: string | undefined;

  // Metrics
  let totalReads = 0;
  let primaryReads = 0;
  let replicaReads = 0;
  let totalWrites = 0;
  let failovers = 0;
  let routingLatencySamples: number[] = [];

  // Initialize primary
  const primaryKey = serializeReplicaId(primaryId);
  primaryInfo = {
    info: {
      id: primaryId,
      status: 'active',
      role: 'primary',
      lastLSN: 0n,
      lastHeartbeat: Date.now(),
      registeredAt: Date.now(),
      doUrl: '',
    },
    avgLatencyMs: 0,
    latencySamples: [],
    lastUpdated: Date.now(),
  };
  replicas.set(primaryKey, primaryInfo);

  // ==========================================================================
  // ROUTING LOGIC
  // ==========================================================================

  async function routeRead(
    sql: string,
    consistency: ConsistencyLevel,
    session?: SessionState
  ): Promise<RoutingDecision> {
    const startTime = performance.now();
    totalReads++;

    let target: ReplicaId;
    let reason: string;
    let fallback = false;

    switch (consistency) {
      case 'strong':
        // Always route to primary
        target = primaryId;
        reason = 'Strong consistency requires primary';
        primaryReads++;
        break;

      case 'session':
        if (session) {
          // Find replica that has caught up to session's last write
          const caughtUpReplica = findReplicaCaughtUpTo(session.lastWriteLSN);
          if (caughtUpReplica) {
            target = caughtUpReplica.info.id;
            reason = 'Session consistency - replica caught up to session LSN';
            replicaReads++;
          } else {
            // Fall back to primary
            target = primaryId;
            reason = 'Session consistency - no replica caught up, falling back to primary';
            primaryReads++;
            fallback = true;
          }
        } else {
          // No session, treat as eventual
          const nearest = findNearestHealthyReplica();
          if (nearest) {
            target = nearest.info.id;
            reason = 'Session consistency without session - using nearest replica';
            replicaReads++;
          } else {
            target = primaryId;
            reason = 'Session consistency - no healthy replica, falling back to primary';
            primaryReads++;
            fallback = true;
          }
        }
        break;

      case 'bounded':
        // Find replica within staleness bounds
        const boundedReplica = findReplicaWithinBounds();
        if (boundedReplica) {
          target = boundedReplica.info.id;
          reason = `Bounded staleness - replica lag ${boundedReplica.lag?.lagMs ?? 0}ms within ${fullConfig.boundedStalenessMs}ms`;
          replicaReads++;
        } else {
          target = primaryId;
          reason = 'Bounded staleness - no replica within bounds, falling back to primary';
          primaryReads++;
          fallback = true;
        }
        break;

      case 'eventual':
      default:
        // Find nearest healthy replica
        const nearest = findNearestHealthyReplica();
        if (nearest) {
          target = nearest.info.id;
          reason = `Eventual consistency - nearest healthy replica (${nearest.avgLatencyMs}ms avg latency)`;
          replicaReads++;
        } else {
          target = primaryId;
          reason = 'Eventual consistency - no healthy replica, falling back to primary';
          primaryReads++;
          fallback = true;
        }
        break;
    }

    const routingTime = performance.now() - startTime;
    updateRoutingLatency(routingTime);

    const targetReplica = replicas.get(serializeReplicaId(target));

    return {
      target,
      consistency,
      reason,
      estimatedLatencyMs: targetReplica?.avgLatencyMs,
      fallback,
    };
  }

  async function routeWrite(sql: string): Promise<RoutingDecision> {
    const startTime = performance.now();
    totalWrites++;

    // Writes always go to primary
    const routingTime = performance.now() - startTime;
    updateRoutingLatency(routingTime);

    return {
      target: primaryId,
      consistency: 'strong',
      reason: 'Write operations always route to primary',
      estimatedLatencyMs: primaryInfo?.avgLatencyMs,
      fallback: false,
    };
  }

  // ==========================================================================
  // REPLICA SELECTION HELPERS
  // ==========================================================================

  function findNearestHealthyReplica(): RoutableReplica | null {
    let nearest: RoutableReplica | null = null;
    let bestScore = Infinity;

    for (const replica of replicas.values()) {
      if (!isHealthy(replica)) continue;
      if (replica.info.role === 'primary') continue;

      // Calculate score based on latency and region preference
      let score = replica.avgLatencyMs;

      // Bonus for same region
      if (currentRegion && replica.info.id.region === currentRegion) {
        score *= 0.5; // 50% bonus for same region
      }

      if (score < bestScore) {
        bestScore = score;
        nearest = replica;
      }
    }

    return nearest;
  }

  function findReplicaCaughtUpTo(lsn: bigint): RoutableReplica | null {
    let best: RoutableReplica | null = null;
    let bestLatency = Infinity;

    for (const replica of replicas.values()) {
      if (!isHealthy(replica)) continue;
      if (replica.info.role === 'primary') continue;
      if (replica.info.lastLSN < lsn) continue;

      if (replica.avgLatencyMs < bestLatency) {
        bestLatency = replica.avgLatencyMs;
        best = replica;
      }
    }

    return best;
  }

  function findReplicaWithinBounds(): RoutableReplica | null {
    let best: RoutableReplica | null = null;
    let bestLatency = Infinity;

    for (const replica of replicas.values()) {
      if (!isHealthy(replica)) continue;
      if (replica.info.role === 'primary') continue;

      // Check if lag is within bounds
      const lagMs = replica.lag?.lagMs ?? 0;
      if (lagMs > fullConfig.boundedStalenessMs) continue;

      if (replica.avgLatencyMs < bestLatency) {
        bestLatency = replica.avgLatencyMs;
        best = replica;
      }
    }

    return best;
  }

  function isHealthy(replica: RoutableReplica): boolean {
    const { status } = replica.info;
    return status === 'active' || status === 'syncing';
  }

  // ==========================================================================
  // REPLICA MANAGEMENT
  // ==========================================================================

  async function getNearestReplica(region: string): Promise<ReplicaId | null> {
    // First, try to find a replica in the same region
    for (const replica of replicas.values()) {
      if (replica.info.id.region === region && isHealthy(replica)) {
        return replica.info.id;
      }
    }

    // Fall back to nearest healthy replica
    const nearest = findNearestHealthyReplica();
    return nearest?.info.id ?? null;
  }

  function updateReplicaStatus(
    replicaId: ReplicaId,
    status: ReplicaStatus,
    lag?: ReplicationLag
  ): void {
    const key = serializeReplicaId(replicaId);
    let replica = replicas.get(key);

    if (!replica) {
      // Create new entry
      replica = {
        info: {
          id: replicaId,
          status,
          role: 'replica',
          lastLSN: lag?.replicaLSN ?? 0n,
          lastHeartbeat: Date.now(),
          registeredAt: Date.now(),
          doUrl: '',
        },
        lag,
        avgLatencyMs: 0,
        latencySamples: [],
        lastUpdated: Date.now(),
      };
      replicas.set(key, replica);
    } else {
      replica.info.status = status;
      replica.info.lastHeartbeat = Date.now();
      if (lag) {
        replica.lag = lag;
        replica.info.lastLSN = lag.replicaLSN;
      }
      replica.lastUpdated = Date.now();
    }

    // Track status changes for failover counting
    if (status === 'offline' && replica.info.role === 'primary') {
      failovers++;
    }
  }

  // ==========================================================================
  // LATENCY TRACKING
  // ==========================================================================

  function updateRoutingLatency(latencyMs: number): void {
    routingLatencySamples.push(latencyMs);
    if (routingLatencySamples.length > 1000) {
      routingLatencySamples.shift();
    }
  }

  /**
   * Record latency sample for a replica
   */
  function recordLatency(replicaId: ReplicaId, latencyMs: number): void {
    const key = serializeReplicaId(replicaId);
    const replica = replicas.get(key);

    if (replica) {
      replica.latencySamples.push(latencyMs);
      if (replica.latencySamples.length > 100) {
        replica.latencySamples.shift();
      }

      // Update average
      replica.avgLatencyMs = replica.latencySamples.reduce((a, b) => a + b, 0) /
        replica.latencySamples.length;
    }
  }

  // ==========================================================================
  // METRICS
  // ==========================================================================

  function getMetrics(): RouterMetrics {
    const avgRoutingLatencyMs = routingLatencySamples.length > 0
      ? routingLatencySamples.reduce((a, b) => a + b, 0) / routingLatencySamples.length
      : 0;

    return {
      totalReads,
      primaryReads,
      replicaReads,
      totalWrites,
      failovers,
      avgRoutingLatencyMs,
    };
  }

  // ==========================================================================
  // ADDITIONAL METHODS
  // ==========================================================================

  /**
   * Set the current region for routing decisions
   */
  function setCurrentRegion(region: string): void {
    currentRegion = region;
  }

  /**
   * Register a replica with the router
   */
  function registerReplica(info: ReplicaInfo): void {
    const key = serializeReplicaId(info.id);
    replicas.set(key, {
      info,
      avgLatencyMs: 0,
      latencySamples: [],
      lastUpdated: Date.now(),
    });
  }

  /**
   * Deregister a replica from the router
   */
  function deregisterReplica(replicaId: ReplicaId): void {
    const key = serializeReplicaId(replicaId);
    replicas.delete(key);
  }

  /**
   * Get all registered replicas
   */
  function getReplicas(): ReplicaInfo[] {
    return Array.from(replicas.values()).map(r => r.info);
  }

  /**
   * Get primary replica info
   */
  function getPrimary(): ReplicaInfo | null {
    return primaryInfo?.info ?? null;
  }

  // Return interface with extensions
  return {
    routeRead,
    routeWrite,
    getNearestReplica,
    updateReplicaStatus,
    getMetrics,
    // Extended methods accessible via casting
    setCurrentRegion: setCurrentRegion as any,
    registerReplica: registerReplica as any,
    deregisterReplica: deregisterReplica as any,
    getReplicas: getReplicas as any,
    getPrimary: getPrimary as any,
    recordLatency: recordLatency as any,
  } as ReplicationRouter;
}

// =============================================================================
// EXTENDED ROUTER INTERFACE
// =============================================================================

/**
 * Extended router interface with additional methods
 */
export interface ExtendedReplicationRouter extends ReplicationRouter {
  /** Set the current region for routing decisions */
  setCurrentRegion(region: string): void;
  /** Register a replica with the router */
  registerReplica(info: ReplicaInfo): void;
  /** Deregister a replica from the router */
  deregisterReplica(replicaId: ReplicaId): void;
  /** Get all registered replicas */
  getReplicas(): ReplicaInfo[];
  /** Get primary replica info */
  getPrimary(): ReplicaInfo | null;
  /** Record latency sample for a replica */
  recordLatency(replicaId: ReplicaId, latencyMs: number): void;
}

/**
 * Create an extended replication router
 */
export function createExtendedRouter(
  primaryId: ReplicaId,
  config?: Partial<ReplicationConfig>
): ExtendedReplicationRouter {
  return createReplicationRouter(primaryId, config) as ExtendedReplicationRouter;
}

// =============================================================================
// LOAD BALANCING STRATEGIES
// =============================================================================

/**
 * Load balancing strategy
 */
export type LoadBalancingStrategy = 'round-robin' | 'random' | 'least-connections' | 'latency-weighted';

/**
 * Create a load-balanced router with multiple strategies
 */
export function createLoadBalancedRouter(
  primaryId: ReplicaId,
  strategy: LoadBalancingStrategy = 'latency-weighted',
  config?: Partial<ReplicationConfig>
): ExtendedReplicationRouter {
  const baseRouter = createExtendedRouter(primaryId, config);

  // Track connections for least-connections strategy
  const connectionCounts = new Map<string, number>();
  let roundRobinIndex = 0;

  // Override getNearestReplica based on strategy
  const originalGetNearestReplica = baseRouter.getNearestReplica.bind(baseRouter);

  baseRouter.getNearestReplica = async (region: string): Promise<ReplicaId | null> => {
    const replicas = baseRouter.getReplicas().filter(r => r.role !== 'primary');

    if (replicas.length === 0) {
      return originalGetNearestReplica(region);
    }

    switch (strategy) {
      case 'round-robin':
        roundRobinIndex = (roundRobinIndex + 1) % replicas.length;
        return replicas[roundRobinIndex].id;

      case 'random':
        const randomIndex = Math.floor(Math.random() * replicas.length);
        return replicas[randomIndex].id;

      case 'least-connections':
        let minConnections = Infinity;
        let minReplica: ReplicaId | null = null;

        for (const replica of replicas) {
          const key = serializeReplicaId(replica.id);
          const connections = connectionCounts.get(key) ?? 0;
          if (connections < minConnections) {
            minConnections = connections;
            minReplica = replica.id;
          }
        }
        return minReplica;

      case 'latency-weighted':
      default:
        return originalGetNearestReplica(region);
    }
  };

  return baseRouter;
}

// =============================================================================
// SESSION MANAGER
// =============================================================================

/**
 * Session manager for read-your-writes consistency
 */
export class SessionManager {
  private sessions = new Map<string, SessionState>();
  private readonly maxSessions: number;
  private readonly sessionTtlMs: number;

  constructor(maxSessions: number = 10000, sessionTtlMs: number = 3600000) {
    this.maxSessions = maxSessions;
    this.sessionTtlMs = sessionTtlMs;
  }

  /**
   * Create a new session
   */
  createSession(preferredRegion?: string): SessionState {
    const sessionId = `sess_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;

    const session: SessionState = {
      sessionId,
      lastWriteLSN: 0n,
      startedAt: Date.now(),
      preferredRegion,
    };

    this.sessions.set(sessionId, session);
    this.cleanup();

    return session;
  }

  /**
   * Get an existing session
   */
  getSession(sessionId: string): SessionState | null {
    const session = this.sessions.get(sessionId);
    if (!session) return null;

    // Check TTL
    if (Date.now() - session.startedAt > this.sessionTtlMs) {
      this.sessions.delete(sessionId);
      return null;
    }

    return session;
  }

  /**
   * Update session's last write LSN
   */
  updateSession(sessionId: string, lastWriteLSN: bigint): void {
    const session = this.sessions.get(sessionId);
    if (session) {
      session.lastWriteLSN = lastWriteLSN;
    }
  }

  /**
   * Delete a session
   */
  deleteSession(sessionId: string): void {
    this.sessions.delete(sessionId);
  }

  /**
   * Cleanup expired sessions
   */
  private cleanup(): void {
    const now = Date.now();

    // Remove expired sessions
    for (const [id, session] of this.sessions) {
      if (now - session.startedAt > this.sessionTtlMs) {
        this.sessions.delete(id);
      }
    }

    // If still over limit, remove oldest
    if (this.sessions.size > this.maxSessions) {
      const sortedSessions = Array.from(this.sessions.entries())
        .sort((a, b) => a[1].startedAt - b[1].startedAt);

      const toRemove = sortedSessions.slice(0, this.sessions.size - this.maxSessions);
      for (const [id] of toRemove) {
        this.sessions.delete(id);
      }
    }
  }

  /**
   * Get session count
   */
  getSessionCount(): number {
    return this.sessions.size;
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a router with session management
 */
export function createRouterWithSessions(
  primaryId: ReplicaId,
  config?: Partial<ReplicationConfig>
): { router: ExtendedReplicationRouter; sessionManager: SessionManager } {
  return {
    router: createExtendedRouter(primaryId, config),
    sessionManager: new SessionManager(),
  };
}
