/**
 * Warm Tier Cache Invalidation
 *
 * R2 cache invalidation strategy for the DoLake warm tier.
 * Handles invalidation on INSERT/UPDATE/DELETE operations,
 * supports partial invalidation by partition, batches multiple
 * invalidations efficiently, and propagates across replicas.
 */

import { type CDCEvent, type CDCOperation } from './types.js';

// =============================================================================
// Types and Interfaces
// =============================================================================

/**
 * Cache invalidation configuration
 */
export interface CacheInvalidationConfig {
  /** Whether cache invalidation is enabled */
  enabled: boolean;
  /** Maximum batch size before flush */
  batchSize: number;
  /** Maximum delay before batch flush (ms) */
  batchDelayMs: number;
  /** Default TTL for cache entries (ms) */
  ttlMs: number;
  /** Whether to propagate invalidation to replicas */
  propagateToReplicas: boolean;
  /** Prevent stale reads during invalidation */
  staleReadPrevention: boolean;
  /** Consistency mode for invalidation */
  consistencyMode: 'strict' | 'eventual';
  /** Enable batch invalidation */
  batchInvalidation: boolean;
  /** Enable deduplication of invalidations */
  deduplicateInvalidations: boolean;
  /** Enable read-your-writes consistency */
  readYourWritesConsistency: boolean;
  /** Enable sliding expiration for TTL */
  slidingExpiration: boolean;
  /** Maximum propagation delay for eventual consistency (ms) */
  maxPropagationDelayMs: number;
}

/**
 * Default cache invalidation configuration
 */
export const DEFAULT_CACHE_INVALIDATION_CONFIG: CacheInvalidationConfig = {
  enabled: true,
  batchSize: 100,
  batchDelayMs: 50,
  ttlMs: 300_000, // 5 minutes
  propagateToReplicas: true,
  staleReadPrevention: true,
  consistencyMode: 'strict',
  batchInvalidation: true,
  deduplicateInvalidations: true,
  readYourWritesConsistency: true,
  slidingExpiration: false,
  maxPropagationDelayMs: 5000,
};

/**
 * Result of a cache invalidation operation
 */
export interface CacheInvalidationResult {
  success: boolean;
  keysInvalidated: string[];
  replicasNotified: number;
  durationMs: number;
}

/**
 * Cache entry status
 */
export interface CacheEntryStatus {
  cached: boolean;
  expiresAt: number;
  tableName: string;
  partition?: string | undefined;
  lastAccessed: number;
  hitCount: number;
}

/**
 * Cache metrics for monitoring
 */
export interface CacheMetrics {
  hitRate: number;
  missRate: number;
  staleReads: number;
  invalidationsPending: number;
  lastInvalidationTime: number;
  bulkInvalidationCount: number;
  individualInvalidationCount: number;
  invalidationRequestsReceived: number;
  invalidationRequestsDeduplicated: number;
  actualInvalidationsPerformed: number;
  batchInvalidationOps: number;
  individualInvalidationOps: number;
  keysInvalidated: number;
  staleReadsBlocked: number;
  staleReadsServed: number;
  staleReadAttempts: number;
}

/**
 * Partition cache status
 */
export interface PartitionCacheStatus {
  partition: string;
  cached: boolean;
  invalidated: boolean;
  lastUpdated: number;
  expiresAt: number;
}

/**
 * Replica status for invalidation propagation
 */
export interface ReplicaStatus {
  name: string;
  notified: boolean;
  confirmed: boolean;
  error?: string | undefined;
  retryScheduled: boolean;
}

/**
 * Replica configuration for invalidation propagation
 */
export interface ReplicaConfig {
  enabled: boolean;
  replicas: string[];
  invalidationPropagation: boolean;
  consistencyMode: 'strict' | 'eventual';
  maxPropagationDelayMs: number;
}

/**
 * Per-table TTL configuration
 */
export interface TableTTLConfig {
  ttlMs: number;
}

/**
 * Invalidation request for batching
 */
interface InvalidationRequest {
  table: string;
  partition?: string | undefined;
  key: string;
  timestamp: number;
  operation: CDCOperation;
}

/**
 * Session for read-your-writes consistency
 */
interface SessionState {
  token: string;
  createdAt: number;
  lastWriteSequence: number;
  tablesModified: Set<string>;
}

// =============================================================================
// Cache Entry
// =============================================================================

/**
 * Individual cache entry
 */
class CacheEntry {
  readonly tableName: string;
  readonly partition: string | undefined;
  readonly key: string;
  private _expiresAt: number;
  private _lastAccessed: number;
  private _hitCount: number = 0;
  private _invalidated: boolean = false;
  private readonly slidingExpiration: boolean;
  private readonly ttlMs: number;

  constructor(
    tableName: string,
    partition: string | undefined,
    key: string,
    ttlMs: number,
    slidingExpiration: boolean
  ) {
    this.tableName = tableName;
    this.partition = partition;
    this.key = key;
    this.ttlMs = ttlMs;
    this.slidingExpiration = slidingExpiration;
    const now = Date.now();
    this._expiresAt = now + ttlMs;
    this._lastAccessed = now;
  }

  get expiresAt(): number {
    return this._expiresAt;
  }

  get lastAccessed(): number {
    return this._lastAccessed;
  }

  get hitCount(): number {
    return this._hitCount;
  }

  get invalidated(): boolean {
    return this._invalidated;
  }

  get isExpired(): boolean {
    return Date.now() >= this._expiresAt;
  }

  get isCached(): boolean {
    return !this._invalidated && !this.isExpired;
  }

  access(): void {
    this._hitCount++;
    this._lastAccessed = Date.now();
    if (this.slidingExpiration) {
      this._expiresAt = this._lastAccessed + this.ttlMs;
    }
  }

  invalidate(): void {
    this._invalidated = true;
  }

  getStatus(): CacheEntryStatus {
    return {
      cached: this.isCached,
      expiresAt: this._expiresAt,
      tableName: this.tableName,
      partition: this.partition,
      lastAccessed: this._lastAccessed,
      hitCount: this._hitCount,
    };
  }
}

// =============================================================================
// Cache Invalidator
// =============================================================================

/**
 * Cache Invalidator for R2 warm tier
 *
 * Handles cache invalidation on CDC events (INSERT/UPDATE/DELETE),
 * supports partial invalidation by partition, batches multiple
 * invalidations, and propagates across replicas.
 */
export class CacheInvalidator {
  private readonly config: CacheInvalidationConfig;
  private readonly entries: Map<string, CacheEntry> = new Map();
  private readonly tableEntries: Map<string, Set<string>> = new Map();
  private readonly partitionEntries: Map<string, Set<string>> = new Map();
  private readonly pendingInvalidations: InvalidationRequest[] = [];
  private readonly pendingInvalidationKeys: Set<string> = new Set();
  private readonly replicaStatuses: Map<string, ReplicaStatus> = new Map();
  private readonly sessions: Map<string, SessionState> = new Map();
  private readonly tableTTLs: Map<string, number> = new Map();

  private replicaConfig: ReplicaConfig = {
    enabled: false,
    replicas: [],
    invalidationPropagation: false,
    consistencyMode: 'strict',
    maxPropagationDelayMs: 5000,
  };

  private metrics: CacheMetrics = {
    hitRate: 0,
    missRate: 0,
    staleReads: 0,
    invalidationsPending: 0,
    lastInvalidationTime: 0,
    bulkInvalidationCount: 0,
    individualInvalidationCount: 0,
    invalidationRequestsReceived: 0,
    invalidationRequestsDeduplicated: 0,
    actualInvalidationsPerformed: 0,
    batchInvalidationOps: 0,
    individualInvalidationOps: 0,
    keysInvalidated: 0,
    staleReadsBlocked: 0,
    staleReadsServed: 0,
    staleReadAttempts: 0,
  };

  private batchFlushTimer: ReturnType<typeof setTimeout> | null = null;
  private totalHits: number = 0;
  private totalMisses: number = 0;

  constructor(config: Partial<CacheInvalidationConfig> = {}) {
    this.config = { ...DEFAULT_CACHE_INVALIDATION_CONFIG, ...config };
  }

  // ===========================================================================
  // Configuration
  // ===========================================================================

  /**
   * Update cache configuration
   */
  updateConfig(config: Partial<CacheInvalidationConfig>): void {
    Object.assign(this.config, config);
  }

  /**
   * Get current configuration
   */
  getConfig(): CacheInvalidationConfig {
    return { ...this.config };
  }

  /**
   * Configure replication settings
   */
  configureReplication(config: Partial<ReplicaConfig>): void {
    Object.assign(this.replicaConfig, config);

    // Initialize replica statuses
    if (this.replicaConfig.replicas) {
      for (const replica of this.replicaConfig.replicas) {
        if (!this.replicaStatuses.has(replica)) {
          this.replicaStatuses.set(replica, {
            name: replica,
            notified: false,
            confirmed: false,
            retryScheduled: false,
          });
        }
      }
    }
  }

  /**
   * Get replication configuration
   */
  getReplicationConfig(): ReplicaConfig {
    return { ...this.replicaConfig };
  }

  /**
   * Configure per-table TTLs
   */
  configureTableTTLs(configs: Record<string, TableTTLConfig>): void {
    for (const [table, config] of Object.entries(configs)) {
      this.tableTTLs.set(table, config.ttlMs);
    }
  }

  /**
   * Get per-table TTL configurations
   */
  getTableTTLs(): Record<string, TableTTLConfig> {
    const result: Record<string, TableTTLConfig> = {};
    for (const [table, ttlMs] of this.tableTTLs) {
      result[table] = { ttlMs };
    }
    return result;
  }

  // ===========================================================================
  // Cache Entry Management
  // ===========================================================================

  /**
   * Register a cache entry for a query result
   */
  registerCacheEntry(tableName: string, partition?: string): string {
    const key = this.buildCacheKey(tableName, partition);
    const ttlMs = this.tableTTLs.get(tableName) ?? this.config.ttlMs;

    const entry = new CacheEntry(
      tableName,
      partition,
      key,
      ttlMs,
      this.config.slidingExpiration
    );

    this.entries.set(key, entry);

    // Track by table
    if (!this.tableEntries.has(tableName)) {
      this.tableEntries.set(tableName, new Set());
    }
    this.tableEntries.get(tableName)!.add(key);

    // Track by partition
    if (partition) {
      const partitionKey = `${tableName}/${partition}`;
      if (!this.partitionEntries.has(partitionKey)) {
        this.partitionEntries.set(partitionKey, new Set());
      }
      this.partitionEntries.get(partitionKey)!.add(key);
    }

    return key;
  }

  /**
   * Access a cache entry (updates hit stats and sliding expiration)
   */
  accessCacheEntry(key: string): CacheEntryStatus | null {
    const entry = this.entries.get(key);
    if (!entry) {
      this.totalMisses++;
      this.updateHitMissRates();
      return null;
    }

    if (entry.isExpired) {
      this.removeCacheEntry(key);
      this.totalMisses++;
      this.updateHitMissRates();
      return null;
    }

    if (entry.invalidated) {
      // Check for stale read prevention
      if (this.config.staleReadPrevention && this.config.consistencyMode === 'strict') {
        this.metrics.staleReadAttempts++;
        this.metrics.staleReadsBlocked++;
        return null;
      }
      this.metrics.staleReads++;
      this.metrics.staleReadsServed++;
    }

    entry.access();
    this.totalHits++;
    this.updateHitMissRates();
    return entry.getStatus();
  }

  /**
   * Get cache entry status without updating access stats
   */
  getCacheEntryStatus(tableName: string): CacheEntryStatus | null {
    const key = this.buildCacheKey(tableName);
    const entry = this.entries.get(key);
    if (!entry) {
      return null;
    }
    return entry.getStatus();
  }

  /**
   * Remove a cache entry
   */
  private removeCacheEntry(key: string): void {
    const entry = this.entries.get(key);
    if (!entry) return;

    this.entries.delete(key);

    // Remove from table tracking
    const tableSet = this.tableEntries.get(entry.tableName);
    if (tableSet) {
      tableSet.delete(key);
      if (tableSet.size === 0) {
        this.tableEntries.delete(entry.tableName);
      }
    }

    // Remove from partition tracking
    if (entry.partition) {
      const partitionKey = `${entry.tableName}/${entry.partition}`;
      const partitionSet = this.partitionEntries.get(partitionKey);
      if (partitionSet) {
        partitionSet.delete(key);
        if (partitionSet.size === 0) {
          this.partitionEntries.delete(partitionKey);
        }
      }
    }
  }

  /**
   * Build a cache key from table and partition
   */
  private buildCacheKey(tableName: string, partition?: string): string {
    return partition ? `${tableName}/${partition}` : tableName;
  }

  /**
   * Update hit/miss rates
   */
  private updateHitMissRates(): void {
    const total = this.totalHits + this.totalMisses;
    if (total > 0) {
      this.metrics.hitRate = this.totalHits / total;
      this.metrics.missRate = this.totalMisses / total;
    }
  }

  // ===========================================================================
  // CDC Integration
  // ===========================================================================

  /**
   * Process CDC events to trigger cache invalidation
   */
  async processCDCEvents(events: CDCEvent[]): Promise<void> {
    if (!this.config.enabled) return;

    for (const event of events) {
      await this.processCDCEvent(event);
    }

    // Trigger batch flush if needed
    if (this.config.batchInvalidation) {
      this.scheduleBatchFlush();
    } else {
      await this.flushPendingInvalidations();
    }
  }

  /**
   * Process a single CDC event
   */
  private async processCDCEvent(event: CDCEvent): Promise<void> {
    const { operation, table, after, before } = event;

    // Skip non-data-modifying operations
    if (operation !== 'INSERT' && operation !== 'UPDATE' && operation !== 'DELETE') {
      return;
    }

    // Determine affected partition from event data
    const partition = this.extractPartitionFromEvent(event);

    // Queue invalidation request
    this.queueInvalidation(table, partition, operation);
  }

  /**
   * Extract partition key from CDC event data
   */
  private extractPartitionFromEvent(event: CDCEvent): string | undefined {
    const data = event.after ?? event.before;
    if (!data || typeof data !== 'object' || data === null) return undefined;

    const record = data as Record<string, unknown>;

    // Look for common partition keys
    // Date-based partitions
    if ('day' in record && typeof record.day === 'string') {
      return `day=${record.day}`;
    }
    if ('date' in record && typeof record.date === 'string') {
      return `date=${record.date}`;
    }

    // Extract date from timestamp if available
    if ('timestamp' in record) {
      const ts = record.timestamp;
      if (typeof ts === 'number' || typeof ts === 'string') {
        const date = new Date(ts);
        return `day=${date.toISOString().split('T')[0]}`;
      }
    }

    return undefined;
  }

  /**
   * Queue an invalidation request for batching
   */
  private queueInvalidation(
    table: string,
    partition: string | undefined,
    operation: CDCOperation
  ): void {
    this.metrics.invalidationRequestsReceived++;

    const key = this.buildCacheKey(table, partition);

    // Deduplicate if enabled
    if (this.config.deduplicateInvalidations) {
      if (this.pendingInvalidationKeys.has(key)) {
        this.metrics.invalidationRequestsDeduplicated++;
        return;
      }
    }

    this.pendingInvalidations.push({
      table,
      partition,
      key,
      timestamp: Date.now(),
      operation,
    });
    this.pendingInvalidationKeys.add(key);
    this.metrics.invalidationsPending = this.pendingInvalidations.length;
  }

  /**
   * Schedule batch flush with debounce
   */
  private scheduleBatchFlush(): void {
    // Clear existing timer
    if (this.batchFlushTimer) {
      clearTimeout(this.batchFlushTimer);
    }

    // Flush immediately if batch size reached
    if (this.pendingInvalidations.length >= this.config.batchSize) {
      void this.flushPendingInvalidations();
      return;
    }

    // Schedule delayed flush
    this.batchFlushTimer = setTimeout(() => {
      void this.flushPendingInvalidations();
    }, this.config.batchDelayMs);
  }

  /**
   * Flush all pending invalidations
   */
  async flushPendingInvalidations(): Promise<CacheInvalidationResult> {
    if (this.batchFlushTimer) {
      clearTimeout(this.batchFlushTimer);
      this.batchFlushTimer = null;
    }

    if (this.pendingInvalidations.length === 0) {
      return {
        success: true,
        keysInvalidated: [],
        replicasNotified: 0,
        durationMs: 0,
      };
    }

    const startTime = Date.now();
    const keysInvalidated: string[] = [];
    const requests = [...this.pendingInvalidations];

    // Clear pending
    this.pendingInvalidations.length = 0;
    this.pendingInvalidationKeys.clear();
    this.metrics.invalidationsPending = 0;

    // Process invalidations
    for (const request of requests) {
      const invalidatedKeys = this.invalidateByKey(request.key, request.table, request.partition);
      keysInvalidated.push(...invalidatedKeys);
    }

    // Update metrics
    this.metrics.actualInvalidationsPerformed += keysInvalidated.length;
    this.metrics.keysInvalidated += keysInvalidated.length;
    this.metrics.lastInvalidationTime = Date.now();

    if (requests.length > 1) {
      this.metrics.batchInvalidationOps++;
      this.metrics.bulkInvalidationCount++;
    } else {
      this.metrics.individualInvalidationOps++;
      this.metrics.individualInvalidationCount++;
    }

    // Propagate to replicas
    let replicasNotified = 0;
    if (this.replicaConfig.invalidationPropagation && this.replicaConfig.replicas.length > 0) {
      replicasNotified = await this.propagateToReplicas(keysInvalidated);
    }

    const durationMs = Date.now() - startTime;

    return {
      success: true,
      keysInvalidated,
      replicasNotified,
      durationMs,
    };
  }

  /**
   * Invalidate cache entries by key
   */
  private invalidateByKey(key: string, table: string, partition?: string): string[] {
    const keysInvalidated: string[] = [];

    // Direct key invalidation
    const entry = this.entries.get(key);
    if (entry) {
      entry.invalidate();
      keysInvalidated.push(key);
    }

    // Invalidate related table entries if partition is specified
    if (partition) {
      const partitionKey = `${table}/${partition}`;
      const partitionSet = this.partitionEntries.get(partitionKey);
      if (partitionSet) {
        for (const entryKey of partitionSet) {
          const e = this.entries.get(entryKey);
          if (e && !e.invalidated) {
            e.invalidate();
            keysInvalidated.push(entryKey);
          }
        }
      }
    }

    // Also invalidate general table entry
    const tableSet = this.tableEntries.get(table);
    if (tableSet) {
      for (const entryKey of tableSet) {
        const e = this.entries.get(entryKey);
        if (e && !e.invalidated) {
          e.invalidate();
          keysInvalidated.push(entryKey);
        }
      }
    }

    return [...new Set(keysInvalidated)];
  }

  // ===========================================================================
  // Manual Invalidation
  // ===========================================================================

  /**
   * Manually invalidate cache for specific table and partitions
   */
  async invalidate(
    table: string,
    partitions?: string[],
    partitionPattern?: string
  ): Promise<CacheInvalidationResult> {
    const startTime = Date.now();
    const keysInvalidated: string[] = [];

    if (partitionPattern) {
      // Wildcard pattern invalidation
      const matchedKeys = this.matchPartitionPattern(table, partitionPattern);
      for (const key of matchedKeys) {
        const entry = this.entries.get(key);
        if (entry) {
          entry.invalidate();
          keysInvalidated.push(key);
        }
      }
    } else if (partitions && partitions.length > 0) {
      // Specific partitions
      for (const partition of partitions) {
        const key = this.buildCacheKey(table, partition);
        const entry = this.entries.get(key);
        if (entry) {
          entry.invalidate();
          keysInvalidated.push(key);
        }
      }
    } else {
      // Invalidate all entries for the table
      const tableSet = this.tableEntries.get(table);
      if (tableSet) {
        for (const key of tableSet) {
          const entry = this.entries.get(key);
          if (entry) {
            entry.invalidate();
            keysInvalidated.push(key);
          }
        }
      }
    }

    // Update metrics
    this.metrics.actualInvalidationsPerformed += keysInvalidated.length;
    this.metrics.keysInvalidated += keysInvalidated.length;
    this.metrics.lastInvalidationTime = Date.now();

    // Propagate to replicas
    let replicasNotified = 0;
    if (this.replicaConfig.invalidationPropagation && this.replicaConfig.replicas.length > 0) {
      replicasNotified = await this.propagateToReplicas(keysInvalidated);
    }

    return {
      success: true,
      keysInvalidated,
      replicasNotified,
      durationMs: Date.now() - startTime,
    };
  }

  /**
   * Match partitions against a wildcard pattern
   */
  private matchPartitionPattern(table: string, pattern: string): string[] {
    const matchedKeys: string[] = [];
    const regexPattern = pattern.replace(/\*/g, '.*');
    const regex = new RegExp(`^${table}/${regexPattern}$`);

    for (const key of this.entries.keys()) {
      if (regex.test(key)) {
        matchedKeys.push(key);
      }
    }

    // For date patterns like "day=2024-01-*", generate all days
    if (pattern.includes('day=') && pattern.endsWith('*')) {
      const prefix = pattern.replace('*', '');
      // For "day=2024-01-*", we need to match day=2024-01-01 through day=2024-01-31
      const dateMatch = prefix.match(/day=(\d{4})-(\d{2})-/);
      if (dateMatch) {
        const yearStr = dateMatch[1];
        const monthStr = dateMatch[2];
        if (yearStr && monthStr) {
          const year = parseInt(yearStr, 10);
          const month = parseInt(monthStr, 10);
          const daysInMonth = new Date(year, month, 0).getDate();
          for (let day = 1; day <= daysInMonth; day++) {
            const dayStr = day.toString().padStart(2, '0');
            const partitionKey = `day=${year}-${monthStr}-${dayStr}`;
            matchedKeys.push(`${table}/${partitionKey}`);
          }
        }
      }
    }

    return matchedKeys;
  }

  // ===========================================================================
  // Replica Propagation
  // ===========================================================================

  /**
   * Propagate invalidation to replicas
   */
  private async propagateToReplicas(keys: string[]): Promise<number> {
    if (!this.replicaConfig.enabled || this.replicaConfig.replicas.length === 0) {
      return 0;
    }

    let notified = 0;
    let confirmed = 0;

    for (const replica of this.replicaConfig.replicas) {
      const status = this.replicaStatuses.get(replica) ?? {
        name: replica,
        notified: false,
        confirmed: false,
        retryScheduled: false,
      };

      try {
        // In a real implementation, this would make an RPC call to the replica
        // For now, we simulate the notification
        await this.notifyReplica(replica, keys);
        status.notified = true;
        status.confirmed = true;
        status.error = undefined;
        status.retryScheduled = false;
        notified++;
        confirmed++;
      } catch (error) {
        status.notified = true;
        status.confirmed = false;
        status.error = error instanceof Error ? error.message : String(error);
        status.retryScheduled = true;
        notified++;
        // Schedule retry for failed replicas
        void this.scheduleReplicaRetry(replica, keys);
      }

      this.replicaStatuses.set(replica, status);
    }

    return notified;
  }

  /**
   * Notify a single replica of invalidation
   */
  private async notifyReplica(replica: string, keys: string[]): Promise<void> {
    // Simulate replica notification
    // In production, this would use WebSocket or RPC to notify replicas
    const status = this.replicaStatuses.get(replica);

    // Check for injected failures (for testing)
    if (status?.error === 'network_timeout') {
      throw new Error('Network timeout');
    }

    // Simulate async notification
    await new Promise(resolve => setTimeout(resolve, 1));
  }

  /**
   * Schedule retry for failed replica notification
   */
  private async scheduleReplicaRetry(replica: string, keys: string[]): Promise<void> {
    // Exponential backoff retry
    const retryDelays = [100, 500, 1000, 2000, 5000];
    let attempt = 0;

    const retry = async (): Promise<void> => {
      if (attempt >= retryDelays.length) {
        return;
      }

      await new Promise(resolve => setTimeout(resolve, retryDelays[attempt]));
      attempt++;

      const status = this.replicaStatuses.get(replica);
      if (!status || status.confirmed) {
        return;
      }

      try {
        await this.notifyReplica(replica, keys);
        if (status) {
          status.confirmed = true;
          status.error = undefined;
          status.retryScheduled = false;
        }
      } catch {
        void retry();
      }
    };

    void retry();
  }

  /**
   * Inject failure for testing
   */
  injectFailure(replica: string, failureType: string): void {
    const status = this.replicaStatuses.get(replica) ?? {
      name: replica,
      notified: false,
      confirmed: false,
      retryScheduled: false,
    };
    status.error = failureType;
    this.replicaStatuses.set(replica, status);
  }

  /**
   * Get invalidation propagation status
   */
  getInvalidationStatus(): {
    replicasNotified: number;
    replicasConfirmed: number;
    pendingReplicas: string[];
    failedReplicas: Array<{ name: string; error: string; retryScheduled: boolean }>;
  } {
    let notified = 0;
    let confirmed = 0;
    const pending: string[] = [];
    const failed: Array<{ name: string; error: string; retryScheduled: boolean }> = [];

    for (const [name, status] of this.replicaStatuses) {
      if (status.notified) notified++;
      if (status.confirmed) confirmed++;
      if (status.notified && !status.confirmed) {
        pending.push(name);
        if (status.error) {
          failed.push({
            name,
            error: status.error,
            retryScheduled: status.retryScheduled,
          });
        }
      }
    }

    return {
      replicasNotified: notified,
      replicasConfirmed: confirmed,
      pendingReplicas: pending,
      failedReplicas: failed,
    };
  }

  // ===========================================================================
  // Partition Cache Status
  // ===========================================================================

  /**
   * Get partition cache status for a table
   */
  getPartitionCacheStatus(table: string): {
    partitions: PartitionCacheStatus[];
    invalidatedPartitions: string[];
    unchangedPartitions: string[];
  } {
    const partitions: PartitionCacheStatus[] = [];
    const invalidatedPartitions: string[] = [];
    const unchangedPartitions: string[] = [];

    // Get all partition keys for the table
    for (const [key, entry] of this.entries) {
      if (entry.tableName === table && entry.partition) {
        const status: PartitionCacheStatus = {
          partition: entry.partition,
          cached: entry.isCached,
          invalidated: entry.invalidated,
          lastUpdated: entry.lastAccessed,
          expiresAt: entry.expiresAt,
        };
        partitions.push(status);

        if (entry.invalidated) {
          invalidatedPartitions.push(entry.partition);
        } else {
          unchangedPartitions.push(entry.partition);
        }
      }
    }

    return { partitions, invalidatedPartitions, unchangedPartitions };
  }

  /**
   * Get query cache status for a table
   */
  getQueryCacheStatus(table: string): {
    invalidatedQueries: string[];
    cachedQueries: string[];
  } {
    const invalidatedQueries: string[] = [];
    const cachedQueries: string[] = [];

    const tableSet = this.tableEntries.get(table);
    if (tableSet) {
      for (const key of tableSet) {
        const entry = this.entries.get(key);
        if (entry) {
          if (entry.invalidated || entry.isExpired) {
            invalidatedQueries.push(key);
          } else {
            cachedQueries.push(key);
          }
        }
      }
    }

    return { invalidatedQueries, cachedQueries };
  }

  // ===========================================================================
  // Session Management (Read-Your-Writes)
  // ===========================================================================

  /**
   * Create a new session for read-your-writes consistency
   */
  createSession(): { token: string } {
    const token = crypto.randomUUID();
    this.sessions.set(token, {
      token,
      createdAt: Date.now(),
      lastWriteSequence: 0,
      tablesModified: new Set(),
    });
    return { token };
  }

  /**
   * Record a write in a session
   */
  recordSessionWrite(token: string, table: string, sequence: number): void {
    const session = this.sessions.get(token);
    if (session) {
      session.lastWriteSequence = Math.max(session.lastWriteSequence, sequence);
      session.tablesModified.add(table);
    }
  }

  /**
   * Check if a read can see a write in the same session
   */
  canReadOwnWrite(token: string, table: string): boolean {
    if (!this.config.readYourWritesConsistency) {
      return true;
    }

    const session = this.sessions.get(token);
    if (!session) {
      return true;
    }

    // If the table was modified in this session, ensure we don't serve stale cache
    if (session.tablesModified.has(table)) {
      // The cache for this table should be invalidated
      const tableSet = this.tableEntries.get(table);
      if (tableSet) {
        for (const key of tableSet) {
          const entry = this.entries.get(key);
          if (entry && !entry.invalidated) {
            return false; // Must bypass cache
          }
        }
      }
    }

    return true;
  }

  // ===========================================================================
  // Metrics
  // ===========================================================================

  /**
   * Get cache metrics
   */
  getMetrics(): CacheMetrics {
    return { ...this.metrics };
  }

  /**
   * Get pending invalidation count
   */
  getPendingCount(): number {
    return this.pendingInvalidations.length;
  }

  /**
   * Reset metrics
   */
  resetMetrics(): void {
    this.metrics = {
      hitRate: 0,
      missRate: 0,
      staleReads: 0,
      invalidationsPending: 0,
      lastInvalidationTime: 0,
      bulkInvalidationCount: 0,
      individualInvalidationCount: 0,
      invalidationRequestsReceived: 0,
      invalidationRequestsDeduplicated: 0,
      actualInvalidationsPerformed: 0,
      batchInvalidationOps: 0,
      individualInvalidationOps: 0,
      keysInvalidated: 0,
      staleReadsBlocked: 0,
      staleReadsServed: 0,
      staleReadAttempts: 0,
    };
    this.totalHits = 0;
    this.totalMisses = 0;
  }

  // ===========================================================================
  // Cleanup
  // ===========================================================================

  /**
   * Clean up expired cache entries
   */
  cleanupExpired(): number {
    let count = 0;
    const now = Date.now();

    for (const [key, entry] of this.entries) {
      if (now >= entry.expiresAt) {
        this.removeCacheEntry(key);
        count++;
      }
    }

    return count;
  }

  /**
   * Clear all cache entries
   */
  clearAll(): void {
    this.entries.clear();
    this.tableEntries.clear();
    this.partitionEntries.clear();
    this.pendingInvalidations.length = 0;
    this.pendingInvalidationKeys.clear();
    this.resetMetrics();
  }

  /**
   * Destroy the cache invalidator (cleanup timers)
   */
  destroy(): void {
    if (this.batchFlushTimer) {
      clearTimeout(this.batchFlushTimer);
      this.batchFlushTimer = null;
    }
  }
}
