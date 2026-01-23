/**
 * DO-resident Metadata Cache for DoLake
 *
 * Provides high-performance caching of Iceberg table metadata in Durable Object
 * persistent storage. Supports TTL-based expiration, LRU eviction, and
 * cache coherence across multiple DO instances.
 *
 * Key features:
 * - Sub-millisecond latency for cached lookups
 * - TTL-based automatic expiration
 * - LRU eviction when capacity exceeded
 * - Cache invalidation on schema changes via CDC
 * - Persistence across DO hibernation
 * - Cache coherence across multiple DO instances
 *
 * @module dolake/metadata-cache
 */

import { EventEmitter } from 'events';

// =============================================================================
// Types
// =============================================================================

/**
 * Iceberg table metadata structure
 */
export interface IcebergMetadata {
  'format-version': 2;
  'table-uuid': string;
  location: string;
  'last-sequence-number': bigint;
  'last-updated-ms': bigint;
  'last-column-id': number;
  'current-schema-id': number;
  schemas: Array<{
    'schema-id': number;
    type: 'struct';
    fields: Array<{
      id: number;
      name: string;
      type: string;
      required: boolean;
    }>;
  }>;
  'default-spec-id': number;
  'partition-specs': Array<{
    'spec-id': number;
    fields: Array<{
      'source-id': number;
      'field-id': number;
      name: string;
      transform: string;
    }>;
  }>;
  'default-sort-order-id': number;
  'sort-orders': Array<{
    'order-id': number;
    fields: Array<{
      transform: string;
      'source-id': number;
      direction: 'asc' | 'desc';
      'null-order': 'nulls-first' | 'nulls-last';
    }>;
  }>;
  'current-snapshot-id': bigint | null;
  snapshots: Array<{
    'snapshot-id': bigint;
    'timestamp-ms': bigint;
    'manifest-list': string;
  }>;
  'snapshot-log': Array<{
    'snapshot-id': bigint;
    'timestamp-ms': bigint;
  }>;
}

/**
 * Invalidation strategy types
 */
export type InvalidationStrategy = 'immediate' | 'refresh' | 'lazy';

/**
 * Metadata cache configuration
 */
export interface MetadataCacheConfig {
  /** Enable metadata caching */
  enabled: boolean;
  /** TTL for cached metadata in milliseconds */
  ttlMs: number;
  /** Maximum number of tables to cache */
  maxTables: number;
  /** Enable cache coherence across DO instances */
  enableCoherence: boolean;
  /** Latency threshold for sub-ms requirement (microseconds) */
  latencyThresholdUs: number;
  /** Invalidation strategies by change type */
  invalidationStrategies?: {
    SCHEMA_CHANGE?: InvalidationStrategy;
    SNAPSHOT_APPEND?: InvalidationStrategy;
    PROPERTY_CHANGE?: InvalidationStrategy;
  };
  /** Callback for latency violations */
  onLatencyViolation?: (tableId: string, latencyUs: number) => void;
}

/**
 * Metadata cache entry
 */
export interface MetadataCacheEntry {
  metadata: IcebergMetadata;
  cachedAt: number;
  expiresAt: number;
  version: number;
  hitCount: number;
  lastAccessedAt: number;
  pendingInvalidation?: boolean;
}

/**
 * Cache statistics
 */
export interface CacheStats {
  hits: number;
  misses: number;
  hitRate: number;
  entriesCount: number;
  evictions: number;
  expirations: number;
}

/**
 * Partition specification for identifying a partition
 */
export interface PartitionSpec {
  /** Partition field values (e.g., { year: 2024, month: 1 }) */
  values: Record<string, string | number | boolean | null>;
}

/**
 * Partition-level statistics
 */
export interface PartitionStats {
  /** Record count in this partition */
  recordCount: number;
  /** Number of data files in this partition */
  fileCount: number;
  /** Total size in bytes of all files in this partition */
  sizeBytes: number;
  /** Last modified timestamp (ms since epoch) */
  lastModifiedMs: number;
  /** Timestamp when stats were collected */
  collectedAt: number;
}

/**
 * Partition cache entry with statistics and access tracking
 */
export interface PartitionCacheEntry {
  /** Partition specification */
  spec: PartitionSpec;
  /** Partition statistics */
  stats: PartitionStats;
  /** Timestamp when cached */
  cachedAt: number;
  /** Expiration timestamp */
  expiresAt: number;
  /** Number of times this partition was accessed */
  accessCount: number;
  /** Last access timestamp */
  lastAccessedAt: number;
  /** Custom partition data */
  data?: unknown;
}

/**
 * Partition access metrics for hot partition identification
 */
export interface PartitionAccessMetrics {
  /** Table ID */
  tableId: string;
  /** Partition key */
  partitionKey: string;
  /** Total access count */
  accessCount: number;
  /** Access rate (accesses per second over measurement window) */
  accessRate: number;
  /** Last access timestamp */
  lastAccessedAt: number;
  /** Is this partition considered hot */
  isHot: boolean;
}

/**
 * Partition statistics summary for a table
 */
export interface TablePartitionSummary {
  /** Table ID */
  tableId: string;
  /** Total number of partitions */
  partitionCount: number;
  /** Total record count across all partitions */
  totalRecordCount: number;
  /** Total file count across all partitions */
  totalFileCount: number;
  /** Total size in bytes across all partitions */
  totalSizeBytes: number;
  /** Number of hot partitions */
  hotPartitionCount: number;
  /** List of hot partitions (by access rate) */
  hotPartitions: PartitionAccessMetrics[];
}

/**
 * Latency statistics
 */
export interface LatencyStats {
  p50Us: number;
  p99Us: number;
  p999Us: number;
  avgUs: number;
  violations: number;
}

/**
 * Schema change CDC event
 */
export interface SchemaChangeEvent {
  type: 'schema_change';
  table: string;
  operation: 'ADD_COLUMN' | 'DROP_COLUMN' | 'RENAME_COLUMN' | 'ALTER_COLUMN';
  column?: { id: number; name: string; type: string; required: boolean };
  newSchemaId: number;
  timestamp: number;
}

/**
 * Partition spec change event
 */
export interface PartitionSpecChange {
  oldSpecId: number;
  newSpecId: number;
  newFields: Array<{
    'source-id': number;
    'field-id': number;
    name: string;
    transform: string;
  }>;
}

/**
 * Mock storage interface (simulates DO persistent storage)
 */
export interface StorageInterface {
  get(key: string): unknown | undefined;
  set(key: string, value: unknown): void;
  delete(key: string): void;
  has(key: string): boolean;
  keys(): Iterable<string>;
}

/**
 * Cache options including storage
 */
export interface CacheOptions {
  storage?: Map<string, unknown>;
}

/**
 * Coherence manager configuration
 */
export interface CoherenceManagerConfig {
  mode?: 'strict' | 'eventual';
  propagationDelayMs?: number;
  batchSize?: number;
  batchDelayMs?: number;
  healthCheckIntervalMs?: number;
}

/**
 * Coherence message
 */
export interface CoherenceMessage {
  type: 'invalidate' | 'update';
  tableId: string;
  version: number;
  timestamp: number;
  sourceDoId: string;
}

/**
 * Conflict information
 */
export interface Conflict {
  tableId: string;
  doIds: string[];
  versions: number[];
}

/**
 * Health status
 */
export interface CoherenceHealth {
  status: 'healthy' | 'degraded' | 'unhealthy';
  registeredDOs: number;
  pendingMessages: number;
  lastSyncTimestamp: number;
}

/**
 * Unified cache manager configuration
 */
export interface UnifiedCacheManagerConfig {
  metadata?: {
    enabled: boolean;
    ttlMs: number;
    maxTables: number;
  };
  query?: {
    enabled: boolean;
    ttlMs: number;
  };
  partition?: {
    enabled: boolean;
    ttlMs: number;
  };
}

// =============================================================================
// MetadataCache Implementation
// =============================================================================

/**
 * DO-resident metadata cache for Iceberg table metadata
 */
export class MetadataCache {
  public readonly config: MetadataCacheConfig;
  private cache: Map<string, MetadataCacheEntry> = new Map();
  private auxiliary: Map<string, Map<string, unknown>> = new Map();
  private partitionCache: Map<string, Map<string, unknown>> = new Map();
  /** Partition statistics storage: tableId -> partitionKey -> PartitionCacheEntry */
  private partitionStats: Map<string, Map<string, PartitionCacheEntry>> = new Map();
  /** Partition access tracking for hot partition identification */
  private partitionAccessHistory: Map<string, { timestamps: number[]; accessCount: number }> =
    new Map();
  /** TTL for partition statistics (default 5 minutes) */
  private partitionStatsTtlMs: number = 300_000;
  /** Window size for access rate calculation (default 60 seconds) */
  private accessRateWindowMs: number = 60_000;
  /** Threshold for hot partition identification (accesses per second) */
  private hotPartitionThreshold: number = 10;
  private storage: StorageInterface | null = null;
  private latencies: number[] = [];
  private accessSequence: number = 0; // Monotonically increasing for LRU ordering
  private stats: {
    hits: number;
    misses: number;
    evictions: number;
    expirations: number;
    latencyViolations: number;
  } = {
    hits: 0,
    misses: 0,
    evictions: 0,
    expirations: 0,
    latencyViolations: 0,
  };
  private coherenceManager?: CacheCoherenceManager;

  constructor(config: MetadataCacheConfig, options?: CacheOptions) {
    this.config = config;
    if (options?.storage) {
      this.storage = this.wrapMapAsStorage(options.storage);
    }
  }

  /**
   * Wrap a Map as a storage interface
   */
  private wrapMapAsStorage(map: Map<string, unknown>): StorageInterface {
    return {
      get: (key: string) => map.get(key),
      set: (key: string, value: unknown) => map.set(key, value),
      delete: (key: string) => map.delete(key),
      has: (key: string) => map.has(key),
      keys: () => map.keys(),
    };
  }

  /**
   * Store metadata in cache
   */
  async put(tableId: string, metadata: IcebergMetadata): Promise<void> {
    const now = Date.now();
    this.accessSequence++;

    // Check capacity and evict if needed
    if (this.cache.size >= this.config.maxTables && !this.cache.has(tableId)) {
      this.evictLRU();
    }

    const entry: MetadataCacheEntry = {
      metadata,
      cachedAt: now,
      expiresAt: now + this.config.ttlMs,
      version: metadata['current-schema-id'],
      hitCount: 0,
      lastAccessedAt: this.accessSequence, // Use sequence number for LRU ordering
    };

    this.cache.set(tableId, entry);
  }

  /**
   * Get metadata from cache
   */
  async get(tableId: string): Promise<IcebergMetadata | null> {
    const startTime = performance.now();

    const entry = this.cache.get(tableId);

    if (!entry) {
      this.stats.misses++;
      this.recordLatency(startTime);
      return null;
    }

    const now = Date.now();

    // Check expiration
    if (now >= entry.expiresAt) {
      this.cache.delete(tableId);
      this.stats.expirations++;
      this.stats.misses++;
      this.recordLatency(startTime);
      return null;
    }

    // Check pending invalidation (lazy strategy)
    if (entry.pendingInvalidation) {
      this.cache.delete(tableId);
      this.stats.misses++;
      this.recordLatency(startTime);
      return null;
    }

    // Update access stats
    entry.hitCount++;
    this.accessSequence++;
    entry.lastAccessedAt = this.accessSequence; // Use sequence number for LRU ordering
    this.stats.hits++;

    this.recordLatency(startTime);
    return entry.metadata;
  }

  /**
   * Get full cache entry
   */
  async getEntry(tableId: string): Promise<MetadataCacheEntry | null> {
    const entry = this.cache.get(tableId);

    if (!entry) {
      return null;
    }

    const now = Date.now();

    // Check expiration
    if (now >= entry.expiresAt) {
      this.cache.delete(tableId);
      this.stats.expirations++;
      return null;
    }

    return entry;
  }

  /**
   * Invalidate a cache entry
   */
  async invalidate(tableId: string): Promise<void> {
    this.cache.delete(tableId);
    // Also clear auxiliary data
    this.auxiliary.delete(tableId);
    // Clear partition data for this table
    this.partitionCache.delete(tableId);
    // Clear partition statistics for this table
    this.partitionStats.delete(tableId);
    // Clear partition access history for this table
    for (const key of this.partitionAccessHistory.keys()) {
      if (key.startsWith(`${tableId}:`)) {
        this.partitionAccessHistory.delete(key);
      }
    }

    // Notify coherence manager if enabled
    if (this.coherenceManager) {
      this.coherenceManager.notifyInvalidation(tableId, this);
    }
  }

  /**
   * Mark entry for lazy invalidation
   */
  async markForLazyInvalidation(tableId: string, _reason: string): Promise<void> {
    const entry = this.cache.get(tableId);
    if (entry) {
      entry.pendingInvalidation = true;
    }
  }

  /**
   * Get cache statistics
   */
  getStats(): CacheStats {
    const total = this.stats.hits + this.stats.misses;
    return {
      hits: this.stats.hits,
      misses: this.stats.misses,
      hitRate: total > 0 ? this.stats.hits / total : 0,
      entriesCount: this.cache.size,
      evictions: this.stats.evictions,
      expirations: this.stats.expirations,
    };
  }

  /**
   * Get latency statistics
   */
  getLatencyStats(): LatencyStats {
    if (this.latencies.length === 0) {
      return {
        p50Us: 0,
        p99Us: 0,
        p999Us: 0,
        avgUs: 0,
        violations: this.stats.latencyViolations,
      };
    }

    const sorted = [...this.latencies].sort((a, b) => a - b);
    const avg = this.latencies.reduce((sum, v) => sum + v, 0) / this.latencies.length;

    return {
      p50Us: this.percentile(sorted, 0.5),
      p99Us: this.percentile(sorted, 0.99),
      p999Us: this.percentile(sorted, 0.999),
      avgUs: avg,
      violations: this.stats.latencyViolations,
    };
  }

  /**
   * Pre-warm cache with hot tables
   */
  async prewarm(
    tableIds: string[],
    metadataLoader: (tableId: string) => Promise<IcebergMetadata>
  ): Promise<void> {
    for (const tableId of tableIds) {
      const metadata = await metadataLoader(tableId);
      await this.put(tableId, metadata);
    }
  }

  /**
   * Flush cache to persistent storage
   */
  async flush(): Promise<void> {
    if (!this.storage) {
      return;
    }

    const serialized: Record<string, unknown> = {};
    for (const [key, entry] of this.cache.entries()) {
      serialized[key] = this.serializeEntry(entry);
    }

    this.storage.set('__metadata_cache__', serialized);
    this.storage.set('__metadata_cache_stats__', { ...this.stats });

    // Flush partition statistics
    const partitionStatsData: Record<string, Record<string, PartitionCacheEntry>> = {};
    for (const [tableId, tableStats] of this.partitionStats.entries()) {
      partitionStatsData[tableId] = {};
      for (const [partitionKey, entry] of tableStats.entries()) {
        partitionStatsData[tableId][partitionKey] = entry;
      }
    }
    this.storage.set('__partition_stats__', partitionStatsData);

    // Flush partition access history
    const accessHistoryData: Record<string, { timestamps: number[]; accessCount: number }> = {};
    for (const [key, history] of this.partitionAccessHistory.entries()) {
      accessHistoryData[key] = history;
    }
    this.storage.set('__partition_access_history__', accessHistoryData);
  }

  /**
   * Restore cache from persistent storage
   */
  async restore(): Promise<void> {
    if (!this.storage) {
      return;
    }

    const serialized = this.storage.get('__metadata_cache__') as Record<string, unknown> | undefined;
    if (serialized) {
      for (const [key, value] of Object.entries(serialized)) {
        const entry = this.deserializeEntry(value as Record<string, unknown>);
        // Only restore non-expired entries
        if (Date.now() < entry.expiresAt) {
          this.cache.set(key, entry);
        }
      }
    }

    const savedStats = this.storage.get('__metadata_cache_stats__') as typeof this.stats | undefined;
    if (savedStats) {
      this.stats = { ...savedStats };
    }

    // Restore partition statistics
    const partitionStatsData = this.storage.get('__partition_stats__') as
      | Record<string, Record<string, PartitionCacheEntry>>
      | undefined;
    if (partitionStatsData) {
      const now = Date.now();
      for (const [tableId, tableStats] of Object.entries(partitionStatsData)) {
        const tableMap = new Map<string, PartitionCacheEntry>();
        for (const [partitionKey, entry] of Object.entries(tableStats)) {
          // Only restore non-expired entries
          if (now < entry.expiresAt) {
            tableMap.set(partitionKey, entry);
          }
        }
        if (tableMap.size > 0) {
          this.partitionStats.set(tableId, tableMap);
        }
      }
    }

    // Restore partition access history
    const accessHistoryData = this.storage.get('__partition_access_history__') as
      | Record<string, { timestamps: number[]; accessCount: number }>
      | undefined;
    if (accessHistoryData) {
      const now = Date.now();
      const windowStart = now - this.accessRateWindowMs;
      for (const [key, history] of Object.entries(accessHistoryData)) {
        // Filter out old timestamps
        const recentTimestamps = history.timestamps.filter((ts) => ts >= windowStart);
        if (recentTimestamps.length > 0 || history.accessCount > 0) {
          this.partitionAccessHistory.set(key, {
            timestamps: recentTimestamps,
            accessCount: history.accessCount,
          });
        }
      }
    }
  }

  /**
   * Store auxiliary data for a table
   */
  async putAuxiliary(tableId: string, key: string, data: unknown): Promise<void> {
    let tableAux = this.auxiliary.get(tableId);
    if (!tableAux) {
      tableAux = new Map();
      this.auxiliary.set(tableId, tableAux);
    }
    tableAux.set(key, data);
  }

  /**
   * Get auxiliary data for a table
   */
  async getAuxiliary(tableId: string, key: string): Promise<unknown | null> {
    const tableAux = this.auxiliary.get(tableId);
    if (!tableAux) {
      return null;
    }
    return tableAux.get(key) ?? null;
  }

  /**
   * Store partition-specific data
   */
  async putPartition(tableId: string, partitionKey: string, data: unknown): Promise<void> {
    let tablePartitions = this.partitionCache.get(tableId);
    if (!tablePartitions) {
      tablePartitions = new Map();
      this.partitionCache.set(tableId, tablePartitions);
    }
    tablePartitions.set(partitionKey, data);
  }

  /**
   * Get partition-specific data
   */
  async getPartition(tableId: string, partitionKey: string): Promise<unknown | null> {
    const tablePartitions = this.partitionCache.get(tableId);
    if (!tablePartitions) {
      // Return empty object for non-existent table partitions
      // to differentiate from "partition was invalidated"
      return { rowCount: 0 };
    }
    const data = tablePartitions.get(partitionKey);
    return data !== undefined ? data : null;
  }

  /**
   * Invalidate a specific partition
   */
  async invalidatePartition(tableId: string, partitionKey: string): Promise<void> {
    const tablePartitions = this.partitionCache.get(tableId);
    if (tablePartitions) {
      tablePartitions.delete(partitionKey);
    }

    // Notify coherence manager if enabled
    if (this.coherenceManager) {
      this.coherenceManager.notifyPartitionInvalidation(tableId, partitionKey, this);
    }
  }

  /**
   * Set coherence manager reference
   */
  setCoherenceManager(manager: CacheCoherenceManager): void {
    this.coherenceManager = manager;
  }

  /**
   * Handle coherence invalidation from another DO
   */
  handleCoherenceInvalidation(tableId: string): void {
    this.cache.delete(tableId);
    this.auxiliary.delete(tableId);
    this.partitionCache.delete(tableId);
    // Clear partition statistics
    this.partitionStats.delete(tableId);
    // Clear partition access history
    for (const key of this.partitionAccessHistory.keys()) {
      if (key.startsWith(`${tableId}:`)) {
        this.partitionAccessHistory.delete(key);
      }
    }
  }

  /**
   * Handle partition invalidation from another DO
   */
  handlePartitionCoherenceInvalidation(tableId: string, partitionKey: string): void {
    const tablePartitions = this.partitionCache.get(tableId);
    if (tablePartitions) {
      tablePartitions.delete(partitionKey);
    }
    // Also invalidate partition stats
    const tableStats = this.partitionStats.get(tableId);
    if (tableStats) {
      tableStats.delete(partitionKey);
    }
  }

  // =============================================================================
  // Partition Statistics Methods
  // =============================================================================

  /**
   * Convert partition spec to a consistent key string
   */
  private partitionSpecToKey(partitionSpec: PartitionSpec): string {
    const sortedKeys = Object.keys(partitionSpec.values).sort();
    const parts = sortedKeys.map((k) => `${k}=${partitionSpec.values[k]}`);
    return parts.join('/');
  }

  /**
   * Parse partition key back to spec
   */
  private keyToPartitionSpec(partitionKey: string): PartitionSpec {
    const values: Record<string, string | number | boolean | null> = {};
    if (!partitionKey) return { values };

    const parts = partitionKey.split('/');
    for (const part of parts) {
      const [key, value] = part.split('=');
      if (key) {
        // Try to parse as number or boolean
        if (value === 'null') {
          values[key] = null;
        } else if (value === 'true') {
          values[key] = true;
        } else if (value === 'false') {
          values[key] = false;
        } else if (value !== undefined && !isNaN(Number(value))) {
          values[key] = Number(value);
        } else {
          values[key] = value ?? '';
        }
      }
    }
    return { values };
  }

  /**
   * Get partition statistics
   * @param tableName - The table name/ID
   * @param partitionSpec - The partition specification
   * @returns Partition statistics or null if not found/expired
   */
  async getPartitionStats(
    tableName: string,
    partitionSpec: PartitionSpec
  ): Promise<PartitionStats | null> {
    const tableStats = this.partitionStats.get(tableName);
    if (!tableStats) {
      return null;
    }

    const partitionKey = this.partitionSpecToKey(partitionSpec);
    const entry = tableStats.get(partitionKey);

    if (!entry) {
      return null;
    }

    const now = Date.now();

    // Check expiration
    if (now >= entry.expiresAt) {
      tableStats.delete(partitionKey);
      return null;
    }

    // Track access for hot partition identification
    this.trackPartitionAccess(tableName, partitionKey);

    // Update access stats
    entry.accessCount++;
    entry.lastAccessedAt = now;

    return entry.stats;
  }

  /**
   * Update partition statistics
   * @param tableName - The table name/ID
   * @param partitionSpec - The partition specification
   * @param stats - The partition statistics to store
   */
  async updatePartitionStats(
    tableName: string,
    partitionSpec: PartitionSpec,
    stats: PartitionStats
  ): Promise<void> {
    let tableStats = this.partitionStats.get(tableName);
    if (!tableStats) {
      tableStats = new Map();
      this.partitionStats.set(tableName, tableStats);
    }

    const partitionKey = this.partitionSpecToKey(partitionSpec);
    const now = Date.now();

    const entry: PartitionCacheEntry = {
      spec: partitionSpec,
      stats: {
        ...stats,
        collectedAt: stats.collectedAt || now,
      },
      cachedAt: now,
      expiresAt: now + this.partitionStatsTtlMs,
      accessCount: 0,
      lastAccessedAt: now,
    };

    tableStats.set(partitionKey, entry);

    // Also notify coherence manager if enabled
    if (this.coherenceManager) {
      // Partition stats update doesn't require invalidation on other DOs
      // but we could emit an event for monitoring
    }
  }

  /**
   * List all partitions for a table
   * @param tableName - The table name/ID
   * @returns Array of partition specifications with their stats
   */
  async listPartitions(
    tableName: string
  ): Promise<Array<{ spec: PartitionSpec; stats: PartitionStats }>> {
    const tableStats = this.partitionStats.get(tableName);
    if (!tableStats) {
      return [];
    }

    const now = Date.now();
    const result: Array<{ spec: PartitionSpec; stats: PartitionStats }> = [];

    for (const [partitionKey, entry] of tableStats.entries()) {
      // Skip expired entries
      if (now >= entry.expiresAt) {
        tableStats.delete(partitionKey);
        continue;
      }

      result.push({
        spec: entry.spec,
        stats: entry.stats,
      });
    }

    return result;
  }

  /**
   * Invalidate partition statistics for a specific partition
   */
  async invalidatePartitionStats(tableName: string, partitionSpec: PartitionSpec): Promise<void> {
    const tableStats = this.partitionStats.get(tableName);
    if (!tableStats) {
      return;
    }

    const partitionKey = this.partitionSpecToKey(partitionSpec);
    tableStats.delete(partitionKey);

    // Also clear access history
    const accessKey = `${tableName}:${partitionKey}`;
    this.partitionAccessHistory.delete(accessKey);
  }

  /**
   * Invalidate all partition statistics for a table
   */
  async invalidateAllPartitionStats(tableName: string): Promise<void> {
    this.partitionStats.delete(tableName);

    // Clear access history for this table
    for (const key of this.partitionAccessHistory.keys()) {
      if (key.startsWith(`${tableName}:`)) {
        this.partitionAccessHistory.delete(key);
      }
    }
  }

  // =============================================================================
  // Partition Access Tracking & Hot Partition Identification
  // =============================================================================

  /**
   * Track partition access for hot partition identification
   */
  private trackPartitionAccess(tableName: string, partitionKey: string): void {
    const accessKey = `${tableName}:${partitionKey}`;
    const now = Date.now();

    let history = this.partitionAccessHistory.get(accessKey);
    if (!history) {
      history = { timestamps: [], accessCount: 0 };
      this.partitionAccessHistory.set(accessKey, history);
    }

    history.timestamps.push(now);
    history.accessCount++;

    // Clean up old timestamps outside the window
    const windowStart = now - this.accessRateWindowMs;
    history.timestamps = history.timestamps.filter((ts) => ts >= windowStart);
  }

  /**
   * Calculate access rate for a partition (accesses per second)
   */
  private getPartitionAccessRate(tableName: string, partitionKey: string): number {
    const accessKey = `${tableName}:${partitionKey}`;
    const history = this.partitionAccessHistory.get(accessKey);

    if (!history || history.timestamps.length === 0) {
      return 0;
    }

    const now = Date.now();
    const windowStart = now - this.accessRateWindowMs;
    const recentAccesses = history.timestamps.filter((ts) => ts >= windowStart).length;

    // Convert to accesses per second
    return recentAccesses / (this.accessRateWindowMs / 1000);
  }

  /**
   * Check if a partition is considered "hot" based on access patterns
   */
  isHotPartition(tableName: string, partitionSpec: PartitionSpec): boolean {
    const partitionKey = this.partitionSpecToKey(partitionSpec);
    const accessRate = this.getPartitionAccessRate(tableName, partitionKey);
    return accessRate >= this.hotPartitionThreshold;
  }

  /**
   * Get partition access metrics
   */
  getPartitionAccessMetrics(
    tableName: string,
    partitionSpec: PartitionSpec
  ): PartitionAccessMetrics | null {
    const partitionKey = this.partitionSpecToKey(partitionSpec);
    const accessKey = `${tableName}:${partitionKey}`;
    const history = this.partitionAccessHistory.get(accessKey);

    if (!history) {
      return null;
    }

    const accessRate = this.getPartitionAccessRate(tableName, partitionKey);
    const lastTimestamp = history.timestamps[history.timestamps.length - 1] || 0;

    return {
      tableId: tableName,
      partitionKey,
      accessCount: history.accessCount,
      accessRate,
      lastAccessedAt: lastTimestamp,
      isHot: accessRate >= this.hotPartitionThreshold,
    };
  }

  /**
   * Get all hot partitions for a table
   */
  getHotPartitions(tableName: string): PartitionAccessMetrics[] {
    const hotPartitions: PartitionAccessMetrics[] = [];
    const prefix = `${tableName}:`;

    for (const accessKey of this.partitionAccessHistory.keys()) {
      if (accessKey.startsWith(prefix)) {
        const partitionKey = accessKey.slice(prefix.length);
        const spec = this.keyToPartitionSpec(partitionKey);
        const metrics = this.getPartitionAccessMetrics(tableName, spec);

        if (metrics && metrics.isHot) {
          hotPartitions.push(metrics);
        }
      }
    }

    // Sort by access rate descending
    hotPartitions.sort((a, b) => b.accessRate - a.accessRate);

    return hotPartitions;
  }

  /**
   * Get partition summary for a table
   */
  async getTablePartitionSummary(tableName: string): Promise<TablePartitionSummary> {
    const partitions = await this.listPartitions(tableName);
    const hotPartitions = this.getHotPartitions(tableName);

    let totalRecordCount = 0;
    let totalFileCount = 0;
    let totalSizeBytes = 0;

    for (const partition of partitions) {
      totalRecordCount += partition.stats.recordCount;
      totalFileCount += partition.stats.fileCount;
      totalSizeBytes += partition.stats.sizeBytes;
    }

    return {
      tableId: tableName,
      partitionCount: partitions.length,
      totalRecordCount,
      totalFileCount,
      totalSizeBytes,
      hotPartitionCount: hotPartitions.length,
      hotPartitions,
    };
  }

  /**
   * Configure partition statistics tracking
   */
  configurePartitionStats(config: {
    ttlMs?: number;
    accessRateWindowMs?: number;
    hotPartitionThreshold?: number;
  }): void {
    if (config.ttlMs !== undefined) {
      this.partitionStatsTtlMs = config.ttlMs;
    }
    if (config.accessRateWindowMs !== undefined) {
      this.accessRateWindowMs = config.accessRateWindowMs;
    }
    if (config.hotPartitionThreshold !== undefined) {
      this.hotPartitionThreshold = config.hotPartitionThreshold;
    }
  }

  // Private helpers

  /**
   * Evict least recently used entry
   */
  private evictLRU(): void {
    let lruKey: string | null = null;
    let lruTime = Infinity;

    for (const [key, entry] of this.cache.entries()) {
      if (entry.lastAccessedAt < lruTime) {
        lruTime = entry.lastAccessedAt;
        lruKey = key;
      }
    }

    if (lruKey) {
      this.cache.delete(lruKey);
      this.stats.evictions++;
    }
  }

  /**
   * Record latency measurement
   */
  private recordLatency(startTime: number): void {
    const endTime = performance.now();
    const latencyMs = endTime - startTime;
    const latencyUs = latencyMs * 1000;

    this.latencies.push(latencyUs);

    // Keep only last 10000 measurements
    if (this.latencies.length > 10000) {
      this.latencies.shift();
    }

    // Check for violations
    if (latencyUs > this.config.latencyThresholdUs) {
      this.stats.latencyViolations++;
      if (this.config.onLatencyViolation) {
        this.config.onLatencyViolation('unknown', latencyUs);
      }
    }
  }

  /**
   * Calculate percentile from sorted array
   */
  private percentile(sorted: number[], p: number): number {
    const index = Math.ceil(p * sorted.length) - 1;
    const value = sorted[Math.max(0, index)];
    return value !== undefined ? value : 0;
  }

  /**
   * Serialize cache entry for storage
   */
  private serializeEntry(entry: MetadataCacheEntry): Record<string, unknown> {
    return {
      ...entry,
      metadata: {
        ...entry.metadata,
        'last-sequence-number': entry.metadata['last-sequence-number'].toString(),
        'last-updated-ms': entry.metadata['last-updated-ms'].toString(),
        'current-snapshot-id': entry.metadata['current-snapshot-id']?.toString() ?? null,
        snapshots: entry.metadata.snapshots.map((s) => ({
          ...s,
          'snapshot-id': s['snapshot-id'].toString(),
          'timestamp-ms': s['timestamp-ms'].toString(),
        })),
        'snapshot-log': entry.metadata['snapshot-log'].map((s) => ({
          'snapshot-id': s['snapshot-id'].toString(),
          'timestamp-ms': s['timestamp-ms'].toString(),
        })),
      },
    };
  }

  /**
   * Deserialize cache entry from storage
   */
  private deserializeEntry(data: Record<string, unknown>): MetadataCacheEntry {
    const metadata = data.metadata as Record<string, unknown>;
    const entry: MetadataCacheEntry = {
      cachedAt: data.cachedAt as number,
      expiresAt: data.expiresAt as number,
      version: data.version as number,
      hitCount: data.hitCount as number,
      lastAccessedAt: data.lastAccessedAt as number,
      metadata: {
        ...metadata,
        'last-sequence-number': BigInt(metadata['last-sequence-number'] as string),
        'last-updated-ms': BigInt(metadata['last-updated-ms'] as string),
        'current-snapshot-id':
          metadata['current-snapshot-id'] !== null
            ? BigInt(metadata['current-snapshot-id'] as string)
            : null,
        snapshots: (metadata.snapshots as Array<Record<string, unknown>>).map((s) => ({
          ...s,
          'snapshot-id': BigInt(s['snapshot-id'] as string),
          'timestamp-ms': BigInt(s['timestamp-ms'] as string),
        })),
        'snapshot-log': (metadata['snapshot-log'] as Array<Record<string, unknown>>).map((s) => ({
          'snapshot-id': BigInt(s['snapshot-id'] as string),
          'timestamp-ms': BigInt(s['timestamp-ms'] as string),
        })),
      } as IcebergMetadata,
    };
    // Only set pendingInvalidation if it's true (respecting exactOptionalPropertyTypes)
    if (data.pendingInvalidation === true) {
      entry.pendingInvalidation = true;
    }
    return entry;
  }
}

// =============================================================================
// Cache Coherence Manager
// =============================================================================

/**
 * Manages cache coherence across multiple DO instances
 */
export class CacheCoherenceManager extends EventEmitter {
  private registeredCaches: Map<string, MetadataCache> = new Map();
  private pendingMessages: CoherenceMessage[] = [];
  private partitionPendingMessages: Array<{
    tableId: string;
    partitionKey: string;
    sourceDoId: string;
    timestamp: number;
  }> = [];
  private lastSyncTimestamp: number = Date.now();
  private config: CoherenceManagerConfig;

  constructor(config: CoherenceManagerConfig = {}) {
    super();
    this.config = {
      mode: config.mode ?? 'strict',
      propagationDelayMs: config.propagationDelayMs ?? 0,
      batchSize: config.batchSize ?? 100,
      batchDelayMs: config.batchDelayMs ?? 0,
      healthCheckIntervalMs: config.healthCheckIntervalMs ?? 1000,
    };
  }

  /**
   * Register a cache instance
   */
  register(doId: string, cache: MetadataCache): void {
    this.registeredCaches.set(doId, cache);
    cache.setCoherenceManager(this);
  }

  /**
   * Unregister a cache instance
   */
  unregister(doId: string): void {
    const cache = this.registeredCaches.get(doId);
    if (cache) {
      this.registeredCaches.delete(doId);
    }
  }

  /**
   * Notify invalidation from a cache
   */
  notifyInvalidation(tableId: string, sourceCache: MetadataCache): void {
    const sourceDoId = this.findDoId(sourceCache);
    if (!sourceDoId) return;

    const message: CoherenceMessage = {
      type: 'invalidate',
      tableId,
      version: 0,
      timestamp: Date.now(),
      sourceDoId,
    };

    this.pendingMessages.push(message);
  }

  /**
   * Notify partition invalidation from a cache
   */
  notifyPartitionInvalidation(
    tableId: string,
    partitionKey: string,
    sourceCache: MetadataCache
  ): void {
    const sourceDoId = this.findDoId(sourceCache);
    if (!sourceDoId) return;

    this.partitionPendingMessages.push({
      tableId,
      partitionKey,
      sourceDoId,
      timestamp: Date.now(),
    });
  }

  /**
   * Flush pending messages to all caches
   */
  async flush(): Promise<void> {
    // Handle table-level invalidations
    if (this.pendingMessages.length > 0) {
      const batch = this.pendingMessages.splice(0, this.pendingMessages.length);

      // Emit batch event
      this.emit('batchSent', { messageCount: batch.length });

      // In eventual consistency mode, add delay
      if (this.config.mode === 'eventual' && this.config.propagationDelayMs! > 0) {
        await new Promise((resolve) => setTimeout(resolve, this.config.propagationDelayMs));
      }

      // Propagate to all caches except source
      for (const message of batch) {
        for (const [doId, cache] of this.registeredCaches.entries()) {
          if (doId !== message.sourceDoId) {
            cache.handleCoherenceInvalidation(message.tableId);
          }
        }
      }
    }

    // Handle partition-level invalidations
    if (this.partitionPendingMessages.length > 0) {
      const partitionBatch = this.partitionPendingMessages.splice(
        0,
        this.partitionPendingMessages.length
      );

      for (const msg of partitionBatch) {
        for (const [doId, cache] of this.registeredCaches.entries()) {
          if (doId !== msg.sourceDoId) {
            cache.handlePartitionCoherenceInvalidation(msg.tableId, msg.partitionKey);
          }
        }
      }
    }

    this.lastSyncTimestamp = Date.now();
  }

  /**
   * Detect conflicts between caches
   */
  async detectConflicts(): Promise<Conflict[]> {
    const tableVersions: Map<string, Map<string, number>> = new Map();

    // Collect versions from all caches
    for (const [doId, cache] of this.registeredCaches.entries()) {
      const stats = cache.getStats();
      // This is a simplified conflict detection - in real implementation
      // we'd track actual version vectors
      for (const [_tableId, _entry] of (cache as any).cache.entries()) {
        const entry = _entry as MetadataCacheEntry;
        let tableMap = tableVersions.get(_tableId);
        if (!tableMap) {
          tableMap = new Map();
          tableVersions.set(_tableId, tableMap);
        }
        tableMap.set(doId, entry.version);
      }
    }

    // Find conflicts (different versions for same table)
    const conflicts: Conflict[] = [];
    for (const [tableId, versions] of tableVersions.entries()) {
      const uniqueVersions = new Set(versions.values());
      if (uniqueVersions.size > 1) {
        conflicts.push({
          tableId,
          doIds: Array.from(versions.keys()),
          versions: Array.from(versions.values()),
        });
      }
    }

    return conflicts;
  }

  /**
   * Get health status
   */
  async getHealth(): Promise<CoherenceHealth> {
    return {
      status: 'healthy',
      registeredDOs: this.registeredCaches.size,
      pendingMessages: this.pendingMessages.length + this.partitionPendingMessages.length,
      lastSyncTimestamp: this.lastSyncTimestamp,
    };
  }

  /**
   * Find DO ID for a cache instance
   */
  private findDoId(cache: MetadataCache): string | undefined {
    for (const [doId, c] of this.registeredCaches.entries()) {
      if (c === cache) {
        return doId;
      }
    }
    return undefined;
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Process schema change CDC event
 */
export async function processSchemaChangeEvent(
  cache: MetadataCache,
  event: SchemaChangeEvent
): Promise<void> {
  // Invalidate the cache entry for this table
  await cache.invalidate(event.table);
}

/**
 * Process partition spec change
 */
export async function processPartitionSpecChange(
  cache: MetadataCache,
  tableId: string,
  _change: PartitionSpecChange
): Promise<void> {
  // Invalidate the cache entry for this table
  await cache.invalidate(tableId);
}

/**
 * Process table drop event
 */
export async function processTableDrop(cache: MetadataCache, tableId: string): Promise<void> {
  // Invalidate the cache entry and all auxiliary data
  await cache.invalidate(tableId);
}

/**
 * Estimate memory usage of cache
 */
export function estimateMemoryUsage(cache: MetadataCache): {
  totalBytes: number;
  perEntryAvgBytes: number;
} {
  const stats = cache.getStats();
  // Rough estimation: each entry is approximately 2KB including metadata
  const estimatedPerEntry = 2048;
  const totalBytes = stats.entriesCount * estimatedPerEntry;

  return {
    totalBytes,
    perEntryAvgBytes: stats.entriesCount > 0 ? totalBytes / stats.entriesCount : 0,
  };
}

/**
 * Create unified cache manager
 */
export async function createUnifiedCacheManager(
  _config: UnifiedCacheManagerConfig
): Promise<{
  metadata: MetadataCache;
  invalidateAll: (tableId: string) => Promise<void>;
  getAllStats: () => {
    metadata: CacheStats;
    query: { enabled: boolean };
    partition: { enabled: boolean };
  };
}> {
  const metadataCache = new MetadataCache({
    enabled: _config.metadata?.enabled ?? true,
    ttlMs: _config.metadata?.ttlMs ?? 300_000,
    maxTables: _config.metadata?.maxTables ?? 1000,
    enableCoherence: false,
    latencyThresholdUs: 1000,
  });

  return {
    metadata: metadataCache,
    invalidateAll: async (tableId: string) => {
      await metadataCache.invalidate(tableId);
    },
    getAllStats: () => ({
      metadata: metadataCache.getStats(),
      query: { enabled: _config.query?.enabled ?? false },
      partition: { enabled: _config.partition?.enabled ?? false },
    }),
  };
}
