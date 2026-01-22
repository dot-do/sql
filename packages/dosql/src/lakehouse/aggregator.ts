/**
 * Lakehouse Aggregator for DoSQL
 *
 * Aggregates CDC events from multiple DOs:
 * - Collects CDC from shards
 * - Deduplicates (same LSN from replicas)
 * - Orders by global LSN
 * - Writes unified stream to lakehouse
 */

import type { WALEntry, WALReader } from '../wal/types.js';
import type { CDCSubscription, CDCFilter, ChangeEvent } from '../cdc/types.js';

import {
  type DOCDCEvent,
  type AggregatedCDCEvent,
  type DeduplicationKey,
  type CDCCursor,
  type AggregatorState,
  type AggregatorStats,
  type R2Bucket,
  LakehouseError,
  LakehouseErrorCode,
} from './types.js';

import { Ingestor } from './ingestor.js';

// =============================================================================
// Aggregator Configuration
// =============================================================================

/**
 * DO source configuration
 */
export interface DOSource {
  /** Unique DO identifier */
  doId: string;
  /** Shard ID (if applicable) */
  shardId?: string;
  /** WAL reader for this DO */
  reader: WALReader;
  /** CDC subscription for this DO */
  subscription?: CDCSubscription;
  /** Starting LSN (for new sources) */
  startLSN?: bigint;
  /** Whether this is a replica (for deduplication) */
  isReplica?: boolean;
  /** Primary DO ID (if this is a replica) */
  primaryDoId?: string;
}

/**
 * Aggregator configuration
 */
export interface AggregatorConfig {
  /** DO sources to aggregate */
  sources: DOSource[];
  /** Deduplication window in milliseconds (default: 60000) */
  deduplicationWindowMs?: number;
  /** Maximum events to buffer before forcing write (default: 10000) */
  maxBufferSize?: number;
  /** Flush interval in milliseconds (default: 5000) */
  flushIntervalMs?: number;
  /** CDC filter to apply to all sources */
  filter?: CDCFilter;
  /** Data decoder */
  decoder?: (data: Uint8Array) => Record<string, unknown>;
  /** Error callback */
  onError?: (error: Error, source: DOSource) => void;
  /** Event callback for monitoring */
  onEvent?: (event: AggregatedCDCEvent) => void;
}

// =============================================================================
// Deduplication
// =============================================================================

/**
 * Build deduplication key from event
 */
function buildDeduplicationKey(event: DOCDCEvent): string {
  // Combine table + primary key + txnId for deduplication
  const keyStr = event.entry.key
    ? btoa(String.fromCharCode(...event.entry.key))
    : '';

  return `${event.entry.table}:${keyStr}:${event.entry.txnId}`;
}

/**
 * Generate global LSN from DO ID and local LSN
 * Format: {timestamp}:{doId}:{localLSN}
 * This provides a consistent ordering across DOs
 */
function generateGlobalLSN(doId: string, localLSN: bigint, timestamp: number): string {
  // Use timestamp for primary ordering, then doId for tie-breaking
  const ts = timestamp.toString(36).padStart(12, '0');
  const lsn = localLSN.toString(36).padStart(16, '0');
  return `${ts}:${doId}:${lsn}`;
}

/**
 * Parse global LSN components
 */
function parseGlobalLSN(globalLSN: string): {
  timestamp: number;
  doId: string;
  localLSN: bigint;
} {
  const [ts, doId, lsn] = globalLSN.split(':');
  return {
    timestamp: parseInt(ts, 36),
    doId,
    localLSN: BigInt('0x' + lsn),
  };
}

// =============================================================================
// Aggregator Class
// =============================================================================

/**
 * CDC Aggregator - collects and merges CDC from multiple DOs
 */
export class Aggregator {
  private config: Required<Omit<AggregatorConfig, 'decoder' | 'onError' | 'onEvent' | 'filter'>> &
    Pick<AggregatorConfig, 'decoder' | 'onError' | 'onEvent' | 'filter'>;
  private ingestor: Ingestor;
  private sources: Map<string, DOSource> = new Map();
  private state: AggregatorState;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private cleanupTimer: ReturnType<typeof setTimeout> | null = null;
  private running = false;

  constructor(ingestor: Ingestor, config: AggregatorConfig) {
    this.ingestor = ingestor;
    this.config = {
      sources: config.sources,
      deduplicationWindowMs: config.deduplicationWindowMs ?? 60000,
      maxBufferSize: config.maxBufferSize ?? 10000,
      flushIntervalMs: config.flushIntervalMs ?? 5000,
      filter: config.filter,
      decoder: config.decoder,
      onError: config.onError,
      onEvent: config.onEvent,
    };

    // Initialize sources map
    for (const source of config.sources) {
      this.sources.set(source.doId, source);
    }

    // Initialize state
    this.state = {
      cursor: {
        doPositions: new Map(),
        lastGlobalLSN: '',
        updatedAt: Date.now(),
      },
      pendingEvents: [],
      deduplicationWindow: new Map(),
      activeDOs: new Set(this.sources.keys()),
      stats: {
        eventsProcessed: 0,
        eventsDeduplicated: 0,
        eventsWritten: 0,
        chunksCreated: 0,
        maxLagMs: 0,
        lastFlushAt: 0,
      },
    };

    // Initialize cursor positions
    for (const source of config.sources) {
      this.state.cursor.doPositions.set(source.doId, source.startLSN ?? 0n);
    }
  }

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  /**
   * Start the aggregator
   */
  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;

    // Start subscriptions for each source
    const subscriptionPromises: Promise<void>[] = [];

    for (const source of this.sources.values()) {
      subscriptionPromises.push(this.startSourceSubscription(source));
    }

    // Start flush timer
    this.flushTimer = setInterval(async () => {
      await this.flush();
    }, this.config.flushIntervalMs);

    // Start deduplication window cleanup
    this.cleanupTimer = setInterval(() => {
      this.cleanupDeduplicationWindow();
    }, this.config.deduplicationWindowMs / 2);

    // Wait for all subscriptions to start
    await Promise.allSettled(subscriptionPromises);
  }

  /**
   * Stop the aggregator
   */
  async stop(): Promise<void> {
    this.running = false;

    // Stop timers
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }

    // Stop subscriptions
    for (const source of this.sources.values()) {
      if (source.subscription) {
        source.subscription.stop();
      }
    }

    // Final flush
    await this.flush();
  }

  /**
   * Start subscription for a single source
   */
  private async startSourceSubscription(source: DOSource): Promise<void> {
    const startLSN = this.state.cursor.doPositions.get(source.doId) ?? 0n;

    try {
      // Create or use existing subscription
      if (!source.subscription) {
        // For now, we'll poll the reader directly
        // A full implementation would use the subscription
        await this.pollSource(source, startLSN);
      } else {
        // Use existing subscription
        for await (const entry of source.subscription.subscribe(startLSN, this.config.filter)) {
          if (!this.running) break;
          await this.processEntry(source, entry);
        }
      }
    } catch (error) {
      if (this.config.onError) {
        this.config.onError(
          error instanceof Error ? error : new Error(String(error)),
          source
        );
      }
    }
  }

  /**
   * Poll a source for new entries
   */
  private async pollSource(source: DOSource, fromLSN: bigint): Promise<void> {
    while (this.running) {
      try {
        const entries = await source.reader.readEntries({
          fromLSN,
          limit: 1000,
          operations: this.config.filter?.operations,
        });

        if (entries.length === 0) {
          // No new entries, wait and retry
          await new Promise(resolve => setTimeout(resolve, 100));
          continue;
        }

        for (const entry of entries) {
          await this.processEntry(source, entry);
          fromLSN = entry.lsn;
        }

        // Update cursor
        this.state.cursor.doPositions.set(source.doId, fromLSN);
        this.state.cursor.updatedAt = Date.now();
      } catch (error) {
        if (this.config.onError) {
          this.config.onError(
            error instanceof Error ? error : new Error(String(error)),
            source
          );
        }
        // Wait before retrying on error
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  }

  // ===========================================================================
  // Event Processing
  // ===========================================================================

  /**
   * Process a single WAL entry from a source
   */
  private async processEntry(source: DOSource, entry: WALEntry): Promise<void> {
    // Skip non-data operations
    if (!this.isDataOperation(entry.op)) {
      return;
    }

    // Decode data
    const data = this.decodeData(entry);

    // Create CDC event
    const cdcEvent: DOCDCEvent = {
      doId: source.doId,
      shardId: source.shardId,
      entry,
      data: data ?? {},
      receivedAt: Date.now(),
    };

    // Check for deduplication
    const aggregatedEvent = this.deduplicateEvent(cdcEvent, source);
    if (!aggregatedEvent) {
      // Event was deduplicated
      this.state.stats.eventsDeduplicated++;
      return;
    }

    // Add to pending events
    this.state.pendingEvents.push(aggregatedEvent);
    this.state.stats.eventsProcessed++;

    // Callback
    if (this.config.onEvent) {
      this.config.onEvent(aggregatedEvent);
    }

    // Check if we should flush
    if (this.state.pendingEvents.length >= this.config.maxBufferSize) {
      await this.flush();
    }
  }

  /**
   * Deduplicate an event
   */
  private deduplicateEvent(
    event: DOCDCEvent,
    source: DOSource
  ): AggregatedCDCEvent | null {
    const dedupKey = buildDeduplicationKey(event);
    const now = Date.now();

    // Check if we've seen this event recently
    const lastSeen = this.state.deduplicationWindow.get(dedupKey);

    if (lastSeen) {
      // Event was seen within deduplication window
      // Only deduplicate if this is a replica
      if (source.isReplica) {
        return null;
      }
    }

    // Mark as seen
    this.state.deduplicationWindow.set(dedupKey, now);

    // Generate global LSN
    const globalLSN = generateGlobalLSN(
      event.doId,
      event.entry.lsn,
      event.entry.timestamp
    );

    return {
      globalLSN,
      sourceDOs: [event.doId],
      event,
      isDeduplicated: lastSeen !== undefined,
    };
  }

  /**
   * Clean up old entries from deduplication window
   */
  private cleanupDeduplicationWindow(): void {
    const cutoff = Date.now() - this.config.deduplicationWindowMs;

    for (const [key, timestamp] of this.state.deduplicationWindow) {
      if (timestamp < cutoff) {
        this.state.deduplicationWindow.delete(key);
      }
    }
  }

  // ===========================================================================
  // Flush Operations
  // ===========================================================================

  /**
   * Flush pending events to ingestor
   */
  async flush(): Promise<void> {
    if (this.state.pendingEvents.length === 0) return;

    // Sort by global LSN for consistent ordering
    const events = [...this.state.pendingEvents];
    events.sort((a, b) => a.globalLSN.localeCompare(b.globalLSN));

    // Convert to DOCDCEvents for ingestor
    const cdcEvents = events.map(e => e.event);

    try {
      // Ingest events
      const result = await this.ingestor.ingest(cdcEvents);

      // Update stats
      this.state.stats.eventsWritten += result.eventsProcessed;
      this.state.stats.chunksCreated += result.chunksWritten.length;
      this.state.stats.lastFlushAt = Date.now();

      // Update last global LSN
      if (events.length > 0) {
        this.state.cursor.lastGlobalLSN = events[events.length - 1].globalLSN;
      }

      // Clear pending events
      this.state.pendingEvents = [];

      // Calculate lag
      this.updateLagMetrics();
    } catch (error) {
      throw new LakehouseError(
        LakehouseErrorCode.AGGREGATION_ERROR,
        `Failed to flush aggregated events: ${error instanceof Error ? error.message : 'Unknown'}`,
        { eventCount: events.length },
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Update lag metrics
   */
  private updateLagMetrics(): void {
    const now = Date.now();
    let maxLag = 0;

    for (const source of this.sources.values()) {
      // Estimate lag based on last event timestamp
      // A full implementation would track actual lag from each DO
      const position = this.state.cursor.doPositions.get(source.doId);
      if (position) {
        // Rough estimate: assume 1ms per LSN
        const estimatedLag = Number(position) % 1000; // Simplified
        maxLag = Math.max(maxLag, estimatedLag);
      }
    }

    this.state.stats.maxLagMs = maxLag;
  }

  // ===========================================================================
  // Source Management
  // ===========================================================================

  /**
   * Add a new DO source
   */
  addSource(source: DOSource): void {
    if (this.sources.has(source.doId)) {
      throw new LakehouseError(
        LakehouseErrorCode.CONFIG_ERROR,
        `Source ${source.doId} already exists`
      );
    }

    this.sources.set(source.doId, source);
    this.state.activeDOs.add(source.doId);
    this.state.cursor.doPositions.set(source.doId, source.startLSN ?? 0n);

    // Start subscription if running
    if (this.running) {
      this.startSourceSubscription(source);
    }
  }

  /**
   * Remove a DO source
   */
  removeSource(doId: string): void {
    const source = this.sources.get(doId);
    if (!source) return;

    // Stop subscription
    if (source.subscription) {
      source.subscription.stop();
    }

    this.sources.delete(doId);
    this.state.activeDOs.delete(doId);
    // Keep cursor position for potential re-add
  }

  /**
   * Get source by ID
   */
  getSource(doId: string): DOSource | undefined {
    return this.sources.get(doId);
  }

  /**
   * List all sources
   */
  listSources(): DOSource[] {
    return Array.from(this.sources.values());
  }

  // ===========================================================================
  // Helper Methods
  // ===========================================================================

  /**
   * Check if operation is a data operation
   */
  private isDataOperation(op: string): boolean {
    return op === 'INSERT' || op === 'UPDATE' || op === 'DELETE';
  }

  /**
   * Decode WAL entry data
   */
  private decodeData(entry: WALEntry): Record<string, unknown> | null {
    const data = entry.after ?? entry.before;
    if (!data) return null;

    if (this.config.decoder) {
      return this.config.decoder(data);
    }

    // Default JSON decoder
    try {
      const json = new TextDecoder().decode(data);
      return JSON.parse(json);
    } catch {
      return null;
    }
  }

  // ===========================================================================
  // State & Statistics
  // ===========================================================================

  /**
   * Get current cursor
   */
  getCursor(): CDCCursor {
    return { ...this.state.cursor };
  }

  /**
   * Get aggregator statistics
   */
  getStatistics(): AggregatorStats {
    return { ...this.state.stats };
  }

  /**
   * Get pending event count
   */
  getPendingCount(): number {
    return this.state.pendingEvents.length;
  }

  /**
   * Check if aggregator is running
   */
  isRunning(): boolean {
    return this.running;
  }

  /**
   * Get full state (for debugging/monitoring)
   */
  getState(): Readonly<AggregatorState> {
    return this.state;
  }
}

// =============================================================================
// Shard-Aware Aggregator
// =============================================================================

/**
 * Shard topology for aggregation
 */
export interface ShardTopology {
  /** Shard ID */
  shardId: string;
  /** Primary DO ID */
  primaryDoId: string;
  /** Replica DO IDs */
  replicaDoIds: string[];
}

/**
 * Create an aggregator from shard topology
 */
export function createAggregatorFromTopology(
  ingestor: Ingestor,
  topology: ShardTopology[],
  readers: Map<string, WALReader>,
  config: Omit<AggregatorConfig, 'sources'>
): Aggregator {
  const sources: DOSource[] = [];

  for (const shard of topology) {
    // Add primary
    const primaryReader = readers.get(shard.primaryDoId);
    if (primaryReader) {
      sources.push({
        doId: shard.primaryDoId,
        shardId: shard.shardId,
        reader: primaryReader,
        isReplica: false,
      });
    }

    // Add replicas
    for (const replicaId of shard.replicaDoIds) {
      const replicaReader = readers.get(replicaId);
      if (replicaReader) {
        sources.push({
          doId: replicaId,
          shardId: shard.shardId,
          reader: replicaReader,
          isReplica: true,
          primaryDoId: shard.primaryDoId,
        });
      }
    }
  }

  return new Aggregator(ingestor, { ...config, sources });
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an aggregator
 */
export function createAggregator(
  ingestor: Ingestor,
  config: AggregatorConfig
): Aggregator {
  return new Aggregator(ingestor, config);
}
