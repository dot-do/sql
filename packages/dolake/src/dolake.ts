/**
 * DoLake Durable Object
 *
 * The main lakehouse Durable Object that:
 * - Receives CDC events from DoSQL via WebSocket
 * - Batches events by table/partition
 * - Writes Parquet files to R2
 * - Maintains Iceberg metadata (snapshots, manifests)
 * - Exposes REST Catalog API for external query engines
 *
 * Uses WebSocket Hibernation for 95% cost reduction on idle connections.
 *
 * This module delegates to focused handlers:
 * - websocket-handler.ts: WebSocket message handling and connection management
 * - state-machine.ts: State transitions and persistence
 * - catalog-router.ts: REST catalog API routing
 * - flush-manager.ts: Buffer flush and Parquet write coordination
 */

import {
  type DoLakeConfig,
  type DoLakeState,
  type FlushResult,
  type FlushTrigger,
  type CDCEvent,
  DEFAULT_DOLAKE_CONFIG,
} from './types.js';
import {
  RateLimiter,
  type RateLimitConfig,
  DEFAULT_RATE_LIMIT_CONFIG,
} from './rate-limiter.js';
import { CDCBufferManager, type BufferSnapshot } from './buffer.js';
import { R2IcebergStorage } from './iceberg.js';
import { RestCatalogHandler } from './catalog.js';
import {
  CompactionManager,
  type CompactionResult,
  DEFAULT_COMPACTION_CONFIG,
} from './compaction.js';
import { PartitionManager } from './partitioning.js';
import { QueryEngine } from './query-engine.js';
import {
  ParallelWriteManager,
  PartitionCompactionManager,
  PartitionRebalancer,
  LargeFileHandler,
  HorizontalScalingManager,
  MemoryEfficientProcessor,
  type ScalingConfig,
  DEFAULT_SCALING_CONFIG,
} from './scalability.js';
import {
  AnalyticsEventHandler,
  P2_DURABILITY_CONFIG,
} from './analytics-events.js';
import { CacheInvalidator } from './cache-invalidation.js';

// Import modular handlers
import {
  WebSocketHandler,
  restoreFromHibernation,
  setupWebSocketAutoResponse,
  type ExtendedWebSocketAttachment,
} from './websocket-handler.js';
import {
  DoLakeStateMachine,
  createStatePersistence,
  handleAlarm,
  scheduleAlarm,
  createRecoveryHandler,
  type StatePersistence,
} from './state-machine.js';
import { CatalogRouter, type CatalogRouterDeps } from './catalog-router.js';
import { FlushManager, recoverFromFallback, type PartitionBuffer } from './flush-manager.js';

// =============================================================================
// Environment Interface
// =============================================================================

/**
 * Environment bindings for DoLake
 */
export interface DoLakeEnv {
  /** R2 bucket for lakehouse data */
  LAKEHOUSE_BUCKET: R2Bucket;

  /** Optional KV for metadata caching */
  LAKEHOUSE_KV?: KVNamespace;
}

// =============================================================================
// DoLake Durable Object
// =============================================================================

/**
 * DoLake Durable Object
 *
 * The main lakehouse Durable Object that receives CDC events from DoSQL instances,
 * batches them efficiently, and writes them to R2 as Parquet files with Iceberg metadata.
 *
 * @remarks
 * DoLake uses WebSocket hibernation for 95% cost reduction on idle connections.
 * It supports multiple CDC producers (DoSQL shards) connected simultaneously,
 * with automatic deduplication, rate limiting, and backpressure signaling.
 *
 * @example
 * ```typescript
 * // In your worker, export the DoLake class
 * import { DoLake } from '@dotdo/dolake';
 * export { DoLake };
 *
 * // Route requests to DoLake
 * export default {
 *   async fetch(request: Request, env: Env): Promise<Response> {
 *     const id = env.DOLAKE.idFromName('default');
 *     const stub = env.DOLAKE.get(id);
 *     return stub.fetch(request);
 *   },
 * };
 * ```
 */
export class DoLake implements DurableObject {
  /** Durable Object state for persistence and WebSocket management */
  private readonly ctx: DurableObjectState;

  /** Environment bindings including R2 bucket and optional KV namespace */
  private readonly env: DoLakeEnv;

  /** DoLake configuration (flush thresholds, buffer sizes, etc.) */
  private readonly config: DoLakeConfig;

  /** Rate limiting configuration (connections/s, messages/s, payload limits) */
  private readonly rateLimitConfig: RateLimitConfig;

  // Core components
  /** CDC buffer manager for batching and deduplication */
  private buffer: CDCBufferManager;

  /** R2 storage adapter for Iceberg table operations */
  private readonly storage: R2IcebergStorage;

  /** REST Catalog handler for Iceberg API compatibility */
  private readonly catalogHandler: RestCatalogHandler;

  /** Compaction manager for merging small Parquet files */
  private readonly compactionManager: CompactionManager;

  /** Rate limiter with token bucket and IP-based limits */
  private readonly rateLimiter: RateLimiter;

  // State management
  /** State machine for tracking DoLake operational state */
  private readonly stateMachine: DoLakeStateMachine;

  /** Persistence layer for state recovery after hibernation */
  private readonly persistence: StatePersistence;

  /** Circuit breaker state for R2 failure handling */
  private circuitBreakerState: 'closed' | 'open' | 'half-open' = 'closed';

  /** Active compaction operations by table/partition */
  private readonly compactionInProgress: Map<string, Promise<CompactionResult>> = new Map();

  // Modular handlers
  /** WebSocket message handler (CDC batches, heartbeats, etc.) */
  private readonly wsHandler: WebSocketHandler;

  /** HTTP router for REST Catalog API endpoints */
  private readonly catalogRouter: CatalogRouter;

  /** Flush coordinator for buffer-to-R2 writes */
  private readonly flushManager: FlushManager;

  /** Recovery handler for fallback storage replay */
  private readonly recoveryHandler: (events: CDCEvent[]) => Promise<{ success: boolean; eventsRecovered: number; tablesProcessed: number; error?: string }>;

  // Scalability components
  /** Partition manager for routing events to partitions */
  private readonly partitionManager: PartitionManager;

  /** Query engine for partition pruning and query planning */
  private readonly queryEngine: QueryEngine;

  /** Parallel write manager for concurrent Parquet file creation */
  private parallelWriteManager: ParallelWriteManager;

  /** Partition-level compaction coordinator */
  private readonly partitionCompactionManager: PartitionCompactionManager;

  /** Load balancer for partition rebalancing */
  private readonly partitionRebalancer: PartitionRebalancer;

  /** Handler for oversized files requiring chunked processing */
  private largeFileHandler: LargeFileHandler;

  /** Coordinator for multi-DO horizontal scaling */
  private readonly horizontalScalingManager: HorizontalScalingManager;

  /** Memory-efficient processor for streaming large datasets */
  private readonly memoryProcessor: MemoryEfficientProcessor;

  /** Scaling configuration (parallelism, memory limits, etc.) */
  private scalingConfig: ScalingConfig;

  // Analytics (P2 durability)
  /** Analytics event handler with P2 durability guarantees */
  private analyticsHandler: AnalyticsEventHandler;

  // Cache invalidation
  /** Cache invalidator for downstream cache consistency */
  private readonly cacheInvalidator: CacheInvalidator;

  /**
   * Creates a new DoLake instance.
   *
   * @param ctx - Durable Object state provided by the Cloudflare runtime.
   *              Used for persistence, WebSocket management, and alarm scheduling.
   * @param env - Environment bindings containing:
   *              - `LAKEHOUSE_BUCKET`: R2 bucket for Parquet/Iceberg storage (required)
   *              - `LAKEHOUSE_KV`: KV namespace for metadata caching (optional)
   *
   * @remarks
   * The constructor initializes all internal components synchronously:
   * - Core: buffer manager, R2 storage, catalog handler, compaction manager, rate limiter
   * - State: state machine, persistence layer
   * - Handlers: WebSocket handler, catalog router, flush manager, recovery handler
   * - Scalability: partition manager, query engine, parallel write manager, etc.
   * - Analytics: P2 durability handler
   * - Cache: invalidation coordinator
   *
   * After initialization, it restores state from hibernation and sets up
   * WebSocket auto-response for ping/pong keep-alive.
   */
  constructor(ctx: DurableObjectState, env: DoLakeEnv) {
    this.ctx = ctx;
    this.env = env;
    this.config = DEFAULT_DOLAKE_CONFIG;
    this.rateLimitConfig = DEFAULT_RATE_LIMIT_CONFIG;

    // Initialize core components
    this.buffer = new CDCBufferManager(this.config);
    this.storage = new R2IcebergStorage(
      env.LAKEHOUSE_BUCKET,
      this.config.r2BasePath,
      ctx.storage
    );
    this.catalogHandler = new RestCatalogHandler(this.storage, {
      warehouseLocation: `r2://${this.config.r2BucketName}/${this.config.r2BasePath}`,
      catalogName: 'dolake',
    });
    this.compactionManager = new CompactionManager(DEFAULT_COMPACTION_CONFIG);
    this.rateLimiter = new RateLimiter(this.rateLimitConfig);

    // Initialize state management
    this.stateMachine = new DoLakeStateMachine({
      flushIntervalMs: this.config.flushIntervalMs,
      enableFallback: this.config.enableFallback,
    });
    this.persistence = createStatePersistence(ctx);

    // Initialize scalability components
    this.scalingConfig = DEFAULT_SCALING_CONFIG;
    this.partitionManager = new PartitionManager();
    this.queryEngine = new QueryEngine();
    this.parallelWriteManager = new ParallelWriteManager(this.scalingConfig);
    this.partitionCompactionManager = new PartitionCompactionManager();
    this.partitionRebalancer = new PartitionRebalancer();
    this.largeFileHandler = new LargeFileHandler(this.scalingConfig);
    this.horizontalScalingManager = new HorizontalScalingManager(this.scalingConfig);
    this.memoryProcessor = new MemoryEfficientProcessor();

    // Initialize analytics handler (P2 durability)
    this.analyticsHandler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);

    // Initialize cache invalidator
    this.cacheInvalidator = new CacheInvalidator();

    // Initialize flush manager
    this.flushManager = new FlushManager({
      config: this.config,
      storage: this.storage,
      ctx: this.ctx,
    });

    // Initialize WebSocket handler
    this.wsHandler = new WebSocketHandler(
      {
        ctx: this.ctx,
        buffer: this.buffer,
        rateLimiter: this.rateLimiter,
        getState: () => this.stateMachine.getState(),
        setState: (state: DoLakeState) => this.stateMachine.forceState(state, 'ws_handler'),
        getCircuitBreakerState: () => this.circuitBreakerState,
        scheduleFlush: (trigger) => this.scheduleFlush(trigger as FlushTrigger),
        flush: (trigger) => this.flush(trigger as FlushTrigger),
      },
      { rateLimitConfig: this.rateLimitConfig }
    );

    // Initialize catalog router
    const catalogRouterDeps: CatalogRouterDeps = {
      config: this.config,
      storage: this.storage,
      catalogHandler: this.catalogHandler,
      compactionManager: this.compactionManager,
      partitionManager: this.partitionManager,
      queryEngine: this.queryEngine,
      parallelWriteManager: this.parallelWriteManager,
      partitionCompactionManager: this.partitionCompactionManager,
      partitionRebalancer: this.partitionRebalancer,
      largeFileHandler: this.largeFileHandler,
      horizontalScalingManager: this.horizontalScalingManager,
      memoryProcessor: this.memoryProcessor,
      analyticsHandler: this.analyticsHandler,
      cacheInvalidator: this.cacheInvalidator,
      scalingConfig: this.scalingConfig,
      compactionInProgress: this.compactionInProgress,
      getScalingConfig: () => this.scalingConfig,
      setScalingConfig: (config) => { this.scalingConfig = config; },
      updateScalingDependencies: (config) => {
        this.parallelWriteManager = new ParallelWriteManager(config);
        this.largeFileHandler = new LargeFileHandler(config);
      },
    };
    this.catalogRouter = new CatalogRouter(catalogRouterDeps);

    // Initialize recovery handler
    this.recoveryHandler = createRecoveryHandler(
      this.stateMachine,
      this.persistence,
      async (tableName, events) => {
        const buffers: PartitionBuffer[] = [{
          table: tableName,
          partitionKey: null,
          events,
          sizeBytes: 0,
        }];
        await this.flushManager.flushTable(tableName, buffers);
      }
    );

    // Restore state from hibernation
    this.restoreFromHibernation();

    // Setup auto-response for ping/pong
    setupWebSocketAutoResponse(ctx);
  }

  // ===========================================================================
  // HTTP Handler
  // ===========================================================================

  /**
   * Handle incoming HTTP requests
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // WebSocket upgrade for CDC streaming
    if (request.headers.get('Upgrade') === 'websocket') {
      return this.wsHandler.handleWebSocketUpgrade(request);
    }

    // Analytics API (P2 durability)
    if (url.pathname === '/analytics/status') {
      return this.catalogRouter.handleAnalyticsStatus();
    }

    // Try catalog router first
    const catalogResponse = await this.catalogRouter.route(request, url);
    if (catalogResponse) {
      return catalogResponse;
    }

    // Management endpoints
    switch (url.pathname) {
      case '/status':
        return this.handleStatusRequest();
      case '/flush':
        if (request.method === 'POST') {
          return this.handleFlushRequest();
        }
        break;
      case '/cdc':
        if (request.method === 'POST') {
          return this.handleCDCRequest(request);
        }
        break;
      case '/health':
        return new Response('OK', { status: 200 });
      case '/metrics':
        return this.handleMetricsRequest();
    }

    return new Response('Not Found', { status: 404 });
  }

  // ===========================================================================
  // WebSocket Handling (delegated)
  // ===========================================================================

  /**
   * Handle incoming WebSocket message
   */
  async webSocketMessage(ws: WebSocket, message: ArrayBuffer | string): Promise<void> {
    await this.wsHandler.handleMessage(ws, message);
  }

  /**
   * Handle WebSocket close
   */
  async webSocketClose(
    ws: WebSocket,
    code: number,
    reason: string,
    wasClean: boolean
  ): Promise<void> {
    await this.wsHandler.handleClose(ws, code, reason, wasClean);
  }

  /**
   * Handle WebSocket error
   */
  async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    await this.wsHandler.handleError(ws, error);
  }

  // ===========================================================================
  // Flush Operations
  // ===========================================================================

  private async scheduleFlush(trigger: FlushTrigger): Promise<void> {
    await this.flushManager.scheduleFlush(
      trigger,
      this.buffer,
      (state) => this.stateMachine.forceState(state === 'flushing' ? 'flushing' : 'idle', 'schedule_flush')
    );
  }

  /**
   * Flush buffer to R2 as Parquet/Iceberg
   */
  async flush(trigger: FlushTrigger = 'manual'): Promise<FlushResult> {
    const result = await this.flushManager.flush(
      trigger,
      this.buffer,
      (state) => this.stateMachine.forceState(state === 'flushing' ? 'flushing' : 'idle', 'flush')
    );

    // Flush any pending cache invalidations after data is written to R2
    if (result.success) {
      await this.cacheInvalidator.flushPendingInvalidations();
    }

    return result;
  }

  // ===========================================================================
  // Alarm Handler
  // ===========================================================================

  /**
   * Handles scheduled alarms for periodic operations.
   *
   * This method is called by the Cloudflare Durable Object runtime when an alarm fires.
   * Alarms are used to:
   * - Trigger periodic buffer flushes to ensure data durability
   * - Wake the DO from hibernation to check for pending work
   * - Recover from fallback storage if primary writes failed
   *
   * Note: When the DO is hibernated, the alarm will wake it automatically.
   * The alarm is rescheduled after each execution based on flushIntervalMs.
   *
   * @see https://developers.cloudflare.com/durable-objects/api/alarms/
   */
  async alarm(): Promise<void> {
    await handleAlarm(
      {
        stateMachine: this.stateMachine,
        persistence: this.persistence,
        buffer: this.buffer,
        flush: (trigger) => this.flush(trigger as FlushTrigger),
        recoverFromFallback: async (events) => {
          await this.recoveryHandler(events);
        },
      },
      {
        flushIntervalMs: this.config.flushIntervalMs,
        enableFallback: this.config.enableFallback,
      }
    );
  }

  // ===========================================================================
  // State Management
  // ===========================================================================

  private async restoreFromHibernation(): Promise<void> {
    this.buffer = await restoreFromHibernation(this.ctx, this.buffer, this.config);
  }

  // ===========================================================================
  // HTTP Handlers
  // ===========================================================================

  private handleStatusRequest(): Response {
    const stats = this.buffer.getStats();
    const sockets = this.ctx.getWebSockets();
    const rateLimitMetrics = this.rateLimiter.getMetrics();

    return new Response(
      JSON.stringify({
        state: this.stateMachine.getState(),
        buffer: stats,
        connectedSources: sockets.length,
        sourceStates: Array.from(this.buffer.getSourceStates().entries()).map(
          ([id, state]) => ({ id, ...state })
        ),
        dedupStats: this.buffer.getDedupStats(),
        rateLimits: {
          connectionsPerSecond: this.rateLimitConfig.connectionsPerSecond,
          messagesPerSecond: this.rateLimitConfig.messagesPerSecond,
          maxPayloadSize: this.rateLimitConfig.maxPayloadSize,
          burstCapacity: this.rateLimitConfig.burstCapacity,
        },
        rateLimitMetrics,
        degradedMode: this.rateLimiter.isDegraded(),
        loadLevel: this.rateLimiter.getLoadLevel(),
      }),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }

  private async handleFlushRequest(): Promise<Response> {
    const result = await this.flush('manual');
    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  private handleMetricsRequest(): Response {
    const stats = this.buffer.getStats();
    const dedupStats = this.buffer.getDedupStats();
    const sockets = this.ctx.getWebSockets();
    const rateLimitMetrics = this.rateLimiter.getMetrics();

    // Prometheus-style metrics
    const metrics = `
# HELP dolake_buffer_events Number of events in buffer
# TYPE dolake_buffer_events gauge
dolake_buffer_events ${stats.eventCount}

# HELP dolake_buffer_bytes Buffer size in bytes
# TYPE dolake_buffer_bytes gauge
dolake_buffer_bytes ${stats.totalSizeBytes}

# HELP dolake_buffer_utilization Buffer utilization ratio
# TYPE dolake_buffer_utilization gauge
dolake_buffer_utilization ${stats.utilization}

# HELP dolake_connected_sources Number of connected source DOs
# TYPE dolake_connected_sources gauge
dolake_connected_sources ${sockets.length}

# HELP dolake_dedup_checks Total deduplication checks
# TYPE dolake_dedup_checks counter
dolake_dedup_checks ${dedupStats.totalChecks}

# HELP dolake_dedup_duplicates Total duplicates found
# TYPE dolake_dedup_duplicates counter
dolake_dedup_duplicates ${dedupStats.duplicatesFound}

# HELP dolake_rate_limited_total Total rate limited requests
# TYPE dolake_rate_limited_total counter
dolake_rate_limited_total ${rateLimitMetrics.connectionsRateLimited + rateLimitMetrics.messagesRateLimited}

# HELP dolake_rate_limit_violations Total rate limit violations
# TYPE dolake_rate_limit_violations counter
dolake_rate_limit_violations ${rateLimitMetrics.payloadRejections + rateLimitMetrics.eventSizeRejections}

# HELP dolake_connections_rate_limited Connections rate limited
# TYPE dolake_connections_rate_limited counter
dolake_connections_rate_limited ${rateLimitMetrics.connectionsRateLimited}

# HELP dolake_messages_rate_limited Messages rate limited
# TYPE dolake_messages_rate_limited counter
dolake_messages_rate_limited ${rateLimitMetrics.messagesRateLimited}

# HELP dolake_payload_rejections Payload size rejections
# TYPE dolake_payload_rejections counter
dolake_payload_rejections ${rateLimitMetrics.payloadRejections}

# HELP dolake_preparse_size_rejections Pre-parse size rejections (before JSON.parse)
# TYPE dolake_preparse_size_rejections counter
dolake_preparse_size_rejections ${rateLimitMetrics.payloadRejections}

# HELP dolake_event_size_rejections Event size rejections
# TYPE dolake_event_size_rejections counter
dolake_event_size_rejections ${rateLimitMetrics.eventSizeRejections}

# HELP dolake_connections_closed_violations Connections closed for violations
# TYPE dolake_connections_closed_violations counter
dolake_connections_closed_violations ${rateLimitMetrics.connectionsClosed}

# HELP dolake_ip_rejections IP-based rejections
# TYPE dolake_ip_rejections counter
dolake_ip_rejections ${rateLimitMetrics.ipRejections}

# HELP dolake_load_shedding_events Load shedding events
# TYPE dolake_load_shedding_events counter
dolake_load_shedding_events ${rateLimitMetrics.loadSheddingEvents}

# HELP dolake_active_connections Currently active connections
# TYPE dolake_active_connections gauge
dolake_active_connections ${rateLimitMetrics.activeConnections}

# HELP dolake_peak_connections Peak connections
# TYPE dolake_peak_connections gauge
dolake_peak_connections ${rateLimitMetrics.peakConnections}
`.trim();

    return new Response(metrics, {
      headers: { 'Content-Type': 'text/plain' },
    });
  }

  /**
   * Handle HTTP CDC request (for testing and HTTP-based CDC ingestion)
   */
  private async handleCDCRequest(request: Request): Promise<Response> {
    try {
      const body = await request.json() as { events: CDCEvent[] };
      const events = body.events;

      if (!events || !Array.isArray(events) || events.length === 0) {
        return new Response(JSON.stringify({ error: 'No events provided' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      // Add events to buffer
      const sourceId = 'http-cdc-' + Date.now();
      const result = this.buffer.addBatch(
        sourceId,
        events,
        Date.now(),
        'http-cdc'
      );

      // Process CDC events for cache invalidation
      await this.cacheInvalidator.processCDCEvents(events);

      return new Response(JSON.stringify({
        success: true,
        eventsReceived: events.length,
        eventsAccepted: result.eventsAccepted,
        isDuplicate: result.isDuplicate,
      }), {
        headers: { 'Content-Type': 'application/json' },
      });
    } catch (error) {
      return new Response(JSON.stringify({ error: String(error) }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }
}

export default DoLake;
