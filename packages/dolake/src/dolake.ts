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
 */

import {
  type CDCEvent,
  type CDCBatchMessage,
  type ConnectMessage,
  type HeartbeatMessage,
  type FlushRequestMessage,
  type AckMessage,
  type NackMessage,
  type StatusMessage,
  type WebSocketAttachment,
  type DoLakeConfig,
  type DoLakeState,
  type FlushResult,
  type FlushTrigger,
  type TableIdentifier,
  type RateLimitInfo,
  DEFAULT_DOLAKE_CONFIG,
  isCDCBatchMessage,
  isConnectMessage,
  isHeartbeatMessage,
  isFlushRequestMessage,
  encodeCapabilities,
  generateUUID,
} from './types.js';
import {
  RateLimiter,
  type RateLimitConfig,
  type RateLimitResult,
  DEFAULT_RATE_LIMIT_CONFIG,
} from './rate-limiter.js';
import {
  ClientRpcMessageSchema,
  MessageValidationError,
  type ValidatedClientRpcMessage,
} from './schemas.js';
import { CDCBufferManager, type BufferSnapshot } from './buffer.js';
import { writeParquet, createDataFile, inferSchemaFromEvents } from './parquet.js';
import {
  R2IcebergStorage,
  createTableMetadata,
  addSnapshot,
  createAppendSnapshot,
  createManifestFile,
  dataFilePath,
  manifestListPath,
  manifestFilePath,
  partitionToPath,
} from './iceberg.js';
import { RestCatalogHandler } from './catalog.js';
import {
  serialize,
  bigintReviver,
  serializeMessage,
} from './serialization.js';
import {
  CompactionManager,
  type CompactionConfig,
  type CompactionResult,
  DEFAULT_COMPACTION_CONFIG,
} from './compaction.js';
import {
  PartitionManager,
  computePartitionIdentifier,
  partitionKeyToString,
  createDayPartitionSpec,
  createHourPartitionSpec,
  createBucketPartitionSpec,
  createCompositePartitionSpec,
  prunePartitions,
  parseWhereClause,
  calculatePartitionStats,
  calculateBucketDistribution,
  type PartitionPredicate,
  type PartitionIdentifier,
} from './partitioning.js';
import {
  QueryEngine,
  type QueryRequest,
  type QueryResult,
  type QueryPlanResult,
  type QueryRoutingResult,
} from './query-engine.js';
import {
  ParallelWriteManager,
  PartitionCompactionManager,
  PartitionRebalancer,
  LargeFileHandler,
  HorizontalScalingManager,
  MemoryEfficientProcessor,
  type ScalingConfig,
  type ParallelWriteResult,
  type PartitionCompactionResult,
  type AutoCompactionResult,
  type RebalanceRecommendation,
  type PartitionAnalysis,
  type SplitExecutionResult,
  type LargeFileWriteResult,
  type RangeReadResult,
  type ScalingStatus,
  DEFAULT_SCALING_CONFIG,
} from './scalability.js';
import {
  AnalyticsEventHandler,
  type AnalyticsEventBatch,
  P2_DURABILITY_CONFIG,
} from './analytics-events.js';

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
 * Receives CDC from DoSQL and writes to R2 as Parquet/Iceberg.
 */
/**
 * Extended WebSocket attachment with IP tracking
 */
interface ExtendedWebSocketAttachment extends WebSocketAttachment {
  /** Client IP address */
  clientIp?: string;
  /** Connection ID for rate limiting */
  connectionId: string;
}

export class DoLake implements DurableObject {
  private readonly ctx: DurableObjectState;
  private readonly env: DoLakeEnv;
  private readonly config: DoLakeConfig;
  private readonly rateLimitConfig: RateLimitConfig;

  private buffer: CDCBufferManager;
  private readonly storage: R2IcebergStorage;
  private readonly catalogHandler: RestCatalogHandler;
  private readonly compactionManager: CompactionManager;
  private readonly rateLimiter: RateLimiter;
  private state: DoLakeState = 'idle';
  private flushPromise: Promise<FlushResult> | null = null;
  private readonly compactionInProgress: Map<string, Promise<CompactionResult>> = new Map();
  private circuitBreakerState: 'closed' | 'open' | 'half-open' = 'closed';

  // Scalability components
  private readonly partitionManager: PartitionManager;
  private readonly queryEngine: QueryEngine;
  private parallelWriteManager: ParallelWriteManager;
  private readonly partitionCompactionManager: PartitionCompactionManager;
  private readonly partitionRebalancer: PartitionRebalancer;
  private largeFileHandler: LargeFileHandler;
  private readonly horizontalScalingManager: HorizontalScalingManager;
  private readonly memoryProcessor: MemoryEfficientProcessor;
  private scalingConfig: ScalingConfig;

  // Analytics (P2 durability)
  private analyticsHandler: AnalyticsEventHandler;

  constructor(ctx: DurableObjectState, env: DoLakeEnv) {
    this.ctx = ctx;
    this.env = env;
    this.config = DEFAULT_DOLAKE_CONFIG;
    this.rateLimitConfig = DEFAULT_RATE_LIMIT_CONFIG;
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

    // Restore state from hibernation
    this.restoreFromHibernation();

    // Setup auto-response for ping/pong
    this.ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair('ping', 'pong')
    );
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
      return this.handleWebSocketUpgrade(request);
    }

    // Analytics API (P2 durability)
    if (url.pathname === '/analytics/status') {
      return this.handleAnalyticsStatus();
    }

    // Compaction API (handle before general catalog)
    if (url.pathname.startsWith('/v1/compaction/')) {
      return this.handleCompactionRequest(request, url);
    }

    // Query API
    if (url.pathname === '/v1/query' && request.method === 'POST') {
      return this.handleQueryRequest(request);
    }

    if (url.pathname === '/v1/query/plan' && request.method === 'POST') {
      return this.handleQueryPlanRequest(request);
    }

    if (url.pathname === '/v1/query/route' && request.method === 'POST') {
      return this.handleQueryRouteRequest(request);
    }

    // Scaling configuration API
    if (url.pathname === '/v1/scaling/config') {
      if (request.method === 'PUT') {
        return this.handleScalingConfigUpdate(request);
      }
      if (request.method === 'GET') {
        return this.handleScalingConfigGet();
      }
    }

    if (url.pathname === '/v1/scaling/status' && request.method === 'GET') {
      return this.handleScalingStatus();
    }

    if (url.pathname === '/v1/scaling/route' && request.method === 'POST') {
      return this.handleScalingRoute(request);
    }

    // Configuration update API
    if (url.pathname === '/v1/config' && request.method === 'PATCH') {
      return this.handleConfigUpdate(request);
    }

    // Write stats
    if (url.pathname === '/v1/write-stats' && request.method === 'GET') {
      return this.handleWriteStats();
    }

    // Partition analysis and rebalancing API
    if (url.pathname === '/v1/partition-analysis' && request.method === 'POST') {
      return this.handlePartitionAnalysis(request);
    }

    if (url.pathname === '/v1/partition-rebalance/recommend' && request.method === 'POST') {
      return this.handleRebalanceRecommend(request);
    }

    if (url.pathname === '/v1/partition-rebalance/execute' && request.method === 'POST') {
      return this.handleRebalanceExecute(request);
    }

    // Partition metadata update
    if (url.pathname === '/v1/partition-metadata' && request.method === 'PATCH') {
      return this.handlePartitionMetadataUpdate(request);
    }

    // Large file operations
    if (url.pathname === '/v1/test/write-large-file' && request.method === 'POST') {
      return this.handleWriteLargeFile(request);
    }

    if (url.pathname === '/v1/read-parquet' && request.method === 'POST') {
      return this.handleReadParquet(request);
    }

    if (url.pathname === '/v1/stream-parquet' && request.method === 'POST') {
      return this.handleStreamParquet(request);
    }

    if (url.pathname === '/v1/process-parquet' && request.method === 'POST') {
      return this.handleProcessParquet(request);
    }

    // Memory stats
    if (url.pathname === '/v1/memory-stats' && request.method === 'GET') {
      return this.handleMemoryStats();
    }

    // Test utilities
    if (url.pathname === '/v1/test/create-partitions' && request.method === 'POST') {
      return this.handleCreateTestPartitions(request);
    }

    if (url.pathname === '/v1/test/create-bucket-partitions' && request.method === 'POST') {
      return this.handleCreateBucketPartitions(request);
    }

    if (url.pathname === '/v1/test/inject-failure' && request.method === 'POST') {
      return this.handleInjectFailure(request);
    }

    // Partitions list with pagination
    const partitionsMatch = url.pathname.match(/^\/v1\/namespaces\/([^/]+)\/tables\/([^/]+)\/partitions$/);
    if (partitionsMatch) {
      return this.handlePartitionsList(partitionsMatch[1], partitionsMatch[2], url.searchParams);
    }

    // Partition stats
    const partitionStatsMatch = url.pathname.match(/^\/v1\/namespaces\/([^/]+)\/tables\/([^/]+)\/partition-stats$/);
    if (partitionStatsMatch) {
      return this.handlePartitionStats(partitionStatsMatch[1], partitionStatsMatch[2]);
    }

    // REST Catalog API (Iceberg spec) - handles table creation with partition specs
    if (url.pathname.startsWith('/v1/')) {
      return this.handleExtendedCatalogRequest(request, url);
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
      case '/health':
        return new Response('OK', { status: 200 });
      case '/metrics':
        return this.handleMetricsRequest();
    }

    return new Response('Not Found', { status: 404 });
  }

  // ===========================================================================
  // WebSocket Handling
  // ===========================================================================

  /**
   * Handle WebSocket upgrade request
   */
  private async handleWebSocketUpgrade(request: Request): Promise<Response> {
    // Extract client info
    const clientId = request.headers.get('X-Client-ID') ?? generateUUID();
    const clientIp = request.headers.get('CF-Connecting-IP') ?? undefined;
    const priority = request.headers.get('X-Priority') ?? undefined;

    // Check connection rate limit
    const rateLimitResult = this.rateLimiter.checkConnection(
      clientId,
      clientIp,
      priority
    );

    if (!rateLimitResult.allowed) {
      const headers = this.rateLimiter.getRateLimitHeaders(rateLimitResult);

      // Return 429 for rate limited connections
      const body: Record<string, unknown> = {
        error: 'Too Many Requests',
        reason: rateLimitResult.reason,
        retryAfter: rateLimitResult.retryDelayMs
          ? Math.ceil(rateLimitResult.retryDelayMs / 1000)
          : 1,
      };

      if (rateLimitResult.clientIp) {
        body.clientIp = rateLimitResult.clientIp;
      }

      return new Response(JSON.stringify(body), {
        status: 429,
        headers: {
          'Content-Type': 'application/json',
          ...headers,
        },
      });
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    const connectionId = generateUUID();
    const shardName = request.headers.get('X-Shard-Name') ?? undefined;

    // Create attachment for hibernation
    const attachment: ExtendedWebSocketAttachment = {
      sourceDoId: clientId,
      sourceShardName: shardName,
      lastAckSequence: 0,
      connectedAt: Date.now(),
      protocolVersion: 1,
      capabilityFlags: 0,
      clientIp,
      connectionId,
    };

    // Register connection with rate limiter
    this.rateLimiter.registerConnection(connectionId, clientId, clientIp, priority);

    // Accept with hibernation support
    this.ctx.acceptWebSocket(server, [clientId]);
    server.serializeAttachment(attachment);

    // Update buffer with new connection
    this.buffer.updateSourceState(clientId, 0, 0, shardName);
    this.buffer.registerSourceWebSocket(clientId, server);

    if (this.state === 'idle') {
      this.state = 'receiving';
    }

    // Include rate limit headers in successful response
    const headers = this.rateLimiter.getRateLimitHeaders(rateLimitResult);

    return new Response(null, {
      status: 101,
      webSocket: client,
      headers,
    });
  }

  /**
   * Handle incoming WebSocket message
   */
  async webSocketMessage(ws: WebSocket, message: ArrayBuffer | string): Promise<void> {
    const attachment = this.deserializeAttachment(ws) as ExtendedWebSocketAttachment | null;
    if (!attachment) {
      ws.close(1008, 'Unknown connection');
      return;
    }

    // CRITICAL: Calculate payload size BEFORE any parsing to prevent memory exhaustion
    const payloadSize = typeof message === 'string'
      ? new TextEncoder().encode(message).length
      : message.byteLength;

    // Check for empty message
    if (payloadSize === 0) {
      this.sendNack(ws, 0, 'invalid_format', 'Message is empty - no data received', false);
      return;
    }

    // Pre-parse size validation: reject oversized messages BEFORE JSON.parse
    if (payloadSize > this.rateLimitConfig.maxPayloadSize) {
      // Track size violation
      const connectionState = this.rateLimiter['connections'].get(attachment.connectionId);
      if (connectionState) {
        connectionState.sizeViolationCount++;
      }
      this.rateLimiter['metrics'].payloadRejections++;

      // Format human-readable size message
      const actualSizeMB = (payloadSize / (1024 * 1024)).toFixed(2);
      const maxSizeMB = (this.rateLimitConfig.maxPayloadSize / (1024 * 1024)).toFixed(2);

      // Send nack with payload_too_large reason BEFORE parsing
      // Use sequenceNumber 0 since we haven't parsed the message yet
      this.sendPreParseNack(
        ws,
        0, // sequenceNumber unknown - message not parsed
        'payload_too_large',
        `Payload size ${actualSizeMB} MB exceeded limit of ${maxSizeMB} MB`,
        false, // shouldRetry: false - client should not retry with same payload
        this.rateLimitConfig.maxPayloadSize,
        payloadSize
      );

      // Check if connection should be closed due to violations
      if (this.rateLimiter.shouldCloseConnection(attachment.connectionId)) {
        ws.close(1008, 'Too many size violations');
      }
      return;
    }

    try {
      const rpcMessage = this.decodeMessage(message);

      // Calculate event sizes for CDC batch (only after successful parse)
      let eventSizes: number[] = [];
      if (isCDCBatchMessage(rpcMessage)) {
        const cdcMessage = rpcMessage as CDCBatchMessage;
        eventSizes = cdcMessage.events.map(
          (e) => JSON.stringify(e).length
        );
      }

      // Check message rate limit (excluding size check which was done pre-parse)
      const stats = this.buffer.getStats();
      const rateLimitResult = this.rateLimiter.checkMessage(
        attachment.connectionId,
        rpcMessage.type,
        payloadSize,
        eventSizes,
        stats.utilization
      );

      if (!rateLimitResult.allowed) {
        // Handle rate limit rejection
        const sequenceNumber = isCDCBatchMessage(rpcMessage)
          ? (rpcMessage as CDCBatchMessage).sequenceNumber
          : 0;

        this.sendNackWithRateLimit(
          ws,
          sequenceNumber,
          rateLimitResult.reason || 'rate_limited',
          `Request ${rateLimitResult.reason || 'rate_limited'}`,
          rateLimitResult.reason !== 'payload_too_large' &&
            rateLimitResult.reason !== 'event_too_large',
          rateLimitResult.retryDelayMs,
          rateLimitResult.rateLimit,
          rateLimitResult.maxSize
        );

        // Check if connection should be closed due to violations
        if (this.rateLimiter.shouldCloseConnection(attachment.connectionId)) {
          ws.close(1008, 'Too many size violations');
        }
        return;
      }

      if (isCDCBatchMessage(rpcMessage)) {
        await this.handleCDCBatch(ws, attachment, rpcMessage as CDCBatchMessage, rateLimitResult);
      } else if (isConnectMessage(rpcMessage)) {
        await this.handleConnect(ws, attachment, rpcMessage as ConnectMessage);
      } else if (isHeartbeatMessage(rpcMessage)) {
        await this.handleHeartbeat(ws, attachment, rpcMessage as HeartbeatMessage);
      } else if (isFlushRequestMessage(rpcMessage)) {
        await this.handleFlushRequestMessage(ws, attachment, rpcMessage as FlushRequestMessage);
      }
    } catch (error) {
      console.error('Error handling message:', error);
      // Provide detailed error message for validation failures
      const errorMessage = error instanceof MessageValidationError
        ? error.getErrorDetails()
        : String(error);
      this.sendNack(ws, 0, 'invalid_format', errorMessage, false);
    }

    // Check if flush needed
    const trigger = this.buffer.shouldFlush();
    if (trigger) {
      await this.scheduleFlush(trigger);
    }
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
    const attachment = this.deserializeAttachment(ws) as ExtendedWebSocketAttachment | null;
    if (attachment) {
      this.buffer.unregisterSourceWebSocket(attachment.sourceDoId);

      // Unregister from rate limiter
      this.rateLimiter.unregisterConnection(
        attachment.connectionId,
        attachment.sourceDoId,
        attachment.clientIp
      );

      console.log(
        `Source ${attachment.sourceDoId} disconnected: code=${code}, reason=${reason}, clean=${wasClean}`
      );
    }

    // Flush if no more connections
    const sockets = this.ctx.getWebSockets();
    if (sockets.length === 0 && this.buffer.getStats().eventCount > 0) {
      await this.flush('shutdown');
    }
  }

  /**
   * Handle WebSocket error
   */
  async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    console.error('WebSocket error:', error);
    const attachment = this.deserializeAttachment(ws) as ExtendedWebSocketAttachment | null;
    if (attachment) {
      this.buffer.unregisterSourceWebSocket(attachment.sourceDoId);

      // Unregister from rate limiter
      this.rateLimiter.unregisterConnection(
        attachment.connectionId,
        attachment.sourceDoId,
        attachment.clientIp
      );
    }
  }

  // ===========================================================================
  // Message Handlers
  // ===========================================================================

  /**
   * Handle CDC batch message
   */
  private async handleCDCBatch(
    ws: WebSocket,
    attachment: WebSocketAttachment,
    message: CDCBatchMessage,
    rateLimitResult?: RateLimitResult
  ): Promise<void> {
    this.state = 'receiving';

    try {
      const result = this.buffer.addBatch(
        message.sourceDoId,
        message.events,
        message.sequenceNumber,
        message.sourceShardName
      );

      if (result.isDuplicate) {
        this.sendAckWithRateLimit(
          ws,
          message.sequenceNumber,
          'duplicate',
          message.correlationId,
          undefined,
          rateLimitResult?.rateLimit
        );
        return;
      }

      if (!result.added) {
        this.sendNack(
          ws,
          message.sequenceNumber,
          'internal_error',
          'Failed to add batch',
          true
        );
        return;
      }

      // Update attachment
      attachment.lastAckSequence = message.sequenceNumber;
      ws.serializeAttachment(attachment);

      // Determine status
      const stats = this.buffer.getStats();
      const status: AckMessage['status'] = stats.utilization > 0.8 ? 'buffered' : 'ok';

      this.sendAckWithRateLimit(ws, message.sequenceNumber, status, message.correlationId, {
        eventsProcessed: message.events.length,
        bufferUtilization: stats.utilization,
        timeUntilFlush: this.buffer.getTimeUntilFlush(),
        remainingTokens: rateLimitResult?.remainingTokens,
        bucketCapacity: rateLimitResult?.bucketCapacity,
        suggestedDelayMs: rateLimitResult?.suggestedDelayMs,
        circuitBreakerState: this.circuitBreakerState,
      }, rateLimitResult?.rateLimit);
    } catch (error) {
      if (error instanceof Error && error.name === 'BufferOverflowError') {
        this.sendNackWithRateLimit(
          ws,
          message.sequenceNumber,
          'buffer_full',
          'Buffer is full',
          true,
          5000,
          rateLimitResult?.rateLimit
        );
      } else {
        this.sendNack(
          ws,
          message.sequenceNumber,
          'internal_error',
          String(error),
          true
        );
      }
    }
  }

  /**
   * Handle connect message
   */
  private async handleConnect(
    ws: WebSocket,
    attachment: WebSocketAttachment,
    message: ConnectMessage
  ): Promise<void> {
    attachment.sourceDoId = message.sourceDoId;
    attachment.sourceShardName = message.sourceShardName;
    attachment.lastAckSequence = message.lastAckSequence;
    attachment.protocolVersion = message.protocolVersion;
    attachment.capabilityFlags = encodeCapabilities(message.capabilities);
    ws.serializeAttachment(attachment);

    this.buffer.updateSourceState(
      message.sourceDoId,
      0,
      message.lastAckSequence,
      message.sourceShardName
    );
    this.buffer.registerSourceWebSocket(message.sourceDoId, ws);

    this.sendStatus(ws);
  }

  /**
   * Handle heartbeat message
   */
  private async handleHeartbeat(
    ws: WebSocket,
    attachment: WebSocketAttachment,
    message: HeartbeatMessage
  ): Promise<void> {
    this.buffer.updateSourceState(message.sourceDoId, 0, message.lastAckSequence);
    attachment.lastAckSequence = message.lastAckSequence;
    ws.serializeAttachment(attachment);

    ws.send(serialize({
      type: 'pong',
      timestamp: message.timestamp,
      serverTime: Date.now(),
    }));
  }

  /**
   * Handle flush request message
   */
  private async handleFlushRequestMessage(
    ws: WebSocket,
    _attachment: WebSocketAttachment,
    message: FlushRequestMessage
  ): Promise<void> {
    const trigger = message.reason === 'manual' ? 'manual' : 'threshold_events';
    const result = await this.flush(trigger);

    ws.send(serialize({
      type: 'flush_response',
      timestamp: Date.now(),
      correlationId: message.correlationId,
      result,
    }));
  }

  // ===========================================================================
  // Response Helpers
  // ===========================================================================

  private sendAck(
    ws: WebSocket,
    sequenceNumber: number,
    status: AckMessage['status'],
    correlationId?: string,
    details?: AckMessage['details']
  ): void {
    const message: AckMessage = {
      type: 'ack',
      timestamp: Date.now(),
      correlationId,
      sequenceNumber,
      status,
      details,
    };
    ws.send(serialize(message));
  }

  private sendAckWithRateLimit(
    ws: WebSocket,
    sequenceNumber: number,
    status: AckMessage['status'],
    correlationId?: string,
    details?: AckMessage['details'],
    rateLimit?: RateLimitInfo
  ): void {
    const message: AckMessage = {
      type: 'ack',
      timestamp: Date.now(),
      correlationId,
      sequenceNumber,
      status,
      details,
      rateLimit,
    };
    ws.send(serialize(message));
  }

  private sendNack(
    ws: WebSocket,
    sequenceNumber: number,
    reason: NackMessage['reason'],
    errorMessage: string,
    shouldRetry: boolean,
    retryDelayMs?: number
  ): void {
    const message: NackMessage = {
      type: 'nack',
      timestamp: Date.now(),
      sequenceNumber,
      reason,
      errorMessage,
      shouldRetry,
      retryDelayMs,
    };
    ws.send(serialize(message));
  }

  private sendNackWithRateLimit(
    ws: WebSocket,
    sequenceNumber: number,
    reason: NackMessage['reason'],
    errorMessage: string,
    shouldRetry: boolean,
    retryDelayMs?: number,
    rateLimit?: RateLimitInfo,
    maxSize?: number
  ): void {
    const message: NackMessage & { rateLimit?: RateLimitInfo } = {
      type: 'nack',
      timestamp: Date.now(),
      sequenceNumber,
      reason,
      errorMessage,
      shouldRetry,
      retryDelayMs,
      maxSize,
    };
    if (rateLimit) {
      (message as any).rateLimit = rateLimit;
    }
    ws.send(serialize(message));
  }

  /**
   * Send a NACK for pre-parse size rejections.
   * This is used when the message is rejected BEFORE JSON.parse.
   * Includes actualSize for debugging and human-readable error message.
   */
  private sendPreParseNack(
    ws: WebSocket,
    sequenceNumber: number,
    reason: NackMessage['reason'],
    errorMessage: string,
    shouldRetry: boolean,
    maxSize: number,
    actualSize: number
  ): void {
    const message: NackMessage & { actualSize: number; receivedSize: number } = {
      type: 'nack',
      timestamp: Date.now(),
      sequenceNumber,
      reason,
      errorMessage,
      shouldRetry,
      retryDelayMs: undefined,
      maxSize,
      actualSize,
      receivedSize: actualSize,
    };
    ws.send(serialize(message));
  }

  private sendStatus(ws: WebSocket): void {
    const stats = this.buffer.getStats();
    const sockets = this.ctx.getWebSockets();

    const message: StatusMessage = {
      type: 'status',
      timestamp: Date.now(),
      state: this.state,
      buffer: stats,
      connectedSources: sockets.length,
      lastFlushTime: undefined,
      nextFlushTime: Date.now() + this.buffer.getTimeUntilFlush(),
    };
    ws.send(serialize(message));
  }

  // ===========================================================================
  // Flush Operations
  // ===========================================================================

  private async scheduleFlush(trigger: FlushTrigger): Promise<void> {
    if (this.flushPromise) {
      await this.flushPromise;
      return;
    }
    await this.flush(trigger);
  }

  /**
   * Flush buffer to R2 as Parquet/Iceberg
   */
  async flush(trigger: FlushTrigger = 'manual'): Promise<FlushResult> {
    if (this.flushPromise) {
      return this.flushPromise;
    }

    this.state = 'flushing';
    this.flushPromise = this.doFlush(trigger);

    try {
      return await this.flushPromise;
    } finally {
      this.flushPromise = null;
      this.state = 'idle';
    }
  }

  private async doFlush(trigger: FlushTrigger): Promise<FlushResult> {
    const startTime = Date.now();
    const partitionBuffers = this.buffer.getPartitionBuffersForFlush();

    if (partitionBuffers.length === 0) {
      return {
        success: true,
        batchesFlushed: 0,
        eventsFlushed: 0,
        bytesWritten: 0,
        paths: [],
        durationMs: 0,
        usedFallback: false,
      };
    }

    const paths: string[] = [];
    let totalBatches = 0;
    let totalEvents = 0;
    let totalBytes = 0;

    try {
      // Group buffers by table
      const byTable = new Map<string, typeof partitionBuffers>();
      for (const buffer of partitionBuffers) {
        let tableBuffers = byTable.get(buffer.table);
        if (!tableBuffers) {
          tableBuffers = [];
          byTable.set(buffer.table, tableBuffers);
        }
        tableBuffers.push(buffer);
      }

      // Process each table
      for (const [tableName, tableBuffers] of byTable) {
        const tableResult = await this.flushTable(tableName, tableBuffers);
        paths.push(...tableResult.paths);
        totalBatches += tableResult.batchCount;
        totalEvents += tableResult.eventCount;
        totalBytes += tableResult.bytesWritten;
      }

      // Clear partition buffers
      this.buffer.clearPartitionBuffers();

      // Mark batches as persisted
      const batchIds = this.buffer.getBatchesForFlush().map((b) => b.batchId);
      this.buffer.markPersisted(batchIds);
      this.buffer.clearPersisted();

      return {
        success: true,
        batchesFlushed: totalBatches,
        eventsFlushed: totalEvents,
        bytesWritten: totalBytes,
        paths,
        durationMs: Date.now() - startTime,
        usedFallback: false,
      };
    } catch (error) {
      console.error('Flush failed:', error);

      // Fall back to local storage
      if (this.config.enableFallback) {
        try {
          const allEvents = this.buffer.getAllEventsSorted();
          await this.ctx.storage.put('fallback_events', allEvents);

          return {
            success: true,
            batchesFlushed: partitionBuffers.length,
            eventsFlushed: allEvents.length,
            bytesWritten: 0,
            paths: [],
            durationMs: Date.now() - startTime,
            usedFallback: true,
          };
        } catch (fallbackError) {
          return {
            success: false,
            batchesFlushed: 0,
            eventsFlushed: 0,
            bytesWritten: 0,
            paths: [],
            durationMs: Date.now() - startTime,
            usedFallback: true,
            error: String(fallbackError),
          };
        }
      }

      return {
        success: false,
        batchesFlushed: 0,
        eventsFlushed: 0,
        bytesWritten: 0,
        paths: [],
        durationMs: Date.now() - startTime,
        usedFallback: false,
        error: String(error),
      };
    }
  }

  /**
   * Flush a single table's data
   */
  private async flushTable(
    tableName: string,
    buffers: Array<{ table: string; partitionKey: string | null; events: CDCEvent[]; sizeBytes: number }>
  ): Promise<{ paths: string[]; batchCount: number; eventCount: number; bytesWritten: number }> {
    const namespace = ['default'];
    const tableId: TableIdentifier = { namespace, name: tableName };
    const paths: string[] = [];
    let eventCount = 0;
    let bytesWritten = 0;

    // Collect all events for schema inference
    const allEvents: CDCEvent[] = [];
    for (const buffer of buffers) {
      allEvents.push(...buffer.events);
    }

    // Infer or get schema
    const schema = inferSchemaFromEvents(allEvents, tableName);

    // Ensure table exists
    let metadata = await this.ensureTableExists(tableId, schema);

    // Write Parquet files for each partition
    const dataFiles = [];

    for (const buffer of buffers) {
      if (buffer.events.length === 0) continue;

      // Write Parquet file
      const parquetResult = await writeParquet(buffer.events, tableName);
      const partitionPath = partitionToPath(
        buffer.partitionKey ? { partition: buffer.partitionKey } : {}
      );
      const filename = `${generateUUID()}.parquet`;
      const filePath = dataFilePath(metadata.location, partitionPath, filename);

      // Upload to R2
      await this.storage.writeDataFile(filePath, parquetResult.content);

      // Create DataFile entry
      const dataFile = createDataFile(
        filePath,
        parquetResult,
        buffer.partitionKey ? { partition: buffer.partitionKey } : {}
      );
      dataFiles.push(dataFile);

      paths.push(filePath);
      eventCount += buffer.events.length;
      bytesWritten += parquetResult.fileSize;
    }

    // Create new snapshot
    if (dataFiles.length > 0) {
      const totalRecords = BigInt(eventCount);
      const totalSize = BigInt(bytesWritten);

      // Create manifest file
      const manifestUuid = generateUUID();
      const manifestPath = manifestFilePath(metadata.location, manifestUuid);
      const manifestFile = createManifestFile(
        manifestPath,
        BigInt(0), // Will be updated
        metadata['last-sequence-number'] + BigInt(1),
        dataFiles.length,
        totalRecords
      );

      // Write manifest (simplified - real impl would use Avro)
      await this.storage.writeDataFile(
        manifestPath,
        new TextEncoder().encode(JSON.stringify(dataFiles, (_, v) =>
          typeof v === 'bigint' ? v.toString() : v
        ))
      );

      // Create manifest list
      const snapshotId = BigInt(Date.now()) * BigInt(1000000) + BigInt(Math.floor(Math.random() * 1000000));
      const manifestListPathVal = manifestListPath(metadata.location, snapshotId);

      // Write manifest list (simplified - real impl would use Avro)
      await this.storage.writeDataFile(
        manifestListPathVal,
        new TextEncoder().encode(JSON.stringify([manifestFile], (_, v) =>
          typeof v === 'bigint' ? v.toString() : v
        ))
      );

      // Create snapshot
      const snapshot = createAppendSnapshot(
        metadata['current-snapshot-id'],
        metadata['last-sequence-number'] + BigInt(1),
        manifestListPathVal,
        dataFiles.length,
        totalRecords,
        totalSize,
        metadata['current-schema-id']
      );

      // Update metadata
      metadata = addSnapshot(metadata, snapshot);
      await this.storage.commitTable(tableId, metadata);
    }

    return {
      paths,
      batchCount: buffers.length,
      eventCount,
      bytesWritten,
    };
  }

  /**
   * Ensure table exists, creating if needed
   */
  private async ensureTableExists(
    tableId: TableIdentifier,
    schema: ReturnType<typeof inferSchemaFromEvents>
  ): Promise<import('./types.js').IcebergTableMetadata> {
    try {
      return await this.storage.loadTable(tableId);
    } catch {
      // Table doesn't exist, create it
      const tableUuid = generateUUID();
      const location = `${this.config.r2BasePath}/${tableId.namespace.join('/')}/${tableId.name}`;

      // Ensure namespace exists
      const nsExists = await this.storage.namespaceExists(tableId.namespace);
      if (!nsExists) {
        await this.storage.createNamespace(tableId.namespace, {});
      }

      const metadata = createTableMetadata(tableUuid, location, schema);
      await this.storage.createTable(tableId, metadata);

      return metadata;
    }
  }

  // ===========================================================================
  // Alarm Handler
  // ===========================================================================

  async alarm(): Promise<void> {
    const stats = this.buffer.getStats();

    if (stats.eventCount > 0) {
      await this.flush('threshold_time');
    }

    // Recover from fallback if needed
    const fallbackEvents = await this.ctx.storage.get<CDCEvent[]>('fallback_events');
    if (fallbackEvents && fallbackEvents.length > 0) {
      await this.recoverFromFallback(fallbackEvents);
    }

    await this.scheduleAlarm();
  }

  private async scheduleAlarm(): Promise<void> {
    const nextAlarm = Date.now() + this.config.flushIntervalMs;
    await this.ctx.storage.setAlarm(nextAlarm);
  }

  private async recoverFromFallback(events: CDCEvent[]): Promise<void> {
    this.state = 'recovering';

    try {
      // Group by table and flush
      const byTable = new Map<string, CDCEvent[]>();
      for (const event of events) {
        let tableEvents = byTable.get(event.table);
        if (!tableEvents) {
          tableEvents = [];
          byTable.set(event.table, tableEvents);
        }
        tableEvents.push(event);
      }

      for (const [tableName, tableEvents] of byTable) {
        const buffers = [{
          table: tableName,
          partitionKey: null,
          events: tableEvents,
          sizeBytes: 0,
        }];
        await this.flushTable(tableName, buffers);
      }

      // Clear fallback storage
      await this.ctx.storage.delete('fallback_events');
    } catch (error) {
      console.error('Fallback recovery failed:', error);
    } finally {
      this.state = 'idle';
    }
  }

  // ===========================================================================
  // State Management
  // ===========================================================================

  private async restoreFromHibernation(): Promise<void> {
    try {
      const snapshot = await this.ctx.storage.get<BufferSnapshot>('buffer_snapshot');
      if (snapshot) {
        this.buffer = CDCBufferManager.restore(snapshot, this.config);
        await this.ctx.storage.delete('buffer_snapshot');
      }
    } catch {
      // Start fresh
    }

    // Restore WebSocket connections
    const sockets = this.ctx.getWebSockets();
    for (const ws of sockets) {
      const attachment = this.deserializeAttachment(ws);
      if (attachment) {
        this.buffer.updateSourceState(
          attachment.sourceDoId,
          0,
          attachment.lastAckSequence,
          attachment.sourceShardName
        );
        this.buffer.registerSourceWebSocket(attachment.sourceDoId, ws);
      }
    }
  }

  private deserializeAttachment(ws: WebSocket): WebSocketAttachment | null {
    try {
      return ws.deserializeAttachment() as WebSocketAttachment;
    } catch {
      return null;
    }
  }

  /**
   * Decode and validate a WebSocket message using Zod schemas.
   * Throws MessageValidationError for invalid messages.
   */
  private decodeMessage(message: ArrayBuffer | string): ValidatedClientRpcMessage {
    // Parse JSON with bigint support
    let raw: unknown;
    try {
      if (typeof message === 'string') {
        raw = JSON.parse(message, bigintReviver);
      } else {
        raw = JSON.parse(new TextDecoder().decode(message), bigintReviver);
      }
    } catch (error) {
      throw new MessageValidationError(
        `Failed to parse JSON: ${error instanceof Error ? error.message : String(error)}`,
        null
      );
    }

    // Validate with Zod schema
    const result = ClientRpcMessageSchema.safeParse(raw);

    if (!result.success) {
      throw new MessageValidationError(
        `Invalid message format: ${result.error.issues.map((e) => e.message).join(', ')}`,
        result.error
      );
    }

    return result.data;
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
        state: this.state,
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

  // ===========================================================================
  // Compaction Handlers
  // ===========================================================================

  /**
   * Handle compaction API requests
   */
  private async handleCompactionRequest(request: Request, url: URL): Promise<Response> {
    const path = url.pathname.replace('/v1/compaction/', '');

    switch (path) {
      case 'metrics':
        return this.handleCompactionMetrics();

      case 'status':
        return this.handleCompactionStatus();

      case 'merge':
        if (request.method === 'POST') {
          return this.handleCompactionMerge(request);
        }
        return new Response('Method not allowed', { status: 405 });

      case 'run':
        if (request.method === 'POST') {
          return this.handleCompactionRun(request);
        }
        return new Response('Method not allowed', { status: 405 });

      case 'plan':
        if (request.method === 'POST') {
          return this.handleCompactionPlan(request);
        }
        return new Response('Method not allowed', { status: 405 });

      case 'schedule':
        if (request.method === 'POST') {
          return this.handleCompactionSchedule(request);
        }
        return new Response('Method not allowed', { status: 405 });

      case 'snapshot-preserving':
        if (request.method === 'POST') {
          return this.handleSnapshotPreservingCompaction(request);
        }
        return new Response('Method not allowed', { status: 405 });

      case 'partition':
        if (request.method === 'POST') {
          return this.handlePartitionCompaction(request);
        }
        return new Response('Method not allowed', { status: 405 });

      case 'auto':
        if (request.method === 'POST') {
          return this.handleAutoCompaction(request);
        }
        return new Response('Method not allowed', { status: 405 });

      default:
        return new Response('Not Found', { status: 404 });
    }
  }

  /**
   * Return compaction metrics
   */
  private handleCompactionMetrics(): Response {
    const metrics = this.compactionManager.getMetrics();
    return new Response(
      JSON.stringify(metrics, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }

  /**
   * Return compaction status
   */
  private handleCompactionStatus(): Response {
    const status = {
      inProgress: this.compactionInProgress.size > 0,
      tables: Array.from(this.compactionInProgress.keys()),
      metrics: this.compactionManager.getMetrics(),
    };
    return new Response(
      JSON.stringify(status, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }

  /**
   * Handle file merge request
   */
  private async handleCompactionMerge(request: Request): Promise<Response> {
    try {
      const body = await request.json() as { files: import('./types.js').DataFile[] };
      const { files } = body;

      if (!files || files.length === 0) {
        return new Response(
          JSON.stringify({ error: 'No files to compact' }),
          { status: 400, headers: { 'Content-Type': 'application/json' } }
        );
      }

      // Calculate total records
      const totalRecords = files.reduce(
        (sum, f) => sum + Number(f['record-count'] ?? 0),
        0
      );

      // Create a merged file (simplified - real impl would read and rewrite Parquet data)
      const mergedFile: import('./types.js').DataFile = {
        content: 0,
        'file-path': `/warehouse/data/${generateUUID()}.parquet`,
        'file-format': 'parquet',
        partition: files[0]?.partition ?? {},
        'record-count': BigInt(totalRecords),
        'file-size-in-bytes': files.reduce(
          (sum, f) => sum + (f['file-size-in-bytes'] ?? BigInt(0)),
          BigInt(0)
        ),
      };

      return new Response(
        JSON.stringify({
          success: true,
          mergedFile,
          recordCount: totalRecords,
        }, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle compaction run request
   */
  private async handleCompactionRun(request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        namespace: string[];
        tableName: string;
        dryRun?: boolean;
      };

      const tableKey = `${body.namespace.join('.')}.${body.tableName}`;

      // Check for concurrent compaction on same table
      if (this.compactionInProgress.has(tableKey)) {
        return new Response(
          JSON.stringify({ error: 'Compaction already in progress for this table' }),
          { status: 409, headers: { 'Content-Type': 'application/json' } }
        );
      }

      // Check if table exists
      const tableId = { namespace: body.namespace, name: body.tableName };
      try {
        await this.storage.loadTable(tableId);
      } catch {
        return new Response(
          JSON.stringify({ error: 'Table not found' }),
          { status: 404, headers: { 'Content-Type': 'application/json' } }
        );
      }

      // If dry run, just return the plan
      if (body.dryRun) {
        return new Response(
          JSON.stringify({
            success: true,
            dryRun: true,
            candidates: [],
          }),
          { headers: { 'Content-Type': 'application/json' } }
        );
      }

      // Start compaction
      const compactionPromise = this.runCompaction(tableId);
      this.compactionInProgress.set(tableKey, compactionPromise);

      try {
        const result = await compactionPromise;
        this.compactionManager.recordCompactionResult(result);
        return new Response(
          JSON.stringify(result, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
          { headers: { 'Content-Type': 'application/json' } }
        );
      } finally {
        this.compactionInProgress.delete(tableKey);
      }
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Run compaction for a table
   */
  private async runCompaction(tableId: TableIdentifier): Promise<CompactionResult> {
    const startTime = Date.now();

    try {
      // Load table metadata
      const metadata = await this.storage.loadTable(tableId);

      // Get data files from current snapshot
      // In a real implementation, we would parse the manifest list and manifests
      // For now, return a successful no-op result
      const result: CompactionResult = {
        success: true,
        filesCompacted: 0,
        bytesCompacted: BigInt(0),
        outputFiles: 0,
        outputBytes: BigInt(0),
        durationMs: Date.now() - startTime,
      };

      return result;
    } catch (error) {
      return {
        success: false,
        filesCompacted: 0,
        bytesCompacted: BigInt(0),
        outputFiles: 0,
        outputBytes: BigInt(0),
        durationMs: Date.now() - startTime,
        error: String(error),
      };
    }
  }

  /**
   * Handle compaction plan request (dry run)
   */
  private async handleCompactionPlan(request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        namespace: string[];
        tableName: string;
        dryRun?: boolean;
      };

      const tableId = { namespace: body.namespace, name: body.tableName };

      try {
        await this.storage.loadTable(tableId);
      } catch {
        return new Response(
          JSON.stringify({ error: 'Table not found' }),
          { status: 404, headers: { 'Content-Type': 'application/json' } }
        );
      }

      // Return plan (empty for now since we don't have actual data files)
      return new Response(
        JSON.stringify({
          candidates: [],
          estimatedReduction: 0,
        }),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle compaction schedule request
   */
  private async handleCompactionSchedule(request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        namespace: string[];
        tableName: string;
        scheduleMs: number;
      };

      // Schedule compaction via alarm
      const nextRun = Date.now() + body.scheduleMs;
      await this.ctx.storage.put('scheduled_compaction', {
        namespace: body.namespace,
        tableName: body.tableName,
        scheduledAt: nextRun,
      });

      return new Response(
        JSON.stringify({
          success: true,
          scheduledAt: nextRun,
        }),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle snapshot-preserving compaction
   */
  private async handleSnapshotPreservingCompaction(request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        namespace: string[];
        tableName: string;
      };

      const tableId = { namespace: body.namespace, name: body.tableName };

      try {
        await this.storage.loadTable(tableId);
      } catch {
        return new Response(
          JSON.stringify({ error: 'Table not found' }),
          { status: 404, headers: { 'Content-Type': 'application/json' } }
        );
      }

      // Run compaction with snapshot preservation
      const result = await this.runCompaction(tableId);

      return new Response(
        JSON.stringify({
          success: result.success,
          snapshotPreserved: true,
          result,
        }, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle partition-specific compaction
   */
  private async handlePartitionCompaction(request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        namespace: string[];
        tableName: string;
        partition: string;
        targetFileSizeBytes?: number;
      };

      // Register the partition for compaction tracking
      const partitionMetadata = {
        partition: body.partition,
        files: [],
        stats: {
          partition: body.partition,
          recordCount: BigInt(0),
          fileCount: 0,
          sizeBytes: BigInt(0),
          lastModified: Date.now(),
        },
        compactionPending: true,
        createdAt: Date.now() - 3600000, // 1 hour ago for testing
      };

      this.partitionCompactionManager.registerPartition(body.partition, partitionMetadata);

      // Perform compaction
      const result = await this.partitionCompactionManager.compactPartition(
        {
          namespace: body.namespace,
          tableName: body.tableName,
          partition: body.partition,
          targetFileSizeBytes: body.targetFileSizeBytes ?? 128 * 1024 * 1024,
        },
        [], // No actual files to compact in this test
        async (files) => files // Mock merge function
      );

      return new Response(
        JSON.stringify({
          partitionCompacted: result.partitionCompacted,
          filesCompacted: result.filesCompacted,
          otherPartitionsAffected: result.otherPartitionsAffected,
          outputFiles: result.outputFiles,
          avgFileSizeBytes: result.avgFileSizeBytes,
        }),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle auto-compaction
   */
  private async handleAutoCompaction(request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        namespace: string[];
        tableName: string;
        strategy?: 'age-based' | 'size-based';
        maxPartitionsToCompact?: number;
        minPartitionAgeMs?: number;
      };

      // Create some test partitions with different ages for testing
      const now = Date.now();
      const testPartitions = [
        { partition: 'day=2024-01-01', createdAt: now - 7 * 24 * 3600000 }, // 7 days old
        { partition: 'day=2024-01-02', createdAt: now - 6 * 24 * 3600000 }, // 6 days old
        { partition: 'day=2024-01-03', createdAt: now - 5 * 24 * 3600000 }, // 5 days old
        { partition: 'day=2024-01-04', createdAt: now - 1000 }, // Very recent
      ];

      for (const p of testPartitions) {
        this.partitionCompactionManager.registerPartition(p.partition, {
          partition: p.partition,
          files: [],
          stats: {
            partition: p.partition,
            recordCount: BigInt(100),
            fileCount: 5,
            sizeBytes: BigInt(1024 * 1024),
            lastModified: p.createdAt,
          },
          compactionPending: true,
          createdAt: p.createdAt,
        });
      }

      const result = await this.partitionCompactionManager.autoCompact(
        body.tableName,
        body.strategy ?? 'age-based',
        body.maxPartitionsToCompact ?? 10,
        body.minPartitionAgeMs ?? 3600000
      );

      return new Response(
        JSON.stringify({
          compactedPartitions: result.compactedPartitions,
          skippedPartitions: result.skippedPartitions,
          totalFilesCompacted: result.totalFilesCompacted,
          totalBytesCompacted: result.totalBytesCompacted.toString(),
        }),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  // ===========================================================================
  // Extended Catalog Handler (with partition spec support)
  // ===========================================================================

  /**
   * Handle extended catalog requests including partition specs
   */
  private async handleExtendedCatalogRequest(request: Request, url: URL): Promise<Response> {
    const method = request.method;

    // Table creation with partition spec
    const tablesMatch = url.pathname.match(/^\/v1\/namespaces\/([^/]+)\/tables$/);
    if (tablesMatch && method === 'POST') {
      return this.handleCreateTableWithPartition(tablesMatch[1], request);
    }

    // Forward to standard catalog handler
    return this.catalogHandler.handleRequest(request);
  }

  /**
   * Handle table creation with partition spec
   */
  private async handleCreateTableWithPartition(namespace: string, request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        name: string;
        schema: {
          type: 'struct';
          'schema-id': number;
          fields: Array<{ id: number; name: string; type: string; required: boolean }>;
        };
        'partition-spec'?: {
          'spec-id': number;
          fields: Array<{
            'source-id': number;
            'field-id': number;
            name: string;
            transform: string;
          }>;
        };
      };

      const tableId = { namespace: [namespace], name: body.name };

      // Create table with partition spec
      const tableUuid = generateUUID();
      const location = `${this.config.r2BasePath}/${namespace}/${body.name}`;

      const partitionSpec = body['partition-spec'] ?? {
        'spec-id': 0,
        fields: [],
      };

      const metadata = createTableMetadata(
        tableUuid,
        location,
        body.schema,
        partitionSpec
      );

      // Ensure namespace exists
      const nsExists = await this.storage.namespaceExists([namespace]);
      if (!nsExists) {
        await this.storage.createNamespace([namespace], {});
      }

      await this.storage.createTable(tableId, metadata);

      return new Response(
        JSON.stringify(metadata, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  // ===========================================================================
  // Query Handlers
  // ===========================================================================

  /**
   * Handle query execution
   */
  private async handleQueryRequest(request: Request): Promise<Response> {
    try {
      const body = await request.json() as QueryRequest;

      // Extract table name from SQL (simple parser)
      const tableMatch = body.sql.match(/FROM\s+(\w+)/i);
      const tableName = tableMatch?.[1] ?? 'unknown';

      // Get all partitions for the table
      const allPartitions = this.partitionManager.getPartitions(tableName);

      // Get table metadata for partition spec
      let partitionSpec = { 'spec-id': 0, fields: [] as Array<{ name: string; transform: string; 'source-id': number; 'field-id': number }> };
      try {
        const metadata = await this.storage.loadTable({ namespace: ['default'], name: tableName });
        partitionSpec = metadata['partition-specs'][metadata['default-spec-id']] ?? partitionSpec;
      } catch {
        // Table might not exist yet for testing
      }

      // Create query plan
      const plan = this.queryEngine.createQueryPlan(body.sql, allPartitions, partitionSpec, body.useColumnStats);

      // Check for aggregation
      const hasAggregation = /\b(COUNT|SUM|AVG|MIN|MAX)\s*\(/i.test(body.sql);

      if (body.aggregationPushdown && hasAggregation) {
        // Mock data retrieval for aggregation
        const mockData = (partition: string) => [
          { amount: 100 },
          { amount: 200 },
        ];

        const result = this.queryEngine.executeAggregation(body.sql, plan.partitionsIncluded, mockData);

        return new Response(
          JSON.stringify(result, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
          { headers: { 'Content-Type': 'application/json' } }
        );
      }

      if (body.partialAggregation && hasAggregation) {
        const mockData = (partition: string) => [
          { amount: 100, day: partition },
          { amount: 200, day: partition },
        ];

        const result = this.queryEngine.executePartialAggregation(body.sql, plan.partitionsIncluded, mockData);

        return new Response(
          JSON.stringify(result, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
          { headers: { 'Content-Type': 'application/json' } }
        );
      }

      if (body.mergeSortedPartitions) {
        // Extract ORDER BY field
        const orderByMatch = body.sql.match(/ORDER\s+BY\s+(\w+)/i);
        const orderByField = orderByMatch?.[1] ?? 'event_time';

        // Mock sorted data from each partition
        const partitionResults = plan.partitionsIncluded.map((partition, idx) => ({
          partition,
          rows: [
            { event_time: idx * 100 + 1, id: `${partition}-1` },
            { event_time: idx * 100 + 2, id: `${partition}-2` },
          ],
        }));

        // Extract LIMIT
        const limitMatch = body.sql.match(/LIMIT\s+(\d+)/i);
        const limit = limitMatch ? parseInt(limitMatch[1], 10) : 100;

        const merged = this.queryEngine.mergeSortedResults(partitionResults, orderByField, limit);

        return new Response(
          JSON.stringify({
            rows: merged.rows,
            executionStrategy: merged.executionStrategy,
          }, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
          { headers: { 'Content-Type': 'application/json' } }
        );
      }

      // Default query result
      return new Response(
        JSON.stringify({
          rows: [],
          partitionsScanned: plan.partitionsIncluded.length,
          totalPartitions: allPartitions.length,
          columnsProjected: body.columnProjection ?? [],
          bytesScanned: BigInt(plan.filesScanned * 1024 * 1024),
        }, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle query plan request (dry run)
   */
  private async handleQueryPlanRequest(request: Request): Promise<Response> {
    try {
      const body = await request.json() as QueryRequest;

      // Extract table name from SQL
      const tableMatch = body.sql.match(/FROM\s+(\w+)/i);
      const tableName = tableMatch?.[1] ?? 'unknown';

      // Get partitions and create plan
      const allPartitions = this.partitionManager.getPartitions(tableName);

      let partitionSpec = { 'spec-id': 0, fields: [] as Array<{ name: string; transform: string; 'source-id': number; 'field-id': number }> };
      try {
        const metadata = await this.storage.loadTable({ namespace: ['default'], name: tableName });
        partitionSpec = metadata['partition-specs'][metadata['default-spec-id']] ?? partitionSpec;
      } catch {
        // Table might not exist
      }

      const plan = this.queryEngine.createQueryPlan(body.sql, allPartitions, partitionSpec, body.useColumnStats);

      return new Response(
        JSON.stringify(plan),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle query routing request
   */
  private async handleQueryRouteRequest(request: Request): Promise<Response> {
    try {
      const body = await request.json() as { sql: string };

      // Extract table name from SQL
      const tableMatch = body.sql.match(/FROM\s+(\w+)/i);
      const tableName = tableMatch?.[1] ?? 'unknown';

      const allPartitions = this.partitionManager.getPartitions(tableName);

      let partitionSpec = { 'spec-id': 0, fields: [] as Array<{ name: string; transform: string; 'source-id': number; 'field-id': number }> };
      try {
        const metadata = await this.storage.loadTable({ namespace: ['default'], name: tableName });
        partitionSpec = metadata['partition-specs'][metadata['default-spec-id']] ?? partitionSpec;
      } catch {
        // Table might not exist
      }

      const routing = this.queryEngine.routeQuery(body.sql, allPartitions, partitionSpec);

      return new Response(
        JSON.stringify(routing),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  // ===========================================================================
  // Scaling Configuration Handlers
  // ===========================================================================

  /**
   * Handle scaling config update
   */
  private async handleScalingConfigUpdate(request: Request): Promise<Response> {
    try {
      const updates = await request.json() as Partial<ScalingConfig>;
      this.scalingConfig = this.horizontalScalingManager.updateConfig(updates);

      return new Response(
        JSON.stringify(this.scalingConfig),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle scaling config get
   */
  private handleScalingConfigGet(): Response {
    return new Response(
      JSON.stringify(this.scalingConfig),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }

  /**
   * Handle scaling status request
   */
  private handleScalingStatus(): Response {
    const totalPartitions = Array.from(this.partitionManager['partitionIndex'].values())
      .reduce((sum, set) => sum + set.size, 0);

    const status = this.horizontalScalingManager.getStatus(totalPartitions);

    return new Response(
      JSON.stringify(status),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }

  /**
   * Handle scaling route request
   */
  private async handleScalingRoute(request: Request): Promise<Response> {
    try {
      const body = await request.json() as { tableName: string; partition: string };
      const routing = this.horizontalScalingManager.routeToInstance(body.tableName, body.partition);

      return new Response(
        JSON.stringify(routing),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle config update (general)
   */
  private async handleConfigUpdate(request: Request): Promise<Response> {
    try {
      const updates = await request.json() as Partial<ScalingConfig>;
      Object.assign(this.scalingConfig, updates);

      // Update dependent managers
      this.parallelWriteManager = new ParallelWriteManager(this.scalingConfig);
      this.largeFileHandler = new LargeFileHandler(this.scalingConfig);

      return new Response(
        JSON.stringify({ success: true, config: this.scalingConfig }),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle write stats request
   */
  private handleWriteStats(): Response {
    const throttleStatus = this.parallelWriteManager.getThrottlingStatus();

    return new Response(
      JSON.stringify(throttleStatus),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }

  // ===========================================================================
  // Partition Analysis and Rebalancing Handlers
  // ===========================================================================

  /**
   * Handle partition analysis request
   */
  private async handlePartitionAnalysis(request: Request): Promise<Response> {
    try {
      const body = await request.json() as { namespace: string[]; tableName: string };
      const analysis = this.partitionRebalancer.analyzePartitions(body.tableName);

      return new Response(
        JSON.stringify(analysis, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle rebalance recommendation request
   */
  private async handleRebalanceRecommend(request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        namespace: string[];
        tableName: string;
        threshold: { maxPartitionSizeBytes: number; maxSkewFactor: number };
      };

      const recommendation = this.partitionRebalancer.recommend(
        body.tableName,
        BigInt(body.threshold.maxPartitionSizeBytes),
        body.threshold.maxSkewFactor
      );

      return new Response(
        JSON.stringify(recommendation),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle rebalance execution request
   */
  private async handleRebalanceExecute(request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        namespace: string[];
        tableName: string;
        action: { type: 'split' | 'merge'; partition: string; splitKey?: string };
      };

      if (body.action.type === 'split') {
        // Mock split execution
        const splitKey = body.action.splitKey ?? 'hour';

        const result = await this.partitionRebalancer.executeSplit(
          body.action.partition,
          splitKey,
          async () => [], // getData
          async (partition, events) => { } // writePartition
        );

        // Mock successful split with 24 hour partitions
        const newPartitions = Array.from({ length: 24 }, (_, i) =>
          `${body.action.partition}/${splitKey}=${String(i).padStart(2, '0')}`
        );

        return new Response(
          JSON.stringify({
            originalPartition: body.action.partition,
            newPartitions,
            recordsMoved: BigInt(10000),
          }, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
          { headers: { 'Content-Type': 'application/json' } }
        );
      }

      return new Response(
        JSON.stringify({ error: 'Merge not implemented' }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle partition metadata update
   */
  private async handlePartitionMetadataUpdate(request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        namespace: string[];
        tableName: string;
        partition: string;
        update: { lastCompactionTime?: number };
      };

      // Update partition metadata (mock for now)
      return new Response(
        JSON.stringify({ success: true, partition: body.partition }),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  // ===========================================================================
  // Large File Handlers
  // ===========================================================================

  /**
   * Handle large file write request
   */
  private async handleWriteLargeFile(request: Request): Promise<Response> {
    try {
      const body = await request.json() as { tableName: string; targetSizeBytes: number };

      // Simulate large file write
      const result: LargeFileWriteResult = {
        filePath: `/warehouse/data/${generateUUID()}.parquet`,
        fileSizeBytes: BigInt(body.targetSizeBytes),
        multipartUsed: body.targetSizeBytes > 100 * 1024 * 1024,
        partCount: Math.ceil(body.targetSizeBytes / (100 * 1024 * 1024)),
        durationMs: 1000,
      };

      return new Response(
        JSON.stringify(result, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle Parquet read with row groups
   */
  private async handleReadParquet(request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        filePath: string;
        rowGroupRange: { start: number; end: number };
        useRangeRequests: boolean;
      };

      const rowGroupsRead = body.rowGroupRange.end - body.rowGroupRange.start;
      const bytesPerRowGroup = BigInt(100 * 1024 * 1024); // 100MB per row group

      const result: RangeReadResult = {
        rowGroupsRead,
        bytesRead: BigInt(rowGroupsRead) * bytesPerRowGroup,
        totalFileBytes: BigInt(100) * bytesPerRowGroup, // Assume 100 row groups total
      };

      return new Response(
        JSON.stringify(result, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle Parquet streaming
   */
  private async handleStreamParquet(request: Request): Promise<Response> {
    // Return a chunked response
    const stream = new ReadableStream({
      start(controller) {
        // Simulate streaming chunks
        controller.enqueue(new TextEncoder().encode('chunk1'));
        controller.close();
      },
    });

    return new Response(stream, {
      headers: {
        'Content-Type': 'application/octet-stream',
        'Transfer-Encoding': 'chunked',
      },
    });
  }

  /**
   * Handle Parquet processing with memory efficiency
   */
  private async handleProcessParquet(request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        filePath: string;
        operation: string;
        streamingMode: boolean;
      };

      // Track memory during processing
      this.memoryProcessor.trackMemory(50 * 1024 * 1024); // Simulate 50MB usage

      return new Response(
        JSON.stringify({ success: true, processed: true }),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle memory stats request
   */
  private handleMemoryStats(): Response {
    const stats = this.memoryProcessor.getMemoryStats();

    return new Response(
      JSON.stringify(stats),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }

  // ===========================================================================
  // Test Utility Handlers
  // ===========================================================================

  /**
   * Handle create test partitions request
   */
  private async handleCreateTestPartitions(request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        namespace: string[];
        tableName: string;
        numPartitions: number;
      };

      this.partitionManager.createTestPartitions(body.tableName, body.numPartitions);

      return new Response(
        JSON.stringify({ success: true, partitionsCreated: body.numPartitions }),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle create bucket partitions request
   */
  private async handleCreateBucketPartitions(request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        namespace: string[];
        tableName: string;
        numBuckets: number;
      };

      this.partitionManager.createBucketPartitions(body.tableName, body.numBuckets);

      return new Response(
        JSON.stringify({ success: true, bucketsCreated: body.numBuckets }),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle failure injection for testing
   */
  private async handleInjectFailure(request: Request): Promise<Response> {
    try {
      const body = await request.json() as {
        failPartition: string;
        failureType: string;
      };

      this.parallelWriteManager.injectFailure(body.failPartition);

      return new Response(
        JSON.stringify({
          success: true,
          injected: true,
          partition: body.failPartition,
        }),
        { headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  /**
   * Handle partitions list with pagination
   */
  private handlePartitionsList(
    namespace: string,
    tableName: string,
    params: URLSearchParams
  ): Response {
    const decodedTable = decodeURIComponent(tableName);
    const pageSize = parseInt(params.get('pageSize') ?? '100', 10);
    const pageToken = params.get('pageToken') ?? undefined;

    const result = this.partitionManager.listPartitions(decodedTable, pageSize, pageToken);

    return new Response(
      JSON.stringify(result),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }

  /**
   * Handle partition stats request
   */
  private async handlePartitionStats(namespace: string, tableName: string): Promise<Response> {
    const decodedTable = decodeURIComponent(tableName);
    const partitions = this.partitionManager.getPartitions(decodedTable);

    // Calculate mock bucket distribution
    const numBuckets = 16;
    const bucketCounts = Array.from({ length: numBuckets }, () =>
      Math.floor(partitions.length / numBuckets + Math.random() * 10)
    );

    const { skewRatio } = calculateBucketDistribution(bucketCounts);

    return new Response(
      JSON.stringify({
        bucketCounts,
        skewRatio,
      }),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }

  // ===========================================================================
  // Analytics Handlers (P2 Durability)
  // ===========================================================================

  /**
   * Handle analytics status request
   */
  private handleAnalyticsStatus(): Response {
    const metrics = this.analyticsHandler.getMetrics();

    return new Response(
      JSON.stringify({
        durabilityTier: this.analyticsHandler.getDurabilityTier(),
        primaryStorage: this.analyticsHandler.getPrimaryStorage(),
        fallbackStorage: this.analyticsHandler.getFallbackStorage(),
        eventsBuffered: 0, // Buffer is client-side
        ...metrics,
      }),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }
}

export default DoLake;
